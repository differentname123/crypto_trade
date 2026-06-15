# -*- coding: utf-8 -*-
"""
:description:
基于生产者-消费者模型的高并发、极速、双引擎 K 线数据获取基建。
内部采用完全异步高并发架构，外部暴露极简的纯同步阻塞式接口。
"""

import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import time
import os
import json
import logging
import aiohttp
from datetime import datetime, timedelta

# 解除 Pandas 控制台打印时的省略号限制，展示完整列
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# 配置基础日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("QuantSniper")


# =====================================================================
# 📦 模块一：核心处理器 (Consumer / Data Processor)
# =====================================================================
async def data_processor(queue, symbol_list, target_time_ms, timeframe_ms,
                         completion_event, memory_pool):
    logger.info(f"[Processor] 🧠 数据处理大脑已启动，管理 {len(symbol_list)} 个币种队列...")

    reached_symbols = set()
    stats = {"HIST": 0, "WS": 0, "REST_POLL": 0}

    while True:
        symbol, kline, source, is_closed = await queue.get()
        ts = int(kline[0])
        stats[source] += 1

        memory_pool[symbol][ts] = kline

        if symbol not in reached_symbols:
            condition_ws_closed = (ts == target_time_ms and is_closed)
            condition_next_candle = (ts >= target_time_ms + timeframe_ms)

            if condition_ws_closed or condition_next_candle:
                reached_symbols.add(symbol)
                pending = set(symbol_list) - reached_symbols
                pending_str = f"等待滞后项: {pending}" if pending else "全员集结完毕"
                trigger_reason = "WS精准收线" if condition_ws_closed else f"探测到下周期({ts})"

                logger.info(
                    f"[Processor] 🏁 {symbol} 目标K线已圆满闭合! [{trigger_reason}], 功臣:[{source}] | {pending_str}")

                if len(reached_symbols) == len(symbol_list):
                    logger.info(
                        f"[Processor] 🎯 核心目标 {target_time_ms} 完美收线达成！生命周期吞吐量: 历史追赶={stats['HIST']}条, WS并发={stats['WS']}条, 轮询={stats['REST_POLL']}条")
                    completion_event.set()
                    queue.task_done()
                    break

        queue.task_done()


# =====================================================================
# 🚜 模块二：数据搬运工 (Producers / Data Fetchers)
# =====================================================================

async def fetch_historical_rest(exchange, symbol, timeframe, since_ms, queue):
    start_t = time.time()
    limit = 1000
    curr_since = since_ms
    total_fetched = 0

    while True:
        try:
            ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit)
            if not ohlcvs:
                break

            total_fetched += len(ohlcvs)
            for k in ohlcvs:
                await queue.put((symbol, k, "HIST", False))

            curr_since = ohlcvs[-1][0] + 1
            if len(ohlcvs) < limit:
                break
        except asyncio.CancelledError:
            logger.info(f"[History] 🛑 {symbol} 历史追赶被强行中断。")
            raise
        except Exception as e:
            logger.warning(f"[History] {symbol} 拉取异常: {e}，重试中...")
            await asyncio.sleep(2)

    cost_t = time.time() - start_t
    logger.info(f"[History] 📦 {symbol} 历史追赶完成 | 共入库 {total_fetched} 根 | 耗时 {cost_t:.2f}s")


async def fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url):
    ws_symbols = [s.replace("/", "").split(":")[0].lower() for s in symbol_list]
    stream_names = [f"{s}@kline_{timeframe}" for s in ws_symbols]
    stream_url = f"wss://fstream.binance.com/market/stream?streams={'/'.join(stream_names)}"

    logger.info(f"[WS Engine] ⚡ 准备点火! 完整订阅 URL: {stream_url}")
    msg_count = 0

    try:
        timeout = aiohttp.ClientTimeout(total=None, connect=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            logger.info(f"[WS Engine] 📡 正在通过代理 {proxy_url} 发起 WebSocket 握手...")

            async with session.ws_connect(stream_url, proxy=proxy_url, heartbeat=10) as ws:
                logger.info("[WS Engine] ✅ 握手成功！全双工通道已建立，正在蹲守数据...")

                async for msg in ws:
                    if msg_count < 3:
                        logger.info(
                            f"[WS Engine - 探针] 收到报文 | 类型: {msg.type} | 长度: {len(str(msg.data))} | 内容预览: {str(msg.data)[:150]}")

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if 'data' in data and 'k' in data['data']:
                            k_data = data['data']['k']
                            msg_count += 1

                            symbol_raw = data['data']['s']
                            target_symbol = f"{symbol_raw[:-4]}/{symbol_raw[-4:]}:USDT"

                            kline = [
                                int(k_data['t']), float(k_data['o']), float(k_data['h']),
                                float(k_data['l']), float(k_data['c']), float(k_data['v'])
                            ]
                            is_closed = bool(k_data['x'])
                            await queue.put((target_symbol, kline, "WS", is_closed))

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error(f"[WS Engine] WS 连接出现底层异常: {ws.exception()}")
                        break
                    elif msg.type == aiohttp.WSMsgType.CLOSED:
                        logger.warning("[WS Engine] WS 被远程服务器主动关闭。")
                        break

    except asyncio.TimeoutError:
        logger.error(
            "[WS Engine] ❌ 致命错误: WebSocket 握手超时 (>10s)！大概率是代理软件不支持 WSS 转发或节点被墙。")
    except aiohttp.ClientError as e:
        logger.error(f"[WS Engine] ❌ 致命错误: aiohttp 客户端网络异常: {e}")
    except asyncio.CancelledError:
        logger.info(f"[WS Engine] 🛑 终结信号生效，WS 连接已安全掐断 (存活期共解析 {msg_count} 条流数据)")
        raise
    except Exception as e:
        logger.error(f"[WS Engine] ❌ 未知异常断开: {e}", exc_info=True)


async def fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue):
    logger.info(f"[REST Engine] 🚜 REST 轮询兜底引擎点火 (频率: 2次/秒)...")
    poll_count = 0
    try:
        while True:
            req_start_time = time.time()

            tasks = [exchange.fetch_ohlcv(sym, timeframe, limit=2) for sym in symbol_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            req_end_time = time.time()
            cost_ms = (req_end_time - req_start_time) * 1000

            local_now_str = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            poll_count += 1

            for sym, ohlcvs in zip(symbol_list, results):
                if isinstance(ohlcvs, Exception):
                    logger.warning(f"[REST Engine] {sym} 拉取异常: {ohlcvs}")
                    continue
                if ohlcvs:
                    latest_ts = ohlcvs[-1][0]
                    latest_time_bj = pd.to_datetime(latest_ts, unit='ms').tz_localize('UTC').tz_convert(
                        'Asia/Shanghai').strftime('%H:%M:%S')

                    logger.info(
                        f"[REST 探针] 本地时间: {local_now_str} | 网络耗时: {cost_ms:.0f}ms | {sym} 返回的最新K线: {latest_ts} ({latest_time_bj})")

                    for k in ohlcvs:
                        await queue.put((sym, k, "REST_POLL", False))

            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        logger.info(f"[REST Engine] 🛑 终结信号生效，REST 轮询瞬间终止 (存活期共轮询 {poll_count} 次)")
        raise

    # =====================================================================


# 💾 模块三：异步落盘机制 (Storage Engine)
# =====================================================================
def _save_to_csv_sync(final_dfs, cache_dir):
    os.makedirs(cache_dir, exist_ok=True)
    for symbol, df in final_dfs.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_latest.csv")
        df.to_csv(path, index=False)


async def save_to_csv_background(final_dfs, cache_dir="data"):
    await asyncio.to_thread(_save_to_csv_sync, final_dfs, cache_dir)
    logger.info(f"[Storage] 💾 异步落盘任务完成 ({len(final_dfs)} 个币种已持久化至 {cache_dir}/ 目录)")


# =====================================================================
# 🧠 模块四：中央大脑与流程编排 (Task Orchestrator)
# =====================================================================
async def _async_core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str,
                                           use_ws, use_rest, proxy_url):
    """
    【私有异步引擎】负责处理所有的并发逻辑，不建议外部直接调用。
    """
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'aiohttp_proxy': proxy_url,
        'proxies': {
            'http': proxy_url,
            'https': proxy_url
        }
    })

    try:
        logger.info(f"[Orchestrator] 初始化 | 交易所: Binance | 代理: {proxy_url} | 预热市场数据...")
        await exchange.load_markets()
    except Exception as e:
        logger.error(f"[Orchestrator] 预热失败，请检查代理: {e}")
        await exchange.close()
        return {}

    queue = asyncio.Queue()
    memory_pool = {sym: {} for sym in symbol_list}
    completion_event = asyncio.Event()

    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    target_time_ms = int(pd.to_datetime(target_time_str).tz_localize('Asia/Shanghai').timestamp() * 1000)
    start_time_ms = target_time_ms - (days * 24 * 60 * 60 * 1000)
    current_time_ms = exchange.milliseconds()

    target_close_time_ms = target_time_ms + timeframe_ms

    logger.info(
        f"[Orchestrator] 参数锁死 | 目标K线: {target_time_str} ({target_time_ms}) | 将于 {exchange.iso8601(target_close_time_ms)} 圆满收线")
    logger.info(f"[Orchestrator] 引擎配置 | WebSocket启用: {use_ws} | REST轮询启用: {use_rest}")

    processor_task = asyncio.create_task(
        data_processor(queue, symbol_list, target_time_ms, timeframe_ms, completion_event, memory_pool)
    )

    logger.info(f"[Orchestrator] 🚀 历史数据追赶任务已分发至底层协程...")
    history_tasks = [
        asyncio.create_task(fetch_historical_rest(exchange, sym, timeframe, start_time_ms, queue))
        for sym in symbol_list
    ]

    sleep_time_ms = target_close_time_ms - 15000 - current_time_ms
    if sleep_time_ms > 0:
        logger.info(
            f"[Orchestrator] 💤 历史引擎轰鸣中... 主线程战术休眠 {sleep_time_ms / 1000:.1f}s 等待目标K线最后 15s 闭合窗口...")
        await asyncio.sleep(sleep_time_ms / 1000)
    else:
        logger.info("[Orchestrator] ⚠️ 目标时间已在 15 秒内或已成历史，直接进入激进双擎狙击模式！")

    engine_tasks = []
    if use_ws:
        ws_task = asyncio.create_task(fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url))
        engine_tasks.append(ws_task)
    if use_rest:
        rest_polling_task = asyncio.create_task(fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue))
        engine_tasks.append(rest_polling_task)

    if not engine_tasks:
        logger.warning("[Orchestrator] ⚠️ 警告：WS和REST引擎均未启用，如果目标时间未成历史，程序将无法获取实时收线数据！")

    await completion_event.wait()

    logger.info("[Orchestrator] 🔫 目标全线圆满！发令枪响，瞬间强杀(Cancel)所有底层拉取协程...")
    for task in history_tasks:
        if not task.done():
            task.cancel()
    for task in engine_tasks:
        if not task.done():
            task.cancel()

    if not processor_task.done():
        processor_task.cancel()

    await exchange.close()

    final_dfs = {}
    for sym in symbol_list:
        kline_list = list(memory_pool[sym].values())
        kline_list.sort(key=lambda x: x[0])
        df = pd.DataFrame(kline_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        df = df[(df['timestamp'] >= start_time_ms) & (df['timestamp'] <= target_time_ms)].reset_index(drop=True)
        df['datetime_bj'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Shanghai')
        final_dfs[sym] = df

    await save_to_csv_background(final_dfs)

    total_pts = sum(len(df) for df in final_dfs.values())
    logger.info(f"[Orchestrator] 🎉 任务圆满结束！瞬间清洗并返回 {total_pts} 条高质量(已完全闭合)的 K 线数据。")
    return final_dfs


# =====================================================================
# 🌟 对外暴露的公共 API (Public Synchronous Interface)
# =====================================================================
def snipe_kline_data(symbol_list, timeframe, days, target_time_str,
                     use_ws=True, use_rest=True, proxy_url='http://127.0.0.1:7890'):
    """
    极速双引擎 K 线数据狙击手（对外提供的纯同步阻塞调用接口）。
    调用此函数会阻塞当前线程，直到数据抓取落盘完毕并返回 DataFrame 字典。

    参数:
        symbol_list: 币种列表，如 ["BTC/USDT:USDT"]
        timeframe: K线周期，如 "1m", "5m"
        days: 回溯历史的天数
        target_time_str: 目标等待的收线时间字符串，如 "2023-10-01 12:00"
        use_ws: 是否启用 WebSocket 引擎
        use_rest: 是否启用 REST 轮询兜底引擎
        proxy_url: 科学上网代理地址

    返回:
        dict: key 为币种，value 为对应的 Pandas DataFrame
    """
    # 捕获如果用户在 Jupyter Notebook 等自带事件循环的环境中调用的异常
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # 如果是在 Jupyter 中运行，提示用户使用 nest_asyncio 或特定的写法
        raise RuntimeError("检测到当前环境（如 Jupyter Notebook）已存在运行中的异步事件循环。\n"
                           "请在顶部执行：\n"
                           "import nest_asyncio\n"
                           "nest_asyncio.apply()\n"
                           "然后再调用本函数。")

    return asyncio.run(
        _async_core_sniping_orchestrator(
            symbol_list, timeframe, days, target_time_str, use_ws, use_rest, proxy_url
        )
    )


# =====================================================================
# 🚀 启动入口 (普通调用者视角)
# =====================================================================
if __name__ == "__main__":
    # 模拟普通开发者的调用场景：干净、整洁、无异步心智负担
    symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"]

    # 设定目标时间 (假设狙击当前分钟的收线)
    target_time = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")

    print(">>> 准备调用数据引擎...")

    # 就像调用普通函数一样，一键获取数据
    result_map = snipe_kline_data(
        symbol_list=symbols,
        timeframe="1m",
        days=1,
        target_time_str=target_time,
        use_ws=True,
        use_rest=True,
        proxy_url='http://127.0.0.1:7890'
    )

    print("\n>>> 调用完毕，主线程继续执行！")

    if result_map and "BTC/USDT:USDT" in result_map:
        print("\n最终产出预览 (BTC):")
        print(result_map["BTC/USDT:USDT"].tail())
    else:
        print("\n[系统提示] 未获取到完整数据。")