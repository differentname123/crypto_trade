# -*- coding: utf-8 -*-
"""
:description:
基于生产者-消费者模型的高并发、极速、双引擎 K 线数据获取基建。
包含历史追赶、15秒精确唤醒、WebSocket与REST双重并发狙击。
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
    """
    唯一的大脑：负责从队列取出数据，更新内存，去重，并判断是否满足“完全收线”的返回条件。
    """
    logger.info(f"[Processor] 🧠 数据处理大脑已启动，管理 {len(symbol_list)} 个币种队列...")

    reached_symbols = set()
    stats = {"HIST": 0, "WS": 0, "REST_POLL": 0}

    while True:
        # 1. 解包数据 (新增 is_closed 标识用于判断是否圆满)
        symbol, kline, source, is_closed = await queue.get()
        ts = int(kline[0])
        stats[source] += 1

        # 2. 内存写入与去重 (无缝覆盖更新)
        memory_pool[symbol][ts] = kline

        # 3. 达标判定逻辑 (严苛的圆满条件)
        if symbol not in reached_symbols:
            # 核心逻辑：如何证明 target_time_ms 的数据已经圆满？
            # 条件1：WS引擎精准推送了该目标时间戳的“闭合/收线”标记
            condition_ws_closed = (ts == target_time_ms and is_closed)
            # 条件2：REST或WS接收到了“下一根”K线的数据，绝对反推目标时间已成历史
            condition_next_candle = (ts >= target_time_ms + timeframe_ms)

            if condition_ws_closed or condition_next_candle:
                reached_symbols.add(symbol)
                pending = set(symbol_list) - reached_symbols
                pending_str = f"等待滞后项: {pending}" if pending else "全员集结完毕"
                trigger_reason = "WS精准收线" if condition_ws_closed else f"探测到下周期({ts})"

                logger.info(
                    f"[Processor] 🏁 {symbol} 目标K线已圆满闭合! [{trigger_reason}], 功臣:[{source}] | {pending_str}")

                # 4. 一锤定音：所有币种均已证明收线圆满！
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
    """历史数据搬运工"""
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
                # 历史拉取的数据默认视为未验证收线 (交由 ts >= target + timeframe 验证)
                await queue.put((symbol, k, "HIST", False))

            curr_since = ohlcvs[-1][0] + 1
            if len(ohlcvs) < limit:
                break
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"[History] {symbol} 拉取异常: {e}，重试中...")
            await asyncio.sleep(2)

    cost_t = time.time() - start_t
    logger.info(f"[History] 📦 {symbol} 历史追赶完成 | 共入库 {total_fetched} 根 | 耗时 {cost_t:.2f}s")


async def fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url):
    """一号实时引擎：WebSocket 毫秒级狙击手 (附带收线探测)"""
    ws_symbols = [s.replace("/", "").split(":")[0].lower() for s in symbol_list]
    stream_names = [f"{s}@kline_{timeframe}" for s in ws_symbols]
    stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"

    logger.info(f"[WS Engine] ⚡ WebSocket 极速引擎点火，监听 {len(symbol_list)} 个全双工流...")
    msg_count = 0

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(stream_url, proxy=proxy_url) as ws:
                async for msg in ws:
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

                            # 提取币安特有的收线字段 x
                            is_closed = bool(k_data['x'])
                            await queue.put((target_symbol, kline, "WS", is_closed))

                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("[WS Engine] WS 连接出现内部错误")
                        break
    except asyncio.CancelledError:
        logger.info(f"[WS Engine] 🛑 终结信号生效，WS 连接已安全掐断 (存活期共解析 {msg_count} 条流数据)")
    except Exception as e:
        logger.error(f"[WS Engine] 异常断开: {e}")


async def fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue):
    """二号实时引擎：REST 高频轮询兜底"""
    logger.info(f"[REST Engine] 🚜 REST 轮询兜底引擎点火 (频率: 2次/秒)...")
    poll_count = 0
    try:
        while True:
            tasks = [exchange.fetch_ohlcv(sym, timeframe, limit=2) for sym in symbol_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            poll_count += 1

            for sym, ohlcvs in zip(symbol_list, results):
                if isinstance(ohlcvs, Exception):
                    continue
                if ohlcvs:
                    for k in ohlcvs:
                        # 轮询的数据同样交由 ts >= target + timeframe 去验证其绝对收线
                        await queue.put((sym, k, "REST_POLL", False))

            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        logger.info(f"[REST Engine] 🛑 终结信号生效，REST 轮询瞬间终止 (存活期共轮询 {poll_count} 次)")


# =====================================================================
# 💾 模块三：异步落盘机制 (Storage Engine)
# =====================================================================
async def save_to_csv_background(final_dfs, cache_dir="data"):
    """后台静默保存"""
    os.makedirs(cache_dir, exist_ok=True)
    for symbol, df in final_dfs.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_latest.csv")
        df.to_csv(path, index=False)
    logger.info(f"[Storage] 💾 异步落盘任务完成 ({len(final_dfs)} 个币种已持久化至 {cache_dir}/ 目录)")


# =====================================================================
# 🧠 模块四：中央大脑与流程编排 (Task Orchestrator)
# =====================================================================
async def core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str):
    proxy_url = 'http://127.0.0.1:7890'
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

    # 真正的收盘时间等于 开盘时间 + K线周期
    target_close_time_ms = target_time_ms + timeframe_ms

    logger.info(
        f"[Orchestrator] 参数锁死 | 目标K线: {target_time_str} ({target_time_ms}) | 将于 {exchange.iso8601(target_close_time_ms)} 圆满收线")

    # 1. 启动大脑 (传入 timeframe_ms)
    processor_task = asyncio.create_task(
        data_processor(queue, symbol_list, target_time_ms, timeframe_ms, completion_event, memory_pool)
    )

    # 2. 启动历史追赶
    logger.info(f"[Orchestrator] 🚀 历史数据追赶任务已分发至底层协程...")
    history_tasks = [
        asyncio.create_task(fetch_historical_rest(exchange, sym, timeframe, start_time_ms, queue))
        for sym in symbol_list
    ]

    # 3. 智能休眠判断 (The Sniper Window - 调整为在收线前15秒唤醒)
    sleep_time_ms = target_close_time_ms - 15000 - current_time_ms
    if sleep_time_ms > 0:
        logger.info(
            f"[Orchestrator] 💤 历史引擎轰鸣中... 主线程战术休眠 {sleep_time_ms / 1000:.1f}s 等待目标K线最后 15s 闭合窗口...")
        await asyncio.sleep(sleep_time_ms / 1000)
    else:
        logger.info("[Orchestrator] ⚠️ 目标时间已在 15 秒内或已成历史，直接进入激进双擎狙击模式！")

    # 4. 双引擎点火
    ws_task = asyncio.create_task(fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url))
    rest_polling_task = asyncio.create_task(fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue))

    # 5. 等待发令枪响
    await completion_event.wait()

    # 6. 瞬间拔除
    logger.info("[Orchestrator] 🔫 目标全线圆满！发令枪响，瞬间强杀(Cancel)所有底层拉取协程...")
    for task in history_tasks:
        if not task.done():
            task.cancel()
    ws_task.cancel()
    rest_polling_task.cancel()
    processor_task.cancel()
    await exchange.close()

    # 7. 组装 DataFrames 返回
    final_dfs = {}
    for sym in symbol_list:
        kline_list = list(memory_pool[sym].values())
        kline_list.sort(key=lambda x: x[0])
        df = pd.DataFrame(kline_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # 截取范围，确保只截取到 target_time_ms 本身
        df = df[(df['timestamp'] >= start_time_ms) & (df['timestamp'] <= target_time_ms)].reset_index(drop=True)
        df['datetime_bj'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Shanghai')
        final_dfs[sym] = df

    # 8. 后台异步落盘
    asyncio.create_task(save_to_csv_background(final_dfs))

    total_pts = sum(len(df) for df in final_dfs.values())
    logger.info(f"[Orchestrator] 🎉 任务圆满结束！瞬间清洗并返回 {total_pts} 条高质量(已完全闭合)的 K 线数据。")
    return final_dfs


# =====================================================================
# 🚀 启动入口
# =====================================================================
if __name__ == "__main__":
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


    async def main():
        symbol_list = ["BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"]
        timeframe = "1m"
        days = 3

        # 测试逻辑：目标锁定为当前时间的后1分钟
        target_time_str = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")

        result_map = await core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str)

        if result_map and "BTC/USDT:USDT" in result_map:
            print("\n最终产出预览 (BTC):")
            print(result_map["BTC/USDT:USDT"].tail())
        else:
            print("\n[系统提示] 未获取到完整数据。")


    asyncio.run(main())