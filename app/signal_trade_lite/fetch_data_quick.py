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

# 配置基础日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("QuantSniper")


# =====================================================================
# 📦 模块一：核心处理器 (Consumer / Data Processor)
# =====================================================================
async def data_processor(queue, symbol_list, target_time_ms,
                         completion_event, memory_pool):
    """
    唯一的大脑：负责从队列取出数据，更新内存，去重，并判断是否满足返回条件。
    """
    logger.info("[Processor] 数据处理大脑已启动，监听队列中...")

    while True:
        # 1. 从队列获取数据：(币种, [时间戳, 开, 高, 低, 收, 量])
        symbol, kline = await queue.get()
        ts = int(kline[0])

        # 2. 内存写入与去重 (利用 Dict 的 Key 唯一性无缝去重)
        memory_pool[symbol][ts] = kline

        # 3. 达标判定逻辑
        # 检查当前收到数据的币种，其最新时间戳是否已达标
        if ts >= target_time_ms:
            all_ready = True
            for sym in symbol_list:
                if not memory_pool[sym]:
                    all_ready = False
                    break
                # 获取该币种在内存中的最新时间戳
                latest_ts = max(memory_pool[sym].keys())
                if latest_ts < target_time_ms:
                    all_ready = False
                    break

            # 4. 一锤定音：所有币种均达标，扣动扳机！
            if all_ready:
                logger.info(f"[Processor] 🎯 核心目标达成！所有币种数据均已到达目标时间: {target_time_ms}")
                completion_event.set()
                queue.task_done()
                break

        queue.task_done()


# =====================================================================
# 🚜 模块二：数据搬运工 (Producers / Data Fetchers)
# =====================================================================

async def fetch_historical_rest(exchange, symbol, timeframe, since_ms, queue):
    """历史数据搬运工：负责把指定时间起到现在的历史数据全部拉完并放入队列"""
    logger.info(f"[{symbol}] 开始追赶历史数据...")
    limit = 1000
    curr_since = since_ms

    while True:
        try:
            ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit)
            if not ohlcvs:
                break

            for k in ohlcvs:
                await queue.put((symbol, k))

            curr_since = ohlcvs[-1][0] + 1
            if len(ohlcvs) < limit:
                break  # 拉到底了
        except Exception as e:
            logger.warning(f"[{symbol}] 历史拉取异常: {e}，重试中...")
            await asyncio.sleep(2)

    logger.info(f"[{symbol}] 历史数据追赶完成！")


async def fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url):
    """一号实时引擎：WebSocket 毫秒级狙击手 (只在最后 15 秒存活)"""
    # 将 BTC/USDT 转换为 btcusdt 格式以符合币安 WS 规范
    ws_symbols = [s.replace("/", "").split(":")[0].lower() for s in symbol_list]
    stream_names = [f"{s}@kline_{timeframe}" for s in ws_symbols]
    stream_url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"

    logger.info(f"[WS Engine] ⚡ WebSocket 极速引擎已点火，监听 {len(symbol_list)} 个流...")

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(stream_url, proxy=proxy_url) as ws:
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if 'data' in data and 'k' in data['data']:
                            k_data = data['data']['k']
                            # 只处理收线的 K 线 或者 强行截取
                            if k_data['x']:  # x 代表 is_final 是否收线
                                symbol_raw = data['data']['s']  # 例: BTCUSDT
                                # 还原回请求的 symbol 格式 (简易处理，实际应用中可用映射表)
                                target_symbol = f"{symbol_raw[:-4]}/{symbol_raw[-4:]}:USDT"

                                kline = [
                                    int(k_data['t']), float(k_data['o']), float(k_data['h']),
                                    float(k_data['l']), float(k_data['c']), float(k_data['v'])
                                ]
                                await queue.put((target_symbol, kline))
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        logger.error("[WS Engine] WS 连接出现内部错误")
                        break
    except asyncio.CancelledError:
        logger.info("[WS Engine] 🛑 收到终结信号，WebSocket 连接瞬间掐断。")
    except Exception as e:
        logger.error(f"[WS Engine] WS 断开或异常: {e}")


async def fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue):
    """二号实时引擎：REST 高频轮询兜底 (只在最后 15 秒存活)"""
    logger.info(f"[REST Engine] 🚜 REST 轮询兜底引擎已点火...")
    try:
        while True:
            # 并发请求最新的一根 K 线
            tasks = [exchange.fetch_ohlcv(sym, timeframe, limit=2) for sym in symbol_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for sym, ohlcvs in zip(symbol_list, results):
                if isinstance(ohlcvs, Exception):
                    continue
                if ohlcvs:
                    # 把最新两根全塞进队列，大脑会自动去重
                    for k in ohlcvs:
                        await queue.put((sym, k))

            await asyncio.sleep(0.5)  # 每 500 毫秒轮询一次，极其高频
    except asyncio.CancelledError:
        logger.info("[REST Engine] 🛑 收到终结信号，REST 轮询瞬间终止。")


# =====================================================================
# 💾 模块三：异步落盘机制 (Storage Engine)
# =====================================================================
async def save_to_csv_background(final_dfs, cache_dir="data"):
    """后台静默保存，不阻塞主程序返回"""
    os.makedirs(cache_dir, exist_ok=True)
    for symbol, df in final_dfs.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_latest.csv")
        df.to_csv(path, index=False)
    logger.info(f"[Storage] 💾 所有币种数据已后台静默持久化至 {cache_dir}。")


# =====================================================================
# 🧠 模块四：中央大脑与流程编排 (Task Orchestrator)
# =====================================================================
async def core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str):
    proxy_url = 'http://127.0.0.1:7890'
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'aiohttp_proxy': proxy_url,  # [修改点1]: 显式指定 aiohttp 专属的异步代理配置
        'proxies': {
            'http': proxy_url,
            'https': proxy_url
        }
    })

    # [修复点]: 提前显式加载市场数据，防止多协程启动时隐式触发带来的并发踩踏
    try:
        logger.info("[Orchestrator] 正在初始化交易所并预热市场数据，检查代理连接...")
        await exchange.load_markets()
    except Exception as e:
        logger.error(f"[Orchestrator] 预热失败，请检查代理 {proxy_url} 是否运行正常: {e}")
        await exchange.close()
        return {}

    # 初始化通信队列和内存池
    queue = asyncio.Queue()
    memory_pool = {sym: {} for sym in symbol_list}
    completion_event = asyncio.Event()

    # 时间戳计算（修复：显式将输入的字符串解析为东八区，再获取真正的UTC时间戳）
    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    target_time_ms = int(pd.to_datetime(target_time_str).tz_localize('Asia/Shanghai').timestamp() * 1000)
    start_time_ms = target_time_ms - (days * 24 * 60 * 60 * 1000)
    current_time_ms = exchange.milliseconds()

    logger.info(f"[Orchestrator] 任务初始化: 目标时间 {target_time_str} ({target_time_ms}) 当前时间 {exchange.iso8601(current_time_ms)} ({current_time_ms}) 追赶范围 {days} 天")

    # 1. 启动大脑 (Consumer)
    processor_task = asyncio.create_task(
        data_processor(queue, symbol_list, target_time_ms, completion_event, memory_pool)
    )

    # 2. 启动历史追赶 (Producers - Phase 1)
    history_tasks = [
        asyncio.create_task(fetch_historical_rest(exchange, sym, timeframe, start_time_ms, queue))
        for sym in symbol_list
    ]

    # 3. 智能休眠判断 (The Sniper Window)
    sleep_time_ms = target_time_ms - 15000 - current_time_ms  # 提前 15 秒唤醒
    if sleep_time_ms > 0:
        logger.info(f"[Orchestrator] 💤 历史拉取中，系统将休眠 {sleep_time_ms / 1000:.1f} 秒，等待进入狙击窗口...")
        await asyncio.sleep(sleep_time_ms / 1000)
    else:
        logger.info("[Orchestrator] ⚠️ 目标时间已在 15 秒内或已成历史，直接进入激进狙击模式！")

    # 4. 双引擎点火 (Producers - Phase 2: WS & REST)
    ws_task = asyncio.create_task(fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url))
    rest_polling_task = asyncio.create_task(fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue))

    # 5. 挂起主程序，死等发令枪响
    await completion_event.wait()

    # 6. 枪响！瞬间拔除所有正在运行的拉取任务 (无情 Cancel)
    for task in history_tasks:
        if not task.done():
            task.cancel()
    ws_task.cancel()
    rest_polling_task.cancel()
    processor_task.cancel()
    await exchange.close()

    # 7. 整理内存字典，组装成 DataFrames 返回
    final_dfs = {}
    for sym in symbol_list:
        # 将 Dict 转换为按时间排序的 DataFrame
        kline_list = list(memory_pool[sym].values())
        kline_list.sort(key=lambda x: x[0])
        df = pd.DataFrame(kline_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # 截取所需的数据范围
        df = df[(df['timestamp'] >= start_time_ms) & (df['timestamp'] <= target_time_ms)].reset_index(drop=True)
        # 增加北京时间方便查看
        df['datetime_bj'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Shanghai')
        final_dfs[sym] = df

    # 8. 触发后台异步落盘 (不阻塞 `return`)
    asyncio.create_task(save_to_csv_background(final_dfs))

    logger.info(f"[Orchestrator] 🎉 任务圆满结束！瞬间返回 {len(final_dfs)} 个币种的数据结构。")
    return final_dfs


# =====================================================================
# 🚀 启动入口
# =====================================================================
if __name__ == "__main__":
    # 使用 Windows 系统运行 asyncio 时需加此防报错策略
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


    async def main():
        symbol_list = ["BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"]
        timeframe = "1m"
        days = 3

        # 获取当前时间，向后推延 2 分钟，并格式化为指定字符串
        target_time_str = (datetime.now() + timedelta(minutes=2)).strftime("%Y-%m-%d %H:%M:%S")

        result_map = await core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str)

        # [修改点2]: 增加安全判断，防止网络不通返回空字典时引发 KeyError 崩溃
        if result_map and "BTC/USDT:USDT" in result_map:
            print("\n最终产出预览 (BTC):")
            print(result_map["BTC/USDT:USDT"].tail())
        else:
            print("\n[系统提示] 未获取到完整数据，请检查上方的网络或代理报错日志。")


    asyncio.run(main())