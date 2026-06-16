# -*- coding: utf-8 -*-
"""
:description:
基于生产者-消费者模型的高并发、极速、双引擎 K 线数据获取基建。
【升级版】增加本地缓存智能读取、独立守护线程无阻塞落盘、模块化重构。
【性能修复版】彻底消除 iterrows 主线程阻塞，重构后台落盘规避冗余 IO 与数据丢失。
【终极优化版】修复主线程切片双重计算冗余、引入临时文件原子性落盘防损坏、静默回收取消协程防止内存泄露。
【极限竞速版】二次历史前置释放、REST 脏数据严格过滤、最后 5s 无延迟脉冲式狂暴轮询。
"""

import asyncio
import uuid

import ccxt.async_support as ccxt
import pandas as pd
import time
import os
import json
import logging
import aiohttp
import threading
from datetime import datetime, timedelta

# 解除 Pandas 控制台打印限制
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# 配置基础日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("QuantSniper")


# =====================================================================
# 🗄️ 模块一：缓存与存储引擎 (Cache & Storage Manager) [已修复]
# =====================================================================
def load_local_cache(symbol_list, start_time_ms, cache_dir="data"):
    """
    智能加载本地缓存数据。
    如果本地缓存涵盖了所需历史的起点，则只需拉取缺失的增量数据；
    否则，标记为全量回溯。
    """
    memory_pool = {sym: {} for sym in symbol_list}
    fetch_since_map = {sym: start_time_ms for sym in symbol_list}

    for sym in symbol_list:
        safe_symbol = sym.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_latest.csv")

        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                if df.empty or 'timestamp' not in df.columns:
                    continue

                min_ts = int(df['timestamp'].min())
                max_ts = int(df['timestamp'].max())

                # [修复点1] 极致性能优化：抛弃 iterrows，使用 values.tolist() 提速百倍
                records = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].values.tolist()
                for row in records:
                    ts = int(row[0])
                    # row[0]是float，需强转为int作为字典key，后半部使用切片拼接
                    memory_pool[sym][ts] = [ts] + row[1:]

                # 智能判断需要追赶的起点
                if min_ts <= start_time_ms:
                    # 缓存已覆盖起点，将追赶起点设为 max_ts（重叠拉取最后一根，确保其已彻底闭合）
                    fetch_since_map[sym] = max_ts
                    # [修复点2] 时区修正：将 max_ts 转换为北京时间打印
                    max_time_bj = pd.to_datetime(max_ts, unit='ms').tz_localize('UTC').tz_convert(
                        'Asia/Shanghai').strftime('%Y-%m-%d %H:%M')
                    logger.info(f"[Cache] ♻️ {sym} 命中缓存! 起点已覆盖，将从 {max_time_bj} 开始增量追赶。")
                else:
                    # 缓存缺失早期数据，必须全量拉取
                    logger.info(f"[Cache] ⚠️ {sym} 缓存不完整 (最早的一根晚于目标起点)。将全量回溯。")
            except Exception as e:
                logger.warning(f"[Cache] 读取 {sym} 缓存失败: {e}")
        else:
            logger.info(f"[Cache] 🔍 {sym} 无本地缓存记录，准备全量初始化。")

    return memory_pool, fetch_since_map


def _save_csv_sync_fast(full_dfs_for_cache, cache_dir):
    """
    （后台线程专用）直接将内存池中的全量最新数据覆写落盘，避免二次读取
    """
    os.makedirs(cache_dir, exist_ok=True)
    for symbol, df in full_dfs_for_cache.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_latest.csv")
        temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
        try:
            # [终极修复] 原子性写入保护：先写临时文件，完成后系统级瞬间覆盖，防止程序被强杀导致数月缓存归零损坏
            df.to_csv(temp_path, index=False)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"[Storage] ❌ 异步保存 {symbol} 失败: {e}")
            # 如果出错尝试清理临时文件
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

    logger.info(f"[Storage] 💾 独立线程落盘任务圆满完成! ({len(full_dfs_for_cache)} 个币种最新状态已归档)")


def dispatch_background_save(full_dfs_for_cache, cache_dir="data"):
    """
    启动独立守护线程进行全量覆盖覆写，彻底解放主线程
    """
    save_thread = threading.Thread(
        target=_save_csv_sync_fast,
        args=(full_dfs_for_cache, cache_dir),
        # 设置为非守护线程，确保主程序体面等待落盘完成才彻底退出，防止文件损坏
        daemon=False
    )
    save_thread.start()


# =====================================================================
# 📦 模块二：核心处理器 (Consumer) [严格未修改]
# =====================================================================
async def data_processor(queue, symbol_list, target_time_ms, timeframe_ms,
                         completion_event, memory_pool):
    logger.info(f"[Processor] 🧠 数据大脑启动，监控 {len(symbol_list)} 个数据流...")
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

                logger.info(f"[Processor] 🏁 {symbol} 目标闭合 [{trigger_reason}], 功臣:[{source}] | {pending_str}")

                if len(reached_symbols) == len(symbol_list):
                    logger.info(
                        f"[Processor] 🎯 收线达成！吞吐量: 历史={stats['HIST']}, WS={stats['WS']}, 轮询={stats['REST_POLL']}")
                    completion_event.set()
                    queue.task_done()
                    break
        queue.task_done()


# =====================================================================
# 🚜 模块三：数据搬运工 (Producers) [已引入 REST 核心保护]
# =====================================================================
async def fetch_historical_rest(exchange, symbol, timeframe, since_ms, queue):
    start_t = time.time()
    limit = 1000
    curr_since = since_ms
    total_fetched = 0

    while True:
        try:
            ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit)
            if not ohlcvs: break

            total_fetched += len(ohlcvs)
            for k in ohlcvs: await queue.put((symbol, k, "HIST", False))

            curr_since = ohlcvs[-1][0] + 1
            if len(ohlcvs) < limit: break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"[History] {symbol} 拉取异常: {e}，重试中...")
            await asyncio.sleep(2)

    cost_t = time.time() - start_t
    if total_fetched > 0:
        logger.info(f"[History] 📦 {symbol} 追赶完成 | 入库 {total_fetched} 根 | 耗时 {cost_t:.2f}s")


async def fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url):
    ws_symbols = [s.replace("/", "").split(":")[0].lower() for s in symbol_list]
    stream_url = f"wss://fstream.binance.com/market/stream?streams={'/'.join([f'{s}@kline_{timeframe}' for s in ws_symbols])}"

    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(stream_url, proxy=proxy_url, heartbeat=10) as ws:
                logger.info("[WS Engine] ✅ WebSocket 通道建立，蹲守数据中...")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        if 'data' in data and 'k' in data['data']:
                            k_data = data['data']['k']
                            target_symbol = f"{data['data']['s'][:-4]}/{data['data']['s'][-4:]}:USDT"
                            kline = [int(k_data['t']), float(k_data['o']), float(k_data['h']),
                                     float(k_data['l']), float(k_data['c']), float(k_data['v'])]
                            await queue.put((target_symbol, kline, "WS", bool(k_data['x'])))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"[WS Engine] ❌ 异常断开: {e}")


async def fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue):
    try:
        while True:
            tasks = [exchange.fetch_ohlcv(sym, timeframe, limit=2) for sym in symbol_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, ohlcvs in zip(symbol_list, results):
                if isinstance(ohlcvs, Exception) or not ohlcvs: continue

                # [改造点2] 严格的数据完整性校验
                latest_ts = ohlcvs[-1][0]
                for k in ohlcvs:
                    # 只有时间戳小于最新一条的数据，才被视为前一分钟完全走完的数据
                    is_closed = (k[0] < latest_ts)
                    await queue.put((sym, k, "REST_POLL", is_closed))

            # [改造点3] 已彻底剥离 await asyncio.sleep，配合最后5秒启动机制，实现无延迟的极速脉冲轮询
    except asyncio.CancelledError:
        raise


# =====================================================================
# 🧠 模块四：中央大脑与流程编排 (Task Orchestrator) [引入极致时间调度]
# =====================================================================
def parse_time_params(exchange, timeframe, days, target_time_str):
    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    target_time_ms = int(pd.to_datetime(target_time_str).tz_localize('Asia/Shanghai').timestamp() * 1000)
    start_time_ms = target_time_ms - (days * 24 * 60 * 60 * 1000)
    target_close_time_ms = target_time_ms + timeframe_ms
    return timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms


# 注：已移除原有的 process_final_data 函数，因其会导致二次排序和 DataFrame 构建的巨大性能冗余

async def _async_core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str,
                                           use_ws, use_rest, proxy_url):
    exchange = ccxt.binance({
        'enableRateLimit': True, 'options': {'defaultType': 'swap'},
        'aiohttp_proxy': proxy_url, 'proxies': {'http': proxy_url, 'https': proxy_url}
    })

    try:
        await exchange.load_markets()

        # 1. 计算时间参数
        timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms = parse_time_params(
            exchange, timeframe, days, target_time_str)

        logger.info(
            f"[Orchestrator] 锁定目标: {target_time_str} | 将于 {exchange.iso8601(target_close_time_ms)} 圆满收线")

        # 2. 智能缓存装载 & 内存池初始化
        memory_pool, fetch_since_map = load_local_cache(symbol_list, start_time_ms)

        queue = asyncio.Queue()
        completion_event = asyncio.Event()

        # 3. 启动后台协程任务 (首波历史追赶)
        processor_task = asyncio.create_task(
            data_processor(queue, symbol_list, target_time_ms, timeframe_ms, completion_event, memory_pool)
        )

        history_tasks = [
            asyncio.create_task(fetch_historical_rest(exchange, sym, timeframe, fetch_since_map[sym], queue))
            for sym in symbol_list
        ]

        # 4. [改造点1] 战术休眠第一阶段：提前一分钟休眠至目标时间到来，随后立刻触发二次追赶释放压力
        sleep_to_target = target_time_ms - exchange.milliseconds()
        if sleep_to_target > 0:
            logger.info(f"[Orchestrator] 💤 战术休眠 {sleep_to_target / 1000:.1f}s，等待目标起始时间到达...")
            await asyncio.sleep(sleep_to_target / 1000)

        logger.info("[Orchestrator] 🔧 目标时间已现！触发【二次历史追赶】，提前获取已完全闭合的前置K线...")
        gap_tasks = []
        for sym in symbol_list:
            # 动态算出缺口起点，避免拉取重复数据
            gap_start_ms = max(memory_pool[sym].keys()) + 1 if memory_pool[sym] else fetch_since_map[sym]
            gap_tasks.append(asyncio.create_task(
                fetch_historical_rest(exchange, sym, timeframe, gap_start_ms, queue)
            ))
        history_tasks.extend(gap_tasks)

        # 实时双擎机制分配：WS流可以即刻点火，监听收线全过程
        engine_tasks = []
        if use_ws:
            engine_tasks.append(asyncio.create_task(fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url)))

        # [改造点3] 战术休眠第二阶段：死等最后 5 秒，再瞬间点爆无延迟脉冲 REST
        sleep_to_rest = target_close_time_ms - 5000 - exchange.milliseconds()
        if sleep_to_rest > 0:
            logger.info(
                f"[Orchestrator] 💤 战术休眠 {sleep_to_rest / 1000:.1f}s，等待最后 5s 开启无延迟 REST 狂暴轮询...")
            await asyncio.sleep(sleep_to_rest / 1000)

        if use_rest:
            engine_tasks.append(
                asyncio.create_task(fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue)))

        # 5. 阻塞等待：等待核心大脑发出完成信号
        await completion_event.wait()

        # 6. 发令枪响：瞬间强杀所有底层协程
        logger.info("[Orchestrator] 🔫 目标全线圆满！发令枪响，瞬间强杀(Cancel)所有底层拉取协程...")
        all_tasks = history_tasks + engine_tasks + [processor_task]
        for task in all_tasks:
            if not task.done(): task.cancel()

        # [终极修复] 静默回收所有被 Cancel 的任务，防止报 Task was destroyed but it is pending! 的内存/网络连接泄露警告
        await asyncio.gather(*all_tasks, return_exceptions=True)

        # 构建用于后台落盘的全量数据 (Source of Truth)，确保缓存不会丢失切片外的边缘数据
        MAX_CACHE_ROWS = 525600  # 最大保存1年的数据(基于1m周期: 365*24*60)，防止无限膨胀导致OOM
        full_dfs_for_cache = {}
        for sym in symbol_list:
            all_klines = list(memory_pool[sym].values())
            all_klines.sort(key=lambda x: x[0])

            if len(all_klines) > MAX_CACHE_ROWS:
                all_klines = all_klines[-MAX_CACHE_ROWS:]

            full_dfs_for_cache[sym] = pd.DataFrame(all_klines,
                                                   columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # 无阻塞落盘：直接把全量数据甩给后台线程
        dispatch_background_save(full_dfs_for_cache)

        # 7. 数据清洗封装 (给用户返回精确切片的数据)
        # [终极修复] 消灭 O(N) 冗余计算：直接利用上面排好序的完美全量 DataFrame 利用 Pandas C底层向量化特性切片
        final_dfs = {}
        for sym, full_df in full_dfs_for_cache.items():
            sliced_df = full_df[
                (full_df['timestamp'] >= start_time_ms) & (full_df['timestamp'] <= target_time_ms)].reset_index(
                drop=True)
            sliced_df['datetime_bj'] = pd.to_datetime(sliced_df['timestamp'], unit='ms').dt.tz_localize(
                'UTC').dt.tz_convert('Asia/Shanghai')
            final_dfs[sym] = sliced_df

        total_pts = sum(len(df) for df in final_dfs.values())
        logger.info(f"[Orchestrator] 🎉 任务结束! 瞬间返回 {total_pts} 条精准数据。(落盘任务已脱壳交由后台独立线程运行)")
        return final_dfs

    finally:
        await exchange.close()


# =====================================================================
# 🌟 对外暴露的公共 API [严格未修改]
# =====================================================================
def snipe_kline_data(symbol_list, timeframe, days, target_time_str,
                     use_ws=True, use_rest=True, proxy_url='http://127.0.0.1:7890'):
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        raise RuntimeError("检测到已存在运行中的异步事件循环。\n请在顶部执行：import nest_asyncio; nest_asyncio.apply()")

    return asyncio.run(
        _async_core_sniping_orchestrator(
            symbol_list, timeframe, days, target_time_str, use_ws, use_rest, proxy_url
        )
    )


# =====================================================================
# 🚀 启动入口 [严格未修改]
# =====================================================================
if __name__ == "__main__":
    symbols = ["BTC/USDT:USDT", "ETH/USDT:USDT", "BNB/USDT:USDT"]
    target_time = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")

    print(">>> 准备调用数据引擎...")

    result_map = snipe_kline_data(
        symbol_list=symbols,
        timeframe="1m",
        days=5,  # 测试大天数，第二次运行将秒开
        target_time_str=target_time,
        use_ws=True,
        use_rest=True,
        proxy_url='http://127.0.0.1:7890'
    )

    print("\n>>> 调用完毕，主线程继续执行！由于采用后台线程保存，你会立刻看到这行字。")

    if result_map and "BTC/USDT:USDT" in result_map:
        print("\n最终产出预览 (BTC):")
        print(result_map["BTC/USDT:USDT"].tail())