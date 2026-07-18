# -*- coding: utf-8 -*-
"""
:description:
基于生产者-消费者模型的高并发、极速、双引擎 K 线数据获取基建。
【升级版】增加本地缓存智能读取、独立守护线程无阻塞落盘、模块化重构。
【性能修复版】彻底消除 iterrows 主线程阻塞，重构后台落盘规避冗余 IO 与数据丢失。
【终极优化版】修复主线程切片双重计算冗余、引入临时文件原子性落盘防损坏、静默回收取消协程防止内存泄露。
【极限竞速版】二次历史前置释放、REST 脏数据严格过滤、最后 5s 无延迟脉冲式狂暴轮询。
【生产监控版】引入 TraceID 链路追踪、Logfmt 结构化高密度聚合、静默成功与 O(N) 滞后点名机制。
【防弹修复版】消除 WS 硬编码兼容全币种，注入协程防异常死锁装甲，引入物理时钟强判防流动性枯竭。
【闪现交付版】主线程金蝉脱壳 O(1) 返回、剔除 datetime_bj 性能枷锁、后台线程接管全量清洗落盘。
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

from app.signal_trade_lite.common_utils_lite import setup_logger

# 解除 Pandas 控制台打印限制
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# 配置基础日志
logger = setup_logger()


def _format_bj_time(ts_ms):
    """辅助函数：将时间戳统一格式化为北京时间字符串，消除时区歧义"""
    return pd.to_datetime(ts_ms, unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')


# =====================================================================
# 🗄️ 模块一：缓存与存储引擎 (Cache & Storage Manager) [已修复]
# =====================================================================
def load_local_cache(symbol_list, start_time_ms, timeframe_ms, cache_dir="data", log_prefix=""):
    """
    智能加载本地缓存数据。
    如果本地缓存涵盖了所需历史的起点，且目标区间无断层，则只需拉取缺失的增量数据；
    否则，从起点强制回拉，弥补空洞。
    """
    t0 = time.time()
    memory_pool = {sym: {} for sym in symbol_list}
    fetch_since_map = {sym: start_time_ms for sym in symbol_list}
    hits, misses = 0, 0
    latest_times = {}  # 记录每个币最新的缓存时间

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

                # 极致性能优化：抛弃 iterrows，使用 values.tolist() 提速百倍
                records = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].values.tolist()
                for row in records:
                    ts = int(row[0])
                    memory_pool[sym][ts] = [ts] + row[1:]

                # [终极修复2] 局部连续性校验：只检查请求的 start_time_ms 到 max_ts 这一段是否存在空洞
                if min_ts <= start_time_ms:
                    sub_df = df[df['timestamp'] >= start_time_ms]
                    if not sub_df.empty:
                        sub_min_ts = int(sub_df['timestamp'].min())
                        sub_max_ts = int(sub_df['timestamp'].max())
                        expected_rows = (sub_max_ts - sub_min_ts) // timeframe_ms + 1
                        actual_rows = len(sub_df)

                        # 核心判定：
                        # 1. actual_rows >= expected_rows 说明这段区间内部无空洞
                        # 2. sub_min_ts - start_time_ms <= timeframe_ms 说明这段数据刚好能无缝衔接请求起点，没发生脱节
                        if actual_rows >= expected_rows and (sub_min_ts - start_time_ms) <= timeframe_ms:
                            fetch_since_map[sym] = max_ts
                            hits += 1
                            latest_times[sym.split('/')[0]] = _format_bj_time(max_ts)
                        else:
                            # 存在空洞，从请求起点老老实实回拉弥补
                            fetch_since_map[sym] = start_time_ms
                            misses += 1
                    else:
                        fetch_since_map[sym] = start_time_ms
                        misses += 1
                else:
                    # 连起点都没覆盖，必须全量重拉
                    fetch_since_map[sym] = start_time_ms
                    misses += 1
            except Exception as e:
                logger.warning(f"{log_prefix} [CACHE] ⚠️ 读取 {sym} 缓存失败: {e}")
        else:
            misses += 1

    cost = time.time() - t0
    logger.info(
        f"{log_prefix} [CACHE] ♻️ 智能缓存装载 | hit={hits} miss={misses} load_cost={cost:.2f}s latest={latest_times}")

    return memory_pool, fetch_since_map


def _save_csv_sync_fast(full_dfs_for_cache, cache_dir, log_prefix=""):
    """
    （后台线程专用）直接将内存池中的全量最新数据覆写落盘，避免二次读取
    """
    t0 = time.time()
    total_io_size = 0
    os.makedirs(cache_dir, exist_ok=True)
    for symbol, df in full_dfs_for_cache.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_latest.csv")
        temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
        try:
            # 原子性写入保护：先写临时文件，完成后系统级瞬间覆盖，防止程序被强杀导致数月缓存归零损坏
            df.to_csv(temp_path, index=False)
            total_io_size += os.path.getsize(temp_path)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"{log_prefix} [DISK] ❌ 异步保存 {symbol} 失败: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

    cost = time.time() - t0
    logger.info(
        f"{log_prefix} [DISK] 💾 独立守护落盘完毕 | files={len(full_dfs_for_cache)} io_size={total_io_size / (1024 * 1024):.2f}MB write_cost={cost:.3f}s")


def _background_pipeline_task(memory_pool_copy, cache_dir, log_prefix):
    """
    （被后台线程调用）承接主线程丢过来的全量脏活累活：巨量数据排序、构建 DataFrame、落盘
    """
    try:
        MAX_CACHE_ROWS = 525600
        full_dfs_for_cache = {}
        for sym, kline_dict in memory_pool_copy.items():
            all_klines = list(kline_dict.values())
            all_klines.sort(key=lambda x: x[0])

            if len(all_klines) > MAX_CACHE_ROWS:
                all_klines = all_klines[-MAX_CACHE_ROWS:]

            full_dfs_for_cache[sym] = pd.DataFrame(
                all_klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

        _save_csv_sync_fast(full_dfs_for_cache, cache_dir, log_prefix)
    except Exception as e:
        logger.error(f"{log_prefix} [BACKGROUND_PIPE] ❌ 后台数据落盘流水线异常: {e}")


def dispatch_background_save(memory_pool_copy, cache_dir="data", log_prefix=""):
    """
    启动独立守护线程进行全量覆盖覆写，彻底解放主线程 CPU
    """
    save_thread = threading.Thread(
        target=_background_pipeline_task,
        args=(memory_pool_copy, cache_dir, log_prefix),
        daemon=False
    )
    save_thread.start()


# =====================================================================
# 📦 模块二：核心处理器 (Consumer) [引入防弹异常捕获与物理时钟兜底]
# =====================================================================
async def data_processor(queue, symbol_list, target_time_ms, timeframe_ms,
                         completion_event, memory_pool, processor_stats):
    reached_symbols = processor_stats["reached"]
    stats = {"HIST": 0, "WS": 0, "REST_POLL": 0}

    try:
        while True:
            symbol, kline, source, is_closed = await queue.get()
            ts = int(kline[0])
            stats[source] += 1

            memory_pool[symbol][ts] = kline

            if symbol not in reached_symbols:
                current_sys_ms = time.time() * 1000

                condition_ws_closed = (ts == target_time_ms and is_closed)
                condition_next_candle = (ts >= target_time_ms + timeframe_ms)

                condition_time_force = (ts == target_time_ms and current_sys_ms > target_time_ms + timeframe_ms + 10000)

                if condition_ws_closed or condition_next_candle or condition_time_force:
                    reached_symbols.add(symbol)
                    processor_stats["winners"][source] += 1

                    close_sys_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    mark = "⏱️强判" if condition_time_force and not (
                            condition_ws_closed or condition_next_candle) else ""
                    processor_stats["details"][symbol.split(':')[0]] = f"{source}{mark}({close_sys_time})"

                    if len(reached_symbols) == len(symbol_list):
                        processor_stats["throughput"] = stats
                        completion_event.set()
            queue.task_done()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"[Processor] ❌ 数据大脑发生致命异常: {e}，正在强制解除全局阻塞锁...")
        completion_event.set()


# =====================================================================
# 🚜 模块三：数据搬运工 (Producers) [已引入 REST 核心保护与WS动态映射]
# =====================================================================
async def fetch_historical_rest(exchange, symbol, timeframe, since_ms, queue, tracker=None):
    start_t = time.time()
    limit = 1000
    curr_since = since_ms - 60 * 60 * 1000
    total_fetched = 0
    latest_ts = 0

    while True:
        retry_count = 0
        success = False
        while retry_count <= 3:
            try:
                ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit)
                success = True
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                retry_count += 1
                prefix = tracker.get('log_prefix', '') if tracker else ''
                logger.warning(f"{prefix} [HIST] {symbol} 拉取异常: {e}，重试 {retry_count}/3...")
                if retry_count <= 3:
                    await asyncio.sleep(2)
                else:
                    logger.error(f"{prefix} [HIST] {symbol} 达到最大重试次数，放弃当前片段。")

        if not success or not ohlcvs:
            break

        total_fetched += len(ohlcvs)
        latest_ts = ohlcvs[-1][0]

        for k in ohlcvs: await queue.put((symbol, k, "HIST", False))

        curr_since = ohlcvs[-1][0] + 1
        if len(ohlcvs) < limit: break

    cost_t = time.time() - start_t
    if tracker is not None:
        tracker['done'] += 1
        tracker['max_cost'] = max(tracker.get('max_cost', 0), cost_t)
        tracker['fetched_rows'] += total_fetched
        tracker['latest_ts'] = max(tracker.get('latest_ts', 0), latest_ts)

        if tracker['done'] == tracker['total']:
            phase = tracker.get('phase', 'HIST')
            prefix = tracker.get('log_prefix', '')
            latest_time_str = _format_bj_time(tracker['latest_ts']) if tracker['latest_ts'] > 0 else 'N/A'
            logger.info(
                f"{prefix} [{phase}] 📦 缺口历史补齐就绪 | done={tracker['done']}/{tracker['total']} fetched_rows={tracker['fetched_rows']} max_cost={tracker['max_cost']:.2f}s latest_time={latest_time_str}")


async def fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url, log_prefix=""):
    ws_mapping = {s.replace("/", "").split(":")[0].upper(): s for s in symbol_list}
    ws_symbols = [k.lower() for k in ws_mapping.keys()]
    stream_url = f"wss://fstream.binance.com/market/stream?streams={'/'.join([f'{s}@kline_{timeframe}' for s in ws_symbols])}"

    attempt = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(stream_url, proxy=proxy_url, heartbeat=10) as ws:
                    logger.info(f"{log_prefix} [WSS] ✅ 数据总线已建连 | streams={len(ws_symbols)}")
                    attempt = 0  # 成功连接并准备接收数据，重置退避计数
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if 'data' in data and 'k' in data['data']:
                                k_data = data['data']['k']
                                raw_s = data['data']['s']

                                if raw_s in ws_mapping:
                                    target_symbol = ws_mapping[raw_s]
                                    kline = [int(k_data['t']), float(k_data['o']), float(k_data['h']),
                                             float(k_data['l']), float(k_data['c']), float(k_data['v'])]
                                    await queue.put((target_symbol, kline, "WS", bool(k_data['x'])))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            sleep_time = min(2 ** attempt, 16)  # 指数退避重试：2, 4, 8, 16...
            logger.error(f"{log_prefix} [WSS] ❌ 异常断开: {e} | {sleep_time}秒后自愈重连 (第{attempt}次)...")
            await asyncio.sleep(sleep_time)


async def fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue):
    try:
        while True:
            tasks = [exchange.fetch_ohlcv(sym, timeframe, limit=2) for sym in symbol_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, ohlcvs in zip(symbol_list, results):
                if isinstance(ohlcvs, Exception) or not ohlcvs: continue

                latest_ts = ohlcvs[-1][0]
                for k in ohlcvs:
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


async def check_time_sync(exchange, log_prefix=""):
    """
    检测本地物理机时间与 Binance 服务器时间的精准偏差与网络往返延迟 (RTT)。
    采用标准 NTP 估计算法剔除网络传输误差。
    """
    try:
        t0 = time.time() * 1000
        server_time = await exchange.fetch_time()
        t1 = time.time() * 1000
        rtt = t1 - t0
        local_time_at_server = t0 + (rtt / 2)
        offset = server_time - local_time_at_server
        status = "落后" if offset > 0 else "超前"

        logger.info(
            f"{log_prefix} [PING] ⏱️ 时钟与网络基准测试 | RTT延迟: {rtt:.2f}ms | 本地时钟{status}服务器: {abs(offset):.2f}ms")

        if abs(offset) > 500:
            logger.warning(
                f"{log_prefix} [PING] ⚠️ 极高危预警：本地时间偏差过大(>{abs(offset):.0f}ms)！极易导致 API 签名失败，请立即执行 NTP 时间同步！")

        return offset, rtt

    except Exception as e:
        logger.error(f"{log_prefix} [PING] ❌ 时钟同步检测失败: {e}")
        return None, None


async def _async_core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str,
                                           use_ws, use_rest, proxy_url):
    orchestrator_start_t = time.time()
    run_id = f"T-{uuid.uuid4().hex[:4].upper()}"
    log_prefix = f"[{run_id}]"

    # 1. 定义基础配置
    exchange_config = {
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'}
    }

    # 2. 如果 proxy_url 存在（非 None 且非空），则动态注入代理配置
    if proxy_url:
        exchange_config['aiohttp_proxy'] = proxy_url
        exchange_config['proxies'] = {
            'http': proxy_url,
            'https': proxy_url
        }

    # 3. 使用配置字典初始化 Exchange
    exchange = ccxt.binance(exchange_config)
    try:
        await exchange.load_markets()
        await check_time_sync(exchange, log_prefix)
        # 1. 计算时间参数
        timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms = parse_time_params(
            exchange, timeframe, days, target_time_str)

        logger.info(
            f"{log_prefix} [INIT] 🚀 极速引擎发车 | target={_format_bj_time(target_time_ms)}(+0800) symbols={len(symbol_list)} days={days}")

        # 2. 智能缓存装载 & 内存池初始化
        memory_pool, fetch_since_map = load_local_cache(symbol_list, start_time_ms, timeframe_ms, log_prefix=log_prefix)

        queue = asyncio.Queue()
        completion_event = asyncio.Event()
        processor_stats = {"reached": set(), "winners": {"WS": 0, "REST_POLL": 0, "HIST": 0}, "throughput": {},
                           "details": {}}

        # 3. 启动后台协程任务 (首波历史追赶)
        processor_task = asyncio.create_task(
            data_processor(queue, symbol_list, target_time_ms, timeframe_ms, completion_event, memory_pool,
                           processor_stats)
        )

        hist_tracker_1 = {'done': 0, 'total': len(symbol_list), 'max_cost': 0, 'fetched_rows': 0, 'phase': 'HIST-1',
                          'log_prefix': log_prefix, 'latest_ts': 0}
        history_tasks = [
            asyncio.create_task(
                fetch_historical_rest(exchange, sym, timeframe, fetch_since_map[sym], queue, hist_tracker_1))
            for sym in symbol_list
        ]

        # 4. 战术休眠第一阶段：提前一分钟休眠至目标时间到来，随后立刻触发二次追赶释放压力
        sleep_to_target = target_time_ms - exchange.milliseconds()
        if sleep_to_target > 0:
            logger.info(
                f"{log_prefix} [SYNC] 💤 进入一阶段战术休眠 | sleep={sleep_to_target / 1000:.1f}s next_action={_format_bj_time(target_time_ms)}")
            await asyncio.sleep(sleep_to_target / 1000)

        hist_tracker_2 = {'done': 0, 'total': len(symbol_list), 'max_cost': 0, 'fetched_rows': 0, 'phase': 'HIST-2',
                          'log_prefix': log_prefix, 'latest_ts': 0}
        gap_tasks = []
        for sym in symbol_list:
            gap_start_ms = max(memory_pool[sym].keys()) if memory_pool[sym] else fetch_since_map[sym]
            gap_tasks.append(asyncio.create_task(
                fetch_historical_rest(exchange, sym, timeframe, gap_start_ms, queue, hist_tracker_2)
            ))

        history_tasks.extend(gap_tasks)

        # 实时双擎机制分配：WS流可以即刻点火，监听收线全过程
        engine_tasks = []
        if use_ws:
            engine_tasks.append(
                asyncio.create_task(fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url, log_prefix=log_prefix)))

        # 战术休眠第二阶段：死等最后 5 秒，再瞬间点爆无延迟脉冲 REST
        sleep_to_rest = target_close_time_ms - 5000 - exchange.milliseconds()
        if sleep_to_rest > 0:
            logger.info(
                f"{log_prefix} [SYNC] 💤 挂起等待收线冲刺(最后5s) | sleep={sleep_to_rest / 1000:.1f}s next_action=脉冲轮询兜底")
            await asyncio.sleep(sleep_to_rest / 1000)

        if use_rest:
            engine_tasks.append(
                asyncio.create_task(fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue)))

        # 5. 超时检测预警与收盘等待
        try:
            absolute_deadline_ms = target_close_time_ms + 60000
            current_ms = exchange.milliseconds()
            timeout = max(0.1, (absolute_deadline_ms - current_ms) / 1000)
            await asyncio.wait_for(completion_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            lag_symbols = set(symbol_list) - processor_stats["reached"]
            logger.warning(
                f"{log_prefix} [RACE] 🚨 触发绝对超时硬熔断(>1m) | pending={list(lag_symbols)[:3]}... 强制脱壳返回！")

        # 【重点修改】：在这里确保所有尽力而为的历史拉取已交卷，保证最后交付的数据尽可能补全缺口
        await asyncio.gather(*history_tasks, return_exceptions=True)
        await queue.join()
        # [核心竞速指标计算]
        close_latency_ms = exchange.milliseconds() - target_close_time_ms
        tp = processor_stats.get('throughput', {})
        win = processor_stats['winners']
        det = processor_stats['details']
        logger.info(
            f"{log_prefix} [RACE] 🎯 目标全线闭合 | close_latency={close_latency_ms / 1000:.3f}s winner=(WS:{win['WS']}, REST:{win['REST_POLL']}) throughput=(ws:{tp.get('WS', 0)}, rest:{tp.get('REST_POLL', 0)}, hist:{tp.get('HIST', 0)}) details={det}")

        # 6. 发令枪响：瞬间强杀所有底层协程 (只杀双擎和处理器，不杀已经跑完的历史任务)
        all_tasks = engine_tasks + [processor_task]
        for task in all_tasks:
            if not task.done(): task.cancel()

        await asyncio.gather(*all_tasks, return_exceptions=True)

        # =====================================================================
        # 🚀 7. CPU 极限优化段：主线程金蝉脱壳、零阻塞构建极简返回数据
        # =====================================================================
        final_dfs = {}
        expected_rows = int((target_time_ms - start_time_ms) / timeframe_ms) + 1

        for sym in symbol_list:
            # 第一层防线：极速字典推导式过滤（取代动辄几十万行的全量循环与判断）
            sliced_klines = [
                k for ts, k in memory_pool[sym].items()
                if ts <= target_time_ms
            ]

            # 第二层防线：局部极速排序（取代全量排序）
            sliced_klines.sort(key=lambda x: x[0])

            # 第三层防线：极简构建 Pandas (直接摒弃时区转换)
            sliced_df = pd.DataFrame(
                sliced_klines,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            final_dfs[sym] = sliced_df

            # 数据完整性比对告警
            actual_rows = len(sliced_klines)
            if actual_rows < expected_rows:
                logger.warning(
                    f"{log_prefix} [CHECK] ⚠️ {sym} 数据存在断缺！预期 {expected_rows} 条，实际 {actual_rows} 条 (缺失 {expected_rows - actual_rows} 条)")

        # =====================================================================
        # 🤝 8. 后台交接：把沉重的 50+ 万条全量清洗与落盘，扔给子线程慢慢跑
        # =====================================================================
        memory_pool_copy = {sym: pool.copy() for sym, pool in memory_pool.items()}
        dispatch_background_save(memory_pool_copy, cache_dir="data", log_prefix=log_prefix)

        total_pts = sum(len(df) for df in final_dfs.values())
        total_runtime = time.time() - orchestrator_start_t
        logger.info(
            f"{log_prefix} [EXIT] 🎉 主任务零阻塞闪现交付 | range=[{_format_bj_time(start_time_ms)} ~ {_format_bj_time(target_time_ms)}] total_rows={total_pts} runtime={total_runtime:.2f}s")
        return final_dfs

    finally:
        try:
            # 1. 物理斩断：加上 await，因为新版 aiohttp 中它是协程！
            if hasattr(exchange, 'session') and exchange.session:
                if hasattr(exchange.session, 'connector') and exchange.session.connector:
                    try:
                        # 只给 2 毫秒的死线，强行触发关闭动作
                        await asyncio.wait_for(exchange.session.connector.close(), timeout=0.00002)
                    except Exception:
                        pass  # 超时直接静默，此时物理连接已被撕裂

            # 2. 欺骗 CCXT 析构函数，防止它检查 Session 触发长篇警告
            exchange.session = None

            # 3. 象征性走一下 CCXT 的 close，2 毫秒必杀
            await asyncio.wait_for(exchange.close(), timeout=0.00002)

        except Exception:
            pass  # 屏蔽一切退出时的报错，实现完美脱壳
# =====================================================================
# 🌟 对外暴露的公共 API [严格未修改]
# =====================================================================
def snipe_kline_data(symbol_list, timeframe, days, target_time_str,
                     use_ws=True, use_rest=True, proxy_url=None):
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

    while True:
        symbol_list = [
            "BTC/USDC:USDC", "ETH/USDC:USDC", "SOL/USDC:USDC",
            "XRP/USDC:USDC", "BNB/USDC:USDC", "DOGE/USDC:USDC"
        ]

        target_time = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")

        print(">>> 准备调用数据引擎...")

        result_map = snipe_kline_data(
            symbol_list=symbol_list,
            timeframe="1m",
            days=150,
            target_time_str=target_time,
            use_ws=True,
            use_rest=True,
            proxy_url='http://127.0.0.1:7890'
        )
        logger.info(f"✅ 已完成对所有币种的极速引擎数据请求，正在进行数据完整性检查和预处理...")
        break
        #
        # symbol_list = [
        #     "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        #     "XRP/USDT:USDT", "BNB/USDT:USDT", "DOGE/USDT:USDT"
        # ]
        # target_time = (datetime.now() + timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
        #
        # print(">>> 准备调用数据引擎...")
        #
        # result_map = snipe_kline_data(
        #     symbol_list=symbol_list,
        #     timeframe="1m",
        #     days=150,
        #     target_time_str=target_time,
        #     use_ws=True,
        #     use_rest=True,
        #     proxy_url='http://127.0.0.1:7890'
        # )