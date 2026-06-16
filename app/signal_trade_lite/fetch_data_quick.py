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


def _format_bj_time(ts_ms):
    """辅助函数：将时间戳统一格式化为北京时间字符串，消除时区歧义"""
    return pd.to_datetime(ts_ms, unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')


# =====================================================================
# 🗄️ 模块一：缓存与存储引擎 (Cache & Storage Manager) [已修复]
# =====================================================================
def load_local_cache(symbol_list, start_time_ms, timeframe_ms, cache_dir="data", log_prefix=""):
    """
    智能加载本地缓存数据。
    如果本地缓存涵盖了所需历史的起点，则只需拉取缺失的增量数据；
    否则，标记为全量回溯。
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

                # [修复点1] 极致性能优化：抛弃 iterrows，使用 values.tolist() 提速百倍
                records = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].values.tolist()
                for row in records:
                    ts = int(row[0])
                    # row[0]是float，需强转为int作为字典key，后半部使用切片拼接
                    memory_pool[sym][ts] = [ts] + row[1:]

                # 计算该缓存的理论期望行数与实际行数，用于甄别“瑞士奶酪”断层数据
                expected_rows = (max_ts - min_ts) // timeframe_ms + 1
                actual_rows = len(records)

                # 智能判断需要追赶的起点
                if min_ts <= start_time_ms and actual_rows >= expected_rows:
                    # 缓存已覆盖起点，且中间绝对无断层，将追赶起点设为 max_ts（重叠拉取最后一根，确保其已彻底闭合）
                    fetch_since_map[sym] = max_ts
                    hits += 1
                    latest_times[sym.split('/')[0]] = _format_bj_time(max_ts)
                else:
                    # 缓存缺失早期数据，或中间存在缺失(空洞)，强制标记为从头拉取补齐
                    fetch_since_map[sym] = start_time_ms
                    misses += 1
            except Exception as e:
                logger.warning(f"{log_prefix} [CACHE] ⚠️ 读取 {sym} 缓存失败: {e}")
        else:
            misses += 1

    cost = time.time() - t0
    # [改造点] O(N) 级别刷屏日志替换为单条高密度汇总聚合，并加入每个币最新的时间
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
            # [终极修复] 原子性写入保护：先写临时文件，完成后系统级瞬间覆盖，防止程序被强杀导致数月缓存归零损坏
            df.to_csv(temp_path, index=False)
            total_io_size += os.path.getsize(temp_path)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"{log_prefix} [DISK] ❌ 异步保存 {symbol} 失败: {e}")
            # 如果出错尝试清理临时文件
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass

    cost = time.time() - t0
    # [改造点] 暴露落盘的核心 IO 指标，方便告警监控
    logger.info(
        f"{log_prefix} [DISK] 💾 独立守护落盘完毕 | files={len(full_dfs_for_cache)} io_size={total_io_size / (1024 * 1024):.2f}MB write_cost={cost:.3f}s")


def dispatch_background_save(full_dfs_for_cache, cache_dir="data", log_prefix=""):
    """
    启动独立守护线程进行全量覆盖覆写，彻底解放主线程
    """
    save_thread = threading.Thread(
        target=_save_csv_sync_fast,
        args=(full_dfs_for_cache, cache_dir, log_prefix),
        # 设置为非守护线程，确保主程序体面等待落盘完成才彻底退出，防止文件损坏
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

                # [防死锁防线] 物理时钟强判：针对流动性弱的冷门币种，如果收到了目标K线且现实时间已跨越寿命终点 10 秒，强制闭合！
                condition_time_force = (ts == target_time_ms and current_sys_ms > target_time_ms + timeframe_ms + 10000)

                if condition_ws_closed or condition_next_candle or condition_time_force:
                    reached_symbols.add(symbol)
                    processor_stats["winners"][source] += 1

                    # 记录该币种闭合时的精确系统时间与方式
                    close_sys_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    # 给强判脱困的币种打个 ⏱️ 标记，方便运维追踪复盘
                    mark = "⏱️强判" if condition_time_force and not (
                            condition_ws_closed or condition_next_candle) else ""
                    processor_stats["details"][symbol.split(':')[0]] = f"{source}{mark}({close_sys_time})"

                    # [改造点] 取消单条频刷日志，保持静默，等待主协程汇报
                    if len(reached_symbols) == len(symbol_list):
                        processor_stats["throughput"] = stats
                        completion_event.set()
                        queue.task_done()
                        break
            queue.task_done()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        # [防静默崩溃] 捕获所有字典异常，强制释放事件锁，绝不让主线程无限死等卡住
        logger.error(f"[Processor] ❌ 数据大脑发生致命异常: {e}，正在强制解除全局阻塞锁...")
        completion_event.set()


# =====================================================================
# 🚜 模块三：数据搬运工 (Producers) [已引入 REST 核心保护与WS动态映射]
# =====================================================================
async def fetch_historical_rest(exchange, symbol, timeframe, since_ms, queue, tracker=None):
    start_t = time.time()
    limit = 1000
    curr_since = since_ms
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
    # [改造点] 利用 tracker 聚合日志，消灭 O(N) 冗余
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
    # [核心修复] 动态构建 WS 的 raw_symbol (如 ETHUSDC) 到 target_symbol (如 ETH/USDC:USDC) 的映射字典
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

                                # 通过字典动态匹配，彻底告别硬编码！完美兼容 USDC/USDT 等所有计价方式
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


async def check_time_sync(exchange, log_prefix=""):
    """
    检测本地物理机时间与 Binance 服务器时间的精准偏差与网络往返延迟 (RTT)。
    采用标准 NTP 估计算法剔除网络传输误差。
    """
    try:
        # 发起请求前的本地时间
        t0 = time.time() * 1000

        # 异步获取币安服务器时间
        server_time = await exchange.fetch_time()

        # 收到响应后的本地时间
        t1 = time.time() * 1000

        # 1. RTT (Round Trip Time): 网络往返总延迟
        rtt = t1 - t0

        # 2. 精准偏差 (Offset): 假设上下行网络是对称的，计算出请求到达币安的那一刻，我们本地是几点，然后与币安时间做差集
        local_time_at_server = t0 + (rtt / 2)
        offset = server_time - local_time_at_server

        # 判断时钟快慢 (为了日志直观，我们将语义转换为：本地机器相对于服务器是快了还是慢了)
        status = "落后" if offset > 0 else "超前"

        logger.info(
            f"{log_prefix} [PING] ⏱️ 时钟与网络基准测试 | RTT延迟: {rtt:.2f}ms | 本地时钟{status}服务器: {abs(offset):.2f}ms")

        # 强预警机制：如果偏差超过 500ms，API 极易拒绝签名
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
    # [改造点] 生成全局 TraceID，后续用于链路绑定
    run_id = f"T-{uuid.uuid4().hex[:4].upper()}"
    log_prefix = f"[{run_id}]"

    exchange = ccxt.binance({
        'enableRateLimit': True, 'options': {'defaultType': 'swap'},
        'aiohttp_proxy': proxy_url, 'proxies': {'http': proxy_url, 'https': proxy_url}
    })

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
        # [改造点] 统计字典，用于提取监控面数据
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

        # 阻塞等待历史数据拉取完成，保证不遗漏补充数据
        await asyncio.gather(*history_tasks, return_exceptions=True)

        # 4. [改造点1] 战术休眠第一阶段：提前一分钟休眠至目标时间到来，随后立刻触发二次追赶释放压力
        sleep_to_target = target_time_ms - exchange.milliseconds()
        if sleep_to_target > 0:
            logger.info(
                f"{log_prefix} [SYNC] 💤 进入一阶段战术休眠 | sleep={sleep_to_target / 1000:.1f}s next_action={_format_bj_time(target_time_ms)}")
            await asyncio.sleep(sleep_to_target / 1000)

        hist_tracker_2 = {'done': 0, 'total': len(symbol_list), 'max_cost': 0, 'fetched_rows': 0, 'phase': 'HIST-2',
                          'log_prefix': log_prefix, 'latest_ts': 0}
        gap_tasks = []
        for sym in symbol_list:
            # 动态算出缺口起点，避免拉取重复数据
            gap_start_ms = max(memory_pool[sym].keys()) if memory_pool[sym] else fetch_since_map[sym]
            gap_tasks.append(asyncio.create_task(
                fetch_historical_rest(exchange, sym, timeframe, gap_start_ms, queue, hist_tracker_2)
            ))

        # 阻塞等待第二阶段缺口拉取完成
        await asyncio.gather(*gap_tasks, return_exceptions=True)
        history_tasks.extend(gap_tasks)

        # 实时双擎机制分配：WS流可以即刻点火，监听收线全过程
        engine_tasks = []
        if use_ws:
            engine_tasks.append(
                asyncio.create_task(fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url, log_prefix=log_prefix)))

        # [改造点3] 战术休眠第二阶段：死等最后 5 秒，再瞬间点爆无延迟脉冲 REST
        sleep_to_rest = target_close_time_ms - 5000 - exchange.milliseconds()
        if sleep_to_rest > 0:
            logger.info(
                f"{log_prefix} [SYNC] 💤 挂起等待收线冲刺(最后5s) | sleep={sleep_to_rest / 1000:.1f}s next_action=脉冲轮询兜底")
            await asyncio.sleep(sleep_to_rest / 1000)

        if use_rest:
            engine_tasks.append(
                asyncio.create_task(fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue)))

        # 5. [改造点] 超时检测预警：正常收线保持静默，超时滞后则 O(N) 穿透点名
        try:
            absolute_deadline_ms = target_close_time_ms + 60000
            current_ms = exchange.milliseconds()
            timeout = max(0.1, (absolute_deadline_ms - current_ms) / 1000)
            await asyncio.wait_for(completion_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            lag_symbols = set(symbol_list) - processor_stats["reached"]
            # 移除无限等待，发生超时直接向下执行发令枪强杀
            logger.warning(
                f"{log_prefix} [RACE] 🚨 触发绝对超时硬熔断(>1m) | pending={list(lag_symbols)[:3]}... 强制脱壳返回！")

        # [核心竞速指标计算]
        close_latency_ms = exchange.milliseconds() - target_close_time_ms
        tp = processor_stats.get('throughput', {})
        win = processor_stats['winners']
        det = processor_stats['details']
        logger.info(
            f"{log_prefix} [RACE] 🎯 目标全线闭合 | close_latency={close_latency_ms / 1000:.3f}s winner=(WS:{win['WS']}, REST:{win['REST_POLL']}) throughput=(ws:{tp.get('WS', 0)}, rest:{tp.get('REST_POLL', 0)}, hist:{tp.get('HIST', 0)}) details={det}")

        # 6. 发令枪响：瞬间强杀所有底层协程
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
        dispatch_background_save(full_dfs_for_cache, cache_dir="data", log_prefix=log_prefix)

        # 7. 数据清洗封装 (给用户返回精确切片的数据)
        # [终极修复] 消灭 O(N) 冗余计算：直接利用上面排好序的完美全量 DataFrame 利用 Pandas C底层向量化特性切片
        final_dfs = {}
        expected_rows = int((target_time_ms - start_time_ms) / timeframe_ms) + 1

        for sym, full_df in full_dfs_for_cache.items():
            sliced_df = full_df[
                (full_df['timestamp'] >= start_time_ms) & (full_df['timestamp'] <= target_time_ms)].reset_index(
                drop=True)
            sliced_df['datetime_bj'] = pd.to_datetime(sliced_df['timestamp'], unit='ms').dt.tz_localize(
                'UTC').dt.tz_convert('Asia/Shanghai')

            actual_rows = len(sliced_df)
            if actual_rows < expected_rows:
                logger.warning(
                    f"{log_prefix} [CHECK] ⚠️ {sym} 数据存在断缺！预期 {expected_rows} 条，实际 {actual_rows} 条 (缺失 {expected_rows - actual_rows} 条)")

            final_dfs[sym] = sliced_df

        total_pts = sum(len(df) for df in final_dfs.values())
        total_runtime = time.time() - orchestrator_start_t
        logger.info(
            f"{log_prefix} [EXIT] 🎉 主任务脱壳交付 | range=[{_format_bj_time(start_time_ms)} ~ {_format_bj_time(target_time_ms)}] total_rows={total_pts} runtime={total_runtime:.2f}s")
        return final_dfs

    finally:
        try:
            await exchange.close()
        except Exception as e:
            logger.warning(f"{log_prefix} [EXIT] 释放交易所资源时出现异常: {e}")


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
            days=1,  # 测试大天数，第二次运行将秒开
            target_time_str=target_time,
            use_ws=True,
            use_rest=True,
            proxy_url='http://127.0.0.1:7890'
        )

        symbol_list = [

            "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",

            "XRP/USDT:USDT", "BNB/USDT:USDT", "DOGE/USDT:USDT"

        ]
        target_time = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")

        print(">>> 准备调用数据引擎...")

        result_map = snipe_kline_data(
            symbol_list=symbol_list,
            timeframe="1m",
            days=1,  # 测试大天数，第二次运行将秒开
            target_time_str=target_time,
            use_ws=True,
            use_rest=True,
            proxy_url='http://127.0.0.1:7890'
        )