# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/6/13 7:13
:last_date:
    2026/6/13 7:13
:description:

"""
import ccxt
import pandas as pd
import time
import os
import logging

from app.signal_trade_lite.common_utils_lite import format_ts_to_bj, setup_logger


def fetch_binance_futures_klines(symbol, timeframe='1h', days=30, retries=5, proxies=None, cache_dir="data",
                                 logger=None):
    """
    专门获取币安 U本位永续合约 (Futures/Swap) 的 K 线 (OHLCV) 数据。
    包含错误重试、跳变修复、分页无缝拼接。底层存储强制使用 UTC 毫秒时间戳，展示层返回北京时间。

    :param symbol: 交易对，如 'BTC/USDT:USDT' 或 'BTC/USDT'
    :param timeframe: K线周期，如 '1h', '15m', '1d'
    :param days: 获取过去多少天的数据
    :param retries: 遇到网络或API报错时的最大重试次数
    :param proxies: 代理配置字典，为空则直连
    :param cache_dir: 缓存目录，默认相对路径 "data"
    :param logger: 传入的日志实例
    :return: 包含 K 线数据的 Pandas DataFrame
    """
    if logger is None:
        logger = logging.getLogger("QuantBot")  # Fallback

    # 规范化日志前缀：增加周期上下文，彻底消灭孤儿日志
    log_prefix = f"[{symbol}][{timeframe}]"

    start_time_proc = time.time()

    # 1. 动态实例化交易所配置
    exchange_params = {
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap',
        }
    }
    if proxies:
        exchange_params['proxies'] = proxies
        log_prefix += "[ProxyEnabled]"

    exchange = ccxt.binance(exchange_params)

    # 处理 symbol 格式兼容性
    if ':' not in symbol and symbol.endswith('USDT'):
        symbol = f"{symbol}:USDT"

    # 2. 缓存路径与文件初始化
    os.makedirs(cache_dir, exist_ok=True)
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    cache_file = os.path.join(cache_dir, f"{safe_symbol}_{timeframe}.csv")

    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    current_ms = exchange.milliseconds()
    requested_since = current_ms - int(days * 24 * 60 * 60 * 1000)

    since = requested_since
    cache_df = pd.DataFrame()

    # 3. 读取本地增量缓存 (底层强制统一使用 UNIX ms 整数)
    if os.path.exists(cache_file):
        try:
            cache_df = pd.read_csv(cache_file)
            if not cache_df.empty:
                # 优化点 C：对“旧版时间格式的遗留缓存”进行平稳的向下兼容，不作为错误抛出
                if cache_df['timestamp'].dtype == 'O':
                    logger.info(f"{log_prefix} 发现旧版时间格式缓存，主动丢弃并全量重建底层结构")
                    cache_df = pd.DataFrame()
                    existing_latest_time_str = "无"
                    since = requested_since
                else:
                    cache_df['timestamp'] = cache_df['timestamp'].astype('int64')
                    cache_oldest_ms = cache_df['timestamp'].iloc[0]
                    cache_latest_ms = cache_df['timestamp'].iloc[-1]

                    existing_latest_time_str = format_ts_to_bj(cache_latest_ms)

                    if cache_oldest_ms <= requested_since:
                        # 缓存已覆盖请求起点，仅做增量拉取（回退 2 个周期容错，防止最后未收线）
                        since = cache_latest_ms - timeframe_ms * 2
                    else:
                        # 缓存不够指定天数，补充早期的缺失数据（拉取起点设为请求起点）
                        since = requested_since
        except Exception as e:
            logger.warning(f"{log_prefix} 读取缓存异常: {e}，将执行全量重拉取。")
            cache_df = pd.DataFrame()
            existing_latest_time_str = "无"
    else:
        existing_latest_time_str = "无"

    # 新增：计算寻址范围跨度（天）
    api_span_days = (current_ms - since) / (1000 * 60 * 60 * 24)
    # 优化点 H：去除无意义的数字硬编码，统一任务阶段标签 [TaskStart]
    logger.info(
        f"{log_prefix} [TaskStart] 本地最新: {existing_latest_time_str} | API 寻址范围: {format_ts_to_bj(since)} -> 当前 (跨度: {api_span_days:.2f}天)")

    limit = 1000
    all_ohlcv = []
    curr_since = since
    page_count = 0  # 用于触发心跳日志

    # 4. 主干数据拉取循环
    while True:
        curr_ohlcv = []
        fetch_success = False

        for net_attempt in range(retries):
            try:
                curr_ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit)
                fetch_success = True
                break
            except ccxt.RateLimitExceeded as e:
                # 优化点 A：限频是常态抵抗机制，降级为 WARNING，防止污染报警监控
                logger.warning(f"{log_prefix} 触发币安限频 (HTTP 429)，休眠 5 秒...")
                time.sleep(5)
            except ccxt.NetworkError as e:
                logger.warning(f"{log_prefix} 网络抖动: {e}，休眠 2 秒...")
                time.sleep(2)
            except Exception as e:
                logger.warning(f"{log_prefix} 未知拉取异常: {e}，休眠 2 秒...")
                time.sleep(2)

        # 优化点 B：精确拆分 "彻底失败" 与 "空数据触底" 的语义
        if not fetch_success:
            logger.error(f"{log_prefix} 耗尽重试次数 ({retries})，API 拉取异常中断。", exc_info=True)
            break

        if not curr_ohlcv:
            logger.info(f"{log_prefix} API 返回空数据，触底正常结束分页拉取。")
            break

        all_ohlcv.extend(curr_ohlcv)
        curr_since = curr_ohlcv[-1][0] + 1  # 推进到下一毫秒
        page_count += 1

        # 优化点 G：主干长耗时任务的进度心跳日志
        if page_count % 10 == 0:
            logger.info(
                f"{log_prefix} 分页拉取中... 已获取 {page_count} 页 ({len(all_ohlcv)} 条), 当前推进至: {format_ts_to_bj(curr_since)}")

        # 核心修正：分页结束的安全标志（如果当前拉到的数量小于上限，绝对是拉到底了）
        if len(curr_ohlcv) < limit:
            break

    # 5. 合并并转正数据结构
    new_df = pd.DataFrame()
    repair_ohlcv_total_len = 0  # 为后续来源统筹做准备

    if all_ohlcv:
        new_df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    df = pd.concat([cache_df, new_df], ignore_index=True)

    if not df.empty:
        # 类型规范与去重
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        df[numeric_cols] = df[numeric_cols].astype(float)
        df['timestamp'] = df['timestamp'].astype('int64')
        df = df.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp').reset_index(drop=True)

        # 6. 数据完整性与跳变智能修复（抛弃全局清空，改为定向修补）
        time_diffs = df['timestamp'].diff()
        gap_mask = time_diffs > timeframe_ms

        if gap_mask.any():
            gap_indices = df.index[gap_mask]
            # 优化点 D：前置汇总日志
            logger.warning(f"{log_prefix} 发现 {len(gap_indices)} 处历史时间跳变(缺口)，开始智能修补...")

            repair_ohlcv = []
            physical_missing_count = 0

            for idx in gap_indices:
                gap_start_ms = df.loc[idx - 1, 'timestamp']
                gap_end_ms = df.loc[idx, 'timestamp']

                # 优化点 D：将循环内的明细降级为 DEBUG
                logger.debug(
                    f"{log_prefix} 尝试修补区间: {format_ts_to_bj(gap_start_ms)} -> {format_ts_to_bj(gap_end_ms)}")

                # 尝试拉取缝隙中间的数据
                repair_since = gap_start_ms + timeframe_ms
                while repair_since < gap_end_ms:
                    repair_fetch = None
                    try:
                        repair_fetch = exchange.fetch_ohlcv(symbol, timeframe, since=repair_since, limit=limit)
                    except Exception as e:
                        logger.warning(f"{log_prefix} 修补数据时发生异常: {e}")

                    if not repair_fetch:
                        logger.debug(
                            f"{log_prefix} 修补拉取无新数据，区间: {format_ts_to_bj(repair_since)} - 确认系交易所物理缺失，予以保留。")
                        physical_missing_count += 1
                        break

                    # 过滤掉不属于缺口内的数据
                    valid_repair = [k for k in repair_fetch if k[0] < gap_end_ms]
                    if not valid_repair:
                        break

                    repair_ohlcv.extend(valid_repair)
                    repair_since = valid_repair[-1][0] + 1

                    if len(repair_fetch) < limit:
                        break

            # 如果修复补拉成功，再次合并清洗
            if repair_ohlcv:
                repair_ohlcv_total_len = len(repair_ohlcv)
                repair_df = pd.DataFrame(repair_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                repair_df[numeric_cols] = repair_df[numeric_cols].astype(float)
                repair_df['timestamp'] = repair_df['timestamp'].astype('int64')
                df = pd.concat([df, repair_df], ignore_index=True)
                df = df.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp').reset_index(
                    drop=True)

            # 优化点 D：完成后的总结性日志，彻底告别刷屏
            logger.info(
                f"{log_prefix} 数据修补完成: 共尝试 {len(gap_indices)} 处缺口, 成功补齐 {repair_ohlcv_total_len} 条数据, 确认物理缺失 {physical_missing_count} 处。")

        # 7. 原子写入 (安全落盘机制，防止断电写残 CSV)
        try:
            tmp_file = cache_file + ".tmp"
            df.to_csv(tmp_file, index=False)
            os.replace(tmp_file, cache_file)
        except Exception as e:
            logger.error(f"{log_prefix} 缓存文件原子写入失败: {e}", exc_info=True)

    # 8. 截取请求的天数范围，并在内存展现层新增带有北京时间的 Datetime 对象字段
    final_df = pd.DataFrame()
    if not df.empty:
        final_df = df[df['timestamp'] >= requested_since].reset_index(drop=True)
        # ⚠️ 这里是最终展现层：不覆盖原有的 timestamp 毫秒字段，新增 datetime_bj 字段存储东八区时间
        final_df['datetime_bj'] = pd.to_datetime(final_df['timestamp'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Shanghai')

    cost_time = time.time() - start_time_proc

    if not final_df.empty:
        # 修改日志提取逻辑，使用新生成的 datetime_bj 字段进行格式化输出
        final_start_str = final_df['datetime_bj'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
        final_end_str = final_df['datetime_bj'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')

        # 新增：计算最终数据的实际跨度（天）
        final_start_ms = final_df['timestamp'].iloc[0]
        final_end_ms = final_df['timestamp'].iloc[-1]
        final_span_days = (final_end_ms - final_start_ms) / (1000 * 60 * 60 * 24)

        # 优化点 F：在总结处补充数据来源构成比例，便于观测 Cache 健康度
        logger.info(
            f"{log_prefix} [TaskEnd] 耗时: {cost_time:.2f}秒 | 范围: {final_start_str} -> {final_end_str} (跨度: {final_span_days:.2f}天) | 最终输出: {len(final_df)} 条 (来源: 本地缓存 {len(cache_df)} 条, API增量 {len(all_ohlcv) + repair_ohlcv_total_len} 条)")
    else:
        logger.warning(f"{log_prefix} [TaskEnd] 耗时: {cost_time:.2f}秒 | 内存产出范围: 无数据")

    return final_df


if __name__ == "__main__":
    # 使用注入的独立配置进行调用
    run_logger = setup_logger()

    # 模拟 Linux 云端环境 (默认关闭代理，如需本地测试可在下面传入 {'http': 'http://127.0.0.1:7890'})
    proxy_config = {
        'http': 'http://127.0.0.1:7890',  # 请根据实际运行环境决定是否注释
        'https': 'http://127.0.0.1:7890',
    }

    df_klines = fetch_binance_futures_klines(
        symbol="BNB/USDT:USDT",
        timeframe="1m",
        days=3,
        retries=3,
        proxies=proxy_config,
        cache_dir="data",
        logger=run_logger
    )