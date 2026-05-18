import ccxt
import pandas as pd
import time
import os


def fetch_binance_futures_klines(symbol, timeframe='1h', days=30, retries=5):
    """
    专门获取币安 U本位永续合约 (Futures/Swap) 的 K 线 (OHLCV) 数据。
    包含错误重试、分页无缝拼接、并自动转换为北京时间。带本地增量缓存更新机制。

    :param symbol: 交易对，如 'BTC/USDT:USDT' 或 'BTC/USDT'
    :param timeframe: K线周期，如 '1h', '15m', '1d'
    :param days: 获取过去多少天的数据
    :param retries: 遇到网络或API报错时的最大重试次数
    :return: 包含 K 线数据的 Pandas DataFrame
    """
    # === 新增：记录开始运行的当前时间 ===
    start_time_proc = time.time()
    start_time_str = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')

    # 1. 正确实例化币安合约交易所对象
    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap',  # ⚠️ 核心修正：强制指定为永续合约市场
        },
        'proxies': {
            'http': 'http://127.0.0.1:7890',  # 请根据实际运行环境决定是否注释
            'https': 'http://127.0.0.1:7890',
        },
    })

    # 处理 symbol 格式兼容性 (将 'BTC/USDT' 自动转为 ccxt 认识的 'BTC/USDT:USDT')
    if ':' not in symbol and symbol.endswith('USDT'):
        symbol = f"{symbol}:USDT"

    # === 新增：缓存与增量拉取逻辑 ===
    cache_dir = r"W:\project\python_project\crypto_trade\data"
    os.makedirs(cache_dir, exist_ok=True)
    # 将 symbol 中的特殊字符替换为下划线，作为文件名
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    cache_file = os.path.join(cache_dir, f"{safe_symbol}_{timeframe}.csv")

    # 获取 K线周期的毫秒数，用于计算重叠数量和跳变校验
    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    overlap_ms = timeframe_ms * 10  # 默认重叠 10 根 K 线的容错时间

    current_ms = exchange.milliseconds()
    requested_since = current_ms - int(days * 24 * 60 * 60 * 1000)
    since = requested_since

    cache_df = pd.DataFrame()
    existing_latest_time_str = "无"  # 记录已有数据最新时间

    if os.path.exists(cache_file):
        try:
            cache_df = pd.read_csv(cache_file)
            if not cache_df.empty:
                # 转换 timestamp 为 datetime 对象（原本存的是北京时间）
                cache_df['timestamp'] = pd.to_datetime(cache_df['timestamp'])

                # 提取最新时间记录，用于第一条日志打印
                existing_latest_time_str = cache_df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')

                # 提取缓存最老和最新的时间戳（将其从北京时间转回 UTC 毫秒）
                cache_oldest_ms = int(
                    cache_df['timestamp'].iloc[0].tz_localize('Asia/Shanghai').tz_convert('UTC').timestamp() * 1000)
                cache_latest_ms = int(
                    cache_df['timestamp'].iloc[-1].tz_localize('Asia/Shanghai').tz_convert('UTC').timestamp() * 1000)

                # 判断缓存是否足够覆盖请求的天数
                if cache_oldest_ms <= requested_since:
                    # 缓存充足，只需增量更新，哪怕差距为0，也会往前推10根K线进行覆盖更新
                    since = cache_latest_ms - overlap_ms
                else:
                    # 缓存不够指定的天数，需要全量拉取，后续会再进行合并
                    since = requested_since
        except Exception:
            cache_df = pd.DataFrame()

    # === 【日志要求 1】打印第一行：开始拉取的情况 ===
    since_str = pd.to_datetime(since, unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai').tz_localize(
        None).strftime('%Y-%m-%d %H:%M:%S')
    current_ms_str = pd.to_datetime(current_ms, unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai').tz_localize(
        None).strftime('%Y-%m-%d %H:%M:%S')
    print(
        f"1. [拉取开始] {symbol} 当前时间: {start_time_str} | 已有数据最新时间: {existing_latest_time_str} | 新拉取时间范围: {since_str} -> {current_ms_str}")

    limit = 1000  # 币安单次最高 1000 条
    df = pd.DataFrame()

    # === 新增：数据完整性全局重试机制（拦截异常跳变数据） ===
    integrity_retries = 3  # 如果发现跳变，最大重试3次

    for attempt in range(integrity_retries):
        all_ohlcv = []
        curr_since = since  # 使用局部 since，以便重试时可以重置

        # === 拉取循环 ===
        while True:
            curr_ohlcv = None
            # 网络错误重试机制
            for net_attempt in range(retries):
                try:
                    curr_ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit)
                    break
                except Exception:
                    if net_attempt == retries - 1:
                        break
                    time.sleep(1)

            # 如果彻底没拉到数据，或者拉取为空，则跳出循环
            if not curr_ohlcv:
                break

            all_ohlcv.extend(curr_ohlcv)

            # 更新 curr_since 为最后一条数据时间戳 + 1毫秒，进行下一页分页
            last_timestamp = curr_ohlcv[-1][0]
            curr_since = last_timestamp + 1

            # 如果获取到的最新数据距离当前时间不足1分钟，说明拉取到最新了
            if last_timestamp >= current_ms - 60000:
                break

        # === 数据合并 ===
        new_df = pd.DataFrame()
        if all_ohlcv:
            new_df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            new_df[numeric_cols] = new_df[numeric_cols].astype(float)
            new_df['timestamp'] = pd.to_datetime(new_df['timestamp'], unit='ms').dt.tz_localize('UTC')
            new_df['timestamp'] = new_df['timestamp'].dt.tz_convert('Asia/Shanghai')
            new_df['timestamp'] = new_df['timestamp'].dt.tz_localize(None)

        if not cache_df.empty and not new_df.empty:
            df = pd.concat([cache_df, new_df], ignore_index=True)
        elif not new_df.empty:
            df = new_df
        else:
            df = cache_df

        # 健壮性 1：去重
        if not df.empty:
            df = df.drop_duplicates(subset=['timestamp'], keep='last').sort_values('timestamp').reset_index(drop=True)

            # 健壮性 2：绝对无跳变校验（你的刚性要求）
            expected_diff = pd.Timedelta(milliseconds=timeframe_ms)
            time_diffs = df['timestamp'].diff().iloc[1:]
            jump_mask = time_diffs > expected_diff

            if jump_mask.any():
                # 发现跳变：要么重试，要么拦截返回空
                if attempt < integrity_retries - 1:
                    # 强行清空可能有问题的缓存数据，重置 since 为最原本的请求时间，重新全量拉取
                    cache_df = pd.DataFrame()
                    since = requested_since
                    time.sleep(2)  # 稍微停顿后重试
                    continue
                else:
                    # 重试耗尽依然有跳变（说明是交易所物理缺失），直接将 df 强行清空，进入“要么返回空数据”分支
                    df = pd.DataFrame()
                    break
            else:
                # 完美数据，无跳变，执行写入缓存并结束尝试
                try:
                    df.to_csv(cache_file, index=False)
                except Exception:
                    pass
                break
        else:
            break

    # 过滤数据：确保只返回用户实际请求的 days 长度的数据
    final_df = pd.DataFrame()
    if not df.empty:
        requested_start_dt = pd.to_datetime(requested_since, unit='ms').tz_localize('UTC').tz_convert(
            'Asia/Shanghai').tz_localize(None)
        final_df = df[df['timestamp'] >= requested_start_dt].reset_index(drop=True)

    # === 【日志要求 2】打印第二行：拉取完成日志 ===
    end_time_str = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    cost_time = time.time() - start_time_proc

    if not final_df.empty:
        final_start_str = final_df['timestamp'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
        final_end_str = final_df['timestamp'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
        print(
            f"2. [拉取完成] 当前时间: {end_time_str} | 耗时: {cost_time:.2f}秒 | 最终返回时间范围: {final_start_str} -> {final_end_str} | 数量: {len(final_df)}")
    else:
        print(f"2. [拉取完成] 当前时间: {end_time_str} | 耗时: {cost_time:.2f}秒 | 最终返回时间范围: 无 | 数量: 0")

    return final_df


if __name__ == "__main__":
    symbol = "BNB/USDT:USDT"
    timeframe = "1m"
    days = 1

    # 拉取数据
    df_klines = fetch_binance_futures_klines(symbol=symbol, timeframe=timeframe, days=days)