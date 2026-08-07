import time
import ccxt
import pandas as pd
import numpy as np
import warnings
from datetime import datetime

warnings.filterwarnings('ignore')

# ============================================================================
# 0. 全局配置与最优参数
# ============================================================================
GLOBAL_PROXY = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

# 你选定的黄金平原参数
OPTIMAL_PARAMS = {
    'BAR_MINUTES': 60,
    'UPPER_WICK_THRESH': 0.60,
    'VOL_QUANTILE': 0.95,
    'HIGH_CLOSE_THRESH': 0.90,
    'WARMUP_DAYS': 30  # 因子计算必须的30天历史窗口
}


# ============================================================================
# 1. 交易所初始化与数据拉取
# ============================================================================
def init_exchange(exchange_name, default_type='swap'):
    config = {
        'enableRateLimit': True,
        'proxies': GLOBAL_PROXY, # 视你的网络环境决定是否取消注释
    }
    if default_type:
        config['options'] = {'defaultType': default_type}
    return getattr(ccxt, exchange_name)(config)


def fetch_with_pagination(exchange, fetch_func, symbol, since, limit_per_request, timeframe=None):
    all_data = []
    current_since = since
    max_retries = 3

    while True:
        retry_count = 0
        success = False
        data = []

        while retry_count < max_retries and not success:
            try:
                kwargs = {'since': current_since, 'limit': limit_per_request}
                if timeframe:
                    data = fetch_func(symbol, timeframe, **kwargs)
                else:
                    data = fetch_func(symbol, **kwargs)
                success = True
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"❌ {symbol} 拉取失败，跳过。错误: {e}")
                    return []
                time.sleep(2 * retry_count)

        if not data:
            break

        all_data.extend(data)
        last_timestamp = int(data[-1][0])
        current_since = last_timestamp + 1

        if last_timestamp >= exchange.milliseconds() - 60000:
            break
        time.sleep(0.05)

    return all_data


def get_klines_df(exchange, symbol, days=35, timeframe='1h'):
    """拉取指定天数的 1h K线 (35天足够覆盖30天Warmup + 3天扫描)"""
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    raw_data = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, since, 1000, timeframe=timeframe)

    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values('timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    dt_series = pd.to_datetime(df['timestamp'], unit='ms')
    df['datetime_bj'] = dt_series.dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
    df.attrs['symbol'] = symbol
    return df


# ============================================================================
# 2. 获取涨跌幅榜前10
# ============================================================================
def get_top_movers(exchange, top_n=10):
    print("📡 正在获取全市场 USDT 永续合约 Tickers...")
    tickers = exchange.fetch_tickers(params={'type': 'swap'})

    usdt_swaps = {k: v for k, v in tickers.items() if k.endswith(':USDT') and v.get('percentage') is not None}
    df = pd.DataFrame(usdt_swaps).T
    df = df.sort_values('percentage', ascending=False)

    gainers = df.head(top_n).index.tolist()
    losers = df.tail(top_n).index.tolist()

    targets = list(set(gainers + losers))
    print(f"🔥 涨幅榜前{top_n}: {gainers}")
    print(f"💧 跌幅榜前{top_n}: {losers}")
    print(f"🎯 最终监控列表 ({len(targets)}个): {targets}\n")
    return targets


# ============================================================================
# 3. 核心信号计算逻辑
# ============================================================================
def scan_signals(df, params):
    if len(df) < 720:
        return []

    o, h, l, c, v = df['open'], df['high'], df['low'], df['close'], df['volume']
    W = 24 * params['WARMUP_DAYS']
    N = 24

    EPS = 1e-12
    maxH_N = h.rolling(N, min_periods=max(2, N // 2)).max()
    rng = (h - l) + EPS
    uw = (h - np.maximum(o, c)) / rng
    inside = (h < h.shift(1)) & (l > l.shift(1))
    vol_q = v.rolling(W, min_periods=50).quantile(params['VOL_QUANTILE']).shift(1)

    kline_long_upper_wick = uw > params['UPPER_WICK_THRESH']
    volume_spike = v > vol_q

    entry_signal = (c / (maxH_N + EPS) > params['HIGH_CLOSE_THRESH']) & kline_long_upper_wick & volume_spike
    exit_signal = inside.shift(1, fill_value=False) & (c > h.shift(1)) & volume_spike

    signals = []
    symbol = df.attrs.get('symbol', 'UNKNOWN')

    cutoff_time = df['datetime_bj'].max() - pd.Timedelta(days=3)

    for i in range(len(df)):
        if df.loc[i, 'datetime_bj'] < cutoff_time:
            continue

        if entry_signal.iloc[i]:
            signals.append({
                'symbol': symbol,
                'signal_type': '🟢 ENTRY (接针做多)',
                'datetime_bj': df.loc[i, 'datetime_bj'],
                'price': c.iloc[i],
                'reason': f"高位长上影({uw.iloc[i]:.2f}) + 爆量({v.iloc[i]:.0f} > {vol_q.iloc[i]:.0f})"
            })
        if exit_signal.iloc[i]:
            signals.append({
                'symbol': symbol,
                'signal_type': '🔴 EXIT (突破止盈)',
                'datetime_bj': df.loc[i, 'datetime_bj'],
                'price': c.iloc[i],
                'reason': f"孕线突破 + 爆量({v.iloc[i]:.0f} > {vol_q.iloc[i]:.0f})"
            })

    return signals


# ============================================================================
# 4. 主流程
# ============================================================================
def main():
    exchange = init_exchange('binance', default_type='swap')
    targets = get_top_movers(exchange, top_n=10)

    all_signals = []
    print(f"🚀 开始扫描 {len(targets)} 个币种的 1h K线信号 (包含30天Warmup)...\n" + "-" * 50)

    for idx, symbol in enumerate(targets):
        print(f"[{idx + 1}/{len(targets)}] 扫描: {symbol} ...", end=" ")
        df = get_klines_df(exchange, symbol, days=35, timeframe='1h')

        if df.empty:
            print("❌ 无数据")
            continue

        signals = scan_signals(df, OPTIMAL_PARAMS)
        if signals:
            all_signals.extend(signals)
            print(f"✅ 发现 {len(signals)} 个信号!")
        else:
            print("⚪ 无信号")

    print("-" * 50)

    if not all_signals:
        print("⚠️ 最近 3 天内，涨跌幅榜前10的币种均未触发任何信号。")
        return

    # 整理为 DataFrame
    df_res = pd.DataFrame(all_signals)

    # 🎯 核心排序优化：按“每个币种的最新信号时间”升序排列，同币种内按时间正序
    # 1. 计算每个币种的最新(最大)信号时间
    latest_times = df_res.groupby('symbol')['datetime_bj'].max().reset_index()
    latest_times.rename(columns={'datetime_bj': 'latest_time'}, inplace=True)

    # 2. 合并回原表
    df_res = df_res.merge(latest_times, on='symbol', how='left')

    # 3. 排序：先按最新信号时间升序(Ascending)，再按币种名，最后按信号时间升序
    df_res.sort_values(['latest_time', 'symbol', 'datetime_bj'], ascending=[True, True, True], inplace=True)

    # 4. 移除辅助列
    df_res.drop(columns=['latest_time'], inplace=True)

    # 格式化时间字符串
    df_res['datetime_str'] = df_res['datetime_bj'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # 保存 CSV (CSV中也是按此规则排列)
    df_save = df_res[['symbol', 'signal_type', 'datetime_str', 'price', 'reason']]
    filename = f"signal_scan_latest.csv"
    df_save.to_csv(filename, index=False, encoding='utf-8-sig')

    print(f"\n🎉 扫描完成！共发现 {len(df_res)} 个信号。")
    print(f"💾 结果已保存至: {filename}")

    # 🎯 终端打印重构
    print("\n📊 信号汇总表 (按币种最新信号时间升序, 币内时间正序):")
    print("=" * 100)

    # 注意：必须使用 sort=False，否则 groupby 会强行按字母顺序重新打乱排序
    grouped = df_res.groupby('symbol', sort=False)
    for symbol, group in grouped:
        # 获取该币种的最新信号时间用于标题显示
        latest_dt = group['datetime_bj'].max().strftime('%m-%d %H:%M')
        print(f"🪙 【{symbol}】 (最新信号: {latest_dt} | 共 {len(group)} 个信号)")
        for _, row in group.iterrows():
            sig = row['signal_type']
            dt = row['datetime_str']
            px = f"{row['price']:.8g}"
            reason = row['reason']
            print(f"   {sig} | {dt} | 价格: {px:<12} | {reason}")
        print("-" * 100)


if __name__ == "__main__":
    main()