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
        'proxies': GLOBAL_PROXY,  # 视你的网络环境决定是否取消注释
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
# 3. 核心信号计算与交易配对逻辑
# ============================================================================
def scan_signals_and_trades(df, params):
    if len(df) < 720:
        return [], [], None

    o, h, l, c, v = df['open'], df['high'], df['low'], df['close'], df['volume']
    W = 24 * params['WARMUP_DAYS']  # 720
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
    trades = []
    open_trade = None
    symbol = df.attrs.get('symbol', 'UNKNOWN')

    # 🎯 计算预热期结束的时间点
    warmup_bars = W  # 720根K线作为预热期
    warmup_end_time = df['datetime_bj'].iloc[warmup_bars] if len(df) > warmup_bars else df['datetime_bj'].iloc[0]

    for i in range(len(df)):
        dt = df.loc[i, 'datetime_bj']
        is_entry = entry_signal.iloc[i]
        is_exit = exit_signal.iloc[i]

        # 🎯 跳过预热期内的所有信号
        if dt < warmup_end_time:
            continue

        # 1. 收集信号
        if is_entry:
            signals.append({
                'symbol': symbol, 'signal_type': '🟢 ENTRY (接针做多)', 'datetime_bj': dt,
                'price': c.iloc[i],
                'reason': f"高位长上影({uw.iloc[i]:.2f}) + 爆量({v.iloc[i]:.0f} > {vol_q.iloc[i]:.0f})"
            })
        if is_exit:
            signals.append({
                'symbol': symbol, 'signal_type': '🔴 EXIT (突破止盈)', 'datetime_bj': dt,
                'price': c.iloc[i], 'reason': f"孕线突破 + 爆量({v.iloc[i]:.0f} > {vol_q.iloc[i]:.0f})"
            })

        # 2. 状态机配对交易（只在预热期结束后才开始）
        if is_entry and open_trade is None:
            open_trade = {'entry_time': dt, 'entry_price': c.iloc[i]}
        elif is_exit and open_trade is not None:
            exit_price = c.iloc[i]
            entry_price = open_trade['entry_price']
            pnl_pct = (exit_price / entry_price - 1.0) * 100

            trades.append({
                'symbol': symbol,
                'entry_time': open_trade['entry_time'],
                'entry_price': entry_price,
                'exit_time': dt,
                'exit_price': exit_price,
                'pnl_pct': pnl_pct,
                'hold_duration': dt - open_trade['entry_time']
            })
            open_trade = None

    # 3. 交易排序（已取消cutoff限制，保留所有预热期后的交易）
    recent_trades = trades.copy()
    recent_trades.sort(key=lambda x: x['entry_time'])

    # 4. 当前未平仓持仓
    current_holding = None
    if open_trade is not None:
        current_price = c.iloc[-1]
        float_pnl = (current_price / open_trade['entry_price'] - 1.0) * 100
        current_holding = {
            'symbol': symbol,
            'entry_time': open_trade['entry_time'],
            'entry_price': open_trade['entry_price'],
            'current_price': current_price,
            'float_pnl': float_pnl,
            'hold_duration': df['datetime_bj'].iloc[-1] - open_trade['entry_time']
        }

    return signals, recent_trades, current_holding


# ============================================================================
# 4. 主流程
# ============================================================================
def main():
    exchange = init_exchange('binance', default_type='swap')
    targets = get_top_movers(exchange, top_n=10)
    # targets = ['BTC/USDT:USDT']
    all_signals = []
    symbol_trade_data = {}  # 存储每个币的交易复盘数据
    all_recent_trades = []  # 存储全局近期交易

    print(f"🚀 开始扫描 {len(targets)} 个币种的 1h K线信号 (包含30天Warmup)...\n" + "-" * 50)

    for idx, symbol in enumerate(targets):
        print(f"[{idx + 1}/{len(targets)}] 扫描: {symbol} ...", end=" ")
        df = get_klines_df(exchange, symbol, days=35, timeframe='1h')

        if df.empty:
            print("❌ 无数据")
            continue

        signals, recent_trades, current_holding = scan_signals_and_trades(df, OPTIMAL_PARAMS)

        if signals:
            all_signals.extend(signals)
            print(f"✅ 发现 {len(signals)} 个信号!", end="")
            if recent_trades or current_holding:
                print(f" (含 {len(recent_trades)} 笔闭环, {1 if current_holding else 0} 笔持仓)")
            else:
                print()
        else:
            print("⚪ 无信号")

        if recent_trades or current_holding:
            symbol_trade_data[symbol] = {'trades': recent_trades, 'holding': current_holding}
            all_recent_trades.extend(recent_trades)

    print("-" * 50)

    if not all_signals and not all_recent_trades:
        print("⚠️ 涨跌幅榜前10的币种均未触发任何信号或交易。")
        return

    # ==========================
    # 1. 打印信号汇总表 (保持原有逻辑)
    # ==========================
    if all_signals:
        df_res = pd.DataFrame(all_signals)
        latest_times = df_res.groupby('symbol')['datetime_bj'].max().reset_index()
        latest_times.rename(columns={'datetime_bj': 'latest_time'}, inplace=True)
        df_res = df_res.merge(latest_times, on='symbol', how='left')
        df_res.sort_values(['latest_time', 'symbol', 'datetime_bj'], ascending=[True, True, True], inplace=True)
        df_res.drop(columns=['latest_time'], inplace=True)

        df_res['datetime_str'] = df_res['datetime_bj'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_save = df_res[['symbol', 'signal_type', 'datetime_str', 'price', 'reason']]

        filename = f"signal_scan_latest.csv"
        df_save.to_csv(filename, index=False, encoding='utf-8-sig')

        print(f"\n🎉 信号扫描完成！共发现 {len(df_res)} 个信号。 💾 保存至: {filename}")
        print("\n📊 信号汇总表 (按币种最新信号时间升序, 币内时间正序):")
        print("=" * 100)

        grouped = df_res.groupby('symbol', sort=False)
        for symbol, group in grouped:
            latest_dt = group['datetime_bj'].max().strftime('%m-%d %H:%M')
            print(f"🪙 【{symbol}】 (最新信号: {latest_dt} | 共 {len(group)} 个信号)")
            for _, row in group.iterrows():
                print(
                    f"   {row['signal_type']} | {row['datetime_str']} | 价格: {row['price']:<12.8g} | {row['reason']}")

            print("-" * 100)

    # ==========================
    # 2. 打印交易复盘与统计 (新增核心功能)
    # ==========================
    if all_recent_trades or any(data['holding'] for data in symbol_trade_data.values()):
        print("\n" + "=" * 100)
        print("📊 交易复盘与统计")
        print("=" * 100)

        # 提取全局的未闭环持仓
        all_holdings = [data['holding'] for data in symbol_trade_data.values() if data['holding']]

        # ========================== 单币种明细 ==========================
        # 按照浮盈降序排序，当前浮亏越多的越在下面，无持仓的放在最底下
        sorted_trade_data = sorted(
            symbol_trade_data.items(),
            key=lambda x: x[1]['holding']['float_pnl'] if x[1]['holding'] else float('inf'),
            reverse=True
        )

        for symbol, data in sorted_trade_data:
            trades = data['trades']
            holding = data['holding']

            print(f"\n🪙 【{symbol}】")
            if trades:
                wins = [t for t in trades if t['pnl_pct'] > 0]
                losses = [t for t in trades if t['pnl_pct'] <= 0]
                total_pnl = sum(t['pnl_pct'] for t in trades)
                win_rate = (len(wins) / len(trades)) * 100

                print(
                    f"📈 统计摘要: 闭环 {len(trades)} 笔 | 胜率 {win_rate:.1f}% ({len(wins)}胜 {len(losses)}负) | 累计收益 {total_pnl:+.2f}%")
                print("📜 交易明细:")
                for idx, t in enumerate(trades, 1):
                    e_str = t['entry_time'].strftime('%m-%d %H:%M')
                    x_str = t['exit_time'].strftime('%m-%d %H:%M')
                    dur = str(t['hold_duration']).split('.')[0]
                    print(
                        f"   [{idx}] 🟢 {e_str} ({t['entry_price']:.6g}) ➔ 🔴 {x_str} ({t['exit_price']:.6g}) | 收益: {t['pnl_pct']:+.2f}% | 持仓: {dur}")
            else:
                print("📈 统计摘要: 无已闭环交易。")

            if holding:
                e_str = holding['entry_time'].strftime('%m-%d %H:%M')
                dur = str(holding['hold_duration']).split('.')[0]
                print(
                    f"⏳ 当前持仓: 🟢 {e_str} ({holding['entry_price']:.6g}) | 现价: {holding['current_price']:.6g} | 浮盈: {holding['float_pnl']:+.2f}% | 持仓: {dur}")
            print("-" * 100)

        # ========================== 全局统计汇总 (已移至末尾) ==========================
        if all_recent_trades or all_holdings:
            print("\n🌍 全局汇总:")

            # 初始化汇总变量
            closed_wins = closed_total_pnl = total_closed = 0
            open_wins = open_total_pnl = total_open = 0

            # 1. 闭环交易统计
            total_closed = len(all_recent_trades)
            if total_closed > 0:
                closed_wins = sum(1 for t in all_recent_trades if t['pnl_pct'] > 0)
                closed_total_pnl = sum(t['pnl_pct'] for t in all_recent_trades)
                closed_win_rate = (closed_wins / total_closed) * 100
                closed_avg_pnl = closed_total_pnl / total_closed
                print(
                    f"   [闭 环 交 易] {total_closed} 笔 | 胜率 {closed_win_rate:.1f}% ({closed_wins}胜 {total_closed - closed_wins}负) | 累计毛收益 {closed_total_pnl:+.2f}% | 平均收益 {closed_avg_pnl:+.2f}%")
            else:
                print(f"   [闭 环 交 易] 0 笔")

            # 2. 未闭环(持仓)统计
            total_open = len(all_holdings)
            if total_open > 0:
                open_wins = sum(1 for h in all_holdings if h['float_pnl'] > 0)
                open_total_pnl = sum(h['float_pnl'] for h in all_holdings)
                open_win_rate = (open_wins / total_open) * 100
                open_avg_pnl = open_total_pnl / total_open
                print(
                    f"   [未闭环/持仓] {total_open} 笔 | 浮盈胜率 {open_win_rate:.1f}% ({open_wins}胜 {total_open - open_wins}负) | 累计总浮盈 {open_total_pnl:+.2f}% | 平均浮盈 {open_avg_pnl:+.2f}%")
            else:
                print(f"   [未闭环/持仓] 0 笔")

            # 3. 总体统计 (闭环 + 未闭环)
            total_all = total_closed + total_open
            if total_all > 0:
                all_wins = closed_wins + open_wins
                all_total_pnl = closed_total_pnl + open_total_pnl
                all_win_rate = (all_wins / total_all) * 100
                all_avg_pnl = all_total_pnl / total_all
                print(
                    f"   [总 体 汇 总] {total_all} 笔 | 综合胜率 {all_win_rate:.1f}% ({all_wins}胜 {total_all - all_wins}负) | 综合总收益 {all_total_pnl:+.2f}% (未扣手续费) | 综合均收益 {all_avg_pnl:+.2f}%")

            print("=" * 100)

        # 导出交易明细 CSV
        if all_recent_trades:
            df_trades = pd.DataFrame(all_recent_trades)
            df_trades['entry_time'] = df_trades['entry_time'].dt.strftime('%Y-%m-%d %H:%M')
            df_trades['exit_time'] = df_trades['exit_time'].dt.strftime('%Y-%m-%d %H:%M')
            df_trades['hold_duration'] = df_trades['hold_duration'].astype(str).str.split('.').str[0]

            df_trades_save = df_trades[
                ['symbol', 'entry_time', 'entry_price', 'exit_time', 'exit_price', 'pnl_pct', 'hold_duration']]
            trade_filename = f"trades_scan_latest.csv"
            df_trades_save.to_csv(trade_filename, index=False, encoding='utf-8-sig')
            print(f"\n💾 交易明细已保存至: {trade_filename}")


if __name__ == "__main__":
    main()