import pandas as pd
import numpy as np


def generate_bottom_powder_short_signals(kline_df, fr_df, oi_df, bar_minutes=15):
    """
    高度内聚的回测与信号生成函数：
    1. 策略逻辑：BOTTOM_STABILIZE 触发开空 (SHORT)，POWDER_KEG 触发平空。
    2. 计算逻辑严格复刻原版系统的 N/M/W 滚动窗口特征。
    3. 生成与实盘一致的标准化交易事件流 (OPEN/CLOSE 分离)。
    """

    # ==========================================
    # 1. 策略参数配置 (严格对齐原始系统)
    # ==========================================
    STRATEGY_PARAMS = {
        'N_HOURS': 24,  # 基础趋势周期 (24小时)
        'M_HOURS': 4,  # 快速动量周期 (4小时)
        'W_DAYS': 14,  # 排名滚动窗口 (14天)

        # POWDER_KEG (火药桶) 参数
        'POWDER_OI_RK': 0.90,  # OI极高分位
        'POWDER_VOL_RK': 0.30,  # 交易量极低分位

        'TARGET_WEIGHT': 1.0,
        'MAX_WEIGHT': 1.0,
        'STRATEGY_NAME': 'bottom_stabilize_powder_keg_short'
    }

    cols = ['time', 'action', 'coin', 'direction', 'event', 'price',
            'reason', 'target_weight', 'pnl', 'top_k', 'max_weight',
            'signal_timestamp_ms', 'STRATEGY_NAME', 'symbol']

    if kline_df is None or len(kline_df) == 0 or fr_df is None or len(fr_df) == 0 or oi_df is None or len(oi_df) == 0:
        return pd.DataFrame(columns=cols)

    symbol = kline_df['symbol'].iloc[0] if 'symbol' in kline_df.columns else kline_df.attrs.get('symbol', 'UNKNOWN')
    coin_name = kline_df['coin_name'].iloc[0] if 'coin_name' in kline_df.columns else (
        symbol.split('/')[0] if '/' in symbol else symbol
    )

    def _pick(df_to_check, cands, what):
        for c in cands:
            if c in df_to_check.columns:
                return c
        raise KeyError(f"[{what}] 找不到列 {cands}，实际列: {list(df_to_check.columns)}")

    # ==========================================
    # 2. 数据加载与对齐 (合并 Kline, FR, OI)
    # ==========================================
    bar = f"{bar_minutes}min"

    # 处理 K线
    k = kline_df.copy()
    kt = _pick(k, ['timestamp', 'open_time', 'time', 'ts'], 'kline')
    k['dt'] = pd.to_datetime(k[kt], unit='ms', utc=True)
    k = k.drop_duplicates(subset=[kt]).sort_values('dt').set_index('dt')
    agg = k.resample(bar, label='left', closed='left').agg(
        open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'),
        volume=('volume', 'sum')
    )
    agg['close'] = agg['close'].ffill()
    agg = agg[agg['close'].notna()]
    agg['open'] = agg['open'].fillna(agg['close'])
    agg['low'] = agg['low'].fillna(agg['close'])

    # 处理 资金费率
    fr = fr_df.copy()
    ft = _pick(fr, ['timestamp', 'fundingTime', 'time', 'ts'], 'fr')
    fc = _pick(fr, ['funding_rate', 'fundingRate', 'rate'], 'fr')
    fr['dt'] = pd.to_datetime(fr[ft], unit='ms', utc=True)
    fr_s = fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float).resample(bar,
                                                                                                        label='left',
                                                                                                        closed='left').last()

    # 处理 OI数据
    oi = oi_df.copy()
    ot = _pick(oi, ['timestamp', 'time', 'ts'], 'oi')
    oc = _pick(oi, ['oi_amount', 'openInterest', 'open_interest', 'sumOpenInterest', 'oi'], 'oi')
    oi['dt'] = pd.to_datetime(oi[ot], unit='ms', utc=True)
    oi_s = oi.drop_duplicates(subset=[ot]).sort_values('dt').set_index('dt')[oc].astype(float).resample(bar,
                                                                                                        label='left',
                                                                                                        closed='left').last()

    # 合并
    df = agg.copy()
    df['funding_rate'] = fr_s.reindex(df.index).ffill()
    df['oi_amount'] = oi_s.reindex(df.index).ffill()

    start = df[['funding_rate', 'oi_amount']].apply(lambda s: s.first_valid_index()).max()
    if start is not None:
        df = df.loc[start:].copy()
    df = df.dropna(subset=['funding_rate', 'oi_amount'])
    df = df[df['close'] > 0]

    if len(df) == 0:
        return pd.DataFrame(columns=cols)

    # ==========================================
    # 3. 核心指标与信号计算 (严格复刻原始因子库)
    # ==========================================
    EPS = 1e-12
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))

    N = B(STRATEGY_PARAMS['N_HOURS'])
    M = B(STRATEGY_PARAMS['M_HOURS'])
    W = B(STRATEGY_PARAMS['W_DAYS'] * 24)
    mp = max(50, W // 5)

    c = df['close']
    l = df['low']
    v = df['volume']
    oi_amt = df['oi_amount']
    fr_rate = df['funding_rate']

    # --- 基础计算量 ---
    minL_N = l.rolling(N, min_periods=max(2, N // 2)).min()
    oi_min_M = oi_amt.rolling(M, min_periods=2).min()

    # 排名滚动
    rk_oi = oi_amt.rolling(W, min_periods=mp).rank(pct=True)
    rk_v = v.rolling(W, min_periods=mp).rank(pct=True)
    fr_rank = fr_rate.rolling(W, min_periods=mp).rank(pct=True)

    # --- 信号 A (开仓): BOTTOM_STABILIZE ---
    # 1. OI_BOTTOM_DIVERGENCE: 价格贴近前低(<1.03) 且 OI较M周期底反弹(>1.05)
    oi_bottom_div = (c / (minL_N + EPS) < 1.03) & (oi_amt > oi_min_M * 1.05)
    # 2. FR_LOW_NEG: 费率绝对值为负 或 处于前20%极低位
    fr_low_neg = (fr_rank < 0.20) | (fr_rate < 0)
    # 3. PRICE_HIGHER_LOWS: N周期前低 > N周期再往前的低点
    price_higher_lows = minL_N > minL_N.shift(N)

    df['signal_open'] = oi_bottom_div & fr_low_neg & price_higher_lows

    # --- 信号 B (平仓): POWDER_KEG ---
    # OI分位极高(爆仓池已满) 且 交易量分位极低(流动性干涸)
    df['signal_close'] = (rk_oi > STRATEGY_PARAMS['POWDER_OI_RK']) & (rk_v < STRATEGY_PARAMS['POWDER_VOL_RK'])

    df['signal_open'] = df['signal_open'].fillna(False)
    df['signal_close'] = df['signal_close'].fillna(False)

    # ==========================================
    # 4. 状态机撮合模拟 (空头交易)
    # ==========================================
    records = []
    in_pos = False
    entry_price = 0.0

    for i in range(len(df) - 1):
        if not in_pos and df['signal_open'].iloc[i]:
            in_pos = True

            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            entry_price = float(df['open'].iloc[exec_idx])

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            records.append({
                'time': dt_bj_str,
                'action': 'SELL',  # 开空是SELL
                'coin': coin_name,
                'direction': 'SHORT',  # 明确做空方向
                'event': 'OPEN',
                'price': entry_price,
                'reason': 'BOTTOM_STABILIZE',
                'target_weight': STRATEGY_PARAMS['TARGET_WEIGHT'],
                'pnl': None,
                'top_k': 1,
                'max_weight': STRATEGY_PARAMS['MAX_WEIGHT'],
                'signal_timestamp_ms': signal_ts_ms,
                'STRATEGY_NAME': STRATEGY_PARAMS['STRATEGY_NAME'],
                'symbol': symbol
            })

        elif in_pos and df['signal_close'].iloc[i]:
            in_pos = False

            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            exit_price = float(df['open'].iloc[exec_idx])

            # 做空的收益率计算 (1 - 平仓价/开仓价)
            pnl = 1.0 - (exit_price / entry_price)
            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            records.append({
                'time': dt_bj_str,
                'action': 'BUY',  # 平空是BUY
                'coin': coin_name,
                'direction': 'SHORT',
                'event': 'CLOSE',
                'price': exit_price,
                'reason': 'POWDER_KEG',
                'target_weight': 0.0,
                'pnl': pnl,
                'top_k': 1,
                'max_weight': STRATEGY_PARAMS['MAX_WEIGHT'],
                'signal_timestamp_ms': signal_ts_ms,
                'STRATEGY_NAME': STRATEGY_PARAMS['STRATEGY_NAME'],
                'symbol': symbol
            })

    # 强制平仓收尾
    if in_pos:
        exec_time_dt = df.index[-1]
        exit_price = float(df['close'].iloc[-1])

        pnl = 1.0 - (exit_price / entry_price)
        signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
        dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

        records.append({
            'time': dt_bj_str,
            'action': 'BUY',
            'coin': coin_name,
            'direction': 'SHORT',
            'event': 'CLOSE',
            'price': exit_price,
            'reason': "FORCE_CLOSE_AT_END",
            'target_weight': 0.0,
            'pnl': pnl,
            'top_k': 1,
            'max_weight': STRATEGY_PARAMS['MAX_WEIGHT'],
            'signal_timestamp_ms': signal_ts_ms,
            'STRATEGY_NAME': STRATEGY_PARAMS['STRATEGY_NAME'],
            'symbol': symbol
        })

    return pd.DataFrame(records, columns=cols)

# ==========================================
# 测试入口
# ==========================================
if __name__ == "__main__":

    # 这是用作和以前交易进行对比的
    trades_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\factor_out_15m_debugtest\trades_AIOT.csv.gz')
    filtered_trades_df = trades_df[
        (trades_df["entry_factor"] == "ENTRY_BOTTOM_STABILIZE") &
        (trades_df["exit_factor"] == "REGIME_POWDER_KEG") &
        (trades_df["direction"] == "Short") &
        (trades_df["filter_mode"] == "original")
        ].copy()

    kline_file_path = r'W:\project\python_project\crypto_trade\app\data\AIOT_USDT_USDT_1m_kline.csv'
    fr_file_path = r'W:\project\python_project\crypto_trade\app\data\AIOT_USDT_USDT_funding_rates.csv'
    oi_file_path = r'W:\project\python_project\crypto_trade\app\data\AIOT_USDT_USDT_5m_oi.csv'
    try:
        kline_df = pd.read_csv(kline_file_path)
        # 为 kline_df 补充 symbol 属性 (模拟实盘数据源结构)
        kline_df['symbol'] = 'AIOT/USDT'

        fr_df = pd.read_csv(fr_file_path)
        fr_df['datetime'] = pd.to_datetime(fr_df['timestamp'], unit='ms', utc=True)


        oi_df = pd.read_csv(oi_file_path)

        print("正在处理数据与回测...")
        trade_records_df = generate_bottom_powder_short_signals(kline_df, fr_df,oi_df, bar_minutes=15)

        print(f"回测执行成功！共生成 {len(trade_records_df)} 条事件记录。\n")
        if not trade_records_df.empty:
            # 打印前几条查看规范化后的数据结构
            print(trade_records_df[['time', 'action', 'event', 'price', 'pnl', 'reason']].head())

    except Exception as e:
        print(f"执行出错: {e}")