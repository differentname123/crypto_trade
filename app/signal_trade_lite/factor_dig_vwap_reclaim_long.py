import pandas as pd
import numpy as np


def generate_vwap_reclaim_long_signals(kline_df, fr_df, oi_df, bar_minutes=30):
    """
    高度内聚的回测与信号生成函数：
    1. 策略逻辑：VWAP_RECLAIM_OI 触发开多 (LONG)，FR_LOW_NEG 触发平多。
    2. 计算逻辑严格复刻原版系统的 N/M/W 滚动窗口特征，计算真正的 VWAP 与历史资金费率 Rank。
    3. 生成与实盘一致的标准化交易事件流 (OPEN/CLOSE 分离)。
    """

    # ==========================================
    # 1. 策略参数配置 (严格对齐原始系统)
    # ==========================================
    STRATEGY_PARAMS = {
        'N_HOURS': 24,  # 基础趋势周期 (24小时 VWAP 和 OI比较)
        'W_DAYS': 14,  # 排名滚动窗口 (14天 资金费率 Rank)
        'TARGET_WEIGHT': 1.0,
        'MAX_WEIGHT': 1.0,
        'STRATEGY_NAME': 'vwap_reclaim_oi_long'  # 客观的策略命名
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
    fr_s = fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float).resample(
        bar, label='left', closed='left'
    ).last()

    # 处理 OI数据
    oi = oi_df.copy()
    ot = _pick(oi, ['timestamp', 'time', 'ts'], 'oi')
    oc = _pick(oi, ['oi_amount', 'openInterest', 'open_interest', 'sumOpenInterest', 'oi'], 'oi')
    oi['dt'] = pd.to_datetime(oi[ot], unit='ms', utc=True)
    oi_s = oi.drop_duplicates(subset=[ot]).sort_values('dt').set_index('dt')[oc].astype(float).resample(
        bar, label='left', closed='left'
    ).last()

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
    W = B(STRATEGY_PARAMS['W_DAYS'] * 24)
    mp = max(50, W // 5)

    c = df['close']
    l = df['low']
    v = df['volume']
    oi_amt = df['oi_amount']
    fr_rate = df['funding_rate']

    # --- 基础计算量 ---
    # 计算 N 周期成交量加权均价 (VWAP)
    rsum_v_N = v.rolling(N, min_periods=max(2, N // 2)).sum()
    vwap_N = (c * v).rolling(N, min_periods=max(2, N // 2)).sum() / (rsum_v_N + EPS)

    # 排名滚动计算 (资金费率分位)
    fr_rank = fr_rate.rolling(W, min_periods=mp).rank(pct=True)

    # --- 信号 A (开仓 - 做多): VWAP_RECLAIM_OI ---
    # 1. VWAP_RECLAIM: 盘中跌破 VWAP_N，但收盘拉回 VWAP_N 之上 (下影线洗盘假跌破)
    vwap_reclaim = (l < vwap_N) & (c > vwap_N)

    # 2. OI_SLOPE_UP: 当前 OI 大于 24 小时前的 OI (增量资金配合)
    oi_slope_up = oi_amt > oi_amt.shift(N)

    # 3. FR_MILD: 资金费率在近 14 天处于 10% ~ 90% 的健康温和水平
    fr_mild = (fr_rank > 0.10) & (fr_rank < 0.90)

    df['signal_open'] = vwap_reclaim & oi_slope_up & fr_mild

    # --- 信号 B (平仓 - 平多): FR_LOW_NEG ---
    # 资金费率降至近 14 天最低的 20%，或者直接跌破 0 (多头持仓被倒逼，或进入轧空末端前主动离场)
    df['signal_close'] = (fr_rank < 0.20) | (fr_rate < 0)

    df['signal_open'] = df['signal_open'].fillna(False)
    df['signal_close'] = df['signal_close'].fillna(False)

    # ==========================================
    # 4. 状态机撮合模拟 (多头交易)
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
                'action': 'BUY',  # 开多是 BUY
                'coin': coin_name,
                'direction': 'LONG',  # 明确做多方向
                'event': 'OPEN',
                'price': entry_price,
                'reason': 'VWAP_RECLAIM_OI',
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

            # 做多的收益率计算 (平仓价/开仓价 - 1)
            pnl = (exit_price / entry_price) - 1.0
            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            records.append({
                'time': dt_bj_str,
                'action': 'SELL',  # 平多是 SELL
                'coin': coin_name,
                'direction': 'LONG',
                'event': 'CLOSE',
                'price': exit_price,
                'reason': 'FR_LOW_NEG',
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

        pnl = (exit_price / entry_price) - 1.0
        signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
        dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

        records.append({
            'time': dt_bj_str,
            'action': 'SELL',
            'coin': coin_name,
            'direction': 'LONG',
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


if __name__ == "__main__":

    # 这是用作和以前交易进行对比的
    trades_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\factor_out_30m_debugtest\trades_AIOT.csv.gz')
    filtered_trades_df = trades_df[
        (trades_df["entry_factor"] == "ENTRY_VWAP_RECLAIM_OI") &
        (trades_df["exit_factor"] == "FR_LOW_NEG") &
        (trades_df["direction"] == "Long") &
        (trades_df["filter_mode"] == "original")
        ].copy()

    kline_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_30m_latest.csv'
    fr_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_funding_latest.csv'


    oi_file_path = r'W:\project\python_project\crypto_trade\app\data\AIOT_USDT_USDT_5m_oi.csv'
    try:
        kline_df = pd.read_csv(kline_file_path)
        # 为 kline_df 补充 symbol 属性 (模拟实盘数据源结构)
        kline_df['symbol'] = 'AIOT/USDT'
        # kline_df = kline_df.tail(3000)
        fr_df = pd.read_csv(fr_file_path)
        fr_df['datetime'] = pd.to_datetime(fr_df['timestamp'], unit='ms', utc=True)


        oi_df = pd.read_csv(oi_file_path)

        print("正在处理数据与回测...")
        trade_records_df = generate_vwap_reclaim_long_signals(kline_df, fr_df,oi_df, bar_minutes=30)

        print(f"回测执行成功！共生成 {len(trade_records_df)} 条事件记录。\n")
        if not trade_records_df.empty:
            # 打印前几条查看规范化后的数据结构
            print(trade_records_df[['time', 'action', 'event', 'price', 'pnl', 'reason']].head())

    except Exception as e:
        print(f"执行出错: {e}")