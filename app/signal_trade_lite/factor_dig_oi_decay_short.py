import numpy as np
import pandas as pd


def generate_oi_decay_short_signals(kline_df, oi_df, bar_minutes=30):
    """
    高度内聚的回测与信号生成函数：
    1. 策略参数配置统一在开头，严格复刻原代码的时间窗口（4h, 24h, 14d）。
    2. 做空逻辑(SHORT)：当 OI市值均线死叉(动能衰竭)时做空，在 OI极高且价格未拉升(多空僵持/酝酿异动)时平仓。
    3. 生成与实盘一致的标准化交易事件流 (OPEN/CLOSE 分离)。
    """

    # ==========================================
    # 1. 策略参数配置 (严格对齐原逻辑)
    # ==========================================
    STRATEGY_PARAMS = {
        'M_HOURS': 4,  # 短周期：4小时
        'N_HOURS': 24,  # 长周期：24小时
        'W_DAYS': 14,  # 排名滚动窗口：14天
        'OI_RANK_EXTREME_TH': 0.95,  # OI极值排名阈值 (>95分位)
        'OI_HOT_TH': 0.050,  # 价格过热判定阈值 (<5%乖离率)
        'TARGET_WEIGHT': -1.0,  # 目标仓位 (做空为 -1.0)
        'MAX_WEIGHT': 1.0,  # 最大允许名义仓位 (绝对值)
        'STRATEGY_NAME': 'oi_value_decay_short'
    }

    EPS = 1e-12  # 防除零微小值

    # 规范化的输出列名
    cols = ['time', 'action', 'coin', 'direction', 'event', 'price',
            'reason', 'target_weight', 'pnl', 'top_k', 'max_weight',
            'signal_timestamp_ms', 'STRATEGY_NAME', 'symbol']

    # 边界保护
    if kline_df is None or len(kline_df) == 0 or oi_df is None or len(oi_df) == 0:
        return pd.DataFrame(columns=cols)

    # 提取标的信息
    symbol = kline_df['symbol'].iloc[0] if 'symbol' in kline_df.columns else kline_df.attrs.get('symbol', 'UNKNOWN')
    coin_name = kline_df['coin_name'].iloc[0] if 'coin_name' in kline_df.columns else (
        symbol.split('/')[0] if '/' in symbol else symbol
    )

    # --- 内嵌辅助函数：寻找可用列名 ---
    def _pick(df_to_check, cands, what):
        for c in cands:
            if c in df_to_check.columns:
                return c
        raise KeyError(f"[{what}] 找不到列 {cands}，实际列: {list(df_to_check.columns)}")

    # ==========================================
    # 2. 数据加载与对齐
    # ==========================================
    bar = f"{bar_minutes}min"

    # 处理 K线数据
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

    # 处理 OI数据
    oi = oi_df.copy()
    ot = _pick(oi, ['timestamp', 'time', 'ts'], 'oi')
    oc = _pick(oi, ['oi_amount', 'openInterest', 'open_interest', 'sumOpenInterest', 'oi'], 'oi')
    oi['dt'] = pd.to_datetime(oi[ot], unit='ms', utc=True)
    _oi_raw = (oi.drop_duplicates(subset=[ot]).sort_values('dt').set_index('dt')[oc].astype(float))
    oi_s = _oi_raw.resample(bar, label='left', closed='left').last()

    # 合并对齐
    df = agg.copy()
    df['oi_amount'] = oi_s.reindex(df.index).ffill()

    start = df['oi_amount'].first_valid_index()
    if start is not None:
        df = df.loc[start:].copy()
    df['oi_amount'] = df['oi_amount'].ffill()
    df = df.dropna(subset=['oi_amount'])
    df = df[df['close'] > 0]

    if len(df) == 0:
        return pd.DataFrame(columns=cols)

    # ==========================================
    # 3. 核心指标与信号计算 (完全复刻原始公式)
    # ==========================================
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))

    M = B(STRATEGY_PARAMS['M_HOURS'])
    N = B(STRATEGY_PARAMS['N_HOURS'])
    W = B(STRATEGY_PARAMS['W_DAYS'] * 24)
    mp = max(50, W // 5)

    c = df['close']
    oi_amt = df['oi_amount']

    # -- 计算底层指标 --
    # 1. OI名义市值及其长短 EMA
    oi_value = oi_amt * c
    oiv_ema_M = oi_value.ewm(span=M, adjust=False).mean()
    oiv_ema_N = oi_value.ewm(span=N, adjust=False).mean()

    # 2. 价格24小时均线
    ma_N = c.rolling(N, min_periods=max(2, N // 2)).mean()

    # 3. OI历史 14 天百分位排名
    rk_oi = oi_amt.rolling(W, min_periods=mp).rank(pct=True)

    # -- 构造交易信号 --
    # 信号 A: 开仓 (原 EXIT_OI_VALUE_MA_DEAD_CROSS) -> 动能衰竭，开空
    # 逻辑: 4h EMA 向下击穿 24h EMA (Cross Down)
    df['signal_open_short'] = (oiv_ema_M < oiv_ema_N) & (oiv_ema_M.shift(1) >= oiv_ema_N.shift(1))

    # 信号 B: 平仓 (原 OI_EXTREME_PRICE_NOT_HOT) -> 火药桶酝酿，平空规避爆拉风险
    # 逻辑: OI 处于 95 分位以上 且 乖离率小于 5%
    df['signal_close_short'] = (rk_oi > STRATEGY_PARAMS['OI_RANK_EXTREME_TH']) & (
                (c / (ma_N + EPS) - 1.0) < STRATEGY_PARAMS['OI_HOT_TH'])

    df['signal_open_short'] = df['signal_open_short'].fillna(False)
    df['signal_close_short'] = df['signal_close_short'].fillna(False)

    # ==========================================
    # 4. 状态机撮合模拟 (生成规范化事件流)
    # ==========================================
    records = []
    in_pos = False
    entry_price = 0.0

    for i in range(len(df) - 1):
        if not in_pos and df['signal_open_short'].iloc[i]:
            in_pos = True

            # 假定于下一根K线开盘成交
            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            entry_price = float(df['open'].iloc[exec_idx])

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            entry_reason = "OI_VALUE_DEAD_CROSS(EMA4h < EMA24h)"
            records.append({
                'time': dt_bj_str,
                'action': 'SELL',  # 开空动作为 卖出
                'coin': coin_name,
                'direction': 'SHORT',  # 仓位方向为 空头
                'event': 'OPEN',
                'price': entry_price,
                'reason': entry_reason,
                'target_weight': STRATEGY_PARAMS['TARGET_WEIGHT'],
                'pnl': None,
                'top_k': 1,
                'max_weight': STRATEGY_PARAMS['MAX_WEIGHT'],
                'signal_timestamp_ms': signal_ts_ms,
                'STRATEGY_NAME': STRATEGY_PARAMS['STRATEGY_NAME'],
                'symbol': symbol
            })

        elif in_pos and df['signal_close_short'].iloc[i]:
            in_pos = False

            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            exit_price = float(df['open'].iloc[exec_idx])

            # 做空的收益率计算公式
            pnl = (entry_price - exit_price) / entry_price

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            exit_reason = f"OI_EXTREME_PRICE_NOT_HOT(Rank_OI>{STRATEGY_PARAMS['OI_RANK_EXTREME_TH']} & Dev<5%)"
            records.append({
                'time': dt_bj_str,
                'action': 'BUY',  # 平空动作为 买入
                'coin': coin_name,
                'direction': 'SHORT',  # 平除的是 空头仓位
                'event': 'CLOSE',
                'price': exit_price,
                'reason': exit_reason,
                'target_weight': 0.0,
                'pnl': pnl,
                'top_k': 1,
                'max_weight': STRATEGY_PARAMS['MAX_WEIGHT'],
                'signal_timestamp_ms': signal_ts_ms,
                'STRATEGY_NAME': STRATEGY_PARAMS['STRATEGY_NAME'],
                'symbol': symbol
            })

    # 强制平仓收尾 (回测结束时清理持仓)
    if in_pos:
        exec_time_dt = df.index[-1]
        exit_price = float(df['close'].iloc[-1])

        pnl = (entry_price - exit_price) / entry_price
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


if __name__ == "__main__":

    # 这是用作和以前交易进行对比的
    trades_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\factor_out_30m_debugtest\trades_AIOT.csv.gz')
    filtered_trades_df = trades_df[
        (trades_df["entry_factor"] == "EXIT_OI_VALUE_MA_DEAD_CROSS") &
        (trades_df["exit_factor"] == "OI_EXTREME_PRICE_NOT_HOT") &
        (trades_df["direction"] == "Short") &
        (trades_df["filter_mode"] == "original")
        ].copy()

    kline_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_30m_latest.csv'
    oi_file_path = r'W:\project\python_project\crypto_trade\app\data\AIOT_USDT_USDT_5m_oi.csv'
    try:
        kline_df = pd.read_csv(kline_file_path)
        # 为 kline_df 补充 symbol 属性 (模拟实盘数据源结构)
        kline_df['symbol'] = 'AIOT/USDT'

        oi_df = pd.read_csv(oi_file_path)

        print("正在处理数据与回测...")
        trade_records_df = generate_oi_decay_short_signals(kline_df,oi_df, bar_minutes=30)

        print(f"回测执行成功！共生成 {len(trade_records_df)} 条事件记录。\n")
        if not trade_records_df.empty:
            # 打印前几条查看规范化后的数据结构
            print(trade_records_df[['time', 'action', 'event', 'price', 'pnl', 'reason']].head())

    except Exception as e:
        print(f"执行出错: {e}")