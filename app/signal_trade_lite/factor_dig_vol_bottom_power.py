import numpy as np
import pandas as pd


def generate_bottom_powder_signals(kline_df, fr_df, oi_df, bar_minutes=15):
    """
    高度内聚的回测与信号生成函数：
    1. 策略参数配置统一在开头。
    2. 入场: BOTTOM_STABILIZE (原 ENTRY_BOTTOM_STABILIZE)
    3. 出场: POWDER_KEG (原 REGIME_POWDER_KEG)
    4. 内部自我包含所有对齐和撮合逻辑，零外部依赖。
    """

    # ==========================================
    # 1. 策略参数配置
    # ==========================================
    STRATEGY_PARAMS = {
        'N_HOURS': 24,  # 宏观结构判定周期(小时)
        'M_HOURS': 4,  # 微观突变判定周期(小时)
        'W_DAYS': 14,  # 排名/分位数滚动窗口(天)
        'POWDER_OI_RK': 0.90,  # 火药桶: OI分位下限
        'POWDER_VOL_RK': 0.30,  # 火药桶: 成交量分位上限
        'TARGET_WEIGHT': 1.0,  # 目标仓位
        'MAX_WEIGHT': 1.0,  # 最大允许仓位
        'STRATEGY_NAME': 'bottom_stabilize_powder_keg_long'
    }

    EPS = 1e-12

    # 规范化的输出列名
    cols = ['time', 'action', 'coin', 'direction', 'event', 'price',
            'reason', 'target_weight', 'pnl', 'top_k', 'max_weight',
            'signal_timestamp_ms', 'STRATEGY_NAME', 'symbol']

    # 边界保护
    if any(df is None or len(df) == 0 for df in [kline_df, fr_df, oi_df]):
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
    agg['high'] = agg['high'].fillna(agg['close'])
    agg['low'] = agg['low'].fillna(agg['close'])
    agg['volume'] = agg['volume'].fillna(0.0)

    # 处理 资金费率数据 (按状态采样)
    fr = fr_df.copy()
    ft = _pick(fr, ['timestamp', 'fundingTime', 'time', 'ts'], 'fr')
    fc = _pick(fr, ['funding_rate', 'fundingRate', 'rate'], 'fr')
    fr['dt'] = pd.to_datetime(fr[ft], unit='ms', utc=True)
    _fr_raw = (fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float))
    fr_s = _fr_raw.resample(bar, label='left', closed='left').last()

    # 处理 OI数据
    oi = oi_df.copy()
    ot = _pick(oi, ['timestamp', 'time', 'ts'], 'oi')
    oc = _pick(oi, ['oi_amount', 'openInterest', 'open_interest', 'sumOpenInterest', 'oi'], 'oi')
    oi['dt'] = pd.to_datetime(oi[ot], unit='ms', utc=True)
    oi_s = (oi.drop_duplicates(subset=[ot]).sort_values('dt').set_index('dt')[oc]
            .astype(float).resample(bar, label='left', closed='left').last())

    # 合并对齐 (以K线为主体)
    df = agg.copy()
    df['funding_rate'] = fr_s.reindex(df.index).ffill()
    df['oi_amount'] = oi_s.reindex(df.index).ffill()

    # 截取全部数据有效的起点
    fv = df[['oi_amount', 'funding_rate']].apply(lambda s: s.first_valid_index())
    start = max([x for x in fv.tolist() if x is not None], default=df.index[0])
    df = df.loc[start:].copy()
    df[['oi_amount', 'funding_rate']] = df[['oi_amount', 'funding_rate']].ffill()
    df = df.dropna(subset=['oi_amount', 'funding_rate'])
    df = df[df['close'] > 0]

    if len(df) == 0:
        return pd.DataFrame(columns=cols)

    # ==========================================
    # 3. 核心指标与信号计算 (严格复刻原始代码逻辑)
    # ==========================================
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))

    N = B(STRATEGY_PARAMS['N_HOURS'])
    M = B(STRATEGY_PARAMS['M_HOURS'])
    W = B(STRATEGY_PARAMS['W_DAYS'] * 24)
    mp = max(50, W // 5)  # 滚动窗口的最小有效长度

    c = df['close']
    l = df['low']
    v = df['volume']
    oi_amt = df['oi_amount']
    fr_rate = df['funding_rate']

    # --- 基础特征 ---
    minL_N = l.rolling(N, min_periods=max(2, N // 2)).min()

    # 1. 底部持仓背离 (OI_BOTTOM_DIVERGENCE)
    oi_min_M = oi_amt.rolling(M, min_periods=2).min()
    oi_bottom_divergence = (c / (minL_N + EPS) < 1.03) & (oi_amt > oi_min_M * 1.05)

    # 2. 情绪极度悲观 (FR_LOW_NEG)
    fr_rank = fr_rate.rolling(W, min_periods=mp).rank(pct=True)
    fr_low_neg = (fr_rank < 0.20) | (fr_rate < 0)

    # 3. 价格结构抬升 (PRICE_HIGHER_LOWS)
    price_higher_lows = minL_N > minL_N.shift(N)

    # 4. 杠杆高企 & 流动性枯竭 (POWDER_KEG)
    rk_oi = oi_amt.rolling(W, min_periods=mp).rank(pct=True)
    rk_v = v.rolling(W, min_periods=mp).rank(pct=True)
    powder_keg = (rk_oi > STRATEGY_PARAMS['POWDER_OI_RK']) & (rk_v < STRATEGY_PARAMS['POWDER_VOL_RK'])

    # --- 组合最终信号 ---
    df['signal_entry'] = oi_bottom_divergence & fr_low_neg & price_higher_lows
    df['signal_exit'] = powder_keg

    df['signal_entry'] = df['signal_entry'].fillna(False)
    df['signal_exit'] = df['signal_exit'].fillna(False)

    # ==========================================
    # 4. 状态机撮合模拟 (生成规范化事件流)
    # ==========================================
    records = []
    in_pos = False
    entry_price = 0.0

    for i in range(len(df) - 1):
        if not in_pos and df['signal_entry'].iloc[i]:
            in_pos = True

            # 假定于下一根K线开盘成交
            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            entry_price = float(df['open'].iloc[exec_idx])

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            # 使用客观、去除前缀的命名
            entry_reason = "BOTTOM_STABILIZE(OI_div & FR_neg & HL)"
            records.append({
                'time': dt_bj_str,
                'action': 'BUY',
                'coin': coin_name,
                'direction': 'LONG',
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

        elif in_pos and df['signal_exit'].iloc[i]:
            in_pos = False

            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            exit_price = float(df['open'].iloc[exec_idx])

            pnl = (exit_price / entry_price) - 1.0
            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            # 使用客观、去除前缀的命名
            exit_reason = f"POWDER_KEG(OI_rank>{STRATEGY_PARAMS['POWDER_OI_RK']} & VOL_rank<{STRATEGY_PARAMS['POWDER_VOL_RK']})"
            records.append({
                'time': dt_bj_str,
                'action': 'SELL',
                'coin': coin_name,
                'direction': 'LONG',
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

    kline_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_15m_latest.csv'
    fr_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_funding_latest.csv'
    oi_file_path = r'W:\project\python_project\crypto_trade\app\data\AAOI_USDT_USDT_5m_oi.csv'
    try:
        kline_df = pd.read_csv(kline_file_path)
        # 为 kline_df 补充 symbol 属性 (模拟实盘数据源结构)
        kline_df['symbol'] = 'AIOT/USDT'

        fr_df = pd.read_csv(fr_file_path)
        fr_df['datetime'] = pd.to_datetime(fr_df['timestamp'], unit='ms', utc=True)


        oi_df = pd.read_csv(oi_file_path)

        print("正在处理数据与回测...")
        trade_records_df = generate_bottom_powder_signals(kline_df, fr_df,oi_df, bar_minutes=15)

        print(f"回测执行成功！共生成 {len(trade_records_df)} 条事件记录。\n")
        if not trade_records_df.empty:
            # 打印前几条查看规范化后的数据结构
            print(trade_records_df[['time', 'action', 'event', 'price', 'pnl', 'reason']].head())

    except Exception as e:
        print(f"执行出错: {e}")