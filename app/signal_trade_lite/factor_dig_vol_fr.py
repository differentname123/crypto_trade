import pandas as pd
import numpy as np


def generate_vol_fr_signals(kline_df, fr_df, bar_minutes=5):
    """
    高度内聚的回测与信号生成函数：
    根据截面动量挖掘框架，使用 VOL_LOW_TO_HIGH 作为开仓信号，FR_RECOVERY_FROM_LOW 作为平仓信号。
    """

    # ==========================================
    # 1. 策略参数配置 (严格对齐原逻辑常数)
    # ==========================================
    STRATEGY_PARAMS = {
        'M_HOURS': 4,  # 动量/状态对比回溯周期 (对应原代码的 M)
        'N_HOURS': 24,  # ATR计算周期 (对应原代码的 N)
        'W_DAYS': 14,  # 排名(Rank)滚动窗口 (对应原代码的 W)

        # VOL_LOW_TO_HIGH 信号参数
        'ATR_RANK_LOW_TH': 0.20,  # 4小时前波动率极度萎缩的最高分位数 (20%)
        'ATR_RANK_HIGH_TH': 0.60,  # 当前波动率爆发的最低分位数 (60%)

        # FR_RECOVERY_FROM_LOW 信号参数
        'FR_RANK_LOW_TH': 0.10,  # 4小时前资金费率极度悲观的最高分位数 (10%)

        'TARGET_WEIGHT': 1.0,  # 目标做多仓位
        'MAX_WEIGHT': 1.0,  # 最大允许仓位
        'STRATEGY_NAME': 'vol_breakout_fr_recovery_long'
    }

    # 规范化的输出列名
    cols = ['time', 'action', 'coin', 'direction', 'event', 'price',
            'reason', 'target_weight', 'pnl', 'top_k', 'max_weight',
            'signal_timestamp_ms', 'STRATEGY_NAME', 'symbol']

    # 边界保护
    if kline_df is None or len(kline_df) == 0 or fr_df is None or len(fr_df) == 0:
        return pd.DataFrame(columns=cols)

    # 提取标的信息
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
    # 2. 数据加载与对齐 (基于左闭右开区间重采样)
    # ==========================================
    bar = f"{bar_minutes}min"

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

    fr = fr_df.copy()
    ft = _pick(fr, ['timestamp', 'fundingTime', 'time', 'ts'], 'fr')
    fc = _pick(fr, ['funding_rate', 'fundingRate', 'rate'], 'fr')
    fr['dt'] = pd.to_datetime(fr[ft], unit='ms', utc=True)
    _fr_raw = (fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float))
    fr_s = _fr_raw.resample(bar, label='left', closed='left').last()

    df = agg.copy()
    df['funding_rate'] = fr_s.reindex(df.index).ffill()
    start = df['funding_rate'].first_valid_index()
    if start is not None:
        df = df.loc[start:].copy()
    df['funding_rate'] = df['funding_rate'].ffill()
    df = df.dropna(subset=['funding_rate'])
    df = df[df['close'] > 0]

    if len(df) == 0:
        return pd.DataFrame(columns=cols)

    # ==========================================
    # 3. 核心指标计算 (严格复刻原逻辑)
    # ==========================================
    EPS = 1e-12
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))

    M = B(STRATEGY_PARAMS['M_HOURS'])
    N = B(STRATEGY_PARAMS['N_HOURS'])
    W = B(STRATEGY_PARAMS['W_DAYS'] * 24)
    mp = max(50, W // 5)  # 原逻辑 min_periods 要求

    o, h, l, c = df['open'], df['high'], df['low'], df['close']
    fr_series = df['funding_rate']
    pc = c.shift(1)

    # 计算 真实波幅(TR) 与 相对ATR百分比(atr_pct)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr_N = tr.rolling(N, min_periods=max(2, N // 2)).mean()
    atr_pct = atr_N / (c + EPS)

    # 滚动计算百分位 Rank (使用 W 作为历史窗口)
    rk_atr = atr_pct.rolling(W, min_periods=mp).rank(pct=True)
    fr_rank = fr_series.rolling(W, min_periods=mp).rank(pct=True)

    # ==========================================
    # 4. 生成客观独立信号
    # ==========================================

    # 信号1: VOL_LOW_TO_HIGH (波动率从极低突然放大)
    signal_vol_low_to_high = (rk_atr.shift(M) < STRATEGY_PARAMS['ATR_RANK_LOW_TH']) & \
                             (rk_atr > STRATEGY_PARAMS['ATR_RANK_HIGH_TH'])

    # 信号2: FR_RECOVERY_FROM_LOW (资金费率从极度悲观中拐头抬升)
    signal_fr_recovery_from_low = (fr_rank.shift(M) < STRATEGY_PARAMS['FR_RANK_LOW_TH']) & \
                                  (fr_rank > fr_rank.shift(1))

    df['signal_long_open'] = signal_vol_low_to_high.fillna(False)
    df['signal_long_close'] = signal_fr_recovery_from_low.fillna(False)

    # ==========================================
    # 5. 状态机撮合模拟 (生成规范化事件流)
    # ==========================================
    records = []
    in_pos = False
    entry_price = 0.0

    for i in range(len(df) - 1):
        if not in_pos and df['signal_long_open'].iloc[i]:
            in_pos = True

            # 信号在当前bar(i)产生，下一个bar(i+1)开盘价执行
            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            entry_price = float(df['open'].iloc[exec_idx])

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            entry_reason = f"VOL_LOW_TO_HIGH(rk_atr_{STRATEGY_PARAMS['M_HOURS']}h_ago<0.2 & curr>0.6)"
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

        elif in_pos and df['signal_long_close'].iloc[i]:
            in_pos = False

            # 平仓同样在下一个bar(i+1)开盘价执行
            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            exit_price = float(df['open'].iloc[exec_idx])

            pnl = (exit_price / entry_price) - 1.0
            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            exit_reason = f"FR_RECOVERY_FROM_LOW(rk_fr_{STRATEGY_PARAMS['M_HOURS']}h_ago<0.1 & curr>prev)"
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

    # 强制平仓收尾(如果到数据末尾依然有未平仓位)
    if in_pos:
        exec_time_dt = df.index[-1]
        exit_price = float(df['close'].iloc[-1])  # 最后一根取收盘价收尾

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
    trades_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\factor_out_5m_debugtest\trades_AIOT.csv.gz')
    filtered_trades_df = trades_df[
        (trades_df["entry_factor"] == "VOL_LOW_TO_HIGH") &
        (trades_df["exit_factor"] == "FR_RECOVERY_FROM_LOW") &
        (trades_df["direction"] == "Long") &
        (trades_df["filter_mode"] == "original")
        ].copy()


    kline_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_5m_latest.csv'
    fr_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_funding_latest.csv'

    try:
        kline_df = pd.read_csv(kline_file_path)
        # 为 kline_df 补充 symbol 属性 (模拟实盘数据源结构)
        kline_df['symbol'] = 'AIOT/USDT'

        fr_df = pd.read_csv(fr_file_path)
        fr_df['datetime'] = pd.to_datetime(fr_df['timestamp'], unit='ms', utc=True)

        print("正在处理数据与回测...")
        trade_records_df = generate_vol_fr_signals(kline_df, fr_df, bar_minutes=5)

        print(f"回测执行成功！共生成 {len(trade_records_df)} 条事件记录。\n")
        if not trade_records_df.empty:
            # 打印前几条查看规范化后的数据结构
            print(trade_records_df[['time', 'action', 'event', 'price', 'pnl', 'reason']].head())

    except Exception as e:
        print(f"执行出错: {e}")