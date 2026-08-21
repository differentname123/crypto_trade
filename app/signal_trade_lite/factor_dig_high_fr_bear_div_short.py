import pandas as pd
import numpy as np


def generate_short_fr_signals(kline_df, fr_df, bar_minutes=15):
    """
    高度内聚的回测与信号生成函数：
    根据绝对费率极值开空，费率与价格顶背离平空。
    """

    # ==========================================
    # 1. 策略参数配置
    # ==========================================
    STRATEGY_PARAMS = {
        'N_HOURS': 24,  # 价格与费率背离的回溯窗口(小时)
        'FR_ABS_TH': 0.001,  # 绝对单期资金费率极值阈值 (0.1%)
        'TARGET_WEIGHT': 1.0,  # 目标仓位
        'MAX_WEIGHT': 1.0,  # 最大允许仓位
        'STRATEGY_NAME': 'short_high_fr_bear_div'
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

    # 处理 资金费率数据
    fr = fr_df.copy()
    ft = _pick(fr, ['timestamp', 'fundingTime', 'time', 'ts'], 'fr')
    fc = _pick(fr, ['funding_rate', 'fundingRate', 'rate'], 'fr')
    fr['dt'] = pd.to_datetime(fr[ft], unit='ms', utc=True)
    _fr_raw = (fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float))
    fr_s = _fr_raw.resample(bar, label='left', closed='left').last()

    # 合并对齐
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
    # 3. 核心指标与信号计算 (复刻原始框架代码)
    # ==========================================
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))
    N = B(STRATEGY_PARAMS['N_HOURS'])

    c = df['close']
    fr_series = df['funding_rate']

    # 信号: FR_ABSOLUTE_HIGH_POS (开仓信号 - 资金费率绝对值极高)
    df['signal_open'] = fr_series > STRATEGY_PARAMS['FR_ABS_TH']

    # 信号: FR_PRICE_BEAR_DIV (平仓信号 - 价格新高但费率下降的顶背离)
    df['signal_close'] = (c > c.shift(N)) & (fr_series < fr_series.shift(N))

    # 填充缺失值为 False
    df['signal_open'] = df['signal_open'].fillna(False)
    df['signal_close'] = df['signal_close'].fillna(False)

    # ==========================================
    # 4. 状态机撮合模拟 (做空机制)
    # ==========================================
    records = []
    in_pos = False
    entry_price = 0.0

    for i in range(len(df) - 1):
        if not in_pos and df['signal_open'].iloc[i]:
            in_pos = True

            # 假定于下一根K线开盘成交
            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            entry_price = float(df['open'].iloc[exec_idx])

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            open_reason = f"FR_ABSOLUTE_HIGH_POS(fr>{STRATEGY_PARAMS['FR_ABS_TH']})"
            records.append({
                'time': dt_bj_str,
                'action': 'SELL',  # 开空动作为 SELL
                'coin': coin_name,
                'direction': 'SHORT',  # 方向改为 SHORT
                'event': 'OPEN',
                'price': entry_price,
                'reason': open_reason,
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

            # 做空收益计算公式：1.0 - (平仓价 / 开仓价)
            pnl = 1.0 - (exit_price / entry_price)

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            close_reason = f"FR_PRICE_BEAR_DIV(24h)"
            records.append({
                'time': dt_bj_str,
                'action': 'BUY',  # 平空动作为 BUY
                'coin': coin_name,
                'direction': 'SHORT',
                'event': 'CLOSE',
                'price': exit_price,
                'reason': close_reason,
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

        # 做空收益计算公式
        pnl = 1.0 - (exit_price / entry_price)

        signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
        dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

        records.append({
            'time': dt_bj_str,
            'action': 'BUY',  # 平空动作为 BUY
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
        (trades_df["entry_factor"] == "FR_ABSOLUTE_HIGH_POS") &
        (trades_df["exit_factor"] == "FR_PRICE_BEAR_DIV") &
        (trades_df["direction"] == "Short") &
        (trades_df["filter_mode"] == "original")
        ].copy()


    kline_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_15m_latest.csv'
    fr_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\AIOT_USDT_USDT_funding_latest.csv'

    try:
        kline_df = pd.read_csv(kline_file_path)
        # 为 kline_df 补充 symbol 属性 (模拟实盘数据源结构)
        kline_df['symbol'] = 'AIOT/USDT'

        fr_df = pd.read_csv(fr_file_path)
        fr_df['datetime'] = pd.to_datetime(fr_df['timestamp'], unit='ms', utc=True)

        print("正在处理数据与回测...")
        trade_records_df = generate_short_fr_signals(kline_df, fr_df, bar_minutes=15)

        print(f"回测执行成功！共生成 {len(trade_records_df)} 条事件记录。\n")
        if not trade_records_df.empty:
            # 打印前几条查看规范化后的数据结构
            print(trade_records_df[['time', 'action', 'event', 'price', 'pnl', 'reason']].head())

    except Exception as e:
        print(f"执行出错: {e}")