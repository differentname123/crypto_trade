import pandas as pd
import numpy as np


def generate_short_fr_signals(kline_df, fr_df, bar_minutes=15):
    """
    高度内聚的回测与信号生成函数 (做空策略)：
    - 开仓信号: EXIT_FR_EXTREME_HIGH (极端高费率，散户多头极其拥挤) -> 做空
    - 平仓信号: FR_COLD_START (24h价格已强势拉升且费率未热/冷启动) -> 平空
    """

    # ==========================================
    # 1. 策略参数配置 (严格对齐原代码逻辑)
    # ==========================================
    STRATEGY_PARAMS = {
        'N_HOURS': 24,  # 动量回溯周期(24小时)
        'W_DAYS': 14,  # 排名滚动窗口(14天)
        'ENTRY_FR_RANK_THRESHOLD': 0.95,  # 做空入场: 资金费率处于14天极高位 (>95%)
        'EXIT_RET_RANK_THRESHOLD': 0.80,  # 平空出场条件1: 24h收益率处于极高位 (>80%)
        'EXIT_FR_RANK_THRESHOLD': 0.50,  # 平空出场条件2: 资金费率处于中低位 (<50%)
        'TARGET_WEIGHT': 1.0,  # 目标名义仓位
        'MAX_WEIGHT': 1.0,  # 最大允许仓位
        'STRATEGY_NAME': 'extreme_fr_short_cold_start_close'
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
    # 2. 数据加载与对齐 (与原框架一致)
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
    # 3. 核心指标与信号计算 (复刻原代码物理学逻辑)
    # ==========================================
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))

    N = B(STRATEGY_PARAMS['N_HOURS'])  # 24小时对应的 Bar 数
    W = B(STRATEGY_PARAMS['W_DAYS'] * 24)  # 14天对应的 Bar 数
    mp = max(50, W // 5)  # 最小计算窗口 (防启动期失真)

    c = df['close']
    fr_series = df['funding_rate']

    # --- 计算 24小时收益率及排名 ---
    ret_N = c.pct_change(N)
    rk_ret_N = ret_N.rolling(W, min_periods=mp).rank(pct=True)

    # --- 计算 资金费率排名 ---
    fr_rank = fr_series.rolling(W, min_periods=mp).rank(pct=True)

    # 信号 A (做空入场): EXIT_FR_EXTREME_HIGH (原出场因子改作入场)
    df['signal_entry'] = fr_rank > STRATEGY_PARAMS['ENTRY_FR_RANK_THRESHOLD']

    # 信号 B (平空出场): FR_COLD_START (冷启动: 24h强势拉升 + 费率未热)
    df['signal_exit'] = (rk_ret_N > STRATEGY_PARAMS['EXIT_RET_RANK_THRESHOLD']) & \
                        (fr_rank < STRATEGY_PARAMS['EXIT_FR_RANK_THRESHOLD'])

    df['signal_entry'] = df['signal_entry'].fillna(False)
    df['signal_exit'] = df['signal_exit'].fillna(False)

    # ==========================================
    # 4. 状态机撮合模拟 (生成做空事件流)
    # ==========================================
    records = []
    in_pos = False
    entry_price = 0.0

    for i in range(len(df) - 1):
        # 触发做空开仓信号
        if not in_pos and df['signal_entry'].iloc[i]:
            in_pos = True

            # 假定于下一根K线开盘成交
            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            entry_price = float(df['open'].iloc[exec_idx])

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            entry_reason = f"EXIT_FR_EXTREME_HIGH(fr_rank>{STRATEGY_PARAMS['ENTRY_FR_RANK_THRESHOLD']})"
            records.append({
                'time': dt_bj_str,
                'action': 'SELL',  # 做空开仓
                'coin': coin_name,
                'direction': 'SHORT',  # 方向标记为做空
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

        # 触发做空平仓信号
        elif in_pos and df['signal_exit'].iloc[i]:
            in_pos = False

            exec_idx = i + 1
            exec_time_dt = df.index[exec_idx]
            exit_price = float(df['open'].iloc[exec_idx])

            # 做空收益计算 (未计手续费与滑点，此处采用 U本位 基础公式)
            pnl = 1.0 - (exit_price / entry_price)

            signal_ts_ms = int(exec_time_dt.timestamp() * 1000)
            dt_bj_str = exec_time_dt.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

            exit_reason = f"FR_COLD_START(ret24_rank>{STRATEGY_PARAMS['EXIT_RET_RANK_THRESHOLD']}&fr_rank<{STRATEGY_PARAMS['EXIT_FR_RANK_THRESHOLD']})"
            records.append({
                'time': dt_bj_str,
                'action': 'BUY',  # 买入平空
                'coin': coin_name,
                'direction': 'SHORT',
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

    # ==========================================
    # 5. 强制平仓收尾 (回测结束时清理持仓)
    # ==========================================
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
    trades_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\factor_out_30m_debug\trades_ACU.csv.gz')
    filtered_trades_df = trades_df[
        (trades_df["entry_factor"] == "EXIT_FR_EXTREME_HIGH") &
        (trades_df["exit_factor"] == "FR_COLD_START") &
        (trades_df["direction"] == "Short") &
        (trades_df["filter_mode"] == "original")
        ].copy()


    kline_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\ACU_USDT_USDT_30m_latest.csv'
    fr_file_path = r'W:\project\python_project\crypto_trade\app\signal_trade_lite\data\ACU_USDT_USDT_funding_latest.csv'

    try:
        kline_df = pd.read_csv(kline_file_path)
        # 为 kline_df 补充 symbol 属性 (模拟实盘数据源结构)
        kline_df['symbol'] = 'ACU/USDT'

        fr_df = pd.read_csv(fr_file_path)
        fr_df['datetime'] = pd.to_datetime(fr_df['timestamp'], unit='ms', utc=True)

        print("正在处理数据与回测...")
        trade_records_df = generate_short_fr_signals(kline_df, fr_df, bar_minutes=30)

        print(f"回测执行成功！共生成 {len(trade_records_df)} 条事件记录。\n")
        if not trade_records_df.empty:
            # 打印前几条查看规范化后的数据结构
            print(trade_records_df[['time', 'action', 'event', 'price', 'pnl', 'reason']].head())

    except Exception as e:
        print(f"执行出错: {e}")