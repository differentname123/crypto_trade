import numpy as np
import pandas as pd
import warnings

warnings.filterwarnings("ignore")


# ==========================================
# 1. 数据加载与对齐 (完全提取自原代码的 load_symbol)
# ==========================================
def _pick(df, cands, what):
    for c in cands:
        if c in df.columns:
            return c
    raise KeyError(f"[{what}] 找不到列 {cands}，实际列: {list(df.columns)}")


def load_and_align_data(kline_file, fr_file, bar_minutes=15):
    bar = f"{bar_minutes}min"

    # 1. 处理 K线数据 & 重采样
    k = pd.read_csv(kline_file)
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

    # 2. 处理 资金费率数据 & 重采样
    fr = pd.read_csv(fr_file)
    ft = _pick(fr, ['timestamp', 'fundingTime', 'time', 'ts'], 'fr')
    fc = _pick(fr, ['funding_rate', 'fundingRate', 'rate'], 'fr')
    fr['dt'] = pd.to_datetime(fr[ft], unit='ms', utc=True)
    _fr_raw = (fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float))
    fr_s = _fr_raw.resample(bar, label='left', closed='left').last()

    # 3. 数据合并对齐
    df = agg.copy()
    df['funding_rate'] = fr_s.reindex(df.index).ffill()

    # 截取资金费率有数据的有效区间
    start = df['funding_rate'].first_valid_index()
    if start is not None:
        df = df.loc[start:].copy()
    df['funding_rate'] = df['funding_rate'].ffill()
    df = df.dropna(subset=['funding_rate'])
    df = df[df['close'] > 0]

    return df


# ==========================================
# 2. 回测主逻辑 (复刻原始框架)
# ==========================================
def backtest_custom_combo(df: pd.DataFrame, bar_minutes: int = 15,
                          fee_rate: float = 0.0005, slippage: float = 0.0005) -> pd.DataFrame:
    # 构造参数体系 (复刻 make_params)
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))

    M = B(4)  # 4小时对应的 bar 数
    W = B(24 * 14)  # 14天对应的 bar 数
    mp = max(50, W // 5)  # min_periods 最小滚动周期阈值

    c = df['close']
    fr = df['funding_rate']

    # --- 信号 A: EXIT_SHORT_SURGE_EXTREME (作为做多入场) ---
    ret_M = c.pct_change(M)
    rk_ret_M = ret_M.rolling(W, min_periods=mp).rank(pct=True)
    df['signal_entry'] = rk_ret_M > 0.98

    # --- 信号 B: FR_LOW_NEG (作为做多出场) ---
    fr_rank = fr.rolling(W, min_periods=mp).rank(pct=True)
    df['signal_exit'] = (fr_rank < 0.20) | (fr < 0)

    # 填充 NaN 脏数据区域
    df['signal_entry'] = df['signal_entry'].fillna(False)
    df['signal_exit'] = df['signal_exit'].fillna(False)

    # --- 状态机撮合模拟 ---
    trades = []
    in_pos = False
    entry_idx = -1

    for i in range(len(df) - 1):
        if not in_pos and df['signal_entry'].iloc[i]:
            in_pos = True
            entry_idx = i

        elif in_pos and df['signal_exit'].iloc[i]:
            in_pos = False
            exit_idx = i

            # 成交假定于产生信号后的 下一根 K线开盘
            entry_exec_time = df.index[entry_idx + 1]
            entry_price = df['open'].iloc[entry_idx + 1]
            exit_exec_time = df.index[exit_idx + 1]
            exit_price = df['open'].iloc[exit_idx + 1]

            # 计算收益 (剥离双边手续费与滑点)
            net_return = (exit_price / entry_price) - 1.0 - 2.0 * (fee_rate + slippage)

            trades.append({
                'entry_signal_time': df.index[entry_idx],
                'entry_exec_time': entry_exec_time,
                'entry_price': entry_price,
                'exit_signal_time': df.index[exit_idx],
                'exit_exec_time': exit_exec_time,
                'exit_price': exit_price,
                'net_return': net_return,
                'hold_bars': exit_idx - entry_idx
            })

    # 强制平仓 (FORCE_CLOSE_AT_END 逻辑)
    if in_pos:
        exit_idx = len(df) - 1
        entry_price = df['open'].iloc[entry_idx + 1] if entry_idx + 1 < len(df) else df['close'].iloc[-1]
        exit_price = df['close'].iloc[-1]
        net_return = (exit_price / entry_price) - 1.0 - 2.0 * (fee_rate + slippage)
        trades.append({
            'entry_signal_time': df.index[entry_idx],
            'entry_exec_time': df.index[entry_idx + 1] if entry_idx + 1 < len(df) else df.index[entry_idx],
            'entry_price': entry_price,
            'exit_signal_time': df.index[exit_idx],
            'exit_exec_time': df.index[exit_idx],
            'exit_price': exit_price,
            'net_return': net_return,
            'hold_bars': exit_idx - entry_idx
        })

    return pd.DataFrame(trades)


# ==========================================
# 3. 测试入口 (请修改这里的文件路径)
# ==========================================
if __name__ == "__main__":

    origin_pairs = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\extracted_raw_trades\extracted_target_pairs.csv')

    trades_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\factor_out_30m_debug\trades_AIOT.csv.gz')

    filtered_trades_df = trades_df[
        (trades_df["entry_factor"] == "EXIT_SHORT_SURGE_EXTREME") &
        (trades_df["exit_factor"] == "FR_LOW_NEG") &
        (trades_df["direction"] == "Long") &
        (trades_df["filter_mode"] == "original")
        ].copy()

    # 替换为你实际的 CSV 文件路径
    kline_file_path = r'W:\project\python_project\crypto_trade\app\data\AIOT_USDT_USDT_1m_kline.csv'
    # 注意：你需要补充资金费率的文件路径
    fr_file_path = r'W:\project\python_project\crypto_trade\app\data\AIOT_USDT_USDT_funding_rates.csv'

    print("正在加载数据与重采样对齐...")
    try:
        # 第一步：加载并清洗数据
        df_aligned = load_and_align_data(kline_file_path, fr_file_path, bar_minutes=30)

        print(f"数据加载完成，有效 K 线数量: {len(df_aligned)} 根")
        print("正在进行回测撮合...")

        # 第二步：执行自定义回测
        trade_records = backtest_custom_combo(df_aligned, bar_minutes=30)

        # 第三步：打印结果
        if len(trade_records) > 0:
            print("\n========== 交易流水前 5 笔 ==========")
            print(trade_records.head())
            print("\n========== 回测统计 ==========")
            print(f"总交易笔数: {len(trade_records)}")
            print(f"胜率: {(trade_records['net_return'] > 0).mean() * 100:.2f}%")
            print(f"累计净收益 (加总非复利): {trade_records['net_return'].sum() * 100:.2f}%")
            print(f"平均单笔收益: {trade_records['net_return'].mean() * 100:.2f}%")
        else:
            print("没有触发任何交易。")

    except Exception as e:
        print(f"\n执行出错: {e}")
