import pandas as pd
import numpy as np


def preprocess_kline(df, bar_minutes=1):
    """
    函数1：数据预处理与重采样 (完全复刻原代码 load_symbol 逻辑)

    参数:
    df (pd.DataFrame): 原始 1m 级别的 DataFrame，需包含 ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    bar_minutes (int): 目标 K 线周期，默认为 1。如果设为 15，则会自动重采样为 15m。

    返回:
    pd.DataFrame: 处理后的标准时序 DataFrame
    """
    df = df.copy()

    # 1. 转换时间戳并设为索引 (处理 image_c54f0b.png 中的 13位毫秒级 timestamp)
    if 'timestamp' in df.columns:
        df['dt'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df = df.drop_duplicates(subset=['timestamp']).sort_values('dt').set_index('dt')

    # 2. 如果指定了大于1的周期，完全按照原代码逻辑进行左闭左开重采样
    if bar_minutes > 1:
        bar_str = f"{bar_minutes}min"
        df = df.resample(bar_str, label='left', closed='left').agg(
            open=('open', 'first'),
            high=('high', 'max'),
            low=('low', 'min'),
            close=('close', 'last'),
            volume=('volume', 'sum')
        )

        # 严格执行原代码的脏数据清理与向前填充逻辑
        df['close'] = df['close'].ffill()
        df = df[df['close'].notna()]
        df['open'] = df['open'].fillna(df['close'])
        df['high'] = df['high'].fillna(df['close'])
        df['low'] = df['low'].fillna(df['close'])
        df['volume'] = df['volume'].fillna(0.0)

    return df


def generate_ma_signals(df, bar_minutes=1):
    """
    函数2：均线出场信号生成 (完全复刻原代码 build_factors 和 make_params 逻辑)

    参数:
    df (pd.DataFrame): 经过 preprocess_kline 处理过的 DataFrame
    bar_minutes (int): 当前 df 的 K线周期，必须与预处理时的周期一致，用于精确换算均线参数

    返回:
    pd.DataFrame: 包含原 df 数据以及两个布尔型信号列的结果
    """
    # 1. 动态参数体系复刻 (make_params)
    bph = 60.0 / bar_minutes

    def B(hours):
        return max(1, int(round(hours * bph)))

    # 换算对应小时数所需要的 K线根数
    H24 = B(24)
    H48 = B(48)
    H72 = B(72)
    H168 = B(168)  # 7天

    c = df['close']

    # 2. 核心计算算子复刻
    def MA(n):
        # min_periods 严格按照原代码：max(2, n // 2)
        return c.rolling(n, min_periods=max(2, n // 2)).mean()

    def CD(a, b):
        # 死叉判定逻辑：当前 a < b，且上一根 K 线 a >= b
        return (a < b) & (a.shift(1) >= b.shift(1))

    # 3. 计算各个均线
    ma_24h = MA(H24)
    ma_48h = MA(H48)
    ma_72h = MA(H72)

    ma_fast = ma_48h
    ma_slow = MA(H168)

    # 4. 生成目标因子 (输出布尔值)
    # 因子1：多均线共振跌破
    exit_multi_ma_break = (c < ma_24h) & (c < ma_48h) & (c < ma_72h)

    # 因子2：快慢线死叉
    exit_ma_dead_cross = CD(ma_fast, ma_slow)

    # 5. 拼装结果并返回
    result_df = df.copy()
    # 使用 fillna(False) 防止因滚动窗口初始 NaN 值引发的类型转换错误，严格保持 bool 类型
    result_df['ENTRY_SIGNAL'] = exit_multi_ma_break.fillna(False).astype(bool)
    result_df['EXIT_SIGNAL'] = exit_ma_dead_cross.fillna(False).astype(bool)

    return result_df


if __name__ == "__main__":
    trades_df = pd.read_csv(r"W:\project\python_project\crypto_trade\app\factor_dig\factor_out_1m_debug\trades_BTC.csv.gz")

    TARGET_BAR_MINUTES = 1
    raw_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\data\BTC_USDT_USDT_1m_kline.csv')
    print(f"\n1. 开始清洗数据并重采样至 {TARGET_BAR_MINUTES} 分钟级别...")
    processed_df = preprocess_kline(raw_df, bar_minutes=TARGET_BAR_MINUTES)

    print(f"2. 开始计算基于 {TARGET_BAR_MINUTES} 分钟的均线出场信号...")
    signals_df = generate_ma_signals(processed_df, bar_minutes=TARGET_BAR_MINUTES)