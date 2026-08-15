import pandas as pd
import numpy as np


def preprocess_kline(df, bar_minutes=1):
    """
    函数1：数据预处理与重采样 (完全复刻原代码 load_symbol 逻辑)

    参数:
    df (pd.DataFrame): 原始 1m 级别的 DataFrame，需包含 ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    bar_minutes (int): 目标 K 线周期，默认为 1。如果设为 15，则会自动重采样为 15m。

    返回:
    pd.DataFrame: 处理后的标准时序 DataFrame，保留所有原始列。
    """
    df = df.copy()

    # 1. 转换时间戳并设为索引 (处理 13位毫秒级 timestamp)
    if 'timestamp' in df.columns:
        df['dt'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df = df.drop_duplicates(subset=['timestamp']).sort_values('dt').set_index('dt')

    # 2. 如果指定了大于1的周期，进行重采样
    if bar_minutes > 1:
        bar_str = f"{bar_minutes}min"

        # 预先定义已知的标准聚合规则
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }

        # 对于原始 df 中存在，但不在默认聚合规则中的其他列，默认取 'last'
        for col in df.columns:
            if col not in agg_dict and col != 'timestamp':  # timestamp 会在重采样后作为值保留的话容易混淆，但如果需要保留也可以取 last
                agg_dict[col] = 'last'

        # 执行重采样
        df = df.resample(bar_str, label='left', closed='left').agg(agg_dict)

        # 严格执行原代码的脏数据清理与向前填充逻辑
        if 'close' in df.columns:
            df['close'] = df['close'].ffill()
            df = df[df['close'].notna()]
            df['open'] = df['open'].fillna(df['close'])
            df['high'] = df['high'].fillna(df['close'])
            df['low'] = df['low'].fillna(df['close'])
        if 'volume' in df.columns:
            df['volume'] = df['volume'].fillna(0.0)

    # 3. 新增 timestamp_utc 字段（将 UTC 时间索引转化为可读字符串）
    df['timestamp_utc'] = df.index.strftime('%Y-%m-%d %H:%M:%S')

    return df



def generate_ma_signals(df, bar_minutes=1):
    """
    函数2：均线信号生成 (包含新增的 rank_loss_24h 入场条件)

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

    # 4. 获取 rank_loss_24h 字段，空值或 nan 默认填充为 999
    if 'rank_loss_24h' in df.columns:
        rank_loss = df['rank_loss_24h'].fillna(999)
    else:
        # 防御性设置：如果原始df恰好没有传入该列，默认赋值为999，避免报错
        rank_loss = pd.Series(999, index=df.index)

    # 5. 生成目标因子 (输出布尔值)
    # 因子1：多均线共振跌破，且 rank_loss_24h <= 5 作为最终的入场信号
    entry_condition = (c < ma_24h) & (c < ma_48h) & (c < ma_72h)

    # 因子2：快慢线死叉作为出场信号
    exit_ma_dead_cross = CD(ma_fast, ma_slow)

    # 6. 拼装结果并返回
    result_df = df.copy()
    # 使用 fillna(False) 防止因滚动窗口初始 NaN 值引发的类型转换错误，严格保持 bool 类型
    result_df['ENTRY_SIGNAL'] = entry_condition.fillna(False).astype(bool)
    result_df['EXIT_SIGNAL'] = exit_ma_dead_cross.fillna(False).astype(bool)

    return result_df

def generate_trades_df(df):
    """
    函数3：根据信号生成交易记录 DataFrame
    """
    trades = []
    in_position = False

    entry_time = None
    entry_price = None

    # 提取底层 numpy 数组及格式化时间以加速循环
    # pd.to_datetime 解析后已经是 utc=True 的格式，strftime 直接提取可读形式
    times = df.index.strftime('%Y-%m-%d %H:%M:%S').tolist()
    opens = df['open'].values
    entry_signals = df['ENTRY_SIGNAL'].values
    exit_signals = df['EXIT_SIGNAL'].values

    n = len(df)
    # 遍历至倒数第二个（以确保能取到次根K线即 i+1 作为执行K线）
    for i in range(n - 1):
        if not in_position:
            # 如果不在场内，且出现入场信号
            if entry_signals[i]:
                entry_time = times[i]  # 严格使用信号产生的那根 Bar 的时间
                entry_price = opens[i + 1]  # 价格依旧取下一根 Bar 的开盘价
                in_position = True
        else:
            # 如果在场内，且出现出场信号
            if exit_signals[i]:
                exit_time = times[i]  # 严格使用信号产生的那根 Bar 的时间
                exit_price = opens[i + 1]  # 价格依旧取下一根 Bar 的开盘价

                # 计算单笔交易收益率 (默认按照做多逻辑计算)
                # 注：如果你的策略是做空，请将此处改为 (entry_price - exit_price) / entry_price
                trade_return = (exit_price - entry_price) / entry_price

                trades.append({
                    'entry_time': entry_time,
                    'exit_time': exit_time,
                    'entry_price': entry_price,
                    'exit_price': exit_price,
                    'return': trade_return
                })
                # 出场后重置状态机
                in_position = False

    # 构造要求的 DataFrame
    trades_df = pd.DataFrame(trades, columns=[
        'entry_time', 'exit_time', 'entry_price', 'exit_price', 'return'
    ])

    return trades_df
def generate_multi_ma_signals(raw_df, bar_minutes=5):
    """
    深度精简版：多均线共振破位策略 (高度内聚 3合1 函数)

    重构亮点：
    1. 参数高度解耦：将核心均线周期、权重等参数提取至顶部，一目了然，方便后期回测调参。
    2. 极速状态机：利用向量化计算与事件驱动机制，跳过无用遍历，极大提升运算速度。
    3. 撮合逻辑调整：【按照最新需求】，执行价格取“信号触发当根K线的收盘价”。
    """

    # ====================================================
    # 0. 策略最优参数与基础设置 (解耦提取区)
    # ====================================================
    STRATEGY_PARAMS = {
        # --- 入场条件参数 ---
        'ENTRY_MA_HOURS': [24, 48, 72],  # 入场判定：价格需同时跌破这3根均线(小时)

        # --- 出场条件参数 ---
        'EXIT_FAST_MA_HOURS': 48,  # 出场判定：快线均线周期(小时)
        'EXIT_SLOW_MA_HOURS': 168,  # 出场判定：慢线均线周期(小时)，168代表7天

        # --- 仓位与风控参数 ---
        'TARGET_WEIGHT': 1.0,  # 触发信号后的目标仓位
        'MAX_WEIGHT': 0.14  # 策略设定的最大允许仓位限制
    }

    cols = ['time', 'action', 'coin', 'direction', 'event', 'price',
            'reason', 'target_weight', 'pnl', 'top_k', 'max_weight',
            'signal_timestamp_ms', 'STRATEGY_NAME', 'symbol']

    if raw_df is None or len(raw_df) == 0:
        return [], pd.DataFrame(columns=cols)

    df = raw_df.copy()

    # 提取币种信息
    symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else df.attrs.get('symbol', 'UNKNOWN')
    coin_name = df['coin_name'].iloc[0] if 'coin_name' in df.columns else (
        symbol.split('/')[0] if '/' in symbol else symbol)

    # ====================================================
    # 1. 数据预处理与重采样
    # ====================================================
    if 'timestamp' in df.columns:
        df['dt'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True)
        df = df.drop_duplicates(subset=['timestamp']).sort_values('dt').set_index('dt')

    if bar_minutes > 1:
        bar_str = f"{bar_minutes}min"
        agg_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}

        for col in df.columns:
            if col not in agg_dict and col != 'timestamp':
                agg_dict[col] = 'last'

        df = df.resample(bar_str, label='left', closed='left').agg(agg_dict)

        if 'close' in df.columns:
            df['close'] = df['close'].ffill()
            df = df[df['close'].notna()]
            df['open'] = df['open'].fillna(df['close'])
            df['high'] = df['high'].fillna(df['close'])
            df['low'] = df['low'].fillna(df['close'])
        if 'volume' in df.columns:
            df['volume'] = df['volume'].fillna(0.0)

    # ====================================================
    # 2. 核心指标与信号向量化计算
    # ====================================================
    # K线周期转小时系数
    bph = 60.0 / bar_minutes

    def B(hours):
        return max(1, int(round(hours * bph)))

    c = df['close']

    def MA(n):
        return c.rolling(n, min_periods=max(2, n // 2)).mean()

    # 根据顶部配置项，动态计算均线 (将小时数转换为K线根数)
    entry_h1, entry_h2, entry_h3 = STRATEGY_PARAMS['ENTRY_MA_HOURS']
    ma_entry_1 = MA(B(entry_h1))
    ma_entry_2 = MA(B(entry_h2))
    ma_entry_3 = MA(B(entry_h3))

    ma_fast = MA(B(STRATEGY_PARAMS['EXIT_FAST_MA_HOURS']))
    ma_slow = MA(B(STRATEGY_PARAMS['EXIT_SLOW_MA_HOURS']))

    # 理论信号计算 (布尔矩阵)
    # 入场：价格同时跌破配置的3根均线
    entry_signal = (c < ma_entry_1) & (c < ma_entry_2) & (c < ma_entry_3)
    # 出场：快线与慢线形成死叉 (当前周期快线<慢线，且上一周期快线>=慢线)
    exit_signal = (ma_fast < ma_slow) & (ma_fast.shift(1) >= ma_slow.shift(1))

    entry_signal = entry_signal.fillna(False)
    exit_signal = exit_signal.fillna(False)

    # ====================================================
    # 3. 极速事件驱动状态机
    # ====================================================
    signals = []
    actual_signals_list = []

    # 直接提取所有发出信号的事件点（移除了下一根K线的非空判定，因为现在当根收盘价就能成交）
    valid_event_mask = entry_signal | exit_signal
    event_indices = df.index[valid_event_mask]

    actual_pos = 0  # 0: 空仓, 1: 持有多单
    actual_entry_price = 0.0

    for idx in event_indices:
        is_entry = entry_signal.at[idx]
        is_exit = exit_signal.at[idx]

        dt_utc_str = idx.strftime('%Y-%m-%d %H:%M:%S')
        dt_bj = idx.tz_convert('Asia/Shanghai').tz_localize(None)
        signal_ts_ms = int(idx.timestamp() * 1000)

        # 【核心改动】：执行价格直接取当前触发信号的K线收盘价
        exec_price = c.at[idx]

        # 动态提取日志原因
        entry_reason = f"均线跌破(C<{ma_entry_1.at[idx]:.4f}, C<{ma_entry_2.at[idx]:.4f}, C<{ma_entry_3.at[idx]:.4f})"
        exit_reason = f"快慢死叉(MA{STRATEGY_PARAMS['EXIT_FAST_MA_HOURS']} < MA{STRATEGY_PARAMS['EXIT_SLOW_MA_HOURS']})"

        # --- A. 记录所有理论信号 (signals) ---
        if is_entry:
            signals.append({
                'symbol': symbol, 'signal_type': '🟢 ENTRY (多均线跌破开多)', 'datetime_bj': dt_bj,
                'price': exec_price, 'reason': entry_reason
            })
        if is_exit:
            signals.append({
                'symbol': symbol, 'signal_type': '🔴 EXIT (快慢线死叉平多)', 'datetime_bj': dt_bj,
                'price': exec_price, 'reason': exit_reason
            })

        # --- B. 记录受持仓状态机控制的实际操作信号 ---
        if actual_pos == 0 and is_entry:
            actual_pos = 1
            actual_entry_price = exec_price

            actual_signals_list.append({
                'time': dt_utc_str, 'action': 'BUY', 'coin': coin_name, 'direction': 'LONG',
                'event': 'OPEN', 'price': actual_entry_price, 'reason': entry_reason,
                'target_weight': STRATEGY_PARAMS['TARGET_WEIGHT'],
                'pnl': None, 'top_k': 1,
                'max_weight': STRATEGY_PARAMS['MAX_WEIGHT'],
                'signal_timestamp_ms': signal_ts_ms, 'STRATEGY_NAME': 'multi_ma_break_long', 'symbol': symbol
            })

        elif actual_pos == 1 and is_exit:
            actual_pos = 0
            pnl_pct_actual = ((exec_price - actual_entry_price) / actual_entry_price) * 100

            actual_signals_list.append({
                'time': dt_utc_str, 'action': 'SELL', 'coin': coin_name, 'direction': 'LONG',
                'event': 'CLOSE', 'price': exec_price, 'reason': exit_reason,
                'target_weight': 0.0,
                'pnl': pnl_pct_actual, 'top_k': 1,
                'max_weight': STRATEGY_PARAMS['MAX_WEIGHT'],
                'signal_timestamp_ms': signal_ts_ms, 'STRATEGY_NAME': 'multi_ma_break_long', 'symbol': symbol
            })

    # ====================================================
    # 4. 组装返回
    # ====================================================
    df_actual_signals = pd.DataFrame(actual_signals_list, columns=cols)
    return signals, df_actual_signals

if __name__ == "__main__":
    TARGET_BAR_MINUTES = 5


    # trades_df = pd.read_csv(
    #     rf"W:\project\python_project\crypto_trade\app\factor_dig\factor_out_{TARGET_BAR_MINUTES}m_debug\trades_MYX.csv.gz")
    #
    # filtered_df = trades_df[
    #     (trades_df["entry_factor"] == "EXIT_MULTI_MA_BREAK") &
    #     (trades_df["exit_factor"] == "EXIT_MA_DEAD_CROSS") &
    #     (trades_df["direction"] == "Long")
    #     &(trades_df["filter_mode"] == "bottom_5")
    #     ].copy()
    # # 打印return的和
    # filtered_df_return = filtered_df["return"].sum() * 100


    raw_df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\data\MYX_USDT_USDT_1m_kline.csv')
    print(f"\n1. 开始清洗数据并重采样至 {TARGET_BAR_MINUTES} 分钟级别...")
    processed_df = preprocess_kline(raw_df, bar_minutes=TARGET_BAR_MINUTES)

    print(f"2. 开始计算基于 {TARGET_BAR_MINUTES} 分钟的均线出场信号...")
    signals_df = generate_ma_signals(processed_df, bar_minutes=TARGET_BAR_MINUTES)

    print(f"3. 开始根据交易信号生成交易流水列表 trades_df...")
    final_trades_df = generate_trades_df(signals_df)
    final_trades_df_return = final_trades_df["return"].sum() * 100

    signals, df_actual_signals = generate_multi_ma_signals(raw_df, bar_minutes=TARGET_BAR_MINUTES)
    print("\n[交易数据结果概览]:")
    print(final_trades_df.head())
    print()