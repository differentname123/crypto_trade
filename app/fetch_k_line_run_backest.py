"""
=============================================================================
[功能摘要]：跨周期共振量化回测引擎 (5m级别持仓/资金费率因子 + 1m级别区间突破)

[输入数据]：
  - 5分钟级别K线CSV (需包含特征：持仓量oi_amount, 预测资金费率predicted_funding_rate)
  - 1分钟级别K线CSV (需包含特征：高开低收价格序列, 开盘时间戳)

[数据流转/交互]：
  1. 【提取】分别在5m和1m各自的上下文中计算滚动指标 (90% OI分位数、资金费率连跌、12h高低点)。
  2. 【对齐】将5m级别的“火药桶”就绪状态，通过时间戳索引合并并前向填充(ffill)至1m数据，实现跨周期共振。
  3. 【推演】回测引擎以1m数据为时间轴逐行推演，信号一旦触发，严格于下一根K线开盘(Open)进行市价撮合。

[输出数据]：
  - 结构化的交易日志流 (控制台输出)
  - 全局交易历史记录列表 (含进出场时间、价格、单笔真实盈亏)
=============================================================================
"""

import pandas as pd

# ==========================================
# 1. 策略核心参数配置区
# ==========================================
OI_LOOKBACK_DAYS = 30  # OI 历史分位数回溯天数
OI_PERCENTILE = 0.90  # OI 极端分位数阈值 (90%)
FR_THRESHOLD = -0.0005  # 资金费率极值阈值 (-0.05%)
FR_CONSEC_PERIODS = 3  # 资金费率连续满足条件的周期数

RANGE_LOOKBACK_HOURS = 12  # 1m数据：震荡区间回溯时间 (12小时)
FEE_RATE = 0.001  # 单边手续费/滑点成本 (0.1%)


# ==========================================
# 2. 数据处理与信号生成
# ==========================================
def load_and_prepare_data(df_5m_path, df_1m_path):
    """
    加载并融合双时间周期数据，生成底层交易信号。

    [输入 Shape 约束]:
      df_5m_path (str): CSV需含列 ['timestamp', 'oi_amount', 'predicted_funding_rate']
      df_1m_path (str): CSV需含列 ['timestamp' 或 'open_time', 'open', 'high', 'low', 'close']

    [输出 Shape 约束]:
      返回 DataFrame 需包含列 ['timestamp', 'open', 'close', 'high', 'low', 'long_trigger', 'exit_trigger', 'range_high', 'range_low']
    """
    print(
        f"[数据加载/初始化] 准备解析多周期K线数据 | 关键参数: 5m路径=【{df_5m_path}】, 1m路径=【{df_1m_path}】 | 结果: 【开始读取】")

    df_5m = pd.read_csv(df_5m_path)
    df_1m = pd.read_csv(df_1m_path)

    # --- 阶段 1：时间戳规范化 ---
    if "open_time" in df_1m.columns:
        df_1m["timestamp"] = pd.to_datetime(df_1m["open_time"], unit="ms", utc=True).dt.tz_convert(
            "Asia/Shanghai").dt.tz_localize(None)
    else:
        df_1m["timestamp"] = pd.to_datetime(df_1m["timestamp"])

    df_5m['timestamp'] = pd.to_datetime(df_5m['timestamp'])

    # 防御性排序：确保时光倒流不出错
    df_5m = df_5m.sort_values('timestamp').reset_index(drop=True)
    df_1m = df_1m.sort_values('timestamp').reset_index(drop=True)

    # --- 阶段 2：处理 5分钟 燃料池逻辑 ---
    oi_window = OI_LOOKBACK_DAYS * 24 * 12
    df_5m['oi_90_pct'] = df_5m['oi_amount'].rolling(window=oi_window, min_periods=288).quantile(OI_PERCENTILE)
    df_5m['condition_A'] = df_5m['oi_amount'] > df_5m['oi_90_pct']

    df_5m['is_negative_fr'] = df_5m['predicted_funding_rate'] < FR_THRESHOLD
    df_5m['condition_B'] = df_5m['is_negative_fr'].rolling(window=FR_CONSEC_PERIODS).sum() >= FR_CONSEC_PERIODS

    # 燃料池锁定状态 (核心防御：shift(1) 防止当前周期信号使用当前周期收盘才知晓的特征)
    df_5m['powder_keg_ready'] = (df_5m['condition_A'] & df_5m['condition_B']).shift(1)

    # --- 阶段 3：降维合并到 1分钟 K线 ---
    df_1m.set_index('timestamp', inplace=True)
    df_5m.set_index('timestamp', inplace=True)

    df_1m['powder_keg_ready'] = df_5m['powder_keg_ready'].reindex(df_1m.index).ffill().fillna(False)

    # --- 阶段 4：处理 1分钟 区间突破逻辑 ---
    range_window = RANGE_LOOKBACK_HOURS * 60

    df_1m['range_high'] = df_1m['high'].rolling(window=range_window, min_periods=range_window).max().shift(1)
    df_1m['range_low'] = df_1m['low'].rolling(window=range_window, min_periods=range_window).min().shift(1)

    df_1m['long_trigger'] = (df_1m['close'] > df_1m['range_high']) & df_1m['powder_keg_ready']
    df_1m['exit_trigger'] = df_1m['close'] < df_1m['range_low']

    print(
        f"[数据加载/特征融合] 跨周期因子计算完毕 | 关键参数: 产出K线总数=【{len(df_1m)}条】 | 结果: 【成功对齐，准备回测】")
    return df_1m.reset_index()


# ==========================================
# 3. 核心执行引擎
# ==========================================
def run_backtest(df_1m):
    """
    遍历行情进行状态机推演并埋点日志。

    [输入 Shape 约束]:
      df_1m (DataFrame): 需包含 load_and_prepare_data 产出的所有信号列

    [输出 Shape 约束]:
      trade_history (List[Dict]): 包含单笔交易明细的字典列表。核心 Key 包含 ['entry_time', 'exit_time', 'entry_price', 'exit_price', 'pnl_pct']
    """
    print("\n" + "=" * 50)
    print(f"[回测引擎/启动] 开始逐线推演 | 关键参数: 初始状态=【空仓等待】 | 结果: 【引擎运行中】")
    print("=" * 50)

    in_position = False
    entry_price = 0.0
    entry_time = None

    pending_long = False
    pending_exit = False

    trade_history = []
    records = df_1m.to_dict('records')

    for bar in records:
        timestamp = bar['timestamp']
        open_price = bar['open']
        close_price = bar['close']

        # ---------------- 动作流：执行积压订单 ----------------
        if pending_long and not in_position:
            entry_price = open_price * (1 + FEE_RATE)
            in_position = True
            entry_time = timestamp
            pending_long = False
            print(
                f"[回测引擎/建仓] 次周期市价做多 | 关键参数: 时间=【{timestamp}】, 开仓均价=【{entry_price:.4f}】(含税) | 结果: 【多单持有中】")

        elif pending_exit and in_position:
            exit_price = open_price * (1 - FEE_RATE)
            pnl_pct = (exit_price - entry_price) / entry_price
            trade_history.append({
                'entry_time': entry_time,
                'exit_time': timestamp,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'pnl_pct': pnl_pct
            })

            pnl_display = f"+{pnl_pct * 100:.2f}%" if pnl_pct > 0 else f"{pnl_pct * 100:.2f}%"
            print(
                f"[回测引擎/平仓] 支撑位破位清仓 | 关键参数: 时间=【{timestamp}】, 平仓均价=【{exit_price:.4f}】, 单笔盈亏=【{pnl_display}】 | 结果: 【重置为空仓】")

            in_position = False
            pending_exit = False
            entry_price = 0.0
            entry_time = None

        # ---------------- 信号流：监听行情突破 ----------------
        if not in_position:
            if bar['long_trigger']:
                pending_long = True
                print(
                    f"[回测引擎/信号] 捕获火药桶爆发点 | 关键参数: 时间=【{timestamp}】, 突破价=【{close_price:.4f}】, 历史高点阻力=【{bar['range_high']:.4f}】 | 结果: 【锁定次周期做多】")
        else:
            if bar['exit_trigger']:
                pending_exit = True
                print(
                    f"[回测引擎/信号] 行情跌破关键结构 | 关键参数: 时间=【{timestamp}】, 跌穿价=【{close_price:.4f}】, 历史低点支撑=【{bar['range_low']:.4f}】 | 结果: 【锁定次周期清仓】")

    # ---------------- 回测结果汇总 ----------------
    print("\n" + "=" * 50)
    print(f"[回测引擎/汇总] 历史数据遍历结束 | 关键参数: 总交易频次=【{len(trade_history)}笔】 | 结果: 【生成最终统计】")

    if trade_history:
        trades_df = pd.DataFrame(trade_history)
        win_rate = (trades_df['pnl_pct'] > 0).mean() * 100
        cumulative_return = (1 + trades_df['pnl_pct']).prod() - 1

        print(f"  > 胜率预期: 【{win_rate:.2f}%】")
        print(f"  > 累计净盈亏: 【{cumulative_return * 100:.2f}%】 (按全仓无杠杆复利测算)")
        print(f"  > 均笔盈亏: 【{trades_df['pnl_pct'].mean() * 100:.2f}%】")
    else:
        print(
            "  > 结果判定: 【无任何交易触发】\n  > 排查建议: 行情过分平淡或阈值(90% OI)设置过高，请调整基础配置参数重试。")
    print("=" * 50)

    return trade_history


# ==========================================
# 4. 执行入口
# ==========================================
if __name__ == "__main__":
    target_symbol = "BLESS"

    file_5m = f'./data/{target_symbol}_USDT_USDT_5m_ler_data.csv'
    file_1m = rf"W:\project\python_project\oke_auto_trade\kline_data\{target_symbol}USDT_1m_2025-01-01_merged.csv"

    try:
        merged_1m_df = load_and_prepare_data(file_5m, file_1m)
        history = run_backtest(merged_1m_df)

    except FileNotFoundError as e:
        print(f"\n[系统级错误/IO故障] 读取核心底层数据失败 | 关键参数: 丢失文件=【{e.filename}】 | 结果: 【强制中断退栈】")
        print("💡 排查建议: 当前处于数据准备阶段。请检查CSV文件是否真实存在于上述路径，或检查拼写是否有误。")
        raise  # 严禁吞噬系统级异常，强制抛出定位根因
    except KeyError as e:
        print(f"\n[系统级错误/数据异常] K线特征字段缺失 | 关键参数: 缺失列名=【{str(e)}】 | 结果: 【强制中断退栈】")
        print("💡 排查建议: CSV表头格式可能不符，请确认是否包含必需的价格与特征指标列。")
        raise