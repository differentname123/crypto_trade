import pandas as pd
import numpy as np
from datetime import datetime

# ==========================================
# 1. 策略核心参数配置区 (方便后续调整)
# ==========================================
# 阶段一：燃料池参数 (5分钟级别)
OI_LOOKBACK_DAYS = 30  # OI 历史分位数回溯天数
OI_PERCENTILE = 0.90  # OI 极端分位数阈值 (90%)
FR_THRESHOLD = -0.0005  # 资金费率极值阈值 (-0.05%)
FR_CONSEC_PERIODS = 3  # 资金费率连续满足条件的周期数

# 阶段二/三：技术面触发参数 (1分钟级别)
RANGE_LOOKBACK_HOURS = 12  # 震荡区间回溯时间 (12小时)

# 交易成本参数
FEE_RATE = 0.001  # 单边手续费/滑点成本 (0.1%)


# ==========================================
# 2. 数据处理与信号生成函数
# ==========================================
def load_and_prepare_data(df_5m_path, df_1m_path):
    print("正在加载并处理数据...")

    # 读取数据
    df_5m = pd.read_csv(df_5m_path)
    df_1m = pd.read_csv(df_1m_path)

    df_1m["timestamp"] = (
        pd.to_datetime(df_1m["open_time"], unit="ms", utc=True)
        .dt.tz_convert("Asia/Shanghai")
        .dt.strftime("%Y-%m-%d %H:%M:%S")
    )


    # 转换时间戳 (请确保CSV里的timestamp字段能够被正确解析)
    df_5m['timestamp'] = pd.to_datetime(df_5m['timestamp'])
    df_1m['timestamp'] = pd.to_datetime(df_1m['timestamp'])

    # 按照时间排序以防止乱序
    df_5m = df_5m.sort_values('timestamp').reset_index(drop=True)
    df_1m = df_1m.sort_values('timestamp').reset_index(drop=True)

    # ------------------ 处理 5分钟 燃料池逻辑 ------------------
    # 1. 计算过去30天 (30 * 24 * 12 个 5分钟K线) 的 90% OI 分位数
    oi_window = OI_LOOKBACK_DAYS * 24 * 12
    df_5m['oi_90_pct'] = df_5m['oi_amount'].rolling(window=oi_window, min_periods=288).quantile(OI_PERCENTILE)

    # 条件A：当前 OI 大于 90% 分位数
    df_5m['condition_A'] = df_5m['oi_amount'] > df_5m['oi_90_pct']

    # 条件B：资金费率连续3个周期低于 -0.05%
    # 注意：截图1中的列名为 'predicted_funding_rate'
    df_5m['is_negative_fr'] = df_5m['predicted_funding_rate'] < FR_THRESHOLD
    df_5m['condition_B'] = df_5m['is_negative_fr'].rolling(window=FR_CONSEC_PERIODS).sum() >= FR_CONSEC_PERIODS

    # 燃料池锁定状态 (火药桶已就绪)
    df_5m['powder_keg_ready'] = df_5m['condition_A'] & df_5m['condition_B']

    # ------------------ 降维合并到 1分钟 K线 ------------------
    df_1m.set_index('timestamp', inplace=True)
    df_5m.set_index('timestamp', inplace=True)

    # 将 5分钟的火药桶状态前向填充（ffill）到 1分钟数据上
    # 这样 17:45 产生的 True 状态，会应用给 17:45~17:49 的 1分钟K线
    df_1m['powder_keg_ready'] = df_5m['powder_keg_ready'].reindex(df_1m.index).ffill()
    df_1m['powder_keg_ready'] = df_1m['powder_keg_ready'].fillna(False)

    # ------------------ 处理 1分钟 区间突破逻辑 ------------------
    range_window = RANGE_LOOKBACK_HOURS * 60

    # 使用 shift(1) 避免未来函数：当前K线的突破，是对比过去12小时的最高/最低点（不含当前K线本身）
    df_1m['range_high'] = df_1m['high'].rolling(window=range_window, min_periods=60).max().shift(1)
    df_1m['range_low'] = df_1m['low'].rolling(window=range_window, min_periods=60).min().shift(1)

    # 触发条件信号
    df_1m['long_trigger'] = (df_1m['close'] > df_1m['range_high']) & df_1m['powder_keg_ready']
    df_1m['exit_trigger'] = df_1m['close'] < df_1m['range_low']

    return df_1m.reset_index()


# ==========================================
# 3. 核心执行引擎 (遍历回测并埋点日志)
# ==========================================
def run_backtest(df_1m):
    print("\n================ 开始执行回测 ================")

    in_position = False
    entry_price = 0.0
    entry_time = None

    pending_long = False
    pending_exit = False

    trade_history = []

    # 将数据转换为字典列表加速迭代
    records = df_1m.to_dict('records')

    for i in range(len(records)):
        current_bar = records[i]
        timestamp = current_bar['timestamp']
        open_price = current_bar['open']
        close_price = current_bar['close']

        # 1. 优先处理上一根 K 线传导过来的待执行订单 (开盘直接市价执行)
        if pending_long and not in_position:
            # 执行多单
            entry_price = open_price * (1 + FEE_RATE)  # 计算滑点/手续费
            in_position = True
            entry_time = timestamp
            pending_long = False
            print(f"[入场日志] {timestamp} | 逼空点火！次根K线市价做多 | 开仓价: {entry_price:.4f} (已含手续费)")

        elif pending_exit and in_position:
            # 执行平仓
            exit_price = open_price * (1 - FEE_RATE)  # 计算滑点/手续费
            pnl_pct = (exit_price - entry_price) / entry_price

            trade_history.append({
                'entry_time': entry_time,
                'exit_time': timestamp,
                'entry_price': entry_price,
                'exit_price': exit_price,
                'pnl_pct': pnl_pct
            })

            print(
                f"[出场日志] {timestamp} | 跌破支撑止损/止盈 | 平仓价: {exit_price:.4f} | 单笔盈亏: {pnl_pct * 100:.2f}%")

            in_position = False
            pending_exit = False
            entry_price = 0.0
            entry_time = None

        # 2. 判断当前 K 线是否触发新的信号
        if not in_position:
            # 如果触发做多，标记下一根 K 线开盘执行
            if current_bar['long_trigger']:
                pending_long = True
                print(
                    f"\n[信号日志] {timestamp} | 发现火药桶突破! | 突破价(Close): {close_price:.4f} | 阻力位: {current_bar['range_high']:.4f}")
        else:
            # 如果触发离场，标记下一根 K 线开盘执行
            if current_bar['exit_trigger']:
                pending_exit = True
                print(
                    f"\n[信号日志] {timestamp} | 结构破坏信号! | 跌破价(Close): {close_price:.4f} | 支撑位: {current_bar['range_low']:.4f}")

    # 回测统计摘要
    print("\n================ 回测结果摘要 ================")
    if len(trade_history) > 0:
        trades_df = pd.DataFrame(trade_history)
        win_rate = (trades_df['pnl_pct'] > 0).mean() * 100
        cumulative_return = (1 + trades_df['pnl_pct']).prod() - 1

        print(f"总交易次数: {len(trade_history)} 次")
        print(f"策略胜率: {win_rate:.2f}%")
        print(f"累计净收益: {cumulative_return * 100:.2f}% (按每次全仓复利计算)")
        print(f"平均单笔盈亏: {trades_df['pnl_pct'].mean() * 100:.2f}%")
        print("交易记录:")
        print(trades_df.to_string())
    else:
        print("没有触发任何交易。建议检查数据时间范围或放宽过滤条件 (如调低90%的分位数要求)。")

    return trade_history


# ==========================================
# 4. 执行入口
# ==========================================
if __name__ == "__main__":
    # 文件路径替换为你本地真实的路径
    file_5m = './data/ADA_USDT_USDT_5m_ler_data.csv'
    file_1m = r"W:\project\python_project\oke_auto_trade\kline_data\ADAUSDT_1m_2025-01-01_merged.csv"



    # 为了防止你没上传完整CSV报错，这里用 try-except 保护一下
    try:
        merged_1m_df = load_and_prepare_data(file_5m, file_1m)
        history = run_backtest(merged_1m_df)
    except FileNotFoundError:
        print("未找到指定CSV文件，请确保文件路径正确并与脚本在同一目录下。")