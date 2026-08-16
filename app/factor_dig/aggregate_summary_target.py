import os
import glob
import pandas as pd
import numpy as np

# =====================================================================
# 核心配置区
# =====================================================================
# 你的交易记录目录列表 (可以根据实际情况添加或修改)
INPUT_DIRS = [
    './factor_out_60m_debug'
]
# 汇总结果保存目录
OUTPUT_DIR = './summary_results'


def process_group(g):
    """
    对单个策略组合 (entry, exit, direction, filter_mode) 进行高阶指标测算
    """
    # 确保按出场时间排序，用于资金曲线和生命周期分析
    g_sorted = g.sort_values('exit_time').reset_index(drop=True)

    # ---------------------------------------------------------
    # ⏱️ 预计算策略全局时间跨度
    # ---------------------------------------------------------
    # 策略生命周期（从该组合产生第一笔入场 到 最后一笔出场跨越的总小时数）
    strategy_lifetime_h = (g_sorted['exit_time'].max() - g_sorted['entry_time'].min()).total_seconds() / 3600.0

    # ---------------------------------------------------------
    # 📊 第一组：规模与收益类指标 (* 100 转 %)
    # ---------------------------------------------------------
    total_trades = len(g)
    sum_return = g['return'].sum() * 100
    sum_fr_impact = g['fr_impact'].sum() * 100
    sum_net_return = g['net_return'].sum() * 100

    # ---------------------------------------------------------
    # 🎯 第二组：胜率与质量类指标 (* 100 转 %)
    # ---------------------------------------------------------
    win_rate = (g['return'] > 0).mean() * 100
    true_win_rate = (g['net_return'] > 0).mean() * 100
    avg_net_return = g['net_return'].mean() * 100

    coin_rets = g.groupby('coin')['net_return'].sum()
    unique_coins = len(coin_rets)
    true_win_coins = (coin_rets > 0).sum()
    true_coin_win_rate = (true_win_coins / unique_coins * 100) if unique_coins > 0 else 0.0

    # ---------------------------------------------------------
    # 📉 第三组：盘中风险与痛点指标（MAE）(* 100 转 %)
    # ---------------------------------------------------------
    avg_mdd = g['max_drawdown'].mean() * 100
    mdd_5 = g['max_drawdown'].quantile(0.05) * 100
    mdd_10 = g['max_drawdown'].quantile(0.10) * 100

    # 真实盈潜比 = 平均单笔真实净收益 / 平均单笔承受回撤的绝对值 (这是个比值乘数，不需要乘100)
    true_return_mae_ratio = (avg_net_return / abs(avg_mdd)) if avg_mdd != 0 else np.nan

    # ---------------------------------------------------------
    # ⏱️ 第四组：时间与暴露度指标
    # ---------------------------------------------------------
    # 新增: 单笔最长持仓时间 (天)
    max_hold_time_d = g['hold_time_h'].max() / 24.0

    # 单位时间的资金回报率 (%/天)：总真实净收益 / 总持仓天数
    sum_hold_time_d = g['hold_time_h'].sum() / 24.0
    capital_time_ret_per_day = (sum_net_return / sum_hold_time_d) if sum_hold_time_d > 0 else np.nan

    # 新增: 平均资金暴露度 (%)
    if strategy_lifetime_h > 0:
        # 单币种持仓总时长 / 策略总生命周期 = 各币种自身的暴露度
        coin_hold_hours = g.groupby('coin')['hold_time_h'].sum()
        coin_exposures = coin_hold_hours / strategy_lifetime_h
        avg_exposure = coin_exposures.mean() * 100
    else:
        avg_exposure = 0.0

    # Top1 / Top3 收益集中度 (%)
    coin_rets_sorted = coin_rets.sort_values(ascending=False)
    top1_ret = coin_rets_sorted.iloc[0] * 100 if len(coin_rets_sorted) > 0 else 0.0
    top3_ret = coin_rets_sorted.head(3).sum() * 100 if len(coin_rets_sorted) > 0 else 0.0

    top1_ratio = (top1_ret / sum_net_return * 100) if sum_net_return > 0 else np.nan
    top3_ratio = (top3_ret / sum_net_return * 100) if sum_net_return > 0 else np.nan

    # ---------------------------------------------------------
    # 🌊 第五组：时序与并发指标（Portfolio 级）
    # ---------------------------------------------------------
    # 1. 策略级资金曲线最大回撤 & 持续时间
    cum_eq = g_sorted['net_return'].cumsum() * 100  # 乘以100化为百分比幅度
    running_max = cum_eq.cummax()
    drawdowns = running_max - cum_eq
    curve_maxdd = drawdowns.max()

    maxdd_duration_d = 0.0
    if curve_maxdd > 1e-8:
        # 找到最大回撤落底时刻
        trough_idx = drawdowns.idxmax()
        trough_time = g_sorted.loc[trough_idx, 'exit_time']
        # 找到引发该次下跌的最高峰时刻（在底谷之前最高点）
        peak_idx = cum_eq.loc[:trough_idx].idxmax()
        peak_time = g_sorted.loc[peak_idx, 'exit_time']
        # 修改为天
        maxdd_duration_d = (trough_time - peak_time).total_seconds() / 86400.0

    # 最大回撤时间占比 (%)
    maxdd_duration_ratio = (maxdd_duration_d / (strategy_lifetime_h / 24.0) * 100) if strategy_lifetime_h > 0 else 0.0

    # 2. 最大并发持仓数量
    events = [(t, 1) for t in g['entry_time']] + [(t, -1) for t in g['exit_time']]
    events.sort(key=lambda x: (x[0], x[1]))

    concurrency = 0
    max_concurrency = 0
    for _, val in events:
        concurrency += val
        if concurrency > max_concurrency:
            max_concurrency = concurrency

    # ---------------------------------------------------------
    # 🏆 终极指标：策略赚钱性价比 (Calmar Ratio)
    # ---------------------------------------------------------
    if curve_maxdd > 1e-8:
        strategy_cost_effectiveness = sum_net_return / curve_maxdd
    else:
        # 应对极其罕见的“零回撤一直赚钱”情况，赋予一个极高值
        strategy_cost_effectiveness = 999.0 if sum_net_return > 0 else 0.0

    # 返回打包结果 (均已完成要求的格式化调整)
    return pd.Series({
        '总交易笔数': total_trades,
        '纯价差总收益(%)': sum_return,
        '资金费总损益(%)': sum_fr_impact,
        '总真实净收益(%)': sum_net_return,

        '纯价差胜率(%)': win_rate,
        '真实净胜率(%)': true_win_rate,
        '单笔净期望(%)': avg_net_return,
        '涉及币种数': unique_coins,
        '跨币种胜率(%)': true_coin_win_rate,

        '均值单笔回撤(%)': avg_mdd,
        '最差5%极端回撤(%)': mdd_5,
        '最差10%极端回撤(%)': mdd_10,
        '真实盈潜比(Ret/MAE)': true_return_mae_ratio,

        '单笔最长持仓(天)': max_hold_time_d,
        '资金时间回报(%/天)': capital_time_ret_per_day,
        '平均资金暴露度(%)': avg_exposure,
        'Top1币收益占比(%)': top1_ratio,
        'Top3币收益占比(%)': top3_ratio,

        '策略组合资金最大回撤(%)': curve_maxdd,
        '最大回撤历时(天)': maxdd_duration_d,
        '最大回撤历时占比(%)': maxdd_duration_ratio,
        '最大并发持仓数': max_concurrency,

        '策略赚钱性价比': strategy_cost_effectiveness
    })

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for input_dir in INPUT_DIRS:
        if not os.path.exists(input_dir):
            print(f"⚠️ 找不到目录: {input_dir}，跳过...")
            continue

        timeframe = input_dir.split('_')[-1]

        file_pattern = os.path.join(input_dir, 'trades_*.csv.gz')
        trade_files = glob.glob(file_pattern)

        if not trade_files:
            print(f"⚠️ {input_dir} 下未找到 trades_*.csv.gz 文件，跳过...")
            continue

        print(f"\n🚀 开始处理 {timeframe} 数据，共找到 {len(trade_files)} 个币种记录文件...")

        df_list = []
        for f in trade_files:
            try:
                df = pd.read_csv(f)
                df_list.append(df)
            except Exception as e:
                print(f"读取 {f} 失败: {e}")

        if not df_list:
            continue

        df_all = pd.concat(df_list, ignore_index=True)
        print(f"✅ 数据加载完毕。总记录数: {len(df_all)}。正在执行预处理...")

        # ---------------------------------------------------------
        # 🟢 第0步：数据预处理（前置逻辑）
        # ---------------------------------------------------------
        df_all['entry_time'] = pd.to_datetime(df_all['entry_time'])
        df_all['exit_time'] = pd.to_datetime(df_all['exit_time'])

        is_long = df_all['direction'] == 'Long'
        df_all['fr_impact'] = np.where(is_long, -df_all['fr_sum'], df_all['fr_sum'])
        df_all['net_return'] = df_all['return'] + df_all['fr_impact']
        df_all['hold_time_h'] = (df_all['exit_time'] - df_all['entry_time']).dt.total_seconds() / 3600.0

        # ---------------------------------------------------------
        # ⚡ 核心聚合运算
        # ---------------------------------------------------------
        groupby_keys = ['entry_factor', 'exit_factor', 'direction', 'filter_mode']
        print(f"⏳ 正在按策略指纹 {groupby_keys} 聚合并测算高阶指标，这可能需要一点时间...")

        summary = df_all.groupby(groupby_keys, group_keys=False).apply(process_group).reset_index()

        # 按 '总真实净收益(%)' 和 '真实盈潜比' 降序排列
        summary.sort_values(by=['总真实净收益(%)', '真实盈潜比(Ret/MAE)'], ascending=[False, False], inplace=True)

        out_file = os.path.join(OUTPUT_DIR, f'advanced_summary_{timeframe}.csv')
        summary.to_csv(out_file, index=False, encoding='utf-8-sig', float_format="%.4f")
        print(f"🎉 {timeframe} 深度统计报告已生成: {os.path.abspath(out_file)}")


if __name__ == "__main__":
    main()