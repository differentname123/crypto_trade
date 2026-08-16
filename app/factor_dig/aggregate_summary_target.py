import os
import glob
import pandas as pd
import numpy as np
import datetime  # 新增：用于生成带有时间戳的文件名

# =====================================================================
# 核心配置区
# =====================================================================
# 修改点：使用字典明确指定多周期及其对应的目录，避免 split 解析错误
INPUT_DIRS_MAP = {
    '60m': './factor_out_60m_debugtest',
    '30m': './factor_out_30m_debugtest',
    '15m': './factor_out_15m_debugtest',
    '5m': './factor_out_5m_debugtest'
}
# 汇总结果保存目录
OUTPUT_DIR = './summary_results'


def process_group(g):
    """
    对单个策略组合 (entry, exit, direction, filter_mode) 进行高阶指标测算
    """
    # [优化] 核心算力降维：由于外部已经完成了全局预排序，这里直接去掉了极其耗时的 sort_values，
    # 仅保留 reset_index 用于确保后续的索引是连续的。
    g_sorted = g.reset_index(drop=True)

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

    # [优化] 框架级降维：将后续需要计算的收益和暴露度(时长)的两次 groupby 合并为一次执行。
    # observed=True 配合分类变量使用，避免生成无意义的空分类维度。
    coin_agg = g.groupby('coin', observed=True)[['net_return', 'hold_time_h']].sum()
    coin_rets = coin_agg['net_return']
    coin_hold_hours = coin_agg['hold_time_h']

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

    # 新增: 平均持仓时间 (天)
    avg_hold_time_d = g['hold_time_h'].mean() / 24.0

    # 新增: 持仓时间中位数 (天)
    median_hold_time_d = g['hold_time_h'].median() / 24.0

    # 新增: 持仓时间90%阈值 (天)
    quantile_90_hold_time_d = g['hold_time_h'].quantile(0.90) / 24.0

    # 单位时间的资金回报率 (%/天)：总真实净收益 / 总持仓天数
    sum_hold_time_d = g['hold_time_h'].sum() / 24.0
    capital_time_ret_per_day = (sum_net_return / sum_hold_time_d) if sum_hold_time_d > 0 else np.nan

    # 新增: 平均资金暴露度 (%)
    if strategy_lifetime_h > 0:
        # 使用合并聚合计算得出的 coin_hold_hours
        coin_exposures = coin_hold_hours / strategy_lifetime_h
        avg_exposure = coin_exposures.mean() * 100
    else:
        avg_exposure = 0.0

    # [优化] 算法小降维：直接使用 max 和 nlargest 替代对全 Series 的 sort_values
    top1_ret = coin_rets.max() * 100 if len(coin_rets) > 0 else 0.0
    top3_ret = coin_rets.nlargest(3).sum() * 100 if len(coin_rets) > 0 else 0.0

    top1_ratio = (top1_ret / sum_net_return * 100) if sum_net_return > 0 else np.nan
    top3_ratio = (top3_ret / sum_net_return * 100) if sum_net_return > 0 else np.nan

    # ---------------------------------------------------------
    # 🌊 第五组：时序与并发指标（Portfolio 级）
    # ---------------------------------------------------------
    # 1. 策略级资金曲线最大回撤 & 持续时间
    # [优化] 底层级降维：提取 .values 使用 numpy 高速计算累加与回撤
    net_rets_arr = g_sorted['net_return'].values
    exit_times_arr = g_sorted['exit_time'].values

    cum_eq = np.cumsum(net_rets_arr) * 100
    running_max = np.maximum.accumulate(cum_eq)
    drawdowns = running_max - cum_eq
    curve_maxdd = drawdowns.max() if len(drawdowns) > 0 else 0.0

    maxdd_duration_d = 0.0
    if curve_maxdd > 1e-8:
        # 找到底谷和前置最高峰的索引，避免使用耗时的 .loc
        trough_idx = np.argmax(drawdowns)
        trough_time = exit_times_arr[trough_idx]

        peak_idx = np.argmax(cum_eq[:trough_idx + 1])
        peak_time = exit_times_arr[peak_idx]

        # 使用 numpy 时差格式直接提取出天数浮点值
        maxdd_duration_d = float((trough_time - peak_time) / np.timedelta64(1, 'D'))

    # 最大回撤时间占比 (%)
    maxdd_duration_ratio = (maxdd_duration_d / (strategy_lifetime_h / 24.0) * 100) if strategy_lifetime_h > 0 else 0.0

    # 2. 最大并发持仓数量
    # [优化] 底层级降维：消灭原生 for 循环，全面改用 numpy 数组切片与累加。
    # 且 lexsort 处理时间冲突时，出场（-1）天生优先于入场（1），严格符合原始业务逻辑。
    entry_times = g['entry_time'].values
    exit_times = exit_times_arr  # 借用上方已有的数组

    times = np.concatenate([entry_times, exit_times])
    weights = np.concatenate([np.ones(len(entry_times), dtype=np.int8),
                              -np.ones(len(exit_times), dtype=np.int8)])

    sort_idx = np.lexsort((weights, times))
    concurrencies = np.cumsum(weights[sort_idx])
    max_concurrency = concurrencies.max() if len(concurrencies) > 0 else 0

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
        '平均持仓时间(天)': avg_hold_time_d,
        '持仓时间中位数(天)': median_hold_time_d,
        '持仓时间90%阈值(天)': quantile_90_hold_time_d,
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

    all_summaries = []  # 存储所有周期的测算结果，用于最终聚合合并
    groupby_keys = ['entry_factor', 'exit_factor', 'direction', 'filter_mode']

    # 核心修改点：遍历 60m, 30m, 15m, 5m 等多周期目录
    for timeframe, input_dir in INPUT_DIRS_MAP.items():
        if not os.path.exists(input_dir):
            print(f"⚠️ 找不到目录: {input_dir}，跳过...")
            continue

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
        print(f"✅ {timeframe} 数据加载完毕。总记录数: {len(df_all)}。正在执行预处理...")

        # ---------------------------------------------------------
        # 🟢 第0步：数据预处理（前置逻辑）
        # ---------------------------------------------------------
        df_all['entry_time'] = pd.to_datetime(df_all['entry_time'])
        df_all['exit_time'] = pd.to_datetime(df_all['exit_time'])

        # [优化] 内存与速度双赢：将字符串分组键与币种转为 Category 类型
        category_cols = ['entry_factor', 'exit_factor', 'direction', 'filter_mode', 'coin']
        for col in category_cols:
            if col in df_all.columns:
                df_all[col] = df_all[col].astype('category')

        is_long = df_all['direction'] == 'Long'
        df_all['fr_impact'] = np.where(is_long, -df_all['fr_sum'], df_all['fr_sum'])
        df_all['net_return'] = df_all['return'] + df_all['fr_impact']
        df_all['hold_time_h'] = (df_all['exit_time'] - df_all['entry_time']).dt.total_seconds() / 3600.0

        # ---------------------------------------------------------
        # ⚡ 核心聚合运算
        # ---------------------------------------------------------
        # [优化] 结构级核心降维：在拆组前，对全量数据完成一次全局预排序
        df_all.sort_values(by=groupby_keys + ['exit_time'], inplace=True)

        print(f"⏳ 正在按策略指纹聚合并测算 {timeframe} 的高阶指标...")

        # 务必加上 observed=True，否则 Pandas 会产生巨量内存爆炸
        summary = df_all.groupby(groupby_keys, group_keys=False, observed=True).apply(process_group).reset_index()

        # 修改点：为了最终大宽表能够横向对比，给所有的指标列重命名，带上周期后缀 (如: _60m)
        rename_dict = {col: f"{col}_{timeframe}" for col in summary.columns if col not in groupby_keys}
        summary.rename(columns=rename_dict, inplace=True)

        all_summaries.append(summary)

    # ---------------------------------------------------------
    # 🔗 多周期数据终极聚合连接 (Outer Join)
    # ---------------------------------------------------------
    if not all_summaries:
        print("\n⚠️ 没有任何周期数据被成功处理，退出。")
        return

    print("\n✨ 正在进行多周期策略大融合 (横向拼接宽表)...")
    final_summary = all_summaries[0]
    for i in range(1, len(all_summaries)):
        final_summary = pd.merge(final_summary, all_summaries[i], on=groupby_keys, how='outer')

    # 按 '总真实净收益(%)' 和 '真实盈潜比' 降序排列 (优先依据 60m 的表现排序，如果 60m 缺失则选第一个找到的周期)
    sort_cols = []
    ascending_flags = []
    if '总真实净收益(%)_60m' in final_summary.columns:
        sort_cols.extend(['总真实净收益(%)_60m', '真实盈潜比(Ret/MAE)_60m'])
        ascending_flags.extend([False, False])
    else:
        # Fallback：寻找存在的 '总真实净收益' 列进行排序
        fallback_ret = [c for c in final_summary.columns if '总真实净收益(%)' in c]
        fallback_mae = [c for c in final_summary.columns if '真实盈潜比(Ret/MAE)' in c]
        if fallback_ret and fallback_mae:
            sort_cols.extend([fallback_ret[0], fallback_mae[0]])
            ascending_flags.extend([False, False])

    if sort_cols:
        final_summary.sort_values(by=sort_cols, ascending=ascending_flags, inplace=True)

    # 核心修改点：加入时间信息，生成唯一文件标识
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file = os.path.join(OUTPUT_DIR, f'advanced_summary_combined_ALL.csv')

    final_summary.to_csv(out_file, index=False, encoding='utf-8-sig', float_format="%.4f")
    print(f"🎉 四周期深度融合统计报告已生成: {os.path.abspath(out_file)}")


if __name__ == "__main__":
    # df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\summary_results\advanced_summary_combined_ALL.csv')

    main()