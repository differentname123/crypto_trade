import os
import itertools
import pandas as pd
import numpy as np
import datetime


# =====================================================================
# 指标测算核心函数
# =====================================================================
def calculate_portfolio_metrics(g_sorted):
    if g_sorted.empty:
        return pd.Series()

    entry_min = g_sorted['entry_time'].min()
    exit_max = g_sorted['exit_time'].max()
    strategy_lifetime_h = (exit_max - entry_min).total_seconds() / 3600.0 if pd.notnull(entry_min) and pd.notnull(
        exit_max) else 0.0

    total_trades = len(g_sorted)
    sum_return = g_sorted['return'].sum() * 100
    sum_fr_impact = g_sorted['fr_impact'].sum() * 100
    sum_net_return = g_sorted['net_return'].sum() * 100

    win_rate = (g_sorted['return'] > 0).mean() * 100
    true_win_rate = (g_sorted['net_return'] > 0).mean() * 100
    avg_net_return = g_sorted['net_return'].mean() * 100

    coin_agg = g_sorted.groupby('coin', observed=True)[['net_return', 'hold_time_h']].sum()
    coin_rets = coin_agg['net_return']
    coin_hold_hours = coin_agg['hold_time_h']

    unique_coins = len(coin_rets)
    true_win_coins = (coin_rets > 0).sum()
    true_coin_win_rate = (true_win_coins / unique_coins * 100) if unique_coins > 0 else 0.0

    avg_mdd = g_sorted['max_drawdown'].mean() * 100
    mdd_5 = g_sorted['max_drawdown'].quantile(0.05) * 100
    mdd_10 = g_sorted['max_drawdown'].quantile(0.10) * 100
    true_return_mae_ratio = (avg_net_return / abs(avg_mdd)) if avg_mdd != 0 else np.nan

    max_hold_time_d = g_sorted['hold_time_h'].max() / 24.0
    avg_hold_time_d = g_sorted['hold_time_h'].mean() / 24.0
    median_hold_time_d = g_sorted['hold_time_h'].median() / 24.0
    quantile_90_hold_time_d = g_sorted['hold_time_h'].quantile(0.90) / 24.0

    sum_hold_time_d = g_sorted['hold_time_h'].sum() / 24.0
    capital_time_ret_per_day = (sum_net_return / sum_hold_time_d) if sum_hold_time_d > 0 else np.nan

    avg_exposure = (coin_hold_hours / strategy_lifetime_h).mean() * 100 if strategy_lifetime_h > 0 else 0.0

    top1_ret = coin_rets.max() * 100 if len(coin_rets) > 0 else 0.0
    top3_ret = coin_rets.nlargest(3).sum() * 100 if len(coin_rets) > 0 else 0.0
    top1_ratio = (top1_ret / sum_net_return * 100) if sum_net_return > 0 else np.nan
    top3_ratio = (top3_ret / sum_net_return * 100) if sum_net_return > 0 else np.nan

    net_rets_arr = g_sorted['net_return'].values
    exit_times_arr = g_sorted['exit_time'].values

    cum_eq = np.cumsum(net_rets_arr) * 100
    running_max = np.maximum.accumulate(cum_eq)
    drawdowns = running_max - cum_eq
    curve_maxdd = drawdowns.max() if len(drawdowns) > 0 else 0.0

    maxdd_duration_d = 0.0
    if curve_maxdd > 1e-8:
        trough_idx = np.argmax(drawdowns)
        trough_time = exit_times_arr[trough_idx]
        peak_idx = np.argmax(cum_eq[:trough_idx + 1])
        peak_time = exit_times_arr[peak_idx]
        maxdd_duration_d = float((trough_time - peak_time) / np.timedelta64(1, 'D'))

    maxdd_duration_ratio = (maxdd_duration_d / (strategy_lifetime_h / 24.0) * 100) if strategy_lifetime_h > 0 else 0.0

    entry_times = g_sorted['entry_time'].values
    times = np.concatenate([entry_times, exit_times_arr])
    weights = np.concatenate([
        np.ones(len(entry_times), dtype=np.int8),
        -np.ones(len(exit_times_arr), dtype=np.int8)
    ])
    sort_idx = np.lexsort((weights, times))
    concurrencies = np.cumsum(weights[sort_idx])
    max_concurrency = concurrencies.max() if len(concurrencies) > 0 else 0

    if curve_maxdd > 1e-8:
        strategy_cost_effectiveness = sum_net_return / curve_maxdd
    else:
        strategy_cost_effectiveness = 999.0 if sum_net_return > 0 else 0.0

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


# =====================================================================
# 🖥️ 控制台极简可视化看板打印器
# =====================================================================
def print_synergy_dashboard(single_df, pair_df, top_n=5):
    """
    在控制台打印排版清晰的对比看板，快速识别 1+1 > 2 的组合
    """
    print("\n" + "=" * 92)
    print(" 🌟 多因子策略【单体基准】 VS 【两两组合增强】绩效对比看板")
    print("=" * 92)

    # 1. 打印单策略基准 Top 5
    print("\n📌 【单体策略基准 Top 5】(按性价比排序):")
    print("-" * 92)
    print(
        f"{'排名':<4} | {'策略指纹 (TF | Entry | Exit | Dir | Filter)':<48} | {'净收益(%)':>10} | {'最大回撤(%)':>11} | {'性价比':>8}")
    print("-" * 92)
    for i, (_, row) in enumerate(single_df.head(5).iterrows(), 1):
        strat_name = f"{row['周期']} | {row['入场']} | {row['出场']} | {row['方向']} | {row['过滤']}"
        if len(strat_name) > 46:
            strat_name = strat_name[:43] + "..."
        print(
            f"#{i:<3} | {strat_name:<48} | {row['总真实净收益(%)']:>9.2f}% | {row['策略组合资金最大回撤(%)']:>10.2f}% | {row['策略赚钱性价比']:>8.2f}")

    # 2. 打印协同增强组合 Top N (按照 联合后的 赚钱性价比 降序排序)
    pair_df_sorted = pair_df.sort_values(by='联合_赚钱性价比', ascending=False)
    top_synergy = pair_df_sorted.head(top_n)

    print("\n" + "=" * 92)
    print(f" 🚀 【最强联合组合 Top {len(top_synergy)}】(按【联合后赚钱性价比】降序排序)")
    print("=" * 92)

    for rank, (_, row) in enumerate(top_synergy.iterrows(), 1):
        cost_gain = row['【提升】性价比增量(vs单体最优)']
        gain_flag = "🟢 [显著提升]" if cost_gain > 0 else "🔴 [无增益/稀释]"

        print(f"\n🏆 第 {rank} 名组合 {gain_flag}")
        print(
            f"  ├─ 组合 A: [{row['组合A_周期']}] {row['组合A_入场']} -> {row['组合A_出场']} ({row['组合A_方向']}_{row['组合A_过滤']})")
        print(
            f"  └─ 组合 B: [{row['组合B_周期']}] {row['组合B_入场']} -> {row['组合B_出场']} ({row['组合B_方向']}_{row['组合B_过滤']})")
        print(f"  {'-' * 88}")
        print(f"  {'对比维度':<14} | {'单策略 A':>14} | {'单策略 B':>14} | {'两两联合组合':>16} | {'增益/变化':>16}")
        print(f"  {'-' * 88}")

        # 1. 赚钱性价比
        c_a = row['单A_赚钱性价比']
        c_b = row['单B_赚钱性价比']
        c_combo = row['联合_赚钱性价比']
        c_diff_sign = f"+{cost_gain:.2f}" if cost_gain >= 0 else f"{cost_gain:.2f}"
        icon = "🔺" if cost_gain >= 0 else "🔻"
        print(f"  {'赚钱性价比':<14} | {c_a:>14.2f} | {c_b:>14.2f} | {c_combo:>16.2f} | {c_diff_sign:>14} {icon}")

        # 2. 资金最大回撤
        m_a = row['单A_资金最大回撤(%)']
        m_b = row['单B_资金最大回撤(%)']
        m_combo = row['联合_资金最大回撤(%)']
        m_diff = row['【风险】回撤变动(vs单体最低)(%)']
        m_diff_sign = f"+{m_diff:.2f}%" if m_diff >= 0 else f"{m_diff:.2f}%"
        print(f"  {'资金最大回撤':<14} | {m_a:>13.2f}% | {m_b:>13.2f}% | {m_combo:>15.2f}% | {m_diff_sign:>16}")

        # 3. 最大回撤历时(天)
        dur_a = row['单A_最大回撤历时(天)']
        dur_b = row['单B_最大回撤历时(天)']
        dur_combo = row['联合_最大回撤历时(天)']
        print(f"  {'最大回撤历时(天)':<14} | {dur_a:>14.2f} | {dur_b:>14.2f} | {dur_combo:>16.2f} | {'---':>16}")

        # 4. 最大并发持仓
        con_a = int(row['单A_最大并发持仓'])
        con_b = int(row['单B_最大并发持仓'])
        con_combo = int(row['联合_最大并发持仓'])
        print(f"  {'最大并发持仓':<14} | {con_a:>14} | {con_b:>14} | {con_combo:>16} | {'---':>16}")

        # 5. Top1币收益占比
        top1_a = row['单A_Top1币收益占比(%)']
        top1_b = row['单B_Top1币收益占比(%)']
        top1_combo = row['联合_Top1币收益占比(%)']
        print(f"  {'Top1币收益占比':<14} | {top1_a:>13.2f}% | {top1_b:>13.2f}% | {top1_combo:>15.2f}% | {'---':>16}")

    print("\n" + "=" * 92)


# =====================================================================
# 核心处理主流程
# =====================================================================
def analyze_pair_combinations_with_baseline(
        df_trades,
        output_dir='./summary_results',
        pair_output_filename='pair_combinations_with_comparison.csv',
        single_output_filename='single_strategy_summary.csv',
        show_top_n_dashboard=5
):
    os.makedirs(output_dir, exist_ok=True)
    df = df_trades.copy()

    df['entry_time'] = pd.to_datetime(df['entry_time'])
    df['exit_time'] = pd.to_datetime(df['exit_time'])

    if 'fr_impact' not in df.columns:
        is_long = df['direction'] == 'Long'
        df['fr_impact'] = np.where(is_long, -df['fr_sum'], df['fr_sum'])
    if 'net_return' not in df.columns:
        df['net_return'] = df['return'] + df['fr_impact']
    if 'hold_time_h' not in df.columns:
        df['hold_time_h'] = (df['exit_time'] - df['entry_time']).dt.total_seconds() / 3600.0

    strat_keys = ['timeframe', 'entry_factor', 'exit_factor', 'direction', 'filter_mode']
    df['strategy_id'] = df[strat_keys].astype(str).agg(' | '.join, axis=1)

    grouped_strats = {name: group.sort_values('exit_time').reset_index(drop=True)
                      for name, group in df.groupby('strategy_id')}

    unique_strategies = list(grouped_strats.keys())
    print(f"📊 识别到 {len(unique_strategies)} 个独立单策略，正在计算单策略基准指标...")

    single_metrics_dict = {}
    single_records = []

    for s_id in unique_strategies:
        s_df = grouped_strats[s_id]
        m = calculate_portfolio_metrics(s_df)
        single_metrics_dict[s_id] = m

        s_info = s_id.split(' | ')
        s_meta = pd.Series({
            '策略ID': s_id,
            '周期': s_info[0],
            '入场': s_info[1],
            '出场': s_info[2],
            '方向': s_info[3],
            '过滤': s_info[4]
        })
        single_records.append(pd.concat([s_meta, m]))

    single_summary_df = pd.DataFrame(single_records)
    single_summary_df.sort_values(by=['策略赚钱性价比', '总真实净收益(%)'], ascending=[False, False], inplace=True)

    single_out_path = os.path.join(output_dir, single_output_filename)
    single_summary_df.to_csv(single_out_path, index=False, encoding='utf-8-sig', float_format="%.4f")

    if len(unique_strategies) < 2:
        print("⚠️ 独立策略少于 2 个，跳过两两组合测算。")
        return single_summary_df, pd.DataFrame()

    pair_combos = list(itertools.combinations(unique_strategies, 2))
    total_pairs = len(pair_combos)
    print(f"🚀 开始测算 {total_pairs} 个两两组合对...")

    results = []
    for idx, (s1, s2) in enumerate(pair_combos, 1):
        df1 = grouped_strats[s1]
        df2 = grouped_strats[s2]
        m1 = single_metrics_dict[s1]
        m2 = single_metrics_dict[s2]

        combined_df = pd.concat([df1, df2], ignore_index=True)
        combined_df.sort_values('exit_time', inplace=True)
        combined_df.reset_index(drop=True, inplace=True)

        m_combo = calculate_portfolio_metrics(combined_df)

        best_single_cost = max(m1['策略赚钱性价比'], m2['策略赚钱性价比'])
        cost_diff = m_combo['策略赚钱性价比'] - best_single_cost

        best_single_ret = max(m1['总真实净收益(%)'], m2['总真实净收益(%)'])
        ret_diff = m_combo['总真实净收益(%)'] - best_single_ret

        lowest_single_mdd = min(m1['策略组合资金最大回撤(%)'], m2['策略组合资金最大回撤(%)'])
        mdd_change = m_combo['策略组合资金最大回撤(%)'] - lowest_single_mdd

        s1_info = s1.split(' | ')
        s2_info = s2.split(' | ')

        comparison_info = pd.Series({
            '组合A_标识': s1,
            '组合B_标识': s2,
            '组合A_周期': s1_info[0],
            '组合A_入场': s1_info[1],
            '组合A_出场': s1_info[2],
            '组合A_方向': s1_info[3],
            '组合A_过滤': s1_info[4],
            '组合B_周期': s2_info[0],
            '组合B_入场': s2_info[1],
            '组合B_出场': s2_info[2],
            '组合B_方向': s2_info[3],
            '组合B_过滤': s2_info[4],

            '【提升】性价比增量(vs单体最优)': cost_diff,
            '【提升】净收益增量(vs单体最优)(%)': ret_diff,
            '【风险】回撤变动(vs单体最低)(%)': mdd_change,

            '联合_赚钱性价比': m_combo['策略赚钱性价比'],
            '单A_赚钱性价比': m1['策略赚钱性价比'],
            '单B_赚钱性价比': m2['策略赚钱性价比'],

            '联合_总真实净收益(%)': m_combo['总真实净收益(%)'],
            '单A_总真实净收益(%)': m1['总真实净收益(%)'],
            '单B_总真实净收益(%)': m2['总真实净收益(%)'],

            '联合_资金最大回撤(%)': m_combo['策略组合资金最大回撤(%)'],
            '单A_资金最大回撤(%)': m1['策略组合资金最大回撤(%)'],
            '单B_资金最大回撤(%)': m2['策略组合资金最大回撤(%)'],

            '联合_最大回撤历时(天)': m_combo['最大回撤历时(天)'],
            '单A_最大回撤历时(天)': m1['最大回撤历时(天)'],
            '单B_最大回撤历时(天)': m2['最大回撤历时(天)'],

            '联合_真实净胜率(%)': m_combo['真实净胜率(%)'],
            '单A_真实净胜率(%)': m1['真实净胜率(%)'],
            '单B_真实净胜率(%)': m2['真实净胜率(%)'],

            '联合_最大并发持仓': m_combo['最大并发持仓数'],
            '单A_最大并发持仓': m1['最大并发持仓数'],
            '单B_最大并发持仓': m2['最大并发持仓数'],

            '联合_Top1币收益占比(%)': m_combo['Top1币收益占比(%)'],
            '单A_Top1币收益占比(%)': m1['Top1币收益占比(%)'],
            '单B_Top1币收益占比(%)': m2['Top1币收益占比(%)'],

            '联合_总交易笔数': m_combo['总交易笔数'],
            '联合_纯价差总收益(%)': m_combo['纯价差总收益(%)'],
            '联合_资金费总损益(%)': m_combo['资金费总损益(%)'],
            '联合_单笔净期望(%)': m_combo['单笔净期望(%)'],
            '联合_真实盈潜比(Ret/MAE)': m_combo['真实盈潜比(Ret/MAE)'],

            '联合_最大回撤历时占比(%)': m_combo['最大回撤历时占比(%)'],
            '联合_平均持仓时间(天)': m_combo['平均持仓时间(天)'],
            '联合_资金时间回报(%/天)': m_combo['资金时间回报(%/天)'],
            '联合_平均资金暴露度(%)': m_combo['平均资金暴露度(%)'],

            '联合_Top3币收益占比(%)': m_combo['Top3币收益占比(%)']
        })
        results.append(comparison_info)

    pair_summary_df = pd.DataFrame(results)
    pair_summary_df.sort_values(
        by=['【提升】性价比增量(vs单体最优)', '联合_赚钱性价比'],
        ascending=[False, False],
        inplace=True
    )

    pair_out_path = os.path.join(output_dir, pair_output_filename)
    pair_summary_df.to_csv(pair_out_path, index=False, encoding='utf-8-sig', float_format="%.4f")

    # 🖥️ 调用控制台高亮看板
    print_synergy_dashboard(single_summary_df, pair_summary_df, top_n=show_top_n_dashboard)

    print(f"📁 详细单体指标已保存至: {os.path.abspath(single_out_path)}")
    print(f"📁 详细组合对比已保存至: {os.path.abspath(pair_out_path)}")

    return single_summary_df, pair_summary_df


# =====================================================================
# 执行入口
# =====================================================================
if __name__ == '__main__':
    trades_file = './extracted_raw_trades/extracted_target_pairs.csv'

    if os.path.exists(trades_file):
        trades_df = pd.read_csv(trades_file)

        single_df, pair_df = analyze_pair_combinations_with_baseline(
            df_trades=trades_df,
            output_dir='./summary_results',
            pair_output_filename='pair_combinations_with_comparison.csv',
            single_output_filename='single_strategy_summary.csv',
            show_top_n_dashboard=5  # 控制台打印前 5 个提升最显著的组合
        )
    else:
        print(f"❌ 找不到文件: {trades_file}，请先执行提取脚本。")

    origin_df = pd.read_csv(
        r'W:\project\python_project\crypto_trade\app\factor_dig\summary_results\advanced_summary_combined_ALL.csv')

    df = pd.read_csv(
        r'W:\project\python_project\crypto_trade\app\factor_dig\summary_results\single_strategy_summary.csv')
    pair_df = pd.read_csv(
        r'W:\project\python_project\crypto_trade\app\factor_dig\summary_results\pair_combinations_with_comparison.csv')
    print()