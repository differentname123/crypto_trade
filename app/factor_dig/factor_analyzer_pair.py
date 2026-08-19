import os
import itertools
import pandas as pd
import numpy as np
import datetime


# =====================================================================
# 全局信号脱敏映射模块 (新增)
# =====================================================================
_SIGNAL_MAPPING = {}
_SIGNAL_COUNTER = 1

def get_masked_signal(sig_name):
    """获取脱敏后的信号名称，首次遇到则生成如 SIGNAL_001"""
    global _SIGNAL_COUNTER
    sig_str = str(sig_name)
    if sig_str not in _SIGNAL_MAPPING:
        _SIGNAL_MAPPING[sig_str] = f"SIGNAL_{_SIGNAL_COUNTER:03d}"
        _SIGNAL_COUNTER += 1
    return _SIGNAL_MAPPING[sig_str]

def save_signal_mapping_table(output_dir='./summary_results'):
    """保存原始信号名称与脱敏名称的映射表"""
    if not _SIGNAL_MAPPING:
        return
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, 'signal_mapping.csv')
    df_map = pd.DataFrame(list(_SIGNAL_MAPPING.items()), columns=['原始信号名称', '脱敏信号名称'])
    df_map.to_csv(path, index=False, encoding='utf-8-sig')


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
    # =================================================================
    # 新增过滤：仅提取最大并发持仓 <= 100 的结果用于打印，剔除>100的选项
    # =================================================================
    valid_single_df = single_df[single_df['最大并发持仓数'] <= 100]
    valid_pair_df = pair_df[pair_df['联合_最大并发持仓'] <= 100]

    print("\n" + "=" * 92)
    print(" 🌟 多因子策略【单体基准】 VS 【两两组合增强】绩效对比看板")
    print("=" * 92)

    # 1. 打印单策略基准 Top 5
    print("\n📌 【单体策略基准 Top 5】(按性价比排序):")
    print("-" * 92)
    print(
        f"{'排名':<4} | {'策略指纹 (TF | Entry | Exit | Dir | Filter)':<48} | {'净收益(%)':>10} | {'最大回撤(%)':>11} | {'性价比':>8}")
    print("-" * 92)
    for i, (_, row) in enumerate(valid_single_df.head(5).iterrows(), 1):
        # 此处使用 get_masked_signal 进行脱敏
        strat_name = f"{row['周期']} | {get_masked_signal(row['入场'])} | {get_masked_signal(row['出场'])} | {row['方向']} | {row['过滤']}"
        if len(strat_name) > 46:
            strat_name = strat_name[:43] + "..."
        print(
            f"#{i:<3} | {strat_name:<48} | {row['总真实净收益(%)']:>9.2f}% | {row['策略组合资金最大回撤(%)']:>10.2f}% | {row['策略赚钱性价比']:>8.2f}")

    # 2. 打印协同增强组合 Top N (按照 联合后的 赚钱性价比 降序排序)
    pair_df_sorted = valid_pair_df.sort_values(by='联合_赚钱性价比', ascending=False)
    top_synergy = pair_df_sorted.head(top_n)

    print("\n" + "=" * 92)
    print(f" 🚀 【最强联合组合 Top {len(top_synergy)}】(按【联合后赚钱性价比】降序排序)")
    print("=" * 92)

    for rank, (_, row) in enumerate(top_synergy.iterrows(), 1):
        cost_gain = row['【提升】性价比增量(vs单体最优)']
        gain_flag = "🟢 [显著提升]" if cost_gain > 0 else "🔴 [无增益/稀释]"

        print(f"\n组合编号 {2}_{rank} 表现如下")

        # 此处使用 get_masked_signal 进行脱敏
        print(
            f"  ├─ 组合 A: [{row['组合A_周期']}] {get_masked_signal(row['组合A_入场'])} -> {get_masked_signal(row['组合A_出场'])} ({row['组合A_方向']}_{row['组合A_过滤']})")
        print(
            f"  └─ 组合 B: [{row['组合B_周期']}] {get_masked_signal(row['组合B_入场'])} -> {get_masked_signal(row['组合B_出场'])} ({row['组合B_方向']}_{row['组合B_过滤']})")
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
# 🖥️ 新增: k联组合(k>=3) 控制台看板打印器 (基准 = 最优已评估子组合)
# =====================================================================
def print_multi_synergy_dashboard(level_df, k, top_n=5):
    """
    针对 k(>=3) 联组合的控制台看板：与【最优已评估子组合】基准对比。
    只有打赢自己最强子组合的 k 联，才算真正的增量。
    """
    if level_df is None or level_df.empty:
        return

    # =================================================================
    # 新增过滤：仅提取最大并发持仓 <= 100 的结果用于打印，剔除>100的选项
    # =================================================================
    valid_level_df = level_df[level_df['联合_最大并发持仓'] <= 100]
    if valid_level_df.empty:
        return

    df_sorted = valid_level_df.sort_values(by='联合_赚钱性价比', ascending=False)
    top = df_sorted.head(top_n)
    combo_label = f"{k}联组合"

    print("\n" + "=" * 92)
    print(f" 🚀 【最强 {k} 联组合 Top {len(top)}】(按【联合后赚钱性价比】降序排序, 基准=最优子组合)")
    print("=" * 92)

    for rank, (_, row) in enumerate(top.iterrows(), 1):
        cost_gain = row['【提升】性价比增量(vs最优子组合)']
        gain_flag = "🟢 [显著提升]" if cost_gain > 0 else "🔴 [无增益/稀释]"

        print(f"\n组合编号 {k}_{rank} 表现如下")
        for i in range(1, k + 1):
            prefix = "├─" if i < k else "└─"
            # 此处使用 get_masked_signal 进行脱敏
            print(f"  {prefix} 腿 {i}: [{row[f'腿{i}_周期']}] {get_masked_signal(row[f'腿{i}_入场'])} -> {get_masked_signal(row[f'腿{i}_出场'])} "
                  f"({row[f'腿{i}_方向']}_{row[f'腿{i}_过滤']}) | 单体性价比 {row[f'腿{i}_单体性价比']:.2f}")
        print(f"  ★ 对比基准(最优已评估子组合, 规模{int(row['最优子组合_规模'])}): {row['最优子组合_对应腿']}")
        print(f"  {'-' * 88}")
        print(f"  {'对比维度':<14} | {'最优子组合':>16} | {combo_label:>16} | {'增益/变化':>16}")
        print(f"  {'-' * 88}")

        # 1. 赚钱性价比
        c_sub = row['最优子组合_赚钱性价比']
        c_combo = row['联合_赚钱性价比']
        c_diff_sign = f"+{cost_gain:.2f}" if cost_gain >= 0 else f"{cost_gain:.2f}"
        icon = "🔺" if cost_gain >= 0 else "🔻"
        print(f"  {'赚钱性价比':<14} | {c_sub:>16.2f} | {c_combo:>16.2f} | {c_diff_sign:>14} {icon}")

        # 2. 资金最大回撤
        m_sub = row['最优子组合_资金最大回撤(%)']
        m_combo_v = row['联合_资金最大回撤(%)']
        m_diff = row['【风险】回撤变动(vs最优子组合)(%)']
        m_diff_sign = f"+{m_diff:.2f}%" if m_diff >= 0 else f"{m_diff:.2f}%"
        print(f"  {'资金最大回撤':<14} | {m_sub:>15.2f}% | {m_combo_v:>15.2f}% | {m_diff_sign:>16}")

        # 3. 最大回撤历时(天)
        dur_sub = row['最优子组合_最大回撤历时(天)']
        dur_combo = row['联合_最大回撤历时(天)']
        print(f"  {'最大回撤历时(天)':<14} | {dur_sub:>16.2f} | {dur_combo:>16.2f} | {'---':>16}")

        # 4. 最大并发持仓
        con_sub = int(row['最优子组合_最大并发持仓'])
        con_combo = int(row['联合_最大并发持仓'])
        print(f"  {'最大并发持仓':<14} | {con_sub:>16} | {con_combo:>16} | {'---':>16}")

        # 5. Top1币收益占比
        top1_sub = row['最优子组合_Top1币收益占比(%)']
        top1_combo = row['联合_Top1币收益占比(%)']
        print(f"  {'Top1币收益占比':<14} | {top1_sub:>15.2f}% | {top1_combo:>15.2f}% | {'---':>16}")

    print("\n" + "=" * 92)


# =====================================================================
# 核心处理主流程
# =====================================================================
def analyze_pair_combinations_with_baseline(
        df_trades,
        output_dir='./summary_results',
        pair_output_filename='pair_combinations_with_comparison.csv',
        single_output_filename='single_strategy_summary.csv',
        show_top_n_dashboard=5,
        max_combo_size=4,
        improvement_threshold=0.0,
        max_seeds_per_level=200,
        multi_output_filename_tpl='combo_{k}_combinations_with_comparison.csv'
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

    # =====================================================================
    # 筛选满足【最大并发持仓数 < 100】的腿才能作为后续组合的材料
    # =====================================================================
    valid_strategies_for_combo = [s_id for s_id in unique_strategies if single_metrics_dict[s_id]['最大并发持仓数'] < 100]

    if len(valid_strategies_for_combo) < 2:
        print("⚠️ 满足并发条件的独立策略少于 2 个，跳过组合测算。")
        save_signal_mapping_table(output_dir)
        return single_summary_df, pd.DataFrame(), {}

    # ▼▼▼ 新增: 已评估组合指标注册表 (frozenset(成员) -> 指标Series)，单策略先入册
    combo_metrics_registry = {frozenset([s_id]): m for s_id, m in single_metrics_dict.items()}
    # ▼▼▼ 新增: 上一级中【有提升】的合格组合列表, 元素为 (combo_key, 联合性价比, 性价比增量)
    qualified_prev_level = []

    # 使用筛选后的合法策略组合
    pair_combos = list(itertools.combinations(valid_strategies_for_combo, 2))
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

        # ▼▼▼ 新增: 注册两两组合指标, 并按提升阈值筛选出可进入 3 联扩展的合格种子
        combo_key = frozenset((s1, s2))
        combo_metrics_registry[combo_key] = m_combo
        if cost_diff > improvement_threshold:
            qualified_prev_level.append((combo_key, m_combo['策略赚钱性价比'], cost_diff))
        # ▲▲▲ 新增结束

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

    # =================================================================
    # 🧬 新增: 多级组合扩展 (3 联 ~ max_combo_size 联)
    #   规则: 只有上一级中【性价比增量 > improvement_threshold】的组合
    #         才有资格作为种子, 向外扩展一个新策略腿;
    #   基准: k联组合的增量 = 联合性价比 - 该组合所有【已评估真子集】
    #         (单体 + 全部两两 + 已测的更低联) 中的最优性价比。
    # =================================================================
    level_dfs = {2: pair_summary_df}
    level_out_paths = {}

    for k in range(3, max_combo_size + 1):
        if not qualified_prev_level:
            print(f"\n⚠️ 上一级({k - 1}联)没有满足提升阈值(>{improvement_threshold})的组合，停止扩展至 {k} 联。")
            break

        # 种子排序: 优先联合性价比, 其次增量 (可按需改为 x[2] 优先增量)
        seeds_sorted = sorted(qualified_prev_level, key=lambda x: (x[1], x[2]), reverse=True)
        if max_seeds_per_level is not None and len(seeds_sorted) > max_seeds_per_level:
            print(f"\n✂️ {k - 1}联合格种子共 {len(seeds_sorted)} 个，按联合性价比截取前 {max_seeds_per_level} 个用于扩展。")
            seeds_sorted = seeds_sorted[:max_seeds_per_level]
        seeds = [item[0] for item in seeds_sorted]

        # 候选生成: 种子 + 任意一条不在种子内的策略腿, frozenset 天然去重
        candidate_keys = set()
        for seed in seeds:
            # 使用筛选后的合法策略组合向外扩展
            for s_id in valid_strategies_for_combo:
                if s_id not in seed:
                    candidate_keys.add(seed | frozenset([s_id]))
        candidate_keys = sorted(
            (ck for ck in candidate_keys if ck not in combo_metrics_registry),
            key=lambda fs: tuple(sorted(fs))
        )

        if not candidate_keys:
            print(f"\n⚠️ {k}联组合没有可扩展的新候选，停止扩展。")
            break

        print(f"\n🚀 [{k}联组合] 由 {len(seeds)} 个合格种子扩展出 {len(candidate_keys)} 个去重候选，开始测算...")

        results_k = []
        qualified_this_level = []

        for idx, combo_key in enumerate(candidate_keys, 1):
            members = sorted(combo_key)

            combined_df = pd.concat([grouped_strats[mid] for mid in members], ignore_index=True)
            combined_df.sort_values('exit_time', inplace=True)
            combined_df.reset_index(drop=True, inplace=True)

            m_combo = calculate_portfolio_metrics(combined_df)
            combo_metrics_registry[combo_key] = m_combo

            # 基准: 所有已评估真子集中的最优性价比
            best_sub_key = None
            best_sub_cost = -np.inf
            for sub_size in range(1, k):
                for sub in itertools.combinations(members, sub_size):
                    sub_m = combo_metrics_registry.get(frozenset(sub))
                    if sub_m is not None and sub_m['策略赚钱性价比'] > best_sub_cost:
                        best_sub_cost = sub_m['策略赚钱性价比']
                        best_sub_key = frozenset(sub)

            best_sub_m = combo_metrics_registry[best_sub_key]
            cost_diff = m_combo['策略赚钱性价比'] - best_sub_cost

            row = {'组合规模': k}
            for i, mid in enumerate(members, 1):
                info = mid.split(' | ')
                row[f'腿{i}_标识'] = mid
                row[f'腿{i}_周期'] = info[0]
                row[f'腿{i}_入场'] = info[1]
                row[f'腿{i}_出场'] = info[2]
                row[f'腿{i}_方向'] = info[3]
                row[f'腿{i}_过滤'] = info[4]
                row[f'腿{i}_单体性价比'] = single_metrics_dict[mid]['策略赚钱性价比']

            row['最优子组合_规模'] = len(best_sub_key)
            row['最优子组合_成员'] = ' ++ '.join(sorted(best_sub_key))
            row['最优子组合_对应腿'] = ' + '.join(
                [f"腿{i}" for i, mid in enumerate(members, 1) if mid in best_sub_key])
            row['最优子组合_赚钱性价比'] = best_sub_cost
            row['最优子组合_资金最大回撤(%)'] = best_sub_m['策略组合资金最大回撤(%)']
            row['最优子组合_最大回撤历时(天)'] = best_sub_m['最大回撤历时(天)']
            row['最优子组合_最大并发持仓'] = best_sub_m['最大并发持仓数']
            row['最优子组合_Top1币收益占比(%)'] = best_sub_m['Top1币收益占比(%)']

            row['【提升】性价比增量(vs最优子组合)'] = cost_diff
            row['【风险】回撤变动(vs最优子组合)(%)'] = (
                    m_combo['策略组合资金最大回撤(%)'] - best_sub_m['策略组合资金最大回撤(%)'])

            row['联合_赚钱性价比'] = m_combo['策略赚钱性价比']
            row['联合_总真实净收益(%)'] = m_combo['总真实净收益(%)']
            row['联合_资金最大回撤(%)'] = m_combo['策略组合资金最大回撤(%)']
            row['联合_最大回撤历时(天)'] = m_combo['最大回撤历时(天)']
            row['联合_最大回撤历时占比(%)'] = m_combo['最大回撤历时占比(%)']
            row['联合_真实净胜率(%)'] = m_combo['真实净胜率(%)']
            row['联合_最大并发持仓'] = m_combo['最大并发持仓数']
            row['联合_Top1币收益占比(%)'] = m_combo['Top1币收益占比(%)']
            row['联合_Top3币收益占比(%)'] = m_combo['Top3币收益占比(%)']
            row['联合_总交易笔数'] = m_combo['总交易笔数']
            row['联合_纯价差总收益(%)'] = m_combo['纯价差总收益(%)']
            row['联合_资金费总损益(%)'] = m_combo['资金费总损益(%)']
            row['联合_单笔净期望(%)'] = m_combo['单笔净期望(%)']
            row['联合_真实盈潜比(Ret/MAE)'] = m_combo['真实盈潜比(Ret/MAE)']
            row['联合_平均持仓时间(天)'] = m_combo['平均持仓时间(天)']
            row['联合_资金时间回报(%/天)'] = m_combo['资金时间回报(%/天)']
            row['联合_平均资金暴露度(%)'] = m_combo['平均资金暴露度(%)']

            results_k.append(row)

            if cost_diff > improvement_threshold:
                qualified_this_level.append((combo_key, m_combo['策略赚钱性价比'], cost_diff))

        level_df = pd.DataFrame(results_k)
        level_df.sort_values(
            by=['【提升】性价比增量(vs最优子组合)', '联合_赚钱性价比'],
            ascending=[False, False],
            inplace=True
        )

        level_out_path = os.path.join(output_dir, multi_output_filename_tpl.format(k=k))
        level_df.to_csv(level_out_path, index=False, encoding='utf-8-sig', float_format="%.4f")
        level_out_paths[k] = level_out_path
        level_dfs[k] = level_df

        print(f"✅ [{k}联组合] 测算完成: 共评估 {len(level_df)} 个，其中满足提升阈值的 {len(qualified_this_level)} 个。")

        # 🖥️ 调用 k联组合控制台看板
        print_multi_synergy_dashboard(level_df, k, top_n=show_top_n_dashboard)

        qualified_prev_level = qualified_this_level

    # =====================================================================
    # 将汇总好的信号字典落盘保存映射表 (方便您查询具体是哪个信号)
    # =====================================================================
    save_signal_mapping_table(output_dir)

    return single_summary_df, pair_summary_df, level_dfs


# =====================================================================
# 执行入口
# =====================================================================
if __name__ == '__main__':
    trades_file = './extracted_raw_trades/extracted_target_pairs.csv'
    print(f"{'=' * 90}")
    print("💡 【数据维度说明】:")
    print(" 🔹 周期 (Timeframe) : K线图的时间粒度 (如 60m=1小时线, 5m=5分钟线)。直观反映策略在不同级别趋势中的适应性。")
    print(" 🔹 过滤模式 (Filter): 基于过去24小时涨跌幅的截面选币过滤机制。")
    print("      - bottom_N : 仅在跌幅最大（排名垫底）的前 N 个币种上允许开仓。")
    print("      - top_N    : 仅在涨幅最大（排名靠前）的前 N 个币种上允许开仓。")
    print(
        "      所有利润 或者 胜率都是完全考虑了滑点 资金费率之后的数据 回测没有用到任何的未来函数 回测标的的流动性也没有任何问题")
    print(f"{'=' * 90}")
    if os.path.exists(trades_file):
        trades_df = pd.read_csv(trades_file)

        single_df, pair_df, multi_dfs = analyze_pair_combinations_with_baseline(
            df_trades=trades_df,
            output_dir='./summary_results',
            pair_output_filename='pair_combinations_with_comparison.csv',
            single_output_filename='single_strategy_summary.csv',
            show_top_n_dashboard=20,  # 控制台打印前 5 个提升最显著的组合
            max_combo_size=4,  # 最多扩展到 4 联组合
            improvement_threshold=0.0,  # 增量 > 该阈值才有资格进入下一级扩展
            max_seeds_per_level=200  # 每级最多取多少个合格种子向外扩展(防组合爆炸)
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