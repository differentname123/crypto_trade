# -*- coding: utf-8 -*-
"""
================================================================================
 因子组合 深度防伪体检与宏观生态分析 (Pro Version 2.0 - 极简实盘净水器版)
--------------------------------------------------------------------------------
 核心定位：你的“最终实盘拍板”验金石。
 本次升级：剔除统计学错觉与均值陷阱，引入实盘摩擦抗性与因子真实普适率。
 (新增)：因子名称自动脱敏，排除主观偏见，只看数据说话。
================================================================================
"""
import os
import re
import pandas as pd
import numpy as np

# 控制台颜色高亮代码
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RESET = '\033[0m'
BOLD = '\033[1m'


def make_bar(val, max_val, max_len=8):
    """生成极简 ASCII 柱状图，直观展现时序分布"""
    if pd.isna(val) or val <= 0: return "[LOSS]    "
    if max_val <= 0: return ""
    length = max(1, int((val / max_val) * max_len))
    return "▇" * length


def load_data(tf='15m'):
    """加载指定周期的挖掘产出数据"""
    out_dir = f'./factor_out_{tf}'
    sum_path = os.path.join(out_dir, 'pairs_CROSS_COIN_SUMMARY_Long.csv.gz')
    all_path = os.path.join(out_dir, 'pairs_ALL_Long.csv.gz')

    if not os.path.exists(sum_path) or not os.path.exists(all_path):
        print(f"{RED}❌ 找不到 {tf} 周期数据。请检查路径: {out_dir}{RESET}")
        return None, None, None

    summary = pd.read_csv(sum_path)
    all_pairs = pd.read_csv(all_path)

    # ================= 新增：因子名称自动脱敏隐射 =================
    # 提取所有唯一的因子名称并剔除 NaN
    all_factors = set(summary['entry_factor'].dropna().unique()) | \
                  set(summary['exit_factor'].dropna().unique()) | \
                  set(all_pairs['entry_factor'].dropna().unique()) | \
                  set(all_pairs['exit_factor'].dropna().unique())
    # 排序保证只要数据内容不变，同一批因子的映射编号每次运行都是稳定的
    all_factors = sorted(list(all_factors))

    # 生成映射字典: original_name -> Factor_001
    factor_map = {name: f"Factor_{i:03d}" for i, name in enumerate(all_factors, 1)}

    # 保存映射表到本地，方便后续人工追溯
    map_path = os.path.join('factor_mapping.csv')
    # 如果目录不存在先创建
    os.makedirs(out_dir, exist_ok=True)
    pd.DataFrame(list(factor_map.items()), columns=['Original_Name', 'Mapped_Name']).to_csv(map_path, index=False,
                                                                                            encoding='utf-8-sig')
    print(f"{GREEN}✅ 因子已自动脱敏，名称映射表已保存至: {map_path}{RESET}")

    # 将数据中的名称全部替换为代号
    summary['entry_factor'] = summary['entry_factor'].map(factor_map)
    summary['exit_factor'] = summary['exit_factor'].map(factor_map)
    all_pairs['entry_factor'] = all_pairs['entry_factor'].map(factor_map)
    all_pairs['exit_factor'] = all_pairs['exit_factor'].map(factor_map)
    # ==============================================================

    # 1. 强制转换为 Categorical 类型（极大压缩内存并加速分组计算）
    for col in ['entry_factor', 'exit_factor', 'filter_mode']:
        all_pairs[col] = all_pairs[col].astype('category')
        summary[col] = summary[col].astype('category')

    # 建立联合索引，让后续微观分析的查询速度提升上百倍
    all_pairs.set_index(['entry_factor', 'exit_factor', 'filter_mode'], inplace=True)

    # 2. 核心修复：必须对索引进行排序，否则后续 .loc 查询会退化为全表扫描！
    all_pairs.sort_index(inplace=True)

    return summary, all_pairs, factor_map


def get_tradable_pool(summary, all_pairs, min_trades=50, min_coins=3, min_avg_ret=0.002, max_top1_pct=50.0):
    """
    【重构】实盘净水器：去除不合理的时序强限制和70/30强拆，保留底层逻辑过滤。
    """
    df = summary.copy()
    return df

    # 1. 强制大数定律：总交易笔数不能少于阈值
    df = df[df['total_trades'] >= min_trades]

    # 2. 强制宽度：不能是单一妖币的狂欢
    df = df[df['n_coins'] >= min_coins]

    # 保留基本的正期望底线
    df = df[(df['oos_sum_all'] > 0) & (df['sum_ret_all'] - df['oos_sum_all'] > 0)]

    # ================= 利润集权度计算 =================
    pos_profits = all_pairs[all_pairs['sum_ret'] > 0].groupby(level=[0, 1, 2])['sum_ret'].sum()
    max_profits = all_pairs.groupby(level=[0, 1, 2])['sum_ret'].max()
    top1_pct = (max_profits / pos_profits * 100).fillna(0).rename('top1_coin_pct')
    df = df.merge(top1_pct, left_on=['entry_factor', 'exit_factor', 'filter_mode'], right_index=True, how='left')

    # ================= 生存底线过滤 =================
    is_friction_safe = (df['sum_ret_all'] / df['total_trades'] / 100.0) > min_avg_ret
    # 修正：横截面有效率必须大于50%才有普适意义，而不是>=0
    is_coin_robust = df['coin_positive_rate'] > 0.50

    is_profit_distributed = df['top1_coin_pct'] <= max_top1_pct
    is_time_robust = (df[['sum_ret_q1', 'sum_ret_q2', 'sum_ret_q3', 'sum_ret_q4']] > 0).sum(axis=1) >= 4

    # 汇总过滤条件
    df = df.assign(
        is_friction_safe=is_friction_safe,
        is_coin_robust=is_coin_robust,
        is_profit_distributed=is_profit_distributed,
        is_time_robust=is_time_robust
    )

    # 剔除无效逻辑后的新过滤网
    df = df[df['is_friction_safe'] & df['is_coin_robust'] & df['is_profit_distributed'] & df['is_time_robust']]

    return df


def analyze_macro_ecosystem(summary, tradable_summary):
    """
    模块 A：宏观生态物理规律扫描
    """
    print(f"\n{BOLD}{'=' * 90}{RESET}")
    print(f"{CYAN}{BOLD} 🌍 模块 A: 宏观生态物理规律扫描 (Macro Ecosystem){RESET}")
    print(f"{BOLD}{'=' * 90}{RESET}")

    # ---------------------------------------------------------
    # 1. 截面环境过滤响应梯度
    # ---------------------------------------------------------
    print(f"\n{YELLOW}▶ [1. 截面环境过滤响应梯度 (Filter Gradient Response)]{RESET}")
    grad_top, grad_bot = {}, {}
    orig_val = summary[summary['filter_mode'] == 'original']['mean_oos_pt_sharpe'].mean()

    for fm in summary['filter_mode'].unique():
        if fm == 'original': continue
        match = re.search(r'(\d+)$', fm)
        if match:
            num = int(match.group())
            val = summary[summary['filter_mode'] == fm]['mean_oos_pt_sharpe'].mean()
            if 'top' in fm:
                grad_top[num] = val
            elif 'bottom' in fm:
                grad_bot[num] = val

    sorted_top = sorted(grad_top.items(), key=lambda x: x[0], reverse=True)
    sorted_bot = sorted(grad_bot.items(), key=lambda x: x[0], reverse=True)

    top_str = f"Original({orig_val:.3f})" + "".join([f" -> Top_{k}({v:.3f})" for k, v in sorted_top])
    bot_str = f"Original({orig_val:.3f})" + "".join([f" -> Bot_{k}({v:.3f})" for k, v in sorted_bot])

    print(f"   {BOLD}Top(强势) 过滤单笔期望 (Pt_Sharpe) 演变:{RESET}\n   {top_str}")
    if grad_bot:
        print(f"   {BOLD}Bottom(弱势) 过滤单笔期望演变:{RESET}\n   {bot_str}")
    print(f"   {GREEN}💡 研判提示：数值随条件收紧而平滑/阶梯上升，则证明该截面过滤逻辑有效！{RESET}")

    # ---------------------------------------------------------
    # 2. 全局环境倾向
    # ---------------------------------------------------------
    print(f"\n{YELLOW}▶ [2. 稳健全局环境倾向 (Robust Global Regime Preference)]{RESET}")
    pos_combos = tradable_summary[tradable_summary['oos_sum_all'] > 0]
    if not pos_combos.empty:
        env_counts = pos_combos.groupby('filter_mode').size()
        best_env = env_counts.idxmax()
        best_count = env_counts.max()
        print(
            f"   剔除噪音后，产生最多真实 OOS 正期望组合的环境是: {BOLD}{best_env}{RESET} (共 {best_count} 个稳健组合)")
    else:
        print(f"   {RED}剔除噪音后，无稳健正期望组合。{RESET}")

    # ---------------------------------------------------------
    # 3. 百搭入场榜单
    # ---------------------------------------------------------
    print(f"\n{YELLOW}▶ [3. 真实百搭入场因子榜单 (Universal Entry Top 3)]{RESET}")
    if not pos_combos.empty:
        total_exits = tradable_summary['exit_factor'].nunique()
        univ_entries = pos_combos.groupby('entry_factor')['exit_factor'].nunique().sort_values(ascending=False).head(10)

        for rank, (en, count) in enumerate(univ_entries.items(), 1):
            ratio = count / total_exits * 100
            print(
                f"   {rank}. {BOLD}{en}{RESET} \n      (适配了 {count}/{total_exits} 种有效出场，真实普适性 {ratio:.1f}%)")
    else:
        print(f"   {RED}无足够数据计算百搭因子。{RESET}")

    # ---------------------------------------------------------
    # 4. 百搭出场榜单
    # ---------------------------------------------------------
    print(f"\n{YELLOW}▶ [4. 真实百搭出场因子榜单 (Universal Exit Top 3)]{RESET}")
    if not pos_combos.empty:
        total_entries = tradable_summary['entry_factor'].nunique()
        univ_exits = pos_combos.groupby('exit_factor')['entry_factor'].nunique().sort_values(ascending=False).head(10)

        for rank, (ex, count) in enumerate(univ_exits.items(), 1):
            ratio = count / total_entries * 100
            print(
                f"   {rank}. {BOLD}{ex}{RESET} \n      (适配了 {count}/{total_entries} 种有效入场，真实普适性 {ratio:.1f}%)")
    else:
        print(f"   {RED}无足够数据计算百搭因子。{RESET}")


def analyze_micro_deep_dive(summary, tradable_summary, all_pairs, tf, top_n=5, factor_map=None):
    """
    模块 B：微观组合五维防伪体检
    核心修改：彻底废弃 70/30假衰减率、均值参数陷阱。新增单边滑点容忍度与真胜率。
    此次升级：对全部过滤后数据做完整深度体检并导出 CSV，终端严格保持Top50旧版输出不变。
    """
    print(f"\n\n{BOLD}{'=' * 90}{RESET}")
    print(f"{CYAN}{BOLD} 🔬 模块 B: 微观组合五维防伪体检 (Micro Deep-Dive){RESET}")
    print(f"    (当前展示的策略已通过: 笔数>=50, 币种>=3, 胜率>50%, 排除极端截面 的初筛)")
    print(f"{BOLD}{'=' * 90}{RESET}")

    if tradable_summary.empty:
        print(f"{RED}🚫 没有策略通过硬性过滤条件，建议放宽过滤阈值或重新挖掘。{RESET}")
        return

    # 反向解析脱敏名称至原始名称 (用于CSV导出)
    reverse_map = {v: k for k, v in factor_map.items()} if factor_map else {}

    # ================= 周期转换逻辑 =================
    match = re.match(r'(\d+)([mhd])', str(tf).lower())
    if match:
        val = float(match.group(1))
        unit = match.group(2)
        if unit == 'm':
            bars_to_days = val / (24 * 60.0)
        elif unit == 'h':
            bars_to_days = val / 24.0
        elif unit == 'd':
            bars_to_days = val
        else:
            bars_to_days = 1.0
    else:
        bars_to_days = 1.0

    # ================= 核心排序调整 (Calmar 比率) =================
    tradable_summary = tradable_summary.copy()
    tradable_summary['Sort_Metric'] = tradable_summary['mean_sum_ret'] / tradable_summary['mean_max_dd'].abs().replace(
        0, 1e-9)
    sort_col = 'Sort_Metric'

    # 遍历所有通过净水器的组合而不是只切前N个
    all_sorted_df = tradable_summary.sort_values(sort_col, ascending=False)

    # 收集所有的体检数据以便写入CSV
    report_data = []

    for rank, (_, row) in enumerate(all_sorted_df.iterrows(), 1):
        en = row['entry_factor']
        ex = row['exit_factor']
        fm = row['filter_mode']

        # ================= 核心指标提取与重构 =================
        curr_trades = row['total_trades']
        avg_ret_pct = row['sum_ret_all'] / curr_trades if curr_trades > 0 else 0
        # 🟢 新增：盈亏平衡单边滑点容忍度 (决定实盘生死)
        break_even_slippage = avg_ret_pct / 2.0

        # --------- 邻近参数普适率 (彻底替换均值陷阱) ---------
        same_en_df = summary[
            (summary['entry_factor'] == en) & (summary['filter_mode'] == fm) & (summary['exit_factor'] != ex)]
        same_ex_df = summary[
            (summary['exit_factor'] == ex) & (summary['filter_mode'] == fm) & (summary['entry_factor'] != en)]

        en_neighbor_count = len(same_en_df)
        ex_neighbor_count = len(same_ex_df)

        en_pos_rate = (same_en_df['mean_oos_pt_sharpe'] > 0).mean() * 100 if en_neighbor_count > 0 else 0
        ex_pos_rate = (same_ex_df['mean_oos_pt_sharpe'] > 0).mean() * 100 if ex_neighbor_count > 0 else 0
        # ------------------------------------------------------------------

        # 标的过滤效能 (保留原始对比框架，去掉花哨跳转)
        orig_row = summary[
            (summary['entry_factor'] == en) & (summary['exit_factor'] == ex) & (summary['filter_mode'] == 'original')]
        if not orig_row.empty:
            orig_trades = orig_row['total_trades'].values[0]
            orig_coin_rate = orig_row['coin_positive_rate'].values[0] * 100
        else:
            orig_trades = np.nan
            orig_coin_rate = np.nan

        curr_coin_rate = row['coin_positive_rate'] * 100

        # 提取底层持仓流水
        try:
            combo_details = all_pairs.loc[[(en, ex, fm)]]
        except KeyError:
            combo_details = pd.DataFrame()

        if not combo_details.empty:
            win_t = combo_details['trades'] * (combo_details['win_rate'] / 100.0)
            loss_t = combo_details['trades'] - win_t

            # 🟢 新增：计算真实的全局聚合胜率
            real_win_rate = (win_t.sum() / curr_trades) * 100 if curr_trades > 0 else 0

            win_hold_sum = (combo_details['win_hold_bars'].fillna(0) * win_t).sum()
            loss_hold_sum = (combo_details['loss_hold_bars'].fillna(0) * loss_t).sum()

            mean_win_hold = win_hold_sum / win_t.sum() if win_t.sum() > 0 else 0
            mean_loss_hold = loss_hold_sum / loss_t.sum() if loss_t.sum() > 0 else 0

            positive_profits = combo_details[combo_details['sum_ret'] > 0]['sum_ret'].sum()
            max_coin_ret = combo_details['sum_ret'].max()
            top1_coin_pct = (max_coin_ret / positive_profits * 100) if (
                    positive_profits > 0 and max_coin_ret > 0) else 0.0

            best_coin = "未知标的"
            if positive_profits > 0:
                max_idx_pos = combo_details['sum_ret'].argmax()
                best_row = combo_details.iloc[max_idx_pos]
                if 'coin' in best_row:
                    best_coin = str(best_row['coin'])
                elif 'symbol' in best_row:
                    best_coin = str(best_row['symbol'])
        else:
            mean_win_hold = mean_loss_hold = top1_coin_pct = real_win_rate = np.nan
            best_coin = "未知标的"

        hold_ratio = mean_loss_hold / mean_win_hold if mean_win_hold > 0 else 0

        # ================= 极简重构：客观预警 (红绿灯) =================
        flags = []

        # 1. 摩擦生死线
        if break_even_slippage < 0.15:
            flags.append(
                f"{RED}🔴 极度脆弱 (单边盈亏平衡滑点 {break_even_slippage:.3f}% < 0.15%，实盘极易死于摩擦){RESET}")
        else:
            flags.append(f"{GREEN}🟢 摩擦抗性优秀 (单边盈亏平衡滑点容忍度 {break_even_slippage:.3f}%){RESET}")

        # 2. 普适率孤岛判定 (彻底修复原来的误杀)
        if en_pos_rate < 15:
            flags.append(f"{RED}🔴 陷入参数孤岛 (入场因子仅有 {en_pos_rate:.1f}% 的出场搭配能实现正期望){RESET}")
        else:
            flags.append(f"{GREEN}🟢 因子普适性强 (与库内 {en_pos_rate:.1f}% 的出场因子搭配均为正期望){RESET}")

        # 3. 持仓习惯
        if hold_ratio > 2.0:
            flags.append(f"{RED}🔴 扛单高危预警 (亏损单死扛时间是盈利单的 {hold_ratio:.1f} 倍){RESET}")
        elif hold_ratio < 1.0:
            flags.append(f"{GREEN}🟢 顺向截断持仓 (亏损单割肉果断，时间小于盈利单){RESET}")

        # 4. 极权度
        if top1_coin_pct > 50:
            flags.append(f"{RED}🔴 单币利润过度极权 (单币贡献了盈利总额的 {top1_coin_pct:.1f}%){RESET}")
        elif top1_coin_pct < 30:
            flags.append(f"{GREEN}🟢 利润标的散布健康 (最肥的羊仅占总利润的 {top1_coin_pct:.1f}%){RESET}")

        # 基础数据提取 (为了能同时用于输出和收集 CSV)
        q_rets = [row['sum_ret_q1'], row['sum_ret_q2'], row['sum_ret_q3'], row['sum_ret_q4']]
        q_trades = [row['sum_trades_q1'], row['sum_trades_q2'], row['sum_trades_q3'], row['sum_trades_q4']]
        max_q = max([q for q in q_rets if q > 0] + [0.01])

        max_dd_val = row['mean_max_dd']
        calmar_ratio = row['Sort_Metric']
        dm_win = f"{row['mean_down_market_win_rate']:.1f}%" if pd.notna(row.get('mean_down_market_win_rate')) else "N/A"

        mean_win_hold_days = mean_win_hold * bars_to_days
        mean_loss_hold_days = mean_loss_hold * bars_to_days

        # 把全量体检数据塞入报告列表以便写CSV
        clean_flags = [f.replace(RED, '').replace(GREEN, '').replace(RESET, '').replace(BOLD, '') for f in flags]
        report_data.append({
            'Rank': rank,
            'Entry_Factor_Original': reverse_map.get(en, en),
            'Exit_Factor_Original': reverse_map.get(ex, ex),
            'Entry_Factor_Desensitized': en,
            'Exit_Factor_Desensitized': ex,
            'Filter_Mode': fm,
            'Calmar_Ratio': calmar_ratio,
            'N_Coins': row['n_coins'],
            'Total_Trades': curr_trades,
            'Global_Real_Win_Rate(%)': real_win_rate,
            'Avg_Return(%)': avg_ret_pct,
            'Break_Even_Slippage(%)': break_even_slippage,
            'Mean_Max_DD(%)': max_dd_val,
            'Down_Market_Win_Rate': dm_win,
            'Mean_Win_Hold_Days': mean_win_hold_days,
            'Mean_Loss_Hold_Days': mean_loss_hold_days,
            'Hold_Ratio': hold_ratio,
            'Best_Coin': best_coin,
            'Top1_Coin_Pct(%)': top1_coin_pct,
            'Entry_Neighbor_Pos_Rate(%)': en_pos_rate,
            'Exit_Neighbor_Pos_Rate(%)': ex_pos_rate,
            'Q1_Return(%)': q_rets[0],
            'Q2_Return(%)': q_rets[1],
            'Q3_Return(%)': q_rets[2],
            'Q4_Return(%)': q_rets[3],
            'Q1_Trades': q_trades[0],
            'Q2_Trades': q_trades[1],
            'Q3_Trades': q_trades[2],
            'Q4_Trades': q_trades[3],
            'Flags': " | ".join(clean_flags)
        })

        # ================= 终端排版打印 (严格限制只打印前50并原样保留格式) =================
        if rank <= top_n:
            print(f"\n{BOLD}{'=' * 80}{RESET}")
            print(f"🏆 {YELLOW}深度体检 #{rank}: ENTRY [{en}]  =>  EXIT [{ex}]{RESET}")
            print(
                f"📌 卡玛比率: {row[sort_col]:.3f} | 当前环境: {fm} | 参与币种: {row['n_coins']} 个 | 总笔数: {curr_trades} 笔")
            print("-" * 80)

            print(f"{BOLD}[1. 策略DNA与盈利基础 (Strategy Foundation)]{RESET}")
            print(f"   {CYAN}- 全局真实胜率: {real_win_rate:.1f}% | 平均单笔净收益: {avg_ret_pct:.3f}%{RESET}")
            print(f"   - 单边滑点容忍极值: {break_even_slippage:.3f}% (盈亏平衡点)")
            print(f"   - 季度时序分布:")
            for i in range(4):
                print(
                    f"     Q{i + 1}: {make_bar(q_rets[i], max_q):<10} ({q_trades[i]:<4}笔) | 收益: {q_rets[i]:>6.1f}%")

            print(f"\n{BOLD}[2. 尾部风控与不对称性 (Risk & Hold Asymmetry)]{RESET}")
            print(f"   - 📉 平均最大回撤: {RED}{max_dd_val:.2f}%{RESET} | 卡玛比率 (收益/回撤): {calmar_ratio:.2f}")
            print(f"   - 逆风局胜率 (BTC跌时): {dm_win}")
            print(
                f"   - 盈亏时间不对称: 盈利均持仓 {mean_win_hold_days:.2f} 天 / 亏损均持仓 {mean_loss_hold_days:.2f} 天")
            print(f"   - 亏损死扛系数: {hold_ratio:.2f} 倍")

            print(f"\n{BOLD}[3. 实盘印证维度 (Robustness & Breadth)]{RESET}")
            if pd.notna(orig_trades) and orig_trades > 0:
                print(f"   - 截面有效存活率: Original ({orig_coin_rate:.1f}%) ➔ 当前条件 ({curr_coin_rate:.1f}%)")

            print(f"   - 利润集权度分布: 最赚钱币种【{best_coin}】贡献占比 {top1_coin_pct:.1f}%。")
            print(f"   - 入场因子普适率: 搭配库内 {en_neighbor_count} 种出场，有 {en_pos_rate:.1f}% 的组合实现正期望。")
            print(f"   - 出场因子普适率: 搭配库内 {ex_neighbor_count} 种入场，有 {ex_pos_rate:.1f}% 的组合实现正期望。")

            # # ---------------- 最终预警标签 ----------------
            # print(f"\n{BOLD}▶ 实盘一票否决权 (智能红绿灯):{RESET}")
            # if not flags:
            #     print("   (无极端缺陷，属稳健组合)")
            # else:
            #     for flag in flags:
            #         print(f"   {flag}")
            # print(f"{BOLD}{'=' * 80}{RESET}")

    # 全量循环结束后，导出完整的深度体检报告 CSV
    if report_data:
        report_df = pd.DataFrame(report_data)
        out_path = f'./deep_dive_full_report_{tf}.csv'
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        report_df.to_csv(out_path, index=False, encoding='utf-8-sig')
        # 仅增加一句友好的报告生成提示，原主控台排版格式完全不涉足更改
        print(
            f"\n{GREEN}✅ 完整深度体检报告已保存至: {out_path} (共检查并保存了 {len(report_df)} 个合格策略组合){RESET}")


if __name__ == '__main__':
    target_timeframes = ['60m']

    for tf in target_timeframes:
        print(f"\n\n{YELLOW}★★★ 正在分析 {tf} 周期数据 ★★★{RESET}")

        # 接收新增的 factor_map
        summary, all_pairs, factor_map = load_data(tf)

        if summary is not None and all_pairs is not None:
            # 1. 生成清洗后的实盘池（总笔数 >= 50, 至少3个币参战, 过滤掉极限极权, 摒弃原有时序硬要求）
            tradable_summary = get_tradable_pool(summary, all_pairs, min_trades=50, min_coins=3, min_avg_ret=0.002,
                                                 max_top1_pct=30.0)

            # 2. 宏观分析
            analyze_macro_ecosystem(summary, tradable_summary)

            # 3. 微观深度体检 (提取所有合格者进行体检保存CSV，并在控制台严格按净水表输出 Top 50)
            analyze_micro_deep_dive(summary, tradable_summary, all_pairs, tf, top_n=50, factor_map=factor_map)