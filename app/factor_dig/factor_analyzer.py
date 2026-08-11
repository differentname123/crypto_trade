# -*- coding: utf-8 -*-
"""
================================================================================
 因子组合 深度防伪体检与宏观生态分析 (Pro Version 2.0 - 实盘净水器版)
--------------------------------------------------------------------------------
 核心定位：你的“最终实盘拍板”验金石。
 本次升级：前置隔离“妖币拟合”与“单笔幸存者偏差”，确保进入 Top 解剖台的策略
           全部具备【统计显著性】与【横截面普适性】。
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
        return None, None

    summary = pd.read_csv(sum_path)
    all_pairs = pd.read_csv(all_path)

    # 建立联合索引，让后续微观分析的查询速度提升上百倍
    all_pairs.set_index(['entry_factor', 'exit_factor', 'filter_mode'], inplace=True)

    return summary, all_pairs


def get_tradable_pool(summary, all_pairs, min_trades=50, min_coins=3, min_avg_ret=0.002, max_top1_pct=50.0,
                      max_decay_rate=40.0):
    """
    【新增】实盘净水器：在进入深度分析前，强制剔除统计学意义上的噪音。
    引入 max_top1_pct：强制过滤利润过度集权的策略。
    引入 max_decay_rate：强制过滤 OOS 收益率衰减过大的过拟合策略。
    """
    df = summary.copy()

    # 1. 强制大数定律：总交易笔数不能少于阈值
    df = df[df['total_trades'] >= min_trades]

    # 2. 强制宽度：不能是单一妖币的狂欢
    df = df[df['n_coins'] >= min_coins]

    df = df[(df['oos_sum_all'] > 0) & (df['sum_ret_all'] - df['oos_sum_all'] > 0)]

    # ================= 核心修改：增加利润集权度计算 =================
    # a. 计算每个因子组合中，所有赚钱的币种的“总盈利额”
    pos_profits = all_pairs[all_pairs['sum_ret'] > 0].groupby(level=[0, 1, 2])['sum_ret'].sum()

    # b. 计算每个因子组合中，赚得最多的“单币最大盈利额”
    max_profits = all_pairs.groupby(level=[0, 1, 2])['sum_ret'].max()

    # c. 计算集权度比例 (单币最大盈利 / 总盈利 * 100)
    top1_pct = (max_profits / pos_profits * 100).fillna(0).rename('top1_coin_pct')

    # 将计算结果合并回主表 (利用组合键合并)
    df = df.merge(top1_pct, left_on=['entry_factor', 'exit_factor', 'filter_mode'], right_index=True, how='left')
    # ================================================================

    # ================= 新增：生存底线过滤 (硬门槛) =================
    # 抗摩擦底线: 平均单笔收益必须大于绝对阈值 (例如千分之二)
    is_friction_safe = df['mean_avg_ret'] > min_avg_ret

    # 横截面底线: coin_positive_rate 必须 >= 0.0
    is_coin_robust = df['coin_positive_rate'] >= 0.0

    # 时序底线: 4 个季度中，至少有 4 个季度的 sum_ret_qX 为正
    is_time_robust = (df[['sum_ret_q1', 'sum_ret_q2', 'sum_ret_q3', 'sum_ret_q4']] > 0).sum(axis=1) >= 4

    # 【新增】利润分散底线: 单一币种利润贡献不能超过阈值（如 50%）
    is_profit_distributed = df['top1_coin_pct'] <= max_top1_pct

    # ================= 【新增】时序衰减底线 (防范 -56.9% 塌方) =================
    is_trades_arr = np.maximum(df['total_trades'] * 0.7, 1)
    oos_trades_arr = np.maximum(df['total_trades'] * 0.3, 1)
    is_pt_ret_arr = (df['sum_ret_all'] - df['oos_sum_all']) / is_trades_arr
    oos_pt_ret_arr = df['oos_sum_all'] / oos_trades_arr

    decay_rate_arr = (oos_pt_ret_arr / is_pt_ret_arr - 1) * 100
    # 必须保证 IS 赚钱，且 OOS 折损率不能低于负的允许最大值 (例如允许衰减40%，则过滤掉跌幅超 -40% 的策略)
    is_decay_safe = (is_pt_ret_arr > 0) & (decay_rate_arr >= -max_decay_rate)
    # =========================================================================

    # 在代码汇总时，增加 Boolean 列
    df = df.assign(
        is_friction_safe=is_friction_safe,
        is_coin_robust=is_coin_robust,
        is_time_robust=is_time_robust,
        is_profit_distributed=is_profit_distributed,  # 注入新标签
        is_decay_safe=is_decay_safe  # 注入衰减防护网标签
    )

    # 任何一项为 False 的策略，直接在最终排序前剔除
    df = df[df['is_friction_safe'] & df['is_coin_robust'] & df['is_time_robust'] & df['is_profit_distributed'] & df[
        'is_decay_safe']]
    # ================================================================

    return df


def analyze_macro_ecosystem(summary, tradable_summary):
    """
    模块 A：宏观生态物理规律扫描
    注意：物理规律梯度用全量数据看，但百搭因子必须用清洗后的 tradable_summary 看。
    """
    print(f"\n{BOLD}{'=' * 90}{RESET}")
    print(f"{CYAN}{BOLD} 🌍 模块 A: 宏观生态物理规律扫描 (Macro Ecosystem){RESET}")
    print(f"{BOLD}{'=' * 90}{RESET}")

    # ---------------------------------------------------------
    # 1. 截面环境过滤响应梯度 (用全量 summary，展示物理规律的极限)
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
    # 2. 全局环境倾向 (用清洗后的 tradable_summary)
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
    # 3. 百搭入场榜单 (用清洗后的 tradable_summary)
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
    # 4. 百搭出场榜单 (用清洗后的 tradable_summary) 【新增模块】
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


def analyze_micro_deep_dive(summary, tradable_summary, all_pairs, top_n=5):
    """
    模块 B：微观组合五维防伪体检
    注意：使用 tradable_summary 进行排序和抽取，彻底屏蔽“1笔交易爆赚1500%”的妖孽策略。
    """
    print(f"\n\n{BOLD}{'=' * 90}{RESET}")
    print(f"{CYAN}{BOLD} 🔬 模块 B: 微观组合五维防伪体检 (Micro Deep-Dive){RESET}")
    print(f"    (当前展示的策略已通过: 笔数>=50, 币种>=3, 排除极端截面 的初筛)")
    print(f"{BOLD}{'=' * 90}{RESET}")

    if tradable_summary.empty:
        print(f"{RED}🚫 没有策略通过硬性过滤条件，建议放宽过滤阈值或重新挖掘。{RESET}")
        return

    # ================= 核心排序调整 (Calmar 比率思想) =================
    tradable_summary = tradable_summary.copy()
    tradable_summary['Sort_Metric'] = tradable_summary['mean_sum_ret'] / tradable_summary['mean_max_dd'].abs().replace(
        0, 1e-9)
    sort_col = 'Sort_Metric'
    top_df = tradable_summary.sort_values(sort_col, ascending=False).head(top_n)
    # ======================================================================

    for rank, (_, row) in enumerate(top_df.iterrows(), 1):
        en = row['entry_factor']
        ex = row['exit_factor']
        fm = row['filter_mode']

        # ================= 数据提取与计算区 =================
        oos_ret = row['oos_sum_all']
        is_ret = row['sum_ret_all'] - oos_ret

        is_trades = max(row['total_trades'] * 0.7, 1)
        oos_trades = max(row['total_trades'] * 0.3, 1)

        is_pt_ret = is_ret / is_trades
        oos_pt_ret = oos_ret / oos_trades

        if is_pt_ret <= 0:
            decay_rate = -999.0
        else:
            decay_rate = (oos_pt_ret / is_pt_ret - 1) * 100

        # --------- 邻近参数平原验证计算 (增加计算参与比对的因子个数) ---------
        same_en_df = summary[
            (summary['entry_factor'] == en) & (summary['filter_mode'] == fm) & (summary['exit_factor'] != ex)]
        same_ex_df = summary[
            (summary['exit_factor'] == ex) & (summary['filter_mode'] == fm) & (summary['entry_factor'] != en)]

        en_neighbor_avg = same_en_df['mean_oos_pt_sharpe'].mean() if not same_en_df.empty else np.nan
        ex_neighbor_avg = same_ex_df['mean_oos_pt_sharpe'].mean() if not same_ex_df.empty else np.nan

        # 新增：记录有多少个因子参与了对比测试
        en_neighbor_count = len(same_en_df)
        ex_neighbor_count = len(same_ex_df)
        # ------------------------------------------------------------------

        # 标的过滤效能 (对比 Original 环境)
        orig_row = summary[
            (summary['entry_factor'] == en) & (summary['exit_factor'] == ex) & (summary['filter_mode'] == 'original')]
        if not orig_row.empty:
            orig_pt_sharpe = orig_row['mean_oos_pt_sharpe'].values[0]
            orig_trades = orig_row['total_trades'].values[0]
        else:
            orig_pt_sharpe = orig_trades = np.nan

        curr_pt_sharpe = row['mean_oos_pt_sharpe']
        curr_trades = row['total_trades']

        # 提取底层持仓流水
        mask = (all_pairs.index.get_level_values(0) == en) & \
               (all_pairs.index.get_level_values(1) == ex) & \
               (all_pairs.index.get_level_values(2) == fm)
        combo_details = all_pairs[mask]

        if not combo_details.empty:
            win_t = combo_details['trades'] * (combo_details['win_rate'] / 100.0)
            loss_t = combo_details['trades'] - win_t

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
            mean_win_hold = mean_loss_hold = top1_coin_pct = np.nan
            best_coin = "未知标的"

        # ================= 生成客观预警 (红绿灯) =================
        flags = []
        if decay_rate == -999.0:
            flags.append(f"{RED}🔴 IS 样本内收益为负或极低，无对比基准，废弃。{RESET}")
        elif decay_rate > 50:
            flags.append(f"{YELLOW}🟡 逆向畸高预警 (OOS 收益反超 IS 50%以上，警惕大盘顺风车假象){RESET}")
        elif decay_rate > 20:
            flags.append(f"{YELLOW}🟡 OOS 逆向提升 (收益升幅 {decay_rate:.1f}%，需人工复核){RESET}")
        elif -40 <= decay_rate <= 20:
            flags.append(f"{GREEN}🟢 OOS 时序平稳 (年化折损 {decay_rate:.1f}%，极其健康){RESET}")
        else:
            flags.append(f"{RED}🔴 OOS 严重衰减 (衰减 {decay_rate:.1f}%){RESET}")

        if top1_coin_pct > 50:
            flags.append(f"{RED}🔴 单币利润过度极权 (单币贡献了盈利总额的 {top1_coin_pct:.1f}%){RESET}")
        elif top1_coin_pct < 30:
            flags.append(f"{GREEN}🟢 利润标的散布健康 (最肥的羊仅占总盈利的 {top1_coin_pct:.1f}%){RESET}")

        if mean_loss_hold > mean_win_hold * 2:
            flags.append(f"{RED}🔴 逆向持仓预警 (亏损单死扛时间是盈利单的1.5倍以上){RESET}")
        elif mean_win_hold > mean_loss_hold:
            flags.append(f"{GREEN}🟢 顺向持仓 (截断亏损，让利润奔跑){RESET}")

        if pd.isna(en_neighbor_avg) or pd.isna(ex_neighbor_avg):
            flags.append(f"{YELLOW}🟡 缺乏邻近参数数据 (无法验证平原特征){RESET}")
        elif en_neighbor_avg <= 0 or ex_neighbor_avg <= 0:
            flags.append(f"{RED}🔴 陷入参数孤岛 (更换同类进/出场即变为负期望){RESET}")
        else:
            flags.append(f"{GREEN}🟢 参数平原宽广 (周边逻辑全部验证有效){RESET}")

        # ================= 终端排版打印 =================
        print(f"\n{BOLD}{'=' * 80}{RESET}")
        print(f"🏆 {YELLOW}深度体检 #{rank}: ENTRY [{en}]  =>  EXIT [{ex}]{RESET}")
        print(
            f"📌 排名依据: {sort_col} = {row[sort_col]:.3f} | 当前环境: {fm} | 参与币种: {row['n_coins']} 个 | 总笔数: {curr_trades} 笔")
        print("-" * 80)

        # [1] 时序平稳性
        q_rets = [row['sum_ret_q1'], row['sum_ret_q2'], row['sum_ret_q3'], row['sum_ret_q4']]
        q_trades = [row['sum_trades_q1'], row['sum_trades_q2'], row['sum_trades_q3'], row['sum_trades_q4']]
        max_q = max([q for q in q_rets if q > 0] + [0.01])
        print(f"{BOLD}[1. 时序平稳与单笔净利 (Temporal & Profit)]{RESET}")
        print(f"   {CYAN}- 单笔平均净水: OOS 每笔赚 {oos_pt_ret:.3f}% (IS 为 {is_pt_ret:.3f}%){RESET}")
        print(f"   - OOS 真实折损: {decay_rate:.1f}%  (IS总计 {is_ret:.1f}% -> OOS总计 {oos_ret:.1f}%)")
        print(f"   - 季度分布:")
        for i in range(4):
            print(f"     Q{i + 1}: {make_bar(q_rets[i], max_q):<10} ({q_trades[i]:<4}笔) | 收益: {q_rets[i]:>6.1f}%")

        # [2] 截面主动过滤效能
        print(f"\n{BOLD}[2. 主动过滤效能 (Filter Efficacy - 待你裁决)]{RESET}")
        if pd.notna(orig_pt_sharpe) and orig_trades > 0:
            retention_rate = curr_trades / orig_trades * 100
            orig_coin_rate = orig_row['coin_positive_rate'].values[0] * 100
            curr_coin_rate = row['coin_positive_rate'] * 100

            print(f"   - 单笔期望跃升: Original ({orig_pt_sharpe:.3f}) ➔ 当前 ({curr_pt_sharpe:.3f})")
            print(f"   - 交易笔数保留: {curr_trades}/{int(orig_trades)} 笔 (保留了 {retention_rate:.1f}% 的交易)")
            print(f"   - 存活标的胜率: Original ({orig_coin_rate:.1f}%) ➔ 当前 ({curr_coin_rate:.1f}%)")
        else:
            print("   - (无 Original 对比数据，当前可能本身就是 Original)")

        # [3] 参数平原防伪 (此处更新了因子个数的输出)
        print(f"\n{BOLD}[3. 邻近参数平原验证 (Parameter Plain)]{RESET}")
        print(
            f"   - 保持当前进场，搭配库内【 {en_neighbor_count} 种 】其它出场 -> OOS 平均单笔期望: {en_neighbor_avg:.3f}")
        print(
            f"   - 保持当前出场，搭配库内【 {ex_neighbor_count} 种 】其它进场 -> OOS 平均单笔期望: {ex_neighbor_avg:.3f}")

        # [4] 尾部风险与持仓不对称
        print(f"\n{BOLD}[4. 尾部风险与持仓特征 (Risk & Hold Asymmetry)]{RESET}")
        dm_win = f"{row['mean_down_market_win_rate']:.1f}%" if pd.notna(row.get('mean_down_market_win_rate')) else "N/A"
        skew_val = f"{row['mean_skew']:.2f}" if pd.notna(row.get('mean_skew')) else "N/A"
        print(f"   - 逆风局胜率 (BTC跌时): {dm_win}  |  偏度 (Skew): {skew_val}")
        print(f"   - 盈亏不对称: 盈利单均持仓 {mean_win_hold:.1f} Bars / 亏损单均持仓 {mean_loss_hold:.1f} Bars 持有时间比值 为 {mean_loss_hold / mean_win_hold:.2f} 倍")

        # [5] 广度与利润极权度
        print(f"\n{BOLD}[5. 广度与利润极权度 (Breadth & Concentration)]{RESET}")
        alive_rate = row['coin_positive_rate'] * 100
        print(f"   - 参战胜率面: 过滤后剩余 {row['n_coins']} 个币种参战，其中 {alive_rate:.1f}% 为正期望。")

        if oos_ret <= 0:
            print(f"   - 利润集权度: {RED}整体OOS期望为负，极权度指标失效{RESET}")
        else:
            print(f"   - 利润集权度: 盈利最高的【{best_coin}】贡献了总利润的 {top1_coin_pct:.1f}%。")

        # ---------------- 最终预警标签 ----------------
        print(f"\n{BOLD}▶ 智能红绿灯研判提示:{RESET}")
        if not flags:
            print("   (无明显优劣极端特征，属中庸组合)")
        else:
            for flag in flags:
                print(f"   {flag}")
        print(f"{BOLD}{'=' * 80}{RESET}")

if __name__ == '__main__':
    target_timeframes = ['60m']

    for tf in target_timeframes:
        print(f"\n\n{YELLOW}★★★ 正在分析 {tf} 周期数据 ★★★{RESET}")
        summary, all_pairs = load_data(tf)

        if summary is not None and all_pairs is not None:
            # 1. 生成清洗后的实盘池（总笔数 >= 50, 至少3个币参战, 单笔利润>0.002, 利润极权上限40%, 允许最大衰减40%）
            tradable_summary = get_tradable_pool(summary, all_pairs, min_trades=50, min_coins=3, min_avg_ret=0.002,
                                                 max_top1_pct=20.0, max_decay_rate=40.0)

            # 2. 宏观分析 (传入原始表为了看物理极值，传入净水表为了看真实的百搭规律)
            analyze_macro_ecosystem(summary, tradable_summary)

            # 3. 微观深度体检 (严格在净水表里提取 Top 10)
            analyze_micro_deep_dive(summary, tradable_summary, all_pairs, top_n=50)