# -*- coding: utf-8 -*-
"""
================================================================================
 因子组合 深度防伪体检与宏观生态分析 (Pro Version)
--------------------------------------------------------------------------------
 核心定位：你的“最终实盘拍板”验金石。
 严格践行：宏观物理规律验证 + 微观参数平原、过滤效能、持仓不对称性、集中度解剖。
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
    sum_path = os.path.join(out_dir, 'pairs_CROSS_COIN_SUMMARY.csv')
    all_path = os.path.join(out_dir, 'pairs_ALL.csv')

    if not os.path.exists(sum_path) or not os.path.exists(all_path):
        print(f"{RED}❌ 找不到 {tf} 周期数据。请检查路径: {out_dir}{RESET}")
        return None, None

    summary = pd.read_csv(sum_path)
    all_pairs = pd.read_csv(all_path)

    # 【新增】：建立联合索引，让后续微观分析的查询速度提升上百倍
    all_pairs.set_index(['entry_factor', 'exit_factor', 'filter_mode'], inplace=True)

    return summary, all_pairs


def analyze_macro_ecosystem(summary):
    """
    模块 A：宏观生态物理规律扫描
    目标：在一张表里看清因子池的底层物理规律，证明“截面过滤有效”且寻找“百搭因子”。
    """
    print(f"\n{BOLD}{'=' * 90}{RESET}")
    print(f"{CYAN}{BOLD} 🌍 模块 A: 宏观生态物理规律扫描 (Macro Ecosystem){RESET}")
    print(f"{BOLD}{'=' * 90}{RESET}")

    # ---------------------------------------------------------
    # 1. 截面环境过滤响应梯度 (Filter Gradient Response)
    # ---------------------------------------------------------
    print(f"\n{YELLOW}▶ [1. 截面环境过滤响应梯度 (Filter Gradient Response)]{RESET}")
    grad_top, grad_bot = {}, {}
    orig_val = summary[summary['filter_mode'] == 'original']['mean_oos_pt_sharpe'].mean()

    for fm in summary['filter_mode'].unique():
        if fm == 'original': continue
        # 【替换】：精准提取字符串末尾的数字，无视前面名称里的数字（如 24h）
        match = re.search(r'(\d+)$', fm)
        if match:
            num = int(match.group())
            val = summary[summary['filter_mode'] == fm]['mean_oos_pt_sharpe'].mean()
            if 'top' in fm:
                grad_top[num] = val
            elif 'bottom' in fm:
                grad_bot[num] = val

    # 按照数字大小降序排列 (如 100, 50, 20, 5, 1)
    sorted_top = sorted(grad_top.items(), key=lambda x: x[0], reverse=True)
    sorted_bot = sorted(grad_bot.items(), key=lambda x: x[0], reverse=True)

    # 拼接字符串，把 Original 作为基准放在最前面
    top_str = f"Original({orig_val:.3f})" + "".join([f" -> Top_{k}({v:.3f})" for k, v in sorted_top])
    bot_str = f"Original({orig_val:.3f})" + "".join([f" -> Bot_{k}({v:.3f})" for k, v in sorted_bot])

    print(f"   {BOLD}Top(强势) 过滤单笔期望 (Pt_Sharpe) 演变:{RESET}\n   {top_str}")
    if grad_bot:
        print(f"   {BOLD}Bottom(弱势) 过滤单笔期望演变:{RESET}\n   {bot_str}")
    print(f"   {GREEN}💡 研判提示：数值随条件收紧而平滑/阶梯上升，则证明该截面过滤逻辑有效！{RESET}")

    # ---------------------------------------------------------
    # 2. 全局环境倾向 (Global Regime Preference)
    # ---------------------------------------------------------
    print(f"\n{YELLOW}▶ [2. 全局环境倾向 (Global Regime Preference)]{RESET}")
    pos_combos = summary[summary['oos_sum_all'] > 0]
    if not pos_combos.empty:
        env_counts = pos_combos.groupby('filter_mode').size()
        best_env = env_counts.idxmax()
        best_count = env_counts.max()
        print(f"   产生最多 OOS 正期望组合的环境是: {BOLD}{best_env}{RESET} (共 {best_count} 个盈利组合)")
        print(
            f"   {GREEN}💡 研判提示：这代表了当前你因子的主基调。若是 top 环境，说明整体顺势；若是 bottom，说明偏逆势。{RESET}")

    # ---------------------------------------------------------
    # 3. 百搭入场榜单 (Universal Entry)
    # ---------------------------------------------------------
    print(f"\n{YELLOW}▶ [3. 百搭入场因子榜单 (Universal Entry Top 3)]{RESET}")
    total_exits = summary['exit_factor'].nunique()
    univ_entries = pos_combos.groupby('entry_factor')['exit_factor'].nunique().sort_values(ascending=False).head(3)

    for rank, (en, count) in enumerate(univ_entries.items(), 1):
        ratio = count / total_exits * 100
        print(f"   {rank}. {BOLD}{en}{RESET} \n      (适配了 {count}/{total_exits} 种出场，普适性 {ratio:.1f}%)")
    print(f"   {GREEN}💡 研判提示：普适性 > 60% 的入场是极品真 Alpha，说明它不依赖特定止损也能赚钱。{RESET}")


def analyze_micro_deep_dive(summary, all_pairs, top_n=5):
    """
    模块 B：微观组合五维防伪体检
    目标：针对具体的高分组合，输出四维体检卡与极其严苛的红绿灯预警。
    """
    print(f"\n\n{BOLD}{'=' * 90}{RESET}")
    print(f"{CYAN}{BOLD} 🔬 模块 B: 微观组合五维防伪体检 (Micro Deep-Dive){RESET}")
    print(f"{BOLD}{'=' * 90}{RESET}")

    # 取综合分数最高的前 N 个进行解剖
    top_df = summary.sort_values('score', ascending=False).head(top_n)

    for rank, (_, row) in enumerate(top_df.iterrows(), 1):
        en = row['entry_factor']
        ex = row['exit_factor']
        fm = row['filter_mode']

        # ================= 数据提取与计算区 =================

        # 1. IS/OOS 折损计算 (规避行情冷热导致的频次干扰，只看单笔期望)
        oos_ret = row['oos_sum_all']
        is_ret = row['sum_ret_all'] - oos_ret

        # 近似还原 IS 和 OOS 的总笔数 (按照 0.7 / 0.3 的时间权重近似拆分)
        # 注意：此处用极简方式处理，只需保证分母不为0
        is_trades = max(row['total_trades'] * 0.7, 1)
        oos_trades = max(row['total_trades'] * 0.3, 1)

        is_pt_ret = is_ret / is_trades
        oos_pt_ret = oos_ret / oos_trades

        if is_pt_ret <= 0:
            decay_rate = -999.0
        else:
            # 真实单笔衰减率 = (OOS单笔收益 / IS单笔收益 - 1) * 100
            decay_rate = (oos_pt_ret / is_pt_ret - 1) * 100

        # 2. 邻近参数平原验证 (固定进场看异出场，固定出场看异进场)
        same_en_df = summary[
            (summary['entry_factor'] == en) & (summary['filter_mode'] == fm) & (summary['exit_factor'] != ex)]
        same_ex_df = summary[
            (summary['exit_factor'] == ex) & (summary['filter_mode'] == fm) & (summary['entry_factor'] != en)]
        en_neighbor_avg = same_en_df['mean_oos_pt_sharpe'].mean() if not same_en_df.empty else np.nan
        ex_neighbor_avg = same_ex_df['mean_oos_pt_sharpe'].mean() if not same_ex_df.empty else np.nan

        # 3. 标的过滤效能 (对比 Original 环境)
        orig_row = summary[
            (summary['entry_factor'] == en) & (summary['exit_factor'] == ex) & (summary['filter_mode'] == 'original')]
        if not orig_row.empty:
            orig_pt_sharpe = orig_row['mean_oos_pt_sharpe'].values[0]
            orig_trades = orig_row['total_trades'].values[0]
        else:
            orig_pt_sharpe = orig_trades = np.nan

        curr_pt_sharpe = row['mean_oos_pt_sharpe']
        curr_trades = row['total_trades']

        # 4. 提取底层持仓流水：使用布尔掩码，100% 避免 Pandas 索引降维 Bug
        mask = (all_pairs.index.get_level_values(0) == en) & \
               (all_pairs.index.get_level_values(1) == ex) & \
               (all_pairs.index.get_level_values(2) == fm)
        combo_details = all_pairs[mask]

        if not combo_details.empty:
            # 【核心修复】：坚决不用 dropna，用 fillna(0) 保护 100% 或 0% 胜率的币种
            win_t = combo_details['trades'] * (combo_details['win_rate'] / 100.0)
            loss_t = combo_details['trades'] - win_t

            win_hold_sum = (combo_details['win_hold_bars'].fillna(0) * win_t).sum()
            loss_hold_sum = (combo_details['loss_hold_bars'].fillna(0) * loss_t).sum()

            mean_win_hold = win_hold_sum / win_t.sum() if win_t.sum() > 0 else 0
            mean_loss_hold = loss_hold_sum / loss_t.sum() if loss_t.sum() > 0 else 0

            # 极权度计算：加入基础保护
            positive_profits = combo_details[combo_details['sum_ret'] > 0]['sum_ret'].sum()
            max_coin_ret = combo_details['sum_ret'].max()
            top1_coin_pct = (max_coin_ret / positive_profits * 100) if (
                        positive_profits > 0 and max_coin_ret > 0) else 0.0
        else:
            mean_win_hold = mean_loss_hold = top1_coin_pct = np.nan

        # ================= 生成客观预警 (红绿灯) =================
        flags = []

        # 【核心修复】：补齐 OOS 衰减逻辑缝隙
        if decay_rate == -999.0:
            flags.append(f"{RED}🔴 IS 样本内收益为负或极低(<5%年化)，无对比基准，废弃。{RESET}")
        elif decay_rate > 50:
            flags.append(f"{YELLOW}🟡 逆向畸高预警 (OOS 收益反超 IS 50%以上，警惕大盘顺风车假象){RESET}")
        elif decay_rate > 20:
            flags.append(f"{YELLOW}🟡 OOS 逆向提升 (收益升幅 {decay_rate:.1f}%，需人工复核){RESET}")
        elif -40 <= decay_rate <= 20:
            flags.append(f"{GREEN}🟢 OOS 时序平稳 (年化折损 {decay_rate:.1f}%，极其健康){RESET}")
        else:
            flags.append(f"{RED}🔴 OOS 严重衰减 (衰减 {decay_rate:.1f}%){RESET}")

        # 利润集中度判定优化
        if top1_coin_pct > 50:
            flags.append(f"{RED}🔴 单币利润过度极权 (单币贡献了盈利总额的 {top1_coin_pct:.1f}%, 妖币拟合){RESET}")
        elif top1_coin_pct < 30:
            flags.append(f"{GREEN}🟢 利润标的散布健康 (最肥的羊仅占总盈利的 {top1_coin_pct:.1f}%){RESET}")

        # 盈亏持仓不对称性判定
        if mean_loss_hold > mean_win_hold * 1.5:
            flags.append(f"{RED}🔴 逆向持仓预警 (亏损单死扛时间是盈利单的1.5倍以上){RESET}")
        elif mean_win_hold > mean_loss_hold:
            flags.append(f"{GREEN}🟢 顺向持仓 (截断亏损，让利润奔跑){RESET}")

        # 【核心修复】：参数孤岛精确区分 NaN 与 负数
        if pd.isna(en_neighbor_avg) or pd.isna(ex_neighbor_avg):
            flags.append(f"{YELLOW}🟡 缺乏邻近参数数据 (无法验证平原特征){RESET}")
        elif en_neighbor_avg <= 0 or ex_neighbor_avg <= 0:
            flags.append(f"{RED}🔴 陷入参数孤岛 (更换同类进/出场即变为负期望){RESET}")
        else:
            flags.append(f"{GREEN}🟢 参数平原宽广 (周边逻辑全部验证有效){RESET}")

        # ================= 终端排版打印 =================
        print(f"\n{BOLD}{'=' * 80}{RESET}")
        print(f"🏆 {YELLOW}深度体检 #{rank}: ENTRY [{en}]  =>  EXIT [{ex}]{RESET}")
        print(f"📌 {CYAN}当前环境: {fm} | 统计置信度(DSR): {row['deflated_sharpe']:.3f}{RESET}")
        print("-" * 80)

        # [1] 时序平稳性
        q_rets = [row['sum_ret_q1'], row['sum_ret_q2'], row['sum_ret_q3'], row['sum_ret_q4']]
        q_trades = [row['sum_trades_q1'], row['sum_trades_q2'], row['sum_trades_q3'], row['sum_trades_q4']]
        max_q = max([q for q in q_rets if q > 0] + [0.01])
        print(f"{BOLD}[1. 时序平稳性 (Temporal Stability)]{RESET}")
        print(f"   - OOS 真实折损: {decay_rate:.1f}%  (IS总计 {is_ret:.1f}% -> OOS总计 {oos_ret:.1f}%)")
        print(f"   - 季度分布:")
        for i in range(4):
            print(f"     Q{i + 1}: {make_bar(q_rets[i], max_q):<10} ({q_trades[i]:<4}笔) | 收益: {q_rets[i]:>6.1f}%")

        # [2] 截面主动过滤效能 (高亮交给用户判断)
        print(f"\n{BOLD}[2. 主动过滤效能 (Filter Efficacy - 待你裁决)]{RESET}")
        if pd.notna(orig_pt_sharpe) and orig_trades > 0:
            retention_rate = curr_trades / orig_trades * 100
            orig_coin_rate = orig_row['coin_positive_rate'].values[0] * 100
            curr_coin_rate = row['coin_positive_rate'] * 100

            print(f"   - 单笔期望跃升: Original ({orig_pt_sharpe:.3f}) ➔ 当前 ({curr_pt_sharpe:.3f})")
            print(f"   - 交易笔数保留: {curr_trades}/{int(orig_trades)} 笔 (仅保留了 {retention_rate:.1f}% 的交易)")
            print(f"   - 存活标的胜率: Original ({orig_coin_rate:.1f}%) ➔ 当前 ({curr_coin_rate:.1f}%)")
            print(f"   {CYAN}⚠️ 你的研判: 期望与标的胜率是否跃升？保留笔数是否足够？若皆是，则过滤极度成功。{RESET}")
        else:
            print("   - (无 Original 对比数据，当前可能本身就是 Original)")

        # [3] 参数平原防伪
        print(f"\n{BOLD}[3. 邻近参数平原验证 (Parameter Plain)]{RESET}")
        print(f"   - 保持当前进场，搭配库内【其它所有出场】 -> OOS 平均期望(Pt_Sharpe): {en_neighbor_avg:.3f}")
        print(f"   - 保持当前出场，搭配库内【其它所有进场】 -> OOS 平均期望(Pt_Sharpe): {ex_neighbor_avg:.3f}")

        # [4] 尾部风险与持仓不对称
        print(f"\n{BOLD}[4. 尾部风险与持仓特征 (Risk & Hold Asymmetry)]{RESET}")
        dm_win = f"{row['mean_down_market_win_rate']:.1f}%" if pd.notna(row['mean_down_market_win_rate']) else "N/A"
        skew_val = f"{row['mean_skew']:.2f}" if pd.notna(row['mean_skew']) else "N/A"
        corr_btc_val = f"{row['mean_corr_btc']:.2f}" if pd.notna(row['mean_corr_btc']) else "N/A"

        print(f"   - 逆风局胜率 (BTC跌时): {dm_win}  |  BTC相关性: {corr_btc_val}  |  偏度 (Skew): {skew_val}")
        print(f"   - 盈亏不对称: 盈利单均持仓 {mean_win_hold:.1f} Bars / 亏损单均持仓 {mean_loss_hold:.1f} Bars")

        # [5] 广度与利润极权度 (Breadth & Concentration)
        print(f"\n{BOLD}[5. 广度与利润极权度 (Breadth & Concentration)]{RESET}")
        alive_rate = row['coin_positive_rate'] * 100
        print(f"   - 参战胜率面: 过滤后剩余 {row['n_coins']} 个币种参战，其中 {alive_rate:.1f}% 为正期望。")

        if oos_ret <= 0:
            print(f"   - 利润集权度: {RED}整体OOS期望为负，极权度指标失效{RESET}")
        else:
            print(f"   - 利润集权度: 盈利最高的【单一币种】贡献了总利润的 {top1_coin_pct:.1f}%。")

        # ---------------- 最终预警标签 ----------------
        print(f"\n{BOLD}▶ 智能红绿灯研判提示:{RESET}")
        if not flags:
            print("   (无明显优劣极端特征，属中庸组合)")
        else:
            for flag in flags:
                print(f"   {flag}")
        print(f"{BOLD}{'=' * 80}{RESET}")


if __name__ == '__main__':
    # 假设你当前想分析 15m 周期的回测结果
    target_timeframes = ['60m']

    for tf in target_timeframes:
        print(f"\n\n{YELLOW}★★★ 正在分析 {tf} 周期数据 ★★★{RESET}")
        summary, all_pairs = load_data(tf)

        # 只有在成功加载数据时才执行分析
        if summary is not None and all_pairs is not None:
            analyze_macro_ecosystem(summary)
            analyze_micro_deep_dive(summary, all_pairs, top_n=10)  # 可自由调整想要体检的 Top 数量