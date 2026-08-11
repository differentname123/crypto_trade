# -*- coding: utf-8 -*-
"""
================================================================================
 单一因子组合 穿透查档器 (Combo X-Ray Viewer)
--------------------------------------------------------------------------------
 核心定位：输入指定的 入场 + 出场 + 环境，直接调取其完整体检报告与底层币种盈亏明细。
================================================================================
"""
import os
import pandas as pd
import numpy as np

# 控制台颜色高亮代码
GREEN = '\033[92m'
RED = '\033[91m'
YELLOW = '\033[93m'
CYAN = '\033[96m'
RESET = '\033[0m'
BOLD = '\033[1m'


def make_bar(val, max_val, max_len=10):
    """生成极简 ASCII 柱状图"""
    if pd.isna(val) or val <= 0: return "[LOSS]      "
    if max_val <= 0: return ""
    length = max(1, int((val / max_val) * max_len))
    return "▇" * length + " " * (max_len - length)


def view_specific_combo(tf, target_entry, target_exit, target_filter):
    print(f"\n{BOLD}{'=' * 90}{RESET}")
    print(f"{CYAN}{BOLD} 🔍 因子组合穿透查档器 | 周期: {tf}{RESET}")
    print(f"    入场: {YELLOW}{target_entry}{RESET}")
    print(f"    出场: {YELLOW}{target_exit}{RESET}")
    print(f"    环境: {YELLOW}{target_filter}{RESET}")
    print(f"{BOLD}{'=' * 90}{RESET}\n")

    # 1. 检查并加载数据
    out_dir = f'./factor_out_{tf}'
    sum_path = os.path.join(out_dir, 'pairs_CROSS_COIN_SUMMARY.csv')
    all_path = os.path.join(out_dir, 'pairs_ALL.csv')

    if not os.path.exists(sum_path) or not os.path.exists(all_path):
        print(f"{RED}❌ 找不到 {tf} 周期数据。请检查路径: {out_dir}{RESET}")
        return

    summary = pd.read_csv(sum_path)
    all_pairs = pd.read_csv(all_path)

    # 2. 定位宏观汇总数据
    sum_mask = (summary['entry_factor'] == target_entry) & \
               (summary['exit_factor'] == target_exit) & \
               (summary['filter_mode'] == target_filter)

    sum_row = summary[sum_mask]

    if sum_row.empty:
        print(f"{RED}🚫 在 {tf} 周期下，未找到该组合的数据。可能原因：{RESET}")
        print("   1. 拼写错误。")
        print("   2. 该组合在挖掘阶段一笔交易都没触发，被底层引擎直接抛弃。")
        return

    row = sum_row.iloc[0]

    # 3. 定位微观币种数据
    all_mask = (all_pairs['entry_factor'] == target_entry) & \
               (all_pairs['exit_factor'] == target_exit) & \
               (all_pairs['filter_mode'] == target_filter)

    combo_details = all_pairs[all_mask].copy()

    # ================= 核心计算区 =================
    oos_ret = row['oos_sum_all']
    is_ret = row['sum_ret_all'] - oos_ret
    curr_trades = row['total_trades']

    is_trades = max(curr_trades * 0.7, 1)
    oos_trades = max(curr_trades * 0.3, 1)

    is_pt_ret = is_ret / is_trades
    oos_pt_ret = oos_ret / oos_trades
    decay_rate = (oos_pt_ret / is_pt_ret - 1) * 100 if is_pt_ret > 0 else -999.0

    # 盈亏持仓时间计算
    if not combo_details.empty:
        win_t = combo_details['trades'] * (combo_details['win_rate'] / 100.0)
        loss_t = combo_details['trades'] - win_t
        win_hold_sum = (combo_details['win_hold_bars'].fillna(0) * win_t).sum()
        loss_hold_sum = (combo_details['loss_hold_bars'].fillna(0) * loss_t).sum()
        mean_win_hold = win_hold_sum / win_t.sum() if win_t.sum() > 0 else 0
        mean_loss_hold = loss_hold_sum / loss_t.sum() if loss_t.sum() > 0 else 0

        positive_profits = combo_details[combo_details['sum_ret'] > 0]['sum_ret'].sum()
        max_coin_ret = combo_details['sum_ret'].max()
        top1_coin_pct = (max_coin_ret / positive_profits * 100) if (positive_profits > 0 and max_coin_ret > 0) else 0.0
    else:
        mean_win_hold = mean_loss_hold = top1_coin_pct = 0.0
        positive_profits = 0.0

    # ================= 打印汇总报告 =================
    print(f"{CYAN}【基础体检报告】{RESET}")
    print(f" - 参战币种: {row['n_coins']} 个 (正期望币种比例: {row.get('coin_positive_rate', 0) * 100:.1f}%)")
    print(f" - 交易总数: {curr_trades} 笔 (平均单笔收益: {row.get('mean_avg_ret', 0) * 100:.3f}%)")
    print(
        f" - 胜率情况: 整体胜率 {row.get('mean_win_rate', 0):.1f}% | 逆风胜率(大盘跌) {row.get('mean_down_market_win_rate', 0):.1f}%")
    print(f" - 盈亏持仓: 盈利单均扛 {mean_win_hold:.1f} 根 K线 | 亏损单均扛 {mean_loss_hold:.1f} 根 K线")

    if is_pt_ret > 0:
        print(f" - 样本衰减: {decay_rate:.1f}% (IS 收益 {is_ret:.1f}% -> OOS 收益 {oos_ret:.1f}%)")
    else:
        print(f" - 样本衰减: {RED}IS收益为负，无基准{RESET} (IS 收益 {is_ret:.1f}% -> OOS 收益 {oos_ret:.1f}%)")

    # 季度分布
    q_rets = [row['sum_ret_q1'], row['sum_ret_q2'], row['sum_ret_q3'], row['sum_ret_q4']]
    q_trades = [row['sum_trades_q1'], row['sum_trades_q2'], row['sum_trades_q3'], row['sum_trades_q4']]
    max_q = max([q for q in q_rets if q > 0] + [0.01])

    print(f"\n{CYAN}【季度平稳性分布】{RESET}")
    for i in range(4):
        print(f"  Q{i + 1}: {make_bar(q_rets[i], max_q)} ({q_trades[i]:<4}笔) | 收益: {q_rets[i]:>6.1f}%")

    # ================= 打印底层币种明细 =================
    print(f"\n{CYAN}【底层币种收益排行榜 (Top 5 盈利 & 垫底 3 亏损)】{RESET}")
    if not combo_details.empty:
        # 获取币种列名 (兼容 coin 或 symbol)
        coin_col = 'coin' if 'coin' in combo_details.columns else 'symbol'

        # 按收益降序排序
        combo_details = combo_details.sort_values(by='sum_ret', ascending=False)

        print(
            f"{BOLD}{'排名':<4} | {'标的':<10} | {'收益率':<8} | {'交易笔数':<8} | {'胜率':<6} | {'盈利持仓':<8} | {'亏损持仓'}{RESET}")
        print("-" * 75)

        # 提取前 5 名赚钱的
        top_coins = combo_details.head(5)
        for i, (_, c_row) in enumerate(top_coins.iterrows(), 1):
            color = GREEN if c_row['sum_ret'] > 0 else RED
            print(
                f" {i:<3} | {c_row[coin_col]:<10} | {color}{c_row['sum_ret']:>7.1f}%{RESET} | {c_row['trades']:>6} 笔 | {c_row['win_rate']:>5.1f}% | {c_row.get('win_hold_bars', 0):>6.1f} | {c_row.get('loss_hold_bars', 0):>6.1f}")

        print(" ... (中间省略) ...")

        # 提取后 3 名亏钱的 (前提是总数大于8个)
        if len(combo_details) > 5:
            bottom_coins = combo_details.tail(min(3, len(combo_details) - 5))
            for i, (_, c_row) in enumerate(bottom_coins.iterrows(), len(combo_details) - len(bottom_coins) + 1):
                color = RED if c_row['sum_ret'] < 0 else GREEN
                print(
                    f" {i:<3} | {c_row[coin_col]:<10} | {color}{c_row['sum_ret']:>7.1f}%{RESET} | {c_row['trades']:>6} 笔 | {c_row['win_rate']:>5.1f}% | {c_row.get('win_hold_bars', 0):>6.1f} | {c_row.get('loss_hold_bars', 0):>6.1f}")

        print("-" * 75)
        if positive_profits > 0 and len(combo_details) > 0:
            best_coin = combo_details.iloc[0][coin_col]
            print(f"📌 {YELLOW}利润极权度警告: 赚得最多的【{best_coin}】独占了总盈利的 {top1_coin_pct:.1f}%{RESET}")
    else:
        print(f"{RED}暂无底层币种交易明细。{RESET}")

    print(f"\n{BOLD}{'=' * 90}{RESET}\n")


if __name__ == '__main__':
    # ================= 配置你需要查询的参数 =================
    TIMEFRAME = '60m'
    TARGET_ENTRY = 'EXIT_UPPER_WICK_REJECTION'
    TARGET_EXIT = 'ENTRY_INSIDE_BREAK_VOLUME'
    TARGET_FILTER = 'top_20'
    # ========================================================

    view_specific_combo(
        tf=TIMEFRAME,
        target_entry=TARGET_ENTRY,
        target_exit=TARGET_EXIT,
        target_filter=TARGET_FILTER
    )