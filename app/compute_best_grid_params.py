# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/4/5 20:32
:last_date:
    2026/4/5 20:32
:description:

"""
import math
import os
import traceback
from multiprocessing import Pool

import numpy as np
import pandas as pd


def backtest_grid(df, grid_ratio, leverage=100, fee_rate=0.0005, lot_size=1.0, direction="short", initial_price=2500):
    """
    简易网格交易回测函数
    参数:
      df: 包含 'open', 'high', 'low', 'close' 的 DataFrame
      grid_ratio: 网格间距比例 (例如 0.001 代表 0.1%)
      leverage: 杠杆倍数 (默认 100)
      fee_rate: 单边手续费率 (默认 0.05% = 0.0005)
      lot_size: 每格交易的数量 (默认 1 个单位)
      direction: 网格策略整体方向，可选 "long" (做多) 或 "short" (做空)，默认为 "short"
      initial_price: 网格初始价格 (作为做多的最高价限制)
    返回:
      包含各项回测指标的字典
    """
    if df.empty:
        return None

    # 基准价格：修改为传入的初始价格作为网格的零点
    p0 = initial_price
    first_open = df['open'].iloc[0]  # 获取实际第一根K线开盘价用于初始化价格轨迹
    last_close = df['close'].iloc[-1]  # 记录最终收盘价

    realized_pnl = 0.0  # 已实现盈亏
    paired_profit = 0.0  # 新增：已配对的利润（仅记录完成一开一平的闭环利润）
    positions = {}  # 当前持仓记录字典: { 网格层级 k : 开仓价格 }

    # ================== 🚀 核心优化一：引入全局状态追踪变量 ==================
    current_pos_count = 0  # 当前持仓总单数，替代 len(positions)
    sum_entry_prices = 0.0  # 当前持仓的开仓价总和，替代 sum(positions.values())
    # ======================================================================

    max_capital_needed = 0.0  # 历史记录中【所需的最大初始资金】(即最小不爆仓保证金)
    raw_equity_curve = []  # 记录纯净的资金变化曲线(不含初始资金)
    total_trades = 0  # 新增：记录总的开平仓总次数

    # 新增：记录发生 max_capital_needed 时的“案发现场”信息
    worst_case_info = {
        "time": None,  # 修改：名称改为 time
        "worst_price": first_open,
        "worst_position_count": 0,
        "worst_price_change_rate": 0.0,
        "worst_realized_pnl": 0.0,  # 新增：当时的已实现利润
        "worst_floating_pnl": 0.0,  # 新增：当时的持仓总浮亏(或浮盈)
        "worst_total_trades": 0,  # 新增：当时的已成交单数
        "worst_required_margin": 0.0  # 新增：当时的仓位所需保证金 (用于完美核对公式)
    }

    # 内部函数：每次价格变动或成交后，校验并更新最高所需保证金
    def update_margin(current_price, current_time):
        nonlocal max_capital_needed, worst_case_info
        # 优化：通过数值判断代替遍历字典判空，速度更快
        if current_pos_count == 0:
            return

        # ================== 🚀 核心优化二：O(1) 代数降维计算 ==================
        # 浮动盈亏计算：提取公因式，彻底消除原先的 sum(...) O(N) 循环
        if direction == "long":
            floating_pnl = (current_price * current_pos_count - sum_entry_prices) * lot_size
        else:
            floating_pnl = (sum_entry_prices - current_price * current_pos_count) * lot_size

        # 当前所需保证金计算：消除 sum(...) 循环
        required_margin = (sum_entry_prices * lot_size) / leverage
        # ======================================================================

        # 资金缺口 = 所需保证金 - 已实现盈亏 - 浮动盈亏
        capital_needed = required_margin - realized_pnl - floating_pnl

        if capital_needed > max_capital_needed:
            max_capital_needed = capital_needed
            # 记录刷新极值时的现场数据
            worst_case_info["time"] = current_time  # 修改：记录真实 time
            worst_case_info["worst_price"] = current_price
            worst_case_info["worst_position_count"] = current_pos_count  # 优化：直接读取状态变量
            worst_case_info["worst_price_change_rate"] = (current_price - p0) / p0 if p0 > 0 else 0.0
            worst_case_info["worst_realized_pnl"] = realized_pnl  # 记录当时已实现利润
            worst_case_info["worst_floating_pnl"] = floating_pnl  # 记录当时浮动盈亏
            worst_case_info["worst_total_trades"] = total_trades  # 记录当时成交单数
            worst_case_info["worst_required_margin"] = required_margin  # 记录当时所需保证金

    p_prev = first_open
    k_prev = math.floor((p_prev - p0) / (p0 * grid_ratio))

    # 修改：获取实际的时间序列 (优先取 'time' 或 'timestamp' 列，否则用 index)
    if 'time' in df.columns:
        time_seq = df['time']
    elif 'timestamp' in df.columns:
        time_seq = df['timestamp']
    else:
        time_seq = df.index

    # 遍历每一分钟 (优化点: 放弃 iterrows，使用 zip 原生迭代加速几十倍，并引入真实时间标识)
    for current_time, open_p, high_p, low_p, close_p in zip(time_seq, df['open'], df['high'], df['low'], df['close']):

        # 根据K线阴阳，模拟分钟内部的价格轨迹，使回测更加贴近真实
        if close_p > open_p:
            points = [open_p, low_p, high_p, close_p]
        else:
            points = [open_p, high_p, low_p, close_p]

        # 沿着价格轨迹模拟穿越网格线
        for p in points:
            # 当前价格所处的网格层级
            k_curr = math.floor((p - p0) / (p0 * grid_ratio))

            if k_curr > k_prev:
                # 价格上涨
                for k in range(k_prev + 1, k_curr + 1):
                    pk = p0 * (1 + k * grid_ratio)
                    update_margin(pk, current_time)  # 碰线前结算一次极值

                    if direction == "long":
                        if (k - 1) in positions:  # 如果持有多单，则平仓
                            entry_p = positions.pop(k - 1)
                            # === 同步维护 O(1) 状态 ===
                            current_pos_count -= 1
                            sum_entry_prices -= entry_p
                            if current_pos_count == 0:
                                sum_entry_prices = 0.0
                            # ==========================

                            gross_profit = (pk - entry_p) * lot_size
                            fee = pk * lot_size * fee_rate
                            realized_pnl += (gross_profit - fee)
                            total_trades += 1

                            # 新增：计算配对利润（扣除开平双边手续费）
                            open_fee = entry_p * lot_size * fee_rate
                            paired_profit += (gross_profit - fee - open_fee)

                            update_margin(pk, current_time)
                    else:  # direction == "short"
                        if k not in positions:  # 价格上涨，触发卖出开空
                            positions[k] = pk
                            # === 同步维护 O(1) 状态 ===
                            current_pos_count += 1
                            sum_entry_prices += pk
                            # ==========================

                            fee = pk * lot_size * fee_rate
                            realized_pnl -= fee
                            total_trades += 1
                            update_margin(pk, current_time)

            elif k_curr < k_prev:
                # 价格下跌
                for k in range(k_prev, k_curr, -1):
                    pk = p0 * (1 + k * grid_ratio)
                    update_margin(pk, current_time)  # 碰线前结算一次极值

                    if direction == "long":
                        # 修改：使用 k <= 0 确保新开仓的价格严格小于等于 p0(最高价限制)
                        if k not in positions and k <= 0:
                            positions[k] = pk
                            # === 同步维护 O(1) 状态 ===
                            current_pos_count += 1
                            sum_entry_prices += pk
                            # ==========================

                            fee = pk * lot_size * fee_rate
                            realized_pnl -= fee
                            total_trades += 1
                            update_margin(pk, current_time)
                    else:  # direction == "short"
                        if (k + 1) in positions:  # 价格下跌，触发买入平空(检查之前是否在上层开空过)
                            entry_p = positions.pop(k + 1)
                            # === 同步维护 O(1) 状态 ===
                            current_pos_count -= 1
                            sum_entry_prices -= entry_p
                            if current_pos_count == 0:
                                sum_entry_prices = 0.0
                            # ==========================

                            gross_profit = (entry_p - pk) * lot_size
                            fee = pk * lot_size * fee_rate
                            realized_pnl += (gross_profit - fee)
                            total_trades += 1

                            # 新增：计算配对利润（扣除开平双边手续费）
                            open_fee = entry_p * lot_size * fee_rate
                            paired_profit += (gross_profit - fee - open_fee)

                            update_margin(pk, current_time)

            p_prev = p
            k_prev = k_curr
            update_margin(p, current_time)  # 轨迹点结算

        # 分钟结束，记录当期总权益 (用于后续画图或算回撤)
        # ================== 🚀 核心优化三：K线末尾 O(1) 计算 ==================
        if direction == "long":
            minute_floating_pnl = (close_p * current_pos_count - sum_entry_prices) * lot_size
        else:
            minute_floating_pnl = (sum_entry_prices - close_p * current_pos_count) * lot_size
        # ======================================================================

        raw_equity_curve.append(realized_pnl + minute_floating_pnl)

    # =============== 统计结果 ===============

    # 如果极值小于0说明光靠利润就够扛了，但理论上首次开仓必定需要保证金，给个基础兜底
    min_margin = max_capital_needed if max_capital_needed > 0 else (p0 * lot_size / leverage)

    # ================== 🚀 核心优化四：NumPy 向量化计算最大回撤 ==================
    # 彻底消除原生 for 循环计算回撤的缓慢过程
    if raw_equity_curve:
        # 生成真实的资产曲线 Numpy 数组
        equity_array = min_margin + np.array(raw_equity_curve)
        # 计算历史峰值数组
        peak_array = np.maximum.accumulate(equity_array)
        # 使用 np.where 完美避开除以 0 的情况，计算所有时点的回撤
        with np.errstate(divide='ignore', invalid='ignore'):
            drawdowns = np.where(peak_array > 0, (peak_array - equity_array) / peak_array, 0.0)
        max_dd = float(np.max(drawdowns))
    else:
        max_dd = 0.0
    # ==========================================================================

    # ================== 修改部分开始 ==================
    # 最终收益指标：直观地取最后一刻的总净盈亏，不再使用减去 min_margin 的绕弯子逻辑
    total_profit = raw_equity_curve[-1] if raw_equity_curve else 0.0
    profit_rate = total_profit / min_margin if min_margin > 0 else 0.0

    # 新增：计算最后一刻的持仓数量和持仓浮动盈亏
    final_position_count = current_pos_count  # 优化：直接读取状态变量
    if direction == "long":
        final_floating_pnl = (last_close * current_pos_count - sum_entry_prices) * lot_size
    else:
        final_floating_pnl = (sum_entry_prices - last_close * current_pos_count) * lot_size
    # ================== 修改部分结束 ==================

    # 新增：计算每小时平均交易次数 (基于每行数据代表 1 分钟的前提)
    total_minutes = len(df)
    total_hours = total_minutes / 60.0 if total_minutes > 0 else 1.0
    trades_per_hour = total_trades / total_hours if total_hours > 0 else 0.0

    # 新增：开始价格到最终价格的涨跌幅
    price_change_rate = (last_close - p0) / p0 if p0 > 0 else 0.0

    # 新增：统计有效 bar 数量（针对 long 方向）
    valid_bar_count = 0
    if direction == "long":
        valid_bar_count = int((df['close'] < initial_price).sum())

    return {
        "direction": direction,
        "grid_ratio": grid_ratio,
        "price_change_rate": price_change_rate,  # 标的物期间涨跌幅
        "valid_bar_count": valid_bar_count,  # 新增：多头有效运作区间的K线数量
        "total_profit": total_profit,  # 纯收益 (已扣除手续费)
        "paired_profit": paired_profit,  # 新增：已配对的闭环净利润
        "min_margin_needed": min_margin,  # 最小不爆仓所需初始保证金
        "profit_to_margin_ratio": profit_rate,  # 收益 / 保证金 (核心参考价值)
        "max_drawdown": max_dd,  # 最大回撤率
        "total_trades": total_trades,  # 交易总次数(开平仓均计算在内)
        "trades_per_hour": trades_per_hour,  # 每小时平均交易次数
        "final_position_count": final_position_count,  # 新增：最后一刻的持仓单数
        "final_floating_pnl": final_floating_pnl,  # 新增：最后一刻的持仓浮动盈亏
        "time": worst_case_info["time"],  # 修改：极值发生时间(获取自真实的 time/timestamp 列或行号)
        "worst_price": worst_case_info["worst_price"],  # 极值发生时的价格
        "worst_position_count": worst_case_info["worst_position_count"],  # 极值发生时的持仓单数
        "worst_price_change_rate": worst_case_info["worst_price_change_rate"],  # 极值发生时相对于初始价格的涨跌幅
        "worst_realized_pnl": worst_case_info["worst_realized_pnl"],  # 极值发生时的已实现利润
        "worst_floating_pnl": worst_case_info["worst_floating_pnl"],  # 极值发生时的持仓浮动盈亏
        "worst_total_trades": worst_case_info["worst_total_trades"],  # 极值发生时的已成交单数
        "worst_required_margin": worst_case_info["worst_required_margin"]  # 极值发生时的持仓所需本金
    }


# ================== 新增的包裹函数 ==================
# 用于多进程参数解包（Python 多进程要求传入的函数在顶层作用域）
def _run_single_backtest(kwargs):
    return backtest_grid(**kwargs)
# ====================================================


def batch_backtest_grid_ratios(df, output_csv, leverage=100, fee_rate=0.0005, lot_size=1.0, direction="short", initial_price=2500):
    """
    批量回测不同网格间距(0.001 到 0.1，步长 0.001)并保存为 CSV
    修改为 20 并发的多进程运行。
    """
    if df.empty:
        print("数据为空，无法进行批量回测。")
        return
    print(f"数据加载成功，包含 {len(df)} 行记录。开始批量回测网格参数...")

    results = []
    print(f"开始批量并行回测 (方向: {direction}) ...")

    # --- 构造多进程任务参数池 ---
    tasks = []
    # 从 10 遍历到 200，对应 0.001 到 0.0199...
    for i in range(10, 200):
        grid_ratio = round(i * 0.0001, 5)
        tasks.append({
            'df': df,
            'grid_ratio': grid_ratio,
            'leverage': leverage,
            'fee_rate': fee_rate,
            'lot_size': lot_size,
            'direction': direction,
            'initial_price': initial_price
        })

    # --- 启动 20 进程的进程池 ---
    with Pool(processes=30) as pool:
        # imap_unordered 可以最高效地调度任务，并且能够在进行中立刻获得返回以更新进度
        for i, res in enumerate(pool.imap_unordered(_run_single_backtest, tasks)):
            if res:
                results.append(res)
            # 简单打印进度
            if (i + 1) % 10 == 0:
                print(f"已完成并行任务进度: {i + 1} / {len(tasks)}")

    if results:
        # 由于 imap_unordered 会打乱返回顺序，我们在保存前按照 grid_ratio 重新排个序
        results = sorted(results, key=lambda x: x['grid_ratio'])
        res_df = pd.DataFrame(results)
        res_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"批量回测全部完成！共 {len(results)} 条结果，已保存至: {output_csv}")

    return results


if __name__ == "__main__":
    param_list = [
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\ETHUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\SOLUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\BNBUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\DOGEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\LINKUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\TRXUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\AAVEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\TONUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\SKYUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\UNIUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\STXUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RENDERUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RUNEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\PENDLEUSDT_1m_2025-01-01_merged.csv"
        },
        {
            "csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\KASUSDT_1m_2025-01-01_merged.csv"
        }
    ]

    for param in param_list:
        csv_file_path = param["csv_file_path"]

        # ---------------- 新增：根据CSV自动计算价格参数开始 ----------------
        try:
            # 预先读取文件一次用于计算 base_initial_price 和 min_price
            temp_df = pd.read_csv(csv_file_path)

            # 时间列转换
            if 'open_time' in temp_df.columns:
                temp_df['time'] = pd.to_datetime(temp_df['open_time'], unit='ms')
            elif 'time' in temp_df.columns:
                temp_df['time'] = pd.to_datetime(temp_df['time'])

            # 筛选2026年后(含2026-01-01)的数据
            df_2026 = temp_df[temp_df['time'] >= pd.to_datetime('2026-01-01')]
            if df_2026.empty:
                df_2026 = temp_df  # 如果没有2026年后的数据，默认使用全部数据作为保底

            # 获取最高价作为最大值，最低价求50%作为最小值 (优先判断k线标准列high/low，无则用close)
            if 'high' in df_2026.columns and 'low' in df_2026.columns:
                base_initial_price = float(df_2026['high'].max())
                min_price = float(df_2026['low'].min())
            elif 'close' in df_2026.columns:
                base_initial_price = float(df_2026['close'].max())
                min_price = float(df_2026['close'].min())
            else:
                print(f"跳过 {csv_file_path}：数据列中未找到 high/low 或 close")
                continue

        except Exception as e:
            print(f"读取文件以计算自动价格失败: {csv_file_path}, 错误: {e}")
            continue
        # ---------------- 新增：根据CSV自动计算价格参数结束 ----------------
        print(f"根据CSV数据计算得到的初始价格: {base_initial_price}, 最小价格: {min_price} {csv_file_path}")
        initial_price = base_initial_price

        # 只要当前价格大于等于下限价格，就继续循环
        while initial_price >= min_price:
            print(f"\n=== 回测初始价格: {initial_price} ===")

            output_csv_path = csv_file_path.replace(".csv", f"_grid_backtest_results_{initial_price}.csv")
            if os.path.exists(output_csv_path):
                print(f"结果文件已存在，跳过回测: {output_csv_path}")
                initial_price = initial_price * 0.99
                continue
            try:

                df = pd.read_csv(csv_file_path)  # 先尝试读取，确保文件存在且格式正确

                if 'open_time' in df.columns:
                    df['time'] = pd.to_datetime(df['open_time'], unit='ms')

                # 2. 批量跑网格参数并导出 CSV (以 0.001 步长一直算到 0.1)
                print("\n启动批量参数回测...")
                batch_backtest_grid_ratios(df, output_csv=output_csv_path, leverage=100, fee_rate=0.0000, lot_size=0.1,
                                           direction="long", initial_price=initial_price)

            except FileNotFoundError:
                traceback.print_exc()
                print(f"找不到文件: {csv_file_path}，请检查路径。")

            # 每次回测后，价格降低当前值的 1%
            initial_price = initial_price * 0.99