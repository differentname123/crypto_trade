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
import traceback
from pathlib import Path

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
        if not positions:
            return

        # 浮动盈亏 = Σ (盈亏价差) * 数量 （依据多空方向区分）
        if direction == "long":
            floating_pnl = sum((current_price - p) * lot_size for p in positions.values())
        else:
            floating_pnl = sum((p - current_price) * lot_size for p in positions.values())

        # 当前所需保证金 = Σ (开仓价 * 数量 / 杠杆)
        required_margin = sum(p * lot_size / leverage for p in positions.values())

        # 资金缺口 = 所需保证金 - 已实现盈亏 - 浮动盈亏
        capital_needed = required_margin - realized_pnl - floating_pnl

        if capital_needed > max_capital_needed:
            max_capital_needed = capital_needed
            # 记录刷新极值时的现场数据
            worst_case_info["time"] = current_time  # 修改：记录真实 time
            worst_case_info["worst_price"] = current_price
            worst_case_info["worst_position_count"] = len(positions)
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
                            fee = pk * lot_size * fee_rate
                            realized_pnl -= fee
                            total_trades += 1
                            update_margin(pk, current_time)
                    else:  # direction == "short"
                        if (k + 1) in positions:  # 价格下跌，触发买入平空(检查之前是否在上层开空过)
                            entry_p = positions.pop(k + 1)
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
        if direction == "long":
            minute_floating_pnl = sum((close_p - p) * lot_size for p in positions.values())
        else:
            minute_floating_pnl = sum((p - close_p) * lot_size for p in positions.values())

        raw_equity_curve.append(realized_pnl + minute_floating_pnl)

    # =============== 统计结果 ===============

    # 如果极值小于0说明光靠利润就够扛了，但理论上首次开仓必定需要保证金，给个基础兜底
    min_margin = max_capital_needed if max_capital_needed > 0 else (p0 * lot_size / leverage)

    # 生成真实的资产曲线用于计算回撤 (初始资金 + 过程盈亏)
    equity_curve = [min_margin + eq for eq in raw_equity_curve]

    # 计算最大回撤率 (Max Drawdown)
    max_dd = 0.0
    if equity_curve:
        peak = equity_curve[0]
        for eq in equity_curve:
            if eq > peak:
                peak = eq
            dd = (peak - eq) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd

    # ================== 修改部分开始 ==================
    # 最终收益指标：直观地取最后一刻的总净盈亏，不再使用减去 min_margin 的绕弯子逻辑
    total_profit = raw_equity_curve[-1] if raw_equity_curve else 0.0
    profit_rate = total_profit / min_margin if min_margin > 0 else 0.0

    # 新增：计算最后一刻的持仓数量和持仓浮动盈亏
    final_position_count = len(positions)
    if direction == "long":
        final_floating_pnl = sum((last_close - p) * lot_size for p in positions.values())
    else:
        final_floating_pnl = sum((p - last_close) * lot_size for p in positions.values())
    # ================== 修改部分结束 ==================

    # 新增：计算每小时平均交易次数 (基于每行数据代表 1 分钟的前提)
    total_minutes = len(df)
    total_hours = total_minutes / 60.0 if total_minutes > 0 else 1.0
    trades_per_hour = total_trades / total_hours if total_hours > 0 else 0.0

    # 新增：开始价格到最终价格的涨跌幅
    price_change_rate = (last_close - p0) / p0 if p0 > 0 else 0.0

    return {
        "direction": direction,
        "grid_ratio": grid_ratio,
        "price_change_rate": price_change_rate,  # 标的物期间涨跌幅
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


def batch_backtest_grid_ratios(df, output_csv, leverage=100, fee_rate=0.0005, lot_size=1.0, direction="short", initial_price=2500):
    """
    批量回测不同网格间距(0.001 到 0.1，步长 0.001)并保存为 CSV
    """
    if df.empty:
        print("数据为空，无法进行批量回测。")
        return
    print(f"数据加载成功，包含 {len(df)} 行记录。开始批量回测网格参数...")

    results = []
    print(f"开始批量回测 (方向: {direction}) ...")

    # 从 1 遍历到 100，对应 0.001 到 0.100 (避免直接浮点数相加产生的精度丢失)
    for i in range(1, 100):
        grid_ratio = round(i * 0.001, 5)
        res = backtest_grid(df, grid_ratio=grid_ratio, leverage=leverage, fee_rate=fee_rate, lot_size=lot_size,
                            direction=direction, initial_price=initial_price)
        if res:
            results.append(res)
            # 简单打印进度
            if i % 10 == 0:
                print(f"已完成网格步长: {grid_ratio}")

    if results:
        res_df = pd.DataFrame(results)
        res_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
        print(f"批量回测全部完成！共 {len(results)} 条结果，已保存至: {output_csv}")

    return results


import pandas as pd
from pathlib import Path

if __name__ == "__main__":
    # 1. 定义文件夹路径和匹配模式
    folder_path = Path(r"W:\project\python_project\oke_auto_trade\kline_data")
    file_pattern = "*DOGEUSDT_1m_2025-01-01_merged_grid_backtest_results_*.csv"

    # 获取所有匹配的文件列表
    matched_files = list(folder_path.glob(file_pattern))

    if not matched_files:
        print("未找到任何匹配的文件，请检查路径和文件名规则。")
    else:
        df_list = []

        # 2. 遍历读取每个文件并计算 score1
        for file in matched_files:
            try:
                temp_df = pd.read_csv(file)
                temp_df['file_name'] = file.name.split('grid_backtest_results_')[1].split('.csv')[0]  # 提取参数信息作为新列

                # 检查必要的列是否存在，防止报错中断 (新增了 total_trades)
                required_cols = ['paired_profit', 'min_margin_needed', 'grid_ratio', 'total_trades']
                if all(col in temp_df.columns for col in required_cols):
                    # 计算 score1
                    temp_df['score1'] = 100 * temp_df['paired_profit'] / temp_df['min_margin_needed']

                    # 保留所有的列
                    df_list.append(temp_df)
                else:
                    print(f"跳过 {file.name}：缺少必要的字段 (需包含 {required_cols})")

            except pd.errors.EmptyDataError:
                print(f"跳过 {file.name}：文件为空")
            except Exception as e:
                print(f"读取 {file.name} 时出错: {e}")

        # 3. 将所有有效数据合并并进行聚合
        if df_list:
            # 合并所有提取出来的数据
            all_data = pd.concat(df_list, ignore_index=True)

            # 以 grid_ratio 进行分组，采用多列命名聚合语法，同时计算 score1 和 total_trades 的统计信息
            final_df = all_data.groupby('grid_ratio').agg(
                score1_mean=('score1', 'mean'),
                score1_std=('score1', 'std'),
                score1_max=('score1', 'max'),
                score1_min=('score1', 'min'),
                sample_count=('score1', 'count'),  # 统计一下每个 grid_ratio 下有多少条数据
                total_trades_mean=('total_trades', 'mean'), # 新增：平均交易次数
                total_trades_max=('total_trades', 'max'),   # 新增：最大交易次数
                total_trades_min=('total_trades', 'min')    # 新增：最小交易次数
            ).reset_index()

            # --- 修改点：新增统计相邻 score1_mean 平均值的字段 ---
            # 此时 final_df 已经是按 grid_ratio 从小到大排序的，可以直接计算相邻均值
            # 使用 window=3, center=True 表示取它本身和前后各一个（共3个）进行平均计算，min_periods=1 保证首尾也能算出均值
            final_df['score1_mean_adj_avg'] = final_df['score1_mean'].rolling(window=3, center=True, min_periods=1).mean()
            # ---------------------------------------------------

            # 按照平均分降序排列，方便直接看到表现最好的参数
            final_df = final_df.sort_values(by='score1_mean', ascending=False)
            final_df['score'] = final_df['score1_mean']*final_df['score1_min']  # 计算最终的 score（均值除以标准差）
            final_df['score2'] = final_df['score1_mean']*final_df['score1_min']/(final_df['grid_ratio'] + 0.1)  # 计算最终的 score（均值除以标准差）

            print("聚合计算完成！结果如下：")
            print(final_df)

            # 如果需要保存结果，可以取消下面这行的注释
            # final_df.to_csv(folder_path / "aggregated_grid_scores.csv", index=False)
        else:
            print("没有提取到任何有效数据。")