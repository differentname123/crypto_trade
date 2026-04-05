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

import pandas as pd

from app.fetch_k_line import gen_csv_file


def backtest_grid(df, grid_ratio, leverage=100, fee_rate=0.0005, lot_size=1.0, direction="short"):
    """
    简易网格交易回测函数
    参数:
      df: 包含 'open', 'high', 'low', 'close' 的 DataFrame
      grid_ratio: 网格间距比例 (例如 0.001 代表 0.1%)
      leverage: 杠杆倍数 (默认 100)
      fee_rate: 单边手续费率 (默认 0.05% = 0.0005)
      lot_size: 每格交易的数量 (默认 1 个单位)
      direction: 网格策略整体方向，可选 "long" (做多) 或 "short" (做空)，默认为 "short"
    返回:
      包含各项回测指标的字典
    """
    if df.empty:
        return None

    # 基准价格：以第一根K线的开盘价作为网格的零点
    p0 = df['open'].iloc[0]
    last_close = df['close'].iloc[-1]  # 记录最终收盘价

    realized_pnl = 0.0  # 已实现盈亏
    positions = {}  # 当前持仓记录字典: { 网格层级 k : 开仓价格 }
    max_capital_needed = 0.0  # 历史记录中【所需的最大初始资金】(即最小不爆仓保证金)
    raw_equity_curve = []  # 记录纯净的资金变化曲线(不含初始资金)
    total_trades = 0  # 新增：记录总的开平仓总次数

    # 新增：记录发生 max_capital_needed 时的“案发现场”信息
    worst_case_info = {
        "time": None,  # 修改：名称改为 time
        "worst_price": p0,
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

    p_prev = p0
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
                        if k not in positions:  # 价格下跌，触发买入开多
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


def batch_backtest_grid_ratios(df, output_csv, leverage=100, fee_rate=0.0005, lot_size=1.0, direction="short"):
    """
    批量回测不同网格间距(0.001 到 0.1，步长 0.001)并保存为 CSV
    """
    if df.empty:
        print("数据为空，无法进行批量回测。")
        return

    results = []
    print(f"开始批量回测 (方向: {direction}) ...")

    # 从 1 遍历到 100，对应 0.001 到 0.100 (避免直接浮点数相加产生的精度丢失)
    for i in range(1, 2):
        grid_ratio = round(i * 0.001, 3)
        res = backtest_grid(df, grid_ratio=grid_ratio, leverage=leverage, fee_rate=fee_rate, lot_size=lot_size,
                            direction=direction)
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


if __name__ == "__main__":
    csv_file_path = gen_csv_file()
    data_dir = r"W:\project\python_project\crypto_trade\data"

    output_csv_path = f'{data_dir}/grid_backtest_results.csv'
    df = pd.read_csv(output_csv_path)
    df['score'] = df['profit_to_margin_ratio'] * df['total_trades']  # 简单的综合评分指标，越高越好
    print()
    # data_df = pd.read_csv(csv_file_path)  # 先尝试读取，确保文件存在且格式正确
    # print()
    try:

        df = pd.read_csv(csv_file_path)  # 先尝试读取，确保文件存在且格式正确

        # 如果您的数据中包含时间列，例如 'timestamp'，建议在传入前将其设为索引，这样 worst_time 就能记录下真实时间：
        # if 'timestamp' in df.columns:
        #     df.set_index('timestamp', inplace=True)

        # 1. 单次测试示例
        # print("单次测试结果：")
        # single_result = backtest_grid(df, grid_ratio=0.001, leverage=100, fee_rate=0.0005, lot_size=1.0, direction="short")
        # print(single_result)

        # 2. 批量跑网格参数并导出 CSV (以 0.001 步长一直算到 0.1)
        print("\n启动批量参数回测...")
        batch_backtest_grid_ratios(df, output_csv=output_csv_path, leverage=100, fee_rate=0.0001, lot_size=0.02,
                                   direction="long")

    except FileNotFoundError:
        traceback.print_exc()
        print(f"找不到文件: {csv_file_path}，请检查路径。")