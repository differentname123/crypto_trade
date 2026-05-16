# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/5/16
:description:
    ETH 网格与期权动态对冲寻优引擎 (生产版)
"""
import os
import requests
import pandas as pd
from datetime import datetime, timezone


# ==========================================
# 1. 数据获取模块
# ==========================================
def get_binance_eth_options():
    """从币安拉取最新的 ETH 期权行情数据"""
    url = "https://eapi.binance.com/eapi/v1/ticker"
    print(f"[INFO] 正在请求币安期权接口: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        eth_options = [item for item in data if item['symbol'].startswith('ETH-')]
        if not eth_options:
            print("[WARNING] 接口请求成功，但未找到 ETH 相关期权数据。")
            return None

        df = pd.DataFrame(eth_options)
        columns_to_show = ['symbol', 'lastPrice', 'bidPrice', 'askPrice', 'volume']
        df = df[[col for col in columns_to_show if col in df.columns]]

        for col in ['lastPrice', 'bidPrice', 'askPrice', 'volume']:
            df[col] = df[col].astype(float)

        print(f"[INFO] 成功拉取并清洗 {len(df)} 条 ETH 期权数据。")
        return df
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] 请求币安 API 失败: {e}")
        return None


def get_current_eth_price():
    """获取当前 ETH 现货价格"""
    try:
        resp = requests.get("https://api.binance.com/api/v3/ticker/price?symbol=ETHUSDT", timeout=5)
        resp.raise_for_status()
        return float(resp.json()['price'])
    except Exception as e:
        print(f"[WARNING] 获取 ETH 现货价格失败: {e}。默认返回 3000.0 U")
        return 3000.0


# ==========================================
# 2. 网格保证金计算引擎
# ==========================================
def calculate_multi_group_margin(
        leverage: float,
        target_loss_percent: float,
        max_grids_per_group: int,
        fixed_qty: float = 1.0,
        add_step_percent: float = 0.01,
        initial_price: float = 1.0,
        direction: str = 'long'
) -> dict:
    """计算复杂多组网格的极限资金占用"""
    if leverage <= 0 or target_loss_percent <= 0 or add_step_percent <= 0 or fixed_qty <= 0 or max_grids_per_group <= 0:
        raise ValueError("所有数值参数必须 > 0")
    if direction not in ['long', 'short']:
        raise ValueError("direction 参数必须是 'long' 或 'short'")
    if direction == 'long' and target_loss_percent >= 100.0:
        raise ValueError("做多时，价格最大跌幅不能 >= 100%。")

    r = add_step_percent / 100.0
    sign = 1 if direction == 'long' else -1
    target_price = initial_price * (1.0 - target_loss_percent / 100.0) if direction == 'long' else initial_price * (
                1.0 + target_loss_percent / 100.0)

    current_price = initial_price
    total_margin_all_groups = 0.0
    groups_info = []
    group_id = 1

    def is_active(cp, tp):
        return cp >= tp if direction == 'long' else cp <= tp

    while is_active(current_price, target_price):
        group_start_price = current_price
        group_qty = 0.0
        group_cost = 0.0
        group_max_margin = 0.0
        grids_in_group = 0
        last_executed_price = current_price

        while grids_in_group < max_grids_per_group and is_active(current_price, target_price):
            group_qty += fixed_qty
            group_cost += fixed_qty * current_price
            grids_in_group += 1
            last_executed_price = current_price

            upnl_at_open = sign * (group_qty * current_price - group_cost)
            required_for_margin = (group_cost / leverage) - upnl_at_open
            if required_for_margin > group_max_margin:
                group_max_margin = required_for_margin

            if direction == 'long':
                next_price = current_price * (1 - r)
                check_price = max(next_price, target_price)
            else:
                next_price = current_price * (1 + r)
                check_price = min(next_price, target_price)

            upnl_at_bottom = sign * (group_qty * check_price - group_cost)
            required_for_survival = -upnl_at_bottom
            if required_for_survival > group_max_margin:
                group_max_margin = required_for_survival

            current_price = next_price

        upnl_at_global_target = sign * (group_qty * target_price - group_cost)
        required_at_global_target = (group_cost / leverage) - upnl_at_global_target
        if required_at_global_target > group_max_margin:
            group_max_margin = required_at_global_target

        groups_info.append({
            "group_id": group_id,
            "start_price": round(group_start_price, 6),
            "end_price": round(last_executed_price, 6),
            "grid_count": grids_in_group,
            "group_qty": round(group_qty, 6),
            "required_margin": round(group_max_margin, 6)
        })

        total_margin_all_groups += group_max_margin
        group_id += 1

    return {
        "total_margin": round(total_margin_all_groups, 6),
        "groups_info": groups_info
    }


# ==========================================
# 3. 期权策略寻优引擎
# ==========================================
def find_optimal_options_strategy(df, target_price, target_profit, min_hours=48, max_margin=1000):
    """全局遍历寻找给定目标价与利润要求下的最优期权价差组合"""
    results = []
    now_utc = datetime.now(timezone.utc)

    # 预处理查找表 (加速卖出腿的匹配)
    put_bids = {}
    for _, r in df.iterrows():
        sym = r['symbol']
        if isinstance(sym, str) and sym.startswith('ETH-') and sym.endswith('-P'):
            p = sym.split('-')
            if len(p) == 4:
                bid_p = r.get('bidPrice', 0)
                if not pd.isna(bid_p) and bid_p > 0:
                    put_bids[(p[1], float(p[2]))] = (sym, float(bid_p))

    for index, row in df.iterrows():
        symbol = row['symbol']
        ask_price = row.get('askPrice', 0)

        if pd.isna(ask_price) or ask_price <= 0:
            continue

        parts = symbol.split('-')
        if len(parts) != 4:
            continue

        _, date_str, strike_str, opt_type = parts
        strike = float(strike_str)
        opt_type_name = '看跌(Put)' if opt_type == 'P' else '看涨(Call)'

        year, month, day = int("20" + date_str[0:2]), int(date_str[2:4]), int(date_str[4:6])
        expiry_datetime_utc = datetime(year, month, day, 8, 0, 0, tzinfo=timezone.utc)

        total_seconds_left = (expiry_datetime_utc - now_utc).total_seconds()
        if total_seconds_left <= max(60, min_hours * 3600):
            continue

        days = int(total_seconds_left // 86400)
        hours = int((total_seconds_left % 86400) // 3600)
        minutes = int((total_seconds_left % 3600) // 60)
        time_left_str = f"{days}天{hours}小时{minutes}分" if days > 0 else (
            f"{hours}小时{minutes}分" if hours > 0 else f"{minutes}分钟")

        exact_days_to_expiry = total_seconds_left / 86400.0

        # 原裸买基准数据
        naked_payoff = max(0, strike - target_price) if opt_type == 'P' else max(0, target_price - strike)
        naked_net_profit = naked_payoff - ask_price
        naked_total_cost = (target_profit / naked_net_profit * ask_price) if naked_net_profit > 0 else float('inf')
        naked_daily_cost = naked_total_cost / exact_days_to_expiry if naked_net_profit > 0 else float('inf')

        best_sell_symbol, best_sell_bid, best_net_ask_price = "无", 0.0, 0.0
        best_net_profit_per_contract, best_num_contracts = 0.0, 0.0
        best_total_cost, best_daily_cost = float('inf'), float('inf')
        found_valid = False

        if opt_type == 'P':
            base_payoff = max(0, strike - target_price)
            candidates = [("无", 0.0, 0.0)]

            available_strikes = [k[1] for k in put_bids.keys() if k[0] == date_str and k[1] < strike]
            for sell_strike in available_strikes:
                sym, bid = put_bids[(date_str, sell_strike)]
                sell_liab = max(0, sell_strike - target_price)
                candidates.append((sym, bid, sell_liab))

            for s_sym, s_bid, s_liab in candidates:
                current_payoff = base_payoff - s_liab
                current_net_ask = max(0.0001, ask_price - s_bid)
                current_net_profit = current_payoff - current_net_ask

                if current_net_profit <= 0:
                    continue

                current_num_contracts = target_profit / current_net_profit
                current_total_cost = current_num_contracts * current_net_ask

                if current_total_cost > max_margin:
                    continue

                current_daily_cost = current_total_cost / exact_days_to_expiry
                if current_daily_cost < best_daily_cost:
                    best_daily_cost = current_daily_cost
                    best_total_cost = current_total_cost
                    best_num_contracts = current_num_contracts
                    best_net_profit_per_contract = current_net_profit
                    best_net_ask_price = current_net_ask
                    best_sell_bid = s_bid
                    best_sell_symbol = s_sym
                    found_valid = True

        elif opt_type == 'C':
            current_payoff = max(0, target_price - strike)
            current_net_ask = ask_price
            current_net_profit = current_payoff - current_net_ask
            if current_net_profit > 0:
                current_num_contracts = target_profit / current_net_profit
                current_total_cost = current_num_contracts * current_net_ask
                if current_total_cost <= max_margin:
                    best_daily_cost = current_total_cost / exact_days_to_expiry
                    best_total_cost = current_total_cost
                    best_num_contracts = current_num_contracts
                    best_net_profit_per_contract = current_net_profit
                    best_net_ask_price = current_net_ask
                    found_valid = True

        if not found_valid:
            continue

        results.append({
            '期权名称 (买入腿)': symbol,
            '期权名称 (卖出腿)': best_sell_symbol,
            '类型': opt_type_name,
            '距离结束的时间': time_left_str,
            '买入单价(ask)': ask_price,
            '卖出抵扣(bid)': best_sell_bid,
            '组合净单价': round(best_net_ask_price, 4),
            '单张预期净利': round(best_net_profit_per_contract, 4),
            '需买组合张数': round(best_num_contracts, 4),
            '原裸买总成本': round(naked_total_cost, 2) if naked_total_cost != float('inf') else '无法回本',
            '新组合总成本': round(best_total_cost, 2),
            '【原每日成本】': round(naked_daily_cost, 2) if naked_daily_cost != float('inf') else '无法回本',
            '【新每日成本】': round(best_daily_cost, 2)
        })

    if not results:
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    result_df = result_df.sort_values(by='【新每日成本】', ascending=True)
    return result_df


# ==========================================
# 4. 主流程逻辑
# ==========================================
if __name__ == "__main__":
    eth_df_file = 'binance_eth_options.csv'

    if os.path.exists(eth_df_file):
        print(f"[INFO] 找到本地缓存文件 {eth_df_file}，直接加载。")
        eth_df = pd.read_csv(eth_df_file)
    else:
        eth_df = get_binance_eth_options()
        if eth_df is not None:
            eth_df.to_csv(eth_df_file, index=False)
            print(f"[INFO] 数据已成功缓存至 {eth_df_file}。")

    current_eth_price = get_current_eth_price()
    print(f"\n[INFO] 当前 ETH 现货基准价格: {current_eth_price} U\n")

    # 核心业务假设参数
    grid_principal_to_protect = 1000.0  # 我们始终投入并保护的网格标准化本金
    option_max_margin = float('inf')  # 解除占用上限，交由全局 ROI 来做公平裁决

    summary_results = []
    print("[INFO] 启动寻优引擎: 正在全局测算 TARGET_LOSS (3% -> 30%) 的理论极限解...\n")

    for target_loss_pct in range(3, 31):
        target_price = current_eth_price * (1 - target_loss_pct / 100.0)

        try:
            margin_result = calculate_multi_group_margin(
                leverage=125.0,
                target_loss_percent=float(target_loss_pct),
                max_grids_per_group=169,
                fixed_qty=0.051,
                add_step_percent=0.15,
                initial_price=current_eth_price,
                direction='long'
            )
            actual_grid_margin_needed = margin_result['total_margin']
        except ValueError:
            continue

        if actual_grid_margin_needed <= 0 or actual_grid_margin_needed == float('inf'):
            continue

        # 将网格日利润缩放至千 U 标准化
        standardized_daily_profit = 20.0 * (grid_principal_to_protect / actual_grid_margin_needed)

        # 寻找保护千 U 本金的最优期权方案
        best_strategy_df = find_optimal_options_strategy(
            eth_df,
            target_price=target_price,
            target_profit=grid_principal_to_protect,
            min_hours=24,  # 强制过滤小于 24H 的垃圾末日轮
            max_margin=option_max_margin
        )

        if not best_strategy_df.empty:
            best_option = best_strategy_df.iloc[0]

            option_daily_cost = best_option['【新每日成本】']
            option_total_cost = best_option['新组合总成本']
            buy_leg = best_option['期权名称 (买入腿)']
            sell_leg = best_option['期权名称 (卖出腿)']
            option_time_left = best_option['距离结束的时间']
            num_contracts = best_option['需买组合张数']

            # 【新增修正与深度指标】
            net_daily_profit = standardized_daily_profit - option_daily_cost
            total_capital_employed = grid_principal_to_protect + option_total_cost  # 修复总投入资金错位 Bug
            theoretical_roi_percent = (net_daily_profit / total_capital_employed) * 100

            # 粗略估算做市商滑点/点差抽水
            friction_cost = (best_option['买入单价(ask)'] - best_option['卖出抵扣(bid)']) * num_contracts

            summary_results.append({
                '跌幅': f"{target_loss_pct}%",
                '爆仓价': round(target_price, 1),
                '期权到期时间': option_time_left,
                '标准网格本金': round(grid_principal_to_protect, 1),
                '期权占用': round(option_total_cost, 2),
                '【总投入资金】': round(total_capital_employed, 2),
                '期权组合': f"{buy_leg} / {sell_leg}",
                '【需买张数】': round(num_contracts, 2),
                '【点差摩擦】': round(friction_cost, 2),
                '网格日利': round(standardized_daily_profit, 2),
                '期权日耗': round(option_daily_cost, 2),
                '【千U日净利】': round(net_daily_profit, 2),
                '【日化ROI】': round(theoretical_roi_percent, 2)
            })

    if summary_results:
        summary_df = pd.DataFrame(summary_results)
        # 用最公平的“全口径资金日化ROI”进行排序
        summary_df = summary_df.sort_values(by='【日化ROI】', ascending=False)

        print(
            "======================================== 全局理论最优策略排行榜 ========================================")
        print(summary_df.head(20).to_string(index=False))
        print(
            "========================================================================================================")

        best_overall = summary_df.iloc[0]
        print(f"\n[CONCLUSION] 理论寻优结束。")
        print(f"基于全口径资金利用率，最高性价比跌幅设置为: TARGET_LOSS = {best_overall['跌幅']}")
        print(
            f"该方案日化 ROI 达 {best_overall['【日化ROI】']}% (注: 需结合【点差摩擦】与【期权到期时间】判断实盘落地可行性)")
    else:
        print("\n[WARNING] 在当前约束条件下，未能找到任何有效的正收益期权套保策略。")