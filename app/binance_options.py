# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/5/16
:description:
    网格与期权动态对冲寻优引擎 (生产版) - 支持多币种动态配置
"""
import os
import requests
import pandas as pd
from datetime import datetime, timezone


# ==========================================
# 1. 数据获取模块
# ==========================================
def get_binance_options(symbol="ETH"):
    """从币安拉取最新的指定币种期权行情数据"""
    url = "https://eapi.binance.com/eapi/v1/ticker"
    print(f"[INFO] 正在请求币安期权接口: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        target_prefix = f"{symbol.upper()}-"
        target_options = [item for item in data if item['symbol'].startswith(target_prefix)]
        if not target_options:
            print(f"[WARNING] 接口请求成功，但未找到 {symbol.upper()} 相关期权数据。")
            return None

        df = pd.DataFrame(target_options)
        columns_to_show = ['symbol', 'lastPrice', 'bidPrice', 'askPrice', 'volume']
        df = df[[col for col in columns_to_show if col in df.columns]]

        for col in ['lastPrice', 'bidPrice', 'askPrice', 'volume']:
            df[col] = df[col].astype(float)

        print(f"[INFO] 成功拉取并清洗 {len(df)} 条 {symbol.upper()} 期权数据。")
        return df
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] 请求币安 API 失败: {e}")
        return None


def get_current_spot_price(symbol="ETH"):
    """获取当前指定币种现货价格"""
    try:
        target_pair = f"{symbol.upper()}USDT"
        resp = requests.get(f"https://api.binance.com/api/v3/ticker/price?symbol={target_pair}", timeout=5)
        resp.raise_for_status()
        return float(resp.json()['price'])
    except Exception as e:
        print(f"[WARNING] 获取 {symbol.upper()} 现货价格失败: {e}。")
        return 0.0


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
def find_optimal_options_strategy(df, target_price, target_profit, symbol="ETH", min_hours=48, max_margin=1000):
    """全局遍历寻找给定目标价与利润要求下的最优期权价差组合"""
    results = []
    now_utc = datetime.now(timezone.utc)
    target_prefix = f"{symbol.upper()}-"

    # 预处理查找表 (加速卖出腿的匹配)
    put_bids = {}
    for _, r in df.iterrows():
        sym = r['symbol']
        if isinstance(sym, str) and sym.startswith(target_prefix) and sym.endswith('-P'):
            p = sym.split('-')
            if len(p) == 4:
                bid_p = r.get('bidPrice', 0)
                if not pd.isna(bid_p) and bid_p > 0:
                    put_bids[(p[1], float(p[2]))] = (sym, float(bid_p))

    for index, row in df.iterrows():
        opt_symbol = row['symbol']
        ask_price = row.get('askPrice', 0)

        if pd.isna(ask_price) or ask_price <= 0:
            continue

        parts = opt_symbol.split('-')
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
            '期权名称 (买入腿)': opt_symbol,
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
# 4. 业务流程拆解 (提高可读性)
# ==========================================
def load_options_data(symbol: str) -> pd.DataFrame:
    """加载并缓存期权数据"""
    cache_file = f'binance_{symbol.lower()}_options.csv'
    # if os.path.exists(cache_file):
    #     print(f"[INFO] 找到本地缓存文件 {cache_file}，直接加载。")
    #     return pd.read_csv(cache_file)
    # else:
    df = get_binance_options(symbol)
    if df is not None and not df.empty:
        df.to_csv(cache_file, index=False)
        print(f"[INFO] 数据已成功缓存至 {cache_file}。")
    return df


def run_optimization_engine(symbol: str, current_price: float, options_df: pd.DataFrame) -> pd.DataFrame:
    """执行核心测算寻优逻辑"""
    grid_principal_to_protect = 1000.0  # 始终投入并保护的网格标准化本金
    option_max_margin = float('inf')  # 解除占用上限，交由全局 ROI 裁决

    summary_results = []
    print(f"[INFO] 启动 {symbol.upper()} 寻优引擎: 正在全局测算 TARGET_LOSS (3% -> 30%) 的理论极限解...\n")

    for target_loss_pct in range(1, 31):
        target_price = current_price * (1 - target_loss_pct / 100.0)

        try:
            margin_result = calculate_multi_group_margin(
                leverage=125.0,
                target_loss_percent=float(target_loss_pct),
                max_grids_per_group=169,
                fixed_qty=0.051,
                add_step_percent=0.15,
                initial_price=current_price,
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
            df=options_df,
            target_price=target_price,
            target_profit=grid_principal_to_protect,
            symbol=symbol,
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

            net_daily_profit = standardized_daily_profit - option_daily_cost
            total_capital_employed = grid_principal_to_protect + option_total_cost
            theoretical_roi_percent = (net_daily_profit / total_capital_employed) * 100

            summary_results.append({
                '跌幅': f"{target_loss_pct}%",
                '爆仓价': round(target_price, 1),
                '期权到期时间': option_time_left,
                '标准网格本金': round(grid_principal_to_protect, 1),
                '期权占用': round(option_total_cost, 2),
                '【总投入资金】': round(total_capital_employed, 2),
                '期权组合': f"{buy_leg} / {sell_leg}",
                '【需买张数】': round(num_contracts, 2),
                '网格日利': round(standardized_daily_profit, 2),
                '期权日耗': round(option_daily_cost, 2),
                '【千U日净利】': round(net_daily_profit, 2),
                '【日化ROI】': round(theoretical_roi_percent, 2)
            })

    return pd.DataFrame(summary_results)


def display_results(summary_df: pd.DataFrame):
    """格式化打印输出结论"""
    if not summary_df.empty:
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
        print(f"该方案日化 ROI 达 {best_overall['【日化ROI】']}% (注: 需结合【期权到期时间】判断实盘落地可行性)")
    else:
        print("\n[WARNING] 在当前约束条件下，未能找到任何有效的正收益期权套保策略。")


# ==========================================
# 5. 主程序入口
# ==========================================
if __name__ == "__main__":
    # Pandas 显示配置
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')

    # --- 核心控制参数 ---
    TARGET_SYMBOL = "ETH"  # 修改此处可一键切换为 BTC、SOL 等其他期权币种
    # --------------------

    # 1. 准备数据
    df_options = load_options_data(TARGET_SYMBOL)
    if df_options is None or df_options.empty:
        print(f"[ERROR] 无法获取或加载 {TARGET_SYMBOL} 期权数据，程序退出。")
        exit()

    current_spot_price = get_current_spot_price(TARGET_SYMBOL)
    if current_spot_price <= 0:
        print(f"[ERROR] 无法获取 {TARGET_SYMBOL} 现货价格，程序退出。")
        exit()

    print(f"\n[INFO] 当前 {TARGET_SYMBOL.upper()} 现货基准价格: {current_spot_price} U\n")

    # 2. 运行寻优引擎
    final_results = run_optimization_engine(TARGET_SYMBOL, current_spot_price, df_options)

    # 3. 打印报表
    display_results(final_results)