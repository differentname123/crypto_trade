# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/5/16 0:20
:last_date:
    2026/5/16 0:20
:description:
    
"""
import os

import requests
import pandas as pd


def get_binance_eth_options():
    # 币安期权 (eOptions) 基础 API 域名
    base_url = "https://eapi.binance.com"

    # 24小时价格变动接口 (不传 symbol 默认返回所有期权)
    endpoint = "/eapi/v1/ticker"
    url = f"{base_url}{endpoint}"

    print(f"正在从 {url} 拉取数据...")

    try:
        # 发送 GET 请求
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # 检查请求是否成功

        data = response.json()

        # 币安期权的命名规则通常为: 标的-到期日-行权价-看涨/看跌 (例如: ETH-240531-3500-C)
        # 我们通过判断 symbol 是否以 'ETH-' 开头来筛选 ETH 期权
        eth_options = [item for item in data if item['symbol'].startswith('ETH-')]

        if not eth_options:
            print("未找到 ETH 相关的期权数据。")
            return None

        # 将数据转换为 Pandas DataFrame 以便更好地处理和展示
        df = pd.DataFrame(eth_options)

        # 我们挑选一些核心的字段进行展示
        # symbol: 期权名称
        # lastPrice: 最新成交价
        # bidPrice: 买一价
        # askPrice: 卖一价
        # volume: 24小时成交量 (张)
        columns_to_show = ['symbol', 'lastPrice', 'bidPrice', 'askPrice', 'volume']

        # 保留存在的列
        df = df[[col for col in columns_to_show if col in df.columns]]

        # 将价格和交易量转换为浮点数格式，方便后续排序或计算
        for col in ['lastPrice', 'bidPrice', 'askPrice', 'volume']:
            df[col] = df[col].astype(float)

        return df

    except requests.exceptions.RequestException as e:
        print(f"请求币安 API 失败，错误信息: {e}")
        return None


import pandas as pd
from datetime import datetime, timezone


def find_optimal_options_strategy(df, target_price, target_profit, min_hours=0, max_margin=100):
    results = []
    # 获取当前的绝对时间 (UTC 时区)
    now_utc = datetime.now(timezone.utc)

    # ================= 新增：预处理查找表 =================
    put_bids = {}
    for _, r in df.iterrows():
        sym = r['symbol']
        if isinstance(sym, str) and sym.startswith('ETH-') and sym.endswith('-P'):
            p = sym.split('-')
            if len(p) == 4:
                bid_p = r.get('bidPrice', 0)
                if not pd.isna(bid_p) and bid_p > 0:
                    put_bids[(p[1], float(p[2]))] = (sym, float(bid_p))
    # ======================================================

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

        # 1. 解析到期日字符串 (YYMMDD)
        year = int("20" + date_str[0:2])
        month = int(date_str[2:4])
        day = int(date_str[4:6])

        # 2. 构造精确的到期交割时间：UTC时间 到期日的 08:00:00
        expiry_datetime_utc = datetime(year, month, day, 8, 0, 0, tzinfo=timezone.utc)

        # 3. 计算精确的剩余时间
        time_left = expiry_datetime_utc - now_utc
        total_seconds_left = time_left.total_seconds()

        if total_seconds_left <= 60:
            continue

        # ================= 新增时间过滤逻辑 =================
        if total_seconds_left <= min_hours * 3600:
            continue
        # =================================================

        # ================= 新增：格式化“距离结束的时间” =================
        days = int(total_seconds_left // 86400)
        hours = int((total_seconds_left % 86400) // 3600)
        minutes = int((total_seconds_left % 3600) // 60)

        if days > 0:
            time_left_str = f"{days}天{hours}小时{minutes}分"
        elif hours > 0:
            time_left_str = f"{hours}小时{minutes}分"
        else:
            time_left_str = f"{minutes}分钟"
        # ==============================================================

        # 将秒数转化为精确的“天数”用于计算每日成本
        exact_days_to_expiry = total_seconds_left / 86400.0

        # ================= 修改：全局遍历寻找最优价差组合 =================
        # 【原裸买逻辑的成本保留计算，用于对比】
        naked_payoff = max(0, strike - target_price) if opt_type == 'P' else max(0, target_price - strike)
        naked_net_profit = naked_payoff - ask_price

        if naked_net_profit > 0:
            naked_num_contracts = target_profit / naked_net_profit
            naked_total_cost = naked_num_contracts * ask_price
            naked_daily_cost = naked_total_cost / exact_days_to_expiry
        else:
            naked_total_cost = float('inf')
            naked_daily_cost = float('inf')

        # 初始化最优解记录变量
        best_sell_symbol = "无"
        best_sell_bid = 0.0
        best_net_ask_price = 0.0
        best_net_profit_per_contract = 0.0
        best_num_contracts = 0.0
        best_total_cost = float('inf')
        best_daily_cost = float('inf')
        found_valid = False

        if opt_type == 'P':
            base_payoff = max(0, strike - target_price)
            # 候选卖出腿列表：默认包含不卖出(裸买)的情况
            candidates = [("无", 0.0, 0.0)]

            # 获取所有同到期日且行权价低于买入腿的 Put，全部加入候选进行全量测算
            available_strikes = [k[1] for k in put_bids.keys() if k[0] == date_str and k[1] < strike]
            for sell_strike in available_strikes:
                sym, bid = put_bids[(date_str, sell_strike)]
                sell_liab = max(0, sell_strike - target_price)
                candidates.append((sym, bid, sell_liab))

            # 遍历所有的卖出腿组合，选出满足资金上限且每日成本绝对最低的最优解
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
            # 看涨期权保持单腿逻辑
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

        # 将寻找到的最优数据赋给结果变量
        sell_symbol = best_sell_symbol
        sell_bid = best_sell_bid
        net_ask_price = best_net_ask_price
        net_profit_per_contract = best_net_profit_per_contract
        num_contracts = best_num_contracts
        total_cost = best_total_cost
        daily_cost = best_daily_cost
        # ==============================================================

        results.append({
            '期权名称 (买入腿)': symbol,
            '期权名称 (卖出腿)': sell_symbol,
            '类型': opt_type_name,
            '距离结束的时间': time_left_str,
            '买入单价(ask)': ask_price,
            '卖出抵扣(bid)': sell_bid,
            '组合净单价': round(net_ask_price, 4),
            '单张预期净利': round(net_profit_per_contract, 4),
            '需买组合张数': round(num_contracts, 4),
            '原裸买总成本': round(naked_total_cost, 2) if naked_total_cost != float('inf') else '无法回本',
            '新组合总成本': round(total_cost, 2),
            '【原每日成本】': round(naked_daily_cost, 2) if naked_daily_cost != float('inf') else '无法回本',
            '【新每日成本】': round(daily_cost, 2)
        })

    if not results:
        print(
            f"当前行情下，没有找到能在目标价位 {target_price} 赚取 {target_profit} 的可行合约 (资金占用上限: {max_margin})。")
        return pd.DataFrame()

    result_df = pd.DataFrame(results)
    # 按【新每日成本】从低到高排序
    result_df = result_df.sort_values(by='【新每日成本】', ascending=True)
    return result_df

if __name__ == "__main__":
    eth_df_file = 'binance_eth_options.csv'
    if os.path.exists(eth_df_file):
        eth_df = pd.read_csv(eth_df_file)
    else:
        eth_df = get_binance_eth_options()
    # eth_df = get_binance_eth_options()

    # 将 eth_df保存为 CSV 文件，方便后续分析和使用
    if eth_df is not None:
        eth_df.to_csv(eth_df_file, index=False)
        print(f"已将 ETH 期权数据保存到 'binance_eth_options.csv' 文件中！")

    # ================= 使用示例 =================
    # 假设 eth_df 是你之前代码跑出来的包含了最新行情的 DataFrame
    target_price = 2100   # 预期跌到的目标价
    target_profit = 1000  # 预期赚到的净利润

    print(f"\n寻找目标价 {target_price} 时净赚 {target_profit} 的最优策略...")
    best_strategy_df = find_optimal_options_strategy(eth_df, target_price=target_price, target_profit=target_profit)

    if not best_strategy_df.empty:
        print("\n====== 每日成本最低的 Top 10 方案 ======")
        print(best_strategy_df.head(100).to_string(index=False))

    # if eth_df is not None:
    #     print(f"\n成功获取到 {len(eth_df)} 个 ETH 期权合约最新数据！\n")
    #
    #     # 按24小时交易量从大到小排序，并提取前 15 名
    #     top_volume_df = eth_df.sort_values(by='volume', ascending=False).head(15)
    #
    #     print("====== 24小时交易量 Top 15 的 ETH 期权 ======")
    #     # 打印结果，不显示索引
    #     print(top_volume_df.to_string(index=False))