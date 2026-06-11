import os
import pandas as pd
import numpy as np
from typing import List

# 假设该依赖存在，保持不变
from app.fetch_biance_kline import fetch_binance_futures_klines


# ==========================================
# 1. 数据预处理适配器
# ==========================================
def build_4h_cross_section(minute_klines_list: List[pd.DataFrame], time_offset: str = '0h') -> pd.DataFrame:
    """
    将交易所拉取的【分钟级 K线列表】无损转换为信号引擎所需的【4H 截面 DataFrame】
    返回的 DataFrame 列名包含各币种的 open, high, low 以及作为 close 的币种名。
    """
    resampled_coin_dfs = []

    for df in minute_klines_list:
        if df is None or df.empty:
            continue

        df_coin = df.copy()

        # --- 兼容逻辑：处理 timestamp 字段开始 ---
        if 'open_time' not in df_coin.columns:
            if 'timestamp' in df_coin.columns:
                df_coin.rename(columns={'timestamp': 'open_time'}, inplace=True)
            elif df_coin.index.name == 'timestamp':
                df_coin.reset_index(inplace=True)
                df_coin.rename(columns={'timestamp': 'open_time'}, inplace=True)
        # --- 兼容逻辑：处理 timestamp 字段结束 ---

        # 1. 提取当前 df 对应的币种名称
        coin_name = df_coin['coin_name'].iloc[0]

        # 2. 统一时间索引处理
        if pd.api.types.is_numeric_dtype(df_coin['open_time']) and df_coin['open_time'].max() > 1e11:
            df_coin['open_time'] = pd.to_datetime(df_coin['open_time'], unit='ms')
        elif not pd.api.types.is_datetime64_any_dtype(df_coin['open_time']):
            df_coin['open_time'] = pd.to_datetime(df_coin['open_time'])

        # 冗余保留 time 列以防外部依赖
        df_coin['time'] = df_coin['open_time']

        df_coin.set_index('open_time', inplace=True)
        df_coin.sort_index(inplace=True)

        # 3. 核心对齐：使用与回测一致的重采样逻辑生成高低价
        df_coin_4h = df_coin['close'].resample('4h', offset=time_offset).agg(
            open='first',
            high='max',
            low='min',
            close='last'
        )

        # 4. 清理因时间错位产生的碎片空 K 线
        df_coin_4h.dropna(how='all', inplace=True)

        # 5. 统一重命名规范 (重要：下游引擎依赖此命名格式)
        df_coin_4h.rename(columns={
            'open': f"{coin_name}_open",
            'high': f"{coin_name}_high",
            'low': f"{coin_name}_low",
            'close': coin_name
        }, inplace=True)

        resampled_coin_dfs.append(df_coin_4h)

    if not resampled_coin_dfs:
        raise ValueError("传入的 minute_klines_list 全为空或无法解析！")

    # 6. 横向合并与前向填充兜底
    df_merged_raw = pd.concat(resampled_coin_dfs, axis=1).sort_index()
    df_merged_filled = df_merged_raw.ffill()

    return df_merged_filled


# ==========================================
# 2. 核心流式推演引擎 (账本级对齐)
# ==========================================
def run_strategy_simulation(
        df_cross_section: pd.DataFrame,
        strategy_params: dict,
        trade_mode: str,
        initial_capital: float = 10000.0,
        start_trade_date: str = '2026-04-27 00:00:00'
) -> pd.DataFrame:
    """
    流式模拟引擎：基于横截面特征，运行策略状态机推演，生成与回测 100% 一致的交易账本记录。
    """
    MOM_WINDOW = strategy_params['MOM_WINDOW']
    VOL_WINDOW = strategy_params['VOL_WINDOW']
    BTC_TREND_WINDOW = strategy_params['BTC_TREND_WINDOW']
    TOP_K = int(strategy_params.get('TOP_K', 2))
    MAX_WEIGHT = strategy_params['MAX_WEIGHT']
    FEE_RATE = 0.0005

    # 提取纯币种名称列（即收盘价列）
    target_coins = [c for c in df_cross_section.columns if
                    not any(suffix in c for suffix in ['_open', '_high', '_low'])]
    n_coins = len(target_coins)
    coin_to_idx = {c: idx for idx, c in enumerate(target_coins)}

    if 'BTC' not in target_coins:
        raise ValueError("数据矩阵中必须包含 BTC 作为宏观开关！")

    # === 批量计算向量化指标 ===
    df_close = df_cross_section[target_coins]
    df_returns = df_close.pct_change(MOM_WINDOW)

    df_high = df_cross_section[[f"{c}_high" for c in target_coins]].copy()
    df_high.columns = target_coins
    df_low = df_cross_section[[f"{c}_low" for c in target_coins]].copy()
    df_low.columns = target_coins
    df_prev_close = df_close.shift(1)

    # TR 和 ATR 计算
    true_range_arr = np.fmax.reduce([
        (df_high - df_low).values,
        (df_high - df_prev_close).abs().values,
        (df_low - df_prev_close).abs().values
    ])
    df_atr = pd.DataFrame(true_range_arr, index=df_cross_section.index, columns=target_coins).rolling(
        window=VOL_WINDOW).mean()
    df_volatility_pct = df_atr / df_close

    # 风险调整后动量
    df_adj_mom = df_returns / (df_volatility_pct + 1e-8)

    # 宏观大盘开关
    df_btc_ma = df_cross_section['BTC'].rolling(window=BTC_TREND_WINDOW).mean()
    df_btc_trend_active = df_cross_section['BTC'] > df_btc_ma

    # 提取底层的 Numpy 数组加速后续流式循环
    mom_arr = df_adj_mom.values
    vol_arr = df_volatility_pct.values
    btc_trend_arr = df_btc_trend_active.values
    close_arr = df_close.values
    time_index = df_cross_section.index

    # === 状态机初始化 ===
    cash = float(initial_capital)
    positions_arr = np.zeros(n_coins, dtype=float)
    coin_states = {c: {'qty': 0.0, 'cost': 0.0, 'side': None} for c in target_coins}
    trade_ledger = []

    warmup_period = max(MOM_WINDOW, VOL_WINDOW, BTC_TREND_WINDOW)
    kline_signal_diagnostics = ["无信号: 指标预热期"] * len(df_cross_section)
    start_trade_timestamp = pd.to_datetime(start_trade_date) if start_trade_date else None

    # === 逐根 K 线流转推演 ===
    for i in range(warmup_period, len(df_cross_section)):
        current_time = time_index[i]
        current_prices = close_arr[i]

        total_equity = cash + np.dot(positions_arr, current_prices)

        current_mom = mom_arr[i]
        current_vol = vol_arr[i]
        is_btc_trend_on = btc_trend_arr[i]

        candidate_longs = []
        candidate_shorts = []

        # 候选队列筛选
        if is_btc_trend_on:
            if trade_mode in ['BOTH', 'LONG_ONLY']:
                mask = ~np.isnan(current_mom) & (current_mom > 0)
                if mask.any():
                    valid_idx = np.where(mask)[0]
                    valid_vals = current_mom[valid_idx]
                    order = np.argsort(-valid_vals, kind='stable')
                    candidate_longs = [target_coins[idx] for idx in valid_idx[order[:TOP_K]]]
        else:
            if trade_mode in ['BOTH', 'SHORT_ONLY']:
                mask = ~np.isnan(current_mom) & (current_mom < 0)
                if mask.any():
                    valid_idx = np.where(mask)[0]
                    valid_vals = current_mom[valid_idx]
                    order = np.argsort(valid_vals, kind='stable')
                    candidate_shorts = [target_coins[idx] for idx in valid_idx[order[:TOP_K]]]

        # 时间拦截器：未到发车时间，强制掐断交易候选名单
        if start_trade_timestamp is not None and current_time < start_trade_timestamp:
            candidate_longs = []
            candidate_shorts = []
            kline_signal_diagnostics[i] = "无信号: 未到设定的发车时间"
        else:
            # 记录当根 K 线的具体信号状态及原因
            if is_btc_trend_on:
                if trade_mode in ['BOTH', 'LONG_ONLY']:
                    if candidate_longs:
                        kline_signal_diagnostics[i] = f"有信号 (做多): {', '.join(candidate_longs)}"
                    else:
                        kline_signal_diagnostics[i] = "无信号: 大盘看多，但所有标的动量均不满足做多阈值"
                else:
                    kline_signal_diagnostics[i] = "无信号: 大盘看多，但策略模式禁止做多"
            else:
                if trade_mode in ['BOTH', 'SHORT_ONLY']:
                    if candidate_shorts:
                        kline_signal_diagnostics[i] = f"有信号 (做空): {', '.join(candidate_shorts)}"
                    else:
                        kline_signal_diagnostics[i] = "无信号: 大盘看空，但所有标的动量均不满足做空阈值"
                else:
                    kline_signal_diagnostics[i] = "无信号: 大盘看空，但策略模式禁止做空"

        # --- A. 平仓逻辑 ---
        for idx_c in range(n_coins):
            c = target_coins[idx_c]

            # 平多
            if positions_arr[idx_c] > 0 and c not in candidate_longs:
                sell_amount = positions_arr[idx_c]
                actual_sell_val = sell_amount * current_prices[idx_c]
                fee = actual_sell_val * FEE_RATE
                positions_arr[idx_c] = 0
                cash += (actual_sell_val - fee)

                cost = coin_states[c]['cost']
                net_pnl = sell_amount * (current_prices[idx_c] - cost) - fee
                pnl_pct = (net_pnl / (cost * sell_amount)) * 100 if cost > 0 else 0.0

                close_reason = "大盘开关关闭" if not is_btc_trend_on else "掉出排名"

                trade_ledger.append({
                    "time": current_time, "action": "SELL", "coin": c, "direction": "LONG", "event": "CLOSE",
                    "price": current_prices[idx_c], "amount": sell_amount, "value": actual_sell_val, "fee": fee,
                    "reason": close_reason, "target_weight": 0.0, "pnl": pnl_pct,
                    "top_k": TOP_K, "max_weight": MAX_WEIGHT
                })
                coin_states[c] = {'qty': 0.0, 'cost': 0.0, 'side': None}

            # 平空
            elif positions_arr[idx_c] < 0 and c not in candidate_shorts:
                buy_amount = abs(positions_arr[idx_c])
                actual_buy_val = buy_amount * current_prices[idx_c]
                fee = actual_buy_val * FEE_RATE
                positions_arr[idx_c] = 0
                cash -= (actual_buy_val + fee)

                cost = coin_states[c]['cost']
                net_pnl = buy_amount * (cost - current_prices[idx_c]) - fee
                pnl_pct = (net_pnl / (cost * buy_amount)) * 100 if cost > 0 else 0.0

                close_reason = "大盘开关关闭" if is_btc_trend_on else "掉出排名"

                trade_ledger.append({
                    "time": current_time, "action": "BUY", "coin": c, "direction": "SHORT", "event": "CLOSE",
                    "price": current_prices[idx_c], "amount": buy_amount, "value": actual_buy_val, "fee": fee,
                    "reason": close_reason, "target_weight": 0.0, "pnl": pnl_pct,
                    "top_k": TOP_K, "max_weight": MAX_WEIGHT
                })
                coin_states[c] = {'qty': 0.0, 'cost': 0.0, 'side': None}

        # --- B. 开仓逻辑 (多) ---
        if candidate_longs:
            inv_vols = [1.0 / current_vol[coin_to_idx[c]] if current_vol[coin_to_idx[c]] > 0 else 0 for c in
                        candidate_longs]
            total_inv_vol = sum(inv_vols)

            for k_, c in enumerate(candidate_longs):
                idx_c = coin_to_idx[c]
                if positions_arr[idx_c] == 0 and total_inv_vol > 0:
                    target_weight = min(inv_vols[k_] / total_inv_vol, MAX_WEIGHT)
                    target_val = total_equity * target_weight

                    buy_val = target_val / (1 + FEE_RATE) if cash >= target_val / (1 + FEE_RATE) else cash / (
                                1 + FEE_RATE)

                    if buy_val > 1.0:
                        fee = buy_val * FEE_RATE
                        buy_amount = buy_val / current_prices[idx_c]
                        positions_arr[idx_c] += buy_amount
                        cash -= (buy_val + fee)

                        coin_states[c] = {
                            'qty': buy_amount,
                            'cost': current_prices[idx_c] + (fee / buy_amount),
                            'side': 'LONG'
                        }

                        trade_ledger.append({
                            "time": current_time, "action": "BUY", "coin": c, "direction": "LONG", "event": "OPEN",
                            "price": current_prices[idx_c], "amount": buy_amount, "value": buy_val, "fee": fee,
                            "reason": "Signal Entry Long", "target_weight": target_weight, "pnl": np.nan,
                            "top_k": TOP_K, "max_weight": MAX_WEIGHT
                        })

        # --- C. 开仓逻辑 (空) ---
        if candidate_shorts:
            inv_vols = [1.0 / current_vol[coin_to_idx[c]] if current_vol[coin_to_idx[c]] > 0 else 0 for c in
                        candidate_shorts]
            total_inv_vol = sum(inv_vols)

            for k_, c in enumerate(candidate_shorts):
                idx_c = coin_to_idx[c]
                if positions_arr[idx_c] == 0 and total_inv_vol > 0:
                    target_weight = min(inv_vols[k_] / total_inv_vol, MAX_WEIGHT)
                    sell_val = total_equity * target_weight / (1 + FEE_RATE)

                    if sell_val > 1.0:
                        fee = sell_val * FEE_RATE
                        sell_amount = sell_val / current_prices[idx_c]
                        positions_arr[idx_c] -= sell_amount
                        cash += (sell_val - fee)

                        coin_states[c] = {
                            'qty': -sell_amount,
                            'cost': current_prices[idx_c] - (fee / sell_amount),
                            'side': 'SHORT'
                        }

                        trade_ledger.append({
                            "time": current_time, "action": "SELL", "coin": c, "direction": "SHORT", "event": "OPEN",
                            "price": current_prices[idx_c], "amount": sell_amount, "value": sell_val, "fee": fee,
                            "reason": "Signal Entry Short", "target_weight": target_weight, "pnl": np.nan,
                            "top_k": TOP_K, "max_weight": MAX_WEIGHT
                        })

    # 将状态原因落表追踪
    df_cross_section['signal_status'] = kline_signal_diagnostics

    return pd.DataFrame(trade_ledger)


# ==========================================
# 3. 实盘自动化主流水线
# ==========================================
def run_live_pipeline(minute_klines_list: List[pd.DataFrame]):
    """
    接收最新行情数据，驱动整套策略流水线，并生成当前最新截面的操作指令。
    重要解释说明：
        1.合成的4hbar的时间代表的是4h的起始时间，也就是这行数据代表的是这个时间 到 +4h的价格变动情况。而且只要有1分钟数据了，就会生成4hbar，所以这个4hbar不一定包含了4小时的完整数据，可能是1分钟数据，也可能是2小时数据，甚至3小时数据，这取决于当前时间距离上一个4hbar的起始时间有多远。
        2.最终信号的时间表示交易的产生时间，按照理想的信号生成情况就是使用完整的bar进行信号的产生，所以最终信号的时间应该是这个4hbar的起始时间 + 4小时。因为这个时候才是真正的这个4hbar走完了，才有了完整的价格变动情况，才可以进行信号的计算和生成。


    """
    BEST_PARAMS = {
        'MOM_WINDOW': 90,
        'VOL_WINDOW': 120,
        'BTC_TREND_WINDOW': 720,
        'MAX_WEIGHT': 0.05,
        'TOP_K': 3
    }
    TIME_OFFSET = '0h'
    TRADE_MODE = 'SHORT_ONLY'

    print("⏳ 1. 正在将分钟级数据组装为 4H 矩阵...")
    df_4h_features = build_4h_cross_section(minute_klines_list, time_offset=TIME_OFFSET)

    if df_4h_features is None or df_4h_features.empty:
        return

    # 日志：展示数据边界
    start_time_str = df_4h_features.index[0].strftime('%Y-%m-%d %H:%M:%S')
    end_time_str = df_4h_features.index[-1].strftime('%Y-%m-%d %H:%M:%S')
    print(f"   起始: {start_time_str} | 截止: {end_time_str}")

    print("🧠 2. 正在运行状态推演机，生成全量理论交易账本...")
    trade_ledger_df = run_strategy_simulation(
        df_cross_section=df_4h_features,
        strategy_params=BEST_PARAMS,
        trade_mode=TRADE_MODE
    )

    if trade_ledger_df.empty:
        print("► 历史流转中尚未产生任何交易信号。")
        return

    # 重要：对齐实盘执行时间。4H K线的开盘时间需加上4小时，代表K线走完真正执行信号的时刻。
    trade_ledger_df['time'] = pd.to_datetime(trade_ledger_df['time']) + pd.Timedelta(hours=4)

    # 3. 导出完整的流水日志
    output_path = "live_simulation_logs.csv"
    trade_ledger_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"✅ 全量交易流水(Ledger)已生成: {output_path} (共 {len(trade_ledger_df)} 条记录)")

    # 4. 提取当下的实盘发单指令
    latest_kline_end_time = df_4h_features.index[-1] + pd.Timedelta(hours=4)
    latest_trade_signals = trade_ledger_df[trade_ledger_df['time'] == latest_kline_end_time]

    print(f"\n🎯 [当前截面时刻: {latest_kline_end_time} 实盘发单指令]")
    if latest_trade_signals.empty:
        print("   ► 当前无平仓或开仓信号，继续保持现有仓位。")
    else:
        for _, row in latest_trade_signals.iterrows():
            if row['event'] == 'CLOSE':
                print(
                    f"   🔴 平仓指令 | {row['action']:<4} {row['coin']:<4} | 方向: {row['direction']:<5} | 数量: {row['amount']:.4f} | 原因: {row['reason']}")
            elif row['event'] == 'OPEN':
                print(
                    f"   🟢 开仓指令 | {row['action']:<4} {row['coin']:<4} | 方向: {row['direction']:<5} | 目标权重: {row['target_weight'] * 100:.1f}% | 原因: {row['reason']}")


# ==========================================
# 4. 程序入口点
# ==========================================
def execute_trading_bot_workflow():
    """
    拉取数据并启动整套交易工作流
    """
    fetched_raw_data = []

    # 基础设置
    lookback_days = 300
    symbol_list = [
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "XRP/USDT:USDT", "BNB/USDT:USDT", "DOGE/USDT:USDT"
    ]
    timeframe = "1m"

    for symbol in symbol_list:
        df_klines = fetch_binance_futures_klines(symbol=symbol, timeframe=timeframe, days=lookback_days)
        # 提取纯币种名如 'BTC'
        coin_name = symbol.split('/')[0]
        df_klines['coin_name'] = coin_name

        fetched_raw_data.append(df_klines)

    if not fetched_raw_data:
        print("❌ 错误：没有任何数据被成功加载，程序退出。请检查网络或 fetch_binance_futures_klines 模块。")
    else:
        print(f"\n🚀 数据加载完毕，共 {len(fetched_raw_data)} 个标的。准备进入信号生成流水线...\n")
        print("═" * 70)
        run_live_pipeline(fetched_raw_data)


if __name__ == "__main__":
    execute_trading_bot_workflow()