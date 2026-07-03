from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from common_utils_lite import setup_logger
from fetch_data_quick import snipe_kline_data


def build_4h_cross_section(logger, minute_klines_list, time_offset='0h'):
    """
    将交易所拉取的【分钟级 K线列表】无损转换为信号引擎所需的【4H 截面 DataFrame】
    返回的 DataFrame 列名包含各币种的 open, high, low 以及作为 close 的币种名。
    """
    resampled_coin_dfs = []

    # [新增] 记录所有币种在底层 1m 级别的时间首尾边界
    m1_starts = []
    m1_ends = []

    for df in minute_klines_list:
        if df is None or df.empty:
            continue

        df_coin = df.copy()

        # 1. 提取当前 df 对应的币种名称
        coin_name = df_coin['coin_name'].iloc[0]

        # 2. 统一时间索引处理：保持标准 UTC 时间，不直接进行暴力的数值加减
        df_coin['timestamp'] = pd.to_datetime(df_coin['timestamp'], unit='ms')

        # 冗余保留 time 列以防外部依赖
        df_coin['time'] = df_coin['timestamp']

        df_coin.set_index('timestamp', inplace=True)
        df_coin.sort_index(inplace=True)

        # [新增] 提取当前币种 1m 数据的精确首尾时间
        m1_starts.append(df_coin.index[0])
        m1_ends.append(df_coin.index[-1])

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

    # [新增] 严格取交集：找出最晚上市的起点和最早结束的终点，输出精确 1m 日志
    common_1m_start = max(m1_starts)
    common_1m_end = min(m1_ends)

    # 格式化时间为字符串，去除时区偏移信息，保持清爽
    start_str = common_1m_start.tz_localize('UTC').tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')
    end_str = common_1m_end.tz_localize('UTC').tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')
    logger.info(f"真正对最终 4h 数据有贡献的底层 1m 数据范围: {start_str} 至 {end_str} (北京时间)")

    # 6. 横向合并与严格交集对齐 (对齐了 prepare_environment 中的公共区间截断逻辑)
    df_merged_raw = pd.concat(resampled_coin_dfs, axis=1).sort_index()

    # 提取只包含收盘价(即币种名)的主列
    main_coins = [c for c in df_merged_raw.columns if not any(x in c for x in ['_open', '_high', '_low'])]

    # 锁定所有币种在 4h 级别都有数据的绝对公共区间
    coin_starts_4h = [df_merged_raw[c].first_valid_index() for c in main_coins]
    coin_ends_4h = [df_merged_raw[c].last_valid_index() for c in main_coins]

    common_4h_start = max(coin_starts_4h)
    common_4h_end = min(coin_ends_4h)

    # 先用 loc 截断所有非公共期的参差不齐的数据，然后再执行 ffill 兜底
    df_merged_filled = df_merged_raw.loc[common_4h_start:common_4h_end].ffill()

    return df_merged_filled


# ==========================================
# 2. 核心流式推演引擎 (账本级对齐) - 严禁修改核心逻辑
# ==========================================
def run_strategy_simulation(
        df_cross_section: pd.DataFrame,
        strategy_params: dict,
        trade_mode: str,
        initial_capital: float = 10000.0,
        start_trade_date: str = '2026-04-27 00:00:00',
        logger = None
) -> pd.DataFrame:
    """
    流式模拟引擎：基于横截面特征，运行策略状态机推演，生成与回测 100% 一致的交易账本记录。
    """
    MOM_WINDOW = strategy_params['MOM_WINDOW']
    VOL_WINDOW = strategy_params['VOL_WINDOW']
    BTC_TREND_WINDOW = strategy_params['BTC_TREND_WINDOW']
    TOP_K = int(strategy_params.get('TOP_K', 2))
    MAX_WEIGHT = strategy_params['MAX_WEIGHT']
    FEE_RATE = 0.000

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

    # --- 新增：为日志提取必需的基准数组 ---
    # 1. 提取 MOM_WINDOW 周期前的价格作为零动量阈值价格矩阵
    ref_price_arr = df_close.shift(MOM_WINDOW).values
    # 2. 提取 BTC 均线数组，避免在循环体内产生 pd.Series 寻址开销
    btc_ma_arr = df_btc_ma.values
    # -----------------------------------

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

                if not is_btc_trend_on:
                    close_reason = "大盘开关关闭"
                elif current_mom[idx_c] <= 0:
                    close_reason = "动量转负退场"
                else:
                    close_reason = "掉出前K名排名"
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

                if is_btc_trend_on:
                    close_reason = "大盘开关关闭"
                elif current_mom[idx_c] >= 0:
                    close_reason = "动量转正退场"
                else:
                    close_reason = "掉出前K名排名"
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

        # --- 新增：打印最新一根 K 线的详细情况 (含参数周期、零动量价格及价格偏差) ---
        if i == len(df_cross_section) - 1 and logger is not None:
            logger.info(f"\n{'=' * 25} 最新 K 线信号详情 {'=' * 25}")
            logger.info(f"时间: {current_time}")
            logger.info(f"策略参数: 动量周期={MOM_WINDOW}, 波动率周期={VOL_WINDOW}, 前K名={TOP_K}")

            # 提取大盘数据与偏差
            btc_idx = coin_to_idx.get('BTC', -1)
            if btc_idx != -1:
                current_btc_price = current_prices[btc_idx]
                current_btc_ma = btc_ma_arr[i]
                btc_deviation = (current_btc_price - current_btc_ma) / current_btc_ma if current_btc_ma > 0 else 0.0

                logger.info(f"BTC 大盘开关: {'打开 (多头趋势)' if is_btc_trend_on else '关闭 (空头趋势)'}")
                logger.info(f"  ├─ BTC 当前价: {current_btc_price:.2f}")
                logger.info(f"  ├─ 均线阈值 ({BTC_TREND_WINDOW} 周期): {current_btc_ma:.2f}")
                logger.info(f"  └─ 当前偏差幅度: {btc_deviation:+.2%}")
            else:
                logger.info(f"BTC 大盘开关: {'打开 (多头趋势)' if is_btc_trend_on else '关闭 (空头趋势)'}")

            logger.info(f"当前策略模式: {trade_mode}")
            logger.info(f"本期信号诊断: {kline_signal_diagnostics[i]}")

            # 判断当前激活的候选列表
            active_candidates = candidate_longs if is_btc_trend_on else candidate_shorts
            direction_str = "做多候选" if is_btc_trend_on else "做空候选"

            logger.info("-" * 20 + " 候选币种详细参数 " + "-" * 20)
            if active_candidates:
                for c in active_candidates:
                    idx = coin_to_idx[c]
                    p_current = current_prices[idx]
                    p_threshold = ref_price_arr[i, idx]
                    p_dev = (p_current - p_threshold) / p_threshold if p_threshold > 0 else 0.0

                    logger.info(
                        f"[{direction_str}] 标的: {c:<8} | 风险调整后动量: {current_mom[idx]:>8.4f} | 波动率: {current_vol[idx]:.4%}")
                    logger.info(
                        f"            └─ 当前价: {p_current:<10.4f} | 零动量阈值价: {p_threshold:<10.4f} | 价格偏差涨跌幅: {p_dev:+.2%}")
            else:
                logger.info("当前无候选发车币种 (可能原因: 动量不达标 / 策略模式限制 / 未到发车时间)。")

            logger.info("-" * 20 + " 其他未入选币种情况 " + "-" * 20)
            other_coins = [c for c in target_coins if c not in active_candidates]
            for c in other_coins:
                idx = coin_to_idx[c]
                p_current = current_prices[idx]
                p_threshold = ref_price_arr[i, idx]
                p_dev = (p_current - p_threshold) / p_threshold if p_threshold > 0 else 0.0

                logger.info(
                    f"[未入选]   标的: {c:<8} | 风险调整后动量: {current_mom[idx]:>8.4f} | 波动率: {current_vol[idx]:.4%}")
                logger.info(
                    f"            └─ 当前价: {p_current:<10.4f} | 零动量阈值价: {p_threshold:<10.4f} | 价格偏差涨跌幅: {p_dev:+.2%}")

            logger.info("=" * 68 + "\n")

    # 将状态原因落表追踪
    df_cross_section['signal_status'] = kline_signal_diagnostics

    return pd.DataFrame(trade_ledger)


# ==========================================
# 3. 实盘自动化主流水线
# ==========================================
def run_live_pipeline(minute_klines_list: list, strategy_params_list: list, logger):
    """
    接收最新行情数据，遍历运行多个参数策略流水线，并生成各参数当前最新截面的操作指令。
    返回合并后的全量交易流水账本 DataFrame。
    """
    all_strategy_ledgers = []

    # [新增] 动态建立纯币种名到完整 symbol 的映射字典，避免硬编码后缀
    coin_to_symbol = {}
    for df in minute_klines_list:
        if df is not None and not df.empty and 'coin_name' in df.columns and 'symbol' in df.columns:
            coin_to_symbol[df['coin_name'].iloc[0]] = df['symbol'].iloc[0]

    # 循环遍历运行多个参数
    for params in strategy_params_list:
        strategy_name = params['STRATEGY_NAME']
        time_offset = params['TIME_OFFSET']
        trade_mode = params['TRADE_MODE']

        logger.info(f"⏳ [策略: {strategy_name}] 正在将分钟级数据组装为 4H 矩阵 (Offset: {time_offset})...")
        df_4h_features = build_4h_cross_section(logger, minute_klines_list, time_offset=time_offset)

        if df_4h_features is None or df_4h_features.empty:
            continue

        # 日志：展示数据边界 (标准处理：声明当前为 UTC，并转换为北京时间展示)
        start_time_bjt = df_4h_features.index[0].tz_localize('UTC').tz_convert('Asia/Shanghai').strftime(
            '%Y-%m-%d %H:%M:%S')
        end_time_bjt = df_4h_features.index[-1].tz_localize('UTC').tz_convert('Asia/Shanghai').strftime(
            '%Y-%m-%d %H:%M:%S')
        logger.info(f"   [{strategy_name}] 起始: {start_time_bjt} | 截止: {end_time_bjt} (北京时间)")

        trade_ledger_df = run_strategy_simulation(
            df_cross_section=df_4h_features,
            strategy_params=params,
            trade_mode=trade_mode,
            logger=logger
        )

        if trade_ledger_df.empty:
            logger.info(f"🧠 [{strategy_name}] 正在运行状态推演机，生成理论交易账本 | 生成数量: 0 | 最新信号时间: 无")
            logger.info(f"► [{strategy_name}] 历史流转中尚未产生任何交易信号。")
        else:
            # 重要：对齐实盘执行时间。4H K线的开盘时间需加上4小时，代表K线走完真正执行信号的时刻。(此时依然是纯净的 UTC 时间)
            trade_ledger_df['time'] = pd.to_datetime(trade_ledger_df['time']) + pd.Timedelta(hours=4)

            # 【核心健壮点 1】生成绝对准确的毫秒级 Unix 时间戳 (基于纯净 UTC 计算，无视任何时区漂移，保证下游 API 不会认错)
            trade_ledger_df['signal_timestamp_ms'] = trade_ledger_df['time'].astype('int64') // 10 ** 6

            # 【核心健壮点 2】将 dataframe 的 time 列安全转换为北京时间，最后剥离时区标签 (tz_localize(None))，保证输出给人类看的 CSV 格式清爽且时间正确
            trade_ledger_df['time'] = trade_ledger_df['time'].dt.tz_localize('UTC').dt.tz_convert(
                'Asia/Shanghai').dt.tz_localize(None)

            # 新增参数标识，方便区分产生的交易数据
            trade_ledger_df['STRATEGY_NAME'] = strategy_name

            # [新增] 动态为账本添加完整的 symbol 字段，若映射失败则根据示例规则自动拼接兜底
            trade_ledger_df['symbol'] = trade_ledger_df['coin'].map(coin_to_symbol).fillna(
                trade_ledger_df['coin'] + '/USDT:USDT')

            all_strategy_ledgers.append(trade_ledger_df)

            gen_count = len(trade_ledger_df)
            latest_signal_time_bjt = trade_ledger_df['time'].max().strftime('%Y-%m-%d %H:%M:%S')

            # 后置推演日志：此时已获知推演量及时间
            logger.info(
                f"🧠 [{strategy_name}] 正在运行状态推演机，生成理论交易账本 | 生成数量: {gen_count} | 最新信号时间: {latest_signal_time_bjt} (北京时间)")

        # 4. 提取当前策略在当下截面的实盘发单指令
        # 计算最新截面执行时间的 UTC 值
        latest_kline_end_time_utc = df_4h_features.index[-1] + pd.Timedelta(hours=4)
        # 将其转化为无标签的北京时间，用于和账本（已转为北京时间）进行精准匹配
        latest_kline_end_time_bjt = latest_kline_end_time_utc.tz_localize('UTC').tz_convert(
            'Asia/Shanghai').tz_localize(None)

        if not trade_ledger_df.empty:
            latest_trade_signals = trade_ledger_df[trade_ledger_df['time'] == latest_kline_end_time_bjt]
        else:
            latest_trade_signals = pd.DataFrame()

        logger.info(
            f"🎯 [当前截面时刻: {latest_kline_end_time_bjt.strftime('%Y-%m-%d %H:%M:%S')} (北京时间) | 策略: {strategy_name} 实盘发单指令]")
        if latest_trade_signals.empty:
            logger.info("   ► 当前无平仓或开仓信号，继续保持现有仓位。")
        else:
            for _, row in latest_trade_signals.iterrows():
                if row['event'] == 'CLOSE':
                    logger.info(
                        f"   🔴 平仓指令 | {row['action']:<4} {row['coin']:<4} | 方向: {row['direction']:<5} | 价格: {row['price']} | 数量: {row['amount']:.4f} | 原因: {row['reason']}")
                elif row['event'] == 'OPEN':
                    logger.info(
                        f"   🟢 开仓指令 | {row['action']:<4} {row['coin']:<4} | 方向: {row['direction']:<5} | 价格: {row['price']} | 目标权重: {row['target_weight'] * 100:.1f}% | 原因: {row['reason']}")
        logger.info("-" * 70)

    # 3. 汇总并导出完整的流水日志
    output_path = "live_simulation_logs.csv"
    if all_strategy_ledgers:
        final_ledger_df = pd.concat(all_strategy_ledgers, ignore_index=True)
        # 依然保留导出 CSV 的动作，方便本地查阅
        final_ledger_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"\n✅ 所有策略的全量交易流水(Ledger)已合并生成: {output_path} (共 {len(final_ledger_df)} 条记录)")

        # 修改点：直接返回 DataFrame 对象
        return final_ledger_df
    else:
        logger.info("\n► 历史流转中所有策略均未产生任何交易信号。")
        # 修改点：当没有交易信号时，返回空的 DataFrame 而不是空字符串
        return pd.DataFrame()

# ==========================================
# 4. 程序入口点
# ==========================================
def execute_trading_bot_workflow(target_time, proxy_url=None):
    """
    拉取数据并启动整套交易工作流
    返回最终生成的信号文件内容
    """
    fetched_raw_data = []

    # 2. 支持多组参数批量运行的列表格式
    strategy_params_list = [
        {
            'STRATEGY_NAME': 'Grid_No.43629',
            'MOM_WINDOW': 48,
            'VOL_WINDOW': 42,
            'BTC_TREND_WINDOW': 120,
            'MAX_WEIGHT': 0.5,
            'TOP_K': 1,
            'TIME_OFFSET': '2h',
            'TRADE_MODE': 'LONG_ONLY'
        },
        {
            'STRATEGY_NAME': 'Grid_No.69393',
            'MOM_WINDOW': 90,
            'VOL_WINDOW': 120,
            'BTC_TREND_WINDOW': 720,
            'MAX_WEIGHT': 0.05,
            'TOP_K': 3,
            'TIME_OFFSET': '0h',
            'TRADE_MODE': 'SHORT_ONLY'
        }
    ]

    # 3. 动态计算 lookback_days：最大所需时间(Max 4H Bar Window)转换成天数，再加上 1 天
    # 1个 4H bar 等于 4 小时，一天有 6 个 4H bar (24/4 = 6)
    max_window = 0
    for params in strategy_params_list:
        current_max = max(params['MOM_WINDOW'], params['VOL_WINDOW'], params['BTC_TREND_WINDOW'])
        if current_max > max_window:
            max_window = current_max

    lookback_days = int(np.ceil(max_window / 6)) + 30

    run_logger = setup_logger()
    run_logger.info(f"📊 基于最大策略指标窗口({max_window} bars)，动态计算所需历史预热数据天数: {lookback_days} 天。")

    symbol_list = [
        "BTC/USDC:USDC", "ETH/USDC:USDC", "SOL/USDC:USDC",
        "XRP/USDC:USDC", "BNB/USDC:USDC", "DOGE/USDC:USDC"
    ]

    timeframe = "1m"
    # 【修改点 1】一次性调用高并发极速双擎获取全部币种数据
    result_map = snipe_kline_data(
        symbol_list=symbol_list,
        timeframe=timeframe,
        days=lookback_days,
        target_time_str=target_time,
        use_ws=True,
        use_rest=True,
        proxy_url=proxy_url
    )
    run_logger.info(f"✅ 已完成对所有币种的极速引擎数据请求，正在进行数据完整性检查和预处理...")
    # 1分钟周期的理论预期总行数：天数 * 24小时 * 60分钟 + 1根
    expected_rows = lookback_days * 24 * 60 + 1

    for symbol in symbol_list:
        # 从极速引擎返回的字典中安全提取对应币种的数据
        df_klines = result_map.get(symbol, pd.DataFrame())

        # 【修改点 2】检查数据缺失并输出告警日志
        if df_klines.empty:
            run_logger.warning(f"❌ 警告：{symbol} 数据完全丢失！缺失 {expected_rows} 条数据。")
            continue

        actual_rows = len(df_klines)
        if actual_rows < expected_rows:
            # 提取已拿到数据的实际时间跨度
            start_time_str = df_klines['datetime_bj'].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
            end_time_str = df_klines['datetime_bj'].iloc[-1].strftime('%Y-%m-%d %H:%M:%S')
            missing_count = expected_rows - actual_rows

            run_logger.warning(
                f"⚠️ 数据缺失告警：{symbol} | "
                f"缺失量: {missing_count} 条 (预期 {expected_rows}, 实际 {actual_rows}) | "
                f"可用数据区间: [{start_time_str} 至 {end_time_str}]"
            )

        # 提取纯币种名如 'BTC'
        coin_name = symbol.split('/')[0]
        df_klines['coin_name'] = coin_name
        # [新增] 将完整的原始 symbol 存入 dataframe 中，向下游无损传递符号元数据
        df_klines['symbol'] = symbol

        fetched_raw_data.append(df_klines)

    if not fetched_raw_data:
        run_logger.error("❌ 错误：没有任何数据被成功加载，程序退出。请检查网络或 fetch_binance_futures_klines 模块。")
        return ""
    else:
        run_logger.info(f"\n🚀 数据加载完毕，共 {len(fetched_raw_data)} 个标的。")
        run_logger.info("═" * 70)
        # 执行实盘流传并捕获信号文件内容进行返回
        signal_file_content = run_live_pipeline(fetched_raw_data, strategy_params_list, run_logger)
        return signal_file_content


if __name__ == "__main__":
    target_time = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")

    execute_trading_bot_workflow(target_time)