"""
[功能摘要]
本模块用于量化网格交易的标的筛选与评级，通过结合历史波动率特征与实时价格跌幅，动态计算各加密资产的网格交易性价比。

[输入数据]
1. 历史数据：本地 CSV 格式的分钟级 K 线数据（包含 open_time, open, high, low, close）。
2. 保证金配置：JSON 格式的键值对，映射不同回撤深度（%）所对应的理论所需保证金。
3. 实时数据：通过 Binance API 获取的最新盘口价格。

[数据流转/交互]
1. 预处理与缓存：全量加载 CSV 历史数据并标准化时间轴与数据类型，驻留内存以消除重复 I/O。
2. 静态特征提取：纵向重采样计算多周期（5min~24h）的归一化真实波幅（NTR）日均得分；通过BTC参照收盘价对比计算各币种兑BTC的最大比例特征。
3. 理论底线推演：以 BTC 46000 结合最大的比例来反推当前币的新理论底价。
4. 动态估值（实时交互）：拉取各币种实时现价，计算当前距离新理论底部的回撤比例，向下兼容 BTC 实时跌幅作为底线要求，查表获取所需保证金，求得最终性价比分数。

[输出数据]
输出并持久化一份包含静态统计特征、理论极值边界、实时价格及最终评级分数的综合排序 DataFrame（导出为 CSV）。
"""

import os

import numpy as np
import pandas as pd
import requests

from app.signal_trade_lite.common_utils_lite import setup_logger
from common.common_utils import read_json, save_json

logger = setup_logger(app_name="grid_optimizer")



def _prepare_dataframe(df):
    """
    What & Why:
    标准化历史 K 线数据。将时间戳转化为 DatetimeIndex，并将 OHLC 列强制转为浮点型。
    这是所有周期重采样和极值计算的基石，确保后续算子的输入绝对纯净与幂等。
    """
    data = df.copy()

    if not isinstance(data.index, pd.DatetimeIndex):
        if 'open_time' not in data.columns:
            raise ValueError("DataFrame 缺失 'open_time' 列")

        if not pd.api.types.is_datetime64_any_dtype(data['open_time']):
            data['open_time'] = pd.to_datetime(pd.to_numeric(data['open_time']), unit='ms')
        data.set_index('open_time', inplace=True)

    if not data.index.is_monotonic_increasing:
        data.sort_index(inplace=True)

    ohlc = ['open', 'high', 'low', 'close']
    for col in ohlc:
        if col in data.columns and data[col].dtype != float:
            data[col] = data[col].astype(float)

    return data


def calculate_grid_score(df, resample_rule='15min'):
    """
    What & Why:
    计算单标的在指定周期下的网格收益能力。
    通过重采样过滤高频毛刺，计算归一化真实波幅 (NTR)。仅统计近 1 年数据以反映近期活跃度。
    """
    data = _prepare_dataframe(df)

    if not data.empty:
        one_year_ago = data.index.max() - pd.Timedelta(days=365)
        data = data[data.index >= one_year_ago]

    if len(data) < 2:
        return 0.0, 0.0

    resampled = data.resample(resample_rule).agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
    }).dropna()

    if len(resampled) < 2:
        return 0.0, 0.0

    resampled['prev_close'] = resampled['close'].shift(1)

    # 计算真实波幅 (True Range) 并归一化
    hl = resampled['high'] - resampled['low']
    hc = (resampled['high'] - resampled['prev_close']).abs()
    lc = (resampled['low'] - resampled['prev_close']).abs()

    tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
    resampled['ntr'] = tr / resampled['prev_close']

    valid_ntr = resampled.loc[resampled['ntr'] > 0, 'ntr']
    median_ntr_pct = (valid_ntr.median() * 100) if not valid_ntr.empty else 0.0

    time_span_days = (resampled.index.max() - resampled.index.min()).total_seconds() / 86400.0
    final_score = resampled['ntr'].sum() / time_span_days if time_span_days > 0 else 0.0

    return final_score, median_ntr_pct


def _extract_coin_features(coin_name, df, periods, btc_ref_close=None, ratio_resample_rule='1D',
                           ratio_window_days=365):
    """
    What & Why:
    解耦提取单个币种所有静态特征的逻辑。
    计算与 BTC 的历史比率极值及各周期网格得分（已剔除与新分数无关的自身最大回撤逻辑）。
    """
    result = {'币种': coin_name}

    max_ratio = None
    max_ratio_time = None
    btc_price_at_max_ratio = None
    coin_price_at_max_ratio = None

    try:
        if btc_ref_close is not None and coin_name.upper() not in ['BTC', 'BTCUSDT']:
            # 当前币种收盘价按传入的周期参数进行重采样
            coin_ref_close = df['close'].resample(ratio_resample_rule).last().dropna()
            # 获取具有重叠时间的历史数据交集
            common_idx = btc_ref_close.index.intersection(coin_ref_close.index)

            if not common_idx.empty:
                # 仅保留指定时间窗口（天数）内的数据
                time_window_start = common_idx.max() - pd.Timedelta(days=ratio_window_days)
                common_idx = common_idx[common_idx >= time_window_start]

                if not common_idx.empty:
                    btc_aligned = btc_ref_close.loc[common_idx]
                    coin_aligned = coin_ref_close.loc[common_idx]

                    # 计算比例： btc价格 / 当前币种价格
                    ratio_series = btc_aligned / coin_aligned
                    # 处理异常数值
                    ratio_series = ratio_series.replace([float('inf'), -float('inf')], pd.NA).dropna()

                    if not ratio_series.empty:
                        max_ratio = ratio_series.max()
                        max_ratio_time = ratio_series.idxmax()
                        btc_price_at_max_ratio = btc_aligned.loc[max_ratio_time]
                        coin_price_at_max_ratio = coin_aligned.loc[max_ratio_time]
        elif coin_name.upper() in ['BTC', 'BTCUSDT']:
            # 若是BTC自身，比例固定为1.0
            max_ratio = 1.0
    except Exception as e:
        logger.warning(f"[{coin_name}] 计算BTC收盘比例特征时发生异常: {e}")

    result.update({
        '相对BTC最大比例': max_ratio,
        '最大比例发生时间': max_ratio_time,
        '最大比例时BTC价格': btc_price_at_max_ratio,
        '最大比例时本币价格': coin_price_at_max_ratio
    })

    scores = []
    for period in periods:
        score, med_pct = calculate_grid_score(df, resample_rule=period)
        result[f'{period}_得分'] = score
        result[f'{period}_中位数(%)'] = med_pct
        scores.append(score)

    result['平均网格得分'] = sum(scores) / len(scores) if scores else 0.0
    return result


def generate_statistics(param_list, output_file="grid_statistics_result.csv", ratio_resample_rule='1min',
                        ratio_window_days=365):
    """
    What & Why:
    统筹全局静态数据的缓存、解析与跨标的换算。
    构建一次性内存缓存池 (data_cache) 消除重复 I/O，并建立 BTC 极值比例基准体系推导全市场新理论底部。
    """
    if os.path.exists(output_file):
        logger.info(f"统计文件 [{output_file}] 已存在，直接加载跳过重算。")
        return pd.read_csv(output_file)

    # 1. 内存级缓存加载，避免多遍读写 CSV
    data_cache = {}
    for param in param_list:
        file_path = param.get("csv_file_path")
        if not file_path: continue

        coin_name = os.path.basename(file_path).split('_')[0].upper()
        try:
            raw_df = pd.read_csv(file_path)

            if 'open_time' in raw_df.columns and not raw_df.empty:
                try:
                    max_ms = float(raw_df['open_time'].max())
                    if pd.notna(max_ms):
                        one_month_ago_ms = (pd.Timestamp.now('UTC') - pd.DateOffset(months=1)).timestamp() * 1000
                        if max_ms < one_month_ago_ms:
                            logger.info(f"[{coin_name}] 的最新数据不在最近一个月内，跳过处理该币种。")
                            continue
                except Exception as e:
                    logger.warning(f"[{coin_name}] 检查 ms 级别 open_time 时异常，继续处理: {e}")

            data_cache[coin_name] = _prepare_dataframe(raw_df)
        except Exception as e:
            logger.error(f"解析文件失败 {file_path}: {e}")

    if not data_cache:
        logger.warning("数据缓存池为空，请检查数据源配置。")
        return None

    # 2. 提取 BTC 参照收盘价供比率计算
    btc_ref_close = None
    btc_df = data_cache.get('BTC', data_cache.get('BTCUSDT'))
    if btc_df is not None and not btc_df.empty:
        btc_ref_close = btc_df['close'].resample(ratio_resample_rule).last().dropna()
        logger.info("成功锁定 BTC 用于对比的收盘价基准数据")
    else:
        logger.warning("未定位到 BTC 数据，比值极值推导将受限。")

    # 3. 遍历计算特征
    periods = ['1h', '2h', '4h', '8h', '12h', '24h']
    results = []

    for coin_name, df in data_cache.items():
        coin_result = _extract_coin_features(
            coin_name, df, periods,
            btc_ref_close=btc_ref_close,
            ratio_resample_rule=ratio_resample_rule,
            ratio_window_days=ratio_window_days
        )
        results.append(coin_result)
        logger.info(f"[{coin_name}] 静态特征提取完成")

    final_df = pd.DataFrame(results)

    # 4. 动态换算新理论最低价
    btc_row = final_df[final_df['币种'].isin(['BTC', 'BTCUSDT'])]
    if not btc_row.empty:
        final_df['新理论底价'] = final_df.apply(
            lambda row: (46000.0 / row['相对BTC最大比例']) if pd.notna(row['相对BTC最大比例']) and row['相对BTC最大比例'] > 0 else None,
            axis=1
        )
    else:
        logger.warning("未能建立 BTC 回撤换算基准。")
        final_df['新理论底价'] = None

    # 5. 格式化、排序并持久化
    final_df.sort_values(by='平均网格得分', ascending=False, inplace=True)
    final_df.reset_index(drop=True, inplace=True)

    final_df.to_csv(output_file, index=False)
    logger.info(f"静态评分已落地至: {output_file}")

    return final_df


def get_latest_price(symbol):
    """
    What & Why:
    对接 Binance 公开接口拉取实时盘口现价。
    """
    sym = symbol.upper()
    if not sym.endswith('USDT'):
        sym += 'USDT'

    url = f"https://api.binance.com/api/v3/ticker/price?symbol={sym}"
    proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}

    try:
        response = requests.get(url, proxies=proxies, timeout=5)
        if response.status_code == 200:
            return float(response.json()['price'])
        logger.warning(f"[{sym}] 报价拉取失败, 状态码: {response.status_code}")
    except Exception as e:
        logger.warning(f"[{sym}] 报价拉取异常: {e}")
    return None



def calculate_final_score(df, margin_info, up_pct_target=10):
    """
    What & Why:
    完全基于“新理论底价”的逻辑线，融合实时现价生成最终资金分配策略得分。
    去除了与新分数无关的所有旧日回撤及旧分数衍生字段，并保证字段名纯中文。
    """
    result_df = df.copy()
    latest_prices_dict = {}

    new_btc_drop_pct = None
    btc_price = None
    btc_new_score = None

    new_btc_drop_pct_at_target = None
    btc_up_target_new_score = None

    # 1. 批量预取现价，锁定 BTC 实时回撤底线
    for coin in result_df['币种']:
        latest_prices_dict[coin] = get_latest_price(coin)

    btc_row = result_df[result_df['币种'].isin(['BTC', 'BTCUSDT'])]
    if not btc_row.empty:
        btc_coin = btc_row.iloc[0]['币种']
        btc_price = latest_prices_dict.get(btc_coin)

        new_btc_theory_lowest = btc_row.iloc[0]['新理论底价']
        btc_avg_score = btc_row.iloc[0]['平均网格得分']

        if btc_price and btc_price > 0:
            btc_up_price = btc_price * (1 + up_pct_target / 100.0)

            if pd.notna(new_btc_theory_lowest):
                # 【当前状态】BTC现价跌幅与分数
                new_btc_drop_pct = max(0.0, (btc_price - new_btc_theory_lowest) / btc_price * 100)
                btc_new_margin_key = min(margin_info.keys(),
                                         key=lambda k: abs(k - new_btc_drop_pct)) if margin_info else 0.0
                btc_new_req_margin = margin_info.get(btc_new_margin_key, 0.0)
                if btc_new_req_margin > 0:
                    btc_new_score = (btc_avg_score / btc_new_req_margin * 10000)

                # 【未来状态】BTC上涨 up_pct_target 后的跌幅与分数
                new_btc_drop_pct_at_target = max(0.0, (btc_up_price - new_btc_theory_lowest) / btc_up_price * 100)
                btc_up_margin_key = min(margin_info.keys(),
                                        key=lambda k: abs(k - new_btc_drop_pct_at_target)) if margin_info else 0.0
                btc_up_req_margin = margin_info.get(btc_up_margin_key, 0.0)
                if btc_up_req_margin > 0:
                    btc_up_target_new_score = (btc_avg_score / btc_up_req_margin * 10000)

    # 2. 动态计分评估（仅保留新分数逻辑线核心指标）
    metrics = {
        '最新价格': [],
        '距新理论底价跌幅(%)': [],
        '新所需保证金': [],
        '新最终分数': [],
        '基于当前BTC的理论价': [],
        '现价偏离理论价(%)': [],
        '对标BTC当前分数所需涨跌幅(%)': [],
        '对标BTC当前分数所需目标价': [],
        f'上涨{up_pct_target}%目标价': [],
        f'上涨{up_pct_target}%后新分数': [],
        f'对标BTC上涨{up_pct_target}%分数所需涨跌幅(%)': [],
        f'对标BTC上涨{up_pct_target}%分数所需目标价': []
    }

    for _, row in result_df.iterrows():
        coin = row['币种']
        avg_score = row['平均网格得分']
        new_theory_lowest = row['新理论底价']
        max_ratio = row.get('相对BTC最大比例', None)

        price = latest_prices_dict.get(coin)
        metrics['最新价格'].append(price)

        # 计算上涨 up_pct_target 后的价格及其分数
        up_price = None
        up_target_score = None

        if price and price > 0:
            up_price = price * (1 + up_pct_target / 100.0)
            if pd.notna(new_theory_lowest):
                drop_at_up = max(0.0, (up_price - new_theory_lowest) / up_price * 100)
                if new_btc_drop_pct_at_target is not None and coin.upper() not in ['BTC', 'BTCUSDT']:
                    drop_at_up = max(drop_at_up, new_btc_drop_pct_at_target)

                up_req_margin_key = min(margin_info.keys(), key=lambda k: abs(k - drop_at_up)) if margin_info else 0.0
                up_req_margin = margin_info.get(up_req_margin_key, 0.0)

                if up_req_margin > 0:
                    up_target_score = (avg_score / up_req_margin * 10000)

        metrics[f'上涨{up_pct_target}%目标价'].append(up_price)
        metrics[f'上涨{up_pct_target}%后新分数'].append(up_target_score)

        # 基于 新理论底价 计算的核心分数指标
        if price and pd.notna(new_theory_lowest) and price > 0:
            new_drop_pct = max(0.0, (price - new_theory_lowest) / price * 100)
            if new_btc_drop_pct is not None and coin.upper() not in ['BTC', 'BTCUSDT']:
                new_drop_pct = max(new_drop_pct, new_btc_drop_pct)
            metrics['距新理论底价跌幅(%)'].append(new_drop_pct)

            new_req_margin = min(margin_info.keys(), key=lambda k: abs(k - new_drop_pct)) if margin_info else 0.0
            new_req_margin = margin_info.get(new_req_margin, 0.0)
            metrics['新所需保证金'].append(new_req_margin)

            new_f_score = (avg_score / new_req_margin * 10000) if new_req_margin > 0 else 0.0
            metrics['新最终分数'].append(new_f_score)
        else:
            metrics['距新理论底价跌幅(%)'].append(None)
            metrics['新所需保证金'].append(None)
            metrics['新最终分数'].append(None)

        # 基于当前BTC最新价的反推理论价及偏差
        ltp = None
        dev_pct = None

        if btc_price and pd.notna(max_ratio) and max_ratio > 0:
            ltp = btc_price / max_ratio
            if price and pd.notna(ltp) and ltp > 0:
                dev_pct = (price - ltp) / ltp * 100

        metrics['基于当前BTC的理论价'].append(ltp)
        metrics['现价偏离理论价(%)'].append(dev_pct)

        # 反推同等 BTC 当前分数所需的理论涨跌幅及具体价格
        req_pct = None
        req_target_price = None

        if btc_new_score is not None and btc_new_score > 0 and price and price > 0 and pd.notna(
                new_theory_lowest) and margin_info:
            if coin.upper() in ['BTC', 'BTCUSDT']:
                req_pct = 0.0
                req_target_price = price
            else:
                target_margin = (avg_score / btc_new_score) * 10000
                best_k = min(margin_info.keys(), key=lambda k: abs(margin_info[k] - target_margin))

                if best_k < 100:
                    p_target = new_theory_lowest / (1 - best_k / 100.0)
                    req_target_price = p_target
                    req_pct = (p_target - price) / price * 100

        metrics['对标BTC当前分数所需涨跌幅(%)'].append(req_pct)
        metrics['对标BTC当前分数所需目标价'].append(req_target_price)

        # 反推同等 BTC 上涨后分数所需的理论涨跌幅及具体价格
        req_up_pct = None
        req_up_target_price = None

        if btc_up_target_new_score is not None and btc_up_target_new_score > 0 and price and price > 0 and pd.notna(
                new_theory_lowest) and margin_info:
            if coin.upper() in ['BTC', 'BTCUSDT']:
                req_up_pct = up_pct_target
                req_up_target_price = up_price
            else:
                target_margin_up = (avg_score / btc_up_target_new_score) * 10000
                best_k_up = min(margin_info.keys(), key=lambda k: abs(margin_info[k] - target_margin_up))

                if best_k_up < 100:
                    p_target_up = new_theory_lowest / (1 - best_k_up / 100.0)
                    req_up_target_price = p_target_up
                    req_up_pct = (p_target_up - price) / price * 100

        metrics[f'对标BTC上涨{up_pct_target}%分数所需涨跌幅(%)'].append(req_up_pct)
        metrics[f'对标BTC上涨{up_pct_target}%分数所需目标价'].append(req_up_target_price)

    # 3. 数据融合与输出整理
    for key, val_list in metrics.items():
        result_df[key] = val_list

    # 直接使用纯洁版的新最终分数进行降序排序
    result_df.sort_values(by='新最终分数', ascending=False, inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    return result_df

def optimize_grid_interval(df, step_pct=0.1, min_pct=0.2, max_pct=3.0, fee_pct=0.05):
    """
    (未涉及部分保持不变，由于该功能与新分数无关，已从上层主干流程中剥离调用关联)
    寻找最优网格间距 (支持多种时间格式与DatetimeIndex自适应，并加入全链路日志)
    """
    logger.info(f"开始进行网格间距寻优 | 区间: {min_pct}% ~ {max_pct}% | 步长: {step_pct}%")
    original_len = len(df)

    try:
        if isinstance(df.index, pd.DatetimeIndex):
            max_time = df.index.max()
            cutoff_time = max_time - pd.Timedelta(days=365)
            df = df[df.index >= cutoff_time].copy()
        elif 'open_time' in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df['open_time']):
                max_time = df['open_time'].max()
                cutoff_time = max_time - pd.Timedelta(days=365)
                df = df[df['open_time'] >= cutoff_time].copy()
            else:
                max_time = df['open_time'].max()
                if max_time > 1e12:
                    offset = 365 * 24 * 60 * 60 * 1000
                else:
                    offset = 365 * 24 * 60 * 60
                cutoff_time = max_time - offset
                df = df[df['open_time'] >= cutoff_time].copy()
        else:
            logger.warning("未检测到有效的时间轴，将使用全量数据进行网格寻优。")

        filtered_len = len(df)
        if filtered_len < original_len:
            logger.info(f"时间范围截取完成: 截取最近 365 天数据 | 记录数: {original_len} -> {filtered_len}")

    except Exception as e:
        logger.error(f"时间过滤逻辑发生异常，将使用全量数据进行寻优: {e}")

    if df.empty:
        logger.warning("截取最近365天后，数据量为空，退出寻优。")
        return pd.DataFrame()

    cols = ['open', 'high', 'low', 'close']
    for col in cols:
        if df[col].dtype != np.float64:
            df[col] = df[col].astype(float)

    step = step_pct / 100.0
    fee = fee_pct / 100.0
    min_interval = min_pct / 100.0
    max_interval = max_pct / 100.0

    max_price = df['high'].max()

    num_steps = int(round((max_interval - min_interval) / step)) + 1
    intervals = [round(min_interval + i * step, 5) for i in range(num_steps)]

    o_vals = df['open'].values
    h_vals = df['high'].values
    l_vals = df['low'].values
    c_vals = df['close'].values

    is_bull = c_vals > o_vals
    p1 = np.where(is_bull, l_vals, h_vals)
    p2 = np.where(is_bull, h_vals, l_vals)
    p3 = c_vals

    n = len(df)
    all_prices = np.empty(n * 3 + 1, dtype=np.float64)
    all_prices[0] = o_vals[0]
    all_prices[1::3] = p1
    all_prices[2::3] = p2
    all_prices[3::3] = p3

    results = []

    for interval in intervals:
        spacing_abs = max_price * interval
        if spacing_abs == 0:
            continue

        zones = np.floor((max_price - all_prices) / spacing_abs).astype(np.int32)
        diffs = np.diff(zones)
        change_idx = np.where(diffs != 0)[0]

        if len(change_idx) > 0:
            lines_crossed = np.maximum(zones[change_idx], zones[change_idx + 1])
            valid_mask = np.concatenate(([True], np.diff(lines_crossed) != 0))
            valid_lines = lines_crossed[valid_mask]

            trades = len(valid_lines) - 1
            trades = max(0, trades)
        else:
            trades = 0

        score = (interval - fee) * trades * interval

        results.append({
            'Grid_Interval_Pct': f"{interval * 100:.1f}%",
            'Interval_Float': interval,
            'Trades': int(trades),
            'Score': score
        })

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values(by='Score', ascending=False).reset_index(drop=True)
        best_cfg = result_df.iloc[0]
        logger.info(
            f"寻优完成 | 测试组合数: {len(intervals)} | 最优间距: {best_cfg['Grid_Interval_Pct']} | 模拟成交: {best_cfg['Trades']} 次 | 得分: {best_cfg['Score']:.4f}")
    else:
        logger.warning("未能计算出任何有效的网格间距。")

    return result_df

if __name__ == "__main__":
    temp_path = "test.json"
    read_json(temp_path)
    save_json(temp_path, {"test": "test"})

    # 1. 抽取交易对列表
    symbols_list = [
        "AAVEUSDT", "AVAXUSDT", "BNBUSDT", "BTCUSDT", "DOGEUSDT", "ETHUSDT",
        "GMXUSDT", "JUPUSDT", "KASUSDT", "LDOUSDT", "LINKUSDT", "NEARUSDT",
        "ONDOUSDT", "PENDLEUSDT", "PYTHUSDT", "RENDERUSDT", "RUNEUSDT",
        "SKYUSDT", "SOLUSDT", "STXUSDT", "TAOUSDT", "TRXUSDT", "UNIUSDT"
    ]

    # 2. 提取公共的目录路径和文件后缀
    base_dir = r"W:\project\python_project\oke_auto_trade\kline_data"
    suffix = "_1m_2021-01-01_merged.csv"

    # 3. 使用列表推导式优雅拼接
    param_list = [
        {"csv_file_path": rf"{base_dir}\{symbol}{suffix}"}
        for symbol in symbols_list
    ]
    raw_margin_info = read_json("margin_info.json")
    margin_info = {float(k): v for k, v in raw_margin_info.items()} if raw_margin_info else {}

    logger.info("=== 启动静态波动率统计分析 ===")
    final_df = generate_statistics(param_list, output_file="grid_statistics_result.csv")

    if final_df is not None:
        logger.info("=== 启动动态实时报价与计分计算 ===")
        # 直接围绕新最终分数体系生成结果
        final_df = calculate_final_score(final_df, margin_info)
        logger.info("流程全量执行完毕。")