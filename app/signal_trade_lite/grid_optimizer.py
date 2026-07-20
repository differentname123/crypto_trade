"""
[功能摘要]
本模块用于量化网格交易的标的筛选与评级，通过结合历史波动率特征与实时价格跌幅，动态计算各加密资产的网格交易性价比。

[输入数据]
1. 历史数据：本地 CSV 格式的分钟级 K 线数据（包含 open_time, open, high, low, close）。
2. 保证金配置：JSON 格式的键值对，映射不同回撤深度（%）所对应的理论所需保证金。
3. 实时数据：通过 Binance API 获取的最新盘口价格。

[数据流转/交互]
1. 预处理与缓存：全量加载 CSV 历史数据并标准化时间轴与数据类型，驻留内存以消除重复 I/O。
2. 静态特征提取：以 BTC 历史最高点的时间为全市场对齐锚点，横向计算所有标的的最大回撤边界；纵向重采样计算多周期（5min~24h）的归一化真实波幅（NTR）日均得分。
3. 理论底线推演：以 BTC 的历史最大回撤为标尺，通过比例映射推导出各山寨币的“理论最低价”。
4. 动态估值（实时交互）：拉取各币种实时现价，计算当前距离理论底部的回撤比例，向下兼容 BTC 实时跌幅作为底线要求，查表获取所需保证金，最终求得 (日均得分/保证金) 的动态性价比分数。

[输出数据]
输出并持久化一份包含静态统计特征、理论极值边界、实时价格及最终评级分数的综合排序 DataFrame（导出为 CSV），供后续自动交易或人工决策使用。
"""

import os
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


def _extract_coin_features(coin_name, df, btc_max_price_time, periods, btc_ref_close=None, ratio_resample_rule='1D',
                           ratio_window_days=365):
    """
    What & Why:
    解耦提取单个币种所有静态特征的逻辑。
    对齐 BTC 峰值时间寻找山寨币同期极值，并计算历史最大回撤及各周期网格得分。
    （新增：接收BTC参照收盘价对比计算各币种兑BTC的最大比例特征，周期与时间窗口参数化控制）
    """
    result = {'Coin': coin_name}

    try:
        high_col, low_col = df['high'], df['low']

        # 确定极值点：若非 BTC，则在 BTC 峰值前后1个月窗口内寻找共振最高点
        if btc_max_price_time is not None and coin_name.upper() not in ['BTC', 'BTCUSDT']:
            window_start = btc_max_price_time - pd.DateOffset(months=1)
            window_end = btc_max_price_time + pd.DateOffset(months=1)
            window_high = high_col.loc[window_start:window_end]

            max_price_series = window_high if not window_high.empty else high_col
        else:
            max_price_series = high_col

        max_price = max_price_series.max()
        max_price_time = max_price_series.idxmax()

        # 计算历史最大回撤及起止点
        cum_max = high_col.cummax()
        drawdowns = (low_col - cum_max) / cum_max
        max_dd_end_time = drawdowns.idxmin()
        max_dd_pct = drawdowns.min() * 100

        max_dd_end_price = low_col.loc[max_dd_end_time]
        if isinstance(max_dd_end_price, pd.Series):
            max_dd_end_price = max_dd_end_price.iloc[0]

        pre_dd_high = high_col.loc[:max_dd_end_time]
        max_dd_start_time = pre_dd_high.idxmax()
        max_dd_start_price = pre_dd_high.loc[max_dd_start_time]
        if isinstance(max_dd_start_price, pd.Series):
            max_dd_start_price = max_dd_start_price.iloc[0]

        result.update({
            'Max_Price': max_price, 'Max_Price_Time': max_price_time, 'Max_DD(%)': max_dd_pct,
            'Max_DD_Start_Time': max_dd_start_time, 'Max_DD_Start_Price': max_dd_start_price,
            'Max_DD_End_Time': max_dd_end_time, 'Max_DD_End_Price': max_dd_end_price
        })

    except Exception as e:
        logger.warning(f"[{coin_name}] 极值和回撤统计失败: {e}")
        result.update({
            'Max_Price': None, 'Max_Price_Time': None, 'Max_DD(%)': None,
            'Max_DD_Start_Time': None, 'Max_DD_Start_Price': None,
            'Max_DD_End_Time': None, 'Max_DD_End_Price': None
        })

    # ================= 新增逻辑：计算与 BTC 指定周期的参照收盘价最大比例等特征 =================
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
        'Max_Ratio_vs_BTC': max_ratio,
        'Max_Ratio_Time': max_ratio_time,
        'BTC_Price_At_Max_Ratio': btc_price_at_max_ratio,
        'Coin_Price_At_Max_Ratio': coin_price_at_max_ratio
    })
    # =================================================================================

    scores = []
    for period in periods:
        score, med_pct = calculate_grid_score(df, resample_rule=period)
        result[f'{period}_Score'] = score
        result[f'{period}_Med(%)'] = med_pct
        scores.append(score)

    result['Avg_Score'] = sum(scores) / len(scores) if scores else 0.0
    return result


def generate_statistics(param_list, output_file="grid_statistics_result.csv", ratio_resample_rule='1min',
                        ratio_window_days=365):
    """
    What & Why:
    统筹全局静态数据的缓存、解析与跨标的换算。
    构建一次性内存缓存池 (data_cache) 消除重复 I/O，并建立 BTC 回撤基准体系推导全市场理论底部。
    (提供 ratio_resample_rule 和 ratio_window_days 暴露给外部调用，以实现对提取计算的参数化管控)
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
            data_cache[coin_name] = _prepare_dataframe(raw_df)
        except Exception as e:
            logger.error(f"解析文件失败 {file_path}: {e}")

    if not data_cache:
        logger.warning("数据缓存池为空，请检查数据源配置。")
        return None

    # 2. 提取全市场共振时间锚点 (BTC 峰值) 以及 BTC 参照收盘价供比率计算
    btc_max_price_time = None
    btc_ref_close = None
    btc_df = data_cache.get('BTC', data_cache.get('BTCUSDT'))
    if btc_df is not None and not btc_df.empty:
        btc_max_price_time = btc_df['high'].idxmax()
        # 提取BTC指定周期的收盘价，用于透传到各标的特征提取中
        btc_ref_close = btc_df['close'].resample(ratio_resample_rule).last().dropna()
        logger.info(f"成功锁定 BTC 峰值时间锚点: {btc_max_price_time}")
    else:
        logger.warning("未定位到 BTC 数据，各标的将独立寻找历史极值。")

    # 3. 遍历计算特征
    periods = ['1h', '2h', '4h', '8h', '12h', '24h']
    results = []

    for coin_name, df in data_cache.items():
        # 透传参数
        coin_result = _extract_coin_features(
            coin_name, df, btc_max_price_time, periods,
            btc_ref_close=btc_ref_close,
            ratio_resample_rule=ratio_resample_rule,
            ratio_window_days=ratio_window_days
        )
        results.append(coin_result)
        logger.info(f"[{coin_name}] 静态特征提取完成")

    final_df = pd.DataFrame(results)

    # 4. 动态换算理论回撤与最低价 (依赖 BTC 基准)
    btc_row = final_df[final_df['Coin'].isin(['BTC', 'BTCUSDT'])]
    if not btc_row.empty and pd.notna(btc_row.iloc[0]['Max_Price']) and btc_row.iloc[0]['Max_DD(%)'] != 0:
        btc_max_price = btc_row.iloc[0]['Max_Price']
        btc_max_dd_pct = btc_row.iloc[0]['Max_DD(%)']
        btc_theory_lowest = 46000.0
        btc_theory_dd_pct = (btc_theory_lowest - btc_max_price) / btc_max_price * 100

        dd_ratio = final_df['Max_DD(%)'] / btc_max_dd_pct
        final_df['Theory_DD(%)'] = dd_ratio * btc_theory_dd_pct
        final_df['Theory_Lowest_Price'] = final_df['Max_Price'] * (1 + final_df['Theory_DD(%)'] / 100)

        # 新增计算新的一种理论最低价格：以 BTC 46000 结合最大的比例来反推当前币的理论底价
        final_df['New_Theory_Lowest_Price'] = final_df.apply(
            lambda row: (46000.0 / row['Max_Ratio_vs_BTC']) if pd.notna(row['Max_Ratio_vs_BTC']) and row[
                'Max_Ratio_vs_BTC'] > 0 else None,
            axis=1
        )
    else:
        logger.warning("未能建立 BTC 回撤换算基准。")
        final_df['Theory_DD(%)'] = None
        final_df['Theory_Lowest_Price'] = None
        final_df['New_Theory_Lowest_Price'] = None

    # 5. 格式化、排序并持久化
    final_df.sort_values(by='Avg_Score', ascending=False, inplace=True)
    final_df.reset_index(drop=True, inplace=True)

    ordered_columns = ['Coin']
    for p in periods:
        ordered_columns.extend([f'{p}_Score', f'{p}_Med(%)'])

    # 将所有的新增统计列也补充到最终输出结果的列序中
    ordered_columns.extend([
        'Avg_Score', 'Max_Price', 'Max_Price_Time', 'Max_DD(%)', 'Theory_DD(%)', 'Theory_Lowest_Price',
        'Max_DD_Start_Time', 'Max_DD_Start_Price', 'Max_DD_End_Time', 'Max_DD_End_Price',
        'Max_Ratio_vs_BTC', 'Max_Ratio_Time', 'BTC_Price_At_Max_Ratio', 'Coin_Price_At_Max_Ratio',
        'New_Theory_Lowest_Price'
    ])

    final_df = final_df[[col for col in ordered_columns if col in final_df.columns]]
    final_df.to_csv(output_file, index=False)
    logger.info(f"静态评分已落地至: {output_file}")

    return final_df


def get_latest_price(symbol):
    """
    What & Why:
    对接 Binance 公开接口拉取实时盘口现价。
    保持原有代理策略穿透网络限制，为动态评分提供当前时点的数据支撑。
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


def calculate_final_score(df, margin_info):
    """
    What & Why:
    融合实时现价生成最终资金分配策略得分。
    通过比对当前价格与理论底价计算实时跌幅，映射保证金表，折算投入产出比。
    采用“不破坏原有 DataFrame”设计模式。
    （新增1：关于 New_Theory_Lowest_Price 的等效回撤、保证金与评分字段）
    （新增2：基于BTC最新价推导出的理论价，以及现价偏离该理论价的百分比）
    （新增3：基于当前最新价计算上涨20%的价格）
    """
    result_df = df.copy()
    latest_prices_dict = {}

    btc_drop_pct = None
    new_btc_drop_pct = None
    btc_price = None  # 将 btc_price 提出来供全局计算使用

    # 1. 批量预取现价，锁定 BTC 实时回撤底线
    for coin in result_df['Coin']:
        latest_prices_dict[coin] = get_latest_price(coin)

    btc_row = result_df[result_df['Coin'].isin(['BTC', 'BTCUSDT'])]
    if not btc_row.empty:
        btc_coin = btc_row.iloc[0]['Coin']
        btc_price = latest_prices_dict.get(btc_coin)

        btc_theory_lowest = btc_row.iloc[0]['Theory_Lowest_Price']
        new_btc_theory_lowest = btc_row.iloc[0]['New_Theory_Lowest_Price']

        if btc_price and btc_price > 0:
            # 原有逻辑：BTC基于旧理论价格的跌幅
            if pd.notna(btc_theory_lowest):
                btc_drop_pct = max(0.0, (btc_price - btc_theory_lowest) / btc_price * 100)

            # 之前新增：BTC基于新理论价格的跌幅
            if pd.notna(new_btc_theory_lowest):
                new_btc_drop_pct = max(0.0, (btc_price - new_btc_theory_lowest) / btc_price * 100)

    # 2. 动态计分评估 (扩充了字典以存储新分数及计算字段)
    metrics = {
        'price': [],
        'drop_pct': [], 'margin': [], 'score': [],
        'new_drop_pct': [], 'new_margin': [], 'new_score': [],
        'latest_theory_price': [], 'deviation_pct': [],
        'up_20pct_price': []  # [最新增量字段3]
    }

    for _, row in result_df.iterrows():
        coin = row['Coin']
        avg_score = row['Avg_Score']
        theory_lowest = row['Theory_Lowest_Price']
        new_theory_lowest = row['New_Theory_Lowest_Price']
        max_ratio = row.get('Max_Ratio_vs_BTC', None)  # 获取历史计算的最大比例

        price = latest_prices_dict.get(coin)
        metrics['price'].append(price)

        # -------------------- [新增逻辑3] 计算上涨20%的价格 --------------------
        if price and price > 0:
            metrics['up_20pct_price'].append(price * 1.20)
        else:
            metrics['up_20pct_price'].append(None)

        # ==================== [原有逻辑] 基于 Theory_Lowest_Price ====================
        if price and pd.notna(theory_lowest) and price > 0:
            drop_pct = max(0.0, (price - theory_lowest) / price * 100)

            # 严格约束：山寨币距离理论低点的回撤跌幅不得小于 BTC 的现有跌幅
            if btc_drop_pct is not None and coin.upper() not in ['BTC', 'BTCUSDT']:
                drop_pct = max(drop_pct, btc_drop_pct)
            metrics['drop_pct'].append(drop_pct)

            # 就近匹配保证金档位
            req_margin = min(margin_info.keys(), key=lambda k: abs(k - drop_pct)) if margin_info else 0.0
            req_margin = margin_info.get(req_margin, 0.0)
            metrics['margin'].append(req_margin)

            f_score = (avg_score / req_margin * 10000) if req_margin > 0 else 0.0
            metrics['score'].append(f_score)
        else:
            metrics['drop_pct'].append(None)
            metrics['margin'].append(None)
            metrics['score'].append(None)

        # ==================== [新增逻辑1] 基于 New_Theory_Lowest_Price ====================
        if price and pd.notna(new_theory_lowest) and price > 0:
            new_drop_pct = max(0.0, (price - new_theory_lowest) / price * 100)

            # 新维度的严格约束：山寨币距离新理论低点的回撤不得小于 BTC 基于新理论的现有跌幅
            if new_btc_drop_pct is not None and coin.upper() not in ['BTC', 'BTCUSDT']:
                new_drop_pct = max(new_drop_pct, new_btc_drop_pct)
            metrics['new_drop_pct'].append(new_drop_pct)

            new_req_margin = min(margin_info.keys(), key=lambda k: abs(k - new_drop_pct)) if margin_info else 0.0
            new_req_margin = margin_info.get(new_req_margin, 0.0)
            metrics['new_margin'].append(new_req_margin)

            new_f_score = (avg_score / new_req_margin * 10000) if new_req_margin > 0 else 0.0
            metrics['new_score'].append(new_f_score)
        else:
            metrics['new_drop_pct'].append(None)
            metrics['new_margin'].append(None)
            metrics['new_score'].append(None)

        # ==================== [新增逻辑2] 基于当前BTC最新价的理论价及偏差 ====================
        ltp = None
        dev_pct = None

        # 只要目前拿到BTC价格，并且当前币种算出了最大比例
        if btc_price and pd.notna(max_ratio) and max_ratio > 0:
            ltp = btc_price / max_ratio  # 计算：基于BTC最新价的理论价格

            if price and pd.notna(ltp) and ltp > 0:
                # 计算偏差百分比： (现价 - 理论价) / 理论价 * 100
                dev_pct = (price - ltp) / ltp * 100

        metrics['latest_theory_price'].append(ltp)
        metrics['deviation_pct'].append(dev_pct)

    # 3. 数据融合与输出整理
    result_df['最新价格'] = metrics['price']

    # 填充原有字段
    result_df['到理论低价的回撤比例'] = metrics['drop_pct']
    result_df['所需资金'] = metrics['margin']
    result_df['最终分数'] = metrics['score']

    # 填充新增逻辑1字段
    result_df['到新理论低价的回撤比例'] = metrics['new_drop_pct']
    result_df['新所需资金'] = metrics['new_margin']
    result_df['新最终分数'] = metrics['new_score']

    # 填充新增逻辑2字段（理论价格与偏差）
    result_df['基于当前BTC的理论价格'] = metrics['latest_theory_price']
    result_df['现价偏离理论价(%)'] = metrics['deviation_pct']

    # 填充新增逻辑3字段（上涨20%目标价）
    result_df['上涨20%目标价'] = metrics['up_20pct_price']

    # 保持对原有排序逻辑不干扰，依旧采用旧的最终分数进行主要排序
    result_df.sort_values(by='最终分数', ascending=False, inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    return result_df

if __name__ == "__main__":
    temp_path = "test.json"
    read_json(temp_path)
    save_json(temp_path, {"test": "test"})

    param_list = [
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\ETHUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\SOLUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\BNBUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\DOGEUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\LINKUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\TRXUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\AAVEUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\TONUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\UNIUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\STXUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RENDERUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RUNEUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\PENDLEUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\KASUSDT_1m_2021-01-01_merged.csv"}
    ]

    raw_margin_info = read_json("margin_info.json")
    margin_info = {float(k): v for k, v in raw_margin_info.items()} if raw_margin_info else {}

    logger.info("=== 启动静态波动率统计分析 ===")
    final_df = generate_statistics(param_list, output_file="grid_statistics_result.csv")

    if final_df is not None:
        logger.info("=== 启动动态实时报价与计分计算 ===")
        # 新最终分数作为排序依据，选哪个看这个字段就行了
        final_df = calculate_final_score(final_df, margin_info)
        logger.info("流程全量执行完毕。")