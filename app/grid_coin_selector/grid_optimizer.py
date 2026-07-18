import os
import pandas as pd
import requests

from common.common_utils import read_json, save_json


def _prepare_dataframe(df):
    """
    内部辅助函数：对原始 DataFrame 进行幂等的标准化处理。
    解析时间戳为唯一递增的 DatetimeIndex，并将 OHLC 列转换为标准浮点型，避免后续重复计算。
    """
    data = df.copy()

    # 确保 open_time 转化为标准 DatetimeIndex
    if not isinstance(data.index, pd.DatetimeIndex):
        if 'open_time' in data.columns:
            if not pd.api.types.is_datetime64_any_dtype(data['open_time']):
                data['open_time'] = pd.to_datetime(pd.to_numeric(data['open_time']), unit='ms')
            data.set_index('open_time', inplace=True)
        else:
            raise ValueError("DataFrame 缺少 'open_time' 列且未建立 DatetimeIndex")

    if not data.index.is_monotonic_increasing:
        data.sort_index(inplace=True)

    # 统一转换价格列为浮点数
    cols = ['open', 'high', 'low', 'close']
    for col in cols:
        if col in data.columns and data[col].dtype != float:
            data[col] = data[col].astype(float)

    return data


def calculate_grid_score(df, resample_rule='15min'):
    """
    计算网格交易频率预期评分 (Score) 与 中位数波动率 (Median NTR)。
    通过重采样降低高频噪点，基于真实波幅 (TR) 计算归一化波动率。
    """
    # 标准化输入数据
    data = _prepare_dataframe(df)

    # 限制统计区间：仅计算最近一年以内的数据
    if not data.empty:
        max_time = data.index.max()
        one_year_ago = max_time - pd.Timedelta(days=365)
        data = data[data.index >= one_year_ago]

    if len(data) < 2:
        return 0.0, 0.0

    # 1. 降噪：重采样至目标周期
    resampled = data.resample(resample_rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).dropna()

    if len(resampled) < 2:
        return 0.0, 0.0

    # 2. 计算归一化真实波幅 (NTR)
    resampled['prev_close'] = resampled['close'].shift(1)

    high_to_low = resampled['high'] - resampled['low']
    high_to_pc = (resampled['high'] - resampled['prev_close']).abs()
    low_to_pc = (resampled['low'] - resampled['prev_close']).abs()

    tr = pd.concat([high_to_low, high_to_pc, low_to_pc], axis=1).max(axis=1)
    resampled['ntr'] = tr / resampled['prev_close']

    # 提取大于0的有效波动，规避停盘死水时间对中位数的干扰
    valid_ntr = resampled.loc[resampled['ntr'] > 0, 'ntr']
    median_ntr_pct = (valid_ntr.median() * 100) if not valid_ntr.empty else 0.0

    # 3. 结合时间跨度计算日均得分
    total_ntr = resampled['ntr'].sum()
    time_span = resampled.index.max() - resampled.index.min()
    total_days = time_span.total_seconds() / 86400.0

    if total_days <= 0:
        return 0.0, 0.0

    final_score = total_ntr / total_days
    return final_score, median_ntr_pct


def generate_statistics(param_list, output_file="grid_statistics_result.csv"):
    """
    批量生成并保存所有标的的周期统计报表。
    支持历史计算缓存加载，并基于 BTC 的相对位置动态校准各币种的理论极值与理论回撤。
    """
    if os.path.exists(output_file):
        print(f"[INFO] 统计文件 [{output_file}] 已存在，直接加载并跳过计算...")
        return pd.read_csv(output_file)

    # 1. 预先获取 BTC 的最高价发生时间作为全市场基准锚点
    btc_max_price_time = None
    for param in param_list:
        csv_file_path = param.get("csv_file_path")
        if not csv_file_path:
            continue
        try:
            coin_name = os.path.basename(csv_file_path).split('_')[0].upper()
            if coin_name in ['BTC', 'BTCUSDT']:
                temp_btc_df = pd.read_csv(csv_file_path)
                temp_btc_df = _prepare_dataframe(temp_btc_df)
                if not temp_btc_df.empty:
                    btc_max_price_time = temp_btc_df['high'].idxmax()
                break
        except Exception as e:
            print(f"[WARNING] 预获取 BTC 峰值时间失败: {e}")

    periods = ['5min', '15min', '30min', '1h', '2h', '4h', '8h', '12h', '24h']
    results = []

    # 2. 遍历各币种文件，提取波动率特征与历史极值 drawdown
    for param in param_list:
        csv_file_path = param.get("csv_file_path")
        if not csv_file_path:
            continue

        coin_name = os.path.basename(csv_file_path).split('_')[0]

        try:
            raw_df = pd.read_csv(csv_file_path)
            # 一次性标准化，规避多重子循环中的重复解析
            temp_df = _prepare_dataframe(raw_df)
        except Exception as e:
            print(f"[ERROR] 读取或解析文件失败 {csv_file_path}: {e}")
            continue

        coin_result = {'Coin': coin_name}

        try:
            high_col = temp_df['high']
            low_col = temp_df['low']

            # 确定各标的的最大价格及其发生时间 (非 BTC 标的采用 BTC 峰值前后1个月的双向窗口进行对齐)
            if btc_max_price_time is not None and coin_name.upper() not in ['BTC', 'BTCUSDT']:
                window_start = btc_max_price_time - pd.DateOffset(months=1)
                window_end = btc_max_price_time + pd.DateOffset(months=1)
                window_high = high_col.loc[window_start:window_end]

                if not window_high.empty:
                    max_price = window_high.max()
                    max_price_time = window_high.idxmax()
                else:
                    max_price = high_col.max()
                    max_price_time = high_col.idxmax()
            else:
                max_price = high_col.max()
                max_price_time = high_col.idxmax()

            # 计算历史最大回撤 (基于累计最高价)
            cum_max = high_col.cummax()
            drawdowns = (low_col - cum_max) / cum_max
            max_dd_pct = drawdowns.min() * 100

            max_dd_end_time = drawdowns.idxmin()
            max_dd_end_price = low_col.loc[max_dd_end_time]
            if isinstance(max_dd_end_price, pd.Series):
                max_dd_end_price = max_dd_end_price.iloc[0]

            # 寻找跌至谷底前经历的最高价格起点
            pre_dd_high = high_col.loc[:max_dd_end_time]
            max_dd_start_time = pre_dd_high.idxmax()
            max_dd_start_price = pre_dd_high.loc[max_dd_start_time]
            if isinstance(max_dd_start_price, pd.Series):
                max_dd_start_price = max_dd_start_price.iloc[0]

            coin_result.update({
                'Max_Price': max_price,
                'Max_Price_Time': max_price_time,
                'Max_DD(%)': max_dd_pct,
                'Max_DD_Start_Time': max_dd_start_time,
                'Max_DD_Start_Price': max_dd_start_price,
                'Max_DD_End_Time': max_dd_end_time,
                'Max_DD_End_Price': max_dd_end_price
            })
        except Exception as e:
            print(f"[WARNING] [{coin_name}] 极值和回撤统计计算失败: {e}")
            coin_result.update({
                'Max_Price': None, 'Max_Price_Time': None, 'Max_DD(%)': None,
                'Max_DD_Start_Time': None, 'Max_DD_Start_Price': None,
                'Max_DD_End_Time': None, 'Max_DD_End_Price': None
            })

        # 3. 快速计算多周期评分
        scores = []
        for period in periods:
            score, med_pct = calculate_grid_score(temp_df, resample_rule=period)
            coin_result[f'{period}_Score'] = score
            coin_result[f'{period}_Med(%)'] = med_pct
            scores.append(score)

        coin_result['Avg_Score'] = sum(scores) / len(scores) if scores else 0.0
        results.append(coin_result)
        print(f"[INFO] [{coin_name}] 周期特征提取完成")

    if not results:
        print("[WARNING] 未成功计算任何数据，请检查数据源配置")
        return None

    final_df = pd.DataFrame(results)

    # 4. 动态计算理论最低价与理论回撤比例 (以 BTC 历史回撤和设定的理论低点为标准基准)
    btc_row = final_df[final_df['Coin'].str.upper().isin(['BTC', 'BTCUSDT'])]
    if not btc_row.empty:
        btc_max_price = btc_row.iloc[0]['Max_Price']
        btc_max_dd_pct = btc_row.iloc[0]['Max_DD(%)']

        if pd.notna(btc_max_price) and pd.notna(btc_max_dd_pct) and btc_max_dd_pct != 0:
            btc_theory_lowest = 46000.0
            btc_theory_dd_pct = (btc_theory_lowest - btc_max_price) / btc_max_price * 100

            # 线性映射各币种回撤比例并反推理论安全边界价格
            dd_ratio = final_df['Max_DD(%)'] / btc_max_dd_pct
            final_df['Theory_DD(%)'] = dd_ratio * btc_theory_dd_pct
            final_df['Theory_Lowest_Price'] = final_df['Max_Price'] * (1 + final_df['Theory_DD(%)'] / 100)
        else:
            final_df['Theory_DD(%)'] = None
            final_df['Theory_Lowest_Price'] = None
    else:
        print("[WARNING] 未找到 BTC 基准数据，无法完成理论低价与理论回撤换算")
        final_df['Theory_DD(%)'] = None
        final_df['Theory_Lowest_Price'] = None

    # 5. 格式化输出：排序并按业务周期对齐各列
    final_df.sort_values(by='Avg_Score', ascending=False, inplace=True)
    final_df.reset_index(drop=True, inplace=True)

    ordered_columns = ['Coin']
    for p in periods:
        ordered_columns.extend([f'{p}_Score', f'{p}_Med(%)'])
    ordered_columns.append('Avg_Score')
    ordered_columns.extend([
        'Max_Price', 'Max_Price_Time', 'Max_DD(%)', 'Theory_DD(%)', 'Theory_Lowest_Price',
        'Max_DD_Start_Time', 'Max_DD_Start_Price', 'Max_DD_End_Time', 'Max_DD_End_Price'
    ])

    present_columns = [col for col in ordered_columns if col in final_df.columns]
    final_df = final_df[present_columns]

    try:
        final_df.to_csv(output_file, index=False)
        print(f"[INFO] 静态评分数据已成功保存至: {output_file}")
    except Exception as e:
        print(f"[ERROR] 写入本地 CSV 文件失败: {e}")

    return final_df


def get_latest_price(symbol):
    """
    通过 Binance 公开接口拉取最新的实时价格。
    配置本地代理（默认 127.0.0.1:7890）确保请求网络顺畅。
    """
    try:
        sym = symbol.upper()
        if not sym.endswith('USDT'):
            sym += 'USDT'

        url = f"https://api.binance.com/api/v3/ticker/price?symbol={sym}"
        proxies = {
            "http": "http://127.0.0.1:7890",
            "https": "http://127.0.0.1:7890"
        }

        response = requests.get(url, proxies=proxies, timeout=5)
        if response.status_code == 200:
            return float(response.json()['price'])
        else:
            print(f"[WARNING] 获取 {sym} 实时价失败，HTTP 状态码: {response.status_code}")
            return None
    except Exception as e:
        print(f"[WARNING] 获取 {symbol} 实时价遭遇异常: {e}")
        return None


def calculate_final_score(df, margin_info):
    """
    结合实时价格和阶梯保证金配置，动态评估标的的所需资金与最终性价比得分。
    本过程保持对原始统计 DataFrame 的无污染设计（不修改原对象）。
    """
    result_df = df.copy()

    # 1. 批量抓取最新价格并锁定 BTC 的实时回撤深度作为底线标尺
    latest_prices_dict = {}
    btc_drop_pct = None

    for _, row in result_df.iterrows():
        coin = row['Coin']
        price = get_latest_price(coin)
        latest_prices_dict[coin] = price

        if coin.upper() in ['BTC', 'BTCUSDT']:
            theory_lowest = row['Theory_Lowest_Price']
            if price is not None and pd.notna(theory_lowest) and price > 0:
                drop_pct = max(0.0, (price - theory_lowest) / price * 100)
                btc_drop_pct = drop_pct

    latest_prices = []
    drop_to_lowest_pcts = []
    required_margins = []
    final_scores = []

    # 2. 动态匹配保证金档位并折算最终性价比综合得分
    for _, row in result_df.iterrows():
        coin = row['Coin']
        theory_lowest = row['Theory_Lowest_Price']
        avg_score = row['Avg_Score']

        price = latest_prices_dict.get(coin)
        latest_prices.append(price)

        if price is not None and pd.notna(theory_lowest) and price > 0:
            drop_pct = max(0.0, (price - theory_lowest) / price * 100)

            # 约束条件：山寨币的理论回撤比例强制不低于 BTC 的实时回撤比例
            if btc_drop_pct is not None and coin.upper() not in ['BTC', 'BTCUSDT']:
                if drop_pct < btc_drop_pct:
                    drop_pct = btc_drop_pct

            drop_to_lowest_pcts.append(drop_pct)

            # 依据最近邻原则，在 margin_info 配置中定位对应的保证金需求
            if margin_info:
                closest_margin_key = min(margin_info.keys(), key=lambda k: abs(k - drop_pct))
                req_margin = margin_info[closest_margin_key]
            else:
                req_margin = 0.0

            required_margins.append(req_margin)

            # 性价比动态评分 = (日均平均得分 / 所需保证金) * 放大系数
            f_score = (avg_score / req_margin * 10000) if req_margin > 0 else 0.0
            final_scores.append(f_score)
        else:
            drop_to_lowest_pcts.append(None)
            required_margins.append(None)
            final_scores.append(None)

    # 3. 写入追加指标并重排序
    result_df['最新价格'] = latest_prices
    result_df['到理论低价的回撤比例'] = drop_to_lowest_pcts
    result_df['所需资金'] = required_margins
    result_df['最终分数'] = final_scores

    result_df.sort_values(by='最终分数', ascending=False, inplace=True)
    result_df.reset_index(drop=True, inplace=True)

    return result_df


if __name__ == "__main__":
    # 执行通用配置加载测试
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
    margin_info = {float(k): v for k, v in raw_margin_info.items()}

    # 1. 运算静态基础统计数据
    final_df = generate_statistics(param_list, output_file="grid_statistics_result.csv")

    if final_df is not None:
        # 2. 联合在线实时报价，完成性价比动态计分
        final_df = calculate_final_score(final_df, margin_info)
        print(final_df.round(4))