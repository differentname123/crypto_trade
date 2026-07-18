import pandas as pd
import os

from common.common_utils import read_json, save_json


def calculate_grid_score(df: pd.DataFrame, resample_rule: str = '15min') -> tuple:
    """
    计算网格交易频率预期评分 (Score) 与 中位数波动率 (Median NTR)

    返回:
    tuple: (日均震荡路径总跨度评分 final_score, 单周期中位数波动百分比 median_ntr_pct)
    """
    # 1. 保护原始数据，避免修改原 df
    data = df.copy()

    # 2. 处理时间戳，并设为索引以便重采样
    if not pd.api.types.is_datetime64_any_dtype(data['open_time']):
        data['open_time'] = pd.to_datetime(pd.to_numeric(data['open_time']), unit='ms')
    data.set_index('open_time', inplace=True)
    data.sort_index(inplace=True)  # 确保索引按时间排序

    # ================= 新增逻辑 1：最多只统计最近一年以内的数据 =================
    if not data.empty:
        max_time = data.index.max()
        one_year_ago = max_time - pd.Timedelta(days=365)
        data = data[data.index >= one_year_ago]
    # ============================================================================

    # 确保 OHLC 列为浮点数
    cols = ['open', 'high', 'low', 'close']
    data[cols] = data[cols].astype(float)

    # 3. 第一步：时间轴降噪（重采样）
    resampled = data.resample(resample_rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).dropna()

    if len(resampled) < 2:
        return 0.0, 0.0

    # 4. 第二步：计算归一化真实波幅 (NTR)
    resampled['prev_close'] = resampled['close'].shift(1)

    high_to_low = resampled['high'] - resampled['low']
    high_to_pc = (resampled['high'] - resampled['prev_close']).abs()
    low_to_pc = (resampled['low'] - resampled['prev_close']).abs()

    resampled['tr'] = pd.concat([high_to_low, high_to_pc, low_to_pc], axis=1).max(axis=1)
    resampled['ntr'] = resampled['tr'] / resampled['prev_close']

    # ================= 逻辑：计算中位数波动率 =================
    # 提取大于0的有效波动，避免死水停盘时间拉低中位数
    valid_ntr = resampled.loc[resampled['ntr'] > 0, 'ntr']
    median_ntr = valid_ntr.median() if not valid_ntr.empty else 0.0
    # 转换为百分比展示，例如 0.005 转换为 0.5 (代表 0.5%)
    median_ntr_pct = median_ntr * 100
    # ==============================================================

    # 5. 第三步：累计并计算日均得分
    total_ntr = resampled['ntr'].sum()

    time_span = resampled.index.max() - resampled.index.min()
    total_days = time_span.total_seconds() / (24 * 3600.0)

    if total_days <= 0:
        return 0.0, 0.0

    final_score = total_ntr / total_days

    return final_score, median_ntr_pct


# ================= 新增逻辑 3：包装为函数，支持持久化 =================
def generate_statistics(param_list: list, output_file: str = "grid_statistics_result.csv"):
    """
    生成并保存整体统计DF。若指定的文件存在则直接加载跳过计算。
    """
    if os.path.exists(output_file):
        print(f"统计文件 [{output_file}] 已存在，直接加载并跳过计算...")
        return pd.read_csv(output_file)

    periods = ['5min', '15min', '30min', '1h', '4h']
    results = []

    for param in param_list:
        csv_file_path = param["csv_file_path"]

        try:
            coin_name = os.path.basename(csv_file_path).split('_')[0]
        except Exception:
            coin_name = "Unknown"

        try:
            temp_df = pd.read_csv(csv_file_path)
        except Exception as e:
            print(f"读取文件失败 {csv_file_path}: {e}")
            continue

        coin_result = {'Coin': coin_name}

        # ================= 新增逻辑 2：基于完整 df 计算全周期最大价格和最大回撤（带起止时价） =================
        try:
            full_df = temp_df.copy()
            if not pd.api.types.is_datetime64_any_dtype(full_df['open_time']):
                full_df['open_time'] = pd.to_datetime(pd.to_numeric(full_df['open_time']), unit='ms')
            full_df.set_index('open_time', inplace=True)
            full_df.sort_index(inplace=True)

            high_col = full_df['high'].astype(float)
            low_col = full_df['low'].astype(float)

            # 1. 统计最大价格及其时间
            max_price = high_col.max()
            max_price_time = high_col.idxmax()

            # 2. 统计最大回撤起止时间和价格（最大回撤 = (当期低点 - 历史最高点) / 历史最高点 的最小值）
            cum_max = high_col.cummax()
            drawdowns = (low_col - cum_max) / cum_max
            max_dd_pct = drawdowns.min() * 100

            # 结束（谷底）时间与价格
            max_dd_end_time = drawdowns.idxmin()
            max_dd_end_price = low_col.loc[max_dd_end_time]
            if isinstance(max_dd_end_price, pd.Series):
                max_dd_end_price = max_dd_end_price.iloc[0]

            # 开始（峰顶）时间与价格（在跌至谷底前所经历的最高点）
            max_dd_start_time = high_col.loc[:max_dd_end_time].idxmax()
            max_dd_start_price = high_col.loc[max_dd_start_time]
            if isinstance(max_dd_start_price, pd.Series):
                max_dd_start_price = max_dd_start_price.iloc[0]

            coin_result['Max_Price'] = max_price
            coin_result['Max_Price_Time'] = max_price_time
            coin_result['Max_DD(%)'] = max_dd_pct
            coin_result['Max_DD_Start_Time'] = max_dd_start_time
            coin_result['Max_DD_Start_Price'] = max_dd_start_price
            coin_result['Max_DD_End_Time'] = max_dd_end_time
            coin_result['Max_DD_End_Price'] = max_dd_end_price
        except Exception as e:
            print(f"[{coin_name}] 计算完整价格和回撤失败: {e}")
            coin_result['Max_Price'] = None
            coin_result['Max_Price_Time'] = None
            coin_result['Max_DD(%)'] = None
            coin_result['Max_DD_Start_Time'] = None
            coin_result['Max_DD_Start_Price'] = None
            coin_result['Max_DD_End_Time'] = None
            coin_result['Max_DD_End_Price'] = None
        # ========================================================================================

        # 循环计算并分别解包 Score 和 Median（此时内部最多只算近一年数据）
        scores = []
        for period in periods:
            score, med_pct = calculate_grid_score(temp_df, resample_rule=period)
            coin_result[f'{period}_Score'] = score
            coin_result[f'{period}_Med(%)'] = med_pct
            scores.append(score)

        # 仅对 Score 求平均值用于最终排序
        coin_result['Avg_Score'] = sum(scores) / len(scores)

        results.append(coin_result)
        print(f"[{coin_name}] 多周期评分及中位数计算完成...")

    if results:
        final_df = pd.DataFrame(results)

        # 按照综合平均分降序排列
        final_df.sort_values(by='Avg_Score', ascending=False, inplace=True)
        final_df.reset_index(drop=True, inplace=True)

        # 调整列顺序，让同一个周期的 Score 和 Med 挨在一起，再拼上新增的统计字段
        ordered_columns = ['Coin']
        for p in periods:
            ordered_columns.append(f'{p}_Score')
            ordered_columns.append(f'{p}_Med(%)')
        ordered_columns.append('Avg_Score')
        ordered_columns.extend(
            ['Max_Price', 'Max_Price_Time', 'Max_DD(%)', 'Max_DD_Start_Time', 'Max_DD_Start_Price', 'Max_DD_End_Time',
             'Max_DD_End_Price'])

        final_df = final_df[ordered_columns]

        # 持久化保存
        try:
            final_df.to_csv(output_file, index=False)
            print(f"结果已成功持久化至: {output_file}")
        except Exception as e:
            print(f"持久化保存文件失败: {e}")

        return final_df
    else:
        print("没有成功计算出任何结果，请检查数据路径。")
        return None


# ============================================================================


if __name__ == "__main__":
    # 示例加载json保存json的代码
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
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\SKYUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\UNIUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\STXUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RENDERUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\RUNEUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\PENDLEUSDT_1m_2021-01-01_merged.csv"},
        {"csv_file_path": r"W:\project\python_project\oke_auto_trade\kline_data\KASUSDT_1m_2021-01-01_merged.csv"}
    ]

    # 调用封装好的主函数
    final_df = generate_statistics(param_list, output_file="grid_statistics_result.csv")

    if final_df is not None:
        print("\n" + "=" * 120)
        print("所有币种网格评分及中位数参考 (按 Avg_Score 降序):")
        print("=" * 120)

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1200)
        # 将浮点数保留合理的小数位数，使表格更清爽
        print(final_df.round(4))