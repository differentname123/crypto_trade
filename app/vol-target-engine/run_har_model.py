import os
import numpy as np
import pandas as pd
import statsmodels.api as sm
from sklearn.metrics import mean_absolute_error, r2_score
import warnings

warnings.filterwarnings('ignore')


# ==========================================
# 1. 数据处理引擎 (修正 ffill 污染，增加完整率校验)
# ==========================================
def process_kline_rv(kline_path, timeframe='5min'):
    # 读取数据
    df = pd.read_csv(kline_path, usecols=['open_time', 'close'])
    df['datetime'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    df['close'] = pd.to_numeric(df['close'], errors='coerce')

    # 清洗重复和空值
    df = (df.dropna(subset=['datetime', 'close'])
          .sort_values('datetime')
          .drop_duplicates('datetime', keep='last')
          .set_index('datetime'))

    # 重采样(取最后一个有效价格)，**严禁无限 ffill**
    close = df['close'].resample(timeframe).last()

    # 仅当当前点和上一个点都非空时，才计算收益率(防止跨越长时间断线的假收益)
    log_return = np.log(close).diff()
    valid_return = close.notna() & close.shift(1).notna()
    log_return = log_return.where(valid_return)

    # 计算预期的每天 K 线数量，用于完整率校验
    seconds = pd.Timedelta(timeframe).total_seconds()
    expected_bars = int(86400 / seconds)

    # 计算每日统计
    daily_count = log_return.resample('1D').count()
    daily_rv = log_return.pow(2).resample('1D').sum()

    # 【重要校验】：每天的数据完整率必须达到 95% 以上，否则当天 RV 视为无效 (NaN)
    valid_day = daily_count >= int(expected_bars * 0.95)
    dropped_days = len(daily_count) - valid_day.sum()
    if dropped_days > 0:
        print(f"  -> [数据质检] 发现 {dropped_days} 天的数据完整率低于 95%，已作无效处理以防止RV失真。")

    daily_rv = daily_rv.where(valid_day)

    # 提取结果，去除时区影响，确保生成连续自然日历
    result = daily_rv.rename('RV_1d').to_frame()
    result.index = result.index.tz_localize(None)
    result = result.asfreq('D').reset_index()
    result.rename(columns={'datetime': 'date'}, inplace=True)

    return result


def load_dvol_data(dvol_path):
    df_dvol = pd.read_csv(dvol_path)
    df_dvol['date'] = pd.to_datetime(df_dvol['date'])
    return df_dvol[['date', 'Implied_Variance_It']]


# ==========================================
# 2. Log-HAR 特征构建 (修正 rolling/shift 顺序)
# ==========================================
def create_har_base_features(df_rv):
    """
    第一阶段：在连续日历上严格构造 HAR 特征和 t+1 预测目标
    """
    data = df_rv.copy()
    data = data.set_index('date').sort_index().asfreq('D')

    eps = 1e-12
    data['RV_1d'] = data['RV_1d'].clip(lower=eps)

    # 因为是连续日历（.asfreq('D')），这里的 rolling(7) 才是真正的过去 7 个自然日
    data['RV_W'] = data['RV_1d'].rolling(window=7, min_periods=7).mean()
    data['RV_M'] = data['RV_1d'].rolling(window=30, min_periods=30).mean()

    # 目标为下一日的 RV
    data['Target_Y'] = data['RV_1d'].shift(-1)
    data['Forecast_Date'] = data.index + pd.Timedelta(days=1)  # 明确记录预测目标的真实日期

    # 增加基准对比特征 (不含未来信息)
    data['Last_RV_Pred'] = data['RV_1d']
    data['EWMA_Pred'] = data['RV_1d'].ewm(alpha=0.06, adjust=False).mean()

    # 取对数
    data['log_D'] = np.log(data['RV_1d'])
    data['log_W'] = np.log(data['RV_W'])
    data['log_M'] = np.log(data['RV_M'])
    data['log_Target_Y'] = np.log(data['Target_Y'])

    return data.reset_index()


def run_out_of_sample_backtest(data, use_iv=False, window_size=1095):
    try:
        if len(data) <= window_size:
            print(f"  -> [警告] 提取特征后有效天数为 {len(data)} 天，不足以满足训练窗口 ({window_size} 天)，跳过回测。")
            return pd.DataFrame()

        predictions = []
        actuals = []
        forecast_dates = []
        last_rvs = []
        ewmas = []

        feature_cols = ['log_D', 'log_W', 'log_M']
        if use_iv:
            feature_cols.append('log_I')

        # 开始滚动样本外回测
        for t in range(window_size, len(data)):
            train_data = data.iloc[t - window_size: t]

            X_train = sm.add_constant(train_data[feature_cols])
            y_train = train_data['log_Target_Y']

            # OLS 拟合
            model = sm.OLS(y_train, X_train).fit()

            # Smearing 修正
            smearing_factor = np.mean(np.exp(model.resid))

            current_day_features = sm.add_constant(data.iloc[[t]][feature_cols], has_constant='add')
            log_pred = model.predict(current_day_features).values[0]

            pred_variance = np.exp(log_pred) * smearing_factor

            predictions.append(pred_variance)
            actuals.append(data.iloc[t]['Target_Y'])
            forecast_dates.append(data.iloc[t]['Forecast_Date'])  # 使用下一天作为标签
            last_rvs.append(data.iloc[t]['Last_RV_Pred'])
            ewmas.append(data.iloc[t]['EWMA_Pred'])

        results_df = pd.DataFrame({
            'Forecast_Date': forecast_dates,
            'Actual_Variance': actuals,
            'Predicted_Variance': predictions,
            'Last_RV_Pred': last_rvs,
            'EWMA_Pred': ewmas
        })

        return results_df
    except Exception as e:
        print(f"[Error] 回测过程中出现异常: {e}")
        return pd.DataFrame()


# ==========================================
# 3. 评估指标计算 (引入 QLIKE 与交集对齐)
# ==========================================
def qlike_loss(y_true, y_pred, eps=1e-12):
    """QLIKE 损失函数：对波动率预测极度重要的非对称评价"""
    y_true = np.maximum(np.asarray(y_true), eps)
    y_pred = np.maximum(np.asarray(y_pred), eps)
    ratio = y_true / y_pred
    return np.mean(ratio - np.log(ratio) - 1)


def evaluate_models_comparatively(results_pure, results_full):
    if results_pure.empty:
        print("  -> 警告: 纯 K 线回测无结果。\n")
        return

    # 【核心修正】：A模型和B模型必须在完全相同的预测日期上比较
    if results_full.empty:
        print("  -> [警告] 双核回测无结果，仅评估纯 K 线基准。")
        common_df = results_pure
        has_iv = False
    else:
        # 通过 Forecast_Date 取交集，保证考试题目完全一致
        common_df = pd.merge(
            results_pure,
            results_full[['Forecast_Date', 'Predicted_Variance']],
            on='Forecast_Date',
            suffixes=('_HAR', '_HAR_IV')
        )
        has_iv = True

    if common_df.empty:
        print("  -> [警告] 两个模型没有重合的样本外测试日期，无法评估。")
        return

    print(f"  -> 有效对齐回测天数: {len(common_df)} 天 (所有模型都在相同测试集上)")

    # 真实值与基准值
    y_true = common_df['Actual_Variance']
    pred_last = common_df['Last_RV_Pred']
    pred_ewma = common_df['EWMA_Pred']
    pred_har = common_df['Predicted_Variance_HAR'] if has_iv else common_df['Predicted_Variance']

    # QLIKE 计算
    qlike_last = qlike_loss(y_true, pred_last)
    qlike_ewma = qlike_loss(y_true, pred_ewma)
    qlike_har = qlike_loss(y_true, pred_har)

    print("\n  [基准模型表现 (QLIKE越小越好)]")
    print(f"    - Last-RV (昨天预测今天): QLIKE = {qlike_last:.5f}")
    print(f"    - EWMA (指数加权平均):    QLIKE = {qlike_ewma:.5f}")

    print("\n  [模型 A: 单核引擎 (纯 K 线 Log-HAR)]")
    print(f"    - QLIKE Loss: {qlike_har:.5f}")
    print(f"    - R-squared : {r2_score(y_true, pred_har):.4f}")
    print(
        f"    - 对比 EWMA : {'提升' if qlike_har < qlike_ewma else '下降'} {(qlike_ewma - qlike_har) / qlike_ewma * 100:.2f}%")

    if has_iv:
        pred_har_iv = common_df['Predicted_Variance_HAR_IV']
        qlike_har_iv = qlike_loss(y_true, pred_har_iv)
        print("\n  [模型 B: 双核引擎 (K 线 + 期权 IV)]")
        print(f"    - QLIKE Loss: {qlike_har_iv:.5f}")
        print(f"    - R-squared : {r2_score(y_true, pred_har_iv):.4f}")
        print(
            f"    - 对比 纯HAR  : {'提升' if qlike_har_iv < qlike_har else '下降'} {(qlike_har - qlike_har_iv) / qlike_har * 100:.2f}%")
    print("----------------------------------------------\n")


# ==========================================
# 4. 主控函数：封装完整实验流程
# ==========================================
def run_prediction_experiment(kline_file, dvol_file, timeframe='5min', train_window=1095):
    asset_name = os.path.basename(kline_file).split('_')[0] if kline_file else "UNKNOWN"

    print("==============================================")
    print(f"   OccamVol 预测引擎对照实验 (A/B Test)       ")
    print(f"   测试对象: {asset_name} | 频率: {timeframe} | 窗口: {train_window}天")
    print("==============================================")

    if not os.path.exists(kline_file):
        print(f"[Error] 找不到 K 线文件: {kline_file}")
        return

    # 1. 第一阶段构建：提取 RV 并生成严格纯正的 HAR 特征和 Target
    df_rv = process_kline_rv(kline_file, timeframe=timeframe)
    base_data = create_har_base_features(df_rv)

    # 纯净版只需舍弃包含 NaN 的行
    df_pure = base_data.dropna(subset=['log_D', 'log_W', 'log_M', 'log_Target_Y']).reset_index(drop=True)

    # 2. 第二阶段合并：加载 IV 并采用 LEFT JOIN（不会破坏原有序列），然后再剔除缺失的行
    df_full = pd.DataFrame()
    if os.path.exists(dvol_file):
        df_iv = load_dvol_data(dvol_file)
        if not df_iv.empty:
            df_full = pd.merge(base_data, df_iv, on='date', how='left')
            df_full['Implied_Variance_It'] = df_full['Implied_Variance_It'].clip(lower=1e-12)
            df_full['log_I'] = np.log(df_full['Implied_Variance_It'])
            df_full = df_full.dropna(subset=['log_D', 'log_W', 'log_M', 'log_I', 'log_Target_Y']).reset_index(drop=True)
    else:
        print(f"  -> [警告] 找不到期权 IV 文件: {dvol_file}。将仅运行纯 K 线版本。")

    # 3. 分别运行回测
    results_pure = run_out_of_sample_backtest(df_pure, use_iv=False, window_size=train_window)

    if df_full.empty:
        results_full = pd.DataFrame()
    else:
        results_full = run_out_of_sample_backtest(df_full, use_iv=True, window_size=train_window)

    # 4. 在共同预测日上严格对齐对比评估
    evaluate_models_comparatively(results_pure, results_full)


# ==========================================
# 5. 运行入口
# ==========================================
if __name__ == "__main__":
    KLINE_FILE = r'W:\project\python_project\oke_auto_trade\kline_data\ETHUSDT_1m_2021-01-01_merged.csv'
    DVOL_FILE = 'eth_dvol_2021_now.csv'
    TRAIN_WINDOW = 1095

    RESAMPLE_TIMEFRAME_list = ['1min', '5min', '15min', '30min', '1h']

    for RESAMPLE_TIMEFRAME in RESAMPLE_TIMEFRAME_list:
        run_prediction_experiment(
            kline_file=KLINE_FILE,
            dvol_file=DVOL_FILE,
            timeframe=RESAMPLE_TIMEFRAME,
            train_window=TRAIN_WINDOW
        )

    KLINE_FILE = r'W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2021-01-01_merged.csv'
    DVOL_FILE = 'btc_dvol_2021_now.csv'
    for RESAMPLE_TIMEFRAME in RESAMPLE_TIMEFRAME_list:
        run_prediction_experiment(
            kline_file=KLINE_FILE,
            dvol_file=DVOL_FILE,
            timeframe=RESAMPLE_TIMEFRAME,
            train_window=TRAIN_WINDOW
        )

    KLINE_FILE = r'W:\project\python_project\oke_auto_trade\kline_data\SOLUSDT_1m_2021-01-01_merged.csv'
    DVOL_FILE = 'sol_dvol_2021_now.csv'
    for RESAMPLE_TIMEFRAME in RESAMPLE_TIMEFRAME_list:
        run_prediction_experiment(
            kline_file=KLINE_FILE,
            dvol_file=DVOL_FILE,
            timeframe=RESAMPLE_TIMEFRAME,
            train_window=TRAIN_WINDOW
        )