"""
================================================================================
[核心数据流与功能摘要] 混合驱动长周期 (365天+) 加密货币特征回测矩阵构建引擎
================================================================================
[功能摘要]
本模块突破了单一 API 的物理限流与时长限制，自动化拉取、拼装并聚合多维度的市场特征。

[输入数据]
1. 交易所 API (CCXT & Binance fAPI)：提供实时至中短期的 K线、OI(持仓量)、底层买卖量与溢价指数。
2. Binance Vision (HTTP ZIP)：提供日维度的历史底座数据 (Metrics CSV)。

[数据流转/交互]
- 本地历史拼装：根据需求天数，逆向推算并下载 Vision ZIP 归档，解压并构建 Pandas 内存总表。
- 游标增量拼接：利用最后时间戳作为断点，调用 API 追平最新数据。
- 异构矩阵聚合：将 K线、OI增量、CVD(累计交易量差)、预测资金费率通过 timestamp 强对齐 (Inner Join)，
  随后使用时序网格 (resample) 抹平断层，重建 CVD。

[输出数据]
输出高度对齐、无重复、可直接用于量化机器学习/回测的 CSV 特征矩阵文件。
================================================================================
"""

import os
import time
import zipfile
import traceback
import warnings

import ccxt
import requests
import pandas as pd

# 忽略 pandas 的时区链式警告及降级警告，保证控制台整洁
warnings.filterwarnings('ignore')

# ============================================================================
# 全局配置常量
# ============================================================================
GLOBAL_PROXY = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

EXCHANGE_LIMITS = {
    'binance': 1000,
}

VISION_DATA_DIR = './vision_data'
BACKTEST_DATA_DIR = './data'

# ============================================================================
# 工具函数层 (时间与网络抽象)
# ============================================================================

def init_exchange(exchange_name, default_type='swap'):
    """
    初始化 CCXT 交易所实例
    出参: CCXT Exchange Object
    """
    config = {
        'enableRateLimit': True,
        'proxies': GLOBAL_PROXY,
    }
    if default_type:
        config['options'] = {'defaultType': default_type}
    return getattr(ccxt, exchange_name)(config)


def convert_to_beijing_time(df, timestamp_col='timestamp'):
    """
    将包含毫秒级时间戳、字符串日期或异构时区的时间列，统一并标准化为北京时间 (无时区信息)
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    df = df.copy()

    # 步骤 1: 智能解析底层数据为 datetime
    # 如果是纯数字(毫秒时间戳)，必须使用 unit='ms'；如果是字符串或已是datetime，则依赖 pandas 自动推导
    if pd.api.types.is_numeric_dtype(df[timestamp_col]):
        dt_series = pd.to_datetime(df[timestamp_col], unit='ms', errors='coerce')
    else:
        dt_series = pd.to_datetime(df[timestamp_col], errors='coerce')

    # 步骤 2: 统一的时区本地化与转换
    # 检查是否已有 timezone 信息，若无则默认赋予 UTC
    if dt_series.dt.tz is None:
        dt_series = dt_series.dt.tz_localize('UTC')

    # 转为上海时间 -> 剥离时区属性 -> 规整到秒
    df[timestamp_col] = dt_series.dt.tz_convert('Asia/Shanghai').dt.tz_localize(None).dt.round('s')

    # 剔除无法解析的脏数据
    df.dropna(subset=[timestamp_col], inplace=True)
    return df


def fetch_with_pagination(exchange, fetch_func, symbol, timeframe, since, limit_per_request):
    """
    通用分页拉取逻辑 (标准 CCXT API)
    出参: List[Dict/List]. 原始 API 响应数据的展平集合
    """
    all_data = []
    current_since = since
    retry_count = 0

    while True:
        try:
            # 兼容带有和不带 timeframe 的 API 签名
            if timeframe:
                data = fetch_func(symbol, timeframe, since=current_since, limit=limit_per_request)
            else:
                data = fetch_func(symbol, since=current_since, limit=limit_per_request)

            if not data:
                break

            all_data.extend(data)
            last_timestamp = int(data[-1].get('timestamp', 0)) if isinstance(data[0], dict) else int(data[-1][0])

            if not last_timestamp:
                break

            current_since = last_timestamp + 1

            if last_timestamp >= exchange.milliseconds() - 60000:
                break

            retry_count = 0
            time.sleep(0.05)

        except Exception as e:
            retry_count += 1
            if retry_count > 3:
                raise RuntimeError(f"[API分页拉取] 超过最大重试次数 | 标的: 【{symbol}】 | 游标: 【{current_since}】 | 失败原因: 【{e}】") from e
            print(f"[API分页拉取] 触发限流或网络波动 | 标的: 【{symbol}】 | 等待重试 ({retry_count}/3)...")
            time.sleep(2)

    return all_data

def _fetch_binance_fapi_paginated(exchange, endpoint_func, raw_symbol, timeframe_id, since):
    """
    币安私有 fAPI 专用分页拉取逻辑
    出参: List[List]. 原始 fAPI 响应数据的展平集合
    """
    all_data = []
    current_since = since
    retry_count = 0

    while True:
        try:
            params = {
                'symbol': raw_symbol,
                'interval': timeframe_id,
                'startTime': current_since,
                'limit': 1000
            }
            data = endpoint_func(params)

            if not data:
                break

            all_data.extend(data)
            last_timestamp = int(data[-1][0])
            current_since = last_timestamp + 1

            if last_timestamp >= exchange.milliseconds() - 60000:
                break

            retry_count = 0
            time.sleep(0.05)

        except Exception as e:
            retry_count += 1
            if retry_count > 3:
                raise RuntimeError(f"[fAPI分页拉取] 超过最大重试次数 | 标的: 【{raw_symbol}】 | 失败原因: 【{e}】") from e
            time.sleep(2)

    return all_data


# ============================================================================
# 数据拉取层 (业务指标构建)
# ============================================================================

def fetch_long_history(exchange_name, symbol, timeframe='5m', days=365):
    """
    拉取基础历史 K 线
    出参: DataFrame. 核心Shape: ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    """
    exchange = init_exchange(exchange_name, default_type=None)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    limit = EXCHANGE_LIMITS.get(exchange_name, 1000)

    try:
        raw_data = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, timeframe, since, limit)
    except Exception as e:
        print(f"[历史K线] 拉取失败 | 标的: 【{symbol}】 | 错误详情: 【{e}】")
        return pd.DataFrame()

    if not raw_data:
        return pd.DataFrame()

    df = pd.DataFrame(raw_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = convert_to_beijing_time(df)
    print(f"[历史K线] 拉取成功 | 标的: 【{symbol}】 | 结果: 【共 {len(df)} 条对齐数据】")
    return df


def auto_download_and_merge_daily(symbol, days=365, save_dir=VISION_DATA_DIR, max_retries=3):
    """
    本地优先的 Binance Vision 历史指标归档拉取引擎
    出参: DataFrame. 核心Shape: ['timestamp', 'oi_amount']
    """
    clean_symbol = symbol.split(':')[0].replace('/', '')
    os.makedirs(save_dir, exist_ok=True)
    out_path = os.path.join(save_dir, f"{clean_symbol}_metrics.csv")

    end_date = pd.Timestamp.utcnow().floor('D') - pd.Timedelta(days=1)
    start_date = (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).floor('D')

    print(f"\n[Vision引擎] 启动历史底座拉取 | 标的: 【{clean_symbol}】 | 范围: 【{start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}】")

    all_dfs = []
    current_date = start_date

    while current_date <= end_date:
        ymd_str = current_date.strftime('%Y-%m-%d')
        zip_filename = f"{clean_symbol}-metrics-{ymd_str}.zip"
        zip_path = os.path.join(save_dir, zip_filename)

        if os.path.exists(zip_path):
            try:
                with zipfile.ZipFile(zip_path) as z:
                    csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                    df = pd.read_csv(z.open(csv_name), low_memory=False)
                    df['_source_date'] = ymd_str
                    all_dfs.append(df)
                print(f"[Vision引擎] 本地命中 | 日期: 【{ymd_str}】 | 结果: 【缓存读取成功】")
            except Exception as e:
                print(f"[Vision引擎] 本地缓存异常 | 日期: 【{ymd_str}】 | 文件可能损坏，建议手动删除 | 报错: 【{e}】")
        else:
            daily_url = f"https://data.binance.vision/data/futures/um/daily/metrics/{clean_symbol}/{zip_filename}"
            # 引入重试机制包裹网络请求层
            for attempt in range(max_retries):
                try:
                    resp = requests.get(daily_url, proxies=GLOBAL_PROXY, timeout=10)
                    if resp.status_code == 200:
                        with open(zip_path, 'wb') as f:
                            f.write(resp.content)
                        with zipfile.ZipFile(zip_path) as z:
                            csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                            df = pd.read_csv(z.open(csv_name), low_memory=False)
                            df['_source_date'] = ymd_str
                            all_dfs.append(df)
                        print(f"[Vision引擎] 网络下载 | 日期: 【{ymd_str}】 | 结果: 【落地缓存并解析成功】")
                        time.sleep(0.1)
                        break  # 下载成功，跳出重试循环
                    elif resp.status_code == 404:
                        print(f"[Vision引擎] 网络异常 | 日期: 【{ymd_str}】 | 结果: 【HTTP 404 远端尚未生成此日数据】")
                        break  # 确定远端无数据，无需重试，跳出
                    else:
                        print(f"[Vision引擎] 网络异常 | 日期: 【{ymd_str}】 | 结果: 【HTTP {resp.status_code}】")
                        if attempt < max_retries - 1:
                            time.sleep(1) # 非200/404的异常状态码，稍作等待重试
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"[Vision引擎] 网络下载崩溃 | 日期: 【{ymd_str}】 | 报错: 【{e}】 | 状态: 【等待触发第 {attempt + 1} 次重试】")
                        time.sleep(2)  # 给网络层释放和重置的缓冲时间
                    else:
                        print(f"[Vision引擎] 网络下载崩溃 | 日期: 【{ymd_str}】 | 报错: 【{e}】 | 结果: 【已达到最大重试次数 {max_retries}，彻底跳过】")

        current_date += pd.Timedelta(days=1)

    if not all_dfs:
        print(f"[Vision引擎] 警告 | 标的: 【{clean_symbol}】 | 结果: 【未获取任何历史归档，将完全退化为 API 实时抓取模式】")
        return pd.DataFrame()

    merged_df = pd.concat(all_dfs, ignore_index=True)

    # 数据连续性诊断
    expected_dates = pd.date_range(start=start_date, end=end_date).strftime('%Y-%m-%d').tolist()
    actual_dates = merged_df['_source_date'].unique().tolist()
    missing_dates = sorted(list(set(expected_dates) - set(actual_dates)))

    if missing_dates:
        print(f"[Vision诊断] 数据断层警告 | 缺失天数: 【{len(missing_dates)} 天】 | 缺失明细: 【{', '.join(missing_dates)}】")
    else:
        print(f"[Vision诊断] 连续性检查 | 状态: 【完美通过】 | 无任何断层数据。")

    merged_df.drop(columns=['_source_date'], inplace=True)

    # 格式化特征并落地
    if 'create_time' in merged_df.columns and 'sum_open_interest' in merged_df.columns:
        merged_df.rename(columns={'create_time': 'timestamp', 'sum_open_interest': 'oi_amount'}, inplace=True)
        merged_df = convert_to_beijing_time(merged_df)
        merged_df.sort_values('timestamp', inplace=True)

        duplicate_count = merged_df.duplicated(subset=['timestamp']).sum()
        if duplicate_count > 0:
            merged_df.drop_duplicates('timestamp', keep='last', inplace=True)

        cutoff_bj = pd.Timestamp.utcnow().tz_convert('Asia/Shanghai').tz_localize(None) - pd.Timedelta(days=days)
        merged_df = merged_df[merged_df['timestamp'] >= cutoff_bj]

        merged_df.to_csv(out_path, index=False)
        print(f"[Vision组装] 合并落地完成 | 路径: 【{out_path}】 | 有效特征: 【{len(merged_df)} 条】\n")
        return merged_df

    return pd.DataFrame()


def fetch_historical_oi(exchange, symbol, timeframe='5m', days=365):
    """
    混合数据引擎：加载 Vision 历史底座，并向后挂载 API 实时增量
    出参: DataFrame. 核心Shape: ['timestamp', 'oi_amount', 'oi_amount_change_pct']
    """
    target_since_ms = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    target_since_bj = pd.Timestamp.now(tz='Asia/Shanghai').tz_localize(None) - pd.Timedelta(days=days)

    clean_symbol = symbol.split(':')[0].replace('/', '')
    vision_file = os.path.join(VISION_DATA_DIR, f"{clean_symbol}_metrics.csv")

    # 强制同步底层历史文件
    auto_download_and_merge_daily(symbol, days=days, save_dir=VISION_DATA_DIR)

    df_vision = pd.DataFrame()
    if os.path.exists(vision_file):
        try:
            raw_vision = pd.read_csv(vision_file, low_memory=False)

            raw_vision['timestamp'] = pd.to_datetime(raw_vision['timestamp'])


            df_vision = raw_vision[raw_vision['timestamp'] >= target_since_bj][['timestamp', 'oi_amount']]
            print(f"[OI 引擎] 本地底座挂载 | 标的: 【{symbol}】 | 结果: 【成功载入 {len(df_vision)} 条】")
        except Exception as e:
            print(f"[OI 引擎] 本地底座挂载失败 | 标的: 【{symbol}】 | 报错: 【{e}】")

    # 计算 API 拉取起止点
    if not df_vision.empty:
        last_bj_time = df_vision['timestamp'].max()
        # 北京时间无时区 -> 赋予上海时区 -> 转 UTC -> 取时间戳
        api_since = int(pd.Timestamp(last_bj_time).tz_localize('Asia/Shanghai').tz_convert('UTC').timestamp() * 1000)
        print(f"[OI 引擎] 增量拉取准备 | 标的: 【{symbol}】 | 断点时间: 【{last_bj_time}】")
    else:
        api_since = target_since_ms

    try:
        raw_oi = fetch_with_pagination(exchange, exchange.fetch_open_interest_history, symbol, timeframe, api_since, 500)
    except Exception as e:
        print(f"[OI 引擎] API 增量拉取失败 | 标的: 【{symbol}】 | 报错: 【{e}】")
        raw_oi = []

    df_api = pd.DataFrame([{
        'timestamp': item.get('timestamp', 0),
        'oi_amount': item.get('openInterestAmount', 0),
    } for item in raw_oi]) if raw_oi else pd.DataFrame()

    df_api = convert_to_beijing_time(df_api)

    # 拼装与特征工程
    df = pd.concat([df_vision, df_api], ignore_index=True)
    if df.empty:
        return pd.DataFrame()

    df['oi_amount'] = df['oi_amount'].astype(float)
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values(by='timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    df['oi_amount_change_pct'] = (df['oi_amount'].pct_change() * 100).round(4)
    df.dropna(subset=['oi_amount_change_pct'], inplace=True)

    print(f"[OI 引擎] 增量拼装完成 | 标的: 【{symbol}】 | 最终有效特征: 【{len(df)} 条】")
    return df

def fetch_binance_cvd_history(symbol, timeframe='5m', days=365):
    """
    底层买卖量差 (CVD) 计算
    出参: DataFrame. 核心Shape: ['timestamp', 'volume_delta']
    """
    exchange = init_exchange('binance')
    try:
        exchange.load_markets()
        raw_symbol = exchange.market(symbol)['id']
    except Exception as e:
        print(f"[CVD 计算] 市场标识解析失败 | 标的: 【{symbol}】 | 报错: 【{e}】")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)

    try:
        raw_klines = _fetch_binance_fapi_paginated(exchange, exchange.fapiPublicGetKlines, raw_symbol, timeframe_id, since)
    except Exception as e:
        print(f"[CVD 计算] 原始底层数据拉取异常 | 标的: 【{symbol}】 | 报错: 【{e}】")
        return pd.DataFrame()

    if not raw_klines:
        return pd.DataFrame()

    df = pd.DataFrame(raw_klines).iloc[:, [0, 5, 9]]
    df.columns = ['timestamp', 'volume', 'taker_buy_base_vol']
    df = df.apply(pd.to_numeric, errors='coerce').dropna()

    df['taker_sell_base_vol'] = df['volume'] - df['taker_buy_base_vol']
    df['volume_delta'] = df['taker_buy_base_vol'] - df['taker_sell_base_vol']
    df = convert_to_beijing_time(df)

    print(f"[CVD 计算] 处理完成 | 标的: 【{symbol}】 | 结果: 【共 {len(df)} 条数据】")
    return df[['timestamp', 'volume_delta']]

def fetch_premium_index_klines(symbol, timeframe='5m', days=365):
    """
    拉取溢价指数并计算预测资金费率
    出参: DataFrame. 核心Shape: ['timestamp', 'premium_close', 'predicted_funding_rate']
    """
    exchange = init_exchange('binance')
    try:
        exchange.load_markets()
        raw_symbol = exchange.market(symbol)['id']
    except Exception as e:
        print(f"[溢价指数] 市场标识解析失败 | 标的: 【{symbol}】 | 报错: 【{e}】")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)

    try:
        raw_klines = _fetch_binance_fapi_paginated(exchange, exchange.fapiPublicGetPremiumIndexKlines, raw_symbol, timeframe_id, since)
    except Exception as e:
        print(f"[溢价指数] 数据拉取异常 | 标的: 【{symbol}】 | 报错: 【{e}】")
        return pd.DataFrame()

    if not raw_klines:
        return pd.DataFrame()

    df = pd.DataFrame(raw_klines).iloc[:, [0, 4]]
    df.columns = ['timestamp', 'premium_close']
    df = df.apply(pd.to_numeric, errors='coerce').dropna()
    df = convert_to_beijing_time(df)

    # 按照特定业务逻辑计算预测资金率
    df['predicted_funding_rate'] = (df['premium_close'] + 0.0001) * 100

    print(f"[溢价指数] 处理完成 | 标的: 【{symbol}】 | 结果: 【共 {len(df)} 条数据】")
    return df[['timestamp', 'premium_close', 'predicted_funding_rate']]


# ============================================================================
# 聚合管线引擎：合并异构数据
# ============================================================================

def prepare_ler_backtest_data(symbol, timeframe='5m', days=365, save_dir=BACKTEST_DATA_DIR):
    """
    主控管线：调度各大子模块拉取数据并执行 Inner Join，最终落地回测所需特征矩阵
    出参: DataFrame 或 None (当合并失败时)
    """
    print("\n" + "=" * 80)
    print(f"[聚合管线] 矩阵构建启动 | 标的: 【{symbol}】 | 目标深度: 【{days}天】")

    os.makedirs(save_dir, exist_ok=True)
    file_path = os.path.join(save_dir, f"{symbol.replace('/', '_').replace(':', '_')}_{timeframe}_ler_data.csv")

    local_df = pd.DataFrame()
    pull_days = days

    if os.path.exists(file_path):
        try:
            local_df = pd.read_csv(file_path)
            local_df['timestamp'] = pd.to_datetime(local_df['timestamp'])
            last_local_ts = local_df['timestamp'].max()
            now_bj = pd.Timestamp.now(tz='Asia/Shanghai').tz_localize(None)

            delta_days = (now_bj - last_local_ts).total_seconds() / (24 * 3600)
            pull_days = max(delta_days + 1.0, 1.0)
            print(f"[聚合管线] 发现本地有效快照 | 动态收缩网络拉取量至: 【{pull_days:.2f}天】")
        except Exception as e:
            print(f"[聚合管线] 本地快照读取失败，将执行全量重构 | 错误: 【{e}】")
            local_df = pd.DataFrame()

    exchange_inst = init_exchange('binance')

    df_oi = fetch_historical_oi(exchange_inst, symbol, timeframe, pull_days)
    df_cvd = fetch_binance_cvd_history(symbol, timeframe, pull_days)
    df_premium = fetch_premium_index_klines(symbol, timeframe, pull_days)
    df_klines = fetch_long_history('binance', symbol, timeframe, pull_days)

    if df_klines.empty or df_oi.empty or df_cvd.empty or df_premium.empty:
        print(f"[聚合管线] 矩阵合并阻断 | 原因: 【部分核心维度数据流失，无法对齐】")
        return None

    print(f"[聚合管线] 正在执行多维特征时间戳强对齐 (Inner Join)...")
    df_merged = (
        df_klines
        .merge(df_oi[['timestamp', 'oi_amount', 'oi_amount_change_pct']], on='timestamp', how='inner')
        .merge(df_cvd[['timestamp', 'volume_delta']], on='timestamp', how='inner')
        .merge(df_premium[['timestamp', 'premium_close', 'predicted_funding_rate']], on='timestamp', how='inner')
    )

    if not local_df.empty:
        # 兼容历史数据结构
        if 'cvd' in local_df.columns and 'volume_delta' not in local_df.columns:
            local_df['volume_delta'] = local_df['cvd'].diff().fillna(local_df['cvd'])
        if 'cvd' in local_df.columns:
            local_df.drop(columns=['cvd'], inplace=True)

        df_merged = pd.concat([local_df, df_merged])
        df_merged = df_merged.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
        print(f"[聚合管线] 快照缝合完毕 | 当前矩阵总长度: 【{len(df_merged)} 条】")

    # 累加重建 CVD，避免断层导致偏差
    df_merged['cvd'] = df_merged['volume_delta'].cumsum()

    # 重建严格的时间网格，补齐缝隙
    freq_str = timeframe.replace('m', 'min').replace('h', 'h')
    df_merged = df_merged.set_index('timestamp')
    df_merged = df_merged[~df_merged.index.duplicated(keep='last')]
    df_merged = df_merged.resample(freq_str).asfreq()
    df_merged = df_merged.reset_index()

    # 尾部切片，确保数据不超过请求的时长要求
    cutoff_time = pd.Timestamp.now(tz='Asia/Shanghai').tz_localize(None) - pd.Timedelta(days=days)
    df_merged = df_merged[df_merged['timestamp'] >= cutoff_time]

    df_merged.to_csv(file_path, index=False)
    print(f"[聚合管线] LER 特征矩阵落地成功 ✅ | 路径: 【{file_path}】\n")

    return df_merged

def get_top_volume_symbols(exchange_name='binance', top_n=10, quote_currency='USDT'):
    """
    动态嗅探全网交易量最高的交易对
    出参: List[str]. 核心Shape: 标的字符串列表 ['BTC/USDT:USDT', ...]
    """
    exchange = init_exchange(exchange_name, default_type='swap')
    try:
        exchange.load_markets()
        tickers = exchange.fetch_tickers()
    except Exception as e:
        print(f"[标的嗅探] 核心市场 Tickers 拉取崩溃 | 报错: 【{e}】")
        raise

    target_symbols = [t for s, t in tickers.items() if s.endswith(f':{quote_currency}')]
    sorted_tickers = sorted(target_symbols, key=lambda x: float(x.get('quoteVolume') or 0), reverse=True)
    return [t['symbol'] for t in sorted_tickers[:top_n]]


if __name__ == "__main__":
    LOOP_INTERVAL = 14400

    os.makedirs(VISION_DATA_DIR, exist_ok=True)
    print("="*80)
    print(f"[系统提示] 确保存放 Binance Vision CSV 至: 【{os.path.abspath(VISION_DATA_DIR)}】")
    print(f"[系统提示] 命名规范示例: BTCUSDT_metrics.csv")
    print("="*80)

    while True:
        try:
            # 获取头部热门标的
            top_symbols = get_top_volume_symbols('binance', 1, 'USDT')
        except Exception as e:
            print(f"[引擎调度] 无法嗅探标的列表，暂停重试中... | 错误: 【{e}】")
            time.sleep(60)
            continue

        for idx, sym in enumerate(top_symbols):
            try:
                prepare_ler_backtest_data(symbol=sym, timeframe='5m', days=365)
            except Exception as e:
                print(f"[任务异常] 主管线严重崩溃跳过 | 标的: 【{sym}】 | 错误: 【{e}】")
                traceback.print_exc()

        print(f"\n[引擎调度] ✅ 当前轮次巡检完毕 | 休眠倒计时: 【{LOOP_INTERVAL} 秒】...")
        time.sleep(LOOP_INTERVAL)