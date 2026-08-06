"""
================================================================================
[核心数据流与功能摘要] 加密货币独立特征拉取引擎 (无聚合/无合并/保留原始时间)
================================================================================
[功能摘要]
本模块突破了单一 API 的物理限流，独立拉取三大核心数据，并分别存储为独立文件：
1. 1m K线数据 (保留原始毫秒时间戳)
2. 5m 持仓量(OI)数据 (结合 Vision 底座 + API 增量，保留原始时间戳)
3. 历史已结算资金费率 (保留原始时间戳)

[数据流转]
- 各模块完全独立，不再进行任何 Inner Join 或重采样补齐。
- 支持断点续传：自动识别本地 CSV 的最后一条记录，仅拉取增量数据，极大降低 1m K线的网络请求压力。
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

# 忽略 pandas 的警告
warnings.filterwarnings('ignore')

# ============================================================================
# 全局配置常量
# ============================================================================
GLOBAL_PROXY = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

VISION_DATA_DIR = './vision_data'
BACKTEST_DATA_DIR = './data'

os.makedirs(VISION_DATA_DIR, exist_ok=True)
os.makedirs(BACKTEST_DATA_DIR, exist_ok=True)

# ============================================================================
# 工具函数层
# ============================================================================

def init_exchange(exchange_name, default_type='swap'):
    """初始化 CCXT 交易所实例"""
    config = {
        'enableRateLimit': True,
        'proxies': GLOBAL_PROXY,
    }
    if default_type:
        config['options'] = {'defaultType': default_type}
    return getattr(ccxt, exchange_name)(config)

def add_readable_time(df, ts_col='timestamp'):
    """
    保留原始毫秒时间戳的基础上，追加一列易读的北京时间，方便人工核对。
    不会修改原有的 timestamp 列。
    """
    if df.empty or ts_col not in df.columns:
        return df

    df = df.copy()
    # 将原始时间戳转为数值型
    df[ts_col] = pd.to_numeric(df[ts_col], errors='coerce')
    df.dropna(subset=[ts_col], inplace=True)
    df[ts_col] = df[ts_col].astype(int)

    # 增加易读时间列
    dt_series = pd.to_datetime(df[ts_col], unit='ms', errors='coerce')
    df['datetime_bj'] = dt_series.dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)

    return df

def fetch_with_pagination(exchange, fetch_func, symbol, since, limit_per_request, timeframe=None):
    """
    通用分页拉取逻辑 (标准 CCXT API)
    """
    all_data = []
    current_since = since
    retry_count = 0

    while True:
        try:
            if timeframe:
                data = fetch_func(symbol, timeframe, since=current_since, limit=limit_per_request)
            else:
                data = fetch_func(symbol, since=current_since, limit=limit_per_request)

            if not data:
                break

            all_data.extend(data)

            # 适配字典格式或列表格式的响应
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
                raise RuntimeError(f"[API分页拉取] 超过最大重试 | 标的: {symbol} | 报错: {e}") from e
            print(f"[API分页拉取] 触发限流，等待重试 ({retry_count}/3)...")
            time.sleep(2)

    return all_data


# ============================================================================
# 数据拉取与落地引擎 (三大独立管线)
# ============================================================================

def sync_1m_klines(exchange, symbol, days=365):
    """
    独立管线 1：拉取并保存 1 分钟级别 K 线
    """
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_1m_kline.csv")

    # 断点续传逻辑
    if os.path.exists(out_path):
        old_df = pd.read_csv(out_path)
        since = int(old_df['timestamp'].max()) + 1
        print(f"  [1m K线] 发现本地文件，从 {old_df['datetime_bj'].max()} 开始增量拉取...")
    else:
        old_df = pd.DataFrame()
        since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
        print(f"  [1m K线] 无本地文件，拉取全量 {days} 天数据...")

    raw_data = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, since, 1000, timeframe='1m')

    if not raw_data:
        print(f"  [1m K线] 无最新数据产生。")
        return

    new_df = pd.DataFrame(raw_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    new_df = add_readable_time(new_df)

    df = pd.concat([old_df, new_df], ignore_index=True)
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values('timestamp', inplace=True)
    df.to_csv(out_path, index=False)
    print(f"  [1m K线] ✅ 保存成功 | 路径: {out_path} | 总量: {len(df)} 条")


def auto_download_vision_daily_oi(symbol, days=365):
    """底层依赖：从 Binance Vision 拉取历史 OI 底座"""
    clean_symbol = symbol.split(':')[0].replace('/', '')
    end_date = pd.Timestamp.utcnow().floor('D') - pd.Timedelta(days=1)
    start_date = (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).floor('D')

    all_dfs = []
    current_date = start_date

    while current_date <= end_date:
        ymd_str = current_date.strftime('%Y-%m-%d')
        zip_filename = f"{clean_symbol}-metrics-{ymd_str}.zip"
        zip_path = os.path.join(VISION_DATA_DIR, zip_filename)

        if os.path.exists(zip_path):
            try:
                with zipfile.ZipFile(zip_path) as z:
                    csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                    df = pd.read_csv(z.open(csv_name), low_memory=False)
                    all_dfs.append(df)
            except Exception:
                pass
        else:
            daily_url = f"https://data.binance.vision/data/futures/um/daily/metrics/{clean_symbol}/{zip_filename}"
            try:
                resp = requests.get(daily_url, proxies=GLOBAL_PROXY, timeout=10)
                if resp.status_code == 200:
                    with open(zip_path, 'wb') as f:
                        f.write(resp.content)
                    with zipfile.ZipFile(zip_path) as z:
                        csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                        df = pd.read_csv(z.open(csv_name), low_memory=False)
                        all_dfs.append(df)
                    time.sleep(0.1)
            except Exception:
                pass
        current_date += pd.Timedelta(days=1)

    if not all_dfs:
        return pd.DataFrame()

    merged_df = pd.concat(all_dfs, ignore_index=True)
    if 'create_time' in merged_df.columns and 'sum_open_interest' in merged_df.columns:
        # 币安 Vision 默认的 create_time 是毫秒级时间戳或字符串
        merged_df.rename(columns={'create_time': 'timestamp', 'sum_open_interest': 'oi_amount'}, inplace=True)

        # 强制转换为毫秒级时间戳 (如果是字符串型时间戳则转换)
        if pd.api.types.is_string_dtype(merged_df['timestamp']):
            merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp']).astype('int64') // 10**6

        return merged_df[['timestamp', 'oi_amount']]
    return pd.DataFrame()

def sync_5m_oi(exchange, symbol, days=365):
    """
    独立管线 2：拉取并保存 5 分钟级别 持仓量(OI)
    """
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_5m_oi.csv")

    # 1. 尝试拉取 Vision 历史底座
    df_vision = auto_download_vision_daily_oi(symbol, days)

    # 2. 确定 API 增量拉取起点
    if not df_vision.empty:
        api_since = int(df_vision['timestamp'].max()) + 1
        print(f"  [5m OI] Vision 底座加载成功，接力 API 增量拉取...")
    else:
        api_since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
        print(f"  [5m OI] 无 Vision 底座，全量 API 拉取 (可能受限交易所时长)...")

    # 3. 拉取 API 增量
    raw_oi = fetch_with_pagination(exchange, exchange.fetch_open_interest_history, symbol, api_since, 500, timeframe='5m')

    df_api = pd.DataFrame([{
        'timestamp': item.get('timestamp', 0),
        'oi_amount': item.get('openInterestAmount', 0),
    } for item in raw_oi]) if raw_oi else pd.DataFrame()

    # 4. 合并本地旧文件、Vision底座、API增量
    old_df = pd.read_csv(out_path) if os.path.exists(out_path) else pd.DataFrame()

    df = pd.concat([old_df, df_vision, df_api], ignore_index=True)
    if df.empty:
        print(f"  [5m OI] 无任何有效数据。")
        return

    df['oi_amount'] = df['oi_amount'].astype(float)
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df = add_readable_time(df)
    df.sort_values(by='timestamp', ascending=True, inplace=True)

    df.to_csv(out_path, index=False)
    print(f"  [5m OI] ✅ 保存成功 | 路径: {out_path} | 总量: {len(df)} 条")


def sync_funding_rates(exchange, symbol, days=365):
    """
    独立管线 3：拉取并保存历史已结算资金费率
    """
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_funding_rates.csv")

    # 断点续传
    if os.path.exists(out_path):
        old_df = pd.read_csv(out_path)
        since = int(old_df['timestamp'].max()) + 1
        print(f"  [资金费率] 发现本地文件，从 {old_df['datetime_bj'].max()} 开始增量拉取...")
    else:
        old_df = pd.DataFrame()
        since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
        print(f"  [资金费率] 无本地文件，拉取全量 {days} 天数据...")

    raw_data = fetch_with_pagination(exchange, exchange.fetch_funding_rate_history, symbol, since, 1000, timeframe=None)

    if not raw_data:
        print(f"  [资金费率] 无最新数据产生。")
        return

    new_df = pd.DataFrame([{
        'timestamp': item.get('timestamp', 0),
        'funding_rate': item.get('fundingRate', 0),
    } for item in raw_data])

    new_df = add_readable_time(new_df)

    df = pd.concat([old_df, new_df], ignore_index=True)
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values('timestamp', inplace=True)
    df.to_csv(out_path, index=False)
    print(f"  [资金费率] ✅ 保存成功 | 路径: {out_path} | 总量: {len(df)} 条")


def fetch_independent_datasets(symbol, days=365):
    """
    主控函数：依次调用三个独立引擎，分别保存各自的文件，互不干扰，不进行合并。
    """
    print("\n" + "=" * 80)
    print(f"🚀 开始抓取独立特征数据集 | 标的: 【{symbol}】 | 目标范围: 【{days}天】")

    exchange = init_exchange('binance')

    try:
        sync_1m_klines(exchange, symbol, days)
    except Exception as e:
        print(f"  [1m K线] 抓取异常: {e}")

    try:
        sync_5m_oi(exchange, symbol, days)
    except Exception as e:
        print(f"  [5m OI] 抓取异常: {e}")

    try:
        sync_funding_rates(exchange, symbol, days)
    except Exception as e:
        print(f"  [资金费率] 抓取异常: {e}")

    print(f"✅ 【{symbol}】 抓取流程结束。")


# ============================================================================
# 热门标的扫描与任务调度
# ============================================================================

def get_top_gainers_losers(exchange_name='binance', top_n=20, quote_currency='USDT'):
    """获取热门波动标的列表"""
    exchange = init_exchange(exchange_name, default_type='swap')
    try:
        exchange.load_markets()
        tickers = exchange.fetch_tickers()
    except Exception as e:
        print(f"[扫描失败] {e}")
        raise

    blacklist = {'XAU', 'XAG', 'CL', 'NG', 'NVDA', 'AMD', 'TSLA', 'AAPL', 'MSFT', 'META', 'GOOG', 'AMZN', 'COIN', 'SPX', 'QQQ'}
    candidates = []

    for symbol, ticker in tickers.items():
        if not symbol.endswith(f':{quote_currency}'):
            continue
        base = symbol.split('/')[0]
        if base in blacklist:
            continue

        pct = ticker.get('percentage')
        if pct is None: continue

        try:
            pct = float(pct)
            volume = float(ticker.get('quoteVolume') or 0)
        except:
            continue

        if volume < 1_000_000:
            continue

        candidates.append({"symbol": symbol, "percentage": pct, "volume": volume})

    gainers = sorted(candidates, key=lambda x:x['percentage'], reverse=True)[:top_n]
    losers = sorted(candidates, key=lambda x:x['percentage'])[:top_n]

    result, seen = [], set()
    for item in gainers + losers:
        if item['symbol'] not in seen:
            seen.add(item['symbol'])
            result.append(item['symbol'])

    return result

if __name__ == "__main__":
    LOOP_INTERVAL = 14400

    print("="*80)
    print(f"[系统提示] 数据将被独立存储于: 【{os.path.abspath(BACKTEST_DATA_DIR)}】")
    print(f"[系统提示] OI历史底座拉取路径: 【{os.path.abspath(VISION_DATA_DIR)}】")
    print("="*80)

    while True:
        try:
            top_symbols = get_top_gainers_losers('binance', 20, 'USDT') # 调整了默认抓取数量避免时间过长
        except Exception as e:
            print(f"[引擎调度] 无法嗅探标的列表，暂停重试中... | 错误: 【{e}】")
            time.sleep(60)
            continue

        for idx, sym in enumerate(top_symbols):
            try:
                fetch_independent_datasets(symbol=sym, days=365)
            except Exception as e:
                print(f"[任务异常] 标的 {sym} 严重崩溃跳过 | 错误: {e}")
                traceback.print_exc()

        print(f"\n[引擎调度] ✅ 当前轮次所有标的处理完毕 | 休眠倒计时: {LOOP_INTERVAL} 秒...")
        time.sleep(LOOP_INTERVAL)