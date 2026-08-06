"""
================================================================================
[核心数据流与功能摘要] 加密货币独立特征拉取引擎 (无聚合/无合并/保留原始时间)
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
# 通用工具与数据处理层
# ============================================================================

def init_exchange(exchange_name, default_type='swap'):
    config = {
        'enableRateLimit': True,
        'proxies': GLOBAL_PROXY,
    }
    if default_type:
        config['options'] = {'defaultType': default_type}
    return getattr(ccxt, exchange_name)(config)


def fetch_with_pagination(exchange, fetch_func, symbol, since, limit_per_request, timeframe=None):
    all_data = []
    current_since = since
    max_retries = 5

    print(f"  [API分页/准备] 标的: [{symbol}] | 预期拉取起点: {pd.to_datetime(since, unit='ms')} (TS: {since})")

    while True:
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                kwargs = {'since': current_since, 'limit': limit_per_request}
                if timeframe:
                    data = fetch_func(symbol, timeframe, **kwargs)
                else:
                    data = fetch_func(symbol, **kwargs)
                success = True
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    raise RuntimeError(f"[API分页/致命异常] 超过最大重试次数 | 标的: [{symbol}] | 报错: {e}") from e
                print(f"  ⚠️ [API分页/重试] 拉取异常 | 标的: [{symbol}] | 等待重试: ({retry_count}/{max_retries}) | 错误: {e}")
                time.sleep(3 * retry_count)

        if not data:
            break

        all_data.extend(data)

        last_item = data[-1]
        last_timestamp = int(last_item.get('timestamp', 0)) if isinstance(last_item, dict) else int(last_item[0])

        if not last_timestamp:
            break

        current_since = last_timestamp + 1

        if last_timestamp >= exchange.milliseconds() - 60000:
            break

        time.sleep(0.05)

    if all_data:
        first_ts = int(all_data[0].get('timestamp', 0)) if isinstance(all_data[0], dict) else int(all_data[0][0])
        last_ts = int(all_data[-1].get('timestamp', 0)) if isinstance(all_data[-1], dict) else int(all_data[-1][0])
        print(f"  [API分页/结束] 标的: [{symbol}] | 实际拉取总量: {len(all_data)} 条 | 范围: {pd.to_datetime(first_ts, unit='ms')} 至 {pd.to_datetime(last_ts, unit='ms')}")
    else:
        print(f"  [API分页/结束] 标的: [{symbol}] | 未拉取到新数据。")

    return all_data


def _get_resume_timestamp(out_path, exchange, days):
    if os.path.exists(out_path):
        old_df = pd.read_csv(out_path)
        if not old_df.empty and 'timestamp' in old_df.columns:
            return old_df, int(old_df['timestamp'].max()) + 1

    start_ms = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    return pd.DataFrame(), start_ms


def _merge_and_save(out_path, old_df, new_df, symbol, data_type, expected_interval_ms=None):
    if new_df.empty:
        return 0

    df = pd.concat([old_df, new_df], ignore_index=True)
    if df.empty:
        return 0

    original_len = len(df)

    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)
    df['timestamp'] = df['timestamp'].astype(int)

    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    dedup_len = len(df)
    if original_len > dedup_len:
        print(f"  🔍 [完整性/去重] 标的: [{symbol}-{data_type}] | 发现并清理了 {original_len - dedup_len} 条重复数据 (保留最新值)")

    df.sort_values('timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    if expected_interval_ms and len(df) > 1:
        time_diffs = df['timestamp'].diff().dropna()
        gaps = time_diffs[time_diffs > (expected_interval_ms + 10000)]

        if not gaps.empty:
            print(f"  🚨🚨🚨 [完整性警告/数据缺失] 标的: [{symbol}-{data_type}] | 严重警告: 发现 {len(gaps)} 处时间断层！")
            gap_indices = gaps.index
            for idx in gap_indices[:5]:
                gap_start = df.loc[idx-1, 'timestamp']
                gap_end = df.loc[idx, 'timestamp']
                missing_duration = (gap_end - gap_start) / 1000 / 60
                print(f"    🆘 缺失区间: {pd.to_datetime(gap_start, unit='ms')} -> {pd.to_datetime(gap_end, unit='ms')} (跨度: {missing_duration:.1f} 分钟)")
            if len(gaps) > 5:
                print(f"    🆘 ... (省略剩余 {len(gaps)-5} 处缺失日志，请重点检查)")
        else:
            print(f"  ✅ [完整性/连续] 标的: [{symbol}-{data_type}] | 时间序列连续，未发现数据缺失。")

    dt_series = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
    df['datetime_bj'] = dt_series.dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)

    df.to_csv(out_path, index=False)

    start_time_str = pd.to_datetime(df['timestamp'].iloc[0], unit='ms')
    end_time_str = pd.to_datetime(df['timestamp'].iloc[-1], unit='ms')
    print(f"  [落地统计] {data_type} 保存成功 | 总量: {len(df)} | 实际覆盖: {start_time_str} 至 {end_time_str}")

    return len(df)


# ============================================================================
# 数据拉取与落地引擎 (三大独立管线)
# ============================================================================

def sync_1m_klines(exchange, symbol, days=365):
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_1m_kline.csv")

    old_df, since = _get_resume_timestamp(out_path, exchange, days)
    mode_str = "增量补齐" if not old_df.empty else "全量初始化"

    raw_data = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, since, 1000, timeframe='1m')
    if not raw_data:
        print(f"  [1m K线/跳过] 当前已是最新状态 | 标的: [{symbol}] | 模式: [{mode_str}]")
        return

    new_df = pd.DataFrame(raw_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    total = _merge_and_save(out_path, old_df, new_df, symbol, "1m K线", expected_interval_ms=60000)
    print(f"  [1m K线/完结] 文件已更新 | 标的: [{symbol}] | 模式: [{mode_str}]")


def auto_download_vision_daily_oi(symbol, days=365):
    clean_symbol = symbol.split(':')[0].replace('/', '')
    end_date = pd.Timestamp.utcnow().floor('D') - pd.Timedelta(days=1)
    start_date = (pd.Timestamp.utcnow() - pd.Timedelta(days=days)).floor('D')

    print(f"  [Vision底座/准备] 标的: [{symbol}] | 预期同步区间: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}")

    all_dfs = []
    current_date = start_date
    success_count, cache_hit_count, fail_count = 0, 0, 0

    while current_date <= end_date:
        ymd_str = current_date.strftime('%Y-%m-%d')
        zip_filename = f"{clean_symbol}-metrics-{ymd_str}.zip"
        zip_path = os.path.join(VISION_DATA_DIR, zip_filename)

        df = None
        if os.path.exists(zip_path):
            try:
                with zipfile.ZipFile(zip_path) as z:
                    csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                    df = pd.read_csv(z.open(csv_name), low_memory=False)
                    cache_hit_count += 1
            except zipfile.BadZipFile:
                print(f"    ⚠️ [Vision底座/损坏] {ymd_str} 本地缓存破损，执行清理并重新拉取...")
                os.remove(zip_path)

        if df is None:
            daily_url = f"https://data.binance.vision/data/futures/um/daily/metrics/{clean_symbol}/{zip_filename}"
            retries = 3

            for attempt in range(1, retries + 1):
                try:
                    resp = requests.get(daily_url, proxies=GLOBAL_PROXY, timeout=15)
                    if resp.status_code == 200:
                        with open(zip_path, 'wb') as f:
                            f.write(resp.content)
                        with zipfile.ZipFile(zip_path) as z:
                            csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                            df = pd.read_csv(z.open(csv_name), low_memory=False)

                        print(f"    ⬇️ [Vision底座/下载成功] 标的: {clean_symbol} | 日期: {ymd_str}")
                        success_count += 1
                        time.sleep(0.2)
                        break
                    elif resp.status_code == 404:
                        fail_count += 1
                        break
                    else:
                        raise requests.RequestException(f"HTTP Code {resp.status_code}")
                except requests.RequestException as e:
                    if attempt == retries:
                        print(f"    ❌ [Vision底座/下载失败] 标的: {clean_symbol} | 日期: {ymd_str} | 已放弃重试 | 原因: {e}")
                        fail_count += 1
                    else:
                        time.sleep(1 * attempt)

        if df is not None and not df.empty:
            all_dfs.append(df)

        current_date += pd.Timedelta(days=1)

    print(f"  [Vision底座/统计] 标的: [{symbol}] | 命中缓存: {cache_hit_count} 天 | 新下载: {success_count} 天 | 缺失/失败: {fail_count} 天")

    if not all_dfs:
        return pd.DataFrame()

    merged_df = pd.concat(all_dfs, ignore_index=True)
    if 'create_time' in merged_df.columns and 'sum_open_interest' in merged_df.columns:
        merged_df.rename(columns={'create_time': 'timestamp', 'sum_open_interest': 'oi_amount'}, inplace=True)

        # [核心修复1]: 彻底解决 Pandas 2.0+ 将 string 转换导致除以 10**6 漂移到 1970 年的 BUG
        if pd.api.types.is_string_dtype(merged_df['timestamp']):
            # 强制按毫秒格式处理并直接生成正确的 int64
            merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp']).astype('datetime64[ms]').astype('int64')

        return merged_df[['timestamp', 'oi_amount']]

    return pd.DataFrame()


def sync_5m_oi(exchange, symbol, days=365):
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_5m_oi.csv")
    old_df = pd.read_csv(out_path) if os.path.exists(out_path) else pd.DataFrame()

    df_vision = auto_download_vision_daily_oi(symbol, days)

    # [核心修复2]: 增加币安 API 限制逻辑的防御
    now_ms = exchange.milliseconds()
    if not df_vision.empty:
        api_since = int(df_vision['timestamp'].max()) + 1
        print(f"  [5m OI/阶段1] Vision 底座加载成功 | 标的: [{symbol}] | 准备接力 API 增量...")
    else:
        api_since = now_ms - int(days * 24 * 60 * 60 * 1000)
        print(f"  [5m OI/阶段1] 无 Vision 底座支撑 | 标的: [{symbol}] | 退化为全量 API 拉取...")

    # 安全钳制：币安 openInterestHist 接口强制最多只能回溯 30 天
    max_api_history_ms = now_ms - int(29.5 * 24 * 60 * 60 * 1000)
    if api_since < max_api_history_ms:
        print(f"  ⚠️ [5m OI/安全限制] 预期起点 {pd.to_datetime(api_since, unit='ms')} 超出币安 API 的30天限制！")
        api_since = max_api_history_ms
        print(f"  ⚠️ [5m OI/安全限制] 强制重置 API 起点为: {pd.to_datetime(api_since, unit='ms')}")

    raw_oi = fetch_with_pagination(exchange, exchange.fetch_open_interest_history, symbol, api_since, 500,
                                   timeframe='5m')
    df_api = pd.DataFrame([{
        'timestamp': item.get('timestamp', 0),
        'oi_amount': float(item.get('openInterestAmount', 0)),
    } for item in raw_oi]) if raw_oi else pd.DataFrame()

    df_combined_new = pd.concat([df_vision, df_api], ignore_index=True)
    if df_combined_new.empty:
        print(f"  [5m OI/跳过] 无任何有效底层或增量数据 | 标的: [{symbol}]")
        return

    total = _merge_and_save(out_path, old_df, df_combined_new, symbol, "5m OI", expected_interval_ms=300000)
    print(f"  [5m OI/完结] 数据已落地 | 标的: [{symbol}]")


def sync_funding_rates(exchange, symbol, days=365):
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_funding_rates.csv")

    old_df, since = _get_resume_timestamp(out_path, exchange, days)
    mode_str = "增量补齐" if not old_df.empty else "全量初始化"

    raw_data = fetch_with_pagination(exchange, exchange.fetch_funding_rate_history, symbol, since, 1000, timeframe=None)

    if not raw_data:
        print(f"  [资金费率/跳过] 当前已是最新状态 | 标的: [{symbol}] | 模式: [{mode_str}]")
        return

    new_df = pd.DataFrame([{
        'timestamp': item.get('timestamp', 0),
        'funding_rate': item.get('fundingRate', 0),
    } for item in raw_data])

    total = _merge_and_save(out_path, old_df, new_df, symbol, "资金费率", expected_interval_ms=28800000)
    print(f"  [资金费率/完结] 数据已落地 | 标的: [{symbol}] | 模式: [{mode_str}]")


def fetch_independent_datasets(symbol, days=365):
    print("\n" + "=" * 80)
    print(f"🚀 [引擎触发] 开始执行独立特征抓取管线 | 标的: 【{symbol}】 | 回溯范围: 【{days}天】")

    exchange = init_exchange('binance')

    pipelines = [
        ('1m K线', sync_1m_klines),
        ('5m OI', sync_5m_oi),
        ('资金费率', sync_funding_rates)
    ]

    for name, func in pipelines:
        try:
            print(f"\n---> 启动子管线: {name}")
            func(exchange, symbol, days)
        except Exception as e:
            print(f"  ❌ [{name}/异常] 管线崩溃中止 | 标的: [{symbol}] | 错误明细: {e}")

    print(f"\n✅ [引擎流转] 【{symbol}】 所有可用管线执行完毕。")


# ============================================================================
# 热门标的扫描与任务调度 (入口层)
# ============================================================================

def get_top_gainers_losers(exchange_name='binance', top_n=20, quote_currency='USDT'):
    exchange = init_exchange(exchange_name, default_type='swap')
    try:
        exchange.load_markets()
        tickers = exchange.fetch_tickers()
    except Exception as e:
        raise RuntimeError(f"嗅探 CCXT 市场列表失败，请检查网络与代理配置。详情: {e}") from e

    blacklist = {'XAU', 'XAG', 'CL', 'NG', 'NVDA', 'AMD', 'TSLA', 'AAPL', 'MSFT', 'META', 'GOOG', 'AMZN', 'COIN', 'SPX',
                 'QQQ'}
    candidates = []

    for symbol, ticker in tickers.items():
        if not symbol.endswith(f':{quote_currency}'):
            continue

        base = symbol.split('/')[0]
        if base in blacklist:
            continue

        pct = ticker.get('percentage')
        if pct is None:
            continue

        try:
            pct = float(pct)
            volume = float(ticker.get('quoteVolume') or 0)
        except (ValueError, TypeError):
            continue

        if volume >= 1_000_000:
            candidates.append({"symbol": symbol, "percentage": pct})

    sorted_cands = sorted(candidates, key=lambda x: x['percentage'])
    gainers = sorted_cands[-top_n:]
    losers = sorted_cands[:top_n]

    result_set = {item['symbol'] for item in (gainers + losers)}
    return list(result_set)


if __name__ == "__main__":
    LOOP_INTERVAL = 14400

    print("=" * 80)
    print(f"🔧 [系统初始] 数据将被独立存储于: 【{os.path.abspath(BACKTEST_DATA_DIR)}】")
    print(f"🔧 [系统初始] OI历史底座缓存路径: 【{os.path.abspath(VISION_DATA_DIR)}】")
    print("=" * 80)

    while True:
        try:
            print("\n🔍 [全局调度] 开始执行新一轮标的嗅探...")
            top_symbols = get_top_gainers_losers('binance', 20, 'USDT')
        except Exception as e:
            print(f"❌ [全局调度/异常] 无法获取最新市场标的 | 可能原因: 网络断开或 API 熔断 | 等待 60 秒后重试... | 错误明细: 【{e}】")
            time.sleep(60)
            continue

        for sym in top_symbols:
            try:
                fetch_independent_datasets(symbol=sym, days=365)
            except Exception as e:
                print(f"❌ [任务总线/严重崩溃] 标的 {sym} 主进程崩溃已被跳过 | 错误明细: {e}")
                traceback.print_exc()

        print(f"\n💤 [全局调度] ✅ 当前轮次所有 {len(top_symbols)} 个标的处理完毕 | 进入休眠倒计时: 【{LOOP_INTERVAL} 秒】...")
        time.sleep(LOOP_INTERVAL)