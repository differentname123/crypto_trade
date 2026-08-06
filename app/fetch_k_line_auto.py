"""
================================================================================
[核心数据流与功能摘要] 加密货币独立特征拉取引擎 (无聚合/无合并/保留原始时间)
================================================================================
[功能摘要]
本模块突破了单一 API 的物理限流，独立拉取三大核心数据，并分别存储为独立文件：
1. 1m K线数据 (保留原始毫秒时间戳)
2. 5m 持仓量(OI)数据 (结合 Vision 底座 + API 增量，保留原始时间戳)
3. 历史已结算资金费率 (保留原始时间戳)

[输入数据]
- 动态嗅探的高优标的列表 (Symbol List)
- 本地现存的 CSV 历史数据 (用于推断断点续传的 since 时间戳)
- Binance Vision 历史每日 ZIP 数据压缩包 (OI 底座)

[数据流转/交互]
1. 调度器 -> 获取活跃标的 -> 循环下发任务。
2. 独立管线 -> 探活本地 CSV -> 获取最后一条记录的时间戳 -> 向 CCXT / Binance 发起增量拉取。
3. Pandas 管道 -> 拼接(旧数据 + 增量) -> 按时间戳去重 -> 排序 -> 追加易读北京时间。

[输出数据]
- 针对每个标的独立输出 3 份增量更新的 CSV 文件，不执行重采样，数据严格保真。
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
    """初始化 CCXT 交易所实例 (持有网络代理与默认合约类型)"""
    config = {
        'enableRateLimit': True,
        'proxies': GLOBAL_PROXY,
    }
    if default_type:
        config['options'] = {'defaultType': default_type}
    return getattr(ccxt, exchange_name)(config)


def fetch_with_pagination(exchange, fetch_func, symbol, since, limit_per_request, timeframe=None):
    """
    [分页增量拉取引擎] 增加了重试与详细的起止预期日志
    入参形貌: symbol, since(毫秒时间戳), limit_per_request(单次拉取量)
    出参形貌: list[dict/list] (CCXT 的原始响应结构，未解析)
    """
    all_data = []
    current_since = since
    max_retries = 5 # 增强重试次数保证完整性

    print(f"  [API分页/准备] 标的: [{symbol}] | 预期拉取起点: {pd.to_datetime(since, unit='ms')} (TS: {since})")

    while True:
        retry_count = 0
        success = False

        while retry_count < max_retries and not success:
            try:
                # 兼容带有 timeframe (K线/OI) 和不带 timeframe (资金费率) 的 API
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
                time.sleep(3 * retry_count) # 退避重试策略

        # 卫语句：查无增量，直接结束
        if not data:
            break

        all_data.extend(data)

        # 兼容 CCXT 两种常见返回格式 (字典列表 or 数组列表)
        last_item = data[-1]
        last_timestamp = int(last_item.get('timestamp', 0)) if isinstance(last_item, dict) else int(last_item[0])

        if not last_timestamp:
            break

        # 推进游标 (避免毫秒级重复抓取)
        current_since = last_timestamp + 1

        # 触达当前时间 (预留 1 分钟安全冗余)，结束拉取
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
    """提取本地旧文件的最新时间戳，推断增量拉取起点"""
    if os.path.exists(out_path):
        old_df = pd.read_csv(out_path)
        if not old_df.empty and 'timestamp' in old_df.columns:
            return old_df, int(old_df['timestamp'].max()) + 1

    # 无有效旧文件，返回全量拉取起点
    start_ms = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    return pd.DataFrame(), start_ms


def _merge_and_save(out_path, old_df, new_df, symbol, data_type, expected_interval_ms=None):
    """
    [数据落地引擎] 执行去重、排序、人性化时间追加与文件覆写
    【修改点】加入了极度严格的数据完整性校验（查重排缺失）
    """
    if new_df.empty:
        return 0

    df = pd.concat([old_df, new_df], ignore_index=True)
    if df.empty:
        return 0

    original_len = len(df)

    # 核心转换：剔除无效时间戳 -> 去重 -> 排序
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True)
    df['timestamp'] = df['timestamp'].astype(int)

    # 去重并记录重复数量
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    dedup_len = len(df)
    if original_len > dedup_len:
        print(f"  🔍 [完整性/去重] 标的: [{symbol}-{data_type}] | 发现并清理了 {original_len - dedup_len} 条重复数据 (保留最新值)")

    df.sort_values('timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 【完整性检测】检测时间断层（缺失数据）
    if expected_interval_ms and len(df) > 1:
        time_diffs = df['timestamp'].diff().dropna()
        # 容忍 1 秒的误差(主要针对某些奇怪的API毫秒偏离)，大于预期区间则视为断层
        gaps = time_diffs[time_diffs > (expected_interval_ms + 1000)]

        if not gaps.empty:
            print(f"  🚨🚨🚨 [完整性警告/数据缺失] 标的: [{symbol}-{data_type}] | 严重警告: 发现 {len(gaps)} 处时间断层！")
            gap_indices = gaps.index
            # 打印最明显的几处断层供人工核对
            for idx in gap_indices[:5]:
                gap_start = df.loc[idx-1, 'timestamp']
                gap_end = df.loc[idx, 'timestamp']
                missing_duration = (gap_end - gap_start) / 1000 / 60 # 转换为分钟
                print(f"    🆘 缺失区间: {pd.to_datetime(gap_start, unit='ms')} -> {pd.to_datetime(gap_end, unit='ms')} (跨度: {missing_duration:.1f} 分钟)")
            if len(gaps) > 5:
                print(f"    🆘 ... (省略剩余 {len(gaps)-5} 处缺失日志，请重点检查)")
        else:
            print(f"  ✅ [完整性/连续] 标的: [{symbol}-{data_type}] | 时间序列连续，未发现数据缺失。")

    # 追加人性化北京时间列 (不影响原 timestamp 数据流)
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
    """独立管线 1：拉取并保存 1 分钟级别 K 线"""
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_1m_kline.csv")

    old_df, since = _get_resume_timestamp(out_path, exchange, days)
    mode_str = "增量补齐" if not old_df.empty else "全量初始化"

    raw_data = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, since, 1000, timeframe='1m')
    if not raw_data:
        print(f"  [1m K线/跳过] 当前已是最新状态 | 标的: [{symbol}] | 模式: [{mode_str}]")
        return

    # K线标准 Shape
    new_df = pd.DataFrame(raw_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    # 1分钟 = 60 * 1000 ms = 60000
    total = _merge_and_save(out_path, old_df, new_df, symbol, "1m K线", expected_interval_ms=60000)
    print(f"  [1m K线/完结] 文件已更新 | 标的: [{symbol}] | 模式: [{mode_str}]")


def auto_download_vision_daily_oi(symbol, days=365):
    """
    底层依赖：从 Binance Vision 逐日拉取历史 OI 底座
    【修改点】增加了丰富的本地缓存与网络请求日志，并对单日下载增加了稳健的重试机制
    """
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
        # 1. 尝试读取本地缓存
        if os.path.exists(zip_path):
            try:
                with zipfile.ZipFile(zip_path) as z:
                    csv_name = [n for n in z.namelist() if n.endswith('.csv')][0]
                    df = pd.read_csv(z.open(csv_name), low_memory=False)
                    # 避免日志刷屏，仅以小标记形式输出
                    cache_hit_count += 1
            except zipfile.BadZipFile:
                print(f"    ⚠️ [Vision底座/损坏] {ymd_str} 本地缓存破损，执行清理并重新拉取...")
                os.remove(zip_path)  # 破损文件直接清理

        # 2. 本地无缓存，尝试远端拉取并固化 (带重试)
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
                        time.sleep(0.2)  # 简易限流防御
                        break
                    elif resp.status_code == 404:
                        # 404 代表当天交易所确实没生成数据，直接跳出重试
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
        # 清洗可能存在的字符串时间戳格式为毫秒级 int
        if pd.api.types.is_string_dtype(merged_df['timestamp']):
            merged_df['timestamp'] = pd.to_datetime(merged_df['timestamp']).astype('int64') // 10 ** 6
        return merged_df[['timestamp', 'oi_amount']]

    return pd.DataFrame()


def sync_5m_oi(exchange, symbol, days=365):
    """独立管线 2：拉取并保存 5 分钟级别 持仓量(OI)"""
    clean_symbol = symbol.replace('/', '_').replace(':', '_')
    out_path = os.path.join(BACKTEST_DATA_DIR, f"{clean_symbol}_5m_oi.csv")
    old_df = pd.read_csv(out_path) if os.path.exists(out_path) else pd.DataFrame()

    # 第一阶段：Vision 历史底座加载
    df_vision = auto_download_vision_daily_oi(symbol, days)

    # 第二阶段：推算 API 增量起点
    if not df_vision.empty:
        api_since = int(df_vision['timestamp'].max()) + 1
        print(f"  [5m OI/阶段1] Vision 底座加载成功 | 标的: [{symbol}] | 准备接力 API 增量...")
    else:
        api_since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
        print(f"  [5m OI/阶段1] 无 Vision 底座支撑 | 标的: [{symbol}] | 退化为全量 API 拉取...")

    # 第三阶段：API 增量抓取
    raw_oi = fetch_with_pagination(exchange, exchange.fetch_open_interest_history, symbol, api_since, 500,
                                   timeframe='5m')
    df_api = pd.DataFrame([{
        'timestamp': item.get('timestamp', 0),
        'oi_amount': float(item.get('openInterestAmount', 0)),
    } for item in raw_oi]) if raw_oi else pd.DataFrame()

    # 第四阶段：三方数据 (本地旧库 + Vision底层 + API增量) 融合洗牌并固化
    df_combined_new = pd.concat([df_vision, df_api], ignore_index=True)
    if df_combined_new.empty:
        print(f"  [5m OI/跳过] 无任何有效底层或增量数据 | 标的: [{symbol}]")
        return

    # 5分钟 = 5 * 60 * 1000 ms = 300000
    total = _merge_and_save(out_path, old_df, df_combined_new, symbol, "5m OI", expected_interval_ms=300000)
    print(f"  [5m OI/完结] 数据已落地 | 标的: [{symbol}]")


def sync_funding_rates(exchange, symbol, days=365):
    """独立管线 3：拉取并保存历史已结算资金费率"""
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

    # 资金费率一般为 8小时 (28800000 ms) 或 4小时。这里用 8h 校验，如遇到非标合约控制台也会正常打印缺失。
    total = _merge_and_save(out_path, old_df, new_df, symbol, "资金费率", expected_interval_ms=28800000)
    print(f"  [资金费率/完结] 数据已落地 | 标的: [{symbol}] | 模式: [{mode_str}]")


def fetch_independent_datasets(symbol, days=365):
    """
    [任务总控] 针对单一标的，依次驱动 3 个独立的特征抓取管线。
    采用严格的防雪崩隔离：单一管线失败不阻塞其他管线。
    """
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
            # 捕获并直白化输出错误，确保异常被拦截，管线继续流转
            print(f"  ❌ [{name}/异常] 管线崩溃中止 | 标的: [{symbol}] | 错误明细: {e}")

    print(f"\n✅ [引擎流转] 【{symbol}】 所有可用管线执行完毕。")


# ============================================================================
# 热门标的扫描与任务调度 (入口层)
# ============================================================================

def get_top_gainers_losers(exchange_name='binance', top_n=20, quote_currency='USDT'):
    """获取高波动且高流通量的过滤版目标列表"""
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

    # 按涨跌幅分别截取两端，再聚合去重
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
            print(
                f"❌ [全局调度/异常] 无法获取最新市场标的 | 可能原因: 网络断开或 API 熔断 | 等待 60 秒后重试... | 错误明细: 【{e}】")
            time.sleep(60)
            continue

        for sym in top_symbols:
            try:
                fetch_independent_datasets(symbol=sym, days=365)
            except Exception as e:
                print(f"❌ [任务总线/严重崩溃] 标的 {sym} 主进程崩溃已被跳过 | 错误明细: {e}")
                traceback.print_exc()

        print(
            f"\n💤 [全局调度] ✅ 当前轮次所有 {len(top_symbols)} 个标的处理完毕 | 进入休眠倒计时: 【{LOOP_INTERVAL} 秒】...")
        time.sleep(LOOP_INTERVAL)