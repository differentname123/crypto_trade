"""
================================================================================
核心数据流摘要
================================================================================
[功能摘要]
加密货币合约市场异构数据（K线、OI、CVD、资金费率）采集、对齐与增量合并管线。

[输入数据]
- 来源: Binance / OKX 交易所 (通过 ccxt 及币安 fAPI 私有接口)。
- 载荷: Symbol (如 'BTC/USDT:USDT')、Timeframe (如 '5m')、拉取天数 (days)。

[数据流转/交互]
1. 并发分页拉取: 根据交易所限频自动循环分页，拉取 标准K线、历史持仓量(OI)、买卖量(用于计算CVD)、溢价指数K线。
2. 数据清洗与对齐:
   - 提取所需索引切片，抹平 JSON 弱类型引发的格式问题。
   - 强转时间戳为北京时间 (Asia/Shanghai) 并抹除毫秒误差。
3. 衍生计算(无未来函数):
   - OI截面变化率(oi_amount_change_pct)
   - 累计成交量Delta(cvd)
   - 预测资金费率(predicted_funding_rate)
4. 增量矩阵合并: 基于时间戳(timestamp)进行 Inner Join，并将增量数据覆盖或追加到本地历史 CSV 中。

[输出数据]
- Shape: 包含 [timestamp, open, high, low, close, volume, oi_amount, oi_amount_change_pct, cvd, premium_close, predicted_funding_rate] 的 pandas DataFrame。
- 副作用: 数据持久化写入本地 CSV，供 LER 量化模型直接读取。
================================================================================
"""

import os
import time
import ccxt
import pandas as pd

# ============================================================================
# 全局配置常量
# ============================================================================
DEFAULT_PROXY = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

EXCHANGE_LIMITS = {
    'binance': 1000,
    'okx': 100
}


# ============================================================================
# 工具函数层
# ============================================================================

def init_exchange(exchange_name, default_type='swap'):
    """
    初始化 CCXT 交易所实例。
    出参 Shape: CCXT Exchange Object
    """
    config = {
        'enableRateLimit': True,
        'proxies': DEFAULT_PROXY,
    }
    if default_type:
        config['options'] = {'defaultType': default_type}

    return getattr(ccxt, exchange_name)(config)


def convert_to_beijing_time(df, timestamp_col='timestamp'):
    """
    清洗并将毫秒时间戳转换为无时区标记的北京时间，抹平毫秒误差以便于 Inner Join。
    入参 Shape: 必须包含 timestamp_col 的 DataFrame。
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    df = df.copy()
    # 转换为 DateTime，强制转为 UTC 后再转东八区，最后剥离时区信息
    dt_series = pd.to_datetime(df[timestamp_col], unit='ms', errors='coerce')
    dt_series = dt_series.dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)

    # 强制抹平毫秒误差，保证后续 merge 匹配无懈可击
    df[timestamp_col] = dt_series.dt.round('s')

    # 清理因转换失败产生的 NaT 脏数据
    df.dropna(subset=[timestamp_col], inplace=True)
    return df


def fetch_with_pagination(exchange, fetch_func, symbol, timeframe, since, limit_per_request):
    """
    通用的标准 CCXT 历史数据分页引擎。
    出参 Shape: List[Dict] 或 List[List] (取决于 ccxt 原生底层实现)
    """
    all_data = []
    current_since = since

    while True:
        try:
            # 兼容带有 timeframe 或不带 timeframe 的 CCXT 接口
            if timeframe:
                data = fetch_func(symbol, timeframe, since=current_since, limit=limit_per_request)
            else:
                data = fetch_func(symbol, since=current_since, limit=limit_per_request)

            if not data:
                break

            all_data.extend(data)

            # 适配 ccxt 不同的数据返回结构 (K线是列表，OI通常是字典)
            last_timestamp = int(data[-1].get('timestamp', 0)) if isinstance(data[0], dict) else int(data[-1][0])

            if not last_timestamp:
                print(f"[通用分页拉取] 异常熔断 | 标的: 【{symbol}】 | 错误原因: 【尾部数据缺失时间戳，无法推进游标】")
                break

            current_since = last_timestamp + 1

            # FIXME: 原逻辑在此处直接丢弃了最近60秒内的数据。保留原业务逻辑边界，不作干预。
            if last_timestamp >= exchange.milliseconds() - 60000:
                break

            time.sleep(0.05)

        except Exception as e:
            # FIXME: 原代码在此处 catch 异常后直接 break 返回 all_data，会导致静默吞噬网络错误并生成残缺数据集。
            # 现改为打印人类可读错误并强制抛出，通过中断阻断脏数据落盘。
            err_msg = f"接口请求中途网络异常或触发流控 | 游标: 【{current_since}】 | 底层报错: {str(e)}"
            print(f"[通用分页拉取] 致命异常 | 标的: 【{symbol}】 | 失败原因: 【{err_msg}】")
            raise RuntimeError(err_msg) from e

    return all_data


def _fetch_binance_fapi_paginated(exchange, endpoint_func, raw_symbol, timeframe_id, since):
    """
    专属币安私有接口 (fapiPublicGetXXX) 的分页拉取引擎，消除散落各处的冗余循环。
    出参 Shape: List[List] 原始币安 K 线数组结构
    """
    all_data = []
    current_since = since

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

            time.sleep(0.05)

        except Exception as e:
            err_msg = f"币安私有接口拉取崩溃 | 游标: 【{current_since}】 | 详情: {str(e)}"
            print(f"[私有API分页] 致命异常 | 标的: 【{raw_symbol}】 | 失败原因: 【{err_msg}】")
            raise RuntimeError(err_msg) from e

    return all_data


# ============================================================================
# 数据拉取层 (业务指标构建)
# ============================================================================

def fetch_long_history(exchange_name, symbol, timeframe='1h', days=30):
    exchange = init_exchange(exchange_name, default_type=None)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    limit = EXCHANGE_LIMITS.get(exchange_name, 100)

    try:
        raw_data = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, timeframe, since, limit)
    except Exception:
        return pd.DataFrame()

    if not raw_data:
        print(f"[历史K线] 拉取结果为空 | 标的: 【{symbol}】 | 参数: 【{days}天】")
        return pd.DataFrame()

    df = pd.DataFrame(raw_data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = convert_to_beijing_time(df)

    print(f"[历史K线] 拉取成功 | 标的: 【{symbol}】 | 结果: 【共 {len(df)} 条数据】")
    return df


def fetch_historical_oi(exchange, symbol, timeframe='1h', days=30):
    """
    拉取历史持仓量 (OI) 数据并计算无未来函数的变化率特征。
    """
    if not exchange.has.get('fetchOpenInterestHistory'):
        print(f"[历史OI] 功能不受支持 | 标的: 【{symbol}】 | 结果: 【跳过，返回空】")
        return pd.DataFrame()

    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    limit = 500 if exchange.id == 'binance' else 100

    try:
        raw_oi = fetch_with_pagination(exchange, exchange.fetch_open_interest_history, symbol, timeframe, since, limit)
    except Exception:
        return pd.DataFrame()

    if not raw_oi:
        print(f"[历史OI] 拉取结果为空 | 标的: 【{symbol}】 | 参数: 【{days}天】")
        return pd.DataFrame()

    df = pd.DataFrame([{
        'timestamp': item.get('timestamp', 0),
        'oi_amount': item.get('openInterestAmount', 0),
    } for item in raw_oi])

    df = convert_to_beijing_time(df)
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values(by='timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 核心业务特征：当前时刻 T 记录的是 [T-1, T] 的持仓变化率（无未来函数）
    df['oi_amount_change_pct'] = (df['oi_amount'].pct_change() * 100).round(4)
    df.dropna(subset=['oi_amount_change_pct'], inplace=True)

    print(f"[历史OI] 拉取并清洗成功 | 标的: 【{symbol}】 | 结果: 【共 {len(df)} 条特征】")
    return df


def fetch_binance_cvd_history(symbol, timeframe='1h', days=30):
    exchange = init_exchange('binance')

    try:
        exchange.load_markets()
        raw_symbol = exchange.market(symbol)['id']
    except Exception as e:
        print(f"[CVD解析] 无法解析底层交易对ID | 标的: 【{symbol}】 | 错误: 【{e}】")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)

    try:
        raw_klines = _fetch_binance_fapi_paginated(
            exchange, exchange.fapiPublicGetKlines, raw_symbol, timeframe_id, since
        )
    except Exception:
        return pd.DataFrame()

    if not raw_klines:
        print(f"[CVD数据] 拉取结果为空 | 标的: 【{symbol}】")
        return pd.DataFrame()

    # 原生直接切片，抛弃无用数据降低内存开销。
    # 索引对应: 0:timestamp, 5:volume, 9:taker_buy_base_vol
    df = pd.DataFrame(raw_klines).iloc[:, [0, 5, 9]]
    df.columns = ['timestamp', 'volume', 'taker_buy_base_vol']

    # 防御性类型强转
    df = df.apply(pd.to_numeric, errors='coerce')
    df.dropna(inplace=True)

    # 业务逻辑计算：衍生 CVD
    df['taker_sell_base_vol'] = df['volume'] - df['taker_buy_base_vol']
    df['volume_delta'] = df['taker_buy_base_vol'] - df['taker_sell_base_vol']
    df['cvd'] = df['volume_delta'].cumsum()

    df = convert_to_beijing_time(df)

    print(f"[CVD数据] 衍生计算完成 | 标的: 【{symbol}】 | 结果: 【共 {len(df)} 条数据】")
    return df[['timestamp', 'cvd']]


def fetch_premium_index_klines(symbol, timeframe='5m', days=30):
    exchange = init_exchange('binance')

    try:
        exchange.load_markets()
        raw_symbol = exchange.market(symbol)['id']
    except Exception as e:
        print(f"[溢价指数] 无法解析底层交易对ID | 标的: 【{symbol}】 | 错误: 【{e}】")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)

    try:
        raw_klines = _fetch_binance_fapi_paginated(
            exchange, exchange.fapiPublicGetPremiumIndexKlines, raw_symbol, timeframe_id, since
        )
    except Exception:
        return pd.DataFrame()

    if not raw_klines:
        print(f"[溢价指数] 拉取结果为空 | 标的: 【{symbol}】")
        return pd.DataFrame()

    # 原生切片提取核心要素 0:timestamp, 4:premium_close
    df = pd.DataFrame(raw_klines).iloc[:, [0, 4]]
    df.columns = ['timestamp', 'premium_close']

    df = df.apply(pd.to_numeric, errors='coerce')
    df.dropna(inplace=True)
    df = convert_to_beijing_time(df)

    # FIXME: 保留原硬编码业务逻辑推导资金费率
    df['predicted_funding_rate'] = (df['premium_close'] + 0.0001) * 100

    print(f"[溢价指数] 处理完成 | 标的: 【{symbol}】 | 结果: 【共 {len(df)} 条数据】")
    return df[['timestamp', 'premium_close', 'predicted_funding_rate']]


# ============================================================================
# 聚合管线引擎：合并异构数据
# ============================================================================

def prepare_ler_backtest_data(symbol, timeframe='5m', days=29.5, save_dir='./data'):
    """
    聚合引擎：执行各维度数据的抓取，并通过 Inner Join 确保时序对齐，落地为最终特征宽表。
    基于本地已存在的数据时间戳，智能计算增量请求天数，避免重复拉取。
    """
    print("\n" + "=" * 80)
    print(f"[管线启动] 正在构建 LER 回测矩阵 | 标的: 【{symbol}】 | 基础回溯: 【{days}天】")

    file_path = os.path.join(save_dir, f"{symbol.replace('/', '_').replace(':', '_')}_{timeframe}_ler_data.csv")
    local_df = pd.DataFrame()
    pull_days = days

    # 1. 检测本地历史数据并计算增量时间
    if os.path.exists(file_path):
        try:
            local_df = pd.read_csv(file_path)
            local_df['timestamp'] = pd.to_datetime(local_df['timestamp'])

            last_local_ts = local_df['timestamp'].max()
            now_bj = pd.Timestamp.now(tz='Asia/Shanghai').tz_localize(None)

            # 默认冗余1天数据进行交叉覆盖，防患极少数数据断层
            delta_days = (now_bj - last_local_ts).total_seconds() / (24 * 3600)
            pull_days = max(delta_days + 1.0, 1.0)

            print(f"[增量检测] 发现本地快照 | 快照时间: 【{last_local_ts}】 | 调整拉取天数: 【{pull_days:.2f}天】")
        except Exception as e:
            print(f"[增量检测] 本地文件损坏或解析失败，降级为全量拉取 | 错误: 【{e}】")
            local_df = pd.DataFrame()

    # 2. 并行/顺序执行数据获取
    exchange_name = 'binance'
    exchange_inst = init_exchange(exchange_name)

    df_klines = fetch_long_history(exchange_name, symbol, timeframe, pull_days)
    df_oi = fetch_historical_oi(exchange_inst, symbol, timeframe, pull_days)
    df_cvd = fetch_binance_cvd_history(symbol, timeframe, pull_days)
    df_premium = fetch_premium_index_klines(symbol, timeframe, pull_days)

    # 卫语句拦截：任意维度缺失即中断本次合并，防止产出脏矩阵
    if df_klines.empty or df_oi.empty or df_cvd.empty or df_premium.empty:
        print(f"[管线合并] 【失败】存在关键维度数据缺失，放弃写入本地 | 标的: 【{symbol}】")
        return None

    # 3. 对齐合并
    print(f"[管线合并] 正在执行 Inner Join 时序对齐...")
    df_merged = (
        df_klines
        .merge(df_oi[['timestamp', 'oi_amount', 'oi_amount_change_pct']], on='timestamp', how='inner')
        .merge(df_cvd[['timestamp', 'cvd']], on='timestamp', how='inner')
        .merge(df_premium[['timestamp', 'premium_close', 'predicted_funding_rate']], on='timestamp', how='inner')
    )

    # 4. 增量整合与去重写入
    if not local_df.empty:
        df_merged['timestamp'] = pd.to_datetime(df_merged['timestamp'])
        df_merged = pd.concat([local_df, df_merged])
        df_merged = df_merged.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
        print(f"[管线合并] 增量数据拼装完毕 | 标的: 【{symbol}】 | 总量扩容至: 【{len(df_merged)} 条】")

    os.makedirs(save_dir, exist_ok=True)
    df_merged.to_csv(file_path, index=False)
    print(f"[持久化] 数据已安全落盘 | 路径: 【{file_path}】\n")

    return df_merged


def get_top_volume_symbols(exchange_name='binance', top_n=100, quote_currency='USDT'):
    """
    通过交易所 Ticker 接口筛选出成交量最大的 N 个合约。
    出参 Shape: List[String] (如 ['BTC/USDT:USDT', 'ETH/USDT:USDT'])
    """
    exchange = init_exchange(exchange_name, default_type='swap')

    try:
        exchange.load_markets()
        tickers = exchange.fetch_tickers()
    except Exception as e:
        print(f"[标的嗅探] 获取 Tickers 列表时遭遇致命网络异常 | 错误: 【{e}】")
        raise

    target_symbols = [
        ticker for symbol, ticker in tickers.items()
        if symbol.endswith(f':{quote_currency}')
    ]

    # 按 quoteVolume 降序，安全处理空值
    sorted_tickers = sorted(
        target_symbols,
        key=lambda x: float(x.get('quoteVolume') or 0),
        reverse=True
    )

    return [t['symbol'] for t in sorted_tickers[:top_n]]


if __name__ == "__main__":
    LOOP_INTERVAL = 3600  # 建议休眠时长，防止过度榨取 API 限频额度

    while True:
        print("\n" + "="*80)
        print(f"🔄 [引擎调度] 开始新一轮全量扫描调度 | 当前时间: 【{pd.Timestamp.now()}】")
        print("="*80)

        try:
            top_symbols = get_top_volume_symbols('binance', 100, 'USDT')
            print(f"[调度中心] 核心池锁定完毕 | 热度最高标的预览: 【{top_symbols[:5]}...】")
        except Exception:
            print("[调度中心] 【失败】由于网络原因无法获取当前标的池，进入短暂重试等待...")
            time.sleep(60)
            continue

        for idx, sym in enumerate(top_symbols):
            print(f"\n[任务派发] 进度: 【{idx+1}/100】 | 目标标的: 【{sym}】")
            try:
                # 引擎全量托管，无抛错则安全推进
                prepare_ler_backtest_data(symbol=sym, timeframe='5m', days=30)
            except Exception as e:
                print(f"[任务异常] 标的管线未捕获的致命错误 | 标的: 【{sym}】 | 错误详情: 【{e}】")

        print(f"\n✅ [引擎调度] 本轮池化数据更新完毕 |  即将休眠: 【{LOOP_INTERVAL} 秒】")
        time.sleep(LOOP_INTERVAL)