"""
================================================================================
核心数据流摘要
================================================================================
[功能摘要]
加密货币合约市场数据分析系统，支持多交易所(Binance/OKX)的历史数据拉取、
多维度波动率筛选、订单流(CVD)/持仓量(OI)/资金费率分析，并生成多种交易信号。

[输入数据]
- 交易所API: 通过ccxt库连接Binance/OKX，获取OHLCV、OI、Funding Rate、Ticker等数据
- 配置参数: 交易对、时间周期、历史天数、并发线程数等
- 本地缓存: CSV文件存储历史数据以支持增量更新

[数据流转/交互]
1. 数据拉取层: 分页拉取历史数据 → UTC时间戳 → 北京时间转换
2. 市场筛选层: 24h振幅初筛 → 波动率排行 → 锁定高波动标的
3. 信号分析层: CVD+OI+Funding多维合并 → 量价背离检测 → 信号强度评分
4. 数据持久层: 增量比对 → CSV追加/覆盖保存

[输出数据]
- 波动率排行榜 (DataFrame)
- 交易信号数据集 (含信号类型、强度、理由的DataFrame)
- 本地CSV缓存文件 (支持7x24h增量更新)
================================================================================
"""

from pathlib import Path
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import ccxt
import numpy as np
import pandas as pd

# ============================================================================
# 全局配置常量
# ============================================================================
DEFAULT_PROXY = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

DATA_DIR = Path(r"W:\project\python_project\crypto_trade\data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================================
# 工具函数层
# ============================================================================

def init_exchange(exchange_name, default_type='swap'):
    """
    初始化交易所实例，统一配置代理和速率限制。

    核心作用: 避免在每个函数中重复配置交易所参数
    """
    exchange_class = getattr(ccxt, exchange_name)
    config = {
        'enableRateLimit': True,
        'proxies': DEFAULT_PROXY,
    }
    if default_type:
        config['options'] = {'defaultType': default_type}

    return exchange_class(config)


def convert_to_beijing_time(df, timestamp_col='timestamp'):
    """
    统一的时间转换函数：毫秒时间戳 → UTC → 北京时间(无时区标记)

    核心作用: 消除各函数中重复的时间转换逻辑
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='ms')
    df[timestamp_col] = df[timestamp_col].dt.tz_localize('UTC')
    df[timestamp_col] = df[timestamp_col].dt.tz_convert('Asia/Shanghai')
    df[timestamp_col] = df[timestamp_col].dt.tz_localize(None)

    return df


def fetch_with_pagination(exchange, fetch_func, symbol, timeframe, since, limit_per_request):
    """
    通用分页拉取函数，支持重试机制。

    核心作用: 统一处理分页逻辑，避免代码重复

    入参形貌:
    - fetch_func: 交易所的拉取方法(如 fetch_ohlcv, fetch_open_interest_history)
    - limit_per_request: 单次请求的最大条数

    出参形貌:
    - list: 所有拉取到的原始数据列表
    """
    all_data = []
    current_since = since

    while True:
        try:
            if timeframe:
                data = fetch_func(symbol, timeframe, since=current_since, limit=limit_per_request)
            else:
                data = fetch_func(symbol, since=current_since, limit=limit_per_request)

            if not data:
                break

            all_data.extend(data)

            # 更新游标 - 兼容dict和list两种数据格式
            if isinstance(data[0], dict):
                last_timestamp = data[-1].get('timestamp', 0)
            else:
                last_timestamp = data[-1][0]

            current_since = last_timestamp + 1

            # 终止条件：已拉取到最新数据
            if last_timestamp >= exchange.milliseconds() - 60000:
                break

        except Exception as e:
            print(f"[分页拉取] 出错 | 标的: [{symbol}] | 错误: [{e}]")
            break

    return all_data


# ============================================================================
# 数据拉取层
# ============================================================================

def fetch_long_history(exchange_name, symbol, timeframe='1h', days=30):
    """
    分页拉取长历史K线数据并转换为北京时间。

    [功能摘要]
    从指定交易所拉取过去N天的OHLCV数据，自动处理分页和时间转换。

    [输入数据]
    - exchange_name: 交易所名称(如 'binance', 'okx')
    - symbol: 交易对(如 'BTC/USDT:USDT')
    - timeframe: K线周期
    - days: 拉取的历史天数

    [数据流转]
    API分页拉取 → 原始时间戳 → UTC → 北京时间

    [输出数据]
    DataFrame: 包含 timestamp, open, high, low, close, volume 列
    """
    exchange = init_exchange(exchange_name, default_type=None)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

    print(f"[历史K线] 开始拉取 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}]")

    # 币安 limit 最大 1000，欧易 limit 最大 100
    limit = 1000 if exchange_name == 'binance' else 100

    all_ohlcv = fetch_with_pagination(
        exchange, exchange.fetch_ohlcv, symbol, timeframe, since, limit
    )

    if not all_ohlcv:
        print(f"[历史K线] 拉取失败或无数据 | 标的: [{symbol}]")
        return pd.DataFrame()

    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = convert_to_beijing_time(df)

    print(f"[历史K线] 拉取完成 | 标的: [{symbol}] | 数据量: [{len(df)}] 条")
    return df


def fetch_long_funding_history(exchange_name, symbol, days=30):
    """
    分页拉取资金费率历史并转换为北京时间。

    [功能摘要]
    拉取指定合约的资金费率历史数据，计算费率变化百分比。

    [输入数据]
    - exchange_name: 交易所名称
    - symbol: 交易对
    - days: 拉取的历史天数

    [数据流转]
    API分页拉取 → 费率提取 → 时间转换 → 变化率计算

    [输出数据]
    DataFrame: 包含 timestamp, funding_rate, funding_rate_pct 列
    """
    exchange = init_exchange(exchange_name, default_type=None)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

    print(f"[资金费率] 开始拉取 | 交易所: [{exchange_name}] | 标的: [{symbol}]")

    limit = 1000 if exchange_name == 'binance' else 100

    all_funding = fetch_with_pagination(
        exchange, exchange.fetch_funding_rate_history, symbol, None, since, limit
    )

    if not all_funding:
        print(f"[资金费率] 拉取失败或无数据 | 标的: [{symbol}]")
        return pd.DataFrame(columns=['timestamp', 'funding_rate', 'funding_rate_pct'])

    df = pd.DataFrame([{
        'timestamp': item['timestamp'],
        'funding_rate': item.get('fundingRate', 0.0) * 100
    } for item in all_funding])

    # 时间转换并抹平毫秒级误差
    df = convert_to_beijing_time(df)
    df['timestamp'] = df['timestamp'].dt.round('s')

    # 计算费率变化百分比
    df['funding_rate_pct'] = df['funding_rate'].pct_change() * 100
    df = df.sort_values('timestamp').reset_index(drop=True)

    print(f"[资金费率] 拉取完成 | 标的: [{symbol}] | 数据量: [{len(df)}] 条")
    return df


def fetch_historical_oi(exchange_name, symbol, timeframe='1h', days=30):
    """
    分页拉取历史持仓量(OI)数据，计算涨跌幅。

    [功能摘要]
    拉取历史OI数据，计算数量和价值的变化百分比。

    [输入数据]
    - exchange_name: 交易所名称
    - symbol: 交易对
    - timeframe: 时间周期
    - days: 拉取的历史天数

    [数据流转]
    API分页拉取 → 时间转换 → 去重 → 涨跌幅计算

    [输出数据]
    DataFrame: 包含 timestamp, oi_amount, oi_value, oi_amount_change_pct, oi_value_change_pct 列
    """
    exchange = init_exchange(exchange_name, default_type=None)

    if not exchange.has.get('fetchOpenInterestHistory'):
        print(f"[历史OI] 不支持 | 交易所: [{exchange_name}] 不支持历史OI查询")
        return pd.DataFrame()

    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

    print(f"[历史OI] 开始拉取 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}]")

    limit = 500 if exchange_name == 'binance' else 100

    all_oi = fetch_with_pagination(
        exchange, exchange.fetch_open_interest_history, symbol, timeframe, since, limit
    )

    if not all_oi:
        print(f"[历史OI] 拉取失败或无数据 | 标的: [{symbol}]")
        return pd.DataFrame()

    df = pd.DataFrame([{
        'timestamp': item['timestamp'],
        'oi_amount': item.get('openInterestAmount', 0),
        'oi_value': item.get('openInterestValue', 0)
    } for item in all_oi])

    # 时间转换
    df = convert_to_beijing_time(df)

    # 去重
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values(by='timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 计算涨跌幅
    df['oi_amount_change_pct'] = df['oi_amount'].pct_change() * 100

    # 防御性编程：处理 oi_value 全为0的情况
    if df['oi_value'].sum() > 0:
        df['oi_value_change_pct'] = df['oi_value'].pct_change() * 100
    else:
        df['oi_value_change_pct'] = 0.0

    # 填充NaN并格式化
    df.fillna({'oi_amount_change_pct': 0, 'oi_value_change_pct': 0}, inplace=True)
    df['oi_amount_change_pct'] = df['oi_amount_change_pct'].round(4)
    df['oi_value_change_pct'] = df['oi_value_change_pct'].round(4)

    print(f"[历史OI] 拉取完成 | 标的: [{symbol}] | 数据量: [{len(df)}] 条")
    return df


def fetch_binance_cvd_history(symbol, timeframe='1h', days=30):
    """
    分页拉取币安U本位合约的CVD数据。

    [功能摘要]
    通过币安私有API拉取K线数据，计算CVD(累计成交量Delta)。

    [输入数据]
    - symbol: 交易对
    - timeframe: K线周期
    - days: 拉取的历史天数

    [数据流转]
    解析Symbol → API分页拉取 → 数值清洗 → CVD计算 → 时间转换

    [输出数据]
    DataFrame: 包含 timestamp, OHLCV, taker买卖量, volume_delta, cvd 列
    """
    exchange = init_exchange('binance')

    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        raw_symbol = market['id']
    except Exception as e:
        print(f"[CVD数据] Symbol解析失败 | 标的: [{symbol}] | 错误: [{e}]")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

    print(f"[CVD数据] 开始拉取 | 标的: [{symbol}] | 天数: [{days}]")

    all_klines = []
    current_since = since

    while True:
        try:
            params = {
                'symbol': raw_symbol,
                'interval': timeframe_id,
                'startTime': current_since,
                'limit': 1000
            }
            curr_klines = exchange.fapiPublicGetKlines(params)

            if not curr_klines:
                break

            all_klines.extend(curr_klines)
            last_timestamp = int(curr_klines[-1][0])
            current_since = last_timestamp + 1

            if last_timestamp >= exchange.milliseconds() - 60000:
                break

        except Exception as e:
            print(f"[CVD数据] 拉取出错 | 标的: [{symbol}] | 错误: [{e}]")
            break

    if not all_klines:
        print(f"[CVD数据] 拉取失败或无数据 | 标的: [{symbol}]")
        return pd.DataFrame()

    columns = [
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'trades',
        'taker_buy_base_vol', 'taker_buy_quote_vol', 'ignore'
    ]
    df = pd.DataFrame(all_klines, columns=columns)

    # 数据清洗
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'taker_buy_base_vol', 'taker_buy_quote_vol']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=['timestamp'] + numeric_cols)

    # CVD核心计算
    df['taker_sell_base_vol'] = df['volume'] - df['taker_buy_base_vol']
    df['volume_delta'] = df['taker_buy_base_vol'] - df['taker_sell_base_vol']
    df['cvd'] = df['volume_delta'].cumsum()

    # 时间转换
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
    df = df.dropna(subset=['timestamp'])

    # 调用统一的时间转换函数
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Shanghai')
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)

    result_df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'taker_buy_base_vol', 'taker_sell_base_vol', 'volume_delta', 'cvd']]

    print(f"[CVD数据] 拉取完成 | 标的: [{symbol}] | 数据量: [{len(result_df)}] 条")
    return result_df


def get_open_interest(symbol, exchange_name='binance'):
    """
    获取指定合约的实时持仓量(OI)。

    [功能摘要]
    通过统一API获取实时OI数据。

    [输入数据]
    - symbol: 交易对
    - exchange_name: 交易所名称

    [输出数据]
    dict: 包含OI信息，失败返回None
    """
    exchange = init_exchange(exchange_name, default_type=None)

    print(f"[实时OI] 开始拉取 | 交易所: [{exchange_name}] | 标的: [{symbol}]")

    try:
        if exchange.has.get('fetchOpenInterest'):
            oi_data = exchange.fetch_open_interest(symbol)

            base_volume = oi_data.get('openInterestAmount')
            quote_value = oi_data.get('openInterestValue')

            print(f"[实时OI] 拉取成功 | 标的: [{symbol}] | 数量: [{base_volume}] | 价值: [{quote_value}] USDT")

            return {
                'Symbol': symbol,
                'Exchange': exchange_name.upper(),
                'OI (Base Coin)': base_volume,
                'OI Value (USDT)': quote_value,
                'Timestamp': oi_data.get('timestamp'),
                'Datetime': oi_data.get('datetime')
            }
        else:
            print(f"[实时OI] 不支持 | 交易所: [{exchange_name}] 不支持OI查询")
            return None

    except Exception as e:
        print(f"[实时OI] 拉取失败 | 标的: [{symbol}] | 错误: [{e}]")
        return None


# ============================================================================
# 市场筛选层
# ============================================================================

def get_volatility_ranking(exchange_name, minutes_list=[15, 30, 60], max_workers=20, top_n=800):
    """
    获取交易所U本位永续合约的多维度平均分钟波动率及资金费率。

    [功能摘要]
    通过24h振幅初筛 → 多线程并发拉取K线 → 计算多维度波动率排行。

    [输入数据]
    - exchange_name: 交易所名称('binance' 或 'okx')
    - minutes_list: 需要计算的分钟数列表
    - max_workers: 并发线程数
    - top_n: 初筛保留的前N个高波动合约

    [数据流转]
    加载市场 → Ticker初筛 → 资金费率拉取 → K线并发拉取 → 波动率计算 → 排序

    [输出数据]
    DataFrame: 包含Symbol, Latest Price, Funding Rate %, 各维度波动率 列
    """
    if isinstance(minutes_list, int):
        minutes_list = [minutes_list]

    max_minutes = max(minutes_list)

    print(f"\n[波动率排行] 开始分析 | 交易所: [{exchange_name.upper()}] | 时间维度: {minutes_list} | 线程数: [{max_workers}]")

    exchange = init_exchange(exchange_name)
    markets = exchange.load_markets()

    # 过滤活跃U本位永续合约
    # 币安使用 quote=='USDT'，OKX使用 settle=='USDT'
    if exchange_name == 'binance':
        symbols = [
            symbol for symbol, market in markets.items()
            if market.get('active') and market.get('linear')
            and market.get('quote') == 'USDT' and market.get('type') == 'swap'
        ]
    else:  # okx
        symbols = [
            symbol for symbol, market in markets.items()
            if market.get('active') and market.get('linear')
            and market.get('settle') == 'USDT' and market.get('type') == 'swap'
        ]

    print(f"[波动率排行] 市场加载完成 | 活跃合约数: [{len(symbols)}]")

    # 24小时Ticker初筛
    print(f"[波动率排行] 执行初筛 | 通过24h振幅过滤死币...")
    tickers = exchange.fetch_tickers(symbols)

    proxy_vols = []
    for sym in symbols:
        ticker = tickers.get(sym, {})
        high = ticker.get('high')
        low = ticker.get('low')
        quote_volume = ticker.get('quoteVolume')

        # 过滤条件：数据完整且24H成交额>100万USDT
        if high and low and low > 0 and quote_volume and quote_volume > 1000000:
            rough_vol = (high - low) / low * 100
            proxy_vols.append({'symbol': sym, 'rough_vol': rough_vol})

    # 按振幅排序，提取前top_n名
    proxy_vols = sorted(proxy_vols, key=lambda x: x['rough_vol'], reverse=True)
    target_symbols = [item['symbol'] for item in proxy_vols[:top_n]]

    print(f"[波动率排行] 初筛完成 | 锁定高波动标的: [{len(target_symbols)}] 个")

    # 批量获取资金费率
    print(f"[波动率排行] 拉取资金费率 | 标的数: [{len(target_symbols)}]")
    funding_rates_data = {}
    for attempt in range(5):
        try:
            funding_rates_data = exchange.fetch_funding_rates(target_symbols)
            break
        except Exception as e:
            if attempt == 4:
                print(f"[波动率排行] 资金费率拉取失败(重试5次) | 错误: [{e}]")
            else:
                time.sleep(1)

    # 确定K线拉取数量
    if max_minutes > 1000:
        print(f"[波动率排行] 警告: 分钟数超限，截断为1000条")
        limit = 1000
    else:
        limit = max_minutes

    # 定义单线程处理函数
    def fetch_and_calc(symbol):
        try:
            all_ohlcv = []
            now = exchange.milliseconds()
            current_since = now - int(limit * 60 * 1000)

            # 分批拉取K线
            while len(all_ohlcv) < limit:
                fetch_limit = min(limit - len(all_ohlcv), 1000 if exchange_name == 'binance' else 300)

                for attempt in range(5):
                    try:
                        ohlcv = exchange.fetch_ohlcv(symbol, '1m', since=current_since, limit=fetch_limit)
                        break
                    except Exception as e:
                        if attempt == 4:
                            raise Exception(f"K线拉取失败: {e}")
                        time.sleep(1)

                if not ohlcv:
                    break

                all_ohlcv.extend(ohlcv)
                current_since = ohlcv[-1][0] + 60000

                if len(ohlcv) < fetch_limit:
                    break

            # 数据完整性检查
            if len(all_ohlcv) < (limit * 0.8):
                print(f"[波动率排行] K线不足 | 标的: [{symbol}] | 期望: [{limit}] | 实际: [{len(all_ohlcv)}]")
                return None

            df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)
            df = df[df['low'] > 0]

            if df.empty:
                return None

            # 计算波动率
            df['volatility'] = (df['high'] - df['low']) / df['low'] * 100
            latest_price = df['close'].iloc[-1]

            # 获取资金费率
            fr_info = funding_rates_data.get(symbol, {})
            funding_rate = fr_info.get('fundingRate') or fr_info.get('info', {}).get('fundingRate') or 0
            funding_rate_pct = float(funding_rate) * 100 if funding_rate else 0.0

            symbol_data = {
                'Symbol': symbol,
                'Latest Price': latest_price,
                'Funding Rate %': funding_rate_pct
            }

            temp_vols = []
            for m in minutes_list:
                df_m = df.tail(m)
                avg_vol = df_m['volatility'].mean() if not df_m.empty else 0
                symbol_data[f'Avg Vol ({m}m) %'] = avg_vol
                temp_vols.append(avg_vol)

            symbol_data['Overall Avg Vol %'] = sum(temp_vols) / len(temp_vols) if temp_vols else 0
            return symbol_data

        except Exception as e:
            print(f"[波动率排行] 处理异常 | 标的: [{symbol}] | 错误: [{e}]")
            return None

    # 多线程并发执行
    print(f"[波动率排行] 启动并发拉取 | 线程数: [{max_workers}] | 标的数: [{len(target_symbols)}]")

    results = []
    completed_count = 0
    total_symbols = len(target_symbols)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_and_calc, sym): sym for sym in target_symbols}

        for future in as_completed(future_to_symbol):
            completed_count += 1
            res = future.result()
            if res is not None:
                results.append(res)

            # 进度打印
            progress_interval = 10 if exchange_name == 'binance' else 50
            if completed_count % progress_interval == 0 or completed_count == total_symbols:
                print(f"[波动率排行] 进度 | 已完成: [{completed_count}/{total_symbols}]")

    df_res = pd.DataFrame(results)

    if not df_res.empty:
        # 列排序
        col_order = ['Symbol', 'Latest Price', 'Funding Rate %', 'Overall Avg Vol %'] + \
                    [f'Avg Vol ({m}m) %' for m in minutes_list]
        df_res = df_res[col_order]

        # 按综合波动率降序排序
        df_res = df_res.sort_values(by='Overall Avg Vol %', ascending=False).reset_index(drop=True)

        # 格式化小数
        df_res['Funding Rate %'] = df_res['Funding Rate %'].round(4)
        df_res['Overall Avg Vol %'] = df_res['Overall Avg Vol %'].round(4)
        for m in minutes_list:
            df_res[f'Avg Vol ({m}m) %'] = df_res[f'Avg Vol ({m}m) %'].round(4)

    print(f"[波动率排行] 分析完成 | 有效结果: [{len(df_res)}] 条")
    return df_res


def get_binance_volatility_ranking(minutes_list=[15, 30, 60], max_workers=20, top_n=800):
    """兼容旧接口，调用统一的波动率排行函数"""
    return get_volatility_ranking('binance', minutes_list, max_workers, top_n)


def get_okx_volatility_ranking(minutes_list=[15, 30, 60], max_workers=100):
    """兼容旧接口，调用统一的波动率排行函数"""
    return get_volatility_ranking('okx', minutes_list, max_workers)


def get_binance_futures_change(hours=48):
    """
    获取币安所有U本位合约过去N小时的涨跌幅排行。

    [功能摘要]
    计算指定时间窗口内的价格变化百分比。

    [输入数据]
    - hours: 时间窗口(小时)

    [数据流转]
    加载市场 → 过滤合约 → 获取当前价格 → 获取历史价格 → 计算涨跌幅

    [输出数据]
    DataFrame: 包含Symbol, Current Price, Historical Price, Change(%) 列
    """
    print(f"\n[涨跌幅排行] 开始分析 | 时间窗口: [{hours}] 小时")

    exchange = init_exchange('binance')
    markets = exchange.load_markets()

    symbols = [
        symbol for symbol, market in markets.items()
        if market.get('active') and market.get('linear')
        and market.get('quote') == 'USDT' and market.get('type') == 'swap'
    ]

    print(f"[涨跌幅排行] 市场加载完成 | 合约数: [{len(symbols)}]")

    now = exchange.milliseconds()
    since = now - int(hours * 60 * 60 * 1000)

    tickers = exchange.fetch_tickers(symbols)
    results = []

    print(f"[涨跌幅排行] 拉取历史价格 | 预计耗时约30秒...")

    for i, symbol in enumerate(symbols):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, '1h', since=since, limit=1)

            if not ohlcv:
                continue

            historical_timestamp = ohlcv[0][0]
            historical_price = ohlcv[0][1]
            current_price = tickers[symbol].get('last')

            # 过滤新币(历史数据不足)
            if not current_price or (historical_timestamp - since > 2 * 60 * 60 * 1000):
                continue

            change_pct = (current_price - historical_price) / historical_price * 100

            results.append({
                'Symbol': symbol,
                'Current Price (USDT)': current_price,
                f'Price {hours}h Ago': historical_price,
                'Change (%)': change_pct
            })

            if (i + 1) % 50 == 0 or (i + 1) == len(symbols):
                print(f"[涨跌幅排行] 进度 | 已完成: [{i + 1}/{len(symbols)}]")

        except Exception:
            continue

    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by='Change (%)', ascending=False).reset_index(drop=True)
        df['Change (%)'] = df['Change (%)'].round(2)

    print(f"[涨跌幅排行] 分析完成 | 有效结果: [{len(df)}] 条")
    return df


# ============================================================================
# 数据合并层
# ============================================================================

def merge_cvd_oi_complete(cvd_df, oi_df, timeframe='1h'):
    """
    深度合并CVD和OI数据。

    [功能摘要]
    将CVD数据和OI数据按时间对齐合并，计算衍生指标。

    [输入数据]
    - cvd_df: CVD数据DataFrame
    - oi_df: OI数据DataFrame
    - timeframe: 时间周期(用于对齐)

    [数据流转]
    时间偏移对齐 → 左连接合并 → 衍生指标计算

    [输出数据]
    DataFrame: 合并后的完整数据集
    """
    if cvd_df.empty or oi_df.empty:
        print(f"[数据合并] 失败: 输入数据为空")
        return pd.DataFrame()

    cvd = cvd_df.copy()
    oi = oi_df.copy()
    cvd['timestamp'] = pd.to_datetime(cvd['timestamp'])
    oi['timestamp'] = pd.to_datetime(oi['timestamp'])

    # OI时间偏移对齐
    delta = pd.to_timedelta(timeframe)
    oi['match_timestamp'] = oi['timestamp'] - delta

    df = pd.merge(
        cvd, oi,
        left_on='timestamp', right_on='match_timestamp',
        how='left', suffixes=('', '_oi_raw')
    )

    if 'match_timestamp' in df.columns:
        df.drop(columns=['match_timestamp'], inplace=True)
    df.rename(columns={'timestamp_oi_raw': 'oi_snapshot_time'}, inplace=True)

    # 计算衍生指标
    df['price_change_pct'] = df['close'].pct_change() * 100
    df['volume_change'] = df['volume'].diff()
    df['delta_ratio'] = np.where(
        df['volume'] != 0,
        (df['volume_delta'] / df['volume']) * 100, 0
    )

    if 'oi_value_change_pct' not in df.columns:
        df['oi_value_change_pct'] = df['oi_value'].pct_change() * 100

    # 填充NaN并格式化
    fill_cols = ['price_change_pct', 'volume_change', 'delta_ratio', 'oi_value_change_pct']
    for col in fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    round_cols = ['price_change_pct', 'oi_amount_change_pct', 'oi_value_change_pct', 'delta_ratio']
    for col in round_cols:
        if col in df.columns:
            df[col] = df[col].round(4)

    return df


def merge_cvd_oi_funding_complete(cvd_df, oi_df, funding_df, timeframe='1h'):
    """
    深度合并CVD、OI和资金费率数据。

    [功能摘要]
    在CVD+OI基础上加入资金费率数据。

    [输入数据]
    - cvd_df, oi_df, funding_df: 三个数据源DataFrame

    [输出数据]
    DataFrame: 三源合并的完整数据集
    """
    if cvd_df.empty or oi_df.empty:
        print(f"[三源合并] 失败: CVD或OI数据为空")
        return pd.DataFrame()

    cvd = cvd_df.copy()
    oi = oi_df.copy()
    funding = funding_df.copy() if not funding_df.empty else pd.DataFrame(
        columns=['timestamp', 'funding_rate', 'funding_rate_pct'])

    cvd['timestamp'] = pd.to_datetime(cvd['timestamp'])
    oi['timestamp'] = pd.to_datetime(oi['timestamp'])
    if not funding.empty:
        funding['timestamp'] = pd.to_datetime(funding['timestamp'])

    # OI时间偏移对齐
    delta = pd.to_timedelta(timeframe)
    oi['match_timestamp'] = oi['timestamp'] - delta

    df = pd.merge(
        cvd, oi,
        left_on='timestamp', right_on='match_timestamp',
        how='left', suffixes=('', '_oi_raw')
    )

    if 'match_timestamp' in df.columns:
        df.drop(columns=['match_timestamp'], inplace=True)
    df.rename(columns={'timestamp_oi_raw': 'oi_snapshot_time'}, inplace=True)

    # 合并资金费率
    if not funding.empty:
        df = pd.merge(
            df, funding[['timestamp', 'funding_rate', 'funding_rate_pct']],
            on='timestamp', how='left'
        )
    else:
        df['funding_rate'] = np.nan
        df['funding_rate_pct'] = np.nan

    # 计算衍生指标
    df['price_change_pct'] = df['close'].pct_change() * 100
    df['volume_change'] = df['volume'].diff()
    df['delta_ratio'] = np.where(
        df['volume'] != 0,
        (df['volume_delta'] / df['volume']) * 100, 0
    )

    if 'oi_value_change_pct' not in df.columns:
        df['oi_value_change_pct'] = df['oi_value'].pct_change() * 100

    fill_cols = ['price_change_pct', 'volume_change', 'delta_ratio', 'oi_value_change_pct']
    for col in fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    round_cols = ['price_change_pct', 'oi_amount_change_pct', 'oi_value_change_pct', 'delta_ratio']
    for col in round_cols:
        if col in df.columns:
            df[col] = df[col].round(4)

    return df


# ============================================================================
# 信号检测层
# ============================================================================

def detect_oi_signals_with_confidence(df):
    """
    带有置信度评分的OI信号识别。

    [功能摘要]
    基于OI变化识别多种交易信号，并计算置信度分数(0-100)。

    [输入数据]
    - df: 包含oi_amount_change_pct, oi_value_change_pct的DataFrame

    [输出数据]
    DataFrame: 新增Signal, Confidence_%, Reason列
    """
    df = df.sort_values('timestamp').reset_index(drop=True)

    signals = []
    reasons = []
    confidences = []

    # 状态机变量
    in_grid_zone = False
    recent_blow_off = False
    hours_since_danger = 0

    for i in range(len(df)):
        current_row = df.iloc[i]
        amt_pct = current_row['oi_amount_change_pct']
        val_pct = current_row['oi_value_change_pct']

        signal = "Neutral"
        reason = ""
        conf_score = 0.0

        hours_since_danger += 1

        # DANGER: 燃料暴增
        if amt_pct > 15:
            signal = "🛑 DANGER"
            reason = "巨量燃料注入！"
            in_grid_zone = False
            recent_blow_off = False
            hours_since_danger = 0
            conf_score = 50 + (amt_pct - 15) * 2

        # WARNING: 无量干拔
        elif amt_pct < 3 and val_pct > 18:
            signal = "⚠️ WARNING"
            reason = "无量干拔 (燃料停滞+价值飙升)"
            recent_blow_off = True
            conf_score = 60 + (val_pct - 18) * 1.5 + (3 - amt_pct) * 3

        # GRID_START: 大资金撤退
        elif recent_blow_off and amt_pct < -5 and val_pct < -8:
            signal = "✅ GRID_START"
            reason = "单边结束，大资金撤离"
            in_grid_zone = True
            recent_blow_off = False
            conf_score = 60 + abs(amt_pct + 5) * 2 + abs(val_pct + 8) * 1

        # SHORT_ENTRY / DOWN_TREND
        elif in_grid_zone and hours_since_danger > 3:
            if amt_pct <= 1.5 and val_pct >= 10:
                signal = "🎯 SHORT_ENTRY"
                reason = "空心假拉升，网格高抛点"
                conf_score = 65 + (val_pct - 10) * 2 + (1.5 - amt_pct) * 4
            elif amt_pct < -5 and val_pct < -10:
                signal = "📉 DOWN_TREND"
                reason = "震荡向下破位"
                conf_score = 50 + abs(amt_pct + 5) * 1.5

        # 分数限制
        if signal != "Neutral":
            conf_score = min(100.0, max(0.0, conf_score))
            conf_score = round(conf_score, 2)
        else:
            conf_score = 0.0

        signals.append(signal)
        reasons.append(reason)
        confidences.append(conf_score)

    df['Signal'] = signals
    df['Confidence_%'] = confidences
    df['Reason'] = reasons

    return df


def detect_long_grid_signals_strict(df):
    """
    严格版做多网格信号识别。

    [功能摘要]
    专为做多网格设计的信号检测，过滤死猫跳陷阱。

    [输入数据]
    - df: 包含oi_amount_change_pct, oi_value_change_pct的DataFrame

    [输出数据]
    DataFrame: 新增Signal, Confidence_%, Reason列
    """
    df = df.sort_values('timestamp').reset_index(drop=True)

    signals = []
    reasons = []
    confidences = []

    in_long_grid_zone = False
    recent_capitulation = False
    hours_since_danger = 0

    for i in range(len(df)):
        current_row = df.iloc[i]
        amt_pct = current_row['oi_amount_change_pct']
        val_pct = current_row['oi_value_change_pct']

        signal = "Neutral"
        reason = ""
        conf_score = 0.0

        hours_since_danger += 1

        # DANGER: 主动追空瀑布
        if val_pct < -5 and amt_pct > 6:
            signal = "🛑 DANGER"
            reason = "主动追空瀑布！空军携巨资入场，做多网格立刻停机！"
            in_long_grid_zone = False
            recent_capitulation = False
            hours_since_danger = 0
            conf_score = 60 + abs(val_pct + 5) * 1.5 + (amt_pct - 6) * 2

        # WARNING: 多头大血洗
        elif val_pct < -15 and amt_pct < -10:
            signal = "⚠️ WARNING"
            reason = "多头连环爆仓。抛压释放中，不接飞刀，密切观察。"
            recent_capitulation = True
            in_long_grid_zone = False
            conf_score = 65 + abs(val_pct + 15) * 1.5 + abs(amt_pct + 10) * 2.5

        # BEAR_TRAP: 死猫跳陷阱
        elif recent_capitulation and val_pct > 8 and amt_pct < -5:
            signal = "☠️ BEAR_TRAP"
            reason = "空头平仓死猫跳！价格虚高但资金流出，即将二次探底，绝对观望！"
            conf_score = 80 + (val_pct - 8) * 1.5 + abs(amt_pct + 5) * 2

        # GRID_START: 真实止跌企稳
        elif recent_capitulation and abs(val_pct) <= 4 and abs(amt_pct) <= 2.5:
            signal = "✅ GRID_START"
            reason = "真实底部企稳。波动率极度收缩，多空双方熄火，安全开启网格！"
            in_long_grid_zone = True
            recent_capitulation = False
            conf_score = 70 + (4 - abs(val_pct)) * 5

        # LONG_ENTRY / UP_TREND
        elif in_long_grid_zone and hours_since_danger > 3:
            if val_pct <= -6 and -1 <= amt_pct <= 2:
                signal = "🎯 LONG_ENTRY"
                reason = "完美空心砸盘！无爆仓无追空，纯属流动性缺失假摔，低吸买点！"
                conf_score = 70 + abs(val_pct + 6) * 2.5 + (2 - amt_pct) * 2
            elif val_pct > 5 and amt_pct > 3:
                signal = "📈 UP_TREND"
                reason = "底部企稳回升，真实买盘介入。"
                conf_score = 50 + val_pct * 1.5

        if signal != "Neutral":
            conf_score = min(100.0, max(0.0, conf_score))
            conf_score = round(conf_score, 2)
        else:
            conf_score = 0.0

        signals.append(signal)
        reasons.append(reason)
        confidences.append(conf_score)

    df['Signal'] = signals
    df['Confidence_%'] = confidences
    df['Reason'] = reasons

    return df


def detect_signal_a(df, short_window=3, long_window=6, ntr_threshold=0.06,
                   price_flat_pct=0.005, extreme_ntr=0.15, vol_spike_mult=1.5):
    """
    信号A: CVD-价格极端背离检测(冰山吸收信号)。

    [功能摘要]
    检测CVD与价格的极端背离，识别冰山订单吸收现象。

    [输入数据]
    - df: 包含volume_delta, cvd等列的DataFrame
    - 多个阈值参数

    [输出数据]
    DataFrame: 新增signal, signal_strength, signal_intensity, annotation列
    """
    r = df.copy()

    # 补全列
    if 'volume_delta' not in r.columns:
        r['volume_delta'] = r['taker_buy_base_vol'] - r['taker_sell_base_vol']
    if 'cvd' not in r.columns:
        r['cvd'] = r['volume_delta'].cumsum()

    # 单根K线涨跌幅
    prev_close = r['close'].shift(1)
    r['pct_change'] = (r['close'] - prev_close) / prev_close

    # 双窗口滚动指标
    for w, tag in [(short_window, 'sw'), (long_window, 'lw')]:
        base_close = r['close'].shift(w)
        r[f'price_chg_{tag}'] = (r['close'] - base_close) / base_close
        r[f'cvd_chg_{tag}'] = r['cvd'].diff(w)
        r[f'vd_sum_{tag}'] = r['volume_delta'].rolling(w, min_periods=w).sum()
        r[f'vol_sum_{tag}'] = r['volume'].rolling(w, min_periods=w).sum()
        r[f'ntr_{tag}'] = r[f'vd_sum_{tag}'] / r[f'vol_sum_{tag}'].replace(0, np.nan)

    vol_lb = max(long_window * 3, 12)
    r['vol_ma'] = r['volume'].rolling(vol_lb, min_periods=1).mean()
    r['vol_ratio'] = r['volume'] / r['vol_ma'].replace(0, np.nan)

    # 背离方向检测
    for tag in ['sw', 'lw']:
        ntr = r[f'ntr_{tag}']
        pc = r[f'price_chg_{tag}']
        h_buy = ntr > ntr_threshold
        h_sell = ntr < -ntr_threshold
        p_up = pc > price_flat_pct
        p_dn = pc < -price_flat_pct
        p_fl = pc.abs() <= price_flat_pct
        r[f'bull_{tag}'] = h_sell & (p_fl | p_up)
        r[f'bull_rev_{tag}'] = h_sell & p_up
        r[f'bear_{tag}'] = h_buy & (p_fl | p_dn)
        r[f'bear_rev_{tag}'] = h_buy & p_dn

    # 综合信号
    bull_any = r['bull_sw'] | r['bull_lw']
    bear_any = r['bear_sw'] | r['bear_lw']
    bull_both = r['bull_sw'] & r['bull_lw']
    bear_both = r['bear_sw'] & r['bear_lw']
    conflict = bull_any & bear_any

    r['signal'] = 'NEUTRAL'
    r.loc[bull_any & ~conflict, 'signal'] = 'BULLISH_ABSORPTION'
    r.loc[bear_any & ~conflict, 'signal'] = 'BEARISH_ABSORPTION'

    is_bull = r['signal'] == 'BULLISH_ABSORPTION'
    is_bear = r['signal'] == 'BEARISH_ABSORPTION'
    active = is_bull | is_bear

    # 连续信号计数
    sig_n = r['signal'].map({
        'BULLISH_ABSORPTION': 1, 'BEARISH_ABSORPTION': -1, 'NEUTRAL': 0
    })
    consec, cnt, prev = [], 0, 0
    for v in sig_n:
        cnt = cnt + 1 if (v != 0 and v == prev) else (1 if v != 0 else 0)
        consec.append(cnt)
        prev = v
    r['consecutive_signals'] = consec

    # 整数强度(0-5)
    s = pd.Series(0, index=r.index, dtype=int)
    s[active] = 1
    s[is_bull & bull_both] += 1
    s[is_bear & bear_both] += 1
    bull_rev = r['bull_rev_sw'] | r['bull_rev_lw']
    bear_rev = r['bear_rev_sw'] | r['bear_rev_lw']
    s[is_bull & bull_rev] += 1
    s[is_bear & bear_rev] += 1
    max_ntr = r[['ntr_sw', 'ntr_lw']].abs().max(axis=1)
    s[active & (max_ntr > extreme_ntr)] += 1
    s[active & (r['vol_ratio'] > vol_spike_mult)] += 1
    r['signal_strength'] = s.clip(0, 5)
    r.loc[~active, 'signal_strength'] = 0

    # 连续强度(0-100)
    ntr_score = ((max_ntr - ntr_threshold) / (0.30 - ntr_threshold)).clip(0, 1) * 40

    pc_sw = r['price_chg_sw'].fillna(0)
    pc_lw = r['price_chg_lw'].fillna(0)
    price_contra = pd.Series(0.0, index=r.index)
    price_contra[is_bull] = np.maximum(pc_sw[is_bull], pc_lw[is_bull]).clip(0, None)
    price_contra[is_bear] = (-np.minimum(pc_sw[is_bear], pc_lw[is_bear])).clip(0, None)
    price_score = (price_contra / 0.03).clip(0, 1) * 25

    vol_score = ((r['vol_ratio'].fillna(1) - 1.0) / 2.0).clip(0, 1) * 20

    cc = pd.Series(consec, index=r.index, dtype=float)
    time_score = ((cc - 1) / 5.0).clip(0, 1) * 15

    intensity = ntr_score + price_score + vol_score + time_score
    intensity[~active] = 0.0
    r['signal_intensity'] = intensity.round(2)

    # 生成注释
    def _v(x):
        return float(x) if pd.notna(x) else 0.0

    annotations = []
    for i in range(len(r)):
        row = r.iloc[i]
        sig = row['signal']
        if sig == 'NEUTRAL':
            annotations.append('')
            continue

        st = int(row['signal_strength'])
        si = row['signal_intensity']
        st_label = {1: '弱', 2: '较弱', 3: '中等', 4: '强', 5: '极强'}[st]

        sw_a = abs(_v(row['ntr_sw']))
        lw_a = abs(_v(row['ntr_lw']))
        tag = 'lw' if lw_a >= sw_a else 'sw'
        wn = str(long_window) if tag == 'lw' else str(short_window)
        ntr_v = _v(row[f'ntr_{tag}']) * 100
        pc_v = _v(row[f'price_chg_{tag}']) * 100
        bar_chg = _v(row['pct_change']) * 100
        cc_v = int(row['consecutive_signals'])

        if sig == 'BULLISH_ABSORPTION':
            icon = '🟢'
            label = '看涨吸收'
            taker_d = f'Taker净卖出占比{abs(ntr_v):.1f}%'
            price_d = (f'窗口价格反涨{pc_v:+.2f}%' if pc_v > price_flat_pct * 100
                      else f'窗口价格拒跌({pc_v:+.2f}%)')
            detail = '限价买单吸收卖压 → 后续大概率拉升'
        else:
            icon = '🔴'
            label = '看跌吸收'
            taker_d = f'Taker净买入占比{abs(ntr_v):.1f}%'
            price_d = (f'窗口价格反跌{pc_v:+.2f}%' if pc_v < -price_flat_pct * 100
                      else f'窗口价格拒涨({pc_v:+.2f}%)')
            detail = '限价卖单吸收买压 → 后续大概率砸盘'

        parts = [
            f"{icon} {label} | 档位{st}/5({st_label}) | 强度{si:.1f}/100",
            f"  {wn}K窗口: {taker_d}, 但{price_d}",
            f"  当根涨跌幅: {bar_chg:+.3f}%",
            f"  → {detail}",
        ]
        if cc_v > 1:
            parts.append(f"  ⏱ 连续{cc_v}根K线同向吸收")
        annotations.append('\n'.join(parts))

    r['annotation'] = annotations

    # 列排序
    signal_cols = [
        'signal', 'signal_strength', 'signal_intensity',
        'pct_change', 'consecutive_signals', 'annotation',
        'ntr_sw', 'ntr_lw', 'price_chg_sw', 'price_chg_lw',
        'cvd_chg_sw', 'cvd_chg_lw', 'vol_ratio',
    ]
    ts_col = ['timestamp'] if 'timestamp' in r.columns else []
    front = ts_col + [c for c in signal_cols if c in r.columns]
    middle = [c for c in df.columns if c not in set(front)]
    rest = [c for c in r.columns if c not in set(front) and c not in set(middle)]
    ordered = front + middle + rest

    seen, final = set(), []
    for c in ordered:
        if c not in seen:
            final.append(c)
            seen.add(c)

    return r[final]


def generate_microstructure_signals(df, oi_drop_threshold=-1.5, price_move_threshold=1.0):
    """
    基于订单流和持仓量微观结构的交易信号生成器。

    [功能摘要]
    检测爆仓/清算后的反转机会。

    [输入数据]
    - df: 包含OHLCV、CVD、OI等列的DataFrame

    [输出数据]
    DataFrame: 新增signal, signal_strength, logic_explanation列
    """
    res_df = df.copy()

    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'taker_buy_base_vol',
                    'taker_sell_base_vol', 'volume_delta', 'cvd', 'oi_amount_change_pct',
                    'price_change_pct', 'delta_ratio']
    for col in numeric_cols:
        if col in res_df.columns:
            res_df[col] = pd.to_numeric(res_df[col], errors='coerce')

    # K线结构辅助列
    res_df['body_size'] = abs(res_df['close'] - res_df['open'])
    res_df['lower_shadow'] = res_df[['open', 'close']].min(axis=1) - res_df['low']
    res_df['upper_shadow'] = res_df['high'] - res_df[['open', 'close']].max(axis=1)

    signals = []
    strengths = []
    explanations = []

    for index, row in res_df.iterrows():
        signal = 0
        strength = 0.0
        logic_texts = []

        oi_change = row['oi_amount_change_pct']
        px_change = row['price_change_pct']
        vol_delta = row['volume_delta']

        # 场景1: 多头爆仓后的做多机会
        if px_change < -price_move_threshold and oi_change < oi_drop_threshold:
            score = 40
            logic_texts.append(f"【多头爆仓确认】价格下跌{px_change:.2f}%, OI骤降{oi_change:.2f}%")

            if row['lower_shadow'] > row['body_size'] * 1.5:
                score += 30
                logic_texts.append("【底部吸收】存在长下影线")
            elif row['lower_shadow'] > row['body_size'] * 0.5:
                score += 15
                logic_texts.append("【微弱吸收】有一定下影线支撑")

            if vol_delta > 0:
                score += 30
                logic_texts.append("【主动买盘回归】Volume Delta转正")
            elif row['taker_sell_base_vol'] > 0 and (row['taker_buy_base_vol'] / row['taker_sell_base_vol']) > 0.8:
                score += 15
                logic_texts.append("【卖压衰竭】买卖比回升")

            strength = score
            if strength >= 70:
                signal = 1

        # 场景2: 空头爆仓后的做空机会
        elif px_change > price_move_threshold and oi_change < oi_drop_threshold:
            score = 40
            logic_texts.append(f"【空头轧空确认】价格上涨{px_change:.2f}%, OI骤降{oi_change:.2f}%")

            if row['upper_shadow'] > row['body_size'] * 1.5:
                score += 30
                logic_texts.append("【顶部吸收】存在长上影线")
            elif row['upper_shadow'] > row['body_size'] * 0.5:
                score += 15
                logic_texts.append("【微弱吸收】有一定上影线压制")

            if vol_delta < 0:
                score += 30
                logic_texts.append("【主动卖盘砸盘】Volume Delta转负")
            elif row['taker_buy_base_vol'] > 0 and (row['taker_sell_base_vol'] / row['taker_buy_base_vol']) > 0.8:
                score += 15
                logic_texts.append("【买力衰竭】空头反击比例提高")

            strength = -score
            if strength <= -70:
                signal = -1

        if signal == 0:
            logic_explanation = "持仓观望。未检测到异常。"
        else:
            logic_explanation = "综合判定触发：\n" + "\n".join(logic_texts)

        signals.append(signal)
        strengths.append(strength)
        explanations.append(logic_explanation)

    try:
        ts_idx = res_df.columns.get_loc('timestamp')
    except KeyError:
        ts_idx = -1

    res_df.insert(ts_idx + 1, 'signal', signals)
    res_df.insert(ts_idx + 2, 'signal_strength', strengths)
    res_df.insert(ts_idx + 3, 'logic_explanation', explanations)

    res_df = res_df.drop(columns=['body_size', 'lower_shadow', 'upper_shadow'])

    return res_df


def calculate_oi_extremes(df, window=20, future_window=24):
    """
    计算基于持仓量极值的量价异常信号。

    [功能摘要]
    生成5大类核心交易信号：做空、做多、吸筹做多、资金费率背离做空/做多。

    [输入数据]
    - df: 包含oi_amount, close等列的DataFrame
    - window: 极值计算窗口
    - future_window: 未来评估窗口

    [输出数据]
    DataFrame: 新增各类信号列
    """
    start_time = pd.Timestamp.now()
    df = df.copy()

    # 计算价格历史百分位
    if window is None:
        hist_high_max = df['high'].expanding(min_periods=1).max()
        hist_low_min = df['low'].expanding(min_periods=1).min()
    else:
        hist_high_max = df['high'].rolling(window=window, min_periods=1).max()
        hist_low_min = df['low'].rolling(window=window, min_periods=1).min()

    hist_diff = hist_high_max - hist_low_min
    df['price_window_percentile'] = np.round(np.where(
        hist_diff == 0, 0.0, ((df['close'] - hist_low_min) / hist_diff * 100)
    ), 2)

    # 计算未来窗口极值
    future_high_max = df['high'].shift(-1).iloc[::-1].rolling(window=future_window, min_periods=1).max().iloc[::-1]
    future_low_min = df['low'].shift(-1).iloc[::-1].rolling(window=future_window, min_periods=1).min().iloc[::-1]

    df['future_max_high_pct'] = (future_high_max / df['close'] - 1) * 100
    df['future_min_low_pct'] = (future_low_min / df['close'] - 1) * 100

    # 定义核心列顺序
    key_cols = [
        'timestamp', 'price_window_percentile', 'future_max_high_pct', 'future_min_low_pct',
        'short_simple_signal', 'short_signal', 'long_simple_signal', 'long_signal',
        'long_acc_simple_signal', 'long_acc_signal', 'funding_short_signal', 'funding_long_signal',
        'oi_price_ratio', 'oi_pct_from_max', 'price_pct_from_max_oi', 'max_oi_net_vol_pct',
        'oi_pct_from_min', 'price_pct_from_min_oi', 'min_oi_net_vol_pct'
    ]
    ref_cols = ['max_oi_time', 'max_oi_val', 'min_oi_time', 'min_oi_val']

    n = len(df)
    oi_amounts = df['oi_amount'].values
    closes = df['close'].values
    timestamps = df['timestamp'].values

    taker_buys = df.get('taker_buy_base_vol', pd.Series(np.zeros(n))).values
    taker_sells = df.get('taker_sell_base_vol', pd.Series(np.zeros(n))).values

    has_fr = 'funding_rate' in df.columns
    funding_rates = df['funding_rate'].values if has_fr else np.full(n, np.nan)

    # 初始化存储容器
    max_oi_time = [None] * n
    max_oi_val = np.full(n, np.nan)
    min_oi_time = [None] * n
    min_oi_val = np.full(n, np.nan)

    oi_pct_from_max = np.full(n, np.nan)
    price_pct_from_max_oi = np.full(n, np.nan)
    max_oi_net_vol_pct = np.full(n, np.nan)

    oi_pct_from_min = np.full(n, np.nan)
    price_pct_from_min_oi = np.full(n, np.nan)
    min_oi_net_vol_pct = np.full(n, np.nan)

    oi_price_ratio = np.full(n, np.nan)

    short_simple_signal = np.zeros(n, dtype=bool)
    short_signal = np.zeros(n, dtype=bool)
    long_simple_signal = np.zeros(n, dtype=bool)
    long_signal = np.zeros(n, dtype=bool)
    long_acc_simple_signal = np.zeros(n, dtype=bool)
    long_acc_signal = np.zeros(n, dtype=bool)
    funding_short_signal = np.zeros(n, dtype=bool)
    funding_long_signal = np.zeros(n, dtype=bool)

    # 预计算累计和
    cum_buys = np.zeros(n + 1)
    cum_sells = np.zeros(n + 1)
    cum_buys[1:] = np.cumsum(np.nan_to_num(taker_buys))
    cum_sells[1:] = np.cumsum(np.nan_to_num(taker_sells))

    start_idx = 1 if window is None else window

    last_signal_price_pct = -np.inf
    last_long_signal_price_pct = np.inf

    curr_global_max_idx = 0
    curr_global_min_idx = 0

    for i in range(start_idx, n):
        # 获取极值索引
        if window is None:
            if oi_amounts[i] > oi_amounts[curr_global_max_idx]:
                curr_global_max_idx = i
            if oi_amounts[i] < oi_amounts[curr_global_min_idx]:
                curr_global_min_idx = i
            idx_max = curr_global_max_idx
            idx_min = curr_global_min_idx
        else:
            start_w = max(0, i - window + 1)
            window_slice = oi_amounts[start_w: i + 1]
            idx_max = start_w + np.argmax(window_slice)
            idx_min = start_w + np.argmin(window_slice)

        max_oi_time[i] = timestamps[idx_max]
        max_oi_val[i] = oi_amounts[idx_max]
        min_oi_time[i] = timestamps[idx_min]
        min_oi_val[i] = oi_amounts[idx_min]

        # 计算百分比
        curr_oi_pct = (oi_amounts[i] / oi_amounts[idx_max] - 1) * 100
        curr_price_pct = (closes[i] / closes[idx_max] - 1) * 100

        oi_pct_from_max[i] = curr_oi_pct
        price_pct_from_max_oi[i] = curr_price_pct

        curr_oi_pct_min = (oi_amounts[i] / oi_amounts[idx_min] - 1) * 100
        curr_price_pct_min = (closes[i] / closes[idx_min] - 1) * 100

        oi_pct_from_min[i] = curr_oi_pct_min
        price_pct_from_min_oi[i] = curr_price_pct_min

        # Net Vol Pct计算
        buy_max = cum_buys[i + 1] - cum_buys[idx_max]
        sell_max = cum_sells[i + 1] - cum_sells[idx_max]
        max_oi_net_vol_pct[i] = ((buy_max - sell_max) / (buy_max + sell_max) * 100) if (buy_max + sell_max) != 0 else 0

        buy_min = cum_buys[i + 1] - cum_buys[idx_min]
        sell_min = cum_sells[i + 1] - cum_sells[idx_min]
        min_oi_net_vol_pct[i] = ((buy_min - sell_min) / (buy_min + sell_min) * 100) if (buy_min + sell_min) != 0 else 0

        # 做空信号逻辑
        prev_oi_pct = oi_pct_from_max[i - 1]

        if not np.isnan(prev_oi_pct):
            lookback_start = 1 if window is None else max(1, i - window)
            slice_vals = oi_pct_from_max[lookback_start: i]
            valid_mask = ~np.isnan(slice_vals)

            if np.any(valid_mask):
                is_prev_pct_min = bool(prev_oi_pct <= np.min(slice_vals[valid_mask]))
            else:
                is_prev_pct_min = False

            is_oi_turning_up = bool(oi_amounts[i] > oi_amounts[i - 1])
            is_deep_enough = bool(prev_oi_pct < -0.1)

            if is_prev_pct_min and is_oi_turning_up and is_deep_enough:
                if curr_price_pct > last_signal_price_pct:
                    short_signal[i] = True
                    last_signal_price_pct = curr_price_pct
                short_simple_signal[i] = True

        # 做多信号逻辑
        prev_oi_pct_from_min = oi_pct_from_min[i - 1]

        if not np.isnan(prev_oi_pct_from_min):
            lookback_start = 1 if window is None else max(1, i - window)
            slice_vals_long = oi_pct_from_min[lookback_start: i]
            valid_mask_long = ~np.isnan(slice_vals_long)

            if np.any(valid_mask_long):
                is_prev_long_pct_max = bool(prev_oi_pct_from_min >= np.max(slice_vals_long[valid_mask_long]))
            else:
                is_prev_long_pct_max = False

            is_oi_turning_down = bool(oi_amounts[i] < oi_amounts[i - 1])
            is_high_enough = bool(prev_oi_pct_from_min > 0.1)

            if is_prev_long_pct_max and is_oi_turning_down and is_high_enough:
                long_simple_signal[i] = True
                if curr_price_pct_min < last_long_signal_price_pct:
                    long_signal[i] = True
                    last_long_signal_price_pct = curr_price_pct_min

        # 吸筹做多信号逻辑
        if not np.isnan(curr_oi_pct_min) and not np.isnan(curr_price_pct_min):
            min_oi_surge = 10.0
            price_multiplier = max((curr_price_pct_min + 100) / 100.0, 0.0001)
            current_ratio = curr_oi_pct_min / price_multiplier
            oi_price_ratio[i] = current_ratio

            lookback_start = 1 if window is None else max(1, i - window)

            valid_history_mask = (oi_pct_from_min[lookback_start: i] > min_oi_surge)
            valid_ratios = oi_price_ratio[lookback_start: i][valid_history_mask]
            valid_oi_pcts = oi_pct_from_min[lookback_start: i][valid_history_mask]

            valid_ratios = valid_ratios[~np.isnan(valid_ratios)]
            if len(valid_ratios) == 0:
                is_ratio_new_high = True
            else:
                is_ratio_new_high = bool(current_ratio > np.max(valid_ratios))

            valid_oi_pcts = valid_oi_pcts[~np.isnan(valid_oi_pcts)]
            if len(valid_oi_pcts) == 0:
                is_oi_pct_new_high = True
            else:
                is_oi_pct_new_high = bool(curr_oi_pct_min > np.max(valid_oi_pcts))

            is_oi_surged = bool(curr_oi_pct_min > min_oi_surge)

            if is_oi_surged and (oi_amounts[i] > oi_amounts[i - 1]):
                if is_ratio_new_high:
                    long_acc_simple_signal[i] = True
                    if is_oi_pct_new_high:
                        long_acc_signal[i] = True

        # 资金费率背离信号逻辑
        if has_fr and not np.isnan(funding_rates[i]):
            curr_fr = funding_rates[i]
            curr_close = closes[i]

            fr_lookback_start = 0 if window is None else max(0, i - window)
            fr_window = funding_rates[fr_lookback_start: i]
            valid_fr_mask = ~np.isnan(fr_window)

            if np.any(valid_fr_mask):
                valid_fr_history = fr_window[valid_fr_mask]

                prev_min_fr = np.min(valid_fr_history)
                if curr_fr < prev_min_fr:
                    indices = np.where(fr_window == prev_min_fr)[0]
                    idx_prev_min = fr_lookback_start + indices[-1]
                    if curr_close > closes[idx_prev_min]:
                        funding_short_signal[i] = True

                prev_max_fr = np.max(valid_fr_history)
                if curr_fr > prev_max_fr:
                    indices = np.where(fr_window == prev_max_fr)[0]
                    idx_prev_max = fr_lookback_start + indices[-1]
                    if curr_close < closes[idx_prev_max]:
                        funding_long_signal[i] = True

    # 写回DataFrame
    df['max_oi_time'] = max_oi_time
    df['max_oi_val'] = max_oi_val
    df['min_oi_time'] = min_oi_time
    df['min_oi_val'] = min_oi_val

    df['oi_pct_from_max'] = oi_pct_from_max
    df['price_pct_from_max_oi'] = price_pct_from_max_oi
    df['max_oi_net_vol_pct'] = max_oi_net_vol_pct

    df['oi_pct_from_min'] = oi_pct_from_min
    df['price_pct_from_min_oi'] = price_pct_from_min_oi
    df['min_oi_net_vol_pct'] = min_oi_net_vol_pct

    df['oi_price_ratio'] = oi_price_ratio

    df['short_simple_signal'] = short_simple_signal
    df['short_signal'] = short_signal
    df['long_simple_signal'] = long_simple_signal
    df['long_signal'] = long_signal
    df['long_acc_simple_signal'] = long_acc_simple_signal
    df['long_acc_signal'] = long_acc_signal
    df['funding_short_signal'] = funding_short_signal
    df['funding_long_signal'] = funding_long_signal

    remaining_cols = [c for c in df.columns if c not in key_cols and c not in ref_cols]

    print(f"[OI极值信号] 计算完成 | 耗时: [{(pd.Timestamp.now() - start_time).total_seconds():.3f}] 秒")
    return df[key_cols + ref_cols + remaining_cols]


# ============================================================================
# 可视化层
# ============================================================================

def plot_interactive_trend_plotly(df):
    """
    使用Plotly绘制交互式趋势对比图。

    [功能摘要]
    归一化Close和OI，展示Delta Ratio原始数据。
    """
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots

    df_plot = df.copy()
    df_plot['timestamp'] = pd.to_datetime(df_plot['timestamp'])
    df_plot = df_plot.sort_values('timestamp')

    # 归一化处理
    cols_to_plot = ['close', 'oi_amount']
    for col in cols_to_plot:
        min_val, max_val = df_plot[col].min(), df_plot[col].max()
        if max_val != min_val:
            df_plot[f'{col}_norm'] = (df_plot[col] - min_val) / (max_val - min_val)
        else:
            df_plot[f'{col}_norm'] = 0.5

    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Close
    fig.add_trace(go.Scatter(
        x=df_plot['timestamp'], y=df_plot['close_norm'],
        customdata=df_plot['close'], mode='lines', name='Close',
        line=dict(color='#1f77b4', width=2),
        hovertemplate='Close : %{customdata}'
    ), secondary_y=False)

    # OI Amount
    fig.add_trace(go.Scatter(
        x=df_plot['timestamp'], y=df_plot['oi_amount_norm'],
        customdata=df_plot['oi_amount'], mode='lines', name='OI Amount',
        line=dict(color='#ff7f0e', width=2),
        hovertemplate='OI Amount : %{customdata}'
    ), secondary_y=False)

    # Delta Ratio
    fig.add_trace(go.Scatter(
        x=df_plot['timestamp'], y=df_plot['delta_ratio'],
        mode='lines', name='Delta Ratio',
        line=dict(color='#2ca02c', width=2),
        hovertemplate='Delta Ratio : %{y}'
    ), secondary_y=True)

    # 0轴基准线
    fig.add_hline(
        y=0, line_dash="dash", line_color="red", line_width=1.5, opacity=0.6,
        annotation_text="Delta = 0", annotation_position="top left",
        annotation_font_color="red", secondary_y=True
    )

    # 布局配置
    fig.update_layout(
        title="Interactive Trend: Close vs OI vs Delta Ratio",
        xaxis=dict(
            rangeselector=dict(buttons=list([
                dict(count=7, label="7天", step="day", stepmode="backward"),
                dict(count=1, label="1个月", step="month", stepmode="backward"),
                dict(step="all", label="全部")
            ])),
            rangeslider=dict(visible=True), type="date",
            hoverformat="%Y-%m-%d %H:%M:%S"
        ),
        yaxis=dict(visible=False, showgrid=False),
        yaxis2=dict(title="Delta Ratio (Raw)", visible=True, showgrid=True,
                   gridcolor='rgba(0,0,0,0.05)', zeroline=False),
        template="plotly_white", hovermode="x unified"
    )

    fig.show()


# ============================================================================
# 数据持久层
# ============================================================================

def fetch_full_market_data_with_retry(exchange_name, symbol, timeframe, days, retries=5,
                                       save_csv=False, file_path=None):
    """
    带重试机制的全量市场数据拉取中心。

    [功能摘要]
    拉取OI、CVD、资金费率并合并，支持增量保存。

    [输入数据]
    - exchange_name, symbol, timeframe, days: 拉取参数
    - save_csv: 是否保存CSV
    - file_path: 保存路径

    [输出数据]
    DataFrame: 合并后的完整数据
    """
    print(f"[全量拉取] 开始 | 标的: [{symbol}] | 周期: [{timeframe}] | 天数: [{days}]")

    # 1. 拉取OI
    oi_df = fetch_historical_oi(exchange_name=exchange_name, symbol=symbol, timeframe=timeframe, days=days)
    if oi_df is None or oi_df.empty:
        return None

    # 2. 拉取CVD
    cvd_df = fetch_binance_cvd_history(symbol=symbol, timeframe=timeframe, days=days)
    if cvd_df is None or cvd_df.empty:
        return None

    # 3. 拉取资金费率
    funding_df = fetch_long_funding_history(exchange_name=exchange_name, symbol=symbol, days=days)

    # 4. 合并
    merge_df = merge_cvd_oi_funding_complete(cvd_df, oi_df, funding_df, timeframe=timeframe)

    # 5. 保存
    if save_csv and merge_df is not None and not merge_df.empty and file_path:
        merge_df['timestamp'] = pd.to_datetime(merge_df['timestamp'])
        file_path = Path(file_path)

        if file_path.exists():
            try:
                existing_df = pd.read_csv(file_path, usecols=['timestamp'])

                if not existing_df.empty:
                    existing_df['timestamp'] = pd.to_datetime(existing_df['timestamp'])
                    orig_len = len(existing_df)
                    orig_end = existing_df['timestamp'].max()

                    new_data = merge_df[merge_df['timestamp'] > orig_end]

                    if not new_data.empty:
                        new_data.to_csv(file_path, mode='a', header=False, index=False)
                        print(f"[增量保存] 成功 | 标的: [{symbol}] | 新增: [{len(new_data)}] 条")
                    else:
                        print(f"[增量保存] 已是最新 | 标的: [{symbol}]")
                else:
                    raise ValueError("本地文件为空")

            except Exception as e:
                print(f"[全量保存] 回退到全量模式 | 原因: [{e}]")
                merge_df.to_csv(file_path, index=False)
                print(f"[全量保存] 完成 | 标的: [{symbol}] | 数据量: [{len(merge_df)}] 条")
        else:
            merge_df.to_csv(file_path, index=False)
            print(f"[首次保存] 完成 | 标的: [{symbol}] | 数据量: [{len(merge_df)}] 条")

    return merge_df


def fetch_and_save_full_market_data(exchange_name, symbol, timeframe, days, file_path):
    """
    一键拉取并保存完整市场数据。

    [功能摘要]
    拉取OI、CVD、资金费率，合并后保存为CSV。
    """
    print(f"\n[数据流水线] 开始 | 标的: [{symbol}] | 周期: [{timeframe}] | 天数: [{days}]")

    oi_df = fetch_historical_oi(exchange_name=exchange_name, symbol=symbol, timeframe=timeframe, days=days)
    cvd_df = fetch_binance_cvd_history(symbol=symbol, timeframe=timeframe, days=days)
    funding_df = fetch_long_funding_history(exchange_name=exchange_name, symbol=symbol, days=days)

    merge_df = merge_cvd_oi_funding_complete(cvd_df, oi_df, funding_df, timeframe=timeframe)

    if merge_df is not None and not merge_df.empty:
        merge_df.to_csv(file_path, index=False)
        print(f"[数据流水线] 保存成功 | 数据量: [{len(merge_df)}] 条 | 路径: [{file_path}]")
    else:
        print(f"[数据流水线] 保存失败: 数据合并为空")

    return merge_df


# ============================================================================
# 编排层
# ============================================================================

def get_high_volatility_oi_signals(days=7, timeframe='1h', top_k=15, max_workers=10):
    """
    综合自动化工作流：筛选高波动率合约并分析OI信号。

    [功能摘要]
    1. 获取币安高波动率排行
    2. 多线程拉取OI数据
    3. 信号分析
    4. 合并结果

    [输入数据]
    - days, timeframe: 数据参数
    - top_k: 选取前K个合约
    - max_workers: 并发线程数

    [输出数据]
    DataFrame: 所有合约的分析结果
    """
    print(f"\n[OI信号分析] 阶段1: 筛选高波动率合约")

    calc_minutes_list = [15, 30, 60, 90, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1000]
    bin_df = get_binance_volatility_ranking(minutes_list=calc_minutes_list, max_workers=20)

    if bin_df is None or bin_df.empty:
        print(f"[OI信号分析] 失败: 未获取到波动率数据")
        return pd.DataFrame()

    target_symbols = bin_df['Symbol'].head(top_k).tolist()

    print(f"\n[OI信号分析] 阶段2: 多线程拉取OI | 目标数: [{len(target_symbols)}] | 线程数: [{max_workers}]")

    all_analyzed_dfs = []

    def process_symbol(symbol):
        oi_df = None

        for attempt in range(5):
            try:
                oi_df = fetch_historical_oi(exchange_name='binance', symbol=symbol, timeframe=timeframe, days=days)

                if oi_df is not None and not oi_df.empty:
                    break

                if attempt < 4:
                    time.sleep(1)

            except Exception as e:
                if attempt == 4:
                    print(f"[OI拉取] 失败(重试5次) | 标的: [{symbol}] | 错误: [{e}]")
                    return None
                time.sleep(1)

        if oi_df is None or oi_df.empty:
            print(f"[OI拉取] 无数据 | 标的: [{symbol}]")
            return None

        try:
            anlyse_df = detect_oi_signals_with_confidence(oi_df)
            anlyse_df.insert(0, 'Symbol', symbol)
            return anlyse_df

        except Exception as e:
            print(f"[信号分析] 失败 | 标的: [{symbol}] | 错误: [{e}]")
            return None

    completed_count = 0
    total_symbols = len(target_symbols)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(process_symbol, sym): sym for sym in target_symbols}

        for future in as_completed(future_to_symbol):
            completed_count += 1
            sym = future_to_symbol[future]

            try:
                res_df = future.result()
                if res_df is not None and not res_df.empty:
                    all_analyzed_dfs.append(res_df)
            except Exception as e:
                print(f"[线程执行] 致命错误 | 标的: [{sym}] | 错误: [{e}]")

            if completed_count % 5 == 0 or completed_count == total_symbols:
                print(f"[OI信号分析] 进度 | 已完成: [{completed_count}/{total_symbols}]")

    print(f"\n[OI信号分析] 阶段3: 合并数据")

    if not all_analyzed_dfs:
        print(f"[OI信号分析] 失败: 无有效分析数据")
        return pd.DataFrame()

    final_combined_df = pd.concat(all_analyzed_dfs, ignore_index=True)
    final_combined_df = final_combined_df.sort_values(
        by=['Symbol', 'timestamp'], ascending=[True, False]
    ).reset_index(drop=True)

    print(f"[OI信号分析] 完成 | 总记录: [{len(final_combined_df)}] 条")
    return final_combined_df


def get_high_volatility_merged_signals(days=7, timeframe='1h', top_k=15, max_workers=10,
                                       window=None, use_local=True):
    """
    优化版综合工作流：支持本地缓存。

    [功能摘要]
    1. 筛选高波动率合约
    2. 检查本地缓存或拉取新数据
    3. 多线程信号分析

    [输入数据]
    - use_local: 是否使用本地缓存

    [输出数据]
    DataFrame: 合并后的分析结果
    """
    print(f"\n[综合信号] 阶段1: 筛选高波动率合约")

    calc_minutes_list = [15, 30, 60, 90, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1000]
    bin_df = get_binance_volatility_ranking(minutes_list=calc_minutes_list, max_workers=20, top_n=top_k)

    if bin_df is None or bin_df.empty:
        print(f"[综合信号] 失败: 未获取到波动率数据")
        return pd.DataFrame()

    target_symbols = bin_df['Symbol'].head(top_k).tolist()

    print(f"\n[综合信号] 阶段2: 并行分析 | 本地缓存: [{'开启' if use_local else '关闭'}] | 目标数: [{len(target_symbols)}]")

    all_analyzed_dfs = []

    def process_symbol(symbol):
        try:
            safe_symbol = symbol.replace('/', '_').replace(':', '_')
            filename = f"merge_cvd_oi_funding_{safe_symbol}_{timeframe}_{days}d.csv"
            file_path = DATA_DIR / filename

            merge_cvd_oi_df = None

            # 本地缓存检查
            if use_local and file_path.exists():
                print(f"[缓存读取] 命中 | 标的: [{symbol}]")
                merge_cvd_oi_df = pd.read_csv(file_path)
                if 'timestamp' in merge_cvd_oi_df.columns:
                    merge_cvd_oi_df['timestamp'] = pd.to_datetime(merge_cvd_oi_df['timestamp'])
            else:
                print(f"[数据拉取] 开始 | 标的: [{symbol}]")
                merge_cvd_oi_df = fetch_full_market_data_with_retry(
                    exchange_name='binance', symbol=symbol, timeframe=timeframe,
                    days=days, retries=5, save_csv=True, file_path=str(file_path)
                )

            if merge_cvd_oi_df is None or merge_cvd_oi_df.empty:
                print(f"[数据处理] 跳过 | 标的: [{symbol}] | 原因: 数据为空")
                return None

            anlyse_df = calculate_oi_extremes(merge_cvd_oi_df, window=window)
            anlyse_df.insert(0, 'Symbol', symbol)
            return anlyse_df

        except Exception as e:
            print(f"[数据处理] 异常 | 标的: [{symbol}] | 错误: [{e}]")
            return None

    completed_count = 0
    total_symbols = len(target_symbols)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(process_symbol, sym): sym for sym in target_symbols}

        for future in as_completed(future_to_symbol):
            completed_count += 1
            res_df = future.result()
            if res_df is not None and not res_df.empty:
                all_analyzed_dfs.append(res_df)

            if completed_count % 5 == 0 or completed_count == total_symbols:
                print(f"[综合信号] 进度 | 已完成: [{completed_count}/{total_symbols}]")

    if not all_analyzed_dfs:
        print(f"[综合信号] 失败: 无分析结果")
        return pd.DataFrame()

    final_combined_df = pd.concat(all_analyzed_dfs, ignore_index=True)
    sort_cols = ['Symbol', 'timestamp'] if 'timestamp' in final_combined_df.columns else ['Symbol']
    final_combined_df = final_combined_df.sort_values(by=sort_cols, ascending=[True, False]).reset_index(drop=True)

    print(f"\n[综合信号] 完成 | 总行数: [{len(final_combined_df)}]")
    return final_combined_df


def continuous_incremental_update(exchange_name='binance', timeframe='5m', fetch_days=2, sleep_interval=1.5):
    """
    7x24h持续增量更新全市场数据。

    [功能摘要]
    循环遍历所有合约，增量更新本地数据。

    [输入数据]
    - exchange_name: 交易所
    - timeframe: K线周期
    - fetch_days: 每次拉取天数
    - sleep_interval: 休眠间隔
    """
    exchange = init_exchange(exchange_name, default_type=None)

    print(f"[持续更新] 初始化: 获取全市场合约列表")
    markets = exchange.load_markets()

    symbols = [
        sym for sym, market in markets.items()
        if market.get('active') and market.get('linear')
        and market.get('quote') == 'USDT' and market.get('type') == 'swap'
    ]

    print(f"[持续更新] 锁定活跃合约: [{len(symbols)}] 个 | 进入7x24h循环")

    while True:
        for i, symbol in enumerate(symbols):
            try:
                safe_symbol = symbol.replace('/', '_').replace(':', '_')
                file_path = DATA_DIR / f"merge_cvd_oi_funding_{safe_symbol}_{timeframe}_history.csv"

                print(f"\n[巡检更新] [{i + 1}/{len(symbols)}] | 标的: [{symbol}]")

                # 智能天数选择：有历史则只拉2天
                days_to_fetch = 2 if file_path.exists() else 30

                fetch_full_market_data_with_retry(
                    exchange_name=exchange_name, symbol=symbol, timeframe=timeframe,
                    days=days_to_fetch, retries=3, save_csv=True, file_path=str(file_path)
                )

                time.sleep(sleep_interval)

            except Exception as e:
                print(f"[巡检更新] 异常 | 标的: [{symbol}] | 错误: [{e}]")
                time.sleep(sleep_interval)

        print(f"\n[持续更新] 一轮完成 | 休眠3分钟后开始下一轮...")
        time.sleep(180)


# ============================================================================
# 主入口
# ============================================================================

if __name__ == "__main__":
    # 启动持续增量更新
    continuous_incremental_update(exchange_name='binance', timeframe='5m', fetch_days=30, sleep_interval=1.5)