"""
================================================================================
核心数据流摘要
================================================================================
[功能摘要]
加密货币合约市场数据获取模块 (fetch_data)。
支持多交易所(Binance/OKX)的历史数据、资金费率、持仓量及CVD数据的标准化拉取。

[输入数据]
- 交易所API: 通过ccxt库连接Binance/OKX，获取OHLCV、OI、Funding Rate、Ticker等数据
- 配置参数: 交易对、时间周期、历史天数等

[输出数据]
- 标准化的 DataFrame (包含统一的北京时间转换)
================================================================================
"""

from pathlib import Path
import ccxt
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