"""
================================================================================
核心数据流摘要
================================================================================
[功能摘要]
加密货币合约市场数据获取模块。支持多交易所(Binance/OKX)的历史K线、资金费率、持仓量(OI)及CVD数据的标准化拉取与清洗。

[输入数据]
- 交易所API: 通过 ccxt 库连接 Binance/OKX，配置全局代理与限速。
- 业务参数: 交易对(如 'BTC/USDT:USDT')、时间周期(如 '1h')、历史回溯天数。

[数据流转/交互]
1. 请求构建: 根据交易所特性(Binance 1000条/次, OKX 100条/次)自动适配分页游标。
2. 数据拉取: 循环调用 ccxt 标准接口或币安私有接口(fapiPublicGetKlines)获取原始 JSON。
3. 标准化清洗: 统一转换为 DataFrame，抹平毫秒时间戳，转换为无时区标记的北京时间(Asia/Shanghai)。
4. 衍生计算: 自动计算资金费率变化率、OI涨跌幅、以及基于主买/主卖量推导的 CVD(累计成交量Delta)。

[输出数据]
- 标准化 Pandas DataFrame: 包含统一时间索引及业务衍生指标，直接供下游量化分析使用。
================================================================================
"""

import ccxt
import pandas as pd

# ============================================================================
# 全局配置常量
# ============================================================================
DEFAULT_PROXY = {
    'http': 'http://127.0.0.1:7890',
    'https': 'http://127.0.0.1:7890',
}

# 交易所单次分页拉取上限配置
EXCHANGE_LIMITS = {
    'binance': 1000,
    'okx': 100
}


# ============================================================================
# 工具函数层
# ============================================================================

def init_exchange(exchange_name, default_type='swap'):
    """
    初始化交易所实例，统一注入代理与限速配置。

    [入参形貌]
    - exchange_name: 交易所标识 (str, 如 'binance')
    - default_type: 市场类型 (str, 默认 'swap' 合约)

    [出参形貌]
    - object: ccxt 交易所实例对象
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
    统一的时间标准化管道：毫秒时间戳 → UTC → 北京时间(剥离时区标记)。

    [入参形貌]
    - df: 包含数值型毫秒时间戳的 DataFrame
    - timestamp_col: 时间列名 (str)

    [出参形貌]
    - DataFrame: 时间列已转换为无时区 datetime64[ns] 格式的副本
    """
    if df.empty or timestamp_col not in df.columns:
        return df

    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='ms', errors='coerce')
    df[timestamp_col] = df[timestamp_col].dt.tz_localize('UTC')
    df[timestamp_col] = df[timestamp_col].dt.tz_convert('Asia/Shanghai')
    df[timestamp_col] = df[timestamp_col].dt.tz_localize(None)

    return df


def fetch_with_pagination(exchange, fetch_func, symbol, timeframe, since, limit_per_request):
    """
    通用分页拉取与游标推进引擎，内置容错与熔断机制。

    [入参形貌]
    - fetch_func: 交易所拉取方法引用 (如 exchange.fetch_ohlcv)
    - limit_per_request: 单次请求最大条数 (int)

    [出参形貌]
    - list: 原始数据列表，元素为 list(如K线) 或 dict(如资金费率)
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

            # 兼容 dict(资金费率) 和 list(K线) 两种底层数据格式
            last_timestamp = data[-1].get('timestamp', 0) if isinstance(data[0], dict) else data[-1][0]

            # 防御性拦截：防止脏数据导致游标重置引发死循环
            if not last_timestamp:
                print(f"[分页拉取] 游标更新异常熔断 | 标的: [{symbol}] | 结果: [返回数据缺失时间戳，强制终止以防死循环]")
                break

            current_since = last_timestamp + 1

            # 触达最新数据边界，正常终止
            if last_timestamp >= exchange.milliseconds() - 60000:
                break

        except Exception as e:
            print(f"[分页拉取] 接口请求异常中断 | 标的: [{symbol}] | 游标: [{current_since}] | 异常: [{e}] | 结果: [已熔断并返回部分成功数据，可能原因为网络波动或触发限速]")
            break

    return all_data


# ============================================================================
# 数据拉取层
# ============================================================================

def fetch_long_history(exchange_name, symbol, timeframe='1h', days=30):
    """
    分页拉取长历史K线数据并完成时间标准化。

    [入参形貌]
    - exchange_name: 交易所标识 (str)
    - symbol: 交易对 (str, 如 'BTC/USDT:USDT')
    - timeframe: K线周期 (str, 如 '1h')
    - days: 回溯天数 (int)

    [出参形貌]
    - DataFrame: 包含 timestamp, open, high, low, close, volume 列
    """
    exchange = init_exchange(exchange_name, default_type=None)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000
    limit = EXCHANGE_LIMITS.get(exchange_name, 100)

    all_ohlcv = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, timeframe, since, limit)

    if not all_ohlcv:
        print(f"[历史K线] 数据拉取失败 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}] | 结果: [空数据或API异常]")
        return pd.DataFrame()

    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = convert_to_beijing_time(df)

    print(f"[历史K线] 数据拉取完成 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}] | 结果: [成功获取 {len(df)} 条]")
    return df


def fetch_long_funding_history(exchange_name, symbol, days=30):
    """
    分页拉取资金费率历史，计算费率变化百分比。

    [入参形貌]
    - exchange_name: 交易所标识 (str)
    - symbol: 交易对 (str)
    - days: 回溯天数 (int)

    [出参形貌]
    - DataFrame: 包含 timestamp, funding_rate, funding_rate_pct 列
    """
    exchange = init_exchange(exchange_name, default_type=None)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000
    limit = EXCHANGE_LIMITS.get(exchange_name, 100)

    all_funding = fetch_with_pagination(exchange, exchange.fetch_funding_rate_history, symbol, None, since, limit)

    if not all_funding:
        print(f"[资金费率] 数据拉取失败 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}] | 结果: [空数据或API异常]")
        return pd.DataFrame(columns=['timestamp', 'funding_rate', 'funding_rate_pct'])

    df = pd.DataFrame([{
        'timestamp': item['timestamp'],
        'funding_rate': item.get('fundingRate', 0.0) * 100
    } for item in all_funding])

    df = convert_to_beijing_time(df)
    df['timestamp'] = df['timestamp'].dt.round('s')  # 抹平毫秒级误差以对齐其他周期数据

    df['funding_rate_pct'] = df['funding_rate'].pct_change() * 100
    df['funding_rate_pct'] = df['funding_rate_pct'].fillna(0.0)

    df = df.sort_values('timestamp').reset_index(drop=True)

    print(f"[资金费率] 数据拉取完成 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}] | 结果: [成功获取 {len(df)} 条]")
    return df


def fetch_historical_oi(exchange_name, symbol, timeframe='1h', days=30):
    """
    分页拉取历史持仓量(OI)数据，计算数量与价值的涨跌幅。

    [入参形貌]
    - exchange_name: 交易所标识 (str)
    - symbol: 交易对 (str)
    - timeframe: 时间周期 (str)
    - days: 回溯天数 (int)

    [出参形貌]
    - DataFrame: 包含 timestamp, oi_amount, oi_value 及对应变化率列
    """
    exchange = init_exchange(exchange_name, default_type=None)

    if not exchange.has.get('fetchOpenInterestHistory'):
        print(f"[历史OI] 接口不支持 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 结果: [该交易所未实现 fetchOpenInterestHistory 方法]")
        return pd.DataFrame()

    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000
    limit = 500 if exchange_name == 'binance' else 100

    all_oi = fetch_with_pagination(exchange, exchange.fetch_open_interest_history, symbol, timeframe, since, limit)

    if not all_oi:
        print(f"[历史OI] 数据拉取失败 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}] | 结果: [空数据或API异常]")
        return pd.DataFrame()

    df = pd.DataFrame([{
        'timestamp': item['timestamp'],
        'oi_amount': item.get('openInterestAmount', 0),
        'oi_value': item.get('openInterestValue', 0)
    } for item in all_oi])

    df = convert_to_beijing_time(df)

    # 去重与排序
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values(by='timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 衍生指标计算
    df['oi_amount_change_pct'] = df['oi_amount'].pct_change() * 100

    # 防御性编程：处理 oi_value 全为0导致 pct_change 崩溃的情况
    if df['oi_value'].sum() > 0:
        df['oi_value_change_pct'] = df['oi_value'].pct_change() * 100
    else:
        df['oi_value_change_pct'] = 0.0

    # 清洗 NaN 并格式化
    df.fillna({'oi_amount_change_pct': 0, 'oi_value_change_pct': 0}, inplace=True)
    df['oi_amount_change_pct'] = df['oi_amount_change_pct'].round(4)
    df['oi_value_change_pct'] = df['oi_value_change_pct'].round(4)

    print(f"[历史OI] 数据拉取完成 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 天数: [{days}] | 结果: [成功获取 {len(df)} 条]")
    return df


def fetch_binance_cvd_history(symbol, timeframe='1h', days=30):
    """
    通过币安私有API分页拉取U本位合约K线，推导计算CVD(累计成交量Delta)。

    [入参形貌]
    - symbol: 交易对 (str)
    - timeframe: K线周期 (str)
    - days: 回溯天数 (int)

    [出参形貌]
    - DataFrame: 包含 timestamp, OHLCV, taker买卖量, volume_delta, cvd 列
    """
    exchange = init_exchange('binance')

    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        raw_symbol = market['id']
    except Exception as e:
        print(f"[CVD数据] 交易对解析失败 | 标的: [{symbol}] | 异常: [{e}] | 结果: [交易对名称拼写错误或该合约已下架]")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

    all_klines = []
    current_since = since

    # 币安私有接口分页逻辑
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
            print(f"[CVD数据] 接口请求异常中断 | 标的: [{symbol}] | 游标: [{current_since}] | 异常: [{e}] | 结果: [已熔断并返回部分成功数据]")
            break

    if not all_klines:
        print(f"[CVD数据] 数据拉取失败 | 标的: [{symbol}] | 天数: [{days}] | 结果: [空数据或API异常]")
        return pd.DataFrame()

    columns = [
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'trades',
        'taker_buy_base_vol', 'taker_buy_quote_vol', 'ignore'
    ]
    df = pd.DataFrame(all_klines, columns=columns)

    # 数值类型清洗
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'taker_buy_base_vol', 'taker_buy_quote_vol']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=['timestamp'] + numeric_cols)

    # CVD 核心推导
    df['taker_sell_base_vol'] = df['volume'] - df['taker_buy_base_vol']
    df['volume_delta'] = df['taker_buy_base_vol'] - df['taker_sell_base_vol']
    df['cvd'] = df['volume_delta'].cumsum()

    # 复用统一时间转换管道
    df = convert_to_beijing_time(df)
    df = df.dropna(subset=['timestamp'])

    result_df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'taker_buy_base_vol', 'taker_sell_base_vol', 'volume_delta', 'cvd']]

    print(f"[CVD数据] 数据拉取完成 | 标的: [{symbol}] | 天数: [{days}] | 结果: [成功获取 {len(result_df)} 条]")
    return result_df


def get_open_interest(symbol, exchange_name='binance'):
    """
    获取指定合约的实时持仓量(OI)快照。

    [入参形貌]
    - symbol: 交易对 (str)
    - exchange_name: 交易所标识 (str)

    [出参形貌]
    - dict: 包含 OI 数量、价值及时间戳的字典，失败返回 None
    """
    exchange = init_exchange(exchange_name, default_type=None)

    try:
        if not exchange.has.get('fetchOpenInterest'):
            print(f"[实时OI] 接口不支持 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 结果: [该交易所未实现 fetchOpenInterest 方法]")
            return None

        oi_data = exchange.fetch_open_interest(symbol)
        base_volume = oi_data.get('openInterestAmount')
        quote_value = oi_data.get('openInterestValue')

        print(f"[实时OI] 数据拉取完成 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 结果: [数量 {base_volume} | 价值 {quote_value} USDT]")

        return {
            'Symbol': symbol,
            'Exchange': exchange_name.upper(),
            'OI (Base Coin)': base_volume,
            'OI Value (USDT)': quote_value,
            'Timestamp': oi_data.get('timestamp'),
            'Datetime': oi_data.get('datetime')
        }

    except Exception as e:
        print(f"[实时OI] 数据拉取失败 | 交易所: [{exchange_name}] | 标的: [{symbol}] | 异常: [{e}] | 结果: [网络中断或标的已下架]")
        return None


# ============================================================================
# 新增：溢价指数与预测费率重构模块 (支撑 LER 方案 Step 0 数据规格)
# ============================================================================

def fetch_premium_index_klines(symbol, timeframe='5m', days=30):
    """
    通过币安私有API分页拉取U本位合约的 历史溢价指数(Premium Index) K线。
    这是 LER 策略回测中计算“无未来函数的预测资金费率”的唯一正确数据源。

    [入参形貌]
    - symbol: 交易对 (str, 必须是 ccxt 标准格式，如 'BTC/USDT:USDT')
    - timeframe: K线周期 (str, 如 '5m')
    - days: 回溯天数 (int)

    [出参形貌]
    - DataFrame: 包含 timestamp, premium_open, premium_high, premium_low, premium_close
    """
    exchange = init_exchange('binance')

    try:
        # 将 ccxt 标准 symbol (BTC/USDT:USDT) 转换为币安原生 symbol (BTCUSDT)
        exchange.load_markets()
        market = exchange.market(symbol)
        raw_symbol = market['id']
    except Exception as e:
        print(f"[溢价指数] 交易对解析失败 | 标的: [{symbol}] | 异常: [{e}]")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

    all_klines = []
    current_since = since

    print(f"[溢价指数] 开始拉取 | 标的: [{symbol}] | 周期: [{timeframe}] | 天数: [{days}]")

    while True:
        try:
            # 调用币安 U本位合约专有接口：拉取溢价指数 K线
            params = {
                'symbol': raw_symbol,
                'interval': timeframe_id,
                'startTime': current_since,
                'limit': 1000
            }
            # ccxt 隐式 API 调用
            curr_klines = exchange.fapiPublicGetPremiumIndexKlines(params)

            if not curr_klines:
                break

            all_klines.extend(curr_klines)
            last_timestamp = int(curr_klines[-1][0])
            current_since = last_timestamp + 1

            if last_timestamp >= exchange.milliseconds() - 60000:
                break

        except Exception as e:
            print(f"[溢价指数] 请求异常中断 | 游标: [{current_since}] | 异常: [{e}]")
            break

    if not all_klines:
        print(f"[溢价指数] 拉取失败: 空数据")
        return pd.DataFrame()

    # 提取有用的列：时间戳与OHLC，忽略成交量（溢价指数没有成交量）
    columns = ['timestamp', 'premium_open', 'premium_high', 'premium_low', 'premium_close',
               'ignore1', 'close_time', 'ignore2', 'ignore3', 'ignore4', 'ignore5', 'ignore6']
    df = pd.DataFrame(all_klines, columns=columns)

    # 数据类型转换与清洗
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    for col in ['premium_open', 'premium_high', 'premium_low', 'premium_close']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # 丢弃无用列并复用你的时间清洗管道
    df = df[['timestamp', 'premium_open', 'premium_high', 'premium_low', 'premium_close']].dropna()
    df = convert_to_beijing_time(df)

    print(f"[溢价指数] 数据拉取完成 | 标的: [{symbol}] | 结果: [成功获取 {len(df)} 条]")
    return df


def main():
    """
    全管线拉取测试：模拟回测系统启动时的数据准备阶段 (Step 0)
    """
    print("=" * 80)
    print(" LER (Liquidation Exhaustion Reversal) 强平耗竭回归系统 - 数据流测试启动")
    print("=" * 80)

    # ---------------------------
    # 1. 设定测试参数
    # ---------------------------
    test_symbol = 'BTC/USDT:USDT'
    test_timeframe = '5m'  # LER方案核心基于 5分钟级别 残段
    test_days = 2  # 为了测试速度，只拉取最近 2 天的数据
    exchange = 'binance'

    print(f"--> 测试标的: {test_symbol}")
    print(f"--> 测试周期: {test_timeframe}")
    print(f"--> 回溯天数: {test_days} 天\n")

    # ---------------------------
    # 2. 并行/串行拉取各项核心数据
    # ---------------------------

    # [模块 A] 价格K线数据 (用于Gate B 跌幅校验、系统止盈止损计算)
    df_klines = fetch_long_history(exchange, test_symbol, timeframe=test_timeframe, days=test_days)

    # [模块 B] 历史持仓量 OI (用于物理燃料湮灭确认)
    df_oi = fetch_historical_oi(exchange, test_symbol, timeframe=test_timeframe, days=test_days)

    # [模块 C] CVD 数据 (用于Gate B 纯粹抛压诊断)
    df_cvd = fetch_binance_cvd_history(test_symbol, timeframe=test_timeframe, days=test_days)

    # [模块 D] 溢价指数 (新加：用于历史预测费率重构，防止未来函数)
    df_premium = fetch_premium_index_klines(test_symbol, timeframe=test_timeframe, days=test_days)

    print("\n" + "=" * 80)
    print(" 数据拉取汇总质量检查")
    print("=" * 80)

    # ---------------------------
    # 3. 数据校验与合并演示
    # ---------------------------
    if not df_klines.empty and not df_oi.empty and not df_cvd.empty and not df_premium.empty:
        # 将所有数据按 timestamp (对齐到北京时间) 进行合并 (Merge)
        # 实际回测中，必须确保时间戳严格对齐
        df_merged = df_klines.merge(df_oi[['timestamp', 'oi_amount', 'oi_amount_change_pct']], on='timestamp',
                                    how='inner')
        df_merged = df_merged.merge(df_cvd[['timestamp', 'cvd']], on='timestamp', how='inner')
        df_merged = df_merged.merge(df_premium[['timestamp', 'premium_close']], on='timestamp', how='inner')

        print("\n[成功] 核心数据集已全部合并完成！")
        print(f"合并后总数据条数: {len(df_merged)}")

        # 打印最新3条数据，审查字段规格
        print("\n[数据切片预览 (最新3条)]: ")
        # 挑选LER核心字段展示
        preview_cols = ['timestamp', 'close', 'oi_amount', 'oi_amount_change_pct', 'cvd', 'premium_close']
        print(df_merged[preview_cols].tail(3).to_markdown(index=False))

        print("\n--> LER 数据管线 (Step 0) 测试通过，可开始后续特征工程计算 (如滚动分位数、ATR等)。")
    else:
        print("\n[警告] 部分数据拉取失败，请检查网络代理或交易所接口限速！")



if __name__ == "__main__":
    # 确保本地已启动代理，端口与 DEFAULT_PROXY 匹配
    main()