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
import time
import os

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
    exchange_class = getattr(ccxt, exchange_name)
    config = {
        'enableRateLimit': True,
        'proxies': DEFAULT_PROXY,
    }
    if default_type:
        config['options'] = {'defaultType': default_type}

    return exchange_class(config)


def convert_to_beijing_time(df, timestamp_col='timestamp'):
    if df.empty or timestamp_col not in df.columns:
        return df

    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], unit='ms', errors='coerce')
    df[timestamp_col] = df[timestamp_col].dt.tz_localize('UTC')
    df[timestamp_col] = df[timestamp_col].dt.tz_convert('Asia/Shanghai')
    df[timestamp_col] = df[timestamp_col].dt.tz_localize(None)

    # 强制抹平毫秒误差，保证合并无懈可击
    df[timestamp_col] = df[timestamp_col].dt.round('s')

    return df


def fetch_with_pagination(exchange, fetch_func, symbol, timeframe, since, limit_per_request):
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

            if isinstance(data[0], dict):
                last_timestamp = int(data[-1].get('timestamp', 0))
            else:
                last_timestamp = int(data[-1][0])

            if not last_timestamp:
                print(f"[分页拉取] 游标更新异常熔断 | 标的: [{symbol}] | 结果: [返回数据缺失时间戳]")
                break

            current_since = last_timestamp + 1

            if last_timestamp >= exchange.milliseconds() - 60000:
                break

            time.sleep(0.05)

        except Exception as e:
            print(f"[分页拉取] 接口请求异常中断 | 标的: [{symbol}] | 游标: [{current_since}] | 异常: [{e}]")
            break

    return all_data


# ============================================================================
# 数据拉取层
# ============================================================================

def fetch_long_history(exchange_name, symbol, timeframe='1h', days=30):
    exchange = init_exchange(exchange_name, default_type=None)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    limit = EXCHANGE_LIMITS.get(exchange_name, 100)

    all_ohlcv = fetch_with_pagination(exchange, exchange.fetch_ohlcv, symbol, timeframe, since, limit)

    if not all_ohlcv:
        print(f"[历史K线] 数据拉取失败 | 标的: [{symbol}] | 天数: [{days}]")
        return pd.DataFrame()

    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
    df = convert_to_beijing_time(df)

    print(f"[历史K线] 数据拉取完成 | 标的: [{symbol}] | 结果: [成功获取 {len(df)} 条]")
    return df


def fetch_historical_oi(exchange, symbol, timeframe='1h', days=30):
    """拉取历史持仓量 (OI) 数据并计算无未来函数的变化率特征"""
    if not exchange.has.get('fetchOpenInterestHistory'):
        print(f"[历史OI] 接口不支持 | 标的: [{symbol}] | 结果: [跳过拉取]")
        return pd.DataFrame()

    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)
    limit = 500 if exchange.id == 'binance' else 100

    all_oi = fetch_with_pagination(exchange, exchange.fetch_open_interest_history, symbol, timeframe, since, limit)
    if not all_oi:
        print(f"[历史OI] 数据拉取失败 | 标的: [{symbol}] | 天数: [{days}] | 结果: [返回空数据]")
        return pd.DataFrame()

    df = pd.DataFrame([{
        'timestamp': item['timestamp'],
        'oi_amount': item.get('openInterestAmount', 0),
        'oi_value': item.get('openInterestValue', 0)
    } for item in all_oi])

    df = convert_to_beijing_time(df)
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)
    df.sort_values(by='timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 计算 OI 变化率。
    # 物理意义：当前时间戳 T 的 pct_change 代表 [T-1, T] 区间内的 OI 变化。
    # 这是在 T 时刻完全可见的截面衍生特征，严格杜绝未来函数(移除了原先致命的 shift(-1))。
    df['oi_amount_change_pct'] = df['oi_amount'].pct_change() * 100

    df.dropna(subset=['oi_amount_change_pct'], inplace=True)
    df['oi_amount_change_pct'] = df['oi_amount_change_pct'].round(4)

    print(f"[历史OI] 数据拉取完成 | 标的: [{symbol}] | 结果: [成功获取 {len(df)} 条]")
    return df

def fetch_binance_cvd_history(symbol, timeframe='1h', days=30):
    exchange = init_exchange('binance')
    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        raw_symbol = market['id']
    except Exception as e:
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)

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
            if not curr_klines: break

            all_klines.extend(curr_klines)
            last_timestamp = int(curr_klines[-1][0])
            current_since = last_timestamp + 1

            if last_timestamp >= exchange.milliseconds() - 60000: break
            time.sleep(0.05)
        except Exception as e:
            break

    if not all_klines:
        print(f"[CVD数据] 数据拉取失败 | 标的: [{symbol}] | 天数: [{days}]")
        return pd.DataFrame()

    columns = [
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'trades',
        'taker_buy_base_vol', 'taker_buy_quote_vol', 'ignore'
    ]
    df = pd.DataFrame(all_klines, columns=columns)

    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'taker_buy_base_vol']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=['timestamp'] + numeric_cols)

    df['taker_sell_base_vol'] = df['volume'] - df['taker_buy_base_vol']
    df['volume_delta'] = df['taker_buy_base_vol'] - df['taker_sell_base_vol']
    df['cvd'] = df['volume_delta'].cumsum()

    df = convert_to_beijing_time(df)
    df = df.dropna(subset=['timestamp'])

    result_df = df[['timestamp', 'taker_buy_base_vol', 'taker_sell_base_vol', 'volume_delta', 'cvd']]

    print(f"[CVD数据] 数据拉取完成 | 标的: [{symbol}] | 结果: [成功获取 {len(result_df)} 条]")
    return result_df


def fetch_premium_index_klines(symbol, timeframe='5m', days=30):
    exchange = init_exchange('binance')
    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        raw_symbol = market['id']
    except Exception as e:
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - int(days * 24 * 60 * 60 * 1000)

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
            curr_klines = exchange.fapiPublicGetPremiumIndexKlines(params)
            if not curr_klines: break

            all_klines.extend(curr_klines)
            last_timestamp = int(curr_klines[-1][0])
            current_since = last_timestamp + 1

            if last_timestamp >= exchange.milliseconds() - 60000: break
            time.sleep(0.05)
        except Exception as e:
            break

    if not all_klines:
        print(f"[溢价指数] 拉取失败: 空数据")
        return pd.DataFrame()

    columns = ['timestamp', 'premium_open', 'premium_high', 'premium_low', 'premium_close',
               'ignore1', 'close_time', 'ignore2', 'ignore3', 'ignore4', 'ignore5', 'ignore6']
    df = pd.DataFrame(all_klines, columns=columns)

    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')
    df['premium_close'] = pd.to_numeric(df['premium_close'], errors='coerce')

    df = df[['timestamp', 'premium_close']].dropna()
    df = convert_to_beijing_time(df)

    # LER策略逻辑：推导无未来函数的预测资金费率
    df['predicted_funding_rate'] = (df['premium_close'] + 0.0001) * 100

    print(f"[溢价指数] 数据拉取完成 | 标的: [{symbol}] | 结果: [成功获取 {len(df)} 条]")
    return df


# ============================================================================
# 聚合管线引擎：组合所有数据为回测可用 DataFrame
# ============================================================================

def prepare_ler_backtest_data(symbol, timeframe='5m', days=29.5, save_dir='./data'):
    """
    聚合管线：每一行的时间戳（如 17:45:00）代表一个 5 分钟切片的起点。

    严格的时间语义对齐如下：
    1. 期初可见特征：oi_amount 是 17:45:00 瞬间的持仓快照；oi_amount_change_pct 是过去 5 分钟（17:40~17:45）的持仓变化率。
    2. 随后演化标签：K线/CVD/预测费率 记录了随后 5 分钟（17:45~17:50）内的交易演化过程。

    此结构直接支持“用期初可见特征预测随后5分钟走势”的无未来函数回测。
    """
    print("=" * 80)
    print(f"🚀 开始拉取 LER 策略回测数据 | 标的: {symbol} | 周期: {timeframe} | 基础天数: {days}天")
    print("=" * 80)

    exchange = 'binance'

    # 准备本地文件路径
    safe_name = symbol.replace('/', '_').replace(':', '_')
    file_path = os.path.join(save_dir, f"{safe_name}_{timeframe}_ler_data.csv")

    # ================= 增量拉取与本地数据加载逻辑 =================
    local_df = pd.DataFrame()
    if os.path.exists(file_path):
        try:
            local_df = pd.read_csv(file_path)
            local_df['timestamp'] = pd.to_datetime(local_df['timestamp'])
            print(f"[*] 成功加载本地历史数据: {len(local_df)} 条")
        except Exception as e:
            print(f"[警告] 读取本地数据失败: {e}")
            local_df = pd.DataFrame()

    pull_days = days
    if not local_df.empty:
        last_local_ts = local_df['timestamp'].max()
        # 使用北京时间计算差值
        now_bj = pd.Timestamp.now(tz='Asia/Shanghai').tz_localize(None)
        delta_days = (now_bj - last_local_ts).total_seconds() / (24 * 3600)
        # 覆盖1天，即加上1天，且至少为1天
        pull_days = max(delta_days + 1.0, 1.0)
        print(f"[*] 本地最新数据时间: {last_local_ts}，计算增量拉取天数: {pull_days:.2f} 天")
    # ============================================================

    # 1. 独立拉取各项数据 (使用增量天数 pull_days)
    df_klines = fetch_long_history(exchange, symbol, timeframe, pull_days)
    df_oi = fetch_historical_oi(exchange, symbol, timeframe, pull_days)
    df_cvd = fetch_binance_cvd_history(symbol, timeframe, pull_days)
    df_premium = fetch_premium_index_klines(symbol, timeframe, pull_days)

    if df_klines.empty or df_oi.empty or df_cvd.empty or df_premium.empty:
        print("\n[警告] 部分数据拉取失败，合并终止。")
        return None

    # 2. 内连接合并数据，严格对齐期初时间戳
    print("\n[*] 正在对齐并合并数据矩阵...")
    df_merged = df_klines
    df_merged = df_merged.merge(df_oi[['timestamp', 'oi_amount', 'oi_amount_change_pct']], on='timestamp', how='inner')
    df_merged = df_merged.merge(df_cvd[['timestamp', 'cvd']], on='timestamp', how='inner')
    df_merged = df_merged.merge(df_premium[['timestamp', 'premium_close', 'predicted_funding_rate']], on='timestamp', how='inner')

    # ================= 增量合并与去重逻辑 =================
    if not local_df.empty:
        print("[*] 正在与本地历史数据进行增量合并...")
        df_merged['timestamp'] = pd.to_datetime(df_merged['timestamp'])

        # 拼接本地和新数据，新数据在后
        combined_df = pd.concat([local_df, df_merged])
        # 按时间排序，并对 timestamp 去重，保留最后出现的（即新拉取的数据覆盖老数据）
        combined_df = combined_df.sort_values('timestamp').drop_duplicates(subset=['timestamp'], keep='last')
        df_merged = combined_df
        print(f"[*] 增量合并完成，当前总数据量: {len(df_merged)} 条 (未截断历史数据，持续扩大中)")
    # =====================================================

    # 3. 结果保存
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

    df_merged.to_csv(file_path, index=False)

    print("=" * 80)
    print(f"✅ 数据聚合完成！共合并 {len(df_merged)} 条有效特征。")
    print(f"📁 数据已保存至: {file_path}")
    print("=" * 80)

    print("\n[核心特征截取预览 (最新 3 条)]:")
    preview_cols = ['timestamp', 'close', 'oi_amount_change_pct', 'cvd', 'predicted_funding_rate']
    print(df_merged[preview_cols].tail(3))
    print("\n--> LER 数据管线 (Step 0) 已达军工级纯度，可直接进行回测。")

    return df_merged


def get_top_volume_symbols(exchange_name='binance', top_n=100, quote_currency='USDT'):
    """获取当前成交量前 top_n 的合约标的"""
    exchange = init_exchange(exchange_name, default_type='swap')
    exchange.load_markets()
    tickers = exchange.fetch_tickers()

    # 过滤出指定 quote currency 的永续合约 (如 :USDT)
    target_symbols = []
    for symbol, ticker in tickers.items():
        if symbol.endswith(f':{quote_currency}'):
            target_symbols.append(ticker)

    # 按 quoteVolume 降序排序
    sorted_tickers = sorted(target_symbols, key=lambda x: float(x.get('quoteVolume') or 0), reverse=True)

    return [t['symbol'] for t in sorted_tickers[:top_n]]


if __name__ == "__main__":
    loop_interval_seconds = 3600  # 每次循环间隔时间(秒)，5m级别数据建议1小时以上避免频繁触发限速

    while True:
        print("\n" + "="*80)
        print(f"🔄 [{pd.Timestamp.now()}] 开始新一轮循环，获取成交量 Top 100 币种...")
        print("="*80)

        try:
            top_symbols = get_top_volume_symbols('binance', 100, 'USDT')
            print(f"[*] 成功获取 Top 100 币种，例如: {top_symbols[:5]}...")
        except Exception as e:
            print(f"[错误] 获取 Top 100 币种失败: {e}")
            time.sleep(60)
            continue

        for idx, sym in enumerate(top_symbols):
            print(f"\n[{idx+1}/100] 正在处理标的: {sym}")
            try:
                # 首次运行或本地无数据时，days 参数生效；后续增量拉取会自动覆盖计算
                df_final = prepare_ler_backtest_data(symbol=sym, timeframe='5m', days=29.5)
            except Exception as e:
                print(f"[错误] 处理 {sym} 时发生异常: {e}")

        print(f"\n✅ 本轮 Top 100 拉取完毕，休眠 {loop_interval_seconds} 秒后进入下一轮...")
        time.sleep(loop_interval_seconds)
