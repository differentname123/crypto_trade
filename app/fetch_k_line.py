from pathlib import Path

import ccxt
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_long_history(exchange_name, symbol, timeframe='1h', days=30):
    """
    分页拉取长历史数据并转换为北京时间
    :param days: 获取过去多少天的数据
    """
    exchange_class = getattr(ccxt, exchange_name)
    exchange = exchange_class({
        'enableRateLimit': True,
        'proxies': {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890',
        },
    })

    # 计算起始时间戳 (毫秒)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000

    all_ohlcv = []

    print(f"开始拉取 {exchange_name} 的 {symbol} 历史数据...")

    while True:
        try:
            # 每次拉取数据，since 随循环更新
            # 币安 limit 最大 1000，欧易 limit 最大 100
            limit = 1000 if exchange_name == 'binance' else 100
            curr_ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=limit)

            if not curr_ohlcv:
                break

            all_ohlcv.extend(curr_ohlcv)

            # 更新 since 为最后一条数据的时间戳 + 1毫秒，避免重复
            last_timestamp = curr_ohlcv[-1][0]
            since = last_timestamp + 1

            print(f"已获取到: {pd.to_datetime(last_timestamp, unit='ms')}，累计 {len(all_ohlcv)} 条")

            # 如果最后一条数据的时间已经接近当前时间，则停止
            if last_timestamp >= exchange.milliseconds() - 60000:  # 1分钟内
                break

            # 尊重频率限制
            # time.sleep(exchange.rateLimit / 1000)

        except Exception as e:
            print(f"拉取出错: {e}")
            break

    # 转换为 DataFrame
    df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

    # --- 时间处理核心步骤 ---
    # 1. 转换为 UTC 时间
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC')
    # 2. 转换为北京时间
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Shanghai')
    # 3. (可选) 如果不需要显示时区后缀，可以转为无时区格式
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)

    return df


def get_binance_volatility_ranking(minutes_list=[15, 30, 60], max_workers=100):
    """
    获取币安 U本位永续合约的多维度平均分钟波动率及资金费率。
    使用多线程并发拉取，从高到低排序。

    :param minutes_list: 需要计算的过去分钟数的列表，例如 [15, 30, 60]
    :param max_workers: 线程池并发数，建议 10-20，过高容易触发币安 IP 频率限制
    """
    if isinstance(minutes_list, int):
        minutes_list = [minutes_list]

    max_minutes = max(minutes_list)

    print(f"\n==========================================")
    print(f"🚀 开始并发获取币安合约波动率 & 资金费率")
    print(f"时间维度: {minutes_list} 分钟 | 并发线程数: {max_workers}")
    print(f"==========================================")

    # 实例化交易所
    exchange = ccxt.binance({
        'enableRateLimit': True,  # 开启内置速率限制保护
        'options': {
            'defaultType': 'swap',
        },
        'proxies': {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890',
        },
    })

    print("正在加载币安合约市场数据...")
    markets = exchange.load_markets()

    # 过滤出所有活跃的 U本位永续合约
    symbols = [
        symbol for symbol, market in markets.items()
        if market.get('active')
           and market.get('linear')
           and market.get('quote') == 'USDT'
           and market.get('type') == 'swap'
    ]
    print(f"共发现 {len(symbols)} 个交易中的 U本位永续合约。")

    # ==========================================
    # 批量获取资金费率 (只需 1 次 API 请求)
    # ==========================================
    print("正在批量拉取全市场最新资金费率...")
    try:
        funding_rates_data = exchange.fetch_funding_rates(symbols)
    except Exception as e:
        print(f"⚠️ 获取资金费率失败: {e}")
        funding_rates_data = {}

    if max_minutes > 1000:
        print("⚠️ 警告: 请求的分钟数超过了 1000 分钟。已截断为 1000 条。")
        limit = 1000
    else:
        limit = max_minutes

    now = exchange.milliseconds()
    since = now - int(limit * 60 * 1000)

    results = []

    # ==========================================
    # 定义单线程处理函数
    # ==========================================
    def fetch_and_calc(symbol):
        try:
            # 拉取 K 线数据 (公开接口，无需签名)
            ohlcv = exchange.fetch_ohlcv(symbol, '1m', since=since, limit=limit)

            if not ohlcv or len(ohlcv) < (limit * 0.8):
                return None  # 数据不足，丢弃

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = df[df['low'] > 0]

            # 计算波动率
            df['volatility'] = (df['high'] - df['low']) / df['low'] * 100
            latest_price = df['close'].iloc[-1]

            # 获取该币种的资金费率（转化为百分比，通常资金费率为 0.0001 表示 0.01%）
            fr_info = funding_rates_data.get(symbol, {})
            funding_rate = fr_info.get('fundingRate', 0)
            funding_rate_pct = funding_rate * 100 if funding_rate else 0.0

            symbol_data = {
                'Symbol': symbol,
                'Latest Price': latest_price,
                'Funding Rate %': funding_rate_pct
            }

            temp_vols = []
            for m in minutes_list:
                df_m = df.tail(m)
                avg_vol = df_m['volatility'].mean()
                symbol_data[f'Avg Vol ({m}m) %'] = avg_vol
                temp_vols.append(avg_vol)

            # 计算综合平均波动率
            symbol_data['Overall Avg Vol %'] = sum(temp_vols) / len(temp_vols)
            return symbol_data

        except Exception as e:
            return None

    # ==========================================
    # 启动多线程并发执行
    # ==========================================
    print(f"正在启动 {max_workers} 个线程拉取 {limit} 根 K线数据...")

    completed_count = 0
    total_symbols = len(symbols)

    # 使用 ThreadPoolExecutor 管理并发
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务到线程池
        future_to_symbol = {executor.submit(fetch_and_calc, sym): sym for sym in symbols}

        # as_completed 会在某个线程完成时立刻 yield
        for future in as_completed(future_to_symbol):
            completed_count += 1
            res = future.result()
            if res is not None:
                results.append(res)

            # 打印进度条
            if completed_count % 50 == 0 or completed_count == total_symbols:
                print(f"进度: 已处理 {completed_count}/{total_symbols} 个合约...")

    df_res = pd.DataFrame(results)

    if not df_res.empty:
        # 强制排列列的顺序：基础信息 -> 综合波动率 -> 各维度波动率
        col_order = ['Symbol', 'Latest Price', 'Funding Rate %', 'Overall Avg Vol %'] + [f'Avg Vol ({m}m) %' for m in
                                                                                         minutes_list]
        df_res = df_res[col_order]

        # 按照“综合平均波动率”降序排序
        df_res = df_res.sort_values(by='Overall Avg Vol %', ascending=False).reset_index(drop=True)

        # 格式化小数位数
        df_res['Funding Rate %'] = df_res['Funding Rate %'].round(4)
        df_res['Overall Avg Vol %'] = df_res['Overall Avg Vol %'].round(4)
        for m in minutes_list:
            df_res[f'Avg Vol ({m}m) %'] = df_res[f'Avg Vol ({m}m) %'].round(4)

    return df_res


def get_binance_futures_change(hours=48):
    """
    获取币安所有U本位合约过去 N 小时的涨跌幅，并从高到低排序
    :param hours: 过去多少小时，默认为 48
    """
    print(f"==========================================")
    print(f"开始获取币安合约过去 {hours} 小时的涨跌幅排行")
    print(f"==========================================")

    exchange = ccxt.binance({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap',  # 指定默认市场类型为永续合约 (Swap)
        },
        'proxies': {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890',
        },
    })

    print("正在加载币安合约市场数据...")
    markets = exchange.load_markets()

    # 过滤出所有处于激活状态的 U本位永续合约 (USDT 为计价和结算货币)
    # 过滤出所有处于激活状态的 U本位永续合约 (USDT 为计价和结算货币，且必须是 swap 永续类型)
    symbols = [
        symbol for symbol, market in markets.items()
        if market.get('active')
           and market.get('linear')
           and market.get('quote') == 'USDT'
           and market.get('type') == 'swap'  # 新增：严格限制为永续合约，剔除交割合约
    ]
    print(f"共发现 {len(symbols)} 个交易中的 U本位永续合约。")

    # 计算时间戳
    now = exchange.milliseconds()
    since = now - int(hours * 60 * 60 * 1000)

    print("正在批量获取当前最新价格...")
    # 批量获取所有合约的 ticker 数据非常快，消耗权重低
    tickers = exchange.fetch_tickers(symbols)

    results = []

    print(f"正在拉取 {hours} 小时前的历史价格 (这需要向币安发送数百个请求，请耐心等待大约半分钟)...")

    for i, symbol in enumerate(symbols):
        try:
            # 拉取 1小时 级别的 K线，限定只拿1条，以获取 N 小时前那一刻的价格
            ohlcv = exchange.fetch_ohlcv(symbol, '1h', since=since, limit=1)

            if not ohlcv:
                continue

            historical_timestamp = ohlcv[0][0]
            historical_price = ohlcv[0][1]  # 取该小时的开盘价作为基准历史价
            current_price = tickers[symbol].get('last')

            # 剔除数据缺失或刚上线不足规定时间的币种
            # 如果获取到的 K线时间比我们要求的 since 晚了超过2小时，说明这个币是新币，历史数据不足 N 小时
            if not current_price or (historical_timestamp - since > 2 * 60 * 60 * 1000):
                continue

            # 计算涨跌幅: (现价 - 历史价) / 历史价 * 100
            change_pct = (current_price - historical_price) / historical_price * 100

            results.append({
                'Symbol': symbol,
                'Current Price (USDT)': current_price,
                f'Price {hours}h Ago': historical_price,
                'Change (%)': change_pct
            })

            # 打印进度条
            if (i + 1) % 50 == 0 or (i + 1) == len(symbols):
                print(f"进度: 已处理 {i + 1}/{len(symbols)} 个合约...")

        except Exception as e:
            # 忽略个别拉取失败的标的
            continue

    # 转换为 DataFrame 并按涨跌幅从高到低排序
    df = pd.DataFrame(results)
    if not df.empty:
        df = df.sort_values(by='Change (%)', ascending=False).reset_index(drop=True)
        # 将涨跌幅保留两位小数，提升可读性
        df['Change (%)'] = df['Change (%)'].round(2)

    return df


def gen_csv_file():
    # ==========================================
    # 1. 核心参数配置区
    # ==========================================
    exchange = 'okx'  # 交易平台
    target_symbol = 'ETH/USDT:USDT'  # 交易对
    timeframe = "1m"  # 时间粒度
    days_to_fetch = 3  # 时间范围（天数）

    # ==========================================
    # 2. 路径与文件名处理
    # ==========================================
    # 使用 pathlib 处理路径，自动适配不同操作系统的路径分隔符
    data_dir = Path(r"W:\project\python_project\crypto_trade\data")

    # 确保保存目录存在，如果不存在则自动创建
    data_dir.mkdir(parents=True, exist_ok=True)

    # 替换交易对中的特殊字符（/ 和 : 在 Windows/Linux 文件名中会导致报错或解析问题）
    safe_symbol = target_symbol.replace('/', '_').replace(':', '_')


    # 动态生成带有明确信息的文件名
    # 最终格式示例: okx_ETH_USDT_USDT_1m_1days_20260406.csv
    filename = f"{exchange}_{safe_symbol}_{timeframe}_{days_to_fetch}days.csv"
    csv_file_path = data_dir / filename
    return csv_file_path


if __name__ == "__main__":
    # # ==========================================
    # # 1. 核心参数配置区
    # # ==========================================
    # exchange = 'okx'  # 交易平台
    # target_symbol = 'ETH/USDT:USDT'  # 交易对
    # timeframe = "1s"  # 时间粒度
    # days_to_fetch = 30  # 时间范围（天数）
    #
    # # ==========================================
    # # 2. 路径与文件名处理
    # # ==========================================
    # # 使用 pathlib 处理路径，自动适配不同操作系统的路径分隔符
    # data_dir = Path(r"W:\project\python_project\crypto_trade\data")
    #
    # # 确保保存目录存在，如果不存在则自动创建
    # data_dir.mkdir(parents=True, exist_ok=True)
    #
    # # 替换交易对中的特殊字符（/ 和 : 在 Windows/Linux 文件名中会导致报错或解析问题）
    # safe_symbol = target_symbol.replace('/', '_').replace(':', '_')
    #
    #
    # # 动态生成带有明确信息的文件名
    # # 最终格式示例: okx_ETH_USDT_USDT_1m_1days_20260406.csv
    # filename = f"{exchange}_{safe_symbol}_{timeframe}_{days_to_fetch}days.csv"
    # csv_file_path = gen_csv_file()
    #
    # # ==========================================
    # # 3. 数据获取与保存
    # # ==========================================
    # print(f"正在从 {exchange} 获取 {target_symbol} ({timeframe}, 最近 {days_to_fetch} 天) 的历史数据...")
    #
    # # 统一使用配置好的变量传入函数，避免硬编码
    # history_data = fetch_long_history(
    #     exchange,
    #     target_symbol,
    #     timeframe=timeframe,
    #     days=days_to_fetch
    # )
    #
    # # 保存文件
    # history_data.to_csv(csv_file_path, index=False)
    # print(f"✅ 数据已成功保存至:\n{csv_file_path}")


    # # 获取合约指定时间的涨跌幅
    # change_hours = 2
    # df_ranking = get_binance_futures_change(hours=change_hours)
    #
    # print(f"\n✅ 币安合约过去 {change_hours} 小时涨跌幅排行 (前 10 名):")
    # print(df_ranking.head(10).to_string())


    # # 获取合约指定时间的波动率

    calc_minutes_list = [15, 30, 60, 90, 120, 180, 240, 300, 360, 420, 480, 540, 600]

    df_volatility = get_binance_volatility_ranking(minutes_list=calc_minutes_list)

    print(f"\n✅ 币安合约多维度波动率排行 (默认按 {calc_minutes_list[0]}m 排序，前 15 名):")
    # 为了防止控制台打印时列被折叠，稍微调整一下 Pandas 显示设置
    pd.set_option('display.max_columns', None)