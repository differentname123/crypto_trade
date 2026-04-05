from pathlib import Path

import ccxt
import pandas as pd
import time


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

def get_binance_volatility_ranking(minutes_list=[15, 30, 60]):
    """
    获取币安 U本位永续合约多个时间段的平均分钟波动率，并计算综合平均波动率，从高到低排序。
    :param minutes_list: 包含需要计算的过去分钟数的列表，例如 [15, 30, 60]
    """
    # 兼容处理：如果用户不小心传入了单个整数，转成列表
    if isinstance(minutes_list, int):
        minutes_list = [minutes_list]

    max_minutes = max(minutes_list)

    print(f"\n==========================================")
    print(f"开始获取币安合约波动率排行")
    print(f"时间维度: {minutes_list} 分钟")
    print(f"==========================================")

    exchange = ccxt.binance({
        'enableRateLimit': True,
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

    # 限制单次拉取的最大上限 (币安 fetch_ohlcv 单次最多一般是 1000 条)
    if max_minutes > 1000:
        print("⚠️ 警告: 请求的分钟数超过了 1000 分钟。已将拉取条数截断为 1000 条 (约 16.6 小时)。")
        limit = 1000
    else:
        limit = max_minutes

    now = exchange.milliseconds()
    since = now - int(limit * 60 * 1000)

    results = []
    print(f"正在逐个拉取最大所需的 {limit} 根 K线数据 (约需要几十秒，请稍候)...")

    for i, symbol in enumerate(symbols):
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, '1m', since=since, limit=limit)

            # 如果获取的数据极少，直接跳过
            if not ohlcv or len(ohlcv) < (limit * 0.8):
                continue

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df = df[df['low'] > 0]

            # 基础波动率计算: (High - Low) / Low * 100
            df['volatility'] = (df['high'] - df['low']) / df['low'] * 100

            latest_price = df['close'].iloc[-1]

            # 初始化当前币种的数据字典
            symbol_data = {
                'Symbol': symbol,
                'Latest Price': latest_price
            }

            # 用于存储当前币种不同时间维度的波动率，以便后续求总平均
            temp_vols = []

            # 循环截取不同的时间段，计算各自的平均波动率
            for m in minutes_list:
                df_m = df.tail(m)
                avg_vol = df_m['volatility'].mean()
                symbol_data[f'Avg Vol ({m}m) %'] = avg_vol
                temp_vols.append(avg_vol)

            # 计算所有要求时间段的【综合平均波动率】
            overall_avg_vol = sum(temp_vols) / len(temp_vols)
            symbol_data['Overall Avg Vol %'] = overall_avg_vol

            results.append(symbol_data)

            if (i + 1) % 50 == 0 or (i + 1) == len(symbols):
                print(f"进度: 已处理 {i + 1}/{len(symbols)} 个合约...")

        except Exception as e:
            continue

    df_res = pd.DataFrame(results)

    if not df_res.empty:
        # ==========================================
        # 核心：重排列顺序并排序
        # ==========================================
        # 1. 明确列的顺序：将 'Overall Avg Vol %' 强制放在第三列（索引为2）
        col_order = ['Symbol', 'Latest Price', 'Overall Avg Vol %'] + [f'Avg Vol ({m}m) %' for m in minutes_list]
        df_res = df_res[col_order]

        # 2. 按照综合平均波动率进行降序排序
        df_res = df_res.sort_values(by='Overall Avg Vol %', ascending=False).reset_index(drop=True)

        # 3. 将所有的波动率列保留 4 位小数，使界面更清爽
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


    # 获取合约指定时间的涨跌幅
    change_hours = 2
    df_ranking = get_binance_futures_change(hours=change_hours)

    print(f"\n✅ 币安合约过去 {change_hours} 小时涨跌幅排行 (前 10 名):")
    print(df_ranking.head(10).to_string())


    # # 获取合约指定时间的波动率

    calc_minutes_list = [15, 30, 60, 90, 120, 180, 240, 300, 360, 420, 480, 540, 600]

    df_volatility = get_binance_volatility_ranking(minutes_list=calc_minutes_list)

    print(f"\n✅ 币安合约多维度波动率排行 (默认按 {calc_minutes_list[0]}m 排序，前 15 名):")
    # 为了防止控制台打印时列被折叠，稍微调整一下 Pandas 显示设置
    pd.set_option('display.max_columns', None)