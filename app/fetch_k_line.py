from pathlib import Path

import ccxt
import numpy as np
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


def get_binance_volatility_ranking(minutes_list=[15, 30, 60], max_workers=20, top_n=80):
    """
    获取币安 U本位永续合约的多维度平均分钟波动率及资金费率。
    使用多线程并发拉取，从高到低排序。

    :param minutes_list: 需要计算的过去分钟数的列表，例如 [15, 30, 60]
    :param max_workers: 线程池并发数，建议 10-20 (加入初筛后，无需过高)
    :param top_n: 新增参数，通过 24 小时振幅初筛出的前 N 个高波动合约，防止过度请求 API
    """
    import time  # 确保内部可用

    if isinstance(minutes_list, int):
        minutes_list = [minutes_list]

    max_minutes = max(minutes_list)

    print(f"\n==========================================")
    print(f"🚀 开始并发获取币安合约波动率 & 资金费率")
    print(f"时间维度: {minutes_list} 分钟 | 并发线程数: {max_workers} | 初筛数量: {top_n}")
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
    # 🌟 新增核心逻辑：24小时 Ticker 初筛 (仅需 1 次 API 请求)
    # ==========================================
    print("正在通过 Ticker 接口进行全市场初筛过滤...")
    tickers = exchange.fetch_tickers(symbols)
    proxy_vols = []

    for sym in symbols:
        ticker = tickers.get(sym, {})
        high = ticker.get('high')
        low = ticker.get('low')
        quote_volume = ticker.get('quoteVolume')

        # 过滤掉数据不全或 24H 成交额小于 100 万 USDT 的死气沉沉的币种
        if high and low and low > 0 and quote_volume and quote_volume > 1000000:
            rough_vol = (high - low) / low * 100
            proxy_vols.append({'symbol': sym, 'rough_vol': rough_vol})

    # 按照 24 小时振幅降序排序，提取前 top_n 名
    proxy_vols = sorted(proxy_vols, key=lambda x: x['rough_vol'], reverse=True)
    target_symbols = [item['symbol'] for item in proxy_vols[:top_n]]
    print(f"✅ 初筛完成！已为你锁定最活跃的 {len(target_symbols)} 个高波动合约，抛弃沉寂标的。")

    # ==========================================
    # 批量获取资金费率 (增加 5 次重试机制) - 修改为只获取初筛后的标的
    # ==========================================
    print("正在批量拉取目标标的的最新资金费率...")
    funding_rates_data = {}
    for attempt in range(5):
        try:
            funding_rates_data = exchange.fetch_funding_rates(target_symbols)
            break
        except Exception as e:
            if attempt == 4:
                print(f"⚠️ 获取资金费率失败(已重试5次): {e}")
            else:
                time.sleep(1)

    if max_minutes > 1000:
        print("⚠️ 警告: 请求的分钟数超过了 1000 分钟。已截断为 1000 条。")
        limit = 1000
    else:
        limit = max_minutes

    results = []

    # ==========================================
    # 定义单线程处理函数 (完全保留你的原逻辑，不作改动)
    # ==========================================
    def fetch_and_calc(symbol):
        try:
            all_ohlcv = []
            now = exchange.milliseconds()
            current_since = now - int(limit * 60 * 1000)

            # 循环分批拉取，防止超过币安限制
            while len(all_ohlcv) < limit:
                fetch_limit = min(limit - len(all_ohlcv), 1000)
                ohlcv = None

                # 拉取 K 线数据 (增加 5 次重试机制)
                for attempt in range(5):
                    try:
                        ohlcv = exchange.fetch_ohlcv(symbol, '1m', since=current_since, limit=fetch_limit)
                        break
                    except Exception as e:
                        if attempt == 4:
                            raise Exception(f"K线拉取失败(已重试5次): {e}")
                        time.sleep(1)

                if not ohlcv:
                    break  # 如果没有数据返回，说明到底了

                all_ohlcv.extend(ohlcv)
                current_since = ohlcv[-1][0] + 60000

                if len(ohlcv) < fetch_limit:
                    break

            if len(all_ohlcv) < (limit * 0.8):
                print(f"⚠️ [{symbol}] K线数量不足: 期望 {limit} 根, 实际拼接到 {len(all_ohlcv)} 根, 已丢弃。")
                return None

            df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # 强制转换为 float，防止数据污染报错
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(
                float)
            df = df[df['low'] > 0]

            if df.empty:
                return None

            # 计算波动率
            df['volatility'] = (df['high'] - df['low']) / df['low'] * 100
            latest_price = df['close'].iloc[-1]

            # 获取该币种的资金费率（防字符串异常处理）
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

            # 计算综合平均波动率
            symbol_data['Overall Avg Vol %'] = sum(temp_vols) / len(temp_vols) if temp_vols else 0
            return symbol_data

        except Exception as e:
            print(f"❌ [{symbol}] 处理时发生异常: {e}")
            return None

    # ==========================================
    # 启动多线程并发执行 (目标群体改为 target_symbols)
    # ==========================================
    print(f"正在启动 {max_workers} 个线程拉取核心标的的 {limit} 根 K线数据...")

    completed_count = 0
    total_symbols = len(target_symbols)

    # 使用 ThreadPoolExecutor 管理并发
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务到线程池 (注意这里传入的是 target_symbols)
        future_to_symbol = {executor.submit(fetch_and_calc, sym): sym for sym in target_symbols}

        # as_completed 会在某个线程完成时立刻 yield
        for future in as_completed(future_to_symbol):
            completed_count += 1
            res = future.result()
            if res is not None:
                results.append(res)

            # 打印进度条 (调整打印频率适应更小的样本量)
            if completed_count % 10 == 0 or completed_count == total_symbols:
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

def get_okx_volatility_ranking(minutes_list=[15, 30, 60], max_workers=100):
    """
    获取欧易 (OKX) U本位永续合约的多维度平均分钟波动率及资金费率。
    """
    import time  # 确保内部可用

    if isinstance(minutes_list, int):
        minutes_list = [minutes_list]

    max_minutes = max(minutes_list)

    print(f"\n==========================================")
    print(f"🚀 开始并发获取欧易(OKX)合约波动率 & 资金费率")
    print(f"时间维度: {minutes_list} 分钟 | 并发线程数: {max_workers}")
    print(f"==========================================")

    # 实例化交易所 (OKX)
    exchange = ccxt.okx({
        'enableRateLimit': True,
        'options': {
            'defaultType': 'swap',
        },
        # 如果在海外服务器运行，请注释掉 proxies
        'proxies': {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890',
        },
    })

    print("正在加载欧易合约市场数据...")
    markets = exchange.load_markets()

    # 过滤出欧易的 U本位永续合约
    symbols = [
        symbol for symbol, market in markets.items()
        if market.get('active')
           and market.get('linear')
           and market.get('settle') == 'USDT'  # 核心变动：OKX 使用 settle 判断结算货币
           and market.get('type') == 'swap'
    ]
    print(f"共发现 {len(symbols)} 个交易中的 U本位永续合约。")

    # ==========================================
    # 批量获取资金费率 (增加 5 次重试机制)
    # ==========================================
    print("正在拉取全市场最新资金费率 (OKX)...")
    funding_rates_data = {}
    for attempt in range(5):
        try:
            funding_rates_data = exchange.fetch_funding_rates(symbols)
            break
        except Exception as e:
            if attempt == 4:
                print(f"⚠️ 获取资金费率失败(已重试5次): {e}")
            else:
                time.sleep(1)

    if max_minutes > 1000:
        print("⚠️ 警告: 请求的分钟数超过限制，已截断为 1000 条。")
        limit = 1000
    else:
        limit = max_minutes

    results = []

    def fetch_and_calc(symbol):
        try:
            all_ohlcv = []
            now = exchange.milliseconds()
            current_since = now - int(limit * 60 * 1000)

            # 💡 核心修复：循环分批拉取，突破 OKX 单次最多 300 根的限制
            while len(all_ohlcv) < limit:
                fetch_limit = min(limit - len(all_ohlcv), 300)
                ohlcv = None

                # 拉取 K 线数据 (增加 5 次重试机制)
                for attempt in range(5):
                    try:
                        ohlcv = exchange.fetch_ohlcv(symbol, '1m', since=current_since, limit=fetch_limit)
                        break
                    except Exception as e:
                        if attempt == 4:
                            raise Exception(f"K线拉取失败(已重试5次): {e}")
                        time.sleep(1)

                if not ohlcv:
                    break  # 如果没有数据返回，说明到底了

                all_ohlcv.extend(ohlcv)
                current_since = ohlcv[-1][0] + 60000

                if len(ohlcv) < fetch_limit:
                    break

            if len(all_ohlcv) < (limit * 0.8):
                print(f"⚠️ [{symbol}] K线数量不足: 期望 {limit} 根, 实际拼接到 {len(all_ohlcv)} 根, 已丢弃。")
                return None

            df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

            # 强制转换为 float，防止数据污染
            df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(
                float)

            df = df[df['low'] > 0]

            if df.empty:
                return None

            # 计算波动率 (振幅)
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
            print(f"❌ [{symbol}] 处理时发生异常: {e}")
            return None

    print(f"正在启动 {max_workers} 个线程拉取 {limit} 根 K线数据...")

    completed_count = 0
    total_symbols = len(symbols)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_symbol = {executor.submit(fetch_and_calc, sym): sym for sym in symbols}

        for future in as_completed(future_to_symbol):
            completed_count += 1
            res = future.result()
            if res is not None:
                results.append(res)

            if completed_count % 50 == 0 or completed_count == total_symbols:
                print(f"进度: 已处理 {completed_count}/{total_symbols} 个合约...")

    df_res = pd.DataFrame(results)

    if not df_res.empty:
        col_order = ['Symbol', 'Latest Price', 'Funding Rate %', 'Overall Avg Vol %'] + [f'Avg Vol ({m}m) %' for m in
                                                                                         minutes_list]
        df_res = df_res[col_order]

        df_res = df_res.sort_values(by='Overall Avg Vol %', ascending=False).reset_index(drop=True)

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


def get_open_interest(symbol, exchange_name='binance'):
    """
    获取指定合约的实时持仓量 (Open Interest)

    :param exchange_name: 交易所名称，例如 'binance' 或 'okx'
    :param symbol: 统一格式的合约符号，例如 'BTC/USDT:USDT'
    :return: 包含持仓量信息的字典，如果获取失败或不支持则返回 None
    """
    # 实例化对应的交易所类
    exchange_class = getattr(ccxt, exchange_name)
    exchange = exchange_class({
        'enableRateLimit': True,
        'proxies': {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890',
        },
    })

    print(f"正在拉取 {exchange_name.upper()} 交易所 {symbol} 的实时持仓量 (OI)...")

    try:
        # CCXT 提供了统一的 API 来检查和获取 OI
        if exchange.has.get('fetchOpenInterest'):
            oi_data = exchange.fetch_open_interest(symbol)

            # 提取核心数据
            base_volume = oi_data.get('openInterestAmount')  # 持仓量 (按币的个数计算)
            quote_value = oi_data.get('openInterestValue')  # 持仓价值 (按计价货币计算，通常是 U本位价值)

            print(f"✅ 成功获取 [{symbol}] 持仓量: {base_volume} 币 | 价值: {quote_value} USDT")

            return {
                'Symbol': symbol,
                'Exchange': exchange_name.upper(),
                'OI (Base Coin)': base_volume,
                'OI Value (USDT)': quote_value,
                'Timestamp': oi_data.get('timestamp'),
                'Datetime': oi_data.get('datetime')
            }
        else:
            print(f"⚠️ 交易所 {exchange_name} 不支持通过统一 API 获取 {symbol} 的持仓量。")
            return None

    except Exception as e:
        print(f"❌ 获取 [{symbol}] 持仓量失败: {e}")
        return None


def fetch_long_funding_history(exchange_name, symbol, days=30):
    """
    分页拉取指定合约最近N天的资金费率历史并转换为北京时间
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

    all_funding = []

    print(f"开始拉取 {exchange_name} 的 {symbol} 资金费率历史...")

    while True:
        try:
            # 资金费率历史分页：币安最大1000，欧易安全值100（与fetch_long_history完全一致的逻辑）
            limit = 1000 if exchange_name == 'binance' else 100
            curr_funding = exchange.fetch_funding_rate_history(symbol, since=since, limit=limit)

            if not curr_funding:
                break

            all_funding.extend(curr_funding)

            # 更新 since 为最后一条数据的时间戳 + 1毫秒，避免重复
            last_timestamp = curr_funding[-1]['timestamp']
            since = last_timestamp + 1

            print(f"已获取到: {pd.to_datetime(last_timestamp, unit='ms')}，累计 {len(all_funding)} 条")

            # 如果最后一条数据的时间已经接近当前时间，则停止
            if last_timestamp >= exchange.milliseconds() - 60000:  # 1分钟内
                break

            # 尊重频率限制
            # time.sleep(exchange.rateLimit / 1000)

        except Exception as e:
            print(f"拉取出错: {e}")
            break

    # 转换为 DataFrame（仅保留时间戳和资金费率，格式与原代码风格一致）
    if not all_funding:
        return pd.DataFrame(columns=['timestamp', 'funding_rate'])

    df = pd.DataFrame([{
        'timestamp': item['timestamp'],
        'funding_rate': item.get('fundingRate', 0.0) * 100
    } for item in all_funding])

    # --- 时间处理核心步骤 ---（完全复制 fetch_long_history 中的代码，一字不改）
    # 1. 转换为 UTC 时间
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC')
    # 2. 转换为北京时间
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Shanghai')
    # 3. (可选) 如果不需要显示时区后缀，可以转为无时区格式
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)
    df['funding_rate_pct'] = df['funding_rate'].pct_change() * 100

    # 按时间正序排列（确保数据完整性）
    df = df.sort_values('timestamp').reset_index(drop=True)

    return df


def fetch_historical_oi(exchange_name, symbol, timeframe='1h', days=30):
    """
    分页拉取长历史持仓量 (OI) 数据，转换为北京时间，并计算相对上一时刻的涨跌幅
    :param days: 获取过去多少天的数据，默认 30 天
    """
    import time
    import pandas as pd

    exchange_class = getattr(ccxt, exchange_name)
    exchange = exchange_class({
        'enableRateLimit': True,
        'proxies': {
            'http': 'http://127.0.0.1:7890',
            'https': 'http://127.0.0.1:7890',
        },
    })

    if not exchange.has.get('fetchOpenInterestHistory'):
        print(f"❌ 警告: 交易所 {exchange_name} 的 CCXT 模块暂不支持通过统一 API 获取历史持仓量。")
        return pd.DataFrame()

    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000
    all_oi = []

    print(f"开始拉取 {exchange_name} 的 {symbol} 历史持仓量 (OI) 数据...")

    while True:
        try:
            limit = 500 if exchange_name == 'binance' else 100
            curr_oi = exchange.fetch_open_interest_history(symbol, timeframe, since=since, limit=limit)

            if not curr_oi:
                break

            all_oi.extend(curr_oi)

            last_timestamp = curr_oi[-1]['timestamp']
            since = last_timestamp + 1

            print(f"已获取到: {pd.to_datetime(last_timestamp, unit='ms')}，累计 {len(all_oi)} 条历史 OI 数据")

            if last_timestamp >= exchange.milliseconds() - 60000:
                break

            time.sleep(exchange.rateLimit / 1000 * 1.5)

        except Exception as e:
            print(f"拉取历史 OI 出错: {e}")
            break

    if not all_oi:
        print(f"⚠️ 未能获取到 {symbol} 的历史持仓数据。")
        return pd.DataFrame()

    parsed_data = []
    for item in all_oi:
        parsed_data.append({
            'timestamp': item['timestamp'],
            'oi_amount': item.get('openInterestAmount', 0),
            'oi_value': item.get('openInterestValue', 0)
        })

    df = pd.DataFrame(parsed_data)

    # --- 时间处理 ---
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms').dt.tz_localize('UTC')
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Shanghai')
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)

    # 去重处理，以防翻页时间戳重叠
    df.drop_duplicates(subset=['timestamp'], keep='last', inplace=True)

    # ==========================================
    # 🌟 新增核心逻辑：计算持仓量涨跌幅
    # ==========================================
    # 1. 确保数据严格按照时间升序排列，这是计算涨跌幅的前提
    df.sort_values(by='timestamp', ascending=True, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 2. 计算数量的涨跌幅 (%)：当前值相对上一值的变化百分比
    df['oi_amount_change_pct'] = df['oi_amount'].pct_change() * 100

    # 3. 计算价值的涨跌幅 (%)
    # 防御性编程：如果某些交易所不支持返回 oi_value (全为0)，则直接将其涨跌幅置为0，防止产生 NaN 或无穷大
    if df['oi_value'].sum() > 0:
        df['oi_value_change_pct'] = df['oi_value'].pct_change() * 100
    else:
        df['oi_value_change_pct'] = 0.0

    # 4. 数据清理：第一行没有“上一时刻”，会产生 NaN，将其填充为 0
    df.fillna({'oi_amount_change_pct': 0, 'oi_value_change_pct': 0}, inplace=True)

    # 5. 格式化：保留四位小数，使其与资金费率等数据的精度对齐，便于观看
    df['oi_amount_change_pct'] = df['oi_amount_change_pct'].round(4)
    df['oi_value_change_pct'] = df['oi_value_change_pct'].round(4)

    return df


def detect_oi_signals_with_confidence(df):
    """
    带有置信度 (Confidence Score) 的 OI 信号识别函数。
    分数范围 0 - 100，越高代表信号越极其明显。
    """
    df = df.sort_values('timestamp').reset_index(drop=True)

    signals = []
    reasons = []
    confidences = []  # 新增：记录置信度

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
        conf_score = 0.0  # 默认置信度为 0

        hours_since_danger += 1

        # -------------------------------------------------------------
        # 🛑 DANGER: 燃料暴增
        # -------------------------------------------------------------
        if amt_pct > 15:
            signal = "🛑 DANGER"
            reason = "巨量燃料注入！"
            in_grid_zone = False
            recent_blow_off = False
            hours_since_danger = 0

            # 置信度计算：基数 50，超过 15% 的部分，每多 1% 加 2 分。
            # 例如：增量 30%，分数 = 50 + (30-15)*2 = 80
            conf_score = 50 + (amt_pct - 15) * 2

        # -------------------------------------------------------------
        # ⚠️ WARNING: 无量干拔 (见顶预警)
        # -------------------------------------------------------------
        elif amt_pct < 3 and val_pct > 18:
            signal = "⚠️ WARNING"
            reason = "无量干拔 (燃料停滞+价值飙升)"
            recent_blow_off = True

            # 置信度计算：价格拉得越高、持仓掉得越猛，置信度越高。
            # 基数 60。价格每超 18% 加 1.5 分；持仓每比 3% 低 1%，加 3 分。
            conf_score = 60 + (val_pct - 18) * 1.5 + (3 - amt_pct) * 3

        # -------------------------------------------------------------
        # ✅ GRID_START: 大资金撤退 (网格确认)
        # -------------------------------------------------------------
        elif recent_blow_off and amt_pct < -5 and val_pct < -8:
            signal = "✅ GRID_START"
            reason = "单边结束，大资金撤离"
            in_grid_zone = True
            recent_blow_off = False

            # 置信度计算：双杀跌得越深，确认度越高。
            # 基数 60。持仓跌幅超 5% 的部分乘以 2；价格跌幅超 8% 的部分乘以 1。
            conf_score = 60 + abs(amt_pct + 5) * 2 + abs(val_pct + 8) * 1

        # -------------------------------------------------------------
        # 🎯 SHORT_ENTRY: 震荡期空心针 (最佳做空点)
        # -------------------------------------------------------------
        elif in_grid_zone and hours_since_danger > 3:
            if amt_pct <= 1.5 and val_pct >= 10:
                signal = "🎯 SHORT_ENTRY"
                reason = "空心假拉升，网格高抛点"

                # 置信度计算：纯粹的背离度。持仓越少（甚至为负），价格拉得越高，越准！
                # 基数 65。价格超 10% 的部分乘 2；持仓少于 1.5% 的部分乘 4 (权重极大)。
                conf_score = 65 + (val_pct - 10) * 2 + (1.5 - amt_pct) * 4

            elif amt_pct < -5 and val_pct < -10:
                signal = "📉 DOWN_TREND"
                reason = "震荡向下破位"
                conf_score = 50 + abs(amt_pct + 5) * 1.5

        # --- 限制置信度范围在 0 到 100 之间，并保留两位小数 ---
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


def get_high_volatility_oi_signals(days=7, timeframe='1h', top_k=15, max_workers=10):
    """
    综合自动化工作流：
    1. 获取币安高波动率合约排行。
    2. 使用多线程并发拉取这些高波动合约的 7 天 1h 历史持仓量 (OI) 数据 (带错误重试机制)。
    3. 进行 OI 信号与置信度分析。
    4. 将所有合约的分析结果合并为一个完整的 DataFrame。

    :param days: 拉取历史 OI 数据天数，默认 7 天
    :param timeframe: K线周期，默认 '1h'
    :param top_k: 选取波动率排名前 K 的合约进行精准分析
    :param max_workers: 并发线程数，默认 10
    """
    import pandas as pd
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    print("\n" + "=" * 50)
    print("🚀 第一阶段：开始筛选高波动率合约...")
    print("=" * 50)

    # 1. 运行高波动率获取逻辑
    calc_minutes_list = [15, 30, 60, 90, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1000]
    bin_df = get_binance_volatility_ranking(minutes_list=calc_minutes_list, max_workers=20)

    if bin_df is None or bin_df.empty:
        print("❌ 未能获取到波动率数据，流程终止。")
        return pd.DataFrame()

    # 提取高波动率排名前 top_k 的合约
    target_symbols = bin_df['Symbol'].head(top_k).tolist()

    print("\n" + "=" * 50)
    print(f"🚀 第二阶段：开始多线程批量拉取并分析 OI 信号")
    print(f"目标标的 (前 {top_k} 名): {target_symbols}")
    print(f"并发线程数: {max_workers} | 失败重试次数: 5次")
    print("=" * 50)

    all_analyzed_dfs = []

    # ==========================================
    # 定义单线程处理函数：包含拉取、重试机制和分析
    # ==========================================
    def process_symbol(symbol):
        oi_df = None

        # 增加 5 次重试机制，失败等待 1 秒
        for attempt in range(5):
            try:
                oi_df = fetch_historical_oi(exchange_name='binance', symbol=symbol, timeframe=timeframe, days=days)

                # 如果成功拿到数据且不为空，跳出重试循环
                if oi_df is not None and not oi_df.empty:
                    break

                # 如果拿到的是空数据（可能网络延迟没抛错），也视作失败进行重试
                if attempt < 4:
                    time.sleep(1)

            except Exception as e:
                if attempt == 4:
                    print(f"⚠️ [{symbol}] 拉取 OI 数据发生异常(已重试5次): {e}")
                    return None
                time.sleep(1)

        # 校验最终拿到的数据
        if oi_df is None or oi_df.empty:
            print(f"⚠️ [{symbol}] 最终未能获取到历史 OI 数据，跳过该标的。")
            return None

        try:
            # 调用信号识别函数
            anlyse_df = detect_oi_signals_with_confidence(oi_df)

            # ⚠️ 核心步骤：在 DataFrame 第 0 列插入当前币种的 Symbol，防合并后数据混乱
            anlyse_df.insert(0, 'Symbol', symbol)
            return anlyse_df

        except Exception as e:
            print(f"❌ [{symbol}] 信号分析时发生异常: {e}")
            return None

    # ==========================================
    # 启动多线程并发执行
    # ==========================================
    completed_count = 0
    total_symbols = len(target_symbols)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务到线程池
        future_to_symbol = {executor.submit(process_symbol, sym): sym for sym in target_symbols}

        # as_completed 保证任何一个线程完成时立即收集结果
        for future in as_completed(future_to_symbol):
            completed_count += 1
            sym = future_to_symbol[future]

            try:
                res_df = future.result()
                if res_df is not None and not res_df.empty:
                    all_analyzed_dfs.append(res_df)
            except Exception as e:
                print(f"❌ [{sym}] 线程内部执行发生致命错误: {e}")

            # 打印进度条
            if completed_count % 5 == 0 or completed_count == total_symbols:
                print(f"进度: 已处理并分析 {completed_count}/{total_symbols} 个合约...")

    print("\n" + "=" * 50)
    print("🚀 第三阶段：合并全部数据")
    print("=" * 50)

    # 3. 将所有分析完成的 df 纵向合并为一个完整的 df
    if not all_analyzed_dfs:
        print("❌ 所有标的均未产生有效分析数据。")
        return pd.DataFrame()

    final_combined_df = pd.concat(all_analyzed_dfs, ignore_index=True)

    # 为了便于阅读，按照 合约名称 (升序) 和 时间 (降序) 进行排序
    final_combined_df = final_combined_df.sort_values(by=['Symbol', 'timestamp'], ascending=[True, False]).reset_index(
        drop=True)

    print(f"✅ 综合分析工作流执行完毕！共生成 {len(final_combined_df)} 条分析记录。")

    return final_combined_df


def detect_long_grid_signals_strict(df):
    """
    V2 铁血严格版：专为【做多网格】设计的 OI 信号识别函数。
    彻底修复“空头平仓死猫跳”陷阱，严格过滤连环瀑布。
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

        # -------------------------------------------------------------
        # 1. 🛑 DANGER: 主动追空瀑布 (绝对禁区) - [你的截图 00:00 和 04:00]
        # 价格暴跌，同时持仓量暴增，说明新空军进场屠杀。
        # -------------------------------------------------------------
        if val_pct < -5 and amt_pct > 6:
            signal = "🛑 DANGER"
            reason = "主动追空瀑布！空军携巨资入场，做多网格立刻停机！"
            in_long_grid_zone = False
            recent_capitulation = False
            hours_since_danger = 0
            conf_score = 60 + abs(val_pct + 5) * 1.5 + (amt_pct - 6) * 2

        # -------------------------------------------------------------
        # 2. ⚠️ WARNING: 多头大血洗 (连环爆仓预警) - [你的截图 18:00]
        # -------------------------------------------------------------
        elif val_pct < -15 and amt_pct < -10:
            signal = "⚠️ WARNING"
            reason = "多头连环爆仓。抛压释放中，不接飞刀，密切观察。"
            recent_capitulation = True
            in_long_grid_zone = False  # 确保网格关闭
            conf_score = 65 + abs(val_pct + 15) * 1.5 + abs(amt_pct + 10) * 2.5

        # -------------------------------------------------------------
        # 3. ☠️ BEAR_TRAP: 死猫跳陷阱 (剔除你截图 19:00 的罪魁祸首)
        # 价格暴涨，但 OI 暴跌。这是空头止盈的假象，绝对不能做多！
        # -------------------------------------------------------------
        elif recent_capitulation and val_pct > 8 and amt_pct < -5:
            signal = "☠️ BEAR_TRAP"
            reason = "空头平仓死猫跳！价格虚高但资金流出，即将二次探底，绝对观望！"
            # 保持 recent_capitulation = True，因为我们还在等真正的底
            conf_score = 80 + (val_pct - 8) * 1.5 + abs(amt_pct + 5) * 2

        # -------------------------------------------------------------
        # 4. ✅ GRID_START: 真实止跌企稳 (严格的网格启动确认)
        # 必须是：价格不再剧烈波动 (±4%)，且资金不再大幅流出 (±2.5%)。
        # -------------------------------------------------------------
        elif recent_capitulation and abs(val_pct) <= 4 and abs(amt_pct) <= 2.5:
            signal = "✅ GRID_START"
            reason = "真实底部企稳。波动率极度收缩，多空双方熄火，安全开启网格！"
            in_long_grid_zone = True
            recent_capitulation = False
            conf_score = 70 + (4 - abs(val_pct)) * 5

        # -------------------------------------------------------------
        # 5. 🎯 LONG_ENTRY: 严苛版空心砸盘 (震荡期最佳低吸点)
        # 必须确保跌的时候没有人在爆仓 (amt_pct >= -1)。
        # -------------------------------------------------------------
        elif in_long_grid_zone and hours_since_danger > 3:
            # 价格大跌，但资金既没有恐慌流出，也没有空头加仓
            if val_pct <= -6 and -1 <= amt_pct <= 2:
                signal = "🎯 LONG_ENTRY"
                reason = "完美空心砸盘！无爆仓无追空，纯属流动性缺失假摔，低吸买点！"
                conf_score = 70 + abs(val_pct + 6) * 2.5 + (2 - amt_pct) * 2

            elif val_pct > 5 and amt_pct > 3:
                signal = "📈 UP_TREND"
                reason = "底部企稳回升，真实买盘介入。"
                conf_score = 50 + val_pct * 1.5

        # --- 分数限制与清理 ---
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


def fetch_binance_cvd_history(symbol, timeframe='1h', days=30):
    """
    分页拉取币安 U本位合约的 CVD (Cumulative Volume Delta) 数据并转换为北京时间。
    """
    import ccxt
    import pandas as pd
    import numpy as np

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

    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        raw_symbol = market['id']
    except Exception as e:
        print(f"解析 Symbol 失败: {e}")
        return pd.DataFrame()

    timeframe_id = exchange.timeframes.get(timeframe, timeframe)
    since = exchange.milliseconds() - days * 24 * 60 * 60 * 1000
    all_klines = []

    print(f"开始拉取 Binance {symbol} 的 CVD 基础数据...")

    while True:
        try:
            params = {
                'symbol': raw_symbol,
                'interval': timeframe_id,
                'startTime': since,
                'limit': 1000
            }

            curr_klines = exchange.fapiPublicGetKlines(params)

            if not curr_klines:
                break

            all_klines.extend(curr_klines)
            last_timestamp = int(curr_klines[-1][0])
            since = last_timestamp + 1

            # 打印时也加入容错处理，避免 print 时报错
            readable_time = pd.to_datetime(last_timestamp, unit='ms', errors='coerce')
            print(f"已获取到: {readable_time}，累计 {len(all_klines)} 条")

            if last_timestamp >= exchange.milliseconds() - 60000:
                break

        except Exception as e:
            print(f"拉取 CVD 数据出错: {e}")
            break

    if not all_klines:
        return pd.DataFrame()

    columns = [
        'timestamp', 'open', 'high', 'low', 'close', 'volume',
        'close_time', 'quote_asset_volume', 'trades',
        'taker_buy_base_vol', 'taker_buy_quote_vol', 'ignore'
    ]
    df = pd.DataFrame(all_klines, columns=columns)

    # ==========================================
    # 数据清洗与转换 (关键修复点)
    # ==========================================

    # 1. 先确保 timestamp 列是数值类型，非数字的会变成 NaN
    df['timestamp'] = pd.to_numeric(df['timestamp'], errors='coerce')

    # 2. 转换数值型列
    numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'taker_buy_base_vol', 'taker_buy_quote_vol']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')

    # 3. 移除任何包含无效数据的行（预防 API 返回异常数据）
    df = df.dropna(subset=['timestamp'] + numeric_cols)

    # ==========================================
    # CVD 核心计算逻辑
    # ==========================================
    df['taker_sell_base_vol'] = df['volume'] - df['taker_buy_base_vol']
    df['volume_delta'] = df['taker_buy_base_vol'] - df['taker_sell_base_vol']
    df['cvd'] = df['volume_delta'].cumsum()

    # ==========================================
    # 时间处理 (增加 errors='coerce' 预防溢出)
    # ==========================================
    # 使用 errors='coerce'，即使有极个别离群值导致溢出，也会变成 NaT 而不是让程序崩溃
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')

    # 再次清理可能转换失败的时间
    df = df.dropna(subset=['timestamp'])

    # 时区转换
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    df['timestamp'] = df['timestamp'].dt.tz_convert('Asia/Shanghai')
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)

    result_df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume',
                    'taker_buy_base_vol', 'taker_sell_base_vol', 'volume_delta', 'cvd']]

    return result_df


import pandas as pd
import numpy as np


def detect_signal_a(
    df,
    short_window=3,
    long_window=6,
    ntr_threshold=0.06,
    price_flat_pct=0.005,
    extreme_ntr=0.15,
    vol_spike_mult=1.5,
):
    """
    信号A: CVD-价格极端背离检测 (冰山吸收信号)

    ★ 所有计算仅使用当前行及历史行, 无未来数据泄露.
    ★ pct_change = (close[i] - close[i-1]) / close[i-1]  标准单根涨跌幅
    ★ price_chg_sw/lw = (close[i] - close[i-w]) / close[i-w]  窗口累计涨跌幅

    signal_strength  : 0-5   整数档位
    signal_intensity : 0-100 连续浮点强度
    """

    r = df.copy()

    # ── 0. 补全列 ──
    if 'volume_delta' not in r.columns:
        r['volume_delta'] = r['taker_buy_base_vol'] - r['taker_sell_base_vol']
    if 'cvd' not in r.columns:
        r['cvd'] = r['volume_delta'].cumsum()

    # ── 1. 单根K线涨跌幅 = (当前收盘 - 上一根收盘) / 上一根收盘 ──
    prev_close = r['close'].shift(1)
    r['pct_change'] = (r['close'] - prev_close) / prev_close

    # ── 2. 双窗口滚动指标 ──
    #   每个指标只用 shift / rolling 回看, 不触及未来行
    for w, tag in [(short_window, 'sw'), (long_window, 'lw')]:
        # 窗口累计涨跌幅: (close[i] - close[i-w]) / close[i-w]
        base_close = r['close'].shift(w)
        r[f'price_chg_{tag}'] = (r['close'] - base_close) / base_close

        # CVD 变化量: cvd[i] - cvd[i-w]
        r[f'cvd_chg_{tag}'] = r['cvd'].diff(w)

        # 窗口 volume_delta 合计 & NTR
        r[f'vd_sum_{tag}'] = r['volume_delta'].rolling(w, min_periods=w).sum()
        r[f'vol_sum_{tag}'] = r['volume'].rolling(w, min_periods=w).sum()
        r[f'ntr_{tag}'] = r[f'vd_sum_{tag}'] / r[f'vol_sum_{tag}'].replace(0, np.nan)

    vol_lb = max(long_window * 3, 12)
    r['vol_ma'] = r['volume'].rolling(vol_lb, min_periods=1).mean()
    r['vol_ratio'] = r['volume'] / r['vol_ma'].replace(0, np.nan)

    # ── 3. 背离方向检测 ──
    #   NTR 取窗口统计, 价格取同窗口累计涨跌幅, 时间尺度一致
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

    # ── 4. 综合信号 ──
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

    # ── 5. 连续信号计数 (逐行前向扫描, 仅回看) ──
    sig_n = r['signal'].map({
        'BULLISH_ABSORPTION': 1, 'BEARISH_ABSORPTION': -1, 'NEUTRAL': 0
    })
    consec, cnt, prev = [], 0, 0
    for v in sig_n:
        cnt = cnt + 1 if (v != 0 and v == prev) else (1 if v != 0 else 0)
        consec.append(cnt)
        prev = v
    r['consecutive_signals'] = consec

    # ── 6a. 整数强度 signal_strength (0-5) ──
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

    # ── 6b. 连续强度 signal_intensity (0-100) ──
    #   NTR偏移(40) + 价格反向(25) + 成交量(20) + 持续性(15)
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

    # ── 7. 注释 ──
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
            price_d = (f'窗口价格反涨{pc_v:+.2f}%'
                       if pc_v > price_flat_pct * 100
                       else f'窗口价格拒跌({pc_v:+.2f}%)')
            detail = '限价买单吸收卖压 → 后续大概率拉升'
        else:
            icon = '🔴'
            label = '看跌吸收'
            taker_d = f'Taker净买入占比{abs(ntr_v):.1f}%'
            price_d = (f'窗口价格反跌{pc_v:+.2f}%'
                       if pc_v < -price_flat_pct * 100
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

    # ── 8. 列排序: timestamp → 信号列 → 原始列 → 诊断列 ──
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


def merge_cvd_oi_complete(cvd_df, oi_df, timeframe='1h'):
    """
    深度合并 CVD 和 OI 数据，保留所有原始字段并新增分析指标。

    新增字段说明：
    - price_change_pct: 收盘价涨跌幅 (%)
    - volume_change: 成交量相对上一周期的变化量
    - delta_ratio: Volume Delta 占总成交量的比例 (%)
    - oi_value_change_pct: OI 价值涨跌幅 (%) (如果原始 df 没有则计算)
    """
    if cvd_df.empty or oi_df.empty:
        print("❌ 错误: 输入的 DataFrame 为空，无法合并")
        return pd.DataFrame()

    # 1. 拷贝数据，确保时间戳格式
    cvd = cvd_df.copy()
    oi = oi_df.copy()
    cvd['timestamp'] = pd.to_datetime(cvd['timestamp'])
    oi['timestamp'] = pd.to_datetime(oi['timestamp'])

    # 2. 【核心对齐逻辑】
    # 将 OI 的时间戳向前偏移一个周期。
    # 例子：OI 的 06:00 减去 1h 变成 05:00，从而与 CVD 的 05:00 完美 merge
    delta = pd.to_timedelta(timeframe)
    oi['match_timestamp'] = oi['timestamp'] - delta

    # 3. 执行合并 (保留 cvd 的所有行)
    # 使用左连接，确保即使某时刻没有 OI 数据，K线数据也不会丢失
    df = pd.merge(
        cvd,
        oi,
        left_on='timestamp',
        right_on='match_timestamp',
        how='left',
        suffixes=('', '_oi_raw')  # 如果有重名列，OI 的列名会带后缀
    )

    # 4. 清理多余的对齐辅助列
    if 'match_timestamp' in df.columns:
        df.drop(columns=['match_timestamp'], inplace=True)
    # oi_df 原本的 timestamp 现在代表“数据结算时刻” (即 T+1)
    df.rename(columns={'timestamp_oi_raw': 'oi_snapshot_time'}, inplace=True)

    # 5. 【新增指标计算】

    # A. 价格涨跌幅 (%) - 基于收盘价
    df['price_change_pct'] = df['close'].pct_change() * 100

    # B. 成交量变化量 (volume_change) - 当前 K 线成交量减去上一根 K 线成交量
    df['volume_change'] = df['volume'].diff()

    # C. Volume Delta 占比 (%) - 衡量主动买盘的侵略性
    # 公式: (Taker Buy - Taker Sell) / Total Volume
    df['delta_ratio'] = np.where(
        df['volume'] != 0,
        (df['volume_delta'] / df['volume']) * 100,
        0
    )

    # D. OI 价值涨跌幅 (%)
    # 如果你之前的 fetch 已经算过了 'oi_value_change_pct'，这里会自动保留
    # 如果没有，我们在这里补算一次
    if 'oi_value_change_pct' not in df.columns:
        df['oi_value_change_pct'] = df['oi_value'].pct_change() * 100

    # 6. 数据最后清理
    # 填充因为 diff() 或 pct_change() 产生的第一行 NaN 值
    fill_cols = ['price_change_pct', 'volume_change', 'delta_ratio', 'oi_value_change_pct']
    for col in fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # 保留小数位数，增强可读性
    round_cols = ['price_change_pct', 'oi_amount_change_pct', 'oi_value_change_pct', 'delta_ratio']
    for col in round_cols:
        if col in df.columns:
            df[col] = df[col].round(4)

    return df



import pandas as pd
import numpy as np


import pandas as pd
import numpy as np


import pandas as pd
import numpy as np


import pandas as pd
import numpy as np


def detect_short_signal(df,
                        price_up_min=None,
                        oi_down_min=None,
                        lookback=8,
                        min_squeeze_bars=2,
                        gap_tolerance=1):
    """
    做空时机检测
    ════════════
    核心: 价格大涨 + OI下降 = 空头回补的虚假拉升 → 回补结束后做空

    原理链条:
      OI下降         = 旧仓位在平仓(不是新仓位建立)
      价格涨 + 平仓  = 空头在买入平仓(止损/爆仓)
      空头买入平仓    = 被动买盘, 不是新多头主动入场
      被动买盘的上涨  = 回补结束后无人接盘 → 必跌

    为什么直接做空会被轧:
      挤压 = 正反馈循环:
      空头止损→市价买入→推高价格→更多空头止损→推更高...
      你新开的空单 = 新燃料, 也会被卷入这个循环

    本函数的核心价值:
      判断挤压是否"快结束了", 结束后才给做空信号

    信号(从强到弱):
      trend_reversal : 挤压结束 + 新空入场 + 价格跌 → 最强做空
      short_now      : 挤压结束 + 价格掉头          → 做空
      short_ready    : 挤压正在减速(2/3确认)        → 小仓位试空
      short_wait     : 挤压进行中未减速              → 等待
      short_danger   : 挤压正在加速                  → 绝对不能空
      invalidated    : 新多头入场, 做空逻辑失效      → 放弃
      no_pattern     : 不符合做空条件                → 忽略

    三个减速指标(用于判断挤压是否接近尾声):
      1. OI降幅收窄:  本根|OI降| < 挤压均值的70% → 可被逼平的空头在减少
      2. 涨幅收窄:    本根涨幅 < 挤压均值的70%   → 回补推力在衰减
      3. CVD为负:     主动卖单 > 买单             → 真实意愿是卖, 涨价纯靠被动回补

    必需列: price_change_pct, oi_amount_change_pct, delta_ratio

    Parameters
    ----------
    price_up_min     : 涨幅阈值(%), None=自动取数据|price|的中位数
    oi_down_min      : OI降幅阈值(%), None=自动取数据|OI|的中位数
    lookback         : 回看K线根数(找连续挤压)
    min_squeeze_bars : 至少连续几根才确认挤压模式
    gap_tolerance    : 当前根距最近一根挤压根允许的最大间隔
    """

    df = df.copy().reset_index(drop=True)
    n = len(df)

    # ── 自动阈值 ──
    if price_up_min is None:
        price_up_min = round(
            df['price_change_pct'].abs().quantile(0.5), 4)
    if oi_down_min is None:
        oi_down_min = round(
            df['oi_amount_change_pct'].abs().quantile(0.5), 4)

    sig = ['no_pattern'] * n
    sc  = [0.0] * n
    rsn = [''] * n

    for i in range(n):
        oi_i = df.loc[i, 'oi_amount_change_pct']
        pr_i = df.loc[i, 'price_change_pct']
        cv_i = df.loc[i, 'delta_ratio']

        # ============================================
        #  1. 在 [i-gap, i] 里找最近一根挤压根
        # ============================================
        sq_end = None
        for k in range(i, max(-1, i - gap_tolerance - 1), -1):
            if (df.loc[k, 'price_change_pct'] > 0 and
                    df.loc[k, 'oi_amount_change_pct'] < 0):
                sq_end = k
                break
        if sq_end is None:
            continue

        # ============================================
        #  2. 从 sq_end 往回数连续挤压根
        # ============================================
        sq_ois = []
        sq_prs = []
        for j in range(sq_end, max(-1, sq_end - lookback), -1):
            pj = df.loc[j, 'price_change_pct']
            oj = df.loc[j, 'oi_amount_change_pct']
            if pj > 0 and oj < 0:
                sq_ois.append(oj)
                sq_prs.append(pj)
            else:
                break

        sq_n = len(sq_ois)
        if sq_n < min_squeeze_bars:
            continue

        cum_oi     = sum(sq_ois)            # 负值: 累计OI流出
        cum_pr     = sum(sq_prs)            # 正值: 累计涨幅
        avg_oi_abs = abs(cum_oi / sq_n)     # 平均每根OI降幅(取正)
        avg_pr     = cum_pr / sq_n          # 平均每根涨幅

        post = (i > sq_end)  # True = 当前根已不在挤压序列中

        # ============================================
        #  3A. 当前根仍在挤压中
        # ============================================
        if not post:

            # 三个减速指标
            oi_slow = (abs(oi_i) < avg_oi_abs * 0.7) if sq_n >= 3 else False
            pr_slow = (pr_i < avg_pr * 0.7)          if sq_n >= 3 else False
            cvd_neg = (cv_i < 0)
            cf = sum([oi_slow, pr_slow, cvd_neg])

            # 评分基础 = 挤压规模
            base = (min(sq_n / 6, 1) * 20
                    + min(abs(cum_oi) / 8, 1) * 15)

            # ── cf >= 2 → 可以小仓位试空 ──
            if cf >= 2:
                sig[i] = 'short_ready'
                sc[i]  = min(round(base + 30 + cf * 5), 100)
                rsn[i] = (
                    f'【准备做空·挤压正在减速】\n'
                    f'连续{sq_n}根"价格涨+OI降"'
                    f'（累计OI流出{cum_oi:.2f}%，累计涨幅{cum_pr:.2f}%）。\n'
                    f'本质：空头止损/爆仓→止损单=市价买入→推高价格→'
                    f'同时关闭仓位(OI降)。整个上涨靠旧空头被迫平仓驱动，'
                    f'没有新多头真金白银入场。\n\n'
                    f'减速确认（{cf}/3）：\n'
                    f'{"✅" if oi_slow else "❌"} OI降幅'
                    f'{"收窄" if oi_slow else "未收窄"}'
                    f'（本根{oi_i:.2f}% vs 均值{-avg_oi_abs:.2f}%）'
                    f'{"→可被逼平的空头越来越少，燃料快烧完了" if oi_slow else ""}\n'
                    f'{"✅" if pr_slow else "❌"} 涨幅'
                    f'{"收窄" if pr_slow else "未收窄"}'
                    f'（本根{pr_i:.2f}% vs 均值{avg_pr:.2f}%）'
                    f'{"→每轮回补推动的幅度越来越小" if pr_slow else ""}\n'
                    f'{"✅" if cvd_neg else "❌"} CVD {cv_i:+.2f}%'
                    f'{"→主动卖单>买单，真实意愿是卖，涨纯靠被动回补撑着" if cvd_neg else "→有主动买盘参与"}\n\n'
                    f'操作：小仓位(≤1/3)试空，等下一根价格掉头后加仓。'
                    f'止损放在本轮挤压最高点上方。'
                )

            # ── cf == 1 → 等待 ──
            elif cf == 1:
                sig[i] = 'short_wait'
                sc[i]  = min(round(base + 15), 100)
                rsn[i] = (
                    f'【等待·挤压仍在进行】\n'
                    f'连续{sq_n}根空头挤压'
                    f'（累计OI流出{cum_oi:.2f}%，累计涨幅{cum_pr:.2f}%）。\n'
                    f'减速确认仅{cf}/3，挤压尚未明显减速。\n\n'
                    f'为什么现在不能空：\n'
                    f'你新开的空单 = 新的燃料。价格继续涨→你被止损→'
                    f'你的止损=市价买入→推更高→更多空头止损→连环踩踏。'
                    f'你的亏损不是线性的而是加速的。\n\n'
                    f'等待条件：OI降幅收窄 + 涨幅收窄 + CVD转负，'
                    f'至少2个满足再考虑入场。'
                )

            # ── cf == 0 → 危险 ──
            else:
                sig[i] = 'short_danger'
                sc[i]  = min(round(base + 5), 100)
                rsn[i] = (
                    f'【危险·挤压正在加速】\n'
                    f'连续{sq_n}根挤压'
                    f'（累计OI流出{cum_oi:.2f}%，累计涨幅{cum_pr:.2f}%），'
                    f'0个减速信号。\n\n'
                    f'OI暴降({oi_i:.2f}%)→空头正在被大量强平\n'
                    f'涨幅扩大({pr_i:.2f}%)→每轮回补推得更高\n'
                    f'CVD({cv_i:+.2f}%)未转负\n\n'
                    f'这是踩踏的高潮阶段，做空=送钱，绝对不要碰。'
                )

        # ============================================
        #  3B. 挤压已结束, 评估当前根
        # ============================================
        else:
            base = (50
                    + min(sq_n / 6, 1) * 15
                    + min(abs(cum_oi) / 8, 1) * 15)

            # ── B1: 价格跌 + OI涨 → 新空入场, 最强做空 ──
            if pr_i < 0 and oi_i > 0:
                bonus = 10 if cv_i < 0 else 0
                sig[i] = 'trend_reversal'
                sc[i]  = min(round(base + 20 + bonus), 100)
                rsn[i] = (
                    f'【趋势反转·最强做空信号】\n'
                    f'前面{sq_n}根空头挤压'
                    f'（累计OI流出{cum_oi:.2f}%，累计涨幅{cum_pr:.2f}%）已结束。\n\n'
                    f'当前转折：\n'
                    f'• 价格下跌({pr_i:+.2f}%) → 回补买盘已消失，'
                    f'上涨失去唯一支撑\n'
                    f'• OI增加({oi_i:+.2f}%) → 新的空头正在入场建仓\n'
                    + (f'• CVD为负({cv_i:.2f}%) → 主动卖盘确认，'
                       f'三重共振\n' if cv_i < 0 else '') +
                    f'\n之前{cum_pr:.2f}%的涨幅全靠空头被迫平仓产生的'
                    f'被动买盘，没有新多头接盘。现在回补结束 + 新空入场，'
                    f'价格将回吐全部虚假涨幅甚至更多。\n\n'
                    f'做空，止损放在挤压创造的最高点上方。'
                    f'目标：回吐挤压涨幅的50-100%。'
                )

            # ── B2: 价格跌 + OI没涨 → 纯回落 ──
            elif pr_i < 0:
                sig[i] = 'short_now'
                sc[i]  = min(round(base + 10), 100)
                rsn[i] = (
                    f'【做空入场·挤压结束价格掉头】\n'
                    f'前面{sq_n}根空头挤压'
                    f'（累计OI流出{cum_oi:.2f}%，累计涨幅{cum_pr:.2f}%）后，'
                    f'价格开始回落({pr_i:+.2f}%)。\n\n'
                    f'回补买盘消失→价格失去支撑→自然回落。'
                    f'OI变化{oi_i:+.2f}%'
                    f'{"（还没有新空入场，后续可能加速下跌）" if oi_i <= 0 else ""}。\n\n'
                    f'做空，止损在挤压最高点上方。'
                    f'如后续OI开始增加(新空入场)可加仓。'
                )

            # ── B3: 价格涨+OI涨+CVD正 → 新多入场 → 失效 ──
            elif pr_i > 0 and oi_i > 0 and cv_i > 0:
                sig[i] = 'invalidated'
                sc[i]  = 0
                rsn[i] = (
                    f'【做空失效·新多头接管】\n'
                    f'虽然前面有{sq_n}根挤压模式，但当前：\n'
                    f'价格涨({pr_i:+.2f}%) + OI增({oi_i:+.2f}%) '
                    f'+ CVD正({cv_i:+.2f}%)\n'
                    f'→ 新多头真金白银入场，上涨性质已从"空头被动回补"'
                    f'转为"新多主动推动"。\n'
                    f'这是完全不同的上涨，做空逻辑已失效，不要逆势！'
                )

            # ── B4: 其他(横盘/微涨) → 准备 ──
            else:
                sig[i] = 'short_ready'
                sc[i]  = min(round(base), 100)
                rsn[i] = (
                    f'【准备做空·挤压暂停中】\n'
                    f'前面{sq_n}根挤压'
                    f'（累计OI流出{cum_oi:.2f}%，涨幅{cum_pr:.2f}%）后，'
                    f'当前价格{pr_i:+.2f}%，OI{oi_i:+.2f}%，'
                    f'CVD{cv_i:+.2f}%。\n'
                    f'回补力度已减弱。可小仓位试空，等价格掉头确认后加仓。\n'
                    f'如果下一根OI增+价格涨+CVD正→立刻止损，做空逻辑失效。'
                )

    df['short_signal'] = sig
    df['short_score']  = sc
    df['short_reason'] = rsn
    return df


import pandas as pd
import numpy as np


def calculate_short_signal(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算纯合约山寨币「价格大涨+OI大降」的高胜率做空信号

    参数:
        df: 输入DataFrame，需包含以下列：
            - timestamp: 时间戳
            - open, high, low, close: OHLC价格
            - volume: 成交量
            - cvd: 累计成交量Delta
            - oi_amount: 未平仓合约量
            - oi_amount_change_pct: 未平仓合约量变化百分比

    返回:
        新增信号列的DataFrame，包含：
            - signal_type: 信号类型 ('short', 'none')
            - signal_strength: 信号强度 (0-100)
            - signal_reason: 信号判断理由
            - entry_price: 建议入场价
            - stop_loss: 建议止损价
            - take_profit_1: 第一止盈价
            - take_profit_2: 第二止盈价
    """
    # 1. 数据预处理
    df = df.copy().sort_values('timestamp').reset_index(drop=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # 2. 计算关键辅助指标（避免未来数据，全部用shift(1)或历史窗口）
    # 2.1 计算日均波动（过去24根1小时K线的平均涨跌幅）
    df['price_change_pct'] = (df['close'] - df['close'].shift(1)) / df['close'].shift(1) * 100
    df['daily_volatility'] = df['price_change_pct'].abs().rolling(window=24).mean().shift(1)

    # 2.2 计算连续3根K线的累计涨幅
    df['3k_cum_pct'] = (df['close'] - df['close'].shift(3)) / df['close'].shift(3) * 100

    # 2.3 计算近10根K线的前高（用于判断是否创新高）
    df['10k_high'] = df['high'].rolling(window=10).max().shift(1)

    # 2.4 计算连续2根K线的OI累计降幅
    df['2k_oi_cum_pct'] = df['oi_amount_change_pct'] + df['oi_amount_change_pct'].shift(1)

    # 2.5 计算CVD变化及强度
    df['cvd_change'] = df['cvd'] - df['cvd'].shift(1)
    df['cvd_strength'] = abs(df['cvd_change']) / df['volume'].replace(0, np.nan) * 100  # 转为百分比

    # 2.6 计算前5根K线的平均成交量
    df['5k_avg_volume'] = df['volume'].rolling(window=5).mean().shift(1)

    # 3. 初始化信号列
    df['signal_type'] = 'none'
    df['signal_strength'] = 0
    df['signal_reason'] = ''
    df['entry_price'] = np.nan
    df['stop_loss'] = np.nan
    df['take_profit_1'] = np.nan
    df['take_profit_2'] = np.nan

    # 4. 遍历判断信号（从25开始，避免边界问题）
    for i in range(25, len(df)):
        # ----------------------
        # 第一阶：量化定义（必选）
        # ----------------------
        # 条件1：连续3根K线累计涨幅≥日均波动的50%
        condition1 = df.loc[i - 1, '3k_cum_pct'] >= df.loc[i - 1, 'daily_volatility'] * 0.5

        # 条件2：最后一根K线收盘价创近10根新高
        condition2 = df.loc[i - 1, 'close'] > df.loc[i - 1, '10k_high']

        # 条件3：最后1根K线OI降幅≥15%
        condition3 = df.loc[i - 1, 'oi_amount_change_pct'] <= -15

        # 条件4：连续2根K线OI累计降幅≥20%
        condition4 = df.loc[i - 1, '2k_oi_cum_pct'] <= -20

        quant_conditions = condition1 and condition2 and condition3 and condition4

        if not quant_conditions:
            continue

        # ----------------------
        # 第二阶：原因区分（必选，核心！区分出货/爆仓）
        # ----------------------
        # 条件5：OI降的K线CVD为负（主动卖盘多）
        condition5 = df.loc[i - 1, 'cvd_change'] < 0

        # 条件6：CVD强度≥20%
        condition6 = df.loc[i - 1, 'cvd_strength'] >= 20

        # 条件7：成交量≥前5根平均的2倍
        condition7 = df.loc[i - 1, 'volume'] >= df.loc[i - 1, '5k_avg_volume'] * 2

        cause_conditions = condition5 and condition6 and condition7

        if not cause_conditions:
            continue

        # ----------------------
        # 第三阶：确认信号（必选，确认失去上涨动力）
        # ----------------------
        # 条件8：当前K线收盘价无法再创新高
        condition8 = df.loc[i, 'close'] <= df.loc[i - 1, '10k_high']

        # 条件9：当前K线收盘价跌破最后一根上涨K线的最低价
        condition9 = df.loc[i, 'close'] < df.loc[i - 1, 'low']

        confirm_conditions = condition8 and condition9

        if not confirm_conditions:
            continue

        # ----------------------
        # 所有条件满足，生成做空信号
        # ----------------------
        df.loc[i, 'signal_type'] = 'short'

        # 计算信号强度
        strength = 50  # 基础分：满足量化定义
        strength += 20  # 满足原因区分（核心）
        strength += 20  # 满足确认信号
        strength += 10 if df.loc[i - 1, 'oi_amount_change_pct'] <= -20 else 0  # OI降超20%加10分
        df.loc[i, 'signal_strength'] = min(strength, 100)

        # 生成判断理由
        reason = (
            f"【四阶条件全满足】\n"
            f"1. 量化定义：连续3根K线累计涨{df.loc[i - 1, '3k_cum_pct']:.1f}%（≥日均{df.loc[i - 1, 'daily_volatility']:.1f}%的50%），创近10根新高；最后1根K线OI降{abs(df.loc[i - 1, 'oi_amount_change_pct']):.1f}%，2根累计降{abs(df.loc[i - 1, '2k_oi_cum_pct']):.1f}%。\n"
            f"2. 原因区分：OI降的K线CVD为负（主动卖盘主导），强度{df.loc[i - 1, 'cvd_strength']:.1f}%≥20%；成交量{df.loc[i - 1, 'volume']:.0f}≥前5根平均{df.loc[i - 1, '5k_avg_volume']:.0f}的2倍，确认庄家出货。\n"
            f"3. 确认信号：当前K线收盘价未创新高，且跌破最后一根上涨K线最低价{df.loc[i - 1, 'low']:.2f}，确认失去上涨动力。"
        )
        df.loc[i, 'signal_reason'] = reason

        # 计算建议交易价格
        df.loc[i, 'entry_price'] = df.loc[i, 'close']  # 入场价：当前K线收盘价
        df.loc[i, 'stop_loss'] = df.loc[i - 1, 'high'] * 1.01  # 止损价：最后一根上涨K线最高点上方1%
        # 计算上涨起点（3根K线前的收盘价）
        start_price = df.loc[i - 4, 'close']
        df.loc[i, 'take_profit_1'] = start_price  # 第一止盈价：上涨起点
        df.loc[i, 'take_profit_2'] = start_price - (
                    df.loc[i - 1, 'close'] - start_price) * 0.618  # 第二止盈价：上涨起点下方0.618倍涨幅

    # 5. 清理辅助列，返回结果
    df = df.drop(columns=[
        'price_change_pct', 'daily_volatility', '3k_cum_pct', '10k_high',
        '2k_oi_cum_pct', 'cvd_change', 'cvd_strength', '5k_avg_volume'
    ])
    return df

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

    #
    # # # 获取合约指定时间的波动率
    #
    # calc_minutes_list = [15, 30, 60, 90, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1000]
    #
    # bin_df = get_binance_volatility_ranking(minutes_list=calc_minutes_list, max_workers=20)
    # okx_df = get_okx_volatility_ranking(minutes_list=calc_minutes_list, max_workers=20)
    #
    # # 2. 添加来源列 (直接插入到最前面的第0列)
    # if not okx_df.empty: okx_df.insert(0, 'Source', 'OKX')
    # if not bin_df.empty: bin_df.insert(0, 'Source', 'Binance')
    #
    # # 3. 纵向合并 (直接叠加：50行 + 50行 = 100行)
    # df_combined = pd.concat([okx_df, bin_df], ignore_index=True)
    #
    # # 4. 合并后全局重新降序排序
    # if not df_combined.empty:
    #     df_combined = df_combined.sort_values(by='Overall Avg Vol %', ascending=False).reset_index(drop=True)
    #
    # print(f"合并成功！总行数: {len(df_combined)}")
    #
    #
    # 获取指定合约的实时持仓量 (OI)


    symbol = 'ARIA/USDT:USDT'
    days = 30
    timeframe = '1d'



    cvd_df = fetch_binance_cvd_history(symbol=symbol, timeframe=timeframe, days=days)
    result = detect_signal_a(cvd_df)

    oi_df = fetch_historical_oi(exchange_name='binance', symbol=symbol, timeframe=timeframe, days=days)

    merge_cvd_oi_df = merge_cvd_oi_complete(cvd_df, oi_df, timeframe=timeframe)

    # 将merge_cvd_oi_df保存为csv文件
    merge_cvd_oi_df.to_csv(r'W:\project\python_project\crypto_trade\data\grid_backtest_results.csv', index=False)




    file_path = r'W:\project\python_project\crypto_trade\data\grid_backtest_results.csv'
    merge_cvd_oi_df = pd.read_csv(file_path)
    calc_signal_df1 = calculate_short_signal(merge_cvd_oi_df)
    calc_signal_df = detect_short_signal(merge_cvd_oi_df)



    anlyse_df = detect_oi_signals_with_confidence(oi_df)
    # long_anlyse_df = detect_long_grid_signals_strict(oi_df)
    print()


    # 直接拉取高波动数据并且计算信号
    final_df = get_high_volatility_oi_signals(days=7, timeframe='1h', top_k=20)
    print()