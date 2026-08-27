import ccxt
import time
import logging

from app.signal_trade_lite.common_utils_lite import get_config

# 配置基础日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

import ccxt
import time
import logging

# 配置基础日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def set_all_futures_max_leverage(api_key, secret_key, proxies=None):
    """
    一键将币安 U本位合约 的所有活跃交易对设置为最大杠杆
    """
    logger.info(">>> 开始初始化交易所并获取全市场数据...")
    config = {
        'apiKey': api_key,
        'secret': secret_key,
        'enableRateLimit': True,
        'options': {
            'defaultType': 'future',
            'adjustForTimeDifference': True
        }
    }
    if proxies:
        config['proxies'] = proxies

    exchange = ccxt.binance(config)

    try:
        # 1. 加载所有市场信息
        exchange.load_markets()

        # 2. 获取当前账户的所有杠杆阶梯配置 (CCXT 的标准方法是 fetch_leverage_tiers)
        logger.info(">>> 正在拉取全市场杠杆阶梯配置...")
        tiers = exchange.fetch_leverage_tiers()

        # 筛选出活跃的 U本位 USDT 交易对
        active_symbols = [
            symbol for symbol, market in exchange.markets.items()
            if market.get('active', False) and market.get('linear', False) and market.get('settle') == 'USDT'
        ]

        total = len(active_symbols)
        logger.info(f">>> 共发现 {total} 个活跃的 USDT 本位合约。开始批量设置...")

        success_count = 0
        fail_count = 0

        for i, symbol in enumerate(active_symbols, 1):
            try:
                # 解析该币种允许的最大杠杆
                max_leverage = 20  # 默认兜底 20 倍

                if tiers and symbol in tiers:
                    # tiers[symbol] 是一个包含多个阶梯的列表，我们提取所有阶梯中支持的最大杠杆
                    max_leverage = max([tier.get('maxLeverage', 1) for tier in tiers[symbol]])
                else:
                    # 兜底：从 market 元数据读取
                    market_info = exchange.market(symbol)
                    max_leverage = market_info.get('limits', {}).get('leverage', {}).get('max', 20)

                max_leverage = int(max_leverage)

                # 调用 API 设置杠杆
                exchange.set_leverage(max_leverage, symbol)
                logger.info(f"[{i}/{total}] 成功 | {symbol} 已设置为 {max_leverage}x")
                success_count += 1

            except Exception as e:
                error_msg = str(e)
                # 优化：如果是“当前已经是目标杠杆”，视为成功
                if "Target leverage" in error_msg or "-2027" in error_msg:
                    logger.info(f"[{i}/{total}] 成功 | {symbol} 当前已经是 {max_leverage}x (无需修改)")
                    success_count += 1
                else:
                    logger.error(f"[{i}/{total}] 失败 | {symbol} 设置杠杆失败: {error_msg}")
                    fail_count += 1

            # 【极其重要】防封禁休眠：币安对修改杠杆的 API (POST) 权重限制较严
            # 设置 0.15 秒的间隔，绝对安全
            time.sleep(0.15)

        logger.info(f"\n>>> 批量设置完成！总计: {total} 个币种 | 成功: {success_count} | 失败: {fail_count}")

    except Exception as e:
        logger.critical(f"执行过程中发生致命错误: {e}")


if __name__ == "__main__":
    YOUR_API_KEY = get_config("nana_biance_api_copy_key")
    YOUR_SECRET_KEY = get_config("nana_biance_api_copy_secret")
    # 国内运行需要配置代理，海外服务器可传 None
    YOUR_PROXIES = {
        'http': 'http://127.0.0.1:7890',
        'https': 'http://127.0.0.1:7890'
    }

    set_all_futures_max_leverage(YOUR_API_KEY, YOUR_SECRET_KEY, proxies=YOUR_PROXIES)