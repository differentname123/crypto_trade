"""
通过 CoinGlass 免费 API 获取长周期 OI 数据
需要先在 https://www.coinglass.com/pricing 注册免费账号获取 API Key
免费 tier: 每月 1000 次请求
"""
import requests
import pandas as pd
import time

COINGLASS_API_KEY = "7a01e9ae80864ad9a49a365090bc8ac2"  # 注册免费获取
BASE_URL = "https://open-api-v3.coinglass.com/api"

def fetch_coinglass_oi_history(symbol='BTC', exchange='Binance', interval='1h', days=180):
    """
    CoinGlass OI History 接口
    免费 tier 支持的 interval: 1h, 4h, 1d
    数据深度远超交易所原生 API
    """
    headers = {
        "accept": "application/json",
        "CG-API-KEY": COINGLASS_API_KEY,
    }

    all_data = []
    end_time = int(time.time())
    start_time = end_time - days * 86400

    # CoinGlass v3 接口示例 — 获取 OI 历史
    url = f"{BASE_URL}/futures/openInterest/ohlc-history"
    params = {
        "symbol": symbol,
        "exchangeName": exchange,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
    }

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        result = resp.json()

        if result.get('code') == '0' and result.get('data'):
            data_list = result['data']
            for item in data_list:
                all_data.append({
                    'timestamp': pd.to_datetime(item['t'], unit='ms'),
                    'oi_open': item.get('o', 0),
                    'oi_high': item.get('h', 0),
                    'oi_low': item.get('l', 0),
                    'oi_close': item.get('c', 0),
                })
        else:
            print(f"CoinGlass 返回异常: {result.get('msg', 'unknown error')}")

    except Exception as e:
        print(f"请求 CoinGlass 出错: {e}")

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data)
    df.sort_values('timestamp', inplace=True)
    df.reset_index(drop=True, inplace=True)

    # 转换为北京时间
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)

    # 计算涨跌幅（基于收盘 OI）
    df['oi_change_pct'] = (df['oi_close'].pct_change() * 100).round(4).fillna(0)

    return df
if __name__ == '__main__':
    df = fetch_coinglass_oi_history('RAVE', 'Binance', '1h', days=180)
    print()