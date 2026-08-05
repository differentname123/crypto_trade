import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

# 1. 设置全局代理 (匹配你的端口)
os.environ['HTTP_PROXY'] = 'http://127.0.0.1:7890'
os.environ['HTTPS_PROXY'] = 'http://127.0.0.1:7890'


def fetch_dvol_data(currency="BTC", start_year=2021, output_file=None):
    """
    分段拉取从 start_year 到当前时间的指定币种(currency) DVOL 历史数据并保存为 CSV
    """
    # 如果没有指定文件名，则默认生成一个
    if output_file is None:
        output_file = f"{currency.lower()}_dvol_historical.csv"

    # 起始时间：2021-01-01 UTC
    start_dt = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    end_dt = datetime.now(timezone.utc)

    url = "https://www.deribit.com/api/v2/public/get_volatility_index_data"

    all_records = []
    chunk_days = 200  # 每次拉取 200 天的数据，保证稳定不会超载

    current_start = start_dt
    print(
        f"开始分段拉取 {currency} DVOL 数据（起点: {start_dt.strftime('%Y-%m-%d')}，终点: {end_dt.strftime('%Y-%m-%d')}）...\n")

    while current_start < end_dt:
        current_end = min(current_start + timedelta(days=chunk_days), end_dt)

        start_ts = int(current_start.timestamp() * 1000)
        end_ts = int(current_end.timestamp() * 1000)

        params = {
            "currency": currency,  # 使用动态参数
            "start_timestamp": start_ts,
            "end_timestamp": end_ts,
            "resolution": "1D"  # 日线级别
        }

        try:
            res = requests.get(url, params=params, timeout=15)
            res.raise_for_status()
            data = res.json()
            records = data.get("result", {}).get("data", [])

            p_start = current_start.strftime('%Y-%m-%d')
            p_end = current_end.strftime('%Y-%m-%d')

            if records:
                all_records.extend(records)
                print(f"  [✓] 成功获取区间数据: {p_start} ~ {p_end} (共 {len(records)} 条记录)")
            else:
                print(f"  [-] 区间 {p_start} ~ {p_end} 无数据返回 (可能早于官方上线时间或不支持该币种)")

        except Exception as e:
            print(f"  [X] 拉取区间 {current_start.strftime('%Y-%m-%d')} 失败，错误: {e}")

        # 推进到下一个时间窗口
        current_start = current_end
        time.sleep(0.3)  # 礼貌等待，防止频控

    if not all_records:
        print(f"\n未能拉取到 {currency} 的任何有效数据，可能 Deribit 暂未提供该币种的 DVOL 指数。")
        return None

    # 2. 转为 DataFrame 并做清洗与去重
    df = pd.DataFrame(all_records, columns=["timestamp", "open", "high", "low", "close"])

    # 按照时间戳去重并排序
    df = df.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)

    # 转换为 UTC 日期
    df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime('%Y-%m-%d')

    # 动态生成列名，比如 BTC_DVOL, ETH_DVOL
    dvol_col = f"{currency}_DVOL"
    df[dvol_col] = df['close']

    # 3. 核心计算：转化为同期限的物理日方差 Implied_Variance_It
    df['Implied_Variance_It'] = (df[dvol_col] / 100) ** 2 / 365

    # 只提取对 OccamVol 项目有用的列
    final_df = df[['date', 'timestamp', dvol_col, 'Implied_Variance_It']].copy()

    # 4. 保存为本地 CSV 文件
    final_df.to_csv(output_file, index=False)

    print("\n" + "=" * 50)
    print(f"{currency} 下载完成！全量数据已成功导出至文件: {output_file}")
    print(f"数据的总天数: {len(final_df)} 天")
    print(f"最早日期: {final_df['date'].iloc[0]}")
    print(f"最新日期: {final_df['date'].iloc[-1]}")
    print("=" * 50)

    return final_df


if __name__ == "__main__":
    symbol_list = [
        "BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
        "XRP/USDT:USDT", "BNB/USDT:USDT", "DOGE/USDT:USDT"
    ]

    for symbol in symbol_list:
        # 从交易对名称中提取基础币种，例如把 "BTC/USDT:USDT" 切割提取出 "BTC"
        currency = symbol.split('/')[0]

        print(f"\n\n>>> 正在处理资产: {symbol} -> 提取币种: {currency} <<<")

        # 动态设置保存的文件名
        output_filename = f"{currency.lower()}_dvol_2021_now.csv"
        if os.path.exists(output_filename):
            print(f"文件 {output_filename} 已存在，跳过下载。")
            continue
        # 调用下载函数
        df_dvol = fetch_dvol_data(currency=currency, start_year=2021, output_file=output_filename)

        # 打印几行预览确认数据正常
        if df_dvol is not None:
            print(f"\n{currency} 前 3 行预览：")
            print(df_dvol.head(3))