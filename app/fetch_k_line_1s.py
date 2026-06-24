import requests
import pandas as pd
import time
import os


def fetch_binance_1s_klines_by_time(symbol="BTCUSDT", start_time='2026-04-26 14:19:15', end_time='2026-04-26 14:25:00'):
    """
    通过连续合约接口拉取指定时间段的 1s 级别历史 K 线数据

    :param symbol: 交易对，如 BTCUSDT
    :param start_time: 开始时间字符串 (默认以北京时间/东八区为准)
    :param end_time: 结束时间字符串
    :return: 包含完整数据的 Pandas DataFrame
    """

    # 推荐使用 fapi 域名以规避 www 域名的严格 WAF 防火墙限制
    url = 'https://fapi.binance.com/fapi/v1/continuousKlines'

    # 利用 Pandas 直接将字符串时间转换为毫秒级时间戳 (指定东八区)
    start_ts = int(pd.Timestamp(start_time, tz='Asia/Shanghai').timestamp() * 1000)
    end_ts = int(pd.Timestamp(end_time, tz='Asia/Shanghai').timestamp() * 1000)

    # 如果在国内无法直连，可以在这里配置代理
    proxies = {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890"
    }

    # 伪装基本的请求头
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36'
    }

    # ================== 【修改模块】：本地历史数据检测与精确续传处理 ==================
    csv_filename = f"{symbol}_1s_klines.csv"
    current_start = start_ts
    local_df_exists = False
    local_ts_set = set()  # 用于存储本地已有数据的毫秒时间戳集合
    local_df = pd.DataFrame()  # 提前初始化，方便后续合并

    print(f"开始任务: {symbol} 1s K线数据...")
    print(f"目标范围: {start_time} 至 {end_time}")

    if os.path.exists(csv_filename):
        try:
            local_df = pd.read_csv(csv_filename)
            if not local_df.empty:
                # 【底层细节优化】：先将读取到的字符串转化为 Pandas Datetime 时间格式
                local_df['Open_Time'] = pd.to_datetime(local_df['Open_Time'])

                # 【严格要求落实】：只要加载了本地数据，立刻进行去重和排序，确保判定基准完美无瑕
                local_df = local_df.drop_duplicates(subset=['Open_Time']).sort_values('Open_Time').reset_index(
                    drop=True)

                local_df_exists = True

                # 【核心修复】：使用与 UTC 起源时间的绝对差值除以 1ms，免疫任何 Pandas 版本精度差异
                local_df_tz = local_df['Open_Time'].dt.tz_localize('Asia/Shanghai')
                local_ts_array = ((local_df_tz - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta('1ms')).astype(
                    'int64')
                local_ts_set = set(local_ts_array.tolist())

                # 精确计算目标区间内总共需要的秒数，以及本地已经包含的秒数
                expected_ts_count = (end_ts - start_ts) // 1000 + 1
                in_range_count = local_ts_array[(local_ts_array >= start_ts) & (local_ts_array <= end_ts)].nunique()

                if in_range_count >= expected_ts_count:
                    current_start = end_ts + 1000  # 将游标直接顶出边界，安全跳过 while 循环
                    print(
                        f"💡 本地数据已完全覆盖所请求的时间段 (包含该区间内完整 {in_range_count} 秒数据)，跳过网络请求。")
                elif in_range_count > 0:
                    print(
                        f"💡 发现本地历史数据！当前目标区间已覆盖 {in_range_count}/{expected_ts_count} 秒，自动跳过重复秒，开始精准补漏拉取...")
                else:
                    print(f"💡 本地文件存在，但未包含目标时间段的数据，开始全面拉取...")
        except Exception as e:
            print(f"⚠️ 读取本地历史数据失败，将尝试从头开始拉取: {e}")
    # ==================================================================================

    all_klines = []

    # ================== 【修改模块】：引入强制安全网 try...except...finally ==================
    try:
        while current_start <= end_ts:
            # 如果当前秒在本地已存在，则以1s为步长向后滑行，直到遇到真正缺失的数据点
            if local_df_exists and current_start in local_ts_set:
                while current_start <= end_ts and current_start in local_ts_set:
                    current_start += 1000
                continue  # 重新判定外层 while 条件

            params = {
                'pair': symbol,
                'contractType': 'PERPETUAL',  # 永续合约
                'interval': '1s',  # 隐藏的 1s 级别
                'limit': 1000,  # 单次最大拉取 1000 条 (相当于 1000 秒)
                'startTime': current_start,
                'endTime': end_ts
            }

            response = requests.get(url, params=params, headers=headers, proxies=proxies, timeout=10)

            # 如果遇到 429 错误说明请求太快被限流了
            if response.status_code == 429:
                print("触发频率限制，暂停 5 秒后重试...")
                time.sleep(5)
                continue

            response.raise_for_status()
            data = response.json()

            # 如果返回的数据为空，说明已经拉完了或该时间段无数据，跳出循环
            if not data:
                break

            all_klines.extend(data)

            # 拿到本次拉取的最后一条数据的时间戳
            last_kline_time = data[-1][0]

            # 打印进度条
            last_kline_time_str = pd.to_datetime(last_kline_time, unit='ms').tz_localize('UTC').tz_convert(
                'Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')
            print(f"已从接口拉取到: {last_kline_time_str} | 本次运行已积攒: {len(all_klines)} 条")

            # 更新下一次请求的 startTime (上一批最后一条的时间戳 + 1000毫秒也就是1秒)
            current_start = last_kline_time + 1000

            # 强制休眠 0.2 秒，防止把币安服务器薅得太狠导致封 IP
            time.sleep(0.2)

    except KeyboardInterrupt:
        print("\n🛑 用户手动强制中断了数据拉取！")
    except Exception as e:
        print(f"\n❌ 请求过程发生异常/中断: {e}")

    finally:
        # 无论发生什么情况（正常结束、报错崩溃、手动中断），都会进入这里强制保存
        if all_klines:
            print("💾 触发数据保护机制，正在持久化保存已拉取到的新数据...")
            columns = [
                "Open_Time", "Open", "High", "Low", "Close", "Volume",
                "Close_Time", "Quote_Asset_Volume", "Number_of_Trades",
                "Taker_Buy_Base_Volume", "Taker_Buy_Quote_Volume", "Ignore"
            ]
            new_df = pd.DataFrame(all_klines, columns=columns)

            # 清洗数据格式
            new_df['Open_Time'] = pd.to_datetime(new_df['Open_Time'], unit='ms') + pd.Timedelta(hours=8)
            new_df['Close_Time'] = pd.to_datetime(new_df['Close_Time'], unit='ms') + pd.Timedelta(hours=8)

            numeric_cols = ["Open", "High", "Low", "Close", "Volume", "Quote_Asset_Volume", "Taker_Buy_Base_Volume",
                            "Taker_Buy_Quote_Volume"]
            new_df[numeric_cols] = new_df[numeric_cols].astype(float)

            # 【严格要求落实】：不再使用粗暴的追加模式，而是与完美的历史数据合并，统一全局去重排序后覆写
            if local_df_exists and not local_df.empty:
                merged_df = pd.concat([local_df, new_df], ignore_index=True)
            else:
                merged_df = new_df

            # 【保存前最终守门】：无论是第一次创建文件，还是多次增量合成，这里对Datetime的排序能确保落地CSV的绝对整洁
            merged_df = merged_df.drop_duplicates(subset=['Open_Time']).sort_values('Open_Time').reset_index(drop=True)
            merged_df.to_csv(csv_filename, index=False)

            print(
                f"✅ 成功将本次 {len(new_df)} 条增量数据与本地融合，已全局去重并排序，当前文件共包含 {len(merged_df)} 条数据: {csv_filename}")
        else:
            print("ℹ️ 本次运行并未产生需要保存的新增量数据。")

    # ================== 【修改模块】：最终统一从本地数据库提取请求区间返回 ==================
    if os.path.exists(csv_filename):
        final_df = pd.read_csv(csv_filename)
        final_df['Open_Time'] = pd.to_datetime(final_df['Open_Time'])
        final_df['Close_Time'] = pd.to_datetime(final_df['Close_Time'])

        # 精确切割用户指定的起止时间
        start_dt = pd.to_datetime(start_time)
        end_dt = pd.to_datetime(end_time)
        mask = (final_df['Open_Time'] >= start_dt) & (final_df['Open_Time'] <= end_dt)
        result_df = final_df.loc[mask].copy()

        if not result_df.empty:
            # 【终极兜底守门员】：不管硬盘里的历史文件残留了多少脏数据，提取交付前强制进行一次去重和排序！
            result_df = result_df.drop_duplicates(subset=['Open_Time']).sort_values('Open_Time').reset_index(drop=True)

            print(
                f"\n🎉 数据提取交付完成！当前目标区间 [{start_time} - {end_time}] 共拥有完整的 {len(result_df)} 条数据。")
            return result_df
        else:
            print("\n本地数据未覆盖该时间段且未拉取到新数据。")
            return None
    else:
        print("\n未获取到任何数据。")
        return None


if __name__ == "__main__":
    # 调用封装好的函数
    # 请根据您的实际需求修改时间和币种
    target_start = '2026-01-01 00:00:00'
    target_end = '2026-06-03 00:00:00'

    df_result = fetch_binance_1s_klines_by_time(
        symbol="BTCUSDT",
        start_time=target_start,
        end_time=target_end
    )

    if df_result is not None:
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        # 打印头尾数据核对
        print("\n头部数据预览:")
        print(df_result[['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume']].head(3))
        print("\n尾部数据预览:")
        print(df_result[['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume']].tail(3))