"""
================================================================================
[功能摘要] 纯粹逼空捕获系统 (Pure Squeeze Catcher)：寻找极度拥挤且空头失血的行情进行突破做多。
[输入数据]
  - df_oi (DataFrame): 必须包含 ['timestamp', 'oi_amount'] 毫秒级快照。
  - df_fr (DataFrame): 必须包含 ['timestamp', 'funding_rate'] 毫秒级快照。
  - df_klines (DataFrame): 必须包含 ['timestamp', 'open', 'high', 'low', 'close'] 毫秒级K线。
[数据流转/交互]
  1. 计算衍生指标：对 OI 计算 30天滚动 90% 分位数；对 K线 分别计算 12h滚动前高与 1h滚动前低。
  2. 跨周期降维：以 1分钟 K线时间轴为基准，利用向下合并 (backward merge) 吸纳最近的 OI 与 FR 数据。
  3. 状态机流转：基于清洗后的主表迭代，通过信号布尔值判断进出场，并模拟成交资金变化。
[输出数据] 输出格式化的大白话业务执行日志，并在内存中产出最终的回测绩效统计面板。
================================================================================
"""

import pandas as pd
import os

# ==========================================
# 1. 策略核心参数区
# ==========================================
OI_WINDOW_DAYS = 30  # OI 历史分位数统计周期（天）
OI_PERCENTILE = 0.90  # OI 极端拥挤水位线（90%分位数）
FR_THRESHOLD = -0.0005  # 空头流血阈值，即 -0.05%
BREAKOUT_WINDOW_H = 12  # 进场突破计算周期（小时）
STOPLOSS_WINDOW_H = 1  # 出场止损计算周期（小时）
FEE_RATE = 0.001  # 单边交易成本（0.1%）


def run_backtest(target_coin, oi_file, fr_file, kline_file):
    print(f">>> [系统初始化] 正在加载并预处理 【{target_coin}】 底层数据源...")

    # ------------------------------------------
    # 2. 数据读取与防御性拦截
    # ------------------------------------------
    try:
        df_oi = pd.read_csv(oi_file)
        df_fr = pd.read_csv(fr_file)
        df_klines = pd.read_csv(kline_file)
    except FileNotFoundError as e:
        # 异常不被静默吞噬，通过大白话指引排查方向后直接终止流转
        print(f"❌ [数据加载失败] 找不到底层数据文件，详细原因: {e}")
        return

    # ------------------------------------------
    # 3. 核心指标构建 (利用 on='datetime' 避免频繁修改索引)
    # ------------------------------------------
    df_oi['datetime'] = pd.to_datetime(df_oi['timestamp'], unit='ms')
    df_oi = df_oi.sort_values('datetime').reset_index(drop=True)
    df_oi['oi_90pct'] = df_oi.rolling(f'{OI_WINDOW_DAYS}D', on='datetime')['oi_amount'].quantile(OI_PERCENTILE)
    df_oi['cond_A'] = df_oi['oi_amount'] > df_oi['oi_90pct']

    df_fr['datetime'] = pd.to_datetime(df_fr['timestamp'], unit='ms')
    df_fr = df_fr.sort_values('datetime').reset_index(drop=True)
    df_fr['cond_B'] = df_fr['funding_rate'] <= FR_THRESHOLD

    df_klines['datetime'] = pd.to_datetime(df_klines['timestamp'], unit='ms')
    df_klines = df_klines.sort_values('datetime').reset_index(drop=True)

    # 防未来函数：价格错位1格，确保使用上一根已走完的K线极值作为阻力/支撑
    df_klines['high_prev'] = df_klines['high'].shift(1)
    df_klines['low_prev'] = df_klines['low'].shift(1)
    df_klines['resist_12h'] = df_klines.rolling(f'{BREAKOUT_WINDOW_H}h', on='datetime')['high_prev'].max()
    df_klines['support_1h'] = df_klines.rolling(f'{STOPLOSS_WINDOW_H}h', on='datetime')['low_prev'].min()

    # ------------------------------------------
    # 4. 跨周期时间轴对齐
    # ------------------------------------------
    # FIXME: 业务边界缺陷预警 - backward 合并如果在长时间断网或无交易期间，可能会把极为陈旧的 FR 费率强行顺延，实盘时需补充“快照有效性过期”判定逻辑。
    df_master = pd.merge_asof(
        df_klines,
        df_oi[['timestamp', 'cond_A', 'oi_amount', 'oi_90pct']],
        on='timestamp', direction='backward'
    )
    df_master = pd.merge_asof(
        df_master,
        df_fr[['timestamp', 'cond_B', 'funding_rate']],
        on='timestamp', direction='backward'
    )

    # 预留30天的数据积累期，确保各项滚动指标足够真实
    valid_start_time = df_oi['datetime'].iloc[0] + pd.Timedelta(days=OI_WINDOW_DAYS)
    df_master = df_master[df_master['datetime'] >= valid_start_time].copy()

    # 巧妙地将下一根 K 线的开盘时间和价格前置到当前行，彻底消灭回测主循环中的跨行索引逻辑
    # FIXME: 业务边界缺陷预警 - 此处假设信号触发后能在次根K线无缝成交。实盘面对极端行情，次根K线开盘必有巨大滑点甚至直接跳空。
    df_master['next_open'] = df_master['open'].shift(-1)
    df_master['next_datetime'] = df_master['datetime'].shift(-1)
    # 剔除最后一根无"下一K线"数据的无效行
    df_master = df_master.dropna(subset=['next_open', 'next_datetime']).reset_index(drop=True)

    print(f">>> [数据清洗完毕] 已剔除预热期，实际用于回测的有效 K 线数量: 【{len(df_master)}】 行")

    if len(df_master) > 0:
        # 全局信号探查统计，直接暴露条件卡点，极大降低无交易时的排查成本
        time_start = df_master['datetime'].iloc[0].strftime('%Y-%m-%d')
        time_end = df_master['datetime'].iloc[-1].strftime('%Y-%m-%d')

        count_cond_a = df_master['cond_A'].sum()
        count_cond_b = df_master['cond_B'].sum()
        count_ab_sync = (df_master['cond_A'] & df_master['cond_B']).sum()
        count_all_sync = (df_master['cond_A'] & df_master['cond_B'] & (df_master['close'] > df_master['resist_12h'])).sum()

        print(f">>> [回测时间轴] 数据起止区间: 【{time_start}】 至 【{time_end}】")
        print(f">>> [信号探查器] 核心条件命中分布统计 (用于诊断零交易卡点):")
        print(f"    ┣━ 条件A: 燃料池充足 (OI > {OI_PERCENTILE*100:.0f}%分位数) : 命中 【{count_cond_a}】 行")
        print(f"    ┣━ 条件B: 空头正流血 (FR <= {FR_THRESHOLD*100:.2f}%)       : 命中 【{count_cond_b}】 行")
        print(f"    ┣━ 共振1: OI 与 FR [两者同时满足]          : 命中 【{count_ab_sync}】 行")
        print(f"    ┗━ 共振2: 三大条件完全共振 (外加价格突破前高) : 命中 【{count_all_sync}】 行 (即潜在点火次数)")

    print("\n>>> [启动引擎] 核心状态机开始遍历回测...\n")

    # ------------------------------------------
    # 5. 核心交易状态机 (扁平化遍历)
    # ------------------------------------------
    in_position = False
    entry_price = 0.0
    entry_time = None

    trades = []
    initial_capital = 10000.0
    capital = initial_capital

    # 使用 itertuples 替代 iloc，大幅提升遍历效率与代码可读性
    for row in df_master.itertuples():
        curr_time_str = row.datetime.strftime('%Y-%m-%d %H:%M:%S')

        if not in_position:
            # 状态判定：等待三大共振信号
            if row.cond_A and row.cond_B and (row.close > row.resist_12h):
                entry_price = row.next_open
                entry_time = row.next_datetime
                in_position = True

                print(
                    f"[交易/点火入场] 突破12h阻力 | 触发: [{curr_time_str}] | 执行价: [{entry_price:.4f}] | 燃料OI: [{row.oi_amount}>={row.oi_90pct:.1f}] | 流血FR: [{row.funding_rate * 100:.4f}%]")

        else:
            # 状态判定：一旦收盘价跌破1小时低点则无条件止损/止盈
            if row.close < row.support_1h:
                exit_price = row.next_open
                exit_time = row.next_datetime

                # 收益结算
                gross_return = (exit_price - entry_price) / entry_price
                net_return = gross_return - (FEE_RATE * 2)
                capital += capital * net_return

                trades.append({
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'exit_time': exit_time,
                    'exit_price': exit_price,
                    'net_return': net_return,
                    'capital': capital
                })

                in_position = False

                net_color_sign = "+" if net_return > 0 else ""
                print(
                    f"[交易/破位出场] 跌破1h支撑 | 触发: [{curr_time_str}] | 执行价: [{exit_price:.4f}] | 单笔净收益: [{net_color_sign}{net_return * 100:.2f}%] | 当前净值: [{capital:.2f}]")

    # ------------------------------------------
    # 6. 回测报告产出
    # ------------------------------------------
    print("\n==================================================")
    print(f"📊 [绩效看板] Pure Squeeze Catcher - 标的: 【{target_coin}】")
    print("==================================================")

    if not trades:
        print("💡 回测结论: 期间未触发任何符合所有前置条件的交易信号，建议检查数据时间跨度或适当放宽参数阈值。")
        print("==================================================\n")
        return

    trades_df = pd.DataFrame(trades)
    total_trades = len(trades_df)
    win_rate = len(trades_df[trades_df['net_return'] > 0]) / total_trades
    total_return_pct = (capital - initial_capital) / initial_capital

    # 修复了原回撤计算缺陷，现在严格比较每一次峰顶资产与当前资产
    capital_cummax = trades_df['capital'].cummax()
    max_drawdown = ((capital_cummax - trades_df['capital']) / capital_cummax).max()

    print(f"总交易笔数       : 【{total_trades}】 笔")
    print(f"策略胜率         : 【{win_rate * 100:.2f}%】")
    print(f"总净收益率       : 【{total_return_pct * 100:.2f}%】")
    print(f"区间最大回撤     : 【{max_drawdown * 100:.2f}%】")
    print(f"平均每笔净收益   : 【{trades_df['net_return'].mean() * 100:.2f}%】")
    print("==================================================\n")


def scan_and_run_batch(data_dir='./data'):
    """扫描指定目录下的数据文件，并自动拼装参数执行批量回测"""
    if not os.path.exists(data_dir):
        print(f"❌ [严重错误] 找不到数据目录 【{data_dir}】，请确保当前执行路径下存在该文件夹。")
        return

    # 通过嗅探 kline 文件提取所有潜在币种名称
    file_list = os.listdir(data_dir)
    kline_files = [f for f in file_list if f.endswith('_USDT_USDT_1m_kline.csv')]

    if not kline_files:
        print(f"⚠️ [无数据] 在 【{data_dir}】 目录下未发现任何符合 '*_USDT_USDT_1m_kline.csv' 格式的文件。")
        return

    print(f"🔍 [自动嗅探] 共发现 【{len(kline_files)}】 个待测币种，开始批量执行回测...")
    print("=" * 60)

    # 遍历每一个提取出来的币种，拼装文件路径进行回测
    for kf in kline_files:
        target_coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
        oi_file = os.path.join(data_dir, f'{target_coin}_USDT_USDT_5m_oi.csv')
        fr_file = os.path.join(data_dir, f'{target_coin}_USDT_USDT_funding_rates.csv')
        kline_file = os.path.join(data_dir, kf)

        # 防呆：必须确保三份数据同时存在才能回测
        if os.path.exists(oi_file) and os.path.exists(fr_file):
            print(f"\n🚀 正在启动针对标的 【{target_coin}】 的策略实例")
            print("-" * 60)
            run_backtest(target_coin, oi_file, fr_file, kline_file)
        else:
            print(f"⚠️ [跳过标的] 币种 【{target_coin}】 缺乏完整的底层数据(需要同时具备 oi, funding_rates, kline)，已自动跳过。")


if __name__ == "__main__":
    scan_and_run_batch('./data')