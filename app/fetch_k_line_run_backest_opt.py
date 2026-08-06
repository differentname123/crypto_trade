"""
================================================================================
[功能摘要] B进A出 趋势骑行系统 (Trend Rider B→A)：
  进场 = 方案B：价格台阶上行 + OI新钱同步爬升 + FR温和不拥挤；
  出场 = 方案A 高潮后破位：抛物线加速 + (FR极端[正负皆可] 或 OI价值爆量) 后，
         价格跌破关键摆动低点（右侧确认）。
  纯多头、全程只使用这两个信号，无其他进出场路径。带可视化交互看板。
[输入数据]
  - df_oi: ['timestamp','oi_amount'] 毫秒级快照
  - df_fr: ['timestamp','funding_rate'] 毫秒级快照
  - df_klines: ['timestamp','open','high','low','close'] 毫秒级K线
[数据流转]
  1. K线: 趋势快慢线、关键摆动低(72h前低, shift(1)防未来函数)；
  2. OI(5m): 吸纳最新价计算 oi_value，算 快/慢MA(新钱) 与 30d 90%分位(价值爆量)；
  3. FR: 30d [10%,90%] 分位带 → 温和/极端 判定；
  4. merge_asof 对齐到1m主轴；状态机仅两信号迭代模拟。
[输出] 大白话日志、绩效看板、Plotly 交互图。
================================================================================
"""

import pandas as pd
import os
from zoneinfo import ZoneInfo

# ==========================================
# 1. 策略核心参数区
# ==========================================
# --- 方案B 进场 ---
TREND_FAST_H = 48        # 趋势快线（小时）
TREND_SLOW_H = 168       # 趋势慢线（小时=7天）
OI_FAST_D = 2            # OI 快均线（天）
OI_SLOW_D = 7            # OI 慢均线（天）
FR_WINDOW_DAYS = 30      # FR 自身分位统计周期（天）
FR_HI_PCT = 0.90         # FR 温和带上限
FR_LO_PCT = 0.10         # FR 温和带下限

# --- 方案A 出场 ---
OI_VALUE_WINDOW_DAYS = 30  # OI价值分位统计周期（天）
OI_VALUE_PCT = 0.90        # OI价值爆量水位（90%分位）
CLIMAX_EXT = 0.20          # 抛物线判定1: 收盘价偏离慢线 ≥20%
CLIMAX_SURGE_48H = 0.12    # 抛物线判定2: 48h涨幅 ≥12%
SWING_LOW_WINDOW_H = 72    # 关键摆动低周期（小时）

FEE_RATE = 0.001           # 单边交易成本（0.1%）


def plot_interactive_chart(df, trades, target_coin, start_time='2026-05-01', end_time='2026-08-01'):
    """[功能摘要] Plotly WebGL 交互图表：价格+趋势线+FR+OI数量+OI价值+买卖标记。"""

    pass


def run_backtest(target_coin, oi_file, fr_file, kline_file):
    print(f">>> [系统初始化] 正在加载并预处理 【{target_coin}】 底层数据源...")

    try:
        df_oi = pd.read_csv(oi_file)
        df_fr = pd.read_csv(fr_file)
        df_klines = pd.read_csv(kline_file)
    except FileNotFoundError as e:
        print(f"❌ [数据加载失败] 找不到底层数据文件，详细原因: {e}")
        return

    # ------------------------------------------
    # 3. 核心指标构建
    # ------------------------------------------
    # --- 价格维度：台阶上行结构 + 关键摆动低 ---
    df_klines['datetime'] = pd.to_datetime(df_klines['timestamp'], unit='ms')
    df_klines = df_klines.sort_values('datetime').reset_index(drop=True)
    df_klines['ma_fast'] = df_klines.rolling(f'{TREND_FAST_H}h', on='datetime')['close'].mean()
    df_klines['ma_slow'] = df_klines.rolling(f'{TREND_SLOW_H}h', on='datetime')['close'].mean()
    # 台阶上行 = 快线>慢线 且 价格在快线上方（同时防止破位后立刻重进场）
    df_klines['cond_trend'] = (df_klines['ma_fast'] > df_klines['ma_slow']) & (df_klines['close'] > df_klines['ma_fast'])

    df_klines['low_prev'] = df_klines['low'].shift(1)  # 防未来函数
    df_klines['support_key'] = df_klines.rolling(f'{SWING_LOW_WINDOW_H}h', on='datetime')['low_prev'].min()

    # --- OI维度：新钱爬升 + 名义价值爆量 ---
    df_oi['datetime'] = pd.to_datetime(df_oi['timestamp'], unit='ms')
    df_oi = df_oi.sort_values('datetime').reset_index(drop=True)
    # 吸纳最新价用于计算 OI 名义价值
    df_oi = pd.merge_asof(df_oi, df_klines[['timestamp', 'close']], on='timestamp', direction='backward')
    df_oi['oi_value'] = df_oi['oi_amount'] * df_oi['close']
    df_oi['oi_ma_fast'] = df_oi.rolling(f'{OI_FAST_D}D', on='datetime')['oi_amount'].mean()
    df_oi['oi_ma_slow'] = df_oi.rolling(f'{OI_SLOW_D}D', on='datetime')['oi_amount'].mean()
    df_oi['cond_oi_up'] = df_oi['oi_ma_fast'] > df_oi['oi_ma_slow']
    df_oi['oi_value_90pct'] = df_oi.rolling(f'{OI_VALUE_WINDOW_DAYS}D', on='datetime')['oi_value'].quantile(OI_VALUE_PCT)
    df_oi['cond_blowoff'] = df_oi['oi_value'] > df_oi['oi_value_90pct']

    # --- FR维度：温和带 / 极端 ---
    df_fr['datetime'] = pd.to_datetime(df_fr['timestamp'], unit='ms')
    df_fr = df_fr.sort_values('datetime').reset_index(drop=True)
    df_fr['fr_hi'] = df_fr.rolling(f'{FR_WINDOW_DAYS}D', on='datetime')['funding_rate'].quantile(FR_HI_PCT)
    df_fr['fr_lo'] = df_fr.rolling(f'{FR_WINDOW_DAYS}D', on='datetime')['funding_rate'].quantile(FR_LO_PCT)
    df_fr['cond_fr_mild'] = (df_fr['funding_rate'] <= df_fr['fr_hi']) & (df_fr['funding_rate'] >= df_fr['fr_lo'])
    df_fr['cond_fr_extreme'] = ~df_fr['cond_fr_mild']

    # ------------------------------------------
    # 4. 跨周期对齐 + 抛物线检测
    # ------------------------------------------
    # FIXME: backward 合并在长断网期会顺延陈旧快照，实盘需补“快照过期”判定。
    df_master = pd.merge_asof(df_klines,
        df_oi[['timestamp', 'oi_amount', 'oi_ma_fast', 'oi_ma_slow', 'cond_oi_up', 'oi_value', 'oi_value_90pct', 'cond_blowoff']],
        on='timestamp', direction='backward')
    df_master = pd.merge_asof(df_master,
        df_fr[['timestamp', 'funding_rate', 'fr_hi', 'fr_lo', 'cond_fr_mild', 'cond_fr_extreme']],
        on='timestamp', direction='backward')

    # 48h 前收盘价（gap 安全）。用整数毫秒位移，彻底避开 datetime 分辨率(ms/us)陷阱
    lag = df_master[['timestamp', 'close']].copy()
    lag['timestamp'] = lag['timestamp'] + 48 * 3600 * 1000
    lag = lag.rename(columns={'close': 'close_lag_48h'})
    df_master = pd.merge_asof(df_master, lag, on='timestamp', direction='backward')

    # 抛物线加速 = 偏离慢线≥CLIMAX_EXT 或 48h涨幅≥CLIMAX_SURGE_48H
    df_master['parabolic'] = ((df_master['close'] / df_master['ma_slow'] - 1) >= CLIMAX_EXT) | \
                             ((df_master['close'] / df_master['close_lag_48h'] - 1) >= CLIMAX_SURGE_48H)
    # 高潮 = 抛物线 + (FR极端 或 OI价值爆量)
    df_master['climax_now'] = df_master['parabolic'] & (df_master['cond_fr_extreme'] | df_master['cond_blowoff'])

    # 预热期：取三源“起点+各自最长窗口”的最大值
    valid_start_time = max(
        df_oi['datetime'].iloc[0] + pd.Timedelta(days=OI_VALUE_WINDOW_DAYS),
        df_fr['datetime'].iloc[0] + pd.Timedelta(days=FR_WINDOW_DAYS),
        df_klines['datetime'].iloc[0] + pd.Timedelta(hours=TREND_SLOW_H),
    )
    df_master = df_master[df_master['datetime'] >= valid_start_time].copy()
    df_master = df_master.dropna(subset=['ma_slow', 'close_lag_48h', 'support_key']).reset_index(drop=True)

    # FIXME: 次根开盘成交假设无滑点，极端行情实盘需滑点模型。
    df_master['next_open'] = df_master['open'].shift(-1)
    df_master['next_datetime'] = df_master['datetime'].shift(-1)
    df_master = df_master.dropna(subset=['next_open', 'next_datetime']).reset_index(drop=True)

    print(f">>> [数据清洗完毕] 有效 K 线数量: 【{len(df_master)}】 行")

    if len(df_master) > 0:
        c_trend = int(df_master['cond_trend'].sum())
        c_oi = int(df_master['cond_oi_up'].sum())
        c_fr = int(df_master['cond_fr_mild'].sum())
        c_regime = int((df_master['cond_trend'] & df_master['cond_oi_up'] & df_master['cond_fr_mild']).sum())
        c_climax = int(df_master['climax_now'].sum())
        print(f">>> [回测时间轴] {df_master['datetime'].iloc[0]:%Y-%m-%d} ~ {df_master['datetime'].iloc[-1]:%Y-%m-%d}")
        print(f">>> [信号探查器] 条件命中分布:")
        print(f"    ┣━ B-台阶上行(快>慢且价>快)   : 【{c_trend}】 行")
        print(f"    ┣━ B-新钱爬升(OI快MA>慢MA)    : 【{c_oi}】 行")
        print(f"    ┣━ B-FR温和(分位带内)         : 【{c_fr}】 行")
        print(f"    ┣━ B-进场共振(三者同时)        : 【{c_regime}】 行 (潜在点火次数)")
        print(f"    ┗━ A-高潮出现(抛物线+极端/爆量): 【{c_climax}】 行")

    print("\n>>> [启动引擎] 状态机遍历回测（仅B进/A出两信号）...\n")

    # ------------------------------------------
    # 5. 核心交易状态机
    # ------------------------------------------
    in_position = False
    climax_seen = False
    entry_price = 0.0
    entry_time = None

    trades = []
    initial_capital = 10000.0
    capital = initial_capital

    for row in df_master.itertuples():
        curr_time_str = row.datetime.replace(tzinfo=ZoneInfo("UTC")) \
            .astimezone(ZoneInfo("Asia/Shanghai")).strftime('%Y-%m-%d %H:%M')

        if not in_position:
            # ===== 信号1: 方案B 进场 =====
            if row.cond_trend and row.cond_oi_up and row.cond_fr_mild:
                entry_price = row.next_open
                entry_time = row.next_datetime
                in_position = True
                climax_seen = False
                print(f"[方案B入场] 台阶上行+新钱爬升+FR温和 | 触发: [{curr_time_str}] | 执行价: [{entry_price:.4f}] "
                      f"| OI快慢MA: [{row.oi_ma_fast:.1f}>{row.oi_ma_slow:.1f}] | FR: [{row.funding_rate * 100:.4f}%]")
        else:
            # 持仓期：高潮粘滞记忆
            if row.climax_now and not climax_seen:
                climax_seen = True
                print(f"[高潮确认] 抛物线+({('FR极端' if row.cond_fr_extreme else '')}{'/' if row.cond_fr_extreme and row.cond_blowoff else ''}{('OI价值爆量' if row.cond_blowoff else '')}) "
                      f"| [{curr_time_str}] | 等待破位出场...")

            # ===== 信号2: 方案A 高潮后破位出场 =====
            if climax_seen and (row.close < row.support_key):
                exit_price = row.next_open
                exit_time = row.next_datetime
                gross_return = (exit_price - entry_price) / entry_price
                net_return = gross_return - 2 * FEE_RATE
                capital *= (1 + net_return)
                trades.append({'entry_time': entry_time, 'entry_price': entry_price,
                               'exit_time': exit_time, 'exit_price': exit_price,
                               'net_return': net_return, 'capital': capital})
                in_position = False
                print(f"[方案A出场] 高潮后跌破{SWING_LOW_WINDOW_H}h摆动低 | 触发: [{curr_time_str}] | 执行价: [{exit_price:.4f}] "
                      f"| 单笔净收益: [{net_return * 100:+.2f}%] | 当前净值: [{capital:.2f}]")

    # 回测结束仍持仓 → 按最后收盘价强制结算
    if in_position:
        last_row = df_master.iloc[-1]
        exit_price = last_row['close']
        gross_return = (exit_price - entry_price) / entry_price
        net_return = gross_return - 2 * FEE_RATE
        capital *= (1 + net_return)
        trades.append({'entry_time': entry_time, 'entry_price': entry_price,
                       'exit_time': last_row['datetime'], 'exit_price': exit_price,
                       'net_return': net_return, 'capital': capital})
        print(f"[强制结算] 回测结束仍持仓，按 [{exit_price:.4f}] 平仓 | 单笔净收益: [{net_return * 100:+.2f}%]")

    # ------------------------------------------
    # 6. 绩效报告与绘图
    # ------------------------------------------
    print("\n==================================================")
    print(f"📊 [绩效看板] B进A出 Trend Rider - 【{target_coin}】")
    print("==================================================")

    if not trades:
        print("💡 回测结论: 未触发交易。方案B进场本就'条件不齐不做'，零交易=该币没有健康趋势段。")
        print("==================================================\n")
        if len(df_master) > 0:
            plot_interactive_chart(df_master, trades, target_coin)
        return

    trades_df = pd.DataFrame(trades)
    total_trades = len(trades_df)
    win_rate = len(trades_df[trades_df['net_return'] > 0]) / total_trades
    total_return_pct = (capital - initial_capital) / initial_capital
    capital_cummax = trades_df['capital'].cummax()
    max_drawdown = ((capital_cummax - trades_df['capital']) / capital_cummax).max()

    print(f"总交易笔数       : 【{total_trades}】 笔")
    print(f"策略胜率         : 【{win_rate * 100:.2f}%】")
    print(f"总净收益率       : 【{total_return_pct * 100:.2f}%】")
    print(f"区间最大回撤     : 【{max_drawdown * 100:.2f}%】")
    print(f"平均每笔净收益   : 【{trades_df['net_return'].mean() * 100:.2f}%】")
    print(f"平均持仓时间     : 【{(trades_df['exit_time'] - trades_df['entry_time']).dt.total_seconds().mean() / 3600:.1f}】 小时")
    print("==================================================\n")

    if len(df_master) > 0:
        plot_interactive_chart(df_master, trades, target_coin)


def scan_and_run_batch(data_dir='./data'):
    """扫描数据目录，自动拼装参数批量回测"""
    if not os.path.exists(data_dir):
        print(f"❌ [严重错误] 找不到数据目录 【{data_dir}】")
        return

    kline_files = [f for f in os.listdir(data_dir) if f.endswith('_USDT_USDT_1m_kline.csv')]
    if not kline_files:
        print(f"️ [无数据] 未发现 '*_USDT_USDT_1m_kline.csv' 文件。")
        return

    print(f"🔍 [自动嗅探] 共发现 【{len(kline_files)}】 个待测币种...")
    print("=" * 60)

    for kf in kline_files:
        target_coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
        oi_file = os.path.join(data_dir, f'{target_coin}_USDT_USDT_5m_oi.csv')
        fr_file = os.path.join(data_dir, f'{target_coin}_USDT_USDT_funding_rates.csv')
        kline_file = os.path.join(data_dir, kf)

        if os.path.exists(oi_file) and os.path.exists(fr_file):
            print(f"\n🚀 启动标的 【{target_coin}】 策略实例")
            print("-" * 60)
            run_backtest(target_coin, oi_file, fr_file, kline_file)
        else:
            print(f"⚠️ [跳过标的] 【{target_coin}】 数据不完整。")


if __name__ == "__main__":
    scan_and_run_batch('./data')