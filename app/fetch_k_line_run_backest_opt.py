"""
================================================================================
[功能摘要] B进A出 趋势骑行系统 (Trend Rider B→A) - 信号预计算极简架构
  进场 = 方案B：价格台阶上行 + OI新钱同步爬升 + FR温和不拥挤；
  出场 = 方案A 高潮后破位：抛物线加速 + (FR极端[正负皆可] 或 OI价值爆量) 后，
         价格跌破关键摆动低点（右侧确认）。
================================================================================
"""

import pandas as pd
import numpy as np
import os
from zoneinfo import ZoneInfo

# ==========================================
# 1. 策略核心参数区
# ==========================================
# --- 方案B 进场 ---
TREND_FAST_H = 48  # 趋势快线（小时）
TREND_SLOW_H = 168  # 趋势慢线（小时=7天）
OI_FAST_D = 2  # OI 快均线（天）
OI_SLOW_D = 7  # OI 慢均线（天）
FR_WINDOW_DAYS = 30  # FR 自身分位统计周期（天）
FR_HI_PCT = 0.90  # FR 温和带上限
FR_LO_PCT = 0.10  # FR 温和带下限

# --- 方案A 出场 ---
OI_VALUE_WINDOW_DAYS = 30  # OI价值分位统计周期（天）
OI_VALUE_PCT = 0.90  # OI价值爆量水位（90%分位）
CLIMAX_EXT = 0.20  # 抛物线判定1: 收盘价偏离慢线 ≥20%
CLIMAX_SURGE_48H = 0.12  # 抛物线判定2: 48h涨幅 ≥12%
SWING_LOW_WINDOW_H = 72  # 关键摆动低周期（小时）

FEE_RATE = 0.001  # 单边交易成本（0.1%）


# ==========================================
# 步骤 1: 基础特征计算 (Data & Features)
# ==========================================
def prepare_base_features(df_oi, df_fr, df_klines):
    """仅负责时间戳对齐与基础技术指标的计算，不涉及任何交易逻辑判定"""

    # --- K线特征 ---
    df_klines['datetime'] = pd.to_datetime(df_klines['timestamp'], unit='ms')
    df_klines = df_klines.sort_values('datetime').reset_index(drop=True)
    df_klines['ma_fast'] = df_klines.rolling(f'{TREND_FAST_H}h', on='datetime')['close'].mean()
    df_klines['ma_slow'] = df_klines.rolling(f'{TREND_SLOW_H}h', on='datetime')['close'].mean()
    df_klines['low_prev'] = df_klines['low'].shift(1)
    df_klines['support_key'] = df_klines.rolling(f'{SWING_LOW_WINDOW_H}h', on='datetime')['low_prev'].min()

    # --- OI特征 ---
    df_oi['datetime'] = pd.to_datetime(df_oi['timestamp'], unit='ms')
    df_oi = df_oi.sort_values('datetime').reset_index(drop=True)
    df_oi = pd.merge_asof(df_oi, df_klines[['timestamp', 'close']], on='timestamp', direction='backward')
    df_oi['oi_value'] = df_oi['oi_amount'] * df_oi['close']
    df_oi['oi_ma_fast'] = df_oi.rolling(f'{OI_FAST_D}D', on='datetime')['oi_amount'].mean()
    df_oi['oi_ma_slow'] = df_oi.rolling(f'{OI_SLOW_D}D', on='datetime')['oi_amount'].mean()
    df_oi['oi_value_90pct'] = df_oi.rolling(f'{OI_VALUE_WINDOW_DAYS}D', on='datetime')['oi_value'].quantile(
        OI_VALUE_PCT)

    # --- FR特征 ---
    df_fr['datetime'] = pd.to_datetime(df_fr['timestamp'], unit='ms')
    df_fr = df_fr.sort_values('datetime').reset_index(drop=True)
    df_fr['fr_hi'] = df_fr.rolling(f'{FR_WINDOW_DAYS}D', on='datetime')['funding_rate'].quantile(FR_HI_PCT)
    df_fr['fr_lo'] = df_fr.rolling(f'{FR_WINDOW_DAYS}D', on='datetime')['funding_rate'].quantile(FR_LO_PCT)

    # --- 对齐合并 ---
    df_master = pd.merge_asof(df_klines,
                              df_oi[
                                  ['timestamp', 'oi_amount', 'oi_ma_fast', 'oi_ma_slow', 'oi_value', 'oi_value_90pct']],
                              on='timestamp', direction='backward')
    df_master = pd.merge_asof(df_master,
                              df_fr[['timestamp', 'funding_rate', 'fr_hi', 'fr_lo']],
                              on='timestamp', direction='backward')

    # 计算48小时前的收盘价
    lag = df_master[['timestamp', 'close']].copy()
    lag['timestamp'] = lag['timestamp'] + 48 * 3600 * 1000
    lag = lag.rename(columns={'close': 'close_lag_48h'})
    df_master = pd.merge_asof(df_master, lag, on='timestamp', direction='backward')

    # 剔除指标预热期的无效数据
    valid_start_time = max(
        df_oi['datetime'].iloc[0] + pd.Timedelta(days=OI_VALUE_WINDOW_DAYS),
        df_fr['datetime'].iloc[0] + pd.Timedelta(days=FR_WINDOW_DAYS),
        df_klines['datetime'].iloc[0] + pd.Timedelta(hours=TREND_SLOW_H),
    )
    df_master = df_master[df_master['datetime'] >= valid_start_time].copy()
    df_master = df_master.dropna(subset=['ma_slow', 'close_lag_48h', 'support_key']).reset_index(drop=True)

    # 预留次根K线开盘价作为执行价
    df_master['next_open'] = df_master['open'].shift(-1)
    df_master['next_datetime'] = df_master['datetime'].shift(-1)
    df_master = df_master.dropna(subset=['next_open', 'next_datetime']).reset_index(drop=True)

    return df_master


# ==========================================
# 步骤 2: 独立信号生成器 (Signal Pre-computation)
# ==========================================
def generate_signals(df):
    """
    负责独立推演进出场信号。
    利用极速 Numpy 数组处理路径依赖（持仓记忆），对外仅输出干净的指令。
    """
    # 1. 基础布尔事件向量化（无需记忆的部分）
    df['cond_trend'] = (df['ma_fast'] > df['ma_slow']) & (df['close'] > df['ma_fast'])
    df['cond_oi_up'] = df['oi_ma_fast'] > df['oi_ma_slow']
    df['cond_fr_mild'] = (df['funding_rate'] <= df['fr_hi']) & (df['funding_rate'] >= df['fr_lo'])

    # 独立生成进场信号
    df['sig_entry'] = df['cond_trend'] & df['cond_oi_up'] & df['cond_fr_mild']

    # 2. 出场事件分解
    df['cond_blowoff'] = df['oi_value'] > df['oi_value_90pct']
    df['cond_fr_extreme'] = ~df['cond_fr_mild']
    parabolic = ((df['close'] / df['ma_slow'] - 1) >= CLIMAX_EXT) | \
                ((df['close'] / df['close_lag_48h'] - 1) >= CLIMAX_SURGE_48H)

    evt_climax = parabolic & (df['cond_fr_extreme'] | df['cond_blowoff'])
    evt_breakdown = df['close'] < df['support_key']

    # 3. Numpy 极速状态机推演 (解决路径依赖核心)
    entry_arr = df['sig_entry'].values
    climax_arr = evt_climax.values
    breakdown_arr = evt_breakdown.values

    exit_arr = np.zeros(len(df), dtype=bool)
    alert_arr = np.zeros(len(df), dtype=bool)  # 专门用于通知引擎打印"高潮"日志

    in_pos = False
    climax_seen = False

    for i in range(len(df)):
        if not in_pos:
            if entry_arr[i]:
                in_pos = True
                climax_seen = False  # 新开仓，重置高潮记忆
        else:
            if climax_arr[i] and not climax_seen:
                climax_seen = True
                alert_arr[i] = True  # 记录首次发生高潮的瞬间，供引擎打日志

            if climax_seen and breakdown_arr[i]:
                exit_arr[i] = True
                in_pos = False  # 离场，等待下次进场

    df['sig_exit'] = exit_arr
    df['sig_climax_alert'] = alert_arr

    return df


# ==========================================
# 步骤 3: 纯净回测引擎 (Backtest Engine)
# ==========================================
def execute_backtest(target_coin, df_master):
    """
    终极解耦引擎：不知指标为何物，只认 sig_entry 和 sig_exit。
    附带读取各项原始特征用于高保真日志打印。
    """
    print(f">>> [数据清洗完毕] 有效 K 线数量: 【{len(df_master)}】 行")
    print(f">>> [回测时间轴] {df_master['datetime'].iloc[0]:%Y-%m-%d} ~ {df_master['datetime'].iloc[-1]:%Y-%m-%d}")
    print(f">>> [信号探查器] 潜在入场点: 【{df_master['sig_entry'].sum()}】 次")
    print("\n>>> [启动引擎] 极简状态机执行撮合...\n")

    in_position = False
    entry_price = 0.0
    entry_time = None

    trades = []
    initial_capital = 10000.0
    capital = initial_capital

    for row in df_master.itertuples():
        curr_time_str = row.datetime.replace(tzinfo=ZoneInfo("UTC")) \
            .astimezone(ZoneInfo("Asia/Shanghai")).strftime('%Y-%m-%d %H:%M')

        # 引擎执行层只认三个独立信号
        if not in_position:
            if row.sig_entry:
                entry_price = row.next_open
                entry_time = row.next_datetime
                in_position = True
                print(f"[方案B入场] 台阶上行+新钱爬升+FR温和 | 触发: [{curr_time_str}] | 执行价: [{entry_price:.4f}] "
                      f"| OI快慢MA: [{row.oi_ma_fast:.1f}>{row.oi_ma_slow:.1f}] | FR: [{row.funding_rate * 100:.4f}%]")
        else:
            # 引擎收到日志广播指令，打印中间状态
            if row.sig_climax_alert:
                fr_str = 'FR极端' if row.cond_fr_extreme else ''
                slash = '/' if (row.cond_fr_extreme and row.cond_blowoff) else ''
                oi_str = 'OI价值爆量' if row.cond_blowoff else ''
                print(f"[高潮确认] 抛物线+({fr_str}{slash}{oi_str}) | [{curr_time_str}] | 等待破位出场...")

            if row.sig_exit:
                exit_price = row.next_open
                exit_time = row.next_datetime
                gross_return = (exit_price - entry_price) / entry_price
                net_return = gross_return - 2 * FEE_RATE
                capital *= (1 + net_return)

                trades.append({'entry_time': entry_time, 'entry_price': entry_price,
                               'exit_time': exit_time, 'exit_price': exit_price,
                               'net_return': net_return, 'capital': capital})
                in_position = False
                print(
                    f"[方案A出场] 高潮后跌破{SWING_LOW_WINDOW_H}h摆动低 | 触发: [{curr_time_str}] | 执行价: [{exit_price:.4f}] "
                    f"| 单笔净收益: [{net_return * 100:+.2f}%] | 当前净值: [{capital:.2f}]")

    # 回测结束强制结算
    if in_position:
        last_row = df_master.iloc[-1]
        exit_price = last_row.close
        gross_return = (exit_price - entry_price) / entry_price
        net_return = gross_return - 2 * FEE_RATE
        capital *= (1 + net_return)
        trades.append({'entry_time': entry_time, 'entry_price': entry_price,
                       'exit_time': last_row.datetime, 'exit_price': exit_price,
                       'net_return': net_return, 'capital': capital})
        print(f"[强制结算] 回测结束仍持仓，按 [{exit_price:.4f}] 平仓 | 单笔净收益: [{net_return * 100:+.2f}%]")

    # --- 绩效报告 ---
    print("\n==================================================")
    print(f"📊 [绩效看板] B进A出 Trend Rider - 【{target_coin}】")
    print("==================================================")

    if not trades:
        print("💡 回测结论: 未触发交易循环 (零信号或过滤严格)。")
        print("==================================================\n")
        return None

    trades_df = pd.DataFrame(trades)
    total_trades = len(trades_df)
    win_rate = len(trades_df[trades_df['net_return'] > 0]) / total_trades
    total_return_pct = (capital - initial_capital) / initial_capital
    capital_cummax = trades_df['capital'].cummax()
    max_drawdown = ((capital_cummax - trades_df['capital']) / capital_cummax).max()
    avg_net_return = trades_df['net_return'].mean()
    avg_duration_h = (trades_df['exit_time'] - trades_df['entry_time']).dt.total_seconds().mean() / 3600

    print(f"总交易笔数       : 【{total_trades}】 笔")
    print(f"策略胜率         : 【{win_rate * 100:.2f}%】")
    print(f"总净收益率       : 【{total_return_pct * 100:.2f}%】")
    print(f"区间最大回撤     : 【{max_drawdown * 100:.2f}%】")
    print(f"平均每笔净收益   : 【{avg_net_return * 100:.2f}%】")
    print(f"平均持仓时间     : 【{avg_duration_h:.1f}】 小时")
    print("==================================================\n")

    return {
        '币种': target_coin,
        '交易笔数': total_trades,
        '胜率(%)': win_rate * 100,
        '总收益率(%)': total_return_pct * 100,
        '最大回撤(%)': max_drawdown * 100,
        '均笔收益(%)': avg_net_return * 100,
        '均持仓(小时)': avg_duration_h
    }


# ==========================================
# 步骤 4: 主调度流程
# ==========================================
def run_backtest(target_coin, oi_file, fr_file, kline_file):
    print(f">>> [系统初始化] 正在加载并预处理 【{target_coin}】 底层数据源...")

    try:
        df_oi = pd.read_csv(oi_file)
        df_fr = pd.read_csv(fr_file)
        df_klines = pd.read_csv(kline_file)
    except FileNotFoundError as e:
        print(f"❌ [数据加载失败] 找不到底层数据文件，详细原因: {e}")
        return None

    # 规范的流水线架构
    df = prepare_base_features(df_oi, df_fr, df_klines)
    df = generate_signals(df)

    if df is None or len(df) == 0:
        return None

    return execute_backtest(target_coin, df)


def scan_and_run_batch(data_dir='./data'):
    """扫描数据目录，批量执行策略引擎，输出全局汇总"""
    if not os.path.exists(data_dir):
        print(f"❌ [严重错误] 找不到数据目录 【{data_dir}】")
        return

    kline_files = [f for f in os.listdir(data_dir) if f.endswith('_USDT_USDT_1m_kline.csv')]
    if not kline_files:
        print(f"️ [无数据] 未发现 '*_USDT_USDT_1m_kline.csv' 文件。")
        return

    print(f"🔍 [自动嗅探] 共发现 【{len(kline_files)}】 个待测币种...")
    print("=" * 60)

    all_results = []

    for kf in kline_files:
        target_coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
        oi_file = os.path.join(data_dir, f'{target_coin}_USDT_USDT_5m_oi.csv')
        fr_file = os.path.join(data_dir, f'{target_coin}_USDT_USDT_funding_rates.csv')
        kline_file = os.path.join(data_dir, kf)

        if os.path.exists(oi_file) and os.path.exists(fr_file):
            print(f"\n🚀 启动标的 【{target_coin}】 策略实例")
            print("-" * 60)
            res = run_backtest(target_coin, oi_file, fr_file, kline_file)
            if res is not None:
                all_results.append(res)
        else:
            print(f"⚠️ [跳过标的] 【{target_coin}】 数据不完整。")

    # --- 全局最终汇总报告 ---
    if all_results:
        summary_df = pd.DataFrame(all_results)

        pd.set_option('display.max_rows', None)
        pd.set_option('display.width', 1000)
        pd.set_option('display.unicode.ambiguous_as_wide', True)
        pd.set_option('display.unicode.east_asian_width', True)

        print("\n\n" + "🌟" * 40)
        print("🏆 [多标的批量回测 - 最终结果大汇总] 🏆")
        print("🌟" * 40)

        display_df = summary_df.copy()
        for col in ['胜率(%)', '总收益率(%)', '最大回撤(%)', '均笔收益(%)']:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.2f}%")
        display_df['均持仓(小时)'] = display_df['均持仓(小时)'].apply(lambda x: f"{x:.1f}")

        print(display_df.to_string(index=False))
        print("-" * 80)
        print(f"💡 综合统计:")
        print(f"   ┣━ 参与产生交易的币种总数 : 【{len(summary_df)}】 个")
        print(f"   ┣━ 所有币种平均胜率       : 【{summary_df['胜率(%)'].mean():.2f}%】")
        print(f"   ┣━ 所有币种平均总收益率   : 【{summary_df['总收益率(%)'].mean():.2f}%】")
        print(f"   ┗━ 所有币种平均最大回撤   : 【{summary_df['最大回撤(%)'].mean():.2f}%】")
        print("=" * 80 + "\n")
    else:
        print("\n⚠️ 跑完了，但是没有任何币种触发有效交易，无最终统计可展示。\n")


if __name__ == "__main__":
    scan_and_run_batch('./data')