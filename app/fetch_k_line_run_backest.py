import pandas as pd
import numpy as np
import os
import glob
import pandas as pd


# ============================================================================
# 工具函数：计算 ATR (Average True Range)
# ============================================================================
def calculate_atr(df, period=14):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift())
    low_close = np.abs(df['low'] - df['close'].shift())
    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return true_range.rolling(period, min_periods=1).mean()


# ============================================================================
# LER 策略回测引擎核心类
# ============================================================================
class LERBacktester:
    def __init__(self, data_path, btc_data_path, symbol, slip_and_fee=0.0015):
        """
        初始化回测引擎
        :param data_path: 标的 5m LER 特征宽表路径
        :param btc_data_path: BTC 的 K线数据路径 (适配 Binance open_time 毫秒格式)
        :param slip_and_fee: 综合滑点与手续费预期
        """
        self.data_path = data_path
        self.btc_data_path = btc_data_path
        self.symbol = symbol
        self.slip_and_fee = slip_and_fee
        self.df = None
        self.trades = []

    def load_and_prepare_data(self):
        print(f"[{self.symbol}] 正在加载并对齐本地数据...")

        # ---------------------------------------------------------
        # 1. 标的数据：严格时间语义对齐 (彻底消除未来函数)
        # ---------------------------------------------------------
        df = pd.read_csv(self.data_path)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df.sort_values('timestamp', inplace=True)

        # 【核心修正】：对齐期初与期末视场
        # 根据需求：当前行 i 的 K线/CVD 发生在该 5 分钟"期间"
        # 那么在这个 5 分钟"期末"结束时，我们观测到的最新 OI 快照，其实是下一行 (i+1) 的期初 OI。
        df['oi_amount_end'] = df['oi_amount'].shift(-1)
        df['oi_change_pct_end'] = df['oi_amount_change_pct'].shift(-1)

        # 仅将索引设为时间戳，【严禁 dropna 破坏底层的连续时间网格】
        df.set_index('timestamp', inplace=True)

        # ---------------------------------------------------------
        # 2. BTC 数据：解析 open_time 并重采样
        # ---------------------------------------------------------
        btc_df = pd.read_csv(self.btc_data_path)
        # 解析 Binance 的 open_time 毫秒戳，并转为无时区的北京时间 (与你的抓取脚本保持严格一致)
        btc_df['timestamp'] = pd.to_datetime(btc_df['open_time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert(
            'Asia/Shanghai').dt.tz_localize(None)
        btc_df.sort_values('timestamp', inplace=True)
        btc_df.set_index('timestamp', inplace=True)

        # 强制按 5 分钟左闭左开重采样，确保与标的币的 timeframe 完全卡位

        btc_5m = btc_df.resample('5min', label='left', closed='left').agg({'close': 'last'})

        btc_30m_drop = (btc_5m['close'] / btc_5m['close'].shift(6) - 1).rename('btc_30m_ret')

        # ---------------------------------------------------------
        # 3. 数据集熔合与业务特征衍算
        # ---------------------------------------------------------
        df = df.join(btc_30m_drop, how='left')
        # 【防御性修复】：前向填充填补缺失的 BTC 收益率，避免 NaN 污染过滤条件导致静默漏单
        df['btc_30m_ret'] = df['btc_30m_ret'].ffill().fillna(0)
        print(f"[{self.symbol}] 正在生成 LER 状态机阈值特征 (无时间泄漏)...")
        df['atr_14'] = calculate_atr(df, 14)

        # 滚动窗口参数 (5m K线: 1天 = 288根)
        window_90d = 288 * 90
        window_30d = 288 * 30
        window_20d = 288 * 20

        # 特征必须使用 "期末落定" 的对齐变量进行计算，保证历史观测的纯净性
        df['fr_90d_90p'] = df['predicted_funding_rate'].rolling(window=window_90d, min_periods=288 * 3).quantile(0.9)
        df['oi_20d_80p'] = df['oi_amount_end'].rolling(window=window_20d, min_periods=288 * 3).quantile(0.8)

        # 降幅极值统计（由于是负数，取极小侧的 0.05 分位）
        df['oi_drop_30d_95p'] = df['oi_change_pct_end'].rolling(window=window_30d, min_periods=288 * 3).quantile(0.05)
        df['oi_change_std_30d'] = df['oi_change_pct_end'].rolling(window=window_30d, min_periods=288 * 3).std()

        # 【核心修复】：保留时间空洞 (NaN)，不使用 dropna 摧毁物理时间连续性
        self.df = df.reset_index()
        missing_count = self.df['close'].isna().sum()
        print(f"[{self.symbol}] 特征生成完毕，总容量: {len(self.df)} 条K线，缺失数据洞: {missing_count} 个。")

    def run_backtest(self):
        print(f"[{self.symbol}] 启动状态机事件回测...")
        df = self.df
        n = len(df)

        i = 1  # 从 1 开始，预留前 1 根用于获取基准环境
        while i < n - 10:
            row = df.iloc[i]
            prev_row = df.iloc[i - 1]

            # 【新增防线】：如果遇到数据洞 (断网导致的时间断层保留的NaN)，直接跳过当前索引
            if pd.isna(row['close']) or pd.isna(prev_row['close']):
                i += 1
                continue

            # ==========================================
            # Gate A: 燃料门 (前置状态监控)
            # ==========================================
            # 【核心修复】：必须取崩塌前的稳态 (prev_row)，避免被崩塌中当根K线污染
            cond_a_funding = (prev_row['predicted_funding_rate'] > 0) and (
                        prev_row['predicted_funding_rate'] > prev_row['fr_90d_90p'])
            cond_a_oi = prev_row['oi_amount_end'] > prev_row['oi_20d_80p']
            cond_a_btc = prev_row['btc_30m_ret'] > -0.025  # BTC跌幅需较温和，规避系统性泥沙俱下

            if not (cond_a_funding and cond_a_oi and cond_a_btc):
                i += 1
                continue

            # ==========================================
            # Gate B: 级联确认 (寻找强平坍塌)
            # ==========================================
            gate_b_passed = False
            gate_b_end_idx = i

            # 级联起点基准：当前 K线 i 的 "期初" (即 i 发生前的稳态)
            base_price = row['open']
            base_oi = prev_row['oi_amount_end']  # 【核心修复】：取事件前一刻期末的持仓量作为基准
            base_cvd = prev_row['cvd']  # 级联发生前一刻的 CVD 水位
            pre_atr = prev_row['atr_14']

            # 探索连续 1 到 5 根的级联组合 (索引 j)
            for j in range(i, min(i + 5, n - 4)):
                cur_row = df.iloc[j]

                # 【新增防线】：连续窗口内遭遇断流数据洞，级联探索作废
                if pd.isna(cur_row['close']):
                    break

                # 【新增防线】：强制校验物理时间连续性，严禁跨小时甚至跨天的两根K线被拼接认定为连续级联
                expected_time = row['timestamp'] + pd.Timedelta(minutes=5 * (j - i))
                if cur_row['timestamp'] != expected_time:
                    break

                # B1: 价格跌幅 (从基准价到级联极低点)
                min_low_b = df.iloc[i:j + 1]['low'].min()
                price_drop_pct = (base_price - min_low_b) / base_price
                price_threshold = max(4 * pre_atr / base_price, 0.05)

                # B2: 物理燃料湮灭 (降幅 = 级联期末OI - 级联期初OI)
                oi_drop_pct = (cur_row['oi_amount_end'] - base_oi) / base_oi
                oi_threshold = min(-0.03, row['oi_drop_30d_95p'] / 100)

                # B3: 净抛压否决 (CVD_期末 - CVD_期初)
                cvd_delta = cur_row['cvd'] - base_cvd

                if (price_drop_pct > price_threshold) and (oi_drop_pct < oi_threshold) and (cvd_delta < 0):
                    gate_b_passed = True
                    gate_b_end_idx = j
                    break

            if not gate_b_passed:
                i += 1
                continue

            # ==========================================
            # Gate C: 耗竭确认 (等待余震平息)
            # ==========================================
            # 级联在 j 结束，严格观测随后的 j+1 和 j+2 根 K线
            c1_idx = gate_b_end_idx + 1
            c2_idx = gate_b_end_idx + 2

            row_c1 = df.iloc[c1_idx]
            row_c2 = df.iloc[c2_idx]

            # 【新增防线】：观察期遇空洞则直接流产
            if pd.isna(row_c1['close']) or pd.isna(row_c2['close']):
                i = c2_idx
                continue

            normal_oi_std = prev_row['oi_change_std_30d']  # 【核心修复】：取崩塌前稳态的波动率基准

            # C1: 账本企稳 (后续两根 K 线的内部异动平息至 1 倍标准差内)
            oi_stable = (abs(row_c1['oi_change_pct_end']) < normal_oi_std) and (
                    abs(row_c2['oi_change_pct_end']) < normal_oi_std)

            # C2: 价格拒绝 (拒绝创出 Gate B 砸出的新低)
            b_lowest = df.iloc[i:gate_b_end_idx + 1]['low'].min()
            c_lowest = min(row_c1['low'], row_c2['low'])
            no_new_low = c_lowest >= b_lowest

            # C3: 买盘承接形态 (确认蜡烛图收在自身上 40% 分位)
            c2_range = row_c2['high'] - row_c2['low']
            c2_upper_40 = row_c2['low'] + (c2_range * 0.6)
            rejection_close = row_c2['close'] >= c2_upper_40

            # 【核心补充】：方案规定的 C门累计 5% 绝对燃烧下限兜底
            final_oi_burn = (row_c2['oi_amount_end'] - base_oi) / base_oi
            depth_exhausted = final_oi_burn <= -0.05

            if not (oi_stable and no_new_low and rejection_close and depth_exhausted):
                i = c2_idx
                continue

            # ==========================================
            # 影子执行体系 (严格包含滑点与延时模拟)
            # ==========================================
            # 在 c2 结束的瞬间发出信号，订单会在下根 K线 (c2+1) 的期初开盘时成交
            entry_idx = c2_idx + 1
            entry_row = df.iloc[entry_idx]

            if pd.isna(entry_row['close']):
                i = entry_idx
                continue

            # Taker 滑点模型折损
            entry_price = entry_row['open'] * (1 + self.slip_and_fee)

            # 风控三线
            sl_price = b_lowest - (0.75 * pre_atr)
            b_highest = df.iloc[i:gate_b_end_idx + 1]['high'].max()
            b_drop_range = b_highest - b_lowest

            tp1_price = b_lowest + (0.382 * b_drop_range)
            tp2_price = b_lowest + (0.500 * b_drop_range)

            # 【核心修复】：基于物理时间防线计算超时锚点，抵抗数据缺失导致的时间无限期拉长
            entry_time = entry_row['timestamp']
            time_stop_time = entry_time + pd.Timedelta(hours=4)

            # 移交组合风控接管 (逐 K 线步进)
            trade_result = self.simulate_trade(entry_idx, entry_price, sl_price, tp1_price, tp2_price, time_stop_time)

            trade = {
                'entry_time': entry_time,
                'exit_time': trade_result['exit_time'],
                'gate_b_drop': price_drop_pct,
                'gate_b_oi_burn': final_oi_burn,
                'entry_price': entry_price,
                'exit_price': trade_result['exit_price'],
                'reason': trade_result['reason'],
                'pnl_pct': (trade_result['exit_price'] - entry_price) / entry_price
            }
            self.trades.append(trade)

            # 完成处决后，将指针推进至平仓点后，重启侦听
            i = trade_result['exit_idx'] + 1

    def simulate_trade(self, entry_idx, entry_price, sl, tp1, tp2, time_stop_time):
        """单笔刚性风控引擎 (合并持仓计算均价模型)"""
        tp1_hit = False
        df = self.df
        n = len(df)
        tp1_real_price = 0  # 缓存 TP1 真实扣费成交价

        for curr_idx in range(entry_idx, n):
            row = df.iloc[curr_idx]

            if pd.isna(row['close']):
                continue

            # 3. 时间维度枯竭 (优先拦截：L型死寂无条件清仓)
            if row['timestamp'] >= time_stop_time:
                exit_price = row['open'] * (1 - self.slip_and_fee)
                reason = 'Time_Stop_Half' if tp1_hit else 'Time_Stop_Full'
                final_exit = (tp1_real_price + exit_price) / 2 if tp1_hit else exit_price
                return {'exit_idx': curr_idx, 'exit_time': row['timestamp'], 'exit_price': final_exit, 'reason': reason}

            # 1. 结构性防守破位 (触发物理止损)
            if row['low'] <= sl:
                # 【核心修复】：真实跳空止损还原。如果开盘已跳空跌破止损，承受极端滑点，绝不产生虚假的高盈亏比幻觉
                actual_sl_price = min(sl, row['open'])
                exit_price = actual_sl_price * (1 - self.slip_and_fee)
                if tp1_hit:
                    return {'exit_idx': curr_idx, 'exit_time': row['timestamp'],
                            'exit_price': (tp1_real_price + exit_price) / 2, 'reason': 'TP1_then_SL'}
                return {'exit_idx': curr_idx, 'exit_time': row['timestamp'], 'exit_price': exit_price,
                        'reason': 'Stop_Loss'}

            # 2. 分批撤离逻辑
            if not tp1_hit and row['high'] >= tp1:
                tp1_hit = True
                tp1_real_price = tp1 * (1 - self.slip_and_fee)

            if tp1_hit and row['high'] >= tp2:
                exit_price = tp2 * (1 - self.slip_and_fee)
                return {'exit_idx': curr_idx, 'exit_time': row['timestamp'],
                        'exit_price': (tp1_real_price + exit_price) / 2, 'reason': 'Take_Profit_All'}

        # 数据尾部强平
        exit_row = df.iloc[-1]
        exit_price = exit_row['close'] * (1 - self.slip_and_fee)
        reason = 'End_of_Data'
        final_exit = (tp1_real_price + exit_price) / 2 if tp1_hit else exit_price
        return {'exit_idx': n - 1, 'exit_time': exit_row['timestamp'], 'exit_price': final_exit, 'reason': reason}

    def evaluate_performance(self):
        print("\n" + "=" * 60)
        print("💡 [LER 验证流水线 Step 1-2 报告]")
        print("=" * 60)

        if not self.trades:
            print("当前样本集无触发信号，底层防线成功拦截了所有假阳性事件。")
            return pd.DataFrame()

        res_df = pd.DataFrame(self.trades)
        total_trades = len(res_df)
        win_trades = len(res_df[res_df['pnl_pct'] > 0])
        win_rate = win_trades / total_trades if total_trades > 0 else 0

        avg_win = res_df[res_df['pnl_pct'] > 0]['pnl_pct'].mean() if win_trades > 0 else 0
        avg_loss = res_df[res_df['pnl_pct'] <= 0]['pnl_pct'].mean() if total_trades > win_trades else 0
        rr_ratio = abs(avg_win / avg_loss) if avg_loss != 0 else 0

        print(f"| 实战派发单量 : {total_trades} 笔")
        print(f"| 已实现胜率   : {win_rate * 100:.2f}% (预期域: 50%-56%)")
        print(f"| 已实现盈亏比 : {rr_ratio:.2f} (预期域: 1.2-1.5)")
        print(f"| 单笔净期望   : {res_df['pnl_pct'].mean() * 100:.3f}%")

        print("\n[平仓动因横向剖析]")
        print(res_df['reason'].value_counts())

        return res_df


# ============================================================================
# 执行入口与流水线编排
# ============================================================================
if __name__ == "__main__":
    DATA_DIR = './data'
    # 大盘 Beta 过滤器用的 BTC 1m 数据底座不变
    BTC_DATA = r"W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2025-01-01_merged.csv"

    # 使用 glob 匹配该目录下所有标准的 ler 特征宽表
    search_pattern = os.path.join(DATA_DIR, '*_ler_data.csv')
    target_files = glob.glob(search_pattern)

    if not target_files:
        print(f"❌ 目录 {DATA_DIR} 下未找到任何匹配的 *_ler_data.csv 文件，请检查数据抓取管线。")
        exit()

    print(f"\n{'=' * 80}")
    print(f"🚀 [系统启动] 发现 {len(target_files)} 个标的数据，开始执行 LER 全市场级联扫描...")
    print(f"{'=' * 80}")

    all_trades_list = []  # 用于收集所有标的的交易记录以生成汇总报告

    for file_path in target_files:
        # 从文件名逆向推导 Symbol (例如: ETH_USDT_USDT_5m_ler_data.csv -> ETH/USDT)
        filename = os.path.basename(file_path)
        parts = filename.split('_')

        # 兜底命名解析防呆
        if len(parts) >= 2:
            symbol = f"{parts[0]}/{parts[1]}"
        else:
            symbol = filename.split('.')[0]

        print(f"\n▶️ 正在处理: {symbol} | 文件: {filename}")

        # 实例化引擎，此处可统一切换双倍压力测试摩擦系数 0.003
        tester = LERBacktester(
            data_path=file_path,
            btc_data_path=BTC_DATA,
            symbol=symbol,
            slip_and_fee=0.0015
        )

        try:
            tester.load_and_prepare_data()
            tester.run_backtest()
            trade_history = tester.evaluate_performance()

            # 只有当该标的真实触发了交易，才进行独立落盘和汇总池追加
            if trade_history is not None and not trade_history.empty:
                # 注入标的名称以便于后期归因分析
                trade_history.insert(0, 'symbol', symbol)

                # 独立保存该标的的执行明细
                out_name = filename.replace('_ler_data.csv', '_verified_trades.csv')
                out_path = os.path.join(DATA_DIR, out_name)
                trade_history.to_csv(out_path, index=False)

                all_trades_list.append(trade_history)
                print(f"✅ [{symbol}] 交易明细已独立归档 ➜ {out_name}")
            else:
                print(f"➖ [{symbol}] 样本期内无交易信号，防线静默。")

        except Exception as e:
            # 捕获单体崩溃，隔离故障域，不影响下一个币种的测试
            print(f"❌ [{symbol}] 遭遇无法自愈的异常，已熔断跳过。报错详情: {e}")
            continue

    # ========================================================================
    # 全市场维度汇总报告 (Portfolio Level)
    # ========================================================================
    print("\n\n" + "=" * 80)
    print("🏆 [LER 组合风控级验证报告]")
    print("=" * 80)

    if all_trades_list:
        portfolio_df = pd.concat(all_trades_list, ignore_index=True)
        # 按时间进行全局排序，以模拟真实的资金流水
        portfolio_df.sort_values('entry_time', inplace=True)

        summary_path = os.path.join(DATA_DIR, 'PORTFOLIO_trade_history_summary.csv')
        portfolio_df.to_csv(summary_path, index=False)

        total_trades = len(portfolio_df)
        win_trades = len(portfolio_df[portfolio_df['pnl_pct'] > 0])
        win_rate = win_trades / total_trades if total_trades > 0 else 0
        avg_pnl = portfolio_df['pnl_pct'].mean()

        print(f"| 全市场扫描标的数 : {len(target_files)} 个")
        print(f"| 实际产生交易标的 : {len(portfolio_df['symbol'].unique())} 个")
        print(f"| 全局组合总派发量 : {total_trades} 笔")
        print(f"| 全局综合胜率     : {win_rate * 100:.2f}%")
        print(f"| 全局单笔均净期望 : {avg_pnl * 100:.3f}%")
        print(f"✅ 组合汇总账本已安全落盘 ➜ {summary_path}")
    else:
        print("⚠️ 整个市场样本中，没有任何标的触发有效的 Gate A+B+C 耗竭信号。")