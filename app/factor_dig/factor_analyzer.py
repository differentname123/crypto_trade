# -*- coding: utf-8 -*-
"""
================================================================================
 组合有效性深度分析器 (Combo Effectiveness Deep Analyzer) [纯 Parquet 极速版]
--------------------------------------------------------------------------------
 · 彻底废弃 trades_ALL.csv，强制读取 trades_ALL.parquet，杜绝内存溢出
 · 极速计算持仓时间，享受二进制列式存储带来的毫秒级解析体验
 · 全局宏观扫雷 + 指定组合深度体检
================================================================================
"""
import os
import math
import logging
import warnings

import numpy as np
import pandas as pd
from scipy import stats

warnings.filterwarnings("ignore")


# ======================================================================
# 日志配置
# ======================================================================
def setup_logger(out_dir):
    logger = logging.getLogger('ComboAnalyzer')
    logger.setLevel(logging.INFO)

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-7s | %(message)s',
        datefmt='%H:%M:%S'
    )

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    log_path = os.path.join(out_dir, 'combo_analysis.log')
    fh = logging.FileHandler(log_path, encoding='utf-8', mode='w')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger, log_path


# ======================================================================
# 核心分析器
# ======================================================================
class ComboAnalyzer:
    def __init__(self, out_dir):
        self.out_dir = out_dir
        self.logger, self.log_path = setup_logger(out_dir)

        self.trades = None
        self.timeseries = None
        self.profile = None
        self.pairs_all = None
        self.cross_summary = None

        self._load_data()

    # ------------------------------------------------------------------
    # 数据加载 (纯 Parquet 引擎)
    # ------------------------------------------------------------------
    def _load_data(self):
        self.logger.info("=" * 90)
        self.logger.info(f"加载数据目录: {os.path.abspath(self.out_dir)}")
        self.logger.info("=" * 90)

        # 1. 强制读取 Parquet 流水表 (摒弃 CSV)
        trades_pq = os.path.join(self.out_dir, 'trades_ALL.parquet')

        if os.path.exists(trades_pq):
            self.logger.info("  检测到 trades_ALL.parquet，正在启用极速加载模式...")
            self.trades = pd.read_parquet(trades_pq)
            # 主动将字符串列转为 categorical，确保内存占用锁定在最低
            self.trades['combo_id'] = self.trades['combo_id'].astype('category')
            self.trades['coin'] = self.trades['coin'].astype('category')
            self.logger.info(f"  已加载 trades_ALL.parquet | {len(self.trades):>10,} 行")
        else:
            self.logger.warning("  未找到 trades_ALL.parquet 流水文件")

        # 2. 加载其他聚合表 (由于体积极小，继续保留轻量级 CSV 格式)
        files_map = {
            'timeseries': 'combo_timeseries_ALL.csv',
            'profile': 'Combo_Profile_ALL.csv',
            'pairs_all': 'pairs_ALL.csv',
            'cross_summary': 'pairs_CROSS_COIN_SUMMARY.csv',
        }

        for attr, fname in files_map.items():
            fpath = os.path.join(self.out_dir, fname)
            if os.path.exists(fpath):
                if attr == 'timeseries':
                    df = pd.read_csv(fpath, dtype={'combo_id': 'category'})
                else:
                    df = pd.read_csv(fpath)
                setattr(self, attr, df)
                self.logger.info(f"  已加载 {fname:<32} | {len(df):>10,} 行")
            else:
                self.logger.warning(f"  未找到 {fname}")

        # 3. 处理时间戳与指标计算
        if self.trades is not None and not self.trades.empty:
            self.logger.info("  正在预处理流水指标...")

            # 【核心优化】Parquet 天生保留 datetime64[ns] 格式，直接进行无缝数学运算
            self.trades['hold_hours'] = (self.trades['exit_time'] - self.trades[
                'entry_time']).dt.total_seconds() / 3600.0

            # 由于底层用了 float32 降维，这里强转 float 防止高精度计算溢出
            self.trades['alpha'] = self.trades['net_return'].astype(float) - self.trades['benchmark_return'].astype(
                float)

        if self.timeseries is not None and not self.timeseries.empty:
            # timeseries 依然是 csv，因此仍需解析一下日期
            self.timeseries['date'] = pd.to_datetime(self.timeseries['date'], format='ISO8601', exact=False)

        self.logger.info("-" * 90)

    # ------------------------------------------------------------------
    # 工具方法
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_stat(func, arr, default=np.nan):
        try:
            if len(arr) == 0:
                return default
            return func(arr)
        except Exception:
            return default

    def get_combo_ids(self, top_n=None, sort_by='deflated_sharpe'):
        if self.profile is None or self.profile.empty:
            return []
        df = self.profile.copy()
        if sort_by in df.columns:
            df = df.sort_values(sort_by, ascending=False)
        if top_n:
            df = df.head(top_n)
        return df['combo_id'].tolist()

    # ==================================================================
    # 全局宏观扫雷
    # ==================================================================
    def run_global_screening(self, top_n=20):
        self.logger.info("\n" + "#" * 90)
        self.logger.info(f"全局宏观扫雷 (Top {top_n} by DSR)")
        self.logger.info("#" * 90)

        if self.profile is None or self.profile.empty:
            self.logger.warning("无 Profile 数据，跳过")
            return

        total_combos = len(self.profile)
        self.logger.info(f"\n  总测试组合数量 : {total_combos:>10,}")

        # True N 过滤
        if 'true_n_trades' in self.profile.columns:
            tn_valid = self.profile['true_n_trades'].dropna()
            tn_gt30 = (tn_valid > 30).sum()
            self.logger.info(f"  True N > 30 的组合 : {tn_gt30:>10,} ({tn_gt30 / len(tn_valid) * 100:.2f}%)")
            tn_lt30 = (tn_valid < 30).sum()
            if tn_lt30 > 0:
                self.logger.info(f"  True N < 30 的组合 : {tn_lt30:>10,} (统计失效，直接淘汰)")

        # DSR 分布
        if 'deflated_sharpe' in self.profile.columns:
            dsr_valid = self.profile['deflated_sharpe'].dropna()
            if len(dsr_valid) > 0:
                dsr_gt90 = (dsr_valid > 0.90).sum()
                dsr_lt01 = (dsr_valid < 0.01).sum()
                self.logger.info(f"  DSR > 0.90 的组合 : {dsr_gt90:>10,} ({dsr_gt90 / len(dsr_valid) * 100:.2f}%)")
                self.logger.info(f"  DSR < 0.01 的组合 : {dsr_lt01:>10,} ({dsr_lt01 / len(dsr_valid) * 100:.2f}%)")
                self.logger.info(f"  DSR 中位数       : {dsr_valid.median():>10.4f}")

                if dsr_gt90 == 0:
                    self.logger.info("")
                    self.logger.info("  [判定] 全局策略池已全军覆没。")
                    self.logger.info("         在考虑多重检验惩罚后，没有任何组合的 DSR 超过 0.90。")
                    self.logger.info("         当前特征池大概率仅为随机噪音，建议彻底更换因子方向。")

        # Top N 看板
        df_top = self.profile.copy()
        if 'deflated_sharpe' in df_top.columns:
            df_top = df_top.sort_values('deflated_sharpe', ascending=False)
        df_top = df_top.head(top_n)

        self.logger.info(f"\n{'#':>3} | {'Combo ID':<55} | {'True N':>8} | {'OOS SR':>8} | {'DSR':>8} | 判定")
        self.logger.info("-" * 110)

        for i, (_, row) in enumerate(df_top.iterrows(), 1):
            cid = row['combo_id']
            true_n = row.get('true_n_trades', np.nan)
            oos_sr = row.get('is_oos_sharpe', np.nan)
            dsr = row.get('deflated_sharpe', np.nan)

            judgment = ""
            if pd.notna(dsr):
                if dsr < 0.01:
                    judgment = "噪音"
                elif dsr < 0.05:
                    judgment = "存疑"
                elif dsr > 0.90:
                    judgment = "显著"
                else:
                    judgment = "观察"

            if pd.notna(true_n) and true_n < 30:
                judgment += " (样本不足)"

            dsr_str = f"{dsr:.4f}" if pd.notna(dsr) else "N/A"
            oos_str = f"{oos_sr:.4f}" if pd.notna(oos_sr) else "N/A"
            tn_str = f"{true_n:.1f}" if pd.notna(true_n) else "N/A"

            self.logger.info(f"{i:>3} | {cid:<55} | {tn_str:>8} | {oos_str:>8} | {dsr_str:>8} | {judgment}")

        if self.trades is not None and not self.trades.empty:
            self.logger.info(f"\n  全局总交易笔数   : {len(self.trades):>10,}")
            self.logger.info(f"  全局覆盖币种数   : {self.trades['coin'].nunique():>10}")
            self.logger.info(f"  全局独立组合数   : {self.trades['combo_id'].nunique():>10,}")

    # ==================================================================
    # 指定组合深度体检
    # ==================================================================
    def analyze_combo(self, combo_id):
        self.logger.info("\n" + "=" * 90)
        self.logger.info(f"深度分析组合: {combo_id}")
        self.logger.info("=" * 90)

        result = {'combo_id': combo_id}
        red_flags = []
        green_flags = []

        if self.profile is not None and not self.profile.empty:
            prof = self.profile[self.profile['combo_id'] == combo_id]
            if not prof.empty:
                row = prof.iloc[0]
                self.logger.info(f"\n[1/5] 基础档案 (Profile)")
                self.logger.info(f"  总测试次数 (Total Trials) : {int(row.get('total_trials', 0)):>10,}")
                self.logger.info(f"  原始总交易数              : {int(row.get('total_trades', 0)):>10,}")
                self.logger.info(f"  有效独立样本 (True N)     : {row.get('true_n_trades', 0):>10.1f}")
                self.logger.info(f"  样本外夏普 (OOS Sharpe)   : {row.get('is_oos_sharpe', 0):>10.4f}")
                self.logger.info(f"  缩水平夏普 (DSR)          : {row.get('deflated_sharpe', 0):>10.4f}")

                dsr = row.get('deflated_sharpe', np.nan)
                true_n = row.get('true_n_trades', 0)

                if pd.notna(true_n) and true_n < 30:
                    self.logger.info(f"  [判定] True N < 30，有效独立样本过少，所有统计结论不可靠")
                    red_flags.append("True N < 30 (独立样本不足，统计失效)")
                elif pd.notna(true_n) and true_n >= 30:
                    green_flags.append(f"True N = {true_n:.1f} (独立样本充足)")

                if pd.notna(dsr):
                    if dsr < 0.01:
                        self.logger.info(f"  [判定] DSR < 0.01，极大概率是数据挖掘噪音，建议直接淘汰")
                        red_flags.append("DSR < 0.01 (多重检验不显著)")
                    elif dsr < 0.05:
                        self.logger.info(f"  [警示] DSR < 0.05，显著性存疑")
                    elif dsr > 0.90:
                        self.logger.info(f"  [判定] DSR > 0.90，在考虑多重检验后依然高度显著")
                        green_flags.append(f"DSR = {dsr:.4f} (高度显著)")

                result['profile'] = row.to_dict()
            else:
                self.logger.warning(f"  在 Profile 表中未找到该组合")

        if self.trades is not None and not self.trades.empty:
            t = self.trades[self.trades['combo_id'] == combo_id]
            if not t.empty:
                self.logger.info(f"\n[2/5] 逐笔交易分布 (共 {len(t)} 笔)")

                net = t['net_return'].values.astype(float)
                bench = t['benchmark_return'].values.astype(float)
                alpha = t['alpha'].values.astype(float)
                conc = t['concurrent_signals'].values.astype(float)
                hold = t['hold_hours'].values.astype(float)

                self.logger.info(f"\n  --- 净收益 (Net Return) ---")
                self.logger.info(f"    均值       : {np.mean(net) * 100:>9.4f}%")
                self.logger.info(f"    中位数     : {np.median(net) * 100:>9.4f}%")
                self.logger.info(f"    标准差     : {np.std(net, ddof=1) * 100:>9.4f}%")

                skew_val = self._safe_stat(stats.skew, net, 0.0)
                kurt_val = self._safe_stat(stats.kurtosis, net, 0.0)
                self.logger.info(f"    偏度       : {skew_val:>9.4f}")
                self.logger.info(f"    峰度       : {kurt_val:>9.4f}")
                self.logger.info(f"    最大盈利   : {np.max(net) * 100:>9.4f}%")
                self.logger.info(f"    最大亏损   : {np.min(net) * 100:>9.4f}%")
                self.logger.info(f"    胜率       : {(net > 0).mean() * 100:>9.2f}%")
                self.logger.info(f"    累计收益   : {np.sum(net) * 100:>9.2f}%")

                self.logger.info(f"\n    分位数分布:")
                for p in [5, 10, 25, 50, 75, 90, 95]:
                    self.logger.info(f"      P{p:<3}: {np.percentile(net, p) * 100:>9.4f}%")

                trimmed_mean = np.nan
                if len(net) >= 20:
                    trimmed = stats.trimboth(net, 0.05)
                    trimmed_mean = np.mean(trimmed)
                    self.logger.info(f"\n    截尾均值(去前后5%极值): {trimmed_mean * 100:>9.4f}%")

                p95 = np.percentile(net, 95)
                p5 = np.percentile(net, 5)
                tail_ratio = p95 / abs(p5) if abs(p5) > 1e-9 else np.nan
                if pd.notna(tail_ratio):
                    self.logger.info(f"    尾部比率(95%/|5%|): {tail_ratio:>9.4f}")

                wins = net[net > 0]
                losses = net[net < 0]
                if len(losses) > 0 and len(wins) > 0:
                    profit_factor = wins.sum() / abs(losses.sum())
                    avg_win = wins.mean()
                    avg_loss = abs(losses.mean())
                    self.logger.info(f"\n    盈利因子   : {profit_factor:>9.4f}")
                    self.logger.info(f"    平均盈利   : {avg_win * 100:>9.4f}%")
                    self.logger.info(f"    平均亏损   : {avg_loss * 100:>9.4f}%")
                    self.logger.info(f"    盈亏比     : {avg_win / avg_loss:>9.4f}")

                self.logger.info(f"\n    最佳5笔: {np.sort(net)[-5:][::-1] * 100}")
                self.logger.info(f"    最差5笔: {np.sort(net)[:5] * 100}")

                if len(wins) > 0:
                    top5_sum = np.sort(net)[-5:].sum()
                    total_profit = wins.sum()
                    top5_pct = top5_sum / total_profit * 100 if total_profit > 0 else 0
                    self.logger.info(f"\n    Top5笔利润占总盈利比: {top5_pct:>9.2f}%")

                    if len(net) > 50 and top5_pct > 50:
                        self.logger.info(f"  [判定] 极值依赖严重！利润极度集中于极少数历史偶然事件，实盘容错率极低")
                        red_flags.append(f"Top5笔利润占比 {top5_pct:.1f}% (极值依赖)")

                if pd.notna(trimmed_mean) and trimmed_mean < 0:
                    self.logger.info(f"  [判定] 截尾均值 < 0，去掉极值后策略期望为负，核心逻辑不赚钱")
                    red_flags.append("截尾均值 < 0 (核心逻辑不赚钱)")

                self.logger.info(f"\n  --- 基准收益 (Benchmark) ---")
                self.logger.info(f"    持仓期基准均值 : {np.mean(bench) * 100:>9.4f}%")
                self.logger.info(f"    持仓期基准累计 : {np.sum(bench) * 100:>9.2f}%")

                self.logger.info(f"\n  --- 纯 Alpha (Net - Benchmark) ---")
                self.logger.info(f"    Alpha 均值     : {np.mean(alpha) * 100:>9.4f}%")
                self.logger.info(f"    Alpha 累计     : {np.sum(alpha) * 100:>9.2f}%")
                self.logger.info(f"    Alpha 胜率     : {(alpha > 0).mean() * 100:>9.2f}%")

                if len(alpha) > 2:
                    alpha_std = np.std(alpha, ddof=1)
                    if alpha_std > 0:
                        alpha_t = np.mean(alpha) / (alpha_std / np.sqrt(len(alpha)))
                        alpha_p = 2 * (1 - stats.t.cdf(abs(alpha_t), len(alpha) - 1))
                        self.logger.info(f"    Alpha T统计量  : {alpha_t:>9.4f}")
                        self.logger.info(f"    Alpha P值      : {alpha_p:>9.4f}")

                if len(net) >= 10:
                    self.logger.info(f"\n  --- CAPM 回归: R_combo = alpha + beta * R_benchmark + epsilon ---")
                    try:
                        X = np.column_stack([np.ones(len(bench)), bench])
                        coeffs = np.linalg.lstsq(X, net, rcond=None)[0]
                        alpha_reg, beta_reg = coeffs[0], coeffs[1]

                        y_pred = X @ coeffs
                        ss_res = np.sum((net - y_pred) ** 2)
                        ss_tot = np.sum((net - np.mean(net)) ** 2)
                        r2 = 1 - ss_res / ss_tot if ss_tot > 0 else 0

                        n = len(net)
                        mse = ss_res / (n - 2) if n > 2 else 0
                        denom = np.sum((bench - np.mean(bench)) ** 2)
                        var_alpha = mse * (1 / n + np.mean(bench) ** 2 / denom) if denom > 0 else 0
                        se_alpha = np.sqrt(var_alpha) if var_alpha > 0 else np.nan
                        t_alpha = alpha_reg / se_alpha if pd.notna(se_alpha) and se_alpha > 0 else np.nan

                        self.logger.info(f"    Alpha (截距) : {alpha_reg * 100:>9.4f}%")
                        self.logger.info(f"    Beta  (斜率) : {beta_reg:>9.4f}")
                        self.logger.info(f"    R squared    : {r2:>9.4f}")
                        if pd.notna(t_alpha):
                            self.logger.info(f"    Alpha T值    : {t_alpha:>9.4f}")

                        if pd.notna(t_alpha) and t_alpha < 1.65 and r2 > 0.70:
                            self.logger.info(f"  [判定] 伪 Alpha 预警！超额收益不显著且与基准高度相关，本质是 Beta 暴露")
                            red_flags.append(f"Alpha T={t_alpha:.2f} 且 R2={r2:.2f} (伪Alpha)")
                        elif pd.notna(t_alpha) and t_alpha > 2.33:
                            green_flags.append(f"Alpha T={t_alpha:.2f} (纯Alpha显著)")
                    except Exception as e:
                        self.logger.warning(f"    回归计算失败: {e}")

                self.logger.info(f"\n  --- 并发与独立性 ---")
                self.logger.info(f"    平均并发信号数 : {np.mean(conc):>9.2f}")
                self.logger.info(f"    最大并发信号数 : {np.max(conc):>9.0f}")
                self.logger.info(f"    并发>3的占比   : {(conc > 3).mean() * 100:>9.2f}%")
                self.logger.info(f"    并发>5的占比   : {(conc > 5).mean() * 100:>9.2f}%")

                self.logger.info(f"\n  --- 持仓时间 ---")
                self.logger.info(f"    平均持仓       : {np.mean(hold):>9.2f} 小时")
                self.logger.info(f"    中位持仓       : {np.median(hold):>9.2f} 小时")
                self.logger.info(f"    最长持仓       : {np.max(hold):>9.2f} 小时")

                coin_counts = t['coin'].value_counts()
                n_coins = len(coin_counts)
                coin_profit = t.groupby('coin')['net_return'].sum()
                n_pos_coins = (coin_profit > 0).sum()

                self.logger.info(f"\n  --- 币种覆盖 ---")
                self.logger.info(f"    覆盖币种数     : {n_coins:>9}")
                self.logger.info(f"    盈利币种数     : {n_pos_coins:>9}")
                if n_coins > 0:
                    self.logger.info(f"    币种盈利占比   : {n_pos_coins / n_coins * 100:>9.2f}%")

                if n_coins > 0:
                    pos_rate = n_pos_coins / n_coins
                    if pos_rate < 0.30:
                        self.logger.info(f"  [判定] 盈利币种占比 < 30%，缺乏跨币种泛化能力")
                        red_flags.append(f"盈利币种占比 {pos_rate * 100:.1f}% (无泛化能力)")
                    elif pos_rate > 0.60:
                        green_flags.append(f"盈利币种占比 {pos_rate * 100:.1f}% (泛化能力强)")

                if n_coins > 1:
                    top_coin = coin_profit.index[0]
                    top_coin_profit = coin_profit.iloc[0]
                    total_profit = coin_profit[coin_profit > 0].sum()
                    if total_profit > 0:
                        top_coin_pct = top_coin_profit / total_profit * 100
                        self.logger.info(f"    Top1币种       : {top_coin} ({top_coin_pct:.1f}%)")
                        if top_coin_pct > 60:
                            self.logger.info(f"  [判定] Top1币种利润贡献 > 60%，策略退化为单一标的专属定时器")
                            red_flags.append(f"Top1币种利润贡献 {top_coin_pct:.1f}% (单币过拟合)")

                result['trades'] = {
                    'n_trades': len(t),
                    'mean_net': np.mean(net),
                    'median_net': np.median(net),
                    'std_net': np.std(net),
                    'skew': skew_val,
                    'kurtosis': kurt_val,
                    'mean_alpha': np.mean(alpha),
                    'sum_alpha': np.sum(alpha),
                    'trimmed_mean': trimmed_mean,
                }

        if self.timeseries is not None and not self.timeseries.empty:
            ts = self.timeseries[self.timeseries['combo_id'] == combo_id].copy()
            if not ts.empty:
                ts = ts.sort_values('date').reset_index(drop=True)
                self.logger.info(f"\n[3/5] 时序资金曲线 (共 {len(ts)} 个交易日)")

                daily_ret = ts['daily_return'].values.astype(float)
                nav = ts['daily_nav'].values.astype(float)

                self.logger.info(f"\n  --- 日度收益 ---")
                self.logger.info(f"    日均收益       : {np.mean(daily_ret) * 100:>9.4f}%")
                self.logger.info(f"    日收益标准差   : {np.std(daily_ret, ddof=1) * 100:>9.4f}%")
                self.logger.info(f"    盈利天数占比   : {(daily_ret > 0).mean() * 100:>9.2f}%")

                std_d = np.std(daily_ret, ddof=1)
                sharpe_d = np.mean(daily_ret) / std_d * np.sqrt(365) if std_d > 0 else np.nan
                if pd.notna(sharpe_d):
                    self.logger.info(f"    年化夏普(日度) : {sharpe_d:>9.4f}")

                peak = np.maximum.accumulate(nav)
                dd = (peak - nav) / np.where(peak > 0, peak, 1)
                max_dd = np.max(dd)
                self.logger.info(f"    最大回撤       : {max_dd * 100:>9.2f}%")

                in_dd = dd > 0.001
                if in_dd.any():
                    groups = (~in_dd).cumsum()
                    dd_durations = pd.Series(in_dd).groupby(groups).sum()
                    max_dd_dur = dd_durations.max()
                    self.logger.info(f"    最长回撤持续   : {max_dd_dur:>9.0f} 天")

                downside = daily_ret[daily_ret < 0]
                if len(downside) > 0:
                    down_std = np.std(downside, ddof=1)
                    sortino = np.mean(daily_ret) / down_std * np.sqrt(365) if down_std > 0 else np.nan
                    if pd.notna(sortino):
                        self.logger.info(f"    Sortino比率    : {sortino:>9.4f}")

                if max_dd > 0 and len(ts) > 1:
                    total_ret = nav[-1] / nav[0] - 1 if nav[0] != 0 else 0
                    years = len(ts) / 365.0
                    ann_ret = (1 + total_ret) ** (1 / years) - 1 if years > 0 else 0
                    calmar = ann_ret / max_dd
                    self.logger.info(f"    Calmar比率     : {calmar:>9.4f}")

                if len(daily_ret) > 20:
                    s = pd.Series(daily_ret)
                    acf1 = s.autocorr(lag=1)
                    acf5 = s.autocorr(lag=5)
                    self.logger.info(f"\n    自相关(Lag=1)  : {acf1:>9.4f}")
                    self.logger.info(f"    自相关(Lag=5)  : {acf5:>9.4f}")

                ts['month'] = ts['date'].dt.to_period('M')
                monthly = ts.groupby('month')['daily_return'].sum()
                pos_months = (monthly > 0).sum()
                total_months = len(monthly)

                self.logger.info(f"\n  --- 月度一致性 ---")
                self.logger.info(f"    总月数         : {total_months:>9}")
                self.logger.info(f"    盈利月数       : {pos_months:>9}")
                if total_months > 0:
                    monthly_wr = pos_months / total_months
                    self.logger.info(f"    月度胜率       : {monthly_wr * 100:>9.2f}%")
                    if total_months >= 6 and monthly_wr < 0.30:
                        self.logger.info(f"  [判定] 月度胜率 < 30%，收益可能由极少数月份贡献，缺乏时间稳健性")
                        red_flags.append(f"月度胜率 {monthly_wr * 100:.1f}% (周期过拟合)")
                    elif monthly_wr > 0.60:
                        green_flags.append(f"月度胜率 {monthly_wr * 100:.1f}% (时序稳健)")

                if len(monthly) > 0:
                    self.logger.info(f"    最佳月         : {monthly.max() * 100:>9.2f}%")
                    self.logger.info(f"    最差月         : {monthly.min() * 100:>9.2f}%")

                ts['quarter'] = ts['date'].dt.to_period('Q')
                quarterly = ts.groupby('quarter')['daily_return'].sum()
                pos_q = (quarterly > 0).sum()
                self.logger.info(f"\n  --- 季度一致性 ---")
                self.logger.info(f"    总季度数       : {len(quarterly):>9}")
                self.logger.info(f"    盈利季度数     : {pos_q:>9}")

                result['timeseries'] = {
                    'n_days': len(ts),
                    'daily_sharpe': sharpe_d,
                    'max_dd': max_dd,
                    'monthly_win_rate': monthly_wr if total_months > 0 else np.nan,
                }

        if self.cross_summary is not None and not self.cross_summary.empty:
            parts = combo_id.split('|')
            if len(parts) == 2:
                entry, exit_f = parts
                cs = self.cross_summary[
                    (self.cross_summary['entry_factor'] == entry) &
                    (self.cross_summary['exit_factor'] == exit_f)
                    ]
                if not cs.empty:
                    row = cs.iloc[0]
                    self.logger.info(f"\n[4/5] 跨币种稳健性")
                    self.logger.info(f"  覆盖币种数       : {int(row.get('n_coins', 0)):>9}")
                    self.logger.info(f"  跨币总交易数     : {int(row.get('total_trades', 0)):>9,}")
                    self.logger.info(f"  盈利币种占比     : {row.get('coin_positive_rate', 0) * 100:>9.2f}%")
                    self.logger.info(f"  跨币平均均笔收益 : {row.get('mean_avg_ret', 0):>9.4f}%")
                    self.logger.info(f"  跨币平均胜率     : {row.get('mean_win_rate', 0):>9.2f}%")
                    self.logger.info(f"  跨币总收益       : {row.get('sum_ret_all', 0):>9.2f}%")
                    self.logger.info(f"  样本外跨币总收益 : {row.get('oos_sum_all', 0):>9.2f}%")
                    self.logger.info(f"  综合Score        : {row.get('score', 0):>9.4f}")

                    result['cross_coin'] = row.to_dict()

        self.logger.info(f"\n[5/5] 综合诊断")

        if red_flags:
            self.logger.info(f"  红旗 (风险点):")
            for f in red_flags:
                self.logger.info(f"     · {f}")
        else:
            self.logger.info(f"  红旗: 无")

        if green_flags:
            self.logger.info(f"  绿旗 (优势点):")
            for f in green_flags:
                self.logger.info(f"     · {f}")

        self.logger.info("\n" + "-" * 90)
        self.logger.info(f"分析完成: {combo_id}")
        self.logger.info("-" * 90)

        result['red_flags'] = red_flags
        result['green_flags'] = green_flags

        return result


# ======================================================================
# 使用方式：直接修改下面的路径，然后运行本文件即可
# ======================================================================
if __name__ == '__main__':

    # ========== 请修改这里 ==========
    OUT_DIR = './factor_out_60m'  # 自动对应新引擎的输出目录
    TARGET_COMBO = None  # 指定要深度分析的组合ID，如 "ENTRY_A|EXIT_B"
    TOP_N = 20  # 全局筛选显示 Top N
    ANALYZE_ALL_TOP = False  # 是否对 Top N 逐个深度分析
    # ================================

    analyzer = ComboAnalyzer(OUT_DIR)

    # 1. 全局宏观扫雷
    analyzer.run_global_screening(top_n=TOP_N)

    # 2. 指定组合深度分析
    if TARGET_COMBO:
        analyzer.analyze_combo(TARGET_COMBO)

    # 3. Top N 逐个深度分析
    if ANALYZE_ALL_TOP:
        top_ids = analyzer.get_combo_ids(top_n=TOP_N, sort_by='deflated_sharpe')
        for cid in top_ids:
            analyzer.analyze_combo(cid)

    print(f"\n日志已保存至: {os.path.abspath(analyzer.log_path)}")