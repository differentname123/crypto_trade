# -*- coding: utf-8 -*-
"""
================================================================================
 OccamVol  —  日频已实现波动率预测（生产版）
================================================================================
 已验证的最优配置（勿改动）:
   目标      : 次日 RV（5min 收益平方和，UTC 自然日）
   特征      : log(RV_1d) / log(RV_7d均值) / log(RV_30d均值) [+ log(期权隐含方差)]
   模型      : 1095 日滚动窗 OLS on log-RV，QLIKE 最优乘性缩放反变换
   频率      : 5min（1min 无额外收益，已证伪；15min 同样可用）
 无未来函数 : winsorize 用 expanding+shift(1)；训练窗严格 [t-1095, t-1]
 输出       : 逐日样本外预测 + 最后一行为"明天"的实盘预测
================================================================================
"""
import numpy as np
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

FLOOR = 1e-10
WINDOW = 1095          # 滚动训练窗（天）
TIMEFRAME = '5min'     # RV 采样频率


# ==============================================================================
# 内部工具
# ==============================================================================
def _expanding_floor(s, q=0.005, min_periods=200):
    """扩展窗分位下限并 shift(1)：t 时刻的截断阈值只用 t-1 及以前的数据。"""
    f = s.expanding(min_periods=min_periods).quantile(q).shift(1)
    return f.fillna(FLOOR).clip(lower=FLOOR)


def _load_rv(kline_csv, timeframe=TIMEFRAME, min_complete=0.95):
    """从 1m K 线聚合日频 RV。断线不产生假收益；单日完整率 <95% 视为无效。"""
    df = pd.read_csv(kline_csv, usecols=['open_time', 'close'])
    df['dt'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    df = (df.dropna(subset=['dt', 'close']).sort_values('dt')
            .drop_duplicates('dt', keep='last').set_index('dt'))

    close = df['close'].resample(timeframe).last()
    r = np.log(close).diff().where(close.notna() & close.shift(1).notna())

    exp_bars = int(round(86400 / pd.Timedelta(timeframe).total_seconds()))
    cnt = r.resample('1D').count()
    rv = r.pow(2).resample('1D').sum().where(cnt >= int(exp_bars * min_complete))

    rv.index = rv.index.tz_localize(None)
    rv = rv.asfreq('D').rename('RV')
    rv.index.name = 'date'
    n_bad = int(cnt.notna().sum() - (cnt >= int(exp_bars * min_complete)).sum())
    if n_bad:
        print(f"  [数据质检] {n_bad} 天完整率不足，已剔除")
    return rv


def _load_iv(dvol_csv):
    d = pd.read_csv(dvol_csv)
    d['date'] = pd.to_datetime(d['date'])
    s = (d[['date', 'Implied_Variance_It']].dropna()
         .drop_duplicates('date', keep='last').set_index('date')
         ['Implied_Variance_It'].sort_index())
    s.index.name = 'date'
    return s


def _engine(rv, iv=None, window=WINDOW):
    """
    滚动 OLS 引擎。返回含逐日样本外预测的 DataFrame，
    最后一行 Actual_RV 为 NaN，即"明天"的实盘预测。
    """
    rv = rv.sort_index().asfreq('D')
    rv = rv.clip(lower=_expanding_floor(rv))

    d = pd.DataFrame(index=rv.index)
    d['RV_today'] = rv
    d['log_D'] = np.log(rv)
    d['log_W'] = np.log(rv.rolling(7,  min_periods=7).mean())
    d['log_M'] = np.log(rv.rolling(30, min_periods=30).mean())
    d['Target'] = rv.shift(-1)                       # 预测对象：次日 RV
    d['Bench_RW'] = rv                               # 基准1：昨天=今天
    d['Bench_EWMA'] = np.exp(np.log(rv).ewm(alpha=0.10, adjust=False).mean())  # 基准2

    cols = ['log_D', 'log_W', 'log_M']
    if iv is not None:
        s = iv.reindex(d.index)
        s = s.clip(lower=_expanding_floor(s))
        d['log_I'] = np.log(s)
        cols.append('log_I')

    d = d.dropna(subset=cols + ['RV_today'])         # 保留 Target 为 NaN 的末行
    n = len(d)
    if n <= window:
        raise ValueError(f"有效天数 {n} 不足训练窗 {window}，请缩短 window 或补数据")

    X = np.column_stack([np.ones(n)] + [d[c].values for c in cols])
    y = np.log(d['Target'].values)                   # 对数空间回归
    lvl = d['Target'].values

    rows = []
    for t in range(window, n):
        sl = slice(t - window, t)                    # 训练集：t-1095 .. t-1（目标均已实现）
        Xtr, ytr, ltr = X[sl], y[sl], lvl[sl]
        m = np.isfinite(ytr)
        if m.sum() < window * 0.8:
            continue
        beta, *_ = np.linalg.lstsq(Xtr[m], ytr[m], rcond=None)
        fit = Xtr[m] @ beta
        c = float(np.mean(ltr[m] / np.exp(fit)))     # QLIKE 下最优乘性缩放（无偏化）
        pred = float(np.exp(X[t] @ beta) * c)
        rows.append((d.index[t] + pd.Timedelta(days=1), max(pred, FLOOR),
                     lvl[t], d['RV_today'].iloc[t],
                     d['Bench_RW'].iloc[t], d['Bench_EWMA'].iloc[t]))

    out = pd.DataFrame(rows, columns=['Forecast_Date', 'Pred_RV', 'Actual_RV',
                                      'RV_today', 'Bench_RW', 'Bench_EWMA'])
    for c in ['Pred_RV', 'Actual_RV', 'RV_today', 'Bench_RW', 'Bench_EWMA']:
        out[c.replace('_RV', '') + '_AnnVol%' if c.endswith('_RV') else c + '_AnnVol%'] = \
            np.sqrt(out[c] * 365) * 100
    return out


# ==============================================================================
# 函数 1 —— 无期权数据（纯 K 线）
# ==============================================================================
def forecast_rv(kline_csv, timeframe=TIMEFRAME, window=WINDOW):
    """
    纯 K 线波动率预测（log-HAR）。适用于无期权数据的品种。

    返回 DataFrame，每行 = 一次样本外预测；最后一行 Actual_RV=NaN 即明天的预测。
    """
    rv = _load_rv(kline_csv, timeframe)
    return _engine(rv, None, window)


# ==============================================================================
# 函数 2 —— 有期权数据（K 线 + DVOL 隐含方差）
# ==============================================================================
def forecast_rv_with_iv(kline_csv, dvol_csv, timeframe=TIMEFRAME, window=WINDOW):
    """
    双源波动率预测（log-HAR + 期权隐含方差）。BTC 上比纯 K 线再好 6~9%。

    要求 dvol_csv 的 date 标签为「当日 UTC 收盘」的隐含方差（已核验 BTC/ETH 满足）。
    """
    rv = _load_rv(kline_csv, timeframe)
    iv = _load_iv(dvol_csv)
    return _engine(rv, iv, window)


# ==============================================================================
# 函数 3 —— 效果报告（人话版）
# ==============================================================================
def report(res, name='模型'):
    """把预测结果翻译成人能直接读懂的评估报告。"""
    df = res.dropna(subset=['Actual_RV']).reset_index(drop=True)
    tomorrow = res[res['Actual_RV'].isna()]
    y, f = df['Actual_RV'].values, df['Pred_RV'].values
    ly, lf = np.log(y), np.log(f)
    dates = pd.to_datetime(df['Forecast_Date'])

    def qlike(a, b):
        r = np.maximum(a, FLOOR) / np.maximum(b, FLOOR)
        return float(np.mean(r - np.log(r) - 1))

    W = 66
    print("\n" + "=" * W)
    print(f" 波动率预测效果报告  |  {name}")
    print("=" * W)

    # ---- 1 基本情况
    av = np.sqrt(y * 365) * 100
    print("\n[1] 基本情况")
    print(f"    样本外预测天数 : {len(df)} 天   ({dates.min().date()} → {dates.max().date()})")
    print(f"    实际波动率     : 平均年化 {av.mean():.1f}%   "
          f"(最低 {av.min():.1f}%  最高 {av.max():.1f}%)")

    # ---- 2 准不准
    sig = float(np.std(ly - lf, ddof=1))
    band_lo, band_hi = np.exp(-sig), np.exp(sig)
    dir_hit = np.mean(np.sign(f - df['RV_today'].values) ==
                      np.sign(y - df['RV_today'].values)) * 100
    print("\n[2] 到底准不准（人话版）")
    print(f"    典型误差倍数   : {band_hi:.2f} 倍")
    print(f"      -> 实际波动率约 68% 的概率落在 预测值 × [{band_lo:.2f}, {band_hi:.2f}] 之内")
    print(f"    涨跌方向命中率 : {dir_hit:.1f}%   (预测明天比今天高/低，随机水平 = 50%)")

    # ---- 3 分档表（最实用）
    q = pd.qcut(df['Pred_RV'], 5, labels=['很低', '低', '中', '高', '很高'])
    g = df.groupby(q).agg(pred=('Pred_RV', 'mean'), act=('Actual_RV', 'mean'),
                          n=('Actual_RV', 'size'))
    mono = int(np.sum(np.diff(g['act'].values) > 0)) + 1
    print("\n[3] 分档检验：按预测值从低到高分 5 组，看各组实际波动率")
    print(f"    {'档位':<6}{'预测年化%':>11}{'实际年化%':>11}{'天数':>7}")
    print("    " + "-" * 36)
    for k, r in g.iterrows():
        print(f"    {str(k):<6}{np.sqrt(r['pred']*365)*100:>10.1f}%"
              f"{np.sqrt(r['act']*365)*100:>10.1f}%{int(r['n']):>7}")
    print(f"    -> 实际列单调递增档数: {mono}/5   "
          f"({'通过，模型能有效区分高低波动环境' if mono == 5 else '注意：分辨力不足'})")

    # ---- 4 与简单办法对比
    q_m = qlike(y, f)
    q_rw = qlike(y, df['Bench_RW'].values)
    q_ew = qlike(y, df['Bench_EWMA'].values)
    r2 = 1 - np.sum((ly - lf) ** 2) / np.sum((ly - ly.mean()) ** 2)
    print("\n[4] 和简单办法比好多少（QLIKE 越小越好）")
    print(f"    {'方法':<22}{'QLIKE':>9}{'本模型优于它':>14}")
    print("    " + "-" * 45)
    print(f"    {'昨天=今天 (RW)':<22}{q_rw:>9.4f}{(q_m-q_rw)/q_rw*100:>13.1f}%")
    print(f"    {'EWMA(对数,半衰期7天)':<20}{q_ew:>9.4f}{(q_m-q_ew)/q_ew*100:>13.1f}%")
    print(f"    {'>> 本模型':<22}{q_m:>9.4f}{'—':>14}")
    print(f"    样本外 log-R²  : {r2:+.3f}   "
          f"(可解释 {max(r2,0)*100:.1f}% 的波动率变化；日频波动率上限约 0.30)")

    # ---- 5 校准
    ratio = f / y
    print("\n[5] 有没有系统性偏高/偏低")
    print(f"    预测/实际 中位数 : {np.median(ratio):.3f}   (>1 偏高, <1 偏低)")
    print(f"    高估天数占比     : {np.mean(ratio > 1)*100:.1f}%   (理想 ≈ 50%)")
    bias = abs(np.median(ratio) - 1)
    print(f"    -> {'校准良好' if bias < 0.10 else '存在系统偏差，建议检查'}"
          f"（偏离 {bias*100:.1f}%）")

    # ---- 6 逐年
    print("\n[6] 逐年表现（检查是否靠某一年）")
    print(f"    {'年份':<8}{'本模型':>9}{'EWMA':>9}{'胜出?':>8}{'天数':>7}")
    print("    " + "-" * 42)
    for yr in sorted(dates.dt.year.unique()):
        m = (dates.dt.year == yr).values
        a, b = qlike(y[m], f[m]), qlike(y[m], df['Bench_EWMA'].values[m])
        print(f"    {yr:<8}{a:>9.4f}{b:>9.4f}{'YES' if a < b else 'no':>8}{int(m.sum()):>7}")

    # ---- 7 近期明细
    print("\n[7] 最近 10 天明细")
    print(f"    {'日期':<12}{'预测年化%':>11}{'实际年化%':>11}{'预测/实际':>11}")
    print("    " + "-" * 45)
    for _, r in df.tail(10).iterrows():
        pv, avv = np.sqrt(r['Pred_RV']*365)*100, np.sqrt(r['Actual_RV']*365)*100
        print(f"    {str(pd.Timestamp(r['Forecast_Date']).date()):<12}"
              f"{pv:>10.1f}%{avv:>10.1f}%{r['Pred_RV']/r['Actual_RV']:>11.2f}")

    # ---- 8 明天
    if len(tomorrow):
        r = tomorrow.iloc[-1]
        p = r['Pred_RV']
        pv = np.sqrt(p * 365) * 100
        pct = float((f < p).mean() * 100)
        print("\n[8] 明天的预测")
        print(f"    日期           : {pd.Timestamp(r['Forecast_Date']).date()}")
        print(f"    预测年化波动率 : {pv:.1f}%   (日波动 {np.sqrt(p)*100:.2f}%)")
        print(f"    历史分位       : {pct:.0f}%   "
              f"({'低波动环境' if pct < 33 else '高波动环境' if pct > 67 else '中等波动环境'})")
        print(f"    68% 参考区间   : {pv*np.sqrt(band_lo):.1f}% ~ {pv*np.sqrt(band_hi):.1f}%")
    print("\n" + "=" * W + "\n")

    return dict(n=len(df), qlike=q_m, qlike_ewma=q_ew, r2_log=r2,
                err_mult=band_hi, dir_hit=dir_hit, mono=mono,
                calib=float(np.median(ratio)))


# ==============================================================================
# 用法示例
# ==============================================================================
if __name__ == "__main__":
    BTC_K = r'W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2021-01-01_merged.csv'
    BTC_IV = 'btc_dvol_2021_now.csv'
    SOL_K = r'W:\project\python_project\oke_auto_trade\kline_data\SOLUSDT_1m_2021-01-01_merged.csv'

    # 有期权数据 —— 首选
    res = forecast_rv_with_iv(BTC_K, BTC_IV)
    report(res, 'BTCUSDT · HAR + 期权IV')
    res.to_csv('forecast_btc.csv', index=False, encoding='utf-8-sig')

    # 无期权数据
    res2 = forecast_rv(SOL_K)
    report(res2, 'SOLUSDT · 纯K线 HAR')

    # 取明天的预测值直接用于仓位缩放
    pred_var = res['Pred_RV'].iloc[-1]
    ann_vol = np.sqrt(pred_var * 365)
    print(f"[实盘] 明日预测年化波动率 = {ann_vol*100:.1f}%")
    print(f"[实盘] 目标年化20%时的仓位系数 = {min(0.20/ann_vol, 3.0):.2f}")