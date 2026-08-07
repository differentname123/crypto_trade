# -*- coding: utf-8 -*-
"""
================================================================================
 FACTOR ANALYZER v3  ——  目标: 组合层面整体正期望 且 统计上确定非偶然
--------------------------------------------------------------------------------
 相对 v2 的改动:
   [1] 目标函数变更
       v2: min_coin_edge (每个币都要正)  →  v3: pooled_ret (整体加权期望)
       广度(frac_pos)降级为诊断指标, 不再作为硬门槛
   [2] 【核心】按币聚类的稳健标准误
       山寨同涨同跌 ⇒ 交易不独立 ⇒ 按笔数算的 t 值严重虚高
       cluster_t 把每个币视为一个聚类, 有效样本量 ≈ 币数而非笔数
   [3] 【核心】White Reality Check (按币 block bootstrap)
       回答: "4.5万个组合里最好的那个, 是否超过了纯随机能达到的最好水平"
       给出考虑了多重检验+组合间相关性的 RC p 值
   [4] 集中度诊断
       top1币贡献占比 / 去掉最好币 / 去掉最大一笔 / LOO 最差
   [5] 容量与年化
       并发持仓数 / 每槽位年化, 判断是否可实盘
   保留 v2 的: 真实BH基准(修drift污染) / 时间归一化 / 标的池拆分
--------------------------------------------------------------------------------
 用法:
     python factor_analyzer_v3.py ./factor_out_15m ./data
     from factor_analyzer_v3 import analyze, combo_detail
     res = analyze('./factor_out_15m', './data')
     combo_detail(res, 'ENTRY_X', 'EXIT_Y')
================================================================================
"""
from __future__ import annotations

import os
import gc
import glob
import math
import time
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

W = 120

# ==============================================================================
# 0. 配置
# ==============================================================================
DEFAULTS = dict(
    COST_PCT          = 0.20,   # 单次往返成本(%) = 2*(FEE+SLIP)*100，必须与挖掘脚本一致

    # --- 实例级硬过滤（关于"策略身份"，不是关于"广度"）---
    MIN_TRADES_INST   = 20,     # 单币单组合最少笔数
    MAX_EXPOSURE      = 70.0,   # 持仓占比上限(%)，超过=伪买入持有
    MIN_HOLD_H        = 0.5,    # 持仓下限(h)，太短成本敏感

    # --- 组合级门槛（数据广度，不是盈利广度）---
    MIN_COINS_DATA    = 20,     # 至少要在多少个币上有合格实例
    MIN_TOTAL_TRADES  = 300,    # 组合总笔数

    # --- 显著性 ---
    CLUSTER_T_TH      = 2.5,
    RC_P_TH           = 0.10,
    N_BOOT            = 1000,   # bootstrap 次数
    BOOT_CI_TOPK      = 3000,   # 只为前 K 个组合保存 bootstrap 分布以算 CI
    SEED              = 20240601,

    # --- 集中度 ---
    MAX_TOP1_COIN     = 0.50,   # 单币贡献占总盈亏上限
    MAX_TOP1_TRADE    = 0.35,   # 单笔贡献占总盈亏上限

    # --- 稳健性 ---
    WINSOR_P          = 0.01,   # 对 avg_ret 做双侧 winsor 的分位

    TOP_N             = 25,
    STOCK_MIN_SPAN_D  = 150,
    STOCK_HINT        = ('AAOI','ALAB','APP','NBIS','DELL','WDC','SNDK','FLNC','AXTI',
                         'SAMSUNG','MINIMAX','GENIUS','OPG','BSP','MUU','MVLL','KORU',
                         'RE','SNXX','BTW','CAP','SHAZ','GRVT'),
)


# ==============================================================================
# 1. 打印
# ==============================================================================
def _h(t, ch='='):
    print('\n' + ch * W); print(f'  {t}'); print(ch * W)


def _kv(k, v, note=''):
    print(f'    {k:<42}: {v}' + (f'    {note}' if note else ''))


def _tbl(d, cols=None, n=None, fmt='{:.4f}'):
    if d is None or len(d) == 0:
        print('      (无数据)'); return
    x = d if cols is None else d[[c for c in cols if c in d.columns]]
    if n: x = x.head(n)
    with pd.option_context('display.width', 460, 'display.max_columns', 100,
                           'display.max_colwidth', 40,
                           'display.float_format', lambda v: fmt.format(v)):
        print(x.to_string(index=False))


def _bar(n, n0, width=44):
    return '█' * max(1, int(width * n / max(n0, 1)))


# ==============================================================================
# 2. 统计工具
# ==============================================================================
def _erf(x):
    x = np.asarray(x, float); s, x = np.sign(x), np.abs(x)
    a = (0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429)
    t = 1.0 / (1.0 + 0.3275911 * x)
    y = 1.0 - (((((a[4]*t + a[3])*t) + a[2])*t + a[1])*t + a[0]) * t * np.exp(-x*x)
    return s * y


def _p2(z):
    z = np.abs(np.asarray(z, float))
    return np.where(np.isfinite(z), 1.0 - _erf(z / math.sqrt(2.0)), np.nan)


def _bh(p):
    p = np.asarray(p, float); out = np.full(p.shape, np.nan)
    ok = np.flatnonzero(np.isfinite(p))
    if ok.size == 0: return out
    pv = p[ok]; o = np.argsort(pv); m = pv.size
    q = pv[o] * m / (np.arange(m) + 1)
    q = np.minimum.accumulate(q[::-1])[::-1]
    r = np.empty(m); r[o] = np.clip(q, 0, 1); out[ok] = r
    return out


def _spearman(a, b):
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() < 30: return np.nan
    return float(pd.Series(a[m]).corr(pd.Series(b[m]), method='spearman'))


def _q(s, ps=(0.01, 0.05, 0.25, 0.50, 0.75, 0.95, 0.99)):
    s = pd.Series(s).replace([np.inf, -np.inf], np.nan).dropna()
    if s.empty: return {f'p{int(p*100)}': np.nan for p in ps}
    return {f'p{int(p*100)}': float(s.quantile(p)) for p in ps}


# ==============================================================================
# 3. 加载
# ==============================================================================
def _load_pairs(out_dir):
    p = os.path.join(out_dir, 'pairs_ALL.csv')
    if os.path.exists(p):
        df, src = pd.read_csv(p), ['pairs_ALL.csv']
    else:
        fl = sorted(f for f in glob.glob(os.path.join(out_dir, 'pairs_*.csv'))
                    if not os.path.basename(f).startswith(('pairs_ALL', 'pairs_CROSS')))
        if not fl: raise FileNotFoundError(f'{out_dir} 下无 pairs_*.csv')
        df = pd.concat([pd.read_csv(f) for f in fl], ignore_index=True)
        src = [os.path.basename(f) for f in fl]
    for c in df.select_dtypes(include=['float64']).columns:
        df[c] = pd.to_numeric(df[c], downcast='float')
    for c in df.select_dtypes(include=['int64']).columns:
        df[c] = pd.to_numeric(df[c], downcast='integer')
    for c in ['coin', 'entry_factor', 'exit_factor']:
        if c in df.columns: df[c] = df[c].astype('category')
    return df, src


def _load_diag(out_dir):
    d = os.path.join(out_dir, 'factor_diag_ALL.csv')
    if os.path.exists(d): return pd.read_csv(d)
    fl = sorted(glob.glob(os.path.join(out_dir, 'factor_diag_*.csv')))
    return pd.concat([pd.read_csv(f) for f in fl], ignore_index=True) if fl else None


# ==============================================================================
# 4. 真实买入持有基准（修 v1 的 drift 污染）
# ==============================================================================
def _bh_kline(coin, data_dir):
    f = os.path.join(data_dir, f'{coin}_USDT_USDT_1m_kline.csv')
    if not os.path.exists(f): return None
    try:
        df = pd.read_csv(f)
    except Exception:
        return None
    tc = next((c for c in ['timestamp', 'open_time', 'time', 'ts'] if c in df.columns), None)
    if tc is None or 'close' not in df.columns or len(df) < 200: return None
    df = df[[tc, 'close']].dropna(); df = df[df['close'] > 0].sort_values(tc)
    if len(df) < 200: return None
    c0, c1 = float(df['close'].iloc[0]), float(df['close'].iloc[-1])
    t0 = pd.to_datetime(df[tc].iloc[0], unit='ms', utc=True)
    t1 = pd.to_datetime(df[tc].iloc[-1], unit='ms', utc=True)
    hrs = (t1 - t0).total_seconds() / 3600.0
    if hrs <= 24: return None
    return ((c1 / c0 - 1.0) * 100.0 / hrs, len(df), t0, t1)


def _bh_regress(sub, cost):
    s = sub.dropna(subset=['avg_ret', 'avg_hold_h']).copy()
    if 'exposure' in s.columns: s = s[s['exposure'] > 30]
    s = s[(s['avg_hold_h'] > 1) & (s['trades'] >= 10)]
    if len(s) < 50: return np.nan
    y = (s['avg_ret'] + cost).to_numpy(float)
    x = s['avg_hold_h'].to_numpy(float); w = s['trades'].to_numpy(float)
    ok = np.isfinite(y) & np.isfinite(x) & (x > 0)
    y, x, w = y[ok], x[ok], w[ok]
    if x.size < 50: return np.nan
    sl = float((w * x * y).sum() / max((w * x * x).sum(), 1e-12))
    for _ in range(2):
        r = y - sl * x
        mad = 1.4826 * np.median(np.abs(r - np.median(r))) + 1e-9
        k = np.abs(r) < 3 * mad
        if k.sum() < 30: break
        yy, xx, ww = y[k], x[k], w[k]
        sl = float((ww * xx * yy).sum() / max((ww * xx * xx).sum(), 1e-12))
    return sl


def build_benchmark(df, data_dir, cost):
    rows = []
    for coin, sub in df.groupby('coin', observed=True):
        r = _bh_kline(str(coin), data_dir) if data_dir else None
        if r:
            bh, bars, t0, t1 = r
            rows.append(dict(coin=str(coin), bh_per_h=bh, src='kline', bars=bars,
                             t_start=t0, t_end=t1,
                             span_days=(t1 - t0).total_seconds() / 86400))
        else:
            rows.append(dict(coin=str(coin), bh_per_h=_bh_regress(sub, cost),
                             src='regress', bars=np.nan, t_start=pd.NaT,
                             t_end=pd.NaT, span_days=np.nan))
    b = pd.DataFrame(rows)
    med = b.loc[b['src'] == 'kline', 'bh_per_h'].median()
    b['bh_per_h'] = b['bh_per_h'].fillna(med if np.isfinite(med) else 0.0)
    b['span_days'] = b['span_days'].fillna(b['span_days'].median())
    return b


# ==============================================================================
# 5. 池拆分 + 实例级指标
# ==============================================================================
def split_pools(df, bench, cfg):
    info = bench.set_index('coin'); hint = set(cfg['STOCK_HINT']); tag = {}
    for coin in df['coin'].astype(str).unique():
        span = info['span_days'].get(coin, np.nan)
        short = np.isfinite(span) and span < cfg['STOCK_MIN_SPAN_D']
        tag[coin] = 'B_短样本类股' if (coin in hint or short) else 'A_山寨永续'
    return tag


def enrich(df, bench, cfg, tag):
    cost = cfg['COST_PCT']
    d = df.copy()
    d['coin'] = d['coin'].astype(str)
    d['avg_hold_h'] = d['avg_hold_h'].replace(0, np.nan)
    d['pool'] = d['coin'].map(tag)
    bi = bench.set_index('coin')
    d['bh_per_h'] = d['coin'].map(bi['bh_per_h'])
    d['span_days'] = d['coin'].map(bi['span_days'])

    d['gross_ret'] = d['avg_ret'] + cost
    d['bench_ret'] = d['bh_per_h'] * d['avg_hold_h']
    d['excess'] = d['avg_ret'] - d['bench_ret']
    d['ret_2x'] = d['avg_ret'] - cost                    # 成本翻倍
    d['edge_100h'] = d['excess'] / d['avg_hold_h'] * 100
    d['ret_100h'] = d['avg_ret'] / d['avg_hold_h'] * 100

    if 'std_ret' not in d.columns: d['std_ret'] = np.nan
    # winsor: 用分位裁剪极端 avg_ret，得到稳健版本
    lo, hi = d['avg_ret'].quantile([cfg['WINSOR_P'], 1 - cfg['WINSOR_P']])
    d['avg_ret_w'] = d['avg_ret'].clip(lo, hi)

    if 'max_win' in d.columns:
        d['sum_ex_max'] = d['sum_ret'] - d['max_win']
    else:
        d['max_win'] = np.nan; d['sum_ex_max'] = np.nan

    bad = d['trades'] < cfg['MIN_TRADES_INST']
    if 'exposure' in d.columns:
        bad |= d['exposure'] > cfg['MAX_EXPOSURE']
    bad |= (d['avg_hold_h'] < cfg['MIN_HOLD_H']).fillna(False)
    d['filtered_out'] = bad
    return d


# ==============================================================================
# 6. 【核心】矩阵化 + 聚类稳健统计
# ==============================================================================
def build_matrices(q, value_col='avg_ret'):
    """
    构造 [J组合 × I币] 的 笔数矩阵 N 和 均值矩阵 M
    返回 combos(DataFrame索引), coins(list), N, M, S(std), P(sum_ret), X(excess), MW(max_win)
    """
    q = q.copy()
    q['_j'] = (q['entry_factor'].astype(str) + '\x01' + q['exit_factor'].astype(str))
    jcat = pd.Categorical(q['_j']); icat = pd.Categorical(q['coin'])
    J, I = len(jcat.categories), len(icat.categories)
    jj, ii = jcat.codes.astype(np.int64), icat.codes.astype(np.int64)
    flat = jj * I + ii

    def acc(vals):
        a = np.zeros(J * I, dtype=np.float64)
        np.add.at(a, flat, np.nan_to_num(np.asarray(vals, float)))
        return a.reshape(J, I)

    N = acc(q['trades'])
    SUM = acc(q['avg_ret'] * q['trades'])
    SUMW = acc(q['avg_ret_w'] * q['trades'])
    SUMX = acc(q['excess'] * q['trades'])
    SUME = acc(q['edge_100h'] * q['trades'])
    SS = acc((q['trades'] - 1).clip(lower=0) * q['std_ret'].fillna(0) ** 2)
    PNL = acc(q['sum_ret'])
    MW = np.zeros((J, I))
    np.maximum.at(MW.reshape(-1), flat, np.nan_to_num(q['max_win'].to_numpy(float)))
    EXPO = acc(q['exposure'] * q['trades']) if 'exposure' in q.columns else np.zeros((J, I))
    HOLD = acc(q['avg_hold_h'].fillna(0) * q['trades'])

    with np.errstate(invalid='ignore', divide='ignore'):
        M = np.where(N > 0, SUM / np.maximum(N, 1e-12), 0.0)
        MW_ = np.where(N > 0, SUMW / np.maximum(N, 1e-12), 0.0)
        MX = np.where(N > 0, SUMX / np.maximum(N, 1e-12), 0.0)
        ME = np.where(N > 0, SUME / np.maximum(N, 1e-12), 0.0)
        MEX = np.where(N > 0, EXPO / np.maximum(N, 1e-12), 0.0)
        MH = np.where(N > 0, HOLD / np.maximum(N, 1e-12), 0.0)

    parts = [c.split('\x01') for c in jcat.categories]
    combos = pd.DataFrame(parts, columns=['entry_factor', 'exit_factor'])

    # OOS
    OOS_N = OOS_M = None
    if {'oos_trades', 'oos_avg_ret'}.issubset(q.columns):
        on = acc(q['oos_trades'].fillna(0))
        os_ = acc(q['oos_avg_ret'].fillna(0) * q['oos_trades'].fillna(0))
        with np.errstate(invalid='ignore', divide='ignore'):
            OOS_M = np.where(on > 0, os_ / np.maximum(on, 1e-12), 0.0)
        OOS_N = on

    return dict(combos=combos, coins=list(icat.categories),
                N=N.astype(np.float32), M=M.astype(np.float32),
                MWIN=MW_.astype(np.float32), MX=MX.astype(np.float32),
                ME=ME.astype(np.float32), SS=SS, PNL=PNL,
                MAXW=MW, EXPO=MEX, HOLD=MH,
                OOS_N=None if OOS_N is None else OOS_N.astype(np.float32),
                OOS_M=None if OOS_M is None else OOS_M.astype(np.float32))


def pooled_stats(mat, span_years):
    """聚类稳健的池化统计"""
    N, M, SS = mat['N'].astype(np.float64), mat['M'].astype(np.float64), mat['SS']
    T = N.sum(1)
    ok = T > 0
    mu = np.where(ok, (N * M).sum(1) / np.maximum(T, 1e-12), np.nan)

    # --- 朴素 SE（假设交易独立）---
    tot_ss = SS + (N * (M - mu[:, None]) ** 2)
    var_pool = tot_ss.sum(1) / np.maximum(T - 1, 1)
    se_naive = np.sqrt(np.maximum(var_pool, 0) / np.maximum(T, 1))

    # --- 聚类稳健 SE（每币一个聚类）---
    wsh = N / np.maximum(T[:, None], 1e-12)
    G = (N > 0).sum(1).astype(float)
    between = ((wsh * (M - mu[:, None])) ** 2).sum(1)
    fin = np.where(G > 1, G / np.maximum(G - 1, 1), 1.0)
    with np.errstate(invalid='ignore', divide='ignore'):
        within = ((wsh ** 2) * np.where(N > 0, SS / np.maximum(N * (N - 1), 1e-12), 0)).sum(1)
    se_cluster = np.sqrt(np.maximum(between * fin + within, 1e-24))

    # --- LOO：剔除任一币后的最差池化均值 ---
    num, den = (N * M).sum(1), T
    loo = (num[:, None] - N * M) / np.maximum(den[:, None] - N, 1e-12)
    loo = np.where(N > 0, loo, np.nan)
    loo_min = np.nanmin(np.where(np.isfinite(loo), loo, np.inf), axis=1)
    loo_min = np.where(np.isfinite(loo_min), loo_min, np.nan)

    # --- 集中度 ---
    PNL = mat['PNL']; tot_pnl = PNL.sum(1)
    best_coin = PNL.max(1)
    with np.errstate(invalid='ignore', divide='ignore'):
        top1_coin = np.where(tot_pnl > 0, best_coin / tot_pnl, np.nan)
        top1_trade = np.where(tot_pnl > 0, mat['MAXW'].max(1) / tot_pnl, np.nan)
    pnl_ex_coin = tot_pnl - best_coin
    pnl_ex_trade = tot_pnl - mat['MAXW'].max(1)

    d = pd.DataFrame(dict(
        n_coins=(N > 0).sum(1), total_trades=T,
        pooled_ret=mu,
        pooled_ret_w=np.where(ok, (N * mat['MWIN']).sum(1) / np.maximum(T, 1e-12), np.nan),
        pooled_excess=np.where(ok, (N * mat['MX']).sum(1) / np.maximum(T, 1e-12), np.nan),
        pooled_edge100h=np.where(ok, (N * mat['ME']).sum(1) / np.maximum(T, 1e-12), np.nan),
        mean_coin_ret=np.where(N > 0, M, np.nan).mean(1) if False else
                      np.nansum(np.where(N > 0, M, np.nan), 1) / np.maximum((N > 0).sum(1), 1),
        med_coin_ret=np.nanmedian(np.where(N > 0, M, np.nan), axis=1),
        frac_pos=np.where(N > 0, (M > 0), np.nan).sum(1) / np.maximum((N > 0).sum(1), 1),
        min_coin_ret=np.nanmin(np.where(N > 0, M, np.inf), axis=1),
        max_coin_ret=np.nanmax(np.where(N > 0, M, -np.inf), axis=1),
        se_naive=se_naive, se_cluster=se_cluster,
        t_naive=mu / np.maximum(se_naive, 1e-12),
        cluster_t=mu / np.maximum(se_cluster, 1e-12),
        loo_min_ret=loo_min,
        total_pnl=tot_pnl, top1_coin_share=top1_coin, top1_trade_share=top1_trade,
        pnl_ex_best_coin=pnl_ex_coin, pnl_ex_top_trade=pnl_ex_trade,
        med_expo=np.nanmedian(np.where(N > 0, mat['EXPO'], np.nan), axis=1),
        med_hold=np.nanmedian(np.where(N > 0, mat['HOLD'], np.nan), axis=1),
    ))
    d['min_coin_ret'] = d['min_coin_ret'].replace(np.inf, np.nan)
    d['max_coin_ret'] = d['max_coin_ret'].replace(-np.inf, np.nan)
    d['t_inflation'] = d['t_naive'] / d['cluster_t'].replace(0, np.nan)

    # 容量/年化：并发槽位 = Σ exposure/100
    concur = np.nansum(np.where(N > 0, mat['EXPO'], 0), axis=1) / 100.0
    d['avg_concurrent'] = concur
    d['ann_ret_per_slot'] = np.where(concur > 0.2,
                                     d['total_pnl'] / max(span_years, 1e-6) / np.maximum(concur, 1e-6),
                                     np.nan)
    d['trades_per_year'] = d['total_trades'] / max(span_years, 1e-6)

    if mat['OOS_N'] is not None:
        on, om = mat['OOS_N'].astype(np.float64), mat['OOS_M'].astype(np.float64)
        ot = on.sum(1)
        d['oos_trades'] = ot
        d['oos_pooled_ret'] = np.where(ot > 0, (on * om).sum(1) / np.maximum(ot, 1e-12), np.nan)
        wsh_o = on / np.maximum(ot[:, None], 1e-12)
        Go = (on > 0).sum(1).astype(float)
        bo = ((wsh_o * (om - d['oos_pooled_ret'].to_numpy()[:, None])) ** 2).sum(1)
        seo = np.sqrt(np.maximum(bo * np.where(Go > 1, Go / np.maximum(Go - 1, 1), 1.0), 1e-24))
        d['oos_cluster_t'] = d['oos_pooled_ret'] / np.maximum(seo, 1e-12)
        d['oos_frac_pos'] = np.where(on > 0, (om > 0), np.nan).sum(1) / np.maximum((on > 0).sum(1), 1)

    return pd.concat([mat['combos'].reset_index(drop=True), d.reset_index(drop=True)], axis=1)


# ==============================================================================
# 7. 【核心】White Reality Check —— 按币 block bootstrap
# ==============================================================================
def reality_check(mat, stats, cfg):
    """
    零假设: 所有组合真实期望为 0
    做法: 对"币"做有放回重采样, 重算所有组合的池化均值, 取 studentized 后的最大值
          得到 max-t 的零分布 → 与观测到的 t 比较
    返回: rc_p (每个组合的 RC p 值), max_t 零分布, 观测最大 t
    """
    N = mat['N'].astype(np.float32)
    NM = (mat['N'].astype(np.float32) * mat['M'].astype(np.float32))
    J, I = N.shape
    mu = stats['pooled_ret'].to_numpy(np.float64)
    se = np.maximum(stats['se_cluster'].to_numpy(np.float64), 1e-9)
    t_obs = mu / se

    rng = np.random.default_rng(cfg['SEED'])
    B = cfg['N_BOOT']
    max_t_null = np.empty(B, dtype=np.float64)

    # 只为最有希望的一批组合保存 bootstrap 分布（省内存）
    order = np.argsort(-np.nan_to_num(t_obs, nan=-1e9))
    keep = order[:min(cfg['BOOT_CI_TOPK'], J)]
    boot_keep = np.empty((B, keep.size), dtype=np.float32)

    t0 = time.time()
    for b in range(B):
        idx = rng.integers(0, I, size=I)
        c = np.bincount(idx, minlength=I).astype(np.float32)
        den = N @ c
        num = NM @ c
        with np.errstate(invalid='ignore', divide='ignore'):
            mu_b = np.where(den > 0, num / np.maximum(den, 1e-12), np.nan)
        # 中心化 → 零假设分布
        t_b = (mu_b - mu) / se
        max_t_null[b] = np.nanmax(t_b)
        boot_keep[b] = mu_b[keep].astype(np.float32)
        if (b + 1) % 200 == 0:
            el = time.time() - t0
            print(f'      bootstrap {b+1}/{B}  {el:.0f}s  预计剩余 {el/(b+1)*(B-b-1):.0f}s')

    # 单步 RC p 值
    rc_p = np.full(J, np.nan)
    valid = np.isfinite(t_obs)
    srt = np.sort(max_t_null)
    rc_p[valid] = 1.0 - np.searchsorted(srt, t_obs[valid], side='left') / B
    rc_p = np.clip(rc_p, 1.0 / B, 1.0)

    ci = pd.DataFrame({
        'boot_lo5': np.nanpercentile(boot_keep, 5, axis=0),
        'boot_med': np.nanpercentile(boot_keep, 50, axis=0),
        'boot_hi95': np.nanpercentile(boot_keep, 95, axis=0),
        'boot_frac_pos': np.nanmean(boot_keep > 0, axis=0),
    }, index=keep)
    return rc_p, max_t_null, t_obs, ci


# ==============================================================================
# 8. 边际统计（池化口径）
# ==============================================================================
def marginal_pooled(q, col):
    g = q.groupby(col, observed=True)
    rows = []
    for k, s in g:
        T = s['trades'].sum()
        if T <= 0: continue
        mu = float((s['avg_ret'] * s['trades']).sum() / T)
        mx = float((s['excess'] * s['trades']).sum() / T)
        cm = s.groupby('coin', observed=True).apply(
            lambda x: float((x['avg_ret'] * x['trades']).sum() / max(x['trades'].sum(), 1)))
        G = len(cm)
        se = cm.std(ddof=1) / math.sqrt(max(G, 1)) if G > 1 else np.nan
        rows.append(dict(**{col: k}, 组合数=len(s), 币种数=G, 总笔数=int(T),
                         池化单笔=mu, 池化超额=mx,
                         币均值=float(cm.mean()), 币中位=float(cm.median()),
                         盈利币占比=float((cm > 0).mean()),
                         cluster_t=float(cm.mean() / se) if se and np.isfinite(se) and se > 0 else np.nan,
                         中位胜率=float(s['win_rate'].median()),
                         中位持仓h=float(s['avg_hold_h'].median()),
                         中位exposure=float(s['exposure'].median()) if 'exposure' in s else np.nan,
                         中位最大盈利=float(s['max_win'].median()) if 'max_win' in s else np.nan))
    return pd.DataFrame(rows)


# ==============================================================================
# 9. 单池分析
# ==============================================================================
def analyze_pool(name, d, diag, cfg, bench, save_dir=None):
    coins = sorted(d['coin'].unique()); NC = len(coins)
    span_years = float(bench[bench['coin'].isin(coins)]['span_days'].median() / 365.25)

    _h(f'【{name}】 币种 {NC} | 实例 {len(d):,} | 样本跨度中位 {span_years*12:.1f} 个月')

    # ---------- 基准 ----------
    print('  [基准] 买入持有 每100h涨幅(%)  —— 判断样本期市况')
    bt = bench[bench['coin'].isin(coins)].copy()
    bt['bench_100h'] = bt['bh_per_h'] * 100
    bt = bt.merge(d.groupby('coin')['avg_ret'].median().rename('中位单笔').reset_index(), on='coin')
    bt = bt.sort_values('bench_100h', ascending=False)
    _tbl(bt[['coin', 'src', 'span_days', 'bench_100h', '中位单笔']], n=200, fmt='{:.3f}')
    _kv('\n  BH 为正的币', f"{int((bt['bench_100h']>0).sum())} / {NC}",
        f"中位 {bt['bench_100h'].median():+.4f}%/100h")

    # ---------- 分布 ----------
    _h('实例级指标分布', '-')
    rows = []
    for c, lab in [('trades', '笔数'), ('avg_ret', '单笔净收益(%)'),
                   ('bench_ret', '同时长BH基准(%)'), ('excess', '超额(%)'),
                   ('edge_100h', 'edge/100h(%)'), ('win_rate', '胜率(%)'),
                   ('avg_hold_h', '持仓(h)'), ('exposure', 'exposure(%)'),
                   ('max_win', '最大单笔盈利(%)')]:
        if c in d.columns:
            r = {'指标': lab}; r.update(_q(d[c]))
            r['mean'] = float(pd.Series(d[c]).replace([np.inf, -np.inf], np.nan).mean())
            rows.append(r)
    _tbl(pd.DataFrame(rows), fmt='{:.3f}')
    r_h = _spearman(d['avg_hold_h'].to_numpy(float), d['excess'].to_numpy(float))
    _kv('\n  秩相关(持仓时长, 超额)', f'{r_h:+.3f}', '← |r|<0.2 表示基准修正有效')

    # ---------- 过滤 ----------
    _h('实例级过滤（关于策略身份，不涉及盈利广度）', '-')
    n0 = len(d); steps = [('① 全部实例', n0)]
    m = d['trades'] >= cfg['MIN_TRADES_INST']
    steps.append((f"② 笔数 ≥{cfg['MIN_TRADES_INST']}", int(m.sum())))
    if 'exposure' in d.columns:
        m &= d['exposure'] <= cfg['MAX_EXPOSURE']
        steps.append((f"③ exposure ≤{cfg['MAX_EXPOSURE']:.0f}%", int(m.sum())))
    m &= ~(d['avg_hold_h'] < cfg['MIN_HOLD_H']).fillna(False)
    steps.append((f"④ 持仓 ≥{cfg['MIN_HOLD_H']}h", int(m.sum())))
    prev = None
    for nm, n in steps:
        print(f"    {nm:<26} {n:>9,}  {_bar(n, n0)}" + ('' if prev is None else f'  (-{prev-n:,})'))
        prev = n
    q = d[m].copy()
    if len(q) < 2000:
        print('\n    ⚠ 过滤后样本不足'); return None

    # ---------- 矩阵 + 池化统计 ----------
    _h('组合级池化统计（聚类稳健）', '-')
    t0 = time.time()
    mat = build_matrices(q)
    S = pooled_stats(mat, span_years)
    S = S[(S['n_coins'] >= cfg['MIN_COINS_DATA']) &
          (S['total_trades'] >= cfg['MIN_TOTAL_TRADES'])].reset_index(drop=True)
    # 同步裁剪矩阵
    key_all = mat['combos']['entry_factor'] + '\x01' + mat['combos']['exit_factor']
    key_keep = S['entry_factor'] + '\x01' + S['exit_factor']
    sel = key_all.isin(set(key_keep))
    for k in ['N', 'M', 'MWIN', 'MX', 'ME', 'SS', 'PNL', 'MAXW', 'EXPO', 'HOLD']:
        mat[k] = mat[k][sel.to_numpy()]
    for k in ['OOS_N', 'OOS_M']:
        if mat[k] is not None: mat[k] = mat[k][sel.to_numpy()]
    mat['combos'] = mat['combos'][sel.to_numpy()].reset_index(drop=True)
    S = pooled_stats(mat, span_years)

    _kv('参与统计的组合数', f'{len(S):,}',
        f"(门槛: 币数≥{cfg['MIN_COINS_DATA']}, 总笔数≥{cfg['MIN_TOTAL_TRADES']})")
    _kv('构建耗时', f'{time.time()-t0:.1f}s')
    _kv('pooled_ret > 0 的组合占比', f"{(S['pooled_ret']>0).mean()*100:.1f}%",
        '← 应≈50%，显著偏离说明样本有系统性方向')
    _kv('t 值虚高倍数 中位', f"{S['t_inflation'].median():.2f}x",
        '← naive_t / cluster_t。这就是 v1/v2 高估显著性的倍数')

    # ---------- Reality Check ----------
    _h('★ Reality Check —— 全篇最重要的一节', '-')
    print(f"  对『币』做 {cfg['N_BOOT']} 次有放回重采样，重算全部 {len(S):,} 个组合，")
    print('  记录每次的最大 studentized t。这给出"纯随机情况下能达到的最好水平"。\n')
    rc_p, max_t_null, t_obs, ci = reality_check(mat, S, cfg)
    S['rc_p'] = rc_p
    S['fdr_q'] = _bh(_p2(S['cluster_t'].to_numpy(float)))
    S = S.join(ci.rename_axis('i').reset_index().set_index('i'), how='left')

    obs_max = float(np.nanmax(t_obs))
    p50, p90, p95, p99 = np.nanpercentile(max_t_null, [50, 90, 95, 99])
    print()
    _kv('观测到的最大 cluster_t', f'{obs_max:.3f}')
    _kv('零分布 max-t  中位 / p90 / p95 / p99',
        f'{p50:.3f} / {p90:.3f} / {p95:.3f} / {p99:.3f}')
    _kv('最优组合的 RC p 值', f'{float(np.nanmin(rc_p)):.4f}',
        '← <0.05 表示整轮挖掘确实找到了非偶然的东西')
    _kv(f"RC p < {cfg['RC_P_TH']} 的组合数", f"{int((S['rc_p'] < cfg['RC_P_TH']).sum()):,}")
    _kv(f"cluster_t > {cfg['CLUSTER_T_TH']} 的组合数", f"{int((S['cluster_t'] > cfg['CLUSTER_T_TH']).sum()):,}")
    print()
    if obs_max <= p95:
        print('  ⚠ 观测最大 t 未超过零分布 95 分位 —— 无法排除"全部由运气产生"。')
    else:
        print(f'  ✔ 观测最大 t 超过零分布 95 分位（{obs_max:.2f} > {p95:.2f}），存在非随机信号。')

    # ---------- 漏斗 ----------
    _h('决策漏斗（面向"整体正期望 + 非偶然"）', '-')
    f = S.copy(); n0 = len(f); fun = [('① 通过基础门槛的组合', n0)]
    f = f[f['pooled_ret'] > 0];                       fun.append(('② 池化单笔期望 > 0', len(f)))
    f = f[f['pooled_excess'] > 0];                    fun.append(('③ 超额(扣BH基准) > 0', len(f)))
    f = f[f['pooled_ret_w'] > 0];                     fun.append(('④ winsor后仍 > 0（抗极值）', len(f)))
    f = f[f['cluster_t'] > cfg['CLUSTER_T_TH']];      fun.append((f"⑤ cluster_t > {cfg['CLUSTER_T_TH']}", len(f)))
    f = f[f['loo_min_ret'] > 0];                      fun.append(('⑥ 剔除任一币后仍 > 0', len(f)))
    f = f[(f['top1_coin_share'] < cfg['MAX_TOP1_COIN']) | f['top1_coin_share'].isna()]
    fun.append((f"⑦ 单币贡献 <{cfg['MAX_TOP1_COIN']:.0%}", len(f)))
    f = f[(f['top1_trade_share'] < cfg['MAX_TOP1_TRADE']) | f['top1_trade_share'].isna()]
    fun.append((f"⑧ 单笔贡献 <{cfg['MAX_TOP1_TRADE']:.0%}", len(f)))
    f = f[f['pnl_ex_top_trade'] > 0];                 fun.append(('⑨ 去掉最大一笔仍盈利', len(f)))
    if 'oos_pooled_ret' in f.columns:
        f = f[(f['oos_pooled_ret'] > 0) | f['oos_pooled_ret'].isna()]
        fun.append(('⑩ 样本外池化期望 > 0', len(f)))
    f = f[f['rc_p'] < cfg['RC_P_TH']];                fun.append((f"⑪ RC p < {cfg['RC_P_TH']}", len(f)))
    prev = None
    for nm, n in fun:
        print(f"    {nm:<32} {n:>7,}  {_bar(n, n0, 40)}" + ('' if prev is None else f'  (-{prev-n:,})'))
        prev = n
    shortlist = f.sort_values('cluster_t', ascending=False).copy()

    # ---------- 排行 ----------
    show = ['entry_factor', 'exit_factor', 'n_coins', 'total_trades',
            'pooled_ret', 'pooled_excess', 'pooled_ret_w', 'cluster_t', 't_naive',
            'rc_p', 'fdr_q', 'boot_lo5', 'boot_frac_pos', 'loo_min_ret',
            'frac_pos', 'med_coin_ret', 'top1_coin_share', 'top1_trade_share',
            'med_hold', 'med_expo', 'total_pnl', 'ann_ret_per_slot', 'avg_concurrent']
    if 'oos_pooled_ret' in S.columns:
        show += ['oos_pooled_ret', 'oos_cluster_t', 'oos_frac_pos']

    for k, desc, asc in [
        ('cluster_t', 'K1 · 聚类稳健 t —— 主排序键（整体期望的显著性）', False),
        ('pooled_ret', 'K2 · 池化单笔期望(%) —— 绝对赚钱能力', False),
        ('boot_lo5', 'K3 · bootstrap 5% 分位 —— 最悲观情形下的期望', False),
        ('rc_p', 'K4 · Reality Check p 值 —— 多重检验后的可信度', True),
    ]:
        if k not in S.columns: continue
        print('\n' + '-' * W); print(f'  {desc}'); print('-' * W)
        _tbl(S.sort_values(k, ascending=asc), show, n=cfg['TOP_N'])

    # ---------- shortlist ----------
    _h(f'★ 最终候选（通过全部 {len(fun)} 层）：{len(shortlist)} 条', '-')
    if len(shortlist) == 0:
        print('  漏斗清空。逐层看上面的计数，定位是在哪一步被砍掉的：')
        print('    · 卡在 ②③ → 该样本期做多整体无正期望')
        print('    · 卡在 ⑤   → 有正期望但幅度不够抗噪，需要更长样本或更少候选')
        print('    · 卡在 ⑥⑦⑧⑨ → 收益集中在个别币/个别交易，不可复制')
        print('    · 卡在 ⑩   → 样本内有效样本外失效，过拟合')
        print('    · 卡在 ⑪   → 单看不错，但放进 4 万个候选里不算突出')
    else:
        _tbl(shortlist, show, n=cfg['TOP_N'])
        print('\n  [按进场因子去重后的独立想法]')
        _tbl(shortlist.drop_duplicates('entry_factor'),
             ['entry_factor', 'exit_factor', 'pooled_ret', 'cluster_t', 'rc_p',
              'boot_lo5', 'frac_pos', 'total_trades', 'med_hold', 'ann_ret_per_slot'], n=15)
        _h('候选决策卡', '-')
        for _, r in shortlist.head(8).iterrows():
            tags = []
            if r['cluster_t'] > 4: tags.append('统计强')
            if r.get('rc_p', 1) < 0.02: tags.append('RC强')
            if r['frac_pos'] >= 0.6: tags.append(f"广度{r['frac_pos']:.0%}")
            if r['top1_coin_share'] < 0.2: tags.append('分散')
            if np.isfinite(r.get('oos_cluster_t', np.nan)) and r['oos_cluster_t'] > 1.5:
                tags.append('OOS确认')
            if r['med_hold'] > 120: tags.append('长持')
            elif r['med_hold'] < 8: tags.append('短打·成本敏感')
            print(f"\n    ▸ {r['entry_factor']}  ➜  {r['exit_factor']}")
            print(f"        单笔期望={r['pooled_ret']:+.4f}%  超额={r['pooled_excess']:+.4f}%  "
                  f"winsor={r['pooled_ret_w']:+.4f}%")
            print(f"        cluster_t={r['cluster_t']:.2f} (naive={r['t_naive']:.1f}, "
                  f"虚高{r['t_inflation']:.1f}x)  RC_p={r['rc_p']:.4f}  "
                  f"boot[5%,95%]=[{r['boot_lo5']:+.4f}, {r['boot_hi95']:+.4f}]")
            print(f"        币数={int(r['n_coins'])} 笔数={int(r['total_trades'])} "
                  f"盈利币={r['frac_pos']:.0%} LOO最差={r['loo_min_ret']:+.4f} "
                  f"单币占比={r['top1_coin_share']:.0%} 单笔占比={r['top1_trade_share']:.0%}")
            print(f"        持仓={r['med_hold']:.1f}h 并发={r['avg_concurrent']:.1f} "
                  f"每槽年化={r['ann_ret_per_slot']:.1f}% 总盈亏={r['total_pnl']:.0f}%")
            print(f"        标签: {' | '.join(tags) if tags else '—'}")

    # ---------- 边际 ----------
    _h('进场因子边际（池化口径，按 cluster_t 降序）', '-')
    ent = marginal_pooled(q, 'entry_factor').sort_values('cluster_t', ascending=False)
    ec = ['entry_factor', '组合数', '币种数', '总笔数', '池化单笔', '池化超额',
          '币均值', '币中位', '盈利币占比', 'cluster_t', '中位胜率', '中位持仓h',
          '中位exposure', '中位最大盈利']
    _tbl(ent, ec, n=cfg['TOP_N'])
    print('\n  [BOTTOM 8 —— 显著负向，可反向或用作过滤]')
    _tbl(ent.tail(8)[::-1], ec)

    if diag is not None and 'factor' in diag.columns:
        fw = [c for c in diag.columns if c.startswith('fwd_') and c.endswith('_t')]
        if fw:
            cols = fw + (['density'] if 'density' in diag.columns else [])
            dg = diag.groupby('factor')[cols].mean().reset_index()
            mg = ent[['entry_factor', '池化单笔', 'cluster_t', '盈利币占比']].merge(
                dg, left_on='entry_factor', right_on='factor', how='inner')
            print('\n  [独立证据链 · 单因子前瞻收益 t 值]')
            for c in fw:
                _kv(f'秩相关 池化单笔 vs {c}',
                    f"{_spearman(mg['池化单笔'].to_numpy(float), mg[c].to_numpy(float)):+.3f}")
            main = fw[-1]
            dual = mg[(mg['池化单笔'] > 0) & (mg[main] > 1.5)].sort_values('cluster_t', ascending=False)
            print(f'\n    双重确认（池化单笔>0 且 {main}>1.5）: {len(dual)} 个')
            _tbl(dual, ['entry_factor', '池化单笔', 'cluster_t', '盈利币占比'] + cols, n=20, fmt='{:.3f}')

    _h('出场因子边际（池化口径）', '-')
    ext = marginal_pooled(q, 'exit_factor').sort_values('cluster_t', ascending=False)
    _tbl(ext, ['exit_factor'] + ec[1:], n=cfg['TOP_N'])
    if '中位最大盈利' in ext.columns:
        print('\n  [按 中位最大盈利 降序 —— 右尾捕获能力]')
        _tbl(ext.sort_values('中位最大盈利', ascending=False),
             ['exit_factor', '中位最大盈利', '池化单笔', 'cluster_t', '中位胜率',
              '中位持仓h', '盈利币占比', '总笔数'], n=15, fmt='{:.3f}')

    # ---------- OOS ----------
    _h('样本内 → 样本外', '-')
    if {'is_avg_ret', 'oos_avg_ret'}.issubset(q.columns):
        s = q.dropna(subset=['is_avg_ret', 'oos_avg_ret']).copy()
        if len(s) > 200:
            _kv('实例级 IS/OOS 秩相关',
                f"{_spearman(s['is_avg_ret'].to_numpy(float), s['oos_avg_ret'].to_numpy(float)):+.3f}")
    if 'oos_pooled_ret' in S.columns:
        s2 = S.dropna(subset=['oos_pooled_ret'])
        if len(s2) > 200:
            _kv('组合级 IS/OOS 池化期望 秩相关',
                f"{_spearman(s2['pooled_ret'].to_numpy(float), s2['oos_pooled_ret'].to_numpy(float)):+.3f}",
                '← 这个比实例级更有意义')
            s2 = s2.copy()
            s2['档'] = pd.qcut(s2['pooled_ret'].rank(method='first'), 10, labels=range(1, 11))
            dc = s2.groupby('档').agg(n=('oos_pooled_ret', 'size'),
                                     IS池化=('pooled_ret', 'mean'),
                                     OOS池化=('oos_pooled_ret', 'mean'),
                                     OOS为正=('oos_pooled_ret', lambda x: (x > 0).mean())).reset_index()
            _tbl(dc, fmt='{:.4f}')
            _kv('全体 OOS 池化均值', f"{s2['oos_pooled_ret'].mean():.4f}")
            _kv('IS第10档 OOS 池化均值', f"{dc.loc[dc['档']==10,'OOS池化'].values[0]:.4f}")

    if save_dir:
        os.makedirs(save_dir, exist_ok=True)
        sfx = 'A' if name.startswith('A_') else 'B'
        S.sort_values('cluster_t', ascending=False).to_csv(
            os.path.join(save_dir, f'combo_pooled_{sfx}.csv'), index=False, encoding='utf-8-sig')
        shortlist.to_csv(os.path.join(save_dir, f'shortlist_{sfx}.csv'),
                         index=False, encoding='utf-8-sig')
        ent.to_csv(os.path.join(save_dir, f'marginal_entry_{sfx}.csv'),
                   index=False, encoding='utf-8-sig')
        ext.to_csv(os.path.join(save_dir, f'marginal_exit_{sfx}.csv'),
                   index=False, encoding='utf-8-sig')
        pd.DataFrame({'max_t_null': max_t_null}).to_csv(
            os.path.join(save_dir, f'rc_null_{sfx}.csv'), index=False)

    del mat; gc.collect()
    return dict(stats=S, shortlist=shortlist, entry=ent, exit=ext,
                max_t_null=max_t_null, q=q)


# ==============================================================================
# 10. 主入口
# ==============================================================================
def analyze(out_dir='./factor_out_15m', data_dir='./data', save=True, **kw):
    cfg = dict(DEFAULTS); cfg.update(kw)
    pd.set_option('display.unicode.east_asian_width', True)

    print('\n' + '#' * W)
    print(f'#  FACTOR ANALYZER v3  ——  目标: 整体正期望 + 统计非偶然')
    print(f'#  out={os.path.abspath(out_dir)}')
    print(f'#  data={os.path.abspath(data_dir) if data_dir else "(无，用回归估基准)"}')
    print('#' * W)

    pairs, src = _load_pairs(out_dir)
    diag = _load_diag(out_dir)

    _h('§1  数据概览')
    _kv('来源', ', '.join(src)[:90])
    _kv('记录数', f'{len(pairs):,}')
    _kv('币种数', f"{pairs['coin'].nunique()}")
    _kv('唯一组合数', f"{pairs.groupby(['entry_factor','exit_factor'], observed=True).ngroups:,}")
    _kv('假定往返成本', f"{cfg['COST_PCT']:.3f}%", '← 必须与挖掘脚本一致')
    _kv('全样本单笔净收益中位', f"{pairs['avg_ret'].median():+.4f}%",
        '← 若≈ -成本，说明随机组合收敛到理论值（框架健全）')

    _h('§2  买入持有基准估计')
    bench = build_benchmark(pairs, data_dir, cfg['COST_PCT'])
    nk = int((bench['src'] == 'kline').sum())
    _kv('K线首尾直算 / 回归兜底', f'{nk} / {len(bench)-nk}')
    _kv('基准中位', f"{bench['bh_per_h'].median()*100:+.4f} %/100h")

    tag = split_pools(pairs, bench, cfg)
    d = enrich(pairs, bench, cfg, tag)
    del pairs; gc.collect()

    _h('§3  标的池拆分')
    cnt = pd.Series(tag).value_counts()
    for k, v in cnt.items():
        mem = sorted([c for c, t in tag.items() if t == k])
        print(f'    {k:<16} {v:>3} 个')
        for i in range(0, len(mem), 12):
            print('        ' + ', '.join(mem[i:i+12]))

    save_dir = os.path.join(out_dir, 'analysis_v3') if save else None
    out = {}
    for nm in sorted(cnt.index):
        sub = d[d['pool'] == nm]
        if sub['coin'].nunique() < cfg['MIN_COINS_DATA']:
            print(f'\n  [{nm}] 币种少于 {cfg["MIN_COINS_DATA"]}，跳过池化分析')
            continue
        out[nm] = analyze_pool(nm, sub, diag, cfg, bench, save_dir)

    _h('§9  字段口径')
    for k, v in [
        ('pooled_ret', '按笔数加权的单笔净收益期望(%)。等名义仓位下，这就是"整体期望"'),
        ('pooled_excess', 'pooled_ret 扣掉同时长买入持有基准 —— 判断是不是只在吃 beta'),
        ('pooled_ret_w', '对 avg_ret 双侧 winsor 后重算 —— 抗极值版本'),
        ('t_naive', '假设交易独立的 t 值。山寨同涨同跌 ⇒ 严重虚高，仅作对照'),
        ('cluster_t', '★把每个币视为一个聚类的稳健 t。有效样本量≈币数。这才是真显著性'),
        ('t_inflation', 't_naive / cluster_t，即朴素方法高估了多少倍'),
        ('rc_p', '★White Reality Check p 值。对"币"做 block bootstrap 得到零假设下'),
        ('', '  max-t 分布，考虑了 4 万次多重检验 + 组合间相关性。<0.05 才算真发现'),
        ('boot_lo5/hi95', 'bootstrap 重采样币种后，池化期望的 5%/95% 分位（置信区间）'),
        ('boot_frac_pos', 'bootstrap 中池化期望为正的比例，越接近 1 越稳'),
        ('loo_min_ret', '逐一剔除每个币后，池化期望的最小值。>0 = 不依赖任何单一币'),
        ('top1_coin_share', '贡献最大的币占总盈亏比例。>50% = 伪装成策略的单币行情'),
        ('top1_trade_share', '最大单笔占总盈亏比例。>35% = 彩票'),
        ('frac_pos', '盈利币种占比（诊断用，不作硬门槛 —— 这是你新目标下的降级项）'),
        ('avg_concurrent', 'Σexposure/100 ≈ 平均并发持仓数，用于估容量'),
        ('ann_ret_per_slot', '总盈亏 / 年数 / 并发数 ≈ 每个仓位槽的年化(%)'),
    ]:
        print(f'    {k:<18} {v}')

    if save:
        os.makedirs(save_dir, exist_ok=True)
        bench.to_csv(os.path.join(save_dir, 'benchmark.csv'), index=False, encoding='utf-8-sig')
        print(f'\n    已写出 -> {os.path.abspath(save_dir)}')
        for fn in ['benchmark.csv', 'combo_pooled_A.csv', 'shortlist_A.csv',
                   'marginal_entry_A.csv', 'marginal_exit_A.csv', 'rc_null_A.csv']:
            print(f'      {fn}')

    print('\n' + '#' * W + '\n')
    return dict(raw=d, bench=bench, pools=out, pool_tag=tag)


def combo_detail(res, entry_factor, exit_factor):
    df = res['raw']
    s = df[(df['entry_factor'] == entry_factor) & (df['exit_factor'] == exit_factor)]
    if s.empty:
        print('未找到该组合'); return
    _h(f'{entry_factor}  >  {exit_factor}')
    cols = [c for c in ['coin', 'pool', 'trades', 'win_rate', 'avg_ret', 'bench_ret',
                        'excess', 'edge_100h', 'sum_ret', 'max_dd', 'avg_hold_h',
                        'exposure', 'max_win', 'max_loss', 'oos_trades', 'oos_avg_ret',
                        'filtered_out'] if c in s.columns]
    _tbl(s.sort_values('sum_ret', ascending=False), cols, fmt='{:.4f}')
    T = s['trades'].sum()
    mu = float((s['avg_ret'] * s['trades']).sum() / max(T, 1))
    cm = s.groupby('coin').apply(lambda x: float((x['avg_ret']*x['trades']).sum()/max(x['trades'].sum(),1)))
    se = cm.std(ddof=1) / math.sqrt(max(len(cm), 1)) if len(cm) > 1 else np.nan
    print()
    _kv('覆盖币种 / 通过过滤', f"{s['coin'].nunique()} / {int((~s['filtered_out']).sum())}")
    _kv('总笔数', f'{int(T):,}')
    _kv('池化单笔期望', f'{mu:+.4f}%')
    _kv('cluster_t', f"{mu/se:.2f}" if se and np.isfinite(se) and se > 0 else 'n/a')
    _kv('盈利币 / 总币', f"{int((cm>0).sum())} / {len(cm)}")
    _kv('总盈亏', f"{s['sum_ret'].sum():.1f}%",
        f"去掉最好的币 {s['sum_ret'].sum()-s['sum_ret'].max():.1f}%")
    _kv('最大贡献币', f"{s.loc[s['sum_ret'].idxmax(),'coin']} "
                     f"({s['sum_ret'].max()/max(s['sum_ret'].sum(),1e-9)*100:.0f}%)")


if __name__ == '__main__':
    import sys
    o = sys.argv[1] if len(sys.argv) > 1 else './factor_out_15m'
    dd = sys.argv[2] if len(sys.argv) > 2 else './data'
    analyze(o, dd)