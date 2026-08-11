# -*- coding: utf-8 -*-
"""
================================================================================
 ALT-COIN LAUNCH FACTOR MINER  (山寨启动 · 因子挖掘 / 两两组合回测)
--------------------------------------------------------------------------------
 · 全量计算因子池 -> 每根 bar 一个 bool signal
 · 所有因子两两有序组合 (A进场,B出场) != (B进场,A出场)
 · 纯做多、每笔等名义仓位、收益率【加总不复利】
 · 结果全量落盘 CSV，含 IS/OOS 切分与跨币种稳健性
 · [终极定稿] 内存优化版：彻底抛弃逐笔流水，在内存截取高阶统计特征，告别OOM
 · [新增特性] 事后对入场信号进行 15 组不同强度的横截面(Rank)环境过滤测试
 · [本次改造] 只落盘 pairs_{coin}.csv；断点续跑 + 原子写入；无感注入"测试基数"
              (pairs_ALL / pairs_CROSS_COIN_SUMMARY 改由独立还原脚本重建)
 · [性能优化] 统计基元标量化(逐位复刻) / 因子基础量去重缓存 /
              列式内存装配 / BTC 基准按 worker 一次性下发   —— 结果不变
================================================================================
"""
from __future__ import annotations
import traceback

import os
import sys
import time
import math
import itertools
import warnings
import csv
import gc

import concurrent.futures
import multiprocessing

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ======================================================================
# 0. 全局配置与环境过滤模式
# ======================================================================
CFG = dict(
    DATA_DIR='../data',
    OUT_DIR='./factor_out_15m',

    # --- 采样与执行 ---
    BAR_MINUTES=15,
    FEE_RATE=0.0005,
    SLIPPAGE=0.0005,
    COOLDOWN_BARS=0,
    FORCE_CLOSE_AT_END=True,

    # --- 因子行为 ---
    RANK_SHIFT=0,
    DEDUPE_IDENTICAL=False,
    MIN_SIGNALS=1,
    MAX_DENSITY=0.9,
    INCLUDE_PATH_EXITS=True,

    # --- 组合与输出 ---
    ALLOW_SAME_FACTOR=False,
    MAX_TRADES_PER_COMBO=100000,
    MIN_TRADES_REPORT=1,
    OOS_SPLIT=0.70,
    ENTRY_PREFIX_FILTER=None,
    EXIT_PREFIX_FILTER=None,
    COINS=None,

    # --- 因子体检 ---
    FWD_HORIZONS_H=(4, 12, 24, 72),
)

EPS = 1e-12

# 【新增】15种环境过滤模式
FILTER_MODES = [
    ('original', None, 0),
    ('top', 'rank_gain_24h', 1), ('top', 'rank_gain_24h', 3), ('top', 'rank_gain_24h', 5),
    ('top', 'rank_gain_24h', 10), ('top', 'rank_gain_24h', 20), ('top', 'rank_gain_24h', 50),
    ('top', 'rank_gain_24h', 100),
    ('bottom', 'rank_loss_24h', 1), ('bottom', 'rank_loss_24h', 3), ('bottom', 'rank_loss_24h', 5),
    ('bottom', 'rank_loss_24h', 10), ('bottom', 'rank_loss_24h', 20), ('bottom', 'rank_loss_24h', 50),
    ('bottom', 'rank_loss_24h', 100),
]

# ======================================================================
# 1. numba 可选加速 (已优化为稀疏索引跳跃查询)
# ======================================================================
try:
    from numba import njit

    HAS_NUMBA = True
except Exception:
    HAS_NUMBA = False


def _core_static(entry_idx, exit_idx, n, cooldown, max_trades):
    ent = np.empty(max_trades, dtype=np.int64)
    ext = np.empty(max_trades, dtype=np.int64)
    k = 0
    pos = 0
    ne = entry_idx.size
    nx = exit_idx.size
    while pos < n - 1 and k < max_trades:
        # 二分查找定位下一个大于等于 pos 的入场点
        a = np.searchsorted(entry_idx, pos)
        if a >= ne:
            break
        e = entry_idx[a]
        if e >= n - 1:
            break
        # 二分查找定位下一个大于 e 的出场点
        b = np.searchsorted(exit_idx, e + 1)
        if b < nx:
            found = exit_idx[b]
        else:
            found = n - 1
        ent[k] = e
        ext[k] = found
        k += 1
        pos = found + 1 + cooldown
    return ent[:k], ext[:k]


def _core_path(entry_idx, static_exit, close, low, atr, n, cooldown, max_trades,
               exec_px,
               use_fixed, fixed_pct,
               use_barlow,
               use_atr, atr_k,
               use_time, time_n, time_th,
               use_gb, gb_th,
               use_lock, lock_th, lock_trail):
    ent = np.empty(max_trades, dtype=np.int64)
    ext = np.empty(max_trades, dtype=np.int64)
    k = 0
    pos = 0
    ne = entry_idx.size
    while pos < n - 1 and k < max_trades:
        # 二分查找极速定位入场点，拒绝在布尔数组上循环爬行
        a = np.searchsorted(entry_idx, pos)
        if a >= ne:
            break
        e = entry_idx[a]
        if e >= n - 1:
            break

        ep = exec_px[e]
        el = low[e]
        peak = close[e]
        peak_prof = 0.0
        j = e + 1
        hit = -1
        # 出场因为严重依赖入场价(ep)，只能局部顺序扫描
        while j < n:
            cj = close[j]
            if cj > peak:
                peak = cj
            prof = cj / ep - 1.0
            if prof > peak_prof:
                peak_prof = prof
            trig = False
            if static_exit[j]:
                trig = True
            if (not trig) and use_fixed and cj < ep * (1.0 - fixed_pct):
                trig = True
            if (not trig) and use_barlow and cj < el:
                trig = True
            if (not trig) and use_atr:
                a_v = atr[j]
                if a_v == a_v and cj < peak - atr_k * a_v:
                    trig = True
            if (not trig) and use_time and (j - e) > time_n and prof < time_th:
                trig = True
            if (not trig) and use_gb and (peak_prof - prof) > gb_th:
                trig = True
            if (not trig) and use_lock and prof > lock_th and cj < peak * (1.0 - lock_trail):
                trig = True
            if trig:
                hit = j
                break
            j += 1
        if hit < 0:
            hit = n - 1
        ent[k] = e
        ext[k] = hit
        k += 1
        pos = hit + 1 + cooldown
    return ent[:k], ext[:k]


if HAS_NUMBA:
    _core_static = njit(cache=True, nogil=True)(_core_static)
    _core_path = njit(cache=True, nogil=True)(_core_path)


def _match_static_ss(entry_idx, exit_idx, n, cooldown, max_trades):
    ent, ext = [], []
    ne, nx = entry_idx.size, exit_idx.size
    pos = 0
    while pos < n - 1 and len(ent) < max_trades:
        a = np.searchsorted(entry_idx, pos, side='left')
        if a >= ne:
            break
        e = int(entry_idx[a])
        if e >= n - 1:
            break
        b = np.searchsorted(exit_idx, e + 1, side='left')
        x = int(exit_idx[b]) if b < nx else n - 1
        ent.append(e)
        ext.append(x)
        pos = x + 1 + cooldown
    return np.asarray(ent, np.int64), np.asarray(ext, np.int64)


def _path_scan_np(e, ep, el, n, close, atr, static_exit, p):
    start = e + 1
    peak = close[e]
    peak_prof = 0.0
    chunk = 256
    while start < n:
        stop = min(start + chunk, n)
        cl = close[start:stop]
        pk = np.maximum.accumulate(np.maximum(cl, peak))
        prof = cl / ep - 1.0
        pp = np.maximum.accumulate(np.maximum(prof, peak_prof))
        trig = static_exit[start:stop].copy()
        if p['use_fixed']:
            trig |= cl < ep * (1 - p['fixed_pct'])
        if p['use_barlow']:
            trig |= cl < el
        if p['use_atr']:
            trig |= cl < (pk - p['atr_k'] * atr[start:stop])
        if p['use_time']:
            bars = np.arange(start, stop) - e
            trig |= (bars > p['time_n']) & (prof < p['time_th'])
        if p['use_gb']:
            trig |= (pp - prof) > p['gb_th']
        if p['use_lock']:
            trig |= (prof > p['lock_th']) & (cl < pk * (1 - p['lock_trail']))
        w = np.flatnonzero(trig)
        if w.size:
            return start + int(w[0])
        peak = pk[-1]
        peak_prof = pp[-1]
        start = stop
        chunk = min(chunk * 2, 32768)
    return n - 1


def _match_path_np(entry_idx, static_exit, close, low, atr, exec_px, n, cooldown, max_trades, p):
    ent, ext = [], []
    ne = entry_idx.size
    pos = 0
    while pos < n - 1 and len(ent) < max_trades:
        a = np.searchsorted(entry_idx, pos, side='left')
        if a >= ne:
            break
        e = int(entry_idx[a])
        if e >= n - 1:
            break
        x = _path_scan_np(e, exec_px[e], low[e], n, close, atr, static_exit, p)
        ent.append(e)
        ext.append(x)
        pos = x + 1 + cooldown
    return np.asarray(ent, np.int64), np.asarray(ext, np.int64)


# ======================================================================
# 2. 数据加载 / 重采样 / 对齐
# ======================================================================
def _pick(df, cands, what):
    for c in cands:
        if c in df.columns:
            return c
    raise KeyError(f"[{what}] 找不到列 {cands}，实际列: {list(df.columns)}")


def load_symbol(kline_file, oi_file, fr_file, bar_minutes):
    bar = f"{bar_minutes}min"
    k = pd.read_csv(kline_file)
    kt = _pick(k, ['timestamp', 'open_time', 'time', 'ts'], 'kline')
    k['dt'] = pd.to_datetime(k[kt], unit='ms', utc=True)
    k = k.drop_duplicates(subset=[kt]).sort_values('dt').set_index('dt')

    # 【新增】兼容如果某些基础标的(如 BTC) 没有这俩字段时，默认给超大值避开过滤
    if 'rank_gain_24h' not in k.columns:
        k['rank_gain_24h'] = 999999.0
    if 'rank_loss_24h' not in k.columns:
        k['rank_loss_24h'] = 999999.0

    # 【修改】聚合时带上排名特征
    agg = k.resample(bar, label='left', closed='left').agg(
        open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'),
        volume=('volume', 'sum'),
        rank_gain_24h=('rank_gain_24h', 'last'),
        rank_loss_24h=('rank_loss_24h', 'last')
    )
    agg['close'] = agg['close'].ffill()
    agg = agg[agg['close'].notna()]
    agg['open'] = agg['open'].fillna(agg['close'])
    agg['high'] = agg['high'].fillna(agg['close'])
    agg['low'] = agg['low'].fillna(agg['close'])
    agg['volume'] = agg['volume'].fillna(0.0)

    # 【新增】填充空洞排名以避免 NaN
    agg['rank_gain_24h'] = agg['rank_gain_24h'].fillna(999999.0)
    agg['rank_loss_24h'] = agg['rank_loss_24h'].fillna(999999.0)

    oi = pd.read_csv(oi_file)
    ot = _pick(oi, ['timestamp', 'time', 'ts'], 'oi')
    oc = _pick(oi, ['oi_amount', 'openInterest', 'open_interest', 'sumOpenInterest', 'oi'], 'oi')
    oi['dt'] = pd.to_datetime(oi[ot], unit='ms', utc=True)
    oi_s = (oi.drop_duplicates(subset=[ot]).sort_values('dt').set_index('dt')[oc]
            .astype(float).resample(bar, label='left', closed='left').last())

    fr = pd.read_csv(fr_file)
    ft = _pick(fr, ['timestamp', 'fundingTime', 'time', 'ts'], 'fr')
    fc = _pick(fr, ['funding_rate', 'fundingRate', 'rate'], 'fr')
    fr['dt'] = pd.to_datetime(fr[ft], unit='ms', utc=True)
    fr_s = (fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc]
            .astype(float).resample(bar, label='left', closed='left').last())

    df = agg.copy()
    df['oi_amount'] = oi_s.reindex(df.index).ffill()
    df['funding_rate'] = fr_s.reindex(df.index).ffill()

    fv = df[['oi_amount', 'funding_rate']].apply(lambda s: s.first_valid_index())
    start = max([x for x in fv.tolist() if x is not None], default=df.index[0])
    df = df.loc[start:].copy()
    df[['oi_amount', 'funding_rate']] = df[['oi_amount', 'funding_rate']].ffill()
    df = df.dropna(subset=['oi_amount', 'funding_rate'])
    for c in ['open', 'high', 'low', 'close']:
        df = df[df[c] > 0]
    return df


# ======================================================================
# 3. 参数体系
# ======================================================================
def make_params(bar_minutes, n_rows):
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))
    P = {}
    P['BPH'] = B(1)
    P['N'] = B(24)
    P['M'] = B(4)
    P['W'] = B(24 * 30)
    P['H12'], P['H24'], P['H48'] = B(12), B(24), B(48)
    P['H72'], P['H168'] = B(72), B(168)
    P['D2'], P['D7'] = B(48), B(168)

    if n_rows < P['W'] * 2:
        P['W'] = max(200, n_rows // 3)
    P['MINP_W'] = max(50, P['W'] // 5)

    N, M = P['N'], P['M']
    P.update(
        K_UP_BARS=max(2, int(0.60 * N)),
        K_NEWHIGH=max(2, int(0.15 * N)),
        K_SMALL_GREEN=max(3, int(0.50 * N)),
        K_STRONG_CLOSE=max(3, int(0.50 * N)),
        K_OI_UP=max(2, int(0.70 * N)),
        K_WEAK_CLOSE=max(3, int(0.60 * N)),
        VOL_BREAK_MULT=2.0,
        ATR_K=3.0,
        ADX_TH=25.0,
        GAP_TH=0.003,
        EXHAUST_TH=0.002,
        FLAT_TH=0.010,
        SILENT_TH=0.010,
        OI_ROC_TH=0.020,
        OI_HOT_TH=0.050,
        CORR_TH=0.20,
        STOP_PCT=0.05,
        TIME_STOP_BARS=B(72),
        TIME_STOP_TH=0.00,
        GIVEBACK_TH=0.05,
        LOCK_TH=0.10,
        LOCK_TRAIL=0.05,
    )
    P['WARMUP'] = int(P['W'] + P['H168'] + 3 * N)
    return P


# ======================================================================
# 4. 因子库
# ======================================================================
def build_factors(df, P, rank_shift=0):
    W, N, M = P['W'], P['N'], P['M']
    mp = P['MINP_W']

    o, h, l, c = df['open'], df['high'], df['low'], df['close']
    v, oi, fr = df['volume'], df['oi_amount'], df['funding_rate']

    def RK(s):
        r = s.rolling(W, min_periods=mp).rank(pct=True)
        return r.shift(rank_shift) if rank_shift else r

    def ZS(s):
        m = s.rolling(W, min_periods=mp).mean()
        sd = s.rolling(W, min_periods=mp).std()
        return (s - m) / (sd + EPS)

    def QT(s, p):
        return s.rolling(W, min_periods=mp).quantile(p).shift(1)

    def CU(a, b):
        return (a > b) & (a.shift(1) <= b.shift(1))

    def CD(a, b):
        return (a < b) & (a.shift(1) >= b.shift(1))

    def bs(s, k=1):
        return s.shift(k, fill_value=False)

    def pctc(s, n):
        return s.pct_change(n).replace([np.inf, -np.inf], np.nan)

    def RSUM(s, n):
        return s.rolling(n, min_periods=max(2, n // 2)).sum()

    # 【性能】同窗口的收盘均线在不同因子里被重复请求(H24==N / H48 / H168)，
    #        这里做纯记忆化：窗口相同 -> 完全相同的计算 -> 结果逐位一致
    _ma_cache = {}

    def MA(n):
        r = _ma_cache.get(n)
        if r is None:
            r = c.rolling(n, min_periods=max(2, n // 2)).mean()
            _ma_cache[n] = r
        return r

    # ---------- 基础量 ----------
    ret_1 = c.pct_change()
    ret_1h = c.pct_change(P['BPH'])
    ret_N = c.pct_change(N)
    ret_M = c.pct_change(M)

    ma_N, ma_M = MA(N), MA(M)
    ma_fast, ma_slow = MA(P['H48']), MA(P['H168'])
    ma_12h, ma_24h, ma_48h, ma_72h, ma_7d = MA(P['H12']), MA(P['H24']), MA(P['H48']), MA(P['H72']), MA(P['H168'])

    maxH_N = h.rolling(N, min_periods=max(2, N // 2)).max()
    minL_N = l.rolling(N, min_periods=max(2, N // 2)).min()
    maxH_M = h.rolling(M, min_periods=max(2, M // 2)).max()
    minL_M = l.rolling(M, min_periods=max(2, M // 2)).min()

    pc = c.shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    atr_N = tr.rolling(N, min_periods=max(2, N // 2)).mean()
    tr_sum = tr.rolling(N, min_periods=max(2, N // 2)).sum()

    vma_N = v.rolling(N, min_periods=max(2, N // 2)).mean()
    vma_fast = v.rolling(M, min_periods=max(2, M // 2)).mean()
    vma_slow = vma_N
    # 【性能】RSUM(v, N) 原本被 vwap / _vwm / VOLUME_UP_RATIO 各算一次
    rsum_v_N = RSUM(v, N)
    vwap_N = RSUM(c * v, N) / (rsum_v_N + EPS)

    obv = (np.sign(c.diff()).fillna(0.0) * v).cumsum()
    obv_ma_N = obv.rolling(N, min_periods=max(2, N // 2)).mean()
    ad = (((c - l) - (h - c)) / ((h - l) + EPS) * v).cumsum()
    ad_ma_N = ad.rolling(N, min_periods=max(2, N // 2)).mean()

    oi_value = oi * c
    oi_pct_N, oi_pct_M = pctc(oi, N), pctc(oi, M)
    oi_ma_fast = oi.rolling(P['D2'], min_periods=max(2, P['D2'] // 2)).mean()
    oi_ma_slow = oi.rolling(P['D7'], min_periods=max(2, P['D7'] // 2)).mean()
    oiv_pct_N = pctc(oi_value, N)
    # 【性能】oi_value 的两条 EMA 原本在 OI_VALUE_EMA_CROSS / EXIT_OI_VALUE_MA_DEAD_CROSS 各算一次
    oiv_ema_M = oi_value.ewm(span=M, adjust=False).mean()
    oiv_ema_N = oi_value.ewm(span=N, adjust=False).mean()

    rng = (h - l) + EPS
    lw = (np.minimum(o, c) - l) / rng
    uw = (h - np.maximum(o, c)) / rng
    clpos = (c - l) / rng

    # ---------- 缓存的排名/分位 ----------
    rk_ret_N, rk_ret_M = RK(ret_N), RK(ret_M)
    rk_v = RK(v)
    oi_value_rank = RK(oi_value)
    fr_rank = RK(fr)
    rk_oi = RK(oi)
    rk_oipct_N, rk_oipct_M = RK(oi_pct_N), RK(oi_pct_M)
    atr_pct = atr_N / (c + EPS)
    rk_atr = RK(atr_pct)
    rngp = (maxH_N - minL_N) / (c + EPS)
    rk_rng = RK(rngp)
    ext_slow = c / (ma_slow + EPS) - 1.0
    rk_ext_slow = RK(ext_slow)

    F = {}

    F['FILTER_LIQUIDITY_OI_VALUE'] = oi_value > QT(oi_value, 0.30)
    F['FILTER_LIQUIDITY_VOLUME'] = v > QT(v, 0.30)
    F['FILTER_NOT_OVERCROWDED'] = (fr_rank < 0.95) & (oi_value_rank < 0.95) & (rk_ret_N < 0.98)
    F['FILTER_TREND_REGIME_UP'] = (c > ma_slow) & (ma_slow > ma_slow.shift(M))

    F['PRICE_MA_STACK'] = (c > ma_fast) & (ma_fast > ma_slow)
    F['PRICE_MULTI_MA_STACK'] = (ma_12h > ma_24h) & (ma_24h > ma_72h) & (ma_72h > ma_7d) & (c > ma_12h)
    F['PRICE_MA_CROSS_UP'] = CU(ma_fast, ma_slow)
    F['PRICE_CLOSE_CROSS_MA_UP'] = CU(c, ma_N)
    F['PRICE_MA_SLOPE_UP'] = ma_N > ma_N.shift(M)
    _sprd = (ma_fast - ma_slow).abs()
    _sprd_r = _sprd / (ma_slow.abs() + EPS)
    F['PRICE_MA_SQUEEZE_UP'] = (RK(_sprd_r).shift(1) < 0.20) & (ma_fast > ma_slow) & (_sprd > _sprd.shift(1))
    F['PRICE_HIGHER_LOWS'] = minL_N > minL_N.shift(N)
    F['PRICE_HIGHER_HIGHS_REAL'] = maxH_N > maxH_N.shift(N)
    F['PRICE_HH_HL_REAL'] = F['PRICE_HIGHER_HIGHS_REAL'] & F['PRICE_HIGHER_LOWS']
    _dslow = (ma_slow - ma_slow.shift(M)).abs()
    F['PRICE_SLOW_FLATTEN_TURN'] = (ma_slow.shift(M) < ma_slow.shift(2 * M)) & (_dslow < QT(_dslow, 0.30))
    _extN = c / (ma_N + EPS) - 1.0
    F['PRICE_HEALTHY_EXTENSION'] = (_extN > 0) & (_extN < QT(_extN, 0.80))
    _dc = c.diff()
    F['PRICE_TREND_STRENGTH_UP'] = RSUM(_dc.clip(lower=0), N) / (RSUM(_dc.abs(), N) + EPS) > 0.60

    up_move, down_move = h.diff(), -l.diff()
    plus_dm = pd.Series(np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index)
    plus_di = 100 * RSUM(plus_dm, N) / (tr_sum + EPS)
    minus_di = 100 * RSUM(minus_dm, N) / (tr_sum + EPS)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di + EPS)) * 100
    adx = dx.rolling(N, min_periods=max(2, N // 2)).mean()
    F['ADX_TREND_UP_REAL'] = (adx > P['ADX_TH']) & (plus_di > minus_di)

    F['MOM_RETURN_STRONG'] = rk_ret_N > 0.80
    F['MOM_ACCELERATION'] = ret_N > ret_N.shift(M)
    F['MOM_BURST'] = rk_ret_M > 0.90
    F['MOM_ZSCORE_STRONG'] = ZS(ret_N) > 1.5
    F['MOM_TURN_POSITIVE'] = (ret_N > 0) & (ret_N.shift(1) <= 0)
    F['MOM_CONSISTENT_UP_BARS'] = (c > c.shift(1)).rolling(N).sum() >= P['K_UP_BARS']
    F['MOM_NEW_HIGH_FREQ'] = (c >= maxH_M.shift(1)).rolling(N).sum() >= P['K_NEWHIGH']
    _recov = c / (minL_N + EPS) - 1.0
    F['MOM_RECOVERY_FROM_LOW'] = _recov > QT(_recov, 0.80)
    F['MOM_PERSISTENCE'] = (ret_N > 0) & (ret_N.shift(N) > 0)
    F['MOM_NOT_OVERHEATED'] = rk_ret_N < 0.95
    _vwm = RSUM(ret_M * v, N) / (rsum_v_N + EPS)
    F['MOM_VOLUME_WEIGHTED'] = RK(_vwm) > 0.80

    F['VOL_RETURN_COMPRESSION'] = RK(ret_1h.rolling(N, min_periods=max(2, N // 2)).std()) < 0.20
    F['VOL_TRUE_RANGE_COMPRESSION'] = rk_atr < 0.20
    F['VOL_RANGE_COMPRESSION_REAL'] = rk_rng < 0.25
    F['VOL_BODY_COMPRESSION'] = RK((c - o).abs().rolling(N, min_periods=max(2, N // 2)).mean()) < 0.20
    F['VOL_ATR_EXPANSION'] = ZS(atr_pct) > 1.0
    F['VOL_SQUEEZE_TO_EXPAND_REAL'] = bs(F['VOL_TRUE_RANGE_COMPRESSION']) & F['VOL_ATR_EXPANSION']
    F['VOL_LOW_TO_HIGH'] = (rk_atr.shift(M) < 0.20) & (rk_atr > 0.60)
    F['VOL_NOT_EXTREME'] = rk_atr < 0.90
    F['VOL_EXTREME_RISK'] = rk_atr > 0.95
    F['VOL_DOWN_SPIKE'] = F['VOL_ATR_EXPANSION'] & (ret_M < 0)

    F['BREAK_N_HIGH_REAL'] = c > maxH_N.shift(1)
    F['BREAK_DONCHIAN_HIGH_EVENT_REAL'] = (c >= maxH_N.shift(1)) & (c.shift(1) < maxH_N.shift(2))
    F['BREAK_BOLLINGER_UPPER'] = c > ma_N + 2 * c.rolling(N, min_periods=max(2, N // 2)).std()
    F['BREAK_RANGE_QUANTILE'] = c > QT(c, 0.90)
    F['BREAK_LONG_CONSOLIDATION_REAL'] = (rk_rng.shift(1) < 0.20) & (c > maxH_N.shift(1))
    F['BREAK_STRONG_CLOSE'] = (c > maxH_N.shift(1)) & (c > o)
    _bd = c / (maxH_N.shift(1) + EPS) - 1.0
    F['BREAK_MEANINGFUL_DISTANCE'] = _bd > QT(_bd, 0.80)
    F['BREAK_FLAT_THEN_BREAK'] = bs(F['VOL_RANGE_COMPRESSION_REAL']) & (c > maxH_N.shift(1))
    F['BREAK_RETEST_HOLD_REAL'] = (c.shift(1) > maxH_N.shift(2)) & (l <= maxH_N.shift(2) * 1.01) & (c > maxH_N.shift(2))
    F['BREAK_SECOND_WAVE_REBREAK'] = (c.shift(M) > maxH_N.shift(M + 1)) & (c.shift(1) < maxH_N.shift(M + 1)) & (
            c > maxH_N.shift(M + 1))
    F['BREAK_INSIDE_BREAK_UP'] = (h.shift(1) < h.shift(2)) & (l.shift(1) > l.shift(2)) & (c > h.shift(1))
    _rpos = (c - minL_N) / ((maxH_N - minL_N) + EPS)
    F['STRUCT_RANGE_POSITION_STRONG'] = _rpos > 0.80
    F['STRUCT_RANGE_POSITION_WEAK'] = _rpos < 0.20
    F['STRUCT_NEAR_HIGH_BASE'] = (c / (maxH_N + EPS) > 0.90) & (l > minL_M)
    F['STRUCT_SUPPORT_HOLD'] = (l <= ma_N * 1.01) & (c > ma_N)
    F['BREAKDOWN_N_LOW'] = c < minL_N.shift(1)
    F['BREAKDOWN_DONCHIAN_LOW_REAL'] = c <= minL_N.shift(1)
    F['BREAK_FAIL_LEVEL'] = (c.shift(1) > maxH_N.shift(2)) & (c < maxH_N.shift(2))

    _bodyr = c / (o + EPS) - 1.0
    # 【性能】RK(_bodyr) 原本在 KLINE_STRONG_GREEN / KLINE_STRONG_RED 各算一次(滚动rank最贵)
    rk_bodyr = RK(_bodyr)
    F['KLINE_STRONG_GREEN'] = (c > o) & (rk_bodyr > 0.90)
    F['KLINE_SMALL_GREEN_ACCUM'] = ((c > o) & (_bodyr > 0) & (_bodyr < 0.01)).rolling(N).sum() >= P['K_SMALL_GREEN']
    F['KLINE_LONG_LOWER_WICK_REAL'] = lw > 0.50
    F['KLINE_LOWER_WICK_RECLAIM'] = (lw > 0.50) & (c > o)
    F['KLINE_LONG_UPPER_WICK'] = uw > 0.50
    F['KLINE_CLOSE_UPPER_RANGE'] = clpos > 0.70
    F['KLINE_CLOSE_LOWER_RANGE'] = clpos < 0.30
    F['KLINE_LOW_OPEN_RECOVER'] = (o < c.shift(1)) & (c > o)
    F['KLINE_HIGH_OPEN_STRONG'] = (o > c.shift(1)) & (c > o)
    F['KLINE_CONSEC_STRONG_CLOSE'] = ((c > o) & (c > ma_M)).rolling(N).sum() >= P['K_STRONG_CLOSE']
    F['KLINE_BODY_STRENGTH_UP_REAL'] = (c > o) & ((c - o) / rng > 0.60)
    F['KLINE_BODY_STRENGTH_DOWN_REAL'] = (c < o) & ((o - c) / rng > 0.60)
    F['KLINE_STRONG_RED'] = (c < o) & (rk_bodyr < 0.10)
    F['KLINE_RED_BREAK_MA'] = (c < o) & (c < ma_N)
    F['KLINE_SMALL_RED_PULLBACK'] = (c < o) & (RK((c - o).abs()) < 0.30) & (c > ma_N)
    F['KLINE_GAP_UP'] = o > c.shift(1) * (1 + P['GAP_TH'])
    F['KLINE_GAP_DOWN'] = o < c.shift(1) * (1 - P['GAP_TH'])
    F['KLINE_DOWN_EXHAUST'] = (ret_N.shift(1) < 0) & (c < o) & (_bodyr.abs() < P['EXHAUST_TH']) & (c > l)
    F['KLINE_SHOOTING_STAR'] = (ret_N.shift(1) > 0) & (uw > 0.50) & (c < o)
    F['KLINE_HAMMER'] = (ret_N.shift(1) < 0) & (lw > 0.50) & (c > o)
    F['KLINE_INSIDE_BAR'] = (h < h.shift(1)) & (l > l.shift(1))
    F['KLINE_OUTSIDE_BAR_UP'] = (h > h.shift(1)) & (l < l.shift(1)) & (c > h.shift(1)) & (c > o)
    F['KLINE_OUTSIDE_BAR_DOWN'] = (h > h.shift(1)) & (l < l.shift(1)) & (c < l.shift(1)) & (c < o)
    F['KLINE_ENGULFING_UP'] = (c > o) & (c.shift(1) < o.shift(1)) & (c > o.shift(1)) & (o < c.shift(1))
    F['KLINE_ENGULFING_DOWN'] = (c < o) & (c.shift(1) > o.shift(1)) & (c < o.shift(1)) & (o > c.shift(1))
    F['KLINE_THREE_GREEN_UP'] = ((c > o) & (c.shift(1) > o.shift(1)) & (c.shift(2) > o.shift(2))
                                 & (c > c.shift(1)) & (c.shift(1) > c.shift(2)))
    F['KLINE_THREE_RED_DOWN'] = ((c < o) & (c.shift(1) < o.shift(1)) & (c.shift(2) < o.shift(2))
                                 & (c < c.shift(1)) & (c.shift(1) < c.shift(2)))

    F['VOLUME_MA_UP'] = vma_fast > vma_slow
    F['VOLUME_SPIKE'] = v > QT(v, 0.95)
    F['VOLUME_Z_SPIKE'] = ZS(v) > 2.0
    F['VOLUME_RANK_HIGH'] = rk_v > 0.80
    F['VOLUME_DRY_UP'] = rk_v < 0.20
    F['VOLUME_EXPAND_PRICE_UP'] = F['VOLUME_SPIKE'] & (ret_M > 0)
    F['VOLUME_EXPAND_PRICE_DOWN'] = F['VOLUME_SPIKE'] & (ret_M < 0)
    F['VOLUME_CONFIRM_BREAK'] = F['BREAK_N_HIGH_REAL'] & F['VOLUME_SPIKE']
    F['VOLUME_DRY_PULLBACK'] = (ret_M < 0) & F['VOLUME_DRY_UP'] & (c > ma_N)
    F['VOLUME_TREND_UP'] = RSUM(v * (c > c.shift(1)), N) > RSUM(v * (c < c.shift(1)), N)
    _vpc = pctc(v, M)
    F['VOLUME_PRICE_CORR_POS'] = _vpc.rolling(W, min_periods=mp).corr(ret_M) > 0
    F['VOLUME_LEADS_PRICE'] = _vpc.shift(M).rolling(W, min_periods=mp).corr(ret_M) > P['CORR_TH']
    F['VWAP_ABOVE'] = c > vwap_N
    F['VWAP_CROSS_UP'] = CU(c, vwap_N)
    F['VWAP_RECLAIM'] = (l < vwap_N) & (c > vwap_N)
    F['VOLUME_AT_BREAKOUT_LEVEL'] = (c > maxH_N.shift(1)) & (v > vma_N * P['VOL_BREAK_MULT'])
    F['VOLUME_DRY_AT_SUPPORT'] = (l <= ma_N * 1.01) & F['VOLUME_DRY_UP'] & (c > ma_N)
    F['VOLUME_HIGH_CLOSE_STRONG'] = F['KLINE_CLOSE_UPPER_RANGE'] & F['VOLUME_SPIKE']
    F['OBV_UP'] = obv > obv_ma_N
    F['OBV_CROSS_UP'] = CU(obv, obv_ma_N)
    F['OBV_BULL_DIV'] = (c < c.shift(N)) & (obv > obv.shift(N))
    F['OBV_BEAR_DIV'] = (c > c.shift(N)) & (obv < obv.shift(N))
    F['AD_LINE_UP'] = ad > ad_ma_N
    F['AD_LINE_BULL_DIV'] = (c < c.shift(N)) & (ad > ad.shift(N))
    F['AD_LINE_BEAR_DIV'] = (c > c.shift(N)) & (ad < ad.shift(N))
    F['VOLUME_UP_RATIO'] = RSUM(v * (c > o), N) / (rsum_v_N + EPS) > 0.60
    F['VOLUME_CLIMAX_UP'] = (ret_N > 0) & (rk_v > 0.98)
    F['VOLUME_CLIMAX_DOWN'] = (ret_N < 0) & (rk_v > 0.98)

    F['OI_MA_CROSS_UP'] = CU(oi_ma_fast, oi_ma_slow)
    F['OI_MA_UP'] = oi_ma_fast > oi_ma_slow
    F['OI_SLOPE_UP'] = oi > oi.shift(N)
    F['OI_SURGE_RANK'] = rk_oipct_N > 0.80
    _zoi = ZS(oi)
    F['OI_ZSCORE_UP'] = (_zoi > 0) & (_zoi > _zoi.shift(M))
    F['OI_NEW_HIGH'] = oi >= oi.rolling(N, min_periods=max(2, N // 2)).max()
    F['OI_LOW_TO_UP'] = (rk_oi.shift(M) < 0.20) & (rk_oi > 0.50)
    F['OI_PERSISTENT_UP'] = (oi_pct_M > 0).rolling(N).sum() >= P['K_OI_UP']
    F['OI_ACCELERATION'] = oi_pct_M > oi_pct_N / (N / M)
    F['OI_HIDDEN_RISE_PRICE_FLAT'] = (ret_N.abs() < P['FLAT_TH']) & (rk_oipct_N > 0.80)
    F['OI_BREAKOUT_CONFIRM'] = F['BREAK_N_HIGH_REAL'] & (rk_oipct_M > 0.70)
    F['OI_PRICE_BOTH_UP'] = (ret_N > 0) & (oi_pct_N > 0)
    F['OI_PRICE_UP_OI_DOWN'] = (ret_N > 0) & (oi_pct_N < 0)
    F['OI_PRICE_DOWN_OI_UP'] = (ret_N < 0) & (oi_pct_N > 0)
    F['OI_PRICE_DOWN_OI_DOWN'] = (ret_N < 0) & (oi_pct_N < 0)
    F['OI_LEADS_PRICE'] = oi_pct_M.shift(M).rolling(W, min_periods=mp).corr(ret_M) > P['CORR_TH']
    _coi = oi_pct_M.rolling(W, min_periods=mp).corr(ret_M)
    F['OI_PRICE_CORR_TURN_POS'] = (_coi > 0) & (_coi.shift(1) <= 0)
    F['OI_TOP_DIVERGENCE'] = (c / (maxH_N + EPS) > 0.98) & (oi < oi.rolling(M, min_periods=2).max() * 0.90)
    F['OI_BOTTOM_DIVERGENCE'] = (c / (minL_N + EPS) < 1.03) & (oi > oi.rolling(M, min_periods=2).min() * 1.05)
    F['OI_RESET_THEN_UP'] = (oi_pct_N.shift(N) < 0) & (oi_pct_M > 0)
    F['OI_AMOUNT_UP_VALUE_NOT_HOT'] = (oi_pct_N > 0) & (oi_value_rank < 0.90)
    F['OI_VALUE_UP_AMOUNT_NOT_UP'] = (oiv_pct_N > 0) & (oi_pct_N <= 0)
    F['OI_VALUE_EMA_CROSS'] = CU(oiv_ema_M, oiv_ema_N)
    F['OI_VALUE_SURGE'] = RK(oiv_pct_N) > 0.90
    F['OI_ROC_BURST'] = oi_pct_N > QT(oi_pct_N, 0.95)
    F['OI_ROC_PEAK'] = (oi_pct_N >= oi_pct_N.rolling(M, min_periods=2).max()) & (oi_pct_N > P['OI_ROC_TH'])
    F['OI_EXTREME_PRICE_NOT_HOT'] = (rk_oi > 0.95) & ((c / (ma_N + EPS) - 1) < P['OI_HOT_TH'])
    F['OI_VALUE_HEALTHY'] = (oi_value_rank > 0.50) & (oi_value_rank < 0.90)
    F['OI_VALUE_HOT_EXTREME'] = oi_value_rank > 0.90
    F['OI_DROP_EXTREME'] = rk_oipct_N < 0.05

    F['FR_MILD'] = (fr_rank > 0.10) & (fr_rank < 0.90)
    F['FR_LOW_NEG'] = (fr_rank < 0.20) | (fr < 0)
    F['FR_VERY_LOW'] = fr_rank < 0.05
    F['FR_ZERO_ZONE'] = fr.abs() < fr.rolling(W, min_periods=mp).std() * 0.20
    F['FR_TURN_POSITIVE'] = (fr > 0) & (fr.shift(1) <= 0)
    F['FR_TURN_NEGATIVE'] = (fr < 0) & (fr.shift(1) >= 0)
    F['FR_RECOVERY_FROM_LOW'] = (fr_rank.shift(M) < 0.10) & (fr_rank > fr_rank.shift(1))
    F['FR_SLOPE_RISING'] = fr > fr.shift(N)
    F['FR_SPIKE_UP'] = RK(fr - fr.shift(N)) > 0.90
    F['FR_HIGH_EXTREME'] = fr_rank > 0.90
    F['FR_EXTREME_LOW'] = fr_rank < 0.05
    F['FR_ROLL_OVER_FROM_HIGH'] = (fr_rank < 0.90) & (fr_rank.shift(1) >= 0.90)
    _frstd = fr.rolling(N, min_periods=max(2, N // 2)).std()
    # 【性能】RK(_frstd) 原本在 FR_STABLE / FR_UNSTABLE 各算一次
    rk_frstd = RK(_frstd)
    F['FR_STABLE'] = rk_frstd < 0.30
    F['FR_UNSTABLE'] = rk_frstd > 0.90
    F['FR_PRICE_UP_NOT_HOT'] = (ret_N > 0) & (fr_rank < 0.80)
    F['FR_PRICE_UP_HOT'] = (ret_N > 0) & (fr_rank > 0.90)
    F['FR_RESET_AFTER_HOT'] = (fr_rank.shift(M) > 0.90) & (fr_rank < 0.70)
    F['FR_NEG_PRICE_HOLD'] = (fr < 0) & (c > minL_N) & (c > o)
    F['FR_POS_NOT_HOT'] = (fr > 0) & (fr_rank < 0.80)
    F['FR_PRICE_BULL_DIV'] = (c < c.shift(N)) & (fr > fr.shift(N))
    F['FR_PRICE_BEAR_DIV'] = (c > c.shift(N)) & (fr < fr.shift(N))
    F['FR_COLD_START'] = (rk_ret_N > 0.80) & (fr_rank < 0.50)

    F['ENTRY_ABSORPTION_BREAKOUT_VOLUME'] = (bs(F['VOL_RANGE_COMPRESSION_REAL'])
                                             & bs(F['OI_HIDDEN_RISE_PRICE_FLAT'])
                                             & bs(F['FR_MILD'])
                                             & F['BREAK_N_HIGH_REAL'] & F['VOLUME_SPIKE'])
    F['ENTRY_SILENT_ACCUMULATION'] = (ret_N.abs() < P['SILENT_TH']) & (oi_pct_N > 0) & F['FR_LOW_NEG']
    F['ENTRY_SHORT_SQUEEZE_LAUNCH'] = (bs(F['FR_VERY_LOW']) & bs(F['PRICE_HIGHER_LOWS'])
                                       & bs(F['OI_SLOPE_UP']) & F['BREAK_N_HIGH_REAL'])
    F['ENTRY_SHORT_SQUEEZE_VOLUME'] = (fr < 0) & F['VOLUME_SPIKE'] & (ret_M > 0) & (oi_pct_M < 0)
    F['ENTRY_OI_FLASH_SURGE'] = F['OI_SURGE_RANK'] & F['PRICE_HEALTHY_EXTENSION'] & F['FR_MILD']
    F['ENTRY_BEAR_TRAP_RECLAIM_VOLUME'] = (l < minL_N.shift(1)) & (c > o) & F['VOLUME_SPIKE'] & F['OI_SLOPE_UP']
    F['ENTRY_HIGH_PRESSURE_OI_BREAKOUT'] = (rk_oi > 0.95) & F['BREAK_N_HIGH_REAL'] & F['VOLUME_SPIKE']
    F['ENTRY_TREND_CONFIRM_B'] = F['PRICE_MA_STACK'] & F['OI_MA_UP'] & F['FR_MILD']
    F['ENTRY_PULLBACK_RESTART_VOLUME'] = (bs(F['PRICE_MA_STACK']) & bs(F['VOLUME_DRY_PULLBACK'])
                                          & (oi_pct_N > -0.05) & F['VOLUME_SPIKE'] & (c > maxH_M.shift(1)))
    F['ENTRY_BREAKOUT_RETEST_OI_STABLE'] = F['BREAK_RETEST_HOLD_REAL'] & (oi_pct_N > -0.03) & F['FR_MILD']
    F['ENTRY_FR_RESET_SECOND_WAVE'] = (F['FR_RESET_AFTER_HOT'] & F['PRICE_MA_STACK']
                                       & F['OI_SLOPE_UP'] & F['BREAK_N_HIGH_REAL'])
    F['ENTRY_HEALTHY_ACCELERATION_VOLUME'] = (F['MOM_RETURN_STRONG'] & F['VOLUME_SPIKE']
                                              & F['OI_SURGE_RANK'] & F['FR_PRICE_UP_NOT_HOT'])
    F['ENTRY_UNCROWDED_MOMENTUM'] = F['MOM_RETURN_STRONG'] & F['FR_POS_NOT_HOT'] & F['OI_VALUE_HEALTHY']
    F['ENTRY_OI_LEAD_MOMENTUM'] = bs(F['OI_LEADS_PRICE']) & bs(F['STRUCT_NEAR_HIGH_BASE']) & F['MOM_BURST']
    F['ENTRY_OI_LEAD_SQUEEZE'] = F['OI_LEADS_PRICE'] & F['VOL_RANGE_COMPRESSION_REAL']
    F['ENTRY_FUNDING_COLD_START_TREND'] = F['FR_COLD_START'] & F['PRICE_MA_STACK'] & F['OI_MA_UP']
    F['ENTRY_PRICE_UP_OI_DOWN_SPOT_PUSH'] = F['PRICE_MA_STACK'] & F['OI_PRICE_UP_OI_DOWN'] & F['FR_LOW_NEG']
    F['ENTRY_LONG_CONSOL_OI_VOLUME_CONFIRM'] = F['BREAK_LONG_CONSOLIDATION_REAL'] & F['OI_SURGE_RANK'] & F[
        'VOLUME_SPIKE']
    F['ENTRY_OI_LOW_RECOVERY_BREAK'] = bs(F['OI_LOW_TO_UP']) & F['BREAK_N_HIGH_REAL']
    F['ENTRY_BOTTOM_STABILIZE'] = F['OI_BOTTOM_DIVERGENCE'] & F['FR_LOW_NEG'] & F['PRICE_HIGHER_LOWS']
    F['ENTRY_VWAP_RECLAIM_OI'] = F['VWAP_RECLAIM'] & F['OI_SLOPE_UP'] & F['FR_MILD']
    F['ENTRY_OBV_BULL_DIV_BREAK'] = bs(F['OBV_BULL_DIV']) & F['BREAK_N_HIGH_REAL'] & F['VOLUME_SPIKE']
    F['ENTRY_INSIDE_BREAK_VOLUME'] = bs(F['KLINE_INSIDE_BAR']) & (c > h.shift(1)) & F['VOLUME_SPIKE']
    F['ENTRY_OUTSIDE_BAR_VOLUME'] = F['KLINE_OUTSIDE_BAR_UP'] & F['VOLUME_SPIKE'] & F['OI_SLOPE_UP']
    F['ENTRY_HAMMER_VOLUME_OI'] = F['KLINE_HAMMER'] & F['VOLUME_SPIKE'] & F['OI_SLOPE_UP'] & F['FR_LOW_NEG']
    F['ENTRY_THREE_GREEN_VOLUME_OI'] = F['KLINE_THREE_GREEN_UP'] & F['VOLUME_MA_UP'] & F['OI_MA_UP']

    F['EXIT_CHANDDELIER_N'] = c < (maxH_N.shift(1) - P['ATR_K'] * atr_N)
    F['EXIT_CLOSE_BELOW_MA_N'] = c < ma_N
    F['EXIT_MA_DEAD_CROSS'] = CD(ma_fast, ma_slow)
    F['EXIT_BREAK_N_LOW'] = c < minL_N.shift(1)
    F['EXIT_HIGH_VOLUME_BREAKDOWN'] = F['BREAKDOWN_N_LOW'] & F['VOLUME_SPIKE']
    F['EXIT_MULTI_MA_BREAK'] = (c < ma_24h) & (c < ma_48h) & (c < ma_72h)
    F['EXIT_HIGH_STALL_BREAK'] = ((c > ma_slow)
                                  & ((c < maxH_N.shift(1)).rolling(M).sum() >= M)
                                  & (c < minL_M.shift(1)))
    F['EXIT_WEAK_CLOSE_STREAK'] = ((c < o) & (c < ma_M)).rolling(N).sum() >= P['K_WEAK_CLOSE']
    F['EXIT_MOMENTUM_DEATH'] = (c < ma_fast) & (ret_N < 0) & F['BREAKDOWN_N_LOW']
    F['EXIT_LONG_LIQUIDATION_CASCADE_REAL'] = (rk_ret_M < 0.05) & F['VOLUME_SPIKE'] & F['OI_DROP_EXTREME']
    F['EXIT_TOP_OI_DIVERGENCE'] = F['OI_TOP_DIVERGENCE'] & F['FR_HIGH_EXTREME']
    F['EXIT_PRICE_NEWHIGH_OI_WEAK'] = (c >= maxH_N.shift(1)) & (oi < oi.shift(M))
    F['EXIT_FR_EXTREME_HIGH'] = fr_rank > 0.95
    F['EXIT_FR_ROLL_OVER'] = F['FR_ROLL_OVER_FROM_HIGH']
    F['EXIT_FOMO_TOP'] = (fr_rank > 0.95) & (rk_ext_slow > 0.95) & (ret_M <= 0)
    F['EXIT_PARABOLIC_EXTENSION'] = rk_ext_slow > 0.95
    F['EXIT_SHORT_SURGE_EXTREME'] = rk_ret_M > 0.98
    F['EXIT_VOL_EXTREME_DOWN'] = F['VOL_EXTREME_RISK'] & (ret_M < 0)
    F['EXIT_OI_VALUE_EXTREME'] = oi_value_rank > 0.95
    F['EXIT_OI_DROP_AFTER_HIGH'] = (c / (maxH_N + EPS) > 0.95) & F['OI_DROP_EXTREME']
    F['EXIT_OI_ROC_PEAK'] = F['OI_ROC_PEAK']
    _crowd = (rk_ext_slow.fillna(0.5) + rk_ret_M.fillna(0.5)
              + fr_rank.fillna(0.5) + oi_value_rank.fillna(0.5))
    F['EXIT_CROWDING_SCORE_HIGH'] = _crowd > QT(_crowd, 0.90)
    F['EXIT_GAP_DOWN_RISK'] = F['KLINE_GAP_DOWN'] & (c < ma_fast)
    F['EXIT_STRONG_RED_BAR_VOLUME'] = F['KLINE_STRONG_RED'] & F['VOLUME_SPIKE'] & (c < ma_fast)
    F['EXIT_RANGE_POSITION_WEAK'] = F['STRUCT_RANGE_POSITION_WEAK']
    F['EXIT_FAILED_BREAKOUT'] = F['BREAK_FAIL_LEVEL']
    F['EXIT_FR_SPIKE_THEN_COOL'] = bs(F['FR_SPIKE_UP']) & (fr < fr.shift(1))
    F['EXIT_OI_VALUE_MA_DEAD_CROSS'] = CD(oiv_ema_M, oiv_ema_N)
    F['EXIT_UPPER_WICK_REJECTION'] = (c / (maxH_N + EPS) > 0.95) & F['KLINE_LONG_UPPER_WICK'] & F['VOLUME_SPIKE']
    F['EXIT_HIGH_VOLUME_STALL'] = (c / (maxH_N + EPS) > 0.95) & F['VOLUME_SPIKE'] & (ret_M <= 0)
    F['EXIT_VOLUME_DIVERGENCE_BEAR'] = (c > maxH_N.shift(1)) & (v < vma_N)
    F['EXIT_OBV_DIVERGENCE_BEAR'] = F['OBV_BEAR_DIV']
    F['EXIT_VWAP_BREAK'] = CD(c, vwap_N)
    F['EXIT_VOLUME_CLIMAX'] = (rk_v > 0.98) & F['KLINE_LONG_UPPER_WICK'] & F['KLINE_CLOSE_LOWER_RANGE']
    F['EXIT_CROWDED_BLOWOFF'] = ((rk_ret_N > 0.98) & (rk_v > 0.98)
                                 & (F['FR_HIGH_EXTREME'] | F['OI_VALUE_HOT_EXTREME']))
    F['EXIT_VOLUME_SPIKE_DOWN'] = F['VOLUME_EXPAND_PRICE_DOWN'] & (c < ma_fast)
    F['EXIT_MICRO_DISTRIBUTION'] = ((c / (maxH_N + EPS) > 0.95) & F['VOLUME_SPIKE']
                                    & F['KLINE_CLOSE_LOWER_RANGE']
                                    & (F['OI_DROP_EXTREME'] | F['FR_HIGH_EXTREME']))

    out = {}
    for k_, s in F.items():
        out[k_] = np.ascontiguousarray(s.fillna(False).to_numpy(dtype=bool))
    aux = dict(atr=atr_N.to_numpy(float))
    return out, aux


# ---------- 路径依赖出场（只能做出场） ----------
def path_exit_specs(P):
    z = dict(use_fixed=False, fixed_pct=0.0, use_barlow=False,
             use_atr=False, atr_k=0.0, use_time=False, time_n=0, time_th=0.0,
             use_gb=False, gb_th=0.0, use_lock=False, lock_th=0.0, lock_trail=0.0,
             static=None)
    S = {}
    S['EXIT_FIXED_STOP'] = {**z, 'use_fixed': True, 'fixed_pct': P['STOP_PCT']}
    S['EXIT_ATR_TRAILING'] = {**z, 'use_atr': True, 'atr_k': P['ATR_K']}
    S['EXIT_ENTRY_BAR_LOW_BREAK'] = {**z, 'use_barlow': True}
    S['EXIT_TIME_STOP'] = {**z, 'use_time': True, 'time_n': P['TIME_STOP_BARS'],
                           'time_th': P['TIME_STOP_TH']}
    S['EXIT_PROFIT_GIVEBACK'] = {**z, 'use_gb': True, 'gb_th': P['GIVEBACK_TH']}
    S['EXIT_PROFIT_LOCK_TRAIL'] = {**z, 'use_lock': True, 'lock_th': P['LOCK_TH'],
                                   'lock_trail': P['LOCK_TRAIL']}
    S['EXIT_FULL_PROTECTION'] = {**z, 'use_fixed': True, 'fixed_pct': P['STOP_PCT'],
                                 'use_atr': True, 'atr_k': P['ATR_K'],
                                 'use_time': True, 'time_n': P['TIME_STOP_BARS'],
                                 'time_th': P['TIME_STOP_TH'],
                                 'use_gb': True, 'gb_th': P['GIVEBACK_TH'],
                                 'static': 'EXIT_HIGH_VOLUME_BREAKDOWN'}
    return S


# ======================================================================
# 5. 绩效计算 (修复了单笔夏普输出，以及补齐切片笔数防歧义)
# ======================================================================
# ---------------------------------------------------------------
# 【性能】快速统计基元：逐位复刻 scipy / numpy 的运算顺序，
#         只是剥掉了它们外层昂贵的 Python 校验与容器构造。
#         输入约定：1-D float64、无 NaN/Inf（调用方已用 isfinite 过滤）。
# ---------------------------------------------------------------
_Q_KEYS = (('ret_q1', 'trades_q1'), ('ret_q2', 'trades_q2'),
           ('ret_q3', 'trades_q3'), ('ret_q4', 'trades_q4'))


def _skew_unbiased(x, n):
    """等价 scipy.stats.skew(x, bias=False)（n >= 3）"""
    mean = x.mean()
    d = x - mean
    d2 = d * d
    m2 = d2.mean()
    m3 = (d2 * d).mean()          # 与 scipy._moment 的平方求幂顺序一致
    if not (m2 > 0.0):
        return np.nan
    return float(math.sqrt((n - 1.0) * n) / (n - 2.0) * m3 / m2 ** 1.5)


def _kurt_unbiased(x, n):
    """等价 scipy.stats.kurtosis(x, bias=False, fisher=True)（n >= 4）"""
    mean = x.mean()
    d = x - mean
    d2 = d * d
    m2 = d2.mean()
    m4 = (d2 * d2).mean()
    if not (m2 > 0.0):
        return np.nan
    nval = 1.0 / (n - 2) / (n - 3) * ((n ** 2 - 1.0) * m4 / m2 ** 2.0 - 3 * (n - 1) ** 2.0)
    # scipy 先 +3 再 -3（fisher），此处保留同样的浮点路径
    return float((nval + 3.0) - 3.0)


def _corr_pearson(a, b):
    """等价 np.corrcoef(a, b)[0, 1]：复刻 np.cov 的 1/(n-1) 缩放与 clip，
       但不构造 2xN 矩阵、不生成 2x2 协方差矩阵。"""
    n = a.shape[0]
    if n < 2:
        return np.nan
    da = a - a.mean()
    db = b - b.mean()
    inv = 1.0 / (n - 1)
    c00 = float(np.dot(da, da)) * inv
    c11 = float(np.dot(db, db)) * inv
    if not (c00 > 0.0) or not (c11 > 0.0):
        return np.nan
    c01 = float(np.dot(da, db)) * inv
    r = (c01 / math.sqrt(c00)) / math.sqrt(c11)
    if r > 1.0:
        return 1.0
    if r < -1.0:
        return -1.0
    return r


def _quantile05_linear(x):
    """等价 np.percentile(x, 5)（method='linear'）：用 O(N) 的 partition
       取代 O(N logN) 全量排序，虚拟下标与 _lerp 分支完全照抄 numpy。"""
    n = x.shape[0]
    vi = (5 / 100.0) * (n - 1)
    prev = int(vi)                      # vi >= 0，等价 floor
    g = vi - prev
    nxt = prev + 1
    if nxt >= n:
        return float(np.partition(x, prev)[prev]) if n > 1 else float(x[0])
    part = np.partition(x, (prev, nxt))
    a = float(part[prev])
    b = float(part[nxt])
    diff = b - a
    if g < 0.5:
        return a + diff * g
    return b - diff * (1.0 - g)


def _median_fast(x):
    """等价 np.median(x)（输入无 NaN）：直接用 partition 取中位序统计量"""
    n = x.shape[0]
    half = n // 2
    if n & 1:
        return float(np.partition(x, half)[half])
    part = np.partition(x, (half - 1, half))
    return float((part[half - 1] + part[half]) / 2.0)


def trade_stats(rets, ent, ext, bar_minutes, n_bars, start_idx=0, bh_rets=None, prefix=''):
    d = {}
    T = int(len(rets))
    d[prefix + 'trades'] = T

    # 【新增】加入 pt_sharpe (单笔夏普), trades_q1~4
    empty_keys = ['win_rate', 'sum_ret', 'avg_ret', 'med_ret', 'std_ret', 'tstat', 'sharpe', 'pt_sharpe',
                  'profit_factor', 'max_dd', 'avg_hold_h', 'exposure', 'max_win', 'max_loss',
                  'skew', 'kurt', 'cvar_5', 'equity_r2', 'corr_btc', 'down_market_win_rate',
                  'win_hold_bars', 'loss_hold_bars',
                  'ret_q1', 'ret_q2', 'ret_q3', 'ret_q4',
                  'trades_q1', 'trades_q2', 'trades_q3', 'trades_q4']

    if T == 0:
        for k in empty_keys:
            d[prefix + k] = np.nan
        return d

    pos_mask = rets > 0                       # 【性能】胜负掩码只算一次，后面复用
    d[prefix + 'win_rate'] = float(pos_mask.mean() * 100)
    d[prefix + 'sum_ret'] = float(rets.sum() * 100)
    d[prefix + 'avg_ret'] = float(rets.mean() * 100)
    d[prefix + 'med_ret'] = float(_median_fast(rets) * 100)

    sd = float(rets.std(ddof=1) * 100) if T > 1 else np.nan
    d[prefix + 'std_ret'] = sd

    years = max(n_bars * bar_minutes / (365 * 24 * 60.0), 0.0001)
    trades_per_year = T / years

    # 【修复】同时保留纯单笔夏普(pt_sharpe)用于统计学推导，以及年化夏普用于展示
    if sd and sd > 1e-8:
        d[prefix + 'tstat'] = float(d[prefix + 'avg_ret'] / (sd / math.sqrt(T)))
        per_trade_sharpe = d[prefix + 'avg_ret'] / sd
        d[prefix + 'pt_sharpe'] = float(per_trade_sharpe)
        d[prefix + 'sharpe'] = float(per_trade_sharpe * math.sqrt(trades_per_year))
    else:
        d[prefix + 'tstat'] = np.nan
        d[prefix + 'pt_sharpe'] = np.nan
        d[prefix + 'sharpe'] = np.nan

    g = rets[pos_mask].sum()
    b = -rets[rets < 0].sum()
    d[prefix + 'profit_factor'] = float(min(g / b, 999.0)) if b > 0 else (999.0 if g > 0 else 0.0)

    eq = np.concatenate(([0.0], np.cumsum(rets)))
    d[prefix + 'max_dd'] = float((np.maximum.accumulate(eq) - eq).max() * 100)
    hold = (ext - ent).astype(float)
    d[prefix + 'avg_hold_h'] = float(hold.mean() * bar_minutes / 60.0)
    d[prefix + 'exposure'] = float(hold.sum() / max(n_bars, 1) * 100)
    d[prefix + 'max_win'] = float(rets.max() * 100)
    d[prefix + 'max_loss'] = float(rets.min() * 100)

    try:
        if sd > 1e-8:
            d[prefix + 'skew'] = _skew_unbiased(rets, T) if T >= 3 else np.nan
            d[prefix + 'kurt'] = _kurt_unbiased(rets, T) if T >= 4 else np.nan
        else:
            d[prefix + 'skew'], d[prefix + 'kurt'] = 0.0, 0.0
    except Exception:
        d[prefix + 'skew'], d[prefix + 'kurt'] = np.nan, np.nan

    if T > 2:
        p05 = _quantile05_linear(rets)
        cvar_arr = rets[rets <= p05]
        d[prefix + 'cvar_5'] = float(cvar_arr.mean() * 100) if len(cvar_arr) > 0 else np.nan
        ideal = np.arange(len(eq))

        # T > 2 => len(eq) >= 4 => np.std(ideal) >= 1.118，恒大于 1e-8，无需再算
        if np.std(eq) > 1e-8:
            corr = _corr_pearson(eq, ideal)
            if not np.isnan(corr):
                d[prefix + 'equity_r2'] = float(corr ** 2) if corr > 0 else float(-(corr ** 2))
            else:
                d[prefix + 'equity_r2'] = np.nan
        else:
            d[prefix + 'equity_r2'] = np.nan
    else:
        d[prefix + 'cvar_5'], d[prefix + 'equity_r2'] = np.nan, np.nan

    if bh_rets is not None and T > 2:
        if np.std(rets) > 1e-8 and np.std(bh_rets) > 1e-8:
            corr_btc = _corr_pearson(rets, bh_rets)
            d[prefix + 'corr_btc'] = float(corr_btc) if not np.isnan(corr_btc) else np.nan
        else:
            d[prefix + 'corr_btc'] = np.nan

        down_idx = bh_rets < 0
        if down_idx.sum() > 0:
            d[prefix + 'down_market_win_rate'] = float((rets[down_idx] > 0).mean() * 100)
        else:
            d[prefix + 'down_market_win_rate'] = np.nan
    else:
        d[prefix + 'corr_btc'], d[prefix + 'down_market_win_rate'] = np.nan, np.nan

    win_idx = pos_mask
    loss_idx = rets <= 0
    d[prefix + 'win_hold_bars'] = float(hold[win_idx].mean()) if win_idx.sum() > 0 else np.nan
    d[prefix + 'loss_hold_bars'] = float(hold[loss_idx].mean()) if loss_idx.sum() > 0 else np.nan

    relative_ent = ent - start_idx
    chunk_size = max(n_bars / 4.0, 1.0)
    for i in range(4):
        mask = (relative_ent >= i * chunk_size) & (relative_ent < (i + 1) * chunk_size)
        chunk_rets = rets[mask]
        # 【修复】顺手记录每个切片的触发笔数，防止 0.0 歧义
        _rk, _tk = _Q_KEYS[i]
        if len(chunk_rets) > 0:
            d[prefix + _rk] = float(chunk_rets.sum() * 100)
            d[prefix + _tk] = int(len(chunk_rets))
        else:
            d[prefix + _rk] = 0.0
            d[prefix + _tk] = 0

    return d


# ======================================================================
# 6. 单币种全组合挖掘 (彻底移除Parquet，截面特征极速落盘)
# ======================================================================
def mine_symbol(coin, df, cfg, btc_close=None):
    bm = cfg['BAR_MINUTES']
    P = make_params(bm, len(df))

    t0 = time.time()
    F, aux = build_factors(df, P, rank_shift=cfg['RANK_SHIFT'])
    warm = min(P['WARMUP'], len(df) - 100)
    if warm < 0 or len(df) - warm < 200:
        return None, {'total_combos': 0}, 0

    df = df.iloc[warm:].copy()
    rk_gain = df['rank_gain_24h'].to_numpy(float)
    rk_loss = df['rank_loss_24h'].to_numpy(float)

    F = {k: v[warm:] for k, v in F.items()}
    atr = aux['atr'][warm:]

    if btc_close is not None:
        btc_c = btc_close.reindex(df.index).ffill().bfill().to_numpy(float)
    else:
        btc_c = None

    n = len(df)
    kline_days = n * bm / 1440.0
    max_allowed_trades = kline_days * 24.0 * 60

    op = df['open'].to_numpy(float)
    cl = df['close'].to_numpy(float)
    lo = df['low'].to_numpy(float)
    exec_px = np.empty(n, float)
    exec_px[:-1] = op[1:]
    exec_px[-1] = cl[-1]
    cost = 2.0 * (cfg['FEE_RATE'] + cfg['SLIPPAGE'])

    names_all = list(F.keys())
    keep, dens = [], {}
    for k in names_all:
        s = int(F[k].sum())
        dens[k] = s / n
        if s >= cfg['MIN_SIGNALS'] and dens[k] <= cfg['MAX_DENSITY']:
            keep.append(k)

    alias = {}
    if cfg['DEDUPE_IDENTICAL']:
        seen = {}
        uniq = []
        for k in keep:
            hsh = hash(F[k].tobytes())
            if hsh in seen and np.array_equal(F[k], F[seen[hsh]]):
                alias.setdefault(seen[hsh], []).append(k)
            else:
                seen[hsh] = k
                uniq.append(k)
        keep = uniq

    P_EXITS = path_exit_specs(P) if cfg['INCLUDE_PATH_EXITS'] else {}
    entry_names = [k for k in keep]
    exit_names = [k for k in keep] + list(P_EXITS.keys())

    if cfg['ENTRY_PREFIX_FILTER']:
        entry_names = [k for k in entry_names if k.startswith(tuple(cfg['ENTRY_PREFIX_FILTER']))]
    if cfg['EXIT_PREFIX_FILTER']:
        exit_names = [k for k in exit_names if k.startswith(tuple(cfg['EXIT_PREFIX_FILTER']))]

    idx_cache = {k: np.flatnonzero(F[k]).astype(np.int64) for k in keep}
    zeros_static = np.zeros(n, dtype=bool)
    split_bar = int(n * cfg['OOS_SPLIT'])
    max_tr = min(cfg['MAX_TRADES_PER_COMBO'], n // 2 + 2)

    # 【细化统计】初始化漏斗 + 按 mode 计数
    mode_keys = [(f"{m[0]}_{m[2]}" if m[0] != 'original' else 'original') for m in FILTER_MODES]
    stats = {
        'total_combos': len(entry_names) * len(exit_names),
        'skip_same_factor': 0,
        'skip_zero_trades': 0,
        'skip_too_few': 0,
        'skip_too_many': 0,
        'mode_pass_counts': {mk: 0 for mk in mode_keys},
    }
    # 【性能/内存】列式装配：不再堆积上百万个 dict，改为每列一个 list。
    #   列顺序由第一行的键序确定 —— 与 pd.DataFrame(List[Dict]) 的
    #   fast_unique_multiple_list_gen(sort=False) 行为完全一致（各行键集合恒等）。
    col_data = None
    done = 0

    for en in entry_names:
        eidx = idx_cache[en]
        for xn in exit_names:
            done += 1
            if (not cfg['ALLOW_SAME_FACTOR']) and xn == en:
                stats['skip_same_factor'] += 1
                continue

            if xn in P_EXITS:
                spec = P_EXITS[xn]
                st = F[spec['static']] if (spec['static'] and spec['static'] in F) else zeros_static
                if HAS_NUMBA:
                    ent, ext = _core_path(eidx, st, cl, lo, atr, n, cfg['COOLDOWN_BARS'], max_tr,
                                          exec_px,
                                          spec['use_fixed'], spec['fixed_pct'],
                                          spec['use_barlow'],
                                          spec['use_atr'], spec['atr_k'],
                                          spec['use_time'], spec['time_n'], spec['time_th'],
                                          spec['use_gb'], spec['gb_th'],
                                          spec['use_lock'], spec['lock_th'], spec['lock_trail'])
                else:
                    ent, ext = _match_path_np(eidx, st, cl, lo, atr, exec_px, n,
                                              cfg['COOLDOWN_BARS'], max_tr, spec)
                x_dens = np.nan
            else:
                if HAS_NUMBA:
                    ent, ext = _core_static(eidx, idx_cache[xn], n, cfg['COOLDOWN_BARS'], max_tr)
                else:
                    ent, ext = _match_static_ss(eidx, idx_cache[xn], n, cfg['COOLDOWN_BARS'], max_tr)
                x_dens = dens.get(xn, np.nan)

            if ent.size == 0:
                stats['skip_zero_trades'] += 1
                continue

            rets = exec_px[ext] / exec_px[ent] - 1.0 - cost
            ok = np.isfinite(rets)
            ent_base, ext_base, rets_base = ent[ok], ext[ok], rets[ok]

            for mode_name, rank_col, threshold in FILTER_MODES:
                if mode_name == 'original':
                    ent_f, ext_f, rets_f = ent_base, ext_base, rets_base
                    filter_label = 'original'
                else:
                    if mode_name == 'top':
                        entry_ranks = rk_gain[ent_base]
                    else:
                        entry_ranks = rk_loss[ent_base]

                    mask = entry_ranks <= threshold
                    ent_f = ent_base[mask]
                    ext_f = ext_base[mask]
                    rets_f = rets_base[mask]
                    filter_label = f"{mode_name}_{threshold}"

                # 【细化】拆分拦截原因
                if ent_f.size < cfg['MIN_TRADES_REPORT']:
                    stats['skip_too_few'] += 1
                    continue
                if ent_f.size > max_allowed_trades:
                    stats['skip_too_many'] += 1
                    continue

                # 【细化】记录该 mode 通过
                stats['mode_pass_counts'][filter_label] += 1

                if btc_c is not None:
                    bh_rets_f = btc_c[ext_f] / btc_c[ent_f] - 1.0
                else:
                    bh_rets_f = None

                row = dict(coin=coin, entry_factor=en, exit_factor=xn,
                           filter_mode=filter_label,
                           entry_density=dens.get(en, np.nan), exit_density=x_dens)

                row.update(trade_stats(rets_f, ent_f, ext_f, bm, n_bars=n, start_idx=0, bh_rets=bh_rets_f))

                m_is = ent_f < split_bar
                row.update(trade_stats(rets_f[m_is], ent_f[m_is], ext_f[m_is], bm, n_bars=split_bar,
                                       start_idx=0, bh_rets=bh_rets_f[m_is] if bh_rets_f is not None else None,
                                       prefix='is_'))

                row.update(trade_stats(rets_f[~m_is], ent_f[~m_is], ext_f[~m_is], bm, n_bars=n - split_bar,
                                       start_idx=split_bar, bh_rets=bh_rets_f[~m_is] if bh_rets_f is not None else None,
                                       prefix='oos_'))

                if col_data is None:
                    col_data = {k_: [v_] for k_, v_ in row.items()}
                else:
                    for k_, v_ in row.items():
                        col_data[k_].append(v_)

    out = pd.DataFrame(col_data) if col_data else pd.DataFrame()

    # ==================================================================
    # 【新增】无感注入"测试基数"(多重检验搜索空间)，用于事后精确还原 DSR
    #   n_trials_combos : 入场×出场 组合数（原口径，只按此计算会让 DSR 虚高）
    #   n_trials_modes  : 横截面环境过滤模式数（本次为 15）
    #   n_trials_total  : 真实搜索空间 = 组合数 × 模式数  ← 还原 DSR 请用这一列
    #   n_trials_alive  : 实际存留(落盘)的记录数（另一种"有效试验数"口径）
    # 这些列是常量列，不参与任何回测逻辑，纯粹为可复现性服务。
    # ==================================================================
    if not out.empty:
        n_modes = len(FILTER_MODES)
        out['bar_minutes'] = int(bm)
        out['kline_days'] = float(kline_days)
        out['n_trials_combos'] = int(stats['total_combos'])
        out['n_trials_modes'] = int(n_modes)
        out['n_trials_total'] = int(stats['total_combos'] * n_modes)
        out['n_trials_alive'] = int(len(out))

    return out, stats, kline_days


# ======================================================================
# 7. 主流程 (引入多进程架构隔离与调度) - 已解决 OOM 内存泄漏
# ======================================================================
# 【性能】BTC 基准只在 worker 启动时下发一次(而不是每个 task 都 pickle 一遍)，
#         也绝不在子进程里重复解析 BTC 的 1m CSV（那等于每个币重算一次，是负优化）。
_BTC_CLOSE = None


def _init_worker(btc_close):
    global _BTC_CLOSE
    _BTC_CLOSE = btc_close


def mine_symbol_wrapper(args):
    """ 多进程工作节点的包装函数 (核心优化：子进程落地CSV，避免跨进程传大数据) """
    kf, cfg = args
    coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
    oi_f = os.path.join(cfg['DATA_DIR'], f'{coin}_USDT_USDT_5m_oi.csv')
    fr_f = os.path.join(cfg['DATA_DIR'], f'{coin}_USDT_USDT_funding_rates.csv')
    try:
        df = load_symbol(os.path.join(cfg['DATA_DIR'], kf), oi_f, fr_f, cfg['BAR_MINUTES'])
        if len(df) < 800:
            # 统一返回 7 个元素，前两个 0 分别代表 valid_combos, total_saved_trades
            return kf, coin, 0, 0, {'total_combos': 0}, 0, "bar 数不足"

        pairs, stats, kline_days = mine_symbol(coin, df, cfg, _BTC_CLOSE)

        valid_combos = 0
        total_saved_trades = 0

        # --- 【核心修改 1】将数据落盘操作移到子进程内，仅将统计数据传回主进程 ---
        if pairs is not None and not pairs.empty:
            valid_combos = len(pairs)
            total_saved_trades = int(pairs['trades'].sum())

            # 在子进程完成排序
            pairs.sort_values('sum_ret', ascending=False, inplace=True)
            # 在子进程完成写文件
            _atomic_to_csv(pairs, _coin_out_path(cfg['OUT_DIR'], coin))

            # 帮助子进程尽快释放内存
            del pairs
            gc.collect()

        return kf, coin, valid_combos, total_saved_trades, stats, kline_days, "OK"
    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        return kf, coin, 0, 0, {'total_combos': 0}, 0, f"执行异常: {e}\n{tb}"




# ======================================================================
# 7. 主流程 (只落盘单币结果 + 断点续跑 + 原子写入)
# ======================================================================
def _coin_out_path(out_dir, coin):
    """单币结果文件的唯一路径（断点续跑判定依据）"""
    return os.path.join(out_dir, f'pairs_{coin}.csv')


def _atomic_to_csv(df, path):
    """【新增】原子落盘：先写 .tmp 再 os.replace，杜绝中断产生半截文件污染断点"""
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False, encoding='utf-8-sig')
    os.replace(tmp, path)


def _clean_stale_tmp(out_dir):
    """【新增】清理上一次异常中断残留的 .tmp（它们不是有效结果，不能被当作已完成）"""
    n = 0
    for f in os.listdir(out_dir):
        if f.startswith('pairs_') and f.endswith('.csv.tmp'):
            try:
                os.remove(os.path.join(out_dir, f))
                n += 1
            except OSError:
                pass
    return n


# ======================================================================
# 8. 主进程入口 (只落盘单币结果 + 断点续跑 + 原子写入 + 清理Future)
# ======================================================================
def main(cfg=CFG):
    os.makedirs(cfg['OUT_DIR'], exist_ok=True)
    data_dir = cfg['DATA_DIR']
    if not os.path.isdir(data_dir):
        print(f"❌ 数据目录不存在: {data_dir}")
        return

    kfiles = sorted(f for f in os.listdir(data_dir) if f.endswith('_USDT_USDT_1m_kline.csv'))
    if not kfiles:
        print("❌ 未发现 *_USDT_USDT_1m_kline.csv")
        return

    n_tmp = _clean_stale_tmp(cfg['OUT_DIR'])
    if n_tmp:
        print(f"🧹 已清理上次中断残留的临时文件 {n_tmp} 个")

    valid_coins = []
    valid_kfiles = []
    resume_coins = []
    for kf in kfiles:
        coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
        if cfg['COINS'] and coin not in cfg['COINS']:
            continue
        # 断点续跑：已存在 pairs_{coin}.csv 则完全跳过回测
        if os.path.exists(_coin_out_path(cfg['OUT_DIR'], coin)):
            resume_coins.append(coin)
            continue
        valid_coins.append(coin)
        valid_kfiles.append(kf)

    print("=" * 78)
    print(f"  因子挖掘启动 | bar={cfg['BAR_MINUTES']}min | numba={'ON' if HAS_NUMBA else 'OFF'}")
    print(f"⏭️  断点续跑: 已存在结果 {len(resume_coins)} 个 -> 直接跳过回测")
    print(f"🎯 本次计划回测币种个数: {len(valid_coins)} 个")
    if len(valid_coins) <= 30:
        print(f"📜 币种名单: {', '.join(valid_coins)}")
    else:
        print(f"📜 币种名单: {', '.join(valid_coins[:30])} ... 等")
    print("=" * 78)

    if not valid_coins:
        print("✅ 该周期全部币种均已落盘，无需新增计算。")
        return

    btc_file = os.path.join(data_dir, 'BTC_USDT_USDT_1m_kline.csv')
    btc_close = None
    if os.path.exists(btc_file):
        print(f"📈 发现基准数据: {btc_file}，正在加载作 Beta 剥离...")
        try:
            btc_df = pd.read_csv(btc_file)
            btc_t = _pick(btc_df, ['timestamp', 'open_time', 'time', 'ts'], 'kline')
            btc_df['dt'] = pd.to_datetime(btc_df[btc_t], unit='ms', utc=True)
            btc_df = btc_df.drop_duplicates(subset=[btc_t]).sort_values('dt').set_index('dt')
            btc_close = btc_df.resample(f"{cfg['BAR_MINUTES']}min", label='left', closed='left')['close'].last().ffill()
        except Exception as e:
            print(f"⚠️ BTC基准数据加载失败: {e}")
    else:
        print("⚠️ 未发现 BTC_USDT_USDT_1m_kline.csv，将使用标的自身收益作为 fallback")

    tasks = [(kf, cfg) for kf in valid_kfiles]
    n_ok, n_empty, n_fail = 0, 0, 0

    max_workers = min(28, max(1, multiprocessing.cpu_count() - 2))

    if HAS_NUMBA:
        print("🔧 正在主进程预热 Numba JIT 编译器，防止并发竞态...")
        _d_idx = np.array([0, 1], dtype=np.int64)
        _d_bool = np.array([False, True], dtype=bool)
        _d_float = np.array([1.0, 2.0], dtype=float)
        _core_static(_d_idx, _d_idx, 2, 0, 1)
        _core_path(_d_idx, _d_bool, _d_float, _d_float, _d_float, 2, 0, 1, _d_float,
                   False, 0.0, False, False, 0.0, False, 0, 0.0, False, 0.0, False, 0.0, 0.0)
    print(f"\n🚀 启动并发回测... (分配进程核心数: {max_workers})")

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers,
                                               initializer=_init_worker,
                                               initargs=(btc_close,)) as executor:
        futures = {executor.submit(mine_symbol_wrapper, t): t for t in tasks}

        for fut in concurrent.futures.as_completed(futures):
            # --- 【核心修改 2】从字典中删除当前 future 对象的强引用，阻断慢性内存泄漏 ---
            del futures[fut]

            # 解析 7 个返回值（去除了 pairs DataFrame 对象）
            kf, coin, valid_combos, total_saved_trades, stats, kline_days, msg = fut.result()

            if msg != "OK":
                n_fail += 1
                print(f"⚠️ [{coin}] 执行跳过/失败: {msg}")
                continue

            # 如果产出了有效的组合数量
            if valid_combos > 0:
                # ---- 漏斗百分比 ----
                total_combos = stats.get('total_combos', 0)
                skip_same = stats.get('skip_same_factor', 0)
                skip_zero = stats.get('skip_zero_trades', 0)
                skip_too_few = stats.get('skip_too_few', 0)
                skip_too_many = stats.get('skip_too_many', 0)
                mode_pass = stats.get('mode_pass_counts', {})

                stage1_pass = total_combos - skip_same
                pct1 = (stage1_pass / total_combos * 100) if total_combos > 0 else 0

                stage2_pass = stage1_pass - skip_zero
                pct2 = (stage2_pass / stage1_pass * 100) if stage1_pass > 0 else 0

                theoretical_filter_records = stage2_pass * len(FILTER_MODES)
                pct3 = (valid_combos / theoretical_filter_records * 100) if theoretical_filter_records > 0 else 0

                # ---- 找出最严苛 & 最宽松的 mode ----
                if mode_pass:
                    worst_mode = min(mode_pass, key=mode_pass.get)
                    worst_cnt = mode_pass[worst_mode]
                    best_mode = max(mode_pass, key=mode_pass.get)
                    best_cnt = mode_pass[best_mode]
                else:
                    worst_mode, worst_cnt = 'N/A', 0
                    best_mode, best_cnt = 'N/A', 0

                print(f"✅ [{coin}] K线: {kline_days:.1f}天 | "
                      f"总组合:{total_combos} -> "
                      f"去同因子:{stage1_pass}({pct1:.1f}%) -> "
                      f"有交易:{stage2_pass}({pct2:.1f}%) -> "
                      f"存留:{valid_combos}/{theoretical_filter_records}({pct3:.1f}%) | "
                      f"不足3笔:{skip_too_few} | 超限:{skip_too_many} | "
                      f"最严苛:{worst_mode}({worst_cnt}) | "
                      f"最宽松:{best_mode}({best_cnt}) | "
                      f"测试基数:{total_combos}×{len(FILTER_MODES)}={total_combos * len(FILTER_MODES)} | "
                      f"总笔数: {total_saved_trades}")

                n_ok += 1
            else:
                n_empty += 1
                print(f"✅ [{coin}] 执行完毕，但未产出有效组合(不落盘，下次仍会重试)。")

    print("\n" + "=" * 78)
    print(f"🏁 本轮结束 | 新增落盘: {n_ok} 个 | 断点跳过: {len(resume_coins)} 个 | "
          f"无有效结果: {n_empty} 个 | 失败: {n_fail} 个")
    print(f"📁 单币结果目录: {os.path.abspath(cfg['OUT_DIR'])}")
    print("ℹ️  pairs_ALL.csv / pairs_CROSS_COIN_SUMMARY.csv 已取消在此生成；")
    print("    请运行 rebuild_pairs_summary.py，由 pairs_<coin>.csv 精确还原(含真实测试基数的 DSR)。")
    print("=" * 78)


if __name__ == '__main__':
    import copy

    # 定义你需要运行的周期列表
    target_bar_minutes = [1, 5, 15, 30, 60]
    target_bar_minutes.reverse()
    for bm in target_bar_minutes:
        print(f"\n\n" + "★" * 78)
        print(f"★ 正在启动批量回测任务: {bm} 分钟级别数据")
        print("★" * 78)

        # 复制一份全局配置，避免在循环中污染原始配置字典
        run_cfg = copy.deepcopy(CFG)

        # 修改当前任务的 K线周期
        run_cfg['BAR_MINUTES'] = bm

        # 【关键】动态修改输出目录，防止不同周期的文件互相覆盖
        run_cfg['OUT_DIR'] = f'./factor_out_{bm}m'

        # 调用主函数执行
        main(run_cfg)