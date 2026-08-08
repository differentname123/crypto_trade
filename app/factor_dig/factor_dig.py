# -*- coding: utf-8 -*-
"""
================================================================================
 ALT-COIN LAUNCH FACTOR MINER  (山寨启动 · 因子挖掘 / 两两组合回测)
--------------------------------------------------------------------------------
 · 全量计算因子池 -> 每根 bar 一个 bool signal
 · 所有因子两两有序组合 (A进场,B出场) != (B进场,A出场)
 · 纯做多、每笔等名义仓位、收益率【加总不复利】
 · 结果全量落盘 CSV，含 IS/OOS 切分与跨币种稳健性
 · [终极定稿] 2张底层长表 + 1张宏观看板，支撑 DSR/True N/Beta剥离 证伪
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

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ======================================================================
# 0. 全局配置
# ======================================================================
CFG = dict(
    DATA_DIR='../data',
    OUT_DIR='./factor_out_15m',

    # --- 采样与执行 ---
    BAR_MINUTES=15,  # 1 也能跑，但组合数×数据量会非常重；建议 5/15
    FEE_RATE=0.0005,  # 单边手续费
    SLIPPAGE=0.0005,  # 单边滑点(山寨务必给足)
    COOLDOWN_BARS=0,  # 平仓后冷却多少根才允许再入场
    FORCE_CLOSE_AT_END=True,

    # --- 因子行为 ---
    RANK_SHIFT=0,  # rank_W 是否 shift(1)。0=不shift(推荐) 1=按你原方案
    DEDUPE_IDENTICAL=True,  # 自动合并完全相同的因子(别名)
    MIN_SIGNALS=20,  # 信号数少于此的因子丢弃
    MAX_DENSITY=0.995,  # 信号密度高于此的因子丢弃(退化为常真)
    INCLUDE_PATH_EXITS=True,  # 是否纳入路径依赖出场因子

    # --- 组合与输出 ---
    ALLOW_SAME_FACTOR=False,  # 是否允许 A进A出
    MAX_TRADES_PER_COMBO=100000,
    MIN_TRADES_REPORT=3,  # 少于此笔数不写入结果(设0=全写)
    OOS_SPLIT=0.70,  # 前70%样本内 后30%样本外
    ENTRY_PREFIX_FILTER=None,  # 例: ('ENTRY_','PRICE_','OI_') 只用这些前缀当进场
    EXIT_PREFIX_FILTER=None,  # 例: ('EXIT_','BREAKDOWN_','KLINE_')
    COINS=None,  # None=全部；或 ['PEPE','WIF']

    # --- 因子体检 ---
    FWD_HORIZONS_H=(4, 12, 24, 72),
)

EPS = 1e-12

# ======================================================================
# 1. numba 可选加速
# ======================================================================
try:
    from numba import njit

    HAS_NUMBA = True
except Exception:
    HAS_NUMBA = False


def _core_static(entry_flag, exit_flag, n, cooldown, max_trades):
    ent = np.empty(max_trades, dtype=np.int64)
    ext = np.empty(max_trades, dtype=np.int64)
    k = 0
    i = 0
    while i < n - 1 and k < max_trades:
        if entry_flag[i]:
            j = i + 1
            found = -1
            while j < n:
                if exit_flag[j]:
                    found = j
                    break
                j += 1
            if found < 0:
                found = n - 1
            ent[k] = i
            ext[k] = found
            k += 1
            i = found + 1 + cooldown
        else:
            i += 1
    return ent[:k], ext[:k]


def _core_path(entry_flag, static_exit, close, low, atr, n, cooldown, max_trades,
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
    i = 0
    while i < n - 1 and k < max_trades:
        if entry_flag[i]:
            e = i
            ep = exec_px[e]
            el = low[e]
            peak = close[e]
            peak_prof = 0.0
            j = e + 1
            hit = -1
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
                    a = atr[j]
                    if a == a and cj < peak - atr_k * a:  # a==a 过滤 NaN
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
            i = hit + 1 + cooldown
        else:
            i += 1
    return ent[:k], ext[:k]


if HAS_NUMBA:
    _core_static = njit(cache=True, nogil=True)(_core_static)
    _core_path = njit(cache=True, nogil=True)(_core_path)


def _match_static_ss(entry_idx, exit_idx, n, cooldown, max_trades):
    """无 numba 时的跳跃匹配: O(交易数 * log n)"""
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
    """无 numba 时的分块路径扫描"""
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
    agg = k.resample(bar, label='left', closed='left').agg(
        open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'),
        volume=('volume', 'sum'))
    agg['close'] = agg['close'].ffill()
    agg = agg[agg['close'].notna()]
    agg['open'] = agg['open'].fillna(agg['close'])
    agg['high'] = agg['high'].fillna(agg['close'])
    agg['low'] = agg['low'].fillna(agg['close'])
    agg['volume'] = agg['volume'].fillna(0.0)

    oi = pd.read_csv(oi_file)
    ot = _pick(oi, ['timestamp', 'time', 'ts'], 'oi')
    oc = _pick(oi, ['oi_amount', 'openInterest', 'open_interest',
                    'sumOpenInterest', 'oi'], 'oi')
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

    # 只砍前导 NaN，保持时间网格规整(next_open 假设才成立)
    fv = df[['oi_amount', 'funding_rate']].apply(lambda s: s.first_valid_index())
    start = max([x for x in fv.tolist() if x is not None], default=df.index[0])
    df = df.loc[start:].copy()
    df[['oi_amount', 'funding_rate']] = df[['oi_amount', 'funding_rate']].ffill()
    df = df.dropna(subset=['oi_amount', 'funding_rate'])
    for c in ['open', 'high', 'low', 'close']:
        df = df[df[c] > 0]
    return df


# ======================================================================
# 3. 参数体系（全部按小时折算成 bar 数）
# ======================================================================
def make_params(bar_minutes, n_rows):
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))
    P = {}
    P['BPH'] = B(1)
    P['N'] = B(24)  # 主回看周期 24h
    P['M'] = B(4)  # 辅助短周期 4h
    P['W'] = B(24 * 30)  # 滚动统计窗 30d
    P['H12'], P['H24'], P['H48'] = B(12), B(24), B(48)
    P['H72'], P['H168'] = B(72), B(168)
    P['D2'], P['D7'] = B(48), B(168)

    # 样本不够时自动收缩统计窗
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
        # --- 路径依赖出场参数 ---
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
# 4. 因子库（全量因子池实现）
# ======================================================================
def build_factors(df, P, rank_shift=0):
    W, N, M = P['W'], P['N'], P['M']
    mp = P['MINP_W']

    o, h, l, c = df['open'], df['high'], df['low'], df['close']
    v, oi, fr = df['volume'], df['oi_amount'], df['funding_rate']

    def RK(s):  # rank_W —— 含当前值(因果安全)，可选 shift
        r = s.rolling(W, min_periods=mp).rank(pct=True)
        return r.shift(rank_shift) if rank_shift else r

    def ZS(s):
        m = s.rolling(W, min_periods=mp).mean()
        sd = s.rolling(W, min_periods=mp).std()
        return (s - m) / (sd + EPS)

    def QT(s, p):  # q_W —— 阈值必须完全来自过去
        return s.rolling(W, min_periods=mp).quantile(p).shift(1)

    def CU(a, b):
        return (a > b) & (a.shift(1) <= b.shift(1))

    def CD(a, b):
        return (a < b) & (a.shift(1) >= b.shift(1))

    def bs(s, k=1):  # bool shift
        return s.shift(k, fill_value=False)

    def pctc(s, n):
        return s.pct_change(n).replace([np.inf, -np.inf], np.nan)

    def RSUM(s, n):
        return s.rolling(n, min_periods=max(2, n // 2)).sum()

    def MA(n):
        return c.rolling(n, min_periods=max(2, n // 2)).mean()

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
    vwap_N = RSUM(c * v, N) / (RSUM(v, N) + EPS)

    obv = (np.sign(c.diff()).fillna(0.0) * v).cumsum()
    obv_ma_N = obv.rolling(N, min_periods=max(2, N // 2)).mean()
    ad = (((c - l) - (h - c)) / ((h - l) + EPS) * v).cumsum()
    ad_ma_N = ad.rolling(N, min_periods=max(2, N // 2)).mean()

    oi_value = oi * c
    oi_pct_N, oi_pct_M = pctc(oi, N), pctc(oi, M)
    oi_ma_fast = oi.rolling(P['D2'], min_periods=max(2, P['D2'] // 2)).mean()
    oi_ma_slow = oi.rolling(P['D7'], min_periods=max(2, P['D7'] // 2)).mean()
    oiv_pct_N = pctc(oi_value, N)

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

    # ===== 一、通用过滤 =====
    F['FILTER_LIQUIDITY_OI_VALUE'] = oi_value > QT(oi_value, 0.30)
    F['FILTER_LIQUIDITY_VOLUME'] = v > QT(v, 0.30)
    F['FILTER_NOT_OVERCROWDED'] = (fr_rank < 0.95) & (oi_value_rank < 0.95) & (rk_ret_N < 0.98)
    F['FILTER_TREND_REGIME_UP'] = (c > ma_slow) & (ma_slow > ma_slow.shift(M))

    # ===== 二、价格趋势与结构 =====
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
    plus_di = 100 * RSUM(plus_dm, N) / (tr_sum + EPS)  # 注: 原式用 atr_N(均值)量纲不对，此处修正为 TR 求和
    minus_di = 100 * RSUM(minus_dm, N) / (tr_sum + EPS)
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di + EPS)) * 100
    adx = dx.rolling(N, min_periods=max(2, N // 2)).mean()
    F['ADX_TREND_UP_REAL'] = (adx > P['ADX_TH']) & (plus_di > minus_di)

    # ===== 三、动量 =====
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
    _vwm = RSUM(ret_M * v, N) / (RSUM(v, N) + EPS)
    F['MOM_VOLUME_WEIGHTED'] = RK(_vwm) > 0.80

    # ===== 四、波动率 =====
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

    # ===== 五、突破 / 平台 / 结构 =====
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

    # ===== 六、K线形态 =====
    _bodyr = c / (o + EPS) - 1.0
    F['KLINE_STRONG_GREEN'] = (c > o) & (RK(_bodyr) > 0.90)
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
    F['KLINE_STRONG_RED'] = (c < o) & (RK(_bodyr) < 0.10)
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

    # ===== 七、成交量与量价 =====
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
    F['VOLUME_UP_RATIO'] = RSUM(v * (c > o), N) / (RSUM(v, N) + EPS) > 0.60
    F['VOLUME_CLIMAX_UP'] = (ret_N > 0) & (rk_v > 0.98)
    F['VOLUME_CLIMAX_DOWN'] = (ret_N < 0) & (rk_v > 0.98)

    # ===== 八、OI 资金面 =====
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
    F['OI_VALUE_EMA_CROSS'] = CU(oi_value.ewm(span=M, adjust=False).mean(),
                                 oi_value.ewm(span=N, adjust=False).mean())
    F['OI_VALUE_SURGE'] = RK(oiv_pct_N) > 0.90
    F['OI_ROC_BURST'] = oi_pct_N > QT(oi_pct_N, 0.95)
    F['OI_ROC_PEAK'] = (oi_pct_N >= oi_pct_N.rolling(M, min_periods=2).max()) & (oi_pct_N > P['OI_ROC_TH'])
    F['OI_EXTREME_PRICE_NOT_HOT'] = (rk_oi > 0.95) & ((c / (ma_N + EPS) - 1) < P['OI_HOT_TH'])
    F['OI_VALUE_HEALTHY'] = (oi_value_rank > 0.50) & (oi_value_rank < 0.90)
    F['OI_VALUE_HOT_EXTREME'] = oi_value_rank > 0.90
    F['OI_DROP_EXTREME'] = rk_oipct_N < 0.05

    # ===== 九、Funding Rate 情绪 =====
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
    F['FR_STABLE'] = RK(_frstd) < 0.30
    F['FR_UNSTABLE'] = RK(_frstd) > 0.90
    F['FR_PRICE_UP_NOT_HOT'] = (ret_N > 0) & (fr_rank < 0.80)
    F['FR_PRICE_UP_HOT'] = (ret_N > 0) & (fr_rank > 0.90)
    F['FR_RESET_AFTER_HOT'] = (fr_rank.shift(M) > 0.90) & (fr_rank < 0.70)
    F['FR_NEG_PRICE_HOLD'] = (fr < 0) & (c > minL_N) & (c > o)
    F['FR_POS_NOT_HOT'] = (fr > 0) & (fr_rank < 0.80)
    F['FR_PRICE_BULL_DIV'] = (c < c.shift(N)) & (fr > fr.shift(N))
    F['FR_PRICE_BEAR_DIV'] = (c > c.shift(N)) & (fr < fr.shift(N))
    F['FR_COLD_START'] = (rk_ret_N > 0.80) & (fr_rank < 0.50)

    # ===== 十、交叉入场 =====
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

    # ===== 十一、出场与风险（静态部分） =====
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
    F['EXIT_OI_VALUE_MA_DEAD_CROSS'] = CD(oi_value.ewm(span=M, adjust=False).mean(),
                                          oi_value.ewm(span=N, adjust=False).mean())
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

    # 统一转 bool ndarray
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
# 5. 绩效计算
# ======================================================================
def trade_stats(rets, ent, ext, bar_minutes, n_bars, prefix=''):
    d = {}
    T = int(len(rets))
    d[prefix + 'trades'] = T
    if T == 0:
        for k in ['win_rate', 'sum_ret', 'avg_ret', 'med_ret', 'std_ret', 'tstat', 'sharpe',
                  'profit_factor', 'max_dd', 'avg_hold_h', 'exposure', 'max_win', 'max_loss']:
            d[prefix + k] = np.nan
        return d
    d[prefix + 'win_rate'] = float((rets > 0).mean() * 100)
    d[prefix + 'sum_ret'] = float(rets.sum() * 100)
    d[prefix + 'avg_ret'] = float(rets.mean() * 100)
    d[prefix + 'med_ret'] = float(np.median(rets) * 100)
    sd = float(rets.std(ddof=1) * 100) if T > 1 else np.nan
    d[prefix + 'std_ret'] = sd
    d[prefix + 'tstat'] = float(d[prefix + 'avg_ret'] / (sd / math.sqrt(T))) if (sd and sd > 0) else np.nan
    d[prefix + 'sharpe'] = float(d[prefix + 'avg_ret'] / sd) if (sd and sd > 0) else np.nan
    g = rets[rets > 0].sum()
    b = -rets[rets < 0].sum()
    d[prefix + 'profit_factor'] = float(g / b) if b > 0 else (np.inf if g > 0 else np.nan)
    eq = np.concatenate(([0.0], np.cumsum(rets)))
    d[prefix + 'max_dd'] = float((np.maximum.accumulate(eq) - eq).max() * 100)
    hold = (ext - ent).astype(float)
    d[prefix + 'avg_hold_h'] = float(hold.mean() * bar_minutes / 60.0)
    d[prefix + 'exposure'] = float(hold.sum() / max(n_bars, 1) * 100)
    d[prefix + 'max_win'] = float(rets.max() * 100)
    d[prefix + 'max_loss'] = float(rets.min() * 100)
    return d


# ======================================================================
# 6. 单币种全组合挖掘
# ======================================================================
def mine_symbol(coin, df, cfg, btc_close=None):
    bm = cfg['BAR_MINUTES']
    P = make_params(bm, len(df))

    t0 = time.time()
    F, aux = build_factors(df, P, rank_shift=cfg['RANK_SHIFT'])
    warm = min(P['WARMUP'], len(df) - 100)
    if warm < 0 or len(df) - warm < 200:
        print(f"    ! 数据过短(有效 {len(df) - max(warm, 0)} 根)，跳过")
        return None, None, None, 0, 0.0, 0, 0
    df = df.iloc[warm:].copy()
    F = {k: v[warm:] for k, v in F.items()}
    atr = aux['atr'][warm:]

    # 获取时间戳用于逐笔流水表
    timestamps = df.index.to_numpy()
    kline_days = len(df) * bm / 1440.0
    max_allowed_trades = kline_days * 24  # 每天最多平均24笔(平均1小时1笔)

    # 对齐 BTC 价格序列
    if btc_close is not None:
        btc_c = btc_close.reindex(df.index).ffill().to_numpy(float)
    else:
        btc_c = None

    n = len(df)
    op = df['open'].to_numpy(float)
    cl = df['close'].to_numpy(float)
    lo = df['low'].to_numpy(float)
    exec_px = np.empty(n, float)
    exec_px[:-1] = op[1:]
    exec_px[-1] = cl[-1]
    cost = 2.0 * (cfg['FEE_RATE'] + cfg['SLIPPAGE'])

    # ---------- 因子筛选 + 去重 ----------
    names_all = list(F.keys())
    keep, dens = [], {}
    for k in names_all:
        s = int(F[k].sum())
        dens[k] = s / n
        if s >= cfg['MIN_SIGNALS'] and dens[k] <= cfg['MAX_DENSITY']:
            keep.append(k)
    dropped = [k for k in names_all if k not in keep]

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

    # ---------- 因子体检: 信号后前瞻收益 ----------
    diag = []
    hz = {f'fwd_{hh}h': max(1, int(hh * 60 / bm)) for hh in cfg['FWD_HORIZONS_H']}
    fwd = {kk: (np.concatenate([cl[v:], np.full(v, np.nan)]) / cl - 1.0) for kk, v in hz.items()}
    for k in keep:
        idx = np.flatnonzero(F[k])
        row = dict(coin=coin, factor=k, n_signal=int(idx.size),
                   density=float(idx.size / n), alias='|'.join(alias.get(k, [])))
        for kk, arr in fwd.items():
            a = arr[idx]
            a = a[np.isfinite(a)]
            row[kk + '_mean'] = float(np.mean(a) * 100) if a.size else np.nan
            row[kk + '_t'] = float(np.mean(a) / (np.std(a, ddof=1) / math.sqrt(a.size)) * 1.0) \
                if a.size > 2 and np.std(a, ddof=1) > 0 else np.nan
        diag.append(row)
    diag_df = pd.DataFrame(diag)

    # ---------- 组合 ----------
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

    total = len(entry_names) * len(exit_names)

    rows = []
    trades_list = []  # 收集逐笔交易
    done = 0
    t1 = time.time()
    for en in entry_names:
        ea, eidx = F[en], idx_cache[en]
        for xn in exit_names:
            done += 1
            if (not cfg['ALLOW_SAME_FACTOR']) and xn == en:
                continue

            if xn in P_EXITS:
                spec = P_EXITS[xn]
                st = F[spec['static']] if (spec['static'] and spec['static'] in F) else zeros_static
                if HAS_NUMBA:
                    ent, ext = _core_path(ea, st, cl, lo, atr, n, cfg['COOLDOWN_BARS'], max_tr,
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
                    ent, ext = _core_static(ea, F[xn], n, cfg['COOLDOWN_BARS'], max_tr)
                else:
                    ent, ext = _match_static_ss(eidx, idx_cache[xn], n, cfg['COOLDOWN_BARS'], max_tr)
                x_dens = dens.get(xn, np.nan)

            if ent.size == 0:
                continue

            # --- 过滤器: 频率过高直接跳过，节省内存和后续计算 ---
            if ent.size < cfg['MIN_TRADES_REPORT']:
                continue
            if ent.size > max_allowed_trades:
                continue
            # ----------------------------------------------------

            rets = exec_px[ext] / exec_px[ent] - 1.0 - cost
            ok = np.isfinite(rets)
            ent, ext, rets = ent[ok], ext[ok], rets[ok]

            if ent.size < cfg['MIN_TRADES_REPORT']:
                continue

            # ================= 构建逐笔交易明细 =================
            if btc_c is not None:
                bh_rets = btc_c[ext] / btc_c[ent] - 1.0
            else:
                bh_rets = cl[ext] / cl[ent] - 1.0

            ent_dt = timestamps[ent]
            ext_dt = timestamps[ext]

            # [完美规避Pandas Series广播报错：先用标量构建DataFrame，再转Category降维内存]
            trades_df = pd.DataFrame({
                'combo_id': f"{en}|{xn}",
                'coin': coin,
                'entry_time': ent_dt,
                'exit_time': ext_dt,
                'net_return': rets.astype(np.float32),
                'benchmark_return': bh_rets.astype(np.float32)
            })
            trades_df['combo_id'] = trades_df['combo_id'].astype('category')
            trades_df['coin'] = trades_df['coin'].astype('category')

            trades_list.append(trades_df)
            # ==========================================================

            row = dict(coin=coin, entry_factor=en, exit_factor=xn,
                       entry_density=dens.get(en, np.nan), exit_density=x_dens)
            row.update(trade_stats(rets, ent, ext, bm, n))
            m_is = ent < split_bar
            row.update(trade_stats(rets[m_is], ent[m_is], ext[m_is], bm, split_bar, prefix='is_'))
            row.update(trade_stats(rets[~m_is], ent[~m_is], ext[~m_is], bm, n - split_bar, prefix='oos_'))
            rows.append(row)

    all_trades_df = pd.concat(trades_list, ignore_index=True) if trades_list else pd.DataFrame()
    actual_combos = len(rows)
    total_saved_trades = sum(r['trades'] for r in rows) if rows else 0
    return pd.DataFrame(rows), diag_df, all_trades_df, total, kline_days, actual_combos, total_saved_trades


# ======================================================================
# 7. 主流程
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

    print("=" * 78)
    print(f"  因子挖掘启动 | bar={cfg['BAR_MINUTES']}min | numba={'ON' if HAS_NUMBA else 'OFF'}")
    print("=" * 78)

    # ---- 加载全局 BTC 基准数据作 Beta 剥离 ----
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
    # ----------------------------------------------------

    all_pairs, all_diag, all_trades = [], [], []
    all_coin_stats = []  # 记录所有币种的宏观统计信息
    total_trials_tested = 0  # 记录全局测试总次数

    for kf in kfiles:
        try:
            coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
            if cfg['COINS'] and coin not in cfg['COINS']:
                continue
            oi_f = os.path.join(data_dir, f'{coin}_USDT_USDT_5m_oi.csv')
            fr_f = os.path.join(data_dir, f'{coin}_USDT_USDT_funding_rates.csv')
            if not (os.path.exists(oi_f) and os.path.exists(fr_f)):
                print(f"⚠️  {coin} 数据不完整，跳过")
                continue

            print(f"\n🚀 [{coin}]")
            try:
                df = load_symbol(os.path.join(data_dir, kf), oi_f, fr_f, cfg['BAR_MINUTES'])
            except Exception as e:
                print(f"    ! 加载失败: {e}")
                continue
            if len(df) < 800:
                print(f"    ! bar 数不足 ({len(df)})，跳过")
                continue
            print(f"    · {df.index[0]} ~ {df.index[-1]}  共 {len(df)} 根 bar")

            # 传递 btc_close 以便对齐数据
            pairs, diag, trades_df, trials, kline_days, actual_combos, total_saved_trades = mine_symbol(coin, df, cfg,
                                                                                                        btc_close)

            # 收集该币种统计信息
            theoretical_combos = trials
            retention_rate = actual_combos / theoretical_combos if theoretical_combos > 0 else 0
            avg_trade_freq = (total_saved_trades / actual_combos / kline_days) if (
                        actual_combos > 0 and kline_days > 0) else 0

            all_coin_stats.append({
                'coin': coin,
                'kline_days': round(kline_days, 2),
                'theoretical_combos': theoretical_combos,
                'actual_combos': actual_combos,
                'retention_rate': round(retention_rate, 4),
                'total_saved_trades': total_saved_trades,
                'avg_trades_per_day': round(avg_trade_freq, 4)
            })

            # 搜索空间取最大组合数（不随币种翻倍）
            total_trials_tested = max(total_trials_tested, trials)

            if pairs is None or pairs.empty:
                continue

            all_trades.append(trades_df)
            pairs.sort_values('sum_ret', ascending=False, inplace=True)
            pairs.to_csv(os.path.join(cfg['OUT_DIR'], f'pairs_{coin}.csv'),
                         index=False, encoding='utf-8-sig')
            diag.to_csv(os.path.join(cfg['OUT_DIR'], f'factor_diag_{coin}.csv'),
                        index=False, encoding='utf-8-sig')
            all_pairs.append(pairs)
            all_diag.append(diag)

            top = pairs.head(10)
            print("    ── TOP10 (按累加总收益) ──")
            for _, r in top.iterrows():
                print(f"      {r['entry_factor'][:34]:<34} -> {r['exit_factor'][:30]:<30} "
                      f"| N={int(r['trades']):>5} | Σ={r['sum_ret']:>9.1f}% "
                      f"| 胜率={r['win_rate']:>5.1f}% | 均={r['avg_ret']:>6.2f}% "
                      f"| OOSΣ={r['oos_sum_ret'] if pd.notna(r['oos_sum_ret']) else float('nan'):>8.1f}%")
        except Exception as e:
            traceback.print_exc()
            print(f"❌ [{coin}] 处理失败: {e}")
            continue

    if not all_pairs:
        print("\n⚠️ 没有任何有效结果。")
        return

    big = pd.concat(all_pairs, ignore_index=True)
    big.to_csv(os.path.join(cfg['OUT_DIR'], 'pairs_ALL.csv'), index=False, encoding='utf-8-sig')
    pd.concat(all_diag, ignore_index=True).to_csv(
        os.path.join(cfg['OUT_DIR'], 'factor_diag_ALL.csv'), index=False, encoding='utf-8-sig')

    # ================= 记录并打印全局统计信息 =================
    if all_coin_stats:
        stats_df = pd.DataFrame(all_coin_stats)
        stats_df.rename(columns={
            'coin': '币种',
            'kline_days': 'K线天数',
            'theoretical_combos': '理论组合数量',
            'actual_combos': '实际回测的组合数量',
            'retention_rate': '保留率',
            'total_saved_trades': '最终保存的交易数量',
            'avg_trades_per_day': '平均交易频率(次/天)'
        }, inplace=True)
        stats_df.to_csv(os.path.join(cfg['OUT_DIR'], 'backtest_stats_ALL.csv'), index=False, encoding='utf-8-sig')

        print("\n" + "=" * 78)
        print("📊 各币种回测过滤与统计信息")
        print("=" * 78)
        print(stats_df.to_string(index=False))
    # ==========================================================

    # ================= 生成表1：全局逐笔交易流水 =================
    all_trades = [t for t in all_trades if t is not None and not t.empty]
    if all_trades:
        all_trades_big = pd.concat(all_trades, ignore_index=True)
        # 精准计算并发数：同一 combo 在同一 entry_time 触发的币种数量
        concurrent_counts = all_trades_big.groupby(['combo_id', 'entry_time']).size().rename('concurrent_signals')
        all_trades_big = all_trades_big.merge(concurrent_counts, left_on=['combo_id', 'entry_time'], right_index=True,
                                              how='left')

        cols_t1 = ['combo_id', 'coin', 'entry_time', 'exit_time', 'net_return', 'benchmark_return',
                   'concurrent_signals']
        all_trades_big[cols_t1].to_csv(os.path.join(cfg['OUT_DIR'], 'trades_ALL.csv'), index=False,
                                       encoding='utf-8-sig')
    else:
        all_trades_big = pd.DataFrame()
    # ====================================================================

    # 跨币种稳健性汇总
    g = big.groupby(['entry_factor', 'exit_factor'])
    summ = g.agg(n_coins=('coin', 'nunique'),
                 total_trades=('trades', 'sum'),
                 sum_ret_all=('sum_ret', 'sum'),
                 mean_sum_ret=('sum_ret', 'mean'),
                 median_sum_ret=('sum_ret', 'median'),
                 mean_avg_ret=('avg_ret', 'mean'),
                 mean_win_rate=('win_rate', 'mean'),
                 mean_max_dd=('max_dd', 'mean'),
                 mean_hold_h=('avg_hold_h', 'mean'),
                 oos_sum_all=('oos_sum_ret', 'sum')).reset_index()
    pos_rate = g.apply(lambda x: (x['sum_ret'] > 0).mean()).rename('coin_positive_rate').reset_index()
    summ = summ.merge(pos_rate, on=['entry_factor', 'exit_factor'])
    summ['score'] = (summ['mean_avg_ret'].fillna(0)
                     * np.sqrt(summ['total_trades'].clip(lower=1))
                     * summ['coin_positive_rate'])
    summ.sort_values('score', ascending=False, inplace=True)
    summ.to_csv(os.path.join(cfg['OUT_DIR'], 'pairs_CROSS_COIN_SUMMARY.csv'),
                index=False, encoding='utf-8-sig')

    # ================= 生成表2：组合时序切片长表 =================
    if not all_trades_big.empty:
        all_trades_big['date'] = pd.to_datetime(all_trades_big['entry_time']).dt.date
        daily_agg = all_trades_big.groupby(['combo_id', 'date']).agg(
            daily_return=('net_return', 'sum'),
            active_coins=('coin', 'nunique')
        ).reset_index()
        daily_agg = daily_agg.sort_values(['combo_id', 'date'])
        daily_agg['daily_nav'] = daily_agg.groupby('combo_id')['daily_return'].cumsum() + 1.0

        cols_t2 = ['combo_id', 'date', 'daily_nav', 'daily_return', 'active_coins']
        daily_agg[cols_t2].to_csv(os.path.join(cfg['OUT_DIR'], 'combo_timeseries_ALL.csv'), index=False,
                                  encoding='utf-8-sig')
    # ====================================================================

    # ================= 生成表3：宏观统计档案看板 =================
    combo_profile = big.groupby(['entry_factor', 'exit_factor']).agg(
        total_trades=('trades', 'sum'),
        is_oos_sharpe=('oos_sharpe', 'mean')
    ).reset_index()
    combo_profile['combo_id'] = combo_profile['entry_factor'] + '|' + combo_profile['exit_factor']

    if not all_trades_big.empty:
        def calc_profile_stats(group):
            conc = group['concurrent_signals'].values
            true_n = np.sum(1.0 / conc) if len(conc) > 0 else 0
            rets = group['net_return'].values
            if len(rets) > 2:
                skew = pd.Series(rets).skew()
                kurt = pd.Series(rets).kurtosis()
            else:
                skew, kurt = 0.0, 0.0
            return pd.Series({'true_n_trades': true_n, 'skew': skew, 'kurt': kurt})

        stats = all_trades_big.groupby('combo_id').apply(calc_profile_stats).reset_index()
        combo_profile = combo_profile.merge(stats, on='combo_id', how='left')
    else:
        combo_profile['true_n_trades'] = np.nan
        combo_profile['skew'] = np.nan
        combo_profile['kurt'] = np.nan

    def calc_dsr(row, total_trials):
        sr = row['is_oos_sharpe']
        T = row['total_trades']
        skew = row.get('skew', 0)
        kurt = row.get('kurt', 0)
        if pd.isna(sr) or T <= 0 or total_trials <= 1:
            return np.nan
        if pd.isna(skew): skew = 0
        if pd.isna(kurt): kurt = 0

        emsr = np.sqrt(2 * np.log(total_trials))
        var_sr = (1 - skew * sr + (kurt + 2) / 4 * sr ** 2) / T
        if var_sr <= 0: var_sr = 1e-6
        z = (sr - emsr) / np.sqrt(var_sr)
        dsr = 0.5 * (1 + math.erf(z / np.sqrt(2)))
        return dsr

    combo_profile['total_trials'] = total_trials_tested
    combo_profile['deflated_sharpe'] = combo_profile.apply(lambda r: calc_dsr(r, total_trials_tested), axis=1)

    cols_t3 = ['combo_id', 'total_trials', 'true_n_trades', 'is_oos_sharpe', 'deflated_sharpe']
    for c in cols_t3:
        if c not in combo_profile.columns:
            combo_profile[c] = np.nan
    combo_profile[cols_t3].to_csv(os.path.join(cfg['OUT_DIR'], 'Combo_Profile_ALL.csv'), index=False,
                                  encoding='utf-8-sig')
    # ====================================================================

    print("\n" + "=" * 78)
    print("🏆 跨币种稳健 TOP20 (score = 均笔收益 × √笔数 × 盈利币种占比)")
    print("=" * 78)
    show = summ.head(20)[['entry_factor', 'exit_factor', 'n_coins', 'total_trades',
                          'mean_avg_ret', 'mean_win_rate', 'coin_positive_rate',
                          'sum_ret_all', 'oos_sum_all', 'score']]
    pd.set_option('display.width', 240)
    pd.set_option('display.max_colwidth', 40)
    print(show.to_string(index=False, float_format=lambda x: f'{x:.3f}'))
    print(f"\n✅ 结果已保存至 {os.path.abspath(cfg['OUT_DIR'])}")
    print("   · backtest_stats_ALL.csv        [新增] 各币种回测过滤与统计信息")
    print("   · pairs_<COIN>.csv              单币全组合明细")
    print("   · pairs_ALL.csv                 全部币种全组合明细")
    print("   · pairs_CROSS_COIN_SUMMARY.csv  跨币种稳健性汇总")
    print("   · factor_diag_<COIN>.csv        单因子体检(信号数/密度/前瞻收益t值)")
    print("   · trades_ALL.csv                [表1] 全局逐笔交易流水(含并发与BTC基准)")
    print("   · combo_timeseries_ALL.csv      [表2] 组合时序切片长表(按日聚合)")
    print("   · Combo_Profile_ALL.csv         [表3] 宏观统计档案看板(含DSR与True N)")


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