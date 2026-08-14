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
 · [新增特性] 对入场信号进行 15 组不同强度的横截面(Rank)环境过滤测试
 · [本次改造] 只落盘 pairs_{coin}.csv.gz；断点续跑 + 原子写入；无感注入"测试基数"
              (pairs_ALL / pairs_CROSS_COIN_SUMMARY 改由独立还原脚本重建)
 · [性能优化] 统计基元标量化(逐位复刻) / 因子基础量去重缓存 /
              列式内存装配 / BTC 基准按 worker 一次性下发   —— 结果不变
 · [新增特性] 支持双向回测：静态信号出场同时输出 Long/Short 两行数据
 · [本次修复①] 新增「资金费率求和」：按真实持仓区间 [入场执行时刻, 出场执行时刻)
              左闭右开，累加该币【真实资金费率结算事件历史】(fr_event)，
              绝不使用 8 小时估算。新增列：fr_sum / fr_avg (含 is_ / oos_ 前缀)。
              符号为原始符号：做多实际成本 = -fr_sum，做空实际收益 = +fr_sum。
 · [本次修复②] 修复 FILTER_MODES「事后过滤」导致的路径依赖(占坑效应)：
              横截面排名过滤已【前置】到入场信号层——先把不达标的 bar 从入场
              候选集中剔除，再交给状态机撮合。每个 filter_mode 现在是一条独立
              的策略路径，劣质信号再也无法占坑挡住达标的优质机会。
              (同一入场因子下过滤结果完全相同的 mode 会自动合并，只撮合一次)
 · [本次因子池改版] ①删除 GAP_TH 及全部跳空脏数据因子；②滚动窗口 W: 30天->14天；
              ③KLINE_DOWN_EXHAUST 改为 ATR 动态阈值(0.1*ATR)；④OI_ROC_TH 提至 0.05；
              ⑤剔除 10 个高度共线/结构错误因子(被复合因子引用者降级为局部中间量，
                复合因子结果逐位不变)；⑥新增绝对费率极值/火药桶僵持区/爆仓猎杀V反/
                终极轧空/高位派发OI撤退/现货抛压镇压 共 7 个客观因子。
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
    MIN_SIGNALS=0,
    MAX_DENSITY=1.9,

    # --- 组合与输出 ---
    ALLOW_SAME_FACTOR=False,
    MAX_TRADES_PER_COMBO=1000000,
    MIN_TRADES_REPORT=1,
    OOS_SPLIT=0.70,
    ENTRY_PREFIX_FILTER=None,
    EXIT_PREFIX_FILTER=None,
    # 【需求1】精确筛选目标信号因子
    ENTRY_EXACT_FILTER=[
    "EXIT_MULTI_MA_BREAK",
    "ENTRY_ALWAYS_TRUE"
],
    EXIT_EXACT_FILTER=[
    "EXIT_MA_DEAD_CROSS"
],
    COINS=None,

    # --- 因子体检 ---
    FWD_HORIZONS_H=(4, 12, 24, 72),
)

EPS = 1e-12

# 【新增】15种环境过滤模式 都是用的之前24小时的数据，不涉及未来数据
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


if HAS_NUMBA:
    _core_static = njit(cache=True, nogil=True)(_core_static)


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
    _fr_raw = (fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float))
    # 因子用的"状态值"(与原逻辑完全一致)
    fr_s = _fr_raw.resample(bar, label='left', closed='left').last()
    # 【新增】真实"结算事件"序列：只在结算时刻所属的那根 bar 上记账，其余 bar 记 0
    #        (同一根 bar 落入多次结算时自动相加；不做任何 8h 估算)
    fr_event_s = _fr_raw.resample(bar, label='left', closed='left').sum()

    df = agg.copy()
    df['oi_amount'] = oi_s.reindex(df.index).ffill()
    df['funding_rate'] = fr_s.reindex(df.index).ffill()
    df['fr_event'] = fr_event_s.reindex(df.index).fillna(0.0).astype(float)

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
    # 【改动②】滚动统计窗口 30 天 -> 14 天 (RK / ZS / QT / corr 全部同步变短)
    P['W'] = B(24 * 14)
    P['H12'], P['H24'], P['H48'] = B(12), B(24), B(48)
    P['H72'], P['H168'] = B(72), B(168)
    P['D2'], P['D7'] = B(48), B(168)

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
        # 【改动①】GAP_TH 已删除：加密永续 7×24 无休市，"跳空"实为数据空洞 ffill 产生的脏数据
        # 【改动③】十字星/衰竭实体阈值改为基于 ATR 的动态阈值 (0.1 * ATR_N)
        EXHAUST_ATR_MULT=0.10,
        FLAT_TH=0.010,
        SILENT_TH=0.010,
        # 【改动④】OI 变化率底线过滤阈值 0.020 -> 0.050 (5%)
        OI_ROC_TH=0.050,
        OI_HOT_TH=0.050,
        CORR_TH=0.20,
        # ---- 【新增】绝对费率极值 / 终极因子阈值 ----
        FR_ABS_TH=0.001,  # 单期资金费率绝对阈值 (±0.1%)
        LIQ_OI_DROP_TH=-0.05,  # 爆仓猎杀: OI 断崖下跌 -5%
        DIST_OI_DROP_TH=-0.02,  # 高位派发: OI 实质性下降 -2%
        LIQ_WICK_TH=0.50,  # 长下影线占全长比
        SPOT_SUPPRESS_ATR_MULT=0.50,  # 现货镇压: 阴线实体 > 0.5 * ATR
        POWDER_OI_RK=0.90,  # 火药桶: OI 分位下限
        POWDER_VOL_RK=0.30,  # 火药桶: 成交量分位上限
        SQUEEZE_OI_RK=0.80,  # 终极轧空: OI 分位下限
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
    oi_pct_1h = pctc(oi, P['BPH'])  # 【新增】精确计算过去 1 小时的 OI 变化率
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
    # 【新增·基础特征补丁】火药桶僵持区：OI 堆积天量杠杆 + 流动性干涸 -> 变盘在即
    F['REGIME_POWDER_KEG'] = (rk_oi > P['POWDER_OI_RK']) & (rk_v < P['POWDER_VOL_RK'])

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
    # 【去重】MOM_ZSCORE_STRONG 删除：Z-score>1.5 与 Rank>0.9 高度共线
    F['MOM_TURN_POSITIVE'] = (ret_N > 0) & (ret_N.shift(1) <= 0)
    F['MOM_CONSISTENT_UP_BARS'] = (c > c.shift(1)).rolling(N).sum() >= P['K_UP_BARS']
    F['MOM_NEW_HIGH_FREQ'] = (c >= maxH_M.shift(1)).rolling(N).sum() >= P['K_NEWHIGH']
    _recov = c / (minL_N + EPS) - 1.0
    F['MOM_RECOVERY_FROM_LOW'] = _recov > QT(_recov, 0.80)
    F['MOM_PERSISTENCE'] = (ret_N > 0) & (ret_N.shift(N) > 0)
    F['MOM_NOT_OVERHEATED'] = rk_ret_N < 0.95
    _vwm = RSUM(ret_M * v, N) / (rsum_v_N + EPS)
    F['MOM_VOLUME_WEIGHTED'] = RK(_vwm) > 0.80

    # 【去重】波动收敛(盘整)类 4 个因子仅保留最直观的振幅收敛 VOL_RANGE_COMPRESSION_REAL；
    #        其中"真实波幅收敛"被 VOL_SQUEEZE_TO_EXPAND_REAL 引用，故降级为内部中间量，
    #        以保证复合因子结果逐位不变。
    _vol_tr_comp = rk_atr < 0.20
    F['VOL_RANGE_COMPRESSION_REAL'] = rk_rng < 0.25
    F['VOL_ATR_EXPANSION'] = ZS(atr_pct) > 1.0
    F['VOL_SQUEEZE_TO_EXPAND_REAL'] = bs(_vol_tr_comp) & F['VOL_ATR_EXPANSION']
    F['VOL_LOW_TO_HIGH'] = (rk_atr.shift(M) < 0.20) & (rk_atr > 0.60)
    F['VOL_NOT_EXTREME'] = rk_atr < 0.90
    F['VOL_EXTREME_RISK'] = rk_atr > 0.95
    F['VOL_DOWN_SPIKE'] = F['VOL_ATR_EXPANSION'] & (ret_M < 0)

    # 【去重】BREAK_N_HIGH_REAL(持续状态) 与 BREAK_DONCHIAN_HIGH_EVENT_REAL(瞬时事件) 高度重合，
    #        作为状态机入场触发器保留事件型；持续状态降级为内部中间量供复合因子复用。
    _break_n_high = c > maxH_N.shift(1)
    F['BREAK_DONCHIAN_HIGH_EVENT_REAL'] = (c >= maxH_N.shift(1)) & (c.shift(1) < maxH_N.shift(2))
    F['BREAK_BOLLINGER_UPPER'] = c > ma_N + 2 * c.rolling(N, min_periods=max(2, N // 2)).std()
    # 【去重】BREAK_RANGE_QUANTILE 删除：价格绝对位置 Rank>0.9 与极端动量类共线
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
    # 【改动①】KLINE_GAP_UP / KLINE_GAP_DOWN 已删除(7×24 市场无休市，全是数据空洞脏值)
    # 【改动③】衰竭小实体判定：由固定相对阈值改为 ATR 动态阈值 (0.1 * ATR_N)
    F['KLINE_DOWN_EXHAUST'] = ((ret_N.shift(1) < 0) & (c < o)
                               & ((o - c) < P['EXHAUST_ATR_MULT'] * atr_N) & (c > l))
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
    F['VOLUME_CONFIRM_BREAK'] = _break_n_high & F['VOLUME_SPIKE']
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
    F['OI_BREAKOUT_CONFIRM'] = _break_n_high & (rk_oipct_M > 0.70)
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
    # 【去重】OI_VALUE_SURGE 删除：OI市值 = OI数量 × 价格，与纯 OI_SURGE_RANK 高度共线且混入价格
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
    # 【新增·基础特征补丁】绝对费率极值：弥补 Rank 的缺陷，过滤真实持有成本极高的状态
    F['FR_ABSOLUTE_DEEP_NEG'] = fr < -P['FR_ABS_TH']
    F['FR_ABSOLUTE_HIGH_POS'] = fr > P['FR_ABS_TH']
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
                                             & _break_n_high & F['VOLUME_SPIKE'])
    F['ENTRY_SILENT_ACCUMULATION'] = (ret_N.abs() < P['SILENT_TH']) & (oi_pct_N > 0) & F['FR_LOW_NEG']
    F['ENTRY_SHORT_SQUEEZE_LAUNCH'] = (bs(F['FR_VERY_LOW']) & bs(F['PRICE_HIGHER_LOWS'])
                                       & bs(F['OI_SLOPE_UP']) & _break_n_high)
    F['ENTRY_SHORT_SQUEEZE_VOLUME'] = (fr < 0) & F['VOLUME_SPIKE'] & (ret_M > 0) & (oi_pct_M < 0)
    F['ENTRY_OI_FLASH_SURGE'] = F['OI_SURGE_RANK'] & F['PRICE_HEALTHY_EXTENSION'] & F['FR_MILD']
    F['ENTRY_BEAR_TRAP_RECLAIM_VOLUME'] = (l < minL_N.shift(1)) & (c > o) & F['VOLUME_SPIKE'] & F['OI_SLOPE_UP']
    F['ENTRY_HIGH_PRESSURE_OI_BREAKOUT'] = (rk_oi > 0.95) & _break_n_high & F['VOLUME_SPIKE']
    F['ENTRY_TREND_CONFIRM_B'] = F['PRICE_MA_STACK'] & F['OI_MA_UP'] & F['FR_MILD']
    F['ENTRY_PULLBACK_RESTART_VOLUME'] = (bs(F['PRICE_MA_STACK']) & bs(F['VOLUME_DRY_PULLBACK'])
                                          & (oi_pct_N > -0.05) & F['VOLUME_SPIKE'] & (c > maxH_M.shift(1)))
    F['ENTRY_BREAKOUT_RETEST_OI_STABLE'] = F['BREAK_RETEST_HOLD_REAL'] & (oi_pct_N > -0.03) & F['FR_MILD']
    F['ENTRY_FR_RESET_SECOND_WAVE'] = (F['FR_RESET_AFTER_HOT'] & F['PRICE_MA_STACK']
                                       & F['OI_SLOPE_UP'] & _break_n_high)
    F['ENTRY_HEALTHY_ACCELERATION_VOLUME'] = (F['MOM_RETURN_STRONG'] & F['VOLUME_SPIKE']
                                              & F['OI_SURGE_RANK'] & F['FR_PRICE_UP_NOT_HOT'])
    F['ENTRY_UNCROWDED_MOMENTUM'] = F['MOM_RETURN_STRONG'] & F['FR_POS_NOT_HOT'] & F['OI_VALUE_HEALTHY']
    F['ENTRY_OI_LEAD_MOMENTUM'] = bs(F['OI_LEADS_PRICE']) & bs(F['STRUCT_NEAR_HIGH_BASE']) & F['MOM_BURST']
    F['ENTRY_OI_LEAD_SQUEEZE'] = F['OI_LEADS_PRICE'] & F['VOL_RANGE_COMPRESSION_REAL']
    F['ENTRY_FUNDING_COLD_START_TREND'] = F['FR_COLD_START'] & F['PRICE_MA_STACK'] & F['OI_MA_UP']
    F['ENTRY_PRICE_UP_OI_DOWN_SPOT_PUSH'] = F['PRICE_MA_STACK'] & F['OI_PRICE_UP_OI_DOWN'] & F['FR_LOW_NEG']
    F['ENTRY_LONG_CONSOL_OI_VOLUME_CONFIRM'] = F['BREAK_LONG_CONSOLIDATION_REAL'] & F['OI_SURGE_RANK'] & F[
        'VOLUME_SPIKE']
    F['ENTRY_OI_LOW_RECOVERY_BREAK'] = bs(F['OI_LOW_TO_UP']) & _break_n_high
    F['ENTRY_BOTTOM_STABILIZE'] = F['OI_BOTTOM_DIVERGENCE'] & F['FR_LOW_NEG'] & F['PRICE_HIGHER_LOWS']
    F['ENTRY_VWAP_RECLAIM_OI'] = F['VWAP_RECLAIM'] & F['OI_SLOPE_UP'] & F['FR_MILD']
    F['ENTRY_OBV_BULL_DIV_BREAK'] = bs(F['OBV_BULL_DIV']) & _break_n_high & F['VOLUME_SPIKE']
    F['ENTRY_INSIDE_BREAK_VOLUME'] = bs(F['KLINE_INSIDE_BAR']) & (c > h.shift(1)) & F['VOLUME_SPIKE']
    F['ENTRY_OUTSIDE_BAR_VOLUME'] = F['KLINE_OUTSIDE_BAR_UP'] & F['VOLUME_SPIKE'] & F['OI_SLOPE_UP']
    F['ENTRY_HAMMER_VOLUME_OI'] = F['KLINE_HAMMER'] & F['VOLUME_SPIKE'] & F['OI_SLOPE_UP'] & F['FR_LOW_NEG']
    F['ENTRY_THREE_GREEN_VOLUME_OI'] = F['KLINE_THREE_GREEN_UP'] & F['VOLUME_MA_UP'] & F['OI_MA_UP']

    # ---------- 【新增】终极入场因子 ----------
    # 1. 连环爆仓 V 反 / 流动性猎杀 (Liquidation Sweep Bottom)
    #    跌破前低 -> 多头连环爆仓(OI 物理消灭) -> 长下影线极速拉回 + 局部爆量
    #    注：OI 断崖判定沿用给定公式 oi_pct_M (M=4h 窗口)；若需 1h 口径改为 pctc(oi, P['BPH'])
    # 1. 连环爆仓 V 反 / 流动性猎杀 (Liquidation Sweep Bottom)
    F['ENTRY_LIQUIDATION_SWEEP_BOTTOM'] = ((l < minL_N.shift(1))
                                           & (lw > P['LIQ_WICK_TH'])
                                           & (oi_pct_1h < P['LIQ_OI_DROP_TH'])  # 【改动】这里改成了 oi_pct_1h
                                           & F['VOLUME_SPIKE'])
    # 2. 终极轧空爆发 (Extreme Short Squeeze)
    #    空头死扛(OI 高位攀升) + 支付极高利息(深负费率) + 突破 24h 高点事件 -> 空头踩踏
    F['ENTRY_EXTREME_SHORT_SQUEEZE'] = (F['BREAK_DONCHIAN_HIGH_EVENT_REAL']
                                        & (rk_oi > P['SQUEEZE_OI_RK'])
                                        & F['OI_SLOPE_UP']
                                        & ((fr_rank < 0.05) | F['FR_ABSOLUTE_DEEP_NEG']))

    # 【新增】表示都为True的因子，每根bar都是信号
    F['ENTRY_ALWAYS_TRUE'] = pd.Series(True, index=df.index)

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
    # 【改动①】EXIT_GAP_DOWN_RISK 已删除(依赖被删的 KLINE_GAP_DOWN，捕捉的是脏数据)
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

    # ---------- 【新增】终极出场因子 ----------
    # 3. 高位派发与 OI 撤退 (Distribution Exhaustion Top)
    #    价格仍在绝对高位(距 24h 高点<5%)，但 OI 已实质性下降(>2%) -> 大户在平多/开空
    F['EXIT_DISTRIBUTION_EXHAUSTION_TOP'] = ((c / (maxH_N + EPS) > 0.95)
                                             & (oi_pct_M < P['DIST_OI_DROP_TH']))
    # 4. 现货抛压镇压 (Spot Suppression Exit)
    #    合约多头极度拥挤(费率狂热) + 爆量 + 实体大阴线(> 0.5*ATR) -> 主力现货砸盘
    F['EXIT_SPOT_SUPPRESSION'] = (((fr_rank > 0.95) | F['FR_ABSOLUTE_HIGH_POS'])
                                  & F['VOLUME_SPIKE']
                                  & (c < o)
                                  & ((o - c) > P['SPOT_SUPPRESS_ATR_MULT'] * atr_N))

    out = {}
    for k_, s in F.items():
        out[k_] = np.ascontiguousarray(s.fillna(False).to_numpy(dtype=bool))
    return out


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
    m3 = (d2 * d).mean()  # 与 scipy._moment 的平方求幂顺序一致
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
    prev = int(vi)  # vi >= 0，等价 floor
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


def trade_stats(rets, ent, ext, bar_minutes, n_bars, start_idx=0, bh_rets=None, prefix='',
                fr_trades=None):
    """
    fr_trades: 与 rets 一一对应的【每笔资金费率之和】(原始小数, 非百分比)。
               取值口径 = 真实持仓区间 [入场执行时刻, 出场执行时刻) 左闭右开内
               所有真实资金费率结算事件之和(见 mine_symbol 的 fr_cum)。
               输出 fr_sum / fr_avg 单位为 %，符号为原始符号：
                   做多实际成本 = -fr_sum ; 做空实际收益 = +fr_sum
    """
    d = {}
    T = int(len(rets))
    d[prefix + 'trades'] = T

    # 【新增】加入 pt_sharpe (单笔夏普), trades_q1~4, fr_sum/fr_avg (资金费率求和)
    empty_keys = ['win_rate', 'sum_ret', 'avg_ret', 'med_ret', 'std_ret', 'tstat', 'sharpe', 'pt_sharpe',
                  'profit_factor', 'max_dd', 'avg_hold_h', 'exposure', 'max_win', 'max_loss',
                  'skew', 'kurt', 'cvar_5', 'equity_r2', 'corr_btc', 'down_market_win_rate',
                  'win_hold_bars', 'loss_hold_bars',
                  'ret_q1', 'ret_q2', 'ret_q3', 'ret_q4',
                  'trades_q1', 'trades_q2', 'trades_q3', 'trades_q4',
                  'fr_sum', 'fr_avg']

    if T == 0:
        for k in empty_keys:
            d[prefix + k] = np.nan
        return d

    pos_mask = rets > 0  # 【性能】胜负掩码只算一次，后面复用
    d[prefix + 'win_rate'] = float(pos_mask.mean() * 100)
    d[prefix + 'sum_ret'] = float(rets.sum() * 100)
    d[prefix + 'avg_ret'] = float(rets.mean() * 100)
    d[prefix + 'med_ret'] = float(_median_fast(rets) * 100)

    # 【新增】资金费率求和 / 单笔均值（单位 %）
    if fr_trades is not None and len(fr_trades) > 0:
        d[prefix + 'fr_sum'] = float(np.sum(fr_trades) * 100)
        d[prefix + 'fr_avg'] = float(np.mean(fr_trades) * 100)
    else:
        d[prefix + 'fr_sum'] = np.nan
        d[prefix + 'fr_avg'] = np.nan

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
    F = build_factors(df, P, rank_shift=cfg['RANK_SHIFT'])
    warm = min(P['WARMUP'], len(df) - 100)
    if warm < 0 or len(df) - warm < 200:
        return None, {'total_combos': 0}, 0, None

    df = df.iloc[warm:].copy()
    rk_gain = df['rank_gain_24h'].to_numpy(float)
    rk_loss = df['rank_loss_24h'].to_numpy(float)

    # 【新增】资金费率结算事件的前缀和：
    #   fr_cum[i] = sum(fr_event[0:i])
    #   考虑到实盘系统的计算与网络延迟（Latency），这里的计费区间做了“贴近实战”的偏移：
    #   1. 入场：信号在 bar e 结算产生，e+1 开盘执行。由于实盘延迟，真实建仓通常略晚于 e+1 的准点快照，因此【完美避开】e+1 的资金费。
    #   2. 出场：信号在 bar x 结算产生，x+1 开盘执行。由于实盘延迟，真实平仓也略晚于 x+1 的准点快照，此时仍持有仓位，因此【无法逃避】x+1 的资金费。
    #   => 真实应计资金费率的 bar 索引区间为 [e+2, x+1] (闭区间)
    #   => 利用前缀和计算：fr_sum = fr_cum[(x + 1) + 1] - fr_cum[(e + 1) + 1]
    #   => 化简为：fr_sum = fr_cum[x + 2] - fr_cum[e + 2]
    if 'fr_event' in df.columns:
        fr_ev = df['fr_event'].to_numpy(float)
    else:
        fr_ev = np.zeros(len(df), dtype=float)
    fr_ev = np.nan_to_num(fr_ev, nan=0.0, posinf=0.0, neginf=0.0)
    fr_cum = np.concatenate(([0.0], np.cumsum(fr_ev)))

    F = {k: v[warm:] for k, v in F.items()}

    if btc_close is not None:
        btc_c = btc_close.reindex(df.index).ffill().bfill().to_numpy(float)
    else:
        btc_c = None

    n = len(df)
    kline_days = n * bm / 1440.0
    op = df['open'].to_numpy(float)
    cl = df['close'].to_numpy(float)
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

    entry_names = [k for k in keep]
    exit_names = [k for k in keep]

    # 【修改】执行精确过滤（优先级高于前缀过滤）
    if cfg.get('ENTRY_EXACT_FILTER'):
        entry_names = [k for k in entry_names if k in cfg['ENTRY_EXACT_FILTER']]
    elif cfg.get('ENTRY_PREFIX_FILTER'):
        entry_names = [k for k in entry_names if k.startswith(tuple(cfg['ENTRY_PREFIX_FILTER']))]

    if cfg.get('EXIT_EXACT_FILTER'):
        exit_names = [k for k in exit_names if k in cfg['EXIT_EXACT_FILTER']]
    elif cfg.get('EXIT_PREFIX_FILTER'):
        exit_names = [k for k in exit_names if k.startswith(tuple(cfg['EXIT_PREFIX_FILTER']))]

    idx_cache = {k: np.flatnonzero(F[k]).astype(np.int64) for k in keep}
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
    col_data = None

    # 【新增】存储每笔交易的数组记录
    trade_records = []
    df_times = df.index
    lows = df['low'].to_numpy(float)
    highs = df['high'].to_numpy(float)

    # 【新增】精确统计真实搜索空间（考虑静态组合产出2行）
    theoretical_tests = 0
    done = 0

    # ==================================================================
    # 【Bug 修复】排名过滤前置化
    #   旧逻辑: 全量入场 -> 撮合(唯一仓位) -> 事后按 rank 删行
    #           => 排名 80 的劣质信号先占坑，把持仓期内真正达标(rank<=50)的
    #              优质信号挡在门外，最后自己又被删掉 -> 报表大量"漏单"
    #   新逻辑: 每个 filter_mode 先把不达标的 bar 从"入场候选集"中剔除，
    #           再交给状态机撮合 => 每个 mode 是一条独立、无污染的策略路径。
    #   性能补偿: 同一入场因子下，过滤结果完全相同的 mode 合并为一个 group，
    #             只撮合一次、只算一次统计，再复制给组内每个 label(结果逐位一致)。
    # ==================================================================
    def _entry_groups(eidx):
        groups = []  # [ [e_arr, [labels...], has_original] , ... ]
        sig_map = {}
        for mode_name, rank_col, threshold in FILTER_MODES:
            label = 'original' if mode_name == 'original' else f"{mode_name}_{threshold}"
            if mode_name == 'original':
                e_m = eidx
            elif mode_name == 'top':
                e_m = eidx[rk_gain[eidx] <= threshold]
            else:
                e_m = eidx[rk_loss[eidx] <= threshold]

            key = (int(e_m.size), hash(e_m.tobytes()))
            gi = sig_map.get(key, -1)
            if gi >= 0 and np.array_equal(groups[gi][0], e_m):
                groups[gi][1].append(label)
                if label == 'original':
                    groups[gi][2] = True
            else:
                sig_map[key] = len(groups)
                groups.append([np.ascontiguousarray(e_m), [label], label == 'original'])
        return groups

    for en in entry_names:
        eidx = idx_cache[en]
        e_dens = dens.get(en, np.nan)
        egroups = _entry_groups(eidx)

        for xn in exit_names:
            done += 1
            if (not cfg['ALLOW_SAME_FACTOR']) and xn == en:
                stats['skip_same_factor'] += 1
                continue

            # 累加理论测试次数：静态出场测多空(×2)
            multiplier = 2
            theoretical_tests += multiplier * len(FILTER_MODES)

            xidx = idx_cache[xn]
            x_dens = dens.get(xn, np.nan)

            for e_arr, labels, has_orig in egroups:
                n_lab = len(labels)

                # 该 mode 组过滤后已无入场候选 -> 必然 0 笔
                if e_arr.size == 0:
                    if has_orig:
                        stats['skip_zero_trades'] += 1
                        stats['skip_too_few'] += 2 * (n_lab - 1)
                    else:
                        stats['skip_too_few'] += 2 * n_lab
                    continue

                if HAS_NUMBA:
                    ent, ext = _core_static(e_arr, xidx, n, cfg['COOLDOWN_BARS'], max_tr)
                else:
                    ent, ext = _match_static_ss(e_arr, xidx, n, cfg['COOLDOWN_BARS'], max_tr)

                if ent.size == 0:
                    if has_orig:
                        stats['skip_zero_trades'] += 1
                        stats['skip_too_few'] += 2 * (n_lab - 1)
                    else:
                        stats['skip_too_few'] += 2 * n_lab
                    continue

                # 【新增】每笔资金费率之和：左闭右开真实持仓区间 -> bar 索引 (e, x]
                safe_ent = np.minimum(ent + 2, n)
                safe_ext = np.minimum(ext + 2, n)
                fr_tr = fr_cum[safe_ext] - fr_cum[safe_ent]

                # === 计算做多收益 ===
                rets_long = exec_px[ext] / exec_px[ent] - 1.0 - cost
                ok_l = np.isfinite(rets_long)

                # === 计算做空收益 (U本位 计算) ===
                rets_short = 1.0 - (exec_px[ext] / exec_px[ent]) - cost
                ok_s = np.isfinite(rets_short)

                for direction, okm, rets_all in (('Long', ok_l, rets_long),
                                                 ('Short', ok_s, rets_short)):
                    ent_f = ent[okm]
                    ext_f = ext[okm]
                    rets_f = rets_all[okm]
                    fr_f = fr_tr[okm]

                    # 【新增】计算每笔交易的最大回撤
                    mdds = np.empty(len(ent_f), dtype=float)
                    for i in range(len(ent_f)):
                        e = ent_f[i]
                        x = ext_f[i]
                        if e <= x:
                            if direction == 'Long':
                                lowest = np.min(lows[e:x + 1])
                                mdds[i] = min(0.0, lowest / exec_px[e] - 1.0 - cost)
                            else:
                                highest = np.max(highs[e:x + 1])
                                mdds[i] = min(0.0, 1.0 - highest / exec_px[e] - cost)
                        else:
                            mdds[i] = 0.0

                    bh_rets_f = (btc_c[ext_f] / btc_c[ent_f] - 1.0) if btc_c is not None else None

                    # 同一 group 内各 label 的成交流水完全一致 -> 统计只算一次
                    st = trade_stats(rets_f, ent_f, ext_f, bm, n_bars=n, start_idx=0,
                                     bh_rets=bh_rets_f, fr_trades=fr_f)
                    m_is = ent_f < split_bar
                    st.update(trade_stats(rets_f[m_is], ent_f[m_is], ext_f[m_is], bm,
                                          n_bars=split_bar, start_idx=0,
                                          bh_rets=bh_rets_f[m_is] if bh_rets_f is not None else None,
                                          prefix='is_', fr_trades=fr_f[m_is]))
                    st.update(trade_stats(rets_f[~m_is], ent_f[~m_is], ext_f[~m_is], bm,
                                          n_bars=n - split_bar, start_idx=split_bar,
                                          bh_rets=bh_rets_f[~m_is] if bh_rets_f is not None else None,
                                          prefix='oos_', fr_trades=fr_f[~m_is]))

                    for lab in labels:
                        stats['mode_pass_counts'][lab] += 1
                        row = dict(coin=coin, entry_factor=en, exit_factor=xn, direction=direction,
                                   filter_mode=lab, entry_density=e_dens, exit_density=x_dens)
                        row.update(st)

                        if col_data is None:
                            col_data = {k_: [v_] for k_, v_ in row.items()}
                        else:
                            for k_, v_ in row.items():
                                col_data[k_].append(v_)

                        # 【新增】记录单笔交易详情（带入最大回撤）
                        for i in range(len(ent_f)):
                            trade_records.append({
                                'coin': coin,
                                'entry_factor': en,
                                'exit_factor': xn,
                                'direction': direction,
                                'filter_mode': lab,
                                'entry_time': df_times[ent_f[i]],
                                'exit_time': df_times[ext_f[i]],
                                'entry_price': exec_px[ent_f[i]],
                                'exit_price': exec_px[ext_f[i]],
                                'return': rets_f[i],
                                'max_drawdown': mdds[i],
                                'fr_sum': fr_f[i]
                            })

    out = pd.DataFrame(col_data) if col_data else pd.DataFrame()
    df_trades = pd.DataFrame(trade_records) if trade_records else pd.DataFrame()

    # ==================================================================
    # 【新增】无感注入"测试基数"(多重检验搜索空间)，用于事后精确还原 DSR
    # ==================================================================
    if not out.empty:
        n_modes = len(FILTER_MODES)
        out['bar_minutes'] = int(bm)
        out['kline_days'] = float(kline_days)
        out['n_trials_combos'] = int(stats['total_combos'])
        out['n_trials_modes'] = int(n_modes)
        out['n_trials_total'] = int(theoretical_tests)
        out['n_trials_alive'] = int(len(out))

    return out, stats, kline_days, df_trades


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
        # if len(df) < 800:
        #     # 统一返回 7 个元素，前两个 0 分别代表 valid_combos, total_saved_trades
        #     return kf, coin, 0, 0, {'total_combos': 0}, 0, "bar 数不足"

        pairs, stats, kline_days, df_trades = mine_symbol(coin, df, cfg, _BTC_CLOSE)

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

            # 【新增】写出每笔交易详情及回撤记录
            if df_trades is not None and not df_trades.empty:
                trades_path = os.path.join(cfg['OUT_DIR'], f'trades_{coin}.csv.gz')
                _atomic_to_csv(df_trades, trades_path)

            # 帮助子进程尽快释放内存
            del pairs
            if df_trades is not None:
                del df_trades
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
    return os.path.join(out_dir, f'pairs_{coin}.csv.gz')


def _atomic_to_csv(df, path):
    """【新增】原子落盘：先写 .tmp 再 os.replace，杜绝中断产生半截文件污染断点"""
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False, encoding='utf-8-sig', float_format='%.5f', compression='gzip')
    os.replace(tmp, path)


def _clean_stale_tmp(out_dir):
    """【新增】清理上一次异常中断残留的 .tmp（它们不是有效结果，不能被当作已完成）"""
    n = 0
    for f in os.listdir(out_dir):
        if f.startswith('pairs_') and f.endswith('.tmp'):
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
        # 断点续跑：已存在 pairs_{coin}.csv.gz 则完全跳过回测
        if os.path.exists(_coin_out_path(cfg['OUT_DIR'], coin)):
            resume_coins.append(coin)
            continue
        valid_coins.append(coin)
        valid_kfiles.append(kf)

    print("=" * 78)
    print(f"  因子挖掘启动 | bar={cfg['BAR_MINUTES']}min | numba={'ON' if HAS_NUMBA else 'OFF'}")
    print(f"⏭️  断点续跑: 已存在结果 {len(resume_coins)} 个 ->直接跳过回测")
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
        _core_static(_d_idx, _d_idx, 2, 0, 1)

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
    print("    请运行 rebuild_pairs_summary.py，由 pairs_<coin>.csv.gz 精确还原(含真实测试基数的 DSR)。")
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

        # 【关键，需求3修改】动态修改输出目录为debug专用目录，防止影响正常的文件
        run_cfg['OUT_DIR'] = f'./factor_out_{bm}m_debug_test'

        # 调用主函数执行
        main(run_cfg)