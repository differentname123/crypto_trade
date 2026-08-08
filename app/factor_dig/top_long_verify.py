# -*- coding: utf-8 -*-
"""
================================================================================
 ALT-COIN LAUNCH FACTOR MINER (固定组合回测 - 统计日志精排版)
 进行验证
 策略逻辑:
   - 入场: EXIT_UPPER_WICK_REJECTION (高位长上影线+放量拒接)
   - 出场: ENTRY_INSIDE_BREAK_VOLUME (孕线上破+放量)
================================================================================
"""
from __future__ import annotations
import os
import math
import warnings
import itertools

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ======================================================================
# 0. 全局配置
# ======================================================================
CFG = dict(
    DATA_DIR='./data',  # 数据目录
    BAR_MINUTES=15,  # 周期时长
    FEE_RATE=0.0005,  # 单边手续费
    SLIPPAGE=0.0005,  # 单边滑点
    COOLDOWN_BARS=0,  # 冷却K线数
    OOS_SPLIT=0.70,  # 样本内外切分比例
    COINS=None,  # 填入指定的币种列表如 ['PEPE', 'WIF']，None表示全跑
    RANK_MODE='both',  # 排行榜过滤模式：'both', 'top', 'bottom'
)

EPS = 1e-12
GLOBAL_PLATEAU_RESULTS = []

# 全局内存缓存，用于极大地加速数据读取和指标计算
_DF_CACHE = {}
_FACTOR_CACHE = {}
_CROSS_CACHE = {}  # 新增：用于缓存全市场截面排名数据

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


if HAS_NUMBA:
    _core_static = njit(cache=True, nogil=True)(_core_static)


def _match_static_ss(entry_idx, exit_idx, n, cooldown, max_trades):
    """无 numba 时的跳跃匹配"""
    ent, ext = [], []
    ne, nx = entry_idx.size, exit_idx.size
    pos = 0
    while pos < n - 1 and len(ent) < max_trades:
        a = np.searchsorted(entry_idx, pos, side='left')
        if a >= ne: break
        e = int(entry_idx[a])
        if e >= n - 1: break
        b = np.searchsorted(exit_idx, e + 1, side='left')
        x = int(exit_idx[b]) if b < nx else n - 1
        ent.append(e)
        ext.append(x)
        pos = x + 1 + cooldown
    return np.asarray(ent, np.int64), np.asarray(ext, np.int64)


# ======================================================================
# 2. 数据加载与对齐 (加入内存缓存机制)
# ======================================================================
def _pick(df, cands, what):
    for c in cands:
        if c in df.columns: return c
    raise KeyError(f"[{what}] 找不到列 {cands}")


def load_symbol(kline_file, oi_file, fr_file, bar_minutes):
    bar = f"{bar_minutes}min"

    k = pd.read_csv(kline_file)
    kt = _pick(k, ['timestamp', 'open_time', 'time', 'ts'], 'kline')
    k['dt'] = pd.to_datetime(k[kt], unit='ms', utc=True)
    k = k.drop_duplicates(subset=[kt]).sort_values('dt').set_index('dt')
    agg = k.resample(bar, label='left', closed='left').agg(
        open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'), volume=('volume', 'sum'))
    agg['close'] = agg['close'].ffill()
    agg = agg[agg['close'].notna()]
    agg['open'] = agg['open'].fillna(agg['close'])
    agg['high'] = agg['high'].fillna(agg['close'])
    agg['low'] = agg['low'].fillna(agg['close'])
    agg['volume'] = agg['volume'].fillna(0.0)

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

    # 提取精确的 funding rate 及其发生时间
    fr_exact = fr.drop_duplicates(subset=[ft]).sort_values('dt').set_index('dt')[fc].astype(float)

    # 1. 用于 ffill 的 funding rate (保持原有逻辑)
    fr_s = fr_exact.resample(bar, label='left', closed='left').last()

    # 2. 用于计算实际结算的 funding rate (只在结算bar有值，其他为0，避免重复计算)
    fr_settlement = fr_exact.resample(bar, label='left', closed='left').last().fillna(0.0)

    df = agg.copy()
    df['oi_amount'] = oi_s.reindex(df.index).ffill()
    df['funding_rate'] = fr_s.reindex(df.index).ffill()
    df['funding_settlement'] = fr_settlement.reindex(df.index).fillna(0.0)

    fv = df[['oi_amount', 'funding_rate']].apply(lambda s: s.first_valid_index())
    start = max([x for x in fv.tolist() if x is not None], default=df.index[0])
    df = df.loc[start:].copy()
    df[['oi_amount', 'funding_rate']] = df[['oi_amount', 'funding_rate']].ffill()
    df = df.dropna(subset=['oi_amount', 'funding_rate'])
    for c in ['open', 'high', 'low', 'close']: df = df[df[c] > 0]
    return df


def _load_symbol_cached(kline_file, oi_file, fr_file, bar_minutes):
    """带全局缓存的数据加载包装器，大幅度降低跨参数回测I/O时间"""
    cache_key = (kline_file, bar_minutes)
    if cache_key not in _DF_CACHE:
        _DF_CACHE[cache_key] = load_symbol(kline_file, oi_file, fr_file, bar_minutes)
    return _DF_CACHE[cache_key]


# ======================================================================
# 3. 极简因子计算 (加入滚动指标复用缓存)
# ======================================================================
def make_params(bar_minutes, n_rows):
    bph = 60.0 / bar_minutes
    B = lambda hours: max(1, int(round(hours * bph)))
    P = {}
    P['N'] = B(24)
    P['W'] = B(24 * 30)
    if n_rows < P['W'] * 2: P['W'] = max(200, n_rows // 3)
    P['MINP_W'] = max(50, P['W'] // 5)
    P['WARMUP'] = int(P['W'] + B(168) + 3 * P['N'])
    return P


def build_factors(df, P, cross_mask=None):
    W, N = P['W'], P['N']
    mp = P['MINP_W']
    o, h, l, c = df['open'], df['high'], df['low'], df['close']
    v = df['volume']

    # 唯一标识同一份DataFrame，用于缓存耗时的因子运算
    df_id = id(df)

    # 提取搜索参数
    upper_wick_thresh = P.get('UPPER_WICK_THRESH', 0.50)
    vol_quantile = P.get('VOL_QUANTILE', 0.95)
    high_close_thresh = P.get('HIGH_CLOSE_THRESH', 0.95)

    def QT(s, p):
        key = (df_id, 'QT', W, mp, p)
        if key not in _FACTOR_CACHE:
            _FACTOR_CACHE[key] = s.rolling(W, min_periods=mp).quantile(p).shift(1)
        return _FACTOR_CACHE[key]

    def bs(s, k=1):
        return s.shift(k, fill_value=False)

    # 缓存最耗时的 rolling 算子操作
    key_maxH = (df_id, 'maxH', N)
    if key_maxH not in _FACTOR_CACHE:
        _FACTOR_CACHE[key_maxH] = h.rolling(N, min_periods=max(2, N // 2)).max()
    maxH_N = _FACTOR_CACHE[key_maxH]

    key_uw = (df_id, 'uw')
    if key_uw not in _FACTOR_CACHE:
        rng = (h - l) + EPS
        _FACTOR_CACHE[key_uw] = (h - np.maximum(o, c)) / rng
    uw = _FACTOR_CACHE[key_uw]

    key_inside = (df_id, 'inside')
    if key_inside not in _FACTOR_CACHE:
        _FACTOR_CACHE[key_inside] = (h < h.shift(1)) & (l > l.shift(1))

    F = {}
    F['KLINE_LONG_UPPER_WICK'] = uw > upper_wick_thresh
    F['VOLUME_SPIKE'] = v > QT(v, vol_quantile)
    F['KLINE_INSIDE_BAR'] = _FACTOR_CACHE[key_inside]

    # 原生入场信号: 高位长上影且放量 (未经过滤器)
    F['EXIT_UPPER_WICK_REJECTION_RAW'] = (c / (maxH_N + EPS) > high_close_thresh) & F['KLINE_LONG_UPPER_WICK'] & F[
        'VOLUME_SPIKE']

    # 动态应用截面排名前/后 50 过滤，同时保留被过滤掉的信号用于排雷计算
    if cross_mask is not None:
        F['EXIT_UPPER_WICK_REJECTION'] = F['EXIT_UPPER_WICK_REJECTION_RAW'] & cross_mask
        F['EXIT_REJECTED'] = F['EXIT_UPPER_WICK_REJECTION_RAW'] & (~cross_mask)
    else:
        F['EXIT_UPPER_WICK_REJECTION'] = F['EXIT_UPPER_WICK_REJECTION_RAW']
        F['EXIT_REJECTED'] = pd.Series(False, index=df.index)

    # 出场信号: 孕线之后突破且放量
    F['ENTRY_INSIDE_BREAK_VOLUME'] = bs(F['KLINE_INSIDE_BAR']) & (c > h.shift(1)) & F['VOLUME_SPIKE']

    out = {}
    for k_ in ['EXIT_UPPER_WICK_REJECTION', 'ENTRY_INSIDE_BREAK_VOLUME', 'EXIT_REJECTED']:
        out[k_] = np.ascontiguousarray(F.get(k_, pd.Series(False, index=df.index)).fillna(False).to_numpy(dtype=bool))
    return out


# ======================================================================
# 4. 绩效计算与匹配回测
# ======================================================================
def trade_stats(rets, ent, ext, bar_minutes, n_bars, prefix=''):
    T = int(len(rets))
    d = {prefix + 'trades': T}
    if T == 0:
        for k in ['win_rate', 'sum_ret', 'avg_ret', 'med_ret', 'profit_factor', 'max_dd', 'avg_hold_h', 'exposure',
                  'max_win', 'max_loss', 'pl_ratio', 'max_consec_loss']:
            d[prefix + k] = np.nan
        return d

    d[prefix + 'win_rate'] = float((rets > 0).mean() * 100)
    d[prefix + 'sum_ret'] = float(rets.sum() * 100)
    d[prefix + 'avg_ret'] = float(rets.mean() * 100)
    d[prefix + 'med_ret'] = float(np.median(rets) * 100)

    eq = np.concatenate(([0.0], np.cumsum(rets)))
    d[prefix + 'max_dd'] = float((np.maximum.accumulate(eq) - eq).max() * 100)

    hold = (ext - ent).astype(float)
    d[prefix + 'avg_hold_h'] = float(hold.mean() * bar_minutes / 60.0)
    d[prefix + 'exposure'] = float(hold.sum() / max(n_bars, 1) * 100)
    d[prefix + 'max_win'] = float(rets.max() * 100)
    d[prefix + 'max_loss'] = float(rets.min() * 100)

    # 新增指标: 盈亏比 (Profit/Loss Ratio)
    avg_win = rets[rets > 0].mean() if (rets > 0).any() else 0
    avg_loss = abs(rets[rets < 0].mean()) if (rets < 0).any() else 1e-9
    d[prefix + 'pl_ratio'] = float(avg_win / avg_loss)

    # 新增指标: 最大连续亏损次数
    is_loss = rets < 0
    if is_loss.any():
        loss_runs = []
        current_run = 0
        for l in is_loss:
            if l:
                current_run += 1
            else:
                if current_run > 0:
                    loss_runs.append(current_run)
                current_run = 0
        if current_run > 0:
            loss_runs.append(current_run)
        d[prefix + 'max_consec_loss'] = int(max(loss_runs)) if loss_runs else 0
    else:
        d[prefix + 'max_consec_loss'] = 0

    return d


def mine_symbol(coin, df, cfg, cross_mask=None):
    bm = cfg['BAR_MINUTES']
    P = make_params(bm, len(df))
    if 'SEARCH_PARAMS' in cfg:
        P.update(cfg['SEARCH_PARAMS'])
    F = build_factors(df, P, cross_mask)

    warm = min(P['WARMUP'], len(df) - 100)
    if warm < 0 or len(df) - warm < 200:
        return None, [], [], [], []

    df = df.iloc[warm:].copy()
    F = {k: v[warm:] for k, v in F.items()}

    n = len(df)
    op, cl = df['open'].to_numpy(float), df['close'].to_numpy(float)

    exec_px = np.empty(n, float)
    exec_px[:-1] = op[1:]
    exec_px[-1] = cl[-1]
    cost = 2.0 * (cfg['FEE_RATE'] + cfg['SLIPPAGE'])

    # 精确截取到回测时间段的基准收益和回测天数
    bench_ret = float((cl[-1] / cl[0] - 1.0) * 100)
    bt_days = float(n * bm / 1440.0)  # 1440分钟 = 1天

    # 优化点：基于向量化累加彻底消灭计算资金费率扣除的 for 循环
    fr_arr = df['funding_settlement'].to_numpy(float)
    fr_cumsum = np.zeros(n + 1, dtype=float)
    np.cumsum(fr_arr, out=fr_cumsum[1:])
    max_tr = n // 2 + 2

    def _calc_rets(entry_arr, exit_arr):
        if HAS_NUMBA:
            ent_, ext_ = _core_static(entry_arr, exit_arr, n, cfg['COOLDOWN_BARS'], max_tr)
        else:
            eidx = np.flatnonzero(entry_arr).astype(np.int64)
            xidx = np.flatnonzero(exit_arr).astype(np.int64)
            ent_, ext_ = _match_static_ss(eidx, xidx, n, cfg['COOLDOWN_BARS'], max_tr)

        if ent_.size < 1:
            return np.array([], dtype=np.int64), np.array([], dtype=np.int64), np.array([], dtype=float), np.array([],
                                                                                                                   dtype=float)

        start_idx = np.minimum(ent_ + 1, n)
        end_idx = np.minimum(ext_ + 1, n)
        fc_ = fr_cumsum[end_idx] - fr_cumsum[start_idx]

        # 扣除手续费、滑点和资金费率
        r_ = exec_px[ext_] / exec_px[ent_] - 1.0 - cost - fc_
        ok = np.isfinite(r_)
        return ent_[ok], ext_[ok], r_[ok], fc_[ok]

    # 分别计算通过过滤器的交易 和 被拦截的废弃交易
    ent, ext, rets, funding_costs = _calc_rets(F['EXIT_UPPER_WICK_REJECTION'], F['ENTRY_INSIDE_BREAK_VOLUME'])
    _, _, rets_rej, _ = _calc_rets(F['EXIT_REJECTED'], F['ENTRY_INSIDE_BREAK_VOLUME'])

    if ent.size < 1:
        return None, [], [], [], []

    split_bar = int(n * cfg['OOS_SPLIT'])
    # 将基准收益和时长存入行数据
    row = dict(coin=coin, pool='A_山寨永续', bench_ret=bench_ret, bt_days=bt_days)

    row.update(trade_stats(rets, ent, ext, bm, n))
    m_is = ent < split_bar
    row.update(trade_stats(rets[~m_is], ent[~m_is], ext[~m_is], bm, n - split_bar, prefix='oos_'))

    # 资金费率统计
    row['total_fr_cost'] = float(funding_costs.sum() * 100)
    row['avg_fr_cost'] = float(funding_costs.mean() * 100) if len(funding_costs) > 0 else 0.0

    # 计算复合指标
    row['excess'] = row['avg_ret'] - row['bench_ret']
    row['edge_100h'] = (row['excess'] / row['avg_hold_h'] * 100) if row.get('avg_hold_h', 0) > 0 else np.nan
    row['filtered_out'] = True

    return row, rets.tolist(), rets[m_is].tolist(), rets[~m_is].tolist(), rets_rej.tolist()


# ======================================================================
# 5. 主流程
# ======================================================================
def main(cfg=CFG):
    data_dir = cfg['DATA_DIR']
    if not os.path.isdir(data_dir):
        print(f"❌ 数据目录不存在: {data_dir}")
        return

    kfiles = sorted(f for f in os.listdir(data_dir) if f.endswith('_USDT_USDT_1m_kline.csv'))
    if not kfiles:
        print("❌ 未发现 K线数据文件。")
        return

    # 【本次修改的核心部分】: 让控制台输出完整的参数组合，排查进度一目了然
    print("=" * 100)
    if 'SEARCH_PARAMS' in cfg:
        p_str = " | ".join(f"{k}={v}" for k, v in cfg['SEARCH_PARAMS'].items())
        print(f" 🚀 正在执行参数组合: {p_str} | numba={'ON' if HAS_NUMBA else 'OFF'}")
    else:
        print(f" 🚀 定制策略回测启动 | bar={cfg['BAR_MINUTES']}min | numba={'ON' if HAS_NUMBA else 'OFF'}")
    print("=" * 100)

    # ------------------------------------------------------------------
    # 【新增逻辑】：在跑主循环前，获取整个币种池的数据计算24H涨跌幅截面排名
    # ------------------------------------------------------------------
    valid_coins_info = []
    for kf in kfiles:
        coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
        if cfg['COINS'] and coin not in cfg['COINS']: continue
        oi_f = os.path.join(data_dir, f'{coin}_USDT_USDT_5m_oi.csv')
        fr_f = os.path.join(data_dir, f'{coin}_USDT_USDT_funding_rates.csv')
        if not (os.path.exists(oi_f) and os.path.exists(fr_f)): continue

        try:
            # 只需触发读取以加入 _DF_CACHE
            df_chk = _load_symbol_cached(os.path.join(data_dir, kf), oi_f, fr_f, cfg['BAR_MINUTES'])
            if len(df_chk) >= 800:
                valid_coins_info.append((kf, coin, oi_f, fr_f))
        except Exception:
            pass

    if not valid_coins_info:
        print("❌ 未发现符合条件的有效币种数据。")
        return

    bm = cfg['BAR_MINUTES']
    top_k = cfg.get('SEARCH_PARAMS', {}).get('CROSS_RANK_K', 9999)
    rank_mode = cfg.get('SEARCH_PARAMS', {}).get('RANK_MODE', cfg.get('RANK_MODE', 'both'))

    # 极速计算并缓存全局 24H 涨跌幅截面，消除由于周期带来的多次运算
    if bm not in _CROSS_CACHE:
        close_dict = {}
        for kf, coin, oi_f, fr_f in valid_coins_info:
            dft = _load_symbol_cached(os.path.join(data_dir, kf), oi_f, fr_f, bm)
            close_dict[coin] = dft['close']

        df_all_close = pd.DataFrame(close_dict)
        bars_24h = int(24 * 60 / bm)
        # 计算过去 24H 的收益率
        ret_24h = df_all_close.pct_change(periods=bars_24h)
        # 排名 (1 = 涨幅最大)
        rank_24h = ret_24h.rank(axis=1, ascending=False, method='min')
        # 每根K线的总有效交易对数量
        valid_count = ret_24h.notna().sum(axis=1)

        _CROSS_CACHE[bm] = (rank_24h, valid_count)

    rank_24h, valid_count = _CROSS_CACHE[bm]
    # 判断是否为涨幅前 K 名
    is_top_k = rank_24h.le(top_k)
    # 判断是否为跌幅前 K 名 (即后 K 名)
    # 使用 subtract 防止 reshape/broadcasting 产生的异常报错
    is_bottom_k = rank_24h.subtract(valid_count, axis=0).gt(-top_k)

    # 根据动态 RANK_MODE 构建掩码池
    if rank_mode == 'top':
        cross_mask_df = is_top_k
    elif rank_mode == 'bottom':
        cross_mask_df = is_bottom_k
    else:
        cross_mask_df = is_top_k | is_bottom_k
    # ------------------------------------------------------------------

    results = []
    all_rets = []
    all_is_rets = []
    all_oos_rets = []
    all_rej_rets = []

    for kf, coin, oi_f, fr_f in valid_coins_info:
        try:
            # 采用内存读取缓存方式
            df = _load_symbol_cached(os.path.join(data_dir, kf), oi_f, fr_f, cfg['BAR_MINUTES'])

            # 将该币种的截面布尔列摘出（重置索引以完全对齐回测使用的df）
            if coin in cross_mask_df.columns:
                coin_mask_series = cross_mask_df[coin].reindex(df.index).fillna(False)
            else:
                coin_mask_series = pd.Series(True, index=df.index)

            res = mine_symbol(coin, df, cfg, coin_mask_series)

            if res is not None and res[0] is not None:
                res_dict, rets_list, is_rets_list, oos_rets_list, rej_rets_list = res
                results.append(res_dict)
                all_rets.extend(rets_list)
                all_is_rets.extend(is_rets_list)
                all_oos_rets.extend(oos_rets_list)
                all_rej_rets.extend(rej_rets_list)

        except Exception as e:
            pass

    if not results:
        print("\n⚠️ 所有币种均未产生交易信号。")
        return

    df_res = pd.DataFrame(results)
    df_res.sort_values('sum_ret', ascending=False, inplace=True)
    df_res['oos_trades'] = df_res['oos_trades'].fillna(0).astype(int)

    # ==========================
    # 打印底部全局统计摘要
    # ==========================
    all_rets_arr = np.array(all_rets) * 100
    all_is_rets_arr = np.array(all_is_rets) * 100
    all_oos_rets_arr = np.array(all_oos_rets) * 100
    all_rej_rets_arr = np.array(all_rej_rets) * 100

    total_trades = len(all_rets_arr)
    pooled_expected = all_rets_arr.mean() if total_trades > 0 else 0
    std_ret = all_rets_arr.std(ddof=1) if total_trades > 1 else 0
    cluster_t = pooled_expected / (std_ret / np.sqrt(total_trades)) if std_ret > 0 else float('nan')

    # IS 期望 与 排雷期望 计算
    is_pooled_expected = all_is_rets_arr.mean() if len(all_is_rets_arr) > 0 else 0.0
    rej_pooled_expected = all_rej_rets_arr.mean() if len(all_rej_rets_arr) > 0 else 0.0
    # 排雷指标：池化期望 - 被拦截信号期望（大于0证明成功拦截垃圾信号，小于0证明错杀了优质信号）
    mine_sweeper_metric = pooled_expected - rej_pooled_expected if len(all_rej_rets_arr) > 0 else 0.0

    # 全局新增指标计算
    global_win_rate = float((all_rets_arr > 0).mean() * 100) if total_trades > 0 else 0.0
    global_avg_win = all_rets_arr[all_rets_arr > 0].mean() if (all_rets_arr > 0).any() else 0
    global_avg_loss = abs(all_rets_arr[all_rets_arr < 0].mean()) if (all_rets_arr < 0).any() else 1e-9
    global_pl_ratio = global_avg_win / global_avg_loss

    # 获取池子中单个币种曾发生过的最劣最大回撤和平均最大回撤
    global_max_dd_worst = df_res['max_dd'].max() if 'max_dd' in df_res.columns and not df_res[
        'max_dd'].isna().all() else 0.0
    global_max_dd_mean = df_res['max_dd'].mean() if 'max_dd' in df_res.columns and not df_res[
        'max_dd'].isna().all() else 0.0

    is_loss_global = all_rets_arr < 0
    if is_loss_global.any():
        loss_runs_g = []
        curr_g = 0
        for l in is_loss_global:
            if l:
                curr_g += 1
            else:
                if curr_g > 0: loss_runs_g.append(curr_g)
                curr_g = 0
        if curr_g > 0: loss_runs_g.append(curr_g)
        global_max_consec_loss = max(loss_runs_g) if loss_runs_g else 0
    else:
        global_max_consec_loss = 0

    total_fr_cost_sum = df_res['total_fr_cost'].sum() if 'total_fr_cost' in df_res.columns else 0.0

    oos_trades = len(all_oos_rets_arr)
    oos_pooled_expected = all_oos_rets_arr.mean() if oos_trades > 0 else 0
    oos_retention = (oos_pooled_expected / pooled_expected * 100) if pooled_expected != 0 else 0

    profitable_coins = (df_res['sum_ret'] > 0).sum()
    total_coins = len(df_res)
    total_pnl = df_res['sum_ret'].sum()

    # 提取全局基准统计
    total_bench_ret = df_res['bench_ret'].sum() if 'bench_ret' in df_res.columns else 0.0
    avg_bt_days = df_res['bt_days'].mean() if 'bt_days' in df_res.columns else 0.0
    alpha_ret = total_pnl - total_bench_ret

    max_coin_idx = df_res['sum_ret'].idxmax()
    max_coin = df_res.loc[max_coin_idx, 'coin']
    max_coin_pnl = df_res.loc[max_coin_idx, 'sum_ret']
    pnl_ex_best = total_pnl - max_coin_pnl
    max_contrib_pct = (max_coin_pnl / total_pnl * 100) if total_pnl > 0 else 0

    print("\n")
    print(f"    原始有效池 / 最终产出信号币种                    : {len(valid_coins_info)} / {total_coins}")
    print(f"    平均回测时长                                    : {avg_bt_days:.1f} 天")
    print(f"    总笔数                                         : {total_trades}")
    print(f"    全局胜率                                        : {global_win_rate:.2f}%")
    print(f"    池化单笔期望 (总体)                              : +{pooled_expected:.4f}%")
    print(f"    IS (前半段) 池化期望                             : +{is_pooled_expected:.4f}%")
    print(f"    OOS (后半段) 总笔数 / 池化期望                   : {oos_trades} / +{oos_pooled_expected:.4f}%")
    print(f"    OOS 收益留存率                                 : {oos_retention:.1f}%")
    print(f"    被拦截/过滤废弃的信号期望                        : {rej_pooled_expected:.4f}%")
    print(f"    排雷指标 (大于0证明拦截了差交易,小于0证明误杀)      : {mine_sweeper_metric:.4f}%")
    print(f"    cluster_t                                    : {cluster_t:.2f}")
    print(f"    全局盈亏比 (P/L Ratio)                         : {global_pl_ratio:.2f}")
    print(
        f"    单币最大回撤 (均值 / 最劣)                       : {global_max_dd_mean:.2f}% / {global_max_dd_worst:.2f}%")
    print(f"    全局最大连续亏损                                : {global_max_consec_loss}")
    print(f"    盈利币 / 总币                                  : {profitable_coins} / {total_coins}")
    print(f"    策略总盈亏                                      : {total_pnl:.1f}%")
    print(f"    剔除妖币 [{max_coin}] 后剩余盈亏                 : {pnl_ex_best:.1f}%")
    print(f"    同期基准总收益 (Buy&Hold)                       : {total_bench_ret:.1f}%")
    print(f"    策略Alpha (总盈亏 - 基准)                        : {alpha_ret:.1f}%")
    print(f"    总资金费率扣除                                  : {total_fr_cost_sum:.2f}%")

    # 收集参数平原数据 (新增胜率和最大回撤)
    if 'SEARCH_PARAMS' in cfg:
        GLOBAL_PLATEAU_RESULTS.append({
            'params': cfg['SEARCH_PARAMS'],
            'total_trades': total_trades,
            'global_win_rate': global_win_rate,
            'is_expected': is_pooled_expected,
            'oos_expected': oos_pooled_expected,
            'pooled_expected': pooled_expected,
            'mine_sweeper': mine_sweeper_metric,
            'cluster_t': cluster_t,
            'oos_retention': oos_retention,
            'global_pl_ratio': global_pl_ratio,
            'max_dd_worst': global_max_dd_worst,
            'global_max_consec_loss': global_max_consec_loss,
            'total_pnl': total_pnl,
            'pnl_ex_best': pnl_ex_best,
            'total_bench_ret': total_bench_ret,
            'alpha': alpha_ret
        })


if __name__ == '__main__':
    # 固定使用表现最好的 15分钟 周期进行参数平原搜索
    CFG['BAR_MINUTES'] = 15

    # 划定核心参数的搜索空间 (新增了 RANK_MODE)
    param_grid = {
        'UPPER_WICK_THRESH': [0.60],  # 上影线占比阈值
        'VOL_QUANTILE': [0.95],  # 成交量分位数
        'HIGH_CLOSE_THRESH': [0.90],  # 高位收盘价阈值
        'CROSS_RANK_K': [5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100],
        'BAR_MINUTES': [15, 30, 60],
        'RANK_MODE': ['both', 'top', 'bottom']  # <--- 新增搜索空间
    }

    keys = list(param_grid.keys())
    combinations = list(itertools.product(*param_grid.values()))

    print(f"🚀 启动参数平原搜索 | 周期: {CFG['BAR_MINUTES']}min | 组合数: {len(combinations)}")

    for combo in combinations:
        params = dict(zip(keys, combo))
        CFG['SEARCH_PARAMS'] = params
        # 修正: 保证字典里传入了新的 BAR_MINUTES 时覆盖回主流程中，以便不同周期重采样生效并缓存命中
        CFG['BAR_MINUTES'] = params.get('BAR_MINUTES', CFG['BAR_MINUTES'])
        main(CFG)

    # ==========================
    # 最终分析：寻找参数平原
    # ==========================
    print("\n\n" + "=" * 100)
    print(" 🏆 参数平原分析结果 (Parameter Plateau Analysis)")
    print("=" * 100)

    if GLOBAL_PLATEAU_RESULTS:
        df_plateau = pd.DataFrame(GLOBAL_PLATEAU_RESULTS)
        # 将 params 字典展开为列，方便查看
        params_df = pd.DataFrame(df_plateau['params'].tolist())
        df_final = pd.concat([params_df, df_plateau.drop(columns=['params'])], axis=1)

        # 排序：优先看 OOS 留存率、Cluster T、池化期望
        df_final.sort_values(by=['oos_retention', 'cluster_t', 'pooled_expected'], ascending=False, inplace=True)

        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print(df_final.to_string(index=False))

        print("\n💡 稳健性建议: 不要盲目选择排名第一的参数！请观察表格，寻找那些在参数微调时，")
        print("   OOS留存率、Cluster T 和 池化期望 依然保持高位的'平原'区域（即相邻参数组合表现都很稳定的区域）。")
        print("   ※ 重点观察新增防过拟合指标：")
        print("     [is_expected] vs [oos_expected]: 谨防只是前半段行情有效，后半段亏损。")
        print("     [mine_sweeper]: 大于 0 说明横截面过滤算法拦截了无效交易，小于 0 则说明误杀了优质收益。")
        print("     [pnl_ex_best]: 与 total_pnl 对比，如果该值大幅缩水甚至为负，说明完全靠一个妖币硬顶！")
        print("     [alpha]: 若总盈亏很高但 alpha 为负，说明策略仅仅是吃到了大盘红利。")
    else:
        print("⚠️ 未收集到任何回测结果。")