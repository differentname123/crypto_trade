# -*- coding: utf-8 -*-
"""
================================================================================
 ALT-COIN LAUNCH FACTOR MINER (固定组合回测 - 统计日志精排版)
 策略逻辑:
   - 入场: EXIT_UPPER_WICK_REJECTION (高位长上影线+放量拒接)
   - 出场: ENTRY_INSIDE_BREAK_VOLUME (孕线上破+放量)
================================================================================
"""
from __future__ import annotations
import os
import math
import warnings

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
# 2. 数据加载与对齐
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
    for c in ['open', 'high', 'low', 'close']: df = df[df[c] > 0]
    return df


# ======================================================================
# 3. 极简因子计算
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


def build_factors(df, P):
    W, N = P['W'], P['N']
    mp = P['MINP_W']
    o, h, l, c = df['open'], df['high'], df['low'], df['close']
    v = df['volume']

    def QT(s, p): return s.rolling(W, min_periods=mp).quantile(p).shift(1)

    def bs(s, k=1): return s.shift(k, fill_value=False)

    maxH_N = h.rolling(N, min_periods=max(2, N // 2)).max()
    rng = (h - l) + EPS
    uw = (h - np.maximum(o, c)) / rng

    F = {}
    F['KLINE_LONG_UPPER_WICK'] = uw > 0.50
    F['VOLUME_SPIKE'] = v > QT(v, 0.95)
    F['KLINE_INSIDE_BAR'] = (h < h.shift(1)) & (l > l.shift(1))

    # 入场信号: 高位长上影且放量
    F['EXIT_UPPER_WICK_REJECTION'] = (c / (maxH_N + EPS) > 0.95) & F['KLINE_LONG_UPPER_WICK'] & F['VOLUME_SPIKE']
    # 出场信号: 孕线之后突破且放量
    F['ENTRY_INSIDE_BREAK_VOLUME'] = bs(F['KLINE_INSIDE_BAR']) & (c > h.shift(1)) & F['VOLUME_SPIKE']

    out = {}
    for k_ in ['EXIT_UPPER_WICK_REJECTION', 'ENTRY_INSIDE_BREAK_VOLUME']:
        out[k_] = np.ascontiguousarray(F[k_].fillna(False).to_numpy(dtype=bool))
    return out


# ======================================================================
# 4. 绩效计算与匹配回测
# ======================================================================
def trade_stats(rets, ent, ext, bar_minutes, n_bars, prefix=''):
    T = int(len(rets))
    d = {prefix + 'trades': T}
    if T == 0:
        for k in ['win_rate', 'sum_ret', 'avg_ret', 'med_ret', 'profit_factor', 'max_dd', 'avg_hold_h', 'exposure',
                  'max_win', 'max_loss']:
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

    return d


def mine_symbol(coin, df, cfg):
    bm = cfg['BAR_MINUTES']
    P = make_params(bm, len(df))
    F = build_factors(df, P)

    warm = min(P['WARMUP'], len(df) - 100)
    if warm < 0 or len(df) - warm < 200:
        return None, [], []

    df = df.iloc[warm:].copy()
    F = {k: v[warm:] for k, v in F.items()}

    n = len(df)
    op, cl = df['open'].to_numpy(float), df['close'].to_numpy(float)

    exec_px = np.empty(n, float)
    exec_px[:-1] = op[1:]
    exec_px[-1] = cl[-1]
    cost = 2.0 * (cfg['FEE_RATE'] + cfg['SLIPPAGE'])
    bench_ret = float((cl[-1] / cl[0] - 1.0) * 100)

    # 核心匹配
    entry_arr = F['EXIT_UPPER_WICK_REJECTION']
    exit_arr = F['ENTRY_INSIDE_BREAK_VOLUME']

    max_tr = n // 2 + 2
    if HAS_NUMBA:
        ent, ext = _core_static(entry_arr, exit_arr, n, cfg['COOLDOWN_BARS'], max_tr)
    else:
        eidx = np.flatnonzero(entry_arr).astype(np.int64)
        xidx = np.flatnonzero(exit_arr).astype(np.int64)
        ent, ext = _match_static_ss(eidx, xidx, n, cfg['COOLDOWN_BARS'], max_tr)

    if ent.size < 1:
        return None, [], []

    rets = exec_px[ext] / exec_px[ent] - 1.0 - cost
    ok = np.isfinite(rets)
    ent, ext, rets = ent[ok], ext[ok], rets[ok]

    if ent.size < 1:
        return None, [], []

    split_bar = int(n * cfg['OOS_SPLIT'])
    row = dict(coin=coin, pool='A_山寨永续', bench_ret=bench_ret)

    row.update(trade_stats(rets, ent, ext, bm, n))
    m_is = ent < split_bar
    row.update(trade_stats(rets[~m_is], ent[~m_is], ext[~m_is], bm, n - split_bar, prefix='oos_'))

    # 计算复合指标
    row['excess'] = row['avg_ret'] - row['bench_ret']
    row['edge_100h'] = (row['excess'] / row['avg_hold_h'] * 100) if row.get('avg_hold_h', 0) > 0 else np.nan
    row['filtered_out'] = True

    return row, rets.tolist(), rets[~m_is].tolist()


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

    print("=" * 70)
    print(f" 🚀 定制策略回测启动 | bar={cfg['BAR_MINUTES']}min | numba={'ON' if HAS_NUMBA else 'OFF'}")
    print("=" * 70)

    results = []
    all_rets = []
    all_oos_rets = []

    for kf in kfiles:
        coin = kf.split('_USDT_USDT_1m_kline.csv')[0]
        if cfg['COINS'] and coin not in cfg['COINS']:
            continue

        oi_f = os.path.join(data_dir, f'{coin}_USDT_USDT_5m_oi.csv')
        fr_f = os.path.join(data_dir, f'{coin}_USDT_USDT_funding_rates.csv')

        if not (os.path.exists(oi_f) and os.path.exists(fr_f)):
            continue

        try:
            df = load_symbol(os.path.join(data_dir, kf), oi_f, fr_f, cfg['BAR_MINUTES'])
            if len(df) < 800:
                continue

            res_dict, rets_list, oos_rets_list = mine_symbol(coin, df, cfg)

            if res_dict is not None:
                results.append(res_dict)
                all_rets.extend(rets_list)
                all_oos_rets.extend(oos_rets_list)

        except Exception as e:
            pass

    if not results:
        print("\n⚠️ 所有币种均未产生交易信号。")
        return

    df_res = pd.DataFrame(results)
    df_res.sort_values('sum_ret', ascending=False, inplace=True)
    df_res['oos_trades'] = df_res['oos_trades'].fillna(0).astype(int)

    # ==========================
    # 打印特定排版的交易日志表
    # ==========================
    cols_to_show = [
        'coin', 'pool', 'trades', 'win_rate', 'avg_ret', 'bench_ret', 'excess', 'edge_100h',
        'sum_ret', 'max_dd', 'avg_hold_h', 'exposure', 'max_win', 'max_loss',
        'oos_trades', 'oos_avg_ret', 'filtered_out'
    ]

    print("\n" + "=" * 120)

    # 临时设定 pandas 格式输出以完全匹配目标对齐效果
    pd.set_option('display.float_format', lambda x: f'{x:.4f}')
    print(df_res[cols_to_show].to_string(index=False))

    # ==========================
    # 打印底部全局统计摘要
    # ==========================
    all_rets_arr = np.array(all_rets) * 100
    all_oos_rets_arr = np.array(all_oos_rets) * 100

    total_trades = len(all_rets_arr)
    pooled_expected = all_rets_arr.mean() if total_trades > 0 else 0
    std_ret = all_rets_arr.std(ddof=1) if total_trades > 1 else 0
    cluster_t = pooled_expected / (std_ret / np.sqrt(total_trades)) if std_ret > 0 else float('nan')

    oos_trades = len(all_oos_rets_arr)
    oos_pooled_expected = all_oos_rets_arr.mean() if oos_trades > 0 else 0
    oos_retention = (oos_pooled_expected / pooled_expected * 100) if pooled_expected != 0 else 0

    profitable_coins = (df_res['sum_ret'] > 0).sum()
    total_coins = len(df_res)
    total_pnl = df_res['sum_ret'].sum()

    max_coin_idx = df_res['sum_ret'].idxmax()
    max_coin = df_res.loc[max_coin_idx, 'coin']
    max_coin_pnl = df_res.loc[max_coin_idx, 'sum_ret']
    pnl_ex_best = total_pnl - max_coin_pnl
    max_contrib_pct = (max_coin_pnl / total_pnl * 100) if total_pnl > 0 else 0

    print("\n")
    print(f"    覆盖币种 / 通过过滤                               : {total_coins} / 0")
    print(f"    总笔数                                       : {total_trades}")
    print(f"    池化单笔期望                                    : +{pooled_expected:.4f}%")
    print(f"    cluster_t                                 : {cluster_t:.2f}")
    print(f"    OOS 总笔数 / 池化期望                            : {oos_trades} / +{oos_pooled_expected:.4f}%")
    print(f"    OOS 收益留存率                                 : {oos_retention:.1f}%")
    print(f"    盈利币 / 总币                                  : {profitable_coins} / {total_coins}")
    print(f"    总盈亏                                       : {total_pnl:.1f}%    去掉最好的币 {pnl_ex_best:.1f}%")
    print(f"    最大贡献币                                     : {max_coin} ({max_contrib_pct:.0f}%)")


if __name__ == '__main__':
    main(CFG)