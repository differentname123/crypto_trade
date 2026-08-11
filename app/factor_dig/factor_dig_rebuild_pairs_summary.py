# -*- coding: utf-8 -*-
"""
================================================================================
 REBUILD (Optimal Balance Version)
--------------------------------------------------------------------------------
 · 汇总表 (SUMMARY)：保留所有的宏观策略表现指标（最大回撤、夏普、敞口等），便于人工复盘。
 · 明细表 (ALL)：仅保留下游分析真正使用的核心列，极大降低磁盘与 IO 内存占用。
 · OOM 根治：彻底废弃需要驻留千万行级别内存的中位数(EXACT_MEDIAN)计算。
================================================================================
"""
from __future__ import annotations

import os
import math
import gc
import numpy as np
import pandas as pd

try:
    from scipy.special import ndtr as _norm_cdf
except Exception:
    _norm_cdf = np.vectorize(lambda z: 0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))

RCFG = dict(
    OUT_DIRS=[f'./factor_out_{bm}m' for bm in (1, 5, 15, 30, 60)],
    REBUILD_PAIRS_ALL=True,
    REBUILD_SUMMARY=True,
    CHUNKSIZE=300_000,
    TRIALS_MODE='total',
    TOPN_PRINT=20,
)

KEY_COLS = ['entry_factor', 'exit_factor', 'filter_mode']

# 【完美平衡 1】：这是微观明细表 pairs_ALL.csv 需要写入的极简字段（防止几千万行把磁盘撑爆）
ALL_PAIRS_NEEDED = ['trades', 'win_rate', 'win_hold_bars', 'loss_hold_bars', 'sum_ret']

# 【完美平衡 2】：恢复了 SUMMARY 汇总表里的所有宏观核心指标，保证你复盘时什么都能看到！
SUM_COLS = ['trades', 'sum_ret', 'oos_sum_ret',
            'ret_q1', 'ret_q2', 'ret_q3', 'ret_q4',
            'trades_q1', 'trades_q2', 'trades_q3', 'trades_q4']

MEAN_COLS = ['sum_ret', 'avg_ret', 'win_rate', 'max_dd', 'avg_hold_h',
             'skew', 'kurt', 'exposure', 'equity_r2', 'corr_btc',
             'down_market_win_rate', 'cvar_5', 'oos_sharpe', 'oos_pt_sharpe']

TRIAL_COLS = ['n_trials_combos', 'n_trials_modes', 'n_trials_total', 'n_trials_alive']

# 内存读取时需要的交集
NEED_COLS = list(dict.fromkeys(KEY_COLS + ALL_PAIRS_NEEDED + SUM_COLS + MEAN_COLS + TRIAL_COLS))

FINAL_ORDER = KEY_COLS + [
    'n_coins', 'total_trades', 'sum_ret_all', 'mean_sum_ret',
    'mean_avg_ret', 'mean_win_rate', 'mean_max_dd', 'mean_hold_h', 'oos_sum_all',
    'mean_skew', 'mean_kurt', 'mean_exposure', 'mean_equity_r2', 'mean_corr_btc',
    'mean_down_market_win_rate', 'mean_cvar_5', 'mean_oos_sharpe', 'mean_oos_pt_sharpe',
    'sum_ret_q1', 'sum_trades_q1', 'sum_ret_q2', 'sum_trades_q2',
    'sum_ret_q3', 'sum_trades_q3', 'sum_ret_q4', 'sum_trades_q4',
    'coin_positive_rate', 'score', 'avg_trades_per_coin',
    'total_trials', 'deflated_sharpe',
    'total_trials_legacy', 'deflated_sharpe_legacy', 'trials_mode',
]


def _atomic_replace(tmp_path, final_path):
    os.replace(tmp_path, final_path)


def _atomic_to_csv(df, path):
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False, encoding='utf-8-sig')
    _atomic_replace(tmp, path)


def _list_coin_files(out_dir):
    exclude = {'pairs_ALL.csv', 'pairs_CROSS_COIN_SUMMARY.csv'}
    fs = []
    for f in sorted(os.listdir(out_dir)):
        if not f.startswith('pairs_') or not f.endswith('.csv'): continue
        if f in exclude: continue
        fs.append(os.path.join(out_dir, f))
    return fs


class GroupAccumulator:
    def __init__(self):
        self.key2gid = {}
        self.keys = []
        self.n = 0

        self.acc_names = (['rows', 'pos']
                          + [f'{c}__sum' for c in SUM_COLS]
                          + [f'{c}__msum' for c in MEAN_COLS]
                          + [f'{c}__mcnt' for c in MEAN_COLS])
        self.col_of = {nm: i for i, nm in enumerate(self.acc_names)}
        self.cap = 1 << 16
        self.data = np.zeros((self.cap, len(self.acc_names)), dtype=np.float64)

    def _ensure(self, need):
        if need <= self.data.shape[0]: return
        cap = self.data.shape[0]
        while cap < need: cap *= 2
        new = np.zeros((cap, self.data.shape[1]), dtype=np.float64)
        new[:self.n] = self.data[:self.n]
        self.data = new

    def _gids(self, keys: pd.Series) -> np.ndarray:
        mapped = keys.map(self.key2gid)
        miss = mapped.isna().to_numpy()
        if miss.any():
            new_keys = pd.unique(keys.to_numpy(dtype=object)[miss])
            start = self.n
            need = start + len(new_keys)
            self._ensure(need)
            for i, k in enumerate(new_keys):
                self.key2gid[k] = start + i
            self.keys.extend(list(new_keys))
            self.n = need
            mapped = keys.map(self.key2gid)
        return mapped.to_numpy(dtype=np.int64)

    def update(self, df: pd.DataFrame):
        n = len(df)
        if n == 0: return

        key = (df['entry_factor'].astype(str) + '\x01'
               + df['exit_factor'].astype(str) + '\x01'
               + df['filter_mode'].astype(str))
        gid = self._gids(key)
        D = self.data

        if np.unique(gid).size != gid.size:
            def add(col, vals):
                np.add.at(D, (gid, col), vals)
        else:
            def add(col, vals):
                D[gid, col] += vals

        def num(c):
            if c in df.columns:
                return pd.to_numeric(df[c], errors='coerce').to_numpy(dtype=np.float64)
            return np.full(n, np.nan)

        sr = num('sum_ret')

        add(self.col_of['rows'], 1.0)
        add(self.col_of['pos'], (sr > 0).astype(np.float64))

        for c in SUM_COLS:
            v = sr if c == 'sum_ret' else num(c)
            add(self.col_of[f'{c}__sum'], np.where(np.isnan(v), 0.0, v))

        for c in MEAN_COLS:
            v = sr if c == 'sum_ret' else num(c)
            ok = ~np.isnan(v)
            add(self.col_of[f'{c}__msum'], np.where(ok, v, 0.0))
            add(self.col_of[f'{c}__mcnt'], ok.astype(np.float64))

    def finalize(self) -> pd.DataFrame:
        D = self.data[:self.n]
        col = self.col_of

        split = [k.split('\x01') for k in self.keys[:self.n]]
        out = pd.DataFrame(split, columns=KEY_COLS)

        def _sum(c): return D[:, col[f'{c}__sum']]

        def _mean(c):
            s = D[:, col[f'{c}__msum']]
            cnt = D[:, col[f'{c}__mcnt']]
            with np.errstate(invalid='ignore', divide='ignore'):
                return np.where(cnt > 0, s / np.where(cnt > 0, cnt, 1.0), np.nan)

        rows = D[:, col['rows']]
        out['n_coins'] = rows.astype(np.int64)
        out['total_trades'] = np.rint(_sum('trades')).astype(np.int64)
        out['sum_ret_all'] = _sum('sum_ret')
        out['mean_sum_ret'] = _mean('sum_ret')

        out['mean_avg_ret'] = _mean('avg_ret')
        out['mean_win_rate'] = _mean('win_rate')
        out['mean_max_dd'] = _mean('max_dd')
        out['mean_hold_h'] = _mean('avg_hold_h')
        out['oos_sum_all'] = _sum('oos_sum_ret')
        out['mean_skew'] = _mean('skew')
        out['mean_kurt'] = _mean('kurt')
        out['mean_exposure'] = _mean('exposure')
        out['mean_equity_r2'] = _mean('equity_r2')
        out['mean_corr_btc'] = _mean('corr_btc')
        out['mean_down_market_win_rate'] = _mean('down_market_win_rate')
        out['mean_cvar_5'] = _mean('cvar_5')
        out['mean_oos_sharpe'] = _mean('oos_sharpe')
        out['mean_oos_pt_sharpe'] = _mean('oos_pt_sharpe')

        for q in (1, 2, 3, 4):
            out[f'sum_ret_q{q}'] = _sum(f'ret_q{q}')
            out[f'sum_trades_q{q}'] = np.rint(_sum(f'trades_q{q}')).astype(np.int64)

        out['coin_positive_rate'] = np.where(rows > 0, D[:, col['pos']] / np.where(rows > 0, rows, 1.0), np.nan)
        out['score'] = (np.nan_to_num(out['mean_avg_ret'].to_numpy(float), nan=0.0)
                        * np.sqrt(np.clip(out['total_trades'].to_numpy(float), 1, None))
                        * out['coin_positive_rate'].to_numpy(float))
        out['avg_trades_per_coin'] = (out['total_trades'].to_numpy(float)
                                      / np.clip(out['n_coins'].to_numpy(float), 1, None))
        return out


def calc_dsr_vec(summ: pd.DataFrame, total_trials: int) -> np.ndarray:
    n = len(summ)
    if total_trials is None or total_trials <= 1:
        return np.full(n, np.nan)

    sr = summ['mean_oos_pt_sharpe'].to_numpy(dtype=float)
    T = np.maximum(summ['avg_trades_per_coin'].to_numpy(dtype=float), 3.0)
    sk = np.nan_to_num(summ['mean_skew'].to_numpy(dtype=float), nan=0.0)
    ku = np.nan_to_num(summ['mean_kurt'].to_numpy(dtype=float), nan=0.0)

    emsr = np.sqrt(2.0 * np.log(total_trials))
    with np.errstate(invalid='ignore'):
        var_sr = (1.0 - sk * sr + (ku + 2.0) / 4.0 * sr ** 2) / T
        var_sr = np.where(var_sr <= 0, 1e-6, var_sr)
        z = (sr - emsr) / np.sqrt(var_sr)
        dsr = np.asarray(_norm_cdf(z), dtype=float)
    dsr[~np.isfinite(sr)] = np.nan
    return dsr


def rebuild_dir(out_dir, rcfg=RCFG):
    if not os.path.isdir(out_dir): return
    files = _list_coin_files(out_dir)
    if not files: return

    print("=" * 78)
    print(f"📂 {os.path.abspath(out_dir)} | 汇总全部指标，保留下游防伪明细")
    print("=" * 78)

    if not (rcfg['REBUILD_PAIRS_ALL'] or rcfg['REBUILD_SUMMARY']): return

    all_path = os.path.join(out_dir, 'pairs_ALL.csv')
    all_tmp = all_path + '.tmp'
    fo = None
    header_pending = True

    # 获取列名，做安全过滤
    head_df = pd.read_csv(files[0], nrows=0)
    usecols = [c for c in NEED_COLS if c in head_df.columns]

    # pairs_ALL.csv 只写入明细表真正需要的极简列，防止磁盘与分析脚本爆炸
    write_cols = [c for c in (KEY_COLS + ALL_PAIRS_NEEDED) if c in head_df.columns]

    if rcfg['REBUILD_PAIRS_ALL']:
        fo = open(all_tmp, 'w', newline='', encoding='utf-8-sig')

    acc = GroupAccumulator() if rcfg['REBUILD_SUMMARY'] else None
    trials = {c: 0 for c in TRIAL_COLS}
    n_rows_total = 0

    try:
        for i, f in enumerate(files, 1):
            n_rows_file = 0
            for chunk in pd.read_csv(f, usecols=usecols, chunksize=rcfg['CHUNKSIZE'], low_memory=False):
                # 1. 向磁盘写明细表：只写最关键的 8 个列！
                if fo is not None:
                    chunk[write_cols].to_csv(fo, index=False, header=header_pending)
                    header_pending = False

                # 2. 累加宏观汇总表：使用全部指标（max_dd, oos_sharpe等）在内存中累加
                if acc is not None:
                    acc.update(chunk)

                for tc in TRIAL_COLS:
                    if tc in chunk.columns:
                        v = pd.to_numeric(chunk[tc], errors='coerce').max()
                        if pd.notna(v):
                            trials[tc] = max(trials[tc], int(v))

                n_rows_file += len(chunk)
                n_rows_total += len(chunk)

            print(f"   [{i}/{len(files)}] {os.path.basename(f):<38s} rows={n_rows_file:>10,} | 累计 {n_rows_total:,}")
            gc.collect()
    finally:
        if fo is not None:
            fo.close()

    if rcfg['REBUILD_PAIRS_ALL']:
        _atomic_replace(all_tmp, all_path)
        print(f"✅ pairs_ALL.csv 已重建完毕(明细字段极简版)")

    if acc is None: return

    summ = acc.finalize()
    del acc
    gc.collect()

    legacy_trials = trials.get('n_trials_combos', 0)
    total_trials = trials.get('n_trials_total', 0)
    alive_trials = trials.get('n_trials_alive', 0)

    if total_trials <= 1:
        total_trials = int(
            summ['entry_factor'].nunique() * summ['exit_factor'].nunique() * summ['filter_mode'].nunique())
        legacy_trials = int(summ['entry_factor'].nunique() * summ['exit_factor'].nunique())

    mode = rcfg['TRIALS_MODE']
    used_trials = {'total': total_trials, 'combos': legacy_trials, 'alive': alive_trials}.get(mode, total_trials)
    if used_trials <= 1: used_trials = total_trials

    summ['total_trials'] = int(used_trials)
    summ['deflated_sharpe'] = calc_dsr_vec(summ, used_trials)
    summ['total_trials_legacy'] = int(max(legacy_trials, 0))
    summ['deflated_sharpe_legacy'] = calc_dsr_vec(summ, legacy_trials)
    summ['trials_mode'] = mode

    summ = summ.reindex(columns=[c for c in FINAL_ORDER if c in summ.columns])
    summ.sort_values('score', ascending=False, inplace=True)

    sum_path = os.path.join(out_dir, 'pairs_CROSS_COIN_SUMMARY.csv')
    _atomic_to_csv(summ, sum_path)
    print(f"✅ pairs_CROSS_COIN_SUMMARY.csv 已重建完毕(全量宏观指标保留): -> {sum_path}")
    print()


def main(rcfg=RCFG):
    for d in rcfg['OUT_DIRS']:
        rebuild_dir(d, rcfg)


if __name__ == '__main__':
    main()