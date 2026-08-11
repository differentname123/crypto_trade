# -*- coding: utf-8 -*-
"""
================================================================================
 REBUILD  ·  由 pairs_<coin>.csv 还原 pairs_ALL.csv / pairs_CROSS_COIN_SUMMARY.csv
--------------------------------------------------------------------------------
 · 单次流式遍历(chunk)：一边拼 pairs_ALL，一边做分组累加，不把全量读进内存
 · 聚合口径与挖掘脚本 1:1 对齐：
     sum   -> 跳过 NaN（同 pandas sum）
     mean  -> 非 NaN 求和 / 非 NaN 计数（同 pandas mean skipna）
     median-> 流式暂存 (gid:int32, sum_ret:float32) 后一次性精确求中位数
     n_coins / coin_positive_rate -> 组内行数（每币每组合恰好一行）
 · DSR 使用单币文件里"无感注入"的真实测试基数 n_trials_total(= 组合数 × 过滤模式数)
   同时额外输出 deflated_sharpe_legacy(只按组合数, 即旧口径的虚高值) 供对比
 · 原子落盘：先写 .tmp 再 os.replace
================================================================================
"""
from __future__ import annotations

import os
import math
import gc

import numpy as np
import pandas as pd

try:
    from scipy.special import ndtr as _norm_cdf  # 标准正态 CDF，向量化
except Exception:
    _norm_cdf = np.vectorize(lambda z: 0.5 * (1.0 + math.erf(z / math.sqrt(2.0))))

# ======================================================================
# 配置
# ======================================================================
RCFG = dict(
    # 要还原的目录（与挖掘脚本的 OUT_DIR 一致）
    OUT_DIRS=[f'./factor_out_{bm}m' for bm in (1, 5, 15, 30, 60)],

    REBUILD_PAIRS_ALL=True,   # 是否重建 pairs_ALL.csv（体积可能极大；只要汇总表可设 False，速度快 3~5 倍）
    REBUILD_SUMMARY=True,     # 是否重建 pairs_CROSS_COIN_SUMMARY.csv
    EXACT_MEDIAN=True,        # median_sum_ret 精确还原（需暂存 8 字节/行；关掉则该列为 NaN）

    CHUNKSIZE=300_000,        # 每次读入行数
    TRIALS_MODE='total',      # DSR 测试基数口径: 'total'(推荐) | 'combos'(旧口径,虚高) | 'alive'
    TOPN_PRINT=20,
)

KEY_COLS = ['entry_factor', 'exit_factor', 'filter_mode']

# 需要"求和"的列（NaN 视作 0）
SUM_COLS = ['trades', 'sum_ret', 'oos_sum_ret',
            'ret_q1', 'ret_q2', 'ret_q3', 'ret_q4',
            'trades_q1', 'trades_q2', 'trades_q3', 'trades_q4']

# 需要"求均值"的列（跳过 NaN）
MEAN_COLS = ['sum_ret', 'avg_ret', 'win_rate', 'max_dd', 'avg_hold_h',
             'skew', 'kurt', 'exposure', 'equity_r2', 'corr_btc',
             'down_market_win_rate', 'cvar_5', 'oos_sharpe', 'oos_pt_sharpe']

# 挖掘脚本注入的测试基数列
TRIAL_COLS = ['n_trials_combos', 'n_trials_modes', 'n_trials_total', 'n_trials_alive']

NEED_COLS = KEY_COLS + list(dict.fromkeys(SUM_COLS + MEAN_COLS)) + TRIAL_COLS

FINAL_ORDER = KEY_COLS + [
    'n_coins', 'total_trades', 'sum_ret_all', 'mean_sum_ret', 'median_sum_ret',
    'mean_avg_ret', 'mean_win_rate', 'mean_max_dd', 'mean_hold_h', 'oos_sum_all',
    'mean_skew', 'mean_kurt', 'mean_exposure', 'mean_equity_r2', 'mean_corr_btc',
    'mean_down_market_win_rate', 'mean_cvar_5', 'mean_oos_sharpe', 'mean_oos_pt_sharpe',
    'sum_ret_q1', 'sum_trades_q1', 'sum_ret_q2', 'sum_trades_q2',
    'sum_ret_q3', 'sum_trades_q3', 'sum_ret_q4', 'sum_trades_q4',
    'coin_positive_rate', 'score', 'avg_trades_per_coin',
    'total_trials', 'deflated_sharpe',
    # 还原增强列（原脚本没有，用于验证 DSR 虚高幅度）
    'total_trials_legacy', 'deflated_sharpe_legacy', 'trials_mode',
]


# ======================================================================
# 工具
# ======================================================================
def _atomic_replace(tmp_path, final_path):
    os.replace(tmp_path, final_path)


def _atomic_to_csv(df, path):
    tmp = f"{path}.tmp"
    df.to_csv(tmp, index=False, encoding='utf-8-sig')
    _atomic_replace(tmp, path)


def _list_coin_files(out_dir):
    """列出所有单币结果文件，排除还原产物与临时文件"""
    exclude = {'pairs_ALL.csv', 'pairs_CROSS_COIN_SUMMARY.csv'}
    fs = []
    for f in sorted(os.listdir(out_dir)):
        if not f.startswith('pairs_') or not f.endswith('.csv'):
            continue
        if f in exclude:
            continue
        fs.append(os.path.join(out_dir, f))
    return fs


# ======================================================================
# 内存受控的分组累加器
# ======================================================================
class GroupAccumulator:
    def __init__(self, keep_median=True):
        self.key2gid = {}
        self.keys = []
        self.n = 0
        self.keep_median = keep_median

        self.acc_names = (['rows', 'pos']
                          + [f'{c}__sum' for c in SUM_COLS]
                          + [f'{c}__msum' for c in MEAN_COLS]
                          + [f'{c}__mcnt' for c in MEAN_COLS])
        self.col_of = {nm: i for i, nm in enumerate(self.acc_names)}

        self.cap = 1 << 16
        self.data = np.zeros((self.cap, len(self.acc_names)), dtype=np.float64)

        self.med_gid = []
        self.med_val = []

    # ---- 容量管理 ----
    def _ensure(self, need):
        if need <= self.data.shape[0]:
            return
        cap = self.data.shape[0]
        while cap < need:
            cap *= 2
        new = np.zeros((cap, self.data.shape[1]), dtype=np.float64)
        new[:self.n] = self.data[:self.n]
        self.data = new

    # ---- key -> gid ----
    def _gids(self, keys: pd.Series) -> np.ndarray:
        mapped = keys.map(self.key2gid)
        miss = mapped.isna().to_numpy()
        if miss.any():
            new_keys = pd.unique(keys.to_numpy(dtype=object)[miss])
            start = self.n
            for i, k in enumerate(new_keys):
                self.key2gid[k] = start + i
            self.keys.extend(list(new_keys))
            self.n = start + len(new_keys)
            self._ensure(self.n)
            mapped = keys.map(self.key2gid)          # 补齐后重映射，保证正确
        return mapped.to_numpy(dtype=np.int64)

    # ---- 累加一个 chunk ----
    def update(self, df: pd.DataFrame):
        n = len(df)
        if n == 0:
            return
        for c in KEY_COLS:
            if c not in df.columns:
                raise KeyError(f"文件缺少关键列 {c}")

        key = (df['entry_factor'].astype(str) + '\x01'
               + df['exit_factor'].astype(str) + '\x01'
               + df['filter_mode'].astype(str))
        gid = self._gids(key)
        D = self.data

        # 正常情况下单文件内组合键唯一 -> gid 唯一，可直接向量化 +=
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

        if self.keep_median:
            self.med_gid.append(gid.astype(np.int32))
            self.med_val.append(sr.astype(np.float32))

    # ---- 输出 ----
    def finalize(self) -> pd.DataFrame:
        D = self.data[:self.n]
        col = self.col_of

        split = [k.split('\x01') for k in self.keys[:self.n]]
        out = pd.DataFrame(split, columns=KEY_COLS)

        def _sum(c):
            return D[:, col[f'{c}__sum']]

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

        # median
        if self.keep_median and self.med_gid:
            g_all = np.concatenate(self.med_gid)
            v_all = np.concatenate(self.med_val).astype(np.float64)
            med = pd.Series(v_all).groupby(g_all).median()   # 与 pandas median 一致(跳过 NaN)
            arr = np.full(self.n, np.nan)
            arr[med.index.to_numpy()] = med.to_numpy()
            out['median_sum_ret'] = arr
            del g_all, v_all, med
            self.med_gid, self.med_val = [], []
            gc.collect()
        else:
            out['median_sum_ret'] = np.nan

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


# ======================================================================
# DSR（与挖掘脚本 calc_dsr_approx 数学完全一致，向量化实现）
# ======================================================================
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


# ======================================================================
# 单个目录的还原
# ======================================================================
def rebuild_dir(out_dir, rcfg=RCFG):
    if not os.path.isdir(out_dir):
        print(f"⚠️ 目录不存在，跳过: {out_dir}")
        return

    files = _list_coin_files(out_dir)
    if not files:
        print(f"⚠️ {out_dir} 下未发现 pairs_<coin>.csv，跳过")
        return

    total_bytes = sum(os.path.getsize(f) for f in files)
    print("=" * 78)
    print(f"📂 {os.path.abspath(out_dir)}")
    print(f"   单币文件: {len(files)} 个 | 合计体积: {total_bytes / 1024 ** 3:.3f} GB")
    print(f"   重建 pairs_ALL={rcfg['REBUILD_PAIRS_ALL']} | 重建 SUMMARY={rcfg['REBUILD_SUMMARY']} "
          f"| 精确中位数={rcfg['EXACT_MEDIAN']}")
    print("=" * 78)

    if not (rcfg['REBUILD_PAIRS_ALL'] or rcfg['REBUILD_SUMMARY']):
        return

    # ---- pairs_ALL 输出流（列结构以第一个文件为准）----
    all_path = os.path.join(out_dir, 'pairs_ALL.csv')
    all_tmp = all_path + '.tmp'
    fo = None
    ref_cols = None
    header_pending = True
    if rcfg['REBUILD_PAIRS_ALL']:
        ref_cols = pd.read_csv(files[0], nrows=0).columns.tolist()
        fo = open(all_tmp, 'w', newline='', encoding='utf-8-sig')

    acc = GroupAccumulator(keep_median=rcfg['EXACT_MEDIAN']) if rcfg['REBUILD_SUMMARY'] else None

    # 若不需要 pairs_ALL，则只读汇总所需列（大幅提速）
    usecols = None
    if not rcfg['REBUILD_PAIRS_ALL']:
        head = pd.read_csv(files[0], nrows=0)
        usecols = [c for c in NEED_COLS if c in set(head.columns)]

    trials = {c: 0 for c in TRIAL_COLS}
    n_rows_total = 0

    try:
        for i, f in enumerate(files, 1):
            n_rows_file = 0
            for chunk in pd.read_csv(f, usecols=usecols, chunksize=rcfg['CHUNKSIZE'], low_memory=False):
                if fo is not None:
                    if list(chunk.columns) != ref_cols:
                        chunk_w = chunk.reindex(columns=ref_cols)
                    else:
                        chunk_w = chunk
                    chunk_w.to_csv(fo, index=False, header=header_pending)
                    header_pending = False

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
            del chunk
            gc.collect()
    finally:
        if fo is not None:
            fo.close()

    if rcfg['REBUILD_PAIRS_ALL']:
        _atomic_replace(all_tmp, all_path)
        print(f"✅ pairs_ALL.csv 已重建: {n_rows_total:,} 行 -> {all_path}")

    if acc is None:
        return

    # ---- 组装汇总 ----
    print("\n[还原] 正在生成跨币种宏观评估指标 ...")
    summ = acc.finalize()
    del acc
    gc.collect()

    # ---- 测试基数（DSR 去膨胀的核心）----
    legacy_trials = trials.get('n_trials_combos', 0)
    total_trials = trials.get('n_trials_total', 0)
    alive_trials = trials.get('n_trials_alive', 0)

    if total_trials <= 1:
        n_e = summ['entry_factor'].nunique()
        n_x = summ['exit_factor'].nunique()
        n_m = summ['filter_mode'].nunique()
        total_trials = int(n_e * n_x * n_m)
        legacy_trials = int(n_e * n_x) if legacy_trials <= 1 else legacy_trials
        print("⚠️ 单币文件中未发现注入的测试基数列(旧版结果)，"
              f"退化为按存留组合估算: {n_e}×{n_x}×{n_m}={total_trials}（会低估 -> DSR 偏乐观）")

    mode = rcfg['TRIALS_MODE']
    used_trials = {'total': total_trials, 'combos': legacy_trials, 'alive': alive_trials}.get(mode, total_trials)
    if used_trials <= 1:
        used_trials = total_trials

    print(f"   测试基数: n_trials_total={total_trials:,} | n_trials_combos={legacy_trials:,} "
          f"| n_trials_alive={alive_trials:,} | 本次 DSR 采用 '{mode}' = {used_trials:,}")
    print(f"   E[max SR] 门槛: 真实口径 {math.sqrt(2 * math.log(max(used_trials, 2))):.3f} "
          f"vs 旧口径 {math.sqrt(2 * math.log(max(legacy_trials, 2))):.3f}")

    summ['total_trials'] = int(used_trials)
    summ['deflated_sharpe'] = calc_dsr_vec(summ, used_trials)
    summ['total_trials_legacy'] = int(max(legacy_trials, 0))
    summ['deflated_sharpe_legacy'] = calc_dsr_vec(summ, legacy_trials)
    summ['trials_mode'] = mode

    summ = summ.reindex(columns=[c for c in FINAL_ORDER if c in summ.columns])
    summ.sort_values('score', ascending=False, inplace=True)

    sum_path = os.path.join(out_dir, 'pairs_CROSS_COIN_SUMMARY.csv')
    _atomic_to_csv(summ, sum_path)
    print(f"✅ pairs_CROSS_COIN_SUMMARY.csv 已重建: {len(summ):,} 组 -> {sum_path}")

    # ---- TOP N 展示（与原脚本一致的口径）----
    topn = rcfg['TOPN_PRINT']
    print("\n" + "=" * 78)
    print(f"🏆 跨币种稳健 TOP{topn} (score = 均笔收益 × √笔数 × 盈利币种占比)")
    print("=" * 78)
    show_cols = ['entry_factor', 'exit_factor', 'filter_mode', 'n_coins', 'total_trades',
                 'mean_avg_ret', 'mean_win_rate', 'coin_positive_rate',
                 'sum_ret_all', 'oos_sum_all', 'score', 'deflated_sharpe', 'deflated_sharpe_legacy']
    pd.set_option('display.width', 260)
    pd.set_option('display.max_colwidth', 40)
    print(summ.head(topn)[show_cols].to_string(index=False, float_format=lambda x: f'{x:.3f}'))
    print()

    del summ
    gc.collect()


def main(rcfg=RCFG):
    for d in rcfg['OUT_DIRS']:
        print("\n\n" + "★" * 78)
        print(f"★ 还原任务: {d}")
        print("★" * 78)
        rebuild_dir(d, rcfg)


if __name__ == '__main__':
    main()