# -*- coding: utf-8 -*-
import concurrent.futures
import glob
import multiprocessing
import os
import sys
import gc  # 引入垃圾回收模块
import warnings
from collections import deque

import numpy as np
import pandas as pd

# ==========================================
# 1. 配置路径 (已改为由主函数内的列表循环控制)
# ==========================================
TIMEFRAMES = ["5m", "15m", "30m", "60m"]
TIMEFRAMES.reverse()
# ==========================================
# 2. 字段映射与保留列 (极限控制内存)
# ==========================================
USECOLS = [
    'coin',
    'entry_factor',
    'exit_factor',
    'filter_mode',
    'direction',
    'trades',
    'sum_ret',
    'fr_sum',
    'max_dd',
    'win_rate',
    'win_hold_bars',
    'loss_hold_bars',
    'is_trades',
    'is_sum_ret',
    'is_fr_sum',
    'is_win_rate',
    'oos_trades',
    'oos_sum_ret',
    'oos_fr_sum',
    'oos_win_rate',
    'trades_q1',
    'ret_q1',
    'trades_q2',
    'ret_q2',
    'trades_q3',
    'ret_q3',
    'trades_q4',
    'ret_q4',
]

# 新增：定义极致压缩的内存数据类型映射
DTYPE_MAP = {
    'coin': 'category',
    'entry_factor': 'category',
    'exit_factor': 'category',
    'filter_mode': 'category',
    'direction': 'category',
    'trades': 'float32',
    'sum_ret': 'float32',
    'fr_sum': 'float32',
    'max_dd': 'float32',
    'win_rate': 'float32',
    'win_hold_bars': 'float32',
    'loss_hold_bars': 'float32',
    'is_trades': 'float32',
    'is_sum_ret': 'float32',
    'is_fr_sum': 'float32',
    'is_win_rate': 'float32',
    'oos_trades': 'float32',
    'oos_sum_ret': 'float32',
    'oos_fr_sum': 'float32',
    'oos_win_rate': 'float32',
    'trades_q1': 'float32',
    'ret_q1': 'float32',
    'trades_q2': 'float32',
    'ret_q2': 'float32',
    'trades_q3': 'float32',
    'ret_q3': 'float32',
    'trades_q4': 'float32',
    'ret_q4': 'float32',
}

# ==========================================
# 3. 流式归约相关常量
# ==========================================
GROUP_KEYS = ['entry_factor', 'exit_factor', 'filter_mode']

# 需要跨币种“求和”的列（子进程算好后直接进累加器，明细即刻丢弃）
SUM_COLS = [
    'trades', 'sum_ret', 'fr_sum',
    'win_trades', 'loss_trades', 'win_hold_sum', 'loss_hold_sum',
    'is_trades', 'is_sum_ret', 'is_fr_sum', 'is_win_trades',
    'oos_trades', 'oos_sum_ret', 'oos_fr_sum', 'oos_win_trades',
    'trades_q1', 'ret_q1', 'q1_fr', 'q1_win_trades',
    'trades_q2', 'ret_q2', 'q2_fr', 'q2_win_trades',
    'trades_q3', 'ret_q3', 'q3_fr', 'q3_win_trades',
    'trades_q4', 'ret_q4', 'q4_fr', 'q4_win_trades',
    'is_profitable',
]
SUM_RET_POS = SUM_COLS.index('sum_ret')
FR_SUM_POS = SUM_COLS.index('fr_sum')  # <--- 新增：定义资金费率的位置索引

# 读文件时期望存在的数值列（缺列自动补 0，避免聚合阶段 KeyError）
BASE_NUM_COLS = [
    c for c in USECOLS
    if c not in ('coin', 'entry_factor', 'exit_factor', 'filter_mode', 'direction')
]

# 唯一需要保留“逐币明细”的指标是 max_dd 的中位数（只 1 列 float32）。
# 若你不需要该列，把它设为 False，主进程内存可再降一个量级。
COMPUTE_DD_MEDIAN = True

# 中位数分块计算的行数（控制临时矩阵峰值）
MEDIAN_CHUNK = 65536


# ==========================================
# 4. 子进程：读取 + 派生 + 打包成紧凑 payload
# ==========================================
def _build_payload(sub, coin):
    """把单币单方向的明细压成 (coin, 类别表, 键codes, 求和值, max_dd) 的紧凑结构。"""
    # 防御：同一 (entry, exit, filter) 若出现重复行，先按原语义归并，保证键唯一
    if sub.duplicated(subset=GROUP_KEYS).any():
        agg_map = {c: 'sum' for c in SUM_COLS}
        agg_map['max_dd'] = 'max'
        sub = sub.groupby(
            GROUP_KEYS, observed=True, sort=False, as_index=False
        ).agg(agg_map)

    n = len(sub)
    cats = []
    codes = np.empty((n, len(GROUP_KEYS)), dtype=np.int32)
    for i, k in enumerate(GROUP_KEYS):
        col = sub[k]
        if not isinstance(col.dtype, pd.CategoricalDtype):
            col = col.astype('category')
        cats.append(np.asarray(col.cat.categories, dtype=object))
        codes[:, i] = col.cat.codes.to_numpy()

    # 键为空的行直接剔除（与原 groupby 默认丢弃 NaN 键的行为一致）
    valid = (codes >= 0).all(axis=1)
    if not valid.all():
        sub = sub.loc[valid]
        codes = codes[valid]
        if len(sub) == 0:
            return None

    vals = sub[SUM_COLS].to_numpy(dtype=np.float32, copy=True)
    dd = sub['max_dd'].to_numpy(dtype=np.float32, copy=True)
    return (str(coin), cats, codes, vals, dd)


def process_single_file(filepath):
    """子进程执行函数：读取单一文件，预计算中间变量，填充缺失值，并压成紧凑 payload。"""
    try:
        # 修改：引入 dtype 限制，读取时立刻降维内存占用
        df = pd.read_csv(
            filepath,
            usecols=lambda c: c in USECOLS,
            dtype={k: v for k, v in DTYPE_MAP.items() if k in USECOLS}
        )
        if df.empty or 'trades' not in df.columns:
            return [], []

        # 过滤未触发交易的无效行
        df = df.loc[df['trades'] > 0].copy()
        if df.empty:
            return [], []

        # 缺失列补 0，保证后续列选择不报错
        for c in BASE_NUM_COLS:
            if c not in df.columns:
                df[c] = np.float32(0.0)

        # 分组键为空的行无意义，提前剔除
        key_cols = [k for k in GROUP_KEYS if k in df.columns]
        if len(key_cols) < len(GROUP_KEYS):
            return [], []
        df = df.dropna(subset=key_cols)
        if df.empty:
            return [], []

        # 填充数值列的 NaN，防止后续计算与提取最值报错
        num_cols = df.select_dtypes(include=[np.number]).columns
        df[num_cols] = df[num_cols].fillna(0.0)

        # ---- 派生列全部用 numpy 一次性算好，最后一次性拼接，避免 DataFrame 碎片化 ----
        trades = df['trades'].to_numpy(dtype=np.float32)
        win_rate = df['win_rate'].to_numpy(dtype=np.float32)
        fr_sum = df['fr_sum'].to_numpy(dtype=np.float32)

        new_cols = {}

        # 1. 计算盈利和亏损交易次数
        win_trades = np.round(trades * win_rate / 100.0).astype('float32')
        loss_trades = (trades - win_trades).astype('float32')
        new_cols['win_trades'] = win_trades
        new_cols['loss_trades'] = loss_trades

        # 2. 计算 K线持仓总根数 (便于加权平均)
        new_cols['win_hold_sum'] = (
                df['win_hold_bars'].to_numpy(dtype=np.float32) * win_trades
        ).astype('float32')
        new_cols['loss_hold_sum'] = (
                df['loss_hold_bars'].to_numpy(dtype=np.float32) * loss_trades
        ).astype('float32')

        # 3. 计算 IS/OOS 的盈利交易次数
        new_cols['is_win_trades'] = np.round(
            df['is_trades'].to_numpy(dtype=np.float32)
            * df['is_win_rate'].to_numpy(dtype=np.float32) / 100.0
        ).astype('float32')
        new_cols['oos_win_trades'] = np.round(
            df['oos_trades'].to_numpy(dtype=np.float32)
            * df['oos_win_rate'].to_numpy(dtype=np.float32) / 100.0
        ).astype('float32')

        # 4. 【修复缺陷1】真实核算季度的盈利笔数与资金费率（非写0或NaN）
        safe_trades = np.where(trades > 0, trades, 1.0).astype('float32')
        for q in ['q1', 'q2', 'q3', 'q4']:
            tq = df[f'trades_{q}'].to_numpy(dtype=np.float32)
            # 季度盈利笔数核算：基于胜率估算
            new_cols[f'{q}_win_trades'] = np.round(
                tq * (win_rate / 100.0)
            ).astype('float32')
            # 季度资金费率核算：基于该季度交易笔数占总笔数的比例拆分资金费率
            trades_ratio = np.where(trades > 0, tq / safe_trades, 0.0)
            new_cols[f'{q}_fr'] = (fr_sum * trades_ratio).astype('float32')

        # 5. 辅助列：标记正收益币种
        new_cols['is_profitable'] = (
                df['sum_ret'].to_numpy(dtype=np.float32) > 0
        ).astype('float32')

        df = pd.concat([df, pd.DataFrame(new_cols, index=df.index)], axis=1)
        del new_cols

        # 拆分为做多和做空（只回传聚合必需的紧凑数组，不回传整张明细表）
        out_long, out_short = [], []
        for direction_name, bucket in (('Long', out_long), ('Short', out_short)):
            mask = (df['direction'] == direction_name).to_numpy()
            if not mask.any():
                continue
            sub_all = df.loc[mask]
            coin_vals = pd.unique(sub_all['coin'].astype(object))
            if len(coin_vals) <= 1:
                coin = coin_vals[0] if len(coin_vals) else os.path.basename(filepath)
                p = _build_payload(sub_all, coin)
                if p is not None:
                    bucket.append(p)
            else:
                for coin, sub in sub_all.groupby('coin', observed=True, sort=False):
                    p = _build_payload(sub, coin)
                    if p is not None:
                        bucket.append(p)
            del sub_all

        del df
        return out_long, out_short
    except Exception as e:
        print(f"读取文件失败 {filepath}: {e}")
        return [], []


# ==========================================
# 5. 主进程：定长累加器（内存只与参数组合数有关，与文件数无关）
# ==========================================
class DirectionAccumulator:
    """流式归约容器：把每个币的 payload 直接累加进定长数组，明细即刻释放。"""

    LEVEL_BITS = 21  # 每层分组键最多 2,097,152 个取值
    LEVEL_MASK = (1 << LEVEL_BITS) - 1
    SHIFTS = (42, 21, 0)

    def __init__(self, name):
        self.name = name
        self.fr_mult = -1 if name == 'Long' else 1  # <--- 新增：多空方向资金费率乘数
        self.level_vocab = [[] for _ in GROUP_KEYS]  # code -> 原始名称
        self.level_lookup = [{} for _ in GROUP_KEYS]  # 原始名称 -> code
        self.keys = None  # int64 组合键（唯一）
        self.key_index = None  # pd.Index，用于向量化定位
        self.sums = None  # (n, len(SUM_COLS)) float64，保证累加精度
        self.coin_count = None
        self.max_dd_max = None
        # <--- 修改：为支持 Top 3 统计，升级为 (n, 3) 形状的矩阵
        self.best_ret = None
        self.best_coin = None
        self.worst_ret = None
        self.worst_coin = None
        self.dd_cols = []  # 仅为中位数保留：每币一列 float32

    # ---------- 内部：容量管理 ----------
    def _alloc(self, n):
        self.sums = np.zeros((n, len(SUM_COLS)), dtype=np.float64)
        self.coin_count = np.zeros(n, dtype=np.int32)
        self.max_dd_max = np.full(n, -np.inf, dtype=np.float64)
        # <--- 修改：初始化 (n, 3) 大小的矩阵
        self.best_ret = np.full((n, 3), -np.inf, dtype=np.float64)
        self.worst_ret = np.full((n, 3), np.inf, dtype=np.float64)
        self.best_coin = np.empty((n, 3), dtype=object)
        self.worst_coin = np.empty((n, 3), dtype=object)

    def _grow(self, n):
        old = self.sums.shape[0]
        if n <= old:
            return
        add = n - old
        self.sums = np.concatenate(
            [self.sums, np.zeros((add, len(SUM_COLS)), dtype=np.float64)], axis=0
        )
        self.coin_count = np.concatenate(
            [self.coin_count, np.zeros(add, dtype=np.int32)]
        )
        self.max_dd_max = np.concatenate(
            [self.max_dd_max, np.full(add, -np.inf, dtype=np.float64)]
        )
        # <--- 修改：扩容时同样拼接 (add, 3) 形状的块
        self.best_ret = np.concatenate(
            [self.best_ret, np.full((add, 3), -np.inf, dtype=np.float64)], axis=0
        )
        self.worst_ret = np.concatenate(
            [self.worst_ret, np.full((add, 3), np.inf, dtype=np.float64)], axis=0
        )
        self.best_coin = np.concatenate(
            [self.best_coin, np.empty((add, 3), dtype=object)], axis=0
        )
        self.worst_coin = np.concatenate(
            [self.worst_coin, np.empty((add, 3), dtype=object)], axis=0
        )

    def _map_level(self, li, cats):
        """把单文件的类别表映射到全局码表（类别数很少，开销可忽略）。"""
        lookup = self.level_lookup[li]
        vocab = self.level_vocab[li]
        out = np.empty(len(cats), dtype=np.int64)
        for i, c in enumerate(cats):
            code = lookup.get(c, -1)
            if code < 0:
                code = len(vocab)
                vocab.append(c)
                lookup[c] = code
            out[i] = code
        if len(vocab) > self.LEVEL_MASK:
            raise ValueError(
                f"{GROUP_KEYS[li]} 取值数超过 {self.LEVEL_MASK}，请调大 LEVEL_BITS"
            )
        return out

    # ---------- 核心：单币归约 ----------
    def update(self, payload):
        coin, cats, codes, vals, dd = payload
        n_rows = codes.shape[0]
        if n_rows == 0:
            return

        combined = np.zeros(n_rows, dtype=np.int64)
        for li in range(len(GROUP_KEYS)):
            mapping = self._map_level(li, cats[li])
            combined |= (mapping[codes[:, li]] << self.SHIFTS[li])

        if self.key_index is None:
            self.keys = combined.copy()
            self.key_index = pd.Index(self.keys)
            self._alloc(n_rows)
            pos = np.arange(n_rows, dtype=np.int64)
        else:
            pos = np.asarray(self.key_index.get_indexer(combined), dtype=np.int64)
            miss = pos < 0
            if miss.any():
                new_keys = combined[miss]  # 单币内键唯一，故无重复
                old_n = self.keys.shape[0]
                self.keys = np.concatenate([self.keys, new_keys])
                self.key_index = pd.Index(self.keys)
                self._grow(self.keys.shape[0])
                pos[miss] = np.arange(
                    old_n, old_n + new_keys.shape[0], dtype=np.int64
                )

        # 求和类指标（float64 累加，避免 float32 精度塌陷）
        self.sums[pos] += vals
        self.coin_count[pos] += 1

        # 全局最大回撤（跨币取 max）
        cur = self.max_dd_max[pos]
        np.maximum(cur, dd, out=cur)
        self.max_dd_max[pos] = cur

        # =======================================================
        # 【修复 & 新增】严格计算净收益，并维护 Top 3 极值矩阵
        # =======================================================
        gross_ret = vals[:, SUM_RET_POS].astype(np.float64)
        fr_val = vals[:, FR_SUM_POS].astype(np.float64)
        coin_net_ret = gross_ret + self.fr_mult * fr_val  # 计算出扣费后的真实净收益

        # 利用 hstack 和 argsort 进行基于向量化的 Top3 快速筛选插入
        row_indices = np.arange(n_rows)[:, None]
        new_coin_col = np.full((n_rows, 1), coin, dtype=object)
        new_ret_col = coin_net_ret[:, None]

        # 1. 维护前 3 名最大收益 (降序)
        concat_best_ret = np.hstack([self.best_ret[pos], new_ret_col])
        concat_best_coin = np.hstack([self.best_coin[pos], new_coin_col])
        idx_best = np.argsort(-concat_best_ret, axis=1)[:, :3]  # 负号实现降序，排除 -inf
        self.best_ret[pos] = concat_best_ret[row_indices, idx_best]
        self.best_coin[pos] = concat_best_coin[row_indices, idx_best]

        # 2. 维护前 3 名最小收益/最大亏损 (升序)
        concat_worst_ret = np.hstack([self.worst_ret[pos], new_ret_col])
        concat_worst_coin = np.hstack([self.worst_coin[pos], new_coin_col])
        idx_worst = np.argsort(concat_worst_ret, axis=1)[:, :3]  # 正常升序，排除 inf
        self.worst_ret[pos] = concat_worst_ret[row_indices, idx_worst]
        self.worst_coin[pos] = concat_worst_coin[row_indices, idx_worst]
        # =======================================================

        # 中位数需要逐币样本，只保留 max_dd 这一列
        if COMPUTE_DD_MEDIAN:
            col = np.full(self.keys.shape[0], np.nan, dtype=np.float32)
            col[pos] = dd
            self.dd_cols.append(col)

    # ---------- 收尾 ----------
    def _median(self):
        n = self.keys.shape[0]
        if (not COMPUTE_DD_MEDIAN) or (not self.dd_cols):
            self.dd_cols = []
            return np.full(n, np.nan, dtype=np.float64)

        m = len(self.dd_cols)
        out = np.full(n, np.nan, dtype=np.float64)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=RuntimeWarning)
            for s in range(0, n, MEDIAN_CHUNK):
                e = min(s + MEDIAN_CHUNK, n)
                buf = np.full((e - s, m), np.nan, dtype=np.float32)
                for j, col in enumerate(self.dd_cols):
                    L = col.shape[0]
                    if L <= s:
                        continue
                    ee = min(e, L)
                    buf[:ee - s, j] = col[s:ee]
                out[s:e] = np.nanmedian(buf, axis=1)
                del buf
        self.dd_cols = []
        gc.collect()
        return out

    def to_frame(self):
        """输出与原 groupby().agg() 完全一致的列名结构，供下游组装。"""
        if self.key_index is None or self.keys.shape[0] == 0:
            return None

        med = self._median()
        data = {}
        for i, shift in enumerate(self.SHIFTS):
            codes = ((self.keys >> shift) & self.LEVEL_MASK).astype(np.int64)
            vocab_arr = np.asarray(self.level_vocab[i], dtype=object)
            data[GROUP_KEYS[i]] = vocab_arr[codes]

        for j, c in enumerate(SUM_COLS):
            if c == 'is_profitable':
                data[f'{c}_sum'] = self.sums[:, j].astype('int64')
            else:
                data[f'{c}_sum'] = self.sums[:, j]

        data['coin_count'] = self.coin_count
        data['max_dd_max'] = self.max_dd_max
        data['max_dd_median'] = med

        # =======================================================
        # 输出最优最差币种的【第一名】以兼容现有代码，并输出 Top 3 的总和
        # =======================================================
        data['best_coin_name'] = self.best_coin[:, 0]
        data['best_coin_ret'] = np.where(self.best_ret[:, 0] == -np.inf, np.nan, self.best_ret[:, 0])
        data['worst_coin_name'] = self.worst_coin[:, 0]
        data['worst_coin_ret'] = np.where(self.worst_ret[:, 0] == np.inf, np.nan, self.worst_ret[:, 0])

        # 将无效的占位符(-inf/inf)替换为0后，分别对 Top 3 进行横向求和
        valid_best = np.where(self.best_ret == -np.inf, 0.0, self.best_ret)
        data['top3_ret_sum'] = valid_best.sum(axis=1)

        valid_worst = np.where(self.worst_ret == np.inf, 0.0, self.worst_ret)
        data['worst3_ret_sum'] = valid_worst.sum(axis=1)
        # =======================================================

        grouped = pd.DataFrame(data)
        del data
        # 与原先 groupby(sort=True) 保持一致的稳定顺序
        grouped.sort_values(GROUP_KEYS, kind='stable', inplace=True, ignore_index=True)
        return grouped


# 修改：增加 output_dir 参数，接收动态的输出路径
def aggregate_direction_data(grouped, direction_name, output_dir):
    """主进程聚合函数：基于流式归约结果，组装最终报表。"""
    if grouped is None or grouped.empty:
        print(f"没有 {direction_name} 的有效数据。")
        return

    print(f"正在组装 {direction_name} 的全市场聚合结果（组合数: {len(grouped)}）...")

    # 3. 计算加权平均持仓K线数
    win_trades_sum = grouped['win_trades_sum'].replace(0, np.nan)
    loss_trades_sum = grouped['loss_trades_sum'].replace(0, np.nan)

    avg_win_hold = (grouped['win_hold_sum_sum'] / win_trades_sum).fillna(0.0)
    avg_loss_hold = (grouped['loss_hold_sum_sum'] / loss_trades_sum).fillna(0.0)

    # =========================================================================
    # 派生指标逻辑新增区
    # 根据方向处理资金费率乘数：做多需减去资金费率(-1)，做空需加上资金费率(1)
    # =========================================================================
    fr_mult = -1 if direction_name == 'Long' else 1

    # 核心净收益计算
    net_ret = grouped['sum_ret_sum'] + fr_mult * grouped['fr_sum_sum']
    is_net_ret = grouped['is_sum_ret_sum'] + fr_mult * grouped['is_fr_sum_sum']
    oos_net_ret = grouped['oos_sum_ret_sum'] + fr_mult * grouped['oos_fr_sum_sum']

    # 1. 胜率 (%)
    safe_trades = grouped['trades_sum'].replace(0, np.nan)
    win_rate = (grouped['win_trades_sum'] / safe_trades * 100).fillna(0.0)

    # 2. 单笔平均净收益
    avg_net_ret = (net_ret / safe_trades).fillna(0.0)

    # 3. 样本内外平均单笔净收益
    safe_is_trades = grouped['is_trades_sum'].replace(0, np.nan)
    is_avg_net_ret = (is_net_ret / safe_is_trades).fillna(0.0)

    safe_oos_trades = grouped['oos_trades_sum'].replace(0, np.nan)
    oos_avg_net_ret = (oos_net_ret / safe_oos_trades).fillna(0.0)

    # 4. 盈亏持仓时间比
    safe_avg_loss_hold = avg_loss_hold.replace(0, np.nan)
    win_loss_hold_ratio = (avg_win_hold / safe_avg_loss_hold).fillna(0.0)

    # =========================================================================
    # 计算极值币种占比系列指标（分母皆使用扣费后的 safe_net_ret）
    # =========================================================================
    safe_net_ret = net_ret.replace(0, np.nan)
    best_coin_pct = (grouped['best_coin_ret'] / safe_net_ret * 100).fillna(0.0)
    top3_best_pct = (grouped['top3_ret_sum'] / safe_net_ret * 100).fillna(0.0)
    top3_worst_pct = (grouped['worst3_ret_sum'] / safe_net_ret * 100).fillna(0.0)

    # 6. 净盈利季度数量
    profitable_q_count = pd.Series(0, index=grouped.index)
    for q in ['q1', 'q2', 'q3', 'q4']:
        q_net_ret = grouped[f'ret_{q}_sum'] + fr_mult * grouped[f'{q}_fr_sum']
        profitable_q_count += (q_net_ret > 0).astype(int)
    # =========================================================================

    # 4. 组装最终结果表，严格遵循要求的列名
    final_df = pd.DataFrame()
    final_df['入场信号名称'] = grouped['entry_factor']
    final_df['出场信号名称'] = grouped['exit_factor']
    final_df['过滤模式'] = grouped['filter_mode']

    final_df['产生交易的币种总数'] = grouped['coin_count']
    final_df['盈利的币数'] = grouped['is_profitable_sum']

    final_df['总交易数'] = grouped['trades_sum']
    final_df['总收益'] = grouped['sum_ret_sum'].round(4)
    final_df['总资金费率'] = grouped['fr_sum_sum'].round(4)

    # 记录单币遭遇过的最惨回撤
    final_df['全局最大回撤'] = grouped['max_dd_max'].round(4)

    final_df['盈利交易次数'] = grouped['win_trades_sum'].astype(int)
    final_df['盈利单平均持仓 K 线根数'] = avg_win_hold.round(2)
    final_df['亏损单平均持仓 K 线根数'] = avg_loss_hold.round(2)

    # 季度切片 (已完成真实数据核算)
    for q in ['q1', 'q2', 'q3', 'q4']:
        final_df[f'{q.upper()}交易次数'] = grouped[f'trades_{q}_sum']
        final_df[f'{q.upper()}收益'] = grouped[f'ret_{q}_sum'].round(4)
        final_df[f'{q.upper()}资金费率'] = grouped[f'{q}_fr_sum'].round(4)
        final_df[f'{q.upper()}盈利交易次数'] = (
            grouped[f'{q}_win_trades_sum'].astype(int)
        )

    # 样本内外
    final_df['样本内交易次数'] = grouped['is_trades_sum']
    final_df['样本内收益'] = grouped['is_sum_ret_sum'].round(4)
    final_df['样本内资金费率'] = grouped['is_fr_sum_sum'].round(4)
    final_df['样本内盈利交易次数'] = grouped['is_win_trades_sum'].astype(int)

    final_df['样本外交易次数'] = grouped['oos_trades_sum']
    final_df['样本外收益'] = grouped['oos_sum_ret_sum'].round(4)
    final_df['样本外资金费率'] = grouped['oos_fr_sum_sum'].round(4)
    final_df['样本外盈利交易次数'] = grouped['oos_win_trades_sum'].astype(int)

    # 极值币种属性
    final_df['最大收益币名称'] = grouped['best_coin_name']
    final_df['最大收益币的收益'] = grouped['best_coin_ret'].round(4)
    final_df['最低收益币名称'] = grouped['worst_coin_name']
    final_df['最低收益币的收益'] = grouped['worst_coin_ret'].round(4)

    final_df['各币种全局最大回撤的中位数'] = grouped['max_dd_median'].round(4)

    # =========================================================================
    # 附加新增的派生指标到最终报表中
    # =========================================================================
    final_df['胜率'] = win_rate.round(2)
    final_df['单笔平均净收益'] = avg_net_ret.round(4)
    final_df['样本内平均单笔净收益'] = is_avg_net_ret.round(4)
    final_df['样本外平均单笔净收益'] = oos_avg_net_ret.round(4)
    final_df['盈亏持仓时间比'] = win_loss_hold_ratio.round(2)

    # 核心修复及新增的净利润占比
    final_df['最优币占总净收益百分比'] = best_coin_pct.round(2)
    final_df['Top3币种利润占总净收益百分比'] = top3_best_pct.round(2)
    final_df['Top3币种亏损占总净收益百分比'] = top3_worst_pct.round(2)

    final_df['净盈利季度数量'] = profitable_q_count
    # =========================================================================

    # 降序排序输出
    final_df.sort_values(
        by=['盈利的币数', '总收益'], ascending=[False, False], inplace=True
    )

    # 保存结果 (修改：使用透传进来的 output_dir)
    os.makedirs(output_dir, exist_ok=True)
    out_path = os.path.join(
        output_dir, f'aggregated_summary_{direction_name}.csv'
    )
    final_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    print(f"✅ {direction_name} 聚合完成，已保存至: {out_path}")


def main():
    # 外层套循环，实现多个维度的周期数据串行处理
    for tf in TIMEFRAMES:
        input_dir = f'./factor_out_{tf}'
        output_dir = f'./summary_results_{tf}'

        print(f"\n{'=' * 50}")
        print(f"🚀 开始聚合时间周期: {tf}")
        print(f"{'=' * 50}")

        if not os.path.exists(input_dir):
            print(f"❌ 找不到输入目录: {input_dir}，自动跳过此周期。")
            continue

        file_pattern = os.path.join(input_dir, 'pairs_*.csv.gz')
        files = glob.glob(file_pattern)

        if not files:
            file_pattern = os.path.join(input_dir, 'pairs_*.csv')
            files = glob.glob(file_pattern)

        if not files:
            print(f"❌ 在 {input_dir} 目录下未找到 pairs_ 文件，自动跳过此周期。")
            continue

        files.sort()
        total = len(files)
        print(f"🔍 找到 {total} 个币种的回测文件，开始多进程流式归约...")

        accumulators = {
            'Long': DirectionAccumulator('Long'),
            'Short': DirectionAccumulator('Short'),
        }

        max_workers = min(20, max(1, multiprocessing.cpu_count() - 2))
        # 有界窗口：完成但未消费的结果最多驻留 window 个，杜绝结果队列无限堆积
        window = max_workers + 2

        pool_kwargs = {'max_workers': max_workers}
        if sys.version_info >= (3, 11):
            # 定期重启 worker，归还 pandas 造成的堆碎片
            pool_kwargs['max_tasks_per_child'] = 16
        try:
            executor = concurrent.futures.ProcessPoolExecutor(**pool_kwargs)
        except (TypeError, ValueError):
            executor = concurrent.futures.ProcessPoolExecutor(max_workers=max_workers)

        done = 0
        with executor:
            file_iter = iter(files)
            pending = deque()

            def _submit_next():
                for fp in file_iter:
                    pending.append(executor.submit(process_single_file, fp))
                    return True
                return False

            for _ in range(window):
                if not _submit_next():
                    break

            while pending:
                fut = pending.popleft()
                try:
                    res = fut.result()
                except Exception as e:
                    print(f"子进程任务异常: {e}")
                    res = None
                del fut

                _submit_next()  # 消费一个立刻补一个，保持背压

                if res:
                    payloads_long, payloads_short = res
                    for p in payloads_long:
                        accumulators['Long'].update(p)
                    payloads_long.clear()
                    for p in payloads_short:
                        accumulators['Short'].update(p)
                    payloads_short.clear()
                    del payloads_long, payloads_short
                del res

                done += 1
                if done % 20 == 0 or done == total:
                    print(f"已归约 {done}/{total} 个文件...")
                    gc.collect()

        print(f"✅ {tf} 周期的全网文件解析完毕，开始进入归约(Reduce)收尾阶段...")

        for direction_name in ('Long', 'Short'):
            acc = accumulators.pop(direction_name)
            grouped = acc.to_frame()
            del acc
            gc.collect()
            # 透传当前的 output_dir 给聚合函数
            aggregate_direction_data(grouped, direction_name, output_dir)
            del grouped
            gc.collect()

        print(f"🎉 {tf} 周期的聚合任务结束！")

    print(f"\n🏆 所有设定的时间周期 ({', '.join(TIMEFRAMES)}) 全部串行聚合完毕！")


if __name__ == '__main__':
    main()