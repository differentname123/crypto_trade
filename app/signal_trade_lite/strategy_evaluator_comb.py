# -*- coding: utf-8 -*-
"""
======================================================================
马丁格尔策略 —— 组合(Portfolio)回测与搜索引擎
----------------------------------------------------------------------
Stage A : extract_target_trades_csv()
          从 stage1_*.pkl(.gz) 缓存中抽取目标参数的逐笔明细，
          并用与单策略榜单完全一致的口径把收益归一化成 "Margin 倍数(M倍)" 落盘。
Stage B : evaluate_multi_strategy_portfolios()
          读取多个单策略明细，穷举 K=min_k..max_k 的组合，
          在"共同重叠窗口"内做资金均分组合回测、风险聚合与打分排名。
======================================================================
"""

import os
import re
import gc
import glob
import gzip
import math
import pickle
import itertools

import numpy as np
import pandas as pd
import unicodedata

from app.signal_trade_lite.martin_strategy_backest import TimelineReplayer, evaluate_free_ride

# =====================================================================
# 路径与全局常量
# =====================================================================
CACHE_DIR = r"W:\backtest_data_1m_detail"  # 做多策略默认缓存目录
SHORT_CACHE_DIR = r"W:\backtest_data_1m_detail"  # 做空策略缓存目录

_TIME_LIKE_KEYS = ("time", "stamp", "epoch", "date", "millis", "nanos", "_ms", "_ns")

PNL_COL_CANDIDATES = ["net_pnl_in_margin", "pnl_in_margin", "net_pnl", "pnl", "profit", "net_profit"]
MDD_COL_CANDIDATES = ["max_drawdown", "max_drawdown_in_margin", "max_dd", "max_loss",
                      "max_loss_in_margin", "mdd", "max_floating_loss"]

BLOWUP_LOSS_THRESHOLD_M = 0.8  # 归一化后(M倍)单笔亏损超过该阈值视为爆仓(与原代码 -0.8*margin 等价)
DAYS_PER_YEAR = 365.0
INDEX_FILE = "_single_strategy_index.csv"  # Stage A 产出的元数据索引(Stage B 会读取, 且不会当成交易明细)

# =====================================================================
# 目标参数清单 (可加 "multiplier" 字段来精确锁定加仓倍数, 强烈建议加)
# =====================================================================
TARGET_CONFIGS = [
    # 高净收益
    {
        "symbol": "UNIUSDT",
        "strategy": "factor_044_6",
        "direction": "Long",
        "add_step": 0.010,
        "tp_step": 0.010,
        "margin": 6,
        "备注": "推荐次数为 6；最高共识。适合第一阶段核心运行，重点验证稳定性、连续盈利能力和实盘分润表现"
    },
    {
        "symbol": "SOLUSDT",
        "strategy": "factor_024_1",
        "direction": "Long",
        "add_step": 0.020,
        "tp_step": 0.012,
        "margin": 6,
        "备注": "推荐次数为 5；核心稳健型。适合第一阶段稳定做数据，跨多个 Margin 连续成立"
    },
    {
        "symbol": "UNIUSDT",
        "strategy": "factor_044_4",
        "direction": "Long",
        "add_step": 0.010,
        "tp_step": 0.007,
        "margin": 10,
        "备注": "推荐次数为 5；高频分润型。适合第一阶段或第一阶段后半段测试，高开仓频率、短持仓"
    },
    {
        "symbol": "LINKUSDT",
        "strategy": "factor_044_3",
        "direction": "Long",
        "add_step": 0.015,
        "tp_step": 0.012,
        "margin": 6,
        "备注": "推荐次数为 3；第三币种核心候选。适合第二阶段加入，用于验证非 UNI/SOL 币种上的持续有效性"
    },
    {
        "symbol": "UNIUSDT",
        "strategy": "factor_044_5",
        "direction": "Long",
        "add_step": 0.010,
        "tp_step": 0.009,
        "margin": 9,
        "备注": "推荐次数为 3；高收益型 UNI 备选。适合第二阶段，与 factor_044_6 二选一或做对照，不建议简单当作独立分散"
    },
    {
        "symbol": "SOLUSDT",
        "strategy": "factor_044_3",
        "direction": "Long",
        "add_step": 0.015,
        "tp_step": 0.012,
        "margin": 5,
        "备注": "推荐次数为 2；SOL 扩展候选。适合第二阶段观察，与 factor_024_1 SOL 做不同开仓逻辑对照"
    },
    {
        "symbol": "UNIUSDT",
        "strategy": "factor_044_3",
        "direction": "Long",
        "add_step": 0.010,
        "tp_step": 0.005,
        "margin": 5,
        "备注": "推荐次数为 2；极高频 UNI 候选。适合第二或第三阶段小规模观察，不应因为高频直接替代有效性更强的 UNI 主策略"
    },
    {
        "symbol": "SOLUSDT",
        "strategy": "factor_044_1",
        "direction": "Long",
        "add_step": 0.025,
        "tp_step": 0.012,
        "margin": 7,
        "备注": "推荐次数为 1；高平原 SOL 候选。适合第三阶段验证，目前共识度低于 factor_024_1 SOL"
    },

    # # 10000筛选
    #
    # # 第一阶段：做数据养号期（核心目标：绝对存活、极高安全垫、曲线平滑）
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 10, "备注": "推荐次数：7；第一阶段做数据（绝对防御底座首选，存活与安全垫全场最强）"},
    # {"symbol": "SOLUSDT", "strategy": "factor_023_2", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 10, "备注": "推荐次数：3；第一阶段做数据（SOL生态备选长跑王，超200天存活无回撤）"},
    #
    # # 第二阶段：带单收割期（核心目标：高频平仓、流水最大化、平原均值高防滑点）
    # {"symbol": "SOLUSDT", "strategy": "factor_023_3", "direction": "Long", "add_step": 0.030, "tp_step": 0.007,
    #  "margin": 6, "备注": "推荐次数：6；第二阶段赚分润（极限高频收割机，总收益流水霸榜，适合冲刺表现费）"},
    # {"symbol": "SOLUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.007,
    #  "margin": 7, "备注": "推荐次数：4；第二阶段赚分润（同族因子无缝平滑切换，平原均值全场顶格，容错率最高）"},
    # {"symbol": "SOLUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.025, "tp_step": 0.006,
    #  "margin": 8, "备注": "推荐次数：3；第二阶段赚分润（收紧加仓间距高频吃单，Margin8提供更高的极端安全容错）"},
    #
    # # 第二阶段：备选方案（不换币平替与高净利增强）
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 6, "备注": "推荐次数：2；第二阶段赚分润（AAVE不换币平替，保持极高安全垫与净利润，适合大资金稳健收租）"},
    # {"symbol": "SOLUSDT", "strategy": "factor_023_2", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 6, "备注": "推荐次数：2；第二阶段赚分润（极高净利润增强备选，适合小比例资金搭配对冲）"},
    #
    # # 5000筛选
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 9, "备注": "推荐的次数为 7；第一阶段做数据（绝对防御底座首选）"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.007,
    #  "margin": 7, "备注": "推荐的次数为 6；第二阶段赚分润（流水与容错综合收益首选）"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_023_3", "direction": "Long", "add_step": 0.030, "tp_step": 0.007,
    #  "margin": 6, "备注": "推荐的次数为 5；第二阶段赚分润（高爆发收割进攻备选）"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_024_2", "direction": "Long", "add_step": 0.040, "tp_step": 0.007,
    #  "margin": 6, "备注": "推荐的次数为 2；第二阶段赚分润（0.040宽距异源因子对冲）"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_023_7", "direction": "Long", "add_step": 0.030, "tp_step": 0.005,
    #  "margin": 9, "备注": "推荐的次数为 2；第一阶段做数据（近8个月长寿视觉备选）"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_023_2", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 10, "备注": "推荐的次数为 2；第一阶段做数据（SOL本币极限防守备选）"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 8, "备注": "推荐的次数为 1；第一阶段做数据（SOL同因子防守备选）"},

    # # 全局筛选
    # {"symbol": "SOLUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.012,
    #  "margin": 6, "备注": "推荐的次数为 5；第二阶段赚分润"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.009,
    #  "margin": 6, "备注": "推荐的次数为 3；第二阶段赚分润"},
    # {"symbol": "AAVEUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.006,
    #  "margin": 8, "备注": "推荐的次数为 3；第一阶段稳定做数据"},
    #
    # {"symbol": "SOLUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.011,
    #  "margin": 7, "备注": "推荐的次数为 2；第一阶段稳定做数据 1 次，第二阶段赚分润 1 次"},
    # {"symbol": "SOLUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.009,
    #  "margin": 7, "备注": "推荐的次数为 2；第二阶段赚分润"},
    # {"symbol": "SOLUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.012,
    #  "margin": 7, "备注": "推荐的次数为 2；第二阶段赚分润"},
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_9", "direction": "Long", "add_step": 0.020, "tp_step": 0.007,
    #  "margin": 9, "备注": "推荐的次数为 2；第一阶段稳定做数据"},
    #
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_9", "direction": "Long", "add_step": 0.020, "tp_step": 0.006,
    #  "margin": 8, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "LINKUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.011,
    #  "margin": 7, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 10, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "BNBUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.010,
    #  "margin": 8, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "SOLUSDT", "strategy": "factor_023_6", "direction": "Long", "add_step": 0.030, "tp_step": 0.008,
    #  "margin": 7, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "LINKUSDT", "strategy": "factor_024_6", "direction": "Long", "add_step": 0.030, "tp_step": 0.008,
    #  "margin": 9, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
    #  "margin": 6, "备注": "推荐的次数为 1；第二阶段赚分润"},
    # {"symbol": "SOLUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.009,
    #  "margin": 8, "备注": "推荐的次数为 1；第二阶段赚分润"},
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.008,
    #  "margin": 7, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "AAVEUSDT", "strategy": "factor_044_9", "direction": "Long", "add_step": 0.020, "tp_step": 0.007,
    #  "margin": 10, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    # {"symbol": "AAVEUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.008,
    #  "margin": 8, "备注": "推荐的次数为 1；第一阶段稳定做数据"},

    # === 做空 Short ===
    # {"symbol": "AAVEUSDT", "strategy": "factor_043_9", "direction": "Short", "add_step": 0.030, "tp_step": 0.007,
    #  "margin": 9},
    # {"symbol": "AAVEUSDT", "strategy": "factor_043_10", "direction": "Short", "add_step": 0.015, "tp_step": 0.007,
    #  "margin": 7},
    # {"symbol": "SOLUSDT", "strategy": "factor_043_9", "direction": "Short", "add_step": 0.030, "tp_step": 0.008,
    #  "margin": 9},
    # {"symbol": "SOLUSDT", "strategy": "factor_043_9", "direction": "Short", "add_step": 0.025, "tp_step": 0.008,
    #  "margin": 7},

    # 做空 高净收益
    {
        "symbol": "NEARUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.015,
        "tp_step": 0.011,
        "margin": 5,
        "备注": "推荐次数 7，最高共识核心策略；适合第一阶段稳定做数据"
    },

    {
        "symbol": "AAVEUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.015,
        "tp_step": 0.011,
        "margin": 5,
        "备注": "推荐次数 7，最高共识核心策略；跨币验证较强"
    },

    {
        "symbol": "SOLUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.015,
        "tp_step": 0.011,
        "margin": 5,
        "备注": "推荐次数 7，收益增强版本；波动更高"
    },

    {
        "symbol": "NEARUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.015,
        "tp_step": 0.009,
        "margin": 5,
        "备注": "推荐次数 3，平原均净利更优秀，偏稳定版本"
    },

    {
        "symbol": "AAVEUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.015,
        "tp_step": 0.009,
        "margin": 5,
        "备注": "推荐次数 3，稳定底仓版本"
    },

    # ============================
    # 第二梯队：高稳定参数区域
    # factor_043_10 0.018~0.020
    # ============================

    {
        "symbol": "NEARUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.018,
        "tp_step": 0.009,
        "margin": 3,
        "备注": "推荐次数 3，参数高原区域，偏长期稳定"
    },

    {
        "symbol": "AAVEUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.020,
        "tp_step": 0.009,
        "margin": 3,
        "备注": "推荐次数 3，低回撤压舱石版本"
    },

    # ============================
    # 第三梯队：高收益激进区域
    # factor_043_9 0.010
    # ============================

    {
        "symbol": "NEARUSDT",
        "strategy": "factor_043_9",
        "direction": "Short",
        "add_step": 0.010,
        "tp_step": 0.012,
        "margin": 5,
        "备注": "推荐次数 3，高收益尖峰；收益高但参数敏感"
    },

    {
        "symbol": "SOLUSDT",
        "strategy": "factor_043_9",
        "direction": "Short",
        "add_step": 0.010,
        "tp_step": 0.012,
        "margin": 5,
        "备注": "推荐次数 3，高频分润版本"
    },

    {
        "symbol": "AAVEUSDT",
        "strategy": "factor_043_9",
        "direction": "Short",
        "add_step": 0.010,
        "tp_step": 0.012,
        "margin": 5,
        "备注": "推荐次数 3，高收益区域验证"
    },

    # ============================
    # factor_043_10 0.010~0.012
    # ============================

    {
        "symbol": "SOLUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.010,
        "tp_step": 0.012,
        "margin": 10,
        "备注": "推荐次数 2，高利润增强仓"
    },

    {
        "symbol": "NEARUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.010,
        "tp_step": 0.012,
        "margin": 10,
        "备注": "推荐次数 2，高收益版本"
    },

    {
        "symbol": "SOLUSDT",
        "strategy": "factor_043_10",
        "direction": "Short",
        "add_step": 0.012,
        "tp_step": 0.011,
        "margin": 5,
        "备注": "推荐次数 2，进攻型收益版本"
    },

    # ============================
    # 其他补充策略
    # ============================

    {
        "symbol": "AAVEUSDT",
        "strategy": "factor_024_8",
        "direction": "Short",
        "add_step": 0.025,
        "tp_step": 0.011,
        "margin": 9,
        "备注": "推荐次数 2，非043补充策略，用于分散因子风险"
    },

    {
        "symbol": "LINKUSDT",
        "strategy": "factor_008_8",
        "direction": "Short",
        "add_step": 0.025,
        "tp_step": 0.011,
        "margin": 7,
        "备注": "推荐次数 1，补充观察策略"
    },

    {
        "symbol": "UNIUSDT",
        "strategy": "factor_043_9",
        "direction": "Short",
        "add_step": 0.025,
        "tp_step": 0.009,
        "margin": 8,
        "备注": "推荐次数 1，平原均净利较好，收益较低，作为防守观察"
    },

    {
        "symbol": "UNIUSDT",
        "strategy": "factor_043_7",
        "direction": "Short",
        "add_step": 0.030,
        "tp_step": 0.011,
        "margin": 8,
        "备注": "推荐次数 1，低频稳定观察策略"
    },

    {
        "symbol": "AAVEUSDT",
        "strategy": "factor_043_2",
        "direction": "Short",
        "add_step": 0.030,
        "tp_step": 0.010,
        "margin": 8,
        "备注": "推荐次数 1，低风险补充策略"
    }

]


# =====================================================================
# 通用工具函数
# =====================================================================
def _parse_filename(filename):
    """解析文件名 -> (symbol, strategy_name, direction)，使用正则匹配增强鲁棒性"""
    # 匹配规范如: stage1_BTCUSDT_strategy_1_vwap_zscore_Long_xxxxx.pkl
    pattern = r"^stage1_([A-Z0-9]+)_(.+?)_(Long|Short)_"
    match = re.search(pattern, filename)
    if match:
        symbol = match.group(1)
        strategy_name = match.group(2)
        direction = match.group(3)
    else:
        symbol, strategy_name, direction = "Unknown", filename, "Unknown"
    return symbol, strategy_name, direction


def _load_pickle(fpath):
    if fpath.endswith(".gz"):
        with gzip.open(fpath, 'rb') as f:
            return pickle.load(f)
    with open(fpath, 'rb', buffering=4 * 1024 * 1024) as f:
        return pickle.load(f)


def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_dt(s):
    """与原始单策略代码完全一致的时间列解析口径"""
    if pd.api.types.is_numeric_dtype(s):
        v = pd.to_numeric(s, errors="coerce")
        mx = float(v.max()) if len(v) else 0.0
        if mx > 1e15:
            unit = "ns"
        elif mx > 1e11:
            unit = "ms"
        else:
            unit = "s"
        return pd.to_datetime(v, unit=unit)
    return pd.to_datetime(s, errors="coerce")


def _detect_time_cols(df):
    """沿用原代码的启发式：找出 (开仓时间列, 平仓时间列)"""
    time_col = start_col = end_col = None
    for col in df.columns:
        c_lower = str(col).lower()
        if any(k in c_lower for k in _TIME_LIKE_KEYS):
            if any(k in c_lower for k in ("close", "end", "finish")):
                end_col = col
                if time_col is None:
                    time_col = col
            elif any(k in c_lower for k in ("open", "start", "begin")):
                start_col = col
            else:
                if time_col is None:
                    time_col = col
    if end_col is None:
        end_col = time_col
    if start_col is None:
        start_col = end_col
    return start_col, end_col


def _max_true_run(mask):
    """布尔序列中最长连续 True 的长度"""
    m = np.asarray(mask, dtype=np.int8)
    if m.size == 0 or not m.any():
        return 0
    d = np.diff(np.concatenate(([0], m, [0])))
    starts = np.where(d == 1)[0]
    ends = np.where(d == -1)[0]
    return int((ends - starts).max())


def get_display_width(s):
    w = 0
    for c in str(s):
        w += 2 if unicodedata.east_asian_width(c) in ('F', 'W', 'A') else 1
    return w


def right_align(s, width):
    s = str(s)
    return " " * max(0, width - get_display_width(s)) + s


def format_val(val):
    if isinstance(val, (float, np.floating)):
        if not np.isfinite(val):
            return "-"
        return f"{val:.3f}" if 0 < abs(val) < 0.1 else f"{val:.2f}"
    return str(val)


def print_table(df_display):
    cols = list(df_display.columns)
    widths = []
    for col in cols:
        w = get_display_width(col)
        for v in df_display[col]:
            w = max(w, get_display_width(format_val(v)))
        widths.append(w)
    header = " | ".join(right_align(c, widths[i]) for i, c in enumerate(cols))
    sep = "-" * len(header)
    print(sep);
    print(header);
    print(sep)
    for _, row in df_display.iterrows():
        print(" | ".join(right_align(format_val(row[c]), widths[i]) for i, c in enumerate(cols)))
    print(sep)


def _make_label(sym, strat, direct, margin, add_s, tp_s):
    short = sym.replace("USDT", "")
    d = "L" if str(direct).capitalize() == "Long" else "S"
    return f"{short}|{strat}|{d}|M{int(margin)}|a{add_s * 1000:.0f}|t{tp_s * 1000:.0f}"


# =====================================================================
# 平原宽表精确匹配 (修复原来只用 币种+策略+Margin 的错误匹配)
# =====================================================================
def _match_plateau_row(plateau_df, sym, strat, direct, margin, add_s, tp_s, mult=None):
    empty = {"平原存活安全垫(天)": np.nan, "平原均净利(M倍)": np.nan, "平原均总收益(M倍)": np.nan,
             "平原存活率(%)": np.nan, "平原90%分位无盈利(天)": np.nan, "中位存活(天)": np.nan,
             "全币邻居数": np.nan}
    if plateau_df is None or plateau_df.empty:
        return empty
    m = plateau_df
    cond = pd.Series(True, index=m.index)
    if "币种" in m.columns:
        cond &= (m["币种"].astype(str) == sym)
    if "策略" in m.columns:
        cond &= (m["策略"].astype(str) == strat)
    if "方向" in m.columns:
        cond &= (m["方向"].astype(str).str.capitalize() == str(direct).capitalize())
    if "Margin" in m.columns:
        cond &= np.isclose(pd.to_numeric(m["Margin"], errors="coerce").fillna(-1), float(margin), atol=1e-6)
    if "加仓间距" in m.columns:
        cond &= np.isclose(pd.to_numeric(m["加仓间距"], errors="coerce").fillna(-1), float(add_s), atol=1e-6)
    if "止盈间距" in m.columns:
        cond &= np.isclose(pd.to_numeric(m["止盈间距"], errors="coerce").fillna(-1), float(tp_s), atol=1e-6)
    if mult is not None and "加仓倍数" in m.columns:
        cond &= np.isclose(pd.to_numeric(m["加仓倍数"], errors="coerce").fillna(-1), float(mult), atol=1e-6)
    sub = m[cond]
    if sub.empty:
        return empty
    if len(sub) > 1:
        print(f"   [警告] 平原表匹配到 {len(sub)} 行(参数不唯一, 建议补 multiplier): {sym}/{strat}/M{margin}")
    r = sub.iloc[0]
    out = dict(empty)
    for k in list(empty.keys()):
        if k in sub.columns:
            try:
                out[k] = float(r[k])
            except Exception:
                out[k] = np.nan
    return out


# =====================================================================
# Stage A : 归一化并导出单策略逐笔明细
# =====================================================================
def _normalize_trades_df(trades_df, cycles_df, margin):
    """
    与原始单策略榜单完全一致的口径:
      1) 取 pnl 列 -> 与 report['total_net_pnl_in_margin'] 对齐算 ratio -> 得到 pnl_M (单位:M倍)
      2) 解析开/平仓时间 -> open_dt / close_dt / holding_h
      3) 爆仓判定: 优先显式列, 其次 pnl_M <= -0.8, 并与 report['n_blowup'] 交叉校验
      4) 浮亏(单笔最大回撤)同比例换算 -> float_loss_M
    返回 (out_df, summary_dict)
    """
    if trades_df is None or len(trades_df) == 0:
        raise RuntimeError("trades_df 为空")

    out = trades_df.copy()
    try:
        report = evaluate_free_ride(trades_df, cycles_df, margin) or {}
    except Exception as e:
        report = {}
        print(f"   [警告] evaluate_free_ride 执行失败({e})，report 相关字段将缺失")

    pnl_col = _pick_col(out, PNL_COL_CANDIDATES)
    if pnl_col is None:
        raise RuntimeError(f"trades_df 中找不到收益列，现有列: {list(out.columns)[:20]}")

    raw = pd.to_numeric(out[pnl_col], errors="coerce").fillna(0.0).astype(float)
    gross_profit = float(raw[raw > 0].sum())
    gross_loss = float(raw[raw < 0].sum())
    net_pnl_sum = gross_profit + gross_loss
    report_net = float(report.get("total_net_pnl_in_margin", 0.0) or 0.0)

    ratio = 1.0
    if abs(net_pnl_sum) > 1e-6 and abs(report_net) > 1e-6 and abs(net_pnl_sum - report_net) > 1e-6:
        ratio = report_net / net_pnl_sum  # <<< 关键: 单位换算靠 report 校准, 绝不靠"总和大小"猜

    out["pnl_M"] = raw * ratio
    if abs(report_net) > 1e-9 and abs(out["pnl_M"].sum() - report_net) > 1e-3:
        print(f"   [警告] 归一化后净利({out['pnl_M'].sum():.4f}) 与 report({report_net:.4f}) 不一致")

    # ---- 时间 ----
    start_col, end_col = _detect_time_cols(trades_df)
    if end_col is None:
        raise RuntimeError("无法识别平仓时间列")
    close_dt = _to_dt(out[end_col])
    open_dt = _to_dt(out[start_col]) if start_col else close_dt
    bad = open_dt.isna() | (open_dt > close_dt)
    open_dt = open_dt.mask(bad, close_dt)
    out["open_dt"] = open_dt
    out["close_dt"] = close_dt
    out["holding_h"] = (close_dt - open_dt).dt.total_seconds() / 3600.0

    # ---- 爆仓 ----
    blow_col = next((c for c in out.columns
                     if any(k in str(c).lower() for k in ("blowup", "blow_up", "liquidat", "is_bust"))), None)
    if blow_col is not None:
        is_blow = out[blow_col].fillna(False).astype(bool)
        blow_src = f"col:{blow_col}"
    else:
        is_blow = out["pnl_M"] <= -BLOWUP_LOSS_THRESHOLD_M
        blow_src = f"pnl_M<=-{BLOWUP_LOSS_THRESHOLD_M}"
    rep_blow = int(report.get("n_blowup", 0) or 0)
    if rep_blow > 0 and int(is_blow.sum()) == 0:
        # 兜底: 取最亏的 rep_blow 笔当爆仓, 避免风险被完全漏掉
        worst_idx = out["pnl_M"].nsmallest(rep_blow).index
        is_blow = pd.Series(False, index=out.index)
        is_blow.loc[worst_idx] = True
        blow_src = "fallback:worst_n_by_report"
    if rep_blow and abs(int(is_blow.sum()) - rep_blow) > 0:
        print(f"   [提示] 爆仓数不一致: 明细={int(is_blow.sum())} report={rep_blow} (来源 {blow_src})")
    out["is_blowup_flag"] = is_blow.values

    # ---- 单笔浮亏(持仓期最大浮亏), 用于组合"同时深水"评估 ----
    mdd_col = _pick_col(out, MDD_COL_CANDIDATES)
    if mdd_col is not None:
        out["float_loss_M"] = pd.to_numeric(out[mdd_col], errors="coerce").abs().fillna(0.0) * abs(ratio)
        has_float = True
    else:
        out["float_loss_M"] = 0.0
        has_float = False

    out["pnl_scale_ratio"] = ratio
    out = out.sort_values("close_dt").reset_index(drop=True)

    hold_ok = out["holding_h"][out["holding_h"] >= 0]
    exp_life_h = report.get("expected_lifespan_hour", np.inf)
    summary = {
        "总信号数": int(report.get("n_cycles_total", 0) or 0),
        "实际开仓数": int(len(out)),
        "胜率(%)": round(float(report.get("win_rate", 0) or 0) * 100, 2),
        "爆仓次数": int(is_blow.sum()),
        "爆仓判定来源": blow_src,
        "预期存活(天)": (round(float(exp_life_h) / 24.0, 2) if np.isfinite(exp_life_h) else 999.0),
        "总收益(M倍)": round(gross_profit * ratio, 2),
        "总亏损(M倍)": round(gross_loss * ratio, 2),
        "净利润(M倍)": round(float(out["pnl_M"].sum()), 2),
        "平均持仓(h)": round(float(hold_ok.mean()) if len(hold_ok) else 0.0, 2),
        "最大持仓(h)": round(float(hold_ok.max()) if len(hold_ok) else 0.0, 2),
        "平均单笔浮亏(M)": round(float(out["float_loss_M"].mean()), 4),
        "最大单笔浮亏(M)": round(float(out["float_loss_M"].max()), 4),
        "有浮亏数据": bool(has_float),
        "pnl换算ratio": round(ratio, 8),
        "数据起": str(out["open_dt"].iloc[0]),
        "数据止": str(out["close_dt"].iloc[-1]),
    }
    return out, summary


def extract_target_trades_csv(cache_dir=CACHE_DIR,
                              short_cache_dir=SHORT_CACHE_DIR,
                              output_dir="./extracted_trades_csv",
                              target_configs=None,
                              plateau_csv=None):
    """
    针对给定的精选参数组合，独立回放并导出对应的 trades_df 逐笔交易记录 CSV。
    修复要点:
      * 同一(币/策略/方向/加仓/止盈) 只加载 pkl 一次, 内循环跑多个 Margin (IO 降 N 倍)
      * 收益统一换算为 M 倍并落盘 pnl_M (禁止后续阶段再猜单位)
      * 记录/校验 multiplier(加仓倍数), 写进文件名, 避免张冠李戴与互相覆盖
      * 附带导出 _single_strategy_index.csv (含平原精确匹配指标 + 推荐权重)
    """
    target_configs = target_configs or TARGET_CONFIGS
    os.makedirs(output_dir, exist_ok=True)

    plateau_df = None
    if plateau_csv:
        if os.path.exists(plateau_csv):
            plateau_df = pd.read_csv(plateau_csv)
            print(f"📖 已加载平原宽表: {plateau_csv} ({len(plateau_df):,} 行)")
        else:
            print(f"[警告] 平原宽表不存在: {plateau_csv}，安全垫等指标将为空(不会用默认值静默放行)")

    # ---- 1. 归并目标 ----
    groups = {}
    for cfg in target_configs:
        key = (cfg["symbol"], cfg["strategy"], str(cfg["direction"]).capitalize(),
               round(float(cfg["add_step"]), 6), round(float(cfg["tp_step"]), 6))
        g = groups.setdefault(key, {"margins": {}, "multiplier": cfg.get("multiplier")})
        if cfg.get("multiplier") is not None:
            g["multiplier"] = float(cfg["multiplier"])
        m = int(cfg["margin"])
        note = str(cfg.get("备注", ""))
        w = cfg.get("weight")
        if w is None:
            mt = re.search(r"推荐的次数为\s*(\d+)", note)
            w = float(mt.group(1)) if mt else 1.0
        if m in g["margins"]:
            print(f"[提示] 目标配置重复, 已自动去重: {key} M={m}")
        g["margins"][m] = {"note": note, "weight": float(w)}

    n_targets = sum(len(g["margins"]) for g in groups.values())
    print(f"🎯 待抽取目标: {n_targets} 个 (归并为 {len(groups)} 个缓存文件读取任务)")

    # ---- 2. 扫描并按文件名粗筛 ----
    files = set(glob.glob(os.path.join(cache_dir, "stage1_*.pkl"))) | \
            set(glob.glob(os.path.join(cache_dir, "stage1_*.pkl.gz")))
    if os.path.exists(short_cache_dir):
        files |= set(glob.glob(os.path.join(short_cache_dir, "stage1_*.pkl"))) | \
                 set(glob.glob(os.path.join(short_cache_dir, "stage1_*.pkl.gz")))
    files = sorted(files)

    coarse = {(k[0], k[1], k[2]) for k in groups}
    candidates = []
    for fp in files:
        s, st, d = _parse_filename(os.path.basename(fp))
        if (s, st, str(d).capitalize()) in coarse:
            candidates.append(fp)
    print(f"📂 缓存文件总数 {len(files)}，按币种/策略/方向粗筛后候选 {len(candidates)} 个，开始逐个加载...\n")

    # ---- 3. 逐候选文件加载一次, 内循环多 Margin ----
    done = set()
    summaries = []
    mult_seen = {}
    for fi, fpath in enumerate(candidates, 1):
        fname = os.path.basename(fpath)
        sym, strat, direct = _parse_filename(fname)
        direct = str(direct).capitalize()
        try:
            data = _load_pickle(fpath)
        except Exception as e:
            print(f"[警告] 读取失败已跳过: {fname} | {e}")
            continue

        attrs = data.get("attrs", {}) or {}
        add_s = round(float(attrs.get("add_step", -1)), 6)
        tp_s = round(float(attrs.get("tp_step", -1)), 6)
        mult = attrs.get("multiplier", None)
        mult = float(mult) if mult is not None else None
        key = (sym, strat, direct, add_s, tp_s)

        if key not in groups:
            del data
            gc.collect()
            continue

        g = groups[key]
        if g["multiplier"] is not None and mult is not None and not math.isclose(g["multiplier"], mult, abs_tol=1e-6):
            del data
            gc.collect()
            continue

        mult_seen.setdefault(key, set()).add(mult)
        if g["multiplier"] is None and len(mult_seen[key]) > 1:
            print(f"[⚠ 重要] {key} 在缓存中存在多个加仓倍数 {sorted(x for x in mult_seen[key] if x is not None)}；"
                  f"已按倍数分别导出，请在 TARGET_CONFIGS 中显式补 'multiplier' 以锁定榜单那一行！")

        cycles_df = data.pop("df")
        cycles_df.attrs = attrs
        data.clear();
        del data
        gc.collect()

        if cycles_df is None or len(cycles_df) == 0:
            print(f"[警告] cycles 为空: {fname}")
            del cycles_df
            gc.collect()
            continue

        replayer = TimelineReplayer(cycles_df)
        for margin in sorted(g["margins"].keys()):
            meta = g["margins"][margin]
            try:
                trades_df = replayer.run(margin)
                norm_df, summ = _normalize_trades_df(trades_df, cycles_df, margin)
            except Exception as e:
                print(f"[警告] 回放/归一化失败: {sym}|{strat}|{direct}|M{margin} | {type(e).__name__}: {e}")
                continue

            norm_df["symbol"] = sym
            norm_df["strategy"] = strat
            norm_df["direction"] = direct
            norm_df["margin"] = margin
            norm_df["add_step"] = add_s
            norm_df["tp_step"] = tp_s
            norm_df["multiplier"] = mult if mult is not None else np.nan

            mtag = f"_x{mult:g}" if mult is not None else ""
            out_filename = (f"trades_{sym}_{strat}_{direct}_M{margin}"
                            f"_add{add_s:.3f}_tp{tp_s:.3f}{mtag}.csv")
            norm_df.to_csv(os.path.join(output_dir, out_filename), index=False, encoding="utf-8-sig")

            row = {
                "file": out_filename,
                "label": _make_label(sym, strat, direct, margin, add_s, tp_s) + mtag,
                "币种": sym, "策略": strat, "方向": direct, "Margin": margin,
                "加仓间距": add_s, "止盈间距": tp_s, "加仓倍数": mult,
                "推荐权重": meta["weight"], "备注": meta["note"],
            }
            row.update(summ)
            row.update(_match_plateau_row(plateau_df, sym, strat, direct, margin, add_s, tp_s, mult))
            summaries.append(row)
            done.add((key, margin))
            print(f"[{len(done)}/{n_targets}] ✅ {out_filename} | 笔数={summ['实际开仓数']} "
                  f"净利={summ['净利润(M倍)']}M 爆仓={summ['爆仓次数']} ratio={summ['pnl换算ratio']}")

            del trades_df, norm_df
            gc.collect()

        del cycles_df, replayer
        gc.collect()

    # ---- 4. 汇总索引 ----
    if summaries:
        idx_df = pd.DataFrame(summaries).sort_values(["币种", "策略", "方向", "Margin"])
        idx_df.to_csv(os.path.join(output_dir, INDEX_FILE), index=False, encoding="utf-8-sig")

    missing = [(k, m) for k, g in groups.items() for m in g["margins"] if (k, m) not in done]
    print("\n" + "=" * 70)
    print(f"🎉 导出完成: {len(done)}/{n_targets} 个，目录: {output_dir}")
    print(f"📑 元数据索引: {INDEX_FILE}")
    if missing:
        print(f"⚠️ 未命中 {len(missing)} 个组合(请核对 add/tp/multiplier 与缓存 attrs):")
        for k, m in missing:
            print(f"   - {k[0]} | {k[1]} | {k[2]} | add={k[3]} tp={k[4]} | M={m}")
    print("=" * 70)


def _load_strategy_records(csv_dir, plateau_csv=None):
    """读取交易明细；兼容原调用，严格解析爆仓布尔标志，时间统一为 UTC。

    plateau_csv 保留用于兼容原接口；本次指标不需要读取平原表。
    已有 is_blowup_flag 优先；只有完全缺少爆仓字段时才按原阈值推断。
    """
    def parse_flags(series, filename):
        # astype(bool) 会把字符串 "False"、"0" 都转换成 True。
        values = series.astype("string").str.strip().str.lower()
        mapping = {
            "true": True, "1": True, "1.0": True,
            "false": False, "0": False, "0.0": False,
        }
        missing = series.isna() | values.eq("").fillna(False)
        invalid = ~missing & ~values.isin(mapping)
        if invalid.any():
            bad = series[invalid].astype(str).unique()[:5].tolist()
            raise ValueError(f"{filename}: 无法识别 is_blowup_flag={bad}")
        return values.map(mapping).fillna(False).astype(bool)

    files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
    files = [f for f in files if not os.path.basename(f).startswith("_")]
    if not files:
        return [], None

    idx_path = os.path.join(csv_dir, INDEX_FILE)
    idx_df = pd.read_csv(idx_path) if os.path.exists(idx_path) else None
    records = []
    for f in files:
        fname = os.path.basename(f)
        df = pd.read_csv(f)
        if df.empty:
            continue

        margin = float(df["margin"].iloc[0]) if "margin" in df.columns else 1.0
        if not np.isfinite(margin) or margin <= 0:
            raise ValueError(f"{fname}: margin 必须为正数")

        if "pnl_M" in df.columns:
            pnl = pd.to_numeric(df["pnl_M"], errors="coerce").astype(float)
        else:
            pcol = _pick_col(df, PNL_COL_CANDIDATES)
            if pcol is None:
                print(f"[提示] 无收益列，跳过: {fname}")
                continue
            pnl = pd.to_numeric(df[pcol], errors="coerce").astype(float)
            if "in_margin" not in pcol:
                pnl = pnl / margin
            print(f"[警告] {fname} 缺少 pnl_M，按列名换算；建议重跑 Stage A。")

        if "start_ms" in df.columns and "end_ms" in df.columns:
            open_dt = pd.to_datetime(pd.to_numeric(df["start_ms"], errors="coerce"),
                                     unit="ms", errors="coerce", utc=True)
            close_dt = pd.to_datetime(pd.to_numeric(df["end_ms"], errors="coerce"),
                                      unit="ms", errors="coerce", utc=True)
        elif "close_dt" in df.columns:
            close_dt = pd.to_datetime(df["close_dt"], errors="coerce", utc=True)
            open_dt = (pd.to_datetime(df["open_dt"], errors="coerce", utc=True)
                       if "open_dt" in df.columns else close_dt.copy())
        else:
            sc, ec = _detect_time_cols(df)
            if ec is None:
                print(f"[警告] 无平仓时间列，跳过: {fname}")
                continue
            close_dt = pd.to_datetime(_to_dt(df[ec]), errors="coerce", utc=True)
            open_dt = (pd.to_datetime(_to_dt(df[sc]), errors="coerce", utc=True)
                       if sc else close_dt.copy())
        open_dt = open_dt.fillna(close_dt)
        open_dt = open_dt.mask(open_dt > close_dt, close_dt)

        if "is_blowup_flag" in df.columns:
            is_blowup = parse_flags(df["is_blowup_flag"], fname)
        elif "outcome" in df.columns:
            is_blowup = (df["outcome"].astype("string").str.strip().str.lower()
                         .isin(["blowup", "blow_up", "liquidation", "liquidated", "bust"]))
        else:
            is_blowup = pnl <= -BLOWUP_LOSS_THRESHOLD_M
            print(f"[警告] {fname} 无爆仓标志，按 pnl_M <= "
                  f"-{BLOWUP_LOSS_THRESHOLD_M:g} 推断；建议重跑 Stage A。")

        ok = close_dt.notna() & np.isfinite(pnl)
        if not ok.all():
            # 缺失收益不能静默填 0，否则会影响盈利窗口率、周期率及排名。
            raise ValueError(f"{fname}: 有 {int((~ok).sum())} 行平仓时间或收益无效，请修复明细")

        order = np.argsort(close_dt.to_numpy(), kind="stable")
        df = df.iloc[order].reset_index(drop=True)
        pnl = pnl.iloc[order].reset_index(drop=True)
        open_dt = open_dt.iloc[order].reset_index(drop=True)
        close_dt = close_dt.iloc[order].reset_index(drop=True)
        is_blowup = is_blowup.iloc[order].reset_index(drop=True)
        float_loss = pd.to_numeric(
            df.get("float_loss_M", pd.Series(0.0, index=df.index)), errors="coerce"
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0).abs().to_numpy()

        sym = str(df["symbol"].iloc[0]) if "symbol" in df.columns else "UNK"
        strat = str(df["strategy"].iloc[0]) if "strategy" in df.columns else "UNK"
        direct = str(df["direction"].iloc[0]).capitalize() if "direction" in df.columns else "UNK"
        add_s = float(df["add_step"].iloc[0]) if "add_step" in df.columns else 0.0
        tp_s = float(df["tp_step"].iloc[0]) if "tp_step" in df.columns else 0.0
        mult = (float(df["multiplier"].iloc[0])
                if "multiplier" in df.columns and pd.notna(df["multiplier"].iloc[0]) else None)
        weight, note = 1.0, ""
        if idx_df is not None and "file" in idx_df.columns:
            matched = idx_df[idx_df["file"] == fname]
            if not matched.empty:
                meta = matched.iloc[0]
                weight = float(meta["推荐权重"]) if pd.notna(meta.get("推荐权重")) else 1.0
                note = str(meta["备注"]) if pd.notna(meta.get("备注")) else ""

        records.append({
            "file": fname,
            "label": _make_label(sym, strat, direct, margin, add_s, tp_s)
                     + (f"_x{mult:g}" if mult is not None else ""),
            "symbol": sym, "strategy": strat, "direction": direct, "margin": margin,
            "add_step": add_s, "tp_step": tp_s, "multiplier": mult,
            "signal_key": (sym, strat, direct, round(add_s, 6), round(tp_s, 6), mult),
            "pnl": pnl.to_numpy(dtype=float),
            "open_dt": open_dt.dt.tz_convert(None).to_numpy(dtype="datetime64[ns]"),
            "close_dt": close_dt.dt.tz_convert(None).to_numpy(dtype="datetime64[ns]"),
            "float_loss": float_loss, "is_blowup": is_blowup.to_numpy(dtype=bool),
            "weight": weight, "note": note, "has_float": bool(np.any(float_loss > 0)),
        })
    return records, idx_df


def evaluate_multi_strategy_portfolios(
        csv_dir="./extracted_trades_csv",
        plateau_csv=None,
        output_csv="portfolio_multi_ranking.csv",
        min_k=2,
        max_k=5,
        top_n_per_k=5,
        allow_same_signal=False,
        min_overlap_days=180,
        weight_mode="equal",
        max_combos=400000,
        filter_q_balance=10.0,
        filter_roll_profit_win_rate_30=None,
        filter_roll_profit_win_rate_7=None,
        filter_roll_profit_win_rate_1=None,
):
    """按完整爆仓周期盈利率降序，再按 30 日盈利窗口率降序评估组合。

    口径：
      * 日收益按平仓日入账，包含无交易的自然日；M 是保证金归一化单位。
      * 共同窗口沿用各成员“首笔开仓日～末笔平仓日”的交集，含首尾日。
      * MDD/Calmar 为日末已实现口径；初始累计收益 0、初始权益 1M。
      * 两两持仓重合 = 同时持仓日 / 至少一方持仓日，在共同窗口计算；
        忽略多空方向，开平仓当日均计持仓日，双方均空仓时约定为 0。
      * 完整周期是 (上次爆仓时刻, 下次爆仓时刻]，含末端爆仓损失；
        组合边界取所有成员爆仓时刻的并集，不代表组合账户本身爆仓。
        同日不同时刻分别统计，同一时刻同时发生的爆仓合并为一个边界。
        边界必须都在共同窗口内；无完整周期返回 NaN，排序放最后。
      * Profit Factor 使用加权逐笔正/负收益，不能先做日内净额抵消。
      * 数值不提前 round；仅显示时格式化，保证排序使用完整精度。
    """
    def fmt(value, digits=2, suffix=""):
        if pd.isna(value):
            return "N/A"
        if np.isposinf(value):
            return "∞" + suffix
        if np.isneginf(value):
            return "-∞" + suffix
        return f"{value:.{digits}f}" + suffix

    def ratio(numerator, denominator):
        if denominator > 0:
            return float(numerator / denominator)
        return float("inf") if numerator > 0 else float("nan")

    def realized_risk(daily):
        # 把初始 0 纳入峰值；首日亏损也形成回撤和水下期。
        cum = np.cumsum(daily, dtype=float)
        peak = np.maximum.accumulate(np.r_[0.0, cum])[1:]
        dd = np.maximum(peak - cum, 0.0)
        net = float(cum[-1])
        annual = net * DAYS_PER_YEAR / len(daily)
        mdd = float(dd.max())
        return {
            "net": net, "annual": annual, "mdd": mdd,
            "relative_dd": float(np.max(dd / (1.0 + peak)) * 100.0),
            "underwater": _max_true_run(dd > 0),
            "calmar": ratio(annual, mdd),
        }

    def rolling_stats(prefix, days):
        if len(prefix) - 1 < days:
            return float("nan"), float("nan")
        values = prefix[days:] - prefix[:-days]
        return float(np.mean(values > 0) * 100.0), float(values.min())

    def window_blowups(i, lo, hi):
        boundaries = event_data[i][2]
        a = np.searchsorted(boundaries, day_ns[lo], side="left")
        b = np.searchsorted(boundaries, day_ns[hi + 1], side="left")
        return boundaries[a:b]

    def cycle_stats(indices, weights, boundaries):
        count = len(boundaries) - 1
        if count <= 0:
            return 0, float("nan"), float("nan")
        profits = np.zeros(count, dtype=float)
        for i, weight in zip(indices, weights):
            times, prefix, _ = event_data[i]
            # C(t) 包含时刻 t 的全部平仓；C(b)-C(a) 正好对应 (a,b]。
            positions = np.searchsorted(times, boundaries, side="right")
            profits += weight * np.diff(prefix[positions])
        return count, float(np.mean(profits > 0) * 100.0), float(profits.mean())

    if weight_mode not in ("equal", "recommend"):
        raise ValueError("weight_mode 只能为 'equal' 或 'recommend'")
    for name, value, minimum in (
            ("min_k", min_k, 1), ("max_k", max_k, 1),
            ("top_n_per_k", top_n_per_k, 0),
            ("min_overlap_days", min_overlap_days, 1), ("max_combos", max_combos, 1)):
        if not isinstance(value, (int, np.integer)) or value < minimum:
            raise ValueError(f"{name} 必须为不小于 {minimum} 的整数")
    if min_k > max_k:
        raise ValueError("min_k 不能大于 max_k")
    filters = {
        30: filter_roll_profit_win_rate_30,
        7: filter_roll_profit_win_rate_7,
        1: filter_roll_profit_win_rate_1,
    }
    for days, threshold in filters.items():
        if threshold is not None and (not np.isfinite(threshold) or not 0 <= threshold <= 100):
            raise ValueError(f"{days} 日盈利窗口率阈值必须在 0～100 之间，或为 None")
    if filter_q_balance is not None and not np.isfinite(filter_q_balance):
        raise ValueError("filter_q_balance 必须是有限数值或 None")

    records, _ = _load_strategy_records(csv_dir, plateau_csv)
    N = len(records)
    if N < min_k:
        print(f"[提示] 有效策略数 {N} 少于 min_k={min_k}，无法构建组合。")
        return
    max_k = min(max_k, N)
    total_combos = sum(math.comb(N, k) for k in range(min_k, max_k + 1))
    if total_combos > max_combos:
        print(f"[提示] 组合数 {total_combos:,} 超过 max_combos={max_combos:,}，已中止。")
        return

    base_w = np.array([r["weight"] for r in records], dtype=float)
    if weight_mode == "recommend" and (not np.isfinite(base_w).all() or np.any(base_w <= 0)):
        raise ValueError("recommend 模式要求所有推荐权重均为有限正数")

    print("=" * 112)
    print("马丁组合评估 | 排序：完整爆仓周期盈利率 ↓ → 30日盈利窗口率 ↓")
    print(f"成员 {N} | K={min_k}～{max_k} | 最少共同窗口 {min_overlap_days} 天 | 权重 {weight_mode}")
    print("M=保证金归一化单位；回撤为日末已实现口径，初始权益=1M。")
    print("窗口按交易明细起止日交集推定；持仓重合按日统计，不区分多空方向。")
    print("任一成员爆仓即切分；完整周期=(上次爆仓时刻,下次爆仓时刻]；同时刻合并边界。")
    print("盈利窗口和盈利周期均要求净利润 > 0；无足够数据的指标显示 N/A。")
    print("四段贡献按共同窗口的时间顺序四等分，并非自然季度；净利≤0时显示 N/A。")
    q_filter = "关闭" if filter_q_balance is None else f"最低阶段贡献≥{filter_q_balance:g}%"
    roll_filters = " | ".join(
        f"{days}日：{'关闭' if threshold is None else f'≥{threshold:g}%'}"
        for days, threshold in filters.items())
    print(f"过滤：四段 {q_filter} | 盈利窗口率 {roll_filters}")
    print("=" * 112)

    g_start = min(pd.Timestamp(r["open_dt"].min()) for r in records).normalize()
    g_end = max(pd.Timestamp(r["close_dt"].max()) for r in records).normalize()
    all_days = pd.date_range(g_start, g_end, freq="D")
    T = len(all_days)
    day_ns = pd.date_range(g_start, periods=T + 1, freq="D").asi8
    month_key = all_days.year.to_numpy() * 12 + all_days.month.to_numpy() - 1
    PNL, POS, NEG, CNT, FLOAT = (np.zeros((N, T)) for _ in range(5))
    HOLD = np.zeros((N, T), dtype=bool)
    first_i = np.zeros(N, dtype=np.int64)
    last_i = np.zeros(N, dtype=np.int64)
    event_data = []

    for i, r in enumerate(records):
        opens = pd.DatetimeIndex(r["open_dt"])
        closes = pd.DatetimeIndex(r["close_dt"])
        si = np.asarray((opens.normalize() - g_start).days, dtype=np.int64)
        ei = np.asarray((closes.normalize() - g_start).days, dtype=np.int64)
        si = np.minimum(si, ei)
        p = np.asarray(r["pnl"], dtype=float)
        np.add.at(PNL[i], ei, p)
        np.add.at(POS[i], ei, np.maximum(p, 0.0))
        np.add.at(NEG[i], ei, np.minimum(p, 0.0))
        np.add.at(CNT[i], ei, 1.0)

        d_hold = np.zeros(T + 1)
        np.add.at(d_hold, si, 1.0)
        np.add.at(d_hold, ei + 1, -1.0)
        HOLD[i] = np.cumsum(d_hold)[:T] > 0
        fl = r.get("float_loss")
        if fl is not None and len(fl) == len(p) and np.any(fl > 0):
            # 仅为原辅助字段保留的持仓区间最大浮亏代理，不是 MTM 曲线。
            d_float = np.zeros(T + 1)
            np.add.at(d_float, si, fl)
            np.add.at(d_float, ei + 1, -fl)
            FLOAT[i] = np.maximum(np.cumsum(d_float)[:T], 0.0)

        first_i[i], last_i[i] = int(si.min()), int(ei.max())
        # 保留实际平仓时间，避免 BLOWUP 日布尔矩阵丢失同日多个边界。
        order = np.argsort(closes.asi8, kind="stable")
        times = closes.asi8[order]
        prefix = np.r_[0.0, np.cumsum(p[order], dtype=float)]
        flags = np.asarray(r["is_blowup"], dtype=bool)[order]
        event_data.append((times, prefix, np.unique(times[flags])))

    # 保留原 CSV 的方向签名重合代理；这不是收益相关系数。
    CORR = np.full((N, N), np.nan)
    for i, j in itertools.combinations(range(N), 2):
        lo, hi = max(first_i[i], first_i[j]), min(last_i[i], last_i[j])
        if hi - lo + 1 >= 30:
            a, b = HOLD[i, lo:hi + 1], HOLD[j, lo:hi + 1]
            union = int(np.count_nonzero(a | b))
            overlap = float(np.count_nonzero(a & b) / union) if union else 0.0
            sign = 1.0 if records[i]["direction"] == records[j]["direction"] else -1.0
            CORR[i, j] = CORR[j, i] = overlap * sign

    sig_keys = [r["signal_key"] for r in records]
    member_cache = {}

    def member_risk(i, lo, hi):
        key = (i, lo, hi)
        if key not in member_cache:
            member_cache[key] = realized_risk(PNL[i, lo:hi + 1])
        return member_cache[key]

    results = []
    skipped = {"同源": 0, "重叠不足": 0, "盈利窗口率": 0, "四段贡献": 0}
    processed = 0
    print(f"数据矩阵：{T} 天 | 待扫描 {total_combos:,} 个组合\n")
    for k in range(min_k, max_k + 1):
        for idxs in itertools.combinations(range(N), k):
            processed += 1
            if processed % 20000 == 0:
                print(f"   ...已扫描 {processed:,}/{total_combos:,}")
            if not allow_same_signal and len({sig_keys[i][:5] for i in idxs}) < k:
                skipped["同源"] += 1
                continue
            ii = list(idxs)
            lo, hi = int(first_i[ii].max()), int(last_i[ii].min())
            n_days = hi - lo + 1
            if n_days < min_overlap_days:
                skipped["重叠不足"] += 1
                continue
            sl = slice(lo, hi + 1)
            w = (base_w[ii] / base_w[ii].sum() if weight_mode == "recommend"
                 else np.full(k, 1.0 / k))
            daily = (PNL[ii, sl] * w[:, None]).sum(axis=0)
            prefix = np.r_[0.0, np.cumsum(daily, dtype=float)]
            net = float(prefix[-1])
            win_7, worst_7 = rolling_stats(prefix, 7)
            win_30, worst_30 = rolling_stats(prefix, 30)
            win_1 = float(np.mean(daily > 0) * 100.0)
            rates = {1: win_1, 7: win_7, 30: win_30}
            if any(threshold is not None and
                   (not np.isfinite(rates[days]) or rates[days] < threshold)
                   for days, threshold in filters.items()):
                skipped["盈利窗口率"] += 1
                continue

            if net > 0 and n_days >= 4:
                q_ratios = [float(chunk.sum() / net * 100.0)
                            for chunk in np.array_split(daily, 4)]
                q_min = min(q_ratios)
                q_str = " | ".join(f"Q{i + 1} {value:.1f}%" for i, value in enumerate(q_ratios))
            else:
                q_ratios = [float("nan")] * 4
                q_min = float("nan")
                q_str = "N/A（总净利≤0或窗口不足4天）"
            if filter_q_balance is not None and (not np.isfinite(q_min) or q_min < filter_q_balance):
                skipped["四段贡献"] += 1
                continue

            _, worst_90 = rolling_stats(prefix, 90)
            boundaries = np.unique(np.concatenate([window_blowups(i, lo, hi) for i in ii]))
            cycle_count, cycle_win, cycle_mean = cycle_stats(ii, w, boundaries)
            risk = realized_risk(daily)
            gp = float((POS[ii, sl] * w[:, None]).sum())
            gl = float((NEG[ii, sl] * w[:, None]).sum())
            pf = ratio(gp, abs(gl))
            member_metrics = [member_risk(i, lo, hi) for i in ii]
            mean_member_dd = float(np.mean([m["mdd"] for m in member_metrics]))
            div_dd = ratio(risk["mdd"], mean_member_dd)
            member_calmars = [m["calmar"] for m in member_metrics if not np.isnan(m["calmar"])]
            best_calmar = max(member_calmars) if member_calmars else float("nan")
            best_net = max(m["net"] for m in member_metrics)

            pair_overlaps = []
            for a, b in itertools.combinations(ii, 2):
                hold_a, hold_b = HOLD[a, sl], HOLD[b, sl]
                union = int(np.count_nonzero(hold_a | hold_b))
                pair_overlaps.append(float(np.count_nonzero(hold_a & hold_b) / union)
                                     if union else 0.0)
            mean_overlap = float(np.mean(pair_overlaps) * 100.0) if pair_overlaps else float("nan")
            max_overlap = float(np.max(pair_overlaps) * 100.0) if pair_overlaps else float("nan")

            # 继续保留原 CSV 的辅助指标，不在控制台占用展示空间。
            mk = month_key[sl]
            monthly = np.bincount(mk - mk[0], weights=daily)
            monthly_nonzero = monthly[monthly != 0]
            conc = HOLD[ii, sl].sum(axis=0)
            float_sum = (FLOAT[ii, sl] * w[:, None]).sum(axis=0)
            cvals = [CORR[a, b] for a, b in itertools.combinations(ii, 2)
                     if np.isfinite(CORR[a, b])]
            symbol_weights = {}
            for i, weight in zip(ii, w):
                sym = records[i]["symbol"]
                symbol_weights[sym] = symbol_weights.get(sym, 0.0) + weight
            n_long = sum(records[i]["direction"] == "Long" for i in ii)
            half_net = float(prefix[n_days // 2])

            row = {
                "组合数量(K)": k,
                "组合策略清单": "  ➕  ".join(records[i]["label"] for i in ii),
                "重叠起": str(all_days[lo].date()), "重叠止": str(all_days[hi].date()),
                "重叠天数": n_days,
                "组合净利(M)": net, "年化净利(M/年)": risk["annual"],
                "组合总收益(M)": gp, "组合总亏损(M)": gl, "Profit Factor": pf,
                "已实现MDD(M)": risk["mdd"], "相对已实现MDD(%)": risk["relative_dd"],
                "最长水下期(天)": risk["underwater"], "已实现Calmar": risk["calmar"],
                "最差7日收益(M)": worst_7, "最差30日收益(M)": worst_30,
                "最差90日收益(M)": worst_90,
                "完整周期数": cycle_count, "周期盈利率(%)": cycle_win,
                "周期平均净利润(M)": cycle_mean,
                "7日盈利窗口率(%)": win_7, "30日盈利窗口率(%)": win_30,
                "1日盈利窗口率(%)": win_1,
                "四段净利分布": q_str, "最低阶段贡献(%)": q_min,
                "平均两两持仓重合(%)": mean_overlap, "最高两两持仓重合(%)": max_overlap,
                "分散化系数": div_dd, "成员均回撤(M)": mean_member_dd,
                "单策略最优Calmar": best_calmar, "单策略最优净利(M)": best_net,
                "1+1>2": "是" if risk["calmar"] > best_calmar else "否",
                "最长无盈利(天)": _max_true_run(daily <= 0),
                "最差单日(M)": float(daily.min()),
                "最差单月(M)": float(monthly_nonzero.min()) if len(monthly_nonzero) else 0.0,
                "盈利月占比(%)": float(np.mean(monthly_nonzero > 0) * 100.0)
                                  if len(monthly_nonzero) else 0.0,
                "后半段净利占比(%)": (net - half_net) / net * 100.0 if net != 0 else np.nan,
                "峰值合计浮亏(M)": float(float_sum.max()),
                "平均合计浮亏(M)": float(float_sum.mean()),
                "深水>0.5天数": int(np.sum(float_sum > 0.5)),
                "平均同时持仓数": float(conc.mean()), "最大同时持仓数": int(conc.max()),
                "资金利用率(%)": float(conc.mean() / k * 100.0),
                "平均日相关": float(np.mean(cvals)) if cvals else 0.0,
                "最大日相关": float(np.max(cvals)) if cvals else 0.0,
                "独立币种数": len(symbol_weights), "多空(L/S)": f"{n_long}/{k - n_long}",
                "最大单币权重(%)": float(max(symbol_weights.values()) * 100.0),
                "总开仓数": int(CNT[ii, sl].sum()),
                "_lo": lo, "_hi": hi, "_idx": ",".join(map(str, ii)),
            }
            row.update({f"Q{j + 1}利润贡献(%)": value for j, value in enumerate(q_ratios)})
            # 旧列名仅作兼容别名，数值全部使用新口径；控制台使用新名称。
            row.update({
                "组合持仓重合度": mean_overlap / 100.0,
                "盈亏比": pf, "组合回撤(M)": risk["mdd"],
                "相对回撤(%)": risk["relative_dd"], "水下最长(天)": risk["underwater"],
                "组合Calmar": risk["calmar"], "30日滚动胜率(%)": win_30,
                "7日滚动胜率(%)": win_7, "1日滚动胜率(%)": win_1,
                "爆仓周期胜率(%)": cycle_win, "最差单段贡献(%)": q_min,
            })
            results.append(row)

    print("\n过滤统计：" + " | ".join(f"{name} {count:,}" for name, count in skipped.items()))
    if not results:
        print("[提示] 没有组合通过当前过滤条件。")
        return

    df_all = pd.DataFrame(results)
    # 用未舍入的值排序；完全同分时保留穷举顺序，不引入隐藏的收益/Calmar排序。
    df_all["_scan_order"] = np.arange(len(df_all))
    df_all.sort_values(
        by=["周期盈利率(%)", "30日盈利窗口率(%)", "_scan_order"],
        ascending=[False, False, True], na_position="last", inplace=True,
    )
    df_all.drop(columns="_scan_order", inplace=True)
    df_all.reset_index(drop=True, inplace=True)
    output_parent = os.path.dirname(os.path.abspath(output_csv))
    os.makedirs(output_parent, exist_ok=True)
    df_all.drop(columns=["_lo", "_hi", "_idx"]).to_csv(
        output_csv, index=False, encoding="utf-8-sig", na_rep="N/A")
    print(f"组合评估完成：{len(df_all):,} 个有效组合 | 全量排名：{output_csv}\n")

    for k in range(min_k, max_k + 1):
        df_k = df_all[df_all["组合数量(K)"] == k].head(top_n_per_k)
        if df_k.empty:
            continue
        print("=" * 112)
        print(f"🏆 【{k} 策略顶级组合】TOP {len(df_k)}")
        print("   主排序：周期盈利率 ↓ → 30日盈利窗口率 ↓（N/A 排最后）")
        print("=" * 112)
        for rank, (_, r) in enumerate(df_k.iterrows(), 1):
            print(f"\nNo.{rank} | 完整周期 {int(r['完整周期数'])} 段 "
                  f"| 周期盈利率 {fmt(r['周期盈利率(%)'], suffix='%')}")
            print(f"   窗口 {r['重叠起']} ~ {r['重叠止']} | 共 {int(r['重叠天数'])} 天")
            print(f"   分散化系数 {fmt(r['分散化系数'], 3)} "
                  f"| 平均两两持仓重合 {fmt(r['平均两两持仓重合(%)'], suffix='%')} "
                  f"| 最高两两持仓重合 {fmt(r['最高两两持仓重合(%)'], suffix='%')}")
            print(f"   收益 | 净利润 {fmt(r['组合净利(M)'])} M "
                  f"| 年化净利润 {fmt(r['年化净利(M/年)'], 3)} M/年 "
                  f"| Profit Factor {fmt(r['Profit Factor'])}")
            print(f"   已实现风险 | 最大回撤 {fmt(r['已实现MDD(M)'])} M "
                  f"| 相对最大回撤 {fmt(r['相对已实现MDD(%)'], suffix='%')} "
                  f"| 最长水下期 {int(r['最长水下期(天)'])} 天 "
                  f"| 已实现 Calmar {fmt(r['已实现Calmar'])}")
            print(f"   滚动尾部 | 最差7日收益 {fmt(r['最差7日收益(M)'])} M "
                  f"| 最差30日收益 {fmt(r['最差30日收益(M)'])} M "
                  f"| 最差90日收益 {fmt(r['最差90日收益(M)'])} M")
            print(f"   时间稳定性 | 7日盈利窗口率 {fmt(r['7日盈利窗口率(%)'], suffix='%')} "
                  f"| 30日盈利窗口率 {fmt(r['30日盈利窗口率(%)'], suffix='%')}")
            print(f"   四段利润贡献 | {r['四段净利分布']} "
                  f"| 最低阶段贡献 {fmt(r['最低阶段贡献(%)'], 1, suffix='%')}")

            lo, hi = int(r["_lo"]), int(r["_hi"])
            ii = [int(value) for value in str(r["_idx"]).split(",")]
            rows = []
            for i in ii:
                m = member_risk(i, lo, hi)
                _, member_win, _ = cycle_stats([i], [1.0], window_blowups(i, lo, hi))
                rows.append({
                    "成员": records[i]["label"],
                    "窗口净利(M)": fmt(m["net"]),
                    "已实现MDD(M)": fmt(m["mdd"]),
                    "已实现Calmar": fmt(m["calmar"]),
                    "周期盈利率(%)": fmt(member_win),
                })
            print_table(pd.DataFrame(rows))
        print()
    return df_all

if __name__ == "__main__":
    PLATEAU_CSV = "strategy_leaderboard_100800_files_plateau_back.csv"  # 若无平原表填 None

    # Stage A: 抽取并归一化逐笔明细(只需在参数或缓存变化时跑一次)
    extract_target_trades_csv(
        cache_dir=CACHE_DIR,
        short_cache_dir=SHORT_CACHE_DIR,
        output_dir="./extracted_trades_csv",
        target_configs=TARGET_CONFIGS,
        plateau_csv=PLATEAU_CSV,
    )

    # Stage B: 穷举 K=2~5 组合并排名
    evaluate_multi_strategy_portfolios(
        csv_dir="./extracted_trades_csv",
        plateau_csv=PLATEAU_CSV,
        output_csv="portfolio_multi_ranking.csv",
        min_k=2,
        max_k=5,
        top_n_per_k=50,
        allow_same_signal=False,  # 想看"同信号不同 Margin"的叠加效果时改 True
        min_overlap_days=180,
        weight_mode="equal",  # 或 "recommend" 按你备注里的推荐次数加权
    )