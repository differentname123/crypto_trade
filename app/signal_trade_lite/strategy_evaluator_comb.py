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
CACHE_DIR = r"W:\backtest_data_1m_detail"        # 做多策略默认缓存目录
SHORT_CACHE_DIR = r"W:\backtest_data_1m_detail"  # 做空策略缓存目录

_TIME_LIKE_KEYS = ("time", "stamp", "epoch", "date", "millis", "nanos", "_ms", "_ns")

PNL_COL_CANDIDATES = ["net_pnl_in_margin", "pnl_in_margin", "net_pnl", "pnl", "profit", "net_profit"]
MDD_COL_CANDIDATES = ["max_drawdown", "max_drawdown_in_margin", "max_dd", "max_loss",
                      "max_loss_in_margin", "mdd", "max_floating_loss"]

BLOWUP_LOSS_THRESHOLD_M = 0.8   # 归一化后(M倍)单笔亏损超过该阈值视为爆仓(与原代码 -0.8*margin 等价)
DAYS_PER_YEAR = 365.0
INDEX_FILE = "_single_strategy_index.csv"   # Stage A 产出的元数据索引(Stage B 会读取, 且不会当成交易明细)

# ---------- 组合打分权重(可自由调参, 全部显式暴露) ----------
SCORE_CFG = {
    "w_annual": 8.0, "cap_annual": 35.0,          # 年化净利(M/年)
    "w_calmar": 4.0, "cap_calmar": 20.0,          # 年化 Calmar
    "w_gain": 4.0, "cap_gain_lo": -8.0, "cap_gain_hi": 12.0,   # 相对最优单策略的 Calmar 增益
    "corr_base": 0.25, "w_corr": 30.0, "cap_corr_lo": -12.0, "cap_corr_hi": 12.0,  # 最大两两相关
    "w_div_dd": 20.0, "cap_div_dd": 10.0,         # 回撤分散化收益
    "w_symbols": 2.0, "cap_symbols": 6.0,         # 币种分散
    "w_cushion": 0.15, "cap_cushion": 10.0,       # 平原短板安全垫
    "p_resonance": 15.0, "cap_resonance": 40.0,   # 共振同爆(重罚)
    "p_rel_dd": 0.4, "cap_rel_dd": 18.0,          # 相对回撤(%)
    "p_noprofit": 0.25, "cap_noprofit": 12.0,     # 最长无盈利天数
    "p_underwater": 0.04, "cap_underwater": 10.0, # 水下最长天数
    "float_base": 0.35, "p_peakfloat": 25.0, "cap_peakfloat": 15.0,  # 峰值合计浮亏
}

# =====================================================================
# 目标参数清单 (可加 "multiplier" 字段来精确锁定加仓倍数, 强烈建议加)
# =====================================================================
TARGET_CONFIGS = [
    {"symbol": "SOLUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.012,
     "margin": 6, "备注": "推荐的次数为 5；第二阶段赚分润"},

    {"symbol": "SOLUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.009,
     "margin": 6, "备注": "推荐的次数为 3；第二阶段赚分润"},
    {"symbol": "AAVEUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.006,
     "margin": 8, "备注": "推荐的次数为 3；第一阶段稳定做数据"},

    {"symbol": "SOLUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.011,
     "margin": 7, "备注": "推荐的次数为 2；第一阶段稳定做数据 1 次，第二阶段赚分润 1 次"},
    {"symbol": "SOLUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.009,
     "margin": 7, "备注": "推荐的次数为 2；第二阶段赚分润"},
    {"symbol": "SOLUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.012,
     "margin": 7, "备注": "推荐的次数为 2；第二阶段赚分润"},
    {"symbol": "AAVEUSDT", "strategy": "factor_044_9", "direction": "Long", "add_step": 0.020, "tp_step": 0.007,
     "margin": 9, "备注": "推荐的次数为 2；第一阶段稳定做数据"},

    {"symbol": "AAVEUSDT", "strategy": "factor_044_9", "direction": "Long", "add_step": 0.020, "tp_step": 0.006,
     "margin": 8, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "LINKUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.011,
     "margin": 7, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "AAVEUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
     "margin": 10, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "BNBUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.010,
     "margin": 8, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "SOLUSDT", "strategy": "factor_023_6", "direction": "Long", "add_step": 0.030, "tp_step": 0.008,
     "margin": 7, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "LINKUSDT", "strategy": "factor_024_6", "direction": "Long", "add_step": 0.030, "tp_step": 0.008,
     "margin": 9, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "AAVEUSDT", "strategy": "factor_044_1", "direction": "Long", "add_step": 0.030, "tp_step": 0.006,
     "margin": 6, "备注": "推荐的次数为 1；第二阶段赚分润"},
    {"symbol": "SOLUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.009,
     "margin": 8, "备注": "推荐的次数为 1；第二阶段赚分润"},
    {"symbol": "AAVEUSDT", "strategy": "factor_044_10", "direction": "Long", "add_step": 0.020, "tp_step": 0.008,
     "margin": 7, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "AAVEUSDT", "strategy": "factor_044_9", "direction": "Long", "add_step": 0.020, "tp_step": 0.007,
     "margin": 10, "备注": "推荐的次数为 1；第一阶段稳定做数据"},
    {"symbol": "AAVEUSDT", "strategy": "factor_007_1", "direction": "Long", "add_step": 0.020, "tp_step": 0.008,
     "margin": 8, "备注": "推荐的次数为 1；第一阶段稳定做数据"},

    # === 做空 Short ===
    {"symbol": "AAVEUSDT", "strategy": "factor_043_9", "direction": "Short", "add_step": 0.030, "tp_step": 0.007,
     "margin": 9},
    {"symbol": "AAVEUSDT", "strategy": "factor_043_10", "direction": "Short", "add_step": 0.015, "tp_step": 0.007,
     "margin": 7},
    {"symbol": "SOLUSDT", "strategy": "factor_043_9", "direction": "Short", "add_step": 0.030, "tp_step": 0.008,
     "margin": 9},
    {"symbol": "SOLUSDT", "strategy": "factor_043_9", "direction": "Short", "add_step": 0.025, "tp_step": 0.008,
     "margin": 7}
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
    print(sep); print(header); print(sep)
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
        ratio = report_net / net_pnl_sum   # <<< 关键: 单位换算靠 report 校准, 绝不靠"总和大小"猜

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
        data.clear(); del data
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


# =====================================================================
# Stage B : 组合回测
# =====================================================================
def _load_strategy_records(csv_dir, plateau_csv=None):
    files = sorted(glob.glob(os.path.join(csv_dir, "*.csv")))
    files = [f for f in files if not os.path.basename(f).startswith("_")]  # 排除索引/汇总表
    if not files:
        return [], None

    idx_path = os.path.join(csv_dir, INDEX_FILE)
    idx_df = pd.read_csv(idx_path) if os.path.exists(idx_path) else None
    plateau_df = pd.read_csv(plateau_csv) if (plateau_csv and os.path.exists(plateau_csv)) else None

    records = []
    for f in files:
        fname = os.path.basename(f)
        df = pd.read_csv(f)
        if df.empty:
            continue

        margin = float(df["margin"].iloc[0]) if "margin" in df.columns else 1.0

        # ---- 收益(必须是 M 倍) ----
        if "pnl_M" in df.columns:
            pnl = pd.to_numeric(df["pnl_M"], errors="coerce").fillna(0.0).astype(float)
        else:
            pcol = _pick_col(df, PNL_COL_CANDIDATES)
            if pcol is None:
                print(f"[警告] 无收益列, 跳过: {fname}")
                continue
            pnl = pd.to_numeric(df[pcol], errors="coerce").fillna(0.0).astype(float)
            if "in_margin" not in pcol:
                pnl = pnl / max(margin, 1e-9)
            print(f"[⚠] {fname} 缺少 pnl_M 列(旧版明细)，已按列名启发式换算，强烈建议重跑 Stage A！")

        # ---- 时间 (优先使用明确提供的 start_ms 和 end_ms 列) ----
        if "start_ms" in df.columns and "end_ms" in df.columns:
            open_dt = _to_dt(df["start_ms"])
            close_dt = _to_dt(df["end_ms"])
        elif "close_dt" in df.columns:
            close_dt = pd.to_datetime(df["close_dt"], errors="coerce")
            open_dt = pd.to_datetime(df["open_dt"], errors="coerce") if "open_dt" in df.columns else close_dt
        else:
            sc, ec = _detect_time_cols(df)
            close_dt = _to_dt(df[ec]);
            open_dt = _to_dt(df[sc]) if sc else close_dt
        open_dt = open_dt.fillna(close_dt)
        ok = close_dt.notna()
        df, pnl, open_dt, close_dt = df[ok].reset_index(drop=True), pnl[ok].reset_index(drop=True), \
            open_dt[ok].reset_index(drop=True), close_dt[ok].reset_index(drop=True)
        if df.empty:
            continue

        # ---- 爆仓 / 浮亏 ----
        if "is_blowup_flag" in df.columns:
            is_blow = df["is_blowup_flag"].astype(str).str.lower().isin(["true", "1", "1.0", "yes"]).values
        else:
            is_blow = (pnl.values <= -BLOWUP_LOSS_THRESHOLD_M)
        float_loss = pd.to_numeric(df.get("float_loss_M", pd.Series(0.0, index=df.index)),
                                   errors="coerce").fillna(0.0).abs().values

        sym = str(df["symbol"].iloc[0]) if "symbol" in df.columns else "UNK"
        strat = str(df["strategy"].iloc[0]) if "strategy" in df.columns else "UNK"
        direct = str(df["direction"].iloc[0]).capitalize() if "direction" in df.columns else "UNK"
        add_s = float(df["add_step"].iloc[0]) if "add_step" in df.columns else 0.0
        tp_s = float(df["tp_step"].iloc[0]) if "tp_step" in df.columns else 0.0
        mult = float(df["multiplier"].iloc[0]) if ("multiplier" in df.columns
                                                   and pd.notnull(df["multiplier"].iloc[0])) else None

        # ---- 元数据(平原安全垫等): 优先索引表, 否则精确匹配平原宽表 ----
        cushion = np.nan;
        med_surv = np.nan;
        weight = 1.0;
        note = ""
        if idx_df is not None and "file" in idx_df.columns and (idx_df["file"] == fname).any():
            r = idx_df[idx_df["file"] == fname].iloc[0]
            cushion = float(r.get("平原存活安全垫(天)", np.nan)) if pd.notnull(r.get("平原存活安全垫(天)")) else np.nan
            med_surv = float(r.get("中位存活(天)", np.nan)) if pd.notnull(r.get("中位存活(天)")) else np.nan
            weight = float(r.get("推荐权重", 1.0)) if pd.notnull(r.get("推荐权重")) else 1.0
            note = str(r.get("备注", "") or "")
        elif plateau_df is not None:
            pm = _match_plateau_row(plateau_df, sym, strat, direct, margin, add_s, tp_s, mult)
            cushion = pm["平原存活安全垫(天)"];
            med_surv = pm["中位存活(天)"]

        records.append({
            "file": fname,
            "label": _make_label(sym, strat, direct, margin, add_s, tp_s) + (f"_x{mult:g}" if mult else ""),
            "symbol": sym, "strategy": strat, "direction": direct, "margin": margin,
            "add_step": add_s, "tp_step": tp_s, "multiplier": mult,
            "signal_key": (sym, strat, direct, round(add_s, 6), round(tp_s, 6), mult),
            "pnl": pnl.values, "open_dt": open_dt.values, "close_dt": close_dt.values,
            "is_blow": np.asarray(is_blow, dtype=bool), "float_loss": float_loss,
            "cushion": cushion, "median_survival": med_surv,
            "weight": weight, "note": note,
            "has_float": bool(np.nanmax(float_loss) > 0) if len(float_loss) else False,
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
        max_combos=400000
):
    """
    :param allow_same_signal: 是否允许同一"信号源"(同币同策略同间距, 仅 Margin 不同)进入同一组合。
                              默认 False —— 否则相关性≈1 的伪分散会霸榜。
    :param min_overlap_days : 组合成员共同重叠交易窗口的最小天数, 不足则丢弃(样本不足无统计意义)
    :param weight_mode      : "equal"(默认, 资金均分) 或 "recommend"(按 备注/推荐次数 加权)
    """
    records, _ = _load_strategy_records(csv_dir, plateau_csv)
    if not records:
        print(f"[错误] 目录 {csv_dir} 下未找到任何有效交易明细 CSV！")
        return
    N = len(records)

    # ================= 说明 =================
    print("=" * 108)
    print(" 📖 组合回测口径说明")
    print("-" * 108)
    print(" [资金模型]   每个成员独立预留自己的 Margin 作为 1 个资金单位；组合总资金=K 单位；")
    print("              组合日收益 = Σ(w_i × 成员日收益)，w 默认 1/K。故所有 M 倍数值均为『每单位组合总资金』。")
    print(" [重叠窗口]   所有指标仅在成员共同存在数据的窗口 [max(起), min(止)] 内计算，绝不用 0 填充上线前的日子。")
    print(" [年化口径]   年化净利 = 窗口净利 / 窗口年数；Calmar = 年化净利 / 最大回撤(M)。绝对净利受时长影响不可直接比。")
    print(" [峰值合计浮亏] 各成员同一天持仓浮亏之和 × w 的最大值 —— 马丁组合真正的死因(同时深水)，>0.6 高危。")
    print(" [共振同爆]   任意两成员在 ±1 天内同时爆仓的次数(pair 计数) + 单日≥2 成员同爆天数。")
    print(" [相关性度量] 针对马丁策略已调整为【持仓交并比(IoU)】：同向重合越高相关性越接近1；反向重叠则视作对冲负相关。")
    print(" [分散化系数] 组合最大回撤 ÷ 成员平均最大回撤(同窗口)。<1 才是真互补；≥1 说明只是把风险摊平没抵消。")
    print("-" * 108)
    n_nofloat = sum(1 for r in records if not r["has_float"])
    # if n_nofloat:
    #     print(f" [⚠ 数据缺失] {n_nofloat}/{N} 个成员的明细里没有单笔最大浮亏列，"
    #           f"『峰值合计浮亏』会被低估，不要当成 0 风险！")
    print(f" 成员数={N} | K∈[{min_k},{max_k}] | 同信号源同组={'允许' if allow_same_signal else '禁止'} "
          f"| 最小重叠={min_overlap_days}天 | 权重={weight_mode}")
    print("=" * 108 + "\n")

    # ================= 构建日级矩阵 =================
    g_start = min(pd.Timestamp(r["open_dt"].min()) for r in records).normalize()
    g_end = max(pd.Timestamp(r["close_dt"].max()) for r in records).normalize()
    all_days = pd.date_range(g_start, g_end, freq="D")
    T = len(all_days)
    day_of_year = all_days.dayofyear.values
    month_key = (all_days.year.values * 12 + all_days.month.values - 1)

    PNL = np.zeros((N, T));
    POS = np.zeros((N, T));
    NEG = np.zeros((N, T))
    CNT = np.zeros((N, T));
    BLOW = np.zeros((N, T));
    FLOAT = np.zeros((N, T))
    HOLD = np.zeros((N, T), dtype=bool)
    first_i = np.zeros(N, dtype=np.int64);
    last_i = np.zeros(N, dtype=np.int64)

    for i, r in enumerate(records):
        o = pd.DatetimeIndex(r["open_dt"]).normalize()
        c = pd.DatetimeIndex(r["close_dt"]).normalize()
        si = ((o - g_start).days.values).astype(np.int64)
        ei = ((c - g_start).days.values).astype(np.int64)
        si = np.clip(si, 0, T - 1);
        ei = np.clip(ei, 0, T - 1)
        si = np.minimum(si, ei)
        p = r["pnl"]

        np.add.at(PNL[i], ei, p)
        np.add.at(POS[i], ei, np.where(p > 0, p, 0.0))
        np.add.at(NEG[i], ei, np.where(p < 0, p, 0.0))
        np.add.at(CNT[i], ei, 1.0)
        np.add.at(BLOW[i], ei, r["is_blow"].astype(float))

        # 持仓覆盖 & 浮亏覆盖(差分+前缀和, O(n) 向量化)
        d_hold = np.zeros(T + 1);
        np.add.at(d_hold, si, 1.0);
        np.add.at(d_hold, ei + 1, -1.0)
        HOLD[i] = np.cumsum(d_hold)[:T] > 0.5
        fl = r["float_loss"]
        if fl is not None and len(fl) == len(p) and np.nanmax(fl) > 0:
            d_f = np.zeros(T + 1);
            np.add.at(d_f, si, fl);
            np.add.at(d_f, ei + 1, -fl)
            FLOAT[i] = np.maximum(np.cumsum(d_f)[:T], 0.0)

        first_i[i] = int(si.min());
        last_i[i] = int(ei.max())

    # ================= 两两预计算(相关性 / 共振同爆) =================
    CORR = np.full((N, N), np.nan)
    COBLOW = np.zeros((N, N))
    blow_day = BLOW > 0
    blow_win = blow_day.copy()
    blow_win[:, 1:] |= blow_day[:, :-1]
    blow_win[:, :-1] |= blow_day[:, 1:]

    # 提取多空方向用于相关性方向惩罚判定
    dirs = [str(r.get("direction", "")).upper() for r in records]

    for i in range(N):
        for j in range(i + 1, N):
            lo = max(first_i[i], first_i[j]);
            hi = min(last_i[i], last_i[j])
            if hi - lo + 1 >= 30:
                # ------ 马丁专版相关性：改用持仓交并比(IoU) ------
                hold_i = HOLD[i, lo:hi + 1]
                hold_j = HOLD[j, lo:hi + 1]

                intersect = np.sum(hold_i & hold_j)
                union = np.sum(hold_i | hold_j)

                overlap_ratio = float(intersect) / float(union) if union > 0 else 0.0

                # 同方向: 重合度越高越差，计为正相关
                # 反方向: 重合度越高越好，互为对冲计为负相关
                if dirs[i] == dirs[j]:
                    CORR[i, j] = CORR[j, i] = overlap_ratio
                else:
                    CORR[i, j] = CORR[j, i] = -overlap_ratio

            cb = float(np.sum(blow_day[i] & blow_win[j]) + np.sum(blow_day[j] & blow_win[i])) / 2.0
            COBLOW[i, j] = COBLOW[j, i] = cb

    sig_keys = [r["signal_key"] for r in records]
    base_w = np.array([r["weight"] for r in records], dtype=float)

    total_combos = sum(math.comb(N, k) for k in range(min_k, max_k + 1))
    print(f"数据矩阵完成: {T} 天 | 待穷举组合上限 {total_combos:,} 个\n")
    if total_combos > max_combos:
        print(f"[警告] 组合数超过 max_combos={max_combos:,}，请缩小 max_k 或成员数。已中止。")
        return

    results = []
    skipped_same_signal = 0
    skipped_overlap = 0
    processed = 0

    for k in range(min_k, max_k + 1):
        for idxs in itertools.combinations(range(N), k):
            processed += 1
            if not allow_same_signal:
                if len({sig_keys[i][:5] for i in idxs}) < k:  # 忽略 Margin, 只看信号源
                    skipped_same_signal += 1
                    continue
            ii = list(idxs)
            lo = int(max(first_i[ii]));
            hi = int(min(last_i[ii]))
            n_days = hi - lo + 1
            if n_days < min_overlap_days:
                skipped_overlap += 1
                continue

            sl = slice(lo, hi + 1)
            if weight_mode == "recommend":
                w = base_w[ii] / base_w[ii].sum()
            else:
                w = np.full(k, 1.0 / k)
            w = w.reshape(-1, 1)

            daily = (PNL[ii, sl] * w).sum(axis=0)
            pos_d = (POS[ii, sl] * w).sum(axis=0)
            neg_d = (NEG[ii, sl] * w).sum(axis=0)
            cum = np.cumsum(daily)
            years = n_days / DAYS_PER_YEAR

            net = float(cum[-1])
            gp = float(pos_d.sum());
            gl = float(neg_d.sum())
            annual = net / years if years > 0 else 0.0

            peak = np.maximum.accumulate(cum)
            dd = peak - cum
            max_dd = float(dd.max())
            equity = 1.0 + cum
            eq_peak = np.maximum.accumulate(np.maximum(equity, 1.0))
            rel_dd = float(np.max((eq_peak - equity) / np.maximum(eq_peak, 1e-9)) * 100.0)
            underwater = _max_true_run(dd > 1e-12)
            calmar = (annual / max_dd) if max_dd > 1e-9 else 99.0
            busted = bool(equity.min() <= 0)

            sd = float(daily.std(ddof=0))
            sharpe = float(daily.mean() / sd * math.sqrt(DAYS_PER_YEAR)) if sd > 1e-12 else 0.0
            downs = daily[daily < 0]
            dsd = float(downs.std(ddof=0)) if len(downs) > 1 else 0.0
            sortino = float(daily.mean() / dsd * math.sqrt(DAYS_PER_YEAR)) if dsd > 1e-12 else 0.0

            win_day_ratio = float(np.mean(daily > 0) * 100.0)
            longest_np = _max_true_run(daily <= 0)
            worst_day = float(daily.min())

            mk = month_key[sl]
            msum = np.bincount(mk - mk[0], weights=daily)
            msum = msum[msum != 0] if len(msum) else msum
            worst_month = float(msum.min()) if len(msum) else 0.0
            win_month_ratio = float(np.mean(msum > 0) * 100.0) if len(msum) else 0.0

            half = n_days // 2
            first_half = float(cum[half - 1]) if half >= 1 else 0.0
            second_half = net - first_half
            second_ratio = (second_half / net * 100.0) if abs(net) > 1e-9 else 0.0

            n_blow = float(BLOW[ii, sl].sum())
            blow_per_year = n_blow / years if years > 0 else 0.0
            multi_blow_days = int(np.sum(blow_day[ii, sl].sum(axis=0) >= 2))
            max_same_day_blow = int(blow_day[ii, sl].sum(axis=0).max())
            resonance = float(sum(COBLOW[a, b] for a, b in itertools.combinations(ii, 2)))

            conc = HOLD[ii, sl].sum(axis=0)
            mean_conc = float(conc.mean());
            max_conc = int(conc.max())
            util = mean_conc / k * 100.0

            fsum = (FLOAT[ii, sl] * w).sum(axis=0)
            peak_float = float(fsum.max());
            mean_float = float(fsum.mean())
            deep_days = int(np.sum(fsum > 0.5))

            cvals = [CORR[a, b] for a, b in itertools.combinations(ii, 2)]
            cvals = [c for c in cvals if np.isfinite(c)]
            mean_corr = float(np.mean(cvals)) if cvals else 0.0
            max_corr = float(np.max(cvals)) if cvals else 0.0

            # 成员在同一窗口内的独立表现(可比!)
            m_net, m_dd, m_cal = [], [], []
            for i in ii:
                c_i = np.cumsum(PNL[i, sl])
                d_i = float((np.maximum.accumulate(c_i) - c_i).max())
                a_i = float(c_i[-1]) / years if years > 0 else 0.0
                m_net.append(float(c_i[-1]));
                m_dd.append(d_i)
                m_cal.append(a_i / d_i if d_i > 1e-9 else 99.0)
            best_single_calmar = float(max(m_cal));
            best_single_net = float(max(m_net))
            mean_member_dd = float(np.mean(m_dd))
            div_dd = (max_dd / mean_member_dd) if mean_member_dd > 1e-9 else 1.0

            cushions = [records[i]["cushion"] for i in ii]
            min_cushion = float(np.nanmin(cushions)) if np.any(np.isfinite(cushions)) else np.nan
            msurv = [records[i]["median_survival"] for i in ii]
            min_msurv = float(np.nanmin(msurv)) if np.any(np.isfinite(msurv)) else np.nan

            syms = {records[i]["symbol"] for i in ii}
            n_long = sum(1 for i in ii if records[i]["direction"] == "Long")
            sym_cnt = {}
            for i in ii:
                sym_cnt[records[i]["symbol"]] = sym_cnt.get(records[i]["symbol"], 0) + 1
            max_sym_w = max(sym_cnt.values()) / k * 100.0
            trades = float(CNT[ii, sl].sum())

            # ---------------- 打分 ----------------
            c = SCORE_CFG
            score = 0.0
            score += float(np.clip(annual * c["w_annual"], 0, c["cap_annual"]))
            score += float(np.clip(calmar * c["w_calmar"], 0, c["cap_calmar"]))
            score += float(np.clip((calmar - best_single_calmar) * c["w_gain"], c["cap_gain_lo"], c["cap_gain_hi"]))
            score += float(np.clip((c["corr_base"] - max_corr) * c["w_corr"], c["cap_corr_lo"], c["cap_corr_hi"]))
            score += float(np.clip((1.0 - div_dd) * c["w_div_dd"], 0, c["cap_div_dd"]))
            score += float(np.clip(len(syms) * c["w_symbols"], 0, c["cap_symbols"]))
            if np.isfinite(min_cushion):
                score += float(np.clip(min_cushion * c["w_cushion"], 0, c["cap_cushion"]))
            score -= float(np.clip(resonance * c["p_resonance"], 0, c["cap_resonance"]))
            score -= float(np.clip(rel_dd * c["p_rel_dd"], 0, c["cap_rel_dd"]))
            score -= float(np.clip(longest_np * c["p_noprofit"], 0, c["cap_noprofit"]))
            score -= float(np.clip(underwater * c["p_underwater"], 0, c["cap_underwater"]))
            score -= float(np.clip(max(0.0, peak_float - c["float_base"]) * c["p_peakfloat"], 0, c["cap_peakfloat"]))
            if busted:
                score = 0.0
            score = round(float(np.clip(score, 0, 100)), 2)

            results.append({
                "组合数量(K)": k,
                "组合策略清单": "  ➕  ".join(records[i]["label"] for i in ii),
                "综合得分": score,
                "1+1>2": "🔥 是" if calmar > best_single_calmar else "否",
                "曾归零": "💀 是" if busted else "否",
                "重叠起": str(all_days[lo].date()), "重叠止": str(all_days[hi].date()),
                "重叠天数": n_days,
                "组合净利(M)": round(net, 2),
                "年化净利(M/年)": round(annual, 3),
                "组合总收益(M)": round(gp, 2),
                "组合总亏损(M)": round(gl, 2),
                "盈亏比": round(abs(gp / gl), 2) if abs(gl) > 1e-9 else 99.0,
                "组合回撤(M)": round(max_dd, 2),
                "相对回撤(%)": round(rel_dd, 2),
                "水下最长(天)": underwater,
                "组合Calmar": round(calmar, 2),
                "单策略最优Calmar": round(best_single_calmar, 2),
                "单策略最优净利(M)": round(best_single_net, 2),
                "成员均回撤(M)": round(mean_member_dd, 2),
                "分散化系数": round(div_dd, 3),
                "夏普(年化)": round(sharpe, 2),
                "Sortino": round(sortino, 2),
                "盈利天占比(%)": round(win_day_ratio, 2),
                "最长无盈利(天)": longest_np,
                "最差单日(M)": round(worst_day, 3),
                "最差单月(M)": round(worst_month, 2),
                "盈利月占比(%)": round(win_month_ratio, 2),
                "后半段净利占比(%)": round(second_ratio, 1),
                "爆仓总次数": int(n_blow),
                "年化爆仓次数": round(blow_per_year, 2),
                "共振同爆(pair)": round(resonance, 1),
                "单日≥2同爆(天)": multi_blow_days,
                "单日最多同爆": max_same_day_blow,
                "峰值合计浮亏(M)": round(peak_float, 3),
                "平均合计浮亏(M)": round(mean_float, 3),
                "深水>0.5天数": deep_days,
                "平均同时持仓数": round(mean_conc, 2),
                "最大同时持仓数": max_conc,
                "资金利用率(%)": round(util, 1),
                "平均日相关": round(mean_corr, 3),
                "最大日相关": round(max_corr, 3),
                "独立币种数": len(syms),
                "多空(L/S)": f"{n_long}/{k - n_long}",
                "最大单币权重(%)": round(max_sym_w, 1),
                "平原短板安全垫(天)": round(min_cushion, 1) if np.isfinite(min_cushion) else np.nan,
                "短板中位存活(天)": round(min_msurv, 1) if np.isfinite(min_msurv) else np.nan,
                "总开仓数": int(trades),
                "_lo": lo, "_hi": hi, "_idx": ",".join(map(str, ii)),
            })

            if processed % 20000 == 0:
                print(f"   ...已扫描 {processed:,}/{total_combos:,} 个组合")

    if not results:
        print("[提示] 没有任何组合通过 同信号源/重叠窗口 过滤，请放宽 min_overlap_days 或 allow_same_signal。")
        return

    df_all = pd.DataFrame(results)
    df_all.sort_values(by=["综合得分", "组合Calmar", "年化净利(M/年)"],
                       ascending=[False, False, False], inplace=True)
    df_all.drop(columns=["_lo", "_hi", "_idx"]).to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"\n🎉 组合评估完成: 有效 {len(df_all):,} 个 "
          f"(同信号源剔除 {skipped_same_signal:,} / 重叠不足剔除 {skipped_overlap:,})")
    print(f"📄 全量排名已保存: {output_csv}\n")

    # ================= 分 K 打印 =================
    for k in range(min_k, max_k + 1):
        df_k = df_all[df_all["组合数量(K)"] == k].head(top_n_per_k)
        if df_k.empty:
            continue
        print("=" * 128)
        print(f" 🏆 【{k} 个策略组合】最佳互补排行榜 TOP {len(df_k)}   (按综合得分)")
        print("=" * 128)
        for rank, (_, r) in enumerate(df_k.iterrows(), 1):
            print(f"🥇 No.{rank} [得分 {r['综合得分']}] | 1+1>2: {r['1+1>2']} | 曾归零: {r['曾归零']} "
                  f"| 窗口 {r['重叠起']} ~ {r['重叠止']} ({r['重叠天数']}天)")
            print(f"   🧩 {r['组合策略清单']}")
            print(f"   💰 收益 -> 净利 {r['组合净利(M)']}M | 年化 {r['年化净利(M/年)']}M/年 | "
                  f"总收益 {r['组合总收益(M)']}M | 总亏损 {r['组合总亏损(M)']}M | 盈亏比 {r['盈亏比']}")
            print(f"   📉 风险 -> 最大回撤 {r['组合回撤(M)']}M ({r['相对回撤(%)']}%) | 水下最长 {r['水下最长(天)']}天 | "
                  f"Calmar {r['组合Calmar']}(单最优 {r['单策略最优Calmar']}) | 分散化系数 {r['分散化系数']} | "
                  f"夏普 {r['夏普(年化)']}")
            print(f"   💀 爆仓 -> 总 {r['爆仓总次数']}次 / 年化 {r['年化爆仓次数']} | 共振同爆 {r['共振同爆(pair)']} | "
                  f"单日≥2同爆 {r['单日≥2同爆(天)']}天 | 峰值合计浮亏 {r['峰值合计浮亏(M)']}M | 深水>0.5 {r['深水>0.5天数']}天")
            print(f"   🔗 结构 -> 最大相关 {r['最大日相关']} / 平均 {r['平均日相关']} | 币种 {r['独立币种数']}个 | "
                  f"多空 {r['多空(L/S)']} | 最大单币权重 {r['最大单币权重(%)']}% | "
                  f"同时持仓 均{r['平均同时持仓数']}/最大{r['最大同时持仓数']} (占用 {r['资金利用率(%)']}%)")
            print(f"   🧘 体验 -> 盈利天 {r['盈利天占比(%)']}% | 最长无盈利 {r['最长无盈利(天)']}天 | "
                  f"最差单日 {r['最差单日(M)']}M | 最差单月 {r['最差单月(M)']}M | 盈利月 {r['盈利月占比(%)']}% | "
                  f"后半段贡献 {r['后半段净利占比(%)']}% | 短板安全垫 {r['平原短板安全垫(天)']}天")

            # 成员在同一窗口内的可比明细
            lo, hi = int(r["_lo"]), int(r["_hi"])
            ii = [int(x) for x in str(r["_idx"]).split(",")]
            rows = []
            yrs = (hi - lo + 1) / DAYS_PER_YEAR
            for i in ii:
                c_i = np.cumsum(PNL[i, lo:hi + 1])
                d_i = float((np.maximum.accumulate(c_i) - c_i).max())
                rows.append({
                    "成员": records[i]["label"],
                    "窗口净利(M)": round(float(c_i[-1]), 2),
                    "窗口回撤(M)": round(d_i, 2),
                    "窗口Calmar": round((float(c_i[-1]) / yrs / d_i) if d_i > 1e-9 else 99.0, 2),
                    "爆仓": int(BLOW[i, lo:hi + 1].sum()),
                    "开仓": int(CNT[i, lo:hi + 1].sum()),
                    "持仓占比(%)": round(float(HOLD[i, lo:hi + 1].mean() * 100), 1),
                    # "峰值浮亏(M)": round(float(FLOAT[i, lo:hi + 1].max()), 3),
                    "安全垫(天)": records[i]["cushion"],
                })
            print_table(pd.DataFrame(rows))
            print("-" * 128)
        print("\n")

    return df_all


if __name__ == "__main__":
    PLATEAU_CSV = "strategy_leaderboard_57600_files_plateau.csv"   # 若无平原表填 None

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
        top_n_per_k=5,
        allow_same_signal=False,   # 想看"同信号不同 Margin"的叠加效果时改 True
        min_overlap_days=180,
        weight_mode="equal",       # 或 "recommend" 按你备注里的推荐次数加权
    )