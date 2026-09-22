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
    print(sep)
    print(header)
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


def _worker_process_cache_file(fpath, groups, plateau_df, output_dir):
    """
    多进程 Worker 函数：处理单个缓存文件，独立完成反序列化、回放与归一化导出。
    提取到顶层以满足 ProcessPoolExecutor 的序列化(Pickle)要求。
    """
    import os
    import gc
    import math
    import numpy as np

    local_summaries = []
    local_done = set()
    local_mult_seen = {}
    logs = []

    fname = os.path.basename(fpath)
    sym, strat, direct = _parse_filename(fname)
    direct = str(direct).capitalize()

    try:
        data = _load_pickle(fpath)
    except Exception as e:
        logs.append(f"[警告] 读取失败已跳过: {fname} | {e}")
        return local_summaries, local_done, local_mult_seen, logs

    attrs = data.get("attrs", {}) or {}
    add_s = round(float(attrs.get("add_step", -1)), 6)
    tp_s = round(float(attrs.get("tp_step", -1)), 6)
    mult = attrs.get("multiplier", None)
    mult = float(mult) if mult is not None else None
    key = (sym, strat, direct, add_s, tp_s)

    if key not in groups:
        del data
        gc.collect()
        return local_summaries, local_done, local_mult_seen, logs

    g = groups[key]
    if g["multiplier"] is not None and mult is not None and not math.isclose(g["multiplier"], mult, abs_tol=1e-6):
        del data
        gc.collect()
        return local_summaries, local_done, local_mult_seen, logs

    local_mult_seen[key] = {mult}

    cycles_df = data.pop("df")
    cycles_df.attrs = attrs
    data.clear()
    del data
    gc.collect()

    if cycles_df is None or len(cycles_df) == 0:
        logs.append(f"[警告] cycles 为空: {fname}")
        del cycles_df
        gc.collect()
        return local_summaries, local_done, local_mult_seen, logs

    replayer = TimelineReplayer(cycles_df)
    for margin in sorted(g["margins"].keys()):
        meta = g["margins"][margin]
        try:
            trades_df = replayer.run(margin)
            norm_df, summ = _normalize_trades_df(trades_df, cycles_df, margin)
        except Exception as e:
            logs.append(f"[警告] 回放/归一化失败: {sym}|{strat}|{direct}|M{margin} | {type(e).__name__}: {e}")
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

        local_summaries.append(row)
        local_done.add((key, margin))
        logs.append(f"✅ {out_filename} | 笔数={summ['实际开仓数']} "
                    f"净利={summ['净利润(M倍)']}M 爆仓={summ['爆仓次数']} ratio={summ['pnl换算ratio']}")

        del trades_df, norm_df
        gc.collect()

    del cycles_df, replayer
    gc.collect()

    return local_summaries, local_done, local_mult_seen, logs


def extract_target_trades_csv(cache_dir=CACHE_DIR,
                              short_cache_dir=SHORT_CACHE_DIR,
                              output_dir="./extracted_trades_csv",
                              target_configs=None,
                              plateau_csv=None,
                              target_csv_list=None):
    """
    针对给定的精选参数组合，独立回放并导出对应的 trades_df 逐笔交易记录 CSV。
    修复要点:
      * 同一(币/策略/方向/加仓/止盈) 只加载 pkl 一次, 内循环跑多个 Margin (IO 降 N 倍)
      * 收益统一换算为 M 倍并落盘 pnl_M (禁止后续阶段再猜单位)
      * 记录/校验 multiplier(加仓倍数), 写进文件名, 避免张冠李戴与互相覆盖
      * 附带导出 _single_strategy_index.csv (含平原精确匹配指标 + 推荐权重)
      * [新增] 支持直接从给定的 CSV 列表加载策略组合，替代 TARGET_CONFIGS
      * [新增] 引入并发度为 10 的多进程处理，极速榨干 CPU
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # 默认加载的 CSV 列表
    if target_csv_list is None:
        target_csv_list = [
            r"W:\project\python_project\crypto_trade\app\signal_trade_lite\good_longs.csv",
            r"W:\project\python_project\crypto_trade\app\signal_trade_lite\good_shorts.csv"
        ]

    # ---- 0. 解析 CSV 参数文件列表 ----
    if target_csv_list:
        loaded_configs = []
        for csv_file in target_csv_list:
            if os.path.exists(csv_file):
                try:
                    df_csv = pd.read_csv(csv_file)
                    count = 0
                    for _, row in df_csv.iterrows():
                        if pd.isna(row.get("币种")):
                            continue
                        cfg = {
                            "symbol": str(row["币种"]),
                            "strategy": str(row["策略"]),
                            "direction": str(row["方向"]),
                            "add_step": float(row["加仓间距"]),
                            "tp_step": float(row["止盈间距"]),
                            "margin": float(row["Margin"]),
                        }
                        if "加仓倍数" in row and pd.notna(row["加仓倍数"]):
                            cfg["multiplier"] = float(row["加仓倍数"])
                        if "备注" in row and pd.notna(row["备注"]):
                            cfg["备注"] = str(row["备注"])

                        loaded_configs.append(cfg)
                        count += 1
                    print(f"📄 从文件 [{os.path.basename(csv_file)}] 成功加载了 {count} 个参数组合。")
                except Exception as e:
                    print(f"❌ 读取参数文件 {csv_file} 失败: {e}")
            else:
                print(f"⚠️ 找不到参数文件 (已跳过): {csv_file}")

        if loaded_configs:
            target_configs = loaded_configs
            print(f"✅ 共从 CSV 列表加载了 {len(target_configs)} 个目标配置 (已覆盖默认 TARGET_CONFIGS)。\n")
        else:
            print("⚠️ CSV列表未加载到有效数据，将回退使用默认的 TARGET_CONFIGS。\n")
            target_configs = target_configs or TARGET_CONFIGS
    else:
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
    print(f"📂 缓存文件总数 {len(files)}，按币种/策略/方向粗筛后候选 {len(candidates)} 个，准备并行处理...\n")

    # ---- 3. 并行加载文件, 内循环多 Margin ----
    done = set()
    summaries = []
    mult_seen = {}

    print(f"🚀 启动多进程并发处理 (并发度 10) ...")

    with ProcessPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_worker_process_cache_file, fpath, groups, plateau_df, output_dir): fpath
            for fpath in candidates
        }

        for future in as_completed(futures):
            fpath = futures[future]
            try:
                local_summaries, local_done, local_mult_seen, logs = future.result()
            except Exception as e:
                print(f"❌ 进程抛出未捕获异常: {os.path.basename(fpath)} | {e}")
                continue

            # 聚合结果
            summaries.extend(local_summaries)

            for k, v_set in local_mult_seen.items():
                mult_seen.setdefault(k, set()).update(v_set)
                if groups[k]["multiplier"] is None and len(mult_seen[k]) > 1:
                    print(f"[⚠ 重要] {k} 在缓存中存在多个加仓倍数 {sorted(x for x in mult_seen[k] if x is not None)}；"
                          f"已按倍数分别导出，请在 TARGET_CONFIGS 中显式补 'multiplier' 以锁定榜单那一行！")

            # 为了平滑打印日志进度，我们先更新 done 集合，然后再打印 log
            done.update(local_done)
            for log in logs:
                if log.startswith("✅"):
                    print(f"[{len(done)}/{n_targets}] {log}")
                else:
                    print(log)

    # ---- 4. 汇总索引 ----
    if summaries:
        idx_df = pd.DataFrame(summaries).sort_values(["币种", "策略", "方向", "Margin"])
        idx_df.to_csv(os.path.join(output_dir, INDEX_FILE), index=False, encoding="utf-8-sig")

    missing = [(k, m) for k, g in groups.items() for m in g["margins"] if (k, m) not in done]
    print("\n" + "=" * 70)
    print(f"🎉 并行导出完成: {len(done)}/{n_targets} 个，目录: {output_dir}")
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
        max_combos=None,
        filter_q_balance=10.0,
        filter_roll_profit_win_rate_30=None,
        filter_roll_profit_win_rate_7=None,
        filter_roll_profit_win_rate_1=None,
        search_mode="prune",  # [修改点] 默认开启剪枝搜索
        beam_width=1000,
        prune_tolerance=0.8,  # [新增] 下限容忍系数
        prune_metric="calmar",  # [新增] 层级剪枝的核心评估指标：'calmar' 或 'pf'(盈亏比)
):
    """组合回测：加入了基于下限容忍的层级剪枝策略，以及精确搜索/近视搜索的支持。"""
    from functools import lru_cache
    import heapq
    count_bits = getattr(int, "bit_count", lambda value: bin(value).count("1"))

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
            positions = np.searchsorted(times, boundaries, side="right")
            profits += weight * np.diff(prefix[positions])
        return count, float(np.mean(profits > 0) * 100.0), float(profits.mean())

    if weight_mode not in ("equal", "recommend"):
        raise ValueError("weight_mode 只能为 'equal' 或 'recommend'")
    if search_mode not in ("exact", "beam", "prune"):
        raise ValueError("search_mode 只能为 'exact', 'beam' 或 'prune'")
    for name, value, minimum in (
            ("min_k", min_k, 1), ("max_k", max_k, 1),
            ("top_n_per_k", top_n_per_k, 0),
            ("min_overlap_days", min_overlap_days, 1),
            ("beam_width", beam_width, 1)):
        if isinstance(value, (bool, np.bool_)) or not isinstance(value, (int, np.integer)) or value < minimum:
            raise ValueError(f"{name} 必须为不小于 {minimum} 的整数")
    if max_combos is not None and (
            isinstance(max_combos, (bool, np.bool_)) or
            not isinstance(max_combos, (int, np.integer)) or max_combos < 1):
        raise ValueError("max_combos 必须为正整数或 None")
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
    base_w = np.array([r["weight"] for r in records], dtype=float)
    if weight_mode == "recommend" and (not np.isfinite(base_w).all() or np.any(base_w <= 0)):
        raise ValueError("recommend 模式要求所有推荐权重均为有限正数")

    required_days = max(
        [min_overlap_days] +
        [days for days, threshold in filters.items() if threshold is not None] +
        ([4] if filter_q_balance is not None else [])
    )
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
    budget_text = "不限" if max_combos is None else f"{max_combos:,}"
    print(f"搜索模式 {search_mode} | 理论组合 {total_combos:,} | 实际评估预算 {budget_text}")
    if search_mode == "beam":
        print(f"近似搜索：每层保留至多 {beam_width:,} 个可扩展种子，可能遗漏优质组合。")
    elif search_mode == "prune":
        print(f"层级剪枝搜索：每一层的高维组合，基于核心指标({prune_metric})必须 ≥ 0.8×[其最差直系父组合指标]")
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
            d_float = np.zeros(T + 1)
            np.add.at(d_float, si, fl)
            np.add.at(d_float, ei + 1, -fl)
            FLOAT[i] = np.maximum(np.cumsum(d_float)[:T], 0.0)

        first_i[i], last_i[i] = int(si.min()), int(ei.max())
        order = np.argsort(closes.asi8, kind="stable")
        times = closes.asi8[order]
        prefix = np.r_[0.0, np.cumsum(p[order], dtype=float)]
        flags = np.asarray(r["is_blowup"], dtype=bool)[order]
        event_data.append((times, prefix, np.unique(times[flags])))

    sig_keys = [r["signal_key"][:5] for r in records]
    active = [i for i in range(N) if last_i[i] - first_i[i] + 1 >= required_days]
    active_mask = sum(1 << i for i in active)
    compatible = [0] * N
    blocked_same = blocked_window = 0
    for i, j in itertools.combinations(active, 2):
        if not allow_same_signal and sig_keys[i] == sig_keys[j]:
            blocked_same += 1
            continue
        lo = max(int(first_i[i]), int(first_i[j]))
        hi = min(int(last_i[i]), int(last_i[j]))
        if hi - lo + 1 < required_days:
            blocked_window += 1
            continue
        compatible[i] |= 1 << j
        compatible[j] |= 1 << i

    print(f"数据矩阵：{T} 天 | 有效最短共同窗口 {required_days} 天")
    print(f"结构预筛：窗口过短成员 {N - len(active):,} | "
          f"同源禁配对 {blocked_same:,} | 窗口禁配对 {blocked_window:,}")
    if len(active) < min_k:
        print("[提示] 满足必要窗口长度的成员不足，无法构建组合。")
        return
    if not allow_same_signal and len({sig_keys[i] for i in active}) < min_k:
        print("[提示] 不同信号源数量不足，无法构建组合。")
        return

    @lru_cache(maxsize=20000)
    def member_risk(i, lo, hi):
        return realized_risk(PNL[i, lo:hi + 1])

    @lru_cache(maxsize=20000)
    def pair_overlap(a, b, lo, hi):
        hold_a, hold_b = HOLD[a, lo:hi + 1], HOLD[b, lo:hi + 1]
        union = int(np.count_nonzero(hold_a | hold_b))
        return float(np.count_nonzero(hold_a & hold_b) / union) if union else 0.0

    CORR = np.full((N, N), np.nan)
    for i, j in itertools.combinations(active, 2):
        if not (compatible[i] & (1 << j)):
            continue
        lo, hi = max(int(first_i[i]), int(first_i[j])), min(int(last_i[i]), int(last_i[j]))
        if hi - lo + 1 >= 30:
            sign = 1.0 if records[i]["direction"] == records[j]["direction"] else -1.0
            CORR[i, j] = CORR[j, i] = pair_overlap(i, j, lo, hi) * sign

    results = []
    skipped = {"盈利窗口率": 0, "四段贡献": 0}
    processed = 0
    evaluated_by_k = {}
    insufficient_branches = 0
    beam_discarded = 0

    def exact_candidates(target_k):
        def visit(chosen, candidates, lo, hi):
            nonlocal insufficient_branches
            need = target_k - len(chosen)
            if count_bits(candidates) < need:
                insufficient_branches += 1
                return
            while candidates:
                if count_bits(candidates) < need:
                    insufficient_branches += 1
                    break
                bit = candidates & -candidates
                candidates ^= bit
                i = bit.bit_length() - 1
                child = chosen + (i,)
                child_lo = max(lo, int(first_i[i]))
                child_hi = min(hi, int(last_i[i]))
                if need == 1:
                    yield child, child_lo, child_hi
                else:
                    yield from visit(child, candidates & compatible[i], child_lo, child_hi)

        yield from visit((), active_mask, 0, T - 1)

    def evaluate_core(idxs, lo, hi, need_rank):
        nonlocal processed
        if max_combos is not None and processed >= max_combos:
            raise RuntimeError(
                f"实际评估数达到 max_combos={max_combos:,}，搜索尚未完成；"
                "本次结果未写入 CSV。可设 max_combos=None 取消预算，"
                "或显式使用 search_mode='beam' 并调小 beam_width。"
            )
        processed += 1
        k = len(idxs)
        evaluated_by_k[k] = evaluated_by_k.get(k, 0) + 1
        if processed % 20000 == 0:
            print(f"   ...已评估 {processed:,} 个候选 | 当前 K={k}")
        ii = list(idxs)
        n_days = hi - lo + 1
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
        roll_failed = any(
            threshold is not None and
            (not np.isfinite(rates[days]) or rates[days] < threshold)
            for days, threshold in filters.items()
        )

        if net > 0 and n_days >= 4:
            q_ratios = [float(chunk.sum() / net * 100.0)
                        for chunk in np.array_split(daily, 4)]
            q_min = min(q_ratios)
            q_str = " | ".join(f"Q{i + 1} {value:.1f}%" for i, value in enumerate(q_ratios))
        else:
            q_ratios = [float("nan")] * 4
            q_min = float("nan")
            q_str = "N/A（总净利≤0或窗口不足4天）"
        q_failed = filter_q_balance is not None and (not np.isfinite(q_min) or q_min < filter_q_balance)
        passed = not roll_failed and not q_failed
        if k >= min_k:
            if roll_failed:
                skipped["盈利窗口率"] += 1
            elif q_failed:
                skipped["四段贡献"] += 1
        if not passed and not need_rank:
            return None

        boundaries = np.unique(np.concatenate([window_blowups(i, lo, hi) for i in ii]))
        cycle_count, cycle_win, cycle_mean = cycle_stats(ii, w, boundaries)
        return {
            "idxs": idxs, "ii": ii, "k": k, "lo": lo, "hi": hi,
            "n_days": n_days, "sl": sl, "w": w, "daily": daily, "prefix": prefix,
            "net": net, "win_7": win_7, "worst_7": worst_7,
            "win_30": win_30, "worst_30": worst_30, "win_1": win_1,
            "q_ratios": q_ratios, "q_min": q_min, "q_str": q_str,
            "cycle_count": cycle_count, "cycle_win": cycle_win, "cycle_mean": cycle_mean,
            "passed": passed,
        }

    def make_row(core):
        k, ii, lo, hi = core["k"], core["ii"], core["lo"], core["hi"]
        n_days, sl, w = core["n_days"], core["sl"], core["w"]
        daily, prefix, net = core["daily"], core["prefix"], core["net"]
        win_7, worst_7 = core["win_7"], core["worst_7"]
        win_30, worst_30, win_1 = core["win_30"], core["worst_30"], core["win_1"]
        q_ratios, q_min, q_str = core["q_ratios"], core["q_min"], core["q_str"]
        cycle_count, cycle_win, cycle_mean = core["cycle_count"], core["cycle_win"], core["cycle_mean"]
        _, worst_90 = rolling_stats(prefix, 90)
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

        pair_overlaps = [pair_overlap(a, b, lo, hi) for a, b in itertools.combinations(ii, 2)]
        mean_overlap = float(np.mean(pair_overlaps) * 100.0) if pair_overlaps else float("nan")
        max_overlap = float(np.max(pair_overlaps) * 100.0) if pair_overlaps else float("nan")

        mk = month_key[sl]
        monthly = np.bincount(mk - mk[0], weights=daily)
        monthly_nonzero = monthly[monthly != 0]
        conc = HOLD[ii, sl].sum(axis=0)
        float_sum = (FLOAT[ii, sl] * w[:, None]).sum(axis=0)
        cvals = [CORR[a, b] for a, b in itertools.combinations(ii, 2) if np.isfinite(CORR[a, b])]
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
            "盈利月占比(%)": float(np.mean(monthly_nonzero > 0) * 100.0) if len(monthly_nonzero) else 0.0,
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
        row.update({
            "组合持仓重合度": mean_overlap / 100.0,
            "盈亏比": pf, "组合回撤(M)": risk["mdd"],
            "相对回撤(%)": risk["relative_dd"], "水下最长(天)": risk["underwater"],
            "组合Calmar": risk["calmar"], "30日滚动胜率(%)": win_30,
            "7日滚动胜率(%)": win_7, "1日滚动胜率(%)": win_1,
            "爆仓周期胜率(%)": cycle_win, "最差单段贡献(%)": q_min,
        })
        return row

    if search_mode == "exact":
        for k in range(min_k, max_k + 1):
            before = processed
            for idxs, lo, hi in exact_candidates(k):
                core = evaluate_core(idxs, lo, hi, need_rank=False)
                if core is not None:
                    results.append(make_row(core))
            print(f"K={k}：评估 {processed - before:,} 个结构可行候选")

    elif search_mode == "beam":
        frontier = [((), 0, T - 1, active_mask)]
        for k in range(1, max_k + 1):
            seen = set()
            next_heap = []
            expandable_count = 0
            before = processed
            for chosen, parent_lo, parent_hi, parent_mask in frontier:
                candidates = parent_mask
                while candidates:
                    bit = candidates & -candidates
                    candidates ^= bit
                    i = bit.bit_length() - 1
                    idxs = tuple(sorted(chosen + (i,)))
                    if idxs in seen:
                        continue
                    seen.add(idxs)
                    lo = max(parent_lo, int(first_i[i]))
                    hi = min(parent_hi, int(last_i[i]))
                    child_mask = parent_mask & compatible[i]
                    if k < min_k and count_bits(child_mask) < min_k - k:
                        insufficient_branches += 1
                        continue
                    can_expand = k < max_k and bool(child_mask)
                    core = evaluate_core(idxs, lo, hi, need_rank=can_expand)
                    if core is not None and core["passed"] and k >= min_k:
                        results.append(make_row(core))
                    if not can_expand:
                        continue

                    cycle_score = core["cycle_win"] if np.isfinite(core["cycle_win"]) else -1.0
                    roll_score = core["win_30"] if np.isfinite(core["win_30"]) else -1.0
                    priority = (int(core["passed"]), cycle_score, roll_score, tuple(-j for j in idxs))
                    item = (priority, (idxs, lo, hi, child_mask))
                    expandable_count += 1
                    if len(next_heap) < beam_width:
                        heapq.heappush(next_heap, item)
                    elif priority > next_heap[0][0]:
                        heapq.heapreplace(next_heap, item)

            discarded = expandable_count - len(next_heap)
            beam_discarded += discarded
            print(
                f"K={k}：评估 {processed - before:,} 个候选 | 下层种子 {len(next_heap):,} | 近似淘汰种子 {discarded:,}")
            frontier = sorted((item[1] for item in next_heap), key=lambda node: node[0])
            if not frontier:
                break

    elif search_mode == "prune":
        # ======================================================================
        # 新增的核心逻辑：基于容忍下限的高阶组合前向剪枝搜索
        # 无需维持巨大的seen集合，利用位运算严格保证组合拓展无重复。
        # 记录每一层合法组合的核心指标，如果某个父组合已经被剪枝，或者当前
        # 新产生的K阶组合相对其父节点回落过大（下限容忍度），则废弃不繁衍。
        # ======================================================================
        combo_metrics_cache = {}
        # 初始 BFS 边缘: (已选中的节点元组, 共同窗口左端点, 共同窗口右端点, 剩余候选集Mask)
        frontier = [((), 0, T - 1, active_mask)]

        for k in range(1, max_k + 1):
            next_frontier = []
            before = processed

            for chosen, parent_lo, parent_hi, parent_mask in frontier:
                candidates = parent_mask
                while candidates:
                    bit = candidates & -candidates
                    candidates ^= bit  # 剔除已取出的位，防止(i, j)与(j, i)的重复
                    i = bit.bit_length() - 1
                    idxs = chosen + (i,)

                    # 1. 回溯查询 K-1 阶所有直系父节点的指标
                    if k > 1:
                        parent_metrics = []
                        valid_parents = True
                        for p_idxs in itertools.combinations(idxs, k - 1):
                            if p_idxs not in combo_metrics_cache:
                                valid_parents = False
                                break
                            parent_metrics.append(combo_metrics_cache[p_idxs])

                        # 只要有一个父组合之前因为没达标被剪枝了，子组合也无资格繁衍
                        if not valid_parents:
                            insufficient_branches += 1
                            continue
                        min_parent_metric = min(parent_metrics)
                    else:
                        min_parent_metric = None

                    # 获取新的交集约束
                    child_mask = candidates & compatible[i]
                    lo = max(parent_lo, int(first_i[i]))
                    hi = min(parent_hi, int(last_i[i]))

                    if k < min_k and count_bits(child_mask) < min_k - k:
                        insufficient_branches += 1
                        continue

                    core = evaluate_core(idxs, lo, hi, need_rank=True)
                    if core is None:
                        continue

                    # 2. 提取核心指标
                    if prune_metric.lower() == "calmar":
                        risk = realized_risk(core["daily"])
                        metric_val = risk["calmar"]
                        if np.isnan(metric_val):
                            metric_val = -np.inf
                    else:
                        # 退化备用为 Profit Factor (盈亏比)
                        sl = core["sl"]
                        w = core["w"]
                        gp = float((POS[list(idxs), sl] * w[:, None]).sum())
                        gl = float((NEG[list(idxs), sl] * w[:, None]).sum())
                        metric_val = ratio(gp, abs(gl))
                        if np.isnan(metric_val):
                            metric_val = 0.0

                    # 3. 容忍下限验证
                    if k > 1 and min_parent_metric is not None:
                        # 兼容处理指标出现负数的防错（若最差父级为负，应当放宽下限）
                        if min_parent_metric > 0:
                            threshold = prune_tolerance * min_parent_metric
                        else:
                            # e.g., 若容忍系数0.8，父代-10，则下限降至 -12，允许进一步恶化一丢丢
                            threshold = min_parent_metric * (2.0 - prune_tolerance)

                        # 若指标触发了断崖式恶化，直接不让它繁衍下一层
                        if metric_val < threshold:
                            beam_discarded += 1
                            continue

                    # 4. 保留“半成品”数据至缓存
                    combo_metrics_cache[idxs] = metric_val

                    # 若组合达标，则推入结果报告中
                    if k >= min_k and core["passed"]:
                        results.append(make_row(core))

                    # 把满足基础繁衍条件的推入下层
                    if k < max_k and bool(child_mask):
                        next_frontier.append((idxs, lo, hi, child_mask))

            print(
                f"K={k}：评估 {processed - before:,} 个候选 | 容忍下限剪枝淘汰 {beam_discarded:,} | 下层种子 {len(next_frontier):,}")
            frontier = next_frontier
            if not frontier:
                break

    exhaustive = (search_mode == "exact") or (beam_discarded == 0)
    coverage_text = "已穷尽所有结构可行候选" if exhaustive else "执行剪枝搜索，未穷尽全部候选"
    print(f"\n搜索统计：实际评估 {processed:,} | {coverage_text}")
    print(f"候选不足分支 {insufficient_branches:,} | 剪枝/近似淘汰种子 {beam_discarded:,}")
    print("已评估目标组合的过滤统计：" + " | ".join(f"{name} {count:,}" for name, count in skipped.items()))
    if not results:
        print("[提示] 本次搜索未发现通过当前过滤条件的组合。")
        return

    results.sort(key=lambda row: (row["组合数量(K)"], tuple(map(int, row["_idx"].split(",")))))
    df_all = pd.DataFrame(results)
    df_all["_scan_order"] = np.arange(len(df_all))
    df_all.sort_values(
        by=["周期盈利率(%)", "30日盈利窗口率(%)", "_scan_order"],
        ascending=[False, False, True], na_position="last", inplace=True,
    )
    df_all.drop(columns="_scan_order", inplace=True)
    df_all.reset_index(drop=True, inplace=True)
    df_all["搜索模式"] = search_mode
    df_all["搜索是否穷尽"] = exhaustive
    df_all.attrs["search"] = {
        "mode": search_mode, "exhaustive": exhaustive,
        "theoretical_combos": total_combos, "evaluated": processed,
        "evaluated_by_k": evaluated_by_k,
        "beam_discarded": beam_discarded,
        "insufficient_branches": insufficient_branches,
        "required_overlap_days": required_days,
    }
    output_parent = os.path.dirname(os.path.abspath(output_csv))
    os.makedirs(output_parent, exist_ok=True)
    df_all.drop(columns=["_lo", "_hi", "_idx"]).to_csv(
        output_csv, index=False, encoding="utf-8-sig", na_rep="N/A")
    print(f"组合评估完成：{len(df_all):,} 个已发现的有效组合 | {coverage_text} | {output_csv}\n")

    member_alias_map = {}

    for k in range(min_k, max_k + 1):
        df_k = df_all[df_all["组合数量(K)"] == k].head(top_n_per_k)
        if df_k.empty:
            continue
        print("=" * 112)
        print(f"🏆 【{k} 策略组合】本次结果 TOP {len(df_k)}")
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

                member_label = records[i]["label"]
                if member_label not in member_alias_map:
                    member_alias_map[member_label] = f"成员{len(member_alias_map) + 1}"

                rows.append({
                    "成员": member_label,
                    "成员编号": member_alias_map[member_label],
                    "窗口净利(M)": fmt(m["net"]),
                    "已实现MDD(M)": fmt(m["mdd"]),
                    "已实现Calmar": fmt(m["calmar"]),
                    "周期盈利率(%)": fmt(member_win),
                })

            df_print = pd.DataFrame(rows)

            display_cols = [
                "成员",
                "成员编号",
                "窗口净利(M)",
                "已实现MDD(M)",
                "已实现Calmar",
                "周期盈利率(%)"
            ]

            print_table(df_print[display_cols])
        print()
    return df_all


def print_ranking_report_from_csv(
        ranking_csv="portfolio_multi_ranking.csv",
        csv_dir="./extracted_trades_csv",
        top_n_per_k=5
):
    """
    独立打印函数：从已生成的 CSV 中直接读取排名，
    并提取对应的明细重构窗口内的成员指标，免去重新搜索。
    """
    import os
    import pandas as pd
    import numpy as np

    if not os.path.exists(ranking_csv):
        print(f"❌ 找不到排名文件: {ranking_csv}，请确认文件名或路径。")
        return

    df_all = pd.read_csv(ranking_csv)
    if df_all.empty:
        print("❌ 排名文件为空。")
        return

    print("正在加载明细以重构成员数据 (这只需要几秒钟)...")
    records, _ = _load_strategy_records(csv_dir)
    label_to_idx = {r["label"]: i for i, r in enumerate(records)}

    if not records:
        print("❌ 找不到单策略明细，无法重构成指标。")
        return

    # 重建时间轴基准，用于对齐窗口
    g_start = min(pd.Timestamp(r["open_dt"].min()) for r in records).normalize()
    g_end = max(pd.Timestamp(r["close_dt"].max()) for r in records).normalize()
    all_days = pd.date_range(g_start, g_end, freq="D")
    T = len(all_days)
    day_ns = pd.date_range(g_start, periods=T + 1, freq="D").asi8

    PNL = np.zeros((len(records), T))
    event_data = []

    for i, r in enumerate(records):
        opens = pd.DatetimeIndex(r["open_dt"])
        closes = pd.DatetimeIndex(r["close_dt"])
        si = np.asarray((opens.normalize() - g_start).days, dtype=np.int64)
        ei = np.asarray((closes.normalize() - g_start).days, dtype=np.int64)
        si = np.minimum(si, ei)
        p = np.asarray(r["pnl"], dtype=float)
        np.add.at(PNL[i], ei, p)

        order = np.argsort(closes.asi8, kind="stable")
        times = closes.asi8[order]
        prefix = np.r_[0.0, np.cumsum(p[order], dtype=float)]
        flags = np.asarray(r["is_blowup"], dtype=bool)[order]
        event_data.append((times, prefix, np.unique(times[flags])))

    def member_risk(i, lo, hi):
        daily = PNL[i, lo:hi + 1]
        cum = np.cumsum(daily, dtype=float)
        peak = np.maximum.accumulate(np.r_[0.0, cum])[1:]
        dd = np.maximum(peak - cum, 0.0)
        net = float(cum[-1])
        annual = net * DAYS_PER_YEAR / len(daily)
        mdd = float(dd.max())
        calmar = (annual / mdd) if mdd > 0 else (float("inf") if net > 0 else float("nan"))
        return {"net": net, "mdd": mdd, "calmar": calmar}

    def window_blowups(i, lo, hi):
        boundaries = event_data[i][2]
        a = np.searchsorted(boundaries, day_ns[lo], side="left")
        b = np.searchsorted(boundaries, day_ns[hi + 1], side="left")
        return boundaries[a:b]

    def cycle_stats(indices, weights, boundaries):
        count = len(boundaries) - 1
        if count <= 0:
            return 0, float("nan")
        profits = np.zeros(count, dtype=float)
        for i, weight in zip(indices, weights):
            times, prefix, _ = event_data[i]
            positions = np.searchsorted(times, boundaries, side="right")
            profits += weight * np.diff(prefix[positions])
        return count, float(np.mean(profits > 0) * 100.0)

    def fmt(value, digits=2, suffix=""):
        if pd.isna(value): return "N/A"
        if np.isposinf(value): return "∞" + suffix
        if np.isneginf(value): return "-∞" + suffix
        return f"{value:.{digits}f}" + suffix

    member_alias_map = {}

    # 按照 K 的大小进行分组打印
    for k in sorted(df_all["组合数量(K)"].unique()):
        df_k = df_all[df_all["组合数量(K)"] == k].head(top_n_per_k)
        if df_k.empty:
            continue

        print("\n" + "=" * 112)
        print(f"🏆 【{k} 策略组合】历史排名记录读取 TOP {len(df_k)}")
        print("=" * 112)

        for rank, (_, r) in enumerate(df_k.iterrows(), 1):
            print(
                f"\nNo.{rank} | 完整周期 {int(r['完整周期数'])} 段 | 周期盈利率 {fmt(r['周期盈利率(%)'], suffix='%')}")
            print(f"   窗口 {r['重叠起']} ~ {r['重叠止']} | 共 {int(r['重叠天数'])} 天")
            print(
                f"   分散化系数 {fmt(r['分散化系数'], 3)} | 平均两两持仓重合 {fmt(r['平均两两持仓重合(%)'], suffix='%')} | 最高两两持仓重合 {fmt(r['最高两两持仓重合(%)'], suffix='%')}")
            print(
                f"   收益 | 净利润 {fmt(r['组合净利(M)'])} M | 年化净利润 {fmt(r['年化净利(M/年)'], 3)} M/年 | Profit Factor {fmt(r['Profit Factor'])}")
            print(
                f"   已实现风险 | 最大回撤 {fmt(r['已实现MDD(M)'])} M | 相对最大回撤 {fmt(r['相对已实现MDD(%)'], suffix='%')} | 最长水下期 {int(r['最长水下期(天)'])} 天 | 已实现 Calmar {fmt(r['已实现Calmar'])}")
            print(
                f"   滚动尾部 | 最差7日收益 {fmt(r['最差7日收益(M)'])} M | 最差30日收益 {fmt(r['最差30日收益(M)'])} M | 最差90日收益 {fmt(r['最差90日收益(M)'])} M")
            print(
                f"   时间稳定性 | 7日盈利窗口率 {fmt(r['7日盈利窗口率(%)'], suffix='%')} | 30日盈利窗口率 {fmt(r['30日盈利窗口率(%)'], suffix='%')}")
            print(f"   四段利润贡献 | {r['四段净利分布']} | 最低阶段贡献 {fmt(r['最低阶段贡献(%)'], 1, suffix='%')}")

            # 解析组合对应的单策略标签
            labels = str(r["组合策略清单"]).split("  ➕  ")

            # 定位时间窗口索引
            lo_dt = pd.Timestamp(r['重叠起']).normalize()
            hi_dt = pd.Timestamp(r['重叠止']).normalize()
            lo = (lo_dt - g_start).days
            hi = (hi_dt - g_start).days

            rows = []
            for lbl in labels:
                lbl = lbl.strip()
                if lbl not in label_to_idx:
                    print(f"   [警告] 缓存中找不到策略 {lbl}，略过其详情打印。")
                    continue

                i = label_to_idx[lbl]
                m = member_risk(i, lo, hi)
                _, member_win = cycle_stats([i], [1.0], window_blowups(i, lo, hi))

                if lbl not in member_alias_map:
                    member_alias_map[lbl] = f"成员{len(member_alias_map) + 1}"

                rows.append({
                    "成员": lbl,
                    "成员编号": member_alias_map[lbl],
                    "窗口净利(M)": fmt(m["net"]),
                    "已实现MDD(M)": fmt(m["mdd"]),
                    "已实现Calmar": fmt(m["calmar"]),
                    "周期盈利率(%)": fmt(member_win),
                })

            if rows:
                df_print = pd.DataFrame(rows)
                display_cols = ["成员", "成员编号", "窗口净利(M)", "已实现MDD(M)", "已实现Calmar", "周期盈利率(%)"]
                print_table(df_print[display_cols])
        print()

if __name__ == "__main__":
    PLATEAU_CSV = "strategy_leaderboard_100800_files_plateau.csv"  # 若无平原表填 None

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
        search_mode="prune",  # 使用最新集成的层级容忍剪枝算法
        prune_tolerance=0.8,  # 指标回落容忍度，0.8代表允许最大20%回撤
        prune_metric="calmar",  # 核心评估指标 (可选 "calmar" 或 "pf")
    )

    # print_ranking_report_from_csv()