# -*- coding: utf-8 -*-
"""
======================================================================
加密货币马丁格尔策略 —— 全局表现分析与排名引擎 (低内存版)
======================================================================
"""

import os
import glob
import pickle
import gc
import re
import time
import warnings
import multiprocessing as mp
import gzip  # === 兼容新老文件修改点 1：引入 gzip ===

import numpy as np
import pandas as pd
import unicodedata

from app.signal_trade_lite.martin_strategy_backest import TimelineReplayer, evaluate_free_ride

# =====================================================================
# 参数配置
# =====================================================================
# 缓存文件所在的目录 (请与你生成数据时保持一致)
# CACHE_DIR = "./backest/"  # 做多策略默认缓存目录
# SHORT_CACHE_DIR = r"G:\short_data"  # 新增：做空策略缓存目录

CACHE_DIR = r"W:\backtest_data_1m"  # 做多策略默认缓存目录
SHORT_CACHE_DIR = r"W:\backtest_data_1m"  # 新增：做空策略缓存目录

# 回测测试用的保证金深度 (Margin) 列表
TEST_MARGINS = [2,3,4,5,6,7,8,9,10]

# === 新增：打印过滤与并行处理参数 ===
# 最终打印时，过滤掉预期存活(天)小于此数值的结果
MIN_LIFESPAN_DAYS = 60

# 最终打印时，过滤掉 净利润(Margin倍数) 小于此数值的结果
MIN_NET_PROFIT = -1

# 使用并行的加载，加快速度，并行度配置
PARALLEL_WORKERS = 20

# === 新增：需要被评估和展示的目标策略白名单（加上字符串引号） ===
TARGET_STRATEGIES = [
]

# =====================================================================
# 内存优化开关
# =====================================================================
USE_SUBPROCESS = True
SHRINK_DTYPES = True
DOWNCAST_FLOAT32 = True
FLOAT32_SAFE_MAX_ABS = 1e7
KEEP_COLUMNS = None
PRINT_MEMORY = False

_TIME_LIKE_KEYS = ("time", "stamp", "epoch", "date", "millis", "nanos", "_ms", "_ns")


# =====================================================================
# 内部工具函数
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


def _available_memory_ok(need_bytes):
    try:
        import psutil
        return psutil.virtual_memory().available > need_bytes
    except Exception:
        return True


def _shrink_dtypes(df):
    if not SHRINK_DTYPES or df is None or len(df) == 0:
        return df
    if df.columns.duplicated().any():
        return df

    try:
        cur_mem = float(df.memory_usage(index=True, deep=False).sum())
    except Exception:
        cur_mem = 0.0
    if cur_mem > 0 and not _available_memory_ok(cur_mem * 0.7):
        return df

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        if DOWNCAST_FLOAT32:
            f_cols = [c for c in df.columns if df[c].dtype == np.float64]
            for c in f_cols:
                try:
                    arr = df[c].to_numpy(dtype=np.float64, copy=False)
                    if arr.size:
                        lo, hi = np.nanmin(arr), np.nanmax(arr)
                    else:
                        lo = hi = 0.0
                    lo = 0.0 if not np.isfinite(lo) else float(lo)
                    hi = 0.0 if not np.isfinite(hi) else float(hi)
                    max_abs = max(abs(lo), abs(hi))
                    name_like_time = any(k in str(c).lower() for k in _TIME_LIKE_KEYS)
                    del arr

                    if (not name_like_time) and max_abs < FLOAT32_SAFE_MAX_ABS:
                        df[c] = df[c].astype(np.float32)
                    else:
                        df[c] = df[c].copy()
                except Exception:
                    continue
            gc.collect()

        i_cols = [c for c in df.columns if str(df[c].dtype) in ("int64", "uint64")]
        for c in i_cols:
            try:
                s = df[c]
                mn, mx = s.min(), s.max()
                if (pd.notnull(mn) and pd.notnull(mx)
                        and mn >= np.iinfo(np.int32).min and mx <= np.iinfo(np.int32).max):
                    df[c] = s.astype(np.int32)
                else:
                    df[c] = s.copy()
                del s
            except Exception:
                continue
        gc.collect()

    return df


def _build_row(symbol, strategy_name, direction, report, add_step, tp_step, mult):
    holding_time = report.get("avg_holding_hour_traded", 0.0)
    expected_lifespan_hour = report.get("expected_lifespan_hour", np.inf)
    free_ride_win_rate = report.get("free_ride_win_rate", np.nan)
    n_cycles_total = report.get("n_cycles_total", 0)
    n_blowup = report.get("n_blowup", 0)
    n_trades_actual = report.get("n_trades", 0)  # 获取实际开仓数

    # 修复：使用实际开仓数计算爆仓几率
    blowup_prob = (n_blowup / n_trades_actual * 100) if n_trades_actual > 0 else 0.0

    return {
        "币种": symbol,
        "策略": strategy_name,
        "方向": direction,
        "加仓间距": add_step,
        "止盈间距": tp_step,
        "加仓倍数": mult,
        "总信号数": n_cycles_total,
        "实际开仓数": n_trades_actual,
        "胜率(%)": round(report.get("win_rate", 0) * 100, 2) if pd.notnull(report.get("win_rate")) else 0.0,
        "爆仓次数": n_blowup,
        "爆仓几率(%)": round(blowup_prob, 2),  # 现在这里计算准确了
        "预期存活(天)": round(expected_lifespan_hour / 24.0, 2) if not np.isinf(
            expected_lifespan_hour) else "999 (未爆仓)",
        "中位存活(天)": round(report.get("_median_survival", 0), 2),
        "最大存活(天)": round(report.get("_max_survival", 0), 2),
        "最小存活(天)": round(report.get("_min_survival", 0), 2),
        "平均持仓(h)": round(holding_time, 2) if pd.notnull(holding_time) else 0.0,
        "中位数持仓(h)": round(report.get("_median_holding", 0), 2),
        "最大持仓(h)": round(report.get("_max_holding", 0), 2),
        "持仓时间占比(%)": round(report.get("_holding_ratio", 0), 2),
        "死前翻倍胜率(%)": round(free_ride_win_rate * 100, 2) if pd.notnull(free_ride_win_rate) else 0.0,
        "平均回撤(M倍)": round(report.get("_avg_mdd", 0), 4),
        "中位回撤(M倍)": round(report.get("_median_mdd", 0), 4),
        "总收益(Margin倍数)": round(report.get("_extracted_gross_profit", 0), 2),
        "总亏损(Margin倍数)": round(report.get("_extracted_gross_loss", 0), 2),
        "净利润(Margin倍数)": round(report.get("total_net_pnl_in_margin", 0), 2),
        "平均每天收益(M倍)": round(report.get("_avg_daily_profit", 0), 4),
        "每天中位数收益(M倍)": round(report.get("_median_daily_profit", 0), 4),
        "最长无盈利(天)": report.get("_max_consecutive_no_profit", 0),
        "无盈利占比(%)": round(report.get("_no_profit_ratio", 0), 2),
        "年化爆仓次数": round(report.get("blowups_per_year", 0), 2),
        "翻倍所需时间(小时)": round(report.get("time_to_double_hour", 0), 2),
        "0-1层解决战斗比例(%)": round(report.get("low_layer_ratio", 0) * 100, 2),
        "手续费占毛利(%)": round(report.get("fee_ratio_traded", 0) * 100, 2)
    }

def _process_one_file(file_path):
    filename = os.path.basename(file_path)
    symbol, strategy_name, direction = _parse_filename(filename)
    rows_by_margin = {}

    gc.disable()
    try:
        # === 兼容新老文件修改点 2：根据后缀动态判断读取方式 ===
        if file_path.endswith(".gz"):
            with gzip.open(file_path, 'rb') as f:
                cached_data = pickle.load(f)
        else:
            with open(file_path, 'rb', buffering=4 * 1024 * 1024) as f:
                cached_data = pickle.load(f)
    finally:
        gc.enable()

    attrs = cached_data.get('attrs', {})
    cycles_df = cached_data.pop('df')
    cached_data.clear()
    del cached_data
    cycles_df.attrs = attrs

    # 提取参数空间的具体参数
    add_step = attrs.get("add_step", 0.002)
    tp_step = attrs.get("tp_step", 0.003)
    mult = attrs.get("multiplier", 2.0)

    gc.collect()

    if len(cycles_df) == 0:
        del cycles_df
        gc.collect()
        return rows_by_margin

    if KEEP_COLUMNS:
        keep = [c for c in KEEP_COLUMNS if c in cycles_df.columns]
        if keep and len(keep) < len(cycles_df.columns):
            cycles_df = cycles_df[keep].copy()
            gc.collect()
    cycles_df = _shrink_dtypes(cycles_df)
    cycles_df.attrs = attrs

    replayer = TimelineReplayer(cycles_df)

    for margin in TEST_MARGINS:
        trades_df = replayer.run(margin)
        report = evaluate_free_ride(trades_df, cycles_df, margin)

        # === 新增：解析每一笔闭环，智能提取总收益和总亏损 ===
        pnl_col = next((c for c in ["net_pnl_in_margin", "pnl_in_margin", "net_pnl", "pnl", "profit", "net_profit"] if
                        c in trades_df.columns), None)

        avg_daily = 0.0
        median_daily = 0.0
        max_consecutive_no_profit = 0
        no_profit_ratio = 0.0
        median_holding = 0.0
        max_holding = 0.0
        holding_ratio = 0.0
        avg_mdd = 0.0
        median_mdd = 0.0
        median_survival = 0.0
        max_survival = 0.0
        min_survival = 0.0

        if pnl_col:
            gross_profit = float(trades_df.loc[trades_df[pnl_col] > 0, pnl_col].sum())
            gross_loss = float(trades_df.loc[trades_df[pnl_col] < 0, pnl_col].sum())

            # 若原始闭环收益列与最终报告净利润做了除以本金等换算操作，此处进行等比缩放对齐
            net_pnl_sum = gross_profit + gross_loss
            report_net = float(report.get("total_net_pnl_in_margin", 0.0))
            ratio = 1.0
            if abs(net_pnl_sum) > 1e-6 and abs(report_net) > 1e-6 and abs(net_pnl_sum - report_net) > 1e-6:
                ratio = report_net / net_pnl_sum
                gross_profit *= ratio
                gross_loss *= ratio

            report["_extracted_gross_profit"] = gross_profit
            report["_extracted_gross_loss"] = gross_loss

            # --- 新增：计算不考虑爆仓亏损时的平均每天收益和中位数收益，及最长连亏 ---
            try:
                time_col = None
                start_col = None
                end_col = None
                for col in trades_df.columns:
                    c_lower = str(col).lower()
                    if any(k in c_lower for k in _TIME_LIKE_KEYS):
                        if any(k in c_lower for k in ("close", "end", "finish")):
                            end_col = col
                            if time_col is None: time_col = col
                        elif any(k in c_lower for k in ("open", "start", "begin")):
                            start_col = col
                        else:
                            if time_col is None: time_col = col

                if end_col is None: end_col = time_col
                if start_col is None: start_col = end_col

                if time_col is not None and not trades_df.empty:
                    # 获取时间序列
                    if pd.api.types.is_numeric_dtype(trades_df[time_col]):
                        if trades_df[time_col].max() > 1e15:
                            dt_series = pd.to_datetime(trades_df[time_col], unit='ns')
                        elif trades_df[time_col].max() > 1e11:
                            dt_series = pd.to_datetime(trades_df[time_col], unit='ms')
                        else:
                            dt_series = pd.to_datetime(trades_df[time_col], unit='s')
                    else:
                        dt_series = pd.to_datetime(trades_df[time_col])

                    dates = dt_series.dt.date

                    # 仅保留大于0的收益（即不考虑爆仓情况下的盈利闭环）
                    win_mask = trades_df[pnl_col] > 0
                    valid_pnl = trades_df.loc[win_mask, pnl_col] * ratio

                    if not dates.empty:
                        min_date = dates.min()
                        max_date = dates.max()

                        if pd.notnull(min_date) and pd.notnull(max_date):
                            # 重建首单至末单的完整日期索引，保证未开仓或全亏损的天数算作0
                            full_dates = pd.date_range(start=min_date, end=max_date).date

                            if not valid_pnl.empty:
                                daily_pnl = valid_pnl.groupby(dates[win_mask]).sum()
                                daily_pnl = daily_pnl.reindex(full_dates, fill_value=0.0)
                            else:
                                daily_pnl = pd.Series(0.0, index=full_dates)

                            avg_daily = float(daily_pnl.mean()) if not daily_pnl.empty else 0.0
                            median_daily = float(daily_pnl.median()) if not daily_pnl.empty else 0.0

                            # --- 新增：最长连续无盈利天数及占比 ---
                            all_pnl = trades_df[pnl_col] * ratio
                            daily_net_pnl = all_pnl.groupby(dates).sum()
                            daily_net_pnl = daily_net_pnl.reindex(full_dates, fill_value=0.0)

                            no_profit_mask = daily_net_pnl <= 0
                            no_profit_days = int(no_profit_mask.sum())
                            total_days = len(full_dates)
                            no_profit_ratio = (no_profit_days / total_days * 100) if total_days > 0 else 0.0

                            if not no_profit_mask.empty:
                                is_profit = ~no_profit_mask
                                groups = is_profit.cumsum()
                                max_consecutive_no_profit = int(no_profit_mask.groupby(groups).sum().max())
            except Exception as e:
                pass

            # --- 新增：回撤统计、持仓时间以及存活时间 ---
            try:
                if not trades_df.empty:
                    # 1. 回撤统计
                    mdd_col = next((c for c in ["max_drawdown", "max_drawdown_in_margin", "max_dd", "max_loss",
                                                "max_loss_in_margin", "mdd", "max_floating_loss"] if
                                    c in trades_df.columns), None)
                    if mdd_col:
                        mdds = trades_df[mdd_col].abs()
                        avg_mdd = float(mdds.mean()) if not mdds.empty else 0.0
                        median_mdd = float(mdds.median()) if not mdds.empty else 0.0

                    # 2. 持仓和存活时间
                    if start_col and end_col:
                        def to_dt(s):
                            if pd.api.types.is_numeric_dtype(s):
                                if s.max() > 1e15:
                                    return pd.to_datetime(s, unit='ns')
                                elif s.max() > 1e11:
                                    return pd.to_datetime(s, unit='ms')
                                else:
                                    return pd.to_datetime(s, unit='s')
                            return pd.to_datetime(s)

                        st_series = to_dt(trades_df[start_col])
                        ed_series = to_dt(trades_df[end_col])

                        dur_h = (ed_series - st_series).dt.total_seconds() / 3600.0
                        dur_h = dur_h[dur_h >= 0]
                        if not dur_h.empty:
                            median_holding = float(dur_h.median())
                            max_holding = float(dur_h.max())

                        # 持仓时间占比
                        intervals = list(zip(st_series, ed_series))
                        intervals.sort(key=lambda x: x[0])
                        merged = []
                        for interval in intervals:
                            if not merged:
                                merged.append(interval)
                            else:
                                last = merged[-1]
                                if interval[0] <= last[1]:
                                    merged[-1] = (last[0], max(last[1], interval[1]))
                                else:
                                    merged.append(interval)

                        total_holding_s = sum((m[1] - m[0]).total_seconds() for m in merged)
                        span_s = (merged[-1][1] - merged[0][0]).total_seconds() if merged else 0
                        holding_ratio = (total_holding_s / span_s * 100) if span_s > 0 else 0.0

                        # 存活时间统计
                        blowup_col = next((c for c in trades_df.columns if
                                           "blowup" in str(c).lower() or "is_liquidated" in str(c).lower()), None)
                        if blowup_col:
                            is_blow = trades_df[blowup_col] == True
                        elif pnl_col:
                            is_blow = trades_df[pnl_col] < -0.8 * margin
                        else:
                            is_blow = pd.Series(False, index=trades_df.index)

                        blow_indices = trades_df.index[is_blow].tolist()

                        survival_days = []
                        last_time = st_series.iloc[0]
                        for b_idx in blow_indices:
                            blow_time = ed_series.loc[b_idx]
                            days = (blow_time - last_time).total_seconds() / 86400.0
                            if days > 0:
                                survival_days.append(days)
                            last_time = blow_time

                        if last_time < ed_series.iloc[-1]:
                            days = (ed_series.iloc[-1] - last_time).total_seconds() / 86400.0
                            if days > 0:
                                survival_days.append(days)

                        if survival_days:
                            median_survival = float(np.median(survival_days))
                            max_survival = float(np.max(survival_days))
                            min_survival = float(np.min(survival_days))
            except Exception as e:
                pass

        else:
            # 防御性回退：如果找不到对应列名，尝试读取报告可能内置的字段
            report["_extracted_gross_profit"] = report.get("gross_profit_in_margin", report.get("total_profit", 0.0))
            report["_extracted_gross_loss"] = report.get("gross_loss_in_margin", report.get("total_loss", 0.0))

        report["_avg_daily_profit"] = avg_daily
        report["_median_daily_profit"] = median_daily
        report["_max_consecutive_no_profit"] = max_consecutive_no_profit
        report["_no_profit_ratio"] = no_profit_ratio
        report["_median_holding"] = median_holding
        report["_max_holding"] = max_holding
        report["_holding_ratio"] = holding_ratio
        report["_avg_mdd"] = avg_mdd
        report["_median_mdd"] = median_mdd
        report["_median_survival"] = median_survival
        report["_max_survival"] = max_survival
        report["_min_survival"] = min_survival
        # =====================================================

        del trades_df
        rows_by_margin[margin] = _build_row(symbol, strategy_name, direction, report, add_step, tp_step, mult)
        del report
        gc.collect()

    del cycles_df, replayer
    gc.collect()
    return rows_by_margin


def _process_one_file_safe(file_path):
    try:
        # 新增将 file_path 原样带出，方便后续日志打印寻找文件名
        return {"ok": True, "file_path": file_path, "rows": _process_one_file(file_path)}
    except BaseException as e:
        return {"ok": False, "file_path": file_path, "err": f"{type(e).__name__}: {e}"}


# 为了控制台打印依然清爽，控制台展示仍保留原有过滤逻辑
def check_lifespan(val):
    if isinstance(val, str) and "未爆仓" in val:
        return True
    try:
        return float(val) >= MIN_LIFESPAN_DAYS
    except:
        return True


def analyze_all_strategies():
    print("=" * 80)
    print(f" 🚀 启动全局策略评估引擎 | 设定测试 Margins = {TEST_MARGINS}")
    print("=" * 80)

    # === 兼容新老文件修改点 3：让 glob 同时搜索 .pkl 和 .pkl.gz 文件 ===
    files_main = glob.glob(os.path.join(CACHE_DIR, "stage1_*.pkl")) + \
                 glob.glob(os.path.join(CACHE_DIR, "stage1_*.pkl.gz"))

    if os.path.exists(SHORT_CACHE_DIR):
        files_short = glob.glob(os.path.join(SHORT_CACHE_DIR, "stage1_*.pkl")) + \
                      glob.glob(os.path.join(SHORT_CACHE_DIR, "stage1_*.pkl.gz"))
    else:
        files_short = []

    pkl_files = list(set(files_main + files_short))

    if not pkl_files:
        print(f"[错误] 在 {CACHE_DIR} 及 {SHORT_CACHE_DIR} 目录下均未找到任何 stage1_*.pkl(或.gz) 文件！")
        return

    # 文件前置过滤逻辑，仅保留属于白名单策略的文件
    filtered_pkl_files = []
    for filepath in pkl_files:
        filename = os.path.basename(filepath)
        _, strategy_name, _ = _parse_filename(filename)
        # 如果需要过滤特定的策略，可以取消下面这行的注释
        # if strategy_name in TARGET_STRATEGIES:
        filtered_pkl_files.append(filepath)

    pkl_files = filtered_pkl_files

    if not pkl_files:
        print(f"[提示] 未找到匹配目标列表 TARGET_STRATEGIES 的任何文件，请检查命名。")
        return

    mode_desc = f"子进程池隔离(并行度={PARALLEL_WORKERS}) + dtype 瘦身" if USE_SUBPROCESS else "主进程 + dtype 瘦身"
    print(f"共匹配到 {len(pkl_files)} 个属于目标列表的缓存文件，开启低内存模式({mode_desc})...\n")

    results_by_margin = {m: [] for m in TEST_MARGINS}
    use_subprocess = USE_SUBPROCESS
    pool = None

    if use_subprocess:
        try:
            ctx = mp.get_context("spawn")
            pool = ctx.Pool(processes=PARALLEL_WORKERS, maxtasksperchild=1)
        except Exception as e:
            print(f"[警告] 子进程池初始化失败，自动切换为主进程内处理...")
            use_subprocess = False

    if use_subprocess:
        results_iter = pool.imap_unordered(_process_one_file_safe, pkl_files)
    else:
        results_iter = map(_process_one_file_safe, pkl_files)

    for idx, result in enumerate(results_iter, 1):
        file_path = result.get("file_path", "Unknown")
        filename = os.path.basename(file_path)

        if result.get("ok"):
            rows = result.get("rows") or {}
            for margin in TEST_MARGINS:
                row = rows.get(margin)
                if row is not None:
                    results_by_margin[margin].append(row)
        else:
            print(f"[警告] 处理失败，已跳过: {filename} | {result.get('err')}")

        if idx % 500 == 0 or idx == len(pkl_files):
            print(f"进度: {idx}/{len(pkl_files)} 个策略文件已处理完成...")

    if pool is not None:
        pool.close()
        pool.join()

    print("\n" + "=" * 80)
    print(f" 🎉 分析完成！开始按策略展示表现...")
    print("=" * 80)

    # 汇总所有结果形成宽表
    all_results = []
    for margin in TEST_MARGINS:
        for row in results_by_margin.get(margin, []):
            r = dict(row)
            r['Margin'] = margin
            all_results.append(r)

    if not all_results:
        print("没有有效数据产生，无结果可展示。")
        return

    df_all = pd.DataFrame(all_results)

    # === 修改点：1. 最终保存的这个csv文件不要进行过滤，且包含文件数量信息 ===
    num_files = len(pkl_files)
    output_csv = f"strategy_leaderboard_{num_files}_files.csv"
    df_all.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"已将未过滤的完整结果保存至: {output_csv}\n")

    df_filtered = df_all[df_all["预期存活(天)"].apply(check_lifespan)]
    df_filtered = df_filtered[df_filtered["净利润(Margin倍数)"] >= MIN_NET_PROFIT]

    # === 增加新增的两列至展示列表 ===
    display_cols = ["Margin", "币种", "方向", "加仓间距", "止盈间距", "加仓倍数", "实际开仓数", "胜率(%)", "爆仓次数",
                    "爆仓几率(%)", "预期存活(天)", "中位存活(天)", "最大存活(天)", "最小存活(天)",
                    "平均持仓(h)", "中位数持仓(h)", "最大持仓(h)", "持仓时间占比(%)", "死前翻倍胜率(%)",
                    "平均回撤(M倍)", "中位回撤(M倍)",
                    "总收益(Margin倍数)", "总亏损(Margin倍数)", "净利润(Margin倍数)",
                    "平均每天收益(M倍)", "每天中位数收益(M倍)", "最长无盈利(天)", "无盈利占比(%)"]

    def get_display_width(s):
        w = 0
        for c in str(s):
            if unicodedata.east_asian_width(c) in ('F', 'W', 'A'):
                w += 2
            else:
                w += 1
        return w

    def right_align(s, width):
        s = str(s)
        pad_len = width - get_display_width(s)
        return " " * max(0, pad_len) + s

    def format_val(val):
        if isinstance(val, (float, np.float32, np.float64)):
            return f"{val:.3f}" if val < 0.1 and val > 0 else f"{val:.2f}"
        return str(val)

    # 注意这里使用 df_filtered 做打印展示，防止刷屏
    for strategy_name, df_strat in df_filtered.groupby("策略"):
        print(f"\n🏆 策略 = {strategy_name} | 多币种 & 不同 Margin 综合表现:")

        # 排序
        df_display = df_strat.sort_values(by=["币种", "方向", "Margin", "加仓间距", "止盈间距", "加仓倍数"]).copy()

        if df_display.empty:
            continue

        df_display = df_display[display_cols]

        df_display.rename(columns={
            "死前翻倍胜率(%)": "翻倍胜率(%)",
            "总收益(Margin倍数)": "总收益(M倍)",
            "总亏损(Margin倍数)": "总亏损(M倍)",
            "净利润(Margin倍数)": "净利润(M倍)",
            "平均每天收益(M倍)": "日均收益(M)",
            "每天中位数收益(M倍)": "日中位收益(M)"
        }, inplace=True)

        cols = list(df_display.columns)
        col_widths = []

        for col in cols:
            max_w = get_display_width(col)
            for val in df_display[col]:
                max_w = max(max_w, get_display_width(format_val(val)))
            col_widths.append(max_w)

        header_cells = [right_align(col, col_widths[i]) for i, col in enumerate(cols)]
        header_str = " | ".join(header_cells)
        sep_line = "-" * len(header_str)

        print(sep_line)
        print(header_str)
        print(sep_line)

        for _, row in df_display.iterrows():
            row_cells = [right_align(format_val(row[col]), col_widths[i]) for i, col in enumerate(cols)]
            print(" | ".join(row_cells))

        print(sep_line)


def show_leaderboard_csv(csv_file="strategy_leaderboard_15600_files.csv", direction="both", min_trades=1000,
                         min_net_profit=-1000, min_total_profit=20):
    """
    专门用于读取并展示 CSV 文件的函数。
    【保留策略分组，且策略区块之间按该组的最大“总收益(M倍)”降序排列】
    """
    if csv_file is None or csv_file == "strategy_leaderboard_filtered.csv":
        import glob
        files = glob.glob("strategy_leaderboard_*_files.csv")
        if files:
            csv_file = sorted(files, key=os.path.getmtime, reverse=True)[0]
        else:
            csv_file = "strategy_leaderboard_filtered.csv"

    if not os.path.exists(csv_file):
        print(f"[错误] 未找到文件: {csv_file}")
        return

    try:
        df_all = pd.read_csv(csv_file)
    except Exception as e:
        print(f"[错误] 读取 CSV 失败: {e}")
        return

    if df_all.empty:
        print("[提示] CSV 文件为空，无数据可展示。")
        return

    # 1. 过滤方向
    d_filter = direction.strip().lower()
    if d_filter == 'long':
        df_all = df_all[df_all["方向"].str.capitalize() == 'Long']
    elif d_filter == 'short':
        df_all = df_all[df_all["方向"].str.capitalize() == 'Short']

    # 2. 过滤交易次数
    if "实际开仓数" in df_all.columns:
        df_all = df_all[df_all["实际开仓数"] >= min_trades]

    df_all = df_all[df_all["中位存活(天)"].apply(check_lifespan)]

    # 3. 过滤净利润
    if "净利润(Margin倍数)" in df_all.columns:
        df_all = df_all[df_all["净利润(Margin倍数)"] >= min_net_profit]

    if "Margin" in df_all.columns:
        df_all = df_all[df_all["Margin"] == 10]

    if "总收益(Margin倍数)" in df_all.columns:
        df_all = df_all[df_all["总收益(Margin倍数)"] >= min_total_profit]

    if df_all.empty:
        print(f"[提示] 根据条件过滤后，无匹配数据。")
        return

    # 提前缩写部分表头名称，方便后续计算和展示
    df_all.rename(columns={
        "死前翻倍胜率(%)": "翻倍胜率(%)",
        "总收益(Margin倍数)": "总收益(M倍)",
        "总亏损(Margin倍数)": "总亏损(M倍)",
        "净利润(Margin倍数)": "净利润(M倍)",
        "平均每天收益(M倍)": "日均收益(M)",
        "每天中位数收益(M倍)": "日中位收益(M)"
    }, inplace=True)

    # === 核心调整 1：计算每个策略的最高总收益，用于策略间的赛区排名 ===
    strategy_max_profits = {}
    for strategy_name, df_strat in df_all.groupby("策略"):
        if "总收益(M倍)" in df_strat.columns:
            strategy_max_profits[strategy_name] = df_strat["总收益(M倍)"].max()
        else:
            strategy_max_profits[strategy_name] = -float('inf')

    # 根据最高收益对策略名称进行降序排序
    sorted_strategies = sorted(strategy_max_profits.keys(), key=lambda k: strategy_max_profits[k], reverse=True)

    # 定义要展示的列（分组展示，因此不包含"策略"列，省空间）
    # 修改 show_leaderboard_csv 中的 display_cols
    display_cols = ["Margin", "币种", "方向", "加仓间距", "止盈间距", "加仓倍数", "实际开仓数",
                    "胜率(%)", "0-1层解决战斗比例(%)", "手续费占毛利(%)",
                    "爆仓次数", "爆仓几率(%)", "预期存活(天)", "中位存活(天)", "最大存活(天)", "最小存活(天)",
                    "平均持仓(h)", "中位数持仓(h)", "最大持仓(h)", "持仓时间占比(%)",
                    "翻倍胜率(%)", "平均回撤(M倍)", "中位回撤(M倍)",
                    "总收益(M倍)", "总亏损(M倍)", "净利润(M倍)",
                    "最长无盈利(天)", "无盈利占比(%)"
                    ]

    display_cols = [ "币种", "加仓间距", "止盈间距", "加仓倍数", "实际开仓数",
                    # "0-1层解决战斗比例(%)",
                    "爆仓次数", "预期存活(天)", "中位存活(天)", "最大存活(天)",
                    "平均持仓(h)", "中位数持仓(h)", "最大持仓(h)",
                    # "持仓时间占比(%)",
                    "翻倍胜率(%)",
                    "总收益(M倍)", "净利润(M倍)"
                    ]
    display_cols = [c for c in display_cols if c in df_all.columns]

    # ========== 内部辅助排版函数 ==========
    def get_display_width(s):
        w = 0
        for c in str(s):
            if unicodedata.east_asian_width(c) in ('F', 'W', 'A'):
                w += 2
            else:
                w += 1
        return w

    def right_align(s, width):
        s = str(s)
        pad_len = width - get_display_width(s)
        return " " * max(0, pad_len) + s

    def format_val(val):
        if isinstance(val, (float, np.float32, np.float64)):
            return f"{val:.3f}" if 0 < val < 0.1 else f"{val:.2f}"
        return str(val)

    # === 核心调整 2：按照排好序的策略列表依次打印 ===
    index_count = 0
    for strategy_name in sorted_strategies:
        index_count += 1
        # 提取当前策略的数据
        df_strat = df_all[df_all["策略"] == strategy_name].copy()

        # 组内排序：按 总收益(M倍) 降序
        if "总收益(M倍)" in df_strat.columns:
            secondary_cols = [c for c in ["币种", "方向", "Margin", "加仓间距", "止盈间距", "加仓倍数"] if
                              c in df_strat.columns]
            sort_cols = ["总收益(M倍)"] + secondary_cols
            ascendings = [False] + [True] * len(secondary_cols)
            df_strat.sort_values(by=sort_cols, ascending=ascendings, inplace=True)

        df_display = df_strat[display_cols]

        # 打印表头，附带展示该策略的最高收益，一目了然
        max_p = strategy_max_profits[strategy_name]
        # print(f"\n🏆 开仓策略编号{index_count} | 方向: {direction.upper()} | 本组最高收益: {max_p:.2f} M倍")
        print(f"\n🏆 开仓策略编号{index_count} | {strategy_name} | 方向: {direction.upper()} | 本组最高收益: {max_p:.2f} M倍")

        cols = list(df_display.columns)
        col_widths = []

        # 计算列宽
        for col in cols:
            max_w = get_display_width(col)
            for val in df_display[col]:
                max_w = max(max_w, get_display_width(format_val(val)))
            col_widths.append(max_w)

        header_cells = [right_align(col, col_widths[i]) for i, col in enumerate(cols)]
        header_str = " | ".join(header_cells)
        sep_line = "-" * len(header_str)

        print(sep_line)
        print(header_str)
        print(sep_line)

        # 打印数据
        for _, row in df_display.iterrows():
            row_cells = [right_align(format_val(row[col]), col_widths[i]) for i, col in enumerate(cols)]
            print(" | ".join(row_cells))

        print(sep_line)


if __name__ == "__main__":
    # time.sleep(3600 * 4)
    mp.freeze_support()
    analyze_all_strategies()
    # csv_file = "strategy_leaderboard_43633_files.csv"
    # # csv_file = "strategy_leaderboard_15600_files.csv"
    #
    # show_leaderboard_csv(csv_file=csv_file, direction="long")  # 默认不传文件路径会自动搜索刚才生成的未过滤的新文件