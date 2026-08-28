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
import warnings
import multiprocessing as mp

import numpy as np
import pandas as pd
import unicodedata

from app.signal_trade_lite.martin_strategy_backest import TimelineReplayer, evaluate_free_ride

# =====================================================================
# 参数配置
# =====================================================================
# 缓存文件所在的目录 (请与你生成数据时保持一致)
CACHE_DIR = "./backest/"  # 做多策略默认缓存目录
SHORT_CACHE_DIR = r"G:\short_data"  # 新增：做空策略缓存目录

# CACHE_DIR = r"E:\backtest_data_1m"  # 做多策略默认缓存目录
# SHORT_CACHE_DIR = r"E:\backtest_data_1m"  # 新增：做空策略缓存目录

# 回测测试用的保证金深度 (Margin) 列表
TEST_MARGINS = [2.55, 10.0]

# === 新增：打印过滤与并行处理参数 ===
# 最终打印时，过滤掉预期存活(天)小于此数值的结果
MIN_LIFESPAN_DAYS = 30

# 最终打印时，过滤掉 净利润(Margin倍数) 小于此数值的结果
MIN_NET_PROFIT = -1

# 使用并行的加载，加快速度，并行度配置
PARALLEL_WORKERS = 20

# === 新增：需要被评估和展示的目标策略白名单（加上字符串引号） ===
TARGET_STRATEGIES = [
    "strategy_1_vwap_zscore",
    "strategy_2_quantile_deviation",
    "strategy_4_volume_price_absorption",
    "strategy_5_liquidity_vacuum",
    "strategy_6_volume_climax",
    "strategy_8_rolling_stop_hunt",
    "strategy_12_kaufman_efficiency_ratio",
    "strategy_15_micro_autocorrelation",
    "strategy_17_sniper_combo_long",
    "strategy_18_sniper_combo_short",
    "strategy_19_pulse_dryup_long",
    "strategy_20_pulse_dryup_short",
    "strategy_21_squeeze_snapback_long",
    "strategy_22_squeeze_snapback_short",
    "strategy_23_volume_climax_absorption_long",
    "strategy_24_volume_climax_absorption_short",
    "strategy_25_flash_crash_rebound_long",
    "strategy_26_flash_crash_rebound_short"
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
    blowup_prob = (n_blowup / n_cycles_total * 100) if n_cycles_total > 0 else 0.0

    return {
        "币种": symbol,
        "策略": strategy_name,
        "方向": direction,
        "加仓间距": add_step,
        "止盈间距": tp_step,
        "加仓倍数": mult,
        "总信号数": n_cycles_total,
        "实际开仓数": report.get("n_trades", 0),
        "胜率(%)": round(report.get("win_rate", 0) * 100, 2) if pd.notnull(report.get("win_rate")) else 0.0,
        "爆仓次数": n_blowup,
        "爆仓几率(%)": round(blowup_prob, 2),
        "预期存活(天)": round(expected_lifespan_hour / 24.0, 2) if not np.isinf(
            expected_lifespan_hour) else "999 (未爆仓)",
        "平均持仓(h)": round(holding_time, 2) if pd.notnull(holding_time) else 0.0,
        "死前翻倍胜率(%)": round(free_ride_win_rate * 100, 2) if pd.notnull(free_ride_win_rate) else 0.0,
        "总收益(Margin倍数)": round(report.get("_extracted_gross_profit", 0), 2),
        "总亏损(Margin倍数)": round(report.get("_extracted_gross_loss", 0), 2),
        "净利润(Margin倍数)": round(report.get("total_net_pnl_in_margin", 0), 2),
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
        if pnl_col:
            gross_profit = float(trades_df.loc[trades_df[pnl_col] > 0, pnl_col].sum())
            gross_loss = float(trades_df.loc[trades_df[pnl_col] < 0, pnl_col].sum())

            # 若原始闭环收益列与最终报告净利润做了除以本金等换算操作，此处进行等比缩放对齐
            net_pnl_sum = gross_profit + gross_loss
            report_net = float(report.get("total_net_pnl_in_margin", 0.0))
            if abs(net_pnl_sum) > 1e-6 and abs(report_net) > 1e-6 and abs(net_pnl_sum - report_net) > 1e-6:
                ratio = report_net / net_pnl_sum
                gross_profit *= ratio
                gross_loss *= ratio

            report["_extracted_gross_profit"] = gross_profit
            report["_extracted_gross_loss"] = gross_loss
        else:
            # 防御性回退：如果找不到对应列名，尝试读取报告可能内置的字段
            report["_extracted_gross_profit"] = report.get("gross_profit_in_margin", report.get("total_profit", 0.0))
            report["_extracted_gross_loss"] = report.get("gross_loss_in_margin", report.get("total_loss", 0.0))
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


def analyze_all_strategies():
    print("=" * 80)
    print(f" 🚀 启动全局策略评估引擎 | 设定测试 Margins = {TEST_MARGINS}")
    print("=" * 80)

    search_pattern_main = os.path.join(CACHE_DIR, "stage1_*.pkl")
    search_pattern_short = os.path.join(SHORT_CACHE_DIR, "stage1_*.pkl")

    # === 修复点 1：使用 set 对文件路径去重，防止目录相同时数据翻倍 ===
    files_main = glob.glob(search_pattern_main)
    files_short = glob.glob(search_pattern_short) if os.path.exists(SHORT_CACHE_DIR) else []
    pkl_files = list(set(files_main + files_short))

    if not pkl_files:
        print(f"[错误] 在 {CACHE_DIR} 及 {SHORT_CACHE_DIR} 目录下均未找到任何 stage1_*.pkl 文件！")
        return

    # 文件前置过滤逻辑，仅保留属于白名单策略的文件
    filtered_pkl_files = []
    for filepath in pkl_files:
        filename = os.path.basename(filepath)
        _, strategy_name, _ = _parse_filename(filename)
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

    # === 修复点 2：将过滤逻辑提前，保证导出的 CSV 也是干净的 ===
    def check_lifespan(val):
        if isinstance(val, str) and "未爆仓" in val:
            return True
        try:
            return float(val) >= MIN_LIFESPAN_DAYS
        except:
            return True

    df_all = df_all[df_all["预期存活(天)"].apply(check_lifespan)]
    df_all = df_all[df_all["净利润(Margin倍数)"] >= MIN_NET_PROFIT]

    # 输出完整结果 CSV 报告
    output_csv = "strategy_leaderboard_filtered.csv"
    df_all.to_csv(output_csv, index=False, encoding='utf-8-sig')
    print(f"已将过滤后的结果保存至: {output_csv}\n")

    display_cols = ["Margin", "币种", "方向", "加仓间距", "止盈间距", "加仓倍数", "实际开仓数", "胜率(%)", "爆仓次数",
                    "爆仓几率(%)", "预期存活(天)", "平均持仓(h)", "死前翻倍胜率(%)",
                    "总收益(Margin倍数)", "总亏损(Margin倍数)", "净利润(Margin倍数)"]

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

    for strategy_name, df_strat in df_all.groupby("策略"):
        print(f"\n🏆 策略 = {strategy_name} | 多币种 & 不同 Margin 综合表现:")

        # 排序
        df_display = df_strat.sort_values(by=["币种", "方向", "Margin", "加仓间距", "止盈间距", "加仓倍数"]).copy()

        # === 修复点 3：此处无需重复过滤，已被提前 ===
        if df_display.empty:
            continue

        df_display = df_display[display_cols]

        df_display.rename(columns={
            "死前翻倍胜率(%)": "翻倍胜率(%)",
            "总收益(Margin倍数)": "总收益(M倍)",
            "总亏损(Margin倍数)": "总亏损(M倍)",
            "净利润(Margin倍数)": "净利润(M倍)"
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

if __name__ == "__main__":
    mp.freeze_support()
    analyze_all_strategies()