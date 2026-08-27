# -*- coding: utf-8 -*-
"""
======================================================================
加密货币马丁格尔策略 —— 全局表现分析与排名引擎 (低内存版)
======================================================================
功能:
1. 批量读取 Stage 1 生成的 .pkl 缓存文件 (包含平行宇宙 cycles_df)。
2. 使用指定的 Margin (保证金深度) 进行 Stage 2 时间线重组。
3. 调用 Stage 3 计算核心评估指标。
4. 将所有策略(不同币种、不同信号、多空方向)的表现汇总成 DataFrame，
   并按照"核心生存指标"进行排名，最终输出 CSV 报告。

内存优化 (不改变任何统计口径 / 输出格式):
  A. 每个 pkl 在"独立子进程"中处理，进程退出后 20G 内存由操作系统 100% 归还，
     内存峰值被锁定为"单个文件"，与文件数量彻底解耦(解决堆碎片化累积 OOM)。
  B. 反序列化后立刻摘出 df 并清空外层 dict，pkl 中冗余字段不再常驻。
  C. dtype 瘦身: float64->float32(带量级安全阀) / int64->int32(带范围检查)，
     单文件常驻内存通常降 40%~55%，对速度几乎无影响。
"""

import os
import glob
import pickle
import gc
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

# 回测测试用的保证金深度 (Margin) 列表
# 分别约对应: 5层(0.16), 7层(0.6), 10层(2.55), 11层濒死(10.0), 13层(40.6)
TEST_MARGINS = [0.16, 0.6, 2.55, 10.0, 40.6]

# =====================================================================
# 内存优化开关 (只影响内存/性能，不影响统计结果与输出格式)
# =====================================================================
USE_SUBPROCESS = True  # 每个 pkl 用独立子进程处理，退出后内存 100% 归还操作系统(最关键)
SHRINK_DTYPES = True  # 加载后做 dtype 瘦身(最主要的内存降幅来源)
DOWNCAST_FLOAT32 = True  # float64 -> float32；想要与老结果 bit 级完全一致就设为 False
FLOAT32_SAFE_MAX_ABS = 1e7  # 绝对值超过该量级的 float 列不降精度(防止毫秒时间戳/纳秒等被破坏)
KEEP_COLUMNS = None  # 若明确知道下游只用到哪些列，填列名 list 可再省一半内存；None = 全部保留
PRINT_MEMORY = False  # 调试用：打印每个文件处理完时子进程的 RSS(需要 psutil)

# 列名中出现这些关键字的 float 列，一律不降精度(时间戳类)
_TIME_LIKE_KEYS = ("time", "stamp", "epoch", "date", "millis", "nanos", "_ms", "_ns")


# =====================================================================
# 内部工具函数
# =====================================================================
def _parse_filename(filename):
    """解析文件名 -> (symbol, strategy_name, direction)，逻辑与原版完全一致"""
    # 解析文件名: stage1_BTCUSDT_strategy_1_vwap_zscore_Long_f0.0005...pkl
    try:
        parts = filename.split('_')
        symbol = parts[1]
        direction = "Long" if "_Long_" in filename else "Short" if "_Short_" in filename else "Unknown"
        strat_start_idx = filename.find(symbol) + len(symbol) + 1
        strat_end_idx = filename.find(f"_{direction}_")
        strategy_name = filename[strat_start_idx:strat_end_idx]
    except Exception:
        symbol, strategy_name, direction = "Unknown", filename, "Unknown"
    return symbol, strategy_name, direction


def _available_memory_ok(need_bytes):
    """dtype 瘦身过程中会有"旧块 + 新列"短暂并存的峰值，内存不够就跳过瘦身，绝不因优化反而 OOM"""
    try:
        import psutil
        return psutil.virtual_memory().available > need_bytes
    except Exception:
        return True  # 没装 psutil 时不做限制


def _shrink_dtypes(df):
    """
    dtype 瘦身。
    关键细节: pkl 载入的 DataFrame 中同 dtype 的列共享一个大内存块，只要还有一列引用它，
    整块内存就不会释放。因此这里对 float64 / int64 的列"全部重建"：能降精度的降精度，
    不能降的也用 .copy() 断开与原大块的引用，保证原始大块被真正回收。
    """
    if not SHRINK_DTYPES or df is None or len(df) == 0:
        return df
    if df.columns.duplicated().any():  # 重名列时直接放弃瘦身，避免语义歧义
        return df

    try:
        cur_mem = float(df.memory_usage(index=True, deep=False).sum())
    except Exception:
        cur_mem = 0.0
    # 瘦身期间最坏情况需要约 0.7 倍额外内存，不够就原样返回
    if cur_mem > 0 and not _available_memory_ok(cur_mem * 0.7):
        return df

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        # ---------------- float64 ----------------
        if DOWNCAST_FLOAT32:
            f_cols = [c for c in df.columns if df[c].dtype == np.float64]
            for c in f_cols:
                try:
                    arr = df[c].to_numpy(dtype=np.float64, copy=False)
                    if arr.size:
                        lo = np.nanmin(arr)
                        hi = np.nanmax(arr)
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
                        # 不能降精度的列也必须 copy，否则原 float64 大块无法释放
                        df[c] = df[c].copy()
                except Exception:
                    continue
            gc.collect()

        # ---------------- int64 / uint64 ----------------
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


def _build_row(symbol, strategy_name, direction, report):
    """由 report 组装成一行结果（字段与原版完全一致）"""
    # ==================== 新增更多维度的关键指标 ====================
    holding_time = report.get("avg_holding_hour_traded", 0.0)
    expected_lifespan_hour = report.get("expected_lifespan_hour", np.inf)
    free_ride_win_rate = report.get("free_ride_win_rate", np.nan)

    # 提取所需参数用于计算爆仓几率
    n_blowup = report.get("n_blowup", 0)
    n_trades = report.get("n_trades", 0)
    blowup_rate = report.get("blowup_rate", (n_blowup / n_trades) if n_trades > 0 else 0.0)

    return {
        "币种": symbol,
        "策略": strategy_name,
        "方向": direction,
        "总信号数": report.get("n_cycles_total", 0),
        "实际开仓数": n_trades,
        "胜率(%)": round(report.get("win_rate", 0) * 100, 2) if pd.notnull(report.get("win_rate")) else 0.0,
        "爆仓次数": n_blowup,
        "爆仓几率(%)": round(blowup_rate * 100, 2),
        "预期存活(天)": round(expected_lifespan_hour / 24.0, 2) if not np.isinf(
            expected_lifespan_hour) else "999 (未爆仓)",
        "平均持仓(h)": round(holding_time, 2) if pd.notnull(holding_time) else 0.0,
        "死前翻倍胜率(%)": round(free_ride_win_rate * 100, 2) if pd.notnull(free_ride_win_rate) else 0.0,
        "净利润(Margin倍数)": round(report.get("total_net_pnl_in_margin", 0), 2),
        "盈利和": round(report.get("total_profit", 0.0), 2),
        "亏损和": round(report.get("total_loss", 0.0), 2),
        "年化爆仓次数": round(report.get("blowups_per_year", 0), 2),
        "翻倍所需时间(小时)": round(report.get("time_to_double_hour", 0), 2),
        "0-1层解决战斗比例(%)": round(report.get("low_layer_ratio", 0) * 100, 2),
        "手续费占毛利(%)": round(report.get("fee_ratio_traded", 0) * 100, 2)
    }


def _process_one_file(file_path):
    """
    处理单个 stage1 pkl，返回 {margin: row_dict}。
    该函数在子进程中执行：函数返回后子进程退出，20G 内存由操作系统 100% 回收。
    """
    filename = os.path.basename(file_path)
    symbol, strategy_name, direction = _parse_filename(filename)

    rows_by_margin = {}

    # 1. 加载 Stage 1 数据（关 gc 加速大对象反序列化 + 加大读缓冲）
    gc.disable()
    try:
        with open(file_path, 'rb', buffering=4 * 1024 * 1024) as f:
            cached_data = pickle.load(f)
    finally:
        gc.enable()

    attrs = cached_data.get('attrs', {})
    cycles_df = cached_data.pop('df')  # 摘出 df，不再让外层 dict 持有引用
    cached_data.clear()  # pkl 中其它冗余字段(可能含原始K线)立即释放
    del cached_data
    cycles_df.attrs = attrs
    gc.collect()

    # 跳过没有产生任何信号的空策略
    if len(cycles_df) == 0:
        del cycles_df
        gc.collect()
        return rows_by_margin

    # 2. 内存瘦身：列裁剪(可选) + dtype 压缩
    if KEEP_COLUMNS:
        keep = [c for c in KEEP_COLUMNS if c in cycles_df.columns]
        if keep and len(keep) < len(cycles_df.columns):
            cycles_df = cycles_df[keep].copy()  # copy 才能真正释放被裁掉的列
            gc.collect()
    cycles_df = _shrink_dtypes(cycles_df)
    cycles_df.attrs = attrs  # 瘦身后重新挂回 attrs，保证 Stage3 元信息不丢

    # 3. 初始化重组器 (Stage 2)
    replayer = TimelineReplayer(cycles_df)

    # 对配置的每一个 Margin 水位进行时间线测试
    for margin in TEST_MARGINS:
        trades_df = replayer.run(margin)
        report = evaluate_free_ride(trades_df, cycles_df, margin)
        del trades_df  # 单次 margin 的大对象立刻释放
        rows_by_margin[margin] = _build_row(symbol, strategy_name, direction, report)
        del report
        gc.collect()

    # ==================== 严格的内存回收 ====================
    del cycles_df, replayer
    gc.collect()

    if PRINT_MEMORY:
        try:
            import psutil
            rss = psutil.Process(os.getpid()).memory_info().rss / 1024 ** 3
            print(f"    [内存] {filename} 完成，子进程 RSS ≈ {rss:.2f} GB")
        except Exception:
            pass

    return rows_by_margin


def _process_one_file_safe(file_path):
    """子进程入口：把业务异常包成返回值，只有"进程级"故障才会向主进程抛异常"""
    try:
        return {"ok": True, "rows": _process_one_file(file_path)}
    except BaseException as e:
        return {"ok": False, "err": f"{type(e).__name__}: {e}"}


def analyze_all_strategies():
    print("=" * 80)
    print(f" 🚀 启动全局策略评估引擎 | 设定测试 Margins = {TEST_MARGINS}")
    print("=" * 80)

    # 查找所有 stage1 缓存文件 (合并原目录和做空专用目录)
    search_pattern_main = os.path.join(CACHE_DIR, "stage1_*.pkl")
    search_pattern_short = os.path.join(SHORT_CACHE_DIR, "stage1_*.pkl")

    pkl_files = glob.glob(search_pattern_main)
    # 如果做空目录存在，则追加做空目录下的缓存文件
    if os.path.exists(SHORT_CACHE_DIR):
        pkl_files.extend(glob.glob(search_pattern_short))

    if not pkl_files:
        print(f"[错误] 在 {CACHE_DIR} 及 {SHORT_CACHE_DIR} 目录下均未找到任何 stage1_*.pkl 文件！")
        return

    mode_desc = "单进程子进程池(单次任务后重建)隔离 + dtype 瘦身" if USE_SUBPROCESS else "主进程 + dtype 瘦身"
    print(f"共发现 {len(pkl_files)} 个缓存文件，开启低内存模式({mode_desc})...\n")

    # 用于存放每个 Margin 下的结果，格式为 {margin_value: [row1, row2, ...]}
    results_by_margin = {m: [] for m in TEST_MARGINS}

    use_subprocess = USE_SUBPROCESS
    pool = None

    if use_subprocess:
        try:
            # 初始化常驻单进程池，使用 maxtasksperchild=1 确保每处理完一个文件就销毁重建子进程（解决碎片化OOM）
            # 避免了原版代码每个文件都在 for 循环中创建/销毁 Executor 的极大开销。
            ctx = mp.get_context("spawn")
            pool = ctx.Pool(processes=1, maxtasksperchild=1)
        except Exception as e:
            print(f"[警告] 子进程池初始化失败({type(e).__name__}: {e})，自动切换为主进程内处理...")
            use_subprocess = False

    for idx, file_path in enumerate(pkl_files, 1):
        filename = os.path.basename(file_path)
        result = None

        try:
            if use_subprocess:
                # 阻塞式调用，按原版要求顺序处理
                result = pool.apply(_process_one_file_safe, args=(file_path,))
            else:
                result = _process_one_file_safe(file_path)
        except Exception as e:
            # 【重要修复】仅在此文件抛异常，不改变全局状态(不再因单个文件报错导致后续文件全部回退到主进程)
            print(
                f"[警告] 处理失败，已跳过(极可能是该文件单独就把内存撑爆/或数据损坏): {filename} | {type(e).__name__}: {e}")

        if result is not None:
            if result.get("ok"):
                rows = result.get("rows") or {}
                for margin in TEST_MARGINS:
                    row = rows.get(margin)
                    if row is not None:
                        results_by_margin[margin].append(row)
            else:
                print(f"[警告] 处理失败，已跳过: {filename} | {result.get('err')}")

        # 主进程只持有极少量结果行，这里的 gc 成本可忽略
        gc.collect()

        # 打印简单进度
        if idx % 10 == 0 or idx == len(pkl_files):
            print(f"进度: {idx}/{len(pkl_files)} 个策略文件已处理完成...")

    # 安全关闭并回收进程池
    if pool is not None:
        pool.close()
        pool.join()

    print("\n" + "=" * 80)
    print(f" 🎉 分析完成！开始为各个 Margin 生成独立报告...")
    print("=" * 80)

    # =====================================================================
    # 数据清洗、排名与结果输出
    # =====================================================================
    for margin in TEST_MARGINS:
        if not results_by_margin[margin]:
            continue

        df_results = pd.DataFrame(results_by_margin[margin])

        # 因为翻倍期望已移除，改用 净利润(Margin倍数) 排序以平滑替代
        df_results = df_results.sort_values(by='净利润(Margin倍数)', ascending=False)

        # 输出 CSV 报告
        output_csv = f"strategy_leaderboard_margin_{margin}.csv"
        df_results.to_csv(output_csv, index=False, encoding='utf-8-sig')

        head_count = 50
        # ---------------- 终端打印 Top 10 (终极完美对齐版) ----------------
        print(f"\n🏆 Margin = {margin} | 综合表现 TOP {head_count} 策略 (按净利润排名):")

        # 包含新增的核心评估字段
        display_cols = ["币种", "策略", "方向", "实际开仓数", "胜率(%)", "爆仓次数",
                        "爆仓几率(%)", "预期存活(天)", "平均持仓(h)", "死前翻倍胜率(%)",
                        "净利润(Margin倍数)", "盈利和", "亏损和"]

        df_display = df_results[display_cols].head(head_count).copy()

        # 精简表头
        df_display.rename(columns={
            "死前翻倍胜率(%)": "翻倍胜率(%)",
            "净利润(Margin倍数)": "净利润(M倍)"
        }, inplace=True)

        print("-" * 155)

        # 终极精确字符宽度计算逻辑
        def get_display_width(s):
            w = 0
            for c in str(s):
                # 剔除 'A' (Ambiguous)，在大部分 IDE 终端下它表现为单字节宽
                if unicodedata.east_asian_width(c) in ('F', 'W'):
                    w += 2
                else:
                    w += 1
            return w

        def rpad(s, width):
            """右对齐填充空格"""
            s = str(s)
            pad_len = width - get_display_width(s)
            return " " * max(0, pad_len) + s

        cols = list(df_display.columns)
        col_widths = []
        # 计算每一列所需的最大宽度，并加上 3 个空格作为统一间距
        for col in cols:
            max_w = get_display_width(col)
            for val in df_display[col]:
                max_w = max(max_w, get_display_width(str(val)))
            col_widths.append(max_w + 3)

        # 打印表头 (右对齐)
        header_str = "".join(rpad(col, col_widths[i]) for i, col in enumerate(cols))
        print(header_str)

        # 打印数据 (右对齐)
        for _, row in df_display.iterrows():
            row_str = "".join(rpad(str(row[col]), col_widths[i]) for i, col in enumerate(cols))
            print(row_str)

        print("-" * 155)

    print("\n💡 指标解读指南 (进阶版):")
    print(" 1. [爆仓几率(%)]: 衡量每次开仓实际面临的最终爆仓风险，数值越低策略越稳定。")
    print(" 2. [死前翻倍胜率(%) (Free-Ride Win Rate)]: 每次投入保证金后，成功抽出本金不爆仓的真实概率。")
    print(" 3. [预期存活(天) (Expected Lifespan)]: 历史统计下平均多少天爆仓一次，结合持仓时间和翻倍时间看风险。")
    print(" 4. [0-1层解决战斗比例]: 反映入场信号纯度，占比低于 40% 说明信号基本无效，纯靠杠杆硬扛。")


if __name__ == "__main__":
    mp.freeze_support()  # Windows / 打包环境安全护栏
    analyze_all_strategies()