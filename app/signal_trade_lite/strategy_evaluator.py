# -*- coding: utf-8 -*-
"""
======================================================================
加密货币马丁格尔策略 —— 全局表现分析与排名引擎
======================================================================
功能:
1. 批量读取 Stage 1 生成的 .pkl 缓存文件 (包含平行宇宙 cycles_df)。
2. 使用指定的 Margin (保证金深度) 进行 Stage 2 时间线重组。
3. 调用 Stage 3 计算核心评估指标。
4. 将所有策略(不同币种、不同信号、多空方向)的表现汇总成 DataFrame，
   并按照"核心生存指标"进行排名，最终输出 CSV 报告。
"""

import os
import glob
import pickle
import numpy as np
import pandas as pd
import gc
import unicodedata
from concurrent.futures import ProcessPoolExecutor, as_completed

from app.signal_trade_lite.martin_strategy_backest import TimelineReplayer, evaluate_free_ride

# =====================================================================
# 参数配置
# =====================================================================
# 缓存文件所在的目录 (请与你生成数据时保持一致)
CACHE_DIR = "./backest/"  # 如果在子目录，请修改，例如 "./backtest/"

# 回测测试用的保证金深度 (Margin) 列表
# 分别约对应: 5层(0.16), 7层(0.6), 10层(2.55), 11层濒死(10.0), 13层(40.6)
TEST_MARGINS = [0.16, 0.6, 2.55, 10.0, 40.6]


def _process_single_file(file_path, test_margins):
    """独立进程工作函数：处理单个文件，释放多核算力，防止内存泄漏"""
    filename = os.path.basename(file_path)

    try:
        parts = filename.split('_')
        symbol = parts[1]
        direction = "Long" if "_Long_" in filename else "Short" if "_Short_" in filename else "Unknown"
        strat_start_idx = filename.find(symbol) + len(symbol) + 1
        strat_end_idx = filename.find(f"_{direction}_")
        strategy_name = filename[strat_start_idx:strat_end_idx]
    except Exception as e:
        symbol, strategy_name, direction = "Unknown", filename, "Unknown"

    with open(file_path, 'rb') as f:
        cached_data = pickle.load(f)
        cycles_df = cached_data['df']
        cycles_df.attrs = cached_data['attrs']

    if len(cycles_df) == 0:
        return []

    replayer = TimelineReplayer(cycles_df)
    results = []

    for margin in test_margins:
        trades_df = replayer.run(margin)
        report = evaluate_free_ride(trades_df, cycles_df, margin)

        doubles_per_blowup = report.get("doubles_per_blowup", np.nan)
        # 提取关键信息：平均持仓时间
        holding_time = report.get("avg_holding_hour_traded", 0.0)

        row = {
            "Margin": margin,
            "币种": symbol,
            "策略": strategy_name,
            "方向": direction,
            "总信号数": report.get("n_cycles_total", 0),
            "实际开仓数": report.get("n_trades", 0),
            "胜率(%)": round(report.get("win_rate", 0) * 100, 2) if report.get("win_rate") else 0.0,
            "爆仓次数": report.get("n_blowup", 0),
            "平均持仓(h)": round(holding_time, 2) if pd.notnull(holding_time) else 0.0,
            "净利润(Margin倍数)": round(report.get("total_net_pnl_in_margin", 0), 2),
            "年化爆仓次数": round(report.get("blowups_per_year", 0), 2),
            "翻倍所需时间(小时)": round(report.get("time_to_double_hour", 0), 2),
            "死前翻倍期望 (Doubles/Blowup)": round(doubles_per_blowup, 3) if not np.isinf(
                doubles_per_blowup) else "999 (未爆仓)",
            "0-1层解决战斗比例(%)": round(report.get("low_layer_ratio", 0) * 100, 2),
            "手续费占毛利(%)": round(report.get("fee_ratio_traded", 0) * 100, 2)
        }
        results.append(row)

    # 主动释放大块内存
    del cycles_df, cached_data, replayer, trades_df
    gc.collect()
    return results


def analyze_all_strategies():
    print("=" * 80)
    print(f" 🚀 启动全局策略评估引擎 | 设定测试 Margins = {TEST_MARGINS}")
    print("=" * 80)

    # 查找所有 stage1 缓存文件
    search_pattern = os.path.join(CACHE_DIR, "stage1_*.pkl")
    pkl_files = glob.glob(search_pattern)

    if not pkl_files:
        print(f"[错误] 在 {CACHE_DIR} 目录下未找到任何 stage1_*.pkl 文件！")
        return

    print(f"共发现 {len(pkl_files)} 个缓存文件，开启多进程并发计算...\n")

    # 用于存放每个 Margin 下的结果，格式为 {margin_value: [row1, row2, ...]}
    results_by_margin = {m: [] for m in TEST_MARGINS}

    # ========================== 修改点 1: 多进程加速 ==========================
    max_workers = min(os.cpu_count() or 4, len(pkl_files))

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_single_file, fp, TEST_MARGINS): fp for fp in pkl_files}

        processed_count = 0
        for future in as_completed(futures):
            res = future.result()
            for row in res:
                # 把附带的 Margin 剥离，将行插入相应的归属组
                margin = row.pop("Margin")
                results_by_margin[margin].append(row)

            processed_count += 1
            if processed_count % 10 == 0 or processed_count == len(pkl_files):
                print(f"进度: {processed_count}/{len(pkl_files)} 个策略文件已处理完成...")

    print("\n" + "=" * 80)
    print(f" 🎉 分析完成！开始为各个 Margin 生成独立报告...")
    print("=" * 80)

    # =====================================================================
    # 数据清洗、排名与结果输出
    # =====================================================================
    # 设置 pandas 控制台打印对齐参数，确保中文等宽
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 180)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.unicode.ambiguous_as_wide', True)

    for margin in TEST_MARGINS:
        if not results_by_margin[margin]:
            continue

        # 转化为 DataFrame
        df_results = pd.DataFrame(results_by_margin[margin])

        # 将 "999 (未爆仓)" 替换回数值用于排序，排完序再格式化回去
        df_results['sort_score'] = df_results['死前翻倍期望 (Doubles/Blowup)'].replace("999 (未爆仓)", 9999.0).astype(
            float)

        # 按照 "死前翻倍期望" 降序排列 (数值越大越好)
        df_results = df_results.sort_values(by='sort_score', ascending=False)
        df_results = df_results.drop(columns=['sort_score'])

        # 动态生成当前 Margin 的 CSV 文件名
        output_csv = f"strategy_leaderboard_margin_{margin}.csv"
        df_results.to_csv(output_csv, index=False, encoding='utf-8-sig')

        # ---------------- 终端打印 Top 10 (优化排版版) ----------------
        print(f"\n🏆 Margin = {margin} | 综合表现 TOP 10 策略 (按死前翻倍期望排名):")

        # ========================== 修改点 2: 增加持仓统计字段 ==========================
        display_cols = ["币种", "策略", "方向", "实际开仓数", "胜率(%)", "爆仓次数", "平均持仓(h)",
                        "死前翻倍期望 (Doubles/Blowup)",
                        "净利润(Margin倍数)"]

        # 为了让终端严格对齐，临时对表头进行缩略重命名，不影响生成的 CSV
        df_display = df_results[display_cols].head(10).copy()
        df_display.rename(columns={
            "死前翻倍期望 (Doubles/Blowup)": "翻倍期望",
            "净利润(Margin倍数)": "净利润(M倍)"
        }, inplace=True)

        print("-" * 135)

        # ========================== 修改点 3: 强制完美对齐算法 ==========================
        # 弃用 pandas 的 to_string (防止截图上的错位现象)，使用真实东亚字符宽度渲染
        def get_display_width(s):
            w = 0
            for c in str(s):
                # 'F'全角, 'W'宽字(汉字), 'A'模棱两可均按双字节对待
                if unicodedata.east_asian_width(c) in ('F', 'W', 'A'):
                    w += 2
                else:
                    w += 1
            return w

        cols = list(df_display.columns)
        col_widths = []
        for col in cols:
            max_w = get_display_width(col)
            for val in df_display[col]:
                max_w = max(max_w, get_display_width(val))
            col_widths.append(max_w)

        # 打印表头 (右对齐)
        header_str = ""
        for i, col in enumerate(cols):
            pad = col_widths[i] - get_display_width(col)
            header_str += " " * pad + col + "  "
        print(header_str)

        # 打印数据 (右对齐)
        for _, row in df_display.iterrows():
            row_str = ""
            for i, col in enumerate(cols):
                val_str = str(row[col])
                pad = col_widths[i] - get_display_width(val_str)
                row_str += " " * pad + val_str + "  "
            print(row_str)
        # =======================================================================
        print("-" * 135)

    print("\n💡 指标解读指南:")
    print(" 1. [死前翻倍期望 (Doubles/Blowup)]: 马丁策略的生死线！")
    print("    - 值 > 1 : 期望为正。在爆仓前，你大概率能把本金抽出来，长期跑是赚钱的。")
    print("    - 值 < 1 : 期望为负。还没翻倍抽出本金就爆仓了，长期必亏。")
    print("    - 999  : 圣杯区间(样本内未发生爆仓)，需要警惕是否是拟合或测试时间太短。")
    print(" 2. [0-1层解决战斗比例]: 反映了入场信号的纯度，如果占比低于 40%，说明信号无效，纯靠杠杆硬扛。")
    print(" 3. [净利润(Margin倍数)]: 1.5 代表你赚了 1.5 个 Margin(爆仓容忍本金) 的钱。")


if __name__ == "__main__":
    analyze_all_strategies()