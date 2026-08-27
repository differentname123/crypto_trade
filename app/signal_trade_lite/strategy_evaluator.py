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
import gc
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

    print(f"共发现 {len(pkl_files)} 个缓存文件，开启极致单进程内存回收模式...\n")

    # 用于存放每个 Margin 下的结果，格式为 {margin_value: [row1, row2, ...]}
    results_by_margin = {m: [] for m in TEST_MARGINS}

    for idx, file_path in enumerate(pkl_files, 1):
        filename = os.path.basename(file_path)

        # 解析文件名: stage1_BTCUSDT_strategy_1_vwap_zscore_Long_f0.0005...pkl
        try:
            parts = filename.split('_')
            symbol = parts[1]
            direction = "Long" if "_Long_" in filename else "Short" if "_Short_" in filename else "Unknown"
            strat_start_idx = filename.find(symbol) + len(symbol) + 1
            strat_end_idx = filename.find(f"_{direction}_")
            strategy_name = filename[strat_start_idx:strat_end_idx]
        except Exception as e:
            symbol, strategy_name, direction = "Unknown", filename, "Unknown"

        # 1. 加载 Stage 1 数据
        with open(file_path, 'rb') as f:
            cached_data = pickle.load(f)
            cycles_df = cached_data['df']
            cycles_df.attrs = cached_data['attrs']

        # 跳过没有产生任何信号的空策略
        if len(cycles_df) == 0:
            del cycles_df, cached_data
            continue

        # 2. 初始化重组器 (Stage 2)
        replayer = TimelineReplayer(cycles_df)

        # 对配置的每一个 Margin 水位进行时间线测试
        for margin in TEST_MARGINS:
            trades_df = replayer.run(margin)
            report = evaluate_free_ride(trades_df, cycles_df, margin)

            # ==================== 新增更多维度的关键指标 ====================
            doubles_per_blowup = report.get("doubles_per_blowup", np.nan)
            holding_time = report.get("avg_holding_hour_traded", 0.0)
            expected_lifespan_hour = report.get("expected_lifespan_hour", np.inf)
            free_ride_win_rate = report.get("free_ride_win_rate", np.nan)

            row = {
                "币种": symbol,
                "策略": strategy_name,
                "方向": direction,
                "总信号数": report.get("n_cycles_total", 0),
                "实际开仓数": report.get("n_trades", 0),
                "胜率(%)": round(report.get("win_rate", 0) * 100, 2) if pd.notnull(report.get("win_rate")) else 0.0,
                "爆仓次数": report.get("n_blowup", 0),
                "预期存活(天)": round(expected_lifespan_hour / 24.0, 2) if not np.isinf(
                    expected_lifespan_hour) else "999 (未爆仓)",
                "平均持仓(h)": round(holding_time, 2) if pd.notnull(holding_time) else 0.0,
                "死前翻倍胜率(%)": round(free_ride_win_rate * 100, 2) if pd.notnull(free_ride_win_rate) else 0.0,
                "净利润(Margin倍数)": round(report.get("total_net_pnl_in_margin", 0), 2),
                "年化爆仓次数": round(report.get("blowups_per_year", 0), 2),
                "翻倍所需时间(小时)": round(report.get("time_to_double_hour", 0), 2),
                "死前翻倍期望 (Doubles/Blowup)": round(doubles_per_blowup, 3) if not np.isinf(
                    doubles_per_blowup) else "999 (未爆仓)",
                "0-1层解决战斗比例(%)": round(report.get("low_layer_ratio", 0) * 100, 2),
                "手续费占毛利(%)": round(report.get("fee_ratio_traded", 0) * 100, 2)
            }

            results_by_margin[margin].append(row)

            # 清理单次 margin 的局部大对象
            del trades_df, report

        # ==================== 严格的单进程内存回收 ====================
        del cycles_df, cached_data, replayer
        gc.collect()

        # 打印简单进度
        if idx % 10 == 0 or idx == len(pkl_files):
            print(f"进度: {idx}/{len(pkl_files)} 个策略文件已处理完成...")

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

        # 将 "999 (未爆仓)" 替换回数值用于排序
        df_results['sort_score'] = df_results['死前翻倍期望 (Doubles/Blowup)'].replace("999 (未爆仓)", 9999.0).astype(
            float)
        df_results = df_results.sort_values(by='sort_score', ascending=False)
        df_results = df_results.drop(columns=['sort_score'])

        # 输出 CSV 报告
        output_csv = f"strategy_leaderboard_margin_{margin}.csv"
        df_results.to_csv(output_csv, index=False, encoding='utf-8-sig')

        # ---------------- 终端打印 Top 10 (终极完美对齐版) ----------------
        print(f"\n🏆 Margin = {margin} | 综合表现 TOP 10 策略 (按死前翻倍期望排名):")

        # 包含新增的核心评估字段
        display_cols = ["币种", "策略", "方向", "实际开仓数", "胜率(%)", "爆仓次数",
                        "预期存活(天)", "平均持仓(h)", "死前翻倍胜率(%)",
                        "死前翻倍期望 (Doubles/Blowup)", "净利润(Margin倍数)"]

        df_display = df_results[display_cols].head(10).copy()

        # 精简表头
        df_display.rename(columns={
            "死前翻倍胜率(%)": "翻倍胜率(%)",
            "死前翻倍期望 (Doubles/Blowup)": "翻倍期望",
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
    print(" 1. [死前翻倍期望 (Doubles/Blowup)]: 生死线！> 1 表示期望为正，在爆仓前大概率能抽回本金。")
    print(" 2. [死前翻倍胜率(%) (Free-Ride Win Rate)]: 每次投入保证金后，成功抽出本金不爆仓的真实概率。")
    print(" 3. [预期存活(天) (Expected Lifespan)]: 历史统计下平均多少天爆仓一次，结合持仓时间和翻倍时间看风险。")
    print(" 4. [0-1层解决战斗比例]: 反映入场信号纯度，占比低于 40% 说明信号基本无效，纯靠杠杆硬扛。")


if __name__ == "__main__":
    analyze_all_strategies()