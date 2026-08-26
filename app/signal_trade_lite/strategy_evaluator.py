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

from app.signal_trade_lite.martin_strategy_backest import TimelineReplayer, evaluate_free_ride

# =====================================================================
# 参数配置
# =====================================================================
# 缓存文件所在的目录 (请与你生成数据时保持一致)
CACHE_DIR = "./backtest"  # 如果在子目录，请修改，例如 "./backtest/"

# 回测测试用的保证金深度 (Margin)
# 单位：首单名义价值的倍数。2.55 约等于能扛 10 层，10.0 约等于能扛 15 层。
TEST_MARGIN = 2.55

# 结果输出的 CSV 文件名
OUTPUT_CSV = f"strategy_leaderboard_margin_{TEST_MARGIN}.csv"


def analyze_all_strategies():
    print("=" * 80)
    print(f" 🚀 启动全局策略评估引擎 | 设定测试 Margin = {TEST_MARGIN}")
    print("=" * 80)

    # 查找所有 stage1 缓存文件
    search_pattern = os.path.join(CACHE_DIR, "stage1_*.pkl")
    pkl_files = glob.glob(search_pattern)

    if not pkl_files:
        print(f"[错误] 在 {CACHE_DIR} 目录下未找到任何 stage1_*.pkl 文件！")
        return

    print(f"共发现 {len(pkl_files)} 个缓存文件，开始逐一重组与评估...\n")

    results = []

    for idx, file_path in enumerate(pkl_files, 1):
        filename = os.path.basename(file_path)

        # 解析文件名: stage1_BTCUSDT_strategy_1_vwap_zscore_Long_f0.0005...pkl
        # 为了提取 币种、策略名、方向，我们需要对文件名进行分割
        try:
            parts = filename.split('_')
            symbol = parts[1]
            # 方向通常在参数 f0.0005 前面一个位置
            direction = "Long" if "_Long_" in filename else "Short" if "_Short_" in filename else "Unknown"

            # 提取策略名称 (去掉 stage1_、币种、方向和后面的参数)
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
            continue

        # 2. 初始化重组器 (Stage 2)
        replayer = TimelineReplayer(cycles_df)

        # 3. 运行重组器，生成真实交易时间线
        trades_df = replayer.run(TEST_MARGIN)

        # 4. 获取评估报告 (Stage 3)
        report = evaluate_free_ride(trades_df, cycles_df, TEST_MARGIN)

        # 5. 提取我们最关心的"关键指标"
        # ---- 核心生存指标：爆仓前能翻倍几次？(>1 才有长期数学期望) ----
        doubles_per_blowup = report.get("doubles_per_blowup", np.nan)

        row = {
            "币种": symbol,
            "策略": strategy_name,
            "方向": direction,
            "总信号数": report.get("n_cycles_total", 0),
            "实际开仓数": report.get("n_trades", 0),
            "胜率(%)": round(report.get("win_rate", 0) * 100, 2),
            "爆仓次数": report.get("n_blowup", 0),
            "净利润(Margin倍数)": round(report.get("total_net_pnl_in_margin", 0), 2),
            "年化爆仓次数": round(report.get("blowups_per_year", 0), 2),
            "翻倍所需时间(小时)": round(report.get("time_to_double_hour", 0), 2),
            "死前翻倍期望 (Doubles/Blowup)": round(doubles_per_blowup, 3) if not np.isinf(
                doubles_per_blowup) else "999 (未爆仓)",
            "0-1层解决战斗比例(%)": round(report.get("low_layer_ratio", 0) * 100, 2),
            "手续费占毛利(%)": round(report.get("fee_ratio_traded", 0) * 100, 2)
        }

        results.append(row)

        # 打印简单进度
        if idx % 10 == 0 or idx == len(pkl_files):
            print(f"进度: {idx}/{len(pkl_files)} 文件已处理完成...")

    # 转化为 DataFrame
    df_results = pd.DataFrame(results)

    # =====================================================================
    # 数据清洗与排名逻辑
    # =====================================================================
    # 将 "999 (未爆仓)" 替换回数值用于排序，排完序再格式化回去
    df_results['sort_score'] = df_results['死前翻倍期望 (Doubles/Blowup)'].replace("999 (未爆仓)", 9999.0).astype(float)

    # 按照 "死前翻倍期望" 降序排列 (数值越大越好)
    df_results = df_results.sort_values(by='sort_score', ascending=False)
    df_results = df_results.drop(columns=['sort_score'])

    # 保存为 CSV
    df_results.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')

    print("\n" + "=" * 80)
    print(f" 🎉 分析完成！共评估 {len(df_results)} 个策略维度。")
    print(f" 📊 完整排行榜已保存至: {OUTPUT_CSV}")
    print("=" * 80)

    # 在控制台打印 Top 10 最优策略
    print("\n🏆 综合表现 TOP 10 策略 (按死前翻倍期望排名):")

    # 选择要在控制台展示的列
    display_cols = ["币种", "策略", "方向", "实际开仓数", "胜率(%)", "爆仓次数", "死前翻倍期望 (Doubles/Blowup)",
                    "净利润(Margin倍数)"]

    # 格式化输出 (借助 pandas自带的表格功能)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 150)
    pd.set_option('display.unicode.east_asian_width', True)  # 对齐中文字符

    print("-" * 120)
    print(df_results[display_cols].head(10).to_string(index=False))
    print("-" * 120)

    print("\n💡 指标解读指南:")
    print(" 1. [死前翻倍期望 (Doubles/Blowup)]: 马丁策略的生死线！")
    print("    - 值 > 1 : 期望为正。在爆仓前，你大概率能把本金抽出来，长期跑是赚钱的。")
    print("    - 值 < 1 : 期望为负。还没翻倍抽出本金就爆仓了，长期必亏。")
    print("    - 999  : 圣杯区间(样本内未发生爆仓)，需要警惕是否是拟合或测试时间太短。")
    print(" 2. [0-1层解决战斗比例]: 反映了入场信号的纯度，如果占比低于 40%，说明信号无效，纯靠杠杆硬扛。")
    print(" 3. [净利润(Margin倍数)]: 1.5 代表你赚了 1.5 个 Margin(爆仓容忍本金) 的钱。")


if __name__ == "__main__":
    analyze_all_strategies()