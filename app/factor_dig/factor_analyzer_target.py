

import pandas as pd
import numpy as np
import os

# =====================================================================
# 核心配置区 (新增与原有配置)
# =====================================================================
# 过滤条件：每个周期（60m,30m,15m,5m）的交易数量都必须大于该值
MIN_TRADES_PER_TF = 50

# 过滤条件：每个周期（60m,30m,15m,5m）的最大回撤历时占比(%)都不能超过该值
MAX_DRAWDOWN_DURATION_PCT = 20.0

# 过滤条件：每个周期（60m,30m,15m,5m）的平均持仓时间(天)必须小于该值 (新增)
MAX_AVG_HOLDING_DAYS = 60.0

# 过滤条件：每个周期（60m,30m,15m,5m）的 Top3币收益占比(%) 必须小于该值 (新增)
MAX_TOP3_PROFIT_PCT = 60.0

# 过滤条件：每个周期（60m,30m,15m,5m）的 真实盈潜比(Ret/MAE) 必须大于该值 (新增)
MIN_RET_MAE_RATIO = 0.01

# 模糊化信号映射表的保存路径
SIGNAL_MAPPING_FILE = './summary_results/signal_mapping.csv'

long_pair = [
    ("VOLUME_CLIMAX_DOWN", "ENTRY_SILENT_ACCUMULATION"),
    ("EXIT_VOL_EXTREME_DOWN", "ENTRY_SILENT_ACCUMULATION"),
    ("EXIT_FR_ROLL_OVER", "ENTRY_SILENT_ACCUMULATION"),
    ("FR_ABSOLUTE_HIGH_POS", "OI_BOTTOM_DIVERGENCE"),
    ("FR_HIGH_EXTREME", "ENTRY_SILENT_ACCUMULATION"),
    ("MOM_RECOVERY_FROM_LOW", "FR_RECOVERY_FROM_LOW"),
    ("EXIT_OI_ROC_PEAK", "EXIT_MA_DEAD_CROSS"),
    ("OI_NEW_HIGH", "EXIT_MA_DEAD_CROSS"),
    ("ENTRY_OI_LEAD_MOMENTUM", "VOL_DOWN_SPIKE"),
    ("FR_RECOVERY_FROM_LOW", "STRUCT_RANGE_POSITION_WEAK"),
    ("EXIT_SHORT_SURGE_EXTREME", "FR_LOW_NEG"),
    ("STRUCT_SUPPORT_HOLD", "FR_RECOVERY_FROM_LOW"),
    ("FR_ROLL_OVER_FROM_HIGH", "EXIT_DISTRIBUTION_EXHAUSTION_TOP"),
]
def display_pivot_panels(csv_path, top_n=50, target_direction='Long'):
    """
    生成高密度的策略表现二维透视面板 (增加脱敏、多维度展示与交易次数过滤)
    """
    if not os.path.exists(csv_path):
        print(f"❌ 找不到文件: {csv_path}")
        return

    # 1. 加载数据
    df = pd.read_csv(csv_path)

    # 将df 按照 target_direction 过滤
    df = df[df['direction'] == target_direction].copy()

    # ---------------------------------------------------------
    # 🌟 新增功能 2: 信号名称模糊化与保存映射
    # ---------------------------------------------------------
    # 提取所有独特的因子名称并排序以保持一致性
    all_signals = pd.concat([df['entry_factor'], df['exit_factor']]).dropna().unique()
    # 【修复点】：兼容 Pandas 2.0+ 的 PyArrow String Array，使用标准的 sorted + list 转换
    all_signals = sorted(list(all_signals))

    # 构建映射字典 { '原始因子名' : 'SIGNAL_001' }
    signal_mapping = {sig: f"SIGNAL_{i:003d}" for i, sig in enumerate(all_signals, 1)}

    # 保存映射表到 CSV
    mapping_df = pd.DataFrame(list(signal_mapping.items()), columns=['Original_Signal', 'Obfuscated_Signal'])
    os.makedirs(os.path.dirname(SIGNAL_MAPPING_FILE) or '.', exist_ok=True)
    mapping_df.to_csv(SIGNAL_MAPPING_FILE, index=False, encoding='utf-8-sig')

    # 执行替换模糊化
    df['entry_factor'] = df['entry_factor'].map(signal_mapping)
    df['exit_factor'] = df['exit_factor'].map(signal_mapping)

    # ---------------------------------------------------------
    # 2. 识别数据中包含的周期 (例如 60m, 30m, 15m, 5m)
    all_calmar_cols = [c for c in df.columns if '策略赚钱性价比_' in c]
    if not all_calmar_cols:
        print("⚠️ 数据中找不到 '策略赚钱性价比' 相关的列，请检查 CSV。")
        return

    timeframes = [c.split('_')[-1] for c in all_calmar_cols]
    std_order = ['60m', '30m', '15m', '5m']
    timeframes = [tf for tf in std_order if tf in timeframes] + [tf for tf in timeframes if tf not in std_order]

    # 3. 计算跨周期平均性价比，并过滤方向
    df['avg_calmar'] = df[all_calmar_cols].mean(axis=1, skipna=True)
    df_dir = df[df['direction'] == target_direction].copy()

    if df_dir.empty:
        print(f"⚠️ 在数据中没有找到方向为 {target_direction} 的记录。")
        return

    # 4. 提取表现最好的 Top N 个组合 (基于单行最高平均性价比，同时过滤重复表现数据)
    df_sorted = df_dir.sort_values(by='avg_calmar', ascending=False)

    seen_pairs = set()
    seen_signatures = set()
    top_pairs = []

    # 提取所有数值型指标列用于计算策略表现数据指纹
    exclude_cols = {'entry_factor', 'exit_factor', 'direction', 'filter_mode'}
    numeric_cols = [c for c in df_dir.columns if c not in exclude_cols and pd.api.types.is_numeric_dtype(df_dir[c])]

    trade_cols = [c for c in df_dir.columns if '总交易笔数_' in c]
    dd_duration_cols = [c for c in df_dir.columns if '最大回撤历时占比(%)_' in c]
    holding_days_cols = [c for c in df_dir.columns if '平均持仓时间(天)_' in c] # 新增提取持仓时间列
    top3_profit_cols = [c for c in df_dir.columns if 'Top3币收益占比(%)_' in c] # 新增提取Top3币收益占比列
    ret_mae_cols = [c for c in df_dir.columns if '真实盈潜比(Ret/MAE)_' in c] # 新增提取真实盈潜比列

    for _, row in df_sorted.iterrows():
        pair = (row['entry_factor'], row['exit_factor'])
        if pair in seen_pairs:
            continue

        # ========================================================
        # 🚦 修改点：在输出排序结果前，严格校验当前登顶的这行数据是否满足条件
        # ========================================================
        # 1. 验证交易次数 (如果该档位下某些周期没开单导致NaN，视为0次)
        if trade_cols:
            if row[trade_cols].fillna(0).min() <= MIN_TRADES_PER_TF:
                continue  # 当前 filter_mode 不满足条件，跳过，等待该 pair 其他达标的 filter_mode

        # 2. 验证最大回撤历时占比 (如果数据缺失NaN，视为100%直接淘汰)
        if dd_duration_cols:
            if row[dd_duration_cols].fillna(100).max() > MAX_DRAWDOWN_DURATION_PCT:
                continue  # 当前 filter_mode 不满足条件，跳过

        # 3. 验证平均持仓时间(天) (如果数据缺失NaN，视为持仓极长直接淘汰) 新增逻辑
        if holding_days_cols:
            if row[holding_days_cols].fillna(9999).max() >= MAX_AVG_HOLDING_DAYS:
                continue  # 当前 filter_mode 不满足持仓时间小于阈值的条件，跳过

        # 4. 验证Top3币收益占比(%) (如果数据缺失NaN，视为100%直接淘汰) 新增逻辑
        if top3_profit_cols:
            if row[top3_profit_cols].fillna(100).max() >= MAX_TOP3_PROFIT_PCT:
                continue  # 当前 filter_mode 收益过度集中于前3个币，跳过

        # 5. 验证真实盈潜比(Ret/MAE) (如果数据缺失NaN，视为0直接淘汰) 新增逻辑
        if ret_mae_cols:
            if row[ret_mae_cols].fillna(0).min() <= MIN_RET_MAE_RATIO:
                continue  # 当前 filter_mode 盈潜比未达到最低要求，跳过
        # ========================================================

        # 如果通过了上述检验，提取该组合所有过滤模式下的完整矩阵（保留全局统计数据用于绘制面板）
        sub_pair_df = df_dir[(df_dir['entry_factor'] == pair[0]) & (df_dir['exit_factor'] == pair[1])].sort_values(
            'filter_mode')

        # 将排序后的 filter_mode 与数值特征序列化为 tuple（保留4位小数避免浮点误差）
        perf_signature = tuple(
            tuple(sub_pair_df['filter_mode'].tolist()) +
            tuple(np.round(sub_pair_df[numeric_cols].fillna(-999999).to_numpy().flatten(), 4))
        )

        seen_pairs.add(pair)

        # 如果此数据表现已经存在，跳过该重复组合
        if perf_signature in seen_signatures:
            continue

        seen_signatures.add(perf_signature)
        top_pairs.append({
            'entry': row['entry_factor'],
            'exit': row['exit_factor'],
            'best_filter': row['filter_mode'],
            'best_score': row['avg_calmar']
        })
        if len(top_pairs) >= top_n:
            break

    # 5. 定义过滤模式的严格排序（确保横向时间轴的逻辑连贯性）
    FILTER_ORDER = [
        'bottom_100', 'bottom_50', 'bottom_20', 'bottom_10', 'bottom_5', 'bottom_3', 'bottom_1',
        'original',
        'top_1', 'top_3', 'top_5', 'top_10', 'top_20', 'top_50', 'top_100'
    ]

    # 🌟 核心面板绘制函数
    def print_metric_matrix(sub_df, metric_prefix, title, fmt_str="{:.4f}"):
        available_filters = sub_df['filter_mode'].unique()
        # 兼容 PyArrow array 的查询方式
        available_filters_list = list(available_filters)
        sorted_filters = [f for f in FILTER_ORDER if f in available_filters_list]
        sorted_filters += [f for f in available_filters_list if f not in FILTER_ORDER]

        col_widths = {}
        for f in sorted_filters:
            col_widths[f] = len(f)

        for tf in timeframes:
            col_name = f"{metric_prefix}_{tf}"
            if col_name in sub_df.columns:
                for f in sorted_filters:
                    val_series = sub_df[sub_df['filter_mode'] == f][col_name]
                    if not val_series.empty and pd.notna(val_series.values[0]):
                        val_str = fmt_str.format(val_series.values[0])
                    else:
                        val_str = "-"
                    col_widths[f] = max(col_widths[f], len(val_str))

        print(f"\n>> {title}")
        print("-" * 120)

        header = f"{'过滤模式':<6}" + "".join([f.rjust(col_widths[f] + 2) for f in sorted_filters])
        print(header)
        print("周期")

        for tf in timeframes:
            col_name = f"{metric_prefix}_{tf}"
            if col_name not in sub_df.columns:
                continue

            row_str = f"{tf:<8}"
            for f in sorted_filters:
                val_series = sub_df[sub_df['filter_mode'] == f][col_name]
                if not val_series.empty and pd.notna(val_series.values[0]):
                    val_str = fmt_str.format(val_series.values[0])
                else:
                    val_str = "-"
                row_str += val_str.rjust(col_widths[f] + 2)
            print(row_str)

    if not top_pairs:
        print("⚠️ 没有符合过滤条件（交易次数、回撤历时、持仓时间、Top3收益占比及真实盈潜比）的策略组合可以输出。")
        return

    for rank, pair_info in enumerate(top_pairs, 1):
        entry = pair_info['entry']
        exit_factor = pair_info['exit']
        best_f = pair_info['best_filter']
        best_score = pair_info['best_score']

        sub_df = df_dir[(df_dir['entry_factor'] == entry) & (df_dir['exit_factor'] == exit_factor)]

        print("\n" + "=" * 120)
        print(f" 📊 策略表现透视面板 | 方向: {target_direction} | 入场: {entry} | 出场: {exit_factor}")
        print(f" 组合编号: #{rank} (当前面板最佳且达标过滤档位: {best_f} , 平均性价比: {best_score:.4f})")

        # 🌟 高密度透视面板输出多个指定维度 (定制不同的数字格式)
        print_metric_matrix(sub_df, "总真实净收益(%)", "🎯 【净利润(%)】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "总交易笔数", "📝 【总交易笔数】 横向截面对比", "{:.0f}")
        print_metric_matrix(sub_df, "单笔净期望(%)", "💰 【单笔净收益 / 单笔净期望(%)】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "策略赚钱性价比", "⚡ 【策略性价比 (收益风险比)】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "最大回撤历时占比(%)", "⚡ 【最大回撤历时占比(%)】 横向截面对比", "{:.4f}")

        print("=" * 120)

# =====================================================================
# 新增功能区：专门用于输出指定组合面板的独立函数（无任何过滤条件）
# =====================================================================
def display_specific_pairs_panels(csv_path, target_pairs, target_direction='Long'):
    """
    针对给定的特定因子组合列表，单独输出它们的高密度透视面板（自动适应脱敏映射，且不附加任何过滤条件）
    """
    if not os.path.exists(csv_path):
        print(f"❌ 找不到文件: {csv_path}")
        return

    # 1. 加载数据与过滤方向
    df = pd.read_csv(csv_path)
    df = df[df['direction'] == target_direction].copy()

    # 执行与原代码完全一致的因子脱敏逻辑，以便正常读取 DataFrame
    all_signals = pd.concat([df['entry_factor'], df['exit_factor']]).dropna().unique()
    all_signals = sorted(list(all_signals))
    signal_mapping = {sig: f"SIGNAL_{i:003d}" for i, sig in enumerate(all_signals, 1)}

    df['entry_factor'] = df['entry_factor'].map(signal_mapping)
    df['exit_factor'] = df['exit_factor'].map(signal_mapping)

    # 2. 识别数据中包含的周期
    all_calmar_cols = [c for c in df.columns if '策略赚钱性价比_' in c]
    if not all_calmar_cols:
        print("⚠️ 数据中找不到 '策略赚钱性价比' 相关的列，请检查 CSV。")
        return

    timeframes = [c.split('_')[-1] for c in all_calmar_cols]
    std_order = ['60m', '30m', '15m', '5m']
    timeframes = [tf for tf in std_order if tf in timeframes] + [tf for tf in timeframes if tf not in std_order]

    df['avg_calmar'] = df[all_calmar_cols].mean(axis=1, skipna=True)
    df_dir = df[df['direction'] == target_direction].copy()

    if df_dir.empty:
        print(f"⚠️ 在数据中没有找到方向为 {target_direction} 的记录。")
        return

    FILTER_ORDER = [
        'bottom_100', 'bottom_50', 'bottom_20', 'bottom_10', 'bottom_5', 'bottom_3', 'bottom_1',
        'original',
        'top_1', 'top_3', 'top_5', 'top_10', 'top_20', 'top_50', 'top_100'
    ]

    # 面板绘制函数(与原函数严格保持一致)
    def print_metric_matrix(sub_df, metric_prefix, title, fmt_str="{:.4f}"):
        available_filters = sub_df['filter_mode'].unique()
        available_filters_list = list(available_filters)
        sorted_filters = [f for f in FILTER_ORDER if f in available_filters_list]
        sorted_filters += [f for f in available_filters_list if f not in FILTER_ORDER]

        col_widths = {}
        for f in sorted_filters:
            col_widths[f] = len(f)

        for tf in timeframes:
            col_name = f"{metric_prefix}_{tf}"
            if col_name in sub_df.columns:
                for f in sorted_filters:
                    val_series = sub_df[sub_df['filter_mode'] == f][col_name]
                    if not val_series.empty and pd.notna(val_series.values[0]):
                        val_str = fmt_str.format(val_series.values[0])
                    else:
                        val_str = "-"
                    col_widths[f] = max(col_widths[f], len(val_str))

        print(f"\n>> {title}")
        print("-" * 120)
        header = f"{'过滤模式':<6}" + "".join([f.rjust(col_widths[f] + 2) for f in sorted_filters])
        print(header)
        print("周期")

        for tf in timeframes:
            col_name = f"{metric_prefix}_{tf}"
            if col_name not in sub_df.columns:
                continue

            row_str = f"{tf:<8}"
            for f in sorted_filters:
                val_series = sub_df[sub_df['filter_mode'] == f][col_name]
                if not val_series.empty and pd.notna(val_series.values[0]):
                    val_str = fmt_str.format(val_series.values[0])
                else:
                    val_str = "-"
                row_str += val_str.rjust(col_widths[f] + 2)
            print(row_str)

    # 3. 遍历指定列表进行输出
    # print(f"\n🔥 开始生成指定 {len(target_pairs)} 个组合的专属报告 (无任何前置过滤)...")
    for rank, (orig_entry, orig_exit) in enumerate(target_pairs, 1):
        # 查询映射表
        mapped_entry = signal_mapping.get(orig_entry)
        mapped_exit = signal_mapping.get(orig_exit)

        if not mapped_entry or not mapped_exit:
            print(f"\n⚠️ 组合 #{rank} [{orig_entry}, {orig_exit}] 在数据集中未找到映射记录，已跳过。")
            continue

        # 提取当前组合的所有数据
        sub_df = df_dir[(df_dir['entry_factor'] == mapped_entry) & (df_dir['exit_factor'] == mapped_exit)].sort_values('filter_mode')

        if sub_df.empty:
            print(f"\n⚠️ 组合 #{rank} [{orig_entry}, {orig_exit}] (方向:{target_direction}) 在该方向无数据，已跳过。")
            continue

        # 因为要求无任何过滤，所以直接找平均性价比最高的一条作为最佳提示（不包含任何次数、回撤校验）
        sub_df_sorted = sub_df.sort_values(by='avg_calmar', ascending=False)
        best_f = sub_df_sorted.iloc[0]['filter_mode']
        best_score = sub_df_sorted.iloc[0]['avg_calmar']

        # 打印面板
        print("\n" + "=" * 120)
        print(f" 🎯 专属组合透视面板 | 方向: {target_direction}")
        # print(f" 原始因子组合: 入场 [{orig_entry}] -> 出场 [{orig_exit}]")
        print(f" 脱敏映射编号: 入场 ({mapped_entry}) -> 出场 ({mapped_exit})")
        # print(f" 列表进度编号: #{rank} (性价比最高档位: {best_f} , 对应平均性价比: {best_score:.4f})")

        print_metric_matrix(sub_df, "总真实净收益(%)", "🎯 【净利润(%)】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "总交易笔数", "📝 【总交易笔数】 横向截面对比", "{:.0f}")
        print_metric_matrix(sub_df, "单笔净期望(%)", "💰 【单笔净收益 / 单笔净期望(%)】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "跨币种胜率(%)", "💰 【跨币种胜率(%))】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "均值单笔回撤(%)", "💰 【均值单笔回撤(%))】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "平均持仓时间(天)", "💰 【平均持仓时间(天)】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "持仓时间中位数(天)", "💰 【持仓时间中位数(天))】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "平均资金暴露度(%)", "💰 【平均资金暴露度(%))】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "Top3币收益占比(%)", "💰 【Top3币收益占比(%)】 横向截面对比", "{:.4f}")



        print_metric_matrix(sub_df, "策略赚钱性价比", "⚡ 【策略性价比 (收益风险比)】 横向截面对比", "{:.4f}")
        print_metric_matrix(sub_df, "最大回撤历时占比(%)", "⚡ 【最大回撤历时占比(%)】 横向截面对比", "{:.4f}")

        print("=" * 120)


if __name__ == "__main__":
    print(f"{'=' * 90}")
    print("💡 【数据维度说明】:")
    print(" 🔹 周期 (Timeframe) : K线图的时间粒度 (如 60m=1小时线, 5m=5分钟线)。直观反映策略在不同级别趋势中的适应性。")
    print(" 🔹 过滤模式 (Filter): 基于过去24小时涨跌幅的截面选币过滤机制。")
    print("      - bottom_N : 仅在跌幅最大（排名垫底）的前 N 个币种上允许开仓。")
    print("      - original : 原始状态，不对币种进行任何截面过滤，全市场轮动。")
    print("      - top_N    : 仅在涨幅最大（排名靠前）的前 N 个币种上允许开仓。")
    print("      所有利润 或者 胜率都是完全考虑了滑点 资金费率之后的数据 回测没有用到任何的未来函数 回测标的的流动性也没有任何问题")
    print(f"{'=' * 90}")

    # 替换为你实际的大宽表路径
    TARGET_CSV = './summary_results/advanced_summary_combined_ALL.csv'

    # ==== 打印指定列表，无任何过滤条件 ====
    display_specific_pairs_panels(csv_path=TARGET_CSV, target_pairs=long_pair, target_direction='Long')

    # 如果需要恢复原来的输出 Top N 功能，取消下面一行的注释即可
    # display_pivot_panels(csv_path=TARGET_CSV, top_n=50, target_direction='Long')