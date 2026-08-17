import os
import glob
import pandas as pd
import numpy as np
import datetime

# =====================================================================
# 配置区
# =====================================================================
# 新增：你指定的需要单独提取的特定做空组合
short_pair = [
    ("FR_TURN_NEGATIVE", "FR_RESET_AFTER_HOT", "60m", "bottom_100"),
    ('EXIT_MA_DEAD_CROSS', 'PRICE_CLOSE_CROSS_MA_UP', '5m', 'bottom_10'),
    ('REGIME_POWDER_KEG', 'FR_HIGH_EXTREME', '15m', 'bottom_10'),
    ("ENTRY_HIGH_PRESSURE_OI_BREAKOUT", "FR_HIGH_EXTREME", "30m", "top_50"),
    ('EXIT_FR_EXTREME_HIGH', 'FR_COLD_START', '30m', 'top_1'),
    ('FR_ABSOLUTE_DEEP_NEG', 'FR_TURN_POSITIVE', '5m', 'top_20'),
    ('VOLUME_CONFIRM_BREAK', 'FR_COLD_START', '5m', 'top_1'),
]

# 你指定的需要单独提取的特定组合 (进场信号, 出场信号, 周期, 过滤条件)
long_pair = [
    ('VOLUME_CLIMAX_DOWN', 'ENTRY_SILENT_ACCUMULATION', '30m', 'bottom_3'),
    ('EXIT_VOL_EXTREME_DOWN', 'ENTRY_SILENT_ACCUMULATION', '30m', 'bottom_3'),
    ('EXIT_FR_ROLL_OVER', 'ENTRY_SILENT_ACCUMULATION', '15m', 'bottom_10'),
    ('FR_ABSOLUTE_HIGH_POS', 'OI_BOTTOM_DIVERGENCE', '15m', 'bottom_50'),
    ('FR_HIGH_EXTREME', 'ENTRY_SILENT_ACCUMULATION', '30m', 'top_5'),
    ('MOM_RECOVERY_FROM_LOW', 'FR_RECOVERY_FROM_LOW', '30m', 'bottom_5'),
    ('EXIT_OI_ROC_PEAK', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_10'),
    ('OI_NEW_HIGH', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('ENTRY_OI_LEAD_MOMENTUM', 'VOL_DOWN_SPIKE', '5m', 'top_10'),
    ('FR_RECOVERY_FROM_LOW', 'STRUCT_RANGE_POSITION_WEAK', '60m', 'top_1'),
    ('EXIT_SHORT_SURGE_EXTREME', 'FR_LOW_NEG', '30m', 'bottom_5'),
    ('STRUCT_SUPPORT_HOLD', 'FR_RECOVERY_FROM_LOW', '30m', 'top_5'),
    ('FR_ROLL_OVER_FROM_HIGH', 'EXIT_DISTRIBUTION_EXHAUSTION_TOP', '15m', 'top_20'),
]

# 原始数据存放目录
INPUT_DIRS_MAP = {
    '60m': './factor_out_60m_debugtest',
    '30m': './factor_out_30m_debugtest',
    '15m': './factor_out_15m_debugtest',
    '5m': './factor_out_5m_debugtest'
}

# 提取出的原始交易记录保存目录
OUTPUT_DIR = './extracted_raw_trades'


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_extracted_records = []

    print(f"🎯 开始提取指定的 {len(long_pair)} 个多头组合与 {len(short_pair)} 个空头组合...")

    # 1. 为了提高检索效率，按周期 (Timeframe) 对指定的 pair 进行分组，并附加上对应的多空方向
    pairs_by_tf = {}

    # 录入多头组合
    for entry_sig, exit_sig, tf, filter_cond in long_pair:
        if tf not in pairs_by_tf:
            pairs_by_tf[tf] = []
        pairs_by_tf[tf].append((entry_sig, exit_sig, filter_cond, 'Long'))

    # 录入空头组合
    for entry_sig, exit_sig, tf, filter_cond in short_pair:
        if tf not in pairs_by_tf:
            pairs_by_tf[tf] = []
        pairs_by_tf[tf].append((entry_sig, exit_sig, filter_cond, 'Short'))

    # 2. 按周期遍历对应的文件夹读取数据
    for tf, tf_pairs in pairs_by_tf.items():
        input_dir = INPUT_DIRS_MAP.get(tf)

        if not input_dir or not os.path.exists(input_dir):
            print(f"⚠️ 找不到 {tf} 对应的目录: {input_dir}，跳过该周期提取...")
            continue

        file_pattern = os.path.join(input_dir, 'trades_*.csv.gz')
        trade_files = glob.glob(file_pattern)

        if not trade_files:
            print(f"⚠️ {input_dir} 下未找到 trades_*.csv.gz 文件，跳过...")
            continue

        print(f"\n🔍 正在处理 {tf} 数据，目标组合数: {len(tf_pairs)}，扫描文件数: {len(trade_files)}")

        matched_count_tf = 0

        # 3. 逐个文件读取并立即执行掩码过滤 (极致节省内存)
        for f in trade_files:
            try:
                df = pd.read_csv(f)

                # 初始化一个全 False 的布尔掩码
                mask = pd.Series(False, index=df.index)

                # 遍历当前周期需要提取的组合条件，执行 OR (或) 操作
                for entry_sig, exit_sig, filter_cond, direction in tf_pairs:
                    # 注意：此处加入了对 direction 的过滤，以准确区分多空信号
                    current_mask = (
                            (df['entry_factor'] == entry_sig) &
                            (df['exit_factor'] == exit_sig) &
                            (df['filter_mode'] == filter_cond) &
                            (df['direction'] == direction)
                    )
                    mask = mask | current_mask

                # 获取命中的数据
                df_filtered = df[mask].copy()

                if not df_filtered.empty:
                    df_filtered['timeframe'] = tf  # 增加一个标识列，表明来源周期
                    all_extracted_records.append(df_filtered)
                    matched_count_tf += len(df_filtered)

            except Exception as e:
                print(f"❌ 读取或过滤 {f} 时出错: {e}")

        print(f"✅ {tf} 周期扫描完成，共提取到 {matched_count_tf} 笔匹配的交易记录。")

    # =====================================================================
    # 结果整合与基础数据复原计算
    # =====================================================================
    if not all_extracted_records:
        print("\n📭 没有提取到任何匹配的交易记录，请检查指定的组合名或目录数据。")
        return

    # 拼接所有被选中的数据
    final_df = pd.concat(all_extracted_records, ignore_index=True)

    # 统一转换时间格式并计算附加字段（还原策略测算时的字段）
    final_df['entry_time'] = pd.to_datetime(final_df['entry_time'])
    final_df['exit_time'] = pd.to_datetime(final_df['exit_time'])

    # 计算净收益和持仓时间
    is_long = final_df['direction'] == 'Long'
    final_df['fr_impact'] = np.where(is_long, -final_df['fr_sum'], final_df['fr_sum'])
    final_df['net_return'] = final_df['return'] + final_df['fr_impact']
    final_df['hold_time_h'] = (final_df['exit_time'] - final_df['entry_time']).dt.total_seconds() / 3600.0

    # 为了方便人工核查，按周期、进场信号、出场信号、入场时间进行排序
    final_df.sort_values(by=['timeframe', 'entry_factor', 'exit_factor', 'filter_mode', 'entry_time'], inplace=True)

    # 导出文件（文件名修改为 target_pairs 以包含多空双向）
    out_file = os.path.join(OUTPUT_DIR, f'extracted_target_pairs.csv')

    final_df.to_csv(out_file, index=False, encoding='utf-8-sig', float_format="%.6f")
    print(f"\n🎉 提取完毕！总共提取到 {len(final_df)} 笔交易。")
    print(f"📁 详细交易记录已保存至: {os.path.abspath(out_file)}")


if __name__ == "__main__":
    main()