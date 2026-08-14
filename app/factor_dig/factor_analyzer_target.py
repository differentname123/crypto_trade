import os
import pandas as pd


def show_strategy_multi_timeframe(entry_factor, exit_factor, direction,
                                  data_path="summary_results/summary_all_intervals_combined.csv"):
    """
    展示指定策略组合在各个周期下的多维关键指标对比。
    采用“指标透视”视角，将不同的【过滤模式(截面涨跌幅)】横向平铺对比，一目了然。
    """
    if not os.path.exists(data_path):
        print(f"❌ 找不到数据文件: {data_path}，请先运行 aggregate_results() 生成。")
        return

    # 1. 读取大表
    df = pd.read_csv(data_path)

    # 2. 筛选指定条件
    mask = (
            (df['方向'] == direction) &
            (df['入场信号名称'] == entry_factor) &
            (df['出场信号名称'] == exit_factor)
    )
    filtered_df = df[mask]

    if filtered_df.empty:
        print(f"⚠️ 未找到匹配的记录: 方向={direction}, 入场={entry_factor}, 出场={exit_factor}")
        return

    # ================= 增加：前置字段含义说明 =================
    print(f"\n{'=' * 90}")
    print(f" 📊 策略表现透视面板 | 方向: {direction} | 入场: {entry_factor} | 出场: {exit_factor}")

    # 3. 定义我们需要展示的核心指标
    target_metrics = [
        '净利润(%)',
        '平均胜率(%)',
        '单笔平均净利润(%)',
        '总交易笔数'
    ]

    # 4. 解析宽表，转化为长表以便透视
    records = []
    for _, row in filtered_df.iterrows():
        f_mode = str(row['过滤模式']).lower()  # 统一转小写防止大小写不一致
        for col_name, val in row.items():
            if col_name.startswith('[') and ']' in col_name:
                interval = col_name.split(']')[0][1:]
                metric = col_name.split(']')[1].strip()

                if metric in target_metrics:
                    records.append({
                        '周期': interval,
                        '过滤模式': f_mode,
                        '指标': metric,
                        '数值': val
                    })

    if not records:
        print("⚠️ 未提取到有效的指标数据。")
        return

    long_df = pd.DataFrame(records)

    # 5. 定义【过滤模式】的自定义排序函数
    def custom_sort_key(col_name):
        """
        将过滤模式映射为数字以实现正确排序:
        bottom_100 -> -100
        original   -> 0
        top_50     -> 50
        """
        name = str(col_name).lower()
        if name == 'original':
            return 0
        elif name.startswith('bottom_'):
            try:
                # 提取数字并转为负数，数字越大排名越靠前 (如 -100 < -50)
                return -int(name.split('_')[1])
            except ValueError:
                return -0.1
        elif name.startswith('top_'):
            try:
                # 提取数字并转为正数，数字越小越靠前 (在 original 之后，如 5 < 10)
                return int(name.split('_')[1])
            except ValueError:
                return 0.1
        else:
            return 999  # 未知格式放在最后边

    # 6. 依次生成透视表
    for metric in target_metrics:
        metric_df = long_df[long_df['指标'] == metric]

        # 透视：行=周期，列=过滤模式，值=具体数值
        pivot_df = metric_df.pivot(index='周期', columns='过滤模式', values='数值')

        # 排序：固定周期的展示顺序 (行)
        standard_intervals = ['60m', '30m', '15m', '5m', '1m']
        actual_rows = [tf for tf in standard_intervals if tf in pivot_df.index]
        actual_rows += [tf for tf in pivot_df.index if tf not in standard_intervals]
        pivot_df = pivot_df.loc[actual_rows]

        # 排序：使用自定义函数对过滤模式排序 (列)
        # 根据 custom_sort_key 转换后的数字从小到大排序
        sorted_cols = sorted(pivot_df.columns, key=custom_sort_key)
        pivot_df = pivot_df.reindex(sorted_cols, axis=1)

        print(f"\n>> 🎯 【{metric}】 横向截面对比")
        print("-" * 90)
        try:
            # 格式化输出：浮点数保留两位小数；处理缺失值为 '-'
            formatter = lambda x: f"{x:.2f}" if pd.notnull(x) and isinstance(x, (int, float)) else (
                "-" if pd.isnull(x) else x)
            print(pivot_df.map(formatter).to_markdown())
        except ImportError:
            print(pivot_df.fillna('-').to_string())

    print("\n" + "=" * 90)


# 调用示例 (可以在文件末尾加上下面的测试代码)
# ==========================================
if __name__ == "__main__":
    print(f"{'=' * 90}")
    print("💡 【数据维度说明】:")
    print(" 🔹 周期 (Timeframe) : K线图的时间粒度 (如 60m=1小时线, 5m=5分钟线)。直观反映策略在不同级别趋势中的适应性。")
    print(" 🔹 过滤模式 (Filter): 基于过去24小时涨跌幅的截面选币过滤机制。")
    print("      - bottom_N : 仅在跌幅最大（排名垫底）的前 N 个币种上允许开仓。")
    print("      - original : 原始状态，不对币种进行任何截面过滤，全市场轮动。")
    print("      - top_N    : 仅在涨幅最大（排名靠前）的前 N 个币种上允许开仓。")
    print(f"{'=' * 90}")


    # 假设你的大表已经生成完毕，在这里调用：
    show_strategy_multi_timeframe(
        entry_factor='EXIT_MULTI_MA_BREAK',
        exit_factor='EXIT_MA_DEAD_CROSS',
        direction='Long'
    )