import os
import glob
import numpy as np
import pandas as pd
from functools import reduce  # 新增：用于多个周期的 DataFrame 合并


def aggregate_results(intervals=['60m', '30m', '15m', '5m', '1m'], input_dir_template="factor_out_{}_debug",
                      output_dir="summary_results"):
    # 1. 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 用于保存每个周期合并多空后的 DataFrame，以便最后做跨周期大聚合
    interval_dfs_dict = {}

    for interval in intervals:
        input_dir = input_dir_template.format(interval)

        # 2. 匹配输入目录下所有的 pairs_*.csv.gz 文件
        file_pattern = os.path.join(input_dir, "pairs_*.csv.gz")
        files = glob.glob(file_pattern)

        if not files:
            print(f"⚠️ 在目录 '{input_dir}' 下未找到任何 pairs_*.csv.gz 文件，跳过 {interval} 周期！")
            continue

        print(f"📂 共找到 {len(files)} 个 {interval} 周期的币种结果文件，正在读取...")

        # 3. 读取并合并所有数据
        df_list = []
        for f in files:
            try:
                df = pd.read_csv(f, compression='gzip')
                df_list.append(df)
            except Exception as e:
                print(f"读取文件 {f} 时出错: {e}")

        if not df_list:
            print(f"❌ {interval} 周期没有成功读取到任何数据！")
            continue

        all_data = pd.concat(df_list, ignore_index=True)

        # 定义聚合逻辑 (去掉了 avg_ret，因为我们要自己算真实的平均净利润)
        agg_dict = {
            'coin': 'count',  # 触发该组合的币种数量
            'trades': 'sum',  # 总交易笔数
            'sum_ret': 'sum',  # 总收益率求和 (%)
            'fr_sum': 'sum',  # 资金费率总和 (%)
            'win_rate': 'mean',  # 胜率均值 (%)
            'sharpe': 'mean',  # 夏普比率均值
            'max_dd': ['mean', 'max'],  # 资金曲线回撤：平均值 & 全局最惨
            'max_loss': 'min',  # 单笔最大亏损
            'profit_factor': 'mean',  # 盈亏比均值
            'avg_hold_h': 'mean',  # 平均持仓时间 (小时)
            'fr_avg': 'mean'  # 单笔资金费率均值 (%)
        }

        # 分别处理 做多 (Long) 和 做空 (Short)
        for direction in ['Long', 'Short']:
            df_dir = all_data[all_data['direction'] == direction]

            if df_dir.empty:
                print(f"⚠️ 未找到 {interval} 周期的 {direction} 方向的数据，跳过生成。")
                continue

            # 4. 按照核心字段分组并聚合
            grouped = df_dir.groupby(['entry_factor', 'exit_factor', 'filter_mode'], as_index=False).agg(agg_dict)

            # 扁平化多级列名
            grouped.columns = [f"{col[0]}_{col[1]}" if col[1] else col[0] for col in grouped.columns]

            # ---------------------------------------------------------
            # 5. 核心派生指标计算 (根据你的需求调整)
            # ---------------------------------------------------------
            # 【要求2】：做多减去资金费率，做空加上资金费率
            if direction == 'Long':
                grouped['net_profit'] = grouped['sum_ret_sum'] - grouped['fr_sum_sum']
            else:
                grouped['net_profit'] = grouped['sum_ret_sum'] + grouped['fr_sum_sum']

            # 【要求3】：单笔平均利润 = 净利润 / 交易次数 (把分母0替换为NaN防报错)
            safe_trades = grouped['trades_sum'].replace(0, np.nan)
            grouped['avg_net_ret'] = grouped['net_profit'] / safe_trades

            # 排序：按净利润降序排
            grouped.sort_values(by='net_profit', ascending=False, inplace=True)

            # ---------------------------------------------------------
            # 6. 【要求1】：重命名为中文，并优化列的展示顺序
            # ---------------------------------------------------------
            rename_map = {
                'entry_factor': '入场信号名称',
                'exit_factor': '出场信号名称',
                'filter_mode': '过滤模式',
                'coin_count': '触发币种数量',
                'trades_sum': '总交易笔数',
                'sum_ret_sum': '原始总收益(%)',
                'fr_sum_sum': '总资金费率(%)',
                'net_profit': '净利润(%)',
                'avg_net_ret': '单笔平均净利润(%)',
                'win_rate_mean': '平均胜率(%)',
                'max_loss_min': '单笔最惨亏损(%)',
                'max_dd_max': '全局最惨资金回撤(%)',
                'max_dd_mean': '平均资金回撤(%)',
                'sharpe_mean': '平均夏普比率',
                'profit_factor_mean': '平均盈亏比',
                'avg_hold_h_mean': '平均持仓时间(小时)',
                'fr_avg_mean': '平均单笔资金费率(%)'
            }
            grouped.rename(columns=rename_map, inplace=True)

            # 显式规定列的排列顺序，把核心收益指标放前面
            final_cols = [
                '入场信号名称', '出场信号名称', '过滤模式', '触发币种数量', '总交易笔数',
                '净利润(%)', '单笔平均净利润(%)', '原始总收益(%)', '总资金费率(%)',
                '平均胜率(%)', '单笔最惨亏损(%)', '全局最惨资金回撤(%)', '平均资金回撤(%)',
                '平均夏普比率', '平均盈亏比', '平均持仓时间(小时)', '平均单笔资金费率(%)'
            ]

            # 只取存在于表中的列，生成最终表
            grouped = grouped[[c for c in final_cols if c in grouped.columns]]

            # 7. 落盘保存各个周期单独的报告
            out_name = f"summary_{direction.lower()}_{interval}.csv"
            out_path = os.path.join(output_dir, out_name)

            # 将小数四舍五入到4位，让CSV报表看起来更干净（可选）
            grouped = grouped.round(4)
            grouped.to_csv(out_path, index=False, encoding='utf-8-sig')

            print(f"✅ 成功生成 {interval} {direction} 聚合报告: {out_path} (共 {len(grouped)} 条组合记录)")

            # ---------------------------------------------------------
            # 8. 为最终跨周期聚合做准备
            # ---------------------------------------------------------
            df_for_merge = grouped.copy()
            df_for_merge.insert(0, '方向', direction)  # 插入方向字段作为合并的主键之一

            # 给指标列加上当前周期的前缀（排除掉用于 Merge 的 Key）
            merge_keys = ['方向', '入场信号名称', '出场信号名称', '过滤模式']
            rename_prefix = {}
            for col in df_for_merge.columns:
                if col not in merge_keys:
                    rename_prefix[col] = f"[{interval}] {col}"
            df_for_merge.rename(columns=rename_prefix, inplace=True)

            if interval not in interval_dfs_dict:
                interval_dfs_dict[interval] = []
            interval_dfs_dict[interval].append(df_for_merge)

    # ---------------------------------------------------------
    # 9. 跨周期和多空的大聚合逻辑
    # ---------------------------------------------------------
    final_merge_list = []
    # 按照周期先将各个周期的多空数据 concat 在一起
    for interval, df_list in interval_dfs_dict.items():
        if df_list:
            combined_dir_df = pd.concat(df_list, ignore_index=True)
            final_merge_list.append(combined_dir_df)

    if final_merge_list:
        print("\n🔄 正在生成跨周期多空聚合总表...")
        # 利用 reduce 按照指定的合并键进行 outer merge，确保任何周期有过触发的组合都能被保留
        merge_keys = ['方向', '入场信号名称', '出场信号名称', '过滤模式']
        final_df = reduce(lambda left, right: pd.merge(left, right, on=merge_keys, how='outer'), final_merge_list)

        # 排序：按照方向、入场、出场、过滤模式排序，保证同策略整齐排列
        final_df.sort_values(by=merge_keys, inplace=True)

        final_out_path = os.path.join(output_dir, "summary_all_intervals_combined.csv")
        final_df.to_csv(final_out_path, index=False, encoding='utf-8-sig')
        print(f"🎉 成功生成跨周期聚合大表: {final_out_path} (共 {len(final_df)} 条组合记录)")
    else:
        print("⚠️ 未收集到任何有效数据，无法生成跨周期聚合大表。")


if __name__ == "__main__":
    # 原代码中的无关行予以保留
    try:
        df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\summary_results\summary_all_intervals_combined.csv')
    except Exception:
        pass  # 防止没文件时在开头报错阻断程序执行

    # 设定你要跑的所有周期，以及输出目录
    target_intervals = ['60m', '30m', '15m', '5m', '1m']
    output_directory = "summary_results"

    # input_dir_template 占位符 {} 会在循环中被替换为 '60m', '30m' 等
    aggregate_results(
        intervals=target_intervals,
        input_dir_template="factor_out_{}_debug",
        output_dir=output_directory
    )