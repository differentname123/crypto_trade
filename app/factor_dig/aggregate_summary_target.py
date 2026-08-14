import os
import glob
import numpy as np
import pandas as pd


def aggregate_results(input_dir="factor_out_60m_debug", output_dir="summary_results"):
    # 1. 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)

    # 2. 匹配输入目录下所有的 pairs_*.csv.gz 文件
    file_pattern = os.path.join(input_dir, "pairs_*.csv.gz")
    files = glob.glob(file_pattern)

    if not files:
        print(f"❌ 在目录 '{input_dir}' 下未找到任何 pairs_*.csv.gz 文件！")
        return

    print(f"📂 共找到 {len(files)} 个币种的结果文件，正在读取...")

    # 3. 读取并合并所有数据
    df_list = []
    for f in files:
        try:
            df = pd.read_csv(f, compression='gzip')
            df_list.append(df)
        except Exception as e:
            print(f"读取文件 {f} 时出错: {e}")

    if not df_list:
        print("❌ 没有成功读取到任何数据！")
        return

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
            print(f"⚠️ 未找到 {direction} 方向的数据，跳过生成。")
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

        # 7. 落盘保存
        out_name = f"summary_{direction.lower()}_60m.csv"
        out_path = os.path.join(output_dir, out_name)

        # 将小数四舍五入到4位，让CSV报表看起来更干净（可选）
        grouped = grouped.round(4)
        grouped.to_csv(out_path, index=False, encoding='utf-8-sig')

        print(f"✅ 成功生成 {direction} 聚合报告: {out_path} (共 {len(grouped)} 条组合记录)")


if __name__ == "__main__":
    df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\summary_results\summary_long_60m.csv')

    # 指定你的输入和输出目录
    input_directory = "factor_out_5m_debug"
    output_directory = "summary_results"

    aggregate_results(input_dir=input_directory, output_dir=output_directory)