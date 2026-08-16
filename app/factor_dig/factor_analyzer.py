import pandas as pd
import numpy as np
import os
import unicodedata
from datetime import datetime

# ==========================================
# 0. 过滤条件配置 (Filter Configuration)
# ==========================================
FILTER_CONFIG = {
    'min_total_oos_trades': 30,  # 四周期样本外总交易数底线
    'max_single_coin_concentration': 40.0,  # 单币集中度(%)最大限制（大于该值则过滤）
    'max_top3_profit_concentration': 80.0,  # [新增] Top3利润占比(%)最大限制（大于该值则过滤）
    'min_profitable_coin_ratio': 0.0,  # [新增] 盈利币比例(%)最小限制，需大于该值
    'max_holding_hours': 10 * 24,  # 盈利单与亏损单平均持仓时间(小时)最大限制
    'min_avg_net_profit': 0.2,  # 单笔平均净收益最小限制(%)，需大于该值
    'min_win_rate': 0.0,  # [新增] 胜率(%)最小限制，需大于该值
    'min_profit_loss_time_ratio': 0.99,  # 盈亏持仓时间比最小限制，需大于等于该值
    'min_profitable_quarters': 3,  # 盈利季度数最小限制，需严格大于该值
    'min_60m_avg_net_profit': 1.0  # [新增] 60m单笔平均净收益最小限制(%)，需大于该值
}

# ==========================================
# 1. 信号脱敏系统 (Anonymization System)
# ==========================================
signal_map = {}
signal_counter = 1


def anonymize(signal_name):
    """将信号名称脱敏为 SIGNAL_XXX 格式，并记录在案"""
    global signal_counter
    if pd.isna(signal_name) or str(signal_name).strip() == '' or signal_name == 'Unknown':
        return 'Unknown'

    signal_str = str(signal_name).strip()
    if signal_str not in signal_map:
        signal_map[signal_str] = f"SIGNAL_{signal_counter:03d}"
        signal_counter += 1
    return signal_map[signal_str]


def save_mapping():
    """保存脱敏映射表至 CSV"""
    if not signal_map:
        return
    mapping_df = pd.DataFrame(list(signal_map.items()), columns=['真实信号名称', '脱敏代码'])
    mapping_path = 'signal_mapping.csv'
    mapping_df.to_csv(mapping_path, index=False, encoding='utf-8-sig')
    print(f"[*] 信号脱敏已完成，映射表已保存至: {mapping_path}")


# ==========================================
# 2. 终端显示与格式化工具 (Formatting Tools)
# ==========================================
def visual_len(text):
    """计算中英混合字符串的视觉长度（中文算2格）"""
    return sum(2 if unicodedata.east_asian_width(c) in 'WF' else 1 for c in str(text))


def pad_label(text, width=16):
    """智能补齐标签长度，保证表格竖线对齐"""
    text = str(text)
    vlen = visual_len(text)
    return text + ' ' * max(0, width - vlen)


def get_val(row, col_base, tf, fmt='raw'):
    """安全地从 DataFrame 行中提取并格式化特定周期的字段"""
    col_name = f"{col_base}_{tf}"
    if col_name not in row or pd.isna(row[col_name]):
        return "N/A"

    val = row[col_name]
    try:
        if fmt == 'int': return f"{int(val)}"
        if fmt == 'pct': return f"{val:.2f}%"
        if fmt == 'pct_plus': return f"{val:+.2f}%"
        if fmt == 'float2': return f"{val:.2f}"
        if fmt == 'float2_plus': return f"{val:+.2f}"
        return str(val)
    except:
        return "N/A"


def print_row(label, vals):
    """打印完美对齐的表格行，支持动态列数"""
    lbl = pad_label(label, 16)
    cols = []
    for i, v in enumerate(vals):
        if i < len(vals) - 1:
            cols.append(pad_label(v, 24))
        else:
            cols.append(str(v))
    if not cols:
        print(f"  {lbl} |")
    else:
        print(f"  {lbl} | " + " | ".join(cols))


# ==========================================
# 3. 数据处理与报告生成逻辑
# ==========================================
def load_and_prep_data():
    paths = {
        '60m': './summary_results_60m/aggregated_summary_Long.csv',
        '30m': './summary_results_30m/aggregated_summary_Long.csv',
        '15m': './summary_results_15m/aggregated_summary_Long.csv',
        '5m': './summary_results_5m/aggregated_summary_Long.csv'
    }

    dfs = {}
    for tf, path in paths.items():
        if not os.path.exists(path):
            continue  # 若路径不存在，则不报错，直接跳过处理后续有的
        df = pd.read_csv(path)

        # 脱敏入场和出场信号
        df['入场信号名称'] = df['入场信号名称'].apply(anonymize)
        df['出场信号名称'] = df['出场信号名称'].apply(anonymize)
        df['过滤模式'] = df['过滤模式'].fillna('Unknown')

        dfs[tf] = df

    save_mapping()
    return dfs


def generate_report():
    print("正在加载与处理多周期因子数据，请稍候...\n")

    try:
        dfs = load_and_prep_data()
    except Exception as e:
        print(f"系统错误: {e}")
        return

    if not dfs:
        print("没有找到任何周期的有效数据文件，无法生成报告。")
        return

    tfs = list(dfs.keys())

    # 1. 提取全局宏观统计
    df_all = pd.concat(list(dfs.values()), ignore_index=True)
    valid_returns = df_all[df_all['总收益'].abs() > 1e-6]
    global_funding_friction = (valid_returns['总资金费率'] / valid_returns[
        '总收益'].abs()).median() * 100 if not valid_returns.empty else 0
    filter_oos_medians = df_all.groupby('过滤模式')['样本外平均单笔净收益'].median()

    # 2. 横向拼接多周期宽表 (Full Outer Join)
    keys = ['入场信号名称', '出场信号名称', '过滤模式']
    d_list = [df.set_index(keys).add_suffix(f'_{tf}') for tf, df in dfs.items()]

    merged_df = d_list[0]
    if len(d_list) > 1:
        merged_df = merged_df.join(d_list[1:], how='outer')
    merged_df = merged_df.reset_index()

    # 底线过滤：剔除所有有效周期样本外交易总数过少的组合
    merged_df['total_oos_trades'] = 0
    for tf in tfs:
        merged_df['total_oos_trades'] += merged_df[f'样本外交易次数_{tf}'].fillna(0)

    # 提取过滤条件
    min_trades = FILTER_CONFIG['min_total_oos_trades']
    max_conc = FILTER_CONFIG['max_single_coin_concentration']
    max_top3_conc = FILTER_CONFIG['max_top3_profit_concentration']
    min_prof_coin_ratio = FILTER_CONFIG['min_profitable_coin_ratio']
    max_hours = FILTER_CONFIG['max_holding_hours']
    min_avg_net = FILTER_CONFIG['min_avg_net_profit']
    min_win_rate = FILTER_CONFIG['min_win_rate']
    min_pl_time_ratio = FILTER_CONFIG['min_profit_loss_time_ratio']
    min_profitable_quarters = FILTER_CONFIG['min_profitable_quarters']
    min_60m_avg_net = FILTER_CONFIG['min_60m_avg_net_profit']

    # 动态构建过滤条件
    cond_all = (merged_df['total_oos_trades'] >= min_trades)

    # 单独对 60m 周期附加 1% 的单笔平均净收益条件（如果存在 60m 数据）
    if '60m' in tfs:
        cond_all &= merged_df['单笔平均净收益_60m'].fillna(-999) > min_60m_avg_net

    for tf in tfs:
        cond_all &= merged_df[f'最优币占总净收益百分比_{tf}'].fillna(0) <= max_conc
        cond_all &= merged_df[f'Top3币种利润占总净收益百分比_{tf}'].fillna(0) <= max_top3_conc

        # 盈利币比例过滤：(盈利的币数 / 产生交易的币种总数 * 100) > min_prof_coin_ratio
        prof_coin_ratio = (merged_df[f'盈利的币数_{tf}'].fillna(0) / merged_df[f'产生交易的币种总数_{tf}'].replace(0,
                                                                                                                   np.nan)) * 100
        cond_all &= prof_coin_ratio.fillna(0) > min_prof_coin_ratio

        multiplier = {'60m': 1, '30m': 0.5, '15m': 0.25, '5m': 5 / 60.0}[tf]
        cond_all &= (merged_df[f'盈利单平均持仓 K 线根数_{tf}'].fillna(0) * multiplier <= max_hours)
        cond_all &= (merged_df[f'亏损单平均持仓 K 线根数_{tf}'].fillna(0) * multiplier <= max_hours)

        cond_all &= merged_df[f'单笔平均净收益_{tf}'].fillna(-999) > min_avg_net
        cond_all &= merged_df[f'胜率_{tf}'].fillna(0) > min_win_rate
        cond_all &= merged_df[f'盈亏持仓时间比_{tf}'].fillna(-1) >= min_pl_time_ratio
        cond_all &= merged_df[f'净盈利季度数量_{tf}'].fillna(0) > min_profitable_quarters

    # 联合过滤
    filtered_df = merged_df[cond_all].copy()

    # 计算各个周期的总净收益并计算复合排序分
    for tf in tfs:
        filtered_df[f'总净收益_{tf}'] = filtered_df[f'单笔平均净收益_{tf}'] * filtered_df[f'总交易数_{tf}']
        filtered_df[f'排序分_{tf}'] = filtered_df[f'总净收益_{tf}'] * filtered_df[f'单笔平均净收益_{tf}']

    # 计算排序锚点 (所有存在的周期的复合得分指标的平均值)
    filtered_df['avg_complex_score'] = filtered_df[
        [f'排序分_{tf}' for tf in tfs]
    ].mean(axis=1)

    # 提取存活 Top 10 (要求所有的存在周期 OOS 收益皆 > 0)
    surv_cond = pd.Series(True, index=filtered_df.index)
    for tf in tfs:
        surv_cond &= (filtered_df[f'样本外平均单笔净收益_{tf}'] > 0)
    survivors = filtered_df[surv_cond]
    top_entry_factors = survivors['入场信号名称'].value_counts().head(10)
    top_exit_factors = survivors['出场信号名称'].value_counts().head(10)

    # 生成 Top 50 榜单 (按新复合锚点降序排列)
    top50 = filtered_df.sort_values(by='avg_complex_score', ascending=False).head(100)

    # ==========================================
    # 保存筛选后的数据并打印日志
    # ==========================================
    save_path = os.path.abspath('filtered_results.csv')
    save_df = filtered_df.copy()

    # 建立反向映射，将脱敏代码还原为原始信号名称
    inv_map = {v: k for k, v in signal_map.items()}
    save_df['入场信号名称'] = save_df['入场信号名称'].map(lambda x: inv_map.get(x, x))
    save_df['出场信号名称'] = save_df['出场信号名称'].map(lambda x: inv_map.get(x, x))

    # 保存为 CSV
    save_df.to_csv(save_path, index=False, encoding='utf-8-sig')

    # 统计相关数量并打印日志
    final_count = len(save_df)
    unique_combo_count = save_df[['入场信号名称', '出场信号名称']].drop_duplicates().shape[0]
    print(
        f"[*] 日志: 筛选数据已保存至: {save_path} | 最终数量: {final_count} | 按照 进场信号 出场信号去重的数量: {unique_combo_count}")

    # ==========================================
    # 打印报告
    # ==========================================
    print("\n" + "=" * 110)
    print(">>> [加密货币因子挖掘 - 多周期全景印证报告] <<<")
    print(
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 排序基准：多周期 (总净收益 × 单笔平均净利) 的平均值")
    print("=" * 110 + "\n")

    print("【第一部分：宏观水位线 (Macro Baseline)】")
    print("2. 过滤模式 (Filter) 样本外单笔净利中位数：")
    filter_str = "  "
    for idx, val in filter_oos_medians.items():
        filter_str += f"[{idx}]: {val:+.2f}% | "
    print(filter_str.strip(" | "))

    print("\n3. 四周期存活率 Top 10 (全周期样本外皆盈利)：")
    print("  " + pad_label("[高频入场 (Entry)]", 45) + "[高频出场 (Exit)]")
    entry_list = list(top_entry_factors.items())
    exit_list = list(top_exit_factors.items())
    max_len = max(len(entry_list), len(exit_list))
    total_survivors = len(survivors)

    for i in range(max_len):
        if i < len(entry_list):
            name, count = entry_list[i]
            pct = (count / total_survivors * 100) if total_survivors > 0 else 0
            e_str = f"- {name} ({count}次, 占比{pct:.1f}%)"
        else:
            e_str = ""

        if i < len(exit_list):
            name, count = exit_list[i]
            pct = (count / total_survivors * 100) if total_survivors > 0 else 0
            x_str = f"- {name} ({count}次, 占比{pct:.1f}%)"
        else:
            x_str = ""

        print(f"  {pad_label(e_str, 45)} {x_str}")

    print("\n" + "=" * 110)
    print("【第二部分：各个组合微观多维切片 (Micro Profiling)】")
    print("=" * 110)

    for rank, (idx, row) in enumerate(top50.iterrows(), 1):
        print(f"\n[组合编号 #{rank:02d}] ")
        print(f"组合身份: Entry = {row['入场信号名称']} | Exit = {row['出场信号名称']} | Filter = {row['过滤模式']}")
        print("-" * 110)

        # 第一版块：核心绩效
        header_lbl = pad_label("核心与衰减指标", 16)
        h_cols = []
        for i, tf in enumerate(tfs):
            if i < len(tfs) - 1:
                h_cols.append(pad_label(f"[{tf} 周期]", 24))
            else:
                h_cols.append(f"[{tf} 周期]")
        header_str = " | ".join(h_cols) if h_cols else ""
        print(f"  {header_lbl} | {header_str}")
        print("  " + "-" * 110)

        print_row("总交易数", [get_val(row, '总交易数', tf, 'int') for tf in tfs])
        print_row("胜率", [get_val(row, '胜率', tf, 'pct') for tf in tfs])

        # 动态推算总体平均单笔净利
        def get_overall_avg_net(tf):
            try:
                col = f'单笔平均净收益_{tf}'
                if col in row and pd.notna(row[col]):
                    return f"{float(row[col]):+.2f}%"

                total_t = float(row[f'总交易数_{tf}']) if pd.notna(row.get(f'总交易数_{tf}')) else 0
                oos_t = float(row[f'样本外交易次数_{tf}']) if pd.notna(row.get(f'样本外交易次数_{tf}')) else 0
                is_t = total_t - oos_t

                if total_t > 0:
                    oos_net = float(row[f'样本外平均单笔净收益_{tf}']) if pd.notna(
                        row.get(f'样本外平均单笔净收益_{tf}')) else 0
                    is_net = float(row[f'样本内平均单笔净收益_{tf}']) if pd.notna(
                        row.get(f'样本内平均单笔净收益_{tf}')) else 0
                    overall = (is_net * is_t + oos_net * oos_t) / total_t
                    return f"{overall:+.2f}%"
            except:
                pass
            return "N/A"

        print_row("平均单笔净利", [get_overall_avg_net(tf) for tf in tfs])
        print_row("总净利润", [get_val(row, '总净收益', tf, 'pct_plus') for tf in tfs])
        print_row("样本内单笔净利", [get_val(row, '样本内平均单笔净收益', tf, 'pct_plus') for tf in tfs])
        print_row("样本外单笔净利", [get_val(row, '样本外平均单笔净收益', tf, 'pct_plus') for tf in tfs])

        # 平均盈亏持仓展示
        def get_holding_hours(tf):
            multiplier = {'60m': 1, '30m': 0.5, '15m': 0.25, '5m': 5 / 60.0}[tf]
            w_raw = get_val(row, '盈利单平均持仓 K 线根数', tf, 'raw')
            l_raw = get_val(row, '亏损单平均持仓 K 线根数', tf, 'raw')
            if w_raw != 'N/A' and l_raw != 'N/A':
                try:
                    w_hrs = float(w_raw) * multiplier
                    l_hrs = float(l_raw) * multiplier
                    return f"盈:{w_hrs:.1f}h / 亏:{l_hrs:.1f}h"
                except:
                    return "N/A"
            return "N/A"

        print_row("平均盈亏持仓", [get_holding_hours(tf) for tf in tfs])
        print_row("盈亏持仓时间比", [get_val(row, '盈亏持仓时间比', tf, 'float2') for tf in tfs])

        # 第二版块：广度与集中度
        print("\n> 截面宽度与单币风险")

        def get_breadth(tf):
            w = get_val(row, '盈利的币数', tf, 'int')
            t = get_val(row, '产生交易的币种总数', tf, 'int')
            if w != 'N/A' and t != 'N/A' and int(t) > 0:
                return f"{(int(w) / int(t) * 100):.1f}% ({w}/{t})"
            return "N/A"

        print_row("盈利币比例", [get_breadth(tf) for tf in tfs])
        print_row("单币集中度", [get_val(row, '最优币占总净收益百分比', tf, 'pct') for tf in tfs])

        # [修改点 1]：新增 Top3 利润与亏损占总净收益百分比
        print_row("Top3利润占比", [get_val(row, 'Top3币种利润占总净收益百分比', tf, 'pct') for tf in tfs])
        print_row("Top3亏损占比", [get_val(row, 'Top3币种亏损占总净收益百分比', tf, 'pct') for tf in tfs])

        print_row("最大收益币", [get_val(row, '最大收益币名称', tf) for tf in tfs])

        # 第三版块：时间序列平稳性
        print("\n> 季度平稳性 (Q1-Q4净收益)")

        def get_q_net(q, tf):
            try:
                rev = float(row[f"{q}收益_{tf}"])
                fee = float(row[f"{q}资金费率_{tf}"])
                if pd.notna(rev) and pd.notna(fee):
                    return f"{(rev - fee):+.2f}"
            except:
                pass
            return "N/A"

        print_row("Q1 净收益", [get_q_net('Q1', tf) for tf in tfs])
        print_row("Q2 净收益", [get_q_net('Q2', tf) for tf in tfs])
        print_row("Q3 净收益", [get_q_net('Q3', tf) for tf in tfs])
        print_row("Q4 净收益", [get_q_net('Q4', tf) for tf in tfs])

        def get_q_win(tf):
            v = get_val(row, '净盈利季度数量', tf, 'int')
            return f"{v} / 4" if v != 'N/A' else 'N/A'

        print_row("盈利季度数", [get_q_win(tf) for tf in tfs])
        print("-" * 110)


def query_strategy_combination(entry_name, exit_name, filter_name):
    """
    独立查询特定策略组合的多周期详细信息。
    只需传入真实入场名、出场名和过滤模式即可。
    """
    import pandas as pd
    import os
    import unicodedata

    # ==========================================
    # 内部工具函数 (完全自包含)
    # ==========================================
    def visual_len(text):
        return sum(2 if unicodedata.east_asian_width(c) in 'WF' else 1 for c in str(text))

    def pad_label(text, width=16):
        text = str(text)
        return text + ' ' * max(0, width - visual_len(text))

    def get_val(row_data, col_base, tf, fmt='raw'):
        col_name = f"{col_base}_{tf}"
        if col_name not in row_data or pd.isna(row_data[col_name]):
            return "N/A"
        val = row_data[col_name]
        try:
            if fmt == 'int': return f"{int(val)}"
            if fmt == 'pct': return f"{val:.2f}%"
            if fmt == 'pct_plus': return f"{val:+.2f}%"
            if fmt == 'float2': return f"{val:.2f}"
            if fmt == 'float2_plus': return f"{val:+.2f}"
            return str(val)
        except:
            return "N/A"

    def print_row(label, vals):
        lbl = pad_label(label, 16)
        cols = []
        for i, v in enumerate(vals):
            if i < len(vals) - 1:
                cols.append(pad_label(v, 24))
            else:
                cols.append(str(v))
        if not cols:
            print(f"  {lbl} |")
        else:
            print(f"  {lbl} | " + " | ".join(cols))

    # ==========================================
    # 1. 动态读取并精准过滤数据
    # ==========================================
    paths = {
        '60m': './summary_results_60m/aggregated_summary_Long.csv',
        '30m': './summary_results_30m/aggregated_summary_Long.csv',
        '15m': './summary_results_15m/aggregated_summary_Long.csv',
        '5m': './summary_results_5m/aggregated_summary_Long.csv'
    }

    dfs = []
    tfs = []
    print(f"[*] 正在从本地文件中提取组合 [{entry_name} | {exit_name} | {filter_name}] 的数据...\n")

    for tf, path in paths.items():
        if not os.path.exists(path):
            continue

        df = pd.read_csv(path)
        df['过滤模式'] = df['过滤模式'].fillna('Unknown')

        mask = (df['入场信号名称'] == entry_name) & \
               (df['出场信号名称'] == exit_name) & \
               (df['过滤模式'] == filter_name)

        target = df[mask]
        if not target.empty:
            target = target.set_index(['入场信号名称', '出场信号名称', '过滤模式']).add_suffix(f'_{tf}')
            dfs.append(target)
            tfs.append(tf)

    if not dfs:
        print(f"[查询失败] 数据库中未找到该组合的任何周期记录！")
        return

    merged_df = pd.concat(dfs, axis=1).reset_index()

    for tf in tfs:
        if f'单笔平均净收益_{tf}' in merged_df.columns and f'总交易数_{tf}' in merged_df.columns:
            merged_df[f'总净收益_{tf}'] = merged_df[f'单笔平均净收益_{tf}'] * merged_df[f'总交易数_{tf}']

    row = merged_df.iloc[0]

    # ==========================================
    # 辅助计算闭包
    # ==========================================
    def get_overall_avg_net(tf):
        try:
            col = f'单笔平均净收益_{tf}'
            if col in row and pd.notna(row[col]):
                return f"{float(row[col]):+.2f}%"
            total_t = float(row[f'总交易数_{tf}']) if pd.notna(row.get(f'总交易数_{tf}')) else 0
            oos_t = float(row[f'样本外交易次数_{tf}']) if pd.notna(row.get(f'样本外交易次数_{tf}')) else 0
            is_t = total_t - oos_t
            if total_t > 0:
                oos_net = float(row[f'样本外平均单笔净收益_{tf}']) if pd.notna(
                    row.get(f'样本外平均单笔净收益_{tf}')) else 0
                is_net = float(row[f'样本内平均单笔净收益_{tf}']) if pd.notna(
                    row.get(f'样本内平均单笔净收益_{tf}')) else 0
                overall = (is_net * is_t + oos_net * oos_t) / total_t
                return f"{overall:+.2f}%"
        except:
            pass
        return "N/A"

    def get_holding_hours(tf):
        multiplier = {'60m': 1, '30m': 0.5, '15m': 0.25, '5m': 5 / 60.0}[tf]
        w_raw = get_val(row, '盈利单平均持仓 K 线根数', tf, 'raw')
        l_raw = get_val(row, '亏损单平均持仓 K 线根数', tf, 'raw')
        if w_raw != 'N/A' and l_raw != 'N/A':
            try:
                w_hrs = float(w_raw) * multiplier
                l_hrs = float(l_raw) * multiplier
                return f"盈:{w_hrs:.1f}h / 亏:{l_hrs:.1f}h"
            except:
                return "N/A"
        return "N/A"

    def get_breadth(tf):
        w = get_val(row, '盈利的币数', tf, 'int')
        t = get_val(row, '产生交易的币种总数', tf, 'int')
        if w != 'N/A' and t != 'N/A' and int(t) > 0:
            return f"{(int(w) / int(t) * 100):.1f}% ({w}/{t})"
        return "N/A"

    def get_q_net(q, tf):
        try:
            rev = float(row[f"{q}收益_{tf}"])
            fee = float(row[f"{q}资金费率_{tf}"])
            if pd.notna(rev) and pd.notna(fee):
                return f"{(rev - fee):+.2f}"
        except:
            pass
        return "N/A"

    def get_q_win(tf):
        v = get_val(row, '净盈利季度数量', tf, 'int')
        return f"{v} / 4" if v != 'N/A' else 'N/A'

    # ==========================================
    # 2. 打印完整报告
    # ==========================================
    print(f">>> [特定组合详情直通车查询] <<<")
    print(f"组合身份: Entry = {entry_name} | Exit = {exit_name} | Filter = {filter_name}")
    print("-" * 110)

    header_lbl = pad_label("核心与衰减指标", 16)
    h_cols = []
    for i, tf in enumerate(tfs):
        if i < len(tfs) - 1:
            h_cols.append(pad_label(f"[{tf} 周期]", 24))
        else:
            h_cols.append(f"[{tf} 周期]")
    header_str = " | ".join(h_cols) if h_cols else ""
    print(f"  {header_lbl} | {header_str}")
    print("  " + "-" * 110)

    print_row("总交易数", [get_val(row, '总交易数', tf, 'int') for tf in tfs])
    print_row("胜率", [get_val(row, '胜率', tf, 'pct') for tf in tfs])
    print_row("平均单笔净利", [get_overall_avg_net(tf) for tf in tfs])
    print_row("总净利润", [get_val(row, '总净收益', tf, 'pct_plus') for tf in tfs])
    print_row("样本内单笔净利", [get_val(row, '样本内平均单笔净收益', tf, 'pct_plus') for tf in tfs])
    print_row("样本外单笔净利", [get_val(row, '样本外平均单笔净收益', tf, 'pct_plus') for tf in tfs])
    print_row("平均盈亏持仓", [get_holding_hours(tf) for tf in tfs])
    print_row("盈亏持仓时间比", [get_val(row, '盈亏持仓时间比', tf, 'float2') for tf in tfs])

    print("\n> 截面宽度与单币风险")
    print_row("盈利币比例", [get_breadth(tf) for tf in tfs])
    print_row("单币集中度", [get_val(row, '最优币占总净收益百分比', tf, 'pct') for tf in tfs])

    # [修改点 2]：新增 Top3 利润与亏损占总净收益百分比
    print_row("Top3利润占比", [get_val(row, 'Top3币种利润占总净收益百分比', tf, 'pct') for tf in tfs])
    print_row("Top3亏损占比", [get_val(row, 'Top3币种亏损占总净收益百分比', tf, 'pct') for tf in tfs])

    print_row("最大收益币", [get_val(row, '最大收益币名称', tf) for tf in tfs])

    print("\n> 季度平稳性 (Q1-Q4净收益)")
    print_row("Q1 净收益", [get_q_net('Q1', tf) for tf in tfs])
    print_row("Q2 净收益", [get_q_net('Q2', tf) for tf in tfs])
    print_row("Q3 净收益", [get_q_net('Q3', tf) for tf in tfs])
    print_row("Q4 净收益", [get_q_net('Q4', tf) for tf in tfs])
    print_row("盈利季度数", [get_q_win(tf) for tf in tfs])
    print("-" * 110)


if __name__ == "__main__":
    import warnings

    warnings.filterwarnings("ignore")
    generate_report()
    # query_strategy_combination('EXIT_MULTI_MA_BREAK', 'EXIT_MA_DEAD_CROSS', 'bottom_5')