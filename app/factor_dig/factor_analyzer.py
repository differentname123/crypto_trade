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
    'max_holding_hours': 30 * 24,  # 盈利单与亏损单平均持仓时间(小时)最大限制
    'min_avg_net_profit': 0.2,  # 单笔平均净收益最小限制(%)，需大于该值
    'min_profit_loss_time_ratio': 0.5  # 盈亏持仓时间比最小限制，需大于等于该值
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


def print_row(label, v60, v30, v15, v5):
    """打印完美对齐的表格行"""
    lbl = pad_label(label, 16)      # <--- 改为 16
    c60 = pad_label(v60, 24)        # <--- 改为 24
    c30 = pad_label(v30, 24)        # <--- 改为 24
    c15 = pad_label(v15, 24)        # <--- 改为 24
    c5 = str(v5)
    print(f"  {lbl} | {c60} | {c30} | {c15} | {c5}")


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
            raise FileNotFoundError(f"找不到文件: {path}。请确认路径。")
        df = pd.read_csv(path)

        # 脱敏入场和出场信号
        df['入场信号名称'] = df['入场信号名称'].apply(anonymize)
        df['出场信号名称'] = df['出场信号名称'].apply(anonymize)
        df['过滤模式'] = df['过滤模式'].fillna('Unknown')

        dfs[tf] = df

    save_mapping()
    return dfs['60m'], dfs['30m'], dfs['15m'], dfs['5m']


def generate_report():
    print("正在加载与处理多周期因子数据，请稍候...\n")

    try:
        df_60m, df_30m, df_15m, df_5m = load_and_prep_data()
    except Exception as e:
        print(f"系统错误: {e}")
        return

    # 1. 提取全局宏观统计
    df_all = pd.concat([df_60m, df_30m, df_15m, df_5m], ignore_index=True)
    valid_returns = df_all[df_all['总收益'].abs() > 1e-6]
    global_funding_friction = (valid_returns['总资金费率'] / valid_returns[
        '总收益'].abs()).median() * 100 if not valid_returns.empty else 0
    filter_oos_medians = df_all.groupby('过滤模式')['样本外平均单笔净收益'].median()

    # 2. 横向拼接四周期宽表 (Full Outer Join)
    keys = ['入场信号名称', '出场信号名称', '过滤模式']
    d60 = df_60m.set_index(keys).add_suffix('_60m')
    d30 = df_30m.set_index(keys).add_suffix('_30m')
    d15 = df_15m.set_index(keys).add_suffix('_15m')
    d5 = df_5m.set_index(keys).add_suffix('_5m')

    merged_df = d60.join([d30, d15, d5], how='outer').reset_index()

    # 底线过滤：剔除四周期样本外交易总数过少的组合
    merged_df['total_oos_trades'] = (
            merged_df['样本外交易次数_60m'].fillna(0) +
            merged_df['样本外交易次数_30m'].fillna(0) +
            merged_df['样本外交易次数_15m'].fillna(0) +
            merged_df['样本外交易次数_5m'].fillna(0)
    )

    # 提取过滤条件
    min_trades = FILTER_CONFIG['min_total_oos_trades']
    max_conc = FILTER_CONFIG['max_single_coin_concentration']
    max_hours = FILTER_CONFIG['max_holding_hours']
    min_avg_net = FILTER_CONFIG['min_avg_net_profit']
    min_pl_time_ratio = FILTER_CONFIG['min_profit_loss_time_ratio']

    # 计算条件 (缺失值按0或-1处理，防止因某个周期无交易被误杀；缺失则按很小的值处理防误通过)
    cond_trades = merged_df['total_oos_trades'] >= min_trades
    cond_conc_60m = merged_df['最优币占总净收益百分比_60m'].fillna(0) <= max_conc
    cond_conc_30m = merged_df['最优币占总净收益百分比_30m'].fillna(0) <= max_conc
    cond_conc_15m = merged_df['最优币占总净收益百分比_15m'].fillna(0) <= max_conc
    cond_conc_5m = merged_df['最优币占总净收益百分比_5m'].fillna(0) <= max_conc

    # 将各周期持仓K线数转化为持仓时间(小时)并限制盈利单与亏损单平均时间
    cond_hours_60m = (merged_df['盈利单平均持仓 K 线根数_60m'].fillna(0) * 1 <= max_hours) & \
                     (merged_df['亏损单平均持仓 K 线根数_60m'].fillna(0) * 1 <= max_hours)
    cond_hours_30m = (merged_df['盈利单平均持仓 K 线根数_30m'].fillna(0) * 0.5 <= max_hours) & \
                     (merged_df['亏损单平均持仓 K 线根数_30m'].fillna(0) * 0.5 <= max_hours)
    cond_hours_15m = (merged_df['盈利单平均持仓 K 线根数_15m'].fillna(0) * 0.25 <= max_hours) & \
                     (merged_df['亏损单平均持仓 K 线根数_15m'].fillna(0) * 0.25 <= max_hours)
    cond_hours_5m = (merged_df['盈利单平均持仓 K 线根数_5m'].fillna(0) * (5/60.0) <= max_hours) & \
                    (merged_df['亏损单平均持仓 K 线根数_5m'].fillna(0) * (5/60.0) <= max_hours)

    cond_avg_net_60m = merged_df['单笔平均净收益_60m'].fillna(-999) > min_avg_net
    cond_avg_net_30m = merged_df['单笔平均净收益_30m'].fillna(-999) > min_avg_net
    cond_avg_net_15m = merged_df['单笔平均净收益_15m'].fillna(-999) > min_avg_net
    cond_avg_net_5m = merged_df['单笔平均净收益_5m'].fillna(-999) > min_avg_net

    cond_pl_ratio_60m = merged_df['盈亏持仓时间比_60m'].fillna(-1) >= min_pl_time_ratio
    cond_pl_ratio_30m = merged_df['盈亏持仓时间比_30m'].fillna(-1) >= min_pl_time_ratio
    cond_pl_ratio_15m = merged_df['盈亏持仓时间比_15m'].fillna(-1) >= min_pl_time_ratio
    cond_pl_ratio_5m = merged_df['盈亏持仓时间比_5m'].fillna(-1) >= min_pl_time_ratio

    # 联合过滤
    filtered_df = merged_df[
        cond_trades &
        cond_conc_60m & cond_conc_30m & cond_conc_15m & cond_conc_5m &
        cond_hours_60m & cond_hours_30m & cond_hours_15m & cond_hours_5m &
        cond_avg_net_60m & cond_avg_net_30m & cond_avg_net_15m & cond_avg_net_5m &
        cond_pl_ratio_60m & cond_pl_ratio_30m & cond_pl_ratio_15m & cond_pl_ratio_5m
        ].copy()

    # 计算排序锚点 (改为：四周期平均单笔净收益均值)
    filtered_df['avg_net_profit'] = filtered_df[
        ['单笔平均净收益_60m', '单笔平均净收益_30m', '单笔平均净收益_15m', '单笔平均净收益_5m']
    ].mean(axis=1)

    # 提取存活 Top 10 (要求四个周期 OOS 收益皆 > 0)
    survivors = filtered_df[
        (filtered_df['样本外平均单笔净收益_60m'] > 0) &
        (filtered_df['样本外平均单笔净收益_30m'] > 0) &
        (filtered_df['样本外平均单笔净收益_15m'] > 0) &
        (filtered_df['样本外平均单笔净收益_5m'] > 0)
        ]
    top_entry_factors = survivors['入场信号名称'].value_counts().head(10)
    top_exit_factors = survivors['出场信号名称'].value_counts().head(10)

    # 生成 Top 50 榜单 (按新锚点降序排列)
    top50 = filtered_df.sort_values(by='avg_net_profit', ascending=False).head(100)

    # ==========================================
    # 打印报告
    # ==========================================
    print("\n" + "=" * 110)
    print(">>> [加密货币因子挖掘 - 多周期全景印证报告] <<<")
    print(f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | 排序基准：四周期平均单笔净收益")
    print("=" * 110 + "\n")

    print("【第一部分：宏观水位线 (Macro Baseline)】")
    # print(f"1. 全局基准：总资金费率占毛收益的平均中位数摩擦比例为 {global_funding_friction:.2f}%")

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

        # 第一版块：核心绩效 (Header)
        header_lbl = pad_label("核心与衰减指标", 16)  # <--- 改为 16
        h60 = pad_label("[60m 周期]", 24)  # <--- 改为 24
        h30 = pad_label("[30m 周期]", 24)  # <--- 改为 24
        h15 = pad_label("[15m 周期]", 24)  # <--- 改为 24
        h5 = "[5m 周期]"
        print(f"  {header_lbl} | {h60} | {h30} | {h15} | {h5}")
        print("  " + "-" * 110)  # <--- 分割线加长到 110

        print_row("总交易数",
                  get_val(row, '总交易数', '60m', 'int'),
                  get_val(row, '总交易数', '30m', 'int'),
                  get_val(row, '总交易数', '15m', 'int'),
                  get_val(row, '总交易数', '5m', 'int'))

        print_row("胜率",
                  get_val(row, '胜率', '60m', 'pct'),
                  get_val(row, '胜率', '30m', 'pct'),
                  get_val(row, '胜率', '15m', 'pct'),
                  get_val(row, '胜率', '5m', 'pct'))

        # 动态推算总体平均单笔净利（处理CSV中缺失该字段的情况）
        def get_overall_avg_net(tf):
            try:
                # 优先使用真实字段 '单笔平均净收益'
                col = f'单笔平均净收益_{tf}'
                if col in row and pd.notna(row[col]):
                    return f"{float(row[col]):+.2f}%"

                # 如果依然没有该字段，用 样本内 和 样本外 的单笔收益按交易数进行加权平均作为备用方案
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

        print_row("平均单笔净利",
                  get_overall_avg_net('60m'),
                  get_overall_avg_net('30m'),
                  get_overall_avg_net('15m'),
                  get_overall_avg_net('5m'))

        print_row("样本内单笔净利",
                  get_val(row, '样本内平均单笔净收益', '60m', 'pct_plus'),
                  get_val(row, '样本内平均单笔净收益', '30m', 'pct_plus'),
                  get_val(row, '样本内平均单笔净收益', '15m', 'pct_plus'),
                  get_val(row, '样本内平均单笔净收益', '5m', 'pct_plus'))

        print_row("样本外单笔净利",
                  get_val(row, '样本外平均单笔净收益', '60m', 'pct_plus'),
                  get_val(row, '样本外平均单笔净收益', '30m', 'pct_plus'),
                  get_val(row, '样本外平均单笔净收益', '15m', 'pct_plus'),
                  get_val(row, '样本外平均单笔净收益', '5m', 'pct_plus'))

        # 盈亏持仓(小时)整合展示
        def get_holding_hours(tf):
            multiplier = {'60m': 1, '30m': 0.5, '15m': 0.25, '5m': 5/60.0}[tf]
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

        print_row("盈亏持仓(小时)", get_holding_hours('60m'), get_holding_hours('30m'), get_holding_hours('15m'), get_holding_hours('5m'))

        print_row("盈亏持仓时间比",
                  get_val(row, '盈亏持仓时间比', '60m', 'float2'),
                  get_val(row, '盈亏持仓时间比', '30m', 'float2'),
                  get_val(row, '盈亏持仓时间比', '15m', 'float2'),
                  get_val(row, '盈亏持仓时间比', '5m', 'float2'))

        # 第二版块：广度与集中度
        print("\n> 截面宽度与单币风险")

        def get_breadth(tf):
            w = get_val(row, '盈利的币数', tf, 'int')
            t = get_val(row, '产生交易的币种总数', tf, 'int')
            if w != 'N/A' and t != 'N/A' and int(t) > 0:
                return f"{(int(w) / int(t) * 100):.1f}% ({w}/{t})"
            return "N/A"

        print_row("盈利币比例", get_breadth('60m'), get_breadth('30m'), get_breadth('15m'), get_breadth('5m'))
        print_row("单币集中度",
                  get_val(row, '最优币占总净收益百分比', '60m', 'pct'),
                  get_val(row, '最优币占总净收益百分比', '30m', 'pct'),
                  get_val(row, '最优币占总净收益百分比', '15m', 'pct'),
                  get_val(row, '最优币占总净收益百分比', '5m', 'pct'))
        print_row("最大收益币",
                  get_val(row, '最大收益币名称', '60m'),
                  get_val(row, '最大收益币名称', '30m'),
                  get_val(row, '最大收益币名称', '15m'),
                  get_val(row, '最大收益币名称', '5m'))

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

        print_row("Q1 净收益", get_q_net('Q1', '60m'), get_q_net('Q1', '30m'), get_q_net('Q1', '15m'), get_q_net('Q1', '5m'))
        print_row("Q2 净收益", get_q_net('Q2', '60m'), get_q_net('Q2', '30m'), get_q_net('Q2', '15m'), get_q_net('Q2', '5m'))
        print_row("Q3 净收益", get_q_net('Q3', '60m'), get_q_net('Q3', '30m'), get_q_net('Q3', '15m'), get_q_net('Q3', '5m'))
        print_row("Q4 净收益", get_q_net('Q4', '60m'), get_q_net('Q4', '30m'), get_q_net('Q4', '15m'), get_q_net('Q4', '5m'))

        def get_q_win(tf):
            v = get_val(row, '净盈利季度数量', tf, 'int')
            return f"{v} / 4" if v != 'N/A' else 'N/A'

        print_row("盈利季度数", get_q_win('60m'), get_q_win('30m'), get_q_win('15m'), get_q_win('5m'))
        print("-" * 110)


def query_strategy_combination(entry_name, exit_name, filter_name):
    """
    独立查询特定策略组合的多周期详细信息。
    只需传入真实入场名、出场名和过滤模式即可。

    示例: query_strategy_combination('EXIT_SHORT_SURGE_EXTREME', 'FR_ZERO_ZONE', 'bottom_5')
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

    def print_row(label, v60, v30, v15, v5):
        lbl = pad_label(label, 16)  # <--- 改为 16
        c60 = pad_label(v60, 24)  # <--- 改为 24
        c30 = pad_label(v30, 24)  # <--- 改为 24
        c15 = pad_label(v15, 24)  # <--- 改为 24
        c5 = str(v5)
        print(f"  {lbl} | {c60} | {c30} | {c15} | {c5}")

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
    print(f"[*] 正在从本地文件中提取组合 [{entry_name} | {exit_name} | {filter_name}] 的数据...\n")

    for tf, path in paths.items():
        if not os.path.exists(path):
            print(f"[警告] 找不到文件: {path}")
            continue

        df = pd.read_csv(path)
        df['过滤模式'] = df['过滤模式'].fillna('Unknown')

        # 精准定位这一行（使用真实名称，无需经过脱敏系统）
        mask = (df['入场信号名称'] == entry_name) & \
               (df['出场信号名称'] == exit_name) & \
               (df['过滤模式'] == filter_name)

        target = df[mask]
        if not target.empty:
            # 只取匹配到的数据，加上周期后缀并设好索引以便合并
            target = target.set_index(['入场信号名称', '出场信号名称', '过滤模式']).add_suffix(f'_{tf}')
            dfs.append(target)

    if not dfs:
        print(f"[查询失败] 数据库中未找到该组合的任何周期记录！")
        return

    # 将提取出的单个组合的多周期数据横向合并成一条宽记录
    merged_df = pd.concat(dfs, axis=1).reset_index()
    row = merged_df.iloc[0]

    # ==========================================
    # 辅助计算闭包 (绑定到上面提取出的 row)
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
        multiplier = {'60m': 1, '30m': 0.5, '15m': 0.25, '5m': 5/60.0}[tf]
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

    header_lbl = pad_label("核心与衰减指标", 16)  # <--- 改为 16
    h60 = pad_label("[60m 周期]", 24)  # <--- 改为 24
    h30 = pad_label("[30m 周期]", 24)  # <--- 改为 24
    h15 = pad_label("[15m 周期]", 24)  # <--- 改为 24
    h5 = "[5m 周期]"
    print(f"  {header_lbl} | {h60} | {h30} | {h15} | {h5}")
    print("  " + "-" * 110)  # <--- 分割线加长到 110

    print_row("总交易数", get_val(row, '总交易数', '60m', 'int'), get_val(row, '总交易数', '30m', 'int'),
              get_val(row, '总交易数', '15m', 'int'), get_val(row, '总交易数', '5m', 'int'))
    print_row("胜率", get_val(row, '胜率', '60m', 'pct'), get_val(row, '胜率', '30m', 'pct'),
              get_val(row, '胜率', '15m', 'pct'), get_val(row, '胜率', '5m', 'pct'))
    print_row("平均单笔净利", get_overall_avg_net('60m'), get_overall_avg_net('30m'), get_overall_avg_net('15m'),
              get_overall_avg_net('5m'))
    print_row("样本内单笔净利", get_val(row, '样本内平均单笔净收益', '60m', 'pct_plus'),
              get_val(row, '样本内平均单笔净收益', '30m', 'pct_plus'),
              get_val(row, '样本内平均单笔净收益', '15m', 'pct_plus'),
              get_val(row, '样本内平均单笔净收益', '5m', 'pct_plus'))
    print_row("样本外单笔净利", get_val(row, '样本外平均单笔净收益', '60m', 'pct_plus'),
              get_val(row, '样本外平均单笔净收益', '30m', 'pct_plus'),
              get_val(row, '样本外平均单笔净收益', '15m', 'pct_plus'),
              get_val(row, '样本外平均单笔净收益', '5m', 'pct_plus'))
    print_row("盈亏持仓(小时)", get_holding_hours('60m'), get_holding_hours('30m'), get_holding_hours('15m'), get_holding_hours('5m'))
    print_row("盈亏持仓时间比", get_val(row, '盈亏持仓时间比', '60m', 'float2'),
              get_val(row, '盈亏持仓时间比', '30m', 'float2'), get_val(row, '盈亏持仓时间比', '15m', 'float2'),
              get_val(row, '盈亏持仓时间比', '5m', 'float2'))

    print("\n> 截面宽度与单币风险")
    print_row("盈利币比例", get_breadth('60m'), get_breadth('30m'), get_breadth('15m'), get_breadth('5m'))
    print_row("单币集中度", get_val(row, '最优币占总净收益百分比', '60m', 'pct'),
              get_val(row, '最优币占总净收益百分比', '30m', 'pct'),
              get_val(row, '最优币占总净收益百分比', '15m', 'pct'), get_val(row, '最优币占总净收益百分比', '5m', 'pct'))
    print_row("最大收益币", get_val(row, '最大收益币名称', '60m'), get_val(row, '最大收益币名称', '30m'),
              get_val(row, '最大收益币名称', '15m'), get_val(row, '最大收益币名称', '5m'))

    print("\n> 季度平稳性 (Q1-Q4净收益)")
    print_row("Q1 净收益", get_q_net('Q1', '60m'), get_q_net('Q1', '30m'), get_q_net('Q1', '15m'),
              get_q_net('Q1', '5m'))
    print_row("Q2 净收益", get_q_net('Q2', '60m'), get_q_net('Q2', '30m'), get_q_net('Q2', '15m'),
              get_q_net('Q2', '5m'))
    print_row("Q3 净收益", get_q_net('Q3', '60m'), get_q_net('Q3', '30m'), get_q_net('Q3', '15m'),
              get_q_net('Q3', '5m'))
    print_row("Q4 净收益", get_q_net('Q4', '60m'), get_q_net('Q4', '30m'), get_q_net('Q4', '15m'),
              get_q_net('Q4', '5m'))
    print_row("盈利季度数", get_q_win('60m'), get_q_win('30m'), get_q_win('15m'), get_q_win('5m'))
    print("-" * 110)

if __name__ == "__main__":
    import warnings

    warnings.filterwarnings("ignore")
    generate_report()
    query_strategy_combination('EXIT_SHORT_SURGE_EXTREME', 'FR_ZERO_ZONE', 'bottom_5')