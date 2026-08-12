# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os


# ==========================================
# 核心分析类
# ==========================================
class StrategyAnalyzer:
    def __init__(self, filepath, direction):
        """
        :param filepath: 聚合后的 csv 文件路径
        :param direction: 'Long' 或 'Short'，用于判断资金费率加减逻辑
        """
        self.filepath = filepath
        self.direction = direction.capitalize()
        self.df = pd.read_csv(filepath)
        print(f"\n{'=' * 50}")
        print(f"📊 开始分析 {self.direction} 策略数据，总组合数: {len(self.df)}")
        print(f"{'=' * 50}")

    def anonymize_signals(self, mapping_file='./summary_results/signal_mapping.csv'):
        """对信号名称进行脱敏，并维护一个全局映射表"""
        print(f"🔒 正在执行信号名称脱敏...")

        # 提取当前数据中所有的独特信号名称
        entry_sigs = self.df['入场信号名称'].dropna().unique().tolist()
        exit_sigs = self.df['出场信号名称'].dropna().unique().tolist()
        current_signals = sorted(list(set(entry_sigs) | set(exit_sigs)))

        # 尝试读取已有的映射表，保证跨文件/跨运行时的脱敏一致性
        mapping = {}
        current_max_id = 0
        if os.path.exists(mapping_file):
            existing_df = pd.read_csv(mapping_file)
            mapping = dict(zip(existing_df['Original_Signal'], existing_df['Masked_Signal']))
            if mapping:
                # 提取已有的最大序号，例如从 "SIGNAL_045" 中提取 45
                current_max_id = max([int(v.split('_')[1]) for v in mapping.values()])

        # 为当前数据中的新信号生成脱敏名称
        new_mapping_count = 0
        for sig in current_signals:
            if sig not in mapping:
                current_max_id += 1
                mapping[sig] = f"SIGNAL_{current_max_id:03d}"
                new_mapping_count += 1

        # 将新的映射关系保存回文件
        if new_mapping_count > 0 or not os.path.exists(mapping_file):
            os.makedirs(os.path.dirname(mapping_file), exist_ok=True)
            mapping_df = pd.DataFrame(list(mapping.items()), columns=['Original_Signal', 'Masked_Signal'])
            mapping_df.to_csv(mapping_file, index=False, encoding='utf-8-sig')
            print(f"   已新增 {new_mapping_count} 个信号映射，映射表已保存至: {mapping_file}")
        else:
            print(f"   所有信号均在已有映射表中，无需更新文件。")

        # 将 DataFrame 中的原始名称替换为脱敏名称
        self.df['入场信号名称'] = self.df['入场信号名称'].map(mapping)
        self.df['出场信号名称'] = self.df['出场信号名称'].map(mapping)

        return self

    def calculate_derived_fields(self):
        """0. 计算派生字段"""
        df = self.df.copy()

        # 1. 资金费率逻辑处理 (做多减去费率，做空加上费率)
        sign = -1 if self.direction == 'Long' else 1

        # 计算各类净收益
        df['总净收益'] = df['总收益'] + sign * df['总资金费率']
        df['样本内净收益'] = df['样本内收益'] + sign * df['样本内资金费率']
        df['样本外净收益'] = df['样本外收益'] + sign * df['样本外资金费率']

        for q in ['Q1', 'Q2', 'Q3', 'Q4']:
            df[f'{q}净收益'] = df[f'{q}收益'] + sign * df[f'{q}资金费率']

        # 2. 计算胜率 (%)
        df['胜率(%)'] = np.where(df['总交易数'] > 0, (df['盈利交易次数'] / df['总交易数']) * 100, 0)

        # 3. 计算单笔平均净收益
        df['单笔平均净收益'] = np.where(df['总交易数'] > 0, df['总净收益'] / df['总交易数'], 0)
        df['样本内单笔净收益'] = np.where(df['样本内交易次数'] > 0, df['样本内净收益'] / df['样本内交易次数'], 0)
        df['样本外单笔净收益'] = np.where(df['样本外交易次数'] > 0, df['样本外净收益'] / df['样本外交易次数'], 0)

        # 4. 盈利的币比例
        df['盈利币比例'] = np.where(df['产生交易的币种总数'] > 0, df['盈利的币数'] / df['产生交易的币种总数'], 0)

        # 5. 盈亏持仓时间比
        df['盈亏持仓时间比'] = np.where(df['亏损单平均持仓 K 线根数'] > 0,
                                        df['盈利单平均持仓 K 线根数'] / df['亏损单平均持仓 K 线根数'],
                                        np.inf)

        # 6. 最优币占总净收益百分比 (%)
        df['最优币净收益占比(%)'] = np.where(df['总净收益'] > 0,
                                             (df['最大收益币的收益'] / df['总净收益']) * 100,
                                             0)

        self.df = df
        return self

    def apply_hard_filters(self, min_avg_net_ret=0.3, min_trades=50, min_coins=5, min_profit_coin_ratio=0.5):
        """1. 硬过滤并打印漏斗分析（通过率）"""
        print("\n🔍 正在执行硬过滤漏斗分析...")
        df = self.df
        total_initial = len(df)

        if total_initial == 0:
            print("❌ 数据为空，跳过过滤。")
            return self

        filters = {
            f"单笔平均净收益 > {min_avg_net_ret}%": df['单笔平均净收益'] > min_avg_net_ret,
            f"总交易次数 > {min_trades}": df['总交易数'] > min_trades,
            f"产生交易的币种总数 > {min_coins}": df['产生交易的币种总数'] > min_coins,
            f"盈利币比例 > {min_profit_coin_ratio * 100}%": df['盈利币比例'] > min_profit_coin_ratio,
            "各季度净收益均为正": (df['Q1净收益'] > 0) & (df['Q2净收益'] > 0) & (df['Q3净收益'] > 0) & (
                        df['Q4净收益'] > 0)
        }

        current_df = df
        for filter_name, condition in filters.items():
            prev_count = len(current_df)
            current_df = current_df.loc[current_df.index.intersection(df[condition].index)]
            curr_count = len(current_df)

            pass_rate = (curr_count / prev_count * 100) if prev_count > 0 else 0
            total_retention = (curr_count / total_initial * 100)

            print(
                f"  [{filter_name:<25}] | 剩余: {curr_count:<5} | 步骤通过率: {pass_rate:>6.2f}% | 总留存率: {total_retention:>6.2f}%")

        self.filtered_df = current_df
        print(f"\n✅ 过滤完成！最终符合条件的组合数量: {len(self.filtered_df)}")
        return self

    def display_top_n(self, top_n=10, sort_by='单笔平均净收益'):
        """2. 排序并优雅地展示前 N 个结果"""
        if self.filtered_df.empty:
            print(f"\n⚠️ 没有组合通过过滤条件，无法展示 Top {top_n}。")
            return

        print(f"\n🏆 按照 [{sort_by}] 降序排列，展示 Top {top_n} 策略组合:\n")

        top_df = self.filtered_df.sort_values(by=sort_by, ascending=False).head(top_n)

        for i, (_, row) in enumerate(top_df.iterrows(), 1):
            print(f"==== Top {i} ========================================================")
            print(f"🔹 【参数配置】")
            print(f"   入场信号: {row['入场信号名称']}")
            print(f"   出场信号: {row['出场信号名称']}")
            print(f"   过滤模式: {row['过滤模式']}")

            print(f"\n🔹 【核心绩效】")
            print(
                f"   总交易次数: {row['总交易数']}  |  胜率: {row['胜率(%)']:.2f}%  |  总净收益: {row['总净收益']:.2f}%")
            print(f"   单笔平均净收益: {row['单笔平均净收益']:.4f}%  |  全局最大回撤: {row['全局最大回撤']:.2f}%")

            print(f"\n🔹 【时序稳定性】")
            print(f"   Q1净收益: {row['Q1净收益']:>6.2f}%  |  Q2净收益: {row['Q2净收益']:>6.2f}%")
            print(f"   Q3净收益: {row['Q3净收益']:>6.2f}%  |  Q4净收益: {row['Q4净收益']:>6.2f}%")
            print(
                f"   样本内平均单笔: {row['样本内单笔净收益']:.4f}%  |  样本外平均单笔: {row['样本外单笔净收益']:.4f}%")

            print(f"\n🔹 【持仓与风险分布】")
            print(
                f"   盈利单均持仓(K线): {row['盈利单平均持仓 K 线根数']}  |  亏损单均持仓(K线): {row['亏损单平均持仓 K 线根数']}")
            print(f"   盈亏持仓时间比: {row['盈亏持仓时间比']:.2f} (越大越能扛盈止损)")
            print(f"   最优币名称: {row['最大收益币名称']}  |  最优币贡献占比: {row['最优币净收益占比(%)']:.2f}%")
            print("=================================================================\n")


# ==========================================
# 执行入口 (灵活配置)
# ==========================================
if __name__ == '__main__':
    DIRECTION = 'Long'  # 'Long' 或 'Short'
    TIME_FRAME = '60m'  # 可选: '60m', '30m', '15m'
    # --- 你可以在这里灵活修改参数 ---
    TARGET_FILE = f'./summary_results_{TIME_FRAME}/aggregated_summary_{DIRECTION}.csv'
    MAPPING_FILE = f'./summary_results_{TIME_FRAME}/signal_mapping.csv'

    # 过滤参数
    MIN_AVG_NET_RET = 0.3
    MIN_TRADES = 50
    MIN_COINS = 5
    MIN_PROFIT_RATIO = 0.5
    # ------------------------------

    if os.path.exists(TARGET_FILE):
        analyzer = StrategyAnalyzer(filepath=TARGET_FILE, direction=DIRECTION)

        # 链式调用：脱敏 -> 算指标 -> 过滤 -> 展示
        analyzer.anonymize_signals(mapping_file=MAPPING_FILE) \
            .calculate_derived_fields() \
            .apply_hard_filters(
            min_avg_net_ret=MIN_AVG_NET_RET,
            min_trades=MIN_TRADES,
            min_coins=MIN_COINS,
            min_profit_coin_ratio=MIN_PROFIT_RATIO
        ) \
            .display_top_n(top_n=10, sort_by='单笔平均净收益')
    else:
        print(f"❌ 找不到文件: {TARGET_FILE}，请确认上一步的聚合结果是否存在。")