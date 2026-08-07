# -*- coding: utf-8 -*-
"""
终极精选组合深度分析脚本 (全量大合集)
包含所有右侧突破、左侧反转、情绪极值、量价确认等核心逻辑策略
"""
from factor_analyzer import load_res, combo_detail


# 注：如果你使用的是 v3 版本，请将上一行改为从 factor_analyzer_v3 导入

def main():
    # 1. 加载缓存数据 (请确保路径与您本地一致)
    out_dir = './factor_out_15m'
    res = load_res(out_dir)

    if res is None:
        print("❌ 缓存加载失败，请检查路径。")
        return

    # 汇总所有代码片段中的 (入场, 出场) 组合与原版逻辑说明
    all_combos = [
        # ═══ 核心逻辑组 1：右侧突破 + 极端情绪/狂热出场（高收益爆发，盈亏比极高） ═══
        ('OI_BREAKOUT_CONFIRM', 'EXIT_FR_EXTREME_HIGH'),  # [逻辑] 真实带量突破进场 -> 费率极度狂热(散户FOMO)砸盘离场 (t=3.49)
        ('BREAK_RETEST_HOLD_REAL', 'EXIT_FR_EXTREME_HIGH'),  # [逻辑] 突破回踩企稳进场 -> 费率狂热止盈 (最安全的右侧买点)
        ('VOLUME_AT_BREAKOUT_LEVEL', 'EXIT_FR_EXTREME_HIGH'),  # [逻辑] 突破放量进场 -> 费率极度狂热出场

        # ═══ 核心逻辑组 2：量价确认 + 动能衰竭出场（高期望，OOS留存极好） ═══
        ('VOLUME_CONFIRM_BREAK', 'KLINE_CONSEC_STRONG_CLOSE'),  # [逻辑] 放量突破 -> 动能衰竭/连续强势顶分型离场 (池化期望3.44%)
        ('ENTRY_TREND_CONFIRM_B', 'KLINE_CONSEC_STRONG_CLOSE'),  # [逻辑] 均线与资金面多重健康趋势确认 -> 连续强势K线离场 (顺势波段)
        ('EXIT_FR_EXTREME_HIGH', 'KLINE_CONSEC_STRONG_CLOSE'),  # [逻辑] 费率极值出场因子转入场 -> 连续强K出场 (t=2.94, OOS留存93%)
        ('EXIT_PARABOLIC_EXTENSION', 'KLINE_CONSEC_STRONG_CLOSE'),  # [逻辑] 抛物线延伸 -> 强K线出场 (4108笔, OOS留存85%)

        # ═══ 核心逻辑组 3：左侧反转与恐慌抄底（极高胜率与偏离接刀验证） ═══
        ('EXIT_MULTI_MA_BREAK', 'PRICE_MA_STACK'),  # [逻辑] 均线严重破位 -> 多头排列恢复 (抄底接刀极高胜率 83%)
        ('EXIT_VOL_EXTREME_DOWN', 'KLINE_CONSEC_STRONG_CLOSE'),  # [逻辑] 向下波动率极值(恐慌深坑) -> 连续反弹修复离场 (期望2.87%)

        # ═══ 核心逻辑组 4：箱体震荡与结构形态（统计最强组补充） ═══
        ('OI_VALUE_EMA_CROSS', 'BREAK_RANGE_QUANTILE'),  # [逻辑] 低回撤震荡突破型：缩量洗盘潜伏 -> 脉冲到箱体上沿出局
        ('EXIT_HIGH_STALL_BREAK', 'BREAK_RANGE_QUANTILE'),  # [逻辑] 高位停顿突破 (t=3.30, 6176笔最厚)
        ('EXIT_UPPER_WICK_REJECTION', 'ENTRY_INSIDE_BREAK_VOLUME'),  # [逻辑] 上影线拒绝 (60币最广, OOS留存108%)
        ('EXIT_OI_VALUE_EXTREME', 'EXIT_PRICE_NEWHIGH_OI_WEAK'),  # [逻辑] OI价值极值 (OOS留存220%, oos_t=5.24)
        ('OBV_BULL_DIV', 'PRICE_MULTI_MA_STACK'),  # [逻辑] OBV底背离 -> 均线多头堆叠 (t=3.10, 胜率69%)

        # ═══ 核心逻辑组 5：高期望/OOS优秀 其他留存组 ═══
        ('FR_ROLL_OVER_FROM_HIGH', 'EXIT_OI_VALUE_EXTREME'),  # [特征] OOS留存119%
        ('FR_HIGH_EXTREME', 'MOM_CONSISTENT_UP_BARS'),  # [特征] OOS留存102%, 2477笔
        ('BREAKDOWN_N_LOW', 'PRICE_TREND_STRENGTH_UP'),  # [特征] OOS留存94%, 中位回撤较大需注意
        ('VOLUME_EXPAND_PRICE_UP', 'BREAK_FLAT_THEN_BREAK'),  # [特征] boot_lo5=2.81, pooled=5.23%

        # ═══ 核心逻辑组 6：跨版本验证组（v1跨币种高分 → v3交叉验证） ═══
        ('ENTRY_HEALTHY_ACCELERATION_VOLUME', 'FR_VERY_LOW'),  # [特征] v1: 83币6797笔
        ('EXIT_CROWDED_BLOWOFF', 'FR_VERY_LOW'),  # [特征] v1: 71币3246笔
        ('VOLUME_EXPAND_PRICE_DOWN', 'ENTRY_SHORT_SQUEEZE_VOLUME')  # [特征] v1: 57币1637笔
    ]

    for i, (entry_factor, exit_factor) in enumerate(all_combos, 1):
        combo_detail(res, entry_factor, exit_factor)
if __name__ == '__main__':
    main()