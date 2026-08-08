from app.factor_analyzer import load_res, combo_detail

res = load_res('./factor_out_15m')

# ==============================================================================
# S级：统计最显著 / RC最接近通过 (高胜率、高显著性、低集中度)
# ==============================================================================
combo_detail(res, 'EXIT_MULTI_MA_BREAK', 'PRICE_MA_STACK')
combo_detail(res, 'OI_BREAKOUT_CONFIRM', 'EXIT_FR_EXTREME_HIGH')
combo_detail(res, 'OI_VALUE_EMA_CROSS', 'BREAK_RANGE_QUANTILE')
combo_detail(res, 'EXIT_HIGH_STALL_BREAK', 'BREAK_RANGE_QUANTILE')

# ==============================================================================
# A级：内在质量高 / Bootstrap悲观情形仍稳健 (LOO极高、OOS确认)
# ==============================================================================
combo_detail(res, 'VOLUME_CONFIRM_BREAK', 'KLINE_CONSEC_STRONG_CLOSE')
combo_detail(res, 'FR_ROLL_OVER_FROM_HIGH', 'EXIT_OI_VALUE_EXTREME')
combo_detail(res, 'ENTRY_TREND_CONFIRM_B', 'KLINE_CONSEC_STRONG_CLOSE')
combo_detail(res, 'VOLUME_EXPAND_PRICE_UP', 'BREAK_FLAT_THEN_BREAK')
combo_detail(res, 'ENTRY_HEALTHY_ACCELERATION_VOLUME', 'FR_RESET_AFTER_HOT')
combo_detail(res, 'VOLUME_AT_BREAKOUT_LEVEL', 'EXIT_FR_EXTREME_HIGH')
combo_detail(res, 'FR_HIGH_EXTREME', 'MOM_CONSISTENT_UP_BARS')
combo_detail(res, 'OBV_BULL_DIV', 'PRICE_MULTI_MA_STACK')

# ==============================================================================
# B级：大样本 / 高分散 / OOS 异常突出 (覆盖广、样本外极强)
# ==============================================================================
combo_detail(res, 'EXIT_UPPER_WICK_REJECTION', 'ENTRY_INSIDE_BREAK_VOLUME')
combo_detail(res, 'EXIT_OI_VALUE_EXTREME', 'EXIT_PRICE_NEWHIGH_OI_WEAK')

# ==============================================================================
# C级：高绝对收益 / 高赔率 (右尾肥大，绝对暴利，需重点防单币污染)
# ==============================================================================
combo_detail(res, 'OI_EXTREME_PRICE_NOT_HOT', 'OI_BOTTOM_DIVERGENCE')
combo_detail(res, 'EXIT_VOL_EXTREME_DOWN', 'OI_BOTTOM_DIVERGENCE')
combo_detail(res, 'VOL_EXTREME_RISK', 'OI_BOTTOM_DIVERGENCE')

# ==============================================================================
# D级：跨币种稳健 TOP20 榜单候选 (爆发力极强，需验证普适性)
# ==============================================================================
combo_detail(res, 'KLINE_SMALL_GREEN_ACCUM', 'ENTRY_OI_LEAD_SQUEEZE')
combo_detail(res, 'PRICE_CLOSE_CROSS_MA_UP', 'ENTRY_SHORT_SQUEEZE_VOLUME')
combo_detail(res, 'EXIT_MICRO_DISTRIBUTION', 'OI_BOTTOM_DIVERGENCE')