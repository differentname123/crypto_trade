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
    ('FR_TURN_NEGATIVE', 'FR_RESET_AFTER_HOT', '60m', 'bottom_100'),
    ('FR_TURN_NEGATIVE', 'FR_RESET_AFTER_HOT', '15m', 'bottom_100'),
    ('BREAK_RETEST_HOLD_REAL', 'OI_EXTREME_PRICE_NOT_HOT', '15m', 'bottom_100'),
    ('BREAK_RETEST_HOLD_REAL', 'OI_EXTREME_PRICE_NOT_HOT', '30m', 'bottom_100'),
    ('FR_TURN_POSITIVE', 'FR_RESET_AFTER_HOT', '60m', 'bottom_100'),
    ('FR_TURN_POSITIVE', 'FR_RESET_AFTER_HOT', '30m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'REGIME_POWDER_KEG', '15m', 'bottom_10'),
    ('ENTRY_BOTTOM_STABILIZE', 'REGIME_POWDER_KEG', '30m', 'bottom_20'),
    ('BREAK_DONCHIAN_HIGH_EVENT_REAL', 'REGIME_POWDER_KEG', '15m', 'bottom_100'),
    ('BREAK_DONCHIAN_HIGH_EVENT_REAL', 'REGIME_POWDER_KEG', '30m', 'bottom_100'),
    ('ENTRY_SILENT_ACCUMULATION', 'REGIME_POWDER_KEG', '60m', 'top_50'),
    ('ENTRY_SILENT_ACCUMULATION', 'REGIME_POWDER_KEG', '30m', 'top_50'),
    ('BREAK_STRONG_CLOSE', 'REGIME_POWDER_KEG', '15m', 'bottom_100'),
    ('BREAK_STRONG_CLOSE', 'REGIME_POWDER_KEG', '60m', 'bottom_100'),
    ('EXIT_OI_VALUE_MA_DEAD_CROSS', 'OI_EXTREME_PRICE_NOT_HOT', '5m', 'top_3'),
    ('EXIT_OI_VALUE_MA_DEAD_CROSS', 'OI_EXTREME_PRICE_NOT_HOT', '30m', 'top_3'),
    ('EXIT_VWAP_BREAK', 'OI_SLOPE_UP', '30m', 'top_1'),
    ('EXIT_VWAP_BREAK', 'OI_SLOPE_UP', '5m', 'top_1'),
    ('EXIT_FR_EXTREME_HIGH', 'FR_COLD_START', '30m', 'top_1'),
    ('EXIT_FR_EXTREME_HIGH', 'FR_COLD_START', '5m', 'top_1'),
    ('BREAK_STRONG_CLOSE', 'EXIT_FR_SPIKE_THEN_COOL', '5m', 'bottom_100'),
    ('BREAK_STRONG_CLOSE', 'EXIT_FR_SPIKE_THEN_COOL', '15m', 'bottom_100'),
    ('BREAK_DONCHIAN_HIGH_EVENT_REAL', 'EXIT_FR_SPIKE_THEN_COOL', '30m', 'bottom_100'),
    ('BREAK_DONCHIAN_HIGH_EVENT_REAL', 'EXIT_FR_SPIKE_THEN_COOL', '5m', 'bottom_100'),
    ('BREAK_RETEST_HOLD_REAL', 'ENTRY_THREE_GREEN_VOLUME_OI', '30m', 'bottom_100'),
    ('BREAK_RETEST_HOLD_REAL', 'ENTRY_THREE_GREEN_VOLUME_OI', '15m', 'bottom_100'),
    ('VOLUME_AT_BREAKOUT_LEVEL', 'FR_COLD_START', '60m', 'bottom_100'),
    ('VOLUME_AT_BREAKOUT_LEVEL', 'FR_COLD_START', '15m', 'bottom_100'),
    ('REGIME_POWDER_KEG', 'FR_HIGH_EXTREME', '15m', 'bottom_10'),
    ('REGIME_POWDER_KEG', 'FR_HIGH_EXTREME', '5m', 'bottom_5'),
    ('EXIT_OI_VALUE_MA_DEAD_CROSS', 'ENTRY_TREND_CONFIRM_B', '5m', 'top_3'),
    ('EXIT_OI_VALUE_MA_DEAD_CROSS', 'ENTRY_TREND_CONFIRM_B', '60m', 'top_3'),
    ('VOL_LOW_TO_HIGH', 'VOL_DOWN_SPIKE', '30m', 'top_5'),
    ('VOL_LOW_TO_HIGH', 'VOL_DOWN_SPIKE', '30m', 'top_10'),
    ('ENTRY_HIGH_PRESSURE_OI_BREAKOUT', 'FR_HIGH_EXTREME', '30m', 'top_100'),
    ('BREAK_RETEST_HOLD_REAL', 'FR_COLD_START', '30m', 'bottom_100'),
    ('BREAK_RETEST_HOLD_REAL', 'FR_COLD_START', '15m', 'bottom_100'),
    ('EXIT_FAILED_BREAKOUT', 'FR_COLD_START', '15m', 'top_1'),
    ('EXIT_FAILED_BREAKOUT', 'FR_COLD_START', '5m', 'top_1'),
    ('PRICE_MA_SQUEEZE_UP', 'FR_COLD_START', '30m', 'top_1'),
    ('PRICE_MA_SQUEEZE_UP', 'FR_COLD_START', '15m', 'top_1'),
    ('EXIT_PRICE_NEWHIGH_OI_WEAK', 'FR_COLD_START', '15m', 'top_10'),
    ('EXIT_PRICE_NEWHIGH_OI_WEAK', 'FR_COLD_START', '60m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'ENTRY_HEALTHY_ACCELERATION_VOLUME', '30m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'ENTRY_HEALTHY_ACCELERATION_VOLUME', '15m', 'bottom_100'),
    ('EXIT_SPOT_SUPPRESSION', 'FR_COLD_START', '15m', 'top_1'),
    ('EXIT_SPOT_SUPPRESSION', 'FR_COLD_START', '5m', 'top_1'),
    ('OI_MA_CROSS_UP', 'OI_HIDDEN_RISE_PRICE_FLAT', '5m', 'bottom_1'),
    ('OI_MA_CROSS_UP', 'OI_HIDDEN_RISE_PRICE_FLAT', '60m', 'bottom_5'),
    ('BREAK_FLAT_THEN_BREAK', 'REGIME_POWDER_KEG', '5m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'REGIME_POWDER_KEG', '60m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'OI_MA_CROSS_UP', '5m', 'bottom_10'),
    ('ENTRY_BOTTOM_STABILIZE', 'OI_MA_CROSS_UP', '5m', 'bottom_20'),
    ('FR_ABSOLUTE_HIGH_POS', 'FR_ZERO_ZONE', '60m', 'top_1'),
    ('FR_ABSOLUTE_HIGH_POS', 'FR_ZERO_ZONE', '5m', 'top_1'),
    ('BREAK_DONCHIAN_HIGH_EVENT_REAL', 'FR_COLD_START', '60m', 'bottom_100'),
    ('BREAK_DONCHIAN_HIGH_EVENT_REAL', 'FR_COLD_START', '5m', 'top_1'),
    ('PRICE_MA_SQUEEZE_UP', 'VWAP_CROSS_UP', '5m', 'top_1'),
    ('PRICE_MA_SQUEEZE_UP', 'VWAP_CROSS_UP', '15m', 'top_1'),
    ('ENTRY_UNCROWDED_MOMENTUM', 'VOL_EXTREME_RISK', '60m', 'bottom_100'),
    ('ENTRY_UNCROWDED_MOMENTUM', 'VOL_EXTREME_RISK', '5m', 'bottom_100'),
    ('EXIT_OI_VALUE_EXTREME', 'EXIT_FR_SPIKE_THEN_COOL', '15m', 'bottom_1'),
    ('EXIT_OI_VALUE_EXTREME', 'EXIT_FR_SPIKE_THEN_COOL', '60m', 'bottom_1'),
    ('BREAK_STRONG_CLOSE', 'FR_COLD_START', '60m', 'bottom_100'),
    ('BREAK_STRONG_CLOSE', 'FR_COLD_START', '30m', 'top_1'),
    ('ENTRY_BOTTOM_STABILIZE', 'FR_ZERO_ZONE', '60m', 'bottom_10'),
    ('ENTRY_BOTTOM_STABILIZE', 'FR_ZERO_ZONE', '60m', 'bottom_50'),
    ('FR_ABSOLUTE_HIGH_POS', 'ENTRY_TREND_CONFIRM_B', '60m', 'top_1'),
    ('FR_ABSOLUTE_HIGH_POS', 'ENTRY_TREND_CONFIRM_B', '5m', 'top_1'),
    ('EXIT_PRICE_NEWHIGH_OI_WEAK', 'OI_MA_UP', '60m', 'bottom_100'),
    ('EXIT_PRICE_NEWHIGH_OI_WEAK', 'OI_MA_UP', '5m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'VOLUME_DRY_AT_SUPPORT', '15m', 'bottom_20'),
    ('ENTRY_BOTTOM_STABILIZE', 'VOLUME_DRY_AT_SUPPORT', '15m', 'bottom_10'),
    ('ENTRY_BOTTOM_STABILIZE', 'ENTRY_SILENT_ACCUMULATION', '15m', 'bottom_20'),
    ('ENTRY_BOTTOM_STABILIZE', 'ENTRY_SILENT_ACCUMULATION', '15m', 'bottom_10'),
    ('ENTRY_BOTTOM_STABILIZE', 'FR_STABLE', '15m', 'bottom_20'),
    ('ENTRY_BOTTOM_STABILIZE', 'FR_STABLE', '15m', 'bottom_10'),
    ('EXIT_PRICE_NEWHIGH_OI_WEAK', 'ENTRY_FUNDING_COLD_START_TREND', '5m', 'bottom_100'),
    ('EXIT_PRICE_NEWHIGH_OI_WEAK', 'ENTRY_FUNDING_COLD_START_TREND', '60m', 'bottom_100'),
    ('FR_ABSOLUTE_HIGH_POS', 'FR_COLD_START', '5m', 'top_1'),
    ('FR_ABSOLUTE_HIGH_POS', 'FR_COLD_START', '60m', 'top_1'),
    ('EXIT_MA_DEAD_CROSS', 'OI_RESET_THEN_UP', '15m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'OI_RESET_THEN_UP', '15m', 'bottom_5'),
    ('ENTRY_BOTTOM_STABILIZE', 'ENTRY_UNCROWDED_MOMENTUM', '5m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'ENTRY_UNCROWDED_MOMENTUM', '5m', 'bottom_50'),
    ('EXIT_MA_DEAD_CROSS', 'PRICE_CLOSE_CROSS_MA_UP', '15m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'PRICE_CLOSE_CROSS_MA_UP', '15m', 'bottom_5'),
    ('EXIT_MA_DEAD_CROSS', 'STRUCT_SUPPORT_HOLD', '15m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'STRUCT_SUPPORT_HOLD', '15m', 'bottom_5'),
    ('ENTRY_BOTTOM_STABILIZE', 'OI_HIDDEN_RISE_PRICE_FLAT', '15m', 'bottom_20'),
    ('ENTRY_BOTTOM_STABILIZE', 'OI_HIDDEN_RISE_PRICE_FLAT', '5m', 'bottom_100'),
    ('OI_MA_CROSS_UP', 'EXIT_FR_SPIKE_THEN_COOL', '30m', 'top_100'),
    ('OI_MA_CROSS_UP', 'EXIT_FR_SPIKE_THEN_COOL', '15m', 'top_50'),
    ('EXIT_MA_DEAD_CROSS', 'OI_VALUE_EMA_CROSS', '15m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'OI_VALUE_EMA_CROSS', '30m', 'bottom_5'),
    ('FR_PRICE_UP_HOT', 'FR_POS_NOT_HOT', '60m', 'top_1'),
    ('FR_PRICE_UP_HOT', 'FR_POS_NOT_HOT', '5m', 'top_1'),
    ('FR_HIGH_EXTREME', 'FR_POS_NOT_HOT', '60m', 'top_1'),
    ('FR_HIGH_EXTREME', 'FR_POS_NOT_HOT', '5m', 'top_1'),
    ('EXIT_MA_DEAD_CROSS', 'PRICE_HEALTHY_EXTENSION', '15m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'PRICE_HEALTHY_EXTENSION', '5m', 'bottom_10'),
    ('FR_TURN_NEGATIVE', 'EXIT_HIGH_VOLUME_STALL', '30m', 'top_1'),
    ('FR_TURN_NEGATIVE', 'EXIT_HIGH_VOLUME_STALL', '60m', 'top_1'),
    ('ENTRY_UNCROWDED_MOMENTUM', 'EXIT_VOL_EXTREME_DOWN', '5m', 'bottom_100'),
    ('ENTRY_UNCROWDED_MOMENTUM', 'EXIT_VOL_EXTREME_DOWN', '30m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'ENTRY_THREE_GREEN_VOLUME_OI', '60m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'ENTRY_THREE_GREEN_VOLUME_OI', '5m', 'bottom_100'),
    ('EXIT_MA_DEAD_CROSS', 'VWAP_RECLAIM', '5m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'VWAP_RECLAIM', '15m', 'bottom_10'),
    ('VOL_LOW_TO_HIGH', 'VOLUME_EXPAND_PRICE_DOWN', '30m', 'top_5'),
    ('VOL_LOW_TO_HIGH', 'VOLUME_EXPAND_PRICE_DOWN', '30m', 'top_10'),
    ('BREAK_LONG_CONSOLIDATION_REAL', 'ENTRY_THREE_GREEN_VOLUME_OI', '60m', 'bottom_100'),
    ('BREAK_LONG_CONSOLIDATION_REAL', 'ENTRY_THREE_GREEN_VOLUME_OI', '5m', 'bottom_100'),
    ('EXIT_MA_DEAD_CROSS', 'VWAP_ABOVE', '5m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'VWAP_ABOVE', '15m', 'bottom_10'),
    ('FR_ABSOLUTE_HIGH_POS', 'FR_PRICE_BEAR_DIV', '5m', 'top_1'),
    ('FR_ABSOLUTE_HIGH_POS', 'FR_PRICE_BEAR_DIV', '15m', 'top_1'),
    ('FR_SPIKE_UP', 'ENTRY_UNCROWDED_MOMENTUM', '30m', 'bottom_3'),
    ('FR_SPIKE_UP', 'ENTRY_UNCROWDED_MOMENTUM', '60m', 'bottom_3'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_MA_CROSS_UP', '60m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_MA_CROSS_UP', '5m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'EXIT_FR_SPIKE_THEN_COOL', '15m', 'bottom_20'),
    ('ENTRY_BOTTOM_STABILIZE', 'EXIT_FR_SPIKE_THEN_COOL', '15m', 'bottom_100'),
    ('EXIT_VOLUME_CLIMAX', 'FR_COLD_START', '5m', 'top_1'),
    ('EXIT_VOLUME_CLIMAX', 'FR_COLD_START', '30m', 'top_1'),
    ('EXIT_MA_DEAD_CROSS', 'PRICE_MA_SLOPE_UP', '60m', 'bottom_5'),
    ('EXIT_MA_DEAD_CROSS', 'PRICE_MA_SLOPE_UP', '30m', 'bottom_5'),
    ('BREAK_LONG_CONSOLIDATION_REAL', 'OI_ZSCORE_UP', '60m', 'bottom_100'),
    ('BREAK_LONG_CONSOLIDATION_REAL', 'OI_ZSCORE_UP', '30m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_DROP_EXTREME', '60m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_DROP_EXTREME', '30m', 'bottom_100'),
    ('EXIT_MA_DEAD_CROSS', 'VOLUME_MA_UP', '15m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'VOLUME_MA_UP', '15m', 'bottom_5'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_ZSCORE_UP', '60m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_ZSCORE_UP', '5m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_MA_UP', '60m', 'bottom_100'),
    ('BREAK_FLAT_THEN_BREAK', 'OI_MA_UP', '5m', 'bottom_100'),
    ('PRICE_MA_SQUEEZE_UP', 'ENTRY_FUNDING_COLD_START_TREND', '15m', 'top_1'),
    ('PRICE_MA_SQUEEZE_UP', 'ENTRY_FUNDING_COLD_START_TREND', '30m', 'top_1'),
    ('EXIT_MA_DEAD_CROSS', 'ENTRY_VWAP_RECLAIM_OI', '5m', 'bottom_5'),
    ('EXIT_MA_DEAD_CROSS', 'ENTRY_VWAP_RECLAIM_OI', '5m', 'bottom_10'),
    ('OI_MA_CROSS_UP', 'ENTRY_UNCROWDED_MOMENTUM', '5m', 'bottom_10'),
    ('OI_MA_CROSS_UP', 'ENTRY_UNCROWDED_MOMENTUM', '30m', 'top_50'),
    ('EXIT_MA_DEAD_CROSS', 'VWAP_CROSS_UP', '5m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'VWAP_CROSS_UP', '5m', 'bottom_5'),
    ('PRICE_MULTI_MA_STACK', 'EXIT_OI_VALUE_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('PRICE_MULTI_MA_STACK', 'EXIT_OI_VALUE_MA_DEAD_CROSS', '15m', 'bottom_10'),
    ('FR_ABSOLUTE_HIGH_POS', 'ENTRY_FUNDING_COLD_START_TREND', '5m', 'top_1'),
    ('FR_ABSOLUTE_HIGH_POS', 'ENTRY_FUNDING_COLD_START_TREND', '15m', 'top_1'),
    ('EXIT_DISTRIBUTION_EXHAUSTION_TOP', 'ENTRY_FUNDING_COLD_START_TREND', '60m', 'bottom_20'),
    ('EXIT_DISTRIBUTION_EXHAUSTION_TOP', 'ENTRY_FUNDING_COLD_START_TREND', '30m', 'bottom_20'),
    ('FR_ABSOLUTE_DEEP_NEG', 'OI_MA_CROSS_UP', '60m', 'bottom_3'),
    ('FR_ABSOLUTE_DEEP_NEG', 'OI_MA_CROSS_UP', '30m', 'bottom_3'),
    ('ENTRY_BOTTOM_STABILIZE', 'VOLUME_CLIMAX_UP', '30m', 'bottom_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'VOLUME_CLIMAX_UP', '60m', 'bottom_100'),
    ('OI_MA_CROSS_UP', 'EXIT_PARABOLIC_EXTENSION', '30m', 'bottom_3'),
    ('OI_MA_CROSS_UP', 'EXIT_PARABOLIC_EXTENSION', '15m', 'bottom_3'),
    ('EXIT_FR_ROLL_OVER', 'VOL_ATR_EXPANSION', '60m', 'bottom_10'),
    ('EXIT_FR_ROLL_OVER', 'VOL_ATR_EXPANSION', '15m', 'bottom_5'),
    ('EXIT_MA_DEAD_CROSS', 'FR_POS_NOT_HOT', '15m', 'bottom_10'),
    ('EXIT_MA_DEAD_CROSS', 'FR_POS_NOT_HOT', '5m', 'bottom_10'),
    ('FR_ABSOLUTE_DEEP_NEG', 'FR_POS_NOT_HOT', '15m', 'bottom_3'),
    ('FR_ABSOLUTE_DEEP_NEG', 'FR_POS_NOT_HOT', '5m', 'bottom_1'),
    ('ENTRY_BOTTOM_STABILIZE', 'OI_PRICE_CORR_TURN_POS', '15m', 'top_100'),
    ('ENTRY_BOTTOM_STABILIZE', 'OI_PRICE_CORR_TURN_POS', '30m', 'top_100'),
    ('OI_MA_CROSS_UP', 'EXIT_OI_VALUE_EXTREME', '60m', 'bottom_5'),
    ('OI_MA_CROSS_UP', 'EXIT_OI_VALUE_EXTREME', '5m', 'bottom_5'),
]


# 你指定的需要单独提取的特定组合 (进场信号, 出场信号, 周期, 过滤条件)
long_pair = [
    ('VOLUME_CLIMAX_DOWN', 'ENTRY_SILENT_ACCUMULATION', '60m', 'bottom_3'),
    ('VOLUME_CLIMAX_DOWN', 'ENTRY_SILENT_ACCUMULATION', '30m', 'bottom_5'),
    ('ENTRY_VWAP_RECLAIM_OI', 'FR_LOW_NEG', '30m', 'bottom_10'),
    ('ENTRY_VWAP_RECLAIM_OI', 'FR_LOW_NEG', '5m', 'bottom_10'),
    ('MOM_RECOVERY_FROM_LOW', 'FR_RECOVERY_FROM_LOW', '30m', 'bottom_5'),
    ('MOM_RECOVERY_FROM_LOW', 'FR_RECOVERY_FROM_LOW', '5m', 'bottom_5'),
    ('EXIT_HIGH_VOLUME_BREAKDOWN', 'ENTRY_SILENT_ACCUMULATION', '60m', 'bottom_3'),
    ('EXIT_HIGH_VOLUME_BREAKDOWN', 'ENTRY_SILENT_ACCUMULATION', '30m', 'bottom_5'),
    ('MOM_RECOVERY_FROM_LOW', 'ENTRY_SILENT_ACCUMULATION', '5m', 'bottom_5'),
    ('MOM_RECOVERY_FROM_LOW', 'ENTRY_SILENT_ACCUMULATION', '60m', 'bottom_5'),
    ('EXIT_SHORT_SURGE_EXTREME', 'FR_LOW_NEG', '30m', 'bottom_10'),
    ('EXIT_SHORT_SURGE_EXTREME', 'FR_LOW_NEG', '30m', 'bottom_5'),
    ('VOLUME_Z_SPIKE', 'ENTRY_SILENT_ACCUMULATION', '60m', 'bottom_3'),
    ('VOLUME_Z_SPIKE', 'ENTRY_SILENT_ACCUMULATION', '5m', 'bottom_3'),
    ('ENTRY_TREND_CONFIRM_B', 'FR_EXTREME_LOW', '5m', 'bottom_3'),
    ('ENTRY_TREND_CONFIRM_B', 'FR_EXTREME_LOW', '30m', 'bottom_5'),
    ('STRUCT_SUPPORT_HOLD', 'FR_RECOVERY_FROM_LOW', '30m', 'top_5'),
    ('STRUCT_SUPPORT_HOLD', 'FR_RECOVERY_FROM_LOW', '15m', 'top_5'),
    ('OI_NEW_HIGH', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('OI_NEW_HIGH', 'EXIT_MA_DEAD_CROSS', '15m', 'bottom_5'),
    ('VOL_LOW_TO_HIGH', 'FR_RECOVERY_FROM_LOW', '5m', 'bottom_10'),
    ('VOL_LOW_TO_HIGH', 'FR_RECOVERY_FROM_LOW', '5m', 'bottom_20'),
    ('EXIT_SHORT_SURGE_EXTREME', 'VOL_NOT_EXTREME', '60m', 'bottom_20'),
    ('EXIT_SHORT_SURGE_EXTREME', 'VOL_NOT_EXTREME', '5m', 'bottom_20'),
    ('FR_POS_NOT_HOT', 'FR_RECOVERY_FROM_LOW', '5m', 'bottom_1'),
    ('FR_POS_NOT_HOT', 'FR_RECOVERY_FROM_LOW', '15m', 'bottom_1'),
    ('EXIT_FR_ROLL_OVER', 'ENTRY_SILENT_ACCUMULATION', '15m', 'bottom_10'),
    ('EXIT_FR_ROLL_OVER', 'ENTRY_SILENT_ACCUMULATION', '15m', 'top_10'),
    ('EXIT_LONG_LIQUIDATION_CASCADE_REAL', 'FR_LOW_NEG', '15m', 'bottom_20'),
    ('EXIT_LONG_LIQUIDATION_CASCADE_REAL', 'FR_LOW_NEG', '30m', 'bottom_5'),
    ('EXIT_MULTI_MA_BREAK', 'OI_ROC_PEAK', '15m', 'top_10'),
    ('EXIT_MULTI_MA_BREAK', 'OI_ROC_PEAK', '30m', 'top_10'),
    ('EXIT_OI_ROC_PEAK', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_10'),
    ('EXIT_OI_ROC_PEAK', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('ENTRY_OI_FLASH_SURGE', 'FR_LOW_NEG', '15m', 'top_5'),
    ('ENTRY_OI_FLASH_SURGE', 'FR_LOW_NEG', '30m', 'top_5'),
    ('VWAP_CROSS_UP', 'PRICE_HIGHER_LOWS', '15m', 'bottom_1'),
    ('VWAP_CROSS_UP', 'PRICE_HIGHER_LOWS', '5m', 'bottom_5'),
    ('ENTRY_VWAP_RECLAIM_OI', 'FR_PRICE_BULL_DIV', '5m', 'bottom_5'),
    ('ENTRY_VWAP_RECLAIM_OI', 'FR_PRICE_BULL_DIV', '15m', 'bottom_10'),
    ('OI_LOW_TO_UP', 'EXIT_FR_SPIKE_THEN_COOL', '15m', 'bottom_3'),
    ('OI_LOW_TO_UP', 'EXIT_FR_SPIKE_THEN_COOL', '5m', 'bottom_3'),
    ('VWAP_CROSS_UP', 'STRUCT_RANGE_POSITION_WEAK', '30m', 'bottom_10'),
    ('VWAP_CROSS_UP', 'STRUCT_RANGE_POSITION_WEAK', '30m', 'bottom_5'),
    ('ENTRY_VWAP_RECLAIM_OI', 'EXIT_OI_VALUE_MA_DEAD_CROSS', '15m', 'bottom_10'),
    ('ENTRY_VWAP_RECLAIM_OI', 'EXIT_OI_VALUE_MA_DEAD_CROSS', '60m', 'bottom_5'),
    ('MOM_RECOVERY_FROM_LOW', 'FR_SLOPE_RISING', '60m', 'bottom_1'),
    ('MOM_RECOVERY_FROM_LOW', 'FR_SLOPE_RISING', '60m', 'bottom_3'),
    ('VOLUME_MA_UP', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('VOLUME_MA_UP', 'EXIT_MA_DEAD_CROSS', '60m', 'bottom_5'),
    ('VWAP_RECLAIM', 'FR_LOW_NEG', '5m', 'bottom_5'),
    ('VWAP_RECLAIM', 'FR_LOW_NEG', '30m', 'bottom_5'),
    ('FR_POS_NOT_HOT', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('FR_POS_NOT_HOT', 'EXIT_MA_DEAD_CROSS', '15m', 'bottom_5'),
    ('VWAP_ABOVE', 'EXIT_RANGE_POSITION_WEAK', '30m', 'bottom_5'),
    ('VWAP_ABOVE', 'EXIT_RANGE_POSITION_WEAK', '5m', 'bottom_10'),
    ('VOLUME_Z_SPIKE', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('VOLUME_Z_SPIKE', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_3'),
    ('PRICE_CLOSE_CROSS_MA_UP', 'FR_LOW_NEG', '30m', 'bottom_10'),
    ('PRICE_CLOSE_CROSS_MA_UP', 'FR_LOW_NEG', '5m', 'bottom_10'),
    ('FR_RESET_AFTER_HOT', 'FR_LOW_NEG', '15m', 'top_10'),
    ('FR_RESET_AFTER_HOT', 'FR_LOW_NEG', '60m', 'top_10'),
    ('ENTRY_VWAP_RECLAIM_OI', 'STRUCT_RANGE_POSITION_WEAK', '15m', 'bottom_10'),
    ('ENTRY_VWAP_RECLAIM_OI', 'STRUCT_RANGE_POSITION_WEAK', '30m', 'bottom_10'),
    ('ENTRY_OI_LEAD_MOMENTUM', 'VOL_DOWN_SPIKE', '30m', 'top_10'),
    ('ENTRY_OI_LEAD_MOMENTUM', 'VOL_DOWN_SPIKE', '5m', 'top_10'),
    ('MOM_BURST', 'FR_LOW_NEG', '30m', 'bottom_3'),
    ('MOM_BURST', 'FR_LOW_NEG', '15m', 'bottom_3'),
    ('OI_ACCELERATION', 'EXIT_MA_DEAD_CROSS', '60m', 'bottom_5'),
    ('OI_ACCELERATION', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('VWAP_CROSS_UP', 'FR_LOW_NEG', '30m', 'bottom_10'),
    ('VWAP_CROSS_UP', 'FR_LOW_NEG', '5m', 'bottom_10'),
    ('VOLUME_SPIKE', 'EXIT_MA_DEAD_CROSS', '30m', 'bottom_5'),
    ('VOLUME_SPIKE', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('FR_ZERO_ZONE', 'FR_TURN_POSITIVE', '15m', 'bottom_5'),
    ('FR_ZERO_ZONE', 'FR_TURN_POSITIVE', '30m', 'bottom_5'),
    ('OI_MA_UP', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('OI_MA_UP', 'EXIT_MA_DEAD_CROSS', '15m', 'bottom_5'),
    ('FR_ZERO_ZONE', 'FR_VERY_LOW', '15m', 'bottom_3'),
    ('FR_ZERO_ZONE', 'FR_VERY_LOW', '60m', 'bottom_5'),
    ('PRICE_HEALTHY_EXTENSION', 'FR_LOW_NEG', '30m', 'top_5'),
    ('PRICE_HEALTHY_EXTENSION', 'FR_LOW_NEG', '15m', 'top_3'),
    ('EXIT_VWAP_BREAK', 'FR_PRICE_BULL_DIV', '60m', 'bottom_10'),
    ('EXIT_VWAP_BREAK', 'FR_PRICE_BULL_DIV', '30m', 'bottom_10'),
    ('FR_MILD', 'EXIT_MA_DEAD_CROSS', '30m', 'bottom_3'),
    ('FR_MILD', 'EXIT_MA_DEAD_CROSS', '15m', 'bottom_3'),
    ('STRUCT_SUPPORT_HOLD', 'FR_LOW_NEG', '30m', 'bottom_10'),
    ('STRUCT_SUPPORT_HOLD', 'FR_LOW_NEG', '15m', 'top_5'),
    ('FR_ABSOLUTE_HIGH_POS', 'OI_MA_CROSS_UP', '5m', 'bottom_50'),
    ('FR_ABSOLUTE_HIGH_POS', 'OI_MA_CROSS_UP', '60m', 'bottom_20'),
    ('VOLUME_RANK_HIGH', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_3'),
    ('VOLUME_RANK_HIGH', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('OI_VALUE_HOT_EXTREME', 'FR_TURN_POSITIVE', '5m', 'bottom_1'),
    ('OI_VALUE_HOT_EXTREME', 'FR_TURN_POSITIVE', '5m', 'bottom_5'),
    ('KLINE_RED_BREAK_MA', 'FR_LOW_NEG', '60m', 'top_10'),
    ('KLINE_RED_BREAK_MA', 'FR_LOW_NEG', '30m', 'top_10'),
    ('KLINE_STRONG_RED', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('KLINE_STRONG_RED', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_3'),
    ('VOLUME_EXPAND_PRICE_UP', 'FR_LOW_NEG', '30m', 'bottom_10'),
    ('VOLUME_EXPAND_PRICE_UP', 'FR_LOW_NEG', '60m', 'bottom_20'),
    ('FILTER_NOT_OVERCROWDED', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_3'),
    ('FILTER_NOT_OVERCROWDED', 'EXIT_MA_DEAD_CROSS', '60m', 'bottom_3'),
    ('EXIT_MULTI_MA_BREAK', 'OI_DROP_EXTREME', '60m', 'top_20'),
    ('EXIT_MULTI_MA_BREAK', 'OI_DROP_EXTREME', '60m', 'bottom_5'),
    ('FR_ROLL_OVER_FROM_HIGH', 'FR_LOW_NEG', '15m', 'top_5'),
    ('FR_ROLL_OVER_FROM_HIGH', 'FR_LOW_NEG', '5m', 'top_10'),
    ('ENTRY_VWAP_RECLAIM_OI', 'OI_PRICE_DOWN_OI_DOWN', '15m', 'bottom_20'),
    ('ENTRY_VWAP_RECLAIM_OI', 'OI_PRICE_DOWN_OI_DOWN', '30m', 'bottom_20'),
    ('EXIT_FR_ROLL_OVER', 'EXIT_DISTRIBUTION_EXHAUSTION_TOP', '5m', 'bottom_50'),
    ('EXIT_FR_ROLL_OVER', 'EXIT_DISTRIBUTION_EXHAUSTION_TOP', '30m', 'top_20'),
    ('EXIT_FR_EXTREME_HIGH', 'OI_MA_CROSS_UP', '5m', 'bottom_10'),
    ('EXIT_FR_EXTREME_HIGH', 'OI_MA_CROSS_UP', '30m', 'bottom_20'),
    ('FR_POS_NOT_HOT', 'FR_LOW_NEG', '60m', 'bottom_3'),
    ('FR_POS_NOT_HOT', 'FR_LOW_NEG', '30m', 'bottom_1'),
    ('EXIT_MULTI_MA_BREAK', 'EXIT_MA_DEAD_CROSS', '5m', 'bottom_5'),
    ('EXIT_MULTI_MA_BREAK', 'EXIT_MA_DEAD_CROSS', '30m', 'bottom_5'),
    ('FR_HIGH_EXTREME', 'FR_LOW_NEG', '60m', 'top_10'),
    ('FR_HIGH_EXTREME', 'FR_LOW_NEG', '60m', 'bottom_10'),
    ('VWAP_RECLAIM', 'FR_PRICE_BULL_DIV', '5m', 'bottom_3'),
    ('VWAP_RECLAIM', 'FR_PRICE_BULL_DIV', '15m', 'bottom_3'),
    ('FR_PRICE_UP_HOT', 'FR_LOW_NEG', '60m', 'top_10'),
    ('FR_PRICE_UP_HOT', 'FR_LOW_NEG', '15m', 'top_10'),
    ('VOLUME_HIGH_CLOSE_STRONG', 'FR_LOW_NEG', '60m', 'bottom_20'),
    ('VOLUME_HIGH_CLOSE_STRONG', 'FR_LOW_NEG', '60m', 'bottom_10'),
    ('OI_LOW_TO_UP', 'FR_SPIKE_UP', '60m', 'bottom_5'),
    ('OI_LOW_TO_UP', 'FR_SPIKE_UP', '15m', 'bottom_3'),
    ('EXIT_FR_EXTREME_HIGH', 'FR_LOW_NEG', '15m', 'bottom_20'),
    ('EXIT_FR_EXTREME_HIGH', 'FR_LOW_NEG', '30m', 'bottom_20'),
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
    df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\extracted_raw_trades\extracted_target_pairs.csv')
    main()