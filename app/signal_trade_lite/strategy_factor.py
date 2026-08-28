import gc
import os
import time

import pandas as pd
import numpy as np


# ==================== 一、价格偏离与位置类 ====================

def factor_001(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    mean_60 = df['close'].rolling(window_60).mean()
    std_60 = df['close'].rolling(window_60).std()
    z_score = (df['close'] - mean_60) / std_60.replace(0, 1e-9)

    q_val = z_score.rolling(window_1440).quantile(q_low)
    df['signal'] = (z_score < q_val).fillna(False).astype(bool)
    return df


def factor_002(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    mean_60 = df['close'].rolling(window_60).mean()
    std_60 = df['close'].rolling(window_60).std()
    z_score = (df['close'] - mean_60) / std_60.replace(0, 1e-9)

    q_val = z_score.rolling(window_1440).quantile(q_high)
    df['signal'] = (z_score > q_val).fillna(False).astype(bool)
    return df


def factor_003(df):
    window_240 = 240
    p_low = 0.05

    high_240 = df['high'].rolling(window_240).max()
    low_240 = df['low'].rolling(window_240).min()
    position = (df['close'] - low_240) / (high_240 - low_240).replace(0, 1e-9)

    df['signal'] = (position <= p_low).fillna(False).astype(bool)
    return df


def factor_004(df):
    window_240 = 240
    p_high = 0.95

    high_240 = df['high'].rolling(window_240).max()
    low_240 = df['low'].rolling(window_240).min()
    position = (df['close'] - low_240) / (high_240 - low_240).replace(0, 1e-9)

    df['signal'] = (position >= p_high).fillna(False).astype(bool)
    return df


def factor_005(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    vwap_60 = (df['close'] * df['volume']).rolling(window_60).sum() / df['volume'].rolling(window_60).sum().replace(0, 1e-9)
    dev = (df['close'] - vwap_60) / vwap_60.replace(0, 1e-9)

    q_val = dev.rolling(window_1440).quantile(q_low)
    df['signal'] = (dev < q_val).fillna(False).astype(bool)
    return df


def factor_006(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    vwap_60 = (df['close'] * df['volume']).rolling(window_60).sum() / df['volume'].rolling(window_60).sum().replace(0, 1e-9)
    dev = (df['close'] - vwap_60) / vwap_60.replace(0, 1e-9)

    q_val = dev.rolling(window_1440).quantile(q_high)
    df['signal'] = (dev > q_val).fillna(False).astype(bool)
    return df


def factor_007(df):
    window_60 = 60
    lag_15 = 15
    window_1440 = 1440
    q_low = 0.05

    mean_60 = df['close'].rolling(window_60).mean()
    std_60 = df['close'].rolling(window_60).std()
    z_score = (df['close'] - mean_60) / std_60.replace(0, 1e-9)

    z_q = z_score.rolling(window_1440).quantile(q_low)
    cond1 = z_score < z_q
    cond2 = z_score > z_score.shift(lag_15)

    df['signal'] = (cond1 & cond2).fillna(False).astype(bool)
    return df


def factor_008(df):
    window_60 = 60
    lag_15 = 15
    window_1440 = 1440
    q_high = 0.95

    mean_60 = df['close'].rolling(window_60).mean()
    std_60 = df['close'].rolling(window_60).std()
    z_score = (df['close'] - mean_60) / std_60.replace(0, 1e-9)

    z_q = z_score.rolling(window_1440).quantile(q_high)
    cond1 = z_score > z_q
    cond2 = z_score < z_score.shift(lag_15)

    df['signal'] = (cond1 & cond2).fillna(False).astype(bool)
    return df


# ==================== 二、动量与路径结构类 ====================

def factor_009(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    ret_60 = df['close'] / df['close'].shift(window_60).replace(0, 1e-9) - 1
    q_val = ret_60.rolling(window_1440).quantile(q_low)

    df['signal'] = (ret_60 < q_val).fillna(False).astype(bool)
    return df


def factor_010(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    ret_60 = df['close'] / df['close'].shift(window_60).replace(0, 1e-9) - 1
    q_val = ret_60.rolling(window_1440).quantile(q_high)

    df['signal'] = (ret_60 > q_val).fillna(False).astype(bool)
    return df


def factor_011(df):
    window_15 = 15
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    ret_60 = df['close'] / df['close'].shift(window_60).replace(0, 1e-9) - 1
    accel = ret_15 - ret_60

    q_val = ret_60.rolling(window_1440).quantile(q_low)
    df['signal'] = ((ret_60 < q_val) & (accel > 0)).fillna(False).astype(bool)
    return df


def factor_012(df):
    window_15 = 15
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    ret_60 = df['close'] / df['close'].shift(window_60).replace(0, 1e-9) - 1
    accel = ret_15 - ret_60

    q_val = ret_60.rolling(window_1440).quantile(q_high)
    df['signal'] = ((ret_60 > q_val) & (accel < 0)).fillna(False).astype(bool)
    return df


def factor_013(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    net_disp = (df['close'] - df['close'].shift(window_60)).abs()
    path_len = df['close'].diff().abs().rolling(window_60).sum()
    path_eff = net_disp / path_len.replace(0, 1e-9)

    q_val = path_eff.rolling(window_1440).quantile(q_high)
    df['signal'] = (path_eff > q_val).fillna(False).astype(bool)
    return df


def factor_014_high(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    ret_1 = df['close'].pct_change()
    autocorr = ret_1.rolling(window_60).corr(ret_1.shift(1))

    q_val = autocorr.rolling(window_1440).quantile(q_high)
    df['signal'] = (autocorr > q_val).fillna(False).astype(bool)
    return df


def factor_014_low(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    ret_1 = df['close'].pct_change()
    autocorr = ret_1.rolling(window_60).corr(ret_1.shift(1))

    q_val = autocorr.rolling(window_1440).quantile(q_low)
    df['signal'] = (autocorr < q_val).fillna(False).astype(bool)
    return df


def factor_015(df):
    window_1440 = 1440
    q_high = 0.95

    up = df['close'] > df['close'].shift(1)
    # 巧妙利用 groupby 和 cumsum 计算连续上涨次数
    streak_up = up.groupby((~up).cumsum()).cumcount() + 1
    streak_up = streak_up.where(up, 0)

    q_val = streak_up.rolling(window_1440).quantile(q_high)
    df['signal'] = ((streak_up > q_val) & (streak_up > 0)).fillna(False).astype(bool)
    return df


def factor_016(df):
    window_1440 = 1440
    q_high = 0.95

    down = df['close'] < df['close'].shift(1)
    streak_down = down.groupby((~down).cumsum()).cumcount() + 1
    streak_down = streak_down.where(down, 0)

    q_val = streak_down.rolling(window_1440).quantile(q_high)
    df['signal'] = ((streak_down > q_val) & (streak_down > 0)).fillna(False).astype(bool)
    return df


def factor_017(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    diff = df['close'].diff()
    sign = np.sign(diff)
    block = (sign != sign.shift(1)).cumsum()
    # 计算每段连续同向的累计收益绝对值
    cum_disp = diff.groupby(block).cumsum().abs()

    max_disp = cum_disp.rolling(window_60).max()
    q_val = max_disp.rolling(window_1440).quantile(q_high)

    df['signal'] = (max_disp > q_val).fillna(False).astype(bool)
    return df


# ==================== 三、波动率与振幅类 ====================

def factor_018_high(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift(1)).abs()
    tr3 = (df['low'] - df['close'].shift(1)).abs()
    tr = tr1.combine(tr2, max).combine(tr3, max)

    atr_pct = tr.rolling(window_60).mean() / df['close'].replace(0, 1e-9)
    q_val = atr_pct.rolling(window_1440).quantile(q_high)
    df['signal'] = (atr_pct > q_val).fillna(False).astype(bool)
    return df


def factor_018_low(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    tr1 = df['high'] - df['low']
    tr2 = (df['high'] - df['close'].shift(1)).abs()
    tr3 = (df['low'] - df['close'].shift(1)).abs()
    tr = tr1.combine(tr2, max).combine(tr3, max)

    atr_pct = tr.rolling(window_60).mean() / df['close'].replace(0, 1e-9)
    q_val = atr_pct.rolling(window_1440).quantile(q_low)
    df['signal'] = (atr_pct < q_val).fillna(False).astype(bool)
    return df


def factor_019_high(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    ret_std = df['close'].pct_change().rolling(window_60).std()
    q_val = ret_std.rolling(window_1440).quantile(q_high)
    df['signal'] = (ret_std > q_val).fillna(False).astype(bool)
    return df


def factor_019_low(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    ret_std = df['close'].pct_change().rolling(window_60).std()
    q_val = ret_std.rolling(window_1440).quantile(q_low)
    df['signal'] = (ret_std < q_val).fillna(False).astype(bool)
    return df


def factor_020_high(df):
    window_15 = 15
    window_240 = 240
    window_1440 = 1440
    q_high = 0.95

    ret = df['close'].pct_change()
    std_15 = ret.rolling(window_15).std()
    std_240 = ret.rolling(window_240).std()
    ratio = std_15 / std_240.replace(0, 1e-9)

    q_val = ratio.rolling(window_1440).quantile(q_high)
    df['signal'] = (ratio > q_val).fillna(False).astype(bool)
    return df


def factor_020_low(df):
    window_15 = 15
    window_240 = 240
    window_1440 = 1440
    q_low = 0.05

    ret = df['close'].pct_change()
    std_15 = ret.rolling(window_15).std()
    std_240 = ret.rolling(window_240).std()
    ratio = std_15 / std_240.replace(0, 1e-9)

    q_val = ratio.rolling(window_1440).quantile(q_low)
    df['signal'] = (ratio < q_val).fillna(False).astype(bool)
    return df


def factor_021(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    amplitude = df['high'] - df['low']
    mean_amp = amplitude.rolling(window_60).mean()
    ratio = amplitude / mean_amp.replace(0, 1e-9)

    q_val = ratio.rolling(window_1440).quantile(q_high)
    df['signal'] = (ratio > q_val).fillna(False).astype(bool)
    return df


def factor_022(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    amp_pct = (df['high'] - df['low']) / df['close'].replace(0, 1e-9)
    mean_amp_pct = amp_pct.rolling(window_60).mean()
    ret_std = df['close'].pct_change().rolling(window_60).std()

    ratio = mean_amp_pct / ret_std.replace(0, 1e-9)
    q_val = ratio.rolling(window_1440).quantile(q_high)

    df['signal'] = (ratio > q_val).fillna(False).astype(bool)
    return df


# ==================== 四、K 线微观结构类 ====================

def factor_023(df):
    window_5 = 5
    window_1440 = 1440
    q_low = 0.05

    k_pos = (df['close'] - df['low']) / (df['high'] - df['low']).replace(0, 1e-9)
    mean_pos = k_pos.rolling(window_5).mean()

    q_val = mean_pos.rolling(window_1440).quantile(q_low)
    df['signal'] = (mean_pos < q_val).fillna(False).astype(bool)
    return df


def factor_024(df):
    window_5 = 5
    window_1440 = 1440
    q_high = 0.95

    k_pos = (df['close'] - df['low']) / (df['high'] - df['low']).replace(0, 1e-9)
    mean_pos = k_pos.rolling(window_5).mean()

    q_val = mean_pos.rolling(window_1440).quantile(q_high)
    df['signal'] = (mean_pos > q_val).fillna(False).astype(bool)
    return df


def factor_025(df):
    window_15 = 15
    window_1440 = 1440
    q_high = 0.95

    body = (df['close'] - df['open']).abs()
    amp = (df['high'] - df['low']).replace(0, 1e-9)
    body_ratio = body / amp

    mean_ratio = body_ratio.rolling(window_15).mean()
    q_val = mean_ratio.rolling(window_1440).quantile(q_high)

    df['signal'] = (mean_ratio > q_val).fillna(False).astype(bool)
    return df


def factor_026(df):
    window_15 = 15
    window_1440 = 1440
    q_low = 0.05

    upper = df['high'] - df[['open', 'close']].max(axis=1)
    lower = df[['open', 'close']].min(axis=1) - df['low']
    amp = (df['high'] - df['low']).replace(0, 1e-9)

    imb = (upper - lower) / amp
    mean_imb = imb.rolling(window_15).mean()

    q_val = mean_imb.rolling(window_1440).quantile(q_low)
    df['signal'] = (mean_imb < q_val).fillna(False).astype(bool)
    return df


def factor_027(df):
    window_15 = 15
    window_1440 = 1440
    q_high = 0.95

    upper = df['high'] - df[['open', 'close']].max(axis=1)
    lower = df[['open', 'close']].min(axis=1) - df['low']
    amp = (df['high'] - df['low']).replace(0, 1e-9)

    imb = (upper - lower) / amp
    mean_imb = imb.rolling(window_15).mean()

    q_val = mean_imb.rolling(window_1440).quantile(q_high)
    df['signal'] = (mean_imb > q_val).fillna(False).astype(bool)
    return df


def factor_028(df):
    window_1440 = 1440
    q_low = 0.05

    gap = df['open'] / df['close'].shift(1).replace(0, 1e-9) - 1
    q_val = gap.rolling(window_1440).quantile(q_low)

    df['signal'] = (gap < q_val).fillna(False).astype(bool)
    return df


def factor_029(df):
    window_1440 = 1440
    q_high = 0.95

    gap = df['open'] / df['close'].shift(1).replace(0, 1e-9) - 1
    q_val = gap.rolling(window_1440).quantile(q_high)

    df['signal'] = (gap > q_val).fillna(False).astype(bool)
    return df


def factor_030(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    jump = (df['open'] - df['close'].shift(1)).abs()
    amp = df['high'] - df['low']

    jump_sum = jump.rolling(window_60).sum()
    amp_sum = amp.rolling(window_60).sum()
    ratio = jump_sum / amp_sum.replace(0, 1e-9)

    q_val = ratio.rolling(window_1440).quantile(q_high)
    df['signal'] = (ratio > q_val).fillna(False).astype(bool)
    return df


# ==================== 五、成交量与流动性类 ====================

def factor_031_high(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    v_mean = df['volume'].rolling(window_60).mean()
    v_std = df['volume'].rolling(window_60).std()
    v_z = (df['volume'] - v_mean) / v_std.replace(0, 1e-9)

    q_val = v_z.rolling(window_1440).quantile(q_high)
    df['signal'] = (v_z > q_val).fillna(False).astype(bool)
    return df


def factor_031_low(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    v_mean = df['volume'].rolling(window_60).mean()
    v_std = df['volume'].rolling(window_60).std()
    v_z = (df['volume'] - v_mean) / v_std.replace(0, 1e-9)

    q_val = v_z.rolling(window_1440).quantile(q_low)
    df['signal'] = (v_z < q_val).fillna(False).astype(bool)
    return df


def factor_032(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    mean_amp = (df['high'] - df['low']).rolling(window_60).mean()
    mean_vol = df['volume'].rolling(window_60).mean()
    impact = mean_amp / mean_vol.replace(0, 1e-9)

    q_val = impact.rolling(window_1440).quantile(q_high)
    df['signal'] = (impact > q_val).fillna(False).astype(bool)
    return df


# ==================== 六、量价关系类 ====================

def factor_033(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    is_up = df['close'] > df['open']
    up_vol = df['volume'].where(is_up, 0)

    up_vol_ratio = up_vol.rolling(window_60).sum() / df['volume'].rolling(window_60).sum().replace(0, 1e-9)
    q_val = up_vol_ratio.rolling(window_1440).quantile(q_high)

    df['signal'] = (up_vol_ratio > q_val).fillna(False).astype(bool)
    return df


def factor_034(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    is_up = df['close'] > df['open']
    up_vol = df['volume'].where(is_up, 0)

    up_vol_ratio = up_vol.rolling(window_60).sum() / df['volume'].rolling(window_60).sum().replace(0, 1e-9)
    q_val = up_vol_ratio.rolling(window_1440).quantile(q_low)

    df['signal'] = (up_vol_ratio < q_val).fillna(False).astype(bool)
    return df


def factor_035_high(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    ret = df['close'].pct_change()
    corr = ret.rolling(window_60).corr(df['volume'])

    q_val = corr.rolling(window_1440).quantile(q_high)
    df['signal'] = (corr > q_val).fillna(False).astype(bool)
    return df


def factor_035_low(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    ret = df['close'].pct_change()
    corr = ret.rolling(window_60).corr(df['volume'])

    q_val = corr.rolling(window_1440).quantile(q_low)
    df['signal'] = (corr < q_val).fillna(False).astype(bool)
    return df


def factor_036(df):
    window_60 = 60
    window_1440 = 1440
    q_low = 0.05

    # 压力用 (High - Close)/(High - Low) 表示
    k_pressure = (df['high'] - df['close']) / (df['high'] - df['low']).replace(0, 1e-9)
    vw_pressure = (k_pressure * df['volume']).rolling(window_60).sum() / df['volume'].rolling(window_60).sum().replace(0, 1e-9)

    q_val = vw_pressure.rolling(window_1440).quantile(q_low)
    df['signal'] = (vw_pressure < q_val).fillna(False).astype(bool)
    return df


def factor_037(df):
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    k_pressure = (df['high'] - df['close']) / (df['high'] - df['low']).replace(0, 1e-9)
    vw_pressure = (k_pressure * df['volume']).rolling(window_60).sum() / df['volume'].rolling(window_60).sum().replace(0, 1e-9)

    q_val = vw_pressure.rolling(window_1440).quantile(q_high)
    df['signal'] = (vw_pressure > q_val).fillna(False).astype(bool)
    return df


def factor_038(df):
    window_15 = 15
    window_dist_240 = 240
    window_vol_60 = 60
    q_high = 0.95

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    mean_vol_15 = df['volume'].rolling(window_15).mean()

    ret_q = ret_15.rolling(window_dist_240).quantile(q_high)
    vol_q = mean_vol_15.rolling(window_vol_60).quantile(q_high)

    cond = (ret_15 > ret_q) & (mean_vol_15 > vol_q)
    df['signal'] = cond.fillna(False).astype(bool)
    return df


def factor_039(df):
    window_15 = 15
    window_dist_240 = 240
    window_vol_60 = 60
    q_low = 0.05
    q_high = 0.95

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    mean_vol_15 = df['volume'].rolling(window_15).mean()

    ret_q = ret_15.rolling(window_dist_240).quantile(q_low)
    vol_q = mean_vol_15.rolling(window_vol_60).quantile(q_high)

    cond = (ret_15 < ret_q) & (mean_vol_15 > vol_q)
    df['signal'] = cond.fillna(False).astype(bool)
    return df


# ==================== 七、区间、压缩与突破类 ====================

def factor_040_high(df):
    window_240 = 240
    window_1440 = 1440
    q_high = 0.95

    high_240 = df['high'].rolling(window_240).max()
    low_240 = df['low'].rolling(window_240).min()
    width = (high_240 - low_240) / df['close'].replace(0, 1e-9)

    q_val = width.rolling(window_1440).quantile(q_high)
    df['signal'] = (width > q_val).fillna(False).astype(bool)
    return df


def factor_040_low(df):
    window_240 = 240
    window_1440 = 1440
    q_low = 0.05

    high_240 = df['high'].rolling(window_240).max()
    low_240 = df['low'].rolling(window_240).min()
    width = (high_240 - low_240) / df['close'].replace(0, 1e-9)

    q_val = width.rolling(window_1440).quantile(q_low)
    df['signal'] = (width < q_val).fillna(False).astype(bool)
    return df


def factor_041(df):
    window_60 = 60
    base_240 = 240
    window_1440 = 1440
    q_high = 0.95

    max_240_prev = df['high'].rolling(base_240).max().shift(1)
    min_240_prev = df['low'].rolling(base_240).min().shift(1)

    is_new_high = (df['close'] > max_240_prev).astype(int)
    is_new_low = (df['close'] < min_240_prev).astype(int)

    net_times = is_new_high.rolling(window_60).sum() - is_new_low.rolling(window_60).sum()
    q_val = net_times.rolling(window_1440).quantile(q_high)

    df['signal'] = (net_times > q_val).fillna(False).astype(bool)
    return df


def factor_042(df):
    window_60 = 60
    base_240 = 240
    window_1440 = 1440
    q_low = 0.05

    max_240_prev = df['high'].rolling(base_240).max().shift(1)
    min_240_prev = df['low'].rolling(base_240).min().shift(1)

    is_new_high = (df['close'] > max_240_prev).astype(int)
    is_new_low = (df['close'] < min_240_prev).astype(int)

    net_times = is_new_high.rolling(window_60).sum() - is_new_low.rolling(window_60).sum()
    q_val = net_times.rolling(window_1440).quantile(q_low)

    df['signal'] = (net_times < q_val).fillna(False).astype(bool)
    return df


def factor_043(df):
    window_15 = 15
    base_240 = 240

    # 过去的240极值（不包含这15根K线）
    boundary = df['high'].shift(window_15).rolling(base_240).max()

    # 过去15根内曾突破该边界
    has_breakout = df['high'].rolling(window_15).max() > boundary
    # 当前跌回边界下方
    is_recycled = df['close'] < boundary

    df['signal'] = (has_breakout & is_recycled).fillna(False).astype(bool)
    return df


def factor_044(df):
    window_15 = 15
    base_240 = 240

    boundary = df['low'].shift(window_15).rolling(base_240).min()

    has_breakout = df['low'].rolling(window_15).min() < boundary
    is_recycled = df['close'] > boundary

    df['signal'] = (has_breakout & is_recycled).fillna(False).astype(bool)
    return df


def factor_045_high(df):
    base_240 = 240
    window_1440 = 1440
    q_high = 0.95

    # argmax返回窗口内最大值的相对索引(0到239)
    # 距离当前的分钟数即为 (窗口大小 - 1) - 相对索引
    dist_max = (base_240 - 1) - df['high'].rolling(base_240).apply(np.argmax, raw=True)
    dist_min = (base_240 - 1) - df['low'].rolling(base_240).apply(np.argmin, raw=True)
    min_dist = np.minimum(dist_max, dist_min)

    q_val = min_dist.rolling(window_1440).quantile(q_high)
    df['signal'] = (min_dist > q_val).fillna(False).astype(bool)
    return df


def factor_045_low(df):
    base_240 = 240
    window_1440 = 1440
    q_low = 0.05

    dist_max = (base_240 - 1) - df['high'].rolling(base_240).apply(np.argmax, raw=True)
    dist_min = (base_240 - 1) - df['low'].rolling(base_240).apply(np.argmin, raw=True)
    min_dist = np.minimum(dist_max, dist_min)

    q_val = min_dist.rolling(window_1440).quantile(q_low)
    df['signal'] = (min_dist < q_val).fillna(False).astype(bool)
    return df


# ==================== 八、尾部风险与分布类 ====================

def factor_046(df):
    window_240 = 240
    window_1440 = 1440
    q_high = 0.95

    skew = df['close'].pct_change().rolling(window_240).skew()
    q_val = skew.rolling(window_1440).quantile(q_high)

    df['signal'] = (skew > q_val).fillna(False).astype(bool)
    return df


def factor_047(df):
    window_240 = 240
    window_1440 = 1440
    q_low = 0.05

    skew = df['close'].pct_change().rolling(window_240).skew()
    q_val = skew.rolling(window_1440).quantile(q_low)

    df['signal'] = (skew < q_val).fillna(False).astype(bool)
    return df


def factor_048(df):
    window_240 = 240
    window_1440 = 1440
    q_high = 0.95

    kurt = df['close'].pct_change().rolling(window_240).kurt()
    q_val = kurt.rolling(window_1440).quantile(q_high)

    df['signal'] = (kurt > q_val).fillna(False).astype(bool)
    return df


def factor_049(df):
    window_240 = 240
    window_1440 = 1440
    q_high_k = 0.95
    q_high_freq = 0.95

    ret_abs = df['close'].pct_change().abs()
    threshold = ret_abs.rolling(window_1440).quantile(q_high_k)

    is_extreme = (ret_abs > threshold).astype(int)
    extreme_count = is_extreme.rolling(window_240).sum()

    q_val = extreme_count.rolling(window_1440).quantile(q_high_freq)
    df['signal'] = (extreme_count > q_val).fillna(False).astype(bool)
    return df


def factor_050(df):
    window_240 = 240
    window_1440 = 1440
    q_high = 0.95

    ret = df['close'].pct_change()
    up_ret = ret.where(ret > 0, np.nan)
    down_ret = ret.where(ret < 0, np.nan)

    # 增加 min_periods，只要过去240分钟内有60根上涨/下跌K线即可计算标准差
    min_p = window_240 // 4
    up_std = up_ret.rolling(window_240, min_periods=min_p).std()
    down_std = down_ret.rolling(window_240, min_periods=min_p).std()
    ratio = up_std / down_std.replace(0, 1e-9)

    q_val = ratio.rolling(window_1440).quantile(q_high)
    df['signal'] = (ratio > q_val).fillna(False).astype(bool)
    return df


def factor_051(df):
    window_240 = 240
    window_1440 = 1440
    q_low = 0.05

    ret = df['close'].pct_change()
    up_ret = ret.where(ret > 0, np.nan)
    down_ret = ret.where(ret < 0, np.nan)

    min_p = window_240 // 4
    up_std = up_ret.rolling(window_240, min_periods=min_p).std()
    down_std = down_ret.rolling(window_240, min_periods=min_p).std()
    ratio = up_std / down_std.replace(0, 1e-9)

    q_val = ratio.rolling(window_1440).quantile(q_low)
    df['signal'] = (ratio < q_val).fillna(False).astype(bool)
    return df


# ==================== 九、时间结构类 ====================

def factor_052(df):
    # 默认选出每天 0 点（UTC）作为信号触发，方便测试，可自行更改
    target_hour = 0

    dt = pd.to_datetime(df['open_time'], unit='ms')
    df['signal'] = (dt.dt.hour == target_hour).fillna(False).astype(bool)
    return df


def factor_053(df):
    # 默认选出周一 (0=周一, 6=周日) 作为信号触发
    target_weekday = 0

    dt = pd.to_datetime(df['open_time'], unit='ms')
    df['signal'] = (dt.dt.dayofweek == target_weekday).fillna(False).astype(bool)
    return df


def factor_054(df):
    threshold_min = 15
    period_hours = 4

    dt = pd.to_datetime(df['open_time'], unit='ms')
    mins_of_day = dt.dt.hour * 60 + dt.dt.minute
    period_mins = period_hours * 60

    mod_val = mins_of_day % period_mins
    # 计算距离边界的绝对分钟数（向上或向下的最近距离）
    dist = np.minimum(mod_val, period_mins - mod_val)

    df['signal'] = (dist <= threshold_min).fillna(False).astype(bool)
    return df


def factor_055(df):
    threshold_min = 15
    period_hours = 8

    dt = pd.to_datetime(df['open_time'], unit='ms')
    mins_of_day = dt.dt.hour * 60 + dt.dt.minute
    period_mins = period_hours * 60

    mod_val = mins_of_day % period_mins
    dist = np.minimum(mod_val, period_mins - mod_val)

    df['signal'] = (dist <= threshold_min).fillna(False).astype(bool)
    return df


# ==================== 十、跨周期一致性类 ====================

def factor_056(df):
    window_15 = 15
    window_240 = 240

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    ret_240 = df['close'] / df['close'].shift(window_240).replace(0, 1e-9) - 1

    cond = (ret_15 > 0) & (ret_240 > 0)
    df['signal'] = cond.fillna(False).astype(bool)
    return df


def factor_057(df):
    window_15 = 15
    window_240 = 240

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    ret_240 = df['close'] / df['close'].shift(window_240).replace(0, 1e-9) - 1

    cond = (ret_15 < 0) & (ret_240 < 0)
    df['signal'] = cond.fillna(False).astype(bool)
    return df


# ==================== 十一、短周期异常类 ====================

def factor_058(df):
    window_15 = 15
    window_240 = 240
    q_high = 0.95

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    q_val = ret_15.rolling(window_240).quantile(q_high)

    df['signal'] = (ret_15 > q_val).fillna(False).astype(bool)
    return df


def factor_059(df):
    window_15 = 15
    window_240 = 240
    q_low = 0.05

    ret_15 = df['close'] / df['close'].shift(window_15).replace(0, 1e-9) - 1
    q_val = ret_15.rolling(window_240).quantile(q_low)

    df['signal'] = (ret_15 < q_val).fillna(False).astype(bool)
    return df


def factor_060(df):
    window_15 = 15
    window_60 = 60
    window_1440 = 1440
    q_high = 0.95

    high_15 = df['high'].rolling(window_15).max()
    low_15 = df['low'].rolling(window_15).min()
    amp_15 = high_15 - low_15

    mean_amp_60 = (df['high'] - df['low']).rolling(window_60).mean()
    ratio = amp_15 / mean_amp_60.replace(0, 1e-9)

    q_val = ratio.rolling(window_1440).quantile(q_high)
    df['signal'] = (ratio > q_val).fillna(False).astype(bool)
    return df

def evaluate_all_signal_rates(symbols, strategies, base_path, file_suffix="_1m_2021-01-01_merged.csv"):
    """
    独立计算并客观打印每个策略在各个币种上的信号率。

    参数:
    - symbols: list，币种列表 (例如 ["BTCUSDT", "ETHUSDT"])
    - strategies: list，策略函数列表
    - base_path: str，K线数据存放的文件夹路径
    - file_suffix: str，K线文件名的后缀

    返回:
    - df_results: pd.DataFrame，包含所有信号率的二维表
    """
    print("=" * 80)
    print("开始执行多币种-多策略【信号率】客观统计...")
    print("=" * 80)

    # 使用 float32 节省内存
    CSV_DTYPES = {"open_time": np.int64, "open": np.float32, "high": np.float32,
                  "low": np.float32, "close": np.float32, "volume": np.float32}

    # 存储最终结果的嵌套字典：results[symbol][strategy_name] = rate
    results_dict = {}

    t_start = time.time()

    for sym_i, symbol in enumerate(symbols, 1):
        file_path = os.path.join(base_path, f"{symbol}{file_suffix}")

        if not os.path.exists(file_path):
            print(f"[警告] 找不到对应的 K 线文件，跳过: {file_path}")
            results_dict[symbol] = {strat.__name__: np.nan for strat in strategies}
            continue

        print(f"[{sym_i}/{len(symbols)}] 正在加载 {symbol} ...", end=" ", flush=True)
        t0 = time.time()
        try:
            df_main = pd.read_csv(file_path, dtype=CSV_DTYPES)
        except Exception:
            df_main = pd.read_csv(file_path)  # 回退默认读取

        total_bars = len(df_main)
        print(f"成功 (共 {total_bars} 行, 耗时 {time.time() - t0:.1f}s)")

        symbol_rates = {}
        for strat in strategies:
            strat_name = strat.__name__
            try:
                # 浅拷贝送入策略计算，节约内存
                df_strat = strat(df_main.copy(deep=False))

                # 统计客观信号率
                n_sig = df_strat['signal'].fillna(False).astype(np.int8).sum()
                rate = (n_sig / total_bars) * 100.0
                symbol_rates[strat_name] = rate

            except Exception as e:
                print(f"  -> [错误] {strat_name} 运行异常: {e}")
                symbol_rates[strat_name] = np.nan

            finally:
                if 'df_strat' in locals():
                    del df_strat
                gc.collect()

        results_dict[symbol] = symbol_rates

        # 释放单个币种的 K线 DataFrame
        del df_main
        gc.collect()

    # ==========================
    # 格式化输出客观二维表
    # ==========================
    df_results = pd.DataFrame(results_dict)

    print("\n\n" + "=" * 90)
    print(" " * 32 + "【 策 略 信 号 率 统 计 表 】")
    print("=" * 90)

    # 设置 pandas 显示选项以确保完全展开对齐展示
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.float_format', '{:.4f}%'.format)

    print(df_results)

    print("=" * 90)
    print(f"统计完成，总耗时: {time.time() - t_start:.2f} 秒")
    print("=" * 90)

    # 恢复默认 pandas 设置
    pd.reset_option('display.max_rows')
    pd.reset_option('display.max_columns')
    pd.reset_option('display.width')
    pd.reset_option('display.float_format')

    return df_results


if __name__ == "__main__":
    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "LINKUSDT",
        "AAVEUSDT",
        "BNBUSDT"
    ]

    strategies = [
        # 一、价格偏离与位置类
        factor_001, factor_002, factor_003, factor_004,
        factor_005, factor_006, factor_007, factor_008,

        # 二、动量与路径结构类
        factor_009, factor_010, factor_011, factor_012,
        factor_013, factor_014_high, factor_014_low, factor_015, factor_016,
        factor_017,

        # 三、波动率与振幅类
        factor_018_high, factor_018_low, factor_019_high, factor_019_low,
        factor_020_high, factor_020_low, factor_021, factor_022,

        # 四、K 线微观结构类
        factor_023, factor_024, factor_025, factor_026,
        factor_027, factor_028, factor_029, factor_030,

        # 五、成交量与流动性类
        factor_031_high, factor_031_low, factor_032,

        # 六、量价关系类
        factor_033, factor_034, factor_035_high, factor_035_low,
        factor_036, factor_037, factor_038, factor_039,

        # 七、区间、压缩与突破类
        factor_040_high, factor_040_low, factor_041, factor_042,
        factor_043, factor_044, factor_045_high, factor_045_low,

        # 八、尾部风险与分布类
        factor_046, factor_047, factor_048, factor_049,
        factor_050, factor_051,

        # 九、时间结构类
        factor_052, factor_053, factor_054, factor_055,

        # 十、跨周期一致性类
        factor_056, factor_057,

        # 十一、短周期异常类
        factor_058, factor_059, factor_060
    ]


    # 测试参数
    symbols = ["BTCUSDT"]
    base_path = r"W:\project\python_project\oke_auto_trade\kline_data"

    # 调用上方提供的函数，单独输出信号率报告
    df_rates = evaluate_all_signal_rates(symbols, strategies, base_path)