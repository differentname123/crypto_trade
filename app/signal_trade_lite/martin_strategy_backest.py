# -*- coding: utf-8 -*-
"""
======================================================================
加密货币马丁格尔策略 —— 双阶段高频回测引擎（"抽本利润跑"模式专用）
======================================================================
Stage 1 : 无限保证金平行宇宙生成 -> cycles_df (含浮亏阶梯表 dd_steps)
Stage 2 : 浮亏阶梯查表重组       -> trades_df (任意 Margin, 毫秒级)
Stage 3 : Free-Ride 指标评估      -> report

输入: 一个 DataFrame, 必需列:
    open_time (ms 级 int 时间戳, 严格递增), open, high, low, close, volume
    信号列: long_signal / short_signal  (1/0 或 True/False, 在该 bar 收盘成交)

重要单位约定
------------
Margin 的单位是"首单名义价值的倍数"(1 Unit = 首单 Notional)。
例: margin=2.55 表示账户可承受 2.55 倍首单名义价值的资金缺口 (约撑到第 10 层)。
用 build_ladder() 查表选 Margin。

性能说明 (Stage 1)
------------------
1) _simulate_cycle 由 numba 编译为机器码 (njit, nogil, cache), 不开 fastmath,
   表达式书写顺序与原纯 Python 版完全一致 => 浮点结果位级等价。
2) 浮亏阶梯在 njit 内部用"倍增动态数组"记录 (不是定长预分配), 既不越界也不吃内存。
   (定长 (n-i0)//60+2 在 bar 非严格 1s 等距时会越界写内存, 已废弃该思路)
3) 外层按"块"提交到 ThreadPoolExecutor (非逐 cycle 提交), 依赖 nogil 真并行,
   结果按下标写入预分配数组 => 与串行版逐位一致、可复现。
4) 已删除 tolist() (fast_lists) 路径, K 线一律以连续 float64/int64 数组传入。

内存说明 (Stage 1, v2)
----------------------
浮亏阶梯不再"每 Cycle 一个小 ndarray"(百万级 Cycle 时仅对象头就占 1~2GB),
而是压缩为 CSR 扁平结构:
    cycles_df["dd_off"]         : int64, 该 Cycle 在扁平数组中的起始下标
    cycles_df["n_dd_steps"]     : int32, 台阶点个数
    cycles_df.attrs["dd_times_flat"] : int32 数组, 单位 = 分钟 (= ms // 60000, 无损)
    cycles_df.attrs["dd_vals_flat"]  : float64 数组, 浮亏值
    cycles_df.attrs["dd_time_scale"] : 60000 (还原成 ms 的乘数)
TimelineReplayer 同时兼容新 CSR 格式与老的 dd_times/dd_vals/dd_steps 格式。
"""

import gc
import os
import pickle
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------
# numba 可选依赖: 缺失时自动退化为纯 Python (逻辑同源, 仅速度不同)
# ---------------------------------------------------------------------
try:
    from numba import njit as _njit

    _HAS_NUMBA = True
except Exception:  # pragma: no cover
    _HAS_NUMBA = False


    def _njit(*args, **kwargs):
        if len(args) == 1 and callable(args[0]) and not kwargs:
            return args[0]

        def _wrap(f):
            return f

        return _wrap

MS_MIN = 60000
MS_HOUR = 3600000.0

DEFAULT_FEE = 0.0005  # 单边综合成本(手续费+滑点+资金费率折算)
DEFAULT_ADD_STEP = 0.002  # 加仓间距(基于持仓均价)
DEFAULT_TP_STEP = 0.003  # 止盈间距(基于持仓均价)
DEFAULT_MULT = 2.0  # 加仓倍数(数量翻倍)


# =====================================================================
# 日志与内存观测工具 (轻量, 无强依赖)
# =====================================================================
_LOG_T0 = time.time()
_PROC = None


def _rss_mb():
    """当前进程物理内存(MB); 无 psutil 时返回 nan (不影响主流程)"""
    global _PROC
    if _PROC is None:
        try:
            import psutil  # 可选
            _PROC = psutil.Process(os.getpid())
        except Exception:
            _PROC = False
    if _PROC is False:
        return float("nan")
    try:
        return _PROC.memory_info().rss / 1048576.0
    except Exception:
        return float("nan")


def _log(msg, indent=0):
    r = _rss_mb()
    mem = ("RSS=%6.0fMB" % r) if r == r else "RSS=   n/a "
    print("[%s |%8.1fs| %s] %s%s"
          % (time.strftime("%H:%M:%S"), time.time() - _LOG_T0, mem, "  " * indent, msg),
          flush=True)


def _fmt_hms(sec):
    try:
        if not np.isfinite(sec):
            return "--:--:--"
    except Exception:
        return "--:--:--"
    sec = int(max(sec, 0))
    return "%02d:%02d:%02d" % (sec // 3600, (sec % 3600) // 60, sec % 60)


# ---------------------------------------------------------
# 维度一：统计偏离类（测度“均值引力”）
# ---------------------------------------------------------

def strategy_1_vwap_zscore(df):
    """
    1. 微观 VWAP 偏离度 (VWAP Z-Score)
    参数: 1分钟(60秒)计算VWAP, 5分钟(300秒)计算Z-Score
    信号: 偏离度超过2个标准差
    """
    vwap_window = 60
    z_window = 300

    cv_sum = (df['close'] * df['volume']).rolling(window=vwap_window, min_periods=1).sum()
    v_sum = df['volume'].rolling(window=vwap_window, min_periods=1).sum()
    vwap = cv_sum / (v_sum + 1e-8)

    deviation = (df['close'] - vwap) / (vwap + 1e-8)

    mean_dev = deviation.rolling(window=z_window, min_periods=1).mean()
    std_dev = deviation.rolling(window=z_window, min_periods=1).std()
    z_score = (deviation - mean_dev) / (std_dev + 1e-8)

    # Z-Score绝对值大于2.0触发信号
    df['signal'] = np.abs(z_score) > 2.0
    return df


def strategy_2_quantile_deviation(df):
    """
    2. 稳健分位数偏离 (Quantile Deviation)
    参数: 过去5分钟(300秒)的价格分布
    信号: 当前价格突破了99%或1%的分位数
    """
    window = 300

    q_high = df['close'].rolling(window=window, min_periods=1).quantile(0.99)
    q_low = df['close'].rolling(window=window, min_periods=1).quantile(0.01)

    # 向上突破极高值 或 向下突破极低值
    df['signal'] = (df['close'] > q_high) | (df['close'] < q_low)
    return df


def strategy_3_periodic_open_deviation(df):
    """
    3. 自然周期开盘价偏离 (Periodic Open Deviation)
    参数: 1分钟周期的开盘价，偏离阈值设为0.15% (加密市场1s内千分之1.5算剧烈偏离)
    """
    threshold = 0.0015

    # 将时间戳转换为datetime，提取每分钟的起始
    time_series = pd.to_datetime(df['open_time'], unit='ms')
    minute_blocks = time_series.dt.floor('1min')

    # 获取当前所在1分钟周期的开盘价 (利用groupby的first)
    period_open = df.groupby(minute_blocks)['open'].transform('first')

    deviation = np.abs(df['close'] - period_open) / (period_open + 1e-8)

    df['signal'] = deviation > threshold
    return df


# ---------------------------------------------------------
# 维度二：量价微观结构类（透视“订单簿博弈”）
# ---------------------------------------------------------

def strategy_4_volume_price_absorption(df):
    """
    4. 量价吸收/滞涨滞跌 (Volume-Price Absorption)
    参数: 过去1分钟(60秒)成交量，Z-Score > 2.5 (天量)，实体占比 < 0.2 (价格没动)
    """
    window = 60

    vol_mean = df['volume'].rolling(window=window, min_periods=1).mean()
    vol_std = df['volume'].rolling(window=window, min_periods=1).std()
    vol_zscore = (df['volume'] - vol_mean) / (vol_std + 1e-8)

    body = np.abs(df['close'] - df['open'])
    spread = df['high'] - df['low']
    body_ratio = body / (spread + 1e-8)

    df['signal'] = (vol_zscore > 2.5) & (body_ratio < 0.2)
    return df


def strategy_5_liquidity_vacuum(df):
    """
    5. 缩量真空滑行 (Liquidity Vacuum)
    参数: 过去15秒净位移 / 过去15秒总成交量，当这个“虚假滑行”效率处于过去5分钟的95%分位时触发
    """
    window = 15
    eval_window = 300

    net_price_move = np.abs(df['close'] - df['close'].shift(window))
    total_volume = df['volume'].rolling(window=window, min_periods=1).sum()

    vacuum_ratio = net_price_move / (total_volume + 1e-8)

    # 判断当前的真空比例是否处于极端水平
    q95 = vacuum_ratio.rolling(window=eval_window, min_periods=1).quantile(0.95)

    df['signal'] = vacuum_ratio > q95
    return df


def strategy_6_volume_climax(df):
    """
    6. 巨量高潮极值 (Volume Climax)
    参数: 过去5分钟(300秒)统计，当前1s成交量突破99.5%分位，且单根振幅也突破99.5%分位
    """
    window = 300

    vol_q = df['volume'].rolling(window=window, min_periods=1).quantile(0.995)

    spread = df['high'] - df['low']
    spread_q = spread.rolling(window=window, min_periods=1).quantile(0.995)

    df['signal'] = (df['volume'] > vol_q) & (spread > spread_q)
    return df


def strategy_7_proxy_cvd_divergence(df):
    """
    7. 近似量价背离 (Proxy CVD Divergence)
    参数: 过去1分钟(60秒)。价格创60秒新高，但CVD未创新高（或相反）
    """
    window = 60

    # 伪CVD：阳线成交量为正，阴线为负
    sign = np.where(df['close'] >= df['open'], 1, -1)
    pseudo_vol = df['volume'] * sign
    cvd_window = pseudo_vol.rolling(window=window, min_periods=1).sum()

    # 价格创N秒新高/新低
    highest_close = df['close'].rolling(window=window, min_periods=1).max()
    lowest_close = df['close'].rolling(window=window, min_periods=1).min()

    # CVD创N秒新高/新低
    highest_cvd = cvd_window.rolling(window=window, min_periods=1).max()
    lowest_cvd = cvd_window.rolling(window=window, min_periods=1).min()

    # 顶背离：价格平齐或新高，但CVD没跟上极值
    bearish_div = (df['close'] == highest_close) & (cvd_window < highest_cvd)
    # 底背离：价格平齐或新低，但CVD没跟上极低
    bullish_div = (df['close'] == lowest_close) & (cvd_window > lowest_cvd)

    df['signal'] = bearish_div | bullish_div
    return df


# ---------------------------------------------------------
# 维度三：微观形态与猎杀类（捕捉“假突破”）
# ---------------------------------------------------------

def strategy_8_rolling_stop_hunt(df):
    """
    8. 滚动极值假突破 (Rolling Stop Hunt)
    参数: 过去1分钟(60秒)高低点。瞬间刺穿前高/前低，但收盘收回。
    """
    window = 60

    # 使用 shift(1) 避免把当前这根K线算作前高
    prev_high = df['high'].shift(1).rolling(window=window, min_periods=1).max()
    prev_low = df['low'].shift(1).rolling(window=window, min_periods=1).min()

    # 向上假突破：最高价刺穿前高，但收盘价低于前高
    hunt_up = (df['high'] > prev_high) & (df['close'] < prev_high)
    # 向下假突破：最低价刺穿前低，但收盘价高于前低
    hunt_down = (df['low'] < prev_low) & (df['close'] > prev_low)

    df['signal'] = hunt_up | hunt_down
    return df


def strategy_9_wick_rejection_ratio(df):
    """
    9. 极端影线拒绝度 (Wick Rejection Ratio)
    参数: 影线占比 > 0.75。需过滤掉价格几乎没动的死水行情（振幅需大于过去60秒均值）
    """
    window = 60
    threshold = 0.75

    spread = df['high'] - df['low']
    spread_mean = spread.rolling(window=window, min_periods=1).mean()

    upper_wick = df['high'] - np.maximum(df['open'], df['close'])
    lower_wick = np.minimum(df['open'], df['close']) - df['low']

    upper_ratio = upper_wick / (spread + 1e-8)
    lower_ratio = lower_wick / (spread + 1e-8)

    # 有效拒接：单侧影线占比极高，且该K线并非一潭死水
    df['signal'] = ((upper_ratio > threshold) | (lower_ratio > threshold)) & (spread > spread_mean)
    return df


def strategy_10_close_position_bias(df):
    """
    10. 极端收盘位置 (Close Position Bias)
    参数: 连续2根K线收盘位置极值(>0.8 或 <0.2)，随后当前这根发生逆转
    """
    spread = df['high'] - df['low']
    pos = (df['close'] - df['low']) / (spread + 1e-8)

    # 前两秒收盘都在绝对高位，这秒突然收在绝对低位 (微观多头破位)
    long_trap = (pos.shift(2) > 0.8) & (pos.shift(1) > 0.8) & (pos < 0.2)

    # 前两秒收盘都在绝对低位，这秒突然收在绝对高位 (微观空头破位)
    short_trap = (pos.shift(2) < 0.2) & (pos.shift(1) < 0.2) & (pos > 0.8)

    df['signal'] = long_trap | short_trap
    return df


def strategy_11_tick_gap_reversion(df):
    """
    11. 秒级跳空断层 (Tick Gap Reversion)
    参数: 1s跳空幅度大于万分之1 (对于1s级别，0.0001 的价格断裂已属流动性瞬间衰竭)
    """
    threshold = 0.0001

    gap = np.abs(df['open'] - df['close'].shift(1)) / (df['close'].shift(1) + 1e-8)

    df['signal'] = gap > threshold
    return df


# ---------------------------------------------------------
# 维度四：路径拓扑与动力学类（剖析“运动轨迹”）
# ---------------------------------------------------------

def strategy_12_kaufman_efficiency_ratio(df):
    """
    12. 路径效率比 (Kaufman Efficiency Ratio - ER)
    参数: N=30秒。ER < 0.15 代表极度无序震荡（马丁吃肉开仓区）
    """
    window = 30
    threshold = 0.15

    direction = np.abs(df['close'] - df['close'].shift(window))
    volatility = np.abs(df['close'] - df['close'].shift(1)).rolling(window=window, min_periods=1).sum()

    er = direction / (volatility + 1e-8)

    # 这里我们只取无序震荡作为信号（ER低），因为这是马丁策略的天然温床
    df['signal'] = er < threshold
    return df


def strategy_13_run_length_streaks(df):
    """
    13. 连续单边极值 (Run-Length / Streaks)
    参数: 连续 7 秒同向（收阳或收阴），作为概率极端的反转搏杀点
    """
    streak_threshold = 7

    # 连续收阳
    is_up = (df['close'] > df['open']).astype(int)
    up_streak = is_up.rolling(window=streak_threshold).sum()

    # 连续收阴
    is_down = (df['close'] < df['open']).astype(int)
    down_streak = is_down.rolling(window=streak_threshold).sum()

    df['signal'] = (up_streak == streak_threshold) | (down_streak == streak_threshold)
    return df


def strategy_14_volatility_squeeze_expansion(df):
    """
    14. 波幅挤压/扩张率 (Volatility Squeeze/Expansion)
    参数: 当前振幅达到过去2分钟(120秒)中位数的 5 倍（暴走） 或 低于 0.1倍（死水）
    """
    window = 120

    spread = df['high'] - df['low']
    median_spread = spread.rolling(window=window, min_periods=1).median()

    ratio = spread / (median_spread + 1e-8)

    df['signal'] = (ratio > 5.0) | (ratio < 0.1)
    return df


def strategy_15_micro_autocorrelation(df):
    """
    15. 微观收益率自相关性 (Micro Autocorrelation)
    参数: 过去60秒 1s收益率的一阶自相关系数。相关系数 < -0.3 意味着强烈的来回震荡(负相关)
    """
    window = 60

    # 1s收益率
    ret = df['close'].pct_change()

    # 滚动计算滞后1阶的自相关系数 (使用 pandas 内置的高效 corr 方法)
    autocorr = ret.rolling(window=window).corr(ret.shift(1))

    # 当自相关性呈现显著负值时，代表此时行情每走一步就想回头，适合马丁
    df['signal'] = autocorr < -0.3
    return df


# ---------------------------------------------------------
# 维度五：时间对齐类（提取“日历特征”）
# ---------------------------------------------------------

def strategy_16_clock_aligned_anomaly(df):
    """
    16. 时钟节点微观冲击 (Clock-Aligned Anomaly)
    参数: 取整分(00秒)和换线前(59秒)，这些节点通常是高频做市商撤单或TWAP算法集中下单的时刻。
    """
    # 提取毫秒时间戳对应的秒数 (0-59)
    # open_time // 1000 得到秒级时间戳，再 % 60 得到当前所在秒
    seconds = (df['open_time'] // 1000) % 60

    # 在 59秒（抢跑）或 00秒（整点爆发）时标记信号
    df['signal'] = (seconds == 59) | (seconds == 0)
    return df


# =====================================================================
# 0. 纯数学马丁阶梯表 (与行情无关, 用于选 Margin / 反推死亡层数)
# =====================================================================
def _ladder_cost(direction_sign, fee_rate, add_step, multiplier, k_max):
    """返回 cost_k (k=0..k_max) 与 dd_boundary_k = cost_k*(add_step+fee)"""
    cost = 1.0
    vol = 1.0
    last_q = 1.0
    add_mul = 1.0 - direction_sign * add_step
    costs = np.empty(k_max + 1, dtype=np.float64)
    for k in range(k_max + 1):
        costs[k] = cost
        p_avg = cost / vol
        p_add = p_avg * add_mul
        q = last_q * multiplier
        cost += p_add * q
        vol += q
        last_q = q
    return costs, costs * (add_step + fee_rate)


def build_ladder(max_layer=20, direction="Long",
                 fee_rate=DEFAULT_FEE, add_step=DEFAULT_ADD_STEP,
                 tp_step=DEFAULT_TP_STEP, multiplier=DEFAULT_MULT):
    """
    马丁网格理论台阶表。
    margin_to_reach_layer : 触发第 (layer+1) 层加仓那一瞬间的资金缺口。
        => Margin 必须 > 该值, 才可能活着打到第 (layer+1) 层。
    """
    s = 1.0 if str(direction).lower().startswith("l") else -1.0
    add_mul = 1.0 - s * add_step
    tp_mul = 1.0 + s * tp_step
    rows = []
    cost = 1.0
    vol = 1.0
    last_q = 1.0
    for k in range(max_layer + 1):
        p_avg = cost / vol
        fees = cost * fee_rate
        rows.append((k, vol, cost, p_avg, p_avg * add_mul, p_avg * tp_mul,
                     fees, cost * add_step + fees))
        q = last_q * multiplier
        cost += p_avg * add_mul * q
        vol += q
        last_q = q
    return pd.DataFrame(rows, columns=["layer", "total_volume", "cost_basis",
                                       "avg_price", "next_add_price", "tp_price",
                                       "accum_fees", "margin_to_reach_layer"])


# =====================================================================
# 1. Stage 1 : 单 Cycle 精确模拟 (归一化 / 纯数学推演)
# =====================================================================
@_njit(cache=True, nogil=True)
def _times_strictly_increasing(t):
    """单遍短路扫描, 零临时分配 (等价于 not np.any(np.diff(t) <= 0))"""
    for i in range(1, t.shape[0]):
        if t[i] <= t[i - 1]:
            return False
    return True


@_njit(cache=True, nogil=True)
def _all_finite3(a, b, c):
    """单遍短路扫描, 零临时分配 (等价于 np.all(isfinite(a)&isfinite(b)&isfinite(c)))"""
    for i in range(a.shape[0]):
        if not (np.isfinite(a[i]) and np.isfinite(b[i]) and np.isfinite(c[i])):
            return False
    return True


@_njit(cache=True, nogil=True)
def _simulate_cycle(i0, n, adv, fav, closes, times, s,
                    fee, add_mul, tp_mul, mult, mtm_fee,
                    dd_abort, has_abort, max_layer_hard):
    """
    s    : +1.0 = Long, -1.0 = Short
    adv  : 逆向价序列 (Long -> low , Short -> high)
    fav  : 顺向价序列 (Long -> high, Short -> low)
    has_abort/dd_abort : 熔断开关 + 阈值 (拆成两参数是为了避免 Optional 类型污染 JIT)
    返回 : (end_i, status, layer, net_pnl, total_fees, dd_t, dd_v, max_dd)
           status: 1=止盈闭环, 0=数据耗尽MTM, -1=熔断截断
    剪枝依据(等价变换):
        p_add 恒 < 历史逆向极值 => 只有逆向价创新极值的 bar 才可能加仓/刷新 max_dd
    """
    p0 = closes[i0]
    inv = 1.0 / p0

    vol = 1.0  # Total_Volume
    last_q = 1.0  # 上一笔订单数量
    cost = 1.0  # Total_Cost_Basis (按成交价累加的名义价值)
    fees = fee  # Accumulated_Fees (首单名义价值 = 1.0)
    layer = 0

    p_add = add_mul  # = 1.0 * add_mul
    p_tp = tp_mul

    worst = 1.0  # Cycle 内最差(逆向)归一化价
    max_dd = fees  # 开仓瞬间的资金缺口 = 已付手续费
    t0 = times[i0]

    # ---- 浮亏阶梯: 倍增动态数组 (不做定长预分配, 兼容非等距 bar 且不吃内存) ----
    cap = 8
    dd_t = np.empty(cap, dtype=np.int64)
    dd_v = np.empty(cap, dtype=np.float64)
    dd_t[0] = t0 - t0 % MS_MIN
    dd_v[0] = max_dd
    idx = 1

    for i in range(i0 + 1, n):
        a = adv[i] * inv
        if a * s < worst * s:
            worst = a
            # ---- 1) 加仓优先(同 K 线可连破多层) ----
            if a * s <= p_add * s:
                while a * s <= p_add * s:
                    q = last_q * mult
                    notional = p_add * q
                    cost += notional
                    fees += notional * fee
                    vol += q
                    last_q = q
                    layer += 1
                    p_avg = cost / vol
                    p_add = p_avg * add_mul
                    p_tp = p_avg * tp_mul
                    if layer >= max_layer_hard:
                        break
            # ---- 2) 浮亏阶梯 (悲观: 极值发生在止盈之前) ----
            dd = s * (cost - vol * a) + fees
            if dd > max_dd:
                max_dd = dd
                mm = times[i]
                mm -= mm % MS_MIN
                if mm == dd_t[idx - 1]:
                    dd_v[idx - 1] = dd  # 同分钟只留最大值
                else:
                    if idx == cap:
                        cap = cap * 2
                        nt = np.empty(cap, dtype=np.int64)
                        nv = np.empty(cap, dtype=np.float64)
                        nt[:idx] = dd_t[:idx]
                        nv[:idx] = dd_v[:idx]
                        dd_t = nt
                        dd_v = nv
                    dd_t[idx] = mm
                    dd_v[idx] = dd
                    idx += 1
            # ---- 3) 熔断(防御性, 仅当 dd_abort > 所有待测 Margin 时无影响) ----
            if layer >= max_layer_hard or (has_abort and max_dd >= dd_abort):
                pe = closes[i] * inv
                cf = vol * pe * mtm_fee
                return (i, -1, layer, s * (vol * pe - cost) - fees - cf,
                        fees + cf, dd_t[:idx].copy(), dd_v[:idx].copy(), max_dd)
        # ---- 4) 止盈 ----
        if fav[i] * inv * s >= p_tp * s:
            xn = vol * p_tp
            cf = xn * fee
            return (i, 1, layer, s * (xn - cost) - fees - cf,
                    fees + cf, dd_t[:idx].copy(), dd_v[:idx].copy(), max_dd)

    # ---- 5) 数据耗尽: 强制盯市结算 ----
    i = n - 1
    pe = closes[i] * inv
    cf = vol * pe * mtm_fee
    return (i, 0, layer, s * (vol * pe - cost) - fees - cf,
            fees + cf, dd_t[:idx].copy(), dd_v[:idx].copy(), max_dd)


def _warmup_jit():
    """主线程内先把签名编译好, 避免多线程首次调用抢 numba 编译锁"""
    if not _HAS_NUMBA:
        return
    f1 = np.ones(1, dtype=np.float64)
    i1 = np.zeros(1, dtype=np.int64)
    _times_strictly_increasing(i1)
    _all_finite3(f1, f1, f1)
    _simulate_cycle(0, 1, f1, f1, f1, i1, 1.0,
                    0.0, 1.0, 1.0, 2.0, 0.0, 0.0, False, 1)


def run_stage1(df,
               long_col="long_signal",
               short_col="short_signal",
               fee_rate=DEFAULT_FEE,
               add_step=DEFAULT_ADD_STEP,
               tp_step=DEFAULT_TP_STEP,
               multiplier=DEFAULT_MULT,
               mtm_charge_close_fee=True,
               dd_abort=None,
               max_layer_hard=512,
               dd_format="array",
               fast_lists=None,
               progress=0,
               n_jobs=None,
               log_interval_sec=15.0,
               verbose=True):
    """
    第一阶段: 无限保证金平行宇宙生成。

    dd_format:
        "array" (默认, 省内存/最快): 浮亏阶梯以 CSR 扁平结构存放
                cycles_df["dd_off"] + cycles_df["n_dd_steps"]
                cycles_df.attrs["dd_times_flat"] (int32, 单位=分钟)
                cycles_df.attrs["dd_vals_flat"]  (float64)
                cycles_df.attrs["dd_time_scale"] = 60000
            (v1 的 dd_times/dd_vals 对象列已废弃: 百万级 Cycle 时仅 ndarray
             对象头就要吃掉 1~2GB, 且 pickle 极慢。TimelineReplayer 仍兼容旧文件。)
        "list"  : 额外生成方案原文要求的 dd_steps 列 = [(ms, dd), ...] (仅供人读, 极吃内存)
        "both"  : 两者都有
    dd_abort:
        浮亏熔断阈值。None = 严格按方案(不熔断)。若设置, 必须 > 你要测试的最大 Margin,
        否则 Stage 2 会报错以防污染结论。
        注意: 只要 dd_abort > 所有待测 Margin, Stage 2 的 trades 时间线与不熔断位级等价;
              但 cycles_df 中被截断 Cycle 的 net_pnl / max_dd / is_closed 会变,
              从而影响 Stage 3 "仅闭环 Cycle" 的横向统计表。
    fast_lists:
        【已废弃, 保留仅为向后兼容, 传什么都被忽略】
    n_jobs:
        Stage 1 并发线程数 (None = os.cpu_count())。底层 K 线只读共享, 内存不随线程增长。
        无 numba 时强制退化为 1 (GIL 无法释放, 多线程只会更慢)。
    log_interval_sec:
        后台心跳日志间隔(秒)。>0 时会启动守护线程周期输出 进度/速率/ETA/RSS,
        即使卡在单个"长尾 Cycle"上也能看到程序仍在推进。0 或 None 表示关闭。
    verbose:
        是否输出 Stage 1 各阶段的关键日志。
    """
    t_stage = time.time()

    for c in ("open_time", "high", "low", "close"):
        if c not in df.columns:
            raise ValueError("缺少必需列: %s" % c)

    if verbose:
        _log("Stage1 启动 | numba=%s | 输入 %d 行 x %d 列"
             % (_HAS_NUMBA, len(df), df.shape[1]), 1)

    t = time.time()
    _warmup_jit()
    if verbose:
        _log("JIT 预热完成 (%.2fs)" % (time.time() - t), 2)

    # ---------------- 时间轴 ----------------
    t = time.time()
    times_np = np.ascontiguousarray(df["open_time"].to_numpy(dtype=np.int64))
    n = int(times_np.shape[0])
    if n == 0:
        raise ValueError("空数据")
    if n > 1:
        if _HAS_NUMBA:
            ok_inc = bool(_times_strictly_increasing(times_np))
        else:
            ok_inc = not bool(np.any(np.diff(times_np) <= 0))
        if not ok_inc:
            raise ValueError("open_time 必须严格递增(请先排序去重)")
    if verbose:
        _log("时间轴校验通过: %d 根 K 线 | %s ~ %s (%.2f 天) | 耗时 %.2fs"
             % (n, pd.to_datetime(int(times_np[0]), unit="ms"),
                pd.to_datetime(int(times_np[-1]), unit="ms"),
                (times_np[-1] - times_np[0]) / 86400000.0, time.time() - t), 2)

    # ---------------- OHLC ----------------
    t = time.time()
    highs_np = np.ascontiguousarray(df["high"].to_numpy(dtype=np.float64))
    lows_np = np.ascontiguousarray(df["low"].to_numpy(dtype=np.float64))
    closes_np = np.ascontiguousarray(df["close"].to_numpy(dtype=np.float64))
    if _HAS_NUMBA:
        ok_fin = bool(_all_finite3(highs_np, lows_np, closes_np))
    else:
        ok_fin = bool(np.all(np.isfinite(highs_np) & np.isfinite(lows_np) & np.isfinite(closes_np)))
    if not ok_fin:
        raise ValueError("high/low/close 存在 NaN/Inf")
    if verbose:
        _log("OHLC 数组就绪并校验完毕 (%.0f MB, 耗时 %.2fs)"
             % ((highs_np.nbytes + lows_np.nbytes + closes_np.nbytes + times_np.nbytes) / 1048576.0,
                time.time() - t), 2)

    # ---------------- 信号采集 ----------------
    t = time.time()
    if long_col in df.columns:
        li = np.flatnonzero(df[long_col].to_numpy() != 0)
    else:
        li = np.empty(0, dtype=np.int64)
    if short_col in df.columns:
        si = np.flatnonzero(df[short_col].to_numpy() != 0)
    else:
        si = np.empty(0, dtype=np.int64)
    n_li = int(li.shape[0])
    n_si = int(si.shape[0])
    sig_idx = np.concatenate([li.astype(np.int64), si.astype(np.int64)])
    sig_dir = np.concatenate([np.ones(n_li), -np.ones(n_si)])
    del li, si
    order = np.argsort(sig_idx, kind="stable")  # 同 bar: Long 先于 Short
    sig_idx = sig_idx[order]
    sig_dir = sig_dir[order]
    del order
    m = int(sig_idx.shape[0])
    if verbose:
        _log("信号采集完成: 多 %d + 空 %d = %d 个 Cycle (信号率 %.4f%%) | 耗时 %.2fs"
             % (n_li, n_si, m, 100.0 * m / max(n, 1), time.time() - t), 2)
        if m == 0:
            _log("警告: 本次没有任何信号, 将输出空 cycles 表", 2)

    add_mul_l = 1.0 - add_step
    tp_mul_l = 1.0 + tp_step
    add_mul_s = 1.0 + add_step
    tp_mul_s = 1.0 - tp_step
    mtm_fee = fee_rate if mtm_charge_close_fee else 0.0
    has_abort = dd_abort is not None
    dd_abort_f = float(dd_abort) if has_abort else 0.0
    max_layer_hard_i = int(max_layer_hard)

    # ---------------- 结果缓冲 (紧凑 dtype, 不再用 object 存方向/阶梯) ----------------
    out_is_long = np.empty(m, dtype=bool)
    out_bar = np.empty(m, dtype=np.int64)
    out_s = np.empty(m, dtype=np.int64)
    out_e = np.empty(m, dtype=np.int64)
    out_status = np.empty(m, dtype=np.int8)
    out_layer = np.empty(m, dtype=np.int32)
    out_net = np.empty(m, dtype=np.float64)
    out_fee = np.empty(m, dtype=np.float64)
    out_mdd = np.empty(m, dtype=np.float64)
    out_nst = np.empty(m, dtype=np.int32)
    if verbose:
        _log("结果缓冲已分配 (%.1f MB)"
             % ((out_is_long.nbytes + out_bar.nbytes + out_s.nbytes + out_e.nbytes
                 + out_status.nbytes + out_layer.nbytes + out_net.nbytes + out_fee.nbytes
                 + out_mdd.nbytes + out_nst.nbytes) / 1048576.0), 2)

    _lock = threading.Lock()
    _done = [0]
    _blocks = [0]
    _lastp = [0]

    def _run_range(k0, k1):
        """
        处理 [k0, k1) 这一块 cycle; 标量结果按绝对下标 k 写入 -> 与串行版逐位一致。
        浮亏阶梯在块内先攒成"块级大数组", 让 numba 返回的百万个小数组尽早被回收,
        块内合并后时间戳由 int64 ms 无损压成 int32 分钟 (阶梯点本就整分钟对齐)。
        """
        loc_t = []
        loc_v = []
        pend = 0
        for k in range(k0, k1):
            i0 = int(sig_idx[k])
            s = float(sig_dir[k])
            if s > 0.0:
                res = _simulate_cycle(i0, n, lows_np, highs_np, closes_np, times_np, 1.0,
                                      fee_rate, add_mul_l, tp_mul_l, multiplier,
                                      mtm_fee, dd_abort_f, has_abort, max_layer_hard_i)
            else:
                res = _simulate_cycle(i0, n, highs_np, lows_np, closes_np, times_np, -1.0,
                                      fee_rate, add_mul_s, tp_mul_s, multiplier,
                                      mtm_fee, dd_abort_f, has_abort, max_layer_hard_i)
            end_i, status, layer, net, tfee, dd_t, dd_v, mdd = res
            out_is_long[k] = s > 0.0
            out_bar[k] = i0
            out_s[k] = times_np[i0]
            out_e[k] = times_np[end_i]
            out_status[k] = status
            out_layer[k] = layer
            out_net[k] = net
            out_fee[k] = tfee
            out_mdd[k] = mdd
            out_nst[k] = dd_t.shape[0]
            loc_t.append(dd_t)
            loc_v.append(dd_v)
            pend += 1
            if pend >= 32:  # 高频刷新计数, 保证心跳日志有粒度
                with _lock:
                    _done[0] += pend
                pend = 0
        if pend:
            with _lock:
                _done[0] += pend
        with _lock:
            _blocks[0] += 1
            if progress and (_done[0] - _lastp[0]) >= progress:
                _lastp[0] = _done[0]
                print("[stage1] %d / %d cycles" % (_done[0], m), flush=True)
        if not loc_t:
            return np.empty(0, dtype=np.int32), np.empty(0, dtype=np.float64)
        ct = loc_t[0] if len(loc_t) == 1 else np.concatenate(loc_t)
        cv = loc_v[0] if len(loc_v) == 1 else np.concatenate(loc_v)
        del loc_t, loc_v
        return (ct // MS_MIN).astype(np.int32), cv

    parts = None
    if m:
        if n_jobs is None:
            nj = os.cpu_count() or 1
        else:
            nj = int(n_jobs)
        if nj < 1:
            nj = 1
        if not _HAS_NUMBA:
            nj = 1  # 纯 Python 下 GIL 未释放, 多线程无收益
        nj = min(nj, 32, m)
        # 块数远多于线程数 => 天然工作窃取, 化解"长尾 Cycle"负载不均
        chunk = (m + nj * 64 - 1) // (nj * 64)
        if chunk < 1:
            chunk = 1
        if chunk > 4096:
            chunk = 4096
        bounds = [(k0, min(k0 + chunk, m)) for k0 in range(0, m, chunk)]
        nb = len(bounds)
        if verbose:
            _log("并发配置: 线程 %d | 块大小 %d | 块数 %d | 心跳 %s"
                 % (nj, chunk, nb,
                    ("%.0fs" % log_interval_sec) if (log_interval_sec and log_interval_sec > 0) else "off"), 2)
            _log("开始模拟 %d 个平行宇宙 Cycle ..." % m, 2)

        _stop = threading.Event()
        _t_run0 = time.time()

        def _heartbeat():
            last_d = 0
            stall = 0
            while not _stop.wait(log_interval_sec):
                with _lock:
                    d = _done[0]
                    bd = _blocks[0]
                el = time.time() - _t_run0
                rate = (d / el) if el > 0 else 0.0
                eta = ((m - d) / rate) if rate > 0 else float("inf")
                if d == last_d:
                    stall += 1
                else:
                    stall = 0
                last_d = d
                _log("进度 %d/%d (%5.2f%%) | 块 %d/%d | %.0f cyc/s | 已用 %s | ETA %s%s"
                     % (d, m, 100.0 * d / m, bd, nb, rate, _fmt_hms(el), _fmt_hms(eta),
                        ("   <单个长尾 Cycle 正在长距离扫描, 连续 %d 次无新增>" % stall) if stall else ""), 3)

        hb = None
        if verbose and log_interval_sec and log_interval_sec > 0:
            hb = threading.Thread(target=_heartbeat, name="stage1-hb", daemon=True)
            hb.start()
        try:
            if nj <= 1:
                parts = [_run_range(b[0], b[1]) for b in bounds]
            else:
                with ThreadPoolExecutor(max_workers=nj) as ex:
                    parts = list(ex.map(lambda b: _run_range(b[0], b[1]), bounds))
        finally:
            _stop.set()
            if hb is not None:
                hb.join(timeout=0.2)
        if verbose:
            el = time.time() - _t_run0
            _log("模拟循环结束: %d cycles | 耗时 %s | 均速 %.0f cyc/s"
                 % (m, _fmt_hms(el), (m / el) if el > 0 else 0.0), 2)

    del sig_idx, sig_dir

    # ---------------- 浮亏阶梯: 合并为 CSR 扁平数组 ----------------
    t = time.time()
    if m:
        cs = np.cumsum(out_nst.astype(np.int64))
        dd_off = np.empty(m, dtype=np.int64)
        dd_off[0] = 0
        if m > 1:
            dd_off[1:] = cs[:-1]
        total_steps = int(cs[-1])
        del cs
    else:
        dd_off = np.empty(0, dtype=np.int64)
        total_steps = 0

    ddt_flat = np.empty(total_steps, dtype=np.int32)
    ddv_flat = np.empty(total_steps, dtype=np.float64)
    pos = 0
    if parts:
        for _i in range(len(parts)):
            pt, pv = parts[_i]
            L = pt.shape[0]
            if L:
                ddt_flat[pos:pos + L] = pt
                ddv_flat[pos:pos + L] = pv
                pos += L
            parts[_i] = None  # 尽早释放块级缓冲
    del parts
    gc.collect()
    if verbose:
        _log("浮亏阶梯汇总: %d 个台阶点 (均 %.2f/cycle, 最多 %d) | 扁平内存 %.1f MB | 耗时 %.2fs"
             % (total_steps, total_steps / max(m, 1), int(out_nst.max()) if m else 0,
                (ddt_flat.nbytes + ddv_flat.nbytes) / 1048576.0, time.time() - t), 2)

    # ---------------- 组装 cycles_df ----------------
    t = time.time()
    cycles = pd.DataFrame({
        "cycle_id": np.arange(m, dtype=np.int64),
        "direction": pd.Categorical.from_codes(
            np.where(out_is_long, 0, 1).astype(np.int8), categories=["Long", "Short"]),
        "start_time": pd.to_datetime(out_s, unit="ms"),
        "tp_time": pd.to_datetime(out_e, unit="ms"),
        "duration_hour": (out_e - out_s) / MS_HOUR,
        "is_closed": out_status == 1,
        "max_layer": out_layer,
        "net_pnl": out_net,
        "total_fees": out_fee,
        "max_dd": out_mdd,
        "n_dd_steps": out_nst,
        "status": pd.Categorical.from_codes(
            np.where(out_status == 1, 0, np.where(out_status == 0, 1, 2)).astype(np.int8),
            categories=["tp", "mtm", "truncated"]),
        "signal_bar": out_bar,
        "start_ms": out_s,
        "end_ms": out_e,
        "dd_off": dd_off,
    })
    n_tp = int(np.sum(out_status == 1))
    n_mtm = int(np.sum(out_status == 0))
    n_trunc = int(np.sum(out_status == -1))
    layer_mean = float(out_layer.mean()) if m else float("nan")
    layer_max = int(out_layer.max()) if m else 0
    mdd_max = float(out_mdd.max()) if m else float("nan")
    # 峰值削减: DataFrame 已持有副本, 立即释放中间缓冲
    del out_is_long, out_bar, out_s, out_e, out_status, out_layer, out_net, out_fee, out_mdd
    gc.collect()
    if verbose:
        _log("cycles_df 组装完成 (%.1f MB, 耗时 %.2fs)"
             % (cycles.memory_usage(deep=False).sum() / 1048576.0, time.time() - t), 2)

    cycles.attrs.update({
        "data_start_ms": int(times_np[0]),
        "data_end_ms": int(times_np[-1]),
        "n_bars": int(n),
        "n_cycles": int(m),
        "fee_rate": fee_rate,
        "add_step": add_step,
        "tp_step": tp_step,
        "multiplier": multiplier,
        "dd_abort": dd_abort,
        "max_layer_hard": max_layer_hard,
        "mtm_charge_close_fee": bool(mtm_charge_close_fee),
        "dd_times_flat": ddt_flat,
        "dd_vals_flat": ddv_flat,
        "dd_time_scale": MS_MIN,
        "dd_total_steps": int(total_steps),
    })

    if dd_format in ("list", "both"):
        t = time.time()
        if verbose:
            _log("正在生成 dd_steps 可读列表 (极吃内存, 仅调试用) ...", 2)
        _off = dd_off.tolist()
        _cnt = out_nst.tolist()
        cycles["dd_steps"] = pd.Series(
            [list(zip((ddt_flat[o:o + c].astype(np.int64) * MS_MIN).tolist(),
                      ddv_flat[o:o + c].tolist()))
             for o, c in zip(_off, _cnt)],
            index=cycles.index, dtype=object)
        del _off, _cnt
        if verbose:
            _log("dd_steps 生成完毕 (%.2fs)" % (time.time() - t), 2)
    if dd_format == "list":
        cycles.drop(columns=["dd_off"], inplace=True)
        cycles.attrs.pop("dd_times_flat", None)
        cycles.attrs.pop("dd_vals_flat", None)
        del ddt_flat, ddv_flat
        gc.collect()

    del out_nst, dd_off
    gc.collect()

    if verbose:
        el = time.time() - t_stage
        _log("Stage1 完成: %d cycles | 止盈 %d (%.2f%%) | 末端盯市 %d | 熔断 %d"
             % (m, n_tp, 100.0 * n_tp / max(m, 1), n_mtm, n_trunc), 1)
        _log("层数 均值 %.3f / 最大 %d | 最深浮亏 %.5f | 阶梯点 %d | 总耗时 %s"
             % (layer_mean, layer_max, mdd_max, total_steps, _fmt_hms(el)), 1)
    return cycles


def format_dd_steps(dd_times, dd_vals, digits=6):
    """把 (int64 ms, float) 阶梯表渲染成方案示例的可读形式 [('2026-01-01 10:01', 50.5), ...]"""
    ts = pd.to_datetime(np.asarray(dd_times), unit="ms").strftime("%Y-%m-%d %H:%M")
    return [(ts[i], round(float(dd_vals[i]), digits)) for i in range(len(dd_vals))]


# =====================================================================
# 2. Stage 2 : 查表重组与时间线回测
# =====================================================================
class TimelineReplayer:
    """基于 cycles_df 的极速时间线重组器。一次构建, 可反复 run(margin)。"""

    def __init__(self, cycles_df, data_start_ms=None, data_end_ms=None):
        d = cycles_df
        if not d["start_ms"].is_monotonic_increasing:
            d = d.sort_values(["start_ms", "cycle_id"], kind="mergesort")
        self.cycles = d
        self._cid = d["cycle_id"].to_numpy(np.int64)

        # ---- direction: 优先走 Categorical codes, 避免生成 m 长度的 object 数组 ----
        _dirs = d["direction"]
        if isinstance(_dirs.dtype, pd.CategoricalDtype):
            _cats = list(_dirs.cat.categories)
            _code = _cats.index("Long") if "Long" in _cats else -1
            self._is_long = np.ascontiguousarray(_dirs.cat.codes.to_numpy() == _code)
        else:
            self._is_long = np.ascontiguousarray(_dirs.to_numpy(object) == "Long")

        self._start = np.ascontiguousarray(d["start_ms"].to_numpy(np.int64))
        self._end = np.ascontiguousarray(d["end_ms"].to_numpy(np.int64))
        self._net = np.ascontiguousarray(d["net_pnl"].to_numpy(np.float64))
        self._fee = np.ascontiguousarray(d["total_fees"].to_numpy(np.float64))
        self._mdd = np.ascontiguousarray(d["max_dd"].to_numpy(np.float64))
        self._layer = np.ascontiguousarray(d["max_layer"].to_numpy(np.int64))
        self._closed = np.ascontiguousarray(d["is_closed"].to_numpy(bool))

        _st = d["status"]
        if isinstance(_st.dtype, pd.CategoricalDtype):
            _cats = list(_st.cat.categories)
            _code = _cats.index("truncated") if "truncated" in _cats else -1
            self._trunc = np.ascontiguousarray(_st.cat.codes.to_numpy() == _code)
        else:
            self._trunc = np.ascontiguousarray(_st.astype(str).to_numpy() == "truncated")

        # ---- 浮亏阶梯: 三种格式兼容 (新 CSR / 旧 dd_times+dd_vals / dd_steps) ----
        a = cycles_df.attrs
        _tf = a.get("dd_times_flat", None)
        _vf = a.get("dd_vals_flat", None)
        if _tf is not None and _vf is not None and "dd_off" in d.columns:
            self._csr = True
            self._ddt_flat = _tf
            self._ddv_flat = _vf
            self._dd_off = np.ascontiguousarray(d["dd_off"].to_numpy(np.int64))
            self._dd_cnt = np.ascontiguousarray(d["n_dd_steps"].to_numpy(np.int64))
            self._dd_scale = int(a.get("dd_time_scale", 1))
            self._ddt = None
            self._ddv = None
        else:
            self._csr = False
            self._dd_scale = 1
            self._ddt_flat = None
            self._ddv_flat = None
            if "dd_times" in d.columns:
                self._ddt = list(d["dd_times"].to_numpy())
                self._ddv = list(d["dd_vals"].to_numpy())
            else:  # dd_format == "list"
                self._ddt = [np.fromiter((t for t, _ in s), np.int64, len(s)) for s in d["dd_steps"]]
                self._ddv = [np.fromiter((v for _, v in s), np.float64, len(s)) for s in d["dd_steps"]]

        self.data_start_ms = int(data_start_ms if data_start_ms is not None
                                 else a.get("data_start_ms", self._start.min() if len(self._start) else 0))
        self.data_end_ms = int(data_end_ms if data_end_ms is not None
                               else a.get("data_end_ms", self._end.max() if len(self._end) else 0))
        self.params = dict(a)
        fee = a.get("fee_rate", DEFAULT_FEE)
        add = a.get("add_step", DEFAULT_ADD_STEP)
        mult = a.get("multiplier", DEFAULT_MULT)
        kmax = int(a.get("max_layer_hard", 512))
        _, self._bnd_l = _ladder_cost(1.0, fee, add, mult, min(kmax, 900))
        _, self._bnd_s = _ladder_cost(-1.0, fee, add, mult, min(kmax, 900))

    def _death_layer(self, dd, is_long):
        b = self._bnd_l if is_long else self._bnd_s
        return int(np.searchsorted(b, dd, side="right"))

    def _dd_arrays(self, j):
        """返回 (times_ms_like, vals); CSR 下 times 需乘 self._dd_scale 还原 ms"""
        if self._csr:
            o = int(self._dd_off[j])
            c = int(self._dd_cnt[j])
            return self._ddt_flat[o:o + c], self._ddv_flat[o:o + c]
        return self._ddt[j], self._ddv[j]

    def run(self, margin):
        """
        输入 Margin(单位 = 首单名义价值倍数), 输出真实连续交易记录 trades_df。
        逻辑严格按方案第三部分; 指针同时满足 start_time>=current_time 与索引严格递增(防自锁)。
        """
        margin = float(margin)
        if not margin > 0.0:
            raise ValueError("margin 必须 > 0")
        start, end, net = self._start, self._end, self._net
        mdd, closed, trunc = self._mdd, self._closed, self._trunc
        scale = self._dd_scale
        n = start.shape[0]

        cur = self.data_start_ms
        last = -1
        rows = []
        cum = 0.0
        while True:
            j = int(np.searchsorted(start, cur, side="left"))
            if j <= last:
                j = last + 1
            if j >= n:
                break
            last = j
            is_long = bool(self._is_long[j])
            dir_str = "Long" if is_long else "Short"
            if margin > mdd[j]:
                # ---------- 存活 ----------
                if trunc[j]:
                    raise ValueError(
                        "cycle_id=%d 被 dd_abort 熔断却在 margin=%g 下存活, "
                        "结果不可信。请调大 dd_abort 或取消熔断。" % (self._cid[j], margin))
                pnl = float(net[j])
                cum += pnl
                rows.append((self._cid[j], dir_str, int(start[j]), int(end[j]),
                             "tp" if closed[j] else "mtm", pnl, float(self._fee[j]),
                             int(self._layer[j]), float(mdd[j]), cum))
                if not closed[j]:
                    break  # 历史终点未平仓单 -> 回测强制结束
                cur = int(end[j])
            else:
                # ---------- 爆仓(查表, dd_vals 严格单调 => 二分) ----------
                tj, v = self._dd_arrays(j)
                k = int(np.searchsorted(v, margin, side="left"))
                death = int(tj[k]) * scale
                pnl = -margin
                cum += pnl
                rows.append((self._cid[j], dir_str, int(start[j]), death,
                             "blowup", pnl, np.nan,
                             self._death_layer(float(v[k]), is_long),
                             float(v[k]), cum))
                cur = death

        cols = ["cycle_id", "direction", "start_ms", "end_ms", "outcome",
                "net_pnl", "total_fees", "layer_at_end", "dd_at_end", "cum_pnl"]
        tr = pd.DataFrame(rows, columns=cols)
        if len(tr) == 0:
            tr["start_time"] = pd.to_datetime([], unit="ms")
            tr["end_time"] = pd.to_datetime([], unit="ms")
            tr["duration_hour"] = np.array([], dtype=np.float64)
        else:
            tr["start_time"] = pd.to_datetime(tr["start_ms"], unit="ms")
            tr["end_time"] = pd.to_datetime(tr["end_ms"], unit="ms")
            tr["duration_hour"] = (tr["end_ms"] - tr["start_ms"]) / MS_HOUR
        tr = tr[["cycle_id", "direction", "start_time", "end_time", "duration_hour",
                 "outcome", "net_pnl", "total_fees", "layer_at_end", "dd_at_end",
                 "cum_pnl", "start_ms", "end_ms"]]
        tr.attrs.update({"margin": margin,
                         "data_start_ms": self.data_start_ms,
                         "data_end_ms": self.data_end_ms})
        return tr


# =====================================================================
# 3. Stage 3 : 核心指标挖掘与量化评估
# =====================================================================
def evaluate_free_ride(trades_df, cycles_df, margin,
                       data_start_ms=None, data_end_ms=None):
    """输出方案第四部分的全部核心指标。"""
    margin = float(margin)
    a = cycles_df.attrs
    t0 = int(data_start_ms if data_start_ms is not None else
             trades_df.attrs.get("data_start_ms", a.get("data_start_ms", 0)))
    t1 = int(data_end_ms if data_end_ms is not None else
             trades_df.attrs.get("data_end_ms", a.get("data_end_ms", 0)))
    span_h = (t1 - t0) / MS_HOUR
    span_y = span_h / 8760.0

    rep = {"margin": margin, "span_hour": span_h, "span_day": span_h / 24.0,
           "span_year": span_y, "n_cycles_total": int(len(cycles_df))}

    s_ms = trades_df["start_ms"].to_numpy(np.int64)
    e_ms = trades_df["end_ms"].to_numpy(np.int64)
    pnl = trades_df["net_pnl"].to_numpy(np.float64)
    oc = trades_df["outcome"].to_numpy(object)
    nt = pnl.shape[0]

    rep["n_trades"] = nt
    rep["n_tp"] = int(np.sum(oc == "tp"))
    rep["n_blowup"] = int(np.sum(oc == "blowup"))
    rep["n_mtm"] = int(np.sum(oc == "mtm"))
    rep["signal_utilization"] = (nt / len(cycles_df)) if len(cycles_df) else np.nan
    rep["win_rate"] = (rep["n_tp"] / nt) if nt else np.nan
    rep["total_net_pnl"] = float(pnl.sum()) if nt else 0.0
    rep["total_net_pnl_in_margin"] = rep["total_net_pnl"] / margin
    rep["margins_per_year"] = (rep["total_net_pnl_in_margin"] / span_y) if span_y > 0 else np.nan
    rep["avg_trade_pnl"] = float(pnl.mean()) if nt else np.nan
    win = pnl[oc == "tp"]
    rep["avg_win_pnl"] = float(win.mean()) if win.size else np.nan
    rep["avg_holding_hour_traded"] = (float(trades_df["duration_hour"].mean())
                                      if nt else np.nan)

    # ---- 生命周期切分: 起点/每次爆仓后重置 ----
    lives = []
    life_start = -1
    cum = 0.0
    ttd = -1
    for i in range(nt):
        if life_start < 0:
            life_start = int(s_ms[i])
        cum += pnl[i]
        if ttd < 0 and cum >= margin:
            ttd = int(e_ms[i])
        if oc[i] == "blowup":
            lives.append((life_start, int(e_ms[i]), True, ttd))
            life_start, cum, ttd = -1, 0.0, -1
    if life_start >= 0:
        lives.append((life_start, int(e_ms[-1]) if nt else t1, False, ttd))

    ttd_h = [(L[3] - L[0]) / MS_HOUR for L in lives if L[3] >= 0]
    deaths = [L[1] for L in lives if L[2]]
    done = [L for L in lives if L[2]]

    rep["n_lives"] = len(lives)
    rep["n_lives_completed"] = len(done)
    rep["n_lives_doubled"] = int(sum(1 for L in lives if L[3] >= 0))
    rep["time_to_double_hour"] = float(np.mean(ttd_h)) if ttd_h else np.nan
    rep["time_to_double_median_hour"] = float(np.median(ttd_h)) if ttd_h else np.nan
    rep["n_doubles"] = len(ttd_h)
    rep["free_ride_win_rate"] = (sum(1 for L in done if L[3] >= 0) / len(done)) if done else np.nan

    if deaths:
        samples = np.diff(np.array([t0] + deaths, dtype=np.float64)) / MS_HOUR
        rep["expected_lifespan_hour"] = float(samples.mean())
        rep["blowup_interval_hour"] = (float(np.mean(np.diff(np.array(deaths,
                                                                      dtype=np.float64)) / MS_HOUR)) if len(
            deaths) > 1 else np.nan)
        rep["mean_life_hour"] = float(np.mean([(L[1] - L[0]) / MS_HOUR for L in done]))
        rep["blowups_per_year"] = (len(deaths) / span_y) if span_y > 0 else np.nan
    else:
        rep["expected_lifespan_hour"] = np.inf  # 样本内未爆仓(右删失)
        rep["blowup_interval_hour"] = np.nan
        rep["mean_life_hour"] = np.nan
        rep["blowups_per_year"] = 0.0

    if ttd_h and rep["expected_lifespan_hour"] == np.inf:
        rep["doubles_per_blowup"] = np.inf
    elif ttd_h and rep["time_to_double_hour"] > 0:
        rep["doubles_per_blowup"] = rep["expected_lifespan_hour"] / rep["time_to_double_hour"]
    else:
        rep["doubles_per_blowup"] = np.nan

    # ---- cycles_df 横向统计(仅闭环) ----
    cl = cycles_df[cycles_df["is_closed"].to_numpy(bool)]
    rep["n_closed_cycles"] = int(len(cl))
    if len(cl):
        g = cl.groupby("max_layer", sort=True)
        tab = pd.DataFrame({
            "count": g.size(),
            "pct": g.size() / len(cl),
            "hold_mean_h": g["duration_hour"].mean(),
            "hold_median_h": g["duration_hour"].median(),
            "hold_p95_h": g["duration_hour"].quantile(0.95),
            "dd_mean": g["max_dd"].mean(),
            "dd_max": g["max_dd"].max(),
            "net_pnl_mean": g["net_pnl"].mean(),
        })
        tab["cum_pct"] = tab["pct"].cumsum()
        rep["layer_table"] = tab
        rep["low_layer_ratio"] = float(tab.loc[tab.index <= 1, "pct"].sum())
        rep["holding_overall"] = {
            "mean_h": float(cl["duration_hour"].mean()),
            "median_h": float(cl["duration_hour"].median()),
            "p95_h": float(cl["duration_hour"].quantile(0.95)),
            "max_h": float(cl["duration_hour"].max()),
        }
        gross = (cl["net_pnl"] + cl["total_fees"]).sum()
        rep["gross_pnl_cycles"] = float(gross)
        rep["total_fees_cycles"] = float(cl["total_fees"].sum())
        rep["fee_ratio_cycles"] = float(cl["total_fees"].sum() / gross) if gross != 0 else np.nan
        rep["max_dd_quantiles"] = {q: float(cl["max_dd"].quantile(q))
                                   for q in (0.5, 0.9, 0.99, 0.999, 1.0)}
    else:
        rep["layer_table"] = pd.DataFrame()
        rep["low_layer_ratio"] = np.nan
        rep["holding_overall"] = {}
        rep["fee_ratio_cycles"] = np.nan
        rep["max_dd_quantiles"] = {}

    tp = trades_df[trades_df["outcome"].to_numpy(object) == "tp"]
    if len(tp):
        gr = (tp["net_pnl"] + tp["total_fees"]).sum()
        rep["fee_ratio_traded"] = float(tp["total_fees"].sum() / gr) if gr != 0 else np.nan
    else:
        rep["fee_ratio_traded"] = np.nan

    v = []
    r = rep["doubles_per_blowup"]
    if not np.isnan(r):
        v.append("Doubles/Blow-up = %.2f -> %s" % (
            r, "数学期望上允许爆仓前完成翻倍抽本 (>1)" if r > 1 else "长期必定向下破产 (<1)"))
    if not np.isnan(rep["low_layer_ratio"]):
        v.append("0-1 层占比 %.1f%% -> %s" % (
            rep["low_layer_ratio"] * 100,
            "信号有效" if rep["low_layer_ratio"] >= 0.4 else "信号端失效, 纯靠资金杠杆硬扛"))
    if not np.isnan(rep["fee_ratio_cycles"]):
        v.append("手续费/毛利 = %.1f%% -> %s" % (
            rep["fee_ratio_cycles"] * 100,
            "成本陷阱, 需放宽止盈/间距" if rep["fee_ratio_cycles"] > 0.5 else "成本可接受"))
    rep["verdict"] = v
    return rep


def print_report(rep):
    f = lambda x, n=4: ("%%.%df" % n % x) if isinstance(x, float) and np.isfinite(x) else str(x)
    print("=" * 74)
    print(" 马丁格尔『抽本利润跑』评估报告   Margin = %s Unit(首单名义价值倍数)"
          % f(rep["margin"], 6))
    print("=" * 74)
    print("[数据区间] %.1f 天 (%.2f 年) | Stage1 平行宇宙 Cycle 数: %d | 闭环: %d"
          % (rep["span_day"], rep["span_year"], rep["n_cycles_total"], rep["n_closed_cycles"]))
    print("-" * 74)
    print("【时间线重组结果】")
    print("  成交笔数 %d (止盈 %d / 爆仓 %d / 末端未平 %d) | 信号利用率 %.1f%%"
          % (rep["n_trades"], rep["n_tp"], rep["n_blowup"], rep["n_mtm"],
             100.0 * (rep["signal_utilization"] if np.isfinite(rep["signal_utilization"]) else np.nan)))
    print("  胜率 %.2f%% | 净利合计 %s Unit = %.3f 倍 Margin | 年化 %.2f 倍 Margin"
          % (100.0 * rep["win_rate"] if np.isfinite(rep["win_rate"]) else float("nan"),
             f(rep["total_net_pnl"], 6), rep["total_net_pnl_in_margin"], rep["margins_per_year"]))
    print("-" * 74)
    print("【存亡博弈 (The Free Ride Metrics)】")
    print("  Time to Double      : %s h  (中位 %s h, 样本 %d)"
          % (f(rep["time_to_double_hour"], 2), f(rep["time_to_double_median_hour"], 2), rep["n_doubles"]))
    print("  Expected Lifespan   : %s h  (相邻爆仓间隔 %s h, 年爆仓 %.2f 次)"
          % (f(rep["expected_lifespan_hour"], 2), f(rep["blowup_interval_hour"], 2),
             rep["blowups_per_year"]))
    print("  Doubles per Blow-up : %s" % f(rep["doubles_per_blowup"], 3))
    print("  翻倍胜率(死前抽本成功率): %s   (完整生命 %d 条)"
          % (f(rep["free_ride_win_rate"], 3), rep["n_lives_completed"]))
    print("-" * 74)
    print("【资金深度与效率 (仅闭环 Cycle)】")
    if len(rep["layer_table"]):
        t = rep["layer_table"]
        print("  层数  占比     累计     持仓均值h  中位h   P95h    平均最大浮亏  最深浮亏")
        for k in t.index:
            r = t.loc[k]
            print("  %4d  %6.2f%%  %6.2f%%  %9.2f  %6.2f  %6.2f  %12.5f  %8.5f"
                  % (k, r["pct"] * 100, r["cum_pct"] * 100, r["hold_mean_h"],
                     r["hold_median_h"], r["hold_p95_h"], r["dd_mean"], r["dd_max"]))
        h = rep["holding_overall"]
        print("  全体持仓: 均值 %.2f h | 中位 %.2f h | P95 %.2f h | 最长 %.2f h"
              % (h["mean_h"], h["median_h"], h["p95_h"], h["max_h"]))
        q = rep["max_dd_quantiles"]
        print("  Cycle 最大浮亏分位: " + " | ".join("%g%%=%.5f" % (k * 100, v) for k, v in q.items()))
    print("-" * 74)
    print("【微观摩擦】手续费/毛利 = %s (Stage1 闭环) | %s (实际成交)"
          % (f(rep["fee_ratio_cycles"], 4), f(rep["fee_ratio_traded"], 4)))
    print("-" * 74)
    for s in rep["verdict"]:
        print("  * " + s)
    print("=" * 74)


def sweep_margins(replayer, margins, verbose=False):
    """毫秒级扫描多个 Margin, 返回横向对比表。"""
    rows = []
    for mg in margins:
        tr = replayer.run(mg)
        rp = evaluate_free_ride(tr, replayer.cycles, mg)
        rows.append({
            "margin": mg,
            "trades": rp["n_trades"],
            "tp": rp["n_tp"],
            "blowup": rp["n_blowup"],
            "win_rate": rp["win_rate"],
            "net_pnl_unit": rp["total_net_pnl"],
            "net_in_margin": rp["total_net_pnl_in_margin"],
            "margins_per_year": rp["margins_per_year"],
            "time_to_double_h": rp["time_to_double_hour"],
            "expected_lifespan_h": rp["expected_lifespan_hour"],
            "doubles_per_blowup": rp["doubles_per_blowup"],
            "free_ride_win_rate": rp["free_ride_win_rate"],
        })
        if verbose:
            print("margin=%-10g trades=%-6d blowup=%-5d ratio=%s"
                  % (mg, rp["n_trades"], rp["n_blowup"], rp["doubles_per_blowup"]))
    return pd.DataFrame(rows)


def run_backtest(df, data_name="default_data", margins=(0.02, 0.16, 0.6, 2.55, 10.0, 40.6),
                 report_margin=None, **stage1_kw):
    """一站式: Stage1 -> Replayer -> 扫描 -> 详细报告 (已支持 Stage 1 结果本地缓存跳过)"""

    # 抽取核心参数，组装缓存文件名
    fee = stage1_kw.get('fee_rate', DEFAULT_FEE)
    add = stage1_kw.get('add_step', DEFAULT_ADD_STEP)
    tp = stage1_kw.get('tp_step', DEFAULT_TP_STEP)
    mult = stage1_kw.get('multiplier', DEFAULT_MULT)
    dd_abort = stage1_kw.get('dd_abort', 'None')
    max_l = stage1_kw.get('max_layer_hard', 512)
    mtm = stage1_kw.get('mtm_charge_close_fee', True)

    cache_filename = f"backest/stage1_{data_name}_f{fee}_a{add}_t{tp}_m{mult}_da{dd_abort}_ml{max_l}_mtm{mtm}.pkl"

    if os.path.exists(cache_filename):
        _log("[缓存系统] 发现匹配的 Stage 1 缓存: %s (%.1f MB), 跳过高昂计算直接加载..."
             % (cache_filename, os.path.getsize(cache_filename) / 1048576.0))
        t = time.time()
        with open(cache_filename, 'rb') as f:
            cached_data = pickle.load(f)
            cycles = cached_data['df']
            cycles.attrs = cached_data['attrs']
        del cached_data
        gc.collect()
        _log("[缓存系统] 加载完成: %d cycles, 耗时 %.1fs" % (len(cycles), time.time() - t))
    else:
        _log("[缓存系统] 未发现匹配缓存, 开始运行 Stage 1, 结果将保存至: %s" % cache_filename)
        cycles = run_stage1(df, **stage1_kw)
        t = time.time()
        with open(cache_filename, 'wb') as f:
            # Pandas 偶尔在序列化时丢弃 attrs，使用字典确保元数据一同保存
            pickle.dump({'df': cycles, 'attrs': dict(cycles.attrs)}, f,
                        protocol=pickle.HIGHEST_PROTOCOL)
        _log("[缓存系统] 已写盘: %s (%.1f MB, %.1fs)"
             % (cache_filename, os.path.getsize(cache_filename) / 1048576.0, time.time() - t))

    _log("Stage2 构建时间线重组器 ...")
    t = time.time()
    rp = TimelineReplayer(cycles)
    _log("Stage2 重组器就绪 (%.2fs), 开始扫描 %d 个 Margin ..." % (time.time() - t, len(margins)))
    t = time.time()
    sweep = sweep_margins(rp, margins)
    _log("Margin 扫描完成 (%.2fs)" % (time.time() - t))
    if report_margin is None and len(margins):
        report_margin = list(margins)[len(margins) // 2]
    trades = rp.run(report_margin)
    report = evaluate_free_ride(trades, cycles, report_margin)
    print_report(report)
    return cycles, rp, sweep, trades, report


# =====================================================================
# 4. 执行入口 (按需批量化一阶段生成)
# =====================================================================
if __name__ == "__main__":
    if not _HAS_NUMBA:
        print("[警告] 未检测到 numba, Stage 1 将退化为纯 Python 单线程 (慢 100 倍以上)。"
              "请执行: pip install numba")

    _log("引擎启动 | Python %s | numpy %s | pandas %s | numba=%s | CPU %d"
         % (sys.version.split()[0], np.__version__, pd.__version__, _HAS_NUMBA, os.cpu_count() or 1))
    if _rss_mb() != _rss_mb():
        _log("提示: 未安装 psutil, 日志中的 RSS 内存观测不可用 (pip install psutil 可开启)")

    symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "LINKUSDT",
        "AAVEUSDT",
        "BNBUSDT"
    ]

    # 将 16 个策略加入列表以供循环调用
    strategies = [
        strategy_1_vwap_zscore,
        strategy_2_quantile_deviation,
        strategy_3_periodic_open_deviation,
        strategy_4_volume_price_absorption,
        strategy_5_liquidity_vacuum,
        strategy_6_volume_climax,
        strategy_7_proxy_cvd_divergence,
        strategy_8_rolling_stop_hunt,
        strategy_9_wick_rejection_ratio,
        strategy_10_close_position_bias,
        strategy_11_tick_gap_reversion,
        strategy_12_kaufman_efficiency_ratio,
        strategy_13_run_length_streaks,
        strategy_14_volatility_squeeze_expansion,
        strategy_15_micro_autocorrelation,
        strategy_16_clock_aligned_anomaly
    ]

    base_path = r"W:\project\python_project\oke_auto_trade\kline_data"

    # 显式 dtype: 避免 read_csv 类型推断产生额外内存峰值
    CSV_DTYPES = {"open_time": np.int64, "open": np.float64, "high": np.float64,
                  "low": np.float64, "close": np.float64, "volume": np.float64}

    total_task = len(symbols) * len(strategies) * 2
    task_done = 0
    t_all = time.time()

    # ==========================
    # 核心批量处理逻辑
    # ==========================
    for sym_i, symbol in enumerate(symbols, 1):
        file_path = os.path.join(base_path, f"{symbol}_1s_2021-01-01_merged_6cols.csv")

        if not os.path.exists(file_path):
            _log("[警告] 找不到对应的 K 线文件, 跳过该币种: %s" % file_path)
            task_done += len(strategies) * 2
            continue

        _log("=" * 78)
        _log("[币种 %d/%d] 正在加载本地 K 线数据: %s" % (sym_i, len(symbols), symbol))
        t = time.time()
        try:
            df_main = pd.read_csv(file_path, dtype=CSV_DTYPES)
        except Exception as _e:
            _log("显式 dtype 读取失败 (%s), 回退默认解析" % _e, 1)
            df_main = pd.read_csv(file_path)
        gc.collect()
        _log("加载完成: %d 行 x %d 列 | DataFrame %.0f MB | 耗时 %.1fs"
             % (len(df_main), df_main.shape[1],
                df_main.memory_usage(deep=False).sum() / 1048576.0, time.time() - t), 1)

        # 遍历 16 个策略进行处理
        for st_i, strat_func in enumerate(strategies, 1):
            strat_name = strat_func.__name__
            _log("-" * 78)
            _log("[%s | 策略 %d/%d] %s   (全局任务 %d/%d, 累计 %s)"
                 % (symbol, st_i, len(strategies), strat_name, task_done, total_task,
                    _fmt_hms(time.time() - t_all)))

            data_name_long = f"{symbol}_{strat_name}_Long"
            data_name_short = f"{symbol}_{strat_name}_Short"
            # 沿用原来的缓存命名方式，保证未来如果接入 run_backtest 也兼容
            cache_filename_long = f"backest/stage1_{data_name_long}_f{DEFAULT_FEE}_a{DEFAULT_ADD_STEP}_t{DEFAULT_TP_STEP}_m{DEFAULT_MULT}_daNone_ml512_mtmTrue.pkl"
            cache_filename_short = f"backest/stage1_{data_name_short}_f{DEFAULT_FEE}_a{DEFAULT_ADD_STEP}_t{DEFAULT_TP_STEP}_m{DEFAULT_MULT}_daNone_ml512_mtmTrue.pkl"
            has_long = os.path.exists(cache_filename_long)
            has_short = os.path.exists(cache_filename_short)

            if has_long and has_short:
                _log("多空缓存均已存在, 连信号计算一起跳过", 1)
                task_done += 2
                continue

            # ---- 信号计算: copy(deep=False) 共享底层数据块, 省掉一份整表拷贝 ----
            t = time.time()
            df_strat = strat_func(df_main.copy(deep=False))
            # 确保信号列被规范化为 0 或 1，且没有 NaN
            signal_np = df_strat['signal'].fillna(False).astype(np.int8).to_numpy()
            del df_strat  # 立刻释放整份策略副本, 压低内存峰值
            gc.collect()
            n_sig = int(signal_np.sum())
            _log("信号计算完成: 耗时 %.1fs | 命中 %d / %d (%.4f%%)"
                 % (time.time() - t, n_sig, signal_np.shape[0],
                    100.0 * n_sig / max(signal_np.shape[0], 1)), 1)
            if n_sig == 0:
                _log("提示: 本策略在该币种上无任何信号, Stage 1 将输出空表", 1)

            # Stage 1 只需要 open_time/high/low/close + 信号列。
            # 这里直接把信号列临时挂到 df_main 上(用完即 drop), 彻底避免再复制一份 K 线。
            for tag, sig_col, cache_fn, exists in (
                    ("做多", "long_signal", cache_filename_long, has_long),
                    ("做空", "short_signal", cache_filename_short, has_short)):
                task_done += 1
                if exists:
                    _log("> [%s] 缓存已存在, 跳过运算: %s" % (tag, cache_fn), 1)
                    continue

                # 清理上一轮残留信号列(run_stage1 对不存在的列视作"无信号", 无需 zeros 数组)
                _drop = [c for c in ("long_signal", "short_signal") if c in df_main.columns]
                if _drop:
                    df_main.drop(columns=_drop, inplace=True)
                df_main[sig_col] = signal_np

                _log("> [%s] 开始运行 Stage 1 ... (全局任务 %d/%d)" % (tag, task_done, total_task), 1)
                t1 = time.time()
                cycles_tmp = run_stage1(df_main, log_interval_sec=15.0, verbose=True)
                _log("> [%s] Stage 1 结束, 耗时 %s, 开始写盘 ..." % (tag, _fmt_hms(time.time() - t1)), 1)

                t2 = time.time()
                with open(cache_fn, 'wb') as f:
                    pickle.dump({'df': cycles_tmp, 'attrs': dict(cycles_tmp.attrs)}, f,
                                protocol=pickle.HIGHEST_PROTOCOL)
                _log("> [%s] 一阶段已保存: %s (%.1f MB, 写盘 %.1fs)"
                     % (tag, cache_fn, os.path.getsize(cache_fn) / 1048576.0, time.time() - t2), 1)

                del cycles_tmp
                _drop = [c for c in ("long_signal", "short_signal") if c in df_main.columns]
                if _drop:
                    df_main.drop(columns=_drop, inplace=True)
                gc.collect()
                _log("> [%s] 内存已回收 | 全局进度 %d/%d | 累计耗时 %s"
                     % (tag, task_done, total_task, _fmt_hms(time.time() - t_all)), 1)

            del signal_np
            gc.collect()

        del df_main
        gc.collect()
        _log("%s 全部策略处理完毕, 已释放该币种 K 线内存" % symbol, 1)

    _log("=" * 78)
    _log("所有币种(6个) x 策略(16个) x 多/空(2种) = %d 份一阶段文件已处理完毕! 总耗时 %s"
         % (total_task, _fmt_hms(time.time() - t_all)))