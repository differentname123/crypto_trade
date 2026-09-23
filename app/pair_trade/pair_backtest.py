# -*- coding: utf-8 -*-
"""ALT/BTC 固定期限统计套利：LONG_ALT / SHORT_ALT 分方向最终精搜。

作者：zhuxiaohu, AI Assistant

运行：python alt_directional_fine_search.py
依赖：pandas、numpy；tqdm 可选。请先修改 Config 的数据路径。

默认搜索：LONG_ALT 1080 组 + SHORT_ALT 1440 组 = 2520 组。
只跑多头：Config.SEARCH_DIRECTIONS = ("LONG_ALT",)
只跑空头：Config.SEARCH_DIRECTIONS = ("SHORT_ALT",)
SHORT_* 参数均表示“短期确认”，不表示只用于做空。

模型口径沿用原脚本：
1. USDT 线性合约，volume 为基础币数量；时间戳为 UTC 收盘边界 open_time + 1h。
2. 在信号小时收盘价开仓；未模拟信号计算延迟、滑点、资金费、订单精度或强平。
3. 每笔双腿初始毛名义固定，以长期 Beta 分配并冻结带符号数量。
4. 每个参数组合只交易指定 ALT 方向，每币同时最多一笔，固定期限退出。
5. 多空任务独立回测，没有共享资金或跨方向仓位约束；统计为独立交易样本。
6. MAE/MFE 按小时收盘的净清算收益率计算，含开/平仓时点及费用。
   MAE <= 0、MFE >= 0；无对应方向偏移时为 0 且时间为空。
   价格路径不完整或交易未结算时，四个极值字段全部为空。
7. 为兼容原字段，ret_24h 实际使用 SIGNAL_WINDOW_HOURS；avg_turnover_30d
   实际使用 BETA_WINDOW_DAYS 天的历史日均成交额，不一定是 24h / 30d。
8. CSV 源文件须在整次搜索期间保持不变；启动时缺失 ALT 文件会跳过，
   启动后文件消失则报错，避免各组币池不一致。
9. 每笔原双腿交易同步计算 ALT_ONLY 反事实：使用相同信号、方向、入场时点和固定持有期，
   初始名义为 PAIR_GROSS_NOTIONAL，仅持有 ALT，BTC 数量恒为 0，费用只按 ALT 实际成交额计。
"""

import glob
import hashlib
import itertools
import json
import multiprocessing
import os
import tempfile
import traceback
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime, timezone

import numpy as np
import pandas as pd

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable


GRID_KEYS = (
    "Z_THRESHOLDS_TO_TEST", "HOLDING_PERIODS_TO_TEST",
    "BETA_WINDOWS_TO_TEST", "SIGNAL_WINDOWS_TO_TEST",
    "SHORT_BETA_WINDOWS_TO_TEST", "SHORT_SIGNAL_WINDOWS_TO_TEST",
    "SHORT_EXCESS_THRESHOLDS_TO_TEST", "SHORT_MIN_BAR_RATIOS_TO_TEST",
    "SHORT_CONFIRM_MODES_TO_TEST", "SHORT_CONFIRM_TIMINGS_TO_TEST",
)
VALID_DIRECTIONS = ("LONG_ALT", "SHORT_ALT")


def require_integer(name, value, minimum=1):
    try:
        valid = (not isinstance(value, (bool, np.bool_))
                 and np.isfinite(value) and int(value) == value and value >= minimum)
    except (TypeError, ValueError, OverflowError):
        valid = False
    if not valid:
        raise ValueError(f"{name}必须为不小于{minimum}的整数")
    return int(value)


def require_number(name, value, minimum=0.0, strict=False, maximum=None):
    try:
        valid = (not isinstance(value, (bool, np.bool_)) and np.isfinite(value)
                 and (value > minimum if strict else value >= minimum)
                 and (maximum is None or value <= maximum))
    except (TypeError, ValueError, OverflowError):
        valid = False
    if not valid:
        relation = ">" if strict else ">="
        raise ValueError(f"{name}必须为有限数值且{relation}{minimum}"
                         + (f"，同时<={maximum}" if maximum is not None else ""))
    return float(value)


# ==========================================
# 1. 全局配置与两个独立搜索空间
# ==========================================
class Config:
    DATA_DIR = r"W:\project\python_project\oke_auto_trade\kline_data"
    BASE_OUTPUT_DIR = r"trade_results"
    BTC_SYMBOL = "BTCUSDT"
    KLINE_FILE_TEMPLATE = "{symbol}_1h_2021-01-01_merged.csv"
    SYMBOLS_FILE = "symbols.json"

    # 同时运行两个方向，或者改成只有一个方向的元组。
    SEARCH_DIRECTIONS = ("LONG_ALT", "SHORT_ALT")

    # 每个 worker 由任务覆盖以下单组参数；不会同时开多和开空 ALT。
    TRADE_DIRECTION = "LONG_ALT"
    BETA_WINDOW_DAYS = 45
    SIGNAL_WINDOW_HOURS = 18
    Z_SCORE_THRESHOLD = 5.0
    HOLDING_PERIOD_HOURS = 72
    SHORT_BETA_WINDOW_DAYS = 5
    SHORT_SIGNAL_WINDOW_HOURS = 18
    SHORT_EXCESS_THRESHOLD = 0.0
    SHORT_MIN_BAR_RATIO = 0.0
    SHORT_MIN_REGIME_BARS = 2
    SHORT_CONFIRM_MODE = "UP"
    SHORT_CONFIRM_TIMING = "ROLLING"

    FEE_RATE = 0.001  # 每腿每次实际成交额的 0.1%，不另计资金费。
    PAIR_GROSS_NOTIONAL = 1000.0
    MIN_BTC_VARIANCE = 1e-16
    MIN_RESIDUAL_STD = 1e-10
    ENTRY_START = None
    EVALUATION_END = None

    # LONG ALT 专用最终精搜：5*4*3*3*2*3*1*1*1*1 = 1080。
    LONG_PARAM_SPACE = {
        "Z_THRESHOLDS_TO_TEST": [5.0, 5.5, 6.0, 6.5, 7.0],
        "HOLDING_PERIODS_TO_TEST": [72, 84, 96, 120],
        "BETA_WINDOWS_TO_TEST": [45, 60, 90],
        "SIGNAL_WINDOWS_TO_TEST": [18, 24, 30],
        "SHORT_BETA_WINDOWS_TO_TEST": [5, 7],
        "SHORT_SIGNAL_WINDOWS_TO_TEST": [18, 24, 30],
        "SHORT_EXCESS_THRESHOLDS_TO_TEST": [0.0],
        "SHORT_MIN_BAR_RATIOS_TO_TEST": [0.0],
        "SHORT_CONFIRM_MODES_TO_TEST": ["UP"],
        "SHORT_CONFIRM_TIMINGS_TO_TEST": ["ROLLING"],
    }

    # SHORT ALT 专用最终精搜：5*3*3*1*1*4*1*4*2*1 = 1440。
    SHORT_PARAM_SPACE = {
        "Z_THRESHOLDS_TO_TEST": [8.0, 8.5, 9.0, 9.5, 10.0],
        "HOLDING_PERIODS_TO_TEST": [72, 96, 120],
        "BETA_WINDOWS_TO_TEST": [45, 60, 90],
        "SIGNAL_WINDOWS_TO_TEST": [24],
        "SHORT_BETA_WINDOWS_TO_TEST": [7],
        "SHORT_SIGNAL_WINDOWS_TO_TEST": [8, 12, 16, 20],
        "SHORT_EXCESS_THRESHOLDS_TO_TEST": [0.0],
        "SHORT_MIN_BAR_RATIOS_TO_TEST": [0.0, 0.25, 0.50, 0.75],
        "SHORT_CONFIRM_MODES_TO_TEST": ["UP", "BOTH"],
        "SHORT_CONFIRM_TIMINGS_TO_TEST": ["POST_TRIGGER"],
    }

    MAX_GRID_COMBINATIONS = 3000  # 两组相加为 2520，不能沿用原来的 1000。
    MAX_WORKERS = max(1, min(10, (os.cpu_count() or 1) - 1))
    NUMERIC_THREADS_PER_WORKER = 1
    CACHE_VERSION = "directional_fine_search_fixed_beta_excursion_net_alt_only_v6"

    BETA_WINDOW_HOURS = BETA_WINDOW_DAYS * 24
    SHORT_BETA_WINDOW_HOURS = SHORT_BETA_WINDOW_DAYS * 24
    PARAM_FOLDER = ""
    OUTPUT_DIR = ""
    RUN_ID = ""
    MARKET_ID = ""

    @classmethod
    def update_params(cls, trade_direction, z_score, holding_period,
                      beta_window=45, signal_window=18,
                      short_beta_window=5, short_signal_window=18,
                      short_excess_threshold=0.0, short_min_bar_ratio=0.0,
                      short_confirm_mode="UP", short_confirm_timing="ROLLING"):
        """参数元组第一个元素是方向；校验完成后才更新 Config。"""
        if trade_direction not in VALID_DIRECTIONS:
            raise ValueError("trade_direction必须为LONG_ALT或SHORT_ALT")
        z_score = require_number("z_score", z_score, strict=True)
        holding_period = require_integer("holding_period", holding_period)
        beta_window = require_integer("beta_window", beta_window)
        signal_window = require_integer("signal_window", signal_window)
        short_beta_window = require_integer("short_beta_window", short_beta_window)
        short_signal_window = require_integer("short_signal_window", short_signal_window, 2)
        short_excess_threshold = require_number("short_excess_threshold", short_excess_threshold)
        short_min_bar_ratio = require_number("short_min_bar_ratio", short_min_bar_ratio, maximum=1)
        require_integer("SHORT_MIN_REGIME_BARS", cls.SHORT_MIN_REGIME_BARS, 2)
        if beta_window * 24 <= signal_window:
            raise ValueError("长期Beta历史小时数必须长于长期信号窗口")
        if short_confirm_mode not in ("NET", "UP", "DOWN", "BOTH", "EITHER"):
            raise ValueError("short_confirm_mode必须为NET/UP/DOWN/BOTH/EITHER")
        if short_confirm_timing not in ("ROLLING", "POST_TRIGGER"):
            raise ValueError("short_confirm_timing必须为ROLLING或POST_TRIGGER")

        cls.TRADE_DIRECTION = trade_direction
        cls.Z_SCORE_THRESHOLD = z_score
        cls.HOLDING_PERIOD_HOURS = holding_period
        cls.BETA_WINDOW_DAYS = beta_window
        cls.SIGNAL_WINDOW_HOURS = signal_window
        cls.SHORT_BETA_WINDOW_DAYS = short_beta_window
        cls.SHORT_SIGNAL_WINDOW_HOURS = short_signal_window
        cls.SHORT_EXCESS_THRESHOLD = short_excess_threshold
        cls.SHORT_MIN_BAR_RATIO = short_min_bar_ratio
        cls.SHORT_CONFIRM_MODE = short_confirm_mode
        cls.SHORT_CONFIRM_TIMING = short_confirm_timing
        cls.BETA_WINDOW_HOURS = beta_window * 24
        cls.SHORT_BETA_WINDOW_HOURS = short_beta_window * 24
        cls.PARAM_FOLDER = (
            f"{trade_direction}_Z{z_score}_H{holding_period}"
            f"_B{beta_window}_S{signal_window}"
            f"_b{short_beta_window}_s{short_signal_window}"
            f"_e{short_excess_threshold}_p{short_min_bar_ratio}"
            f"_n{cls.SHORT_MIN_REGIME_BARS}_{short_confirm_mode}_{short_confirm_timing}"
        )
        cls.OUTPUT_DIR = os.path.join(os.path.abspath(cls.BASE_OUTPUT_DIR),
                                      trade_direction, cls.PARAM_FOLDER)
        cls.RUN_ID = cls.MARKET_ID = ""


TRADE_COLUMNS = [
    "run_id", "search_direction", "trade_id", "symbol", "status", "entry_time",
    "scheduled_exit_time", "exit_time", "direction", "btc_direction", "entry_price",
    "exit_price", "btc_entry", "btc_exit", "beta", "z_score", "exit_z_score",
    "hist_res_mean", "hist_res_std", "avg_turnover_30d", "market_median_at_entry",
    "vol_group", "alt_qty", "btc_qty", "alt_entry_notional", "btc_entry_notional",
    "entry_gross_notional", "alt_exit_notional", "btc_exit_notional",
    "alt_gross_pnl", "btc_gross_pnl", "gross_pnl", "entry_cost", "exit_cost",
    "total_cost", "net_pnl", "gross_return", "net_return", "holding_hours",
    "planned_holding_hours", "fee_rate", "z_threshold", "beta_window_hours",
    "signal_window_hours", "short_beta_window_hours", "short_signal_window_hours",
    "short_excess_threshold", "short_min_bar_ratio", "short_min_regime_bars",
    "short_confirm_mode", "short_confirm_timing", "short_beta",
    "short_mean_excess", "short_up_mean_excess", "short_down_mean_excess",
    "short_support_ratio", "short_up_support_ratio", "short_down_support_ratio",
    "short_up_count", "short_down_count", "long_trigger_time", "long_trigger_z",
    "exit_reason", "mae_return", "mfe_return", "mae_time", "mfe_time",
    "alt_only_status", "alt_only_exit_time", "alt_only_exit_price",
    "alt_only_holding_hours", "alt_only_exit_reason", "alt_only_qty",
    "alt_only_btc_qty", "alt_only_entry_notional", "alt_only_exit_notional",
    "alt_only_gross_pnl", "alt_only_entry_cost", "alt_only_exit_cost",
    "alt_only_total_cost", "alt_only_net_pnl", "alt_only_gross_return",
    "alt_only_net_return", "alt_only_mae_return", "alt_only_mfe_return",
    "alt_only_mae_time", "alt_only_mfe_time",
]


def utc_timestamp(value):
    if value is None:
        return None
    result = pd.to_datetime(value, utc=True)
    if pd.isna(result):
        raise ValueError("时间边界不能为NaT")
    return result


def kline_path(symbol):
    return os.path.join(Config.DATA_DIR, Config.KLINE_FILE_TEMPLATE.format(symbol=symbol))


def load_kline(symbol):
    """排序、拒绝重复时间戳，保留缺口为 NaN；禁止填价格。"""
    df = pd.read_csv(kline_path(symbol), usecols=["open_time", "close", "volume"])
    if df.empty:
        raise ValueError(f"{symbol}: K线文件为空")
    df["open_time"] = pd.to_datetime(
        pd.to_numeric(df["open_time"], errors="raise"), unit="ms", utc=True)
    if df["open_time"].isna().any() or df["open_time"].duplicated().any():
        raise ValueError(f"{symbol}: 空时间戳或重复时间戳，需先修复源数据")
    if not df["open_time"].eq(df["open_time"].dt.floor("h")).all():
        raise ValueError(f"{symbol}: 数据不是整点小时线")
    df = df.set_index("open_time").sort_index()
    df.index = df.index + pd.Timedelta(hours=1)
    df.index.name = "close_time"
    for col in ("close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
        valid = np.isfinite(df[col]) & (df[col] > 0 if col == "close" else df[col] >= 0)
        df[col] = df[col].where(valid)
    return df.reindex(pd.date_range(df.index.min(), df.index.max(), freq="h", name="close_time"))


def align_hourly(df_alt, df_btc):
    """公共时间轴延伸到较晚结尾，显式保留退市和尾部缺口。"""
    start = max(df_alt.index.min(), df_btc.index.min())
    end = max(df_alt.index.max(), df_btc.index.max())
    limit = utc_timestamp(Config.EVALUATION_END)
    if limit is not None:
        end = min(end, limit)
    index = pd.date_range(start, end, freq="h", name="close_time")
    return df_alt.reindex(index), df_btc.reindex(index)


def eligible_pool(df_alt, df_btc):
    n = Config.BETA_WINDOW_HOURS + Config.SIGNAL_WINDOW_HOURS + 1
    good = (df_alt["close"].notna() & df_alt["volume"].notna()
            & df_btc["close"].notna())
    return good.astype(float).rolling(n, min_periods=n).sum().eq(n)


def historical_turnover(df):
    # 排除当前小时；虽然保留原字段名，但窗口跟随长期 Beta 天数。
    return (df["volume"] * df["close"]).shift(1).rolling(
        Config.BETA_WINDOW_HOURS, min_periods=Config.BETA_WINDOW_HOURS).mean() * 24


def atomic_csv(df, path, reuse_existing=False, **kwargs):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    if reuse_existing and os.path.isfile(path):
        return
    fd, temporary = tempfile.mkstemp(prefix=os.path.basename(path) + ".",
                                     suffix=".tmp", dir=os.path.dirname(path) or ".")
    os.close(fd)
    try:
        df.to_csv(temporary, **kwargs)
        try:
            os.replace(temporary, path)
        except PermissionError:
            # Windows 可能无法替换其他进程正在读取的同指纹市场缓存。
            if not (reuse_existing and os.path.isfile(path)):
                raise
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)


def atomic_json(value, path):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=os.path.basename(path) + ".",
                                     suffix=".tmp", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False, indent=2, allow_nan=False)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)


def result_path(name):
    # 目录已经包含方向和全部参数；文件名不再重复参数，以降低 Windows 路径长度。
    return os.path.join(Config.OUTPUT_DIR, name)


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def fingerprint(value):
    encoded = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()
    return hashlib.sha256(encoded).hexdigest()


def prepare_run(symbols, data_fingerprints=None):
    """计算指纹和路径；manifest 由持有运行锁的调用方发布。"""
    require_number("FEE_RATE", Config.FEE_RATE)
    require_number("PAIR_GROSS_NOTIONAL", Config.PAIR_GROSS_NOTIONAL, strict=True)
    require_number("MIN_BTC_VARIANCE", Config.MIN_BTC_VARIANCE)
    require_number("MIN_RESIDUAL_STD", Config.MIN_RESIDUAL_STD, strict=True)
    if Config.TRADE_DIRECTION not in VALID_DIRECTIONS or not Config.PARAM_FOLDER:
        raise ValueError("须先调用Config.update_params设置本次搜索方向与参数")
    begin, end = utc_timestamp(Config.ENTRY_START), utc_timestamp(Config.EVALUATION_END)
    if begin is not None and end is not None and begin >= end:
        raise ValueError("ENTRY_START必须早于EVALUATION_END")
    universe = sorted(set(symbols) | {Config.BTC_SYMBOL})
    missing = [s for s in universe if not os.path.isfile(kline_path(s))]
    if missing:
        raise FileNotFoundError(f"静态币池中的源文件缺失: {missing}；请检查后重新启动搜索")
    if data_fingerprints is None:
        data_fingerprints = {s: file_sha256(kline_path(s)) for s in universe}
    if any(s not in data_fingerprints for s in universe):
        raise ValueError("数据指纹未覆盖本次完整币池")

    # 市场分组只依赖数据、币池、长期窗口和结束边界，可在多空之间共用。
    market = dict(
        version=Config.CACHE_VERSION, code=file_sha256(os.path.abspath(__file__)),
        data={s: data_fingerprints[s] for s in universe}, universe=universe,
        btc=Config.BTC_SYMBOL, beta=Config.BETA_WINDOW_HOURS,
        signal=Config.SIGNAL_WINDOW_HOURS, end=end.isoformat() if end is not None else None,
        pandas=pd.__version__, numpy=np.__version__,
    )
    Config.MARKET_ID = fingerprint(market)
    manifest = dict(
        market=market, trade_direction=Config.TRADE_DIRECTION,
        z=Config.Z_SCORE_THRESHOLD, holding=Config.HOLDING_PERIOD_HOURS,
        short_beta=Config.SHORT_BETA_WINDOW_HOURS,
        short_signal=Config.SHORT_SIGNAL_WINDOW_HOURS,
        short_excess_threshold=Config.SHORT_EXCESS_THRESHOLD,
        short_min_bar_ratio=Config.SHORT_MIN_BAR_RATIO,
        short_min_regime_bars=Config.SHORT_MIN_REGIME_BARS,
        short_confirm_mode=Config.SHORT_CONFIRM_MODE,
        short_confirm_timing=Config.SHORT_CONFIRM_TIMING,
        short_basis="mean_hourly_log_residual_fixed_pre_window_beta",
        direction_policy="one_alt_side_per_run_normal_band_rearm",
        fee=Config.FEE_RATE, gross=Config.PAIR_GROSS_NOTIONAL,
        alt_only=dict(enabled=True, same_signal=True, same_holding=True,
                      gross=Config.PAIR_GROSS_NOTIONAL, btc_qty=0.0,
                      fee_basis="alt_actual_turnover"),
        start=begin.isoformat() if begin is not None else None,
        min_var=Config.MIN_BTC_VARIANCE, min_std=Config.MIN_RESIDUAL_STD,
        excursion=dict(basis="net_liquidation_return", sampling="hourly_close",
                       include_entry=True, include_exit=True,
                       missing_policy="invalidate_all_four_fields",
                       zero_time_policy="NaT", tie_policy="first"),
    )
    Config.RUN_ID = fingerprint(manifest)
    Config.OUTPUT_DIR = os.path.join(
        os.path.abspath(Config.BASE_OUTPUT_DIR), Config.TRADE_DIRECTION,
        Config.PARAM_FOLDER + "_" + Config.RUN_ID[:16])
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    return manifest


# ==========================================
# 2. 原有长期指标和固定短期 Beta 确认
# ==========================================
def calculate_indicators(df_alt, df_btc):
    """用信号窗口之前估计的固定 Beta 重算历史残差分布。"""
    df_alt, df_btc = align_hourly(df_alt, df_btc)
    df = pd.DataFrame(index=df_alt.index)
    df["close"] = df_alt["close"]
    df["volume"] = df_alt["volume"]
    df["btc_close"] = df_btc["close"]
    B, S = Config.BETA_WINDOW_HOURS, Config.SIGNAL_WINDOW_HOURS
    df["ret_1h"] = np.log(df["close"]).diff()
    df["btc_ret_1h"] = np.log(df["btc_close"]).diff()
    df["ret_24h"] = df["ret_1h"].rolling(S, min_periods=S).sum()
    df["btc_ret_24h"] = df["btc_ret_1h"].rolling(S, min_periods=S).sum()
    cov = df["ret_1h"].rolling(B, min_periods=B).cov(df["btc_ret_1h"])
    var = df["btc_ret_1h"].rolling(B, min_periods=B).var()
    df["beta"] = cov / var.where(var > Config.MIN_BTC_VARIANCE)
    df["beta_shifted"] = df["beta"].shift(S).replace([np.inf, -np.inf], np.nan)
    beta = df["beta_shifted"]
    df["residual_24h"] = df["ret_24h"] - beta * df["btc_ret_24h"]

    # Beta 训练收益区间：[t-S-B+1, t-S]。
    # 完整包含于训练区间的 S 小时窗口端点：[t-B, t-S]，共 B-S+1 个。
    N = B - S + 1
    a = df["ret_24h"].shift(S)
    m = df["btc_ret_24h"].shift(S)
    ar, mr = a.rolling(N, min_periods=N), m.rolling(N, min_periods=N)
    df["hist_res_mean"] = ar.mean() - beta * mr.mean()
    res_var = ar.var() + beta.pow(2) * mr.var() - 2 * beta * ar.cov(m)
    df["hist_res_std"] = np.sqrt(res_var.clip(lower=0))
    df["eligible"] = eligible_pool(df_alt, df_btc)
    std = df["hist_res_std"].where(df["hist_res_std"] > Config.MIN_RESIDUAL_STD)
    df["z_score"] = ((df["residual_24h"] - df["hist_res_mean"]) / std).where(df["eligible"])
    df["z_score"] = df["z_score"].replace([np.inf, -np.inf], np.nan)
    df["turnover"] = df["volume"] * df["close"]
    df["avg_turnover_30d"] = historical_turnover(df)
    return calculate_short_confirmation(df)


def calculate_short_confirmation(df):
    """短期 Beta 训练在确认窗口之前结束，窗口内各 bar 使用同一个 Beta。

    e_i(t) = alt_ret_i - short_beta_t * btc_ret_i，不减历史均值。
    做多要求正残差均值，做空要求负残差均值，幅度使用严格不等式。
    UP/DOWN 是 BTC 涨跌小时子样本；BOTH 表示两个子样本均须通过。
    BTC 零收益 bar 只进入 NET；UP/DOWN 样本不足不得自动通过。
    """
    B, C = Config.SHORT_BETA_WINDOW_HOURS, Config.SHORT_SIGNAL_WINDOW_HOURS
    a, m = df["ret_1h"], df["btc_ret_1h"]
    cov = a.rolling(B, min_periods=B).cov(m)
    var = m.rolling(B, min_periods=B).var()
    df["short_beta_shifted"] = (cov / var.where(
        var > Config.MIN_BTC_VARIANCE)).shift(C).replace([np.inf, -np.inf], np.nan)
    beta = df["short_beta_shifted"].to_numpy()
    n = len(df)
    groups = ("net", "up", "down")
    counts = {g: np.zeros(n, dtype=np.int64) for g in groups}
    sums = {g: np.zeros(n, dtype=float) for g in groups}
    negatives = {g: np.zeros(n, dtype=np.int64) for g in groups}
    positives = {g: np.zeros(n, dtype=np.int64) for g in groups}
    for lag in range(C):
        ar, mr = a.shift(lag).to_numpy(), m.shift(lag).to_numpy()
        residual = ar - beta * mr
        valid = np.isfinite(residual)
        masks = {"net": valid, "up": valid & (mr > 0), "down": valid & (mr < 0)}
        for group in groups:
            mask = masks[group]
            counts[group] += mask
            sums[group] += np.where(mask, residual, 0.0)
            negatives[group] += mask & (residual < 0)
            positives[group] += mask & (residual > 0)

    complete = counts["net"] == C
    threshold, ratio = Config.SHORT_EXCESS_THRESHOLD, Config.SHORT_MIN_BAR_RATIO
    short_ok, long_ok = {}, {}
    for group in groups:
        prefix = "short" if group == "net" else f"short_{group}"
        count = counts[group]
        mean = np.divide(sums[group], count, out=np.full(n, np.nan), where=count > 0)
        negative_ratio = np.divide(negatives[group], count,
                                   out=np.full(n, np.nan), where=count > 0)
        positive_ratio = np.divide(positives[group], count,
                                   out=np.full(n, np.nan), where=count > 0)
        df[f"{prefix}_mean_excess"] = np.where(complete, mean, np.nan)
        df[f"{prefix}_negative_ratio"] = np.where(complete, negative_ratio, np.nan)
        df[f"{prefix}_positive_ratio"] = np.where(complete, positive_ratio, np.nan)
        if group != "net":
            df[f"{prefix}_count"] = np.where(complete, count, np.nan)
        required = C if group == "net" else Config.SHORT_MIN_REGIME_BARS
        enough = complete & (count >= required)
        short_ok[group] = enough & (mean < -threshold) & (negative_ratio >= ratio)
        long_ok[group] = enough & (mean > threshold) & (positive_ratio >= ratio)

    def combine(signals):
        mode = Config.SHORT_CONFIRM_MODE
        if mode == "NET":
            return signals["net"]
        if mode == "UP":
            return signals["up"]
        if mode == "DOWN":
            return signals["down"]
        if mode == "BOTH":
            return signals["up"] & signals["down"]
        if mode == "EITHER":
            return signals["up"] | signals["down"]
        raise ValueError(f"未知短期确认方式: {mode}")

    df["short_ready_short"] = combine(short_ok)
    df["short_ready_long"] = combine(long_ok)
    return df


# ==========================================
# 3. 市场分组与双腿估值
# ==========================================
def generate_market_median(symbols, btc_df=None):
    """只在当时历史完整的 ALT 币池中计算成交额中位数。"""
    if not Config.MARKET_ID:
        prepare_run(symbols)
    cache_path = os.path.join(os.path.abspath(Config.BASE_OUTPUT_DIR),
                              f"market_median_{Config.MARKET_ID}.csv")
    if os.path.isfile(cache_path):
        cached = pd.read_csv(cache_path)
        cached["close_time"] = pd.to_datetime(cached["close_time"], utc=True)
        return cached.set_index("close_time")["median_turnover"]
    print("预计算全市场动态截面成交额中位数...")
    if btc_df is None:
        btc_df = load_kline(Config.BTC_SYMBOL)
    turnovers = []
    for symbol in tqdm(sorted(set(symbols))):
        if symbol == Config.BTC_SYMBOL:
            continue
        alt, btc = align_hourly(load_kline(symbol), btc_df)
        turnover = historical_turnover(alt).where(eligible_pool(alt, btc))
        turnovers.append(turnover.rename(symbol))
    series = (pd.concat(turnovers, axis=1).median(axis=1) if turnovers else
              pd.Series(index=pd.DatetimeIndex([], tz="UTC", name="close_time"), dtype=float))
    series.name = "median_turnover"
    atomic_csv(series, cache_path, reuse_existing=True, header=True, index_label="close_time")
    return series


def valid_pair_prices(alt_price, btc_price):
    return (np.isfinite(alt_price) and alt_price > 0
            and np.isfinite(btc_price) and btc_price > 0)


def pair_valuation(position, alt_price, btc_price):
    """固定数量和冻结费率公式，同时用于逐小时盯市与最终结算。"""
    q_alt, q_btc = position["alt_qty"], position["btc_qty"]
    alt_pnl = q_alt * (alt_price - position["entry_price"])
    btc_pnl = q_btc * (btc_price - position["btc_entry"])
    alt_exit, btc_exit = abs(q_alt) * alt_price, abs(q_btc) * btc_price
    exit_cost = (alt_exit + btc_exit) * position["fee_rate"]
    gross = alt_pnl + btc_pnl
    total_cost = position["entry_cost"] + exit_cost
    denominator = position["entry_gross_notional"]
    return dict(alt_exit_notional=alt_exit, btc_exit_notional=btc_exit,
                alt_gross_pnl=alt_pnl, btc_gross_pnl=btc_pnl, gross_pnl=gross,
                exit_cost=exit_cost, total_cost=total_cost, net_pnl=gross - total_cost,
                gross_return=gross / denominator,
                net_return=(gross - total_cost) / denominator)


def valid_alt_price(alt_price):
    return np.isfinite(alt_price) and alt_price > 0


def alt_only_valuation(position, alt_price):
    """ALT_ONLY 反事实：只持有 ALT，BTC 数量恒为 0。"""
    q_alt = position["alt_only_qty"]
    gross = q_alt * (alt_price - position["entry_price"])
    exit_notional = abs(q_alt) * alt_price
    exit_cost = exit_notional * position["fee_rate"]
    total_cost = position["alt_only_entry_cost"] + exit_cost
    denominator = position["alt_only_entry_notional"]
    return dict(alt_only_exit_notional=exit_notional, alt_only_gross_pnl=gross,
                alt_only_exit_cost=exit_cost, alt_only_total_cost=total_cost,
                alt_only_net_pnl=gross - total_cost,
                alt_only_gross_return=gross / denominator,
                alt_only_net_return=(gross - total_cost) / denominator)


def invalidate_alt_only_excursions(position):
    position.update(alt_only_mae_return=np.nan, alt_only_mfe_return=np.nan,
                    alt_only_mae_time=pd.NaT, alt_only_mfe_time=pd.NaT,
                    _alt_only_excursion_valid=False)


def update_alt_only_excursions(position, now, alt_price):
    """ALT_ONLY 按小时收盘更新净清算收益；只要求 ALT 价格路径连续。"""
    if not position["_alt_only_excursion_valid"]:
        return
    previous = position["_alt_only_last_mark_time"]
    if (not valid_alt_price(alt_price)
            or (previous is not None
                and now - previous not in (pd.Timedelta(0), pd.Timedelta(hours=1)))):
        invalidate_alt_only_excursions(position)
        return
    current_return = alt_only_valuation(position, alt_price)["alt_only_net_return"]
    if not np.isfinite(current_return):
        invalidate_alt_only_excursions(position)
        return
    position["_alt_only_last_mark_time"] = now
    if current_return < position["alt_only_mae_return"]:
        position.update(alt_only_mae_return=current_return, alt_only_mae_time=now)
    if current_return > position["alt_only_mfe_return"]:
        position.update(alt_only_mfe_return=current_return, alt_only_mfe_time=now)


def invalidate_excursions(position):
    position.update(mae_return=np.nan, mfe_return=np.nan,
                    mae_time=pd.NaT, mfe_time=pd.NaT, _excursion_valid=False)


def update_excursions(position, now, alt_price, btc_price):
    """逐小时更新净清算收益；并列极值保留首次时间。"""
    if not position["_excursion_valid"]:
        return
    previous = position["_last_mark_time"]
    if (not valid_pair_prices(alt_price, btc_price)
            or (previous is not None
                and now - previous not in (pd.Timedelta(0), pd.Timedelta(hours=1)))):
        invalidate_excursions(position)
        return
    current_return = pair_valuation(position, alt_price, btc_price)["net_return"]
    if not np.isfinite(current_return):
        invalidate_excursions(position)
        return
    position["_last_mark_time"] = now
    if current_return < position["mae_return"]:
        position.update(mae_return=current_return, mae_time=now)
    if current_return > position["mfe_return"]:
        position.update(mfe_return=current_return, mfe_time=now)


def open_pair_position(symbol, row, median, scheduled_exit_time=pd.NaT,
                       planned_holding_hours=np.nan):
    """方向由任务指定，长期 Beta 可正、负或零；开仓后数量固定。"""
    now, z, beta = row.Index, row.z_score, row.beta_shifted
    if Config.TRADE_DIRECTION not in VALID_DIRECTIONS:
        raise ValueError("未设置有效交易方向")
    direction = 1 if Config.TRADE_DIRECTION == "LONG_ALT" else -1
    signal_matches = (z < -Config.Z_SCORE_THRESHOLD if direction == 1
                      else z > Config.Z_SCORE_THRESHOLD)
    if not signal_matches or not np.isfinite(beta):
        raise ValueError("开仓信号与指定ALT方向不一致，或Beta无效")
    if not valid_pair_prices(row.close, row.btc_close):
        raise ValueError("开仓双腿价格必须为有限正数")
    alt_notional = Config.PAIR_GROSS_NOTIONAL / (1 + abs(beta))
    btc_signed_notional = -direction * beta * alt_notional
    position = dict(
        run_id=Config.RUN_ID, search_direction=Config.TRADE_DIRECTION,
        trade_id=f"{Config.RUN_ID[:16]}_{symbol}_{Config.TRADE_DIRECTION}_{now.isoformat()}",
        symbol=symbol, status="OPEN", entry_time=now, scheduled_exit_time=scheduled_exit_time,
        direction=Config.TRADE_DIRECTION,
        btc_direction=("LONG_BTC" if btc_signed_notional > 0 else
                       "SHORT_BTC" if btc_signed_notional < 0 else "FLAT"),
        entry_price=row.close, btc_entry=row.btc_close, beta=beta, z_score=z,
        hist_res_mean=row.hist_res_mean, hist_res_std=row.hist_res_std,
        avg_turnover_30d=row.avg_turnover_30d, market_median_at_entry=median,
        vol_group="High_Vol" if row.avg_turnover_30d >= median else "Low_Vol",
        alt_qty=direction * alt_notional / row.close,
        btc_qty=btc_signed_notional / row.btc_close,
        alt_entry_notional=alt_notional, btc_entry_notional=abs(btc_signed_notional),
        entry_gross_notional=Config.PAIR_GROSS_NOTIONAL,
        entry_cost=Config.PAIR_GROSS_NOTIONAL * Config.FEE_RATE,
        planned_holding_hours=planned_holding_hours, fee_rate=Config.FEE_RATE,
        z_threshold=Config.Z_SCORE_THRESHOLD, beta_window_hours=Config.BETA_WINDOW_HOURS,
        signal_window_hours=Config.SIGNAL_WINDOW_HOURS,
        short_beta_window_hours=Config.SHORT_BETA_WINDOW_HOURS,
        short_signal_window_hours=Config.SHORT_SIGNAL_WINDOW_HOURS,
        short_excess_threshold=Config.SHORT_EXCESS_THRESHOLD,
        short_min_bar_ratio=Config.SHORT_MIN_BAR_RATIO,
        short_min_regime_bars=Config.SHORT_MIN_REGIME_BARS,
        short_confirm_mode=Config.SHORT_CONFIRM_MODE,
        short_confirm_timing=Config.SHORT_CONFIRM_TIMING,
        short_beta=row.short_beta_shifted,
        short_mean_excess=row.short_mean_excess,
        short_up_mean_excess=row.short_up_mean_excess,
        short_down_mean_excess=row.short_down_mean_excess,
        short_support_ratio=(row.short_negative_ratio if direction == -1
                             else row.short_positive_ratio),
        short_up_support_ratio=(row.short_up_negative_ratio if direction == -1
                                else row.short_up_positive_ratio),
        short_down_support_ratio=(row.short_down_negative_ratio if direction == -1
                                  else row.short_down_positive_ratio),
        short_up_count=row.short_up_count, short_down_count=row.short_down_count,
        exit_reason=None, mae_return=0.0, mfe_return=0.0, mae_time=pd.NaT, mfe_time=pd.NaT,
        alt_only_status="OPEN", alt_only_exit_time=pd.NaT, alt_only_exit_price=np.nan,
        alt_only_holding_hours=np.nan, alt_only_exit_reason=None,
        alt_only_qty=direction * Config.PAIR_GROSS_NOTIONAL / row.close,
        alt_only_btc_qty=0.0, alt_only_entry_notional=Config.PAIR_GROSS_NOTIONAL,
        alt_only_entry_cost=Config.PAIR_GROSS_NOTIONAL * Config.FEE_RATE,
        alt_only_mae_return=0.0, alt_only_mfe_return=0.0,
        alt_only_mae_time=pd.NaT, alt_only_mfe_time=pd.NaT,
        _excursion_valid=True, _last_mark_time=None,
        _alt_only_excursion_valid=True, _alt_only_last_mark_time=None,
    )
    update_excursions(position, now, row.close, row.btc_close)
    update_alt_only_excursions(position, now, row.close)
    return position


def close_pair_position(position, now, alt_price, btc_price, z, reason):
    if not valid_pair_prices(alt_price, btc_price):
        invalidate_excursions(position)
        position.update(status="UNRESOLVED_MISSING_EXIT", exit_reason=reason)
        return False
    update_excursions(position, now, alt_price, btc_price)
    position.update(pair_valuation(position, alt_price, btc_price))
    position.update(status="CLOSED", exit_time=now, exit_price=alt_price,
                    btc_exit=btc_price, exit_z_score=z,
                    holding_hours=(now - position["entry_time"]).total_seconds() / 3600,
                    exit_reason=reason)
    return True


def close_alt_only_position(position, now, alt_price, reason):
    if not valid_alt_price(alt_price):
        invalidate_alt_only_excursions(position)
        position.update(alt_only_status="UNRESOLVED_MISSING_EXIT",
                        alt_only_exit_reason="MISSING_SCHEDULED_EXIT")
        return False
    update_alt_only_excursions(position, now, alt_price)
    position.update(alt_only_valuation(position, alt_price))
    position.update(alt_only_status="CLOSED", alt_only_exit_time=now,
                    alt_only_exit_price=alt_price,
                    alt_only_holding_hours=(now - position["entry_time"]).total_seconds() / 3600,
                    alt_only_exit_reason=reason)
    return True


# ==========================================
# 4. 单方向入场状态机
# ==========================================
def backtest_single_symbol(symbol, df, market_median_series):
    """每次只交易指定方向；反向极值永不产生仓位。

    沿用原规则：先见到正常区间才允许首次突破；突破后仍须在同侧阈值外。
    返回正常区间、无效指标或进入反向极值都会清除等待。
    平仓当根不重新开仓；平仓后的重新武装仍要求正常区间。
    """
    if Config.TRADE_DIRECTION not in VALID_DIRECTIONS:
        raise ValueError("回测方向必须为LONG_ALT或SHORT_ALT")
    trades = []
    position = None
    armed = False
    pending = False
    trigger_time, trigger_z = None, np.nan
    threshold = Config.Z_SCORE_THRESHOLD
    is_long = Config.TRADE_DIRECTION == "LONG_ALT"
    begin, end = utc_timestamp(Config.ENTRY_START), utc_timestamp(Config.EVALUATION_END)
    hold = pd.Timedelta(hours=Config.HOLDING_PERIOD_HOURS)
    confirmation_wait = pd.Timedelta(hours=Config.SHORT_SIGNAL_WINDOW_HOURS)

    for row in df.itertuples():
        now, z = row.Index, row.z_score
        valid_z = np.isfinite(z) and np.isfinite(row.beta_shifted)
        normal = valid_z and -threshold <= z <= threshold

        # 退出和盯市优先于指标有效性判断。
        if position is not None:
            if now <= position["scheduled_exit_time"]:
                update_excursions(position, now, row.close, row.btc_close)
                update_alt_only_excursions(position, now, row.close)
            if now < position["scheduled_exit_time"]:
                continue
            if now != position["scheduled_exit_time"]:
                invalidate_excursions(position)
                invalidate_alt_only_excursions(position)
                position.update(status="UNRESOLVED_MISSING_EXIT",
                                exit_reason="MISSING_SCHEDULED_EXIT",
                                alt_only_status="UNRESOLVED_MISSING_EXIT",
                                alt_only_exit_reason="MISSING_SCHEDULED_EXIT")
                trades.append(position)
                position = None
                break  # 无法按计划结算，停止本币后续交易，不私自延期。
            close_alt_only_position(position, now, row.close, "FIXED_HOLD")
            if not valid_pair_prices(row.close, row.btc_close):
                invalidate_excursions(position)
                position.update(status="UNRESOLVED_MISSING_EXIT",
                                exit_reason="MISSING_SCHEDULED_EXIT")
                trades.append(position)
                position = None
                break  # 无法按计划结算，停止本币后续交易，不私自延期。
            close_pair_position(position, now, row.close, row.btc_close, z, "FIXED_HOLD")
            trades.append(position)
            position = None
            armed = bool(normal)
            pending = False
            trigger_time, trigger_z = None, np.nan
            continue

        if not valid_z:
            armed = pending = False
            trigger_time, trigger_z = None, np.nan
            continue
        if normal:
            armed = True
            pending = False
            trigger_time, trigger_z = None, np.nan
            continue

        target_extreme = z < -threshold if is_long else z > threshold
        if not target_extreme:
            # 反向极值不建仓，且不能将跨零跳变冒充从正常区间产生的新突破。
            armed = pending = False
            trigger_time, trigger_z = None, np.nan
            continue
        if not pending:
            if not armed:
                continue
            armed = False
            pending = True
            trigger_time, trigger_z = now, z

        # t0 突破，C 小时确认：最早 t0+C 入场，收益 bar 端点为 t0+1...t0+C。
        # 短期 Beta 仍在各确认窗口开始之前估计，并非冻结在首次突破时点。
        if (Config.SHORT_CONFIRM_TIMING == "POST_TRIGGER"
                and now - trigger_time < confirmation_wait):
            continue
        ready = row.short_ready_long if is_long else row.short_ready_short
        if pd.isna(ready) or not bool(ready):
            continue

        pending = False  # 第一次双条件满足即消耗信号，区间/分组失败时不追单。
        if begin is not None and now < begin:
            continue
        if end is not None and (now >= end or now + hold > end):
            continue
        median = market_median_series.get(now, np.nan)
        if not np.isfinite(median) or not np.isfinite(row.avg_turnover_30d):
            continue
        if not valid_pair_prices(row.close, row.btc_close):
            continue
        position = open_pair_position(symbol, row, median, now + hold,
                                      Config.HOLDING_PERIOD_HOURS)
        # long_trigger_* 是“长期信号触发”，不表示 LONG_ALT。
        position.update(long_trigger_time=trigger_time, long_trigger_z=trigger_z)

    if position is not None:
        invalidate_excursions(position)
        invalidate_alt_only_excursions(position)
        position.update(status="UNRESOLVED_END_OF_DATA", exit_reason="END_OF_DATA",
                        alt_only_status="UNRESOLVED_END_OF_DATA",
                        alt_only_exit_reason="END_OF_DATA")
        trades.append(position)
    return pd.DataFrame(trades, columns=TRADE_COLUMNS)


def process_symbol(symbol, btc_df, market_median_series):
    """成功的零交易也保存表头；原子写入，异常不伪造成功缓存。"""
    output_csv = result_path(f"{symbol}_trades.csv")
    if os.path.isfile(output_csv):
        columns = pd.read_csv(output_csv, nrows=0).columns.tolist()
        if columns != TRADE_COLUMNS:
            raise ValueError(f"缓存字段不匹配，请检查后删除对应缓存: {output_csv}")
        return
    indicators = calculate_indicators(load_kline(symbol), btc_df)
    records = backtest_single_symbol(symbol, indicators, market_median_series)
    atomic_csv(records, output_csv, index=False)


def save_evaluation_window(market_median_series):
    valid = market_median_series.dropna()
    start = valid.index.min() if not valid.empty else None
    end = market_median_series.index.max() if not market_median_series.empty else None
    requested_start = utc_timestamp(Config.ENTRY_START)
    if start is not None and requested_start is not None:
        start = max(start, requested_start)
    if start is not None and end is not None and start > end:
        start = end
    atomic_json(dict(run_id=Config.RUN_ID, search_direction=Config.TRADE_DIRECTION,
                     timezone="UTC", start=start.isoformat() if start is not None else None,
                     end=end.isoformat() if end is not None else None,
                     basis="first_eligible_market_hour_to_last_observed_hour"),
                result_path("evaluation_window.json"))


def run_all_backtests(symbols, data_fingerprints=None, prepared=False):
    symbols = sorted(set(symbols) - {Config.BTC_SYMBOL})
    if not symbols:
        raise ValueError("没有可回测的ALT币种")
    if not prepared:
        manifest = prepare_run(symbols, data_fingerprints)
        atomic_json(manifest, result_path("run_manifest.json"))
    print("加载 BTC 基准数据...")
    btc_df = load_kline(Config.BTC_SYMBOL)
    median = generate_market_median(symbols, btc_df)
    save_evaluation_window(median)
    print(f"开始 {Config.TRADE_DIRECTION} 回测，共 {len(symbols)} 个ALT；输出: {Config.OUTPUT_DIR}")
    errors = []
    for symbol in tqdm(symbols):
        try:
            process_symbol(symbol, btc_df, median)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
    if errors:
        raise RuntimeError("部分币种失败，禁止把残缺样本汇总为完整结果:\n" + "\n".join(errors))


# ==========================================
# 5. 单组统计：零交易也保存摘要，不混入反方向交易
# ==========================================
TASK_FIELDS = (
    "search_direction", "z_threshold", "holding_period_hours",
    "beta_window_days", "signal_window_hours",
    "short_beta_window_days", "short_signal_window_hours",
    "short_excess_threshold", "short_min_bar_ratio",
    "short_confirm_mode", "short_confirm_timing",
)


def task_metadata(params):
    return dict(zip(TASK_FIELDS, params))


def current_task_params():
    return (Config.TRADE_DIRECTION, Config.Z_SCORE_THRESHOLD, Config.HOLDING_PERIOD_HOURS,
            Config.BETA_WINDOW_DAYS, Config.SIGNAL_WINDOW_HOURS,
            Config.SHORT_BETA_WINDOW_DAYS, Config.SHORT_SIGNAL_WINDOW_HOURS,
            Config.SHORT_EXCESS_THRESHOLD, Config.SHORT_MIN_BAR_RATIO,
            Config.SHORT_CONFIRM_MODE, Config.SHORT_CONFIRM_TIMING)


def optional_float(value):
    return float(value) if pd.notna(value) and np.isfinite(value) else None


def analyze_results(symbols=None):
    """返回可供跨参数比较的摘要；收益统计只基于已平仓的独立交易。"""
    if symbols is None:
        all_files = sorted(glob.glob(result_path("*_trades.csv")))
    else:
        all_files = [result_path(f"{s}_trades.csv")
                     for s in sorted(set(symbols) - {Config.BTC_SYMBOL})]
    if not all_files:
        raise RuntimeError("未找到任何币种结果文件，不能标记整组完成")
    frames = []
    for path in all_files:
        frame = pd.read_csv(path)
        if frame.columns.tolist() != TRADE_COLUMNS:
            raise ValueError(f"交易字段不匹配: {path}")
        if not frame.empty:
            frames.append(frame)
    all_trades = (pd.concat(frames, ignore_index=True) if frames
                  else pd.DataFrame(columns=TRADE_COLUMNS))
    if not all_trades.empty:
        if (not all_trades["run_id"].eq(Config.RUN_ID).all()
                or not all_trades["search_direction"].eq(Config.TRADE_DIRECTION).all()
                or not all_trades["direction"].eq(Config.TRADE_DIRECTION).all()):
            raise ValueError("交易文件混入其他运行指纹或方向，拒绝汇总")
        if all_trades["trade_id"].duplicated().any():
            raise ValueError("交易ID重复，拒绝重复计数")
        if not all_trades["status"].isin(
                ["CLOSED", "UNRESOLVED_MISSING_EXIT", "UNRESOLVED_END_OF_DATA"]).all():
            raise ValueError("交易文件包含未知或尚未终结的状态")
        if not all_trades["alt_only_status"].isin(
                ["CLOSED", "UNRESOLVED_MISSING_EXIT", "UNRESOLVED_END_OF_DATA"]).all():
            raise ValueError("ALT_ONLY交易包含未知或尚未终结的状态")
    for col in ("net_pnl", "net_return", "total_cost", "entry_cost", "mae_return", "mfe_return",
                "alt_only_net_pnl", "alt_only_net_return", "alt_only_total_cost",
                "alt_only_entry_cost", "alt_only_mae_return", "alt_only_mfe_return"):
        all_trades[col] = pd.to_numeric(all_trades[col], errors="raise")
    closed = all_trades.loc[all_trades["status"] == "CLOSED"].copy()
    unresolved = all_trades.loc[all_trades["status"] != "CLOSED"].copy()
    alt_only_closed = all_trades.loc[all_trades["alt_only_status"] == "CLOSED"].copy()
    alt_only_unresolved = all_trades.loc[all_trades["alt_only_status"] != "CLOSED"].copy()
    if not closed.empty and not np.isfinite(
            closed[["net_pnl", "net_return", "total_cost"]].to_numpy(dtype=float)).all():
        raise ValueError("已平仓交易缺少有限的收益或费用，拒绝汇总")
    if not alt_only_closed.empty and not np.isfinite(
            alt_only_closed[["alt_only_net_pnl", "alt_only_net_return",
                             "alt_only_total_cost"]].to_numpy(dtype=float)).all():
        raise ValueError("ALT_ONLY已平仓交易缺少有限的收益或费用，拒绝汇总")

    summary = dict(
        **task_metadata(current_task_params()),
        run_id=Config.RUN_ID, short_min_regime_bars=int(Config.SHORT_MIN_REGIME_BARS),
        fee_rate=float(Config.FEE_RATE), pair_gross_notional=float(Config.PAIR_GROSS_NOTIONAL),
        symbol_count=len(all_files), trade_count=len(all_trades), closed_count=len(closed),
        unresolved_count=len(unresolved), result_complete=unresolved.empty,
        has_closed_trades=not closed.empty,
        unresolved_missing_exit_count=int((unresolved["status"] == "UNRESOLVED_MISSING_EXIT").sum()),
        unresolved_end_of_data_count=int((unresolved["status"] == "UNRESOLVED_END_OF_DATA").sum()),
        unresolved_entry_cost=float(unresolved["entry_cost"].sum()),
        win_rate=optional_float((closed["net_pnl"] > 0).mean()),
        avg_net_return=optional_float(closed["net_return"].mean()),
        median_net_return=optional_float(closed["net_return"].median()),
        avg_net_pnl=optional_float(closed["net_pnl"].mean()),
        sum_net_pnl=optional_float(closed["net_pnl"].sum()) if not closed.empty else None,
        avg_cost=optional_float(closed["total_cost"].mean()),
        excursion_valid_count=int(closed["mae_return"].count()),
        avg_mae_return=optional_float(closed["mae_return"].mean()),
        p10_mae_return=optional_float(closed["mae_return"].quantile(0.10)),
        worst_mae_return=optional_float(closed["mae_return"].min()),
        avg_mfe_return=optional_float(closed["mfe_return"].mean()),
        alt_only_gross_notional=float(Config.PAIR_GROSS_NOTIONAL),
        alt_only_btc_qty=0.0,
        alt_only_closed_count=len(alt_only_closed),
        alt_only_unresolved_count=len(alt_only_unresolved),
        alt_only_result_complete=alt_only_unresolved.empty,
        alt_only_has_closed_trades=not alt_only_closed.empty,
        alt_only_unresolved_missing_exit_count=int(
            (alt_only_unresolved["alt_only_status"] == "UNRESOLVED_MISSING_EXIT").sum()),
        alt_only_unresolved_end_of_data_count=int(
            (alt_only_unresolved["alt_only_status"] == "UNRESOLVED_END_OF_DATA").sum()),
        alt_only_unresolved_entry_cost=float(alt_only_unresolved["alt_only_entry_cost"].sum()),
        alt_only_win_rate=optional_float((alt_only_closed["alt_only_net_pnl"] > 0).mean()),
        alt_only_avg_net_return=optional_float(alt_only_closed["alt_only_net_return"].mean()),
        alt_only_median_net_return=optional_float(alt_only_closed["alt_only_net_return"].median()),
        alt_only_avg_net_pnl=optional_float(alt_only_closed["alt_only_net_pnl"].mean()),
        alt_only_sum_net_pnl=(optional_float(alt_only_closed["alt_only_net_pnl"].sum())
                              if not alt_only_closed.empty else None),
        alt_only_avg_cost=optional_float(alt_only_closed["alt_only_total_cost"].mean()),
        alt_only_excursion_valid_count=int(alt_only_closed["alt_only_mae_return"].count()),
        alt_only_avg_mae_return=optional_float(alt_only_closed["alt_only_mae_return"].mean()),
        alt_only_p10_mae_return=optional_float(alt_only_closed["alt_only_mae_return"].quantile(0.10)),
        alt_only_worst_mae_return=optional_float(alt_only_closed["alt_only_mae_return"].min()),
        alt_only_avg_mfe_return=optional_float(alt_only_closed["alt_only_mfe_return"].mean()),
    )

    group_columns = [
        "vol_group", "direction", "trade_count", "win_rate", "avg_net_return",
        "median_net_return", "avg_net_pnl", "sum_net_pnl", "avg_cost",
        "excursion_valid_count", "avg_mae_return", "p10_mae_return",
        "worst_mae_return", "avg_mfe_return",
    ]
    if closed.empty:
        groups = pd.DataFrame(columns=group_columns)
    else:
        groups = closed.groupby(["vol_group", "direction"]).agg(
            trade_count=("symbol", "count"),
            win_rate=("net_pnl", lambda x: (x > 0).mean()),
            avg_net_return=("net_return", "mean"),
            median_net_return=("net_return", "median"),
            avg_net_pnl=("net_pnl", "mean"), sum_net_pnl=("net_pnl", "sum"),
            avg_cost=("total_cost", "mean"), excursion_valid_count=("mae_return", "count"),
            avg_mae_return=("mae_return", "mean"),
            p10_mae_return=("mae_return", lambda x: x.quantile(0.10)),
            worst_mae_return=("mae_return", "min"), avg_mfe_return=("mfe_return", "mean"),
        ).reset_index()[group_columns]

    alt_only_group_columns = [
        "vol_group", "direction", "alt_only_trade_count", "alt_only_win_rate",
        "alt_only_avg_net_return", "alt_only_median_net_return",
        "alt_only_avg_net_pnl", "alt_only_sum_net_pnl", "alt_only_avg_cost",
        "alt_only_excursion_valid_count", "alt_only_avg_mae_return",
        "alt_only_p10_mae_return", "alt_only_worst_mae_return",
        "alt_only_avg_mfe_return",
    ]
    if alt_only_closed.empty:
        alt_only_groups = pd.DataFrame(columns=alt_only_group_columns)
    else:
        alt_only_groups = alt_only_closed.groupby(["vol_group", "direction"]).agg(
            alt_only_trade_count=("symbol", "count"),
            alt_only_win_rate=("alt_only_net_pnl", lambda x: (x > 0).mean()),
            alt_only_avg_net_return=("alt_only_net_return", "mean"),
            alt_only_median_net_return=("alt_only_net_return", "median"),
            alt_only_avg_net_pnl=("alt_only_net_pnl", "mean"),
            alt_only_sum_net_pnl=("alt_only_net_pnl", "sum"),
            alt_only_avg_cost=("alt_only_total_cost", "mean"),
            alt_only_excursion_valid_count=("alt_only_mae_return", "count"),
            alt_only_avg_mae_return=("alt_only_mae_return", "mean"),
            alt_only_p10_mae_return=("alt_only_mae_return", lambda x: x.quantile(0.10)),
            alt_only_worst_mae_return=("alt_only_mae_return", "min"),
            alt_only_avg_mfe_return=("alt_only_mfe_return", "mean"),
        ).reset_index()[alt_only_group_columns]
    groups = groups.merge(alt_only_groups, on=["vol_group", "direction"], how="outer")
    groups = groups[group_columns + alt_only_group_columns[2:]]
    # 保留原分组汇总文件名；本次只有指定方向的高/低成交额分组。
    atomic_csv(groups, result_path("quadrant_summary.csv"), index=False)
    atomic_json(summary, result_path("run_summary.json"))

    print("\n" + "=" * 60)
    print(f"【参数组合】{Config.PARAM_FOLDER}")
    print("【独立配对交易样本；非共享资金账户收益】")
    print(f"交易总数: {len(all_trades)}；已平仓: {len(closed)}；未结算: {len(unresolved)}")
    if not unresolved.empty:
        print("结果不完整：收益统计仅含已平仓子集，不应据此判断该参数组盈利。")
        print(f"未结算交易已发生开仓成本: {summary['unresolved_entry_cost']:.4f} USDT")
    if closed.empty:
        print("没有已平仓交易；摘要已保存，收益指标为空，不填成零收益。")
    else:
        print(f"胜率: {summary['win_rate']:.2%}")
        print(f"单笔平均净收益率: {summary['avg_net_return']:.4%}")
        print(f"单笔净收益率中位数: {summary['median_net_return']:.4%}")
        print(f"独立交易净盈亏合计: {summary['sum_net_pnl']:.4f} USDT")
        print(f"完整极值样本: {summary['excursion_valid_count']}/{len(closed)}")
        if summary["avg_mae_return"] is not None:
            print(f"平均MAE: {summary['avg_mae_return']:.4%}；"
                  f"P10: {summary['p10_mae_return']:.4%}；最差: {summary['worst_mae_return']:.4%}")

    print("\n【ALT_ONLY：同信号/同持有期，仅开ALT，不做BTC对冲】")
    print(f"初始ALT名义: {Config.PAIR_GROSS_NOTIONAL:.4f} USDT；BTC数量: 0")
    print(f"已平仓: {len(alt_only_closed)}；未结算: {len(alt_only_unresolved)}")
    if not alt_only_unresolved.empty:
        print("ALT_ONLY结果不完整：收益统计仅含ALT_ONLY已平仓子集。")
        print(f"ALT_ONLY未结算交易已发生开仓成本: "
              f"{summary['alt_only_unresolved_entry_cost']:.4f} USDT")
    if alt_only_closed.empty:
        print("ALT_ONLY没有已平仓交易；相关收益指标为空，不填成零收益。")
    else:
        print(f"ALT_ONLY胜率: {summary['alt_only_win_rate']:.2%}")
        print(f"ALT_ONLY单笔平均净收益率: {summary['alt_only_avg_net_return']:.4%}")
        print(f"ALT_ONLY单笔净收益率中位数: {summary['alt_only_median_net_return']:.4%}")
        print(f"ALT_ONLY独立交易净盈亏合计: {summary['alt_only_sum_net_pnl']:.4f} USDT")
        print(f"ALT_ONLY完整极值样本: "
              f"{summary['alt_only_excursion_valid_count']}/{len(alt_only_closed)}")
        if summary["alt_only_avg_mae_return"] is not None:
            print(f"ALT_ONLY平均MAE: {summary['alt_only_avg_mae_return']:.4%}；"
                  f"P10: {summary['alt_only_p10_mae_return']:.4%}；"
                  f"最差: {summary['alt_only_worst_mae_return']:.4%}")

    print("\n【成交额分组 × ALT方向；收益率列为小数】")
    print(groups.to_string(index=False))
    print("未计算组合年化、夏普和最大回撤：需要共享资金分配及逐小时盯市账本。")
    return summary


# ==========================================
# 6. 网格调度、缓存与跨参数汇总
# ==========================================
@contextmanager
def parameter_run_lock():
    """不同参数互不等待；禁止两个脚本同时写相同运行结果。"""
    lock_path = result_path(".run.lock")
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(
            f"同一组正在运行，或上次异常中断留下锁: {lock_path}；"
            "仅在确认对应任务已经结束后，才可手动删除此锁") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"pid": os.getpid(), "run_id": Config.RUN_ID}, f)
        yield
    finally:
        os.remove(lock_path)


def build_param_grid(directions=None):
    """分别构造两个空间的笛卡尔积，然后拼接；绝不把两个空间混合交叉。

    每组元组结构：(direction, z, hold, B_days, S_hours, b_days, s_hours,
                   excess_threshold, min_bar_ratio, confirm_mode, confirm_timing)。
    只去重，不主观删掉可能零交易或时间尺度特殊的合法组合。
    """
    if directions is None:
        directions = Config.SEARCH_DIRECTIONS
    if isinstance(directions, str):
        directions = (directions,)
    directions = tuple(dict.fromkeys(directions))
    if not directions or any(side not in VALID_DIRECTIONS for side in directions):
        raise ValueError("搜索方向只能包含LONG_ALT和/或SHORT_ALT")
    spaces = {"LONG_ALT": Config.LONG_PARAM_SPACE, "SHORT_ALT": Config.SHORT_PARAM_SPACE}
    grid, seen = [], set()
    for direction in directions:
        space = spaces[direction]
        if set(space) != set(GRID_KEYS):
            raise ValueError(f"{direction}搜索空间字段缺失或多余，必须对应GRID_KEYS")
        axes = [space[name] for name in GRID_KEYS]
        if any(not isinstance(axis, (list, tuple)) or not axis for axis in axes):
            raise ValueError(f"{direction}搜索空间的每一维必须为非空列表或元组")
        for values in itertools.product(*axes):
            task = (direction,) + values
            if task not in seen:
                grid.append(task)
                seen.add(task)
    return grid


def validate_grid_limits(param_grid):
    if not param_grid:
        raise ValueError("参数网格为空")
    for name in ("MAX_WORKERS", "NUMERIC_THREADS_PER_WORKER", "MAX_GRID_COMBINATIONS"):
        require_integer(name, getattr(Config, name))
    if len(param_grid) > Config.MAX_GRID_COMBINATIONS:
        raise ValueError(
            f"参数网格共{len(param_grid)}组，超过上限{Config.MAX_GRID_COMBINATIONS}；"
            "请调整上限或搜索列表。程序不会截取网格。")
    for params in param_grid:
        if len(params) != len(TASK_FIELDS) or params[0] not in VALID_DIRECTIONS:
            raise ValueError(f"无效参数元组（首元素必须是方向）: {params}")


def read_completed_summary():
    """.done 表示计算完成；是否全部结算由 result_complete 另行表示。"""
    marker = result_path(".done")
    if not os.path.isfile(marker):
        return None
    with open(marker, "r", encoding="utf-8") as f:
        done = json.load(f)
    if done.get("run_id") != Config.RUN_ID or done.get("status") != "success":
        raise ValueError(f"完成标记与本次指纹不符: {marker}")
    with open(result_path("run_summary.json"), "r", encoding="utf-8") as f:
        summary = json.load(f)
    if (summary.get("run_id") != Config.RUN_ID
            or summary.get("search_direction") != Config.TRADE_DIRECTION):
        raise ValueError("完成摘要与本次运行指纹/方向不一致")
    return summary


def run_parameter_combination(params, symbols, data_fingerprints, config_snapshot):
    """顶层函数兼容 Windows spawn；每个进程同一时刻只跑一组参数。"""
    log_path = None
    try:
        # spawn 不继承父进程运行时修改的类属性，须显式恢复配置。
        for key, value in config_snapshot.items():
            setattr(Config, key, value)
        Config.update_params(*params)
        manifest = prepare_run(symbols, data_fingerprints)

        def completed_result(status, summary):
            return dict(summary, status=status, params=Config.PARAM_FOLDER,
                        output_dir=Config.OUTPUT_DIR,
                        log=result_path("run.log"), pid=os.getpid())

        summary = read_completed_summary()
        if summary is not None:
            return completed_result("SKIPPED", summary)
        with parameter_run_lock():
            # 获取锁前其他进程可能刚好完成，锁内再检查一次。
            summary = read_completed_summary()
            if summary is not None:
                return completed_result("SKIPPED", summary)
            atomic_json(manifest, result_path("run_manifest.json"))
            log_path = result_path("run.log")
            with open(log_path, "w", encoding="utf-8") as log:
                with redirect_stdout(log), redirect_stderr(log):
                    try:
                        print(f"参数: {Config.PARAM_FOLDER} | PID: {os.getpid()}")
                        run_all_backtests(symbols, data_fingerprints, prepared=True)
                        summary = analyze_results(symbols)
                    except Exception:
                        traceback.print_exc()
                        raise
            atomic_json(dict(status="success", run_id=Config.RUN_ID), result_path(".done"))
        return completed_result("OK", summary)
    except Exception as exc:
        return dict(task_metadata(params), status="FAILED", params=str(params),
                    error=str(exc), log=log_path, pid=os.getpid())


def save_grid_summaries(results, param_grid, summary_dir):
    """按方向和参数排序保存；不把最高样本内均值直接宣布为最优策略。"""
    frame = pd.DataFrame(results)
    sort_keys = [key for key in TASK_FIELDS if key in frame.columns]
    if sort_keys:
        frame = frame.sort_values(sort_keys, kind="stable").reset_index(drop=True)
    atomic_csv(frame, os.path.join(summary_dir, "grid_summary.csv"),
               index=False, encoding="utf-8-sig")
    for direction in dict.fromkeys(params[0] for params in param_grid):
        part = frame.loc[frame["search_direction"] == direction]
        atomic_csv(part, os.path.join(summary_dir, f"{direction}_grid_summary.csv"),
                   index=False, encoding="utf-8-sig")
    print(f"\n跨参数汇总已保存: {summary_dir}")
    print("比较时请检查 status、result_complete、closed_count 和 excursion_valid_count。")
    print("收益列均为已平仓交易样本统计，收益率为小数，不是组合收益率。")


def run_parameter_grid(symbols, data_fingerprints, param_grid=None):
    """参数任务共享进程池；每个任务固定一个方向并拥有独立的持仓状态。"""
    param_grid = build_param_grid() if param_grid is None else list(dict.fromkeys(param_grid))
    validate_grid_limits(param_grid)
    symbols = sorted(set(symbols) - {Config.BTC_SYMBOL})
    if not symbols:
        raise ValueError("没有可回测的ALT币种")
    workers = min(int(Config.MAX_WORKERS), len(param_grid))
    if os.name == "nt":
        workers = min(workers, 61)
    config_snapshot = {key: getattr(Config, key) for key in vars(Config) if key.isupper()}
    counts = Counter(params[0] for params in param_grid)
    print(f"\n规划 {len(param_grid)} 组；分方向: {dict(counts)}；并行进程数: {workers}")
    print("各组明细、分组摘要和日志分别写入LONG_ALT / SHORT_ALT目录。")

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%fZ")
    summary_dir = os.path.join(os.path.abspath(Config.BASE_OUTPUT_DIR),
                               f"grid_search_{stamp}_{os.getpid()}")
    os.makedirs(summary_dir, exist_ok=True)
    atomic_json(dict(created_at_utc=stamp, task_count=len(param_grid),
                     counts_by_direction=dict(counts), symbols=symbols, tasks=param_grid),
                os.path.join(summary_dir, "grid_manifest.json"))

    thread_keys = ("OMP_NUM_THREADS", "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS",
                   "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS", "BLIS_NUM_THREADS")
    previous_env = {key: os.environ.get(key) for key in thread_keys}
    results, errors = [], []
    try:
        for key in thread_keys:
            os.environ[key] = str(int(Config.NUMERIC_THREADS_PER_WORKER))
        with ProcessPoolExecutor(max_workers=workers,
                                 mp_context=multiprocessing.get_context("spawn")) as pool:
            futures = {
                pool.submit(run_parameter_combination, params, symbols,
                            data_fingerprints, config_snapshot): params
                for params in param_grid
            }
            for done_count, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result()
                except Exception as exc:
                    params = futures[future]
                    result = dict(task_metadata(params), status="FAILED", params=str(params),
                                  error=str(exc), log=None)
                results.append(result)
                if result["status"] == "OK":
                    print(f"[OK {done_count}/{len(param_grid)}] {result['params']}")
                elif result["status"] == "SKIPPED":
                    print(f"[SKIP {done_count}/{len(param_grid)}] {result['params']}（已完成）")
                else:
                    errors.append(result)
                    print(f"[FAILED {done_count}/{len(param_grid)}] {result['params']}: {result['error']}")
                    if result.get("log"):
                        print(f"  日志: {result['log']}")
    finally:
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        # 中断前已经返回的任务也形成部分汇总；各组 .done 可在重启后继续使用。
        if results:
            save_grid_summaries(results, param_grid, summary_dir)
    if errors:
        raise RuntimeError(f"{len(errors)}/{len(param_grid)}组失败；成功组合和失败清单已保留。")
    return results


# ==========================================
# 7. 启动入口
# ==========================================
def main():
    # 在读取大文件和计算哈希前先检查网格数量。
    grid = build_param_grid()
    validate_grid_limits(grid)
    print(f"本轮搜索组合数: {dict(Counter(params[0] for params in grid))}；合计 {len(grid)}")
    with open(Config.SYMBOLS_FILE, "r", encoding="utf-8-sig") as f:
        requested_symbols = json.load(f)
    if (not isinstance(requested_symbols, list)
            or not all(isinstance(s, str) and s.strip() for s in requested_symbols)):
        raise ValueError("symbols.json应为非空币种名称组成的字符串列表")
    if not os.path.isfile(kline_path(Config.BTC_SYMBOL)):
        raise FileNotFoundError(f"找不到核心基准币种 {Config.BTC_SYMBOL} 的数据，停止运行")

    symbols = []
    for symbol in sorted(set(requested_symbols) - {Config.BTC_SYMBOL}):
        if os.path.isfile(kline_path(symbol)):
            symbols.append(symbol)
        else:
            print(f"自动跳过: 未找到 {symbol} 的K线文件")
    if not symbols:
        raise ValueError("过滤缺失数据后，没有任何可回测的ALT币种")
    print(f"计算静态源数据指纹，ALT币种数: {len(symbols)}...")
    data_fingerprints = {
        symbol: file_sha256(kline_path(symbol))
        for symbol in sorted(set(symbols) | {Config.BTC_SYMBOL})
    }
    run_parameter_grid(symbols, data_fingerprints, grid)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    # 不自动删除 .run.lock；须确认旧任务已结束后，才可手动清理异常遗留锁。
    main()
