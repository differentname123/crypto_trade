# -- coding: utf-8 --
""":authors:
    zhuxiaohu, AI Assistant
:create_date:
    2026/9/18
:description:
    基于Beta调整与固定期限的横截面统计套利策略 (修正版)
    低耦合架构：先逐个币种回测并保存交易明细 -> 最后汇总分析
    支持多参数组合自动网格搜索，并动态打印参数标识
    入场：长期Z偏离与短期多小时相对强弱同时满足；参数组合多进程并行

假设：USDT线性合约，volume单位为基础币，close为小时K线收盘价。
时间戳统一为UTC收盘边界：open_time + 1h；不使用下一根open。
每笔配对的初始毛名义总额固定；两腿按Beta分配，不保证腿间等额。
两腿冻结带符号数量，独立结算。
net_pnl等为USDT金额；net_return才是除以双腿初始毛名义金额的收益率。
未模拟共享资金、杠杆、强平、订单精度，因此输出为独立交易样本统计。
MAE/MFE使用含开仓费及预计平仓费的净清算收益率，按小时收盘价更新。
包含开仓和平仓时点；MAE<=0、MFE>=0，无对应方向的偏移时为0且时间为空。
价格路径不完整或交易未结算时，四个极值字段均为空，避免将局部极值当成完整极值。
依赖：pandas、numpy；tqdm为可选进度条。
"""
import pandas as pd
import numpy as np
import os
import glob
import json
import hashlib
import tempfile
import multiprocessing
import traceback
from contextlib import redirect_stdout, redirect_stderr, contextmanager
from concurrent.futures import ProcessPoolExecutor, as_completed
try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable


# ==========================================
# 1. 全局绝对性配置参数 (支持动态网格搜索)
# ==========================================
class Config:
    DATA_DIR = r"W:\project\python_project\oke_auto_trade\kline_data"
    BASE_OUTPUT_DIR = r"trade_results"
    BTC_SYMBOL = "BTCUSDT"
    KLINE_FILE_TEMPLATE = "{symbol}_1h_2021-01-01_merged.csv"
    SYMBOLS_FILE = "symbols.json"

    BETA_WINDOW_DAYS = 30
    SIGNAL_WINDOW_HOURS = 24
    Z_SCORE_THRESHOLD = 2.0
    HOLDING_PERIOD_HOURS = 6

    # 短期确认单独估计Beta；实际两腿数量仍使用原来的长期Beta。
    SHORT_BETA_WINDOW_DAYS = 7
    SHORT_SIGNAL_WINDOW_HOURS = 6
    SHORT_EXCESS_THRESHOLD = 0.0  # 每小时平均对数残差的绝对门槛；0.0005 = 5bp/h。
    SHORT_MIN_BAR_RATIO = 0.5  # 对应窗口/涨跌子样本中，同方向残差bar的最低比例。
    SHORT_MIN_REGIME_BARS = 2  # UP/DOWN子样本至少2根；无该方向样本不得自动通过。
    SHORT_CONFIRM_MODE = "BOTH"  # NET / UP / DOWN / BOTH / EITHER，见下方网格注释。
    SHORT_CONFIRM_TIMING = "ROLLING"  # ROLLING / POST_TRIGGER。
    FEE_RATE = 0.001  # 每腿每次实际成交额的0.1%；统一成本假设，不另计资金费
    PAIR_GROSS_NOTIONAL = 1000.0  # 每笔双腿初始毛名义总额，USDT
    MIN_BTC_VARIANCE = 1e-16
    MIN_RESIDUAL_STD = 1e-10

    ENTRY_START = None
    EVALUATION_END = None

    # 第一轮定性粗筛：先比较信号结构与时间尺度，再根据结果细调数值。
    # 第一组含持仓期：2 * 3 * 2 * 1 = 12组；Beta天数先固定，不代表最优。
    Z_THRESHOLDS_TO_TEST = [6.0, 9.0]
    HOLDING_PERIODS_TO_TEST = [48, 96, 168]
    SIGNAL_WINDOWS_TO_TEST = [24, 60]
    BETA_WINDOWS_TO_TEST = [60]

    # 第二组：Beta天数与幅度门槛先固定；与第一组完整交叉，不按长短关系删组合。
    SHORT_BETA_WINDOWS_TO_TEST = [7]
    SHORT_SIGNAL_WINDOWS_TO_TEST = [6, 12, 24, 48]
    SHORT_EXCESS_THRESHOLDS_TO_TEST = [0.0]
    # 0.0检验平均表现；0.75检验转弱/转强是否分布在更多bar上。
    SHORT_MIN_BAR_RATIOS_TO_TEST = [0.0, 0.75]
    # NET: 全窗口；UP: BTC上涨小时；DOWN: BTC下跌小时；
    # BOTH: UP且DOWN；EITHER: UP或DOWN。五种均与长期Z条件取AND。
    SHORT_CONFIRM_MODES_TO_TEST = ["NET", "UP", "DOWN", "BOTH", "EITHER"]
    # ROLLING允许确认窗口覆盖突破之前；POST_TRIGGER要求整个窗口在突破之后。
    SHORT_CONFIRM_TIMINGS_TO_TEST = ["ROLLING", "POST_TRIGGER"]
    # 短期组：1 * 4 * 1 * 2 * 5 * 2 = 80组；完整交叉12 * 80 = 960组。
    MAX_GRID_COMBINATIONS = 1000  # 本轮粗筛上限；超限报错，不截取或随机丢弃组合。

    # 每个进程独立运行一组参数；多币种长历史会占内存，可按机器调整。
    MAX_WORKERS = max(1, min(10, (os.cpu_count() or 1) - 1))
    NUMERIC_THREADS_PER_WORKER = 1  # 避免每个进程再启动一整组BLAS线程。

    CACHE_VERSION = "dual_horizon_fixed_beta_excursion_net_v4"

    BETA_WINDOW_HOURS = BETA_WINDOW_DAYS * 24
    SHORT_BETA_WINDOW_HOURS = SHORT_BETA_WINDOW_DAYS * 24
    PARAM_FOLDER = (f"Z{Z_SCORE_THRESHOLD}_H{HOLDING_PERIOD_HOURS}"
                    f"_B{BETA_WINDOW_DAYS}_S{SIGNAL_WINDOW_HOURS}"
                    f"_b{SHORT_BETA_WINDOW_DAYS}_s{SHORT_SIGNAL_WINDOW_HOURS}"
                    f"_e{SHORT_EXCESS_THRESHOLD}_p{SHORT_MIN_BAR_RATIO}"
                    f"_n{SHORT_MIN_REGIME_BARS}_{SHORT_CONFIRM_MODE}_{SHORT_CONFIRM_TIMING}")
    OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, PARAM_FOLDER)
    RUN_ID = ""
    MARKET_ID = ""

    @classmethod
    def update_params(cls, z_score, holding_period, beta_window=30, signal_window=24,
                      short_beta_window=7, short_signal_window=6,
                      short_excess_threshold=0.0, short_min_bar_ratio=0.5,
                      short_confirm_mode="BOTH", short_confirm_timing="ROLLING"):
        """动态更新参数并重建路径配置；实际运行再绑定数据/代码指纹。"""
        if (not np.isfinite(z_score) or z_score <= 0
                or any(int(x) != x or x <= 0 for x in
                       (holding_period, beta_window, signal_window))):
            raise ValueError("阈值必须为正数，时间窗口必须为正整数")
        cls.Z_SCORE_THRESHOLD = float(z_score)
        cls.HOLDING_PERIOD_HOURS = int(holding_period)
        cls.BETA_WINDOW_DAYS = int(beta_window)
        cls.SIGNAL_WINDOW_HOURS = int(signal_window)
        if any(isinstance(x, (bool, np.bool_)) or not np.isfinite(x)
               or int(x) != x or x <= 0
               for x in (short_beta_window, short_signal_window)):
            raise ValueError("短期Beta天数和信号小时数必须为正整数")
        if short_signal_window < 2:
            raise ValueError("短期确认窗口必须至少包含2根小时bar")
        if not np.isfinite(short_excess_threshold) or short_excess_threshold < 0:
            raise ValueError("short_excess_threshold必须为有限非负数")
        if not np.isfinite(short_min_bar_ratio) or not 0 <= short_min_bar_ratio <= 1:
            raise ValueError("short_min_bar_ratio必须在[0, 1]内")
        n = cls.SHORT_MIN_REGIME_BARS
        if (isinstance(n, (bool, np.bool_)) or not np.isfinite(n)
                or int(n) != n or n < 2):
            raise ValueError("SHORT_MIN_REGIME_BARS必须为不小于2的整数")
        if short_confirm_mode not in ("NET", "UP", "DOWN", "BOTH", "EITHER"):
            raise ValueError("short_confirm_mode必须为NET/UP/DOWN/BOTH/EITHER")
        if short_confirm_timing not in ("ROLLING", "POST_TRIGGER"):
            raise ValueError("short_confirm_timing必须为ROLLING或POST_TRIGGER")
        cls.SHORT_BETA_WINDOW_DAYS = int(short_beta_window)
        cls.SHORT_SIGNAL_WINDOW_HOURS = int(short_signal_window)
        cls.SHORT_EXCESS_THRESHOLD = float(short_excess_threshold)
        cls.SHORT_MIN_BAR_RATIO = float(short_min_bar_ratio)
        cls.SHORT_CONFIRM_MODE = short_confirm_mode
        cls.SHORT_CONFIRM_TIMING = short_confirm_timing
        cls.BETA_WINDOW_HOURS = cls.BETA_WINDOW_DAYS * 24
        cls.SHORT_BETA_WINDOW_HOURS = cls.SHORT_BETA_WINDOW_DAYS * 24
        if cls.BETA_WINDOW_HOURS <= cls.SIGNAL_WINDOW_HOURS:
            raise ValueError("Beta历史窗口必须长于信号窗口")
        cls.PARAM_FOLDER = (f"Z{cls.Z_SCORE_THRESHOLD}_H{cls.HOLDING_PERIOD_HOURS}"
                            f"_B{cls.BETA_WINDOW_DAYS}_S{cls.SIGNAL_WINDOW_HOURS}"
                            f"_b{cls.SHORT_BETA_WINDOW_DAYS}_s{cls.SHORT_SIGNAL_WINDOW_HOURS}"
                            f"_e{cls.SHORT_EXCESS_THRESHOLD}_p{cls.SHORT_MIN_BAR_RATIO}"
                            f"_n{cls.SHORT_MIN_REGIME_BARS}_{cls.SHORT_CONFIRM_MODE}"
                            f"_{cls.SHORT_CONFIRM_TIMING}")
        cls.OUTPUT_DIR = os.path.join(cls.BASE_OUTPUT_DIR, cls.PARAM_FOLDER)
        cls.RUN_ID = cls.MARKET_ID = ""

TRADE_COLUMNS = [
    "run_id", "trade_id", "symbol", "status", "entry_time", "scheduled_exit_time",
    "exit_time", "direction", "btc_direction", "entry_price", "exit_price",
    "btc_entry", "btc_exit", "beta", "z_score", "exit_z_score",
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
    "exit_reason",
    "mae_return", "mfe_return", "mae_time", "mfe_time"
]


def utc_timestamp(value):
    return pd.to_datetime(value, utc=True) if value is not None else None


def kline_path(symbol):
    return os.path.join(Config.DATA_DIR, Config.KLINE_FILE_TEMPLATE.format(symbol=symbol))


def load_kline(symbol):
    """排序、拒绝重复时间戳，保留缺口为NaN；禁止填价格。"""
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
    """公共时间轴延伸到两份数据中较晚的结尾，以显式发现退市/尾部缺口。"""
    start = max(df_alt.index.min(), df_btc.index.min())
    end = max(df_alt.index.max(), df_btc.index.max())
    limit = utc_timestamp(Config.EVALUATION_END)
    if limit is not None:
        end = min(end, limit)
    index = pd.date_range(start, end, freq="h", name="close_time")
    return df_alt.reindex(index), df_btc.reindex(index)


def eligible_pool(df_alt, df_btc):
    # B+S小时收益需要B+S+1个价格点；价格有效性同时覆盖BTC。
    n = Config.BETA_WINDOW_HOURS + Config.SIGNAL_WINDOW_HOURS + 1
    good = (df_alt["close"].notna() & df_alt["volume"].notna()
            & df_btc["close"].notna())
    return good.astype(float).rolling(n, min_periods=n).sum().eq(n)


def historical_turnover(df):
    # 排除触发信号的当前小时，避免暴涨放量改变自己的分组。
    return (df["volume"] * df["close"]).shift(1).rolling(
        Config.BETA_WINDOW_HOURS, min_periods=Config.BETA_WINDOW_HOURS).mean() * 24


def atomic_csv(df, path, reuse_existing=False, **kwargs):
    # 不同参数可能同时计算同一个市场中位数缓存；临时文件必须各自独立。
    if reuse_existing and os.path.isfile(path):
        return
    fd, temp = tempfile.mkstemp(prefix=os.path.basename(path) + ".",
                                suffix=".tmp", dir=os.path.dirname(path) or ".")
    os.close(fd)
    try:
        df.to_csv(temp, **kwargs)
        try:
            os.replace(temp, path)
        except PermissionError:
            # Windows可能禁止替换正在被其他进程读取的文件。同一指纹的市场
            # 缓存已经由另一进程完整发布时直接复用；其他写入错误仍然上抛。
            if not (reuse_existing and os.path.isfile(path)):
                raise
    finally:
        if os.path.exists(temp):
            os.remove(temp)


def atomic_json(value, path):
    fd, temp = tempfile.mkstemp(prefix=os.path.basename(path) + ".",
                                suffix=".tmp", dir=os.path.dirname(path) or ".")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(value, f, ensure_ascii=False, indent=2)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.remove(temp)


def result_path(name):
    """目录和文件名同时标识参数；保持交易文件以_trades.csv结尾。"""
    return os.path.join(Config.OUTPUT_DIR, f"{Config.PARAM_FOLDER}_{name}")


def file_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def prepare_run(symbols, data_fingerprints=None):
    """文件内容、币池、全部结果相关参数及本脚本变化均使缓存失效。"""
    if not np.isfinite(Config.FEE_RATE) or Config.FEE_RATE < 0:
        raise ValueError("FEE_RATE必须为有限非负数")
    if not np.isfinite(Config.PAIR_GROSS_NOTIONAL) or Config.PAIR_GROSS_NOTIONAL <= 0:
        raise ValueError("PAIR_GROSS_NOTIONAL必须为有限正数")
    begin, end = utc_timestamp(Config.ENTRY_START), utc_timestamp(Config.EVALUATION_END)
    if begin is not None and end is not None and begin >= end:
        raise ValueError("ENTRY_START必须早于EVALUATION_END")

    universe = sorted(set(symbols) | {Config.BTC_SYMBOL})

    # 【修改点】：取消原来的直接崩溃报错，改为在底层再次确认并兼容跳过逻辑
    missing = [s for s in universe if not os.path.isfile(kline_path(s))]
    if missing:
        if Config.BTC_SYMBOL in missing:
            raise FileNotFoundError(f"❌ 基础对冲币种 {Config.BTC_SYMBOL} 的数据文件不存在，程序无法运行！")
        print(f"⚠️ 警告: prepare_run发现缺失K线文件，将从币池中忽略: {missing}")
        universe = [s for s in universe if s not in missing]

    if data_fingerprints is None:
        data_fingerprints = {s: file_sha256(kline_path(s)) for s in universe}

    market = dict(version=Config.CACHE_VERSION, code=file_sha256(os.path.abspath(__file__)),
                  data={s: data_fingerprints[s] for s in universe}, universe=universe,
                  btc=Config.BTC_SYMBOL, beta=Config.BETA_WINDOW_HOURS,
                  signal=Config.SIGNAL_WINDOW_HOURS, end=Config.EVALUATION_END)
    Config.MARKET_ID = fingerprint(market)
    manifest = dict(market=market, z=Config.Z_SCORE_THRESHOLD,
                    holding=Config.HOLDING_PERIOD_HOURS,
                    short_beta=Config.SHORT_BETA_WINDOW_HOURS,
                    short_signal=Config.SHORT_SIGNAL_WINDOW_HOURS,
                    short_excess_threshold=Config.SHORT_EXCESS_THRESHOLD,
                    short_min_bar_ratio=Config.SHORT_MIN_BAR_RATIO,
                    short_min_regime_bars=Config.SHORT_MIN_REGIME_BARS,
                    short_confirm_mode=Config.SHORT_CONFIRM_MODE,
                    short_confirm_timing=Config.SHORT_CONFIRM_TIMING,
                    short_basis="mean_hourly_log_residual_fixed_pre_window_beta",
                    fee=Config.FEE_RATE,
                    gross=Config.PAIR_GROSS_NOTIONAL, start=Config.ENTRY_START,
                    min_var=Config.MIN_BTC_VARIANCE, min_std=Config.MIN_RESIDUAL_STD,
                    pandas=pd.__version__, numpy=np.__version__,
                    excursion=dict(basis="net_liquidation_return", sampling="hourly_close",
                                   include_entry=True, include_exit=True,
                                   missing_policy="invalidate_all_four_fields",
                                   zero_time_policy="NaT", tie_policy="first"))
    Config.RUN_ID = fingerprint(manifest)
    Config.OUTPUT_DIR = os.path.join(Config.BASE_OUTPUT_DIR, Config.PARAM_FOLDER + "_" + Config.RUN_ID[:16])
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(Config.OUTPUT_DIR, "run_manifest.json")
    atomic_json(manifest, path)

# ==========================================
# 2. 核心信号计算与指标加工模块
# ==========================================
def calculate_indicators(df_alt, df_btc):
    """历史残差均采用当前时点事先估计的同一个Beta，且窗口完整位于30天内。"""
    df_alt, df_btc = align_hourly(df_alt, df_btc)
    df = pd.DataFrame(index=df_alt.index)
    df["close"] = df_alt["close"]
    df["volume"] = df_alt["volume"]
    df["btc_close"] = df_btc["close"]
    B, S = Config.BETA_WINDOW_HOURS, Config.SIGNAL_WINDOW_HOURS
    df["ret_1h"] = np.log(df["close"]).diff()
    df["btc_ret_1h"] = np.log(df["btc_close"]).diff()
    # 使用滚动求和而非端点相除，缺失小时不会被跨过。
    df["ret_24h"] = df["ret_1h"].rolling(S, min_periods=S).sum()
    df["btc_ret_24h"] = df["btc_ret_1h"].rolling(S, min_periods=S).sum()
    cov = df["ret_1h"].rolling(B, min_periods=B).cov(df["btc_ret_1h"])
    var = df["btc_ret_1h"].rolling(B, min_periods=B).var()
    df["beta"] = cov / var.where(var > Config.MIN_BTC_VARIANCE)
    df["beta_shifted"] = df["beta"].shift(S)
    beta = df["beta_shifted"]
    df["residual_24h"] = df["ret_24h"] - beta * df["btc_ret_24h"]

    # 对t时点，Beta收益训练区间是[t-S-B+1, t-S]，共B个小时收益。
    # 完整落在该区间内的S小时收益窗口端点：[t-B, t-S]，共B-S+1个。
    # Var(A-beta*M) = Var(A)+beta²*Var(M)-2*beta*Cov(A,M)。
    # 等价于逐时点用当期Beta重算所有历史残差，避免第二层30天预热。
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
    df = calculate_short_confirmation(df)
    return df


def calculate_short_confirmation(df):
    """短期Beta训练结束于确认窗口之前；窗口内每根bar均使用同一个Beta。

    e_i(t) = alt_ret_i - short_beta_t * btc_ret_i。
    不减去历史残差均值：这里检验实际相对走势，而不是是否低于历史平均。
    均值严格超过幅度门槛，支持bar比例达到下限，才算对应方向确认。
    BTC零收益bar计入NET，不属于UP/DOWN；缺少涨/跌子样本不能视为通过。
    """
    B = Config.SHORT_BETA_WINDOW_HOURS
    C = Config.SHORT_SIGNAL_WINDOW_HOURS
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

    # 沿窗口长度循环，整条时间轴向量化；不创建n*C的大矩阵。
    # 不能先用每小时各自的Beta算残差再rolling，否则窗口内Beta并不固定。
    for lag in range(C):
        ar, mr = a.shift(lag).to_numpy(), m.shift(lag).to_numpy()
        residual = ar - beta * mr
        valid = np.isfinite(residual)
        masks = {"net": valid, "up": valid & (mr > 0), "down": valid & (mr < 0)}
        for g in groups:
            mask = masks[g]
            counts[g] += mask
            sums[g] += np.where(mask, residual, 0.0)
            negatives[g] += mask & (residual < 0)
            positives[g] += mask & (residual > 0)

    complete = counts["net"] == C
    threshold, ratio = Config.SHORT_EXCESS_THRESHOLD, Config.SHORT_MIN_BAR_RATIO
    short_ok, long_ok = {}, {}
    for g in groups:
        prefix = "short" if g == "net" else f"short_{g}"
        count = counts[g]
        mean = np.divide(sums[g], count, out=np.full(n, np.nan), where=count > 0)
        negative_ratio = np.divide(negatives[g], count,
                                   out=np.full(n, np.nan), where=count > 0)
        positive_ratio = np.divide(positives[g], count,
                                   out=np.full(n, np.nan), where=count > 0)
        df[f"{prefix}_mean_excess"] = np.where(complete, mean, np.nan)
        df[f"{prefix}_negative_ratio"] = np.where(complete, negative_ratio, np.nan)
        df[f"{prefix}_positive_ratio"] = np.where(complete, positive_ratio, np.nan)
        if g != "net":
            df[f"{prefix}_count"] = np.where(complete, count, np.nan)
        required = C if g == "net" else Config.SHORT_MIN_REGIME_BARS
        enough = complete & (count >= required)
        short_ok[g] = enough & (mean < -threshold) & (negative_ratio >= ratio)
        long_ok[g] = enough & (mean > threshold) & (positive_ratio >= ratio)

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
# 3. 预计算动态截面中位数 & 单标的回测模块
# ==========================================
def generate_market_median(symbols, btc_df=None):
    """仅用当时满足历史完整性准入要求的币种计算中位数。"""
    if not Config.MARKET_ID:
        prepare_run(symbols)
    cache_path = os.path.join(Config.BASE_OUTPUT_DIR, f"market_median_{Config.MARKET_ID}.csv")
    if os.path.exists(cache_path):
        df_cache = pd.read_csv(cache_path)
        df_cache["close_time"] = pd.to_datetime(df_cache["close_time"], utc=True)
        return df_cache.set_index("close_time")["median_turnover"]
    print("预计算全市场动态截面中位数...")
    if btc_df is None:
        btc_df = load_kline(Config.BTC_SYMBOL)
    turnover_dfs = []
    for symbol in tqdm(sorted(set(symbols))):
        if symbol == Config.BTC_SYMBOL:
            continue
        alt, btc = align_hourly(load_kline(symbol), btc_df)
        turnover = historical_turnover(alt).where(eligible_pool(alt, btc))
        turnover_dfs.append(turnover.rename(symbol))
    series = (pd.concat(turnover_dfs, axis=1).median(axis=1)
              if turnover_dfs else pd.Series(dtype=float))
    series.name = "median_turnover"
    atomic_csv(series, cache_path, reuse_existing=True, header=True, index_label="close_time")
    return series


def valid_pair_prices(alt_price, btc_price):
    return (np.isfinite(alt_price) and alt_price > 0
            and np.isfinite(btc_price) and btc_price > 0)


def pair_valuation(position, alt_price, btc_price):
    """同一套固定数量/冻结费率公式，同时用于逐小时盯市和最终结算。"""
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


def invalidate_excursions(position):
    """只有四个输出字段，不额外输出质量标记；空值明确表示无法得到完整极值。"""
    position.update(mae_return=np.nan, mfe_return=np.nan,
                    mae_time=pd.NaT, mfe_time=pd.NaT, _excursion_valid=False)


def update_excursions(position, now, alt_price, btc_price):
    """先盯市再判断退出；不依赖当前Z/Beta是否有效。并列极值保留首次时间。"""
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
    """每笔总名义固定；Beta可为负或0，数量一经开仓就不再随Beta变化。"""
    now, z, beta = row.Index, row.z_score, row.beta_shifted
    direction = 1 if z < -Config.Z_SCORE_THRESHOLD else -1
    alt_notional = Config.PAIR_GROSS_NOTIONAL / (1 + abs(beta))
    btc_signed_notional = -direction * beta * alt_notional
    position = dict(
        run_id=Config.RUN_ID, trade_id=f"{symbol}_{now.isoformat()}", symbol=symbol,
        status="OPEN", entry_time=now, scheduled_exit_time=scheduled_exit_time,
        direction="LONG_ALT" if direction == 1 else "SHORT_ALT",
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
        exit_reason=None,
        mae_return=0.0, mfe_return=0.0, mae_time=pd.NaT, mfe_time=pd.NaT,
        _excursion_valid=True, _last_mark_time=None)
    # 开仓当刻的假设净清算收益约为-2*fee_rate；没有正收益时MFE保持0/NaT。
    update_excursions(position, now, row.close, row.btc_close)
    return position


def close_pair_position(position, now, alt_price, btc_price, z, reason):
    """含平仓时点极值；无双腿有效平仓价时保留未结算样本，不伪造成交。"""
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


def backtest_single_symbol(symbol, df, market_median_series):
    """独立配对；到期退出先于指标检查；数量固定；缺失平仓价不伪造成交。"""
    trades = []
    position = None
    armed = False  # 必须先看到正常区间，才能确认首次突破。
    pending_side = 0  # +1/-1: 长期Z已突破，等待对应方向的多小时确认。
    trigger_time, trigger_z = None, np.nan
    threshold = Config.Z_SCORE_THRESHOLD
    begin, end = utc_timestamp(Config.ENTRY_START), utc_timestamp(Config.EVALUATION_END)
    hold = pd.Timedelta(hours=Config.HOLDING_PERIOD_HOURS)

    for row in df.itertuples():
        now, z = row.Index, row.z_score
        valid_z = np.isfinite(z) and np.isfinite(row.beta_shifted)
        normal = valid_z and -threshold <= z <= threshold

        # 绝不能因Z/Beta缺失跳过已经到期的仓位。
        if position is not None:
            # 关键：到期前也逐小时更新，且不受Z/Beta缺失影响。
            if now <= position["scheduled_exit_time"]:
                update_excursions(position, now, row.close, row.btc_close)
            if now < position["scheduled_exit_time"]:
                continue
            valid_prices = valid_pair_prices(row.close, row.btc_close)
            if now != position["scheduled_exit_time"] or not valid_prices:
                invalidate_excursions(position)
                position["status"] = "UNRESOLVED_MISSING_EXIT"
                position["exit_reason"] = "MISSING_SCHEDULED_EXIT"
                trades.append(position)
                position = None
                # 仓位未能结算，停止本币后续交易；不删除样本，也不私自延期。
                break
            close_pair_position(position, now, row.close, row.btc_close, z, "FIXED_HOLD")
            trades.append(position)
            position = None
            armed = normal  # 退出时已正常即可复位；持仓期间的回归不算平仓后复位。
            pending_side = 0
            trigger_time, trigger_z = None, np.nan
            continue

        if not valid_z:
            armed = False  # 缺口后首次看到极值，无法确认这是首次突破。
            pending_side = 0
            trigger_time, trigger_z = None, np.nan
            continue
        if normal:
            armed = True
            pending_side = 0
            trigger_time, trigger_z = None, np.nan
            continue
        if pending_side:
            if (1 if z > 0 else -1) != pending_side:
                pending_side = 0  # 跨0跳到另一侧极值，不当成原方向的回归。
                trigger_time, trigger_z = None, np.nan
                continue
        else:
            if not armed:
                continue
            armed = False
            pending_side = 1 if z > 0 else -1
            trigger_time, trigger_z = now, z
        # 长期条件必须仍在同侧阈值外；回到正常区间已在上面清除等待。
        # POST_TRIGGER在突破后至少经过C小时，整个短期收益窗口才位于突破之后。
        if (Config.SHORT_CONFIRM_TIMING == "POST_TRIGGER"
                and now - trigger_time < pd.Timedelta(hours=Config.SHORT_SIGNAL_WINDOW_HOURS)):
            continue
        ready = row.short_ready_short if pending_side == 1 else row.short_ready_long
        if not ready:
            continue  # 短期尚未确认，不消耗突破；后续同侧极值小时继续检查。
        pending_side = 0  # 首次双条件满足消耗信号；沿用原有区间/分组不满足不追单规则。
        if begin is not None and now < begin:
            continue
        if end is not None and (now >= end or now + hold > end):
            continue
        median = market_median_series.get(now, np.nan)
        if not np.isfinite(median) or not np.isfinite(row.avg_turnover_30d):
            continue  # 缺失基准不能自动归为低成交组。
        if not (np.isfinite(row.close) and row.close > 0
                and np.isfinite(row.btc_close) and row.btc_close > 0):
            continue
        position = open_pair_position(symbol, row, median, now + hold,
                                      Config.HOLDING_PERIOD_HOURS)
        position.update(long_trigger_time=trigger_time, long_trigger_z=trigger_z)

    if position is not None:
        invalidate_excursions(position)
        position["status"] = "UNRESOLVED_END_OF_DATA"
        position["exit_reason"] = "END_OF_DATA"
        trades.append(position)
    return pd.DataFrame(trades, columns=TRADE_COLUMNS)


# ==========================================
# 4. 主控调度流程
# ==========================================
def process_symbol(symbol, btc_df, market_median_series):
    """成功的零交易也保存表头；异常不落成功缓存；失败时整组不汇总。"""
    output_csv = result_path(f"{symbol}_trades.csv")
    if os.path.exists(output_csv):
        # 检查结构，避免把旧版/不完整CSV误认为可复用结果。
        columns = pd.read_csv(output_csv, nrows=0).columns.tolist()
        if columns != TRADE_COLUMNS:
            raise ValueError(f"缓存字段不匹配，请删除后重跑: {output_csv}")
        return
    df_alt = load_kline(symbol)
    indicators = calculate_indicators(df_alt, btc_df)
    records = backtest_single_symbol(symbol, indicators, market_median_series)
    atomic_csv(records, output_csv, index=False)


def save_evaluation_window(market_median_series):
    """保存实际观察区间，统计四等分时保留无交易时段，不从首末成交倒推区间。"""
    valid = market_median_series.dropna()
    start = valid.index.min() if not valid.empty else None
    end = market_median_series.index.max() if not market_median_series.empty else None
    requested_start = utc_timestamp(Config.ENTRY_START)
    if start is not None and requested_start is not None:
        start = max(start, requested_start)
    if start is not None and end is not None and start > end:
        start = end  # 配置起点晚于所有数据：无可交易时间、零交易。
    atomic_json(dict(run_id=Config.RUN_ID, timezone="UTC",
                     start=start.isoformat() if start is not None else None,
                     end=end.isoformat() if end is not None else None,
                     basis="first_eligible_market_hour_to_last_observed_hour"),
                os.path.join(Config.OUTPUT_DIR, "evaluation_window.json"))


def run_all_backtests(symbols, data_fingerprints=None, prepared=False):
    """运行所有币种回测。源CSV应为运行期间不变的静态快照。"""
    symbols = sorted(set(symbols))
    if not prepared:
        prepare_run(symbols, data_fingerprints)
    print("加载 BTC 基准数据...")
    btc_df = load_kline(Config.BTC_SYMBOL)
    median = generate_market_median(symbols, btc_df)
    save_evaluation_window(median)
    print(f"开始回测配对交易，共 {len(symbols)} 个币种；输出: {Config.OUTPUT_DIR}")
    errors = []
    for symbol in tqdm(symbols):
        if symbol == Config.BTC_SYMBOL:
            continue
        try:
            process_symbol(symbol, btc_df, median)
        except Exception as e:
            errors.append(f"{symbol}: {e}")
    if errors:
        raise RuntimeError("部分币种失败，禁止把残缺样本汇总为完整结果:\n" + "\n".join(errors))


# ==========================================
# 5. 统计与分析模块 (分组与留出期检验基础)
# ==========================================
def analyze_results():
    """这里只统计独立交易样本，不把交易收益相加伪称组合收益率。"""
    all_files = sorted(glob.glob(os.path.join(Config.OUTPUT_DIR, "*_trades.csv")))
    if not all_files:
        print("没有找到任何交易记录文件。")
        return
    frames = [pd.read_csv(f) for f in all_files]
    nonempty = [x for x in frames if not x.empty]
    if not nonempty:
        print("交易记录为空；各币种的零交易结果已缓存。")
        return
    all_trades = pd.concat(nonempty, ignore_index=True)
    print("\n" + "=" * 60)
    print(f"【参数组合评估】 Z: {Config.Z_SCORE_THRESHOLD} | 持仓: {Config.HOLDING_PERIOD_HOURS}h"
          f" | 长期Beta: {Config.BETA_WINDOW_DAYS}d | 长期信号: {Config.SIGNAL_WINDOW_HOURS}h"
          f" | 短期Beta: {Config.SHORT_BETA_WINDOW_DAYS}d | 短期信号: {Config.SHORT_SIGNAL_WINDOW_HOURS}h"
          f" | 短期门槛: {Config.SHORT_EXCESS_THRESHOLD} | 支持bar比例: {Config.SHORT_MIN_BAR_RATIO}"
          f" | 涨跌最少bar: {Config.SHORT_MIN_REGIME_BARS}"
          f" | 确认: {Config.SHORT_CONFIRM_MODE} | 时序: {Config.SHORT_CONFIRM_TIMING}")
    print("【配对交易样本统计；不是共享资金账户收益】")
    print(all_trades["status"].value_counts().to_string())
    unresolved = all_trades[all_trades["status"] != "CLOSED"]
    df = all_trades[all_trades["status"] == "CLOSED"].copy()
    if not unresolved.empty:
        print("结果不完整：存在未结算交易，下列仅为已平仓子集，不可用于判断策略盈利。")
        print(f"未结算笔数: {len(unresolved)}；这些交易已发生开仓成本: {unresolved['entry_cost'].sum():.4f} USDT")
    if df.empty:
        return
    print(f"已平仓次数: {len(df)}")
    print(f"胜率: {(df['net_pnl'] > 0).mean():.2%}")
    print(f"单笔平均净收益率 (双腿初始毛名义口径): {df['net_return'].mean():.4%}")
    print(f"单笔净收益率中位数: {df['net_return'].median():.4%}")
    print(f"独立交易净盈亏合计: {df['net_pnl'].sum():.4f} USDT")
    mae = df["mae_return"].dropna()
    print(f"完整极值样本: {len(mae)}/{len(df)}；持仓价格缺口使整笔MAE/MFE为空。")
    if not mae.empty:
        print(f"平均单笔最大浮亏率: {mae.mean():.4%} | P10: {mae.quantile(0.10):.4%}"
              f" | 历史最差: {mae.min():.4%}")
    quadrants = df.groupby(["vol_group", "direction"]).agg(
        trade_count=("symbol", "count"),
        win_rate=("net_pnl", lambda x: (x > 0).mean()),
        avg_net_return=("net_return", "mean"),
        median_net_return=("net_return", "median"),
        avg_net_pnl=("net_pnl", "mean"),
        sum_net_pnl=("net_pnl", "sum"),
        avg_cost=("total_cost", "mean"),
        excursion_valid_count=("mae_return", "count"),
        avg_mae_return=("mae_return", "mean"),
        p10_mae_return=("mae_return", lambda x: x.quantile(0.10)),
        worst_mae_return=("mae_return", "min"),
        avg_mfe_return=("mfe_return", "mean")
    ).reset_index()
    print("\n【四象限独立核算表现；收益率列为小数】")
    print(quadrants.to_string(index=False))
    # 无样本象限不制造0收益；独立BTC腿按各笔成交全额计费。
    atomic_csv(quadrants, result_path("quadrant_summary.csv"), index=False)
    print("未计算组合年化/夏普/最大回撤：需要共享资金分配及逐小时盯市账本。")


@contextmanager
def parameter_run_lock():
    """额外防止两个独立脚本同时写同一组结果；不同组合互不等待。"""
    lock_path = os.path.join(Config.OUTPUT_DIR, ".run.lock")
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise RuntimeError(
            f"同一组结果正在运行或上次异常中断留下锁: {lock_path}；"
            "仅在确认无对应任务运行后才可删除此锁") from exc
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"pid": os.getpid(), "run_id": Config.RUN_ID}, f)
        yield
    finally:
        os.remove(lock_path)


def build_param_grid():
    """长期组×持仓期×短期组的完整笛卡尔积；只去重，不按主观规则删组合。

    不强制短期窗口/短期Beta小于长期；也不提前删除可能零交易的组合。
    真正非法的数值在update_params中报FAILED，不会被静默忽略。
    """
    import itertools
    grid = []
    seen = set()
    for params in itertools.product(
            Config.Z_THRESHOLDS_TO_TEST, Config.HOLDING_PERIODS_TO_TEST,
            Config.BETA_WINDOWS_TO_TEST, Config.SIGNAL_WINDOWS_TO_TEST,
            Config.SHORT_BETA_WINDOWS_TO_TEST, Config.SHORT_SIGNAL_WINDOWS_TO_TEST,
            Config.SHORT_EXCESS_THRESHOLDS_TO_TEST, Config.SHORT_MIN_BAR_RATIOS_TO_TEST,
            Config.SHORT_CONFIRM_MODES_TO_TEST, Config.SHORT_CONFIRM_TIMINGS_TO_TEST):
        if params not in seen:
            grid.append(params)
            seen.add(params)
    return grid


def run_parameter_combination(params, symbols, data_fingerprints, config_snapshot):
    """必须为模块顶层函数，供Windows spawn序列化；每进程同一时刻只跑一组。"""
    log_path = None
    try:
        # spawn不会继承父进程对Config类属性的运行时修改，必须显式传入配置。
        for key, value in config_snapshot.items():
            setattr(Config, key, value)
        Config.update_params(*params)
        prepare_run(symbols, data_fingerprints)

        # 【修改点 1】：检查当前参数组合是否已经成功运行过
        done_marker = os.path.join(Config.OUTPUT_DIR, ".done")
        if os.path.exists(done_marker):
            return dict(status="SKIPPED", params=Config.PARAM_FOLDER,
                        output_dir=Config.OUTPUT_DIR, log=None, pid=os.getpid())

        with parameter_run_lock():
            log_path = result_path("run.log")
            with open(log_path, "w", encoding="utf-8") as log:
                with redirect_stdout(log), redirect_stderr(log):
                    try:
                        print(f"参数: {Config.PARAM_FOLDER} | PID: {os.getpid()}")
                        run_all_backtests(symbols, data_fingerprints, prepared=True)
                        analyze_results()
                    except Exception:
                        traceback.print_exc()
                        raise

            # 【修改点 2】：本组参数全部回测与统计完成后，写入成功标记
            with open(done_marker, "w", encoding="utf-8") as f:
                f.write("success")

        return dict(status="OK", params=Config.PARAM_FOLDER,
                    output_dir=Config.OUTPUT_DIR, log=log_path, pid=os.getpid())
    except Exception as exc:
        return dict(status="FAILED", params=str(params), error=str(exc),
                    log=log_path, pid=os.getpid())


def run_parameter_grid(symbols, data_fingerprints, param_grid=None):
    """参数组合之间多进程并行；父进程只输出进度，各组详细输出写自己的日志。"""
    param_grid = build_param_grid() if param_grid is None else list(dict.fromkeys(param_grid))
    if not param_grid:
        raise ValueError("参数网格为空")
    for name in ("MAX_WORKERS", "NUMERIC_THREADS_PER_WORKER", "MAX_GRID_COMBINATIONS"):
        value = getattr(Config, name)
        if isinstance(value, bool) or int(value) != value or value <= 0:
            raise ValueError(f"{name}必须为正整数")
    if len(param_grid) > Config.MAX_GRID_COMBINATIONS:
        raise ValueError(
            f"参数网格共{len(param_grid)}组，超过本轮上限{Config.MAX_GRID_COMBINATIONS}组；"
            "请缩小搜索列表。程序不会截取网格，以免偏向排列靠前的组合。")
    workers = min(int(Config.MAX_WORKERS), len(param_grid))
    if os.name == "nt":
        workers = min(workers, 61)  # ProcessPoolExecutor在Windows上的进程数上限。
    config_snapshot = {key: getattr(Config, key) for key in vars(Config) if key.isupper()}
    print(f"\n规划了 {len(param_grid)} 组参数网格搜索任务，并行进程数: {workers}")
    print("每组明细、汇总和日志均写入各自参数目录。")
    # 父进程已导入numpy，但spawn子进程会在导入numpy之前继承这些环境变量。
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
            for done, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result()
                except Exception as exc:
                    result = dict(status="FAILED", params=str(futures[future]),
                                  error=str(exc), log=None)
                results.append(result)

                # 【修改点 3】：在控制台输出中适配 SKIPPED 状态
                if result["status"] == "OK":
                    print(f"✅ [{done}/{len(param_grid)}] {result['params']}")
                elif result["status"] == "SKIPPED":
                    print(f"⏭️ [{done}/{len(param_grid)}] {result['params']} (已处理，跳过)")
                else:
                    errors.append(result)
                    print(f"❌ [{done}/{len(param_grid)}] {result['params']}: {result['error']}")
                    if result.get("log"):
                        print(f"   日志: {result['log']}")
    finally:
        for key, value in previous_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    if errors:
        raise RuntimeError(f"{len(errors)}/{len(param_grid)} 组任务失败；成功组合结果已保留，详见日志。")
    return results


# ==========================================
# 启动入口 (支持自动化网格搜索)
# ==========================================
if __name__ == "__main__":
    multiprocessing.freeze_support()
    # 不自动删除锁：避免另一个仍在运行的脚本失去互斥保护。
    # 若上次异常退出，先确认相应任务已结束，再手工删除对应目录的.run.lock。
    with open(Config.SYMBOLS_FILE, "r", encoding="utf-8-sig") as f:
        SYMBOLS = json.load(f)
    if not isinstance(SYMBOLS, list) or not all(isinstance(s, str) for s in SYMBOLS):
        raise ValueError("symbols.json应为币种字符串列表")

    # 【新增】在开始计算指纹之前，提前检查并自动过滤掉缺失的文件
    valid_symbols = []
    for s in set(SYMBOLS):
        if os.path.isfile(kline_path(s)):
            valid_symbols.append(s)
        else:
            print(f"⚠️ 自动跳过: 未找到 {s} 的K线文件。")
    SYMBOLS = sorted(valid_symbols)

    # 检查基础币种文件是否存在（没有BTC数据，所有币都无法算对冲收益）
    if not os.path.isfile(kline_path(Config.BTC_SYMBOL)):
        raise FileNotFoundError(f"❌ 核心错误: 无法找到基准币种 {Config.BTC_SYMBOL} 的文件，停止运行。")

    # 静态源文件只在本次网格搜索开始时哈希一次；再次启动会重新验证。
    print(f"计算源数据指纹 (当前有效币种数: {len(SYMBOLS)})...")
    DATA_FINGERPRINTS = {
        s: file_sha256(kline_path(s)) for s in sorted(set(SYMBOLS) | {Config.BTC_SYMBOL})
    }

    run_parameter_grid(SYMBOLS, DATA_FINGERPRINTS)
