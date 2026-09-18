# -- coding: utf-8 --
""":authors:
    zhuxiaohu, AI Assistant
:create_date:
    2026/9/18
:description:
    基于Beta调整与固定期限的横截面统计套利策略 (修正版)
    低耦合架构：先逐个币种回测并保存交易明细 -> 最后汇总分析
    支持多参数组合自动网格搜索，并动态打印参数标识
    新增：可选右侧入场、全市场最大偏差单仓轮换、参数组合多进程并行

假设：USDT线性合约，volume单位为基础币，close为小时K线收盘价。
时间戳统一为UTC收盘边界：open_time + 1h；不使用下一根open。
每笔双腿初始毛名义金额相同；两腿冻结带符号数量，独立结算。
net_pnl等为USDT金额；net_return才是除以双腿初始毛名义金额的收益率。
未模拟共享资金、杠杆、强平、订单精度，因此输出为独立交易样本统计。
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
from tqdm import tqdm


# ==========================================
# 1. 全局绝对性配置参数 (支持动态网格搜索)
# ==========================================
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
    RIGHT_SIDE_ENTRY = False  # True: 突破后等待同号Z向0回头，再开仓。
    POSITION_MODE = "FIXED_HOLD"  # FIXED_HOLD / MAX_DEVIATION
    FEE_RATE = 0.001  # 每腿每次实际成交额的0.1%，已包含全部成本
    PAIR_GROSS_NOTIONAL = 1000.0  # 每笔双腿初始毛名义总额，USDT
    MIN_BTC_VARIANCE = 1e-16
    MIN_RESIDUAL_STD = 1e-10

    ENTRY_START = None
    EVALUATION_END = None

    # 【修改点】扩充后的网格搜索空间
    Z_THRESHOLDS_TO_TEST = [2.0, 2.5, 3.0, 3.5]       # 增加 2.5 观察平滑度
    HOLDING_PERIODS_TO_TEST = [6, 12, 24, 48]         # 增加 48h (长周期回归)
    SIGNAL_WINDOWS_TO_TEST = [12, 24, 48]             # 新增: 信号计算窗口
    BETA_WINDOWS_TO_TEST = [15, 30, 60]               # 新增: Beta历史窗口
    RIGHT_SIDE_ENTRIES_TO_TEST = [False, True]
    POSITION_MODES_TO_TEST = ["FIXED_HOLD", "MAX_DEVIATION"]

    # 每个进程独立运行一组参数；多币种长历史会占内存，可按机器调整。
    MAX_WORKERS = max(1, min(4, (os.cpu_count() or 1) - 1))
    NUMERIC_THREADS_PER_WORKER = 1  # 避免每个进程再启动一整组BLAS线程。

    CACHE_VERSION = "fixed_beta_right_side_rotation_v2"

    BETA_WINDOW_HOURS = BETA_WINDOW_DAYS * 24
    PARAM_FOLDER = (f"Z{Z_SCORE_THRESHOLD}_H{HOLDING_PERIOD_HOURS}"
                    f"_B{BETA_WINDOW_DAYS}_S{SIGNAL_WINDOW_HOURS}"
                    f"_R{int(RIGHT_SIDE_ENTRY)}_M{POSITION_MODE}")
    OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, PARAM_FOLDER)
    RUN_ID = ""
    MARKET_ID = ""

    @classmethod
    def update_params(cls, z_score, holding_period, beta_window=30, signal_window=24,
                      right_side_entry=False, position_mode="FIXED_HOLD"):
        """动态更新参数并重建路径配置；实际运行再绑定数据/代码指纹。"""
        if (not np.isfinite(z_score) or z_score <= 0
                or any(int(x) != x or x <= 0 for x in
                       (holding_period, beta_window, signal_window))):
            raise ValueError("阈值必须为正数，时间窗口必须为正整数")
        cls.Z_SCORE_THRESHOLD = float(z_score)
        cls.HOLDING_PERIOD_HOURS = int(holding_period)
        cls.BETA_WINDOW_DAYS = int(beta_window)
        cls.SIGNAL_WINDOW_HOURS = int(signal_window)
        if not isinstance(right_side_entry, (bool, np.bool_)):
            raise ValueError("right_side_entry必须为布尔值")
        if position_mode not in ("FIXED_HOLD", "MAX_DEVIATION"):
            raise ValueError("position_mode必须为FIXED_HOLD或MAX_DEVIATION")
        cls.RIGHT_SIDE_ENTRY = bool(right_side_entry)
        cls.POSITION_MODE = position_mode
        cls.BETA_WINDOW_HOURS = cls.BETA_WINDOW_DAYS * 24
        if cls.BETA_WINDOW_HOURS <= cls.SIGNAL_WINDOW_HOURS:
            raise ValueError("Beta历史窗口必须长于信号窗口")
        # 单仓轮换不使用H；统一写NA，避免无效H造成重复回测和重复文件。
        hold_tag = str(cls.HOLDING_PERIOD_HOURS) if position_mode == "FIXED_HOLD" else "NA"
        cls.PARAM_FOLDER = (f"Z{cls.Z_SCORE_THRESHOLD}_H{hold_tag}"
                            f"_B{cls.BETA_WINDOW_DAYS}_S{cls.SIGNAL_WINDOW_HOURS}"
                            f"_R{int(cls.RIGHT_SIDE_ENTRY)}_M{cls.POSITION_MODE}")
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
    "signal_window_hours", "right_side_entry", "position_mode", "exit_reason"
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

    market = dict(version=Config.CACHE_VERSION, code=file_sha256(__file__),
                  data={s: data_fingerprints[s] for s in universe}, universe=universe,
                  btc=Config.BTC_SYMBOL, beta=Config.BETA_WINDOW_HOURS,
                  signal=Config.SIGNAL_WINDOW_HOURS, end=Config.EVALUATION_END)
    Config.MARKET_ID = fingerprint(market)
    manifest = dict(market=market, z=Config.Z_SCORE_THRESHOLD,
                    holding=(Config.HOLDING_PERIOD_HOURS
                             if Config.POSITION_MODE == "FIXED_HOLD" else None),
                    right_side_entry=Config.RIGHT_SIDE_ENTRY,
                    position_mode=Config.POSITION_MODE, fee=Config.FEE_RATE,
                    gross=Config.PAIR_GROSS_NOTIONAL, start=Config.ENTRY_START,
                    min_var=Config.MIN_BTC_VARIANCE, min_std=Config.MIN_RESIDUAL_STD,
                    pandas=pd.__version__, numpy=np.__version__)
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


def backtest_single_symbol(symbol, df, market_median_series):
    """独立配对；到期退出先于指标检查；数量固定；缺失平仓价不伪造成交。"""
    trades = []
    position = None
    armed = False  # 必须先看到正常区间，才能确认首次突破。
    pending_side = 0  # +1/-1: 已突破，等待同侧首次向0回头；0: 无等待。
    previous_z, previous_time = np.nan, None
    threshold = Config.Z_SCORE_THRESHOLD
    begin, end = utc_timestamp(Config.ENTRY_START), utc_timestamp(Config.EVALUATION_END)
    hold = pd.Timedelta(hours=Config.HOLDING_PERIOD_HOURS)

    for row in df.itertuples():
        now, z = row.Index, row.z_score
        valid_z = np.isfinite(z) and np.isfinite(row.beta_shifted)
        normal = valid_z and -threshold <= z <= threshold
        turned = (previous_time is not None
                  and now - previous_time == pd.Timedelta(hours=1)
                  and right_side_turn(previous_z, z, threshold))
        previous_z, previous_time = (z if valid_z else np.nan), now

        # 绝不能因Z/Beta缺失跳过已经到期的仓位。
        if position is not None:
            if now < position["scheduled_exit_time"]:
                continue
            valid_prices = (np.isfinite(row.close) and row.close > 0
                            and np.isfinite(row.btc_close) and row.btc_close > 0)
            if now != position["scheduled_exit_time"] or not valid_prices:
                position["status"] = "UNRESOLVED_MISSING_EXIT"
                position["exit_reason"] = "MISSING_SCHEDULED_EXIT"
                trades.append(position)
                position = None
                # 仓位未能结算，停止本币后续交易；不删除样本，也不私自延期。
                break
            q_alt, q_btc = position["alt_qty"], position["btc_qty"]
            alt_pnl = q_alt * (row.close - position["entry_price"])
            btc_pnl = q_btc * (row.btc_close - position["btc_entry"])
            alt_exit, btc_exit = abs(q_alt) * row.close, abs(q_btc) * row.btc_close
            exit_cost = (alt_exit + btc_exit) * Config.FEE_RATE
            gross = alt_pnl + btc_pnl
            total_cost = position["entry_cost"] + exit_cost
            position.update(
                status="CLOSED", exit_time=now, exit_price=row.close, btc_exit=row.btc_close,
                exit_z_score=z, alt_exit_notional=alt_exit, btc_exit_notional=btc_exit,
                alt_gross_pnl=alt_pnl, btc_gross_pnl=btc_pnl, gross_pnl=gross,
                exit_cost=exit_cost, total_cost=total_cost, net_pnl=gross - total_cost,
                gross_return=gross / position["entry_gross_notional"],
                net_return=(gross - total_cost) / position["entry_gross_notional"],
                holding_hours=(now - position["entry_time"]).total_seconds() / 3600,
                exit_reason="FIXED_HOLD")
            trades.append(position)
            position = None
            armed = normal  # 退出时已正常即可复位；持仓期间的回归不算平仓后复位。
            pending_side = 0
            continue

        if not valid_z:
            armed = False  # 缺口后首次看到极值，无法确认这是首次突破。
            pending_side = 0
            continue
        if normal:
            armed = True
            pending_side = 0
            continue
        if Config.RIGHT_SIDE_ENTRY and pending_side:
            if (1 if z > 0 else -1) != pending_side:
                pending_side = 0  # 跨0跳到另一侧极值，不当成原方向的回归。
                continue
            if not turned:
                continue
            pending_side = 0  # 首次回头消耗信号；分组/区间不满足也不追单。
        else:
            if not armed:
                continue
            armed = False  # 消耗这次突破，即使区间/分组要求使本次不成交也不追单。
            if Config.RIGHT_SIDE_ENTRY:
                pending_side = 1 if z > 0 else -1
                continue
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
        direction = 1 if z < -threshold else -1
        beta = row.beta_shifted
        # Beta决定有符号BTC名义金额；abs只用于额度和费用。
        alt_notional = Config.PAIR_GROSS_NOTIONAL / (1 + abs(beta))
        btc_signed_notional = -direction * beta * alt_notional
        position = dict(
            run_id=Config.RUN_ID, trade_id=f"{symbol}_{now.isoformat()}", symbol=symbol,
            status="OPEN", entry_time=now, scheduled_exit_time=now + hold,
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
            planned_holding_hours=Config.HOLDING_PERIOD_HOURS, fee_rate=Config.FEE_RATE,
            z_threshold=threshold, beta_window_hours=Config.BETA_WINDOW_HOURS,
            signal_window_hours=Config.SIGNAL_WINDOW_HOURS,
            right_side_entry=Config.RIGHT_SIDE_ENTRY,
            position_mode=Config.POSITION_MODE, exit_reason=None)

    if position is not None:
        position["status"] = "UNRESOLVED_END_OF_DATA"
        position["exit_reason"] = "END_OF_DATA"
        trades.append(position)
    return pd.DataFrame(trades, columns=TRADE_COLUMNS)


def right_side_turn(previous_z, z, threshold):
    """仅比较已收盘的相邻小时：两端同侧超阈值，当前绝对值严格减小。"""
    return (np.isfinite(previous_z) and np.isfinite(z)
            and ((previous_z > threshold and z > threshold and z < previous_z)
                 or (previous_z < -threshold and z < -threshold and z > previous_z)))


def open_rotation_position(symbol, row, median):
    """沿用原版双腿定额/有符号Beta/冻结数量公式；轮换模式没有计划到期时间。"""
    now, z, beta = row.Index, row.z_score, row.beta_shifted
    direction = 1 if z < -Config.Z_SCORE_THRESHOLD else -1
    alt_notional = Config.PAIR_GROSS_NOTIONAL / (1 + abs(beta))
    btc_signed_notional = -direction * beta * alt_notional
    return dict(
        run_id=Config.RUN_ID, trade_id=f"{symbol}_{now.isoformat()}", symbol=symbol,
        status="OPEN", entry_time=now, scheduled_exit_time=pd.NaT,
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
        planned_holding_hours=np.nan, fee_rate=Config.FEE_RATE,
        z_threshold=Config.Z_SCORE_THRESHOLD, beta_window_hours=Config.BETA_WINDOW_HOURS,
        signal_window_hours=Config.SIGNAL_WINDOW_HOURS,
        right_side_entry=Config.RIGHT_SIDE_ENTRY,
        position_mode=Config.POSITION_MODE, exit_reason=None)


def close_rotation_position(position, now, alt_price, btc_price, z, reason):
    """沿用原版实际成交额计费；没有双腿有效平仓价时不伪造成交。"""
    if not (np.isfinite(alt_price) and alt_price > 0
            and np.isfinite(btc_price) and btc_price > 0):
        position.update(status="UNRESOLVED_MISSING_EXIT", exit_reason=reason)
        return False
    q_alt, q_btc = position["alt_qty"], position["btc_qty"]
    alt_pnl = q_alt * (alt_price - position["entry_price"])
    btc_pnl = q_btc * (btc_price - position["btc_entry"])
    alt_exit, btc_exit = abs(q_alt) * alt_price, abs(q_btc) * btc_price
    exit_cost = (alt_exit + btc_exit) * Config.FEE_RATE
    gross = alt_pnl + btc_pnl
    total_cost = position["entry_cost"] + exit_cost
    position.update(
        status="CLOSED", exit_time=now, exit_price=alt_price, btc_exit=btc_price,
        exit_z_score=z, alt_exit_notional=alt_exit, btc_exit_notional=btc_exit,
        alt_gross_pnl=alt_pnl, btc_gross_pnl=btc_pnl, gross_pnl=gross,
        exit_cost=exit_cost, total_cost=total_cost, net_pnl=gross - total_cost,
        gross_return=gross / position["entry_gross_notional"],
        net_return=(gross - total_cost) / position["entry_gross_notional"],
        holding_hours=(now - position["entry_time"]).total_seconds() / 3600,
        exit_reason=reason)
    return True


def backtest_max_deviation(symbol_indicators, btc_df, market_median_series):
    """在统一时间轴上只管理一个ALT/BTC仓位；不能逐币回测后再筛选交易。

    排名用当小时满足Z、Beta、价格、成交额基准要求的最大abs(Z)。
    右侧条件只管新开仓：新冠军尚未掉头时空仓等待，不持有次优标的。
    不要求重新穿越阈值；排名变化本身即可触发轮换。并列时优先保留旧仓，
    空仓则按symbol排序选取。最后一个评估小时结算且不再开仓。
    symbol_indicators为按symbol排序的(symbol, 指标DataFrame)惰性迭代器。
    """
    if market_median_series.empty:
        return pd.DataFrame(columns=TRADE_COLUMNS)
    index = pd.date_range(market_median_series.index.min(),
                          market_median_series.index.max(), freq="h", name="close_time")
    end = utc_timestamp(Config.EVALUATION_END)
    if end is not None:
        index = index[index <= end]
    if index.empty:
        return pd.DataFrame(columns=TRADE_COLUMNS)
    median = market_median_series.reindex(index).to_numpy()
    btc_prices = btc_df["close"].reindex(index).to_numpy()
    n, threshold = len(index), Config.Z_SCORE_THRESHOLD
    best_score = np.full(n, -np.inf)
    best_symbol = np.full(n, "", dtype=object)
    best_right = np.zeros(n, dtype=bool)
    entry_columns = ("close", "z_score", "beta_shifted", "hist_res_mean",
                     "hist_res_std", "avg_turnover_30d")
    best_values = {key: np.full(n, np.nan) for key in entry_columns}
    histories = {}

    # 逐币生成指标，只保留各币平仓/排名所需的3条数组及每小时冠军的开仓字段。
    # 全样本预计算不使用未来值：排名数组的每一行仅比较同一个收盘时间。
    for symbol, indicators in symbol_indicators:
        frame = indicators.loc[:, list(entry_columns)].reindex(index)
        z = frame["z_score"].to_numpy(copy=True)
        close = frame["close"].to_numpy(copy=True)
        beta = frame["beta_shifted"].to_numpy()
        turnover = frame["avg_turnover_30d"].to_numpy()
        valid = (np.isfinite(z) & (np.abs(z) > threshold) & np.isfinite(beta)
                 & np.isfinite(close) & (close > 0)
                 & np.isfinite(btc_prices) & (btc_prices > 0)
                 & np.isfinite(turnover) & np.isfinite(median))
        scores = np.where(valid, np.abs(z), -np.inf)
        prev_z = np.r_[np.nan, z[:-1]]
        prev_beta = np.r_[np.nan, beta[:-1]]
        right = (np.isfinite(prev_z) & np.isfinite(prev_beta)
                 & (((prev_z > threshold) & (z > threshold) & (z < prev_z))
                    | ((prev_z < -threshold) & (z < -threshold) & (z > prev_z))))
        # 相同分数取字典序小者；持仓并列优先在逐小时循环中处理。
        better = (scores > best_score) | (
            np.isfinite(scores) & (scores == best_score) & (symbol < best_symbol))
        best_score[better] = scores[better]
        best_symbol[better] = symbol
        best_right[better] = right[better]
        for key in entry_columns:
            best_values[key][better] = frame[key].to_numpy()[better]
        histories[symbol] = (close, z, scores)

    winners = pd.DataFrame(best_values, index=index)
    winners["btc_close"] = btc_prices
    winners["symbol"] = best_symbol
    winners["right_ready"] = best_right
    trades, position = [], None
    begin = utc_timestamp(Config.ENTRY_START)
    for i, row in enumerate(winners.itertuples()):
        now = row.Index
        if begin is not None and now < begin:
            continue
        final_bar = i == n - 1
        has_candidate = np.isfinite(best_score[i])
        if position is not None:
            prices, z_values, scores = histories[position["symbol"]]
            same_direction = (
                (position["direction"] == "LONG_ALT" and z_values[i] < -threshold)
                or (position["direction"] == "SHORT_ALT" and z_values[i] > threshold))
            still_largest = has_candidate and scores[i] == best_score[i]
            if not final_bar and still_largest and same_direction:
                continue
            reason = ("END_OF_BACKTEST" if final_bar else
                      "NO_ELIGIBLE_PAIR" if not has_candidate else
                      "DIRECTION_CHANGED" if still_largest and not same_direction else
                      "RANK_CHANGED")
            settled = close_rotation_position(
                position, now, prices[i], btc_prices[i], z_values[i], reason)
            trades.append(position)
            position = None
            if not settled:
                # 旧仓未平，不能假装空仓后再开新仓；停止整个单仓组合。
                break
        if final_bar or not has_candidate:
            continue
        if Config.RIGHT_SIDE_ENTRY and not row.right_ready:
            continue
        position = open_rotation_position(row.symbol, row, median[i])
    return pd.DataFrame(trades, columns=TRADE_COLUMNS)


def process_max_deviation(symbols, btc_df, market_median_series):
    output_csv = result_path("portfolio_trades.csv")
    if os.path.exists(output_csv):
        columns = pd.read_csv(output_csv, nrows=0).columns.tolist()
        if columns != TRADE_COLUMNS:
            raise ValueError(f"缓存字段不匹配，请删除后重跑: {output_csv}")
        return

    def iter_indicators():
        for symbol in tqdm(sorted(set(symbols))):
            if symbol != Config.BTC_SYMBOL:
                yield symbol, calculate_indicators(load_kline(symbol), btc_df)

    records = backtest_max_deviation(iter_indicators(), btc_df, market_median_series)
    atomic_csv(records, output_csv, index=False)


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


def run_all_backtests(symbols, data_fingerprints=None, prepared=False):
    """运行所有币种回测。源CSV应为运行期间不变的静态快照。"""
    symbols = sorted(set(symbols))
    if not prepared:
        prepare_run(symbols, data_fingerprints)
    print("加载 BTC 基准数据...")
    btc_df = load_kline(Config.BTC_SYMBOL)
    median = generate_market_median(symbols, btc_df)
    if Config.POSITION_MODE == "MAX_DEVIATION":
        print(f"开始全市场单仓轮换，共 {len(symbols)} 个币种；输出: {Config.OUTPUT_DIR}")
        process_max_deviation(symbols, btc_df, median)
        return
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
    hold_label = f"{Config.HOLDING_PERIOD_HOURS}h" if Config.POSITION_MODE == "FIXED_HOLD" else "按排名轮换"
    print(f"【参数组合评估】 Z: {Config.Z_SCORE_THRESHOLD} | 持仓: {hold_label} | Beta: {Config.BETA_WINDOW_DAYS}d | 信号: {Config.SIGNAL_WINDOW_HOURS}h | 右侧: {Config.RIGHT_SIDE_ENTRY} | 模式: {Config.POSITION_MODE}")
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
    quadrants = df.groupby(["vol_group", "direction"]).agg(
        trade_count=("symbol", "count"),
        win_rate=("net_pnl", lambda x: (x > 0).mean()),
        avg_net_return=("net_return", "mean"),
        median_net_return=("net_return", "median"),
        avg_net_pnl=("net_pnl", "mean"),
        sum_net_pnl=("net_pnl", "sum"),
        avg_cost=("total_cost", "mean")
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
    """轮换模式去掉无意义的H维度；重复配置只运行一次。"""
    import itertools
    grid = []
    seen = set()
    for z, h, b, s, right, mode in itertools.product(
            Config.Z_THRESHOLDS_TO_TEST, Config.HOLDING_PERIODS_TO_TEST,
            Config.BETA_WINDOWS_TO_TEST, Config.SIGNAL_WINDOWS_TO_TEST,
            Config.RIGHT_SIDE_ENTRIES_TO_TEST, Config.POSITION_MODES_TO_TEST):
        params = (z, h if mode == "FIXED_HOLD" else Config.HOLDING_PERIOD_HOURS,
                  b, s, right, mode)
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
        z, h, b, s, right, mode = params
        Config.update_params(z, h, b, s, right, mode)
        prepare_run(symbols, data_fingerprints)
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
    for name in ("MAX_WORKERS", "NUMERIC_THREADS_PER_WORKER"):
        value = getattr(Config, name)
        if isinstance(value, bool) or int(value) != value or value <= 0:
            raise ValueError(f"{name}必须为正整数")
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
                if result["status"] == "OK":
                    print(f"✅ [{done}/{len(param_grid)}] {result['params']}")
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
    from common.common_utils import read_json

    SYMBOLS = read_json(Config.SYMBOLS_FILE)
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
        s: file_sha256(kline_path(s)) for s in SYMBOLS + [Config.BTC_SYMBOL]
    }

    run_parameter_grid(SYMBOLS, DATA_FINGERPRINTS)
