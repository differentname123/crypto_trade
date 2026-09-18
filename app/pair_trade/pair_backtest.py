# -- coding: utf-8 --
""":authors:
    zhuxiaohu, AI Assistant
:create_date:
    2026/9/18
:description:
    基于Beta调整与固定期限的横截面统计套利策略 (修正版)
    低耦合架构：先逐个币种回测并保存交易明细 -> 最后汇总分析
    支持多参数组合自动网格搜索，并动态打印参数标识

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
from tqdm import tqdm


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
    FEE_RATE = 0.001  # 每腿每次实际成交额的0.1%，已包含全部成本
    PAIR_GROSS_NOTIONAL = 1000.0  # 每笔双腿初始毛名义总额，USDT
    MIN_BTC_VARIANCE = 1e-16
    MIN_RESIDUAL_STD = 1e-10

    # 可指定开仓评估区间，UTC，左闭右开。结束前不足完整持有期不再开仓。
    # 不填时使用文件覆盖范围；不会提前查看未来价格来决定是否开仓。
    # 网格搜索只在研究/决策区间运行；留出期应另设区间并仅运行已冻结参数。
    ENTRY_START = None  # 例如 "2024-01-01"
    EVALUATION_END = None  # 例如 "2025-01-01"；允许恰在此边界平仓

    Z_THRESHOLDS_TO_TEST = [2.0, 3.0, 3.5]
    HOLDING_PERIODS_TO_TEST = [6, 12, 24]
    CACHE_VERSION = "fixed_beta_independent_v1"

    BETA_WINDOW_HOURS = BETA_WINDOW_DAYS * 24
    PARAM_FOLDER = f"Z{Z_SCORE_THRESHOLD}_H{HOLDING_PERIOD_HOURS}_B{BETA_WINDOW_DAYS}_S{SIGNAL_WINDOW_HOURS}"
    OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, PARAM_FOLDER)
    RUN_ID = ""
    MARKET_ID = ""

    @classmethod
    def update_params(cls, z_score, holding_period, beta_window=30, signal_window=24):
        """动态更新参数并重建路径配置；实际运行再绑定数据/代码指纹。"""
        if (not np.isfinite(z_score) or z_score <= 0
                or any(int(x) != x or x <= 0 for x in
                       (holding_period, beta_window, signal_window))):
            raise ValueError("阈值必须为正数，时间窗口必须为正整数")
        cls.Z_SCORE_THRESHOLD = float(z_score)
        cls.HOLDING_PERIOD_HOURS = int(holding_period)
        cls.BETA_WINDOW_DAYS = int(beta_window)
        cls.SIGNAL_WINDOW_HOURS = int(signal_window)
        cls.BETA_WINDOW_HOURS = cls.BETA_WINDOW_DAYS * 24
        if cls.BETA_WINDOW_HOURS <= cls.SIGNAL_WINDOW_HOURS:
            raise ValueError("Beta历史窗口必须长于信号窗口")
        cls.PARAM_FOLDER = f"Z{cls.Z_SCORE_THRESHOLD}_H{cls.HOLDING_PERIOD_HOURS}_B{cls.BETA_WINDOW_DAYS}_S{cls.SIGNAL_WINDOW_HOURS}"
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
    "signal_window_hours"
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


def atomic_csv(df, path, **kwargs):
    temp = path + ".tmp"
    df.to_csv(temp, **kwargs)
    os.replace(temp, path)


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
                    holding=Config.HOLDING_PERIOD_HOURS, fee=Config.FEE_RATE,
                    gross=Config.PAIR_GROSS_NOTIONAL, start=Config.ENTRY_START,
                    min_var=Config.MIN_BTC_VARIANCE, min_std=Config.MIN_RESIDUAL_STD,
                    pandas=pd.__version__, numpy=np.__version__)
    Config.RUN_ID = fingerprint(manifest)
    Config.OUTPUT_DIR = os.path.join(Config.BASE_OUTPUT_DIR, Config.PARAM_FOLDER + "_" + Config.RUN_ID[:16])
    os.makedirs(Config.OUTPUT_DIR, exist_ok=True)
    path = os.path.join(Config.OUTPUT_DIR, "run_manifest.json")
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    os.replace(path + ".tmp", path)

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
    atomic_csv(series, cache_path, header=True, index_label="close_time")
    return series


def backtest_single_symbol(symbol, df, market_median_series):
    """独立配对；到期退出先于指标检查；数量固定；缺失平仓价不伪造成交。"""
    trades = []
    position = None
    armed = False  # 必须先看到正常区间，才能确认首次突破。
    threshold = Config.Z_SCORE_THRESHOLD
    begin, end = utc_timestamp(Config.ENTRY_START), utc_timestamp(Config.EVALUATION_END)
    hold = pd.Timedelta(hours=Config.HOLDING_PERIOD_HOURS)

    for row in df.itertuples():
        now, z = row.Index, row.z_score
        valid_z = np.isfinite(z) and np.isfinite(row.beta_shifted)
        normal = valid_z and -threshold <= z <= threshold

        # 绝不能因Z/Beta缺失跳过已经到期的仓位。
        if position is not None:
            if now < position["scheduled_exit_time"]:
                continue
            valid_prices = (np.isfinite(row.close) and row.close > 0
                            and np.isfinite(row.btc_close) and row.btc_close > 0)
            if now != position["scheduled_exit_time"] or not valid_prices:
                position["status"] = "UNRESOLVED_MISSING_EXIT"
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
                holding_hours=(now - position["entry_time"]).total_seconds() / 3600)
            trades.append(position)
            position = None
            armed = normal  # 退出时已正常即可复位；持仓期间的回归不算平仓后复位。
            continue

        if not valid_z:
            armed = False  # 缺口后首次看到极值，无法确认这是首次突破。
            continue
        if normal:
            armed = True
            continue
        if not armed:
            continue
        armed = False  # 消耗这次突破，即使区间/分组要求使本次不成交也不追单。
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
            signal_window_hours=Config.SIGNAL_WINDOW_HOURS)

    if position is not None:
        position["status"] = "UNRESOLVED_END_OF_DATA"
        trades.append(position)
    return pd.DataFrame(trades, columns=TRADE_COLUMNS)


# ==========================================
# 4. 主控调度流程
# ==========================================
def process_symbol(symbol, btc_df, market_median_series):
    """成功的零交易也保存表头；异常不落成功缓存；失败时整组不汇总。"""
    output_csv = os.path.join(Config.OUTPUT_DIR, f"{symbol}_trades.csv")
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


def run_all_backtests(symbols, data_fingerprints=None):
    """运行所有币种回测。源CSV应为运行期间不变的静态快照。"""
    symbols = sorted(set(symbols))
    prepare_run(symbols, data_fingerprints)
    print("加载 BTC 基准数据...")
    btc_df = load_kline(Config.BTC_SYMBOL)
    median = generate_market_median(symbols, btc_df)
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
    print(f"【参数组合评估】 Z: {Config.Z_SCORE_THRESHOLD} | 持仓: {Config.HOLDING_PERIOD_HOURS}h | Beta: {Config.BETA_WINDOW_DAYS}d | 信号: {Config.SIGNAL_WINDOW_HOURS}h")
    print("【独立配对交易样本统计；不是共享资金账户收益】")
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
    atomic_csv(quadrants, os.path.join(Config.OUTPUT_DIR, "quadrant_summary.csv"), index=False)
    print("未计算组合年化/夏普/最大回撤：需要共享资金分配及逐小时盯市账本。")


# ==========================================
# 启动入口 (支持自动化网格搜索)
# ==========================================
# ==========================================
# 启动入口 (支持自动化网格搜索)
# ==========================================
if __name__ == "__main__":
    from common.common_utils import read_json

    SYMBOLS = read_json(Config.SYMBOLS_FILE)
    if not isinstance(SYMBOLS, list) or not all(isinstance(s, str) for s in SYMBOLS):
        raise ValueError("symbols.json应为币种字符串列表")

    # 【新增修改】：在开始计算指纹之前，提前检查并自动过滤掉缺失的文件
    valid_symbols = []
    for s in set(SYMBOLS):
        if os.path.isfile(kline_path(s)):
            valid_symbols.append(s)
        else:
            print(f"⚠️ 自动跳过: 未找到 {s} 的K线文件。")
    SYMBOLS = sorted(valid_symbols)

    # 检查基础币种文件是否存在（因为如果没有BTC数据，所有币都无法算对冲收益）
    if not os.path.isfile(kline_path(Config.BTC_SYMBOL)):
        raise FileNotFoundError(f"❌ 核心错误: 无法找到基准币种 {Config.BTC_SYMBOL} 的文件，停止运行。")

    # 静态源文件只在本次网格搜索开始时哈希一次；再次启动会重新验证。
    print(f"计算源数据指纹 (当前有效币种数: {len(SYMBOLS)})...")
    DATA_FINGERPRINTS = {
        s: file_sha256(kline_path(s)) for s in SYMBOLS + [Config.BTC_SYMBOL]
    }

    for z in Config.Z_THRESHOLDS_TO_TEST:
        for h in Config.HOLDING_PERIODS_TO_TEST:
            Config.update_params(z_score=z, holding_period=h,
                                 beta_window=Config.BETA_WINDOW_DAYS,
                                 signal_window=Config.SIGNAL_WINDOW_HOURS)
            print("\n" + "*" * 60)
            print(f"🚀 正在执行网格搜索组合: Z_Score = {z}, Holding_Hours = {h}")
            print("*" * 60)
            run_all_backtests(SYMBOLS, DATA_FINGERPRINTS)
            analyze_results()