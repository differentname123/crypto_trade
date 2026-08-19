# =============================================================================
# [功能摘要]
#   加密合约多策略「实盘信号引擎」：拉取行情/资金费率/持仓量(OI)，推演出当下这一刻应该
#   下发的开平仓指令，并把真实成交账本持久化到本地 CSV（跨进程续跑的持仓状态机）。
#   含两条独立主干：A) 4H 横截面动量组合(多币轮动)；B) 6 个单标的截面瞬时策略。
#
# [输入数据]
#   · snipe_kline_data(symbol_list, timeframe, days, ...) -> {symbol: DataFrame}
#       DataFrame 关键列: timestamp(ms,UTC), open, high, low, close, volume
#   · snipe_funding_rate_data(symbol_list, days, ...)      -> {symbol: DataFrame}
#       关键列: timestamp|fundingTime, funding_rate|fundingRate|rate
#   · snipe_oi_data(symbol_list, timeframe, days, ...)     -> {symbol: DataFrame}
#       关键列: timestamp|time, oi_amount|openInterest|sumOpenInterest
#   · 本地历史信号文件 signal_history_<策略名>.csv（跨进程持仓状态来源）
#
# [数据流转/交互]
#   主干A(cross): 1m K线 --重采样(4h+offset)--> 每币 open/high/low/close 4 列
#                --横向concat + 全币公共区间截断 + ffill--> 4H 截面矩阵
#                --run_strategy_simulation(numpy 状态机: BTC均线开关定方向 → 风险调整动量
#                  排序取TopK → 平掉出局仓位 → 按 1/波动率 分配权重开仓)--> 交易账本
#                --时间+4h(K线走完才执行) → 生成UTC毫秒戳 + 北京时间列--> 匹配最新截面发单
#   主干B(截面): 原始K线 --重采样 + FR/OI 前向对齐 + 三源公共起点截断--> 单标的特征表
#                --只算最后一根闭合K线的入/出场布尔--> 候选 record
#                --_sync_persistent_signal_ledger(读本地CSV判定当前是否持仓, 防重复开仓/
#                  防无效平仓, 时间戳去重, 补PnL, 追加落盘)--> 该标的真实信号账本
#
# [输出数据]
#   · 返回值: 全量信号/账本 DataFrame（无信号时为空 DataFrame）
#   · 副作用: 写出 live_simulation_logs.csv / <策略>_signals.csv /
#             signal_history_<策略>.csv；并向 logger 输出结构化的发单指令与体检报告
# =============================================================================

import os
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from common_utils_lite import setup_logger
from fetch_data_quick import snipe_kline_data, snipe_funding_rate_data, snipe_oi_data

# 所有截面策略信号账本的统一列序（下游 CSV / 状态机均依赖此顺序）
SIGNAL_COLS = ['time', 'action', 'coin', 'direction', 'event', 'price',
               'reason', 'target_weight', 'pnl', 'top_k', 'max_weight',
               'signal_timestamp_ms', 'STRATEGY_NAME', 'symbol']


# =============================================================================
# 一、通用小工具（时间 / 列名 / 换算 / 取值）
# =============================================================================
def _fmt_bjt(value):
    """把 UTC 毫秒戳或 UTC 时间对象统一格式化为北京时间字符串，供人类阅读。"""
    if isinstance(value, (int, float, np.integer, np.floating)):
        ts = pd.to_datetime(int(value), unit='ms', utc=True)
    else:
        ts = pd.Timestamp(value)
        ts = ts.tz_localize('UTC') if ts.tzinfo is None else ts
    return ts.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')


def _pick_column(df, candidates, tag):
    """在多种交易所字段命名中挑出第一个可用列名；全都找不到直接报错并暴露真实列。"""
    for col in candidates:
        if col in df.columns:
            return col
    raise KeyError(f"[{tag}] 找不到列 {candidates}，实际列: {list(df.columns)}")


def _bars(hours, bar_minutes):
    """小时数 -> 对应 K 线根数（至少 1 根）。"""
    return max(1, int(round(hours * (60.0 / bar_minutes))))


def _tail_float(series, offset=0, default=0.0):
    """取倒数第 (1+offset) 个值转 float；越界或 NaN 时回落默认值（对齐原代码的 NaN 兜底）。"""
    if len(series) <= offset:
        return default
    val = series.iloc[-1 - offset]
    return default if pd.isna(val) else float(val)


def _frame_of(result_map, symbol):
    """从极速引擎返回的字典里安全取出某标的数据，缺失统一返回空 DataFrame。"""
    df = result_map.get(symbol) if result_map else None
    return df if df is not None else pd.DataFrame()


def _resolve_identity(df):
    """提取标的身份：优先取列，其次取 attrs，最后从 symbol 反解币名。返回 (symbol, coin_name)。"""
    symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else df.attrs.get('symbol', 'UNKNOWN')
    if 'coin_name' in df.columns:
        return symbol, df['coin_name'].iloc[0]
    return symbol, (symbol.split('/')[0] if '/' in symbol else symbol)


# =============================================================================
# 二、行情重采样与多源对齐（K线 + 资金费率 + OI 合成单表）
# =============================================================================
def _attach_series(df, src_df, time_keys, value_keys, out_col, bar, tag):
    """把外部时间序列(FR/OI)重采样后前向对齐进主表，写入 out_col。"""
    src = src_df.copy()
    tcol = _pick_column(src, time_keys, tag)
    vcol = _pick_column(src, value_keys, tag)
    src['dt'] = pd.to_datetime(src[tcol], unit='ms', utc=True)
    series = (src.drop_duplicates(subset=[tcol]).sort_values('dt')
                 .set_index('dt')[vcol].astype(float)
                 .resample(bar, label='left', closed='left').last())
    df[out_col] = series.reindex(df.index).ffill()


def _build_aligned_frame(kline_df, bar_minutes, fr_df=None, oi_df=None):
    """
    构建策略特征底表：K线重采样 + FR/OI 前向对齐 + 多源公共起点截断。

    入参形貌: kline_df 需含 [timestamp|open_time|time|ts, open, high, low, close, volume]
             fr_df 需含 [timestamp|fundingTime|..] + [funding_rate|fundingRate|rate]
             oi_df 需含 [timestamp|time|ts] + [oi_amount|openInterest|sumOpenInterest|oi]
    出参形貌: DataFrame(index=UTC DatetimeIndex,
                       cols=[open, high, low, close, volume, (funding_rate), (oi_amount)])
    """
    bar = f"{bar_minutes}min"
    k = kline_df.copy()
    kt = _pick_column(k, ['timestamp', 'open_time', 'time', 'ts'], 'kline')
    k['dt'] = pd.to_datetime(k[kt], unit='ms', utc=True)
    k = k.drop_duplicates(subset=[kt]).sort_values('dt').set_index('dt')

    df = k.resample(bar, label='left', closed='left').agg(
        open=('open', 'first'), high=('high', 'max'),
        low=('low', 'min'), close=('close', 'last'),
        volume=('volume', 'sum')
    )
    # 空洞 bar 的 close 用前值补齐，OHL 再回落到 close，保证 ATR/最低价类指标不被 NaN 击穿
    df['close'] = df['close'].ffill()
    df = df[df['close'].notna()]
    for col in ('open', 'high', 'low'):
        df[col] = df[col].fillna(df['close'])

    extra_cols = []
    if fr_df is not None and len(fr_df) > 0:
        _attach_series(df, fr_df, ['timestamp', 'fundingTime', 'time', 'ts'],
                       ['funding_rate', 'fundingRate', 'rate'], 'funding_rate', bar, 'fr')
        extra_cols.append('funding_rate')
    if oi_df is not None and len(oi_df) > 0:
        _attach_series(df, oi_df, ['timestamp', 'time', 'ts'],
                       ['oi_amount', 'openInterest', 'open_interest', 'sumOpenInterest', 'oi'],
                       'oi_amount', bar, 'oi')
        extra_cols.append('oi_amount')

    if extra_cols:
        # 以最晚出现的那一路数据为公共起点（原实现在全 NaN 时会因 df.loc[NaN:] 崩溃，此处已修）
        starts = [s for s in (df[c].first_valid_index() for c in extra_cols) if s is not None]
        if starts:
            df = df.loc[max(starts):].copy()
        df = df.dropna(subset=extra_cols)

    return df[df['close'] > 0]


# =============================================================================
# 三、信号账本持久化状态机（跨进程续跑的唯一真相来源）
# =============================================================================
def _sync_persistent_signal_ledger(history_file, symbol, new_record, cols):
    """
    读本地历史信号 -> 用"上一条事件"约束状态机(防重复开仓/防无效平仓) -> 去重落盘 -> 回吐该标的完整账本。

    入参形貌: new_record 需含 SIGNAL_COLS 全部键（尤其 event/price/signal_timestamp_ms），可为 None
    出参形貌: DataFrame(cols=SIGNAL_COLS)，仅含该 symbol 的真实信号（含本次新增）
    """
    df_hist = pd.DataFrame(columns=cols)
    if os.path.exists(history_file):
        try:
            df_hist = pd.read_csv(history_file, dtype={'signal_timestamp_ms': 'int64', 'symbol': str})
        except Exception:
            # 原设计即容错吞噬：历史文件损坏时按空账本重来，避免脏文件阻断整条实盘信号链路
            df_hist = pd.DataFrame(columns=cols)

    for c in cols:
        if c not in df_hist.columns:
            df_hist[c] = None

    df_symbol_hist = df_hist[df_hist['symbol'] == symbol].copy()

    last_event, last_open_price = None, 0.0
    if not df_symbol_hist.empty:
        last_row = df_symbol_hist.iloc[-1]
        last_event = str(last_row['event']).upper()
        last_open_price = float(last_row['price']) if pd.notna(last_row['price']) else 0.0

    # 状态机：空仓才接 OPEN；持仓才接 CLOSE 并补算盈亏
    valid_record = None
    if new_record is not None:
        event = new_record['event']
        if event == 'OPEN' and last_event != 'OPEN':
            valid_record = new_record
        elif event == 'CLOSE' and last_event == 'OPEN':
            new_record['pnl'] = (((new_record['price'] - last_open_price) / last_open_price) * 100
                                 if last_open_price > 0 else 0.0)
            valid_record = new_record

    if valid_record is not None:
        is_duplicate = (not df_hist.empty) and bool(
            ((df_hist['symbol'] == symbol) &
             (df_hist['signal_timestamp_ms'] == valid_record['signal_timestamp_ms'])).any())
        if not is_duplicate:
            df_new_row = pd.DataFrame([valid_record], columns=cols)
            df_hist = pd.concat([df_hist, df_new_row], ignore_index=True)
            df_hist.to_csv(history_file, index=False, encoding='utf-8-sig')
            df_symbol_hist = pd.concat([df_symbol_hist, df_new_row], ignore_index=True)

    return df_symbol_hist[cols] if not df_symbol_hist.empty else pd.DataFrame(columns=cols)


def _build_record(strategy_name, symbol, coin, event, direction, price, reason,
                  signal_ts_ms, target_weight, max_weight):
    """
    组装单条标准信号记录。action 由 (direction, event) 唯一推导：
    LONG+OPEN=BUY / LONG+CLOSE=SELL / SHORT+OPEN=SELL / SHORT+CLOSE=BUY
    """
    action = 'BUY' if (direction == 'LONG') == (event == 'OPEN') else 'SELL'
    return {'time': _fmt_bjt(signal_ts_ms), 'action': action, 'coin': coin, 'direction': direction,
            'event': event, 'price': price, 'reason': reason, 'target_weight': target_weight,
            'pnl': None, 'top_k': 1, 'max_weight': max_weight,
            'signal_timestamp_ms': signal_ts_ms, 'STRATEGY_NAME': strategy_name, 'symbol': symbol}


# =============================================================================
# 四、各策略「最后一根 K 线」信号生成器
#     统一约定: 入参为该标的原始 K线(+FR/+OI)，出参 ([], DataFrame(SIGNAL_COLS))
#     首元素恒为空列表，仅为兼容既有调用方 `signals, df = fn(...)` 的解包写法
# =============================================================================
def generate_top_long_signals(df):
    """
    top_coin_long：小时线「高位长上影 + 爆量」入场，「孕线突破 + 爆量」出场。
    入参形貌: df 需含 [timestamp(ms), open, high, low, close, volume, symbol, coin_name]
    """
    P = {'BAR_MINUTES': 60, 'UPPER_WICK_THRESH': 0.60, 'VOL_QUANTILE': 0.9,
         'HIGH_CLOSE_THRESH': 0.90, 'WARMUP_DAYS': 30}
    W, N, EPS = 24 * P['WARMUP_DAYS'], 24, 1e-12

    if df is None or len(df) < W:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    symbol, coin_name = _resolve_identity(df)
    o, h, l, c, v = df['open'], df['high'], df['low'], df['close'], df['volume']

    max_high_n = h.rolling(N, min_periods=max(2, N // 2)).max()
    upper_wick = (h - np.maximum(o, c)) / ((h - l) + EPS)
    is_inside_bar = (h < h.shift(1)) & (l > l.shift(1))
    vol_threshold = v.rolling(W, min_periods=50).quantile(P['VOL_QUANTILE']).shift(1)
    volume_spike = v > vol_threshold

    entry_signal = (c / (max_high_n + EPS) > P['HIGH_CLOSE_THRESH']) & (upper_wick > P['UPPER_WICK_THRESH']) & volume_spike
    exit_signal = is_inside_bar.shift(1, fill_value=False) & (c > h.shift(1)) & volume_spike

    is_entry, is_exit = bool(entry_signal.iloc[-1]), bool(exit_signal.iloc[-1])
    record = None
    if is_entry or is_exit:
        signal_ts_ms = int(df['timestamp'].iloc[-1]) + P['BAR_MINUTES'] * 60 * 1000
        price, cur_v = float(c.iloc[-1]), float(v.iloc[-1])
        cur_vq, cur_uw = float(vol_threshold.iloc[-1]), float(upper_wick.iloc[-1])
        if is_entry:
            record = _build_record('top_coin_long', symbol, coin_name, 'OPEN', 'LONG', price,
                                   f"高位长上影({cur_uw:.2f}) + 爆量({cur_v:.0f} > {cur_vq:.0f})",
                                   signal_ts_ms, 1.0, 0.14)
        else:
            record = _build_record('top_coin_long', symbol, coin_name, 'CLOSE', 'LONG', price,
                                   f"孕线突破 + 爆量({cur_v:.0f} > {cur_vq:.0f})",
                                   signal_ts_ms, 0.0, 0.14)

    return [], _sync_persistent_signal_ledger("signal_history_top_coin_long.csv", symbol, record, SIGNAL_COLS)


def generate_multi_ma_signals(raw_df, bar_minutes=5):
    """
    multi_ma_break_long：价格同时跌破 24/48/72 小时均线入场，快慢均线死叉出场。
    入参形貌: raw_df 需含 [timestamp(ms), close, symbol, coin_name]
    """
    P = {'ENTRY_MA_HOURS': [24, 48, 72], 'EXIT_FAST_MA_HOURS': 48, 'EXIT_SLOW_MA_HOURS': 168,
         'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 0.14}

    if raw_df is None or len(raw_df) == 0:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    symbol, coin_name = _resolve_identity(raw_df)
    df = raw_df
    if 'timestamp' in df.columns and not df['timestamp'].is_monotonic_increasing:
        df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')

    c = df['close']
    slow_bars = _bars(P['EXIT_SLOW_MA_HOURS'], bar_minutes)
    if len(c) < slow_bars:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    def ma(hours):
        n = _bars(hours, bar_minutes)
        return c.rolling(n, min_periods=max(2, n // 2)).mean()

    h1, h2, h3 = P['ENTRY_MA_HOURS']
    ma1, ma2, ma3 = ma(h1), ma(h2), ma(h3)
    ma_fast, ma_slow = ma2, ma(P['EXIT_SLOW_MA_HOURS'])

    price = float(c.iloc[-1])
    v1, v2, v3 = float(ma1.iloc[-1]), float(ma2.iloc[-1]), float(ma3.iloc[-1])
    curr_fast, prev_fast = float(ma_fast.iloc[-1]), float(ma_fast.iloc[-2])
    curr_slow, prev_slow = float(ma_slow.iloc[-1]), float(ma_slow.iloc[-2])

    is_entry = (price < v1) and (price < v2) and (price < v3)
    is_exit = (curr_fast < curr_slow) and (prev_fast >= prev_slow)

    record = None
    if is_entry or is_exit:
        signal_ts_ms = int(df['timestamp'].iloc[-1]) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record('multi_ma_break_long', symbol, coin_name, 'OPEN', 'LONG', price,
                                   f"均线跌破(C<{v1:.4f}, C<{v2:.4f}, C<{v3:.4f})",
                                   signal_ts_ms, P['TARGET_WEIGHT'], P['MAX_WEIGHT'])
        else:
            record = _build_record('multi_ma_break_long', symbol, coin_name, 'CLOSE', 'LONG', price,
                                   f"快慢死叉(MA{P['EXIT_FAST_MA_HOURS']} < MA{P['EXIT_SLOW_MA_HOURS']})",
                                   signal_ts_ms, 0.0, P['MAX_WEIGHT'])

    return [], _sync_persistent_signal_ledger("signal_history_multi_ma_break_long.csv", symbol, record, SIGNAL_COLS)


def generate_XSR_signals(kline_df, fr_df, bar_minutes=15):
    """
    XSR：4 小时收益率排名冲到历史极值(>98%)追高做多；资金费率排名极低或转负时平仓。
    入参形貌: kline_df=原始K线, fr_df=资金费率(见 _build_aligned_frame 说明)
    """
    P = {'M_HOURS': 4, 'W_DAYS': 14, 'ENTRY_RANK_THRESHOLD': 0.98, 'EXIT_FR_RANK_THRESHOLD': 0.20,
         'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 30 / 2.1, 'STRATEGY_NAME': 'XSR'}

    if kline_df is None or len(kline_df) == 0 or fr_df is None or len(fr_df) == 0:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    symbol, coin_name = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df)

    W = _bars(P['W_DAYS'] * 24, bar_minutes)
    if len(df) < W:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    M, mp = _bars(P['M_HOURS'], bar_minutes), max(50, W // 5)
    c, fr_series = df['close'], df['funding_rate']
    rk_ret_m = c.pct_change(M).rolling(W, min_periods=mp).rank(pct=True)
    fr_rank = fr_series.rolling(W, min_periods=mp).rank(pct=True)

    curr_rk, curr_fr_rank = float(rk_ret_m.iloc[-1]), float(fr_rank.iloc[-1])
    curr_fr, price = float(fr_series.iloc[-1]), float(c.iloc[-1])

    is_entry = curr_rk > P['ENTRY_RANK_THRESHOLD']
    is_exit = (curr_fr_rank < P['EXIT_FR_RANK_THRESHOLD']) or (curr_fr < 0)

    record = None
    if is_entry or is_exit:
        signal_ts_ms = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'OPEN', 'LONG', price,
                                   f"SURGE_EXTREME(rk_ret_M: {curr_rk:.3f} > {P['ENTRY_RANK_THRESHOLD']})",
                                   signal_ts_ms, P['TARGET_WEIGHT'], P['MAX_WEIGHT'])
        else:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'CLOSE', 'LONG', price,
                                   f"FR_LOW_NEG(fr_rank: {curr_fr_rank:.3f} < {P['EXIT_FR_RANK_THRESHOLD']} "
                                   f"or fr: {curr_fr:.4%}<0)",
                                   signal_ts_ms, 0.0, P['MAX_WEIGHT'])

    history_file = f"signal_history_{P['STRATEGY_NAME']}.csv"
    return [], _sync_persistent_signal_ledger(history_file, symbol, record, SIGNAL_COLS)


def generate_short_fr_signals(kline_df, fr_df, bar_minutes=15):
    """
    fr_short：资金费率排名极高(>95%，多头最拥挤)时做空；24h 收益强势而费率回归温和时平空。
    入参形貌: kline_df=原始K线, fr_df=资金费率
    """
    P = {'N_HOURS': 24, 'W_DAYS': 14, 'EXTREME_FR_RANK_THRESHOLD': 0.95,
         'STRONG_RET_RANK_THRESHOLD': 0.80, 'MILD_FR_RANK_THRESHOLD': 0.50,
         'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 7 / 1.6, 'STRATEGY_NAME': 'fr_short'}

    if kline_df is None or len(kline_df) == 0 or fr_df is None or len(fr_df) == 0:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    symbol, coin_name = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df)

    W = _bars(P['W_DAYS'] * 24, bar_minutes)
    if len(df) < W:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    N, mp = _bars(P['N_HOURS'], bar_minutes), max(50, W // 5)
    c, fr_series = df['close'], df['funding_rate']
    rk_ret_n = c.pct_change(N).rolling(W, min_periods=mp).rank(pct=True)
    fr_rank = fr_series.rolling(W, min_periods=mp).rank(pct=True)

    curr_rk_ret, curr_fr_rank, price = float(rk_ret_n.iloc[-1]), float(fr_rank.iloc[-1]), float(c.iloc[-1])
    is_entry = curr_fr_rank > P['EXTREME_FR_RANK_THRESHOLD']
    is_exit = (curr_rk_ret > P['STRONG_RET_RANK_THRESHOLD']) and (curr_fr_rank < P['MILD_FR_RANK_THRESHOLD'])

    record = None
    if is_entry or is_exit:
        signal_ts_ms = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'OPEN', 'SHORT', price,
                                   f"EXTREME_HIGH_FR(fr_rank:{curr_fr_rank:.3f}>{P['EXTREME_FR_RANK_THRESHOLD']})",
                                   signal_ts_ms, P['TARGET_WEIGHT'], P['MAX_WEIGHT'])
        else:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'CLOSE', 'SHORT', price,
                                   f"COLD_START(ret24_rk:{curr_rk_ret:.3f}>{P['STRONG_RET_RANK_THRESHOLD']}"
                                   f"&fr_rank:{curr_fr_rank:.3f}<{P['MILD_FR_RANK_THRESHOLD']})",
                                   signal_ts_ms, 0.0, P['MAX_WEIGHT'])

    history_file = f"signal_history_{P['STRATEGY_NAME']}.csv"
    return [], _sync_persistent_signal_ledger(history_file, symbol, record, SIGNAL_COLS)


def generate_vol_fr_signals(kline_df, fr_df, bar_minutes=5):
    """
    vol_breakout_fr_recovery_long：4 小时前波动率极度萎缩、当下爆发时做多；费率自极低位回升时平仓。
    入参形貌: kline_df=原始K线, fr_df=资金费率
    """
    P = {'M_HOURS': 4, 'N_HOURS': 24, 'W_DAYS': 14,
         'ATR_RANK_LOW_TH': 0.20, 'ATR_RANK_HIGH_TH': 0.60, 'FR_RANK_LOW_TH': 0.10,
         'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 27 / 3,
         'STRATEGY_NAME': 'vol_breakout_fr_recovery_long'}

    if kline_df is None or len(kline_df) == 0 or fr_df is None or len(fr_df) == 0:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    symbol, coin_name = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df)

    W = _bars(P['W_DAYS'] * 24, bar_minutes)
    if len(df) < W:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    M, N, mp, EPS = _bars(P['M_HOURS'], bar_minutes), _bars(P['N_HOURS'], bar_minutes), max(50, W // 5), 1e-12
    h, l, c, fr_series = df['high'], df['low'], df['close'], df['funding_rate']
    prev_close = c.shift(1)

    true_range = pd.concat([h - l, (h - prev_close).abs(), (l - prev_close).abs()], axis=1).max(axis=1)
    atr_pct = true_range.rolling(N, min_periods=max(2, N // 2)).mean() / (c + EPS)
    rk_atr = atr_pct.rolling(W, min_periods=mp).rank(pct=True)
    fr_rank = fr_series.rolling(W, min_periods=mp).rank(pct=True)

    curr_rk_atr, prev_m_rk_atr = float(rk_atr.iloc[-1]), _tail_float(rk_atr, M)
    curr_fr_rank = float(fr_rank.iloc[-1])
    prev_m_fr_rank, prev_1_fr_rank = _tail_float(fr_rank, M), _tail_float(fr_rank, 1)
    price = float(c.iloc[-1])

    is_entry = (prev_m_rk_atr < P['ATR_RANK_LOW_TH']) and (curr_rk_atr > P['ATR_RANK_HIGH_TH'])
    is_exit = (prev_m_fr_rank < P['FR_RANK_LOW_TH']) and (curr_fr_rank > prev_1_fr_rank)

    record = None
    if is_entry or is_exit:
        signal_ts_ms = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'OPEN', 'LONG', price,
                                   f"VOL_LOW_TO_HIGH(rk_atr_{P['M_HOURS']}h_ago:{prev_m_rk_atr:.3f}"
                                   f"<{P['ATR_RANK_LOW_TH']} & curr:{curr_rk_atr:.3f}>{P['ATR_RANK_HIGH_TH']})",
                                   signal_ts_ms, P['TARGET_WEIGHT'], P['MAX_WEIGHT'])
        else:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'CLOSE', 'LONG', price,
                                   f"FR_RECOVERY_FROM_LOW(rk_fr_{P['M_HOURS']}h_ago:{prev_m_fr_rank:.3f}"
                                   f"<{P['FR_RANK_LOW_TH']} & curr:{curr_fr_rank:.3f}>prev:{prev_1_fr_rank:.3f})",
                                   signal_ts_ms, 0.0, P['MAX_WEIGHT'])

    history_file = f"signal_history_{P['STRATEGY_NAME']}.csv"
    return [], _sync_persistent_signal_ledger(history_file, symbol, record, SIGNAL_COLS)


def generate_bottom_powder_short_signals(kline_df, fr_df, oi_df, bar_minutes=15):
    """
    bottom_stabilize_powder_keg_short：底部企稳 + OI 抬升 + 情绪悲观时追空；OI 极高而成交极度萎缩("火药桶")时平空。
    入参形貌: kline_df=原始K线, fr_df=资金费率, oi_df=持仓量
    """
    P = {'N_HOURS': 24, 'M_HOURS': 4, 'W_DAYS': 14, 'POWDER_OI_RK': 0.90, 'POWDER_VOL_RK': 0.30,
         'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 24, 'STRATEGY_NAME': 'bottom_stabilize_powder_keg_short'}

    if (kline_df is None or len(kline_df) == 0 or fr_df is None or len(fr_df) == 0
            or oi_df is None or len(oi_df) == 0):
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    symbol, coin_name = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df, oi_df=oi_df)

    W = _bars(P['W_DAYS'] * 24, bar_minutes)
    if len(df) < W:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    N, M, mp, EPS = _bars(P['N_HOURS'], bar_minutes), _bars(P['M_HOURS'], bar_minutes), max(50, W // 5), 1e-12
    c, low, vol = df['close'], df['low'], df['volume']
    oi_amt, fr_rate = df['oi_amount'], df['funding_rate']

    min_low_n = low.rolling(N, min_periods=max(2, N // 2)).min()
    oi_min_m = oi_amt.rolling(M, min_periods=2).min()
    rk_oi = oi_amt.rolling(W, min_periods=mp).rank(pct=True)
    rk_vol = vol.rolling(W, min_periods=mp).rank(pct=True)
    fr_rank = fr_rate.rolling(W, min_periods=mp).rank(pct=True)

    price = float(c.iloc[-1])
    curr_min_low, prev_n_min_low = _tail_float(min_low_n), _tail_float(min_low_n, N)
    curr_oi, curr_oi_min_m = _tail_float(oi_amt), _tail_float(oi_min_m)
    curr_fr_rank, curr_fr = _tail_float(fr_rank), _tail_float(fr_rate)
    curr_rk_oi, curr_rk_vol = _tail_float(rk_oi), _tail_float(rk_vol)

    oi_bottom_div = (price / (curr_min_low + EPS) < 1.03) and (curr_oi > curr_oi_min_m * 1.05)
    fr_low_neg = (curr_fr_rank < 0.20) or (curr_fr < 0)
    price_higher_lows = curr_min_low > prev_n_min_low

    is_entry = oi_bottom_div and fr_low_neg and price_higher_lows
    is_exit = (curr_rk_oi > P['POWDER_OI_RK']) and (curr_rk_vol < P['POWDER_VOL_RK'])

    record = None
    if is_entry or is_exit:
        signal_ts_ms = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'OPEN', 'SHORT', price,
                                   f"BOTTOM_STABILIZE(c/L:<1.03, oi_amt:{curr_oi:.2f}>minM*1.05, "
                                   f"fr_rk<0.2 or fr<0)",
                                   signal_ts_ms, P['TARGET_WEIGHT'], P['MAX_WEIGHT'])
        else:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'CLOSE', 'SHORT', price,
                                   f"POWDER_KEG(rk_oi:{curr_rk_oi:.2f}>{P['POWDER_OI_RK']}, "
                                   f"rk_v:{curr_rk_vol:.2f}<{P['POWDER_VOL_RK']})",
                                   signal_ts_ms, 0.0, P['MAX_WEIGHT'])

    history_file = f"signal_history_{P['STRATEGY_NAME']}.csv"
    return [], _sync_persistent_signal_ledger(history_file, symbol, record, SIGNAL_COLS)


def generate_oi_decay_short_signals(kline_df, oi_df, bar_minutes=30):
    """
    oi_value_decay_short：OI 名义价值 EMA(4h) 下穿 EMA(24h)（杠杆资金撤退）做空；OI 极高但价格不热时平空。
    入参形貌: kline_df=原始K线, oi_df=持仓量
    """
    P = {'M_HOURS': 4, 'N_HOURS': 24, 'W_DAYS': 14, 'OI_RANK_EXTREME_TH': 0.95, 'OI_HOT_TH': 0.050,
         'TARGET_WEIGHT': -1.0, 'MAX_WEIGHT': 1.0 / 9 / 1.1, 'STRATEGY_NAME': 'oi_value_decay_short'}

    if kline_df is None or len(kline_df) == 0 or oi_df is None or len(oi_df) == 0:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    symbol, coin_name = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, oi_df=oi_df)

    W = _bars(P['W_DAYS'] * 24, bar_minutes)
    if len(df) < W:
        return [], pd.DataFrame(columns=SIGNAL_COLS)

    M, N, mp, EPS = _bars(P['M_HOURS'], bar_minutes), _bars(P['N_HOURS'], bar_minutes), max(50, W // 5), 1e-12
    c, oi_amt = df['close'], df['oi_amount']

    oi_value = oi_amt * c
    ema_fast = oi_value.ewm(span=M, adjust=False).mean()
    ema_slow = oi_value.ewm(span=N, adjust=False).mean()
    ma_n = c.rolling(N, min_periods=max(2, N // 2)).mean()
    rk_oi = oi_amt.rolling(W, min_periods=mp).rank(pct=True)

    price = float(c.iloc[-1])
    curr_ma_n, curr_rk_oi = _tail_float(ma_n), _tail_float(rk_oi)
    curr_fast, curr_slow = _tail_float(ema_fast), _tail_float(ema_slow)
    prev_fast, prev_slow = _tail_float(ema_fast, 1), _tail_float(ema_slow, 1)

    is_entry = (curr_fast < curr_slow) and (prev_fast >= prev_slow)
    is_exit = (curr_rk_oi > P['OI_RANK_EXTREME_TH']) and ((price / (curr_ma_n + EPS) - 1.0) < P['OI_HOT_TH'])

    record = None
    if is_entry or is_exit:
        signal_ts_ms = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'OPEN', 'SHORT', price,
                                   f"OI_VALUE_DEAD_CROSS(EMA4h:{curr_fast:.2f} < EMA24h:{curr_slow:.2f})",
                                   signal_ts_ms, P['TARGET_WEIGHT'], P['MAX_WEIGHT'])
        else:
            record = _build_record(P['STRATEGY_NAME'], symbol, coin_name, 'CLOSE', 'SHORT', price,
                                   f"OI_EXTREME_PRICE_NOT_HOT(Rank_OI:{curr_rk_oi:.2f}"
                                   f">{P['OI_RANK_EXTREME_TH']} & Dev<5%)",
                                   signal_ts_ms, 0.0, P['MAX_WEIGHT'])

    history_file = f"signal_history_{P['STRATEGY_NAME']}.csv"
    return [], _sync_persistent_signal_ledger(history_file, symbol, record, SIGNAL_COLS)


# =============================================================================
# 五、通用截面工作流驱动（所有 execute_trading_bot_workflow_* 的唯一实现）
# =============================================================================
def print_top_long_latest_signals(final_signals_df, logger, timeframe='1h'):
    """
    按当前北京时间向下取整到最新截面，从账本中精准捞出"此刻该执行"的指令并聚合打印。
    入参形貌: final_signals_df 需含 [time(北京时间字符串), event, action, coin, direction, price, reason, pnl, target_weight]
    """
    if final_signals_df is None or final_signals_df.empty:
        logger.info("[发单指令] 全量账本为空，当前无开平仓信号，保持现有仓位")
        return

    strategy_name = (final_signals_df['STRATEGY_NAME'].iloc[0]
                     if 'STRATEGY_NAME' in final_signals_df.columns else "top_coin_long")
    current_bjt = pd.Timestamp.now(tz='Asia/Shanghai').floor(timeframe.lower().replace('m', 'min'))
    latest_time_str = current_bjt.strftime('%Y-%m-%d %H:%M:%S')
    latest = final_signals_df[final_signals_df['time'] == latest_time_str]

    if latest.empty:
        logger.info(f"[发单指令/{strategy_name}] 截面 [{latest_time_str}] (北京时间) 无开平仓信号，保持现有仓位")
        return

    lines = [f"[发单指令/{strategy_name}] 截面 [{latest_time_str}] (北京时间) 共 [{len(latest)}] 条待执行:"]
    for _, row in latest.iterrows():
        base = (f"  ► {row['action']:<4} {row.get('coin', 'UNKNOWN'):<8}"
                f" | 方向: [{row.get('direction', 'LONG')}] | 价格: [{row.get('price', 0.0)}]")
        if row['event'] == 'CLOSE':
            pnl = row.get('pnl', None)
            pnl_str = f"{pnl:.2f}%" if pd.notna(pnl) else "N/A"
            lines.append(f"🔴 平仓{base} | 本次盈亏: [{pnl_str}] | 原因: [{row.get('reason', '')}]")
        else:
            lines.append(f"🟢 开仓{base} | 目标权重: [{row.get('target_weight', 0.0) * 100:.1f}%]"
                         f" | 原因: [{row.get('reason', '')}]")
    logger.info("\n".join(lines))


def _warn_data_gap(logger, label, symbol, df_kline, expected_rows):
    """K 线行数不足时给出「缺多少 + 实际可用区间 + 可能原因」的一条人话告警。"""
    actual = len(df_kline)
    if actual >= expected_rows:
        return
    span = "未知区间"
    if 'timestamp' in df_kline.columns and actual > 0:
        span = f"{_fmt_bjt(df_kline['timestamp'].iloc[0])} ~ {_fmt_bjt(df_kline['timestamp'].iloc[-1])}"
    logger.warning(
        f"⚠️ [{label}/数据体检] 标的 [{symbol}] K线不足，指标可能失真 | "
        f"预期: [{expected_rows}] 实际: [{actual}] 缺口: [{expected_rows - actual}] | "
        f"可用区间: [{span}] (北京时间) | "
        f"可能原因: 该合约上线时间晚于回溯起点、交易所限频丢包或代理不稳"
    )


def _run_signal_workflow(label, target_time, symbol_list, timeframe, bar_minutes, lookback_days,
                         signal_fn, output_path, proxy_url, need_funding=False, need_oi=False):
    """
    截面策略统一工作流：并发取数 -> 逐标的数据体检 -> 调用信号生成器 -> 聚合去重 -> 打印发单 -> 落盘。
    入参形貌: signal_fn(df_kline, fr_df_or_None, oi_df_or_None) -> DataFrame(SIGNAL_COLS)
    出参: 聚合后的信号 DataFrame（无标的进入推演时为空表）；副作用: 写出 output_path
    """
    logger = setup_logger()
    expected_rows = lookback_days * (1440 // bar_minutes) + 1
    logger.info(f"🚀 [{label}/启动] 实盘截面信号推演 | 周期: [{timeframe}] | 标的数: [{len(symbol_list)}] | "
                f"预热天数: [{lookback_days}] | 单标的预期K线: [{expected_rows}] | 目标时刻: [{target_time}]")

    kline_map = snipe_kline_data(symbol_list=symbol_list, timeframe=timeframe, days=lookback_days,
                                target_time_str=target_time, use_ws=True, use_rest=True, proxy_url=proxy_url)
    fr_map = (snipe_funding_rate_data(symbol_list=symbol_list, days=lookback_days, proxy_url=proxy_url)
              if need_funding else {})
    oi_map = (snipe_oi_data(symbol_list=symbol_list, timeframe=timeframe, days=lookback_days,
                            target_time_str=target_time, proxy_url=proxy_url) if need_oi else {})

    logger.info(f"✅ [{label}/取数完成] K线到位: [{sum(1 for s in symbol_list if not _frame_of(kline_map, s).empty)}"
                f"/{len(symbol_list)}]"
                + (f" | 资金费率到位: [{sum(1 for s in symbol_list if not _frame_of(fr_map, s).empty)}"
                   f"/{len(symbol_list)}]" if need_funding else "")
                + (f" | OI到位: [{sum(1 for s in symbol_list if not _frame_of(oi_map, s).empty)}"
                   f"/{len(symbol_list)}]" if need_oi else ""))

    frames, skipped = [], []
    for symbol in symbol_list:
        df_kline = _frame_of(kline_map, symbol)
        df_fr = _frame_of(fr_map, symbol) if need_funding else pd.DataFrame()
        df_oi = _frame_of(oi_map, symbol) if need_oi else pd.DataFrame()

        if df_kline.empty:
            skipped.append(f"{symbol}(K线为空)")
            continue
        if need_funding and df_fr.empty:
            skipped.append(f"{symbol}(资金费率为空)")
            continue
        if need_oi and df_oi.empty:
            skipped.append(f"{symbol}(OI为空)")
            continue

        _warn_data_gap(logger, label, symbol, df_kline, expected_rows)
        df_kline['coin_name'] = symbol.split('/')[0]
        df_kline['symbol'] = symbol

        try:
            frames.append(signal_fn(df_kline,
                                    df_fr if need_funding else None,
                                    df_oi if need_oi else None))
        except Exception as exc:
            logger.error(f"❌ [{label}/推演失败] 标的 [{symbol}] 的信号计算中断 | 原因: [{exc}] | "
                         f"排查线索: 检查该标的 K线/资金费率/OI 的列名是否符合预期、数据长度是否够滚动窗口",
                         exc_info=True)
            raise

    if skipped:
        logger.warning(f"⚠️ [{label}/数据缺口] 已跳过 [{len(skipped)}] 个标的: {skipped} | "
                       f"可能原因: 交易所无该合约、接口限频或代理不通")

    if not frames:
        logger.info(f"► [{label}/收官] 无任何标的进入推演，未产生有效信号")
        return pd.DataFrame(columns=SIGNAL_COLS)

    final_signals_df = pd.concat(frames, ignore_index=True)
    final_signals_df.drop_duplicates(subset=['symbol', 'signal_timestamp_ms', 'event'], inplace=True)
    print_top_long_latest_signals(final_signals_df, logger, timeframe=timeframe)
    final_signals_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"✅ [{label}/账本落盘] 文件: [{output_path}] | 记录数: [{len(final_signals_df)}]")
    return final_signals_df


def execute_trading_bot_workflow_top_long(target_time, symbol_list, proxy_url=None):
    """top_coin_long 工作流（1h 线）：高位长上影+爆量做多，孕线突破+爆量离场。"""
    return _run_signal_workflow(
        label='top_long', target_time=target_time, symbol_list=symbol_list,
        timeframe="1h", bar_minutes=60, lookback_days=210, output_path="top_long_signals.csv",
        proxy_url=proxy_url,
        signal_fn=lambda k, fr, oi: generate_top_long_signals(k)[1]
    )


def execute_trading_bot_workflow_ma_bottom_long(target_time, symbol_list, proxy_url=None):
    """multi_ma_break_long 工作流（5m 线）：跌破多重均线做多，快慢死叉离场。"""
    return _run_signal_workflow(
        label='ma_bottom_long', target_time=target_time, symbol_list=symbol_list,
        timeframe="5m", bar_minutes=5, lookback_days=40, output_path="ma_bottom_long_signals.csv",
        proxy_url=proxy_url,
        signal_fn=lambda k, fr, oi: generate_multi_ma_signals(k)[1]
    )


def execute_trading_bot_workflow_XSR_long(target_time, symbol_list, proxy_url=None):
    """
    XSR 工作流（30m 线，需资金费率）：
    bottom_10 币种出现历史级 4 小时极度暴涨时追高做多，多头情绪彻底冷却（费率极低/转负）时离场。
    回测：598 笔 | 单笔净收益 5.81% | 胜率 48.95% | 均值单笔回撤 -10.96% | 最大并发 30
    """
    return _run_signal_workflow(
        label='XSR', target_time=target_time, symbol_list=symbol_list,
        timeframe="30m", bar_minutes=30, lookback_days=20, output_path="XSR_long_signals.csv",
        proxy_url=proxy_url, need_funding=True,
        signal_fn=lambda k, fr, oi: generate_XSR_signals(k, fr, bar_minutes=30)[1]
    )


def execute_trading_bot_workflow_short_fr(target_time, symbol_list, proxy_url=None):
    """
    Extreme FR Short 工作流（30m 线，需资金费率）：
    狙击全市场"多头最拥挤、做多成本最高"的标的做空，杠杆被清洗、情绪回归平淡时离场。
    回测：261 笔 | 单笔净收益 6.91% | 胜率 67.68% | 均值单笔回撤 -31.65% | 最大并发 7
    """
    return _run_signal_workflow(
        label='fr_short', target_time=target_time, symbol_list=symbol_list,
        timeframe="30m", bar_minutes=30, lookback_days=20, output_path="short_fr_signals.csv",
        proxy_url=proxy_url, need_funding=True,
        signal_fn=lambda k, fr, oi: generate_short_fr_signals(k, fr, bar_minutes=30)[1]
    )


def execute_trading_bot_workflow_vol_fr_long(target_time, symbol_list, proxy_url=None):
    """
    Vol FR Long 工作流（5m 线，需资金费率）：
    策略描述：bottom_10的币种在币价出现历史级别的4小时极度暴涨时直接追高做多，当市场多头情绪彻底冷却（资金费率极低或转负）时平仓走人
    VOL_LOW_TO_HIGH -> FR_RECOVERY_FROM_LOW Long_bottom_20 5m
    回测表现：
    总交易笔数：355  单笔净收益：13.0693%  跨币种胜率(%)： 49.6855  均值单笔回撤(%)：-11.9181 平均持仓时间(天)：15.2574
    持仓时间中位数(天)：0.2674  策略性价比 (收益风险比):15.2262 资金最大回撤：304.7110%   最大并发持仓：27
    """
    return _run_signal_workflow(
        label='vol_fr_long', target_time=target_time, symbol_list=symbol_list,
        timeframe="5m", bar_minutes=5, lookback_days=20, output_path="vol_fr_long_signals.csv",
        proxy_url=proxy_url, need_funding=True,
        signal_fn=lambda k, fr, oi: generate_vol_fr_signals(k, fr, bar_minutes=5)[1]
    )


def execute_trading_bot_workflow_bottom_powder_short(target_time, symbol_list, proxy_url=None):
    """
    Bottom Powder Short 工作流（15m 线，需资金费率 + OI）：
    策略描述：bottom_10的币种在底部出现空头持仓激增且情绪极度悲观（资金费率为负）的短暂企稳时直接顺势追空，当盘面进入杠杆极高且成交量极度萎缩的“火药桶”僵持状态时平仓走人。
    ENTRY_BOTTOM_STABILIZE -> REGIME_POWDER_KEG Short_bottom_10 15m
    回测表现：
    总交易笔数：157  单笔净收益：12.5116%  跨币种胜率(%)： 74.4186  均值单笔回撤(%)：-32.4334 平均持仓时间(天)：23.9702
    持仓时间中位数(天)：16.4792  策略性价比 (收益风险比):18.1403 资金最大回撤：108.2850%   最大并发持仓：24
    """
    return _run_signal_workflow(
        label='bottom_powder_short', target_time=target_time, symbol_list=symbol_list,
        timeframe="15m", bar_minutes=15, lookback_days=20,
        output_path="bottom_powder_short_signals.csv", proxy_url=proxy_url,
        need_funding=True, need_oi=True,
        signal_fn=lambda k, fr, oi: generate_bottom_powder_short_signals(k, fr, oi, bar_minutes=15)[1]
    )


def execute_trading_bot_oi_decay_short(target_time, symbol_list, proxy_url=None):
    """
    OI Value Decay Short 工作流（30m 线，需 OI）：
    策略描述：top_3的币种在市场杠杆资金明显开始撤退降温时顺势做空，当盘面积聚了历史级别的天量杠杆但价格却陷入僵持（面临随时爆拉的反洗风险）时，果断平仓走人。
    EXIT_OI_VALUE_MA_DEAD_CROSS -> OI_EXTREME_PRICE_NOT_HOT Short_top_3 30m
    回测表现：
    总交易笔数：89  单笔净收益：15.4668%  跨币种胜率(%)： 72.0588  均值单笔回撤(%)：-60.4100 平均持仓时间(天)：15.0960
    持仓时间中位数(天)：13.0833  策略性价比 (收益风险比):12.3636 资金最大回撤：111.3390%   最大并发持仓：9
    """
    return _run_signal_workflow(
        label='oi_decay_short', target_time=target_time, symbol_list=symbol_list,
        timeframe="30m", bar_minutes=30, lookback_days=20,
        output_path="oi_decay_short_signals.csv", proxy_url=proxy_url, need_oi=True,
        signal_fn=lambda k, fr, oi: generate_oi_decay_short_signals(k, oi, bar_minutes=30)[1]
    )


# =============================================================================
# 六、主干 A：4H 横截面动量组合（矩阵组装 -> 状态机推演 -> 实盘流水线）
# =============================================================================
def build_4h_cross_section(logger, minute_klines_list, time_offset='0h'):
    """
    分钟级 K 线列表 -> 4H 横截面矩阵（每币 4 列：COIN_open/high/low + COIN 作为收盘价）。

    入参形貌: minute_klines_list=[df(含 timestamp(ms), close, coin_name)]
    出参形貌: DataFrame(index=4H UTC, cols=各币 open/high/low + 币名)，
              已按"全币种 4H 公共区间"截断并 ffill（与回测的公共区间截断逻辑对齐）
    """
    resampled, m1_starts, m1_ends = [], [], []
    for df in minute_klines_list:
        if df is None or df.empty:
            continue
        coin = df['coin_name'].iloc[0]
        s = df.copy()
        s['timestamp'] = pd.to_datetime(s['timestamp'], unit='ms')
        s = s.set_index('timestamp').sort_index()
        m1_starts.append(s.index[0])
        m1_ends.append(s.index[-1])

        bars = s['close'].resample('4h', offset=time_offset).agg(
            open='first', high='max', low='min', close='last'
        ).dropna(how='all')
        # 下游引擎强依赖此命名格式：收盘价列直接用币名
        bars.columns = [f"{coin}_open", f"{coin}_high", f"{coin}_low", coin]
        resampled.append(bars)

    if not resampled:
        raise ValueError("传入的 minute_klines_list 全为空或无法解析！")

    df_raw = pd.concat(resampled, axis=1).sort_index()
    main_coins = [c for c in df_raw.columns if not any(x in c for x in ('_open', '_high', '_low'))]
    common_start = max(df_raw[c].first_valid_index() for c in main_coins)
    common_end = min(df_raw[c].last_valid_index() for c in main_coins)
    df_merged = df_raw.loc[common_start:common_end].ffill()

    logger.info(
        f"[4H矩阵/组装完成] 币种数: [{len(resampled)}] | Offset: [{time_offset}] | "
        f"有效 1m 交集: [{_fmt_bjt(max(m1_starts))} ~ {_fmt_bjt(min(m1_ends))}] | "
        f"4H 公共区间: [{_fmt_bjt(common_start)} ~ {_fmt_bjt(common_end)}] | "
        f"矩阵: [{df_merged.shape[0]}行 x {df_merged.shape[1]}列] (北京时间)"
    )
    return df_merged


def run_strategy_simulation(df_cross_section, strategy_params, trade_mode, initial_capital=10000.0,
                            start_trade_date='2026-04-27 00:00:00', logger=None):
    """
    流式状态机推演：横截面特征 -> 逐根 4H K 线选币/开平仓 -> 与回测 100% 一致的交易账本。

    入参形貌: df_cross_section 列含 {COIN, COIN_open, COIN_high, COIN_low}，必须包含 'BTC'
             strategy_params 必含 MOM_WINDOW / VOL_WINDOW / BTC_TREND_WINDOW / MAX_WEIGHT，可选 TOP_K
    出参形貌: DataFrame 列含 time/action/coin/direction/event/price/amount/value/fee/reason/
             target_weight/pnl/top_k/max_weight
    副作用: 向 df_cross_section 追加 'signal_status' 列（逐根 K 线的信号诊断）
    """
    MOM_WINDOW = strategy_params['MOM_WINDOW']
    VOL_WINDOW = strategy_params['VOL_WINDOW']
    BTC_TREND_WINDOW = strategy_params['BTC_TREND_WINDOW']
    TOP_K = int(strategy_params.get('TOP_K', 2))
    MAX_WEIGHT = strategy_params['MAX_WEIGHT']
    FEE_RATE = 0.000

    target_coins = [c for c in df_cross_section.columns
                    if not any(sfx in c for sfx in ('_open', '_high', '_low'))]
    if 'BTC' not in target_coins:
        raise ValueError("数据矩阵中必须包含 BTC 作为宏观开关！")
    n_coins = len(target_coins)
    coin_to_idx = {c: i for i, c in enumerate(target_coins)}

    # === 向量化指标：动量 / ATR 波动率 / 风险调整动量 / BTC 趋势开关 ===
    df_close = df_cross_section[target_coins]
    df_returns = df_close.pct_change(MOM_WINDOW)

    df_high = df_cross_section[[f"{c}_high" for c in target_coins]].copy()
    df_high.columns = target_coins
    df_low = df_cross_section[[f"{c}_low" for c in target_coins]].copy()
    df_low.columns = target_coins
    df_prev_close = df_close.shift(1)

    true_range_arr = np.fmax.reduce([
        (df_high - df_low).values,
        (df_high - df_prev_close).abs().values,
        (df_low - df_prev_close).abs().values
    ])
    df_atr = pd.DataFrame(true_range_arr, index=df_cross_section.index,
                          columns=target_coins).rolling(window=VOL_WINDOW).mean()
    df_volatility_pct = df_atr / df_close
    df_adj_mom = df_returns / (df_volatility_pct + 1e-8)

    df_btc_ma = df_cross_section['BTC'].rolling(window=BTC_TREND_WINDOW).mean()

    mom_arr = df_adj_mom.values
    vol_arr = df_volatility_pct.values
    btc_trend_arr = (df_cross_section['BTC'] > df_btc_ma).values
    close_arr = df_close.values
    ref_price_arr = df_close.shift(MOM_WINDOW).values   # 零动量阈值价（MOM_WINDOW 周期前的价格）
    btc_ma_arr = df_btc_ma.values
    time_index = df_cross_section.index

    # === 状态机初始化 ===
    cash = float(initial_capital)
    positions_arr = np.zeros(n_coins, dtype=float)
    coin_states = {c: {'qty': 0.0, 'cost': 0.0, 'side': None} for c in target_coins}
    trade_ledger = []
    kline_signal_diagnostics = ["无信号: 指标预热期"] * len(df_cross_section)
    warmup_period = max(MOM_WINDOW, VOL_WINDOW, BTC_TREND_WINDOW)

    start_trade_timestamp = pd.to_datetime(start_trade_date) if start_trade_date else None

    for i in range(warmup_period, len(df_cross_section)):
        current_time = time_index[i]
        current_prices = close_arr[i]
        current_mom, current_vol = mom_arr[i], vol_arr[i]
        is_btc_trend_on = bool(btc_trend_arr[i])
        total_equity = cash + np.dot(positions_arr, current_prices)

        # --- 候选筛选：大盘开关定方向，风险调整动量排序取 TOP_K ---
        is_long_side = is_btc_trend_on
        side_word = '做多' if is_long_side else '做空'
        mode_allowed = trade_mode in (('BOTH', 'LONG_ONLY') if is_long_side else ('BOTH', 'SHORT_ONLY'))

        picks = []
        if mode_allowed:
            mask = ~np.isnan(current_mom) & ((current_mom > 0) if is_long_side else (current_mom < 0))
            valid_idx = np.where(mask)[0]
            if valid_idx.size:
                valid_vals = current_mom[valid_idx]
                order = np.argsort(-valid_vals if is_long_side else valid_vals, kind='stable')
                picks = [target_coins[j] for j in valid_idx[order[:TOP_K]]]

        candidate_longs = picks if is_long_side else []
        candidate_shorts = [] if is_long_side else picks

        # 时间拦截器：未到发车时间，强制掐断候选名单
        if start_trade_timestamp is not None and current_time < start_trade_timestamp:
            candidate_longs, candidate_shorts = [], []
            kline_signal_diagnostics[i] = "无信号: 未到设定的发车时间"
        elif picks:
            kline_signal_diagnostics[i] = f"有信号 ({side_word}): {', '.join(picks)}"
        elif mode_allowed:
            kline_signal_diagnostics[i] = (f"无信号: 大盘{'看多' if is_long_side else '看空'}，"
                                           f"但所有标的动量均不满足{side_word}阈值")
        else:
            kline_signal_diagnostics[i] = (f"无信号: 大盘{'看多' if is_long_side else '看空'}，"
                                           f"但策略模式禁止{side_word}")

        # --- A. 平仓：不在最新候选名单里的持仓一律出清 ---
        for idx_c in range(n_coins):
            pos = positions_arr[idx_c]
            coin = target_coins[idx_c]
            if pos > 0 and coin not in candidate_longs:
                pos_is_long = True
            elif pos < 0 and coin not in candidate_shorts:
                pos_is_long = False
            else:
                continue

            amount = abs(pos)
            price = current_prices[idx_c]
            value = amount * price
            fee = value * FEE_RATE
            cost = coin_states[coin]['cost']
            positions_arr[idx_c] = 0.0

            if pos_is_long:
                cash += (value - fee)
                net_pnl = amount * (price - cost) - fee
                close_reason = ("大盘开关关闭" if not is_btc_trend_on
                                else ("动量转负退场" if current_mom[idx_c] <= 0 else "掉出前K名排名"))
            else:
                cash -= (value + fee)
                net_pnl = amount * (cost - price) - fee
                close_reason = ("大盘开关关闭" if is_btc_trend_on
                                else ("动量转正退场" if current_mom[idx_c] >= 0 else "掉出前K名排名"))

            trade_ledger.append({
                "time": current_time, "action": "SELL" if pos_is_long else "BUY", "coin": coin,
                "direction": "LONG" if pos_is_long else "SHORT", "event": "CLOSE",
                "price": price, "amount": amount, "value": value, "fee": fee,
                "reason": close_reason, "target_weight": 0.0,
                "pnl": (net_pnl / (cost * amount)) * 100 if cost > 0 else 0.0,
                "top_k": TOP_K, "max_weight": MAX_WEIGHT
            })
            coin_states[coin] = {'qty': 0.0, 'cost': 0.0, 'side': None}

        # --- B. 开仓：按 1/波动率 分配权重（MAX_WEIGHT 封顶），多头额外受现金约束 ---
        for side_is_long, candidates in ((True, candidate_longs), (False, candidate_shorts)):
            if not candidates:
                continue
            inv_vols = [1.0 / current_vol[coin_to_idx[c]] if current_vol[coin_to_idx[c]] > 0 else 0
                        for c in candidates]
            total_inv_vol = sum(inv_vols)
            if total_inv_vol <= 0:
                continue

            for k_, coin in enumerate(candidates):
                idx_c = coin_to_idx[coin]
                if positions_arr[idx_c] != 0:
                    continue

                target_weight = min(inv_vols[k_] / total_inv_vol, MAX_WEIGHT)
                notional = total_equity * target_weight / (1 + FEE_RATE)
                if side_is_long and cash < notional:
                    notional = cash / (1 + FEE_RATE)
                if notional <= 1.0:
                    continue

                price = current_prices[idx_c]
                fee = notional * FEE_RATE
                amount = notional / price

                if side_is_long:
                    positions_arr[idx_c] += amount
                    cash -= (notional + fee)
                    coin_states[coin] = {'qty': amount, 'cost': price + (fee / amount), 'side': 'LONG'}
                else:
                    positions_arr[idx_c] -= amount
                    cash += (notional - fee)
                    coin_states[coin] = {'qty': -amount, 'cost': price - (fee / amount), 'side': 'SHORT'}

                trade_ledger.append({
                    "time": current_time, "action": "BUY" if side_is_long else "SELL", "coin": coin,
                    "direction": "LONG" if side_is_long else "SHORT", "event": "OPEN",
                    "price": price, "amount": amount, "value": notional, "fee": fee,
                    "reason": "Signal Entry Long" if side_is_long else "Signal Entry Short",
                    "target_weight": target_weight, "pnl": np.nan,
                    "top_k": TOP_K, "max_weight": MAX_WEIGHT
                })

        # --- C. 最新一根 K 线的全景诊断（聚合为单条日志，避免碎片刷屏） ---
        if logger is not None and i == len(df_cross_section) - 1:
            btc_idx = coin_to_idx['BTC']
            btc_price, btc_ma = current_prices[btc_idx], btc_ma_arr[i]
            btc_dev = (btc_price - btc_ma) / btc_ma if btc_ma > 0 else 0.0
            picked = set(candidate_longs) | set(candidate_shorts)

            lines = [
                f"[策略推演/最新截面] 时间: [{current_time}] | 模式: [{trade_mode}] | "
                f"参数: [MOM={MOM_WINDOW} VOL={VOL_WINDOW} BTC_MA={BTC_TREND_WINDOW} TOP_K={TOP_K}]",
                f"  ├─ 大盘开关: [{'ON 多头趋势' if is_btc_trend_on else 'OFF 空头趋势'}] | "
                f"BTC现价: [{btc_price:.2f}] vs 均线: [{btc_ma:.2f}] | 偏离: [{btc_dev:+.2%}]",
                f"  ├─ 信号诊断: [{kline_signal_diagnostics[i]}]",
                f"  ├─ 账户状态: 权益 [{total_equity:,.2f}] | 现金 [{cash:,.2f}] | "
                f"持仓标的数 [{int(np.count_nonzero(positions_arr))}]",
            ]
            for coin in target_coins:
                idx = coin_to_idx[coin]
                p_now, p_ref = current_prices[idx], ref_price_arr[i, idx]
                p_dev = (p_now - p_ref) / p_ref if p_ref > 0 else 0.0
                tag = f"★入选-{side_word}" if coin in picked else "  未入选"
                lines.append(
                    f"  ├─ [{tag}] {coin:<8} | 风险调整动量: [{current_mom[idx]:>8.4f}] | "
                    f"波动率: [{current_vol[idx]:.4%}] | 现价: [{p_now:<12.4f}] | "
                    f"零动量阈值价: [{p_ref:<12.4f}] | 价格偏离: [{p_dev:+.2%}]"
                )
            lines.append(f"  └─ 本截面新增账本记录: "
                         f"[{sum(1 for r in trade_ledger if r['time'] == current_time)}] 条")
            logger.info("\n".join(lines))

    df_cross_section['signal_status'] = kline_signal_diagnostics
    return pd.DataFrame(trade_ledger)


def run_live_pipeline(minute_klines_list, strategy_params_list, logger):
    """
    多参数实盘流水线：4H 矩阵 -> 状态机推演 -> 账本时间对齐(+4h、UTC毫秒戳、北京时间) -> 发单指令 -> 汇总落盘。

    入参形貌: minute_klines_list=[df(含 timestamp/close/coin_name/symbol)]
             strategy_params_list=[{STRATEGY_NAME, TIME_OFFSET, TRADE_MODE, MOM_WINDOW, VOL_WINDOW,
                                    BTC_TREND_WINDOW, MAX_WEIGHT, TOP_K}]
    出参: 全量账本 DataFrame（无信号时为空表）；副作用: 写出 live_simulation_logs.csv
    """
    # 币名 -> 完整 symbol 的动态映射，避免下游硬编码交易对后缀
    coin_to_symbol = {df['coin_name'].iloc[0]: df['symbol'].iloc[0] for df in minute_klines_list
                      if df is not None and not df.empty and {'coin_name', 'symbol'} <= set(df.columns)}

    all_ledgers = []
    for params in strategy_params_list:
        name, offset, mode = params['STRATEGY_NAME'], params['TIME_OFFSET'], params['TRADE_MODE']
        logger.info(f"⏳ [流水线/{name}] 开始组装 4H 矩阵并推演 | Offset: [{offset}] | 模式: [{mode}]")

        df_4h = build_4h_cross_section(logger, minute_klines_list, time_offset=offset)
        if df_4h is None or df_4h.empty:
            logger.warning(f"⚠️ [流水线/{name}] 4H 矩阵为空已跳过 | "
                           f"可能原因: 分钟级数据缺失或各币种无公共时间区间")
            continue

        ledger = run_strategy_simulation(df_cross_section=df_4h, strategy_params=params,
                                        trade_mode=mode, logger=logger)

        # 4H K 线走完（开盘 +4h）才是信号真正的实盘执行时刻
        latest_exec_bjt = (df_4h.index[-1] + pd.Timedelta(hours=4)) \
            .tz_localize('UTC').tz_convert('Asia/Shanghai').tz_localize(None)

        if ledger.empty:
            logger.info(f"🧠 [流水线/{name}] 推演完成 | 账本记录: [0] | 最新信号时间: [无]")
            latest_signals = pd.DataFrame()
        else:
            ledger['time'] = pd.to_datetime(ledger['time']) + pd.Timedelta(hours=4)
            # 先基于纯净 UTC 生成毫秒戳（无视时区漂移，保证下游 API 不认错），再转北京时间给人看
            ledger['signal_timestamp_ms'] = ledger['time'].astype('int64') // 10 ** 6
            ledger['time'] = ledger['time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
            ledger['STRATEGY_NAME'] = name
            ledger['symbol'] = ledger['coin'].map(coin_to_symbol).fillna(ledger['coin'] + '/USDT:USDT')
            all_ledgers.append(ledger)

            logger.info(f"🧠 [流水线/{name}] 推演完成 | 账本记录: [{len(ledger)}] | "
                        f"最新信号时间: [{ledger['time'].max():%Y-%m-%d %H:%M:%S}] (北京时间)")
            latest_signals = ledger[ledger['time'] == latest_exec_bjt]

        if latest_signals.empty:
            logger.info(f"🎯 [发单指令/{name}] 截面 [{latest_exec_bjt:%Y-%m-%d %H:%M:%S}] (北京时间) "
                        f"无开平仓信号，保持现有仓位")
        else:
            lines = [f"🎯 [发单指令/{name}] 截面 [{latest_exec_bjt:%Y-%m-%d %H:%M:%S}] (北京时间) "
                     f"共 [{len(latest_signals)}] 条待执行:"]
            for _, row in latest_signals.iterrows():
                base = (f"{row['action']:<4} {row['coin']:<8} | 方向: [{row['direction']}] | "
                        f"价格: [{row['price']}]")
                if row['event'] == 'CLOSE':
                    lines.append(f"  ► 🔴 平仓 | {base} | 数量: [{row['amount']:.4f}] | "
                                 f"原因: [{row['reason']}]")
                else:
                    lines.append(f"  ► 🟢 开仓 | {base} | 目标权重: [{row['target_weight'] * 100:.1f}%] | "
                                 f"原因: [{row['reason']}]")
            logger.info("\n".join(lines))

    if not all_ledgers:
        logger.info("► [流水线/收官] 所有策略在历史流转中均未产生任何交易信号")
        return pd.DataFrame()

    output_path = "live_simulation_logs.csv"
    final_ledger_df = pd.concat(all_ledgers, ignore_index=True)
    final_ledger_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(f"✅ [流水线/账本落盘] 文件: [{output_path}] | 记录数: [{len(final_ledger_df)}] | "
                f"覆盖策略数: [{len(all_ledgers)}]")
    return final_ledger_df


def execute_trading_bot_workflow_cross(target_time, proxy_url=None):
    """
    4H 横截面动量组合入口：按最大指标窗口反推预热天数 -> 拉 1m K线 -> 多参数并行推演。
    出参: 全量账本 DataFrame；无任何数据时返回空字符串（见下方）
    """
    strategy_params_list = [
        {'STRATEGY_NAME': 'Grid_No.43629', 'MOM_WINDOW': 48, 'VOL_WINDOW': 42, 'BTC_TREND_WINDOW': 120,
         'MAX_WEIGHT': 2.6, 'TOP_K': 1, 'TIME_OFFSET': '2h', 'TRADE_MODE': 'LONG_ONLY'},
        {'STRATEGY_NAME': 'Grid_No.69393', 'MOM_WINDOW': 90, 'VOL_WINDOW': 120, 'BTC_TREND_WINDOW': 720,
         'MAX_WEIGHT': 0.4, 'TOP_K': 3, 'TIME_OFFSET': '0h', 'TRADE_MODE': 'SHORT_ONLY'},
    ]
    symbol_list = ["BTC/USDT:USDT", "ETH/USDT:USDT", "SOL/USDT:USDT",
                   "XRP/USDT:USDT", "BNB/USDT:USDT", "DOGE/USDT:USDT"]
    timeframe = "1m"

    # 一天 6 根 4H bar，故 bar 窗口 /6 换算成天数，再加 30 天冗余
    max_window = max(max(p['MOM_WINDOW'], p['VOL_WINDOW'], p['BTC_TREND_WINDOW'])
                     for p in strategy_params_list)
    lookback_days = int(np.ceil(max_window / 6)) + 30
    expected_rows = lookback_days * 24 * 60 + 1

    run_logger = setup_logger()
    run_logger.info(f"🚀 [Cross/启动] 4H 横截面动量组合 | 策略数: [{len(strategy_params_list)}] | "
                    f"最大指标窗口: [{max_window} bars] | 预热天数: [{lookback_days}] | "
                    f"标的数: [{len(symbol_list)}] | 单标的预期K线: [{expected_rows}] | "
                    f"目标时刻: [{target_time}]")

    result_map = snipe_kline_data(symbol_list=symbol_list, timeframe=timeframe, days=lookback_days,
                                 target_time_str=target_time, use_ws=True, use_rest=True,
                                 proxy_url=proxy_url)

    fetched_raw_data, missing = [], []
    for symbol in symbol_list:
        df_klines = _frame_of(result_map, symbol)
        if df_klines.empty:
            missing.append(symbol)
            continue
        _warn_data_gap(run_logger, 'Cross', symbol, df_klines, expected_rows)
        df_klines['coin_name'] = symbol.split('/')[0]
        df_klines['symbol'] = symbol   # 向下游无损传递完整符号元数据
        fetched_raw_data.append(df_klines)

    if missing:
        run_logger.warning(f"⚠️ [Cross/数据体检] 完全无数据的标的: {missing} | "
                           f"可能原因: 交易所无该合约、网络/代理不通或 snipe_kline_data 引擎异常")

    if not fetched_raw_data:
        run_logger.error("❌ [Cross/致命] 没有任何标的数据被成功加载，无法组装横截面矩阵 | "
                         "排查线索: 检查网络/代理与 fetch_data_quick 取数引擎")
        return ""

    run_logger.info(f"✅ [Cross/取数完成] 标的到位: [{len(fetched_raw_data)}/{len(symbol_list)}]，开始多参数推演")
    return run_live_pipeline(fetched_raw_data, strategy_params_list, run_logger)


# =============================================================================
# 七、程序入口（本地联调用）
# =============================================================================
if __name__ == "__main__":
    target_time = (datetime.now() - timedelta(minutes=60)).strftime("%Y-%m-%d %H:%M")
    symbol_list = ['AIOT/USDT:USDT']

    execute_trading_bot_oi_decay_short(target_time, symbol_list, 'http://127.0.0.1:7890')