# =============================================================================
# [功能摘要]
#   加密合约多策略实盘信号引擎：统一拉取 K 线/资金费率/OI，生成单标的即时信号、
#   4H 横截面动量组合账本与 1m 因子信号，并将需要跨进程续跑的单标的持仓状态持久化到 CSV。
#
# [输入数据]
#   · snipe_kline_data(...) -> {symbol: DataFrame}
#     K线关键列: timestamp|open_time|time|ts, open, high, low, close, volume
#   · snipe_funding_rate_data(...) -> {symbol: DataFrame}
#     资金费率关键列: timestamp|fundingTime|time|ts + funding_rate|fundingRate|rate
#   · snipe_oi_data(...) -> {symbol: DataFrame}
#     OI关键列: timestamp|time|ts + oi_amount|openInterest|open_interest|sumOpenInterest|oi
#   · signal_history_<策略名>.csv：单标的策略跨进程持仓状态的历史账本
#
# [数据流转/交互]
#   · 单标的策略：原始 K线 -> 重采样 -> FR/OI 前向对齐 -> 最后一根闭合K线计算条件
#                  -> 候选记录 -> CSV 状态机过滤重复/无效事件并补 PnL -> 汇总发单/落盘
#   · 4H横截面：1m K线 -> 4H+offset 重采样 -> 全币公共区间矩阵 -> BTC趋势定方向
#                -> 风险调整动量排序 -> 1/波动率分配权重 -> 交易账本 -> +4h 执行时间 -> 落盘
#   · 1m因子：原始 K线 -> 滚动边界/分位数特征 -> 历史 OPEN 信号 -> 多标的聚合排序 -> 落盘
#
# [输出数据]
#   · 返回 DataFrame：策略账本或因子信号；无信号时返回带稳定列结构的空 DataFrame
#   · 副作用：写出 <策略>_signals.csv、signal_history_<策略>.csv、live_simulation_logs.csv，
#             并通过 logger 输出聚合后的启动、数据体检、最新截面、发单与落盘信息
# =============================================================================

import os
import platform
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from common_utils import setup_logger
from data_provider import snipe_kline_data, snipe_funding_rate_data, snipe_oi_data

SIGNAL_COLS = [
    'time', 'action', 'coin', 'direction', 'event', 'price', 'reason',
    'target_weight', 'pnl', 'top_k', 'max_weight', 'signal_timestamp_ms',
    'STRATEGY_NAME', 'symbol',
]
FACTOR_COLS = [
    'timestamp', 'timestamp_str', 'event', 'direction', 'price',
    'symbol', 'coin_name', 'strategy_name',
]


# =============================================================================
# 一、通用工具：时间、字段解析、窗口换算、身份解析
# =============================================================================
def _fmt_bjt(value):
    """把 UTC 毫秒戳或时间对象统一格式化为北京时间字符串。"""
    if isinstance(value, (int, float, np.integer, np.floating)):
        ts = pd.to_datetime(int(value), unit='ms', utc=True)
    else:
        ts = pd.Timestamp(value)
        ts = ts.tz_localize('UTC') if ts.tzinfo is None else ts
    return ts.tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')


def _pick_column(df, candidates, tag):
    """从候选字段中返回首个存在的列；缺失时直接暴露真实列结构。"""
    for col in candidates:
        if col in df.columns:
            return col
    raise KeyError(f"[{tag}] 找不到列 {candidates}，实际列: {list(df.columns)}")


def _bars(hours, bar_minutes):
    """把小时窗口换算为 K 线根数，最少返回 1。"""
    return max(1, int(round(hours * 60.0 / bar_minutes)))


def _tail_float(series, offset=0, default=0.0):
    """读取倒数第 1+offset 个数值；越界或 NaN 时按既有逻辑回落 default。"""
    if len(series) <= offset:
        return default
    value = series.iloc[-1 - offset]
    return default if pd.isna(value) else float(value)


def _frame_of(result_map, symbol):
    """从取数结果映射中安全取得单标的 DataFrame。"""
    df = result_map.get(symbol) if result_map else None
    return df if df is not None else pd.DataFrame()


def _resolve_identity(df):
    """解析 (symbol, coin_name)：优先数据列，其次 attrs，最后从 symbol 反解币名。"""
    symbol = df['symbol'].iloc[0] if 'symbol' in df.columns else df.attrs.get('symbol', 'UNKNOWN')
    coin_name = df['coin_name'].iloc[0] if 'coin_name' in df.columns else (
        symbol.split('/')[0] if '/' in symbol else symbol
    )
    return symbol, coin_name


def _empty_signal_result():
    """返回与历史账本兼容的空信号结果。"""
    return [], pd.DataFrame(columns=SIGNAL_COLS)


# =============================================================================
# 二、K线重采样与 FR/OI 多源对齐
# =============================================================================
def _attach_series(df, src_df, time_keys, value_keys, out_col, bar, tag):
    """
    把外部时间序列重采样并前向对齐到主 K 线索引。

    入参形貌: src_df 包含一个 time_keys 字段与一个 value_keys 字段；df.index 为 UTC DatetimeIndex。
    副作用: 在 df 上写入 out_col。
    """
    src = src_df.copy()
    time_col = _pick_column(src, time_keys, tag)
    value_col = _pick_column(src, value_keys, tag)
    src['dt'] = pd.to_datetime(src[time_col], unit='ms', utc=True)
    series = (
        src.drop_duplicates(subset=[time_col])
        .sort_values('dt')
        .set_index('dt')[value_col]
        .astype(float)
        .resample(bar, label='left', closed='left')
        .last()
    )
    df[out_col] = series.reindex(df.index).ffill()


def _build_aligned_frame(kline_df, bar_minutes, fr_df=None, oi_df=None):
    """
    构建策略特征底表：K线重采样 + FR/OI 前向对齐 + 多源公共起点截断。

    入参形貌:
      kline_df: [timestamp|open_time|time|ts, open, high, low, close, volume]
      fr_df:     [timestamp|fundingTime|time|ts] + [funding_rate|fundingRate|rate]，可空
      oi_df:     [timestamp|time|ts] + [oi_amount|openInterest|open_interest|sumOpenInterest|oi]，可空
    出参形貌:
      DataFrame(index=UTC DatetimeIndex, cols=[open, high, low, close, volume, 可选funding_rate, 可选oi_amount])
    """
    bar = f"{bar_minutes}min"
    kline = kline_df.copy()
    time_col = _pick_column(kline, ['timestamp', 'open_time', 'time', 'ts'], 'kline')
    kline['dt'] = pd.to_datetime(kline[time_col], unit='ms', utc=True)
    kline = kline.drop_duplicates(subset=[time_col]).sort_values('dt').set_index('dt')

    df = kline.resample(bar, label='left', closed='left').agg(
        open=('open', 'first'),
        high=('high', 'max'),
        low=('low', 'min'),
        close=('close', 'last'),
        volume=('volume', 'sum'),
    )
    df['close'] = df['close'].ffill()
    df = df[df['close'].notna()]
    for col in ('open', 'high', 'low'):
        df[col] = df[col].fillna(df['close'])

    extra_cols = []
    if fr_df is not None and not fr_df.empty:
        _attach_series(
            df, fr_df,
            ['timestamp', 'fundingTime', 'time', 'ts'],
            ['funding_rate', 'fundingRate', 'rate'],
            'funding_rate', bar, 'fr',
        )
        extra_cols.append('funding_rate')

    if oi_df is not None and not oi_df.empty:
        _attach_series(
            df, oi_df,
            ['timestamp', 'time', 'ts'],
            ['oi_amount', 'openInterest', 'open_interest', 'sumOpenInterest', 'oi'],
            'oi_amount', bar, 'oi',
        )
        extra_cols.append('oi_amount')

    if extra_cols:
        starts = [df[col].first_valid_index() for col in extra_cols]
        starts = [start for start in starts if start is not None]
        if starts:
            df = df.loc[max(starts):].copy()
        df = df.dropna(subset=extra_cols)

    return df[df['close'] > 0]


# =============================================================================
# 三、单标的持久化账本状态机
# =============================================================================
def _sync_persistent_signal_ledger(history_file, symbol, new_record, cols):
    """
    读取历史账本，用最后事件约束 OPEN/CLOSE 状态，按时间戳去重，补算平仓 PnL 后落盘。

    入参形貌: new_record 为 None 或包含 cols 全部键，至少含 event/price/direction/signal_timestamp_ms。
    出参形貌: DataFrame(cols=cols)，仅包含当前 symbol 的真实历史事件。

    common_utils: 该 CSV 是跨进程状态源但没有文件锁；并发进程同时读改写时仍可能互相覆盖。
    common_utils: 历史 CSV 读取失败按原设计视为空账本继续运行，这会优先保证信号链路可用性，
           但损坏文件可能造成持仓状态丢失；业务上若更看重一致性，应改为失败即停止。
    """
    history_file = os.path.join('signal_data', history_file)
    os.makedirs(os.path.dirname(history_file), exist_ok=True)

    history = pd.DataFrame(columns=cols)
    if os.path.exists(history_file):
        try:
            history = pd.read_csv(
                history_file,
                dtype={'signal_timestamp_ms': 'int64', 'symbol': str},
            )
        except Exception:
            # 明确保留原容错边界：历史账本损坏时按空账本继续，不在此处改变业务可用性策略。
            history = pd.DataFrame(columns=cols)

    for col in cols:
        if col not in history.columns:
            history[col] = None

    symbol_history = history[history['symbol'] == symbol].copy()
    last_event, last_open_price = None, 0.0
    if not symbol_history.empty:
        last_row = symbol_history.iloc[-1]
        last_event = str(last_row['event']).upper()
        last_open_price = float(last_row['price']) if pd.notna(last_row['price']) else 0.0

    valid_record = None
    if new_record is not None:
        candidate = dict(new_record)
        event = candidate['event']
        if event == 'OPEN' and last_event != 'OPEN':
            valid_record = candidate
        elif event == 'CLOSE' and last_event == 'OPEN':
            pnl = (
                (candidate['price'] - last_open_price) / last_open_price * 100
                if last_open_price > 0 else 0.0
            )
            candidate['pnl'] = -pnl if candidate['direction'] == 'SHORT' else pnl
            valid_record = candidate

    if valid_record is None:
        return symbol_history[cols] if not symbol_history.empty else pd.DataFrame(columns=cols)

    duplicate = (not history.empty) and bool(
        ((history['symbol'] == symbol) &
         (history['signal_timestamp_ms'] == valid_record['signal_timestamp_ms'])).any()
    )
    if duplicate:
        return symbol_history[cols] if not symbol_history.empty else pd.DataFrame(columns=cols)

    new_row = pd.DataFrame([valid_record], columns=cols)
    history = pd.concat([history, new_row], ignore_index=True)
    history.to_csv(history_file, index=False, encoding='utf-8-sig')
    symbol_history = pd.concat([symbol_history, new_row], ignore_index=True)
    return symbol_history[cols]


def _build_record(strategy_name, symbol, coin, event, direction, price, reason,
                  signal_ts_ms, target_weight, max_weight):
    """组装标准事件；action 由 direction + event 唯一推导。"""
    action = 'BUY' if (direction == 'LONG') == (event == 'OPEN') else 'SELL'
    return {
        'time': _fmt_bjt(signal_ts_ms),
        'action': action,
        'coin': coin,
        'direction': direction,
        'event': event,
        'price': price,
        'reason': reason,
        'target_weight': target_weight,
        'pnl': None,
        'top_k': 1,
        'max_weight': max_weight,
        'signal_timestamp_ms': signal_ts_ms,
        'STRATEGY_NAME': strategy_name,
        'symbol': symbol,
    }


# =============================================================================
# 四、单标的“最后一根闭合K线”策略
# =============================================================================
def generate_top_long_signals(df):
    """
    top_coin_long：高位长上影 + 爆量开多；孕线突破 + 爆量平多。

    入参形貌: [timestamp, open, high, low, close, volume, symbol, coin_name]
    出参: (兼容占位空列表, DataFrame(SIGNAL_COLS))
    """
    params = {
        'BAR_MINUTES': 60, 'UPPER_WICK_THRESH': 0.60, 'VOL_QUANTILE': 0.9,
        'HIGH_CLOSE_THRESH': 0.90, 'WARMUP_DAYS': 30,
    }
    warmup = 24 * params['WARMUP_DAYS']
    if df is None or len(df) < warmup:
        return _empty_signal_result()

    if 'timestamp' in df.columns and not df['timestamp'].is_monotonic_increasing:
        df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')

    symbol, coin = _resolve_identity(df)
    open_, high, low, close, volume = df['open'], df['high'], df['low'], df['close'], df['volume']
    max_high = high.rolling(24, min_periods=12).max()
    upper_wick = (high - np.maximum(open_, close)) / ((high - low) + 1e-12)
    inside_bar = (high < high.shift(1)) & (low > low.shift(1))
    volume_threshold = volume.rolling(warmup, min_periods=50).quantile(params['VOL_QUANTILE']).shift(1)
    volume_spike = volume > volume_threshold

    entry = (close / (max_high + 1e-12) > params['HIGH_CLOSE_THRESH']) & (
            upper_wick > params['UPPER_WICK_THRESH']
    ) & volume_spike
    exit_ = inside_bar.shift(1, fill_value=False) & (close > high.shift(1)) & volume_spike

    is_entry, is_exit = bool(entry.iloc[-1]), bool(exit_.iloc[-1])
    record = None
    if is_entry or is_exit:
        signal_ts = int(df['timestamp'].iloc[-1]) + params['BAR_MINUTES'] * 60 * 1000
        price, current_volume = float(close.iloc[-1]), float(volume.iloc[-1])
        threshold = float(volume_threshold.iloc[-1])
        current_wick = float(upper_wick.iloc[-1])
        if is_entry:
            record = _build_record(
                'top_coin_long', symbol, coin, 'OPEN', 'LONG', price,
                f"高位长上影({current_wick:.2f}) + 爆量({current_volume:.0f} > {threshold:.0f})",
                signal_ts, 1.0, 0.14,
            )
        else:
            record = _build_record(
                'top_coin_long', symbol, coin, 'CLOSE', 'LONG', price,
                f"孕线突破 + 爆量({current_volume:.0f} > {threshold:.0f})",
                signal_ts, 0.0, 0.14,
            )

    return [], _sync_persistent_signal_ledger(
        'signal_history_top_coin_long.csv', symbol, record, SIGNAL_COLS
    )


def generate_multi_ma_signals(raw_df, bar_minutes=5):
    """
    multi_ma_break_long：价格同时跌破 24/48/72h 均线开多；48h/168h 死叉平多。

    入参形貌: [timestamp, close, symbol, coin_name]
    """
    params = {
        'ENTRY_MA_HOURS': [24, 48, 72],
        'EXIT_FAST_MA_HOURS': 48,
        'EXIT_SLOW_MA_HOURS': 168,
        'TARGET_WEIGHT': 1.0,
        'MAX_WEIGHT': 0.14,
    }
    if raw_df is None or raw_df.empty:
        return _empty_signal_result()

    symbol, coin = _resolve_identity(raw_df)
    df = raw_df
    if 'timestamp' in df.columns and not df['timestamp'].is_monotonic_increasing:
        df = df.drop_duplicates(subset=['timestamp']).sort_values('timestamp')

    close = df['close']
    slow_bars = _bars(params['EXIT_SLOW_MA_HOURS'], bar_minutes)
    if len(close) < max(2, slow_bars):
        return _empty_signal_result()

    def moving_average(hours):
        bars = _bars(hours, bar_minutes)
        return close.rolling(bars, min_periods=max(2, bars // 2)).mean()

    h1, h2, h3 = params['ENTRY_MA_HOURS']
    ma1, ma2, ma3 = moving_average(h1), moving_average(h2), moving_average(h3)
    ma_slow = moving_average(params['EXIT_SLOW_MA_HOURS'])

    price = float(close.iloc[-1])
    v1, v2, v3 = float(ma1.iloc[-1]), float(ma2.iloc[-1]), float(ma3.iloc[-1])
    current_fast, previous_fast = float(ma2.iloc[-1]), float(ma2.iloc[-2])
    current_slow, previous_slow = float(ma_slow.iloc[-1]), float(ma_slow.iloc[-2])
    is_entry = price < v1 and price < v2 and price < v3
    is_exit = current_fast < current_slow and previous_fast >= previous_slow

    record = None
    if is_entry or is_exit:
        signal_ts = int(df['timestamp'].iloc[-1]) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                'multi_ma_break_long', symbol, coin, 'OPEN', 'LONG', price,
                f"均线跌破(C<{v1:.4f}, C<{v2:.4f}, C<{v3:.4f})",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                'multi_ma_break_long', symbol, coin, 'CLOSE', 'LONG', price,
                f"快慢死叉(MA{params['EXIT_FAST_MA_HOURS']} < MA{params['EXIT_SLOW_MA_HOURS']})",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        'signal_history_multi_ma_break_long.csv', symbol, record, SIGNAL_COLS
    )


def generate_XSR_signals(kline_df, fr_df, bar_minutes=15):
    """
    XSR：4h 收益率历史排名 >98% 开多；资金费率排名极低或转负平多。

    入参形貌: kline_df=原始K线，fr_df=资金费率。
    """
    params = {
        'M_HOURS': 4, 'W_DAYS': 14, 'ENTRY_RANK_THRESHOLD': 0.98,
        'EXIT_FR_RANK_THRESHOLD': 0.20, 'TARGET_WEIGHT': 1.0,
        'MAX_WEIGHT': 1.0 / 30 / 2.1, 'STRATEGY_NAME': 'XSR',
    }
    if kline_df is None or kline_df.empty or fr_df is None or fr_df.empty:
        return _empty_signal_result()

    symbol, coin = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df)
    window = _bars(params['W_DAYS'] * 24, bar_minutes)
    if len(df) < window:
        return _empty_signal_result()

    momentum_bars = _bars(params['M_HOURS'], bar_minutes)
    min_periods = max(50, window // 5)
    close, funding = df['close'], df['funding_rate']
    return_rank = close.pct_change(momentum_bars).rolling(window, min_periods=min_periods).rank(pct=True)
    funding_rank = funding.rolling(window, min_periods=min_periods).rank(pct=True)

    current_rank = float(return_rank.iloc[-1])
    current_funding_rank = float(funding_rank.iloc[-1])
    current_funding, price = float(funding.iloc[-1]), float(close.iloc[-1])
    is_entry = current_rank > params['ENTRY_RANK_THRESHOLD']
    is_exit = current_funding_rank < params['EXIT_FR_RANK_THRESHOLD'] or current_funding < 0

    record = None
    if is_entry or is_exit:
        signal_ts = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'OPEN', 'LONG', price,
                f"SURGE_EXTREME(rk_ret_M: {current_rank:.3f} > {params['ENTRY_RANK_THRESHOLD']})",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'CLOSE', 'LONG', price,
                f"FR_LOW_NEG(fr_rank: {current_funding_rank:.3f} < {params['EXIT_FR_RANK_THRESHOLD']} "
                f"or fr: {current_funding:.4%}<0)",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        f"signal_history_{params['STRATEGY_NAME']}.csv", symbol, record, SIGNAL_COLS
    )


def generate_short_fr_signals(kline_df, fr_df, bar_minutes=15):
    """
    fr_short：资金费率历史排名 >95% 开空；24h收益强且费率排名回落时平空。

    入参形貌: kline_df=原始K线，fr_df=资金费率。
    """
    params = {
        'N_HOURS': 24, 'W_DAYS': 14, 'EXTREME_FR_RANK_THRESHOLD': 0.95,
        'STRONG_RET_RANK_THRESHOLD': 0.80, 'MILD_FR_RANK_THRESHOLD': 0.50,
        'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 7 / 1.6, 'STRATEGY_NAME': 'fr_short',
    }
    if kline_df is None or kline_df.empty or fr_df is None or fr_df.empty:
        return _empty_signal_result()

    symbol, coin = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df)
    window = _bars(params['W_DAYS'] * 24, bar_minutes)
    if len(df) < window:
        return _empty_signal_result()

    return_bars = _bars(params['N_HOURS'], bar_minutes)
    min_periods = max(50, window // 5)
    close, funding = df['close'], df['funding_rate']
    return_rank = close.pct_change(return_bars).rolling(window, min_periods=min_periods).rank(pct=True)
    funding_rank = funding.rolling(window, min_periods=min_periods).rank(pct=True)

    current_return_rank = float(return_rank.iloc[-1])
    current_funding_rank = float(funding_rank.iloc[-1])
    price = float(close.iloc[-1])
    is_entry = current_funding_rank > params['EXTREME_FR_RANK_THRESHOLD']
    is_exit = (
            current_return_rank > params['STRONG_RET_RANK_THRESHOLD']
            and current_funding_rank < params['MILD_FR_RANK_THRESHOLD']
    )

    record = None
    if is_entry or is_exit:
        signal_ts = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'OPEN', 'SHORT', price,
                f"EXTREME_HIGH_FR(fr_rank:{current_funding_rank:.3f}>{params['EXTREME_FR_RANK_THRESHOLD']})",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'CLOSE', 'SHORT', price,
                f"COLD_START(ret24_rk:{current_return_rank:.3f}>{params['STRONG_RET_RANK_THRESHOLD']}"
                f"&fr_rank:{current_funding_rank:.3f}<{params['MILD_FR_RANK_THRESHOLD']})",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        f"signal_history_{params['STRATEGY_NAME']}.csv", symbol, record, SIGNAL_COLS
    )


def generate_vol_fr_signals(kline_df, fr_df, bar_minutes=5):
    """
    vol_breakout_fr_recovery_long：4h前低波动、当前高波动开多；费率从极低位回升平多。

    入参形貌: kline_df=原始K线，fr_df=资金费率。
    """
    params = {
        'M_HOURS': 4, 'N_HOURS': 24, 'W_DAYS': 14,
        'ATR_RANK_LOW_TH': 0.20, 'ATR_RANK_HIGH_TH': 0.60, 'FR_RANK_LOW_TH': 0.10,
        'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 2.2,
        'STRATEGY_NAME': 'vol_breakout_fr_recovery_long',
    }
    if kline_df is None or kline_df.empty or fr_df is None or fr_df.empty:
        return _empty_signal_result()

    symbol, coin = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df)
    window = _bars(params['W_DAYS'] * 24, bar_minutes)
    if len(df) < window:
        return _empty_signal_result()

    m_bars = _bars(params['M_HOURS'], bar_minutes)
    n_bars = _bars(params['N_HOURS'], bar_minutes)
    min_periods = max(50, window // 5)
    high, low, close, funding = df['high'], df['low'], df['close'], df['funding_rate']
    previous_close = close.shift(1)
    true_range = pd.concat(
        [high - low, (high - previous_close).abs(), (low - previous_close).abs()], axis=1
    ).max(axis=1)
    atr_pct = true_range.rolling(n_bars, min_periods=max(2, n_bars // 2)).mean() / (close + 1e-12)
    atr_rank = atr_pct.rolling(window, min_periods=min_periods).rank(pct=True)
    funding_rank = funding.rolling(window, min_periods=min_periods).rank(pct=True)

    current_atr_rank = float(atr_rank.iloc[-1])
    previous_m_atr_rank = _tail_float(atr_rank, m_bars)
    current_funding_rank = float(funding_rank.iloc[-1])
    previous_m_funding_rank = _tail_float(funding_rank, m_bars)
    previous_funding_rank = _tail_float(funding_rank, 1)
    price = float(close.iloc[-1])

    is_entry = (
            previous_m_atr_rank < params['ATR_RANK_LOW_TH']
            and current_atr_rank > params['ATR_RANK_HIGH_TH']
    )
    is_exit = (
            previous_m_funding_rank < params['FR_RANK_LOW_TH']
            and current_funding_rank > previous_funding_rank
    )

    record = None
    if is_entry or is_exit:
        signal_ts = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'OPEN', 'LONG', price,
                f"VOL_LOW_TO_HIGH(rk_atr_{params['M_HOURS']}h_ago:{previous_m_atr_rank:.3f}"
                f"<{params['ATR_RANK_LOW_TH']} & curr:{current_atr_rank:.3f}>{params['ATR_RANK_HIGH_TH']})",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'CLOSE', 'LONG', price,
                f"FR_RECOVERY_FROM_LOW(rk_fr_{params['M_HOURS']}h_ago:{previous_m_funding_rank:.3f}"
                f"<{params['FR_RANK_LOW_TH']} & curr:{current_funding_rank:.3f}>prev:{previous_funding_rank:.3f})",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        f"signal_history_{params['STRATEGY_NAME']}.csv", symbol, record, SIGNAL_COLS
    )


def generate_bottom_powder_short_signals(kline_df, fr_df, oi_df, bar_minutes=15):
    """
    bottom_stabilize_powder_keg_short：底部企稳+OI抬升+悲观费率开空；OI极高且成交萎缩平空。

    入参形貌: kline_df=原始K线，fr_df=资金费率，oi_df=持仓量。
    """
    params = {
        'N_HOURS': 24, 'M_HOURS': 4, 'W_DAYS': 14,
        'POWDER_OI_RK': 0.90, 'POWDER_VOL_RK': 0.30,
        'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 1.4,
        'STRATEGY_NAME': 'bottom_stabilize_powder_keg_short',
    }
    if (
            kline_df is None or kline_df.empty
            or fr_df is None or fr_df.empty
            or oi_df is None or oi_df.empty
    ):
        return _empty_signal_result()

    symbol, coin = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df, oi_df=oi_df)
    window = _bars(params['W_DAYS'] * 24, bar_minutes)
    if len(df) < window:
        return _empty_signal_result()

    n_bars = _bars(params['N_HOURS'], bar_minutes)
    m_bars = _bars(params['M_HOURS'], bar_minutes)
    min_periods = max(50, window // 5)
    close, low, volume = df['close'], df['low'], df['volume']
    oi_amount, funding = df['oi_amount'], df['funding_rate']

    min_low = low.rolling(n_bars, min_periods=max(2, n_bars // 2)).min()
    oi_min = oi_amount.rolling(m_bars, min_periods=2).min()
    oi_rank = oi_amount.rolling(window, min_periods=min_periods).rank(pct=True)
    volume_rank = volume.rolling(window, min_periods=min_periods).rank(pct=True)
    funding_rank = funding.rolling(window, min_periods=min_periods).rank(pct=True)

    price = float(close.iloc[-1])
    current_min_low, previous_n_min_low = _tail_float(min_low), _tail_float(min_low, n_bars)
    current_oi, current_oi_min = _tail_float(oi_amount), _tail_float(oi_min)
    current_funding_rank, current_funding = _tail_float(funding_rank), _tail_float(funding)
    current_oi_rank, current_volume_rank = _tail_float(oi_rank), _tail_float(volume_rank)

    oi_bottom_divergence = (
            price / (current_min_low + 1e-12) < 1.03
            and current_oi > current_oi_min * 1.05
    )
    funding_low_or_negative = current_funding_rank < 0.20 or current_funding < 0
    higher_lows = current_min_low > previous_n_min_low
    is_entry = oi_bottom_divergence and funding_low_or_negative and higher_lows
    is_exit = (
            current_oi_rank > params['POWDER_OI_RK']
            and current_volume_rank < params['POWDER_VOL_RK']
    )

    record = None
    if is_entry or is_exit:
        signal_ts = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'OPEN', 'SHORT', price,
                f"BOTTOM_STABILIZE(c/L:<1.03, oi_amt:{current_oi:.2f}>minM*1.05, fr_rk<0.2 or fr<0)",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'CLOSE', 'SHORT', price,
                f"POWDER_KEG(rk_oi:{current_oi_rank:.2f}>{params['POWDER_OI_RK']}, "
                f"rk_v:{current_volume_rank:.2f}<{params['POWDER_VOL_RK']})",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        f"signal_history_{params['STRATEGY_NAME']}.csv", symbol, record, SIGNAL_COLS
    )


def generate_oi_decay_short_signals(kline_df, oi_df, bar_minutes=30):
    """
    oi_value_decay_short：OI名义价值 EMA4h 下穿 EMA24h 开空；OI极高但价格不热平空。

    入参形貌: kline_df=原始K线，oi_df=持仓量。

    common_utils: 原逻辑给该空头策略 TARGET_WEIGHT=-1.0，而其他空头策略使用正权重；
           下游若把 target_weight 视为绝对仓位比例，这一符号约定不一致。为保持业务行为，此处不改。
    """
    params = {
        'M_HOURS': 4, 'N_HOURS': 24, 'W_DAYS': 14,
        'OI_RANK_EXTREME_TH': 0.95, 'OI_HOT_TH': 0.050,
        'TARGET_WEIGHT': -1.0, 'MAX_WEIGHT': 1.0 / 1.4,
        'STRATEGY_NAME': 'oi_value_decay_short',
    }
    if kline_df is None or kline_df.empty or oi_df is None or oi_df.empty:
        return _empty_signal_result()

    symbol, coin = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, oi_df=oi_df)
    window = _bars(params['W_DAYS'] * 24, bar_minutes)
    if len(df) < window:
        return _empty_signal_result()

    m_bars = _bars(params['M_HOURS'], bar_minutes)
    n_bars = _bars(params['N_HOURS'], bar_minutes)
    min_periods = max(50, window // 5)
    close, oi_amount = df['close'], df['oi_amount']

    oi_value = oi_amount * close
    ema_fast = oi_value.ewm(span=m_bars, adjust=False).mean()
    ema_slow = oi_value.ewm(span=n_bars, adjust=False).mean()
    price_ma = close.rolling(n_bars, min_periods=max(2, n_bars // 2)).mean()
    oi_rank = oi_amount.rolling(window, min_periods=min_periods).rank(pct=True)

    price = float(close.iloc[-1])
    current_price_ma, current_oi_rank = _tail_float(price_ma), _tail_float(oi_rank)
    current_fast, current_slow = _tail_float(ema_fast), _tail_float(ema_slow)
    previous_fast, previous_slow = _tail_float(ema_fast, 1), _tail_float(ema_slow, 1)
    is_entry = current_fast < current_slow and previous_fast >= previous_slow
    is_exit = (
            current_oi_rank > params['OI_RANK_EXTREME_TH']
            and price / (current_price_ma + 1e-12) - 1.0 < params['OI_HOT_TH']
    )

    record = None
    if is_entry or is_exit:
        signal_ts = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'OPEN', 'SHORT', price,
                f"OI_VALUE_DEAD_CROSS(EMA4h:{current_fast:.2f} < EMA24h:{current_slow:.2f})",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'CLOSE', 'SHORT', price,
                f"OI_EXTREME_PRICE_NOT_HOT(Rank_OI:{current_oi_rank:.2f}"
                f">{params['OI_RANK_EXTREME_TH']} & Dev<5%)",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        f"signal_history_{params['STRATEGY_NAME']}.csv", symbol, record, SIGNAL_COLS
    )


def generate_high_fr_bear_div_short_signals(kline_df, fr_df, bar_minutes=15):
    """
    short_high_fr_bear_div：高正资金费率开空；价格创新高但费率下降时平空。

    入参形貌: kline_df=原始K线，fr_df=资金费率。

    common_utils: 业务描述写“资金费率绝对值极高”，原实现实际是 curr_fr > 0.001，
           即只接受高正费率而不是 abs(curr_fr)>阈值。为避免改变策略边界，保留原判断。
    """
    params = {
        'N_HOURS': 24, 'FR_ABS_TH': 0.001,
        'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 2.2,
        'STRATEGY_NAME': 'short_high_fr_bear_div',
    }
    if kline_df is None or kline_df.empty or fr_df is None or fr_df.empty:
        return _empty_signal_result()

    symbol, coin = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df)
    n_bars = _bars(params['N_HOURS'], bar_minutes)
    if len(df) < n_bars + 1:
        return _empty_signal_result()

    close, funding = df['close'], df['funding_rate']
    current_price, previous_price = float(close.iloc[-1]), _tail_float(close, n_bars)
    current_funding, previous_funding = float(funding.iloc[-1]), _tail_float(funding, n_bars)
    is_entry = current_funding > params['FR_ABS_TH']
    is_exit = current_price > previous_price and current_funding < previous_funding

    record = None
    if is_entry or is_exit:
        signal_ts = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'OPEN', 'SHORT', current_price,
                f"FR_ABSOLUTE_HIGH(fr:{current_funding:.5f} > {params['FR_ABS_TH']})",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'CLOSE', 'SHORT', current_price,
                f"FR_PRICE_BEAR_DIV(P_curr:{current_price:.4f}>P_24h:{previous_price:.4f} "
                f"& fr_curr:{current_funding:.5f}<fr_24h:{previous_funding:.5f})",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        f"signal_history_{params['STRATEGY_NAME']}.csv", symbol, record, SIGNAL_COLS
    )


def generate_vwap_reclaim_long_signals(kline_df, fr_df, oi_df, bar_minutes=30):
    """
    vwap_reclaim_oi_long：VWAP 假跌破收回 + OI抬升 + 温和费率开多；费率极低/转负平多。

    入参形貌: kline_df=原始K线，fr_df=资金费率，oi_df=持仓量。
    """
    params = {
        'N_HOURS': 24, 'W_DAYS': 14,
        'TARGET_WEIGHT': 1.0, 'MAX_WEIGHT': 1.0 / 2.2,
        'STRATEGY_NAME': 'vwap_reclaim_oi_long',
    }
    if (
            kline_df is None or kline_df.empty
            or fr_df is None or fr_df.empty
            or oi_df is None or oi_df.empty
    ):
        return _empty_signal_result()

    symbol, coin = _resolve_identity(kline_df)
    df = _build_aligned_frame(kline_df, bar_minutes, fr_df=fr_df, oi_df=oi_df)
    window = _bars(params['W_DAYS'] * 24, bar_minutes)
    if len(df) < window:
        return _empty_signal_result()

    n_bars = _bars(params['N_HOURS'], bar_minutes)
    min_periods = max(50, window // 5)
    close, low, volume = df['close'], df['low'], df['volume']
    oi_amount, funding = df['oi_amount'], df['funding_rate']
    volume_sum = volume.rolling(n_bars, min_periods=max(2, n_bars // 2)).sum()
    vwap = (close * volume).rolling(n_bars, min_periods=max(2, n_bars // 2)).sum() / (volume_sum + 1e-12)
    funding_rank = funding.rolling(window, min_periods=min_periods).rank(pct=True)

    current_close, current_low = float(close.iloc[-1]), float(low.iloc[-1])
    current_vwap = _tail_float(vwap)
    current_oi, previous_oi = _tail_float(oi_amount), _tail_float(oi_amount, n_bars)
    current_funding_rank, current_funding = _tail_float(funding_rank), _tail_float(funding)

    reclaim = current_low < current_vwap < current_close
    oi_rising = current_oi > previous_oi
    funding_mild = 0.10 < current_funding_rank < 0.90
    is_entry = reclaim and oi_rising and funding_mild
    is_exit = current_funding_rank < 0.20 or current_funding < 0

    record = None
    if is_entry or is_exit:
        signal_ts = int(df.index[-1].timestamp() * 1000) + int(bar_minutes * 60 * 1000)
        if is_entry:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'OPEN', 'LONG', current_close,
                f"VWAP_RECLAIM_OI(L<{current_vwap:.4f}<C, oi>{previous_oi:.2f}, 10%<fr_rk<90%)",
                signal_ts, params['TARGET_WEIGHT'], params['MAX_WEIGHT'],
            )
        else:
            record = _build_record(
                params['STRATEGY_NAME'], symbol, coin, 'CLOSE', 'LONG', current_close,
                f"FR_LOW_NEG(fr_rk:{current_funding_rank:.2f}<0.20 or fr:{current_funding:.4%}<0)",
                signal_ts, 0.0, params['MAX_WEIGHT'],
            )

    return [], _sync_persistent_signal_ledger(
        f"signal_history_{params['STRATEGY_NAME']}.csv", symbol, record, SIGNAL_COLS
    )


# =============================================================================
# 五、单标的策略统一工作流
# =============================================================================
def print_top_long_latest_signals(final_signals_df, logger, timeframe='1h'):
    """
    从全量账本中提取“当前北京时间截面”的事件并聚合打印。

    入参形貌: final_signals_df 至少含 [time, event, action, coin, direction, price, reason, pnl, target_weight]。

    common_utils: 该函数按“机器当前时间”找最新截面，而不是按工作流传入的 target_time；
           回放历史 target_time 时日志可能显示“无当前信号”。保持原行为以免改变实盘调用语义。
    """
    if final_signals_df is None or final_signals_df.empty:
        logger.info("[发单指令] 全量账本为空 | 结果: [当前无开平仓信号，保持现有仓位]")
        return

    strategy_name = (
        final_signals_df['STRATEGY_NAME'].iloc[0]
        if 'STRATEGY_NAME' in final_signals_df.columns else 'top_coin_long'
    )
    current_bjt = pd.Timestamp.now(tz='Asia/Shanghai').floor(timeframe.lower().replace('m', 'min'))
    latest_time = current_bjt.strftime('%Y-%m-%d %H:%M:%S')
    latest = final_signals_df[final_signals_df['time'] == latest_time]

    if latest.empty:
        logger.info(
            f"[发单指令/{strategy_name}] 截面: [{latest_time}] (北京时间) | "
            f"结果: [无开平仓信号，保持现有仓位]"
        )
        return

    lines = [
        f"[发单指令/{strategy_name}] 截面: [{latest_time}] (北京时间) | 待执行: [{len(latest)}] 条"
    ]
    for _, row in latest.iterrows():
        base = (
            f"{row['action']:<4} {row.get('coin', 'UNKNOWN'):<8} | "
            f"方向: [{row.get('direction', 'LONG')}] | 价格: [{row.get('price', 0.0)}]"
        )
        if row['event'] == 'CLOSE':
            pnl = row.get('pnl')
            pnl_text = f"{pnl:.2f}%" if pd.notna(pnl) else 'N/A'
            lines.append(
                f"  ► 🔴 平仓 | {base} | 本次盈亏: [{pnl_text}] | 原因: [{row.get('reason', '')}]"
            )
        else:
            lines.append(
                f"  ► 🟢 开仓 | {base} | 目标权重: [{row.get('target_weight', 0.0) * 100:.1f}%] | "
                f"原因: [{row.get('reason', '')}]"
            )
    logger.info("\n".join(lines))


def _warn_data_gap(logger, label, symbol, df_kline, expected_rows):
    """K线不足时一次性输出缺口、可用区间与常见原因。"""
    actual = len(df_kline)
    if actual >= expected_rows:
        return

    span = '未知区间'
    if 'timestamp' in df_kline.columns and actual:
        span = f"{_fmt_bjt(df_kline['timestamp'].iloc[0])} ~ {_fmt_bjt(df_kline['timestamp'].iloc[-1])}"
    logger.warning(
        f"⚠️ [{label}/数据体检] 标的: [{symbol}] | K线预期: [{expected_rows}] | 实际: [{actual}] | "
        f"缺口: [{expected_rows - actual}] | 可用区间: [{span}] (北京时间) | "
        f"影响: [滚动指标可能失真] | 排查线索: [合约上线较晚/交易所限频/网络或代理不稳定]"
    )


def _run_signal_workflow(label, target_time, symbol_list, timeframe, bar_minutes, lookback_days,
                         signal_fn, output_path, proxy_url, need_funding=False, need_oi=False):
    """
    统一单标的策略工作流：取数 -> 数据体检 -> 信号状态机 -> 聚合去重 -> 发单日志 -> CSV。

    入参形貌:
      signal_fn(kline_df, fr_df_or_None, oi_df_or_None) -> DataFrame(SIGNAL_COLS)
    出参形貌:
      DataFrame(SIGNAL_COLS)；无标的可推演时返回同列空表。
    """
    logger = setup_logger()
    expected_rows = lookback_days * (1440 // bar_minutes) + 1
    logger.info(
        f"🚀 [{label}/启动] 周期: [{timeframe}] | 标的数: [{len(symbol_list)}] | "
        f"预热天数: [{lookback_days}] | 单标的预期K线: [{expected_rows}] | 目标时刻: [{target_time}]"
    )

    kline_map = snipe_kline_data(
        symbol_list=symbol_list, timeframe=timeframe, days=lookback_days,
        target_time_str=target_time, use_ws=True, use_rest=True, proxy_url=proxy_url,
    )
    funding_map = (
        snipe_funding_rate_data(symbol_list=symbol_list, days=lookback_days, proxy_url=proxy_url)
        if need_funding else {}
    )
    oi_map = (
        snipe_oi_data(
            symbol_list=symbol_list, timeframe=timeframe, days=lookback_days,
            target_time_str=target_time, proxy_url=proxy_url,
        )
        if need_oi else {}
    )

    kline_ready = sum(not _frame_of(kline_map, symbol).empty for symbol in symbol_list)
    parts = [f"K线: [{kline_ready}/{len(symbol_list)}]"]
    if need_funding:
        funding_ready = sum(not _frame_of(funding_map, symbol).empty for symbol in symbol_list)
        parts.append(f"资金费率: [{funding_ready}/{len(symbol_list)}]")
    if need_oi:
        oi_ready = sum(not _frame_of(oi_map, symbol).empty for symbol in symbol_list)
        parts.append(f"OI: [{oi_ready}/{len(symbol_list)}]")
    logger.info(f"✅ [{label}/取数完成] " + ' | '.join(parts))

    frames, skipped = [], []
    for symbol in symbol_list:
        kline = _frame_of(kline_map, symbol)
        funding = _frame_of(funding_map, symbol) if need_funding else pd.DataFrame()
        oi = _frame_of(oi_map, symbol) if need_oi else pd.DataFrame()

        if kline.empty:
            skipped.append(f"{symbol}(K线为空)")
            continue
        if need_funding and funding.empty:
            skipped.append(f"{symbol}(资金费率为空)")
            continue
        if need_oi and oi.empty:
            skipped.append(f"{symbol}(OI为空)")
            continue

        _warn_data_gap(logger, label, symbol, kline, expected_rows)
        kline = kline.copy()
        kline['coin_name'] = symbol.split('/')[0]
        kline['symbol'] = symbol

        try:
            frames.append(signal_fn(
                kline,
                funding if need_funding else None,
                oi if need_oi else None,
            ))
        except Exception as exc:
            logger.error(
                f"❌ [{label}/推演失败] 标的: [{symbol}] | 当前动作: [计算最新策略信号] | "
                f"原因: [{exc}] | 排查线索: [检查K线/资金费率/OI列名、时间戳和滚动窗口长度]",
                exc_info=True,
            )
            raise

    if skipped:
        logger.warning(
            f"⚠️ [{label}/数据缺口] 跳过标的数: [{len(skipped)}] | 明细: [{', '.join(skipped)}] | "
            f"排查线索: [交易所无合约/接口限频/网络或代理异常]"
        )

    if not frames:
        logger.info(f"[{label}/收官] 可推演标的: [0] | 结果: [未产生有效信号]")
        return pd.DataFrame(columns=SIGNAL_COLS)

    final_df = pd.concat(frames, ignore_index=True)
    if not final_df.empty:
        final_df.drop_duplicates(
            subset=['symbol', 'signal_timestamp_ms', 'event'], inplace=True
        )
    print_top_long_latest_signals(final_df, logger, timeframe=timeframe)

    output_path = os.path.join('signal_data', output_path)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    # logger.info(f"✅ [{label}/账本落盘] 文件: [{output_path}] | 记录数: [{len(final_df)}]")
    return final_df


def execute_trading_bot_workflow_top_long(target_time, symbol_list, proxy_url=None):
    """top_coin_long：1h 高位长上影+爆量开多，孕线突破+爆量平多。"""
    return _run_signal_workflow(
        'top_long', target_time, symbol_list, '1h', 60, 210,
        lambda k, fr, oi: generate_top_long_signals(k)[1],
        'top_long_signals.csv', proxy_url,
    )


def execute_trading_bot_workflow_ma_bottom_long(target_time, symbol_list, proxy_url=None):
    """multi_ma_break_long：5m 多均线跌破开多，快慢均线死叉平多。"""
    return _run_signal_workflow(
        'ma_bottom_long', target_time, symbol_list, '5m', 5, 40,
        lambda k, fr, oi: generate_multi_ma_signals(k)[1],
        'ma_bottom_long_signals.csv', proxy_url,
    )


def execute_trading_bot_workflow_XSR_long(target_time, symbol_list, proxy_url=None):
    """XSR：30m，需资金费率。"""
    return _run_signal_workflow(
        'XSR', target_time, symbol_list, '30m', 30, 20,
        lambda k, fr, oi: generate_XSR_signals(k, fr, bar_minutes=30)[1],
        'XSR_long_signals.csv', proxy_url, need_funding=True,
    )


def execute_trading_bot_workflow_short_fr(target_time, symbol_list, proxy_url=None):
    """fr_short：30m，需资金费率。"""
    return _run_signal_workflow(
        'fr_short', target_time, symbol_list, '30m', 30, 20,
        lambda k, fr, oi: generate_short_fr_signals(k, fr, bar_minutes=30)[1],
        'short_fr_signals.csv', proxy_url, need_funding=True,
    )


def execute_trading_bot_workflow_vol_fr_long(target_time, symbol_list, proxy_url=None):
    """vol_breakout_fr_recovery_long：5m，需资金费率。"""
    return _run_signal_workflow(
        'vol_fr_long', target_time, symbol_list, '5m', 5, 20,
        lambda k, fr, oi: generate_vol_fr_signals(k, fr, bar_minutes=5)[1],
        'vol_fr_long_signals.csv', proxy_url, need_funding=True,
    )


def execute_trading_bot_workflow_bottom_powder_short(target_time, symbol_list, proxy_url=None):
    """bottom_stabilize_powder_keg_short：15m，需资金费率 + OI。"""
    return _run_signal_workflow(
        'bottom_powder_short', target_time, symbol_list, '15m', 15, 20,
        lambda k, fr, oi: generate_bottom_powder_short_signals(k, fr, oi, bar_minutes=15)[1],
        'bottom_powder_short_signals.csv', proxy_url, need_funding=True, need_oi=True,
    )


def execute_trading_bot_oi_decay_short(target_time, symbol_list, proxy_url=None):
    """oi_value_decay_short：30m，仅需 OI。"""
    return _run_signal_workflow(
        'oi_decay_short', target_time, symbol_list, '30m', 30, 20,
        lambda k, fr, oi: generate_oi_decay_short_signals(k, oi, bar_minutes=30)[1],
        'oi_decay_short_signals.csv', proxy_url, need_oi=True,
    )


def execute_trading_bot_high_fr_bear_div_short(target_time, symbol_list, proxy_url=None):
    """short_high_fr_bear_div：15m，需资金费率。"""
    return _run_signal_workflow(
        'high_fr_bear_div_short', target_time, symbol_list, '15m', 15, 5,
        lambda k, fr, oi: generate_high_fr_bear_div_short_signals(k, fr, bar_minutes=15)[1],
        'high_fr_bear_div_short_signals.csv', proxy_url, need_funding=True,
    )


def execute_trading_bot_vwap_reclaim_long(target_time, symbol_list, proxy_url=None):
    """vwap_reclaim_oi_long：30m，需资金费率 + OI。"""
    return _run_signal_workflow(
        'vwap_reclaim_long', target_time, symbol_list, '30m', 30, 20,
        lambda k, fr, oi: generate_vwap_reclaim_long_signals(k, fr, oi, bar_minutes=30)[1],
        'vwap_reclaim_long_signals.csv', proxy_url, need_funding=True, need_oi=True,
    )


# =============================================================================
# 六、4H 横截面动量组合
# =============================================================================
def build_4h_cross_section(logger, minute_klines_list, time_offset='0h'):
    """
    分钟级K线列表重采样为全币种公共 4H 横截面矩阵。

    入参形貌: minute_klines_list=[DataFrame(timestamp, close, coin_name)]。
    出参形貌: index=UTC-naive 4H 时间；每币列为 COIN_open/COIN_high/COIN_low/COIN(close)。
    """
    resampled, minute_starts, minute_ends = [], [], []
    for raw in minute_klines_list:
        if raw is None or raw.empty:
            continue

        coin = raw['coin_name'].iloc[0]
        frame = raw.copy()
        frame['timestamp'] = pd.to_datetime(frame['timestamp'], unit='ms', utc=True).dt.tz_localize(None)
        frame = frame.set_index('timestamp').sort_index()
        bars = frame['close'].resample('4h', offset=time_offset).agg(
            open='first', high='max', low='min', close='last'
        ).dropna(how='all')
        if bars.empty:
            continue

        minute_starts.append(frame.index[0])
        minute_ends.append(frame.index[-1])
        bars.columns = [f'{coin}_open', f'{coin}_high', f'{coin}_low', coin]
        resampled.append(bars)

    if not resampled:
        raise ValueError('传入的 minute_klines_list 全为空或重采样后无有效数据')

    raw_matrix = pd.concat(resampled, axis=1).sort_index()
    main_coins = [
        col for col in raw_matrix.columns
        if not col.endswith(('_open', '_high', '_low')) and col != 'signal_status'
    ]
    starts = [raw_matrix[coin].first_valid_index() for coin in main_coins]
    ends = [raw_matrix[coin].last_valid_index() for coin in main_coins]
    if any(value is None for value in starts + ends):
        raise ValueError('4H矩阵存在全空币种列，无法计算公共时间区间')

    common_start, common_end = max(starts), min(ends)
    if common_start > common_end:
        logger.warning(
            f"⚠️ [4H矩阵/公共区间为空] 币种数: [{len(resampled)}] | Offset: [{time_offset}] | "
            f"公共起点: [{_fmt_bjt(common_start)}] | 公共终点: [{_fmt_bjt(common_end)}] | "
            f"结果: [返回空矩阵]"
        )
        return raw_matrix.iloc[0:0].copy()

    merged = raw_matrix.loc[common_start:common_end].ffill()
    logger.info(
        f"✅ [4H矩阵/组装完成] 币种数: [{len(resampled)}] | Offset: [{time_offset}] | "
        f"1m交集: [{_fmt_bjt(max(minute_starts))} ~ {_fmt_bjt(min(minute_ends))}] | "
        f"4H公共区间: [{_fmt_bjt(common_start)} ~ {_fmt_bjt(common_end)}] | "
        f"矩阵: [{merged.shape[0]}行 x {merged.shape[1]}列]"
    )
    return merged


def run_strategy_simulation(df_cross_section, strategy_params, trade_mode, initial_capital=10000.0,
                            start_trade_date='2026-04-27 00:00:00', logger=None):
    """
    逐根4H状态机：BTC趋势定方向 -> 风险调整动量排名 -> 平旧仓 -> 逆波动率开新仓。

    入参形貌:
      df_cross_section: 每币 {COIN, COIN_open, COIN_high, COIN_low}，必须含 BTC。
      strategy_params: 必含 MOM_WINDOW/VOL_WINDOW/BTC_TREND_WINDOW/MAX_WEIGHT，可含 TOP_K。
    出参形貌:
      DataFrame[time, action, coin, direction, event, price, amount, value, fee, reason,
                target_weight, pnl, top_k, max_weight]
    副作用: 在 df_cross_section 写入 signal_status。

    common_utils: 默认 start_trade_date 固定为 2026-04-27，这是业务门槛而不是技术必需；保留原值。
    """
    mom_window = strategy_params['MOM_WINDOW']
    vol_window = strategy_params['VOL_WINDOW']
    btc_trend_window = strategy_params['BTC_TREND_WINDOW']
    top_k = int(strategy_params.get('TOP_K', 2))
    max_weight = strategy_params['MAX_WEIGHT']
    fee_rate = 0.0

    target_coins = [
        col for col in df_cross_section.columns
        if not col.endswith(('_open', '_high', '_low')) and col != 'signal_status'
    ]
    if 'BTC' not in target_coins:
        raise ValueError('数据矩阵中必须包含 BTC 作为宏观开关')

    required_ohl = [f'{coin}_{suffix}' for coin in target_coins for suffix in ('high', 'low')]
    missing_ohl = [col for col in required_ohl if col not in df_cross_section.columns]
    if missing_ohl:
        raise KeyError(f"4H矩阵缺少必要OHLC列: {missing_ohl}")

    n_coins = len(target_coins)
    coin_to_idx = {coin: idx for idx, coin in enumerate(target_coins)}
    close_df = df_cross_section[target_coins]
    returns_df = close_df.pct_change(mom_window)

    high_df = df_cross_section[[f'{coin}_high' for coin in target_coins]].copy()
    low_df = df_cross_section[[f'{coin}_low' for coin in target_coins]].copy()
    high_df.columns = low_df.columns = target_coins
    previous_close = close_df.shift(1)
    true_range = np.fmax.reduce([
        (high_df - low_df).values,
        (high_df - previous_close).abs().values,
        (low_df - previous_close).abs().values,
    ])
    atr_df = pd.DataFrame(
        true_range, index=df_cross_section.index, columns=target_coins
    ).rolling(window=vol_window).mean()
    volatility_df = atr_df / close_df
    adjusted_momentum_df = returns_df / (volatility_df + 1e-8)
    btc_ma = df_cross_section['BTC'].rolling(window=btc_trend_window).mean()

    momentum = adjusted_momentum_df.values
    volatility = volatility_df.values
    btc_trend = (df_cross_section['BTC'] > btc_ma).values
    closes = close_df.values
    reference_prices = close_df.shift(mom_window).values
    btc_ma_values = btc_ma.values
    time_index = df_cross_section.index

    cash = float(initial_capital)
    positions = np.zeros(n_coins, dtype=float)
    states = {coin: {'qty': 0.0, 'cost': 0.0, 'side': None} for coin in target_coins}
    ledger = []
    diagnostics = ['无信号: 指标预热期'] * len(df_cross_section)
    warmup = max(mom_window, vol_window, btc_trend_window)

    start_ts = pd.Timestamp(start_trade_date) if start_trade_date else None
    if start_ts is not None and start_ts.tzinfo is not None:
        start_ts = start_ts.tz_convert('UTC').tz_localize(None)

    for i in range(warmup, len(df_cross_section)):
        current_time = time_index[i]
        current_prices = closes[i]
        current_momentum, current_volatility = momentum[i], volatility[i]
        btc_trend_on = bool(btc_trend[i])
        total_equity = cash + np.dot(positions, current_prices)

        long_side = btc_trend_on
        side_word = '做多' if long_side else '做空'
        mode_allowed = trade_mode in (
            ('BOTH', 'LONG_ONLY') if long_side else ('BOTH', 'SHORT_ONLY')
        )

        picks = []
        if mode_allowed:
            mask = ~np.isnan(current_momentum) & (
                (current_momentum > 0) if long_side else (current_momentum < 0)
            )
            valid_idx = np.where(mask)[0]
            if valid_idx.size:
                values = current_momentum[valid_idx]
                order = np.argsort(-values if long_side else values, kind='stable')
                picks = [target_coins[idx] for idx in valid_idx[order[:top_k]]]

        candidate_longs = picks if long_side else []
        candidate_shorts = [] if long_side else picks
        if start_ts is not None and current_time < start_ts:
            candidate_longs, candidate_shorts = [], []
            diagnostics[i] = '无信号: 未到设定的发车时间'
        elif picks:
            diagnostics[i] = f"有信号 ({side_word}): {', '.join(picks)}"
        elif mode_allowed:
            diagnostics[i] = (
                f"无信号: 大盘{'看多' if long_side else '看空'}，但所有标的动量均不满足{side_word}阈值"
            )
        else:
            diagnostics[i] = (
                f"无信号: 大盘{'看多' if long_side else '看空'}，但策略模式禁止{side_word}"
            )

        for idx, coin in enumerate(target_coins):
            position = positions[idx]
            if position > 0 and coin not in candidate_longs:
                position_is_long = True
            elif position < 0 and coin not in candidate_shorts:
                position_is_long = False
            else:
                continue

            amount = abs(position)
            price = current_prices[idx]
            value = amount * price
            fee = value * fee_rate
            cost = states[coin]['cost']
            positions[idx] = 0.0

            if position_is_long:
                cash += value - fee
                net_pnl = amount * (price - cost) - fee
                close_reason = (
                    '大盘开关关闭' if not btc_trend_on
                    else ('动量转负退场' if current_momentum[idx] <= 0 else '掉出前K名排名')
                )
            else:
                cash -= value + fee
                net_pnl = amount * (cost - price) - fee
                close_reason = (
                    '大盘开关关闭' if btc_trend_on
                    else ('动量转正退场' if current_momentum[idx] >= 0 else '掉出前K名排名')
                )

            ledger.append({
                'time': current_time,
                'action': 'SELL' if position_is_long else 'BUY',
                'coin': coin,
                'direction': 'LONG' if position_is_long else 'SHORT',
                'event': 'CLOSE',
                'price': price,
                'amount': amount,
                'value': value,
                'fee': fee,
                'reason': close_reason,
                'target_weight': 0.0,
                'pnl': net_pnl / (cost * amount) * 100 if cost > 0 else 0.0,
                'top_k': top_k,
                'max_weight': max_weight,
            })
            states[coin] = {'qty': 0.0, 'cost': 0.0, 'side': None}

        for side_is_long, candidates in ((True, candidate_longs), (False, candidate_shorts)):
            if not candidates:
                continue

            inverse_volatility = [
                1.0 / current_volatility[coin_to_idx[coin]]
                if current_volatility[coin_to_idx[coin]] > 0 else 0.0
                for coin in candidates
            ]
            total_inverse_volatility = sum(inverse_volatility)
            if total_inverse_volatility <= 0:
                continue

            for rank, coin in enumerate(candidates):
                idx = coin_to_idx[coin]
                if positions[idx] != 0:
                    continue

                target_weight = min(inverse_volatility[rank] / total_inverse_volatility, max_weight)
                notional = total_equity * target_weight / (1 + fee_rate)
                if side_is_long and cash < notional:
                    notional = cash / (1 + fee_rate)
                if notional <= 1.0:
                    continue

                price = current_prices[idx]
                fee = notional * fee_rate
                amount = notional / price
                if side_is_long:
                    positions[idx] += amount
                    cash -= notional + fee
                    states[coin] = {'qty': amount, 'cost': price + fee / amount, 'side': 'LONG'}
                else:
                    positions[idx] -= amount
                    cash += notional - fee
                    states[coin] = {'qty': -amount, 'cost': price - fee / amount, 'side': 'SHORT'}

                ledger.append({
                    'time': current_time,
                    'action': 'BUY' if side_is_long else 'SELL',
                    'coin': coin,
                    'direction': 'LONG' if side_is_long else 'SHORT',
                    'event': 'OPEN',
                    'price': price,
                    'amount': amount,
                    'value': notional,
                    'fee': fee,
                    'reason': 'Signal Entry Long' if side_is_long else 'Signal Entry Short',
                    'target_weight': target_weight,
                    'pnl': np.nan,
                    'top_k': top_k,
                    'max_weight': max_weight,
                })

        if logger is not None and i == len(df_cross_section) - 1:
            btc_idx = coin_to_idx['BTC']
            btc_price, btc_ma_value = current_prices[btc_idx], btc_ma_values[i]
            btc_deviation = (btc_price - btc_ma_value) / btc_ma_value if btc_ma_value > 0 else 0.0
            picked = set(candidate_longs) | set(candidate_shorts)
            lines = [
                f"[策略推演/最新截面] 时间: [{current_time}] | 模式: [{trade_mode}] | "
                f"参数: [MOM={mom_window} VOL={vol_window} BTC_MA={btc_trend_window} TOP_K={top_k}]",
                f"  ├─ 大盘开关: [{'ON 多头趋势' if btc_trend_on else 'OFF 空头趋势'}] | "
                f"BTC现价: [{btc_price:.2f}] | BTC均线: [{btc_ma_value:.2f}] | 偏离: [{btc_deviation:+.2%}]",
                f"  ├─ 信号诊断: [{diagnostics[i]}]",
                f"  ├─ 账户状态: 权益 [{total_equity:,.2f}] | 现金 [{cash:,.2f}] | "
                f"持仓标的数 [{int(np.count_nonzero(positions))}]",
            ]
            for coin in target_coins:
                idx = coin_to_idx[coin]
                now_price, ref_price = current_prices[idx], reference_prices[i, idx]
                price_deviation = (now_price - ref_price) / ref_price if ref_price > 0 else 0.0
                tag = f"★入选-{side_word}" if coin in picked else '未入选'
                lines.append(
                    f"  ├─ [{tag}] {coin:<8} | 风险调整动量: [{current_momentum[idx]:>8.4f}] | "
                    f"波动率: [{current_volatility[idx]:.4%}] | 现价: [{now_price:<12.4f}] | "
                    f"零动量阈值价: [{ref_price:<12.4f}] | 价格偏离: [{price_deviation:+.2%}]"
                )
            new_records = sum(record['time'] == current_time for record in ledger)
            lines.append(f"  └─ 本截面新增账本记录: [{new_records}] 条")
            logger.info("\n".join(lines))

    df_cross_section['signal_status'] = diagnostics
    return pd.DataFrame(ledger)


def run_live_pipeline(minute_klines_list, strategy_params_list, logger):
    """
    4H 多参数流水线：矩阵 -> 状态机 -> +4h执行时刻 -> 北京时间/毫秒戳 -> 最新发单 -> 汇总落盘。

    入参形貌:
      minute_klines_list=[DataFrame(timestamp, close, coin_name, symbol)]
      strategy_params_list=[{STRATEGY_NAME,TIME_OFFSET,TRADE_MODE,MOM_WINDOW,VOL_WINDOW,
                             BTC_TREND_WINDOW,MAX_WEIGHT,TOP_K}]
    出参: 全量 4H 交易账本 DataFrame。
    """
    coin_to_symbol = {
        df['coin_name'].iloc[0]: df['symbol'].iloc[0]
        for df in minute_klines_list
        if df is not None and not df.empty and {'coin_name', 'symbol'} <= set(df.columns)
    }

    all_ledgers = []
    for params in strategy_params_list:
        name = params['STRATEGY_NAME']
        offset = params['TIME_OFFSET']
        mode = params['TRADE_MODE']
        logger.info(
            f"⏳ [流水线/{name}] 动作: [组装4H矩阵并推演] | Offset: [{offset}] | 模式: [{mode}]"
        )

        df_4h = build_4h_cross_section(logger, minute_klines_list, time_offset=offset)
        if df_4h.empty:
            logger.warning(
                f"⚠️ [流水线/{name}] 4H矩阵为空 | 结果: [跳过策略] | "
                f"排查线索: [分钟数据缺失或币种之间没有公共时间区间]"
            )
            continue

        ledger = run_strategy_simulation(
            df_cross_section=df_4h,
            strategy_params=params,
            trade_mode=mode,
            logger=logger,
        )
        latest_exec_bjt = (
            (df_4h.index[-1] + pd.Timedelta(hours=4))
            .tz_localize('UTC')
            .tz_convert('Asia/Shanghai')
            .tz_localize(None)
        )

        if ledger.empty:
            logger.info(f"🧠 [流水线/{name}] 推演完成 | 账本记录: [0] | 最新信号时间: [无]")
            latest_signals = pd.DataFrame()
        else:
            ledger['time'] = pd.to_datetime(ledger['time']) + pd.Timedelta(hours=4)
            ledger['signal_timestamp_ms'] = ledger['time'].astype('int64') // 10 ** 6
            ledger['time'] = (
                ledger['time'].dt.tz_localize('UTC')
                .dt.tz_convert('Asia/Shanghai')
                .dt.tz_localize(None)
            )
            ledger['STRATEGY_NAME'] = name

            # common_utils: 缺少 symbol 映射时按原逻辑假定 USDT 永续后缀；
            # 若交易所支持多结算币需重新定义。
            ledger['symbol'] = ledger['coin'].map(coin_to_symbol).fillna(
                ledger['coin'] + '/USDT:USDT'
            )
            all_ledgers.append(ledger)
            logger.info(
                f"🧠 [流水线/{name}] 推演完成 | 账本记录: [{len(ledger)}] | "
                f"最新信号时间: [{ledger['time'].max():%Y-%m-%d %H:%M:%S}] (北京时间)"
            )
            latest_signals = ledger[ledger['time'] == latest_exec_bjt]

        if latest_signals.empty:
            logger.info(
                f"🎯 [发单指令/{name}] ⏰⏰⏰【信号截面: {latest_exec_bjt:%Y-%m-%d %H:%M:%S}】⏰⏰⏰ (北京时间) | "
                f"结果: [无开平仓信号，保持现有仓位]"
            )
            continue

        lines = [
            f"🎯 [发单指令/{name}] ⏰⏰⏰【信号截面: {latest_exec_bjt:%Y-%m-%d %H:%M:%S}】⏰⏰⏰ (北京时间) | "
            f"待执行: [{len(latest_signals)}] 条"
        ]
        for _, row in latest_signals.iterrows():
            base = (
                f"{row['action']:<4} {row['coin']:<8} | 方向: [{row['direction']}] | 价格: [{row['price']}]"
            )
            if row['event'] == 'CLOSE':
                lines.append(
                    f"  ► 🔴 平仓 | {base} | 数量: [{row['amount']:.4f}] | 原因: [{row['reason']}]"
                )
            else:
                lines.append(
                    f"  ► 🔴 开仓 | {base} | 目标权重: [{row['target_weight'] * 100:.1f}%] | "
                    f"原因: [{row['reason']}]"
                )
        logger.info("\n".join(lines))

    if not all_ledgers:
        logger.info('[流水线/收官] 结果: [所有策略均未产生交易账本]')
        return pd.DataFrame()

    output_path = os.path.join('signal_data', 'live_simulation_logs.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    final_ledger = pd.concat(all_ledgers, ignore_index=True)
    final_ledger.to_csv(output_path, index=False, encoding='utf-8-sig')
    logger.info(
        f"✅ [流水线/账本落盘] 文件: [{output_path}] | 记录数: [{len(final_ledger)}] | "
        f"覆盖策略数: [{len(all_ledgers)}]"
    )
    return final_ledger

def execute_trading_bot_workflow_cross(target_time, proxy_url=None):
    """
    4H横截面入口：按最大指标窗口反推预热天数，拉取1m数据，再执行多参数流水线。

    common_utils: 原接口在“完全无行情”时返回空字符串，而其他入口返回 DataFrame；为保持调用兼容继续保留。
    common_utils: Grid_No.43629 的 MAX_WEIGHT=2.6 允许空头/理论目标权重超过100%；
           这是业务参数，不在技术重构中改动。
    """
    strategy_params_list = [
        {
            'STRATEGY_NAME': 'Grid_No.43629',
            'MOM_WINDOW': 48,
            'VOL_WINDOW': 42,
            'BTC_TREND_WINDOW': 120,
            'MAX_WEIGHT': 2.6,
            'TOP_K': 1,
            'TIME_OFFSET': '2h',
            'TRADE_MODE': 'LONG_ONLY',
        },
        {
            'STRATEGY_NAME': 'Grid_No.69393',
            'MOM_WINDOW': 90,
            'VOL_WINDOW': 120,
            'BTC_TREND_WINDOW': 720,
            'MAX_WEIGHT': 0.4,
            'TOP_K': 3,
            'TIME_OFFSET': '0h',
            'TRADE_MODE': 'SHORT_ONLY',
        },
    ]
    symbol_list = [
        'BTC/USDT:USDT',
        'ETH/USDT:USDT',
        'SOL/USDT:USDT',
        'XRP/USDT:USDT',
        'BNB/USDT:USDT',
        'DOGE/USDT:USDT',
    ]
    timeframe = '1m'

    max_window = max(
        max(params['MOM_WINDOW'], params['VOL_WINDOW'], params['BTC_TREND_WINDOW'])
        for params in strategy_params_list
    )
    lookback_days = int(np.ceil(max_window / 6)) + 30
    expected_rows = lookback_days * 24 * 60 + 1

    logger = setup_logger()
    logger.info(
        f"🚀 [Cross/启动] 策略数: [{len(strategy_params_list)}] | "
        f"最大指标窗口: [{max_window} bars] | 预热天数: [{lookback_days}] | "
        f"标的数: [{len(symbol_list)}] | 单标的预期K线: [{expected_rows}] | "
        f"目标时刻: [{target_time}]"
    )

    result_map = snipe_kline_data(
        symbol_list=symbol_list,
        timeframe=timeframe,
        days=lookback_days,
        target_time_str=target_time,
        use_ws=True,
        use_rest=True,
        proxy_url=proxy_url,
    )

    fetched, missing = [], []
    for symbol in symbol_list:
        frame = _frame_of(result_map, symbol)
        if frame.empty:
            missing.append(symbol)
            continue

        _warn_data_gap(logger, 'Cross', symbol, frame, expected_rows)
        frame = frame.copy()
        frame['coin_name'] = symbol.split('/')[0]
        frame['symbol'] = symbol
        fetched.append(frame)

    if missing:
        logger.warning(
            f"⚠️ [Cross/数据体检] 完全无数据: [{', '.join(missing)}] | "
            f"排查线索: [交易所无合约/网络或代理异常/取数引擎异常]"
        )

    if not fetched:
        logger.error(
            '❌ [Cross/致命] 可用标的数: [0] | 当前动作: [组装横截面矩阵] | '
            '结果: [终止] | 排查线索: [检查网络/代理/data_provider]'
        )
        return ''

    logger.info(
        f"✅ [Cross/取数完成] 标的到位: [{len(fetched)}/{len(symbol_list)}] | "
        f"结果: [开始多参数推演]"
    )
    return run_live_pipeline(fetched, strategy_params_list, logger)


# =============================================================================
# 七、1m 因子信号：共用生成器、共用工作流、兼容适配器
# =============================================================================
def _build_factor_result(signal_df, time_col, symbol, coin, strategy_name, direction):
    """
    把命中的原始K线统一映射为因子 OPEN 信号表。

    入参形貌: signal_df 含 close 与 time_col；输出固定 FACTOR_COLS。
    """
    if signal_df.empty:
        return pd.DataFrame()

    result = pd.DataFrame(index=signal_df.index)
    result['timestamp'] = signal_df[time_col].astype('int64') + 60 * 1000
    result['timestamp_str'] = result['timestamp'].map(_fmt_bjt)
    result['event'] = 'OPEN'
    result['direction'] = direction
    result['price'] = signal_df['close'].astype(float)
    result['symbol'] = symbol
    result['coin_name'] = coin
    result['strategy_name'] = strategy_name
    return result[FACTOR_COLS]


def _generate_false_break_signals(df, strategy_name, direction, win_break, win_base):
    """
    通用假突破/假跌破因子：先计算更早窗口边界，再判断最近 win_break 根是否越界并收回。

    入参形貌: df 至少含 [timestamp|open_time|time|ts, high, low, close, symbol, coin_name]。
    出参形貌: DataFrame(FACTOR_COLS)。
    """
    if df is None or len(df) < win_break + win_base:
        return pd.DataFrame()

    symbol, coin = _resolve_identity(df)
    time_col = _pick_column(df, ['timestamp', 'open_time', 'time', 'ts'], 'kline')
    close = df['close'].astype(float)

    if direction == 'LONG':
        price = df['low'].astype(float)
        boundary = price.shift(win_break).rolling(win_base).min()
        signal = price.rolling(win_break).min().lt(boundary) & close.gt(boundary)
    else:
        price = df['high'].astype(float)
        boundary = price.shift(win_break).rolling(win_base).max()
        signal = price.rolling(win_break).max().gt(boundary) & close.lt(boundary)

    hits = df[signal.fillna(False).astype(bool)].copy()
    return _build_factor_result(
        hits, time_col, symbol, coin, strategy_name, direction
    )


def generate_factor_044_1_signals(df):
    """factor_044_1：4根窗口假跌破 40根历史低点后收回，产生 LONG OPEN。"""
    return _generate_false_break_signals(
        df, 'factor_044_1', 'LONG', 4, 40
    )


def generate_factor_024_6_signals(df):
    """
    factor_024_6：5根K线重心均值突破其1440根历史99%分位数，产生 LONG OPEN。

    入参形貌: df 至少含 [timestamp|open_time|time|ts, high, low, close, symbol, coin_name]。
    """
    if df is None or len(df) < 1440:
        return pd.DataFrame()

    symbol, coin = _resolve_identity(df)
    time_col = _pick_column(df, ['timestamp', 'open_time', 'time', 'ts'], 'kline')

    high = df['high'].astype(float)
    low = df['low'].astype(float)
    close = df['close'].astype(float)

    k_position = (close - low) / (high - low).replace(0, 1e-9)
    mean_position = k_position.rolling(5).mean()
    threshold = mean_position.rolling(1440).quantile(0.99)

    hits = df[(mean_position > threshold).fillna(False).astype(bool)].copy()
    return _build_factor_result(
        hits, time_col, symbol, coin, 'factor_024_6', 'LONG'
    )


def generate_factor_043_10_signals(df):
    """factor_043_10：15根窗口假突破 1200根历史高点后回落，产生 SHORT OPEN。"""
    return _generate_false_break_signals(
        df, 'factor_043_10', 'SHORT', 15, 1200
    )


def generate_factor_043_9_signals(df):
    """factor_043_9：15根窗口假突破 720根历史高点后回落，产生 SHORT OPEN。"""
    return _generate_false_break_signals(
        df, 'factor_043_9', 'SHORT', 15, 720
    )


def generate_factor_044_10_signals(df):
    """factor_044_10：15根窗口假跌破 1200根历史低点后收回，产生 LONG OPEN。"""
    return _generate_false_break_signals(
        df, 'factor_044_10', 'LONG', 15, 1200
    )


def _run_factor_workflow(label, target_time, symbol_list, proxy_url, signal_fn, launch_text):
    """
    统一1m因子工作流：兼容旧参数传法 -> 30天取数 -> 逐标计算 -> 聚合排序 -> CSV。

    入参形貌: signal_fn(DataFrame) -> DataFrame(FACTOR_COLS)。
    出参形貌: DataFrame(FACTOR_COLS)。
    """
    if isinstance(target_time, list):
        proxy_url = symbol_list if isinstance(symbol_list, str) else proxy_url
        symbol_list = target_time
        target_time = None

    if not target_time:
        target_time = datetime.now().strftime('%Y-%m-%d %H:%M')

    if not symbol_list:
        raise ValueError('symbol_list 不能为空，请提供需要推演的标的列表')

    timeframe = '1m'
    lookback_days = 30
    expected_rows = lookback_days * 1440 + 1

    logger = setup_logger()
    logger.info(
        f"🚀 [{label}/启动] 动作: [{launch_text}] | 周期: [{timeframe}] | "
        f"标的: [{symbol_list}] | 预热天数: [{lookback_days}] | "
        f"单标的预期K线: [{expected_rows}] | 目标时刻: [{target_time}]"
    )

    kline_map = snipe_kline_data(
        symbol_list=symbol_list,
        timeframe=timeframe,
        days=lookback_days,
        target_time_str=target_time,
        use_ws=True,
        use_rest=True,
        proxy_url=proxy_url,
    )

    frames, skipped = [], []
    for symbol in symbol_list:
        kline = _frame_of(kline_map, symbol)
        if kline.empty:
            skipped.append(f'{symbol}(K线为空)')
            continue

        _warn_data_gap(logger, label, symbol, kline, expected_rows)

        kline = kline.copy()
        kline['coin_name'] = symbol.split('/')[0]
        kline['symbol'] = symbol

        try:
            signal_df = signal_fn(kline)
            if not signal_df.empty:
                frames.append(signal_df)
        except Exception as exc:
            # 明确保留原逐标容错：
            # 单一标的失败不阻断其它标的，但日志给出完整上下文。
            logger.error(
                f"❌ [{label}/推演失败] 标的: [{symbol}] | 当前动作: [计算因子信号] | "
                f"原因: [{exc}] | 结果: [跳过该标的继续处理其它标的]",
                exc_info=True,
            )

    if skipped:
        logger.warning(
            f"⚠️ [{label}/数据缺口] 跳过标的数: [{len(skipped)}] | "
            f"明细: [{', '.join(skipped)}]"
        )

    if not frames:
        logger.info(
            f"[{label}/收官] 回溯: [{lookback_days}天] | 结果: [未产生有效信号]"
        )
        return pd.DataFrame(columns=FACTOR_COLS)

    final_df = pd.concat(frames, ignore_index=True)
    final_df = final_df.sort_values(
        ['timestamp', 'symbol']
    ).reset_index(drop=True)

    output_path = os.path.join('signal_data', f'{label}_signals.csv')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    final_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    # logger.info(
    #     f"✅ [{label}/账本落盘] 文件: [{output_path}] | "
    #     f"记录总数: [{len(final_df)}]"
    # )
    return final_df


def _get_factor_signal(symbol, workflow_fn, label):
    """单 symbol 兼容适配器；按原设计，适配层失败时记录日志并返回空表。"""
    logger = setup_logger()
    proxy_url = (
        None
        if platform.system().lower() == 'linux'
        else 'http://127.0.0.1:7890'
    )

    try:
        return workflow_fn(
            target_time=None,
            symbol_list=[symbol],
            proxy_url=proxy_url,
        )
    except Exception as exc:
        logger.error(
            f"❌ [信号适配器/{label}] 标的: [{symbol}] | "
            f"当前动作: [执行因子工作流] | 原因: [{exc}] | "
            f"结果: [按原兼容约定返回空DataFrame]",
            exc_info=True,
        )
        return pd.DataFrame()


def execute_trading_bot_workflow_factor_044_1(
        target_time=None, symbol_list=None, proxy_url=None):
    """factor_044_1：1m 超短线假跌破做多。"""
    return _run_factor_workflow(
        'factor_044_1',
        target_time,
        symbol_list,
        proxy_url,
        generate_factor_044_1_signals,
        '超短线高频假跌破做多信号生成',
    )


def execute_trading_bot_workflow_factor_024_6(
        target_time=None, symbol_list=None, proxy_url=None):
    """factor_024_6：1m K线重心分位数突破做多。"""
    return _run_factor_workflow(
        'factor_024_6',
        target_time,
        symbol_list,
        proxy_url,
        generate_factor_024_6_signals,
        'K线重心高分位突破信号生成',
    )


def execute_trading_bot_workflow_factor_043_10(
        target_time=None, symbol_list=None, proxy_url=None):
    """factor_043_10：1m 极低频假突破做空。"""
    return _run_factor_workflow(
        'factor_043_10',
        target_time,
        symbol_list,
        proxy_url,
        generate_factor_043_10_signals,
        '极低频重要结构反转做空信号生成',
    )


def execute_trading_bot_workflow_factor_043_9(
        target_time=None, symbol_list=None, proxy_url=None):
    """factor_043_9：1m 大周期关键位假突破做空。"""
    return _run_factor_workflow(
        'factor_043_9',
        target_time,
        symbol_list,
        proxy_url,
        generate_factor_043_9_signals,
        '大周期关键位猎杀做空信号生成',
    )


def execute_trading_bot_workflow_factor_044_10(
        target_time=None, symbol_list=None, proxy_url=None):
    """factor_044_10：1m 极低频假跌破做多。"""
    return _run_factor_workflow(
        'factor_044_10',
        target_time,
        symbol_list,
        proxy_url,
        generate_factor_044_10_signals,
        '极低频重要结构反转做多信号生成',
    )


def get_signal_factor_044_1(symbol):
    return _get_factor_signal(
        symbol,
        execute_trading_bot_workflow_factor_044_1,
        'factor_044_1',
    )


def get_signal_factor_024_6(symbol):
    return _get_factor_signal(
        symbol,
        execute_trading_bot_workflow_factor_024_6,
        'factor_024_6',
    )


def get_signal_factor_043_10(symbol):
    return _get_factor_signal(
        symbol,
        execute_trading_bot_workflow_factor_043_10,
        'factor_043_10',
    )


def get_signal_factor_043_9(symbol):
    return _get_factor_signal(
        symbol,
        execute_trading_bot_workflow_factor_043_9,
        'factor_043_9',
    )


def get_signal_factor_044_10(symbol):
    return _get_factor_signal(
        symbol,
        execute_trading_bot_workflow_factor_044_10,
        'factor_044_10',
    )

# =============================================================================
# 因子 044_5: 中线标准 (LONG)
# =============================================================================
def generate_factor_044_5_signals(df):
    """factor_044_5：10根窗口假跌破 180根历史低点后收回，产生 LONG OPEN。"""
    return _generate_false_break_signals(
        df, 'factor_044_5', 'LONG', 10, 180
    )

def execute_trading_bot_workflow_factor_044_5(
        target_time=None, symbol_list=None, proxy_url=None):
    """factor_044_5：1m 中线标准假跌破做多。"""
    return _run_factor_workflow(
        'factor_044_5',
        target_time,
        symbol_list,
        proxy_url,
        generate_factor_044_5_signals,
        '中线标准假跌破做多信号生成',
    )

def get_signal_factor_044_5(symbol):
    return _get_factor_signal(
        symbol,
        execute_trading_bot_workflow_factor_044_5,
        'factor_044_5',
    )


# =============================================================================
# 八、本地联调入口
# =============================================================================
if __name__ == '__main__':
    target_time = (
            datetime.now() - timedelta(minutes=1)
    ).strftime('%Y-%m-%d %H:%M')

    symbol_list = ['UNI/USDT:USDT']
    signal = get_signal_factor_044_5(symbol_list[0])
    print()