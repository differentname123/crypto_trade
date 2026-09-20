# -*- coding: utf-8 -*-
import os
import numpy as np
import pandas as pd

# =====================================================================
# 1. 核心复用：直接引入你的底层回测引擎组件
# =====================================================================
from app.signal_trade_lite.martin_strategy_backest import (
    run_stage1,
    TimelineReplayer,
    evaluate_free_ride
)


# 导入因子函数 (如果你有专门的 factors 文件请改为从那里 import)
# 这里为了保证代码即插即用，直接复用你提供的因子逻辑
def factor_043_9(df):
    win_break, win_base = 15, 720
    boundary = df['high'].shift(win_break).rolling(win_base).max()
    df['signal'] = ((df['high'].rolling(win_break).max() > boundary) & (df['close'] < boundary)).fillna(False).astype(
        bool)
    return df


# =====================================================================
# 2. 格式对齐：复用你的归一化函数 (保证生成的 CSV 字段与原版一模一样)
# =====================================================================
_TIME_LIKE_KEYS = ("time", "stamp", "epoch", "date", "millis", "nanos", "_ms", "_ns")
PNL_COL_CANDIDATES = ["net_pnl_in_margin", "pnl_in_margin", "net_pnl", "pnl", "profit", "net_profit"]
MDD_COL_CANDIDATES = ["max_drawdown", "max_drawdown_in_margin", "max_dd", "max_loss", "max_loss_in_margin", "mdd",
                      "max_floating_loss"]
BLOWUP_LOSS_THRESHOLD_M = 0.8


def _pick_col(df, candidates):
    for c in candidates:
        if c in df.columns: return c
    return None


def _to_dt(s):
    if pd.api.types.is_numeric_dtype(s):
        v = pd.to_numeric(s, errors="coerce")
        mx = float(v.max()) if len(v) else 0.0
        unit = "ns" if mx > 1e15 else ("ms" if mx > 1e11 else "s")
        return pd.to_datetime(v, unit=unit)
    return pd.to_datetime(s, errors="coerce")


def _detect_time_cols(df):
    time_col = start_col = end_col = None
    for col in df.columns:
        c_lower = str(col).lower()
        if any(k in c_lower for k in _TIME_LIKE_KEYS):
            if any(k in c_lower for k in ("close", "end", "finish")):
                end_col = col; time_col = time_col or col
            elif any(k in c_lower for k in ("open", "start", "begin")):
                start_col = col
            else:
                time_col = time_col or col
    return start_col or end_col or time_col, end_col or time_col


def _normalize_trades_df(trades_df, cycles_df, margin):
    out = trades_df.copy()
    report = evaluate_free_ride(trades_df, cycles_df, margin) or {}

    pnl_col = _pick_col(out, PNL_COL_CANDIDATES)
    raw = pd.to_numeric(out[pnl_col], errors="coerce").fillna(0.0).astype(float)
    net_pnl_sum = float(raw.sum())
    report_net = float(report.get("total_net_pnl_in_margin", 0.0) or 0.0)

    ratio = (report_net / net_pnl_sum) if (abs(net_pnl_sum) > 1e-6 and abs(report_net) > 1e-6) else 1.0
    out["pnl_M"] = raw * ratio

    start_col, end_col = _detect_time_cols(trades_df)
    close_dt = _to_dt(out[end_col])
    open_dt = _to_dt(out[start_col]) if start_col else close_dt
    open_dt = open_dt.mask(open_dt.isna() | (open_dt > close_dt), close_dt)

    out["open_dt"] = open_dt
    out["close_dt"] = close_dt
    out["holding_h"] = (close_dt - open_dt).dt.total_seconds() / 3600.0

    blow_col = next(
        (c for c in out.columns if any(k in str(c).lower() for k in ("blowup", "blow_up", "liquidat", "is_bust"))),
        None)
    if blow_col:
        is_blow = out[blow_col].fillna(False).astype(bool)
    else:
        is_blow = out["pnl_M"] <= -BLOWUP_LOSS_THRESHOLD_M

    out["is_blowup_flag"] = is_blow.values
    mdd_col = _pick_col(out, MDD_COL_CANDIDATES)
    out["float_loss_M"] = (
                pd.to_numeric(out[mdd_col], errors="coerce").abs().fillna(0.0) * abs(ratio)) if mdd_col else 0.0
    out["pnl_scale_ratio"] = ratio

    return out.sort_values("close_dt").reset_index(drop=True)


# =====================================================================
# 3. 最小执行逻辑
# =====================================================================
def run_minimal_backtest():
    # 参数配置
    csv_path = r"W:\project\python_project\crypto_trade\app\trader_bot\data\SOL_USDT_USDT_1m_latest.csv"
    output_csv = "trades_SOLUSDT_factor_043_9_Short_M9.csv"

    symbol = "SOLUSDT"
    strategy_name = "factor_043_9"
    direction = "Short"
    margin = 9.0
    add_step = 0.030  # 3%
    tp_step = 0.008  # 0.8%
    multiplier = 2.0  # 默认加仓倍数
    fee_rate = 0.0005  # 默认手续费

    print(f"1. 加载数据: {csv_path}...")
    df = pd.read_csv(csv_path)

    # 如果没有 open_time 将timestamp 复制
    if 'open_time' not in df.columns and 'timestamp' in df.columns:
        df['open_time'] = df['timestamp']

    print("2. 生成做空信号...")
    df = factor_043_9(df)
    df['short_signal'] = df['signal'].fillna(False).astype(np.int8)

    print("3. 执行 Stage 1 引擎 (构建平行宇宙阶梯)...")
    cycles_df = run_stage1(
        df,
        short_col="short_signal",
        long_col="long_signal",
        fee_rate=fee_rate,
        add_step=add_step,
        tp_step=tp_step,
        multiplier=multiplier,
        max_layer_hard=512,
        verbose=False  # 保持输出清爽
    )

    print(f"4. 执行 Stage 2 引擎 (Margin={margin} 提取真实明细)...")
    replayer = TimelineReplayer(cycles_df)
    trades_df = replayer.run(margin)

    print("5. 归一化对齐与 Meta 注入...")
    norm_df = _normalize_trades_df(trades_df, cycles_df, margin)

    # 追加 Stage A 要求的完全一致的 Meta 字段
    norm_df["symbol"] = symbol
    norm_df["strategy"] = strategy_name
    norm_df["direction"] = direction
    norm_df["margin"] = margin
    norm_df["add_step"] = add_step
    norm_df["tp_step"] = tp_step
    norm_df["multiplier"] = multiplier

    # 落盘
    norm_df.to_csv(output_csv, index=False, encoding="utf-8-sig")
    print(f"\n🎉 成功！回测文件已生成: {output_csv}")
    print(f"统计信息: 实际开仓数 = {len(norm_df)} 笔, 爆仓次数 = {norm_df['is_blowup_flag'].sum()} 次")


if __name__ == "__main__":
    run_minimal_backtest()