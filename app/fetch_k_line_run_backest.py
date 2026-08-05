# -*- coding: utf-8 -*-
"""
LER 强平耗竭回归 —— 极简回测版
简化说明：
  1) 不使用强平推送、不使用盘口价差/深度（无数据）；
  2) 摩擦成本用单边固定 FEE 近似（含手续费+滑点），不做 Maker/Taker 分流；
  3) 资金费率只做 Gate A 的门槛判断，不计持仓期费率损益；
  4) 不做组合级并发/回撤熔断，只统计单笔期望（先看信号质量）。
"""

import os
import glob
import numpy as np
import pandas as pd

# ============================== 参数 ==============================
CFG = dict(
    FEE=0.0008,            # 单边综合摩擦（手续费+滑点），压力测试时改 0.0016
    BAR_PER_DAY=288,       # 5m
    ATR_N=14,

    # Gate A
    FR_Q=0.90,             # 预测费率 90 分位
    OI_Q=0.80,             # 币本位 OI 80 分位
    BTC_FREEZE=-0.025,     # BTC 近 30min 跌超 2.5% 冻结

    # Gate B
    B_MAX_BARS=5,
    PRICE_ATR_MULT=4.0,
    PRICE_DROP_MIN=0.05,
    OI_DROP_Q=0.05,        # OI 单窗变动分布 5 分位（=95 分位降幅）
    OI_DROP_MIN=0.03,      # 绝对下限 3%

    # Gate C
    OI_BURN_MIN=0.05,      # 累计燃烧 5%
    CLOSE_UPPER=0.40,      # 第 2 根收在自身振幅上 40%

    # 风控
    SL_ATR_MULT=0.75,
    TP1_FIB=0.382,
    TP2_FIB=0.500,
    HOLD_HOURS=4,
)


# ============================== 工具 ==============================
def calc_atr(df, n=14):
    tr = pd.concat([df['high'] - df['low'],
                    (df['high'] - df['close'].shift()).abs(),
                    (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean()


def load_btc_ret(path):
    """返回 5m 网格上的 BTC 近 30 分钟收益率"""
    b = pd.read_csv(path)
    if 'open_time' in b.columns:
        ts = pd.to_datetime(b['open_time'], unit='ms', utc=True) \
            .dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
    else:
        ts = pd.to_datetime(b['timestamp'])
    b = b.assign(timestamp=ts).sort_values('timestamp').set_index('timestamp')
    c5 = b['close'].resample('5min', label='left', closed='left').last()
    out = (c5 / c5.shift(6) - 1).rename('btc_ret30').reset_index()
    return out


def prepare(path, btc_ret=None, cfg=CFG):
    df = pd.read_csv(path)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp').reset_index(drop=True)

    # 时间语义对齐：第 i 根 K 线"收盘时"能看到的 OI 快照 = 下一行的期初快照
    df['oi_end'] = df['oi_amount'].shift(-1)

    # OI 变动率统一转成小数（自动识别原字段是 % 还是小数）
    raw = df['oi_amount_change_pct'].shift(-1)
    scale = 100.0 if raw.abs().quantile(0.99) > 1 else 1.0
    df['oi_chg'] = raw / scale

    if 'cvd' not in df.columns:           # 无 CVD 则关闭该否决权
        df['cvd'] = -np.arange(len(df), dtype=float)

    df['atr'] = calc_atr(df, cfg['ATR_N'])

    D = cfg['BAR_PER_DAY']
    mp = D * 3
    df['fr_q']        = df['predicted_funding_rate'].rolling(D * 90, min_periods=mp).quantile(cfg['FR_Q'])
    df['oi_q']        = df['oi_end'].rolling(D * 20, min_periods=mp).quantile(cfg['OI_Q'])
    df['oi_drop_q']   = df['oi_chg'].rolling(D * 30, min_periods=mp).quantile(cfg['OI_DROP_Q'])
    df['oi_chg_std']  = df['oi_chg'].rolling(D * 30, min_periods=mp).std()

    if btc_ret is not None:
        df = df.merge(btc_ret, on='timestamp', how='left')
        df['btc_ret30'] = df['btc_ret30'].ffill().fillna(0.0)
    else:
        df['btc_ret30'] = 0.0
    return df


# ============================== 单笔模拟 ==============================
def simulate(k0, entry, sl, tp1, tp2, A, cfg=CFG):
    o, h, l, c, ts = A['o'], A['h'], A['l'], A['c'], A['ts']
    deadline = ts[k0] + np.timedelta64(cfg['HOLD_HOURS'] * 60, 'm')
    f = cfg['FEE']
    half, tp1_px = False, 0.0

    for k in range(k0, len(c)):
        if np.isnan(c[k]):
            continue
        # 时间止损
        if ts[k] >= deadline:
            px = c[k] * (1 - f)
            return k, ts[k], (tp1_px + px) / 2 if half else px, 'Time' + ('_afterTP1' if half else '')
        # 止损（同根 K 线内先判止损，保守）
        if l[k] <= sl:
            px = min(sl, o[k]) * (1 - f)           # 跳空则按开盘价
            return k, ts[k], (tp1_px + px) / 2 if half else px, 'SL' + ('_afterTP1' if half else '')
        # 分批止盈
        if not half and h[k] >= tp1:
            half, tp1_px = True, tp1 * (1 - f)
        if half and h[k] >= tp2:
            px = tp2 * (1 - f)
            return k, ts[k], (tp1_px + px) / 2, 'TP_All'
    # 数据尾部
    k = len(c) - 1
    px = c[k] * (1 - f)
    return k, ts[k], (tp1_px + px) / 2 if half else px, 'EOD'


# ============================== 状态机 ==============================
def run(df, symbol, cfg=CFG):
    A = dict(o=df['open'].values, h=df['high'].values, l=df['low'].values,
             c=df['close'].values, ts=df['timestamp'].values)
    oi, oichg, cvd = df['oi_end'].values, df['oi_chg'].values, df['cvd'].values
    atr = df['atr'].values
    fr, frq = df['predicted_funding_rate'].values, df['fr_q'].values
    oiq, oidq, oistd = df['oi_q'].values, df['oi_drop_q'].values, df['oi_chg_std'].values
    btc = df['btc_ret30'].values
    o, h, l, c, ts = A['o'], A['h'], A['l'], A['c'], A['ts']

    trades, n, i = [], len(df), 1
    while i < n - 8:
        p = i - 1  # 事件前稳态
        # ---------- Gate A：燃料门 ----------
        if not (fr[p] > 0 and fr[p] > frq[p] and oi[p] > oiq[p] and btc[p] > cfg['BTC_FREEZE']):
            i += 1
            continue

        base_px, base_oi, base_cvd, atr0 = o[i], oi[p], cvd[p], atr[p]
        price_thr = max(cfg['PRICE_ATR_MULT'] * atr0 / base_px, cfg['PRICE_DROP_MIN'])
        oi_thr = min(-cfg['OI_DROP_MIN'], oidq[i])

        # ---------- Gate B：级联确认 ----------
        end = -1
        for j in range(i, min(i + cfg['B_MAX_BARS'], n - 4)):
            drop = (base_px - np.min(l[i:j + 1])) / base_px
            oi_drop = (oi[j] - base_oi) / base_oi
            if drop > price_thr and oi_drop < oi_thr and (cvd[j] - base_cvd) < 0:
                end = j
                break
        if end < 0:
            i += 1
            continue

        # ---------- Gate C：耗竭确认（连续 2 根） ----------
        c1, c2 = end + 1, end + 2
        b_low = np.min(l[i:end + 1])
        rng = h[c2] - l[c2]
        ok = (abs(oichg[c1]) < oistd[p] and abs(oichg[c2]) < oistd[p]          # 账本企稳
              and (oi[c2] - base_oi) / base_oi <= -cfg['OI_BURN_MIN']          # 深度耗尽
              and min(l[c1], l[c2]) >= b_low                                   # 不创新低
              and rng > 0 and c[c2] >= l[c2] + rng * (1 - cfg['CLOSE_UPPER'])) # 收上 40%
        if not ok:
            i = c2
            continue

        # ---------- 入场 ----------
        k0 = c2 + 1
        if np.isnan(o[k0]):
            i = k0
            continue
        entry = o[k0] * (1 + cfg['FEE'])
        sl = b_low - cfg['SL_ATR_MULT'] * atr0
        b_high = np.max(h[i:end + 1])
        tp1 = b_low + cfg['TP1_FIB'] * (b_high - b_low)
        tp2 = b_low + cfg['TP2_FIB'] * (b_high - b_low)
        if sl >= entry:
            i = k0
            continue

        ke, t_exit, exit_px, reason = simulate(k0, entry, sl, tp1, tp2, A, cfg)
        risk = (entry - sl) / entry
        pnl = (exit_px - entry) / entry
        trades.append(dict(symbol=symbol, entry_time=ts[k0], exit_time=t_exit,
                           entry=entry, exit=exit_px, sl=sl,
                           b_drop=(base_px - b_low) / base_px,
                           oi_burn=(oi[c2] - base_oi) / base_oi,
                           reason=reason, pnl_pct=pnl, pnl_R=pnl / risk,
                           bars_held=ke - k0))
        i = ke + 1
    return trades


# ============================== 报告 ==============================
def report(tr, title):
    print("\n" + "=" * 56)
    print(f"📊 {title}")
    print("=" * 56)
    if len(tr) == 0:
        print("无信号")
        return
    d = pd.DataFrame(tr)
    win = d[d.pnl_pct > 0]
    los = d[d.pnl_pct <= 0]
    rr = abs(win.pnl_pct.mean() / los.pnl_pct.mean()) if len(win) and len(los) else float('nan')
    print(f"笔数        : {len(d)}")
    print(f"胜率        : {len(win)/len(d)*100:.1f}%   (预期 50%-56%)")
    print(f"盈亏比      : {rr:.2f}          (预期 1.2-1.5)")
    print(f"单笔净期望  : {d.pnl_pct.mean()*100:.3f}%  /  {d.pnl_R.mean():+.3f} R")
    print(f"累计(等权)  : {d.pnl_pct.sum()*100:.2f}%")
    print(f"持仓中位数  : {d.bars_held.median():.0f} 根 5m")
    print("平仓原因分布:\n" + d.reason.value_counts().to_string())
    return d


# ============================== 入口 ==============================
if __name__ == "__main__":
    DATA_DIR = './data'
    BTC_DATA = r"W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2025-01-01_merged.csv"

    btc_ret = load_btc_ret(BTC_DATA) if os.path.exists(BTC_DATA) else None
    files = sorted(glob.glob(os.path.join(DATA_DIR, '*_ler_data.csv')))
    print(f"发现 {len(files)} 个标的")

    all_tr = []
    for fp in files:
        sym = os.path.basename(fp).replace('_ler_data.csv', '')
        try:
            df = prepare(fp, btc_ret)
            tr = run(df, sym)
            print(f"  {sym:<28} 信号 {len(tr):>3} 笔"
                  f"{'' if not tr else f' | 期望 {np.mean([t[chr(39)+chr(39)] if False else t['pnl_R'] for t in tr]):+.3f}R'}")
            all_tr += tr
        except Exception as e:
            print(f"  {sym} 异常跳过: {e}")

    d = report(all_tr, "LER 全市场组合（Step1 事件级）")
    if d is not None:
        d = d.sort_values('entry_time')
        d.to_csv(os.path.join(DATA_DIR, 'LER_trades.csv'), index=False)
        # 按月看频率与稳定性
        m = d.set_index('entry_time').resample('ME').agg(n=('pnl_R', 'size'), expR=('pnl_R', 'mean'))
        print("\n[按月分布]\n" + m.to_string())
        print("\n已落盘 ➜ LER_trades.csv")