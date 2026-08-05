# -*- coding: utf-8 -*-
"""
OER 极端降仓耗竭反转系统 (Open Interest Exhaustion Reversal) —— 简化回测 Step-1
=================================================================================
目的：先验证「信号质量」，不做资金管理 / 组合并发 / 精细滑点 / 持仓期资金费损益。

严格对齐方案的部分：
  1) Gate A 燃料门（费率拥挤 + 币本位 OI 高位）
  2) Gate B 级联跌破（最多 5 根 5m，价格 max(4*ATR, 5%)；OI 同长度窗口 95 分位降幅 + 3% 硬下限）
  3) Gate C 耗竭确认（连续 2 根：OI 变动回到 1σ 带宽内 / 累计燃烧 >= 5% / 不创新低 + 收在振幅上 40%）
  4) 执行纪律：所有判定基于「K 线收盘」，所有成交一律用「下一根 K 线开盘价」
  5) 摩擦成本：单边 0.1%，一次完整交易刚性扣 0.2%
  6) 风控：0.75*ATR 空间硬止损（收盘破位）/ 38.2% 半仓 + 50% 清仓 / 4h 时间止损 / Gate B 级再坍塌熔断

刻意简化的部分（后续再补）：
  - 资金费率仅用于 Gate A 门槛，不计持仓期收付
  - 不做组合层面并发上限、相关性过滤、日内回撤熔断
  - 每笔按「等权 1 单位名义」统计，不做复利
=================================================================================
"""

import os
import glob
import logging
from collections import Counter

import numpy as np
import pandas as pd

# ============================== 参数 ==============================
CFG = dict(
    # ---------- 成本 / 时间 ----------
    FEE=0.0010,
    BAR_MIN=5,
    BAR_PER_DAY=288,
    ATR_N=14,  # 虽然不再用于止损，但可保留用于跌幅评估

    # ---------- Gate A: 燃料门 (极简物理验证) ----------
    USE_GATE_A=True,
    FR_MODE='abs',  # 直接看绝对值
    FR_Q=0.50,  # 废弃分位数复杂逻辑
    FR_Q_DAYS=90,
    FR_ANN_ABS=0.01,  # 年化大于 1% (即只要费率为正即可，多头在付钱)
    FUNDING_INTERVAL_H=8,
    OI_Q=0.60,  # OI 处于中上水平即可，60分位
    OI_Q_DAYS=20,
    USE_BTC_FREEZE=False,
    BTC_FREEZE=-0.025,

    # ---------- Gate B: 级联跌破 (坚守短时间真空) ----------
    B_MAX_BARS=5,  # 坚守 25 分钟极速爆仓逻辑，不予放宽
    PRICE_ATR_MULT=3.0,  # 3倍日常波动
    PRICE_DROP_MIN=0.035,  # 物理底线：至少跌 3.5%
    OI_DROP_Q=0.10,  # 废弃 95 分位的苛刻要求，用 90 分位
    OI_DROP_MIN=0.025,  # 物理底线：真金白银至少烧掉 2.5%
    OI_DIST_DAYS=30,

    # ---------- Gate C: 耗竭确认 (废除标准差，看绝对流血率) ----------
    C_BARS=2,
    OI_STABLE_K=0.005,  # 【重大重构】不再代表几倍σ，而是绝对值！(需要在下方改一行逻辑)
    OI_BURN_MIN=0.025,  # 与 Gate B 对齐，不再强求 C 段必须额外失血
    CLOSE_UPPER=0.50,  # 只要收盘在下影线之上 (中点上方) 即可，不要强求阳线

    # ---------- 风控 (锚定针尖，拒绝滞后指标) ----------
    SL_ATR_MULT=0.01,  # 【重大重构】此处将其复用为: 距离最低点再下浮 1% (下方改逻辑)
    TP1_FIB=0.50,  # 反弹一半走一半
    TP2_FIB=1.00,  # 完全收复起跌点全走
    TP1_SIZE=1.0,      # 【核心修改】：在 TP1 位置直接 100% 全仓平掉！

    HOLD_HOURS=4,
    USE_CIRCUIT_BREAKER=True,
    CB_REQUIRE_PRICE=True,  # 熔断必须配合价格破位
    B_ANCHOR='ref',

    OI_SNAPSHOT_AT='open',
    MIN_HISTORY_BARS=288 * 3,
    COOLDOWN_BARS=6,
    RESCAN_ON_C_FAIL='c_end',
)
CFG['HOLD_BARS'] = int(CFG['HOLD_HOURS'] * 60 / CFG['BAR_MIN'])
CFG['FUNDING_PER_YEAR'] = 24 / CFG['FUNDING_INTERVAL_H'] * 365


# ============================== 日志 ==============================
def setup_logger(log_path='./oer_backtest.log', console_level=logging.INFO):
    logger = logging.getLogger('OER')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S')

    fh = logging.FileHandler(log_path, mode='w', encoding='utf-8')
    fh.setLevel(logging.DEBUG)          # 文件里保留全部事件级明细
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(console_level)          # 控制台只看汇总
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    return logger


LOG = setup_logger()


# ============================== 工具 ==============================
def calc_atr(df, n=14):
    tr = pd.concat([df['high'] - df['low'],
                    (df['high'] - df['close'].shift()).abs(),
                    (df['low'] - df['close'].shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=n).mean()


def load_btc_ret(path):
    """BTC 近 30 分钟收益率（可选风控开关用）"""
    b = pd.read_csv(path)
    if 'open_time' in b.columns:
        ts = pd.to_datetime(b['open_time'], unit='ms', utc=True) \
            .dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
    else:
        ts = pd.to_datetime(b['timestamp'])
    b = b.assign(timestamp=ts).sort_values('timestamp').set_index('timestamp')
    c5 = b['close'].resample('5min', label='left', closed='left').last()
    return (c5 / c5.shift(6) - 1).rename('btc_ret30').reset_index()


def _pick(df, names):
    for nm in names:
        if nm in df.columns:
            return nm
    return None


def prepare(path, btc_ret=None, cfg=CFG, symbol=''):
    """读取并构造全部前视安全的特征"""
    df = pd.read_csv(path)

    # --- 时间戳 ---
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'])
    elif 'open_time' in df.columns:
        df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms', utc=True) \
            .dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
    else:
        raise ValueError('缺少 timestamp / open_time 字段')
    df = df.sort_values('timestamp').reset_index(drop=True)

    # --- 字段映射 ---
    oi_col = _pick(df, ['oi_amount', 'sumOpenInterest', 'open_interest', 'oi'])
    if oi_col is None:
        raise ValueError('缺少币本位持仓量字段 (oi_amount / sumOpenInterest)')
    fr_col = _pick(df, ['predicted_funding_rate', 'funding_rate', 'fundingRate'])

    # --- OI 时间语义对齐 ---
    # 'open': 第 i 行的 oi_amount 是该 K 线期初快照 -> 第 i 根收盘时可见的是第 i+1 行
    if cfg['OI_SNAPSHOT_AT'] == 'open':
        df['oi_end'] = df[oi_col].shift(-1)
    else:
        df['oi_end'] = df[oi_col]
    df['oi_end'] = df['oi_end'].replace(0, np.nan)

    # --- 多窗口 OI 变动（w = 1..B_MAX_BARS），全部为「截至第 i 根收盘」的已实现变动 ---
    for w in range(1, cfg['B_MAX_BARS'] + 1):
        df[f'oi_chg_{w}'] = df['oi_end'] / df['oi_end'].shift(w) - 1.0
    df['oi_chg'] = df['oi_chg_1']

    # --- 费率 ---
    if fr_col is not None:
        df['fr'] = pd.to_numeric(df[fr_col], errors='coerce')
        # 【刚性修复】如果费率绝对中位数大于 0.005，说明是百分数直存的，必须除以100
        if df['fr'].abs().median() > 0.005:
            df['fr'] = df['fr'] / 100.0
        df['fr_ann'] = df['fr'] * cfg['FUNDING_PER_YEAR']
    else:
        df['fr'] = np.nan

    # --- ATR ---
    df['atr'] = calc_atr(df, cfg['ATR_N'])

    # --- 滚动分布（min_periods 保证样本充足；rolling 天然不含未来） ---
    D, mp = cfg['BAR_PER_DAY'], cfg['MIN_HISTORY_BARS']
    df['fr_q'] = df['fr'].rolling(D * cfg['FR_Q_DAYS'], min_periods=mp).quantile(cfg['FR_Q'])
    df['oi_q'] = df['oi_end'].rolling(D * cfg['OI_Q_DAYS'], min_periods=mp).quantile(cfg['OI_Q'])
    for w in range(1, cfg['B_MAX_BARS'] + 1):
        df[f'oi_dq_{w}'] = df[f'oi_chg_{w}'].rolling(
            D * cfg['OI_DIST_DAYS'], min_periods=mp).quantile(cfg['OI_DROP_Q'])
    df['oi_chg_std'] = df['oi_chg'].rolling(D * cfg['OI_DIST_DAYS'], min_periods=mp).std()

    # --- BTC 联动（可选） ---
    if btc_ret is not None:
        df = df.merge(btc_ret, on='timestamp', how='left')
        df['btc_ret30'] = df['btc_ret30'].ffill().fillna(0.0)
    else:
        df['btc_ret30'] = 0.0

    # --- 数据质量日志埋点 ---
    LOG.info(f'[{symbol}] rows={len(df)} | {df.timestamp.iloc[0]} ~ {df.timestamp.iloc[-1]} | '
             f'OI_nan={df.oi_end.isna().sum()} FR_nan={df.fr.isna().sum()} '
             f'| FR中位年化={np.nanmedian(df.fr_ann):.2%} '
             f'| OI单根变动σ中位={np.nanmedian(df.oi_chg_std):.4%}')
    return df


# ============================== 单笔持仓模拟 ==============================
# ============================== 单笔持仓模拟 (触价单与推保本重构) ==============================
def simulate_trade(A, k0, sl, tp1, tp2, oi_w, oi_wq, cfg, stats):
    o, h, l, c, ts = A['o'], A['h'], A['l'], A['c'], A['ts']
    n = len(c)
    entry = o[k0]

    pos = 1.0
    legs = []
    half_done = False
    mae, mfe = 0.0, 0.0

    current_sl = sl  # 动态止损线
    fee = cfg['FEE']

    k = k0
    while k < n - 1:
        if not np.isnan(h[k]):
            mfe = max(mfe, h[k] / entry - 1.0)
            mae = min(mae, l[k] / entry - 1.0)

        held = k - k0 + 1

        # -------------------------------------------------------------
        # 1. 盘中触价逻辑：优先判定止损 (用 Low 判定)
        # -------------------------------------------------------------
        # 如果最低价打穿了止损线，立刻离场
        if l[k] <= current_sl:
            # 止损模拟市价单，给予 0.1% 滑点惩罚；若开盘直接跳空在SL下方，按开盘价走
            exit_px = min(current_sl * (1.0 - 0.001), o[k])
            reason = 'SL' if not half_done else 'SL_BE'
            legs.append((pos, exit_px, k, reason))
            return k, legs, reason, mae, mfe

        # -------------------------------------------------------------
        # 2. 盘中触价逻辑：判定止盈 (用 High 判定)
        # -------------------------------------------------------------
        # 触及 TP2 (全部清仓)
        if h[k] >= tp2:
            legs.append((pos, tp2, k, 'TP2'))
            reason = 'TP2' + ('_afterTP1' if half_done else '')
            return k, legs, reason, mae, mfe

        # 触及 TP1 (平半仓，并立刻推保本)
        if (not half_done) and h[k] >= tp1:
            legs.append((cfg['TP1_SIZE'], tp1, k, 'TP1'))
            pos -= cfg['TP1_SIZE']
            half_done = True
            stats['tp1_hit'] += 1

            # 【核心护航】：将剩下半仓的止损线上调至入场价上方 (覆盖双边手续费)
            # 这样这笔单子最差也是不亏钱出局
            current_sl = entry * (1.0 + fee * 2.5)

            # -------------------------------------------------------------
        # 3. 盘后确认逻辑：时间止损与熔断 (维持收盘判定，下根开盘走)
        # -------------------------------------------------------------
        px_next = o[k + 1]
        exit_all = None

        # 熔断判定
        if cfg['USE_CIRCUIT_BREAKER'] and held >= 1:
            for w in range(1, min(cfg['B_MAX_BARS'], held) + 1):
                v, q = oi_w[w][k], oi_wq[w][k]
                if np.isnan(v):
                    continue
                thr = min(-cfg['OI_DROP_MIN'], q if not np.isnan(q) else 0.0)
                if v <= thr:
                    if cfg['CB_REQUIRE_PRICE'] and (c[k] / c[k - w] - 1.0) > -cfg['PRICE_DROP_MIN'] / 2:
                        continue
                    exit_all = 'CircuitBreaker'
                    break

        # 时间止损
        if exit_all is None and held >= cfg['HOLD_BARS']:
            exit_all = 'TimeStop'

        if exit_all is not None:
            legs.append((pos, px_next, k + 1, exit_all))
            reason = exit_all + ('_afterTP1' if half_done else '')
            return k + 1, legs, reason, mae, mfe

        k += 1

    # 数据尾部强平
    legs.append((pos, c[n - 1], n - 1, 'EOD'))
    return n - 1, legs, 'EOD' + ('_afterTP1' if half_done else ''), mae, mfe

# ============================== 状态机 ==============================
def run(df, symbol, cfg=CFG):
    A = dict(o=df['open'].values.astype(float), h=df['high'].values.astype(float),
             l=df['low'].values.astype(float), c=df['close'].values.astype(float),
             ts=df['timestamp'].values)
    o, h, l, c, ts = A['o'], A['h'], A['l'], A['c'], A['ts']

    oi = df['oi_end'].values
    oichg = df['oi_chg'].values
    oistd = df['oi_chg_std'].values
    atr = df['atr'].values
    fr, fr_ann, frq = df['fr'].values, df['fr_ann'].values, df['fr_q'].values
    oiq = df['oi_q'].values
    btc = df['btc_ret30'].values
    oi_w = {w: df[f'oi_chg_{w}'].values for w in range(1, cfg['B_MAX_BARS'] + 1)}
    oi_wq = {w: df[f'oi_dq_{w}'].values for w in range(1, cfg['B_MAX_BARS'] + 1)}

    n = len(df)
    trades, events = [], []
    stats = Counter()

    need_tail = cfg['B_MAX_BARS'] + cfg['C_BARS'] + 3
    i = 1
    while i < n - need_tail:
        p = i - 1                                   # 事件前稳态基准
        stats['bars_scanned'] += 1

        # ---------------- Gate A：燃料门 ----------------
        if cfg['USE_GATE_A']:
            if np.isnan(oi[p]) or np.isnan(oiq[p]) or np.isnan(atr[p]) or np.isnan(oistd[p]):
                stats['A_fail_nan'] += 1
                i += 1
                continue

            # 情绪拥挤
            if np.isnan(fr[p]):
                fr_ok = True                        # 无费率数据则不否决
            else:
                q_ok = (not np.isnan(frq[p])) and fr[p] > frq[p]
                a_ok = fr_ann[p] >= cfg['FR_ANN_ABS']
                mode = cfg['FR_MODE']
                fr_ok = (fr[p] > 0) and (
                    q_ok if mode == 'quantile' else
                    a_ok if mode == 'abs' else
                    (q_ok or a_ok) if mode == 'or' else (q_ok and a_ok))
            if not fr_ok:
                stats['A_fail_funding'] += 1
                i += 1
                continue

            # 燃料充足
            if not (oi[p] > oiq[p]):
                stats['A_fail_oi_level'] += 1
                i += 1
                continue

            if cfg['USE_BTC_FREEZE'] and btc[p] <= cfg['BTC_FREEZE']:
                stats['A_fail_btc'] += 1
                i += 1
                continue
        stats['A_pass'] += 1

        ref_px, base_oi, atr0 = c[p], oi[p], atr[p]
        price_thr = max(cfg['PRICE_ATR_MULT'] * atr0 / ref_px, cfg['PRICE_DROP_MIN'])

        # ---------------- Gate B：级联跌破确认 ----------------
        end = -1
        b_drop = b_oi_drop = b_oi_thr = np.nan
        for j in range(i, i + cfg['B_MAX_BARS']):
            w = j - i + 1
            cum_drop = (ref_px - np.nanmin(l[i:j + 1])) / ref_px
            oi_drop = oi_w[w][j]
            if np.isnan(oi_drop):
                continue
            q = oi_wq[w][j]
            oi_thr = min(-cfg['OI_DROP_MIN'], q if not np.isnan(q) else 0.0)
            if cum_drop >= price_thr and oi_drop <= oi_thr:
                end, b_drop, b_oi_drop, b_oi_thr = j, cum_drop, oi_drop, oi_thr
                break
        if end < 0:
            stats['B_fail'] += 1
            i += 1
            continue
        stats['B_event'] += 1

        b_low = float(np.nanmin(l[i:end + 1]))
        b_high = float(np.nanmax(h[i:end + 1]))
        anchor = ref_px if cfg['B_ANCHOR'] == 'ref' else b_high

        # ---------------- Gate C：耗竭确认（连续 2 根） ----------------
        c1, c2 = end + 1, end + cfg['C_BARS']
        rng = h[c2] - l[c2]
        burn = (oi[c2] - base_oi) / base_oi if not np.isnan(oi[c2]) else np.nan
        std_band = cfg['OI_STABLE_K'] * oistd[p]
        cond_stable = (not np.isnan(oichg[c1]) and not np.isnan(oichg[c2])
                       and oichg[c1] >= -cfg['OI_STABLE_K']
                       and oichg[c2] >= -cfg['OI_STABLE_K'])
        cond_burn = (not np.isnan(burn)) and burn <= -cfg['OI_BURN_MIN']
        cond_nolow = float(np.nanmin(l[c1:c2 + 1])) >= b_low
        cond_close = rng > 0 and c[c2] >= l[c2] + rng * (1 - cfg['CLOSE_UPPER'])

        fails = []
        if not cond_stable: fails.append('oi_not_stable')
        if not cond_burn:   fails.append('burn_not_enough')
        if not cond_nolow:  fails.append('new_low')
        if not cond_close:  fails.append('weak_close')

        ev = dict(symbol=symbol, b_start=ts[i], b_end=ts[end], b_bars=end - i + 1,
                  ref_px=ref_px, b_low=b_low, b_drop=b_drop, price_thr=price_thr,
                  oi_drop_B=b_oi_drop, oi_thr_B=b_oi_thr, oi_burn_C=burn,
                  atr0=atr0, atr_pct=atr0 / ref_px,
                  fr=fr[p], fr_ann=fr_ann[p], fr_q=frq[p],
                  oi_chg_c1=oichg[c1], oi_chg_c2=oichg[c2], oi_std_band=std_band,
                  close_pos_c2=(c[c2] - l[c2]) / rng if rng > 0 else np.nan,
                  passed=len(fails) == 0, fail_reason='|'.join(fails))
        events.append(ev)

        if fails:
            for f in fails:
                stats['C_fail_' + f] += 1
            LOG.debug(f'[{symbol}] GateB@{ts[i]} 跌{b_drop:.2%} OI{b_oi_drop:.2%} '
                      f'-> GateC 未通过 ({"|".join(fails)})')
            i = (c2 + 1) if cfg['RESCAN_ON_C_FAIL'] == 'c_end' else (end + 1)
            continue
        stats['C_pass'] += 1

        # ---------------- 入场 ----------------
        k0 = c2 + 1
        if k0 >= n - 2 or np.isnan(o[k0]):
            i = k0 + 1
            continue

        entry_px = o[k0]
        sl = b_low * (1.0 - cfg['SL_ATR_MULT'])


        drop_span = anchor - b_low
        tp1 = b_low + cfg['TP1_FIB'] * drop_span
        tp2 = b_low + cfg['TP2_FIB'] * drop_span


        if (entry_px - b_low) > 0.35 * drop_span:
            stats['skip_missed_train'] += 1
            i = k0 + 1
            continue

        if sl >= entry_px or drop_span <= 0:
            stats['skip_bad_geometry'] += 1
            LOG.debug(f'[{symbol}] 几何异常跳过 @{ts[k0]} entry={entry_px} sl={sl}')
            i = k0 + 1
            continue
        # 入场价已高于 TP2（反弹过猛）则放弃
        if entry_px >= tp2:
            stats['skip_entry_above_tp2'] += 1
            i = k0 + 1
            continue

        risk = (entry_px - sl) / entry_px
        rr1 = (tp1 / entry_px - 1) / risk
        rr2 = (tp2 / entry_px - 1) / risk

        ke, legs, reason, mae, mfe = simulate_trade(A, k0, sl, tp1, tp2, oi_w, oi_wq, cfg, stats)

        gross = sum(wgt * px / entry_px for wgt, px, _, _ in legs)
        pnl = gross - 1.0 - 2 * cfg['FEE']          # 刚性扣减往返 0.2%
        avg_exit = sum(wgt * px for wgt, px, _, _ in legs)

        tr = dict(
            symbol=symbol, entry_time=ts[k0], exit_time=ts[ke],
            entry=entry_px, exit_avg=avg_exit, sl=sl, tp1=tp1, tp2=tp2,
            b_start=ts[i], b_end=ts[end], b_bars=end - i + 1,
            b_low=b_low, b_drop=b_drop, oi_drop_B=b_oi_drop, oi_burn_C=burn,
            atr_pct=atr0 / ref_px, fr_ann=fr_ann[p],
            risk_pct=risk, rr_tp1=rr1, rr_tp2=rr2,
            reason=reason, legs='|'.join(f'{t}@{px:.6g}x{w:.2f}' for w, px, _, t in legs),
            pnl_pct=pnl, pnl_R=pnl / risk if risk > 0 else np.nan,
            mae_pct=mae, mfe_pct=mfe, bars_held=ke - k0,
            entry_gap=(entry_px / c[c2] - 1),        # 开盘跳空幅度，检查执行假设
        )
        trades.append(tr)
        LOG.debug(f'[{symbol}] TRADE {ts[k0]} entry={entry_px:.6g} sl={sl:.6g} '
                  f'tp1={tp1:.6g} tp2={tp2:.6g} Bdrop={b_drop:.2%} burn={burn:.2%} '
                  f'-> {reason} pnl={pnl:+.2%} ({pnl / risk if risk > 0 else float("nan"):+.2f}R) '
                  f'held={ke - k0}bars')

        i = ke + 1 + cfg['COOLDOWN_BARS']

    return trades, events, stats


# ============================== 报告 ==============================
def funnel_report(stats, title='漏斗'):
    print('\n' + '-' * 60)
    print(f'🔎 {title}')
    print('-' * 60)
    keys = ['bars_scanned', 'A_fail_nan', 'A_fail_funding', 'A_fail_oi_level', 'A_fail_btc',
            'A_pass', 'B_fail', 'B_event',
            'C_fail_oi_not_stable', 'C_fail_burn_not_enough', 'C_fail_new_low', 'C_fail_weak_close',
            'C_pass', 'skip_bad_geometry', 'skip_entry_above_tp2', 'tp1_hit']
    for k in keys:
        if stats.get(k, 0):
            print(f'  {k:<26}: {stats[k]:>8}')
    if stats.get('B_event'):
        print(f'  --> GateC 通过率            : {stats.get("C_pass", 0) / stats["B_event"]:.1%}')


def report(tr, title):
    print('\n' + '=' * 60)
    print(f'📊 {title}')
    print('=' * 60)
    if len(tr) == 0:
        print('无信号')
        return None
    d = pd.DataFrame(tr).sort_values('entry_time').reset_index(drop=True)
    win, los = d[d.pnl_pct > 0], d[d.pnl_pct <= 0]
    rr = abs(win.pnl_pct.mean() / los.pnl_pct.mean()) if len(win) and len(los) else float('nan')

    eq = d.pnl_pct.cumsum()
    dd = (eq - eq.cummax()).min()

    print(f'笔数          : {len(d)}   （{d.symbol.nunique()} 个标的）')
    print(f'胜率          : {len(win) / len(d) * 100:.1f}%      (目标 50%-56%)')
    print(f'平均盈亏比    : {rr:.2f}        (目标 1.2-1.5)')
    print(f'单笔净期望    : {d.pnl_pct.mean() * 100:+.3f}%  /  {d.pnl_R.mean():+.3f} R')
    print(f'中位数收益    : {d.pnl_pct.median() * 100:+.3f}%')
    print(f'累计(等权)    : {d.pnl_pct.sum() * 100:+.2f}%   最大回撤(等权累加): {dd * 100:.2f}%')
    print(f'平均风险(R)   : {d.risk_pct.mean() * 100:.2f}%  | 平均 RR(TP1/TP2): '
          f'{d.rr_tp1.mean():.2f} / {d.rr_tp2.mean():.2f}')
    print(f'持仓中位数    : {d.bars_held.median():.0f} 根 5m  | MAE中位 {d.mae_pct.median() * 100:.2f}% '
          f'| MFE中位 {d.mfe_pct.median() * 100:.2f}%')
    print(f'入场跳空中位  : {d.entry_gap.median() * 100:+.3f}%')

    print('\n[平仓原因分布]')
    g = d.groupby('reason').agg(n=('pnl_pct', 'size'), win=('pnl_pct', lambda x: (x > 0).mean()),
                                avg=('pnl_pct', 'mean'), avgR=('pnl_R', 'mean'))
    g['win'] = (g['win'] * 100).round(1)
    g['avg'] = (g['avg'] * 100).round(3)
    print(g.sort_values('n', ascending=False).to_string())

    print('\n[按 B 段跌幅分层]')
    try:
        d['_bin'] = pd.qcut(d.b_drop, 4, duplicates='drop')
        print(d.groupby('_bin', observed=True).agg(
            n=('pnl_R', 'size'), winrate=('pnl_pct', lambda x: round((x > 0).mean() * 100, 1)),
            expR=('pnl_R', 'mean')).to_string())
    except Exception:
        pass

    print('\n[按 OI 燃烧幅度分层]')
    try:
        d['_bin2'] = pd.qcut(d.oi_burn_C, 4, duplicates='drop')
        print(d.groupby('_bin2', observed=True).agg(
            n=('pnl_R', 'size'), winrate=('pnl_pct', lambda x: round((x > 0).mean() * 100, 1)),
            expR=('pnl_R', 'mean')).to_string())
    except Exception:
        pass

    return d.drop(columns=[cc for cc in ['_bin', '_bin2'] if cc in d.columns])


# ============================== 入口 ==============================
if __name__ == '__main__':
    DATA_DIR = './data'
    BTC_DATA = r'W:\project\python_project\oke_auto_trade\kline_data\BTCUSDT_1m_2025-01-01_merged.csv'
    OUT_DIR = DATA_DIR

    btc_ret = load_btc_ret(BTC_DATA) if (CFG['USE_BTC_FREEZE'] and os.path.exists(BTC_DATA)) else None

    files = sorted(glob.glob(os.path.join(DATA_DIR, '*_ler_data.csv'))
                   + glob.glob(os.path.join(DATA_DIR, '*_oer_data.csv')))
    LOG.info(f'发现 {len(files)} 个标的文件')
    print(f'发现 {len(files)} 个标的')

    all_tr, all_ev, total = [], [], Counter()
    for fp in files:
        sym = os.path.basename(fp).replace('_ler_data.csv', '').replace('_oer_data.csv', '')
        try:
            df = prepare(fp, btc_ret, CFG, sym)
            tr, ev, st = run(df, sym, CFG)
            total.update(st)
            all_tr += tr
            all_ev += ev
            expr = np.mean([t['pnl_R'] for t in tr]) if tr else float('nan')
            msg = (f'  {sym:<24} A={st.get("A_pass", 0):>6} B={st.get("B_event", 0):>4} '
                   f'C={st.get("C_pass", 0):>3} 交易={len(tr):>3}'
                   + (f' | 期望 {expr:+.3f}R' if tr else ''))
            print(msg)
            LOG.info(msg)
        except Exception as e:
            print(f'  {sym} 异常跳过: {e}')
            LOG.exception(f'[{sym}] 处理异常')

    funnel_report(total, 'OER 全市场信号漏斗')
    d = report(all_tr, 'OER 全市场组合（Step-1 事件级 · 等权 · 往返 0.2% 成本）')

    if all_ev:
        pd.DataFrame(all_ev).sort_values('b_start').to_csv(
            os.path.join(OUT_DIR, 'OER_events.csv'), index=False)
        print(f'\n事件明细已落盘 ➜ OER_events.csv （{len(all_ev)} 条 Gate B 事件）')

    if d is not None:
        d.to_csv(os.path.join(OUT_DIR, 'OER_trades.csv'), index=False)
        m = d.set_index('entry_time').resample('ME').agg(
            n=('pnl_R', 'size'), expR=('pnl_R', 'mean'), sumPct=('pnl_pct', 'sum'))
        print('\n[按月分布]\n' + m.to_string())
        s = d.groupby('symbol').agg(n=('pnl_R', 'size'), expR=('pnl_R', 'mean'),
                                    sumPct=('pnl_pct', 'sum')).sort_values('sumPct', ascending=False)
        print('\n[按标的 Top/Bottom]\n' + pd.concat([s.head(10), s.tail(10)]).to_string())
        print('\n交易明细已落盘 ➜ OER_trades.csv ；完整日志 ➜ oer_backtest.log')