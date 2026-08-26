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
"""

import numpy as np
import pandas as pd

MS_MIN = 60000
MS_HOUR = 3600000.0

DEFAULT_FEE = 0.0005      # 单边综合成本(手续费+滑点+资金费率折算)
DEFAULT_ADD_STEP = 0.002  # 加仓间距(基于持仓均价)
DEFAULT_TP_STEP = 0.003   # 止盈间距(基于持仓均价)
DEFAULT_MULT = 2.0        # 加仓倍数(数量翻倍)


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
def _simulate_cycle(i0, n, adv, fav, closes, times, s,
                    fee, add_mul, tp_mul, mult, mtm_fee,
                    dd_abort, max_layer_hard):
    """
    s    : +1.0 = Long, -1.0 = Short
    adv  : 逆向价序列 (Long -> low , Short -> high)
    fav  : 顺向价序列 (Long -> high, Short -> low)
    返回 : (end_i, status, layer, net_pnl, total_fees, dd_t, dd_v, max_dd)
           status: 1=止盈闭环, 0=数据耗尽MTM, -1=熔断截断
    剪枝依据(等价变换):
        p_add 恒 < 历史逆向极值 => 只有逆向价创新极值的 bar 才可能加仓/刷新 max_dd
    """
    p0 = closes[i0]
    inv = 1.0 / p0

    vol = 1.0        # Total_Volume
    last_q = 1.0     # 上一笔订单数量
    cost = 1.0       # Total_Cost_Basis (按成交价累加的名义价值)
    fees = fee       # Accumulated_Fees (首单名义价值 = 1.0)
    layer = 0

    p_add = add_mul  # = 1.0 * add_mul
    p_tp = tp_mul

    worst = 1.0      # Cycle 内最差(逆向)归一化价
    max_dd = fees    # 开仓瞬间的资金缺口 = 已付手续费
    t0 = times[i0]
    dd_t = [t0 - t0 % MS_MIN]
    dd_v = [max_dd]

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
                m = times[i]
                m -= m % MS_MIN
                if m == dd_t[-1]:
                    dd_v[-1] = dd          # 同分钟只留最大值
                else:
                    dd_t.append(m)
                    dd_v.append(dd)
            # ---- 3) 熔断(防御性, 仅当 dd_abort > 所有待测 Margin 时无影响) ----
            if layer >= max_layer_hard or (dd_abort is not None and max_dd >= dd_abort):
                pe = closes[i] * inv
                cf = vol * pe * mtm_fee
                return (i, -1, layer, s * (vol * pe - cost) - fees - cf,
                        fees + cf, dd_t, dd_v, max_dd)
        # ---- 4) 止盈 ----
        if fav[i] * inv * s >= p_tp * s:
            xn = vol * p_tp
            cf = xn * fee
            return (i, 1, layer, s * (xn - cost) - fees - cf,
                    fees + cf, dd_t, dd_v, max_dd)

    # ---- 5) 数据耗尽: 强制盯市结算 ----
    i = n - 1
    pe = closes[i] * inv
    cf = vol * pe * mtm_fee
    return (i, 0, layer, s * (vol * pe - cost) - fees - cf,
            fees + cf, dd_t, dd_v, max_dd)


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
               progress=0):
    """
    第一阶段: 无限保证金平行宇宙生成。

    dd_format:
        "array" (默认, 省内存/最快): cycles_df 存 dd_times(int64 ms) / dd_vals(float64) 两个 numpy 列
        "list"  : 额外生成方案原文要求的 dd_steps 列 = [(ms, dd), ...]
        "both"  : 两者都有
    dd_abort:
        浮亏熔断阈值。None = 严格按方案(不熔断)。若设置, 必须 > 你要测试的最大 Margin,
        否则 Stage 2 会报错以防污染结论。
    """
    for c in ("open_time", "high", "low", "close"):
        if c not in df.columns:
            raise ValueError("缺少必需列: %s" % c)

    times_np = np.ascontiguousarray(df["open_time"].to_numpy(dtype=np.int64))
    n = times_np.shape[0]
    if n == 0:
        raise ValueError("空数据")
    if n > 1 and np.any(np.diff(times_np) <= 0):
        raise ValueError("open_time 必须严格递增(请先排序去重)")

    highs_np = np.ascontiguousarray(df["high"].to_numpy(dtype=np.float64))
    lows_np = np.ascontiguousarray(df["low"].to_numpy(dtype=np.float64))
    closes_np = np.ascontiguousarray(df["close"].to_numpy(dtype=np.float64))
    if not np.all(np.isfinite(highs_np) & np.isfinite(lows_np) & np.isfinite(closes_np)):
        raise ValueError("high/low/close 存在 NaN/Inf")

    # 信号采集(允许同一 bar 同时出多空 -> 两个平行 Cycle)
    if long_col in df.columns:
        li = np.flatnonzero(df[long_col].to_numpy() != 0)
    else:
        li = np.empty(0, dtype=np.int64)
    if short_col in df.columns:
        si = np.flatnonzero(df[short_col].to_numpy() != 0)
    else:
        si = np.empty(0, dtype=np.int64)
    sig_idx = np.concatenate([li.astype(np.int64), si.astype(np.int64)])
    sig_dir = np.concatenate([np.ones(li.shape[0]), -np.ones(si.shape[0])])
    order = np.argsort(sig_idx, kind="stable")   # 同 bar: Long 先于 Short
    sig_idx = sig_idx[order]
    sig_dir = sig_dir[order]
    m = sig_idx.shape[0]

    # 性能: list 索引比 numpy 标量索引快 2~3 倍(数值逐位一致)
    if fast_lists is None:
        fast_lists = n <= 3000000
    if fast_lists:
        highs = highs_np.tolist()
        lows = lows_np.tolist()
        closes = closes_np.tolist()
        times = times_np.tolist()
    else:
        highs, lows, closes, times = highs_np, lows_np, closes_np, times_np

    add_mul_l = 1.0 - add_step
    tp_mul_l = 1.0 + tp_step
    add_mul_s = 1.0 + add_step
    tp_mul_s = 1.0 - tp_step
    mtm_fee = fee_rate if mtm_charge_close_fee else 0.0

    out_dir = np.empty(m, dtype=object)
    out_bar = np.empty(m, dtype=np.int64)
    out_s = np.empty(m, dtype=np.int64)
    out_e = np.empty(m, dtype=np.int64)
    out_status = np.empty(m, dtype=np.int8)
    out_layer = np.empty(m, dtype=np.int32)
    out_net = np.empty(m, dtype=np.float64)
    out_fee = np.empty(m, dtype=np.float64)
    out_mdd = np.empty(m, dtype=np.float64)
    out_nst = np.empty(m, dtype=np.int32)
    dd_times_col = np.empty(m, dtype=object)
    dd_vals_col = np.empty(m, dtype=object)

    for k in range(m):
        i0 = int(sig_idx[k])
        s = float(sig_dir[k])
        if s > 0.0:
            res = _simulate_cycle(i0, n, lows, highs, closes, times, 1.0,
                                  fee_rate, add_mul_l, tp_mul_l, multiplier,
                                  mtm_fee, dd_abort, max_layer_hard)
        else:
            res = _simulate_cycle(i0, n, highs, lows, closes, times, -1.0,
                                  fee_rate, add_mul_s, tp_mul_s, multiplier,
                                  mtm_fee, dd_abort, max_layer_hard)
        end_i, status, layer, net, tfee, dd_t, dd_v, mdd = res
        out_dir[k] = "Long" if s > 0.0 else "Short"
        out_bar[k] = i0
        out_s[k] = times_np[i0]
        out_e[k] = times_np[end_i]
        out_status[k] = status
        out_layer[k] = layer
        out_net[k] = net
        out_fee[k] = tfee
        out_mdd[k] = mdd
        nst = len(dd_t)
        out_nst[k] = nst
        dd_times_col[k] = np.fromiter(dd_t, dtype=np.int64, count=nst)
        dd_vals_col[k] = np.fromiter(dd_v, dtype=np.float64, count=nst)
        if progress and (k + 1) % progress == 0:
            print("[stage1] %d / %d cycles" % (k + 1, m))

    cycles = pd.DataFrame({
        "cycle_id": np.arange(m, dtype=np.int64),
        "direction": out_dir,
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
            np.where(out_status == 1, 0, np.where(out_status == 0, 1, 2)),
            categories=["tp", "mtm", "truncated"]),
        "signal_bar": out_bar,
        "start_ms": out_s,
        "end_ms": out_e,
    })
    cycles["dd_times"] = pd.Series(dd_times_col, index=cycles.index, dtype=object)
    cycles["dd_vals"] = pd.Series(dd_vals_col, index=cycles.index, dtype=object)
    if dd_format in ("list", "both"):
        cycles["dd_steps"] = pd.Series(
            [list(zip(dd_times_col[i].tolist(), dd_vals_col[i].tolist())) for i in range(m)],
            index=cycles.index, dtype=object)
    if dd_format == "list":
        cycles.drop(columns=["dd_times", "dd_vals"], inplace=True)

    cycles.attrs.update({
        "data_start_ms": int(times_np[0]),
        "data_end_ms": int(times_np[-1]),
        "n_bars": int(n),
        "fee_rate": fee_rate,
        "add_step": add_step,
        "tp_step": tp_step,
        "multiplier": multiplier,
        "dd_abort": dd_abort,
        "max_layer_hard": max_layer_hard,
        "mtm_charge_close_fee": bool(mtm_charge_close_fee),
    })
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
        self._dir = d["direction"].to_numpy(object)
        self._start = np.ascontiguousarray(d["start_ms"].to_numpy(np.int64))
        self._end = np.ascontiguousarray(d["end_ms"].to_numpy(np.int64))
        self._net = np.ascontiguousarray(d["net_pnl"].to_numpy(np.float64))
        self._fee = np.ascontiguousarray(d["total_fees"].to_numpy(np.float64))
        self._mdd = np.ascontiguousarray(d["max_dd"].to_numpy(np.float64))
        self._layer = np.ascontiguousarray(d["max_layer"].to_numpy(np.int64))
        self._closed = np.ascontiguousarray(d["is_closed"].to_numpy(bool))
        self._trunc = (d["status"].astype(str).to_numpy() == "truncated")

        if "dd_times" in d.columns:
            self._ddt = list(d["dd_times"].to_numpy())
            self._ddv = list(d["dd_vals"].to_numpy())
        else:  # dd_format == "list"
            self._ddt = [np.fromiter((t for t, _ in s), np.int64, len(s)) for s in d["dd_steps"]]
            self._ddv = [np.fromiter((v for _, v in s), np.float64, len(s)) for s in d["dd_steps"]]

        a = cycles_df.attrs
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
        ddt, ddv = self._ddt, self._ddv
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
            is_long = self._dir[j] == "Long"
            if margin > mdd[j]:
                # ---------- 存活 ----------
                if trunc[j]:
                    raise ValueError(
                        "cycle_id=%d 被 dd_abort 熔断却在 margin=%g 下存活, "
                        "结果不可信。请调大 dd_abort 或取消熔断。" % (self._cid[j], margin))
                pnl = float(net[j])
                cum += pnl
                rows.append((self._cid[j], self._dir[j], int(start[j]), int(end[j]),
                             "tp" if closed[j] else "mtm", pnl, float(self._fee[j]),
                             int(self._layer[j]), float(mdd[j]), cum))
                if not closed[j]:
                    break                     # 历史终点未平仓单 -> 回测强制结束
                cur = int(end[j])
            else:
                # ---------- 爆仓(查表, dd_vals 严格单调 => 二分) ----------
                v = ddv[j]
                k = int(np.searchsorted(v, margin, side="left"))
                death = int(ddt[j][k])
                pnl = -margin
                cum += pnl
                rows.append((self._cid[j], self._dir[j], int(start[j]), death,
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
                                       dtype=np.float64)) / MS_HOUR)) if len(deaths) > 1 else np.nan)
        rep["mean_life_hour"] = float(np.mean([(L[1] - L[0]) / MS_HOUR for L in done]))
        rep["blowups_per_year"] = (len(deaths) / span_y) if span_y > 0 else np.nan
    else:
        rep["expected_lifespan_hour"] = np.inf   # 样本内未爆仓(右删失)
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


def run_backtest(df, margins=(0.02, 0.16, 0.6, 2.55, 10.0, 40.6),
                 report_margin=None, **stage1_kw):
    """一站式: Stage1 -> Replayer -> 扫描 -> 详细报告"""
    cycles = run_stage1(df, **stage1_kw)
    rp = TimelineReplayer(cycles)
    sweep = sweep_margins(rp, margins)
    if report_margin is None and len(margins):
        report_margin = list(margins)[len(margins) // 2]
    trades = rp.run(report_margin)
    report = evaluate_free_ride(trades, cycles, report_margin)
    print_report(report)
    return cycles, rp, sweep, trades, report


# =====================================================================
# 4. Demo
# =====================================================================
if __name__ == "__main__":
    rng = np.random.default_rng(7)
    n = 300000                                  # 1m K 线
    t0 = pd.Timestamp("2024-01-01").value // 10 ** 6
    ot = t0 + np.arange(n, dtype=np.int64) * 60000
    ret = rng.standard_normal(n) * 0.0007
    close = 30000.0 * np.exp(np.cumsum(ret))
    op = np.concatenate([[30000.0], close[:-1]])
    wig = np.abs(rng.standard_normal(n)) * 0.0006 * close
    high = np.maximum(op, close) + wig
    low = np.minimum(op, close) - wig
    df = pd.DataFrame({"open_time": ot, "open": op, "high": high, "low": low,
                       "close": close, "volume": rng.random(n) * 10})
    # 简单信号: 收盘跌破 60 周期均线 1% 做多, 突破 1% 做空(仅示例)
    ma = pd.Series(close).rolling(60).mean().to_numpy()
    df["long_signal"] = ((close < ma * 0.99) & (np.arange(n) % 30 == 0)).astype(np.int8)
    df["short_signal"] = ((close > ma * 1.01) & (np.arange(n) % 30 == 0)).astype(np.int8)

    print(build_ladder(14).to_string(index=False))
    cycles, rp, sweep, trades, rep = run_backtest(df)
    print(sweep.to_string(index=False))