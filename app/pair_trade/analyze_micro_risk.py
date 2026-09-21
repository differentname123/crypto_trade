# -*- coding: utf-8 -*-
"""直接从新版逐笔交易计算排行榜、风险指标及四等分时间表现。

依赖：pandas、numpy。本脚本可独立运行，无需导入回测脚本。
仅支持新版结构，不推断或兼容旧数据。百分比在CSV中以带(%)的列明确标注。
"""
import os
import json
import hashlib
import tempfile

import numpy as np
import pandas as pd

# None表示不限制该参数；MAX_DEVIATION不使用持仓期限过滤。
FILTER_Z = [6.0, 7.0, 8.0, 9.0]
FILTER_HOLD = [48, 72, 96, 120, 168]
FILTER_SIG = [24, 48, 60]
FILTER_BETA = [30, 60, 90, 120]
FILTER_RIGHT = [False, True]
FILTER_MODE = ["FIXED_HOLD"]

REQUIRED_COLUMNS = [
    "run_id", "trade_id", "symbol", "status", "entry_time", "exit_time",
    "direction", "vol_group", "position_mode", "entry_gross_notional", "entry_cost",
    "net_pnl", "net_return", "mae_return", "mfe_return", "mae_time", "mfe_time",
]
DATE_COLUMNS = ["entry_time", "exit_time", "mae_time", "mfe_time"]
NUMERIC_COLUMNS = [
    "entry_gross_notional", "entry_cost", "net_pnl", "net_return", "mae_return", "mfe_return",
]
TIME_COLUMNS = [
    "Run", "Run_Complete", "Group", "Period", "Start_UTC", "End_UTC",
    "End_Inclusive", "Trades", "Win(%)", "Avg_Ret(%)", "Sum_Ret(%)", "Net_PnL_USDT",
]


def atomic_csv(df, path):
    fd, temporary = tempfile.mkstemp(prefix=os.path.basename(path) + ".",
                                     suffix=".tmp", dir=os.path.dirname(path) or ".")
    os.close(fd)
    try:
        df.to_csv(temporary, index=False, encoding="utf-8-sig")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True,
                                     ensure_ascii=False).encode()).hexdigest()


def matches_filters(manifest):
    market = manifest["market"]
    checks = [
        (manifest["z"], FILTER_Z),
        (market["beta"] / 24, FILTER_BETA),
        (market["signal"], FILTER_SIG),
        (manifest["right_side_entry"], FILTER_RIGHT),
        (manifest["position_mode"], FILTER_MODE),
    ]
    if manifest["position_mode"] == "FIXED_HOLD":
        checks.append((manifest["holding"], FILTER_HOLD))
    return all(allowed is None or value in allowed for value, allowed in checks)


def load_trades(root, manifest):
    """校验交易身份、类型和极值完整性；避免坏数据无声进入统计。"""
    filenames = sorted(f for f in os.listdir(root) if f.endswith("_trades.csv"))
    if not filenames:
        raise ValueError("完成目录中没有逐笔交易CSV")
    frames = []
    for name in filenames:
        frame = pd.read_csv(os.path.join(root, name))
        missing = sorted(set(REQUIRED_COLUMNS) - set(frame.columns))
        if missing:
            raise ValueError(f"{name}缺少新版字段: {missing}")
        if not frame.empty:
            frames.append(frame[REQUIRED_COLUMNS])
    df = (pd.concat(frames, ignore_index=True) if frames
          else pd.DataFrame(columns=REQUIRED_COLUMNS))
    for col in DATE_COLUMNS:
        df[col] = pd.to_datetime(df[col], utc=True, errors="raise", format="mixed")
    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="raise")
    if df.empty:
        return df
    if df[["run_id", "trade_id", "symbol", "entry_time"]].isna().any().any():
        raise ValueError("交易身份或开仓时间为空")
    if df["trade_id"].duplicated().any():
        raise ValueError("存在重复trade_id，拒绝重复统计")
    if not df["run_id"].eq(fingerprint(manifest)).all():
        raise ValueError("交易run_id与run_manifest.json不一致")
    if not df["position_mode"].eq(manifest["position_mode"]).all():
        raise ValueError("交易仓位模式与manifest不一致")
    if not df["status"].isin([
        "CLOSED", "UNRESOLVED_MISSING_EXIT", "UNRESOLVED_END_OF_DATA"]).all():
        raise ValueError("发现未知交易状态")
    if not df["direction"].isin(["LONG_ALT", "SHORT_ALT"]).all():
        raise ValueError("发现未知ALT方向")
    if not df["vol_group"].isin(["High_Vol", "Low_Vol"]).all():
        raise ValueError("发现未知成交额分组")
    if not (np.isfinite(df["entry_gross_notional"])
            & df["entry_gross_notional"].gt(0)
            & np.isfinite(df["entry_cost"]) & df["entry_cost"].ge(0)).all():
        raise ValueError("开仓名义金额或成本无效")
    closed = df["status"].eq("CLOSED")
    done = df.loc[closed]
    if (done["exit_time"].isna().any()
            or not done["exit_time"].gt(done["entry_time"]).all()
            or not np.isfinite(done[["net_pnl", "net_return"]].to_numpy()).all()):
        raise ValueError("已平仓交易缺少有效盈亏，或平仓时间不晚于开仓时间")
    if not np.isclose(done["net_return"], done["net_pnl"] / done["entry_gross_notional"],
                      rtol=1e-10, atol=1e-12).all():
        raise ValueError("净收益率与净盈亏/初始名义金额不一致")
    excursion_columns = ["mae_return", "mfe_return", "mae_time", "mfe_time"]
    if (df.loc[~closed, "exit_time"].notna().any()
            or df.loc[~closed, excursion_columns].notna().any().any()):
        raise ValueError("未结算交易不能带有已完成的平仓时间或极值")
    if df["mae_return"].notna().ne(df["mfe_return"].notna()).any():
        raise ValueError("MAE与MFE应同时有效或同时为空")
    valid = df["mae_return"].notna()
    if df.loc[~valid, ["mae_time", "mfe_time"]].notna().any().any():
        raise ValueError("极值为空时不能保留极值时间")
    v = df.loc[valid]
    if (not np.isfinite(v[["mae_return", "mfe_return"]].to_numpy()).all()
            or not v["mae_return"].le(0).all() or not v["mfe_return"].ge(0).all()):
        raise ValueError("MAE须为有限非正数，MFE须为有限非负数")
    for value_col, time_col in [("mae_return", "mae_time"), ("mfe_return", "mfe_time")]:
        nonzero = v[value_col].ne(0)
        if v[time_col].notna().ne(nonzero).any():
            raise ValueError(f"{value_col}非零时须有时间，为零时须为空时间")
        times = v.loc[nonzero, time_col]
        if not (times.ge(v.loc[nonzero, "entry_time"])
                & times.le(v.loc[nonzero, "exit_time"])).all():
            raise ValueError(f"{time_col}超出持仓区间")
    if (v["mae_return"].gt(np.minimum(v["net_return"], 0) + 1e-12).any()
            or v["mfe_return"].lt(np.maximum(v["net_return"], 0) - 1e-12).any()):
        raise ValueError("极值没有覆盖最终净收益率，可能漏掉平仓时点")
    return df


def max_consecutive_losses(closed):
    """按平仓顺序统计；同刻按entry_time、symbol、trade_id确定稳定顺序。

    net_pnl<0才算亏损；盈利或恰好为0均中断。并行交易没有唯一自然顺序，
    这里采用可复现的逐笔平仓顺序，不能解释为账户连续亏损小时数。
    """
    ordered = closed.sort_values(["exit_time", "entry_time", "symbol", "trade_id"],
                                 kind="mergesort")
    longest = current = 0
    for losing in ordered["net_pnl"].lt(0):
        current = current + 1 if losing else 0
        longest = max(longest, current)
    return longest


def max_concurrent_pairs(trades):
    """扫描[entry_time, exit_time)区间；未结算交易一直占仓，不捏造退出。

    同时刻先平再开：合并同刻增减后统计，不会将轮换瞬间误算为2对。
    每条记录是一个ALT/BTC配对，不能将两条腿计作两对。
    """
    if trades.empty:
        return 0
    entries = pd.Series(1, index=pd.DatetimeIndex(trades["entry_time"]), dtype="int64")
    closed = trades.loc[trades["status"].eq("CLOSED")]
    exits = pd.Series(-1, index=pd.DatetimeIndex(closed["exit_time"]), dtype="int64")
    counts = pd.concat([entries, exits]).groupby(level=0).sum().sort_index().cumsum()
    if counts.lt(0).any():
        raise ValueError("持仓事件存在负数计数，请检查交易时间")
    return int(counts.max())


def calculate_metrics(trades):
    closed = trades.loc[trades["status"].eq("CLOSED")]
    n = len(closed)
    mae = closed["mae_return"].dropna()
    return dict(
        trade_count=n,
        unresolved_count=len(trades) - n,
        unresolved_entry_cost=trades.loc[~trades["status"].eq("CLOSED"), "entry_cost"].sum(),
        win_rate=closed["net_pnl"].gt(0).mean() if n else np.nan,
        avg_net_return=closed["net_return"].mean(),
        sum_net_return=closed["net_return"].sum(),
        sum_net_pnl=closed["net_pnl"].sum(),
        max_consecutive_losses=max_consecutive_losses(closed),
        max_concurrent_pairs=max_concurrent_pairs(trades),
        mae_valid_count=len(mae),
        mae_missing_count=n - len(mae),
        avg_mae_return=mae.mean(),
        p10_mae_return=mae.quantile(0.10, interpolation="linear") if len(mae) else np.nan,
        worst_mae_return=mae.min(),
        avg_mfe_return=closed["mfe_return"].mean(),
    )


def four_time_periods(closed, start, end):
    """等日历时长，按平仓时间归属；前3段左闭右开，最后一段双闭。"""
    edges = pd.date_range(start=start, end=end, periods=5)
    records = []
    for i in range(4):
        left, right = edges[i], edges[i + 1]
        final = i == 3
        if closed.empty:
            part = closed
        else:
            mask = (closed["exit_time"].ge(left)
                    & (closed["exit_time"].le(right) if final
                       else closed["exit_time"].lt(right)))
            part = closed.loc[mask]
        n = len(part)
        records.append({
            "Period": i + 1, "Start_UTC": left.isoformat(), "End_UTC": right.isoformat(),
            "End_Inclusive": final, "Trades": n,
            "Win(%)": part["net_pnl"].gt(0).mean() * 100 if n else np.nan,
            "Avg_Ret(%)": part["net_return"].mean() * 100,
            "Sum_Ret(%)": part["net_return"].sum() * 100,
            "Net_PnL_USDT": part["net_pnl"].sum(),
        })
    if sum(r["Trades"] for r in records) != len(closed):
        raise ValueError("有平仓时间落在四段观察区间之外，拒绝遗漏交易")
    return records


def metric_columns(prefix, metrics):
    """CSV中的(%)字段单位为百分数，其余收益率不隐式换单位。"""
    return {
        f"{prefix}_Trades": metrics["trade_count"],
        f"{prefix}_Win(%)": metrics["win_rate"] * 100,
        f"{prefix}_Ret(%)": metrics["avg_net_return"] * 100,
        f"{prefix}_SumRet(%)": metrics["sum_net_return"] * 100,
        f"{prefix}_NetPnL": metrics["sum_net_pnl"],
        f"{prefix}_Unresolved": metrics["unresolved_count"],
        f"{prefix}_UnresolvedEntryCost": metrics["unresolved_entry_cost"],
        f"{prefix}_MaxLossStreak": metrics["max_consecutive_losses"],
        f"{prefix}_MaxConcurrent": metrics["max_concurrent_pairs"],
        f"{prefix}_MAEValid": metrics["mae_valid_count"],
        f"{prefix}_MAEMissing": metrics["mae_missing_count"],
        f"{prefix}_AvgMAE(%)": metrics["avg_mae_return"] * 100,
        f"{prefix}_P10MAE(%)": metrics["p10_mae_return"] * 100,
        f"{prefix}_WorstMAE(%)": metrics["worst_mae_return"] * 100,
        f"{prefix}_AvgMFE(%)": metrics["avg_mfe_return"] * 100,
    }


def display_pct(value):
    return "--" if pd.isna(value) else f"{value:.2f}%"


def print_performance_summary(base_dir="trade_results"):
    """读取每组明细一次；不把所有参数组合的逐笔记录同时留在内存中。"""
    if not os.path.isdir(base_dir):
        fallback = os.path.join("..", base_dir)
        if os.path.isdir(fallback):
            base_dir = fallback
        else:
            print(f"目录不存在: {os.path.abspath(base_dir)}")
            return pd.DataFrame()
    base_dir = os.path.abspath(base_dir)
    print(f"正在扫描: {base_dir}")
    print("从新版逐笔明细统计；仅扫描含.done成功标记的参数目录。")
    runs, errors = [], []
    unfinished = 0
    for root, dirs, files in os.walk(base_dir):
        dirs.sort()
        if "run_manifest.json" not in files:
            continue
        if ".done" not in files:
            unfinished += 1
            continue
        try:
            with open(os.path.join(root, "run_manifest.json"), encoding="utf-8") as f:
                manifest = json.load(f)
            if not matches_filters(manifest):
                continue
            if (manifest["market"]["version"] != "fixed_beta_excursion_net_v3"
                    or manifest["excursion"]["basis"] != "net_liquidation_return"):
                raise ValueError("仅支持新版净清算MAE/MFE数据")
            with open(os.path.join(root, "evaluation_window.json"), encoding="utf-8") as f:
                window = json.load(f)
            if window["run_id"] != fingerprint(manifest):
                raise ValueError("观察区间文件与manifest不一致")
            start = pd.to_datetime(window["start"], utc=True) if window["start"] else None
            end = pd.to_datetime(window["end"], utc=True) if window["end"] else None
            if start is not None and (end is None or start > end):
                raise ValueError("观察时间区间无效")
            runs.append(dict(root=root, manifest=manifest, start=start, end=end))
        except Exception as exc:
            errors.append((root, str(exc)))
    # 各参数使用同一组日历边界；完整观察区间的并集保留无交易时段。
    starts = [r["start"] for r in runs if r["start"] is not None]
    ends = [r["end"] for r in runs if r["start"] is not None]
    common_start, common_end = (min(starts), max(ends)) if starts else (None, None)
    summary_rows, group_rows, time_rows = [], [], []
    for run in runs:
        root, manifest = run["root"], run["manifest"]
        try:
            trades = load_trades(root, manifest)
            if not trades.empty and (run["start"] is None
                                     or trades["entry_time"].min() < run["start"]
                                     or trades["entry_time"].max() > run["end"]):
                raise ValueError("开仓时间不在本组实际观察区间内")
            done = trades.loc[trades["status"].eq("CLOSED")]
            if not done.empty and done["exit_time"].max() > run["end"]:
                raise ValueError("平仓时间超过本组实际观察区间")
            complete = len(trades) == len(done)
            risk_complete = complete and done["mae_return"].notna().all()
            market = manifest["market"]
            hold = manifest["holding"]
            run_label = os.path.basename(root)
            row = {
                "Run": run_label, "Run_Complete": complete, "Risk_Complete": bool(risk_complete),
                "Z": float(manifest["z"]), "Hold": f"{hold}h" if hold is not None else "NA",
                "Beta": f"{market['beta'] // 24}d", "Sig": f"{market['signal']}h",
                "RightSide": "Yes" if manifest["right_side_entry"] else "No",
                "Mode": "MAX_DEV" if manifest["position_mode"] == "MAX_DEVIATION" else "FIXED",
                "Run_Start_UTC": run["start"].isoformat() if run["start"] is not None else None,
                "Run_End_UTC": run["end"].isoformat() if run["end"] is not None else None,
            }

            # 增加交叉维度的分组支持
            groups = {
                "All": trades,
                "Long": trades.loc[trades["direction"].eq("LONG_ALT")],
                "Short": trades.loc[trades["direction"].eq("SHORT_ALT")],
                "High": trades.loc[trades["vol_group"].eq("High_Vol")],
                "Low": trades.loc[trades["vol_group"].eq("Low_Vol")],
                "Long_High": trades.loc[trades["direction"].eq("LONG_ALT") & trades["vol_group"].eq("High_Vol")],
                "Long_Low": trades.loc[trades["direction"].eq("LONG_ALT") & trades["vol_group"].eq("Low_Vol")],
                "Short_High": trades.loc[trades["direction"].eq("SHORT_ALT") & trades["vol_group"].eq("High_Vol")],
                "Short_Low": trades.loc[trades["direction"].eq("SHORT_ALT") & trades["vol_group"].eq("Low_Vol")],
            }
            local_groups, local_periods = [], []
            for name, subset in groups.items():
                metrics = calculate_metrics(subset)
                row.update(metric_columns(name, metrics))
                local_groups.append(dict(Run=run_label, Run_Complete=complete,
                                         Group=name, **metrics))
                if common_start is not None:
                    part_closed = subset.loc[subset["status"].eq("CLOSED")]
                    for record in four_time_periods(part_closed, common_start, common_end):
                        local_periods.append(dict(Run=run_label, Run_Complete=complete,
                                                  Group=name, **record))
            # 本组全部校验成功后才发布本组结果，不留下半组统计。
            summary_rows.append(row)
            group_rows.extend(local_groups)
            time_rows.extend(local_periods)
        except Exception as exc:
            errors.append((root, str(exc)))
    if unfinished:
        print(f"跳过 {unfinished} 个未完成参数目录。")
    for root, reason in errors:
        print(f"读取失败，未纳入本次结果: {root}: {reason}")
    if not summary_rows:
        print("没有符合条件且通过校验的新版结果；请检查过滤参数或先运行回测。")
        return pd.DataFrame()
    summary = pd.DataFrame(summary_rows).sort_values(
        ["Run_Complete", "All_Ret(%)", "Z"], ascending=[False, False, True],
        na_position="last", kind="mergesort").reset_index(drop=True)
    summary.insert(0, "ID", range(1, len(summary) + 1))
    groups_df = pd.DataFrame(group_rows)
    periods_df = pd.DataFrame(time_rows, columns=TIME_COLUMNS)
    atomic_csv(summary, os.path.join(base_dir, "performance_summary.csv"))
    atomic_csv(groups_df, os.path.join(base_dir, "performance_groups.csv"))
    atomic_csv(periods_df, os.path.join(base_dir, "performance_time_quarters.csv"))

    # ---------------- 打印输出层扁平化重构 ----------------
    print_df = summary.copy()
    quadrants = ["Long_High", "Long_Low", "Short_High", "Short_Low"]

    if common_start is not None:
        # 保留All的季度数据（仅为打印整体使用）
        all_periods = periods_df.loc[periods_df["Group"].eq("All")]
        for p in range(1, 5):
            p_data = all_periods.loc[all_periods["Period"].eq(p)].set_index("Run")
            print_df[f"Q{p}_Trades"] = print_df["Run"].map(p_data["Trades"])
            print_df[f"Q{p}_Win(%)"] = print_df["Run"].map(p_data["Win(%)"])
            print_df[f"Q{p}_Ret(%)"] = print_df["Run"].map(p_data["Avg_Ret(%)"])
            print_df[f"Q{p}_SumRet(%)"] = print_df["Run"].map(p_data["Sum_Ret(%)"])

        # 为4个象限提取季度数据
        for quad in quadrants:
            quad_periods = periods_df.loc[periods_df["Group"].eq(quad)]
            for p in range(1, 5):
                p_data = quad_periods.loc[quad_periods["Period"].eq(p)].set_index("Run")
                print_df[f"{quad}_Q{p}_Trades"] = print_df["Run"].map(p_data["Trades"])
                print_df[f"{quad}_Q{p}_Ret(%)"] = print_df["Run"].map(p_data["Avg_Ret(%)"])
                print_df[f"{quad}_Q{p}_SumRet(%)"] = print_df["Run"].map(p_data["Sum_Ret(%)"])

    total = len(print_df)
    cond_all = pd.Series(False, index=print_df.index)

    # 按4个象限独立判断过滤条件
    for quad in quadrants:
        cond_quad_q = pd.Series(True, index=print_df.index)
        if common_start is not None:
            quad_total_sum_ret = print_df[f"{quad}_SumRet(%)"].fillna(0)
            for p in range(1, 5):
                col_ret = f"{quad}_Q{p}_Ret(%)"
                col_sum_ret = f"{quad}_Q{p}_SumRet(%)"
                if col_ret in print_df.columns and col_sum_ret in print_df.columns:
                    # fillna(0) 依然限制无交易记为不过滤要求(>0)
                    cond_quad_q = cond_quad_q & (print_df[col_ret].fillna(0) > 0)
                    # 新增条件：每个Q的总收益占比都要超过10%
                    cond_quad_q = cond_quad_q & (print_df[col_sum_ret].fillna(0) > 0.1 * quad_total_sum_ret)
        else:
            cond_quad_q = pd.Series(False, index=print_df.index)

        cond_quad_risk = print_df[f"{quad}_WorstMAE(%)"].fillna(0) > -200

        # 只要该象限同时满足季度均收益>0、季度收益占比>10%和回撤>-200%条件
        cond_quad_pass = cond_quad_q & cond_quad_risk

        # 将通过结果聚合到整体过滤器中 (OR)
        cond_all = cond_all | cond_quad_pass

    print("\n" + "=" * 50 + " 过滤条件与统计 " + "=" * 50)
    print("过滤要求（按4个象限独立判断，满足任意1个即可入选）：")
    print("  1. 该象限每个Q的平均单笔收益是正数 (Q1~Q4_Ret > 0)")
    print("  2. 该象限每个Q的总收益占比均超过 10% (Q1~Q4_SumRet > 10% * 象限总收益)")
    print("  3. 该象限单笔最大回撤小于 200% (WorstMAE > -200%)")
    print(f"\n总策略数: {total}")

    cnt_all = cond_all.sum()
    pct_all = (cnt_all / total * 100) if total else 0
    print(f"最终满足条件通过数量: {cnt_all} ({pct_all:.2f}%)")
    print("=" * 116)

    def get_stat(r_data, prefix):
        t = int(r_data.get(f"{prefix}_Trades", 0)) if pd.notna(r_data.get(f"{prefix}_Trades", np.nan)) else 0
        w = f"{r_data.get(f'{prefix}_Win(%)', np.nan):.2f}%" if pd.notna(
            r_data.get(f"{prefix}_Win(%)", np.nan)) else "--"
        ret = f"{r_data.get(f'{prefix}_Ret(%)', np.nan):.4f}%" if pd.notna(
            r_data.get(f"{prefix}_Ret(%)", np.nan)) else "--"
        sr = f"{r_data.get(f'{prefix}_SumRet(%)', np.nan):.4f}%" if pd.notna(
            r_data.get(f"{prefix}_SumRet(%)", np.nan)) else "--"
        return t, w, ret, sr

    def print_quadrant_detail(row, quad_name, display_name):
        """专为打印某一个象限的详细数据属性"""
        t, w, ret, sr = get_stat(row, quad_name)
        if t == 0:
            print(f"  🔹 {display_name} -> 无交易")
            return

        net_pnl = f"{row.get(f'{quad_name}_NetPnL', np.nan):.2f}" if pd.notna(
            row.get(f'{quad_name}_NetPnL', np.nan)) else "--"
        max_loss = row.get(f'{quad_name}_MaxLossStreak', '--')
        max_concurrent = row.get(f'{quad_name}_MaxConcurrent', '--')
        worst_mae = f"{row.get(f'{quad_name}_WorstMAE(%)', np.nan):.2f}%" if pd.notna(
            row.get(f'{quad_name}_WorstMAE(%)', np.nan)) else "--"
        avg_mae = f"{row.get(f'{quad_name}_AvgMAE(%)', np.nan):.2f}%" if pd.notna(
            row.get(f'{quad_name}_AvgMAE(%)', np.nan)) else "--"

        print(f"  🔹 {display_name} -> 笔数 {t} | 胜率 {w} | 均收益 {ret} | 总收益 {sr} | 净盈亏(USDT) {net_pnl}")
        print(
            f"      ⚠️ 风险指标 -> 最大连亏 {max_loss}次 | 最大并发 {max_concurrent} | 平均MAE {avg_mae} | 最差MAE {worst_mae}")

        q_info = []
        if common_start is not None:
            for p in range(1, 5):
                q_trades = row.get(f"{quad_name}_Q{p}_Trades", np.nan)
                if pd.notna(q_trades) and q_trades > 0:
                    q_ret = row.get(f"{quad_name}_Q{p}_Ret(%)", np.nan)
                    q_ret_str = f"{q_ret:.4f}%" if pd.notna(q_ret) else "--"

                    q_sum_ret = row.get(f"{quad_name}_Q{p}_SumRet(%)", np.nan)
                    q_sum_str = f"{q_sum_ret:.4f}%" if pd.notna(q_sum_ret) else "--"

                    q_info.append(f"Q{p}(笔:{int(q_trades)} 均收益:{q_ret_str} 总收益:{q_sum_str})")
                else:
                    q_info.append(f"Q{p}(无交易)")
        if q_info:
            print(f"      📅 季度分布 -> {' | '.join(q_info)}")

    for complete, title in [(True, "已全部结算组合（已过滤，按单笔平均净收益率排序）"),
                            (False, "含未结算交易子集（已过滤，不进入完整结果排行）")]:
        selected = print_df.loc[print_df["Run_Complete"].eq(complete) & cond_all]
        if selected.empty:
            continue
        print("\n" + "=" * 110 + "\n" + title)
        for _, row in selected.iterrows():
            params = f"ID: {row['ID']} | Z: {row['Z']} | Hold: {row['Hold']} | Beta: {row['Beta']} | Sig: {row['Sig']} | RightSide: {row['RightSide']} | Mode: {row['Mode']}"

            all_t, all_w, all_r, all_sr = get_stat(row, "All")
            net_pnl = f"{row['All_NetPnL']:.2f}" if pd.notna(row['All_NetPnL']) else "--"
            max_loss = row['All_MaxLossStreak']
            max_concurrent = row['All_MaxConcurrent']
            worst_mae = f"{row['All_WorstMAE(%)']:.2f}%" if pd.notna(row['All_WorstMAE(%)']) else "--"
            avg_mae = f"{row['All_AvgMAE(%)']:.2f}%" if pd.notna(row['All_AvgMAE(%)']) else "--"

            print(f"⚙️ 参数 -> {params}")
            print(
                f"💰 整体汇总 -> 交易次数 {all_t} | 胜率 {all_w} | 均净收益率 {all_r} | 总净收益率 {all_sr} | 总净盈亏(USDT) {net_pnl}")
            print(
                f"⚠️ 整体风险 -> 最大连亏 {max_loss}次 | 最大并发 {max_concurrent} | 平均单笔MAE {avg_mae} | 单笔最差MAE {worst_mae}")

            q_info = []
            if common_start is not None:
                for p in range(1, 5):
                    q_trades = row.get(f"Q{p}_Trades", np.nan)
                    if pd.notna(q_trades) and q_trades > 0:
                        q_ret = row.get(f"Q{p}_Ret(%)", np.nan)
                        q_ret_str = f"{q_ret:.4f}%" if pd.notna(q_ret) else "--"

                        q_sum_ret = row.get(f"Q{p}_SumRet(%)", np.nan)
                        q_sum_str = f"{q_sum_ret:.4f}%" if pd.notna(q_sum_ret) else "--"

                        q_info.append(f"Q{p}(笔:{int(q_trades)} 均收益:{q_ret_str} 总收益:{q_sum_str})")
                    else:
                        q_info.append(f"Q{p}(无交易)")
            if q_info:
                print(f"📅 整体季度 -> {' | '.join(q_info)}")

            print("   " + "-" * 105)
            # 分别打印4个象限的详细数据 (包含各自独立的笔数/收益/回撤/季度表现)
            print_quadrant_detail(row, "Long_High", "做多-高成交额(Long_High)")
            print_quadrant_detail(row, "Long_Low", "做多-低成交额(Long_Low)")
            print_quadrant_detail(row, "Short_High", "做空-高成交额(Short_High)")
            print_quadrant_detail(row, "Short_Low", "做空-低成交额(Short_Low)")

            print("-" * 110)

    print("\n[指标说明] MaxLossStreak=逐笔平仓顺序最大连亏；MaxConcurrent=最大同时持仓配堆数。")
    print("MAEValid/MAEMissing=已平仓交易中完整/缺失价格路径的笔数；缺失不填0。")
    print("P10MAE=单笔MAE的10%分位数，并非最差10%样本的均值。")
    print("Run_Complete仅表示全部结算；Risk_Complete还要求全部已平仓交易极值有效。")
    print("空样本的胜率、均收益和MAE显示--；次数与收益率之和为0。")
    print("并行持仓包含未结算仓位；连亏和收益仅使用已平仓样本。")
    print("\n四等分日历时间表现(Q1-Q4)：按平仓时间归属；所有组合使用相同边界；时间均为UTC。")
    if common_start is None:
        print("没有有效观察时间区间。")
    else:
        # 只打印一次时间区间的边界，不每行重复
        all_periods_unique = periods_df.loc[periods_df["Group"].eq("All")].drop_duplicates("Period")
        for part in all_periods_unique.to_dict("records"):
            left = pd.Timestamp(part["Start_UTC"]).strftime("%Y-%m-%d %H:%M")
            right = pd.Timestamp(part["End_UTC"]).strftime("%Y-%m-%d %H:%M")
            bracket = "]" if part["End_Inclusive"] else ")"
            print(f"  Q{part['Period']} 区间边界: [{left} 至 {right}{bracket}")

    print("\n各笔净收益率之和不是账户累计/复利收益；不据此计算年化、夏普或账户最大回撤。")
    print("Long/Short表示ALT方向；High/Low按开仓时Beta窗口日均成交额与截面中位数分组。")
    print("完整明细分组、风险指标、四段时间分析已分别写入3份performance_*.csv。")
    if errors:
        print(f"本次有 {len(errors)} 个目录解析失败，输出未覆盖这些目录。")
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_dir", nargs="?", default="trade_results")
    args = parser.parse_args()
    print_performance_summary(args.base_dir)