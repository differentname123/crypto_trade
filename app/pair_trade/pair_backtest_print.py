# -*- coding: utf-8 -*-
"""按指定参数提取全局 ALT_ONLY 裸空交易，并穷举 2–5 策略组合。

运行：python pair_backtest_combo.py
依赖：pandas、numpy（与原脚本相同）。
先修改下方 CombinationConfig.BASE_DIR / EXTRACT_DIR。
原 print_performance_summary 函数逐字保留，默认入口改为组合分析。

默认统计约定：
* 只使用 ALT_ONLY 的状态、退出时点、收益和 MAE，不按成交额筛选。
* 组合内每个策略独立计一份；跨策略同币同时间的交易保留，逐笔等额累加。
* C(t)=截至 t 的已平仓净收益率之和；E(t)=1+C(t)，不复利、不除以策略数。
* 同一时点平仓先合并收益，防止人为排列生成虚假回撤。
* 默认 Calmar=(总累计收益率/公共回测年数)/最大累计收益回撤。
  回撤采用初始单位口径（百分点）；也输出相对峰值权益的回撤。
  可将 DRAWDOWN_BASIS 改为 PEAK_EQUITY 切换排名分母。
* 水下时间：从首次低于此前累计收益峰值的平仓时点到恢复；
  未恢复则延长到公共回测结束。空闲且处于峰值时不计水下。
* 曲线仅含平仓已实现收益；不能用单笔 MAE 拼出组合持仓中净值。
* 已存在的提取 CSV 验证通过即复用；不会重读源交易或自动覆盖缓存。
* 无原筛选条件：全部 63 策略参与 C(63,2)..C(63,5)，分别保留前 50。
"""

import os
import json
import pandas as pd
import numpy as np


def print_performance_summary(base_dir="trade_results"):
    """
    遍历指定目录下的所有回测结果，汇总并打印参数组合的表现排行榜。
    按成交额分为高成交额和低成交额两组，不展示整体表现。
    支持根据搜索方向，分别使用多头和空头参数空间进行精准过滤打印。
    """
    # ==========================================
    # 在最开始打印阶段(P1-P4)的时间切分说明
    # ==========================================
    print("=" * 140)
    print("📅 时间线分段 (P1-P4) 逻辑说明:")
    print("   已启用【真实时间轴等分】。系统会自动获取每组回测数据的总时间跨度 (如2021~2024)，")
    print("   并将其绝对均匀地切分为 4 个连续的时间阶段 (P1, P2, P3, P4)。")
    print("   过滤条件要求策略在这 4 个连续的历史阶段中，均收益全部 > 0，确保长期穿越牛熊。")
    print("=" * 140)

    # ==========================================
    # 打印过滤参数空间 (只有符合这些条件的才会被读取和打印)
    # ==========================================
    FILTER_SPACES = {
        "LONG_ALT": {
            "Z": [5.0, 5.5, 6.0, 6.5, 7.0],
            "HOLD": [72, 84, 96, 120],
            "SIG": [18, 24, 30],  # 长期信号窗口 (小时)
            "BETA": [45, 60, 90],  # 长期Beta历史窗口 (天)
            "SHORT_BETA": [5, 7],  # 短期Beta历史窗口 (天)
            "SHORT_SIG": [18, 24, 30],  # 短期确认窗口 (小时)
            "SHORT_EXCESS": [0.0],  # 每小时平均对数残差门槛
            "SHORT_RATIO": [0.0],  # 同方向残差bar的最低比例
            "SHORT_MODE": ["UP"],
            "SHORT_TIMING": ["ROLLING"],
        },
        "SHORT_ALT": {
            "Z": [8.0, 8.5, 9.0, 9.5, 10.0],
            "HOLD": [72, 96, 120],
            "SIG": [24],  # 长期信号窗口 (小时)
            "BETA": [45, 60, 90],  # 长期Beta历史窗口 (天)
            "SHORT_BETA": [7],  # 短期Beta历史窗口 (天)
            "SHORT_SIG": [8, 12, 16, 20],  # 短期确认窗口 (小时)
            "SHORT_EXCESS": [0.0],  # 每小时平均对数残差门槛
            "SHORT_RATIO": [0.0, 0.25, 0.50, 0.75],  # 同方向残差bar的最低比例
            "SHORT_MODE": ["UP", "BOTH"],
            "SHORT_TIMING": ["POST_TRIGGER"],
        },
    }
    FILTER_SHORT_MIN_BARS = [2]  # BTC涨/跌子样本各自最少bar数

    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)

    if not os.path.exists(base_dir):
        fallback_dir = os.path.join("..", base_dir)
        if os.path.exists(fallback_dir):
            base_dir = fallback_dir
        else:
            print(f"⚠️ 目录 {base_dir} 不存在，请检查路径是否正确。")
            print(f"当前运行路径: {os.getcwd()}")
            return

    print(f"正在扫描目录: {os.path.abspath(base_dir)} ...")
    print("已启用多空独立参数空间过滤，跳过不符合条件或未标明搜索方向的文件夹。")
    print("=" * 140)

    REQUIRED_COLS = {
        'status', 'entry_time', 'exit_time', 'net_pnl', 'net_return', 'mae_return', 'direction', 'vol_group',
        'symbol', 'mae_time', 'entry_price', 'exit_price', 'btc_entry', 'btc_exit',
        'btc_entry_price', 'btc_exit_price', 'market_entry_price', 'market_exit_price', 'mae_timestamp',
        'alt_only_status', 'alt_only_exit_time', 'alt_only_exit_price',
        'alt_only_net_pnl', 'alt_only_net_return', 'alt_only_mae_return', 'alt_only_mae_time'
    }

    # --- 辅助统计函数 ---
    def get_detailed_stats(sub_df):
        if sub_df.empty:
            return 0, 0.0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, {p: {'count': 0, 'avg_ret': 0.0, 'tot_ret': 0.0} for p in
                                                           [1, 2, 3, 4]}, None

        trades = len(sub_df)

        net_pnl = sub_df['net_pnl'].values
        net_return = sub_df['net_return'].values

        win_rate = (net_pnl > 0).mean() * 100
        avg_ret = net_return.mean() * 100
        tot_ret = net_return.sum() * 100
        tot_pnl = net_pnl.sum()

        mae_series = sub_df['mae_return'].dropna().values
        avg_mae = mae_series.mean() * 100 if len(mae_series) > 0 else 0.0
        worst_mae = mae_series.min() * 100 if len(mae_series) > 0 else 0.0

        entry_times = sub_df['entry_time'].values
        exit_times = sub_df['exit_time'].values

        times = np.concatenate([entry_times, exit_times])
        changes = np.concatenate(
            [np.ones(len(entry_times), dtype=np.int8), np.full(len(exit_times), -1, dtype=np.int8)])

        sort_idx = np.argsort(times)
        max_conc = int(np.cumsum(changes[sort_idx]).max())

        sorted_pnl = sub_df.sort_values('exit_time')['net_pnl'].values
        max_loss_streak = 0
        curr_streak = 0
        for pnl in sorted_pnl:
            if pnl < 0:
                curr_streak += 1
                if curr_streak > max_loss_streak:
                    max_loss_streak = curr_streak
            else:
                curr_streak = 0

        # 【改动点】按照 4 个阶段提取分布特征
        periods = sub_df['period'].values
        p_stats = {}
        for p in [1, 2, 3, 4]:
            mask = (periods == p)
            p_count = mask.sum()
            if p_count > 0:
                p_ret = net_return[mask]
                p_stats[p] = {
                    'count': int(p_count),
                    'avg_ret': float(p_ret.mean() * 100),
                    'tot_ret': float(p_ret.sum() * 100)
                }
            else:
                p_stats[p] = {'count': 0, 'avg_ret': 0.0, 'tot_ret': 0.0}

        # 获取最差MAE详情
        worst_mae_info = None
        valid_mae = sub_df.dropna(subset=['mae_return'])
        if not valid_mae.empty:
            worst_idx = valid_mae['mae_return'].idxmin()
            worst_row = valid_mae.loc[worst_idx]

            symbol = worst_row.get('symbol', 'N/A')
            net_ret_val = worst_row.get('net_return', 0.0)
            net_ret = net_ret_val * 100 if pd.notna(net_ret_val) else 0.0

            mae_time = worst_row.get('mae_time', worst_row.get('mae_timestamp', 'N/A'))
            if pd.isna(mae_time):
                mae_time = 'N/A'

            alt_entry = worst_row.get('entry_price', 'N/A')
            alt_exit = worst_row.get('exit_price', 'N/A')
            btc_entry = worst_row.get('btc_entry',
                                      worst_row.get('btc_entry_price',
                                                    worst_row.get('market_entry_price', 'N/A')))
            btc_exit = worst_row.get('btc_exit',
                                     worst_row.get('btc_exit_price',
                                                   worst_row.get('market_exit_price', 'N/A')))

            worst_mae_info = {
                'mae_return': worst_mae,
                'symbol': symbol,
                'net_return': net_ret,
                'mae_time': mae_time,
                'alt_entry': alt_entry,
                'alt_exit': alt_exit,
                'btc_entry': btc_entry,
                'btc_exit': btc_exit
            }

        return trades, win_rate, avg_ret, tot_ret, tot_pnl, max_loss_streak, max_conc, avg_mae, worst_mae, p_stats, worst_mae_info

    # --- ALT_ONLY 辅助统计函数：同信号、同持有期，仅开 ALT，不做 BTC 对冲 ---
    def get_alt_only_detailed_stats(sub_df):
        if sub_df.empty:
            return 0, 0.0, 0.0, 0.0, 0.0, 0, 0, 0.0, 0.0, {p: {'count': 0, 'avg_ret': 0.0, 'tot_ret': 0.0} for p in
                                                           [1, 2, 3, 4]}, None

        trades = len(sub_df)

        net_pnl = sub_df['alt_only_net_pnl'].values
        net_return = sub_df['alt_only_net_return'].values

        win_rate = (net_pnl > 0).mean() * 100
        avg_ret = net_return.mean() * 100
        tot_ret = net_return.sum() * 100
        tot_pnl = net_pnl.sum()

        mae_series = sub_df['alt_only_mae_return'].dropna().values
        avg_mae = mae_series.mean() * 100 if len(mae_series) > 0 else 0.0
        worst_mae = mae_series.min() * 100 if len(mae_series) > 0 else 0.0

        entry_times = sub_df['entry_time'].values
        exit_times = sub_df['alt_only_exit_time'].values

        times = np.concatenate([entry_times, exit_times])
        changes = np.concatenate(
            [np.ones(len(entry_times), dtype=np.int8), np.full(len(exit_times), -1, dtype=np.int8)])

        sort_idx = np.argsort(times)
        max_conc = int(np.cumsum(changes[sort_idx]).max())

        sorted_pnl = sub_df.sort_values('alt_only_exit_time')['alt_only_net_pnl'].values
        max_loss_streak = 0
        curr_streak = 0
        for pnl in sorted_pnl:
            if pnl < 0:
                curr_streak += 1
                if curr_streak > max_loss_streak:
                    max_loss_streak = curr_streak
            else:
                curr_streak = 0

        periods = sub_df['period'].values
        p_stats = {}
        for p in [1, 2, 3, 4]:
            mask = (periods == p)
            p_count = mask.sum()
            if p_count > 0:
                p_ret = net_return[mask]
                p_stats[p] = {
                    'count': int(p_count),
                    'avg_ret': float(p_ret.mean() * 100),
                    'tot_ret': float(p_ret.sum() * 100)
                }
            else:
                p_stats[p] = {'count': 0, 'avg_ret': 0.0, 'tot_ret': 0.0}

        worst_mae_info = None
        valid_mae = sub_df.dropna(subset=['alt_only_mae_return'])
        if not valid_mae.empty:
            worst_idx = valid_mae['alt_only_mae_return'].idxmin()
            worst_row = valid_mae.loc[worst_idx]

            symbol = worst_row.get('symbol', 'N/A')
            net_ret_val = worst_row.get('alt_only_net_return', 0.0)
            net_ret = net_ret_val * 100 if pd.notna(net_ret_val) else 0.0

            mae_time = worst_row.get('alt_only_mae_time', 'N/A')
            if pd.isna(mae_time):
                mae_time = 'N/A'

            alt_entry = worst_row.get('entry_price', 'N/A')
            alt_exit = worst_row.get('alt_only_exit_price', 'N/A')

            worst_mae_info = {
                'mae_return': worst_mae,
                'symbol': symbol,
                'net_return': net_ret,
                'mae_time': mae_time,
                'alt_entry': alt_entry,
                'alt_exit': alt_exit,
                'btc_entry': '不持仓',
                'btc_exit': '不持仓'
            }

        return trades, win_rate, avg_ret, tot_ret, tot_pnl, max_loss_streak, max_conc, avg_mae, worst_mae, p_stats, worst_mae_info

    # 专门为 P1-P4 格式化打印的函数
    def format_periods(p_stats):
        parts = []
        for p in [1, 2, 3, 4]:
            d = p_stats[p]
            parts.append(f"P{p}(笔:{d['count']} 均收益:{d['avg_ret']:.4f}%)")
        return " | ".join(parts)

    def format_performance(title, stats_tuple):
        tr, wr, ar, tr_ret, tp, ml, mc, am, wm, ps, worst_info = stats_tuple

        line1 = f"{title}\n    💰 整体汇总 -> 交易次数 {tr} | 胜率 {wr:.2f}% | 均净收益率 {ar:.4f}% ⚠️ 整体风险 -> 最大连亏 {ml}次 | 最大并发 {mc} | 平均单笔MAE {am:.2f}%"

        if worst_info is not None:
            mae_val = worst_info['mae_return']
            sym = worst_info['symbol']
            n_ret = worst_info['net_return']
            m_time = worst_info['mae_time']
            a_en = worst_info['alt_entry']
            a_ex = worst_info['alt_exit']
            b_en = worst_info['btc_entry']
            b_ex = worst_info['btc_exit']

            def fmt_p(val):
                if pd.isna(val) or val == 'N/A':
                    return 'N/A'
                try:
                    v = float(val)
                    s = f"{v:.8f}"
                    s = s.rstrip('0').rstrip('.') if '.' in s else s
                    return s
                except:
                    return str(val)

            line2_a = f"    🚨 最差MAE详情 -> {mae_val:.2f}% | 币种: {sym} | 终局收益率: {n_ret:.2f}% | 触极时间: {m_time}"
            line2_b = f"                     ALT [建仓:{fmt_p(a_en)} 平仓:{fmt_p(a_ex)}] | BTC [建仓:{fmt_p(b_en)} 平仓:{fmt_p(b_ex)}]"
        else:
            line2_a = f"    🚨 最差MAE详情 -> N/A"
            line2_b = f"                     ALT [建仓:N/A 平仓:N/A] | BTC [建仓:N/A 平仓:N/A]"

        line3 = f"    📅 阶段切分 -> {format_periods(ps)}"

        return [line1, line2_a, line2_b, line3]

    total_evaluated = 0
    alt_only_total_evaluated = 0

    low_passed_p_ret_count = 0
    low_passed_mc_count = 0
    low_passed_tr_count = 0
    low_passed_both_count = 0

    high_passed_p_ret_count = 0
    high_passed_mc_count = 0
    high_passed_tr_count = 0
    high_passed_both_count = 0

    alt_only_low_passed_p_ret_count = 0
    alt_only_low_passed_mc_count = 0
    alt_only_low_passed_tr_count = 0
    alt_only_low_passed_both_count = 0

    alt_only_high_passed_p_ret_count = 0
    alt_only_high_passed_mc_count = 0
    alt_only_high_passed_tr_count = 0
    alt_only_high_passed_both_count = 0

    valid_results = []
    param_id = 1

    for root, dirs, files in os.walk(base_dir):
        if "run_manifest.json" in files:
            manifest_path = os.path.join(root, "run_manifest.json")

            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)

                search_direction = manifest.get("trade_direction")
                if search_direction not in FILTER_SPACES:
                    continue
                filters = FILTER_SPACES[search_direction]

                required_short_keys = (
                    "short_beta", "short_signal", "short_excess_threshold",
                    "short_min_bar_ratio", "short_min_regime_bars",
                    "short_confirm_mode", "short_confirm_timing"
                )
                if any(key not in manifest for key in required_short_keys):
                    continue

                z_score = float(manifest.get("z", 0.0))
                hold = manifest.get("holding")
                market_info = manifest.get("market", {})
                beta_days = market_info.get("beta", 0) // 24
                sig_hours = market_info.get("signal", 0)
                short_beta_days = manifest["short_beta"] // 24
                short_sig_hours = manifest["short_signal"]
                short_excess_threshold = float(manifest["short_excess_threshold"])
                short_min_bar_ratio = float(manifest["short_min_bar_ratio"])
                short_min_regime_bars = manifest["short_min_regime_bars"]
                short_confirm_mode = manifest["short_confirm_mode"]
                short_confirm_timing = manifest["short_confirm_timing"]

                if z_score not in filters["Z"]: continue
                if hold not in filters["HOLD"]: continue
                if beta_days not in filters["BETA"]: continue
                if sig_hours not in filters["SIG"]: continue
                if short_beta_days not in filters["SHORT_BETA"]: continue
                if short_sig_hours not in filters["SHORT_SIG"]: continue
                if short_excess_threshold not in filters["SHORT_EXCESS"]: continue
                if short_min_bar_ratio not in filters["SHORT_RATIO"]: continue
                if short_min_regime_bars not in FILTER_SHORT_MIN_BARS: continue
                if short_confirm_mode not in filters["SHORT_MODE"]: continue
                if short_confirm_timing not in filters["SHORT_TIMING"]: continue

                trade_files = [f for f in files if f.endswith("_trades.csv")]
                if not trade_files:
                    continue

                df_list = []
                for f in trade_files:
                    try:
                        tdf = pd.read_csv(os.path.join(root, f), usecols=lambda c: c in REQUIRED_COLS)
                        df_list.append(tdf)
                    except Exception:
                        pass

                if not df_list:
                    continue

                df = pd.concat(df_list, ignore_index=True)

                pair_df = df[(df['status'] == 'CLOSED') &
                             (df['direction'] == search_direction)].copy()

                alt_only_required = {
                    'alt_only_status', 'alt_only_exit_time', 'alt_only_exit_price',
                    'alt_only_net_pnl', 'alt_only_net_return',
                    'alt_only_mae_return', 'alt_only_mae_time'
                }
                has_alt_only = alt_only_required.issubset(df.columns)
                if has_alt_only:
                    alt_only_df = df[(df['alt_only_status'] == 'CLOSED') &
                                     (df['direction'] == search_direction)].copy()
                else:
                    alt_only_df = pd.DataFrame()

                if pair_df.empty and alt_only_df.empty:
                    continue

                if not pair_df.empty:
                    pair_df['entry_time'] = pd.to_datetime(pair_df['entry_time'])
                    pair_df['exit_time'] = pd.to_datetime(pair_df['exit_time'])

                    # ========================================================
                    # 【核心修改区】：不按照日历月份分，而是按真实时间轴完全切分 4 份
                    # pd.cut 根据 entry_time 生成 4 个等长的真实时间区间，labels=False 返回 0,1,2,3
                    # ========================================================
                    pair_df = pair_df.sort_values('entry_time').reset_index(drop=True)
                    pair_df['period'] = pd.cut(pair_df['entry_time'], bins=4, labels=False) + 1

                if not alt_only_df.empty:
                    alt_only_df['entry_time'] = pd.to_datetime(alt_only_df['entry_time'])
                    alt_only_df['alt_only_exit_time'] = pd.to_datetime(alt_only_df['alt_only_exit_time'])
                    alt_only_df = alt_only_df.sort_values('entry_time').reset_index(drop=True)
                    alt_only_df['period'] = pd.cut(alt_only_df['entry_time'], bins=4, labels=False) + 1

                hold_str = str(hold) + "h" if hold is not None else "NA"

                low_df = pair_df[pair_df['vol_group'] == 'Low_Vol'].copy() if not pair_df.empty else pd.DataFrame()
                high_df = pair_df[pair_df['vol_group'] == 'High_Vol'].copy() if not pair_df.empty else pd.DataFrame()

                alt_only_low_df = (alt_only_df[alt_only_df['vol_group'] == 'Low_Vol'].copy()
                                   if not alt_only_df.empty else pd.DataFrame())
                alt_only_high_df = (alt_only_df[alt_only_df['vol_group'] == 'High_Vol'].copy()
                                    if not alt_only_df.empty else pd.DataFrame())

                low_stats = get_detailed_stats(low_df)
                high_stats = get_detailed_stats(high_df)
                alt_only_low_stats = get_alt_only_detailed_stats(alt_only_low_df)
                alt_only_high_stats = get_alt_only_detailed_stats(alt_only_high_df)

                low_tr = low_stats[0]
                low_mc = low_stats[6]
                low_ps = low_stats[9]

                high_tr = high_stats[0]
                high_mc = high_stats[6]
                high_ps = high_stats[9]

                alt_only_low_tr = alt_only_low_stats[0]
                alt_only_low_mc = alt_only_low_stats[6]
                alt_only_low_ps = alt_only_low_stats[9]

                alt_only_high_tr = alt_only_high_stats[0]
                alt_only_high_mc = alt_only_high_stats[6]
                alt_only_high_ps = alt_only_high_stats[9]

                if not pair_df.empty:
                    total_evaluated += 1
                if not alt_only_df.empty:
                    alt_only_total_evaluated += 1

                # 条件1：四个时间阶段 (P1-P4) 的平均收益都必须 > 0
                cond_p_ret_low = all(low_ps[p]['avg_ret'] > 0 for p in [1, 2, 3, 4])
                cond_p_ret_high = all(high_ps[p]['avg_ret'] > 0 for p in [1, 2, 3, 4])

                cond_mc_low = low_mc < 10
                cond_mc_high = high_mc < 10

                cond_tr_low = low_tr > 100
                cond_tr_high = high_tr > 100

                low_pass = cond_p_ret_low and cond_mc_low and cond_tr_low
                high_pass = cond_p_ret_high and cond_mc_high and cond_tr_high

                alt_only_cond_p_ret_low = all(
                    alt_only_low_ps[p]['avg_ret'] > 0 for p in [1, 2, 3, 4])
                alt_only_cond_p_ret_high = all(
                    alt_only_high_ps[p]['avg_ret'] > 0 for p in [1, 2, 3, 4])

                alt_only_cond_mc_low = alt_only_low_mc < 10
                alt_only_cond_mc_high = alt_only_high_mc < 10

                alt_only_cond_tr_low = alt_only_low_tr > 100
                alt_only_cond_tr_high = alt_only_high_tr > 100

                alt_only_low_pass = (
                        alt_only_cond_p_ret_low and alt_only_cond_mc_low and alt_only_cond_tr_low)
                alt_only_high_pass = (
                        alt_only_cond_p_ret_high and alt_only_cond_mc_high and alt_only_cond_tr_high)

                if not pair_df.empty:
                    if cond_p_ret_low: low_passed_p_ret_count += 1
                    if cond_mc_low: low_passed_mc_count += 1
                    if cond_tr_low: low_passed_tr_count += 1
                    if low_pass: low_passed_both_count += 1

                    if cond_p_ret_high: high_passed_p_ret_count += 1
                    if cond_mc_high: high_passed_mc_count += 1
                    if cond_tr_high: high_passed_tr_count += 1
                    if high_pass: high_passed_both_count += 1

                if not alt_only_df.empty:
                    if alt_only_cond_p_ret_low: alt_only_low_passed_p_ret_count += 1
                    if alt_only_cond_mc_low: alt_only_low_passed_mc_count += 1
                    if alt_only_cond_tr_low: alt_only_low_passed_tr_count += 1
                    if alt_only_low_pass: alt_only_low_passed_both_count += 1

                    if alt_only_cond_p_ret_high: alt_only_high_passed_p_ret_count += 1
                    if alt_only_cond_mc_high: alt_only_high_passed_mc_count += 1
                    if alt_only_cond_tr_high: alt_only_high_passed_tr_count += 1
                    if alt_only_high_pass: alt_only_high_passed_both_count += 1

                param_str = f"⚙️ 参数 -> ID: {param_id} | Direction: {search_direction} | Z: {z_score} | Hold: {hold_str} | Beta: {beta_days}d | Sig: {sig_hours}h | S_Beta: {short_beta_days}d | S_Sig: {short_sig_hours}h | S_Ex: {short_excess_threshold} | S_P: {short_min_bar_ratio} | Conf: {short_confirm_mode} | Timing: {short_confirm_timing}"
                direction_text = "做多" if search_direction == "LONG_ALT" else "做空"

                # 原双腿和 ALT_ONLY 独立应用相同过滤条件；任一模式通过都打印该参数。
                if low_pass or high_pass or alt_only_low_pass or alt_only_high_pass:
                    res_lines = [param_str]

                    if not pair_df.empty:
                        overall_stats = get_detailed_stats(pair_df)
                        res_lines.extend(format_performance(
                            f"{direction_text}-整体表现", overall_stats))

                    if not alt_only_df.empty:
                        alt_only_overall_stats = get_alt_only_detailed_stats(alt_only_df)
                        res_lines.extend(format_performance(
                            f"{direction_text}-整体表现【ALT_ONLY不对冲】",
                            alt_only_overall_stats))

                    if low_pass:
                        res_lines.extend(format_performance(
                            f"{direction_text}-低成交额", low_stats))

                    if alt_only_low_pass:
                        res_lines.extend(format_performance(
                            f"{direction_text}-低成交额【ALT_ONLY不对冲】",
                            alt_only_low_stats))

                    if high_pass:
                        res_lines.extend(format_performance(
                            f"{direction_text}-高成交额", high_stats))

                    if alt_only_high_pass:
                        res_lines.extend(format_performance(
                            f"{direction_text}-高成交额【ALT_ONLY不对冲】",
                            alt_only_high_stats))

                    valid_results.append(res_lines)

                param_id += 1

            except Exception as e:
                print(f"解析目录 {root} 时出错: {e}")

    if total_evaluated == 0 and alt_only_total_evaluated == 0:
        print("没有找到符合所选参数空间的交易数据，请检查过滤条件或回测是否已运行。")
    else:
        if total_evaluated > 0:
            print("📊 筛选条件通过率统计（原 ALT+BTC 对冲；Low_Vol / High_Vol 独立）:")
            print(f"总计评估有效参数组合数: {total_evaluated}")

            print("\n【低成交额 Low_Vol】")
            print(
                f"✅ 条件1(每阶段均收益>0) 通过率: {low_passed_p_ret_count / total_evaluated * 100:.2f}% ({low_passed_p_ret_count}/{total_evaluated})")
            print(
                f"✅ 条件2(最大并发<10) 通过率: {low_passed_mc_count / total_evaluated * 100:.2f}% ({low_passed_mc_count}/{total_evaluated})")
            print(
                f"✅ 条件3(交易次数>100) 通过率: {low_passed_tr_count / total_evaluated * 100:.2f}% ({low_passed_tr_count}/{total_evaluated})")
            print(
                f"🎯 综合(该组三条件)最终通过率: {low_passed_both_count / total_evaluated * 100:.2f}% ({low_passed_both_count}/{total_evaluated})")

            print("\n【高成交额 High_Vol】")
            print(
                f"✅ 条件1(每阶段均收益>0) 通过率: {high_passed_p_ret_count / total_evaluated * 100:.2f}% ({high_passed_p_ret_count}/{total_evaluated})")
            print(
                f"✅ 条件2(最大并发<10) 通过率: {high_passed_mc_count / total_evaluated * 100:.2f}% ({high_passed_mc_count}/{total_evaluated})")
            print(
                f"✅ 条件3(交易次数>100) 通过率: {high_passed_tr_count / total_evaluated * 100:.2f}% ({high_passed_tr_count}/{total_evaluated})")
            print(
                f"🎯 综合(该组三条件)最终通过率: {high_passed_both_count / total_evaluated * 100:.2f}% ({high_passed_both_count}/{total_evaluated})")

            print("=" * 140)

        if alt_only_total_evaluated > 0:
            print("📊 ALT_ONLY 筛选条件通过率统计（仅开 ALT，不做 BTC 对冲；Low_Vol / High_Vol 独立）:")
            print(f"总计评估有效参数组合数: {alt_only_total_evaluated}")

            print("\n【低成交额 Low_Vol】")
            print(
                f"✅ 条件1(每阶段均收益>0) 通过率: {alt_only_low_passed_p_ret_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_low_passed_p_ret_count}/{alt_only_total_evaluated})")
            print(
                f"✅ 条件2(最大并发<10) 通过率: {alt_only_low_passed_mc_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_low_passed_mc_count}/{alt_only_total_evaluated})")
            print(
                f"✅ 条件3(交易次数>100) 通过率: {alt_only_low_passed_tr_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_low_passed_tr_count}/{alt_only_total_evaluated})")
            print(
                f"🎯 综合(该组三条件)最终通过率: {alt_only_low_passed_both_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_low_passed_both_count}/{alt_only_total_evaluated})")

            print("\n【高成交额 High_Vol】")
            print(
                f"✅ 条件1(每阶段均收益>0) 通过率: {alt_only_high_passed_p_ret_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_high_passed_p_ret_count}/{alt_only_total_evaluated})")
            print(
                f"✅ 条件2(最大并发<10) 通过率: {alt_only_high_passed_mc_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_high_passed_mc_count}/{alt_only_total_evaluated})")
            print(
                f"✅ 条件3(交易次数>100) 通过率: {alt_only_high_passed_tr_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_high_passed_tr_count}/{alt_only_total_evaluated})")
            print(
                f"🎯 综合(该组三条件)最终通过率: {alt_only_high_passed_both_count / alt_only_total_evaluated * 100:.2f}% ({alt_only_high_passed_both_count}/{alt_only_total_evaluated})")

            print("=" * 140)

        print("\n")

        if not valid_results:
            print("没有任何成交额分组通过最终筛选条件（原对冲或 ALT_ONLY）。")
        else:
            for res_lines in valid_results:
                for line in res_lines:
                    print(line)
                print(
                    "   ---------------------------------------------------------------------------------------------------------")


# ===== 新增：用户指定的完整参数字典（以参数值匹配；ID 仅是标签） =====
complete_alt_only_parameters = {
    # =================================================================================
    # 推荐一：极致高胜率“力竭捕捉”型 (Z=9.0 ~ 9.5，主研究族)
    # =================================================================================
    "ID_2384": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 9.5, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_2386": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 9.5, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_2291": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 9.5, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_2293": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 9.5, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_2105": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 9.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_2107": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 9.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_2109": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 9.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},

    # =================================================================================
    # 推荐二：“黄金平衡”长周期型 (Z=8.5)
    # =================================================================================
    "ID_1640": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1642": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1644": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1671": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1673": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},

    # =================================================================================
    # 推荐三：绝对极值“摸顶”型 (Z=10.0，低频)
    # =================================================================================
    "ID_1109": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 10.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1291": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 10.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1293": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 10.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1295": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 10.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},

    # =================================================================================
    # 推荐四：快进快出防守型 (Z=8.0，短线高频)
    # =================================================================================
    "ID_1415": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1417": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1419": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1423": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1425": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1446": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1448": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1450": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1477": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1479": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1508": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1510": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1512": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1516": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1518": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1570": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1572": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1574": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1578": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1580": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1582": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1601": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1603": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1605": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1609": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1611": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 12, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1636": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "UP", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},

    # =================================================================================
    # 推荐五：BOTH模式深度确认型 (独立逻辑分支，极度抗脆弱)
    # =================================================================================
    "ID_1383": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1385": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1414": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1416": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1445": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1447": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1476": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1478": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1480": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.5,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1538": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1540": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1569": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1571": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1600": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1602": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1631": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1633": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.0, "HOLDING_PERIOD_HOURS": 96,
                "BETA_WINDOW_DAYS": 90, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1662": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.25,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1693": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 120,
                "BETA_WINDOW_DAYS": 60, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"},
    "ID_1755": {"TRADE_DIRECTION": "SHORT_ALT", "Z_SCORE_THRESHOLD": 8.5, "HOLDING_PERIOD_HOURS": 72,
                "BETA_WINDOW_DAYS": 45, "SIGNAL_WINDOW_HOURS": 24, "SHORT_BETA_WINDOW_DAYS": 7,
                "SHORT_SIGNAL_WINDOW_HOURS": 8, "SHORT_EXCESS_THRESHOLD": 0.0, "SHORT_MIN_BAR_RATIO": 0.0,
                "SHORT_CONFIRM_MODE": "BOTH", "SHORT_CONFIRM_TIMING": "POST_TRIGGER"}
}

# ===== 新增功能；以上原统计函数及参数字典保持不变 =====
import hashlib
import heapq
import itertools
import math
import tempfile
import time


class CombinationConfig:
    BASE_DIR = "trade_results"
    EXTRACT_DIR = "extract_results"
    RESULT_SUBDIR = "combination_results"
    COMBINATION_SIZES = (2, 3, 4, 5)
    TOP_N = 50
    SHORT_MIN_REGIME_BARS = 2
    BATCH_SIZE = 256
    WORKING_MEMORY_MB = 256
    PROGRESS_SECONDS = 5
    CHECKPOINT_SECONDS = 60
    RESUME_SEARCH = True
    # INITIAL：固定初始单位的回撤；PEAK_EQUITY：相对当时峰值权益的回撤。
    DRAWDOWN_BASIS = "INITIAL"
    # 沿用原代码只统计已平仓样本；未结算数量另行披露。设 True 可禁止不完整样本排名。
    REQUIRE_COMPLETE_ALT_ONLY = False
    # 同一参数匹配到多个源文件夹时，在这里明确指定，程序不会猜测哪个版本正确。
    SOURCE_DIR_OVERRIDES = {}  # 例如 {"ID_2384": r"W:\...\SHORT_ALT_Z9.5_..."}
    ALLOW_MIXED_SOURCE_CONTEXT = False


EXTRACT_SCHEMA = "global_alt_only_short_v1"
SEARCH_SCHEMA = "additive_exit_curve_calmar_v1"
SECONDS_PER_YEAR = 365.25 * 86400
PARAM_KEYS = (
    "TRADE_DIRECTION", "Z_SCORE_THRESHOLD", "HOLDING_PERIOD_HOURS",
    "BETA_WINDOW_DAYS", "SIGNAL_WINDOW_HOURS", "SHORT_BETA_WINDOW_DAYS",
    "SHORT_SIGNAL_WINDOW_HOURS", "SHORT_EXCESS_THRESHOLD", "SHORT_MIN_BAR_RATIO",
    "SHORT_CONFIRM_MODE", "SHORT_CONFIRM_TIMING",
)
TRADE_OUTPUT_COLUMNS = [
    "strategy_id", "source_trade_id", "source_run_id", "symbol", "direction", "status",
    "entry_time", "exit_time", "entry_price", "exit_price", "net_pnl", "net_return",
    "mae_return", "mae_time", "mfe_return", "mfe_time", "holding_hours", "vol_group",
    "alt_qty", "btc_qty", "btc_entry", "btc_exit", "entry_notional", "exit_notional",
    "entry_cost", "exit_cost", "total_cost", "exit_reason",
]


def _json_digest(value):
    payload = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _file_digest(path):
    value = hashlib.sha256()
    with open(path, "rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def _atomic_write(path, writer):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".writing_", dir=os.path.dirname(os.path.abspath(path)))
    os.close(fd)
    try:
        writer(temporary)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.remove(temporary)


def _write_json(path, value):
    def write(temporary):
        with open(temporary, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)

    _atomic_write(path, write)


def _write_csv(path, frame):
    _atomic_write(path, lambda p: frame.to_csv(p, index=False, encoding="utf-8-sig"))


def _write_text(path, value):
    def write(temporary):
        with open(temporary, "w", encoding="utf-8") as stream:
            stream.write(value)

    _atomic_write(path, write)


def _read_json(path):
    with open(path, "r", encoding="utf-8-sig") as stream:
        return json.load(stream)


def _utc_series(values):
    # 同时兼容带/不带小数秒的 ISO 时间；无法解释的非空值直接报错。
    clean = values.replace({"N/A": np.nan, "NaT": np.nan, "": np.nan})
    parsed = pd.to_datetime(clean, utc=True, errors="coerce")
    retry = clean.notna() & parsed.isna()
    if retry.any():
        parsed.loc[retry] = clean.loc[retry].map(lambda x: pd.to_datetime(x, utc=True, errors="raise"))

    # 【修复核心1】：强制转化为纳秒(ns)精度，确保底层数值提取时全系统单位绝对统一
    return parsed.astype("datetime64[ns, UTC]")


def _parameter_signature(params):
    if set(params) != set(PARAM_KEYS):
        raise ValueError("参数字典字段不完整或含有未知字段")
    values = []
    for key in PARAM_KEYS:
        value = params[key]
        if key in ("TRADE_DIRECTION", "SHORT_CONFIRM_MODE", "SHORT_CONFIRM_TIMING"):
            values.append(str(value))
        else:
            number = float(value)
            if not math.isfinite(number):
                raise ValueError(f"参数 {key} 不是有限数值")
            values.append(round(number, 12))
    return tuple(values) + (CombinationConfig.SHORT_MIN_REGIME_BARS,)


def _manifest_signature(manifest):
    market = manifest["market"]
    params = dict(zip(PARAM_KEYS, (
        manifest["trade_direction"], manifest["z"], manifest["holding"],
        float(market["beta"]) / 24, market["signal"],
        float(manifest["short_beta"]) / 24, manifest["short_signal"],
        manifest["short_excess_threshold"], manifest["short_min_bar_ratio"],
        manifest["short_confirm_mode"], manifest["short_confirm_timing"],
    )))
    return _parameter_signature(params)[:-1] + (manifest["short_min_regime_bars"],)


def _source_context(manifest):
    market = manifest.get("market", {})
    # 长短窗口属于策略参数，不纳入共用数据一致性检查。
    return {
        "data": market.get("data"), "universe": market.get("universe"),
        "btc": market.get("btc"), "end": market.get("end"),
        "version": market.get("version"), "code": market.get("code"), "start": manifest.get("start"),
        "fee": manifest.get("fee"), "gross": manifest.get("gross"),
        "short_basis": manifest.get("short_basis"), "excursion": manifest.get("excursion"),
    }


def _validate_extracted(frame, strategy_id):
    absent = set(TRADE_OUTPUT_COLUMNS) - set(frame.columns)
    if absent:
        raise ValueError(f"{strategy_id}: 提取文件缺少列 {sorted(absent)}")
    frame = frame.copy()
    for col in ("entry_time", "exit_time", "mae_time", "mfe_time"):
        frame[col] = _utc_series(frame[col])
    for col in ("entry_price", "exit_price", "net_pnl", "net_return", "mae_return", "mfe_return",
                "holding_hours", "alt_qty", "btc_qty", "entry_notional", "exit_notional",
                "entry_cost", "exit_cost", "total_cost"):
        frame[col] = pd.to_numeric(frame[col], errors="raise")
    if frame.empty:
        return frame
    if not frame["strategy_id"].eq(strategy_id).all() or not frame["direction"].eq("SHORT_ALT").all():
        raise ValueError(f"{strategy_id}: 缓存混入其它策略或方向")
    if not frame["status"].isin(["CLOSED", "UNRESOLVED_MISSING_EXIT", "UNRESOLVED_END_OF_DATA"]).all():
        raise ValueError(f"{strategy_id}: 存在未知或 OPEN 状态")
    if frame["entry_time"].isna().any() or frame["symbol"].isna().any():
        raise ValueError(f"{strategy_id}: 缺少入场时间或币种")
    if frame["source_trade_id"].isna().any() or frame["source_trade_id"].duplicated().any():
        raise ValueError(f"{strategy_id}: 单个策略内交易 ID 缺失或重复")
    if not frame["btc_qty"].eq(0).all():
        raise ValueError(f"{strategy_id}: 提取记录含 BTC 仓位")
    closed = frame.loc[frame["status"].eq("CLOSED")]
    if closed["exit_time"].isna().any() or (closed["exit_time"] <= closed["entry_time"]).any():
        raise ValueError(f"{strategy_id}: 已平仓交易退出时间无效")
    numbers = closed[["entry_price", "exit_price", "net_return", "net_pnl"]].to_numpy(dtype=float)
    if not np.isfinite(numbers).all() or (closed[["entry_price", "exit_price"]] <= 0).any().any():
        raise ValueError(f"{strategy_id}: 已平仓交易缺少有限收益或正价格")
    mae = frame["mae_return"].dropna().to_numpy(dtype=float)
    if not np.isfinite(mae).all() or (mae > 1e-12).any():
        raise ValueError(f"{strategy_id}: MAE 必须为空或有限非正数")
    return frame.sort_values(["entry_time", "symbol", "source_trade_id"], kind="stable").reset_index(drop=True)


def _normalize_alt_only(raw, strategy_id, source_path):
    required = {"direction", "symbol", "entry_time", "entry_price", "alt_only_status",
                "alt_only_exit_time", "alt_only_exit_price", "alt_only_net_pnl",
                "alt_only_net_return", "alt_only_mae_return", "alt_only_mae_time"}
    if not required.issubset(raw.columns):
        raise ValueError(f"{source_path}: 缺少 ALT_ONLY 列 {sorted(required - set(raw.columns))}")
    if not raw.empty and not raw["direction"].eq("SHORT_ALT").all():
        raise ValueError(f"{source_path}: 匹配到的做空目录混入其它方向")
    result = pd.DataFrame(index=raw.index)
    result["strategy_id"] = strategy_id
    mapping = {
        "source_run_id": "run_id", "symbol": "symbol", "direction": "direction",
        "status": "alt_only_status", "entry_time": "entry_time", "exit_time": "alt_only_exit_time",
        "entry_price": "entry_price", "exit_price": "alt_only_exit_price",
        "net_pnl": "alt_only_net_pnl", "net_return": "alt_only_net_return",
        "mae_return": "alt_only_mae_return", "mae_time": "alt_only_mae_time",
        "mfe_return": "alt_only_mfe_return", "mfe_time": "alt_only_mfe_time",
        "holding_hours": "alt_only_holding_hours", "vol_group": "vol_group",
        "alt_qty": "alt_only_qty", "entry_notional": "alt_only_entry_notional",
        "exit_notional": "alt_only_exit_notional", "entry_cost": "alt_only_entry_cost",
        "exit_cost": "alt_only_exit_cost", "total_cost": "alt_only_total_cost",
        "exit_reason": "alt_only_exit_reason",
    }
    for target, source in mapping.items():
        result[target] = raw[source] if source in raw.columns else np.nan
    if "alt_only_btc_qty" in raw and not raw["alt_only_btc_qty"].fillna(0).eq(0).all():
        raise ValueError(f"{source_path}: ALT_ONLY 的 BTC 数量不是零")
    result["btc_qty"] = 0.0
    result["btc_entry"] = result["btc_exit"] = "不持仓"
    if "trade_id" in raw:
        result["source_trade_id"] = raw["trade_id"]
    else:
        result["source_trade_id"] = raw["symbol"].astype(str) + "|" + raw["entry_time"].astype(str)
    return _validate_extracted(result[TRADE_OUTPUT_COLUMNS], strategy_id)


def _cache_paths(extract_dir, strategy_id):
    return (os.path.join(extract_dir, f"{strategy_id}_alt_only_trades.csv"),
            os.path.join(extract_dir, f"{strategy_id}_extract_manifest.json"))


def _load_cached(extract_dir, strategy_id, params):
    csv_path, meta_path = _cache_paths(extract_dir, strategy_id)
    if not os.path.exists(csv_path):
        if os.path.exists(meta_path):
            raise ValueError(f"{strategy_id}: 缓存仅有元数据而缺少 CSV，请核查 {extract_dir}")
        return None
    if not os.path.isfile(meta_path):
        raise ValueError(f"{csv_path} 已存在但无配套提取元数据；不覆盖，请核实/移走该旧缓存")
    meta = _read_json(meta_path)
    if (meta.get("schema") != EXTRACT_SCHEMA or meta.get("strategy_id") != strategy_id
            or meta.get("params") != params
            or meta.get("short_min_regime_bars") != CombinationConfig.SHORT_MIN_REGIME_BARS):
        raise ValueError(f"{strategy_id}: 已存在缓存的参数或格式不匹配；不会自动覆盖")
    if _file_digest(csv_path) != meta.get("csv_sha256"):
        raise ValueError(f"{csv_path}: 缓存内容与提取校验值不一致")
    frame = _validate_extracted(pd.read_csv(csv_path, float_precision="round_trip"), strategy_id)
    print(f"[复用，跳过提取] {strategy_id}: {len(frame)} 条全局 ALT_ONLY 记录")
    return frame, meta


def extract_selected_strategies(parameters, base_dir, extract_dir):
    os.makedirs(extract_dir, exist_ok=True)
    results, pending = {}, {}
    for strategy_id, params in parameters.items():
        if not strategy_id.startswith("ID_") or not strategy_id[3:].isdigit():
            raise ValueError(f"不合法的策略标签: {strategy_id}")
        if params.get("TRADE_DIRECTION") != "SHORT_ALT":
            raise ValueError(f"{strategy_id}: 本功能只允许 SHORT_ALT")
        signature = _parameter_signature(params)
        cached = _load_cached(extract_dir, strategy_id, params)
        if cached is None:
            pending[strategy_id] = signature
        else:
            results[strategy_id] = cached
    if pending:
        if not os.path.isdir(base_dir):
            fallback = os.path.join("..", base_dir)
            if os.path.isdir(fallback):
                base_dir = fallback
            else:
                raise FileNotFoundError(f"源目录不存在: {os.path.abspath(base_dir)}")
        wanted = set(pending.values())
        candidates = {sig: [] for sig in wanted}
        for root, dirs, files in os.walk(base_dir):
            dirs.sort()
            if "run_manifest.json" not in files:
                continue
            try:
                manifest = _read_json(os.path.join(root, "run_manifest.json"))
                signature = _manifest_signature(manifest)
            except (ValueError, TypeError, KeyError) as exc:
                print(f"[无法识别的源清单] {root}: {exc}")
                continue
            if signature in wanted:
                candidates[signature].append((os.path.abspath(root), manifest))
        chosen, errors = {}, []
        for strategy_id, signature in pending.items():
            matches = candidates[signature]
            override = CombinationConfig.SOURCE_DIR_OVERRIDES.get(strategy_id)
            if override:
                selected_path = os.path.normcase(os.path.abspath(override))
                matches = [item for item in matches if os.path.normcase(item[0]) == selected_path]
            if len(matches) != 1:
                paths = "\n    ".join(item[0] for item in matches)
                errors.append(f"{strategy_id}: 匹配目录数 {len(matches)}；参数 {parameters[strategy_id]}\n    {paths}")
            else:
                chosen[strategy_id] = matches[0]
        if errors:
            raise ValueError("指定策略缺失或存在多个版本。多个版本请设置 SOURCE_DIR_OVERRIDES：\n" + "\n".join(errors))
        for strategy_id, (root, manifest) in chosen.items():
            market = manifest.get("market", {})
            symbols = sorted(set(market.get("universe", [])) - {market.get("btc", "BTCUSDT")})
            files = ([f"{symbol}_trades.csv" for symbol in symbols] if symbols else
                     sorted(name for name in os.listdir(root) if name.endswith("_trades.csv")))
            if not files:
                raise ValueError(f"{root}: 没有交易文件")
            frames = []
            for name in files:
                source_path = os.path.join(root, name)
                raw = pd.read_csv(source_path, float_precision="round_trip")  # 读取失败直接报错。
                if "run_id" in raw and not raw.empty and not raw["run_id"].eq(_json_digest(manifest)).all():
                    raise ValueError(f"{source_path}: 交易 run_id 与该目录参数清单不一致")
                frames.append(_normalize_alt_only(raw, strategy_id, source_path))
            frame = _validate_extracted(pd.concat(frames, ignore_index=True), strategy_id)
            closed = frame.loc[frame["status"].eq("CLOSED")]
            expected_hours = parameters[strategy_id]["HOLDING_PERIOD_HOURS"]
            actual_hours = (closed["exit_time"] - closed["entry_time"]).dt.total_seconds() / 3600
            if not np.allclose(actual_hours.to_numpy(dtype=float), expected_hours, rtol=0, atol=1e-8):
                raise ValueError(f"{strategy_id}: 实际 ALT_ONLY 持有时间与指定参数不符")
            window_path = os.path.join(root, "evaluation_window.json")
            window = _read_json(window_path) if os.path.isfile(window_path) else {}
            csv_path, meta_path = _cache_paths(extract_dir, strategy_id)
            _write_csv(csv_path, frame)
            meta = {
                "schema": EXTRACT_SCHEMA, "strategy_id": strategy_id,
                "params": parameters[strategy_id], "short_min_regime_bars": CombinationConfig.SHORT_MIN_REGIME_BARS,
                "source_dir": root, "source_manifest_sha256": _file_digest(os.path.join(root, "run_manifest.json")),
                "source_context": _source_context(manifest), "evaluation_window": window,
                "row_count": len(frame), "closed_count": len(closed), "unresolved_count": len(frame) - len(closed),
                "csv_sha256": _file_digest(csv_path), "scope": "GLOBAL_ALT_ONLY_SHORT_NO_VOLUME_FILTER",
            }
            _write_json(meta_path, meta)  # 最后发布元数据，标记提取完成。
            results[strategy_id] = (frame, meta)
            print(f"[已提取] {strategy_id}: 已平仓 {len(closed)}，未结算 {len(frame) - len(closed)} -> {csv_path}")
    contexts = {_json_digest(meta["source_context"]) for _, meta in results.values()}
    if len(contexts) > 1 and not CombinationConfig.ALLOW_MIXED_SOURCE_CONTEXT:
        raise ValueError(
            "所选策略来自不同数据/费用/模型版本；请统一源目录与缓存，或确认后显式允许 ALLOW_MIXED_SOURCE_CONTEXT")
    unresolved = {key: int(frame["status"].ne("CLOSED").sum()) for key, (frame, _) in results.items()}
    if any(unresolved.values()):
        print("[样本口径] 下列策略存在未结算记录；收益排名仅含已平仓子集：", {k: v for k, v in unresolved.items() if v})
        if CombinationConfig.REQUIRE_COMPLETE_ALT_ONLY:
            raise ValueError("REQUIRE_COMPLETE_ALT_ONLY=True，停止对不完整样本排名")
    return {key: results[key] for key in parameters}


def _common_window(records):
    starts, ends = [], []
    for frame, meta in records.values():
        window = meta.get("evaluation_window", {})
        for key, dest in (("start", starts), ("end", ends)):
            if window.get(key):
                value = pd.to_datetime(window[key], utc=True, errors="raise")
                if pd.notna(value):
                    dest.append(value)
        if not frame.empty:
            starts.append(frame["entry_time"].min())
            exits = frame["exit_time"].dropna()
            if not exits.empty:
                ends.append(exits.max())
    if not starts or not ends or max(ends) <= min(starts):
        raise ValueError("缺少正长度的公共评估区间，无法年化或计算水下时间")
    # 所有策略用同一个日历区间；尚未满足指标历史要求的策略视为闲置。
    return min(starts), max(ends)


def _build_curve_matrix(closed_frames):
    # 【修复核心2】：废弃危险的 .array.asi8，改用 pd.to_numeric.to_numpy() 提取纳秒级整数
    all_times = [pd.to_numeric(frame["exit_time"]).to_numpy() for frame in closed_frames if not frame.empty]

    if not all_times:
        raise ValueError("全部指定策略均无已平仓交易，无法生成排名")

    event_ns = np.unique(np.concatenate(all_times))
    curves = np.zeros((len(closed_frames), len(event_ns)), dtype=np.float64)

    for index, frame in enumerate(closed_frames):
        if not frame.empty:
            # 【修复】：同样获取平仓时点的纳秒级整数进行事件对齐
            exit_ns = pd.to_numeric(frame["exit_time"]).to_numpy()
            positions = np.searchsorted(event_ns, exit_ns)
            np.add.at(curves[index], positions, frame["net_return"].to_numpy(dtype=float))

    np.cumsum(curves, axis=1, out=curves)
    if not np.isfinite(curves).all():
        raise ValueError("累计收益曲线溢出或包含非有限值")
    return event_ns, curves


def _build_concurrency_matrix(closed_frames):
    times = []
    for frame in closed_frames:
        if not frame.empty:
            times.append(pd.to_numeric(frame["entry_time"]).to_numpy())
            times.append(pd.to_numeric(frame["exit_time"]).to_numpy())
    if not times:
        return np.zeros((len(closed_frames), 0), dtype=np.int32)

    event_ns = np.unique(np.concatenate(times))
    conc_matrix = np.zeros((len(closed_frames), len(event_ns)), dtype=np.int32)

    for index, frame in enumerate(closed_frames):
        if not frame.empty:
            entries = pd.to_numeric(frame["entry_time"]).to_numpy()
            exits = pd.to_numeric(frame["exit_time"]).to_numpy()

            entry_pos = np.searchsorted(event_ns, entries)
            exit_pos = np.searchsorted(event_ns, exits)

            np.add.at(conc_matrix[index], entry_pos, 1)
            np.add.at(conc_matrix[index], exit_pos, -1)

    np.cumsum(conc_matrix, axis=1, out=conc_matrix)
    return conc_matrix


def _score_batch(curves, combinations_array, years, basis):
    combined = np.zeros((len(combinations_array), curves.shape[1]), dtype=np.float64)
    for column in range(combinations_array.shape[1]):
        combined += curves[combinations_array[:, column]]
    annual = combined[:, -1] / years
    peaks = np.maximum.accumulate(combined, axis=1)
    np.maximum(peaks, 0.0, out=peaks)  # 初始 0 收益峰值，包含第一笔就亏损的情况。
    if basis == "INITIAL":
        peaks -= combined
    elif basis == "PEAK_EQUITY":
        combined *= -1
        combined += peaks
        peaks += 1.0
        combined /= peaks
        peaks = combined
    else:
        raise ValueError("DRAWDOWN_BASIS 只能是 INITIAL 或 PEAK_EQUITY")
    drawdowns = peaks.max(axis=1)
    scores = np.zeros(len(combinations_array), dtype=float)
    np.divide(annual, drawdowns, out=scores, where=drawdowns > 0)
    scores[(drawdowns == 0) & (annual > 0)] = np.inf
    scores[(drawdowns == 0) & (annual < 0)] = -np.inf
    return scores, annual, drawdowns


def _longest_underwater(event_ns, curve, evaluation_end):
    peak = 0.0
    underwater_start = None
    best_start, best_end, best_seconds, best_open = None, None, 0.0, False
    # 严格低于峰值；用极小相对容差避免浮点加总误差制造数年水下。
    for timestamp, value in zip(event_ns, curve):
        tolerance = 1e-12 * max(1.0, abs(peak), abs(value))
        if value < peak - tolerance:
            if underwater_start is None:
                underwater_start = int(timestamp)
        else:
            if underwater_start is not None:
                seconds = (int(timestamp) - underwater_start) / 1e9
                if seconds > best_seconds:
                    best_start, best_end = underwater_start, int(timestamp)
                    best_seconds, best_open = seconds, False
                underwater_start = None
            peak = max(peak, value)
    if underwater_start is not None:
        seconds = (evaluation_end.value - underwater_start) / 1e9
        if seconds > best_seconds:
            best_start, best_end = underwater_start, evaluation_end.value
            best_seconds, best_open = seconds, True
    iso = lambda value: pd.Timestamp(value, tz="UTC").isoformat() if value is not None else None
    return {
        "longest_underwater_hours": best_seconds / 3600,
        "underwater_start": iso(best_start), "underwater_end": iso(best_end),
        "longest_underwater_unrecovered": best_open,
        "currently_underwater": underwater_start is not None,
    }


def _max_concurrency(frame):
    if frame.empty:
        return 0

    # 【修复核心3】：废弃危险的 .array.asi8，确保入场和出场时间精准转换为纳秒整数，避免并发冲突误判
    entry_ns = pd.to_numeric(frame["entry_time"]).to_numpy()
    exit_ns = pd.to_numeric(frame["exit_time"]).to_numpy()
    times = np.concatenate([entry_ns, exit_ns])

    changes = np.concatenate([np.ones(len(frame), dtype=np.int64), -np.ones(len(frame), dtype=np.int64)])
    order = np.argsort(times, kind="stable")
    sorted_times, sorted_changes = times[order], changes[order]
    starts = np.r_[0, np.flatnonzero(np.diff(sorted_times)) + 1]

    # 持仓区间 [entry, exit)：同一时刻先平后开，不产生虚假并发。
    balances = np.cumsum(np.add.reduceat(sorted_changes, starts))
    return int(max(0, balances.max()))


def _detailed_combination_stats(frame, strategy_ids, event_ns, curve, start, end, unresolved=0):
    values = frame["net_return"].to_numpy(dtype=float)
    net_pnl = frame["net_pnl"].to_numpy(dtype=float)
    count = len(frame)
    years = (end - start).total_seconds() / SECONDS_PER_YEAR
    peaks = np.maximum(np.maximum.accumulate(curve), 0.0)
    drawdown = peaks - curve
    relative_drawdown = drawdown / (1.0 + peaks)
    total_return = float(curve[-1])
    annual = total_return / years
    max_initial, max_peak = float(drawdown.max()), float(relative_drawdown.max())
    denominator = max_initial if CombinationConfig.DRAWDOWN_BASIS == "INITIAL" else max_peak
    calmar = annual / denominator if denominator > 0 else (math.inf if annual > 0 else 0.0)

    result = {
        "strategies": " + ".join(strategy_ids), "strategy_count": len(strategy_ids),
        "trade_count": count, "unresolved_count": unresolved, "result_complete": unresolved == 0,
        "win_rate": float((net_pnl > 0).mean()) if count else None,
        "avg_net_return": float(values.mean()) if count else None,
        "total_net_return": total_return, "total_net_pnl": float(net_pnl.sum()),
        "evaluation_start": start.isoformat(), "evaluation_end": end.isoformat(),
        "evaluation_years": years, "annualized_simple_return": annual,
        "max_drawdown_initial": max_initial, "max_drawdown_peak_equity": max_peak,
        "drawdown_basis": CombinationConfig.DRAWDOWN_BASIS, "calmar": calmar,
        "max_concurrency": _max_concurrency(frame),
        "minimum_additive_equity": float(min(1.0, 1.0 + curve.min())),
    }

    result.update(_longest_underwater(event_ns, curve, end))

    ordered = frame.sort_values(["exit_time", "entry_time", "symbol", "strategy_id", "source_trade_id"], kind="stable")
    streak = longest_streak = 0
    for pnl in ordered["net_pnl"].to_numpy(dtype=float):
        streak = streak + 1 if pnl < 0 else 0
        longest_streak = max(longest_streak, streak)
    result["max_loss_streak"] = longest_streak

    mae_valid = frame.loc[frame["mae_return"].notna()]
    result["mae_valid_count"] = len(mae_valid)
    result["avg_mae_return"] = float(mae_valid["mae_return"].mean()) if len(mae_valid) else None
    result["worst_mae_return"] = float(mae_valid["mae_return"].min()) if len(mae_valid) else None
    result["worst_mae_info"] = None

    if len(mae_valid):
        worst = \
            mae_valid.sort_values(["mae_return", "entry_time", "strategy_id", "source_trade_id"], kind="stable").iloc[0]
        result["worst_mae_info"] = {
            "strategy_id": worst["strategy_id"], "symbol": worst["symbol"],
            "net_return": float(worst["net_return"]), "mae_return": float(worst["mae_return"]),
            "mae_time": str(worst["mae_time"]) if pd.notna(worst["mae_time"]) else "N/A",
            "alt_entry": float(worst["entry_price"]), "alt_exit": float(worst["exit_price"]),
            "btc_entry": "不持仓", "btc_exit": "不持仓",
        }

    # 组合与单策略均用公共评估区间等分，不因参数不同而移动 P1-P4 边界。
    span = end.value - start.value
    boundaries = np.array([start.value + span * p // 4 for p in (1, 2, 3)], dtype=np.int64)

    # 【修复核心4】：完全废除 .array.asi8，通过 pd.to_numeric 取绝对时间戳 (纳秒整数) 确保匹配 P1-P4 边界
    entry_ns = pd.to_numeric(frame["entry_time"]).to_numpy()
    period_index = np.searchsorted(boundaries, entry_ns, side="right") + 1

    periods = {}
    for p in (1, 2, 3, 4):
        selected = values[period_index == p]
        periods[p] = {"count": len(selected), "avg_ret": float(selected.mean()) if len(selected) else None,
                      "tot_ret": float(selected.sum())}
        result[f"p{p}_count"] = len(selected)
        result[f"p{p}_avg_net_return"] = periods[p]["avg_ret"]
        result[f"p{p}_total_net_return"] = periods[p]["tot_ret"]

    result["periods"] = periods
    return result


def _pct(value, decimals=2):
    return "N/A" if value is None or pd.isna(value) else f"{value * 100:.{decimals}f}%"


def _format_combination(stats, title):
    score = stats["calmar"]
    score_text = f"{score:.6f}" if math.isfinite(score) else "+∞（期间无平仓回撤）"
    basis_text = "初始单位" if stats["drawdown_basis"] == "INITIAL" else "峰值权益"
    lines = [title,
             f"    💰 整体汇总 -> 交易次数 {stats['trade_count']} | 胜率 {_pct(stats['win_rate'])} | 均净收益率 {_pct(stats['avg_net_return'], 4)} "
             f"⚠️ 整体风险 -> 最大连亏 {stats['max_loss_streak']}次 | 最大并发 {stats['max_concurrency']} | 平均单笔MAE {_pct(stats['avg_mae_return'])}",
             f"    📈 平仓加总曲线 -> 累计收益 {_pct(stats['total_net_return'], 4)} | 非复利年化 {_pct(stats['annualized_simple_return'], 4)} | "
             f"最大回撤(初始单位) {_pct(stats['max_drawdown_initial'])} | 最大回撤(峰值权益) {_pct(stats['max_drawdown_peak_equity'])} | 非复利Calmar({basis_text}) {score_text}",
             f"    ⏳ 最长水下时间 -> {stats['longest_underwater_hours'] / 24:.2f}天 ({stats['longest_underwater_hours']:.2f}小时) | "
             f"开始 {stats['underwater_start'] or 'N/A'} | 结束 {stats['underwater_end'] or 'N/A'} | "
             f"{'该最长区间到期末仍未恢复' if stats['longest_underwater_unrecovered'] else '该最长区间已恢复'}",
             ]

    periods = " | ".join(f"P{p}(笔:{d['count']} 均收益:{_pct(d['avg_ret'], 4)})" for p, d in stats["periods"].items())
    lines += [f"    📅 阶段切分 -> {periods}",
              f"    🔎 完整性 -> 已平仓MAE有效 {stats['mae_valid_count']}/{stats['trade_count']} | 未结算 {stats['unresolved_count']} | "
              "跨策略重复交易保留；同一时点平仓收益先合并"]
    return "\n".join(lines)


def _flat_stats(stats):
    result = {key: value for key, value in stats.items() if key not in ("periods", "worst_mae_info")}
    if stats["worst_mae_info"]:
        result.update({"worst_trade_" + key: value for key, value in stats["worst_mae_info"].items()})
    return result


def _equity_frame(event_ns, curve, start, end):
    timestamps = np.r_[start.value, event_ns, end.value]
    values = np.r_[0.0, curve, curve[-1]]
    result = pd.DataFrame({"time": pd.to_datetime(timestamps, utc=True), "cumulative_return": values})
    result = result.drop_duplicates("time", keep="last").sort_values("time").reset_index(drop=True)
    result["realized_return_change"] = result["cumulative_return"].diff().fillna(result["cumulative_return"].iloc[0])
    result["additive_equity"] = 1.0 + result["cumulative_return"]
    result["running_peak_return"] = result["cumulative_return"].cummax().clip(lower=0)
    result["drawdown_initial"] = result["running_peak_return"] - result["cumulative_return"]
    result["drawdown_peak_equity"] = result["drawdown_initial"] / (1.0 + result["running_peak_return"])
    return result


def _ranking_key(score, annual, drawdown, combo):
    # 分数相同：年化收益高者优先，再比较回撤，最后按参数字典的稳定次序。
    return float(score), float(annual), -float(drawdown), tuple(-int(x) for x in combo)


def _search_top(curves, conc_matrix, size, years, run_dir, search_id):
    count = len(curves)
    total = math.comb(count, size)
    top_n = min(CombinationConfig.TOP_N, total)
    memory_budget = CombinationConfig.WORKING_MEMORY_MB * 1024 * 1024
    # combined / peaks / advanced-index gather / conc matrix 取用等
    bytes_per_combo = curves.shape[1] * 8 * 4 + conc_matrix.shape[1] * 4
    batch_size = max(1, min(CombinationConfig.BATCH_SIZE,
                            int(memory_budget // max(1, bytes_per_combo))))
    checkpoint_path = os.path.join(run_dir, f"checkpoint_k{size}.json")
    heap, processed = [], 0
    if CombinationConfig.RESUME_SEARCH and os.path.isfile(checkpoint_path):
        checkpoint = _read_json(checkpoint_path)
        if checkpoint.get("search_id") != search_id or checkpoint.get("size") != size:
            raise ValueError(f"{checkpoint_path}: 续跑参数不匹配")
        processed = checkpoint["processed"]
        if not isinstance(processed, int) or not 0 <= processed <= total:
            raise ValueError(f"{checkpoint_path}: processed 无效")
        saved = checkpoint.get("top_combinations", [])
        if len(saved) > top_n or len(saved) > processed:
            raise ValueError(f"{checkpoint_path}: 已保存的前 N 名数量无效")
        if len({tuple(c) for c in saved}) != len(saved):
            raise ValueError(f"{checkpoint_path}: 重复的组合")
        for combo in saved:
            if len(combo) != size or sorted(set(combo)) != combo or min(combo) < 0 or max(combo) >= count:
                raise ValueError(f"{checkpoint_path}: 组合成员无效")
        if saved:
            scores, annual, drawdowns = _score_batch(curves, np.asarray(saved), years, CombinationConfig.DRAWDOWN_BASIS)
            for combo, score, ret, dd in zip(saved, scores, annual, drawdowns):
                combo = tuple(combo)
                heapq.heappush(heap, (_ranking_key(score, ret, dd, combo), combo))
        print(f"[续跑] {size} 策略组合：已完成 {processed:,}/{total:,}")

    def checkpoint_now():
        _write_json(checkpoint_path, {
            "search_id": search_id, "size": size, "processed": processed, "total": total,
            "top_combinations": [list(item[1]) for item in sorted(heap, reverse=True)],
        })

    began = last_progress = last_checkpoint = time.monotonic()
    initial_processed = processed
    print(f"[开始] {size} 策略组合共 {total:,} 个；批大小 {batch_size}；每组保留前 {top_n}")
    # islice 只跳过组合索引，不重新计算已经完成的收益/回撤。
    iterator = itertools.islice(itertools.combinations(range(count), size), processed, None)
    try:
        while processed < total:
            batch = list(itertools.islice(iterator, batch_size))
            if not batch:
                raise RuntimeError("组合迭代提前结束")
            indices = np.asarray(batch, dtype=np.int64)

            # 最大并发过滤 (结合批内所有选择策略的并发矩阵计算)
            batch_conc = np.zeros((len(indices), conc_matrix.shape[1]), dtype=np.int32)
            for column in range(indices.shape[1]):
                batch_conc += conc_matrix[indices[:, column]]
            max_conc = batch_conc.max(axis=1)

            valid_mask = max_conc <= 10
            valid_indices = np.where(valid_mask)[0]

            if len(valid_indices) > 0:
                valid_combos = indices[valid_indices]
                scores, annual, drawdowns = _score_batch(curves, valid_combos, years, CombinationConfig.DRAWDOWN_BASIS)

                n_valid = len(valid_combos)
                take_n = min(top_n, n_valid)
                # 每个有效批次只有其前 take_n 有机会进入全局前 top_n；稳定处理并列。
                order = np.lexsort((np.arange(n_valid), drawdowns, -annual, -scores))[:take_n]
                for index in order:
                    combo = batch[valid_indices[index]]
                    item = (_ranking_key(scores[index], annual[index], drawdowns[index], combo), combo)
                    if len(heap) < top_n:
                        heapq.heappush(heap, item)
                    elif item[0] > heap[0][0]:
                        heapq.heapreplace(heap, item)

            processed += len(batch)
            now = time.monotonic()
            if now - last_progress >= CombinationConfig.PROGRESS_SECONDS or processed == total:
                speed = (processed - initial_processed) / max(now - began, 1e-9)
                remaining = (total - processed) / max(speed, 1e-9)
                print(f"  k={size}: {processed:,}/{total:,} ({processed / total:.2%}) | "
                      f"{speed:,.0f}组/秒 | 预计剩余 {remaining / 60:.1f}分钟", flush=True)
                last_progress = now
            if now - last_checkpoint >= CombinationConfig.CHECKPOINT_SECONDS:
                checkpoint_now()
                last_checkpoint = now
    except KeyboardInterrupt:
        # 中断可能发生于更新 heap 中途：只保存上一个完整批次的检查点。
        print("\n已中断；下次从最近已写入的完整检查点续跑。")
        raise
    checkpoint_now()
    return sorted(heap, reverse=True), total


def _write_top_results(size, winners, strategy_ids, closed_frames, unresolved_counts,
                       event_ns, curves, start, end, run_dir):
    detail_dir = os.path.join(run_dir, f"top{CombinationConfig.TOP_N}_k{size}_details")
    os.makedirs(detail_dir, exist_ok=True)
    summaries, blocks = [], []
    for rank, (ranking_key, indices) in enumerate(winners, 1):
        ids = [strategy_ids[i] for i in indices]
        frame = pd.concat([closed_frames[i] for i in indices], ignore_index=True)
        frame = frame.sort_values(["exit_time", "entry_time", "symbol", "strategy_id", "source_trade_id"],
                                  kind="stable").reset_index(drop=True)
        curve = np.zeros(curves.shape[1], dtype=float)
        for i in indices:
            curve += curves[i]
        stats = _detailed_combination_stats(frame, ids, event_ns, curve, start, end,
                                            sum(unresolved_counts[i] for i in indices))
        if not np.isclose(stats["calmar"], ranking_key[0], rtol=1e-10, atol=1e-10):
            raise RuntimeError("排名与详细统计的 Calmar 口径不一致")
        stats["rank"] = rank
        stats["combination_size"] = size
        prefix = f"rank_{rank:02d}"
        trade_path = os.path.join(detail_dir, prefix + "_trades.csv")
        equity_path = os.path.join(detail_dir, prefix + "_equity.csv")
        _write_csv(trade_path, frame)
        _write_csv(equity_path, _equity_frame(event_ns, curve, start, end))
        flat = _flat_stats(stats)
        flat["trades_file"] = os.path.relpath(trade_path, run_dir)
        flat["equity_file"] = os.path.relpath(equity_path, run_dir)
        summaries.append(flat)
        block = _format_combination(stats, f"🏆 {size}策略组合 第{rank}名 -> {' + '.join(ids)}【全局 ALT_ONLY 裸空】")
        blocks.append(block)
        print(block)
        print("-" * 140)
    _write_csv(os.path.join(run_dir, f"top{CombinationConfig.TOP_N}_k{size}.csv"), pd.DataFrame(summaries))
    _write_text(os.path.join(run_dir, f"top{CombinationConfig.TOP_N}_k{size}.txt"), "\n\n".join(blocks) + "\n")


def run_alt_only_combination_analysis(base_dir=None, extract_dir=None, parameters=None):
    parameters = complete_alt_only_parameters if parameters is None else parameters
    base_dir = CombinationConfig.BASE_DIR if base_dir is None else base_dir
    extract_dir = os.path.abspath(CombinationConfig.EXTRACT_DIR if extract_dir is None else extract_dir)
    if len(parameters) < 2:
        raise ValueError("至少需要两个指定策略")
    if CombinationConfig.DRAWDOWN_BASIS not in ("INITIAL", "PEAK_EQUITY"):
        raise ValueError("DRAWDOWN_BASIS 必须为 INITIAL 或 PEAK_EQUITY")
    for key in ("TOP_N", "BATCH_SIZE", "WORKING_MEMORY_MB"):
        value = getattr(CombinationConfig, key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"{key} 必须为正整数")
    sizes = tuple(CombinationConfig.COMBINATION_SIZES)
    if len(set(sizes)) != len(sizes) or any(not isinstance(k, int) or k < 2 or k > len(parameters) for k in sizes):
        raise ValueError("组合数必须唯一，且介于 2 和策略总数之间")
    counts = {k: math.comb(len(parameters), k) for k in sizes}
    print("=" * 140)
    print(f"严格按参数字典匹配 {len(parameters)} 个策略，全部采用全局 ALT_ONLY 裸空。")
    print(f"穷举计划：{counts}；总计 {sum(counts.values()):,} 个组合")
    print("每笔净收益率在裸空平仓时点累加；不复利、不除以组合策略数、不合并重复信号。")
    print("只计算已实现平仓收益曲线，持仓期间浮亏仍以单笔 MAE 单列。")
    print("=" * 140)
    records = extract_selected_strategies(parameters, base_dir, extract_dir)
    strategy_ids = list(records)
    start, end = _common_window(records)
    closed_frames = [frame.loc[frame["status"].eq("CLOSED")].copy() for frame, _ in records.values()]
    unresolved_counts = [int(frame["status"].ne("CLOSED").sum()) for frame, _ in records.values()]

    event_ns, curves = _build_curve_matrix(closed_frames)
    conc_matrix = _build_concurrency_matrix(closed_frames)

    years = (end - start).total_seconds() / SECONDS_PER_YEAR
    search_settings = {
        "schema": SEARCH_SCHEMA, "implementation_sha256": _file_digest(os.path.abspath(__file__)),
        "strategy_ids": strategy_ids,
        "extract_hashes": [meta["csv_sha256"] for _, meta in records.values()],
        "start": start.isoformat(), "end": end.isoformat(), "year_days": 365.25,
        "sizes": list(sizes), "top_n": CombinationConfig.TOP_N,
        "drawdown_basis": CombinationConfig.DRAWDOWN_BASIS, "trade_policy": "STACK_EVERY_STRATEGY_TRADE",
        "equity_basis": "EXIT_TIME_ADDITIVE_NET_RETURN", "period_basis": "COMMON_CALENDAR_QUARTERS",
        "underwater_basis": "FIRST_BELOW_PEAK_EVENT_TO_RECOVERY_OR_EVALUATION_END",
        "source_contexts": [meta["source_context"] for _, meta in records.values()],
    }
    search_id = _json_digest(search_settings)
    run_dir = os.path.join(extract_dir, CombinationConfig.RESULT_SUBDIR, "search_" + search_id[:16])
    os.makedirs(run_dir, exist_ok=True)
    _write_json(os.path.join(run_dir, "analysis_manifest.json"), dict(search_settings, search_id=search_id,
                                                                      combination_counts={str(k): v for k, v in
                                                                                          counts.items()},
                                                                      unresolved_counts=dict(
                                                                          zip(strategy_ids, unresolved_counts))))
    print(f"公共评估区间：{start} 至 {end}，{years:.4f}年；不同平仓时点 {len(event_ns):,} 个")
    print(f"结果目录：{run_dir}")

    # 记录完全相同的交易样本；仍保留用户要求的所有 ID 和所有组合。
    equivalence, signatures = [], {}
    equivalence_columns = ["symbol", "entry_time", "exit_time", "net_return", "net_pnl", "mae_return", "mae_time"]
    single_summaries, single_blocks = [], []
    for index, (strategy_id, frame) in enumerate(zip(strategy_ids, closed_frames)):
        ordered = frame.sort_values(["entry_time", "symbol", "exit_time"], kind="stable")
        digest = hashlib.sha256(
            pd.util.hash_pandas_object(ordered[equivalence_columns], index=False).to_numpy().tobytes()).hexdigest()
        representative = signatures.setdefault(digest, strategy_id)
        equivalence.append({"strategy_id": strategy_id, "representative": representative, "closed_trade_hash": digest,
                            "closed_count": len(frame), "unresolved_count": unresolved_counts[index]})
        stats = _detailed_combination_stats(frame, [strategy_id], event_ns, curves[index], start, end,
                                            unresolved_counts[index])
        single_summaries.append(_flat_stats(stats))
        single_blocks.append(_format_combination(stats, f"{strategy_id}【全局 ALT_ONLY 裸空】"))
    _write_csv(os.path.join(run_dir, "strategy_equivalence.csv"), pd.DataFrame(equivalence))
    _write_csv(os.path.join(run_dir, "single_strategy_summary.csv"), pd.DataFrame(single_summaries))
    _write_text(os.path.join(run_dir, "single_strategy_summary.txt"), "\n\n".join(single_blocks) + "\n")
    print(f"已平仓交易样本共 {len(signatures)} 种；所有 {len(strategy_ids)} 个 ID 均保留，不据此去重。")
    notes = (
        "所有收益率 CSV 列以小数保存：0.1=10%。TXT/终端以百分比打印。\n"
        "仅 ALT_ONLY 已平仓记录参与统计；提取 CSV 保留未结算记录，数量在每组报表披露。\n"
        "相同策略信号跨 ID 保留为独立仓位，交易数与并发均按独立仓位计数。\n"
        "各策略每笔收益直接相加，不除以策略数，不复利；这不是共享资金实盘账户收益。\n"
        "所有组合使用同一个评估起止日期和 P1-P4 边界。窗口优先纳入源 evaluation_window.json，缺少时使用交易覆盖范围。\n"
        "同一时刻的平仓收益合并后计算回撤；并发区间为 [entry_time, exit_time)。\n"
        "最大连亏按平仓时间、入场时间、币种、策略ID、源交易ID稳定排序；零收益打断连亏。\n"
        "INITIAL: C(t)=sum(r_i)，最大回撤=max(max(0,C(s),s<=t)-C(t))。\n"
        "PEAK_EQUITY: E(t)=1+C(t)，最大回撤=max((peak(E)-E)/peak(E))。\n"
        "非复利 Calmar=(sum(r_i)/评估年数)/所选口径最大回撤，年=365.25天；正收益且零回撤记+inf。\n"
        "默认 INITIAL 保持累计收益与回撤都使用初始单位；设置 PEAK_EQUITY 可改排名分母，CSV始终列出两种回撤。\n"
        "最长水下从首次低于前高的平仓事件开始，至恢复或评估结束；此前处于前高的空闲时间不计入。\n"
        "不使用单笔MAE构造组合回撤：无法从单个极值还原同步持仓净值。\n"
        "原分组过滤（阶段全正、并发<10、交易>100）不应用于此处指定策略或组合。\n"
        "每种组合数独立排名，不跨2/3/4/5混排。相同分数按年化、回撤及参数字典顺序确定排名。\n"
        "已存在提取文件验证后跳过提取，不自动检查源数据的新版本；需要更新时请使用新的 EXTRACT_DIR。\n"
        "checkpoint_k*.json 保存每个完整批次的进度；输入/口径变化会使用不同 search_* 目录。\n"
        "原 print_performance_summary 函数保留，可手动调用；默认运行新的组合分析。\n"
    )
    _write_text(os.path.join(run_dir, "统计口径.txt"), notes)
    for size in sizes:
        winners, evaluated = _search_top(curves, conc_matrix, size, years, run_dir, search_id)
        if evaluated != counts[size]:
            raise RuntimeError("组合数与计划不符")
        _write_top_results(size, winners, strategy_ids, closed_frames, unresolved_counts,
                           event_ns, curves, start, end, run_dir)
    print(f"\n完成全部 {sum(counts.values()):,} 个组合。各组前 {CombinationConfig.TOP_N} 名及交易/曲线已保存：{run_dir}")
    return run_dir


if __name__ == "__main__":
    run_alt_only_combination_analysis()