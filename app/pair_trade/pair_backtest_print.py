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


if __name__ == "__main__":
    print_performance_summary("trade_results")
