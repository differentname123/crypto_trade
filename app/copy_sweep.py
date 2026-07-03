import os
import re
import json
import time
import random
import statistics
from urllib.parse import urlparse
from datetime import datetime, timedelta
from collections import defaultdict

import requests
from filelock import FileLock


# =====================================================================
# 模块 1：基础工具箱 (文件读写、时间转换、链接解析)
# =====================================================================

def read_json(json_path):
    if not os.path.exists(json_path):
        return {}
    with open(json_path, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"无法解析 JSON 文件 '{json_path}': {e}")


def save_json(json_path, data):
    dir_path = os.path.dirname(json_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    lock_path = json_path + ".lock"
    with FileLock(lock_path):
        tmp_path = json_path + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4, default=str)
        os.replace(tmp_path, json_path)


def ms_to_time_str(ms):
    return datetime.fromtimestamp(ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")


def time_str_to_ms(time_str):
    return int(datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)


def extract_lead_id(url: str) -> str:
    if not url or not isinstance(url, str):
        return None
    url = url.strip()
    # 优先匹配带单者详细页的 ID
    match = re.search(r'lead-details/(\d+)', url)
    if match: return match.group(1)

    # 兜底匹配：提取 URL 路径中连续 15 位以上的数字
    try:
        fallback_match = re.search(r'/(\d{15,})', urlparse(url).path)
        if fallback_match: return fallback_match.group(1)
    except Exception:
        pass
    return None


def _generate_order_uid(order):
    """根据订单特征生成唯一主键，用于数据去重"""
    return f"{order.get('symbol')}_{order.get('positionSide')}_{order.get('side')}_{order.get('executedQty')}_{order.get('orderUpdateTime')}"


# =====================================================================
# 模块 2：交易核心基础逻辑 (开平仓判断、碎单聚合、轮次切片)
# =====================================================================

def _is_open(trade):
    """判断当前交易是否为开仓（增加风险暴露）"""
    if trade.get('positionSide') == 'BOTH':
        if trade.get('totalPnl', 0.0) != 0.0: return False
        return trade['side'] in ['BUY', 'SELL']
    return (trade['positionSide'] == 'LONG' and trade['side'] == 'BUY') or \
        (trade['positionSide'] == 'SHORT' and trade['side'] == 'SELL')


def _is_close(trade):
    """判断当前交易是否为平仓（释放风险暴露）"""
    if trade.get('positionSide') == 'BOTH':
        if trade.get('totalPnl', 0.0) != 0.0: return True
        return False
    return (trade['positionSide'] == 'LONG' and trade['side'] == 'SELL') or \
        (trade['positionSide'] == 'SHORT' and trade['side'] == 'BUY')


def _aggregate_trades(trades, time_window_ms=1000):
    """【碎单聚合器】将极短时间内被引擎拆分的市价单合并为一笔逻辑单"""
    sorted_trades = sorted(trades, key=lambda x: x['orderUpdateTime'])
    if not sorted_trades: return []

    aggregated = []
    for t in sorted_trades:
        if not aggregated:
            aggregated.append(t.copy())
            continue

        last = aggregated[-1]
        if (t['symbol'] == last['symbol'] and t['side'] == last['side'] and
                t['positionSide'] == last['positionSide'] and
                (t['orderUpdateTime'] - last['orderUpdateTime']) <= time_window_ms):

            total_qty = last['executedQty'] + t['executedQty']
            if total_qty > 0:
                last['avgPrice'] = (last['avgPrice'] * last['executedQty'] + t['avgPrice'] * t[
                    'executedQty']) / total_qty
            last['executedQty'] = total_qty
            last['totalPnl'] += t.get('totalPnl', 0.0)
            last['orderUpdateTime'] = t['orderUpdateTime']
        else:
            aggregated.append(t.copy())
    return aggregated


def group_trades_by_rounds(all_data):
    """将订单数据聚合并切分为完整的开平仓闭环轮次"""
    sorted_data = sorted(all_data, key=lambda x: x['orderTime'])
    result, ongoing_positions = [], {}

    for trade in sorted_data:
        state_key = (trade['symbol'], trade['positionSide'])
        if state_key not in ongoing_positions:
            ongoing_positions[state_key] = {'qty': 0.0, 'trades': []}

        qty = trade['executedQty']
        is_add = (trade['positionSide'] == 'LONG' and trade['side'] == 'BUY') or \
                 (trade['positionSide'] == 'SHORT' and trade['side'] == 'SELL')

        ongoing_positions[state_key]['qty'] += qty if is_add else -qty
        ongoing_positions[state_key]['trades'].append(trade)

        if abs(ongoing_positions[state_key]['qty']) < 1e-8:
            result.append(ongoing_positions[state_key]['trades'])
            ongoing_positions[state_key] = {'qty': 0.0, 'trades': []}

    for state_data in ongoing_positions.values():
        if state_data['trades']: result.append(state_data['trades'])

    result.sort(key=lambda round_trades: round_trades[-1]['orderTime'], reverse=True)
    return result


# =====================================================================
# 模块 3：量化风控核心指标算法 (马丁、尾部风险、滑点、死扛)
# =====================================================================

def _evaluate_sequence(seq, volume_multiplier):
    """评估序列，命中时额外返回该序列中所有触发马丁格尔节点的实际加仓倍数列表"""
    first_trade = seq[0]
    total_qty = sum(trade['executedQty'] for trade in seq)

    if (total_qty / len(seq)) <= first_trade['executedQty']:
        return False, None, []

    is_martingale, seq_multipliers = False, []
    for i in range(1, len(seq)):
        curr, prev = seq[i], seq[i - 1]
        is_worse_price = (first_trade['positionSide'] == 'LONG' and curr['avgPrice'] < first_trade['avgPrice']) or \
                         (first_trade['positionSide'] == 'SHORT' and curr['avgPrice'] > first_trade['avgPrice'])
        is_heavy_add = curr['executedQty'] >= (prev['executedQty'] * volume_multiplier)

        if is_worse_price and is_heavy_add:
            is_martingale = True
            seq_multipliers.append(curr['executedQty'] / prev['executedQty'])

    if is_martingale: return True, seq, seq_multipliers
    return False, None, []


def calculate_martingale_rate_simplified(trades, volume_multiplier=1.5):
    """指标 1：极简版马丁格尔识别"""
    clean_trades = _aggregate_trades(trades)
    groups = defaultdict(list)
    for t in clean_trades:
        groups[(t['symbol'], t['positionSide'])].append(t)

    total_add_seqs, marting_seqs, evidence_list, all_multipliers = 0, 0, [], []

    for group_trades in groups.values():
        group_trades.sort(key=lambda x: x['orderUpdateTime'])
        current_seq = []
        for trade in group_trades:
            if _is_open(trade):
                current_seq.append(trade)
            elif len(current_seq) >= 2:
                total_add_seqs += 1
                is_marting, evidence, seq_mults = _evaluate_sequence(current_seq, volume_multiplier)
                if is_marting:
                    marting_seqs += 1
                    evidence_list.append(evidence)
                    all_multipliers.extend(seq_mults)
                current_seq = []
            else:
                current_seq = []

        if len(current_seq) >= 2:
            total_add_seqs += 1
            is_marting, evidence, seq_mults = _evaluate_sequence(current_seq, volume_multiplier)
            if is_marting:
                marting_seqs += 1
                evidence_list.append(evidence)
                all_multipliers.extend(seq_mults)

    evidence_list.sort(key=len, reverse=True)
    rate = (marting_seqs / total_add_seqs * 100) if total_add_seqs > 0 else 0.0

    return {
        "summary": {
            "total_add_sequences": total_add_seqs,
            "martingale_sequences": marting_seqs,
            "martingale_rate_percent": round(rate, 2),
            "min_actual_multiplier": round(min(all_multipliers), 2) if all_multipliers else 0.0
        },
        "evidences": evidence_list
    }


def calculate_tail_risk_index(trades):
    """指标 2：伪神剥离比 (Tail-Risk Reveal Index)"""
    close_trades = [t for t in _aggregate_trades(trades) if _is_close(t)]
    profits = [t['totalPnl'] for t in close_trades if t['totalPnl'] > 0]
    loss_trades = [t for t in close_trades if t['totalPnl'] < 0]

    max_loss, evidences = 0.0, []
    if loss_trades:
        worst_loss_trade = min(loss_trades, key=lambda x: x['totalPnl'])
        max_loss = abs(worst_loss_trade['totalPnl'])
        evidences = [worst_loss_trade]

    median_profit = statistics.median(profits) if profits else 0.0
    index_val = (max_loss / median_profit) if median_profit > 0 else (float('1000') if max_loss > 0 else 0.0)

    win_count, loss_count = len(profits), len(loss_trades)
    avg_profit = sum(profits) / win_count if win_count > 0 else 0.0
    avg_loss = sum(abs(t['totalPnl']) for t in loss_trades) / loss_count if loss_count > 0 else 0.0
    normal_pnl_ratio = (avg_profit / avg_loss) if avg_loss > 0 else (float('1000') if avg_profit > 0 else 0.0)

    return {
        "summary": {
            "max_single_loss": round(max_loss, 4),
            "median_single_profit": round(median_profit, 4),
            "tail_risk_index": round(index_val, 2),
            "win_count": win_count,
            "loss_count": loss_count,
            "normal_pnl_ratio": round(normal_pnl_ratio, 4)
        },
        "evidences": evidences
    }


def calculate_slippage_trap_ratio(trades, threshold_mins=5):
    """指标 3：散户绞肉机系数 (Slippage & Latency Trap)"""
    threshold_ms = threshold_mins * 60 * 1000
    groups = defaultdict(list)
    for t in _aggregate_trades(trades):
        groups[(t['symbol'], t['positionSide'])].append(t)

    all_seqs = []
    for group_trades in groups.values():
        group_trades.sort(key=lambda x: x['orderUpdateTime'])
        current_opens = []
        for trade in group_trades:
            if _is_open(trade):
                current_opens.append(trade)
            elif _is_close(trade) and current_opens:
                hold_time_ms = trade['orderUpdateTime'] - current_opens[0]['orderUpdateTime']
                all_seqs.append({
                    "hold_time_ms": hold_time_ms,
                    "hold_time_minutes": round(hold_time_ms / 60000, 2),
                    "sequence_trades": current_opens + [trade]
                })
                current_opens = []

    all_seqs.sort(key=lambda x: x['hold_time_ms'])
    total_seqs = len(all_seqs)
    short_hold_seqs = sum(1 for s in all_seqs if s['hold_time_ms'] < threshold_ms)
    total_hold_mins = sum(s['hold_time_minutes'] for s in all_seqs)

    return {
        "summary": {
            "total_sequences": total_seqs,
            "short_hold_sequences": short_hold_seqs,
            "slippage_trap_ratio_percent": round((short_hold_seqs / total_seqs * 100), 2) if total_seqs > 0 else 0.0,
            "average_hold_time_minutes": round(total_hold_mins / total_seqs, 2) if total_seqs > 0 else 0.0,
            "threshold_minutes": threshold_mins
        },
        "evidences": all_seqs
    }


def calculate_vw_hold_ratio(trades):
    """指标 4：真正死扛指数 (Volume-Weighted Hold Ratio)"""
    open_queues = defaultdict(list)
    profit_ht_vol, profit_vol = 0.0, 0.0
    loss_ht_vol, loss_vol = 0.0, 0.0

    for trade in _aggregate_trades(trades):
        key = (trade['symbol'], trade['positionSide'])
        if _is_open(trade):
            open_queues[key].append({'qty': trade['executedQty'], 'time': trade['orderUpdateTime']})
        elif _is_close(trade):
            qty_to_close = trade['executedQty']
            queue = open_queues[key]

            while qty_to_close > 1e-8 and queue:
                open_order = queue[0]
                match_qty = min(qty_to_close, open_order['qty'])
                hold_time_ms = trade['orderUpdateTime'] - open_order['time']
                chunk_pnl = trade['totalPnl'] * (match_qty / trade['executedQty'])

                if chunk_pnl > 0:
                    profit_ht_vol += hold_time_ms * match_qty
                    profit_vol += match_qty
                elif chunk_pnl < 0:
                    loss_ht_vol += hold_time_ms * match_qty
                    loss_vol += match_qty

                open_order['qty'] -= match_qty
                if open_order['qty'] <= 1e-8: queue.pop(0)
                qty_to_close -= match_qty

    avg_profit_hold = (profit_ht_vol / profit_vol) if profit_vol > 0 else 0
    avg_loss_hold = (loss_ht_vol / loss_vol) if loss_vol > 0 else 0
    vw_index = (avg_loss_hold / avg_profit_hold) if avg_profit_hold > 0 else (
        float('10000') if avg_loss_hold > 0 else 0.0)

    return {
        "avg_profit_hold_time_ms": round(avg_profit_hold, 2),
        "avg_loss_hold_time_ms": round(avg_loss_hold, 2),
        "vw_hold_ratio": round(vw_index, 2)
    }


# =====================================================================
# 模块 4：API 数据抓取引擎 (核心业务及智能拉取)
# =====================================================================

def fetch_history_by_time_range(portfolio_id, start_time_str, end_time_str, max_count=0, max_retries=3):
    """按时间区间拉取币安带单组合的历史订单"""
    url = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/order-history"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/149.0.0.0 Safari/537.36",
        "Content-Type": "application/json",
        "lang": "zh-CN",
        "clienttype": "web",
        # 你的鉴权相关 Header (已保持原样)
        "X-UI-REQUEST-TRACE": "4b36da1d-4a7e-46a0-b54f-5640ef4e23cb",
        "BNC-UUID": "7f010d68-c61a-4dd3-b189-92a9f1b07dab",
        "FVIDEO-ID": "33fcb102fe35aa7f807c0ebc9b09aec9c0b6c879",
        "FVIDEO-TOKEN": "2MdxKhoStvXWpL0UkmBbFpkG0KDo8iZ4RsB7VhaQmRvezJ0hieQxkpzfwwWPyfqpFGah9HSjrGOd7fDXrFDGX++idCK8MA2IVQuFKwDZizKM3Ae35JDt7x6ui4TYP7ek9MciJ/Etc1S/+RcnBlvUAJM/MYBU93akhvHznCe4acucM3I36RH9AA5sgmHS3FNus=20",
    }

    start_ms, current_end_ms = time_str_to_ms(start_time_str), time_str_to_ms(end_time_str)
    all_orders, page_num = [], 1
    session = requests.Session()
    session.headers.update(headers)

    print(f"🚀 目标 Portfolio: {portfolio_id} | 区间: [{start_time_str}] 到 [{end_time_str}]")

    while True:
        print(f"\n正在拉取第 {page_num} 页... (当前上限时间戳: {current_end_ms})")
        payload = {"portfolioId": portfolio_id, "startTime": start_ms, "endTime": current_end_ms, "pageSize": 50}

        retries, success, orders = 0, False, []
        while retries < max_retries and not success:
            try:
                res = session.post(url, json=payload, timeout=10)
                res.raise_for_status()
                res_data = res.json()

                if str(res_data.get("code", "000000")) != "000000" or not res_data.get("success", True):
                    print(f"❌ API 业务失败！Code: {res_data.get('code')}, Message: {res_data.get('message')}")
                    retries += 1
                    if retries >= max_retries: return all_orders
                    time.sleep(3 * retries)
                    continue

                data_body = res_data.get("data", {})
                orders = data_body.get("list", []) if isinstance(data_body, dict) else data_body
                success = True

            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"⚠️ 请求失败 (尝试 {retries}/{max_retries}): {e}")
                if retries >= max_retries: return all_orders
                time.sleep(3 * retries)

        if not orders: break

        all_orders.extend(orders)
        print(f"✅ 获取 {len(orders)} 条记录。累计总数: {len(all_orders)}")

        if max_count > 0 and len(all_orders) >= max_count:
            all_orders = all_orders[:max_count]
            break

        last_time = orders[-1].get("time") or orders[-1].get("createTime") or orders[-1].get("orderTime")
        if not last_time: break

        current_end_ms = int(last_time) - 1
        if current_end_ms <= start_ms: break

        page_num += 1
        time.sleep(random.uniform(1.2, 2.8))

    all_orders.sort(key=lambda x: x.get("orderTime") or x.get("createTime") or x.get("time"), reverse=True)
    return all_orders


def smart_fetch_history_by_time_range(portfolio_id, target_start_str, target_end_str, file_path, max_count=100,
                                      max_retries=3):
    """智能历史订单拉取器（带本地缓存合并与差集填补功能）"""
    target_start_ms, target_end_ms = time_str_to_ms(target_start_str), time_str_to_ms(target_end_str)
    local_data = read_json(file_path)

    unique_orders = {_generate_order_uid(o): o for o in local_data}
    existing_times = [int(o.get("orderUpdateTime")) for o in local_data if o.get("orderUpdateTime")]

    missing_ranges = []
    if not existing_times:
        missing_ranges.append((target_start_ms, target_end_ms))
    else:
        local_min, local_max = min(existing_times), max(existing_times)
        if target_start_ms < local_min:
            missing_ranges.append((target_start_ms, min(target_end_ms, local_min - 1)))
        if target_end_ms > local_max:
            missing_ranges.append((max(target_start_ms, local_max + 1), target_end_ms))

    newly_fetched = []
    for start_ms, end_ms in missing_ranges:
        fetched = fetch_history_by_time_range(
            portfolio_id, ms_to_time_str(start_ms), ms_to_time_str(end_ms), max_count, max_retries
        )
        newly_fetched.extend(fetched)

    # 合并去重并保存
    for order in newly_fetched:
        unique_orders[_generate_order_uid(order)] = order

    all_merged = sorted(unique_orders.values(), key=lambda x: int(x.get("orderUpdateTime", 0)), reverse=True)
    save_json(file_path, all_merged)

    return [x for x in all_merged if target_start_ms <= int(x.get("orderUpdateTime", 0)) <= target_end_ms]


def fetch_binance_copy_traders(target_count: int) -> list:
    """持续拉取币安跟单交易员列表"""
    url = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list"
    headers = {
        "content-type": "application/json",
        "clienttype": "web",
        # 以下凭证请定期更新
        "bnc-uuid": "2772e08c-2f51-4f76-a3a7-f8d5700463fb",
        "fvideo-id": "33e0521b114aa99a931a3ed42ca03cf0147220b0",
        "fvideo-token": "1QkvzsWMC7dTM90sd+1CDpNi9KuLu8Sb2Oozg1K72UFTgZPiCi5ZXRiw45god7t2bEf3/wXtwrWTQiJQqnOKs+d4g/Dsg6ygUsqkmnnjM2SS2XMMT3e1RVjKJCnxlGihE1Cym9j9iXwQa51zQSE8Mhmg4jAtJjX9k7aOxac8iRfpHTkWnN5E22MzV0LecO6IA=76",
    }

    all_traders = []
    page_number = 1

    while len(all_traders) < target_count:
        payload = {
            "pageNumber": page_number, "pageSize": 20, "timeRange": "30D",
            "dataType": "PNL", "favoriteOnly": False, "portfolioType": "PUBLIC_PRIVATE"
        }
        try:
            res = requests.post(url, headers=headers, json=payload)
            res.raise_for_status()
            current_page_data = res.json().get('data', {}).get('list', [])

            if not current_page_data: break

            all_traders.extend(current_page_data)
            print(f"成功获取第 {page_number} 页, 当前总计 {len(all_traders)} 条。")
            page_number += 1
            time.sleep(1.5)
        except Exception as e:
            print(f"拉取出错: {e}")
            break

    return all_traders[:target_count]


# =====================================================================
# 模块 5：最终风控报告生成引擎
# =====================================================================

def get_detect_report(all_data, file_name):
    """装配各大风控模块，生成最终诊断报告"""
    row_data = {'file_name': file_name, 'data_len': len(all_data)}
    detail_map = {}
    risk_score = 0

    # 0. 基础信息装载
    order_times = [item.get("orderTime") for item in all_data if item.get("orderTime")]
    detail_map['overview'] = {
        "total_trades": len(all_data),
        "start_time": min(order_times) if order_times else None,
        "end_time": max(order_times) if order_times else None,
        "risk_score": 50
    }

    if not all_data:
        return row_data, detail_map

    # 1. 模块：马丁格尔识别
    martingale_res = calculate_martingale_rate_simplified(all_data)
    row_data.update(martingale_res.get('summary', {}))
    marting_evidences = martingale_res.get('evidences', [])
    martingale_res['evidences'] = marting_evidences[:1]

    if martingale_res['summary'].get('martingale_rate_percent', 0) > 10 and marting_evidences and len(
            marting_evidences[0]) > 2:
        print(f"文件: {file_name} | 警告：马丁格尔行为过于频繁！")
        detail_map['martingale'] = martingale_res
        risk_score += 25

    # 2. 模块：尾部风险探测
    tail_risk_res = calculate_tail_risk_index(all_data)
    row_data.update(tail_risk_res.get('summary', {}))
    tail_risk_res['evidences'] = tail_risk_res.get('evidences', [])[:1]

    tail_summary = tail_risk_res.get('summary', {})
    if tail_summary.get('tail_risk_index', 0) > 20 or tail_summary.get('normal_pnl_ratio', 1.0) < 0.3:
        print(f"文件: {file_name} | 警告：尾部风险过高！")
        detail_map['tail_risk'] = tail_risk_res
        risk_score += 25

    # 3. 模块：绞肉机/滑点陷阱探测
    slippage_res = calculate_slippage_trap_ratio(all_data)
    row_data.update(slippage_res.get('summary', {}))
    slippage_res['evidences'] = slippage_res.get('evidences', [])[:2]

    if slippage_res['summary'].get('slippage_trap_ratio_percent', 0) > 30:
        print(f"文件: {file_name} | 警告：带单者操作周期过短，摩擦成本极高！")
        detail_map['slippage_trap'] = slippage_res
        risk_score += 25

    # 4. 模块：真正死扛指数
    vw_hold_res = calculate_vw_hold_ratio(all_data)
    row_data.update(vw_hold_res)

    if vw_hold_res.get('vw_hold_ratio', 0) > 5.0:
        print(f"文件: {file_name} | 警告：检测到严重死扛/扛单行为！")
        detail_map['vw_hold_ratio'] = vw_hold_res
        risk_score += 25

    # 汇总风险分
    detail_map['overview']['risk_score'] = risk_score
    return row_data, detail_map


def get_report(url_str):
    lead_id = extract_lead_id(url_str)
    if not lead_id:
        print(f"❌ 无法从链接中提取 Lead ID: {url_str}")
        return {}

    now = datetime.now()
    TARGET_END = now.strftime("%Y-%m-%d %H:%M:%S")
    TARGET_START = (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

    output_dir = "temp_data"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{lead_id}.json")

    print(f"📊 开始生成报告 | 时间范围: [{TARGET_START}] -> [{TARGET_END}]")
    all_data = smart_fetch_history_by_time_range(lead_id, TARGET_START, TARGET_END, output_file)
    row_data, detail_map = get_detect_report(all_data, lead_id)

    return detail_map


# =====================================================================
# 模块 6：Main 执行区
# =====================================================================
if __name__ == "__main__":
    # # 拉取所有的biance带单的人
    traders_list = fetch_binance_copy_traders(400)
    trader_map = {trader['leadPortfolioId']: trader['nickname'] for trader in traders_list}

    for lead_id, nickname in trader_map.items():
        print(f"\n🔹 分析带单者: {nickname} | Lead ID: {lead_id}")
        url_str = f"https://www.binance.com/zh-CN/copy-trading/lead-details/{lead_id}"
        detail_map = get_report(url_str)
        print(f"✅ 分析完成，输出概览: {json.dumps(detail_map.get('overview'), indent=4, ensure_ascii=False)}")
        time.sleep(random.uniform(1.5, 3.0))
    # 示例调用
    url_str = 'https://www.binance.com/zh-CN/copy-trading/lead-details/5014426348046646785'
    detail_map = get_report(url_str)
    print("\n✅ 分析完成，输出概览:", json.dumps(detail_map.get('overview'), indent=4, ensure_ascii=False))