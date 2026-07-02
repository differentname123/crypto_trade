import datetime
import os
from collections import defaultdict
import statistics

import pandas as pd

from common.common_utils import read_json

from collections import defaultdict

def _is_open(trade):
    """判断当前交易是否为开仓（增加风险暴露）"""
    # ====== 新增：兼容单向持仓 BOTH 模式 ======
    if trade.get('positionSide') == 'BOTH':
        # 在单向持仓中，如果有盈亏，必定是平仓，则此处一定不是开仓
        if trade.get('totalPnl', 0.0) != 0.0:
            return False
        # 如果盈亏严格为0，根据订单方向兜底判定
        # 此时 BUY 是开多，SELL 是开空
        return trade['side'] in ['BUY', 'SELL']
    # ==========================================

    # 保持原有双向持仓逻辑严禁修改
    return (trade['positionSide'] == 'LONG' and trade['side'] == 'BUY') or \
           (trade['positionSide'] == 'SHORT' and trade['side'] == 'SELL')

def _is_close(trade):
    """判断当前交易是否为平仓（释放风险暴露）"""
    # ====== 新增：兼容单向持仓 BOTH 模式 ======
    if trade.get('positionSide') == 'BOTH':
        # 在单向持仓中，只要产生任何已实现盈亏(Pnl不为0)，则百分之百是平仓动作
        if trade.get('totalPnl', 0.0) != 0.0:
            return True
        # 如果盈亏严格为0，由于无法拿到前置持仓快照，从风控角度安全兜底为开仓(即非平仓)
        return False
    # ==========================================

    # 保持原有双向持仓逻辑严禁修改
    return (trade['positionSide'] == 'LONG' and trade['side'] == 'SELL') or \
           (trade['positionSide'] == 'SHORT' and trade['side'] == 'BUY')


def _aggregate_trades(trades, time_window_ms=1000):
    """
    【完全体·碎单聚合器】
    将1秒内被撮合引擎拆分的市价单合并为一笔逻辑单。
    重点：除了合并均价和数量，必须合并总盈亏(totalPnl)！
    """
    sorted_trades = sorted(trades, key=lambda x: x['orderUpdateTime'])
    if not sorted_trades: return []

    aggregated = []
    for t in sorted_trades:
        if not aggregated:
            aggregated.append(t.copy())
            continue

        last = aggregated[-1]
        if (t['symbol'] == last['symbol'] and t['side'] == last['side'] and
                t['positionSide'] == last['positionSide'] and (t['orderUpdateTime'] - last['orderUpdateTime']) <= time_window_ms):

            total_qty = last['executedQty'] + t['executedQty']
            if total_qty > 0:
                last['avgPrice'] = (last['avgPrice'] * last['executedQty'] + t['avgPrice'] * t[
                    'executedQty']) / total_qty

            last['executedQty'] = total_qty
            # 【关键修复】：累加拆单产生的碎片盈亏，还原真实的单笔总盈亏
            last['totalPnl'] += t.get('totalPnl', 0.0)
            last['orderUpdateTime'] = t['orderUpdateTime']
        else:
            aggregated.append(t.copy())

    return aggregated

def calculate_martingale_rate_simplified(trades, volume_multiplier=1.5):
    """
    【极简版马丁格尔识别 - 新增实际最小倍数统计】
    """
    # 0. 基础清洗防拆单
    clean_trades = _aggregate_trades(trades)

    # 1. 按方向和币种分组
    groups = defaultdict(list)
    for t in clean_trades:
        groups[(t['symbol'], t['positionSide'])].append(t)

    total_add_sequences = 0
    martingale_sequences = 0
    evidence_list = []
    all_trigger_multipliers = []  # 用于收集所有命中节点的实际加仓倍数

    for key, group_trades in groups.items():
        group_trades.sort(key=lambda x: x['orderUpdateTime'])
        current_open_sequence = []

        for trade in group_trades:
            if _is_open(trade):
                current_open_sequence.append(trade)
            else:
                # 遇到平仓单，结算当前开仓序列
                if len(current_open_sequence) >= 2:
                    total_add_sequences += 1
                    is_marting, evidence, seq_multipliers = _evaluate_sequence(current_open_sequence, volume_multiplier)
                    if is_marting:
                        martingale_sequences += 1
                        evidence_list.append(evidence)
                        all_trigger_multipliers.extend(seq_multipliers)

                current_open_sequence = []

        # 检查循环结束时遗留的未平仓序列
        if len(current_open_sequence) >= 2:
            total_add_sequences += 1
            is_marting, evidence, seq_multipliers = _evaluate_sequence(current_open_sequence, volume_multiplier)
            if is_marting:
                martingale_sequences += 1
                evidence_list.append(evidence)
                all_trigger_multipliers.extend(seq_multipliers)

    # 按照序列长度降序排序
    evidence_list.sort(key=len, reverse=True)

    rate = (martingale_sequences / total_add_sequences * 100) if total_add_sequences > 0 else 0.0

    # 获取全局所有马丁格尔动作中，最克制（最小）的那次加仓倍数
    min_actual_mult = min(all_trigger_multipliers) if all_trigger_multipliers else 0.0

    return {
        "summary": {
            "total_add_sequences": total_add_sequences,
            "martingale_sequences": martingale_sequences,
            "martingale_rate_percent": round(rate, 2),
            "min_actual_multiplier": round(min_actual_mult, 2)  # 【新增长枪实弹字段】
        },
        "evidences": evidence_list
    }


def _evaluate_sequence(seq, volume_multiplier):
    """评估序列，命中时额外返回该序列中所有触发马丁格尔节点的实际加仓倍数列表"""
    first_trade = seq[0]
    first_price = first_trade['avgPrice']
    position_side = first_trade['positionSide']

    # 新增条件：序列的平均数量必须大于初始的数量
    total_qty = sum(trade['executedQty'] for trade in seq)
    avg_qty = total_qty / len(seq)
    if avg_qty <= first_trade['executedQty']:
        return False, None, []

    is_martingale = False
    seq_multipliers = []

    for i in range(1, len(seq)):
        current_trade = seq[i]
        prev_trade = seq[i - 1]

        # 条件一：价格相较于序列第一笔更加劣势
        is_worse_price = False
        if position_side == 'LONG' and current_trade['avgPrice'] < first_price:
            is_worse_price = True
        elif position_side == 'SHORT' and current_trade['avgPrice'] > first_price:
            is_worse_price = True

        # 条件二：当前成交量 >= 上一次成交量的设定倍数
        is_heavy_add = current_trade['executedQty'] >= (prev_trade['executedQty'] * volume_multiplier)

        if is_worse_price and is_heavy_add:
            is_martingale = True
            # 实时计算当前动作的实际加仓倍数
            actual_mult = current_trade['executedQty'] / prev_trade['executedQty']
            seq_multipliers.append(actual_mult)

    if is_martingale:
        return True, seq, seq_multipliers

    return False, None, []


# =====================================================================
# 指标 2：伪神剥离比 (Tail-Risk Reveal Index)
# =====================================================================

def calculate_tail_risk_index(trades):
    """
    【指标名称】：伪神剥离比 (Tail-Risk Reveal Index) - 尾部风险探测器

    【业务逻辑】：
    散户最容易被“高胜率”骗局收割。本函数提取该带单者历史中【最惨烈的一笔宏观平仓亏损】，
    并将其与他【平时的中位数盈利】进行对比。使用中位数是为了剔除极个别好运带来的暴利单干扰。

    【穿透价值 / 前端解读】：
    - 危险 (>20)：典型的“捡钢镚压路机”。辛辛苦苦赚20次几十块钱，只要错1次就亏几千块。这种人高概率靠死扛维系胜率。
    - 良性 (<5) ：优秀的盈亏质量。用小的试错成本去捕捉大波段。

    【返回值说明】：
    - summary: 包含单笔最大亏损绝对值、常态单笔盈利中位数、尾部风险剥离比、盈亏单数量及常规盈亏比。
    - evidences: 列表格式，包含那笔造成“单笔最大亏损”的完整案发现场（原始订单数据）。方便前端直接向跟单者展示“他最惨的一次亏了多少”。（若全胜无亏损，则返回空列表）
    """
    # 1. 挂载聚合器：洗掉碎单，还原带单者真实的单次“宏观操作盈亏”
    clean_trades = _aggregate_trades(trades)

    # 2. 提取所有平仓动作
    close_trades = [t for t in clean_trades if _is_close(t)]

    # 3. 分离正负收益 (过滤掉盈亏严格为0的平平单)
    profits = [t['totalPnl'] for t in close_trades if t['totalPnl'] > 0]

    # 【修改点】：这里不再只取数字，而是保留整个 trade 对象，方便后续提取证据
    loss_trades = [t for t in close_trades if t['totalPnl'] < 0]

    # 4. 计算极值与基准线，同时抓取案发现场
    if loss_trades:
        # 找出 totalPnl 最小（即亏损最大）的那一笔完整交易对象
        worst_loss_trade = min(loss_trades, key=lambda x: x['totalPnl'])
        max_loss = abs(worst_loss_trade['totalPnl'])
        evidences = [worst_loss_trade]
    else:
        max_loss = 0.0
        evidences = []

    median_profit = statistics.median(profits) if profits else 0.0

    # 5. 防零除异常处理 (尾部风险)
    if median_profit == 0:
        index_val = float('inf') if max_loss > 0 else 0.0
    else:
        index_val = max_loss / median_profit

    # ================= 新增逻辑区块开始 =================
    # 6. 统计盈亏单子数量
    win_count = len(profits)
    loss_count = len(loss_trades)

    # 7. 计算正常盈亏比 (平均盈利 / 平均亏损绝对值)
    losses_abs = [abs(t['totalPnl']) for t in loss_trades]

    avg_profit = sum(profits) / win_count if win_count > 0 else 0.0
    avg_loss = sum(losses_abs) / loss_count if loss_count > 0 else 0.0

    if avg_loss == 0:
        normal_pnl_ratio = float('inf') if avg_profit > 0 else 0.0
    else:
        normal_pnl_ratio = avg_profit / avg_loss
    # ================= 新增逻辑区块结束 =================

    # 【修改点】：结构化返回，对齐另外几个风控函数的 JSON 格式
    return {
        "summary": {
            "max_single_loss": round(max_loss, 4),
            "median_single_profit": round(median_profit, 4),
            "tail_risk_index": round(index_val, 2),
            # 新增的统计字段
            "win_count": win_count,
            "loss_count": loss_count,
            "normal_pnl_ratio": round(normal_pnl_ratio, 4)
        },
        "evidences": evidences
    }

# =====================================================================
# 指标 3：散户绞肉机系数 (Slippage & Latency Trap) - 极简周期切片版
# =====================================================================
def calculate_slippage_trap_ratio(trades, threshold_mins=5):
    """
    【指标名称】：散户绞肉机系数 (Slippage & Latency Trap) - 极简切片版

    【业务逻辑（基于连续序列切片）】：
    1. 预处理：按交易对和方向独立分组，并按时间严格正序排列。
    2. 动作捕获：持续收集开仓单，一旦遇到任何平仓动作，立刻将之前的开仓单与该平仓单打包，结算为一个“完整的交易切片”。
    3. 寿命计算：该切片的存活时间 = 触发平仓的时间 - 序列中【第一笔开仓】的时间。
    4. 状态重置：结算后清空历史开仓记录，忽略后续的分批平仓动作（防止同一波操作被放大），直到下一个开仓动作开启全新的切片。

    【穿透价值】：
    极简且精准地提取带单者的宏观“操作周期”，完美过滤分批平仓带来的数据污染。
    在衍生品跟单中，跟单者的滑点和手续费损耗与带单者的操作频率呈绝对正相关。只要带单者的盈利建立在极短周期（如 < 5分钟）的微利上，无论其面板胜率多高，跟单者的本金都会被隐形摩擦成本彻底抽干。

    【前端 / 辩证解读】：
    - 危险 (>30% 短线占比)：高频“割韭菜”机器。带单者利用资金和网速优势赚取微小点差，跟单者承受巨大滑点，最终必然导致“带单者赚钱，跟单者爆仓”。
    - 良性 (<5% 短线占比)：格局较大的波段交易者。平均持仓时长在数小时或数天以上，容纳散户资金量极大，跟单摩擦成本几乎可以忽略不计。

    【返回值说明】：
    - summary (字典) 汇总核心数据：
        - total_sequences: 总计捕获到的“完整开平仓序列”个数。
        - short_hold_sequences: 存活时间小于设定阈值（默认5分钟）的“超短线序列”个数。
        - slippage_trap_ratio_percent: 绞肉机比例（即超短线序列的占比）。
        - average_hold_time_minutes: 全局平均持仓时长（分钟），直观反映该带单者的持单耐心。
        - threshold_minutes: 用于判定超短线的判定标准（默认5分钟）。
    - evidences (列表) 案发现场数据：
        - 按存活时间【升序】排列（即“活得最短、最像绞肉机”的序列直接排在最前面，方便前端直接抓取展示）。
        - 列表内每个元素包含：
            - hold_time_ms: 该序列存活时间的毫秒值。
            - hold_time_minutes: 该序列存活时间的分钟值。
            - sequence_trades: 包含该序列所有动作（前面的N笔开仓 + 触发结算的1笔平仓）的原始交易数据列表。
    """
    threshold_ms = threshold_mins * 60 * 1000
    clean_trades = _aggregate_trades(trades)  # 依然保留前置的碎单合并防误判机制

    groups = defaultdict(list)
    for t in clean_trades:
        groups[(t['symbol'], t['positionSide'])].append(t)

    all_sequences = []

    for key, group_trades in groups.items():
        group_trades.sort(key=lambda x: x['orderUpdateTime'])

        current_opens = []

        for trade in group_trades:
            if _is_open(trade):
                current_opens.append(trade)
            elif _is_close(trade):
                # 只要遇到平仓，且前面有积累的开仓单，就视为一个完整周期被触发结算
                if current_opens:
                    first_open = current_opens[0]
                    hold_time_ms = trade['orderUpdateTime'] - first_open['orderUpdateTime']

                    # 组装完整的案发现场 (所有开仓单 + 本次触发结算的平仓单)
                    seq_trades = current_opens + [trade]

                    all_sequences.append({
                        "hold_time_ms": hold_time_ms,
                        "hold_time_minutes": round(hold_time_ms / 60000, 2),
                        "sequence_trades": seq_trades
                    })

                    # 【核心切片逻辑】：结算后清空，这波多单/空单的周期已盖棺定论。
                    # 如果后续还有连续的平仓单（分批平仓），由于 current_opens 已空，会被自动忽略。
                    current_opens = []

    # 按照持仓时间进行【升序排序】（存活时间最短的、最像绞肉机的排在最前面）
    all_sequences.sort(key=lambda x: x['hold_time_ms'])

    total_seqs = len(all_sequences)
    short_hold_seqs = 0
    total_hold_time_mins = 0.0

    evidences = []

    for seq_data in all_sequences:
        total_hold_time_mins += seq_data['hold_time_minutes']
        if seq_data['hold_time_ms'] < threshold_ms:
            short_hold_seqs += 1
        # 直接收集包含了时间间隔和原始 trade 列表的对象
        evidences.append(seq_data)

    avg_hold_time = round(total_hold_time_mins / total_seqs, 2) if total_seqs > 0 else 0.0
    ratio = round((short_hold_seqs / total_seqs * 100), 2) if total_seqs > 0 else 0.0

    return {
        "summary": {
            "total_sequences": total_seqs,
            "short_hold_sequences": short_hold_seqs,
            "slippage_trap_ratio_percent": ratio,
            "average_hold_time_minutes": avg_hold_time,
            "threshold_minutes": threshold_mins
        },
        "evidences": evidences
    }

# =====================================================================
# 指标 4：真正死扛指数 (Volume-Weighted Hold Ratio)
# =====================================================================
def calculate_vw_hold_ratio(trades):
    """
    【指标名称】：真正死扛指数 (Volume-Weighted Hold Ratio) - 加权持仓时间不对称性

    【业务逻辑】：
    修正了市面上常规“死扛指数”忽略仓位大小的致命漏洞。
    如果用1%底仓扛单10天，99%重仓做短线试错，算不上死扛；但如果是重仓扛单，则是大忌。
    本算法通过 FIFO 队列，将单次平仓盈亏【按匹配比例拆解】到历史开仓单上，
    最终计算：(亏损资金的总占用时长) / (盈利资金的总占用时长)。

    【穿透价值 / 前端解读】：
    - 危险 (>5.0) ：在错误的方向上重仓且迟迟不认错，在正确的方向上却拿不住单子。爆仓倒计时。
    - 良性 (<1.0) ：完美的“截断亏损，让利润奔跑”信徒。

    【返回值说明】：
    - avg_profit_hold_time_ms: 赚钱份额的资金加权平均持有时间
    - avg_loss_hold_time_ms: 亏钱份额的资金加权平均持有时间
    - vw_hold_ratio: 死扛指数
    """
    # 1. 过滤碎单，这一步能极大降低 while 循环中的浮点数运算损耗与精度失真
    clean_trades = _aggregate_trades(trades)
    open_queues = defaultdict(list)

    # 状态累加器：总量 (Volume) 与 资金占用时长 (Hold_Time * Volume)
    profit_hold_time_volume = 0.0
    profit_volume = 0.0
    loss_hold_time_volume = 0.0
    loss_volume = 0.0

    for trade in clean_trades:
        key = (trade['symbol'], trade['positionSide'])

        if _is_open(trade):
            open_queues[key].append({'qty': trade['executedQty'], 'time': trade['orderUpdateTime']})

        elif _is_close(trade):
            qty_to_close = trade['executedQty']
            queue = open_queues[key]

            # FIFO 循环拆解盈亏
            while qty_to_close > 1e-8 and queue:
                open_order = queue[0]
                match_qty = min(qty_to_close, open_order['qty'])
                hold_time_ms = trade['orderUpdateTime'] - open_order['time']

                # 【算法核心】：按抵消数量占本次平仓总量的比例，等比例切分平仓盈亏
                chunk_pnl = trade['totalPnl'] * (match_qty / trade['executedQty'])

                # 按照这部分切分出来的盈亏，将其归类为“盈利存活期”或“亏损存活期”
                if chunk_pnl > 0:
                    profit_hold_time_volume += hold_time_ms * match_qty
                    profit_volume += match_qty
                elif chunk_pnl < 0:
                    loss_hold_time_volume += hold_time_ms * match_qty
                    loss_volume += match_qty

                open_order['qty'] -= match_qty
                if open_order['qty'] <= 1e-8:
                    queue.pop(0)

                qty_to_close -= match_qty

    # 计算 资金加权平均存活时间 = (存活时间 * 成交量) / 成交量
    avg_profit_hold = (profit_hold_time_volume / profit_volume) if profit_volume > 0 else 0
    avg_loss_hold = (loss_hold_time_volume / loss_volume) if loss_volume > 0 else 0

    # 防零除异常处理 (如果没有盈利单，亏损持仓比例视作无限大)
    if avg_profit_hold == 0:
        vw_index = float('inf') if avg_loss_hold > 0 else 0.0
    else:
        vw_index = avg_loss_hold / avg_profit_hold

    return {
        "avg_profit_hold_time_ms": round(avg_profit_hold, 2),
        "avg_loss_hold_time_ms": round(avg_loss_hold, 2),
        "vw_hold_ratio": round(vw_index, 2)
    }


def get_detect_report(all_data):
    # 初始化当前文件的数据行，首先放入 file_name
    row_data = {'file_name': file_name, 'data_len': len(all_data)}
    detail_map = {}
    # ==========================================
    # 模块 1: 马丁格尔率 (如果你不想计算，直接注释掉这整个区块)
    # ==========================================
    result = calculate_martingale_rate_simplified(all_data)
    row_data.update(result.get('summary', {}))  # 收集 summary
    detail_map['martingale'] = result  # 收集 detail

    # 马丁格尔率的警告逻辑
    if 'summary' in result:  # 增加一个安全判断，防止字典为空报错
        martingale_rate_percent = result['summary'].get('martingale_rate_percent', 0)
        evidences = result.get('evidences', [[]])
        if martingale_rate_percent > 10 and len(evidences[0]) > 2:
            print(f"文件: {file_name} | 马丁格尔率: {martingale_rate_percent}% | 警告：马丁格尔行为过于频繁！")

    # ==========================================
    # 模块 2: 尾部风险 (如果你不想计算，直接注释掉这整个区块)
    # ==========================================
    result1 = calculate_tail_risk_index(all_data)
    row_data.update(result1.get('summary', {}))  # 收集 summary
    detail_map['tail_risk'] = result1  # 收集 detail

    # 尾部风险的警告逻辑
    if 'summary' in result1:
        tail_risk_index = result1['summary'].get('tail_risk_index', 0)
        normal_pnl_ratio = result1['summary'].get('normal_pnl_ratio', 1.0)
        if tail_risk_index > 20 or normal_pnl_ratio < 0.3:
            print(f"文件: {file_name} | 尾部风险剥离比: {tail_risk_index} | 警告：尾部风险过高！")

    # ==========================================
    # 模块 3: 滑点陷阱比例 (如果你不想计算，直接注释掉这整个区块)
    # ==========================================
    result2 = calculate_slippage_trap_ratio(all_data)
    row_data.update(result2.get('summary', {}))  # 收集 summary
    detail_map['slippage_trap'] = result2  # 收集 detail
    #
    # # ==========================================
    # # 模块 4: VW 持仓比例 (如果你不想计算，直接注释掉这整个区块)
    # # ==========================================
    result3 = calculate_vw_hold_ratio(all_data)
    row_data.update(result3)  # 收集 summary
    detail_map['vw_hold_ratio'] = result3  # 收集 detail
    # ------------------------------------------
    # 将当前文件收集完毕的完整 row_data 追加到列表中
    return row_data, detail_map


if __name__ == "__main__":
    # raw_data 替换为你上下文中的完整 JSON 列表
    # 扫描 W:\project\python_project\crypto_trade\app\copy_sweeper\temp_data\ 下面的所有json文件
    all_files = []
    for root, dirs, files in os.walk("W:\\project\\python_project\\crypto_trade\\app\\copy_sweeper\\temp_data\\"):
        for file in files:
            if file.endswith(".json"):
                all_files.append(os.path.join(root, file))

    summary_data_list = []

    for file_path in all_files:
        # if "5065891943928626944_老朝奉研究员.json" not in file_path:
        #     continue
        all_data = read_json(file_path)
        file_name = os.path.basename(file_path)

        # 为每个元素增加一个 'orderUpdateTime_str' 字段
        for trade in all_data:
            trade['orderUpdateTime_str'] = datetime.datetime.fromtimestamp(trade['orderUpdateTime'] / 1000).strftime(
                '%Y-%m-%d %H:%M:%S')

        row_data, detail_map = get_detect_report(all_data)
        summary_data_list.append(row_data)

    # 循环结束后，将收集到的列表转换为 DataFrame
    df = pd.DataFrame(summary_data_list)

    # 打印查看生成的 DataFrame 前几行
    print(df.head())