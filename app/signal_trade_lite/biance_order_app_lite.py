import os
import time
import traceback
import uuid
import pandas as pd
import csv
import platform
from datetime import datetime, timedelta

from common_utils_lite import get_config
from run_cross_signal_lite import execute_trading_bot_workflow
# 确保 base_trader.py (或 biance_order_lite) 中已提供必要组件
from biance_order_lite import (
    init_exchange, execute_order, get_total_equity,
    ExecStatus, logger, safe_init_exchange
)

# ==========================================
# 0. 配置与常量
# ==========================================
TRADE_RECORD_FILE = "trade_records.csv"
LEVRAGE = 1
MIN_ORDER_VALUE = 5.0
MAX_ORDER_VALUE = 2000

# 全局运行时缓存字典：用于 API 拉取失败时的无缝兜底
RUNTIME_FALLBACK = {
    "total_equity": 0.0,
    "position_cache": None,
    "open_order_cache": None
}


# ==========================================
# 1. 记账系统：全链路追溯 (纯净无兼容版)
# ==========================================
def record_trade(row, actual_time, total_equity, target_value, amount, status, client_oid, exchange_oid, msg=""):
    """
    持久化交易记录。放弃所有历史兼容，直接拉起最全的标准化表头。
    """
    file_exists = os.path.isfile(TRADE_RECORD_FILE)

    # 提取共有数据
    symbol_val = str(row.get('symbol', '')).strip()
    strat_name = str(row.get('STRATEGY_NAME', '')).strip()
    risk_ratio = float(row.get('max_weight', 0.1))

    with open(TRADE_RECORD_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            # 纯净版全尺寸表头
            writer.writerow([
                "signal_time", "action", "coin", "symbol", "strategy_name", "direction", "event", "signal_price",
                "actual_trade_time", "update_time", "total_equity", "risk_ratio", "target_value", "exec_amount",
                "exec_status", "client_oid", "exchange_oid", "error_msg", "actual_fill_price", "filled_amount"
            ])

        sig_time = row['time'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row['time'], pd.Timestamp) else row['time']

        writer.writerow([
            sig_time, row['action'], row['coin'], symbol_val, strat_name, row['direction'], row['event'], row['price'],
            actual_time, actual_time, total_equity, risk_ratio, target_value, amount, status.value, client_oid,
            exchange_oid, msg, "", ""
        ])


# ==========================================
# 2. 状态缓存与挂单巡检 (极简高效版)
# ==========================================
def sync_and_clean_orders(exchange, open_order_cache):
    """
    巡检活动挂单：同步部分成交进度，清理 2 小时僵尸单。
    完全抛弃对旧字段的 `astype` 判断，直接信任新表结构的规范性。
    """
    if not os.path.isfile(TRADE_RECORD_FILE):
        return open_order_cache

    t_start = time.perf_counter()
    try:
        df = pd.read_csv(TRADE_RECORD_FILE, dtype={'exchange_oid': str})
    except Exception as e:
        logger.error(f"[SYNC] 读取记录文件失败: {e}")
        return open_order_cache

    if df.empty:
        return open_order_cache

    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    has_changes = False

    sync_updates, cleanups = 0, 0
    synced_details, cleaned_details = [], []

    # 快速检索: exchange_oid -> order object
    open_order_dict = {str(o.get('id')): o for orders in open_order_cache.values() for o in orders}
    terminal_states = ['FAIL', 'closed', 'canceled', 'CANCELED_SUCCESS', 'CANCELED_FAIL', 'REJECTED']

    for index, row in df.iterrows():
        status = str(row.get('exec_status', ''))
        exch_oid = str(row.get('exchange_oid', ''))
        client_oid = str(row.get('client_oid', ''))
        short_cid = client_oid.split('_')[-1] if '_' in client_oid else client_oid

        if status in terminal_states or not exch_oid or exch_oid == 'nan':
            continue

        sym = str(row.get('symbol')).strip()
        try:
            sig_time = pd.to_datetime(row['signal_time'])
        except:
            continue

        # 情况 A: 订单仍在挂单池 (处理超时与部分成交进度更新)
        if exch_oid in open_order_dict:
            order_obj = open_order_dict[exch_oid]
            # 【修复点 1】：强制将隐性 None 转为有效数字或空字符串，防止 float(None) 崩溃
            avg_price = order_obj.get('average') or order_obj.get('price') or ''
            filled_amt = float(order_obj.get('filled') or 0.0)

            if (now - sig_time) > timedelta(hours=2):
                try:
                    exchange.cancel_order(exch_oid, sym)
                    df.at[index, 'exec_status'] = 'CANCELED_SUCCESS'
                    df.at[index, 'error_msg'] = '自动清理超时(>2H)挂单成功'
                    df.at[index, 'update_time'] = now_str
                    if filled_amt > 0:
                        df.at[index, 'actual_fill_price'] = avg_price
                        df.at[index, 'filled_amount'] = filled_amt

                    open_order_cache[sym] = [o for o in open_order_cache.get(sym, []) if str(o.get('id')) != exch_oid]
                    has_changes, cleanups = True, cleanups + 1
                    cleaned_details.append(f"{short_cid}(OK)")
                except Exception as e:
                    df.at[index, 'exec_status'] = 'CANCELED_FAIL'
                    df.at[index, 'error_msg'] = f'撤单失败: {e}'
                    df.at[index, 'update_time'] = now_str
                    has_changes = True
                    cleaned_details.append(f"{short_cid}(FAIL)")
            else:
                current_csv_filled = pd.to_numeric(row.get('filled_amount'), errors='coerce')
                csv_filled_float = float(current_csv_filled) if pd.notna(current_csv_filled) else 0.0
                if status != 'open' or filled_amt != csv_filled_float:
                    df.at[index, 'exec_status'] = 'open'
                    df.at[index, 'update_time'] = now_str
                    if filled_amt > 0:
                        df.at[index, 'actual_fill_price'] = avg_price
                        df.at[index, 'filled_amount'] = filled_amt
                    has_changes, sync_updates = True, sync_updates + 1
                    synced_details.append(f"{short_cid}(open_loop)")

        # 情况 B: 订单已不在挂单池 (回溯最终成交或撤销状态)
        else:
            try:
                fetched_order = exchange.fetch_order(exch_oid, sym)
                new_status = fetched_order.get('status', status)
                # 【修复点 1 续】：强制处理 fetched_order 的隐性 None
                filled_amt = float(fetched_order.get('filled') or 0.0)
                avg_price = fetched_order.get('average') or fetched_order.get('price') or ''

                if new_status != status:
                    df.at[index, 'exec_status'] = new_status
                    df.at[index, 'update_time'] = now_str
                    if filled_amt > 0:
                        df.at[index, 'actual_fill_price'] = avg_price
                        df.at[index, 'filled_amount'] = filled_amt
                    has_changes, sync_updates = True, sync_updates + 1
                    synced_details.append(f"{short_cid}({new_status})")
            except Exception as e:
                logger.warning(f"[SYNC] 回溯订单最终态失败 | CID: {client_oid} | {e}")

    if has_changes:
        df.to_csv(TRADE_RECORD_FILE, index=False)

    cost_ms = (time.perf_counter() - t_start) * 1000
    logger.info(
        f"[SYNC] 巡检耗时: {cost_ms:.1f}ms | 状态流转({sync_updates}): {','.join(synced_details) or '无'} | 超时清理({cleanups}): {','.join(cleaned_details) or '无'}")

    return open_order_cache


def preload_account_state(exchange):
    """准点前数据预加载：解决 Hedge Mode 多空相互覆写 Bug"""
    global RUNTIME_FALLBACK
    t_start = time.perf_counter()
    sync_cost, eq_cost, pos_cost, ord_cost = 0, 0, 0, 0
    total_equity = 0.0
    position_cache, open_order_cache = {}, {}

    try:
        t_sync = time.perf_counter()
        exchange.load_time_difference()
        sync_cost = (time.perf_counter() - t_sync) * 1000
    except:
        pass

    # 1. 总资产拉取
    for attempt in range(3):
        t_eq = time.perf_counter()
        eq_status, total_equity = get_total_equity(exchange)
        if eq_status == ExecStatus.OK and total_equity > 0:
            eq_cost = (time.perf_counter() - t_eq) * 1000
            RUNTIME_FALLBACK["total_equity"] = total_equity
            break
        time.sleep(1)
    if total_equity <= 0: total_equity = RUNTIME_FALLBACK["total_equity"]

    # 2. 持仓拉取 (【关键修复】：Key 升维为 Symbol_Direction 彻底隔离多空)
    position_success = False
    for attempt in range(3):
        try:
            t_pos = time.perf_counter()
            positions = exchange.fetch_positions()
            pos_cost = (time.perf_counter() - t_pos) * 1000
            for pos in positions:
                sym = pos['symbol']
                amt = float(pos['info']['positionAmt'])
                if amt != 0:
                    # 动态适配 Binance Hedge Mode (LONG/SHORT)
                    pos_side = str(pos['info'].get('positionSide', '')).upper()
                    if not pos_side or pos_side == 'BOTH':
                        pos_side = 'LONG' if amt > 0 else 'SHORT'

                    pos_key = f"{sym}_{pos_side}"
                    position_cache[pos_key] = amt

            position_success = True
            RUNTIME_FALLBACK["position_cache"] = position_cache
            break
        except Exception as e:
            time.sleep(1)

    if not position_success: position_cache = RUNTIME_FALLBACK["position_cache"]

    # 3. 挂单拉取
    order_success = False
    for attempt in range(3):
        try:
            t_ord = time.perf_counter()
            exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
            for order in exchange.fetch_open_orders():
                open_order_cache.setdefault(order['symbol'], []).append(order)
            order_success = True
            break
        except Exception as e:
            time.sleep(1)

    if not order_success: open_order_cache = RUNTIME_FALLBACK["open_order_cache"]

    if open_order_cache is not None:
        open_order_cache = sync_and_clean_orders(exchange, open_order_cache)
        RUNTIME_FALLBACK["open_order_cache"] = open_order_cache

    total_cost = (time.perf_counter() - t_start) * 1000
    logger.info(
        f"[PRELOAD] 缓存就绪 | 总权益: {total_equity:.2f} | 耗时探针: {sync_cost:.0f}ms/{eq_cost:.0f}ms/{pos_cost:.0f}ms/{ord_cost:.0f}ms")
    return total_equity, position_cache, open_order_cache


# ==========================================
# 3. 核心零延迟执行模块
# ==========================================
def execute_signals_fast(exchange, target_time, total_equity, position_cache, open_order_cache, signal_df):
    """极速批量执行引擎：禁绝网络 IO，依赖内存预加载与拟态状态机"""
    t_start = time.perf_counter()

    # 严格时间区间过滤 (-1m 到 +1m)
    time_lower = target_time - timedelta(minutes=1)
    time_upper = target_time + timedelta(minutes=1)
    # 【修复点 2】：强制将 string 或混合类型的 time 列转换为 datetime 再进行对比，防止抛错
    current_signals = signal_df[
        (pd.to_datetime(signal_df['time']) >= time_lower) & (pd.to_datetime(signal_df['time']) <= time_upper)]

    if current_signals.empty:
        logger.info(f"[EXEC] 当前无信号 | 过滤耗时: {(time.perf_counter() - t_start) * 1000:.2f}ms")
        return

    # 【零延迟优化】：将 CSV 的读取提到循环外，且只在需要平仓时才读取，避免 O(N) 的 IO 开销拖慢发单流
    record_df = pd.DataFrame()
    if any(current_signals['event'].astype(str).str.upper() == 'CLOSE') and os.path.isfile(TRADE_RECORD_FILE):
        record_df = pd.read_csv(TRADE_RECORD_FILE)

    for _, row in current_signals.iterrows():
        try:
            execute_single_signal(exchange, row, total_equity, position_cache, open_order_cache, record_df)
        except Exception as e:
            logger.error(f"发单异常拦截: {e}")

    logger.info(
        f"[EXEC] 本轮信号流转完毕 | 共 {len(current_signals)} 笔 | 总耗时: {(time.perf_counter() - t_start) * 1000:.1f}ms")


def execute_single_signal(exchange, row, total_equity, position_cache, open_order_cache, record_df):
    coin = str(row['coin']).strip().upper()
    action = str(row['action']).strip().upper()
    direction = str(row['direction']).strip().upper()
    event = str(row['event']).strip().upper()
    price = float(row['price'])
    symbol = str(row['symbol']).strip()
    strat_name = str(row.get('STRATEGY_NAME', 'DEF')).strip()

    max_weight = float(row['max_weight'])
    target_position_value = total_equity * LEVRAGE * max_weight

    # 【关键修复】：高密度、防撞车的 Client OID。提取关键字段头部，严格控制在 36 字符限额内
    sig_time_str = pd.to_datetime(row['time']).strftime('%d%H%M')
    # 格式: 策略名(前4)_币种(前4)_多空(1)_买卖(1)_开平(1)_时间(6)_UUID(5) -> 最大长度 26 字符
    order_prefix = f"{strat_name[-6:]}_{coin[:4]}_{direction[0]}_{action[0]}_{event[0]}_{sig_time_str}"
    client_oid = f"{order_prefix}_{uuid.uuid4().hex[:5]}"

    if position_cache is None or open_order_cache is None: return

    # 内存状态机读取（使用独立隔离的多空 Key）
    pos_key = f"{symbol}_{direction}"
    current_pos_amt = position_cache.get(pos_key, 0.0)
    symbol_open_orders = open_order_cache.get(symbol, [])

    # 防重复挂单物理拦截（自带策略名基因，绝不误伤）
    for o in symbol_open_orders:
        o_client_id = o.get('clientOrderId', o.get('info', {}).get('clientOrderId', ''))
        if o_client_id.startswith(order_prefix):
            logger.warning(f"重复拦截 | CID: {client_oid}")
            return

    # --- 开平仓算量逻辑 ---
    if event == "OPEN":
        target_position_value = min(max(target_position_value, MIN_ORDER_VALUE), MAX_ORDER_VALUE)
        raw_amount = target_position_value / price
        amount = float(exchange.amount_to_precision(symbol, raw_amount))

        # 【修复点 3】：移除全局物理持仓拦截，真正释放多策略隔离共存能力。
        # 依赖于前置的 OID 前缀防重发（同一信号防撞车）和资金分配，策略 A 和策略 B 均可自由构建头寸。

    elif event == "CLOSE":
        if abs(current_pos_amt) <= 0:
            return  # 本地缓存显示无持仓，静默跳过

        amount = abs(current_pos_amt)

        # 【关键修复】：纯净版平仓追溯，剔除导致“幻觉持仓”的死单
        if not record_df.empty:
            try:
                mask_coin = record_df['coin'].astype(str).str.strip().str.upper() == coin
                mask_dir = record_df['direction'].astype(str).str.strip().str.upper() == direction
                mask_strat = record_df['strategy_name'].astype(str).str.strip() == strat_name
                mask_event = record_df['event'].astype(str).str.strip().str.upper() == 'OPEN'

                # 核心风控：坚决排除已明确失败或被拒的无效账单
                mask_valid = ~record_df['exec_status'].astype(str).str.upper().isin(
                    ['FAIL', 'REJECTED', 'CANCELED_FAIL'])

                open_records = record_df[mask_coin & mask_dir & mask_strat & mask_event & mask_valid]

                if not open_records.empty:
                    latest_open_record = open_records.iloc[-1]
                    filled_amt = pd.to_numeric(latest_open_record['filled_amount'], errors='coerce')

                    if pd.isna(filled_amt) or filled_amt <= 0:
                        filled_amt = pd.to_numeric(latest_open_record['exec_amount'], errors='coerce')

                    latest_open_amount = float(filled_amt) if pd.notna(filled_amt) else 0.0

                    # 极值安全锁 (基于动态独立多空 Key 取出的真实敞口)
                    raw_amount = min(latest_open_amount, abs(current_pos_amt))
                    amount = float(exchange.amount_to_precision(symbol, raw_amount))
            except Exception as e:
                logger.error(f"[EXEC] 平仓溯源异常，降级为总敞口平仓 | {e}")
                amount = abs(current_pos_amt)
    else:
        return

    if amount <= 0: return

    # --- 最终执行 ---
    result = execute_order(
        exchange=exchange,
        symbol=symbol,
        side=action.lower(),
        amount=amount,
        client_oid=client_oid,
        order_type='limit',
        price=price,
        reduce_only=False,  # 双向持仓强约束
        position_side=direction
    )

    record_trade(
        row=row,
        actual_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        total_equity=total_equity,
        target_value=target_position_value,
        amount=amount,
        status=result.status,
        client_oid=client_oid,
        exchange_oid=result.exchange_oid,
        msg=result.error_msg
    )

    # 【关键修复】：内存拟态状态机更新 (Zero-Latency 闭环)
    # 只要挂单不被立马拒绝，立刻在内存中打标记，防止同秒钟下一个信号读取到旧状态发生重复开仓
    if result.status == ExecStatus.OK:
        open_order_cache.setdefault(symbol, []).append({
            'id': result.exchange_oid,
            'clientOrderId': client_oid,
            'info': {'clientOrderId': client_oid}
        })

        # 预扣减持仓：平仓动作下达后，瞬间清空本地敞口预期，封锁重平漏洞
        if event == 'CLOSE':
            position_cache[pos_key] -= amount




def run_scheduler():
    API_KEY = get_config('nana_biance_api_copy_key')
    SECRET_KEY = get_config('nana_biance_api_copy_secret')

    if platform.system().lower() == 'linux':
        PROXIES, proxy_url = None, None
    else:
        PROXIES = {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'}
        proxy_url = 'http://127.0.0.1:7890'

    exchange = safe_init_exchange(API_KEY, SECRET_KEY, PROXIES)
    logger.info(">>> 调度系统已就绪...")

    while True:
        try:
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            target_time = next_hour - timedelta(minutes=1)
            target_time_str = target_time.strftime("%Y-%m-%d %H:%M")
            preload_time = next_hour - timedelta(minutes=5)

            if now < preload_time:
                time.sleep((preload_time - now).total_seconds())

            # 数据预加载
            total_equity, position_cache, open_order_cache = None, None, None
            preload_success = False
            for attempt in range(1, 4):
                try:
                    total_equity, position_cache, open_order_cache = preload_account_state(exchange)
                    if total_equity > 0 and position_cache is not None and open_order_cache is not None:
                        preload_success = True
                        break
                    time.sleep(3)
                except Exception as e:
                    time.sleep(3)

            if not preload_success:
                logger.error("!!! [预加载严重错误] 连续 3 次失败，放弃本轮调度。")
                time.sleep(60)
                continue

            # 工作流获取信号
            t_wf = time.perf_counter()
            signal_df = execute_trading_bot_workflow(target_time_str, proxy_url=proxy_url)

            # 极速下单
            if signal_df is not None and not signal_df.empty:
                execute_signals_fast(exchange, next_hour, total_equity, position_cache, open_order_cache, signal_df)

        except Exception as e:
            logger.error(f"!!! [SCHEDULER 致命异常] !!!\n{traceback.format_exc()}")
            time.sleep(30)


if __name__ == "__main__":
    run_scheduler()