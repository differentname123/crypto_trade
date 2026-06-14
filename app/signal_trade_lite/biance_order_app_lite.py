import os
import time
import uuid
import pandas as pd
import csv
from datetime import datetime, timedelta

from app.signal_trade_lite.run_cross_signal_lite import execute_trading_bot_workflow
# 从基础文件中导入核心组件 (假设基础文件名为 base_trader.py)
# 确保 base_trader.py 中已经添加了上一轮我给你的 get_total_equity 函数
from biance_order_lite import (
    init_exchange, execute_order, get_total_equity,
    ExecStatus, logger
)
from common.common_utils import get_config

# ==========================================
# 0. 配置与常量
# ==========================================
TRADE_RECORD_FILE = "trade_records.csv"
POSITION_RISK_RATIO = 0.90  # 每次开仓占总资产的 10%


# ==========================================
# 1. 记账系统：全链路追溯
# ==========================================
def record_trade(row, actual_time, total_equity, risk_ratio, target_value, amount, status, client_oid, exchange_oid,
                 msg=""):
    """
    将原始信号参数与实际交易结果、资产状况合并持久化，实现全链路对账。
    """
    file_exists = os.path.isfile(TRADE_RECORD_FILE)

    with open(TRADE_RECORD_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            # 表头：包含原始信号信息 + 实际执行信息 + 资产与风控信息 (新增 update_time)
            writer.writerow([
                "signal_time", "action", "coin", "direction", "event", "signal_price",
                "actual_trade_time", "update_time", "total_equity", "risk_ratio", "target_value", "exec_amount",
                "exec_status", "client_oid", "exchange_oid", "error_msg"
            ])

        # 初始发单时，更新时间 (update_time) 等于实际发单时间 (actual_time)
        writer.writerow([
            row['time'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row['time'], pd.Timestamp) else row['time'],
            row['action'], row['coin'], row['direction'], row['event'], row['price'],
            actual_time, actual_time, total_equity, risk_ratio, target_value, amount, status.value, client_oid,
            exchange_oid, msg
        ])


# ==========================================
# 2. 状态缓存 (解决延迟问题) & 挂单管理
# ==========================================
def sync_and_clean_orders(exchange, open_order_cache):
    """
    1. 同步 trade_records.csv 中的订单状态 (从挂单转为成交或取消)，并刷新 update_time。
    2. 发现 signal_time 超过 1 天的挂单执行撤销，维护撤销状态及更新时间。
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

    # 兼容历史没有 update_time 字段的文件
    if 'update_time' not in df.columns:
        df['update_time'] = df['actual_trade_time']

    # 【核心修复】：强制将可能写入文本的列转换为 object (字符串) 类型。
    # 解决因列全为空值被 Pandas 推断为 float64，导致写入字符串时抛出 TypeError/LossySetitemError 的问题。
    cols_to_cast = ['exec_status', 'error_msg', 'update_time', 'exchange_oid']
    for col in cols_to_cast:
        if col in df.columns:
            df[col] = df[col].astype(object)

    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    has_changes = False

    # 增加聚合统计指标与详情列表，用于实现高密度单行日志
    sync_updates = 0
    cleanups = 0
    synced_details = []
    cleaned_details = []

    # 构建快速检索字典: exchange_oid -> order
    open_order_dict = {}
    for sym, orders in open_order_cache.items():
        for o in orders:
            open_order_dict[str(o.get('id'))] = o

    # 遍历更新订单记录
    for index, row in df.iterrows():
        status = str(row.get('exec_status', ''))
        exch_oid = str(row.get('exchange_oid', ''))
        client_oid = str(row.get('client_oid', ''))
        short_cid = client_oid.split('_')[-1] if '_' in client_oid else client_oid  # 提取尾部用于精简日志显示

        # 定义终态 (无需再向交易所查询的最终状态)
        terminal_states = ['FAIL', 'closed', 'canceled', 'CANCELED_SUCCESS', 'CANCELED_FAIL']
        if status in terminal_states or not exch_oid or exch_oid == 'nan':
            continue

        sym = f"{row['coin']}/USDT:USDT"
        try:
            sig_time = pd.to_datetime(row['signal_time'])
        except Exception:
            continue  # 日期解析异常直接跳过

        # 情况 A: 订单仍在活动挂单缓存中
        if exch_oid in open_order_dict:
            # 校验超时 (大于 1 天)
            if (now - sig_time) > timedelta(days=1):
                try:
                    exchange.cancel_order(exch_oid, sym)
                    df.at[index, 'exec_status'] = 'CANCELED_SUCCESS'
                    df.at[index, 'error_msg'] = '系统自动清理超时(>1天)挂单: 成功'
                    df.at[index, 'update_time'] = now_str
                    # 撤单成功后，将其从内存 cache 剔除，防止本轮后续判断受阻
                    open_order_cache[sym] = [o for o in open_order_cache[sym] if str(o.get('id')) != exch_oid]
                    has_changes = True
                    cleanups += 1
                    cleaned_details.append(f"{short_cid}(OK)")
                except Exception as e:
                    df.at[index, 'exec_status'] = 'CANCELED_FAIL'
                    df.at[index, 'error_msg'] = f'自动清理超时撤单失败: {e}'
                    df.at[index, 'update_time'] = now_str
                    logger.error(f"[CLEANUP] 撤销超时挂单失败 | CID: {client_oid} | 原因: {e}")
                    has_changes = True
                    cleaned_details.append(f"{short_cid}(FAIL)")
            else:
                # 尚未超时，同步状态标记为明确的 open (刚下发时可能是 OK)
                if status != 'open':
                    df.at[index, 'exec_status'] = 'open'
                    df.at[index, 'update_time'] = now_str
                    has_changes = True
                    sync_updates += 1
                    synced_details.append(f"{short_cid}(open)")

        # 情况 B: 订单已不在挂单池中，说明其已成交或被外部取消
        else:
            try:
                # 追溯它的最终状态
                fetched_order = exchange.fetch_order(exch_oid, sym)
                new_status = fetched_order.get('status', status)  # ccxt 标准通常返回 'closed' 或 'canceled'
                if new_status != status:
                    df.at[index, 'exec_status'] = new_status
                    df.at[index, 'update_time'] = now_str
                    has_changes = True
                    sync_updates += 1
                    synced_details.append(f"{short_cid}({new_status})")
            except Exception as e:
                logger.warning(f"[SYNC] 回溯订单状态失败 | CID: {client_oid} | 原因: {e}")

    # 如果发生变化，则回写 CSV (保持全链路一致性)
    if has_changes:
        df.to_csv(TRADE_RECORD_FILE, index=False)

    # 探针输出：无论是否变更，汇总耗时和操作记录。将原本循环里长达几十行的日志压缩成极其干练的 1 行
    cost_ms = (time.perf_counter() - t_start) * 1000
    sync_str = ",".join(synced_details) if synced_details else "无"
    clean_str = ",".join(cleaned_details) if cleaned_details else "无"
    logger.info(
        f"[SYNC] 挂单库巡检完毕 | 耗时: {cost_ms:.1f}ms | 状态流转({sync_updates}笔): {sync_str} | 超时清理({cleanups}笔): {clean_str}")

    return open_order_cache


def preload_account_state(exchange):
    """
    在整点前 1 分钟提前获取并缓存账户权益、所有币种持仓以及当前活动挂单。
    防止整点时网络拥堵，确保整点只做发单动作。
    """
    logger.info(">>> [PRELOAD] 启动资产与挂单预加载...")
    t_start = time.perf_counter()
    eq_cost, pos_cost, ord_cost = 0, 0, 0
    total_equity = 0.0
    position_cache = {}
    open_order_cache = {}

    # 1. 获取总资产 (带重试机制以防偶发网络抖动)
    for attempt in range(3):
        t_eq = time.perf_counter()
        eq_status, total_equity = get_total_equity(exchange)
        if eq_status == ExecStatus.OK:
            eq_cost = (time.perf_counter() - t_eq) * 1000
            break
        logger.warning(f"[PRELOAD] 权益获取延迟/失败 (第{attempt + 1}/3次)，等待1秒后重试...")
        time.sleep(1)

    if total_equity <= 0:
        logger.error("[PRELOAD] 预加载总资产失败或为0，本轮开仓将受限！")

    # 2. 获取全量持仓并构建缓存字典
    try:
        t_pos = time.perf_counter()
        positions = exchange.fetch_positions()
        pos_cost = (time.perf_counter() - t_pos) * 1000
        for pos in positions:
            sym = pos['symbol']  # 格式如 "BTC/USDT:USDT"
            amt = float(pos['info']['positionAmt'])
            if amt != 0:
                position_cache[sym] = amt
    except Exception as e:
        logger.error(f"[PRELOAD] 预加载持仓失败: {e}，平仓操作可能会受阻！")
        position_cache = None  # 标记为失败

    # 3. 获取全量活动挂单并构建缓存字典 (防止重复挂单)
    try:
        t_ord = time.perf_counter()
        # 解决 fetchOpenOrders 无 symbol 拉取全量挂单时的警告拦截
        exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        open_orders = exchange.fetch_open_orders()
        ord_cost = (time.perf_counter() - t_ord) * 1000
        for order in open_orders:
            sym = order['symbol']
            if sym not in open_order_cache:
                open_order_cache[sym] = []
            open_order_cache[sym].append(order)
    except Exception as e:
        logger.error(f"[PRELOAD] 预加载挂单失败: {e}，防重复挂单功能可能会受阻！")
        open_order_cache = None  # 标记为失败

    # 4. 同步并清理历史挂单，过滤完后返回最新的挂单缓存
    if open_order_cache is not None:
        open_order_cache = sync_and_clean_orders(exchange, open_order_cache)

    # 日志聚合探针：将多条分散的就绪日志合并为一条高密度日志，暴露出三大核心 IO 的网络延迟
    pos_len = len(position_cache) if position_cache is not None else 'FAIL'
    ord_len = len(open_order_cache) if open_order_cache is not None else 'FAIL'
    total_cost = (time.perf_counter() - t_start) * 1000
    logger.info(
        f"[PRELOAD] 缓存就绪 | 总权益: {total_equity:.2f} USD | 有效持仓: {pos_len}种 | 存在挂单: {ord_len}种 | 耗时探针(权益/持仓/挂单/总计): {eq_cost:.0f}ms / {pos_cost:.0f}ms / {ord_cost:.0f}ms / {total_cost:.0f}ms")

    return total_equity, position_cache, open_order_cache


# ==========================================
# 3. 核心零延迟执行模块
# ==========================================
def execute_signals_fast(exchange, target_time, total_equity, position_cache, open_order_cache, signal_df):
    """
    极速执行当前整点的信号（纯本地计算 + 直接发单）
    :param target_time: 目标整点时间 (datetime)
    """
    t_start = time.perf_counter()
    target_position_value = total_equity * POSITION_RISK_RATIO
    # 日志聚合：将触发时间和资金风控信息合并为一条极简表头日志，去除微秒中的多余尾数
    logger.info(
        f"========== [EXEC] 准点触发: {datetime.now().strftime('%H:%M:%S.%f')[:-3]} | 总权益: {total_equity:.2f} | 风控限额: {target_position_value:.2f} ({POSITION_RISK_RATIO * 100:.0f}%) ==========")

    if signal_df is None or signal_df.empty:
        logger.info(f"[EXEC] 当前时间点无交易信号 | 文件过滤耗时: {(time.perf_counter() - t_start) * 1000:.2f}ms")
        return
    df = signal_df
    timedelta_minutes = 60
    # 2. 严格筛选当前整点的信号 (容差放宽至前后 1 分钟以防文件生成有微小偏差)
    time_lower = target_time - timedelta(minutes=timedelta_minutes)
    time_upper = target_time + timedelta(minutes=timedelta_minutes)
    current_signals = df[(df['time'] >= time_lower) & (df['time'] <= time_upper)]

    if current_signals.empty:
        logger.info(f"[EXEC] 当前时间点无交易信号 | 文件过滤耗时: {(time.perf_counter() - t_start) * 1000:.2f}ms")
        return

    # 3. 遍历并瞬发信号
    for _, row in current_signals.iterrows():
        try:
            execute_single_signal(exchange, row, total_equity, target_position_value, position_cache, open_order_cache)
        except Exception as e:
            logger.error(f"单次发单异常拦截: {e}")

    logger.info(
        f"[EXEC] 本轮({target_time.strftime('%H:%M')})所有信号处理完毕 | 共匹配 {len(current_signals)} 条 | 模块总耗时: {(time.perf_counter() - t_start) * 1000:.1f}ms")


def execute_single_signal(exchange, row, total_equity, target_position_value, position_cache, open_order_cache):
    coin = str(row['coin']).strip().upper()
    action = str(row['action']).strip().upper()  # BUY / SELL
    direction = str(row['direction']).strip().upper()  # SHORT / LONG
    event = str(row['event']).strip().upper()  # OPEN / CLOSE
    price = float(row['price'])
    symbol = f"{coin}/USDT:USDT"

    # 提取信号时间 (日+时+分)，确保挂单与具体的信号行绝对绑定
    sig_time = row['time'].strftime('%d%H%M') if isinstance(row['time'], pd.Timestamp) else pd.to_datetime(
        row['time']).strftime('%d%H%M')

    # 生成高可读性、防重且与信号时间绑定的 client_oid
    # 格式: {币种}_{方向}_{动作}_{开平}_{时间}_{UUID前4位} => 例: BTC_SHORT_SELL_OPEN_122000_a1b2
    # 此格式结合控制在币安系统限定的 36 字符最大长度之内
    order_prefix = f"{coin}_{direction}_{action}_{event}_{sig_time}"
    uid = uuid.uuid4().hex[:4]
    client_oid = f"{order_prefix}_{uid}"

    # ---------------- 极速状态校验 (纯内存字典查询) ----------------
    if position_cache is None or open_order_cache is None:
        logger.error(f"[EXEC] 致命拦截 | CID: {client_oid} | 原因: 预加载信息失败，放弃校验与下发。")
        return

    # 当前币种本地缓存的真实仓位与挂单信息
    current_pos_amt = position_cache.get(symbol, 0.0)
    symbol_open_orders = open_order_cache.get(symbol, [])

    has_short = current_pos_amt < 0
    has_long = current_pos_amt > 0

    # 【防重复挂单拦截】基于 clientOrderId 精确前缀匹配
    has_duplicate_order = False
    for o in symbol_open_orders:
        o_client_id = o.get('clientOrderId', '')
        if not o_client_id:
            o_client_id = o.get('info', {}).get('clientOrderId', '')

        # 只要当前活动挂单包含相同的该笔信号前缀，说明这笔信号已经被执行过且在排队中
        if o_client_id.startswith(order_prefix):
            has_duplicate_order = True
            break

    if has_duplicate_order:
        logger.warning(f"[EXEC] 重复拦截 | CID: {client_oid} | 发现该信号时段({sig_time})已有排队挂单。")
        return

    # 防呆/仓位拦截 (0 耗时)
    if event == "OPEN":
        if target_position_value <= 0:
            logger.warning(f"[EXEC] 无效开仓 | CID: {client_oid} | 资金预加载为0。")
            return
        if (direction == "SHORT" and has_short) or (direction == "LONG" and has_long):
            logger.warning(
                f"[EXEC] 重复开仓拦截 | CID: {client_oid} | 缓存显示已有 {direction} 仓位 ({current_pos_amt})。")
            return

        raw_amount = target_position_value / price
        amount = float(exchange.amount_to_precision(symbol, raw_amount))
        reduce_only = False

    elif event == "CLOSE":
        if (direction == "SHORT" and not has_short) or (direction == "LONG" and not has_long):
            logger.warning(f"[EXEC] 无效平仓拦截 | CID: {client_oid} | 缓存显示无对应待平仓位。")
            return

        # 平仓：直接取真实持仓全平 (绝对值)
        amount = abs(current_pos_amt)
        # 【核心修复】：双向持仓模式（Hedge Mode）下，API 严格禁止传入 reduce_only = True。
        # 只要带有 position_side 参数，买卖方向做反向操作即可自动平仓，强制设为 False 避免 -1106 拒单。
        reduce_only = False
    else:
        return

    if amount <= 0:
        logger.warning(f"[EXEC] 丢单拦截 | CID: {client_oid} | 计算所得下单量为 0。")
        return

    # ---------------- 发起真实的 API 请求 ----------------
    side = action.lower()
    result = execute_order(
        exchange=exchange,
        symbol=symbol,
        side=side,
        amount=amount,
        client_oid=client_oid,
        order_type='limit',  # 按照 CSV 中指定的价格挂限价单
        price=price,
        reduce_only=reduce_only,
        position_side=direction
    )

    # ---------------- 记录执行结果 ----------------
    record_trade(
        row=row,
        actual_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        total_equity=total_equity,
        risk_ratio=POSITION_RISK_RATIO,
        target_value=target_position_value,
        amount=amount,
        status=result.status,
        client_oid=client_oid,
        exchange_oid=result.exchange_oid,
        msg=result.error_msg
    )

    # 剔除了原先的 [SUCCESS] 重复打印。因为 execute_order 里面已经打出了含完整 CID / EID 的详细发单日志
    # 这里仅在失败时补加一条归档状态的记录即可，避免高频日志刷屏。
    if result.status != ExecStatus.OK:
        logger.error(f"[RECORD] 发单失败/拒绝，状态已归档入库 | CID: {client_oid} | 记录状态: {result.status.value}")

def run_scheduler():
    """
    智能三阶段调度器：
    1. 休眠到 XX:50:00 -> 唤醒执行 execute_trading_bot_workflow 生成最新信号
    2. 休眠到 XX:59:00 -> 唤醒获取并缓存资产和持仓
    3. 休眠到 XX:00:00 -> 精确执行本地文件信号
    """
    API_KEY = get_config('nana_biance_api_key')
    SECRET_KEY = get_config('nana_biance_api_secret')

    logger.info(">>> 初始化交易所实例...")
    try:
        # 代理按需配置
        exchange = init_exchange(API_KEY, SECRET_KEY,
                                 proxies={'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'})
    except Exception:
        logger.critical("交易所初始化失败，程序退出。")
        return

    logger.info(">>> 应用层启动完成！进入调度循环...")

    while True:
        now = datetime.now()

        # 计算下一个整点时间 (如 14:00:00)
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        # 工作流执行时间设为整点前 10 分钟 (如 13:50:00)
        workflow_time = next_hour - timedelta(minutes=10)
        # 预加载时间设为整点前 1 分钟 (如 13:59:00)
        preload_time = next_hour - timedelta(minutes=1)
        # [优化点] 统一计算包含 0.5 秒冗余的目标拔枪时间 (如 14:00:00.500)
        target_exec_time = next_hour + timedelta(seconds=0.5)

        # 阶段一：等待到达信号生成工作流时间 (XX:50:00)
        if now < workflow_time:
            sleep_sec = (workflow_time - now).total_seconds()
            logger.info(f"[SCHEDULER] 休眠 {sleep_sec:.0f}s -> {workflow_time.strftime('%H:%M:%S')} (目标: 信号生成)")
            # [深度优化] 放弃单次大段 sleep，改用每秒检查的细粒度轮询。
            # 防止服务器系统时间 NTP 同步跳跃导致的长眠不醒。
            while datetime.now() < workflow_time:
                time.sleep(1)

        # ----------- 触发信号生成工作流 -----------
        # 再次获取当前时间，为了兼容刚好在 50~59分之间启动程序的补跑逻辑
        now = datetime.now()
        if now < preload_time:
            logger.info(">>> [WORKFLOW] 启动交易机器人工作流 (生成最新信号)...")
            t_wf = time.perf_counter()
            try:
                signal_df = execute_trading_bot_workflow()
                logger.info(f">>> [WORKFLOW] 信号流水线执行完毕！| 耗时: {time.perf_counter() - t_wf:.2f}s")
            except Exception as e:
                logger.error(f">>> [WORKFLOW] 信号生成异常中断: {e} | 耗时: {time.perf_counter() - t_wf:.2f}s")

        # 阶段二：等待到达预加载时间 (XX:59:00)
        now = datetime.now()
        if now < preload_time:
            sleep_sec = (preload_time - now).total_seconds()
            logger.info(f"[SCHEDULER] 休眠 {sleep_sec:.0f}s -> {preload_time.strftime('%H:%M:%S')} (目标: 数据预加载)")
            # [深度优化] 同样使用细粒度休眠，确保精准衔接到 59 分 00 秒
            while datetime.now() < preload_time:
                time.sleep(1)

        # ----------- 触发预加载 -----------
        total_equity, position_cache, open_order_cache = preload_account_state(exchange)

        # 阶段三：精细等待到达目标拔枪时间 (XX:00:00.500)
        now = datetime.now()
        if now < target_exec_time:
            sleep_sec_final = (target_exec_time - now).total_seconds()
            logger.info(f"[SCHEDULER] 资产数据就绪！屏息倒计时 {sleep_sec_final:.1f}s 准备极速拔枪...")

            # [深度优化] 量化级混合精度等待 (普通休眠 + 极速自旋锁)
            while True:
                current_now = datetime.now()
                remaining_time = (target_exec_time - current_now).total_seconds()

                if remaining_time <= 0:
                    break  # 时间到，立即出锁
                elif remaining_time > 0.05:
                    # 距离目标时间大于 50 毫秒，使用短暂 sleep 让出 CPU 给其他系统进程
                    time.sleep(0.01)
                else:
                    # 剩余最后不到 50 毫秒！
                    # 此时严禁使用 time.sleep()。直接执行 pass 进入死循环自旋（Spin-wait）。
                    # 虽然会在这 50 毫秒内霸占单核 100% CPU，但能彻底消除操作系统线程调度造成的延迟，实现真正的微秒级同步。
                    pass

        # Jitter探针：监控 CPU 跳出死循环的精准漂移值，正数代表出锁慢了，负数代表快了
        actual_exit = datetime.now()
        drift_ms = (actual_exit - target_exec_time).total_seconds() * 1000
        logger.info(
            f"========== [SCHEDULER] 破壁出锁 | 实际跳出时间: {actual_exit.strftime('%H:%M:%S.%f')[:-3]} | 自旋误差(Jitter): {drift_ms:+.2f}ms ==========")

        # ----------- 极速拔枪 -----------
        execute_signals_fast(exchange, next_hour, total_equity, position_cache, open_order_cache, signal_df)


if __name__ == "__main__":
    run_scheduler()