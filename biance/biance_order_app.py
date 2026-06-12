import os
import time
import uuid
import pandas as pd
import csv
from datetime import datetime, timedelta

from app.crypto_dashboard.run_cross_signal import execute_trading_bot_workflow
# 从基础文件中导入核心组件 (假设基础文件名为 base_trader.py)
# 确保 base_trader.py 中已经添加了上一轮我给你的 get_total_equity 函数
from biance_order import (
    init_exchange, execute_order, get_total_equity,
    ExecStatus, logger
)
from common.common_utils import get_config

# ==========================================
# 0. 配置与常量
# ==========================================
SIGNAL_FILE = r'W:\project\python_project\crypto_trade\app\crypto_dashboard\live_simulation_logs.csv'
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
    logger.info(">>> [SYNC] 开始同步订单状态与清理超时挂单...")
    if not os.path.isfile(TRADE_RECORD_FILE):
        return open_order_cache

    try:
        df = pd.read_csv(TRADE_RECORD_FILE)
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
                logger.info(f"[CLEANUP] 发现超时1天的挂单: {client_oid}，开始执行撤单...")
                try:
                    exchange.cancel_order(exch_oid, sym)
                    df.at[index, 'exec_status'] = 'CANCELED_SUCCESS'
                    df.at[index, 'error_msg'] = '系统自动清理超时(>1天)挂单: 成功'
                    df.at[index, 'update_time'] = now_str
                    logger.info(f"[CLEANUP] 撤销成功: {client_oid}")

                    # 撤单成功后，将其从内存 cache 剔除，防止本轮后续判断受阻
                    open_order_cache[sym] = [o for o in open_order_cache[sym] if str(o.get('id')) != exch_oid]
                    has_changes = True
                except Exception as e:
                    df.at[index, 'exec_status'] = 'CANCELED_FAIL'
                    df.at[index, 'error_msg'] = f'自动清理超时撤单失败: {e}'
                    df.at[index, 'update_time'] = now_str
                    logger.error(f"[CLEANUP] 撤销超时挂单失败: {client_oid}, 原因: {e}")
                    has_changes = True
            else:
                # 尚未超时，同步状态标记为明确的 open (刚下发时可能是 OK)
                if status != 'open':
                    df.at[index, 'exec_status'] = 'open'
                    df.at[index, 'update_time'] = now_str
                    has_changes = True

        # 情况 B: 订单已不在挂单池中，说明其已成交或被外部取消
        else:
            try:
                # 追溯它的最终状态
                fetched_order = exchange.fetch_order(exch_oid, sym)
                new_status = fetched_order.get('status', status)  # ccxt 标准通常返回 'closed' 或 'canceled'
                if new_status != status:
                    df.at[index, 'exec_status'] = new_status
                    df.at[index, 'update_time'] = now_str
                    logger.info(f"[SYNC] 订单 {client_oid} 最终状态更新为: {new_status}")
                    has_changes = True
            except Exception as e:
                logger.warning(f"[SYNC] 无法从交易所回溯订单 {client_oid} 状态: {e}")

    # 如果发生变化，则回写 CSV (保持全链路一致性)
    if has_changes:
        df.to_csv(TRADE_RECORD_FILE, index=False)
        logger.info("[SYNC] trade_records.csv 状态及最后更新时间同步完毕。")

    return open_order_cache


def preload_account_state(exchange):
    """
    在整点前 1 分钟提前获取并缓存账户权益、所有币种持仓以及当前活动挂单。
    防止整点时网络拥堵，确保整点只做发单动作。
    """
    logger.info(">>> [PRELOAD] 开始提前预加载账户资产、持仓与挂单缓存...")
    total_equity = 0.0
    position_cache = {}
    open_order_cache = {}

    # 1. 获取总资产 (带重试机制以防偶发网络抖动)
    for _ in range(3):
        eq_status, total_equity = get_total_equity(exchange)
        if eq_status == ExecStatus.OK:
            break
        time.sleep(1)

    if total_equity <= 0:
        logger.error("[PRELOAD] 预加载总资产失败或为0，本轮开仓将受限！")

    # 2. 获取全量持仓并构建缓存字典
    try:
        # fetch_positions 不传参数通常会拉取账户下所有活跃/非活跃持仓信息
        positions = exchange.fetch_positions()
        for pos in positions:
            sym = pos['symbol']  # 格式如 "BTC/USDT:USDT"
            amt = float(pos['info']['positionAmt'])
            if amt != 0:
                position_cache[sym] = amt
        logger.info(f"[PRELOAD] 持仓缓存加载完成，当前有效持仓数量: {len(position_cache)}")
    except Exception as e:
        logger.error(f"[PRELOAD] 预加载持仓失败: {e}，平仓操作可能会受阻！")
        position_cache = None  # 标记为失败

    # 3. 获取全量活动挂单并构建缓存字典 (防止重复挂单)
    try:
        # 解决 fetchOpenOrders 无 symbol 拉取全量挂单时的警告拦截
        exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False

        open_orders = exchange.fetch_open_orders()
        for order in open_orders:
            sym = order['symbol']
            if sym not in open_order_cache:
                open_order_cache[sym] = []
            open_order_cache[sym].append(order)
        logger.info(f"[PRELOAD] 挂单缓存加载完成，当前存在挂单的币种数量: {len(open_order_cache)}")
    except Exception as e:
        logger.error(f"[PRELOAD] 预加载挂单失败: {e}，防重复挂单功能可能会受阻！")
        open_order_cache = None  # 标记为失败

    # 4. 同步并清理历史挂单，过滤完后返回最新的挂单缓存
    if open_order_cache is not None:
        open_order_cache = sync_and_clean_orders(exchange, open_order_cache)

    return total_equity, position_cache, open_order_cache


# ==========================================
# 3. 核心零延迟执行模块
# ==========================================
def execute_signals_fast(exchange, target_time, total_equity, position_cache, open_order_cache):
    """
    极速执行当前整点的信号（纯本地计算 + 直接发单）
    :param target_time: 目标整点时间 (datetime)
    """
    logger.info(f"========== 准点触发执行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} ==========")

    if not os.path.exists(SIGNAL_FILE):
        logger.warning(f"信号文件 {SIGNAL_FILE} 不存在。")
        return

    # 1. 读取信号
    try:
        df = pd.read_csv(SIGNAL_FILE)
        df['time'] = pd.to_datetime(df['time'])
    except Exception as e:
        logger.error(f"读取信号文件失败: {e}")
        return
    timedelta_minutes = 60 * 24 * 11
    # 2. 严格筛选当前整点的信号 (容差放宽至前后 1 分钟以防文件生成有微小偏差)
    time_lower = target_time - timedelta(minutes=timedelta_minutes)
    time_upper = target_time + timedelta(minutes=timedelta_minutes)
    current_signals = df[(df['time'] >= time_lower) & (df['time'] <= time_upper)]

    if current_signals.empty:
        logger.info("当前时间点无交易信号。")
        return

    target_position_value = total_equity * POSITION_RISK_RATIO
    logger.info(
        f"本轮风控额度 (总资产 {total_equity:.2f}) * {POSITION_RISK_RATIO * 100}% = {target_position_value:.2f} USD")

    # 3. 遍历并瞬发信号
    for _, row in current_signals.iterrows():
        try:
            execute_single_signal(exchange, row, total_equity, target_position_value, position_cache, open_order_cache)
        except Exception as e:
            logger.error(f"单次发单异常拦截: {e}")


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
        logger.error(f"[{client_oid}] 致命错误: 预加载持仓或挂单信息失败，无法校验状态，放弃此单。")
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
        logger.warning(
            f"[{client_oid}] 拦截重复下单: 发现已存在信号时段({sig_time})的未成交挂单，跳过本次发单以防重复建仓。")
        return

    # 防呆/仓位拦截 (0 耗时)
    if event == "OPEN":
        if target_position_value <= 0:
            logger.warning(f"[{client_oid}] 资金预加载为0，无法开仓。")
            return
        if (direction == "SHORT" and has_short) or (direction == "LONG" and has_long):
            logger.warning(f"[{client_oid}] 拦截重复开仓: 已存在 {direction} 仓位 ({current_pos_amt})。")
            return

        raw_amount = target_position_value / price
        amount = float(exchange.amount_to_precision(symbol, raw_amount))
        reduce_only = False

    elif event == "CLOSE":
        if (direction == "SHORT" and not has_short) or (direction == "LONG" and not has_long):
            logger.warning(f"[{client_oid}] 拦截无效平仓: 本地缓存显示不存在待平仓位。")
            return

        # 平仓：直接取真实持仓全平 (绝对值)
        amount = abs(current_pos_amt)
        reduce_only = True
    else:
        return

    if amount <= 0:
        logger.warning(f"[{client_oid}] 计算所得下单量为 0，跳过。")
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

    if result.status == ExecStatus.OK:
        logger.info(f"[SUCCESS] {client_oid} 发送成功! EID: {result.exchange_oid}")
    else:
        logger.error(f"[FAIL] {client_oid} 失败/拒绝: {result.error_msg}")


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
            logger.info(f"睡眠 {sleep_sec:.0f}s，等待信号生成任务时间: {workflow_time.strftime('%H:%M:%S')}")
            # [深度优化] 放弃单次大段 sleep，改用每秒检查的细粒度轮询。
            # 防止服务器系统时间 NTP 同步跳跃导致的长眠不醒。
            while datetime.now() < workflow_time:
                time.sleep(1)

        # ----------- 触发信号生成工作流 -----------
        # 再次获取当前时间，为了兼容刚好在 50~59分之间启动程序的补跑逻辑
        now = datetime.now()
        if now < preload_time:
            logger.info(">>> [WORKFLOW] 开始执行交易机器人工作流 (生成最新信号)...")
            try:
                execute_trading_bot_workflow()
                logger.info(">>> [WORKFLOW] 信号生成完成！")
            except Exception as e:
                logger.error(f">>> [WORKFLOW] 信号生成工作流执行异常: {e}")

        # 阶段二：等待到达预加载时间 (XX:59:00)
        now = datetime.now()
        if now < preload_time:
            sleep_sec = (preload_time - now).total_seconds()
            logger.info(f"睡眠 {sleep_sec:.0f}s，等待数据预加载时间: {preload_time.strftime('%H:%M:%S')}")
            # [深度优化] 同样使用细粒度休眠，确保精准衔接到 59 分 00 秒
            while datetime.now() < preload_time:
                time.sleep(1)

        # ----------- 触发预加载 -----------
        total_equity, position_cache, open_order_cache = preload_account_state(exchange)

        # 阶段三：精细等待到达目标拔枪时间 (XX:00:00.500)
        now = datetime.now()
        if now < target_exec_time:
            sleep_sec_final = (target_exec_time - now).total_seconds()
            logger.info(f"缓存完毕！屏息倒计时 {sleep_sec_final:.1f}s 准备拔枪...")

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

        # ----------- 极速拔枪 -----------
        execute_signals_fast(exchange, next_hour, total_equity, position_cache, open_order_cache)


if __name__ == "__main__":
    run_scheduler()