import os
import time
import uuid
import pandas as pd
import csv
from datetime import datetime, timedelta

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
def record_trade(row, actual_time, amount, status, client_oid, exchange_oid, msg=""):
    """
    将原始信号参数与实际交易结果合并持久化，实现全链路对账。
    """
    file_exists = os.path.isfile(TRADE_RECORD_FILE)

    with open(TRADE_RECORD_FILE, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if not file_exists:
            # 表头：包含原始信号信息 + 实际执行信息
            writer.writerow([
                "signal_time", "action", "coin", "direction", "event", "signal_price",
                "actual_trade_time", "exec_amount", "exec_status", "client_oid", "exchange_oid", "error_msg"
            ])

        writer.writerow([
            row['time'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row['time'], pd.Timestamp) else row['time'],
            row['action'], row['coin'], row['direction'], row['event'], row['price'],
            actual_time, amount, status.value, client_oid, exchange_oid, msg
        ])


# ==========================================
# 2. 状态缓存 (解决延迟问题)
# ==========================================
def preload_account_state(exchange):
    """
    在整点前 1 分钟提前获取并缓存账户权益和所有币种持仓。
    防止整点时网络拥堵，确保整点只做发单动作。
    """
    logger.info(">>> [PRELOAD] 开始提前预加载账户资产与持仓缓存...")
    total_equity = 0.0
    position_cache = {}

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

    return total_equity, position_cache


# ==========================================
# 3. 核心零延迟执行模块
# ==========================================
def execute_signals_fast(exchange, target_time, total_equity, position_cache):
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
    timedelta_minutes = 60 * 24 * 10
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
            execute_single_signal(exchange, row, target_position_value, position_cache)
        except Exception as e:
            logger.error(f"单次发单异常拦截: {e}")


def execute_single_signal(exchange, row, target_position_value, position_cache):
    coin = str(row['coin']).strip().upper()
    action = str(row['action']).strip().upper()  # BUY / SELL
    direction = str(row['direction']).strip().upper()  # SHORT / LONG
    event = str(row['event']).strip().upper()  # OPEN / CLOSE
    price = float(row['price'])
    symbol = f"{coin}/USDT:USDT"

    # 生成高可读性、利于排查的 client_oid
    # 格式: {币种}_{方向}_{动作}_{开平}_{UUID前4位} => 例: BTC_SHORT_SELL_OPEN_a1b2
    uid = uuid.uuid4().hex[:4]
    client_oid = f"{coin}_{direction}_{action}_{event}_{uid}"

    # ---------------- 极速状态校验 (纯内存字典查询) ----------------
    if position_cache is None:
        logger.error(f"[{client_oid}] 致命错误: 预加载持仓失败，无法校验状态，放弃此单。")
        return

    # 当前币种本地缓存的真实仓位
    current_pos_amt = position_cache.get(symbol, 0.0)
    has_short = current_pos_amt < 0
    has_long = current_pos_amt > 0

    # 防呆/防重发拦截 (0 耗时)
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


# ==========================================
# 4. 高效调度器
# ==========================================
def run_scheduler():
    """
    智能双阶段调度器：
    1. 休眠到 XX:59:00 -> 唤醒获取并缓存资产和持仓
    2. 休眠到 XX:00:00 -> 精确执行本地文件信号
    """
    API_KEY = get_config('nana_biance_api_key')
    SECRET_KEY = get_config('nana_biance_api_secret')

    logger.info(">>> 初始化交易所实例...")
    try:
        # 代理按需配置
        exchange = init_exchange(API_KEY, SECRET_KEY, proxies={'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'})
    except Exception:
        logger.critical("交易所初始化失败，程序退出。")
        return
    # total_equity, position_cache = preload_account_state(exchange)

    logger.info(">>> 应用层启动完成！进入调度循环...")

    while True:
        now = datetime.now()

        # 计算下一个整点时间 (如 14:00:00)
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        # 预加载时间设为整点前 1 分钟 (如 13:59:00)
        preload_time = next_hour - timedelta(minutes=1)

        # # 阶段一：等待到达预加载时间
        # if now < preload_time:
        #     sleep_sec = (preload_time - now).total_seconds()
        #     logger.info(f"睡眠 {sleep_sec:.0f}s，等待数据预加载时间: {preload_time.strftime('%H:%M:%S')}")
        #     time.sleep(sleep_sec)

        # ----------- 触发预加载 -----------
        total_equity, position_cache = preload_account_state(exchange)

        # # 阶段二：精细等待到达整点 (XX:00:00)
        # now = datetime.now()
        # if now < next_hour:
        # 增加 0.5 秒的微小冗余，确保上游的 CSV 文件在整点准时生成并完全刷入磁盘
        sleep_sec_final = (next_hour - now).total_seconds() + 0.5
        # logger.info(f"缓存完毕！屏息倒计时 {sleep_sec_final:.1f}s 准备拔枪...")
        # time.sleep(sleep_sec_final)

        # ----------- 极速拔枪 -----------
        execute_signals_fast(exchange, next_hour, total_equity, position_cache)


if __name__ == "__main__":
    run_scheduler()