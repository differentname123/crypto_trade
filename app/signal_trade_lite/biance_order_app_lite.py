import os
import time
import traceback
import uuid
import pandas as pd
import csv
import platform  # 【修改点】：新增 platform 模块用于判断操作系统环境
from datetime import datetime, timedelta

from common_utils_lite import get_config
from run_cross_signal_lite import execute_trading_bot_workflow
# 从基础文件中导入核心组件 (假设基础文件名为 base_trader.py)
# 确保 base_trader.py 中已经添加了上一轮我给你的 get_total_equity 函数
from biance_order_lite import (
    init_exchange, execute_order, get_total_equity,
    ExecStatus, logger
)

# ==========================================
# 0. 配置与常量
# ==========================================
TRADE_RECORD_FILE = "trade_records.csv"
POSITION_RISK_RATIO = 0.1  # 每次开仓占总资产的 10% (已废弃作为全局开仓比例，仅保留定义防报错，现使用信号自带 max_weight)
LEVRAGE = 1  # 杠杆倍数 (如果需要开杠杆仓位，可以在 execute_order 中使用这个参数)
MIN_ORDER_VALUE = 5.0  # 最小下单金额 (USD)，防止过小订单被拒绝
MAX_ORDER_VALUE = 2000  # 最大下单金额 (USD)，防止仓位太大


# 【修改点 1】：新增全局运行时缓存字典，用于 API 拉取失败时的无缝兜底续命
RUNTIME_FALLBACK = {
    "total_equity": 0.0,
    "position_cache": None,
    "open_order_cache": None
}


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
            # 表头：包含原始信号信息 + 实际执行信息 + 资产与风控信息
            # 【修改点 1】：表头新增 symbol 字段，用于持久化记录交易对
            writer.writerow([
                "signal_time", "action", "coin", "symbol", "direction", "event", "signal_price",
                "actual_trade_time", "update_time", "total_equity", "risk_ratio", "target_value", "exec_amount",
                "exec_status", "client_oid", "exchange_oid", "error_msg"
            ])

        # 【修改点 1 续】：安全提取 signal_df 中的 symbol 字段
        symbol_val = str(row.get('symbol', '')).strip()

        # 初始发单时，更新时间 (update_time) 等于实际发单时间 (actual_time)
        writer.writerow([
            row['time'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(row['time'], pd.Timestamp) else row['time'],
            row['action'], row['coin'], symbol_val, row['direction'], row['event'], row['price'],
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

        # 【修改点 2】：从 CSV 读取精确的 symbol，并向下兼容历史旧数据文件（若旧文件无该列则回退到强拼接）
        sym_val = row.get('symbol')
        if pd.isna(sym_val) or not str(sym_val).strip():
            sym = f"{row['coin']}/USDT:USDT"
        else:
            sym = str(sym_val).strip()

        try:
            sig_time = pd.to_datetime(row['signal_time'])
        except Exception:
            continue  # 日期解析异常直接跳过

        # 情况 A: 订单仍在活动挂单缓存中
        if exch_oid in open_order_dict:
            # 校验超时 (大于 1 天)
            if (now - sig_time) > timedelta(hours=2):
                try:
                    exchange.cancel_order(exch_oid, sym)
                    df.at[index, 'exec_status'] = 'CANCELED_SUCCESS'
                    df.at[index, 'error_msg'] = '系统自动清理超时(>2小时)挂单: 成功'
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
    # 【修改点 2】：引入全局缓存变量
    global RUNTIME_FALLBACK

    logger.info(">>> [PRELOAD] 启动资产与挂单预加载...")
    t_start = time.perf_counter()
    sync_cost, eq_cost, pos_cost, ord_cost = 0, 0, 0, 0
    total_equity = 0.0
    position_cache = {}
    open_order_cache = {}

    # 【修复1】：每次预加载前，强制重新对齐币安服务器时间，消除系统时钟长程漂移 (Error -1021)
    try:
        t_sync = time.perf_counter()
        old_diff = exchange.options.get('timeDifference', 0)
        exchange.load_time_difference()
        new_diff = exchange.options.get('timeDifference', 0)
        sync_cost = (time.perf_counter() - t_sync) * 1000
        # 补全关键日志：直观展示漂移程度与校准耗时
        logger.info(
            f"[PRELOAD] 动态时间偏差重新校准完成 | 漂移补偿: {old_diff}ms -> {new_diff}ms | 耗时: {sync_cost:.0f}ms")
    except Exception as e:
        logger.warning(f"[PRELOAD] 时间偏差动态校准失败，将继续使用旧值: {e}")

    # 1. 获取总资产 (带重试机制以防偶发网络抖动)
    for attempt in range(3):
        t_eq = time.perf_counter()
        eq_status, total_equity = get_total_equity(exchange)
        if eq_status == ExecStatus.OK and total_equity > 0:
            eq_cost = (time.perf_counter() - t_eq) * 1000
            RUNTIME_FALLBACK["total_equity"] = total_equity  # 【修改点 2】：拉取成功则更新缓存
            break
        logger.warning(f"[PRELOAD] 权益获取延迟/失败 (第{attempt + 1}/3次)，等待1秒后重试...")
        time.sleep(1)

    # 【修改点 2】：失败时尝试使用缓存兜底
    if total_equity <= 0:
        if RUNTIME_FALLBACK["total_equity"] > 0:
            total_equity = RUNTIME_FALLBACK["total_equity"]
            logger.warning(f"!!! [PRELOAD] 预加载总资产失败，已启用历史权益缓存续命: {total_equity:.2f} !!!")
        else:
            logger.error("[PRELOAD] 预加载总资产彻底失败或为0，且无可用历史缓存，本轮开仓将受限！")

    # 2. 获取全量持仓并构建缓存字典 (【修复2】：增加 3 次重试机制)
    position_success = False
    for attempt in range(3):
        try:
            t_pos = time.perf_counter()
            positions = exchange.fetch_positions()
            pos_cost = (time.perf_counter() - t_pos) * 1000
            for pos in positions:
                sym = pos['symbol']  # 格式如 "BTC/USDT:USDT"
                amt = float(pos['info']['positionAmt'])
                if amt != 0:
                    position_cache[sym] = amt
            position_success = True
            RUNTIME_FALLBACK["position_cache"] = position_cache  # 【修改点 2】：拉取成功则更新缓存
            break
        except Exception as e:
            logger.warning(f"[PRELOAD] 拉取持仓失败 (第{attempt + 1}/3次): {e}")
            time.sleep(1)

    # 【修改点 2】：失败时尝试使用缓存兜底
    if not position_success:
        if RUNTIME_FALLBACK["position_cache"] is not None:
            position_cache = RUNTIME_FALLBACK["position_cache"]
            logger.warning("!!! [PRELOAD] 预加载持仓失败，已启用历史持仓缓存续命（接受潜在错配风险） !!!")
        else:
            logger.error("[PRELOAD] 预加载持仓彻底失败，且无可用历史缓存，平仓操作可能会受阻！")
            position_cache = None  # 标记为失败

    # 3. 获取全量活动挂单并构建缓存字典 (防止重复挂单) (【修复2】：增加 3 次重试机制)
    order_success = False
    for attempt in range(3):
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
            order_success = True
            break
        except Exception as e:
            logger.warning(f"[PRELOAD] 拉取挂单失败 (第{attempt + 1}/3次): {e}")
            time.sleep(1)

    # 【修改点 2】：失败时尝试使用缓存兜底
    if not order_success:
        if RUNTIME_FALLBACK["open_order_cache"] is not None:
            open_order_cache = RUNTIME_FALLBACK["open_order_cache"]
            logger.warning("!!! [PRELOAD] 预加载挂单失败，已启用历史挂单缓存续命 !!!")
        else:
            logger.error("[PRELOAD] 预加载挂单彻底失败，防重复挂单功能可能会受阻！")
            open_order_cache = None  # 标记为失败

    # 4. 同步并清理历史挂单，过滤完后返回最新的挂单缓存
    if open_order_cache is not None:
        open_order_cache = sync_and_clean_orders(exchange, open_order_cache)
        RUNTIME_FALLBACK["open_order_cache"] = open_order_cache  # 同步后更新最新状态的缓存

    # 日志专点修改：补全了对时耗时，在预加载就绪日志中增加默认杠杆展示
    pos_len = len(position_cache) if position_cache is not None else 'FAIL'
    ord_len = len(open_order_cache) if open_order_cache is not None else 'FAIL'
    total_cost = (time.perf_counter() - t_start) * 1000
    logger.info(
        f"[PRELOAD] 缓存就绪 | 总权益: {total_equity:.2f} USD | 默认杠杆: {LEVRAGE}x | 有效持仓: {pos_len}种 | 存在挂单: {ord_len}种 | 耗时探针(对时/权益/持仓/挂单/总计): {sync_cost:.0f}ms / {eq_cost:.0f}ms / {pos_cost:.0f}ms / {ord_cost:.0f}ms / {total_cost:.0f}ms")

    return total_equity, position_cache, open_order_cache


# ==========================================
# 3. 核心零延迟执行模块
# ==========================================
def execute_signals_fast(exchange, target_time, total_equity, position_cache, open_order_cache, signal_df):
    """
    极速执行当前整点的信号（纯本地计算 +直接发单）
    :param target_time: 目标整点时间 (datetime)
    """
    t_start = time.perf_counter()

    # 【本次修改点】：去除了统一计算 target_position_value，并将开仓限额逻辑转移至内部各信号按照各自的 max_weight 单独计算
    logger.info(
        f"========== [EXEC] 准点触发: {datetime.now().strftime('%H:%M:%S.%f')[:-3]} | 总权益: {total_equity:.2f} | 杠杆倍数: {LEVRAGE}x | 风控限额(含杠杆): 根据各信号 max_weight 独立计算 ==========")

    if signal_df is None or signal_df.empty:
        logger.info(f"[EXEC] 当前时间点无交易信号 | 文件过滤耗时: {(time.perf_counter() - t_start) * 1000:.2f}ms")
        return
    df = signal_df

    # 【修复3】：严格筛选当前整点的信号 (容差缩紧至前后 1 分钟)
    timedelta_minutes = 1
    time_lower = target_time - timedelta(minutes=timedelta_minutes)
    time_upper = target_time + timedelta(minutes=timedelta_minutes)
    current_signals = df[(df['time'] >= time_lower) & (df['time'] <= time_upper)]

    if current_signals.empty:
        logger.info(f"[EXEC] 当前时间点无交易信号 | 文件过滤耗时: {(time.perf_counter() - t_start) * 1000:.2f}ms")
        return

    # 3. 遍历并瞬发信号
    for _, row in current_signals.iterrows():
        try:
            # 【本次修改点】：去除了传入公共全局的风控限额 target_position_value
            execute_single_signal(exchange, row, total_equity, position_cache, open_order_cache)
        except Exception as e:
            logger.error(f"单次发单异常拦截: {e}")

    logger.info(
        f"[EXEC] 本轮({target_time.strftime('%H:%M')})所有信号处理完毕 | 共匹配 {len(current_signals)} 条 | 模块总耗时: {(time.perf_counter() - t_start) * 1000:.1f}ms")


def execute_single_signal(exchange, row, total_equity, position_cache, open_order_cache):
    coin = str(row['coin']).strip().upper()
    action = str(row['action']).strip().upper()  # BUY / SELL
    direction = str(row['direction']).strip().upper()  # SHORT / LONG
    event = str(row['event']).strip().upper()  # OPEN / CLOSE
    price = float(row['price'])

    # 【本次修改点】：直接提取信号行中对应的仓位权重，并依此计算本信号特有的风控开仓额度
    max_weight = float(row['max_weight'])
    target_position_value = total_equity * LEVRAGE * max_weight

    # 输入详细的日志
    logger.info(f"[EXEC] 信号解析 | 币种: {coin} | 方向: {direction} | 动作: {action} | 开平: {event} | 价格: {price:.2f} | 权重: {max_weight:.3f} | 目标开仓额度: {target_position_value:.2f} 总资产: {total_equity:.2f} | 杠杆倍数: {LEVRAGE}x 目标权重: {max_weight:.3f}")


    # 【修改点 3】：直接提取 signal_df 中的精准 symbol 字段（移除对 USDT 的硬编码）
    symbol = str(row['symbol']).strip()

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
        logger.error(f"[EXEC] 致命拦截 | CID: {client_oid} | 原因: 预加载信息失败且无可用缓存，放弃校验与下发。")
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
            logger.warning(f"[EXEC] 无效开仓 | CID: {client_oid} | 资金预加载为0或目标权重异常。")
            return

        target_position_value = min(
            max(target_position_value, MIN_ORDER_VALUE),
            MAX_ORDER_VALUE,
        )

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
        risk_ratio=max_weight,  # 【本次修改点】：不再写入全局的 POSITION_RISK_RATIO，而是存入信号本身独有的 max_weight
        target_value=target_position_value,
        amount=amount,
        status=result.status,
        client_oid=client_oid,
        exchange_oid=result.exchange_oid,
        msg=result.error_msg
    )

    # 剔除了原先的 [SUCCESS] 重复打印。因为 execute_order 里面已经打出了含完整 CID / EID 的详细发单日志
    # 这里仅在失败时补加一条归档状态的记录即可，避免高频日志刷屏。
    # 日志专点修改：在发单失败的底层归档日志中指出对应的杠杆倍数情况
    if result.status != ExecStatus.OK:
        logger.error(
            f"[RECORD] 发单失败/拒绝，状态已归档入库 | CID: {client_oid} | 记录状态: {result.status.value} | 杠杆倍数: {LEVRAGE}x")


def safe_init_exchange(api_key, secret_key, proxies):
    """
    健壮性封装 1：具备无限重试能力的交易所初始化
    防止程序在无人值守启动时，因短暂的网络断开而直接死亡。
    """
    retry_interval = 5
    while True:
        try:
            logger.info(">>> 尝试初始化交易所实例...")
            exchange = init_exchange(api_key, secret_key, proxies=proxies)
            logger.info(">>> 交易所初始化成功！")
            return exchange
        except Exception as e:
            logger.error(f"[初始化失败] 网络或 API 异常: {e}。{retry_interval} 秒后重试...")
            time.sleep(retry_interval)
            # 指数退避：每次重试等待时间延长，最高不超过 60 秒
            retry_interval = min(retry_interval * 2, 60)


def run_scheduler():
    """
    高可用调度器（极简且强健）：
    包含 API 局部重试、全局异常兜底与错误隔离。
    """
    API_KEY = get_config('nana_biance_api_copy_key')
    SECRET_KEY = get_config('nana_biance_api_copy_secret')

    # 【修改点】：动态环境检测判断是否启用代理
    current_os = platform.system().lower()
    if current_os == 'linux':
        PROXIES = None
        proxy_url = None
        logger.info(f">>> [环境检测] 当前系统: Linux，判定为云端环境，已禁用代理。")
    else:
        PROXIES = {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'}
        proxy_url = 'http://127.0.0.1:7890'
        logger.info(f">>> [环境检测] 当前系统: {current_os.capitalize()}，判定为本地环境，已启用代理: {proxy_url}。")

    # 1. 永不宕机的初始化 (依据环境自适应传入代理状态)
    exchange = safe_init_exchange(API_KEY, SECRET_KEY, PROXIES)

    logger.info(">>> 调度系统已就绪，进入 7x24 小时主循环...")

    while True:
        try:
            now = datetime.now()

            # --- 时间锚点 ---
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            # 2. 减去 1 分钟得到 target_time
            target_time = next_hour - timedelta(minutes=1)
            target_time_str = target_time.strftime("%Y-%m-%d %H:%M")

            preload_time = next_hour - timedelta(minutes=5)

            # --- 阶段一：等待预加载 ---
            if now < preload_time:
                sleep_sec = (preload_time - now).total_seconds()
                logger.info(
                    f"[SCHEDULER] 静态休眠 ({sleep_sec:.0f}s) -> 预计 {preload_time.strftime('%H:%M:%S')} 唤醒预加载")
                while datetime.now() < preload_time:
                    time.sleep(1)

            # --- 阶段二：带容错的数据预加载 ---
            # 健壮性封装 2：局部重试。允许拉取资产失败 3 次，防止偶发网络抖动破坏整点交易
            total_equity, position_cache, open_order_cache = None, None, None
            preload_success = False
            for attempt in range(1, 4):
                try:
                    logger.info(f">>> [SCHEDULER] 尝试缓存账户资产 (第 {attempt}/3 次)...")
                    total_equity, position_cache, open_order_cache = preload_account_state(exchange)

                    # 【修改点 3】：修复原有直接赋值 True 被异常吞掉的漏洞。强制校验三个核心数据是否已可用（拉取成功或已启用缓存）
                    if total_equity > 0 and position_cache is not None and open_order_cache is not None:
                        preload_success = True
                        logger.info(">>> [SCHEDULER] 资产数据已就绪（实时拉取或成功应用兜底缓存）。")
                        break  # 成功则跳出重试循环
                    else:
                        logger.warning(f"[预加载警告] 第 {attempt} 次获取资产未能拿到完整数据或缓存，等待3秒后重试...")
                        time.sleep(3)
                except Exception as e:
                    logger.warning(f"[预加载警告] 第 {attempt} 次发生未预料异常: {e}")
                    time.sleep(3)  # 失败后缓 3 秒再试

            if not preload_success:
                logger.error("!!! [预加载严重错误] 连续 3 次拉取失败且无兜底数据支撑！为保障安全，放弃本轮调度。")
                time.sleep(60)  # 休息一分钟，直接进入下一个周期的 while 循环
                continue

            # --- 阶段三：控制权移交 (信号计算与等待) ---
            logger.info(f">>> [SCHEDULER] 进入信号流水线，移交控制权等待目标时间 {target_time_str} ...")
            t_wf = time.perf_counter()

            # 工作流内部自主决定等待 K 线闭合并生成信号 (自适应传入对应的 proxy_url)
            signal_df = execute_trading_bot_workflow(target_time_str, proxy_url=proxy_url)

            logger.info(f">>> [WORKFLOW] 流水线执行完毕！耗时: {time.perf_counter() - t_wf:.2f}s")

            # --- 阶段四：极速下单 ---
            if signal_df is not None and not signal_df.empty:
                logger.info(">>> [SCHEDULER] 收到有效信号，触发下单...")
                execute_signals_fast(exchange, next_hour, total_equity, position_cache, open_order_cache, signal_df)
            else:
                logger.info(">>> [SCHEDULER] 本轮无交易信号。")

        # 健壮性封装 3：全局终极兜底
        except Exception as e:
            # 捕获一切未预料到的致命错误，防止 while True 循环崩溃跳出
            logger.error(f"!!! [SCHEDULER 主循环发生致命异常] !!!", exc_info=True)
            logger.error(traceback.format_exc())  # 打印完整错误堆栈以便复盘调试

            # 发生严重错误时，强制休眠 30 秒，防止死循环疯狂报错打满日志或导致 IP 被封
            logger.info("系统将强制休眠 30 秒后尝试自我恢复...")
            time.sleep(30)


if __name__ == "__main__":
    run_scheduler()