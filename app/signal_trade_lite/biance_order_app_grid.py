# -*- coding: utf-8 -*-
"""
等比网格交易引擎 —— 事件驱动 + 单写者状态机 + WAL 账本 + 对账自愈

分层拓扑:
  基础设施层  : GridLedger(CSV/WAL)  |  ExchangeBroker(收拢全部 CCXT 调用)
  值对象/工具 : Enum | OrderEvent | ParsedOid | GridConfig | NodeContext | OidCodec
  领域层      : GridNode(纯内存状态机, 仅主线程可写)
  对账/调度层 : ReconciliationEngine(只读产事件) | GridStrategy(路由+主循环) | ReconcilerThread

并发安全基石 —— 单一写者原则:
  节点内存的一切修改只发生在主线程 process_event 中;
  看门狗线程只「读 + 查 + 投递事件」, 状态迁移交回主线程串行执行,
  过时 OID 产出的事件在主线程被幂等拦截丢弃, 从根源消除竞态。
"""
import os
import csv
import time
import queue
import platform
import threading
from enum import Enum
from datetime import datetime

import multiprocessing

from app.signal_trade_lite.biance_order_lite import (
    safe_init_exchange, fetch_market_precision, format_price_amount,
    execute_order, ExecStatus, fetch_single_order,
)
from app.signal_trade_lite.common_utils_lite import setup_logger, get_config

logger = setup_logger(app_name="GridTrader")


# ==========================================
# 0. 可调参数 (消灭魔术数字)
# ==========================================
RECONCILE_SKIP_WINDOW_SEC = 5.0    # 运行时对账跳过"刚动过"的节点, 规避 REST 快照传播延迟造成的误判
POINT_CHECK_DELAY_COLD = 0.05      # 冷启动点查限流
POINT_CHECK_DELAY_RUNTIME = 0.1    # 运行时点查限流
PLACE_THROTTLE_SEC = 0.05          # 批量铺单限流
INIT_SETTLE_WAIT_SEC = 1.0         # 铺单后等待撮合/网络传播的缓冲
COLD_START_BACKTRACK = 3           # 冷启动每个节点回溯的历史单号数量


# ==========================================
# 1. 枚举与值对象 (类型安全 + 去魔法字符串)
# ==========================================
class NodeState(Enum):
    INIT = "INIT"
    WAIT_OPEN = "WAIT_OPEN"
    WAIT_CLOSE = "WAIT_CLOSE"
    ERROR = "ERROR"


class OrderAction(Enum):
    BUY = "B"
    SELL = "S"


class OrderStatus(Enum):
    FILLED = "FILLED"
    CANCELED = "CANCELED"


class ParsedOid:
    """client_oid 解析结果的结构化载体。"""

    def __init__(self, strategy_id, node_id, action, cycle):
        self.strategy_id = strategy_id
        self.node_id = node_id
        self.action = action
        self.cycle = cycle


class OrderEvent:
    """事件总线上流转的标准订单事件 (取代原始 dict, 语义收敛)。"""

    def __init__(self, client_oid, status, fill_price=0.0, fill_qty=0.0):
        self.client_oid = client_oid
        self.status = status
        self.fill_price = fill_price
        self.fill_qty = fill_qty


class GridConfig:
    """策略配置: 集中管理原本散落在 main 里的参数。"""

    def __init__(self, strategy_id, symbol, min_price, max_price, price_ratio, quantity):
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.min_price = min_price
        self.max_price = max_price
        self.price_ratio = price_ratio
        self.quantity = quantity


class NodeContext:
    """注入给每个 Node 的运行环境, 让状态机方法拥有干净签名。"""

    def __init__(self, broker, ledger, strategy_id):
        self.broker = broker
        self.ledger = ledger
        self.strategy_id = strategy_id


class OidCodec:
    """
    client_oid 编解码中枢: 全项目不再出现裸 split('_')。
    格式约定: GD_{strategy}_{node}_{action}_{cycle}_{ms后缀}
    约束: strategy_id / node_id 不含下划线 (当前 S01 / N001 满足)。
    """
    PREFIX = "GD"

    @classmethod
    def build(cls, strategy_id, node_id, action, cycle):
        # 毫秒后缀确保反复重挂时单号天然唯一
        ts_suffix = str(int(time.time() * 1000))[-8:]
        return f"{cls.PREFIX}_{strategy_id}_{node_id}_{action.value}_{cycle}_{ts_suffix}"

    @classmethod
    def parse(cls, oid):
        parts = oid.split('_')
        if len(parts) < 6 or parts[0] != cls.PREFIX:
            return None
        try:
            return ParsedOid(parts[1], parts[2], OrderAction(parts[3]), int(parts[4]))
        except ValueError:
            # 非法 action 或非数字 cycle: 视为不可路由订单
            return None

    @classmethod
    def prefix_for(cls, strategy_id):
        return f"{cls.PREFIX}_{strategy_id}_"


# ==========================================
# 2. 基础设施层 (Ledger / Broker)
# ==========================================
class GridLedger:
    """
    追加式领域事件账本 (Append-Only / WAL)。
    标准库 csv 同步写入即审计真相, 遵循"先落盘, 再动作"; 逐行细节仅落 DEBUG, 避免 INFO 噪音。
    """
    COLUMNS = ["ts", "node_id", "cycle", "action", "client_oid", "price", "amount", "status", "msg"]

    def __init__(self, strategy_id):
        self.filename = f"grid_ledger_{strategy_id}.csv"
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(self.COLUMNS)

    def append(self, node_id, cycle, action, client_oid, price, amount, status, msg=""):
        """同步写入一行领域事件 (WAL 落盘), 并留 DEBUG 级细粒度轨迹。"""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(self.filename, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([ts, node_id, cycle, action, client_oid, price, amount, status, msg])
        logger.debug(f"[LEDGER] {action} {node_id} cid={client_oid} {status} {msg}")

    def load_node_oid_history(self):
        """冷启动读取: 返回 {node_id: [client_oid,...]} (按出现序去重), 供多级回溯定位真相。"""
        history, seen = {}, {}
        if not os.path.exists(self.filename):
            return history
        try:
            with open(self.filename, 'r', newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    nid, cid = row.get('node_id', ''), row.get('client_oid', '')
                    if not nid or not cid:
                        continue
                    if nid not in history:
                        history[nid], seen[nid] = [], set()
                    if cid not in seen[nid]:
                        seen[nid].add(cid)
                        history[nid].append(cid)
        except Exception as e:
            logger.error(f"[RECOVER] 读取本地账本失败: {e}")
        return history


class ExchangeBroker:
    """
    交易所网关: 收拢所有 CCXT / 网络调用。
    上层(领域/对账)仅依赖本接口, 切换 OKX / 模拟盘 / 回测只需替换本类, 零改动上层。
    """

    def __init__(self, exchange, symbol):
        self.exchange = exchange
        self.symbol = symbol

    def fetch_precision(self):
        return fetch_market_precision(self.exchange, self.symbol)

    def fetch_last_price(self):
        return self.exchange.fetch_ticker(self.symbol)['last']

    def fetch_open_orders(self):
        return self.exchange.fetch_open_orders(self.symbol)

    def fetch_order(self, client_oid):
        return fetch_single_order(self.exchange, self.symbol, client_oid)

    def place_limit(self, action, amount, price, client_oid, position_side):
        side = "buy" if action == OrderAction.BUY else "sell"
        return execute_order(
            exchange=self.exchange, symbol=self.symbol, side=side, amount=amount,
            client_oid=client_oid, order_type='limit', price=price,
            reduce_only=False, position_side=position_side,
        )


# ==========================================
# 3. 领域层 (GridNode 状态机)
# ==========================================
class GridNode:
    """
    独立网格节点状态机 (最小业务单元)。
    纯内存推演, 副作用经注入的 broker/ledger 落地;
    状态迁移仅由主线程经 process_event 触发, 天然单写者、无需加锁。
    """

    def __init__(self, node_id, open_price, close_price, quantity, ctx):
        # 静态属性 (价格/数量已由拓扑工厂完成精度修约, Node 不再关心精度)
        self.node_id = node_id
        self.target_open_price = open_price
        self.target_close_price = close_price
        self.quantity = quantity
        self.position_side = "LONG"
        self.ctx = ctx

        # 动态状态
        self.state = NodeState.INIT
        self.cycle_count = 0
        self.active_client_oid = ""
        self.active_exchange_oid = ""
        self.last_update_ts = time.time()

    # ---------- 对外能力 ----------
    def open_as_new(self):
        """初始铺位: 进入开仓等待并挂出限价买单。"""
        self.state = NodeState.WAIT_OPEN
        self.active_client_oid = self._new_oid(OrderAction.BUY)
        self._place_limit_order(OrderAction.BUY, self.target_open_price)

    def align(self, cycle, client_oid, exchange_oid, action):
        """依对账真相拨正内存指针 (仅冷启动、单线程环境下调用)。"""
        self.cycle_count = cycle
        self.active_client_oid = client_oid
        self.active_exchange_oid = exchange_oid
        self.state = NodeState.WAIT_OPEN if action == OrderAction.BUY else NodeState.WAIT_CLOSE

    def process_event(self, event):
        """状态机唯一入口: 幂等过滤后按 (状态, 事件) 分发到具体处理器。"""
        # 幂等拦截: 非当前期待的 OID 一律丢弃 (历史延迟 / 重复 / 已过时的对账事件)
        if event.client_oid != self.active_client_oid:
            logger.debug(f"[NODE] {self.node_id} 幂等丢弃 期待={self.active_client_oid} 收到={event.client_oid}")
            return

        if event.status == OrderStatus.FILLED:
            if self.state == NodeState.WAIT_OPEN:
                self._on_open_filled(event)
            elif self.state == NodeState.WAIT_CLOSE:
                self._on_close_filled(event)
        elif event.status == OrderStatus.CANCELED:
            self._on_canceled()

    # ---------- 状态处理器 ----------
    def _on_open_filled(self, event):
        """开仓成交 -> 状态反转, 挂出对应平仓卖单。"""
        self.ctx.ledger.append(self.node_id, self.cycle_count, "OPEN_FILLED",
                               self.active_client_oid, event.fill_price, event.fill_qty, "OK")
        logger.info(f"[FILL] {self.node_id} cycle={self.cycle_count} 开仓成交 "
                    f"@{event.fill_price} x{event.fill_qty} | WAIT_OPEN->WAIT_CLOSE")
        self.state = NodeState.WAIT_CLOSE
        self.active_client_oid = self._new_oid(OrderAction.SELL)
        self._place_limit_order(OrderAction.SELL, self.target_close_price)

    def _on_close_filled(self, event):
        """平仓成交 -> 轮次+1, 状态反转, 重挂开仓买单 (完成一次套利)。"""
        self.ctx.ledger.append(self.node_id, self.cycle_count, "CLOSE_FILLED",
                               self.active_client_oid, event.fill_price, event.fill_qty, "OK", msg="套利完成")
        logger.info(f"[FILL] {self.node_id} cycle={self.cycle_count} 平仓成交 "
                    f"@{event.fill_price} x{event.fill_qty} 套利+1 | WAIT_CLOSE->WAIT_OPEN")
        self.cycle_count += 1
        self.state = NodeState.WAIT_OPEN
        self.active_client_oid = self._new_oid(OrderAction.BUY)
        self._place_limit_order(OrderAction.BUY, self.target_open_price)

    def _on_canceled(self):
        """撤销/拒单 -> 保持状态, 换全新单号原样重挂 (自愈外部干扰)。"""
        if self.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
            return  # ERROR / INIT 不参与自动重挂
        self.ctx.ledger.append(self.node_id, self.cycle_count, "ORDER_CANCELED",
                               self.active_client_oid, 0, 0, "WARN", msg="触发补挂")
        logger.warning(f"[HEAL] {self.node_id} cycle={self.cycle_count} 订单撤销/拒单, "
                       f"原样重挂 | 旧CID:{self.active_client_oid}")
        if self.state == NodeState.WAIT_OPEN:
            action, price = OrderAction.BUY, self.target_open_price
        else:
            action, price = OrderAction.SELL, self.target_close_price
        self.active_client_oid = self._new_oid(action)
        self._place_limit_order(action, price)

    # ---------- 内部工具 ----------
    def _new_oid(self, action):
        return OidCodec.build(self.ctx.strategy_id, self.node_id, action, self.cycle_count)

    def _place_limit_order(self, action, price):
        """WAL 预写意向 -> 发送挂单 -> 按结果落账并输出单条高密度日志。"""
        # WAL: 预写式意向记账, 即使随后进程崩溃也可据此追溯
        self.ctx.ledger.append(self.node_id, self.cycle_count, "INTENT",
                               self.active_client_oid, price, self.quantity, "PENDING", msg="待发送")

        res = self.ctx.broker.place_limit(action, self.quantity, price, self.active_client_oid, self.position_side)
        self.last_update_ts = time.time()
        tag = f"{self.node_id} {action.value}@{price} x{self.quantity} CID:{self.active_client_oid}"

        if res.status == ExecStatus.OK:
            self.active_exchange_oid = res.exchange_oid
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "OK")
            logger.info(f"[PLACE] {tag} -> OK (exch:{res.exchange_oid})")
        elif res.status == ExecStatus.UNKNOWN:
            # 网络断开, 转入防御, 等待看门狗对账修复
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "UNKNOWN")
            logger.critical(f"[PLACE] {tag} -> UNKNOWN 下单状态未知, 转入防御等待对账介入")
        else:
            # 业务拒单(如余额不足), 进入 ERROR 挂起
            self.state = NodeState.ERROR
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "ERROR", msg=res.error_msg)
            logger.error(f"[PLACE] {tag} -> ERROR 明确拒单, 节点挂起 | {res.error_msg}")


def build_geometric_grid(config, broker, ctx):
    """等比生成网格节点; 低价位处若等比价差 < tickSize 则精度塌陷, 提前收口。"""
    precision = broker.fetch_precision()
    _, fmt_qty = format_price_amount(0, config.quantity, precision)
    ratio_factor = 1.0 + config.price_ratio / 100.0

    nodes = {}
    current_close = config.max_price
    i = 0
    while current_close > config.min_price:
        raw_open = current_close / ratio_factor
        fmt_close, _ = format_price_amount(current_close, 0, precision)
        fmt_open, _ = format_price_amount(raw_open, 0, precision)

        # 精度碰撞: 修约后开仓价 >= 平仓价, 说明等比价差已小于最小刻度, 终止下沿生成
        if fmt_open >= fmt_close:
            logger.warning(f"[GRID] 价位 {current_close} 精度塌陷 (修约后开平价同为 {fmt_close}), "
                           f"{config.price_ratio}% 价差已小于 tickSize, 终止下沿生成。")
            break
        if fmt_open < config.min_price:
            break

        node_id = f"N{i:03d}"
        nodes[node_id] = GridNode(node_id, fmt_open, fmt_close, fmt_qty, ctx)
        current_close = fmt_open  # 以当前开仓价作为下一节点平仓价, 无缝拼合
        i += 1

    logger.info(f"[GRID] 生成 {len(nodes)} 个等比节点 | 区间[{config.min_price}-{config.max_price}] "
                f"ratio={config.price_ratio}% qty={fmt_qty}")
    return nodes


# ==========================================
# 4. 对账引擎 (从 GridStrategy 剥离)
# ==========================================
class ReconciliationEngine:
    """
    对账引擎: 比对"交易所真相"与"本地状态", 产出标准事件推入总线。

    单一写者原则(并发安全核心):
      冷启动 recover_on_startup —— 主循环/看门狗未启动, 单线程直接拨正节点并补发事件;
      运行时 repair_runtime    —— 看门狗线程只读节点(GIL 保证原子读)、仅投递事件,
                                 真正的状态迁移交回主线程串行执行;
                                 即便读到过时 OID, 产出的事件也会在主线程被幂等拦截丢弃。
    """

    def __init__(self, broker, ledger, strategy_id, event_queue):
        self.broker = broker
        self.ledger = ledger
        self.strategy_id = strategy_id
        self.event_queue = event_queue

    # ---------- 冷启动: 允许直接拨正节点 (单线程安全) ----------
    def recover_on_startup(self, nodes):
        """多级回溯历史单号锚定真相, 拨正节点并补发事件, 防重启爆铺 / 断电掉单。"""
        active_cids, open_orders = self._snapshot()
        if open_orders is None:
            return
        history = self.ledger.load_node_oid_history()

        aligned = 0
        for node_id, node in nodes.items():
            if node.state != NodeState.INIT:
                continue
            candidates = list(reversed(history.get(node_id, [])[-COLD_START_BACKTRACK:]))  # 最新在前
            if not candidates:
                continue

            truth_cid, truth_order = self._resolve_truth(candidates, active_cids, open_orders, POINT_CHECK_DELAY_COLD)
            if truth_order is not None:
                self._align_and_emit(node, truth_cid, truth_order)
                aligned += 1
            else:
                logger.warning(f"[RECOVER] {node_id} 回溯 {COLD_START_BACKTRACK} 个历史单号均查无此单, "
                               f"判为未送达的幽灵单, 待重新铺设。")
        logger.info(f"[RECOVER] 冷启动对账完成 | 成功锚定恢复 {aligned} 个节点")

    # ---------- 运行时: 只读 + 投递事件, 绝不改节点 (线程安全) ----------
    def repair_runtime(self, nodes, skip_recent=True):
        """只读比对锁定掉单悬挂 -> 点查确认 -> 投递事件; 全程不修改任何节点内存。"""
        active_cids, open_orders = self._snapshot()
        if open_orders is None:
            return

        now = time.time()
        suspects = []  # 检测点固定 (node, cid), 规避与主线程写入的数据竞态
        for node in nodes.values():
            if node.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
                continue
            # 跳过刚动过的节点, 防止状态机与 REST 快照传播延迟造成误判 (init 场景传 False 以即时接管越价成交)
            if skip_recent and now - node.last_update_ts < RECONCILE_SKIP_WINDOW_SEC:
                continue
            cid = node.active_client_oid
            if cid not in active_cids:
                suspects.append((node, cid))

        if not suspects:
            return
        logger.warning(f"[WATCHDOG] 发现 {len(suspects)} 个节点掉单悬挂, 发起兜底点查...")
        for _node, cid in suspects:
            try:
                info = self.broker.fetch_order(cid)
                if info:
                    self._emit_from_order(cid, info)
                else:
                    # 运行时查无此单 = 未送达交易所的幽灵单, 合成 CANCELED 触发原样重挂
                    logger.warning(f"[WATCHDOG] 幽灵单(未送达交易所), 合成 CANCELED 触发重挂 | CID:{cid}")
                    self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))
                time.sleep(POINT_CHECK_DELAY_RUNTIME)
            except Exception as e:
                logger.error(f"[WATCHDOG] 点查异常 CID:{cid} | {e}")

    # ---------- 内部工具 ----------
    def _snapshot(self):
        """拉取本策略活动挂单快照 -> (active_cids, open_orders); 失败返回 (set(), None) 供上层短路。"""
        prefix = OidCodec.prefix_for(self.strategy_id)
        try:
            open_orders = self.broker.fetch_open_orders()
        except Exception as e:
            logger.error(f"[RECONCILE] 获取挂单快照失败: {e}")
            return set(), None
        active_cids = {o.get('clientOrderId', '') for o in open_orders
                       if o.get('clientOrderId', '').startswith(prefix)}
        return active_cids, open_orders

    def _resolve_truth(self, candidates, active_cids, open_orders, point_delay):
        """多级回溯定位真相订单: 快照命中优先(免 API), 否则点查兜底; 返回首个命中的 (cid, order)。"""
        for cid in candidates:
            if cid in active_cids:  # 命中快照, 直接取用免除点查
                order = next((o for o in open_orders if o.get('clientOrderId') == cid), None)
                if order:
                    return cid, order
            else:
                try:
                    info = self.broker.fetch_order(cid)
                    time.sleep(point_delay)
                    if info:
                        return cid, info
                except Exception:
                    pass
        return None, None

    def _align_and_emit(self, node, truth_cid, truth_order):
        """冷启动专用: 依真相拨正节点指针并按其状态补发事件。"""
        parsed = OidCodec.parse(truth_cid)
        if parsed is None:
            return
        node.align(parsed.cycle, truth_cid, truth_order.get('id', ''), parsed.action)
        raw = str(truth_order.get('status', '')).upper()
        logger.info(f"[RECOVER] 锚定真相 | {node.node_id} cycle={parsed.cycle} "
                    f"{parsed.action.value} 状态={raw} CID:{truth_cid}")
        self._emit_from_order(truth_cid, truth_order)

    def _emit_from_order(self, cid, order):
        """交易所原始订单状态 -> 标准事件 (仅成交/终结态产出; 在盘 / 部分成交不产出)。"""
        raw = str(order.get('status', '')).upper()
        if raw in ("CLOSED", "FILLED"):
            self.event_queue.put(OrderEvent(
                cid, OrderStatus.FILLED,
                fill_price=float(order.get('average') or order.get('price') or 0),
                fill_qty=float(order.get('filled') or 0),
            ))
        elif raw in ("CANCELED", "EXPIRED", "REJECTED"):
            self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))


# ==========================================
# 5. 主控与调度 (GridStrategy / Watchdog)
# ==========================================
class GridStrategy:
    """策略主控: 组装依赖, 持有节点集合, 负责初始铺单与事件主循环(路由)。"""

    def __init__(self, config, broker, ledger):
        self.config = config
        self.strategy_id = config.strategy_id
        self.broker = broker
        self.event_queue = queue.Queue()

        ctx = NodeContext(broker, ledger, config.strategy_id)
        self.nodes = build_geometric_grid(config, broker, ctx)
        self.engine = ReconciliationEngine(broker, ledger, config.strategy_id, self.event_queue)

    def recover(self):
        """冷启动对账 (须在主循环/看门狗启动前、单线程执行): 防重启爆铺 / 断电掉单。"""
        self.engine.recover_on_startup(self.nodes)

    def initialize_market_placement(self):
        """
        铺单初始化: 所有新节点统一挂限价买单。
        现价下方 -> 正常挂盘等待; 现价上方 -> 被撮合引擎作为 Taker 瞬时成交。
        随后即时对账(skip_recent=False)接管越价成交, 闭环驱动挂出对应卖单。
        """
        current_price = self.broker.fetch_last_price()

        init_count = 0
        for node in self.nodes.values():
            if node.state != NodeState.INIT:
                continue  # 已由冷启动对账恢复的老节点, 跳过, 杜绝重复铺单
            init_count += 1
            node.open_as_new()
            time.sleep(PLACE_THROTTLE_SEC)

        if init_count == 0:
            logger.info(f"[INIT] 全部节点已从账本恢复进度, 无需新增铺位 | 参考现价 {current_price}")
            return

        logger.info(f"[INIT] 铺出 {init_count} 个新节点初始买单 | 参考现价 {current_price}, "
                    f"呼叫对账引擎接管越价成交底仓...")
        time.sleep(INIT_SETTLE_WAIT_SEC)  # 给撮合与网络传播极短缓冲
        self.engine.repair_runtime(self.nodes, skip_recent=False)

    def run_main_loop(self):
        """单线程主循环 (唯一写者): 消费事件 -> 路由到节点 -> 串行推演状态机。"""
        logger.info("[MAIN] 策略主循环启动, 开始监听事件...")
        while True:
            try:
                event = self.event_queue.get(timeout=1.0)
                self._route(event)
            except queue.Empty:
                pass
            except Exception as e:
                logger.error(f"[MAIN] 事件处理异常: {e}")
                time.sleep(1)

    def _route(self, event):
        """按 OID 解析定位目标节点并投喂事件。"""
        parsed = OidCodec.parse(event.client_oid)
        if parsed is None or parsed.strategy_id != self.strategy_id:
            logger.warning(f"[MAIN] 无法路由的 OID: {event.client_oid}")
            return
        node = self.nodes.get(parsed.node_id)
        if node is None:
            logger.warning(f"[MAIN] 找不到对应节点: {parsed.node_id}")
            return
        node.process_event(event)


class ReconcilerThread(threading.Thread):
    """REST 看门狗线程: 周期性只读对账, 异常仅向总线投递事件, 从不直接改动节点状态。"""

    def __init__(self, engine, nodes, interval_sec=30):
        super().__init__(daemon=True)
        self.engine = engine
        self.nodes = nodes
        self.interval = interval_sec

    def run(self):
        logger.info(f"[WATCHDOG] 看门狗线程启动 | 周期 {self.interval}s")
        while True:
            time.sleep(self.interval)
            try:
                self.engine.repair_runtime(self.nodes, skip_recent=True)
            except Exception as e:
                logger.error(f"[WATCHDOG] 对账轮次异常: {e}")


def run_single_strategy(config):
    """子进程入口：完全独立的执行环境"""

    # 【新增代码 1】：防极端强杀监控线程 (不侵入 strategy 核心代码)
    import os, sys, time, threading
    def _parent_watchdog():
        while True:
            # 如果是非Windows环境且父进程变成 init(1) 或 0，说明主进程已被强杀暴毙
            if sys.platform != "win32" and os.getppid() in (1, 0):
                os._exit(0)  # 物理级斩断孤儿进程
            time.sleep(2)

    threading.Thread(target=_parent_watchdog, daemon=True).start()

    api_key = get_config("myself_biance_api_key")
    secret_key = get_config("myself_biance_api_secret")
    proxies = None if platform.system().lower() == "linux" else {
        "http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890",
    }

    # 注意：每个进程拥有自己独立的 exchange、broker、ledger 实例
    exchange = safe_init_exchange(api_key, secret_key, proxies)
    broker = ExchangeBroker(exchange, config.symbol)
    ledger = GridLedger(config.strategy_id)  # 传入 strategy_id 实现账本隔离
    strategy = GridStrategy(config, broker, ledger)

    strategy.recover()
    strategy.initialize_market_placement()
    # 建议看门狗周期稍微调大一点(比如5秒)，防止多进程并发请求导致触发交易所频率限制
    ReconcilerThread(strategy.engine, strategy.nodes, interval_sec=1).start()
    strategy.run_main_loop()


def main_app():
    """主进程：只负责读取配置、拉起并守护各个子进程"""
    configs = [
        GridConfig(
            strategy_id="S01", symbol="SOL/USDT:USDT",
            min_price=50, max_price=80, price_ratio=0.5, quantity=0.1,
        )
    ]

    processes = []

    # 遍历配置，为每个币种拉起一个独立的进程
    for config in configs:
        p = multiprocessing.Process(target=run_single_strategy, args=(config,))
        p.daemon = True  # 【新增代码 2】：设置为守护进程，主进程正常结束或报错崩溃时，带走子进程
        p.start()
        processes.append(p)
        logger.info(f"[SYSTEM] 已拉起独立进程 | {config.strategy_id} - {config.symbol} (PID: {p.pid})")

    logger.info("[SYSTEM] 所有币种进程启动完毕，主进程进入守护模式。")

    # 主进程挂起，等待子进程执行（也就是一直运行下去）
    # 【新增代码 3】：捕获终端断开或 kill 的异常，防止 p.join 变成死锁
    try:
        for p in processes:
            p.join()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main_app()