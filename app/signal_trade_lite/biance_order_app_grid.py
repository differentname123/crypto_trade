# -*- coding: utf-8 -*-
"""
等比网格交易引擎 (事件驱动 + 状态机 + WAL + 大一统对账自愈)

分层:
  基础设施层 : GridLedger(CSV/WAL) | ExchangeBroker(收拢所有 CCXT 调用)
  值对象/工具 : Enum | OrderEvent | ParsedOid | GridConfig | OidCodec
  领域层     : GridNode(纯内存状态机, 仅主线程写)
  对账/调度层 : ReconciliationEngine(只读产事件) | GridStrategy(路由+主循环) | ReconcilerThread

并发安全的基石 —— 单一写者原则:
  节点状态的一切修改, 只允许发生在主线程 process_event 中;
  看门狗线程只"读+查+投递事件", 绝不直接改动节点内存。
"""
import os
import csv
import time
import queue
import platform
import threading
from enum import Enum
from datetime import datetime
from dataclasses import dataclass
from typing import Optional

from app.signal_trade_lite.biance_order_lite import (
    safe_init_exchange, fetch_market_precision, format_price_amount,
    execute_order, ExecStatus, fetch_single_order,
)
from common.common_utils import get_config, setup_logger

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


@dataclass
class ParsedOid:
    strategy_id: str
    node_id: str
    action: OrderAction
    cycle: int


@dataclass
class OrderEvent:
    """事件总线上流转的标准订单事件 (取代原始 dict, 带类型提示与补全)"""
    client_oid: str
    status: OrderStatus
    fill_price: float = 0.0
    fill_qty: float = 0.0


@dataclass
class GridConfig:
    """策略配置: 集中管理原本散落在 main 里的参数"""
    strategy_id: str
    symbol: str
    min_price: float
    max_price: float
    price_ratio: float
    quantity: float


class OidCodec:
    """
    唯一负责 client_oid 的编解码, 全项目不再出现裸 split('_')。
    格式: GD_{strategy}_{node}_{action}_{cycle}_{ts后缀}
    约束: strategy_id / node_id 不含下划线 (当前 S01 / N001 满足)。
    """
    PREFIX = "GD"

    @classmethod
    def build(cls, strategy_id: str, node_id: str, action: OrderAction, cycle: int) -> str:
        ts_suffix = str(int(time.time() * 1000))[-8:]  # 即使反复重挂也天然唯一
        return f"{cls.PREFIX}_{strategy_id}_{node_id}_{action.value}_{cycle}_{ts_suffix}"

    @classmethod
    def parse(cls, oid: str) -> Optional[ParsedOid]:
        parts = oid.split('_')
        if len(parts) < 6 or parts[0] != cls.PREFIX:
            return None
        try:
            return ParsedOid(
                strategy_id=parts[1], node_id=parts[2],
                action=OrderAction(parts[3]), cycle=int(parts[4]),
            )
        except (ValueError, KeyError):
            return None

    @classmethod
    def prefix_for(cls, strategy_id: str) -> str:
        return f"{cls.PREFIX}_{strategy_id}_"


# ==========================================
# 2. 基础设施层 (Ledger / Broker)
# ==========================================
class GridLedger:
    """
    追加式领域事件日志 (Append-Only / WAL)。
    使用标准库 csv 同步写入: 去除 pandas 重依赖; 保持"先落盘再动作"的 WAL 语义, 不做异步刷盘。
    """
    COLUMNS = ["ts", "node_id", "cycle", "action", "client_oid", "price", "amount", "status", "msg"]

    def __init__(self, filename: str = "grid_ledger.csv"):
        self.filename = filename
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(self.COLUMNS)

    def append(self, node_id, cycle, action, client_oid, price, amount, status, msg=""):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        row = [ts, node_id, cycle, action, client_oid, price, amount, status, msg]
        with open(self.filename, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(row)
        logger.info(f"[LEDGER] {action} | {node_id} | CID:{client_oid} | 状态:{status} | {msg}")

    def load_node_oid_history(self) -> dict:
        """冷启动读取: 返回 {node_id: [client_oid,...]} (按出现顺序去重), 供多级回溯使用。"""
        history: dict = {}
        seen: dict = {}
        if not os.path.exists(self.filename):
            return history
        try:
            with open(self.filename, 'r', newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    nid = row.get('node_id', '')
                    cid = row.get('client_oid', '')
                    if not nid or not cid:
                        continue
                    if nid not in history:
                        history[nid], seen[nid] = [], set()
                    if cid not in seen[nid]:
                        seen[nid].add(cid)
                        history[nid].append(cid)
        except Exception as e:
            logger.error(f"[RECONCILE] 读取本地账本失败: {e}")
        return history


class ExchangeBroker:
    """
    交易所网关: 收拢所有 CCXT / 网络调用。
    领域层(Node)与对账引擎只依赖本接口 —— 换 OKX / 模拟盘 / 回测只需替换本类, 上层零改动。
    """
    def __init__(self, exchange, symbol: str):
        self.exchange = exchange
        self.symbol = symbol

    def fetch_precision(self):
        return fetch_market_precision(self.exchange, self.symbol)

    def fetch_last_price(self) -> float:
        return self.exchange.fetch_ticker(self.symbol)['last']

    def fetch_open_orders(self) -> list:
        return self.exchange.fetch_open_orders(self.symbol)

    def fetch_order(self, client_oid: str) -> Optional[dict]:
        return fetch_single_order(self.exchange, self.symbol, client_oid)

    def place_limit(self, action: OrderAction, amount, price, client_oid: str, position_side: str):
        side = "buy" if action == OrderAction.BUY else "sell"
        return execute_order(
            exchange=self.exchange, symbol=self.symbol, side=side, amount=amount,
            client_oid=client_oid, order_type='limit', price=price,
            reduce_only=False, position_side=position_side,
        )


@dataclass
class NodeContext:
    """注入给每个 Node 的运行环境, 让 process_event 拥有干净的签名。"""
    broker: ExchangeBroker
    ledger: GridLedger
    strategy_id: str


# ==========================================
# 3. 领域层 (GridNode 状态机)
# ==========================================
class GridNode:
    """独立网格节点状态机 (最小业务单元)。

    纯内存推演, 通过注入的 broker / ledger 落地副作用。
    状态迁移只由主线程经 process_event 触发, 保证单一写者、无需加锁。
    """

    def __init__(self, node_id, open_price, close_price, quantity, ctx: NodeContext):
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

    def align(self, cycle: int, client_oid: str, exchange_oid: str, action: OrderAction):
        """依据对账真相拨正内存指针 (仅冷启动阶段、单线程环境下调用)。"""
        self.cycle_count = cycle
        self.active_client_oid = client_oid
        self.active_exchange_oid = exchange_oid
        self.state = NodeState.WAIT_OPEN if action == OrderAction.BUY else NodeState.WAIT_CLOSE

    def process_event(self, event: OrderEvent):
        """状态机唯一入口: 幂等过滤后按 (状态, 事件) 分发到具体处理器。"""
        # 幂等拦截: 非当前期待的 OID 一律丢弃 (历史延迟 / 重复 / 已过时的对账事件)
        if event.client_oid != self.active_client_oid:
            logger.debug(f"[NODE_IGNORE] 幂等丢弃 | {self.node_id} | "
                         f"期待:{self.active_client_oid} 收到:{event.client_oid}")
            return

        if event.status == OrderStatus.FILLED:
            if self.state == NodeState.WAIT_OPEN:
                self._on_open_filled(event)
            elif self.state == NodeState.WAIT_CLOSE:
                self._on_close_filled(event)
        elif event.status == OrderStatus.CANCELED:
            self._on_canceled()

    # ---------- 状态处理器 ----------
    def _on_open_filled(self, event: OrderEvent):
        logger.info(f"[NODE_STATE] {self.node_id} | WAIT_OPEN -> WAIT_CLOSE | 开仓成交")
        self.ctx.ledger.append(self.node_id, self.cycle_count, "OPEN_FILLED",
                               self.active_client_oid, event.fill_price, event.fill_qty, "OK")
        # 状态反转: 去挂平仓卖单
        self.state = NodeState.WAIT_CLOSE
        self.active_client_oid = self._new_oid(OrderAction.SELL)
        self._place_limit_order(OrderAction.SELL, self.target_close_price)

    def _on_close_filled(self, event: OrderEvent):
        logger.info(f"[NODE_STATE] {self.node_id} | WAIT_CLOSE -> WAIT_OPEN | 平仓成交 (套利+1)")
        self.ctx.ledger.append(self.node_id, self.cycle_count, "CLOSE_FILLED",
                               self.active_client_oid, event.fill_price, event.fill_qty, "OK", msg="套利完成")
        # 状态反转: 轮次+1, 去挂开仓买单
        self.cycle_count += 1
        self.state = NodeState.WAIT_OPEN
        self.active_client_oid = self._new_oid(OrderAction.BUY)
        self._place_limit_order(OrderAction.BUY, self.target_open_price)

    def _on_canceled(self):
        """撤销/拒单 -> 保持状态不变, 用全新单号原样重挂 (自动治愈外部干扰)。"""
        if self.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
            return  # ERROR / INIT 不参与自动重挂
        logger.warning(f"[NODE_STATE] {self.node_id} | 订单撤销/拒单, 触发原样重挂 | CID:{self.active_client_oid}")
        self.ctx.ledger.append(self.node_id, self.cycle_count, "ORDER_CANCELED",
                               self.active_client_oid, 0, 0, "WARN", msg="触发补挂")
        if self.state == NodeState.WAIT_OPEN:
            action, price = OrderAction.BUY, self.target_open_price
        else:
            action, price = OrderAction.SELL, self.target_close_price
        self.active_client_oid = self._new_oid(action)
        self._place_limit_order(action, price)

    # ---------- 内部工具 ----------
    def _new_oid(self, action: OrderAction) -> str:
        return OidCodec.build(self.ctx.strategy_id, self.node_id, action, self.cycle_count)

    def _place_limit_order(self, action: OrderAction, price):
        """执行挂单并更新自身记录 (含 WAL 预写)。"""
        # WAL: 预写式意向记账, 留下"犯罪现场"
        self.ctx.ledger.append(self.node_id, self.cycle_count, "INTENT",
                               self.active_client_oid, price, self.quantity, "PENDING", msg="准备发送挂单网络包")

        res = self.ctx.broker.place_limit(action, self.quantity, price, self.active_client_oid, self.position_side)
        self.last_update_ts = time.time()

        if res.status == ExecStatus.OK:
            self.active_exchange_oid = res.exchange_oid
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "OK")
        elif res.status == ExecStatus.UNKNOWN:
            # 网络断开, 转入防御, 等待看门狗对账修复
            logger.critical(f"[NODE_DEFENSE] {self.node_id} 下单状态未知, 等待对账介入 | CID:{self.active_client_oid}")
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "UNKNOWN")
        else:
            # 业务拒单(如余额不足), 进入 ERROR 挂起
            self.state = NodeState.ERROR
            logger.error(f"[NODE_ERROR] {self.node_id} 下单被明确拒绝, 节点挂起 | {res.error_msg}")
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "ERROR", msg=res.error_msg)


def build_geometric_grid(config: GridConfig, broker: ExchangeBroker, ctx: NodeContext) -> dict:
    """等比生成网格节点, 并处理低价位下的 tickSize 精度碰撞塌陷。"""
    precision = broker.fetch_precision()
    _, fmt_qty = format_price_amount(0, config.quantity, precision)
    ratio_factor = 1.0 + config.price_ratio / 100.0

    nodes: dict = {}
    current_close = config.max_price
    i = 0
    while current_close > config.min_price:
        raw_open = current_close / ratio_factor
        fmt_close, _ = format_price_amount(current_close, 0, precision)
        fmt_open, _ = format_price_amount(raw_open, 0, precision)

        # 精度碰撞: 修约后开仓价 >= 平仓价, 说明该处等比价差已小于最小刻度, 终止下沿生成
        if fmt_open >= fmt_close:
            logger.warning(f"[STRATEGY] 价格 {current_close} 处触发精度碰撞 (修约后均为 {fmt_close}), "
                           f"{config.price_ratio}% 价差已小于交易所 tickSize, 终止生成后续下沿节点。")
            break
        if fmt_open < config.min_price:
            break

        node_id = f"N{i:03d}"
        nodes[node_id] = GridNode(node_id, fmt_open, fmt_close, fmt_qty, ctx)
        current_close = fmt_open  # 严格以当前开仓价作为下个节点平仓价, 完美拼合
        i += 1

    logger.info(f"[STRATEGY] 成功生成 {len(nodes)} 个等比网格节点: "
                f"[{config.min_price} - {config.max_price}], Ratio: {config.price_ratio}%")
    return nodes


# ==========================================
# 4. 对账引擎 (从 GridStrategy 剥离)
# ==========================================
class ReconciliationEngine:
    """
    对账引擎: 比对"交易所真相"与"本地状态", 产出标准事件推入总线。

    单一写者原则(并发安全的核心):
      - 冷启动 recover_on_startup: 主循环/看门狗尚未启动, 单线程环境, 直接拨正节点并补发事件。
      - 运行时 repair_runtime    : 由看门狗线程调用, 只读节点(GIL 保证原子读), 绝不修改状态,
                                   仅向队列投递事件; 真正的状态迁移交回主线程串行执行。
        即便读到过时的 OID, 产出的事件也会在主线程被幂等拦截丢弃 —— 从根本上消除并发竞态。
    """

    def __init__(self, broker: ExchangeBroker, ledger: GridLedger, strategy_id: str, event_queue: queue.Queue):
        self.broker = broker
        self.ledger = ledger
        self.strategy_id = strategy_id
        self.event_queue = event_queue

    # ---------- 冷启动: 允许直接拨正节点 (单线程安全) ----------
    def recover_on_startup(self, nodes: dict):
        active_cids, open_orders = self._snapshot()
        if open_orders is None:
            return
        history = self.ledger.load_node_oid_history()

        for node_id, node in nodes.items():
            if node.state != NodeState.INIT:
                continue
            candidates = list(reversed(history.get(node_id, [])[-COLD_START_BACKTRACK:]))  # [N, N-1, N-2]
            if not candidates:
                continue

            truth_cid, truth_order = self._resolve_truth(candidates, active_cids, open_orders, POINT_CHECK_DELAY_COLD)
            if truth_order is not None:
                self._align_and_emit(node, truth_cid, truth_order)
            else:
                logger.warning(f"[RECONCILE] {node_id} 回溯 {COLD_START_BACKTRACK} 个历史单号均查无此单, "
                               f"判定为未送达的幽灵单, 节点等待重新铺设。")

    # ---------- 运行时: 只读 + 投递事件, 绝不改节点 (线程安全) ----------
    def repair_runtime(self, nodes: dict, skip_recent: bool = True):
        active_cids, open_orders = self._snapshot()
        if open_orders is None:
            return

        now = time.time()
        suspects = []  # [(node, cid)] —— 在检测点固定 cid, 避免与主线程的写入产生数据竞态
        for node in nodes.values():
            if node.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
                continue
            # 跳过刚动过的节点, 防止状态机与 REST 快照传播延迟形成误判 (init 场景传 False 以即时接管越价成交)
            if skip_recent and now - node.last_update_ts < RECONCILE_SKIP_WINDOW_SEC:
                continue
            cid = node.active_client_oid
            if cid not in active_cids:
                suspects.append((node, cid))

        if not suspects:
            return
        logger.warning(f"[WATCHDOG] 发现 {len(suspects)} 个节点存在掉单悬挂, 发起兜底点查...")
        for _node, cid in suspects:
            try:
                info = self.broker.fetch_order(cid)
                if info:
                    self._emit_from_order(cid, info)
                else:
                    # 运行时查无此单 = 未送达交易所的幽灵单, 合成 CANCELED 触发原样重挂
                    logger.warning(f"[WATCHDOG_FIX] 确认为未送达的幽灵单, 合成 CANCELED 触发重挂 | CID:{cid}")
                    self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))
                time.sleep(POINT_CHECK_DELAY_RUNTIME)
            except Exception as e:
                logger.error(f"[WATCHDOG] 点查异常 CID:{cid} | {e}")

    # ---------- 内部工具 ----------
    def _snapshot(self):
        """拉取本策略相关的活动挂单快照; 失败返回 (set(), None) 供上层短路。"""
        prefix = OidCodec.prefix_for(self.strategy_id)
        try:
            open_orders = self.broker.fetch_open_orders()
        except Exception as e:
            logger.error(f"[RECONCILE] 获取全局挂单快照失败: {e}")
            return set(), None
        active_cids = {o.get('clientOrderId', '') for o in open_orders
                       if o.get('clientOrderId', '').startswith(prefix)}
        return active_cids, open_orders

    def _resolve_truth(self, candidates, active_cids, open_orders, point_delay):
        """多级回溯定位真相订单: 优先快照命中(免 API), 否则点查兜底。"""
        for cid in candidates:
            if cid in active_cids:  # 极速路径: 在快照里, 直接免除点查
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

    def _align_and_emit(self, node: GridNode, truth_cid: str, truth_order: dict):
        """冷启动专用: 拨正节点指针并按真相状态补发事件。"""
        parsed = OidCodec.parse(truth_cid)
        if parsed is None:
            return
        node.align(parsed.cycle, truth_cid, truth_order.get('id', ''), parsed.action)
        raw = str(truth_order.get('status', '')).upper()
        logger.info(f"[RECONCILE_ALIGN] 锁定真相锚点 | {node.node_id} | CID:{truth_cid} | 状态:{raw}")
        self._emit_from_order(truth_cid, truth_order)

    def _emit_from_order(self, cid: str, order: dict):
        """把交易所原始订单状态翻译成标准事件推入总线 (仅成交/终结态才产出)。"""
        raw = str(order.get('status', '')).upper()
        if raw in ("CLOSED", "FILLED"):
            self.event_queue.put(OrderEvent(
                cid, OrderStatus.FILLED,
                fill_price=float(order.get('average') or order.get('price') or 0),
                fill_qty=float(order.get('filled') or 0),
            ))
        elif raw in ("CANCELED", "EXPIRED", "REJECTED"):
            self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))
        # OPEN / NEW / PARTIALLY_FILLED: 订单仍在盘口, 不产出事件


# ==========================================
# 5. 主控与调度 (GridStrategy / Watchdog)
# ==========================================
class GridStrategy:
    """策略主控: 组装依赖, 持有节点集合, 负责初始铺单与事件主循环(路由)。"""

    def __init__(self, config: GridConfig, broker: ExchangeBroker, ledger: GridLedger):
        self.config = config
        self.strategy_id = config.strategy_id
        self.broker = broker
        self.event_queue: queue.Queue = queue.Queue()

        ctx = NodeContext(broker=broker, ledger=ledger, strategy_id=config.strategy_id)
        self.nodes = build_geometric_grid(config, broker, ctx)
        self.engine = ReconciliationEngine(broker, ledger, config.strategy_id, self.event_queue)

    def recover(self):
        """冷启动对账 (必须在主循环/看门狗启动前、单线程执行): 防重启爆铺、断电掉单。"""
        self.engine.recover_on_startup(self.nodes)

    def initialize_market_placement(self):
        """
        铺单初始化: 所有新节点统一挂限价买单。
        现价下方 -> 正常挂盘等待; 现价上方 -> 被撮合引擎作为 Taker 瞬间成交。
        随后触发一次即时对账(skip_recent=False), 由对账引擎接管越价成交, 闭环驱动挂出对应卖单。
        """
        current_price = self.broker.fetch_last_price()
        logger.info(f"[INIT_PLACE] 开始执行铺单初始化, 当前参考现价: {current_price}")

        init_count = 0
        for node in self.nodes.values():
            if node.state != NodeState.INIT:
                continue  # 已由冷启动对账恢复的老节点, 跳过, 杜绝重复铺单
            init_count += 1
            node.open_as_new()
            time.sleep(PLACE_THROTTLE_SEC)

        if init_count == 0:
            logger.info("[INIT_PLACE] 所有节点均已从账本顺利恢复进度, 无需新增初始铺位。")
            return

        logger.info(f"[INIT_PLACE] 成功发送 {init_count} 个新节点初始买单, 呼叫对账引擎接管越价成交底仓...")
        time.sleep(INIT_SETTLE_WAIT_SEC)  # 给撮合与网络传播极短缓冲
        self.engine.repair_runtime(self.nodes, skip_recent=False)

    def run_main_loop(self):
        """单线程主循环: 消费事件 -> 路由到节点 -> 串行推演状态机。"""
        logger.info("[MAIN_LOOP] 策略主循环启动, 开始监听事件...")
        while True:
            try:
                event = self.event_queue.get(timeout=1.0)
                self._route(event)
            except queue.Empty:
                pass
            except Exception as e:
                logger.error(f"[MAIN_LOOP] 事件处理异常: {e}")
                time.sleep(1)

    def _route(self, event: OrderEvent):
        parsed = OidCodec.parse(event.client_oid)
        if parsed is None or parsed.strategy_id != self.strategy_id:
            logger.warning(f"[EVENT] 无法路由的 OID: {event.client_oid}")
            return
        node = self.nodes.get(parsed.node_id)
        if node is None:
            logger.warning(f"[EVENT] 找不到对应的节点ID: {parsed.node_id}")
            return
        node.process_event(event)


class ReconcilerThread(threading.Thread):
    """REST 看门狗线程: 只读对账, 发现异常仅向事件总线投递事件, 从不直接改动节点状态。"""

    def __init__(self, engine: ReconciliationEngine, nodes: dict, interval_sec: int = 30):
        super().__init__(daemon=True)
        self.engine = engine
        self.nodes = nodes
        self.interval = interval_sec

    def run(self):
        logger.info(f"[WATCHDOG] 启动看门狗线程, 周期 {self.interval} 秒")
        while True:
            time.sleep(self.interval)
            try:
                self.engine.repair_runtime(self.nodes, skip_recent=True)
            except Exception as e:
                logger.error(f"[WATCHDOG] 统一对账轮次异常: {e}")


# ==========================================
# 6. 顶层入口
# ==========================================
def main_app():
    api_key = get_config("myself_biance_api_key")
    secret_key = get_config("myself_biance_api_secret")
    proxies = None if platform.system().lower() == "linux" else {
        "http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890",
    }

    config = GridConfig(
        strategy_id="S01", symbol="SOL/USDT:USDT",
        min_price=50, max_price=70, price_ratio=2, quantity=0.1,
    )

    # 依赖组装 (显式注入, 便于替换为模拟盘/回测)
    exchange = safe_init_exchange(api_key, secret_key, proxies)
    broker = ExchangeBroker(exchange, config.symbol)
    ledger = GridLedger()
    strategy = GridStrategy(config, broker, ledger)

    # 1) 冷启动对账 (单线程, 防重启爆铺/断电掉单)
    strategy.recover()

    # 2) 铺单初始化 (自动跳过已恢复节点, 并即时接管越价成交)
    strategy.initialize_market_placement()

    # 3) 启动看门狗 (只读对账, 产出事件交主线程消费)
    ReconcilerThread(strategy.engine, strategy.nodes, interval_sec=30).start()

    # 4) 阻塞进入主循环
    strategy.run_main_loop()


if __name__ == "__main__":
    main_app()