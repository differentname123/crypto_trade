# -*- coding: utf-8 -*-
"""
================================================================================
等比网格交易引擎 (事件驱动 + 单写者状态机 + WAL账本 + 三层对账自愈)
================================================================================
[功能摘要]
  为每个交易对拉起独立子进程, 在指定价格区间内等比切分网格节点, 全自动执行
  "低买 -> 高卖 -> 回落再买"的循环套利; 依靠 WAL 账本与对账引擎实现断电/掉单/重启自愈。

[输入数据]
  1. 静态配置  : main_app 内声明的 GridConfig(策略ID/交易对/价格区间/等比间距/单笔数量);
  2. 交易所实时: 经 CCXT 拉取的最新价、市场精度、在线挂单快照、单笔订单点查结果;
  3. 本地账本  : grid_ledger_{策略ID}.csv 按节点归集的历史 client_oid 序列(冷启动回溯用)。

[数据流转/交互]
  配置 --> build_geometric_grid: 按交易所精度修约, 自 max_price 向下等比切分出 GridNode 集合;
  冷启动 --> ReconciliationEngine.recover_on_startup: 以「在线单反向认领 -> 账本回溯点查 ->
            孤儿单巡检」三层比对定位每个节点的"真相订单", 直接拨正节点内存指针,
            已终结的订单则合成 OrderEvent 补投 event_queue;
  运行时 --> GridNode 经 ExchangeBroker 挂限价单(先 WAL 落账再发单);
            ReconcilerThread 看门狗周期性只读比对盘口, 掉单节点逐一点查后
            仅向 event_queue 投递 OrderEvent, 绝不直接改写节点;
  主循环 --> GridStrategy.run_main_loop(全系统唯一写者): 消费 OrderEvent, 经 OidCodec 解析
            路由到目标 GridNode, 幂等过滤后串行驱动状态机:
            [WAIT_OPEN 买成交->挂卖 | WAIT_CLOSE 卖成交->轮次+1回挂买 | 撤单->换新OID原价重挂]。

[输出数据]
  1. 交易所侧: 持续滚动维护的限价买/卖挂单 (核心业务副作用);
  2. 本地侧  : 追加式 CSV 领域事件账本(审计真相 + 冷启动依据) + 按进程隔离的日志文件;
  3. 常驻进程, 无函数返回值, 直至进程被终止。

并发安全基石 —— 单一写者原则:
  节点内存的一切修改只发生在主线程 process_event 中; 看门狗线程只「读 + 查 +投递事件」,
  过时 OID 产出的事件在主线程被幂等拦截丢弃, 从根源消除竞态。
================================================================================
"""
import os
import csv
import platform
import time
import queue
import logging
import threading
import multiprocessing
from enum import Enum
from datetime import datetime
from collections import defaultdict

from common_utils_lite import setup_logger, get_config

logger = setup_logger(app_name="grid_trader")

from biance_order_lite import (
    safe_init_exchange, fetch_market_precision, format_price_amount,
    execute_order, ExecStatus, fetch_single_order,
)


# ==========================================
# 0. 全局可调参数 (集中管理, 消灭魔术数字)
# ==========================================
POINT_CHECK_DELAY_COLD = 0.05     # 冷启动逐单点查的限流间隔(秒)
POINT_CHECK_DELAY_RUNTIME = 0.1   # 运行时看门狗点查的限流间隔(秒)
PLACE_THROTTLE_SEC = 0.05         # 批量铺单时相邻两单的限流间隔(秒)
INIT_SETTLE_WAIT_SEC = 1.0        # 铺单完成后等待撮合/网络传播的缓冲(秒)
COLD_START_BACKTRACK = 3          # 冷启动时每个节点向前回溯的历史单号数量
ORDER_GRACE_PERIOD = 5.0          # 新单冷静期(秒): 期内看门狗不判定掉单, 容忍撮合与传播延迟
TAKER_PRICE_MARKUP = 1.03         # 现价上方节点的吃单封顶系数(现价+3%), 规避越价拒单与插针滑点
WATCHDOG_INTERVAL_SEC = 1         # 看门狗巡检周期(秒); 多进程并发若触发交易所限频, 建议调大(如5秒)


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

    def __init__(self, client_oid, status, fill_price=0.0, fill_qty=0.0, update_ts=0):
        self.client_oid = client_oid
        self.status = status
        self.fill_price = fill_price
        self.fill_qty = fill_qty
        self.update_ts = update_ts  # 新增：事件在交易所发生的真实时间(毫秒)


class GridConfig:
    """策略静态配置: 一个实例对应一个独立子进程。"""

    def __init__(self, strategy_id, symbol, min_price, max_price, price_ratio, quantity):
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.min_price = min_price
        self.max_price = max_price
        self.price_ratio = price_ratio
        self.quantity = quantity


class NodeContext:
    """注入给每个 Node 的运行环境, 让状态机方法保持干净签名。"""

    def __init__(self, broker, ledger, strategy_id):
        self.broker = broker
        self.ledger = ledger
        self.strategy_id = strategy_id
        # 新增：全局共享的市场最新价与精度缓存
        self.latest_price = 0.0
        self.precision = None


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
    每次 append 独立 open, 以崩溃安全换取句柄复用; 逐行细节仅落 DEBUG, 避免 INFO 噪音。
    """
    COLUMNS = ["ts", "node_id", "cycle", "action", "client_oid", "price", "amount", "status", "msg"]

    def __init__(self, strategy_id):
        self.filename = f"grid_ledger_{strategy_id}.csv"
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(self.COLUMNS)

    def append(self, node_id, cycle, action, client_oid, price, amount, status, msg=""):
        """同步写入一行领域事件 (先落盘再动作), 并留 DEBUG 级细粒度轨迹。"""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(self.filename, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([ts, node_id, cycle, action, client_oid, price, amount, status, msg])
        logger.debug(f"[账本] {action} 【{node_id}】 CID:[{client_oid}] 状态:[{status}] {msg}")

    def load_node_oid_history(self):
        """冷启动读取: 返回 {node_id: [client_oid,...]} (按出现序去重), 供多级回溯定位真相。"""
        history, seen = {}, {}
        if not os.path.exists(self.filename):
            return history
        try:
            with open(self.filename, 'r', newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    nid, cid = row.get('node_id', ''), row.get('client_oid', '')
                    if not nid or not cid or cid in seen.setdefault(nid, set()):
                        continue
                    seen[nid].add(cid)
                    history.setdefault(nid, []).append(cid)
        except Exception as e:
            logger.error(f"[账本] 冷启动读取本地账本失败, 无法回溯历史进度, 本次仅能依赖交易所在线单反向认领 | "
                         f"文件:[{self.filename}] 错误:[{e}]")
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

    def _get_safe_buy_price(self):
        """利用全局缓存的现价，计算防越界/插针的安全买入价"""
        if self.ctx.latest_price > 0 and self.ctx.precision is not None:
            raw_price = min(self.target_open_price, self.ctx.latest_price * TAKER_PRICE_MARKUP)
            price, _ = format_price_amount(raw_price, 0, self.ctx.precision)
            return price
        return self.target_open_price

    # ---------- 对外能力 ----------
    def open_as_new(self, override_price=None):
        """初始铺位: 进入开仓等待并挂出限价买单; override_price 用于现价上方节点的吃单封顶价。"""
        self.state = NodeState.WAIT_OPEN
        self.active_client_oid = self._new_oid(OrderAction.BUY)
        price_to_place = override_price if override_price is not None else self.target_open_price
        self._place_limit_order(OrderAction.BUY, price_to_place)

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
            logger.debug(f"[节点] 【{self.node_id}】幂等拦截过时事件, 已丢弃 | "
                         f"期待CID:[{self.active_client_oid}] 收到CID:[{event.client_oid}]")
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
        logger.info(f"[成交] 【{self.node_id}】第[{self.cycle_count}]轮 开仓买单成交 @[{event.fill_price}] "
                    f"x[{event.fill_qty}] | 状态:[WAIT_OPEN]->[WAIT_CLOSE], 转挂平仓卖单 @[{self.target_close_price}]")
        self.state = NodeState.WAIT_CLOSE
        self.active_client_oid = self._new_oid(OrderAction.SELL)
        self._place_limit_order(OrderAction.SELL, self.target_close_price)

    def _on_close_filled(self, event):
        """平仓成交 -> 轮次+1, 状态反转, 重挂开仓买单 (完成一次套利闭环)。"""
        self.ctx.ledger.append(self.node_id, self.cycle_count, "CLOSE_FILLED",
                               self.active_client_oid, event.fill_price, event.fill_qty, "OK", msg="套利完成")
        logger.info(f"[成交] 【{self.node_id}】第[{self.cycle_count}]轮 平仓卖单成交 @[{event.fill_price}] "
                    f"x[{event.fill_qty}] | 套利闭环+1, 状态:[WAIT_CLOSE]->[WAIT_OPEN], 回挂开仓买单 @[{self.target_open_price}]")
        self.cycle_count += 1
        self.state = NodeState.WAIT_OPEN
        self.active_client_oid = self._new_oid(OrderAction.BUY)

        # 修改：利用共享的现价缓存计算安全吃单下限，防极端行情瞬间插针越界
        safe_buy_price = self._get_safe_buy_price()
        self._place_limit_order(OrderAction.BUY, safe_buy_price)

    def _on_canceled(self):
        """撤销/拒单 -> 保持状态, 换全新单号按网格价原样重挂 (自愈外部干扰)。"""
        if self.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
            return  # ERROR / INIT 不参与自动重挂
        self.ctx.ledger.append(self.node_id, self.cycle_count, "ORDER_CANCELED",
                               self.active_client_oid, 0, 0, "WARN", msg="触发补挂")
        logger.warning(f"[自愈] 【{self.node_id}】第[{self.cycle_count}]轮 在管订单被撤销/拒单"
                       f"(可能被手工撤单或交易所清理), 正换新单号按网格价重挂 | 旧CID:[{self.active_client_oid}]")

        # 修改：买单触发吃单封顶计算，让错误恢复的节点能按现价限制重挂
        if self.state == NodeState.WAIT_OPEN:
            action = OrderAction.BUY
            price = self._get_safe_buy_price()
        else:
            action, price = OrderAction.SELL, self.target_close_price

        self.active_client_oid = self._new_oid(action)
        self._place_limit_order(action, price)


    # ---------- 内部工具 ----------
    def _new_oid(self, action):
        return OidCodec.build(self.ctx.strategy_id, self.node_id, action, self.cycle_count)

    def _place_limit_order(self, action, price):
        """WAL 预写意向 -> 发送挂单 -> 按结果落账, 输出单条高密度日志。
        注: broker 调用前后各刷新一次 last_update_ts, 防止看门狗在慢网络调用期间误判掉单。"""
        self.ctx.ledger.append(self.node_id, self.cycle_count, "INTENT",
                               self.active_client_oid, price, self.quantity, "PENDING", msg="待发送")

        self.last_update_ts = time.time()
        res = self.ctx.broker.place_limit(action, self.quantity, price, self.active_client_oid, self.position_side)
        self.last_update_ts = time.time()

        direction = "买入" if action == OrderAction.BUY else "卖出"
        tag = f"【{self.node_id}】{direction} @[{price}] x[{self.quantity}] | CID:[{self.active_client_oid}]"

        if res.status == ExecStatus.OK:
            self.active_exchange_oid = res.exchange_oid
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "OK")
            logger.info(f"[挂单] {tag} | 结果:[OK] 交易所单号:[{res.exchange_oid}]")
        elif res.status == ExecStatus.UNKNOWN:
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "UNKNOWN")
            logger.critical(f"[挂单] {tag} | 结果:[UNKNOWN] 请求发出后未收到交易所回执(疑似网络中断), "
                            f"无法确认是否已受理; 节点转入防御, 等待看门狗点查对账修复")
        else:
            self.state = NodeState.ERROR
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER",
                                   self.active_client_oid, price, self.quantity, "ERROR", msg=res.error_msg)
            logger.error(f"[挂单] {tag} | 结果:[明确拒单] 节点已挂起(ERROR)停止自动交易 | "
                         f"可能原因: 保证金不足/价格触发限制/数量精度不合法 | 交易所回执:[{res.error_msg}]")


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
            logger.warning(f"[网格] 价位[{current_close}]处 [{config.price_ratio}%] 等比价差已小于交易所最小报价刻度"
                           f"(开/平价修约后同为[{fmt_close}]), 低价区无法继续细分, 网格生成提前收口")
            break
        if fmt_open < config.min_price:
            break

        node_id = f"N{i:03d}"
        nodes[node_id] = GridNode(node_id, fmt_open, fmt_close, fmt_qty, ctx)
        current_close = fmt_open  # 以当前开仓价作为下一节点平仓价, 无缝拼合
        i += 1

    logger.info(f"[网格] 等比网格生成完成 | 节点数:[{len(nodes)}] 区间:[{config.min_price}-{config.max_price}] "
                f"间距:[{config.price_ratio}%] 单笔数量:[{fmt_qty}]")
    return nodes


# ==========================================
# 4. 对账引擎 (只读产事件, 冷启动例外)
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
        """三层回溯保障准确恢复: 在线单反向认领 -> 账本回溯点查 -> 孤儿单巡检(仅记录不干预)。"""
        order_map = self._snapshot()
        if order_map is None:
            return

        history = self.ledger.load_node_oid_history()
        aligned = 0

        # ── 第1层: 交易所在线单反向认领 (不依赖本地账本, 账本丢失也能兜住活单) ──
        node_live_orders = defaultdict(list)
        for cid in order_map:
            parsed = OidCodec.parse(cid)
            if parsed and parsed.strategy_id == self.strategy_id and parsed.node_id in nodes:
                node_live_orders[parsed.node_id].append((parsed.cycle, cid))

        for node_id, orders in node_live_orders.items():
            node = nodes[node_id]
            if node.state != NodeState.INIT:
                continue
            # 复合主键排序: 首按 cycle 降序(防跨轮次乱序), 次按 ms后缀 降序(同轮取最新)
            orders.sort(key=lambda x: (x[0], x[1].split('_')[-1]), reverse=True)
            truth_cid = orders[0][1]
            self._align_and_emit(node, truth_cid, order_map[truth_cid], via="在线单反向认领")
            aligned += 1

        # ── 第2层: 本地账本多级回溯 (覆盖已成交/已撤销等不在盘口的订单) ──
        for node_id, node in nodes.items():
            if node.state != NodeState.INIT:
                continue
            candidates = list(reversed(history.get(node_id, [])[-COLD_START_BACKTRACK:]))
            if not candidates:
                continue
            truth_cid, truth_order = self._resolve_truth(candidates, order_map, POINT_CHECK_DELAY_COLD)
            if truth_order is not None:
                self._align_and_emit(node, truth_cid, truth_order, via="账本回溯")
                aligned += 1
            else:
                logger.warning(f"[对账] 【{node_id}】最近[{len(candidates)}]笔历史单号在交易所均查无实据(幽灵单), "
                               f"该节点将按全新节点重新铺单")

        # ── 第3层: 孤儿单巡检 (不归属任何节点; 仅高密度报警, 不执行物理撤单) ──
        managed_cids = {n.active_client_oid for n in nodes.values() if n.active_client_oid}
        orphan_count = 0
        for cid, order in order_map.items():
            if cid in managed_cids:
                continue
            orphan_count += 1
            logger.warning(f"[对账] 发现脱管孤儿单(保留未撤销, 请人工核查是否为历史遗留) | "
                           f"CID:[{cid}] 交易所ID:[{order.get('id', 'N/A')}] "
                           f"[{str(order.get('side', 'N/A')).upper()}] @[{order.get('price', 0)}] x[{order.get('amount', 0)}]")

        logger.info(f"[对账] 冷启动对账完成 | 节点恢复:[{aligned}/{len(nodes)}] | 脱管孤儿单:[{orphan_count}]张(未干预)")

    # ---------- 运行时: 只读 + 投递事件, 绝不改节点 (线程安全) ----------
    def repair_runtime(self, nodes):
        """只读比对锁定掉单悬挂 -> 点查确认 -> 投递事件; 全程不修改任何节点内存。"""
        order_map = self._snapshot()
        if order_map is None:
            return

        suspects = []  # 检测点固定 (node, cid) 二元组, 规避与主线程写入的数据竞态
        for node in nodes.values():
            if time.time() - node.last_update_ts < ORDER_GRACE_PERIOD:
                continue
            if node.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
                continue
            cid = node.active_client_oid
            if cid not in order_map:
                suspects.append((node, cid))

        if not suspects:
            return
        logger.warning(f"[看门狗] 发现[{len(suspects)}]个节点的在管订单从盘口消失(疑似已成交或被撤), "
                       f"逐一点查确认真实状态...")
        for _node, cid in suspects:
            try:
                info = self.broker.fetch_order(cid)
                if info:
                    self._emit_from_order(cid, info)
                else:
                    logger.warning(f"[看门狗] 点查无果: 该单从未抵达交易所(多为下单瞬间网络中断的幽灵单), "
                                   f"已合成撤销事件交由主线程原价重挂 | CID:[{cid}]")
                    self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))
                time.sleep(POINT_CHECK_DELAY_RUNTIME)
            except Exception as e:
                logger.error(f"[看门狗] 点查订单状态时接口异常, 该单留待下一轮巡检复查 | CID:[{cid}] 错误:[{e}]")

    # ---------- 内部工具 ----------
    def _snapshot(self):
        """拉取本策略前缀的在线挂单快照 -> {client_oid: 订单原文}; 失败返回 None 供上层短路。"""
        prefix = OidCodec.prefix_for(self.strategy_id)
        try:
            open_orders = self.broker.fetch_open_orders()
        except Exception as e:
            logger.error(f"[对账] 拉取交易所挂单快照失败, 本轮对账中止(已有状态不受影响, 稍后自动重试) | "
                         f"可能原因: 网络抖动/交易所限频 | 错误:[{e}]")
            return None
        order_map = {}
        for o in open_orders:
            cid = o.get('clientOrderId') or ''  # 兜底 None 值, 防止 startswith 崩溃
            if cid.startswith(prefix):
                order_map[cid] = o
        return order_map

    def _resolve_truth(self, candidates, order_map, point_delay):
        """按新->旧尝试候选单号: 快照命中免 API 直取, 否则点查兜底; 返回首个命中的 (cid, order)。"""
        for cid in candidates:
            if cid in order_map:
                return cid, order_map[cid]
            try:
                info = self.broker.fetch_order(cid)
                time.sleep(point_delay)
                if info:
                    return cid, info
            except Exception as e:
                logger.debug(f"[对账] 回溯点查失败, 继续尝试更早单号 | CID:[{cid}] 错误:[{e}]")
        return None, None

    def _align_and_emit(self, node, truth_cid, truth_order, via):
        """冷启动专用: 依真相拨正节点指针, 输出单条锚定日志, 并按订单状态补发事件。"""
        parsed = OidCodec.parse(truth_cid)
        if parsed is None:
            logger.warning(f"[对账] 真相单号解析失败, 放弃拨正节点【{node.node_id}】"
                           f"(该节点将按全新节点铺单) | CID:[{truth_cid}]")
            return
        node.align(parsed.cycle, truth_cid, truth_order.get('id', ''), parsed.action)
        raw = str(truth_order.get('status', '')).upper()
        direction = "买" if parsed.action == OrderAction.BUY else "卖"
        logger.info(f"[对账] 锚定真相({via}) | 【{node.node_id}】第[{parsed.cycle}]轮 [{direction}]单 "
                    f"交易所状态:[{raw}] | CID:[{truth_cid}]")
        self._emit_from_order(truth_cid, truth_order)

    def _emit_from_order(self, cid, order):
        """交易所原始订单状态 -> 标准事件 (仅终结态产出; 在盘 / 部分成交不产出)。"""
        raw = str(order.get('status', '')).upper()

        # 提取交易所时间(毫秒): 优先拿最后成交时间，次选订单更新时间，兜底使用本地当前时间
        ts = order.get('lastTradeTimestamp') or order.get('lastUpdateTimestamp') or order.get('timestamp') or int(
            time.time() * 1000)

        if raw in ("CLOSED", "FILLED"):
            self.event_queue.put(OrderEvent(
                cid, OrderStatus.FILLED,
                fill_price=float(order.get('average') or order.get('price') or 0),
                fill_qty=float(order.get('filled') or 0),
                update_ts=ts  # 注入时间戳
            ))
        elif raw in ("CANCELED", "EXPIRED", "REJECTED"):
            self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED, update_ts=ts))

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

        # 修改：将 ctx 保存为实例属性，以便后续注入实时价格
        self.ctx = NodeContext(broker, ledger, config.strategy_id)
        self.nodes = build_geometric_grid(config, broker, self.ctx)
        self.engine = ReconciliationEngine(broker, ledger, config.strategy_id, self.event_queue)



    def recover(self):
        """冷启动对账 (须在主循环/看门狗启动前、单线程执行): 防重启爆铺 / 断电掉单。"""
        self.engine.recover_on_startup(self.nodes)

    def initialize_market_placement(self):
        """
        铺单初始化: 所有新节点统一挂限价买单。
        现价下方 -> 按网格价正常挂盘等待;
        现价上方 -> 取 min(网格价, 现价*封顶系数) 吃单瞬时成交, 规避越价拒单与插针滑点;
        随后即时对账接管越价成交, 闭环驱动挂出对应卖单。
        """
        current_price = self.broker.fetch_last_price()
        precision = self.broker.fetch_precision()

        # 新增：将初始化拉取到的价格和精度写入全局上下文
        self.ctx.latest_price = current_price
        self.ctx.precision = precision

        init_count, taker_count = 0, 0
        for node in self.nodes.values():
            if node.state != NodeState.INIT:
                continue  # 已由冷启动对账恢复的老节点跳过, 杜绝重复铺单
            init_count += 1

            if node.target_open_price > current_price:
                raw_price = min(node.target_open_price, current_price * TAKER_PRICE_MARKUP)
                execute_price, _ = format_price_amount(raw_price, 0, precision)
                node.open_as_new(override_price=execute_price)
                taker_count += 1
            else:
                node.open_as_new()
            time.sleep(PLACE_THROTTLE_SEC)

        if init_count == 0:
            logger.info(f"[铺单] 全部节点均已从历史恢复进度, 无需新增铺位 | 参考现价:[{current_price}]")
            return

        logger.info(f"[铺单] 初始买单铺设完成 | 新铺节点:[{init_count}]个(其中现价上方吃单:[{taker_count}]个) | "
                    f"参考现价:[{current_price}] | 即将由对账引擎接管越价瞬时成交...")
        time.sleep(INIT_SETTLE_WAIT_SEC)  # 给撮合与网络传播极短缓冲
        self.engine.repair_runtime(self.nodes)


    def run_main_loop(self):
        """单线程主循环 (全系统唯一写者): 消费事件 -> 路由到节点 -> 串行推演状态机。"""
        logger.info("[主循环] 事件主循环启动(全系统唯一写者), 开始监听订单事件...")
        while True:
            try:
                event = self.event_queue.get(timeout=1.0)
                self._route(event)
            except queue.Empty:
                pass
            except Exception as e:
                logger.error(f"[主循环] 处理订单事件时异常, 1秒后继续消费"
                             f"(若有状态遗漏, 看门狗会兜底修复) | 错误:[{e}]")
                time.sleep(1)

    def _route(self, event):
        """按 OID 解析定位目标节点并投喂事件。"""
        parsed = OidCodec.parse(event.client_oid)
        if parsed is None or parsed.strategy_id != self.strategy_id:
            logger.warning(
                f"[主循环] 收到无法路由的订单事件(格式非法或不属于本策略), 已忽略 | OID:[{event.client_oid}]")
            return

        # 完美落实你的思路：只采纳距离当前时间 5 分钟 (300000毫秒) 内的“新鲜成交价”
        now_ms = int(time.time() * 1000)
        if event.status == OrderStatus.FILLED and event.fill_price > 0:
            if (now_ms - event.update_ts) < 300000:
                self.ctx.latest_price = event.fill_price
            else:
                logger.debug(
                    f"[主循环] 拦截到历史陈旧成交事件，跳过价格更新 | 滞后时长:{(now_ms - event.update_ts) / 1000:.1f}秒")

        node = self.nodes.get(parsed.node_id)
        if node is None:
            logger.warning(f"[主循环] 事件目标节点不存在(网格区间可能已变更), 已忽略 | "
                           f"节点:[{parsed.node_id}] OID:[{event.client_oid}]")
            return
        node.process_event(event)

class ReconcilerThread(threading.Thread):
    """REST 看门狗线程: 周期性只读对账; 异常仅向总线投递事件, 从不直接改动节点状态。"""

    def __init__(self, engine, nodes, interval_sec=30):
        super().__init__(daemon=True)
        self.engine = engine
        self.nodes = nodes
        self.interval = interval_sec

    def run(self):
        logger.info(f"[看门狗] 对账巡检线程启动 | 巡检周期:[{self.interval}s]")
        while True:
            time.sleep(self.interval)
            try:
                self.engine.repair_runtime(self.nodes)
            except Exception as e:
                logger.error(f"[看门狗] 本轮巡检对账异常(不影响主循环运行, 下一轮自动重试) | 错误:[{e}]")


# ==========================================
# 6. 进程编排 (每个交易对一个独立子进程)
# ==========================================
def run_single_strategy(config):
    """子进程入口: 独立日志 -> 孤儿自杀看门狗 -> 组装依赖 -> 冷启动对账 -> 铺单 -> 看门狗 -> 主循环。"""
    # 子进程第一件事: 强制重置日志, 清理父进程遗留句柄, 绑定到 {策略ID}_{币种}.log 独立文件
    safe_symbol = config.symbol.replace('/', '_').replace(':', '_')
    log_filename = f"{config.strategy_id}_{safe_symbol}"
    setup_logger(app_name=log_filename, force_reset=True)
    logging.getLogger().info(f"[进程] 子进程独立日志就绪 | 策略:[{config.strategy_id}] "
                             f"交易对:[{config.symbol}] 日志文件:[{log_filename}.log]")

    # 防极端强杀: 主进程暴毙后本进程父ID会变为 init(1), 此时物理自杀, 杜绝孤儿进程裸奔下单
    def _parent_watchdog():
        while True:
            if os.getppid() in (1, 0):
                os._exit(0)
            time.sleep(2)

    threading.Thread(target=_parent_watchdog, daemon=True).start()

    api_key = get_config("nana_biance_api_copy_key")
    secret_key = get_config("nana_biance_api_copy_secret")
    proxies = None if platform.system().lower() == "linux" else {
        "http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890",
    }
    # 每个进程独享 exchange/broker/ledger/strategy 实例, 账本按 strategy_id 物理隔离
    exchange = safe_init_exchange(api_key, secret_key, proxies)
    broker = ExchangeBroker(exchange, config.symbol)
    ledger = GridLedger(config.strategy_id)
    strategy = GridStrategy(config, broker, ledger)

    strategy.recover()
    strategy.initialize_market_placement()
    ReconcilerThread(strategy.engine, strategy.nodes, interval_sec=WATCHDOG_INTERVAL_SEC).start()
    strategy.run_main_loop()


def main_app():
    """主进程: 只负责读取配置、拉起并守护各个策略子进程。"""
    current_symbol = "20260719"

    configs = [
        GridConfig(
            strategy_id=f"PENDLE{current_symbol}", symbol="PENDLE/USDT:USDT",
            min_price=0.5, max_price=2, price_ratio=1.93, quantity=10,
        ), # 消耗  71  u 网格数量 72

        GridConfig(
            strategy_id=f"AAVE{current_symbol}", symbol="AAVE/USDT:USDT",
            min_price=25, max_price=100, price_ratio=1.54, quantity=0.2,
        ),  # 消耗  87  u 网格数量 90

        GridConfig(
            strategy_id=f"DOGE{current_symbol}", symbol="DOGE/USDT:USDT",
            min_price=0.025, max_price=0.1, price_ratio=1.36, quantity=200,
        ), # 消耗  99  u 网格数量 102

        GridConfig(
            strategy_id=f"ETH{current_symbol}", symbol="ETH/USDT:USDT",
            min_price=1000, max_price=2000, price_ratio=1.13, quantity=0.02,
        ),  # 消耗  237  u 网格数量 61

        GridConfig(
            strategy_id=f"SOL{current_symbol}", symbol="SOL/USDT:USDT",
            min_price=25, max_price=85, price_ratio=1.3, quantity=0.2,
        ),  # 消耗  88  u 网格数量 94

        # 总共节点和为 419 实际app上是419个节点才行，多了或者少了都要排查
    ]
    processes = []
    for config in configs:
        p = multiprocessing.Process(target=run_single_strategy, args=(config,))
        p.daemon = True  # 守护进程: 主进程退出/崩溃时自动带走子进程
        p.start()
        processes.append(p)
        logger.info(f"[系统] 已拉起独立策略进程 | 策略:[{config.strategy_id}] "
                    f"交易对:[{config.symbol}] PID:[{p.pid}]")

    logger.info(f"[系统] 全部策略进程启动完毕, 主进程进入守护模式 | 进程数:[{len(processes)}]")

    # 捕获终端断开或 kill 的异常, 防止 join 变成死锁
    try:
        for p in processes:
            p.join()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main_app()