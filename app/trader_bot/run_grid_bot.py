# -*- coding: utf-8 -*-
"""
[功能摘要]
  多进程等比网格交易引擎：支持 LONG/SHORT 双向网格，以单写者状态机、追加式账本和交易所对账实现循环交易与重启自愈。
[输入数据]
  GridConfig 静态配置；交易所最新价/精度/在线单/订单点查结果；logs/grid_ledger_{strategy_id}.csv 历史 client_oid。
[数据流转/交互]
  配置 -> build_geometric_grid 生成节点 -> 冷启动三层对账恢复 -> GridNode 先记 INTENT 再挂单 -> 看门狗只读点查并产 OrderEvent
  -> GridStrategy 主线程作为唯一写者路由事件，串行推进 WAIT_OPEN/WAIT_CLOSE 状态；统计与校时线程只做观测/基础设施维护。
[输出数据]
  交易所限价开/平仓单；CSV 账本、独立日志、方向锁；持续运行的策略子进程。

并发约束：GridNode 业务状态只允许主线程 process_event 修改；ReconcilerThread 只读节点并投递事件。
"""
import csv
import multiprocessing
import os
import platform
import queue
import threading
import time
from collections import Counter, defaultdict, namedtuple
from datetime import datetime
from enum import Enum

from common_utils import get_config, setup_logger
logger = setup_logger(app_name="grid_trader")

# ===== 交易所平台选择 (仅需修改此处即可切换平台) =====
EXCHANGE_PLATFORM = "binance"   # 可选: "binance" | "okx" | "bybit"
# ====================================================

from exchange_factory import get_gateway
_gw = get_gateway(EXCHANGE_PLATFORM)

# 平台无关的枚举类型 (走平台路由, 所有 gateway 均导出相同定义)
ErrKind = _gw.ErrKind
ExecStatus = _gw.ExecStatus

# 以下函数全部走平台路由
cancel_all_orders_of_symbol = _gw.cancel_all_orders_of_symbol
cancel_order_by_id = _gw.cancel_order_by_id
execute_order = _gw.execute_order
fetch_last_price = _gw.fetch_last_price
fetch_market_precision = _gw.fetch_market_precision
fetch_open_orders = _gw.fetch_open_orders
format_price_amount = _gw.format_price_amount
safe_init_exchange = _gw.safe_init_exchange
supports_cancel_all = _gw.supports_cancel_all
sync_exchange_time = _gw.sync_exchange_time

DATA_DIR = "bot_data"
LOG_DIR = "logs"
POINT_CHECK_DELAY_COLD = 0.05
POINT_CHECK_DELAY_RUNTIME = 0.1
PLACE_THROTTLE_SEC = 0.05
INIT_SETTLE_WAIT_SEC = 1.0
COLD_START_BACKTRACK = 3
ORDER_GRACE_PERIOD = 5.0
TAKER_PRICE_MARKUP = 1.03
TAKER_PRICE_MARKDOWN = 0.97
WATCHDOG_INTERVAL_SEC = 2
STATISTICS_INTERVAL_SEC = 120
TIME_SYNC_INTERVAL_SEC = 3600
PARENT_WATCH_INTERVAL_SEC = 2
RECENT_FILL_WINDOW_MS = 300_000
UTILITY_QUERY_DELAY_SEC = 0.1


class NodeState(Enum):
    INIT = "INIT"
    WAIT_OPEN = "WAIT_OPEN"
    WAIT_CLOSE = "WAIT_CLOSE"
    ERROR = "ERROR"


class OrderAction(Enum):
    BUY = "B"
    SELL = "S"


class GridDirection(Enum):
    """决定节点的开/平仓价格侧与买卖动作。"""
    LONG = "LONG"
    SHORT = "SHORT"


class OrderStatus(Enum):
    FILLED = "FILLED"
    CANCELED = "CANCELED"


ParsedOid = namedtuple("ParsedOid", "strategy_id node_id action cycle")
OrderEvent = namedtuple(
    "OrderEvent", "client_oid status fill_price fill_qty update_ts", defaults=(0.0, 0.0, 0)
)


class GridConfig:
    """单策略静态配置；一个实例对应一个独立子进程。"""

    def __init__(self, strategy_id, symbol, min_price, max_price, price_ratio, quantity,
                 direction=GridDirection.LONG, account_name="myself"):
        self.account_name = account_name
        self.strategy_id = f"{account_name}_{strategy_id}"
        # 完整策略ID（含账号与下划线）最多18字符；不截断，避免账本/OID命名碰撞。
        OidCodec.validate_strategy_id(self.strategy_id)
        self.symbol, self.min_price, self.max_price = symbol, min_price, max_price
        self.price_ratio, self.quantity = price_ratio, quantity
        self.direction = GridDirection(direction)

    @property
    def is_long(self):
        return self.direction == GridDirection.LONG


class NodeContext:
    """节点共享环境：broker/ledger/策略标识/方向，以及最新价与精度缓存。"""

    def __init__(self, broker, ledger, strategy_id, direction=GridDirection.LONG):
        self.broker, self.ledger, self.strategy_id, self.direction = broker, ledger, strategy_id, direction
        self.latest_price, self.precision = 0.0, None


class StatisticsThread(threading.Thread):
    """只读聚合网格快照并低频刷新现价，不参与业务状态迁移。"""

    def __init__(self, ctx, nodes, config, interval_sec=60):
        super().__init__(daemon=True)
        self.ctx, self.nodes, self.config, self.interval = ctx, nodes, config, interval_sec
        self.start_time = time.time()

    def run(self):
        logger.info(f"[看板/启动] 统计线程已启动 | 周期:[{self.interval}s]")
        while True:
            time.sleep(self.interval)
            try:
                self._report()
            except Exception as exc:
                logger.error(f"[看板/聚合] 本次播报失败但交易不受影响 | 可能原因:[行情/脏数据] | 错误:[{exc}]")

    def _report(self):
        try:
            latest = self.ctx.broker.fetch_last_price()
            if latest > 0:
                self.ctx.latest_price = latest
        except Exception as exc:
            logger.warning(f"[看板/行情] 刷新现价失败，沿用最近缓存 | 可能原因:[网络/限频] | 错误:[{exc}]")

        price, nodes = self.ctx.latest_price, list(self.nodes.values())
        if price <= 0 or not nodes:
            return

        counts, buys, sells = Counter(n.state for n in nodes), [], []
        for node in nodes:
            if node.state == NodeState.WAIT_OPEN:
                order_price, action = node.target_open_price, node.open_action
            elif node.state == NodeState.WAIT_CLOSE:
                order_price, action = node.target_close_price, node.close_action
            else:
                continue
            (buys if action == OrderAction.BUY else sells).append((order_price, node.node_id))

        closest_buy = max(buys, key=lambda item: item[0]) if buys else None
        closest_sell = min(sells, key=lambda item: item[0]) if sells else None
        uptime = int(time.time() - self.start_time)
        hours, remain = divmod(uptime, 3600)
        minutes, seconds = divmod(remain, 60)
        theoretical_pos = sum(n.quantity for n in nodes if n.state == NodeState.WAIT_CLOSE)
        total_cycles = sum(n.cycle_count for n in nodes)
        base_coin = self.config.symbol.split('/')[0]

        if self.config.is_long:
            direction, pos_name, open_name, close_name = "做多(低买高卖)", "多头", "待买开多", "持多待卖"
            no_buy, no_sell = "已满仓待涨或跌穿下限", "已空仓待跌或突破上限"
            near_min, break_min = "⚠️ 即将跌破下限，面临满仓套牢风险", "🚨 已跌穿下限，停止买入并等待反弹"
            near_max, break_max = "⚠️ 即将突破上限，面临踏空风险", "🚨 已突破上限，停止卖出并等待回调"
        else:
            direction, pos_name, open_name, close_name = "做空(高卖低买)", "空头", "待卖开空", "持空待买"
            no_buy, no_sell = "无空头持仓或跌穿下限", "已满仓待跌或突破上限"
            near_min, break_min = "⚠️ 即将跌破下限，空头利润充分兑现并面临踏空风险", "🚨 已跌穿下限，空头已全部平完，停止追空"
            near_max, break_max = "⚠️ 即将突破上限，面临空头浮亏与强平风险", "🚨 已突破上限，请人工评估空头强平风险"

        lines = [
            f"\n========== [网格运行看板] 账号:[{self.config.account_name}] | 策略:[{self.config.strategy_id}] | {direction} ==========",
            f"现价:[{price:.6f}] | 运行:[{hours}h {minutes}m {seconds}s] | 节点:[{len(nodes)}] | "
            f"{open_name}:[{counts[NodeState.WAIT_OPEN]}] | {close_name}:[{counts[NodeState.WAIT_CLOSE]}] | "
            f"异常:[{counts[NodeState.ERROR]}] | 初始:[{counts[NodeState.INIT]}]",
            f"理论持仓:[{theoretical_pos:.4f} {base_coin}/{pos_name}] | 累计套利:[{total_cycles}]趟",
        ]
        if closest_buy:
            p, nid = closest_buy
            lines.append(f"最近买单:【{nid}】@[{p:.6f}] | 距现价下跌:[{(price - p) / price * 100:.2f}%]")
        else:
            lines.append(f"最近买单:[无] | 说明:[{no_buy}]")
        if closest_sell:
            p, nid = closest_sell
            lines.append(f"最近卖单:【{nid}】@[{p:.6f}] | 距现价上涨:[{(p - price) / price * 100:.2f}%]")
        else:
            lines.append(f"最近卖单:[无] | 说明:[{no_sell}]")

        to_min = (price - self.config.min_price) / price * 100
        to_max = (self.config.max_price - price) / price * 100
        min_text = f"距下限 {to_min:.2f}%" if to_min >= 0 else f"跌穿下限 {-to_min:.2f}%"
        max_text = f"距上限 {to_max:.2f}%" if to_max >= 0 else f"突破上限 {-to_max:.2f}%"
        warnings = [break_min if to_min < 0 else near_min] if to_min <= 5 else []
        if to_max <= 5:
            warnings.append(break_max if to_max < 0 else near_max)
        lines.append(f"网格:[{self.config.min_price}~{self.config.max_price}] | 下沿:[{min_text}] | 上沿:[{max_text}]")
        lines.extend(f"风险:[{warning}]" for warning in warnings)
        logger.info("\n".join(lines + ["==========================================================\n"]))


class OidCodec:
    """编解码 {strategy}_{node36:2}{action}{cycle36:3}{ms36:6}。

    仅OID压缩节点号；内部/账本仍使用N000形式，轮次仍是整数。
    strategy_id可含下划线，从最右侧分隔；完整OID最多31个ASCII字符。
    """
    DIGITS = "0123456789abcdefghijklmnopqrstuvwxyz"
    STRATEGY_CHARS = frozenset(DIGITS + "ABCDEFGHIJKLMNOPQRSTUVWXYZ_-")
    MAX_LENGTH = 31
    NODE_WIDTH = 2
    CYCLE_WIDTH = 3
    STAMP_WIDTH = 6
    TAIL_WIDTH = NODE_WIDTH + 1 + CYCLE_WIDTH + STAMP_WIDTH
    MAX_STRATEGY_LENGTH = MAX_LENGTH - 1 - TAIL_WIDTH
    MAX_NODE_INDEX = 36 ** NODE_WIDTH - 1
    MAX_CYCLE = 36 ** CYCLE_WIDTH - 1
    STAMP_MODULUS = 36 ** STAMP_WIDTH
    _last_ms = -1
    _stamp_lock = threading.Lock()

    @classmethod
    def validate_strategy_id(cls, strategy_id):
        if not isinstance(strategy_id, str) or not 1 <= len(strategy_id) <= cls.MAX_STRATEGY_LENGTH:
            raise ValueError(f"完整策略ID须为1~{cls.MAX_STRATEGY_LENGTH}字符（含账号与下划线）: {strategy_id!r}")
        if any(char not in cls.STRATEGY_CHARS for char in strategy_id):
            raise ValueError(f"策略ID仅允许ASCII字母、数字、下划线和连字符: {strategy_id!r}")

    @classmethod
    def _encode_fixed(cls, value, width, field):
        if type(value) is not int or not 0 <= value < 36 ** width:
            raise ValueError(f"OID字段[{field}]须为0~{36 ** width - 1}的整数: {value!r}")
        chars = []
        while value:
            value, digit = divmod(value, 36)
            chars.append(cls.DIGITS[digit])
        return "".join(reversed(chars)).rjust(width, '0')

    @classmethod
    def encode_node(cls, node_id):
        if (not isinstance(node_id, str) or not node_id.startswith('N')
                or not node_id[1:].isascii() or not node_id[1:].isdigit()):
            raise ValueError(f"非法内部节点ID: {node_id!r}")
        index = int(node_id[1:])
        if node_id != f"N{index:03d}":
            raise ValueError(f"内部节点ID格式须为N000形式: {node_id!r}")
        return cls._encode_fixed(index, cls.NODE_WIDTH, "node")

    @classmethod
    def build(cls, strategy_id, node_id, action, cycle):
        cls.validate_strategy_id(strategy_id)
        node = cls.encode_node(node_id)
        if not isinstance(action, OrderAction):
            raise ValueError(f"非法OID动作: {action!r}")
        cycle_text = cls._encode_fixed(cycle, cls.CYCLE_WIDTH, "cycle")
        # 6位覆盖约25.19天；进程内递增，避免同毫秒补挂或时钟回拨复用后缀。
        with cls._stamp_lock:
            cls._last_ms = max(time.time_ns() // 1_000_000, cls._last_ms + 1)
            stamp = cls._encode_fixed(cls._last_ms % cls.STAMP_MODULUS, cls.STAMP_WIDTH, "ms")
        oid = f"{strategy_id}_{node}{action.value}{cycle_text}{stamp}"
        if len(oid) > cls.MAX_LENGTH:
            raise ValueError(f"OID超出{cls.MAX_LENGTH}字符，禁止下单: {oid}")
        return oid

    @classmethod
    def parse(cls, oid):
        if not isinstance(oid, str) or len(oid) > cls.MAX_LENGTH:
            return None
        strategy_id, separator, tail = oid.rpartition('_')
        if not separator or len(tail) != cls.TAIL_WIDTH:
            return None
        action_pos, cycle_start = cls.NODE_WIDTH, cls.NODE_WIDTH + 1
        cycle_end = cycle_start + cls.CYCLE_WIDTH
        numeric = tail[:action_pos] + tail[cycle_start:]
        if any(char not in cls.DIGITS for char in numeric):
            return None
        try:
            cls.validate_strategy_id(strategy_id)
            parsed = ParsedOid(
                strategy_id, f"N{int(tail[:action_pos], 36):03d}",
                OrderAction(tail[action_pos]), int(tail[cycle_start:cycle_end], 36),
            )
        except (TypeError, ValueError):
            return None
        return parsed

    @classmethod
    def timestamp_hint(cls, oid, reference_ms):
        """无交易所时间时，将6位后缀展开为距参考时间最近的毫秒值。

        后缀不含纪元，只能在半个回绕周期内辅助排序，不能证明全局时间顺序。
        """
        stamp = int(oid[-cls.STAMP_WIDTH:], 36)
        half = cls.STAMP_MODULUS // 2
        return reference_ms + (stamp - reference_ms + half) % cls.STAMP_MODULUS - half

    @classmethod
    def belongs_to(cls, oid, strategy_id):
        parsed = cls.parse(oid)
        return parsed is not None and parsed.strategy_id == strategy_id

    @classmethod
    def prefix_for(cls, strategy_id):
        cls.validate_strategy_id(strategy_id)
        return f"{strategy_id}_"


class GridLedger:
    """追加式领域账本；load_node_oid_history 返回 {node_id: [client_oid, ...]}。"""
    COLUMNS = ["ts", "node_id", "cycle", "action", "client_oid", "price", "amount", "status", "msg"]

    def __init__(self, strategy_id):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.filename = os.path.join(DATA_DIR, f"grid_ledger_{strategy_id}.csv")
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='', encoding='utf-8') as handle:
                csv.writer(handle).writerow(self.COLUMNS)

    def append(self, node_id, cycle, action, client_oid, price, amount, status, msg=""):
        """按 COLUMNS 顺序同步追加一条领域记录。"""
        # : 保留原 close 即视为 WAL 落盘的语义；若要求断电级持久性，应评估 flush+fsync 的性能成本。
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(self.filename, 'a', newline='', encoding='utf-8') as handle:
            csv.writer(handle).writerow([ts, node_id, cycle, action, client_oid, price, amount, status, msg])
        logger.debug(f"[账本/追加] 节点:【{node_id}】 | 动作:[{action}] | CID:[{client_oid}] | 状态:[{status}] | 备注:[{msg}]")

    def load_node_oid_history(self):
        """按出现顺序去重，返回 {node_id: [client_oid, ...]} 供冷启动回溯。"""
        history, seen = defaultdict(list), defaultdict(set)
        if not os.path.exists(self.filename):
            return {}
        try:
            with open(self.filename, 'r', newline='', encoding='utf-8') as handle:
                for row in csv.DictReader(handle):
                    nid, cid = row.get('node_id', ''), row.get('client_oid', '')
                    if not nid or not cid or cid in seen[nid]:
                        continue
                    seen[nid].add(cid)
                    history[nid].append(cid)
        except Exception as exc:
            # 原设计明确允许账本读取失败时退化为“仅在线单恢复”，因此保留该业务边界。
            logger.error(f"[账本/读取] 冷启动账本读取失败，将仅依赖在线单 | 文件:[{self.filename}] | 风险:[历史终态无法回溯] | 错误:[{exc}]")
        return dict(history)


def guard_direction_consistency(config):
    """用 sidecar 文件阻止同一 strategy_id 在 LONG/SHORT 间复用。"""
    # 修改点 2：将 LOG_DIR 替换为 DATA_DIR
    os.makedirs(DATA_DIR, exist_ok=True)
    path, expected = os.path.join(DATA_DIR, f"grid_direction_{config.strategy_id}.lock"), config.direction.value
    try:
        if not os.path.exists(path):
            with open(path, 'w', encoding='utf-8') as handle:
                handle.write(expected)
            logger.info(f"[方向锁/创建] 策略:[{config.strategy_id}] | 方向:[{expected}] | 文件:[{path}]")
            return
        with open(path, 'r', encoding='utf-8') as handle:
            saved = handle.read().strip().upper()
        if not saved or saved == expected:
            return
        logger.critical(
            f"[方向锁/拒绝启动] 策略:[{config.strategy_id}] | 历史:[{saved}] | 当前:[{expected}] | "
            f"风险:[冷启动误判并反向开仓] | 处理:[换新 strategy_id；历史作废后再人工删除方向锁与对应账本]"
        )
        raise SystemExit(1)
    except SystemExit:
        raise
    except Exception as exc:
        logger.warning(f"[方向锁/校验失败] 按原逻辑继续启动 | 策略:[{config.strategy_id}] | 风险:[无法确认历史方向] | 错误:[{exc}]")

class ExchangeBroker:
    """收拢交易所调用；上层仅依赖稳定 broker 语义。"""

    def __init__(self, exchange, symbol):
        self.exchange, self.symbol = exchange, symbol

    def fetch_precision(self):
        return fetch_market_precision(self.exchange, self.symbol)

    def fetch_last_price(self):
        return fetch_last_price(self.exchange, self.symbol)

    def fetch_open_orders_map(self, coid_prefix):
        """返回 {client_oid: UniOrder}；UniOrder 由既有网关定义。"""
        return _gw.fetch_open_orders_map(self.exchange, self.symbol, coid_prefix)

    def fetch_order(self, client_oid):
        """返回 (UniOrder, err)；(None, None) 表示交易所明确查无此单。"""
        return _gw.fetch_order_uni(self.exchange, self.symbol, client_oid)

    def place_limit(self, action, amount, price, client_oid, position_side):
        """按 Hedge Mode 语义挂限价单；reduce_only=False 为既有外部契约。"""
        return execute_order(
            exchange=self.exchange, symbol=self.symbol,
            side="buy" if action == OrderAction.BUY else "sell",
            amount=amount, client_oid=client_oid, order_type='limit', price=price,
            reduce_only=False, position_side=position_side,
        )


class GridNode:
    """单网格节点状态机；复杂输入仅依赖 NodeContext，状态只由主线程修改。"""

    def __init__(self, node_id, open_price, close_price, quantity, ctx):
        self.node_id, self.target_open_price, self.target_close_price = node_id, open_price, close_price
        self.quantity, self.ctx, self.direction = quantity, ctx, ctx.direction
        is_long = self.direction == GridDirection.LONG
        self.position_side = "LONG" if is_long else "SHORT"
        self.open_action = OrderAction.BUY if is_long else OrderAction.SELL
        self.close_action = OrderAction.SELL if is_long else OrderAction.BUY
        self.state, self.cycle_count = NodeState.INIT, 0
        self.active_client_oid = self.active_exchange_oid = ""
        self.last_update_ts = time.time()

    @staticmethod
    def _action_text(action):
        return "买" if action == OrderAction.BUY else "卖"

    def calc_safe_open_price(self):
        """用最新价限制极端跳空的开仓吃单边界；缓存不可用时保持原网格价。"""
        if self.ctx.latest_price <= 0 or self.ctx.precision is None:
            return self.target_open_price
        raw = min(self.target_open_price, self.ctx.latest_price * TAKER_PRICE_MARKUP) \
            if self.open_action == OrderAction.BUY \
            else max(self.target_open_price, self.ctx.latest_price * TAKER_PRICE_MARKDOWN)
        return format_price_amount(raw, 0, self.ctx.precision)[0]

    def open_as_new(self, override_price=None):
        """INIT -> WAIT_OPEN，并挂首张开仓单。"""
        self.state, self.active_client_oid = NodeState.WAIT_OPEN, self._new_oid(self.open_action)
        self._place_limit_order(self.open_action, self.target_open_price if override_price is None else override_price)

    def align(self, cycle, client_oid, exchange_oid, action):
        """冷启动用交易所真相拨正内存指针；action 决定 WAIT_OPEN/WAIT_CLOSE。"""
        self.cycle_count, self.active_client_oid, self.active_exchange_oid = cycle, client_oid, exchange_oid
        self.state = NodeState.WAIT_OPEN if action == self.open_action else NodeState.WAIT_CLOSE
        self.last_update_ts = time.time()

    def process_event(self, event):
        """唯一状态机入口：OID 幂等过滤后处理成交/撤销。"""
        if event.client_oid != self.active_client_oid:
            logger.info(f"[节点/幂等] 丢弃过时事件 | 节点:【{self.node_id}】 | 当前CID:[{self.active_client_oid}] | 事件CID:[{event.client_oid}]")
            return
        if event.status == OrderStatus.CANCELED:
            self._on_canceled()
            return
        if event.status != OrderStatus.FILLED:
            return
        if self.state == NodeState.WAIT_OPEN:
            self._on_open_filled(event)
        elif self.state == NodeState.WAIT_CLOSE:
            self._on_close_filled(event)

    def _on_open_filled(self, event):
        """开仓成交 -> WAIT_CLOSE -> 挂平仓单。"""
        old_oid = self.active_client_oid
        self.ctx.ledger.append(self.node_id, self.cycle_count, "OPEN_FILLED", old_oid, event.fill_price, event.fill_qty, "OK")
        self.state, self.active_client_oid = NodeState.WAIT_CLOSE, self._new_oid(self.close_action)
        logger.info(
            f"[成交/开仓] 节点:【{self.node_id}】 | 轮次:[{self.cycle_count}] | 成交:[{event.fill_price}x{event.fill_qty}] | "
            f"状态:[WAIT_OPEN->WAIT_CLOSE] | 下一单:[{self._action_text(self.close_action)}@{self.target_close_price}]"
        )
        self._place_limit_order(self.close_action, self.target_close_price)

    def _on_close_filled(self, event):
        """平仓成交 -> 轮次+1 -> WAIT_OPEN -> 重挂开仓单。"""
        cycle, old_oid = self.cycle_count, self.active_client_oid
        self.ctx.ledger.append(self.node_id, cycle, "CLOSE_FILLED", old_oid, event.fill_price, event.fill_qty, "OK", msg="套利完成")
        self.cycle_count, self.state = cycle + 1, NodeState.WAIT_OPEN
        self.active_client_oid, safe_price = self._new_oid(self.open_action), self.calc_safe_open_price()
        logger.info(
            f"[成交/平仓] 节点:【{self.node_id}】 | 完成轮次:[{cycle}] | 成交:[{event.fill_price}x{event.fill_qty}] | "
            f"套利:[+1] | 状态:[WAIT_CLOSE->WAIT_OPEN] | 下一单:[{self._action_text(self.open_action)}@{safe_price}]"
        )
        self._place_limit_order(self.open_action, safe_price)

    def _on_canceled(self):
        """在管单撤销/明确不存在时维持业务状态，换新 OID 补挂。"""
        if self.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
            return
        old_oid = self.active_client_oid
        self.ctx.ledger.append(self.node_id, self.cycle_count, "ORDER_CANCELED", old_oid, 0, 0, "WARN", msg="触发补挂")
        action, price = (self.open_action, self.calc_safe_open_price()) \
            if self.state == NodeState.WAIT_OPEN else (self.close_action, self.target_close_price)
        self.active_client_oid = self._new_oid(action)
        logger.info(
            f"[自愈/补挂] 节点:【{self.node_id}】 | 轮次:[{self.cycle_count}] | 旧CID:[{old_oid}] | "
            f"新CID:[{self.active_client_oid}] | 补挂:[{self._action_text(action)}@{price}]"
        )
        self._place_limit_order(action, price)

    def _new_oid(self, action):
        try:
            return OidCodec.build(self.ctx.strategy_id, self.node_id, action, self.cycle_count)
        except ValueError as exc:
            # 容量耗尽时停止该节点，清空旧指针，避免重复处理旧成交或用旧OID补挂。
            self.state = NodeState.ERROR
            self.active_client_oid = self.active_exchange_oid = ""
            logger.critical(f"[节点/OID失败] 节点:【{self.node_id}】 | 轮次:[{self.cycle_count}] | 处理:[停止该节点，请人工处理] | 错误:[{exc}]")
            raise

    def _place_limit_order(self, action, price):
        """WAL INTENT -> 交易所挂单 -> 结果落账；底层异常记录后继续向上抛。"""
        cid = self.active_client_oid
        self.ctx.ledger.append(self.node_id, self.cycle_count, "INTENT", cid, price, self.quantity, "PENDING", msg="待发送")
        self.last_update_ts = time.time()
        try:
            result = self.ctx.broker.place_limit(action, self.quantity, price, cid, self.position_side)
        except Exception as exc:
            logger.error(
                f"[挂单/调用失败] 订单是否到达交易所未知 | 节点:【{self.node_id}】 | CID:[{cid}] | "
                f"方向:[{self._action_text(action)}] | 价格:[{price}] | 数量:[{self.quantity}] | 可能原因:[网络/网关异常] | 错误:[{exc}]"
            )
            raise
        finally:
            self.last_update_ts = time.time()

        context = f"节点:【{self.node_id}】 | 方向:[{self.position_side}/{self._action_text(action)}] | 价格:[{price}] | 数量:[{self.quantity}] | CID:[{cid}]"
        if result.status == ExecStatus.OK:
            self.active_exchange_oid = result.exchange_oid
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER", cid, price, self.quantity, "OK")
            logger.info(f"[挂单/成功] {context} | 交易所单号:[{result.exchange_oid}]")
            return
        if result.status == ExecStatus.UNKNOWN:
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER", cid, price, self.quantity, "UNKNOWN")
            logger.critical(f"[挂单/结果未知] {context} | 处理:[保持状态，由看门狗点查，禁止盲目补单]")
            return
        if result.kind in (ErrKind.TRANSIENT, ErrKind.UNKNOWN_RESULT):
            self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER", cid, price, self.quantity, "WARN", msg="瞬态失败，等待对账")
            logger.warning(
                f"[挂单/瞬态失败] {context} | 原因:[{result.kind.value}] | 回执:[{result.error_msg}] | "
                f"处理:[保持 {self.state.value}，{ORDER_GRACE_PERIOD}s 后由看门狗确认]"
            )
            return
        self.state = NodeState.ERROR
        self.ctx.ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER", cid, price, self.quantity, "ERROR", msg=result.error_msg)
        logger.error(
            f"[挂单/明确拒绝] {context} | 状态:[ERROR] | 原因:[{result.kind.value}] | 回执:[{result.error_msg}] | "
            f"排查:[余额/精度/持仓模式/交易规则]"
        )


def build_geometric_grid(config, broker, ctx):
    """返回 {node_id: GridNode}；相邻低/高价构成一个套利区间。"""
    if config.min_price <= 0 or config.max_price <= config.min_price:
        raise ValueError(f"非法价格区间: min={config.min_price}, max={config.max_price}")
    if config.price_ratio <= 0 or config.quantity <= 0:
        raise ValueError(f"非法网格参数: ratio={config.price_ratio}, quantity={config.quantity}")

    precision = broker.fetch_precision()
    quantity = format_price_amount(0, config.quantity, precision)[1]
    if quantity <= 0:
        raise ValueError(f"数量经交易所精度修约后为0: quantity={config.quantity}, symbol={config.symbol}")
    ctx.precision = precision

    nodes, current_high, index = {}, config.max_price, 0
    factor, is_long = 1.0 + config.price_ratio / 100.0, config.is_long
    while current_high > config.min_price:
        high = format_price_amount(current_high, 0, precision)[0]
        low = format_price_amount(current_high / factor, 0, precision)[0]
        if low >= high:
            logger.warning(
                f"[网格/提前收口] 等比价差小于最小报价刻度 | 原高位:[{current_high}] | 修约:[{high}/{low}] | 间距:[{config.price_ratio}%]"
            )
            break
        if low < config.min_price:
            break
        node_id = f"N{index:03d}"
        OidCodec.encode_node(node_id)  # 铺单前检查容量，禁止截断/回绕节点号。
        open_price, close_price = (low, high) if is_long else (high, low)
        nodes[node_id] = GridNode(node_id, open_price, close_price, quantity, ctx)
        current_high, index = low, index + 1

    if not nodes:
        raise ValueError(f"网格生成为空: symbol={config.symbol}, range={config.min_price}-{config.max_price}, ratio={config.price_ratio}%")
    logger.info(
        f"[网格/生成] 策略:[{config.strategy_id}] | 方向:[{config.direction.value}] | 节点:[{len(nodes)}] | "
        f"区间:[{config.min_price}-{config.max_price}] | 间距:[{config.price_ratio}%] | 单笔:[{quantity}]"
    )
    return nodes


class TimeSyncThread(threading.Thread):
    """低频刷新交易所时间偏差；失败时保留旧偏差。"""

    def __init__(self, exchange, interval_sec=3600):
        super().__init__(daemon=True)
        self.exchange, self.interval_sec = exchange, interval_sec

    def run(self):
        logger.info(f"[时间同步/启动] 周期:[{self.interval_sec}s]")
        while True:
            time.sleep(self.interval_sec)
            try:
                logger.debug(f"[时间同步/完成] 当前偏差:[{sync_exchange_time(self.exchange)}ms]")
            except Exception as exc:
                logger.warning(f"[时间同步/失败] 保留旧偏差 | 可能原因:[网络/限频] | 错误:[{exc}]")


class ReconciliationEngine:
    """交易所订单真相 -> OrderEvent；运行时不直接修改 GridNode。"""

    def __init__(self, broker, ledger, strategy_id, event_queue):
        self.broker, self.ledger, self.strategy_id, self.event_queue = broker, ledger, strategy_id, event_queue

    def recover_on_startup(self, nodes):
        """冷启动三层恢复：在线单认领 -> 账本点查 -> 孤儿单巡检；nodes 形貌为 {node_id: GridNode}。"""
        order_map = self._snapshot()
        if order_map is None:
            # 技术修复：冷启动连在线真相都拿不到时必须停止，不能继续把全部 INIT 节点当新节点铺单。
            raise RuntimeError(f"冷启动无法取得在线挂单快照，策略[{self.strategy_id}]停止以避免重复下单")

        history, aligned, live = self.ledger.load_node_oid_history(), 0, defaultdict(list)
        for cid in order_map:
            parsed = OidCodec.parse(cid)
            if parsed and parsed.strategy_id == self.strategy_id and parsed.node_id in nodes:
                live[parsed.node_id].append((parsed.cycle, cid))

        reference_ms = time.time_ns() // 1_000_000
        for node_id, orders in live.items():
            node = nodes[node_id]
            if node.state != NodeState.INIT:
                continue
            # 优先交易所完整时间；缺失时仅用展开后的毫秒后缀辅助排序。
            orders.sort(key=lambda item: (
                item[0], order_map[item[1]].ts or OidCodec.timestamp_hint(item[1], reference_ms),
                OidCodec.timestamp_hint(item[1], reference_ms),
            ), reverse=True)
            cid = orders[0][1]
            aligned += bool(self._align_and_emit(node, cid, order_map[cid], "在线单反向认领"))

        for node_id, node in nodes.items():
            if node.state != NodeState.INIT:
                continue
            candidates = list(reversed(history.get(node_id, [])[-COLD_START_BACKTRACK:]))
            if not candidates:
                continue
            cid, order = self._resolve_truth(candidates, order_map, POINT_CHECK_DELAY_COLD)
            if order is None:
                logger.info(f"[对账/冷启动] 历史候选均被明确判定不存在 | 节点:【{node_id}】 | 候选:[{len(candidates)}] | 处理:[按新节点初始化]")
                continue
            aligned += bool(self._align_and_emit(node, cid, order, "账本回溯"))

        managed = {n.active_client_oid for n in nodes.values() if n.active_client_oid}
        orphans = [
            f"CID={cid},EXID={o.ex_id},{str(o.side).upper()}@{o.price}x{o.amount}"
            for cid, o in order_map.items() if cid not in managed
        ]
        if orphans:
            logger.warning(f"[对账/孤儿单] 按原策略仅告警不撤销 | 数量:[{len(orphans)}] | 明细:[{' ; '.join(orphans)}]")
        logger.info(f"[对账/冷启动完成] 策略:[{self.strategy_id}] | 恢复:[{aligned}/{len(nodes)}] | 孤儿单:[{len(orphans)}]")

    def repair_runtime(self, nodes):
        """找出盘口消失的在管单，点查后只投递事件。"""
        order_map = self._snapshot()
        if order_map is None:
            return
        now = time.time()
        suspects = [
            n.active_client_oid for n in nodes.values()
            if n.state in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE)
            and now - n.last_update_ts >= ORDER_GRACE_PERIOD
            and n.active_client_oid not in order_map
        ]
        if not suspects:
            return
        logger.info(f"[看门狗/疑似掉单] 在线盘口已找不到在管单 | 数量:[{len(suspects)}] | 处理:[逐一点查]")
        for cid in suspects:
            order, err = self.broker.fetch_order(cid)
            if order is not None:
                self._emit_from_order(cid, order)
            elif err is None:
                logger.info(f"[看门狗/明确无此单] 合成撤销事件交给主线程补挂 | CID:[{cid}]")
                self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))
            else:
                logger.warning(f"[看门狗/点查失败] 不合成撤销 | CID:[{cid}] | 可能原因:[网络/交易所异常] | 错误:[{err}] | 处理:[下轮复查]")
            time.sleep(POINT_CHECK_DELAY_RUNTIME)

    def _snapshot(self):
        """返回 {client_oid: UniOrder}；失败返回 None。"""
        try:
            order_map = self.broker.fetch_open_orders_map(OidCodec.prefix_for(self.strategy_id))
            # strategy_id可含下划线，前缀查询后还须精确匹配，防止混入其他策略。
            return {cid: order for cid, order in order_map.items() if OidCodec.belongs_to(cid, self.strategy_id)}
        except Exception as exc:
            logger.error(f"[对账/快照失败] 策略:[{self.strategy_id}] | 可能原因:[网络/限频] | 错误:[{exc}]")
            return None

    def _resolve_truth(self, candidates, order_map, delay):
        """按新到旧解析候选；网络不确定绝不能等价于“订单不存在”。"""
        errors = []
        for cid in candidates:
            if cid in order_map:
                return cid, order_map[cid]
            order, err = self.broker.fetch_order(cid)
            time.sleep(delay)
            if order is not None:
                return cid, order
            if err is not None:
                errors.append((cid, err))
        if errors:
            cid, err = errors[-1]
            # 技术修复：冷启动点查有不确定结果时停止，避免把网络故障误判为“幽灵单”后重复铺单。
            raise RuntimeError(f"冷启动有[{len(errors)}]笔订单无法确认，最后CID[{cid}]错误[{err}]")
        return None, None

    def _align_and_emit(self, node, cid, order, via):
        """冷启动拨正 node 并补发终态事件；成功返回 True。"""
        parsed = OidCodec.parse(cid)
        if not parsed or parsed.strategy_id != self.strategy_id or parsed.node_id != node.node_id:
            logger.warning(f"[对账/拒绝拨正] OID与目标节点不匹配 | 节点:【{node.node_id}】 | CID:[{cid}] | 处理:[保持INIT]")
            return False
        node.align(parsed.cycle, cid, order.ex_id, parsed.action)
        semantic = "开仓" if parsed.action == node.open_action else "平仓"
        logger.info(
            f"[对账/锚定] 来源:[{via}] | 节点:【{node.node_id}】 | 轮次:[{parsed.cycle}] | 语义:[{semantic}] | "
            f"交易所状态:[{order.status}] | 节点状态:[{node.state.value}] | CID:[{cid}]"
        )
        self._emit_from_order(cid, order)
        return True

    def _emit_from_order(self, cid, order):
        """UniOrder -> OrderEvent；仅终结态产事件。"""
        if order.status in ("CLOSED", "FILLED"):
            self.event_queue.put(OrderEvent(cid, OrderStatus.FILLED, order.avg_price or order.price, order.filled, order.ts))
        elif order.status in ("CANCELED", "EXPIRED", "REJECTED"):
            self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED, update_ts=order.ts))


class GridStrategy:
    """组装节点/对账引擎，并作为唯一写者消费事件。"""

    def __init__(self, config, broker, ledger):
        self.config, self.strategy_id, self.broker = config, config.strategy_id, broker
        self.event_queue = queue.Queue()
        self.ctx = NodeContext(broker, ledger, config.strategy_id, config.direction)
        self.nodes = build_geometric_grid(config, broker, self.ctx)
        self.engine = ReconciliationEngine(broker, ledger, config.strategy_id, self.event_queue)
        if not config.is_long:
            logger.warning(
                f"[策略/做空提示] 策略:[{self.strategy_id}] | 交易对:[{config.symbol}] | "
                f"前置条件:[Hedge Mode；max_price远离强平价；strategy_id不得复用LONG历史账本]"
            )

    def recover(self):
        """在任何铺单/看门狗线程启动前执行冷启动恢复。"""
        self.engine.recover_on_startup(self.nodes)

    def initialize_market_placement(self):
        """只为仍处于 INIT 的节点铺首张开仓单，然后立即运行一次对账。"""
        current_price = self.broker.fetch_last_price()
        if current_price <= 0:
            raise RuntimeError(f"初始化现价非法[{current_price}]，禁止铺单")
        self.ctx.latest_price, self.ctx.precision = current_price, self.broker.fetch_precision()
        init_count = taker_count = 0
        for node in self.nodes.values():
            if node.state != NodeState.INIT:
                continue
            init_count += 1
            need_taker = node.target_open_price > current_price if self.config.is_long else node.target_open_price < current_price
            node.open_as_new(node.calc_safe_open_price() if need_taker else None)
            taker_count += int(need_taker)
            time.sleep(PLACE_THROTTLE_SEC)

        if not init_count:
            logger.info(f"[铺单/跳过] 全部节点已恢复 | 策略:[{self.strategy_id}] | 现价:[{current_price}]")
            return
        logger.info(
            f"[铺单/完成] 策略:[{self.strategy_id}] | 方向:[{'买入开多' if self.config.is_long else '卖出开空'}] | "
            f"新节点:[{init_count}] | 保护性吃单:[{taker_count}] | 现价:[{current_price}]"
        )
        time.sleep(INIT_SETTLE_WAIT_SEC)
        self.engine.repair_runtime(self.nodes)

    def run_main_loop(self):
        """唯一写者：OrderEvent -> OID路由 -> GridNode。"""
        logger.info(f"[主循环/启动] 单写者事件循环已启动 | 策略:[{self.strategy_id}]")
        while True:
            try:
                self._route(self.event_queue.get(timeout=1.0))
            except queue.Empty:
                continue
            except Exception as exc:
                # 原设计明确要求主循环存活并依赖后续对账兜底，因此这里记录后继续。
                logger.exception(
                    f"[主循环/事件失败] 1秒后继续 | 策略:[{self.strategy_id}] | 影响:[本事件可能未完整推进] | "
                    f"兜底:[看门狗对账] | 错误:[{exc}]"
                )
                time.sleep(1)

    def _route(self, event):
        """解析 event.client_oid，更新新鲜成交价，并投递到对应节点。"""
        parsed = OidCodec.parse(event.client_oid)
        if not parsed or parsed.strategy_id != self.strategy_id:
            logger.info(f"[主循环/忽略事件] 非本策略或OID非法 | 策略:[{self.strategy_id}] | OID:[{event.client_oid}]")
            return
        if event.status == OrderStatus.FILLED and event.fill_price > 0 and event.update_ts > 0:
            age = int(time.time() * 1000) - event.update_ts
            if 0 <= age < RECENT_FILL_WINDOW_MS:
                self.ctx.latest_price = event.fill_price
            else:
                logger.debug(f"[主循环/旧成交] 不更新价格缓存 | CID:[{event.client_oid}] | 滞后:[{age / 1000:.1f}s]")
        node = self.nodes.get(parsed.node_id)
        if not node:
            logger.info(f"[主循环/忽略事件] 目标节点不存在 | 节点:[{parsed.node_id}] | OID:[{event.client_oid}]")
            return
        node.process_event(event)


class ReconcilerThread(threading.Thread):
    """周期性只读对账；只产事件，不直接改节点。"""

    def __init__(self, engine, nodes, interval_sec=30):
        super().__init__(daemon=True)
        self.engine, self.nodes, self.interval = engine, nodes, interval_sec

    def run(self):
        logger.info(f"[看门狗/启动] 周期:[{self.interval}s]")
        while True:
            time.sleep(self.interval)
            try:
                self.engine.repair_runtime(self.nodes)
            except Exception as exc:
                logger.error(f"[看门狗/巡检失败] 保持节点状态并等待下轮 | 可能原因:[网络/交易所异常] | 错误:[{exc}]")


def run_single_strategy(config):
    """子进程：日志 -> 方向锁 -> 父进程守护 -> 依赖组装 -> 恢复 -> 铺单 -> 后台线程 -> 主循环。"""
    global logger
    safe_symbol = config.symbol.replace('/', '_').replace(':', '_')
    log_filename = f"{config.strategy_id}_{safe_symbol}"
    # 技术修复：后续所有模块级日志都切换到当前子进程 logger，而不是继续引用父进程 logger 对象。
    logger = setup_logger(app_name=log_filename, force_reset=True)
    logger.info(
        f"[进程/启动] 账号:[{config.account_name}] | 策略:[{config.strategy_id}] | 交易对:[{config.symbol}] | "
        f"方向:[{config.direction.value}] | 日志:[{log_filename}.log]"
    )
    guard_direction_consistency(config)

    def _parent_watchdog():
        while True:
            if os.getppid() in (1, 0):
                os._exit(0)
            time.sleep(PARENT_WATCH_INTERVAL_SEC)

    threading.Thread(target=_parent_watchdog, daemon=True).start()
    # 凭证加载与交易所实例化统一走平台路由, 切换 EXCHANGE_PLATFORM 即自动匹配对应配置键
    proxies = None if platform.system().lower() == "linux" else {
        "http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"
    }
    exchange = _gw.open_session(proxies, config.account_name)
    broker, ledger = ExchangeBroker(exchange, config.symbol), GridLedger(config.strategy_id)
    strategy = GridStrategy(config, broker, ledger)
    strategy.recover()
    strategy.initialize_market_placement()
    ReconcilerThread(strategy.engine, strategy.nodes, WATCHDOG_INTERVAL_SEC).start()
    StatisticsThread(strategy.ctx, strategy.nodes, config, STATISTICS_INTERVAL_SEC).start()
    TimeSyncThread(exchange, TIME_SYNC_INTERVAL_SEC).start()
    strategy.run_main_loop()


def cancel_all_orders_for_symbol(exchange, symbol):
    """人工运维：撤销指定 symbol 全部活动挂单；按既有公开契约返回 bool。"""
    logger.info(f"[运维/撤单] 开始 | 交易对:[{symbol}]")
    try:
        if supports_cancel_all(exchange):
            cancel_all_orders_of_symbol(exchange, symbol)
            logger.info(f"[运维/撤单完成] 交易对:[{symbol}] | 模式:[批量] | 结果:[OK]")
            return True
        orders = fetch_open_orders(exchange, symbol)
        canceled = 0
        for order in orders:
            if order.get('id'):
                cancel_order_by_id(exchange, symbol, order['id'])
                canceled += 1
        logger.info(f"[运维/撤单完成] 交易对:[{symbol}] | 模式:[逐单] | 撤单数:[{canceled}]")
        return True
    except Exception as exc:
        logger.error(f"[运维/撤单失败] 交易对:[{symbol}] | 可能原因:[网络/权限/交易所异常] | 错误:[{exc}]")
        return False


def query_all_open_orders_stats(exchange, symbols=None):
    """人工运维：返回 {symbol: {total,buy,sell,buy_qty,sell_qty}}；无数据/失败返回 None。"""
    logger.info(f"[运维/挂单统计] 开始 | 范围:[{'指定交易对' if symbols else '账户全量'}]")
    try:
        orders = []
        if symbols:
            for symbol in symbols:
                try:
                    orders.extend(fetch_open_orders(exchange, symbol))
                except Exception as exc:
                    logger.warning(f"[运维/挂单统计] 单币查询失败，继续其余币种 | 交易对:[{symbol}] | 错误:[{exc}]")
                time.sleep(UTILITY_QUERY_DELAY_SEC)
        else:
            orders = fetch_open_orders(exchange)
        if not orders:
            logger.info("[运维/挂单统计] 当前无活动挂单 | 总数:[0]")
            return None

        stats = defaultdict(lambda: {"total": 0, "buy": 0, "sell": 0, "buy_qty": 0.0, "sell_qty": 0.0})
        total_buy = total_sell = 0
        for order in orders:
            symbol, side = order.get('symbol', 'UNKNOWN'), str(order.get('side', '')).lower()
            remaining = float(order.get('remaining') or order.get('amount') or 0.0)
            stats[symbol]["total"] += 1
            if side == 'buy':
                stats[symbol]["buy"] += 1; stats[symbol]["buy_qty"] += remaining; total_buy += 1
            elif side == 'sell':
                stats[symbol]["sell"] += 1; stats[symbol]["sell_qty"] += remaining; total_sell += 1

        lines = [f"\n========== [账户挂单统计] 总数:[{len(orders)}] | 买:[{total_buy}] | 卖:[{total_sell}] =========="]
        for symbol, data in stats.items():
            base = symbol.split('/')[0] if '/' in symbol else symbol
            lines.append(
                f"{symbol} | 总:[{data['total']}] | 买:[{data['buy']}/{data['buy_qty']:.4f} {base}] | "
                f"卖:[{data['sell']}/{data['sell_qty']:.4f} {base}]"
            )
        logger.info("\n".join(lines + ["===============================================================\n"]))
        return stats
    except Exception as exc:
        logger.error(f"[运维/挂单统计失败] 可能原因:[网络/返回数据异常] | 错误:[{exc}]")
        return None


def inspect_orphan_and_duplicate_orders(exchange, symbol, strategy_id):
    """人工诊断：按新OID格式归类；移除专用前缀后，仅能按结构识别网格单。"""
    orders = fetch_open_orders(exchange, symbol)
    node_orders, orphans, others = defaultdict(list), [], []
    for order in orders:
        cid = order.get('clientOrderId') or ''
        parsed = OidCodec.parse(cid)
        if parsed and parsed.strategy_id == strategy_id:
            node_orders[parsed.node_id].append(order)
        elif parsed:
            orphans.append(order)
        else:
            others.append(order)

    managed = sum(len(items) for items in node_orders.values())
    lines = [
        f"\n========== [挂单深度诊断] 交易对:[{symbol}] | 策略:[{strategy_id}] ==========",
        f"总:[{len(orders)}] | 本策略:[{managed}] | 其他网格/孤儿:[{len(orphans)}] | 非网格:[{len(others)}]",
    ]
    duplicates = 0
    for node_id, items in sorted(node_orders.items()):
        if len(items) <= 1:
            continue
        duplicates += 1
        detail = "; ".join(f"CID={o.get('clientOrderId')} price={o.get('price')} side={o.get('side')}" for o in items)
        lines.append(f"重复节点:【{node_id}】 | 数量:[{len(items)}] | {detail}")
    if not duplicates:
        lines.append("重复节点:[0]")
    if orphans:
        detail = "; ".join(f"ID={o.get('id')} CID={o.get('clientOrderId')} price={o.get('price')} side={o.get('side')}" for o in orphans)
        lines.append(f"孤儿网格单:[{len(orphans)}] | {detail}")
    logger.info("\n".join(lines + ["===================================================================\n"]))


def main_app():
    """主进程仅生成配置、启动策略子进程并守护。"""
    suffix = "0925"
    configs = [
        GridConfig(f"SHORT-QNT{suffix}", "QNT/USDT:USDT", 86, 300, 2.5, 0.1, GridDirection.SHORT, "ruru"),


    ]
    ids = [config.strategy_id for config in configs]
    duplicates = sorted({strategy_id for strategy_id in ids if ids.count(strategy_id) > 1})
    if duplicates:
        raise ValueError(f"完整策略ID重复，禁止启动以避免账本/OID串线: {duplicates}")

    processes = []
    for config in configs:
        process = multiprocessing.Process(target=run_single_strategy, args=(config,))
        process.daemon = True
        process.start()
        processes.append(process)
        logger.info(
            f"[系统/拉起策略] 账号:[{config.account_name}] | 策略:[{config.strategy_id}] | 交易对:[{config.symbol}] | "
            f"方向:[{config.direction.value}] | PID:[{process.pid}]"
        )
    logger.info(f"[系统/守护] 全部策略已启动 | 进程数:[{len(processes)}]")
    try:
        for process in processes:
            process.join()
    except (KeyboardInterrupt, SystemExit):
        logger.info("[系统/退出] 收到终止信号，daemon 子进程将随主进程退出")


if __name__ == "__main__":
    main_app()
