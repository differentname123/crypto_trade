import time
import sqlite3
import threading
from queue import Queue
from enum import Enum
from dataclasses import dataclass


# 假设这里 import 了所有的网关函数
# from biance_order_lite import execute_order, get_symbol_status, ExecStatus, logger
# from common_utils_lite import ...

# ==========================================
# 0. 核心契约与模型
# ==========================================
class NodeState(Enum):
    WAIT_OPEN = "WAIT_OPEN"
    WAIT_CLOSE = "WAIT_CLOSE"


@dataclass
class StdEvent:
    """标准事件载体，隔离底层异构数据"""
    client_oid: str
    event_type: str  # 'FILLED' or 'CANCELED'
    fill_price: float = 0.0
    source: str = "WATCHDOG"


class GridNode:
    """绝对纯粹的独立网格状态机"""

    def __init__(self, node_id, open_price, close_price, quantity, strat_id):
        self.node_id = node_id
        self.target_open_price = float(open_price)
        self.target_close_price = float(close_price)
        self.quantity = float(quantity)
        self.strat_id = strat_id
        self.prefix = f"CG_{strat_id}"

        # 动态状态
        self.state = NodeState.WAIT_OPEN
        self.cycle_count = 0
        self.active_client_oid = ""
        self.last_update_ts = time.time()

    def gen_oid(self, action: str) -> str:
        """格式: 前缀_策略ID_节点ID_动作_轮次"""
        return f"{self.prefix}_{self.node_id:04d}_{action}_{self.cycle_count:04d}"

    def execute_transition(self, event: StdEvent, exec_func, log_func):
        """状态机唯一入口，严格幂等"""
        if event.client_oid != self.active_client_oid:
            return  # 不是当前期待的订单事件，直接丢弃（防重放）

        self.last_update_ts = time.time()

        if self.state == NodeState.WAIT_OPEN:
            if event.event_type == 'FILLED':
                log_func(self.node_id, "OPEN_FILLED", event.fill_price)
                # 状态反转：挂平仓卖单
                self.active_client_oid = self.gen_oid("C")
                self.state = NodeState.WAIT_CLOSE
                exec_func('sell', self.quantity, self.target_close_price, self.active_client_oid, reduce_only=True)

            elif event.event_type == 'CANCELED':
                # 异常撤单，原地重新铺单补齐
                exec_func('buy', self.quantity, self.target_open_price, self.active_client_oid, reduce_only=False)

        elif self.state == NodeState.WAIT_CLOSE:
            if event.event_type == 'FILLED':
                log_func(self.node_id, "CLOSE_FILLED", event.fill_price)
                # 完成一个套利周期，准备下一轮开仓
                self.cycle_count += 1
                self.active_client_oid = self.gen_oid("O")
                self.state = NodeState.WAIT_OPEN
                exec_func('buy', self.quantity, self.target_open_price, self.active_client_oid, reduce_only=False)

            elif event.event_type == 'CANCELED':
                exec_func('sell', self.quantity, self.target_close_price, self.active_client_oid, reduce_only=True)


# ==========================================
# 1. 策略引擎主控
# ==========================================
class GridStrategyEngine:
    def __init__(self, exchange, symbol, strat_id):
        self.exchange = exchange
        self.symbol = symbol
        self.strat_id = strat_id
        self.nodes = {}  # type: dict[int, GridNode]
        self.event_bus = Queue()  # 统一事件总线

        # 使用 SQLite 替代 Pandas，对高频单行插入更友好、且同样安全
        self.db = sqlite3.connect(f'grid_{strat_id}.db', check_same_thread=False)
        self.db.execute('''CREATE TABLE IF NOT EXISTS journal
                           (ts REAL, node_id INT, action TEXT, price REAL)''')
        self.db.commit()

    def _log_journal(self, node_id, action, price):
        self.db.execute("INSERT INTO journal VALUES (?, ?, ?, ?)", (time.time(), node_id, action, price))
        self.db.commit()

    def _executor(self, side, amount, price, client_oid, reduce_only):
        """代理执行下单，解耦状态机与网络IO"""
        res = execute_order(self.exchange, self.symbol, side, amount, client_oid,
                            order_type='limit', price=price, reduce_only=reduce_only, position_side="LONG")
        if res.status == ExecStatus.REJECT:
            logger.error(f"[EXEC_ERR] 单号 {client_oid} 遭拒，等待看门狗自动补挂")

    def initialize(self, lower, upper, grids, quantity_per_grid):
        """全量建仓（带有穿轴市价聚合）"""
        ticker = self.exchange.fetch_ticker(self.symbol)
        mark_price = float(ticker['last'])
        interval = (upper - lower) / grids

        batch_reqs = []
        market_qty = 0.0

        for i in range(grids):
            op = lower + i * interval
            cp = op + interval
            node = GridNode(i, op, cp, quantity_per_grid, self.strat_id)
            self.nodes[i] = node

            if op < mark_price:
                node.state = NodeState.WAIT_OPEN
                node.active_client_oid = node.gen_oid("O")
                batch_reqs.append({
                    'symbol': self.symbol, 'type': 'limit', 'side': 'buy',
                    'amount': node.quantity, 'price': op,
                    'params': {'newClientOrderId': node.active_client_oid, 'positionSide': 'LONG'}
                })
            else:
                # 高于市价，默认被吃单，直接挂出平仓单
                market_qty += node.quantity
                node.state = NodeState.WAIT_CLOSE
                node.active_client_oid = node.gen_oid("C")
                batch_reqs.append({
                    'symbol': self.symbol, 'type': 'limit', 'side': 'sell',
                    'amount': node.quantity, 'price': cp,
                    'params': {'newClientOrderId': node.active_client_oid, 'positionSide': 'LONG', 'reduceOnly': True}
                })

        # 1. 补齐底仓
        if market_qty > 0:
            logger.info(f"发送市价多单 {market_qty} 补齐穿越轴")
            execute_order(self.exchange, self.symbol, 'buy', market_qty, f"CG_INIT_{self.strat_id}",
                          order_type='market', position_side="LONG")
        # 2. 批量限价单铺设
        if batch_reqs:
            batch_place_orders(self.exchange, self.symbol, batch_reqs)

    # ==========================================
    # 2. 线程1: 事件驱动核心机 (无锁单线程，保证一致性)
    # ==========================================
    def run_event_loop(self):
        logger.info("[ENGINE] 事件消费循环启动")
        while True:
            event = self.event_bus.get()  # 阻塞等待事件

            # 解析归属节点 (格式: CG_策略_节点_动作_轮次)
            parts = event.client_oid.split('_')
            if len(parts) == 5 and parts[1] == self.strat_id:
                node_id = int(parts[2])
                if node_id in self.nodes:
                    self.nodes[node_id].execute_transition(event, self._executor, self._log_journal)

    # ==========================================
    # 3. 线程2: 三级穿透看门狗 (借鉴了你的架构精髓)
    # ==========================================
    def run_watchdog(self):
        logger.info("[WATCHDOG] 混合对账引擎启动")
        while True:
            time.sleep(15)  # 频控休息，因网格需要响应速度，频率略高于小时级

            # [L1级] 拉取本策略活跃挂单
            st, open_dict = fetch_strategy_open_orders(self.exchange, self.symbol, f"CG_{self.strat_id}")
            if st != ExecStatus.OK:
                continue

            recent_dict = None  # 懒加载 L2

            for node_id, node in self.nodes.items():
                # 保护机制：刚执行完动作的节点免除5秒钟盘查，防止 API 延迟导致的误判
                if time.time() - node.last_update_ts < 5.0:
                    continue

                # 发现丢失：内存认为挂单中，但交易所快照里没有！
                if node.active_client_oid not in open_dict:
                    oid = node.active_client_oid
                    logger.warning(f"[WATCHDOG] 发现幽灵订单 OID:{oid} 不在快照中，启动多级穿透查证")

                    ccxt_status = None
                    fill_price = 0.0

                    # [L2级] 懒加载批量拉取近期订单
                    if recent_dict is None:
                        _, recent_dict = fetch_recent_orders_dict(self.exchange, self.symbol)

                    if oid in recent_dict:
                        ccxt_status = str(recent_dict[oid].get('status', '')).lower()
                        fill_price = float(recent_dict[oid].get('average', 0) or recent_dict[oid].get('price', 0))
                    else:
                        # [L3级] 终极单笔兜底
                        st3, order_info = fetch_specific_order(self.exchange, self.symbol, oid)
                        if st3 == ExecStatus.OK and order_info:
                            ccxt_status = str(order_info.get('status', '')).lower()
                            fill_price = float(order_info.get('average', 0) or order_info.get('price', 0))

                    # 将物理实况转译为引擎标准事件，推入事件总线
                    if ccxt_status == 'closed':
                        logger.info(f"[WATCHDOG] 查证完毕，订单已隐式成交，下发合成 FILLED 事件")
                        self.event_bus.put(StdEvent(oid, 'FILLED', fill_price))
                    elif ccxt_status in ('canceled', 'expired', 'rejected'):
                        logger.info(f"[WATCHDOG] 查证完毕，订单意外失效，下发合成 CANCELED 事件")
                        self.event_bus.put(StdEvent(oid, 'CANCELED'))

            # (可选) 在此调用 get_symbol_status 进行持仓一致性告警，同你原有的逻辑

    def start(self):
        threading.Thread(target=self.run_event_loop, daemon=True).start()
        threading.Thread(target=self.run_watchdog, daemon=True).start()

        # 挂起主线程
        while True:
            time.sleep(1)


# ==========================================
# 启动示例
# ==========================================
if __name__ == "__main__":
    import common_utils_lite as utils

    api_key = utils.get_config("api_key")
    secret = utils.get_config("secret")

    # 1. 强力初始化
    exchange = safe_init_exchange(api_key, secret)

    # 2. 建立引擎
    engine = GridStrategyEngine(exchange, "BTC/USDT", "S01")

    # 3. 初始铺单 (例如: 10个格子，从90000铺到95000)
    # engine.initialize(90000, 95000, 10, 0.01)

    # 4. 启动并发引擎
    # engine.start()