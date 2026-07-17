# -*- coding: utf-8 -*-
import platform
import time
import queue
import threading

import pandas as pd
from datetime import datetime

from app.signal_trade_lite.biance_order_lite import safe_init_exchange, fetch_market_precision, format_price_amount, \
    execute_order, ExecStatus, fetch_single_order
from common.common_utils import get_config, setup_logger

logger = setup_logger()

# ==========================================
# 1. 数据与统计层 (Append-Only Ledger)
# ==========================================
class GridLedger:
    def __init__(self, filename="grid_ledger.csv"):
        self.filename = filename
        self.columns = ["ts", "node_id", "cycle", "action", "client_oid", "price", "amount", "status", "msg"]
        self._init_file()

    def _init_file(self):
        import os
        if not os.path.exists(self.filename):
            pd.DataFrame(columns=self.columns).to_csv(self.filename, index=False)

    def append(self, node_id, cycle, action, client_oid, price, amount, status, msg=""):
        row = {
            "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "node_id": node_id,
            "cycle": cycle,
            "action": action,
            "client_oid": client_oid,
            "price": price,
            "amount": amount,
            "status": status,
            "msg": msg
        }
        df = pd.DataFrame([row])
        df.to_csv(self.filename, mode='a', header=False, index=False)
        logger.info(f"[LEDGER] {action} | {node_id} | CID:{client_oid} | 状态:{status} | {msg}")


# ==========================================
# 2. 领域与状态层 (GridNode & Strategy)
# ==========================================
class GridNode:
    """独立网格节点状态机 (最小业务单元)"""

    def __init__(self, node_id, target_open_price, target_close_price, quantity, precision_info):
        # 静态不可变属性
        self.node_id = node_id
        self.target_open_price, _ = format_price_amount(target_open_price, 0, precision_info)
        self.target_close_price, _ = format_price_amount(target_close_price, 0, precision_info)
        _, self.quantity = format_price_amount(0, quantity, precision_info)
        self.position_side = "LONG"

        # 动态状态属性
        self.state = "INIT"  # 状态: INIT, WAIT_OPEN, WAIT_CLOSE, IDLE, ERROR
        self.cycle_count = 0
        self.active_client_oid = ""
        self.active_exchange_oid = ""
        self.last_update_ts = time.time()

    def generate_oid(self, strategy_id, action):
        """生成确定性幂等单号: 前缀_策略ID_节点ID_动作_轮次 (限制36字符)"""
        # 示例: GD_S1_N01_B_0 (B=Buy开仓, S=Sell平仓)
        return f"GD_{strategy_id}_{self.node_id}_{action}_{self.cycle_count}"

    def process_event(self, event, exchange, symbol, strategy_id, ledger):
        """
        状态机唯一入口：处理标准事件并反转状态
        event 格式: {'client_oid': '...', 'status': 'FILLED', 'fill_price': 60000, 'fill_qty': 1.0}
        """
        # 1. 幂等拦截: 若事件的 OID 不等于节点当前期待的 OID，说明是历史延迟事件，丢弃
        if event['client_oid'] != self.active_client_oid:
            logger.debug(
                f"[NODE_IGNORE] 幂等丢弃 | {self.node_id} | 期待:{self.active_client_oid}, 收到:{event['client_oid']}")
            return

        # 2. 状态推演与动作执行
        if self.state == "WAIT_OPEN" and event['status'] == "FILLED":
            logger.info(f"[NODE_STATE] {self.node_id} | WAIT_OPEN -> WAIT_CLOSE | 开仓成交")
            ledger.append(self.node_id, self.cycle_count, "OPEN_FILLED", self.active_client_oid, event['fill_price'],
                          event['fill_qty'], "OK")

            # 状态反转：去挂平仓单(卖)
            self.state = "WAIT_CLOSE"
            self.active_client_oid = self.generate_oid(strategy_id, "S")
            self._place_limit_order(exchange, symbol, "sell", self.target_close_price, ledger)

        elif self.state == "WAIT_CLOSE" and event['status'] == "FILLED":
            logger.info(f"[NODE_STATE] {self.node_id} | WAIT_CLOSE -> WAIT_OPEN | 平仓成交 (套利+1)")
            ledger.append(self.node_id, self.cycle_count, "CLOSE_FILLED", self.active_client_oid, event['fill_price'],
                          event['fill_qty'], "OK", msg="套利完成")

            # 状态反转：轮次+1，去挂开仓单(买)
            self.cycle_count += 1
            self.state = "WAIT_OPEN"
            self.active_client_oid = self.generate_oid(strategy_id, "B")
            self._place_limit_order(exchange, symbol, "buy", self.target_open_price, ledger)

        elif event['status'] in ["CANCELED", "REJECTED"]:
            logger.warning(
                f"[NODE_STATE] {self.node_id} | 订单被撤销或拒单, 触发原样重挂 | CID:{self.active_client_oid}")
            ledger.append(self.node_id, self.cycle_count, "ORDER_CANCELED", self.active_client_oid, 0, 0, "WARN",
                          msg="触发补挂")
            # 保持状态不变，复用原OID重挂
            target_price = self.target_open_price if self.state == "WAIT_OPEN" else self.target_close_price
            side = "buy" if self.state == "WAIT_OPEN" else "sell"
            self._place_limit_order(exchange, symbol, side, target_price, ledger)

    def _place_limit_order(self, exchange, symbol, side, price, ledger):
        """执行挂单动作并更新自身记录"""
        res = execute_order(
            exchange=exchange, symbol=symbol, side=side, amount=self.quantity,
            client_oid=self.active_client_oid, order_type='limit', price=price,
            reduce_only=False, position_side=self.position_side
        )
        self.last_update_ts = time.time()

        if res.status == ExecStatus.OK:
            self.active_exchange_oid = res.exchange_oid
            ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER", self.active_client_oid, price, self.quantity,
                          "OK")
        elif res.status == ExecStatus.UNKNOWN:
            # 网络断开，转入防御，等待看门狗对账修复
            logger.critical(f"[NODE_DEFENSE] {self.node_id} 下单状态未知，等待对账介入 | CID:{self.active_client_oid}")
            ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER", self.active_client_oid, price, self.quantity,
                          "UNKNOWN")
        else:
            # 业务拒单（如余额不足），进入 ERROR 挂起状态
            self.state = "ERROR"
            logger.error(f"[NODE_ERROR] {self.node_id} 下单被明确拒绝，节点挂起 | {res.error_msg}")
            ledger.append(self.node_id, self.cycle_count, "PLACE_ORDER", self.active_client_oid, price, self.quantity,
                          "ERROR", msg=res.error_msg)


class GridStrategy:
    """策略主控: 包含并管理 N 个 Node"""

    def __init__(self, strategy_id, symbol, exchange, ledger):
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.exchange = exchange
        self.ledger = ledger
        self.nodes = {}  # {node_id: GridNode}
        self.precision_info = fetch_market_precision(exchange, symbol)
        self.event_queue = queue.Queue()  # 事件总线 (WS和REST对账产生的事件都在这里排队)

    def generate_nodes(self, min_price, max_price, grid_num, quantity):
        """初始化生成节点拓扑"""
        step = (max_price - min_price) / grid_num
        for i in range(grid_num):
            node_id = f"N{i:03d}"
            open_price = min_price + i * step
            close_price = open_price + step
            self.nodes[node_id] = GridNode(node_id, open_price, close_price, quantity, self.precision_info)
        logger.info(f"[STRATEGY] 成功生成 {len(self.nodes)} 个网格节点区间: [{min_price} - {max_price}]")

    def initialize_market_placement(self, current_price):
        """
        初始化铺位核心逻辑 (解决现价穿越问题)
        1. 现价下方的节点: 挂 WAIT_OPEN (Limit Buy)
        2. 现价上方的节点: 聚合市价买入总底仓，然后逐个挂 WAIT_CLOSE (Limit Sell)
        """
        buy_nodes = []
        sell_nodes = []

        for node in self.nodes.values():
            if node.target_open_price < current_price:
                buy_nodes.append(node)
            else:
                sell_nodes.append(node)

        logger.info(
            f"[INIT_PLACE] 现价 {current_price} | 需挂买单节点:{len(buy_nodes)} | 需建底仓并挂卖单节点:{len(sell_nodes)}")

        # 阶段 A: 处理现价上方的节点 (先聚合市价建仓，再挂出平仓卖单)
        if sell_nodes:
            total_qty = sum(node.quantity for node in sell_nodes)
            agg_buy_oid = f"GD_{self.strategy_id}_INIT_AGG_BUY"
            logger.info(f"[INIT_PLACE] 执行聚合市价底仓买入 | 总量: {total_qty}")

            res = execute_order(self.exchange, self.symbol, "buy", total_qty, agg_buy_oid, order_type="market",
                                position_side="LONG")
            if res.status != ExecStatus.OK:
                logger.critical("[INIT_PLACE] 聚合建仓失败，中止初始化流程！请人工干预。")
                raise Exception("Initialization Aggregated Buy Failed")

            time.sleep(1)  # 节流缓冲

            # 挂卖单
            for node in sell_nodes:
                node.state = "WAIT_CLOSE"
                node.active_client_oid = node.generate_oid(self.strategy_id, "S")
                node._place_limit_order(self.exchange, self.symbol, "sell", node.target_close_price, self.ledger)
                time.sleep(0.05)  # 简易节流

        # 阶段 B: 处理现价下方的节点 (逐个挂买单)
        for node in buy_nodes:
            node.state = "WAIT_OPEN"
            node.active_client_oid = node.generate_oid(self.strategy_id, "B")
            node._place_limit_order(self.exchange, self.symbol, "buy", node.target_open_price, self.ledger)
            time.sleep(0.05)

    def run_main_loop(self):
        """单线程安全主循环：不断消费事件驱动状态机"""
        logger.info("[MAIN_LOOP] 策略主循环启动，开始监听事件...")
        while True:
            try:
                event = self.event_queue.get(timeout=1.0)
                # event = {"client_oid": "...", "status": "FILLED", "fill_price":..., "fill_qty":...}

                # 路由到具体节点 (通过解析 CID: GD_S1_N01_B_0)
                parts = event['client_oid'].split('_')
                if len(parts) >= 3 and parts[0] == "GD" and parts[1] == self.strategy_id:
                    node_id = parts[2]
                    if node_id in self.nodes:
                        # 单向串行推演
                        self.nodes[node_id].process_event(event, self.exchange, self.symbol, self.strategy_id,
                                                          self.ledger)
                    else:
                        logger.warning(f"[EVENT] 找不到对应的节点ID: {node_id}")
            except queue.Empty:
                pass
            except Exception as e:
                logger.error(f"[MAIN_LOOP] 事件处理异常: {e}")
                time.sleep(1)


# ==========================================
# 3. 对账与事件层 (Reconciliation Watchdog)
# ==========================================
class ReconcilerThread(threading.Thread):
    """
    REST 看门狗独立线程。统一产出"标准事件"推入主 Event Queue。
    实现：交易所为真相源的混合对账。
    """

    def __init__(self, strategy, interval_sec=30):
        super().__init__()
        self.strategy = strategy
        self.interval = interval_sec
        self.daemon = True

    def run(self):
        logger.info(f"[WATCHDOG] 启动混合对账看门狗，周期 {self.interval} 秒")
        while True:
            time.sleep(self.interval)
            try:
                self._run_reconciliation()
            except Exception as e:
                logger.error(f"[WATCHDOG] 对账轮次异常: {e}")

    def _run_reconciliation(self):
        t0 = time.perf_counter()
        exchange = self.strategy.exchange
        symbol = self.strategy.symbol
        strategy_id = self.strategy.strategy_id

        # 1. 获取全局活动挂单快照
        open_orders = exchange.fetch_open_orders(symbol)

        # 2. 隔离过滤：只认本策略生成的单
        active_exchange_cids = set()
        prefix = f"GD_{strategy_id}_"
        for o in open_orders:
            cid = o.get('clientOrderId', '')
            if cid.startswith(prefix):
                active_exchange_cids.add(cid)

        # 3. 对账算法 (Diff)
        nodes_to_check = []
        for node in self.strategy.nodes.values():
            if node.state in ["WAIT_OPEN", "WAIT_CLOSE"]:
                # 排除最近 5 秒刚动过的节点(防止状态机和REST延迟形成数据竞态)
                if time.time() - node.last_update_ts < 5.0:
                    continue

                expected_cid = node.active_client_oid
                # 如果本地期待挂单，但在交易所快照中消失了！
                if expected_cid not in active_exchange_cids:
                    nodes_to_check.append(node)

        if not nodes_to_check:
            return  # 一致，无事发生

        logger.warning(f"[WATCHDOG] 发现 {len(nodes_to_check)} 个节点存在掉单悬挂，发起兜底查询...")

        # 4. 点查补救 (点查确认 -> 合成标准事件推入队列)
        for node in nodes_to_check:
            cid = node.active_client_oid
            order_info = fetch_single_order(exchange, symbol, cid)
            if not order_info:
                continue

            status = order_info['status'].upper()

            # 合成标准事件
            synthetic_event = {
                'client_oid': cid,
                'status': 'UNKNOWN',
                'fill_price': float(order_info.get('average', 0) or order_info.get('price', 0)),
                'fill_qty': float(order_info.get('filled', 0))
            }

            if status == "CLOSED":
                logger.info(f"[WATCHDOG_FIX] 确认为遗漏的成交单，合成 FILLED 事件 | CID:{cid}")
                synthetic_event['status'] = "FILLED"
                self.strategy.event_queue.put(synthetic_event)

            elif status in ["CANCELED", "EXPIRED", "REJECTED"]:
                logger.warning(f"[WATCHDOG_FIX] 确认为被撤销/失效单，合成 CANCELED 事件促发补挂 | CID:{cid}")
                synthetic_event['status'] = "CANCELED"
                self.strategy.event_queue.put(synthetic_event)

            time.sleep(0.1)  # 点查限流保护


# ==========================================
# 4. 顶层入口 (启动脚本)
# ==========================================
def main_app():
    api_key = get_config("myself_biance_api_key")
    secret_key = get_config("myself_biance_api_secret")

    if platform.system().lower() == "linux":
        proxies, proxy_url = None, None
    else:
        proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
        proxy_url = "http://127.0.0.1:7890"

    SYMBOL = "SOL/USDT:USDT"
    STRATEGY_ID = "S01"

    # 初始化网关 (复用你现有的 init_exchange 即可)
    exchange = safe_init_exchange(api_key, secret_key, proxies)

    ledger = GridLedger()
    strategy = GridStrategy(STRATEGY_ID, SYMBOL, exchange, ledger)

    # 极简配置：60000 - 65000，10个网格，每个网格买 0.01 BTC
    strategy.generate_nodes(50, 70, grid_num=10, quantity=0.1)

    # 模拟获取当前市价
    ticker = exchange.fetch_ticker(SYMBOL)
    current_price = ticker['last']

    # 铺单初始化
    strategy.initialize_market_placement(current_price)

    # 启动看门狗对账线程
    watchdog = ReconcilerThread(strategy, interval_sec=30)
    watchdog.start()

    # 【扩展】：如果你有 WS 监听器，只需在 WS 的 on_message 回调中，
    # 解析出 status="FILLED" 且 clientOrderId 以 "GD_S01" 开头的数据，
    # 包装成 `{'client_oid': x, 'status': 'FILLED', ...}` 调用 `strategy.event_queue.put(event)` 即可。

    # 阻塞启动主循环
    strategy.run_main_loop()


if __name__ == "__main__":
    main_app()
    pass