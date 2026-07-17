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
        """生成绝对确定的唯一单号: 前缀_策略ID_节点ID_动作_轮次_毫秒级时间戳后缀 (限制36字符)"""
        # 示例: GD_S1_N01_B_0_12345678 (即使反复重试单号也具有唯一性，彻底消除混淆)
        ts_suffix = str(int(time.time() * 1000))[-8:]
        return f"GD_{strategy_id}_{self.node_id}_{action}_{self.cycle_count}_{ts_suffix}"

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
            # 保持状态不变，但生成全新的单号重挂 (自动治愈外部环境干扰)
            target_price = self.target_open_price if self.state == "WAIT_OPEN" else self.target_close_price
            side = "buy" if self.state == "WAIT_OPEN" else "sell"

            action_code = "B" if self.state == "WAIT_OPEN" else "S"
            self.active_client_oid = self.generate_oid(strategy_id, action_code)
            self._place_limit_order(exchange, symbol, side, target_price, ledger)

    def _place_limit_order(self, exchange, symbol, side, price, ledger):
        """执行挂单动作并更新自身记录"""

        # 核心：预写式意向记账（WAL），留下“犯罪现场”
        ledger.append(self.node_id, self.cycle_count, "INTENT", self.active_client_oid, price, self.quantity,
                      "PENDING", msg="准备发送挂单网络包")

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

    def generate_nodes(self, min_price, max_price, price_ratio, quantity):
        """等比初始化生成节点拓扑，并处理由于价格过低导致的精度碰撞问题"""
        current_close = max_price
        ratio_factor = 1.0 + price_ratio / 100.0
        i = 0

        while current_close > min_price:
            raw_open = current_close / ratio_factor

            # 修约格式化
            fmt_close, _ = format_price_amount(current_close, 0, self.precision_info)
            fmt_open, _ = format_price_amount(raw_open, 0, self.precision_info)

            # 【等比网格的精度碰撞拦截】
            # 如果经过 tickSize 精度修约后，计算出的开仓价竟然 >= 平仓价，网格发生重叠塌陷。
            if fmt_open >= fmt_close:
                logger.warning(f"[STRATEGY] 价格 {current_close} 处触发精度碰撞 (修约后均为 {fmt_close})，"
                               f"此区间的 {price_ratio}% 等比价差已小于交易所的最小刻度 (tickSize)。终止生成后续下沿节点。")
                break

            if fmt_open < min_price:
                break

            node_id = f"N{i:03d}"
            # 创建等比节点，下沿为格式化后的开仓价，上沿为格式化后的平仓价
            self.nodes[node_id] = GridNode(node_id, fmt_open, fmt_close, quantity, self.precision_info)

            # 严格以当前的格式化开仓价，作为下个节点的平仓价 (完美拼合相连)
            current_close = fmt_open
            i += 1

        logger.info(
            f"[STRATEGY] 成功生成 {len(self.nodes)} 个等比网格节点区间: [{min_price} - {max_price}], Ratio: {price_ratio}%")

    def run_reconciliation(self, is_startup=False):
        """
        [大一统对账引擎]
        涵盖冷启动恢复与运行时掉单修复，基于 WAL 与快照比对。
        """
        prefix = f"GD_{self.strategy_id}_"

        # 1. 获取全局活动挂单快照 (降频降消耗，优先快照过滤)
        try:
            open_orders = self.exchange.fetch_open_orders(self.symbol)
        except Exception as e:
            logger.error(f"[RECONCILE] 获取全局挂单快照失败: {e}")
            return

        active_exchange_cids = {o.get('clientOrderId', '') for o in open_orders if
                                o.get('clientOrderId', '').startswith(prefix)}

        # 加载本地账本(仅在冷启动时按需读取，优化运行时性能)
        df = None
        if is_startup:
            import os
            if os.path.exists(self.ledger.filename):
                try:
                    df = pd.read_csv(self.ledger.filename)
                except Exception as e:
                    logger.error(f"[RECONCILE] 读取本地账本失败: {e}")

        # 2. 对账算法 (Diff 检查与时光机推演)
        nodes_to_point_check = []

        for node_id, node in self.nodes.items():
            # ====================================================
            # 场景 A: 冷启动对账 (状态 INIT，需从硬盘 WAL 回溯)
            # ====================================================
            if is_startup and node.state == "INIT":
                if df is None or df.empty or node_id not in df['node_id'].values:
                    continue

                node_df = df[df['node_id'] == node_id]
                if node_df.empty:
                    continue

                # 提取最新 3 个 OID 进行多级回溯
                cids = node_df['client_oid'].drop_duplicates().tolist()[-3:]
                cids.reverse()  # [N, N-1, N-2]

                truth_order = None
                truth_cid = None

                for cid in cids:
                    # 极速路径：如果在快照里，直接免除 API 点查！
                    if cid in active_exchange_cids:
                        truth_order = next((o for o in open_orders if o.get('clientOrderId') == cid), None)
                        if truth_order:
                            truth_cid = cid
                            break

                    # 兜底路径：快照没有，利用点查接口强校验是否已成交/撤销
                    try:
                        order_info = fetch_single_order(self.exchange, self.symbol, cid)
                        time.sleep(0.05)  # 点查限流
                        if order_info:
                            truth_order = order_info
                            truth_cid = cid
                            break
                    except Exception:
                        pass

                if truth_order:
                    self._align_node_state(node, truth_cid, truth_order)
                else:
                    logger.warning(
                        f"[RECONCILE] {node_id} 回溯了3个历史单号均查无此单，判定为未送达的幽灵单，节点等待重新铺设。")

            # ====================================================
            # 场景 B: 运行时对账 (状态 WAIT_*，检测是否静默掉单)
            # ====================================================
            elif not is_startup and node.state in ["WAIT_OPEN", "WAIT_CLOSE"]:
                # 排除最近 5 秒刚动过的节点(防止状态机与 REST 快照延迟形成数据竞态)
                if time.time() - node.last_update_ts < 5.0:
                    continue

                expected_cid = node.active_client_oid
                # 期待挂单但在交易所快照中消失，加入点查队列
                if expected_cid not in active_exchange_cids:
                    nodes_to_point_check.append(node)

        # 3. 运行时的掉单点查与修复
        if not is_startup and nodes_to_point_check:
            logger.warning(f"[WATCHDOG] 发现 {len(nodes_to_point_check)} 个节点存在掉单悬挂，发起兜底查询...")
            for node in nodes_to_point_check:
                cid = node.active_client_oid
                try:
                    order_info = fetch_single_order(self.exchange, self.symbol, cid)
                    if order_info:
                        self._align_node_state(node, cid, order_info)
                    else:
                        # 【修复点】：运行时查无此单，绝对是网络幽灵单！立刻合成 CANCELED 触发原样重挂
                        logger.warning(f"[WATCHDOG_FIX] 确认为未送达币安的幽灵单，合成 CANCELED 触发重挂 | CID:{cid}")
                        synthetic_event = {
                            'client_oid': cid,
                            'status': 'CANCELED',
                            'fill_price': 0, 'fill_qty': 0
                        }
                        self.event_queue.put(synthetic_event)
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"[WATCHDOG] 点查异常 CID:{cid} | {e}")

    def _align_node_state(self, node, truth_cid, truth_order):
        """核心私有方法：根据交易所真相，强制拨正节点指针并产出合成事件"""
        parts = truth_cid.split('_')
        if len(parts) >= 5:
            action = parts[3]  # 'B' or 'S'
            cycle = int(parts[4])

            # 拨正内存指针
            node.cycle_count = cycle
            node.active_client_oid = truth_cid
            node.active_exchange_oid = truth_order.get('id', '')

            status = truth_order.get('status', '').upper()
            logger.info(f"[RECONCILE_ALIGN] 锁定真相锚点 | {node.node_id} | CID:{truth_cid} | 状态:{status}")

            # 基础推演：节点停留在发起该意图的状态
            node.state = "WAIT_OPEN" if action == "B" else "WAIT_CLOSE"

            # 顺滑事件分发
            if status in ["CLOSED", "FILLED"]:
                synthetic_event = {
                    'client_oid': truth_cid,
                    'status': 'FILLED',
                    'fill_price': float(truth_order.get('average', 0) or truth_order.get('price', 0)),
                    'fill_qty': float(truth_order.get('filled', 0))
                }
                self.event_queue.put(synthetic_event)
            elif status in ["CANCELED", "EXPIRED", "REJECTED"]:
                synthetic_event = {
                    'client_oid': truth_cid,
                    'status': 'CANCELED',
                    'fill_price': 0,
                    'fill_qty': 0
                }
                self.event_queue.put(synthetic_event)

    def initialize_market_placement(self, current_price):
        """
        初始化铺位核心逻辑 (高内聚版本)
        彻底取消对"现价上下"的杂糅判断。所有新节点统一挂出独立的【限价买单】。
        - 现价下方的节点，会被币安挂在盘口正常等待。
        - 现价上方的节点，会被币安撮合引擎作为 Taker 瞬间成交。
        随后触发对账引擎，利用大一统的对账逻辑自动处理瞬间成交的单据。
        """
        logger.info(f"[INIT_PLACE] 开始执行铺单初始化，当前参考现价: {current_price}")
        init_count = 0

        for node in self.nodes.values():
            if node.state != "INIT":
                continue  # 经过冷启动对账，证明它是老节点且有正在进行中的逻辑，跳过挂单

            init_count += 1
            # 统一动作：所有新节点无差别进入开仓等待，并挂限价买单
            node.state = "WAIT_OPEN"
            node.active_client_oid = node.generate_oid(self.strategy_id, "B")
            node._place_limit_order(self.exchange, self.symbol, "buy", node.target_open_price, self.ledger)
            time.sleep(0.05)  # 限流保护

        if init_count == 0:
            logger.info("[INIT_PLACE] 所有节点均已从硬盘账本顺利恢复进度，无需新增初始铺位。")
        else:
            logger.info(f"[INIT_PLACE] 成功发送 {init_count} 个新节点的初始买单。正呼叫对账引擎接管越价成交的底仓单...")

            # 给币安撮合引擎以及本地网络极短的缓冲时间
            time.sleep(1.0)

            # 关键：主动触发一次非启动期的运行时对账。
            # 那些挂价高于现价的限价买单，会被币安瞬间撮合成交。
            # 对账引擎会发现这些单不在挂单簿里，进而查出 FILLED 状态并往总线推入事件。
            # 从而完美闭环驱动主循环挂出对应的卖单！
            self.run_reconciliation(is_startup=False)

    def run_main_loop(self):
        """单线程安全主循环：不断消费事件驱动状态机"""
        logger.info("[MAIN_LOOP] 策略主循环启动，开始监听事件...")
        while True:
            try:
                event = self.event_queue.get(timeout=1.0)
                # event = {"client_oid": "...", "status": "FILLED", "fill_price":..., "fill_qty":...}

                # 路由到具体节点 (通过解析 CID: GD_S1_N01_B_0_12345678)
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
    REST 看门狗独立线程。调用统一对账引擎，产出标准事件推入 Event Queue。
    """

    def __init__(self, strategy, interval_sec=30):
        super().__init__()
        self.strategy = strategy
        self.interval = interval_sec
        self.daemon = True

    def run(self):
        logger.info(f"[WATCHDOG] 启动看门狗线程，周期 {self.interval} 秒")
        while True:
            time.sleep(self.interval)
            try:
                # 调用统一大引擎 (is_startup=False)
                self.strategy.run_reconciliation(is_startup=False)
            except Exception as e:
                logger.error(f"[WATCHDOG] 统一对账轮次异常: {e}")


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

    # 初始化网关
    exchange = safe_init_exchange(api_key, secret_key, proxies)

    ledger = GridLedger()
    strategy = GridStrategy(STRATEGY_ID, SYMBOL, exchange, ledger)

    # 生成等比网格: 50 到 70区间，每格跨度2%，单个网格仓位 0.1
    strategy.generate_nodes(50, 70, price_ratio=2, quantity=0.1)

    # 模拟获取当前市价
    ticker = exchange.fetch_ticker(SYMBOL)
    current_price = ticker['last']

    # 强制在第一单网络请求发生前，调用统一引擎执行冷启动对账（防重启爆铺、断电掉单）
    strategy.run_reconciliation(is_startup=True)

    # 铺单初始化 (已在内部实现跳过已恢复的活跃挂单，杜绝重复铺单)
    strategy.initialize_market_placement(current_price)

    # 启动看门狗对账线程 (复用 run_reconciliation 统一逻辑)
    watchdog = ReconcilerThread(strategy, interval_sec=30)
    watchdog.start()

    # 阻塞启动主循环
    strategy.run_main_loop()


if __name__ == "__main__":
    main_app()
    pass