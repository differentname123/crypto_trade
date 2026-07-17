# -*- coding: utf-8 -*-
"""
等比网格交易引擎 —— 事件驱动 + 单写者状态机 + WAL 账本 + 对账自愈
(本版: 交易逻辑保持不变, 将"日志 + 账本"重写为面向人的运营视图)

════════════════ 30 秒读懂日志 ════════════════
只需盯三类信息:
  1) [DASH] 网格看板 —— 每 60s 一张全局快照(兼作心跳):
       节点分布 / 完成几轮套利赚了多少 / 哪些格出错及原因 / 最近动态。
       长时间没有 [FILL] 不代表卡死: 只要 [DASH] 还在刷新, 就是在正常蹲行情。
  2) [FILL] 成交叙事 ——
       「买入成交」= 某格接到货, 已自动挂出对应卖单(附目标毛利);
       「卖出成交」= 该格完成一轮套利, 直接给出本轮毛利与累计毛利。
  3) ERROR / WARNING —— 全部带人话解释与建议动作:
       [PLACE]被拒 = 挂单失败原因(价格越界/保证金不足/超持仓等);
       [HEAL] = 挂单丢失后自动补挂; [WATCHDOG] = 核实"从盘口消失的单"的过程。

════════════════ 30 秒读懂账本 CSV ════════════════
一行 = 一个事件, 直接看最后一列 summary(人话); 其余列供筛选统计:
  event : 下单意图/挂单成功/挂单被拒/挂单未知/买入成交/卖出成交/订单补挂/系统
  profit, total_profit : 仅「卖出成交」行有值 = 该轮毛利 / 本进程累计毛利(未扣手续费)
  cycle : 机器轮次从 0 计; summary 里的"第 N 轮" = cycle + 1
  「下单意图」= WAL 预写(先记账后发单), 崩溃后重启靠它回查"这张单到底发出去没有"。

════════════════ 架构(未变) ════════════════
  基础设施层: GridLedger(CSV/WAL) | ExchangeBroker(收拢全部 CCXT 调用)
  领域层    : GridNode(纯内存状态机, 仅主线程可写)
  对账/调度 : ReconciliationEngine(只读产事件) | GridStrategy(路由+主循环) | ReconcilerThread
  并发基石  : 单一写者 —— 节点状态只在主线程 process_event 修改; 看门狗只读+投递事件;
              过时 OID 事件在主线程被幂等拦截。
"""
import os
import csv
import time
import queue
import platform
import threading
from enum import Enum
from datetime import datetime
from collections import Counter

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
RECONCILE_SKIP_WINDOW_SEC = 5.0  # 运行时对账跳过"刚动过"的节点, 规避 REST 快照传播延迟造成的误判
POINT_CHECK_DELAY_COLD = 0.05  # 冷启动点查限流
POINT_CHECK_DELAY_RUNTIME = 0.1  # 运行时点查限流
PLACE_THROTTLE_SEC = 0.05  # 批量铺单限流
INIT_SETTLE_WAIT_SEC = 1.0  # 铺单后等待撮合/网络传播的缓冲
COLD_START_BACKTRACK = 3  # 冷启动每个节点回溯的历史单号数量
ORDER_GRACE_PERIOD = 5.0  # 5 秒冷静期: 刚动过(含发单在途)的节点不参与掉单判定
REPORT_INTERVAL_SEC = 60  # [DASH] 网格看板输出周期(兼系统心跳)
INIT_PROGRESS_STEP = 20  # 初始铺单每 N 格输出一次进度

# ==========================================
# 0.1 拒单人话字典 (以终为始: 报错必须告诉人"是什么+怎么办")
# ==========================================
_REJECT_RULES = [
    ("-4016", "价格越界",
     "挂单价超出交易所的价格保护带(距现价太远), 属于行情位置问题; 待价格走近后重启程序即可恢复该格"),
    ("-2019", "保证金不足",
     "可用保证金不够挂这张单; 请追加保证金, 或调小 quantity / 缩小价格区间 / 增大步长以减少格数"),
    ("-2022", "卖单被拒(超持仓)",
     "交易所判定卖出量超过实际持仓; 常见于重复卖单/孤儿单占用了持仓额度, 需人工核对该交易对的挂单"),
    ("-1021", "时间漂移", "本机时间与交易所偏差过大, 请同步系统时间"),
    ("-4164", "金额过小", "单笔名义价值低于交易所最小限制, 请调大 quantity"),
]
REJECT_ADVICE = {label: hint for _, label, hint in _REJECT_RULES}
REJECT_ADVICE["其他拒单"] = "交易所拒绝了这张单, 原因见原始报文"


def reject_label(raw_msg):
    """把交易所原始报错归类成一个短标签, 供看板/汇总分组。"""
    raw = str(raw_msg or "")
    for code, label, _ in _REJECT_RULES:
        if code in raw:
            return label
    return "其他拒单"


def reject_hint(raw_msg):
    """拒单的人话解释 + 建议动作。"""
    raw = str(raw_msg or "")
    for code, label, hint in _REJECT_RULES:
        if code in raw:
            return f"{label}: {hint}"
    return "其他拒单: 交易所拒绝了这张单, 原因见原始报文"


def compress_node_ids(ids):
    """['N000','N001','N002','N012'] -> 'N000~N002,N012' (看板/汇总专用, 压缩篇幅)。"""
    if not ids:
        return "无"
    nums = []
    for i in ids:
        try:
            nums.append(int(str(i).lstrip("N")))
        except ValueError:
            return ",".join(sorted(map(str, ids)))
    nums.sort()
    parts, s, e = [], nums[0], nums[0]
    for n in nums[1:]:
        if n == e + 1:
            e = n
        else:
            parts.append((s, e))
            s = e = n
    parts.append((s, e))
    return ",".join(f"N{a:03d}" if a == b else f"N{a:03d}~N{b:03d}" for a, b in parts)


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


# —— 账本事件名 (中文, 让 CSV 自解释) ——
EVT_INTENT = "下单意图"
EVT_PLACE_OK = "挂单成功"
EVT_PLACE_REJ = "挂单被拒"
EVT_PLACE_UNK = "挂单未知"
EVT_OPEN_FILL = "买入成交"
EVT_CLOSE_FILL = "卖出成交"
EVT_CANCEL_HEAL = "订单补挂"
EVT_SYS = "系统"


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


class GridStats:
    """全局运行计数器: 仅主线程更新(单写者), 供看板/账本读取。口径 = 本进程启动以来。"""

    def __init__(self):
        self.buy_fills = 0  # 买入成交次数
        self.sell_fills = 0  # 卖出成交次数
        self.cycles_done = 0  # 完成的套利轮数
        self.gross_profit = 0.0  # 累计毛利(未扣手续费)
        self.heals = 0  # 自动补挂次数
        self.rejects = 0  # 拒单次数
        self.unknowns = 0  # 下单结果未知次数
        self.last_event = ""  # 最近一次关键事件(人话)
        self.last_event_ts = ""

    def mark(self, desc):
        self.last_event = desc
        self.last_event_ts = datetime.now().strftime("%H:%M:%S")


class NodeContext:
    """注入给每个 Node 的运行环境, 让状态机方法拥有干净签名。"""

    def __init__(self, broker, ledger, strategy_id, stats):
        self.broker = broker
        self.ledger = ledger
        self.strategy_id = strategy_id
        self.stats = stats


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
    设计原则: 每一行都能被"不懂代码的人"读懂 —— event 用中文, summary 写完整人话;
    profit / total_profit 让盈亏直接可见; 「下单意图」行是 WAL 预写(先落盘再发单)。
    """
    COLUMNS = ["ts", "node_id", "cycle", "event", "side", "price", "amount",
               "status", "profit", "total_profit", "client_oid", "exchange_oid", "summary"]

    def __init__(self, strategy_id):
        self.strategy_id = strategy_id
        self.filename = f"grid_ledger_{strategy_id}.csv"
        if not os.path.exists(self.filename):
            with open(self.filename, 'w', newline='', encoding='utf-8') as f:
                csv.writer(f).writerow(self.COLUMNS)

    def append(self, node_id, cycle, event, client_oid, price, amount, status,
               side="", profit="", total_profit="", exchange_oid="", summary=""):
        """同步写入一行领域事件 (WAL 落盘), 并留 DEBUG 级细粒度轨迹。"""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        with open(self.filename, 'a', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow([ts, node_id, cycle, event, side, price, amount,
                                    status, profit, total_profit, client_oid, exchange_oid, summary])
        logger.debug(f"[LEDGER] {event} {node_id} cid={client_oid} {status} {summary}")

    def system(self, event, summary):
        """系统级事件行 (node_id='-', 不参与恢复回溯), 用于在账本时间线上留下里程碑。"""
        self.append("-", "", f"{EVT_SYS}|{event}", "", "", "", "INFO", summary=summary)

    def load_node_oid_history(self):
        """冷启动读取: 返回 {node_id: [client_oid,...]} (按出现序去重), 供多级回溯定位真相。"""
        history, seen = {}, {}
        if not os.path.exists(self.filename):
            return history
        try:
            with open(self.filename, 'r', newline='', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    nid = (row.get('node_id') or '')
                    cid = (row.get('client_oid') or '')
                    if not nid or not cid or nid == '-':
                        continue
                    if nid not in history:
                        history[nid], seen[nid] = [], set()
                    if cid not in seen[nid]:
                        seen[nid].add(cid)
                        history[nid].append(cid)
        except Exception as e:
            logger.error(f"[RECOVER] 读取本地账本 {self.filename} 失败: {e}")
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

        # 观测辅助 (不参与交易决策)
        self.open_fill_price = None  # 本轮真实买入成交价, 用于计算真实毛利
        self.entry_is_estimate = False  # 恢复的持仓无法得知真实成本, 按格价估算并标注
        self.last_error = ""  # 拒单原文, 供看板归因分组

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
        if self.state == NodeState.WAIT_CLOSE:
            # 重启后真实买入价已不可知, 以格价近似, 盈亏计算处会标注 "≈"
            self.open_fill_price = self.target_open_price
            self.entry_is_estimate = True

    def process_event(self, event):
        """状态机唯一入口: 幂等过滤后按 (状态, 事件) 分发到具体处理器。"""
        # 幂等拦截: 非当前期待的 OID 一律丢弃 (历史延迟 / 重复 / 已过时的对账事件)
        if event.client_oid != self.active_client_oid:
            logger.debug(f"[NODE] {self.node_id} 幂等丢弃过时事件 "
                         f"期待={self.active_client_oid} 收到={event.client_oid}")
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
        fill_price = event.fill_price or self.target_open_price
        fill_qty = event.fill_qty or self.quantity
        self.open_fill_price = fill_price
        self.entry_is_estimate = False
        self.ctx.stats.buy_fills += 1
        expect = (self.target_close_price - fill_price) * self.quantity

        self.ctx.ledger.append(
            self.node_id, self.cycle_count, EVT_OPEN_FILL, self.active_client_oid,
            fill_price, fill_qty, "OK", side="BUY",
            summary=f"第{self.cycle_count + 1}轮: 以 {fill_price} 买入 {fill_qty}, 持仓建立; "
                    f"接着挂 {self.target_close_price} 卖单, 目标毛利 {expect:+.4f} USDT")
        logger.info(f"[FILL] 「买入成交」{self.node_id} 第{self.cycle_count + 1}轮 "
                    f"@{fill_price} x{fill_qty} -> 挂卖单 @{self.target_close_price} "
                    f"(目标毛利 {expect:+.4f} USDT)")
        self.ctx.stats.mark(f"{self.node_id} 买入成交 @{fill_price}")

        self.state = NodeState.WAIT_CLOSE
        self.active_client_oid = self._new_oid(OrderAction.SELL)
        self._place_limit_order(OrderAction.SELL, self.target_close_price)

    def _on_close_filled(self, event):
        """平仓成交 -> 结算本轮毛利, 轮次+1, 状态反转, 重挂开仓买单 (完成一次套利)。"""
        fill_price = event.fill_price or self.target_close_price
        fill_qty = event.fill_qty or self.quantity
        entry = self.open_fill_price if self.open_fill_price is not None else self.target_open_price
        est_tag = "(成本按格价估算)" if self.entry_is_estimate else ""
        profit = (fill_price - entry) * (fill_qty or self.quantity)

        st = self.ctx.stats
        st.sell_fills += 1
        st.cycles_done += 1
        st.gross_profit += profit

        self.ctx.ledger.append(
            self.node_id, self.cycle_count, EVT_CLOSE_FILL, self.active_client_oid,
            fill_price, fill_qty, "OK", side="SELL",
            profit=round(profit, 6), total_profit=round(st.gross_profit, 6),
            summary=f"第{self.cycle_count + 1}轮套利完成: {entry}{est_tag} 买入 -> {fill_price} 卖出, "
                    f"毛利 {profit:+.4f} USDT(未扣手续费); 本进程累计 {st.cycles_done} 轮 / "
                    f"{st.gross_profit:+.4f} USDT; 重挂买单开启下一轮")
        logger.info(f"[FILL] 「卖出成交」{self.node_id} 第{self.cycle_count + 1}轮完成 "
                    f"@{fill_price} x{fill_qty} | 本轮毛利 {profit:+.4f} USDT{est_tag} | "
                    f"累计 {st.cycles_done} 轮 / {st.gross_profit:+.4f} USDT "
                    f"-> 重挂买单 @{self.target_open_price} 开第{self.cycle_count + 2}轮")
        st.mark(f"{self.node_id} 卖出成交 @{fill_price} 毛利{profit:+.4f}")

        self.cycle_count += 1
        self.open_fill_price = None
        self.entry_is_estimate = False
        self.state = NodeState.WAIT_OPEN
        self.active_client_oid = self._new_oid(OrderAction.BUY)
        self._place_limit_order(OrderAction.BUY, self.target_open_price)

    def _on_canceled(self):
        """撤销/拒单 -> 保持状态, 换全新单号原样重挂 (自愈外部干扰)。"""
        if self.state not in (NodeState.WAIT_OPEN, NodeState.WAIT_CLOSE):
            return  # ERROR / INIT 不参与自动重挂
        side = "BUY" if self.state == NodeState.WAIT_OPEN else "SELL"
        self.ctx.stats.heals += 1
        self.ctx.ledger.append(
            self.node_id, self.cycle_count, EVT_CANCEL_HEAL, self.active_client_oid,
            0, 0, "WARN", side=side,
            summary="挂单已失效(被交易所撤销/拒绝, 或从未送达), 自动按原价补挂一张新单")
        logger.warning(f"[HEAL] {self.node_id} 第{self.cycle_count + 1}轮 挂单失效 "
                       f"-> 原价自动补挂 | 旧CID:{self.active_client_oid}")
        self.ctx.stats.mark(f"{self.node_id} 挂单失效自动补挂")

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
        """WAL 预写意向 -> 发送挂单 -> 按结果落账并输出单条自解释日志。"""
        side = "BUY" if action == OrderAction.BUY else "SELL"
        human_round = self.cycle_count + 1
        if action == OrderAction.BUY:
            intent = (f"第{human_round}轮: 等待价格回落到 {price} 买入 {self.quantity}; "
                      f"成交后将挂 {self.target_close_price} 卖出赚差价")
        else:
            entry = self.open_fill_price if self.open_fill_price is not None else self.target_open_price
            expect = (price - entry) * self.quantity
            intent = (f"第{human_round}轮: 持仓成本≈{entry}, 等待价格涨到 {price} 卖出 {self.quantity}, "
                      f"预期毛利 {expect:+.4f} USDT")

        # WAL: 预写式意向记账, 即使随后进程崩溃也可据此追溯
        self.ctx.ledger.append(self.node_id, self.cycle_count, EVT_INTENT,
                               self.active_client_oid, price, self.quantity, "PENDING",
                               side=side, summary=intent)
        # 【最小修复】发单前先刷新时间戳: 让看门狗的冷静期覆盖"发单在途"窗口,
        # 避免慢单(>1s)在途时被点查误判为幽灵单而补挂出重复单(旧日志中 N008 双卖单的根因)。
        self.last_update_ts = time.time()

        res = self.ctx.broker.place_limit(action, self.quantity, price,
                                          self.active_client_oid, self.position_side)
        self.last_update_ts = time.time()
        tag = f"{self.node_id} {'买单' if action == OrderAction.BUY else '卖单'} @{price} x{self.quantity}"

        if res.status == ExecStatus.OK:
            self.active_exchange_oid = res.exchange_oid
            self.ctx.ledger.append(self.node_id, self.cycle_count, EVT_PLACE_OK,
                                   self.active_client_oid, price, self.quantity, "OK",
                                   side=side, exchange_oid=res.exchange_oid,
                                   summary=f"已挂上盘口。{intent}")
            logger.info(f"[PLACE] {tag} 已挂上盘口 | {intent} | "
                        f"CID:{self.active_client_oid} EID:{res.exchange_oid}")
        elif res.status == ExecStatus.UNKNOWN:
            # 网络断开, 转入防御, 等待看门狗对账修复
            self.ctx.stats.unknowns += 1
            self.ctx.ledger.append(self.node_id, self.cycle_count, EVT_PLACE_UNK,
                                   self.active_client_oid, price, self.quantity, "UNKNOWN",
                                   side=side,
                                   summary="发单期间网络中断, 结果未知; 已转入防御, "
                                           "看门狗将向交易所核实(该单可能已在盘口)")
            logger.critical(f"[PLACE] {tag} 结果未知(网络中断) -> 转防御等待看门狗核实 | "
                            f"CID:{self.active_client_oid}")
            self.ctx.stats.mark(f"{self.node_id} 下单结果未知")
        else:
            # 业务拒单(如余额不足), 进入 ERROR 挂起
            self.state = NodeState.ERROR
            self.last_error = res.error_msg or ""
            self.ctx.stats.rejects += 1
            hint = reject_hint(res.error_msg)
            self.ctx.ledger.append(self.node_id, self.cycle_count, EVT_PLACE_REJ,
                                   self.active_client_oid, price, self.quantity, "ERROR",
                                   side=side, summary=f"挂单被拒 -> 节点挂起。{hint} | 原始: {res.error_msg}")
            logger.error(f"[PLACE] {tag} 被拒 -> 节点挂起(退出轮转, 看板可见) | {hint} | "
                         f"CID:{self.active_client_oid} | 原始: {res.error_msg}")
            self.ctx.stats.mark(f"{self.node_id} 拒单({reject_label(res.error_msg)})")


def build_geometric_grid(config, broker, ctx):
    """等比生成网格节点; 低价位处若等比价差 < tickSize 则精度塌陷, 提前收口。"""
    precision = broker.fetch_precision()
    _, fmt_qty = format_price_amount(0, config.quantity, precision)
    ratio_factor = 1.0 + config.price_ratio / 100.0

    nodes = {}
    current_close = config.max_price
    i = 0
    last_node = None
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
        last_node = nodes[node_id]
        current_close = fmt_open  # 以当前开仓价作为下一节点平仓价, 无缝拼合
        i += 1

    if nodes:
        first = nodes["N000"]
        total_notional = sum(n.target_open_price * n.quantity for n in nodes.values())
        logger.info(
            f"[GRID] 生成 {len(nodes)} 个等比格 | 区间[{config.min_price}-{config.max_price}] "
            f"步长{config.price_ratio}% 每格{fmt_qty}\n"
            f"       顶格 {first.node_id}: 买{first.target_open_price}/卖{first.target_close_price} "
            f"... 底格 {last_node.node_id}: 买{last_node.target_open_price}/卖{last_node.target_close_price}\n"
            f"       若所有格同时持仓, 需名义资金 ≈ {total_notional:.2f} USDT(未含杠杆折算) "
            f"—— 请对照可用保证金, 不足时下沿格会批量拒单")
    else:
        logger.info(f"[GRID] 生成 0 个等比节点 | 区间[{config.min_price}-{config.max_price}] "
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
        self._orphan_prev = set()  # 孤儿单两轮确认缓冲(过滤换单瞬间的假阳性)
        self._orphan_warned = set()  # 已告警过的孤儿单集合(同一批只告警一次)

    # ---------- 冷启动: 允许直接拨正节点 (单线程安全) ----------
    def recover_on_startup(self, nodes):
        """多级回溯历史单号锚定真相, 拨正节点并补发事件, 防重启爆铺 / 断电掉单。"""
        active_cids, open_orders = self._snapshot()
        if open_orders is None:
            # 【安全补丁】：严禁 return 降级处理，必须立刻抛出异常物理终止进程，防止资金因错乱爆铺损失
            logger.critical("[RECOVER] 致命错误：冷启动无法取得交易所挂单快照！为防止盲目全量铺单，强制中止进程。")
            raise RuntimeError("冷启动对账基础数据获取失败，触发安全熔断。")

        history = self.ledger.load_node_oid_history()
        if not history:
            logger.info("[RECOVER] 本地账本无历史记录 -> 判定为全新首跑, 直接进入铺单")
            self.ledger.system("恢复", "全新首跑: 账本无历史记录")
            return

        aligned_buy, aligned_sell = 0, 0
        for node_id, node in nodes.items():
            if node.state != NodeState.INIT:
                continue
            candidates = list(reversed(history.get(node_id, [])[-COLD_START_BACKTRACK:]))  # 最新在前
            if not candidates:
                continue

            truth_cid, truth_order = self._resolve_truth(candidates, active_cids,
                                                         open_orders, POINT_CHECK_DELAY_COLD)
            if truth_order is not None:
                action = self._align_and_emit(node, truth_cid, truth_order)
                if action == OrderAction.BUY:
                    aligned_buy += 1
                elif action == OrderAction.SELL:
                    aligned_sell += 1
            else:
                logger.warning(f"[RECOVER] {node_id} 回溯 {COLD_START_BACKTRACK} 个历史单号均查无此单 "
                               f"-> 判为未送达的幽灵单, 该格将重新铺设")
        total = aligned_buy + aligned_sell
        logger.info(f"[RECOVER] 冷启动对账完成 | 恢复 {total} 格续跑"
                    f"(等待买入 {aligned_buy} / 持仓待卖 {aligned_sell}), 其余格将全新铺单")
        self.ledger.system("恢复", f"冷启动恢复 {total} 格(等待买入{aligned_buy}/持仓待卖{aligned_sell})")

    # ---------- 运行时: 只读 + 投递事件, 绝不改节点 (线程安全) ----------
    def repair_runtime(self, nodes, skip_recent=True):
        """只读比对锁定掉单悬挂 -> 点查确认 -> 投递事件; 全程不修改任何节点内存。"""
        active_cids, open_orders = self._snapshot()
        if open_orders is None:
            return

        # 孤儿单巡检(只告警不动作): 交易所在挂、但没有任何节点认领的本策略单
        self._sweep_orphans(nodes, active_cids)

        now = time.time()
        suspects = []  # 检测点固定 (node, cid), 规避与主线程写入的数据竞态
        for node in nodes.values():
            if time.time() - node.last_update_ts < ORDER_GRACE_PERIOD:
                continue
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
        logger.warning(f"[WATCHDOG] {len(suspects)} 个格的挂单从盘口消失"
                       f"(可能已成交/被撤/未送达), 逐一向交易所核实...")
        for node, cid in suspects:
            try:
                info = self.broker.fetch_order(cid)
                if info:
                    kind = self._emit_from_order(cid, info)
                    if kind == "FILLED":
                        logger.info(f"[WATCHDOG] {node.node_id} 核实=已成交 "
                                    f"-> 交回主循环走成交流程 | CID:{cid}")
                    elif kind == "CANCELED":
                        logger.warning(f"[WATCHDOG] {node.node_id} 核实=已被撤销/拒绝 "
                                       f"-> 通知主循环原价补挂 | CID:{cid}")
                    else:
                        logger.info(f"[WATCHDOG] {node.node_id} 核实=仍在盘口/部分成交 "
                                    f"(快照延迟造成的虚警), 不处理 | CID:{cid}")
                else:
                    # 运行时查无此单 = 未送达交易所的幽灵单, 合成 CANCELED 触发原样重挂
                    logger.warning(f"[WATCHDOG] {node.node_id} 交易所查无此单 "
                                   f"-> 判为未送达的幽灵单, 通知主循环补挂 | CID:{cid}")
                    self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))
                time.sleep(POINT_CHECK_DELAY_RUNTIME)
            except Exception as e:
                logger.error(f"[WATCHDOG] 点查异常 CID:{cid} | {e}")

    # ---------- 内部工具 ----------
    def _sweep_orphans(self, nodes, active_cids):
        """孤儿单 = 交易所在挂、但无节点跟踪的本策略单(多为历史竞态遗留)。连续两轮在场才告警。"""
        claimed = {n.active_client_oid for n in nodes.values()}
        orphans = {c for c in active_cids if c and c not in claimed}
        confirmed = orphans & self._orphan_prev
        self._orphan_prev = orphans
        if confirmed and confirmed != self._orphan_warned:
            logger.warning(f"[WATCHDOG] 发现 {len(confirmed)} 张孤儿挂单"
                           f"(交易所在挂、但已无节点跟踪, 其成交不会入账本), "
                           f"建议人工到交易所核对/撤销: {sorted(confirmed)}")
            self._orphan_warned = set(confirmed)

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
        """冷启动专用: 依真相拨正节点指针并按其状态补发事件; 返回该节点恢复出的方向。"""
        parsed = OidCodec.parse(truth_cid)
        if parsed is None:
            return None
        node.align(parsed.cycle, truth_cid, truth_order.get('id', ''), parsed.action)
        raw = str(truth_order.get('status', '')).upper()
        posture = "等待买入" if parsed.action == OrderAction.BUY else "持仓待卖"
        logger.info(f"[RECOVER] {node.node_id} 锚定真相: {posture} 第{parsed.cycle + 1}轮 "
                    f"交易所状态={raw} | CID:{truth_cid}")
        self._emit_from_order(truth_cid, truth_order)
        return parsed.action

    def _emit_from_order(self, cid, order):
        """交易所原始订单状态 -> 标准事件; 返回投递类型 FILLED/CANCELED/None(在盘或部分成交)。"""
        raw = str(order.get('status', '')).upper()
        if raw in ("CLOSED", "FILLED"):
            self.event_queue.put(OrderEvent(
                cid, OrderStatus.FILLED,
                fill_price=float(order.get('average') or order.get('price') or 0),
                fill_qty=float(order.get('filled') or 0),
            ))
            return "FILLED"
        elif raw in ("CANCELED", "EXPIRED", "REJECTED"):
            self.event_queue.put(OrderEvent(cid, OrderStatus.CANCELED))
            return "CANCELED"
        return None


# ==========================================
# 5. 主控与调度 (GridStrategy / Watchdog)
# ==========================================
class GridStrategy:
    """策略主控: 组装依赖, 持有节点集合, 负责初始铺单、事件主循环(路由)与周期看板。"""

    def __init__(self, config, broker, ledger):
        self.config = config
        self.strategy_id = config.strategy_id
        self.broker = broker
        self.ledger = ledger
        self.stats = GridStats()
        self.event_queue = queue.Queue()
        self._last_dash_ts = 0.0  # 0 => 主循环启动后立即输出第一张看板

        ctx = NodeContext(broker, ledger, config.strategy_id, self.stats)
        self.nodes = build_geometric_grid(config, broker, ctx)
        self.engine = ReconciliationEngine(broker, ledger, config.strategy_id, self.event_queue)

    def recover(self):
        """冷启动对账 (须在主循环/看门狗启动前、单线程执行): 防重启爆铺 / 断电掉单。"""
        self.engine.recover_on_startup(self.nodes)

    def initialize_market_placement(self):
        """
        铺单初始化: 所有新节点统一挂限价买单。
        现价下方 -> 正常挂盘等待; 现价上方 -> 被撮合引擎作为 Taker 瞬时成交(正常吃底仓行为)。
        随后即时对账(skip_recent=False)接管越价成交, 闭环驱动挂出对应卖单。
        """
        current_price = self.broker.fetch_last_price()

        todo = [n for n in self.nodes.values() if n.state == NodeState.INIT]
        if not todo:
            logger.info(f"[INIT] 全部格已从账本恢复进度, 无需新增铺位 | 参考现价 {current_price}")
            return

        logger.info(f"[INIT] 现价 {current_price} | 开始为 {len(todo)} 个新格铺初始买单 "
                    f"(买价高于现价的格会立即按市价成交吃入底仓, 属设计行为, 随后自动挂卖单)...")
        for idx, node in enumerate(todo, 1):
            node.open_as_new()
            if idx % INIT_PROGRESS_STEP == 0:
                logger.info(f"[INIT] 铺单进度 {idx}/{len(todo)} ...")
            time.sleep(PLACE_THROTTLE_SEC)

        # —— 铺单结果汇总: 一张"收据"胜过两百行流水 ——
        ok, err_groups = 0, {}
        for node in todo:
            if node.state == NodeState.ERROR:
                err_groups.setdefault(reject_label(node.last_error), []).append(node.node_id)
            else:
                ok += 1
        report = [f"[INIT] ══ 铺单结果汇总 ══ 成功 {ok}/{len(todo)} 格已挂上盘口"]
        summary_bits = [f"铺单成功 {ok}/{len(todo)} 格"]
        for label, ids in err_groups.items():
            report.append(f"       被拒 {len(ids)} 格 [{label}] {compress_node_ids(ids)} "
                          f"-> {REJECT_ADVICE.get(label, '见日志原始报文')}")
            summary_bits.append(f"{label} {len(ids)} 格({compress_node_ids(ids)})")
        logger.info("\n".join(report))
        self.ledger.system("初始化", "; ".join(summary_bits))

        time.sleep(INIT_SETTLE_WAIT_SEC)  # 给撮合与网络传播极短缓冲
        logger.info("[INIT] 呼叫对账引擎: 核实越价成交的格(买价高于现价者), 并为其自动挂出卖单...")
        self.engine.repair_runtime(self.nodes, skip_recent=False)

    def run_main_loop(self):
        """单线程主循环 (唯一写者): 消费事件 -> 路由到节点 -> 串行推演状态机; 空闲时输出看板。"""
        logger.info(f"[MAIN] 主循环启动: 监听成交/撤销事件, 每 {REPORT_INTERVAL_SEC}s 输出一张 [DASH] 网格看板(兼心跳)")
        while True:
            try:
                event = self.event_queue.get(timeout=1.0)
                self._route(event)
            except queue.Empty:
                pass
            except Exception as e:
                logger.error(f"[MAIN] 事件处理异常: {e}")
                time.sleep(1)

            if time.time() - self._last_dash_ts >= REPORT_INTERVAL_SEC:
                self._last_dash_ts = time.time()
                try:
                    self._report_dashboard()
                except Exception as e:
                    logger.error(f"[DASH] 看板生成异常: {e}")

    def _report_dashboard(self):
        """周期看板: 回答"整体健康吗 / 赚了多少 / 哪里出错要我干嘛 / 最近在干嘛"。主线程执行, 无竞态。"""
        states = Counter(n.state for n in self.nodes.values())
        err_groups = {}
        for n in self.nodes.values():
            if n.state == NodeState.ERROR:
                err_groups.setdefault(reject_label(n.last_error), []).append(n.node_id)
        try:
            price_txt = f"现价 {self.broker.fetch_last_price()}"
        except Exception:
            price_txt = "现价 获取失败"

        st = self.stats
        err_txt = " | ".join(f"[{lab}] {compress_node_ids(ids)}"
                             for lab, ids in err_groups.items()) or "无"
        recent = (f"{st.last_event_ts} {st.last_event}" if st.last_event
                  else "本进程尚无成交, 挂单静候行情触碰格线")
        logger.info(
            f"[DASH] ══ 网格看板 {self.strategy_id} {self.config.symbol} | {price_txt} ══\n"
            f"       格局: 共 {len(self.nodes)} 格 = 等待买入 {states.get(NodeState.WAIT_OPEN, 0)} | "
            f"持仓待卖 {states.get(NodeState.WAIT_CLOSE, 0)} | "
            f"未激活 {states.get(NodeState.INIT, 0)} | 异常挂起 {states.get(NodeState.ERROR, 0)}\n"
            f"       战果(本进程): 套利 {st.cycles_done} 轮 累计毛利 {st.gross_profit:+.4f} USDT | "
            f"买入成交 {st.buy_fills} / 卖出成交 {st.sell_fills}\n"
            f"       维护: 自动补挂 {st.heals} | 拒单 {st.rejects} | 结果未知 {st.unknowns}\n"
            f"       异常格(需人工关注): {err_txt}\n"
            f"       最近动态: {recent}"
        )

    def _route(self, event):
        """按 OID 解析定位目标节点并投喂事件。"""
        parsed = OidCodec.parse(event.client_oid)
        if parsed is None or parsed.strategy_id != self.strategy_id:
            logger.warning(f"[MAIN] 无法路由的 OID(可能来自其他策略或手工单): {event.client_oid}")
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
        logger.info(f"[WATCHDOG] 看门狗启动 | 每 {self.interval}s 比对一次交易所盘口与本地状态 "
                    f"(掉单会自动核实并交回主循环处理)")
        while True:
            time.sleep(self.interval)
            try:
                self.engine.repair_runtime(self.nodes, skip_recent=True)
            except Exception as e:
                logger.error(f"[WATCHDOG] 对账轮次异常: {e}")


def run_single_strategy(config):
    """子进程入口：完全独立的执行环境"""

    # 防极端强杀监控线程 (不侵入 strategy 核心代码)
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

    logger.info(
        f"[BOOT] ══ 策略进程就绪 {config.strategy_id} {config.symbol} ══\n"
        f"       参数: 区间 {config.min_price}~{config.max_price} | 步长 {config.price_ratio}% | 每格 {config.quantity}\n"
        f"       日志导读: 全局状态看 [DASH](每{REPORT_INTERVAL_SEC}s一张, 兼心跳); 赚钱看 [FILL]「卖出成交」; "
        f"异常看 ERROR(均附人话原因与建议)")
    ledger.system("启动", f"进程启动 {config.symbol} | 区间{config.min_price}-{config.max_price} "
                          f"步长{config.price_ratio}% 每格{config.quantity}")

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

    for config in configs:
        p = multiprocessing.Process(target=run_single_strategy, args=(config,))
        p.daemon = True  # 主进程正常结束或报错崩溃时，带走子进程
        p.start()
        processes.append(p)
        logger.info(f"[SYSTEM] 已拉起独立进程 | {config.strategy_id} - {config.symbol} (PID: {p.pid})")

    logger.info("[SYSTEM] 所有币种进程启动完毕，主进程进入守护模式。")

    try:
        for p in processes:
            p.join()
    except (KeyboardInterrupt, SystemExit):
        pass


if __name__ == "__main__":
    main_app()