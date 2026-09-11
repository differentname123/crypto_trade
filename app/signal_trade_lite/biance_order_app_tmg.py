# -*- coding: utf-8 -*-
"""
================================================================================
择时马丁交易引擎 (信号驱动 + 单写者串行状态机 + WAL账本 + 订单全生命周期登记表)
================================================================================
[功能摘要]
  为每个 MartinConfig 拉起独立子进程。空闲态轮询外部择时信号(get_signal_x);
  一旦拿到有效开仓信号, 依据【加仓间距/加仓倍数/最大亏损金额】推算出完整马丁蓝图
  (层数由 max_loss_usdt 唯一决定), 一次性把所有层的限价开仓单铺到盘口; 随后串行轮询维护:
  价格全程静态固化(开仓价/每层止盈价/全局止损价一次算死), 数量动态跟随实际持仓;
  止盈成交 / 止损成交 / 兜底强平 / 入场超时 任一发生 -> 收尾清算 -> 回到空闲态。

[唯一主循环: 5 步单向流水线 (每 2 秒一个 Tick, 绝不东一块西一块打补丁)]
  1. Sense     感知: 现价 + 挂单快照(普通单/算法条件单物理隔离) + 实际持仓(分级缓存)
               任一关键信息缺失 -> 本 Tick 立即安全空转, 绝不带残缺世界观决策。
  2. Reconcile 对账【全流水线唯一允许发起查询的环节】:
               快照喂给 OrderRegistry -> 缺失的存活单点查(保护单优先 + 同级轮转防饥饿)
               -> 新增成交立刻入账 PositionBook -> 本轮有新成交则集中强查一次真实持仓
               -> 外部平仓三重确认(仅做库存校正, 终结归因交回 Evaluate)。
  3. Evaluate  决策: 纯内存判断(总量硬闸/首轮越界/超时/击穿止损/已平净归因),
               只产出 "继续 / 停止加仓 / 进入收尾" 三种周期意图, 绝不发起任何网络 IO。
  4. Align     对齐: 【先出场, 再开仓】。统一夹逼入口 _clamp_exit_qty() 算出安全平仓量,
               产出动作清单(撤单 / 挂限价 / 挂条件单 / 挂市价)。
  5. Execute   执行: 唯一网络写入口。入口带【许可检查】: 收尾态只放行市价收尾单,
               停止加仓态一律丢弃开仓动作; 所有 PlaceResult 走同一套错误映射与退避。

[核心不变量 (代码中反复校验)]
  I1 记账唯一来源: 虚拟持仓 = Σ(本策略 client_oid 的成交增量)。永不用交易所仓位算均价。
  I2 幂等入账     : TrackedOrder.acked_qty 记录"已入账成交量", 增量入账, 重复观测天然 no-op。
  I3 单一出场     : 同一时刻止盈 1 张、止损 1 张; 同角色上一代残留单一律先撤净、终态后才补挂;
                    绝不使用 closePosition。
  I4 总量封顶     : Σ开仓成交 <= 蓝图总量 * 容差, 越界立即停止加仓, 只留止盈止损收尾。
  I5 价格刚性     : 均价/止盈价/止损价只由"实际加仓(OPEN)"决定; TP/SL 部分成交只触发数量对齐。
  I6 外部干预闭环 : 交易所该 positionSide 持仓为 0 时可 100% 断定本策略已无仓 -> 本地归零并终结。
  I7 强平幂等     : 市价强平不可撤销。只有登记表中所有单据都已终态(含上一笔强平单),
                    才允许下发下一笔市价单; 卡死则停机等人工, 严禁盲目重发造成反向开仓。
  I8 平仓量统一夹逼: 一切平仓/强平路径都必须经过 _clamp_exit_qty(), 持仓查不到时
     退化为本地虚拟账本量, 绝不按 0 处理, 也绝不绕过夹逼。
  I9 数量守恒     : 平仓入账超出已知库存(且非碎屑) => 账本自相矛盾, 立即停发单 + 记录证据 + 停机,
     绝不静默截断掩盖。

[并发安全] 全系统只有主线程会修改状态(单一写者)。看板线程只读; 校时并入主循环维护。
[前置条件] 1. 必须为【双向持仓 Hedge Mode】; 2. Hedge 下禁传 reduceOnly;
          3. 每个 strategy_id 全局唯一(它同时是账本名与 OID 命名空间);
          4. 账本与单实例锁固定落在脚本同级 martin_data 绝对目录, 杜绝换目录双开。
================================================================================
"""
import os
import csv
import re
import json
import time
import random
import signal as sysignal
import platform
import logging
import threading
import multiprocessing
from abc import ABC, abstractmethod
from enum import Enum
from decimal import Decimal, ROUND_FLOOR, ROUND_CEILING, ROUND_HALF_UP
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd

from common_utils_lite import setup_logger, get_config

logger = setup_logger(app_name="martin_trader")

from biance_order_lite import (
    safe_init_exchange, execute_order, ExecStatus, fetch_single_order,
)

# ------------------------------------------------------------------------------
# 外部信号源: 名称 -> 函数。子进程按配置里的字符串名解析, 保证跨进程可 pickle。
# 约定: get_signal_x(symbol) -> pd.DataFrame, 永不抛异常, 至少返回空 df。
# ------------------------------------------------------------------------------
try:
    from signal_lib import get_signal_1, get_signal_2, get_signal_3  # noqa
except Exception:  # 缺失时给出安全占位, 保证本文件可独立导入
    def get_signal_1(symbol): return pd.DataFrame()
    def get_signal_2(symbol): return pd.DataFrame()
    def get_signal_3(symbol): return pd.DataFrame()

SIGNAL_REGISTRY = {
    "get_signal_1": get_signal_1,
    "get_signal_2": get_signal_2,
    "get_signal_3": get_signal_3,
}


# ==============================================================================
# 0. 全局可调参数 (集中管理, 消灭魔术数字)
# ==============================================================================
API_THROTTLE_SEC = 0.08          # 相邻两次 API 调用的最小间隔(限流保护)
ORDER_GRACE_SEC = 4.0            # 新单冷静期: 期内不因"盘口查不到"发起点查(容忍撮合与传播延迟)
CANCEL_CONFIRM_SEC = 1.5         # 撤单后多久允许重发撤单(仍在盘口说明撤单请求未生效)
MAX_PROBE_PER_TICK = 4           # 单轮主动点查孤儿单的数量上限(防限频, 同级按最久未查轮转)
PROBE_ALERT_EVERY = 15           # 同一单据连续点查未知多少次告警一次
MAX_PLACE_ATTEMPTS = 5           # 单个槽位的最大挂单尝试次数, 超出则永久放弃 + 告警
RETRY_BACKOFF_SEC = (2, 5, 15, 60, 300)   # 开仓单各次失败后的退避秒数(按尝试次数索引)
EXIT_BACKOFF_SEC = 3.0           # 止盈止损单退避上限: 保护单命悬一线, 绝不允许长退避
FORCE_BACKOFF_SEC = 3.0          # 市价强平单被明确拒单后的强制冷却(防瞬秒耗尽次数直接停机)
QTY_EPS_RATIO = 1e-9             # 浮点比较用的极小量
OVERFILL_TOLERANCE = 1.02        # I4: 累计开仓成交 / 蓝图总量 的容忍上限
SL_BREACH_CONFIRM_SEC = 5.0      # 现价击穿止损价后, 等条件单自己触发的宽限时间, 超时则主动强平
POSITION_CACHE_SEC = 5.0         # 实际持仓轻量缓存(常规对齐用); 高危路径与新成交后强制击穿
MAX_CONSECUTIVE_ERRORS = 20      # 主循环连续异常次数上限
HARD_MAX_LAYERS = 50             # 【物理硬顶】纯防死循环底线; 真实层数由 max_loss_usdt 决定
FORCE_CLOSE_MAX_ATTEMPTS = 3     # 市价强平最大尝试次数, 超出转 STOPPED 等人工介入
SL_IMM_TRIG_MAX_DEV_PCT = 50.0   # "会立即触发"回执的本地核验: 止损价与现价偏离超此阈值判定为算错
IDLE_ERROR_SLEEP_SEC = (15.0, 30.0)  # IDLE 态连续异常的长休眠退避区间(绝不停机)
IDLE_NO_PRICE_SLEEP_SEC = 5.0    # IDLE 态拉不到世界快照时的额外退避(防断网高频空转刷屏)
RECOVER_RETRY_SEC = 10.0         # 断点续传接管失败后的重试间隔
RECOVER_MAX_ATTEMPTS = 30        # 接管重试上限, 超出转 STOPPED(但绝不清场)
EXTERNAL_FLAT_CONFIRM_SEC = 6.0  # 交易所持仓归零后的二次确认宽限(防接口滞后误判外部平仓)
CLOSING_STUCK_SEC = 120.0        # 收尾阶段单据持续卡在非终态的容忍时长, 超出停机等人工
POS_PROBE_MAX_UNKNOWN = 20       # 收尾阶段实际持仓连续查询失败的轮数上限, 超出转 STOPPED
TIME_SYNC_SEC = 3600.0           # 主循环定时维护: 与交易所重新校时的间隔(对抗本地时钟漂移)

# 账本与单实例锁的【固定绝对目录】: 无论从哪个工作目录启动, 同一 strategy_id 只可能有一个实例
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "martin_data")


def data_path(name: str) -> str:
    os.makedirs(DATA_DIR, exist_ok=True)
    return os.path.join(DATA_DIR, name)


# ==============================================================================
# 1. 枚举与值对象
# ==============================================================================
class LedgerError(Exception):
    """账本(WAL)写盘失败。这是系统的生命线, 一旦失败必须硬停机, 绝不带病继续交易。"""


class _OrderNotFound:
    """点查语义哨兵: 交易所【明确回执】订单不存在(可安全换号重挂)。"""
    __slots__ = ()

    def __repr__(self):
        return "ORDER_NOT_FOUND"

    def __bool__(self):
        return False


ORDER_NOT_FOUND = _OrderNotFound()


class Direction(Enum):
    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def sign(self) -> int:
        """做多 +1 / 做空 -1。用于把多空公式统一成一条式子, 消灭 if-else 分叉。"""
        return 1 if self is Direction.LONG else -1

    @property
    def open_side(self) -> str:
        return "buy" if self is Direction.LONG else "sell"

    @property
    def close_side(self) -> str:
        return "sell" if self is Direction.LONG else "buy"

    @property
    def position_side(self) -> str:
        return self.value


class OrderRole(Enum):
    OPEN = "O"   # 开仓 / 加仓
    TP = "T"     # 止盈
    SL = "S"     # 止损(含兜底市价强平, 层号 99)


class OrderState(Enum):
    """
    订单在【登记表】中的生命周期。刻意保留三个不同的终态, 因为它们携带完全不同的决策信息:
      FILLED     全部成交
      DEAD       残缺/不可重发(如开仓单部分成交后被撤、参数非法被永久拒)
      NOT_PLACED 该 OID 确定从未形成订单, 或已撤且零成交 -> 槽位允许换号重发
    非终态只有三种; 其中 PENDING / CANCEL_PENDING 真相未明, 一律原地锁定, 绝不换号重发。
    """
    PENDING = "PENDING"                # 请求已发但结果未确定(涵盖旧版 INTENT / UNKNOWN)
    LIVE = "LIVE"                      # 已确认在盘口
    CANCEL_PENDING = "CANCEL_PENDING"  # 撤单已受理但终态未确认(可能其实成交了)
    FILLED = "FILLED"
    DEAD = "DEAD"
    NOT_PLACED = "NOT_PLACED"

    @property
    def alive(self) -> bool:
        return self in (OrderState.PENDING, OrderState.LIVE, OrderState.CANCEL_PENDING)


class EngineState(Enum):
    IDLE = "IDLE"        # 空闲监听信号
    RECOVER = "RECOVER"  # 断点续传接管中(有活仓, 绝不清场, 绝不开新仓)
    ACTIVE = "ACTIVE"    # 周期维护中(收尾也在此态内, 由 cycle.end_reason 驱动)
    STOPPED = "STOPPED"  # 终止: 不再接新信号, 需人工介入


class TickResult(Enum):
    CONTINUE = "CONTINUE"   # 周期继续
    DONE = "DONE"           # 周期已彻底收尾, 可写 CYCLE_END
    HALT = "HALT"           # 出现无法自动裁决的危险局面, 引擎必须停机等人工


class EndReason(Enum):
    TP = "END_TP"                     # 止盈成交
    SL = "END_SL"                     # 止损条件单成交
    SL_FORCED = "END_SL_FORCED"       # 兜底强平(条件单未触发)
    NO_FILL = "END_NO_FILL"           # 入场超时/首轮越界, 一手未成
    TIMEOUT = "END_TIMEOUT"           # 周期超时强平
    MANUAL_FLAT = "END_MANUAL_FLAT"   # 仓位被外部平掉, 周期被动结束


class ErrKind(Enum):
    NONE = "NONE"
    UNKNOWN_RESULT = "UNKNOWN_RESULT"  # 结果未知(网络中断/超时/回执缺失) -> 保留原OID点查, 绝不换号
    TRANSIENT = "TRANSIENT"          # 明确拒单(限频/时间戳) -> 退避重试
    PRICE_BAND = "PRICE_BAND"        # 价格离盘口太远 -> 退避后重挂
    IMMEDIATE_TRIGGER = "IMM_TRIG"   # 条件单会立即触发 -> 需本地现价双重核验
    INSUFFICIENT = "INSUFFICIENT"    # 保证金/余额不足 -> 退避 + 告警
    REDUCE_REJECT = "REDUCE_REJECT"  # 平仓数量超过持仓 -> 仓位被外部动过
    DUPLICATE = "DUPLICATE"          # OID 重复 -> 单子已存在, 保持 PENDING 点查
    INVALID = "INVALID"              # 精度/最小量/单号格式等参数非法 -> 不可重试
    FATAL = "FATAL"                  # 明确拒单但未归类 -> 保守停挂


class UniOrder:
    """交易所订单的统一视图。上层只认它, 换交易所只需改 Gateway 的转换函数。"""
    __slots__ = ("coid", "ex_id", "status", "price", "stop_price", "amount",
                 "filled", "avg_price", "side", "ts", "raw")

    def __init__(self, coid="", ex_id="", status="UNKNOWN", price=0.0, stop_price=0.0,
                 amount=0.0, filled=0.0, avg_price=0.0, side="", ts=0, raw=None):
        self.coid = coid
        self.ex_id = ex_id
        self.status = status          # OPEN / FILLED / CANCELED / REJECTED / UNKNOWN
        self.price = price
        self.stop_price = stop_price
        self.amount = amount
        self.filled = filled
        self.avg_price = avg_price
        self.side = side
        self.ts = ts
        self.raw = raw or {}

    @property
    def remaining(self) -> float:
        return max(0.0, self.amount - self.filled)

    @property
    def is_terminal(self) -> bool:
        return self.status in ("FILLED", "CANCELED", "REJECTED")


class PlaceResult:
    """挂单结果三态: OK(已受理) / UNKNOWN(结果未知, 必须点查) / 拒单(带错误分类)。"""
    __slots__ = ("ok", "unknown", "ex_id", "err", "kind")

    def __init__(self, ok=False, unknown=False, ex_id="", err="", kind=ErrKind.NONE):
        self.ok = ok
        self.unknown = unknown
        self.ex_id = ex_id
        self.err = err
        self.kind = kind


class Signal:
    """净化后的开仓信号。"""
    __slots__ = ("direction", "limit_price", "signal_ts", "source")

    def __init__(self, direction: Direction, limit_price: float, signal_ts: int, source: str):
        self.direction = direction
        self.limit_price = limit_price
        self.signal_ts = int(signal_ts)
        self.source = source

    def __repr__(self):
        return (f"Signal({self.source} {self.direction.value} @{self.limit_price} "
                f"ts={self.signal_ts})")


class World:
    """
    一个 Tick 的【世界快照】。由 Sense 步骤一次性产出;
    其中 pos_qty 允许在 Reconcile 阶段被"强制刷新"覆盖一次(本轮有新成交时), 之后只读。
      algo_ok=False 表示算法条件单接口本轮降级(普通单正常), 此时:
        1. 不采信"条件单不存在"的回执; 2. 绝不宣告盘口已清场、绝不开启新周期。
    """
    __slots__ = ("ts", "price", "orders", "algo_ok", "pos_qty")

    def __init__(self, ts, price, orders, algo_ok, pos_qty):
        self.ts = ts
        self.price = price
        self.orders: Dict[str, UniOrder] = orders
        self.algo_ok = algo_ok
        self.pos_qty = pos_qty


class Action:
    """
    Align 步骤产出的动作意图, 由 Execute 步骤集中下发。
    kind: CANCEL / LIMIT / STOP / MARKET
    slot: 该动作归属的槽位(LayerPlan 或 ExitOrder), 市价强平无槽位。
    """
    __slots__ = ("kind", "role", "layer", "price", "qty", "coid", "slot", "why")

    def __init__(self, kind, role=None, layer=0, price=0.0, qty=0.0, coid="", slot=None, why=""):
        self.kind = kind
        self.role = role
        self.layer = layer
        self.price = float(price)
        self.qty = float(qty)
        self.coid = coid
        self.slot = slot
        self.why = why

    @staticmethod
    def cancel(coid, why):
        return Action("CANCEL", coid=coid, why=why)

    @staticmethod
    def place(kind, role, layer, price, qty, slot, why):
        return Action(kind, role=role, layer=layer, price=price, qty=qty, slot=slot, why=why)


# ==============================================================================
# 2. OID 编解码 (多策略隔离与状态机路由的唯一凭证)
# ==============================================================================
_B36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _to_b36(n: int) -> str:
    n = int(n)
    if n == 0:
        return "0"
    out = []
    while n > 0:
        n, r = divmod(n, 36)
        out.append(_B36[r])
    return "".join(reversed(out))


class ParsedOid:
    __slots__ = ("strategy_id", "cycle_id", "role", "layer")

    def __init__(self, strategy_id, cycle_id, role, layer):
        self.strategy_id = strategy_id
        self.cycle_id = cycle_id
        self.role = role
        self.layer = layer


class OidCodec:
    """
    格式: M_{S_ID}_{C_ID}_{ROLE}{L_ID}_{TS}
      M     系统前缀(马丁)
      S_ID  策略短标识, <=8 位纯字母数字(启动时强校验)
      C_ID  周期流水号 = base36(信号毫秒时间戳) 【完整不截断, 杜绝命名空间循环冲突】
      ROLE  O=开仓/加仓  T=止盈  S=止损
      L_ID  层级 00~99 (T/S 用 00; 兜底市价强平用 99)
      TS    毫秒后4位 + 2位随机, 防同层补挂撞号
    示例: M_B1_1PXQ8K3F_O03_4821XK  (总长 <=30 字符, 低于 Binance 36 上限)
    """
    PREFIX = "M"

    @staticmethod
    def cycle_id_of(signal_ts: int) -> str:
        # 完整 base36 时间戳, 绝不截断(截断会导致约 25 天一循环的 OID 命名空间冲突)
        return _to_b36(int(signal_ts))

    @classmethod
    def build(cls, strategy_id: str, cycle_id: str, role: OrderRole, layer: int) -> str:
        suffix = f"{int(time.time() * 1000) % 10000:04d}{random.choice(_B36)}{random.choice(_B36)}"
        return f"{cls.PREFIX}_{strategy_id}_{cycle_id}_{role.value}{layer:02d}_{suffix}"

    @classmethod
    def parse(cls, oid: str) -> Optional[ParsedOid]:
        if not oid:
            return None
        parts = oid.split("_")
        # 至少: M, S_ID, C_ID, ROLE+L_ID, TS
        if len(parts) < 5 or parts[0] != cls.PREFIX:
            return None
        try:
            rl = parts[-2]
            role = OrderRole(rl[0])
            layer = int(rl[1:])
            cycle_id = parts[-3]
            strategy_id = "_".join(parts[1:-3])  # 倒序切片, 容忍 S_ID 内含下划线
            return ParsedOid(strategy_id, cycle_id, role, layer)
        except Exception:
            return None

    @classmethod
    def strategy_prefix(cls, strategy_id: str) -> str:
        return f"{cls.PREFIX}_{strategy_id}_"


# ==============================================================================
# 3. 交易规格与精度 (最小下单量 / 最小名义价值 / 价格刻度)
# ==============================================================================
def _dec(x) -> Decimal:
    return Decimal(str(x))


def quantize(value: float, step: float, mode: str = "down") -> float:
    """
    按 step 对 value 做定向修约。用 Decimal(str()) 规避二进制浮点误差。
    【浮点塌陷修复】向下/向上修约前, 若值与最近的 step 整数倍差距极小(<1e-8 个 step),
    先吸附到该整数倍, 避免 0.3-0.2=0.0999.. 被 FLOOR 截断成 0.0 而导致加仓量逐层偏小。
    """
    if step is None or step <= 0:
        return float(value)
    v, s = _dec(value), _dec(step)
    n = v / s
    nearest = n.to_integral_value(rounding=ROUND_HALF_UP)
    if abs(n - nearest) <= Decimal("1e-8"):
        n = nearest
    elif mode == "down":
        n = n.to_integral_value(rounding=ROUND_FLOOR)
    elif mode == "up":
        n = n.to_integral_value(rounding=ROUND_CEILING)
    else:
        n = nearest
    return float(n * s)


class InstrumentSpec:
    """
    单个交易对的下单规格。由 Gateway 从交易所原始 filters 解析, 上层只用这里的能力,
    从而做到"换交易所只改 Gateway"。
    """

    def __init__(self, symbol, tick_size, step_size, min_qty, max_qty, min_notional,
                 contract_size=1.0):
        self.symbol = symbol
        self.tick_size = float(tick_size or 0.0)
        self.step_size = float(step_size or 0.0)
        self.min_qty = float(min_qty or 0.0)
        self.max_qty = float(max_qty or 0.0) or float("inf")
        self.min_notional = float(min_notional or 0.0)
        self.contract_size = float(contract_size or 1.0)

    # ---------- 价格 ----------
    def round_price(self, price: float, mode: str = "half") -> float:
        return quantize(price, self.tick_size, mode)

    # ---------- 数量 ----------
    def round_qty(self, qty: float, mode: str = "down") -> float:
        return quantize(qty, self.step_size, mode)

    def normalize_open_qty(self, qty: float, price: float) -> float:
        """
        开仓量修约: 先向下截断(保守), 再兜底抬到 minQty 与 minNotional 之上。
        返回 0 表示无法构造合法数量(调用方应丢弃该层/该信号)。
        """
        if price <= 0:
            return 0.0
        q = self.round_qty(qty, "down")
        need_by_min_qty = self.round_qty(self.min_qty, "up") if self.min_qty > 0 else 0.0
        need_by_notional = 0.0
        if self.min_notional > 0:
            need_by_notional = self.round_qty(
                self.min_notional / (price * self.contract_size), "up")
            # 向上修约后仍可能因浮点差一点点, 再补一个 step
            while need_by_notional * price * self.contract_size < self.min_notional:
                need_by_notional = round(need_by_notional + self.step_size, 12)
        q = max(q, need_by_min_qty, need_by_notional)
        if q > self.max_qty:
            return 0.0
        return q

    def notional(self, price: float, qty: float) -> float:
        return price * qty * self.contract_size

    def qty_is_dust(self, qty: float) -> bool:
        """低于最小交易单位的碎屑: 无法下单, 只能账面归零。"""
        if qty is None:
            return False
        floor_ = max(self.min_qty, self.step_size)
        return qty < floor_ * (1 - 1e-9)

    def __repr__(self):
        return (f"Spec({self.symbol} tick={self.tick_size} step={self.step_size} "
                f"minQty={self.min_qty} minNotional={self.min_notional})")


# ==============================================================================
# 4. 交易所网关 (抽象 + Binance 实现)  —— 唯一与平台耦合的一层
# ==============================================================================
_ERR_CODE_RE = re.compile(r'["\']code["\']\s*:\s*(-\d+)')

# 【明确拒单】拿到交易所确定错误码才可判定"单子没进去", 才允许退避重试
_CODE_KIND = {
    -1000: ErrKind.UNKNOWN_RESULT,  # 未知内部错误, 请求可能已执行
    -1001: ErrKind.UNKNOWN_RESULT,  # 内部断连, 结果未知
    -1006: ErrKind.UNKNOWN_RESULT,  # 收到非预期响应, 结果未知
    -1007: ErrKind.UNKNOWN_RESULT,  # 等待响应超时, 结果未知
    -1003: ErrKind.TRANSIENT,       # 请求过频, 明确被拒
    -1008: ErrKind.TRANSIENT,       # 服务器繁忙, 明确被拒
    -1015: ErrKind.TRANSIENT,       # 下单过频, 明确被拒
    -1021: ErrKind.TRANSIENT,       # 时间戳偏差, 明确被拒
    -1013: ErrKind.INVALID,
    -1102: ErrKind.INVALID,
    -1104: ErrKind.INVALID,
    -1111: ErrKind.INVALID,
    -1116: ErrKind.INVALID,
    -1117: ErrKind.INVALID,
    -1121: ErrKind.INVALID,
    -2010: ErrKind.INVALID,
    -2011: ErrKind.INVALID,
    -2013: ErrKind.INVALID,
    -2018: ErrKind.INSUFFICIENT,
    -2019: ErrKind.INSUFFICIENT,
    -2021: ErrKind.IMMEDIATE_TRIGGER,
    -2022: ErrKind.REDUCE_REJECT,
    -2027: ErrKind.INVALID,
    -4003: ErrKind.INVALID,
    -4005: ErrKind.INVALID,
    -4013: ErrKind.INVALID,
    -4014: ErrKind.INVALID,
    -4015: ErrKind.INVALID,         # 客户端单号格式/长度非法: 参数错误, 绝不是"重复单号"
    -4016: ErrKind.INVALID,
    -4131: ErrKind.PRICE_BAND,
    -4164: ErrKind.INVALID,
    -4165: ErrKind.INVALID,
}

_UNKNOWN_TEXT = ("timeout", "timed out", "read timed out", "connection", "reset by peer",
                 "network", "temporarily", "service unavailable", "bad gateway",
                 "gateway timeout", "502", "503", "504", "520", "521", "ssl", "eof",
                 "no response", "unknown result", "结果未知")


def err_code_of(msg) -> Optional[int]:
    """从报错文本中【正则精确提取】交易所 JSON code 字段, 绝不做子串误匹配。"""
    m = _ERR_CODE_RE.search(str(msg or ""))
    return int(m.group(1)) if m else None


def is_order_not_found(msg) -> bool:
    """是否为交易所明确回执的"订单不存在"。"""
    code = err_code_of(msg)
    if code in (-2013,):
        return True
    low = str(msg or "").lower()
    return ("order does not exist" in low or "order not found" in low)


def classify_error(msg) -> ErrKind:
    """
    错误分类(决定后续行为, 极其关键)。铁律:
      * 只有拿到交易所【明确错误码/明确语义】才判定为"拒单", 才允许换号/退避重发;
      * 网络中断、超时、回执缺失一律 UNKNOWN_RESULT, 保留原 OID 点查, 绝不换号重发。
    """
    raw = str(msg or "")
    if not raw.strip():
        return ErrKind.UNKNOWN_RESULT
    low = raw.lower()
    if any(k in low for k in ("duplicate", "already exist")):
        return ErrKind.DUPLICATE
    code = err_code_of(raw)
    if code is not None:
        kind = _CODE_KIND.get(code)
        if kind is not None:
            return kind
        return ErrKind.FATAL          # 有明确错误码但未归类: 保守停挂, 不重发
    # 无明确错误码 -> 只可能是传输层问题
    if any(k in low for k in _UNKNOWN_TEXT):
        return ErrKind.UNKNOWN_RESULT
    if any(k in low for k in ("too many", "throttl", "429")):
        return ErrKind.TRANSIENT
    if "immediately trigger" in low:
        return ErrKind.IMMEDIATE_TRIGGER
    if any(k in low for k in ("reduceonly", "reduce only")):
        return ErrKind.REDUCE_REJECT
    if any(k in low for k in ("insufficient", "margin is insufficient")):
        return ErrKind.INSUFFICIENT
    if any(k in low for k in ("percent_price", "price_filter", "would immediately match")):
        return ErrKind.PRICE_BAND
    if any(k in low for k in ("min_notional", "notional", "lot_size", "precision")):
        return ErrKind.INVALID
    return ErrKind.UNKNOWN_RESULT     # 兜底: 宁可点查, 绝不盲目重发


def make_fail_result(err) -> PlaceResult:
    kind = classify_error(err)
    if kind is ErrKind.UNKNOWN_RESULT:
        return PlaceResult(unknown=True, err=str(err), kind=kind)
    return PlaceResult(err=str(err), kind=kind)


class ExchangeGateway(ABC):
    """
    交易所网关抽象: 所有网络与平台差异全部收拢在这里。
    上层(状态机/引擎)只依赖本接口, 接入 OKX / 模拟盘 / 回测只需实现一个子类。
    约定: 本层永不抛异常给上层 —— 查询失败返回 None, 挂单失败返回带分类的 PlaceResult。
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._last_call_ts = 0.0

    def _throttle(self):
        gap = time.time() - self._last_call_ts
        if gap < API_THROTTLE_SEC:
            time.sleep(API_THROTTLE_SEC - gap)
        self._last_call_ts = time.time()

    def sync_time(self):
        """定时校时(由主循环维护调用, 默认空实现)。"""
        return

    @abstractmethod
    def load_instrument(self) -> Optional[InstrumentSpec]: ...
    @abstractmethod
    def fetch_last_price(self) -> Optional[float]: ...

    @abstractmethod
    def fetch_open_orders(self, coid_prefix: str) -> Tuple[Optional[Dict[str, UniOrder]], bool]:
        """返回 (挂单快照, 算法条件单接口是否成功)。快照为 None 表示核心信息缺失。"""

    @abstractmethod
    def fetch_order(self, coid: str): ...
    @abstractmethod
    def fetch_position_qty(self, position_side: str) -> Optional[float]: ...
    @abstractmethod
    def place_limit(self, side, qty, price, coid, position_side) -> PlaceResult: ...
    @abstractmethod
    def place_market(self, side, qty, coid, position_side) -> PlaceResult: ...
    @abstractmethod
    def place_stop_market(self, side, qty, stop_price, coid, position_side,
                          working_type) -> PlaceResult: ...
    @abstractmethod
    def cancel(self, coid: str) -> bool: ...
    @abstractmethod
    def is_hedge_mode(self) -> Optional[bool]: ...


class BinanceGateway(ExchangeGateway):
    """Binance U 本位合约实现。复用项目内已验证的 execute_order / fetch_single_order。"""

    _STATUS_MAP = {
        "NEW": "OPEN", "PARTIALLY_FILLED": "OPEN", "PENDING_CANCEL": "OPEN",
        "FILLED": "FILLED", "CANCELED": "CANCELED", "CANCELLED": "CANCELED",
        "EXPIRED": "CANCELED", "EXPIRED_IN_MATCH": "CANCELED", "REJECTED": "REJECTED",
        "OPEN": "OPEN", "CLOSED": "FILLED",
    }

    def __init__(self, exchange, symbol):
        super().__init__(symbol)
        self.ex = exchange
        # 【禁用 ccxt 内置重试】所有重试必须归状态机统一管理, 杜绝"以为发一次实际发两次"
        try:
            self.ex.options["maxRetriesOnFailure"] = 0
            self.ex.options["maxRetriesOnFailureDelay"] = 0
        except Exception as e:
            logger.info(f"[网关] 关闭 ccxt 内置重试失败(继续启动) | 错误:[{e}]")

    # ---------- 转换 ----------
    def _to_uni(self, o: dict) -> UniOrder:
        info = o.get("info") or {}
        raw_status = str(info.get("status") or o.get("status") or "").upper()
        status = self._STATUS_MAP.get(raw_status, "UNKNOWN")
        filled = float(o.get("filled") or info.get("executedQty") or 0.0)
        amount = float(o.get("amount") or info.get("origQty") or 0.0)
        # closed 但未全成 => 实为撤单残留, 按撤单处理, 避免误判"完全成交"
        if status == "FILLED" and amount > 0 and filled < amount * (1 - 1e-9):
            status = "CANCELED"
        return UniOrder(
            coid=o.get("clientOrderId") or info.get("clientOrderId") or "",
            ex_id=str(o.get("id") or info.get("orderId") or ""),
            status=status,
            price=float(o.get("price") or info.get("price") or 0.0),
            stop_price=float(o.get("stopPrice") or info.get("stopPrice") or 0.0),
            amount=amount,
            filled=filled,
            avg_price=float(o.get("average") or info.get("avgPrice") or 0.0),
            side=str(o.get("side") or info.get("side") or "").lower(),
            ts=int(o.get("lastTradeTimestamp") or o.get("lastUpdateTimestamp")
                   or o.get("timestamp") or 0),
            raw=o,
        )

    # ---------- 查询 ----------
    def load_instrument(self) -> Optional[InstrumentSpec]:
        try:
            self._throttle()
            try:
                self.ex.load_markets()
            except Exception:
                pass
            m = self.ex.market(self.symbol)
            tick = step = min_qty = max_qty = min_notional = 0.0
            for f in (m.get("info", {}) or {}).get("filters", []) or []:
                ft = f.get("filterType")
                if ft == "PRICE_FILTER":
                    tick = float(f.get("tickSize") or 0)
                elif ft == "LOT_SIZE":
                    step = float(f.get("stepSize") or 0)
                    min_qty = float(f.get("minQty") or 0)
                    max_qty = float(f.get("maxQty") or 0)
                elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                    min_notional = float(f.get("notional") or f.get("minNotional") or 0)
            # 兜底: 用 ccxt 统一字段补齐
            prec = m.get("precision") or {}
            limits = m.get("limits") or {}
            if tick <= 0:
                tick = float(prec.get("price") or 0) or 0.0
            if step <= 0:
                step = float(prec.get("amount") or 0) or 0.0
            if min_qty <= 0:
                min_qty = float(((limits.get("amount") or {}).get("min")) or 0)
            if min_notional <= 0:
                min_notional = float(((limits.get("cost") or {}).get("min")) or 0) or 5.0
            spec = InstrumentSpec(self.symbol, tick, step, min_qty, max_qty, min_notional,
                                  float(m.get("contractSize") or 1.0))
            if spec.tick_size <= 0 or spec.step_size <= 0:
                logger.critical(f"[网关] 交易规格解析异常, 缺少 tickSize/stepSize, 拒绝启动 | {spec}")
                return None
            return spec
        except Exception as e:
            logger.error(f"[网关] 拉取交易规格失败 | 交易对:[{self.symbol}] 错误:[{e}]")
            return None

    def fetch_last_price(self) -> Optional[float]:
        try:
            self._throttle()
            p = float(self.ex.fetch_ticker(self.symbol).get("last") or 0)
            return p if p > 0 else None
        except Exception as e:
            logger.error(f"[网关] 拉取最新价失败(本轮跳过决策) | 错误:[{e}]")
            return None

    # ---------- 挂单快照: 普通单与算法条件单【物理隔离】, 对外合并成一张干净世界快照 ----------
    def _fetch_normal_orders(self, coid_prefix: str) -> Optional[Dict[str, UniOrder]]:
        """普通挂单(限价开仓/限价止盈/STOP_MARKET)。绝对生命线: 失败返回 None。"""
        try:
            self._throttle()
            out = {}
            for o in self.ex.fetch_open_orders(self.symbol) or []:
                u = self._to_uni(o)
                if u.coid and u.coid.startswith(coid_prefix):
                    out[u.coid] = u
            return out
        except Exception as e:
            logger.error(f"[网关] 拉取普通在线挂单失败, 本轮跳过决策 | 错误:[{e}]")
            return None

    def _fetch_algo_orders(self, coid_prefix: str) -> Optional[Dict[str, UniOrder]]:
        """
        算法条件单(未触发的条件单无法用普通挂单接口查到时, 必须单独走 openAlgoOrders)。
        弹性增强: 失败只返回 None 表示"本轮降级", 不阻断主循环。
        """
        try:
            self._throttle()
            market_id = self.symbol.replace("/", "").split(":")[0]  # "BTC/USDT:USDT" -> "BTCUSDT"
            res = self.ex.fapiPrivateGetOpenAlgoOrders({"symbol": market_id})
            out = {}
            for a in res or []:
                coid = a.get("clientAlgoId") or a.get("clientOrderId") or ""
                if not coid or not coid.startswith(coid_prefix):
                    continue
                out[coid] = UniOrder(
                    coid=coid,
                    ex_id=str(a.get("algoId") or a.get("orderId") or ""),
                    status="OPEN",
                    price=0.0,
                    stop_price=float(a.get("triggerPrice") or a.get("stopPrice") or 0.0),
                    amount=float(a.get("quantity") or a.get("origQty") or 0.0),
                    filled=float(a.get("executedQty") or 0.0),
                    avg_price=0.0,
                    side=str(a.get("side") or "").lower(),
                    ts=int(a.get("bookTime") or a.get("time") or 0),
                    raw=a,
                )
            return out
        except Exception as e:
            logger.info(f"[网关] 算法条件单接口异常(本轮降级, 不采信'条件单不存在'回执): {e}")
            return None

    def fetch_open_orders(self, coid_prefix: str) -> Tuple[Optional[Dict[str, UniOrder]], bool]:
        normal = self._fetch_normal_orders(coid_prefix)
        if normal is None:
            return None, False
        algo = self._fetch_algo_orders(coid_prefix)
        if algo is None:
            return normal, False
        normal.update(algo)
        return normal, True

    def fetch_order(self, coid: str):
        """
        点查订单。返回值语义(极其重要, 上层据此决定是否允许换号重挂):
          UniOrder        -> 拿到确定的订单快照
          ORDER_NOT_FOUND -> 交易所【明确回执】订单不存在, 上层可安全换新号重挂
          None            -> 结果未知(超时/网络/5xx), 上层必须保留原 OID, 绝不换号重发
        """
        try:
            self._throttle()
            o = fetch_single_order(self.ex, self.symbol, coid)
            if o:
                return self._to_uni(o)
        except Exception as e:
            if is_order_not_found(e):
                return ORDER_NOT_FOUND
            logger.info(f"[网关] 点查订单结果未知(保留原OID, 下轮继续点查) | CID:[{coid}] 错误:[{e}]")
            return None
        # 上游返回空: 再用原生接口确认一次, 严格区分"确实不存在"与"查询失败"
        try:
            self._throttle()
            o2 = self.ex.fetch_order(coid, self.symbol, {"origClientOrderId": coid})
            if o2:
                return self._to_uni(o2)
            return ORDER_NOT_FOUND
        except Exception as e:
            if is_order_not_found(e):
                return ORDER_NOT_FOUND
            logger.info(f"[网关] 点查订单结果未知(保留原OID, 下轮继续点查) | CID:[{coid}] 错误:[{e}]")
            return None

    def fetch_position_qty(self, position_side: str) -> Optional[float]:
        """用于夹逼平仓数量与外部干预识别, 绝不参与均价计算(双向持仓下该数字为全账户共享)。"""
        try:
            self._throttle()
            for p in self.ex.fetch_positions([self.symbol]) or []:
                info = p.get("info") or {}
                ps = str(info.get("positionSide") or p.get("side") or "").upper()
                if ps == position_side.upper():
                    return abs(float(p.get("contracts") or info.get("positionAmt") or 0))
            return 0.0
        except Exception as e:
            logger.info(f"[网关] 拉取真实持仓失败(结果未知) | 错误:[{e}]")
            return None

    def is_hedge_mode(self) -> Optional[bool]:
        try:
            self._throttle()
            r = self.ex.fapiPrivateGetPositionSideDual()
            return bool(str(r.get("dualSidePosition")).lower() == "true")
        except Exception as e:
            logger.info(f"[网关] 无法确认持仓模式(跳过校验) | 错误:[{e}]")
            return None

    def sync_time(self):
        try:
            self._throttle()
            self.ex.load_time_difference()
            logger.info(f"[校时] 已重新校准 | 偏差:[{self.ex.options.get('timeDifference', 0)}ms]")
        except Exception as e:
            logger.info(f"[校时] 本次校时失败, 保持旧偏差 | 错误:[{e}]")

    # ---------- 下单 ----------
    def _wrap_exec(self, res) -> PlaceResult:
        st = getattr(res, "status", None)
        if st == ExecStatus.OK:
            return PlaceResult(ok=True, ex_id=getattr(res, "exchange_oid", "") or "")
        if st == ExecStatus.UNKNOWN:
            return PlaceResult(unknown=True, err="结果未知(网络中断)",
                               kind=ErrKind.UNKNOWN_RESULT)
        err = str(getattr(res, "error_msg", "") or "")
        return make_fail_result(err)

    def place_limit(self, side, qty, price, coid, position_side) -> PlaceResult:
        try:
            res = execute_order(
                exchange=self.ex, symbol=self.symbol, side=side, amount=qty,
                client_oid=coid, order_type="limit", price=price,
                reduce_only=False,               # Hedge Mode 严禁传 True, 否则拒单
                position_side=position_side,
            )
            return self._wrap_exec(res)
        except Exception as e:
            return make_fail_result(e)

    def place_market(self, side, qty, coid, position_side) -> PlaceResult:
        try:
            res = execute_order(
                exchange=self.ex, symbol=self.symbol, side=side, amount=qty,
                client_oid=coid, order_type="market", price=None,
                reduce_only=False, position_side=position_side,
            )
            return self._wrap_exec(res)
        except Exception as e:
            return make_fail_result(e)

    def place_stop_market(self, side, qty, stop_price, coid, position_side,
                          working_type="MARK_PRICE") -> PlaceResult:
        """
        条件止损单(STOP_MARKET)。
        【要点】1. 订单类型必须为 "STOP_MARKET"; 2. 严格按精度格式化 stopPrice 与 amount。
        """
        try:
            self._throttle()
            formatted_stop_price = self.ex.price_to_precision(self.symbol, stop_price)
            formatted_qty = float(self.ex.amount_to_precision(self.symbol, qty))

            params = {
                "stopPrice": formatted_stop_price,
                "workingType": working_type,
                "positionSide": position_side,
                "newClientOrderId": coid,
                "priceProtect": "FALSE",  # 必须大写
            }
            o = self.ex.create_order(
                symbol=self.symbol,
                type="STOP_MARKET",
                side=side,
                amount=formatted_qty,
                price=None,
                params=params,
            )
            return PlaceResult(ok=True, ex_id=str((o or {}).get("id") or ""))
        except Exception as e:
            return make_fail_result(e)

    def cancel(self, coid: str) -> bool:
        """
        双轨自适应撤单。先尝试标准撤单; 若提示查无此单(-2011/unknown order), 自动尝试算法单撤销。
        注意: 返回 True 仅代表"撤单请求已被受理", 不代表订单终态(可能刚好成交),
              终态必须由上层点查确认(CANCEL_PENDING -> 点查裁决)。
        """
        try:
            self._throttle()
            self.ex.cancel_order(coid, self.symbol, {"origClientOrderId": coid})
            return True
        except Exception as e:
            msg = str(e).lower()
            # 单子本来就不存在/已终结, 直接视为撤单请求达成(幂等), 终态仍由点查裁决
            if any(k in msg for k in ("-2013", "order not found", "does not exist")):
                return True
            # -2011 (Unknown order): 很可能是未触发的算法条件单, 走算法撤单通道
            if "-2011" in msg or "unknown order" in msg:
                try:
                    self._throttle()
                    market_id = self.symbol.replace("/", "").split(":")[0]
                    self.ex.fapiPrivateDeleteAlgoOrder({
                        "symbol": market_id,
                        "clientAlgoId": coid
                    })
                    logger.info(f"[网关] 算法条件单成功撤销 | CID:[{coid}]")
                    return True
                except Exception as algo_err:
                    a_msg = str(algo_err).lower()
                    if any(k in a_msg for k in ("-2011", "unknown", "not exist", "does not exist")):
                        return True  # 确实已经没有了, 目标达成
                    logger.info(f"[网关] 算法条件单撤销亦失败(留待下一轮对账) | CID:[{coid}] "
                                f"错误:[{algo_err}]")
                    return False
            logger.info(f"[网关] 撤单失败(下一轮自动复查) | CID:[{coid}] 错误:[{e}]")
            return False


# ==============================================================================
# 5. WAL 账本
# ==============================================================================
class MartinLedger:
    """
    追加式领域事件账本 (Write-Ahead Log)。
    铁律: 任何与交易所的写操作 / 任何内存状态变更, 必须【先落盘, 再执行】。
    写盘失败即抛 LedgerError -> 主循环硬停机, 绝不静默带病继续。
    """
    COLUMNS = ["ts", "cycle_id", "signal_ts", "layer", "role", "action",
               "coid", "price", "qty", "status", "msg"]

    A_CYCLE_START = "CYCLE_START"
    A_CYCLE_END = "CYCLE_END"
    A_INTENT_PLACE = "INTENT_PLACE"
    A_PLACE_OK = "PLACE_OK"
    A_PLACE_FAIL = "PLACE_FAIL"
    A_PLACE_UNKNOWN = "PLACE_UNKNOWN"
    A_INTENT_CANCEL = "INTENT_CANCEL"
    A_CANCEL_OK = "CANCEL_OK"
    A_CANCEL_FAIL = "CANCEL_FAIL"
    A_FILL = "FILL"
    A_ALERT = "ALERT"
    A_RECOVER_REPLAY = "RECOVER_REPLAY"   # 冷启动重放汇总(替代逐笔重复写 FILL)

    # ---------- ALERT 的 status 语义分类 (断点续传据此完整还原周期标志) ----------
    S_SUSPEND_ADD = "SUSPEND_ADD"          # 停止加仓
    S_GIVEUP = "GIVEUP"                    # 某槽位永久放弃
    S_EXTERNAL_FLAT = "EXTERNAL_FLAT"      # 外部平仓, 本地库存强制归零
    S_FORCE_FLAT_SYNC = "FORCE_FLAT_SYNC"  # 收尾期实际持仓为0, 本地库存强制归零
    # 需人工介入(重启后必须继续保持停机)的告警
    HALT_STATUS = {
        "CLOSING_STUCK": "收尾阶段单据卡死未终态",
        "CLOSE_NOT_CONVERGED": "市价强平多次未收敛",
        "POS_PROBE_UNKNOWN": "实际持仓连续查询失败",
        "LEDGER_INCONSISTENT": "账本数量不守恒",
    }

    # ---------- 冷启动读取结果码 (决定 fail-open 还是 fail-closed) ----------
    LOAD_FRESH = "FRESH"              # 账本文件不存在: 全新启动, 可清场后 IDLE
    LOAD_IDLE = "IDLE"                # 账本可读且无未收尾周期: 可清场后 IDLE
    LOAD_RECOVER = "RECOVER"          # 账本可读且存在未收尾周期(蓝图完整): 走断点续传
    LOAD_CORRUPT = "CORRUPT"          # 账本存在但结构损坏(权限/表头/字段不配对): fail-closed
    LOAD_BLUEPRINT_BAD = "BP_BAD"     # 找到未收尾周期但蓝图 JSON 损坏/无效: fail-closed

    def __init__(self, strategy_id: str):
        self.filename = data_path(f"martin_ledger_{strategy_id}.csv")
        if not os.path.exists(self.filename):
            with open(self.filename, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.COLUMNS)

    def append(self, cycle_id, signal_ts, layer, role, action, coid="",
               price=0.0, qty=0.0, status="", msg=""):
        row = [datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
               cycle_id, signal_ts, layer, role, action, coid, price, qty, status, msg]
        try:
            with open(self.filename, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(row)
                f.flush()
                os.fsync(f.fileno())     # 强制落盘, 断电也不丢意图
        except Exception as e:
            logger.critical(f"[账本] 写盘失败! WAL 是断点续传唯一依据, 立即硬停机保留现场 | 错误:[{e}]")
            raise LedgerError(str(e))
        logger.info(f"[账本] {action} 周期[{cycle_id}] 层[{layer}] 角色[{role}] "
                    f"CID[{coid}] 价[{price}] 量[{qty}] {status} {msg}")

    @staticmethod
    def parse_ts(s) -> float:
        """把账本首列的可读时间还原为 epoch 秒(用于恢复周期原始起始时间, 保证超时计算正确)。"""
        try:
            return datetime.strptime(str(s).strip(), "%Y-%m-%d %H:%M:%S.%f").timestamp()
        except Exception:
            return 0.0

    # ---------- 冷启动读取 ----------
    def load_state(self) -> Tuple[str, Optional[dict], List[dict], int]:
        """
        返回 (状态码, 未收尾周期的 CYCLE_START 元数据 or None, 该周期所有历史行, 全局最大信号时间戳)。

        状态码语义决定 boot() 是 fail-open 还是 fail-closed:
          FRESH / IDLE          -> 可以安全清场并进入空闲态;
          RECOVER               -> 存在活周期且蓝图完整, 走断点续传;
          CORRUPT / BP_BAD      -> 结构损坏 / 字段缺失 / 蓝图无效: 此刻无法判断有无活仓与保护单,
                                   必须 fail-closed(保留原始账本与现场, 交人工处理)。
        全局最大信号时间戳作为信号去重水位线, 保证重启后不会重复消费旧信号。
        """
        if not os.path.exists(self.filename):
            return self.LOAD_FRESH, None, [], 0
        try:
            with open(self.filename, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                header = list(reader.fieldnames or [])
                rows = list(reader)
        except Exception as e:
            logger.critical(f"[账本] 文件存在但读取/解析失败, 判定 CORRUPT, 将 fail-closed"
                            f"(绝不清场、绝不接新信号) | 错误:[{e}]")
            return self.LOAD_CORRUPT, None, [], 0

        # ---- 结构校验: 表头必须完全一致, 每行必须与表头严格配对(不缺列、不多列) ----
        if header != self.COLUMNS:
            logger.critical(f"[账本] 表头与预期不一致, 判定 CORRUPT | 期望:{self.COLUMNS} 实际:{header}")
            return self.LOAD_CORRUPT, None, [], 0
        max_sig_ts = 0
        for i, r in enumerate(rows, start=2):
            if None in r or any(r.get(c) is None for c in self.COLUMNS):
                logger.critical(f"[账本] 第[{i}]行字段与表头不配对(缺列/多列), 判定 CORRUPT | 内容:{r}")
                return self.LOAD_CORRUPT, None, [], 0
            try:
                max_sig_ts = max(max_sig_ts, int(float(r.get("signal_ts") or 0)))
            except Exception:
                logger.critical(f"[账本] 第[{i}]行 signal_ts 非法, 判定 CORRUPT | 内容:{r}")
                return self.LOAD_CORRUPT, None, [], 0

        # 找最后一个 CYCLE_START, 并检查其后是否有同周期 CYCLE_END
        last_start_idx, last_cycle = -1, None
        for i, r in enumerate(rows):
            if r.get("action") == self.A_CYCLE_START:
                last_start_idx, last_cycle = i, r.get("cycle_id")
        if last_start_idx < 0:
            return self.LOAD_IDLE, None, [], max_sig_ts
        for r in rows[last_start_idx:]:
            if r.get("action") == self.A_CYCLE_END and r.get("cycle_id") == last_cycle:
                return self.LOAD_IDLE, None, [], max_sig_ts       # 已正常收尾

        meta_row = rows[last_start_idx]
        cycle_rows = [r for r in rows[last_start_idx:] if r.get("cycle_id") == last_cycle]
        try:
            meta = json.loads(meta_row.get("msg") or "{}")
            meta["cycle_id"] = meta_row.get("cycle_id")
            layers = meta.get("layers") or []
            valid = (meta.get("dir") in (Direction.LONG.value, Direction.SHORT.value)
                     and len(layers) > 0
                     and all(float(x["p"]) > 0 and float(x["q"]) > 0 and int(x["l"]) >= 0
                             for x in layers))
        except Exception as e:
            logger.critical(f"[账本] 未收尾周期[{last_cycle}]的蓝图解析异常, 判定 BP_BAD | 错误:[{e}]")
            return self.LOAD_BLUEPRINT_BAD, {"cycle_id": last_cycle}, cycle_rows, max_sig_ts
        if not valid:
            logger.critical(f"[账本] 未收尾周期[{last_cycle}]的蓝图无效(方向/层数/价量字段异常), "
                            f"判定 BP_BAD, 绝不使用残缺蓝图继续运行")
            return self.LOAD_BLUEPRINT_BAD, meta, cycle_rows, max_sig_ts
        return self.LOAD_RECOVER, meta, cycle_rows, max_sig_ts


# ==============================================================================
# 6. 配置与信号闸门
# ==============================================================================
class MartinConfig:
    """
    一个实例 = 一个独立子进程 = 一本独立账本 = 一个独立 OID 命名空间。
    同一币种可以配置多个(不同 signal / 不同马丁参数), 互不干扰。
    层数不再由配置指定: 完全由 max_loss_usdt 推导(唯一上限是防死循环的物理硬顶)。
    """

    def __init__(self, strategy_id, symbol, signal_name,
                 first_qty=0.0, first_notional=0.0,
                 step_pct=2.0, qty_mult=2.0, tp_pct=0.8, max_loss_usdt=50.0,
                 layer_loss_budget_ratio=0.80,
                 # ↑ 最深层满仓浮亏预算比例, 必须 < 1, 它决定"最深层成交价 → 止损价"的生存空间:
                 #   P_sl - P_last = sign * (max_loss - loss_last) / Q_full
                 #   若设为 1.0, 最深层成交瞬间浮亏就 ≈ max_loss, 止损价会贴死在最深层成交价上,
                 #   最后一仓刚成交就被扫止损(甚至条件单被交易所拒为 -2021 立即触发)。
                 max_signal_age_sec=90,
                 entry_timeout_sec=900,                # 入场超时: 一手未成则作废周期
                 max_cycle_sec=0,                      # 0=不限, 周期总超时强平
                 poll_interval_sec=2.0,
                 idle_poll_interval_sec=5.0,
                 sl_working_type="MARK_PRICE"):        # MARK_PRICE 防插针 / CONTRACT_PRICE 更灵敏
        self.strategy_id = str(strategy_id)
        self.symbol = symbol
        self.signal_name = signal_name
        self.first_qty = float(first_qty)
        self.first_notional = float(first_notional)
        self.step_pct = float(step_pct)
        self.qty_mult = float(qty_mult)
        self.tp_pct = float(tp_pct)
        self.max_loss_usdt = float(max_loss_usdt)
        self.layer_loss_budget_ratio = float(layer_loss_budget_ratio)
        self.max_signal_age_sec = float(max_signal_age_sec)
        self.entry_timeout_sec = float(entry_timeout_sec)
        self.max_cycle_sec = float(max_cycle_sec)
        self.poll_interval_sec = float(poll_interval_sec)
        self.idle_poll_interval_sec = float(idle_poll_interval_sec)
        self.sl_working_type = sl_working_type

    def validate(self):
        """启动前强校验: 配置错了直接拒绝启动, 绝不带病上线。"""
        errs = []
        if not self.strategy_id or len(self.strategy_id) > 8 or not self.strategy_id.isalnum():
            errs.append("strategy_id 必须为 1~8 位纯字母数字(它是 OID 命名空间与账本名)")
        if self.signal_name not in SIGNAL_REGISTRY:
            errs.append(f"signal_name[{self.signal_name}] 未在 SIGNAL_REGISTRY 中注册")
        if self.first_qty <= 0 and self.first_notional <= 0:
            errs.append("first_qty 与 first_notional 至少一个 > 0")
        if not (0 < self.step_pct <= 50):
            errs.append("step_pct 必须在 (0,50] 区间")
        if not (1.0 <= self.qty_mult <= 5.0):
            errs.append("qty_mult 必须在 [1.0,5.0] 区间(过大会指数爆仓)")
        if not (0 < self.tp_pct <= 50):
            errs.append("tp_pct 必须在 (0,50] 区间")
        if self.max_loss_usdt <= 0:
            errs.append("max_loss_usdt 必须 > 0")
        if not (0.1 <= self.layer_loss_budget_ratio <= 0.95):
            errs.append("layer_loss_budget_ratio 必须在 [0.1,0.95]"
                        "(必须<1, 否则止损价会贴死在最深层成交价上, 最后一仓刚成交就被扫止损)")
        if errs:
            for e in errs:
                logger.critical(f"[配置] 校验失败: {e}")
            raise SystemExit(1)


class SignalGate:
    """
    极简版信号闸门: 专为干净标准化的 DataFrame 设计。
    明确期望字段: timestamp(ms), event(OPEN/CLOSE), direction(LONG/SHORT), price(float)
    """

    def __init__(self, cfg: MartinConfig):
        self.cfg = cfg
        self.func = SIGNAL_REGISTRY[cfg.signal_name]
        self.watermark_ts = 0  # 已消费信号时间戳水位线(去重防刷)

    def set_watermark(self, ts: int):
        self.watermark_ts = max(self.watermark_ts, int(ts or 0))

    def poll(self) -> Optional[Signal]:
        try:
            df = self.func(self.cfg.symbol)
            if df is None or df.empty:
                return None
            row = df.iloc[-1]
            # 马丁策略依靠内部止盈止损平仓, 忽略外部 CLOSE
            if str(row['event']).upper() != "OPEN":
                return None
            direction_str = str(row['direction']).upper()
            if direction_str == "LONG":
                direction = Direction.LONG
            elif direction_str == "SHORT":
                direction = Direction.SHORT
            else:
                return None

            px = float(row['price'])
            ts = int(row['timestamp'])
            if ts <= self.watermark_ts:
                return None  # 老信号, 静默跳过
            age_sec = (time.time() * 1000 - ts) / 1000.0
            if age_sec > self.cfg.max_signal_age_sec:
                logger.info(f"[信号] 信号已过期, 拒绝追单 | 滞后:[{age_sec:.1f}s]")
                self.set_watermark(ts)
                return None
            return Signal(direction, px, ts, self.cfg.signal_name)
        except Exception as e:
            logger.error(f"[信号] 读取标准信号失败: {e}")
            return None


# ==============================================================================
# 7. 马丁蓝图 (价格全静态固化: 开仓价 / 每层止盈价 / 全局止损价)
# ==============================================================================
class LayerPlan:
    """
    一层加仓的静态计划 + 【当前活跃订单指针】。
    指针只指向 OrderRegistry 中的一个 coid; 换单只改指针, 历史订单继续留在登记表被轮询。
    注意: 开仓层的"目标价量"就是蓝图里的 price/qty(永不变), 因此绝不额外持有 target_* 字段。
    """
    __slots__ = ("layer", "price", "qty", "avg", "tp",
                 "coid", "attempts", "next_retry_ts", "abandoned")

    def __init__(self, layer, price, qty, avg=0.0, tp=0.0):
        self.layer = layer
        self.price = float(price)
        self.qty = float(qty)
        self.avg = float(avg)          # 该层满仓后的【理论持仓均价】(一次算死, 永不重算)
        self.tp = float(tp)            # 该层对应的【固定止盈价】(一次算死, 永不重算)
        self.coid = ""
        self.attempts = 0
        self.next_retry_ts = 0.0
        self.abandoned = False         # 永久放弃该层(次数超限 / 参数非法)

    def to_dict(self):
        return {"l": self.layer, "p": self.price, "q": self.qty, "a": self.avg, "t": self.tp}


class Blueprint:
    def __init__(self, direction: Direction, layers: List[LayerPlan], base_price: float,
                 sl_price: float = 0.0):
        self.direction = direction
        self.layers = layers
        self.base_price = base_price
        self.sl_price = float(sl_price)   # 全周期唯一且绝对不变的止损价

    @property
    def total_qty(self) -> float:
        return sum(l.qty for l in self.layers)

    @property
    def total_notional(self) -> float:
        return sum(l.price * l.qty for l in self.layers)

    def to_json_layers(self):
        return [l.to_dict() for l in self.layers]


class BlueprintBuilder:
    """
    由信号 + 配置 + 交易规格推导完整马丁蓝图。价格全部一次性算死, 之后永不修改:
      * 加仓价: 第 i 层价 = 第 i-1 层【理论持仓均价】的等比偏离 (avg * (1 - sign*step_pct%))
      * 止盈价: 第 i 层止盈 = 第 i 层【理论持仓均价】的等比偏离 (avg * (1 + sign*tp_pct%))
      * 止损价: 按最后一层满仓时恰好亏 max_loss_usdt 反解, 全周期唯一固定
    层数判定: 只有当"下一层成交后的浮亏 <= max_loss * layer_loss_budget_ratio"时才允许挂该层。
    唯一的层数硬顶是 HARD_MAX_LAYERS, 纯粹作为防死循环的物理安全底线。
    """

    @staticmethod
    def build(cfg: MartinConfig, spec: InstrumentSpec, sig: Signal) -> Optional[Blueprint]:
        d = sig.direction
        sign = d.sign
        # 首单价修约方向: 做多向下(买得更便宜), 做空向上(卖得更贵), 对自己有利
        p0 = spec.round_price(sig.limit_price, "down" if d is Direction.LONG else "up")
        if p0 <= 0:
            logger.info("[蓝图] 首单价修约后非法, 丢弃信号")
            return None

        base_qty = cfg.first_qty if cfg.first_notional <= 0 else cfg.first_notional / p0
        budget = cfg.max_loss_usdt * cfg.layer_loss_budget_ratio

        layers: List[LayerPlan] = []
        rows = []
        acc_qty = acc_cost = 0.0
        prev_price = prev_avg = 0.0
        i = 0

        while True:
            if i >= HARD_MAX_LAYERS:      # 物理硬顶: 纯防死循环, 正常永远走不到
                logger.critical(f"[蓝图] 触及物理硬顶[{HARD_MAX_LAYERS}]层, 强制收口(防死循环底线)")
                break

            price = p0 if i == 0 else prev_avg * (1 - sign * cfg.step_pct / 100.0)
            price = spec.round_price(price, "down" if d is Direction.LONG else "up")
            if price <= 0:
                break
            # 修约后层间价格不再单调 => 无法继续细分, 层数收口
            if i > 0 and ((d is Direction.LONG and price >= prev_price) or
                          (d is Direction.SHORT and price <= prev_price)):
                logger.info(f"[蓝图] 第[{i}]层相对均价的等比价差已无法继续细分"
                            f"(tick={spec.tick_size}), 层数在此收口")
                break

            qty = spec.normalize_open_qty(base_qty * (cfg.qty_mult ** i), price)
            if qty <= 0:
                logger.info(f"[蓝图] 第[{i}]层无法构造合法数量(超 maxQty 或精度不足), 层数收口")
                break

            n_qty = acc_qty + qty
            n_cost = acc_cost + price * qty
            n_avg = n_cost / n_qty
            loss_at_fill = sign * (n_avg - price) * n_qty        # 该层成交瞬间的浮亏(>=0)

            if i == 0:
                # 首单必须满足最小名义价值, 否则整个信号作废(不能只丢一层, 会破坏马丁结构)
                if spec.notional(price, qty) < spec.min_notional * (1 - 1e-9):
                    logger.info(f"[蓝图] 首单名义价值[{spec.notional(price, qty):.4f}]低于交易所底线"
                                f"[{spec.min_notional}], 丢弃信号")
                    return None
            elif loss_at_fill > budget:
                logger.info(f"[蓝图] 第[{i}]层成交后浮亏[{loss_at_fill:.2f}U]将超出亏损预算"
                            f"[{budget:.2f}U = 最大亏损{cfg.max_loss_usdt}×"
                            f"{cfg.layer_loss_budget_ratio}], 层数在此收口(本周期只铺{i}层)")
                break

            tp = spec.round_price(n_avg * (1 + sign * cfg.tp_pct / 100.0),
                                  "up" if d is Direction.LONG else "down")
            layers.append(LayerPlan(i, price, qty, n_avg, tp))
            rows.append((i, price, qty, n_qty, n_cost, n_avg, tp, loss_at_fill))
            acc_qty, acc_cost, prev_price, prev_avg = n_qty, n_cost, price, n_avg
            i += 1

        if not layers:
            logger.info("[蓝图] 未能生成任何合法层, 丢弃信号")
            return None
        if len(layers) < 2:
            logger.info(f"[蓝图] ⚠️ 仅能生成 [1] 层, 马丁结构退化为单笔交易 | "
                        f"请检查 max_loss_usdt / first_qty 配比是否合理")

        # ---------- 全局唯一止损价: 最后一层满仓时恰好亏 max_loss_usdt ----------
        final_avg = acc_cost / acc_qty
        sl_raw = final_avg - sign * cfg.max_loss_usdt / acc_qty
        sl = spec.round_price(sl_raw, "up" if d is Direction.LONG else "down")
        if d is Direction.LONG:
            if sl <= 0:
                sl = spec.round_price(max(spec.tick_size, p0 * 0.02), "up")
                logger.critical(f"[蓝图] 满仓止损价计算为非正(最大亏损远超满仓名义价值), "
                                f"已夹到极低保护位[{sl}]")
            if sl >= layers[-1].price:
                logger.critical(f"[蓝图] 全局止损价[{sl}]未低于最深层价[{layers[-1].price}], "
                                f"参数异常, 丢弃信号")
                return None
        else:
            if sl <= layers[-1].price:
                logger.critical(f"[蓝图] 全局止损价[{sl}]未高于最深层价[{layers[-1].price}], "
                                f"参数异常, 丢弃信号")
                return None

        head = (f"\n===== [马丁蓝图] {cfg.strategy_id} {cfg.symbol} {d.value} "
                f"信号价:{sig.limit_price} 层数:{len(layers)} =====\n"
                f"{'层':>3} {'开仓价':>14} {'数量':>12} {'累计量':>12} {'累计成本U':>12} "
                f"{'理论均价':>14} {'该层止盈价':>14} {'该层浮亏U':>10}\n")
        body = "\n".join(
            f"{r[0]:>3} {r[1]:>14.8g} {r[2]:>12.8g} {r[3]:>12.8g} {r[4]:>12.2f} "
            f"{r[5]:>14.8g} {r[6]:>14.8g} {r[7]:>10.2f}" for r in rows)
        tail = (f"\n最大名义价值:[{acc_cost:.2f}U] 满仓均价:[{final_avg:.8g}] "
                f"全局固定止损价:[{sl:.8g}](距满仓均价 {abs(sl / final_avg - 1) * 100:.3f}%, "
                f"距最深层成交价 {abs(sl / layers[-1].price - 1) * 100:.3f}%)\n"
                f"最大亏损设定:[{cfg.max_loss_usdt}U] 止盈:[{cfg.tp_pct}%] "
                f"间距:[{cfg.step_pct}% 相对当层理论均价等比] 倍数:[{cfg.qty_mult}]\n"
                f"=======================================================================")
        logger.info(head + body + tail)
        return Blueprint(d, layers, p0, sl)


# ==============================================================================
# 8. 虚拟仓位账 (I1: 唯一记账来源 / I5: 价格刚性 / I9: 数量守恒)
# ==============================================================================
class PositionBook:
    """
    本策略的虚拟仓位账。只由"本策略 OID 的成交增量"驱动, 与交易所仓位完全解耦。

    【会计模型: 标准库存移动平均法】
      avg = cost / open_qty
      * 加仓: open_qty += q, cost += p*q                  -> 均价按加权移动
      * 平仓: 按【当前均价】等比扣减成本 cost -= avg*q      -> 数学上均价恒定不变
    因此"只要没有新的加仓, 均价/止盈价/止损价绝对不动"(I5 价格刚性)。
    realized 仅作统计口径, 绝不参与任何价格计算。

    【I9 数量守恒】add_close 不再静默截断超额部分, 而是把超额量原样返回给调用方裁决;
    void_qty 记录"已被强制归零并已告警核销"的库存, 供后续滞后回执安全抵扣, 避免重复误报。
    """

    def __init__(self, direction: Direction):
        self.direction = direction
        self.open_qty = 0.0            # 当前虚拟持仓数量
        self.cost = 0.0                # 当前持仓对应的总成本(与 open_qty 严格配对)
        self.total_open_filled = 0.0   # 累计开仓成交量(只增, I4 硬闸依据)
        self.total_open_cost = 0.0     # 累计开仓成本(只增, 仅复盘用)
        self.total_close_filled = 0.0  # 累计平仓成交量(只增)
        self.realized = 0.0            # 已实现盈亏(仅统计用)
        self.void_qty = 0.0            # 已核销(强制归零)的虚拟库存, 允许被滞后平仓回执抵扣

    @property
    def avg(self) -> float:
        """当前持仓均价: 只由实际加仓(OPEN)决定, 平仓绝不改变它。"""
        return self.cost / self.open_qty if self.open_qty > 1e-12 else 0.0

    @property
    def entry_avg(self) -> float:
        """本周期加权入场均价(含已平仓部分), 仅用于日志与事后复盘。"""
        return (self.total_open_cost / self.total_open_filled
                if self.total_open_filled > 1e-12 else 0.0)

    def add_open(self, price: float, qty: float):
        if qty <= 0:
            return
        self.open_qty += qty
        self.cost += price * qty
        self.total_open_filled += qty
        self.total_open_cost += price * qty

    def add_close(self, price: float, qty: float) -> float:
        """
        平仓只扣减持仓数量与对应比例的成本, 均价数学恒定(价格刚性)。
        返回【超额量】: >0 表示平仓成交超过本地已知库存 => 账本自相矛盾的硬证据(I9)。
        """
        if qty <= 0:
            return 0.0
        self.total_close_filled += qty          # 统计口径: 交易所实际成交多少就记多少
        eff = min(qty, self.open_qty)
        excess = qty - eff
        if eff > 1e-12:
            cur_avg = self.avg
            self.realized += self.direction.sign * (price - cur_avg) * eff
            self.cost -= cur_avg * eff          # 按当前均价等比扣减 -> avg 保持不变
            self.open_qty -= eff
            if self.open_qty <= 1e-12:          # 彻底归零, 杜绝浮点残余
                self.open_qty = 0.0
                self.cost = 0.0
        if excess > 0 and self.void_qty > 0:    # 已核销并已告警的量: 允许抵扣, 不重复报警
            absorb = min(excess, self.void_qty)
            self.void_qty -= absorb
            excess -= absorb
        return max(0.0, excess)

    def force_flat(self):
        """
        交易所实际持仓已归零时的强制同步(打破收尾死锁 / 闭环外部平仓)。
        被核销的库存记入 void_qty: 之后若收到滞后的平仓回执, 可安全抵扣而不误判不守恒。
        """
        self.void_qty += self.open_qty
        self.open_qty = 0.0
        self.cost = 0.0

    def unrealized(self, price: float) -> float:
        if self.open_qty <= 1e-12:
            return 0.0
        return self.direction.sign * (price - self.avg) * self.open_qty

    def snapshot(self) -> dict:
        return {"open_qty": self.open_qty, "avg": self.avg, "entry_avg": self.entry_avg,
                "realized": self.realized, "open_filled": self.total_open_filled,
                "close_filled": self.total_close_filled}


# ==============================================================================
# 9. 订单全生命周期登记表 (Order Registry)
# ==============================================================================
class TrackedOrder:
    """
    本周期生成过的【每一个】OID 都在这里留档, 直到确认终态。
    幂等入账所需的 acked_qty / acked_cost 也挂在这里 —— 一个订单的全部真相只有一处。
    """
    __slots__ = ("coid", "role", "layer", "price", "qty", "state", "ex_id",
                 "acked_qty", "acked_cost", "act_ts", "seen_ts", "probe_ts", "probes")

    def __init__(self, coid, role, layer, price, qty, state, ts):
        self.coid = coid
        self.role = role
        self.layer = int(layer)
        self.price = float(price or 0.0)
        self.qty = float(qty or 0.0)
        self.state = state
        self.ex_id = ""
        self.acked_qty = 0.0      # I2: 已入账成交量
        self.acked_cost = 0.0     # 已入账累计金额(反求边际成交价)
        self.act_ts = float(ts)   # 最近一次写请求(挂单/撤单)的时间
        self.seen_ts = 0.0        # 最近一次在盘口快照中被看到的时间
        self.probe_ts = 0.0       # 最近一次主动点查的时间(同优先级轮转依据, 防尾部饥饿)
        self.probes = 0           # 连续点查未知次数

    @property
    def is_force_market(self) -> bool:
        """市价兜底强平单: 一经发出即刻撮合、不可撤销, 全系统据此对它豁免撤单。"""
        return self.role is OrderRole.SL and self.layer == 99

    def __repr__(self):
        return f"{self.role.value}{self.layer:02d}/{self.state.value}"


class OrderRegistry:
    """周期级订单登记表: 插入顺序 = 生成顺序, 因此"最后一个"天然就是最新的一代。"""

    def __init__(self):
        self._d: Dict[str, TrackedOrder] = {}

    def open(self, coid, role, layer, price, qty,
             state=OrderState.PENDING, ts=None) -> TrackedOrder:
        t = TrackedOrder(coid, role, layer, price, qty, state,
                         time.time() if ts is None else ts)
        self._d[coid] = t
        return t

    def get(self, coid) -> Optional[TrackedOrder]:
        return self._d.get(coid) if coid else None

    def state_of(self, coid) -> OrderState:
        t = self._d.get(coid) if coid else None
        return t.state if t else OrderState.NOT_PLACED

    def all(self) -> List[TrackedOrder]:
        return list(self._d.values())

    def alive(self, role: Optional[OrderRole] = None) -> List[TrackedOrder]:
        return [t for t in self._d.values()
                if t.state.alive and (role is None or t.role is role)]

    def newest(self, role: OrderRole, layer: int) -> Optional[TrackedOrder]:
        found = None
        for t in self._d.values():
            if t.role is role and t.layer == layer:
                found = t
        return found

    def count(self, role: OrderRole, layer: Optional[int] = None) -> int:
        return sum(1 for t in self._d.values()
                   if t.role is role and (layer is None or t.layer == layer))

    def filled_qty(self, role: OrderRole) -> float:
        return sum(t.acked_qty for t in self._d.values() if t.role is role)


# ==============================================================================
# 10. 周期状态机 (5 步单向流水线的第 2~5 步)
# ==============================================================================
class CycleCtx:
    """
    注入给周期的运行环境。实际持仓的【分级缓存】也放在这里:
      常规对齐用 POSITION_CACHE_SEC 轻量缓存防限频; 高危路径(新成交后刷新/强平裁决/
      外部平仓确认)传 force=True 强制击穿缓存实时拉取。
    """

    def __init__(self, cfg: MartinConfig, gw: ExchangeGateway, ledger: MartinLedger,
                 spec: InstrumentSpec):
        self.cfg = cfg
        self.gw = gw
        self.ledger = ledger
        self.spec = spec
        self._pos_cache = (0.0, "", None)     # (ts, position_side, qty)

    def position(self, position_side: str, force: bool = False) -> Optional[float]:
        ts, side, qty = self._pos_cache
        if (not force) and side == position_side and time.time() - ts < POSITION_CACHE_SEC:
            return qty
        q = self.gw.fetch_position_qty(position_side)
        self._pos_cache = (time.time(), position_side, q)
        return q


class ExitOrder:
    """
    止盈 / 止损槽位: 只持有当前活跃 OID 指针 + 对齐所需的在线值。
    目标价来自蓝图(固定)、目标量来自统一夹逼(动态), 都是现算现用, 因此绝不冗余存储 target_*。
    """
    __slots__ = ("role", "coid", "live_price", "live_remaining",
                 "attempts", "next_retry_ts", "abandoned")

    def __init__(self, role: OrderRole):
        self.role = role
        self.coid = ""
        self.live_price = 0.0
        self.live_remaining = 0.0
        self.attempts = 0
        self.next_retry_ts = 0.0
        self.abandoned = False


class MartinCycle:
    """
    一个马丁周期的完整状态机, 每个 Tick 执行一次单向流水线:
        Reconcile(对账) -> Evaluate(决策) -> Align(先出场后开仓) -> Execute(执行)
    一旦 end_reason 被置上, 流水线自动切换到【收尾两段式】: 先撤净, 全终态后才市价平残余。
    价格全静态(蓝图算死), 数量全动态(跟随 min(虚拟持仓, 交易所实际持仓))。
    所有状态修改只发生在主线程调用的 tick() 内, 天然单写者、无需加锁。
    """

    def __init__(self, ctx: CycleCtx, cycle_id: str, signal_ts: int,
                 direction: Direction, blueprint: Blueprint):
        self.ctx = ctx
        self.cycle_id = cycle_id
        self.signal_ts = signal_ts
        self.direction = direction
        self.bp = blueprint
        self.book = PositionBook(direction)
        self.registry = OrderRegistry()
        self.tp = ExitOrder(OrderRole.TP)
        self.sl = ExitOrder(OrderRole.SL)
        self.add_suspended = False
        self.end_reason: Optional[EndReason] = None
        self.created_ts = time.time()
        self.last_price = 0.0
        self.sl_breach_since = 0.0
        self.ext_flat_since = 0.0
        self.ext_flat_done = False      # 已确认被外部平仓(供 Evaluate 精确归因)
        self.closing_since = 0.0
        self.stuck_since = 0.0
        self.force_attempts = 0
        self.next_force_retry_ts = 0.0  # 强平被拒后的退避冷却截止时间
        self.pos_unknown = 0
        self.book_error = ""             # I9: 账本不守恒证据, 非空即停发单并停机
        self._fill_events = 0            # 本轮新增成交笔数(>0 则强刷真实持仓)
        self._clamp_log_ts = 0.0

    # ---------------------- WAL 记账 (统一填充周期上下文与占位符) ----------------------
    def wal_order(self, action, coid, layer, role, price=0.0, qty=0.0, status="", msg=""):
        """订单级事件: 周期 ID / 信号时间戳自动带上, 业务只传订单自身的增量价量。"""
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role, action,
                               coid, price, qty, status, msg)

    def wal_cycle(self, action, status, msg="", price=0.0, qty=0.0):
        """周期级事件(无归属订单): 层号/角色自动填占位符。"""
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, -1, "-", action,
                               "", price, qty, status, msg)

    # ==========================================================================
    # 主入口: 5 步流水线的第 2~5 步
    # ==========================================================================
    def tick(self, w: World) -> TickResult:
        if w.price > 0:
            self.last_price = w.price
        actions = self._reconcile(w)                 # 2. 对账(全流水线唯一的查询环节)
        if self.book_error:                          # I9: 账本矛盾 -> 立刻停发单, 交人工
            logger.critical(f"[周期] 账本存在数量不守恒矛盾, 已中止本轮一切发单并停机 | "
                            f"证据:[{self.book_error}]")
            return TickResult.HALT
        self._evaluate(w)                            # 3. 纯内存决策
        if self.end_reason:
            return self._closing(w, actions)         # 收尾: 先撤净 -> 全终态 -> 市价平残余
        actions += self._plan_exits(w)               # 4. 对齐: 先出场(保护单优先级最高)
        if self.end_reason:
            return self._closing(w, actions)         #    出场规划可能直接判定收尾
        actions += self._plan_opens(w)               #    再开仓
        self._execute(actions, w)                    # 5. 集中下发(带许可检查)
        return TickResult.CONTINUE

    # ==========================================================================
    # 第二步 Reconcile: 让登记表与交易所真相一致, 并把新增成交入账
    # ==========================================================================
    def _reconcile(self, w: World, probe_budget: Optional[int] = MAX_PROBE_PER_TICK) -> List[Action]:
        now, actions = w.ts, []
        self._fill_events = 0

        # ---- (1) 盘口快照 -> 登记表 ----
        for coid, o in w.orders.items():
            t = self.registry.get(coid)
            if t is None:
                actions += self._handle_foreign(coid, o)
                continue
            self._absorb(t, o, now)
            if t.state is OrderState.PENDING:
                t.state = OrderState.LIVE
            elif not t.state.alive:
                logger.critical(f"[对账] 角色[{t.role.value}]层[{t.layer}]曾被判定终态"
                                f"[{t.state.value}]却重现盘口, 立即纠正为在盘并纳入对齐 | "
                                f"CID:[{coid}]")
                t.state = OrderState.LIVE

        # ---- (2) 不在快照中的存活单: 主动点查裁决(保护单优先 + 同级轮转, 单轮限额防限频) ----
        self._probe_missing(w, probe_budget)

        # ---- (3) 本轮有新成交 => 持仓缓存必然过期: 集中强查一次, 供决策与对齐用最新鲜的值 ----
        if self._fill_events:
            w.pos_qty = self.ctx.position(self.direction.position_side, force=True)
            logger.info(f"[对账] 本轮新增[{self._fill_events}]笔成交入账, 已强制刷新真实持仓:"
                        f"[{w.pos_qty}](防用旧缓存误撤保护单)")

        # ---- (4) 外部平仓三重确认(本阶段允许网络 IO; 只做库存校正, 归因交回 Evaluate) ----
        self._verify_external_flat(w)
        return actions

    def _absorb(self, t: TrackedOrder, o: UniOrder, now: float):
        """
        吸收一次订单观察(快照与点查共用): 同步公共字段 + 幂等入账 + 同步出场槽位在线值。
        状态跃迁刻意留给调用方: 快照代表"确认在盘", 点查代表"拿到确定回执", 语义不同不可合并。
        """
        t.seen_ts = now
        t.probes = 0
        t.ex_id = o.ex_id or t.ex_id
        self._book_fill(t, o)
        if t.role is not OrderRole.OPEN:
            slot = self.tp if t.role is OrderRole.TP else self.sl
            if slot.coid == t.coid:
                slot.live_price = (o.stop_price if (t.role is OrderRole.SL and o.stop_price > 0)
                                   else o.price)
                slot.live_remaining = o.remaining

    def _probe_missing(self, w: World, budget: Optional[int]):
        """
        点查所有"存活但不在快照"的单据。budget=None 表示全量(冷启动接管与外部平仓兜底用)。
        排序: 保护单永远优先; 同优先级内按【最久未点查】轮转, 杜绝尾部单据连续多轮饥饿。
        """
        now = w.ts
        cands = [t for t in self.registry.alive() if t.coid not in w.orders]
        cands.sort(key=lambda t: (1 if t.role is OrderRole.OPEN else 0, t.probe_ts))
        for t in cands:
            if budget is not None and budget <= 0:
                break
            # 市价强平单, 或"实际有仓但本地未记账(秒成单)", 直接穿透冷静期加速对账
            fast = t.is_force_market or (w.pos_qty and w.pos_qty > 0 and self.book.open_qty == 0)
            if not fast and now - max(t.act_ts, t.seen_ts) < ORDER_GRACE_SEC:
                continue
            if budget is not None:
                budget -= 1
            t.probe_ts = time.time()
            self._probe(t, w)

    def _probe(self, t: TrackedOrder, w: World):
        """单据点查裁决。只有拿到【确切回执】才允许拨到终态。"""
        o = self.ctx.gw.fetch_order(t.coid)
        if o is ORDER_NOT_FOUND:
            if t.role is OrderRole.SL and t.layer == 0 and not w.algo_ok:
                logger.info(f"[对账] 条件单快照本轮降级, 暂不采信'订单不存在'回执, "
                            f"保持原状 | CID:[{t.coid}]")
                return
            t.state = OrderState.DEAD if t.acked_qty > 0 else OrderState.NOT_PLACED
            logger.info(f"[对账] {t} 交易所明确回执不存在 -> 置[{t.state.value}] | CID:[{t.coid}]")
            return
        if not isinstance(o, UniOrder):
            t.probes += 1
            if t.probes % PROBE_ALERT_EVERY == 0:
                logger.critical(f"[对账] {t} 已连续[{t.probes}]次点查结果未知, 原地锁定不换号重发, "
                                f"请留意网络/接口状态 | CID:[{t.coid}]")
            return
        self._absorb(t, o, w.ts)
        if o.status == "FILLED":
            t.state = OrderState.FILLED
            logger.info(f"[对账] {t} 已确认完全成交 @[{o.avg_price or o.price}] x[{o.filled}]")
        elif o.is_terminal:
            if t.role is OrderRole.OPEN and o.filled > 0:
                t.state = OrderState.DEAD
                logger.critical(f"[对账] {t} 部分成交后被撤销, 按保守策略不补挂剩余量 | "
                                f"已成交[{o.filled}]/[{o.amount}]")
            else:
                t.state = OrderState.NOT_PLACED
        else:
            t.state = OrderState.LIVE      # 仍在盘口(快照滞后 / 条件单未触发)

    def _handle_foreign(self, coid: str, o: UniOrder) -> List[Action]:
        """盘口出现"带本策略前缀但不在登记表"的单: 旧周期残留 / 手工单 / 极端丢档。"""
        p = OidCodec.parse(coid)
        if p and p.cycle_id == self.cycle_id:
            # 同周期却不在登记表(极罕见): 先纳管以保证其成交必被入账, 再撤销
            logger.critical(f"[对账] 发现同周期孤儿挂单(登记表缺失), 已纳管并撤销 | CID:[{coid}]")
            t = self.registry.open(coid, p.role, p.layer, o.price, o.amount, OrderState.LIVE)
            self._book_fill(t, o)
            return [Action.cancel(coid, "同周期孤儿单(登记表缺失): 纳管后撤销")]
        filled = float(o.filled or 0.0)
        logger.critical(f"[对账] 发现非本周期的本策略残留挂单, 立即撤销 | CID:[{coid}] "
                        f"所属周期:[{p.cycle_id if p else '?'}] 状态:[{o.status}] 成交:[{filled}]")
        if filled > 0:
            self.wal_order(MartinLedger.A_ALERT, coid, p.layer if p else -1,
                           p.role.value if p else "?", o.avg_price or o.price, filled,
                           "CROSS_CYCLE_FILL", "上一代周期挂单发生成交, 未入本周期账本, 需人工核对")
        return [Action.cancel(coid, "非本周期残留挂单")]

    # ---------------------- 幂等入账 (I2) ----------------------
    def _book_fill(self, t: TrackedOrder, o: UniOrder) -> float:
        filled = float(o.filled or 0.0)
        if filled <= 0:
            return 0.0
        delta = filled - t.acked_qty
        if delta <= max(QTY_EPS_RATIO, self.ctx.spec.step_size * 1e-6):
            return 0.0
        # 计算真实边际成交价, 消除大单分批吃单导致的均价漂移
        cum_price = o.avg_price or o.price or 0.0
        if cum_price <= 0:
            cum_price = (self.bp.layers[t.layer].price
                         if (t.role is OrderRole.OPEN and t.layer < len(self.bp.layers))
                         else (self.book.avg or self.last_price))
        now_cost = cum_price * filled
        marginal = (now_cost - t.acked_cost) / delta
        if marginal <= 0:
            marginal = cum_price
        # WAL 铁律: 先落盘成功, 再改内存。写盘失败抛 LedgerError -> 主循环硬停机
        self.wal_order(MartinLedger.A_FILL, t.coid, t.layer, t.role.value, marginal, delta, "OK",
                       f"入账前持仓{self.book.open_qty:.8g} 均价{self.book.avg:.8g}")
        t.acked_qty = filled
        t.acked_cost = now_cost
        self._fill_events += 1
        self._apply_fill(t, marginal, delta, tag="[成交]")
        return delta

    def _apply_fill(self, t: TrackedOrder, price: float, qty: float, tag: str):
        """把成交增量落到虚拟账本(I1/I5/I9)。WAL 重放与实时观测共用这一段。"""
        if t.role is OrderRole.OPEN:
            self.book.add_open(price, qty)
        else:
            excess = self.book.add_close(price, qty)
            if excess > 0 and not self.ctx.spec.qty_is_dust(excess) and not self.book_error:
                # I9: 平仓量超出本地已知库存 => 账本自相矛盾, 绝不静默截断掩盖
                self.book_error = (f"平仓入账[{qty:.8g}]超出本地已知库存, 超额[{excess:.8g}] "
                                   f"(CID[{t.coid}] 角色[{t.role.value}] 层[{t.layer}])")
                logger.critical(f"[账本] 数量不守恒! 立即停止一切发单并停机等待人工核对 | "
                                f"{self.book_error}")
                self.wal_cycle(MartinLedger.A_ALERT, "LEDGER_INCONSISTENT", self.book_error,
                               price, excess)
        logger.info(f"{tag} 角色[{t.role.value}] 层[{t.layer}] @[{price:.8g}] x[{qty:.8g}] "
                    f"=> 持仓[{self.book.open_qty:.8g}] 均价[{self.book.avg:.8g}] "
                    f"已实现[{self.book.realized:+.4f}U]")

    def replay_fill(self, t: TrackedOrder, price: float, qty: float):
        """WAL 断点续传专用: 严格按账本时间序重放成交, 精确还原崩溃前那一刻的均价。"""
        if qty <= 0:
            return
        t.acked_qty += qty
        t.acked_cost += price * qty
        # 数量满额直接推至终态, 防止后续点查报 NOT_FOUND 被误判为"从未下单"
        if t.qty > 0 and t.acked_qty >= t.qty * (1 - 1e-6):
            t.state = OrderState.FILLED
        self._apply_fill(t, price, qty, tag="[重放]")

    # ---------------------- 外部平仓核验 (I6, 唯一的决策前置网络查询) ----------------------
    def _verify_external_flat(self, w: World):
        """
        外部平仓闭环(手工平仓 / 交易所强平 / ADL / 其它工具误操作)。
        唯一安全推论: 双向持仓下 positionSide 持仓为 0 => 本策略必然也已无仓(>0 则什么都推不出)。
        三重确认(快照 -> 宽限计时 -> 强制实时核验 + 全量点查)后才强制本地归零;
        归零后不在此处判定终结原因, 交由 Evaluate 用统一口径归因, 保持决策层单一。
        """
        spec = self.ctx.spec
        if (self.end_reason or spec.qty_is_dust(self.book.open_qty)
                or w.pos_qty is None or not spec.qty_is_dust(w.pos_qty)):
            self.ext_flat_since = 0.0     # 已收尾 / 本地已空 / 信息不全 / 确实还有仓 -> 不做判定
            return
        if self.ext_flat_since == 0.0:
            self.ext_flat_since = w.ts
            logger.critical(f"[外部干预] 本地虚拟持仓[{self.book.open_qty:.8g}] 但交易所"
                            f"[{self.direction.value}]实际持仓已为0, 疑被手工平仓/强平/ADL, "
                            f"进入[{EXTERNAL_FLAT_CONFIRM_SEC}s]二次确认...")
            return
        if w.ts - self.ext_flat_since < EXTERNAL_FLAT_CONFIRM_SEC:
            return

        real = self.ctx.position(self.direction.position_side, force=True)   # 高危: 击穿缓存
        if real is None:
            logger.critical("[外部干预] 二次核验实际持仓失败(结果未知), 暂不归零, 下一轮继续确认")
            return
        if not spec.qty_is_dust(real):
            logger.info(f"[外部干预] 二次核验发现实际持仓[{real:.8g}]仍在(前次为接口滞后), 撤销判定")
            self.ext_flat_since = 0.0
            return
        # 兜底: 全量点查一遍存活单, 万一是本策略止盈/止损刚成交, 应归因为 TP/SL 而非外部平仓
        self._probe_missing(w, None)
        if spec.qty_is_dust(self.book.open_qty):
            logger.info("[外部干预] 兜底全量点查后本地已归零, 判定为本策略出场单成交, 按正常出场归因")
            self.ext_flat_since = 0.0
            return

        residual = self.book.open_qty
        logger.critical(f"[外部干预] 三重确认交易所[{self.direction.value}]持仓确已归零, "
                        f"判定本策略仓位被外部平掉, 强制本地账本归零 | 作废虚拟残余:[{residual:.8g}] "
                        f"均价:[{self.book.avg:.8g}]")
        self.wal_cycle(MartinLedger.A_ALERT, MartinLedger.S_EXTERNAL_FLAT,
                       "交易所实际持仓三重确认为0, 判定外部平仓, 本地强制归零",
                       self.book.avg, residual)
        self.book.force_flat()
        self.ext_flat_since = 0.0
        self.ext_flat_done = True

    # ==========================================================================
    # 第三步 Evaluate: 纯内存决策(零网络 IO), 只产出"继续 / 停止加仓 / 进入收尾"
    # ==========================================================================
    def _evaluate(self, w: World):
        if self.end_reason:
            return
        spec, cfg, now = self.ctx.spec, self.ctx.cfg, w.ts

        # (1) I4 总量硬闸
        if self.book.total_open_filled > self.bp.total_qty * OVERFILL_TOLERANCE:
            self._suspend_add(f"I4 累计开仓成交[{self.book.total_open_filled:.8g}]"
                              f"越界(蓝图总量{self.bp.total_qty:.8g})")

        # (2) 一手未成: 越过止损线(跳空盲区) / 反向脱轨 / 入场超时 -> 直接作废本周期
        if self.book.total_open_filled <= 0:
            if self._crossed(w.price, self.bp.sl_price):
                self._end(EndReason.NO_FILL,
                          f"首单未成且现价[{w.price}]已越过全局止损价[{self.bp.sl_price}], "
                          f"蓝图整体失效, 禁止铺单")
                return
            if w.price > 0 and self.bp.base_price > 0:
                runaway = self.direction.sign * (w.price / self.bp.base_price - 1) * 100
                if runaway > max(cfg.step_pct * 1.5, 2.0):
                    self._end(EndReason.NO_FILL,
                              f"首单未成且行情反向脱轨起飞(偏离{runaway:.2f}%)")
                    return
            if now - self.created_ts > cfg.entry_timeout_sec:
                self._end(EndReason.NO_FILL, f"入场超时{cfg.entry_timeout_sec}s仍无任何成交")
            return

        # (3) 已平净 -> 按本周期出场单累计成交归因(外部平仓已由对账阶段置标记)
        if spec.qty_is_dust(self.book.open_qty):
            tpq = self.registry.filled_qty(OrderRole.TP)
            slq = self.registry.filled_qty(OrderRole.SL)
            if self.ext_flat_done:
                self._end(EndReason.MANUAL_FLAT,
                          "交易所持仓三重确认为0, 仓位被外部平掉, 周期被动结束")
            elif slq > tpq:
                self._end(EndReason.SL_FORCED if self.force_attempts > 0 else EndReason.SL,
                          f"止损出场(止盈成交{tpq:.8g}/止损成交{slq:.8g})")
            elif tpq > 0:
                self._end(EndReason.TP, f"止盈出场(成交{tpq:.8g})")
            else:
                self._end(EndReason.MANUAL_FLAT,
                          "仓位已归零但本周期出场单累计成交为0, 确认为被外部平仓")
            return

        # (4) 周期总超时
        if cfg.max_cycle_sec > 0 and now - self.created_ts > cfg.max_cycle_sec:
            self._end(EndReason.TIMEOUT, f"周期超时{cfg.max_cycle_sec}s, 强平收尾")
            return

        # (5) 止损击穿: 有条件单则给宽限等它触发, 无条件单立即收尾强平
        self._check_sl_breach(w)

    def _check_sl_breach(self, w: World):
        if not self._crossed(w.price, self.bp.sl_price):
            self.sl_breach_since = 0.0
            return
        slp = self.bp.sl_price
        if self.sl_breach_since == 0.0:
            self.sl_breach_since = w.ts
            logger.critical(f"[熔断] 现价[{w.price}]已击穿全局止损价[{slp}], "
                            f"等待条件单触发({SL_BREACH_CONFIRM_SEC}s 宽限)...")
        # 存活即算有效保护(含 PENDING 请求中 / CANCEL_PENDING 撤单中): 绝不抢在它前面发市价单,
        # 否则两笔先后成交会把仓位平成反向。真正无单时才立即强平。
        if not self.registry.state_of(self.sl.coid).alive:
            self._end(EndReason.SL_FORCED, f"现价[{w.price}]击穿止损价[{slp}]且盘口无任何存活条件单")
        elif w.ts - self.sl_breach_since >= SL_BREACH_CONFIRM_SEC:
            self._end(EndReason.SL_FORCED,
                      f"击穿止损价已[{w.ts - self.sl_breach_since:.1f}s]条件单仍未成交, 兜底强平")

    def _crossed(self, price: float, line: float) -> bool:
        """现价是否已越过给定价格线(做多向下越过 / 做空向上越过)。含有效性前置校验。"""
        if price <= 0 or line <= 0:
            return False
        return price <= line if self.direction is Direction.LONG else price >= line

    def _suspend_add(self, why: str):
        """停止加仓(只保留止盈止损收尾)。撤单不在这里做, 交由 Align 步骤每轮持续对齐直到终态。"""
        if self.add_suspended:
            return
        self.add_suspended = True
        logger.critical(f"[降级] 停止一切加仓, 仅维护止盈止损收尾 | 原因:[{why}]")
        self.wal_cycle(MartinLedger.A_ALERT, MartinLedger.S_SUSPEND_ADD, why)

    def _end(self, reason: EndReason, why: str = ""):
        if self.end_reason:
            return
        self.end_reason = reason
        self.add_suspended = True
        logger.critical(f"[周期] 判定终结[{reason.value}], 进入收尾(先撤净, 再平残余) | 原因:[{why}]")
        self.wal_cycle(MartinLedger.A_ALERT, reason.value, why)

    # ==========================================================================
    # 第四步 Align: 只产出动作意图, 不发任何网络请求。顺序: 先出场, 再开仓
    # ==========================================================================
    def _slot_state(self, slot) -> OrderState:
        """
        槽位当前状态 = 其活跃指针所指订单的状态。
        指针指向的订单一旦是 NOT_PLACED(确定从未存在/已撤且零成交), 立即释放指针以便换号重发;
        历史订单本体仍留在登记表中(已终态), 不影响任何判定。
        """
        st = self.registry.state_of(slot.coid)
        if st is OrderState.NOT_PLACED and slot.coid:
            slot.coid = ""
        return st

    def _cancel_action(self, t: Optional[TrackedOrder], w: World, why: str,
                       include_pending: bool = False) -> List[Action]:
        """
        按需生成撤单动作:
          LIVE            -> 直接撤;
          CANCEL_PENDING  -> 仍在盘口说明撤单请求未生效, 超确认期后重发;
          PENDING         -> 仅在必须清场时(停止加仓/收尾)撤, 用撤单去逼出一个确定终态。
        【硬过滤】市价兜底强平单一经发出即刻撮合、不可撤销: 绝不为它生成任何撤单动作,
        让它自然在盘口成交, 终态完全交由 Reconcile 点查裁决(否则必被拒单并打乱收尾时序)。
        """
        if t is None or t.is_force_market:
            return []
        if t.state is OrderState.LIVE:
            return [Action.cancel(t.coid, why)]
        if (t.state is OrderState.CANCEL_PENDING and t.coid in w.orders
                and w.ts - t.act_ts >= CANCEL_CONFIRM_SEC):
            return [Action.cancel(t.coid, why + "(撤单未生效, 重发)")]
        if (include_pending and t.state is OrderState.PENDING
                and w.ts - t.act_ts >= ORDER_GRACE_SEC):
            return [Action.cancel(t.coid, why + "(请求结果未知, 撤单以求确定终态)")]
        return []

    def _abandon(self, slot, why: str):
        if slot.abandoned:
            return
        slot.abandoned = True
        is_exit = isinstance(slot, ExitOrder)
        role = slot.role.value if is_exit else OrderRole.OPEN.value
        layer = 0 if is_exit else slot.layer
        logger.critical(f"[放弃] 角色[{role}] 层[{layer}] 连续[{slot.attempts}]次挂单失败, "
                        f"永久停挂并告警 | 原因:[{why}]")
        self.wal_order(MartinLedger.A_ALERT, slot.coid, layer, role,
                       status=MartinLedger.S_GIVEUP, msg=why)

    def _plan_exits(self, w: World) -> List[Action]:
        """出场单对齐: 价格恒取蓝图固定值, 数量取统一夹逼结果。先止损(更要命)后止盈。"""
        qty = self._clamp_exit_qty(w.pos_qty)
        acts = self._plan_exit(self.sl, self.bp.sl_price, qty, w)
        if self.end_reason:
            return acts      # 止损槽位已裁决为收尾(如连续失败转强平), 本轮不再动止盈单
        return acts + self._plan_exit(self.tp, self.current_tp_price(), qty, w)

    def _plan_exit(self, slot: ExitOrder, target: float, qty: float, w: World) -> List[Action]:
        st = self._slot_state(slot)
        t = self.registry.get(slot.coid)
        spec, now = self.ctx.spec, w.ts

        # I3 单一出场的结构性保证: 同角色只允许"当前指针"那一张存活,
        # 任何上一代残留(换号后旧单仍在盘口 / 误判终态又重现)立即撤销, 且撤净前绝不挂新单。
        rivals = [r for r in self.registry.alive(slot.role)
                  if r.layer == 0 and r.coid != slot.coid]
        acts = []
        for r in rivals:
            acts += self._cancel_action(r, w, f"{slot.role.value}上一代残留单(严守单一出场)")

        if st is OrderState.FILLED:
            if spec.qty_is_dust(qty):
                return acts                            # 已成交且无残余: 等 Evaluate 归因终结
            # 旧保护单已成交, 但账本仍有非粉尘残余(同 Tick 又有加仓成交):
            # 必须允许规划下一代保护单, 绝不给残余仓位留下无保护空窗。
            logger.critical(f"[出场] {slot.role.value}上一代已成交但账本仍有残余[{qty:.8g}], "
                            f"立即补挂下一代保护单(绝不留保护空窗)")
            slot.coid, st = "", OrderState.NOT_PLACED
        elif spec.qty_is_dust(qty):
            return acts + self._cancel_action(t, w, f"{slot.role.value}无可平数量, 撤销残留保护单")

        if target <= 0:
            return acts
        if st is OrderState.PENDING:
            return acts                                # 原地锁定: 它可能正是我们要的保护单
        if st is OrderState.CANCEL_PENDING:
            return acts + self._cancel_action(t, w, f"{slot.role.value}撤单对齐")
        if st is OrderState.LIVE:
            if self._exit_aligned(slot, target, qty):
                return acts
            return acts + [Action.cancel(slot.coid, f"{slot.role.value}对齐(价格固定, 数量跟随持仓)")]

        # ---- NOT_PLACED / DEAD: 需要补挂 ----
        slot.coid = ""
        if rivals:
            return acts    # 上一代残留尚未终结 -> 本轮绝不挂新单(防两张出场单同时成交超量平仓)
        if slot.abandoned or now < slot.next_retry_ts:
            return acts
        if slot.attempts >= MAX_PLACE_ATTEMPTS:
            if slot.role is OrderRole.SL:
                # 止损是生命线: 挂不上就绝不让仓位无保护裸奔, 直接市价离场保全资金
                self._end(EndReason.SL_FORCED,
                          f"止损单连续[{slot.attempts}]次挂单失败, 仓位不可裸奔, 转市价强平保全资金")
            else:
                self._abandon(slot, "止盈单连续失败达上限, 停止更新(不构成风险敞口, "
                                    "仓位仍由止损单与本地击穿熔断保护)")
            return acts
        if slot.role is OrderRole.SL and self._crossed(w.price, target):
            return acts    # 现价已在触发价错误一侧, 挂条件单必被拒; 交由击穿逻辑走收尾强平
        kind = "STOP" if slot.role is OrderRole.SL else "LIMIT"
        why = (f"全局固定止损 均价{self.book.avg:.8g} 最大亏损{self.ctx.cfg.max_loss_usdt}"
               if slot.role is OrderRole.SL else
               f"第{self.tp_layer_idx()}层固定止盈 均价{self.book.avg:.8g} "
               f"止盈{self.ctx.cfg.tp_pct}%")
        return acts + [Action.place(kind, slot.role, 0, target, qty, slot, why)]

    def _exit_aligned(self, slot: ExitOrder, target_price: float, target_qty: float) -> bool:
        """
        防抖核心: 比较维度是【在线单的剩余量】而非订单总量。
        止盈部分成交后, 剩余量本就等于新的持仓量, 不应撤单重挂(否则白交手续费)。
        """
        tick = self.ctx.spec.tick_size or 1e-12
        step = self.ctx.spec.step_size or 1e-12
        return (abs(slot.live_price - target_price) <= tick * 0.6 and
                abs(slot.live_remaining - target_qty) <= step * 0.6)

    def _plan_opens(self, w: World) -> List[Action]:
        """
        开仓层对齐 —— 两条互斥的平铺分支, 无任何递归:
          A. 正常铺单: 按蓝图补挂所有 NOT_PLACED 的层;
             触发总量硬闸 -> 丢弃本轮尚未发出的开仓计划, 就地转入分支 B;
             现价已越过全局止损线 -> 一律不再新增任何加仓(空仓场景 Evaluate 已判 NO_FILL)。
          B. 停止加仓: 持续撤销所有存活开仓单, 直到全部拨到终态。
        """
        if not self.add_suspended:
            acts, now, crossed = [], w.ts, self._crossed(w.price, self.bp.sl_price)
            for lp in self.bp.layers:
                if self._slot_state(lp) is not OrderState.NOT_PLACED:
                    continue        # FILLED / DEAD / 在盘 / 锁定中 -> 一律不动
                if lp.abandoned or now < lp.next_retry_ts:
                    continue
                if crossed:
                    logger.info(f"[加仓] 现价[{w.price}]已越过全局止损价[{self.bp.sl_price}], "
                                f"停止一切加仓(无论是否持仓)")
                    break
                if lp.attempts >= MAX_PLACE_ATTEMPTS:
                    self._abandon(lp, "超过最大尝试次数(该层放弃加仓, 周期用现有仓位收尾)")
                    continue
                # I4 总量硬闸: 已成交 + 本层 不得超过蓝图总量
                if self.book.total_open_filled + lp.qty > self.bp.total_qty * OVERFILL_TOLERANCE:
                    self._suspend_add(f"总量硬闸: 第{lp.layer}层将超出蓝图总量")
                    acts = []       # 本轮此前规划的开仓计划全部作废, 转入撤单分支
                    break
                acts.append(Action.place("LIMIT", OrderRole.OPEN, lp.layer, lp.price, lp.qty, lp,
                                         f"第{lp.layer}层加仓(蓝图固定价)"))
            if not self.add_suspended:
                return acts

        acts = []
        for t in self.registry.alive(OrderRole.OPEN):
            acts += self._cancel_action(t, w, "停止加仓: 撤销开仓单", include_pending=True)
        return acts

    def _clamp_exit_qty(self, real: Optional[float]) -> float:
        """
        【I8 统一夹逼入口】任何平仓/强平路径都必须经过这里, 绝不允许绕过。
        出场数量 = min(本地虚拟持仓, 交易所实际持仓), 防 -2022 拒单与超量平仓;
        real 为 None(查询失败) 时退化为本地虚拟账本量 —— 绝不按 0 处理, 并 CRITICAL 告警。
        """
        q = self.book.open_qty
        now = time.time()
        if real is None:
            if q > 0 and now - self._clamp_log_ts > 60:
                self._clamp_log_ts = now
                logger.critical(f"[夹逼] 无法获取交易所实际持仓(结果未知), 本次平仓数量退化为"
                                f"本地虚拟账本量[{q:.8g}](绝不按0处理), 请留意是否存在外部干预")
        elif real >= 0 and real < q - self.ctx.spec.step_size * 0.5:
            if now - self._clamp_log_ts > 60:
                self._clamp_log_ts = now
                logger.critical(f"[夹逼] 本地虚拟持仓[{q:.8g}] 大于交易所实际持仓[{real:.8g}], "
                                f"按实际持仓下调平仓数量(疑被外部平仓/其它策略干扰), 请人工核对")
            q = real
        return self.ctx.spec.round_qty(q, "down")

    def current_tp_price(self) -> float:
        """当前应使用的固定止盈价 = 已完整成交的最深层对应的蓝图止盈价。"""
        if not self.bp.layers:
            return 0.0
        return self.bp.layers[min(self.tp_layer_idx(), len(self.bp.layers) - 1)].tp

    def tp_layer_idx(self) -> int:
        """
        取"已完整成交"的最深层索引。深层仅部分成交时保守沿用上一层的止盈价
        (该价格更远, 绝不会出现低于真实成本的亏损出场)。
        """
        if self.book.total_open_filled <= 0:
            return 0
        acc, idx = 0.0, 0
        tol = max(self.ctx.spec.step_size * 0.5, 1e-12)
        for lp in self.bp.layers:
            acc += lp.qty
            if self.book.total_open_filled + tol >= acc:
                idx = lp.layer
            else:
                break
        return idx

    # ==========================================================================
    # 收尾: 两段式 —— 先撤净并等全终态, 才允许下发市价单(天然满足 I7 幂等)
    # ==========================================================================
    def _closing(self, w: World, actions: List[Action]) -> TickResult:
        spec, now = self.ctx.spec, w.ts
        if self.closing_since == 0.0:
            self.closing_since = now
            logger.info(f"[收尾] 周期[{self.cycle_id}]进入收尾阶段[{self.end_reason.value}]: "
                        f"第一阶段只做全量撤单, 待所有单据确认终态后才处理残余仓位")

        # ---- 第一阶段: 全量撤单(市价强平单天然豁免); Execute 会拦掉一切开仓动作 ----
        for t in self.registry.alive():
            actions += self._cancel_action(t, w, f"周期收尾({self.end_reason.value})",
                                           include_pending=True)
        self._execute(actions, w)

        stuck = self.registry.alive()
        if stuck:
            if self.stuck_since == 0.0:
                self.stuck_since = now
            elif now - self.stuck_since >= CLOSING_STUCK_SEC:
                logger.critical(f"[收尾] 已连续[{now - self.stuck_since:.0f}s]仍有本周期单据未终态, "
                                f"无法证明它们不会成交, 绝不冒反向开仓风险下发市价单! "
                                f"引擎停机等待人工介入 | 未终态:{stuck}")
                self.wal_cycle(MartinLedger.A_ALERT, "CLOSING_STUCK",
                               f"收尾卡死: 未终态{[t.coid for t in stuck]}",
                               qty=self.book.open_qty)
                return TickResult.HALT
            return TickResult.CONTINUE
        self.stuck_since = 0.0

        # ---- 第二阶段: 所有单据已终态 -> 实时核实并平掉残余 ----
        if not spec.qty_is_dust(self.book.open_qty):
            real = self.ctx.position(self.direction.position_side, force=True)
            if real is None:
                self.pos_unknown += 1
                if self.pos_unknown >= POS_PROBE_MAX_UNKNOWN:
                    logger.critical(f"[收尾] 实际持仓连续[{self.pos_unknown}]轮无法确认, 既不能证明"
                                    f"已平净也不敢盲目重发市价单, 停机等待人工介入 | "
                                    f"本地残余:[{self.book.open_qty:.8g}]")
                    self.wal_cycle(MartinLedger.A_ALERT, "POS_PROBE_UNKNOWN",
                                   f"实际持仓连续{self.pos_unknown}轮查询失败, 需人工介入",
                                   qty=self.book.open_qty)
                    return TickResult.HALT
                logger.critical(f"[收尾] 无法获取实际持仓(第[{self.pos_unknown}]次), 暂缓市价平仓, "
                                f"下一轮继续核实(防双重平仓造成反向开仓)")
                return TickResult.CONTINUE
            self.pos_unknown = 0

            if spec.qty_is_dust(real):
                logger.critical(f"[收尾] 本地虚拟残余[{self.book.open_qty:.8g}] 但交易所实际持仓已归零, "
                                f"强制同步本地账本归零(打破收尾死锁)")
                self.wal_cycle(MartinLedger.A_ALERT, MartinLedger.S_FORCE_FLAT_SYNC,
                               "交易所实际持仓为0, 本地强制归零", qty=self.book.open_qty)
                self.book.force_flat()
            else:
                qty = self._clamp_exit_qty(real)
                if not spec.qty_is_dust(qty):
                    if now < self.next_force_retry_ts:
                        return TickResult.CONTINUE      # 强平被拒后的冷却, 防瞬间耗尽次数
                    if self.force_attempts >= FORCE_CLOSE_MAX_ATTEMPTS:
                        logger.critical(f"[收尾] 已连续[{self.force_attempts}]次市价强平仍未归零"
                                        f"(残余[{qty}] 实际[{real}]), 停机等待人工介入")
                        self.wal_cycle(MartinLedger.A_ALERT, "CLOSE_NOT_CONVERGED",
                                       f"市价强平{self.force_attempts}次未收敛, 需人工介入", qty=qty)
                        return TickResult.HALT
                    logger.critical(f"[收尾] 所有单据已终态且实时持仓确认仍有[{real:.8g}], "
                                    f"下发第[{self.force_attempts + 1}]次市价平仓 | 数量:[{qty}]")
                    self._execute([Action.place("MARKET", OrderRole.SL, 99, 0.0, qty, None,
                                                f"收尾残余平仓({self.end_reason.value})")], w)
                    return TickResult.CONTINUE

        if self.book.open_qty > 0:
            logger.info(f"[收尾] 剩余[{self.book.open_qty:.10g}]低于最小交易单位, 按碎屑账面归零")
            self.book.force_flat()
        if w.orders:
            logger.info(f"[收尾] 本轮快照另有[{len(w.orders)}]张本策略挂单记录"
                        f"(已在对账阶段处置), 不阻断本周期清算")
        if w.pos_qty is not None and not spec.qty_is_dust(w.pos_qty):
            logger.info(f"[收尾] 交易所[{self.direction.value}]仍有持仓[{w.pos_qty:.8g}], "
                        f"但本策略账本已平净且无任何存活单据, 判定该持仓不属于本策略(其它策略/手工仓位)")
        return TickResult.DONE

    # ==========================================================================
    # 第五步 Execute: 唯一的网络写入口, 入口带许可检查, 统一处理所有 PlaceResult
    # ==========================================================================
    def _execute(self, actions: List[Action], w: World):
        """
        【许可检查 = 动作泄漏防线】动作清单可能是在本轮更早时刻生成的, 而生成之后周期状态
        可能已经降级(如出场单被 -2022 拒单触发停止加仓、止损失败触发收尾)。因此每条动作
        执行前必须重新核对当前周期意图:
          * 收尾态   : 只放行撤单与市价收尾单, 其余新挂单一律作废;
          * 停止加仓 : 一律作废开仓动作, 保护单照常维护。
        """
        for a in actions:
            if a.kind == "CANCEL":
                self._send_cancel(a.coid, a.why)
                continue
            blocked = ("收尾" if (self.end_reason and a.kind != "MARKET")
                       else "停止加仓" if (self.add_suspended and a.role is OrderRole.OPEN)
                       else "")
            if blocked:
                logger.critical(f"[执行] 周期已进入[{blocked}]态, 丢弃动作 {a.kind} "
                                f"角色[{a.role.value}] 层[{a.layer}] 价[{a.price}] 量[{a.qty}] | "
                                f"原意图:[{a.why}]")
                continue
            self._send_place(a)

    def _send_cancel(self, coid: str, why: str):
        """
        发出撤单请求。返回受理不代表终态(可能刚好成交), 因此一律置 CANCEL_PENDING,
        由下一轮 Reconcile 点查裁决。
        """
        t = self.registry.get(coid)
        p = OidCodec.parse(coid)
        layer = t.layer if t else (p.layer if p else -1)
        role = t.role.value if t else (p.role.value if p else "?")
        self.wal_order(MartinLedger.A_INTENT_CANCEL, coid, layer, role, status="PENDING", msg=why)
        ok = self.ctx.gw.cancel(coid)
        self.wal_order(MartinLedger.A_CANCEL_OK if ok else MartinLedger.A_CANCEL_FAIL,
                       coid, layer, role,
                       status="ACCEPTED_PENDING_CONFIRM" if ok else "FAIL", msg=why)
        if t:
            t.act_ts = time.time()
            if ok and t.state.alive:
                t.state = OrderState.CANCEL_PENDING

    def _send_place(self, a: Action):
        """WAL 先落意图(带确定 OID), 再发请求; 回执统一交给 _after_place 归类。"""
        coid = OidCodec.build(self.ctx.cfg.strategy_id, self.cycle_id, a.role, a.layer)
        t = self.registry.open(coid, a.role, a.layer, a.price, a.qty)
        slot = a.slot
        if slot is not None:
            slot.coid = coid
            slot.attempts += 1
        if a.kind == "MARKET":
            self.force_attempts += 1

        self.wal_order(MartinLedger.A_INTENT_PLACE, coid, a.layer, a.role.value,
                       a.price, a.qty, "PENDING", a.why)
        gw, d, cfg = self.ctx.gw, self.direction, self.ctx.cfg
        if a.kind == "LIMIT":
            side = d.open_side if a.role is OrderRole.OPEN else d.close_side
            res = gw.place_limit(side, a.qty, a.price, coid, d.position_side)
        elif a.kind == "STOP":
            res = gw.place_stop_market(d.close_side, a.qty, a.price, coid,
                                       d.position_side, cfg.sl_working_type)
        else:
            res = gw.place_market(d.close_side, a.qty, coid, d.position_side)
        t.act_ts = time.time()
        self._after_place(a, t, res)

    def _after_place(self, a: Action, t: TrackedOrder, res: PlaceResult):
        slot = a.slot
        tag = f"角色[{a.role.value}] 层[{a.layer}] 价[{a.price}] 量[{a.qty}] CID[{t.coid}]"
        if res.ok:
            t.state = OrderState.LIVE
            t.ex_id = res.ex_id
            if slot is not None and a.role is not OrderRole.OPEN:
                slot.attempts = 0
            self.wal_order(MartinLedger.A_PLACE_OK, t.coid, a.layer, a.role.value,
                           a.price, a.qty, "OK", res.ex_id)
            logger.info(f"[挂单] {tag} | 结果:[OK] 交易所单号:[{res.ex_id}]")
            return

        if res.unknown or res.kind in (ErrKind.UNKNOWN_RESULT, ErrKind.DUPLICATE):
            t.state = OrderState.PENDING
            self.wal_order(MartinLedger.A_PLACE_UNKNOWN, t.coid, a.layer, a.role.value,
                           a.price, a.qty, "UNKNOWN", res.err)
            logger.critical(f"[挂单] {tag} | 结果:[UNKNOWN/{res.kind.value}] 未收到确定回执, "
                            f"原地锁定等待点查裁决, 严禁换号重发 | 回执:[{res.err}]")
            return

        t.state = OrderState.NOT_PLACED
        if slot is not None:
            slot.coid = ""
        self.wal_order(MartinLedger.A_PLACE_FAIL, t.coid, a.layer, a.role.value,
                       a.price, a.qty, res.kind.value, res.err)

        if a.role is OrderRole.OPEN:
            idx = min(len(RETRY_BACKOFF_SEC) - 1, max(0, (slot.attempts if slot else 1) - 1))
            backoff = float(RETRY_BACKOFF_SEC[idx])
        else:
            backoff = EXIT_BACKOFF_SEC

        if res.kind is ErrKind.PRICE_BAND:
            if a.role is OrderRole.OPEN:
                backoff = max(backoff, 30.0)
            logger.info(f"[挂单] {tag} | 结果:[价格带拒单] {backoff}s 后重挂 | 回执:[{res.err}]")
        elif res.kind is ErrKind.TRANSIENT:
            logger.info(f"[挂单] {tag} | 结果:[明确拒单-瞬态/限频] {backoff}s 后重试 | 回执:[{res.err}]")
        elif res.kind is ErrKind.INSUFFICIENT:
            if a.role is OrderRole.OPEN:
                backoff = max(backoff, 30.0)
            logger.critical(f"[挂单] {tag} | 结果:[保证金不足] 已退避{backoff}s, "
                            f"请立即检查账户可用余额! | 回执:[{res.err}]")
        elif res.kind is ErrKind.IMMEDIATE_TRIGGER:
            px = self.last_price
            dev = (abs(a.price / px - 1) * 100) if (px > 0 and a.price > 0) else 999.9
            if px > 0 and dev <= SL_IMM_TRIG_MAX_DEV_PCT and self._crossed(px, a.price):
                logger.critical(f"[挂单] {tag} | 结果:[条件单会立即触发] 本地现价[{px}]双重核验通过"
                                f"(确已击穿, 偏离{dev:.2f}%), 转入收尾市价强平")
                self._end(EndReason.SL_FORCED, "条件单立即触发且本地现价核验确已击穿")
            else:
                logger.critical(f"[挂单] {tag} | 结果:[条件单会立即触发] 但本地现价[{px}]核验不通过"
                                f"(偏离[{dev:.2f}%]), 判定为止损价计算异常, 拒绝强平! "
                                f"已转退避重挂, 请立即人工核查 | 回执:[{res.err}]")
                self.wal_order(MartinLedger.A_ALERT, t.coid, a.layer, a.role.value,
                               a.price, a.qty, "SL_SANITY_FAIL",
                               f"立即触发回执与本地现价{px}不符, 已拒绝强平")
        elif res.kind is ErrKind.REDUCE_REJECT:
            logger.critical(f"[挂单] {tag} | 结果:[平仓数量超持仓] 本策略仓位疑似被外部平掉! | "
                            f"回执:[{res.err}]")
            self._suspend_add("平仓单被拒(仓位被外部改动)")
        else:
            if slot is not None:
                slot.attempts = MAX_PLACE_ATTEMPTS
            logger.critical(f"[挂单] {tag} | 结果:[明确拒单-{res.kind.value}] 该 OID 永久停挂, "
                            f"处置交由下一轮对齐裁决 | 回执:[{res.err}]")

        # 市价强平单没有槽位承载退避, 单独冷却: 防被连续拒单瞬间耗尽次数直接停机
        if a.kind == "MARKET":
            self.next_force_retry_ts = time.time() + FORCE_BACKOFF_SEC
        if slot is not None:
            slot.next_retry_ts = time.time() + backoff

    # ---------------------- 只读辅助 ----------------------
    def layer_stats(self) -> Tuple[int, int, int]:
        filled = live = gave_up = 0
        for lp in self.bp.layers:
            st = self.registry.state_of(lp.coid)
            if st is OrderState.FILLED:
                filled += 1
            elif st.alive:
                live += 1
            if lp.abandoned:
                gave_up += 1
        return filled, live, gave_up

    def start_meta(self) -> dict:
        return {
            "sig_ts": self.signal_ts, "dir": self.direction.value,
            "base": self.bp.base_price, "step_pct": self.ctx.cfg.step_pct,
            "mult": self.ctx.cfg.qty_mult, "tp_pct": self.ctx.cfg.tp_pct,
            "max_loss": self.ctx.cfg.max_loss_usdt, "sl": self.bp.sl_price,
            "layers": self.bp.to_json_layers(),
        }


# ==============================================================================
# 11. 引擎主循环 (全系统唯一写者)
# ==============================================================================
class MartinEngine:
    def __init__(self, cfg: MartinConfig, gw: ExchangeGateway, ledger: MartinLedger):
        self.cfg = cfg
        self.gw = gw
        self.ledger = ledger
        self.ctx: Optional[CycleCtx] = None
        self.spec: Optional[InstrumentSpec] = None
        self.gate = SignalGate(cfg)
        self.state = EngineState.IDLE
        self.cycle: Optional[MartinCycle] = None
        self.last_price = 0.0
        self.err_streak = 0
        self.stop_flag = False
        self._pending_recover: Optional[Tuple[dict, List[dict]]] = None
        self.recover_attempts = 0
        self.cycles_done = 0
        self.pnl_total = 0.0
        self._next_sync_ts = time.time() + TIME_SYNC_SEC

    # ---------------- 第一步 Sense: 世界快照 + 熔断 ----------------
    def _sense(self, position_side: Optional[str] = None) -> Optional[World]:
        """
        现价与挂单快照任一缺失 -> 返回 None, 调用方必须立即安全空转;
        绝不带着残缺世界观进入后续步骤。
        """
        price = self.gw.fetch_last_price()
        if price is None:
            logger.info("[感知] 现价拉取失败, 本 Tick 安全空转(绝不带残缺世界观决策)")
            return None
        orders, algo_ok = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        if orders is None:
            logger.info("[感知] 挂单快照拉取失败, 本 Tick 安全空转")
            return None
        pos = self.ctx.position(position_side) if position_side else None
        self.last_price = price
        return World(time.time(), price, orders, algo_ok, pos)

    def _purge_orders(self, why: str) -> bool:
        """
        撤销一切带本策略前缀的在线挂单(仅用于冷启动/空闲态清场)。
        返回 True 仅代表【已确认盘口干净】: 必须两条通道都可信 —— 普通单快照成功
        且算法条件单通道有效(algo_ok)。条件单接口降级时绝不宣告清场干净。
        """
        orders, algo_ok = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        if orders is None or not algo_ok:
            logger.info(f"[清场] 无法确认盘口干净(快照缺失或算法条件单通道降级), 不宣告清场完成 | "
                        f"原因:[{why}]")
            return False
        if not orders:
            return True
        logger.critical(f"[清场] 发现[{len(orders)}]张本策略残留挂单, 逐一撤销 | 原因:[{why}]")
        for coid in list(orders.keys()):
            self.ledger.append("-", 0, -1, "-", MartinLedger.A_INTENT_CANCEL,
                               coid, 0, 0, "PENDING", why)
            self.gw.cancel(coid)
        return False

    # ---------------- 启动 ----------------
    def boot(self) -> bool:
        self.cfg.validate()
        self.spec = self.gw.load_instrument()
        if self.spec is None:
            logger.critical("[启动] 交易规格获取失败, 拒绝启动(宁可不跑, 不可乱跑)")
            return False
        self.ctx = CycleCtx(self.cfg, self.gw, self.ledger, self.spec)
        logger.info(f"[启动] 交易规格: {self.spec}")

        if self.gw.is_hedge_mode() is False:
            logger.critical("[启动] 账户非【双向持仓 Hedge Mode】! positionSide 会被拒单, 拒绝启动。"
                            "请在币安合约设置中切换为双向持仓")
            return False

        status, meta, rows, watermark = self.ledger.load_state()
        self.gate.set_watermark(watermark)
        logger.info(f"[启动] 账本读取结果:[{status}] 信号去重水位线恢复为 [{watermark}]")

        # ---- 账本结构损坏 / 蓝图无效 -> fail-closed: 绝不猜、绝不清场、绝不接新信号 ----
        if status in (MartinLedger.LOAD_CORRUPT, MartinLedger.LOAD_BLUEPRINT_BAD):
            cid = (meta or {}).get("cycle_id") or "-"
            logger.critical(f"[启动] 账本结构损坏或未收尾周期[{cid}]蓝图无效! 此刻无法判断: 哪个周期还活着 / "
                            f"哪些 OID 属于自己 / 有无持仓 / 哪些是保护性止损单。绝不做任何推测式平账, "
                            f"严禁 purge、严禁接新信号, 账本与现场原样保留, 直接 STOPPED 等待人工处理")
            self.state = EngineState.STOPPED
            return True

        if status == MartinLedger.LOAD_RECOVER:
            self._pending_recover = (meta, rows)
            if self._recover_cycle(meta, rows):
                self._pending_recover = None
                return True
            self.state = EngineState.RECOVER
            logger.critical("[启动] 未收尾周期接管暂时失败(快照/网络异常), 转入 RECOVER 持续重试; "
                            "绝不清场、绝不开新仓, 现有止盈止损单原样保留")
            return True

        # ---- 全新启动 / 账本可读且无未完成周期 -> 可安全清场后 IDLE ----
        if not self._purge_orders("冷启动: 空闲态不应存在任何本策略挂单"):
            logger.critical("[启动] 冷启动清场未能确认盘口干净(有残留单或算法通道降级), "
                            "已进入空闲态; 开仓前会再次强制核实, 核实不通过绝不开新周期")
        self.state = EngineState.IDLE
        logger.info("[启动] 无未完成周期, 进入空闲监听态")
        return True

    # ---------------- WAL 断点续传: 时间序列重放 + 复用主流水线 ----------------
    def _replay_wal(self, cyc: MartinCycle, rows: List[dict]) -> dict:
        """
        严格按 WAL 行顺序(时间序)重建【登记表 / 虚拟账本 / 周期标志】。
        返回 {'end': EndReason|None, 'halt': str, 'suspended': bool, 'force_seen': bool}
        绝不在这里写任何 FILL 行(会污染审计), 只重放到内存。
        """
        end_by_value = {e.value: e for e in EndReason}
        info = {"end": None, "halt": "", "suspended": False, "force_seen": False}

        for r in rows or []:
            action = r.get("action") or ""
            status = str(r.get("status") or "").strip()
            price = float(r.get("price") or 0)
            qty = float(r.get("qty") or 0)

            # ---- 周期开始: 恢复原始起始时间, 否则入场超时/周期超时全算错 ----
            if action == MartinLedger.A_CYCLE_START:
                ts = MartinLedger.parse_ts(r.get("ts"))
                if ts > 0:
                    cyc.created_ts = ts
                continue

            # ---- 告警行: 严格按语义顺次应用周期级标志 ----
            if action == MartinLedger.A_ALERT:
                if status == MartinLedger.S_SUSPEND_ADD:
                    info["suspended"] = True
                elif status == MartinLedger.S_GIVEUP:
                    self._replay_giveup(cyc, r)
                elif status in (MartinLedger.S_EXTERNAL_FLAT, MartinLedger.S_FORCE_FLAT_SYNC):
                    cyc.book.force_flat()          # 库存校正: 记入 void_qty 以吸收滞后回执
                elif status in end_by_value:
                    info["end"] = end_by_value[status]
                elif status in MartinLedger.HALT_STATUS:
                    info["halt"] = MartinLedger.HALT_STATUS[status]
                continue

            # ---- 订单行: 还原每个 OID 的状态与成交 ----
            coid = (r.get("coid") or "").strip()
            if not coid:
                continue
            p = OidCodec.parse(coid)
            if not p or p.cycle_id != cyc.cycle_id:
                continue
            if p.role is OrderRole.SL and p.layer == 99:
                info["force_seen"] = True
            t = cyc.registry.get(coid)
            if t is None:
                t = cyc.registry.open(coid, p.role, p.layer, price, qty,
                                      OrderState.PENDING, ts=0.0)
            if action == MartinLedger.A_PLACE_OK:
                t.state = OrderState.LIVE
            elif action in (MartinLedger.A_INTENT_PLACE, MartinLedger.A_PLACE_UNKNOWN):
                t.state = OrderState.PENDING
            elif action == MartinLedger.A_PLACE_FAIL:
                t.state = OrderState.NOT_PLACED       # 明确拒单 => 该 OID 从未形成订单
            elif action in (MartinLedger.A_INTENT_CANCEL, MartinLedger.A_CANCEL_OK):
                t.state = OrderState.CANCEL_PENDING
            elif action == MartinLedger.A_FILL:
                cyc.replay_fill(t, price, qty)

        # ---- 指针还原: 每个槽位指向该 (角色,层) 的最新一代订单 ----
        for lp in cyc.bp.layers:
            t = cyc.registry.newest(OrderRole.OPEN, lp.layer)
            if t is not None:
                lp.coid = t.coid
                lp.attempts = cyc.registry.count(OrderRole.OPEN, lp.layer)
                lp.abandoned = lp.abandoned or lp.attempts >= MAX_PLACE_ATTEMPTS
        for slot in (cyc.tp, cyc.sl):
            t = cyc.registry.newest(slot.role, 0)
            if t is not None:
                slot.coid = t.coid      # attempts 保持 0: 保护单必须能立即持续对齐
        cyc.force_attempts = cyc.registry.count(OrderRole.SL, 99)
        if cyc.book_error:
            info["halt"] = info["halt"] or MartinLedger.HALT_STATUS["LEDGER_INCONSISTENT"]
        return info

    @staticmethod
    def _replay_giveup(cyc: MartinCycle, r: dict):
        """还原"槽位永久放弃"标志: 避免恢复后重新去挂一个已判定放弃的槽位。"""
        role = str(r.get("role") or "")
        try:
            layer = int(float(r.get("layer") or -1))
        except Exception:
            layer = -1
        if role == OrderRole.TP.value:
            cyc.tp.abandoned = True
        elif role == OrderRole.SL.value:
            cyc.sl.abandoned = True
        elif role == OrderRole.OPEN.value:
            for lp in cyc.bp.layers:
                if lp.layer == layer:
                    lp.abandoned = True

    def _recover_cycle(self, meta: dict, rows: List[dict]) -> bool:
        """断点续传: WAL 时间序重放 -> 复用主流水线的 Sense + Reconcile 向交易所求真相。"""
        try:
            cycle_id = meta["cycle_id"]
            direction = Direction(meta["dir"])
            layers = [LayerPlan(int(x["l"]), float(x["p"]), float(x["q"]),
                                float(x.get("a") or 0), float(x.get("t") or 0))
                      for x in meta["layers"]]
            sl_px = float(meta.get("sl") or 0)
            if sl_px <= 0 and layers:
                # 老账本兼容: 按满仓均价 + 最大亏损重算全局止损价
                tq = sum(l.qty for l in layers)
                tc = sum(l.qty * l.price for l in layers)
                if tq > 0:
                    favg = tc / tq
                    sl_px = self.spec.round_price(
                        favg - direction.sign * self.cfg.max_loss_usdt / tq,
                        "up" if direction is Direction.LONG else "down")
                    logger.critical(f"[恢复] 账本缺少全局止损价, 已按满仓均价重算为[{sl_px}]")
            bp = Blueprint(direction, layers, float(meta.get("base") or 0), sl_px)
            cyc = MartinCycle(self.ctx, cycle_id, int(meta.get("sig_ts") or 0), direction, bp)
            logger.info(f"[恢复] 检测到未收尾周期[{cycle_id}] {direction.value} 层数[{len(layers)}], "
                        f"开始按账本时间序重放并向交易所求证真相...")

            # 1) 严格按行序重放(精确还原崩溃前那一刻的真实均价、盈亏与所有周期标志)
            info = self._replay_wal(cyc, rows)
            self.ledger.append(cycle_id, cyc.signal_ts, -1, "-", MartinLedger.A_RECOVER_REPLAY,
                               "", cyc.book.avg, cyc.book.open_qty, "OK",
                               json.dumps({"open_qty": cyc.book.open_qty, "avg": cyc.book.avg,
                                           "open_filled": cyc.book.total_open_filled,
                                           "close_filled": cyc.book.total_close_filled,
                                           "realized": round(cyc.book.realized, 8),
                                           "orders": len(cyc.registry.all()),
                                           "force_orders": cyc.force_attempts,
                                           "suspended": info["suspended"],
                                           "end": info["end"].value if info["end"] else "",
                                           "halt": info["halt"],
                                           "created_ts": round(cyc.created_ts, 3)},
                                          separators=(",", ":")))

            # 2) 复用主流水线: Sense + Reconcile(全量点查), 与实时对账 100% 同一套代码路径
            w = self._sense(direction.position_side)
            if w is None:
                logger.critical("[恢复] 世界快照拉取失败, 本次放弃接管(稍后重试), "
                                "绝不带着未知状态运行, 也绝不清场")
                return False
            cyc._reconcile(w, probe_budget=None)

            # 3) 账本标志落地: 停止加仓 / 已判定终结 / 历史强平记录 -> 恢复后只能收尾
            if info["suspended"]:
                cyc.add_suspended = True
                logger.critical("[恢复] 账本显示重启前已停止加仓, 继续保持(绝不重新铺加仓单)")
            if info["end"] is not None:
                cyc._end(info["end"], "账本显示重启前已判定周期终结, 恢复后直接进入收尾")
            elif info["force_seen"]:
                cyc._end(EndReason.SL_FORCED,
                         f"账本存在[{cyc.force_attempts}]笔市价强平记录, 恢复后强制进入收尾态")

            self.cycle = cyc
            filled, live, gave_up = cyc.layer_stats()
            logger.info(f"[恢复] 周期[{cycle_id}]接管成功 | 虚拟持仓:[{cyc.book.open_qty:.8g}] "
                        f"均价:[{cyc.book.avg:.8g}] 全局止损:[{bp.sl_price:.8g}] "
                        f"已实现:[{cyc.book.realized:+.4f}U] | 加仓层(成交{filled}/在挂{live}/放弃{gave_up}) "
                        f"TP:[{cyc.registry.state_of(cyc.tp.coid).value}] "
                        f"SL:[{cyc.registry.state_of(cyc.sl.coid).value}] "
                        f"登记表:{ {t.coid: t.state.value for t in cyc.registry.alive()} }")

            halt = info["halt"] or cyc.book_error
            if halt:
                logger.critical(f"[恢复] 账本显示重启前已因[{halt}]停机等待人工介入, 接管后继续保持 "
                                f"STOPPED, 仓位与挂单原样保留, 不做任何自动处置")
                self.state = EngineState.STOPPED
            else:
                self.state = EngineState.ACTIVE
            return True
        except LedgerError:
            raise
        except Exception as e:
            logger.critical(f"[恢复] 周期重建异常, 本次放弃接管(稍后重试), 绝不清场、绝不开新仓 | "
                            f"错误:[{e}]", exc_info=True)
            return False

    # ---------------- 主循环 ----------------
    def run_forever(self):
        logger.info(f"[主循环] 择时马丁引擎启动(全系统唯一写者) | 策略:[{self.cfg.strategy_id}] "
                    f"交易对:[{self.cfg.symbol}] 信号:[{self.cfg.signal_name}]")
        while not self.stop_flag:
            try:
                self._maintain()
                if self.state == EngineState.STOPPED:
                    logger.critical("[主循环] 引擎处于 STOPPED 态, 已停止一切交易, 等待人工介入")
                    time.sleep(60)
                    continue
                if self.state == EngineState.IDLE:
                    self._idle_tick()
                elif self.state == EngineState.RECOVER:
                    self._recover_tick()
                else:
                    self._active_tick()
                self.err_streak = 0
            except LedgerError as e:
                logger.critical(f"[主循环] 账本(WAL)写盘失败, 立即硬停机保留现场, 等待人工处理磁盘 | "
                                f"错误:[{e}]")
                self.state = EngineState.STOPPED
                time.sleep(5)
            except Exception as e:
                self.err_streak += 1
                logger.error(f"[主循环] 第[{self.err_streak}]次连续异常(状态不变, 下一轮重试) | "
                             f"错误:[{e}]", exc_info=True)
                if self.err_streak >= MAX_CONSECUTIVE_ERRORS:
                    if self.state == EngineState.ACTIVE and self.cycle:
                        logger.critical(f"[主循环] 持仓活动态连续异常达[{self.err_streak}]次, "
                                        f"转入保守模式(停止加仓)")
                        self.cycle._suspend_add("主循环连续异常")
                    elif self.state == EngineState.IDLE:
                        nap = random.uniform(*IDLE_ERROR_SLEEP_SEC)
                        logger.critical(f"[主循环] 空仓空闲态连续异常达[{self.err_streak}]次"
                                        f"(疑网络/交易所维护), 长休眠[{nap:.0f}s]后继续监听, 绝不停机")
                        time.sleep(nap)
                    else:
                        logger.critical(f"[主循环] [{self.state.value}]态连续异常达[{self.err_streak}]次, "
                                        f"保持当前状态继续重试")
                    self.err_streak = 0
                time.sleep(3)
        logger.info("[主循环] 收到退出信号, 已停止。注意: 交易所的止盈/止损单被有意保留, "
                    "下次启动会自动断点续传接管")

    def _maintain(self):
        """主循环定时维护: 目前只有校时(并入主线程, 杜绝跨线程改共享客户端)。"""
        if time.time() < self._next_sync_ts:
            return
        self._next_sync_ts = time.time() + TIME_SYNC_SEC
        self.gw.sync_time()

    def _drive(self, cyc: MartinCycle, w: World):
        """把一次流水线结果落到引擎状态(IDLE 首轮铺单与 ACTIVE 轮询共用这一条路径)。"""
        res = cyc.tick(w)
        if res is TickResult.DONE:
            self._finish_cycle(cyc)
        elif res is TickResult.HALT:
            logger.critical(f"[主循环] 周期[{cyc.cycle_id}]出现无法自动裁决的危险局面, "
                            f"引擎转入 STOPPED 保留现场, 停止一切自动交易, 请立即人工介入")
            self.state = EngineState.STOPPED

    # ---------------- IDLE ----------------
    def _idle_tick(self):
        time.sleep(self.cfg.idle_poll_interval_sec)
        sig = self.gate.poll()          # 先查信号: 无信号则一次网络请求都不发
        if sig is None:
            return
        logger.info(f"[信号] 收到有效开仓信号 {sig}")

        w = self._sense()               # 有信号才请求现价与盘口(开仓前必须世界观完整)
        if w is None:
            logger.info("[信号] 世界快照不完整(现价/挂单拉取失败), 本次放弃开仓"
                        "(信号不消费, 下轮再看)")
            time.sleep(IDLE_NO_PRICE_SLEEP_SEC)
            return
        if not w.algo_ok:
            logger.critical("[信号] 算法条件单查询通道本轮降级, 无法证明盘口不存在未触发条件单, "
                            "绝不在此状态下开启新周期(信号不消费, 下轮再看)")
            return
        if w.orders:
            logger.critical(f"[信号] 空闲态却发现[{len(w.orders)}]张本策略残留挂单, 先清场再考虑开仓, "
                            f"本次信号放弃")
            self._purge_orders("空闲态残留清理")
            self.gate.set_watermark(sig.signal_ts)
            return

        bp = BlueprintBuilder.build(self.cfg, self.spec, sig)
        self.gate.set_watermark(sig.signal_ts)     # 蓝图成败与否, 该信号只消费一次
        if bp is None:
            return

        cycle_id = OidCodec.cycle_id_of(sig.signal_ts)
        cyc = MartinCycle(self.ctx, cycle_id, sig.signal_ts, sig.direction, bp)
        # WAL: 先把完整蓝图落账(含 JSON), 再铺单。崩溃后靠这一行 100% 还原蓝图
        self.ledger.append(cycle_id, sig.signal_ts, -1, "-", MartinLedger.A_CYCLE_START,
                           "", bp.base_price, bp.total_qty, "OK",
                           json.dumps(cyc.start_meta(), separators=(",", ":")))
        self.cycle = cyc
        self.state = EngineState.ACTIVE
        logger.info(f"[周期] 开启新周期[{cycle_id}] {sig.direction.value} | 现价:[{w.price}] "
                    f"层数:[{len(bp.layers)}] 总量:[{bp.total_qty:.8g}] "
                    f"最大名义:[{bp.total_notional:.2f}U] 全局止损:[{bp.sl_price:.8g}] | "
                    f"由同一条流水线立即全量铺单")
        self._drive(cyc, w)             # 复用主流水线完成首轮铺单, 绝不另写一套铺单代码

    # ---------------- RECOVER ----------------
    def _recover_tick(self):
        time.sleep(RECOVER_RETRY_SEC)
        if not self._pending_recover:
            self.state = EngineState.IDLE
            return
        meta, rows = self._pending_recover
        self.recover_attempts += 1
        if self._recover_cycle(meta, rows):
            self._pending_recover = None
            self.recover_attempts = 0
            return
        logger.critical(f"[恢复] 第[{self.recover_attempts}]次接管仍未成功, 保持 RECOVER 继续重试")
        if self.recover_attempts >= RECOVER_MAX_ATTEMPTS:
            logger.critical(f"[恢复] 连续[{self.recover_attempts}]次接管失败, 转入 STOPPED 等人工介入 | "
                            f"仓位与保护性挂单原样保留, 未做任何清理")
            self.state = EngineState.STOPPED

    # ---------------- ACTIVE (含收尾, 由 cycle.end_reason 驱动, 无第二状态机) ----------------
    def _active_tick(self):
        time.sleep(self.cfg.poll_interval_sec)
        cyc = self.cycle
        if cyc is None:
            self.state = EngineState.IDLE
            return
        w = self._sense(cyc.direction.position_side)
        if w is None:
            return                      # 熔断: 世界观残缺, 本 Tick 安全空转
        self._drive(cyc, w)

    def _finish_cycle(self, cyc: MartinCycle):
        reason = cyc.end_reason or EndReason.MANUAL_FLAT
        filled, live, gave_up = cyc.layer_stats()
        snap = cyc.book.snapshot()
        snap.update({"reason": reason.value, "layers_filled": filled,
                     "layers_total": len(cyc.bp.layers), "layers_giveup": gave_up,
                     "force_closes": cyc.force_attempts,
                     "duration_sec": round(time.time() - cyc.created_ts, 1)})
        self.ledger.append(cyc.cycle_id, cyc.signal_ts, -1, "-", MartinLedger.A_CYCLE_END,
                           "", 0, 0, reason.value, json.dumps(snap, separators=(",", ":")))
        self.cycles_done += 1
        self.pnl_total += cyc.book.realized
        logger.info(f"[周期] 周期[{cyc.cycle_id}]清算完成 | 原因:[{reason.value}] "
                    f"成交层数:[{filled}/{len(cyc.bp.layers)}] "
                    f"入场均价:[{snap['entry_avg']:.8g}] "
                    f"本周期盈亏:[{cyc.book.realized:+.4f}U] 累计:[{self.pnl_total:+.4f}U] "
                    f"耗时:[{snap['duration_sec']}s]")
        self.cycle = None
        self.state = EngineState.IDLE
        logger.info("[周期] 已回到空闲态, 立即恢复接收新信号(无冷却)")


# ==============================================================================
# 12. 只读看板线程 —— 绝不参与任何决策, 绝不修改任何状态
# ==============================================================================
class DashboardThread(threading.Thread):
    def __init__(self, engine: MartinEngine, interval_sec=120):
        super().__init__(daemon=True)
        self.eng = engine
        self.interval = interval_sec
        self.t0 = time.time()

    def run(self):
        logger.info(f"[看板] 状态看板线程启动 | 周期:[{self.interval}s]")
        while True:
            time.sleep(self.interval)
            try:
                self._report()
            except Exception as e:
                logger.info(f"[看板] 聚合异常(不影响交易) | 错误:[{e}]")

    def _report(self):
        e = self.eng
        up = int(time.time() - self.t0)
        lines = [f"\n========== [择时马丁看板] {e.cfg.strategy_id} | {e.cfg.symbol} ==========",
                 f" 🧭 状态:[{e.state.value}] 现价:[{e.last_price}] 运行:{up // 3600}h{up % 3600 // 60}m",
                 f" 📈 已完成周期:[{e.cycles_done}] 累计已实现:[{e.pnl_total:+.4f}U] "
                 f"信号水位线:[{e.gate.watermark_ts}]"]
        c = e.cycle
        if c is None:
            lines.append(" 💤 当前无进行中周期, 空闲监听信号中")
        else:
            filled, live, gave_up = c.layer_stats()
            slp, tpp = c.bp.sl_price, c.current_tp_price()
            alive = c.registry.alive()
            lines += [
                f" 🔁 周期:[{c.cycle_id}] 方向:[{c.direction.value}] "
                f"加仓层:[成交{filled}/在挂{live}/放弃{gave_up}/共{len(c.bp.layers)}]"
                + ("  ⚠️已停止加仓" if c.add_suspended else "")
                + (f"  🧹收尾中[{c.end_reason.value}]" if c.end_reason else "")
                + (f"  ⛔账本异常[{c.book_error}]" if c.book_error else ""),
                f" 💰 虚拟持仓:[{c.book.open_qty:.8g}] 均价:[{c.book.avg:.8g}] "
                f"浮亏盈:[{c.book.unrealized(e.last_price):+.4f}U] 已实现:[{c.book.realized:+.4f}U]",
                f" 🎯 固定止盈价:[{tpp:.8g}](第{c.tp_layer_idx()}层/{e.cfg.tp_pct}%) "
                f"🛑 全局固定止损价:[{slp:.8g}](最大亏损{e.cfg.max_loss_usdt}U) | "
                f"TP:[{c.registry.state_of(c.tp.coid).value}] "
                f"SL:[{c.registry.state_of(c.sl.coid).value}]",
                f" 📒 登记表: 总[{len(c.registry.all())}] 存活[{len(alive)}]"
                + (f" {[f'{t.coid[-8:]}:{t.state.value}' for t in alive]}" if alive else "")
                + (f" 强平次数:[{c.force_attempts}]" if c.force_attempts else ""),
            ]
            if e.last_price > 0 and slp > 0 and tpp > 0:
                d = abs(e.last_price / slp - 1) * 100
                lines.append(f" 📏 现价距止损:[{d:.3f}%] 距止盈:[{abs(e.last_price / tpp - 1) * 100:.3f}%]")
        lines.append("=========================================================\n")
        logger.info("\n".join(lines))


# ==============================================================================
# 13. 进程编排
# ==============================================================================
def run_single_strategy(cfg: MartinConfig):
    """子进程入口: 独立日志 -> 单实例锁 -> 父进程自杀看门狗 -> 组装 -> 冷启动 -> 主循环。"""
    safe_symbol = cfg.symbol.replace("/", "_").replace(":", "_")
    setup_logger(app_name=f"MT_{cfg.strategy_id}_{safe_symbol}", force_reset=True)
    logging.getLogger().info(f"[进程] 子进程日志就绪 | 策略:[{cfg.strategy_id}] "
                             f"交易对:[{cfg.symbol}] 信号:[{cfg.signal_name}]")

    # 单实例锁(固定绝对目录): 同一 strategy_id 绝不允许两个进程同时跑, 也不允许换目录绕过
    lock_path = data_path(f"martin_{cfg.strategy_id}.lock")
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        if platform.system().lower() != "windows":
            import fcntl
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(lock_fd, str(os.getpid()).encode())
    except Exception as e:
        logger.critical(f"[进程] 获取单实例锁失败, 疑有同名策略正在运行, 拒绝启动 | "
                        f"策略:[{cfg.strategy_id}] 锁:[{lock_path}] 错误:[{e}]")
        return

    def _parent_watchdog():
        while True:
            if os.getppid() in (1, 0):
                os._exit(0)      # 主进程暴毙 -> 物理自杀, 杜绝孤儿进程裸奔下单
            time.sleep(2)
    threading.Thread(target=_parent_watchdog, daemon=True).start()

    api_key = get_config("myself_biance_api_key")
    secret_key = get_config("myself_biance_api_secret")
    proxies = None if platform.system().lower() == "linux" else {
        "http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
    exchange = safe_init_exchange(api_key, secret_key, proxies)

    gw = BinanceGateway(exchange, cfg.symbol)          # ← 换 OKX 只需替换这一行
    ledger = MartinLedger(cfg.strategy_id)
    engine = MartinEngine(cfg, gw, ledger)

    def _on_term(signum, frame):
        logger.critical(f"[进程] 收到信号[{signum}], 优雅退出(不平仓, 保留交易所止盈止损单)")
        engine.stop_flag = True
    for s in (sysignal.SIGTERM, sysignal.SIGINT):
        try:
            sysignal.signal(s, _on_term)
        except Exception:
            pass

    if not engine.boot():
        logger.critical("[进程] 冷启动检查未通过, 进程退出")
        return
    DashboardThread(engine, interval_sec=120).start()
    engine.run_forever()


def main_app():
    configs = [
        # ── 示例: BTC 用 signal_1, 2% 间距 / 2 倍加仓 / 0.8% 止盈 / 最大亏损 50U ──
        # 层数不再配置, 由 max_loss_usdt × layer_loss_budget_ratio 自动推导
        MartinConfig(
            strategy_id="B1", symbol="BTC/USDT:USDT", signal_name="get_signal_1",
            first_qty=0.002, step_pct=2.0, qty_mult=2.0, tp_pct=0.8,
            max_loss_usdt=50, layer_loss_budget_ratio=0.8,
        ),
        # ── 同一个币, 不同信号 + 不同马丁参数, 通过 strategy_id 完全隔离, 互不干扰 ──
        MartinConfig(
            strategy_id="B2", symbol="BTC/USDT:USDT", signal_name="get_signal_2",
            first_notional=20, step_pct=1.2, qty_mult=1.8, tp_pct=0.5,
            max_loss_usdt=30,
        ),
        # ── 另一个币 ──
        MartinConfig(
            strategy_id="U1", symbol="UNI/USDT:USDT", signal_name="get_signal_3",
            first_qty=1, step_pct=2.5, qty_mult=2.0, tp_pct=1.0,
            max_loss_usdt=40,
        ),
    ]

    # 启动前防呆: strategy_id 必须全局唯一
    ids = [c.strategy_id for c in configs]
    if len(set(ids)) != len(ids):
        logger.critical(f"[系统] strategy_id 存在重复{ids}, 会导致账本与 OID 命名空间冲突, 拒绝启动")
        return

    procs = []
    for c in configs:
        p = multiprocessing.Process(target=run_single_strategy, args=(c,))
        p.daemon = True
        p.start()
        procs.append(p)
        logger.info(f"[系统] 已拉起策略进程 | 策略:[{c.strategy_id}] 交易对:[{c.symbol}] "
                    f"信号:[{c.signal_name}] PID:[{p.pid}]")
    logger.info(f"[系统] 全部进程启动完毕, 主进程进入守护模式 | 进程数:[{len(procs)}] "
                f"数据目录:[{DATA_DIR}]")
    try:
        for p in procs:
            p.join()
    except (KeyboardInterrupt, SystemExit):
        pass


# ==============================================================================
# 14. 运维工具 (人工排障用, 与主流程解耦)
# ==============================================================================
def admin_inspect(exchange, symbol, strategy_id=None):
    """排查盘口: 按策略前缀归类, 检出重复层单与非本系统孤儿单。"""
    orders = exchange.fetch_open_orders(symbol)
    from collections import defaultdict
    by_key, others = defaultdict(list), []
    for o in orders or []:
        cid = o.get("clientOrderId") or ""
        p = OidCodec.parse(cid)
        if p and (strategy_id is None or p.strategy_id == strategy_id):
            by_key[(p.strategy_id, p.cycle_id, p.role.value, p.layer)].append(o)
        else:
            others.append(o)
    logger.info(f"\n===== [马丁挂单诊断 {symbol}] 总计{len(orders or [])}张 =====")
    dup = False
    for k, v in sorted(by_key.items()):
        flag = "⚠️重复" if len(v) > 1 else "  "
        dup = dup or len(v) > 1
        logger.info(f" {flag} 策略[{k[0]}] 周期[{k[1]}] 角色[{k[2]}] 层[{k[3]}] x{len(v)}张 "
                    f"价:{[o.get('price') for o in v]}")
    if not dup:
        logger.info(" ✅ 未发现同一(周期,角色,层)重复挂单")
    if others:
        logger.info(f" ℹ️ 非本系统挂单 {len(others)} 张(不干预)")
    logger.info("==================================================\n")


def admin_cancel_strategy(exchange, symbol, strategy_id):
    """紧急清场: 只撤指定 strategy_id 的挂单, 绝不误伤其它策略与手工单。"""
    prefix = OidCodec.strategy_prefix(strategy_id)
    n = 0
    for o in exchange.fetch_open_orders(symbol) or []:
        cid = o.get("clientOrderId") or ""
        if cid.startswith(prefix):
            try:
                exchange.cancel_order(o.get("id"), symbol)
                n += 1
                logger.info(f"[紧急清场] 已撤 CID:[{cid}]")
            except Exception as e:
                logger.error(f"[紧急清场] 撤单失败 CID:[{cid}] 错误:[{e}]")
    logger.info(f"[紧急清场] 策略[{strategy_id}] 共撤销 {n} 张挂单")


if __name__ == "__main__":
    main_app()