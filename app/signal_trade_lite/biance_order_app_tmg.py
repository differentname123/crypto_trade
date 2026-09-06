# -*- coding: utf-8 -*-
"""
================================================================================
择时马丁交易引擎 (信号驱动 + 单写者串行状态机 + WAL账本 + 幂等成交记账)
================================================================================
[功能摘要]
  为每个 MartinConfig 拉起独立子进程。空闲态轮询外部择时信号(get_signal_x);
  一旦拿到有效开仓信号, 依据【加仓间距/加仓倍数/最大亏损金额】推算出完整马丁蓝图
  (含最大层数), 按完整蓝图逐层铺单，并在层间对账和建立保护; 随后进入串行轮询维护:
  任何一层成交即重算均价 -> 撤旧挂新 -> 更新止盈限价单与止损条件单;
  止盈成交 / 止损成交 / 兜底强平 / 入场超时 任一发生 -> 清算收尾 -> 回到空闲态。

[核心不变量 (代码中反复校验)]
  I1 记账唯一来源: 虚拟持仓 = Σ(本策略 client_oid 的成交增量)。永不用交易所仓位算均价,
     因为双向持仓模式下同 symbol 的仓位是全账户共享的, 必被其它策略/手工单污染。
  I2 幂等入账     : acked[coid] 记录"已入账成交量", 增量入账, 重复观测天然 no-op。
  I3 单一出场     : 同一时刻止盈 1 张、止损 1 张, 正常对齐时数量取虚拟持仓与可用真实仓位的较小值,
     绝不使用 closePosition(会连别人的仓位一起平掉)。
  I4 总量封顶     : Σ开仓成交 <= 蓝图总量 * 容差, 越界立即停止加仓, 只留止盈止损收尾。

[输入]
  1. 静态配置 MartinConfig(策略ID/交易对/信号函数名/间距/倍数/止盈/最大亏损...)
  2. 外部信号 get_signal_x(symbol) -> DataFrame(是否开仓 / 方向 / 限价 / 信号毫秒时间戳)
  3. 交易所实时: 现价、精度过滤器、在线挂单快照、单笔订单点查
  4. 本地账本 martin_ledger_{策略ID}.csv (WAL, 冷启动断点续传的唯一索引)

[输出]
  1. 交易所侧: 阶梯限价开仓单 + 唯一止盈限价单 + 唯一止损条件单
  2. 本地侧  : 追加式 CSV 领域事件账本 + 按进程隔离的日志文件
  3. 常驻进程, 无返回值

[并发安全]
  交易与账本状态只有主线程修改(单一写者)。看板线程只读；校时与交易API在主线程串行；看门狗仅请求退出。

[前置条件]
  1. 合约账户必须为【双向持仓 / Hedge Mode】, 否则 positionSide 会被拒单;
  2. Hedge Mode 下禁止传 reduceOnly(会被拒), 平仓靠 side + positionSide 定向;
  3. 每个 strategy_id 必须全局唯一(它同时是账本名与 OID 命名空间), 严禁复用。
================================================================================
"""
import os
import csv
import json
import time
import math
import secrets
import re
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

from biance_order_lite import safe_init_exchange

# ------------------------------------------------------------------------------
# 外部信号源: 名称 -> 函数。子进程按配置里的字符串名解析, 保证跨进程可 pickle。
# 约定: get_signal_x(symbol) -> pd.DataFrame, 永不抛异常, 至少返回空 df。
# ------------------------------------------------------------------------------
try:
    from signal_lib import get_signal_1, get_signal_2, get_signal_3  # noqa
except Exception as signal_import_error:  # 保留可导入性，但不允许静默启用空信号
    SIGNAL_IMPORT_ERROR = str(signal_import_error)
    def get_signal_1(symbol): return pd.DataFrame()
    def get_signal_2(symbol): return pd.DataFrame()
    def get_signal_3(symbol): return pd.DataFrame()

SIGNAL_IMPORT_ERROR = globals().get("SIGNAL_IMPORT_ERROR", "")

SIGNAL_REGISTRY = {
    "get_signal_1": get_signal_1,
    "get_signal_2": get_signal_2,
    "get_signal_3": get_signal_3,
}


# ==============================================================================
# 0. 全局可调参数 (集中管理, 消灭魔术数字)
# ==============================================================================
API_THROTTLE_SEC = 0.08          # 相邻两次 API 调用的最小间隔(限流保护)
MAX_PLACE_ATTEMPTS = 5           # 单层挂单的最大尝试次数, 超出则永久 DEFERRED + 告警(防疯狂发单)
RETRY_BACKOFF_SEC = (2, 5, 15, 60, 300)   # 各次失败后的退避秒数(按尝试次数索引)
QTY_EPS_RATIO = 1e-9             # 浮点比较用的极小量
OVERFILL_TOLERANCE = 1.02        # I4: 累计开仓成交 / 蓝图总量 的容忍上限
SL_BREACH_CONFIRM_SEC = 5.0      # 现价击穿止损价后, 等条件单自己触发的宽限时间, 超时则主动强平
MAX_CONSECUTIVE_ERRORS = 20      # 主循环连续异常次数上限, 超出转入 SUSPEND_ADD 保守收尾
DEFER_PLACE_WINDOW_PCT = 8.0     # 被价格带拒单的层, 待现价进入该百分比窗口内再补挂


# ==============================================================================
# 1. 枚举与值对象
# ==============================================================================
class LedgerError(RuntimeError):
    """账本不可用：停止交易写操作，保留现场。"""


class ReconcileError(RuntimeError):
    """归属或成交证据矛盾：禁止猜测持仓。"""


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
    SL = "S"     # 止损(含兜底强平)


class Life(Enum):
    """本地跟踪的订单生命周期(与交易所状态解耦, 便于状态机推演)。"""
    NOT_PLACED = "NOT_PLACED"    # 尚未挂出 / 需要补挂
    INTENT = "INTENT"            # 已写 WAL 意图, 未收到回执
    UNKNOWN = "UNKNOWN"          # 请求结果未知(超时/重复OID), 必须点查裁决, 严禁换号重发
    LIVE = "LIVE"                # 在盘口挂着
    FILLED = "FILLED"            # 完全成交
    DEAD = "DEAD"                # 已撤销/拒单且不再补挂(终态)
    DEFERRED = "DEFERRED"        # 因价格带/资金/次数上限暂缓, 条件满足后重试


class EngineState(Enum):
    IDLE = "IDLE"                # 空闲监听信号
    ACTIVE = "ACTIVE"            # 周期维护中
    SUSPEND_ADD = "SUSPEND_ADD"  # 降级: 停止加仓, 只维护止盈止损收尾
    TEARDOWN = "TEARDOWN"        # 清理归位
    STOPPED = "STOPPED"          # 终止: 不再接新信号, 需人工介入


class EndReason(Enum):
    TP = "END_TP"                     # 止盈成交
    SL = "END_SL"                     # 止损条件单成交
    SL_FORCED = "END_SL_FORCED"       # 兜底强平(条件单未触发)
    NO_FILL = "END_NO_FILL"           # 入场超时, 一手未成
    TIMEOUT = "END_TIMEOUT"           # 周期超时强平
    MANUAL_FLAT = "END_MANUAL_FLAT"   # 仓位被外部平掉, 周期被动结束


class ErrKind(Enum):
    NONE = "NONE"
    TRANSIENT = "TRANSIENT"          # 限频/超时/时间戳 -> 退避重试
    PRICE_BAND = "PRICE_BAND"        # 价格离盘口太远 -> 延后补挂
    IMMEDIATE_TRIGGER = "IMM_TRIG"   # 条件单会立即触发 -> 直接市价平
    INSUFFICIENT = "INSUFFICIENT"    # 保证金/余额不足 -> 退避 + 告警
    REDUCE_REJECT = "REDUCE_REJECT"  # 平仓数量超过持仓 -> 仓位被外部动过
    DUPLICATE = "DUPLICATE"          # OID 重复 -> 单子已存在, 转 UNKNOWN 点查
    INVALID = "INVALID"              # 精度/最小量等参数非法 -> 不可重试
    FATAL = "FATAL"                  # 未知错误 -> 保守挂起


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
    def is_open(self) -> bool:
        return self.status == "OPEN"

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
    __slots__ = ("strategy_id", "cycle_id", "role", "layer", "ts")

    def __init__(self, strategy_id, cycle_id, role, layer, ts):
        self.strategy_id = strategy_id
        self.cycle_id = cycle_id
        self.role = role
        self.layer = layer
        self.ts = ts


class OidCodec:
    """
    格式: M_{S_ID}_{C_ID}_{ROLE}{L_ID}_{TS}
      M     系统前缀(马丁)
      S_ID  策略短标识, 建议 <=6 位纯字母数字(启动时强校验)
      C_ID  周期流水号 = 完整 base36(信号毫秒时间戳)
      ROLE  O=开仓/加仓  T=止盈  S=止损
      L_ID  层级 00~99 (T/S 用 00; 兜底强平用 98/99)
      TS    10位随机十六进制；保留旧格式解析能力
    新单最长36字符；历史短周期号与旧后缀仍可解析，仅支持下述 Binance 网关
    """
    PREFIX = "M"

    @staticmethod
    def cycle_id_of(signal_ts: int) -> str:
        return _to_b36(int(signal_ts))  # 完整时间戳，避免原后6位每约25天循环

    @classmethod
    def build(cls, strategy_id: str, cycle_id: str, role: OrderRole, layer: int) -> str:
        suffix = secrets.token_hex(5).upper()
        oid = f"{cls.PREFIX}_{strategy_id}_{cycle_id}_{role.value}{layer:02d}_{suffix}"
        if len(oid) > 36:
            raise ValueError("client OID 超过 Binance 36 字符限制")
        return oid

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
            return ParsedOid(strategy_id, cycle_id, role, layer, parts[-1])
        except Exception:
            return None

    @classmethod
    def strategy_prefix(cls, strategy_id: str) -> str:
        return f"{cls.PREFIX}_{strategy_id}_"

    @classmethod
        # 便于按周期批量识别
    def cycle_prefix(cls, strategy_id: str, cycle_id: str) -> str:
        return f"{cls.PREFIX}_{strategy_id}_{cycle_id}_"


# ==============================================================================
# 3. 交易规格与精度 (最小下单量 / 最小名义价值 / 价格刻度)
# ==============================================================================
def _dec(x) -> Decimal:
    return Decimal(str(x))


def quantize(value: float, step: float, mode: str = "down") -> float:
    """按 step 对 value 做定向修约。用 Decimal(str()) 规避二进制浮点误差。"""
    if step is None or step <= 0:
        return float(value)
    v, s = _dec(value), _dec(step)
    n = v / s
    if mode == "down":
        n = n.to_integral_value(rounding=ROUND_FLOOR)
    elif mode == "up":
        n = n.to_integral_value(rounding=ROUND_CEILING)
    else:
        n = n.to_integral_value(rounding=ROUND_HALF_UP)
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
        self.market_step_size = self.step_size
        self.market_min_qty = self.min_qty
        self.market_max_qty = self.max_qty

    # ---------- 价格 ----------
    def round_price(self, price: float, mode: str = "half") -> float:
        return quantize(price, self.tick_size, mode)

    # ---------- 数量 ----------
    def round_qty(self, qty: float, mode: str = "down") -> float:
        if self.step_size > 0 and math.isfinite(qty):
            nearest = quantize(qty, self.step_size, "half")
            if abs(qty - nearest) <= self.step_size * 1e-8:
                qty = nearest  # 仅消除 .3-.2 这类运算误差，防止整步数量被多截一档
        return quantize(qty, self.step_size, mode)

    def normalize_open_qty(self, qty: float, price: float) -> float:
        """
        开仓量修约: 先向下截断(保守), 再兜底抬到 minQty 与 minNotional 之上。
        返回 0 表示无法构造合法数量(调用方应丢弃该层/该信号)。
        """
        if (not math.isfinite(price) or not math.isfinite(qty)
                or price <= 0 or qty <= 0 or self.step_size <= 0):
            return 0.0
        q = self.round_qty(qty, "down")
        need_by_min_qty = self.round_qty(self.min_qty, "up") if self.min_qty > 0 else 0.0
        need_by_notional = 0.0
        if self.min_notional > 0:
            need_by_notional = self.round_qty(
                self.min_notional / (price * self.contract_size), "up")
            # 向上修约后仍可能因浮点差一点点, 再补一个 step
            if _dec(need_by_notional) * _dec(price) * _dec(self.contract_size) < _dec(self.min_notional):
                need_by_notional = round(need_by_notional + self.step_size, 12)
        q = max(q, need_by_min_qty, need_by_notional)
        if q > self.max_qty:
            return 0.0
        return q

    def notional(self, price: float, qty: float) -> float:
        return price * qty * self.contract_size

    def qty_is_dust(self, qty: float) -> bool:
        """低于最小交易单位；仅代表不可下单，不代表真实仓位为零。"""
        floor_ = max(self.min_qty, self.step_size)
        return qty < floor_ * (1 - 1e-9)

    def __repr__(self):
        return (f"Spec({self.symbol} tick={self.tick_size} step={self.step_size} "
                f"minQty={self.min_qty} minNotional={self.min_notional})")


# ==============================================================================
# 4. 交易所网关 (抽象 + Binance 实现)  —— 唯一与平台耦合的一层
# ==============================================================================
def classify_error(msg: str) -> ErrKind:
    """只分类；是否明确拒单由网关另外判断，超时不能据此重发。"""
    m = str(msg).lower()
    code_match = re.search(r'["\']code["\']\s*:\s*(-\d+)', m)
    code = int(code_match.group(1)) if code_match else None
    if code in (-1003, -1008, -1021, -1001) or any(k in m for k in
            ("429", "throttl", "timeout", "timed out", "service unavailable", "503")):
        return ErrKind.TRANSIENT
    if code in (-4111, -4116) or "duplicate" in m or "already exist" in m:
        return ErrKind.DUPLICATE
    if code == -2021 or "immediately trigger" in m:
        return ErrKind.IMMEDIATE_TRIGGER
    if code == -2022 or "reduceonly" in m or "reduce only" in m:
        return ErrKind.REDUCE_REJECT
    if code in (-2018, -2019) or "insufficient" in m:
        return ErrKind.INSUFFICIENT
    if code in (-4016, -4024, -4131) or "percent_price" in m:
        return ErrKind.PRICE_BAND
    if code in (-1013, -1111, -4003, -4005, -4013, -4014, -4015, -4164) or any(
            k in m for k in ("precision", "min_notional", "lot_size", "price_filter")):
        return ErrKind.INVALID
    return ErrKind.FATAL


class ExchangeGateway(ABC):
    """
    交易所网关抽象: 所有网络与平台差异全部收拢在这里。
    上层(状态机/引擎)只依赖本接口, 接入 OKX / 模拟盘 / 回测只需实现一个子类。
    约定: 本层永不抛异常给上层 —— 查询失败返回 None, 挂单失败返回带分类的 PlaceResult。
    """

    def __init__(self, symbol: str):
        self.symbol = symbol
        self._last_call_ts = 0.0
        self.snapshot_complete = False

    def _throttle(self):
        gap = time.monotonic() - self._last_call_ts
        if gap < API_THROTTLE_SEC:
            time.sleep(API_THROTTLE_SEC - gap)
        self._last_call_ts = time.monotonic()

    @abstractmethod
    def load_instrument(self) -> Optional[InstrumentSpec]: ...
    @abstractmethod
    def fetch_last_price(self) -> Optional[float]: ...
    @abstractmethod
    def fetch_open_orders(self, coid_prefix: str) -> Optional[Dict[str, UniOrder]]: ...
    @abstractmethod
    def fetch_order(self, coid: str) -> Optional[UniOrder]: ...
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

    def fetch_risk_price(self, working_type: str) -> Optional[float]:
        return self.fetch_last_price() if working_type == "CONTRACT_PRICE" else None



class BinanceGateway(ExchangeGateway):
    """Binance USDT 线性合约；普通单和算法单使用明确的原生端点。"""

    _STATUS_MAP = {
        "NEW": "OPEN", "PARTIALLY_FILLED": "OPEN", "PENDING_CANCEL": "OPEN",
        "FILLED": "FILLED", "CANCELED": "CANCELED", "CANCELLED": "CANCELED",
        "EXPIRED": "CANCELED", "EXPIRED_IN_MATCH": "CANCELED", "REJECTED": "REJECTED",
        "OPEN": "OPEN", "CLOSED": "FILLED",
    }

    def __init__(self, exchange, symbol):
        super().__init__(symbol)
        self.ex = exchange
        self.ex.options["maxRetriesOnFailure"] = 0  # 写请求重试归状态机所有
        self.ex.timeout = min(float(getattr(self.ex, "timeout", 10000) or 10000), 10000)

    @property
    def market_id(self):
        return self.ex.market(self.symbol)["id"]

    @staticmethod
    def _is_algo(coid):
        p = OidCodec.parse(coid)
        return bool(p and p.role is OrderRole.SL and p.layer == 0)

    # ---------- 转换 ----------
    def _to_uni(self, o: dict) -> UniOrder:
        info = o.get("info") or o
        status = self._STATUS_MAP.get(str(info.get("status") or o.get("status") or "").upper(), "UNKNOWN")
        filled = float(o.get("filled") or info.get("executedQty") or 0)
        amount = float(o.get("amount") or info.get("origQty") or 0)
        avg = float(o.get("average") or info.get("avgPrice") or 0)
        cum_quote = float(info.get("cumQuote") or o.get("cost") or 0)
        if filled > 0 and cum_quote > 0:
            avg = cum_quote / filled
        if status == "FILLED" and (amount <= 0 or filled < amount * (1 - 1e-9)):
            status = "UNKNOWN"  # 字段矛盾不伪装成已撤单
        return UniOrder(
            coid=o.get("clientOrderId") or info.get("clientOrderId") or "",
            ex_id=str(o.get("id") or info.get("orderId") or ""), status=status,
            price=float(o.get("price") or info.get("price") or 0),
            stop_price=float(info.get("stopPrice") or 0), amount=amount, filled=filled,
            avg_price=avg, side=str(o.get("side") or info.get("side") or "").lower(),
            ts=int(info.get("updateTime") or o.get("lastTradeTimestamp") or o.get("timestamp") or 0),
            raw=info)


    def _to_algo(self, a: dict) -> UniOrder:
        coid = a.get("clientAlgoId") or ""
        actual_id = str(a.get("actualOrderId") or "")
        if actual_id and actual_id != "0":
            self._throttle()
            child = self.ex.fapiPrivateGetOrder({"symbol": self.market_id, "orderId": actual_id})
            u = self._to_uni(child)
            u.coid = coid  # 子订单只通过父算法OID记一次账
            u.stop_price = float(a.get("triggerPrice") or 0)
            u.raw = dict(u.raw, parentAlgoId=a.get("algoId"), actualOrderId=actual_id)
            return u
        st = str(a.get("algoStatus") or "").upper()
        status = {"NEW": "OPEN", "CANCELED": "CANCELED", "CANCELLED": "CANCELED",
                  "REJECTED": "REJECTED", "EXPIRED": "CANCELED"}.get(st, "UNKNOWN")
        # TRIGGERING/TRIGGERED/FINISHED 但没有子订单证据：保持未知，不能凭状态猜成交。
        return UniOrder(coid=coid, ex_id=str(a.get("algoId") or ""), status=status,
                        stop_price=float(a.get("triggerPrice") or 0),
                        amount=float(a.get("quantity") or 0),
                        side=str(a.get("side") or "").lower(),
                        ts=int(a.get("updateTime") or a.get("createTime") or 0), raw=a)

    # ---------- 查询 ----------
    def load_instrument(self) -> Optional[InstrumentSpec]:
        try:
            required = ("fapiPrivateGetOrder", "fapiPrivatePostOrder", "fapiPrivateDeleteOrder",
                        "fapiPrivateGetOpenOrders", "fapiPrivateGetAlgoOrder",
                        "fapiPrivatePostAlgoOrder", "fapiPrivateDeleteAlgoOrder",
                        "fapiPrivateGetOpenAlgoOrders", "fapiPublicGetPremiumIndex")
            if any(not callable(getattr(self.ex, n, None)) for n in required):
                raise ValueError("CCXT 缺少必要原生端点；请安装支持 Algo Order 的版本")
            self._throttle()
            self.ex.load_markets()
            m = self.ex.market(self.symbol)
            if (m.get("linear") is not True or m.get("settle") != "USDT"
                    or float(m.get("contractSize") or 0) != 1):
                raise ValueError("本实现只支持 contractSize=1 的 USDT 线性合约")
            fs = {x["filterType"]: x for x in (m.get("info") or {}).get("filters", [])}
            price, lot, market = fs["PRICE_FILTER"], fs["LOT_SIZE"], fs["MARKET_LOT_SIZE"]
            notional = fs.get("MIN_NOTIONAL") or fs.get("NOTIONAL") or {}
            spec = InstrumentSpec(self.symbol, price["tickSize"], lot["stepSize"],
                                  lot["minQty"], lot["maxQty"],
                                  notional.get("notional") or notional.get("minNotional") or 0)
            spec.market_step_size = float(market["stepSize"]) or spec.step_size
            spec.market_min_qty = float(market["minQty"]) or spec.min_qty
            spec.market_max_qty = float(market["maxQty"])
            values = (spec.tick_size, spec.step_size, spec.min_qty, spec.max_qty,
                      spec.market_step_size, spec.market_min_qty, spec.market_max_qty)
            if any(not math.isfinite(v) or v <= 0 for v in values):
                raise ValueError("交易过滤器缺失或非法；不能把 precision 位数猜成 stepSize")
            return spec
        except Exception as e:
            logger.critical(f"[网关] 交易规格校验失败，拒绝启动: {e}")
            return None

    def fetch_last_price(self) -> Optional[float]:
        try:
            self._throttle()
            p = float(self.ex.fetch_ticker(self.symbol).get("last") or 0)
            return p if math.isfinite(p) and p > 0 else None
        except Exception as e:
            logger.error(f"[网关] 拉取最新价失败(本轮跳过决策) | 错误:[{e}]")
            return None

    def fetch_open_orders(self, coid_prefix: str) -> Optional[Dict[str, UniOrder]]:
        """两类快照均成功才 complete；部分成功仍可用于对账，但不可判定清场完成。"""
        out, successful = {}, 0
        self.snapshot_complete = False
        for endpoint, convert in ((self.ex.fapiPrivateGetOpenOrders, self._to_uni),
                                  (self.ex.fapiPrivateGetOpenAlgoOrders, self._to_algo)):
            try:
                self._throttle()
                rows = endpoint({"symbol": self.market_id})
                if not isinstance(rows, list):
                    raise ValueError("在线订单返回类型不是列表")
                for raw in rows:
                    cid = raw.get("clientAlgoId") or raw.get("clientOrderId") or ""
                    if cid.startswith(coid_prefix):
                        out[cid] = convert(raw)
                successful += 1
            except Exception as e:
                logger.error(f"[网关] 快照不完整；保留已知订单对账，禁止新增开仓和宣告清场: {e}")
        self.snapshot_complete = successful == 2
        return out if successful else None
    def fetch_order(self, coid: str) -> Optional[UniOrder]:
        try:
            self._throttle()
            if self._is_algo(coid):
                a = self.ex.fapiPrivateGetAlgoOrder({"clientAlgoId": coid})
                u = self._to_algo(a)
            else:
                o = self.ex.fapiPrivateGetOrder({"symbol": self.market_id, "origClientOrderId": coid})
                u = self._to_uni(o)
            if u.coid != coid:
                raise ValueError("点查回执 client ID 不匹配")
            return u
        except Exception as e:
            # 包括 -2013：查询无记录不是从未受理的证明，也可能超过历史保存期限。
            logger.warning(f"[网关] 订单尚无确定证据，保留OID继续核对 {coid}: {e}")
            return None


    def fetch_risk_price(self, working_type: str) -> Optional[float]:
        if working_type == "CONTRACT_PRICE":
            return self.fetch_last_price()
        try:
            self._throttle()
            r = self.ex.fapiPublicGetPremiumIndex({"symbol": self.market_id})
            p = float(r.get("markPrice") or 0)
            return p if math.isfinite(p) and p > 0 else None
        except Exception as e:
            logger.warning(f"[网关] 标记价格暂不可用: {e}")
            return None

    def fetch_position_qty(self, position_side: str) -> Optional[float]:
        """仅夹逼本轮平仓量；不据此修改策略账本或猜测外部平仓盈亏。"""
        try:
            self._throttle()
            rows = self.ex.fetch_positions([self.symbol])
            if not isinstance(rows, list):
                raise ValueError("仓位响应非列表")
            for p in rows:
                info = p.get("info") or {}
                ps = str(info.get("positionSide") or p.get("side") or "").upper()
                if ps == position_side:
                    raw = p.get("contracts") if p.get("contracts") is not None else info.get("positionAmt")
                    if raw is None:
                        raise ValueError("仓位数量字段缺失")
                    qty = abs(float(raw))
                    if not math.isfinite(qty):
                        raise ValueError("仓位数量非有限值")
                    return qty
            return 0.0
        except Exception as e:
            logger.warning(f"[网关] 真实仓位未确认，保留保护单，本轮不新增风险: {e}")
            return None

    def is_hedge_mode(self) -> Optional[bool]:
        try:
            self._throttle()
            r = self.ex.fapiPrivateGetPositionSideDual()
            return bool(str(r.get("dualSidePosition")).lower() == "true")
        except Exception as e:
            logger.info(f"[网关] 无法确认持仓模式，启动将被阻止 | 错误:[{e}]")
            return None

    # ---------- 下单 ----------
    def _place_error(self, exc) -> PlaceResult:
        """只有明确服务端拒绝才允许重试；其余异常均视为写入结果未知。"""
        msg = str(exc)
        kind = classify_error(msg)
        match = re.search(r'["\']code["\']\s*:\s*(-\d+)', msg)
        code = int(match.group(1)) if match else None
        definite_codes = {-1001, -1003, -1008, -1021, -1013, -1111, -2018, -2019,
                          -2021, -2022, -4003, -4005, -4013, -4014, -4015,
                          -4016, -4024, -4131, -4164}
        definite = code in definite_codes or msg.strip().lower() == "service unavailable."
        return PlaceResult(unknown=not definite or kind is ErrKind.DUPLICATE,
                           err=msg, kind=kind)


    def _place_regular(self, side, qty, coid, position_side, price=None) -> PlaceResult:
        try:
            params = {"symbol": self.market_id, "side": side.upper(),
                      "positionSide": position_side, "newClientOrderId": coid,
                      "quantity": self.ex.amount_to_precision(self.symbol, qty),
                      "type": "MARKET" if price is None else "LIMIT"}
            if price is not None:
                params.update(price=self.ex.price_to_precision(self.symbol, price), timeInForce="GTC")
        except Exception as e:  # 本地参数阶段，没有发送请求
            return PlaceResult(err=str(e), kind=ErrKind.INVALID)
        try:
            self._throttle()
            o = self.ex.fapiPrivatePostOrder(params)
            if not o or not o.get("orderId") or o.get("clientOrderId") != coid:
                return PlaceResult(unknown=True, err="订单回执缺少匹配的ID", kind=ErrKind.FATAL)
            return PlaceResult(ok=True, ex_id=str(o["orderId"]))
        except Exception as e:
            return self._place_error(e)

    def place_limit(self, side, qty, price, coid, position_side) -> PlaceResult:
        return self._place_regular(side, qty, coid, position_side, price)

    def place_market(self, side, qty, coid, position_side) -> PlaceResult:
        return self._place_regular(side, qty, coid, position_side)

    def place_stop_market(self, side, qty, stop_price, coid, position_side,
                          working_type="MARK_PRICE") -> PlaceResult:
        try:
            params = {"algoType": "CONDITIONAL", "symbol": self.market_id,
                      "type": "STOP_MARKET", "side": side.upper(),
                      "positionSide": position_side, "clientAlgoId": coid,
                      "quantity": self.ex.amount_to_precision(self.symbol, qty),
                      "triggerPrice": self.ex.price_to_precision(self.symbol, stop_price),
                      "workingType": working_type, "priceProtect": "false"}
        except Exception as e:
            return PlaceResult(err=str(e), kind=ErrKind.INVALID)
        try:
            self._throttle()
            o = self.ex.fapiPrivatePostAlgoOrder(params)
            if not o or not o.get("algoId") or o.get("clientAlgoId") != coid:
                return PlaceResult(unknown=True, err="算法单回执缺少匹配的ID", kind=ErrKind.FATAL)
            return PlaceResult(ok=True, ex_id=str(o["algoId"]))
        except Exception as e:
            return self._place_error(e)

    def cancel(self, coid: str) -> bool:
        """仅返回撤单请求结果；上层还必须点查最终状态及最终成交。"""
        try:
            self._throttle()
            if self._is_algo(coid):
                self.ex.fapiPrivateDeleteAlgoOrder({"clientAlgoId": coid})
            else:
                self.ex.fapiPrivateDeleteOrder({"symbol": self.market_id, "origClientOrderId": coid})
            return True
        except Exception as e:
            logger.warning(f"[网关] 撤单未确认，交由最终点查裁决 {coid}: {e}")
            return False

# ==============================================================================
# 5. WAL 账本
# ==============================================================================
class MartinLedger:
    """
    追加式领域事件账本 (Write-Ahead Log)。
    铁律: 任何与交易所的写操作, 必须【先落盘意图, 再发请求, 再落盘结果】。
    account 的意义在于: 崩溃后即使不知道请求有没有发出去, 也知道用过哪个 OID, 从而能点查裁决。
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
    A_ORDER_FINAL = "ORDER_FINAL"
    A_CYCLE_CLOSING = "CYCLE_CLOSING"
    A_SIGNAL_CONSUMED = "SIGNAL_CONSUMED"

    def __init__(self, strategy_id: str):
        self.filename = f"martin_ledger_{strategy_id}.csv"
        if not os.path.exists(self.filename):
            with open(self.filename, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self.COLUMNS)
                f.flush()
                os.fsync(f.fileno())

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
            logger.critical(f"[账本] 写盘失败，停止交易写操作 | 错误:[{e}]")
            raise LedgerError(str(e)) from e
        logger.info(f"[账本] {action} 周期[{cycle_id}] 层[{layer}] 角色[{role}] "
                    f"CID[{coid}] 价[{price}] 量[{qty}] {status} {msg}")

    # ---------- 冷启动读取 ----------
    def load_state(self) -> Tuple[Optional[dict], List[dict], int]:
        """严格顺序读取；损坏账本不等价于空账本，恢复失败不能清空保护单。"""
        try:
            with open(self.filename, "r", newline="", encoding="utf-8") as f:
                raw = f.read()
            if not raw.endswith("\n"):
                raise ValueError("CSV 尾行未完整写入")
            import io
            reader = csv.DictReader(io.StringIO(raw), strict=True)
            if reader.fieldnames != self.COLUMNS:
                raise ValueError("CSV 表头不匹配")
            rows = list(reader)
            active, cycle_rows, max_ts = None, [], 0
            for row in rows:
                if None in row or any(row.get(k) is None for k in self.COLUMNS):
                    raise ValueError("CSV 列数不完整")
                ts = int(row["signal_ts"] or 0)
                max_ts = max(max_ts, ts)
                action = row["action"]
                if action == self.A_CYCLE_START:
                    if active is not None:
                        raise ValueError("存在重叠未结束周期")
                    active = json.loads(row["msg"])
                    if not active.get("layers"):
                        raise ValueError("周期蓝图缺失")
                    active["cycle_id"] = row["cycle_id"]
                    cycle_rows = []
                if active and row["cycle_id"] == active["cycle_id"]:
                    cycle_rows.append(row)
                    if action == self.A_CYCLE_END:
                        json.loads(row["msg"])
                        active, cycle_rows = None, []
            return active, cycle_rows, max_ts
        except Exception as e:
            raise LedgerError(f"账本读取/校验失败，保留现场: {e}") from e


# ==============================================================================
# 6. 配置与信号闸门
# ==============================================================================
class MartinConfig:
    """
    一个实例 = 一个独立子进程 = 一本独立账本 = 一个独立 OID 命名空间。
    同一币种可配置多个；OID和账本隔离，交易所同向仓位仍然共享。
    """

    def __init__(self, strategy_id, symbol, signal_name,
                 first_qty=0.0, first_notional=0.0,
                 step_pct=2.0, qty_mult=2.0, tp_pct=0.8, max_loss_usdt=50.0,
                 step_mode="GEOMETRIC",                # GEOMETRIC=相对上一层  ARITHMETIC=相对首单
                 max_layers=10,                        # 层数硬顶(第一道防疯狂加仓闸)
                 layer_loss_budget_ratio=0.80,         # 层数亏损预算比例, 给止损留缓冲
                 max_position_notional=0.0,            # 0=不限, 名义价值硬顶
                 allowed_directions=("LONG", "SHORT"),
                 max_signal_age_sec=90,
                 max_signal_deviation_pct=1.0,         # 信号价与现价的最大允许偏离
                 entry_timeout_sec=900,                # 入场超时: 一手未成则作废周期
                 max_cycle_sec=0,                      # 0=不限, 周期总超时强平
                 cooldown_sec=60,                      # 周期结束后的冷却期
                 poll_interval_sec=2.0,
                 idle_poll_interval_sec=5.0,
                 sl_working_type="MARK_PRICE",         # MARK_PRICE 防插针 / CONTRACT_PRICE 更灵敏
                 fee_pct_per_side=0.0,                 # >0 则止盈价自动包含往返手续费
                 clamp_exit_by_position=True,          # 用交易所真实仓位夹逼平仓量, 防 -2022
                 signal_columns=None):
        self.strategy_id = str(strategy_id)
        self.symbol = symbol
        self.signal_name = signal_name
        self.first_qty = float(first_qty)
        self.first_notional = float(first_notional)
        self.step_pct = float(step_pct)
        self.qty_mult = float(qty_mult)
        self.tp_pct = float(tp_pct)
        self.max_loss_usdt = float(max_loss_usdt)
        self.step_mode = step_mode
        self.max_layers = int(max_layers)
        self.layer_loss_budget_ratio = float(layer_loss_budget_ratio)
        self.max_position_notional = float(max_position_notional)
        self.allowed_directions = tuple(d.upper() for d in allowed_directions)
        self.max_signal_age_sec = float(max_signal_age_sec)
        self.max_signal_deviation_pct = float(max_signal_deviation_pct)
        self.entry_timeout_sec = float(entry_timeout_sec)
        self.max_cycle_sec = float(max_cycle_sec)
        self.cooldown_sec = float(cooldown_sec)
        self.poll_interval_sec = float(poll_interval_sec)
        self.idle_poll_interval_sec = float(idle_poll_interval_sec)
        self.sl_working_type = sl_working_type
        self.fee_pct_per_side = float(fee_pct_per_side)
        self.clamp_exit_by_position = bool(clamp_exit_by_position)
        self.signal_columns = signal_columns or {}

    def validate(self):
        """启动前强校验: 配置错了直接拒绝启动, 绝不带病上线。"""
        errs = []
        if SIGNAL_IMPORT_ERROR:
            errs.append(f"signal_lib 导入失败: {SIGNAL_IMPORT_ERROR}")
        for key, value in vars(self).items():
            if isinstance(value, float) and not math.isfinite(value):
                errs.append(f"{key} 必须为有限数值")
        if not self.allowed_directions or set(self.allowed_directions) - {"LONG", "SHORT"}:
            errs.append("allowed_directions 只能包含 LONG/SHORT，且不可为空")
        if self.sl_working_type not in ("MARK_PRICE", "CONTRACT_PRICE"):
            errs.append("sl_working_type 非法")
        for name in ("poll_interval_sec", "idle_poll_interval_sec", "entry_timeout_sec", "max_signal_age_sec"):
            if getattr(self, name) <= 0:
                errs.append(f"{name} 必须大于0")
        for name in ("first_qty", "first_notional", "cooldown_sec", "max_cycle_sec",
                     "max_position_notional", "max_signal_deviation_pct", "fee_pct_per_side"):
            if getattr(self, name) < 0:
                errs.append(f"{name} 不可为负数")
        if not re.fullmatch(r"[A-Za-z0-9]{1,8}", self.strategy_id):
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
        if not (1 <= self.max_layers <= 30):
            errs.append("max_layers 必须在 [1,30] 区间")
        if not (0.1 <= self.layer_loss_budget_ratio <= 0.95):
            errs.append("layer_loss_budget_ratio 建议在 [0.1,0.95]")
        if self.step_mode not in ("GEOMETRIC", "ARITHMETIC"):
            errs.append("step_mode 只能是 GEOMETRIC / ARITHMETIC")
        if errs:
            for e in errs:
                logger.critical(f"[配置] 校验失败: {e}")
            raise SystemExit(1)


class SignalGate:
    """
    极简版信号闸门：专为干净标准化的 DataFrame 设计
    明确期望字段: timestamp(ms), event(OPEN/CLOSE), direction(LONG/SHORT), price(float)
    """

    def __init__(self, cfg: MartinConfig):
        self.cfg = cfg
        self.func = SIGNAL_REGISTRY[cfg.signal_name]
        self.watermark_ts = 0  # 已消费信号时间戳水位线(去重防刷)

    def set_watermark(self, ts: int):
        self.watermark_ts = max(self.watermark_ts, int(ts or 0))

    def poll(self, market_price: float) -> Optional[Signal]:
        try:
            df = self.func(self.cfg.symbol)
            if df is None or df.empty:
                return None

            # 1. 取最后一行信号
            row = df.iloc[-1]
            col = lambda name: self.cfg.signal_columns.get(name, name)

            # 2. 只处理开仓信号 (马丁策略依靠内部止盈止损平仓，忽略外部 CLOSE)
            if str(row[col('event')]).upper() != "OPEN":
                return None

            # 3. 提取方向
            direction_str = str(row[col('direction')]).upper()
            if direction_str == "LONG":
                direction = Direction.LONG
            elif direction_str == "SHORT":
                direction = Direction.SHORT
            else:
                return None

            # 检查方向是否在配置的白名单中
            if direction.value not in self.cfg.allowed_directions:
                return None

            # 4. 提取价格和时间戳
            px = float(row[col('price')])
            ts = int(row[col('timestamp')])

            if not math.isfinite(px) or px <= 0 or ts <= 0 or ts > time.time() * 1000 + 5000:
                logger.warning("[信号] 非法价格或未来时间戳，拒绝且不污染水位线")
                return None

            # 5. 风控校验 1：去重与过期作废
            if ts <= self.watermark_ts:
                return None  # 老信号，静默跳过

            age_sec = (time.time() * 1000 - ts) / 1000.0
            if age_sec > self.cfg.max_signal_age_sec:
                logger.info(f"[信号] 信号已过期，拒绝追单 | 滞后:[{age_sec:.1f}s]")
                self.set_watermark(ts)
                return None

            # 6. 风控校验 2：信号价与现价偏离不能过大
            if market_price > 0:
                dev = abs(px / market_price - 1) * 100
                if dev > self.cfg.max_signal_deviation_pct:
                    logger.info(f"[信号] 信号偏离现价过大，丢弃 | 偏离:[{dev:.3f}%]")
                    self.set_watermark(ts)
                    return None

            return Signal(direction, px, ts, self.cfg.signal_name)

        except Exception as e:
            logger.error(f"[信号] 读取标准信号失败: {e}")
            return None

# ==============================================================================
# 7. 马丁蓝图 (层数 / 价格 / 数量 / 风控上限)
# ==============================================================================
class LayerPlan:
    __slots__ = ("layer", "price", "qty", "coid", "ex_id", "life", "attempts",
                 "next_retry_ts", "last_action_ts")

    def __init__(self, layer, price, qty):
        self.layer = layer
        self.price = float(price)
        self.qty = float(qty)
        self.coid = ""
        self.ex_id = ""
        self.life = Life.NOT_PLACED
        self.attempts = 0
        self.next_retry_ts = 0.0
        self.last_action_ts = 0.0

    def to_dict(self):
        return {"l": self.layer, "p": self.price, "q": self.qty}


class Blueprint:
    def __init__(self, direction: Direction, layers: List[LayerPlan], base_price: float):
        self.direction = direction
        self.layers = layers
        self.base_price = base_price

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
    由信号 + 配置 + 交易规格推导完整马丁蓝图。
    层数判定(修正你原方案的边界取法):
        只有当"下一层成交后的浮亏 <= max_loss * layer_loss_budget_ratio"时才允许挂这一层。
        原因: 若取"止损价刚好还在下一层之下"作为边界, 最深层一成交浮亏就已 ≈ 最大亏损,
             止损价会贴在最后成交价上, 加完最后一仓立刻被扫止损 —— 最坏结局。
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

        layers: List[LayerPlan] = []
        acc_qty = acc_cost = 0.0
        prev_price = p0
        budget = cfg.max_loss_usdt * cfg.layer_loss_budget_ratio
        rows = []

        for i in range(cfg.max_layers):
            if i == 0:
                price = p0
            elif cfg.step_mode == "GEOMETRIC":
                price = prev_price * (1 - sign * cfg.step_pct / 100.0)
            else:
                price = p0 * (1 - sign * cfg.step_pct * i / 100.0)
            price = spec.round_price(price, "down" if d is Direction.LONG else "up")

            if price <= 0:
                break
            # 精度塌陷: 修约后层间价格不再单调, 低价币无法继续细分
            if i > 0 and ((d is Direction.LONG and price >= prev_price) or
                          (d is Direction.SHORT and price <= prev_price)):
                logger.info(f"[蓝图] 第[{i}]层等比价差已小于最小报价刻度(tick={spec.tick_size}), "
                            f"层数在此收口")
                break

            qty = spec.normalize_open_qty(base_qty * (cfg.qty_mult ** i), price)
            if qty <= 0:
                logger.info(f"[蓝图] 第[{i}]层无法构造合法数量(超 maxQty 或精度不足), 层数收口")
                break

            n_qty = acc_qty + qty
            n_cost = acc_cost + price * qty
            if n_qty > min(spec.max_qty, spec.market_max_qty):
                logger.info("[蓝图] 累计数量超过单张TP/SL可承载上限，层数收口")
                break
            if cfg.max_position_notional > 0 and n_cost > cfg.max_position_notional:
                logger.info("[蓝图] 包含首层在内的名义价值超过配置上限，层数收口")
                break
            n_avg = n_cost / n_qty
            loss_at_fill = sign * (n_avg - price) * n_qty        # 该层成交瞬间的浮亏(>=0)

            if i == 0:
                # 首单必须满足最小名义价值, 否则整个信号作废(不能只丢一层, 会破坏马丁结构)
                if spec.notional(price, qty) < spec.min_notional * (1 - 1e-9):
                    logger.info(f"[蓝图] 首单名义价值[{spec.notional(price, qty):.4f}]低于交易所底线"
                                f"[{spec.min_notional}], 丢弃信号")
                    return None
            else:
                if loss_at_fill > budget:
                    logger.info(f"[蓝图] 第[{i}]层成交后浮亏[{loss_at_fill:.2f}U]将超出亏损预算"
                                f"[{budget:.2f}U = 最大亏损{cfg.max_loss_usdt}×{cfg.layer_loss_budget_ratio}], "
                                f"层数在此收口")
                    break
                if cfg.max_position_notional > 0 and n_cost > cfg.max_position_notional:
                    logger.info(f"[蓝图] 第[{i}]层将使名义价值[{n_cost:.2f}U]突破上限"
                                f"[{cfg.max_position_notional}U], 层数在此收口")
                    break

            lp = LayerPlan(i, price, qty)
            layers.append(lp)
            acc_qty, acc_cost, prev_price = n_qty, n_cost, price
            sl = n_avg - sign * cfg.max_loss_usdt / n_qty
            rows.append((i, price, qty, n_qty, n_cost, n_avg, loss_at_fill, sl,
                         abs(sl / n_avg - 1) * 100))

        if not layers:
            logger.info("[蓝图] 未能生成任何合法层, 丢弃信号")
            return None
        if len(layers) < 2:
            logger.info(f"[蓝图] ⚠️ 仅能生成 [1] 层, 马丁结构退化为单笔交易 | "
                        f"请检查 max_loss_usdt / first_qty 配比是否合理")

        head = (f"\n===== [马丁蓝图] {cfg.strategy_id} {cfg.symbol} {d.value} "
                f"信号价:{sig.limit_price} 层数:{len(layers)} =====\n"
                f"{'层':>3} {'价格':>14} {'数量':>12} {'累计量':>12} {'累计成本U':>12} "
                f"{'均价':>14} {'该层浮亏U':>10} {'止损价':>14} {'止损距均价%':>11}\n")
        body = "\n".join(
            f"{r[0]:>3} {r[1]:>14.8g} {r[2]:>12.8g} {r[3]:>12.8g} {r[4]:>12.2f} "
            f"{r[5]:>14.8g} {r[6]:>10.2f} {r[7]:>14.8g} {r[8]:>11.3f}" for r in rows)
        tail = (f"\n最大名义价值:[{acc_cost:.2f}U] 最大亏损设定:[{cfg.max_loss_usdt}U] "
                f"止盈:[{cfg.tp_pct}%] 间距:[{cfg.step_pct}% {cfg.step_mode}] 倍数:[{cfg.qty_mult}]\n"
                f"=======================================================================")
        logger.info(head + body + tail)
        return Blueprint(d, layers, p0)


# ==============================================================================
# 8. 虚拟仓位账 (I1: 唯一记账来源)
# ==============================================================================
class PositionBook:
    """
    本策略的虚拟仓位账。只由"本策略 OID 的成交增量"驱动, 与交易所仓位完全解耦,
    隔离本地成本计算，但无法为交易所的共享同向仓位建立所有权隔离。
    """

    def __init__(self, direction: Direction):
        self.direction = direction
        self.open_qty = 0.0        # 当前虚拟持仓
        self.cost = 0.0            # 当前持仓成本(用于均价)
        self.realized = 0.0        # 已实现盈亏(含部分止盈)
        self.total_open_filled = 0.0
        self.total_close_filled = 0.0

    @property
    def avg(self) -> float:
        return self.cost / self.open_qty if self.open_qty > 1e-12 else 0.0

    def add_open(self, price: float, qty: float):
        self.open_qty += qty
        self.cost += price * qty
        self.total_open_filled += qty

    def add_close(self, price: float, qty: float):
        if self.open_qty <= 1e-12:
            return  # 【核心修复】持仓已归零，忽略任何滞后事件，彻底杜绝除零导致天量虚假利润
        qty = min(qty, self.open_qty)
        if qty <= 0:
            return
        avg = self.avg
        self.realized += self.direction.sign * (price - avg) * qty
        self.cost -= avg * qty
        self.open_qty -= qty
        self.total_close_filled += qty
        if self.open_qty <= 1e-12:
            self.open_qty = 0.0
            self.cost = 0.0

    def tp_price(self, tp_pct: float, fee_pct_per_side: float = 0.0) -> float:
        """按往返手续费比例近似抬高止盈目标；不保证实际净收益率。"""
        eff = tp_pct + 2 * fee_pct_per_side
        return self.avg * (1 + self.direction.sign * eff / 100.0)

    def sl_price(self, max_loss: float) -> float:
        """
        止损触发价: 解 sign*(P-avg)*Q + realized = -max_loss
        已实现利润会自动放宽止损空间(已经赚到的钱本就是缓冲), 公式天然统一多空。
        """
        if self.open_qty <= 1e-12:
            return 0.0
        return self.avg - self.direction.sign * (max_loss + self.realized) / self.open_qty

    def unrealized(self, price: float) -> float:
        if self.open_qty <= 1e-12:
            return 0.0
        return self.direction.sign * (price - self.avg) * self.open_qty

    def snapshot(self) -> dict:
        return {"open_qty": self.open_qty, "avg": self.avg, "realized": self.realized,
                "open_filled": self.total_open_filled, "close_filled": self.total_close_filled}


# ==============================================================================
# 9. 周期状态机
# ==============================================================================
class CycleCtx:
    """注入给周期的运行环境, 让状态机方法保持干净签名。"""

    def __init__(self, cfg: MartinConfig, gw: ExchangeGateway, ledger: MartinLedger,
                 spec: InstrumentSpec):
        self.cfg = cfg
        self.gw = gw
        self.ledger = ledger
        self.spec = spec


class ExitOrder:
    """止盈 / 止损单的跟踪器。"""
    __slots__ = ("role", "coid", "ex_id", "target_price", "target_qty", "life",
                 "live_price", "live_remaining", "last_action_ts", "attempts", "next_retry_ts")

    def __init__(self, role: OrderRole):
        self.role = role
        self.coid = ""
        self.ex_id = ""
        self.target_price = 0.0
        self.target_qty = 0.0
        self.life = Life.NOT_PLACED
        self.live_price = 0.0
        self.live_remaining = 0.0
        self.last_action_ts = 0.0
        self.attempts = 0
        self.next_retry_ts = 0.0

    def reset(self):
        self.coid = ""
        self.ex_id = ""
        self.life = Life.NOT_PLACED
        self.live_price = 0.0
        self.live_remaining = 0.0


class MartinCycle:
    """
    一个马丁周期的完整状态机。
    生命周期: 全量铺单 -> (成交->重算均价->撤旧挂新TP/SL)* -> 止盈/止损/超时 -> 终结。
    所有状态修改只发生在主线程调用的 maintain() 内, 天然单写者、无需加锁。
    """

    def __init__(self, ctx: CycleCtx, cycle_id: str, signal_ts: int,
                 direction: Direction, blueprint: Blueprint):
        self.ctx = ctx
        self.cycle_id = cycle_id
        self.signal_ts = signal_ts
        self.direction = direction
        self.bp = blueprint
        self.book = PositionBook(direction)
        self.tp = ExitOrder(OrderRole.TP)
        self.sl = ExitOrder(OrderRole.SL)
        self.acked: Dict[str, float] = {}  # I2: coid -> 已入账成交量
        self.order_cum_cost: Dict[str, float] = {}  # 【新增】coid -> 已入账累计金额，精准反求边际价格
        self.forced_close_coid = ""  # 【新增】记录强平单 OID，防止被清扫机制误杀
        self.add_suspended = False  # 降级标志: 停止加仓, 只收尾
        self.end_reason: Optional[EndReason] = None
        self.created_ts = time.time()
        self.first_fill_ts = 0.0
        self.sl_breach_since = 0.0
        self.forced_close_sent = False
        self.orders: Dict[str, UniOrder] = {}   # 本周期所有用过的OID，直到确定终态才停止查询
        self.final_orders = set()
        self.force_retry_ts = 0.0
        self.force_attempts = 0
        self.snapshot_complete = False
        self.risk_price = None
        self.stop_requested = False

    # ---------------------- 对外 ----------------------
    def maintain(self, snapshot: Dict[str, UniOrder], price: float,
                 ex_pos_qty: Optional[float]) -> Optional[EndReason]:
        """先统一对账，再判断结束/保护，最后才允许补挂开仓单。"""
        now = time.time()
        self.snapshot_complete = (self.ctx.gw.snapshot_complete and price > 0
                                  and (ex_pos_qty is not None or not self.ctx.cfg.clamp_exit_by_position))
        self._reconcile(snapshot)
        self._sweep_untracked(snapshot)
        for lp in self.bp.layers:
            self._maintain_layer(lp, snapshot, price, now)
        self._sync_exit(self.tp, snapshot, now)
        self._sync_exit(self.sl, snapshot, now)
        self._check_invariants(ex_pos_qty)
        if self.add_suspended:
            self._cancel_open_layers("降级持续撤加仓单")
        if self._check_end(price, now):
            self.begin_teardown(self.end_reason)
            return self.end_reason
        risk_price = self.risk_price or 0.0
        self._align_exit_sl(risk_price, ex_pos_qty, now)
        self._align_exit_tp(price, ex_pos_qty, now)
        self._bottom_guard(risk_price, now)
        if self._check_end(price, now):
            self.begin_teardown(self.end_reason)
            return self.end_reason
        # 每轮最多新增一层；初始铺单也经本方法，成交后先建立保护再继续。
        for lp in self.bp.layers:
            if lp.life in (Life.NOT_PLACED, Life.DEFERRED):
                before = lp.attempts
                self._try_place_layer(lp, price, now)
                if lp.attempts != before:
                    break
        return self.end_reason


    def _reconcile(self, snapshot: Dict[str, UniOrder]):
        candidates = dict.fromkeys(list(self.orders) + list(snapshot))
        observations = {}
        for cid in candidates:
            p = OidCodec.parse(cid)
            if not p or p.strategy_id != self.ctx.cfg.strategy_id or p.cycle_id != self.cycle_id:
                raise ReconcileError(f"发现其它周期/归属不明的本策略订单: {cid}")
            if cid in self.final_orders:
                continue
            o = snapshot.get(cid) or self.ctx.gw.fetch_order(cid)
            if o is None:
                prior = self.orders.get(cid)
                self.orders[cid] = UniOrder(coid=cid, status="UNKNOWN",
                                           amount=prior.amount if prior else 0,
                                           filled=self.acked.get(cid, 0))
            else:
                observations[cid] = o
        # REST累计量不能还原交易所逐笔顺序；先开后平避免暂时负仓，WAL记录实际应用顺序。
        for cid in sorted(observations, key=lambda c: OidCodec.parse(c).role is not OrderRole.OPEN):
            self._observe(cid, observations[cid])


    def begin_teardown(self, reason: EndReason):
        if not getattr(self, "closing_recorded", False):
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, -1, "-",
                                   MartinLedger.A_CYCLE_CLOSING, status=reason.value)
            self.closing_recorded = True
        self.end_reason = reason
        self.add_suspended = True

    def place_all_layers(self, price: float):
        """保持完整蓝图；逐层铺单，每次发送后插入对账和保护单维护。"""
        for _ in self.bp.layers:
            if self.stop_requested or self.end_reason or self.add_suspended:
                break
            snapshot = self.ctx.gw.fetch_open_orders(OidCodec.strategy_prefix(self.ctx.cfg.strategy_id))
            self.risk_price = self.ctx.gw.fetch_risk_price(self.ctx.cfg.sl_working_type)
            ex_qty = self.ctx.gw.fetch_position_qty(self.direction.position_side)
            if snapshot is None or not self.ctx.gw.snapshot_complete:
                break
            self.maintain(snapshot, price, ex_qty)
        if not self.end_reason:
            snapshot = self.ctx.gw.fetch_open_orders(OidCodec.strategy_prefix(self.ctx.cfg.strategy_id))
            self.risk_price = self.ctx.gw.fetch_risk_price(self.ctx.cfg.sl_working_type)
            ex_qty = self.ctx.gw.fetch_position_qty(self.direction.position_side)
            self._reconcile(snapshot or {})
            for lp in self.bp.layers:
                self._maintain_layer(lp, snapshot or {}, price, time.time())
            self._sync_exit(self.tp, snapshot or {}, time.time())
            self._sync_exit(self.sl, snapshot or {}, time.time())
            if self._check_end(price, time.time()):
                self.begin_teardown(self.end_reason)
            else:
                self._align_exit_sl(self.risk_price or 0, ex_qty, time.time())
                self._align_exit_tp(price, ex_qty, time.time())

    # ---------------------- A. 清扫 ----------------------
    def _new_oid(self, role: OrderRole, layer: int) -> str:
        for _ in range(10):
            cid = OidCodec.build(self.ctx.cfg.strategy_id, self.cycle_id, role, layer)
            if cid not in self.orders:
                return cid
        raise ReconcileError("OID生成器重复，拒绝覆盖历史订单")

    def _tracked_coids(self) -> set:
        s = {lp.coid for lp in self.bp.layers if lp.coid}
        if self.tp.coid:
            s.add(self.tp.coid)
        if self.sl.coid:
            s.add(self.sl.coid)
        if self.forced_close_coid:
            s.add(self.forced_close_coid)  # 【核心修复】强平单纳入保护白名单
        return s

    def _sweep_untracked(self, snapshot: Dict[str, UniOrder]):
        tracked = self._tracked_coids()
        for coid in list(self.orders):
            if coid in tracked or coid in self.final_orders:
                continue
            self.suspend_add("发现未被当前指针追踪的旧代订单")
            self._cancel(coid, "旧代订单最终确认")

    # ---------------------- B. 开仓层 ----------------------
    def _maintain_layer(self, lp: LayerPlan, snapshot: Dict[str, UniOrder], price: float, now: float):
        if not lp.coid:
            return
        o = self.orders.get(lp.coid)
        if o is None or o.status == "UNKNOWN":
            lp.life = Life.UNKNOWN
            return
        lp.ex_id = o.ex_id or lp.ex_id
        if o.is_terminal:
            if o.status == "FILLED":
                lp.life = Life.FILLED
            elif self.acked.get(lp.coid, 0) > 0 or self.add_suspended or self.end_reason:
                lp.life = Life.DEAD
            else:
                lp.coid, lp.life = "", Life.NOT_PLACED
            return
        lp.life = Life.LIVE
        if self._param_drift(o, lp):
            # 人工改量可能已部分成交；保留原OID证据，放弃该层，不补完整蓝图量。
            self.suspend_add("开仓单参数被外部修改")
            if self._cancel(lp.coid, "参数漂移"):
                lp.life = Life.DEAD

    def _param_drift(self, o: UniOrder, lp: LayerPlan) -> bool:
        tick = self.ctx.spec.tick_size or 1e-12
        step = self.ctx.spec.step_size or 1e-12
        return (abs(o.price - lp.price) > tick * 0.6 or
                abs(o.amount - lp.qty) > step * 0.6)

    def _try_place_layer(self, lp: LayerPlan, price: float, now: float):
        """开仓层挂单, 层层设防, 任何不确定都选择"不挂"。"""
        if self.end_reason or self.add_suspended or self.stop_requested:
            return
        if not self.snapshot_complete or self.risk_price is None:
            return
        if any(o.status == "UNKNOWN" for c, o in self.orders.items() if c not in self.final_orders):
            return
        sl_required = (self.book.open_qty > 1e-12
                       and (self.direction is Direction.SHORT
                            or self.book.sl_price(self.ctx.cfg.max_loss_usdt) > 0))
        if sl_required:
            sl_target = self.ctx.spec.round_price(self.book.sl_price(self.ctx.cfg.max_loss_usdt),
                                                 "up" if self.direction is Direction.LONG else "down")
            if not self._exit_is_aligned(self.sl, sl_target, self.ctx.spec.round_qty(self.book.open_qty)):
                return
        if lp.life in (Life.FILLED, Life.DEAD, Life.LIVE, Life.UNKNOWN, Life.INTENT):
            return
        if now < lp.next_retry_ts:
            return
        if lp.attempts >= MAX_PLACE_ATTEMPTS:
            if lp.life != Life.DEFERRED:
                lp.life = Life.DEFERRED
                logger.critical(f"[层] 第[{lp.layer}]层连续[{lp.attempts}]次挂单失败, 永久停挂并告警 | "
                                f"该层放弃加仓(保守优先), 周期继续用现有仓位收尾")
                self.ctx.ledger.append(self.cycle_id, self.signal_ts, lp.layer,
                                       OrderRole.OPEN.value, MartinLedger.A_ALERT, lp.coid,
                                       lp.price, lp.qty, "GIVEUP", "超过最大尝试次数, 永久停挂")
            return
        # I4 总量硬闸: 已成交 + 本层 不得超过蓝图总量
        if self.book.total_open_filled + lp.qty > self.bp.total_qty * OVERFILL_TOLERANCE:
            logger.critical(f"[风控] 触发总量硬闸, 拒绝挂第[{lp.layer}]层 | 已成交[{self.book.total_open_filled}] "
                            f"蓝图总量[{self.bp.total_qty}]")
            self.suspend_add("总量硬闸触发")
            return
        # 价格带自适应: 曾被交易所以"价格离盘口太远"拒单的层, 等现价靠近再补挂
        if lp.life == Life.DEFERRED and price > 0:
            gap = abs(lp.price / price - 1) * 100
            if gap > DEFER_PLACE_WINDOW_PCT:
                return
        # 无论已成交多少，本层历史成交 + 未决委托只能占用一次蓝图额度。
        reserved = sum(o.remaining for cid, o in self.orders.items()
                       if cid not in self.final_orders and OidCodec.parse(cid).role is OrderRole.OPEN)
        if self.book.total_open_filled + reserved + lp.qty > self.bp.total_qty + self.ctx.spec.step_size * 1e-6:
            self.suspend_add("已成交量加未决委托量超过蓝图")
            return
        # 止损价已被击穿: 不再新增任何加仓单
        if self.book.open_qty > 0 and price > 0:
            slp = self.book.sl_price(self.ctx.cfg.max_loss_usdt)
            if slp > 0 and self._is_breached(self.risk_price, slp):
                logger.info(f"[层] 现价[{price}]已击穿止损价[{slp}], 停止一切加仓")
                return

        lp.attempts += 1
        lp.coid = self._new_oid(OrderRole.OPEN, lp.layer)
        lp.life = Life.INTENT
        lp.last_action_ts = time.time()
        # WAL: 先落意图(带确定的 OID), 再发请求
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, lp.layer, OrderRole.OPEN.value,
                               MartinLedger.A_INTENT_PLACE, lp.coid, lp.price, lp.qty, "PENDING",
                               f"attempt={lp.attempts}")
        self.orders[lp.coid] = UniOrder(coid=lp.coid, status="UNKNOWN", price=lp.price, amount=lp.qty)
        res = self.ctx.gw.place_limit(self.direction.open_side, lp.qty, lp.price,
                                      lp.coid, self.direction.position_side)
        lp.last_action_ts = time.time()
        self._after_place(res, lp.layer, OrderRole.OPEN, lp.coid, lp.price, lp.qty,
                          lp_ref=lp)

    def _after_place(self, res: PlaceResult, layer: int, role: OrderRole, coid: str,
                     price: float, qty: float, lp_ref: Optional[LayerPlan] = None,
                     ex_ref: Optional[ExitOrder] = None):
        holder = lp_ref or ex_ref
        if res.ok:
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role.value,
                                   MartinLedger.A_PLACE_OK, coid, price, qty, "OK", res.ex_id)
            holder.life, holder.ex_id = Life.LIVE, res.ex_id
            if ex_ref:
                holder.live_price, holder.live_remaining = price, qty
                holder.attempts = 0
            self.orders[coid] = UniOrder(coid=coid, ex_id=res.ex_id, status="OPEN", amount=qty,
                                        price=price if role is not OrderRole.SL else 0,
                                        stop_price=price if role is OrderRole.SL else 0)
            return
        if res.unknown or res.kind is ErrKind.DUPLICATE:
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role.value,
                                   MartinLedger.A_PLACE_UNKNOWN, coid, price, qty, "UNKNOWN", res.err)
            holder.life = Life.UNKNOWN
            return
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role.value,
                               MartinLedger.A_PLACE_FAIL, coid, price, qty, res.kind.value, res.err)
        self._observe(coid, UniOrder(coid=coid, status="REJECTED", amount=qty))
        backoff = 3.0 if ex_ref else RETRY_BACKOFF_SEC[min(holder.attempts - 1, len(RETRY_BACKOFF_SEC) - 1)]
        holder.coid = ""
        holder.life = Life.NOT_PLACED
        holder.next_retry_ts = time.time() + backoff
        if res.kind is ErrKind.PRICE_BAND:
            holder.life = Life.DEFERRED  # 仍计入最大次数，避免永久3秒重发
        elif res.kind is ErrKind.INSUFFICIENT:
            holder.next_retry_ts = time.time() + (3.0 if ex_ref else max(backoff, 30))
        elif res.kind is ErrKind.IMMEDIATE_TRIGGER and role is OrderRole.SL:
            self.force_close("条件单立即触发", EndReason.SL_FORCED)
        elif res.kind is ErrKind.REDUCE_REJECT:
            self.suspend_add("真实仓位与虚拟仓位可能不一致")
        elif res.kind in (ErrKind.INVALID, ErrKind.FATAL):
            holder.attempts = MAX_PLACE_ATTEMPTS
            holder.life = Life.DEFERRED
        if role is OrderRole.SL and holder.attempts >= MAX_PLACE_ATTEMPTS:
            self.force_close("止损无法建立", EndReason.SL_FORCED)
    # ---------------------- C. 出场单同步 ----------------------
    def _sync_exit(self, ex: ExitOrder, snapshot: Dict[str, UniOrder], now: float):
        if not ex.coid:
            return
        o = self.orders.get(ex.coid)
        if o is None or o.status == "UNKNOWN":
            ex.life = Life.UNKNOWN
            return
        ex.ex_id = o.ex_id or ex.ex_id
        if o.status == "FILLED":
            ex.life = Life.FILLED
        elif o.is_terminal:
            ex.reset()
        else:
            ex.life = Life.LIVE
            ex.live_remaining = o.remaining
            ex.live_price = o.stop_price if ex.role is OrderRole.SL else o.price

    # ---------------------- D. 不变量校验 ----------------------
    def _check_invariants(self, ex_pos_qty: Optional[float] = None):
        if (self.ctx.cfg.clamp_exit_by_position and ex_pos_qty is not None
                and ex_pos_qty + self.ctx.spec.step_size * 1e-6 < self.book.open_qty):
            self.suspend_add("真实可平仓量小于虚拟量；停止加仓，不伪造平仓成交")

        if self.book.total_open_filled > self.bp.total_qty * OVERFILL_TOLERANCE:
            logger.critical(f"[风控] I4 被破坏! 累计开仓成交[{self.book.total_open_filled}] "
                            f"超过蓝图总量[{self.bp.total_qty}], 立即停止加仓")
            self.suspend_add("I4 总量越界")

    # ---------------------- E/G. 终结与兜底 ----------------------
    def _has_fill(self) -> bool:
        return self.book.total_open_filled > 0

    def _is_breached(self, price: float, sl_price: float) -> bool:
        return (price <= sl_price) if self.direction is Direction.LONG else (price >= sl_price)

    def _check_end(self, price: float, now: float) -> Optional[EndReason]:
        """
        终结判定。
        【修复】：增加行情反向脱轨检测。首单若未成交但行情已起飞，提前撤单作废，不再傻等 15 分钟！
        """
        if self.end_reason:
            return self.end_reason
        spec = self.ctx.spec

        # 1) 虚拟仓位仅剩数值误差且已经开过仓 -> 按最后一张终结的出场单定性
        if self._has_fill() and self.book.open_qty <= 1e-12:
            if self.sl.life == Life.FILLED:
                self.end_reason = EndReason.SL
            elif self.tp.life == Life.FILLED:
                self.end_reason = EndReason.TP
            else:
                self.end_reason = EndReason.MANUAL_FLAT
                logger.critical("[周期] 仓位已归零但止盈/止损单均非成交态, 疑被外部平仓, 周期被动结束")
            logger.info(f"[周期] 终结 | 原因:[{self.end_reason.value}] "
                        f"已实现盈亏:[{self.book.realized:+.4f}U]")
            return self.end_reason

        if self.tp.life is Life.FILLED or self.sl.life is Life.FILLED:
            self.end_reason = EndReason.SL if self.sl.life is Life.FILLED else EndReason.TP
            return self.end_reason

        # 2) 核心修复：入场反向起飞检测 (做多时暴涨，或做空时暴跌)
        if not self._has_fill() and price > 0 and self.bp.base_price > 0:
            runaway_pct = self.direction.sign * (price / self.bp.base_price - 1) * 100
            # 偏离超过加仓间距的 1.5 倍(或至少 2%)，直接作废周期
            if runaway_pct > max(self.ctx.cfg.step_pct * 1.5, 2.0):
                self.end_reason = EndReason.NO_FILL
                logger.info(f"[周期] 首单未成且行情已反向脱轨起飞 (偏离:[{runaway_pct:.2f}%]), 提前作废本周期")
                return self.end_reason

        # 3) 入场硬超时: 一手未成 -> 作废周期
        if not self._has_fill() and now - self.created_ts > self.ctx.cfg.entry_timeout_sec:
            self.end_reason = EndReason.NO_FILL
            logger.info(f"[周期] 入场超时[{self.ctx.cfg.entry_timeout_sec}s]仍无任何成交, "
                        f"撤单作废本周期, 回到空闲态等新信号")
            return self.end_reason

        # 4) 周期总超时(可选)
        if (self.ctx.cfg.max_cycle_sec > 0 and self._has_fill()
                and now - self.created_ts > self.ctx.cfg.max_cycle_sec):
            self.force_close(f"周期超时{self.ctx.cfg.max_cycle_sec}s", EndReason.TIMEOUT)
            return self.end_reason

        return None

    def _bottom_guard(self, price: float, now: float):
        """兜底熔断: 条件止损单没能触发时（使用与条件单相同的价格类型）, 主动市价平仓。"""
        if self.end_reason or price <= 0 or self.book.open_qty <= 1e-12:
            return
        slp = self.book.sl_price(self.ctx.cfg.max_loss_usdt)
        if slp <= 0 or not self._is_breached(price, slp):
            self.sl_breach_since = 0.0
            return
        if self.sl_breach_since == 0.0:
            self.sl_breach_since = now
            logger.critical(f"[熔断] 现价[{price}]已击穿止损价[{slp}], 等待条件单触发 "
                            f"({SL_BREACH_CONFIRM_SEC}s 宽限)...")
            return
        if now - self.sl_breach_since >= SL_BREACH_CONFIRM_SEC:
            logger.critical(f"[熔断] 击穿止损价已[{now - self.sl_breach_since:.1f}s], "
                            f"条件单仍未成交, 主动市价强平本策略数量")
            self.force_close("兜底熔断: 条件单未触发", EndReason.SL_FORCED)

    def force_close(self, why: str, reason: EndReason):
        """先确认其它订单最终终态，再处理唯一市价单；未知结果只查原OID。"""
        self.begin_teardown(reason)
        snapshot = self.ctx.gw.fetch_open_orders(OidCodec.strategy_prefix(self.ctx.cfg.strategy_id))
        complete = snapshot is not None and self.ctx.gw.snapshot_complete
        self._reconcile(snapshot or {})
        if self.forced_close_coid:
            cid = self.forced_close_coid
            if cid not in self.final_orders:
                return
            self.forced_close_coid = ""
            self.forced_close_sent = False
        if not self._cancel_all_working() or not complete:
            return
        if self.book.open_qty <= 1e-12 or time.time() < self.force_retry_ts:
            return
        if self.force_attempts >= MAX_PLACE_ATTEMPTS:
            logger.critical("[强平] 明确失败达上限；保持收尾并告警，需人工核对，绝不伪装空仓")
            return
        ex_qty = self.ctx.gw.fetch_position_qty(self.direction.position_side)
        qty = self._target_exit_qty(ex_qty)
        spec = self.ctx.spec
        qty = quantize(min(qty, spec.market_max_qty), spec.market_step_size, "down")
        if qty < spec.market_min_qty or spec.qty_is_dust(qty):
            logger.critical(f"[强平] 未平数量{self.book.open_qty}不可安全下单，保持收尾，需人工核对")
            return
        cid = self._new_oid(OrderRole.SL, 99)
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, 99, OrderRole.SL.value,
                               MartinLedger.A_INTENT_PLACE, cid, 0, qty, "PENDING", why)
        self.orders[cid] = UniOrder(coid=cid, status="UNKNOWN", amount=qty)
        self.forced_close_coid, self.forced_close_sent = cid, True
        self.force_attempts += 1
        self.force_retry_ts = time.time() + 3
        res = self.ctx.gw.place_market(self.direction.close_side, qty, cid, self.direction.position_side)
        action = (MartinLedger.A_PLACE_OK if res.ok else MartinLedger.A_PLACE_UNKNOWN
                  if res.unknown or res.kind is ErrKind.DUPLICATE else MartinLedger.A_PLACE_FAIL)
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, 99, OrderRole.SL.value,
                               action, cid, 0, qty, "OK" if res.ok else res.kind.value, res.err or why)
        if not res.ok and not res.unknown and res.kind is not ErrKind.DUPLICATE:
            self._observe(cid, UniOrder(coid=cid, status="REJECTED", amount=qty))
        # 是否成交只由后续点查决定，发送成功本身不减少虚拟仓位。


    # ---------------------- F. 出场单对齐 ----------------------
    def _target_exit_qty(self, ex_pos_qty: Optional[float] = None) -> float:
        qty = self.book.open_qty
        if self.ctx.cfg.clamp_exit_by_position:
            if ex_pos_qty is None:
                return 0.0
            qty = min(qty, ex_pos_qty)
        return self.ctx.spec.round_qty(qty, "down")

    def _align_exit_sl(self, price: float, ex_pos_qty: Optional[float], now: float):
        self._align_exit(self.sl, price, ex_pos_qty, now)


    def _align_exit_tp(self, price: float, ex_pos_qty: Optional[float], now: float):
        self._align_exit(self.tp, price, ex_pos_qty, now)


    def _align_exit(self, ex: ExitOrder, price: float, ex_pos_qty: Optional[float], now: float):
        if self.end_reason or self.book.open_qty <= 1e-12:
            return
        if ex.coid and ex.life in (Life.INTENT, Life.UNKNOWN):
            return  # 未知旧单不能撤后换号；先由统一对账得到确定证据
        is_sl = ex.role is OrderRole.SL
        if ex.attempts >= MAX_PLACE_ATTEMPTS:
            if is_sl:
                self.force_close("止损连续失败", EndReason.SL_FORCED)
            return
        spec, cfg = self.ctx.spec, self.ctx.cfg
        qty = self._target_exit_qty(ex_pos_qty)
        if spec.qty_is_dust(qty):
            return  # 无可用仓位证据时不拆掉已有保护，也不把虚拟仓位归零
        raw = self.book.sl_price(cfg.max_loss_usdt) if is_sl else self.book.tp_price(cfg.tp_pct, cfg.fee_pct_per_side)
        target = spec.round_price(raw, "up" if self.direction is Direction.LONG else "down")
        if target <= 0:
            # LONG: 预算可能大于当前持仓跌到0的毛亏损，尚无正数触发价，保留原策略。
            if is_sl and self.direction is Direction.SHORT:
                self.force_close("止损预算已耗尽", EndReason.SL_FORCED)
            return
        if is_sl and price > 0 and self._is_breached(price, target):
            if ex.life is not Life.LIVE or not ex.coid:
                self.force_close("击穿止损且没有已确认止损单", EndReason.SL_FORCED)
            return
        if self._exit_is_aligned(ex, target, qty) or now < ex.next_retry_ts:
            return
        if ex.coid:
            if not self._cancel(ex.coid, "更新止损" if is_sl else "更新止盈"):
                return
            final = self.orders.get(ex.coid)
            ex.reset()
            if final is not None and final.status == "FILLED":
                self.begin_teardown(EndReason.SL if is_sl else EndReason.TP)
                return
            # 撤单竞态可能产生新成交：重新计算，不能沿用撤单前 qty/avg/realized。
            if self._check_end(price, time.time()):
                self.begin_teardown(self.end_reason)
                return
            ex_pos_qty = self.ctx.gw.fetch_position_qty(self.direction.position_side)
            qty = self._target_exit_qty(ex_pos_qty)
            raw = self.book.sl_price(cfg.max_loss_usdt) if is_sl else self.book.tp_price(cfg.tp_pct, cfg.fee_pct_per_side)
            target = spec.round_price(raw, "up" if self.direction is Direction.LONG else "down")
            if spec.qty_is_dust(qty) or target <= 0:
                return
            if is_sl and price > 0 and self._is_breached(price, target):
                self.force_close("撤旧止损后已击穿新止损", EndReason.SL_FORCED)
                return
        if qty > min(spec.max_qty, spec.market_max_qty):
            self.force_close("旧周期仓量超过单张保护单上限", EndReason.SL_FORCED)
            return
        cid = self._new_oid(ex.role, 0)
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, 0, ex.role.value,
                               MartinLedger.A_INTENT_PLACE, cid, target, qty, "PENDING",
                               f"均价{self.book.avg:.8g}")
        ex.coid, ex.life = cid, Life.INTENT
        ex.target_price, ex.target_qty = target, qty
        ex.last_action_ts, ex.attempts = time.time(), ex.attempts + 1
        self.orders[cid] = UniOrder(coid=cid, status="UNKNOWN", amount=qty)
        res = (self.ctx.gw.place_stop_market(self.direction.close_side, qty, target, cid,
                                           self.direction.position_side, cfg.sl_working_type)
               if is_sl else self.ctx.gw.place_limit(self.direction.close_side, qty, target,
                                                   cid, self.direction.position_side))
        self._after_place(res, 0, ex.role, cid, target, qty, ex_ref=ex)


    def _exit_is_aligned(self, ex: ExitOrder, target_price: float, target_qty: float) -> bool:
        """
        防抖核心: 比较维度是【在线单的剩余量】而非订单总量。
        止盈部分成交后, 剩余量本就等于新的持仓量, 不应无谓撤单重挂（撤挂本身通常不收费，但会丢失排队优先级）。
        """
        if ex.life != Life.LIVE or not ex.coid:
            return False
        tick = self.ctx.spec.tick_size or 1e-12
        step = self.ctx.spec.step_size or 1e-12
        return (abs(ex.live_price - target_price) <= tick * 0.6 and
                abs(ex.live_remaining - target_qty) <= step * 0.6)

    # ---------------------- 记账 / 撤单 ----------------------
    def _observe(self, coid: str, o: UniOrder) -> float:
        p = OidCodec.parse(coid)
        if (not p or p.strategy_id != self.ctx.cfg.strategy_id or p.cycle_id != self.cycle_id
                or o.coid != coid):
            raise ReconcileError(f"订单归属不一致: {coid}")
        if coid in self.final_orders:
            return 0.0  # 终态之后的旧快照不能使订单复活
        expected_side = self.direction.open_side if p.role is OrderRole.OPEN else self.direction.close_side
        raw_side = str(o.raw.get("positionSide") or "").upper()
        if (o.side and o.side != expected_side) or (raw_side and raw_side != self.direction.position_side):
            raise ReconcileError(f"订单方向/持仓方向不匹配: {coid}")
        filled = float(o.filled)
        if not math.isfinite(filled) or filled < 0:
            raise ReconcileError("成交数量非法")
        prev = self.acked.get(coid, 0.0)
        delta = filled - prev
        eps = max(1e-12, self.ctx.spec.step_size * 1e-6)
        if delta < -eps:
            return 0.0  # 旧的累计快照，不覆盖更新状态
        if delta > eps:
            if not math.isfinite(o.avg_price) or o.avg_price <= 0:
                logger.warning(f"[成交] 成交价字段尚未齐全，保留OID点查，不猜测成交成本: {coid}")
                self.orders[coid] = UniOrder(coid=coid, status="UNKNOWN", amount=o.amount, filled=prev)
                return 0.0
            cum_cost = o.avg_price * filled
            marginal_price = (cum_cost - self.order_cum_cost.get(coid, 0.0)) / delta
            if not math.isfinite(marginal_price) or marginal_price <= 0:
                raise ReconcileError(f"累计成交金额矛盾: {coid}")
            if p.role is not OrderRole.OPEN and delta > self.book.open_qty + eps:
                raise ReconcileError(f"平仓成交超过已知开仓，不能静默截断: {coid}")
            # 先持久化成交事件，再修改内存；写失败时acked仍保持原值。
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, p.layer, p.role.value,
                                   MartinLedger.A_FILL, coid, marginal_price, delta, "OK",
                                   json.dumps({"cum_qty": filled, "cum_cost": cum_cost,
                                               "exchange_ts": o.ts}, separators=(",", ":")))
            if p.role is OrderRole.OPEN:
                self.book.add_open(marginal_price, delta)
                self.first_fill_ts = self.first_fill_ts or time.time()
            else:
                self.book.add_close(marginal_price, delta)
                if p.role is OrderRole.SL and p.layer in (98, 99):
                    self.force_attempts = 0  # 只有确有平仓进展才重置市价失败计数
            self.acked[coid], self.order_cum_cost[coid] = filled, cum_cost
        self.orders[coid] = o
        if o.is_terminal:
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, p.layer, p.role.value,
                                   MartinLedger.A_ORDER_FINAL, coid, o.avg_price, filled, o.status,
                                   json.dumps({"amount": o.amount, "ex_id": o.ex_id}, separators=(",", ":")))
            self.final_orders.add(coid)
        return max(0.0, delta)
    def _cancel(self, coid: str, why: str) -> bool:
        if coid in self.final_orders:
            return True
        p = OidCodec.parse(coid)
        if not p or p.strategy_id != self.ctx.cfg.strategy_id or p.cycle_id != self.cycle_id:
            raise ReconcileError(f"拒绝撤销归属不明订单: {coid}")
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, p.layer, p.role.value,
                               MartinLedger.A_INTENT_CANCEL, coid, 0, 0, "PENDING", why)
        self.ctx.gw.cancel(coid)
        # 无论撤单回执成功/失败，都点查；撤单失败也可能意味着订单刚好全成。
        o = self.ctx.gw.fetch_order(coid)
        if o is not None:
            self._observe(coid, o)
        ok = coid in self.final_orders
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, p.layer, p.role.value,
                               MartinLedger.A_CANCEL_OK if ok else MartinLedger.A_CANCEL_FAIL,
                               coid, 0, 0, "OK" if ok else "UNKNOWN", why)
        if not ok:
            # 保留OID及可能剩余量；未来统一对账持续查询，撤单ACK不能释放委托额度。
            prior = self.orders.get(coid)
            self.orders[coid] = UniOrder(coid=coid, status="UNKNOWN",
                                        amount=prior.amount if prior else 0,
                                        filled=self.acked.get(coid, 0))
        return ok

    def _cancel_all_working(self):
        ok = True
        for cid in list(self.orders):
            if cid in self.final_orders or cid == self.forced_close_coid:
                continue
            if not self._cancel(cid, "周期收尾/强平前最终确认"):
                ok = False
        return ok


    def _cancel_open_layers(self, why: str):
        for cid in list(self.orders):
            p = OidCodec.parse(cid)
            if p.role is OrderRole.OPEN and cid not in self.final_orders:
                self._cancel(cid, why)
        for lp in self.bp.layers:
            if lp.coid in self.final_orders:
                lp.life = Life.FILLED if self.orders[lp.coid].status == "FILLED" else Life.DEAD

    def suspend_add(self, why: str):
        if not self.add_suspended:
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, -1, "-",
                                   MartinLedger.A_ALERT, status="SUSPEND_ADD", msg=why)
            self.add_suspended = True
            logger.critical(f"[降级] 停止加仓，持续维护保护与撤单确认: {why}")
        # 不能一次失败后因为 add_suspended 已设置就再也不撤。
        self._cancel_open_layers(why)

    # ---------------------- 序列化 ----------------------
    def start_meta(self) -> dict:
        return {
            "schema": 2, "created_ts": self.created_ts,
            "symbol": self.ctx.cfg.symbol, "strategy_id": self.ctx.cfg.strategy_id,
            "config": dict(vars(self.ctx.cfg)),
            "sig_ts": self.signal_ts, "dir": self.direction.value,
            "base": self.bp.base_price, "step_pct": self.ctx.cfg.step_pct,
            "mult": self.ctx.cfg.qty_mult, "tp_pct": self.ctx.cfg.tp_pct,
            "max_loss": self.ctx.cfg.max_loss_usdt, "mode": self.ctx.cfg.step_mode,
            "layers": self.bp.to_json_layers(),
        }


# ==============================================================================
# 10. 引擎主循环 (全系统唯一写者)
# ==============================================================================
class MartinEngine:
    def __init__(self, cfg: MartinConfig, gw: ExchangeGateway, ledger: MartinLedger):
        self.cfg = cfg
        self.gw = gw
        self.ledger = ledger
        self.spec: Optional[InstrumentSpec] = None
        self.gate = SignalGate(cfg)
        self.state = EngineState.IDLE
        self.cycle: Optional[MartinCycle] = None
        self.last_price = 0.0
        self.cooldown_until = 0.0
        self.err_streak = 0
        self.stop_flag = False
        self._last_sync_ts = time.monotonic()
        self.cycles_done = 0
        self.pnl_total = 0.0

    # ---------------- 启动 ----------------
    def boot(self) -> bool:
        self.cfg.validate()
        try:
            self.spec = self.gw.load_instrument()
            if self.spec is None or self.gw.is_hedge_mode() is not True:
                logger.critical("[启动] 规格/双向持仓模式未得到明确确认，拒绝启动")
                return False
            meta, rows, watermark = self.ledger.load_state()
            self.gate.set_watermark(watermark)
            if meta:
                return self._recover_cycle(meta, rows)
            snap = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
            if snap is None or not self.gw.snapshot_complete or snap:
                logger.critical("[启动] 无活动账本但盘口有残单或查询不完整；保留现场，拒绝空闲启动")
                return False
            self.state = EngineState.IDLE
            return True
        except (LedgerError, ReconcileError) as e:
            self.state = EngineState.STOPPED
            logger.critical(f"[启动] 保留现场，未进行清场: {e}")
            return False

    def _recover_cycle(self, meta: dict, rows: List[dict]) -> bool:
        """按WAL应用顺序重放成交，再只追加尚未记账的交易所成交增量。"""
        try:
            if meta.get("schema") != 2:
                raise ReconcileError("活动周期使用旧WAL格式；缺少累计成交边界，需先人工核对迁移，禁止猜测恢复")
            if meta.get("symbol") != self.cfg.symbol or meta.get("strategy_id") != self.cfg.strategy_id:
                raise ReconcileError("活动周期的交易对/策略ID与当前配置不符")
            active_cfg = MartinConfig(**meta["config"])
            active_cfg.validate()
            layers = [LayerPlan(int(x["l"]), float(x["p"]), float(x["q"])) for x in meta["layers"]]
            if [x.layer for x in layers] != list(range(len(layers))):
                raise ReconcileError("蓝图层编号不连续")
            if any(not math.isfinite(x.price) or not math.isfinite(x.qty)
                   or x.price <= 0 or x.qty <= 0 for x in layers):
                raise ReconcileError("蓝图价格/数量非法")
            direction = Direction(meta["dir"])
            bp = Blueprint(direction, layers, float(meta["base"]))
            cyc = MartinCycle(CycleCtx(active_cfg, self.gw, self.ledger, self.spec),
                              meta["cycle_id"], int(meta["sig_ts"]), direction, bp)
            cyc.created_ts = float(meta["created_ts"])
            for r in rows:
                action, cid = r["action"], r["coid"]
                allowed_actions = {getattr(MartinLedger, k) for k in vars(MartinLedger) if k.startswith("A_")}
                if action not in allowed_actions:
                    raise ReconcileError(f"未知账本动作: {action}")
                p = OidCodec.parse(cid) if cid else None
                if cid and (not p or p.strategy_id != self.cfg.strategy_id or p.cycle_id != cyc.cycle_id):
                    raise ReconcileError("WAL内订单归属不符")
                if action == MartinLedger.A_INTENT_PLACE:
                    if cid in cyc.orders:
                        raise ReconcileError("同一OID存在重复下单意图")
                    qty, price = float(r["qty"]), float(r["price"])
                    cyc.orders[cid] = UniOrder(coid=cid, status="UNKNOWN", amount=qty,
                                              price=price if p.role is not OrderRole.SL else 0,
                                              stop_price=price if p.role is OrderRole.SL else 0)
                    if p.role is OrderRole.OPEN:
                        lp = bp.layers[p.layer]
                        lp.coid, lp.life = cid, Life.UNKNOWN
                        lp.attempts += 1
                    elif p.layer == 0:
                        ex = cyc.tp if p.role is OrderRole.TP else cyc.sl
                        ex.coid, ex.life = cid, Life.UNKNOWN
                        ex.attempts += 1
                    elif p.role is OrderRole.SL and p.layer in (98, 99):
                        cyc.forced_close_coid, cyc.forced_close_sent = cid, True
                        cyc.force_attempts += 1
                elif action == MartinLedger.A_FILL:
                    data = json.loads(r["msg"])
                    prev = cyc.acked.get(cid, 0.0)
                    cum = float(data["cum_qty"])
                    qty, price = float(r["qty"]), float(r["price"])
                    cost = float(data["cum_cost"])
                    if (not all(math.isfinite(x) for x in (cum, qty, price, cost))
                            or qty <= 0 or price <= 0
                            or abs(cum - prev - qty) > max(1e-12, self.spec.step_size * 1e-6)
                            or abs(cost - cyc.order_cum_cost.get(cid, 0) - qty * price) > max(1e-8, abs(cost) * 1e-10)):
                        raise ReconcileError("成交事件累计量/金额不连续")
                    if cid not in cyc.orders:
                        # 被发现的同周期孤儿单也会先记录成交，再确认终态。
                        cyc.orders[cid] = UniOrder(coid=cid, status="UNKNOWN")
                    if p.role is OrderRole.OPEN:
                        cyc.book.add_open(price, qty)
                        cyc.first_fill_ts = cyc.first_fill_ts or cyc.created_ts
                    else:
                        if qty > cyc.book.open_qty + self.spec.step_size * 1e-6:
                            raise ReconcileError("WAL平仓量超过开仓量")
                        cyc.book.add_close(price, qty)
                        if p.role is OrderRole.SL and p.layer in (98, 99):
                            cyc.force_attempts = 0
                    cyc.acked[cid], cyc.order_cum_cost[cid] = cum, cost
                elif action == MartinLedger.A_ORDER_FINAL:
                    data = json.loads(r["msg"])
                    if abs(float(r["qty"]) - cyc.acked.get(cid, 0)) > self.spec.step_size * 1e-6:
                        raise ReconcileError("终态成交量与已记账量不一致")
                    o = UniOrder(coid=cid, ex_id=data.get("ex_id", ""), status=r["status"],
                                 amount=float(data["amount"]), filled=float(r["qty"]),
                                 avg_price=float(r["price"]))
                    if not o.is_terminal:
                        raise ReconcileError("ORDER_FINAL 含非终态")
                    cyc.orders[cid] = o
                    cyc.final_orders.add(cid)
                elif action == MartinLedger.A_PLACE_FAIL:
                    # 明确拒单已落盘，崩溃在ORDER_FINAL之前也无需无限点查不存在的单。
                    prior = cyc.orders[cid]
                    cyc.orders[cid] = UniOrder(coid=cid, status="REJECTED", amount=prior.amount)
                    cyc.final_orders.add(cid)
                    holder = (bp.layers[p.layer] if p.role is OrderRole.OPEN else
                              cyc.tp if p.role is OrderRole.TP else cyc.sl if p.layer == 0 else None)
                    if holder is not None:
                        if r["status"] in (ErrKind.INVALID.value, ErrKind.FATAL.value):
                            holder.attempts = MAX_PLACE_ATTEMPTS
                        holder.next_retry_ts = time.time() + (3.0 if p.role is not OrderRole.OPEN else
                            max(30 if r["status"] == ErrKind.INSUFFICIENT.value else 0,
                                RETRY_BACKOFF_SEC[min(max(0, holder.attempts - 1), len(RETRY_BACKOFF_SEC)-1)]))
                elif action == MartinLedger.A_PLACE_OK and p:
                    if p.role in (OrderRole.SL, OrderRole.TP) and p.layer == 0:
                        (cyc.sl if p.role is OrderRole.SL else cyc.tp).attempts = 0
                elif action == MartinLedger.A_ALERT and r["status"] == "SUSPEND_ADD":
                    cyc.add_suspended = True
                elif action == MartinLedger.A_CYCLE_CLOSING:
                    cyc.end_reason = EndReason(r["status"])
                    cyc.add_suspended = cyc.closing_recorded = True
            self.cycle = cyc
            snapshot = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
            cyc._reconcile(snapshot or {})  # 快照失败也只对已知OID点查，不降级为空闲
            for lp in bp.layers:
                cyc._maintain_layer(lp, snapshot or {}, 0, time.time())
            cyc._sync_exit(cyc.tp, snapshot or {}, time.time())
            cyc._sync_exit(cyc.sl, snapshot or {}, time.time())
            self.state = (EngineState.TEARDOWN if cyc.end_reason else EngineState.SUSPEND_ADD
                          if cyc.add_suspended else EngineState.ACTIVE)
            logger.info(f"[恢复] 周期{cyc.cycle_id}持仓{cyc.book.open_qty} 均价{cyc.book.avg} 状态{self.state.value}")
            return True
        except Exception as e:
            self.state = EngineState.STOPPED
            logger.critical(f"[恢复] 失败，保留现场及交易所保护单，拒绝新周期: {e}", exc_info=True)
            return False

    def _purge_strategy_orders(self, why: str) -> bool:
        """有完整周期归属时才清场；空快照不足以证明未知订单不存在。"""
        if self.cycle is None:
            return False
        cyc = self.cycle
        snapshot = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        cyc._reconcile(snapshot or {})
        canceled = cyc._cancel_all_working()
        if snapshot is None or not self.gw.snapshot_complete or not canceled:
            return False
        final_snap = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        if final_snap is None or not self.gw.snapshot_complete:
            return False
        cyc._reconcile(final_snap)
        return not final_snap and all(cid in cyc.final_orders for cid in cyc.orders)

    # ---------------- 主循环 ----------------
    def run_forever(self):
        logger.info(f"[主循环] 择时马丁引擎启动(全系统唯一写者) | 策略:[{self.cfg.strategy_id}] "
                    f"交易对:[{self.cfg.symbol}] 信号:[{self.cfg.signal_name}]")
        while not self.stop_flag:
            try:
                if self.state == EngineState.STOPPED:
                    logger.critical("[主循环] 引擎处于 STOPPED 态, 已停止一切交易, 等待人工介入")
                    time.sleep(60)
                    continue
                if self.state == EngineState.IDLE:
                    self._idle_step()
                elif self.state in (EngineState.ACTIVE, EngineState.SUSPEND_ADD):
                    self._active_step()
                elif self.state == EngineState.TEARDOWN:
                    self._teardown_step()
                self.err_streak = 0
                if time.monotonic() - self._last_sync_ts >= 3600:
                    self._last_sync_ts = time.monotonic()
                    exchange = getattr(self.gw, "ex", None)
                    if exchange is not None:
                        exchange.load_time_difference()  # 与交易API串行，避免同一session跨线程使用
            except LedgerError as e:
                logger.critical(f"[主循环] 账本不可用，停止交易写操作并保留现场: {e}")
                self.state = EngineState.STOPPED
            except ReconcileError as e:
                logger.critical(f"[主循环] 成交/归属证据矛盾，停止并保留现场: {e}")
                self.state = EngineState.STOPPED
            except Exception as e:
                self.err_streak += 1
                logger.error(f"[主循环] 第[{self.err_streak}]次连续异常(状态不变, 下一轮重试) | 错误:[{e}]",
                             exc_info=True)
                if self.err_streak >= MAX_CONSECUTIVE_ERRORS:
                    logger.critical(f"[主循环] 连续异常达[{self.err_streak}]次, 转入保守收尾模式")
                    if self.cycle:
                        self.cycle.suspend_add("主循环连续异常")
                        self.state = EngineState.TEARDOWN if self.cycle.end_reason else EngineState.SUSPEND_ADD
                    else:
                        self.state = EngineState.STOPPED
                    self.err_streak = 0
                time.sleep(3)
        if self.cycle and self.state is not EngineState.STOPPED:
            try:
                self.cycle.stop_requested = True
                self.cycle.suspend_add("进程退出：撤销剩余加仓单，保留现有TP/SL")
                self._active_step()
            except Exception:
                logger.critical("[退出] 最后对账未完成；交易所仍可能有加仓单，需人工检查", exc_info=True)
        logger.info("[主循环] 已退出；未确认订单保持WAL记录，下次启动接管")

    # ---------------- IDLE ----------------
    def _idle_step(self):
        time.sleep(self.cfg.idle_poll_interval_sec)
        if time.time() < self.cooldown_until:
            return
        price = self.gw.fetch_last_price()
        if price is None:
            return
        self.last_price = price

        prior_watermark = self.gate.watermark_ts
        sig = self.gate.poll(price)
        if self.gate.watermark_ts > prior_watermark:
            self.ledger.append("-", self.gate.watermark_ts, -1, "-", MartinLedger.A_SIGNAL_CONSUMED)
        if sig is None:
            return
        logger.info(f"[信号] 收到有效开仓信号 {sig} | 现价:[{price}]")

        # 开仓前最后一道清场: 空闲态不应存在任何本策略挂单
        snap = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        if snap is None or not self.gw.snapshot_complete:
            logger.info("[信号] 无法确认盘口干净度, 本次放弃开仓(信息不全不动手)")
            return
        if snap:
            logger.critical(f"[信号] 空闲态却发现[{len(snap)}]张本策略残留挂单, 先清场再考虑开仓, "
                            f"本次信号放弃")
            self.state = EngineState.STOPPED
            logger.critical("[信号] 无活动账本不能确认残单成交归属；停止并保留现场")
            self.gate.set_watermark(sig.signal_ts)
            return

        bp = BlueprintBuilder.build(self.cfg, self.spec, sig)
        self.ledger.append("-", sig.signal_ts, -1, "-", MartinLedger.A_SIGNAL_CONSUMED)
        self.gate.set_watermark(sig.signal_ts)     # 无论是否成功, 该信号只消费一次
        if bp is None:
            return

        cycle_id = OidCodec.cycle_id_of(sig.signal_ts)
        ctx = CycleCtx(self.cfg, self.gw, self.ledger, self.spec)
        cyc = MartinCycle(ctx, cycle_id, sig.signal_ts, sig.direction, bp)
        # WAL: 先把完整蓝图落账(含 JSON), 再铺单。崩溃后靠这一行 100% 还原蓝图
        self.ledger.append(cycle_id, sig.signal_ts, -1, "-", MartinLedger.A_CYCLE_START,
                           "", bp.base_price, bp.total_qty, "OK",
                           json.dumps(cyc.start_meta(), separators=(",", ":")))
        self.cycle = cyc
        self.state = EngineState.ACTIVE
        logger.info(f"[周期] 开启新周期[{cycle_id}] {sig.direction.value} | 层数:[{len(bp.layers)}] "
                    f"总量:[{bp.total_qty:.8g}] 最大名义:[{bp.total_notional:.2f}U] | 开始全量铺单")
        cyc.place_all_layers(price)
        if cyc.end_reason:
            self.state = EngineState.TEARDOWN

    # ---------------- ACTIVE ----------------
    def _position_qty(self) -> Optional[float]:
        if self.cycle is None:
            return None
        # 平仓量安全校验必须使用本轮查询，不能用加仓前缓存的0仓位。
        return self.gw.fetch_position_qty(self.cycle.direction.position_side)

    def _active_step(self):
        time.sleep(self.cfg.poll_interval_sec)
        cyc = self.cycle
        if cyc is None:
            raise ReconcileError("活动状态缺少周期对象")
        price = self.gw.fetch_last_price()
        if price is not None:
            self.last_price = price
        cyc.risk_price = self.gw.fetch_risk_price(cyc.ctx.cfg.sl_working_type)
        snapshot = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        # 行情或快照缺失不阻断对已知OID的成交对账；只有新开仓需要完整信息。
        if price is None:
            self.gw.snapshot_complete = False
        reason = cyc.maintain(snapshot or {}, price or 0.0, self._position_qty())
        if cyc.add_suspended:
            self.state = EngineState.SUSPEND_ADD
        if reason:
            self.state = EngineState.TEARDOWN

    # ---------------- TEARDOWN ----------------
    def _teardown_step(self):
        cyc = self.cycle
        if cyc is None:
            raise ReconcileError("收尾状态缺少周期对象")
        reason = cyc.end_reason or EndReason.MANUAL_FLAT
        cyc.begin_teardown(reason)
        # force_close持续追踪唯一强平OID；撤单失败/结果未知时绝不换号重发。
        cyc.force_close("周期收尾残余平仓", reason)
        cleaned = self._purge_strategy_orders(f"周期{cyc.cycle_id}收尾")
        if not cleaned or cyc.book.open_qty > 1e-12:
            logger.warning(f"[清理] 等待全部订单终态及虚拟仓位归零；剩余{cyc.book.open_qty}")
            time.sleep(2.0)
            return
        snap = cyc.book.snapshot()
        snap.update(reason=reason.value, cleaned=True,
                    layers_filled=sum(1 for l in cyc.bp.layers if l.life == Life.FILLED),
                    layers_total=len(cyc.bp.layers), duration_sec=round(time.time() - cyc.created_ts, 1))
        self.ledger.append(cyc.cycle_id, cyc.signal_ts, -1, "-", MartinLedger.A_CYCLE_END,
                           status=reason.value, msg=json.dumps(snap, separators=(",", ":")))
        self.cycles_done += 1
        self.pnl_total += cyc.book.realized
        self.cycle = None
        self.cooldown_until = time.time() + self.cfg.cooldown_sec
        self.state = EngineState.IDLE
        logger.info(f"[周期] 结束 {reason.value}，毛盈亏{snap['realized']:+.4f}，冷却后接收新信号")

# ==============================================================================
# 11. 只读看板线程（校时已移至主线程）
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
            filled = sum(1 for l in c.bp.layers if l.life == Life.FILLED)
            live = sum(1 for l in c.bp.layers if l.life == Life.LIVE)
            defer = sum(1 for l in c.bp.layers if l.life == Life.DEFERRED)
            active_cfg = c.ctx.cfg
            slp = c.book.sl_price(active_cfg.max_loss_usdt)
            tpp = c.book.tp_price(active_cfg.tp_pct, active_cfg.fee_pct_per_side)
            lines += [
                f" 🔁 周期:[{c.cycle_id}] 方向:[{c.direction.value}] "
                f"加仓层:[成交{filled}/在挂{live}/暂缓{defer}/共{len(c.bp.layers)}]"
                + ("  ⚠️已降级停止加仓" if c.add_suspended else ""),
                f" 💰 虚拟持仓:[{c.book.open_qty:.8g}] 均价:[{c.book.avg:.8g}] "
                f"浮亏盈:[{c.book.unrealized(e.last_price):+.4f}U] 已实现:[{c.book.realized:+.4f}U]",
                f" 🎯 止盈价:[{tpp:.8g}]({active_cfg.tp_pct}%) 🛑 止损价:[{slp:.8g}] "
                f"(最大亏损{active_cfg.max_loss_usdt}U) | TP:[{c.tp.life.value}] SL:[{c.sl.life.value}]",
            ]
            if e.last_price > 0 and slp > 0:
                d = abs(e.last_price / slp - 1) * 100
                lines.append(f" 📏 现价距止损:[{d:.3f}%] 距止盈:[{abs(e.last_price / tpp - 1) * 100:.3f}%]")
        lines.append("=========================================================\n")
        logger.info("\n".join(lines))




# ==============================================================================
# 12. 进程编排
# ==============================================================================
def run_single_strategy(cfg: MartinConfig):
    """子进程入口: 独立日志 -> 单实例锁 -> 组装 -> 退出看门狗 -> 冷启动 -> 主循环。"""
    safe_symbol = cfg.symbol.replace("/", "_").replace(":", "_")
    setup_logger(app_name=f"MT_{cfg.strategy_id}_{safe_symbol}", force_reset=True)
    logging.getLogger().info(f"[进程] 子进程日志就绪 | 策略:[{cfg.strategy_id}] "
                             f"交易对:[{cfg.symbol}] 信号:[{cfg.signal_name}]")

    # 单实例锁: 同一 strategy_id 绝不允许两个进程同时跑(否则双写账本 + 重复下单)
    lock_path = f"martin_{cfg.strategy_id}.lock"
    try:
        lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR)
        if platform.system().lower() != "windows":
            import fcntl
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            import msvcrt
            if os.fstat(lock_fd).st_size == 0:
                os.write(lock_fd, b" ")
            os.lseek(lock_fd, 0, os.SEEK_SET)
            msvcrt.locking(lock_fd, msvcrt.LK_NBLCK, 1)
        os.lseek(lock_fd, 0, os.SEEK_SET)
        os.write(lock_fd, str(os.getpid()).encode())
        os.ftruncate(lock_fd, os.lseek(lock_fd, 0, os.SEEK_CUR))
    except Exception as e:
        logger.critical(f"[进程] 获取单实例锁失败, 疑有同名策略正在运行, 拒绝启动 | "
                        f"策略:[{cfg.strategy_id}] 错误:[{e}]")
        return

    parent_pid = os.getppid()

    api_key = get_config("myself_biance_api_key")
    secret_key = get_config("myself_biance_api_secret")
    proxies = None if platform.system().lower() == "linux" else {
        "http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
    exchange = safe_init_exchange(api_key, secret_key, proxies)

    gw = BinanceGateway(exchange, cfg.symbol)          # 本实现限定 Binance USDT 线性合约
    ledger = MartinLedger(cfg.strategy_id)
    engine = MartinEngine(cfg, gw, ledger)

    def _parent_watchdog():
        while not engine.stop_flag:
            if os.getppid() != parent_pid:
                engine.stop_flag = True
                if engine.cycle:
                    engine.cycle.stop_requested = True
                return
            time.sleep(2)
    threading.Thread(target=_parent_watchdog, daemon=True).start()

    def _on_term(signum, frame):
        logger.critical(f"[进程] 收到信号[{signum}], 优雅退出(不平仓, 保留交易所止盈止损单)")
        engine.stop_flag = True
        if engine.cycle:
            engine.cycle.stop_requested = True
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
        MartinConfig(
            strategy_id="B1", symbol="BTC/USDT:USDT", signal_name="get_signal_1",
            first_qty=0.002, step_pct=2.0, qty_mult=2.0, tp_pct=0.8,
            max_loss_usdt=50, max_layers=8, layer_loss_budget_ratio=0.8,
            allowed_directions=("LONG", "SHORT"),
        ),
        # ── 同一个币, 不同信号 + 不同马丁参数, strategy_id 隔离账本；同账户同向仓位共享，实盘须自行隔离账户 ──
        MartinConfig(
            strategy_id="B2", symbol="BTC/USDT:USDT", signal_name="get_signal_2",
            first_notional=20, step_pct=1.2, qty_mult=1.8, tp_pct=0.5,
            max_loss_usdt=30, max_layers=10, allowed_directions=("LONG",),
        ),
        # ── 另一个币 ──
        MartinConfig(
            strategy_id="U1", symbol="UNI/USDT:USDT", signal_name="get_signal_3",
            first_qty=1, step_pct=2.5, qty_mult=2.0, tp_pct=1.0,
            max_loss_usdt=40, max_layers=7, allowed_directions=("SHORT",),
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
        p.daemon = False  # 允许子进程执行撤加仓与最后对账
        p.start()
        procs.append(p)
        logger.info(f"[系统] 已拉起策略进程 | 策略:[{c.strategy_id}] 交易对:[{c.symbol}] "
                    f"信号:[{c.signal_name}] PID:[{p.pid}]")
    logger.info(f"[系统] 全部进程启动完毕, 主进程进入守护模式 | 进程数:[{len(procs)}]")
    try:
        for p in procs:
            p.join()
    except (KeyboardInterrupt, SystemExit):
        for p in procs:
            if p.is_alive():
                p.terminate()
        for p in procs:
            p.join(timeout=30)


# ==============================================================================
# 13. 运维工具 (人工排障用, 与主流程解耦)
# ==============================================================================
def admin_inspect(exchange, symbol, strategy_id=None):
    """排查盘口: 按策略前缀归类, 检出重复层单与非本系统孤儿单。"""
    gw = BinanceGateway(exchange, symbol)
    snap = gw.fetch_open_orders("")
    if snap is None:
        logger.error("[诊断] 订单快照不可用")
        return
    logger.info(f"[诊断] 普通单与算法单快照完整={gw.snapshot_complete}")
    orders = [{"clientOrderId": o.coid, "price": o.stop_price or o.price,
               "id": o.ex_id} for o in snap.values()]
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
    """人工紧急撤单（含止损）；调用者须先停引擎，之后核对成交及持仓。"""
    gw = BinanceGateway(exchange, symbol)
    snapshot = gw.fetch_open_orders(OidCodec.strategy_prefix(strategy_id))
    if snapshot is None:
        logger.error("[紧急清场] 无法查询订单")
        return
    for cid in snapshot:
        gw.cancel(cid)
        final = gw.fetch_order(cid)
        logger.info(f"[紧急清场] {cid}: {final.status if final else 'UNKNOWN'}；请通过账本恢复补记成交")


if __name__ == "__main__":
    main_app()
