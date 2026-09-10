# -*- coding: utf-8 -*-
"""
================================================================================
择时马丁交易引擎 (信号驱动 + 单写者串行状态机 + WAL账本 + 幂等成交记账)
================================================================================
[功能摘要]
  为每个 MartinConfig 拉起独立子进程。空闲态轮询外部择时信号(get_signal_x);
  一旦拿到有效开仓信号, 依据【加仓间距/加仓倍数/最大亏损金额】推算出完整马丁蓝图
  (层数由 max_loss_usdt 唯一决定), 一次性把所有层的限价开仓单铺到盘口; 随后串行轮询维护:
  价格全程静态固化(开仓价/每层止盈价/全局止损价一次算死), 数量动态跟随实际持仓;
  止盈成交 / 止损成交 / 兜底强平 / 入场超时 任一发生 -> 清算收尾 -> 回到空闲态。

[核心不变量 (代码中反复校验)]
  I1 记账唯一来源: 虚拟持仓 = Σ(本策略 client_oid 的成交增量)。永不用交易所仓位算均价,
     因为双向持仓模式下同 symbol 的仓位是全账户共享的, 必被其它策略/手工单污染。
  I2 幂等入账     : acked[coid] 记录"已入账成交量", 增量入账, 重复观测天然 no-op。
  I3 单一出场     : 同一时刻止盈 1 张、止损 1 张, 数量恒等于 min(虚拟持仓, 交易所实际持仓),
     绝不使用 closePosition(会连别人的仓位一起平掉)。
  I4 总量封顶     : Σ开仓成交 <= 蓝图总量 * 容差, 越界立即停止加仓, 只留止盈止损收尾。
  I5 价格刚性     : 均价/止盈价/止损价只由"实际加仓(OPEN)"决定; 任何 TP/SL 的部分成交
     绝不触发价格重算, 只触发数量对齐。

[输入]
  1. 静态配置 MartinConfig(策略ID/交易对/信号函数名/间距/倍数/止盈/最大亏损...)
  2. 外部信号 get_signal_x(symbol) -> DataFrame(timestamp/event/direction/price)
  3. 交易所实时: 现价、精度过滤器、在线挂单快照、单笔订单点查、实际持仓
  4. 本地账本 martin_ledger_{策略ID}.csv (WAL, 冷启动断点续传的唯一索引)

[输出]
  1. 交易所侧: 阶梯限价开仓单 + 唯一止盈限价单 + 唯一止损条件单
  2. 本地侧  : 追加式 CSV 领域事件账本 + 按进程隔离的日志文件
  3. 常驻进程, 无返回值

[并发安全]
  全系统只有主线程会修改状态(单一写者)。看板线程与校时线程只读, 不参与任何决策。

[前置条件]
  1. 合约账户必须为【双向持仓 / Hedge Mode】, 否则 positionSide 会被拒单;
  2. Hedge Mode 下禁止传 reduceOnly(会被拒), 平仓靠 side + positionSide 定向;
  3. 每个 strategy_id 必须全局唯一(它同时是账本名与 OID 命名空间), 严禁复用。
================================================================================
"""
import os
import csv
import re
import json
import time
import math
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
ORDER_GRACE_SEC = 4.0            # 新单冷静期: 期内不因"盘口查不到"判定掉单(容忍撮合与传播延迟)
CANCEL_CONFIRM_SEC = 1.5         # 撤单后多久开始点查确认终态(非阻塞, 交给下一轮主循环)
CANCEL_RELEASE_SEC = 20.0        # 撤单终态点查持续未知, 且盘口确认已无该单 -> 安全释放重挂
MAX_PLACE_ATTEMPTS = 5           # 单层挂单的最大尝试次数, 超出则永久 DEFERRED + 告警(防疯狂发单)
RETRY_BACKOFF_SEC = (2, 5, 15, 60, 300)   # 各次失败后的退避秒数(按尝试次数索引)
TEARDOWN_MAX_ROUND = 4           # 清理阶段最多轮询撤单几轮
QTY_EPS_RATIO = 1e-9             # 浮点比较用的极小量
OVERFILL_TOLERANCE = 1.02        # I4: 累计开仓成交 / 蓝图总量 的容忍上限
SL_BREACH_CONFIRM_SEC = 5.0      # 现价击穿止损价后, 等条件单自己触发的宽限时间, 超时则主动强平
POSITION_CACHE_SEC = 10.0        # 交易所真实仓位缓存时长(用于夹逼平仓量与告警)
MAX_CONSECUTIVE_ERRORS = 20      # 主循环连续异常次数上限
HARD_MAX_LAYERS = 50             # 【物理硬顶】纯防死循环底线; 真实层数由 max_loss_usdt 决定
FORCE_CLOSE_MAX_ATTEMPTS = 3     # 市价强平最大尝试次数, 超出转 STOPPED 等人工介入
SL_IMM_TRIG_MAX_DEV_PCT = 50.0   # "会立即触发"回执的本地核验: 止损价与现价偏离超此阈值判定为算错
IDLE_ERROR_SLEEP_SEC = (15.0, 30.0)  # IDLE 态连续异常的长休眠退避区间(绝不停机)
IDLE_NO_PRICE_SLEEP_SEC = 5.0    # IDLE 态拉不到现价时的额外退避(防断网高频空转刷屏)
RECOVER_RETRY_SEC = 10.0         # 断点续传接管失败后的重试间隔
RECOVER_MAX_ATTEMPTS = 30        # 接管重试上限, 超出转 STOPPED(但绝不清场)


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
    SL = "S"     # 止损(含兜底强平)


class Life(Enum):
    """本地跟踪的订单生命周期(与交易所状态解耦, 便于状态机推演)。"""
    NOT_PLACED = "NOT_PLACED"        # 尚未挂出 / 需要补挂
    INTENT = "INTENT"                # 已写 WAL 意图, 未收到回执
    UNKNOWN = "UNKNOWN"              # 请求结果未知(超时/重复OID), 必须点查裁决, 严禁换号重发
    LIVE = "LIVE"                    # 在盘口挂着
    CANCEL_PENDING = "CANCEL_PENDING"  # 撤单已受理但终态未确认(可能其实成交了), 待点查裁决
    FILLED = "FILLED"                # 完全成交
    DEAD = "DEAD"                    # 已撤销/拒单且不再补挂(终态)
    DEFERRED = "DEFERRED"            # 因资金/次数上限暂缓, 条件满足后重试


class EngineState(Enum):
    IDLE = "IDLE"                # 空闲监听信号
    RECOVER = "RECOVER"          # 断点续传接管中(有活仓, 绝不清场, 绝不开新仓)
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
    UNKNOWN_RESULT = "UNKNOWN_RESULT"  # 结果未知(网络中断/超时/回执缺失) -> 保留原OID点查, 绝不换号
    TRANSIENT = "TRANSIENT"          # 明确拒单(限频/时间戳) -> 退避重试
    PRICE_BAND = "PRICE_BAND"        # 价格离盘口太远 -> 退避后重挂
    IMMEDIATE_TRIGGER = "IMM_TRIG"   # 条件单会立即触发 -> 需本地现价双重核验
    INSUFFICIENT = "INSUFFICIENT"    # 保证金/余额不足 -> 退避 + 告警
    REDUCE_REJECT = "REDUCE_REJECT"  # 平仓数量超过持仓 -> 仓位被外部动过
    DUPLICATE = "DUPLICATE"          # OID 重复 -> 单子已存在, 转 UNKNOWN 点查
    INVALID = "INVALID"              # 精度/最小量等参数非法 -> 不可重试
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
      S_ID  策略短标识, <=8 位纯字母数字(启动时强校验)
      C_ID  周期流水号 = base36(信号毫秒时间戳) 【完整不截断, 杜绝命名空间循环冲突】
      ROLE  O=开仓/加仓  T=止盈  S=止损
      L_ID  层级 00~99 (T/S 用 00; 兜底强平用 99)
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
    -4015: ErrKind.DUPLICATE,
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

    @abstractmethod
    def load_instrument(self) -> Optional[InstrumentSpec]: ...
    @abstractmethod
    def fetch_last_price(self) -> Optional[float]: ...
    @abstractmethod
    def fetch_open_orders(self, coid_prefix: str) -> Optional[Dict[str, UniOrder]]: ...
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

    def fetch_open_orders(self, coid_prefix: str) -> Optional[Dict[str, UniOrder]]:
        """
        拉取在线挂单快照 (普通限价单 + 算法条件止损单)。
        【终极健壮版】：
        1. 普通订单是绝对生命线，失败则返回 None (信息不全不动手)。
        2. 算法条件单作为辅助增强，若偶发异常仅报警降级，绝不阻断主循环，杜绝假死！
        """
        out = {}

        # ---------------- 1. 普通挂单 (核心基础) ----------------
        try:
            self._throttle()
            orders = self.ex.fetch_open_orders(self.symbol)
            for o in orders or []:
                u = self._to_uni(o)
                if u.coid and u.coid.startswith(coid_prefix):
                    out[u.coid] = u
        except Exception as e:
            logger.error(f"[网关] 拉取普通在线挂单失败, 本轮跳过决策 | 错误:[{e}]")
            return None  # 普通单是核心，失败必须跳过

        # ---------------- 2. 算法条件单 (Algo Orders, 弹性增强) ----------------
        try:
            self._throttle()
            market_id = self.symbol.replace("/", "").split(":")[0]  # 兼容 "BTC/USDT:USDT" -> "BTCUSDT"
            algo_res = self.ex.fapiPrivateGetOpenAlgoOrders({"symbol": market_id})
            for a in algo_res or []:
                # 兼容币安不同 payload 的字段名
                coid = a.get("clientAlgoId") or a.get("clientOrderId") or ""
                if not coid or not coid.startswith(coid_prefix):
                    continue

                stop_px = float(a.get("triggerPrice") or a.get("stopPrice") or 0.0)
                amount = float(a.get("quantity") or a.get("origQty") or 0.0)
                filled = float(a.get("executedQty") or 0.0)
                eid = str(a.get("algoId") or a.get("orderId") or "")

                out[coid] = UniOrder(
                    coid=coid,
                    ex_id=eid,
                    status="OPEN",
                    price=0.0,
                    stop_price=stop_px,
                    amount=amount,
                    filled=filled,
                    avg_price=0.0,
                    side=str(a.get("side") or "").lower(),
                    ts=int(a.get("bookTime") or a.get("time") or 0),
                    raw=a,
                )
        except Exception as e:
            # 关键修改：算法单拉取失败只打印日志，不 return None！
            # 即使算法单暂时没查到，普通单依然正常处理，且本地有 _bottom_guard 软熔断兜底！
            logger.info(f"[网关] 拉取算法条件单接口异常(已安全降级，不影响主状态机): {e}")

        return out

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
        """用于夹逼平仓数量与告警, 绝不参与均价计算(双向持仓下该数字为全账户共享)。"""
        try:
            self._throttle()
            for p in self.ex.fetch_positions([self.symbol]) or []:
                info = p.get("info") or {}
                ps = str(info.get("positionSide") or p.get("side") or "").upper()
                if ps in (position_side, position_side.lower().upper()):
                    return abs(float(p.get("contracts") or info.get("positionAmt") or 0))
            return 0.0
        except Exception as e:
            logger.info(f"[网关] 拉取真实持仓失败(不影响核心逻辑, 本轮不做夹逼) | 错误:[{e}]")
            return None

    def is_hedge_mode(self) -> Optional[bool]:
        try:
            self._throttle()
            r = self.ex.fapiPrivateGetPositionSideDual()
            return bool(str(r.get("dualSidePosition")).lower() == "true")
        except Exception as e:
            logger.info(f"[网关] 无法确认持仓模式(跳过校验) | 错误:[{e}]")
            return None

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
        【修复】：1. 订单类型必须为 "STOP_MARKET"
                 2. 严格按精度格式化 stopPrice 与 amount
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

            # 核心修复：type 必须是 "STOP_MARKET"，绝不能是 "market"
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
        双轨自适应撤单。
        先尝试标准撤单；若提示查无此单(-2011/unknown order)，自动尝试算法单撤销。
        注意: 返回 True 仅代表"撤单请求已被受理", 不代表订单终态(可能刚好成交),
              终态必须由上层点查确认(CANCEL_PENDING -> 点查裁决)。
        """
        try:
            self._throttle()
            # 1. 尝试普通撤单
            self.ex.cancel_order(coid, self.symbol, {"origClientOrderId": coid})
            return True
        except Exception as e:
            msg = str(e).lower()
            # 单子本来就不存在/已终结，直接视为撤单成功(幂等)
            if any(k in msg for k in ("-2013", "order not found", "does not exist")):
                return True

            # 2. 如果普通接口提示 -2011 (Unknown order)，说明是未触发的算法条件单
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
                        return True  # 确实已经没有了，目标达成
                    logger.info(f"[网关] 算法条件单撤销亦失败(留待下一轮对账) | CID:[{coid}] 错误:[{algo_err}]")
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

    def __init__(self, strategy_id: str):
        self.filename = f"martin_ledger_{strategy_id}.csv"
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

    # ---------- 冷启动读取 ----------
    def load_state(self) -> Tuple[Optional[dict], List[dict], int]:
        """
        返回 (未结束周期的 CYCLE_START 元数据 or None, 该周期所有历史行, 全局最大信号时间戳)。
        全局最大信号时间戳作为信号去重水位线, 保证重启后不会重复消费旧信号。
        """
        rows: List[dict] = []
        if not os.path.exists(self.filename):
            return None, [], 0
        try:
            with open(self.filename, "r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception as e:
            logger.critical(f"[账本] 读取失败, 无法断点续传! 为安全起见将按空闲态启动 | 错误:[{e}]")
            return None, [], 0

        max_sig_ts = 0
        for r in rows:
            try:
                max_sig_ts = max(max_sig_ts, int(float(r.get("signal_ts") or 0)))
            except Exception:
                pass

        # 找最后一个 CYCLE_START, 并检查其后是否有同周期 CYCLE_END
        last_start_idx, last_cycle = -1, None
        for i, r in enumerate(rows):
            if r.get("action") == self.A_CYCLE_START:
                last_start_idx, last_cycle = i, r.get("cycle_id")
        if last_start_idx < 0:
            return None, [], max_sig_ts
        for r in rows[last_start_idx:]:
            if r.get("action") == self.A_CYCLE_END and r.get("cycle_id") == last_cycle:
                return None, [], max_sig_ts       # 已正常收尾

        meta_row = rows[last_start_idx]
        try:
            meta = json.loads(meta_row.get("msg") or "{}")
        except Exception:
            logger.critical("[账本] 周期蓝图 JSON 解析失败, 无法续传该周期")
            meta = {}
        meta["cycle_id"] = meta_row.get("cycle_id")
        cycle_rows = [r for r in rows[last_start_idx:] if r.get("cycle_id") == last_cycle]
        return meta, cycle_rows, max_sig_ts


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
                 #   想提高资金利用率就调到 0.85~0.90, 绝不要设成 1。
                 max_signal_age_sec=90,
                 entry_timeout_sec=900,                # 入场超时: 一手未成则作废周期
                 max_cycle_sec=0,                      # 0=不限, 周期总超时强平
                 poll_interval_sec=2.0,
                 idle_poll_interval_sec=5.0,
                 sl_working_type="MARK_PRICE",         # MARK_PRICE 防插针 / CONTRACT_PRICE 更灵敏
                 clamp_exit_by_position=True):         # 用交易所真实仓位夹逼平仓量, 防 -2022
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
        self.clamp_exit_by_position = bool(clamp_exit_by_position)

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
    极简版信号闸门：专为干净标准化的 DataFrame 设计
    明确期望字段: timestamp(ms), event(OPEN/CLOSE), direction(LONG/SHORT), price(float)
    收到的 direction 是什么就用什么, 不做方向白名单, 不做偏离校验。
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

            # 1. 取最后一行信号
            row = df.iloc[-1]

            # 2. 只处理开仓信号 (马丁策略依靠内部止盈止损平仓，忽略外部 CLOSE)
            if str(row['event']).upper() != "OPEN":
                return None

            # 3. 提取方向 (只认 LONG / SHORT)
            direction_str = str(row['direction']).upper()
            if direction_str == "LONG":
                direction = Direction.LONG
            elif direction_str == "SHORT":
                direction = Direction.SHORT
            else:
                return None

            # 4. 提取价格和时间戳
            px = float(row['price'])
            ts = int(row['timestamp'])

            # 5. 风控校验：去重与过期作废
            if ts <= self.watermark_ts:
                return None  # 老信号，静默跳过

            age_sec = (time.time() * 1000 - ts) / 1000.0
            if age_sec > self.cfg.max_signal_age_sec:
                logger.info(f"[信号] 信号已过期，拒绝追单 | 滞后:[{age_sec:.1f}s]")
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
    __slots__ = ("layer", "price", "qty", "avg", "tp", "coid", "ex_id", "life",
                 "attempts", "next_retry_ts", "last_action_ts")

    def __init__(self, layer, price, qty, avg=0.0, tp=0.0):
        self.layer = layer
        self.price = float(price)
        self.qty = float(qty)
        self.avg = float(avg)          # 该层满仓后的【理论持仓均价】(一次算死, 永不重算)
        self.tp = float(tp)            # 该层对应的【固定止盈价】(一次算死, 永不重算)
        self.coid = ""
        self.ex_id = ""
        self.life = Life.NOT_PLACED
        self.attempts = 0
        self.next_retry_ts = 0.0
        self.last_action_ts = 0.0

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
      ratio 必须 < 1: 由 P_sl - P_last = sign*(max_loss - loss_last)/Q_full 可知,
      若让 loss_last 顶到 max_loss, 止损价就会贴死在最深层成交价上, 最后一仓刚成交即被扫止损。
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
                            f"[{budget:.2f}U = 最大亏损{cfg.max_loss_usdt}×{cfg.layer_loss_budget_ratio}], "
                            f"层数在此收口(本周期只铺{i}层)")
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
# 8. 虚拟仓位账 (I1: 唯一记账来源 / I5: 价格刚性)
# ==============================================================================
class PositionBook:
    """
    本策略的虚拟仓位账。只由"本策略 OID 的成交增量"驱动, 与交易所仓位完全解耦。

    【会计模型: 标准库存移动平均法】
      avg = cost / open_qty
      * 加仓: open_qty += q, cost += p*q                  -> 均价按加权移动
      * 平仓: 按【当前均价】等比扣减成本 cost -= avg*q      -> 数学上均价恒定不变
      因此"只要没有新的加仓, 均价/止盈价/止损价绝对不动"(I5 价格刚性), 同时 cost 与
      open_qty 始终一一对应, 绝不会被历史已平仓部分污染。
    realized 仅作统计口径, 绝不参与任何价格计算。
    total_open_* / total_close_* 为只增统计量, 供 I4 总量硬闸与事后复盘使用。
    """

    def __init__(self, direction: Direction):
        self.direction = direction
        self.open_qty = 0.0            # 当前虚拟持仓数量
        self.cost = 0.0                # 当前持仓对应的总成本(与 open_qty 严格配对)
        self.total_open_filled = 0.0   # 累计开仓成交量(只增, I4 硬闸依据)
        self.total_open_cost = 0.0     # 累计开仓成本(只增, 仅复盘用)
        self.total_close_filled = 0.0  # 累计平仓成交量(只增)
        self.realized = 0.0            # 已实现盈亏(仅统计用)

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

    def add_close(self, price: float, qty: float):
        """平仓只扣减持仓数量与对应比例的成本, 均价数学恒定(价格刚性)。"""
        if qty <= 0:
            return
        self.total_close_filled += qty          # 统计口径: 交易所实际成交多少就记多少
        eff = min(qty, self.open_qty)
        if eff <= 1e-12:
            return      # 库存已空, 忽略滞后事件, 杜绝除零与天量虚假利润
        cur_avg = self.avg
        self.realized += self.direction.sign * (price - cur_avg) * eff
        self.cost -= cur_avg * eff              # 按当前均价等比扣减 -> avg 保持不变
        self.open_qty -= eff
        if self.open_qty <= 1e-12:              # 彻底归零, 杜绝浮点残余
            self.open_qty = 0.0
            self.cost = 0.0

    def force_flat(self):
        """交易所实际持仓已归零时的强制同步(打破收尾死锁)。"""
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
    生命周期: 全量铺单 -> (成交 -> 数量对齐 TP/SL) * -> 止盈/止损/超时 -> 终结。
    价格全静态(蓝图算死), 数量全动态(跟随 min(虚拟持仓, 交易所实际持仓))。
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
        self.acked: Dict[str, float] = {}            # I2: coid -> 已入账成交量
        self.order_cum_cost: Dict[str, float] = {}   # coid -> 已入账累计金额, 反求边际价格
        self.exit_filled: Dict[OrderRole, float] = {OrderRole.TP: 0.0, OrderRole.SL: 0.0}
        self.cross_alerted: Dict[str, float] = {}    # 跨周期成交告警去重
        self.forced_close_coids = set()              # 强平单 OID 白名单(防被清扫误杀)
        self.close_attempts = 0                      # 市价强平尝试次数(上限 FORCE_CLOSE_MAX_ATTEMPTS)
        self.add_suspended = False                   # 降级标志: 停止加仓, 只收尾
        self.end_reason: Optional[EndReason] = None
        self.created_ts = time.time()
        self.first_fill_ts = 0.0
        self.sl_breach_since = 0.0
        self.forced_close_sent = False
        self.last_price = 0.0
        self._clamp_log_ts = 0.0

    # ---------------------- 对外 ----------------------
    def maintain(self, snapshot: Dict[str, UniOrder], price: float,
                 ex_pos_qty: Optional[float]) -> Optional[EndReason]:
        """一轮完整维护。snapshot 为 None 时调用方必须跳过本轮(信息不全不动手)。"""
        now = time.time()
        if price > 0:
            self.last_price = price

        # A. 清理"本策略前缀但不在当前追踪集合"的在线单(旧周期残留 / 重复单 / 旧代补挂单)
        self._sweep_untracked(snapshot)

        # B. 开仓层维护: 认领 / 点查 / 记账 / 补挂 / 撤单终态确认
        for lp in self.bp.layers:
            self._maintain_layer(lp, snapshot, price, now)

        # C. 出场单状态同步(先探明真相, 再谈对齐)
        self._sync_exit(self.tp, snapshot, now)
        self._sync_exit(self.sl, snapshot, now)

        # D. 一致性硬闸 (I4)
        self._check_invariants(ex_pos_qty)

        # E. 终结判定(第一次)
        if self._check_end(price, now):
            return self.end_reason

        # F. 出场单数量对齐: 先止损(更要命)后止盈; 价格恒取蓝图固定值
        self._align_exit_sl(price, ex_pos_qty, now)
        self._align_exit_tp(price, ex_pos_qty, now)

        # G. 兜底熔断 + 终结判定(第二次, 对齐过程可能触发强平)
        self._bottom_guard(price, now)
        self._check_end(price, now)
        return self.end_reason

    def place_all_layers(self, price: float):
        """IDLE -> ACTIVE 的全量铺单。逐层 WAL -> 发单 -> 落账。"""
        if price > 0:
            self.last_price = price
        for lp in self.bp.layers:
            self._try_place_layer(lp, price, time.time())

    def current_tp_price(self) -> float:
        """当前应使用的固定止盈价 = 已完整成交的最深层对应的蓝图止盈价。"""
        if not self.bp.layers:
            return 0.0
        idx = min(self._tp_layer_idx(), len(self.bp.layers) - 1)
        return self.bp.layers[idx].tp

    def _tp_layer_idx(self) -> int:
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

    # ---------------------- A. 清扫 ----------------------
    def _tracked_coids(self) -> set:
        s = {lp.coid for lp in self.bp.layers if lp.coid}
        if self.tp.coid:
            s.add(self.tp.coid)
        if self.sl.coid:
            s.add(self.sl.coid)
        s |= {c for c in self.forced_close_coids if c}
        return s

    def _sweep_untracked(self, snapshot: Dict[str, UniOrder]):
        tracked = self._tracked_coids()
        for coid, o in list(snapshot.items()):
            if coid in tracked:
                continue
            # 先记账/告警(它确实是我们的单, 只是"上一代"), 再物理撤销, 杜绝幽灵仓位
            self._observe(coid, o)
            self._cancel(coid, "非当前追踪订单(旧周期残留/重复单/旧代补挂)")

    # ---------------------- B. 开仓层 ----------------------
    def _maintain_layer(self, lp: LayerPlan, snapshot: Dict[str, UniOrder],
                        price: float, now: float):
        if lp.life in (Life.FILLED, Life.DEAD):
            return

        # 撤单待确认: 盘口快照是权威全量信息 ——
        #   仍在盘口 => 撤单未生效, 重发撤单(绝不重挂新单, 严守 I3 单一出场)
        #   已不在盘口 => 点查裁决终态; 点查长期未知则安全释放(盘口已确认无单, 不可能出现两张)
        if lp.life == Life.CANCEL_PENDING:
            if lp.coid in snapshot:
                self._observe(lp.coid, snapshot[lp.coid])
                if now - lp.last_action_ts >= CANCEL_CONFIRM_SEC:
                    if self._cancel(lp.coid, "撤单未生效(仍在盘口), 重发撤单"):
                        lp.last_action_ts = time.time()
                return
            self._confirm_layer_cancel(lp, price, now)
            return

        if lp.coid and lp.coid in snapshot:
            o = snapshot[lp.coid]
            self._observe(lp.coid, o)
            lp.life = Life.LIVE
            lp.ex_id = o.ex_id or lp.ex_id
            # 参数漂移检测(被人工改单/交易所异常): 撤掉重挂, 保证盘口=蓝图
            if self._param_drift(o, lp):
                logger.critical(f"[层] 第[{lp.layer}]层在线单参数与蓝图不符(疑被外部修改), 撤销后原价重挂 | "
                                f"盘口 价[{o.price}] 量[{o.amount}] vs 蓝图 价[{lp.price}] 量[{lp.qty}]")
                if self._cancel(lp.coid, "参数漂移"):
                    lp.life = Life.CANCEL_PENDING
                    lp.last_action_ts = time.time()
            return

        # 不在盘口
        if lp.life in (Life.NOT_PLACED, Life.DEFERRED) or not lp.coid:
            self._try_place_layer(lp, price, now)
            return
        if now - lp.last_action_ts < ORDER_GRACE_SEC:
            return       # 冷静期: 容忍撮合与网络传播延迟, 避免误判掉单

        o = self.ctx.gw.fetch_order(lp.coid)
        if o is ORDER_NOT_FOUND:
            # 交易所明确回执"订单不存在" => 该单确实没挂出, 可安全换新号重挂
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, lp.layer, OrderRole.OPEN.value,
                                   MartinLedger.A_ALERT, lp.coid, lp.price, lp.qty, "WARN",
                                   "交易所明确回执订单不存在, 判定未挂出, 换新号重挂")
            lp.coid, lp.life = "", Life.NOT_PLACED
            self._try_place_layer(lp, price, now)
            return
        if not isinstance(o, UniOrder):
            logger.info(f"[层] 第[{lp.layer}]层点查结果未知(网络/超时), 保留原OID下一轮继续点查, "
                        f"严禁换号重发 | CID:[{lp.coid}]")
            return

        self._observe(lp.coid, o)
        if o.status == "FILLED":
            lp.life = Life.FILLED
        elif o.status in ("CANCELED", "REJECTED"):
            if o.filled > 0:
                # 部分成交后被撤(多为 ADL / 人工干预): 保守起来不补挂剩余量
                lp.life = Life.DEAD
                logger.critical(f"[层] 第[{lp.layer}]层部分成交后被撤销, 按保守策略不补挂剩余量 | "
                                f"已成交[{o.filled}]/[{o.amount}]")
            else:
                lp.coid, lp.life = "", Life.NOT_PLACED
                logger.info(f"[层] 第[{lp.layer}]层订单被撤销(外部干扰), 换新号原价重挂")
                self._try_place_layer(lp, price, now)
        else:
            lp.life = Life.LIVE      # 快照滞后, 下一轮再看

    def _confirm_layer_cancel(self, lp: LayerPlan, price: float, now: float):
        """
        撤单终态确认(前置条件: 该单已不在盘口快照中)。
        撤单请求发出的瞬间订单可能刚好成交, 必须点查裁决, 绝不凭 cancel() 返回值定终态。
        """
        if now - lp.last_action_ts < CANCEL_CONFIRM_SEC:
            return
        o = self.ctx.gw.fetch_order(lp.coid)
        if o is ORDER_NOT_FOUND:
            lp.coid, lp.life = "", Life.NOT_PLACED
            self._try_place_layer(lp, price, now)
            return
        if not isinstance(o, UniOrder):
            # 点查持续未知: 盘口已确认无该单, 重挂不会出现两张; 超时后释放以防永久死锁
            if now - lp.last_action_ts >= CANCEL_RELEASE_SEC:
                logger.critical(f"[层] 第[{lp.layer}]层撤单终态点查持续未知超[{CANCEL_RELEASE_SEC}s], "
                                f"但盘口快照已确认无该单, 安全释放状态允许重挂 | CID:[{lp.coid}]")
                lp.coid, lp.life = "", Life.NOT_PLACED
                self._try_place_layer(lp, price, now)
            return
        self._observe(lp.coid, o)
        if o.status == "FILLED":
            lp.life = Life.FILLED
            logger.critical(f"[层] 第[{lp.layer}]层撤单确认时发现其实已完全成交, 已按成交入账 | "
                            f"CID:[{lp.coid}]")
            return
        if o.is_terminal:
            if o.filled > 0:
                lp.life = Life.DEAD
                logger.critical(f"[层] 第[{lp.layer}]层部分成交后已撤销, 保守不补挂剩余量 | "
                                f"已成交[{o.filled}]/[{o.amount}]")
            else:
                lp.coid, lp.life = "", Life.NOT_PLACED
                self._try_place_layer(lp, price, now)
            return
        # 仍活跃: 重发撤单
        if self._cancel(lp.coid, "撤单确认发现仍活跃, 重发撤单"):
            lp.last_action_ts = time.time()

    def _param_drift(self, o: UniOrder, lp: LayerPlan) -> bool:
        tick = self.ctx.spec.tick_size or 1e-12
        step = self.ctx.spec.step_size or 1e-12
        return (abs(o.price - lp.price) > tick * 0.6 or
                abs(o.amount - lp.qty) > step * 0.6)

    def _try_place_layer(self, lp: LayerPlan, price: float, now: float):
        """开仓层挂单, 层层设防, 任何不确定都选择"不挂"。"""
        if self.end_reason or self.add_suspended:
            return
        if lp.life in (Life.FILLED, Life.DEAD, Life.LIVE, Life.UNKNOWN, Life.CANCEL_PENDING):
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
        # 止损价已被击穿: 不再新增任何加仓单
        if self.book.open_qty > 0 and price > 0 and self.bp.sl_price > 0:
            if self._is_breached(price, self.bp.sl_price):
                logger.info(f"[层] 现价[{price}]已击穿全局止损价[{self.bp.sl_price}], 停止一切加仓")
                return

        lp.attempts += 1
        lp.coid = OidCodec.build(self.ctx.cfg.strategy_id, self.cycle_id, OrderRole.OPEN, lp.layer)
        lp.life = Life.INTENT
        lp.last_action_ts = time.time()
        # WAL: 先落意图(带确定的 OID), 再发请求
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, lp.layer, OrderRole.OPEN.value,
                               MartinLedger.A_INTENT_PLACE, lp.coid, lp.price, lp.qty, "PENDING",
                               f"attempt={lp.attempts}")
        res = self.ctx.gw.place_limit(self.direction.open_side, lp.qty, lp.price,
                                      lp.coid, self.direction.position_side)
        lp.last_action_ts = time.time()
        self._after_place(res, lp.layer, OrderRole.OPEN, lp.coid, lp.price, lp.qty,
                          lp_ref=lp)

    def _after_place(self, res: PlaceResult, layer: int, role: OrderRole, coid: str,
                     price: float, qty: float, lp_ref: Optional[LayerPlan] = None,
                     ex_ref: Optional[ExitOrder] = None):
        """
        统一处理挂单三态与错误分类, 决定 life 与退避。
        铁律: 只有交易所【明确拒单】才允许换号重发; 结果未知一律保留原 OID 点查。
        """
        holder = lp_ref or ex_ref
        tag = f"层[{layer}] 角色[{role.value}] 价[{price}] 量[{qty}] CID[{coid}]"
        if res.ok:
            if holder is not None:
                holder.life = Life.LIVE
                holder.ex_id = res.ex_id
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role.value,
                                   MartinLedger.A_PLACE_OK, coid, price, qty, "OK", res.ex_id)
            logger.info(f"[挂单] {tag} | 结果:[OK] 交易所单号:[{res.ex_id}]")
            return

        if res.unknown or res.kind in (ErrKind.UNKNOWN_RESULT, ErrKind.DUPLICATE):
            if holder is not None:
                holder.life = Life.UNKNOWN
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role.value,
                                   MartinLedger.A_PLACE_UNKNOWN, coid, price, qty, "UNKNOWN",
                                   res.err)
            logger.critical(f"[挂单] {tag} | 结果:[UNKNOWN/{res.kind.value}] 未收到确定回执, "
                            f"保持原OID等待点查裁决, 严禁换号重发")
            return

        self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role.value,
                               MartinLedger.A_PLACE_FAIL, coid, price, qty,
                               res.kind.value, res.err)

        # 止盈止损单命悬一线, 最大退避绝不超过 3 秒; 加仓单才允许按梯度长休眠
        if role in (OrderRole.TP, OrderRole.SL):
            backoff = 3.0
        else:
            backoff = RETRY_BACKOFF_SEC[min(len(RETRY_BACKOFF_SEC) - 1,
                                            max(0, (holder.attempts if holder else 1) - 1))]

        if res.kind == ErrKind.PRICE_BAND:
            if holder is not None:
                holder.life = Life.NOT_PLACED
                holder.coid = ""
                holder.next_retry_ts = time.time() + max(backoff, 30)
            logger.info(f"[挂单] {tag} | 结果:[价格带拒单] 退避后重试 | 回执:[{res.err}]")
        elif res.kind == ErrKind.TRANSIENT:
            if holder is not None:
                holder.life = Life.NOT_PLACED
                holder.coid = ""
                holder.next_retry_ts = time.time() + backoff
            logger.info(f"[挂单] {tag} | 结果:[明确拒单-瞬态/限频] {backoff}s 后退避重试 | 回执:[{res.err}]")
        elif res.kind == ErrKind.INSUFFICIENT:
            if holder is not None:
                holder.life = Life.NOT_PLACED
                holder.coid = ""
                holder.next_retry_ts = time.time() + max(backoff, 30)
            logger.critical(f"[挂单] {tag} | 结果:[保证金不足] 已退避, 请立即检查账户可用余额! | 回执:[{res.err}]")
        elif res.kind == ErrKind.IMMEDIATE_TRIGGER:
            if holder is not None:
                holder.life = Life.NOT_PLACED
                holder.coid = ""
                holder.next_retry_ts = time.time() + 3
            px = self.last_price
            dev = (abs(price / px - 1) * 100) if (px > 0 and price > 0) else 999.9
            if px > 0 and dev <= SL_IMM_TRIG_MAX_DEV_PCT and self._is_breached(px, price):
                logger.critical(f"[挂单] {tag} | 结果:[条件单会立即触发] 本地现价[{px}]双重核验通过"
                                f"(确已击穿, 偏离{dev:.2f}%), 转市价强平")
                self.force_close("条件单立即触发且本地现价核验确已击穿", EndReason.SL_FORCED)
            else:
                logger.critical(f"[挂单] {tag} | 结果:[条件单会立即触发] 但本地现价[{px}]核验不通过"
                                f"(偏离[{dev:.2f}%]), 判定为止损价计算异常, 拒绝市价强平! "
                                f"已转退避重挂, 请立即人工核查 | 回执:[{res.err}]")
                self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role.value,
                                       MartinLedger.A_ALERT, coid, price, qty, "SL_SANITY_FAIL",
                                       f"立即触发回执与本地现价{px}不符, 已拒绝强平")
        elif res.kind == ErrKind.REDUCE_REJECT:
            if holder is not None:
                holder.life = Life.NOT_PLACED
                holder.coid = ""
                holder.next_retry_ts = time.time() + 5
            logger.critical(f"[挂单] {tag} | 结果:[平仓数量超持仓] 本策略仓位疑似被外部平掉! | 回执:[{res.err}]")
            self.suspend_add("平仓单被拒(仓位被外部改动)")
        else:  # INVALID / FATAL
            if holder is not None:
                holder.life = Life.DEFERRED
                holder.attempts = MAX_PLACE_ATTEMPTS
            logger.critical(f"[挂单] {tag} | 结果:[明确拒单-{res.kind.value}] 该单永久停挂 | 回执:[{res.err}]")

    # ---------------------- C. 出场单同步 ----------------------
    def _sync_exit(self, ex: ExitOrder, snapshot: Dict[str, UniOrder], now: float):
        if not ex.coid or ex.life in (Life.FILLED, Life.NOT_PLACED):
            return

        # 撤单待确认: 与开仓层同构 —— 仍在盘口就重发撤单(严守 I3),
        # 已不在盘口才点查裁决; 点查长期未知则安全释放(盘口无单 + 出场数量被实际持仓夹逼)
        if ex.life == Life.CANCEL_PENDING:
            if ex.coid in snapshot:
                self._observe(ex.coid, snapshot[ex.coid])
                if now - ex.last_action_ts >= CANCEL_CONFIRM_SEC:
                    if self._cancel(ex.coid, f"{ex.role.value}撤单未生效(仍在盘口), 重发撤单"):
                        ex.last_action_ts = time.time()
                return
            self._confirm_exit_cancel(ex, now)
            return

        if ex.coid in snapshot:
            o = snapshot[ex.coid]
            self._observe(ex.coid, o)
            ex.life = Life.LIVE
            ex.live_price = o.stop_price if ex.role is OrderRole.SL and o.stop_price > 0 else o.price
            ex.live_remaining = o.remaining
            ex.ex_id = o.ex_id or ex.ex_id
            return
        if now - ex.last_action_ts < ORDER_GRACE_SEC:
            return
        o = self.ctx.gw.fetch_order(ex.coid)
        if o is ORDER_NOT_FOUND:
            logger.info(f"[出场] {ex.role.value} 单交易所明确回执不存在, 置为待补挂 | CID:[{ex.coid}]")
            ex.reset()
            return
        if not isinstance(o, UniOrder):
            logger.info(f"[出场] {ex.role.value} 单点查结果未知, 保留原OID下一轮再查 | CID:[{ex.coid}]")
            return
        self._observe(ex.coid, o)
        if o.status == "FILLED":
            ex.life = Life.FILLED
            logger.info(f"[出场] {ex.role.value} 单已完全成交 @[{o.avg_price or o.price}] "
                        f"x[{o.filled}] | 周期即将终结")
        elif o.status in ("CANCELED", "REJECTED"):
            ex.reset()
        else:
            ex.life = Life.LIVE
            ex.live_remaining = o.remaining
            ex.live_price = o.stop_price if ex.role is OrderRole.SL and o.stop_price > 0 else o.price

    def _confirm_exit_cancel(self, ex: ExitOrder, now: float):
        """出场单撤单终态确认(前置条件: 该单已不在盘口快照中)。"""
        if now - ex.last_action_ts < CANCEL_CONFIRM_SEC:
            return
        o = self.ctx.gw.fetch_order(ex.coid)
        if o is ORDER_NOT_FOUND:
            ex.reset()
            return
        if not isinstance(o, UniOrder):
            # 点查持续未知会让出场单永久卡死 -> 仓位失去保护。
            # 安全释放的双保险: ① 盘口快照已确认无该单(不可能出现两张)
            #                  ② 新单数量恒取 min(虚拟持仓, 交易所实际持仓), 绝不超量
            if now - ex.last_action_ts >= CANCEL_RELEASE_SEC:
                logger.critical(f"[出场] {ex.role.value} 单撤单终态点查持续未知超[{CANCEL_RELEASE_SEC}s], "
                                f"但盘口已确认无该单, 安全释放以便重挂保护单 | CID:[{ex.coid}]")
                self.ctx.ledger.append(self.cycle_id, self.signal_ts, 0, ex.role.value,
                                       MartinLedger.A_ALERT, ex.coid, 0, 0, "CANCEL_RELEASE",
                                       "撤单终态未知但盘口无该单, 释放状态重挂出场单")
                ex.reset()
            return
        self._observe(ex.coid, o)
        if o.status == "FILLED":
            ex.life = Life.FILLED
            logger.critical(f"[出场] {ex.role.value} 单撤单确认时发现其实已完全成交, 已按成交入账 | "
                            f"CID:[{ex.coid}]")
            return
        if o.is_terminal:
            ex.reset()
            return
        if self._cancel(ex.coid, f"{ex.role.value}撤单确认发现仍活跃, 重发撤单"):
            ex.last_action_ts = time.time()

    # ---------------------- D. 不变量校验 ----------------------
    def _check_invariants(self, ex_pos_qty: Optional[float] = None):
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
        if self.end_reason:
            return self.end_reason
        spec = self.ctx.spec

        # 1) 仓位归零(含碎屑) 且已经开过仓 -> 按本周期出场单累计成交归因(而非单据 life)
        if self._has_fill() and spec.qty_is_dust(self.book.open_qty):
            tpq = self.exit_filled.get(OrderRole.TP, 0.0)
            slq = self.exit_filled.get(OrderRole.SL, 0.0)
            if slq > tpq:
                self.end_reason = EndReason.SL_FORCED if self.forced_close_sent else EndReason.SL
            elif tpq > 0:
                self.end_reason = EndReason.TP
            else:
                self.end_reason = EndReason.MANUAL_FLAT
                logger.critical("[周期] 仓位已归零且本周期出场单累计成交为 0, 确认为被外部平仓, "
                                "周期被动结束")
            logger.info(f"[周期] 终结 | 原因:[{self.end_reason.value}] "
                        f"出场成交(止盈{tpq:.8g}/止损{slq:.8g}) "
                        f"已实现盈亏:[{self.book.realized:+.4f}U]")
            return self.end_reason

        # 2) 入场反向起飞检测 (做多时暴涨, 或做空时暴跌): 提前作废, 不傻等超时
        if not self._has_fill() and price > 0 and self.bp.base_price > 0:
            runaway_pct = self.direction.sign * (price / self.bp.base_price - 1) * 100
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
        """兜底熔断: 条件止损单没能触发时(交易所故障/触发价类型偏差), 主动市价平仓。"""
        if self.end_reason or price <= 0 or self.book.open_qty <= 1e-12:
            return
        slp = self.bp.sl_price
        if slp <= 0 or not self._is_breached(price, slp):
            self.sl_breach_since = 0.0
            return
        if self.sl_breach_since == 0.0:
            self.sl_breach_since = now
            logger.critical(f"[熔断] 现价[{price}]已击穿全局止损价[{slp}], 等待条件单触发 "
                            f"({SL_BREACH_CONFIRM_SEC}s 宽限)...")
            return
        if now - self.sl_breach_since >= SL_BREACH_CONFIRM_SEC:
            logger.critical(f"[熔断] 击穿止损价已[{now - self.sl_breach_since:.1f}s], "
                            f"条件单仍未成交, 主动市价强平本策略数量")
            self.force_close("兜底熔断: 条件单未触发", EndReason.SL_FORCED)

    def force_close(self, why: str, reason: EndReason, ex_pos_qty: Optional[float] = None):
        if self.forced_close_sent:
            self.end_reason = self.end_reason or reason
            return
        if self.close_attempts >= FORCE_CLOSE_MAX_ATTEMPTS:
            logger.critical(f"[强平] 已尝试[{self.close_attempts}]次市价强平仍未归零, "
                            f"停止自动重发, 等待人工介入 | 原因:[{why}]")
            self.end_reason = self.end_reason or reason
            return
        self.add_suspended = True
        # 先撤掉所有未成交的加仓单与出场单, 避免强平后又被加仓单接刀
        self._cancel_all_working()

        qty = self._target_exit_qty(ex_pos_qty)
        if self.ctx.spec.qty_is_dust(qty):
            logger.info(f"[强平] 待平数量[{qty}]低于最小交易单位, 按碎屑归零处理 | 原因:[{why}]")
            self.forced_close_sent = True
            self.end_reason = self.end_reason or reason
            return

        self.close_attempts += 1
        coid = OidCodec.build(self.ctx.cfg.strategy_id, self.cycle_id, OrderRole.SL, 99)
        self.forced_close_coids.add(coid)   # 登记强平 OID, 防止被 _sweep_untracked 误撤销

        self.ctx.ledger.append(self.cycle_id, self.signal_ts, 99, OrderRole.SL.value,
                               MartinLedger.A_INTENT_PLACE, coid, 0, qty, "PENDING",
                               f"市价强平(第{self.close_attempts}次): {why}")
        res = self.ctx.gw.place_market(self.direction.close_side, qty, coid,
                                       self.direction.position_side)
        self.forced_close_sent = True
        if res.ok:
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, 99, OrderRole.SL.value,
                                   MartinLedger.A_PLACE_OK, coid, 0, qty, "OK", why)
            logger.critical(f"[强平] 市价平仓指令已下发 | 数量:[{qty}] 原因:[{why}]")
            time.sleep(1.0)
            o = self.ctx.gw.fetch_order(coid)
            if isinstance(o, UniOrder):
                self._observe(coid, o)
            self.end_reason = self.end_reason or reason
        elif res.unknown:
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, 99, OrderRole.SL.value,
                                   MartinLedger.A_PLACE_UNKNOWN, coid, 0, qty, "UNKNOWN", why)
            logger.critical(f"[强平] 市价平仓结果未知, 保留原OID, 收尾阶段将点查+核对实际持仓后"
                            f"再决定是否重发 | CID:[{coid}]")
            self.end_reason = self.end_reason or reason
        else:
            self.ctx.ledger.append(self.cycle_id, self.signal_ts, 99, OrderRole.SL.value,
                                   MartinLedger.A_PLACE_FAIL, coid, 0, qty,
                                   res.kind.value, res.err)
            logger.critical(f"[强平] 市价平仓被明确拒单! 请立即人工介入 | 分类:[{res.kind.value}] "
                            f"回执:[{res.err}]")
            self.forced_close_sent = False      # 明确拒单 => 单子没进去, 可安全重发
            if res.kind == ErrKind.REDUCE_REJECT:
                self.end_reason = self.end_reason or reason   # 仓位已不存在, 按平掉处理

    # ---------------------- F. 出场单对齐 (价格固定 / 数量跟随) ----------------------
    def _target_exit_qty(self, ex_pos_qty: Optional[float] = None) -> float:
        """出场数量 = min(本地虚拟持仓, 交易所实际持仓)。防 -2022 拒单与超量平仓。"""
        q = self.book.open_qty
        if ex_pos_qty is not None and ex_pos_qty >= 0:
            if ex_pos_qty < q - self.ctx.spec.step_size * 0.5:
                now = time.time()
                if now - self._clamp_log_ts > 60:
                    self._clamp_log_ts = now
                    logger.critical(f"[夹逼] 本地虚拟持仓[{q:.8g}] 大于交易所实际持仓[{ex_pos_qty:.8g}], "
                                    f"按实际持仓下调平仓数量(疑被外部平仓/其它策略干扰), 请人工核对")
                q = ex_pos_qty
        return self.ctx.spec.round_qty(q, "down")

    def _align_exit_sl(self, price: float, ex_pos_qty: Optional[float], now: float):
        if self.end_reason:
            return

        # 熔断拦截: 防止非法参数导致高频死循环发单
        if self.sl.attempts >= MAX_PLACE_ATTEMPTS:
            if self.sl.life != Life.DEFERRED:
                self.sl.life = Life.DEFERRED
                logger.critical("[出场] 止损单连续失败达上限, 停止更新, 维持现状等待人工介入")
            return

        spec, cfg = self.ctx.spec, self.ctx.cfg
        qty = self._target_exit_qty(ex_pos_qty)
        if spec.qty_is_dust(qty):
            if self.sl.coid and self.sl.life == Life.LIVE:
                if self._cancel(self.sl.coid, "无持仓, 撤销残留止损单"):
                    self.sl.life = Life.CANCEL_PENDING
                    self.sl.last_action_ts = time.time()
            return

        target = self.bp.sl_price        # 全局固定, 永不重算
        if target <= 0:
            return

        # 触发价已在现价的错误一侧
        if price > 0 and self._is_breached(price, target):
            if self.sl.life != Life.LIVE or not self.sl.coid:
                logger.critical(f"[熔断] 现价[{price}]击穿止损价[{target}]且盘口无有效条件单, 立即市价强平！")
                self.force_close("击穿止损且盘口无单", EndReason.SL_FORCED, ex_pos_qty)
            return  # 盘口确实有 LIVE 单时, 才交给 _bottom_guard 等待交易所撮合触发

        # 撤单终态未确认: 由 _sync_exit 阶段负责推进(重发撤单 / 点查裁决 / 超时安全释放),
        # 此处绝不越过它挂新单, 严守 I3 单一出场
        if self.sl.life == Life.CANCEL_PENDING:
            return
        if self._exit_is_aligned(self.sl, target, qty):
            return
        if now < self.sl.next_retry_ts:
            return
        if self.sl.coid and self.sl.life in (Life.LIVE, Life.UNKNOWN, Life.INTENT):
            if not self._cancel(self.sl.coid, "止损单数量对齐持仓(价格固定不变)"):
                return
            self.sl.life = Life.CANCEL_PENDING
            self.sl.last_action_ts = time.time()
            return    # 下一轮确认终态后再挂新单

        self.sl.target_price, self.sl.target_qty = target, qty
        self.sl.attempts += 1
        self.sl.coid = OidCodec.build(cfg.strategy_id, self.cycle_id, OrderRole.SL, 0)
        self.sl.life = Life.INTENT
        self.sl.last_action_ts = time.time()
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, 0, OrderRole.SL.value,
                               MartinLedger.A_INTENT_PLACE, self.sl.coid, target, qty,
                               "PENDING", f"全局固定止损 均价{self.book.avg:.8g} "
                                          f"最大亏损{cfg.max_loss_usdt}")
        res = self.ctx.gw.place_stop_market(self.direction.close_side, qty, target,
                                            self.sl.coid, self.direction.position_side,
                                            cfg.sl_working_type)
        self.sl.last_action_ts = time.time()
        self._after_place(res, 0, OrderRole.SL, self.sl.coid, target, qty, ex_ref=self.sl)
        if res.ok:
            self.sl.attempts = 0

    def _align_exit_tp(self, price: float, ex_pos_qty: Optional[float], now: float):
        if self.end_reason:
            return

        if self.tp.attempts >= MAX_PLACE_ATTEMPTS:
            if self.tp.life != Life.DEFERRED:
                self.tp.life = Life.DEFERRED
                logger.critical("[出场] 止盈单连续失败达上限, 停止更新, 维持现状等待人工介入")
            return

        spec, cfg = self.ctx.spec, self.ctx.cfg
        qty = self._target_exit_qty(ex_pos_qty)
        if spec.qty_is_dust(qty):
            if self.tp.coid and self.tp.life == Life.LIVE:
                if self._cancel(self.tp.coid, "无持仓, 撤销残留止盈单"):
                    self.tp.life = Life.CANCEL_PENDING
                    self.tp.last_action_ts = time.time()
            return

        target = self.current_tp_price()      # 蓝图预先算死的当层固定止盈价
        if target <= 0:
            return
        if self.tp.life == Life.CANCEL_PENDING:
            return                            # 同上: 交由 _sync_exit 推进
        if self._exit_is_aligned(self.tp, target, qty):
            return
        if now < self.tp.next_retry_ts:
            return
        if self.tp.coid and self.tp.life in (Life.LIVE, Life.UNKNOWN, Life.INTENT):
            if not self._cancel(self.tp.coid, "止盈单对齐(层位固定价 + 持仓数量)"):
                return
            self.tp.life = Life.CANCEL_PENDING
            self.tp.last_action_ts = time.time()
            return

        self.tp.target_price, self.tp.target_qty = target, qty
        self.tp.attempts += 1
        self.tp.coid = OidCodec.build(cfg.strategy_id, self.cycle_id, OrderRole.TP, 0)
        self.tp.life = Life.INTENT
        self.tp.last_action_ts = time.time()
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, 0, OrderRole.TP.value,
                               MartinLedger.A_INTENT_PLACE, self.tp.coid, target, qty,
                               "PENDING", f"第{self._tp_layer_idx()}层固定止盈 "
                                          f"均价{self.book.avg:.8g} 止盈{cfg.tp_pct}%")
        res = self.ctx.gw.place_limit(self.direction.close_side, qty, target,
                                      self.tp.coid, self.direction.position_side)
        self.tp.last_action_ts = time.time()
        self._after_place(res, 0, OrderRole.TP, self.tp.coid, target, qty, ex_ref=self.tp)
        if res.ok:
            self.tp.attempts = 0

    def _exit_is_aligned(self, ex: ExitOrder, target_price: float, target_qty: float) -> bool:
        """
        防抖核心: 比较维度是【在线单的剩余量】而非订单总量。
        止盈部分成交后, 剩余量本就等于新的持仓量, 不应撤单重挂(否则白交手续费)。
        """
        if ex.life != Life.LIVE or not ex.coid:
            return False
        tick = self.ctx.spec.tick_size or 1e-12
        step = self.ctx.spec.step_size or 1e-12
        return (abs(ex.live_price - target_price) <= tick * 0.6 and
                abs(ex.live_remaining - target_qty) <= step * 0.6)

    # ---------------------- 记账 / 撤单 / 对账 ----------------------
    def _observe(self, coid: str, o: UniOrder) -> float:
        parsed = OidCodec.parse(coid)
        if not parsed:
            return 0.0
        if parsed.cycle_id != self.cycle_id:
            self._alert_cross_cycle(coid, o, parsed)     # 跨周期成交严禁静默忽略
            return 0.0
        filled = float(o.filled or 0.0)
        if filled <= 0:
            return 0.0
        prev = self.acked.get(coid, 0.0)
        delta = filled - prev
        if delta <= max(QTY_EPS_RATIO, self.ctx.spec.step_size * 1e-6):
            return 0.0

        # 计算真实边际成交价, 消除大单分批吃单导致的均价漂移
        cum_avg_price = o.avg_price or o.price or 0.0
        if cum_avg_price <= 0:
            cum_avg_price = (self.bp.layers[parsed.layer].price
                             if parsed.role is OrderRole.OPEN and parsed.layer < len(self.bp.layers)
                             else (self.book.avg or self.last_price))
        now_cum_cost = cum_avg_price * filled
        prev_cum_cost = self.order_cum_cost.get(coid, 0.0)
        marginal_price = (now_cum_cost - prev_cum_cost) / delta
        if marginal_price <= 0:
            marginal_price = cum_avg_price

        # WAL 铁律: 先落盘成功, 再改内存。写盘失败抛 LedgerError -> 主循环硬停机
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, parsed.layer, parsed.role.value,
                               MartinLedger.A_FILL, coid, marginal_price, delta, "OK",
                               f"入账前持仓{self.book.open_qty:.8g} 均价{self.book.avg:.8g}")
        self.order_cum_cost[coid] = now_cum_cost
        self.acked[coid] = filled
        if parsed.role is OrderRole.OPEN:
            self.book.add_open(marginal_price, delta)
            if self.first_fill_ts == 0.0:
                self.first_fill_ts = time.time()
        else:
            self.book.add_close(marginal_price, delta)
            self.exit_filled[parsed.role] = self.exit_filled.get(parsed.role, 0.0) + delta

        logger.info(f"[成交] 角色[{parsed.role.value}] 层[{parsed.layer}] @[{marginal_price:.8g}] "
                    f"x[{delta:.8g}] => 持仓[{self.book.open_qty:.8g}] 均价[{self.book.avg:.8g}] "
                    f"已实现[{self.book.realized:+.4f}U]")
        return delta

    def _alert_cross_cycle(self, coid: str, o: UniOrder, parsed: ParsedOid):
        """上一代周期的残留挂单在本周期成交: 严禁静默, 必须 CRITICAL + 落账留痕。"""
        filled = float(o.filled or 0.0)
        if filled <= 0:
            return
        if self.cross_alerted.get(coid, 0.0) >= filled - 1e-12:
            return
        self.cross_alerted[coid] = filled
        px = o.avg_price or o.price or 0.0
        logger.critical(f"[跨周期] 检测到非本周期挂单发生成交! 未计入本周期账本, 可能形成幽灵持仓, "
                        f"请立即人工核对 | CID:[{coid}] 所属周期:[{parsed.cycle_id}] "
                        f"角色:[{parsed.role.value}] 层:[{parsed.layer}] 成交量:[{filled}] "
                        f"均价:[{px}] 状态:[{o.status}] 数量:[{o.amount}]")
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, parsed.layer, parsed.role.value,
                               MartinLedger.A_ALERT, coid, px, filled, "CROSS_CYCLE_FILL",
                               f"上一代周期[{parsed.cycle_id}]挂单成交未入本周期账本, 需人工核对")

    def _cancel(self, coid: str, why: str) -> bool:
        """
        发出撤单请求。返回 True 仅代表"交易所已受理", 不代表订单终态。
        调用方必须把本地状态置为 CANCEL_PENDING, 由主循环下一轮点查裁决。
        """
        parsed = OidCodec.parse(coid)
        layer = parsed.layer if parsed else -1
        role = parsed.role.value if parsed else "?"
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role,
                               MartinLedger.A_INTENT_CANCEL, coid, 0, 0, "PENDING", why)
        ok = self.ctx.gw.cancel(coid)
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, layer, role,
                               MartinLedger.A_CANCEL_OK if ok else MartinLedger.A_CANCEL_FAIL,
                               coid, 0, 0, "ACCEPTED_PENDING_CONFIRM" if ok else "FAIL", why)
        return ok

    def _cancel_all_working(self):
        for lp in self.bp.layers:
            if lp.coid and lp.life in (Life.LIVE, Life.INTENT, Life.UNKNOWN):
                if self._cancel(lp.coid, "周期收尾/强平前清场"):
                    lp.life = Life.CANCEL_PENDING
                    lp.last_action_ts = time.time()
        for ex in (self.tp, self.sl):
            if ex.coid and ex.life in (Life.LIVE, Life.INTENT, Life.UNKNOWN):
                if self._cancel(ex.coid, "周期收尾/强平前清场"):
                    ex.life = Life.CANCEL_PENDING
                    ex.last_action_ts = time.time()

    def reconcile_final(self):
        """
        兜底对账: 逐一点查本周期用过的所有 OID, 把最终成交量补记入账,
        从而拿到精确的未平仓数量, 再决定是否市价强平(防止漏记形成幽灵持仓)。
        """
        for coid in sorted(self._tracked_coids()):
            o = self.ctx.gw.fetch_order(coid)
            if isinstance(o, UniOrder):
                self._observe(coid, o)

    def suspend_add(self, why: str):
        """降级: 撤掉所有未成交加仓单, 只保留止盈止损收尾。绝不新增仓位。"""
        if self.add_suspended:
            return
        self.add_suspended = True
        logger.critical(f"[降级] 进入 SUSPEND_ADD: 停止一切加仓, 仅维护止盈止损收尾 | 原因:[{why}]")
        self.ctx.ledger.append(self.cycle_id, self.signal_ts, -1, "-",
                               MartinLedger.A_ALERT, "", 0, 0, "SUSPEND_ADD", why)
        for lp in self.bp.layers:
            if lp.coid and lp.life in (Life.LIVE, Life.INTENT):
                if self._cancel(lp.coid, f"降级停止加仓: {why}"):
                    lp.life = Life.CANCEL_PENDING
                    lp.last_action_ts = time.time()

    # ---------------------- 序列化 ----------------------
    def start_meta(self) -> dict:
        return {
            "sig_ts": self.signal_ts, "dir": self.direction.value,
            "base": self.bp.base_price, "step_pct": self.ctx.cfg.step_pct,
            "mult": self.ctx.cfg.qty_mult, "tp_pct": self.ctx.cfg.tp_pct,
            "max_loss": self.ctx.cfg.max_loss_usdt, "sl": self.bp.sl_price,
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
        self.err_streak = 0
        self.stop_flag = False
        self._pos_cache = (0.0, None)     # (ts, qty)
        self._pending_recover: Optional[Tuple[dict, List[dict]]] = None
        self.recover_attempts = 0
        self.cycles_done = 0
        self.pnl_total = 0.0

    # ---------------- 启动 ----------------
    def boot(self) -> bool:
        self.cfg.validate()
        self.spec = self.gw.load_instrument()
        if self.spec is None:
            logger.critical("[启动] 交易规格获取失败, 拒绝启动(宁可不跑, 不可乱跑)")
            return False
        logger.info(f"[启动] 交易规格: {self.spec}")

        hedge = self.gw.is_hedge_mode()
        if hedge is False:
            logger.critical("[启动] 账户非【双向持仓 Hedge Mode】! positionSide 会被拒单, 拒绝启动。"
                            "请在币安合约设置中切换为双向持仓")
            return False

        meta, rows, watermark = self.ledger.load_state()
        self.gate.set_watermark(watermark)
        logger.info(f"[启动] 信号去重水位线恢复为 [{watermark}]")

        if meta and meta.get("layers"):
            self._pending_recover = (meta, rows)
            if self._recover_cycle(meta, rows):
                self._pending_recover = None
                return True
            # 恢复失败绝不清场、绝不进 IDLE: 否则会撤掉保护单让活仓裸奔
            self.state = EngineState.RECOVER
            logger.critical("[启动] 未收尾周期接管暂时失败(快照/网络异常), 转入 RECOVER 持续重试; "
                            "绝不清场、绝不开新仓, 现有止盈止损单原样保留")
            return True

        # 无未完成周期: 清理一切本策略前缀的残留挂单(马丁绝不容忍幽灵单)
        self._purge_strategy_orders("冷启动: 空闲态不应存在任何本策略挂单")
        self.state = EngineState.IDLE
        logger.info("[启动] 无未完成周期, 进入空闲监听态")
        return True

    def _recover_cycle(self, meta: dict, rows: List[dict]) -> bool:
        """断点续传: 以账本给出的 OID 清单为索引, 向交易所逐一点查求真相, 重建全部状态。"""
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
            ctx = CycleCtx(self.cfg, self.gw, self.ledger, self.spec)
            cyc = MartinCycle(ctx, cycle_id, int(meta.get("sig_ts") or 0), direction, bp)
            logger.info(f"[恢复] 检测到未收尾周期[{cycle_id}] {direction.value} 层数[{len(layers)}], "
                        f"开始向交易所求证真相...")

            # 1) 收集该周期用过的所有 OID(按出现顺序去重)
            coids, seen = [], set()
            for r in rows or []:
                c = (r.get("coid") or "").strip()
                if c and c not in seen:
                    seen.add(c)
                    coids.append(c)

            # 2) 盘口快照 + 逐一点查
            snap = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
            if snap is None:
                logger.critical("[恢复] 无法拉取盘口快照, 本次放弃接管(稍后重试), "
                                "绝不带着未知状态运行, 也绝不清场")
                return False
            observations: Dict[str, UniOrder] = {}
            for c in coids:
                if c in snap:
                    observations[c] = snap[c]
                    continue
                o = self.gw.fetch_order(c)
                if isinstance(o, UniOrder):
                    observations[c] = o
            for c, o in snap.items():        # 盘口里出现但账本没记的(极罕见), 一并纳入
                observations.setdefault(c, o)

            # 3) 先入账所有开仓成交, 再入账所有平仓成交(顺序无法精确还原, 已实现盈亏为近似值)
            for role_filter in (OrderRole.OPEN, OrderRole.TP, OrderRole.SL):
                for c, o in observations.items():
                    p = OidCodec.parse(c)
                    if p and p.cycle_id == cycle_id and p.role is role_filter:
                        cyc._observe(c, o)

            # 4) 还原每层 / 出场单的 life 指针
            for lp in bp.layers:
                cands = [c for c in coids
                         if (lambda p: p and p.cycle_id == cycle_id
                             and p.role is OrderRole.OPEN and p.layer == lp.layer)(OidCodec.parse(c))]
                if not cands:
                    lp.life = Life.NOT_PLACED
                    continue
                newest = cands[-1]
                lp.coid = newest
                o = observations.get(newest)
                if o is None:
                    lp.life = Life.UNKNOWN          # 结果未知: 保留原OID, 主循环点查裁决
                    lp.last_action_ts = 0.0
                elif o.is_open:
                    lp.life = Life.LIVE
                    lp.last_action_ts = time.time()
                elif o.status == "FILLED":
                    lp.life = Life.FILLED
                else:
                    lp.life = Life.DEAD if o.filled > 0 else Life.NOT_PLACED
                    if lp.life == Life.NOT_PLACED:
                        lp.coid = ""
            for ex, role in ((cyc.tp, OrderRole.TP), (cyc.sl, OrderRole.SL)):
                cands = [c for c in coids
                         if (lambda p: p and p.cycle_id == cycle_id and p.role is role
                             and p.layer == 0)(OidCodec.parse(c))]
                if not cands:
                    continue
                newest = cands[-1]
                o = observations.get(newest)
                if o is None:
                    continue
                ex.coid, ex.ex_id = newest, o.ex_id
                ex.last_action_ts = time.time()
                if o.is_open:
                    ex.life = Life.LIVE
                    ex.live_remaining = o.remaining
                    ex.live_price = o.stop_price if role is OrderRole.SL and o.stop_price > 0 else o.price
                elif o.status == "FILLED":
                    ex.life = Life.FILLED
                else:
                    ex.reset()

            self.cycle = cyc
            self.state = EngineState.ACTIVE
            logger.info(f"[恢复] 周期[{cycle_id}]接管成功 | 虚拟持仓:[{cyc.book.open_qty:.8g}] "
                        f"均价:[{cyc.book.avg:.8g}] 全局止损:[{bp.sl_price:.8g}] "
                        f"已实现(近似):[{cyc.book.realized:+.4f}U] | "
                        f"层状态:{[f'{l.layer}:{l.life.value}' for l in bp.layers]}")
            return True
        except LedgerError:
            raise
        except Exception as e:
            logger.critical(f"[恢复] 周期重建异常, 本次放弃接管(稍后重试), 绝不清场、绝不空转开新仓 | "
                            f"错误:[{e}]", exc_info=True)
            return False

    def _purge_strategy_orders(self, why: str) -> bool:
        prefix = OidCodec.strategy_prefix(self.cfg.strategy_id)
        for _ in range(TEARDOWN_MAX_ROUND):
            snap = self.gw.fetch_open_orders(prefix)
            if snap is None:
                return False
            if not snap:
                return True
            logger.info(f"[清场] 发现[{len(snap)}]张本策略残留挂单, 逐一撤销 | 原因:[{why}]")
            for coid in list(snap.keys()):
                self.ledger.append("-", 0, -1, "-", MartinLedger.A_INTENT_CANCEL,
                                   coid, 0, 0, "PENDING", why)
                self.gw.cancel(coid)
            time.sleep(1.0)
        return False

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
                elif self.state == EngineState.RECOVER:
                    self._recover_step()
                elif self.state in (EngineState.ACTIVE, EngineState.SUSPEND_ADD):
                    self._active_step()
                elif self.state == EngineState.TEARDOWN:
                    self._teardown_step()
                self.err_streak = 0
            except LedgerError as e:
                logger.critical(f"[主循环] 账本(WAL)写盘失败, 立即硬停机保留现场, 等待人工处理磁盘 | "
                                f"错误:[{e}]")
                self.state = EngineState.STOPPED
                time.sleep(5)
            except Exception as e:
                self.err_streak += 1
                logger.error(f"[主循环] 第[{self.err_streak}]次连续异常(状态不变, 下一轮重试) | 错误:[{e}]",
                             exc_info=True)
                if self.err_streak >= MAX_CONSECUTIVE_ERRORS:
                    if self.state in (EngineState.ACTIVE, EngineState.SUSPEND_ADD) and self.cycle:
                        logger.critical(f"[主循环] 持仓活动态连续异常达[{self.err_streak}]次, "
                                        f"转入保守收尾模式(停止加仓)")
                        self.cycle.suspend_add("主循环连续异常")
                        self.state = EngineState.SUSPEND_ADD
                    elif self.state == EngineState.IDLE:
                        nap = random.uniform(*IDLE_ERROR_SLEEP_SEC)
                        logger.critical(f"[主循环] 空仓空闲态连续异常达[{self.err_streak}]次(疑网络/交易所维护), "
                                        f"长休眠[{nap:.0f}s]退避后继续监听, 绝不停机")
                        time.sleep(nap)
                    else:
                        logger.critical(f"[主循环] [{self.state.value}]态连续异常达[{self.err_streak}]次, "
                                        f"保持当前状态继续重试")
                    self.err_streak = 0
                time.sleep(3)
        logger.info("[主循环] 收到退出信号, 已停止。注意: 交易所的止盈/止损单被有意保留, "
                    "下次启动会自动断点续传接管")

    # ---------------- RECOVER ----------------
    def _recover_step(self):
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

    # ---------------- IDLE ----------------
    def _idle_step(self):
        time.sleep(self.cfg.idle_poll_interval_sec)
        price = self.gw.fetch_last_price()
        if price is None:
            # 拉不到现价属可预期瞬态(不抛异常污染 err_streak), 但必须额外退避,
            # 否则断网期间会以 idle 周期高频空转刷屏并徒增 API 压力
            time.sleep(IDLE_NO_PRICE_SLEEP_SEC)
            return
        self.last_price = price

        sig = self.gate.poll()
        if sig is None:
            return
        logger.info(f"[信号] 收到有效开仓信号 {sig} | 现价:[{price}]")

        # 开仓前最后一道清场: 空闲态不应存在任何本策略挂单
        snap = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        if snap is None:
            logger.info("[信号] 无法确认盘口干净度, 本次放弃开仓(信息不全不动手)")
            return
        if snap:
            logger.critical(f"[信号] 空闲态却发现[{len(snap)}]张本策略残留挂单, 先清场再考虑开仓, "
                            f"本次信号放弃")
            self._purge_strategy_orders("空闲态残留清理")
            self.gate.set_watermark(sig.signal_ts)
            return

        bp = BlueprintBuilder.build(self.cfg, self.spec, sig)
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
                    f"总量:[{bp.total_qty:.8g}] 最大名义:[{bp.total_notional:.2f}U] "
                    f"全局止损:[{bp.sl_price:.8g}] | 开始全量铺单")
        cyc.place_all_layers(price)

    # ---------------- ACTIVE ----------------
    def _position_qty(self) -> Optional[float]:
        if not self.cfg.clamp_exit_by_position or self.cycle is None:
            return None
        ts, qty = self._pos_cache
        if time.time() - ts < POSITION_CACHE_SEC:
            return qty
        q = self.gw.fetch_position_qty(self.cycle.direction.position_side)
        self._pos_cache = (time.time(), q)
        return q

    def _active_step(self):
        time.sleep(self.cfg.poll_interval_sec)
        cyc = self.cycle
        if cyc is None:
            self.state = EngineState.IDLE
            return
        price = self.gw.fetch_last_price()
        if price is None:
            return                # 信息不全 -> 本轮不动手
        self.last_price = price
        snap = self.gw.fetch_open_orders(OidCodec.strategy_prefix(self.cfg.strategy_id))
        if snap is None:
            return                # 信息不全 -> 本轮不动手

        reason = cyc.maintain(snap, price, self._position_qty())
        if cyc.add_suspended and self.state == EngineState.ACTIVE:
            self.state = EngineState.SUSPEND_ADD
        if reason:
            logger.info(f"[周期] 周期[{cyc.cycle_id}]达成终结条件[{reason.value}], 进入清理阶段")
            self.state = EngineState.TEARDOWN

    # ---------------- TEARDOWN ----------------
    def _teardown_step(self):
        cyc = self.cycle
        if cyc is None:
            self.state = EngineState.IDLE
            return
        reason = cyc.end_reason or EndReason.MANUAL_FLAT

        # 1) 挂单清场: 撤掉本策略一切残留挂单(深层加仓单 + 未触发条件单)
        cleaned = self._purge_strategy_orders(f"周期[{cyc.cycle_id}]收尾: {reason.value}")

        # 2) 账本对账: 逐一点查本周期 OID, 把最终成交补记入账, 得到精确未平数量
        cyc.reconcile_final()

        # 3) 残余仓位处理
        residual = self.spec.round_qty(cyc.book.open_qty, "down")
        if not self.spec.qty_is_dust(residual):
            real = self.gw.fetch_position_qty(cyc.direction.position_side)

            # 【安全铁律】拉不到交易所实际持仓(网络未知)时, 绝不盲目重发市价平仓单!
            # 上一张 unknown 的市价单可能其实已成交, Hedge 模式下不带 reduceOnly,
            # 重发会导致超量平仓 -> 直接开出反向仓位。此时只能原地等待网络恢复。
            if real is None:
                logger.critical("[清理] 无法获取交易所实际持仓(结果未知), 暂缓重发市价平仓单, "
                                "防范双重平仓造成反向开仓! 原地等待网络恢复")
                time.sleep(3.0)
                return

            if self.spec.qty_is_dust(real):
                # 交易所已无仓 -> 强制同步本地归零, 打破 TEARDOWN 死循环
                logger.critical(f"[清理] 本地虚拟残余[{residual}] 但交易所实际持仓已归零, "
                                f"强制同步本地账本归零(打破收尾死锁)")
                self.ledger.append(cyc.cycle_id, cyc.signal_ts, -1, "-", MartinLedger.A_ALERT,
                                   "", 0, residual, "FORCE_FLAT_SYNC",
                                   "交易所实际持仓为0, 本地强制归零")
                cyc.book.force_flat()
            elif cyc.close_attempts >= FORCE_CLOSE_MAX_ATTEMPTS:
                logger.critical(f"[清理] 已连续[{cyc.close_attempts}]次市价强平仍未归零(残余[{residual}]), "
                                f"转入 STOPPED 并告警人工介入, 停止一切自动交易")
                self.ledger.append(cyc.cycle_id, cyc.signal_ts, -1, "-", MartinLedger.A_ALERT,
                                   "", 0, residual, "CLOSE_NOT_CONVERGED",
                                   f"市价强平{cyc.close_attempts}次未收敛, 需人工介入")
                self.state = EngineState.STOPPED
                return
            else:
                logger.critical(f"[清理] 周期结束仍有残余持仓[{residual}](交易所实际[{real}]), 市价平掉"
                                f"(第[{cyc.close_attempts + 1}]次尝试)")
                cyc.forced_close_sent = False        # 允许换新 OID 重发, 打破防重锁死
                cyc.force_close("周期收尾残余平仓", reason, ex_pos_qty=real)
                time.sleep(1.0)
                return                              # 下一轮再核对是否归零

        dust = cyc.book.open_qty
        if 0 < dust and self.spec.qty_is_dust(dust):
            logger.info(f"[清理] 剩余[{dust:.10g}]低于最小交易单位, 按碎屑账面归零(不发无效API)")

        # 退出双重铁律: 挂单必须撤净 AND 虚拟仓位必须彻底归零
        if not cleaned or not self.spec.qty_is_dust(cyc.book.open_qty):
            logger.critical("[清理] 挂单未清空或仓位未完全平掉, 保持 TEARDOWN 状态下一轮继续处理, "
                            "绝不回退空闲态！")
            time.sleep(2.0)
            return

        # 4) 清算落账
        snap = cyc.book.snapshot()
        snap.update({"reason": reason.value, "cleaned": cleaned,
                     "layers_filled": sum(1 for l in cyc.bp.layers if l.life == Life.FILLED),
                     "layers_total": len(cyc.bp.layers),
                     "duration_sec": round(time.time() - cyc.created_ts, 1)})
        self.ledger.append(cyc.cycle_id, cyc.signal_ts, -1, "-", MartinLedger.A_CYCLE_END,
                           "", 0, 0, reason.value,
                           json.dumps(snap, separators=(",", ":")))
        self.cycles_done += 1
        self.pnl_total += cyc.book.realized
        logger.info(f"[周期] 周期[{cyc.cycle_id}]清算完成 | 原因:[{reason.value}] "
                    f"成交层数:[{snap['layers_filled']}/{snap['layers_total']}] "
                    f"入场均价:[{snap['entry_avg']:.8g}] "
                    f"本周期盈亏:[{cyc.book.realized:+.4f}U] 累计:[{self.pnl_total:+.4f}U] "
                    f"耗时:[{snap['duration_sec']}s]")

        self.cycle = None
        self._pos_cache = (0.0, None)
        self.state = EngineState.IDLE
        logger.info("[周期] 已回到空闲态, 立即恢复接收新信号(无冷却)")


# ==============================================================================
# 11. 只读辅助线程 (看板 / 校时) —— 绝不参与任何决策
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
            slp = c.bp.sl_price
            tpp = c.current_tp_price()
            lines += [
                f" 🔁 周期:[{c.cycle_id}] 方向:[{c.direction.value}] "
                f"加仓层:[成交{filled}/在挂{live}/暂缓{defer}/共{len(c.bp.layers)}]"
                + ("  ⚠️已降级停止加仓" if c.add_suspended else ""),
                f" 💰 虚拟持仓:[{c.book.open_qty:.8g}] 均价:[{c.book.avg:.8g}] "
                f"浮亏盈:[{c.book.unrealized(e.last_price):+.4f}U] 已实现:[{c.book.realized:+.4f}U]",
                f" 🎯 固定止盈价:[{tpp:.8g}](第{c._tp_layer_idx()}层/{e.cfg.tp_pct}%) "
                f"🛑 全局固定止损价:[{slp:.8g}](最大亏损{e.cfg.max_loss_usdt}U) | "
                f"TP:[{c.tp.life.value}] SL:[{c.sl.life.value}]",
            ]
            if e.last_price > 0 and slp > 0 and tpp > 0:
                d = abs(e.last_price / slp - 1) * 100
                lines.append(f" 📏 现价距止损:[{d:.3f}%] 距止盈:[{abs(e.last_price / tpp - 1) * 100:.3f}%]")
        lines.append("=========================================================\n")
        logger.info("\n".join(lines))


class TimeSyncThread(threading.Thread):
    """周期性刷新与交易所的时间差, 对抗长期运行的本地时钟漂移(-1021 的根因)。"""

    def __init__(self, exchange, interval_sec=3600):
        super().__init__(daemon=True)
        self.ex = exchange
        self.interval = interval_sec

    def run(self):
        while True:
            time.sleep(self.interval)
            try:
                self.ex.load_time_difference()
                logger.info(f"[校时] 已重新校准 | 偏差:[{self.ex.options.get('timeDifference', 0)}ms]")
            except Exception as e:
                logger.info(f"[校时] 本次校时失败, 保持旧偏差 | 错误:[{e}]")


# ==============================================================================
# 12. 进程编排
# ==============================================================================
def run_single_strategy(cfg: MartinConfig):
    """子进程入口: 独立日志 -> 单实例锁 -> 父进程自杀看门狗 -> 组装 -> 冷启动 -> 主循环。"""
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
        os.write(lock_fd, str(os.getpid()).encode())
    except Exception as e:
        logger.critical(f"[进程] 获取单实例锁失败, 疑有同名策略正在运行, 拒绝启动 | "
                        f"策略:[{cfg.strategy_id}] 错误:[{e}]")
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
    TimeSyncThread(exchange, interval_sec=3600).start()
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
    logger.info(f"[系统] 全部进程启动完毕, 主进程进入守护模式 | 进程数:[{len(procs)}]")
    try:
        for p in procs:
            p.join()
    except (KeyboardInterrupt, SystemExit):
        pass


# ==============================================================================
# 13. 运维工具 (人工排障用, 与主流程解耦)
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