# -*- coding: utf-8 -*-
"""
================================================================================
[功能摘要]: 币安 U本位合约平台适配层，负责统一封装 ccxt 接口、处理精度修约、规范化数据结构，并翻译交易所异构错误码，作为上层交易策略与底层的唯一接缝。
[输入数据]: 策略层传入的标准化操作指令 (如 symbol, side, amount, price)、条件单触发要求，以及通过 config 注入的账户 API 凭据。数据形貌皆为 Python 原生基础类型。
[数据流转/交互]:
  1. 会话建立: 凭据注入 -> `init_exchange` 构建带限流、网络防抖与自动时间补偿机制的会话。
  2. 指令下达: 业务请求 -> 经过本层 `InstrumentSpec` 拦截并按照币安特有最小精度与名义价值(Notional)完成修约裁剪。
  3. 核心执行: 将裁剪后合规的参数转换为 ccxt/币安原生结构体并执行 HTTP 请求 -> 与交易所产生实际交互。
  4. 异常收敛: 捕获的底层网络报错或业务拒单(JSON错误码) -> 经由正则剥离，传入字典归类器(ErrKind)进行标准化翻译。
[输出数据]: 成功写入(下单/撤单)一律返回平台无关的统一包装对象 `ExecResult`；状态读取操作一律返回结构化实体(如 `UniOrder`)或抛出明确异常交由上层状态机裁决。
================================================================================
"""
import hashlib
import math
import re
import time
import uuid
import platform
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from enum import Enum

import ccxt
from ccxt.base.errors import NetworkError, InvalidOrder

from common_utils import setup_logger, get_config

logger = setup_logger()

_THROTTLE_TS_ATTR = "_lite_last_call_ts"
COID_MAX_LEN = 36
_COID_ILLEGAL_RE = re.compile(r'[^a-zA-Z0-9_.-]')
_ERR_CODE_RE = re.compile(r'["\']code["\']\s*:\s*(-\d+)')

OS_OPEN = "OPEN"
OS_FILLED = "FILLED"
OS_CANCELED = "CANCELED"
OS_REJECTED = "REJECTED"
OS_UNKNOWN = "UNKNOWN"

_CCXT_STATUS_MAP = {
    "NEW": OS_OPEN, "PARTIALLY_FILLED": OS_OPEN, "PENDING_CANCEL": OS_OPEN,
    "FILLED": OS_FILLED, "CANCELED": OS_CANCELED, "CANCELLED": OS_CANCELED,
    "EXPIRED": OS_CANCELED, "EXPIRED_IN_MATCH": OS_CANCELED, "REJECTED": OS_REJECTED,
    "OPEN": OS_OPEN, "CLOSED": OS_FILLED,
}

_CODE_KIND = {
    -1000: "UNKNOWN_RESULT", -1001: "UNKNOWN_RESULT", -1006: "UNKNOWN_RESULT", -1007: "UNKNOWN_RESULT",
    -1003: "TRANSIENT", -1008: "TRANSIENT", -1015: "TRANSIENT", -1021: "TRANSIENT",
    -1013: "INVALID", -1102: "INVALID", -1104: "INVALID", -1111: "INVALID", -1116: "INVALID", -1117: "INVALID",
    -1121: "INVALID", -2010: "INVALID", -2011: "INVALID", -2013: "INVALID", -2027: "INVALID", -4003: "INVALID",
    -4005: "INVALID", -4013: "INVALID", -4014: "INVALID", -4015: "INVALID", -4016: "INVALID", -4164: "INVALID",
    -4165: "INVALID",
    -2018: "INSUFFICIENT", -2019: "INSUFFICIENT",
    -2021: "IMM_TRIG",
    -2022: "REDUCE_REJECT",
    -4131: "PRICE_BAND",
}

_UNKNOWN_TEXT = ("timeout", "timed out", "read timed out", "connection", "reset by peer",
                 "network", "temporarily", "service unavailable", "bad gateway",
                 "gateway timeout", "502", "503", "504", "520", "521", "ssl", "eof",
                 "no response", "unknown result", "结果未知")


class ExecStatus(Enum):
    OK = "OK"
    REJECT = "REJECT"
    UNKNOWN = "UNKNOWN"


class ErrKind(Enum):
    NONE = "NONE"
    UNKNOWN_RESULT = "UNKNOWN_RESULT"
    TRANSIENT = "TRANSIENT"
    PRICE_BAND = "PRICE_BAND"
    IMMEDIATE_TRIGGER = "IMM_TRIG"
    INSUFFICIENT = "INSUFFICIENT"
    REDUCE_REJECT = "REDUCE_REJECT"
    DUPLICATE = "DUPLICATE"
    INVALID = "INVALID"
    FATAL = "FATAL"


class ExecResult:
    """统一执行结果。属性形貌: status(ExecStatus), client_oid(str), exchange_oid(str), raw_data(dict), kind(ErrKind)"""

    def __init__(self, status, client_oid, exchange_oid="", latency_ms=0, error_msg="", raw_data=None, kind=None):
        self.status = status
        self.client_oid = client_oid
        self.exchange_oid = exchange_oid
        self.latency_ms = latency_ms
        self.error_msg = error_msg
        self.raw_data = raw_data
        self.kind = kind or ErrKind.NONE

    @property
    def ok(self): return self.status == ExecStatus.OK

    @property
    def unknown(self): return self.status == ExecStatus.UNKNOWN

    @property
    def ex_id(self): return self.exchange_oid

    @property
    def err(self): return self.error_msg


class UniOrder:
    """平台无关的统一订单视图。属性形貌: status(str), price/stop_price/amount/filled(float), raw(dict)"""
    __slots__ = ("coid", "ex_id", "status", "price", "stop_price", "amount", "filled", "avg_price", "side", "ts", "raw")

    def __init__(self, coid="", ex_id="", status="UNKNOWN", price=0.0, stop_price=0.0, amount=0.0, filled=0.0,
                 avg_price=0.0, side="", ts=0, raw=None):
        self.coid = coid
        self.ex_id = ex_id
        self.status = status
        self.price = price
        self.stop_price = stop_price
        self.amount = amount
        self.filled = filled
        self.avg_price = avg_price
        self.side = side
        self.ts = ts
        self.raw = raw or {}

    @property
    def remaining(self): return max(0.0, self.amount - self.filled)

    @property
    def is_terminal(self): return self.status in ("FILLED", "CANCELED", "REJECTED")


class _OrderNotFound:
    __slots__ = ()

    def __repr__(self): return "ORDER_NOT_FOUND"

    def __bool__(self): return False


ORDER_NOT_FOUND = _OrderNotFound()


def _dec(x): return Decimal(str(x))


def quantize(value, step, mode="down"):
    if step is None or step <= 0: return float(value)
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
    """市场规格实体，掌管核心数量/价格的合规裁剪。"""

    def __init__(self, symbol, tick_size, step_size, min_qty, max_qty, min_notional, contract_size=1.0):
        self.symbol = symbol
        self.tick_size = float(tick_size or 0.0)
        self.step_size = float(step_size or 0.0)
        self.min_qty = float(min_qty or 0.0)
        self.max_qty = float(max_qty or 0.0) or float("inf")
        self.min_notional = float(min_notional or 0.0)
        self.contract_size = float(contract_size or 1.0)

    def round_price(self, price, mode="half"):
        return quantize(price, self.tick_size, mode)

    def round_qty(self, qty, mode="down"):
        return quantize(qty, self.step_size, mode)

    def normalize_open_qty(self, qty, price):
        # : 强行将订单量抬高至 minNotional 可能会在用户资金极端不足时产生超额爆仓风险，建议业务层做前置余额校验。
        if price <= 0: return 0.0
        q = self.round_qty(qty, "down")
        need_by_min_qty = self.round_qty(self.min_qty, "up") if self.min_qty > 0 else 0.0
        need_by_notional = 0.0
        if self.min_notional > 0:
            need_by_notional = self.round_qty(self.min_notional / (price * self.contract_size), "up")
            while need_by_notional * price * self.contract_size < self.min_notional:
                need_by_notional = round(need_by_notional + self.step_size, 12)
        q = max(q, need_by_min_qty, need_by_notional)
        return 0.0 if q > self.max_qty else q

    def notional(self, price, qty):
        return price * qty * self.contract_size

    def qty_is_dust(self, qty):
        if qty is None: return False
        return qty < max(self.min_qty, self.step_size) * (1 - 1e-9)

    def __repr__(self):
        return f"Spec({self.symbol} tick={self.tick_size} step={self.step_size} minQty={self.min_qty} minNotional={self.min_notional})"


def err_code_of(msg):
    m = _ERR_CODE_RE.search(str(msg or ""))
    return int(m.group(1)) if m else None


def is_order_not_found(msg):
    if err_code_of(msg) == -2013: return True
    low = str(msg or "").lower()
    return "order does not exist" in low or "order not found" in low


def is_cancel_target_gone(err):
    text = (type(err).__name__ + str(err)).lower()
    return any(k in text for k in ("ordernotfound", "-2011", "does not exist", "unknown order"))


def classify_error(msg):
    raw = str(msg or "")
    if not raw.strip(): return ErrKind.UNKNOWN_RESULT
    low = raw.lower()
    if any(k in low for k in ("duplicate", "already exist")): return ErrKind.DUPLICATE

    code = err_code_of(raw)
    if code is not None:
        kind_str = _CODE_KIND.get(code, "FATAL")
        return ErrKind(kind_str)

    if any(k in low for k in _UNKNOWN_TEXT): return ErrKind.UNKNOWN_RESULT
    if any(k in low for k in ("too many", "throttl", "429")): return ErrKind.TRANSIENT
    if "immediately trigger" in low: return ErrKind.IMMEDIATE_TRIGGER
    if any(k in low for k in ("reduceonly", "reduce only")): return ErrKind.REDUCE_REJECT
    if any(k in low for k in ("insufficient", "margin is insufficient")): return ErrKind.INSUFFICIENT
    if any(k in low for k in ("percent_price", "price_filter", "would immediately match")): return ErrKind.PRICE_BAND
    if any(k in low for k in ("min_notional", "notional", "lot_size", "precision")): return ErrKind.INVALID
    return ErrKind.UNKNOWN_RESULT


def make_fail_result(client_oid, err, latency_ms=0):
    kind = classify_error(err)
    status = ExecStatus.UNKNOWN if kind is ErrKind.UNKNOWN_RESULT else ExecStatus.REJECT
    return ExecResult(status, client_oid, latency_ms=latency_ms, error_msg=str(err), kind=kind)


def init_exchange(api_key, secret_key, proxies=None):
    try:
        config = {
            'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True,
            'options': {'defaultType': 'future', 'adjustForTimeDifference': True, 'recvWindow': 10000}
        }
        if proxies: config['proxies'] = proxies
        exchange = ccxt.binance(config)

        exchange.load_time_difference()
        initial_diff = exchange.options.get('timeDifference', 0)
        exchange.load_markets()
        logger.info(f"[网关/初始化] 会话建立成功 | 关键参数: <补偿漂移 {initial_diff}ms> | 结果: [OK]")
        return exchange
    except Exception as e:
        logger.error(
            f"[网关/初始化] 会话建立失败 | 关键参数: <无> | 结果: [FATAL] | 可能原因: 凭据无效或网络无法触达交易所，原始异常: {e}")
        raise


def safe_init_exchange(api_key, secret_key, proxies):
    interval = 5
    while True:
        try:
            return init_exchange(api_key, secret_key, proxies=proxies)
        except Exception as e:
            logger.warning(
                f"[网关/安全初始化] 连接受挫尝试退避重连 | 关键参数: <退避 {interval}s> | 结果: [PENDING] | 可能原因: {e}")
            time.sleep(interval)
            interval = min(interval * 2, 60)


def open_session(proxies=None, account="mama"):
    api_key = get_config(f"{account}_biance_api_key")
    secret_key = get_config(f"{account}_biance_api_secret")
    return safe_init_exchange(api_key, secret_key, proxies)


def sync_exchange_time(exchange):
    exchange.load_time_difference()
    return exchange.options.get('timeDifference', 0)


def disable_builtin_retry(exchange):
    exchange.options["maxRetriesOnFailure"] = 0
    exchange.options["maxRetriesOnFailureDelay"] = 0


def throttle(exchange, min_gap):
    if not min_gap or min_gap <= 0: return
    last = getattr(exchange, _THROTTLE_TS_ATTR, 0.0)
    gap = time.time() - last
    if gap < min_gap: time.sleep(min_gap - gap)
    setattr(exchange, _THROTTLE_TS_ATTR, time.time())


def to_market_id(symbol):
    return str(symbol).replace("/", "").split(":")[0]


def sanitize_coid_part(text, max_len):
    text_str = str(text).strip()
    safe_str = _COID_ILLEGAL_RE.sub('', text_str)
    if not safe_str: safe_str = hashlib.md5(text_str.encode('utf-8')).hexdigest()
    return safe_str[:max_len]


def build_client_oid(parts, rand_len=5):
    prefix = "_".join(sanitize_coid_part(t, n) for t, n in parts)
    client_oid = f"{prefix}_{uuid.uuid4().hex[:rand_len]}"
    return prefix, client_oid[:COID_MAX_LEN]


def supports_cancel_all(exchange):
    return bool(exchange.has.get('cancelAllOrders'))


def fetch_market_precision(exchange, symbol):
    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        return {'price': market['precision']['price'], 'amount': market['precision']['amount']}
    except Exception as e:
        logger.error(
            f"[市场/精度] 获取交易对精度失败 | 关键参数: <{symbol}> | 结果: [REJECT] | 可能原因: 符号拼写错误或接口变动, 异常: {e}")
        return None


def format_price_amount(price, amount, precision):
    p_prec, a_prec = precision['price'], precision['amount']
    p_decimals = max(0, int(round(-math.log10(p_prec)))) if p_prec < 1 else 0
    a_decimals = max(0, int(round(-math.log10(a_prec)))) if a_prec < 1 else 0
    return float(f"{price:.{p_decimals}f}"), float(f"{amount:.{a_decimals}f}")


def amount_to_precision(exchange, symbol, amount):
    return float(exchange.amount_to_precision(symbol, amount))


def fetch_instrument_spec(exchange, symbol):
    try:
        try:
            exchange.load_markets()
        except Exception as e:
            logger.warning(f"[市场/规格] 在线刷新失败转用缓存 | 关键参数: <{symbol}> | 结果: [CACHE] | 可能原因: {e}")

        m = exchange.market(symbol)
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

        prec, limits = m.get("precision") or {}, m.get("limits") or {}
        tick = tick or float(prec.get("price") or 0) or 0.0
        step = step or float(prec.get("amount") or 0) or 0.0
        min_qty = min_qty or float(((limits.get("amount") or {}).get("min")) or 0)
        min_notional = min_notional or float(((limits.get("cost") or {}).get("min")) or 0) or 5.0
        spec = InstrumentSpec(symbol, tick, step, min_qty, max_qty, min_notional, float(m.get("contractSize") or 1.0))

        if spec.tick_size <= 0 or spec.step_size <= 0:
            logger.error(
                f"[市场/规格] 交易规格核心刻度缺失 | 关键参数: <{spec}> | 结果: [FATAL] | 可能原因: 交易所下架该品种或返回结构剧变")
            return None
        return spec
    except Exception as e:
        logger.error(
            f"[市场/规格] 彻底拉取失败 | 关键参数: <{symbol}> | 结果: [FATAL] | 可能原因: 该交易对不存在, 异常: {e}")
        return None


def fetch_last_price(exchange, symbol):
    return exchange.fetch_ticker(symbol)['last']


def fetch_swap_tickers(exchange):
    return exchange.fetch_tickers(params={'type': 'swap'})


def fetch_usdt_swap_changes(exchange):
    tickers = fetch_swap_tickers(exchange)
    return {k: v['percentage'] for k, v in tickers.items() if k.endswith(':USDT') and v.get('percentage') is not None}


def make_position_key(symbol, position_side):
    return f"{symbol}_{str(position_side).upper()}"


def position_key_symbol(pos_key):
    return str(pos_key).rsplit("_", 1)[0]


def get_symbol_status(exchange, symbol):
    t0 = time.perf_counter()
    try:
        balance = exchange.fetch_balance()
        usdt_free = float(balance.get('USDT', {}).get('free', 0.0))
        positions = exchange.fetch_positions([symbol])
        position_amt = float(positions[0]['info']['positionAmt']) if positions else 0.0
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[账户/状态] 核查资金与仓位 | 关键参数: <{symbol}> | 结果: [OK], 耗时 【{latency}ms】, 可用 【{usdt_free:.2f} USDT】, 仓位 【{position_amt}】")
        return ExecStatus.OK, usdt_free, position_amt
    except NetworkError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(
            f"[账户/状态] 核查失败 | 关键参数: <{symbol}> | 结果: [UNKNOWN], 耗时 【{latency}ms】 | 可能原因: 物理网络断联，异常: {e}")
        return ExecStatus.UNKNOWN, 0.0, 0.0
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(
            f"[账户/状态] 业务异常拒绝 | 关键参数: <{symbol}> | 结果: [REJECT], 耗时 【{latency}ms】 | 可能原因: 权限不足或接口签名失效，异常: {e}")
        return ExecStatus.REJECT, 0.0, 0.0


def get_total_equity(exchange):
    t0 = time.perf_counter()
    try:
        balance = exchange.fetch_balance()
        total_equity = float(balance['info']['totalMarginBalance'])
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[账户/总权益] 拉取当前净资产 | 关键参数: <无> | 结果: [OK], 耗时 【{latency}ms】, 总权益 【{total_equity:.2f} USD】")
        return ExecStatus.OK, total_equity
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(
            f"[账户/总权益] 拉取失败 | 关键参数: <无> | 结果: [REJECT], 耗时 【{latency}ms】 | 可能原因: 接口未返回预期字段，异常: {e}")
        return ExecStatus.REJECT, 0.0


def fetch_total_equity(exchange):
    status, equity = get_total_equity(exchange)
    return equity if status == ExecStatus.OK else 0.0


def fetch_account_equity(exchange):
    balance = exchange.fetch_balance(params={"type": "future"})
    value = float(balance["info"]["totalMarginBalance"])
    if not math.isfinite(value): raise ValueError("总权益不是有效数值")
    return value


def count_nonzero_positions(exchange):
    positions = exchange.fetch_positions(params={"type": "future"})
    if not isinstance(positions, list): raise ValueError("持仓接口未返回列表")
    count = 0
    for p in positions:
        qty = float(p.get("contracts") or (p.get("info") or {}).get("positionAmt", 0))
        if not math.isfinite(qty): raise ValueError("持仓数量不是有效数值")
        count += abs(qty) > 0
    return count


def fetch_positions_map(exchange):
    """提取持仓快照。出参形貌: dict, Key: 'BTC/USDT:USDT_LONG', Value: float(带符号数量)"""
    cache = {}
    for pos in exchange.fetch_positions():
        amt = float(pos["info"]["positionAmt"])
        if amt == 0: continue
        side = str(pos["info"].get("positionSide", "")).upper()
        if not side or side == "BOTH": side = "LONG" if amt > 0 else "SHORT"
        cache[make_position_key(pos['symbol'], side)] = amt
    return cache


def fetch_position_qty(exchange, symbol, position_side):
    for p in exchange.fetch_positions([symbol]) or []:
        info = p.get("info") or {}
        ps = str(info.get("positionSide") or p.get("side") or "").upper()
        if ps == position_side.upper():
            return abs(float(p.get("contracts") or info.get("positionAmt") or 0))
    return 0.0


def is_hedge_mode(exchange):
    r = exchange.fapiPrivateGetPositionSideDual()
    return str(r.get("dualSidePosition")).lower() == "true"


def normalize_order_status(raw):
    return _CCXT_STATUS_MAP.get(str(raw).upper(), OS_UNKNOWN)


def to_uni_order(o):
    info = o.get("info") or {}
    status = normalize_order_status(str(info.get("status") or o.get("status") or ""))
    amount = float(o.get("amount") or info.get("origQty") or 0.0)
    filled = float(o.get("filled") or info.get("executedQty") or 0.0)

    # 彻底平仓时的残留撤单校验，避免状态机死锁
    if status == OS_FILLED and amount > 0 and filled < amount * (1 - 1e-9):
        status = OS_CANCELED

    return UniOrder(
        coid=o.get("clientOrderId") or info.get("clientOrderId") or "",
        ex_id=str(o.get("id") or info.get("orderId") or ""),
        status=status,
        price=float(o.get("price") or info.get("price") or 0.0),
        stop_price=float(o.get("stopPrice") or info.get("stopPrice") or 0.0),
        amount=amount, filled=filled,
        avg_price=float(o.get("average") or info.get("avgPrice") or 0.0),
        side=str(o.get("side") or info.get("side") or "").lower(),
        ts=int(o.get("lastTradeTimestamp") or o.get("lastUpdateTimestamp") or o.get("timestamp") or 0),
        raw=o,
    )


def extract_order_view(o):
    o = o or {}
    return {
        "status": normalize_order_status(o.get("status", "")),
        "filled": o.get("filled"),
        "avg_price": o.get("average") or o.get("price") or "",
        "order_id": str(o.get("id", "")),
    }


def order_client_oid(o):
    return str((o or {}).get("clientOrderId") or ((o or {}).get("info") or {}).get("clientOrderId") or "")


def order_exchange_oid(o):
    return str((o or {}).get("id") or ((o or {}).get("info") or {}).get("orderId") or "")


def make_open_order_stub(exchange_oid, client_oid):
    return {"id": exchange_oid, "clientOrderId": client_oid, "info": {"clientOrderId": client_oid}}


def fetch_open_orders(exchange, symbol=None):
    if symbol is None:
        exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
        return exchange.fetch_open_orders()
    return exchange.fetch_open_orders(symbol)


def fetch_open_orders_grouped(exchange):
    cache = {}
    for order in fetch_open_orders(exchange): cache.setdefault(order["symbol"], []).append(order)
    return cache


def fetch_open_orders_map(exchange, symbol, coid_prefix):
    return {u.coid: u for o in exchange.fetch_open_orders(symbol) or [] if
            (u := to_uni_order(o)).coid.startswith(coid_prefix)}


def fetch_open_algo_orders_map(exchange, symbol, coid_prefix):
    out = {}
    market_id = to_market_id(symbol)
    for a in exchange.fapiPrivateGetOpenAlgoOrders({"symbol": market_id}) or []:
        coid = a.get("clientAlgoId") or a.get("clientOrderId") or ""
        if not coid or not coid.startswith(coid_prefix): continue
        out[coid] = UniOrder(
            coid=coid, ex_id=str(a.get("algoId") or a.get("orderId") or ""), status=OS_OPEN,
            stop_price=float(a.get("triggerPrice") or a.get("stopPrice") or 0.0),
            amount=float(a.get("quantity") or a.get("origQty") or 0.0),
            filled=float(a.get("executedQty") or 0.0),
            side=str(a.get("side") or "").lower(),
            ts=int(a.get("bookTime") or a.get("time") or 0), raw=a
        )
    return out


def index_open_orders(exchange, kind="normal"):
    fetch, id_key = ((exchange.fapiPrivateGetOpenOrders, "orderId") if kind == "normal"
                     else (exchange.fapiPrivateGetOpenAlgoOrders, "algoId"))
    rows = fetch({})
    if not isinstance(rows, list): raise ValueError("挂单接口未返回列表，不能视为零挂单")
    return {(str(o["symbol"]), str(o[id_key])): o for o in rows}


def dedup_algo_orders(algo_map, normal_map):
    # : O(N^2) 嵌套查询去重，挂单量极大时存在性能问题，建议保持单账户挂单总数在一个合理范围。
    if algo_map is None: return None
    return sum(
        not (normal_map is not None and str(o.get("actualOrderId") or "") not in ("", "0") and (
        str(o["symbol"]), str(o["actualOrderId"])) in normal_map)
        for o in algo_map.values()
    )


def fetch_recent_orders(exchange, symbol, limit=50):
    return exchange.fetch_orders(symbol, limit=limit)


def fetch_recent_orders_map(exchange, symbol, limit=50):
    return {str(o.get("id")): o for o in fetch_recent_orders(exchange, symbol, limit=limit)}


def fetch_order_by_id(exchange, symbol, order_id):
    return exchange.fetch_order(order_id, symbol)


def fetch_single_order(exchange, symbol, client_oid):
    t0 = time.perf_counter()
    try:
        order = exchange.fetch_order(client_oid, symbol, params={"origClientOrderId": client_oid})
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[订单/点查] 穿透核实挂单 | 关键参数: <CID: {client_oid}> | 结果: [OK], 耗时 【{latency}ms】, 状态 【{order['status']}】")
        return order
    except InvalidOrder:
        # : 捕获 InvalidOrder 直接伪造为 canceled 状态可能掩盖因单号传错导致的问题，若上层强依赖此状态存在风险。
        logger.warning(
            f"[订单/点查] 挂单已不在活跃视窗 | 关键参数: <CID: {client_oid}> | 结果: [FAKE_CANCELED] | 可能原因: 订单已被完全撮合、撤除或被服务端定期清理。")
        return {"status": "canceled", "filled": 0.0, "average": 0.0}
    except Exception as e:
        logger.error(
            f"[订单/点查] 彻底查询失败 | 关键参数: <CID: {client_oid}> | 结果: [FAIL] | 可能原因: 网络堵塞或API超频，异常: {e}")
        return None


def fetch_order_uni(exchange, symbol, client_oid, throttle_gap=0.0):
    fetchers = (
        lambda: fetch_single_order(exchange, symbol, client_oid),
        lambda: exchange.fetch_order(client_oid, symbol, {"origClientOrderId": client_oid})
    )
    for idx, fetcher in enumerate(fetchers):
        try:
            throttle(exchange, throttle_gap)
            if o := fetcher(): return to_uni_order(o), None
            if idx == len(fetchers) - 1: return ORDER_NOT_FOUND, None
        except Exception as e:
            if is_order_not_found(e): return ORDER_NOT_FOUND, None
            return None, e
    return None, None


def fetch_all_open_orders_unified(exchange, symbol):
    """出参形貌: list[dict]，其中 dict 包含 'client_oid', 'exchange_oid', 'status' 等基础字段。"""
    market_id = exchange.market(symbol)['id']
    unified_orders = []

    try:
        for o in exchange.fapiPrivateGetOpenOrders({'symbol': market_id}):
            unified_orders.append({
                'client_oid': o.get('clientOrderId', ''), 'exchange_oid': str(o.get('orderId', '')),
                'symbol': symbol, 'side': o.get('side', '').lower(), 'type': o.get('type', ''),
                'price': float(o.get('price', 0.0)), 'stop_price': float(o.get('stopPrice', 0.0)),
                'amount': float(o.get('origQty', 0.0)), 'filled': float(o.get('executedQty', 0.0)),
                'status': OS_OPEN, 'source': 'NORMAL'
            })
    except Exception as e:
        logger.error(
            f"[订单/全量查询] 普通挂单区拉取失败 | 关键参数: <{symbol}> | 结果: [FAIL] | 可能原因: 接口异常: {e}")

    try:
        for o in exchange.fapiPrivateGetOpenAlgoOrders({'symbol': market_id}):
            unified_orders.append({
                'client_oid': o.get('clientAlgoId', ''), 'exchange_oid': str(o.get('algoId', '')),
                'symbol': symbol, 'side': o.get('side', '').lower(),
                'type': o.get('algoType', o.get('orderType', 'STOP_MARKET')),
                'price': float(o.get('price', 0.0)),
                'stop_price': float(o.get('triggerPrice', o.get('stopPrice', 0.0))),
                'amount': float(o.get('quantity', o.get('origQty', 0.0))), 'filled': float(o.get('executedQty', 0.0)),
                'status': OS_OPEN, 'source': 'ALGO'
            })
    except Exception as e:
        logger.error(
            f"[订单/全量查询] 算法条件单区拉取失败 | 关键参数: <{symbol}> | 结果: [FAIL] | 可能原因: 接口异常: {e}")

    return unified_orders


def execute_order(exchange, symbol, side, amount, client_oid, order_type='market', price=None, reduce_only=False,
                  position_side="LONG"):
    # : 双向持仓模式下直接透传 position_side 可能与 reduce_only 语义产生冲突，上层必须确保传入合法的指令组合。
    if order_type in ['limit', 'maker'] and price is None:
        return ExecResult(ExecStatus.REJECT, client_oid, error_msg="限价单必须提供 price 参数", kind=ErrKind.INVALID)

    t0 = time.perf_counter()
    params = {'newClientOrderId': client_oid}
    if reduce_only: params['reduceOnly'] = True
    if position_side: params['positionSide'] = position_side.upper()
    if order_type == 'maker': params['postOnly'] = True

    ccxt_type = 'limit' if order_type in ['limit', 'maker'] else 'market'
    pos_side_str = f"方向:{position_side.upper()}" if position_side else "方向:单向"
    logger.info(
        f"[执行/下单请求] | 关键参数: <CID: {client_oid}, {symbol} {side.upper()}, {pos_side_str}, 量: {amount}, 价: {price}> | 结果: [SUBMITTING]")

    try:
        order = exchange.create_order(symbol=symbol, type=ccxt_type, side=side, amount=amount, price=price,
                                      params=params)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[执行/下单成功] | 关键参数: <CID: {client_oid}, EID: {order.get('id')}> | 结果: [OK], 耗时 【{latency}ms】")
        return ExecResult(ExecStatus.OK, client_oid, exchange_oid=order.get('id'), latency_ms=latency, raw_data=order)
    except NetworkError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        err_msg = f"物理断联，订单可能已进撮合引擎: {e}"
        logger.critical(
            f"[执行/网络丢失] | 关键参数: <CID: {client_oid}> | 结果: [UNKNOWN], 耗时 【{latency}ms】 | 可能原因: 极其凶险的网络波动，绝对禁止原单号重试！")
        return ExecResult(ExecStatus.UNKNOWN, client_oid, latency_ms=latency, error_msg=err_msg,
                          kind=ErrKind.UNKNOWN_RESULT)
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(
            f"[执行/业务拒单] | 关键参数: <CID: {client_oid}> | 结果: [REJECT], 耗时 【{latency}ms】 | 可能原因: 触碰风控、参数不合规或可用资金不足，异常: {e}")
        return ExecResult(ExecStatus.REJECT, client_oid, latency_ms=latency, error_msg=str(e), kind=classify_error(e))


def place_stop_market_order(exchange, symbol, side, amount, stop_price, client_oid, position_side,
                            working_type="MARK_PRICE"):
    t0 = time.perf_counter()
    try:
        # : priceProtect 强制要求传入字符串 "FALSE"，属于币安非标接口的潜规则设计，切勿重构为布尔值。
        params = {
            "stopPrice": exchange.price_to_precision(symbol, stop_price),
            "workingType": working_type, "positionSide": position_side,
            "newClientOrderId": client_oid, "priceProtect": "FALSE",
        }
        o = exchange.create_order(symbol=symbol, type="STOP_MARKET", side=side,
                                  amount=float(exchange.amount_to_precision(symbol, amount)), price=None, params=params)
        return ExecResult(ExecStatus.OK, client_oid, exchange_oid=str((o or {}).get("id") or ""),
                          latency_ms=int((time.perf_counter() - t0) * 1000), raw_data=o)
    except Exception as e:
        return make_fail_result(client_oid, e, latency_ms=int((time.perf_counter() - t0) * 1000))


def cancel_order_by_id(exchange, symbol, order_id):
    return exchange.cancel_order(order_id, symbol)


def cancel_order_by_client_oid(exchange, symbol, client_oid, throttle_gap=0.0):
    throttle(exchange, throttle_gap)
    try:
        exchange.cancel_order(client_oid, symbol, {"origClientOrderId": client_oid})
        return True, "NORMAL", None
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in ("-2013", "order not found", "does not exist")): return True, "NORMAL", None
        if "-2011" not in msg and "unknown order" not in msg: return False, "NORMAL", e

    throttle(exchange, throttle_gap)
    try:
        exchange.fapiPrivateDeleteAlgoOrder({"symbol": to_market_id(symbol), "clientAlgoId": client_oid})
        return True, "ALGO", None
    except Exception as algo_err:
        a_msg = str(algo_err).lower()
        if any(k in a_msg for k in ("-2011", "unknown", "not exist", "does not exist")): return True, "ALGO_GONE", None
        return False, "ALGO", algo_err


def cancel_single_order(exchange, symbol, order_id, is_client_id=False):
    t0 = time.perf_counter()
    try:
        res = exchange.cancel_order(order_id, symbol, params={'origClientOrderId': order_id} if is_client_id else {})
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[执行/精准撤单] | 关键参数: <ID: {order_id}> | 结果: [OK], 耗时 【{latency}ms】")
        return ExecResult(ExecStatus.OK, client_oid=order_id if is_client_id else "", exchange_oid=order_id,
                          latency_ms=latency, raw_data=res)
    except InvalidOrder:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[执行/幂等撤单] | 关键参数: <ID: {order_id}> | 结果: [OK_SKIP], 耗时 【{latency}ms】 | 可能原因: 订单已被他人/引擎撤除或刚刚成交")
        return ExecResult(ExecStatus.OK, client_oid=order_id if is_client_id else "", exchange_oid=order_id,
                          latency_ms=latency)
    except NetworkError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        return ExecResult(ExecStatus.UNKNOWN, client_oid="", latency_ms=latency, error_msg=str(e),
                          kind=ErrKind.UNKNOWN_RESULT)
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        return ExecResult(ExecStatus.REJECT, client_oid="", latency_ms=latency, error_msg=str(e),
                          kind=classify_error(e))


def cancel_all_orders_of_symbol(exchange, symbol):
    return exchange.cancel_all_orders(symbol)


def cancel_all_orders(exchange, symbol):
    t0 = time.perf_counter()
    try:
        cancel_all_orders_of_symbol(exchange, symbol)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[执行/核弹级撤单] | 关键参数: <{symbol}> | 结果: [OK], 耗时 【{latency}ms】")
        return True
    except Exception as e:
        logger.error(f"[执行/核弹级撤单] | 关键参数: <{symbol}> | 结果: [REJECT] | 可能原因: API网络中断，异常: {e}")
        return False


def cancel_order_universal(exchange, symbol, client_oid=None, order_id=None):
    t0 = time.perf_counter()
    market_id = exchange.market(symbol)['id']
    target_id = client_oid or order_id

    try:
        res = exchange.cancel_order(order_id, symbol, params={'origClientOrderId': client_oid} if client_oid else {})
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[执行/普单通道撤销] | 关键参数: <ID: {target_id}> | 结果: [OK], 耗时 【{latency}ms】")
        return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency,
                          raw_data=res)
    except InvalidOrder:
        logger.info(f"[执行/撤单降级] 普单通道无目标，转移至算法条件单通道 | 关键参数: <ID: {target_id}>")
    except Exception as e:
        logger.error(
            f"[执行/普单撤销异常] | 关键参数: <ID: {target_id}> | 结果: [FAIL] | 可能原因: 普单网络错误，转备用路线，异常: {e}")

    try:
        algo_params = {'symbol': market_id}
        if client_oid: algo_params['clientAlgoId'] = client_oid
        if order_id: algo_params['algoId'] = int(order_id)
        res = exchange.fapiPrivateDeleteAlgoOrder(algo_params)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[执行/条件单通道撤销] | 关键参数: <ID: {target_id}> | 结果: [OK], 耗时 【{latency}ms】")
        return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency,
                          raw_data=res)
    except Exception as algo_err:
        latency = int((time.perf_counter() - t0) * 1000)
        err_msg = str(algo_err).lower()
        if "unknown" in err_msg or "-2011" in err_msg or "not exist" in err_msg:
            logger.info(
                f"[执行/终态幂等撤销] 两大通道均确认无痕，目标已不存在 | 关键参数: <ID: {target_id}> | 结果: [OK], 耗时 【{latency}ms】")
            return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency)
        logger.error(
            f"[执行/多通道撤销全面失败] | 关键参数: <ID: {target_id}> | 结果: [REJECT] | 可能原因: 严重网络中断，异常: {algo_err}")
        return ExecResult(ExecStatus.REJECT, client_oid or "", latency_ms=latency, error_msg=str(algo_err))


if __name__ == "__main__":
    print("==================================================")
    print(">>> 启动量化主策略引擎 & 条件单完整生命周期测试 <<<")
    print("==================================================")

    API_KEY = get_config("myself_biance_api_key") or get_config("nana_biance_api_key")
    SECRET_KEY = get_config("myself_biance_api_secret") or get_config("nana_biance_api_secret")
    SYMBOL = "BTC/USDT:USDT"

    proxies = None if platform.system().lower() == "linux" else {"http": "http://127.0.0.1:7890",
                                                                 "https": "http://127.0.0.1:7890"}

    try:
        bot_exchange = init_exchange(API_KEY, SECRET_KEY, proxies=proxies)
    except Exception as e:
        print(f">>> 交易所初始化失败，程序退出: {e}")
        exit(1)

    print("\n--- [场景 1] 开机巡视：账户权益与当前持仓 ---")
    get_total_equity(bot_exchange)
    status, usdt, pos = get_symbol_status(bot_exchange, SYMBOL)
    if status == ExecStatus.OK:
        print(f"当前可用保证金: {usdt:.2f} USDT | 当前 {SYMBOL} 净持仓: {pos} BTC")
    else:
        print("⚠️ 无法获取持仓及余额状态，请排查网络！")

    print("\n--- [场景 2] 防呆设计校验：缺失必填参数拦截 ---")
    bad_intent_id = f"bad_test_{uuid.uuid4().hex[:8]}"
    res_bad = execute_order(bot_exchange, SYMBOL, "buy", 0.001, bad_intent_id, order_type="limit", price=None)
    if res_bad.status == ExecStatus.REJECT:
        print(f"🛡️ 防呆机制生效，本地拦截非法意图: {res_bad.error_msg}")

    print("\n--- [场景 3] 实盘测试：挂出一张止损市价条件单 (STOP_MARKET) ---")
    algo_client_oid = f"test_sl_{uuid.uuid4().hex[:8]}"
    algo_amount = 0.002
    target_eid = None

    try:
        current_price = fetch_last_price(bot_exchange, SYMBOL)
        print(f"当前市场价: {current_price} USDT")
        algo_stop_price = round(current_price * 0.85, 1)

        print(
            f"准备提交条件单 -> CID:{algo_client_oid} | 方向:SELL(止损) | 触发价:{algo_stop_price} | 数量:{algo_amount}")
        res_algo = place_stop_market_order(bot_exchange, SYMBOL, "sell", algo_amount, algo_stop_price, algo_client_oid,
                                           "LONG", working_type="MARK_PRICE")
        if res_algo.ok:
            target_eid = res_algo.ex_id
            print(f"🎉 条件单挂单成功! 本地CID: {algo_client_oid} | 交易所EID(AlgoID): {target_eid}")
        else:
            print(f"❌ 挂条件单失败 (正常拦截): [{res_algo.kind.value}] {res_algo.err}")
    except Exception as e:
        print(f"❌ 挂条件单致命失败: {e}")

    print("\n--- [场景 4] 使用统一接口拉取当前盘口所有活跃挂单 ---")
    time.sleep(1.0)
    all_open = fetch_all_open_orders_unified(bot_exchange, SYMBOL)
    print(f"当前共查到 {len(all_open)} 张活动订单:")
    for o in all_open:
        print(
            f" -> [{o['source']}] CID:{o['client_oid']} | EID:{o['exchange_oid']} | 类型:{o['type']} | 触发价:{o['stop_price']} | 挂单价:{o['price']} | 数量:{o['amount']}")

    print("\n--- [场景 5] 验证通用撤单接口：精准撤销刚创建的条件单 ---")
    cancel_cid = algo_client_oid
    cancel_eid = target_eid

    if cancel_cid or cancel_eid:
        print(f"正在精准撤除目标: CID={cancel_cid} (EID={cancel_eid}) ...")
        res_cancel = cancel_order_universal(bot_exchange, SYMBOL, client_oid=cancel_cid, order_id=cancel_eid)
        if res_cancel.status == ExecStatus.OK:
            print("🎉 撤单指令已通过通用/算法接口下发成功！")
        else:
            print(f"❌ 撤单失败: {res_cancel.error_msg}")
    else:
        print("⚠️ 未找到可供撤除的订单 ID，跳过撤单阶段。")

    print("\n--- [场景 6] 状态核验：再次查询确认条件单是否彻底清理 ---")
    time.sleep(1.0)
    all_open_after = fetch_all_open_orders_unified(bot_exchange, SYMBOL)
    remained = [o for o in all_open_after if
                (cancel_cid and o['client_oid'] == cancel_cid) or (cancel_eid and o['exchange_oid'] == cancel_eid)]

    if not remained:
        print("✅ 确认成功！该条件单已在盘口与服务端彻底消失！逻辑闭环跑通！")
    else:
        print("⚠️ 警告：订单依然残留在盘口，请核对日志排查异常！")