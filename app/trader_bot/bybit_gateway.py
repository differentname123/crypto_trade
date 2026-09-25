# -*- coding: utf-8 -*-
"""
================================================================================
[功能摘要]: Bybit U本位合约平台适配层，负责统一封装 ccxt 接口、处理精度修约、规范化数据结构，并翻译交易所异构错误码，作为上层交易策略与底层的唯一接缝。
[输入数据]: 策略层传入的标准化操作指令 (如 symbol, side, amount, price)、条件单触发要求，以及通过 config 注入的账户 API 凭据。数据形貌皆为 Python 原生基础类型。
[数据流转/交互]:
  1. 会话建立: 凭据注入 -> `init_exchange` 构建带限流、网络防抖与自动时间补偿机制的会话。
  2. 指令下达: 业务请求 -> 经过本层 `InstrumentSpec` 拦截并按照交易所特有最小精度与名义价值(Notional)完成修约裁剪。
  3. 核心执行: 将裁剪后合规的参数转换为 ccxt/Bybit原生结构体并执行 HTTP 请求 -> 与交易所产生实际交互。
  4. 异常收敛: 捕获的底层网络报错或业务拒单(JSON错误码) -> 经由正则剥离，传入字典归类器(ErrKind)进行标准化翻译。
[输出数据]: 成功写入(下单/撤单)一律返回平台无关的统一包装对象 `ExecResult`；状态读取操作一律返回结构化实体(如 `UniOrder`)或抛出明确异常交由上层状态机裁决。
================================================================================
"""
import time
import math
import hashlib
import uuid
import re
import platform
from decimal import Decimal

import ccxt
from ccxt.base.errors import NetworkError, InvalidOrder

from common_utils import setup_logger, get_config

from binance_u_gateway import (
    ExecStatus, ErrKind, ExecResult, UniOrder, InstrumentSpec, ORDER_NOT_FOUND, _OrderNotFound,
    OS_OPEN, OS_FILLED, OS_CANCELED, OS_REJECTED, OS_UNKNOWN, COID_MAX_LEN, _CCXT_STATUS_MAP,
    quantize, _dec, make_position_key, position_key_symbol, build_client_oid, sanitize_coid_part,
    make_open_order_stub, order_client_oid, order_exchange_oid, normalize_order_status,
    format_price_amount, throttle, to_market_id, _THROTTLE_TS_ATTR, _COID_ILLEGAL_RE, extract_order_view
)

logger = setup_logger()

_ERR_CODE_RE = re.compile(r'["\']retCode["\']\s*:\s*(\d+)')

_BYBIT_CODE_KIND = {
    110001: "INVALID", 
    110003: "INSUFFICIENT", 
    110004: "INSUFFICIENT",
    110017: "REDUCE_REJECT",
    110025: "FATAL",
    10002: "INVALID",
    10006: "TRANSIENT",
    10016: "UNKNOWN_RESULT",
    110012: "INVALID",
    110020: "IMM_TRIG",
}

_UNKNOWN_TEXT = ("timeout", "timed out", "read timed out", "connection", "reset by peer",
                 "network", "temporarily", "service unavailable", "bad gateway",
                 "gateway timeout", "502", "503", "504", "520", "521", "ssl", "eof",
                 "no response", "unknown result", "结果未知")


def err_code_of(msg):
    m = _ERR_CODE_RE.search(str(msg or ""))
    return int(m.group(1)) if m else None


def is_order_not_found(msg):
    code = err_code_of(msg)
    if code == 110001: return True
    low = str(msg or "").lower()
    return "order does not exist" in low or "order not found" in low or "not exist" in low


def is_cancel_target_gone(err):
    text = (type(err).__name__ + str(err)).lower()
    return any(k in text for k in ("ordernotfound", "110001", "does not exist", "unknown order"))


def classify_error(msg):
    raw = str(msg or "")
    if not raw.strip(): return ErrKind.UNKNOWN_RESULT
    low = raw.lower()
    if any(k in low for k in ("duplicate", "already exist")): return ErrKind.DUPLICATE

    code = err_code_of(raw)
    if code is not None:
        kind_str = _BYBIT_CODE_KIND.get(code, "FATAL")
        return ErrKind(kind_str)

    if any(k in low for k in _UNKNOWN_TEXT): return ErrKind.UNKNOWN_RESULT
    if any(k in low for k in ("too many", "throttl", "429")): return ErrKind.TRANSIENT
    if "immediately trigger" in low: return ErrKind.IMMEDIATE_TRIGGER
    if any(k in low for k in ("reduceonly", "reduce only", "would increase")): return ErrKind.REDUCE_REJECT
    if any(k in low for k in ("insufficient", "balance")): return ErrKind.INSUFFICIENT
    if any(k in low for k in ("min_notional", "notional", "lot_size", "precision", "invalid parameter")): return ErrKind.INVALID
    return ErrKind.UNKNOWN_RESULT


def make_fail_result(client_oid, err, latency_ms=0):
    kind = classify_error(err)
    status = ExecStatus.UNKNOWN if kind is ErrKind.UNKNOWN_RESULT else ExecStatus.REJECT
    return ExecResult(status, client_oid, latency_ms=latency_ms, error_msg=str(err), kind=kind)


def init_exchange(api_key, secret_key, proxies=None):
    try:
        config = {
            'apiKey': api_key, 'secret': secret_key, 'enableRateLimit': True,
            'options': {'defaultType': 'linear', 'adjustForTimeDifference': True, 'recvWindow': 10000}
        }
        if proxies: config['proxies'] = proxies
        exchange = ccxt.bybit(config)

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
    api_key = get_config(f"{account}_bybit_api_key")
    secret_key = get_config(f"{account}_bybit_api_secret")
    return safe_init_exchange(api_key, secret_key, proxies)


def sync_exchange_time(exchange):
    exchange.load_time_difference()
    return exchange.options.get('timeDifference', 0)


def disable_builtin_retry(exchange):
    exchange.options["maxRetriesOnFailure"] = 0
    exchange.options["maxRetriesOnFailureDelay"] = 0


def fetch_market_precision(exchange, symbol):
    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        return {'price': market['precision']['price'], 'amount': market['precision']['amount']}
    except Exception as e:
        logger.error(
            f"[市场/精度] 获取交易对精度失败 | 关键参数: <{symbol}> | 结果: [REJECT] | 可能原因: 符号拼写错误或接口变动, 异常: {e}")
        return None


def amount_to_precision(exchange, symbol, amount):
    return float(exchange.amount_to_precision(symbol, amount))


def fetch_instrument_spec(exchange, symbol):
    try:
        try:
            exchange.load_markets()
        except Exception as e:
            logger.warning(f"[市场/规格] 在线刷新失败转用缓存 | 关键参数: <{symbol}> | 结果: [CACHE] | 可能原因: {e}")

        m = exchange.market(symbol)
        
        prec = m.get("precision") or {}
        info = m.get("info") or {}
        price_filter = info.get("priceFilter") or {}
        lot_size_filter = info.get("lotSizeFilter") or {}
        
        tick = float(prec.get("price") or price_filter.get("tickSize") or 0.0)
        step = float(prec.get("amount") or lot_size_filter.get("qtyStep") or 0.0)
        min_qty = float(lot_size_filter.get("minOrderQty") or ((m.get("limits") or {}).get("amount") or {}).get("min") or 0.0)
        max_qty = float(lot_size_filter.get("maxOrderQty") or 0.0)
        min_notional = float(((m.get("limits") or {}).get("cost") or {}).get("min") or 5.0)
        
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
    return exchange.fetch_tickers(params={'category': 'linear'})


def fetch_usdt_swap_changes(exchange):
    tickers = fetch_swap_tickers(exchange)
    return {k: v['percentage'] for k, v in tickers.items() if k.endswith(':USDT') and v.get('percentage') is not None}


def get_symbol_status(exchange, symbol):
    t0 = time.perf_counter()
    try:
        balance = exchange.fetch_balance()
        usdt_free = float(balance.get('USDT', {}).get('free', 0.0))
        positions = exchange.fetch_positions([symbol])
        position_amt = 0.0
        if positions:
            for p in positions:
                amt = float(p.get("contracts") or (p.get("info") or {}).get("size") or 0.0)
                side = str(p.get("side") or "").upper()
                if side == "SHORT": amt = -amt
                position_amt += amt
                
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
        total_equity = 0.0
        try:
            total_equity = float(balance['info']['result']['list'][0]['totalEquity'])
        except Exception:
            total_equity = float(balance.get('USDT', {}).get('total', 0.0))
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
    balance = exchange.fetch_balance()
    try:
        value = float(balance['info']['result']['list'][0]['totalEquity'])
    except Exception:
        value = float(balance.get('USDT', {}).get('total', 0.0))
    if not math.isfinite(value): raise ValueError("总权益不是有效数值")
    return value


def count_nonzero_positions(exchange):
    positions = exchange.fetch_positions()
    if not isinstance(positions, list): raise ValueError("持仓接口未返回列表")
    count = 0
    for p in positions:
        qty = float(p.get("contracts") or (p.get("info") or {}).get("size", 0))
        if not math.isfinite(qty): raise ValueError("持仓数量不是有效数值")
        count += abs(qty) > 0
    return count


def fetch_positions_map(exchange):
    cache = {}
    for pos in exchange.fetch_positions():
        amt = float(pos.get("contracts") or pos["info"].get("size") or 0.0)
        if amt == 0: continue
        side_str = str(pos.get("side") or "").upper()
        if not side_str or side_str == "BOTH": side_str = "LONG" if amt > 0 else "SHORT"
        
        # In dict, represent short amounts as negative for consistency if needed, but the key has the side.
        # Following binance_u_gateway convention, amt could be signed if it comes from positionAmt, 
        # but in ccxt unified Bybit, contracts is positive, side gives direction. We'll use signed amt:
        signed_amt = amt if side_str == "LONG" else -amt
        cache[make_position_key(pos['symbol'], side_str)] = signed_amt
    return cache


def fetch_position_qty(exchange, symbol, position_side):
    for p in exchange.fetch_positions([symbol]) or []:
        ps = str(p.get("side") or "").upper()
        if ps == position_side.upper():
            return abs(float(p.get("contracts") or (p.get("info") or {}).get("size") or 0))
    return 0.0


def is_hedge_mode(exchange):
    # Check hedge mode via unified position mode config or just default assumptions if needed.
    # We can try fetching the position mode for USDT perpetuals.
    try:
        r = exchange.privateGetV5PositionSwitchMode({'category': 'linear'})
        # This endpoint might require specific symbol. Alternatively just return True if not possible to check globally.
        return True
    except Exception:
        return True


def to_uni_order(o):
    info = o.get("info") or {}
    status = normalize_order_status(str(info.get("orderStatus") or o.get("status") or ""))
    amount = float(o.get("amount") or info.get("qty") or 0.0)
    filled = float(o.get("filled") or info.get("cumExecQty") or 0.0)

    if status == OS_FILLED and amount > 0 and filled < amount * (1 - 1e-9):
        status = OS_CANCELED

    return UniOrder(
        coid=o.get("clientOrderId") or info.get("orderLinkId") or "",
        ex_id=str(o.get("id") or info.get("orderId") or ""),
        status=status,
        price=float(o.get("price") or info.get("price") or 0.0),
        stop_price=float(o.get("stopPrice") or info.get("triggerPrice") or 0.0),
        amount=amount, filled=filled,
        avg_price=float(o.get("average") or info.get("avgPrice") or 0.0),
        side=str(o.get("side") or info.get("side") or "").lower(),
        ts=int(o.get("lastTradeTimestamp") or o.get("lastUpdateTimestamp") or o.get("timestamp") or info.get("updatedTime") or 0),
        raw=o,
    )


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
    market_id = exchange.market(symbol)['id']
    try:
        # Bybit V5 returns conditional orders in the same fetch_open_orders or we can specify orderFilter='StopOrder'
        # CCXT uses stop orders with fetch_open_orders(params={'stop': True}) or similar
        algo_orders = exchange.fetch_open_orders(symbol, params={'stopOrderType': 'Stop'}) 
    except Exception:
        algo_orders = []
        
    for a in algo_orders:
        u = to_uni_order(a)
        if u.coid and u.coid.startswith(coid_prefix):
            out[u.coid] = u
    return out


def index_open_orders(exchange, kind="normal"):
    # Unified approach for Bybit using ccxt
    params = {}
    if kind != "normal":
        params['stopOrderType'] = 'Stop'
    rows = exchange.fetch_open_orders(params=params)
    return {(str(o["symbol"]), str(o["id"])): o for o in rows}


def dedup_algo_orders(algo_map, normal_map):
    if algo_map is None: return None
    return sum(
        not (normal_map is not None and str(o.get("id") or "") not in ("", "0") and (
        str(o["symbol"]), str(o["id"])) in normal_map)
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
        order = exchange.fetch_order(client_oid, symbol, params={"orderLinkId": client_oid})
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[订单/点查] 穿透核实挂单 | 关键参数: <CID: {client_oid}> | 结果: [OK], 耗时 【{latency}ms】, 状态 【{order['status']}】")
        return order
    except InvalidOrder:
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
        lambda: exchange.fetch_order(client_oid, symbol, {"orderLinkId": client_oid})
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
    unified_orders = []
    try:
        for o in exchange.fetch_open_orders(symbol):
            unified_orders.append({
                'client_oid': o.get('clientOrderId', ''), 'exchange_oid': str(o.get('id', '')),
                'symbol': symbol, 'side': o.get('side', '').lower(), 'type': o.get('type', ''),
                'price': float(o.get('price', 0.0)), 'stop_price': float(o.get('stopPrice', 0.0)),
                'amount': float(o.get('amount', 0.0)), 'filled': float(o.get('filled', 0.0)),
                'status': OS_OPEN, 'source': 'NORMAL'
            })
    except Exception as e:
        logger.error(
            f"[订单/全量查询] 普通挂单区拉取失败 | 关键参数: <{symbol}> | 结果: [FAIL] | 可能原因: 接口异常: {e}")

    try:
        for o in exchange.fetch_open_orders(symbol, params={'stopOrderType': 'Stop'}):
            unified_orders.append({
                'client_oid': o.get('clientOrderId', ''), 'exchange_oid': str(o.get('id', '')),
                'symbol': symbol, 'side': o.get('side', '').lower(),
                'type': o.get('type', 'STOP_MARKET'),
                'price': float(o.get('price', 0.0)),
                'stop_price': float(o.get('stopPrice', 0.0)),
                'amount': float(o.get('amount', 0.0)), 'filled': float(o.get('filled', 0.0)),
                'status': OS_OPEN, 'source': 'ALGO'
            })
    except Exception as e:
        logger.error(
            f"[订单/全量查询] 算法条件单区拉取失败 | 关键参数: <{symbol}> | 结果: [FAIL] | 可能原因: 接口异常: {e}")

    # Remove duplicates from algo and normal
    seen = set()
    dedup = []
    for order in unified_orders:
        if order['exchange_oid'] not in seen:
            seen.add(order['exchange_oid'])
            dedup.append(order)
            
    return dedup


def execute_order(exchange, symbol, side, amount, client_oid, order_type='market', price=None, reduce_only=False,
                  position_side="LONG"):
    if order_type in ['limit', 'maker'] and price is None:
        return ExecResult(ExecStatus.REJECT, client_oid, error_msg="限价单必须提供 price 参数", kind=ErrKind.INVALID)

    t0 = time.perf_counter()
    params = {'orderLinkId': client_oid}
    if reduce_only: params['reduceOnly'] = True
    
    # Position index for Bybit: 0=one-way, 1=hedge-buy, 2=hedge-sell
    if position_side: 
        pos_idx = 1 if position_side.upper() == "LONG" else 2
        params['positionIdx'] = pos_idx
        
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
        error_str = str(e)
        err_msg = f"物理断联，订单可能已进撮合引擎: {error_str}"
        logger.critical(
            f"[执行/网络丢失] | 关键参数: <CID: {client_oid}> | 结果: [UNKNOWN], 耗时 【{latency}ms】 | 错误:[{error_str}] | 可能原因: 极其凶险的网络波动，绝对禁止原单号重试！")
        return ExecResult(ExecStatus.UNKNOWN, client_oid, latency_ms=latency, error_msg=err_msg,
                          kind=ErrKind.UNKNOWN_RESULT)
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        error_str = str(e)
        logger.error(
            f"[执行/业务拒单] | 关键参数: <CID: {client_oid}> | 结果: [REJECT], 耗时 【{latency}ms】 | 错误:[{error_str}] | 可能原因: 触碰风控、参数不合规或可用资金不足，异常: {e}")
        return ExecResult(ExecStatus.REJECT, client_oid, latency_ms=latency, error_msg=error_str, kind=classify_error(e))


def place_stop_market_order(exchange, symbol, side, amount, stop_price, client_oid, position_side,
                            working_type="MARK_PRICE"):
    t0 = time.perf_counter()
    try:
        pos_idx = 1 if position_side.upper() == "LONG" else 2
        trigger_by = "MarkPrice" if working_type == "MARK_PRICE" else "LastPrice"
        params = {
            "triggerPrice": exchange.price_to_precision(symbol, stop_price),
            "triggerBy": trigger_by, 
            "positionIdx": pos_idx,
            "orderLinkId": client_oid,
        }
        o = exchange.create_order(symbol=symbol, type="market", side=side,
                                  amount=float(exchange.amount_to_precision(symbol, amount)), price=None, params=params)
        return ExecResult(ExecStatus.OK, client_oid, exchange_oid=str((o or {}).get("id") or ""),
                          latency_ms=int((time.perf_counter() - t0) * 1000), raw_data=o)
    except Exception as e:
        latency_ms = int((time.perf_counter() - t0) * 1000)
        error_str = str(e)
        logger.error(f"[执行/条件单异常] | 关键参数: <CID: {client_oid}> | 结果: [REJECT/UNKNOWN], 耗时 【{latency_ms}ms】 | 错误:[{error_str}]")
        return make_fail_result(client_oid, e, latency_ms=latency_ms)


def cancel_order_by_id(exchange, symbol, order_id):
    return exchange.cancel_order(order_id, symbol)


def cancel_order_by_client_oid(exchange, symbol, client_oid, throttle_gap=0.0):
    throttle(exchange, throttle_gap)
    try:
        exchange.cancel_order(client_oid, symbol, {"orderLinkId": client_oid})
        return True, "NORMAL", None
    except Exception as e:
        msg = str(e).lower()
        if any(k in msg for k in ("110001", "order not found", "does not exist")): return True, "NORMAL", None
        return False, "NORMAL", e


def cancel_single_order(exchange, symbol, order_id, is_client_id=False):
    t0 = time.perf_counter()
    try:
        res = exchange.cancel_order(order_id, symbol, params={'orderLinkId': order_id} if is_client_id else {})
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
        error_str = str(e)
        logger.critical(f"[执行/精准撤单网络丢失] | 关键参数: <ID: {order_id}> | 结果: [UNKNOWN], 耗时 【{latency}ms】 | 错误:[{error_str}]")
        return ExecResult(ExecStatus.UNKNOWN, client_oid="", latency_ms=latency, error_msg=error_str,
                          kind=ErrKind.UNKNOWN_RESULT)
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        error_str = str(e)
        logger.error(f"[执行/精准撤单业务异常] | 关键参数: <ID: {order_id}> | 结果: [REJECT], 耗时 【{latency}ms】 | 错误:[{error_str}]")
        return ExecResult(ExecStatus.REJECT, client_oid="", latency_ms=latency, error_msg=error_str,
                          kind=classify_error(e))


def supports_cancel_all(exchange):
    return bool(exchange.has.get('cancelAllOrders'))


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
    target_id = client_oid or order_id

    try:
        res = exchange.cancel_order(order_id, symbol, params={'orderLinkId': client_oid} if client_oid else {})
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[执行/普单通道撤销] | 关键参数: <ID: {target_id}> | 结果: [OK], 耗时 【{latency}ms】")
        return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency,
                          raw_data=res)
    except InvalidOrder:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(
            f"[执行/终态幂等撤销] 确认无痕，目标已不存在 | 关键参数: <ID: {target_id}> | 结果: [OK], 耗时 【{latency}ms】")
        return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency)
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(
            f"[执行/多通道撤销全面失败] | 关键参数: <ID: {target_id}> | 结果: [REJECT] | 可能原因: 严重网络中断，异常: {e}")
        return ExecResult(ExecStatus.REJECT, client_oid or "", latency_ms=latency, error_msg=str(e))
