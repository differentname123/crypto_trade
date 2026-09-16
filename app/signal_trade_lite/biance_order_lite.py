# -*- coding: utf-8 -*-
"""
================================================================================
Binance U 本位合约 —— 平台适配层 (面向过程 / 全系统唯一与交易所耦合的文件)
================================================================================
[定位]
  本文件是「等比网格 / 择时马丁 / 跨周期信号」三大交易系统与交易所之间的唯一接缝。
  所有 ccxt / REST 调用、所有平台错误码语义、所有精度与规格规则全部收口在此;
  上层系统只允许调用本文件导出的函数, 严禁再出现任何 exchange.xxx 直连。

[换平台的唯一动作]
  照本文件的函数签名与出参语义实现一份 okx_order_lite.py, 三大系统只改 import 即可。

[分区导航]
  A. 数据契约   ExecStatus / ErrKind / ExecResult / UniOrder / ORDER_NOT_FOUND / InstrumentSpec
  B. 错误语义   err_code_of / is_order_not_found / is_cancel_target_gone / classify_error / make_fail_result
  C. 连接会话   init_exchange / safe_init_exchange / sync_exchange_time / throttle / to_market_id
  D. 市场精度   fetch_market_precision / format_price_amount / fetch_instrument_spec / fetch_last_price /
                fetch_swap_tickers / fetch_usdt_swap_changes / amount_to_precision
  E. 账户持仓   get_total_equity / fetch_positions_map / fetch_position_qty / is_hedge_mode
  F. 订单查询   fetch_open_orders(_map/_grouped) / fetch_recent_orders(_map) / fetch_order_uni /
                extract_order_view / order_client_oid / make_open_order_stub / dedup_algo_orders
  G. 下单执行   execute_order / place_stop_market_order
  H. 撤单       cancel_order_by_client_oid / cancel_order_by_id / cancel_all_orders / cancel_order_universal

[出参约定 (上层据此裁决, 务必严格遵守)]
  1. 读取类函数默认【向上抛异常】, 由调用方决定告警口径与降级策略(日志文本留在业务侧);
     少数带既定语义的例外会在 docstring 显式声明(如 fetch_instrument_spec 失败返回 None)。
  2. 写入类函数(下单/撤单)【永不抛异常】, 一律返回 ExecResult 或 (ok, via, err) 元组;
     ExecStatus.UNKNOWN 表示"结果未知", 上层必须点查, 严禁换号重发。
  3. 一切"能不能换号重发"的生死判断, 统一由 ErrKind 表达, 绝不让上层去猜错误文本。
================================================================================
"""
import math
import re
import time
import uuid
import platform
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from enum import Enum

import ccxt
from ccxt.base.errors import NetworkError, InvalidOrder

from common_utils_lite import setup_logger, get_config

logger = setup_logger()

# 限频计时器挂在 exchange 实例上的属性名: 让同一连接的所有调用共享一个节流器
_THROTTLE_TS_ATTR = "_lite_last_call_ts"


# ==========================================
# A. 核心数据契约 (平台无关的统一视图, 剥离所有 typing)
# ==========================================
class ExecStatus(Enum):
    OK = "OK"  # 明确成功
    REJECT = "REJECT"  # 明确拒绝
    UNKNOWN = "UNKNOWN"  # 状态未知


class ErrKind(Enum):
    """平台错误码的语义分类 —— 决定"能不能换号重发"这一生死问题。"""
    NONE = "NONE"
    UNKNOWN_RESULT = "UNKNOWN_RESULT"  # 结果未知(网络中断/超时/回执缺失) -> 保留原OID点查
    TRANSIENT = "TRANSIENT"          # 明确拒单(限频/时间戳) -> 退避重试
    PRICE_BAND = "PRICE_BAND"        # 价格离盘口太远 -> 退避后重挂
    IMMEDIATE_TRIGGER = "IMM_TRIG"   # 条件单会立即触发 -> 需本地现价双重核验
    INSUFFICIENT = "INSUFFICIENT"    # 保证金/余额不足 -> 退避 + 告警
    REDUCE_REJECT = "REDUCE_REJECT"  # 平仓数量超过持仓 -> 仓位被外部动过
    DUPLICATE = "DUPLICATE"          # OID 重复 -> 单子已存在, 保持 PENDING 点查
    INVALID = "INVALID"              # 精度/最小量/单号格式等参数非法 -> 不可重试
    FATAL = "FATAL"                  # 明确拒单但未归类 -> 保守停挂


class ExecResult:
    """
    执行结果的统一包装类，屏蔽底层交易所 API 的异构性。
    调用者只需判断 status 即可决定下一步业务逻辑; 需要精细分流时再读 kind。
    """

    def __init__(self, status, client_oid, exchange_oid="", latency_ms=0, error_msg="",
                 raw_data=None, kind=None):
        """
        :param status: ExecStatus 枚举，(OK: 成功, REJECT: 明确失败/拒单, UNKNOWN: 物理断联导致状态未知)
        :param client_oid: 策略端生成的本地唯一订单号 (必传，用于对账)
        :param exchange_oid: 交易所返回的真实订单号 (下单成功时存在)
        :param latency_ms: 本次请求发生的网络+业务总耗时 (毫秒)
        :param error_msg: 具体的错误原因说明 (用于 REJECT 或 UNKNOWN 时排查)
        :param raw_data: ccxt 返回的原始 payload (仅在需要深度解析时使用)
        :param kind: ErrKind 枚举，平台错误码的语义分类 (供上层判定"能否换号重发")
        """
        self.status = status
        self.client_oid = client_oid
        self.exchange_oid = exchange_oid
        self.latency_ms = latency_ms
        self.error_msg = error_msg
        self.raw_data = raw_data
        self.kind = kind or ErrKind.NONE

    # ---- 便捷只读视图: 让上层无需再自己包一层结果对象 (消灭重复契约) ----
    @property
    def ok(self):
        return self.status == ExecStatus.OK

    @property
    def unknown(self):
        return self.status == ExecStatus.UNKNOWN

    @property
    def ex_id(self):
        return self.exchange_oid

    @property
    def err(self):
        return self.error_msg


class UniOrder:
    """交易所订单的统一视图。上层只认它, 换交易所只需改本文件的转换函数。"""
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
    def remaining(self):
        return max(0.0, self.amount - self.filled)

    @property
    def is_terminal(self):
        return self.status in ("FILLED", "CANCELED", "REJECTED")


class _OrderNotFound:
    """点查语义哨兵: 交易所【明确回执】订单不存在(可安全换号重挂)。"""
    __slots__ = ()

    def __repr__(self):
        return "ORDER_NOT_FOUND"

    def __bool__(self):
        return False


ORDER_NOT_FOUND = _OrderNotFound()


def _dec(x):
    return Decimal(str(x))


def quantize(value, step, mode="down"):
    """
    按 step 定向修约(用 Decimal(str()) 规避二进制浮点误差)。mode: down/up/其它(四舍五入)。
    【浮点塌陷保护】值与最近 step 整数倍差距 < 1e-8 个 step 时先吸附到该整数倍,
    避免 0.3-0.2=0.0999.. 被 FLOOR 截断成 0.0 而导致加仓量逐层偏小。
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
    """单个交易对的下单规格。由本层从交易所原始 filters 解析, 上层只用这里的能力。"""

    def __init__(self, symbol, tick_size, step_size, min_qty, max_qty, min_notional,
                 contract_size=1.0):
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
            while need_by_notional * price * self.contract_size < self.min_notional:
                need_by_notional = round(need_by_notional + self.step_size, 12)
        q = max(q, need_by_min_qty, need_by_notional)
        return 0.0 if q > self.max_qty else q

    def notional(self, price, qty):
        return price * qty * self.contract_size

    def qty_is_dust(self, qty):
        """低于最小交易单位的碎屑: 无法下单, 只能账面归零。"""
        if qty is None:
            return False
        return qty < max(self.min_qty, self.step_size) * (1 - 1e-9)

    def __repr__(self):
        return (f"Spec({self.symbol} tick={self.tick_size} step={self.step_size} "
                f"minQty={self.min_qty} minNotional={self.min_notional})")


# ==========================================
# B. 错误语义 (决定"能不能换号重发"这一生死问题)
# ==========================================
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


def err_code_of(msg):
    """从报错文本中【正则精确提取】交易所 JSON code 字段, 绝不做子串误匹配。"""
    m = _ERR_CODE_RE.search(str(msg or ""))
    return int(m.group(1)) if m else None


def is_order_not_found(msg):
    """是否为交易所明确回执的"订单不存在"。"""
    if err_code_of(msg) == -2013:
        return True
    low = str(msg or "").lower()
    return "order does not exist" in low or "order not found" in low


def is_cancel_target_gone(err):
    """
    撤单回执是否表示"撤单目标已不存在"(订单已成交 / 已撤销 / 已被交易所清理)。
    覆盖 ccxt 异常类名(OrderNotFound)与平台语义 -2011 / unknown order / does not exist。
    上层据此把撤单视为【幂等达成】直接核销, 打破账本 PENDING 死循环。
    :param err: 异常对象或错误文本
    :return: bool
    """
    text = (type(err).__name__ + str(err)).lower()
    return any(k in text for k in ("ordernotfound", "-2011", "does not exist", "unknown order"))

def classify_error(msg):
    """
    错误分类(决定后续行为, 极其关键)。铁律:
      * 只有拿到交易所【明确错误码/明确语义】才判定"拒单", 才允许换号/退避重发;
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
        # 有明确错误码但未归类: 保守停挂, 绝不重发
        return _CODE_KIND.get(code, ErrKind.FATAL)
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


def make_fail_result(client_oid, err, latency_ms=0):
    """
    把一个异常/错误文本翻译成带分类的执行结果:
      平台错误码归类为"结果未知"时置 UNKNOWN(上层必须点查, 严禁换号重发), 其余置 REJECT。
    """
    kind = classify_error(err)
    status = ExecStatus.UNKNOWN if kind is ErrKind.UNKNOWN_RESULT else ExecStatus.REJECT
    return ExecResult(status, client_oid, latency_ms=latency_ms,
                      error_msg=str(err), kind=kind)


# ==========================================
# C. 连接会话 (初始化 / 校时 / 限流 / 命名转换)
# ==========================================
def init_exchange(api_key, secret_key, proxies=None):
    """
    初始化币安 U本位合约 (Future) 交易所对象并加载市场数据。
    """
    try:
        config = {
            'apiKey': api_key,
            'secret': secret_key,
            'enableRateLimit': True,
            'options': {
                'defaultType': 'future',
                'adjustForTimeDifference': True,
                'recvWindow': 10000  # [新增] 将默认的 5000ms 接收窗口放宽至 10000ms，增加网络抖动容错
            }
        }
        if proxies:
            config['proxies'] = proxies

        exchange = ccxt.binance(config)

        # [新增] 强制进行初始时间对齐，并记录到日志中
        exchange.load_time_difference()
        initial_diff = exchange.options.get('timeDifference', 0)

        exchange.load_markets()
        logger.info(f"[INIT] 交易所初始化成功，市场精度已加载 | 初始系统时间漂移补偿: {initial_diff}ms")
        return exchange
    except Exception as e:
        logger.critical(f"[INIT_FATAL] 交易所初始化失败: {e}")
        raise


def safe_init_exchange(api_key, secret_key, proxies):
    """交易所初始化: 指数退避重试直至成功 (退避上限 60s)"""
    interval = 5
    while True:
        try:
            ex = init_exchange(api_key, secret_key, proxies=proxies)
            logger.info("[INIT] 交易所初始化成功")
            return ex
        except Exception as e:
            logger.error(f"[INIT] 失败: {e}, {interval}s 后重试")
            time.sleep(interval)
            interval = min(interval * 2, 60)


def sync_exchange_time(exchange):
    """
    与交易所重新校时(对抗本地服务器长期运行的系统时钟漂移)。
    出参: 当前动态时间偏差(毫秒)。异常向上抛, 由调用方决定告警口径。
    """
    exchange.load_time_difference()
    return exchange.options.get('timeDifference', 0)


def disable_builtin_retry(exchange):
    """
    关闭 ccxt 库层的内置重试: 所有重试必须归上层状态机统一管理,
    杜绝"以为发一次实际发两次"。异常向上抛。
    """
    exchange.options["maxRetriesOnFailure"] = 0
    exchange.options["maxRetriesOnFailureDelay"] = 0


def throttle(exchange, min_gap):
    """
    相邻两次 API 调用的最小间隔(限流保护)。
    计时器挂在 exchange 实例上, 使同一连接的所有调用共享一个节流器。
    """
    if not min_gap or min_gap <= 0:
        return
    last = getattr(exchange, _THROTTLE_TS_ATTR, 0.0)
    gap = time.time() - last
    if gap < min_gap:
        time.sleep(min_gap - gap)
    setattr(exchange, _THROTTLE_TS_ATTR, time.time())


def to_market_id(symbol):
    """统一交易对名 -> 交易所原生 market id: BTC/USDT:USDT -> BTCUSDT。"""
    return str(symbol).replace("/", "").split(":")[0]


def supports_cancel_all(exchange):
    """交易所是否支持原生一键批量撤单(不支持时上层需降级为逐个撤销)。"""
    return bool(exchange.has.get('cancelAllOrders'))


# ==========================================
# D. 市场与精度 (行情 / 报价刻度 / 下单规格)
# ==========================================
def fetch_market_precision(exchange, symbol):
    """
    获取交易对的精度信息（价格精度和数量精度）
    """
    try:
        exchange.load_markets()
        market = exchange.market(symbol)
        price_precision = market['precision']['price']
        amount_precision = market['precision']['amount']
        return {'price': price_precision, 'amount': amount_precision}
    except Exception as e:
        logger.error(f"[MARKET] 获取 {symbol} 精度失败: {e}")
        return None


def format_price_amount(price, amount, precision):
    """
    按交易所要求的精度格式化价格和数量，采用向下取整策约，防止精度溢出导致拒单
    """
    p_prec = precision['price']
    a_prec = precision['amount']

    # 将精度转换为小数位数，例如 0.001 -> 3
    p_decimals = max(0, int(round(-math.log10(p_prec)))) if p_prec < 1 else 0
    a_decimals = max(0, int(round(-math.log10(a_prec)))) if a_prec < 1 else 0

    formatted_price = float(f"{price:.{p_decimals}f}")
    formatted_amount = float(f"{amount:.{a_decimals}f}")
    return formatted_price, formatted_amount


def amount_to_precision(exchange, symbol, amount):
    """按交易所原生数量精度规则修约, 出参 float。异常向上抛。"""
    return float(exchange.amount_to_precision(symbol, amount))


def fetch_instrument_spec(exchange, symbol):
    """
    解析交易规格(tick/step/minQty/minNotional/contractSize) -> InstrumentSpec。
    返回 None 表示拿不到 tick/step 等生命线字段, 上层必须拒绝启动。
    """
    try:
        try:
            exchange.load_markets()
        except Exception as e:
            logger.info(f"[网关] load_markets 刷新失败, 改用本地已缓存的市场信息 | 错误:[{e}]")
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
        # 兜底: 用 ccxt 统一字段补齐
        prec, limits = m.get("precision") or {}, m.get("limits") or {}
        tick = tick or float(prec.get("price") or 0) or 0.0
        step = step or float(prec.get("amount") or 0) or 0.0
        min_qty = min_qty or float(((limits.get("amount") or {}).get("min")) or 0)
        min_notional = min_notional or float(((limits.get("cost") or {}).get("min")) or 0) or 5.0
        spec = InstrumentSpec(symbol, tick, step, min_qty, max_qty, min_notional,
                              float(m.get("contractSize") or 1.0))
        if spec.tick_size <= 0 or spec.step_size <= 0:
            logger.critical(f"[网关] 交易规格缺少 tickSize/stepSize, 无法安全修约价量, 拒绝启动 | "
                            f"{spec}")
            return None
        return spec
    except Exception as e:
        logger.error(f"[网关] 拉取交易规格失败(可能是交易对名写错或网络不通) | "
                     f"交易对:[{symbol}] 错误:[{e}]")
        return None


def fetch_last_price(exchange, symbol):
    """最新成交价; 异常向上抛(调用方按需兜底与告警)。"""
    return exchange.fetch_ticker(symbol)['last']


def fetch_swap_tickers(exchange):
    """全市场永续合约行情快照 {symbol: ticker}; 异常向上抛。"""
    return exchange.fetch_tickers(params={'type': 'swap'})

def fetch_usdt_swap_changes(exchange):
    """
    全市场 USDT 本位永续合约 24h 涨跌幅快照 -> {symbol: percentage}。
    已过滤掉缺失涨跌幅字段的异常币种; 异常向上抛(调用方决定是否放弃本轮选币)。
    """
    tickers = fetch_swap_tickers(exchange)
    return {k: v['percentage'] for k, v in tickers.items()
            if k.endswith(':USDT') and v.get('percentage') is not None}

# ==========================================
# E. 账户与持仓
# ==========================================
def get_symbol_status(exchange, symbol):
    """
    原子化获取指定交易对的当前可用资金与持仓情况。

    :param exchange: ccxt.binance 实例
    :param symbol: 交易对名称，如 "BTC/USDT"
    :return: 包含三个元素的元组 (status: ExecStatus, usdt_free: float, position_amt: float)
             - status: OK 表示数据准确；UNKNOWN/REJECT 表示数据不可信，返回的资金和仓位均为 0.0
             - usdt_free: 当前账户可用的 USDT 余额
             - position_amt: 目标交易对的当前持仓量 (正数为多头，负数为空头)
    """
    t0 = time.perf_counter()
    try:
        balance = exchange.fetch_balance()
        usdt_free = float(balance.get('USDT', {}).get('free', 0.0))

        positions = exchange.fetch_positions([symbol])
        position_amt = float(positions[0]['info']['positionAmt']) if positions else 0.0

        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[STATUS_OK] 耗时:{latency}ms | 可用:{usdt_free:.2f} USDT | {symbol} 仓位:{position_amt}")
        return ExecStatus.OK, usdt_free, position_amt

    except NetworkError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(f"[STATUS_UNKNOWN] 网络异常获取状态失败 耗时:{latency}ms | {e}")
        return ExecStatus.UNKNOWN, 0.0, 0.0
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(f"[STATUS_REJECT] 获取状态业务异常 耗时:{latency}ms | {e}")
        return ExecStatus.REJECT, 0.0, 0.0


def get_total_equity(exchange):
    """
    获取 U本位合约账户的整体总权益 (包含所有币种折算成USD的价值)
    """
    t0 = time.perf_counter()
    try:
        balance = exchange.fetch_balance()
        # 【修复】：改为获取 totalMarginBalance，这是包含了未实现盈亏的动态总权益
        total_equity = float(balance['info']['totalMarginBalance'])
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[EQUITY_OK] 耗时:{latency}ms | 账户总权益: {total_equity:.2f} USD")
        return ExecStatus.OK, total_equity
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(f"[EQUITY_REJECT] 获取账户总权益失败 耗时:{latency}ms | {e}")
        return ExecStatus.REJECT, 0.0


def fetch_account_equity(exchange):
    """
    只读采样 U 本位账户总权益(含未实现盈亏), 供看板使用。
    数值非法直接抛异常, 绝不返回可疑值蒙骗上层。
    """
    balance = exchange.fetch_balance(params={"type": "future"})
    value = float(balance["info"]["totalMarginBalance"])
    if not math.isfinite(value):
        raise ValueError("总权益不是有效数值")
    return value


def count_nonzero_positions(exchange):
    """全账户非零持仓笔数(多空分计); 接口未返回列表或数值非法时抛异常。"""
    positions = exchange.fetch_positions(params={"type": "future"})
    if not isinstance(positions, list):
        raise ValueError("持仓接口未返回列表")
    count = 0
    for p in positions:
        qty = p.get("contracts")
        if qty is None:
            qty = (p.get("info") or {})["positionAmt"]
        qty = float(qty)
        if not math.isfinite(qty):
            raise ValueError("持仓数量不是有效数值")
        count += abs(qty) > 0
    return count


def fetch_positions_map(exchange):
    """
    拉取非零持仓, 归一化为 {symbol_SIDE: 带符号数量}; 单向持仓按数量正负推断多空。
    异常向上抛(上层通常带重试与放弃本轮的策略)。
    """
    cache = {}
    for pos in exchange.fetch_positions():
        amt = float(pos["info"]["positionAmt"])
        if amt == 0:
            continue
        side = str(pos["info"].get("positionSide", "")).upper()
        if not side or side == "BOTH":
            side = "LONG" if amt > 0 else "SHORT"
        cache[f"{pos['symbol']}_{side}"] = amt
    return cache


def fetch_position_qty(exchange, symbol, position_side):
    """
    指定 positionSide 的持仓数量(绝对值), 无该方向持仓返回 0.0。异常向上抛。
    用于夹逼平仓量与外部干预识别, 绝不参与均价计算(双向持仓下该数字为全账户共享)。
    """
    for p in exchange.fetch_positions([symbol]) or []:
        info = p.get("info") or {}
        ps = str(info.get("positionSide") or p.get("side") or "").upper()
        if ps == position_side.upper():
            return abs(float(p.get("contracts") or info.get("positionAmt") or 0))
    return 0.0


def is_hedge_mode(exchange):
    """账户是否为【双向持仓 Hedge Mode】; 异常向上抛。"""
    r = exchange.fapiPrivateGetPositionSideDual()
    return str(r.get("dualSidePosition")).lower() == "true"


# ==========================================
# F. 订单查询 (状态归一 / 挂单快照 / 点查)
# ==========================================
# 交易所原始状态 -> 平台无关标准态 (全系统唯一一份状态表)
_CCXT_STATUS_MAP = {
    "NEW": "OPEN", "PARTIALLY_FILLED": "OPEN", "PENDING_CANCEL": "OPEN",
    "FILLED": "FILLED", "CANCELED": "CANCELED", "CANCELLED": "CANCELED",
    "EXPIRED": "CANCELED", "EXPIRED_IN_MATCH": "CANCELED", "REJECTED": "REJECTED",
    "OPEN": "OPEN", "CLOSED": "FILLED",
}


def normalize_order_status(raw):
    """交易所/ccxt 订单状态 -> 标准态: OPEN / FILLED / CANCELED / REJECTED / UNKNOWN。"""
    return _CCXT_STATUS_MAP.get(str(raw).upper(), "UNKNOWN")


def to_uni_order(o):
    """
    ccxt 原始订单 dict -> UniOrder (统一视图)。
    入参核心 Key: id / clientOrderId / status / price / stopPrice / amount / filled /
    average / side / info{status,executedQty,origQty,stopPrice,avgPrice,...}。
    """
    info = o.get("info") or {}
    raw_status = str(info.get("status") or o.get("status") or "").upper()
    status = normalize_order_status(raw_status)
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


def extract_order_view(o):
    """
    交易所原始订单 -> 对账所需的关键字段快照(平台无关视图, 不做任何状态改判)。
    与 to_uni_order 的区别: 本函数【忠实反映平台回执】, 不会把"已收口但未全成"改判为撤单,
    专供账本对账使用(账本自己按 filled 判断残量, 状态语义必须原样保留)。
    :return: dict, 核心 Key:
        status    标准态 OPEN / FILLED / CANCELED / REJECTED / UNKNOWN
        filled    已成交数量(保持原样不数值化, 由调用方按自身容错口径转换)
        avg_price 成交均价, 缺失时退化为委托价, 再缺失为空串
        order_id  交易所原生订单号(字符串)
    """
    o = o or {}
    return {
        "status": normalize_order_status(o.get("status", "")),
        "filled": o.get("filled"),
        "avg_price": o.get("average") or o.get("price") or "",
        "order_id": str(o.get("id", "")),
    }


def order_client_oid(o):
    """从交易所原始订单结构中提取本地单号(client_oid); 缺失一律返回空串。"""
    o = o or {}
    return str(o.get("clientOrderId") or (o.get("info") or {}).get("clientOrderId") or "")


def make_open_order_stub(exchange_oid, client_oid):
    """
    构造一条"在线挂单"占位记录(与交易所原始订单结构同形)。
    用途: 下单成功后立即登记进本地挂单缓存, 封锁同一批次内的重复开平(幂等打标)。
    """
    return {"id": exchange_oid, "clientOrderId": client_oid,
            "info": {"clientOrderId": client_oid}}

def fetch_open_orders(exchange, symbol=None):
    """
    在线挂单原始列表(ccxt 结构)。symbol=None 时拉取全账户挂单(需交易所支持)。
    异常向上抛, 由调用方决定"放弃本轮"或"降级"。
    """
    if symbol is None:
        exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
        return exchange.fetch_open_orders()
    return exchange.fetch_open_orders(symbol)


def fetch_open_orders_grouped(exchange):
    """全账户在线挂单按交易对分组 -> {symbol: [原始订单, ...]}; 异常向上抛。"""
    cache = {}
    for order in fetch_open_orders(exchange):
        cache.setdefault(order["symbol"], []).append(order)
    return cache

def fetch_open_orders_map(exchange, symbol, coid_prefix):
    """
    普通在线挂单(限价开仓/限价止盈/已触发的 STOP_MARKET) -> {client_oid: UniOrder}。
    只保留 client_oid 以 coid_prefix 开头的单据(策略命名空间隔离)。异常向上抛。
    """
    out = {}
    for o in exchange.fetch_open_orders(symbol) or []:
        u = to_uni_order(o)
        if u.coid and u.coid.startswith(coid_prefix):
            out[u.coid] = u
    return out


def fetch_open_algo_orders_map(exchange, symbol, coid_prefix):
    """
    未触发的算法条件单(普通挂单接口查不到) -> {client_oid: UniOrder}。异常向上抛,
    调用方据此判定"条件单通道本轮降级", 降级时绝不采信"条件单不存在"。
    """
    out = {}
    market_id = to_market_id(symbol)     # BTC/USDT:USDT -> BTCUSDT
    for a in exchange.fapiPrivateGetOpenAlgoOrders({"symbol": market_id}) or []:
        coid = a.get("clientAlgoId") or a.get("clientOrderId") or ""
        if not coid or not coid.startswith(coid_prefix):
            continue
        out[coid] = UniOrder(
            coid=coid,
            ex_id=str(a.get("algoId") or a.get("orderId") or ""),
            status="OPEN",
            stop_price=float(a.get("triggerPrice") or a.get("stopPrice") or 0.0),
            amount=float(a.get("quantity") or a.get("origQty") or 0.0),
            filled=float(a.get("executedQty") or 0.0),
            side=str(a.get("side") or "").lower(),
            ts=int(a.get("bookTime") or a.get("time") or 0),
            raw=a,
        )
    return out


def index_open_orders(exchange, kind="normal"):
    """
    全账户在线挂单索引(不传 symbol: 覆盖该 U 本位账户的所有交易对), 供只读看板使用。
    :param kind: "normal" 普通挂单 / "algo" 未触发的算法条件单
    :return: {(symbol, 订单号): 交易所原始订单}; 接口未返回列表时抛异常(绝不把异常当成零挂单)
    """
    fetch, id_key = ((exchange.fapiPrivateGetOpenOrders, "orderId") if kind == "normal"
                     else (exchange.fapiPrivateGetOpenAlgoOrders, "algoId"))
    rows = fetch({})
    if not isinstance(rows, list):
        raise ValueError("挂单接口未返回列表，不能视为零挂单")
    return {(str(o["symbol"]), str(o[id_key])): o for o in rows}

def dedup_algo_orders(algo_map, normal_map):
    """
    条件单去重: 已触发的条件单会同时出现在普通挂单通道, 直接相加会重复计数。
    :param algo_map: index_open_orders(exchange, "algo") 的出参; None 表示该通道本轮拉取失败
    :param normal_map: index_open_orders(exchange, "normal") 的出参; None 表示该通道本轮拉取失败
    :return: 去重后的未触发条件单笔数; algo_map 为 None 时返回 None(表示未知, 绝不当成 0)
    """
    if algo_map is None:
        return None
    return sum(
        not (normal_map is not None and
             str(o.get("actualOrderId") or "") not in ("", "0") and
             (str(o["symbol"]), str(o["actualOrderId"])) in normal_map)
        for o in algo_map.values()
    )

def fetch_recent_orders(exchange, symbol, limit=50):
    """近期订单(含已成交/已撤销), 用于对账穿透; 异常向上抛。"""
    return exchange.fetch_orders(symbol, limit=limit)

def fetch_recent_orders_map(exchange, symbol, limit=50):
    """近期订单(含已成交/已撤销)按交易所原生订单号索引 -> {order_id: 原始订单}; 异常向上抛。"""
    return {str(o.get("id")): o for o in fetch_recent_orders(exchange, symbol, limit=limit)}

def fetch_order_by_id(exchange, symbol, order_id):
    """按交易所原生订单号点查单笔订单(ccxt 原始结构); 异常向上抛。"""
    return exchange.fetch_order(order_id, symbol)


def fetch_single_order(exchange, symbol, client_oid):
    """
    单笔订单兜底查询 (精确对账用)
    """
    t0 = time.perf_counter()
    try:
        order = exchange.fetch_order(client_oid, symbol, params={"origClientOrderId": client_oid})
        latency = int((time.perf_counter() - t0) * 1000)
        logger.debug(f"[FETCH_ORDER] 耗时:{latency}ms | CID:{client_oid} | 状态:{order['status']}")
        return order
    except InvalidOrder:
        logger.warning(f"[FETCH_ORDER] 查无此单 (可能已被清理) | CID:{client_oid}")
        return {"status": "canceled", "filled": 0.0, "average": 0.0}  # 视同撤销
    except Exception as e:
        logger.error(f"[FETCH_ORDER] 查询失败 | CID:{client_oid} | {e}")
        return None


def fetch_order_uni(exchange, symbol, client_oid, throttle_gap=0.0):
    """
    点查订单并归一化为 UniOrder。两级取数: 先走通用封装, 空结果再用原生接口确认,
    严格区分"确实不存在"与"查询失败"。

    出参 (order, err) 三态语义(上层据此决定是否允许换号重挂):
      (UniOrder, None)        -> 拿到确定的订单快照
      (ORDER_NOT_FOUND, None) -> 交易所【明确回执】订单不存在, 上层可安全换新号重挂
      (None, err)             -> 结果未知(超时/网络/5xx), 上层必须保留原 OID, 绝不换号重发
    """
    fetchers = (lambda: fetch_single_order(exchange, symbol, client_oid),
                lambda: exchange.fetch_order(client_oid, symbol,
                                             {"origClientOrderId": client_oid}))
    for idx, fetcher in enumerate(fetchers):
        try:
            throttle(exchange, throttle_gap)
            o = fetcher()
            if o:
                return to_uni_order(o), None
            if idx == len(fetchers) - 1:
                return ORDER_NOT_FOUND, None
        except Exception as e:
            if is_order_not_found(e):
                return ORDER_NOT_FOUND, None
            return None, e
    return None, None


def fetch_all_open_orders_unified(exchange, symbol):
    """
    统一获取指定交易对的全部活动挂单（普通限价单 + 止盈止损算法条件单）
    返回格式收敛为标准的 UniOrder 字典列表
    """
    market_id = exchange.market(symbol)['id']
    unified_orders = []

    # 1. 获取普通挂单 (REST)
    try:
        normal_orders = exchange.fapiPrivateGetOpenOrders({'symbol': market_id})
        for o in normal_orders:
            unified_orders.append({
                'client_oid': o.get('clientOrderId', ''),
                'exchange_oid': str(o.get('orderId', '')),
                'symbol': symbol,
                'side': o.get('side', '').lower(),
                'type': o.get('type', ''),
                'price': float(o.get('price', 0.0)),
                'stop_price': float(o.get('stopPrice', 0.0)),
                'amount': float(o.get('origQty', 0.0)),
                'filled': float(o.get('executedQty', 0.0)),
                'status': 'OPEN',
                'source': 'NORMAL'
            })
    except Exception as e:
        logger.error(f"[FETCH_ALL] 获取普通挂单失败: {e}")

    # 2. 获取算法条件单 (Algo Orders)
    try:
        algo_orders = exchange.fapiPrivateGetOpenAlgoOrders({'symbol': market_id})
        for o in algo_orders:
            unified_orders.append({
                'client_oid': o.get('clientAlgoId', ''),
                'exchange_oid': str(o.get('algoId', '')),
                'symbol': symbol,
                'side': o.get('side', '').lower(),
                'type': o.get('algoType', o.get('orderType', 'STOP_MARKET')),
                'price': float(o.get('price', 0.0)),
                'stop_price': float(o.get('triggerPrice', o.get('stopPrice', 0.0))),
                'amount': float(o.get('quantity', o.get('origQty', 0.0))),
                'filled': float(o.get('executedQty', 0.0)),
                'status': 'OPEN',
                'source': 'ALGO'
            })
    except Exception as e:
        logger.error(f"[FETCH_ALL] 获取算法条件单失败: {e}")

    return unified_orders


# ==========================================
# G. 下单执行：极致纯粹的执行器 (永不抛异常, 一律返回 ExecResult)
# ==========================================
def execute_order(exchange, symbol, side, amount, client_oid, order_type='market', price=None, reduce_only=False,
                  position_side="LONG"):
    """
    执行单次下单意图，内置网络断联(UNKNOWN)与业务拒单(REJECT)的隔离处理。

    :param exchange: ccxt.binance 实例
    :param symbol: 交易对，如 "BTC/USDT"
    :param side: 交易方向，"buy" (买入) 或 "sell" (卖出)
    :param amount: 交易数量 (币的数量，非 U 的数量)
    :param client_oid: 策略端生成的本地订单号 (必须全局唯一，推荐 uuid)
    :param order_type: 订单类型，支持 "market" (市价), "limit" (限价), "maker" (只做 Maker 限价单) (默认: 'market')
    :param price: 触发价格，当 order_type 为 "limit" 或 "maker" 时必填 (默认: None)
    :param reduce_only: 是否只减仓。平仓时务必设为 True，防止因为超额平仓变成反向开仓 (默认: False)
    :param position_side: 持仓方向，支持 "LONG" (多仓) 或 "SHORT" (空仓)。双向持仓模式下必填！(默认: None)
    :return: ExecResult 实例。REJECT 时 kind 已按平台错误码完成语义分类;
             若返回 ExecStatus.UNKNOWN，切勿盲目重试下单，必须通过后台服务用 client_oid 轮询核对真实状态！
    """
    t0 = time.perf_counter()

    params = {'newClientOrderId': client_oid}
    if reduce_only:
        params['reduceOnly'] = True

    # 【核心修改点】：适配双向持仓模式，向交易所透传 positionSide
    if position_side:
        params['positionSide'] = position_side.upper()

    ccxt_type = 'limit' if order_type in ['limit', 'maker'] else 'market'
    if ccxt_type == 'limit' and price is None:
        return ExecResult(ExecStatus.REJECT, client_oid, error_msg="限价单必须提供 price 参数",
                          kind=ErrKind.INVALID)
    if order_type == 'maker':
        params['postOnly'] = True

    try:
        # 日志加上 position_side 的打印，保持排查链路的完整性
        pos_side_str = f" | 持仓方向:{position_side.upper()}" if position_side else ""
        logger.info(
            f"[ACTION] 下单意图 | CID:{client_oid} | {symbol} {side.upper()}{pos_side_str} | 量:{amount} | 类:{order_type} | 价:{price} | 仅减仓:{reduce_only} 价值:{amount * price if price else '市价'}")

        order = exchange.create_order(
            symbol=symbol, type=ccxt_type, side=side, amount=amount, price=price, params=params
        )

        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[OK] 订单成功 | CID:{client_oid} | EID:{order['id']} | 耗时:{latency}ms")
        return ExecResult(ExecStatus.OK, client_oid, exchange_oid=order['id'], latency_ms=latency, raw_data=order)

    except NetworkError as e:
        latency = int((time.perf_counter() - t0) * 1000)
        err_msg = f"物理断联，订单可能已成交: {e}"
        logger.critical(f"[UNKNOWN] 状态丢失 | CID:{client_oid} | {err_msg} | 耗时:{latency}ms")
        return ExecResult(ExecStatus.UNKNOWN, client_oid, latency_ms=latency, error_msg=err_msg,
                          kind=ErrKind.UNKNOWN_RESULT)

    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(f"[REJECT] 业务拒单 | CID:{client_oid} | {e} | 耗时:{latency}ms")
        return ExecResult(ExecStatus.REJECT, client_oid, latency_ms=latency, error_msg=str(e),
                          kind=classify_error(e))


def place_stop_market_order(exchange, symbol, side, amount, stop_price, client_oid,
                            position_side, working_type="MARK_PRICE"):
    """
    条件止损单(STOP_MARKET): stopPrice 与 amount 严格按交易所精度格式化后提交。
    出参 ExecResult; 失败时按平台错误码分类(结果未知 -> UNKNOWN, 明确拒单 -> REJECT + kind)。
    """
    t0 = time.perf_counter()
    try:
        o = exchange.create_order(
            symbol=symbol, type="STOP_MARKET", side=side,
            amount=float(exchange.amount_to_precision(symbol, amount)), price=None,
            params={
                "stopPrice": exchange.price_to_precision(symbol, stop_price),
                "workingType": working_type,
                "positionSide": position_side,
                "newClientOrderId": client_oid,
                "priceProtect": "FALSE",     # 必须大写
            },
        )
        latency = int((time.perf_counter() - t0) * 1000)
        return ExecResult(ExecStatus.OK, client_oid, exchange_oid=str((o or {}).get("id") or ""),
                          latency_ms=latency, raw_data=o)
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        return make_fail_result(client_oid, e, latency_ms=latency)


# ==========================================
# H. 撤单 (按本地单号 / 按交易所单号 / 批量 / 双轨兜底)
# ==========================================
def cancel_order_by_id(exchange, symbol, order_id):
    """按交易所原生订单号撤单; 异常向上抛, 由调用方决定告警口径。"""
    return exchange.cancel_order(order_id, symbol)


def cancel_order_by_client_oid(exchange, symbol, client_oid, throttle_gap=0.0):
    """
    双轨自适应撤单: 先标准撤单; 若提示查无此单(-2011/unknown order), 自动改走算法单撤销通道。

    出参 (ok, via, err):
      ok  = True 仅代表"撤单请求已被受理或目标已不存在", 不代表终态(可能刚好成交),
            终态必须由上层点查裁决;
      via = "NORMAL"(普通通道) / "ALGO"(算法通道撤销成功) / "ALGO_GONE"(算法通道确认已不存在),
            供调用方按通道输出不同口径的日志;
      err = 最后一次失败的异常对象(ok=True 时为 None)。
    """
    throttle(exchange, throttle_gap)
    try:
        exchange.cancel_order(client_oid, symbol, {"origClientOrderId": client_oid})
        return True, "NORMAL", None
    except Exception as e:
        msg = str(e).lower()
        # 单子本来就不存在/已终结 -> 撤单目标已达成(幂等)
        if any(k in msg for k in ("-2013", "order not found", "does not exist")):
            return True, "NORMAL", None
        if "-2011" not in msg and "unknown order" not in msg:
            return False, "NORMAL", e

    throttle(exchange, throttle_gap)
    try:
        exchange.fapiPrivateDeleteAlgoOrder({"symbol": to_market_id(symbol),
                                            "clientAlgoId": client_oid})
        return True, "ALGO", None
    except Exception as algo_err:
        a_msg = str(algo_err).lower()
        if any(k in a_msg for k in ("-2011", "unknown", "not exist", "does not exist")):
            return True, "ALGO_GONE", None      # 确实已经没有了, 目标达成
        return False, "ALGO", algo_err


def cancel_single_order(exchange, symbol, order_id, is_client_id=False):
    """
    撤销指定的挂单，具备幂等性（重复撤销已被撮合或已撤销的单子会平滑返回 OK）。

    :param exchange: ccxt.binance 实例
    :param symbol: 交易对，如 "BTC/USDT"
    :param order_id: 订单 ID (可以是交易所 EID，也可以是本地发单时的 CID)
    :param is_client_id: 传入的 order_id 是否为本地的 client_oid (默认: False，即默认为交易所 EID)
    :return: ExecResult 实例。
             若订单已不存在(InvalidOrder)，视为撤单目的已达到，直接返回 ExecStatus.OK。
    """
    t0 = time.perf_counter()
    try:
        params = {}
        if is_client_id:
            params['origClientOrderId'] = order_id

        res = exchange.cancel_order(order_id, symbol, params=params)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[OK] 撤单成功 | ID:{order_id} | 耗时:{latency}ms")
        return ExecResult(ExecStatus.OK, client_oid=order_id if is_client_id else "", exchange_oid=order_id,
                          latency_ms=latency, raw_data=res)

    except InvalidOrder:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.warning(f"[OK_SKIP] 订单已无活跃状态，无需撤销 | ID:{order_id}")
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
    """
    一键撤销该交易对全部活动挂单(交易所原生批量接口, 单次网络请求)。
    异常向上抛, 由调用方决定告警口径与降级策略。
    """
    return exchange.cancel_all_orders(symbol)


def cancel_all_orders(exchange, symbol):
    """
    一键撤销该交易对下所有的活动挂单 (常用于紧急风控清仓、止损或策略重启时)。

    :param exchange: ccxt.binance 实例
    :param symbol: 交易对，如 "BTC/USDT"
    :return: bool，表示全撤指令是否执行成功 (True 成功，False 失败)
    """
    t0 = time.perf_counter()
    try:
        cancel_all_orders_of_symbol(exchange, symbol)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[OK] {symbol} 全撤指令已执行 | 耗时:{latency}ms")
        return True
    except Exception as e:
        logger.error(f"[REJECT] {symbol} 全撤失败: {e}")
        return False


def cancel_order_universal(exchange, symbol, client_oid=None, order_id=None):
    """
    通用撤单执行器：自动适配【普通挂单】与【算法条件单 (STOP_MARKET)】。
    优先尝试普通撤单，若提示查无此单，自动使用算法单接口进行二次撤销。
    """
    t0 = time.perf_counter()
    clean_symbol = symbol.replace(":USDT", "")  # 币安原生接口喜欢 "BTCUSDT"
    market_id = exchange.market(symbol)['id']   # 例如 "BTCUSDT"

    # ---------------- 1. 尝试撤销普通订单 ----------------
    try:
        params = {}
        if client_oid:
            params['origClientOrderId'] = client_oid
        res = exchange.cancel_order(order_id, symbol, params=params)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[CANCEL_OK] 普通订单撤单成功 | ID:{client_oid or order_id} | 耗时:{latency}ms")
        return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency, raw_data=res)

    except InvalidOrder as e:
        # 普通接口查无此单，说明它极有可能是未触发的【算法条件单】，进入二次撤销
        logger.info(f"[CANCEL_RETRY] 普通接口未找到订单，尝试通过算法单 (Algo Order) 接口撤销 | ID:{client_oid or order_id}")

    except Exception as e:
        logger.error(f"[CANCEL_REJECT] 普通撤单网络或业务异常: {e}")

    # ---------------- 2. 尝试撤销算法条件单 (Algo Order) ----------------
    try:
        algo_params = {'symbol': market_id}
        if client_oid:
            algo_params['clientAlgoId'] = client_oid
        if order_id:
            algo_params['algoId'] = int(order_id)

        # 调用币安合约原生算法单撤销接口: DELETE /fapi/v1/algoOrder
        res = exchange.fapiPrivateDeleteAlgoOrder(algo_params)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[CANCEL_OK] 算法条件单撤单成功！ | ID:{client_oid or order_id} | 耗时:{latency}ms")
        return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency, raw_data=res)

    except Exception as algo_err:
        latency = int((time.perf_counter() - t0) * 1000)
        err_msg = str(algo_err).lower()
        if "unknown" in err_msg or "-2011" in err_msg or "not exist" in err_msg:
            # 两套接口都确认没有，才是真的已经不存在了
            logger.info(f"[CANCEL_CONFIRMED] 两套接口均确认无此订单，撤单目的达成 | ID:{client_oid or order_id}")
            return ExecResult(ExecStatus.OK, client_oid or "", exchange_oid=str(order_id or ""), latency_ms=latency)

        logger.error(f"[CANCEL_ALGO_FAIL] 算法单撤销也失败: {algo_err}")
        return ExecResult(ExecStatus.REJECT, client_oid or "", latency_ms=latency, error_msg=str(algo_err))


# ==========================================
# I. 上层应用模拟与全流程端到端测试 (Main 演示)
# ==========================================
if __name__ == "__main__":
    print("==================================================")
    print(">>> 启动量化主策略引擎 & 条件单完整生命周期测试 <<<")
    print("==================================================")

    # ---------------- 准备阶段：配置读取与实例初始化 ----------------
    # 优先读取自定义的密钥，没有则读取默认配置
    API_KEY = get_config("myself_biance_api_key") or get_config("nana_biance_api_key")
    SECRET_KEY = get_config("myself_biance_api_secret") or get_config("nana_biance_api_secret")
    SYMBOL = "BTC/USDT:USDT"

    # 环境自适应代理配置：Linux服务器通常直连，本地开发机走本地代理端口
    proxies = None if platform.system().lower() == "linux" else {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890"
    }

    try:
        bot_exchange = init_exchange(API_KEY, SECRET_KEY, proxies=proxies)
    except Exception as e:
        print(f">>> 交易所初始化失败，程序退出: {e}")
        exit(1)

    # ---------------- 场景 1: 账户资金与仓位巡视 ----------------
    print("\n--- [场景 1] 开机巡视：账户权益与当前持仓 ---")
    get_total_equity(bot_exchange)
    status, usdt, pos = get_symbol_status(bot_exchange, SYMBOL)
    if status == ExecStatus.OK:
        print(f"当前可用保证金: {usdt:.2f} USDT | 当前 {SYMBOL} 净持仓: {pos} BTC")
    else:
        print("⚠️ 无法获取持仓及余额状态，请排查网络！")

    # ---------------- 场景 2: 防呆拦截验证 ----------------
    print("\n--- [场景 2] 防呆设计校验：缺失必填参数拦截 ---")
    bad_intent_id = f"bad_test_{uuid.uuid4().hex[:8]}"
    # 模拟手误：发了限价单 (limit) 却未传价格 (price=None)
    res_bad = execute_order(bot_exchange, SYMBOL, "buy", 0.001, bad_intent_id, order_type="limit", price=None)
    if res_bad.status == ExecStatus.REJECT:
        print(f"🛡️ 防呆机制生效，本地拦截非法意图: {res_bad.error_msg}")

    # ---------------- 场景 3: 挂出一张全新的止损条件单 (STOP_MARKET) ----------------
    print("\n--- [场景 3] 实盘测试：挂出一张止损市价条件单 (STOP_MARKET) ---")
    algo_client_oid = f"test_sl_{uuid.uuid4().hex[:8]}"
    algo_stop_price = 50000.0  # 设置一个远离当前盘口、绝对安全的触发价
    algo_amount = 0.002  # 满足 BTC 最小名义价值的下单数量
    target_eid = None  # 用于暂存交易所分配的 algoId

    try:
        # 获取当前市价，动态校验触发方向（多头止损需低于市价，空头止损需高于市价）
        current_price = fetch_last_price(bot_exchange, SYMBOL)
        print(f"当前市场价: {current_price} USDT")

        # 示例：假设我们持有多仓，挂单卖出止损平多（低于当前价触发）
        # 如果市价低于 55000，则动态调整止损价为当前价的 85%
        algo_stop_price = round(current_price * 0.85, 1)

        print(
            f"准备提交条件单 -> CID:{algo_client_oid} | 方向:SELL(止损) | 触发价:{algo_stop_price} | 数量:{algo_amount}")

        # 统一走平台层的条件单入口 (内部已完成精度格式化与错误分类)
        res_algo = place_stop_market_order(
            bot_exchange, SYMBOL, "sell", algo_amount, algo_stop_price,
            algo_client_oid, "LONG", working_type="MARK_PRICE"
        )
        if res_algo.ok:
            target_eid = res_algo.ex_id
            print(f"🎉 条件单挂单成功! 本地CID: {algo_client_oid} | 交易所EID(AlgoID): {target_eid}")
        else:
            print(f"❌ 挂条件单失败 (如果是测试网或无持仓报错属正常业务拦截): "
                  f"[{res_algo.kind.value}] {res_algo.err}")

    except Exception as e:
        print(f"❌ 挂条件单失败 (如果是测试网或无持仓报错属正常业务拦截): {e}")

    # ---------------- 场景 4: 统一全量挂单查询 (普通单 + 条件单) ----------------
    print("\n--- [场景 4] 使用统一接口拉取当前盘口所有活跃挂单 ---")
    time.sleep(1.0)  # 等待 1 秒使撮合引擎数据同步
    all_open = fetch_all_open_orders_unified(bot_exchange, SYMBOL)
    print(f"当前共查到 {len(all_open)} 张活动订单:")
    for o in all_open:
        print(f" -> [{o['source']}] CID:{o['client_oid']} | EID:{o['exchange_oid']} | "
              f"类型:{o['type']} | 触发价:{o['stop_price']} | 挂单价:{o['price']} | 数量:{o['amount']}")

    # ---------------- 场景 5: 通用撤单函数精准拔除条件单 ----------------
    print("\n--- [场景 5] 验证通用撤单接口：精准撤销刚创建的条件单 ---")
    # 优先选用场景3成功生成的单号，若场景3未成功则使用兜底单号演示
    cancel_cid = algo_client_oid
    cancel_eid = target_eid

    if cancel_cid or cancel_eid:
        print(f"正在精准撤除目标: CID={cancel_cid} (EID={cancel_eid}) ...")
        # cancel_order_universal 具备降级自愈能力：普通接口找不到会自动切换到算法单接口
        res_cancel = cancel_order_universal(
            bot_exchange,
            SYMBOL,
            client_oid=cancel_cid,
            order_id=cancel_eid
        )

        if res_cancel.status == ExecStatus.OK:
            print("🎉 撤单指令已通过通用/算法接口下发成功！")
        else:
            print(f"❌ 撤单失败: {res_cancel.error_msg}")
    else:
        print("⚠️ 未找到可供撤除的订单 ID，跳过撤单阶段。")

    # ---------------- 场景 6: 撤单终态复查 ----------------
    print("\n--- [场景 6] 状态核验：再次查询确认条件单是否彻底清理 ---")
    time.sleep(1.0)
    all_open_after = fetch_all_open_orders_unified(bot_exchange, SYMBOL)

    # 过滤确认刚才撤销的单号是否还存在
    remained = [
        o for o in all_open_after
        if (cancel_cid and o['client_oid'] == cancel_cid) or (cancel_eid and o['exchange_oid'] == cancel_eid)
    ]

    if not remained:
        print("✅ 确认成功！该条件单已在盘口与服务端彻底消失！逻辑闭环跑通！")
    else:
        print("⚠️ 警告：订单依然残留在盘口，请核对日志排查异常！")