import math

import ccxt
import time
import uuid

from enum import Enum
from ccxt.base.errors import NetworkError, InvalidOrder

from common_utils_lite import setup_logger, get_config


# ==========================================
# 0. 核心数据契约 (剥离所有 typing)
# ==========================================
class ExecStatus(Enum):
    OK = "OK"  # 明确成功
    REJECT = "REJECT"  # 明确拒绝
    UNKNOWN = "UNKNOWN"  # 状态未知


class ExecResult:
    """
    执行结果的统一包装类，屏蔽底层交易所 API 的异构性。
    调用者只需判断 status 即可决定下一步业务逻辑。
    """

    def __init__(self, status, client_oid, exchange_oid="", latency_ms=0, error_msg="", raw_data=None):
        """
        :param status: ExecStatus 枚举，(OK: 成功, REJECT: 明确失败/拒单, UNKNOWN: 物理断联导致状态未知)
        :param client_oid: 策略端生成的本地唯一订单号 (必传，用于对账)
        :param exchange_oid: 交易所返回的真实订单号 (下单成功时存在)
        :param latency_ms: 本次请求发生的网络+业务总耗时 (毫秒)
        :param error_msg: 具体的错误原因说明 (用于 REJECT 或 UNKNOWN 时排查)
        :param raw_data: ccxt 返回的原始 payload (仅在需要深度解析时使用)
        """
        self.status = status
        self.client_oid = client_oid
        self.exchange_oid = exchange_oid
        self.latency_ms = latency_ms
        self.error_msg = error_msg
        self.raw_data = raw_data



logger = setup_logger()


# ==========================================
# 2. 纯函数库：账户与状态查询模块
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


# ==========================================
# 3. 核心交易模块：极致纯粹的执行器
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
    :return: ExecResult 实例。
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
        return ExecResult(ExecStatus.REJECT, client_oid, error_msg="限价单必须提供 price 参数")
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
        return ExecResult(ExecStatus.UNKNOWN, client_oid, latency_ms=latency, error_msg=err_msg)

    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        logger.error(f"[REJECT] 业务拒单 | CID:{client_oid} | {e} | 耗时:{latency}ms")
        return ExecResult(ExecStatus.REJECT, client_oid, latency_ms=latency, error_msg=str(e))

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
        return ExecResult(ExecStatus.UNKNOWN, client_oid="", latency_ms=latency, error_msg=str(e))
    except Exception as e:
        latency = int((time.perf_counter() - t0) * 1000)
        return ExecResult(ExecStatus.REJECT, client_oid="", latency_ms=latency, error_msg=str(e))


def cancel_all_orders(exchange, symbol):
    """
    一键撤销该交易对下所有的活动挂单 (常用于紧急风控清仓、止损或策略重启时)。

    :param exchange: ccxt.binance 实例
    :param symbol: 交易对，如 "BTC/USDT"
    :return: bool，表示全撤指令是否执行成功 (True 成功，False 失败)
    """
    t0 = time.perf_counter()
    try:
        exchange.cancel_all_orders(symbol)
        latency = int((time.perf_counter() - t0) * 1000)
        logger.info(f"[OK] {symbol} 全撤指令已执行 | 耗时:{latency}ms")
        return True
    except Exception as e:
        logger.error(f"[REJECT] {symbol} 全撤失败: {e}")
        return False

# 将此函数添加到你的 base_trader.py 中
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

# ==========================================
# 5. 上层应用模拟 (Main 演示)
# ==========================================
if __name__ == "__main__":

    # 【假装这里是你的主循环引擎或策略中心】
    print(">>> 启动量化主策略引擎...")

    # 1. 填入你的测试 API 密钥（建议使用币安测试网）
    API_KEY = get_config('nana_biance_api_key')
    SECRET_KEY = get_config('nana_biance_api_secret')
    SYMBOL = "BTC/USDT:USDT"

    try:
        # 注意：如果在境内测试，可能需要传 proxies={'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'}
        bot_exchange = init_exchange(API_KEY, SECRET_KEY,
                                     proxies={'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'})
    except Exception:
        print(">>> 交易所初始化失败，程序退出。")
        exit(1)
    get_total_equity(bot_exchange)
    print("\n--- 场景 1: 开机状态检查 ---")
    status, usdt, pos = get_symbol_status(bot_exchange, SYMBOL)
    if status == ExecStatus.OK:
        print(f"当前可用子弹: {usdt} U, 当前持仓: {pos} 个 BTC")
    else:
        print("无法获取状态，检查网络或代理！")

    print("\n--- 场景 2: 上层策略发出防呆拦截指令 ---")
    # 模拟上层传了错误参数（限价单忘记传价格）
    bad_intent_id = f"open_bad_{uuid.uuid4().hex[:8]}"
    res_bad = execute_order(bot_exchange, SYMBOL, "buy", 0.001, bad_intent_id, order_type="limit",price=60000)

    if res_bad.status == ExecStatus.REJECT:
        print(f"被基座直接挡回: {res_bad.error_msg}")
        print("策略引擎: 幸好没发出去，调整参数重新计算。")
    #
    # print("\n--- 场景 3: 正常的平仓意图 (使用 reduceOnly 防翻转) ---")
    # # 主策略决定平仓，主动生成绝对唯一的意图 ID
    # close_intent_id = f"close_pos_{uuid.uuid4().hex[:8]}"
    #
    # res_close = execute_order(
    #     exchange=bot_exchange,
    #     symbol=SYMBOL,
    #     side="sell",  # 平多仓
    #     amount=100.0,  # 直接给个极大值
    #     client_oid=close_intent_id,
    #     reduce_only=True  # 核心保护
    # )
    #
    # # ！！！上层主策略的终极处理范式 ！！！
    # if res_close.status == ExecStatus.OK:
    #     print(f"策略引擎: 平仓成功！交易所单号是 {res_close.exchange_oid}，耗时 {res_close.latency_ms} ms。")
    #     # 此时可以去更新本地的记账数据库或持仓状态
    #
    # elif res_close.status == ExecStatus.REJECT:
    #     print(f"策略引擎: 平仓被拒 ({res_close.error_msg})。")
    #     # 往往是因为刚才根本没仓位，或者余额不足，这种明确被拒的单子，主策略直接忽略即可，不要重试。
    #
    # elif res_close.status == ExecStatus.UNKNOWN:
    #     print(f"策略引擎: 🚨 警报！发生薛定谔状态！单号 {close_intent_id} 失联！")
    #     # 将该单号推入后台的 Redis 队列或死信队列。
    #     # 后台会有一个独立的 Reconciler（对账协程），每隔 5 秒去调用 get_single_order_status 查这个 CID，直到确认它是成交还是被废弃。