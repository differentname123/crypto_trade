"""
币安合约网格机器人 — CCXT 版
要求: pip install ccxt>=4.0
"""
import ccxt
import json

# ═══════════════════════════════════════════
# 1. 初始化交易所
# ═══════════════════════════════════════════
exchange = ccxt.binance({
    'apiKey': 'IcuZe47uAwZIuqeZG0si6vQkRicVGvhdya6a1FttKY1N2yGJKl3b1aU4dwGctG8Z',
    'secret': 'VQwd7LXTHn72vC4RdGz2aF8zsp3mG0vAPQASHYvkfy1hUIRXGdsWWHx8QHIVhyTr',
    'options': {
        'defaultType': 'future',       # 使用 USDT-M 合约
        'adjustForTimeDifference': True, # 自动校正时间偏差
    },
        'proxies': {'http': 'http://127.0.0.1:7890', 'https': 'http://127.0.0.1:7890'}, # 如果需要翻墙请解除注释

})

exchange.load_markets()


# ═══════════════════════════════════════════
# 2. 探测 CCXT 中的隐式方法名
#    （首次使用时运行一次即可确认）
# ═══════════════════════════════════════════
def discover_grid_methods():
    """打印所有包含 'grid' 或 'algo' 的隐式方法名"""
    methods = [m for m in dir(exchange) if 'grid' in m.lower() or ('algo' in m.lower() and 'post' in m.lower())]
    print("可用的网格/Algo 相关方法:")
    for m in sorted(methods):
        print(f"  {m}")
    return methods

# 取消注释以探测：
# discover_grid_methods()


# ═══════════════════════════════════════════
# 3. 创建合约网格策略
# ═══════════════════════════════════════════
def create_futures_grid(
    symbol:      str   = 'BTCUSDT',
    side:        str   = 'NEUTRAL',      # NEUTRAL | LONG | SHORT
    quantity:    float = 1000,            # 总投入保证金 (USDT)
    grid_type:   str   = 'ARITHMETIC',   # ARITHMETIC(等差) | GEOMETRIC(等比)
    lower_limit: float = 60000,
    upper_limit: float = 70000,
    grid_count:  int   = 10,             # 网格数量 (通常 2~170)
    leverage:    int   = 5,
):
    params = {
        'symbol':         symbol,
        'side':           side,
        'quantity':        quantity,
        'gridType':       grid_type,
        'gridLowerLimit':  lower_limit,
        'gridUpperLimit':  upper_limit,
        'gridCount':      grid_count,
        'leverage':       leverage,
    }

    # ── 尝试调用隐式方法（方法名可能因 CCXT 版本略有不同）──
    # 常见候选名称:
    #   sapiPostV1AlgoFuturesNewGridOrder
    #   sapiPostAlgoFuturesNewGridOrder
    #   sapi_post_v1_algo_futures_new_grid_order

    method_candidates = [
        'sapiPostV1AlgoFuturesNewGridOrder',
        'sapiPostAlgoFuturesNewGridOrder',
    ]

    for method_name in method_candidates:
        fn = getattr(exchange, method_name, None)
        if callable(fn):
            print(f"[INFO] 使用 CCXT 隐式方法: {method_name}")
            response = fn(params)
            return response

    # ── 如果以上候选都不存在，使用更底层的方式 ──
    print("[WARN] 隐式方法未找到，尝试底层 request()...")
    response = exchange.request(
        path='algo/futures/newGridOrder',
        api='sapi',
        method='POST',
        params=params,
    )
    return response


# ═══════════════════════════════════════════
# 4. 查询正在运行的网格策略
# ═══════════════════════════════════════════
def get_open_grids():
    method_candidates = [
        'sapiGetV1AlgoFuturesOpenOrders',
        'sapiGetAlgoFuturesOpenOrders',
    ]
    for method_name in method_candidates:
        fn = getattr(exchange, method_name, None)
        if callable(fn):
            return fn({})

    return exchange.request(
        path='algo/futures/openOrders',
        api='sapi',
        method='GET',
        params={},
    )


# ═══════════════════════════════════════════
# 5. 查询网格策略的子订单
# ═══════════════════════════════════════════
def get_grid_sub_orders(algo_id: int, page: int = 1, page_size: int = 100):
    params = {
        'algoId':   algo_id,
        'page':     page,
        'pageSize': page_size,
    }
    method_candidates = [
        'sapiGetV1AlgoFuturesSubOrders',
        'sapiGetAlgoFuturesSubOrders',
    ]
    for method_name in method_candidates:
        fn = getattr(exchange, method_name, None)
        if callable(fn):
            return fn(params)

    return exchange.request(
        path='algo/futures/subOrders',
        api='sapi',
        method='GET',
        params=params,
    )


# ═══════════════════════════════════════════
# 6. 取消（停止）网格策略
# ═══════════════════════════════════════════
def cancel_grid(algo_id: int):
    params = {'algoId': algo_id}
    method_candidates = [
        'sapiDeleteV1AlgoFuturesOrder',
        'sapiDeleteAlgoFuturesOrder',
    ]
    for method_name in method_candidates:
        fn = getattr(exchange, method_name, None)
        if callable(fn):
            return fn(params)

    return exchange.request(
        path='algo/futures/order',
        api='sapi',
        method='DELETE',
        params=params,
    )


# ═══════════════════════════════════════════
# 7. 主流程
# ═══════════════════════════════════════════
if __name__ == '__main__':
    # --- 创建网格 ---
    result = create_futures_grid(
        symbol='BTCUSDT',
        side='NEUTRAL',
        quantity=500,            # 投入 500 USDT 保证金
        grid_type='ARITHMETIC',
        lower_limit=58000,
        upper_limit=72000,
        grid_count=20,
        leverage=3,
    )
    print("[创建结果]", json.dumps(result, indent=2, ensure_ascii=False))

    # 返回中会包含 algoId，后续用它查询/取消
    algo_id = result.get('algoId') or result.get('data', {}).get('algoId')
    print(f"[策略ID] {algo_id}")

    # --- 查询运行中的网格 ---
    open_grids = get_open_grids()
    print("[运行中网格]", json.dumps(open_grids, indent=2, ensure_ascii=False))

    # --- 查询子订单（取消注释使用）---
    # sub_orders = get_grid_sub_orders(algo_id=algo_id)
    # print("[子订单]", json.dumps(sub_orders, indent=2))

    # --- 取消网格（取消注释使用）---
    # cancel_result = cancel_grid(algo_id=algo_id)
    # print("[取消结果]", json.dumps(cancel_result, indent=2))