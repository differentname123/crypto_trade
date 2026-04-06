import time


# ==========================================
# 1. 核心业务逻辑：计算保证金 (带安全拦截机制)
# ==========================================
def calculate_multi_group_margin(
        leverage: float,
        target_loss_percent: float,
        max_grids_per_group: int,
        fixed_qty: float = 1.0,
        add_step_percent: float = 0.01,
        initial_price: float = 1.0,
        direction: str = 'long'
) -> dict:
    if leverage <= 0 or target_loss_percent <= 0 or add_step_percent <= 0 or fixed_qty <= 0 or max_grids_per_group <= 0:
        raise ValueError("所有数值参数必须 > 0")
    if direction not in ['long', 'short']:
        raise ValueError("direction 参数必须是 'long' 或 'short'")

    # 【防死循环保护】现货/普通合约价格不能跌穿归零
    if direction == 'long' and target_loss_percent >= 100.0:
        raise ValueError("做多时，价格最大跌幅不能 >= 100% (价格不能跌破 0)。")

    r = add_step_percent / 100.0
    sign = 1 if direction == 'long' else -1

    if direction == 'long':
        target_price = initial_price * (1.0 - target_loss_percent / 100.0)
    else:
        target_price = initial_price * (1.0 + target_loss_percent / 100.0)

    current_price = initial_price
    total_margin_all_groups = 0.0
    groups_info = []
    group_id = 1

    def is_active(cp, tp):
        return cp >= tp if direction == 'long' else cp <= tp

    while is_active(current_price, target_price):
        group_start_price = current_price
        group_qty = 0.0
        group_cost = 0.0
        group_max_margin = 0.0
        grids_in_group = 0
        last_executed_price = current_price

        while grids_in_group < max_grids_per_group and is_active(current_price, target_price):
            group_qty += fixed_qty
            group_cost += fixed_qty * current_price
            grids_in_group += 1
            last_executed_price = current_price

            upnl_at_open = sign * (group_qty * current_price - group_cost)
            required_for_margin = (group_cost / leverage) - upnl_at_open
            if required_for_margin > group_max_margin:
                group_max_margin = required_for_margin

            if direction == 'long':
                next_price = current_price * (1 - r)
                check_price = max(next_price, target_price)
            else:
                next_price = current_price * (1 + r)
                check_price = min(next_price, target_price)

            upnl_at_bottom = sign * (group_qty * check_price - group_cost)
            required_for_survival = -upnl_at_bottom
            if required_for_survival > group_max_margin:
                group_max_margin = required_for_survival

            current_price = next_price

        upnl_at_global_target = sign * (group_qty * target_price - group_cost)
        required_at_global_target = (group_cost / leverage) - upnl_at_global_target
        if required_at_global_target > group_max_margin:
            group_max_margin = required_at_global_target

        groups_info.append({
            "group_id": group_id,
            "start_price": round(group_start_price, 6),
            "end_price": round(last_executed_price, 6),
            "grid_count": grids_in_group,
            "group_qty": round(group_qty, 6),
            "required_margin": round(group_max_margin, 6)
        })

        total_margin_all_groups += group_max_margin
        group_id += 1

    return {
        "total_margin": round(total_margin_all_groups, 6),
        "groups_info": groups_info
    }


# ==========================================
# 2. 核心数学引擎：通用二分查找求解器 (DRY)
# ==========================================
def _universal_margin_solver(
        target_param_name: str,
        available_margin: float,
        base_params: dict,
        low_bound: float,
        high_bound: float,
        positive_correlation: bool,  # True: 参数越大保证金越多; False: 参数越大保证金越少
        tolerance: float = 0.0001
) -> dict:
    """底层的通用求解引擎，被具体业务包装函数调用"""
    if available_margin <= 0:
        return {"status": "failed", "message": "可用保证金必须 > 0"}

    best_val = None
    best_result = None

    low, high = low_bound, high_bound

    while (high - low) > tolerance:
        mid = (low + high) / 2.0

        # 将测试参数注入字典
        test_params = base_params.copy()
        test_params[target_param_name] = mid

        try:
            current_result = calculate_multi_group_margin(**test_params)
            current_margin = current_result['total_margin']

            if current_margin > available_margin:
                # 保证金爆了，需要向“降低保证金”的方向调整参数
                if positive_correlation:
                    high = mid  # 正相关，减小参数
                else:
                    low = mid  # 负相关，增大参数
            else:
                # 保证金够用，记录结果，并尝试向“挑战极限”的方向压榨
                best_val = mid
                best_result = current_result
                if positive_correlation:
                    low = mid  # 尝试加大参数
                else:
                    high = mid  # 尝试减小参数
        except ValueError as e:
            # 捕获类似 target_loss_percent >= 100 的非法边界异常
            # 如果触发异常，说明当前 mid 值非法，当做“超出了极限”处理
            if positive_correlation:
                high = mid
            else:
                low = mid

    if best_val is None:
        return {
            "status": "failed",
            "message": f"在范围 [{low_bound}, {high_bound}] 内无法找到满足 {available_margin} 保证金的解。可能是资金严重不足。"
        }

    return {
        "status": "success",
        f"optimal_{target_param_name}": round(best_val, 5),
        "actual_margin_used": best_result['total_margin'],
        "margin_utilization_rate": f"{round((best_result['total_margin'] / available_margin) * 100, 2)}%",
        "strategy_details": best_result
    }


# ==========================================
# 3. 对外提供的友好 API (业务层 Wrapper)
# ==========================================

def solve_for_add_step_percent(available_margin: float, params: dict, tolerance=0.001) -> dict:
    """求解最优(最小)加仓间距"""
    # 范围：0.01% (极密) 到 100% (极宽)
    # 负相关：步长越大，加仓越少，需要的保证金越低
    return _universal_margin_solver(
        "add_step_percent", available_margin, params,
        low_bound=0.01, high_bound=100.0, positive_correlation=False, tolerance=tolerance
    )


def solve_for_target_loss_percent(available_margin: float, params: dict, tolerance=0.001) -> dict:
    """求解最优(最大)抗跌/抗涨幅度"""
    # 做多时最高允许 99.9% 跌幅，做空时允许 500% 的涨幅
    max_loss_bound = 99.9 if params.get('direction', 'long') == 'long' else 500.0
    # 正相关：扛的幅度越深，需要的保证金越多
    return _universal_margin_solver(
        "target_loss_percent", available_margin, params,
        low_bound=0.1, high_bound=max_loss_bound, positive_correlation=True, tolerance=tolerance
    )


def solve_for_fixed_qty(available_margin: float, params: dict, tolerance=0.0001) -> dict:
    """求解最优(最大)单次开仓数量"""
    # 范围：0.0001 个币 到 100万个币 (取决于标的物)
    # 正相关：买的越多，需要的保证金越多
    return _universal_margin_solver(
        "fixed_qty", available_margin, params,
        low_bound=0.0001, high_bound=1000000.0, positive_correlation=True, tolerance=tolerance
    )


# ==========================================
# 4. 测试与使用示例
# ==========================================
if __name__ == "__main__":
    # ---------------------------------------------
    # 统一的基础参数配置中心
    # ---------------------------------------------
    TARGET_LOSS = 200.0  # 基准抗涨幅 (做空时扛住价格上涨200%)
    ADD_STEP = 3.83  # 基准加仓步长 (%)
    FIXED_QTY = 849.0  # 基准单次下单量

    # 抽取所有场景都共享的公共参数，确保底层状态完全一致
    common_params = {
        "leverage": 10.0,
        "max_grids_per_group": 10000,
        "initial_price": 0.007,
        "direction": 'short'  # 做空
    }

    print("==================================================")
    print("                策略参数交叉验证测试                ")
    print("==================================================")

    # ---------------------------------------------
    # [功能0] 正向计算: 看看这套参数在数学上究竟需要多少保证金
    # ---------------------------------------------
    base_calc_params = common_params.copy()
    base_calc_params.update({
        "target_loss_percent": TARGET_LOSS,
        "fixed_qty": FIXED_QTY,
        "add_step_percent": ADD_STEP
    })

    # 执行正向计算
    baseline_result = calculate_multi_group_margin(**base_calc_params)
    ACTUAL_MARGIN_NEEDED = baseline_result['total_margin']

    print("\n[功能0] 标准正向计算测试:")
    print(f"👉 严格按照上述参数，您的策略实际需要的极限保证金为: {ACTUAL_MARGIN_NEEDED} U")
    print("-" * 50)

    # 为了实现完美的交叉验证，我们将“钱包资金”设定为刚才算出的真实所需保证金
    VERIFY_WALLET = ACTUAL_MARGIN_NEEDED
    print(f"=== 开始使用真实所需资金 ({VERIFY_WALLET} U) 进行逆向反推验证 ===")

    # ---------------------------------------------
    # [功能1] 已知 资金、单量、步长，求能扛多深 (解 target_loss_percent)
    # 期望结果: 逼近 200.0%
    # ---------------------------------------------
    params_1 = common_params.copy()
    params_1.update({
        "fixed_qty": FIXED_QTY,
        "add_step_percent": ADD_STEP
    })
    res1 = solve_for_target_loss_percent(VERIFY_WALLET, params_1)

    if res1['status'] == 'success':
        print("\n[功能1] 求最大抗涨/跌幅:")
        print(f"✅ 反推得出最大可扛幅度: {res1['optimal_target_loss_percent']}% (原参数为 {TARGET_LOSS}%)")
        print(f"   资金利用率: {res1['margin_utilization_rate']}")
    else:
        print(f"\n[功能1] 计算失败: {res1['message']}")

    # ---------------------------------------------
    # [功能2] 已知 资金、单量、扛单深度，求最小加仓间距 (解 add_step_percent)
    # 期望结果: 逼近 3.83%
    # ---------------------------------------------
    params_2 = common_params.copy()
    params_2.update({
        "fixed_qty": FIXED_QTY,
        "target_loss_percent": TARGET_LOSS
    })
    res2 = solve_for_add_step_percent(VERIFY_WALLET, params_2)

    if res2['status'] == 'success':
        print("\n[功能2] 求最小加仓间距:")
        print(f"✅ 反推得出最小加仓步长: {res2['optimal_add_step_percent']}% (原参数为 {ADD_STEP}%)")
        print(f"   资金利用率: {res2['margin_utilization_rate']}")
    else:
        print(f"\n[功能2] 计算失败: {res2['message']}")

    # ---------------------------------------------
    # [功能3] 已知 资金、步长、扛单深度，求最大开仓数量 (解 fixed_qty)
    # 期望结果: 逼近 849.0
    # ---------------------------------------------
    params_3 = common_params.copy()
    params_3.update({
        "add_step_percent": ADD_STEP,
        "target_loss_percent": TARGET_LOSS
    })
    res3 = solve_for_fixed_qty(VERIFY_WALLET, params_3)

    if res3['status'] == 'success':
        print("\n[功能3] 求最大单次开仓量:")
        print(f"✅ 反推得出最大单次下单量: {res3['optimal_fixed_qty']} 个 (原参数为 {FIXED_QTY} 个)")
        print(f"   资金利用率: {res3['margin_utilization_rate']}")
    else:
        print(f"\n[功能3] 计算失败: {res3['message']}")

    print("\n==================================================")