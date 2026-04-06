import time

def calculate_multi_group_margin(
        leverage: float,
        target_loss_percent: float,  # 全局最大忍受的下跌/上涨比例，例如 5.0 表示 5%
        max_grids_per_group: int,  # 每个大组允许的最大网格数量
        fixed_qty: float = 1.0,  # 每次固定开仓的数量
        add_step_percent: float = 0.01,  # 每次价格波动加仓的百分比
        initial_price: float = 1.0,  # 初始价格
        direction: str = 'long'  # 新增：'long' 表示做多，'short' 表示做空
) -> dict:
    """
    功能：在每次固定开仓数量的多组网格策略下，计算要扛住指定的跌幅/涨幅，
    各组网格的价格区间、单组所需保证金，以及全局总计需要的初始保证金。
    不考虑手续费、MMR 和 滑点。
    """
    if leverage <= 0 or target_loss_percent <= 0 or add_step_percent <= 0 or fixed_qty <= 0 or max_grids_per_group <= 0:
        raise ValueError("所有数值参数必须 > 0")
    if direction not in ['long', 'short']:
        raise ValueError("direction 参数必须是 'long' 或 'short'")

    r = add_step_percent / 100.0
    sign = 1 if direction == 'long' else -1  # 盈亏计算方向乘数

    # 计算全局目标止损价/爆仓价 (底线)
    if direction == 'long':
        target_price = initial_price * (1.0 - target_loss_percent / 100.0)
    else:
        target_price = initial_price * (1.0 + target_loss_percent / 100.0)

    current_price = initial_price

    total_margin_all_groups = 0.0
    groups_info = []
    group_id = 1

    # 定义价格是否尚未触及目标的检查函数
    def is_active(cp, tp):
        return cp >= tp if direction == 'long' else cp <= tp

    # 只要当前价格还没跌穿/涨穿全局目标价，就继续开启新的网格组
    while is_active(current_price, target_price):
        group_start_price = current_price
        group_qty = 0.0
        group_cost = 0.0
        group_max_margin = 0.0
        grids_in_group = 0
        last_executed_price = current_price

        # 模拟单个大组内部顺着网格一路下跌/上涨加仓的过程
        while grids_in_group < max_grids_per_group and is_active(current_price, target_price):
            # 1. 触发加仓：更新该组的持仓和成本
            group_qty += fixed_qty
            group_cost += fixed_qty * current_price
            grids_in_group += 1
            last_executed_price = current_price

            # 2. 检查加仓瞬间的“开仓保证金需求”
            # 利用 sign 自适应做多/做空的浮亏计算
            upnl_at_open = sign * (group_qty * current_price - group_cost)
            required_for_margin = (group_cost / leverage) - upnl_at_open
            if required_for_margin > group_max_margin:
                group_max_margin = required_for_margin

            # 计算下一个网格准备加仓的价格
            if direction == 'long':
                next_price = current_price * (1 - r)
                check_price = max(next_price, target_price)
            else:
                next_price = current_price * (1 + r)
                check_price = min(next_price, target_price)

            # 3. 检查在这个区间内波动时的生存底线需求
            upnl_at_bottom = sign * (group_qty * check_price - group_cost)
            required_for_survival = -upnl_at_bottom
            if required_for_survival > group_max_margin:
                group_max_margin = required_for_survival

            # 跌到/涨到下一个网格价，进入该组的下一次循环
            current_price = next_price

        # 4. 关键点：该组网格建仓完毕（或触及全局目标价）后，它需要一直扛单到“全局 target_price”
        # 该组在全局目标价时的极限浮亏
        upnl_at_global_target = sign * (group_qty * target_price - group_cost)

        # 维持该组仓位到全局目标价所需的极限保证金：(仓位价值 / 杠杆) - 极限浮亏
        required_at_global_target = (group_cost / leverage) - upnl_at_global_target
        if required_at_global_target > group_max_margin:
            group_max_margin = required_at_global_target

        # 记录该大组的信息
        groups_info.append({
            "group_id": group_id,
            "start_price": round(group_start_price, 6),
            "end_price": round(last_executed_price, 6),  # 该组最后一单的成交价
            "grid_count": grids_in_group,
            "group_qty": round(group_qty, 6),
            "required_margin": round(group_max_margin, 6)
        })

        # 累加到总保证金中
        total_margin_all_groups += group_max_margin
        group_id += 1

    return {
        "total_margin": round(total_margin_all_groups, 6),
        "groups_info": groups_info
    }


def find_min_add_step_percent(
        available_margin: float,
        leverage: float,
        target_loss_percent: float,
        max_grids_per_group: int,
        fixed_qty: float = 1.0,
        initial_price: float = 1.0,
        direction: str = 'long',
        tolerance: float = 0.001  # 精度：0.001%
) -> dict:
    """
    功能：已知可用总保证金，利用二分查找法，反推当前资金能支撑的最小加仓百分比 (add_step_percent)。
    """
    if available_margin <= 0:
        raise ValueError("可用保证金必须 > 0")

    # 定义二分查找的边界
    # 下界：0.01% (非常密集的网格，需要海量保证金)
    low_r = 0.01
    # 上界：等于 target_loss_percent (极端情况，只在起点开一单，中途不再加仓)
    high_r = target_loss_percent

    best_r = None
    best_result = None

    # 第一步：先检查上界 (最宽的网格)。如果连一单都扛不住，直接报错
    baseline_result = calculate_multi_group_margin(
        leverage, target_loss_percent, max_grids_per_group,
        fixed_qty, high_r, initial_price, direction
    )
    if baseline_result['total_margin'] > available_margin:
        return {
            "status": "failed",
            "message": f"资金不足！哪怕只开首仓不加仓，也需要 {baseline_result['total_margin']} 保证金，您的资金为 {available_margin}。"
        }

    # 第二步：二分查找核心循环
    while (high_r - low_r) > tolerance:
        mid_r = (low_r + high_r) / 2.0

        # 使用当前的 mid_r 尝试计算所需保证金
        current_result = calculate_multi_group_margin(
            leverage, target_loss_percent, max_grids_per_group,
            fixed_qty, mid_r, initial_price, direction
        )
        current_margin = current_result['total_margin']

        if current_margin > available_margin:
            # 保证金不够，说明网格太密了，需要增大步长
            low_r = mid_r
        else:
            # 保证金够用！记录当前结果，并尝试继续缩小步长（看能不能把网格打得更密）
            best_r = mid_r
            best_result = current_result
            high_r = mid_r

    # 如果循环结束没找到（理论上不会，因为前面已经排除了资金绝对不足的情况）
    if best_r is None:
        best_r = high_r
        best_result = baseline_result

    return {
        "status": "success",
        "suggested_min_add_step_percent": round(best_r, 4),
        "actual_margin_used": best_result['total_margin'],
        "margin_utilization_rate": f"{round((best_result['total_margin'] / available_margin) * 100, 2)}%",
        "strategy_details": best_result
    }



# ==========================================
# 测试与使用示例
# ==========================================
if __name__ == "__main__":

    # # 策略1 网格
    # result = calculate_multi_group_margin(
    #     leverage=125.0,
    #     target_loss_percent=20,  # 扛住 20% 的下跌/上涨
    #     max_grids_per_group=10000,  # 单个大组最多网格数
    #     fixed_qty=0.014,  # 每次买 0.014 个
    #     add_step_percent=0.4,  # 每跌 0.4% 买一次
    #     initial_price=2100,  # 假设初始价格 2100
    #     # direction='short',  # 默认做多，解除注释以做空
    # )
    #
    # print(f"【全局总需准备的保证金】: {result['total_margin']}\n")
    # print("【各网格大组详细信息】:")
    # for g in result['groups_info']:
    #     print(f"组别 {g['group_id']}:")
    #     print(f"  - 价格区间: {g['start_price']} -> {g['end_price']}")
    #     print(f"  - 网格数量: {g['grid_count']} 单")
    #     print(f"  - 该组累计币量: {g['group_qty']}")
    #     print(f"  - 该组需分配保证金: {g['required_margin']}")
    #     print("-" * 30)

    MY_WALLET_MARGIN = 150.0

    print(f"=== 开始反推测算 ===")
    print(f"目标：手里有 {MY_WALLET_MARGIN} U，想扛住 20% 跌幅，求最小加仓间距\n")

    inverse_result = find_min_add_step_percent(
        available_margin=MY_WALLET_MARGIN,
        leverage=125.0,
        target_loss_percent=20.0,
        max_grids_per_group=10000,
        fixed_qty=0.014,
        initial_price=2200.0,
        # direction='short', # 做空
        tolerance=0.001  # 精度控制到万分之一
    )

    if inverse_result['status'] == 'success':
        r = inverse_result['suggested_min_add_step_percent']
        used = inverse_result['actual_margin_used']
        rate = inverse_result['margin_utilization_rate']

        print(f"✅ 计算成功！")
        print(f"👉 建议的最小加仓百分比 (add_step_percent): {r} %")
        print(f"👉 此时会占用保证金: {used} U (资金利用率: {rate})")

        # 打印生成的网格简报
        details = inverse_result['strategy_details']['groups_info'][0]
        print(f"👉 网格运行情况: 在该参数下，总共会加仓 {details['grid_count']} 次，累计买入 {details['group_qty']} 个币。")
    else:
        print(f"❌ 计算失败: {inverse_result['message']}")