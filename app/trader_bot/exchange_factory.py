# -*- coding: utf-8 -*-
"""
================================================================================
[功能摘要]: 交易所网关工厂 —— 根据平台名称返回对应的网关模块。
[使用方法]:
    在交易系统文件顶部:
        EXCHANGE_PLATFORM = "binance"  # 可选: "binance" | "okx" | "bybit"
        from exchange_factory import get_gateway
        ex_api = get_gateway(EXCHANGE_PLATFORM)
    之后使用 ex_api.safe_init_exchange / ex_api.execute_order 等即可。
[设计原则]:
    * 返回的模块拥有与 binance_u_gateway 完全相同的公开 API 表面
    * 选择 "binance" 时直接返回 binance_u_gateway 原模块, 零开销、零风险
    * 此文件不含任何业务逻辑, 只做路由分发
================================================================================
"""

_SUPPORTED_PLATFORMS = ("binance", "okx", "bybit")


def get_gateway(platform="binance"):
    """
    根据平台名称返回对应的交易所网关模块。

    :param platform: 平台标识, 大小写不敏感。支持: binance / okx / bybit
    :return: 网关模块(具有与 binance_u_gateway 相同的公开 API)
    :raises ValueError: 不支持的平台名称
    """
    name = str(platform).lower().strip()

    if name == "binance":
        import binance_u_gateway as gw
        return gw

    if name == "okx":
        import okx_gateway as gw
        return gw

    if name == "bybit":
        import bybit_gateway as gw
        return gw

    raise ValueError(
        f"不支持的交易所平台: '{platform}'。"
        f"当前支持的平台: {', '.join(_SUPPORTED_PLATFORMS)}"
    )
