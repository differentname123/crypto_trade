import asyncio
import aiohttp
import json
from datetime import datetime


async def simple_binance_ws():
    # 改为 @aggTrade：获取 BTCUSDT 合约的实时成交流（毫秒级推送）
    url = "wss://fstream.binance.com/ws/btcusdt@aggTrade"

    proxy = "http://127.0.0.1:7890"

    print(f"正在连接到币安合约: {url} ...")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(url, proxy=proxy, heartbeat=10) as ws:
            print("✅ 连接成功！正在等待实时数据...\n")

            while True:
                try:
                    # 如果代理没问题，这个流的数据非常密集，绝对不会触发 5 秒超时
                    msg = await ws.receive(timeout=5.0)

                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)

                        # 币安 @aggTrade 事件类型是 'aggTrade'
                        if data.get('e') == 'aggTrade':
                            price = data.get('p')  # 成交价格
                            quantity = data.get('q')  # 成交数量
                            is_maker = data.get('m')  # 是否是做市方（True代表主动卖出，False代表主动买入）

                            # 格式化时间
                            ts = data.get('T')
                            time_str = datetime.fromtimestamp(ts / 1000.0).strftime('%H:%M:%S.%f')[:-3]

                            direction = "🔴卖出" if is_maker else "🟢买入"
                            print(f"[{time_str}] {direction} | 最新成交价: {price} | 数量: {quantity}")

                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        print("❌ 连接已断开或发生错误")
                        break

                except asyncio.TimeoutError:
                    print("⚠️ 超过 5 秒没有收到数据！(如果是 @aggTrade 还没数据，绝对是代理被墙或断网了)")


if __name__ == "__main__":
    asyncio.run(simple_binance_ws())