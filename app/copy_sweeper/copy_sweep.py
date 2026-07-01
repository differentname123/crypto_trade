import requests
import json
import time
import random
from datetime import datetime


def time_str_to_ms(time_str):
    dt_obj = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    return int(dt_obj.timestamp() * 1000)


def fetch_history_by_time_range(portfolio_id, start_time_str, end_time_str, max_count=0, max_retries=3):
    """
    按时间区间拉取币安带单组合的历史订单
    max_count: 最大拉取条数，0 表示无上限
    """
    url = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/lead-portfolio/order-history"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
        "Content-Type": "application/json",
        "lang": "zh-CN",
        "X-UI-REQUEST-TRACE": "4b36da1d-4a7e-46a0-b54f-5640ef4e23cb",
        "X-TRACE-ID": "4b36da1d-4a7e-46a0-b54f-5640ef4e23cb",
        "BNC-UUID": "7f010d68-c61a-4dd3-b189-92a9f1b07dab",
        "device-info": "eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTE1MiIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTEwNCIsInN5c3RlbV92ZXJzaW9uIjoiV2luZG93cyAxMCIsImJyYW5kX21vZGVsIjoidW5rbm93biIsInN5c3RlbV9sYW5nIjoiemgtQ04iLCJ0aW1lem9uZSI6IkdNVCswODowMCIsInRpbWV6b25lT2Zmc2V0IjotNDgwLCJ1c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NDsgcnY6MTUyLjApIEdlY2tvLzIwMTAwMTAxIEZpcmVmb3gvMTUyLjAiLCJsaXN0X3BsdWdpbiI6IlBERiBWaWV3ZXIsQ2hyb21lIFBERiBWaWV3ZXIsQ2hyb21pdW0gUERGIFZpZXdlcixNaWNyb3NvZnQgRWRnZSBQREYgVmlld2VyLFdlYktpdCBidWlsdC1pbiBQREYiLCJjYW52YXNfY29kZSI6ImE2OGZmMmRjIiwid2ViZ2xfdmVuZG9yIjoiR29vZ2xlIEluYy4gKE5WSURJQSkiLCJ3ZWJnbF9yZW5kZXJlciI6IkFOR0xFIChOVklESUEsIE5WSURJQSBHZUZvcmNlIEdUWCA5ODAgRGlyZWN0M0QxMSB2c181XzAgcHNfNV8wKSwgb3Igc2ltaWxhciIsImF1ZGlvIjoiMzUuNzQ5OTcyMDkzODUwMzc0IiwicGxhdGZvcm0iOiJXaW4zMiIsIndlYl90aW1lem9uZSI6IkFzaWEvU2hhbmdoYWkiLCJkZXZpY2VfbmFtZSI6InVua25vd24iLCJmaW5nZXJwcmludCI6IjdhZjZlOTIzMjE4OWViYjcxOWU1NDJiOWJlZTk1YzljIiwiZGV2aWNlX2lkIjoiIiwicmVsYXRlZF9kZXZpY2VfaWRzIjoiIn0=",
        "clienttype": "web",
        "FVIDEO-ID": "33fcb102fe35aa7f807c0ebc9b09aec9c0b6c879",
        "FVIDEO-TOKEN": "2MdxKhoStvXWpL0UkmBbFpkG0KDo8iZ4RsB7VhaQmRvezJ0hieQxkpzfwwWPyfqpFGah9HSjrGOd7fDXrFDGX++idCK8MA2IVQuFKwDZizKM3Ae35JDt7x6ui4TYP7ek9MciJ/Etc1S/+RcnBlvUAJM/MYBU93akhvHznCe4acucM3I36RH9AA5sgmHS3FNus=20",
        "csrftoken": "d41d8cd98f00b204e9800998ecf8427e",
        "BNC-Time-Zone": "Asia/Shanghai",
        # 如果需要，可以将抓包中的 Cookie 补充在此处
    }

    start_time_ms = time_str_to_ms(start_time_str)
    current_end_time_ms = time_str_to_ms(end_time_str)

    page_size = 50
    all_orders = []
    page_num = 1

    session = requests.Session()
    session.headers.update(headers)

    print(f"🚀 目标 Portfolio: {portfolio_id}")
    print(f"📅 拉取区间: [{start_time_str}] 到 [{end_time_str}]")
    if max_count > 0:
        print(f"🛑 设置了最大拉取数量上限: {max_count} 条")

    while True:
        print(f"\n正在拉取第 {page_num} 页... (当前上限时间戳: {current_end_time_ms})")

        payload = {
            "portfolioId": portfolio_id,
            "startTime": start_time_ms,
            "endTime": current_end_time_ms,
            "pageSize": page_size
        }

        retries = 0
        success = False
        orders = []

        while retries < max_retries and not success:
            try:
                response = session.post(url, json=payload, timeout=10)
                response.raise_for_status()
                res_data = response.json()

                data_body = res_data.get("data", {})
                if isinstance(data_body, dict):
                    orders = data_body.get("list", [])
                else:
                    orders = data_body

                success = True

            except requests.exceptions.RequestException as e:
                retries += 1
                print(f"⚠️ 请求失败 (尝试 {retries}/{max_retries}): {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"服务器返回: {e.response.text}")

                if retries >= max_retries:
                    print("❌ 重试次数耗尽，中断拉取。")
                    return all_orders

                time.sleep(3 * retries)

        if not orders:
            print("⚠️ 未获取到新订单。可能已触及起始时间，或遭遇隐性风控。")
            break

        all_orders.extend(orders)
        current_total = len(all_orders)
        print(f"✅ 获取 {len(orders)} 条记录。累计总数: {current_total}")

        # 检查是否达到最大数量限制
        if max_count > 0 and current_total >= max_count:
            print(f"🛑 已达到或超过设定的最大数量 ({max_count})，提前结束拉取。")
            # 截断多余的数据，确保返回的长度不超过 max_count
            all_orders = all_orders[:max_count]
            break

        last_order = orders[-1]
        last_time = last_order.get("time") or last_order.get("createTime") or last_order.get("orderTime")

        if not last_time:
            print(f"❌ 数据异常：找不到时间戳字段。记录样例: {list(last_order.keys())}")
            break

        current_end_time_ms = int(last_time) - 1

        if current_end_time_ms <= start_time_ms:
            print(f"🎯 游标已抵达起始时间 [{start_time_str}]，拉取圆满完成。")
            break

        page_num += 1
        time.sleep(random.uniform(1.2, 2.8))

    print(f"\n🎉 任务结束！共返回 {len(all_orders)} 条订单。")
    return all_orders


def group_trades_by_rounds(all_data):
    """
    聚合订单数据为完整的开平仓轮次，返回一个二维列表（List of Lists）。

    返回的数据结构为：
    [
        [订单1, 订单2...], # 某一轮闭环交易 (例如 BTC做空: 开0.001 -> 开0.002 -> 平0.003)
        [订单3, 订单4...], # 另一轮闭环交易
        ...
    ]
    """
    # 1. 按照订单时间升序排序（从最旧到最新），以模拟真实的交易顺序
    sorted_data = sorted(all_data, key=lambda x: x['orderTime'])

    # 最终的聚合结果（一个二维列表）
    result = []

    # 临时追踪字典，用于隔离计算不同币种和不同持仓方向的净数量
    # 格式: {(symbol, positionSide): {'qty': 0.0, 'trades': []}}
    ongoing_positions = {}

    for trade in sorted_data:
        symbol = trade['symbol']
        pos_side = trade['positionSide']
        side = trade['side']
        qty = trade['executedQty']

        # 使用 (币种, 仓位方向) 作为唯一键隔离计算
        state_key = (symbol, pos_side)
        if state_key not in ongoing_positions:
            ongoing_positions[state_key] = {'qty': 0.0, 'trades': []}

        # 2. 根据做多/做空逻辑，累加或累减当前仓位净数量
        if pos_side == 'LONG':
            if side == 'BUY':
                ongoing_positions[state_key]['qty'] += qty  # 做多开仓/加仓
            elif side == 'SELL':
                ongoing_positions[state_key]['qty'] -= qty  # 做多平仓/减仓
        elif pos_side == 'SHORT':
            if side == 'SELL':
                ongoing_positions[state_key]['qty'] += qty  # 做空开仓/加仓
            elif side == 'BUY':
                ongoing_positions[state_key]['qty'] -= qty  # 做空平仓/减仓

        # 将当前订单加入正在进行中的轮次
        ongoing_positions[state_key]['trades'].append(trade)

        # 3. 判断是否完全平仓 (通过判断净数量是否归零，1e-8处理浮点误差)
        if abs(ongoing_positions[state_key]['qty']) < 1e-8:
            # 完整的一轮闭环结束，存入结果列表
            result.append(ongoing_positions[state_key]['trades'])
            # 清空该 (币种, 方向) 的状态，准备迎接下一轮
            ongoing_positions[state_key] = {'qty': 0.0, 'trades': []}

    # 4. 把未平仓完成的剩余订单（如果有）也作为单独的一轮加进去
    for state_key, state_data in ongoing_positions.items():
        if len(state_data['trades']) > 0:
            result.append(state_data['trades'])

    # 5. （可选）按照每轮最后一次交易的时间（平仓时间）倒序排序，最新完成的轮次排在最前面
    result.sort(key=lambda round_trades: round_trades[-1]['orderTime'], reverse=True)

    return result



if __name__ == "__main__":
    # 你只需要在这里修改时间！
    # 格式严格为：'YYYY-MM-DD HH:MM:SS'
    TARGET_START = "2026-06-24 00:00:00"  # 你想要追溯到的最早时间
    TARGET_END = "2026-06-24 23:59:59"  # 你想要拉取的最新截止时间
    all_data = fetch_history_by_time_range("5014426348046646785", TARGET_START, TARGET_END)
    grouped_results = group_trades_by_rounds(all_data)
    print()
    # 建议加上这行，把数据保存到本地文件，方便后续分析
    # with open("binance_orders.json", "w", encoding="utf-8") as f:
    #     json.dump(all_data, f, ensure_ascii=False, indent=2)