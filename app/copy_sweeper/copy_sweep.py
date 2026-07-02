import os
import re
from urllib.parse import urlparse
import requests
import json
import time
import random
from datetime import datetime

from common.common_utils import save_json


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

                # ==================== 新增判断逻辑开始 ====================
                # 1. 正确判定业务是否成功
                api_code = str(res_data.get("code", "000000"))
                api_success = res_data.get("success", True)

                if api_code != "000000" or api_success is False:
                    # 2. 失败时打印详细的日志
                    print(f"❌ API 业务失败！系统正忙或触发风控。")
                    print(f"   详细原因 -> Code: {api_code}, Message: {res_data.get('message')}")
                    print(f"   完整响应 -> {res_data}")

                    retries += 1
                    if retries >= max_retries:
                        print("❌ 业务异常重试次数耗尽，中断当前拉取。")
                        return all_orders

                    # 失败后休眠并重试，避免直接当作 success = True 往下走
                    time.sleep(3 * retries)
                    continue
                    # ==================== 新增判断逻辑结束 ====================

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
    # 将orderTime 按照降序排序，方便后续处理
    all_orders.sort(key=lambda x: x.get("orderTime") or x.get("createTime") or x.get("time"), reverse=True)

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


def fetch_binance_copy_traders(target_count: int) -> list:
    """
    持续拉取币安跟单交易员列表，直到满足指定的数量。

    :param target_count: 需要拉取的总条数
    :return: 包含交易员信息的列表
    """
    url = "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list"

    # 将 curl 中的 headers 转换为字典 (建议定期更新这些会过期的 token)
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "bnc-level": "0",
        "bnc-location": "CN",
        "bnc-time-zone": "Asia/Shanghai",
        "bnc-uuid": "2772e08c-2f51-4f76-a3a7-f8d5700463fb",
        "clienttype": "web",
        "content-type": "application/json",
        "fvideo-id": "33e0521b114aa99a931a3ed42ca03cf0147220b0",
        "fvideo-token": "1QkvzsWMC7dTM90sd+1CDpNi9KuLu8Sb2Oozg1K72UFTgZPiCi5ZXRiw45god7t2bEf3/wXtwrWTQiJQqnOKs+d4g/Dsg6ygUsqkmnnjM2SS2XMMT3e1RVjKJCnxlGihE1Cym9j9iXwQa51zQSE8Mhmg4jAtJjX9k7aOxac8iRfpHTkWnN5E22MzV0LecO6IA=76",
        "device-info": "eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTE1MiIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTEwNCIsInN5c3RlbV92ZXJzaW9uIjoiV2luZG93cyAxMCIsImJyYW5kX21vZGVsIjoidW5rbm93biIsInN5c3RlbV9sYW5nIjoiemgtQ04iLCJ0aW1lem9uZSI6IkdNVCswODowMCIsInRpbWV6b25lT2Zmc2V0IjotNDgwLCJ1c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzE0OS4wLjAuMCBTYWZhcmkvNTM3LjM2IiwibGlzdF9wbHVnaW4iOiJQREYgVmlld2VyLENocm9tZSBQREYgVmlld2VyLENocm9taXVtIFBERiBWaWV3ZXIsTWljcm9zb2Z0IEVkZ2UgUERGIFZpZXdlcixXZWJLaXQgYnVpbHQtaW4gUERGIiwiY2FudmFzX2NvZGUiOiJmZDJkMWY1NyIsIndlYmdsX3ZlbmRvciI6Ikdvb2dsZSBJbmMuIChOVklESUEpIiwid2ViZ2xfcmVuZGVyZXIiOiJBTkdMRSAoTlZJRElBLCBOVklESUEgR2VGb3JjZSBSVFggMzA5MCAoMHgwMDAwMjIwNCkgRGlyZWN0M0QxMSB2c181XzAgcHNfNV8wLCBEM0QxMSkiLCJhdWRpbyI6IjEyNC4wNDM0NzUyNzUxNjA3NCIsInBsYXRmb3JtIjoiV2luMzIiLCJ3ZWJfdGltZXpvbmUiOiJBc2lhL1NoYW5naGFpIiwiZGV2aWNlX25hbWUiOiJDaHJvbWUgVjE0OS4wLjAuMCAoV2luZG93cykiLCJmaW5nZXJwcmludCI6ImQyNWE4ZWY3YWMyNTljODY2MDBiNjgwN2M0OWY1MjJhIiwiZGV2aWNlX2lkIjoiIiwicmVsYXRlZF9kZXZpY2VfaWRzIjoiIn0=",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
        # Cookie 太长了，直接原样放入
        "cookie": "bnc-uuid=2772e08c-2f51-4f76-a3a7-f8d5700463fb; BNC_FV_KEY=33e0521b114aa99a931a3ed42ca03cf0147220b0; lang=zh-CN; userPreferredCurrency=USD_USD; se_sd=AwODgTQ9QRODlRUEMEAYgZZAwVQgMEXWlZcJeW0RVVQWwFVNWVIC1; se_gd=gMaWgRxAOBIDFMTxWAQMgZZEFVhYGBXWlRSJeW0RVVQWwE1NWV4R1; se_gsd=SjUiKz9jJislGSMsNQMxBS4ECVBSBgVRWF5CW1FUVFVWNFNT1; BNC-Location=CN; theme=light; changeBasisTimeZone=; OptanonAlertBoxClosed=2026-04-11T10:53:31.694Z; neo-theme=light_glacier; registerChannel=SEO_page; _h_desk_key=ffdf3642dc6d42a1878a070025c15c37; bu_s=copylink; bu_a=app_square_share_link; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219c67f1247fd9f-0944324976c3ab8-26061d51-2359296-19c67f12480b26%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%2C%22%24latest_utm_source%22%3A%22copylink%22%2C%22%24latest_utm_campaign%22%3A%22app_square_share_link%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTljNjdmMTI0N2ZkOWYtMDk0NDMyNDk3NmMzYWI4LTI2MDYxZDUxLTIzNTkyOTYtMTljNjdmMTI0ODBiMjYifQ%3D%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%2219c67f2cd51ef8-07bb7118048b2d8-26061d51-2359296-19c67f2cd52129f%22%7D; _gcl_au=1.1.665578272.1782303411; futures-layout=pro; aws-waf-token=ab2f887c-774a-44a7-8b45-e2ea0e5dd90b:AQoAumQO0CECAAAA:6mEQZXvkCktP6nvc+/GcHUanl6KwxB8xjdFOuIa4RFdmPJmAG6AcbP4XZs6aDeJD+y8NhNY1Lf89AmSVUgdOly63Cok5xe90aHtTPXbEu3kfa6t+DKMKgXiqVmKxWFB4jkuRqMBax/I/JVawTK8o00QBcdJ+Ah/EkIVtsDgvPRKaa+o04CYthlaSRoW9/dgtHGE=; _gid=GA1.2.823222446.1782871739; BNC_FV_KEY_T=101-Fv5V3XFYpjdexzV%2Bt%2BmZj98vX3HV%2ByphhwnQKMiShaC2iimGR5pW8XZf8xjPrj9yGxqfPgXWfE3PZIDeAnlAJQ%3D%3D-E5WT%2FlNIOpL0tC%2B0Dp8grQ%3D%3D-ac; BNC_FV_KEY_EXPIRE=1782916799952; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Jul+01+2026+20%3A01%3A13+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202604.2.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=5029ba14-0415-4560-be72-530391e051ed&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false&intType=1&geolocation=JP%3B13&isDntEnabled=0&prevHadToken=0; g_state={\"i_l\":0,\"i_ll\":1782907273625,\"i_b\":\"CchDKvRW0UGMwhIG+7AZR9ZZ3Co12oreIRATpIE4d9k\",\"i_e\":{\"enable_itp_optimization\":24},\"i_et\":1782907273625}; _gat_UA-162512367-1=1; _uetsid=ae4cd300751e11f18221df9cd7baf509; _uetvid=5eef827028ac11f18ddb31237e05f437; _ga_3WP50LGEEC=GS2.1.s1782907272$o247$g1$t1782907281$j51$l0$h0; _ga=GA1.1.1913380912.1772289141"
    }

    all_traders = []
    page_number = 1
    page_size = 20  # 每次拉取 20 条

    print(f"开始拉取数据，目标数量: {target_count}...")

    while len(all_traders) < target_count:
        # 构建动态 payload
        payload = {
            "pageNumber": page_number,
            "pageSize": page_size,
            "timeRange": "30D",
            "dataType": "PNL",
            "favoriteOnly": False,
            "hideFull": False,
            "nickname": "",
            "order": "DESC",
            "userAsset": 0,
            "portfolioType": "PUBLIC_PRIVATE",
            "useAiRecommended": True
        }

        try:
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()  # 检查 HTTP 状态码
            response_json = response.json()

            # 这里假设正常返回结构为 response_json['data']['list']
            # 如果结构有变，请根据实际 print 出来的 JSON 调整字典访问逻辑
            current_page_data = response_json.get('data', {}).get('list', [])

            if not current_page_data:
                print(f"第 {page_number} 页返回数据为空，拉取结束。可能已触底。")
                break

            all_traders.extend(current_page_data)
            print(f"第 {page_number} 页拉取成功: 获得 {len(current_page_data)} 条数据，当前总计 {len(all_traders)} 条。")

            page_number += 1

            # 为了防止被高频风控，每次请求后 sleep 一下 (重要)
            time.sleep(1.5)

        except Exception as e:
            print(f"拉取第 {page_number} 页时发生错误: {e}")
            print(f"可能是 Token 过期被拦截，服务器返回内容: {response.text[:200]}")
            break

    # 如果拉取的总数超过了目标数量，进行切片截取
    if len(all_traders) > target_count:
        all_traders = all_traders[:target_count]

    print(f"\n拉取完成，最终获取数量: {len(all_traders)} 条。")
    return all_traders





def extract_lead_id(url: str) -> str:
    """
    从币安带单链接中提取纯数字 ID。

    参数:
        url (str): 包含带单者 ID 的链接

    返回:
        str: 提取出的数字 ID。如果提取失败或输入不合法，返回 None。
    """
    # 1. 基础的输入校验
    if not url or not isinstance(url, str):
        return None

    url = url.strip()  # 去除首尾可能带入的空格或换行符

    # 2. 核心方法：使用正则表达式精准匹配 'lead-details/' 后面的数字
    # \d+ 表示匹配一个或多个数字，() 用于提取这部分内容
    pattern = re.compile(r'lead-details/(\d+)')
    match = pattern.search(url)

    if match:
        return match.group(1)

    # 3. 增强健壮性（Fallback 机制）：
    # 如果未来币安把 'lead-details' 改成了 'portfolio' 或者其他词，上面的正则会失效。
    # 我们可以通过解析 URL 的 Path，寻找里面那串极长的数字（通常超过 15 位）来兜底。
    try:
        parsed_url = urlparse(url)
        path = parsed_url.path  # 获取 URL 路径，自动过滤掉 ?timeRange=30D 这种查询参数

        # 匹配以 / 开头，且连续数字长度大于 15 位的字符串
        fallback_pattern = re.compile(r'/(\d{15,})')
        fallback_match = fallback_pattern.search(path)

        if fallback_match:
            return fallback_match.group(1)

    except Exception as e:
        # 捕捉解析异常，防止程序崩溃
        pass

    # 如果都找不到，返回 None
    return None


def get_report(url_str):
    lead_id = extract_lead_id(url_str)
    if not lead_id:
        print(f"❌ 无法从链接中提取 Lead ID: {url_str}")
        return {}


if __name__ == "__main__":

    traders_list = fetch_binance_copy_traders(400)
    trader_map = {trader['leadPortfolioId']: trader['nickname'] for trader in traders_list}

    # 你只需要在这里修改时间！
    # 格式严格为：'YYYY-MM-DD HH:MM:SS'
    TARGET_START = "2026-05-30 23:59:59"  # 你想要追溯到的最早时间
    TARGET_END = "2026-06-30 23:59:59"  # 你想要拉取的最新截止时间

    output_dir = "temp_data"
    os.makedirs(output_dir, exist_ok=True)
    for portfolio_id, nickname in trader_map.items():
        output_file = os.path.join(output_dir, f"{portfolio_id}_{nickname}.json")
        if os.path.exists(output_file):
            print(f"⚠️ 文件已存在，跳过: {output_file}")
            continue

        all_data = fetch_history_by_time_range(portfolio_id, TARGET_START, TARGET_END)
        save_json(output_file, all_data)
