# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:23
:last_date:
    2026/4/7 0:23
:description:
    
"""
import json
import os
import uuid

import requests
import pandas as pd
import datetime

from common.common_utils import get_config


def publish_to_binance_square(api_key, text_content):
    """
    根据 Binance Skills Hub 最新规范向币安广场发送发帖请求。

    :param api_key: 币安广场创作者中心生成的 OpenAPI Key
    :param text_content: 帖子正文内容
    """
    # 官方文档指定的 API 端点
    url = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"

    # 构造请求头 (Header) - 严格按照规范说明
    headers = {
        "Content-Type": "application/json",
        "clienttype": "binanceSkill",
        "X-Square-OpenAPI-Key": api_key
    }

    # 核心修改：将 "content" 改为官方最新要求的 "bodyTextOnly"
    payload = {
        "bodyTextOnly": text_content
    }

    # 局部代理配置，只影响当前请求
    proxies = {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890"
    }

    try:
        # 隐藏掉中间的 API Key 字符，避免在控制台全量打印（官方安全规范）
        masked_key = f"{api_key[:5]}...{api_key[-4:]}" if len(api_key) > 10 else "***"
        print(f"⏳ 正在向币安广场发送发帖请求 (使用 Key: {masked_key})...")

        # 增加 proxies 参数
        response = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=15)
        result = response.json()

        # 判断业务是否成功
        if result.get('success') or str(result.get('code')) == '000000':
            print("✅ 恭喜！发布成功！")

            # 根据规范，获取返回数据中的 id 来拼接你的帖子直达链接
            post_id = result.get('data', {}).get('id')
            if post_id:
                post_url = f"https://www.binance.com/square/post/{post_id}"
                print(f"👉 帖子链接: {post_url}")
            return True

        else:
            print("❌ 发布失败，接口返回信息:")
            print(json.dumps(result, ensure_ascii=False, indent=2))

            # 如果报 220009 错误，说明达到了每日发帖上限
            if str(result.get('code')) == '220009':
                print("💡 提示：该错误码通常表示已达到每日发帖数量上限 (Daily post limit)。")
            return False

    except requests.exceptions.RequestException as e:
        print(f"🚨 网络请求发生异常: {e}")
        return False


def get_binance_feed(token="DOGE", desire_count=20, orderBy=2):
    """
    获取币安 Feed 数据
    :param token: 币种名称
    :param desire_count: 期望获取的数据条数，默认为20
    :param orderBy: 1 代表热门 2代表最新
    :return: 目标 vos 列表，失败或无数据时返回 []
    """
    url = "https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/trade/list"

    # 将所有复杂的请求头原样照搬
    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9",
        "bnc-location": "",
        "bnc-time-zone": "Asia/Shanghai",
        "bnc-uuid": "e7ea5e07-ca28-4bba-873a-6fd97d181f8b",
        "clienttype": "web",
        "content-type": "application/json",
        "cookie": "aws-waf-token=f51c8e86-3370-4070-ac5c-415f0e361552:AQoAqSoJApETAAAA:MN8Jeh3xuoAr+Cbt162w+olsrObZE8SnSIMlCcLkmP/ameWfIhlg0IspO3dQkPqIgjhIgIPLyuTrt2Xm/fwrARe7fqULwLxvuu1TsSc6gPPzMPxxYVscNKQSvpjcyocs25gs6BLPlRRT//ci+WLICbK9FNdByJeFOnPmEDGjnWNEZLajJGcRx43d/+OTy1MS5lA=; theme=dark; bnc-uuid=e7ea5e07-ca28-4bba-873a-6fd97d181f8b; userPreferredCurrency=USD_USD; sajssdk_2015_cross_new_user=1; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%2219d27b9f9131bb2-0eb21c83c1e9f28-26061f51-2359296-19d27b9f91428e6%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTlkMjdiOWY5MTMxYmIyLTBlYjIxYzgzYzFlOWYyOC0yNjA2MWY1MS0yMzU5Mjk2LTE5ZDI3YjlmOTE0MjhlNiJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%7D; _gid=GA1.2.1029914182.1774487995; BNC_FV_KEY=3375a92aeff0a1088828bb63852a699067f1724f; BNC_FV_KEY_T=101-2UWasrKs4ekF15fS%2BKlMqv5SebKUF5K00BWqJkLjQ8iE0Ib4Py9K8GwrvWnJnJZI7853k1a1F%2B3g6rYZAsNd%2Fg%3D%3D-LA%2F8O0qqYJjNRqhVsXf%2F2g%3D%3D-d7; BNC_FV_KEY_EXPIRE=1774509598712; changeBasisTimeZone=; _gcl_au=1.1.2053461943.1774488311; g_state={\"i_l\":0,\"i_ll\":1774488312411,\"i_b\":\"Hti4UOgdLZEPsnuPqrAwbnp64On+5RvvhciOCmGyePQ\",\"i_e\":{\"enable_itp_optimization\":0}}; _uetsid=a3965c7028b211f1a285650e7063e82b; _uetvid=a3968b6028b211f1a43b73be74d3c81d; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Mar+26+2026+09%3A26%3A23+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202506.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=f791b433-38c4-4a9e-a1a9-f05cb0758f68&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false; _ga_3WP50LGEEC=GS2.1.s1774487994$o1$g1$t1774488393$j26$l0$h0; _ga=GA1.1.596932932.1774487995",
        "csrftoken": "d41d8cd98f00b204e9800998ecf8427e",
        "device-info": "eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTE1MiIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTEwNCIsInN5c3RlbV92ZXJzaW9uIjoiV2luZG93cyAxMCIsImJyYW5kX21vZGVsIjoidW5rbm93biIsInN5c3RlbV9sYW5nIjoiemgtQ04iLCJ0aW1lem9uZSI6IkdNVCswODowMCIsInRpbWV6b25lT2Zmc2V0IjotNDgwLCJ1c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzE0Ni4wLjAuMCBTYWZhcmkvNTM3LjM2IiwibGlzdF9wbHVnaW4iOiJQREYgVmlld2VyLENocm9tZSBQREYgVmlld2VyLENocm9taXVtIFBERiBWaWV3ZXIsTWljcm9zb2Z0IEVkZ2UgUERGIFZpZXdlcixXZWJLaXQgYnVpbHQtaW4gUERGIiwiY2FudmFzX2NvZGUiOiJmZDJkMWY1NyIsIndlYmdsX3ZlbmRvciI6Ikdvb2dsZSBJbmMuIChOVklESUEpIiwid2ViZ2xfcmVuZGVyZXIiOiJBTkdMRSAoTlZJRElBLCBOVklESUEgR2VGb3JjZSBSVFggMzA5MCAoMHgwMDAwMjIwNCkgRGlyZWN0M0QxMSB2c181XzAgcHNfNV8wLCBEM0QxMSkiLCJhdWRpbyI6IjEyNC4wNDM0NzUyNzUxNjA3NCIsInBsYXRmb3JtIjoiV2luMzIiLCJ3ZWJfdGltZXpvbmUiOiJBc2lhL1NoYW5naGFpIiwiZGV2aWNlX25hbWUiOiJDaHJvbWUgVjE0Ni4wLjAuMCAoV2luZG93cykiLCJmaW5nZXJwcmludCI6ImQzM2I2OTcxYTY3NWUxN2RkODJiMGZmOTFkMDcyOTczIiwiZGV2aWNlX2lkIjoiIiwicmVsYXRlZF9kZXZpY2VfaWRzIjoiIn0=",
        "fvideo-id": "3375a92aeff0a1088828bb63852a699067f1724f",
        "fvideo-token": "G1mTiXfnYgwo6jnjqbRxnfp79DA/laP4sr+ns+oaDK9aiFpf+3KBknh1t2NFUX3uDHKhMIIGMkNm6mxVjkc/5emieR7Zh/5bhsg8lDORfAC1ob7S3a3EyVPC18b+NtSRkMe8dwmO42iQ4ub6MQHHS9CF1KQXIkKpbwvkVP1+JQTNmJfeLORXBEsDy9s+ZT+d0=3c",
        "lang": "zh-CN",
        "origin": "https://www.binance.com",
        "referer": "https://www.binance.com/zh-CN/square/community?token=DOGE",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "x-trace-id": "417ca1cb-11cb-42dd-a7fa-4bbbcb2efbdb",
        "x-ui-request-trace": "417ca1cb-11cb-42dd-a7fa-4bbbcb2efbdb"
    }

    # 动态构建 Payload，使用 desire_count 控制 pageSize
    payload = {
        "token": token,
        "pageIndex": 1,
        "pageSize": desire_count,
        "scene": 2,
        "orderBy": orderBy,
        "contentIds": []
    }

    # 局部代理配置，只影响当前请求
    proxies = {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890"
    }

    try:
        response = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=10)
        response.raise_for_status()
        res_data = response.json()

        # 安全解析：确保结构存在且类型正确，避免 KeyError 或 NoneType 报错
        if res_data and isinstance(res_data.get("data"), dict):
            vos = res_data["data"].get("vos")
            if isinstance(vos, list):
                return vos

        return []
    except Exception as e:
        # 捕获所有异常(断网、代理失效、JSON解析失败等)，确保不影响调用方
        print(f"获取Feed请求失败: {e}")
        return []


def follow_binance_square_user(
        target_uid: str,
        cookie_str: str,
        csrf_token: str = "2d2f5e35a6c06fdcaffb1cc4aed07e97",
        fvideo_id: str = "33e0521b114aa99a931a3ed42ca03cf0147220b0",
        fvideo_token: str = "ifVrRUBCY6qZCeXZsw01SxM6jd0Qo0DWME1oJyDSMHiKii4Xt+8Z608h1u5uwoc2D3SD9SVR+b1vZ9oEIFXnHWvGvXQu9e+pgXh8z2nwJW+MBnod2sizCGcpYJZ+kxiA7J91RuB6pTuW3PGelPy1bNHH4GpnujoVmjyQ5jyh/Vp9v+SB6KskCh/TgksSbn85o=2c"
) -> dict:
    """
    通过币安私有API关注币安广场用户。

    :param target_uid: 目标用户的 Square UID (例如: "CfexsWwIVYYbr1N5GJXlVQ")
    :param cookie_str: 完整的 Cookie 字符串
    :param csrf_token: 请求头中的 csrftoken
    :param fvideo_id: 风控设备标识 ID
    :param fvideo_token: 风控加密 Token
    :return: 接口返回的 JSON 字典
    """

    url = "https://www.binance.com/bapi/composite/v2/private/pgc/user/follow"

    # 动态生成 Trace ID，模拟前端每次请求的不同行为
    trace_id = str(uuid.uuid4())

    headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "bnc-level": "0",
        "bnc-location": "CN",
        "bnc-time-zone": "Asia/Shanghai",
        "bnc-uuid": "2772e08c-2f51-4f76-a3a7-f8d5700463fb",  # 可考虑也提取为参数
        "clienttype": "web",
        "content-type": "application/json",
        "cookie": cookie_str,
        "csrftoken": csrf_token,
        # device-info 保持与你的请求一致（Base64编码后的设备指纹）
        "device-info": "eyJzY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTE1MiIsImF2YWlsYWJsZV9zY3JlZW5fcmVzb2x1dGlvbiI6IjIwNDgsMTEwNCIsInN5c3RlbV92ZXJzaW9uIjoiV2luZG93cyAxMCIsImJyYW5kX21vZGVsIjoidW5rbm93biIsInN5c3RlbV9sYW5nIjoiemgtQ04iLCJ0aW1lem9uZSI6IkdNVCswODowMCIsInRpbWV6b25lT2Zmc2V0IjotNDgwLCJ1c2VyX2FnZW50IjoiTW96aWxsYS81LjAgKFdpbmRvd3MgTlQgMTAuMDsgV2luNjQ7IHg2NCkgQXBwbGVXZWJLaXQvNTM3LjM2IChLSFRNTCwgbGlrZSBHZWNrbykgQ2hyb21lLzE0Ni4wLjAuMCBTYWZhcmkvNTM3LjM2IiwibGlzdF9wbHVnaW4iOiJQREYgVmlld2VyLENocm9tZSBQREYgVmlld2VyLENocm9taXVtIFBERiBWaWV3ZXIsTWljcm9zb2Z0IEVkZ2UgUERGIFZpZXdlcixXZWJLaXQgYnVpbHQtaW4gUERGIiwiY2FudmFzX2NvZGUiOiJmZDJkMWY1NyIsIndlYmdsX3ZlbmRvciI6Ikdvb2dsZSBJbmMuIChOVklESUEpIiwid2ViZ2xfcmVuZGVyZXIiOiJBTkdMRSAoTlZJRElBLCBOVklESUEgR2VGb3JjZSBSVFggMzA5MCAoMHgwMDAwMjIwNCkgRGlyZWN0M0QxMSB2c181XzAgcHNfNV8wLCBEM0QxMSkiLCJhdWRpbyI6IjEyNC4wNDM0NzUyNzUxNjA3NCIsInBsYXRmb3JtIjoiV2luMzIiLCJ3ZWJfdGltZXpvbmUiOiJBc2lhL1NoYW5naGFpIiwiZGV2aWNlX25hbWUiOiJDaHJvbWUgVjE0Ni4wLjAuMCAoV2luZG93cykiLCJmaW5nZXJwcmludCI6ImQzM2I2OTcxYTY3NWUxN2RkODJiMGZmOTFkMDcyOTczIiwiZGV2aWNlX2lkIjoiIiwicmVsYXRlZF9kZXZpY2VfaWRzIjoiIn0=",
        "fvideo-id": fvideo_id,
        "fvideo-token": fvideo_token,
        "lang": "zh-CN",
        "origin": "https://www.binance.com",
        "priority": "u=1, i",
        "referer": f"https://www.binance.com/zh-CN/square/post/309692475255842?sqb=1",
        "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        "x-trace-id": trace_id,
        "x-ui-request-trace": trace_id
    }

    payload = {
        "targetSquareUid": target_uid
    }
    proxies = {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890"
    }

    try:
        response = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=10)        # 币安接口通常返回 HTTP 200，具体业务成功与否看 JSON 里的 code 字段
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        # 返回原始响应以便调试
        if hasattr(e, 'response') and e.response is not None:
            print(f"响应内容: {e.response.text}")
        return {}


if __name__ == "__main__":

    # 替换为你实际抓取到的最新 Cookie (请注意保护个人隐私，不要在公开代码库中泄露)
    MY_COOKIE = """bnc-uuid=2772e08c-2f51-4f76-a3a7-f8d5700463fb; BNC_FV_KEY=33e0521b114aa99a931a3ed42ca03cf0147220b0; lang=zh-CN; userPreferredCurrency=USD_USD; _gcl_au=1.1.17001513.1774485620; se_sd=AwODgTQ9QRODlRUEMEAYgZZAwVQgMEXWlZcJeW0RVVQWwFVNWVIC1; se_gd=gMaWgRxAOBIDFMTxWAQMgZZEFVhYGBXWlRSJeW0RVVQWwE1NWV4R1; se_gsd=SjUiKz9jJislGSMsNQMxBS4ECVBSBgVRWF5CW1FUVFVWNFNT1; bu_s=default; g_state={"i_l":0,"i_ll":1774485995809,"i_b":"NKbj7qtR4btoqh2wSA9LO71loBb9HeqsXc2dMxItGG0","i_e":{"enable_itp_optimization":0}}; BNC-Location=CN; theme=light; neo-theme=light; _gid=GA1.2.1014400770.1775363722; aws-waf-token=49523bea-cc30-499d-a6b5-7bb90a39c73e:AQoAhq9Ms/ICAAAA:zFrMUI32RGjn21sFH0tvoqKElNySMSYU8MiIuoJAVDtXDx000yMwC3zqBa1Qa7LApGeXibZUCk4DvTVmY03GcodqnyNhbyIoe11vHrdIaiRNOCpNQ8DKQ1iDndciMjQXY3c6ZKP+3vK/S8fg8PImMas8pAkK7GvaG8SkAb1F+RvJ5YbXEOnorSTveSc61n8E3kg=; changeBasisTimeZone=; _uetsid=c9f13b7031ca11f1950ba10f78e7c9e6; _uetvid=5eef827028ac11f18ddb31237e05f437; futures-layout=pro; _h_desk_key=8ea5542d32cc4284879ca8a4fc7ad41a; BNC_FV_KEY_T=101-Ylebao0wIcEz2Uj9MpbnsXPTrZWNWkYZp3hQyb1VE8vcKbwuvXsebklUt%2BlP0cq24ACFZyMmaWab5EPoNZ%2FyLg%3D%3D-KCNuKETgXuNoVNoZxGJBdA%3D%3D-2d; BNC_FV_KEY_EXPIRE=1775580632060; s9r1=9295BE8EC4891D0CFBBF5377C54B9454; r20t=web.D6D0A9FDF865D8BFC6029A44842218F3; r30t=1; cr00=156D95BCEAE710B7D5E8FC3A9885B90E; d1og=web.1229561321.7C28B45C2405D999E4CD78CE8DAD6698; r2o1=web.1229561321.19F0081C45D206B68BE35A511F1AB277; f30l=web.1229561321.72BF426ACE0E086CEB5DE498225372F0; currentAccount=; logined=y; p20t=web.1229561321.4519E52C213DCEFA2E046A8E6753B05E; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221229561321%22%2C%22first_id%22%3A%2219c67f1247fd9f-0944324976c3ab8-26061d51-2359296-19c67f12480b26%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTljNjdmMTI0N2ZkOWYtMDk0NDMyNDk3NmMzYWI4LTI2MDYxZDUxLTIzNTkyOTYtMTljNjdmMTI0ODBiMjYiLCIkaWRlbnRpdHlfbG9naW5faWQiOiIxMjI5NTYxMzIxIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%221229561321%22%7D%2C%22%24device_id%22%3A%2219c67f2cd51ef8-07bb7118048b2d8-26061d51-2359296-19c67f2cd52129f%22%7D; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Apr+07+2026+18%3A52%3A41+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202506.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=5029ba14-0415-4560-be72-530391e051ed&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false; _ga=GA1.1.1913380912.1772289141; _ga_3WP50LGEEC=GS2.1.s1775559031$o31$g1$t1775559167$j6$l0$h0"""
    TARGET_UID = "CfexsWwIVYYbr1N5GJXlVQ"

    # 执行关注
    result = follow_binance_square_user(
        target_uid=TARGET_UID,
        cookie_str=MY_COOKIE
        # 如果报错风控拦截，你需要在这里手动传入最新的 csrf_token 和 fvideo_token
    )

    print(json.dumps(result, indent=2, ensure_ascii=False))


    #
    #
    #
    # # print("=" * 40)
    # # print("1. 测试: 获取币安 Feed 数据")
    # # print("=" * 40)
    # # feed_data = get_binance_feed(token="DOGE", desire_count=2)
    # # print(f"✅ 获取到 {len(feed_data)} 条 Feed 数据。\n")
    #
    # print("=" * 40)
    # print("3. 测试: 广场自动化发帖")
    # print("=" * 40)
    # YOUR_SQUARE_API_KEY = get_config("myself_square_api_key")
    # test_text = """$ETH $BTC 祝愿大家发大财 天天开心"""
    #
    # if YOUR_SQUARE_API_KEY != "替换成你的_API_KEY":
    #     publish_to_binance_square(api_key=YOUR_SQUARE_API_KEY, text_content=test_text)
    # else:
    #     print("⚠️ 请先在代码中填入你的 API Key 再运行发帖测试！\n")