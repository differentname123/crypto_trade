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
import logging
import os
import random
import re
import time
import traceback
import uuid

import requests
import pandas as pd
import datetime

from common.common_utils import get_config, setup_logger, save_json, read_json

setup_logger()

# 2. 拿到属于当前文件的专属 logger
logger = logging.getLogger(__name__)


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
        "http": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443",
        "https": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443"
    }

    # 隐藏掉中间的 API Key 字符，避免在控制台全量打印（官方安全规范）
    masked_key = f"{api_key[:5]}...{api_key[-4:]}" if len(api_key) > 10 else "***"
    # 提取内容摘要，防止内容过长刷屏
    text_summary = text_content[:20].replace('\n', ' ') + ("..." if len(text_content) > 20 else "")

    try:
        logger.info(f"⏳ 开始向币安广场发帖 | Key: {masked_key} | 内容摘要: {text_summary}")

        # 增加 proxies 参数
        response = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=15)
        result = response.json()

        # 判断业务是否成功
        if result.get('success') or str(result.get('code')) == '000000':
            post_id = result.get('data', {}).get('id')
            post_url = f"https://www.binance.com/square/post/{post_id}" if post_id else "未知链接"
            # 聚合成功日志
            logger.info(f"✅ 发帖成功 | 帖子ID: {post_id} | 直达链接: {post_url}")
            return True

        else:
            # 聚合失败信息、错误码解读、接口原报文至一行，避免多行打印被并发冲散
            error_code = str(result.get('code'))
            hint = " (💡提示: 已达每日发帖上限 Daily limit)" if error_code == '220009' else ""
            logger.error(f"❌ 发帖失败 | 错误码: {error_code}{hint} | 完整响应: {result}")
            return False

    except requests.exceptions.RequestException as e:
        logger.error(f"🚨 发帖网络请求异常 | Key: {masked_key} | 异常信息: {e}")
        return False


def clean_universal_binance_data(raw_data_list):
    """
    终极清洗函数：丢弃无用类型，拉平嵌套内容，找回核心作者ID，并去除占位符。
    """
    if not isinstance(raw_data_list, list):
        logger.error("Data clean failed: Input is not a list.")
        return []

    cleaned_list = []

    def process_item(item):
        if not isinstance(item, dict):
            return

        card_type = item.get("cardType", "UNKNOWN")

        # 规则 1：彻底丢弃纯语聊房 (没有文章内容) 和 投票帖 (POLL)
        if card_type == "SPACE_LIVE" or card_type == "POLL":
            return

        # 规则 2：拉平嵌套的“推文串”，剥洋葱提取有效内容
        if card_type == "BUZZ_REPLY_POST_LIST":
            for nested_item in item.get("replyPostList") or []:
                process_item(nested_item)
            return

        # 规则 3：智能拼接完整文本（已加入复读机修复逻辑）
        text_content = ""
        if card_type == "BUZZ_LONG":
            title = item.get("title") or ""
            sub_title = item.get("subTitle") or ""
            content = item.get("content") or ""

            # 顺手修复媒体号API可能返回的字符串 "null"
            if sub_title.strip().lower() == "null":
                sub_title = ""

            # 核心修复：消除长文标题与正文重复的“复读机”现象
            # 如果正文开头已经包含了标题，就不需要再在前面拼接标题了
            if title and content.startswith(title):
                title_part = ""
            elif title:
                title_part = f"【{title}】\n"
            else:
                title_part = ""

            # 将有效的部分按顺序拼接
            parts = [p for p in [title_part, sub_title, content] if p]
            text_content = "\n\n".join(parts)
        else:
            text_content = item.get("content") or ""

        # 清除币安前端挂件宏定义如 {future}(BTCUSDT)
        text_content = re.sub(r'\{.*?\}.*?\)', '', text_content).strip()

        # 规则 4：找回完整的作者信息（核心跟踪字段）
        author_info = item.get("author") or {}
        square_author_id = author_info.get("squareAuthorId") or item.get("squareAuthorId")
        username = author_info.get("username") or item.get("username")
        author_name = author_info.get("authorName") or item.get("authorName")

        # 规则 5：提取媒体资源
        media_urls = []
        for img in item.get("imageMetaList") or []:
            if isinstance(img, dict) and img.get("url"):
                media_urls.append(img.get("url"))

        cover_meta = item.get("coverMeta")
        if isinstance(cover_meta, dict) and cover_meta.get("url"):
            media_urls.append(cover_meta.get("url"))

        video_vo = item.get("videoVO")
        if isinstance(video_vo, dict) and video_vo.get("videoLink"):
            media_urls.append(video_vo.get("videoLink"))

        # 统一组装对象
        cleaned_item = {
            "post_id": item.get("id"),
            "card_type": card_type,
            "publish_time": item.get("date"),
            "url": item.get("webLink") or item.get("quotedContentWebLink"),
            "author": {
                "squareAuthorId": square_author_id,
                "username": username,
                "authorName": author_name
            },
            "text_content": text_content,
            "media_urls": list(set(media_urls)),
            "metrics": {
                "viewCount": item.get("viewCount") or 0,
                "likeCount": item.get("likeCount") or 0,
                "commentCount": item.get("commentCount") or 0
            }
        }

        # 底线校验：必须有 ID 和 正文才入库
        if cleaned_item["post_id"] and len(cleaned_item["text_content"]) > 0:
            cleaned_list.append(cleaned_item)

    try:
        for item in raw_data_list:
            process_item(item)

        logger.info(f"Data clean complete. Extracted {len(cleaned_list)} pure article records.")
        return cleaned_list
    except Exception as e:
        logger.error(f"Data clean aborted due to exception: {e}")
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
        "http": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443",
        "https": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443"
    }

    try:
        logger.info(f"⏳ 开始关注广场用户 | 目标UID: {target_uid} | TraceID: {trace_id}")

        response = requests.post(url, headers=headers, json=payload, proxies=proxies,
                                 timeout=10)  # 币安接口通常返回 HTTP 200，具体业务成功与否看 JSON 里的 code 字段
        response.raise_for_status()

        resp_json = response.json()
        logger.info(
            f"✅ 关注用户请求完成 | 目标UID: {target_uid} | 返回Code: {resp_json.get('code')} | 完整响应: {resp_json}")
        return resp_json

    except requests.exceptions.RequestException as e:
        # 将错误信息聚合在一行
        resp_text = e.response.text if hasattr(e, 'response') and e.response is not None else "无响应内容"
        logger.error(f"🚨 关注用户请求异常 | 目标UID: {target_uid} | 异常信息: {e} | 响应内容: {resp_text}")
        return {}


def fetch_binance_feed_recommend(required_count, content_ids=None):
    """
    不断拉取币安广场的推荐数据，直到满足需要的数量

    :param required_count: 需要拉取的目标数据总数 (vos 的数量)
    :param content_ids: 已经抓取过的内容ID列表，用于去重和向下滑动参数
    :return: 包含目标数据的列表
    """
    # 修复 Python 危险的默认可变参数问题
    if content_ids is None:
        content_ids = []

    url = "https://www.binance.com/bapi/composite/v9/friendly/pgc/feed/feed-recommend/list"

    # 统一代理设置格式
    proxies = {
        "http": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443",
        "https": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443"
    }

    # 请求头保持原样
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
        "lang": "zh-CN",
        "X-UI-REQUEST-TRACE": str(uuid.uuid4()),
        "X-TRACE-ID": str(uuid.uuid4()),
        "Content-Type": "application/json",
        "clienttype": "web",
        "versioncode": "web",
        "BNC-Time-Zone": "Asia/Shanghai",
        "referrer": "https://www.binance.com/zh-CN/square"
    }

    all_vos = []
    page_index = 1
    page_size = 20
    max_retries = 5
    retry_count = 0

    while len(all_vos) < required_count:
        payload = {
            "pageIndex": page_index,
            "pageSize": page_size,
            "scene": "web-homepage",
            "contentIds": content_ids
        }

        try:
            response = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=15)

            # 统一强暴露 400+ 报错
            if response.status_code >= 400:
                logger.error(f"🚨 HTTP {response.status_code} 报错，币安服务器返回详细原因: {response.text}")

            response.raise_for_status()
            res_data = response.json()

            # 统一安全解析结构
            data_field = res_data.get("data")
            vos = []
            if isinstance(data_field, dict):
                vos = data_field.get("vos") or data_field.get("list") or []
            elif isinstance(data_field, list):
                vos = data_field

            if not vos:
                logger.warning("⚠️ 推荐接口未返回更多数据，可能已经到底。")
                break

            all_vos.extend(vos)
            logger.info(f"第 {page_index} 页抓取成功，新增 {len(vos)} 条，当前总计：{len(all_vos)} 条")

            # 提取已获取内容的ID供下一次请求使用
            for item in vos:
                item_id = item.get("id") or item.get("contentId")
                if item_id:
                    content_ids.append(str(item_id))

            page_index += 1
            retry_count = 0

            # 统一增加防风控休眠
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            # 统一重试日志格式
            logger.error(f"🚨 请求发生异常: {e}")
            retry_count += 1
            if retry_count >= max_retries:
                logger.warning(f"❌ 请求异常累计达到 {max_retries} 次，停止重试。")
                break

    # 统一去重逻辑与日志
    seen_ids = set()
    deduped_vos = []
    for item in all_vos:
        item_id = item.get("id") or item.get("contentId")
        if item_id not in seen_ids:
            seen_ids.add(item_id)
            deduped_vos.append(item)

    original_count = len(all_vos)
    deduped_count = len(deduped_vos)
    logger.info(
        f"✅ [推荐流] 去重完成 | 抓取: {original_count} 条 | 移除重复: {original_count - deduped_count} 条 | 最终保留: {deduped_count} 条")

    return deduped_vos


def fetch_binance_feed_search(keyword, required_count, search_type=1):
    """
    不断拉取币安广场的搜索结果，直到满足需要的数量或没有更多数据

    :param keyword: 搜索关键词 (例如 "doge")
    :param required_count: 需要拉取的目标数据总数
    :param search_type: 搜索类型，默认为1 (通常代表综合或文章)
    :return: 包含目标搜索结果的列表
    """
    url = "https://www.binance.com/bapi/composite/v2/friendly/pgc/feed/search/list"

    # 统一代理设置格式
    proxies = {
        "http": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443",
        "https": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443"
    }

    # 请求头保持原样
    headers = {
        "User-Agent": "Mozilla/5.0 ...",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9...",
        "device-info": "",
        "lang": "zh-CN",
        "Content-Type": "application/json",
        "clienttype": "web",
        "versioncode": "2.61.0",
        "BNC-UUID": str(uuid.uuid4()),
        "referrer": f"https://www.binance.com/zh-CN/square/search?s={keyword}"
    }

    all_vos = []
    page_index = 1
    page_size = 20
    max_retries = 5
    retry_count = 0

    while len(all_vos) < required_count:
        payload = {
            "scene": "web",
            "pageIndex": page_index,
            "pageSize": page_size,
            "searchContent": keyword,
            "type": search_type
        }

        try:
            response = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=15)

            # 统一强暴露 400+ 报错
            if response.status_code >= 400:
                logger.error(f"🚨 HTTP {response.status_code} 报错，币安服务器返回详细原因: {response.text}")

            response.raise_for_status()
            res_data = response.json()

            # 统一安全解析结构
            data_field = res_data.get("data")
            vos = []
            if isinstance(data_field, dict):
                vos = data_field.get("vos") or data_field.get("list") or []
            elif isinstance(data_field, list):
                vos = data_field

            if not vos:
                logger.warning(f"⚠️ 搜索接口未返回更多数据，关键词 '{keyword}' 可能已到底。")
                break

            all_vos.extend(vos)
            logger.info(f"第 {page_index} 页抓取成功，新增 {len(vos)} 条，当前总计：{len(all_vos)} 条")

            page_index += 1
            retry_count = 0

            # 统一增加防风控休眠
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            # 统一重试日志格式
            logger.error(f"🚨 请求发生异常: {e}")
            retry_count += 1
            if retry_count >= max_retries:
                logger.warning(f"❌ 请求异常累计达到 {max_retries} 次，停止重试。")
                break

    # 统一去重逻辑与日志
    seen_ids = set()
    deduped_vos = []
    for item in all_vos:
        item_id = item.get("id") or item.get("contentId")
        if item_id not in seen_ids:
            seen_ids.add(item_id)
            deduped_vos.append(item)

    original_count = len(all_vos)
    deduped_count = len(deduped_vos)
    logger.info(
        f"✅ [搜索:{keyword}] 去重完成 | 抓取: {original_count} 条 | 移除重复: {original_count - deduped_count} 条 | 最终保留: {deduped_count} 条")

    return deduped_vos


def get_binance_feed_token(token="DOGE", required_count=20, orderBy=2):
    """
    获取币安指定币种的社区 Feed 数据

    :param token: 币种名称 (如 "DOGE")
    :param required_count: 期望获取的数据条数，默认为 20
    :param orderBy: 1 代表热门，2 代表最新
    :return: 目标 vos 列表，失败或无数据时返回 []
    """
    url = "https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/trade/list"

    # 请求头保持原样
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,zh-TW;q=0.8,zh-HK;q=0.7,en-US;q=0.6,en;q=0.5",
        "lang": "zh-CN",
        "Content-Type": "application/json",
        "clienttype": "web",
        "device-info": "",
        "BNC-UUID": str(uuid.uuid4()),
        "X-UI-REQUEST-TRACE": str(uuid.uuid4()),
        "X-TRACE-ID": str(uuid.uuid4()),
        "csrftoken": "d41d8cd98f00b204e9800998ecf8427e",
        "BNC-Time-Zone": "Asia/Shanghai",
        "referrer": f"https://www.binance.com/zh-CN/square/community?token={token}"
    }

    # 统一代理设置格式
    proxies = {
        "http": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443",
        "https": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443"
    }

    all_vos = []
    page_index = 1
    page_size = 20
    max_retries = 5
    retry_count = 0

    logger.info(f"⏳ 开始获取Feed数据 | Token: {token} | 目标条数: {required_count} | 排序: {orderBy}")

    while len(all_vos) < required_count:
        payload = {
            "token": token,
            "pageIndex": page_index,
            "pageSize": page_size,
            "scene": 2,
            "orderBy": orderBy
        }

        try:
            response = requests.post(url, headers=headers, json=payload, proxies=proxies, timeout=15)

            # 统一强暴露 400+ 报错
            if response.status_code >= 400:
                logger.error(f"🚨 HTTP {response.status_code} 报错，币安服务器返回详细原因: {response.text}")

            response.raise_for_status()
            res_data = response.json()

            # 统一安全解析结构
            data_field = res_data.get("data")
            vos = []
            if isinstance(data_field, dict):
                vos = data_field.get("vos") or data_field.get("list") or []
            elif isinstance(data_field, list):
                vos = data_field

            if not vos:
                logger.warning(f"⚠️ 币种接口未返回更多数据，Token '{token}' 可能已经到底。")
                break

            all_vos.extend(vos)
            logger.info(f"第 {page_index} 页抓取成功，新增 {len(vos)} 条，当前总计：{len(all_vos)} 条")

            page_index += 1
            retry_count = 0

            # 统一增加防风控休眠
            time.sleep(random.uniform(0.5, 1.5))

        except Exception as e:
            # 统一重试日志格式
            logger.error(f"🚨 请求发生异常: {e}")
            retry_count += 1
            if retry_count >= max_retries:
                logger.warning(f"❌ 请求异常累计达到 {max_retries} 次，停止重试。")
                break

    # 统一去重逻辑与日志
    seen_ids = set()
    deduped_vos = []
    for item in all_vos:
        item_id = item.get("id") or item.get("contentId")
        if item_id not in seen_ids:
            seen_ids.add(item_id)
            deduped_vos.append(item)

    original_count = len(all_vos)
    deduped_count = len(deduped_vos)
    logger.info(
        f"✅ [币种:{token}] 去重完成 | 抓取: {original_count} 条 | 移除重复: {original_count - deduped_count} 条 | 最终保留: {deduped_count} 条")

    return deduped_vos


def fetch_binance_feed(count=20, keyword=None, token=None, **kwargs):
    """
    统一的币安广场数据拉取接口（智能路由版）
    根据传入的参数自动判断拉取模式：
    - 传 keyword -> 走搜索流
    - 传 token -> 走币种流
    - 都不传 -> 走推荐流
    """
    # 1. 优先判断是否是“搜索流” (传了 keyword)
    if keyword:
        logger.info(f"🔄 自动路由: [搜索模式] | 关键词: {keyword} | 目标条数: {count}")
        search_type = kwargs.get("search_type", 1)
        feed_list = fetch_binance_feed_search(keyword=keyword, required_count=count, search_type=search_type)

    # 2. 判断是否是“指定币种流” (传了 token)
    elif token:
        token = str(token).upper()  # 自动转大写容错
        logger.info(f"🔄 自动路由: [币种模式] | 币种: {token} | 目标条数: {count}")
        order_by = kwargs.get("orderBy", 2)
        # 统一参数名：将 desire_count 替换为了 required_count
        feed_list = get_binance_feed_token(token=token, required_count=count, orderBy=order_by)

    # 3. 如果什么目标都没传，默认走“推荐流”
    else:
        logger.info(f"🔄 自动路由: [推荐流模式] | 目标条数: {count}")
        content_ids = kwargs.get("content_ids", [])
        feed_list = fetch_binance_feed_recommend(required_count=count, content_ids=content_ids)
    # clean_feed_list = clean_universal_binance_data(feed_list)
    return feed_list

def fetch_binance_post_detail(post_id):
    """
    根据文章 ID 获取币安广场帖子的详细内容

    :param post_id: 文章/帖子的唯一 ID (例如: 341500227496929)
    :return: 包含详情数据的字典 (通常为解析后的 'data' 字段)，如果失败则返回 None
    """
    # 动态拼接详情页的 API URL
    url = f"https://www.binance.com/bapi/composite/v3/friendly/pgc/special/content/detail/{post_id}"

    # 统一代理设置格式
    proxies = {
        "http": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443",
        "https": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443"
    }

    # 简化 Header：去除了繁琐的 device-info、视频 token 和指纹，保留核心验证头
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "lang": "zh-CN",
        "X-UI-REQUEST-TRACE": str(uuid.uuid4()),  # 动态生成避免被追踪特征
        "X-TRACE-ID": str(uuid.uuid4()),  # 动态生成避免被追踪特征
        "Content-Type": "application/json",
        "clienttype": "web",
        "BNC-Time-Zone": "Asia/Shanghai",
        "referrer": f"https://www.binance.com/zh-CN/square/post/{post_id}"  # referrer 也需要动态跟随 post_id
    }

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            # 注意：原 fetch 请求是 GET 方法，不需要传 payload/json body
            response = requests.get(url, headers=headers, proxies=proxies, timeout=15)

            # 统一强暴露 400+ 报错
            if response.status_code >= 400:
                logger.error(f"🚨 HTTP {response.status_code} 报错，币安服务器返回详细原因: {response.text}")

            # 触发 HTTP 错误异常（走入 except 分支重试）
            response.raise_for_status()
            res_data = response.json()

            # 统一安全解析结构，通常详情在 data 字段下
            data_field = res_data.get("data")

            if not data_field:
                logger.warning(f"⚠️ 详情接口未返回有效数据 (Post ID: {post_id}): {res_data}")
            else:
                logger.info(f"✅ 成功获取文章详情 (Post ID: {post_id})")

            return data_field

        except Exception as e:
            # 统一重试日志格式
            logger.error(f"🚨 请求文章详情发生异常 (Post ID: {post_id}): {e}")
            retry_count += 1

            if retry_count >= max_retries:
                logger.warning(f"❌ 请求异常累计达到 {max_retries} 次，停止重试。")
                break

            # 统一增加防风控休眠 (失败时休眠，防止高频轰炸被封 IP)
            time.sleep(random.uniform(0.5, 1.5))

    return None


def clean_short_posts(raw_data_list):
    cleaned_list = []

    for raw_item in raw_data_list:
        # 1. 安全提取正文
        text_content = raw_item.get("content", "").strip() if raw_item.get("content") else ""

        # 2. 修复提到币种 (完全依赖 API 的结构化字段，摒弃正则防脏数据)
        raw_coins = raw_item.get("coinPairList") or []
        cleaned_coins_list = [coin.replace('$', '').strip() for coin in raw_coins if coin]

        # 从 tradingPairsV2 中提取防漏抓
        trading_pairs_v2 = raw_item.get("tradingPairsV2") or []
        for pair in trading_pairs_v2:
            code = pair.get("code")
            if code:
                cleaned_coins_list.append(code.strip())

        # 合并、去重，并统一转大写
        cleaned_coins = list(set(coin.upper() for coin in cleaned_coins_list if coin))

        # 3. 修复 Hashtags 首尾空格问题
        raw_hashtags = raw_item.get("hashtagList", [])
        cleaned_hashtags = [ht.strip() for ht in raw_hashtags if ht]

        # 组装数据
        cleaned_data = {
            "metadata": {
                "post_id": raw_item.get("id"),
                "card_type": raw_item.get("cardType"),
                "publish_time": raw_item.get("date"),
                "url": raw_item.get("webLink"),
                "is_ai_generated": raw_item.get("isCreatedByAI", False)
            },
            "author": {
                "author_id": raw_item.get("squareAuthorId"),
                "username": raw_item.get("username"),
                "author_name": raw_item.get("authorName"),
                "is_verified": raw_item.get("authorVerificationType", 0) > 0
            },
            "content": {
                "title": raw_item.get("title"),
                "text_content": text_content,
                "hashtags": cleaned_hashtags,
                "mentioned_coins": cleaned_coins
            },
            "media": {
                "images": raw_item.get("images", [])
            },
            "engagement": {
                "view_count": raw_item.get("viewCount", 0),
                "like_count": raw_item.get("likeCount", 0),
                "comment_count": raw_item.get("commentCount", 0),
                "share_count": raw_item.get("shareCount", 0),
                "quote_count": raw_item.get("quoteCount", 0)
            }
        }

        cleaned_list.append(cleaned_data)

    return cleaned_list


def clean_reply_posts(raw_data_list):
    """
    专门清洗 BUZZ_REPLY_POST_LIST (串烧/盖楼帖) 类型的数据
    :param raw_data_list: 原始数据列表 (List[Dict])
    :return: 解包并清洗后的独立帖子列表 (List[Dict])
    """
    cleaned_list = []

    for raw_container in raw_data_list:
        # 确保这是我们要处理的盖楼帖类型
        if raw_container.get("cardType") != "BUZZ_REPLY_POST_LIST":
            continue

        # 提取盖楼帖里面的所有真实子帖子
        reply_post_list = raw_container.get("replyPostList", [])

        for raw_item in reply_post_list:
            # 1. 安全提取正文
            text_content = raw_item.get("content", "").strip() if raw_item.get("content") else ""

            # 2. 修复提到币种 (完全依赖 API 的结构化字段，摒弃正则防脏数据)
            raw_coins = raw_item.get("coinPairList") or []
            cleaned_coins_list = [coin.replace('$', '').strip() for coin in raw_coins if coin]

            # 从 tradingPairsV2 中提取防漏抓
            trading_pairs_v2 = raw_item.get("tradingPairsV2") or []
            for pair in trading_pairs_v2:
                code = pair.get("code")
                if code:
                    cleaned_coins_list.append(code.strip())

            # 合并、去重，并统一转大写
            cleaned_coins = list(set(coin.upper() for coin in cleaned_coins_list if coin))

            # 3. Hashtags 去首尾空格
            raw_hashtags = raw_item.get("hashtagList", [])
            cleaned_hashtags = [ht.strip() for ht in raw_hashtags if ht]

            # 4. 组装数据
            cleaned_data = {
                "metadata": {
                    "post_id": raw_item.get("id"),
                    "card_type": raw_item.get("cardType", "BUZZ_SHORT"),
                    "publish_time": raw_item.get("date"),
                    "url": raw_item.get("webLink"),
                    "is_ai_generated": raw_item.get("isCreatedByAI", False),
                    # 新增这个标记，方便后续分析知道这是不是一条跟帖
                    "is_reply_post": raw_item.get("isReplyPost", False)
                },
                "author": {
                    "author_id": raw_item.get("squareAuthorId"),
                    "username": raw_item.get("username"),
                    "author_name": raw_item.get("authorName"),
                    "is_verified": raw_item.get("authorVerificationType", 0) > 0
                },
                "content": {
                    "title": raw_item.get("title"),
                    "text_content": text_content,
                    "hashtags": cleaned_hashtags,
                    "mentioned_coins": cleaned_coins
                },
                "media": {
                    "images": raw_item.get("images", [])
                },
                "engagement": {
                    "view_count": raw_item.get("viewCount", 0),
                    "like_count": raw_item.get("likeCount", 0),
                    "comment_count": raw_item.get("commentCount", 0),
                    "share_count": raw_item.get("shareCount", 0),
                    "quote_count": raw_item.get("quoteCount", 0)
                }
            }

            # 将解包后的单条清洗数据放入结果列表中
            cleaned_list.append(cleaned_data)

    return cleaned_list


def clean_video_posts(raw_data_list):
    """
    专门清洗 BUZZ_VIDEO (视频帖) 类型的数据
    :param raw_data_list: 原始数据列表 (List[Dict])
    :return: 清洗后的新数据列表 (List[Dict])
    """
    cleaned_list = []

    for raw_item in raw_data_list:
        # 如果你想做混合处理，可以加这个判断：if raw_item.get("cardType") != "BUZZ_VIDEO": continue

        # 1. 安全提取正文和标题
        title = raw_item.get("title", "").strip() if raw_item.get("title") else None
        text_content = raw_item.get("content", "").strip() if raw_item.get("content") else ""

        # 2. 修复提到币种 (完全依赖 API 的结构化字段，摒弃正则防脏数据)
        raw_coins = raw_item.get("coinPairList") or []
        cleaned_coins_list = [coin.replace('$', '').strip() for coin in raw_coins if coin]

        # 从 tradingPairsV2 中提取防漏抓
        trading_pairs_v2 = raw_item.get("tradingPairsV2") or []
        for pair in trading_pairs_v2:
            code = pair.get("code")
            if code:
                cleaned_coins_list.append(code.strip())

        # 合并、去重，并统一转大写
        cleaned_coins = list(set(coin.upper() for coin in cleaned_coins_list if coin))

        # 3. Hashtags 去首尾空格
        raw_hashtags = raw_item.get("hashtagList", [])
        cleaned_hashtags = [ht.strip() for ht in raw_hashtags if ht]

        # 4. 重点：提取视频专属资源 (直链、封面、时长)
        video_info = None
        # 兼容两种取值路径：顶层 videoLink 或 嵌套在 videoVO 里
        video_link = raw_item.get("videoLink") or raw_item.get("videoVO", {}).get("videoLink")

        if video_link:
            video_info = {
                "video_url": video_link,
                "cover_image_url": raw_item.get("coverLight") or raw_item.get("coverMeta", {}).get("url"),
                "duration_seconds": raw_item.get("videoTimeSeconds") or raw_item.get("videoVO", {}).get(
                    "videoTimeSeconds", 0)
            }

        # 5. 组装最终数据
        cleaned_data = {
            "metadata": {
                "post_id": raw_item.get("id"),
                "card_type": raw_item.get("cardType", "BUZZ_VIDEO"),
                "publish_time": raw_item.get("date"),
                "url": raw_item.get("webLink"),
                "is_ai_generated": raw_item.get("isCreatedByAI", False)
            },
            "author": {
                "author_id": raw_item.get("squareAuthorId"),
                "username": raw_item.get("username"),
                "author_name": raw_item.get("authorName"),
                "is_verified": raw_item.get("authorVerificationType", 0) > 0
            },
            "content": {
                "title": title,
                "text_content": text_content,
                "hashtags": cleaned_hashtags,
                "mentioned_coins": cleaned_coins
            },
            "media": {
                "images": raw_item.get("images", []),  # 视频帖的纯图片通常为空，保留格式统一
                "video": video_info  # 视频专属属性
            },
            "engagement": {
                "view_count": raw_item.get("viewCount", 0),
                "like_count": raw_item.get("likeCount", 0),
                "comment_count": raw_item.get("commentCount", 0),
                "share_count": raw_item.get("shareCount", 0),
                "quote_count": raw_item.get("quoteCount", 0)
            }
        }

        cleaned_list.append(cleaned_data)

    return cleaned_list


def clean_long_posts(raw_data_list):
    """
    无损清洗 BUZZ_LONG (长文帖) 类型的数据
    忠实保留原始文本，精细化分类图片属性。
    """
    cleaned_list = []

    for raw_item in raw_data_list:
        if raw_item.get("cardType") != "BUZZ_LONG":
            continue

        # 1. 忠实提取标题和正文（不干预内容）
        title = raw_item.get("title")
        # 优先取 content，如果为空则取 subTitle
        text_content = raw_item.get("content") or raw_item.get("subTitle") or ""

        # 2. 修复提到币种 (完全依赖 API 的结构化字段，摒弃正则防脏数据)
        raw_coins = raw_item.get("coinPairList") or []
        cleaned_coins_list = [coin.replace('$', '').strip() for coin in raw_coins if coin]

        # 从 tradingPairsV2 中提取防漏抓
        trading_pairs_v2 = raw_item.get("tradingPairsV2") or []
        for pair in trading_pairs_v2:
            code = pair.get("code")
            if code:
                cleaned_coins_list.append(code.strip())

        # 合并、去重，并统一转大写
        cleaned_coins = list(set(coin.upper() for coin in cleaned_coins_list if coin))

        # 3. 忠实合并标签 (去首尾空格防脏数据)
        raw_tags_1 = raw_item.get("hashtagList") or []
        raw_tags_2 = raw_item.get("hashtagIdentifyList") or []
        cleaned_hashtags = list(set(ht.strip() for ht in (raw_tags_1 + raw_tags_2) if ht))

        # 4. 🚀 重点：精细化分离图片属性 (封面图 vs 列表内嵌图)
        cover_image = None
        cover_meta = raw_item.get("coverMeta")
        # 提取封面
        if cover_meta and isinstance(cover_meta, dict):
            cover_image = cover_meta.get("url")
        elif raw_item.get("coverLight"):
            cover_image = raw_item.get("coverLight")

        # 提取正文内嵌图 (保留有序列表，以此体现插入的相对先后顺序)
        inline_images = raw_item.get("images") or []

        # 5. 组装数据
        cleaned_data = {
            "metadata": {
                "post_id": raw_item.get("id"),
                "card_type": raw_item.get("cardType", "BUZZ_LONG"),
                "publish_time": raw_item.get("date"),
                "url": raw_item.get("webLink"),
                "is_ai_generated": raw_item.get("isCreatedByAI", False)
            },
            "author": {
                "author_id": raw_item.get("squareAuthorId"),
                "username": raw_item.get("username"),
                "author_name": raw_item.get("authorName"),
                "is_verified": raw_item.get("authorVerificationType", 0) > 0
            },
            "content": {
                "title": title,
                "text_content": text_content,
                "hashtags": cleaned_hashtags,
                "mentioned_coins": cleaned_coins
            },
            "media": {
                "cover_image": cover_image,  # 明确标注这是封面图
                "inline_images": inline_images  # 明确标注这是文章中包含的图(按顺序)
            },
            "engagement": {
                "view_count": raw_item.get("viewCount", 0),
                "like_count": raw_item.get("likeCount", 0),
                "comment_count": raw_item.get("commentCount", 0),
                "share_count": raw_item.get("shareCount", 0),
                "quote_count": raw_item.get("quoteCount", 0)
            }
        }

        cleaned_list.append(cleaned_data)

    return cleaned_list

def clean_long_posts_detail(raw_data_list):
    """
    专门针对带有 `body` 富文本结构的数据进行核心内容与媒体解析。

    :param raw_data_list: 包含 raw_item 的列表
    :return: 仅包含 text_content 和 media 的全新列表
    """
    cleaned_list = []

    for raw_item in raw_data_list:
        body_str = raw_item.get("body")

        text_content = ""
        inline_images = []

        # 1. 核心解析逻辑：深度解构 body 富文本
        if body_str:
            try:
                body_data = json.loads(body_str)
                # 获取渲染顺序
                order_list = body_data.get("layout", {}).get("ViewInstance0", [])
                # 🚀 致命 Bug 修复点：所有的具体节点数据其实都藏在 hash 这个字典里！
                hash_dict = body_data.get("hash", {})

                full_text_parts = []

                def extract_node(node):
                    """递归解析单个富文本节点"""
                    if not isinstance(node, dict):
                        return ""

                    node_id = node.get("id")
                    config = node.get("config", {})

                    # A. 提取图片节点，将其转换为带位置信息的文本标记
                    if node_id == "RichTextImage":
                        src = config.get("src")
                        caption = config.get("caption", "")
                        if src:
                            inline_images.append(src)
                            # 生成供大模型阅读的标记
                            img_mark = f"\n\n[插图: {src}"
                            if caption:
                                img_mark += f" | 描述: {caption}"
                            img_mark += "]\n\n"
                            return img_mark
                        return ""

                    # B. 提取基础文本与标签
                    if node_id in ("RichTextText", "RichTextHashTag", "RichTextCoinPair"):
                        return config.get("content", "")

                    # C. 处理硬换行
                    if node_id == "RichTextHardBreak":
                        return "\n"

                    # D. 递归处理容器节点 (段落、列表、引用等)
                    content_list = config.get("content", [])
                    if not isinstance(content_list, list):
                        content_list = []

                    child_texts = [extract_node(child) for child in content_list]
                    res = "".join(child_texts)

                    # E. 列表项增加项目符号
                    if node_id == "RichTextListItem":
                        res = "• " + res + "\n"

                    return res

                # 按 layout 指定的顺序拼装全部分块
                for uid in order_list:
                    # 🚀 致命 Bug 修复点：从 hash_dict 里根据 uid 取出块内容
                    block = hash_dict.get(uid, {})

                    # 空行处理
                    if block.get("empty"):
                        full_text_parts.append("\n")
                        continue

                    block_text = extract_node(block)
                    if block_text:
                        # 对于段落、引用等块级元素，追加换行
                        if block.get("id") in ("RichTextParagraph", "RichTextQuote", "RichTextHeader", "RichTextList"):
                            full_text_parts.append(block_text + "\n")
                        else:
                            full_text_parts.append(block_text)

                # 合并文本并清理多余的连续空白行（保留最多两个换行符）
                raw_parsed_text = "".join(full_text_parts)
                text_content = re.sub(r'\n{3,}', '\n\n', raw_parsed_text).strip()

            except json.JSONDecodeError:
                # 容错：如果 JSON 解析失败，尝试使用外层的备用字段
                text_content = raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle") or ""
        else:
            # 兜底：如果完全没有 body 字段，按优先级获取纯文本
            text_content = raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle") or ""

        # 2. 提取封面图 (Cover)
        cover_image = None
        # 兼容不同层级的封面图数据结构
        cover_meta = raw_item.get("coverMeta")
        if cover_meta and isinstance(cover_meta, dict):
            cover_image = cover_meta.get("url")
        elif raw_item.get("cover"):
            cover_image = raw_item.get("cover")

        # 3. 组装极致纯净的数据结构
        cleaned_data = {
            "core_text_content": text_content,
            "media": {
                "cover_image": cover_image,
                "inline_images": inline_images
            }
        }

        cleaned_list.append(cleaned_data)

    return cleaned_list

def pull_feed_demo():
    # 拉取帖子数据
    master_feed_list = []

    logger.info("========== 🚀 开始全量数据抓取测试 ==========")

    # # 1. 抓取推荐流 (100条)
    # logger.info("\n--- 1. 准备抓取: 推荐流 ---")
    # recommend_data = fetch_binance_feed(count=500)
    # master_feed_list.extend(recommend_data)
    #
    # # 2. 抓取搜索流 (以 "doge" 为例，100条)
    # logger.info("\n--- 2. 准备抓取: 搜索流 (ETH) ---")
    # search_data = fetch_binance_feed(keyword="ETH", count=500)
    # master_feed_list.extend(search_data)
    #
    # # 3. 抓取特定币种流 (以 "BTC" 为例，100条，按热门排序)
    # logger.info("\n--- 3. 准备抓取: 币种流 (BTC) ---")
    # token_data = fetch_binance_feed(token="BTC", count=500, orderBy=1)
    # master_feed_list.extend(token_data)

    master_feed_list = read_json("master_feed_list.json")
    save_json("master_feed_list.json", master_feed_list)
    # 4. 全局终极去重（防止不同信息流之间的数据交叉重叠）
    global_seen_ids = set()
    final_clean_list = []

    for item in master_feed_list:
        item_id = item.get("id") or item.get("contentId")
        if item_id and item_id not in global_seen_ids:
            global_seen_ids.add(item_id)
            final_clean_list.append(item)

    logger.info(f"✨ 全局去重完成: 发现并移除跨流重复数据 {len(master_feed_list) - len(final_clean_list)} 条。")
    logger.info(f"🎉 最终可用的纯净大列表总数据量: {len(final_clean_list)} 条记录！")

    # 5. 按照 cardType 分组，每种类型最多保留 5 条数据
    logger.info("\n--- 5. 开始按 cardType 分组提取 (每种最多5条) ---")
    group_map = {}

    filter_list = []
    card_type_counts = {}  # 用字典来记录每种类型的数量

    for item in final_clean_list:
        # 获取 cardType，缺省为 "UNKNOWN" 防报错
        card_type = item.get("cardType", "UNKNOWN")
        # if card_type == "SPACE_LIVE" or card_type == "POLL":
        #     continue
        # 获取当前类型已经存入 filter_list 的数量，默认为 0
        current_count = card_type_counts.get(card_type, 0)

        # 核心判断：如果该类型保存的数据还不到 5 条，就继续保留
        if current_count < 10:
            filter_list.append(item)
            # 计数器加 1
            card_type_counts[card_type] = current_count + 1
            if card_type not in group_map:
                group_map[card_type] = []
            group_map[card_type].append(item)

    # 打印最终的分组结果
    logger.info(f"🗂️ 分组提取完成！共发现 {len(card_type_counts)} 种不同的 cardType。")
    logger.info(f"🎉 最终的 filter_list 包含 {len(filter_list)} 条记录！")

    # 直观地打印出每种类型具体保留了多少条，方便你核对数据分布
    logger.info("📊 各类型保留数量统计:")
    for c_type, count in card_type_counts.items():
        logger.info(f"   - {c_type}: {count} 条")

    clean_filter_list = clean_universal_binance_data(filter_list)
    all_clean_list = []
    short_result_list = clean_short_posts(group_map['BUZZ_SHORT'])
    all_clean_list.extend(short_result_list)
    replay_result_list = clean_reply_posts(group_map['BUZZ_REPLY_POST_LIST'])
    all_clean_list.extend(replay_result_list)
    video_result_list = clean_video_posts(group_map['BUZZ_VIDEO'])
    all_clean_list.extend(video_result_list)
    long_result_list = clean_long_posts(group_map['BUZZ_LONG'])
    all_clean_list.extend(long_result_list)

    long_result_list = clean_long_posts(group_map['BUZZ_LONG'])

    # BUZZ_LONG类型的帖子要拉取详细的内容才是完整的内容，在这个基础上进行清洗才有意义
    post_detail_list = []
    for detail in group_map['BUZZ_LONG']:
        post_id = detail.get('id')
        post_detail = fetch_binance_post_detail(post_id)
        if post_detail:
            post_detail_list.append(post_detail)
    long_result_list = clean_long_posts_detail(post_detail_list)


    post_detail_list = read_json("post_detail_list.json")
    long_result_list = clean_long_posts_detail(post_detail_list)

    save_json("post_detail_list.json", post_detail_list)
    print()




if __name__ == "__main__":
    # post_detail = fetch_binance_post_detail('341457719024369')
    pull_feed_demo()

    print()
    #
    # # logger.info("=" * 40)
    # # logger.info("3. 测试: 广场自动化发帖")
    # # logger.info("=" * 40)
    # # YOUR_SQUARE_API_KEY = get_config("myself_square_api_key")
    # # test_text = """$ETH $BTC 祝愿大家发大财 天天开心"""
    #
    # # if YOUR_SQUARE_API_KEY != "替换成你的_API_KEY":
    # #     publish_to_binance_square(api_key=YOUR_SQUARE_API_KEY, text_content=test_text)
    # # else:
    # #     logger.warning("⚠️ 请先在代码中填入你的 API Key 再运行发帖测试！\n")


    # # 替换为你实际抓取到的最新 Cookie (请注意保护个人隐私，不要在公开代码库中泄露)
    # MY_COOKIE = """bnc-uuid=2772e08c-2f51-4f76-a3a7-f8d5700463fb; BNC_FV_KEY=33e0521b114aa99a931a3ed42ca03cf0147220b0; lang=zh-CN; userPreferredCurrency=USD_USD; _gcl_au=1.1.17001513.1774485620; se_sd=AwODgTQ9QRODlRUEMEAYgZZAwVQgMEXWlZcJeW0RVVQWwFVNWVIC1; se_gd=gMaWgRxAOBIDFMTxWAQMgZZEFVhYGBXWlRSJeW0RVVQWwE1NWV4R1; se_gsd=SjUiKz9jJislGSMsNQMxBS4ECVBSBgVRWF5CW1FUVFVWNFNT1; bu_s=default; g_state={"i_l":0,"i_ll":1774485995809,"i_b":"NKbj7qtR4btoqh2wSA9LO71loBb9HeqsXc2dMxItGG0","i_e":{"enable_itp_optimization":0}}; BNC-Location=CN; theme=light; neo-theme=light; _gid=GA1.2.1014400770.1775363722; aws-waf-token=49523bea-cc30-499d-a6b5-7bb90a39c73e:AQoAhq9Ms/ICAAAA:zFrMUI32RGjn21sFH0tvoqKElNySMSYU8MiIuoJAVDtXDx000yMwC3zqBa1Qa7LApGeXibZUCk4DvTVmY03GcodqnyNhbyIoe11vHrdIaiRNOCpNQ8DKQ1iDndciMjQXY3c6ZKP+3vK/S8fg8PImMas8pAkK7GvaG8SkAb1F+RvJ5YbXEOnorSTveSc61n8E3kg=; changeBasisTimeZone=; _uetsid=c9f13b7031ca11f1950ba10f78e7c9e6; _uetvid=5eef827028ac11f18ddb31237e05f437; futures-layout=pro; _h_desk_key=8ea5542d32cc4284879ca8a4fc7ad41a; BNC_FV_KEY_T=101-Ylebao0wIcEz2Uj9MpbnsXPTrZWNWkYZp3hQyb1VE8vcKbwuvXsebklUt%2BlP0cq24ACFZyMmaWab5EPoNZ%2FyLg%3D%3D-KCNuKETgXuNoVNoZxGJBdA%3D%3D-2d; BNC_FV_KEY_EXPIRE=1775580632060; s9r1=9295BE8EC4891D0CFBBF5377C54B9454; r20t=web.D6D0A9FDF865D8BFC6029A44842218F3; r30t=1; cr00=156D95BCEAE710B7D5E8FC3A9885B90E; d1og=web.1229561321.7C28B45C2405D999E4CD78CE8DAD6698; r2o1=web.1229561321.19F0081C45D206B68BE35A511F1AB277; f30l=web.1229561321.72BF426ACE0E086CEB5DE498225372F0; currentAccount=; logined=y; p20t=web.1229561321.4519E52C213DCEFA2E046A8E6753B05E; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221229561321%22%2C%22first_id%22%3A%2219c67f1247fd9f-0944324976c3ab8-26061d51-2359296-19c67f12480b26%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTljNjdmMTI0N2ZkOWYtMDk0NDMyNDk3NmMzYWI4LTI2MDYxZDUxLTIzNTkyOTYtMTljNjdmMTI0ODBiMjYiLCIkaWRlbnRpdHlfbG9naW5faWQiOiIxMjI5NTYxMzIxIn0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%221229561321%22%7D%2C%22%24device_id%22%3A%2219c67f2cd51ef8-07bb7118048b2d8-26061d51-2359296-19c67f2cd52129f%22%7D; OptanonConsent=isGpcEnabled=0&datestamp=Tue+Apr+07+2026+18%3A52%3A41+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202506.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=5029ba14-0415-4560-be72-530391e051ed&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&AwaitingReconsent=false; _ga=GA1.1.1913380912.1772289141; _ga_3WP50LGEEC=GS2.1.s1775559031$o31$g1$t1775559167$j6$l0$h0"""
    # TARGET_UID = "CfexsWwIVYYbr1N5GJXlVQ"
    #
    # # 执行关注
    # result = follow_binance_square_user(
    #     target_uid=TARGET_UID,
    #     cookie_str=MY_COOKIE
    #     # 如果报错风控拦截，你需要在这里手动传入最新的 csrf_token 和 fvideo_token
    # )
    #
    # logger.info(f"执行关注最终结果: {json.dumps(result, ensure_ascii=False)}")
