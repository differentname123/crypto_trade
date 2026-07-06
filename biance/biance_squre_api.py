# -*- coding: utf-8 -*-
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
import random
import re
import time
import uuid

import requests

from common.common_utils import get_config, setup_logger, save_json, read_json

setup_logger()

# 拿到属于当前文件的专属 logger
logger = logging.getLogger(__name__)

# ============================================================
# 全局常量：集中管理代理、分页、重试策略与正则，避免散落重复
# ============================================================
PROXIES = {
    "http": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443",
    "https": "https://YOUR_USER:YOUR_PASS@proxy.easyeverything.top:443"
}
PAGE_SIZE = 20
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15

# 模块级预编译正则（性能优化：避免逐字段/逐条重复编译）
_INVISIBLE_RE = re.compile(r'[\u200b-\u200f\ufeff\u202a-\u202e]')  # 零宽字符、排版控制符等
_MULTI_NEWLINE_RE = re.compile(r'\n{3,}')


def pull_feed_demo():
    # 拉取帖子数据
    logger.info("========== 🚀 开始全量数据抓取测试 ==========")

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

    # 5. 按照 cardType 分组，每种类型最多保留 20 条数据
    logger.info("\n--- 5. 开始按 cardType 分组提取 (每种最多20条) ---")
    group_map = {}
    filter_list = []
    card_type_counts = {}  # 用字典来记录每种类型的数量

    for item in final_clean_list:
        # 获取 cardType，缺省为 "UNKNOWN" 防报错
        card_type = item.get("cardType", "UNKNOWN")

        # 获取当前类型已经存入 filter_list 的数量，默认为 0
        current_count = card_type_counts.get(card_type, 0)

        # 核心判断：如果该类型保存的数据还不到 20 条，就继续保留
        if current_count < 20:
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

    temp_all_result_list = clean_universal_posts(filter_list)
    final_temp_all_result_list = update_posts_in_place(temp_all_result_list)


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

    # 隐藏掉中间的 API Key 字符，避免在控制台全量打印（官方安全规范）
    masked_key = f"{api_key[:5]}...{api_key[-4:]}" if len(api_key) > 10 else "***"
    # 提取内容摘要，防止内容过长刷屏
    text_summary = text_content[:20].replace('\n', ' ') + ("..." if len(text_content) > 20 else "")

    try:
        logger.info(f"⏳ 开始向币安广场发帖 | Key: {masked_key} | 内容摘要: {text_summary}")

        # 使用全局 proxies 和 timeout 配置
        response = requests.post(url, headers=headers, json=payload, proxies=PROXIES, timeout=REQUEST_TIMEOUT)
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

    try:
        logger.info(f"⏳ 开始关注广场用户 | 目标UID: {target_uid} | TraceID: {trace_id}")

        # 使用全局代理配置
        response = requests.post(url, headers=headers, json=payload, proxies=PROXIES,
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


def get_standard_schema():
    """
    统一的数据骨架 (Schema Template)
    确保所有类型的帖子拥有绝对一致的字段结构，防止后续入库或分析时出现 KeyError
    """
    return {
        "metadata": {
            "post_id": None,
            "card_type": "UNKNOWN",
            "publish_time": 0,
            "url": None,
            "is_ai_generated": False
        },
        "author": {
            "author_id": None,
            "username": None,
            "author_name": None,
            "is_verified": False
        },
        "content": {
            "title": None,
            "text_content": "",
            "hashtags": [],
            "mentioned_coins": []
        },
        "media": {
            "cover_image": None,  # 视频或长文的封面
            "images": [],  # 普通帖子的图片列表
            "inline_images": [],  # 长文内嵌的图片列表 (保留阅读顺序)
            "video_url": None,  # 视频直链
            "video_duration": 0  # 视频时长
        },
        "engagement": {
            "view_count": 0,
            "like_count": 0,
            "comment_count": 0,
            "share_count": 0,
            "quote_count": 0
        }
    }


# ============================================================
# 通用原子工具 (跨清洗器复用，保持扁平不深套)
# ============================================================
def _clean_text(raw):
    """清理不可见字符并去首尾空格；空值安全返回空串"""
    if not raw:
        return ""
    return _INVISIBLE_RE.sub('', raw).strip()


def _clean_title(raw):
    """标题清理：空值返回 None，否则清理不可见字符并去空格"""
    if not raw:
        return None
    return _INVISIBLE_RE.sub('', raw).strip()


def _extract_coins(raw_item):
    """
    提取提到的币种：完全依赖 API 结构化字段（摒弃正则防脏数据）
    来源1: coinPairList (去 $ 符号)
    来源2: tradingPairsV2[].code (防漏抓)
    最终合并、去重、统一大写
    """
    coins = [coin.replace('$', '').strip() for coin in (raw_item.get("coinPairList") or []) if coin]

    for pair in (raw_item.get("tradingPairsV2") or []):
        code = pair.get("code")
        if code:
            coins.append(code.strip())

    return list(set(coin.upper() for coin in coins if coin))


def _extract_hashtags(raw_item, merge_keys=None):
    """
    提取并清理 Hashtags（去首尾空格防脏数据）
    默认仅取 hashtagList；传入 merge_keys 时额外合并并去重（长文场景）
    """
    tags = list(raw_item.get("hashtagList") or [])

    if merge_keys:
        for key in merge_keys:
            tags += (raw_item.get(key) or [])
        return list(set(ht.strip() for ht in tags if ht))

    return [ht.strip() for ht in tags if ht]


def _assemble_common(raw_item, card_type_default, title, text_content, hashtags):
    """
    套用统一 Schema，组装所有类型共有的四大区块
    (metadata / author / content / engagement)
    差异化的 media 由各清洗器在返回后自行填充
    """
    data = get_standard_schema()

    data["metadata"].update({
        "post_id": raw_item.get("id"),
        "card_type": raw_item.get("cardType", card_type_default),
        "publish_time": raw_item.get("date"),
        "url": raw_item.get("webLink"),
        "is_ai_generated": raw_item.get("isCreatedByAI", False)
    })

    data["author"].update({
        "author_id": raw_item.get("squareAuthorId"),
        "username": raw_item.get("username"),
        "author_name": raw_item.get("authorName"),
        "is_verified": raw_item.get("authorVerificationType", 0) > 0
    })

    data["content"].update({
        "title": title,
        "text_content": text_content,
        "hashtags": hashtags,
        "mentioned_coins": _extract_coins(raw_item)
    })

    data["engagement"].update({
        "view_count": raw_item.get("viewCount", 0),
        "like_count": raw_item.get("likeCount", 0),
        "comment_count": raw_item.get("commentCount", 0),
        "share_count": raw_item.get("shareCount", 0),
        "quote_count": raw_item.get("quoteCount", 0)
    })

    return data


# ============================================================
# 各类型专用清洗器 (仅保留差异化的 media 处理)
# ============================================================
def clean_short_posts(raw_data_list):
    """清洗 BUZZ_SHORT (短帖) 类型数据"""
    cleaned_list = []
    failed = 0

    for raw_item in raw_data_list:
        try:
            data = _assemble_common(
                raw_item,
                card_type_default=None,
                title=_clean_title(raw_item.get("title")),
                text_content=_clean_text(raw_item.get("content")),
                hashtags=_extract_hashtags(raw_item)
            )
            data["media"]["images"] = raw_item.get("images", [])
            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_SHORT] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    logger.info(f"[BUZZ_SHORT] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_reply_posts(raw_data_list):
    """
    清洗 BUZZ_REPLY_POST_LIST (串烧/盖楼帖) 类型数据
    需先剥离容器，再根据内部真实子帖的类型，动态分发给对应的专用清洗器，防止多模态数据丢失
    """
    cleaned_list = []
    container_failed = 0

    # 建立局部路由字典，复用已有的各类型专用清洗器
    # 这样子帖中的视频、长文就不会丢失独有的媒体提取逻辑
    processor_map = {
        'BUZZ_SHORT': clean_short_posts,
        'BUZZ_VIDEO': clean_video_posts,
        'BUZZ_LONG': clean_long_posts
    }

    for raw_container in raw_data_list:
        try:
            # 仅处理盖楼帖容器
            if raw_container.get("cardType") != "BUZZ_REPLY_POST_LIST":
                continue

            # 获取容器内包裹的子帖列表
            reply_post_list = raw_container.get("replyPostList", [])

            # 将当前容器内的子帖按 cardType 进行分组
            child_group_map = {}
            for raw_item in reply_post_list:
                # 若子帖无类型，兜底降级为短帖处理
                c_type = raw_item.get("cardType", "BUZZ_SHORT")
                child_group_map.setdefault(c_type, []).append(raw_item)

            # 分发给对应的专属清洗器处理
            for c_type, items in child_group_map.items():
                # 如果遇到未知类型，同样使用短帖清洗器兜底
                processor_func = processor_map.get(c_type, clean_short_posts)
                child_cleaned_list = processor_func(items)

                if child_cleaned_list:
                    cleaned_list.extend(child_cleaned_list)

        except Exception as e:
            container_failed += 1
            logger.error(f"[BUZZ_REPLY] 容器解析失败 | container_id={raw_container.get('id')} | error={e}",
                         exc_info=True)

    # 注：子帖清洗的失败日志会由专属清洗器自行打印，这里只统计最终产出和容器解析失败数
    logger.info(
        f"[BUZZ_REPLY] 容器解析及子帖清洗完成 | 共提取有效子帖={len(cleaned_list)} 条 | 容器失败={container_failed}")
    return cleaned_list


def clean_video_posts(raw_data_list):
    """清洗 BUZZ_VIDEO (视频帖) 类型数据，平铺视频专属资源到 media"""
    cleaned_list = []
    failed = 0

    for raw_item in raw_data_list:
        try:
            data = _assemble_common(
                raw_item,
                card_type_default="BUZZ_VIDEO",
                title=_clean_title(raw_item.get("title")),
                text_content=_clean_text(raw_item.get("content")),
                hashtags=_extract_hashtags(raw_item)
            )

            data["media"]["images"] = raw_item.get("images", [])

            # 修复原潜在 Bug：videoVO / coverMeta 存在但值为 None 时的 AttributeError
            video_vo = raw_item.get("videoVO") or {}
            video_link = raw_item.get("videoLink") or video_vo.get("videoLink")
            if video_link:
                cover_meta = raw_item.get("coverMeta") or {}
                data["media"]["video_url"] = video_link
                data["media"]["cover_image"] = raw_item.get("coverLight") or cover_meta.get("url")
                data["media"]["video_duration"] = raw_item.get("videoTimeSeconds") or video_vo.get("videoTimeSeconds",
                                                                                                   0)

            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_VIDEO] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    logger.info(f"[BUZZ_VIDEO] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_long_posts(raw_data_list):
    """
    无损清洗 BUZZ_LONG (长文帖) 类型数据
    忠实保留原始文本，精细化分离封面图与内嵌图
    """
    cleaned_list = []
    failed = 0

    for raw_item in raw_data_list:
        try:
            if raw_item.get("cardType") != "BUZZ_LONG":
                continue

            data = _assemble_common(
                raw_item,
                card_type_default="BUZZ_LONG",
                title=_clean_title(raw_item.get("title")),
                text_content=_clean_text(raw_item.get("content") or raw_item.get("subTitle")),
                hashtags=_extract_hashtags(raw_item, merge_keys=["hashtagIdentifyList"])
            )

            # 精细化分离封面图 (coverMeta.url 优先，其次 coverLight)
            cover_image = None
            cover_meta = raw_item.get("coverMeta")
            if cover_meta and isinstance(cover_meta, dict):
                cover_image = cover_meta.get("url")
            elif raw_item.get("coverLight"):
                cover_image = raw_item.get("coverLight")

            data["media"]["cover_image"] = cover_image
            data["media"]["inline_images"] = raw_item.get("images") or []

            cleaned_list.append(data)
        except Exception as e:
            failed += 1
            logger.error(f"[BUZZ_LONG] 帖子清洗失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    logger.info(f"[BUZZ_LONG] 清洗完成 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


def clean_long_posts_detail(raw_data_list):
    """
    针对带有 `body` 富文本结构的长文详情，做核心内容与内嵌媒体的深度解析
    解析失败时优雅降级到 bodyTextOnly / content / subTitle
    """
    cleaned_list = []
    failed = 0

    for raw_item in raw_data_list:
        try:
            body_str = raw_item.get("body")
            text_content = ""
            inline_images = []

            if body_str:
                try:
                    body_data = json.loads(body_str)
                    order_list = body_data.get("layout", {}).get("ViewInstance0", [])
                    hash_dict = body_data.get("hash", {})
                    full_text_parts = []

                    def extract_node(node):
                        """递归解析单个富文本节点"""
                        if not isinstance(node, dict):
                            return ""

                        node_id = node.get("id")
                        config = node.get("config", {})

                        # 图片节点：记录 src 并生成阅读标记
                        if node_id == "RichTextImage":
                            src = config.get("src")
                            if not src:
                                return ""
                            inline_images.append(src)
                            caption = config.get("caption", "")
                            img_mark = f"\n\n[插图: {src}"
                            if caption:
                                img_mark += f" | 描述: {caption}"
                            return img_mark + "]\n\n"

                        # 叶子文本节点
                        if node_id in ("RichTextText", "RichTextHashTag", "RichTextCoinPair"):
                            return config.get("content", "")

                        if node_id == "RichTextHardBreak":
                            return "\n"

                        # 容器节点：递归拼接子节点
                        content_list = config.get("content", [])
                        if not isinstance(content_list, list):
                            content_list = []
                        res = "".join(extract_node(child) for child in content_list)

                        if node_id == "RichTextListItem":
                            res = "• " + res + "\n"

                        return res

                    for uid in order_list:
                        block = hash_dict.get(uid, {})
                        if block.get("empty"):
                            full_text_parts.append("\n")
                            continue

                        block_text = extract_node(block)
                        if not block_text:
                            continue

                        if block.get("id") in ("RichTextParagraph", "RichTextQuote", "RichTextHeader", "RichTextList"):
                            full_text_parts.append(block_text + "\n")
                        else:
                            full_text_parts.append(block_text)

                    raw_parsed_text = "".join(full_text_parts)
                    raw_text_content = _MULTI_NEWLINE_RE.sub('\n\n', raw_parsed_text).strip()
                    # 清理富文本提取出的不可见字符（此处不能再 strip 破坏内部排版，故单独处理）
                    text_content = _INVISIBLE_RE.sub('', raw_text_content)

                except json.JSONDecodeError:
                    text_content = _clean_text(
                        raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle"))
            else:
                text_content = _clean_text(
                    raw_item.get("bodyTextOnly") or raw_item.get("content") or raw_item.get("subTitle"))

            # 提取封面图 (coverMeta.url 优先，其次 cover)
            cover_image = None
            cover_meta = raw_item.get("coverMeta")
            if cover_meta and isinstance(cover_meta, dict):
                cover_image = cover_meta.get("url")
            elif raw_item.get("cover"):
                cover_image = raw_item.get("cover")

            cleaned_list.append({
                "core_text_content": text_content,
                "media": {
                    "cover_image": cover_image,
                    "inline_images": inline_images
                }
            })

        except Exception as e:
            failed += 1
            logger.error(f"[LONG_DETAIL] 长文详情解析失败 | post_id={raw_item.get('id')} | error={e}", exc_info=True)

    if failed:
        logger.warning(f"[LONG_DETAIL] 详情解析存在失败 | 成功={len(cleaned_list)} 失败={failed}")
    return cleaned_list


# ============================================================
# 主流程编排
# ============================================================
def clean_universal_posts(final_clean_list):
    """
    通用清洗调度：
    1. 按 post_id 去重（节省算力）
    2. 按 cardType 分组统计
    3. 分发到各专用清洗器
    4. 合并结果返回
    """
    card_type_counts = {}
    group_map = {}
    seen_post_ids = set()
    unique_count = 0

    # ---------- 1. 去重 + 分组统计 ----------
    for item in final_clean_list:
        post_id = item.get("id")
        if post_id:
            if post_id in seen_post_ids:
                continue
            seen_post_ids.add(post_id)

        unique_count += 1
        card_type = item.get("cardType", "UNKNOWN")
        card_type_counts[card_type] = card_type_counts.get(card_type, 0) + 1
        group_map.setdefault(card_type, []).append(item)

    # 高密度单条日志：一次性呈现去重结果、种类数与完整分布，建立清晰排查上下文
    logger.info(
        f"🗂️ 分组去重完成 | 唯一记录={unique_count} | cardType种类={len(card_type_counts)} | 分布={card_type_counts}")

    # ---------- 2. 分发处理与合并 ----------
    all_clean_list = []
    processor_map = {
        'BUZZ_SHORT': clean_short_posts,
        'BUZZ_REPLY_POST_LIST': clean_reply_posts,
        'BUZZ_VIDEO': clean_video_posts,
        'BUZZ_LONG': clean_long_posts
    }

    for card_type, processor_func in processor_map.items():
        if card_type in group_map:
            result_list = processor_func(group_map[card_type])
            if result_list:
                all_clean_list.extend(result_list)

    logger.info(f"🎉 通用清洗合并完毕 | 最终产出={len(all_clean_list)} 条")
    return all_clean_list


def update_posts_in_place(final_clean_list):
    """
    就地更新(In-place)帖子内容：
    - 长文：回源抓取详情、覆盖正文、补齐封面标记与内嵌图
    - 非长文：将图片/视频资源以标记形式融合进正文尾部，供大模型阅读
    """
    if not final_clean_list:
        return []

    long_updated = 0
    other_updated = 0
    failed = 0

    for item in final_clean_list:
        post_id = item.get('metadata', {}).get('post_id')
        try:
            card_type = item.get('metadata', {}).get('card_type')
            if not post_id:
                continue

            # ---------- 1. 长文：回源深度处理 ----------
            if card_type == 'BUZZ_LONG':
                post_detail = fetch_binance_post_detail(post_id)
                if not post_detail:
                    continue

                cleaned_details = clean_long_posts_detail([post_detail])
                if not cleaned_details:
                    continue

                detail_item = cleaned_details[0]
                core_text = detail_item.get('core_text_content', '')

                # 补齐长文封面标记供大模型阅读
                cover_image = detail_item.get('media', {}).get('cover_image')
                if cover_image:
                    cover_mark = f"[长文封面: {cover_image}]"
                    if cover_mark not in core_text:
                        core_text = f"{cover_mark}\n\n{core_text}" if core_text else cover_mark

                if core_text:
                    item['content']['text_content'] = core_text

                item['media']['cover_image'] = cover_image
                item['media']['inline_images'] = detail_item.get('media', {}).get('inline_images', [])

                long_updated += 1

            # ---------- 2. 非长文：多模态标记融合 ----------
            else:
                media_info = item.get('media', {})
                media_marks = []

                # A. 普通图片
                for img_url in media_info.get('images', []):
                    if img_url:
                        media_marks.append(f"[插图: {img_url}]")

                # B. 视频及其封面
                video_url = media_info.get('video_url')
                if video_url:
                    cover_image = media_info.get('cover_image')
                    if cover_image:
                        media_marks.append(f"[视频封面: {cover_image}]")
                    media_marks.append(f"[视频: {video_url}]")

                # C. 拼接到正文尾部
                if media_marks:
                    original_text = item.get('content', {}).get('text_content', '').strip()
                    marks_text = "\n".join(media_marks)
                    if marks_text not in original_text:
                        item['content']['text_content'] = (
                            f"{original_text}\n\n{marks_text}" if original_text else marks_text)

                other_updated += 1

        except Exception as e:
            failed += 1
            logger.error(f"[UPDATE] 就地更新多模态信息失败 | post_id={post_id} | error={e}", exc_info=True)

    logger.info(
        f"✅ 详情与多模态融合完毕 | 长文深度更新={long_updated} 条 | 非长文媒体拼接={other_updated} 条 | 失败={failed} 条")
    return final_clean_list


def _extract_vos(res_data):
    """安全解析币安响应体，兼容 data 为 dict / list / 空 的多种结构"""
    data_field = res_data.get("data")
    if isinstance(data_field, dict):
        return data_field.get("vos") or data_field.get("list") or []
    if isinstance(data_field, list):
        return data_field
    return []


def _dedup_vos(all_vos, label):
    """基于 id / contentId 去重，并打印一条高密度的去重结果日志"""
    seen_ids = set()
    deduped = []
    for item in all_vos:
        item_id = item.get("id") or item.get("contentId")
        if item_id not in seen_ids:
            seen_ids.add(item_id)
            deduped.append(item)

    logger.info(
        f"✅ [{label}] 去重完成 | 抓取 {len(all_vos)} 条 | "
        f"移除重复 {len(all_vos) - len(deduped)} 条 | 最终保留 {len(deduped)} 条"
    )
    return deduped


def _paginate_feed(url, headers, build_payload, required_count, label, on_page=None):
    """
    币安广场分页采集核心引擎（POST 分页流通用）

    :param url:            接口地址
    :param headers:        端点专属请求头
    :param build_payload:  接收 page_index，返回该页 payload 的构造函数
    :param required_count: 目标采集条数
    :param label:          业务标签，用于日志上下文（如 "推荐流"、"搜索:ETH"）
    :param on_page:        每页成功后的回调 (vos)，用于推荐流回填 content_ids 等场景
    :return:               去重后的 vos 列表
    """
    all_vos = []
    page_index = 1
    retry_count = 0

    while len(all_vos) < required_count:
        payload = build_payload(page_index)
        response = None
        try:
            response = requests.post(
                url, headers=headers, json=payload,
                proxies=PROXIES, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()

            vos = _extract_vos(response.json())
            if not vos:
                logger.warning(
                    f"⚠️ [{label}] 第 {page_index} 页无更多数据，采集结束（已获取 {len(all_vos)} 条）"
                )
                break

            all_vos.extend(vos)
            if on_page:
                on_page(vos)

            logger.info(
                f"📥 [{label}] 第 {page_index} 页成功 | "
                f"新增 {len(vos)} 条 | 累计 {len(all_vos)}/{required_count}"
            )

            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.5, 1.5))  # 防风控休眠

        except Exception as e:
            # 聚合式单条错误日志：一行内还原完整排查上下文
            detail = ""
            if response is not None and response.status_code >= 400:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.error(
                f"🚨 [{label}] 第 {page_index} 页请求失败 "
                f"(第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止采集")
                break
            time.sleep(random.uniform(0.5, 1.5))  # 异常退避休眠，避免高频空转轰炸

    return _dedup_vos(all_vos, label)


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

    def build_payload(page_index):
        return {
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "scene": "web-homepage",
            "contentIds": content_ids
        }

    def collect_ids(vos):
        # 推荐流独有：将本页 ID 回填至 content_ids，作为下一页的下滑去重参数
        for item in vos:
            item_id = item.get("id") or item.get("contentId")
            if item_id:
                content_ids.append(str(item_id))

    return _paginate_feed(url, headers, build_payload, required_count, "推荐流", on_page=collect_ids)


def fetch_binance_feed_search(keyword, required_count, search_type=1):
    """
    不断拉取币安广场的搜索结果，直到满足需要的数量或没有更多数据

    :param keyword: 搜索关键词 (例如 "doge")
    :param required_count: 需要拉取的目标数据总数
    :param search_type: 搜索类型，默认为1 (通常代表综合或文章)
    :return: 包含目标搜索结果的列表
    """
    url = "https://www.binance.com/bapi/composite/v2/friendly/pgc/feed/search/list"

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

    def build_payload(page_index):
        return {
            "scene": "web",
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "searchContent": keyword,
            "type": search_type
        }

    return _paginate_feed(url, headers, build_payload, required_count, f"搜索:{keyword}")


def get_binance_feed_token(token="DOGE", required_count=20, orderBy=2):
    """
    获取币安指定币种的社区 Feed 数据

    :param token: 币种名称 (如 "DOGE")
    :param required_count: 期望获取的数据条数，默认为 20
    :param orderBy: 1 代表热门，2 代表最新
    :return: 目标 vos 列表，失败或无数据时返回 []
    """
    url = "https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/trade/list"

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

    logger.info(f"⏳ [币种:{token}] 开始采集 | 目标条数: {required_count} | 排序: {orderBy}")

    def build_payload(page_index):
        return {
            "token": token,
            "pageIndex": page_index,
            "pageSize": PAGE_SIZE,
            "scene": 2,
            "orderBy": orderBy
        }

    return _paginate_feed(url, headers, build_payload, required_count, f"币种:{token}")


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
        feed_list = get_binance_feed_token(token=token, required_count=count, orderBy=order_by)

    # 3. 如果什么目标都没传，默认走“推荐流”
    else:
        logger.info(f"🔄 自动路由: [推荐流模式] | 目标条数: {count}")
        content_ids = kwargs.get("content_ids", [])
        feed_list = fetch_binance_feed_recommend(required_count=count, content_ids=content_ids)
    return feed_list


def fetch_binance_post_detail(post_id):
    """
    根据文章 ID 获取币安广场帖子的详细内容

    :param post_id: 文章/帖子的唯一 ID (例如: 341500227496929)
    :return: 包含详情数据的字典 (通常为解析后的 'data' 字段)，如果失败则返回 None
    """
    url = f"https://www.binance.com/bapi/composite/v3/friendly/pgc/special/content/detail/{post_id}"

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
        "referrer": f"https://www.binance.com/zh-CN/square/post/{post_id}"  # referrer 动态跟随 post_id
    }

    retry_count = 0
    while retry_count < MAX_RETRIES:
        response = None
        try:
            # 详情接口为 GET 请求，无需 payload
            response = requests.get(url, headers=headers, proxies=PROXIES, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            data_field = response.json().get("data")
            if not data_field:
                logger.warning(f"⚠️ [详情:{post_id}] 接口未返回有效数据")
            else:
                logger.info(f"✅ [详情:{post_id}] 获取文章详情成功")

            # 与原逻辑一致：拿到响应即返回（无有效数据时不重试）
            return data_field

        except Exception as e:
            detail = ""
            if response is not None and response.status_code >= 400:
                detail = f" | HTTP {response.status_code} | 服务器返回: {response.text[:500]}"
            retry_count += 1
            logger.error(
                f"🚨 [详情:{post_id}] 请求失败 (第 {retry_count}/{MAX_RETRIES} 次){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [详情:{post_id}] 连续失败达到 {MAX_RETRIES} 次上限，终止")
                break
            time.sleep(random.uniform(0.5, 1.5))  # 失败退避休眠

    return None


if __name__ == "__main__":
    master_feed_list = []

    logger.info("========== 🚀 开始全量数据抓取测试 ==========")

    # 1. 抓取推荐流
    logger.info("--- 1. 准备抓取: 推荐流 ---")
    recommend_data = fetch_binance_feed(count=100)
    master_feed_list.extend(recommend_data)

    # 2. 抓取搜索流 (以 "ETH" 为例)
    logger.info("--- 2. 准备抓取: 搜索流 (ETH) ---")
    search_data = fetch_binance_feed(keyword="ETH", count=100)
    master_feed_list.extend(search_data)

    # 3. 抓取特定币种流 (以 "BTC" 为例，按热门排序)
    logger.info("--- 3. 准备抓取: 币种流 (BTC) ---")
    token_data = fetch_binance_feed(token="BTC", count=100, orderBy=1)
    master_feed_list.extend(token_data)

    logger.info(f"========== 🏁 全量抓取结束 | 汇总总计 {len(master_feed_list)} 条 ==========")

    temp_all_result_list = clean_universal_posts(master_feed_list)
    final_temp_all_result_list = update_posts_in_place(temp_all_result_list)
    print()