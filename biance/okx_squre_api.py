# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/10/17
:description:
    OKX Orbit (欧易广场) 推荐流与评论数据抓取、清洗、多模态富化封装模块
    输出 Schema 与 fetch_binance_feed 保持 100% 兼容
"""
import concurrent.futures
import html
import json
import logging
import random
import re
import time
import uuid
import requests
from requests.adapters import HTTPAdapter

from common.common_utils import setup_logger, save_json, read_json, download_web_media

setup_logger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# ============================================================
# 全局常量：集中管理代理、分页、重试策略与预编译正则
# ============================================================
PROXIES = {
    "http": "http://127.0.0.1:7890",
    "https": "http://127.0.0.1:7890"
}
PAGE_SIZE = 20
MAX_RETRIES = 5
REQUEST_TIMEOUT = 15

# 媒体文件本地保存路径（可按需修改为你的 OKX 专属目录或复用原目录）
MEDIA_SAVE_DIR = r'W:\project\python_project\crypto_trade\biance\media_downloads'

# 模块级预编译正则
_INVISIBLE_RE = re.compile(r'[\u200b-\u200f\ufeff\u202a-\u202e]')
_IMG_PLACEHOLDER_RE = re.compile(r'<!--\s*IMG#\d+\s*-->')  # 清理 OKX 正文中的图片占位符
_MULTI_NEWLINE_RE = re.compile(r'\n{3,}')

# 默认的 OKX Web 端特征（若遇到 Cloudflare 强校验，可在调用时传入最新 cookies / extra_headers）
DEFAULT_OKX_DEVID = "2f579465-c748-4469-9ca9-9e13f0df47dc"
DEFAULT_OKX_SITE_INFO = "=03M3ojI5RXa05WZiwiIMFkQPx0Rfh1SPJiOiUGZvNmIsIiULJiOi42bpdWZyJye"


def _build_okx_headers(
        referer="https://www.okx.com/zh-hans/orbit",
        cookies="",
        extra_headers=None,
        timestamp_ms=None
):
    """
    构造 OKX 接口通用请求头，支持外部传入 timestamp_ms 以保证 URL 参数 t 与请求头时间戳严格一致
    """
    now_ms = str(timestamp_ms) if timestamp_ms else str(int(time.time() * 1000))
    headers = {
        "accept": "application/json",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "devid": DEFAULT_OKX_DEVID,
        "platform": "web",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": referer,
        "sec-ch-ua": '"Chromium";v="154", "Google Chrome";v="154", "Not A(Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/154.0.0.0 Safari/537.36"
        ),
        "x-cdn": "https://www.okx.com",
        "x-id-group": f"{now_ms}0050001-c-{random.randint(10, 99)}",
        "x-locale": "zh_CN",
        "x-request-timestamp": now_ms,
        "x-simulated-trading": "undefined",
        "x-site-info": DEFAULT_OKX_SITE_INFO,
        "x-utc": "8",
        "x-zkdex-env": "0",
    }
    if cookies:
        headers["cookie"] = cookies
    if extra_headers and isinstance(extra_headers, dict):
        headers.update(extra_headers)
    return headers


# ============================================================
# OKX 热门币榜单采集函数 (新增)
# ============================================================
def fetch_okx_hot_coins(
        num=25,
        quote_type="USDT",
        country_filter=1,
        rank=0,
        cookies="",
        extra_headers=None,
        session=None
):
    """
    查询 OKX 最新热门币榜单（仅拉取第一页），返回纯币种符号列表

    :param num:            第一页拉取的数量，默认 25
    :param quote_type:     计价币种类型，默认 "USDT"
    :param country_filter: 国家/地区过滤参数，默认 1
    :param rank:           榜单排序类型参数，默认 0
    :param cookies:        可选 OKX Cookie 字符串
    :param extra_headers:  可选额外请求头字典（如 {"x-fptoken": "...", "x-fptoken-signature": "..."}）
    :param session:        可选外部传入的 requests.Session 对象
    :return:               按热度排序且去重后的币种列表，例如 ["BTC", "ETH", "OKB", ...]
    """
    url = "https://www.okx.com/priapi/v5/rubik/web/public/hot-rank"
    referer = "https://www.okx.com/zh-hans/markets/rankings/spot/hot-crypto"
    label = "OKX热门币榜"

    req_client = session if session else requests
    retry_count = 0

    while retry_count < MAX_RETRIES:
        now_ms = str(int(time.time() * 1000))
        params = {
            "countryFilter": country_filter,
            "rank": rank,
            "type": quote_type,
            "pageNum": 1,
            "num": num,
            "t": now_ms
        }
        headers = _build_okx_headers(
            referer=referer,
            cookies=cookies,
            extra_headers=extra_headers,
            timestamp_ms=now_ms
        )
        response = None

        try:
            response = req_client.get(
                url,
                headers=headers,
                params=params,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            json_resp = response.json()

            if str(json_resp.get("code")) != "0":
                logger.error(
                    f"❌ [{label}] API 业务报错 | code={json_resp.get('code')} | msg={json_resp.get('msg')}"
                )
                return []

            data_obj = json_resp.get("data") or {}
            rank_list = data_obj.get("data") or []

            if not rank_list:
                logger.warning(f"⚠️ [{label}] 返回的榜单列表为空")
                return []

            hot_coins = []
            seen_coins = set()

            for item in rank_list:
                if not isinstance(item, dict):
                    continue
                # 优先取 ccyV2 (如 "BTC")，兜底从 instId (如 "BTC-USDT") 拆分
                coin = (item.get("ccyV2") or "").strip().upper()
                if not coin:
                    inst_id = (item.get("instId") or "").strip().upper()
                    if "-" in inst_id:
                        coin = inst_id.split("-")[0].strip()

                if coin and coin not in seen_coins:
                    seen_coins.add(coin)
                    hot_coins.append(coin)

            logger.info(f"🔥 [{label}] 拉取成功 | 共获取 {len(hot_coins)} 个热门币种: {hot_coins}")
            return hot_coins

        except Exception as e:
            detail = ""
            if response is not None and response.status_code >= 400:
                detail = f" | HTTP {response.status_code} | 返回: {response.text[:300]}"
            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 请求失败 ({retry_count}/{MAX_RETRIES}){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，放弃采集")
                break
            time.sleep(random.uniform(1.0, 2.5))

    return []


def fetch_okx_feed(
        count=20,
        existing_ids=None,
        cookies="",
        extra_headers=None,
        download_media=True,
        initial_cursor="1"
):
    """
    统一的 OKX 广场推荐流数据拉取接口（修复了外部 existing_ids 引用无法原地同步更新的问题）
    """
    # 保留外部传入对象的引用，便于后续原地写回更新
    caller_existing_ref = existing_ids
    normalized_existing = set(str(i) for i in existing_ids) if existing_ids else set()

    task_label = "OKX推荐流"
    logger.info(f"🚀 开始探测: [{task_label}] | 目标净增条数: {count}")

    # 1. 拉取推荐流原始数据（内部已结合 normalized_existing 进行去重）
    raw_feed_list = fetch_okx_feed_recommend(
        required_count=count,
        initial_cursor=initial_cursor,
        cookies=cookies,
        extra_headers=extra_headers,
        existing_ids=normalized_existing
    )

    # 2. 早期拦截护城河二次确认，并同步原地更新外部传入的 existing_ids
    filtered_feed_list = []
    intercepted_count = 0

    for raw_item in raw_feed_list:
        content_data = raw_item.get("contentData") or {}
        item_id = str(content_data.get("contentId") or "")
        if item_id and item_id not in normalized_existing:
            filtered_feed_list.append(raw_item)
            normalized_existing.add(item_id)
            if isinstance(caller_existing_ref, set):
                caller_existing_ref.add(item_id)
            elif isinstance(caller_existing_ref, list):
                caller_existing_ref.append(item_id)
        else:
            intercepted_count += 1

    if not filtered_feed_list:
        logger.info(
            f"🛑 [{task_label}] 探测完毕 | 抓取:{len(raw_feed_list)}条 | "
            f"🛡️历史拦截:{intercepted_count}条 | 净增:0条 -> 跳过后续处理"
        )
        return []

    logger.info(
        f"📥 [{task_label}] 探测完毕 | 抓取:{len(raw_feed_list)}条 | "
        f"🛡️历史拦截:{intercepted_count}条 | 净增:{len(filtered_feed_list)}条新帖"
    )

    # 3. 标准化 Schema 清洗
    clean_feed_list = clean_okx_posts(filtered_feed_list)

    # 4. 多线程并发富化（拼接媒体标签、下载图片、按需拉取评论）
    final_clean_feed_list = update_okx_posts_in_place(
        clean_feed_list,
        cookies=cookies,
        extra_headers=extra_headers,
        download_media=download_media
    )

    return final_clean_feed_list



# ============================================================
# 统一数据骨架 (与 fetch_binance_feed 100% 保持一致)
# ============================================================
def get_standard_schema():
    """
    统一的数据骨架 (Schema Template)
    确保 OKX 与 Binance 帖子拥有绝对一致的字段结构，下游处理零感知切换
    """
    return {
        # --- 第一层核心索引与高频过滤字段 ---
        "post_id": None,
        "publish_time": 0,
        "author_id": None,
        "card_type": "UNKNOWN",
        "source": "okx",  # 标识来源为 okx；若下游强校验 "biance"，可改为 "biance"

        # --- 内层结构化容器 ---
        "metadata": {
            "url": None,
            "is_ai_generated": False
        },
        "author": {
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
            "cover_image": None,
            "images": [],
            "inline_images": [],
            "video_url": None,
            "video_duration": 0,
            "local_mapping": {}
        },
        "engagement": {
            "view_count": 0,
            "like_count": 0,
            "comment_count": 0,
            "share_count": 0,
            "quote_count": 0
        },
        "comments": []
    }


# ============================================================
# 文本与特征清洗原子工具
# ============================================================
def _safe_int(val, default=0):
    """安全转为整型，兼容字符串数字与 None"""
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _clean_text(raw):
    """清理 HTML 转义、OKX 内嵌 HTML 注释占位符、不可见字符与多余空行"""
    if not raw:
        return ""
    text = html.unescape(str(raw))
    # 清理所有 HTML 注释形式的占位符（包含 IMG#0 及其他非图片组件注释占位符）
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    text = _INVISIBLE_RE.sub('', text)
    text = _MULTI_NEWLINE_RE.sub('\n\n', text)
    return text.strip()

def _clean_title(raw):
    """清理标题，空值返回 None"""
    cleaned = _clean_text(raw)
    return cleaned if cleaned else None


def _extract_okx_coins(content_data):
    """
    多维提取 OKX 帖子关联币种：
    1. tagList.cashTagList[].coinName (正文 $ 提及)
    2. tokens[].coinName (系统识别绑定的行情代币)
    3. pluginInfo[].metaData (实盘合约/现货晒单卡片中的交易标的)
    """
    coins = set()

    # 来源 1: cashTagList
    tag_list_obj = content_data.get("tagList") or {}
    for cash_item in (tag_list_obj.get("cashTagList") or []):
        if isinstance(cash_item, dict):
            c_name = cash_item.get("coinName") or cash_item.get("displayName") or cash_item.get("tag", "")
            c_name = c_name.replace("$", "").strip().upper()
            if c_name:
                coins.add(c_name)

    # 来源 2: tokens 列表
    for token_item in (content_data.get("tokens") or []):
        if isinstance(token_item, dict):
            c_name = (token_item.get("coinName") or token_item.get("displayName") or "").strip().upper()
            if c_name:
                coins.add(c_name)

    # 来源 3: pluginInfo 实盘持仓插件
    for plugin in (content_data.get("pluginInfo") or []):
        if not isinstance(plugin, dict):
            continue
        # 优先从 displayId 拆解 (如 "ZEC-USDT-SWAP" -> "ZEC")
        display_id = plugin.get("displayId") or ""
        if "-" in display_id:
            base_coin = display_id.split("-")[0].strip().upper()
            if base_coin:
                coins.add(base_coin)

        # 深入解析 metaData JSON 字符串中的 aiData.instNames
        meta_str = plugin.get("metaData")
        if meta_str and isinstance(meta_str, str):
            try:
                meta_json = json.loads(meta_str)
                inst_names = meta_json.get("aiData", {}).get("instNames") or []
                for group in inst_names:
                    if isinstance(group, list):
                        for name in group:
                            if name:
                                coins.add(str(name).strip().upper())
            except Exception:
                pass

    return list(coins)


def _extract_okx_hashtags(content_data):
    """提取并清理 OKX Hashtags"""
    tag_list_obj = content_data.get("tagList") or {}
    hash_list = tag_list_obj.get("hashTagList") or []
    tags = []
    for item in hash_list:
        if isinstance(item, dict):
            tag_str = (item.get("tag") or "").strip()
            if tag_str:
                tags.append(tag_str)
    return list(set(tags))


def _extract_okx_plugin_summary(content_data):
    """
    提取 OKX 独有的实盘合约持仓卡片信息，转化为可读文本标记，辅助下游分析
    例如: [实盘晒单: ZEC-USDT-SWAP | 方向:short | 杠杆:30x | 收益:+176.85 USDT (190.38%) | 开仓均价:1227.69]
    """
    summaries = []
    for plugin in (content_data.get("pluginInfo") or []):
        if not isinstance(plugin, dict):
            continue
        meta_str = plugin.get("metaData")
        if not meta_str or not isinstance(meta_str, str):
            continue
        try:
            meta_json = json.loads(meta_str)
            fut_info = meta_json.get("futuresCurrentInfo") or {}
            if fut_info:
                inst_id = fut_info.get("instId") or plugin.get("displayId", "UNKNOWN")
                direction = fut_info.get("posDirection") or fut_info.get("shareType", "")
                leverage = fut_info.get("leverage", "")
                pnl = fut_info.get("pnl", "")
                pnl_unit = fut_info.get("pnlUnit", "USDT")
                pnl_rate = fut_info.get("pnlRate", "")
                entry_price = fut_info.get("entryPrice", "")

                rate_str = ""
                if pnl_rate:
                    try:
                        rate_str = f" ({float(pnl_rate) * 100:.2f}%)"
                    except ValueError:
                        pass

                summary = (
                    f"[实盘晒单: {inst_id} | 方向:{direction} | 杠杆:{leverage} | "
                    f"收益:{pnl} {pnl_unit}{rate_str} | 开仓价:{entry_price}]"
                )
                summaries.append(summary)
        except Exception:
            continue
    return summaries


def _infer_card_type(content_data):
    """
    将 OKX 的帖子形态映射为与币安一致的 card_type 枚举：
    - 含视频 -> BUZZ_VIDEO
    - 长文 (articleSchemaVersion > 0 或 formatType == 1) -> BUZZ_LONG
    - 默认图文短帖 -> BUZZ_SHORT
    """
    if content_data.get("videoUrl") or content_data.get("videoList"):
        return "BUZZ_VIDEO"
    if _safe_int(content_data.get("articleSchemaVersion"), 0) > 0 or content_data.get("formatType") == 1:
        return "BUZZ_LONG"
    return "BUZZ_SHORT"


# ============================================================
# OKX 帖子数据清洗器
# ============================================================
def clean_okx_posts(raw_data_list):
    """
    将 OKX 原始 dataList 清洗并转换为统一的 get_standard_schema() 结构
    """
    cleaned_list = []
    failed = 0

    for raw_wrapper in raw_data_list:
        try:
            # 兼容直接传入 contentData 或外层带有 contentData 包装的字典
            content_data = raw_wrapper.get("contentData") if "contentData" in raw_wrapper else raw_wrapper
            if not isinstance(content_data, dict):
                continue

            post_id = str(content_data.get("contentId") or content_data.get("id") or "")
            if not post_id:
                continue

            data = get_standard_schema()
            card_type = _infer_card_type(content_data)

            # 1. 顶层核心字段
            data["post_id"] = post_id
            raw_pub_time = _safe_int(content_data.get("publishTime"), 0)
            data["publish_time"] = raw_pub_time // 1000 if raw_pub_time > 10_000_000_000 else raw_pub_time
            author_obj = content_data.get("author") or {}
            author_id = str(author_obj.get("authorId") or "")
            data["author_id"] = author_id if author_id else None
            data["card_type"] = card_type

            # 2. 元数据 (Metadata)
            share_url = content_data.get("shareUrl") or ""
            clean_url = share_url.split("?")[0] if share_url else f"https://www.okx.com/zh-hans/orbit/post/{post_id}"
            bot_types = content_data.get("botTypeList") or []
            data["metadata"].update({
                "url": clean_url,
                "is_ai_generated": len(bot_types) > 0
            })

            # 3. 作者信息 (Author)
            is_verified = (
                str(author_obj.get("officialStatus", "0")) != "0"
                or bool(author_obj.get("certBadge"))
                or bool(author_obj.get("badgeList"))
            )
            data["author"].update({
                "username": author_id or author_obj.get("nickName"),
                "author_name": author_obj.get("nickName"),
                "is_verified": is_verified
            })

            # 4. 正文内容与多语言翻译处理 (Content)
            auto_translate = bool(content_data.get("autoTranslate"))
            if auto_translate and content_data.get("translatedTitle"):
                raw_title = content_data.get("translatedTitle")
            else:
                raw_title = content_data.get("title") or content_data.get("enTitle")

            if auto_translate and content_data.get("translatedContent"):
                raw_text = content_data.get("translatedContent")
            else:
                raw_text = content_data.get("content") or content_data.get("excerpt") or ""

            # 仅做纯文本清洗，不再拼接 [实盘晒单: ...] 等非图片插件占位符
            text_content = _clean_text(raw_text)

            data["content"].update({
                "title": _clean_title(raw_title),
                "text_content": text_content,
                "hashtags": _extract_okx_hashtags(content_data),
                "mentioned_coins": _extract_okx_coins(content_data)
            })

            # 5. 媒体资源提取 (Media)
            image_urls = []
            for img in (content_data.get("imageList") or []):
                if isinstance(img, dict) and img.get("url"):
                    image_urls.append(img.get("url"))
                elif isinstance(img, str) and img:
                    image_urls.append(img)

            cover_photo = content_data.get("coverPhoto")
            cover_url = None
            if isinstance(cover_photo, dict):
                cover_url = cover_photo.get("url")
            elif isinstance(cover_photo, str) and cover_photo:
                cover_url = cover_photo

            data["media"]["cover_image"] = cover_url
            if card_type == "BUZZ_LONG":
                data["media"]["inline_images"] = image_urls
            else:
                data["media"]["images"] = image_urls

            # 6. 互动数据 (Engagement)
            data["engagement"].update({
                "view_count": _safe_int(content_data.get("viewCount"), 0),
                "like_count": _safe_int(content_data.get("likeCount"), 0),
                "comment_count": _safe_int(content_data.get("commentNum"), 0),
                "share_count": _safe_int(content_data.get("retweetTotal"), 0),
                "quote_count": _safe_int(content_data.get("quoteCount"), 0)
            })

            cleaned_list.append(data)

        except Exception as e:
            failed += 1
            logger.error(f"[OKX_CLEAN] 帖子清洗失败 | error={e}", exc_info=True)

    logger.info(f"🗂️ [OKX清洗] 完成 | 成功={len(cleaned_list)}条 | 失败={failed}条")
    return cleaned_list

# ============================================================
# OKX 评论采集与清洗 (与 clean_binance_replies 100% 格式对齐)
# ============================================================
def _format_single_okx_comment(item, default_parent_id):
    """格式化单条 OKX 评论字典"""
    auto_translate = bool(item.get("autoTranslate"))
    original_text = item.get("comment") or ""
    translated_text = item.get("translatedComment") or ""

    # 若开启了自动翻译且有译文（例如外语评论被翻译为中文），优先采用译文
    content = translated_text if (auto_translate and translated_text) else original_text

    reply_root_id = str(item.get("replyRootId") or "0")
    parent_id = default_parent_id if reply_root_id == "0" else reply_root_id

    return {
        "reply_id": str(item.get("commentId") or ""),
        "parent_id": str(parent_id),
        "create_time": _safe_int(item.get("commentTime"), 0),
        "author_uid": str(item.get("authorId") or ""),
        "author_name": item.get("userName") or "",
        "content": _clean_text(content),
        "likes": _safe_int(item.get("likeNum"), 0),
        "replies": _safe_int(item.get("commentNum"), 0),
        "views": 0  # OKX 评论无浏览量字段，填 0 保持结构一致
    }


def clean_okx_replies(raw_replies, content_id=""):
    """
    清洗 OKX 评论列表，同时拍平二级楼中楼回复 (replies.replyList)
    """
    cleaned_data = []
    seen_reply_ids = set()

    for item in raw_replies:
        if not isinstance(item, dict):
            continue

        # 跳过已删除评论
        if str(item.get("isDel", "0")) == "1":
            continue

        main_comment = _format_single_okx_comment(item, default_parent_id=content_id)
        if main_comment["reply_id"] and main_comment["reply_id"] not in seen_reply_ids:
            seen_reply_ids.add(main_comment["reply_id"])
            cleaned_data.append(main_comment)

        # 提取并拍平二级嵌套回复
        replies_container = item.get("replies") or {}
        sub_reply_list = replies_container.get("replyList") or []
        for sub_item in sub_reply_list:
            if not isinstance(sub_item, dict) or str(sub_item.get("isDel", "0")) == "1":
                continue
            sub_comment = _format_single_okx_comment(
                sub_item,
                default_parent_id=main_comment["reply_id"] or content_id
            )
            if sub_comment["reply_id"] and sub_comment["reply_id"] not in seen_reply_ids:
                seen_reply_ids.add(sub_comment["reply_id"])
                cleaned_data.append(sub_comment)

    return cleaned_data


def fetch_okx_replies(content_id, required_count=10, cookies="", extra_headers=None, session=None):
    """
    获取 OKX 指定帖子的评论列表（支持游标翻页与连接池复用）
    """
    url = "https://www.okx.com/priapi/v5/content/public/comments"
    referer = f"https://www.okx.com/zh-hans/orbit/post/{content_id}"

    req_client = session if session else requests
    all_raw_comments = []
    cursor = None
    page_index = 1
    retry_count = 0
    label = f"OKX评论流:{content_id}"

    while len(all_raw_comments) < required_count:
        now_ms = str(int(time.time() * 1000))
        params = {
            "contentId": str(content_id),
            "count": min(20, max(10, required_count)),
            "t": now_ms
        }
        if cursor and cursor != "0":
            params["cursor"] = cursor

        headers = _build_okx_headers(referer=referer, cookies=cookies, extra_headers=extra_headers)
        response = None

        try:
            response = req_client.get(
                url,
                headers=headers,
                params=params,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            json_resp = response.json()

            if str(json_resp.get("code")) != "0":
                logger.error(f"❌ [{label}] API 业务错误 | code={json_resp.get('code')} | msg={json_resp.get('msg')}")
                break

            data_obj = json_resp.get("data") or {}
            comment_list = data_obj.get("commentList") or []
            if not comment_list:
                break

            all_raw_comments.extend(comment_list)
            next_cursor = str(data_obj.get("nextCursor") or "0")

            # OKX 评论 nextCursor 为 "0" 或空时代表无下一页
            if not next_cursor or next_cursor == "0" or next_cursor == cursor:
                break

            cursor = next_cursor
            page_index += 1
            retry_count = 0
            time.sleep(random.uniform(0.4, 1.0))

        except Exception as e:
            detail = ""
            if response is not None:
                detail = f" | HTTP {response.status_code} | 返回: {response.text[:300]}"
            retry_count += 1
            logger.warning(
                f"🚨 [{label}] 第 {page_index} 页请求失败 ({retry_count}/{MAX_RETRIES}){detail} | 异常: {e}"
            )
            if retry_count >= MAX_RETRIES:
                logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止评论采集。")
                break
            time.sleep(random.uniform(1.0, 2.5))

    cleaned_replies = clean_okx_replies(all_raw_comments, content_id=str(content_id))
    return cleaned_replies[:required_count]


# ============================================================
# 多模态富化与并发处理 (与币安 _enrich_single_post 逻辑对齐)
# ============================================================
def _enrich_single_okx_post(item, session, cookies="", extra_headers=None, download_media=True):
    """
    单条 OKX 帖子的多模态标记融合、图片下载与评论拉取 (无锁原子函数)
    """
    stats = {
        'long_updated': 0,
        'other_updated': 0,
        'comments_fetched': 0,
        'total_download_urls': 0,
        'total_download_success': 0,
        'failed': 0
    }

    post_id = item.get('post_id')
    if not post_id:
        return item, stats

    try:
        # ---------- 1. 仅将图片类占位符融合到正文尾部 ----------
        media_info = item.get('media', {})
        media_marks = []

        if media_info.get('cover_image'):
            media_marks.append(f"[封面图: {media_info['cover_image']}]")

        for img_url in media_info.get('images', []):
            if img_url:
                media_marks.append(f"[插图: {img_url}]")

        for img_url in media_info.get('inline_images', []):
            if img_url:
                media_marks.append(f"[插图: {img_url}]")

        if media_marks:
            original_text = item.get('content', {}).get('text_content', '').strip()
            marks_text = "\n".join(media_marks)
            if marks_text not in original_text:
                item['content']['text_content'] = (
                    f"{original_text}\n\n{marks_text}" if original_text else marks_text
                )

        stats['other_updated'] = 1

        # ---------- 2. 拦截媒体资源并下载到本地 ----------
        if 'local_mapping' not in media_info:
            media_info['local_mapping'] = {}

        if download_media:
            urls_to_download = set()
            if media_info.get('cover_image'):
                urls_to_download.add(media_info['cover_image'])
            for img_url in media_info.get('images', []):
                if img_url:
                    urls_to_download.add(img_url)
            for img_url in media_info.get('inline_images', []):
                if img_url:
                    urls_to_download.add(img_url)

            stats['total_download_urls'] = len(urls_to_download)
            proxy_url = PROXIES.get("http")

            for url in urls_to_download:
                try:
                    local_path = download_web_media(
                        url=url,
                        save_dir=MEDIA_SAVE_DIR,
                        proxy=proxy_url
                    )
                    if local_path:
                        media_info['local_mapping'][url] = local_path
                        stats['total_download_success'] += 1
                except Exception as dl_e:
                    logger.error(f"[OKX_DOWNLOAD] 媒体下载失败 | post_id={post_id} | url={url} | error={dl_e}")

        # ---------- 3. 【智能拦截】按需拉取帖子评论 ----------
        try:
            comment_count = item.get('engagement', {}).get('comment_count', 0)
            if comment_count <= 0:
                item['comments'] = []
            else:
                replies = fetch_okx_replies(
                    content_id=post_id,
                    required_count=10,
                    cookies=cookies,
                    extra_headers=extra_headers,
                    session=session
                )
                item['comments'] = replies
                if replies:
                    stats['comments_fetched'] = 1
        except Exception as reply_e:
            logger.error(f"[OKX_COMMENTS] 评论拉取失败 | post_id={post_id} | error={reply_e}")

    except Exception as e:
        stats['failed'] = 1
        logger.error(f"[OKX_UPDATE] 就地富化失败 | post_id={post_id} | error={e}", exc_info=True)

    return item, stats

def update_okx_posts_in_place(clean_list, cookies="", extra_headers=None, download_media=True):
    """
    多线程并发富化 OKX 帖子：挂载扩容后的 HTTPAdapter 连接池 + 无锁统计汇总
    """
    if not clean_list:
        return []

    total_stats = {
        'long_updated': 0,
        'other_updated': 0,
        'comments_fetched': 0,
        'total_download_urls': 0,
        'total_download_success': 0,
        'failed': 0
    }

    max_workers = min(10, len(clean_list))
    logger.debug(f"⏳ [OKX并发富化] 开始处理 {len(clean_list)} 条数据 (线程数={max_workers})...")

    with requests.Session() as session:
        # 扩容连接池，避免 10 线程并发时触发 urllib3 连接池满警告
        adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_item = {
                executor.submit(
                    _enrich_single_okx_post,
                    item,
                    session,
                    cookies,
                    extra_headers,
                    download_media
                ): item
                for item in clean_list
            }

            for future in concurrent.futures.as_completed(future_to_item):
                try:
                    _, task_stats = future.result()
                    for key in total_stats:
                        total_stats[key] += task_stats.get(key, 0)
                except Exception as exc:
                    logger.error(f"🚨 [OKX并发富化] 线程异常: {exc}", exc_info=True)
                    total_stats['failed'] += 1

    logger.info(
        f"✅ [OKX并发富化] 完毕 | 媒体拼接:{total_stats['other_updated']} | "
        f"下载成功:{total_stats['total_download_success']}/{total_stats['total_download_urls']} | "
        f"评论拉取:{total_stats['comments_fetched']} | 失败:{total_stats['failed']}"
    )
    return clean_list


# ============================================================
# OKX 推荐流分页采集与主入口函数
# ============================================================
def fetch_okx_feed_recommend(
        required_count=20,
        initial_cursor="1",
        cookies="",
        extra_headers=None,
        existing_ids=None
):
    """
    不断拉取 OKX Orbit 推荐流数据，直到满足目标数量或无更多数据

    :param required_count: 期望获取的帖子条数
    :param initial_cursor: 初始游标，默认从 "1" 开始，后续自动使用响应中的 nextCursor
    :param cookies:        可选 Cookie 字符串
    :param extra_headers:  可选额外请求头（如 x-fptoken, x-fptoken-signature）
    :param existing_ids:   可选历史已抓取 ID 集合，支持边翻页边过滤
    :return:               去重后的原始帖子字典列表
    """
    url = "https://www.okx.com/priapi/v5/rubik/public/content/feed/1"
    referer = "https://www.okx.com/zh-hans/orbit"

    all_items = []
    seen_ids = set(existing_ids) if existing_ids else set()
    cursor = str(initial_cursor) if initial_cursor is not None else "1"
    page_index = 1
    retry_count = 0
    max_pages = max(15, (required_count // 10) * 3)  # 防止全量重复时无限翻页的保险阈值
    label = "OKX推荐流"

    with requests.Session() as session:
        while len(all_items) < required_count and page_index <= max_pages:
            now_ms = str(int(time.time() * 1000))
            params = {
                "size": min(PAGE_SIZE, 20),
                "cursor": cursor,
                "t": now_ms
            }
            headers = _build_okx_headers(referer=referer, cookies=cookies, extra_headers=extra_headers)
            response = None

            try:
                response = session.get(
                    url,
                    headers=headers,
                    params=params,
                    proxies=PROXIES,
                    timeout=REQUEST_TIMEOUT
                )
                response.raise_for_status()
                json_resp = response.json()

                if str(json_resp.get("code")) != "0":
                    logger.error(
                        f"❌ [{label}] API 业务报错 | code={json_resp.get('code')} | msg={json_resp.get('msg')}"
                    )
                    break

                data_obj = json_resp.get("data") or {}
                data_list = data_obj.get("dataList") or []

                if not data_list:
                    logger.debug(f"⚠️ [{label}] 第 {page_index} 页 dataList 为空，采集结束")
                    break

                new_in_page = 0
                for raw_item in data_list:
                    content_data = raw_item.get("contentData") or {}
                    item_id = str(content_data.get("contentId") or "")
                    if not item_id:
                        continue
                    if item_id not in seen_ids:
                        seen_ids.add(item_id)
                        all_items.append(raw_item)
                        new_in_page += 1

                next_cursor = str(data_obj.get("nextCursor") or "")
                logger.debug(
                    f"📥 [{label}] 第 {page_index} 页成功 | 本页返回:{len(data_list)}条 | "
                    f"净增未见:{new_in_page}条 | 累计净增:{len(all_items)}/{required_count} | nextCursor={next_cursor}"
                )

                if not next_cursor or next_cursor == "0":
                    break

                cursor = next_cursor
                page_index += 1
                retry_count = 0
                time.sleep(random.uniform(0.5, 1.5))

            except Exception as e:
                detail = ""
                if response is not None and response.status_code >= 400:
                    detail = f" | HTTP {response.status_code} | 返回: {response.text[:300]}"
                retry_count += 1
                logger.warning(
                    f"🚨 [{label}] 第 {page_index} 页请求失败 ({retry_count}/{MAX_RETRIES}){detail} | 异常: {e}"
                )
                if retry_count >= MAX_RETRIES:
                    logger.error(f"❌ [{label}] 连续失败达到 {MAX_RETRIES} 次上限，终止采集")
                    break
                time.sleep(random.uniform(1.0, 2.5))

    return all_items[:required_count]


# ============================================================
# 单元测试与调用示例
# ============================================================
if __name__ == "__main__":
    # 1. 测试拉取 OKX 最新热门币榜单（第一页前 25 个）
    hot_coins = fetch_okx_hot_coins(num=25)

    # 2. 测试拉取 OKX 推荐流（与 fetch_binance_feed 返回格式完全一致）
    history_ids = set()
    okx_recommend_posts = fetch_okx_feed(count=10)

    logger.info(f"🎉 OKX 推荐流抓取完成 | 共获取标准格式帖子: {len(okx_recommend_posts)} 条")
    if okx_recommend_posts:
        logger.info(f"🔎 首条标准帖子结构预览:\n{json.dumps(okx_recommend_posts[0], ensure_ascii=False, indent=2)}")