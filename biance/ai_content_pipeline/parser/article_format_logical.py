"""
[功能摘要] 并行采集帖子并提取论据、生成分析文章、发布币安广场图文及执行点赞收藏。
[输入数据] MongoDB 原帖含 post_id/publish_time/content.text_content/media.local_mapping；
文章含 topic/stance/article_info；另读取提示词、交易币种、浏览器配置及账号状态文件。
[数据流转/交互] 热榜与行情 → 采集入库 → 媒体编号 → 模型提取 evidences → 回写 logic_mul；
论据按时效/币种/立场分组 → 按已发布引用次数选材 → 模型生成/校验 → 文章入库；
近 6 小时含图文章 → 评分/冷却筛选 → 浏览器发布 → 账号文件和数据库回写；
近 24 小时已发布文章 → 获取账号凭证 → 点赞收藏 → 回写互动标记。
[输出数据] 帖子论据、生成文章、远端发布与互动结果，以及本地账号状态和分阶段日志。
"""

import json
import math
import os
import random
import re
import threading
import time
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import ccxt

from app.ai_api.model_api import generate_content
from biance.biance_playwright import create_binance_post, get_auth_tokens_robust
from biance.biance_squre_api import (
    fetch_binance_feed, fetch_binance_future_hot_coins, fetch_binance_hot_hashtags,
    fetch_binance_spot_hot_coins, like_and_bookmark,
)
from common.common_utils import (
    get_config, read_file_to_str, read_json, save_json, setup_logger, string_to_object,
)
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager, UniversalPostManager

logger = setup_logger(app_name="media_format")

BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
POST_MAX_AGE_HOURS = 48
MAX_MEDIA_COUNT = 10
MAX_CONCURRENCY = 10
LLM_MAX_RETRIES = 3
PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类MLU提取.txt"
ARTICLE_PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类文章生成.txt"
ARTICLE_MATERIAL_LIMIT = 20
ARTICLE_MIN_MATERIALS = 5
ARTICLE_POST_USAGE_LIMIT = 2
ARTICLE_MAX_CHARS = 260
ARTICLE_VARIANTS = 5
ARTICLE_CONCURRENCY = 5
ARTICLE_GENERATION_INTERVAL_SECONDS = 600
STATE_FILE = "account_publish_state.json"
ACCOUNTS = ["myself", "mama", "ruru", "qiqi"]
FEED_TOKENS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE"]
ACCOUNT_COOLDOWN_SECONDS = 3600
ACCOUNT_TOPIC_COOLDOWN_SECONDS = 2 * 3600
PUBLISH_MAX_ATTEMPTS = 3
PUBLISH_POLL_SECONDS = 600
INTERACTION_INTERVAL_SECONDS = 3600
MEDIA_PATTERN = re.compile(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]")
SHELF_LIFE_SECONDS = {
    "hours": 2 * 3600, "days": 24 * 3600, "weeks": 7 * 24 * 3600,
    "long": 15 * 24 * 3600, "unknown": 24 * 3600,
}




def _publish_time_seconds(value):
    """统一秒/毫秒时间戳；无效或非有限数值显式报错，避免进入时间比较。"""
    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("发布时间不是有效的秒/毫秒时间戳") from exc
    if not math.isfinite(timestamp):
        raise ValueError("发布时间不能是 NaN 或无穷大")
    return timestamp / 1000 if timestamp > 1e11 else timestamp


def _key_error(data, expected_keys, label):
    """检查字典的精确字段集合；输入字典/任意外部值及字段集合，返回错误文本或空串。"""
    if not isinstance(data, dict):
        return f"{label} 必须是字典"
    missing = expected_keys - data.keys()
    extra = data.keys() - expected_keys
    if missing:
        return f"{label} 缺少字段: {', '.join(sorted(missing))}"
    if extra:
        return f"{label} 包含未定义字段: {', '.join(sorted(map(str, extra)))}"
    return ""


def is_need_formatting(post):
    """判断是否需提取论据；post 含 logic_mul、media、publish_time，返回布尔值。"""
    if post.get("logic_mul"):
        return False
    media = post.get("media") or {}
    video_duration = media.get("video_duration")
    if video_duration and video_duration > 0:
        return False
    local_mapping = media.get("local_mapping") or {}
    local_paths = list(local_mapping.values())
    if any(not path or not os.path.isfile(path) for path in local_paths):
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    if any(not local_mapping.get(match.group(2)) for match in MEDIA_PATTERN.finditer(text)):
        return False
    if len(local_paths) >= MAX_MEDIA_COUNT:
        return False
    publish_time = _publish_time_seconds(post.get("publish_time", 0))
    return (time.time() - publish_time) / 3600 <= POST_MAX_AGE_HOURS


def normalize_post_media(post_data):
    """统一占位符编号；输入 content.text_content、media.local_mapping。
    返回 (正文, 路径列表, {标准占位符: 本地路径})，重复 URL 按出现次数保留。
    """
    text = (post_data.get("content") or {}).get("text_content") or ""
    mapping = (post_data.get("media") or {}).get("local_mapping") or {}
    paths, placeholders = [], {}
    counters = {"IMAGE": 0, "VIDEO": 0}

    def replace_match(match):
        """图片与视频分别编号，保持正文顺序与上传顺序一致。"""
        kind = "VIDEO" if match.group(1) == "视频" else "IMAGE"
        counters[kind] += 1
        placeholder = f"[{kind}_{counters[kind]}]"
        path = mapping.get(match.group(2), "")
        paths.append(path)
        placeholders[placeholder] = path
        return placeholder

    return MEDIA_PATTERN.sub(replace_match, text), paths, placeholders


def check_format_info(json_data, placeholders):
    """校验提取结果；输入 {evidences: [论据字典]}、真实占位符列表。
    论据字段及 images 子字段由下方集合定义；返回 (是否合法, 错误文本)。
    """
    if not isinstance(json_data, dict):
        return False, "最外层必须是字典"
    if "evidences" not in json_data:
        return False, "最外层缺少 evidences"
    if not isinstance(json_data["evidences"], list):
        return False, "evidences 必须是列表"

    evidence_keys = {
        "core_fact", "logic_link", "dimension", "coins",
        "stance", "impact_weight", "shelf_life", "images",
    }
    image_keys = {"image_id", "image_type", "context", "usable", "unusable_reason"}
    enums = {
        "dimension": ("K线形态", "技术指标", "链上数据", "资金流", "消息面", "基本面", "情绪判断"),
        "stance": ("看多", "看空", "震荡"),
        "shelf_life": ("hours", "days", "weeks", "long", "unknown"),
    }
    image_types = ("盘面截图", "数据图表", "新闻截图", "社交截图", "收益截图", "梗图表情", "实拍照片", "其他")
    # : 保留原校验边界：顶层允许额外字段、evidences 可为空；
    # core_fact/context/coins 元素不新增类型限制，图片允许遗漏或重复引用，不强制一一覆盖。
    for index, evidence in enumerate(json_data["evidences"], 1):
        label = f"evidences[{index}]"
        error = _key_error(evidence, evidence_keys, label)
        if error:
            return False, error
        if not isinstance(evidence["logic_link"], str):
            return False, f"{label}.logic_link 必须是字符串"
        weight = evidence["impact_weight"]
        if not isinstance(weight, int) or isinstance(weight, bool) or not 1 <= weight <= 5:
            return False, f"{label}.impact_weight 必须是 1—5 的整数，不能是布尔值"
        for field, allowed in enums.items():
            if evidence[field] not in allowed:
                return False, f"{label}.{field} 取值不合法: {evidence[field]!r}"
        if not isinstance(evidence["coins"], list) or not evidence["coins"]:
            return False, f"{label}.coins 必须是非空列表"
        if not isinstance(evidence["images"], list):
            return False, f"{label}.images 必须是列表"

        for image_index, image in enumerate(evidence["images"], 1):
            image_label = f"{label}.images[{image_index}]"
            error = _key_error(image, image_keys, image_label)
            if error:
                return False, error
            if image["image_id"] not in placeholders:
                return False, f"{image_label}.image_id 未出现在原文: {image['image_id']!r}"
            if image["image_type"] not in image_types:
                return False, f"{image_label}.image_type 不在允许枚举内"
            usable, reason = image["usable"], image["unusable_reason"]
            if not isinstance(usable, bool) or not isinstance(reason, str):
                return False, f"{image_label} 的 usable 必须是布尔值，unusable_reason 必须是字符串"
            if usable and reason.strip():
                return False, f"{image_label} 可用时 unusable_reason 必须为空"
            if not usable and not reason.strip():
                return False, f"{image_label} 不可用时必须说明 unusable_reason"
    return True, ""


def gen_media_format_info(post):
    """提取并校验论据；post 含 post_id/content.text_content/media.local_mapping。
    返回 {evidences: [...]}；保留模型重试耗尽返回 {}、前置读取异常上抛的约定。
    """
    text, paths, mapping = normalize_post_media(post)
    full_prompt = f"{read_file_to_str(PROMPT_FILE_PATH)}\n{text}"
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        raw_response = ""
        try:
            result = generate_content(prompt=full_prompt, file_paths=paths)
            raw_response = result.get("content", "")
            format_info = string_to_object(raw_response)
            valid, error = check_format_info(format_info, list(mapping))
            if not valid:
                raise ValueError(error)
            return format_info
        except Exception as exc:
            exhausted = attempt == LLM_MAX_RETRIES
            delay = 0 if exhausted else 2 ** attempt
            log = logger.error if exhausted else logger.warning
            log(
                "%s[论据/提取] 模型调用或数据校验失败 | 帖子: [%s] | 尝试: [%s/%s] "
                "| 原因: [%s] | 响应摘要: [%s] | 后续: [%s] "
                "| 排查: [检查媒体读取、模型服务及提示词字段]",
                "❌ " if exhausted else "", post.get("post_id", "UNKNOWN_ID"),
                attempt, LLM_MAX_RETRIES, exc, repr(raw_response)[:500],
                "返回空数据，等待下轮扫描" if exhausted else f"{delay} 秒后重试",
                exc_info=exhausted,
            )
            if exhausted:
                return {}
            time.sleep(delay)
    return {}


def process_and_save_single_post(post, post_manager):
    """提取后回写 post.logic_mul；post 含 post_id/content/media，管理器接收 [post]。
    返回 None；保留单帖异常隔离，避免一条失败终止整批任务。
    """
    started = time.monotonic()
    post_id = post.get("post_id", "UNKNOWN_ID") if isinstance(post, dict) else "UNKNOWN_ID"
    try:
        format_info = gen_media_format_info(post)
        if not format_info:
            return
        post["logic_mul"] = format_info
        post_manager.upsert_posts([post])
        logger.info(
            "[帖子/回写] 论据已校验并保存 | 帖子: [%s] | 论据: [%s 条] | 耗时: [%.2f 秒]",
            post_id, len(format_info["evidences"]), time.monotonic() - started,
        )
    except Exception:
        logger.exception(
            "❌ [帖子/处理] 当前帖子失败 | 帖子: [%s] | 后续: [结束本帖，继续其他任务] "
            "| 排查: [检查帖子字段、提示词和媒体路径、数据库连接]", post_id,
        )


def _fetch_and_save_posts(post_manager, token_list, orders, feed_count):
    """按币种/排序采集后统一回写；管理器接收 [帖子字典]，其余为币种序列和接口参数。
    保留接口调用顺序与重复帖子，返回 None。
    """
    started = time.monotonic()
    posts, token_count = [], 0
    for token in token_list:
        token_count += 1
        for order in orders:
            posts.extend(fetch_binance_feed(token=token, count=100, orderBy=order))
    posts.extend(fetch_binance_feed(count=feed_count))
    if posts:
        post_manager.upsert_posts(posts)
    logger.info(
        "[采集/完成] 推荐流已处理 | 币种: [%s 个] | 入库: [%s 条，含重复] | 耗时: [%.2f 秒]",
        token_count, len(posts), time.monotonic() - started,
    )


def fetch_post(post_manager):
    """采集固定币种热门流和综合流；post_manager 提供 upsert_posts([帖子])，返回 None。"""
    _fetch_and_save_posts(post_manager, FEED_TOKENS, (1,), 100)


def fetch_hot_post(post_manager, token_list):
    """采集指定币种的热门/最新流和综合流；token_list 为币种序列，管理器接收 [帖子]。"""
    _fetch_and_save_posts(post_manager, token_list, (1, 2), 200)


def get_top_k_movers(top_k=5):
    """按合约涨跌幅取两端；返回 {top_gainers/top_losers: [{symbol, percentage}]}。"""
    # : ccxt 版本及资源释放契约未给出，保留原客户端用法；需在依赖侧确认会话回收。
    exchange = ccxt.binance({
        "enableRateLimit": True,
        "options": {"defaultType": "future"},
        "proxies": {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"},
    })
    tickers = exchange.fetch_tickers()
    ranked = sorted(
        ({"symbol": symbol, "percentage": ticker["percentage"]}
         for symbol, ticker in tickers.items()
         if symbol.endswith(":USDT") and ticker.get("percentage") is not None),
        key=lambda item: item["percentage"], reverse=True,
    )
    # : 保留原切片语义；top_k=0 时跌幅榜会包含全部条目，暂不新增参数限制。
    return {"top_gainers": ranked[:top_k], "top_losers": ranked[-top_k:][::-1]}


def get_hot_coin():
    """合并合约/现货热榜与涨跌榜各前两名；返回去重币种列表，沿用 set 的无序行为。"""
    top_k, coins = 2, []
    for fetch in (fetch_binance_future_hot_coins, fetch_binance_spot_hot_coins):
        coins.extend(item.get("symbol").replace("USDT", "").replace("USDC", "")
                     for item in fetch()[:top_k])
    movers = get_top_k_movers(top_k)
    coins.extend(item["symbol"].split("/")[0]
                 for item in movers["top_gainers"] + movers["top_losers"])
    coins = list(set(coins))
    logger.info("[币种/热榜] 已合并热门币种 | 数量: [%s] | 币种: [%s]", len(coins), coins)
    return coins


def format_image_article():
    """轮询采集、筛选及并发提取；批次异常沿用 60 秒后重试的策略。"""
    while True:
        try:
            started = time.monotonic()
            post_manager = UniversalPostManager(gen_db_object())
            fetch_hot_post(post_manager, get_hot_coin())
            posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
            if not posts:
                logger.info("[帖子/轮询] 暂无原帖 | 下次扫描: [60 秒后]")
                time.sleep(60)
                continue
            # : 保留前置筛选中任一坏帖中断本轮的行为，未改成逐帖跳过。
            valid_posts = [post for post in posts if is_need_formatting(post)]
            logger.info(
                "[帖子/计划] 前置筛选完成 | 扫描: [%s] | 待提取: [%s] | 跳过: [%s] "
                "| 线程上限: [%s] | 规则: [无论据、非视频、本地媒体少于 %s 个、%s 小时内]",
                len(posts), len(valid_posts), len(posts) - len(valid_posts),
                min(MAX_CONCURRENCY, len(valid_posts)), MAX_MEDIA_COUNT, POST_MAX_AGE_HOURS,
            )
            if valid_posts:
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                    futures = [executor.submit(process_and_save_single_post, post, post_manager)
                               for post in valid_posts]
                    for future in as_completed(futures):
                        future.result()
            # : 原查询未分页；满额缩短等待不能保证后续记录被扫描。
            delay = 5 if len(posts) >= POST_QUERY_LIMIT else 3600
            logger.info(
                "[帖子/完成] 本批线程已结束 | 扫描: [%s] | 已提交: [%s] | 耗时: [%.2f 秒] "
                "| 下次扫描: [%s 秒后] | 单帖结果: [见帖子日志]",
                len(posts), len(valid_posts), time.monotonic() - started, delay,
            )
            time.sleep(delay)
        except Exception:
            logger.exception(
                "❌ [帖子/轮询] 本轮中断 | 后续: [60 秒后重新初始化并重试] "
                "| 排查: [检查热榜/采集服务、代理、帖子字段及数据库]"
            )
            time.sleep(60)


def clear_all_media_format_batch():
    """手动清理查询范围内的 logic_mul；无入参，非 None 字段置空后批量回写。"""
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    updates = [post for post in posts if post.get("logic_mul") is not None]
    for post in updates:
        post["logic_mul"] = None
    if updates:
        post_manager.upsert_posts(updates)
    logger.info(
        "[数据/清理] logic_mul 清理完成 | 扫描: [%s] | 更新: [%s]",
        len(posts), len(updates),
    )


def build_source_image_mapping(post):
    """还原真实图片来源；输入 content.text_content、media.local_mapping。
    返回 {IMAGE占位符: {original_placeholder, original_url, local_path}}，排除视频。
    """
    _, _, normalized = normalize_post_media(post)
    text = (post.get("content") or {}).get("text_content") or ""
    return {
        image_id: {
            "original_placeholder": match.group(0),
            "original_url": match.group(2),
            "local_path": path or None,
        }
        for (image_id, path), match in zip(normalized.items(), MEDIA_PATTERN.finditer(text))
        if image_id.startswith("[IMAGE_")
    }


def transform_mlus(mlu_list):
    """把论据转成创作素材，保持多图证据链完整及同帖同图去重。
    输入项含 core_fact、logic_link、images[{image_id, usable, context}]、
    source_post_id、source_image_mapping；缺少映射时由 source_text_content 恢复。
    返回 ([{id, fact, underlying_logic, visual_evidence}], {ASSET占位符: 来源字典})；
    visual_evidence 为 None、单图字典或有序多图列表，图项含 placeholder/what_it_shows。
    """
    materials, image_mapping, asset_by_source = [], {}, {}
    for index, evidence in enumerate(mlu_list, 1):
        material = {
            "id": f"M{index}",
            "fact": evidence.get("core_fact", ""),
            "underlying_logic": evidence.get("logic_link", ""),
            "visual_evidence": None,
        }
        materials.append(material)
        images = [image for image in evidence.get("images", []) if image.get("usable") is True]
        if not images:
            continue
        source_mapping = evidence.get("source_image_mapping")
        if source_mapping is None:
            source_mapping = build_source_image_mapping({
                "content": {"text_content": evidence.get("source_text_content", "")},
                "media": {"local_mapping": evidence.get("source_local_mapping") or {}},
            })
        if not all(
                image.get("image_id") in source_mapping
                and isinstance(image.get("context"), str) and image["context"].strip()
                for image in images
        ):
            logger.warning(
                "[文章/配图] 当前论据取消整组配图并保留文字 | 帖子: [%s] "
                "| 原因: [至少一张图片缺少来源映射或有效描述] | 排查: [核对原文占位符与 context]",
                evidence.get("source_post_id"),
            )
            continue
        visual_items, seen_ids = [], set()
        for image in images:
            image_id = image["image_id"]
            if image_id in seen_ids:
                continue
            seen_ids.add(image_id)
            source_key = (evidence.get("source_post_id"), image_id)
            placeholder = asset_by_source.get(source_key)
            if placeholder is None:
                placeholder = f"[ASSET_IMG_{len(asset_by_source) + 1}]"
                asset_by_source[source_key] = placeholder
                image_mapping[placeholder] = {
                    "source": BINANCE_SOURCE,
                    "source_post_id": evidence.get("source_post_id"),
                    "original_image_id": image_id,
                    "original_image_id_list": [image_id],
                    **source_mapping[image_id],
                }
            visual_items.append({"placeholder": placeholder, "what_it_shows": image["context"]})
        material["visual_evidence"] = visual_items[0] if len(visual_items) == 1 else visual_items
    return materials, image_mapping


def extract_and_group_valid_evidences():
    """按时效/币种/立场筛选并按权重排序，返回 {coin: {stance: [论据]}}。
    论据追加 source_post_id/publish_time/source_text_content/source_image_mapping。
    """
    hot_coins = {coin.upper() for coin in get_hot_coin()}
    active_symbols = str(read_json("active_usdt_symbols.json")).upper()
    started = time.monotonic()
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    grouped = defaultdict(lambda: defaultdict(list))
    now, invalid_times = time.time(), 0
    for post in posts:
        logic_mul, raw_time = post.get("logic_mul"), post.get("publish_time")
        if not isinstance(logic_mul, dict) or not logic_mul.get("evidences") or not raw_time:
            continue
        try:
            publish_time = _publish_time_seconds(raw_time)
        except ValueError:
            invalid_times += 1
            continue
        age = now - publish_time
        if age > 15 * 24 * 3600 or age < -3600:
            continue
        source_mapping = build_source_image_mapping(post)
        for evidence in logic_mul["evidences"]:
            duration = SHELF_LIFE_SECONDS.get(evidence.get("shelf_life", "unknown"), 24 * 3600)
            coins, stance = evidence.get("coins", []), evidence.get("stance")
            if age > duration or not isinstance(coins, list) or not coins or stance not in ("看多", "看空"):
                continue
            enriched = {
                **evidence, "source_post_id": post.get("post_id", "UNKNOWN"),
                "publish_time": publish_time,
                "source_text_content": (post.get("content") or {}).get("text_content", ""),
                "source_image_mapping": source_mapping,
            }
            for coin in coins:
                coin = str(coin).strip().upper()
                if coin and coin in hot_coins and coin in active_symbols:
                    grouped[coin][stance].append(enriched)
    for stances in grouped.values():
        for evidences in stances.values():
            evidences.sort(key=lambda item: item.get("impact_weight", 0), reverse=True)
    # JSON 往返保留原序列化约束，并让跨组论据互不共享可变对象。
    result = json.loads(json.dumps(grouped))
    log = logger.warning if invalid_times else logger.info
    log(
        "[论据/筛选] 已分组并按权重排序 | 扫描: [%s] | 币种: [%s] | 论据: [%s，含跨组重复] "
        "| 无效时间跳过: [%s] | 耗时: [%.2f 秒] | 时间异常排查: [核对 publish_time 秒/毫秒数值]",
        len(posts), len(result), sum(len(items) for stances in result.values() for items in stances.values()),
        invalid_times, time.monotonic() - started,
    )
    return result


def check_article_info(article_info, materials, image_mapping, max_chars):
    """校验文章与证据链；article_info 必须含下方六字段。
    materials 含 id/visual_evidence，image_mapping 为真实 ASSET 来源映射；
    返回 (是否合法, 错误文本)，不修改输入。
    """
    keys = {"status", "text", "image_placeholders", "used_material_ids", "score", "reason"}
    error = _key_error(article_info, keys, "文章")
    if error:
        return False, error
    if article_info["status"] not in ("ok", "skip"):
        return False, "status 必须为 ok 或 skip"
    if not isinstance(article_info["text"], str):
        return False, "text 必须是字符串"
    for key in ("image_placeholders", "used_material_ids"):
        values = article_info[key]
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            return False, f"{key} 必须是非空字符串组成的列表，列表本身可为空"
        if len(values) != len(set(values)):
            return False, f"{key} 不允许重复"
    if article_info["status"] == "skip":
        if (article_info["text"] != "" or article_info["image_placeholders"]
                or article_info["used_material_ids"] or article_info["score"] is not None):
            return False, "skip 必须清空正文、图片、采用素材，score 必须为 null"
        if not isinstance(article_info["reason"], str) or not article_info["reason"].strip():
            return False, "skip 必须说明 reason"
        return True, ""
    text = article_info["text"]
    if not text.strip():
        return False, "ok 正文不能为空"
    if len(re.sub(r"\s", "", text)) > max_chars:
        return False, f"正文非空白字符数超过 {max_chars}"
    if re.search(r"\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]", text):
        return False, "正文不能包含图片或视频占位符"
    if type(article_info["score"]) not in (int, float) or not 0 <= article_info["score"] <= 10:

        return False, "score 必须是 0—10 的浮点数，不能是整数或布尔值"
    if article_info["reason"] is not None:
        return False, "ok 的 reason 必须为 null"
    material_by_id = {item["id"]: item for item in materials}
    used_ids, selected_images = article_info["used_material_ids"], article_info["image_placeholders"]
    if not 1 <= len(used_ids) <= 10 or any(item not in material_by_id for item in used_ids):
        return False, "used_material_ids 必须引用实际存在的 1—10 条素材"
    if len(selected_images) > 3:
        return False, "最多使用 3 张图片"
    allowed_images = set()
    for material_id in used_ids:
        visual = material_by_id[material_id]["visual_evidence"]
        if not visual:
            continue
        chain = visual if isinstance(visual, list) else [visual]
        chain_ids = [item["placeholder"] for item in chain]
        allowed_images.update(chain_ids)
        chosen = [item for item in selected_images if item in chain_ids]
        if chosen and chosen != chain_ids:
            return False, f"素材 {material_id} 的图片证据链必须整组使用并保留输入顺序"
    if any(item not in allowed_images or item not in image_mapping for item in selected_images):
        return False, "图片必须来自已采用素材，且存在真实来源映射"
    return True, ""


def get_post_usage_counts(article_manager, source, post_ids):
    """统计已成功发布文章对原帖的引用次数；输入平台和原帖 ID 序列，返回 {post_id: 次数}。
    管理器返回含 post_id_list 的文章；同一篇文章内同帖只计一次。
    """
    unique_ids = list(dict.fromkeys(post_ids))
    counts = dict.fromkeys(unique_ids, 0)
    if not unique_ids:
        return counts
    articles = article_manager.find_articles(
        query={'source': source, 'status': 'ok', 'publish_status': 'success', 'post_id_list': {'$in': unique_ids}},
        limit=0
    )
    for article in articles:
        for post_id in set(article.get('post_id_list', [])):
            if post_id in counts:
                counts[post_id] += 1
    return counts


def get_recent_openings(article_manager, source, topic, stance, limit=5):
    """取近期同主题/立场文章开头以避免重复；管理器文章含 article_info.text，返回字符串列表。
    只要求生成成功，不要求发布成功；沿用去重前限制查询条数的规则。
    """
    if limit <= 0:
        return []
    articles = article_manager.find_articles(
        query={'source': source, 'topic': topic, 'stance': stance, 'status': 'ok'},
        sort=[('created_at', -1)],
        limit=limit
    )
    openings = []
    for article in articles:
        text = (article.get('article_info') or {}).get('text', '')
        if isinstance(text, str) and text.strip():
            opening = text.strip().splitlines()[0][:80]
            if opening not in openings:
                openings.append(opening)
    return openings


def prepare_article_record_for_db(article_data):
    """校验入库数据并从实际采用素材推导原帖 ID；返回补齐默认字段的浅拷贝。
    输入含 source/topic/stance/status；ok 另需 article_info.used_material_ids 和
    material_post_mapping={素材ID: 原帖ID}；输出追加去重的 post_id_list。
    """
    record = dict(article_data)
    for key in ('source', 'topic', 'stance'):
        if not isinstance(record.get(key), str) or not record[key].strip():
            raise ValueError(f"文章缺失有效字段: {key}")

    status = record.get('status')
    if status not in ('processing', 'ok', 'skip', 'error'):
        raise ValueError(f"不支持的文章状态: {status}")

    post_id_list = []
    if status == 'ok':
        article_info = record.get('article_info')
        material_post_mapping = record.get('material_post_mapping')
        if not isinstance(article_info, dict) or article_info.get('status') != 'ok':
            raise ValueError("成功记录必须包含 status=ok 的 article_info")
        if not isinstance(material_post_mapping, dict):
            raise ValueError("成功记录必须包含 material_post_mapping")
        used_material_ids = article_info.get('used_material_ids')
        if not isinstance(used_material_ids, list) or not used_material_ids:
            raise ValueError("成功文章的 used_material_ids 不能为空")
        for material_id in used_material_ids:
            if not isinstance(material_id, str) or material_id not in material_post_mapping:
                raise ValueError(f"实际采用素材无法反查原帖: {material_id!r}")
            post_id = material_post_mapping[material_id]
            if (not isinstance(post_id, (str, int)) or isinstance(post_id, bool)
                    or post_id in (None, '', 'UNKNOWN')):
                raise ValueError(f"素材对应的原帖 ID 无效: {material_id}")
            if post_id not in post_id_list:
                post_id_list.append(post_id)

    record['post_id_list'] = post_id_list
    record.setdefault('article_info', None)
    record.setdefault('error_message', None)
    record.setdefault('error_history', [])
    record.setdefault('raw_response', None)
    record.setdefault('attempt_count', 0)
    return record


def build_article_task(coin, stance, ev_list, article_manager):
    """从论据列表生成 brief；论据含 source_post_id/core_fact/dimension/shelf_life/impact_weight。
    返回 {is_skip, record, coin, stance, reason?}；可执行任务另含
    full_prompt/materials/image_mapping/model_name，record 保留生成与错误追踪字段。
    """
    brief = {
        "task": {"topic": coin, "stance": stance, "max_chars": ARTICLE_MAX_CHARS, "recent_openings": []},
        "materials": [],
    }
    # : 此字段仅保留历史记录契约；实际调用 medium 模型组，不能据此认定实际模型。
    model_name = "gemini-3.1-pro-preview"
    record = {
        "source": BINANCE_SOURCE, "topic": coin, "stance": stance, "status": "processing",
        "creation_brief": brief, "material_post_mapping": {}, "post_id_list": [],
        "prompt_file_path": ARTICLE_PROMPT_FILE_PATH, "model_name": model_name,
        "prompt_version": "0920v1.0", "article_info": None, "error_message": None,
        "error_history": [], "raw_response": None, "attempt_count": 0,
    }
    candidates = [
        item for item in ev_list
        if isinstance(item, dict) and isinstance(item.get("source_post_id"), (str, int))
        and not isinstance(item.get("source_post_id"), bool)
        and item["source_post_id"] not in ("", "UNKNOWN")
        and isinstance(item.get("core_fact"), str) and item["core_fact"].strip()
    ]
    usage_counts = get_post_usage_counts(
        article_manager, BINANCE_SOURCE, list(dict.fromkeys(item["source_post_id"] for item in candidates)),
    )
    groups = defaultdict(list)
    for item in candidates:
        if usage_counts.get(item["source_post_id"], 0) < ARTICLE_POST_USAGE_LIMIT:
            groups[(item.get("dimension"), item.get("shelf_life"))].append(item)
    groups = {
        key: deque(sorted(items, key=lambda item: (item.get("impact_weight", 0), random.random()), reverse=True))
        for key, items in groups.items()
    }
    selected = []
    while groups and len(selected) < ARTICLE_MATERIAL_LIMIT:
        keys = sorted(groups, key=lambda key: (groups[key][0].get("impact_weight", 0), random.random()), reverse=True)
        for key in keys:
            if len(selected) >= ARTICLE_MATERIAL_LIMIT:
                break
            selected.append(groups[key].popleft())
            if not groups[key]:
                del groups[key]
    if len(selected) < ARTICLE_MIN_MATERIALS:
        reason = (f"可用论据不足 {ARTICLE_MIN_MATERIALS} 条（当前 {len(selected)} 条）；"
                  f"需检查论据有效性及原帖已发布引用次数是否小于 {ARTICLE_POST_USAGE_LIMIT}。")
        record["status"] = "skip"
        record["article_info"] = {
            "status": "skip", "text": "", "image_placeholders": [], "used_material_ids": [],
            "score": None, "reason": reason, "image_mapping": {},
        }
        return {"is_skip": True, "record": record, "coin": coin, "stance": stance, "reason": reason}
    materials, image_mapping = transform_mlus(selected)
    brief["materials"] = materials
    brief["task"]["recent_openings"] = get_recent_openings(article_manager, BINANCE_SOURCE, coin, stance)
    record["material_post_mapping"] = {
        material["id"]: evidence["source_post_id"] for material, evidence in zip(materials, selected)
    }
    prompt = read_file_to_str(ARTICLE_PROMPT_FILE_PATH)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"文章提示词为空或读取失败: {ARTICLE_PROMPT_FILE_PATH}")
    return {
        "is_skip": False, "record": record, "full_prompt": f"{prompt}\n{json.dumps(brief, ensure_ascii=False)}",
        "materials": materials, "image_mapping": image_mapping,
        "coin": coin, "stance": stance, "model_name": model_name,
    }


def execute_article_task(task_data, article_manager):
    """执行 build_article_task 返回的任务，返回含 status/article_info/error_history 的记录。
    只保存 ok；保留模型重试耗尽返回错误记录、数据库保存异常上抛的约定。
    """
    record = task_data["record"]
    coin, stance = task_data["coin"], task_data["stance"]
    if task_data.get("is_skip"):
        logger.info("[文章/跳过] 选材不足 | 币种: [%s] | 立场: [%s] | 原因: [%s]",
                    coin, stance, task_data["reason"])
        return record
    started = time.monotonic()
    image_mapping = task_data["image_mapping"]
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        record["attempt_count"], record["raw_response"] = attempt, None
        try:
            result = generate_content(prompt=task_data["full_prompt"], preset_model_group="medium")
            record["raw_response"] = result.get("content", "")
            article_info = string_to_object(record["raw_response"])
            valid, error = check_article_info(article_info, task_data["materials"], image_mapping, ARTICLE_MAX_CHARS)
            if not valid:
                raise ValueError(error)
            article_info["image_mapping"] = {
                placeholder: dict(image_mapping[placeholder]) for placeholder in article_info["image_placeholders"]
            }
            record["article_info"], record["status"] = article_info, article_info["status"]
            record["error_message"] = None
            break
        except Exception as exc:
            error_message = f"{type(exc).__name__}: {exc}"
            record["error_history"].append({
                "attempt": attempt, "error_message": error_message, "raw_response": record["raw_response"],
            })
            if attempt == LLM_MAX_RETRIES:
                record.update(status="error", article_info=None, error_message=error_message)
                logger.exception(
                    "❌ [文章/生成] 重试耗尽 | 币种: [%s] | 立场: [%s] | 尝试: [%s/%s] "
                    "| 原因: [%s] | 后续: [返回错误记录，不入库] "
                    "| 排查: [检查模型服务、返回字段及素材引用]",
                    coin, stance, attempt, LLM_MAX_RETRIES, exc,
                )
                return record
            logger.warning(
                "[文章/生成] 模型调用或校验失败 | 币种: [%s] | 立场: [%s] | 尝试: [%s/%s] "
                "| 原因: [%s] | 后续: [%s 秒后重试] | 排查: [检查模型服务、返回字段及素材引用]",
                coin, stance, attempt, LLM_MAX_RETRIES, exc, 2 ** attempt,
            )
            time.sleep(2 ** attempt)
    if record["status"] != "ok":
        if record["status"] == "skip":
            logger.info("[文章/跳过] 模型未生成文章 | 币种: [%s] | 立场: [%s] | 原因: [%s]",
                        coin, stance, (record.get("article_info") or {}).get("reason", "未说明"))
        return record
    saved_record = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved_record])
    logger.info(
        "[文章/完成] 已校验并入库 | 币种: [%s] | 立场: [%s] | 尝试: [%s] | 耗时: [%.2f 秒]",
        coin, stance, record["attempt_count"], time.monotonic() - started,
    )
    return saved_record


def generate_analysis_articles_once():
    """每个币种/立场构建多份任务后并发执行，返回正常完成任务的记录列表。
    单任务异常仍隔离；汇总另外统计抛异常的任务，避免漏报失败。
    """
    started = time.monotonic()
    grouped = extract_and_group_valid_evidences()
    groups = [(coin, stance, items) for coin, stances in grouped.items() for stance, items in stances.items()]
    article_manager = GeneratedArticleManager(gen_db_object())
    # : 仍在执行前构建全部任务；未为未发布文章或本轮并发任务预占原帖引用次数。
    tasks = [build_article_task(coin, stance, items, article_manager)
             for coin, stance, items in groups for _ in range(ARTICLE_VARIANTS)]
    local_skips = sum(bool(task.get("is_skip")) for task in tasks)
    logger.info(
        "[文章/计划] 选材完成 | 币种: [%s] | 分组: [%s] | 任务: [%s] | 本地跳过: [%s] "
        "| 模型调用上限: [%s，含重试] | 选材: [已发布引用次数 < %s，素材 %s—%s 条]",
        len(grouped), len(groups), len(tasks), local_skips,
        (len(tasks) - local_skips) * LLM_MAX_RETRIES, ARTICLE_POST_USAGE_LIMIT,
        ARTICLE_MIN_MATERIALS, ARTICLE_MATERIAL_LIMIT,
    )
    results, raised_count, model_skips = [], 0, 0
    if tasks:
        with ThreadPoolExecutor(max_workers=ARTICLE_CONCURRENCY) as executor:
            futures = {executor.submit(execute_article_task, task, article_manager): task for task in tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    model_skips += result.get("status") == "skip" and not task.get("is_skip")
                except Exception:
                    raised_count += 1
                    logger.exception(
                        "❌ [文章/执行] 当前任务异常结束 | 币种: [%s] | 立场: [%s] "
                        "| 后续: [继续其他任务] | 排查: [检查任务字段及数据库；保存失败时先核对入库结果]",
                        task["coin"], task["stance"],
                    )
    counts = Counter(item.get("status") for item in results)
    logger.info(
        "[文章/完成] 本批任务结束 | 任务: [%s] | 成功: [%s] | 失败: [%s] "
        "| 跳过: [%s，本地 %s / 模型 %s] | 耗时: [%.2f 秒]",
        len(tasks), counts["ok"], counts["error"] + raised_count, counts["skip"],
        local_skips, model_skips, time.monotonic() - started,
    )
    return results


def generate_analysis_articles():
    """生成后台任务：有返回记录时等待 600 秒，无记录或异常时等待 60 秒。"""
    while True:
        try:
            results = generate_analysis_articles_once()
            time.sleep(ARTICLE_GENERATION_INTERVAL_SECONDS if results else 60)
        except Exception:
            logger.exception(
                "❌ [文章/轮询] 本轮中断 | 后续: [60 秒后重新查询] "
                "| 排查: [检查热榜、论据结构、提示词及数据库；保存结果不确定时先核对记录]"
            )
            time.sleep(60)


def add_hot_topic_to_article(text, topic, tag_count=2):
    """追加热门话题和币种标签，返回新正文；text/topic 为字符串。"""
    # : 保留 tag_count 未参与选择、仅取首个热话题的规则；标签不去重、不重新限长。
    hashtags = [item["hashtag"] for item in fetch_binance_hot_hashtags()]
    tags = hashtags[:1] + [f"#{topic}", f"${topic} "]
    hot_coins = fetch_binance_future_hot_coins()
    if hot_coins:
        symbol = hot_coins[0]["symbol"].replace("USDT", "").replace("USDC", "")
        tags.extend([f"#{symbol}", f"${symbol} "])
    logger.debug("[文章/标签] 标签已准备 | 主题: [%s] | 标签: [%s]", topic, tags)
    return text + "\n" + "\n".join(tags)


def _publish_articles_once(article_manager):
    """调度并发布一轮；管理器返回含 topic/article_info/publish_attempts 的文章列表。
    article_info 含 text/score/image_placeholders/image_mapping；回写账号状态及发布字段，无返回。
    """
    now = time.time()
    state = (read_json(STATE_FILE) or {}) if os.path.exists(STATE_FILE) else {}
    for account in ACCOUNTS:
        account_state = state.setdefault(account, {})
        for key, value in {
            "total_success": 0, "last_publish_time": 0, "last_error_msg": "",
            "last_error_time": 0, "topic_publish_history": {},
        }.items():
            account_state.setdefault(key, value)
    candidates = article_manager.find_articles(query={
        "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=6)},
        "status": "ok", "publish_status": {"$ne": "success"},
    }) or []
    hot_coins = {str(coin).lower() for coin in (get_hot_coin() or [])}
    # : 原实现要求至少一张图，与旧注释“无图”相反；按实际筛选保留含图要求。
    # : 次数限制只在本轮入口检查，失败文章可被后续账号继续尝试，单轮可能超过上限。
    articles = [
        article for article in candidates
        if isinstance(article.get("article_info", {}), dict)
        and len(article.get("article_info", {}).get("image_placeholders", [])) >= 1
        and article.get("publish_attempts", 0) < PUBLISH_MAX_ATTEMPTS
        and (article.get("topic") or "").lower() in hot_coins
    ]
    articles.sort(key=lambda item: item.get("article_info", {}).get("score", 0), reverse=True)
    logger.info(
        "[发布/计划] 近 6 小时含图文章已按评分排序 | 查询: [%s] | 候选: [%s] "
        "| 配置账号: [%s] | 账号冷却: [%s 秒] | 同账号同主题冷却: [%s 秒]",
        len(candidates), len(articles), len(ACCOUNTS),
        ACCOUNT_COOLDOWN_SECONDS, ACCOUNT_TOPIC_COOLDOWN_SECONDS,
    )
    for account in ACCOUNTS:
        if not articles:
            break
        account_state = state[account]
        remaining = ACCOUNT_COOLDOWN_SECONDS - (now - account_state.get("last_publish_time", 0))
        if remaining > 0:
            logger.debug("[发布/账号] 仍在冷却 | 账号: [%s] | 剩余: [%s 秒]", account, int(remaining))
            continue
        # : 同主题冷却按原始 topic 区分大小写；计时仍统一使用本轮开始时间。
        selected_index = next((
            index for index, article in enumerate(articles)
            if now - account_state["topic_publish_history"].get(article.get("topic", ""), 0)
            >= ACCOUNT_TOPIC_COOLDOWN_SECONDS
        ), None)
        if selected_index is None:
            logger.debug("[发布/账号] 所有候选主题仍在冷却 | 账号: [%s]", account)
            continue
        article = articles[selected_index]
        info, topic = article.get("article_info", {}), article.get("topic", "")
        started = time.monotonic()
        stage, api_result = "准备正文、媒体及账号配置", "尚未调用"
        try:
            text = info.get("text", "")
            if topic:
                text = re.sub(rf"(?<!\$)\b{re.escape(topic)}\b", lambda match: "$" + topic + " ",
                              text, flags=re.IGNORECASE)
            text = add_hot_topic_to_article(text, topic) + "\n\n👇"
            # : 沿用完整 image_mapping 的插入顺序，不按占位符再次筛选；缺失路径传 None。
            image_paths = [mapping.get("local_path") or None for mapping in info.get("image_mapping", {}).values()]
            user_data_dir = get_config(f"{account}_browser_session_dir")
            attempts = article.get("publish_attempts", 0) + 1
            stage, api_result = "调用发布接口", "未知"
            err, success, post_id = create_binance_post(
                content=text, image_path_list=image_paths, user_data_dir=user_data_dir,
                chart_info={"coin": topic, "bridge": "USDT", "type": "future"},
            )
            api_result = "成功" if success else "失败"
            if success:
                account_state["total_success"] += 1
                account_state["last_publish_time"] = now
                if topic:
                    account_state["topic_publish_history"][topic] = now
                changes = {
                    "publish_status": "success", "published_by": account, "publish_time": now,
                    "binance_post_id": post_id, "publish_attempts": attempts,
                }
            else:
                error = f"发布接口未确认成功；可能是会话失效、网络异常、限流或内容校验失败；详情: {err}"
                account_state["last_error_msg"], account_state["last_error_time"] = error, now
                changes = {"publish_status": "failed", "last_error": error, "publish_attempts": attempts}
            # : 保留先文件后数据库的非事务写入；接口抛异常不累计次数，回写失败可能导致重复发布。
            stage = "写入账号状态文件"
            save_json(STATE_FILE, state)
            stage = "回写数据库发布状态"
            article.update(changes)
            article_manager.upsert_articles([article])
        except Exception as exc:
            raise RuntimeError(
                f"发布阶段[{stage}]失败 | 账号: [{account}] | 文章: [{article.get('_id')}] "
                f"| 主题: [{topic}] | 接口结果: [{api_result}]；请核对远端结果与本地状态"
            ) from exc
        if success:
            articles.pop(selected_index)
        log = logger.info if success else logger.error
        log(
            "%s[发布/文章] 本次处理结束 | 账号: [%s] | 主题: [%s] | 文章: [%s] "
            "| 评分: [%s] | 尝试: [%s/%s] | 耗时: [%.2f 秒] | 结果: [%s] | 说明: [%s]",
            "" if success else "❌ ", account, topic, article.get("_id"), info.get("score"),
            attempts, PUBLISH_MAX_ATTEMPTS, time.monotonic() - started, api_result,
            "远端发布及本地状态回写完成" if success else error,
        )


def auto_publish_articles():
    """发布后台任务：独立管理器，启动后等 60 秒，每轮结束后等 600 秒。"""
    article_manager = GeneratedArticleManager(gen_db_object())
    time.sleep(60)
    while True:
        try:
            _publish_articles_once(article_manager)
        except Exception:
            logger.exception(
                "❌ [发布/轮询] 本轮中断 | 后续: [%s 秒后重新扫描] "
                "| 排查: [检查账号状态、配置、发布接口及数据库；远端成功时先核对回写结果]",
                PUBLISH_POLL_SECONDS,
            )
        finally:
            time.sleep(PUBLISH_POLL_SECONDS)



def build_search_text(post):
    """生成搜索文本；post.content 含 text_content/mentioned_coins。
    此入口的 logic_mul 是媒体列表，项含 visual_fact、semantic_core、narrative_role；返回字符串。
    """
    content = post.get("content") or {}
    text = MEDIA_PATTERN.sub("", content.get("text_content") or "").strip()
    coins = content.get("mentioned_coins") or []
    coins_text = ", ".join(coins) if coins else "无"
    parts = [f"【文章正文】\n{text}\n关联币种：{coins_text}\n"]
    for index, media in enumerate(post.get("logic_mul") or [], 1):
        visual = media.get("visual_fact", {})
        semantic = media.get("semantic_core", {})
        narrative = media.get("narrative_role", {})
        concepts = ", ".join(semantic.get("concepts", []) + visual.get("entities", []))
        logic = f"{semantic.get('message', '')} {narrative.get('logic_bridge', '')}".strip()
        parts.append(
            f"\n【配图{index}语义解析】\n核心概念：{concepts}\n"
            f"画面描述：{visual.get('description', '')}\n图文逻辑：{logic}\n"
            f"关键数据(OCR)：{visual.get('ocr_text', '')}\n"
        )
    return "".join(parts).strip()


def process_posts(post_list):
    """导出旧版图文块；输入 post 列表，logic_mul.images 含 visual_fact/semantic_core。
    返回 [{doc_id, content: [文本块或含 type/desc/ocr/logic/placeholder 的媒体块]}]。
    """
    results = []
    for index, post in enumerate(post_list, 1):
        doc_id = f"d_{index}"
        text = (post.get("content") or {}).get("text_content") or ""
        images = (post.get("logic_mul") or {}).get("images", [])
        blocks, counters, last_end = [], {"IMAGE": 0, "VIDEO": 0}, 0
        for match in MEDIA_PATTERN.finditer(text):
            before = text[last_end:match.start()].strip()
            if before:
                blocks.append({"type": "text", "text": before})
            kind = "VIDEO" if match.group(1) == "视频" else "IMAGE"
            image = images[counters["IMAGE"]] if kind == "IMAGE" and counters["IMAGE"] < len(images) else {}
            counters[kind] += 1
            visual = image.get("visual_fact", {})
            prefix = "VID" if kind == "VIDEO" else "IMG"
            blocks.append({
                "type": "video" if kind == "VIDEO" else "image",
                "desc": visual.get("fact", ""), "ocr": visual.get("ocr_text", ""),
                "logic": image.get("semantic_core", {}).get("message", ""),
                "placeholder": f"[{prefix}_{doc_id}_{kind}_{counters[kind]:02d}]",
            })
            last_end = match.end()
        remaining = text[last_end:].strip()
        if remaining:
            blocks.append({"type": "text", "text": remaining})
        results.append({"doc_id": doc_id, "content": blocks})
    return results


def get_all_non_empty_logic_mul_with_clean_text():
    """导出非空元数据和清洗正文；返回 [{text_content, logic_mul}]，含图记录稳定优先。"""
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    results = [
        {
            "text_content": MEDIA_PATTERN.sub("", (post.get("content") or {}).get("text_content") or "").strip(),
            "logic_mul": post["logic_mul"],
        }
        for post in posts if post.get("logic_mul")
    ]

    def has_images(data):
        """保留旧版判据：非空媒体列表、顶层 images 或 evidences.images 均视为含图。"""
        if isinstance(data, list):
            return bool(data)
        if not isinstance(data, dict):
            return False
        images = data.get("images")
        if isinstance(images, list) and images:
            return True
        evidences = data.get("evidences")
        return isinstance(evidences, list) and any(
            isinstance(item, dict) and isinstance(item.get("images"), list) and bool(item["images"])
            for item in evidences
        )

    results.sort(key=lambda item: not has_images(item["logic_mul"]))
    logger.info(
        "[数据/导出] 已清洗正文并按含图优先排序 | 扫描: [%s] | 导出: [%s]",
        len(posts), len(results),
    )
    return results


def _run_task(task):
    """为未处理异常补充任务上下文后重抛；保留线程退出、不自动重启的行为。"""
    try:
        task()
    except Exception:
        logger.exception(
            "❌ [任务/退出] 后台任务异常结束 | 任务: [%s] | 结果: [当前线程停止] "
            "| 排查: [检查对应链路的数据、文件权限及外部服务]", task.__name__,
        )
        raise


def _hudong_once(article_manager):
    """对近 24 小时已发布且未互动的文章点赞收藏；文章含 binance_post_id/interacted。
    凭证为 {account: {cookies, csrf_token}}；返回 None，保留账号隔离及接口/回写失败返回策略。
    """
    started = time.monotonic()
    credentials = {}
    for account in ACCOUNTS:
        try:
            session_dir = get_config(f"{account}_browser_session_dir")
            cookies, csrf_token, _ = get_auth_tokens_robust(session_dir)
            if not cookies or not csrf_token:
                logger.warning(
                    "[互动/账号] 凭证不完整 | 账号: [%s] | 后续: [跳过当前账号] "
                    "| 排查: [检查浏览器会话是否有效以及登录状态]", account,
                )
                continue
            credentials[account] = {"cookies": cookies, "csrf_token": csrf_token}
        except Exception:
            logger.exception(
                "❌ [互动/账号] 获取凭证失败 | 账号: [%s] | 后续: [继续其他账号] "
                "| 排查: [检查会话目录、浏览器启动及登录状态]", account,
            )
    if not credentials:
        logger.warning(
            "[互动/跳过] 无可用账号凭证 | 账号数: [%s] | 排查: [先恢复账号登录与浏览器会话]",
            len(ACCOUNTS),
        )
        return
    now = datetime.now(timezone.utc)
    articles = article_manager.find_articles(query={
        "status": "ok", "publish_status": "success",
        "publish_time": {"$gte": (now - timedelta(hours=24)).timestamp()},
    }) or []
    pending = [article for article in articles if not article.get("interacted")]
    post_ids = [article.get("binance_post_id") for article in pending if article.get("binance_post_id")]
    if not post_ids:
        logger.info("[互动/完成] 没有可提交的帖子 | 待处理记录: [%s] | 耗时: [%.2f 秒]",
                    len(pending), time.monotonic() - started)
        return
    logger.info("[互动/计划] 已匹配待互动帖子 | 帖子: [%s] | 账号: [%s]", len(post_ids), len(credentials))
    try:
        like_and_bookmark(post_ids, credentials)
    except Exception:
        logger.exception(
            "❌ [互动/执行] 点赞收藏接口异常 | 帖子: [%s] | 后续: [不标记，等待下轮] "
            "| 排查: [检查凭证、网络及限流；已完成的远端操作可能被重试]", len(post_ids),
        )
        return
    for article in pending:
        article["interacted"], article["interaction_time"] = True, now.timestamp()
    try:
        article_manager.upsert_articles(pending)
    except Exception:
        logger.exception(
            "❌ [互动/回写] 接口已返回，但状态保存失败 | 记录: [%s] | 后续: [结束本轮] "
            "| 排查: [检查数据库；远端可能已完成互动，重试前核对结果]", len(pending),
        )
        return
    logger.info(
        "[互动/完成] 接口已返回且状态已保存 | 提交帖子: [%s] | 标记记录: [%s] | 耗时: [%.2f 秒]",
        len(post_ids), len(pending), time.monotonic() - started,
    )


def hudong():
    """互动后台任务：每轮结束后等待 3600 秒，沿用异常后继续轮询的策略。"""
    article_manager = GeneratedArticleManager(gen_db_object())
    while True:
        try:
            _hudong_once(article_manager)
        except Exception:
            logger.exception(
                "❌ [互动/轮询] 本轮中断 | 后续: [%s 秒后重试] "
                "| 排查: [检查互动数据、账号配置、网络及数据库连接]", INTERACTION_INTERVAL_SECONDS,
            )
        finally:
            time.sleep(INTERACTION_INTERVAL_SECONDS)


if __name__ == "__main__":
    tasks = [
        generate_analysis_articles,
        format_image_article,
        hudong,
        auto_publish_articles
    ]
    threads = [threading.Thread(target=_run_task, args=(task,), name=task.__name__) for task in tasks]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()