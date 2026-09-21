"""
[功能摘要] 并行运行帖子论据提取、分析文章生成和币安广场自动发布三条任务。
[输入数据] MongoDB 帖子含 post_id、publish_time、content.text_content、
media.local_mapping；生成文章含 topic、stance、article_info；另读取提示词、配置和账号状态文件。
[数据流转/交互] 原帖占位符规范化 → Gemini 提取 evidences → 校验后回写 logic_mul；
论据按时效、币种、立场和权重分组 → 按引用次数选材 → Gemini 生成文章 → 校验后入库；
近 6 小时未成功发布的无图文章 → 评分排序、账号及主题冷却检查 → 发布接口 → 状态回写。
[输出数据] 更新帖子元数据、保存成功生成的文章、发布文字内容，并维护数据库和本地账号状态。
"""

import json
import math
import os
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from app.ai_api.gemini_playwright import generate_gemini_content_playwright
from biance.biance_squre_api import publish_to_binance_square
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
MAX_CONCURRENCY = 3
LLM_MAX_RETRIES = 3
PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类MLU提取.txt"
ARTICLE_PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类文章生成.txt"
ARTICLE_MATERIAL_LIMIT = 20
ARTICLE_POST_USAGE_LIMIT = 5
ARTICLE_MAX_CHARS = 260
ARTICLE_GENERATION_INTERVAL_SECONDS = 600
STATE_FILE = "account_publish_state.json"
ACCOUNTS = ("yang", "ruru")
ACCOUNT_COOLDOWN_SECONDS = 3600
TOPIC_COOLDOWN_SECONDS = 12 * 3600
PUBLISH_POLL_SECONDS = 600
MEDIA_PATTERN = re.compile(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]")
SHELF_LIFE_SECONDS = {
    "hours": 24 * 3600,
    "days": 7 * 24 * 3600,
    "weeks": 30 * 24 * 3600,
    "long": 30 * 24 * 3600,
    "unknown": 24 * 3600,
}

# : 数据库封装未提供关闭协议，保留原初始化方式；
# 连接释放需在 gen_db_object/Manager 的既有实现中确认，不能猜测其 close/client 接口。


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
    local_paths = list((media.get("local_mapping") or {}).values())
    # : 原逻辑只检查路径非空，不检查文件存在，也不核对正文 URL 是否全部映射。
    if any(not path for path in local_paths):
        return False
    # : 保留 >= 10 即拒绝的规则，实际最多 9 个映射；纯文本仍允许进入。
    if len(local_paths) >= MAX_MEDIA_COUNT:
        return False
    publish_time = _publish_time_seconds(post.get("publish_time", 0))
    # : 此处不限制未来时间；下游论据筛选另有“最多超前 1 小时”的规则。
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
    """由原帖和本地媒体提取 {evidences: [...]}；按原约定，重试耗尽返回 {}。
    post 含 post_id、content.text_content、media.local_mapping。
    """
    text, paths, mapping = normalize_post_media(post)
    full_prompt = f"{read_file_to_str(PROMPT_FILE_PATH)}\n{text}"
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        raw_response, error_detail = "", ""
        try:
            error_detail, raw_response = generate_gemini_content_playwright(
                full_prompt, file_path=paths
            )
            # : 保留此阶段只按响应正文判断成功的行为；error_detail 非空是否必须失败待确认。
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
                "[论据/提取] 模型调用或结果校验失败 | 帖子: [%s] | 尝试: [%s/%s] "
                "| 结果: [%s] | 原因: [%s] | 接口信息: [%s] | 响应摘要: [%s] "
                "| 排查: [检查媒体读取、模型服务及提示词字段要求]",
                post.get("post_id", "UNKNOWN_ID"), attempt, LLM_MAX_RETRIES,
                "返回空数据，交由后续轮询重试" if exhausted else f"{delay} 秒后重试",
                exc, error_detail, repr(raw_response)[:500], exc_info=exhausted,
            )
            if exhausted:
                return {}
            time.sleep(delay)
    return {}


def process_and_save_single_post(post, post_manager):
    """处理并回写一条 post.logic_mul；post_manager 沿用 upsert_posts([post])。
    保留单帖失败隔离及无返回值约定，前置筛选异常也必须可见。
    """
    started = time.monotonic()
    post_id = post.get("post_id", "UNKNOWN_ID") if isinstance(post, dict) else "UNKNOWN_ID"
    try:
        # 注意：过滤逻辑已前置到 format_image_article 集中处理，此处直接进入提取环节
        format_info = gen_media_format_info(post)
        if not format_info:
            return
        post["logic_mul"] = format_info
        post_manager.upsert_posts([post])
        logger.info(
            "[帖子/格式化] 论据校验通过并完成回写 | 帖子: [%s] | 耗时: [%.2f 秒]",
            post_id, time.monotonic() - started,
        )
    except Exception:
        # 原设计允许单帖失败而不终止本批次。
        logger.exception(
            "[帖子/格式化] 当前帖子处理失败 | 帖子: [%s] | 结果: [结束本帖任务] "
            "| 排查: [检查帖子字段、提示词/媒体路径及数据库连接]",
            post_id,
        )


def format_image_article():
    """轮询原帖并以 5 个工作线程提取论据；失败按原策略等待后重试。"""
    while True:
        try:
            started = time.monotonic()
            post_manager = UniversalPostManager(gen_db_object())
            posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
            if not posts:
                time.sleep(60)
                continue

            # 集中处理前置筛选，以便运维掌握运行全貌
            valid_posts = [post for post in posts if is_need_formatting(post)]

            logger.info(
                "[帖子/本轮计划] 已完成格式化前置筛选 | 扫描总帖数: [%s] | 需提取论据: [%s] | 已跳过: [%s] "
                "| 实际分配线程数: [%s] | 规则: [无 logic_mul、非视频且不超过允许数量的本地图片、%s 小时内]",
                len(posts), len(valid_posts), len(posts) - len(valid_posts),
                min(MAX_CONCURRENCY, len(valid_posts)) if valid_posts else 0, POST_MAX_AGE_HOURS
            )

            if valid_posts:
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                    futures = [
                        executor.submit(process_and_save_single_post, post, post_manager)
                        for post in valid_posts
                    ]
                    for future in as_completed(futures):
                        future.result()

            # : 原查询未分页；满额仅缩短等待，不能保证后续记录会被扫描。
            delay = 5 if len(posts) >= POST_QUERY_LIMIT else 3600
            logger.info(
                "[帖子/本轮完成] 工作线程已结束 | 扫描: [%s] | 实际处理: [%s] | 耗时: [%.2f 秒] "
                "| 下次扫描: [%s 秒后] | 单帖失败: [见对应帖子日志]",
                len(posts), len(valid_posts), time.monotonic() - started, delay,
            )
            time.sleep(delay)
        except Exception:
            logger.exception(
                "[帖子/轮询] 本轮中断 | 结果: [60 秒后重新初始化数据库并重试] "
                "| 排查: [数据库访问或工作线程出现异常]"
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
    """按有效期筛选、按币种/立场分组并按权重降序排序。
    返回 {coin: {stance: [论据]}}；论据追加 source_post_id、publish_time、
    source_text_content、source_image_mapping，支持脱离原帖使用。
    """
    started = time.monotonic()
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    grouped = defaultdict(lambda: defaultdict(list))
    now, invalid_times = time.time(), 0
    for post in posts:
        logic_mul = post.get("logic_mul")
        if not isinstance(logic_mul, dict) or not logic_mul.get("evidences"):
            continue
        raw_time = post.get("publish_time")
        if not raw_time:
            continue
        try:
            publish_time = _publish_time_seconds(raw_time)
        except ValueError:
            invalid_times += 1
            continue
        age = now - publish_time
        if age > 30 * 24 * 3600 or age < -3600:
            continue
        source_mapping = build_source_image_mapping(post)
        for evidence in logic_mul["evidences"]:
            duration = SHELF_LIFE_SECONDS.get(evidence.get("shelf_life", "unknown"), 24 * 3600)
            if age > duration:
                continue
            coins, stance = evidence.get("coins", []), evidence.get("stance")
            if not isinstance(coins, list) or not coins or not stance:
                continue
            enriched = {
                **evidence,
                "source_post_id": post.get("post_id", "UNKNOWN"),
                "publish_time": publish_time,
                "source_text_content": (post.get("content") or {}).get("text_content", ""),
                "source_image_mapping": source_mapping,
            }
            for coin in coins:
                coin = str(coin).strip().upper()
                if coin:
                    grouped[coin][stance].append(enriched)
    for stances in grouped.values():
        for evidences in stances.values():
            evidences.sort(key=lambda item: item.get("impact_weight", 0), reverse=True)
    # 保留一次 JSON 转换，同时维持跨组副本独立和原有序列化行为。
    result = json.loads(json.dumps(grouped))
    logger.info(
        "[论据/筛选] 已完成时效筛选、分组与权重排序 | 扫描: [%s] | 币种: [%s] "
        "| 论据条目: [%s，含跨组重复] | 无效时间跳过: [%s] | 耗时: [%.2f 秒]",
        len(posts), len(result),
        sum(len(items) for stances in result.values() for items in stances.values()),
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
    if type(article_info["score"]) is not int or not 0 <= article_info["score"] <= 10:
        return False, "score 必须是 0—10 的整数"
    if article_info["reason"] is not None:
        return False, "ok 的 reason 必须为 null"
    material_by_id = {item["id"]: item for item in materials}
    used_ids, selected_images = article_info["used_material_ids"], article_info["image_placeholders"]
    if not 1 <= len(used_ids) <= 3 or any(item not in material_by_id for item in used_ids):
        return False, "used_material_ids 必须引用实际存在的 1—3 条素材"
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
    """统计同平台的成功文章引用次数，不限币种/立场；每篇文章内同帖最多计一次。"""
    unique_ids = list(dict.fromkeys(post_ids))
    counts = dict.fromkeys(unique_ids, 0)
    if not unique_ids:
        return counts
    articles = article_manager.find_articles(
        query={'source': source, 'status': 'ok', 'post_id_list': {'$in': unique_ids}},
        limit=0
    )
    for article in articles:
        for post_id in set(article.get('post_id_list', [])):
            if post_id in counts:
                counts[post_id] += 1
    return counts


def get_recent_openings(article_manager, source, topic, stance, limit=5):
    """取同币种、同立场近期成功文章的开头，供提示词避开重复表达。"""
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
    """文章入库前的数据校验与字段推导（从实际采用素材推导 post_id_list）。"""
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


# =========================================================================
# 待修改的原有逻辑函数 (更新为调用上面的封装以及 Manager 提供的新接口)
# =========================================================================

def generate_and_save_analysis_article(coin, stance, ev_list, article_manager):
    """为一个币种/立场生成文章；ev_list 为已按权重排序的增强论据。"""
    started = time.monotonic()
    brief = {
        "task": {"topic": coin, "stance": stance, "max_chars": ARTICLE_MAX_CHARS, "recent_openings": []},
        "materials": [],
    }
    model_name = "gemini-3.1-pro-preview"
    record = {
        "source": BINANCE_SOURCE, "topic": coin, "stance": stance, "status": "processing",
        "creation_brief": brief, "material_post_mapping": {},
        "prompt_file_path": ARTICLE_PROMPT_FILE_PATH, "post_id_list": [],
        "article_info": None, "error_message": None, "error_history": [],
        "raw_response": None, "model_name": model_name, "prompt_version": "0920v1.0",
        "attempt_count": 0,
    }
    try:
        candidates = [
            evidence for evidence in ev_list
            if isinstance(evidence, dict)
               and isinstance(evidence.get("source_post_id"), (str, int))
               and not isinstance(evidence.get("source_post_id"), bool)
               and evidence.get("source_post_id") not in (None, "", "UNKNOWN")
               and isinstance(evidence.get("core_fact"), str) and evidence["core_fact"].strip()
        ]
        post_ids = list(dict.fromkeys(item["source_post_id"] for item in candidates))

        # [修改点 1]：调用应用层自行封装的查重逻辑
        usage_counts = get_post_usage_counts(article_manager, BINANCE_SOURCE, post_ids)

        selected = [
                       item for item in candidates
                       if usage_counts.get(item["source_post_id"], 0) <= ARTICLE_POST_USAGE_LIMIT
                   ][:ARTICLE_MATERIAL_LIMIT]

        if not selected:
            record["status"] = "skip"
            record["article_info"] = {
                "status": "skip", "text": "", "image_placeholders": [], "used_material_ids": [],
                "score": None, "reason": "本分组没有有效论据，或原帖使用次数均已超过允许阈值。",
                "image_mapping": {},
            }
            return record

        materials, image_mapping = transform_mlus(selected)
        brief["materials"] = materials

        # [修改点 2]：调用应用层自行封装的开头记录拉取逻辑
        brief["task"]["recent_openings"] = get_recent_openings(article_manager, BINANCE_SOURCE, coin, stance)

        record["material_post_mapping"] = {
            material["id"]: evidence["source_post_id"]
            for material, evidence in zip(materials, selected)
        }
        prompt = read_file_to_str(ARTICLE_PROMPT_FILE_PATH)
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError(f"文章提示词为空或读取失败: {ARTICLE_PROMPT_FILE_PATH}")
        full_prompt = f"{prompt}\n{json.dumps(brief, ensure_ascii=False)}"
        for attempt in range(1, LLM_MAX_RETRIES + 1):
            record["attempt_count"], record["raw_response"] = attempt, None
            try:
                error_detail, raw_response = generate_gemini_content_playwright(
                    full_prompt, model_name=model_name
                )
                record["raw_response"] = raw_response
                if error_detail:
                    raise RuntimeError(f"文章生成接口返回异常: {error_detail}")
                article_info = string_to_object(raw_response)
                valid, error = check_article_info(article_info, materials, image_mapping, ARTICLE_MAX_CHARS)
                if not valid:
                    raise ValueError(error)
                article_info["image_mapping"] = {
                    placeholder: dict(image_mapping[placeholder])
                    for placeholder in article_info["image_placeholders"]
                }
                record["article_info"], record["status"] = article_info, article_info["status"]
                record["error_message"] = None
                break
            except Exception as exc:
                record["error_history"].append({
                    "attempt": attempt,
                    "error_message": f"{type(exc).__name__}: {exc}",
                    "raw_response": record["raw_response"],
                })
                if attempt == LLM_MAX_RETRIES:
                    raise
                logger.warning(
                    "[文章/生成] 模型调用或文章校验失败 | 币种: [%s] | 立场: [%s] "
                    "| 尝试: [%s/%s] | 重试等待: [%s 秒] | 原因: [%s] "
                    "| 排查: [检查模型服务、返回字段及素材引用]",
                    coin, stance, attempt, LLM_MAX_RETRIES, 2 ** attempt, exc,
                )
                time.sleep(2 ** attempt)
    except Exception as exc:
        record["status"], record["article_info"] = "error", None
        record["error_message"] = f"{type(exc).__name__}: {exc}"
        logger.exception(
            "[文章/生成] 当前分组失败 | 币种: [%s] | 立场: [%s] | 尝试: [%s] "
            "| 结果: [返回错误记录，不入库] | 排查: [检查素材、提示词读取、数据库查询及模型响应]",
            coin, stance, record["attempt_count"],
        )
    if record["status"] != "ok":
        return record

    # [修改点 3]：清洗数据结构后再交由 manager 入库
    saved_record = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved_record])

    logger.info(
        "[文章/生成] 已完成校验并保存 | 币种: [%s] | 立场: [%s] | 尝试: [%s] | 耗时: [%.2f 秒]",
        coin, stance, record["attempt_count"], time.monotonic() - started,
    )
    return saved_record

def generate_analysis_articles_once():
    """串行处理所有分组，确保前组保存后再查询后组引用次数；返回文章记录列表。"""
    grouped = extract_and_group_valid_evidences()
    groups = [
        (coin, stance, evidences)
        for coin, stances in grouped.items() for stance, evidences in stances.items()
    ]
    non_empty = sum(bool(evidences) for _, _, evidences in groups)
    logger.info(
        "[文章/本轮计划] 已整理候选论据 | 币种: [%s] | 分组: [%s] | 非空分组: [%s] "
        "| 条目: [%s，含跨组重复] | 模型调用上限: [%s，含重试 %s] "
        "| 选材: [原帖使用次数 <= %s，每组前 %s 条；无素材时不调用模型]",
        len(grouped), len(groups), non_empty, sum(len(items) for _, _, items in groups),
        non_empty, non_empty * LLM_MAX_RETRIES, ARTICLE_POST_USAGE_LIMIT, ARTICLE_MATERIAL_LIMIT,
    )
    article_manager = GeneratedArticleManager(gen_db_object())
    # : 此处仅保证单实例串行；多实例共享引用次数仍需数据库锁或事务能力。
    results = [
        generate_and_save_analysis_article(coin, stance, evidences, article_manager)
        for coin, stance, evidences in groups
    ]
    logger.info(
        "[文章/本轮完成] 分组处理结束 | 总数: [%s] | 成功: [%s] | 跳过: [%s] | 失败: [%s]",
        len(results), sum(item["status"] == "ok" for item in results),
        sum(item["status"] == "skip" for item in results),
        sum(item["status"] == "error" for item in results),
    )
    return results


def generate_analysis_articles():
    """文章生成后台线程：完成后等 1 小时；无分组或本轮异常时等 60 秒。"""
    while True:
        try:
            results = generate_analysis_articles_once()
            time.sleep(ARTICLE_GENERATION_INTERVAL_SECONDS if results else 60)
        except Exception:
            logger.exception(
                "[文章/轮询] 本轮中断 | 结果: [60 秒后重新查询] "
                "| 排查: [检查论据结构及数据库读写；保存结果不确定时先核对数据库记录]"
            )
            time.sleep(60)


def _publish_articles_once(article_manager):
    """执行一轮账号调度；发布后回写。"""
    now = time.time()
    state = (read_json(STATE_FILE) or {}) if os.path.exists(STATE_FILE) else {}
    for account in ACCOUNTS:
        state.setdefault(account, {
            "total_success": 0, "last_publish_time": 0, "last_error_msg": "",
            "last_error_time": 0, "topic_publish_history": {},
        })

    query = {
        "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=6)},
        "status": "ok",
        "publish_status": {"$ne": "success"},
    }

    # [修改点 4]：调用 Manager 的通用查找接口 find_articles
    candidates = article_manager.find_articles(query=query) or []

    articles = [
        article for article in candidates
        if isinstance(article.get("article_info", {}), dict)
           and len(article.get("article_info", {}).get("image_placeholders", [])) == 0
    ]
    articles.sort(key=lambda item: item.get("article_info", {}).get("score", 0), reverse=True)
    logger.info(
        "[发布/本轮计划] 已按评分排列近 6 小时无图文章 | 候选: [%s] | 可调度账号: [%s]",
        len(articles), len(ACCOUNTS),
    )

    for account in ACCOUNTS:
        account_state = state[account]
        remaining = ACCOUNT_COOLDOWN_SECONDS - (now - account_state.get("last_publish_time", 0))
        if remaining > 0:
            logger.debug("[发布/账号] 尚在冷却 | 账号: [%s] | 剩余: [%s 秒]", account, int(remaining))
            continue
        if not articles:
            continue
        selected_index = next((
            index for index, article in enumerate(articles)
            if now - account_state["topic_publish_history"].get(article.get("topic", ""), 0)
               >= TOPIC_COOLDOWN_SECONDS
        ), None)
        if selected_index is None:
            logger.debug("[发布/账号] 候选主题均在 12 小时冷却期内 | 账号: [%s]", account)
            continue

        article = articles[selected_index]
        info, topic = article.get("article_info", {}), article.get("topic", "")
        text = info.get("text", "")
        if topic:
            pattern = rf"(?<!\$)\b{re.escape(topic)}\b"
            text = re.sub(pattern, lambda match: "$" + topic, text, flags=re.IGNORECASE)
            text = f"{text}\n\n#{topic}"

        api_key = get_config(f"{account}_square_api_key")
        if not api_key:
            logger.error(
                "[发布/账号] 当前账号无法发布 | 账号: [%s] | 结果: [本轮跳过] "
                "| 原因: [未读取到 API Key] | 排查: [检查对应账号配置]",
                account,
            )
            continue

        started = time.monotonic()
        stage, api_result = "调用发布接口", "未知"
        try:
            success = publish_to_binance_square(api_key=api_key, text_content=text)
            api_result = "成功" if success else "失败"
            if success:
                account_state["total_success"] += 1
                account_state["last_publish_time"] = now
                if topic:
                    account_state["topic_publish_history"][topic] = now
                changes = {"publish_status": "success", "published_by": account, "publish_time": now}
            else:
                error = "发帖失败（可能是网络不通、Key失效或达到每日上限）"
                account_state["last_error_msg"], account_state["last_error_time"] = error, now
                changes = {"publish_status": "failed", "last_error": error}

            stage = "写入账号状态文件"
            save_json(STATE_FILE, state)
            stage = "回写数据库发布状态"

            # [修改点 5]：回写状态更新为调用纯净的 upsert 接口
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
            "[发布/文章] 接口调用及状态回写结束 | 账号: [%s] | 主题: [%s] | 文章: [%s] "
            "| 评分: [%s] | 耗时: [%.2f 秒] | 结果: [%s] | 说明: [%s]",
            account, topic, article.get("_id"), info.get("score"), time.monotonic() - started,
            "发布成功" if success else "发布失败",
            "已调用本地和数据库状态回写" if success else error,
        )

def auto_publish_articles():
    """发布后台线程：沿用独立数据库对象，每轮结束后等待 10 分钟。"""
    article_manager = GeneratedArticleManager(gen_db_object())
    while True:
        try:
            _publish_articles_once(article_manager)
        except Exception:
            logger.exception(
                "[发布/轮询] 本轮中断 | 结果: [600 秒后重新扫描] "
                "| 排查: [检查文章数据、账号状态文件、配置、发布接口和数据库；"
                "若远端已成功，先核对回写状态以排除重复发布]"
            )
        finally:
            time.sleep(PUBLISH_POLL_SECONDS)


# : 以下公开查询/导出入口存在旧数据形态；缺少调用方代码，不能仅因本文件未调用便删除。
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
    """为后台入口的未处理异常补充上下文并重抛；保留线程退出、不自动重启的行为。"""
    try:
        task()
    except Exception:
        logger.exception(
            "[任务/退出] 后台任务异常结束 | 任务: [%s] | 结果: [当前线程停止] "
            "| 排查: [检查对应链路的数据、文件权限及外部服务]",
            task.__name__,
        )
        raise


if __name__ == "__main__":
    tasks = (generate_analysis_articles, format_image_article, auto_publish_articles)
    threads = []
    for task in tasks:
        thread = threading.Thread(target=_run_task, args=(task,), name=task.__name__)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()