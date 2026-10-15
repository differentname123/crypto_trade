"""
[功能摘要] 采集币安广场帖子，经初筛和论据提取生成分析文章，同时支持高流量帖子两阶段筛查与洗稿重塑，再调度发布、互动与历史清理。
[输入数据] MongoDB 原帖 {post_id, publish_time, content.text_content, media.local_mapping,
pre_screen, logic_mul, viral_screen}；文章 {type, topic, stance, article_info}；提示词、币种文件、账号配置与状态文件。
[数据流转/交互] 热榜/行情 → 采集入库 → 每批50帖初筛回写 pre_screen → 媒体编号/模型提取回写 logic_mul → 24h且浏览>1000帖回写 viral_screen；
论据按时效/币种/立场分组 → 按已发布引用次数选材 → 模型生成/校验 → 分析文章(type=analysis)入库；
高潜单帖(usable=true, viral_score>=4) → 1对1洗稿与神评生成/校验 → 高流量文章(type=viral)入库；
近6小时文章 → 评分/图片去重/账号冷却 → 浏览器发布 → 保存账号与数据库状态；
近24小时已发文章 → 凭证/点赞/收藏/评论 → 互动回写；已发文章与远端历史 → 行情/7天边界 → 删除回写。
[输出数据] 初筛标记、论据、高流量筛查标记、文章(含type区分)、发布/互动/删除状态及聚合日志；外部依赖继续由原项目提供。
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
import stat
from functools import lru_cache
from PIL import Image, UnidentifiedImageError
VIRAL_PUBLISH_MAX_AGE_HOURS = 24
STATE_FILE_LOCK = threading.Lock()
# 大模型视觉接口支持的标准光栅图片格式
ALLOWED_IMAGE_FORMATS = {"JPEG", "PNG", "WEBP", "GIF", "BMP", "MPO"}
import ccxt
from common.common_utils import (
    get_config, read_file_to_str, read_json, save_json, setup_logger, string_to_object,
)
logger = setup_logger(app_name="media_format")
from app.ai_api.model_api import generate_content
from biance.biance_playwright import (
    create_binance_post, get_auth_tokens_robust, comment_on_binance_post,
)
from biance.biance_squre_api import (
    fetch_binance_feed, fetch_binance_future_hot_coins, fetch_binance_hot_hashtags,
    fetch_binance_spot_hot_coins, like_and_bookmark, get_latest_crypto_price,
    fetch_binance_square_replies, delete_binance_square_content,
)


from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import GeneratedArticleManager, UniversalPostManager

prompt_version = "1016-1"
BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
POST_MAX_AGE_HOURS = 48
MAX_MEDIA_COUNT = 10
MAX_CONCURRENCY = 20
LLM_MAX_RETRIES = 3

PRE_SCREEN_PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类MLU初筛.txt"
PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类MLU提取.txt"
ARTICLE_PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类文章生成.txt"
VIRAL_STAGE1_PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_娱乐类_单帖可用性筛选.txt"
VIRAL_STAGE2_PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\内容生成方案_娱乐类_单帖洗稿和神评生成.txt"

ARTICLE_TYPE_ANALYSIS = "analysis"
ARTICLE_TYPE_VIRAL = "viral"
VIRAL_POST_MAX_AGE_HOURS = 24
VIRAL_MIN_VIEW_COUNT = 1000
VIRAL_MIN_SCORE = 4
VIRAL_STAGE1_BATCH_SIZE = 50

PRE_SCREEN_BATCH_SIZE = 50
PRE_SCREEN_CONCURRENCY = 5
ARTICLE_MATERIAL_LIMIT = 10
ARTICLE_MIN_MATERIALS = 5
ARTICLE_POST_USAGE_LIMIT = 2
ARTICLE_MAX_CHARS = 260
ARTICLE_VARIANTS = 2
ARTICLE_CONCURRENCY = 5
ARTICLE_GENERATION_INTERVAL_SECONDS = 600
STATE_FILE = "account_publish_state.json"
ACCOUNTS = ["qiqi", "ruru"]
FEED_TOKENS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE"]
ACCOUNT_COOLDOWN_SECONDS = 3600
ACCOUNT_TOPIC_COOLDOWN_SECONDS = 2 * 3600
PUBLISH_MAX_ATTEMPTS = 3
PUBLISH_POLL_SECONDS = 600
INTERACTION_INTERVAL_SECONDS = 1200
MAX_REPLAY_DAYS = 7
DELETE_INTERVAL_SEC = 3600
BEIJING_TZ = timezone(timedelta(hours=8))
MEDIA_PATTERN = re.compile(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]")
COIN_TAG_PATTERN = re.compile(r"\$[A-Za-z0-9_]*[A-Za-z][A-Za-z0-9_]*")
SHELF_LIFE_SECONDS = {
    "hours": 2 * 3600, "days": 24 * 3600, "weeks": 7 * 24 * 3600,
    "long": 15 * 24 * 3600, "unknown": 24 * 3600,
}


# 秒/毫秒数值 → 秒；拒绝无效与非有限值，避免错误时间进入业务比较。
def _publish_time_seconds(value):
    try:
        timestamp = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("发布时间不是有效的秒/毫秒时间戳") from exc
    if not math.isfinite(timestamp):
        raise ValueError("发布时间不能是 NaN 或无穷大")
    return timestamp / 1000 if timestamp > 1e11 else timestamp


# 任意值、精确字段集合 → 错误文本或空串；集中表达模型数据的字段契约。
def _key_error(data, expected_keys, label):
    if not isinstance(data, dict):
        return f"{label} 必须是字典"
    missing, extra = expected_keys - data.keys(), data.keys() - expected_keys
    if missing:
        return f"{label} 缺少字段: {', '.join(sorted(missing))}"
    if extra:
        return f"{label} 包含未定义字段: {', '.join(sorted(map(str, extra)))}"
    return ""


# prompt、校验回调、模型参数 → 校验后的字典；原有重试耗尽返回 {} 的策略保持不变。
def _generate_validated_info(action, prompt, validator, context, **model_kwargs):
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        raw_response = ""
        try:
            result = generate_content(prompt=prompt, **model_kwargs)
            raw_response = result.get("content", "")
            data = string_to_object(raw_response)
            valid, error = validator(data)
            if not valid:
                raise ValueError(error)
            return data
        except Exception as exc:
            exhausted = attempt == LLM_MAX_RETRIES
            delay = 0 if exhausted else 2 ** attempt
            log = logger.error if exhausted else logger.warning
            log(
                "%s[%s] 模型调用或数据校验失败 | 上下文: [%s] | 尝试: [%s/%s] "
                "| 原因: [%s] | 响应摘要: [%s] | 后续: [%s] "
                "| 排查: [检查模型服务、提示词字段及媒体读取]",
                "❌ " if exhausted else "", action, context, attempt, LLM_MAX_RETRIES,
                exc, repr(raw_response)[:500],
                "返回空数据，等待下轮扫描" if exhausted else f"{delay} 秒后重试",
                exc_info=exhausted,
            )
            if exhausted:
                return {}
            time.sleep(delay)
    return {}


# post{post_id,publish_time,content.text_content,media.local_mapping/video_duration} → bool。
# : 媒体上限按映射项计数并排除恰好10项，视频只看时长字段，未来时间仍可能通过；保留原边界。
# (绝对路径, 修改时间纳秒, 字节大小) → bool；校验结构完整性与全像素解码，缓存结果避免初筛与精提重复解码。
@lru_cache(maxsize=32768)
def _verify_image_integrity(abs_path, mtime_ns, file_size):
    try:
        # 1. 校验文件头、格式枚举、尺寸及容器结构（如PNG CRC校验）
        with Image.open(abs_path) as img:
            fmt = (img.format or "").upper()
            if fmt not in ALLOWED_IMAGE_FORMATS:
                return False
            width, height = img.size
            if width <= 0 or height <= 0:
                return False
            img.verify()

        # 2. verify() 后重新打开并调用 load() 强制解码全部像素流，拦截半截/截断图（Truncated Image）
        with Image.open(abs_path) as img:
            img.load()
        return True
    except (UnidentifiedImageError, OSError, SyntaxError, ValueError, TypeError, Image.DecompressionBombError):
        return False
    except Exception:
        return False


# 本地图片路径 → bool；校验文件存在、非0字节且为未损坏的合法图片。
def _is_valid_image_file(path):
    if not isinstance(path, str) or not path.strip():
        return False
    try:
        abs_path = os.path.abspath(path)
        stat_info = os.stat(abs_path)
    except OSError:
        return False
    if not stat.S_ISREG(stat_info.st_mode) or stat_info.st_size <= 0:
        return False
    return _verify_image_integrity(abs_path, stat_info.st_mtime_ns, stat_info.st_size)


# post{post_id,publish_time,content.text_content,media.local_mapping/video_duration} → bool。
# 先完成全部内存级校验与48小时时效过滤，最后再执行磁盘文件存在性与图片损坏检验。
def _is_valid_raw_post(post):
    post_id = post.get("post_id")
    if post_id is None or not str(post_id).strip():
        return False
    media = post.get("media") or {}
    video_duration = media.get("video_duration")
    if video_duration and video_duration > 0:
        return False
    mapping = media.get("local_mapping") or {}
    paths = list(mapping.values())
    if len(paths) >= MAX_MEDIA_COUNT or any(not path for path in paths):
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    if not text.strip() or any(not mapping.get(match.group(2)) for match in MEDIA_PATTERN.finditer(text)):
        return False
    publish_time = _publish_time_seconds(post.get("publish_time", 0))
    if (time.time() - publish_time) / 3600 > POST_MAX_AGE_HOURS:
        return False
    return all(_is_valid_image_file(path) for path in paths)


# 原帖{logic_mul,pre_screen{prompt_version,no_evidence},content,media,publish_time} → bool。
def is_need_pre_screening(post):
    if post.get("logic_mul"):
        return False
    screen = post.get("pre_screen")
    if (isinstance(screen, dict) and screen.get("prompt_version") == prompt_version
            and isinstance(screen.get("no_evidence"), bool)):
        return False
    return _is_valid_raw_post(post)


# post{logic_mul,pre_screen{prompt_version,no_evidence},content,media,publish_time} → bool；只精提初筛通过的帖子。
def is_need_formatting(post):
    screen = post.get("pre_screen")
    if (post.get("logic_mul") or not isinstance(screen, dict)
            or screen.get("prompt_version") != prompt_version or screen.get("no_evidence") is not False):
        return False
    return _is_valid_raw_post(post)


# post{viral_screen,engagement.view_count,publish_time,content,media} → bool；仅筛查24h内且浏览量>1000的纯图文帖。
def is_need_viral_screening(post):
    screen = post.get("viral_screen")
    if (isinstance(screen, dict) and screen.get("prompt_version") == prompt_version
            and isinstance(screen.get("keep"), bool)):
        return False
    engagement = post.get("engagement") or {}
    view_count = engagement.get("view_count", 0)
    if not isinstance(view_count, (int, float)) or isinstance(view_count, bool) or view_count <= VIRAL_MIN_VIEW_COUNT:
        return False
    try:
        publish_time = _publish_time_seconds(post.get("publish_time", 0))
    except ValueError:
        return False
    age_hours = (time.time() - publish_time) / 3600
    if age_hours > VIRAL_POST_MAX_AGE_HOURS or age_hours < -1:
        return False
    text = (post.get("content") or {}).get("text_content") or ""
    if any(match.group(1) == "视频" for match in MEDIA_PATTERN.finditer(text)):
        return False
    return _is_valid_raw_post(post)


# post.content.text_content → {post_id,text_content,images:[image1,...]}；初筛只传编号，不上传媒体。
def build_pre_screen_item(post):
    text = (post.get("content") or {}).get("text_content") or ""
    image_count = sum(match.group(1) != "视频" for match in MEDIA_PATTERN.finditer(text))
    return {
        "post_id": str(post.get("post_id", "")).strip(),
        "text_content": MEDIA_PATTERN.sub("", text).strip(),
        "images": [f"image{i}" for i in range(1, image_count + 1)],
    }


# {post_id:{no_evidence:bool,reason:str}}、预期ID序列 → (bool,错误文本)；额外帖子ID仍允许。
def check_pre_screen_info(json_data, expected_post_ids):
    if not isinstance(json_data, dict):
        return False, "初筛返回最外层必须是字典"
    expected = {str(pid) for pid in expected_post_ids}
    missing = expected - json_data.keys()
    if missing:
        return False, f"初筛返回缺少 post_id: {', '.join(sorted(missing)[:10])}"
    for pid in expected:
        item, label = json_data[pid], f"post[{pid}]"
        error = _key_error(item, {"no_evidence", "reason"}, label)
        if error:
            return False, error
        if not isinstance(item["no_evidence"], bool):
            return False, f"{label}.no_evidence 必须是布尔值"
        if not isinstance(item["reason"], str) or not item["reason"].strip():
            return False, f"{label}.reason 必须是非空字符串"
    return True, ""


# 原帖列表、提示词 → {字符串post_id:{no_evidence,reason}}；校验和重试由公共模型链路处理。
def gen_pre_screen_batch_info(batch_posts, prompt_template):
    items = [build_pre_screen_item(post) for post in batch_posts]
    ids = [item["post_id"] for item in items]
    data = _generate_validated_info(
        "初筛/批次", f"{prompt_template}\n{json.dumps(items, ensure_ascii=False)}",
        lambda response: check_pre_screen_info(response, ids),
        f"批次大小={len(items)}, 首帖ID={ids[0] if ids else 'NONE'}",
        preset_model_group="low",
    )
    return {str(key): value for key, value in data.items()}


# 原帖列表 → 每帖回写 pre_screen{no_evidence,reason,prompt_version,screen_time}；返回(通过数,淘汰数)。
def process_and_save_pre_screen_batch(batch_posts, post_manager, prompt_template):
    started = time.monotonic()
    screen_map = gen_pre_screen_batch_info(batch_posts, prompt_template)
    if not screen_map:
        return 0, 0
    updates, filtered = [], 0
    for post in batch_posts:
        result = screen_map.get(str(post.get("post_id", "")).strip())
        if not result:
            continue
        post["pre_screen"] = {
            "no_evidence": result["no_evidence"], "reason": result["reason"].strip(),
            "prompt_version": prompt_version, "screen_time": time.time(),
        }
        updates.append(post)
        filtered += result["no_evidence"]
    if updates:
        post_manager.upsert_posts(updates)
    passed = len(updates) - filtered
    logger.info(
        "[初筛/回写] 批次已落库 | 处理: [%s] | 通过: [%s] | 淘汰: [%s] | 耗时: [%.2f 秒]",
        len(updates), passed, filtered, time.monotonic() - started,
    )
    return passed, filtered


# 原帖列表、管理器 → 50帖一批并发初筛；保留批次异常隔离，成功结果原地更新供精提使用。
def batch_pre_screen_posts(posts, post_manager):
    # 统计总帖数（若 posts 可能是迭代器，建议先转为 list 或取 len）
    total_posts = len(posts)
    pending = [post for post in posts if is_need_pre_screening(post)]
    skipped_count = total_posts - len(pending)

    if not pending:
        logger.info(
            "[初筛/计划] 当前无待初筛新帖 | 接收总数: [%s] | 全部已跳过/已处理",
            total_posts
        )
        return

    prompt = read_file_to_str(PRE_SCREEN_PROMPT_FILE_PATH)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"初筛提示词为空或读取失败: {PRE_SCREEN_PROMPT_FILE_PATH}")

    batches = [pending[index:index + PRE_SCREEN_BATCH_SIZE]
               for index in range(0, len(pending), PRE_SCREEN_BATCH_SIZE)]

    # 🚀 在实际执行前输出详尽统计日志
    logger.info(
        "🚀 [初筛/启动] 任务就绪开始执行 | 接收总数: [%s] | 待初筛: [%s] (%.1f%%) | 跳过: [%s] "
        "| 划分批次: [%s] | 单批上限: [%s] | 并发上限: [%s] "
        "| 首帖ID: [%s] | 末帖ID: [%s] | 提示词字数: [%s]",
        total_posts,
        len(pending),
        (len(pending) / total_posts * 100) if total_posts > 0 else 0.0,
        skipped_count,
        len(batches),
        PRE_SCREEN_BATCH_SIZE,
        PRE_SCREEN_CONCURRENCY,
        pending[0].get("post_id", "N/A"),
        pending[-1].get("post_id", "N/A"),
        len(prompt),
    )

    started = time.monotonic()
    passed = filtered = failed = 0
    with ThreadPoolExecutor(max_workers=PRE_SCREEN_CONCURRENCY) as executor:
        futures = {executor.submit(process_and_save_pre_screen_batch, batch, post_manager, prompt): batch
                   for batch in batches}
        for future in as_completed(futures):
            batch = futures[future]
            try:
                kept, rejected = future.result()
                passed += kept
                filtered += rejected
                failed += kept + rejected == 0
            except Exception:
                failed += 1
                logger.exception(
                    "❌ [初筛/执行] 批次处理或回写失败 | 首帖: [%s] | 大小: [%s] "
                    "| 后续: [继续其他批次] | 排查: [检查帖子字段与数据库连接]",
                    batch[0].get("post_id"), len(batch),
                )

    logger.info(
        "[初筛/汇总] 本轮结束 | 待筛: [%s] | 批次: [%s] | 每批上限: [%s] | 并发上限: [%s] "
        "| 通过: [%s] | 淘汰: [%s] | 失败批次: [%s] | 耗时: [%.2f 秒]",
        len(pending), len(batches), PRE_SCREEN_BATCH_SIZE, PRE_SCREEN_CONCURRENCY,
        passed, filtered, failed, time.monotonic() - started,
    )


# post{content.text_content,media.local_mapping} → (正文,路径列表,{占位符:路径})；重复URL保留出现顺序。
def normalize_post_media(post_data):
    text = (post_data.get("content") or {}).get("text_content") or ""
    mapping = (post_data.get("media") or {}).get("local_mapping") or {}
    paths, placeholders, counters = [], {}, {"IMAGE": 0, "VIDEO": 0}

    def replace_match(match):
        kind = "VIDEO" if match.group(1) == "视频" else "IMAGE"
        counters[kind] += 1
        placeholder = f"[{kind}_{counters[kind]}]"
        path = mapping.get(match.group(2), "")
        paths.append(path)
        placeholders[placeholder] = path
        return placeholder

    return MEDIA_PATTERN.sub(replace_match, text), paths, placeholders


# {evidences:[{target_coin,stance,dimension,core_fact,logic_link,raw_hook,impact_weight,
# shelf_life,images,impact_reason}]}、真实占位符 → (bool,错误文本)；images字段见 image_keys。
# : 原契约未校验 impact_reason/context 的类型；不擅自收紧模型输出条件。
def check_format_info(json_data, placeholders):
    if not isinstance(json_data, dict):
        return False, "最外层必须是字典"
    if "evidences" not in json_data:
        return False, "最外层缺少 evidences"
    if not isinstance(json_data["evidences"], list):
        return False, "evidences 必须是列表"
    evidence_keys = {
        "target_coin", "stance", "dimension", "core_fact", "logic_link", "raw_hook",
        "impact_weight", "shelf_life", "images", "impact_reason",
    }
    image_keys = {"image_id", "image_type", "context", "usable", "unusable_reason"}
    enums = {
        "dimension": ("K线形态", "技术指标", "链上数据", "资金流", "消息面", "基本面", "情绪判断"),
        "stance": ("看多", "看空", "震荡"),
        "shelf_life": ("hours", "days", "weeks", "long", "unknown"),
    }
    image_types = ("盘面截图", "数据图表", "新闻截图", "社交截图", "收益截图", "梗图表情", "实拍照片", "其他")
    for index, evidence in enumerate(json_data["evidences"], 1):
        label = f"evidences[{index}]"
        error = _key_error(evidence, evidence_keys, label)
        if error:
            return False, error
        coin = evidence["target_coin"]
        if not isinstance(coin, str) or not coin.strip():
            return False, f"{label}.target_coin 必须是非空字符串"
        if coin != coin.upper():
            return False, f"{label}.target_coin 必须为大写英文字符串: {coin!r}"
        if not isinstance(evidence["core_fact"], str) or not evidence["core_fact"].strip():
            return False, f"{label}.core_fact 必须是非空字符串"
        for field in ("logic_link", "raw_hook"):
            if not isinstance(evidence[field], str):
                return False, f"{label}.{field} 必须是字符串（允许为空字符串）"
        weight = evidence["impact_weight"]
        if not isinstance(weight, int) or isinstance(weight, bool) or not 1 <= weight <= 5:
            return False, f"{label}.impact_weight 必须是 1—5 的整数，不能是布尔值"
        for field, allowed in enums.items():
            if evidence[field] not in allowed:
                return False, f"{label}.{field} 取值不合法: {evidence[field]!r}"
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


# 原帖 → {evidences:[论据]}；媒体编号、上传参数和前置文件读取异常行为保持不变。
def gen_media_format_info(post):
    text, paths, mapping = normalize_post_media(post)
    prompt = f"{read_file_to_str(PROMPT_FILE_PATH)}\n{text}"
    return _generate_validated_info(
        "论据/提取", prompt, lambda response: check_format_info(response, list(mapping)),
        f"帖子ID={post.get('post_id', 'UNKNOWN_ID')}", file_paths=paths,
    )


# post{post_id,content.text_content,media.local_mapping}、管理器 → 回写logic_mul；保留单帖异常隔离。
def process_and_save_single_post(post, post_manager):
    started = time.monotonic()
    post_id = post.get("post_id", "UNKNOWN_ID") if isinstance(post, dict) else "UNKNOWN_ID"
    try:
        info = gen_media_format_info(post)
        if not info:
            return
        info["prompt_version"] = prompt_version
        post["logic_mul"] = info
        post_manager.upsert_posts([post])
        logger.info(
            "[帖子/回写] 论据已保存 | 帖子: [%s] | 论据: [%s 条] | 耗时: [%.2f 秒]",
            post_id, len(info["evidences"]), time.monotonic() - started,
        )
    except Exception:
        logger.exception(
            "❌ [帖子/处理] 当前帖子失败 | 帖子: [%s] | 后续: [继续其他任务] "
            "| 排查: [检查帖子字段、提示词、媒体路径及数据库]", post_id,
        )


# 高流量第一阶段批量粗筛输出 [{post_id, keep, event_key}]、预期ID序列 → (bool,错误文本)。
def check_viral_stage1_info(json_data, expected_post_ids):
    if not isinstance(json_data, list):
        return False, "高流量一阶段粗筛返回最外层必须是列表(JSON Array)"
    expected = {str(pid).strip() for pid in expected_post_ids}
    seen = set()
    for index, item in enumerate(json_data, 1):
        label = f"viral_stage1[{index}]"
        error = _key_error(item, {"post_id", "keep", "event_key"}, label)
        if error:
            return False, error
        pid = str(item.get("post_id", "")).strip()
        if not pid:
            return False, f"{label}.post_id 不能为空"
        if not isinstance(item["keep"], bool):
            return False, f"{label}.keep 必须是布尔值"
        if not isinstance(item["event_key"], str):
            return False, f"{label}.event_key 必须是字符串"
        seen.add(pid)
    missing = expected - seen
    if missing:
        return False, f"高流量一阶段粗筛返回缺少 post_id: {', '.join(sorted(missing)[:10])}"
    return True, ""


# 候选原帖批次、提示词模板 → {post_id: {keep: bool, event_key: str}}；仅传带[IMAGE_n]占位符的文本，不传图片文件。
def gen_viral_stage1_info(batch_posts, prompt_template):
    items = []
    for post in batch_posts:
        normalized_text, _, placeholders = normalize_post_media(post)
        items.append({
            "post_id": str(post.get("post_id", "")).strip(),
            "image_count": len(placeholders),
            "text": normalized_text.strip(),
        })
    ids = [item["post_id"] for item in items]
    data = _generate_validated_info(
        "高流量/批量粗筛",
        f"{prompt_template}\n{json.dumps(items, ensure_ascii=False)}",
        lambda response: check_viral_stage1_info(response, ids),
        f"批次大小={len(items)}, 首帖ID={ids[0] if ids else 'NONE'}",
        preset_model_group="low",
    )
    if not isinstance(data, list):
        return {}
    return {
        str(item["post_id"]).strip(): {
            "keep": item["keep"],
            "event_key": item["event_key"].strip(),
        }
        for item in data if isinstance(item, dict) and "post_id" in item
    }


# 单批高流量候选原帖 → 执行第一阶段批量纯文本粗筛并回写 viral_screen；返回 (保留数, 淘汰数)。
def process_and_save_single_viral_post(batch_posts, post_manager, prompt_template):
    started = time.monotonic()
    screen_map = gen_viral_stage1_info(batch_posts, prompt_template)
    if not screen_map:
        return 0, 0
    updates, kept_count = [], 0
    now = time.time()
    for post in batch_posts:
        pid = str(post.get("post_id", "")).strip()
        result = screen_map.get(pid)
        if not result:
            continue
        post["viral_screen"] = {
            "keep": result["keep"],
            "event_key": result["event_key"],
            "prompt_version": prompt_version,
            "screen_time": now,
        }
        updates.append(post)
        kept_count += int(result["keep"])
    if updates:
        post_manager.upsert_posts(updates)
    rejected_count = len(updates) - kept_count
    logger.info(
        "[高流量/粗筛回写] 批次已保存 | 处理: [%s] | 放行(keep=True): [%s] | 淘汰: [%s] | 耗时: [%.2f 秒]",
        len(updates), kept_count, rejected_count, time.monotonic() - started,
    )
    return kept_count, rejected_count


# 原帖列表、管理器 → 筛选24h内且浏览量>1000的待审帖，按批次并发执行第一阶段纯文本粗筛。
def batch_screen_viral_posts(posts, post_manager):
    pending = [post for post in posts if is_need_viral_screening(post)]
    if not pending:
        logger.info(
            "[高流量/计划] 当前无待粗筛新帖 | 扫描: [%s] | 全部已跳过/已处理",
            len(posts),
        )
        return
    prompt = read_file_to_str(VIRAL_STAGE1_PROMPT_FILE_PATH)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"高流量一阶段粗筛提示词为空或读取失败: {VIRAL_STAGE1_PROMPT_FILE_PATH}")
    batches = [pending[i:i + VIRAL_STAGE1_BATCH_SIZE]
               for i in range(0, len(pending), VIRAL_STAGE1_BATCH_SIZE)]
    logger.info(
        "[高流量/计划] 第一阶段批量纯文本粗筛就绪 | 扫描: [%s] | 待粗筛(24h且浏览>%s): [%s] "
        "| 批次: [%s] | 每批上限: [%s] | 并发上限: [%s]",
        len(posts), VIRAL_MIN_VIEW_COUNT, len(pending), len(batches),
        VIRAL_STAGE1_BATCH_SIZE, PRE_SCREEN_CONCURRENCY,
    )
    started = time.monotonic()
    total_kept = total_rejected = failed_batches = 0
    with ThreadPoolExecutor(max_workers=PRE_SCREEN_CONCURRENCY) as executor:
        futures = {
            executor.submit(process_and_save_single_viral_post, batch, post_manager, prompt): batch
            for batch in batches
        }
        for future in as_completed(futures):
            batch = futures[future]
            try:
                kept, rejected = future.result()
                total_kept += kept
                total_rejected += rejected
                failed_batches += (kept + rejected == 0)
            except Exception:
                failed_batches += 1
                logger.exception(
                    "❌ [高流量/粗筛] 批次处理异常 | 首帖: [%s] | 大小: [%s] | 后续: [继续其他批次]",
                    batch[0].get("post_id"), len(batch),
                )
    logger.info(
        "[高流量/汇总] 第一阶段粗筛完成 | 待筛: [%s] | 放行: [%s] | 淘汰: [%s] | 失败批次: [%s] | 耗时: [%.2f 秒]",
        len(pending), total_kept, total_rejected, failed_batches, time.monotonic() - started,
    )


# 币种序列、排序参数 → 采集并 upsert_posts([post])；保留接口顺序和重复帖子。
def _fetch_and_save_posts(post_manager, token_list, orders, feed_count):
    started = time.monotonic()
    posts, token_count = [], 0
    for token in token_list:
        token_count += 1
        for order in orders:
            posts.extend(fetch_binance_feed(token=token, count=feed_count, orderBy=order))
    posts.extend(fetch_binance_feed(count=feed_count * 2))
    if posts:
        post_manager.upsert_posts(posts)
    logger.info(
        "[采集/完成] 推荐流已入库 | 币种: [%s] | 帖子: [%s，含重复] | 耗时: [%.2f 秒]",
        token_count, len(posts), time.monotonic() - started,
    )


# 管理器 → 固定币种热门流和综合流入库；保留手动采集入口。
def fetch_post(post_manager):
    _fetch_and_save_posts(post_manager, FEED_TOKENS, (1,), 100)


# 管理器、币种序列 → 热门/最新流及综合流入库。
def fetch_hot_post(post_manager, token_list):
    _fetch_and_save_posts(post_manager, token_list, (1, 2), 100)


# top_k → {top_gainers/top_losers:[{symbol,percentage}]}；局部行情客户端在异常时也关闭。
# top_k → {top_gainers/top_losers:[{symbol,percentage}]}；安全释放 CCXT 客户端，禁止释放异常阻断正常返回。
def get_top_k_movers(top_k=5):
    exchange = ccxt.binance({
        "enableRateLimit": True, "options": {"defaultType": "future"},
        "proxies": {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"},
    })
    try:
        tickers = exchange.fetch_tickers()
        ranked = sorted(
            ({"symbol": symbol, "percentage": ticker["percentage"]}
             for symbol, ticker in tickers.items()
             if symbol.endswith(":USDT") and ticker.get("percentage") is not None),
            key=lambda item: item["percentage"], reverse=True,
        )
        return {"top_gainers": ranked[:top_k], "top_losers": ranked[-top_k:][::-1]}
    finally:
        # 安全释放：无论主任务成功与否，释放客户端的异常均不应逆向阻断正常业务返回
        try:
            close_fn = getattr(exchange, "close", None)
            if callable(close_fn):
                close_fn()
        except Exception as close_err:
            logger.debug("[行情/释放] CCXT 客户端释放异常（已忽略）: %s", close_err)


# 合约/现货热榜与涨跌榜各前2项 → 去重币种列表；沿用集合的无序返回。
def get_hot_coin():
    top_k, coins = 2, []
    for fetch in (fetch_binance_future_hot_coins, fetch_binance_spot_hot_coins):
        coins.extend(item.get("symbol").replace("USDT", "").replace("USDC", "")
                     for item in fetch()[:top_k])
    movers = get_top_k_movers(top_k)
    coins.extend(item["symbol"].split("/")[0] for item in movers["top_gainers"] + movers["top_losers"])
    coins = list(set(coins))
    logger.info("[币种/热榜] 已合并热门币种 | 数量: [%s] | 币种: [%s]", len(coins), coins)
    return coins


# 采集 → 批量初筛原地回写 → 并发精提 → 高流量第一阶段筛查；保留无帖60秒、满批5秒、普通批次1小时的轮询节奏。
def format_image_article():
    while True:
        try:
            started = time.monotonic()
            post_manager = UniversalPostManager(gen_db_object())
            # fetch_hot_post(post_manager, get_hot_coin())
            posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
            if not posts:
                logger.info("[帖子/轮询] 暂无原帖 | 下次扫描: [60 秒后]")
                time.sleep(60)
                continue
            # batch_pre_screen_posts(posts, post_manager)
            valid_posts = [post for post in posts if is_need_formatting(post)]
            logger.info(
                "[帖子/计划] 初筛后精提 | 扫描: [%s] | 待精提: [%s] | 跳过: [%s] | 并发上限: [%s]",
                len(posts), len(valid_posts), len(posts) - len(valid_posts), MAX_CONCURRENCY,
            )
            if valid_posts:
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                    futures = [executor.submit(process_and_save_single_post, post, post_manager)
                               for post in valid_posts]
                    for future in as_completed(futures):
                        future.result()
            batch_screen_viral_posts(posts, post_manager)
            delay = 5 if len(posts) >= POST_QUERY_LIMIT else 3600
            logger.info(
                "[帖子/完成] 本轮线程结束 | 扫描: [%s] | 已提交: [%s] | 耗时: [%.2f 秒] "
                "| 下次扫描: [%s 秒后] | 单帖结果: [见帖子回写/失败日志]",
                len(posts), len(valid_posts), time.monotonic() - started, delay,
            )
            time.sleep(delay)
        except Exception:
            logger.exception(
                "❌ [帖子/轮询] 本轮中断 | 后续: [60 秒后重新初始化] "
                "| 排查: [检查热榜、采集服务、提示词、代理、帖子字段及数据库]"
            )
            time.sleep(60)


# 无入参 → 查询范围内非None的 logic_mul/pre_screen 置空并批量回写；保留手动清理入口。
def clear_all_media_format_batch():
    manager = UniversalPostManager(gen_db_object())
    posts = manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    updates = [post for post in posts if post.get("logic_mul") is not None or post.get("pre_screen") is not None]
    for post in updates:
        post["logic_mul"] = post["pre_screen"] = None
    if updates:
        manager.upsert_posts(updates)
    logger.info("[数据/清理] 提取与初筛字段已清空 | 扫描: [%s] | 更新: [%s]", len(posts), len(updates))


# 原帖{content.text_content,media.local_mapping} → {IMAGE编号:{original_placeholder,original_url,local_path}}。
def build_source_image_mapping(post):
    _, _, normalized = normalize_post_media(post)
    text = (post.get("content") or {}).get("text_content") or ""
    return {
        image_id: {"original_placeholder": match.group(0), "original_url": match.group(2), "local_path": path or None}
        for (image_id, path), match in zip(normalized.items(), MEDIA_PATTERN.finditer(text))
        if image_id.startswith("[IMAGE_")
    }


# 论据[{core_fact,logic_link,raw_hook,images[{image_id,usable,context}],source_post_id,source_image_mapping}]
# → ([{id,fact,underlying_logic,raw_hook,visual_evidence}],{ASSET编号:来源})；visual为None/单图/有序图组。
def transform_mlus(mlu_list):
    materials, image_mapping, asset_by_source = [], {}, {}
    for index, evidence in enumerate(mlu_list, 1):
        material = {
            "id": f"M{index}", "fact": evidence.get("core_fact", ""),
            "underlying_logic": evidence.get("logic_link", ""), "raw_hook": evidence.get("raw_hook", ""),
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
        if not all(image.get("image_id") in source_mapping
                   and isinstance(image.get("context"), str) and image["context"].strip() for image in images):
            logger.warning(
                "[文章/配图] 当前论据保留文字、取消整组配图 | 帖子: [%s] "
                "| 原因: [至少一图缺少来源或描述] | 排查: [核对原文编号、映射与context]",
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
                    "source": BINANCE_SOURCE, "source_post_id": evidence.get("source_post_id"),
                    "original_image_id": image_id, "original_image_id_list": [image_id], **source_mapping[image_id],
                }
            visual_items.append({"placeholder": placeholder, "what_it_shows": image["context"]})
        material["visual_evidence"] = visual_items[0] if len(visual_items) == 1 else visual_items
    return materials, image_mapping


# 数据库当前版本logic_mul.evidences → {coin:{stance:[论据]}}；追加source_post_id/publish_time/
# source_text_content/source_image_mapping，保留来源以供后续反查素材和配图。
# : 币种仍在币种文件的字符串中做子串匹配；任一图片不可用就淘汰整条论据，无图论据仍可用。
def extract_and_group_valid_evidences():
    hot_coins = {coin.upper() for coin in get_hot_coin()}
    active_symbols = str(read_json("active_usdt_symbols.json")).upper()
    started = time.monotonic()
    manager = UniversalPostManager(gen_db_object())
    posts = manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    grouped = defaultdict(lambda: defaultdict(list))
    now, invalid_times = time.time(), 0
    for post in posts:
        logic, raw_time = post.get("logic_mul"), post.get("publish_time")
        if (not isinstance(logic, dict) or logic.get("prompt_version") != prompt_version
                or not logic.get("evidences") or not raw_time):
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
        for evidence in logic["evidences"]:
            if any(image.get("usable") is not True for image in evidence.get("images", [])):
                continue
            duration = SHELF_LIFE_SECONDS.get(evidence.get("shelf_life", "unknown"), 24 * 3600)

            target_coin = str(evidence.get("target_coin") or "").strip().upper()
            if not target_coin and isinstance(evidence.get("coins"), list):
                candidate_coins = [str(coin).strip().upper() for coin in evidence["coins"] if str(coin).strip()]
            else:
                candidate_coins = [target_coin] if target_coin else []
            stance = evidence.get("stance")
            if age > duration or not candidate_coins or stance not in ("看多", "看空"):
                continue
            enriched = {
                **evidence, "source_post_id": post.get("post_id", "UNKNOWN"), "publish_time": publish_time,
                "source_text_content": (post.get("content") or {}).get("text_content", ""),
                "source_image_mapping": source_mapping,
            }
            for coin in candidate_coins:
                if coin and coin in hot_coins and coin in active_symbols:
                    grouped[coin][stance].append(enriched)
    for stances in grouped.values():
        for evidences in stances.values():
            evidences.sort(key=lambda item: item.get("impact_weight", 0), reverse=True)
    result = json.loads(json.dumps(grouped))
    log = logger.warning if invalid_times else logger.info
    log(
        "[论据/筛选] 已分组并按权重排序 | 扫描: [%s] | 币种: [%s] | 论据: [%s，含跨组重复] "
        "| 无效时间跳过: [%s] | 耗时: [%.2f 秒] | 时间异常排查: [核对秒/毫秒时间戳]",
        len(posts), len(result), sum(len(items) for stances in result.values() for items in stances.values()),
        invalid_times, time.monotonic() - started,
    )
    return result


# article_info{status,text,god_comments,image_placeholders,used_material_ids,score,reason}、
# materials[{id,visual_evidence}]、{ASSET编号:来源} → (bool,错误文本)；整组图片必须保持证据顺序。
def check_article_info(article_info, materials, image_mapping, max_chars):
    keys = {"status", "text", "god_comments", "image_placeholders", "used_material_ids", "score", "reason"}
    error = _key_error(article_info, keys, "文章")
    if error:
        return False, error
    if article_info["status"] not in ("ok", "skip"):
        return False, "status 必须为 ok 或 skip"
    if not isinstance(article_info["text"], str):
        return False, "text 必须是字符串"
    for key in ("god_comments", "image_placeholders", "used_material_ids"):
        values = article_info[key]
        if not isinstance(values, list) or any(not isinstance(value, str) or not value for value in values):
            return False, f"{key} 必须是非空字符串组成的列表，列表本身可为空"
        if key != "god_comments" and len(values) != len(set(values)):
            return False, f"{key} 不允许重复"
    if article_info["status"] == "skip":
        if (article_info["text"] != "" or article_info["image_placeholders"] or article_info["used_material_ids"]
                or article_info["god_comments"] or article_info["score"] is not None):
            return False, "skip 必须清空正文、图片、神评、采用素材，score 必须为 null"
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
        return False, "score 必须是 0—10 的数值，不能是布尔值"
    if article_info["reason"] is not None:
        return False, "ok 的 reason 必须为 null"
    if not article_info["god_comments"]:
        return False, "ok 状态下 god_comments 必须至少包含 1 条神评"
    material_by_id = {item["id"]: item for item in materials}
    used_ids, selected_images = article_info["used_material_ids"], article_info["image_placeholders"]
    if not used_ids or any(item not in material_by_id for item in used_ids):
        return False, "used_material_ids 必须引用实际存在的素材且不能为空"
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


# source、原帖ID序列 → {post_id:已成功发布引用次数}；同一文章同帖只计一次。
def get_post_usage_counts(article_manager, source, post_ids):
    unique_ids = list(dict.fromkeys(post_ids))
    counts = dict.fromkeys(unique_ids, 0)
    if not unique_ids:
        return counts
    articles = article_manager.find_articles(
        query={"source": source, "status": "ok", "publish_status": "success", "post_id_list": {"$in": unique_ids}},
        limit=0,
    )
    for article in articles:
        for post_id in set(article.get("post_id_list", [])):
            if post_id in counts:
                counts[post_id] += 1
    return counts


# source/topic/stance → 近期文章首行列表；保留生成成功即参与、查询限额先于去重的规则。
def get_recent_openings(article_manager, source, topic, stance, limit=5):
    if limit <= 0:
        return []
    articles = article_manager.find_articles(
        query={"source": source, "topic": topic, "stance": stance, "status": "ok"},
        sort=[("created_at", -1)], limit=limit,
    )
    openings = []
    for article in articles:
        text = (article.get("article_info") or {}).get("text", "")
        if isinstance(text, str) and text.strip():
            opening = text.strip().splitlines()[0][:80]
            if opening not in openings:
                openings.append(opening)
    return openings


# record{source,topic,stance,status,article_info.used_material_ids,material_post_mapping}
# → 补齐默认字段的浅拷贝；兼容高流量二阶段 status=skip 时保留 post_id_list 以防重复处理。
def prepare_article_record_for_db(article_data):
    record = dict(article_data)
    record.setdefault("type", ARTICLE_TYPE_ANALYSIS)
    for key in ("source", "topic", "stance"):
        if not isinstance(record.get(key), str) or not record[key].strip():
            raise ValueError(f"文章缺失有效字段: {key}")
    status = record.get("status")
    if status not in ("processing", "ok", "skip", "error"):
        raise ValueError(f"不支持的文章状态: {status}")
    post_ids = []
    if status == "ok":
        info, mapping = record.get("article_info"), record.get("material_post_mapping")
        if not isinstance(info, dict) or info.get("status") != "ok":
            raise ValueError("成功记录必须包含 status=ok 的 article_info")
        if not isinstance(mapping, dict):
            raise ValueError("成功记录必须包含 material_post_mapping")
        used_ids = info.get("used_material_ids")
        if not isinstance(used_ids, list) or not used_ids:
            raise ValueError("成功文章的 used_material_ids 不能为空")
        for material_id in used_ids:
            if not isinstance(material_id, str) or material_id not in mapping:
                raise ValueError(f"实际采用素材无法反查原帖: {material_id!r}")
            post_id = mapping[material_id]
            if (not isinstance(post_id, (str, int)) or isinstance(post_id, bool)
                    or post_id in (None, "", "UNKNOWN")):
                raise ValueError(f"素材对应的原帖 ID 无效: {material_id}")
            if post_id not in post_ids:
                post_ids.append(post_id)
    elif status == "skip" and record.get("type") == ARTICLE_TYPE_VIRAL:
        # 保留二阶段看图验货淘汰(usable=False)的原帖ID，避免下次轮询死循环重跑
        post_ids = [
            pid for pid in (record.get("post_id_list") or [])
            if pid not in (None, "", "UNKNOWN")
        ]
    record["post_id_list"] = post_ids
    for key, default in {
        "article_info": None, "error_message": None, "error_history": [],
        "raw_response": None, "attempt_count": 0,
    }.items():
        record.setdefault(key, default)
    return record


# coin/stance、论据[{source_post_id,core_fact,dimension,shelf_life,impact_weight}]
# → {is_skip,record,coin,stance,reason?}；可执行任务另含full_prompt/materials/image_mapping/model_name。
# : 引用限额只统计已发布文章，不预占并发任务；model_name仅为原有元数据，实际模型仍由medium组选择。
def build_article_task(coin, stance, ev_list, article_manager):
    brief = {"task": {"topic": coin, "stance": stance, "max_chars": ARTICLE_MAX_CHARS}, "materials": []}
    model_name = "gemini-3.1-pro-preview"
    record = {
        "source": BINANCE_SOURCE, "type": ARTICLE_TYPE_ANALYSIS, "topic": coin, "stance": stance,
        "status": "processing", "creation_brief": brief, "material_post_mapping": {}, "post_id_list": [],
        "prompt_file_path": ARTICLE_PROMPT_FILE_PATH, "model_name": model_name,
        "prompt_version": prompt_version, "article_info": None, "error_message": None,
        "error_history": [], "raw_response": None, "attempt_count": 0,
    }
    candidates = [item for item in ev_list
                  if isinstance(item, dict) and isinstance(item.get("source_post_id"), (str, int))
                  and not isinstance(item.get("source_post_id"), bool) and item["source_post_id"] not in ("", "UNKNOWN")
                  and isinstance(item.get("core_fact"), str) and item["core_fact"].strip()]
    usage = get_post_usage_counts(
        article_manager, BINANCE_SOURCE, list(dict.fromkeys(item["source_post_id"] for item in candidates)),
    )
    groups = defaultdict(list)
    for item in candidates:
        if usage.get(item["source_post_id"], 0) < ARTICLE_POST_USAGE_LIMIT:
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
            "status": "skip", "text": "", "god_comments": [], "image_placeholders": [], "used_material_ids": [],
            "score": None, "reason": reason, "image_mapping": {},
        }
        return {"is_skip": True, "record": record, "coin": coin, "stance": stance, "reason": reason}
    materials, image_mapping = transform_mlus(selected)
    brief["materials"] = materials
    record["material_post_mapping"] = {
        material["id"]: evidence["source_post_id"] for material, evidence in zip(materials, selected)
    }
    prompt = read_file_to_str(ARTICLE_PROMPT_FILE_PATH)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"文章提示词为空或读取失败: {ARTICLE_PROMPT_FILE_PATH}")
    return {
        "is_skip": False, "record": record, "full_prompt": f"{prompt}\n{json.dumps(brief, ensure_ascii=False)}",
        "materials": materials, "image_mapping": image_mapping, "coin": coin, "stance": stance, "model_name": model_name,
    }


# build_article_task的任务 → {status,article_info,error_history,...}记录；只保存ok，数据库异常上抛。
def execute_article_task(task_data, article_manager):
    record = task_data["record"]
    coin, stance = task_data["coin"], task_data["stance"]
    if task_data.get("is_skip"):
        logger.info("[文章/跳过] 选材不足 | 币种: [%s] | 立场: [%s] | 原因: [%s]", coin, stance, task_data["reason"])
        return record
    started = time.monotonic()
    image_mapping = task_data["image_mapping"]
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        record["attempt_count"], record["raw_response"] = attempt, None
        try:
            result = generate_content(prompt=task_data["full_prompt"], preset_model_group="medium")
            record["raw_response"] = result.get("content", "")
            info = string_to_object(record["raw_response"])
            valid, error = check_article_info(info, task_data["materials"], image_mapping, ARTICLE_MAX_CHARS)
            if not valid:
                raise ValueError(error)
            info["image_mapping"] = {key: dict(image_mapping[key]) for key in info["image_placeholders"]}
            record["article_info"], record["status"], record["error_message"] = info, info["status"], None
            break
        except Exception as exc:
            error_message = f"{type(exc).__name__}: {exc}"
            record["error_history"].append({
                "attempt": attempt, "error_message": error_message, "raw_response": record["raw_response"],
            })
            exhausted = attempt == LLM_MAX_RETRIES
            if exhausted:
                record.update(status="error", article_info=None, error_message=error_message)
            log = logger.error if exhausted else logger.warning
            log(
                "%s[文章/生成] 模型调用或校验失败 | 币种: [%s] | 立场: [%s] | 尝试: [%s/%s] "
                "| 原因: [%s] | 后续: [%s] | 排查: [检查模型服务、返回字段与素材引用]",
                "❌ " if exhausted else "", coin, stance, attempt, LLM_MAX_RETRIES, exc,
                "返回错误记录，不入库" if exhausted else f"{2 ** attempt} 秒后重试", exc_info=exhausted,
            )
            if exhausted:
                return record
            time.sleep(2 ** attempt)
    if record["status"] != "ok":
        if record["status"] == "skip":
            logger.info(
                "[文章/跳过] 模型未生成文章 | 币种: [%s] | 立场: [%s] | 原因: [%s]",
                coin, stance, (record.get("article_info") or {}).get("reason", "未说明"),
            )
        return record
    saved_record = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved_record])
    logger.info(
        "[文章/完成] 已校验并入库 | 币种: [%s] | 立场: [%s] | 尝试: [%s] | 耗时: [%.2f 秒]",
        coin, stance, record["attempt_count"], time.monotonic() - started,
    )
    return saved_record


# 分组论据 → 每币种/立场2个任务 → 并发执行；返回正常返回的记录列表，保留单任务异常隔离。
def generate_analysis_articles_once():
    started = time.monotonic()
    grouped = extract_and_group_valid_evidences()
    groups = [(coin, stance, items) for coin, stances in grouped.items() for stance, items in stances.items()]
    manager = GeneratedArticleManager(gen_db_object())
    tasks = [build_article_task(coin, stance, items, manager) for coin, stance, items in groups
             for _ in range(ARTICLE_VARIANTS)]
    local_skips = sum(bool(task.get("is_skip")) for task in tasks)
    logger.info(
        "[文章/计划] 选材完成 | 币种: [%s] | 分组: [%s] | 任务: [%s] | 本地跳过: [%s] "
        "| 模型调用上限: [%s，含重试] | 选材: [引用次数<%s，素材%s—%s条]",
        len(grouped), len(groups), len(tasks), local_skips, (len(tasks) - local_skips) * LLM_MAX_RETRIES,
        ARTICLE_POST_USAGE_LIMIT, ARTICLE_MIN_MATERIALS, ARTICLE_MATERIAL_LIMIT,
    )
    results, raised_count, model_skips = [], 0, 0
    if tasks:
        with ThreadPoolExecutor(max_workers=ARTICLE_CONCURRENCY) as executor:
            futures = {executor.submit(execute_article_task, task, manager): task for task in tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    model_skips += result.get("status") == "skip" and not task.get("is_skip")
                except Exception:
                    raised_count += 1
                    logger.exception(
                        "❌ [文章/执行] 任务异常结束 | 币种: [%s] | 立场: [%s] | 后续: [继续其他任务] "
                        "| 排查: [检查任务字段与数据库；保存失败时先核对入库结果]", task["coin"], task["stance"],
                    )
    counts = Counter(item.get("status") for item in results)
    logger.info(
        "[文章/完成] 本轮任务结束 | 任务: [%s] | 成功: [%s] | 失败: [%s] "
        "| 跳过: [%s，本地%s/模型%s] | 耗时: [%.2f 秒]",
        len(tasks), counts["ok"], counts["error"] + raised_count, counts["skip"],
        local_skips, model_skips, time.monotonic() - started,
    )
    return results


# 有返回记录等待600秒，无记录或异常等待60秒；成功/跳过/错误记录均沿用原节奏。
def generate_analysis_articles():
    while True:
        try:
            results = generate_analysis_articles_once()
            time.sleep(ARTICLE_GENERATION_INTERVAL_SECONDS if results else 60)
        except Exception:
            logger.exception(
                "❌ [文章/轮询] 本轮中断 | 后续: [60 秒后重新查询] "
                "| 排查: [检查热榜、论据、提示词与数据库；保存结果不确定时先核对记录]"
            )
            time.sleep(60)


# 管理器、来源、候选post_id序列 → 已执行过二阶段(含 status=ok 与 status=skip)的原帖ID字符串集合。
def get_remodeled_viral_post_ids(article_manager, source, post_ids):
    raw_ids = list(dict.fromkeys(post_ids))
    if not raw_ids:
        return set()
    str_ids = [str(pid).strip() for pid in raw_ids if str(pid).strip()]
    int_ids = [int(pid) for pid in str_ids if pid.isdigit()]
    query_ids = list(dict.fromkeys([*raw_ids, *str_ids, *int_ids]))
    articles = article_manager.find_articles(
        query={
            "source": source,
            "type": ARTICLE_TYPE_VIRAL,
            "status": {"$in": ["ok", "skip"]},
            "post_id_list": {"$in": query_ids},
        },
        limit=0,
    ) or []
    used = set()
    for article in articles:
        for pid in article.get("post_id_list", []):
            if pid is not None:
                used.add(str(pid).strip())
    return used


# 数据库当前版本 viral_screen(keep==True) → 经48h同源事件去重后的待重塑原帖列表（按互动热度排序）。
def extract_valid_viral_posts(article_manager):
    started = time.monotonic()
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    now = time.time()

    # 查询近48小时已生成(status=ok)的高流量文章的 event_key，用于跨轮次同源热点冷却
    recent_viral_articles = article_manager.find_articles(
        query={
            "source": BINANCE_SOURCE,
            "type": ARTICLE_TYPE_VIRAL,
            "status": "ok",
            "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=48)},
        },
        limit=0,
    ) or []
    used_event_keys = Counter()
    for art in recent_viral_articles:
        ek = (((art.get("creation_brief") or {}).get("stage1_screen") or {}).get("event_key") or "").strip()
        if ek:
            used_event_keys[ek] += 1

    candidates = []
    for post in posts:
        screen = post.get("viral_screen")
        if not isinstance(screen, dict) or screen.get("prompt_version") != prompt_version:
            continue
        if screen.get("keep") is not True:
            continue
        engagement = post.get("engagement") or {}
        view_count = engagement.get("view_count", 0)
        like_count = engagement.get("like_count", 0) or 0
        comment_count = engagement.get("comment_count", 0) or 0
        if not isinstance(view_count, (int, float)) or isinstance(view_count, bool) or view_count <= VIRAL_MIN_VIEW_COUNT:
            continue
        try:
            publish_time = _publish_time_seconds(post.get("publish_time", 0))
        except ValueError:
            continue
        age_hours = (now - publish_time) / 3600
        if age_hours > VIRAL_POST_MAX_AGE_HOURS or age_hours < -1:
            continue
        text = (post.get("content") or {}).get("text_content") or ""
        if any(match.group(1) == "视频" for match in MEDIA_PATTERN.finditer(text)):
            continue
        if not _is_valid_raw_post(post):
            continue
        hot_weight = view_count + like_count * 20 + comment_count * 50
        candidates.append((post, hot_weight, publish_time))

    candidate_ids = [item[0].get("post_id") for item in candidates if item[0].get("post_id") is not None]
    processed_ids = get_remodeled_viral_post_ids(article_manager, BINANCE_SOURCE, candidate_ids)
    unprocessed = [
        item for item in candidates
        if str(item[0].get("post_id", "")).strip() not in processed_ids
    ]
    # 优先处理互动热度最高的帖子
    unprocessed.sort(key=lambda item: (item[1], item[2]), reverse=True)

    result, event_skipped = [], 0
    for post, _, _ in unprocessed:
        event_key = ((post.get("viral_screen") or {}).get("event_key") or "").strip()
        # 同源热点事件48小时内最多保留 2 条，避免连发多条相同假酒/同案情刷屏
        if event_key and used_event_keys[event_key] >= 2:
            event_skipped += 1
            continue
        if event_key:
            used_event_keys[event_key] += 1
        result.append(post)

    logger.info(
        "[高流量/选材] 已筛选待重塑候选帖 | 扫描: [%s] | 粗筛达标: [%s] | 已处理跳过: [%s] "
        "| 同源热点冷却跳过: [%s] | 本轮待精改: [%s] | 耗时: [%.2f 秒]",
        len(posts), len(candidates), len(candidates) - len(unprocessed),
        event_skipped, len(result), time.monotonic() - started,
    )
    return result


# 第二阶段模型返回 {usable,reject_reason,topic_tag,selected_images,viral_analysis,post_text,bait_comments} → (bool,错误文本)。
def check_viral_article_info(raw_info, placeholders, max_chars):
    expected_keys = {
        "usable", "decision_reason", "topic_tag", "selected_images",
        "image_role", "viral_analysis", "post_text", "bait_comments",
        "viral_score",
    }
    error = _key_error(raw_info, expected_keys, "高流量二阶段重塑")
    if error:
        return False, error

    usable = raw_info["usable"]
    if not isinstance(usable, bool):
        return False, "usable 必须是布尔值"

    # 无论 usable 为 true 还是 false，decision_reason 都必须是非空字符串
    decision_reason = raw_info["decision_reason"]
    if not isinstance(decision_reason, str) or not decision_reason.strip():
        return False, "decision_reason 必须是非空字符串（放行或淘汰均需说明原因）"

    # 分支1：二阶段看图验货后判定不可用（熔断淘汰，直接放行避免触发无意义重试）
    if not usable:
        return True, ""

    # 分支2：二阶段判定可用，严格校验成稿字段
    allowed_tags = (
        "搞钱与盈亏", "情感与两性", "奇葩与吃瓜",
        "人性与潜规则", "职场与生存", "搞笑与整活",
    )
    if raw_info["topic_tag"] not in allowed_tags:
        return False, f"topic_tag 取值不合法: {raw_info['topic_tag']!r}"

    selected_images = raw_info["selected_images"]
    if not isinstance(selected_images, list) or any(not isinstance(item, str) or not item for item in selected_images):
        return False, "selected_images 必须是非空字符串组成的列表"
    if len(selected_images) != len(set(selected_images)):
        return False, "selected_images 不允许重复"
    if len(selected_images) > 3:
        return False, "selected_images 最多保留 3 张图片"
    if any(item not in placeholders for item in selected_images):
        return False, f"selected_images 必须来自原帖真实图片占位符 {sorted(placeholders)}"

    # 校验 image_role：有图时强制非空（防剧透锚点）；无图时仅校验类型，不因偶发填写"无"而浪费重试
    image_role = raw_info["image_role"]
    if not isinstance(image_role, str):
        return False, "image_role 必须是字符串"
    if selected_images and not image_role.strip():
        return False, "保留配图时 image_role 必须是非空字符串"

    if not isinstance(raw_info["viral_analysis"], str) or not raw_info["viral_analysis"].strip():
        return False, "usable 为 true 时 viral_analysis 必须是非空字符串"

    text = raw_info["post_text"]
    if not isinstance(text, str) or not text.strip():
        return False, "post_text 必须是非空字符串"
    if len(re.sub(r"\s", "", text)) > max_chars:
        return False, f"post_text 非空白字符数超过 {max_chars}"
    if re.search(r"\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]", text):
        return False, "post_text 不能包含图片或视频占位符"

    comments = raw_info["bait_comments"]
    if (not isinstance(comments, list) or not 2 <= len(comments) <= 3
            or any(not isinstance(item, str) or not item.strip() for item in comments)):
        return False, "bait_comments 必须是包含 2—3 条非空字符串的列表"
    if any(len(re.sub(r"\s", "", item)) > 20 for item in comments):
        return False, "bait_comments 单条评论字数过长（需控制在18字左右）"

    # 校验 viral_score：排除 bool，兼容 int/float（如 8 或 8.5）
    viral_score = raw_info["viral_score"]
    if isinstance(viral_score, bool) or not isinstance(viral_score, (int, float)):
        return False, "viral_score 必须是数字"
    if not (0.0 < float(viral_score) <= 10.0):
        return False, "usable 为 true 时 viral_score 必须在 (0, 10] 区间内"

    return True, ""

# 单篇粗筛放行原帖 → 构建第二阶段多模态（直传原图+原文）验货与洗稿重塑任务。
def build_viral_article_task(post):
    post_id = post.get("post_id")
    post_id_str = str(post_id or "").strip()
    screen = post.get("viral_screen") or {}
    normalized_text, file_paths, placeholders = normalize_post_media(post)
    source_mapping = build_source_image_mapping(post)
    image_mapping = {
        img_id: {
            "source": BINANCE_SOURCE,
            "source_post_id": post_id,
            "original_image_id": img_id,
            "original_image_id_list": [img_id],
            **meta,
        }
        for img_id, meta in source_mapping.items()
    }


    comments = post.get("comments", [])
    comments = [comment_info["content"] for comment_info in comments]


    brief = {
        "current_year": datetime.now(BEIJING_TZ).year,
        "original_post": {
            "post_id": post_id_str,
            "text_content": normalized_text.strip(),
            "comments":comments,
            "available_images": list(placeholders.keys()),
        },
        "stage1_screen": {
            "event_key": screen.get("event_key", ""),
        },
    }
    model_name = "gemini-3.1-pro-preview"
    record = {
        "source": BINANCE_SOURCE,
        "type": ARTICLE_TYPE_VIRAL,
        "topic": "奇葩与吃瓜",
        "stance": "无主语直抛",
        "status": "processing",
        "creation_brief": brief,
        "material_post_mapping": {"M1": post_id},
        "post_id_list": [post_id],
        "prompt_file_path": VIRAL_STAGE2_PROMPT_FILE_PATH,
        "model_name": model_name,
        "prompt_version": prompt_version,
        "article_info": None,
        "error_message": None,
        "error_history": [],
        "raw_response": None,
        "attempt_count": 0,
    }
    prompt = read_file_to_str(VIRAL_STAGE2_PROMPT_FILE_PATH)
    if not isinstance(prompt, str) or not prompt.strip():
        raise ValueError(f"高流量文章提示词为空或读取失败: {VIRAL_STAGE2_PROMPT_FILE_PATH}")
    return {
        "record": record,
        "full_prompt": f"{prompt}\n{json.dumps(brief['original_post'], ensure_ascii=False)}",
        "file_paths": file_paths,
        "placeholders": set(source_mapping.keys()),
        "image_mapping": image_mapping,
        "post_id": post_id_str,
        "topic": "奇葩与吃瓜",
        "model_name": model_name,
    }


# 执行单篇高流量帖子多模态验货与洗稿任务，写入 GeneratedArticleManager（支持 usable=False 转为 skip 落库）。
def execute_viral_article_task(task_data, article_manager):
    record = task_data["record"]
    post_id = task_data["post_id"]
    started = time.monotonic()
    image_mapping = task_data["image_mapping"]
    placeholders = task_data["placeholders"]
    file_paths = task_data["file_paths"]

    for attempt in range(1, LLM_MAX_RETRIES + 1):
        record["attempt_count"], record["raw_response"] = attempt, None
        try:
            result = generate_content(
                prompt=task_data["full_prompt"],
                preset_model_group="medium",
                file_paths=file_paths,
            )
            record["raw_response"] = result.get("content", "")
            raw_info = string_to_object(record["raw_response"])
            valid, error = check_viral_article_info(raw_info, placeholders, ARTICLE_MAX_CHARS)
            if not valid:
                raise ValueError(error)

            decision_reason = raw_info["decision_reason"].strip()

            # 二阶段看真图后判定不可用 -> 记为 skip 并落库，防止后续死循环重复拉取
            if not raw_info["usable"]:
                record["topic"] = "无效噪音"
                record["status"] = "skip"
                record["error_message"] = None
                record["article_info"] = {
                    "status": "skip",
                    "usable": False,
                    "decision_reason": decision_reason,
                    "topic_tag": "无效噪音",
                    "selected_images": [],
                    "image_role": "",
                    "viral_analysis": "",
                    "post_text": "",
                    "bait_comments": [],
                    "viral_score": 0.0,
                    "image_mapping": {},
                }
                break

            # 二阶段判定可用 -> 组装正式发布结构
            selected_images = raw_info["selected_images"]
            topic_tag = raw_info["topic_tag"]
            viral_score = round(float(raw_info["viral_score"]), 1)
            record["topic"] = topic_tag
            record["status"] = "ok"
            record["error_message"] = None
            record["article_info"] = {
                "status": "ok",
                "usable": True,
                "decision_reason": decision_reason,
                "topic_tag": topic_tag,
                "selected_images": selected_images,
                "image_role": raw_info["image_role"].strip(),
                "viral_analysis": raw_info["viral_analysis"].strip(),
                "post_text": raw_info["post_text"].strip(),
                "used_material_ids": ["M1"],
                "bait_comments": raw_info["bait_comments"],
                "viral_score": viral_score,
                "image_mapping": {key: dict(image_mapping[key]) for key in selected_images},
            }
            break
        except Exception as exc:
            error_message = f"{type(exc).__name__}: {exc}"
            record["error_history"].append({
                "attempt": attempt, "error_message": error_message, "raw_response": record["raw_response"],
            })
            exhausted = attempt == LLM_MAX_RETRIES
            if exhausted:
                record.update(status="error", article_info=None, error_message=error_message)
            log = logger.error if exhausted else logger.warning
            log(
                "%s[高流量/精改] 模型调用或校验失败 | 原帖: [%s] | 尝试: [%s/%s] "
                "| 原因: [%s] | 后续: [%s] | 排查: [检查模型服务、返回字段与配图引用]",
                "❌ " if exhausted else "", post_id, attempt, LLM_MAX_RETRIES, exc,
                "返回错误记录，不入库" if exhausted else f"{2 ** attempt} 秒后重试", exc_info=exhausted,
            )
            if exhausted:
                return record
            time.sleep(2 ** attempt)

    saved_record = prepare_article_record_for_db(record)
    article_manager.upsert_articles([saved_record])
    if saved_record["status"] == "skip":
        logger.info(
            "[高流量/二阶段淘汰] 看图验货未通过已记skip | 原帖: [%s] | 原因: [%s] | 耗时: [%.2f 秒]",
            post_id, saved_record["article_info"]["decision_reason"], time.monotonic() - started,
        )
    else:
        logger.info(
            "[高流量/完成] 已重塑并入库 | 原帖: [%s] | 赛道: [%s] | 评分: [%s] | 选图: [%s张] | 原因: [%s] | 尝试: [%s] | 耗时: [%.2f 秒]",
            post_id, saved_record["topic"], saved_record["article_info"]["viral_score"],
            len(saved_record["article_info"]["selected_images"]),
            saved_record["article_info"]["decision_reason"],
            record["attempt_count"], time.monotonic() - started,
        )
    return saved_record

# 单轮高流量文章重塑：筛选粗筛通过单帖 → 1对1构建多模态重塑任务 → 并发生成并存入 GeneratedArticleManager。
def generate_viral_articles_once():
    started = time.monotonic()
    manager = GeneratedArticleManager(gen_db_object())
    posts = extract_valid_viral_posts(manager)
    tasks = [build_viral_article_task(post) for post in posts]
    logger.info(
        "[高流量/计划] 二阶段多模态精改就绪 | 待处理任务: [%s] | 并发上限: [%s] | 模型调用上限: [%s，含重试]",
        len(tasks), ARTICLE_CONCURRENCY, len(tasks) * LLM_MAX_RETRIES,
    )
    results, raised_count = [], 0
    if tasks:
        with ThreadPoolExecutor(max_workers=ARTICLE_CONCURRENCY) as executor:
            futures = {executor.submit(execute_viral_article_task, task, manager): task for task in tasks}
            for future in as_completed(futures):
                task = futures[future]
                try:
                    results.append(future.result())
                except Exception:
                    raised_count += 1
                    logger.exception(
                        "❌ [高流量/执行] 任务异常结束 | 原帖: [%s] | 后续: [继续其他任务] "
                        "| 排查: [检查任务字段与数据库；保存失败时先核对入库结果]",
                        task["post_id"],
                    )
    counts = Counter(item.get("status") for item in results)
    logger.info(
        "[高流量/汇总] 本轮精改结束 | 任务: [%s] | 成功(ok): [%s] | 二阶段淘汰(skip): [%s] | 失败(error): [%s] | 耗时: [%.2f 秒]",
        len(tasks), counts["ok"], counts["skip"], counts["error"] + raised_count, time.monotonic() - started,
    )
    return results


# 持续轮询生成高流量重塑文章；有返回记录等待600秒，无记录或异常等待60秒。
def generate_viral_articles():
    while True:
        try:
            results = generate_viral_articles_once()
            time.sleep(ARTICLE_GENERATION_INTERVAL_SECONDS if results else 60)
        except Exception:
            logger.exception(
                "❌ [高流量/轮询] 本轮中断 | 后续: [60 秒后重新查询] "
                "| 排查: [检查原帖筛查结果、提示词与数据库]"
            )
            time.sleep(60)


# text/topic → 追加热门话题与币种标签后的正文；保留原标签顺序及重复标签。
# : tag_count原本未参与逻辑；保留公开参数，但仍只取1个热门话题。
def add_hot_topic_to_article(text, topic, tag_count=2):
    hashtags = [item["hashtag"] for item in fetch_binance_hot_hashtags()]
    tags = hashtags[:1] + [f"#{topic}", f"${topic} "]
    hot_coins = fetch_binance_future_hot_coins()
    if hot_coins:
        symbol = hot_coins[0]["symbol"].replace("USDT", "").replace("USDC", "")
        tags.extend([f"#{symbol}", f"${symbol} "])
    logger.debug("[文章/标签] 标签已准备 | 主题: [%s] | 标签: [%s]", topic, tags)
    return text + "\n\n\n" + "\n".join(tags)


# 正文 → [{text,delay}]；币种片段delay=2、其后普通片段强制一个换行，其他delay=0。
def _build_publish_text_list(text):
    if not text:
        return []
    result, last_end, after_coin = [], 0, False
    for match in [*COIN_TAG_PATTERN.finditer(text), None]:
        segment = text[last_end:match.start() if match else len(text)]
        if after_coin:
            segment = "\n" + segment.lstrip(" \t\r\n")
        if segment:
            result.append({"text": segment, "delay": 0})
        if match is None:
            break
        result.append({"text": match.group(0), "delay": 2})
        last_end, after_coin = match.end(), True
    return result


# 管理器文章{topic,stance,publish_attempts,article_info{text,score,image_placeholders,image_mapping}}
# → 按评分/图片去重/账号冷却发布分析帖；回写账号文件及文章发布字段，不返回业务数据。
def _publish_articles_once(article_manager):
    now = time.time()
    state = _load_and_init_publish_state()

    recent_success = article_manager.find_articles(query={
        "status": "ok", "publish_status": "success", "publish_time": {"$gte": now - 6 * 3600},
    }) or []
    used_addresses, used_primary = set(), set()
    for article in recent_success:
        addresses, primary = _extract_article_image_addresses(article)
        used_addresses.update(addresses)
        used_primary.update(primary)

    # 仅查询非 viral 类型的分析类文章，避免与流量帖队列串扰
    raw_candidates = article_manager.find_articles(query={
        "type": {"$ne": ARTICLE_TYPE_VIRAL},
        "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=6)},
        "status": "ok", "publish_status": {"$ne": "success"},
    }) or []
    hot_coins = {str(coin).lower() for coin in (get_hot_coin() or [])}
    stats = Counter()
    candidates = []
    for article in raw_candidates:
        info = article.get("article_info")
        if article.get("prompt_version") != prompt_version:
            stats["version"] += 1
            continue
        if not isinstance(info, dict) or not isinstance(info.get("image_placeholders", []), list):
            stats["structure"] += 1
            continue
        if article.get("publish_attempts", 0) >= PUBLISH_MAX_ATTEMPTS:
            stats["attempts"] += 1
            continue
        if (article.get("topic") or "").lower() not in hot_coins:
            stats["topic"] += 1
            continue
        addresses, _ = _extract_article_image_addresses(article)
        if addresses & used_addresses:
            stats["image"] += 1
            continue
        candidates.append((article, addresses))
    candidates.sort(key=lambda item: item[0]["article_info"].get("score", 0), reverse=True)
    logger.info(
        "[发布/计划] 近6小时文章已按评分筛选 | 查询: [%s] | 候选: [%s] "
        "| 过滤: [版本%s/结构%s/尝试超限%s/非热币%s/图片重复%s] | 尝试上限: [%s] "
        "| 近6小时已发: [%s] | 占用图片: [%s] | 账号: [%s] | 冷却: [账号%s秒/同主题%s秒]",
        len(raw_candidates), len(candidates), stats["version"], stats["structure"], stats["attempts"],
        stats["topic"], stats["image"], PUBLISH_MAX_ATTEMPTS, len(recent_success), len(used_primary),
        len(ACCOUNTS), ACCOUNT_COOLDOWN_SECONDS, ACCOUNT_TOPIC_COOLDOWN_SECONDS,
    )
    for account in ACCOUNTS:
        if not candidates:
            break
        account_state = state[account]
        remaining = ACCOUNT_COOLDOWN_SECONDS - (now - account_state["last_publish_time"])
        if remaining > 0:
            logger.debug("[发布/账号] 仍在冷却 | 账号: [%s] | 剩余: [%s 秒]", account, int(remaining))
            continue
        selected_index = None
        for index, (article, addresses) in enumerate(candidates):
            last_topic_time = account_state["topic_publish_history"].get(article.get("topic", ""), 0)
            if now - last_topic_time < ACCOUNT_TOPIC_COOLDOWN_SECONDS or addresses & used_addresses:
                continue
            selected_index = index
            break
        if selected_index is None:
            logger.debug("[发布/账号] 所有候选仍在主题冷却或图片冲突 | 账号: [%s]", account)
            continue
        article, addresses = candidates[selected_index]
        info, topic, stance = article["article_info"], article.get("topic", ""), article.get("stance", "")
        started = time.monotonic()
        stage, api_result = "准备正文、媒体及配置", "尚未调用"
        try:
            text = info.get("text", "")
            if topic:
                text = re.sub(rf"(?<!\$)\b{re.escape(topic)}\b", lambda match: "$" + topic + " ",
                              text, flags=re.IGNORECASE)
            text = add_hot_topic_to_article(text, topic) + "\n\n\n\n\n\n👇"
            if topic and not text.startswith(f"${topic}"):
                text = f"${topic} {text}"
            text_list = _build_publish_text_list(text)
            image_paths = [item.get("local_path") or None for item in info.get("image_mapping", {}).values()]
            session_dir = get_config(f"{account}_browser_session_dir")
            attempts = article.get("publish_attempts", 0) + 1
            stage, api_result = "调用发布接口", "未知"
            err, success, post_id = create_binance_post(
                content=text, image_path_list=image_paths, user_data_dir=session_dir,
                chart_info={"coin": topic, "bridge": "USDT", "type": "future"}, sentiment=stance,
                debug=True, text_list=text_list,
            )
            post_id, api_result = str(post_id), "成功" if success else "失败"
            problems, saved = [], []
            latest_price = None
            try:
                latest_price = get_latest_crypto_price(topic)
            except Exception as exc:
                logger.warning(
                    "[发布/行情] 获取发布时刻价格失败 | 主题: [%s] | 异常: [%s] | 后续: [价格记为 None 继续落库]",
                    topic, exc,
                )
            if success:
                new_total = account_state["total_success"] + 1
                account_state["total_success"] = new_total
                account_state["last_publish_time"] = now
                if topic:
                    account_state["topic_publish_history"][topic] = now
                state_updates = {"total_success": new_total, "last_publish_time": now}
                topic_update = (topic, now) if topic else None
                changes = {
                    "publish_status": "success", "published_by": account, "publish_time": now,
                    "binance_post_id": post_id, "publish_attempts": attempts, "publish_price": latest_price,
                }
                used_addresses.update(addresses)
            else:
                error = f"发布接口未确认成功；可能是会话失效、网络异常、限流或内容校验失败；详情: {err}"
                account_state["last_error_msg"], account_state["last_error_time"] = error, now
                state_updates = {"last_error_msg": error, "last_error_time": now}
                topic_update = None
                changes = {
                    "publish_status": "failed", "last_error": error,
                    "publish_attempts": attempts, "publish_price": latest_price,
                }
            stage = "保存已确认的发布结果"
            try:
                _update_account_publish_state(account, state_updates, topic_update=topic_update)
                saved.append("账号状态")
            except Exception as exc:
                problems.append(("写入账号状态", exc))
            article.update(changes)
            try:
                article_manager.upsert_articles([article])
                saved.append("数据库状态")
            except Exception as exc:
                problems.append(("回写数据库", exc))
            if problems:
                details = "; ".join(f"{label}: {type(exc).__name__}: {exc}" for label, exc in problems)
                raise RuntimeError(f"{details} | 已保存: [{', '.join(saved) or '无'}]") from problems[0][1]
        except Exception as exc:
            raise RuntimeError(
                f"发布阶段[{stage}]失败 | 账号: [{account}] | 文章: [{article.get('_id')}] "
                f"| 主题: [{topic}] | 接口结果: [{api_result}]；请核对远端结果与本地状态 | 原因: [{exc}]"
            ) from exc
        if success:
            candidates.pop(selected_index)
        log = logger.info if success else logger.error
        log(
            "%s[发布/文章] 本次处理结束 | 账号: [%s] | 主题: [%s] | 文章: [%s] "
            "| 评分: [%s] | 尝试: [%s/%s] | 耗时: [%.2f 秒] | 结果: [%s] | 说明: [%s]",
            "" if success else "❌ ", account, topic, article.get("_id"), info.get("score"),
            attempts, PUBLISH_MAX_ATTEMPTS, time.monotonic() - started, api_result,
            "远端发布与本地回写完成" if success else error,
        )


# 单轮函数、管理器、间隔 → 持续轮询；仅在调度边界打印一次未处理异常，保留失败后继续的策略。
def _poll_task(task, manager, interval, action):
    while True:
        try:
            task(manager)
        except Exception:
            logger.exception(
                "❌ [%s/轮询] 本轮中断 | 后续: [%s 秒后重新扫描] "
                "| 排查: [检查账号配置、外部接口与数据库；远端操作成功时先核对回写结果]",
                action, interval,
            )
        finally:
            time.sleep(interval)


# 独立文章管理器 → 发布轮询；每轮结束后等待600秒。
def auto_publish_articles():
    _poll_task(_publish_articles_once, GeneratedArticleManager(gen_db_object()), PUBLISH_POLL_SECONDS, "发布")


# action、是否保留用户信息 → ({account:{cookies,csrf_token,user_info?}},异常账号数)；逐账号隔离故障。
def _load_account_credentials(action, with_user_info=False):
    credentials, errors = {}, 0
    for account in ACCOUNTS:
        try:
            session_dir = get_config(f"{account}_browser_session_dir")
            cookies, token, user_info = get_auth_tokens_robust(session_dir, debug=True)
            if not cookies or not token:
                logger.warning(
                    "[%s/账号] 凭证不完整 | 账号: [%s] | 后续: [跳过当前账号] "
                    "| 排查: [检查登录状态与浏览器会话]", action, account,
                )
                continue
            credentials[account] = {"cookies": cookies, "csrf_token": token}
            if with_user_info:
                credentials[account]["user_info"] = user_info or {}
        except Exception:
            errors += 1
            logger.exception(
                "❌ [%s/账号] 获取凭证失败 | 账号: [%s] | 后续: [继续其他账号] "
                "| 排查: [检查会话目录、浏览器启动与登录状态]", action, account,
            )
    return credentials, errors


# 神评列表、其他账号列表、帖子ID/立场 → {评论文本:{account,comment_id,success,error_info}}。
# : 原规则按评论文本作键，重复文本会覆盖；发送异常仍逐账号隔离且不产生结果条目。
def interact_comment(god_comments, other_account_list, post_id, stance):
    results, unconfirmed, raised = {}, 0, 0
    accounts = random.sample(other_account_list, len(other_account_list))
    for account, text in zip(accounts, god_comments):
        session_dir = get_config(f"{account}_browser_session_dir")
        try:
            error, success, comment_id = comment_on_binance_post(
                post_url=f"https://www.binance.com/zh-CN/square/post/{post_id}",
                comment=text, user_data_dir=session_dir, sentiment=stance,
                debug=True
            )
            results[f"{text}"] = {
                "account": account, "comment_id": str(comment_id) if comment_id else "",
                "success": success, "error_info": error,
            }
            unconfirmed += not success
        except Exception:
            raised += 1
            logger.exception(
                "❌ [互动/评论] 发送异常，远端结果未知 | 帖子: [%s] | 账号: [%s] "
                "| 后续: [沿用账号隔离，继续其他评论] | 排查: [检查登录、网络和接口响应]",
                post_id, account,
            )
    if accounts and god_comments:
        log = logger.warning if unconfirmed or raised else logger.info
        log(
            "[互动/评论汇总] 评论尝试结束 | 帖子: [%s] | 配对: [%s] | 未确认: [%s] "
            "| 调用异常: [%s] | 结果条目: [%s] | 未确认排查: [检查结果字典中的error_info及远端评论]",
            post_id, min(len(accounts), len(god_comments)), unconfirmed, raised, len(results),
        )
    return results


# 近24小时已发文章 → 点赞收藏/其他账号评论(兼容分析帖god_comments与流量帖bait_comments) → 回写互动状态。
def _hudong_once(article_manager):
    started = time.monotonic()
    credentials, _ = _load_account_credentials("互动")
    if not credentials:
        logger.warning("[互动/跳过] 无可用凭证 | 账号数: [%s] | 排查: [先恢复登录与会话]", len(ACCOUNTS))
        return
    now = datetime.now(timezone.utc)
    articles = article_manager.find_articles(query={
        "status": "ok", "publish_status": "success",
        "publish_time": {"$gte": (now - timedelta(hours=24)).timestamp()},
    }) or []
    pending = [article for article in articles if not article.get("interacted")]
    post_ids = [article.get("binance_post_id") for article in pending if article.get("binance_post_id")]
    if not post_ids:
        logger.info("[互动/完成] 没有可提交帖子 | 待处理: [%s] | 耗时: [%.2f 秒]",
                    len(pending), time.monotonic() - started)
        return
    try:
        like_and_bookmark(post_ids, credentials)
    except Exception:
        logger.exception(
            "❌ [互动/执行] 点赞收藏接口异常 | 帖子: [%s] | 后续: [不标记，等待下轮] "
            "| 排查: [检查凭证、网络与限流；已完成的远端操作可能被重试]", len(post_ids),
        )
        return
    for article in pending:
        other_accounts = [account for account in ACCOUNTS if account != article.get("published_by")]
        info = article.get("article_info") or {}
        comments = info.get("bait_comments") if article.get("type") == ARTICLE_TYPE_VIRAL else info.get("god_comments")
        raw_stance = article.get("stance")
        sentiment = raw_stance if raw_stance in ("看多", "看空") else None
        article["comment_interact_info"] = interact_comment(
            comments or [], other_accounts,
            article.get("binance_post_id"), sentiment,
        )
        article["interacted"], article["interaction_time"] = True, now.timestamp()
    try:
        article_manager.upsert_articles(pending)
    except Exception:
        logger.exception(
            "❌ [互动/回写] 接口已返回，状态保存失败 | 记录: [%s] | 后续: [结束本轮] "
            "| 排查: [检查数据库；重试前核对已完成的远端互动]", len(pending),
        )
        return
    logger.info(
        "[互动/完成] 接口返回且状态已保存 | 帖子: [%s] | 账号: [%s] | 标记记录: [%s] | 耗时: [%.2f 秒]",
        len(post_ids), len(credentials), len(pending), time.monotonic() - started,
    )

# 独立文章管理器 → 互动轮询；每轮结束后等待1200秒。
def hudong():
    _poll_task(_hudong_once, GeneratedArticleManager(gen_db_object()), INTERACTION_INTERVAL_SECONDS, "互动")


# 旧版post{content{text_content,mentioned_coins},logic_mul:[{visual_fact,semantic_core,narrative_role}]}
# → 搜索字符串；保留公开导出入口，输入形貌由调用方负责。
# : 此入口使用旧版媒体列表；当前evidences字典不能直接作为logic_mul传入。
def build_search_text(post):
    content = post.get("content") or {}
    text = MEDIA_PATTERN.sub("", content.get("text_content") or "").strip()
    coins = content.get("mentioned_coins") or []
    parts = [f"【文章正文】\n{text}\n关联币种：{', '.join(coins) if coins else '无'}\n"]
    for index, media in enumerate(post.get("logic_mul") or [], 1):
        visual, semantic, narrative = (media.get("visual_fact", {}), media.get("semantic_core", {}),
                                       media.get("narrative_role", {}))
        concepts = ", ".join(semantic.get("concepts", []) + visual.get("entities", []))
        logic = f"{semantic.get('message', '')} {narrative.get('logic_bridge', '')}".strip()
        parts.append(
            f"\n【配图{index}语义解析】\n核心概念：{concepts}\n画面描述：{visual.get('description', '')}\n"
            f"图文逻辑：{logic}\n关键数据(OCR)：{visual.get('ocr_text', '')}\n"
        )
    return "".join(parts).strip()


# post列表{content.text_content,logic_mul.images[{visual_fact,semantic_core}]} → [{doc_id,content:[文本/媒体块]}]。
# : 此入口仍使用旧版顶层images；当前evidences格式的图片描述不会自动迁移。
def process_posts(post_list):
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
                "type": "video" if kind == "VIDEO" else "image", "desc": visual.get("fact", ""),
                "ocr": visual.get("ocr_text", ""), "logic": image.get("semantic_core", {}).get("message", ""),
                "placeholder": f"[{prefix}_{doc_id}_{kind}_{counters[kind]:02d}]",
            })
            last_end = match.end()
        remaining = text[last_end:].strip()
        if remaining:
            blocks.append({"type": "text", "text": remaining})
        results.append({"doc_id": doc_id, "content": blocks})
    return results


# 无入参 → [{text_content,logic_mul}]；保留历史媒体结构判据，含图记录稳定优先。
def get_all_non_empty_logic_mul_with_clean_text():
    manager = UniversalPostManager(gen_db_object())
    posts = manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    results = [{"text_content": MEDIA_PATTERN.sub("", (post.get("content") or {}).get("text_content") or "").strip(),
                "logic_mul": post["logic_mul"]} for post in posts if post.get("logic_mul")]

    # logic_mul列表/字典 → bool；仅判断是否含图，不要求图片可用。
    def has_images(data):
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
    logger.info("[数据/导出] 正文已清洗，含图优先 | 扫描: [%s] | 导出: [%s]", len(posts), len(results))
    return results


# 文章列表{article_info,creation_brief} → 仅含正文/神评/配图/素材ID与创作brief的列表。
def clean_article_records(raw_records):
    results = []
    for item in raw_records:
        if not isinstance(item, dict):
            continue
        info, brief = item.get("article_info") or {}, item.get("creation_brief") or {}
        results.append({
            "article_info": {key: info.get(key, "" if key == "text" else [])
                             for key in ("text", "god_comments", "image_placeholders", "used_material_ids")},
            "creation_brief": {"task": brief.get("task", {}), "materials": brief.get("materials", [])},
        })
    return results


# 近6小时当前版本未发布文章随机抽10篇 → (清洗文章列表,[{origin_content,logic_mul}])。
# 原帖查询失败沿用返回空素材的策略；保留管理器提供的三种查询入口。
def get_need_check_article_info():
    manager = GeneratedArticleManager(gen_db_object())
    candidates = manager.find_articles(query={
        "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=6)},
        "status": "ok", "publish_status": {"$ne": "success"},
    }) or []
    candidates = [article for article in candidates if article.get("prompt_version") == prompt_version]
    random.shuffle(candidates)
    selected = candidates[:10]
    data = clean_article_records(selected)
    raw_ids = set()
    for article in selected:
        mapping = article.get("material_post_mapping") or {}
        if isinstance(mapping, dict):
            raw_ids.update(str(post_id).strip() for post_id in mapping.values() if post_id)
    source_posts = []
    if raw_ids:
        post_manager = UniversalPostManager(gen_db_object())
        ids = list(raw_ids)
        query_ids = ids + [int(post_id) for post_id in ids if post_id.isdigit()]
        query = {"post_id": {"$in": query_ids}}
        raw_posts = []
        try:
            if hasattr(post_manager, "collection"):
                raw_posts = list(post_manager.collection.find(query))
            elif hasattr(post_manager, "find_posts"):
                raw_posts = post_manager.find_posts(query=query) or []
            else:
                posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT) or []
                target_ids = set(query_ids)
                raw_posts = [post for post in posts if post.get("post_id") in target_ids]
        except Exception:
            logger.exception(
                "❌ [文章/待审查] 关联原帖查询失败 | 原帖ID数: [%s] | 后续: [返回文章及空素材] "
                "| 排查: [检查数据库连接与原帖查询接口]", len(query_ids),
            )
        source_posts = [{"origin_content": (post.get("content") or {}).get("text_content", ""),
                         "logic_mul": post.get("logic_mul")} for post in raw_posts if isinstance(post, dict)]
    logger.info(
        "[文章/待审查] 审查数据已组装 | 候选: [%s] | 随机抽取: [%s] | 关联原帖: [%s]",
        len(candidates), len(data), len(source_posts),
    )
    return data, source_posts


# article{stance,publish_time}、当前秒、prices=None或(发布价,现价) → (删除原因或None,是否更新涨跌幅)。
# : 看多逆向20%、看空逆向50%才删除，行情与时效均沿用原边界，逆向原因优先。
def _article_delete_reason(article, now, prices):
    price_updated = prices is not None
    if price_updated:
        base_price, current_price = prices
        difference = (current_price - base_price) / base_price
        profit = difference if article.get("stance") == "看多" else -difference
        article["profit_pct"] = round(profit, 4)
        threshold = 0.2 if article.get("stance") == "看多" else 0.5
        if profit <= -threshold:
            return f"逆向 {abs(profit) * 100:.2f}% (基准: {threshold * 100:.0f}%)", True
    publish_time = article.get("publish_time")
    if publish_time is None:
        return None, price_updated
    try:
        elapsed = now - _publish_time_seconds(publish_time)
        if elapsed >= MAX_REPLAY_DAYS * 24 * 3600:
            return f"超时 (已发布 {elapsed / (24 * 3600):.2f} 天, 基准: {MAX_REPLAY_DAYS} 天)", price_updated
    except Exception:
        logger.warning(
            "[历史清理/时间] 无法判断帖子是否超时 | 帖子: [%s] | 发布时间: [%s] "
            "| 后续: [保留当前时效状态] | 排查: [核对秒/毫秒时间戳]",
            article.get("binance_post_id"), publish_time, exc_info=True,
        )
    return None, price_updated


# 管理器已发文章/远端历史列表 → 逆向或超时删除及delete_time/delete_reason/check_time回写。
# : 远端兜底完全信任time_offset返回范围，不逐条复核时间；帖子和回复均取reply_id。
def _delete_old_posts_once(article_manager):
    started, now = time.monotonic(), time.time()
    reverse_count = timeout_count = failed = 0
    deleted_ids = set()
    credentials, account_errors = _load_account_credentials("历史清理", with_user_info=True)
    try:
        articles = article_manager.find_articles(query={
            "publish_status": "success", "binance_post_id": {"$exists": True, "$ne": None},
            "delete_time": {"$exists": False},
        }) or []
        logger.info("[历史清理/巡检] 已查询待检测文章 | 文章: [%s] | 可用账号: [%s]", len(articles), len(credentials))
        for article in articles:
            post_id = str(article.get("binance_post_id") or "").strip()
            if not post_id:
                continue
            account, topic, stance = article.get("published_by"), article.get("topic"), article.get("stance")
            prices = None
            try:
                if topic and stance in ("看多", "看空") and article.get("publish_price") is not None:
                    base_price = float(article["publish_price"])
                    if base_price > 0:
                        latest = get_latest_crypto_price(topic)
                        if latest is not None:
                            prices = base_price, float(latest)
            except Exception:
                logger.warning(
                    "[历史清理/行情] 读取或转换价格失败 | 帖子: [%s] | 币种: [%s] "
                    "| 后续: [继续时效检查] | 排查: [检查价格字段、行情接口与网络]",
                    post_id, topic, exc_info=True,
                )
            reason, price_updated = _article_delete_reason(article, now, prices)
            deleted = False
            credential = credentials.get(account)
            if reason and not credential:
                logger.warning(
                    "[历史清理/无法删除] 已触发删除条件，但账号凭证不可用 | 账号: [%s] | 帖子: [%s] "
                    "| 原因: [%s] | 排查: [恢复对应账号登录与会话]", account, post_id, reason,
                )
            if reason and credential:
                stage = "请求远端删除"
                try:
                    success = delete_binance_square_content(
                        content_id=post_id, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                    )
                    if success:
                        deleted = True
                        deleted_ids.add(post_id)
                        article.update(
                            delete_time=time.time(), delete_reason=reason,
                            check_time=datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        stage = "回写已删除状态"
                        article_manager.upsert_articles([article])
                        reverse_count += reason.startswith("逆向")
                        timeout_count += not reason.startswith("逆向")
                        logger.info(
                            "[历史清理/删除] 远端删除并回写成功 | 帖子: [%s] | 账号: [%s] | 原因: [%s]",
                            post_id, account, reason,
                        )
                    else:
                        failed += 1
                        logger.warning(
                            "[历史清理/删除] 接口未确认成功 | 帖子: [%s] | 账号: [%s] "
                            "| 后续: [下轮再检查] | 排查: [检查凭证、限流与远端帖子是否存在]", post_id, account,
                        )
                except Exception:
                    failed += 1
                    logger.exception(
                        "❌ [历史清理/删除] 帖子处理失败 | 阶段: [%s] | 帖子: [%s] | 账号: [%s] "
                        "| 远端已确认删除: [%s] | 后续: [继续其他帖子] "
                        "| 排查: [检查删除接口或数据库，先核对远端结果]", stage, post_id, account, deleted,
                    )
            if not deleted and price_updated:
                try:
                    article["check_time"] = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    article_manager.upsert_articles([article])
                except Exception:
                    logger.warning(
                        "[历史清理/行情回写] 涨跌幅保存失败 | 帖子: [%s] | 后续: [继续其他帖子] "
                        "| 排查: [检查数据库连接与写入权限]", post_id, exc_info=True,
                    )
    except Exception:
        logger.exception(
            "❌ [历史清理/巡检] 数据库扫描或处理异常 | 后续: [继续远端兜底清理] "
            "| 排查: [检查数据库连接、文章结构与删除结果]"
        )
    try:
        cutoff_ms = int((now - MAX_REPLAY_DAYS * 24 * 3600) * 1000)

        for account in ACCOUNTS:
            credential = credentials.get(account)
            if not credential:
                continue
            square_uid = (credential.get("user_info") or {}).get("squareUid")
            if not square_uid:
                continue
            stage = "拉取历史帖子与回复"
            try:
                items = fetch_binance_square_replies(
                    target_square_uid=square_uid, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                    limit=1000, time_offset=cutoff_ms, filter_type="REPLY",
                ) or []
                items.extend(fetch_binance_square_replies(
                    target_square_uid=square_uid, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                    limit=1000, time_offset=cutoff_ms,
                ) or [])
                for item in items:
                    raw_id = item.get("reply_id")
                    if not raw_id or str(raw_id) in deleted_ids:
                        continue
                    post_id = str(raw_id)
                    try:
                        success = delete_binance_square_content(
                            content_id=post_id, cookies=credential["cookies"], csrf_token=credential["csrf_token"],
                        )
                    except Exception:
                        failed += 1
                        logger.exception(
                            "❌ [历史清理/远端删除] 删除请求异常 | 账号: [%s] | 帖子: [%s] "
                            "| 后续: [继续其他记录] | 排查: [检查凭证、网络与限流，核对远端结果]", account, post_id,
                        )
                        continue
                    if not success:
                        failed += 1
                        logger.warning(
                            "[历史清理/远端删除] 接口未确认成功 | 账号: [%s] | 帖子: [%s] "
                            "| 排查: [检查帖子是否存在、登录与接口响应]", account, post_id,
                        )
                        continue
                    timeout_count += 1
                    deleted_ids.add(post_id)
                    stage = "回写远端已删除记录"
                    matched = article_manager.find_articles(query={"binance_post_id": post_id})
                    if not matched:
                        continue
                    deleted_at = time.time()
                    checked_at = datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")
                    for article in matched:
                        article.update(
                            delete_time=deleted_at, delete_reason=f"超时 (基准: {MAX_REPLAY_DAYS} 天)", check_time=checked_at,
                        )
                    article_manager.upsert_articles(matched)
                    logger.info("[历史清理/远端回写] 已保存删除状态 | 帖子: [%s] | 记录: [%s]", post_id, len(matched))
            except Exception:
                account_errors += 1
                logger.exception(
                    "❌ [历史清理/远端账号] 当前账号处理失败 | 账号: [%s] | 阶段: [%s] "
                    "| 后续: [继续其他账号] | 排查: [检查历史接口、用户信息及数据库；核对已完成的删除]", account, stage,
                )
    except Exception:
        logger.exception(
            "❌ [历史清理/兜底] 远端清理本轮中断 | 后续: [下轮重试] "
            "| 排查: [检查账号用户信息、历史列表与数据库]"
        )
    logger.info(
        "[历史清理/汇总] 本轮结束 | 逆向删除: [%s] | 超时删除: [%s] | 失败: [%s] "
        "| 账号异常: [%s] | 耗时: [%.2f 秒] | 下次检查: [%s 秒后]",
        reverse_count, timeout_count, failed, account_errors, time.monotonic() - started, DELETE_INTERVAL_SEC,
    )


# 独立文章管理器 → 每小时执行数据库巡检和远端兜底；通过 _poll_task 接入异常守护轮询。
def delete_old_post():
    _poll_task(_delete_old_posts_once, GeneratedArticleManager(gen_db_object()), DELETE_INTERVAL_SEC, "历史清理")


# 关键字、小时数、最低浏览量 → [{text_content,images,engagement{view_count,like_count,comment_count}}]。
# : 数据库分支用正则、兜底分支用字面子串；查询时间仍按秒比较，未自动兼容毫秒存储。
def query_hot_posts_by_keyword(target_key, hours=24, min_views=1000):
    manager = UniversalPostManager(gen_db_object())
    start_time = time.time() - hours * 3600
    query = {
        "source": BINANCE_SOURCE, "publish_time": {"$gte": start_time},
        "engagement.view_count": {"$gte": min_views},
        "content.text_content": {"$regex": target_key, "$options": "i"},
    }
    sort = [("engagement.view_count", -1)]
    try:
        if hasattr(manager, "collection"):
            posts = list(manager.collection.find(query).sort(sort))
        elif hasattr(manager, "find_posts"):
            posts = manager.find_posts(query=query, sort=sort)
        else:
            raw_posts = manager.find_posts_by_source(BINANCE_SOURCE, limit=5000)
            posts = [post for post in raw_posts
                     if post.get("publish_time", 0) >= start_time
                     and post.get("engagement", {}).get("view_count", 0) >= min_views
                     and target_key.lower() in (post.get("content", {}).get("text_content", "") or "").lower()]
            posts.sort(key=lambda post: post.get("engagement", {}).get("view_count", 0), reverse=True)
    except Exception:
        logger.exception(
            "❌ [数据/查询] 热门帖查询失败 | 关键字: [%s] | 时长: [%s 小时] | 最低浏览量: [%s] "
            "| 后续: [沿用空列表返回] | 排查: [检查正则表达式、数据库连接与帖子字段]",
            target_key, hours, min_views,
        )
        return []
    results = []
    for post in posts:
        engagement = post.get("engagement") or {}
        results.append({
            "text_content": (post.get("content") or {}).get("text_content", ""),
            "images": (post.get("media") or {}).get("images", []),
            "engagement": {key: engagement.get(key, 0) for key in ("view_count", "like_count", "comment_count")},
        })
    logger.info(
        "[数据/查询] 热门帖查询完成 | 关键字: [%s] | 时长: [%s 小时] | 匹配: [%s]",
        target_key, hours, len(results),
    )
    return results


# 无参任务 → 当前线程执行；未被业务边界处理的异常记录上下文后重抛，不自动重启线程。
def _run_task(task):
    try:
        task()
    except Exception:
        logger.exception(
            "❌ [任务/退出] 后台任务异常结束 | 任务: [%s] | 结果: [当前线程停止] "
            "| 排查: [检查对应链路的数据、文件权限与外部服务]", task.__name__,
        )
        raise


def get_need_check_pre_screen_info(
    limit=100,
    no_evidence_filter=None,
    only_current_version=True,
    random_sample=True,
    save_path=None,
):
    """
    查询已有初筛结果及模型原始输入数据（post_id、清洗后正文、图片占位符列表），用于人工复盘漏判与误判。

    :param limit: 返回的最大样本数（设为 0 或 None 则返回全部匹配记录）
    :param no_evidence_filter:
        None: 返回全部已初筛帖子
        True: 仅返回被判定为【大概率无论据（已淘汰）】的帖子（用于排查“误杀/漏提”）
        False: 仅返回被判定为【可能有论据（已保留）】的帖子（用于排查“放水/多花钱”）
    :param only_current_version: 是否仅筛选当前 prompt_version 的初筛记录
    :param random_sample: 是否随机打乱抽样（避免集中看到同一时段或单一币种的帖子）
    :param save_path: 可选文件路径（如 "pre_screen_review.json"），传入后自动保存为 JSON 文件
    :return: 包含原始输入与初筛结果的字典列表
    """
    post_manager = UniversalPostManager(gen_db_object())

    # 1. 组装 MongoDB 查询条件
    query = {
        "source": BINANCE_SOURCE,
        "pre_screen": {"$exists": True, "$ne": None},
    }
    if only_current_version:
        query["pre_screen.prompt_version"] = prompt_version
    if isinstance(no_evidence_filter, bool):
        query["pre_screen.no_evidence"] = no_evidence_filter

    # 2. 查询数据库（兼容 collection 直查、find_posts 方法与内存过滤兜底）
    raw_posts = []
    try:
        if hasattr(post_manager, "collection"):
            raw_posts = list(post_manager.collection.find(query))
        elif hasattr(post_manager, "find_posts"):
            raw_posts = post_manager.find_posts(query=query) or []
        else:
            all_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT) or []
            for p in all_posts:
                ps = p.get("pre_screen")
                if not isinstance(ps, dict) or not isinstance(ps.get("no_evidence"), bool):
                    continue
                if only_current_version and ps.get("prompt_version") != prompt_version:
                    continue
                if isinstance(no_evidence_filter, bool) and ps.get("no_evidence") != no_evidence_filter:
                    continue
                raw_posts.append(p)
    except Exception as exc:
        logger.error(f"❌ [初筛/审查] 查询已初筛帖子失败 | 异常: {exc}")
        return []

    # 3. 统计整体淘汰与保留分布
    total_matched = len(raw_posts)
    total_no_ev = sum(1 for p in raw_posts if (p.get("pre_screen") or {}).get("no_evidence") is True)
    total_has_ev = total_matched - total_no_ev

    # 4. 抽样与截取
    if random_sample:
        random.shuffle(raw_posts)
    selected_posts = raw_posts[:limit] if (limit and limit > 0) else raw_posts

    # 5. 组装审查数据结构（还原初筛输入 + 拼接初筛结论）
    review_list = []
    for post in selected_posts:
        if not isinstance(post, dict):
            continue
        base_item = build_pre_screen_item(post)
        pre_screen = post.get("pre_screen") or {}

        review_item = {
            "post_id": base_item["post_id"],
            "text_content": base_item["text_content"],
            "images": base_item["images"],
            "no_evidence": pre_screen.get("no_evidence"),
            "reason": pre_screen.get("reason", ""),
        }
        review_list.append(review_item)

    if save_path:
        save_json(save_path, review_list)

    logger.info(
        "[初筛/审查] 已导出初筛复盘数据 | 匹配总数: [%s (淘汰: %s / 保留: %s)] "
        "| 本次抽取: [%s] | 过滤条件(no_evidence): [%s] | 导出文件: [%s]",
        total_matched, total_no_ev, total_has_ev,
        len(review_list), no_evidence_filter, save_path or "未落盘",
    )
    return review_list


def get_need_check_viral_article_info(
        limit=40,
        hours=6,
        only_current_version=True,
        random_sample=True,
        save_path=None,
):
    """
    获取高流量重塑文章及其来源原帖的对照审查数据（严格对齐最新两阶段 Schema）。
    """
    article_manager = GeneratedArticleManager(gen_db_object())

    query = {
        "type": ARTICLE_TYPE_VIRAL,
        # "status": "ok",
        "publish_status": {"$ne": "success"},
    }
    if hours and hours > 0:
        query["created_at"] = {"$gte": datetime.now(timezone.utc) - timedelta(hours=hours)}
    if only_current_version:
        query["prompt_version"] = prompt_version

    candidates = article_manager.find_articles(query=query, sort=[("created_at", -1)]) or []
    if not candidates:
        logger.info("[高流量/审查] 未找到待审查文章 | 筛选时间: [近 %s 小时]", hours)
        return []

    if random_sample:
        random.shuffle(candidates)
    selected_articles = candidates[:limit] if (limit and limit > 0) else candidates

    post_ids = set()
    for art in selected_articles:
        p_list = art.get("post_id_list") or []
        for pid in p_list:
            if pid is not None and str(pid).strip():
                post_ids.add(str(pid).strip())
                if str(pid).strip().isdigit():
                    post_ids.add(int(pid))

    post_map = {}
    if post_ids:
        post_manager = UniversalPostManager(gen_db_object())
        query_ids = list(post_ids)
        p_query = {"post_id": {"$in": query_ids}}
        raw_posts = []
        try:
            if hasattr(post_manager, "collection"):
                raw_posts = list(post_manager.collection.find(p_query))
            elif hasattr(post_manager, "find_posts"):
                raw_posts = post_manager.find_posts(query=p_query) or []
            else:
                all_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT) or []
                raw_posts = [p for p in all_posts if
                             p.get("post_id") in query_ids or str(p.get("post_id")) in query_ids]
        except Exception:
            logger.exception("❌ [高流量/审查] 关联原帖查询异常")

        for p in raw_posts:
            if isinstance(p, dict) and p.get("post_id") is not None:
                post_map[str(p["post_id"]).strip()] = p

    review_list = []
    for article in selected_articles:
        p_list = article.get("post_id_list") or []
        target_post_id = str(p_list[0]).strip() if p_list else ""

        raw_p = post_map.get(target_post_id, {})
        info = article.get("article_info") or {}
        v_screen = raw_p.get("viral_screen") or {}
        engagement = raw_p.get("engagement") or {}

        clean_text, _, mapping = normalize_post_media(raw_p) if raw_p else ("", [], {})
        image_placeholders = list(mapping.keys())

        source_post_data = {
            "post_id": str(raw_p.get("post_id") or target_post_id).strip(),
            "text": clean_text.strip(),
            "images": image_placeholders,
            "engagement": {
                "view_count": engagement.get("view_count", 0),
                "like_count": engagement.get("like_count", 0),
                "comment_count": engagement.get("comment_count", 0),
            },
            "viral_screen": {
                "keep": v_screen.get("keep"),
                "event_key": v_screen.get("event_key", ""),
            },
        }

        remodeled_article_data = {
            "usable": info.get("usable", False),
            "decision_reason": info.get("decision_reason", ""),
            "topic_tag": info.get("topic_tag", ""),
            "selected_images": info.get("selected_images", []),
            "image_role": info.get("image_role", ""),
            "viral_analysis": info.get("viral_analysis", ""),
            "post_text": info.get("post_text", ""),
            "bait_comments": info.get("bait_comments", []),
            "viral_score": info.get("viral_score", 0.0),
        }

        review_list.append({
            "source_post": source_post_data,
            "remodeled_article": remodeled_article_data,
        })

    if save_path:
        save_json(save_path, review_list)

    logger.info(
        "[高流量/审查] 精简复盘数据组装完成 | 候选: [%s] | 抽样返回: [%s] | 导出文件: [%s]",
        len(candidates), len(review_list), save_path or "未落盘",
    )
    return review_list
# article → (全部URL/路径集合, 统计用主地址列表)；同时兼容分析帖(image_placeholders)与流量帖(selected_images)。
def _extract_article_image_addresses(article):
    info = article.get("article_info")
    if not isinstance(info, dict):
        return set(), []
    mapping = info.get("image_mapping")
    if not isinstance(mapping, dict) or not mapping:
        return set(), []
    placeholders = info.get("selected_images") if article.get("type") == ARTICLE_TYPE_VIRAL else info.get("image_placeholders")
    selected = ([mapping[key] for key in placeholders if key in mapping]
                if isinstance(placeholders, list) and placeholders else mapping.values())
    addresses, primary = set(), []
    for item in selected:
        if not isinstance(item, dict):
            continue
        url, path = (item.get("original_url") or "").strip(), (item.get("local_path") or "").strip()
        addresses.update(value for value in (url, path) if value)
        if url or path:
            primary.append(url or path)
    return addresses, primary


# 线程安全读取并初始化账号状态文件；确保分析帖与流量帖的冷却字段共存且互不干扰。
def _load_and_init_publish_state():
    with STATE_FILE_LOCK:
        state = (read_json(STATE_FILE) or {}) if os.path.exists(STATE_FILE) else {}
        if not isinstance(state, dict):
            raise ValueError("账号状态文件必须为 {账号: 状态字典}")
        for account in ACCOUNTS:
            account_state = state.setdefault(account, {})
            if not isinstance(account_state, dict):
                raise ValueError(f"账号状态必须是字典: {account}")
            for key, value in {
                "total_success": 0, "last_publish_time": 0, "last_error_msg": "",
                "last_error_time": 0, "topic_publish_history": {},
                "viral_total_success": 0, "viral_last_publish_time": 0,
                "viral_last_error_msg": "", "viral_last_error_time": 0,
            }.items():
                account_state.setdefault(key, value)
            if not isinstance(account_state["topic_publish_history"], dict):
                raise ValueError(f"账号主题历史必须为 {{主题: 时间戳}}: {account}")
        return state


# 线程安全局部更新指定账号的状态字段并落盘，防止分析帖与流量帖并发发布时互相覆盖。
def _update_account_publish_state(account, field_updates, topic_update=None):
    with STATE_FILE_LOCK:
        state = (read_json(STATE_FILE) or {}) if os.path.exists(STATE_FILE) else {}
        if not isinstance(state, dict):
            state = {}
        account_state = state.setdefault(account, {})
        for key, value in {
            "total_success": 0, "last_publish_time": 0, "last_error_msg": "",
            "last_error_time": 0, "topic_publish_history": {},
            "viral_total_success": 0, "viral_last_publish_time": 0,
            "viral_last_error_msg": "", "viral_last_error_time": 0,
        }.items():
            account_state.setdefault(key, value)
        account_state.update(field_updates)
        if topic_update:
            topic_key, topic_time = topic_update
            if not isinstance(account_state.get("topic_publish_history"), dict):
                account_state["topic_publish_history"] = {}
            account_state["topic_publish_history"][topic_key] = topic_time
        save_json(STATE_FILE, state)


# 近24小时 ARTICLE_TYPE_VIRAL 文章 → 按 viral_score 从高到低排序 → 独立1h账号冷却发布 → 回写账号与数据库状态。
def _publish_viral_articles_once(article_manager):
    now = time.time()
    state = _load_and_init_publish_state()

    # 查询近24小时已成功发布的帖子，用于图片去重
    recent_success = article_manager.find_articles(query={
        "status": "ok",
        "publish_status": "success",
        "publish_time": {"$gte": now - VIRAL_PUBLISH_MAX_AGE_HOURS * 3600},
    }) or []
    used_addresses, used_primary = set(), set()
    for article in recent_success:
        addresses, primary = _extract_article_image_addresses(article)
        used_addresses.update(addresses)
        used_primary.update(primary)

    # 仅查询 24 小时内生成的 ARTICLE_TYPE_VIRAL 待发布帖子
    raw_candidates = article_manager.find_articles(query={
        "type": ARTICLE_TYPE_VIRAL,
        "created_at": {"$gte": datetime.now(timezone.utc) - timedelta(hours=VIRAL_PUBLISH_MAX_AGE_HOURS)},
        "status": "ok",
        "publish_status": {"$ne": "success"},
    }) or []

    stats = Counter()
    candidates = []
    for article in raw_candidates:
        info = article.get("article_info")
        if article.get("prompt_version") != prompt_version:
            stats["version"] += 1
            continue
        if (not isinstance(info, dict)
                or info.get("status") != "ok"
                or info.get("usable") is not True
                or not isinstance(info.get("post_text"), str)
                or not info.get("post_text").strip()
                or not isinstance(info.get("selected_images", []), list)
                or isinstance(info.get("viral_score"), bool)
                or not isinstance(info.get("viral_score"), (int, float))):
            stats["structure"] += 1
            continue
        if article.get("publish_attempts", 0) >= PUBLISH_MAX_ATTEMPTS:
            stats["attempts"] += 1
            continue

        # 校验选中的图片映射及本地文件完整性
        mapping = info.get("image_mapping") or {}
        selected_images = info.get("selected_images") or []
        if any(not isinstance(mapping.get(img_id), dict)
               or not _is_valid_image_file(mapping[img_id].get("local_path"))
               for img_id in selected_images):
            stats["invalid_image"] += 1
            continue

        addresses, _ = _extract_article_image_addresses(article)
        if addresses and (addresses & used_addresses):
            stats["image_dup"] += 1
            continue
        candidates.append((article, addresses))

    # 按照 viral_score 从高到低排序
    candidates.sort(key=lambda item: float(item[0]["article_info"].get("viral_score", 0.0)), reverse=True)
    logger.info(
        "[流量发布/计划] 近%s小时流量帖已按评分从高到低筛选 | 查询: [%s] | 候选: [%s] "
        "| 过滤: [版本%s/结构%s/尝试超限%s/图片缺失%s/图片重复%s] | 尝试上限: [%s] "
        "| 近%s小时已发: [%s] | 占用图片: [%s] | 账号: [%s] | 独立账号冷却: [%s秒]",
        VIRAL_PUBLISH_MAX_AGE_HOURS, len(raw_candidates), len(candidates),
        stats["version"], stats["structure"], stats["attempts"], stats["invalid_image"],
        stats["image_dup"], PUBLISH_MAX_ATTEMPTS, VIRAL_PUBLISH_MAX_AGE_HOURS,
        len(recent_success), len(used_primary), len(ACCOUNTS), ACCOUNT_COOLDOWN_SECONDS,
    )

    for account in ACCOUNTS:
        if not candidates:
            break
        account_state = state[account]
        # 单独按照流量帖计算 1h 冷却，不与其他类型帖子或主题冷却关联
        remaining = ACCOUNT_COOLDOWN_SECONDS - (now - account_state.get("viral_last_publish_time", 0))
        if remaining > 0:
            logger.debug("[流量发布/账号] 仍在独立冷却中 | 账号: [%s] | 剩余: [%s 秒]", account, int(remaining))
            continue

        selected_index = None
        for index, (article, addresses) in enumerate(candidates):
            if addresses and (addresses & used_addresses):
                continue
            selected_index = index
            break
        if selected_index is None:
            logger.debug("[流量发布/账号] 所有候选图片均与已发布冲突 | 账号: [%s]", account)
            continue
        logger.info("[流量发布/账号] 账号可用 | 账号: [%s] | 候选文章: [%s] | 评分: [%s]",
                    account, candidates[selected_index][0].get("_id"), candidates[selected_index][0]["article_info"].get("viral_score"))
        article, addresses = candidates[selected_index]
        info = article["article_info"]
        topic = article.get("topic") or info.get("topic_tag") or ""
        score = info.get("viral_score")
        started = time.monotonic()
        stage, api_result = "准备正文、媒体及配置", "尚未调用"
        try:
            text = info.get("post_text", "").strip()
            text_list = _build_publish_text_list(text)
            mapping = info.get("image_mapping") or {}
            selected_keys = info.get("selected_images") or []
            image_paths = [
                mapping[key].get("local_path")
                for key in selected_keys
                if isinstance(mapping.get(key), dict) and mapping[key].get("local_path")
            ]
            session_dir = get_config(f"{account}_browser_session_dir")
            attempts = article.get("publish_attempts", 0) + 1
            stage, api_result = "调用发布接口", "未知"
            err, success, post_id = create_binance_post(
                content=text,
                image_path_list=image_paths,
                user_data_dir=session_dir,
                chart_info=None,
                sentiment=None,
                debug=True,
                text_list=text_list,
            )
            post_id, api_result = str(post_id), "成功" if success else "失败"
            problems, saved = [], []
            if success:
                new_total = account_state.get("viral_total_success", 0) + 1
                account_state["viral_total_success"] = new_total
                account_state["viral_last_publish_time"] = now
                state_updates = {
                    "viral_total_success": new_total,
                    "viral_last_publish_time": now,
                }
                changes = {
                    "publish_status": "success",
                    "published_by": account,
                    "publish_time": now,
                    "binance_post_id": post_id,
                    "publish_attempts": attempts,
                    "publish_price": None,
                }
                used_addresses.update(addresses)
            else:
                error = f"发布接口未确认成功；可能是会话失效、网络异常、限流或内容校验失败；详情: {err}"
                account_state["viral_last_error_msg"] = error
                account_state["viral_last_error_time"] = now
                state_updates = {
                    "viral_last_error_msg": error,
                    "viral_last_error_time": now,
                }
                changes = {
                    "publish_status": "failed",
                    "last_error": error,
                    "publish_attempts": attempts,
                    "publish_price": None,
                }

            stage = "保存已确认的发布结果"
            try:
                _update_account_publish_state(account, state_updates)
                saved.append("账号状态")
            except Exception as exc:
                problems.append(("写入账号状态", exc))
            article.update(changes)
            try:
                article_manager.upsert_articles([article])
                saved.append("数据库状态")
            except Exception as exc:
                problems.append(("回写数据库", exc))
            if problems:
                details = "; ".join(f"{label}: {type(exc).__name__}: {exc}" for label, exc in problems)
                raise RuntimeError(f"{details} | 已保存: [{', '.join(saved) or '无'}]") from problems[0][1]
        except Exception as exc:
            raise RuntimeError(
                f"流量发布阶段[{stage}]失败 | 账号: [{account}] | 文章: [{article.get('_id')}] "
                f"| 赛道: [{topic}] | 接口结果: [{api_result}]；请核对远端结果与本地状态 | 原因: [{exc}]"
            ) from exc

        if success:
            candidates.pop(selected_index)
        log = logger.info if success else logger.error
        log(
            "%s[流量发布/文章] 本次处理结束 | 账号: [%s] | 赛道: [%s] | 文章: [%s] "
            "| 评分: [%s] | 尝试: [%s/%s] | 耗时: [%.2f 秒] | 结果: [%s] | 说明: [%s]",
            "" if success else "❌ ", account, topic, article.get("_id"), score,
            attempts, PUBLISH_MAX_ATTEMPTS, time.monotonic() - started, api_result,
            "远端发布与本地回写完成" if success else error,
        )


# 独立文章管理器 → 流量帖发布轮询；每轮结束后等待600秒。
def auto_publish_viral_articles():
    _poll_task(
        _publish_viral_articles_once,
        GeneratedArticleManager(gen_db_object()),
        PUBLISH_POLL_SECONDS,
        "流量发布",
    )

if __name__ == "__main__":
    # query_hot_posts_by_keyword(target_key="gtc")
    # get_need_check_article_info()
    # get_need_check_viral_article_info()
    # data = get_need_check_pre_screen_info(limit=1000)

    tasks = [
        # generate_analysis_articles,
        generate_viral_articles,
        format_image_article,
        hudong,
        # auto_publish_articles,
        auto_publish_viral_articles,
        # delete_old_post
    ]
    threads = [threading.Thread(target=_run_task, args=(task,), name=task.__name__) for task in tasks]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()