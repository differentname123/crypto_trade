# -- coding: utf-8 --
# ==========================================
# [功能摘要]: 批量处理币安广场帖子，调用大模型(Gemini)自动生成"带单推广"营销评论，并自动发布回帖。
# [输入数据]: MongoDB 中来源为 "biance" 的原始帖子 (dict)，核心结构含 author / content / comments / engagement / metadata。
# [数据流转/交互]:
#   1. [采集] fetch_binance_feed 按币种拉取推荐流 -> upsert 落库 MongoDB。
#   2. [生成] DB 拉存量帖 -> 三级过滤(黑名单/时效/水位线) -> 清洗正文+Top5热评 -> 拼接Prompt请求Gemini
#             -> 严格校验双视角JSON -> 回写 promo_comment 字段。
#   3. [发布] DB 拉带评论帖 -> 组装带引流链接的评论 -> comment_on_binance_post 发帖 -> 回写 promo_comment_info。
# [输出数据]: 副作用为主 —— 向 MongoDB 帖子文档追加 promo_comment(生成结果) 与 promo_comment_info(发布状态)。
# ==========================================
import datetime
import json
import os
import re
import time
import threading
from common.common_utils import read_file_to_str, string_to_object, setup_logger

logger = setup_logger(app_name="promo_copy")
from app.ai_api.gemini_playwright import generate_gemini_content_playwright
from biance.biance_playwright import comment_on_binance_post, get_auth_tokens_robust
from biance.biance_squre_api import fetch_binance_feed, fetch_binance_square_replies, delete_binance_square_content, \
    fetch_binance_replies

from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager

# ---------------- 全局配置常量（集中管理硬编码，便于维护）----------------
MAX_REPLAY_DAYS = 7  # 仅拉保留最近7天的评论，其它的会被删除
BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
SCHEDULE_INTERVAL_SEC = 3600
COMMENT_SEND_INTERVAL_SEC = 60 * 1  # 同一账号两次发送评论的最小间隔（秒），实际发送间隔必须大于该值
LLM_MAX_RETRIES = 3

GEMINI_MODEL = "gemini-3-flash-thinking"

FEED_TOKENS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE"]
PROMPT_FILE_PATH = r'W:\project\python_project\crypto_trade\prompt\带单推广评论生成.txt'
USER_DATA_DIR_LIST = [
    r"W:\temp\biance_qiqi"
]

DELETE_USER_DATA_DIR_LIST = [
    r"W:\temp\biance_nana",
    r"W:\temp\biance_yang",
    r"W:\temp\biance_daniang",
    r"W:\temp\biance_mama",
    r"W:\temp\biance_jie",
    r"W:\temp\biance_qiqi",

    r"W:\temp\biance_zhouling",
    r"W:\temp\biance_ruru"

]
USER_ACCOUNT_USAGE_FILE = r"W:\project\python_project\crypto_trade\biance\promotion_copy_trade\biance_account_usage.json"
LEAD_DETAIL_URL = "https://www.binance.com/zh-CN/square/post/362858558969979"

FILTER_CONFIG = {
    # 1. 黑名单机制
    "blacklist_keywords": [
        '瓜分', '抽奖', '红包', '空投', '新粉福利', '转发', '留下你的',
        'giveaway', 'prize pool', 'airdrop', 'split'
    ],
    "blacklist_multi_words": ['follow', 'share', 'comment'],  # 三词同时出现才触发拦截

    # 2. 结构与时效底线
    "min_text_length": 20,  # 去链接/标签后的纯文本最短字符数
    "max_age_hours": 720,  # 帖子最长有效时间(小时)

    # 3. 绝对值水位线防线
    "max_comment_count": 100,  # 评论数上限(过高说明太拥挤,推广无曝光)
    "cold_post_hours": 20,  # 判定"死帖"的时间界限(小时)
    "cold_post_min_views": 20  # 死帖最低浏览量要求(超时且低于此值即淘汰)
}

# 账号使用信息仅在当前进程内并发访问，使用线程锁保证“选择账号 + 占用次数”原子化
_user_account_usage_lock = threading.Lock()


def _get_account_name(user_data_dir):
    """从用户数据目录路径中提取账号名称，例如 W:\\temp\\biance_ruru -> biance_ruru。"""
    return os.path.basename(os.path.normpath(user_data_dir))


def _get_account_usage_update_time():
    """返回账号使用信息的更新时间。"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _save_user_account_usage_unlocked(usage_data):
    """原子写入账号使用信息 JSON。调用方必须已持有 _user_account_usage_lock。"""
    usage_dir = os.path.dirname(USER_ACCOUNT_USAGE_FILE)
    if usage_dir:
        os.makedirs(usage_dir, exist_ok=True)

    temp_file = f"{USER_ACCOUNT_USAGE_FILE}.tmp"
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(usage_data, f, ensure_ascii=False, indent=2)
    os.replace(temp_file, USER_ACCOUNT_USAGE_FILE)


def _load_user_account_usage_unlocked():
    """读取并补齐账号使用信息。调用方必须已持有 _user_account_usage_lock。"""
    if not USER_DATA_DIR_LIST:
        raise ValueError("USER_DATA_DIR_LIST 不能为空")

    if os.path.exists(USER_ACCOUNT_USAGE_FILE):
        try:
            with open(USER_ACCOUNT_USAGE_FILE, "r", encoding="utf-8") as f:
                usage_data = json.load(f)
        except Exception as e:
            raise RuntimeError(f"读取账号使用信息失败: {USER_ACCOUNT_USAGE_FILE} | {e}") from e

        if not isinstance(usage_data, dict):
            raise ValueError(f"账号使用信息文件顶层必须是 dict: {USER_ACCOUNT_USAGE_FILE}")
    else:
        usage_data = {}

    changed = False
    for user_data_dir in USER_DATA_DIR_LIST:
        account_name = _get_account_name(user_data_dir)
        if not account_name:
            raise ValueError(f"无法从 USER_DATA_DIR_LIST 中提取账号名称: {user_data_dir}")

        account_usage = usage_data.get(account_name)
        if not isinstance(account_usage, dict):
            account_usage = {}
            usage_data[account_name] = account_usage
            changed = True

        default_fields = {
            "total_count": 0,
            "success_count": 0,
            "failure_count": 0,
            "last_failure_reason": None,
            "last_send_time": 0,
            "update_time": None
        }
        for field, default_value in default_fields.items():
            if field not in account_usage:
                account_usage[field] = default_value
                changed = True

    if changed:
        _save_user_account_usage_unlocked(usage_data)

    return usage_data


def acquire_user_account_for_send():
    """
    优先从已满足发送间隔的账号中选择 total_count 最少的账号，并在返回前先占用一次 total_count。
    total_count 相同时按 USER_DATA_DIR_LIST 中的顺序选择。
    若所有账号均未满足发送间隔，则等待最短剩余时间后重新选择。
    [出参 Shape]: (user_data_dir(str), account_name(str))。
    """
    while True:
        wait_seconds = 0

        with _user_account_usage_lock:
            usage_data = _load_user_account_usage_unlocked()
            now = time.time()
            available_accounts = []
            min_remaining_seconds = None

            for index, user_data_dir in enumerate(USER_DATA_DIR_LIST):
                account_name = _get_account_name(user_data_dir)
                account_usage = usage_data[account_name]
                total_count = int(account_usage.get("total_count", 0) or 0)
                last_send_time = float(account_usage.get("last_send_time", 0) or 0)
                elapsed_seconds = now - last_send_time

                if last_send_time <= 0 or elapsed_seconds > COMMENT_SEND_INTERVAL_SEC:
                    available_accounts.append((index, user_data_dir, account_name, total_count))
                else:
                    remaining_seconds = COMMENT_SEND_INTERVAL_SEC - elapsed_seconds
                    if min_remaining_seconds is None or remaining_seconds < min_remaining_seconds:
                        min_remaining_seconds = remaining_seconds

            if available_accounts:
                _, selected_dir, selected_name, _ = min(
                    available_accounts,
                    key=lambda item: (item[3], item[0])
                )

                account_usage = usage_data[selected_name]
                account_usage["total_count"] = int(account_usage.get("total_count", 0) or 0) + 1
                account_usage["last_send_time"] = time.time()
                account_usage["update_time"] = _get_account_usage_update_time()
                _save_user_account_usage_unlocked(usage_data)

                return selected_dir, selected_name

            wait_seconds = max(min_remaining_seconds or COMMENT_SEND_INTERVAL_SEC, 0) + 0.05

        logger.info(
            f"[发布链路/账号冷却] 所有账号均未满足发送间隔 "
            f"| 关键参数: 【同账号最小间隔: {COMMENT_SEND_INTERVAL_SEC} 秒】 "
            f"| 结果: 【等待 {wait_seconds:.2f} 秒后重新选择账号】"
        )
        time.sleep(wait_seconds)


def record_user_account_send_result(account_name, success, error_info=None):
    """记录账号本次实际发送调用的成功/失败结果。"""
    with _user_account_usage_lock:
        usage_data = _load_user_account_usage_unlocked()
        if account_name not in usage_data:
            usage_data[account_name] = {
                "total_count": 0,
                "success_count": 0,
                "failure_count": 0,
                "last_failure_reason": None,
                "last_send_time": 0,
                "update_time": None
            }

        account_usage = usage_data[account_name]
        if success:
            account_usage["success_count"] = int(account_usage.get("success_count", 0) or 0) + 1
        else:
            account_usage["failure_count"] = int(account_usage.get("failure_count", 0) or 0) + 1
            account_usage["last_failure_reason"] = str(error_info or "未知失败原因")

        account_usage["update_time"] = _get_account_usage_update_time()
        _save_user_account_usage_unlocked(usage_data)


def is_valid_post_for_promo(post):
    """
    判断帖子是否值得推广（黑名单 -> 时效/结构 -> 水位线 三级卫语句过滤）。
    [入参 Shape]: post(dict) 需含 content.text_content / engagement.comment_count,view_count / metadata / publish_time。
    """
    content = post.get("content", {})
    text_content = (content.get("text_content") or "").lower()
    engagement = post.get("engagement", {})
    metadata = post.get("metadata", {})

    # --- 第一步：黑名单秒杀 ---
    if any(kw in text_content for kw in FILTER_CONFIG["blacklist_keywords"]):
        return False
    if all(word in text_content for word in FILTER_CONFIG["blacklist_multi_words"]):
        return False

    # --- 第二步：结构与时效底线（剔除链接与#标签避免污染长度判断）---
    clean_text = re.sub(r'#\S+', '', re.sub(r'http[s]?://\S+', '', text_content)).strip()
    if len(clean_text) < FILTER_CONFIG["min_text_length"]:
        return False
    if metadata.get("is_ai_generated") is True:
        return False

    # 兼容 10 位(秒) 与 13 位(毫秒) 时间戳，统一换算为距今小时数
    publish_time = post.get("publish_time", 0)
    if publish_time > 1e11:
        publish_time = publish_time / 1000
    age_hours = (time.time() - publish_time) / 3600
    if age_hours > FILTER_CONFIG["max_age_hours"]:
        return False

    # --- 第三步：绝对值水位线过滤 ---
    if engagement.get("comment_count", 0) > FILTER_CONFIG["max_comment_count"]:
        return False
    if age_hours > FILTER_CONFIG["cold_post_hours"] and engagement.get("view_count", 0) < FILTER_CONFIG[
        "cold_post_min_views"]:
        return False

    return True


def format_post_for_promo(raw_data):
    """
    清洗原始帖子，提炼大模型推广所需的最小上下文。
    [入参 Shape]: raw_data(dict) 含 author.author_name / content.(text_content, mentioned_coins) / comments[].(content, likes, replies, views)。
    [出参 Shape]: {"post": {"author", "text", "coins"}, "top_comments": [str, ...]}。
    """
    author_name = raw_data.get("author", {}).get("author_name", "未知用户")
    content_info = raw_data.get("content", {})
    mentioned_coins = content_info.get("mentioned_coins", [])

    # 剔除多媒体标记并压缩多余空行
    clean_text = re.sub(r'\[(?:长文封面|插图|视频封面|视频):.*?\]', '', content_info.get("text_content", ""))
    clean_text = re.sub(r'\n{3,}', '\n\n', clean_text).strip()

    # 综合 点赞>回复>浏览 热度降序，取 Top5 非空评论
    sorted_comments = sorted(
        raw_data.get("comments", []),
        key=lambda c: (c.get("likes", 0), c.get("replies", 0), c.get("views", 0)),
        reverse=True
    )
    top_comments = [c.get("content", "").strip() for c in sorted_comments[:5] if c.get("content", "").strip()]

    return {
        "post": {"author": author_name, "text": clean_text, "coins": mentioned_coins},
        "top_comments": top_comments
    }


def check_comment_info(data):
    """
    校验大模型返回结构是否达标（含断句/长度/评分等业务潜规则）。
    [入参 Shape]: data(dict) 必含 trader_perspective 与 follower_perspective，
                 每个视角内含 comment_text / link_text / combined_preview / score / score_reason。
    [出参 Shape]: (is_valid(bool), error_message(str))。
    """
    if not isinstance(data, dict):
        return False, "大模型返回数据不是有效的字典对象"

    required_fields = ["comment_text", "link_text", "combined_preview", "score", "score_reason"]
    forbidden_endings = ("。", "！", "？", ".", "!", "?")

    for perspective in ("trader_perspective", "follower_perspective"):
        if perspective not in data:
            return False, f"缺失顶层角色字段: {perspective}"

        view_data = data[perspective]
        if not isinstance(view_data, dict):
            return False, f"{perspective} 必须是字典结构"

        missing = [f for f in required_fields if f not in view_data]
        if missing:
            return False, f"{perspective} 缺失必要字段: {missing}"

        comment_text, link_text, score = view_data["comment_text"], view_data["link_text"], view_data["score"]

        # 业务潜规则：评论文案不得以终止性标点收尾
        if not isinstance(comment_text, str):
            return False, f"{perspective}.comment_text 类型非字符串"
        if comment_text.endswith(forbidden_endings):
            return False, f"{perspective}.comment_text 违规：绝对禁止以终止性标点结尾"

        # 业务潜规则：引流文案长度严格 2-6 个汉字
        if not isinstance(link_text, str):
            return False, f"{perspective}.link_text 类型非字符串"
        if not (2 <= len(link_text) <= 6):
            return False, f"{perspective}.link_text 违规：引流文案长度必须严格在 2-6 个汉字之间"

        if not isinstance(score, (int, float)):
            return False, f"{perspective}.score 类型非数字"
        if not (0 <= score <= 10):
            return False, f"{perspective}.score 违规：评分必须介于 0-10 之间"

    return True, ""


def gen_promo_comment(post):
    """
    为单帖调度大模型生成推广评论，含指数退避重试与有限降级兜底。
    [入参 Shape]: post(dict) 原始帖子。
    [出参 Shape]: 合法评论结构(dict)；重试耗尽仍失败则按原设计降级返回空字典 {}。
    """
    cleaned_post = format_post_for_promo(post)
    prompt = read_file_to_str(PROMPT_FILE_PATH)
    full_prompt = f'{prompt}\n{cleaned_post}'

    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            # err, raw_response, _images = generate_gemini_content_managed(
            #     prompt=full_prompt,
            #     model_name=GEMINI_MODEL
            # )
            error_detail, raw_response = generate_gemini_content_playwright(full_prompt, model_name="gemini-3.7-flash")

            # raw_response = get_llm_content(prompt=full_prompt)
            comment_info = string_to_object(raw_response)

            is_valid, error_message = check_comment_info(comment_info)
            if not is_valid:
                raise ValueError(f"结构校验不通过: {error_message}")

            return comment_info

        except Exception as e:
            # 达到上限：按原设计有限降级(返回空), 但完整记录终止原因供人工排查
            if attempt == LLM_MAX_RETRIES:
                logger.error(
                    f"[大模型/生成评论] 重试耗尽，放弃当前帖子生成，可能是模型响应格式异常或服务不稳定 "
                    f"| 关键参数: 【尝试: {attempt}/{LLM_MAX_RETRIES}】 | 结果: 【降级返回空 | 失败原因: {e}】")
                return {}

            logger.warning(
                f"[大模型/生成评论] 生成或校验失败，指数退避后重试 "
                f"| 关键参数: 【尝试: {attempt}/{LLM_MAX_RETRIES}】 | 结果: 【失败原因: {e}】")
            time.sleep(2 ** attempt)

    return {}


def fetch_post(post_manager):
    """
    按币种打捞币安广场推荐流并批量落库。
    [入参 Shape]: post_manager 提供 upsert_posts(list) 能力的 DB 管理器实例。
    """
    all_post_data = []
    for token in FEED_TOKENS:
        all_post_data.extend(fetch_binance_feed(token=token, count=100, orderBy=1))

    all_post_data.extend(fetch_binance_feed(count=100))

    if all_post_data:
        post_manager.upsert_posts(all_post_data)

    logger.info(
        f"[采集/推荐流打捞] 多币种+综合推荐流拉取完毕 "
        f"| 关键参数: 【币种数: {len(FEED_TOKENS)}】 | 结果: 【本轮采集入库: {len(all_post_data)} 条】")


def gen_all_promo_comments():
    """
    生成链路总入口：周期性采集新帖并逐条驱动大模型生成推广评论，回写入库。
    【无出入参】，直接产生副作用：读写 MongoDB。
    """
    post_manager = UniversalPostManager(gen_db_object())

    while True:
        fetch_post(post_manager)

        existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
        logger.info(
            f"[生成链路/启动] 拉取待处理帖子完毕 | 关键参数: 【总量: {len(existing_posts)}】 | 结果: 【开始逐条筛选生成】")

        skipped_invalid = skipped_exists = generated = failed = 0
        for post in existing_posts:
            if not is_valid_post_for_promo(post):
                skipped_invalid += 1
                continue

            # 幂等：已有推广评论直接跳过 (如果是因验证被删导致的重置，此处 promo_comment 为 None，会正常重新生成)
            if post.get("promo_comment"):
                skipped_exists += 1
                continue

            post_id = post.get("_id", "UNKNOWN_ID")
            comment_info = gen_promo_comment(post)
            if comment_info:
                post["promo_comment"] = comment_info
                post_manager.upsert_posts([post])
                generated += 1
                logger.info(f"[生成链路/回写] 推广评论生成并落库 | 关键参数: 【帖子ID: {post_id}】 | 结果: 【已入库】")
            else:
                failed += 1
                logger.warning(f"[生成链路/放弃] 未能生成有效评论 | 关键参数: 【帖子ID: {post_id}】 | 结果: 【本帖跳过】")

        logger.info(
            f"[生成链路/本轮小结] 处理完毕，进入休眠 "
            f"| 关键参数: 【新增生成: {generated} | 失败: {failed} | 已存在跳过: {skipped_exists} | 过滤淘汰: {skipped_invalid}】 "
            f"| 结果: 【休眠 {SCHEDULE_INTERVAL_SEC} 秒】")
        time.sleep(SCHEDULE_INTERVAL_SEC)


def get_existing_promo_comments(limit=POST_QUERY_LIMIT, hours_ago=12):
    """
    导出已生成推广评论的合规帖子，聚合"清洗后原文 + 评论结果"供离线分析。
    """
    post_manager = UniversalPostManager(gen_db_object())

    # 1. 定义时间阈值
    time_threshold = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)

    try:
        existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=limit)
    except Exception as e:
        logger.error(f"[数据导出/失败] 无法从数据库读取帖子 | 结果: 【失败原因: {e}】")
        raise

    result_list = []
    for post in existing_posts:
        # 2. 提取帖子的更新时间
        db_update_time = post.get("db_update_time")

        # 3. 时间过滤：如果帖子没有更新时间，或者更新时间早于阈值，则跳过
        if not db_update_time or db_update_time < time_threshold:
            continue

        comment_info = post.get("promo_comment")
        if not comment_info or not is_valid_post_for_promo(post):
            continue

        result_list.append({
            "cleaned_post": format_post_for_promo(post),
            "comment_info": comment_info
        })

    logger.info(f"[数据导出/完成] 聚合最近 {hours_ago} 小时内合规帖子 | 结果: 【聚合总数: {len(result_list)} 条】")
    return result_list


def clear_all_promo_comments_batch():
    """
    数据清理入口：批量把存量帖子的 promo_comment 字段置空并回写。
    【无出入参】，直接产生副作用：读写 MongoDB。
    """
    post_manager = UniversalPostManager(gen_db_object())
    existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    logger.info(
        f"[数据清理/启动] 拉取待清理帖子完毕 | 关键参数: 【总量: {len(existing_posts)}】 | 结果: 【开始扫描待清理项】")

    posts_to_update = []
    for post in existing_posts:
        if "promo_comment" in post:
            post["promo_comment"] = None
            posts_to_update.append(post)

    if posts_to_update:
        post_manager.upsert_posts(posts_to_update)
        logger.info(f"[数据清理/批量落库] 推广评论字段清空完成 | 结果: 【实际更新: {len(posts_to_update)} 条】")
    else:
        logger.info("[数据清理/批量落库] 无需清理 | 结果: 【实际更新: 0 条】")


def send_single_promo_comment(post):
    """
    为单帖组装引流参数并调用外部接口发帖。
    [返回值约定]:
      - (post, "SUCCESS") : 发帖成功，已写入成功状态
      - (post, "FAILED")  : 正常业务失败（如帖子真被删），已写入失败状态
      - (None, "CAPTCHA") : 触发人机验证/风控，未写入状态（保留本帖供下次重试），通知外层中断
      - (None, "SKIPPED") : 缺字段或不满足发帖条件
    """
    comment_info = post.get("promo_comment")
    if not comment_info:
        return None, "SKIPPED"

    promo_info = post.get("promo_comment_info")
    if isinstance(promo_info, dict):
        send_count = promo_info.get("send_count", 0)
        # 控制全局最多尝试次数为 3 次
        if send_count >= 3:
            return None, "SKIPPED"

        status = promo_info.get("status")
        verify_status = promo_info.get("verify_status")

        # 发帖放行条件：如果是已经处理过的，仅当上次发送成功 但 存活验证确认失败，才允许重新发送。
        # 如果还在 pending 或是由于业务失败(failed)，一律跳过。
        if not (status == "success" and verify_status == "failed"):
            return None, "SKIPPED"

    post_id = post.get("post_id")
    if not post_id:
        return None, "SKIPPED"

    trader_perspective = comment_info.get("follower_perspective", {})
    comment_text = trader_perspective.get("comment_text")
    link_text = trader_perspective.get("link_text")

    post_url = f"https://www.binance.com/zh-CN/square/post/{post_id}"
    my_urls = [{"text": link_text, "url": LEAD_DETAIL_URL}]

    user_data_dir, account_name = acquire_user_account_for_send()

    try:
        err, success, c_id = comment_on_binance_post(
            post_url=post_url,
            comment=comment_text,
            url_info_list=my_urls,
            user_data_dir=user_data_dir
        )
    except Exception as e:
        record_user_account_send_result(account_name, success=False, error_info=e)
        raise

    err_str = str(err or "")

    # ================= 健壮的风控 / 人机验证拦截特征 =================
    captcha_signals = [
        "我们需要确认您是人类",
        "Human Verification",
        "安全检查",
        "HTTP 状态码: 405",
        "405",
        "geetest",
        "cf-turnstile"
    ]
    is_captcha = any(signal in err_str for signal in captcha_signals)

    if is_captcha:
        record_user_account_send_result(account_name, success=False, error_info=err_str)
        logger.error(
            f"🚨 [发布链路/风控触发] 检测到【人机验证/WAF阻断】！"
            f"| 帖子ID: 【{post_id}】 | 拦截详情: 【{err_str}】 | 决策: 【不记录失败状态，保留帖子，准备紧急熔断本轮】"
        )
        return None, "CAPTCHA"

    # 正常成败处理：闭环回写数据库
    record_user_account_send_result(account_name, success=success, error_info=err)

    # 提取或初始化状态信息
    promo_info = post.get("promo_comment_info", {})
    if not isinstance(promo_info, dict):
        promo_info = {}

    if success:
        promo_info["send_count"] = promo_info.get("send_count", 0) + 1

    promo_info.update({
        "comment_id": c_id,
        "comment_time": int(time.time() * 1000),
        "status": "success" if success else "failed",
        "error_info": err if not success else None,
        "account_name": account_name,
        "verify_status": "pending",  # 重置验证状态
        "verify_time": None
    })
    post["promo_comment_info"] = promo_info

    if success:
        logger.info(
            f"[发布链路/发帖] 推广评论发布成功 | 关键参数: 【帖子ID: {post_id}】 | 结果: 【评论ID: {c_id} | 累计发送: {promo_info['send_count']} 次】")
        return post, "SUCCESS"
    else:
        logger.error(
            f"[发布链路/发帖] 发帖失败，已记录淘汰 | 关键参数: 【帖子ID: {post_id}】 | 错误详情: 【{err}】"
        )
        return post, "FAILED"


def send_promo_comments():
    """
    发布链路总入口：周期性拉取带评论的帖子并逐条发布，遭遇人机验证时自动熔断本轮回合。
    """
    while True:
        post_manager = UniversalPostManager(gen_db_object())
        existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
        logger.info(
            f"[发布链路/启动] 拉取待处理帖子完毕 | 关键参数: 【总量: {len(existing_posts)}】 | 结果: 【开始逐条发布】"
        )

        sent = skipped = failed = 0
        hit_captcha = False

        for post in existing_posts:
            if not is_valid_post_for_promo(post):
                skipped += 1
                continue

            post_result, status = send_single_promo_comment(post)

            # 遇到人机验证：直接熔断跳出，不再继续访问下一条帖子
            if status == "CAPTCHA":
                hit_captcha = True
                logger.warning(
                    f"🛑 [发布链路/熔断保护] 由于触发安全验证，立即跳过本轮回合的所有剩余任务，准备进入心跳冷却！"
                )
                break

            if status == "SUCCESS":
                post_manager.upsert_posts([post_result])
                sent += 1
            elif status == "FAILED":
                post_manager.upsert_posts([post_result])
                failed += 1
            else:
                skipped += 1

        if hit_captcha:
            logger.warning(
                f"[发布链路/本轮异常终止] 因人机验证提前终止 "
                f"| 本轮成效: 【已成功: {sent} | 业务失败: {failed} | 其它跳过: {skipped}】 "
                f"| 结果: 【进入风控避险冷却，总休眠时长 {SCHEDULE_INTERVAL_SEC} 秒】"
            )

            # 冷却期间，每隔1分钟输出一次日志，避免假死现象让运维误判
            remaining_sleep = SCHEDULE_INTERVAL_SEC
            while remaining_sleep > 0:
                logger.warning(
                    f"🚨 [发布链路/风控冷却中] 触发【人机验证/WAF阻断】，程序暂停中... 距离下次恢复运行还剩 {remaining_sleep} 秒")
                sleep_step = min(60, remaining_sleep)
                time.sleep(sleep_step)
                remaining_sleep -= sleep_step
        else:
            logger.info(
                f"[发布链路/本轮小结] 正常轮询完毕 "
                f"| 关键参数: 【已发布: {sent} | 失败: {failed} | 跳过: {skipped}】 "
                f"| 结果: 【休眠 {SCHEDULE_INTERVAL_SEC} 秒】"
            )
            time.sleep(SCHEDULE_INTERVAL_SEC)


def delete_old_replay():
    """
    删除过期的评论回复，避免账号被封禁。
    作为常驻任务运行，启动时执行一次，之后每隔1天(24小时)运行一次。
    """
    interval_sec = 1 * 60 * 60  # 1h的秒数
    while True:
        logger.info("[历史清理/启动] 开始执行过期评论清理任务")
        try:
            # 获取7天前的ms级别时间戳
            seven_days_ago_ms = int((time.time() - MAX_REPLAY_DAYS * 24 * 60 * 60) * 1000)
            time_offset = seven_days_ago_ms

            total_deleted = 0
            total_failed = 0

            for browser_session_dir in DELETE_USER_DATA_DIR_LIST:
                try:
                    account_name = _get_account_name(browser_session_dir)
                    cookies, token, user_info = get_auth_tokens_robust(browser_session_dir)

                    if not user_info or "squareUid" not in user_info:
                        logger.warning(f"[历史清理/异常] 账号: {account_name} 无法获取有效身份信息，跳过处理。")
                        continue

                    square_uid = user_info.get("squareUid")
                    replies = fetch_binance_square_replies(
                        target_square_uid=square_uid,
                        cookies=cookies,
                        csrf_token=token,
                        limit=1000,
                        time_offset=time_offset
                    )

                    if not replies:
                        continue

                    for item in replies:
                        reply_id = item.get("reply_id")
                        if reply_id:
                            try:
                                success = delete_binance_square_content(
                                    content_id=reply_id, cookies=cookies, csrf_token=token
                                )
                                if success:
                                    total_deleted += 1
                                    logger.info(
                                        f"[历史清理/删除成功] 账号: {account_name} | reply_id={reply_id} | 内容: {item.get('reply_text', '')[:15]}...")
                                else:
                                    total_failed += 1
                                    logger.warning(f"[🔴历史清理/删除失败] 账号: {account_name} | reply_id={reply_id}")
                            except Exception as e:
                                total_failed += 1
                                logger.error(
                                    f"[历史清理/请求异常] 账号: {account_name} | reply_id={reply_id} | 报错信息: {e}")

                except Exception as e:
                    logger.error(f"[历史清理/账号异常] 处理账号 {browser_session_dir} 时发生未捕获异常: {e}")

            logger.info(
                f"[历史清理/本轮小结] 任务完成 | 成功删除: {total_deleted} 条 | 失败: {total_failed} 条 | 准备休眠 {interval_sec} 秒(1天)后再次执行")

        except Exception as e:
            logger.error(f"[历史清理/全局异常] 清理任务发生致命错误: {e}")

        time.sleep(interval_sec)


def verify_promo_comments_task():
    """
    [新增线程] 监控已发送评论的存活率：
    对 10 分钟之前成功发送、且当前仍符合发帖标准、且未验证成功的帖子，
    查询其评论列表并进行存在性验证。拉取数据后同步更新原帖 comments 字段。
    发现被删时记录状态，如果重试未达上限（<3次），则清空已生成的评论内容让其再次进入生成、发送闭环。
    """
    verify_interval_sec = 300  # 每 5 分钟轮询一次
    while True:
        try:
            post_manager = UniversalPostManager(gen_db_object())
            existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)

            verified_success = 0
            verified_failed = 0

            for post in existing_posts:
                promo_info = post.get("promo_comment_info")
                if not isinstance(promo_info, dict):
                    continue

                # 仅验证曾成功下发的
                if promo_info.get("status") != "success":
                    continue

                # 已经验证确认存活的不再重复查
                if promo_info.get("verify_status") == "success":
                    continue

                comment_time_ms = promo_info.get("comment_time", 0)
                if not comment_time_ms:
                    continue

                # 时间需至少发布在 10 分钟以前
                if time.time() * 1000 - comment_time_ms < 10 * 60 * 1000:
                    continue

                # 原帖要求判定，若自身已超龄、过载评论，则自动放弃重试
                if not is_valid_post_for_promo(post):
                    continue

                post_id = post.get("post_id")
                if not post_id:
                    continue

                # 拉取最新评论信息 (为了防止原帖评论多漏查我们自己的回复，适当增加 required_count=100)
                try:
                    comments = fetch_binance_replies(
                        content_id=post_id,
                        sort_by=1,  # 综合热门，兼顾后期更新DB的数据质量
                        required_count=100
                    )
                except Exception as e:
                    logger.error(f"[验证链路/查询异常] 拉取帖子评论失败 | 帖子ID: {post_id} | 错误: {e}")
                    continue

                if not isinstance(comments, list):
                    continue

                # 替换原数据库中的 comments
                post["comments"] = comments

                # 通过 reply_id 定位判断我们的回帖是否存在
                my_comment_id = str(promo_info.get("comment_id", ""))
                is_survived = False
                for c in comments:
                    if str(c.get("reply_id", "")) == my_comment_id:
                        is_survived = True
                        break

                promo_info["verify_time"] = int(time.time() * 1000)

                if is_survived:
                    promo_info["verify_status"] = "success"
                    verified_success += 1
                    logger.info(f"[验证链路/成功] 帖子: {post_id} 的推广评论目前处于存活状态")
                else:
                    promo_info["verify_status"] = "failed"
                    verified_failed += 1
                    send_count = promo_info.get("send_count", 0)

                    logger.warning(
                        f"[验证链路/被删] 帖子: {post_id} 的推广评论不存在/被系统删除 (当前成功发送总计: {send_count}次)")

                    # 联动补发机制：若验证失败且重试未达上限，清除已有生成内容，使其进入下一轮循环重新获取文案
                    if send_count < 3:
                        post["promo_comment"] = None
                        logger.info(
                            f"[验证链路/重置] 帖子: {post_id} 满足重新调度条件，promo_comment 已置空等待下一轮重试")

                # 最终将修改(存活状态 + 最新覆盖的comments)写入数据库
                post_manager.upsert_posts([post])

            logger.info(
                f"[验证链路/本轮小结] 验证存活: {verified_success} | 确认被删: {verified_failed} | 准备休眠 {verify_interval_sec} 秒")

        except Exception as e:
            logger.error(f"[验证链路/全局异常] 任务发生致命错误: {e}")

        time.sleep(verify_interval_sec)


# ==========================================
# 运行入口：生成链路、发布链路、历史清理、存活验证 各起一个守护线程并行运行
# ==========================================
if __name__ == "__main__":
    tasks = [
        send_promo_comments,
        gen_all_promo_comments,  # 注释本行即可停用"评论生成"链路
        delete_old_replay,  # 定时清理过期评论(启动即执行，后续按天循环)
        verify_promo_comments_task  # 监控已发评论的存活率与自动补发
    ]

    threads = []
    for task in tasks:
        t = threading.Thread(target=task)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # ---- 一次性运维/离线分析工具（按需手动启用）----
    # clear_all_promo_comments_batch()
    # data = get_existing_promo_comments()