# [功能摘要] 采集币安广场帖子，生成推广评论，按账号发布并验证存活情况、清理历史回复。
# [输入数据] 推荐流/MongoDB 帖子：post_id、author、content、comments、engagement、metadata、
#            publish_time；提示词文件、浏览器账号目录和账号计数 JSON。
# [数据流转/交互] 推荐流 -> MongoDB -> 黑名单/时效/互动过滤 -> 正文与热评 -> Gemini 双视角校验
#                -> promo_comment；按分数选前 10 帖，发布 follower_perspective，更新账号 JSON、
#                promo_comment_info/history；验证覆盖 comments，未找到且成功发送不足 3 次则清空文案。
#                独立清理任务取得账号凭证，以 7 天前的 time_offset 查询并删除接口返回的回复。
# [输出数据] MongoDB 文案、发送历史、验证状态和最新评论；账号计数 JSON；平台发布/删除及运行日志。
# : 沿用整帖 upsert，多线程可能覆盖彼此的新状态；确认数据库局部更新能力后再改。

import datetime
import json
import ntpath
import os
import re
import tempfile
import threading
import time
from copy import deepcopy

from app.ai_api.gemini_playwright import generate_gemini_content_playwright
from biance.biance_playwright import comment_on_binance_post, get_auth_tokens_robust
from biance.biance_squre_api import (
    delete_binance_square_content,
    fetch_binance_feed,
    fetch_binance_replies,
    fetch_binance_square_replies,
)
from common.common_utils import read_file_to_str, setup_logger, string_to_object
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager

logger = setup_logger(app_name="promo_copy")

BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
SCHEDULE_INTERVAL_SEC = 3600
COMMENT_SEND_INTERVAL_SEC = 60
ACCOUNT_STATS_SYNC_INTERVAL_SEC = 3600
LLM_MAX_RETRIES = 3
GEMINI_MODEL = "gemini-3.7-flash"
MAX_SUCCESSFUL_SENDS = 3
MAX_REPLAY_DAYS = 7
MAX_DAILY_SUCCESS_PER_ACCOUNT = 100
DELETE_INTERVAL_SEC = 3600
VERIFY_INTERVAL_SEC = 300
VERIFY_MIN_AGE_MS = 10 * 60 * 1000

FEED_TOKENS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE"]
PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\带单推广评论生成.txt"
USER_DATA_DIR_LIST = [
    r"W:\temp\biance_nana",
    r"W:\temp\biance_yang",
    r"W:\temp\biance_daniang",
    r"W:\temp\biance_mama",
    r"W:\temp\biance_jie",
    r"W:\temp\biance_qiqi",
    r"W:\temp\biance_zhouling",
    r"W:\temp\biance_yanglin",
    r"W:\temp\biance_ruru",
]

DELETE_USER_DATA_DIR_LIST = [
    r"W:\temp\biance_nana", r"W:\temp\biance_yang", r"W:\temp\biance_daniang",
    r"W:\temp\biance_mama", r"W:\temp\biance_jie", r"W:\temp\biance_qiqi",
    r"W:\temp\biance_zhouling", r"W:\temp\biance_yanglin", r"W:\temp\biance_ruru",
]
USER_ACCOUNT_USAGE_FILE = r"W:\project\python_project\crypto_trade\biance\promotion_copy_trade\biance_account_usage.json"
LEAD_DETAIL_URL = "https://www.binance.com/zh-CN/square/post/362858558969979"

FILTER_CONFIG = {
    "blacklist_keywords": [
        "瓜分", "抽奖", "红包", "空投", "新粉福利", "转发", "留下你的",
        "giveaway", "prize pool", "airdrop", "split",
    ],
    "blacklist_multi_words": ["follow", "share", "comment"],
    "min_text_length": 20,
    "max_age_hours": 24,
    "max_comment_count": 100,
    "cold_post_hours": 20,
    "cold_post_min_views": 20,
}
COMMENT_STATS_DEFAULTS = {"attempts": 0, "success": 0, "survived": 0, "failed": 0}
ACCOUNT_USAGE_DEFAULTS = {
    "total_count": 0, "success_count": 0, "failure_count": 0,
    "last_failure_reason": None, "last_send_time": 0, "update_time": None,
    "daily_stats": COMMENT_STATS_DEFAULTS.copy(), "daily_stats_sync_time": 0,
}
# : 大小写敏感的子串匹配保持不变；任何包含“405”的错误都可能触发暂停。
CAPTCHA_SIGNALS = (
    "我们需要确认您是人类", "Human Verification", "安全检查",
    "405", "geetest", "cf-turnstile",
)
# : 锁只保护本进程的账号计数；同名目录共用记录，发布与清理可能并用浏览器目录。
_user_account_usage_lock = threading.Lock()


def _get_account_name(user_data_dir):
    """以末级目录名识别账号，兼容在其他系统上解析 Windows 路径。"""
    return ntpath.basename(ntpath.normpath(user_data_dir))


def _save_user_account_usage_unlocked(usage_data):
    """持锁调用；同盘原子替换 JSON，失败时自动关闭文件并清理临时目录。
    入参：{账号名: {total_count, success_count, failure_count, last_send_time,
    last_failure_reason, update_time, daily_stats, daily_stats_sync_time}}；保留附加字段。
    """
    usage_dir = os.path.dirname(USER_ACCOUNT_USAGE_FILE)
    if usage_dir:
        os.makedirs(usage_dir, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=usage_dir or ".", prefix=".account-usage-") as temp_dir:
        temp_path = os.path.join(temp_dir, "usage.json")
        with open(temp_path, "w", encoding="utf-8") as usage_file:
            json.dump(usage_data, usage_file, ensure_ascii=False, indent=2)
        os.replace(temp_path, USER_ACCOUNT_USAGE_FILE)


def _load_user_account_usage_unlocked():
    """持锁读取并补齐配置账号；返回账号计数字典，形状见保存函数。
    daily_stats 含 attempts/success/survived/failed；各账号的默认字典相互独立。
    损坏 JSON 抛错，保留未配置账号及附加字段。
    """
    if not USER_DATA_DIR_LIST:
        raise ValueError("USER_DATA_DIR_LIST 不能为空")
    usage_data = {}
    if os.path.exists(USER_ACCOUNT_USAGE_FILE):
        try:
            with open(USER_ACCOUNT_USAGE_FILE, "r", encoding="utf-8") as usage_file:
                usage_data = json.load(usage_file)
        except Exception as exc:
            raise RuntimeError(
                f"读取账号计数失败，请检查权限或 JSON 格式 | 文件: 【{USER_ACCOUNT_USAGE_FILE}】"
            ) from exc
        if not isinstance(usage_data, dict):
            raise ValueError(f"账号计数 JSON 顶层必须是字典 | 文件: 【{USER_ACCOUNT_USAGE_FILE}】")

    changed = False
    for user_data_dir in USER_DATA_DIR_LIST:
        account_name = _get_account_name(user_data_dir)
        if not account_name:
            raise ValueError(f"账号目录没有有效名称: {user_data_dir}")
        account_usage = usage_data.get(account_name)
        if not isinstance(account_usage, dict):
            account_usage = usage_data[account_name] = {}
            changed = True
        for field, value in ACCOUNT_USAGE_DEFAULTS.items():
            if field not in account_usage:
                account_usage[field] = deepcopy(value)
                changed = True
    if changed:
        _save_user_account_usage_unlocked(usage_data)
    return usage_data


def acquire_user_account_for_send():
    """同步统计后按累计使用量预占账号；额度不足返回 (None, None)，冷却时在锁外等待。
    返回：(user_data_dir, account_name)；统计形状为 {账号: {attempts, success, survived, failed}}。
    """
    while True:
        with _user_account_usage_lock:
            usage_data = _load_user_account_usage_unlocked()
            now = time.time()
            accounts = [(_get_account_name(path), path) for path in USER_DATA_DIR_LIST]
            # : “每日”实际为滚动 24 小时，小时缓存不随本次发送递增；缺失账号按零计。
            # 同步仍在锁内执行，失败继续使用当前内存统计，可能超限或长时间占锁。
            if any(
                now - usage_data[name].get("daily_stats_sync_time", 0) > ACCOUNT_STATS_SYNC_INTERVAL_SEC
                for name, _ in accounts
            ):
                try:
                    account_stats = calculate_comment_survival_rate(days=1)
                    for name, _ in accounts:
                        usage_data[name].update({
                            "daily_stats": account_stats.get(name, COMMENT_STATS_DEFAULTS.copy()),
                            "daily_stats_sync_time": now,
                        })
                    _save_user_account_usage_unlocked(usage_data)
                except Exception as exc:
                    logger.error(
                        f"[发布/账号监控] 同步限额统计失败，可能是数据库或计数文件不可用"
                        f" | 异常: 【{exc!r}】 | 结果: 【沿用当前内存统计继续选账号】"
                    )

            available, cooldowns = [], []
            for index, (account_name, user_data_dir) in enumerate(accounts):
                account_usage = usage_data[account_name]
                if account_usage.get("daily_stats", {}).get("success", 0) >= MAX_DAILY_SUCCESS_PER_ACCOUNT:
                    continue
                total_count = int(account_usage.get("total_count", 0) or 0)
                last_send_time = float(account_usage.get("last_send_time", 0) or 0)
                elapsed = now - last_send_time
                if last_send_time <= 0 or elapsed > COMMENT_SEND_INTERVAL_SEC:
                    available.append((total_count, index, user_data_dir, account_name))
                    continue
                cooldowns.append(COMMENT_SEND_INTERVAL_SEC - elapsed)

            if available:
                total_count, _, user_data_dir, account_name = min(available)
                usage_data[account_name].update({
                    "total_count": total_count + 1,
                    "last_send_time": time.time(),
                    "update_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                })
                _save_user_account_usage_unlocked(usage_data)
                return user_data_dir, account_name
            if not cooldowns:
                return None, None
            wait_seconds = max(min(cooldowns), 0) + 0.05
        logger.info(
            f"[发布/账号冷却] 可用额度内的账号均在冷却 | 最小间隔: 【{COMMENT_SEND_INTERVAL_SEC} 秒】"
            f" | 结果: 【等待 {wait_seconds:.2f} 秒后重选】"
        )
        time.sleep(wait_seconds)


def record_user_account_send_result(account_name, success, error_info=None):
    """累加实际请求成败；error_info 为错误文本或异常对象，持锁写回账号 JSON。"""
    with _user_account_usage_lock:
        usage_data = _load_user_account_usage_unlocked()
        account_usage = usage_data.setdefault(account_name, deepcopy(ACCOUNT_USAGE_DEFAULTS))
        field = "success_count" if success else "failure_count"
        account_usage[field] = int(account_usage.get(field, 0) or 0) + 1
        # : 成功不清空 last_failure_reason；该字段不代表最近一次请求的结果。
        if not success:
            account_usage["last_failure_reason"] = str(error_info or "未知失败原因")
        account_usage["update_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        _save_user_account_usage_unlocked(usage_data)


def is_valid_post_for_promo(post):
    """按黑名单、正文、时效和互动水位筛选，返回 bool。
    入参：content.text_content、metadata.is_ai_generated、engagement.{comment_count, view_count}、
    publish_time（秒或毫秒数值）；嵌套节点为字典，缺省字段沿用原默认值。
    """
    text = (post.get("content", {}).get("text_content") or "").lower()
    engagement = post.get("engagement", {})
    if any(word in text for word in FILTER_CONFIG["blacklist_keywords"]):
        return False
    if all(word in text for word in FILTER_CONFIG["blacklist_multi_words"]):
        return False
    clean_text = re.sub(r"#\S+", "", re.sub(r"http[s]?://\S+", "", text)).strip()
    if len(clean_text) < FILTER_CONFIG["min_text_length"]:
        return False
    if post.get("metadata", {}).get("is_ai_generated") is True:
        return False
    publish_time = post.get("publish_time", 0)
    if publish_time > 1e11:
        publish_time /= 1000
    # : 未来时间戳仍参与筛选，不单独拒绝。
    age_hours = (time.time() - publish_time) / 3600
    if age_hours > FILTER_CONFIG["max_age_hours"]:
        return False
    if engagement.get("comment_count", 0) > FILTER_CONFIG["max_comment_count"]:
        return False
    return not (
        age_hours > FILTER_CONFIG["cold_post_hours"]
        and engagement.get("view_count", 0) < FILTER_CONFIG["cold_post_min_views"]
    )


def format_post_for_promo(raw_data):
    """移除媒体占位符并提取热评，保持模型上下文形状。
    入参：author.author_name、content.{text_content, mentioned_coins}、
    comments=[{content, likes, replies, views}]；文本字段为字符串。
    返回：{post: {author, text, coins}, top_comments: [评论文本]}。
    """
    content = raw_data.get("content", {})
    text = re.sub(r"\[(?:长文封面|插图|视频封面|视频):.*?\]", "", content.get("text_content", ""))
    comments = sorted(
        raw_data.get("comments", []),
        key=lambda item: (item.get("likes", 0), item.get("replies", 0), item.get("views", 0)),
        reverse=True,
    )
    # : 先取热度前五再去空，结果可能不足五条。
    top_comments = [item.get("content", "").strip() for item in comments[:5]]
    return {
        "post": {
            "author": raw_data.get("author", {}).get("author_name", "未知用户"),
            "text": re.sub(r"\n{3,}", "\n\n", text).strip(),
            "coins": content.get("mentioned_coins", []),
        },
        "top_comments": [comment for comment in top_comments if comment],
    }


def check_comment_info(data):
    """校验模型结构和原有文案规则，返回 (是否有效, 错误说明)。
    入参：{post_analysis: {}, trader_perspective: 视角字典, follower_perspective: 视角字典}；
    视角及其 reply_draft 的必要 Key 见下方字段集合；保留附加字段。
    """
    if not isinstance(data, dict):
        return False, "大模型返回数据不是有效的字典对象"
    if not isinstance(data.get("post_analysis"), dict):
        return False, "缺失顶层字段或类型错误: post_analysis"
    required_fields = (
        "angle", "comment_text", "link_text", "link_target", "combined_preview",
        "reply_draft", "score", "score_tier", "score_reason",
    )
    reply_required_fields = ("comment_text", "conversion_role", "score", "score_tier", "score_reason")
    # : 空正文、bool 分数仍可通过；分析及部分说明字段只检查存在性/外层形状。
    # 子评论仍不检查终止标点，避免擅自扩大校验规则。
    for perspective in ("trader_perspective", "follower_perspective"):
        if perspective not in data:
            return False, f"缺失顶层角色字段: {perspective}"
        view = data[perspective]
        if not isinstance(view, dict):
            return False, f"{perspective} 必须是字典结构"
        missing = [field for field in required_fields if field not in view]
        if missing:
            return False, f"{perspective} 缺失必要字段: {missing}"
        comment_text, link_text, score = view["comment_text"], view["link_text"], view["score"]
        if not isinstance(comment_text, str):
            return False, f"{perspective}.comment_text 类型非字符串"
        if comment_text.endswith(("。", "！", "？", ".", "!", "?")):
            return False, f"{perspective}.comment_text 违规：绝对禁止以终止性标点结尾"
        if not isinstance(link_text, str):
            return False, f"{perspective}.link_text 类型非字符串"
        # : 实际边界为 2–10 字；如需改为旧提示中的 2–6 字，须先确认业务规则。
        if not 2 <= len(link_text) <= 10:
            return False, f"{perspective}.link_text 违规：引流文案长度必须严格在 2-10 个字符之间"
        if not isinstance(score, (int, float)):
            return False, f"{perspective}.score 类型非数字"
        if not 0 <= score <= 10:
            return False, f"{perspective}.score 违规：评分必须介于 0-10 之间"
        reply_draft = view["reply_draft"]
        if not isinstance(reply_draft, dict):
            return False, f"{perspective}.reply_draft 必须是字典结构"
        missing = [field for field in reply_required_fields if field not in reply_draft]
        if missing:
            return False, f"{perspective}.reply_draft 缺失必要字段: {missing}"
        if not isinstance(reply_draft["comment_text"], str):
            return False, f"{perspective}.reply_draft.comment_text 类型非字符串"
        if not isinstance(reply_draft["score"], (int, float)) or not 0 <= reply_draft["score"] <= 10:
            return False, f"{perspective}.reply_draft.score 违规：子评论评分必须为 0-10 之间的数字"
    return True, ""


def gen_promo_comment(post):
    """生成并校验双视角文案；入参形状见格式化函数，返回校验函数所述字典或 {}。
    仅模型请求/解析/校验失败按原规则重试并降级；文件读取和正文清洗异常继续传播。
    """
    cleaned_post = format_post_for_promo(post)
    prompt = read_file_to_str(PROMPT_FILE_PATH)
    full_prompt = f"{prompt}\n{cleaned_post}"
    post_id = post.get("post_id", post.get("_id", "UNKNOWN_ID"))
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        error_detail = None
        try:
            error_detail, raw_response = generate_gemini_content_playwright(
                full_prompt, model_name=GEMINI_MODEL
            )
            comment_info = string_to_object(raw_response)
            is_valid, error_message = check_comment_info(comment_info)
            if not is_valid:
                raise ValueError(f"结构校验不通过: {error_message}")
            # : 响应通过校验即成功，不因非空 error_detail 单独判失败。
            return comment_info
        except Exception as exc:
            exhausted = attempt == LLM_MAX_RETRIES
            delay = 2 ** attempt
            log = logger.error if exhausted else logger.warning
            action = "重试耗尽，返回空字典" if exhausted else f"等待 {delay} 秒重试"
            log(
                f"[生成/模型异常] 请求或校验失败，可能是服务不稳定或响应格式不符"
                f" | 帖子: 【{post_id}】 | 尝试: 【{attempt}/{LLM_MAX_RETRIES}】"
                f" | 异常: 【{exc!r}】 | 接口提示: 【{error_detail or '无'}】 | 结果: 【{action}】"
            )
            if exhausted:
                return {}
            time.sleep(delay)
    return {}


def fetch_post(post_manager):
    """按币种顺序及综合推荐流采集后统一入库，保留重复帖子和接口参数。
    入参：提供 upsert_posts([帖子字典]) 的管理器；无返回。
    """
    started = time.monotonic()
    posts = []
    for token in FEED_TOKENS:
        posts.extend(fetch_binance_feed(token=token, count=100, orderBy=1))
    posts.extend(fetch_binance_feed(count=100))
    if posts:
        post_manager.upsert_posts(posts)
    logger.info(
        f"[采集/完成] 推荐流已处理 | 币种数: 【{len(FEED_TOKENS)}】"
        f" | 入库条目: 【{len(posts)}】 | 耗时: 【{time.monotonic() - started:.2f} 秒】"
    )


def gen_all_promo_comments():
    """常驻采集、筛选、生成并回写文案；每轮结束休眠，未处理异常终止当前线程。"""
    post_manager = UniversalPostManager(gen_db_object())
    while True:
        started = time.monotonic()
        fetch_post(post_manager)
        posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
        logger.info(f"[生成/开始] 准备筛选帖子 | 扫描: 【{len(posts)}】")
        invalid = existing = generated = failed = 0
        for post in posts:
            if not is_valid_post_for_promo(post):
                invalid += 1
                continue
            if post.get("promo_comment"):
                existing += 1
                continue
            comment_info = gen_promo_comment(post)
            if not comment_info:
                failed += 1
                continue
            post["promo_comment"] = comment_info
            post_manager.upsert_posts([post])
            generated += 1
            logger.info(f"[生成/回写] 双视角文案已入库 | 帖子: 【{post.get('post_id', post.get('_id'))}】")
        logger.info(
            f"[生成/本轮小结] 本轮处理完成 | 生成: 【{generated}】 | 失败: 【{failed}】"
            f" | 已有文案: 【{existing}】 | 过滤: 【{invalid}】"
            f" | 耗时: 【{time.monotonic() - started:.2f} 秒】 | 休眠: 【{SCHEDULE_INTERVAL_SEC} 秒】"
        )
        time.sleep(SCHEDULE_INTERVAL_SEC)


def get_existing_promo_comments(limit=POST_QUERY_LIMIT, hours_ago=12):
    """导出近期更新且有效的文案；输入参数为查询数量和回溯小时数。
    数据库依赖 db_update_time（无时区 datetime）、promo_comment 和帖子字段。
    返回：[{cleaned_post: {post, top_comments}, comment_info: 双视角字典}]。
    """
    post_manager = UniversalPostManager(gen_db_object())
    # : 保留本地无时区比较；跨时区处理需明确数据库的时间约定。
    threshold = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    try:
        posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=limit)
    except Exception as exc:
        logger.error(f"[导出/读取失败] 无法查询帖子，请检查数据库连接和权限 | 异常: 【{exc!r}】")
        raise
    result = []
    for post in posts:
        updated_at = post.get("db_update_time")
        if not updated_at or updated_at < threshold:
            continue
        comment_info = post.get("promo_comment")
        if comment_info and is_valid_post_for_promo(post):
            result.append({"cleaned_post": format_post_for_promo(post), "comment_info": comment_info})
    logger.info(f"[导出/完成] 已聚合有效文案 | 时间范围: 【{hours_ago} 小时】 | 条数: 【{len(result)}】")
    return result


def clear_all_promo_comments_batch():
    """批量清空已存在的 promo_comment 字段；无返回，保留其他帖子字段。"""
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    updates = [post for post in posts if "promo_comment" in post]
    # : 不重置 promo_comment_info；清空文案不等于允许重新发布。
    for post in updates:
        post["promo_comment"] = None
    if updates:
        post_manager.upsert_posts(updates)
    logger.info(f"[文案清理/完成] 已清空目标字段 | 扫描: 【{len(posts)}】 | 更新: 【{len(updates)}】")


def _can_send_promo_comment(post):
    """共用发布资格判断，避免名单扫描与单帖发送规则分叉；返回 bool。
    入参：post_id、promo_comment、promo_comment_info.{send_count, status, verify_status}。
    """
    if not post.get("promo_comment"):
        return False
    promo_info = post.get("promo_comment_info")
    # : 字典状态只允许“发送成功但验证失败”重发；空字典或发送失败均不重试。
    if isinstance(promo_info, dict) and (
        promo_info.get("send_count", 0) >= MAX_SUCCESSFUL_SENDS
        or promo_info.get("status") != "success"
        or promo_info.get("verify_status") != "failed"
    ):
        return False
    # : 保留 str() 规则；None 会变成非空的 "None"，ID 有效性需另行确认。
    return bool(str(post.get("post_id", "")))


def send_single_promo_comment(post):
    """发布跟随者视角文案，更新内存状态；数据库回写由调用方负责。
    入参：发布资格字段及 promo_comment.follower_perspective.{comment_text, link_text}。
    出参：(帖子或 None, SUCCESS/FAILED/SKIPPED/CAPTCHA/ALL_ACCOUNTS_EXHAUSTED)。
    promo_comment_info/history 记录 account_name、comment_id、comment_time、status、
    error_info、verify_status、verify_time；send_count 仅累计成功发送，时间为毫秒。
    """
    if not _can_send_promo_comment(post):
        return None, "SKIPPED"
    post_id = str(post.get("post_id", ""))
    promo_info = post.get("promo_comment_info")
    follower = post["promo_comment"].get("follower_perspective", {})
    comment_text, link_text = follower.get("comment_text"), follower.get("link_text")
    user_data_dir, account_name = acquire_user_account_for_send()
    if not user_data_dir:
        return None, "ALL_ACCOUNTS_EXHAUSTED"

    started = time.monotonic()
    try:
        # : 仅发布 follower_perspective，链接固定；模型 link_target 和 reply_draft 不用于发布。
        error, success, comment_id = comment_on_binance_post(
            post_url=f"https://www.binance.com/zh-CN/square/post/{post_id}",
            comment=comment_text,
            url_info_list=[{"text": link_text, "url": LEAD_DETAIL_URL}],
            user_data_dir=user_data_dir,
        )
        comment_id = str(comment_id) if comment_id else ""
    except Exception as send_error:
        record_error = None
        try:
            record_user_account_send_result(account_name, success=False, error_info=send_error)
        except Exception as exc:
            record_error = exc
        logger.error(
            f"[发布/调用异常] 发帖请求异常，实际结果未知，请检查登录、网络和接口响应"
            f" | 帖子: 【{post_id}】 | 账号: 【{account_name}】 | 异常: 【{send_error!r}】"
            f" | 失败计数: 【{'已记录' if record_error is None else repr(record_error)}】"
            f" | 结果: 【保留原始发送异常并向外抛出】"
        )
        if record_error is not None:
            raise send_error from record_error
        raise

    error_text = str(error or "")
    is_captcha = any(signal in error_text for signal in CAPTCHA_SIGNALS)
    try:
        record_user_account_send_result(
            account_name, success=False if is_captcha else success,
            error_info=error_text if is_captcha else error,
        )
    except Exception as exc:
        logger.error(
            f"[发布/计数失败] 接口已返回，但账号计数未保存，请检查文件权限并核对实际评论"
            f" | 帖子: 【{post_id}】 | 账号: 【{account_name}】 | 评论: 【{comment_id}】"
            f" | 接口成功标志: 【{success}】 | 异常: 【{exc!r}】 | 结果: 【向外抛出，帖子未回写】"
        )
        raise
    # : 安全验证优先于 success；计账号失败，但不更新帖子或追加发送历史。
    if is_captcha:
        logger.error(
            f"[发布/安全验证] 接口提示人机验证或访问阻断，需要检查账号验证状态"
            f" | 帖子: 【{post_id}】 | 账号: 【{account_name}】 | 接口提示: 【{error_text}】"
            f" | 结果: 【停止本轮，保留帖子原状态；账号已计失败】"
        )
        return None, "CAPTCHA"

    if not isinstance(promo_info, dict):
        promo_info = {}
    # : 以接口 success 为成功依据，即使 comment_id 为空也不改判。
    if success:
        promo_info["send_count"] = promo_info.get("send_count", 0) + 1
    send_record = {
        "account_name": account_name,
        "comment_id": comment_id,
        "comment_time": int(time.time() * 1000),
        "status": "success" if success else "failed",
        "error_info": None if success else error,
        "verify_status": "pending",
        "verify_time": None,
    }
    promo_info.update(send_record)
    history = promo_info.get("history")
    if not isinstance(history, list):
        history = []
    history.append(send_record)
    promo_info["history"] = history
    post["promo_comment_info"] = promo_info
    log = logger.info if success else logger.error
    outcome = "接口报告成功" if success else "接口报告失败，可能是帖子不可用或账号受限"
    log(
        f"[发布/请求结果] {outcome} | 帖子: 【{post_id}】 | 账号: 【{account_name}】"
        f" | 评论: 【{comment_id}】 | 成功次数: 【{promo_info.get('send_count', 0)}】"
        f" | 接口提示: 【{error_text or '无'}】 | 耗时: 【{time.monotonic() - started:.2f} 秒】"
        f" | 状态: 【{promo_info['status']}，待调用方落库】"
    )
    return post, "SUCCESS" if success else "FAILED"


def send_promo_comments():
    """每轮按 follower_perspective.score 稳定降序选前十；逐帖回写，验证阻断或限额触发则停止本轮。"""
    while True:
        started = time.monotonic()
        post_manager = UniversalPostManager(gen_db_object())
        posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
        candidates = []
        for post in posts:
            if not is_valid_post_for_promo(post) or not _can_send_promo_comment(post):
                continue
            try:
                score = float(post["promo_comment"].get("follower_perspective", {}).get("score", 0))
            except (ValueError, TypeError):
                score = 0.0
            candidates.append((score, post))
        candidates.sort(key=lambda item: item[0], reverse=True)
        top_candidates = candidates[:10]
        scores = [score for score, _ in top_candidates]
        score_summary = (
            f"最高 {max(scores)}，最低 {min(scores)}，平均 {sum(scores) / len(scores):.2f}"
            if scores else "无待发送候选"
        )
        logger.info(
            f"[发布/名单扫描] 候选筛选完成 | 扫描: 【{len(posts)}】 | 候选: 【{len(candidates)}】"
            f" | 计划发布: 【{len(top_candidates)}】 | 评分: 【{score_summary}】"
        )
        counts = {"SUCCESS": 0, "FAILED": 0, "SKIPPED": 0}
        stop_reason = None
        for _, post in top_candidates:
            post_result, status = send_single_promo_comment(post)
            if status in ("ALL_ACCOUNTS_EXHAUSTED", "CAPTCHA"):
                stop_reason = status
                break
            if status not in ("SUCCESS", "FAILED"):
                counts["SKIPPED"] += 1
                continue
            try:
                post_manager.upsert_posts([post_result])
            except Exception as exc:
                logger.error(
                    f"[发布/回写失败] 发帖接口已返回，但状态未确认入库，请检查数据库并核对实际评论"
                    f" | 帖子: 【{post.get('post_id')}】 | 接口结果: 【{status}】 | 异常: 【{exc!r}】"
                )
                raise
            counts[status] += 1
        outcome = {
            "ALL_ACCOUNTS_EXHAUSTED": "缓存统计显示账号额度已满，本轮提前停止",
            "CAPTCHA": "接口提示安全验证，本轮提前停止，请检查账号验证状态",
        }.get(stop_reason, "本轮处理完成")
        log = logger.warning if stop_reason else logger.info
        log(
            f"[发布/本轮小结] {outcome} | 已回写成功: 【{counts['SUCCESS']}】"
            f" | 已回写失败: 【{counts['FAILED']}】 | 跳过: 【{counts['SKIPPED']}】"
            f" | 耗时: 【{time.monotonic() - started:.2f} 秒】 | 休眠: 【{SCHEDULE_INTERVAL_SEC} 秒】"
        )
        if stop_reason == "CAPTCHA":
            remaining = SCHEDULE_INTERVAL_SEC
            while remaining > 0:
                sleep_step = min(60, remaining)
                time.sleep(sleep_step)
                remaining -= sleep_step
                if remaining > 0:
                    logger.info(f"[发布/验证冷却] 安全验证触发后暂停中 | 距下轮检查: 【{remaining} 秒】")
        else:
            time.sleep(SCHEDULE_INTERVAL_SEC)

def delete_old_replay():
    """每小时按账号清理历史回复；沿用逐条、逐账号及整轮异常隔离，不中断后续轮次。
    身份响应含 squareUid；回复列表项含 reply_id，删除接口接收字符串 ID。
    """
    while True:
        started = time.monotonic()
        deleted = failed = account_errors = 0
        DELETE_USER_DATA_DIR_LIST.reverse()  # 恢复交替反转顺序
        logger.info(f"[历史清理/开始] 准备查询历史回复 | 账号数: 【{len(DELETE_USER_DATA_DIR_LIST)}】")
        try:
            cutoff_ms = int((time.time() - MAX_REPLAY_DAYS * 24 * 60 * 60) * 1000)
            for browser_session_dir in DELETE_USER_DATA_DIR_LIST:
                try:
                    account_name = _get_account_name(browser_session_dir)
                    cookies, token, user_info = get_auth_tokens_robust(browser_session_dir)
                    if not user_info or "squareUid" not in user_info:
                        account_errors += 1
                        logger.warning(
                            f"[历史清理/身份缺失] 无法取得 squareUid，可能是登录失效"
                            f" | 账号: 【{account_name}】 | 结果: 【跳过该账号】"
                        )
                        continue
                    # : 仅依赖 time_offset 获取最多 1000 条，不逐条核验日期，也不补充分页。
                    replies = fetch_binance_square_replies(
                        target_square_uid=user_info.get("squareUid"),
                        cookies=cookies, csrf_token=token, limit=1000, time_offset=cutoff_ms,
                    )
                    if not replies:
                        continue
                    before_deleted, before_failed = deleted, failed
                    for item in replies:
                        raw_reply_id = item.get("reply_id")
                        if not raw_reply_id:
                            continue
                        reply_id = str(raw_reply_id)
                        try:
                            success = delete_binance_square_content(
                                content_id=reply_id, cookies=cookies, csrf_token=token
                            )
                        except Exception as exc:
                            failed += 1
                            logger.error(
                                f"[历史清理/请求异常] 删除回复异常，可能是网络或身份凭证失效"
                                f" | 账号: 【{account_name}】 | 回复: 【{reply_id}】 | 异常: 【{exc!r}】"
                                f" | 结果: 【继续处理其余回复】"
                            )
                            continue
                        if success:
                            deleted += 1
                            continue
                        failed += 1
                        logger.warning(
                            f"[历史清理/删除失败] 接口未确认删除，可能是回复已不存在或权限不足"
                            f" | 账号: 【{account_name}】 | 回复: 【{reply_id}】 | 结果: 【继续处理】"
                        )
                    logger.info(
                        f"[历史清理/账号完成] 已处理接口返回的回复 | 账号: 【{account_name}】"
                        f" | 删除成功: 【{deleted - before_deleted}】 | 删除失败: 【{failed - before_failed}】"
                    )
                except Exception as exc:
                    account_errors += 1
                    logger.error(
                        f"[历史清理/账号异常] 身份获取或回复处理失败，请检查账号会话及接口数据"
                        f" | 账号目录: 【{browser_session_dir}】 | 异常: 【{exc!r}】 | 结果: 【继续其他账号】"
                    )
        except Exception as exc:
            logger.error(
                f"[历史清理/本轮异常] 清理任务中断，请检查配置和接口数据"
                f" | 异常: 【{exc!r}】 | 结果: 【本轮停止，下轮继续】"
            )
        logger.info(
            f"[历史清理/本轮小结] 本轮处理结束 | 删除成功: 【{deleted}】 | 删除失败: 【{failed}】"
            f" | 账号异常: 【{account_errors}】 | 耗时: 【{time.monotonic() - started:.2f} 秒】"
            f" | 休眠: 【{DELETE_INTERVAL_SEC} 秒】"
        )
        time.sleep(DELETE_INTERVAL_SEC)


def verify_promo_comments_task():
    """每五分钟验证发送至少十分钟的评论；查询失败保留状态，整轮异常留待下轮。
    输入帖子含 promo_comment_info.{status, verify_status, comment_time, comment_id, send_count, history}；
    接口返回 [{reply_id, ...}]，覆盖 comments，并更新当前状态和最后一条同 ID 历史记录。
    """
    while True:
        started = time.monotonic()
        survived_count = missing_count = query_errors = 0
        post_id = "未进入帖子处理"
        try:
            post_manager = UniversalPostManager(gen_db_object())
            posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
            for post in posts:
                post_id = post.get("post_id", "缺失")
                promo_info = post.get("promo_comment_info")
                if not isinstance(promo_info, dict) or promo_info.get("status") != "success":
                    continue
                # : 验证存在后不再复查；不再满足推广条件的帖子也不验证。
                if promo_info.get("verify_status") == "success":
                    continue
                comment_time = promo_info.get("comment_time", 0)
                if not comment_time or time.time() * 1000 - comment_time < VERIFY_MIN_AGE_MS:
                    continue
                if not is_valid_post_for_promo(post):
                    continue
                post_id = str(post.get("post_id", ""))
                if not post_id:
                    continue
                try:
                    comments = fetch_binance_replies(content_id=post_id, sort_by=1, required_count=100)
                except Exception as exc:
                    query_errors += 1
                    logger.error(
                        f"[验证/查询异常] 无法拉取最新评论，可能是网络或接口异常"
                        f" | 帖子: 【{post_id}】 | 异常: 【{exc!r}】 | 结果: 【保留原状态并继续其他帖子】"
                    )
                    continue
                if not isinstance(comments, list):
                    query_errors += 1
                    logger.warning(
                        f"[验证/数据异常] 评论响应不是列表，可能是接口返回格式变化"
                        f" | 帖子: 【{post_id}】 | 返回类型: 【{type(comments).__name__}】 | 结果: 【保留原状态】"
                    )
                    continue
                post["comments"] = comments
                comment_id = str(promo_info.get("comment_id", ""))
                # : 仅在返回样本内匹配；未找到不等于已删除，空/None ID 仍可能误匹配。
                survived = any(str(item.get("reply_id", "")) == comment_id for item in comments)
                verification = {
                    "verify_time": int(time.time() * 1000),
                    "verify_status": "success" if survived else "failed",
                }
                promo_info.update(verification)
                history = promo_info.get("history")
                if isinstance(history, list):
                    for record in reversed(history):
                        if str(record.get("comment_id", "")) == comment_id:
                            record.update(verification)
                            break
                if survived:
                    survived_count += 1
                    action = "本次找到评论，后续不再验证"
                else:
                    missing_count += 1
                    retry_allowed = promo_info.get("send_count", 0) < MAX_SUCCESSFUL_SENDS
                    if retry_allowed:
                        post["promo_comment"] = None
                    action = "未找到，清空文案等待重新生成" if retry_allowed else "未找到，成功发送次数已达上限"
                    action += "；评论也可能未进入返回列表"
                post_manager.upsert_posts([post])
                log = logger.info if survived else logger.warning
                log(
                    f"[验证/回写] {action} | 帖子: 【{post_id}】 | 评论: 【{comment_id}】"
                    f" | 样本数: 【{len(comments)}】 | 状态: 【{promo_info['verify_status']}，已入库】"
                )
        except Exception as exc:
            logger.error(
                f"[验证/本轮异常] 验证或回写中断，请检查数据库连接和字段格式"
                f" | 最近处理帖子: 【{post_id}】 | 异常: 【{exc!r}】 | 结果: 【本轮停止，下轮继续】"
            )
        logger.info(
            f"[验证/本轮小结] 本轮处理结束 | 找到: 【{survived_count}】 | 未找到: 【{missing_count}】"
            f" | 查询异常: 【{query_errors}】 | 耗时: 【{time.monotonic() - started:.2f} 秒】"
            f" | 休眠: 【{VERIFY_INTERVAL_SEC} 秒】"
        )
        time.sleep(VERIFY_INTERVAL_SEC)


def calculate_comment_survival_rate(days=1):
    """按滚动时间窗统计并汇总日志，返回 {账号名: {attempts, success, survived, failed}}。
    数据来自 promo_comment_info.history=[{account_name, comment_time, status, verify_status}]；
    failed 指发送成功后验证未找到，success-survived-failed 为待验证数。
    """
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    cutoff_ms = int((time.time() - days * 24 * 3600) * 1000)
    account_stats = {}
    for post in posts:
        promo_info = post.get("promo_comment_info")
        if not isinstance(promo_info, dict):
            continue
        history = promo_info.get("history")
        # : 非列表 history 才回退当前记录；空列表不回退，回退用 >，历史用 >=。
        # 该分支仍参与现有统计，不能作为废弃兼容删除；无历史的异常/安全验证请求也不计入。
        if not isinstance(history, list):
            if not promo_info.get("comment_time", 0) > cutoff_ms:
                continue
            history = [promo_info]
        for record in history:
            if record.get("comment_time", 0) < cutoff_ms:
                continue
            account_name = record.get("account_name", "unknown")
            stats = account_stats.setdefault(account_name, COMMENT_STATS_DEFAULTS.copy())
            stats["attempts"] += 1
            if record.get("status") != "success":
                continue
            stats["success"] += 1
            verify_status = record.get("verify_status")
            if verify_status == "success":
                stats["survived"] += 1
            elif verify_status == "failed":
                stats["failed"] += 1

    def format_rate(numerator, denominator):
        """以统一精度显示比例，空样本保持零值。"""
        return f"{numerator / denominator * 100:.2f}%" if denominator > 0 else "0.00%"

    totals = {field: sum(stats[field] for stats in account_stats.values()) for field in COMMENT_STATS_DEFAULTS}
    scopes = [("总计", totals)] + [(f"账号 {name}", stats) for name, stats in account_stats.items()]
    for scope, stats in scopes:
        attempts, success = stats["attempts"], stats["success"]
        survived, failed = stats["survived"], stats["failed"]
        pending = success - survived - failed
        logger.info(
            f"[统计/评论存活] 统计窗口内的发送记录 | 范围: 【过去 {days} 天】 | 维度: 【{scope}】"
            f" | 尝试: 【{attempts}】 | 接口成功: 【{success}，占尝试 {format_rate(success, attempts)}】"
            f" | 验证存在: 【{survived}，占成功 {format_rate(survived, success)}】"
            f" | 验证未找到: 【{failed}，占成功 {format_rate(failed, success)}】"
            f" | 待验证: 【{pending}，占成功 {format_rate(pending, success)}】"
        )
    return account_stats


def _run_task(task):
    """为未处理异常补充任务入口信息后重抛，保留当前线程退出、不自动重启的行为。"""
    try:
        task()
    except Exception as exc:
        logger.error(
            f"[任务/退出] 任务异常退出，请检查对应链路的数据、文件权限和外部服务"
            f" | 任务: 【{task.__name__}】 | 异常: 【{exc!r}】 | 结果: 【当前线程停止】"
        )
        raise


if __name__ == "__main__":
    tasks = (send_promo_comments, gen_all_promo_comments, delete_old_replay, verify_promo_comments_task)
    threads = []
    for task in tasks:
        thread = threading.Thread(target=_run_task, args=(task,), name=task.__name__)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()