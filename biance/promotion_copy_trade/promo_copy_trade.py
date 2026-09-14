# [功能摘要] 采集币安广场帖子，生成并发布推广评论，定期验证评论和清理历史回复。
# [输入数据] 推荐流及 MongoDB 帖子字典：post_id、author、content、comments、engagement、
#            metadata、publish_time；另读取提示词文件、浏览器账号目录及账号计数 JSON。
# [数据流转/交互] 推荐流 -> MongoDB -> 黑名单/时效/互动过滤 -> 正文与热评上下文 -> Gemini
#                -> 双视角校验 -> promo_comment；发布 follower_perspective 后更新账号计数
#                和 promo_comment_info；验证时覆盖 comments，未找到且成功发送不足 3 次则清空文案。
#                历史清理另按账号获取身份，以 7 天前的时间偏移查询并删除接口返回的回复。
# [输出数据] MongoDB 中的文案、发布/验证状态和最新评论；账号计数 JSON；平台发帖/删除副作用及日志。
# : 生成、发布、验证均整帖 upsert，可能覆盖其他线程的新状态；确认数据库局部更新接口后再处理。

import datetime
import json
import ntpath
import os
import re
import tempfile
import threading
import time

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
LLM_MAX_RETRIES = 3
GEMINI_MODEL = "gemini-3.7-flash"
MAX_SUCCESSFUL_SENDS = 3
MAX_REPLAY_DAYS = 7
# : 原注释写“每天”，实际每 3600 秒执行；保留实际的一小时间隔。
DELETE_INTERVAL_SEC = 3600
VERIFY_INTERVAL_SEC = 300
VERIFY_MIN_AGE_MS = 10 * 60 * 1000

FEED_TOKENS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE"]
PROMPT_FILE_PATH = r"W:\project\python_project\crypto_trade\prompt\带单推广评论生成.txt"
USER_DATA_DIR_LIST = [r"W:\temp\biance_qiqi", r"W:\temp\biance_zhouling"]
DELETE_USER_DATA_DIR_LIST = [
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
USER_ACCOUNT_USAGE_FILE = r"W:\project\python_project\crypto_trade\biance\promotion_copy_trade\biance_account_usage.json"
LEAD_DETAIL_URL = "https://www.binance.com/zh-CN/square/post/362858558969979"

FILTER_CONFIG = {
    "blacklist_keywords": [
        "瓜分", "抽奖", "红包", "空投", "新粉福利", "转发", "留下你的",
        "giveaway", "prize pool", "airdrop", "split",
    ],
    "blacklist_multi_words": ["follow", "share", "comment"],
    "min_text_length": 20,
    "max_age_hours": 720,
    "max_comment_count": 100,
    "cold_post_hours": 20,
    "cold_post_min_views": 20,
}
ACCOUNT_USAGE_DEFAULTS = {
    "total_count": 0,
    "success_count": 0,
    "failure_count": 0,
    "last_failure_reason": None,
    "last_send_time": 0,
    "update_time": None,
}
# : 沿用大小写敏感的子串匹配；任何包含“405”的错误都触发暂停，可能误判。
CAPTCHA_SIGNALS = (
    "我们需要确认您是人类", "Human Verification", "安全检查",
    "405", "geetest", "cf-turnstile",
)
# : 锁仅保护本进程的账号计数；同名目录共用记录，发布与清理仍可能并用同一浏览器目录。
_user_account_usage_lock = threading.Lock()


def _get_account_name(user_data_dir):
    """按路径末级目录识别账号，Windows 路径在其他系统上也能正确解析。"""
    return ntpath.basename(ntpath.normpath(user_data_dir))


def _save_user_account_usage_unlocked(usage_data):
    """同盘临时写入后原子替换，异常时释放文件并清理临时目录；调用方须持锁。
    入参：{账号名: {total_count, success_count, failure_count, last_send_time,
           last_failure_reason, update_time}}；无返回，保留所有附加字段。
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
    """读取并补齐已配置账号的计数字段，损坏文件报错而不重置；调用方须持锁。
    返回：{账号名: {total_count, success_count, failure_count, last_send_time,
           last_failure_reason, update_time}}，保留未配置账号及附加字段。
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
                f"读取账号计数失败，请检查文件权限或 JSON 格式: {USER_ACCOUNT_USAGE_FILE} | {exc}"
            ) from exc
        if not isinstance(usage_data, dict):
            raise ValueError(f"账号计数 JSON 顶层必须是字典: {USER_ACCOUNT_USAGE_FILE}")

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
                account_usage[field] = value
                changed = True

    if changed:
        _save_user_account_usage_unlocked(usage_data)
    return usage_data


def acquire_user_account_for_send():
    """在冷却已结束的账号中按占用次数、配置顺序选择，持锁预占后返回。
    返回：(user_data_dir, account_name)；全部冷却时在锁外等待。
    """
    while True:
        with _user_account_usage_lock:
            usage_data = _load_user_account_usage_unlocked()
            now = time.time()
            available, cooldowns = [], []
            for index, user_data_dir in enumerate(USER_DATA_DIR_LIST):
                account_name = _get_account_name(user_data_dir)
                account_usage = usage_data[account_name]
                total_count = int(account_usage.get("total_count", 0) or 0)
                last_send_time = float(account_usage.get("last_send_time", 0) or 0)
                elapsed = now - last_send_time
                if last_send_time <= 0 or elapsed > COMMENT_SEND_INTERVAL_SEC:
                    available.append((total_count, index, user_data_dir, account_name))
                    continue
                cooldowns.append(COMMENT_SEND_INTERVAL_SEC - elapsed)

            if available:
                total_count, _, user_data_dir, account_name = min(available)
                # : total_count 与冷却时间在请求前预占；请求失败或进程退出也不回滚。
                usage_data[account_name].update({
                    "total_count": total_count + 1,
                    "last_send_time": time.time(),
                    "update_time": time.strftime("%Y-%m-%d %H:%M:%S"),
                })
                _save_user_account_usage_unlocked(usage_data)
                return user_data_dir, account_name

            wait_seconds = max(min(cooldowns), 0) + 0.05
        logger.info(
            f"[发布/账号冷却] 暂无可用账号 | 最小间隔: 【{COMMENT_SEND_INTERVAL_SEC} 秒】"
            f" | 结果: 【等待 {wait_seconds:.2f} 秒后重选】"
        )
        time.sleep(wait_seconds)


def record_user_account_send_result(account_name, success, error_info=None):
    """累加实际请求的成功或失败次数；error_info 可为错误文本或异常对象。"""
    with _user_account_usage_lock:
        usage_data = _load_user_account_usage_unlocked()
        account_usage = usage_data.setdefault(account_name, ACCOUNT_USAGE_DEFAULTS.copy())
        count_field = "success_count" if success else "failure_count"
        account_usage[count_field] = int(account_usage.get(count_field, 0) or 0) + 1
        # : 成功后仍保留历史 last_failure_reason，它不代表最近一次请求结果。
        if not success:
            account_usage["last_failure_reason"] = str(error_info or "未知失败原因")
        account_usage["update_time"] = time.strftime("%Y-%m-%d %H:%M:%S")
        _save_user_account_usage_unlocked(usage_data)


def is_valid_post_for_promo(post):
    """依次执行黑名单、正文/时效、互动水位过滤，返回 bool。
    入参：帖子字典；content.text_content、metadata.is_ai_generated、
    engagement.{comment_count, view_count}、publish_time（秒或毫秒数值）。
    """
    text = (post.get("content", {}).get("text_content") or "").lower()
    engagement = post.get("engagement", {})
    metadata = post.get("metadata", {})
    if any(word in text for word in FILTER_CONFIG["blacklist_keywords"]):
        return False
    if all(word in text for word in FILTER_CONFIG["blacklist_multi_words"]):
        return False

    clean_text = re.sub(r"#\S+", "", re.sub(r"http[s]?://\S+", "", text)).strip()
    if len(clean_text) < FILTER_CONFIG["min_text_length"]:
        return False
    if metadata.get("is_ai_generated") is True:
        return False

    publish_time = post.get("publish_time", 0)
    if publish_time > 1e11:
        publish_time /= 1000
    # : 未来时间戳没有单独拦截，仍按原规则参与后续过滤。
    age_hours = (time.time() - publish_time) / 3600
    if age_hours > FILTER_CONFIG["max_age_hours"]:
        return False
    if engagement.get("comment_count", 0) > FILTER_CONFIG["max_comment_count"]:
        return False
    if (age_hours > FILTER_CONFIG["cold_post_hours"]
            and engagement.get("view_count", 0) < FILTER_CONFIG["cold_post_min_views"]):
        return False
    return True


def format_post_for_promo(raw_data):
    """清洗正文并提取热评，保留模型输入的原有形状。
    入参：author.author_name、content.{text_content, mentioned_coins}、
    comments 列表，每项含 content/likes/replies/views。
    返回：{post: {author, text, coins}, top_comments: [评论文本]}。
    """
    content = raw_data.get("content", {})
    text = re.sub(r"\[(?:长文封面|插图|视频封面|视频):.*?\]", "", content.get("text_content", ""))
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    comments = sorted(
        raw_data.get("comments", []),
        key=lambda item: (item.get("likes", 0), item.get("replies", 0), item.get("views", 0)),
        reverse=True,
    )
    # : 先截取热度前五再去空，可能不足五条；不得改成先去空再取五条。
    top_comments = [item.get("content", "").strip() for item in comments[:5]]
    return {
        "post": {
            "author": raw_data.get("author", {}).get("author_name", "未知用户"),
            "text": text,
            "coins": content.get("mentioned_coins", []),
        },
        "top_comments": [text for text in top_comments if text],
    }


def check_comment_info(data):
    """按原有字段、标点、长度和评分规则校验，返回 (是否有效, 错误说明)。
    入参：trader_perspective/follower_perspective 字典，各含
    comment_text、link_text、combined_preview、score、score_reason。
    """
    if not isinstance(data, dict):
        return False, "大模型返回数据不是有效的字典对象"
    required_fields = ("comment_text", "link_text", "combined_preview", "score", "score_reason")
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
        # : 保留空正文及尾部空白的原判定；预览和评分理由仅检查字段存在。
        if not isinstance(comment_text, str):
            return False, f"{perspective}.comment_text 类型非字符串"
        if comment_text.endswith(("。", "！", "？", ".", "!", "?")):
            return False, f"{perspective}.comment_text 违规：绝对禁止以终止性标点结尾"
        if not isinstance(link_text, str):
            return False, f"{perspective}.link_text 类型非字符串"
        # : 原实现只检查字符数，不验证是否为汉字。
        if not 2 <= len(link_text) <= 6:
            return False, f"{perspective}.link_text 违规：引流文案长度必须严格在 2-6 个汉字之间"
        # : bool 是 int 的子类，原实现接受布尔评分；不擅自收紧。
        if not isinstance(score, (int, float)):
            return False, f"{perspective}.score 类型非数字"
        if not 0 <= score <= 10:
            return False, f"{perspective}.score 违规：评分必须介于 0-10 之间"
    return True, ""


def gen_promo_comment(post):
    """请求模型并校验双视角结果；重试耗尽返回空字典，保留原有降级边界。
    入参：format_post_for_promo 所述帖子；返回 check_comment_info 所述字典或 {}。
    提示词文件读取及正文清洗异常仍直接向外传播。
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
            # : 沿用“响应通过校验即成功”，不以外部 error_detail 单独判失败。
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
    """按原顺序采集各币种及综合推荐流，合并后一次入库。
    入参：提供 upsert_posts([帖子字典]) 的数据库管理器；无返回。
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
    """常驻生成任务：采集、筛选、生成并回写 promo_comment，整轮完成后休眠。"""
    post_manager = UniversalPostManager(gen_db_object())
    while True:
        started = time.monotonic()
        fetch_post(post_manager)
        posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
        logger.info(f"[生成/开始] 准备筛选帖子 | 帖子数: 【{len(posts)}】")
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
            logger.info(
                f"[生成/回写] 双视角文案已入库 | 帖子: 【{post.get('post_id', post.get('_id'))}】"
            )
        logger.info(
            f"[生成/本轮小结] 本轮处理完成 | 生成: 【{generated}】 | 失败: 【{failed}】"
            f" | 已有文案: 【{existing}】 | 过滤: 【{invalid}】"
            f" | 耗时: 【{time.monotonic() - started:.2f} 秒】 | 休眠: 【{SCHEDULE_INTERVAL_SEC} 秒】"
        )
        time.sleep(SCHEDULE_INTERVAL_SEC)


def get_existing_promo_comments(limit=POST_QUERY_LIMIT, hours_ago=12):
    """导出最近更新且仍符合规则的文案，供离线分析。
    数据库字段：db_update_time（无时区 datetime）、promo_comment 及原帖字段。
    返回：[{cleaned_post: {post, top_comments}, comment_info: 双视角字典}]。
    """
    post_manager = UniversalPostManager(gen_db_object())
    # : 沿用本地无时区时间比较；跨时区数据需先明确数据库时间约定。
    threshold = datetime.datetime.now() - datetime.timedelta(hours=hours_ago)
    try:
        posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=limit)
    except Exception as exc:
        logger.error(
            f"[导出/读取失败] 无法查询帖子，请检查数据库连接和访问权限 | 异常: 【{exc!r}】"
        )
        raise
    result = []
    for post in posts:
        updated_at = post.get("db_update_time")
        if not updated_at or updated_at < threshold:
            continue
        comment_info = post.get("promo_comment")
        if not comment_info or not is_valid_post_for_promo(post):
            continue
        result.append({"cleaned_post": format_post_for_promo(post), "comment_info": comment_info})
    logger.info(f"[导出/完成] 已聚合有效文案 | 时间范围: 【{hours_ago} 小时】 | 条数: 【{len(result)}】")
    return result


def clear_all_promo_comments_batch():
    """批量清空已存在的 promo_comment 字段，保留原有发布状态；无返回。"""
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    updates = [post for post in posts if "promo_comment" in post]
    # : 仅清空生成文案，不重置 promo_comment_info，不能视为允许重新发布。
    for post in updates:
        post["promo_comment"] = None
    if updates:
        post_manager.upsert_posts(updates)
    logger.info(f"[文案清理/完成] 已清空目标字段 | 扫描: 【{len(posts)}】 | 更新: 【{len(updates)}】")


def send_single_promo_comment(post):
    """按既有发送状态发布单帖，在内存中更新结果，数据库由调用方回写。
    入参：post_id、promo_comment.follower_perspective.{comment_text, link_text}，
    可含 promo_comment_info.{send_count, status, verify_status, history}。
    返回：(post, SUCCESS/FAILED) 或 (None, CAPTCHA/SKIPPED)。
    """
    comment_info = post.get("promo_comment")
    if not comment_info:
        return None, "SKIPPED"
    promo_info = post.get("promo_comment_info")
    if isinstance(promo_info, dict):
        # : 上限统计成功次数而非尝试次数；已有空字典、业务失败或待验证状态均不再发送。
        if promo_info.get("send_count", 0) >= MAX_SUCCESSFUL_SENDS:
            return None, "SKIPPED"
        if not (promo_info.get("status") == "success" and promo_info.get("verify_status") == "failed"):
            return None, "SKIPPED"
    post_id = post.get("post_id")
    if not post_id:
        return None, "SKIPPED"

    # : 双视角都校验，但只发布 follower；历史文案缺少正文或链接仍交由外部接口处理。
    follower = comment_info.get("follower_perspective", {})
    comment_text, link_text = follower.get("comment_text"), follower.get("link_text")
    user_data_dir, account_name = acquire_user_account_for_send()
    started = time.monotonic()
    try:
        error, success, comment_id = comment_on_binance_post(
            post_url=f"https://www.binance.com/zh-CN/square/post/{post_id}",
            comment=comment_text,
            url_info_list=[{"text": link_text, "url": LEAD_DETAIL_URL}],
            user_data_dir=user_data_dir,
        )
    except Exception as send_error:
        record_error = None
        try:
            record_user_account_send_result(account_name, success=False, error_info=send_error)
        except Exception as exc:
            record_error = exc
        logger.error(
            f"[发布/调用异常] 发帖请求异常，实际发布结果未知，请检查登录状态、网络和接口响应"
            f" | 帖子: 【{post_id}】 | 账号: 【{account_name}】 | 异常: 【{send_error!r}】"
            f" | 失败计数: 【{'已记录' if record_error is None else repr(record_error)}】"
            f" | 结果: 【保留原始发送异常并向外抛出】"
        )
        if record_error is not None:
            raise send_error from record_error
        raise

    error_text = str(error or "")
    is_captcha = any(signal in error_text for signal in CAPTCHA_SIGNALS)
    record_user_account_send_result(
        account_name,
        success=False if is_captcha else success,
        error_info=error_text if is_captcha else error,
    )
    if is_captcha:
        logger.error(
            f"[发布/安全验证] 接口提示人机验证或访问阻断，需要检查账号验证状态"
            f" | 帖子: 【{post_id}】 | 账号: 【{account_name}】 | 接口提示: 【{error_text}】"
            f" | 结果: 【停止本轮，保留帖子原状态；账号已计失败】"
        )
        return None, "CAPTCHA"

    if not isinstance(promo_info, dict):
        promo_info = {}
    if success:
        promo_info["send_count"] = promo_info.get("send_count", 0) + 1

    current_time_ms = int(time.time() * 1000)
    promo_info.update({
        "comment_id": comment_id,
        "comment_time": current_time_ms,
        "status": "success" if success else "failed",
        "error_info": None if success else error,
        "account_name": account_name,
        "verify_status": "pending",
        "verify_time": None,
    })

    # 追加发送历史记录以支持存活率验证不漏掉失败重试的数据
    history = promo_info.get("history")
    if not isinstance(history, list):
        history = []
    history.append({
        "account_name": account_name,
        "comment_id": comment_id,
        "comment_time": current_time_ms,
        "status": promo_info["status"],
        "error_info": promo_info["error_info"],
        "verify_status": promo_info["verify_status"],
        "verify_time": promo_info["verify_time"]
    })
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
    """常驻发布任务：合并成败回写路径，安全验证触发后停止本轮并按分钟报告冷却进度。"""
    while True:
        started = time.monotonic()
        post_manager = UniversalPostManager(gen_db_object())
        posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
        logger.info(f"[发布/开始] 准备筛选并发布 | 帖子数: 【{len(posts)}】")
        counts = {"SUCCESS": 0, "FAILED": 0, "SKIPPED": 0}
        hit_captcha = False
        for post in posts:
            if not is_valid_post_for_promo(post):
                counts["SKIPPED"] += 1
                continue
            post_result, status = send_single_promo_comment(post)
            if status == "CAPTCHA":
                hit_captcha = True
                break
            if status not in ("SUCCESS", "FAILED"):
                counts["SKIPPED"] += 1
                continue
            try:
                post_manager.upsert_posts([post_result])
            except Exception as exc:
                logger.error(
                    f"[发布/回写失败] 发帖接口已返回，但状态未能确认入库，请检查数据库并核对实际评论"
                    f" | 帖子: 【{post.get('post_id')}】 | 接口结果: 【{status}】 | 异常: 【{exc!r}】"
                )
                raise
            counts[status] += 1

        log = logger.warning if hit_captcha else logger.info
        outcome = "安全验证触发，本轮提前停止" if hit_captcha else "本轮处理完成"
        log(
            f"[发布/本轮小结] {outcome} | 成功: 【{counts['SUCCESS']}】"
            f" | 失败: 【{counts['FAILED']}】 | 跳过: 【{counts['SKIPPED']}】"
            f" | 耗时: 【{time.monotonic() - started:.2f} 秒】 | 休眠: 【{SCHEDULE_INTERVAL_SEC} 秒】"
        )
        if not hit_captcha:
            time.sleep(SCHEDULE_INTERVAL_SEC)
            continue
        remaining = SCHEDULE_INTERVAL_SEC
        while remaining > 0:
            sleep_step = min(60, remaining)
            time.sleep(sleep_step)
            remaining -= sleep_step
            if remaining > 0:
                logger.info(f"[发布/验证冷却] 安全验证触发后暂停中 | 距下轮检查: 【{remaining} 秒】")


def delete_old_replay():
    """按账号清理接口返回的历史回复，每小时执行；保留逐条、逐账号和整轮异常隔离。"""
    while True:
        started = time.monotonic()
        deleted = failed = account_errors = 0
        logger.info(f"[历史清理/开始] 准备查询历史回复 | 账号数: 【{len(DELETE_USER_DATA_DIR_LIST)}】")
        # 原有容错边界：清理异常只结束当前条目、账号或本轮，随后继续定时执行。
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
                    # : 沿用接口 time_offset 语义，最多拉 1000 条；不另验时间，也不限定推广回复。
                    replies = fetch_binance_square_replies(
                        target_square_uid=user_info.get("squareUid"),
                        cookies=cookies, csrf_token=token, limit=1000, time_offset=cutoff_ms,
                    )
                    if not replies:
                        continue
                    for item in replies:
                        reply_id = item.get("reply_id")
                        if not reply_id:
                            continue
                        try:
                            success = delete_binance_square_content(
                                content_id=reply_id, cookies=cookies, csrf_token=token
                            )
                        except Exception as exc:
                            failed += 1
                            logger.error(
                                f"[历史清理/请求异常] 删除回复时请求异常，可能是网络或身份凭证失效"
                                f" | 账号: 【{account_name}】 | 回复: 【{reply_id}】 | 异常: 【{exc!r}】"
                                f" | 结果: 【继续处理其余回复】"
                            )
                            continue
                        if success:
                            deleted += 1
                            logger.info(
                                f"[历史清理/删除成功] 接口已确认删除 | 账号: 【{account_name}】 | 回复: 【{reply_id}】"
                            )
                            continue
                        failed += 1
                        logger.warning(
                            f"[历史清理/删除失败] 接口未确认删除，可能是回复已不存在或权限不足"
                            f" | 账号: 【{account_name}】 | 回复: 【{reply_id}】 | 结果: 【继续处理】"
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
    """每五分钟核查发送至少十分钟、仍符合条件的评论，并回写最新评论和验证状态。
    帖子字段：post_id、promo_comment_info.{status, verify_status, comment_time,
    comment_id, send_count, history}；评论列表每项用 reply_id 匹配；无返回。
    """
    while True:
        started = time.monotonic()
        survived_count = missing_count = query_errors = 0
        # 原有容错边界：查询失败跳过单帖，其余异常结束本轮，下一轮继续。
        try:
            post_manager = UniversalPostManager(gen_db_object())
            posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
            for post in posts:
                promo_info = post.get("promo_comment_info")
                if not isinstance(promo_info, dict) or promo_info.get("status") != "success":
                    continue
                # : 验证成功后不再复查；验证失败的帖子在后续轮次仍可能被再次清空文案。
                if promo_info.get("verify_status") == "success":
                    continue
                comment_time = promo_info.get("comment_time", 0)
                if not comment_time or time.time() * 1000 - comment_time < VERIFY_MIN_AGE_MS:
                    continue
                if not is_valid_post_for_promo(post):
                    continue
                post_id = post.get("post_id")
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

                # : 未在返回的 100 条热评中找到就判失败，空列表亦如此，并不等于确认被删。
                post["comments"] = comments
                comment_id = str(promo_info.get("comment_id", ""))
                # : 沿用字符串比较，缺失 ID 或 None 仍可能互相匹配。
                survived = any(str(item.get("reply_id", "")) == comment_id for item in comments)
                verify_time_ms = int(time.time() * 1000)
                promo_info["verify_time"] = verify_time_ms
                promo_info["verify_status"] = "success" if survived else "failed"

                # 同步更新历史列表中的记录存活状态
                history = promo_info.get("history")
                if isinstance(history, list) and history:
                    for record in reversed(history):
                        if record.get("comment_id") == comment_id:
                            record["verify_status"] = promo_info["verify_status"]
                            record["verify_time"] = verify_time_ms
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
                f" | 异常: 【{exc!r}】 | 结果: 【本轮停止，下轮继续】"
            )
        logger.info(
            f"[验证/本轮小结] 本轮处理结束 | 找到: 【{survived_count}】 | 未找到: 【{missing_count}】"
            f" | 查询异常: 【{query_errors}】 | 耗时: 【{time.monotonic() - started:.2f} 秒】"
            f" | 休眠: 【{VERIFY_INTERVAL_SEC} 秒】"
        )
        time.sleep(VERIFY_INTERVAL_SEC)


def calculate_comment_survival_rate(days=1):
    """
    单独调用的统计函数：输出指定时间内（默认1天）的评论统计数据。
    支持总维度及 account_name 维度的统计输出，供运维人员分析当前风控、转化现状。
    """
    post_manager = UniversalPostManager(gen_db_object())
    posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    cutoff_ms = int((time.time() - days * 24 * 3600) * 1000)

    total_attempts = 0
    total_success = 0
    total_survived = 0
    account_stats = {}

    for post in posts:
        promo_info = post.get("promo_comment_info")
        if not isinstance(promo_info, dict):
            continue
        history = promo_info.get("history")

        # 兼容旧数据：如果没有history但包含了近期的发送记录，则伪造一条记录计入统计
        if not isinstance(history, list):
            if promo_info.get("comment_time", 0) > cutoff_ms:
                history = [{
                    "account_name": promo_info.get("account_name", "unknown"),
                    "comment_time": promo_info.get("comment_time", 0),
                    "status": promo_info.get("status"),
                    "verify_status": promo_info.get("verify_status")
                }]
            else:
                continue

        for record in history:
            if record.get("comment_time", 0) < cutoff_ms:
                continue

            acc = record.get("account_name", "unknown")
            if acc not in account_stats:
                account_stats[acc] = {"attempts": 0, "success": 0, "survived": 0}

            total_attempts += 1
            account_stats[acc]["attempts"] += 1

            if record.get("status") == "success":
                total_success += 1
                account_stats[acc]["success"] += 1
                if record.get("verify_status") == "success":
                    total_survived += 1
                    account_stats[acc]["survived"] += 1

    def format_rate(num, den):
        return f"{(num / den * 100):.2f}%" if den > 0 else "0.00%"

    logger.info(f"========== 评论存活率统计 (过去 {days} 天) ==========")
    logger.info(f"【总维度】")
    logger.info(f" - 评论的总尝试(帖子)数量: {total_attempts}")
    logger.info(f" - 收到comment_id的数量: {total_success} (发送成功率: {format_rate(total_success, total_attempts)})")
    logger.info(f" - 最终验证存在的数量: {total_survived} (存活成功率: {format_rate(total_survived, total_success)})")

    logger.info(f"【账号维度】")
    for acc, stats in account_stats.items():
        logger.info(f" - 账号 [{acc}]:")
        logger.info(f"    评论的总尝试数量: {stats['attempts']}")
        logger.info(
            f"    收到comment_id: {stats['success']} (发送成功率: {format_rate(stats['success'], stats['attempts'])})")
        logger.info(
            f"    最终验证存在: {stats['survived']} (存活成功率: {format_rate(stats['survived'], stats['success'])})")
    logger.info("=====================================================")


def _run_task(task):
    """为线程未处理异常补充业务入口信息，记录后继续抛出，不增加自动重启行为。"""
    try:
        task()
    except Exception as exc:
        logger.error(
            f"[任务/退出] 任务异常退出，请检查对应链路的数据、文件权限和外部服务"
            f" | 任务: 【{task.__name__}】 | 异常: 【{exc!r}】 | 结果: 【当前线程停止】"
        )
        raise


if __name__ == "__main__":
    # 如果需要单独统计存活率，可以取消注释执行下行代码
    calculate_comment_survival_rate(days=1)

    tasks = [send_promo_comments, gen_all_promo_comments, delete_old_replay, verify_promo_comments_task]
    threads = []
    for task in tasks:
        thread = threading.Thread(target=_run_task, args=(task,), name=task.__name__)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()