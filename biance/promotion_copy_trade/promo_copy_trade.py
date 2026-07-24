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

import re
import time
import threading

from app.ai_api.gemini_web import generate_gemini_content_managed
from biance.biance_playwright import comment_on_binance_post
from biance.biance_squre_api import fetch_binance_feed
from common.common_utils import read_file_to_str, string_to_object, setup_logger
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager

logger = setup_logger(app_name="promo_copy")

# ---------------- 全局配置常量（集中管理硬编码，便于维护）----------------
BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
SCHEDULE_INTERVAL_SEC = 3600
LLM_MAX_RETRIES = 3

GEMINI_MODEL = "gemini-3-flash-thinking"

FEED_TOKENS = ["BTC", "ETH", "BNB", "SOL", "XRP", "DOGE"]
PROMPT_FILE_PATH = r'W:\project\python_project\crypto_trade\prompt\带单推广评论生成.txt'
USER_DATA_DIR = r"W:\temp\biance_jie"
LEAD_DETAIL_URL = "https://www.binance.com/zh-CN/copy-trading/lead-details/5123703650401459968?timeRange=7D"

FILTER_CONFIG = {
    # 1. 黑名单机制
    "blacklist_keywords": [
        '瓜分', '抽奖', '红包', '空投', '新粉福利', '转发', '留下你的',
        'giveaway', 'prize pool', 'airdrop', 'split'
    ],
    "blacklist_multi_words": ['follow', 'share', 'comment'],  # 三词同时出现才触发拦截

    # 2. 结构与时效底线
    "min_text_length": 20,   # 去链接/标签后的纯文本最短字符数
    "max_age_hours": 720,    # 帖子最长有效时间(小时)

    # 3. 绝对值水位线防线
    "max_comment_count": 100,   # 评论数上限(过高说明太拥挤,推广无曝光)
    "cold_post_hours": 20,      # 判定"死帖"的时间界限(小时)
    "cold_post_min_views": 20   # 死帖最低浏览量要求(超时且低于此值即淘汰)
}


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
    if age_hours > FILTER_CONFIG["cold_post_hours"] and engagement.get("view_count", 0) < FILTER_CONFIG["cold_post_min_views"]:
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
            err, raw_response, _images = generate_gemini_content_managed(
                prompt=full_prompt,
                model_name=GEMINI_MODEL
            )
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

            # 幂等：已有推广评论直接跳过
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


def get_existing_promo_comments(limit=POST_QUERY_LIMIT):
    """
    导出已生成推广评论的合规帖子，聚合"清洗后原文 + 评论结果"供离线分析。
    [入参 Shape]: limit(int) DB 查询上限。
    [出参 Shape]: [{"cleaned_post": format_post_for_promo 结构, "comment_info": promo_comment 结构}, ...]。
    """
    post_manager = UniversalPostManager(gen_db_object())

    try:
        existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=limit)
    except Exception as e:
        logger.error(f"[数据导出/失败] 无法从数据库读取帖子，可能是 DB 连接异常 | 结果: 【失败原因: {e}】")
        raise

    result_list = []
    for post in existing_posts:
        comment_info = post.get("promo_comment")
        # 卫语句：跳过尚未生成评论 / 已不符合推广条件的脏数据
        if not comment_info or not is_valid_post_for_promo(post):
            continue
        result_list.append({
            "cleaned_post": format_post_for_promo(post),
            "comment_info": comment_info
        })

    logger.info(f"[数据导出/完成] 聚合已生成评论的合规帖子 | 结果: 【聚合总数: {len(result_list)} 条】")
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
    为单帖组装引流参数并调用外部接口发帖，无论成败都把发布状态闭环写回 post。
    [入参 Shape]: post(dict) 必须含 post_id 与 promo_comment.trader_perspective.(comment_text, link_text)。
    [出参 Shape]: 触发过发帖动作则返回注入了 promo_comment_info 的 post；因不符合条件被跳过则返回 None。
    """
    # 卫语句：无评论 / 已处理过 直接跳过
    comment_info = post.get("promo_comment")
    if not comment_info or "promo_comment_info" in post:
        return None

    post_id = post.get("post_id")
    if not post_id:
        return None  # 防御：缺 post_id 会拼出残缺 URL，直接拦截

    trader_perspective = comment_info.get("trader_perspective", {})
    comment_text = trader_perspective.get("comment_text")
    link_text = trader_perspective.get("link_text")

    post_url = f"https://www.binance.com/zh-CN/square/post/{post_id}"
    my_urls = [{"text": link_text, "url": LEAD_DETAIL_URL}]

    err, success, c_id = comment_on_binance_post(
        post_url=post_url,
        comment=comment_text,
        url_info_list=my_urls,
        user_data_dir=USER_DATA_DIR
    )

    # 无论成败，统一把发布结果闭环回写
    post["promo_comment_info"] = {
        "comment_id": c_id,
        "comment_time": int(time.time() * 1000),
        "status": "success" if success else "failed",
        "error_info": err if not success else None
    }

    if success:
        logger.info(f"[发布链路/发帖] 推广评论发布成功 | 关键参数: 【帖子ID: {post_id}】 | 结果: 【评论ID: {c_id}】")
    else:
        logger.error(
            f"[发布链路/发帖] 发帖失败，可能是网络波动或触发账号风控 "
            f"| 关键参数: 【帖子ID: {post_id}】 | 结果: 【已记录失败 | 错误详情: {err}】")

    return post


def send_promo_comments():
    """
    发布链路总入口：周期性拉取带评论的帖子并逐条发布，回写发布状态。
    【无出入参】，直接产生副作用：读写 MongoDB 与调用外部发帖接口。
    """
    while True:
        post_manager = UniversalPostManager(gen_db_object())
        existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
        logger.info(
            f"[发布链路/启动] 拉取待发布帖子完毕 | 关键参数: 【总量: {len(existing_posts)}】 | 结果: 【开始逐条发布】")

        sent = skipped = 0
        for post in existing_posts:
            if not is_valid_post_for_promo(post):
                skipped += 1
                continue

            post_result = send_single_promo_comment(post)
            if post_result:
                post_manager.upsert_posts([post_result])
                sent += 1
            else:
                skipped += 1

        logger.info(
            f"[发布链路/本轮小结] 发布完毕，进入休眠 "
            f"| 关键参数: 【已发布: {sent} | 跳过: {skipped}】 | 结果: 【休眠 {SCHEDULE_INTERVAL_SEC} 秒】")
        time.sleep(SCHEDULE_INTERVAL_SEC)


# ==========================================
# 运行入口：生成链路与发布链路各起一个守护线程并行运行
# ==========================================
if __name__ == "__main__":
    tasks = [
        send_promo_comments,
        gen_all_promo_comments,  # 注释本行即可停用"评论生成"链路
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