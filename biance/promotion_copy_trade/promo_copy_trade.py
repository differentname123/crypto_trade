# -- coding: utf-8 --
# ==========================================
# [功能摘要]: 批量处理币安广场帖子，利用大模型(Gemini)自动生成并结构化“带单推广”视角的营销评论。
# [输入数据]: 来源于 MongoDB 的币安广场原始帖子数据 (包含作者、正文、评论及互动数据)。
# [数据流转/交互]:
#   1. 从 DB 批量拉取未处理的原始帖子。
#   2. 清洗帖子多媒体标记，按 点赞/回复/浏览 降序提取 Top5 评论，压缩为精简文本。
#   3. 结合本地 Prompt 文件，请求 Gemini 大模型生成双视角(交易员/跟单员)推广评论。
#   4. 严格校验大模型返回的 JSON 结构。
# [输出数据]: 将合规的推广评论数据 (promo_comment) 追加到原帖子字典中，并回写更新至 MongoDB。
# ==========================================

import re
import time

from app.ai_api.gemini_api import get_llm_content
from app.ai_api.gemini_web import generate_gemini_content_managed
from common.common_utils import read_file_to_str, string_to_object, setup_logger
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager

logger = setup_logger(app_name="promo_copy")

def format_post_for_promo(raw_data):
    """
    清洗原始 JSON 数据，提取模型推广所需的核心字段。
    [入参 Shape]: raw_data (dict) 包含 "author", "content" (text_content, mentioned_coins), "comments" 等。
    [出参 Shape]: dict 包含 "post" (author, text, coins) 和 "top_comments" (list of strings)。
    """
    author_name = raw_data.get("author", {}).get("author_name", "未知用户")
    content_info = raw_data.get("content", {})
    raw_text = content_info.get("text_content", "")
    mentioned_coins = content_info.get("mentioned_coins", [])

    # 清洗多媒体标记并压缩多余换行
    clean_text = re.sub(r'\[(?:长文封面|插图|视频封面|视频):.*?\]', '', raw_text)
    clean_text = re.sub(r'\n{3,}', '\n\n', clean_text).strip()

    # 提取评论并依据核心指标进行综合热度降序
    comments = raw_data.get("comments", [])
    sorted_comments = sorted(
        comments,
        key=lambda x: (x.get("likes", 0), x.get("replies", 0), x.get("views", 0)),
        reverse=True
    )

    top_comments = []
    for comment in sorted_comments[:5]:
        c_content = comment.get("content", "").strip()
        if c_content:
            top_comments.append(c_content)

    return {
        "post": {
            "author": author_name,
            "text": clean_text,
            "coins": mentioned_coins
        },
        "top_comments": top_comments
    }

def check_comment_info(data):
    """
    校验模型返回的字典数据是否符合验收标准。
    [入参 Shape]: data (dict) 需包含 "trader_perspective" 和 "follower_perspective"，
                 内部包含 comment_text, link_text, combined_preview, score, score_reason。
    """
    if not isinstance(data, dict):
        return False, "大模型返回数据不是有效的字典对象"

    required_perspectives = ["trader_perspective", "follower_perspective"]
    required_fields = ["comment_text", "link_text", "combined_preview", "score", "score_reason"]
    forbidden_endings = ("。", "！", "？", ".", "!", "?")

    for perspective in required_perspectives:
        if perspective not in data:
            return False, f"缺失顶层角色字段: {perspective}"

        view_data = data[perspective]
        if not isinstance(view_data, dict):
            return False, f"{perspective} 必须是字典结构"

        for field in required_fields:
            if field not in view_data:
                return False, f"{perspective} 缺失必要字段: {field}"

        comment_text = view_data["comment_text"]
        link_text = view_data["link_text"]
        score = view_data["score"]

        # 核心业务潜规则拦截：断句格式及长度强校验
        if not isinstance(comment_text, str):
            return False, f"{perspective}.comment_text 类型非字符串"
        if comment_text.endswith(forbidden_endings):
            return False, f"{perspective}.comment_text 违规：绝对禁止以终止性标点结尾"

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
    调度大模型为单个帖子生成推广评论，附带重试与兜底机制。
    [入参 Shape]: post (dict) 原始帖子数据。
    [出参 Shape]: dict 生成的合法评论结构（失败则返回空字典 {}）。
    """
    cleaned_post = format_post_for_promo(post)

    prompt_file_path = r'W:\project\python_project\crypto_trade\prompt\带单推广评论生成.txt'
    prompt = read_file_to_str(prompt_file_path)
    full_prompt = f'{prompt}\n{cleaned_post}'

    model_name = "gemini-flash-latest"
    max_retries = 3

    for attempt in range(1, max_retries + 1):
        try:

            # err, raw_response, images = generate_gemini_content_managed(
            #     prompt=prompt,
            #     model_name="gemini-3-flash-thinking",
            #     # files=test_file
            # )



            raw_response = get_llm_content(prompt=full_prompt, model_name=model_name)
            comment_info = string_to_object(raw_response)

            is_valid, error_message = check_comment_info(comment_info)
            if not is_valid:
                raise ValueError(f"校验不通过: {error_message}")

            return comment_info

        except Exception as e:
            if attempt == max_retries:
                logger.error(f"[大模型/生成评论] 达到最大重试次数，当前帖子生成流程中止 | 关键参数: 【尝试次数: {attempt}/{max_retries}】 | 结果: {str(e)}")
                return {}

            logger.warning(f"[大模型/生成评论] 生成或校验异常，准备休眠重试 | 关键参数: 【尝试次数: {attempt}/{max_retries}】 | 结果: {str(e)}")
            time.sleep(2 ** attempt)

    return {}

def gen_all_promo_comments():
    """
    全局调度入口：拉取存量数据并循环驱动评论生成任务。
    """
    post_manager = UniversalPostManager(gen_db_object())
    existing_posts = post_manager.find_posts_by_source("biance", limit=50000)

    logger.info(f"[DB/启动任务] 成功获取币安广场待处理帖子 | 关键参数: 【拉取数量: {len(existing_posts)}】 | 结果: 开始逐条处理")

    for post in existing_posts:
        post_id = post.get("_id", "UNKNOWN_ID")

        # 幂等校验：拦截已生成过的数据
        if post.get("promo_comment"):
            logger.info(f"[数据过滤/帖子检测] 该帖子已存在推广评论 | 关键参数: 【帖子ID: {post_id}】 | 结果: 跳过处理")
            continue

        comment_info = gen_promo_comment(post)

        if comment_info:
            post["promo_comment"] = comment_info
            post_manager.upsert_posts([post])
            logger.info(f"[DB/帖子落库] 推广评论生成并回写成功 | 关键参数: 【帖子ID: {post_id}】 | 结果: 已更新入库")
        else:
            logger.warning(f"[业务跳过/帖子落库] 最终未能生成有效评论 | 关键参数: 【帖子ID: {post_id}】 | 结果: 放弃更新当前帖子")


def get_existing_promo_comments(limit=50000):
    """
    提取数据库中已经成功生成了推广评论的帖子集合，并将其原文与生成的评论聚合输出。

    入参限制:
      limit: 查询数据库的帖子上限阈值。

    出参形貌 (List 中每个元素的字典结构):
      [
        {
          "cleaned_post": {"post": {"author": "...", "text": "...", "coins": [...]}, "top_comments": [...]},
          "comment_info": {"trader_perspective": {...}, "follower_perspective": {...}}
        },
        ...
      ]
    """
    post_manager = UniversalPostManager(gen_db_object())

    try:
        existing_posts = post_manager.find_posts_by_source("biance", limit=limit)
    except Exception as e:
        logger.error(f"[数据提取/失败] 无法从数据库读取帖子数据 | 异常原因: {str(e)}")
        raise

    result_list = []

    for post in existing_posts:
        comment_info = post.get("promo_comment")

        # 卫语句：跳过尚未生成推广评论的脏数据
        if not comment_info:
            continue

        # 复用已有的清洗函数获取格式化后的干净帖子内容
        cleaned_post = format_post_for_promo(post)

        result_list.append({
            "cleaned_post": cleaned_post,
            "comment_info": comment_info
        })

    logger.info(f"[数据提取/完成] 成功拉取已聚合推广评论的帖子 | 聚合总数量: [{len(result_list)}] 条")

    return result_list


# ==========================================
# 💡 测试运行代码
# ==========================================
if __name__ == "__main__":
    gen_all_promo_comments()

    data = get_existing_promo_comments()
    print()