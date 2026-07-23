# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/7/22 7:46
:last_date:
    2026/7/22 7:46
:description:
    
"""
import re
import time

from app.ai_api.gemini_api import get_llm_content
from common.common_utils import read_file_to_str, string_to_object, setup_logger
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager
logger = setup_logger(app_name="promo_copy")


def format_post_for_promo(raw_data):
    """
    清洗币安广场原始 JSON 数据，提取大模型进行带单推广所需的核心字段。
    (全面剔除多媒体标记，综合 likes -> replies -> views 降序排序评论)
    """

    # 1. 提取作者和正文信息
    author_name = raw_data.get("author", {}).get("author_name", "未知用户")
    content_info = raw_data.get("content", {})
    raw_text = content_info.get("text_content", "")
    mentioned_coins = content_info.get("mentioned_coins", [])

    # 【核心修复点】: 使用正则精准匹配你在爬虫中注入的四种媒体标记并将其删除
    # (?:长文封面|插图|视频封面|视频) 是非捕获组，用于匹配前缀
    clean_text = re.sub(r'\[(?:长文封面|插图|视频封面|视频):.*?\]', '', raw_text)

    # 进一步清理：因为你的脚本是往尾部拼接 \n\n，删除标记后可能会留下多个空行
    # 这里将3个及以上的换行符压缩为最多2个，并去除首尾空格，保持文本整洁
    clean_text = re.sub(r'\n{3,}', '\n\n', clean_text).strip()

    # 2. 提取并筛选高质量评论
    comments = raw_data.get("comments", [])

    # 按照 likes(点赞) -> replies(回复) -> views(浏览) 综合降序排序
    sorted_comments = sorted(
        comments,
        key=lambda x: (x.get("likes", 0), x.get("replies", 0), x.get("views", 0)),
        reverse=True
    )

    # 提取前 5 条，并拼接为 "昵称: 内容" 的纯文本字符串格式
    top_comments = []
    for comment in sorted_comments[:5]:
        c_author = comment.get("author_name", "匿名")
        c_content = comment.get("content", "").strip()

        # 过滤掉空评论
        if c_content:
            top_comments.append(f"{c_content}")

    # 3. 组装并返回最终结构
    final_data = {
        "post": {
            "author": author_name,
            "text": clean_text,
            "coins": mentioned_coins
        },
        "top_comments": top_comments
    }

    return final_data


def check_comment_info(data):
    """
    校验生成的字典数据是否符合提示词的验收标准。

    返回两个字段:
    is_valid (bool): 是否通过校验
    error_message (str): 具体的错误信息（如果通过则为空字符串）
    """
    if not isinstance(data, dict):
        return False, "传入的参数必须是一个字典格式"

    required_perspectives = ["trader_perspective", "follower_perspective"]
    for perspective in required_perspectives:
        if perspective not in data:
            return False, f"缺失顶层角色字段: {perspective}"
        if not isinstance(data[perspective], dict):
            return False, f"{perspective} 必须是一个字典对象"

    required_fields = ["comment_text", "link_text", "combined_preview", "score", "score_reason"]
    forbidden_endings = ("。", "！", "？", ".", "!", "?")

    for perspective in required_perspectives:
        view_data = data[perspective]

        for field in required_fields:
            if field not in view_data:
                return False, f"{perspective} 缺失必要字段: {field}"

        comment_text = view_data["comment_text"]
        link_text = view_data["link_text"]
        score = view_data["score"]
        score_reason = view_data["score_reason"]

        if not isinstance(comment_text, str):
            return False, f"{perspective}.comment_text 必须是字符串"
        if comment_text.endswith(forbidden_endings):
            return False, f"{perspective}.comment_text 违规：绝对禁止以终止性标点结尾"

        if not isinstance(link_text, str):
            return False, f"{perspective}.link_text 必须是字符串"
        if not (2 <= len(link_text) <= 6):
            return False, f"{perspective}.link_text 违规：长度必须严格在 2-6 个汉字之间"

        if not isinstance(score, (int, float)):
            return False, f"{perspective}.score 必须是数字"
        if not (0 <= score <= 10):
            return False, f"{perspective}.score 违规：评分必须在 0-10 之间"

        # if not isinstance(score_reason, str):
        #     return False, f"{perspective}.score_reason 必须是字符串"
        # if len(score_reason) > 30:
        #     return False, f"{perspective}.score_reason 违规：长度不能超过 30 字"

    return True, ""

def gen_promo_comment(post):
    """
    根据原始post生成推广评论，主要用于带单推广的场景。
    :return:
    """
    cleaned_post = format_post_for_promo(post)
    prompt_file_path = r'W:\project\python_project\crypto_trade\prompt\带单推广评论生成.txt'
    prompt = read_file_to_str(prompt_file_path)
    full_prompt = f'{prompt}'
    full_prompt += f'\n{cleaned_post}'
    model_name = "gemini-2.5-flash"
    # model_name = "gemini-3-pro-preview"
    comment_info = {}
    max_retries = 3
    for attempt in range(1, max_retries + 1):
        try:
            raw = get_llm_content(prompt=full_prompt, model_name=model_name)
            comment_info = string_to_object(raw)
            is_valid, error_message = check_comment_info(comment_info)
            if not is_valid:
                raise ValueError(f"第 {attempt} 次尝试: LLM 返回的数据格式或内容无效: {error_message}")

            return comment_info
        except Exception as e:
            logger.info(f"第 {attempt} 次尝试: 生成视频内容计划时出错: {e}")
            time.sleep(2 ** attempt)
    return comment_info


def gen_all_promo_comments():
    """
    批量生成所有推广评论。
    :param posts: 带单推广的原始帖子列表
    :return: 包含所有生成的推广评论的列表
    """
    post_manager = UniversalPostManager(gen_db_object())
    existing_posts = post_manager.find_posts_by_source("biance", limit=50000)
    for post in existing_posts:
        # 检查是否已经生成过推广评论
        if post.get("promo_comment"):
            logger.info(f"跳过已生成推广评论的帖子: {post.get('_id')}")
            continue

        comment_info = gen_promo_comment(post)
        post["promo_comment"] = comment_info
        if comment_info:
            post_manager.upsert_posts([post])
            logger.info(f"已生成推广评论并更新帖子: {post.get('_id')}")
        else:
            logger.info(f"未能生成有效的推广评论，跳过帖子: {post.get('_id')}")


# ==========================================
# 💡 测试运行代码
# ==========================================
if __name__ == "__main__":
    gen_all_promo_comments()
