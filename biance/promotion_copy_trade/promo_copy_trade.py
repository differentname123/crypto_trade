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
import json

from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager


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
            top_comments.append(f"{c_author}: {c_content}")

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



# ==========================================
# 💡 测试运行代码
# ==========================================
if __name__ == "__main__":
    post_manager = UniversalPostManager(gen_db_object())
    existing_posts = post_manager.find_posts_by_source("biance", limit=50000)
    count = 0
    for post in existing_posts:
        count += 1
        if count > 50:
            break
        cleaned_post = format_post_for_promo(post)
        print(json.dumps(cleaned_post, ensure_ascii=False, indent=4))
