# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:23
:last_date:
    2026/4/7 0:23
:description:

"""
import logging

from biance.biance_squre_api import fetch_binance_feed, clean_universal_posts, update_posts_in_place
from common.common_utils import setup_logger
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager

setup_logger()

# 拿到属于当前文件的专属 logger
logger = logging.getLogger(__name__)

def fetch_follow_content():
    db_instance = gen_db_object()
    post_manager = UniversalPostManager(db_instance)
    # 币安广场互关互粉全量聚合关键词列表（已去重分类）
    aggregated_binance_follow_keywords = [
        # === 1. 中文核心极简词汇 ===
        "互关",
        "互粉",
        "互赞",
        "互评",
        "互fo",
        "互助",
        "互",
        "涨粉",
        "粉丝互助",
        "互关互粉",
        "互赞互评",
        "互粉互赞",
        "互关互赞",
        "互换关注",

        # === 2. 中文承诺与高意图动作词汇 ===
        "关注必回",
        "点赞必回",
        "评论必回",
        "留下评论必回",
        "必回关",
        "关必回",
        "秒回关",
        "秒回",
        "互关秒回",
        "留印必回",
        "留下脚印",
        "粉必回",
        "有粉必回",
        "必须回关",
        "回关",
        "留关",
        "关注我",
        "点个关注",
        "点赞互关",
        "诚信互关",
        "关注报数",
        "赚积分互助",

        # === 3. 中文币安/广场/加密圈特定场景词汇 ===
        "广场互关",
        "广场互粉",
        "币安互关",
        "币安 互关",
        "币安 互粉",
        "币安 点赞 互关",
        "币安广场互关",
        "币安广场互粉",
        "创作者互关",
        "广场升级互关",
        "粉丝任务",
        "创作者任务",
        "大V任务",
        "新手互关",
        "加密货币 互粉",
        "币圈 互关互粉",

        # === 4. 英文全球通用核心词汇 ===
        "f4f",
        "follow for follow",
        "follow4follow",
        "followback",
        "follow back",
        "mutual follow",
        "mutuals",
        "follow me",
        "followme",
        "follow each other",
        "l4l",
        "like for like",
        "like4like",
        "sub4sub",
        "let's grow together",

        # === 5. 英文币安与 Crypto 特定词汇 ===
        "Binance follow for follow",
        "Binance F4F",
        "Binance mutual follow",
        "follow me Binance",
        "follow back Binance",
        "Binance Square follow",
        "Binance community follow",
        "crypto follow for follow",
        "crypto F4F",
        "F4F crypto",
        "crypto mutual follow",
        "follow back crypto",
        "crypto community follow",
        "crypto followers exchange",
        "web3 follow for follow"
    ]

    # 如果需要纯粹的一维列表（共 82 个独立关键词），直接遍历上述列表即可
    print(f"成功加载，共聚合 {len(aggregated_binance_follow_keywords)} 个唯一搜索关键词。")


    while True:
        master_feed_list = []
        for search_key in aggregated_binance_follow_keywords:
            logger.info(f"--- 准备抓取: 搜索流 ({search_key}) ---")
            search_data = fetch_binance_feed(keyword=search_key, count=10)
            master_feed_list.extend(search_data)
        logger.info(f"抓取完成，共获取 {len(master_feed_list)} 条数据，准备保存...")
        post_manager.upsert_posts(master_feed_list)


if __name__ == "__main__":
    try:
        fetch_follow_content()
    except Exception as e:
        logger.error(f"程序运行中发生异常: {e}", exc_info=True)