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

if __name__ == "__main__":
    master_feed_list = []

    logger.info("========== 🚀 开始全量数据抓取测试 ==========")

    # 1. 抓取推荐流
    logger.info("--- 1. 准备抓取: 推荐流 ---")
    recommend_data = fetch_binance_feed(count=10)
    master_feed_list.extend(recommend_data)

    # 2. 抓取搜索流 (以 "ETH" 为例)
    logger.info("--- 2. 准备抓取: 搜索流 (ETH) ---")
    search_data = fetch_binance_feed(keyword="ETH", count=10)
    master_feed_list.extend(search_data)

    # 3. 抓取特定币种流 (以 "BTC" 为例，按热门排序)
    logger.info("--- 3. 准备抓取: 币种流 (BTC) ---")
    token_data = fetch_binance_feed(token="BTC", count=10, orderBy=1)
    master_feed_list.extend(token_data)

    logger.info(f"========== 🏁 全量抓取结束 | 汇总总计 {len(master_feed_list)} 条 ==========")

    temp_all_result_list = clean_universal_posts(master_feed_list)
    final_temp_all_result_list = update_posts_in_place(temp_all_result_list)
    db_instance = gen_db_object()
    post_manager = UniversalPostManager(db_instance)
    post_manager.upsert_posts(final_temp_all_result_list)