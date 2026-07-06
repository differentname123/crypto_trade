# mongo_manager.py
# -- coding: utf-8 --

import logging
from datetime import datetime, timezone # 替换原有的 from datetime import datetime
from common.common_utils import setup_logger
from common.mongo_db.mongo_base import gen_db_object

setup_logger()

# 拿到属于当前文件的专属 logger
logger = logging.getLogger(__name__)

class UniversalPostManager:
    """
    通用社交媒体帖子数据管理器。
    兼容 Binance, Zhihu, Xiaohongshu, Bilibili 等全平台通用 Schema。
    """

    COLLECTION_NAME = "social_media_posts"
    UNIQUE_KEYS = ["source", "post_id"]

    def __init__(self, db_instance):
        if not db_instance:
            raise ValueError("必须提供一个有效的 MongoBase 实例")
        self.db = db_instance
        self.collection_name = self.COLLECTION_NAME
        self._ensure_indexes()

    def _ensure_indexes(self):
        """
        初始化核心索引，保障查询速度与数据隔离。
        - source + post_id : 联合唯一，防止跨平台 ID 冲突与重复写入
        - publish_time     : 时间线拉取
        - source + card_type : 平台 / 帖子类型维度统计
        """
        self.db.create_index(self.collection_name, [('source', 1), ('post_id', 1)], unique=True)
        self.db.create_index(self.collection_name, [('publish_time', -1)], unique=False)
        self.db.create_index(self.collection_name, [('source', 1), ('card_type', 1)], unique=False)

        logger.info(
            "索引就绪 | collection=%s | indexes=[uniq(source,post_id), publish_time(-1), (source,card_type)]",
            self.collection_name
        )

    def upsert_posts(self, data_list):
        """
        将清洗后的通用 Schema 数据批量安全入库。
        - 命中 (source + post_id) -> 更新最新数据 (如点赞、评论数)
        - 未命中               -> 插入新帖
        """
        if not data_list:
            logger.warning("upsert_posts 收到空数据集，已跳过入库")
            return

        # 先做全量前置校验，再统一打标，避免校验失败时残留脏副作用
        source_counter = {}
        for i, item in enumerate(data_list):
            post_id = item.get("post_id")
            source = item.get("source")
            if not post_id or not source:
                logger.error(
                    "入库校验失败 | index=%s | post_id=%r | source=%r | reason=缺失联合唯一键字段",
                    i, post_id, source
                )
                raise ValueError(f"索引 {i} 数据错误: 必须包含完整的 'post_id' 和 'source'")
            source_counter[source] = source_counter.get(source, 0) + 1

        # 校验全部通过后，统一追加最后更新时间（UTC，避免跨时区歧义）
        update_time = datetime.now(timezone.utc)
        for item in data_list:
            item['db_update_time'] = update_time

        start = datetime.now(timezone.utc)
        self.db.bulk_upsert(self.collection_name, data_list, self.UNIQUE_KEYS)
        cost_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000
        logger.info(
            "批量入库完成 | total=%s | dist=%s | cost=%.1fms | keys=%s",
            len(data_list), source_counter, cost_ms, self.UNIQUE_KEYS
        )

    def find_posts_by_source(self, source, limit=100):
        """按平台来源拉取数据，按发布时间最新排序"""
        posts = self.db.find_many(
            self.collection_name,
            query={"source": source},
            sort=[("publish_time", -1)],
            limit=limit
        )

        logger.info(
            "查询完成 | source=%s | limit=%s | matched=%s",
            source, limit, len(posts) if posts else 0
        )
        return posts


# ==========================================
# 接入清洗流程的使用示例
# ==========================================
if __name__ == "__main__":
    # 1. 建立数据库连接
    db_instance = gen_db_object()
    post_manager = UniversalPostManager(db_instance)

    # 2. 模拟经 clean_universal_posts / update_posts_in_place 清洗完成后的数据
    cleaned_data = [
        {
            "post_id": "binance_1001",
            "publish_time": 1700000000,
            "author_id": "author_001",
            "card_type": "BUZZ_LONG",
            "source": "binance",
            "metadata": {"url": "...", "is_ai_generated": False},
            "author": {"username": "CryptoKing"},
            "content": {"title": "BTC 分析", "text_content": "今天拉盘..."},
            "engagement": {"view_count": 100, "like_count": 10}
        },
        {
            "post_id": "xhs_6688",  # 不同平台的 ID 逻辑完全不同
            "publish_time": 1700000500,
            "author_id": "user_xhs1",
            "card_type": "NOTE_IMAGE",
            "source": "xiaohongshu",
            "metadata": {"url": "...", "is_ai_generated": False},
            "author": {"username": "小红薯"},
            "content": {"title": "OOTD", "text_content": "今天穿搭..."},
            "engagement": {"view_count": 500, "like_count": 200}
        }
    ]

    # 3. 一键入库
    post_manager.upsert_posts(cleaned_data)

    # 4. 验证查询
    binance_posts = post_manager.find_posts_by_source("binance", limit=5)
    logger.info("样例验证 | binance 帖子数=%s", len(binance_posts) if binance_posts else 0)