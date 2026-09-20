# mongo_manager.py
# -- coding: utf-8 --

import logging
from uuid import uuid4
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
        - source + post_id : 联合唯一，防止跨平台 ID 冲突与重复写入 (遵循最左前缀)
        - publish_time     : 时间线拉取
        - source + card_type : 平台 / 帖子类型维度统计
        - post_id          : 新增普通索引，用于脱离 source 纯按 ID 检索的场景
        """
        self.db.create_index(self.collection_name, [('source', 1), ('post_id', 1)], unique=True)
        self.db.create_index(self.collection_name, [('publish_time', -1)], unique=False)
        self.db.create_index(self.collection_name, [('source', 1), ('card_type', 1)], unique=False)

        # 【新增索引】：为了支持单纯按 post_id 列表查询而不引起全表扫描
        self.db.create_index(self.collection_name, [('post_id', 1)], unique=False)

        logger.info(
            "索引就绪 | collection=%s | indexes=[uniq(source,post_id), publish_time(-1), (source,card_type), post_id]",
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

    def find_posts_by_ids(self, post_ids, source=None):
        """
        根据 post_id 列表批量拉取帖子数据。

        :param post_ids: list[str], 帖子 ID 列表 (例如: ["binance_1001", "xhs_6688"])
        :param source: str (可选), 指定平台来源。
                       强烈建议传入此参数！不仅能防止不同平台间偶然的 ID 冲突，
                       还能直接命中 (source, post_id) 的联合唯一索引，查询最快。
        :return: list[dict], 匹配的帖子列表
        """
        if not post_ids:
            return []

        # 核心语法：使用 MongoDB 的 $in 操作符
        query = {"post_id": {"$in": post_ids}}

        # 如果提供了 source，追加到查询条件中
        if source:
            query["source"] = source

        start = datetime.now(timezone.utc)
        posts = self.db.find_many(
            self.collection_name,
            query=query
        )
        cost_ms = (datetime.now(timezone.utc) - start).total_seconds() * 1000

        logger.info(
            "按ID列表查询完成 | source=%s | id_count=%s | matched=%s | cost=%.1fms",
            source or "ALL_PLATFORMS", len(post_ids), len(posts) if posts else 0, cost_ms
        )
        return posts


class GeneratedArticleManager:
    """
    生成文章管理器，独立使用 generated_articles 集合。

    status: processing / ok / skip / error。
    post_id_list: 仅由成功文章的 used_material_ids 反查得到，同一原帖去重。
    article_info: 校验通过的模型 JSON，以及程序补齐的 image_mapping。
    creation_brief / material_post_mapping: 本次输入及素材到原帖的溯源信息。
    error_message / error_history / raw_response: 最终异常、重试记录与原始返回。
    created_at / updated_at: UTC 时间。更新时应继续传入 save_article 返回的记录。

    只调用原 MongoBase 已有的 create_index / find_many / bulk_upsert。
    使用次数查询显式传 limit=0，要求沿用 MongoDB 的“不限制条数”语义。
    """

    COLLECTION_NAME = "generated_articles"
    UNIQUE_KEYS = ["article_id"]

    def __init__(self, db_instance):
        if not db_instance:
            raise ValueError("必须提供一个有效的 MongoBase 实例")
        self.db = db_instance
        self.collection_name = self.COLLECTION_NAME
        self._ensure_indexes()

    def _ensure_indexes(self):
        self.db.create_index(self.collection_name, [('article_id', 1)], unique=True)
        self.db.create_index(
            self.collection_name,
            [('source', 1), ('status', 1), ('post_id_list', 1)],
            unique=False
        )
        self.db.create_index(
            self.collection_name,
            [('source', 1), ('topic', 1), ('stance', 1), ('status', 1), ('created_at', -1)],
            unique=False
        )
        self.db.create_index(self.collection_name, [('updated_at', -1)], unique=False)

    def save_article(self, article_data):
        """按 article_id 幂等写入；从实际采用素材推导 post_id_list，禁止用输入池计数。"""
        record = dict(article_data)
        for key in ('source', 'topic', 'stance'):
            if not isinstance(record.get(key), str) or not record[key].strip():
                raise ValueError(f"文章缺失有效字段: {key}")

        status = record.get('status')
        if status not in ('processing', 'ok', 'skip', 'error'):
            raise ValueError(f"不支持的文章状态: {status}")

        post_id_list = []
        if status == 'ok':
            article_info = record.get('article_info')
            material_post_mapping = record.get('material_post_mapping')
            if not isinstance(article_info, dict) or article_info.get('status') != 'ok':
                raise ValueError("成功记录必须包含 status=ok 的 article_info")
            if not isinstance(material_post_mapping, dict):
                raise ValueError("成功记录必须包含 material_post_mapping")
            used_material_ids = article_info.get('used_material_ids')
            if not isinstance(used_material_ids, list) or not used_material_ids:
                raise ValueError("成功文章的 used_material_ids 不能为空")
            for material_id in used_material_ids:
                if not isinstance(material_id, str) or material_id not in material_post_mapping:
                    raise ValueError(f"实际采用素材无法反查原帖: {material_id!r}")
                post_id = material_post_mapping[material_id]
                if (not isinstance(post_id, (str, int)) or isinstance(post_id, bool)
                        or post_id in (None, '', 'UNKNOWN')):
                    raise ValueError(f"素材对应的原帖 ID 无效: {material_id}")
                if post_id not in post_id_list:
                    post_id_list.append(post_id)

        # skip / error / processing 不消耗素材；不接受调用方直接传入的候选帖子列表。
        record['post_id_list'] = post_id_list
        now = datetime.now(timezone.utc)
        record.setdefault('article_id', uuid4().hex)
        record.setdefault('created_at', now)
        record['updated_at'] = now
        record.setdefault('article_info', None)
        record.setdefault('error_message', None)
        record.setdefault('error_history', [])
        record.setdefault('raw_response', None)
        record.setdefault('attempt_count', 0)
        self.db.bulk_upsert(self.collection_name, [record], self.UNIQUE_KEYS)
        logger.info(
            "文章入库完成 | article_id=%s | status=%s | used_posts=%s",
            record['article_id'], status, len(post_id_list)
        )
        return record

    def get_post_usage_counts(self, source, post_ids):
        """统计同平台的成功文章引用次数，不限币种/立场；每篇文章内同帖最多计一次。"""
        unique_ids = list(dict.fromkeys(post_ids))
        counts = dict.fromkeys(unique_ids, 0)
        if not unique_ids:
            return counts
        articles = self.db.find_many(
            self.collection_name,
            query={'source': source, 'status': 'ok', 'post_id_list': {'$in': unique_ids}},
            limit=0
        )
        # 查询异常必须向上传播，不能把查询失败当成“使用次数为 0”。
        for article in articles:
            for post_id in set(article.get('post_id_list', [])):
                if post_id in counts:
                    counts[post_id] += 1
        return counts

    def get_recent_openings(self, source, topic, stance, limit=5):
        """取同币种、同立场近期成功文章的开头，供提示词避开重复表达。"""
        if limit <= 0:
            return []
        articles = self.db.find_many(
            self.collection_name,
            query={'source': source, 'topic': topic, 'stance': stance, 'status': 'ok'},
            sort=[('created_at', -1)],
            limit=limit
        )
        openings = []
        for article in articles:
            text = (article.get('article_info') or {}).get('text', '')
            if isinstance(text, str) and text.strip():
                opening = text.strip().splitlines()[0][:80]
                if opening not in openings:
                    openings.append(opening)
        return openings


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
