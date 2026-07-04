# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2025/12/5 13:26
:last_date:
    2025/12/5 13:26
:description:
    
"""
import urllib.parse
from pymongo import MongoClient, UpdateOne, ASCENDING, DESCENDING
from pymongo.errors import PyMongoError, DuplicateKeyError, BulkWriteError
from bson.objectid import ObjectId

from utils.common_utils import get_config


class MongoBase:
    _instance = None
    _client = None

    def __new__(cls, *args, **kwargs):
        """
        单例模式：确保全局只维护一个数据库连接池，减少资源消耗
        """
        if not cls._instance:
            cls._instance = super(MongoBase, cls).__new__(cls)
        return cls._instance

    def __init__(self,
                 host="localhost",
                 port=27017,
                 username=None,
                 password=None,
                 db_name="admin",
                 auth_source="admin",
                 max_pool_size=100):
        """
        初始化 MongoDB 连接
        :param auth_source: 认证数据库，通常是 admin
        :param max_pool_size: 连接池最大连接数，根据你的并发线程数设定（默认100）
        """
        if not self._client:
            # 1. 安全处理密码中的特殊字符 (如 @, :, / 等)
            if username and password:
                username = urllib.parse.quote_plus(username)
                password = urllib.parse.quote_plus(password)
                uri = f"mongodb://{username}:{password}@{host}:{port}/{auth_source}"
            else:
                uri = f"mongodb://{host}:{port}/"

            # print(f"正在连接 MongoDB: {host}:{port}...")

            # 2. 建立连接
            # connect=False: 延迟连接，这对多进程(multiprocessing)安全非常重要
            # maxPoolSize: 决定了能支持多少并发操作
            self._client = MongoClient(uri, maxPoolSize=max_pool_size, connect=False)
            self.db = self._client[db_name]
            print(f"✅ MongoDB 连接成功，当前数据库: {db_name}")

    def get_collection(self, collection_name):
        """获取集合对象"""
        return self.db[collection_name]

    # ==========================
    # 基础 CRUD 操作
    # ==========================

    def insert_one(self, collection_name, data):
        """插入单条数据"""
        try:
            col = self.get_collection(collection_name)
            result = col.insert_one(data)
            return result.inserted_id
        except PyMongoError as e:
            print(f"❌ 插入失败: {e}")
            return None

    def insert_many(self, collection_name, data_list):
        """批量插入数据"""
        if not data_list: return
        try:
            col = self.get_collection(collection_name)
            result = col.insert_many(data_list)
            return result.inserted_ids
        except PyMongoError as e:
            print(f"❌ 批量插入失败: {e}")
            return None

    def find_one(self, collection_name, query, projection=None):
        """查询单条数据"""
        col = self.get_collection(collection_name)
        return col.find_one(query, projection)

    def find_many(self, collection_name, query={}, projection=None, limit=0, sort=None):
        """
        查询多条数据
        :param sort: 示例 [('create_time', -1)] 或 [('create_time', DESCENDING)]
        """
        col = self.get_collection(collection_name)
        cursor = col.find(query, projection)

        if sort:
            cursor = cursor.sort(sort)
        if limit > 0:
            cursor = cursor.limit(limit)

        return list(cursor)

    def update_one(self, collection_name, query, update_data, upsert=False):
        """
        更新单条数据
        :param upsert: 如果为True，不存在则插入。这是处理并发写入最常用的安全手段。
        """
        try:
            col = self.get_collection(collection_name)
            # 自动封装 $set，防止误覆盖整个文档
            if "$set" not in update_data and "$inc" not in update_data and "$push" not in update_data:
                update_statement = {"$set": update_data}
            else:
                update_statement = update_data

            result = col.update_one(query, update_statement, upsert=upsert)
            return result.modified_count
        except PyMongoError as e:
            print(f"❌ 更新失败: {e}")
            return 0

    def delete_many(self, collection_name, query):
        """删除数据"""
        try:
            col = self.get_collection(collection_name)
            result = col.delete_many(query)
            return result.deleted_count
        except PyMongoError as e:
            print(f"❌ 删除失败: {e}")
            return 0

    # ==========================
    # 高级 / 并发安全操作
    # ==========================

    def safe_upsert(self, collection_name, unique_key, unique_value, data):
        """
        安全保存（推荐用于素材入库）：
        如果库里没有 -> 插入
        如果库里有 -> 更新
        """
        col = self.get_collection(collection_name)
        try:
            # $setOnInsert: 仅在插入新文档时生效，更新时不修改这些字段（常用于创建时间）
            update_op = {
                "$set": data
            }
            # 如果需要保留首次创建时间，可以在这里添加 $setOnInsert
            # update_op["$setOnInsert"] = {"created_at": datetime.now()}

            result = col.update_one(
                {unique_key: unique_value},
                update_op,
                upsert=True
            )
            return result.upserted_id is not None  # True表示是新插入，False表示是更新
        except PyMongoError as e:
            print(f"❌ Safe Upsert Error: {e}")
            return False

    def atomic_get_and_lock(self, collection_name, query, update_status_to):
        """
        原子性获取并锁定任务（推荐用于多线程抢任务）：
        查找符合 query 的一条数据，并立即修改其状态，
        防止其他线程/进程同时获取到同一条数据。
        """
        col = self.get_collection(collection_name)
        try:
            doc = col.find_one_and_update(
                filter=query,
                update={"$set": {"status": update_status_to}},
                sort=[("created_at", ASCENDING)],  # 优先处理最早的
                return_document=True  # 返回修改后的文档
            )
            return doc
        except PyMongoError as e:
            print(f"❌ 原子操作失败: {e}")
            return None

    def bulk_upsert(self, collection_name, data_list, unique_key_field):
        """
        批量 Upsert（高性能）：
        支持单字段唯一键 (str) 或 多字段联合唯一键 (list)
        """
        if not data_list:
            return

        collection = self.get_collection(collection_name)
        operations = []

        # 统一将 unique_key_field 转为列表处理，兼容传入单个字符串的情况
        unique_keys = unique_key_field if isinstance(unique_key_field, list) else [unique_key_field]

        for item in data_list:
            # 1. 复制数据
            update_data = item.copy()
            query = {}

            # 2. 构建复合查询条件 (Query) 并从更新数据 ($set) 中移除这些键
            missing_key = False
            for key in unique_keys:
                # 尝试从数据中获取并移除 key
                val = update_data.pop(key, None)
                if val is None:
                    # 如果缺少构成唯一键的任何一个字段，则无法进行准确的 upsert
                    missing_key = True
                    break
                query[key] = val

            if missing_key:
                print(f"警告: 数据项缺少关键字段 {unique_keys} 中的某一项，已跳过: {item}")
                continue

            # 3. 构建更新部分 ($set)
            # MongoDB Upsert 逻辑：如果没找到，会合并 query 和 $set 插入；如果找到了，只执行 $set 更新
            update_instruction = {"$set": update_data}

            # 4. 创建 UpdateOne 操作
            operations.append(UpdateOne(query, update_instruction, upsert=True))

        # 5. 执行批量写入
        if operations:
            try:
                # ordered=False 提高性能，单条失败不影响其他
                result = collection.bulk_write(operations, ordered=False)
                # print(f"批量 Upsert 完成: 匹配 {result.matched_count}, 修改 {result.modified_count}, 插入 {result.upserted_count}")
            except Exception as e:
                print(f"批量 Upsert 时发生错误: {e}")
                raise

    def create_index(self, collection_name, keys, unique=False):
        """
        创建索引
        keys: [('field_name', 1)]  (1为升序, -1为降序)
        """
        try:
            col = self.get_collection(collection_name)
            col.create_index(keys, unique=unique)
            # print(f"索引创建成功: {collection_name} -> {keys}")
        except PyMongoError as e:
            print(f"索引创建失败: {e}")


def gen_db_object():
    """
    生成连接的实例
    :return:
    """
    HOST = get_config("local_mongo_host")
    PORT = get_config("local_mongo_port")
    USERNAME = get_config("local_mongo_user")
    PASSWORD = get_config("local_mongo_password")
    DB_NAME = get_config("local_mongo_db_name")
    mongo_instance = MongoBase(
        host=HOST,
        port=PORT,
        username=USERNAME,
        password=PASSWORD,
        db_name=DB_NAME,
        max_pool_size=2
    )
    return mongo_instance


if __name__ == "__main__":
    import time
    from datetime import datetime

    print("--- 1. 初始化数据库连接 ---")
    mongo = gen_db_object()

    # 为了演示干净，先清空测试集合 (实际使用请注释掉)
    mongo.db["source_assets"].drop()
    mongo.db["production_tasks"].drop()
    print("已清空测试数据，开始演示...\n")

    # ==========================================
    # 2. 场景演示：爬虫数据入库 (Safe Upsert)
    # ==========================================
    print("--- 2. 爬虫数据入库 (防止重复) ---")

    # 模拟第一次抓取到视频
    video_info = {
        "video_id": "75732309507",
        "title": "搞笑小猫",
        "play_count": 1000,
        "author": "lin",
        "crawled_at": datetime.now()
    }
    # 使用 video_id 作为唯一键
    is_new = mongo.safe_upsert("source_assets", "video_id", "75732309507", video_info)
    print(f"第一次插入视频 75732309507: {'新插入' if is_new else '已更新'}")

    # 模拟第二次抓取到同一个视频（数据变了，播放量增加了）
    video_info_updated = {
        "video_id": "75732309507",
        "title": "搞笑小猫(火爆)",
        "play_count": 5000,  # 数据更新
        "crawled_at": datetime.now()
    }
    is_new = mongo.safe_upsert("source_assets", "video_id", "75732309507", video_info_updated)
    print(f"第二次插入视频 75732309507: {'新插入' if is_new else '已更新'}")

    # 验证结果
    saved_doc = mongo.find_one("source_assets", {"video_id": "75732309507"})
    print(f"当前数据库中的播放量: {saved_doc.get('play_count')}\n")

    # ==========================================
    # 3. 场景演示：批量高性能写入
    # ==========================================
    print("--- 3. 批量写入数据 ---")
    batch_videos = []
    for i in range(5):
        batch_videos.append({
            "video_id": f"batch_vid_{i}",
            "title": f"批量视频_{i}",
            "play_count": i * 100
        })

    # 一次性写入 5 条，比循环调 update_one 快得多
    mongo.bulk_upsert("source_assets", batch_videos, "video_id")
    print("\n")

    # ==========================================
    # 4. 场景演示：多线程 Worker 抢任务 (原子锁)
    # ==========================================
    print("--- 4. 并发抢任务测试 ---")

    # A. 先创建几个待处理的任务
    tasks = [
        {"task_id": "t1", "user": "lin", "status": "pending"},
        {"task_id": "t2", "user": "mama", "status": "pending"},
        {"task_id": "t3", "user": "lin", "status": "error"}  # 干扰项
    ]
    mongo.insert_many("production_tasks", tasks)
    print(f"已创建 {len(tasks)} 个任务")

    # B. 模拟 Worker 1 来领任务 (只领 user='lin' 且 status='pending' 的)
    query = {"status": "pending", "user": "lin"}

    # 这一步是原子操作，哪怕有10个线程同时运行这行代码，也只有一个能拿到这个 task
    task_fetched = mongo.atomic_get_and_lock(
        collection_name="production_tasks",
        query=query,
        update_status_to="processing"  # 拿到后立马改为 processing
    )

    if task_fetched:
        print(f"Worker 成功抢到任务: ID={task_fetched['task_id']}, 用户={task_fetched['user']}")
        print("正在处理中...")
        # 模拟业务处理时间
        time.sleep(0.5)
        # 处理完成，更新状态为 uploaded
        mongo.update_one("production_tasks", {"_id": task_fetched["_id"]}, {"status": "uploaded", "bvid": "BV123456"})
        print("任务处理完成，状态已更新为 uploaded")
    else:
        print("没有抢到符合条件的任务")

    # C. 验证再次领取（应该领不到了，因为状态已经是 processing/uploaded 了）
    task_again = mongo.atomic_get_and_lock("production_tasks", query, "processing")
    if not task_again:
        print("再次尝试领取: 失败 (符合预期的行为，防止重复处理)\n")

    # ==========================================
    # 5. 简单查询统计
    # ==========================================
    print("--- 5. 最终数据统计 ---")
    asset_count = mongo.db["source_assets"].count_documents({})
    task_status = mongo.find_many("production_tasks", {}, projection={"task_id": 1, "status": 1, "_id": 0})

    print(f"素材表总数: {asset_count}")
    print(f"任务表状态: {task_status}")

    print("\n✅ 所有演示结束")