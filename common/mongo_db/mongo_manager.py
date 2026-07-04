# mongo_manager.py

from datetime import datetime

from utils.mongo_base import MongoBase


class MongoManager:
    """
    业务层面的 MongoDB 操作管理器。
    封装了针对 video_materials 和 publish_tasks 集合的特定操作。
    """

    def __init__(self, db_instance: MongoBase):
        """
        初始化管理器，传入一个 MongoBase 的实例。
        """
        if not db_instance:
            raise ValueError("必须提供一个有效的 MongoBase 实例")
        self.db = db_instance
        self.materials_collection = "video_materials"
        self.tasks_collection = "publish_tasks"
        self._ensure_indexes()

    def find_task_by_exact_video_ids(self, video_id_list: list):
        """
        根据一个 video_id_list 进行精确匹配查询。
        这要求数据库中的数组与给定的列表在元素和顺序上都完全一致。

        Args:
            video_id_list (list): 待查询的视频ID列表。

        Returns:
            dict or None: 如果找到匹配的任务，则返回该任务文档；否则返回 None。
        """
        if not video_id_list:
            return None

        # 确保列表排序，与存储时保持一致
        # video_id_list.sort()

        query = {
            "video_id_list": video_id_list
        }
        return self.db.find_many(self.tasks_collection, query)

    def delete_tasks_by_ids(self, task_list: list):
        """
        根据传入的任务列表，批量删除 publish_tasks 表中的记录。
        会自动从列表中提取 '_id' 字段进行匹配删除。

        Args:
            task_list (list): 包含任务信息的字典列表 (每个元素需包含 '_id')。

        Returns:
            int: 成功删除的文档数量。
        """
        if not task_list:
            return 0

        # 1. 提取 _id
        ids_to_delete = []
        for item in task_list:
            # 如果是字典（通常情况），提取 _id
            if isinstance(item, dict) and '_id' in item:
                ids_to_delete.append(item['_id'])
            # 兼容处理：如果列表里直接装的是id而不是字典
            elif not isinstance(item, dict):
                ids_to_delete.append(item)

        if not ids_to_delete:
            print("警告: 传入的列表中没有提取到有效的 _id")
            return 0

        # 2. 构建查询条件
        query = {
            "_id": {
                "$in": ids_to_delete
            }
        }

        # 3. 执行删除
        # 这里使用了你代码底部演示的 get_collection 方法来获取原生集合对象
        try:
            collection = self.db.get_collection(self.tasks_collection)
            result = collection.delete_many(query)
            # print(f"已删除 {result.deleted_count} 条任务数据。")
            return result.deleted_count
        except Exception as e:
            print(f"删除任务数据时出错: {e}")
            return 0

    def _ensure_indexes(self):
        """
        确保核心查询字段已建立索引，以提高查询性能。
        这个方法应该在应用启动时调用一次。
        """
        # 为 video_materials 的 video_id 创建唯一索引，防止重复插入
        self.db.create_index(self.materials_collection, [('video_id', 1)], unique=True)
        # 为 publish_tasks 的 video_id_list 创建索引，加速查询
        self.db.create_index(self.tasks_collection, [('video_id_list', 1)], unique=False)
        self.db.create_index(self.tasks_collection, [('status', 1)], unique=False)
        self.db.create_index(self.tasks_collection, [('status', 1), ('upload_time', 1)], unique=False)
        self.db.create_index(self.tasks_collection, [('userName', 1), ('create_time', 1)], unique=False)


        # print("✅ 核心索引已确保存在。")

    # ==========================================
    # video_materials 表相关操作
    # ==========================================

    def find_tasks_after_time_with_status(self, target_time: datetime, status_list: list):
        """
        查询 publish_tasks 表中 upload_time 大于指定时间，且 status 在指定列表中的所有任务。

        Args:
            target_time (datetime): 指定的时间阈值（查询该时间之后的数据）。
            status_list (list): 状态字符串列表（例如 ['pending', 'running']）。

        Returns:
            list: 匹配的任务文档列表。
        """
        # 1. 基础校验：如果状态列表为空，查询无意义，直接返回空列表
        if not status_list:
            return []

        # 2. 校验 target_time 类型，确保与数据库存储类型一致
        # 你的原有代码中使用 datetime.now() 存储时间，所以这里强制要求传入 datetime 对象
        if not isinstance(target_time, datetime):
            raise ValueError("target_time 参数必须是 datetime 类型")

        # 3. 构建查询条件
        # MongoDB 隐式的 AND 操作：同时满足 upload_time > target_time 和 status in status_list
        query = {
            "uploaded_time": {
                "$gt": target_time  # $gt (greater than) 大于
            },
            "status": {
                "$in": status_list  # $in 包含于
            }
        }

        # 4. 执行查询
        return self.db.find_many(self.tasks_collection, query)

    def find_by_custom_query(self, collection_name: str, query: dict):
        """
        通用查询接口：支持对任意集合执行任意合法的 MongoDB 查询语句。

        Args:
            collection_name (str): 集合名称。
                                   可以直接传字符串 (如 "users")，
                                   也可以使用类属性 (如 self.tasks_collection)。
            query (dict): 标准的 MongoDB 查询字典。
                          支持操作符 (如 {"$gt": 10}, {"$in": [...]}, {"$regex": "pattern"})。

        Returns:
            list: 包含匹配文档的列表。
        """
        if not collection_name:
            print("错误: 未指定集合名称")
            return []

        if query is None:
            query = {}

        # 调用 MongoBase 的底层查询方法
        return self.db.find_many(collection_name, query)

    def find_tasks_by_status(self, status_list: list):
        """
        查询 publish_tasks 表中指定状态的数据。

        Args:
            status_list (list): 状态字符串列表。
                                例如: ['pending', 'processing', 'failed']

        Returns:
            list: 只要 task 的 status 在 status_list 中，就会被返回。
        """
        if not status_list:
            return []

        query = {
            "status": {
                "$in": status_list
            }
        }
        return self.db.find_many(self.tasks_collection, query)

    def find_unfinished_tasks(self):
        """
        查询所有状态不为 '已完成' 的发布任务。
        这对于获取待处理或正在处理的任务列表非常有用。

        Returns:
            list: 包含所有状态不为 '已完成' 的任务文档的列表。
        """
        query = {
            "status": {
                "$nin": ["已投稿", "待投稿"]
            }
        }
        # 同时，我们也可以排除那些根本没有 status 字段的旧数据（可选，但建议）
        # query["status"] = {"$exists": True, "$ne": "已完成"}
        return self.db.find_many(self.tasks_collection, query)

    def find_materials_by_ids(self, video_id_list: list):
        """
        1. 根据 video_id 列表，查询所有匹配的视频素材记录。

        Args:
            video_id_list (list): 包含视频ID字符串的列表。

        Returns:
            list: 包含所有匹配文档的列表。
        """
        if not video_id_list:
            return []
        query = {
            "video_id": {
                "$in": video_id_list
            }
        }
        return self.db.find_many(self.materials_collection, query)

    def upsert_materials(self, data_list: list):
        """
        2. 批量插入或更新视频素材记录。
           - 如果 video_id 已存在，则更新整条记录。
           - 如果 video_id 不存在，则插入新记录。
           - 自动维护 'update_time' 字段为当前时间。

        Args:
            data_list (list): 包含多个视频素材文档的列表。
                              每个文档必须包含 'video_id' 字段。
        """
        if not data_list:
            print("警告: 传入的 data_list 为空，无需操作。")
            return

        # 自动添加或更新 'update_time' 字段
        now = datetime.now()
        for item in data_list:
            item['update_time'] = now
        for i, item in enumerate(data_list):
            video_id = item.get('video_id')
            if not video_id or not isinstance(video_id, str):
                # 抛出异常，让上层调用者捕获并处理
                raise ValueError(f"第 {i+1} 条素材数据校验失败: 'video_id' 不能为空或非字符串。")
        # 使用 MongoBase 中已有的 bulk_upsert 方法，它非常高效
        # 第三个参数 'video_id' 指定了用于匹配和更新的唯一键
        self.db.bulk_upsert(self.materials_collection, data_list, "video_id")

    # ==========================================
    # publish_tasks 表相关操作
    # ==========================================

    def find_tasks_by_video_ids(self, video_id_list: list):
        """
        1. 查询包含指定 video_id 的所有发布任务。
           注意：只要任务的 video_id_list 字段中包含给定的任何一个 video_id，就会被匹配到。

        Args:
            video_id_list (list): 包含一个或多个 video_id 的列表。

        Returns:
            list: 匹配到的所有任务文档列表。
        """
        if not video_id_list:
            return []
        query = {
            "video_id_list": {
                "$in": video_id_list
            }
        }
        return self.db.find_many(self.tasks_collection, query)

    def upsert_tasks(self, data_list: list):
        """
        2. 批量插入或更新发布任务记录。
           - 基于 ['video_id_list', 'userName', 'creation_guidance_info'] 联合字段进行匹配。
           - 如果三者都完全相同，则更新。
           - 否则，插入新任务。
        """
        if not data_list:
            print("警告: 传入的 data_list 为空，无需操作。")
            return

        for i, item in enumerate(data_list):
            user_name = item.get('userName')
            video_ids = item.get('video_id_list')

            if not user_name or not isinstance(user_name, str):
                raise ValueError(f"第 {i + 1} 条任务数据校验失败: 'userName' 不能为空或非字符串。")

            if not video_ids or not isinstance(video_ids, list) or len(video_ids) == 0:
                raise ValueError(f"第 {i + 1} 条任务数据校验失败: 'video_id_list' 不能为空数组。")

        now = datetime.now()
        for item in data_list:
            item['update_time'] = now
            # # 对 video_id_list 排序，确保列表比较时的唯一性
            # if 'video_id_list' in item and isinstance(item['video_id_list'], list):
            #     item['video_id_list'].sort()

        # 修改点：将唯一键改为由三个字段组成的列表
        unique_keys = ["video_id_list", "userName", "creation_guidance_info"]
        self.db.bulk_upsert(self.tasks_collection, data_list, unique_keys)


# ==========================================
# 使用示例
# ==========================================
if __name__ == "__main__":
    # 1. 初始化数据库连接和管理器
    mongo_base_instance = gen_db_object()
    manager = MongoManager(mongo_base_instance)

    # 清理旧数据以便演示
    manager.db.get_collection(manager.materials_collection).delete_many({})
    manager.db.get_collection(manager.tasks_collection).delete_many({})
    print("\n--- 开始演示 ---")

    # 2. 准备模拟数据
    material_1 = {'video_id': 'vid001', 'base_info': {'video_title': '猫咪视频'}}
    material_2 = {'video_id': 'vid002', 'base_info': {'video_title': '狗狗视频'}}
    material_3 = {'video_id': 'vid003', 'base_info': {'video_title': '兔子视频'}}

    task_1 = {'userName': 'user_a', 'video_id_list': ['vid001', 'vid002']}
    task_2 = {'userName': 'user_b', 'video_id_list': ['vid003']}

    # 3. 演示 video_materials 操作
    print("\n--- 操作 video_materials ---")
    # 3.1 批量插入
    print("Step 1: 首次批量插入3条素材...")
    manager.upsert_materials([material_1, material_2, material_3])

    # 3.2 更新其中一条，并插入一条新的
    print("\nStep 2: 更新'vid001'并新增'vid004'...")
    material_1_updated = {'video_id': 'vid001', 'base_info': {'video_title': '超可爱的猫咪视频'}}
    material_4_new = {'video_id': 'vid004', 'base_info': {'video_title': '仓鼠视频'}}
    manager.upsert_materials([material_1_updated, material_4_new])

    # 3.3 按ID查询
    print("\nStep 3: 查询 'vid001', 'vid003', 'vid999'(不存在)...")
    results = manager.find_materials_by_ids(['vid001', 'vid003', 'vid999'])
    print(f"查询到 {len(results)} 条记录:")
    for doc in results:
        print(f"  - ID: {doc['video_id']}, 标题: {doc['base_info']['video_title']}, 更新时间: {doc['update_time']}")

    # 4. 演示 publish_tasks 操作
    print("\n--- 操作 publish_tasks ---")
    # 4.1 批量插入
    print("Step 1: 首次批量插入2个任务...")
    manager.upsert_tasks([task_1, task_2])

    # 4.2 更新一个任务 (注意 video_id_list 顺序变化)
    print("\nStep 2: 更新'user_a'的任务，故意打乱ID顺序...")
    task_1_updated = {'userName': 'user_a_updated', 'video_id_list': ['vid002', 'vid001']}  # 顺序颠倒
    manager.upsert_tasks([task_1_updated])
    count = manager.db.get_collection(manager.tasks_collection).count_documents({})
    print(f"操作后任务总数: {count} (因为排序，所以是更新而不是插入)")

    # 4.3 按 video_id 查询任务
    print("\nStep 3: 查询和'vid001'或'vid003'相关的任务...")
    task_results = manager.find_tasks_by_video_ids(['vid001', 'vid003'])
    print(f"查询到 {len(task_results)} 个相关任务:")
    for task in task_results:
        print(f"  - 用户: {task['userName']}, 视频列表: {task['video_id_list']}, 更新时间: {task['update_time']}")