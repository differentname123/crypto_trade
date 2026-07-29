# -- coding: utf-8 --
import numpy as np
import chromadb
from sentence_transformers import SentenceTransformer

# ================= 0. 全局默认配置 =================
# 将不会变化的路径写死在常量里，避免每次调用都要传参
DEFAULT_MODEL_PATH = r"C:\Users\zxh\.cache\huggingface\hub\models--BAAI--bge-base-zh-v1.5\snapshots\f03589ceff5aac7111bd60cfc7d497ca17ecac65"
DEFAULT_DB_PATH = r"W:\project\python_project\crypto_trade\biance\ai_content_pipeline\vector_db"


# ================= 1. 全局模型单例缓存管理器 =================
class ModelManager:
    """全局模型缓存，确保同一个模型无论在何处调用，只在显存/内存中存在一份"""
    _cache = {}

    @classmethod
    def get_model(cls, model_path=DEFAULT_MODEL_PATH, device="cuda"):
        if model_path not in cls._cache:
            print(f"🚀 [ModelManager] 正在首次加载模型并存入缓存: {model_path} ...")
            cls._cache[model_path] = SentenceTransformer(model_path, device=device)
        return cls._cache[model_path]


# ================= 2. 核心向量引擎类 =================
class VectorSearchEngine:
    def __init__(self, model_path=DEFAULT_MODEL_PATH, db_path=DEFAULT_DB_PATH, collection_name="my_collection",
                 device="cuda"):
        """
        初始化纯净版向量检索引擎（仅做 ID 索引）
        """
        self.model_path = model_path
        self.device = device
        self.model = ModelManager.get_model(model_path, device)

        self.client = chromadb.PersistentClient(path=db_path)
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"hnsw:space": "cosine"}
        )
        print(
            f"✅ [VectorSearchEngine] 数据库连接成功 | 集合: {collection_name} | 当前数据量: {self.collection.count()}")

    def add_data(self, data_list, batch_size=5000):
        """
        入库接口。数据格式极致精简：
        [{"id": "数据ID", "search_text": "用于生成的超文本"}, ...]
        """
        if not data_list:
            return {"status": "error", "msg": "数据列表为空"}

        existing_ids = set(self.collection.get(include=[])['ids'])

        ids_to_add = []
        docs_to_add = []

        print("🔍 正在比对并过滤已存在的数据节点...")
        for item in data_list:
            node_id = str(item.get("id", "")).strip()
            search_text = str(item.get("search_text", "")).strip()

            if not node_id or not search_text:
                continue

            if node_id in existing_ids:
                continue

            ids_to_add.append(node_id)
            docs_to_add.append(search_text)

        total_new = len(ids_to_add)
        if total_new == 0:
            return {"status": "success", "msg": "没有新数据，无需更新", "added_count": 0}

        print(f"📦 发现 {total_new} 条新数据，开始分批向量化入库 (Batch Size: {batch_size})...")

        for i in range(0, total_new, batch_size):
            end_idx = min(i + batch_size, total_new)
            batch_ids = ids_to_add[i:end_idx]
            batch_docs = docs_to_add[i:end_idx]

            print(f"👉 正在处理第 {i + 1} 到 {end_idx} 条 ...")
            embeddings = self.model.encode(
                batch_docs,
                batch_size=64,
                normalize_embeddings=True,
                show_progress_bar=True
            )

            # 去掉了 metadatas，向量库变得极其轻量
            self.collection.add(
                embeddings=embeddings.tolist(),
                documents=batch_docs,
                ids=batch_ids
            )

        print(f"🎉 全部 {total_new} 条数据入库完成！")
        return {"status": "success", "msg": "入库完成", "added_count": total_new}

    def search(self, keywords, top_n=5):
        """
        搜索接口。返回仅包含 id、匹配词和相似度的列表。
        拿到返回的 ID 后，业务层自行去其他数据库提取详情。
        """
        if isinstance(keywords, str):
            keywords = [keywords]

        unique_nodes = {}

        for kw in keywords:
            query_embedding = self.model.encode(kw, normalize_embeddings=True).tolist()
            results = self.collection.query(
                query_embeddings=[query_embedding],
                n_results=top_n
            )

            if results and results['ids'] and results['ids'][0]:
                for i, node_id in enumerate(results['ids'][0]):
                    similarity = 1 - results['distances'][0][i]

                    if node_id not in unique_nodes or similarity > unique_nodes[node_id]['similarity']:
                        unique_nodes[node_id] = {
                            "id": node_id,
                            "similarity": float(similarity),
                            "matched_keyword": kw
                        }

        sorted_results = sorted(unique_nodes.values(), key=lambda x: x['similarity'], reverse=True)
        return sorted_results


# ================= 3. 基础语义工具函数 =================

def compute_text_similarity(text_a, text_b, model_path=DEFAULT_MODEL_PATH, device="cuda"):
    model = ModelManager.get_model(model_path, device)
    vec_a = model.encode(text_a, normalize_embeddings=True)
    vec_b = model.encode(text_b, normalize_embeddings=True)
    return float(np.dot(vec_a, vec_b))


def compute_vector_similarity(vec_a, vec_b):
    return float(np.dot(np.asarray(vec_a), np.asarray(vec_b)))


if __name__ == "__main__":
    # 初始化时连路径都不用传了，直接用默认值！
    engine = VectorSearchEngine(collection_name="crypto_articles")

    # 构建极简数据
    data_to_add = [
        {"id": "POST_01", "search_text": "小币慢投...关联币种：BTC, SOL...200x"},
        {"id": "POST_02", "search_text": "另一个需要向量化的长文本..."}
    ]

    # 入库
    engine.add_data(data_to_add)

    # 搜索
    results = engine.search("200倍杠杆")
    for res in results:
        print(res["id"], res["similarity"])