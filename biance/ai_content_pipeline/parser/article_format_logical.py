"""
[功能摘要] 定时扫描币安数据源的图文帖子，利用大模型将帖子内嵌的媒体元素（图片/视频）进行语义化解析与格式化重构。
[输入数据] 从 MongoDB 提取的帖子源数据 (post)，核心依赖 `content.text_content` 及其附属的 `media.local_mapping` (占位符到本地文件路径的映射)。
[数据流转/交互]
1. 轮询读取 MongoDB 中未格式化的帖子数据。
2. 文本清洗：将帖子原有的不规则占位符（如 `[插图:http...]`）统一替换为标准占位符（如 `[IMAGE_01]`），并聚合对应的本地媒体文件路径。
3. AI 交互：携带清洗后的文本和物理媒体路径，调用 Gemini/Playwright 接口进行视觉与文本的联合语义解析。
4. 校验拦截：严格校验大模型返回的 JSON 元数据结构，确保返回的 image_id 与解析的占位符数量及名称做到 1:1 绝对映射。
[输出数据] 将解析并严格校验通过的格式化元数据赋值给 post['logic_mul']，随后持久化更新至 MongoDB 数据库。
"""
import json
import re
import threading
import time
from collections import defaultdict
from multiprocessing import get_context

from biance.biance_squre_api import publish_to_binance_square
from common.common_utils import setup_logger, read_file_to_str, string_to_object, get_config, read_json, save_json

# from common.vector_utils import VectorSearchEngine

logger = setup_logger(app_name="media_format")

from app.ai_api.gemini_playwright import generate_gemini_content_playwright
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager, GeneratedArticleManager
from concurrent.futures import ThreadPoolExecutor, as_completed
BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
PROMPT_FILE_PATH = r'W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类MLU提取.txt'
LLM_MAX_RETRIES = 3
ARTICLE_PROMPT_FILE_PATH = r'W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类文章生成.txt'
ARTICLE_MATERIAL_LIMIT = 20
ARTICLE_POST_USAGE_LIMIT = 5  # 严格按“超过 5 次剔除”：已有 5 次仍可入选。
ARTICLE_MAX_CHARS = 260
ARTICLE_GENERATION_INTERVAL_SECONDS = 3600

max_age_hours = 24 * 2
# 全局初始化向量引擎（单例调用，避免重复加载）
# VECTOR_ENGINE = VectorSearchEngine(collection_name="binance_posts_index")


def build_search_text(post):
    """
    数据降维与高密度提纯：将复杂的帖子格式化字典，提取拼装为高浓度的"超级搜索文本"
    [入参]: post 帖子全量字典
    [出参]: 纯字符串 (String)
    """
    # 1. 提取并清理正文（利用正则去除无语义的 [插图: url] 占位符噪音）
    raw_text = post.get("content", {}).get("text_content", "")
    cleaned_text = re.sub(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]", "", raw_text).strip()

    # 2. 提取关联币种
    coins = post.get("content", {}).get("mentioned_coins", [])
    coins_str = ", ".join(coins) if coins else "无"

    # 3. 基础正文拼装
    search_text = f"【文章正文】\n{cleaned_text}\n关联币种：{coins_str}\n"

    # 4. 遍历提取媒体特征 (核心维度)
    logic_mul = post.get("logic_mul", [])
    for i, media in enumerate(logic_mul):
        visual_fact = media.get("visual_fact", {})
        semantic_core = media.get("semantic_core", {})
        narrative = media.get("narrative_role", {})

        # 合并实体与概念，形成高密度标签
        concepts = semantic_core.get("concepts", [])
        entities = visual_fact.get("entities", [])
        all_concepts_str = ", ".join(concepts + entities)

        desc = visual_fact.get("description", "")
        ocr = visual_fact.get("ocr_text", "")

        # 合并叙事意图与逻辑桥梁
        msg = semantic_core.get("message", "")
        logic = narrative.get("logic_bridge", "")
        logic_str = f"{msg} {logic}".strip()

        # 拼装单张图片的语义块
        search_text += f"\n【配图{i + 1}语义解析】\n"
        search_text += f"核心概念：{all_concepts_str}\n"
        search_text += f"画面描述：{desc}\n"
        search_text += f"图文逻辑：{logic_str}\n"
        # OCR 数据作为最硬核的过滤依据，放在最后
        search_text += f"关键数据(OCR)：{ocr}\n"

    return search_text.strip()


# def sync_posts_to_vector_db(post_manager):
#     """
#     批量入库函数：将格式化完毕的帖子列表，提取ID和超级文本后，灌入 ChromaDB。
#     由于底层的 VectorSearchEngine 已做好防重复校验，可放心重复传入历史数据。
#     [入参]: post_list (从 MongoDB 查询出的 post 字典列表)
#     [出参]: 执行状态字典
#     """
#     data_to_add = []
#     existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
#     # 保留logic_mul存在的posts
#     filter_posts = [post for post in existing_posts if post.get("logic_mul")]
#     logger.info(f"[向量库/同步] 查询到 {len(existing_posts)} 条帖子，其中 {len(filter_posts)} 条已完成格式化，准备入库...")
#
#
#     for post in filter_posts:
#         post_id = post.get("post_id")
#         logic_mul = post.get("logic_mul")
#
#         # 拦截校验：只有存在 post_id 且已经过大模型格式化的帖子才允许入库
#         if not post_id or not logic_mul:
#             continue
#
#         search_text = build_search_text(post)
#         data_to_add.append({
#             "id": post_id,
#             "search_text": search_text
#         })
#
#     if data_to_add:
#         logger.info(f"[向量库/同步] 准备将 {len(data_to_add)} 条解析完毕的数据送入向量库提取特征...")
#         return VECTOR_ENGINE.add_data(data_to_add)
#
#     return {"status": "success", "msg": "没有符合条件的帖子需要入库", "added_count": 0}


# def search_recent_posts_by_semantics(keywords, post_manager, top_n=5, recent_hours=24):
#     """
#     语义搜索 + 数据库回表聚合查询：
#     根据自然语言搜索帖子，拉取 MongoDB 中的完整记录，并强制过滤近期时间。
#
#     :param keywords: 搜索关键词 (str 或 list)
#     :param post_manager: UniversalPostManager 实例，用于操作数据库
#     :param top_n: 最终需要返回的记录数量
#     :param recent_hours: 最近时间范围，单位：小时 (默认 24h)
#     :return: list[dict], 包含完整数据库信息与向量相似度的结果列表
#     """
#     # ---------------------------------------------------------
#     # 1. 计算时间阈值 (转换为与 publish_time 匹配的 Unix 时间戳/秒)
#     # ---------------------------------------------------------
#     current_time_s = int(time.time())
#     time_threshold_s = current_time_s - (recent_hours * 3600)
#
#     # ---------------------------------------------------------
#     # 2. 向量库初步召回 (放大召回数，防止被时间过滤后数据不够)
#     # ---------------------------------------------------------
#     # 假设放大系数为 3 (可根据你的实际数据产生频率调整)
#     recall_size = top_n * 3
#     logger.info(f"[语义检索] 开始匹配关键词: {keywords} | 目标返回数: {top_n} | 实际召回数: {recall_size}")
#
#     # 假设 VECTOR_ENGINE 是全局变量或已经初始化的客户端
#     vector_results = VECTOR_ENGINE.search(keywords, top_n=recall_size)
#
#     if not vector_results:
#         logger.info("[语义检索] 未命中任何候选数据。")
#         return []
#
#     # 提取 post_id 列表，并建立 ID -> 相似度信息的映射字典，用于后续组装
#     candidate_ids = []
#     similarity_map = {}
#     for r in vector_results:
#         pid = r['id']
#         candidate_ids.append(pid)
#         similarity_map[pid] = {
#             "similarity": r.get('similarity', 0),
#             "matched_keyword": r.get('matched_keyword', '')
#         }
#
#     # ---------------------------------------------------------
#     # 3. MongoDB 回表查询与时间过滤
#     # ---------------------------------------------------------
#     # 构造复合查询条件：ID 必须在召回列表中，且发布时间 >= 时间阈值
#     query = {
#         "post_id": {"$in": candidate_ids},
#         "publish_time": {"$gte": time_threshold_s}
#     }
#
#     # 直接使用 post_manager 底层的 db 实例执行查询
#     db_records = post_manager.db.find_many(
#         post_manager.collection_name,
#         query=query
#     )
#
#     if not db_records:
#         logger.warning(
#             f"[语义检索] 向量库命中了 {len(candidate_ids)} 条，但在 {recent_hours}h 内的 MongoDB 记录为 0 条。")
#         return []
#
#     # ---------------------------------------------------------
#     # 4. 数据合并与重新排序
#     # ---------------------------------------------------------
#     final_results = []
#     for record in db_records:
#         pid = record.get("post_id")
#         if pid in similarity_map:
#             # 将向量库的"相似度"等衍生数据，无缝贴回到数据库的原始记录中
#             record["_semantic_info"] = similarity_map[pid]
#             final_results.append(record)
#
#     # 关键点：MongoDB 使用 $in 查询返回的数据通常是无序的！
#     # 必须根据向量库赋予的相似度分值 (similarity) 重新从高到低排序
#     final_results.sort(
#         key=lambda x: x.get("_semantic_info", {}).get("similarity", 0),
#         reverse=True
#     )
#
#     # 截取最终用户需要的 top_n
#     final_results = final_results[:top_n]
#
#     logger.info(f"[语义检索] 流程结束 | 最终返回 {len(final_results)} 条，满足 {recent_hours}h 内的时间约束。")
#     return final_results

def is_need_formatting(post):
    """
    判断帖子是否满足格式化前置条件。
    采用单项守卫拦截设计（Guard Clause），方便独立注释/开关任一过滤规则。
    """
    # =========================================================================
    # 1. [核心基石 / 永不修改] 幂等性检查：已成功格式化过的帖子直接跳过，防止重复扣费
    # =========================================================================
    if post.get("logic_mul"):
        return False

    # =========================================================================
    # 2. [技术边界 / 极少修改] 格式排他：当前大模型提取链路不支持视频解析
    # =========================================================================
    video_duration = post.get("media", {}).get("video_duration")
    if video_duration and video_duration > 0:
        return False

    # 提取多媒体路径信息（供第 3、4、5 条规则按需消费）
    local_mapping = post.get("media", {}).get("local_mapping", {})
    local_paths = list(local_mapping.values())

    # =========================================================================
    # 3. [IO 安全 / 极少修改] 媒体完整性：若包含媒体，占位符对应的本地文件必须全部存在（防IO崩溃）
    # =========================================================================
    if local_paths and any(not path for path in local_paths):
        return False

    # =========================================================================
    # 4. [资源保护 / 偶尔调整] 媒体数量上限：单帖图片过多会超出上下文或导致浏览器 OOM
    # =========================================================================
    MAX_MEDIA_COUNT = 10
    if len(local_paths) >= MAX_MEDIA_COUNT:
        return False

    # =========================================================================
    # 5. [业务形态 / 灵活调整] 过滤纯文本：
    #    - 默认保持注释：同时支持【纯文本】和【图文】通过。
    #    - 解除注释：切换回旧模式，【仅允许图文】通过。
    # =========================================================================
    # if not local_mapping:
    #     return False

    # =========================================================================
    # 6. [业务策略 / 频繁变动] 时效性过滤：仅处理近 N 天内的数据
    #    - 跑历史存量数据 / 调试全量数据时，直接整块注释掉此规则即可
    # =========================================================================
    publish_time = post.get("publish_time", 0)
    if publish_time > 1e11:
        publish_time /= 1000  # 毫秒兼容转换为秒
    age_hours = (time.time() - publish_time) / 3600
    if age_hours > max_age_hours:
        return False

    # =========================================================================
    # 所有前置守卫校验通过
    # =========================================================================
    return True


def normalize_post_media(post_data):
    """
    清洗帖子文本中的媒体占位符，统一格式并提取映射清单。
    [入参 Shape]: post_data 字典
    [出参 Shape]: 元组 (清洗后文本内容字符串, 本地媒体路径列表, 新占位符到物理路径的映射字典)
    """
    text_content = post_data.get("content", {}).get("text_content") or ""
    local_mapping = post_data.get("media", {}).get("local_mapping", {})

    local_media_list = []
    new_placeholder_mapping = {}
    counters = {"IMAGE": 1, "VIDEO": 1}

    def replace_match(match):
        prefix = "VIDEO" if match.group(1) == "视频" else "IMAGE"
        placeholder = f"[{prefix}_{counters[prefix]}]"
        counters[prefix] += 1

        local_path = local_mapping.get(match.group(2), "")
        local_media_list.append(local_path)
        new_placeholder_mapping[placeholder] = local_path

        return placeholder

    cleaned_text_content = re.sub(
        r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]",
        replace_match,
        text_content
    )

    return cleaned_text_content, local_media_list, new_placeholder_mapping


def check_format_info(json_data, placeholders):
    """
    防御性校验大模型返回的 JSON 数据结构，确保核心业务字段完整、枚举正确且图片映射无误。
    [入参 Shape]: json_data 解析出的外部数据结构 (Dict), placeholders 文章中真实的图片占位符列表 (如 ['[IMAGE_1]', '[IMAGE_2]']，无图传入 [])
    [出参 Shape]: 元组 (是否合法校验布尔值, 错误详情文本)
    """
    # ================= 1. 最外层结构校验 =================
    if not isinstance(json_data, dict):
        return False, "最外层返回结构必须是字典(Dict)"

    if 'evidences' not in json_data:
        return False, "最外层缺失核心节点 'evidences'"

    evidences = json_data['evidences']

    if not isinstance(evidences, list):
        return False, "'evidences' 节点必须是列表(List)"

    # ================= 2. 校验 evidences (逻辑论据单元) =================
    # 【修改点】：补充了新增的 logic_link 和 impact_weight 字段
    evidence_expected_keys = {
        'core_fact', 'logic_link', 'dimension', 'coins',
        'stance', 'impact_weight', 'shelf_life', 'images'
    }

    valid_dimensions = {'K线形态', '技术指标', '链上数据', '资金流', '消息面', '基本面', '情绪判断'}
    valid_evidence_stances = {'看多', '看空', '震荡'}
    valid_shelf_lives = {'hours', 'days', 'weeks', 'long', 'unknown'}

    image_expected_keys = {'image_id', 'image_type', 'context', 'usable', 'unusable_reason'}
    valid_image_types = {'盘面截图', '数据图表', '新闻截图', '社交截图', '收益截图', '梗图表情', '实拍照片', '其他'}

    for i, ev in enumerate(evidences):
        if not isinstance(ev, dict):
            return False, f"evidences 序列第【{i + 1}】项数据异常，不是标准的字典对象"

        # 核心字段完整性与冗余检查
        missing_ev_keys = evidence_expected_keys - ev.keys()
        if missing_ev_keys:
            return False, f"evidences 序列第【{i + 1}】项缺失核心字段: 【{', '.join(missing_ev_keys)}】"

        extra_ev_keys = ev.keys() - evidence_expected_keys
        if extra_ev_keys:
            return False, f"evidences 序列第【{i + 1}】项存在未定义的冗余字段: 【{', '.join(extra_ev_keys)}】"

        # 【新增】：校验 logic_link 类型
        if not isinstance(ev.get('logic_link'), str):
            return False, f"evidences 第【{i + 1}】项 logic_link 必须是字符串(str)"

        # 【新增】：严格校验 impact_weight 的类型与 1-5 范围
        impact_weight = ev.get('impact_weight')
        # 注意：在 Python 中 bool 是 int 的子类，所以需要同时拦截 bool 类型
        if not isinstance(impact_weight, int) or isinstance(impact_weight, bool):
            return False, f"evidences 第【{i + 1}】项 impact_weight 必须是整数(int)"
        if impact_weight < 1 or impact_weight > 5:
            return False, f"evidences 第【{i + 1}】项 impact_weight 取值范围必须在 1 到 5 之间"

        # 枚举值检查
        if ev.get('dimension') not in valid_dimensions:
            return False, f"evidences 第【{i + 1}】项 dimension【{ev.get('dimension')}】不在允许枚举值内"

        if ev.get('stance') not in valid_evidence_stances:
            return False, f"evidences 第【{i + 1}】项 stance【{ev.get('stance')}】不在允许枚举值内"

        if ev.get('shelf_life') not in valid_shelf_lives:
            return False, f"evidences 第【{i + 1}】项 shelf_life【{ev.get('shelf_life')}】不在允许枚举值内"

        # 校验标的数组 (规定不得为空数组)
        coins = ev.get('coins')
        if not isinstance(coins, list) or len(coins) == 0:
            return False, f"evidences 第【{i + 1}】项 coins 必须是非空列表"

        # 校验嵌套的图片节点
        images = ev.get('images')
        if not isinstance(images, list):
            return False, f"evidences 第【{i + 1}】项 images 必须是列表(List)"

        for j, img in enumerate(images):
            if not isinstance(img, dict):
                return False, f"evidences 第【{i + 1}】项的 images 序列第【{j + 1}】项不是字典"

            # 校验图片节点字段完整性
            missing_img_keys = image_expected_keys - img.keys()
            if missing_img_keys:
                return False, f"evidences 第【{i + 1}】项的 images 序列第【{j + 1}】项缺失字段: 【{', '.join(missing_img_keys)}】"

            # 补上图片节点的冗余字段拦截
            extra_img_keys = img.keys() - image_expected_keys
            if extra_img_keys:
                return False, f"evidences 第【{i + 1}】项的 images 序列第【{j + 1}】项存在未定义的冗余字段: 【{', '.join(extra_img_keys)}】"

            # 严格映射：校验引用的图片占位符是否真的存在于原文中
            img_id = img.get('image_id')
            if img_id not in placeholders:
                return False, f"evidences 第【{i + 1}】项引用的 image_id【{img_id}】非法，只能使用原文真实存在的占位符"

            if img.get('image_type') not in valid_image_types:
                return False, f"evidences 第【{i + 1}】项的 images 第【{j + 1}】项 image_type【{img.get('image_type')}】不在枚举值内"

            # 校验图片可用性 (usable / unusable_reason)
            usable = img.get('usable')
            unusable_reason = img.get('unusable_reason')

            if not isinstance(usable, bool):
                return False, f"evidences 第【{i + 1}】项的 images 第【{j + 1}】项 usable 必须是布尔值(bool)"

            if not isinstance(unusable_reason, str):
                return False, f"evidences 第【{i + 1}】项的 images 第【{j + 1}】项 unusable_reason 必须是字符串(str)"

            # 逻辑互斥检验：能用则原因必须为空，不能用则必须说明原因
            if usable is True and unusable_reason.strip() != "":
                return False, f"evidences 第【{i + 1}】项的 images 第【{j + 1}】项 usable 为 true 时，unusable_reason 必须为空字符串"

            if usable is False and unusable_reason.strip() == "":
                return False, f"evidences 第【{i + 1}】项的 images 第【{j + 1}】项 usable 为 false 时，unusable_reason 不能为空"

    # 全部校验通过
    return True, ""

def gen_media_format_info(post):
    """
    调度外部大模型根据图文内容提取格式化元数据，支持有限重试与降级返回。
    [入参 Shape]: post 帖子全量字典
    [出参 Shape]: 校验无误的媒体格式化列表(List[Dict])，在彻底失败后降级返回空字典 {}
    """
    cleaned_text_content, local_media_list, new_placeholder_mapping = normalize_post_media(post)
    prompt = read_file_to_str(PROMPT_FILE_PATH)
    full_prompt = f'{prompt}\n{cleaned_text_content}'
    placeholders = list(new_placeholder_mapping.keys())
    raw_response = ""
    for attempt in range(1, LLM_MAX_RETRIES + 1):
        try:
            error_detail, raw_response = generate_gemini_content_playwright(full_prompt, file_path=local_media_list)

            format_info = string_to_object(raw_response)
            is_valid, error_message = check_format_info(format_info, placeholders)

            if not is_valid:
                raise ValueError(f"返回数据结构未通过防御性校验 -> {error_message}")

            return format_info

        except Exception as e:
            if attempt == LLM_MAX_RETRIES:
                logger.error(
                    f"[大模型/元数据生成] 重试策略耗尽，彻底放弃当前帖子的格式化 "
                    f"| 关键参数: 【当前重试:{attempt}/{LLM_MAX_RETRIES}】 "
                    f"| 结果: 【触发降级机制，返回空数据】 "
                    f"| 原因: 极大概率是大模型持续吐出无法解析或不符合严格结构的数据 ({e}) \n{raw_response}\n"
                )
                return {}

            logger.warning(
                f"[大模型/元数据生成] 接口生成或数据校验发生意外异常，准备进行指数退避重试 "
                f"| 关键参数: 【当前重试:{attempt}/{LLM_MAX_RETRIES}】 "
                f"| 结果: 【休眠 {2 ** attempt} 秒后重试】 "
                f"| 原因: {e} \n{raw_response}\n"
            )
            time.sleep(2 ** attempt)

    return {}



def process_and_save_single_post(post, post_manager):
    """
    工作线程：负责单条帖子的请求与立即落盘（全异步/并发无锁版）
    """
    post_id = post.get('post_id', 'UNKNOWN_ID')

    if not is_need_formatting(post):
        return

    try:
        # 1. 耗时操作：并发调用大模型
        media_format_info = gen_media_format_info(post)

        if media_format_info:
            post['logic_mul'] = media_format_info

            # 2. 马上保存：直接调用 upsert_posts！无需加锁！
            # 因为 post_manager 底层的 PyMongo 自带连接池和线程安全保障
            post_manager.upsert_posts([post])

            logger.info(
                f"[DB/帖子格式化] 解析校验全量通过并完成回写 "
                f"| PostID: {post_id} | 结果: 【成功入库】"
            )
        else:
            logger.warning(
                f"[DB/帖子格式化] 无效解析数据，跳过落盘 "
                f"| PostID: {post_id} | 结果: 【被丢弃】"
            )

    except Exception as e:
        logger.error(f"[单任务执行] 处理帖子 {post_id} 发生异常: {e}", exc_info=True)


def format_image_article():
    """
    后台守护主流程：持续从数据库拉取待格式化帖子，驱动大模型处理元数据后立即回写。
    """
    MAX_CONCURRENCY = 5  # 并发数量为 5

    while True:
        try:
            post_manager = UniversalPostManager(gen_db_object())
            existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)

            if not existing_posts:
                time.sleep(60)
                continue

            # 开启线程池进行并发处理
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                # 遍历帖子，将 帖子数据 和 数据库管理器 传递给子线程
                for post in existing_posts:
                    executor.submit(process_and_save_single_post, post, post_manager)

            # 动态休眠策略：如果拉取数量达到 limit，说明可能有积压，缩短休眠；否则常规休眠
            if len(existing_posts) >= POST_QUERY_LIMIT:
                logger.info("本批次达到 Limit 上限，说明可能存在积压，仅休眠 5 秒后继续...")
                time.sleep(5)
            else:
                logger.info("本批次所有任务处理完毕，进入常规休眠 (3600秒).")
                time.sleep(3600)

        except Exception as e:
            logger.error(
                f"[系统/守护主循环] 格式化核心链路遭遇未捕获全局异常，挂起后重连 "
                f"| 关键参数: 【无】 "
                f"| 结果: 【当前轮次中断，休眠 60 秒后重建 DB 对象重试】 "
                f"| 原因: {e}",
                exc_info=True
            )
            time.sleep(60)


def clear_all_media_format_batch():
    """
    数据清理入口：批量把存量帖子的 promo_comment 字段置空并回写。
    【无出入参】，直接产生副作用：读写 MongoDB。
    """
    post_manager = UniversalPostManager(gen_db_object())
    existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    logger.info(
        f"[数据清理/启动] 拉取待清理帖子完毕 | 关键参数: 【总量: {len(existing_posts)}】 | 结果: 【开始扫描待清理项】")

    posts_to_update = []
    for post in existing_posts:
        if "logic_mul" in post and post["logic_mul"] is not None:
            post["logic_mul"] = None
            posts_to_update.append(post)

    if posts_to_update:
        post_manager.upsert_posts(posts_to_update)
        logger.info(f"[数据清理/批量落库] 推广评论字段清空完成 | 结果: 【实际更新: {len(posts_to_update)} 条】")
    else:
        logger.info("[数据清理/批量落库] 无需清理 | 结果: 【实际更新: 0 条】")


def process_posts(post_list):
    result = []
    # 匹配各类多媒体占位符的正则
    pattern = re.compile(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]")

    for idx, post in enumerate(post_list, 1):
        # 1. 生成更加简洁的 doc_id，例如 "d_01", "d_02"
        doc_id = f"d_{idx}"

        # 提取原文内容和图片详细数据
        text_content = post.get("content", {}).get("text_content", "")
        media_images = post.get("logic_mul", {}).get("images", [])

        content_list = []
        last_end = 0  # 记录上一段匹配结束的索引
        image_idx = 0  # 追踪当前使用到的图片索引
        video_idx = 0  # 如果有视频的话，单独追踪视频索引

        # 2. 遍历文本中所有匹配到的图片/视频占位符
        for match in pattern.finditer(text_content):
            media_type_str = match.group(1)

            # 截取匹配项之前的纯文本内容
            text_part = text_content[last_end:match.start()].strip()
            if text_part:  # 如果文本不为空，加入到 content 中
                content_list.append({
                    "type": "text",
                    "text": text_part
                })

            # 判断当前是图片还是视频，并生成对应的新占位符
            is_video = (media_type_str == "视频")

            # 获取对应的描述数据
            desc, ocr, logic = "", "", ""

            if not is_video and image_idx < len(media_images):
                # 提取对应图片的结构化信息
                img_data = media_images[image_idx]
                desc = img_data.get("visual_fact", {}).get("fact", "")
                ocr = img_data.get("visual_fact", {}).get("ocr_text", "")
                logic = img_data.get("semantic_core", {}).get("message", "")

                image_idx += 1
                placeholder = f"[IMG_{doc_id}_IMAGE_{image_idx:02d}]"
            elif is_video:
                video_idx += 1
                placeholder = f"[VID_{doc_id}_VIDEO_{video_idx:02d}]"
            else:
                # 兜底：如果原文里的 [插图] 数量多于 json 解析的 images 数组长度
                image_idx += 1
                placeholder = f"[IMG_{doc_id}_IMAGE_{image_idx:02d}]"

            # 添加图片/视频对象到 content
            content_list.append({
                "type": "image" if not is_video else "video",
                "desc": desc,
                "ocr": ocr,
                "logic": logic,
                "placeholder": placeholder
            })

            # 更新游标
            last_end = match.end()

        # 3. 处理最后剩余的文本（末尾最后一张图片后面的文本）
        remaining_text = text_content[last_end:].strip()
        if remaining_text:
            content_list.append({
                "type": "text",
                "text": remaining_text
            })

        # 将当前文档对象追加到总结果
        result.append({
            "doc_id": doc_id,
            "content": content_list
        })

    return result


def build_source_image_mapping(post):
    """复用原占位符编号规则，恢复 [IMAGE_n] 对应的原始 URL 和本地路径。"""
    _, _, normalized_mapping = normalize_post_media(post)
    matches = re.finditer(
        r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]",
        post.get('content', {}).get('text_content') or ''
    )
    return {
        image_id: {
            'original_placeholder': match.group(0),
            'original_url': match.group(2),
            'local_path': local_path or None
        }
        for (image_id, local_path), match in zip(normalized_mapping.items(), matches)
        if image_id.startswith('[IMAGE_')
    }


def transform_mlus(mlu_list):
    """
    清洗并重组 MLU 列表，为第二阶段大模型生成极简的 prompt 喂料。

    Args:
        mlu_list (list): 数据库捞取并按照条件过滤后的原始 MLU 字典列表。

    Returns:
        tuple:
            - cleaned_data (list): 丢给大模型的纯净论据列表。
            - image_mapping (dict): 图片映射表，用于大模型生成后，纯代码层替换真实的图片 URL。
    """
    cleaned_data = []
    image_mapping = {}
    asset_counter = 1  # 全局图片占位符计数器
    asset_by_source = {}
    id_count = 1
    for mlu in mlu_list:
        # 1. 过滤出该 MLU 中所有可用 (usable == True) 的图片
        usable_images = [img for img in mlu.get('images', []) if img.get('usable') is True]

        visual_evidence = None

        # 2. 一张真实图片对应一个占位符；多图以数组表达不可拆分的证据链。
        if usable_images:
            source_mapping = mlu.get('source_image_mapping')
            if source_mapping is None:
                source_mapping = build_source_image_mapping({
                    'content': {'text_content': mlu.get('source_text_content', '')},
                    'media': {'local_mapping': mlu.get('source_local_mapping') or {}}
                })
            chain_is_complete = all(
                img.get('image_id') in source_mapping
                and isinstance(img.get('context'), str) and img['context'].strip()
                for img in usable_images
            )
            if chain_is_complete:
                visual_items = []
                seen_image_ids = set()
                for img in usable_images:
                    image_id = img['image_id']
                    if image_id in seen_image_ids:
                        continue
                    seen_image_ids.add(image_id)
                    source_key = (mlu.get('source_post_id'), image_id)
                    new_placeholder = asset_by_source.get(source_key)
                    if new_placeholder is None:
                        new_placeholder = f"[ASSET_IMG_{asset_counter}]"
                        asset_counter += 1
                        asset_by_source[source_key] = new_placeholder
                        image_mapping[new_placeholder] = {
                            'source': BINANCE_SOURCE,
                            'source_post_id': mlu.get('source_post_id'),
                            'original_image_id': image_id,
                            'original_image_id_list': [image_id],
                            **source_mapping[image_id]
                        }
                    visual_items.append({
                        'placeholder': new_placeholder,
                        'what_it_shows': img['context']
                    })
                visual_evidence = visual_items[0] if len(visual_items) == 1 else visual_items
            else:
                # 缺失任一张映射/描述时，整组不提供配图，但保留文字论据。
                logger.warning("[文章/配图] 证据链无法完整映射，取消该组配图 | post_id=%s",
                               mlu.get('source_post_id'))

        # 3. 构建极简的纯净数据，丢弃所有工程判断字段 (shelf_life, impact_weight, publish_time 等)
        cleaned_mlu = {
            # "dimension": mlu.get('dimension', ''),
            "id": f"M{id_count}",
            "fact": mlu.get('core_fact', ''),
            "underlying_logic": mlu.get('logic_link', ''),
            "visual_evidence": visual_evidence
        }
        id_count += 1
        cleaned_data.append(cleaned_mlu)

    return cleaned_data, image_mapping

def extract_and_group_valid_evidences():
    """
    [功能摘要]
    拉取全量帖子数据，提取出其中通过 LLM 格式化的最小逻辑单元 (evidences)，
    根据时效性 (shelf_life & publish_time) 过滤掉过期的论据（且最长不超过1个月）。
    最后，将论据按「关联币种(coin)」拆分，并按「立场(stance)」进行二维分组。

    [出参结构]
    Dict[coin(str), Dict[stance(str), List[Evidence(dict)]]]
    例如:
    {
        "BTC": {
            "看多": [{evidence_1}, {evidence_2}],
            "看空": [{evidence_3}]
        },
        "SOL": {
            ...
        }
    }
    """
    post_manager = UniversalPostManager(gen_db_object())
    # 拉取数据库中所有相关源的帖子
    existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)

    logger.info(f"[论据提取/启动] 拉取帖子总量: {len(existing_posts)} | 准备进行有效性判定与币种分组...")

    current_time = time.time()

    # 1. 定义时效映射字典 (将 shelf_life 转换为有效的秒数)
    shelf_life_map = {
        'hours': 24 * 3600,  # 24小时
        'days': 7 * 24 * 3600,  # 7天 (7 * 24小时)
        'weeks': 30 * 24 * 3600,  # 30天 (近似1个月)
        'long': 30 * 24 * 3600,  # 长期也受限于1个月硬限制
        'unknown': 24 * 3600  # 未知默认按最短的24小时处理以防干扰
    }

    # 定义绝对时间上限：1个月（30天）
    MAX_AGE_SECONDS = 30 * 24 * 3600

    # 使用 defaultdict 初始化嵌套字典：grouped_results[coin][stance] = list
    grouped_results = defaultdict(lambda: defaultdict(list))

    valid_evidence_count = 0  # 统计最终有效论据总数

    # 2. 开始遍历帖子，提取并过滤论据
    for post in existing_posts:
        logic_mul = post.get('logic_mul')
        # 如果没有格式化数据，或格式化数据没有 evidences 节点，直接跳过
        if not logic_mul or not isinstance(logic_mul, dict) or not logic_mul.get('evidences'):
            continue

        evidences = logic_mul.get('evidences', [])

        # 处理帖子发布时间
        publish_time = post.get('publish_time')
        if not publish_time:
            continue

        try:
            publish_time = float(publish_time)
            # 防御性编程：如果是毫秒级时间戳，转换为秒
            if publish_time > 1e11:
                publish_time /= 1000
        except (ValueError, TypeError):
            continue

        # 计算帖子年龄（秒）
        age_seconds = current_time - publish_time

        # 规则 1：过滤绝对过期数据。如果帖子发布时间超过1个月，或者出现异常的未来时间(防误差容忍3600秒)，直接整帖丢弃
        if age_seconds > MAX_AGE_SECONDS or age_seconds < -3600:
            continue

        # 提前提取原帖文本，注入到论据中，方便下游处理配图和组装文章
        raw_text_content = post.get('content', {}).get('text_content', '')
        post_id = post.get('post_id', 'UNKNOWN')
        source_image_mapping = build_source_image_mapping(post)

        # 3. 遍历提取出来的所有论据单元 (MLU)
        for ev in evidences:
            shelf_life = ev.get('shelf_life', 'unknown')
            valid_duration = shelf_life_map.get(shelf_life, 24 * 3600)

            # 规则 2：动态有效性判断。帖子年龄超过了该论据的保质期，则判定过期
            if age_seconds > valid_duration:
                continue

            # 提取维度信息
            coins = ev.get('coins', [])
            stance = ev.get('stance')

            # 防御性判断：如果没有币种或立场，无法分组
            if not isinstance(coins, list) or not coins or not stance:
                continue

            # 丰富论据上下文：由于你后续需要脱离原帖对论据进行处理，
            # 必须把原帖 ID、发布时间、甚至原文纯文本绑在论据上，否则论据就成孤岛了
            enriched_ev = ev.copy()
            enriched_ev['source_post_id'] = post_id
            enriched_ev['publish_time'] = publish_time
            enriched_ev['source_text_content'] = raw_text_content
            enriched_ev['source_image_mapping'] = source_image_mapping

            # 规则 3：按币种进行多重分发（展平）
            # 假如 coins 是 ["BTC", "ETH"]，这个论据会同时被放进 BTC 和 ETH 的列表中
            for coin in coins:
                coin = str(coin).strip().upper()  # 标准化处理
                if not coin:
                    continue

                # 执行双重分组：按 coin -> 按 stance
                grouped_results[coin][stance].append(enriched_ev)
                valid_evidence_count += 1

    # 为了去除 defaultdict 属性（方便后续序列化或打印），将其转化为普通字典
    final_dict = json.loads(json.dumps(grouped_results))

    logger.info(
        f"[论据提取/完成] 数据清洗分组完毕 | "
        f"获得有效论据总数: {valid_evidence_count} | "
        f"涉及币种数量: {len(final_dict.keys())}"
    )

    # 注意：旧版本这里的 clean_data = process_posts(...) 已不适用，
    # 因为现在分组的最底层数据是【论据(evidence)列表】而不是【帖子(post)列表】了。
    # 具体的组合和清理逻辑应该交由下游负责接收此 Dict 的函数去执行。
    for coin, stances_dict in grouped_results.items():
        for stance, ev_list in stances_dict.items():
            # key 指定根据 impact_weight 排序，防御性使用 get(,0) 防止部分脏数据缺失该字段
            # reverse=True 表示从大到小（降序）
            ev_list.sort(key=lambda x: x.get('impact_weight', 0), reverse=True)

        # 转化为普通字典，剥离 defaultdict 属性
    final_dict = json.loads(json.dumps(grouped_results))

    logger.info(
        f"[论据提取/完成] 数据清洗、分组与【权重排序】完毕 | "
        f"获得有效论据总数: {valid_evidence_count} | "
        f"涉及币种数量: {len(final_dict.keys())}"
    )

    return final_dict

def check_article_info(article_info, materials, image_mapping, max_chars):
    """校验模型输出、真实素材引用、字数和图片证据链；通过后才允许计数入库。"""
    expected_keys = {'status', 'text', 'image_placeholders', 'used_material_ids', 'score', 'reason'}
    if not isinstance(article_info, dict) or set(article_info) != expected_keys:
        return False, "文章必须严格包含 status/text/image_placeholders/used_material_ids/score/reason"
    if article_info['status'] not in ('ok', 'skip'):
        return False, "文章 status 必须为 ok 或 skip"
    if not isinstance(article_info['text'], str):
        return False, "文章 text 必须为字符串"
    for key in ('image_placeholders', 'used_material_ids'):
        values = article_info[key]
        if (not isinstance(values, list)
                or any(not isinstance(value, str) or not value for value in values)):
            return False, f"{key} 必须为字符串列表"
        if len(values) != len(set(values)):
            return False, f"{key} 不允许重复"

    if article_info['status'] == 'skip':
        if (article_info['text'] != '' or article_info['image_placeholders']
                or article_info['used_material_ids'] or article_info['score'] is not None):
            return False, "skip 结果必须清空正文、图片、采用素材，score 必须为 null"
        if not isinstance(article_info['reason'], str) or not article_info['reason'].strip():
            return False, "skip 结果必须说明 reason"
        return True, ''

    if not article_info['text'].strip():
        return False, "ok 结果正文不能为空"
    if len(re.sub(r'\s', '', article_info['text'])) > max_chars:
        return False, f"正文非空白字符数超过 {max_chars}"
    if re.search(r'\[(?:ASSET_IMG|IMAGE|VIDEO)_\d+\]', article_info['text']):
        return False, "正文不能插入图片或视频占位符"
    score = article_info['score']
    if type(score) is not int or not 0 <= score <= 10:
        return False, "ok 结果 score 必须为 0—10 的整数"
    if article_info['reason'] is not None:
        return False, "ok 结果 reason 必须为 null"

    material_by_id = {item['id']: item for item in materials}
    used_ids = article_info['used_material_ids']
    if not 1 <= len(used_ids) <= 3 or any(item not in material_by_id for item in used_ids):
        return False, "used_material_ids 必须引用输入中实际存在的 1—3 条素材"
    selected_images = article_info['image_placeholders']
    if len(selected_images) > 3:
        return False, "最多只能使用 3 张真实图片"

    allowed_images = set()
    for material_id in used_ids:
        visual_evidence = material_by_id[material_id]['visual_evidence']
        if not visual_evidence:
            continue
        chain = visual_evidence if isinstance(visual_evidence, list) else [visual_evidence]
        chain_ids = [item['placeholder'] for item in chain]
        allowed_images.update(chain_ids)
        chosen_chain = [item for item in selected_images if item in chain_ids]
        if chosen_chain and chosen_chain != chain_ids:
            return False, f"素材 {material_id} 的图片证据链必须整组使用，并保留输入顺序"
    if any(item not in allowed_images or item not in image_mapping for item in selected_images):
        return False, "图片必须来自已采用素材，且必须具有真实的图片映射"
    return True, ''


def generate_and_save_analysis_article(coin, stance, ev_list, article_manager):
    """生成一个币种/立场分组的文章。仅 ok 结果入库一次，skip/error 直接返回。"""
    creation_brief = {
        'task': {
            'topic': coin,
            'stance': stance,
            'max_chars': ARTICLE_MAX_CHARS,
            'recent_openings': []
        },
        'materials': []
    }
    model_name = "gemini-3.1-pro-preview"
    prompt_version = "0920v1.0"
    # 生成和重试期间只在内存中维护记录，成功后再统一入库。
    record = {
        'source': BINANCE_SOURCE,
        'topic': coin,
        'stance': stance,
        'status': 'processing',
        'creation_brief': creation_brief,
        'material_post_mapping': {},
        'prompt_file_path': ARTICLE_PROMPT_FILE_PATH,
        'post_id_list': [],
        'article_info': None,
        'error_message': None,
        'error_history': [],
        'raw_response': None,
        "model_name": model_name,
        "prompt_version": prompt_version,
        'attempt_count': 0,
    }
    try:
        candidates = [
            ev for ev in ev_list
            if isinstance(ev, dict)
            and isinstance(ev.get('source_post_id'), (str, int))
            and not isinstance(ev.get('source_post_id'), bool)
            and ev.get('source_post_id') not in (None, '', 'UNKNOWN')
            and isinstance(ev.get('core_fact'), str) and ev['core_fact'].strip()
        ]
        candidate_post_ids = list(dict.fromkeys(ev['source_post_id'] for ev in candidates))
        # 每个分组都重新读库，后续分组能看到本轮前面已成功入库的引用次数。
        usage_counts = article_manager.get_post_usage_counts(BINANCE_SOURCE, candidate_post_ids)
        selected_evidences = [
            ev for ev in candidates
            if usage_counts.get(ev['source_post_id'], 0) <= ARTICLE_POST_USAGE_LIMIT
        ][:ARTICLE_MATERIAL_LIMIT]

        if not selected_evidences:
            record['status'] = 'skip'
            record['article_info'] = {
                'status': 'skip',
                'text': '',
                'image_placeholders': [],
                'used_material_ids': [],
                'score': None,
                'reason': '本分组没有有效论据，或原帖使用次数均已超过允许阈值。',
                'image_mapping': {}
            }
        else:
            materials, image_mapping = transform_mlus(selected_evidences)
            creation_brief['materials'] = materials
            creation_brief['task']['recent_openings'] = article_manager.get_recent_openings(
                BINANCE_SOURCE, coin, stance
            )
            record['material_post_mapping'] = {
                material['id']: evidence['source_post_id']
                for material, evidence in zip(materials, selected_evidences)
            }
            prompt = read_file_to_str(ARTICLE_PROMPT_FILE_PATH)
            if not isinstance(prompt, str) or not prompt.strip():
                raise ValueError(f"文章生成提示词为空或读取失败: {ARTICLE_PROMPT_FILE_PATH}")
            # 原提示词原样使用；其末尾已经包含“下面是本次创作简报”。
            full_prompt = f'{prompt}\n{json.dumps(creation_brief, ensure_ascii=False)}'

            for attempt in range(1, LLM_MAX_RETRIES + 1):
                record['attempt_count'] = attempt
                record['raw_response'] = None
                try:
                    # 本阶段只传素材文字和图片描述，不再上传原图。
                    error_detail, raw_response = generate_gemini_content_playwright(
                        full_prompt, model_name=model_name
                    )
                    record['raw_response'] = raw_response
                    if error_detail:
                        raise RuntimeError(f"文章生成接口返回异常: {error_detail}")
                    article_info = string_to_object(raw_response)
                    valid, error_message = check_article_info(
                        article_info, materials, image_mapping, ARTICLE_MAX_CHARS
                    )
                    if not valid:
                        raise ValueError(error_message)
                    # 模型只输出原来的六个字段；真实图片映射由代码在校验后追加。
                    article_info['image_mapping'] = {
                        placeholder: dict(image_mapping[placeholder])
                        for placeholder in article_info['image_placeholders']
                    }
                    record['article_info'] = article_info
                    record['status'] = article_info['status']
                    record['error_message'] = None
                    break
                except Exception as exc:
                    record['error_history'].append({
                        'attempt': attempt,
                        'error_message': f'{type(exc).__name__}: {exc}',
                        'raw_response': record['raw_response']
                    })
                    if attempt == LLM_MAX_RETRIES:
                        raise
                    logger.warning(
                        "[文章/生成] 准备重试 | topic=%s | stance=%s | attempt=%s/%s | error=%s",
                        coin, stance, attempt, LLM_MAX_RETRIES, exc
                    )
                    time.sleep(2 ** attempt)
    except Exception as exc:
        record['status'] = 'error'
        record['article_info'] = None
        record['error_message'] = f'{type(exc).__name__}: {exc}'
        logger.error("[文章/生成失败] topic=%s | stance=%s | error=%s",
                     coin, stance, exc, exc_info=True)

    # 仅成功时落库一次；跳过、失败和中间重试均不写数据库。
    # 落库仍放在模型重试之外，避免数据库写入异常触发模型重复生成。
    # 写入失败向上传播，停止本轮；不能继续按可能过时的使用次数生成其他分组。
    if record['status'] == 'ok':
        return article_manager.save_article(record)
    return record

def generate_analysis_articles_once():
    """执行一轮：刷新有效论据池，遍历 final_dict，每个币种/立场最多生成一篇。"""
    logger.info("[文章/本轮启动] 正在提取有效论据，准备统计本轮生成任务...")
    final_dict = extract_and_group_valid_evidences()

    # 生成前统一打印本轮规模；这里统计的是使用次数过滤前的池子。
    group_count = 0
    non_empty_group_count = 0
    evidence_count = 0
    for stances_dict in final_dict.values():
        for ev_list in stances_dict.values():
            group_count += 1
            evidence_count += len(ev_list)
            if ev_list:
                non_empty_group_count += 1

    # 每个非空分组正常调用一次；达到使用次数限制等情况会跳过模型调用。
    # LLM_MAX_RETRIES 是每组最多尝试次数，已经包含第一次调用。
    logger.info(
        "[文章/本轮计划] 币种数=%s | 分组数=%s | 非空分组数=%s | "
        "论据条目数=%s（含跨组重复） | 不含重试的模型调用上限=%s次 | "
        "含重试的模型调用上限=%s次（每组最多%s次）",
        len(final_dict), group_count, non_empty_group_count, evidence_count,
        non_empty_group_count, non_empty_group_count * LLM_MAX_RETRIES, LLM_MAX_RETRIES
    )
    logger.info(
        "[文章/调用估算] 每组先剔除原帖使用次数超过%s次的论据，再取前%s条；"
        "过滤后无可用素材的分组不调用模型，因此实际调用次数可能低于上述上限。",
        ARTICLE_POST_USAGE_LIMIT, ARTICLE_MATERIAL_LIMIT
    )

    article_manager = GeneratedArticleManager(gen_db_object())
    results = []
    # 单个文章进程内串行处理，确保先落库、后统计下一个分组的使用次数。
    # 多实例部署若要严格限制全局次数，需要另外提供数据库锁/事务能力。
    for coin, stances_dict in final_dict.items():
        for stance, ev_list in stances_dict.items():
            results.append(generate_and_save_analysis_article(coin, stance, ev_list, article_manager))
    logger.info("[文章/本轮完成] total=%s | ok=%s | skip=%s | error=%s",
                len(results), sum(item['status'] == 'ok' for item in results),
                sum(item['status'] == 'skip' for item in results),
                sum(item['status'] == 'error' for item in results))
    return results


def generate_analysis_articles():
    """独立文章生成进程：定期刷新池子；暂无任何分组时每 60 秒重试。"""
    while True:
        try:
            results = generate_analysis_articles_once()
            time.sleep(ARTICLE_GENERATION_INTERVAL_SECONDS if results else 60)
        except Exception:
            logger.exception("[文章/守护进程] 本轮失败，60 秒后重新查询数据库")
            time.sleep(60)


def get_all_non_empty_logic_mul_with_clean_text():
    """
    数据查询：获取数据库中所有不为空的 logic_mul 字段，并打包带有清洗后（无图片、视频占位符）的原始文本。
    返回时进行排序：将包含有效图片的记录优先排在列表前面。
    [出参 Shape]: List[Dict]，数据结构形如：
                  [
                      {
                          "text_content": "清洗后的纯净文本...",
                          "logic_mul": { 具体的逻辑块数据... }
                      },
                      ...
                  ]
    """
    post_manager = UniversalPostManager(gen_db_object())

    # 沿用原代码的批量拉取规范
    existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)

    valid_data_list = []

    for post in existing_posts:
        logic_mul = post.get("logic_mul")

        # 只要 logic_mul 有效，就提取并清洗对应的文本字段
        if logic_mul:
            # 1. 按照既有数据结构，安全地获取原始正文文本
            raw_text = post.get("content", {}).get("text_content", "")
            # post_id = post.get("post_id", "UNKNOWN_ID")

            # 2. 文本清洗：利用项目原生正则，去除 [插图: http...] / [视频: http...] 等占位符
            cleaned_text = re.sub(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]", "", raw_text).strip()

            # 3. 将清洗后的文本和 logic_mul 组合存入列表
            valid_data_list.append({
                # "post_id":post_id,
                "text_content": cleaned_text,
                "logic_mul": logic_mul
            })

    logger.info(
        f"[数据提取/logic_mul及纯文本] 初步提取完毕 | "
        f"关键参数: 【扫描帖子总量: {len(existing_posts)}】 | "
        f"结果: 【提取到有效数据组数: {len(valid_data_list)}】"
    )

    # ==================== 新增：排序逻辑 ====================
    # 考虑到整个项目中 logic_mul 数据结构可能存在多种版本（字典嵌套 evidences / 直接含 images / 列表）
    # 编写一个兼容的向下探测函数，只要任意结构中包含有效图片，即判定为 True
    def has_images(logic_data):
        if not logic_data:
            return False

        if isinstance(logic_data, dict):
            # 场景 A: 兼容 process_posts 中的逻辑 (logic_mul["images"])
            images_list = logic_data.get("images")
            if isinstance(images_list, list) and len(images_list) > 0:
                return True

            # 场景 B: 兼容 check_format_info 中的逻辑 (logic_mul["evidences"][i]["images"])
            evidences = logic_data.get("evidences")
            if isinstance(evidences, list):
                for ev in evidences:
                    if isinstance(ev, dict):
                        ev_images = ev.get("images")
                        if isinstance(ev_images, list) and len(ev_images) > 0:
                            return True

        # 场景 C: 兼容 build_search_text 中的逻辑 (logic_mul 本身就是一个列表)
        elif isinstance(logic_data, list):
            return len(logic_data) > 0

        return False

    # 根据是否包含 images 进行排序。
    # has_images 为 True 映射为 0 (排在前面)，为 False 映射为 1 (排在后面)
    valid_data_list.sort(key=lambda x: 0 if has_images(x["logic_mul"]) else 1)

    logger.info("[数据提取/logic_mul及纯文本] 已完成按照 '含图片数据优先' 规则的重新排序。")

    return valid_data_list


import os
import re
import time
from datetime import datetime, timedelta, timezone

def auto_publish_articles():
    """
    后台守护主流程：自动发布符合条件的无图帖子到币安广场。
    每轮检查休眠 10 分钟，无限循环。
    """
    STATE_FILE = "account_publish_state.json"
    ACCOUNTS = ["yang", "ruru"]

    # 账号冷却时间 (1小时) 和 同Topic防重时间 (12小时)
    ACCOUNT_COOLDOWN_SECONDS = 3600
    TOPIC_COOLDOWN_SECONDS = 12 * 3600

    db_client = gen_db_object()
    article_manager = GeneratedArticleManager(db_client)

    logger.info("[自动发布] 启动帖子自动发布守护进程...")

    while True:
        try:
            # 用于调度判断的时间戳
            now_timestamp = time.time()

            # 1. 载入本地账号状态
            if os.path.exists(STATE_FILE):
                state = read_json(STATE_FILE) or {}
            else:
                state = {}

            # 初始化账号结构
            for acc in ACCOUNTS:
                if acc not in state:
                    state[acc] = {
                        "total_success": 0,
                        "last_publish_time": 0,
                        "last_error_msg": "",
                        "last_error_time": 0,
                        "topic_publish_history": {}
                    }

            # 2. 查询 MongoDB，过滤 6h 内生成的，且未被成功发布的记录
            # 【修复】：根据 GeneratedArticleManager，使用 UTC datetime 对象进行查询
            six_hours_ago_dt = datetime.now(timezone.utc) - timedelta(hours=6)

            query = {
                "created_at": {"$gte": six_hours_ago_dt},
                "status": "ok",
                "publish_status": {"$ne": "success"}  # 不等于 success，代表没发过或失败过
            }

            # 从数据库中拉取潜在的文章候选列表
            candidates = article_manager.db.find_many(article_manager.collection_name, query=query)
            if not candidates:
                candidates = []

            # 3. 内存进行精细化过滤：过滤出无图片的帖子
            valid_articles = []
            for art in candidates:
                article_info = art.get("article_info", {})
                if not isinstance(article_info, dict):
                    continue

                # 条件：image_placeholders 没有使用图片（列表为空）
                images = article_info.get("image_placeholders", [])
                if len(images) > 0:
                    continue

                valid_articles.append(art)

            # 4. 总体打印统计信息
            logger.info(f"[自动发布] =========================================")
            logger.info(f"[自动发布] 统计信息：近 6h 内待发布(无图)帖子数量 -> {len(valid_articles)}")

            # 5. 按照 score 从大到小排序
            valid_articles.sort(key=lambda x: x.get("article_info", {}).get("score", 0), reverse=True)

            # 6. 发布分配逻辑
            for acc in ACCOUNTS:
                # 检查该账号是否还在 1 小时的发帖冷却期内
                last_pub_time = state[acc].get("last_publish_time", 0)
                if now_timestamp - last_pub_time < ACCOUNT_COOLDOWN_SECONDS:
                    logger.info(f"[自动发布] 账号【{acc}】正在冷却中 (距离下次可用还剩 {int(ACCOUNT_COOLDOWN_SECONDS - (now_timestamp - last_pub_time))} 秒)")
                    continue

                if not valid_articles:
                    logger.info(f"[自动发布] 账号【{acc}】暂无可用的帖子候选。")
                    continue

                # 在候选列表中为该账号寻找符合 "12小时内未发过该 topic" 的最佳帖子
                selected_art = None
                selected_idx = -1
                for idx, art in enumerate(valid_articles):
                    topic = art.get("topic", "")
                    last_topic_pub_time = state[acc]["topic_publish_history"].get(topic, 0)

                    if now_timestamp - last_topic_pub_time >= TOPIC_COOLDOWN_SECONDS:
                        selected_art = art
                        selected_idx = idx
                        break

                if not selected_art:
                    logger.info(f"[自动发布] 账号【{acc}】当前的高分帖子因为【12小时内发过同Topic】防重限制，已被跳过。")
                    continue

                # 7. 文本重构处理
                article_info = selected_art.get("article_info", {})
                original_text = article_info.get("text", "")
                topic = selected_art.get("topic", "")

                if topic:
                    # 使用 \b 确保是完整单词，(?<!\$) 防止把已经写了 $DOGE 的变成 $$DOGE
                    pattern = rf"(?<!\$)\b{re.escape(topic)}\b"
                    formatted_text = re.sub(pattern, f"${topic}", original_text, flags=re.IGNORECASE)

                    # 统一增加标签，追加到文末
                    formatted_text = f"{formatted_text}\n\n#{topic}"
                else:
                    formatted_text = original_text

                logger.info(f"[自动发布] 账号【{acc}】准备发布帖子 | Topic: {topic} | Score: {article_info.get('score')} | ID: {selected_art.get('_id')}")

                # 8. 执行发布
                api_key = get_config(f'{acc}_square_api_key')
                if not api_key:
                    logger.error(f"[自动发布] 找不到账号【{acc}】的 API KEY 配置！请检查配置文件。")
                    continue

                is_success = publish_to_binance_square(api_key=api_key, text_content=formatted_text)

                if is_success:
                    logger.info(f"[自动发布] 🎉 账号【{acc}】发布成功！")
                    # 更新状态并落盘 (记录时间戳)
                    state[acc]["total_success"] += 1
                    state[acc]["last_publish_time"] = now_timestamp
                    if topic:
                        state[acc]["topic_publish_history"][topic] = now_timestamp
                    save_json(STATE_FILE, state)

                    # 回写数据库，标记成功
                    article_manager.db.update_one(
                        article_manager.collection_name,
                        {"_id": selected_art["_id"]},
                        {"$set": {"publish_status": "success", "published_by": acc, "publish_time": now_timestamp}}
                    )

                    # 将这篇刚发出去的帖子从待发队列中移除，避免其他账号重复发
                    valid_articles.pop(selected_idx)
                else:
                    error_msg = "发帖失败（可能是网络不通、Key失效或达到每日上限）"
                    logger.error(f"[自动发布] ❌ 账号【{acc}】发布失败: {error_msg}")

                    # 更新报错记录落盘
                    state[acc]["last_error_msg"] = error_msg
                    state[acc]["last_error_time"] = now_timestamp
                    save_json(STATE_FILE, state)

                    # 回写数据库，记录失败状态供之后重试
                    article_manager.db.update_one(
                        article_manager.collection_name,
                        {"_id": selected_art["_id"]},
                        {"$set": {"publish_status": "failed", "last_error": error_msg}}
                    )

        except Exception as e:
            logger.error(f"[自动发布] 轮询主循环遭遇异常: {e}", exc_info=True)

        finally:
            logger.info("[自动发布] 本轮处理结束，进入 10 分钟 (600s) 休眠...\n")
            time.sleep(600)

def _run_task(task):
    """为未处理异常补充任务入口信息后重抛，保留当前线程退出、不自动重启的行为。"""
    try:
        task()
    except Exception as exc:
        logger.error(
            f"[任务/退出] 任务异常退出，请检查对应链路的数据、文件权限和外部服务"
            f" | 任务: 【{task.__name__}】 | 异常: 【{exc!r}】 | 结果: 【当前线程停止】"
        )
        raise


if __name__ == "__main__":
    tasks = (
        generate_analysis_articles,
        format_image_article,
        auto_publish_articles
    )
    threads = []
    for task in tasks:
        thread = threading.Thread(target=_run_task, args=(task,), name=task.__name__)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()
