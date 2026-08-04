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

import re
import time
from collections import defaultdict

from common.common_utils import setup_logger, read_file_to_str, string_to_object
# from common.vector_utils import VectorSearchEngine

logger = setup_logger(app_name="media_format")

from app.ai_api.gemini_playwright import generate_gemini_content_playwright
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager
from concurrent.futures import ThreadPoolExecutor, as_completed
BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
PROMPT_FILE_PATH = r'W:\project\python_project\crypto_trade\prompt\内容生成方案_分析类MLU提取.txt'
LLM_MAX_RETRIES = 3


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
    判断帖子是否满足格式化前置条件：未被格式化且本地媒体文件已全部就绪。
    [入参 Shape]: post 字典
    [出参 Shape]: bool (是否需要处理)
    """
    # 此处遵循保真红线，不擅自修改业务边界。
    if post.get("logic_mul"):
        return False

    local_mapping = post.get("media", {}).get("local_mapping", {})
    if not local_mapping:
        return False

    video_duration = post.get("media", {}).get("video_duration")
    if video_duration and video_duration > 0:
        return False
    local_paths = list(local_mapping.values())
    valid_paths_count = sum(bool(path) for path in local_paths)

    # 必须保证帖子包含媒体，且所有媒体映射到的本地物理路径都不为空
    return valid_paths_count > 0 and valid_paths_count == len(local_paths) and valid_paths_count < 10


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
    [入参 Shape]: json_data 解析出的外部数据结构 (Dict), placeholders 文章中真实的图片占位符列表 (如 ['[IMAGE_1]', '[IMAGE_2]'])
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
    # 【修改点】：加入了新增的 'raw_golden_quote'
    evidence_expected_keys = {
        'claim', 'support', 'raw_golden_quote', 'support_type',
        'dimension', 'coins', 'stance', 'shelf_life', 'images'
    }

    # 【注意】：严格按照上一版 Prompt，保留了 '泛泛而谈'
    valid_support_types = {'具体数据', '具体事件', '图表形态', '泛泛而谈'}
    valid_dimensions = {'K线形态', '技术指标', '链上数据', '资金流', '消息面', '基本面', '情绪判断'}
    valid_evidence_stances = {'看多', '看空', '震荡'}
    valid_shelf_lives = {'hours', 'days', 'weeks', 'long', 'unknown'}

    image_expected_keys = {'image_id', 'image_type', 'context', 'risk'}
    valid_image_types = {'盘面截图', '数据图表', '新闻截图', '社交截图', '收益截图', '梗图表情', '实拍照片', '其他'}
    valid_risks = {'平台或工具水印', 'KOL或他人言论截图', '推广二维码', '个人私密盈亏截图', '人脸', '敏感内容',
                   '图片模糊或关键内容不可读'}

    for i, ev in enumerate(evidences):
        if not isinstance(ev, dict):
            return False, f"evidences 序列第【{i + 1}】项数据异常，不是标准的字典对象"

        # 核心字段完整性检查
        missing_ev_keys = evidence_expected_keys - ev.keys()
        if missing_ev_keys:
            return False, f"evidences 序列第【{i + 1}】项缺失核心字段: 【{', '.join(missing_ev_keys)}】"

        # 数据类型检查（针对新增金句字段的防御）
        if not isinstance(ev.get('raw_golden_quote'), str):
            return False, f"evidences 序列第【{i + 1}】项的 raw_golden_quote 必须是字符串(String)"

        # 枚举值检查
        if ev.get('support_type') not in valid_support_types:
            return False, f"evidences 第【{i + 1}】项 support_type【{ev.get('support_type')}】不在允许枚举值内"

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

            missing_img_keys = image_expected_keys - img.keys()
            if missing_img_keys:
                return False, f"evidences 第【{i + 1}】项的 images 序列第【{j + 1}】项缺失字段: 【{', '.join(missing_img_keys)}】"

            # 严格映射：校验引用的图片占位符是否真的存在于原文中
            img_id = img.get('image_id')
            if placeholders and (img_id not in placeholders):
                return False, f"evidences 第【{i + 1}】项引用的 image_id【{img_id}】非法，只能使用原文存在的真实占位符"

            if img.get('image_type') not in valid_image_types:
                return False, f"evidences 第【{i + 1}】项的 images 第【{j + 1}】项 image_type【{img.get('image_type')}】不在枚举值内"

            # 校验风险标签数组
            risks = img.get('risk')
            if not isinstance(risks, list):
                return False, f"evidences 第【{i + 1}】项的 images 第【{j + 1}】项 risk 必须是列表(List)"

            for risk in risks:
                if risk not in valid_risks:
                    return False, f"evidences 第【{i + 1}】项的 risk 包含了非法枚举值【{risk}】"

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



def build_analysis_content():
    post_manager = UniversalPostManager(gen_db_object())
    existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    logger.info(
        f"[数据清理/启动] 拉取待清理帖子完毕 | 关键参数: 【总量: {len(existing_posts)}】 | 结果: 【开始扫描待清理项】")

    current_time = time.time()
    one_day_seconds = 24 * 60 * 60
    filtered_posts = []

    # 1 & 2. 找到 logic_mul 存在的 posts，并过滤 publish_time 在 1 天内的数据
    for post in existing_posts:
        # 检查 logic_mul 是否存在
        if not post.get('logic_mul'):
            continue

        publish_time = post.get('publish_time')
        if publish_time is not None:
            try:
                # 转换为 float 防御性编程，处理 publish_time 可能是字符串的情况
                # 判断条件：当前时间减去 1天前的时间 <= 发布时间
                if (current_time - one_day_seconds) <= float(publish_time) <= (current_time + 3600):
                    # 注: 加 3600 秒是为了兼容服务器间可能存在的轻微时间误差（如未来时间戳）
                    filtered_posts.append(post)
            except (ValueError, TypeError):
                # 如果 publish_time 格式异常无法转换，则跳过
                continue

    # 3. 按照 main 和 stance 进行双重分组
    # 使用 defaultdict(lambda: defaultdict(list)) 可以自动初始化缺失的嵌套字典
    grouped_results = defaultdict(lambda: defaultdict(list))

    for post in filtered_posts:
        # 安全获取嵌套字典的值，防止因为数据结构不完整抛出 KeyError
        logic_mul = post.get('logic_mul', {})
        doc = logic_mul.get('doc', {})

        main_topic = doc.get('main')
        stance = doc.get('stance')

        # 确保这两个分组键存在才将其加入结果字典
        if main_topic is not None and stance is not None:
            grouped_results[main_topic][stance].append(post)

    # 如果后续需要标准的 dict 格式，可以直接将 grouped_results 当作普通字典返回或转换
    # 打印一下处理结果（可选）
    logger.info(f"[数据清理/分组] 分组处理完成 | 关键参数: 【符合条件的帖子量: {len(filtered_posts)}】")
    clean_data = process_posts(grouped_results['BTC']['看多'])
    return grouped_results


def get_all_non_empty_logic_mul_with_clean_text():
    """
    数据查询：获取数据库中所有不为空的 logic_mul 字段，并打包带有清洗后（无图片、视频占位符）的原始文本。
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

            # 2. 文本清洗：利用项目原生正则，去除 [插图: http...] / [视频: http...] 等占位符
            cleaned_text = re.sub(r"\[(插图|长文封面|视频封面|视频):\s*(https?://[^\]]+)\]", "", raw_text).strip()

            # 3. 将清洗后的文本和 logic_mul 组合存入列表
            valid_data_list.append({
                "text_content": cleaned_text,
                "logic_mul": logic_mul
            })

    logger.info(
        f"[数据提取/logic_mul及纯文本] 提取完毕 | "
        f"关键参数: 【扫描帖子总量: {len(existing_posts)}】 | "
        f"结果: 【提取到有效数据组数: {len(valid_data_list)}】"
    )

    return valid_data_list



if __name__ == "__main__":
    valid_logic_mul_list = get_all_non_empty_logic_mul_with_clean_text()

    # clear_all_media_format_batch()

    # post_manager = UniversalPostManager(gen_db_object())

    # sync_posts_to_vector_db(post_manager)

    # data = search_recent_posts_by_semantics("200倍杠杆", post_manager, top_n=5)
    # build_analysis_content()

    format_image_article()