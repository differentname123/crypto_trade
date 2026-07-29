"""
[功能摘要] 定时扫描币安数据源的图文帖子，利用大模型将帖子内嵌的媒体元素（图片/视频）进行语义化解析与格式化重构。
[输入数据] 从 MongoDB 提取的帖子源数据 (post)，核心依赖 `content.text_content` 及其附属的 `media.local_mapping` (占位符到本地文件路径的映射)。
[数据流转/交互]
1. 轮询读取 MongoDB 中未格式化的帖子数据。
2. 文本清洗：将帖子原有的不规则占位符（如 `[插图:http...]`）统一替换为标准占位符（如 `[IMAGE_01]`），并聚合对应的本地媒体文件路径。
3. AI 交互：携带清洗后的文本和物理媒体路径，调用 Gemini/Playwright 接口进行视觉与文本的联合语义解析。
4. 校验拦截：严格校验大模型返回的 JSON 元数据结构，确保返回的 image_id 与解析的占位符数量及名称做到 1:1 绝对映射。
[输出数据] 将解析并严格校验通过的格式化元数据赋值给 post['media_format']，随后持久化更新至 MongoDB 数据库。
"""

import re
import time
from common.common_utils import setup_logger, read_file_to_str, string_to_object
from common.vector_utils import VectorSearchEngine

logger = setup_logger(app_name="media_format")

from app.ai_api.gemini_playwright import generate_gemini_content_playwright
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager
from concurrent.futures import ThreadPoolExecutor, as_completed
BINANCE_SOURCE = "biance"
POST_QUERY_LIMIT = 50000
PROMPT_FILE_PATH = r'W:\project\python_project\crypto_trade\prompt\内容生成方案_图片文字化.txt'
LLM_MAX_RETRIES = 3


# 全局初始化向量引擎（单例调用，避免重复加载）
VECTOR_ENGINE = VectorSearchEngine(collection_name="binance_posts_index")


def build_search_text(post):
    """
    数据降维与高密度提纯：将复杂的帖子格式化字典，提取拼装为高浓度的“超级搜索文本”
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
    media_format = post.get("media_format", [])
    for i, media in enumerate(media_format):
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


def sync_posts_to_vector_db():
    """
    批量入库函数：将格式化完毕的帖子列表，提取ID和超级文本后，灌入 ChromaDB。
    由于底层的 VectorSearchEngine 已做好防重复校验，可放心重复传入历史数据。
    [入参]: post_list (从 MongoDB 查询出的 post 字典列表)
    [出参]: 执行状态字典
    """
    data_to_add = []
    post_manager = UniversalPostManager(gen_db_object())
    existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)
    # 保留media_format存在的posts
    filter_posts = [post for post in existing_posts if post.get("media_format")]
    logger.info(f"[向量库/同步] 查询到 {len(existing_posts)} 条帖子，其中 {len(filter_posts)} 条已完成格式化，准备入库...")


    for post in filter_posts:
        post_id = post.get("post_id")
        media_format = post.get("media_format")

        # 拦截校验：只有存在 post_id 且已经过大模型格式化的帖子才允许入库
        if not post_id or not media_format:
            continue

        search_text = build_search_text(post)
        data_to_add.append({
            "id": post_id,
            "search_text": search_text
        })

    if data_to_add:
        logger.info(f"[向量库/同步] 准备将 {len(data_to_add)} 条解析完毕的数据送入向量库提取特征...")
        return VECTOR_ENGINE.add_data(data_to_add)

    return {"status": "success", "msg": "没有符合条件的帖子需要入库", "added_count": 0}


def search_posts_by_semantics(keywords, top_n=5):
    """
    语义搜索函数：根据自然语言关键词，搜索匹配的帖子ID。
    [入参]: keywords(单个字符串或列表均可，如 "200倍杠杆"), top_n(返回数量)
    [出参]: 列表，元素结构为 {"id": "xxx", "similarity": 0.85, "matched_keyword": "xxx"}
    """
    logger.info(f"[向量库/查询] 正在通过语义搜索匹配：{keywords}")
    results = VECTOR_ENGINE.search(keywords, top_n=top_n)

    # 打印简要日志以便调试
    for r in results:
        logger.info(f" -> 命中 PostID: {r['id']} | 相似度: {r['similarity']:.4f}")

    return results

def is_need_formatting(post):
    """
    判断帖子是否满足格式化前置条件：未被格式化且本地媒体文件已全部就绪。
    [入参 Shape]: post 字典
    [出参 Shape]: bool (是否需要处理)
    """
    # 此处遵循保真红线，不擅自修改业务边界。
    if post.get("media_format"):
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
        placeholder = f"[{prefix}_{counters[prefix]:02d}]"
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
    防御性校验大模型返回的 JSON 数据结构，确保业务字段完整且映射无误。
    [入参 Shape]: json_data 解析出的外部数据结构, placeholders 生成的占位符键名列表
    [出参 Shape]: 元组 (是否合法校验布尔值, 错误详情文本)
    """
    if not isinstance(json_data, list):
        return False, "最外层返回结构必须是列表(List)"

    if len(json_data) != len(placeholders):
        return False, f"返回的图片节点数量【{len(json_data)}】与所需占位符总数【{len(placeholders)}】不一致"

    valid_image_types = {'photo', 'chart', 'screenshot', 'meme', 'illustration', 'diagram'}
    valid_roles = {'cover', 'evidence', 'data_chart', 'tutorial', 'atmosphere', 'meme', 'decorative'}
    expected_keys = {'image_id', 'image_type', 'visual_fact', 'semantic_core', 'narrative_role'}

    for i, item in enumerate(json_data):
        if not isinstance(item, dict):
            return False, f"序列第【{i + 1}】项数据异常，不是标准的字典对象"

        missing_keys = expected_keys - item.keys()
        if missing_keys:
            return False, f"序列第【{i + 1}】项缺失核心字段: 【{', '.join(missing_keys)}】"

        expected_id = str(placeholders[i]).strip('[]')
        actual_id = item.get('image_id')
        if actual_id != expected_id:
            return False, f"上下文映射错位：序列第【{i + 1}】项的 image_id【{actual_id}】与要求占位符【{expected_id}】未对齐"

        if item.get('image_type') not in valid_image_types:
            return False, f"序列第【{i + 1}】项 image_type【{item.get('image_type')}】不在允许枚举值内"

        narrative_role = item.get('narrative_role', {})
        # 遵循原业务兼容逻辑：仅在它是字典类型时检查枚举（存在不为字典也能逃逸通过的可能）
        if isinstance(narrative_role, dict) and narrative_role.get('role') not in valid_roles:
            return False, f"序列第【{i + 1}】项 narrative_role.role【{narrative_role.get('role')}】不在允许枚举值内"

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
            error_detail, raw_response = generate_gemini_content_playwright(full_prompt, file_path=local_media_list, model_name="gemini-3.1-pro-preview")

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


def process_single_post(post):
    """
    单条帖子的处理任务（工作线程执行）。
    仅处理网络/CPU耗时操作，不直接操作数据库。
    """
    post_id = post.get('post_id', 'UNKNOWN_ID')

    # 卫语句：拦截无需处理的帖子
    if not is_need_formatting(post):
        return None, "SKIPPED", post_id

    try:
        # 耗时的 I/O 操作 (如大模型 API 调用)
        media_format_info = gen_media_format_info(post)

        if media_format_info:
            post['media_format'] = media_format_info
            return post, "SUCCESS", post_id
        else:
            return None, "NO_DATA", post_id

    except Exception as e:
        logger.error(f"[单任务执行] 处理帖子 {post_id} 发生异常: {e}", exc_info=True)
        return None, "ERROR", post_id


def format_image_article():
    """
    后台守护主流程：持续从数据库拉取待格式化帖子，多线程驱动大模型处理元数据后，批量回写。
    """
    MAX_CONCURRENCY = 5  # 设定并发数量为 5

    while True:
        try:
            # 每次循环获取最新的 DB 实例，确保连接有效性
            post_manager = UniversalPostManager(gen_db_object())
            existing_posts = post_manager.find_posts_by_source(BINANCE_SOURCE, limit=POST_QUERY_LIMIT)

            if not existing_posts:
                logger.info("[DB/帖子格式化] 当前无待处理帖子，休眠 60 秒...")
                time.sleep(60)
                continue

            posts_to_upsert = []

            # 开启线程池进行并发处理
            with ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
                # 提交所有任务
                future_to_post = {executor.submit(process_single_post, post): post for post in existing_posts}

                # as_completed 会在任务完成后立即返回，方便实时捕获状态
                for future in as_completed(future_to_post):
                    result_post, status, post_id = future.result()

                    if status == "SUCCESS":
                        posts_to_upsert.append(result_post)
                        # 降低日志级别或精简日志，避免高并发下刷屏
                        logger.debug(f"[并发/解析] 解析成功 | PostID: {post_id}")
                    elif status == "NO_DATA":
                        logger.warning(
                            f"[并发/解析] 无法获取有效解析数据 | PostID: {post_id} "
                            f"| 排查建议: 可能是帖子包含不支持的媒体结构，或检查 API 响应"
                        )
                    # SKIPPED 和 ERROR 状态无需在此处额外处理，ERROR已在子线程记录

            # 并发结束后，主线程进行批量统一入库 (极大提升性能且保证DB线程安全)
            if posts_to_upsert:
                post_manager.upsert_posts(posts_to_upsert)
                logger.info(
                    f"[DB/帖子格式化] 批次处理完成，成功回写落盘 "
                    f"| 成功数量: {len(posts_to_upsert)} / 总拉取数量: {len(existing_posts)}"
                )

            # 动态休眠策略：如果拉取数量达到 limit，说明可能有积压，缩短休眠；否则常规休眠
            if len(existing_posts) >= POST_QUERY_LIMIT:
                logger.info("本批次达到 Limit 上限，说明可能存在积压，仅休眠 5 秒后继续...")
                time.sleep(5)
            else:
                logger.info("本批次处理完毕，进入常规休眠 (3600秒).")
                time.sleep(3600)

        except Exception as e:
            logger.error(
                f"[系统/守护主循环] 格式化核心链路遭遇未捕获全局异常，挂起后重连 "
                f"| 结果: 【当前轮次中断，休眠 60 秒后重建 DB 对象重试】 "
                f"| 原因: {e}",
                exc_info=True
            )
            time.sleep(60)

if __name__ == "__main__":
    # sync_posts_to_vector_db()
    # search_posts_by_semantics("200倍杠杆", top_n=5)

    format_image_article()