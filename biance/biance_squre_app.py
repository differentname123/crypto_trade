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
import random
import time
from datetime import datetime, timedelta

from biance.biance_squre_api import fetch_binance_feed, clean_universal_posts, update_posts_in_place, \
    toggle_binance_follow, fetch_binance_relations, fetch_binance_user_profile
from common.common_utils import setup_logger, get_config, read_json, save_json
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, as_completed

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
        # 1. 从数据库查询历史数据
        binance_posts = post_manager.find_posts_by_source("biance", limit=50000)


        auto_sync_binance_follows("dahao")
        auto_sync_binance_follows()

        # 2. 核心改造：提取 post_id 构建全局记忆 Set（集合）
        existing_ids = set()
        for post in binance_posts:
            post_id = post.get("post_id") if isinstance(post, dict) else getattr(post, "post_id", None)
            if post_id:
                existing_ids.add(str(post_id))
        logger.info(f"🧠 本轮构建记忆库完成: 数据库中已有 {len(existing_ids)} 条历史帖子记录。")
        for search_key in aggregated_binance_follow_keywords:
            logger.info(f"--- 准备抓取: 搜索流 ({search_key}) ---")

            # 3. 将 existing_ids 传给底层抓取函数，实现“见老停抓/滤旧存新”
            search_data = fetch_binance_feed(
                keyword=search_key,
                count=1000,
                existing_ids=existing_ids  # <-- 核心新增参数
            )
            master_feed_list.extend(search_data)
        logger.info(f"✅ 抓取完成，本轮共获取【全新数据】 {len(master_feed_list)} 条，准备保存...")
        # 4. 优化数据库写入 & 智能休眠机制
        if master_feed_list:
            post_manager.upsert_posts(master_feed_list)


def extract_mutual_follow_users(posts: list, target_time_str: str) -> set:
    """
    极简且高覆盖的互关用户提取器 (一击必中版)

    :param posts: 币安广场帖子数据列表 (List of Dicts)
    :param target_time_str: 时间阈值，格式如 "2026-07-06 00:00:00"
    :return: 满足条件的所有用户ID集合 (Set)
    """

    # 1. 时间边界解析
    try:
        target_datetime = datetime.strptime(target_time_str, "%Y-%m-%d %H:%M:%S")
        target_timestamp = int(target_datetime.timestamp())
    except ValueError as e:
        logger.error(f"❌ [时间解析失败] 请检查输入格式是否为 'YYYY-MM-DD HH:MM:SS'。错误信息: {e}")
        return set()

    # 2. 扩充版强意图词库 (只要命中一个，即视为互关)
    # 剔除了极易误判的："互", "秒回", "涨粉", "留印", "回关", "关注我"
    # 保留并扩充了所有具备双向动作和极强目的性的词汇
    EXPANDED_STRICT_KEYWORDS = [
        # --- 中文绝对意图词 ---
        "互关", "互粉", "互赞", "互评", "互fo",
        "互助互关", "互关互粉", "互赞互评", "互粉互赞", "互关互赞", "互换关注",
        "关注必回", "点赞必回", "评论必回", "留下评论必回",
        "必回关", "关必回", "秒回关", "互关秒回", "粉必回", "有粉必回", "必须回关",
        "点赞互关", "诚信互关", "关注报数", "赚积分互助",
        "广场互关", "广场互粉", "币安互关", "币安互粉",
        "币圈互关", "币圈互粉", "加密货币互粉",

        # --- 英文绝对意图词 (全小写匹配) ---
        "f4f", "follow for follow", "follow4follow",
        "followback", "follow back", "mutual follow", "mutuals",
        "follow each other", "l4l", "like for like", "like4like", "sub4sub",
        "binance follow for follow", "binance f4f", "binance mutual follow",
        "follow back binance", "crypto follow for follow", "crypto f4f",
        "crypto mutual follow", "follow back crypto"
    ]

    extracted_uids = set()

    # 数据漏斗统计指标
    metrics = {
        "total_scanned": len(posts),
        "time_filtered": 0,
        "content_filtered": 0,
        "matched_posts": 0,
        "total_comments_extracted": 0
    }

    logger.info(
        f"🚀 [提取引擎启动] 时间基线: {target_time_str} ({target_timestamp}) | 核心触发词库容量: {len(EXPANDED_STRICT_KEYWORDS)}")

    for post in posts:
        # --- 漏斗第一层：时间过滤 ---
        publish_time = post.get("publish_time", 0)
        if publish_time < target_timestamp:
            metrics["time_filtered"] += 1
            continue

        # --- 漏斗第二层：一击必中内容判定 ---
        content_dict = post.get("content", {})
        text_content = content_dict.get("text_content", "") or ""
        title = content_dict.get("title", "") or ""

        # 统一转小写，并去除多余空格，提高英文和拼接词的匹配命中率
        raw_text = f"{title} {text_content}".lower()
        # 将文本中的多余空格替换为单空格，防止 "币安  互关" 漏判
        clean_text = ' '.join(raw_text.split())

        # 核心判定：只要包含列表中任意一个词汇，直接命中
        matched_keyword = next((keyword for keyword in EXPANDED_STRICT_KEYWORDS if keyword in clean_text), None)

        if not matched_keyword:
            metrics["content_filtered"] += 1
            continue

        # --- 漏斗底部：命中，执行提取 ---
        metrics["matched_posts"] += 1

        # 提取发帖人
        author_id = post.get("author_id")
        if author_id:
            extracted_uids.add(author_id)

        # 提取评论区所有互动者
        comments = post.get("comments", [])
        current_post_commenters = 0

        for comment in comments:
            c_uid = comment.get("author_uid")
            if c_uid:
                extracted_uids.add(c_uid)
                current_post_commenters += 1

        metrics["total_comments_extracted"] += current_post_commenters

    # 3. 最终结果复盘日志 (高密度指标汇总)
    logger.info(
        f"✅ [提取任务完结] 数据漏斗诊断:\n"
        f"   ├─ 输入总贴数: {metrics['total_scanned']}\n"
        f"   ├─ ❌ 因[时间太早]过滤: {metrics['time_filtered']}\n"
        f"   ├─ ❌ 因[无互关特征]过滤: {metrics['content_filtered']}\n"
        f"   ├─ 🎯 成功命中贴数: {metrics['matched_posts']}\n"
        f"   └─ 🏆 最终去重产出: 提取不重复UID共计 【{len(extracted_uids)}】 个 (含主帖作者与 {metrics['total_comments_extracted']} 评次)."
    )

    return extracted_uids

def _get_current_relations(user_name, max_count=10):
    """
    内部辅助函数：获取当前的关注和粉丝集合 (直接返回 set，优化后续的交并差集运算)
    """
    logger.info(f"🔍 开始获取 [{user_name}] 的社交关系链...")

    following_list = fetch_binance_relations(
        target_username=user_name,
        relation_type="following",
        required_count=max_count
    )
    # 直接使用集合推导式，过滤掉空 UID
    following_uids = {user.get('squareUid') for user in following_list if user.get('squareUid')}

    following_user_name_list = [user.get('username') for user in following_list if user.get('username')]
    following_map = {user.get('username'): user.get('squareUid') for user in following_list if user.get('username') and user.get('squareUid')}


    logger.info(f"✅ 成功获取关注列表，共 {len(following_map)} 人")

    followers_list = fetch_binance_relations(
        target_username=user_name,
        relation_type="followers",
        required_count=max_count
    )
    followers_uids = {user.get('squareUid') for user in followers_list if user.get('squareUid')}
    followers_user_name_list = [user.get('username') for user in followers_list if user.get('username')]
    follower_map = {user.get('username'): user.get('squareUid') for user in followers_list if user.get('username') and user.get('squareUid')}

    logger.info(f"✅ 成功获取粉丝列表，共 {len(follower_map)} 人")

    return following_map, follower_map


def _get_uids_from_recent_posts(post_manager, days_ago=7, limit=50000):
    """
    内部辅助函数：从近期帖子中提取潜在的互关目标
    """
    logger.info(f"🔍 开始从最近 {days_ago} 天的帖子中提取互关目标 (上限: {limit} 条)...")

    posts_list = post_manager.find_posts_by_source("biance", limit=limit)
    target_time_str = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")

    extracted_uids = extract_mutual_follow_users(posts_list, target_time_str)

    logger.info(f"✅ 从帖子中提取到 {len(extracted_uids)} 个潜在互关目标")
    return extracted_uids


def auto_sync_binance_follows(user_key=f"nana"):
    """
    主流程：自动化同步并执行币安广场关注任务
    """
    logger.info(f"========== 🚀 开始为 {user_key} 执行币安广场自动关注任务 ==========")

    # 1. 加载并校验核心配置
    my_cookies = get_config(f"{user_key}_cookie")
    csrf_token = get_config(f"{user_key}_csrf")
    my_name = get_config(f"{user_key}_name")

    if not all([my_cookies, csrf_token, my_name]):
        logger.error("❌ 缺少必要的配置信息 (Cookie, CSRF Token 或 用户名)，任务被迫终止。")
        return

    # 2. 初始化数据库和业务管理器 (增加异常捕获)
    try:
        db_instance = gen_db_object()
        post_manager = UniversalPostManager(db_instance)
    except Exception as e:
        logger.error(f"❌ 数据库或 PostManager 初始化失败: {e}")
        return

    # 3. 拉取远端数据
    following_map, follower_map = _get_current_relations(my_name, max_count=10000)
    extracted_uids = _get_uids_from_recent_posts(post_manager, days_ago=30, limit=50000)
    following_uids = set(following_map.values())
    followers_uids = set(follower_map.values())

    valid_square_uid_list = get_worth_following_list(initial_user_name_list=following_map.keys(), target_count=200)

    extracted_uids = extracted_uids.union(set(valid_square_uid_list))
    # 4. 核心逻辑运算 (利用 Set 的高效运算机制)
    # 集合A: 帖子中提取出来的，且我还没关注的
    need_to_follow_from_posts = extracted_uids - following_uids

    # 集合B: 关注了我，但我还没回关的 (粉丝里剥离出我已经关注的)
    need_to_follow_back = followers_uids - following_uids

    # 最终合并需要操作的 UID 集合
    final_uids_to_follow = need_to_follow_from_posts.union(need_to_follow_back)

    logger.info(
        f"📊 运算统计结果:\n"
        f"   - 帖子发掘待关注: {len(need_to_follow_from_posts)} 人\n"
        f"   - 粉丝列表待回关: {len(need_to_follow_back)} 人\n"
        f"   - 去重后总计待关注: {len(final_uids_to_follow)} 人"
    )

    if not final_uids_to_follow:
        logger.info("🎉 当前没有需要关注的新用户，任务圆满结束。")
        return

    # 5. 执行操作并防风控
    success_count = 0
    fail_count = 0

    logger.info("⚙️ 开始执行批量关注操作...")

    for index, uid in enumerate(final_uids_to_follow, 1):
        logger.info(f"[{index}/{len(final_uids_to_follow)}] 正在尝试关注 UID: {uid}")

        # 调用此前写的私有态接口（如果需要 extra_headers 风控参数，记得在这里传入）
        is_success = toggle_binance_follow(
            target_uid=uid,
            action="follow",
            cookies=my_cookies,
            csrf_token=csrf_token
        )
        if is_success:
            success_count += 1
        else:
            fail_count += 1
        # ⚠️ 极度重要：防风控休眠机制
        # 如果这是最后一条，就不需要休眠了
        if index < len(final_uids_to_follow):
            sleep_time = random.uniform(60, 90)
            logger.info(f"⏳ 防风控休眠 {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)

    logger.info(f"========== 🏁 任务执行完毕 | 成功: {success_count} | 失败: {fail_count} ==========")


def predict_follow_back(follow_count, follower_count):
    """
    根据关注数和粉丝数，判断目标用户回关的概率是否大概率(>50%)。
    返回一个字典，包含是否建议关注(is_recommended)、预计概率(probability)和具体原因(reason)。
    """
    # 0. 基础排错：防止除以 0 的情况
    if follower_count == 0:
        return {
            "is_recommended": False,
            "probability": "< 5%",
            "reason": "粉丝数为0，绝对的死号或新号，无参考价值。"
        }

    # 1. 第一道漏斗：粉丝数框定 (锁定能看见你，且珍惜粉丝的人)
    if not (100 <= follower_count <= 3000):
        return {
            "is_recommended": False,
            "probability": "< 10%",
            "reason": f"粉丝数({follower_count})不在黄金区间(100~3000)。小于100是边缘号，大于3000消息易被折叠或有包袱。"
        }

    # 2. 第二道漏斗：关注数框定 (排除高冷自闭号和海量营销号)
    if not (200 <= follow_count <= 1500):
        return {
            "is_recommended": False,
            "probability": "< 10%",
            "reason": f"关注数({follow_count})不在安全区间(200~1500)。小于200极度排外，大于1500信息流爆炸或是脚本号。"
        }

    # 3. 第三道漏斗：关注/粉丝比例 (锁定互惠心理)
    ratio = follow_count / follower_count
    if not (0.8 <= ratio <= 1.5):
        return {
            "is_recommended": False,
            "probability": "< 20%",
            "reason": f"比例({ratio:.2f})不在互惠区间(0.8~1.5)。不是高冷白嫖党就是单向看客。"
        }

    # === 只要活着走到这里的，绝对是优质目标 (胜率 > 50%) ===

    if ratio >= 1.0:
        return {
            "is_recommended": True,
            "probability": "70% - 90%",
            "reason": "绝对VIP目标：活跃真人且关注数大于等于粉丝数，处于强烈的涨粉需求期，必回关！"
        }
    else:
        return {
            "is_recommended": True,
            "probability": "50% - 70%",
            "reason": "优质目标：非常健康的社交活跃用户，数据咬得很紧，大概率顺手回关。"
        }


def is_need_follow_user(user_name, user_info_map):
    """
    判断是否需要关注某个用户 (已移除内部文件读写，纯内存运算适配并发)
    """
    if 'totalFollowCount' not in user_info_map.get(user_name, {}):
        user_info = fetch_binance_user_profile(user_name)
        if not user_info:
            return False, None

        # === 仅在此处修改：只保留预测和业务所需的必要字段，进行内存瘦身 ===
        pruned_info = {
            'squareUid': user_info.get('squareUid'),
            'totalFollowCount': user_info.get('totalFollowCount', 0),
            'totalFollowerCount': user_info.get('totalFollowerCount', 0)
        }

        # 写入内存字典 (使用 setdefault 保证多线程并发时字典键赋值的安全)
        user_info_map.setdefault(user_name, {}).update(pruned_info)
    else:
        user_info = user_info_map[user_name]

    follow_count = user_info.get('totalFollowCount', 0)
    follower_count = user_info.get('totalFollowerCount', 0)
    predict_info = predict_follow_back(follow_count, follower_count)
    return predict_info


# ==================== 抽取：获取关系的单任务 ====================
def _fetch_relation_task(user_name, max_count, has_cache):
    if has_cache:
        logger.info(f"⚠️ [{user_name}] 已在缓存中，跳过重复获取关系链。")
    following_map, follower_map = _get_current_relations(user_name, max_count)
    return user_name, following_map, follower_map


def get_all_relations(user_name_list, max_count=1000, all_user_map={}):
    current_user_map = {}

    logger.info(f"🔄 [关系链获取] 开始并发请求，目标用户数: {len(user_name_list)}，并发度: 20")

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_to_user = {}
        for user_name in user_name_list:
            has_cache = (user_name in all_user_map and
                         'following' in all_user_map[user_name] and
                         'followers' in all_user_map[user_name])

            future = executor.submit(_fetch_relation_task, user_name, max_count, has_cache)
            future_to_user[future] = user_name

        for future in as_completed(future_to_user):
            user_name = future_to_user[future]
            try:
                _, following_map, follower_map = future.result()
                all_user_map[user_name] = {
                    "following": following_map,
                    "followers": follower_map
                }
                current_user_map[user_name] = {
                    "following": following_map,
                    "followers": follower_map
                }
                logger.debug(f"✅ 成功获取 [{user_name}] 关系链: 关注 {len(following_map)} | 粉丝 {len(follower_map)}")
            except Exception as e:
                logger.error(f"❌ [关系链获取] 用户 [{user_name}] 获取异常: {e}")

    logger.info(f"✅ [关系链获取] 本批次并发执行完毕，共获取 {len(current_user_map)} 名用户数据。")
    return current_user_map


# ==================== 抽取：预测用户的单任务 ====================
def _evaluate_user_task(user_name, square_uid, user_info_map):
    predict_info = is_need_follow_user(user_name, user_info_map)
    return user_name, square_uid, predict_info


def get_worth_following_list(initial_user_name_list, target_count):
    """
    基于种子用户不断裂变，使用多线程并发获取值得关注的用户
    (增加外层批量保存缓存数据功能 & 存在列表过滤)
    """
    logger.info(f"🕸️ 开始并发裂变获取，目标数量: {target_count}")

    if not initial_user_name_list:
        logger.error("❌ 致命错误: 传入的 initial_user_name_list 为空！无法启动裂变，请检查外层调用参数。")
        return []

    # 1. 在最外层统一读取历史画像文件，避免并发读写冲突
    user_info_path = "user_profile.json"
    user_info_map = read_json(user_info_path)
    valid_square_uids = set()
    this_turn_good_users = initial_user_name_list

    turn_count = 1  # 增加轮次计数器辅助日志

    while len(valid_square_uids) < target_count:
        logger.info(
            f"🚀 === 第 {turn_count} 轮裂变开始 === | 当前已挖掘高潜用户: {len(valid_square_uids)}/{target_count}")

        current_user_info_map = get_all_relations(this_turn_good_users, max_count=1000, all_user_map=user_info_map)

        # 解析出current_user_map所有的user_name和squareUid，构建一个全局的已存在UID集合
        current_user_id_map = {}
        for user_name, relations in current_user_info_map.items():
            current_user_id_map.update(relations.get('following', {}))
            current_user_id_map.update(relations.get('followers', {}))

        this_turn_good_users = []

        logger.info(f"🔍 [画像预测] 本轮待分析关系链用户数: {len(current_user_id_map)}，开始20线程并发预测...")

        with ThreadPoolExecutor(max_workers=20) as executor:
            # 提交所有待预测用户任务
            future_to_user = {
                executor.submit(_evaluate_user_task, user_name, square_uid, user_info_map): user_name
                for user_name, square_uid in current_user_id_map.items()
            }

            # 并发收集结果
            for future in as_completed(future_to_user):
                user_name = future_to_user[future]
                try:
                    _, uid, predict_info = future.result()
                    is_recommended = predict_info.get('is_recommended', False)
                    # 保留原判断逻辑
                    if is_recommended and uid and str(uid):
                        this_turn_good_users.append(user_name)
                        valid_square_uids.add(str(uid))
                        logger.info(
                            f"🌟 发现高潜用户: [{user_name}] | 当前达标进度: {len(valid_square_uids)}/{target_count}")

                        # 提前终止判断，防止无用的持续解析
                        if len(valid_square_uids) >= target_count:
                            logger.info("🎉 达标数量已满足，正在停止本轮并发预测解析...")
                            break
                except Exception as e:
                    logger.error(f"❌ [画像预测] 用户 [{user_name}] 预测异常: {e}")

        logger.info(
            f"💾 [数据持久化] 第 {turn_count} 轮结束，发现新高潜种子: {len(this_turn_good_users)} 人，正在保存全量用户画像...")
        save_json(user_info_path, user_info_map)
        turn_count += 1

        # 兜底逻辑：防止裂变断层导致死循环
        if not this_turn_good_users and len(valid_square_uids) < target_count:
            logger.warning("⚠️ 警告：本轮未发现任何新的高潜用户，裂变链条断裂，提前退出！")
            break

    logger.info(f"🏁 裂变任务圆满完成！最终收集到 {len(valid_square_uids)} 个有效高潜力用户UID。")
    return list(valid_square_uids)


if __name__ == "__main__":
    try:
        fetch_follow_content()
    except Exception as e:
        logger.error(f"程序运行中发生异常: {e}", exc_info=True)