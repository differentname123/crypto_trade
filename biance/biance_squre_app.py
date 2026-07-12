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
    logger.info(f"✅ 成功获取关注列表，共 {len(following_uids)} 人")

    followers_list = fetch_binance_relations(
        target_username=user_name,
        relation_type="followers",
        required_count=max_count
    )
    followers_uids = {user.get('squareUid') for user in followers_list if user.get('squareUid')}
    followers_user_name_list = [user.get('username') for user in followers_list if user.get('username')]

    logger.info(f"✅ 成功获取粉丝列表，共 {len(followers_uids)} 人")

    return following_uids,following_user_name_list, followers_uids, followers_user_name_list


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
    following_uids,following_user_name_list, followers_uids, followers_user_name_list = _get_current_relations(my_name, max_count=10000)
    extracted_uids = _get_uids_from_recent_posts(post_manager, days_ago=30, limit=50000)


    valid_square_uid_list = get_worth_following_list(initial_user_name_list=following_user_name_list, target_count=100, visited_user_name_list=[], exist_uids=list(following_uids))

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
    if user_name not in user_info_map:
        user_info = fetch_binance_user_profile(user_name)
        if not user_info:
            return False, None

        # === 仅在此处修改：只保留预测和业务所需的必要字段，进行内存瘦身 ===
        pruned_info = {
            'squareUid': user_info.get('squareUid'),
            'totalFollowCount': user_info.get('totalFollowCount', 0),
            'totalFollowerCount': user_info.get('totalFollowerCount', 0)
        }

        # 写入内存字典 (Python 字典单键赋值是线程安全的)
        user_info_map[user_name] = pruned_info
    else:
        user_info = user_info_map[user_name]

    follow_count = user_info.get('totalFollowCount', 0)
    follower_count = user_info.get('totalFollowerCount', 0)
    square_uid = user_info.get('squareUid')

    predict_info = predict_follow_back(follow_count, follower_count)

    return predict_info['is_recommended'], square_uid


def get_worth_following_list(initial_user_name_list, target_count, visited_user_name_list, exist_uids):
    """
    基于种子用户不断裂变，使用多线程并发获取值得关注的用户
    (增加外层批量保存缓存数据功能 & 存在列表过滤)
    """
    logger.info(f"🕸️ 开始并发裂变获取，目标数量: {target_count}")

    if not initial_user_name_list:
        logger.error("❌ 致命错误: 传入的 initial_user_name_list 为空！无法启动裂变，请检查外层调用参数。")
        return []

    # 统一将 exist_uids 转为字符串集合，防止因为 int 和 str 类型不同导致比对失败
    exist_uids_set = set(str(uid) for uid in (exist_uids or []))

    # 1. 在最外层统一读取历史画像文件，避免并发读写冲突
    user_info_path = "user_profile.json"
    user_info_map = read_json(user_info_path) or {}

    valid_square_uids = set()
    visited_names_set = set(visited_user_name_list)
    current_batch = set(initial_user_name_list) - visited_names_set

    if not current_batch:
        logger.warning(
            f"⚠️ 初始名单包含 {len(initial_user_name_list)} 人，但剔除 visited_user_name_list 后剩余 0 人。裂变终止。")
        return []

    logger.info(
        f"🚀 种子初始化成功！实际作为本轮种子的有 {len(current_batch)} 人。已排除的历史(存在)UID有 {len(exist_uids_set)} 个。")

    max_workers = 5

    def _check_user_task(name):
        # 传入内存中的 user_info_map 进行查询和更新
        is_rec, uid = is_need_follow_user(name, user_info_map)
        return name, is_rec, uid

    def _get_relations_task(name):
        return _get_current_relations(name, max_count=1000)

    while current_batch and len(valid_square_uids) < target_count:
        logger.info(f"--- 🔄 本层待检测用户: {len(current_batch)} 人 (并发度: {max_workers}) ---")

        successful_names = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_name = {executor.submit(_check_user_task, name): name for name in current_batch}

            for future in concurrent.futures.as_completed(future_to_name):
                name = future_to_name[future]

                visited_names_set.add(name)
                if name not in visited_user_name_list:
                    visited_user_name_list.append(name)

                try:
                    name, is_recommended, square_uid = future.result()
                    if is_recommended and square_uid:
                        # === 新增逻辑：判断是否在 exist_uids 中 ===
                        if str(square_uid) not in exist_uids_set:
                            valid_square_uids.add(square_uid)
                            successful_names.append(name)
                            logger.info(
                                f"🎯 命中新目标! [{name}] 符合条件. 当前进度: {len(valid_square_uids)}/{target_count}")

                            if len(valid_square_uids) >= target_count:
                                break
                        else:
                            # 虽然不计数，但该用户是优质节点，保留作为下一层的裂变种子
                            successful_names.append(name)
                            logger.info(f"⏭️ [{name}] 优质但已在 exist_uids 中，跳过计数，仅保留为下一层裂变种子。")

                except Exception as e:
                    logger.error(f"❌ 检查用户 [{name}] 时发生异常: {e}")

        if len(valid_square_uids) >= target_count:
            break

        current_batch = set()
        if successful_names:
            logger.info(f"🔍 目标未满，并发获取 {len(successful_names)} 个优质用户的关系链...")

            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_seed = {executor.submit(_get_relations_task, seed): seed for seed in successful_names}

                for future in concurrent.futures.as_completed(future_to_seed):
                    try:
                        following_uids, following_names, followers_uids, follower_names = future.result()
                        current_batch.update(following_names)
                        current_batch.update(follower_names)
                    except Exception as e:
                        logger.error(f"❌ 获取关系链时发生异常: {e}")

            current_batch -= visited_names_set
        else:
            logger.warning("⚠️ 本批次没有符合要求的用户，裂变断层。")

    # 2. 整个裂变网络跑完后，在外层统一落盘保存一次
    save_json(user_info_map, user_info_path)
    logger.info(f"💾 用户画像缓存已批量落盘保存至 {user_info_path}")

    logger.info(f"🎉 裂变获取任务结束，最终获取到 {len(valid_square_uids)} 个有效目标 UID。")
    return list(valid_square_uids)


if __name__ == "__main__":
    try:
        fetch_follow_content()
    except Exception as e:
        logger.error(f"程序运行中发生异常: {e}", exc_info=True)