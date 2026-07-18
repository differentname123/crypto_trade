# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:23
:last_date:
    2026/4/7 0:23
:description:
    深度重构版：高内聚低耦合。
    - 生产者（抓帖子）与消费者（点关注）通过 MongoDB 彻底物理隔离。
    - 并行多账号处理，提取共享共有的高价值用户线索。
    - 压平线程池，消除嵌套，所有 I/O 和计算在同一单线程任务内流转。
    - 提取纯血的 Predictor，实现业务判定与网络/存储层的完全剥离。
"""
import logging
import random
import time
import threading
from datetime import datetime, timedelta
from common.common_utils import setup_logger, get_config, read_json, save_json
logger = setup_logger(app_name="biance_follow")
from concurrent.futures import ThreadPoolExecutor, as_completed
from biance.biance_squre_api import fetch_binance_feed,\
    toggle_binance_follow, fetch_binance_relations, fetch_binance_user_profile


from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager




# =====================================================================
# [基础组件层] 数据清洗、关系链提取与纯血预测引擎
# =====================================================================

def predict_follow_back(user_info: dict) -> dict:
    """
    [改造点4]：纯血的预测引擎 (Predictor)
    完全剥离网络请求和本地存储。仅负责逻辑判定（纯函数），可通过离线历史数据反复调优。
    """
    follow_count = user_info.get('totalFollowCount', 0)
    follower_count = user_info.get('totalFollowerCount', 0)

    # 1. 官方风控与状态过滤 (一票否决)
    if user_info.get('lowQuality', False):
        return {"is_recommended": False, "probability": "0%", "reason": "官方标记的低质量/降权号"}

    if user_info.get('userStatus', 1) != 1:
        return {"is_recommended": False, "probability": "0%", "reason": "账号状态异常(封禁或静默)"}

    # 2. 僵尸号/机器号过滤
    if user_info.get('totalListedPostCount', 0) == 0 and user_info.get('totalLikeCount', 0) == 0:
        if follow_count > 50:
            return {"is_recommended": False, "probability": "0%", "reason": "0发帖0点赞的批量机器/僵尸号"}

    # 3. 活跃度时间过滤 (沉寂号)
    modify_time_ms = user_info.get('modifyTime', 0)
    if modify_time_ms > 0:
        days_inactive = (time.time() * 1000 - modify_time_ms) / (1000 * 3600 * 24)
        if days_inactive > 30:
            return {"is_recommended": False, "probability": "< 5%",
                    "reason": f"长达 {int(days_inactive)} 天未活跃的沉寂号"}

    # 4. T0 级强意图核武器 (直接保送)
    bio = (user_info.get('biography') or '').lower()
    if any(k in bio for k in ['互关', '互粉', '必回', 'f4f', 'follow back']):
        return {"is_recommended": True, "probability": "99%", "reason": "T0级 VIP: 个人简介明确写了互关/必回"}

    # 5. 数据比例核心漏斗
    if follower_count == 0:
        return {"is_recommended": False, "probability": "< 5%", "reason": "粉丝数为0，绝对的死号或新号。"}

    if not (50 <= follower_count <= 1500):
        return {"is_recommended": False, "probability": "< 10%", "reason": f"粉丝数({follower_count})不在饥渴区间。"}

    if not (100 <= follow_count <= 1500):
        return {"is_recommended": False, "probability": "< 10%", "reason": f"关注数({follow_count})不在安全区间。"}

    ratio = follow_count / follower_count
    if not (0.8 <= ratio <= 1.5):
        return {"is_recommended": False, "probability": "< 20%", "reason": f"比例({ratio:.2f})不在互惠区间。"}

    # 优质活跃真人目标
    if ratio >= 1.0:
        return {"is_recommended": True, "probability": "70% - 90%",
                "reason": "强潜目标：活跃真人且关注数大于等于粉丝数，必回关！"}
    else:
        return {"is_recommended": False, "probability": "50% - 70%",
                "reason": "优质目标：健康的社交活跃用户，大概率顺手回关。"}


def extract_mutual_follow_users(posts: list, target_time_str: str) -> set:
    """极简且高覆盖的互关用户提取器 (一击必中版)"""
    try:
        target_datetime = datetime.strptime(target_time_str, "%Y-%m-%d %H:%M:%S")
        target_timestamp = int(target_datetime.timestamp())
    except ValueError as e:
        logger.error(f"❌ [时间解析失败] 请检查输入格式是否为 'YYYY-MM-DD HH:MM:SS'。错误信息: {e}")
        return set()

    EXPANDED_STRICT_KEYWORDS = [
        "互关", "互粉", "互赞", "互评", "互fo", "互助互关", "互关互粉", "互赞互评", "互粉互赞", "互关互赞", "互换关注",
        "关注必回", "点赞必回", "评论必回", "留下评论必回", "必回关", "关必回", "秒回关", "互关秒回", "粉必回",
        "有粉必回", "必须回关",
        "点赞互关", "诚信互关", "关注报数", "赚积分互助", "广场互关", "广场互粉", "币安互关", "币安互粉", "币圈互关",
        "币圈互粉", "加密货币互粉",
        "f4f", "follow for follow", "follow4follow", "followback", "follow back", "mutual follow", "mutuals",
        "follow each other", "l4l", "like for like", "like4like", "sub4sub", "binance follow for follow",
        "binance f4f", "binance mutual follow", "follow back binance", "crypto follow for follow", "crypto f4f",
        "crypto mutual follow", "follow back crypto"
    ]

    extracted_uids = set()
    metrics = {"total_scanned": len(posts), "time_filtered": 0, "content_filtered": 0, "matched_posts": 0,
               "total_comments_extracted": 0}

    for post in posts:
        if post.get("publish_time", 0) < target_timestamp:
            metrics["time_filtered"] += 1
            continue

        content_dict = post.get("content", {})
        raw_text = f"{content_dict.get('title', '')} {content_dict.get('text_content', '')}".lower()
        clean_text = ' '.join(raw_text.split())

        matched_keyword = next((kw for kw in EXPANDED_STRICT_KEYWORDS if kw in clean_text), None)

        if not matched_keyword:
            metrics["content_filtered"] += 1
            continue

        metrics["matched_posts"] += 1
        if post.get("author_id"): extracted_uids.add(post.get("author_id"))

        for comment in post.get("comments", []):
            if comment.get("author_uid"):
                extracted_uids.add(comment.get("author_uid"))
                metrics["total_comments_extracted"] += 1

    return extracted_uids


def _get_current_relations(user_name, max_count=10, progress_ctx=None):
    """获取当前的关注和粉丝集合"""
    following_list = fetch_binance_relations(target_username=user_name, relation_type="following",
                                             required_count=max_count)
    following_map = {user.get('username'): user.get('squareUid') for user in following_list if
                     user.get('username') and user.get('squareUid')}

    followers_list = fetch_binance_relations(target_username=user_name, relation_type="followers",
                                             required_count=max_count)
    follower_map = {user.get('username'): user.get('squareUid') for user in followers_list if
                    user.get('username') and user.get('squareUid')}

    if progress_ctx:
        with progress_ctx["lock"]:
            progress_ctx["count"] += 1

    return following_map, follower_map


def _get_uids_from_recent_posts(post_manager, days_ago=7, limit=50000):
    """从近期帖子中提取潜在的互关目标"""
    posts_list = post_manager.find_posts_by_source("biance", limit=limit)
    target_time_str = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    return extract_mutual_follow_users(posts_list, target_time_str)


# =====================================================================
# [核心业务层] 扁平化任务流与裂变引擎
# =====================================================================

def _analyze_user_task(user_name, user_info_map, stop_event, progress_ctx=None):
    """
    [改造点3]：压平嵌套线程池。单一闭环流转，杜绝线程饥饿。
    """
    if stop_event.is_set():
        return user_name, None, None, None, None

    # --- 1. 读取本地缓存并判定 ---
    user_data = user_info_map.get(user_name, {})
    if 'predict_info' in user_data:
        predict_info = user_data['predict_info']
        if not predict_info.get('is_recommended', False):
            return user_name, user_data.get('squareUid'), predict_info, {}, {}
        # 已判定推荐，且已缓存了关系链
        if 'following' in user_data and 'followers' in user_data:
            if progress_ctx:
                with progress_ctx["lock"]: progress_ctx["count"] += 1
            return user_name, user_data.get('squareUid'), predict_info, user_data['following'], user_data['followers']

    # --- 2. 网络层：拉取基础画像 ---
    if 'squareUid' not in user_data:
        user_info = fetch_binance_user_profile(user_name)
        if not user_info:
            predict_info = {"is_recommended": False, "probability": "0%", "reason": "API拉取失败或用户注销"}
            user_info_map[user_name] = {'predict_info': predict_info}
            if progress_ctx:
                with progress_ctx["lock"]: progress_ctx["count"] += 1
            return user_name, None, predict_info, {}, {}

        # 内存瘦身保留有用字段
        user_info = {
            'squareUid': user_info.get('squareUid'),
            'totalFollowCount': user_info.get('totalFollowCount', 0),
            'totalFollowerCount': user_info.get('totalFollowerCount', 0),
            'lowQuality': user_info.get('lowQuality', False),
            'userStatus': user_info.get('userStatus', 1),
            'totalListedPostCount': user_info.get('totalListedPostCount', 0),
            'totalLikeCount': user_info.get('totalLikeCount', 0),
            'modifyTime': user_info.get('modifyTime', 0),
            'biography': user_info.get('biography', '')
        }
        user_info_map.setdefault(user_name, {}).update(user_info)
    else:
        # 【修复点】：如果本地已经有缓存的 UID 等资料，直接将其赋值给 user_info 供预测引擎使用
        user_info = user_data

    # --- 3. 核心调用：纯血引擎预测 ---
    predict_info = predict_follow_back(user_info)
    square_uid = user_info.get('squareUid')
    user_info_map[user_name]['predict_info'] = predict_info

    # --- 4. 分流处理：差号舍弃，好号接着拉关系链 ---
    if not predict_info.get('is_recommended', False):
        # 清洗缓存防膨胀
        keys_to_remove = [k for k in user_info_map[user_name].keys() if k not in ('predict_info', 'squareUid')]
        for k in keys_to_remove: del user_info_map[user_name][k]
        if progress_ctx:
            with progress_ctx["lock"]: progress_ctx["count"] += 1
        return user_name, square_uid, predict_info, {}, {}

    # 高价值号，直接在同一大线程内发起关系网络请求！
    following_map, follower_map = _get_current_relations(user_name, max_count=1000, progress_ctx=progress_ctx)

    # --- 5. 存储层最后兜底更新 ---
    user_info_map[user_name]['following'] = following_map
    user_info_map[user_name]['followers'] = follower_map

    return user_name, square_uid, predict_info, following_map, follower_map

def get_worth_following_list(initial_user_name_list, target_count):
    """单线程池扁平化裂变引擎 (提取出共有高质量 UID 的核心方法)"""
    logger.info(f"🕸️ 开始并发裂变获取共有高价值池，目标数量: {target_count}")

    if not initial_user_name_list:
        logger.error("❌ 致命错误: 初始种子列表为空！")
        return []

    user_info_path = "user_profile.json"
    user_info_map = read_json(user_info_path)

    valid_square_uids = set()
    evaluated_users = set()

    pending_user_names = list(initial_user_name_list)
    turn_count = 1

    while len(valid_square_uids) < target_count and pending_user_names:
        logger.info(
            f"🚀 === 第 {turn_count} 轮裂变开始 === | 待预测种子数: {len(pending_user_names)} | 进度: {len(valid_square_uids)}/{target_count}")

        next_turn_user_names = []
        stop_event = threading.Event()
        progress_ctx = {"count": 0, "total": len(pending_user_names), "lock": threading.Lock()}

        # [改造点3]：全局唯一的线程池，内聚所有网络和逻辑操作。
        with ThreadPoolExecutor(max_workers=30) as executor:
            future_to_user = {}
            for uname in pending_user_names:
                if uname in evaluated_users:
                    continue
                evaluated_users.add(uname)
                future = executor.submit(_analyze_user_task, uname, user_info_map, stop_event, progress_ctx)
                future_to_user[future] = uname

            for future in as_completed(future_to_user):
                try:
                    uname, uid, predict_info, following_map, follower_map = future.result()
                    if predict_info is None:
                        continue  # 线程已被终止

                    if predict_info.get('is_recommended', False) and uid:
                        valid_square_uids.add(str(uid))
                        # 广度优先：把新挖到的大V的关系链，作为下一轮的种子
                        next_turn_user_names.extend(following_map.keys())
                        next_turn_user_names.extend(follower_map.keys())
                        logger.info(f"🌟 发现高潜目标: [{uname}] | 进度: {len(valid_square_uids)}/{target_count}")

                        if len(valid_square_uids) >= target_count:
                            logger.info("🎉 达标数量已满足，正在向线程池发送阻断信号...")
                            stop_event.set()
                            break
                except Exception as e:
                    logger.error(f"❌ [分析用户任务] 异常: {e}")

        # 数据持久化，并过滤掉已经预测过的，准备下一轮
        save_json(user_info_path, user_info_map)
        pending_user_names = list(set(next_turn_user_names) - evaluated_users)
        turn_count += 1

    return list(valid_square_uids)


# =====================================================================
# [消费者] 独立的消费者程序：提取共有库并并行处理多账号
# =====================================================================

def _sync_single_account_logic(user_key, global_potential_uids):
    """消费者内部任务：对单个账号执行特定的关注过滤与动作"""
    my_cookies = get_config(f"{user_key}_cookie")
    csrf_token = get_config(f"{user_key}_csrf")
    my_name = get_config(f"{user_key}_name")

    if not all([my_cookies, csrf_token, my_name]):
        logger.error(f"❌ [账号: {user_key}] 缺少配置，任务被迫终止。")
        return

    logger.info(f"========== 🚀 [账号: {user_key}] 开始执行关注逻辑 ==========")

    # 1. 拿账号自身的当前关系进行过滤
    following_map, follower_map = _get_current_relations(my_name, max_count=10000)
    following_uids = set(following_map.values())
    followers_uids = set(follower_map.values())

    # 集合A: 共享的大池子里提取出来的，且本号还没关注的
    need_to_follow_from_pool = global_potential_uids - following_uids
    # 集合B: 本号的真实粉丝中，还没回关的
    need_to_follow_back = followers_uids - following_uids

    final_uids_to_follow = need_to_follow_from_pool.union(need_to_follow_back)

    logger.info(
        f"📊 [账号: {user_key}] 差集运算结果:\n"
        f"   - 从公共池过滤待关注: {len(need_to_follow_from_pool)} 人\n"
        f"   - 从自有粉丝补回关: {len(need_to_follow_back)} 人\n"
        f"   - 去重总计最终关注: {len(final_uids_to_follow)} 人"
    )

    # 最多只保留100个
    final_uids_to_follow = list(final_uids_to_follow)[:100]
    if not final_uids_to_follow:
        logger.info(f"🎉 [账号: {user_key}] 没有需要关注的新用户。")
        return

    success_count = 0
    fail_count = 0
    for index, uid in enumerate(final_uids_to_follow, 1):
        logger.info(f"[账号: {user_key}] [{index}/{len(final_uids_to_follow)}] 尝试关注 UID: {uid}")

        is_success = toggle_binance_follow(uid, "follow", my_cookies, csrf_token)
        if is_success:
            success_count += 1
        else:
            fail_count += 1

        # 正常关注完休眠防风控
        if index < len(final_uids_to_follow):
            sleep_time = random.uniform(60, 90)
            logger.info(f"⏳ [账号: {user_key}] 休眠 {sleep_time:.2f} 秒防风控...")
            time.sleep(sleep_time)

    logger.info(f"🏁 [账号: {user_key}] 完毕 | 成功: {success_count} | 失败: {fail_count}")


def consumer_auto_sync_main(accounts=None):
    """
    [改造点1/2]：独立的消费者入口。
    以 MongoDB 作为唯一的线索中转站，聚合出所有值得关注的优质列表后，多账号并行跑风控限制下点关注。
    """
    if accounts is None:
        accounts = ["dahao", "nana"]

    logger.info("========== 🚀 消费者系统启动 (多并发关注) ==========")
    db_instance = gen_db_object()
    post_manager = UniversalPostManager(db_instance)

    while True:
        try:
            # 1. 提取所有公共的泛泛之交 (最近帖子中发互关的)
            shared_post_uids = _get_uids_from_recent_posts(post_manager, days_ago=30, limit=50000)

            # 2. 聚合所有账号的前置种子来进行裂变，抽取提取全网高质量目标池
            seed_user_names = []
            logger.info("🌱 开始提取各账号的初始种子...")
            for acc in accounts:
                my_name = get_config(f"{acc}_name")
                if my_name:
                    f_map, _ = _get_current_relations(my_name, max_count=10000)  # 拿点种子足以起盘
                    seed_user_names.extend(f_map.keys())

            seed_user_names = list(set(seed_user_names))
            global_worth_uids = get_worth_following_list(initial_user_name_list=seed_user_names, target_count=1000)

            # 3. 合成大池子
            global_potential_uids = shared_post_uids.union(set(global_worth_uids))

            if not global_potential_uids:
                logger.info("🤷 当前数据库和裂变链无任何潜在线索，休眠 10 分钟...")
                time.sleep(600)
                continue

            # 4. 极致并行：多账号同时启动，自行运算差集，各自休眠
            logger.info(f"🚦 启动多账号并行执行引擎，并发数: {len(accounts)}")
            with ThreadPoolExecutor(max_workers=len(accounts)) as executor:
                futures = [executor.submit(_sync_single_account_logic, acc, global_potential_uids) for acc in accounts]
                for f in as_completed(futures):
                    f.result()  # 捕获可能抛出的内部异常

            logger.info("♻️ 消费者本轮执行完毕，休眠 1 小时后再次启动清剿...")
            time.sleep(3600)

        except Exception as e:
            logger.error(f"❌ 消费者大循环发生异常: {e}", exc_info=True)
            time.sleep(60)


# =====================================================================
# [生产者] 独立的生产者程序：仅查数据抓字典入库
# =====================================================================

def producer_fetch_content_main():
    """
    [改造点1]：拆分 1 - 生产者系统。
    完全死循环。仅仅按照关键词去抓帖子、提取发帖人数据并用 MongoDB 托底。加上 5 秒防 API 风控。
    """
    db_instance = gen_db_object()
    post_manager = UniversalPostManager(db_instance)

    aggregated_binance_follow_keywords = [
        "互关", "互粉", "互赞", "互评", "互fo", "涨粉", "粉丝互助", "互关互粉", "关注必回", "必回关",
        "秒回关", "点赞互关", "赚积分互助", "广场互关", "币安互关", "f4f", "follow for follow",
        "followback", "mutual follow", "follow me Binance"
    ]

    logger.info(f"========== 📡 生产者抓取引擎启动 (MongoDB 管道已建立) ==========")

    while True:
        time.sleep(10000)

        try:
            # 查底层字典作为过滤边界
            binance_posts = post_manager.find_posts_by_source("biance", limit=50000)
            existing_ids = {str(getattr(post, "post_id", post.get("post_id"))) for post in binance_posts if
                            getattr(post, "post_id", post.get("post_id"))}

            logger.info(f"🧠 本轮构建记忆库完成: DB 中已有 {len(existing_ids)} 条历史帖子记录。")

            for search_key in aggregated_binance_follow_keywords:
                logger.info(f"--- 生产者提取流: 正在抓取 ({search_key}) ---")

                search_data = fetch_binance_feed(
                    keyword=search_key,
                    count=1000,
                    existing_ids=existing_ids
                )

                if search_data:
                    logger.info(f"✅ 获取【全新数据】 {len(search_data)} 条，流向 MongoDB 进行 Upsert...")
                    post_manager.upsert_posts(search_data)

                # [改造点1]: 单纯按词搜索，增加硬休眠防范触发 API 断流
                time.sleep(5)

        except Exception as e:
            logger.error(f"❌ 生产者运行异常: {e}")
            time.sleep(10)


# =====================================================================
# 统一启动入口 (可根据部署情况任选启动方式)
# =====================================================================
if __name__ == "__main__":
    # 为了直观体现“彻底劈成两半作为独立的启动入口”，这里提供多线程挂载启动。
    # 实际生产环境中也可以分成两个独立的 Python 进程 (例如 python app.py --producer 和 python app.py --consumer)

    try:
        logger.info("💥 初始化启动... 生产者与消费者已各自装载")

        t_producer = threading.Thread(target=producer_fetch_content_main, name="ProducerThread", daemon=True)
        t_consumer = threading.Thread(target=consumer_auto_sync_main, kwargs={"accounts": ["dahao", "nana"]},
                                      name="ConsumerThread", daemon=True)

        t_producer.start()
        t_consumer.start()

        # 守护主线程，不让其退出
        t_producer.join()
        t_consumer.join()

    except KeyboardInterrupt:
        logger.info("⚠️ 收到退出信号，系统安全中止。")
    except Exception as e:
        logger.error(f"🚨 系统致命错误: {e}", exc_info=True)