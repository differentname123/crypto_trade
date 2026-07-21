# -*- coding: utf-8 -*-
# =====================================================================
# [功能摘要]
#   币安广场"互关裂变"自动化系统。基于MongoDB解耦，生产者实时抓取广场互关线索入库；
#   消费者提取线索并以BFS关系链裂变挖掘高转化目标，执行多账号防风控自动关注。
#
# [输入数据]
#   1. 外部API流: 币安广场帖子数据流、用户多维画像特征、用户关系链列表 (字典/字典列表格式)。
#   2. 本地缓存: user_profile.json 保存已验证用户的画像与预测打分。
#   3. 全局配置: 各账号执行所需的无头浏览器 Session、Cookie 与 CSRF Token。
#
# [数据流转/交互]
#   1. 生产链路: 预设搜索词 -> 币安Feed接口 -> 历史ID比对去重 -> MongoDB落盘持久化。
#   2. 消费链路(挖掘): 历史帖子(正则提取线索) + 账号现有粉丝 -> 裂变引擎 ->
#      API请求用户画像特征 -> 意愿预测漏斗 -> 合格者深挖关系链 -> 高优目标UID池。
#   3. 消费链路(执行): 高优目标UID池 -> 集合运算(自身粉丝>矩阵粉丝>野生线索) ->
#      过滤已关注 -> 带随机防风控休眠的串行关注请求 -> 币安关注API。
#
# [输出数据]
#   副作用: 向币安服务器发起真实的[Follow]状态变更请求；抓取的帖子全量落盘至DB；
#           动态更新并保存含有评估画像的 user_profile.json 缓存。
# =====================================================================

import logging
import random
import time
import threading
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from biance.biance_playwright import get_auth_tokens_robust
from common.common_utils import setup_logger, get_config, read_json, save_json
from biance.biance_squre_api import fetch_binance_feed, toggle_binance_follow, \
    fetch_binance_relations, fetch_binance_user_profile
from common.mongo_db.mongo_base import gen_db_object
from common.mongo_db.mongo_manager import UniversalPostManager

logger = setup_logger(app_name="biance_follow")

# ---------------------------- 全局常量与锁 ----------------------------
COOKIE_UPDATE_INTERVAL_DAYS = 1
MIN_PROBABILITY = 90
MIN_FISSION_PROBABILITY = 85
PROFILE_CACHE_PATH = "user_profile.json"
PRODUCER_SWEEP_INTERVAL = 10000

MUTUAL_FOLLOW_KEYWORDS = [
    "互关", "必回", "互粉", '回关', "互赞", "互评", "互fo", "互助互关", "互关互粉", "互赞互评", "互粉互赞", "互关互赞",
    "互换关注", "关注必回", "点赞必回", "评论必回", "留下评论必回", "必回关", "关必回", "秒回关", "互关秒回", "粉必回",
    "有粉必回", "必须回关", "点赞互关", "诚信互关", "关注报数", "赚积分互助", "广场互关", "广场互粉", "币安互关",
    "币安互粉", "币圈互关", "币圈互粉", "加密货币互粉", "f4f", "follow for follow", "follow4follow", "followback",
    "follow back", "mutual follow", "mutuals", "follow each other", "l4l", "like for like", "like4like", "sub4sub",
    "binance follow for follow", "binance f4f", "binance mutual follow", "follow back binance",
    "crypto follow for follow",
    "crypto f4f", "crypto mutual follow", "follow back crypto"
]

SEARCH_KEYWORDS = [
    "互关", "互粉", "互赞", "互评", "互fo", "涨粉", "粉丝互助", "互关互粉", "关注必回", "必回关",
    "秒回关", "点赞互关", "赚积分互助", "广场互关", "币安互关", "f4f", "follow for follow",
    "followback", "mutual follow", "follow me Binance"
]

ACCOUNT_AUTH_CACHE = {}
auth_cache_lock = threading.Lock()
profile_cache_lock = threading.Lock()


# =====================================================================
# [基础组件层] 数据解析与业务预测引擎
# =====================================================================

def _normalize_probability(prob):
    """统一转化概率格式，防御脏数据"""
    if isinstance(prob, str):
        try:
            return float(prob.replace('%', ''))
        except ValueError:
            return 0.0
    return prob or 0.0


def _is_high_value_target(predict_info):
    """判断是否为最终需要执行关注的高价值目标"""
    return bool(predict_info.get('is_recommended', False)) and \
        _normalize_probability(predict_info.get('probability', 0)) > MIN_PROBABILITY


def _is_worth_fission(predict_info):
    """判断是否值得深挖其关系链作为裂变跳板"""
    return bool(predict_info.get('is_recommended', False)) and \
        _normalize_probability(predict_info.get('probability', 0)) >= MIN_FISSION_PROBABILITY


def _extract_post_id(post):
    """兼容对象与字典双态结构的ID安全提取器"""
    if isinstance(post, dict):
        return post.get("post_id")
    return getattr(post, "post_id", None)


def predict_follow_back(user_info):
    """
    高概率回关预测引擎：通过三层漏斗层层过滤假人/低意愿用户
    """
    follow_count = user_info.get('totalFollowCount', 0)
    follower_count = user_info.get('totalFollowerCount', 0)
    post_count = user_info.get('totalListedPostCount', 0)
    like_count = user_info.get('totalLikeCount', 0)
    modify_time_ms = user_info.get('modifyTime', 0)
    bio = (user_info.get('biography') or '').lower()

    # 漏斗一：风控与高姿态节点拦截
    if user_info.get('lowQuality', False):
        return {"is_recommended": False, "probability": 0, "reason": "官方降权"}
    if user_info.get('userStatus', 1) != 1 or user_info.get('accountStatus', 0) != 0 or user_info.get('blockType',
                                                                                                      0) != 0:
        return {"is_recommended": False, "probability": 0, "reason": "状态异常受限"}
    if user_info.get('verificationType', 0) > 0 or user_info.get('hasCopyTradingEntrance', False):
        return {"is_recommended": False, "probability": 0, "reason": "大V或带单员高姿态"}
    if follow_count > 4200 or follower_count > 1000:
        return {"is_recommended": False, "probability": 0, "reason": "触碰上限或脱离红利期"}

    # 漏斗二：活跃度与真实性过滤
    if modify_time_ms == 0:
        return {"is_recommended": False, "probability": 10, "reason": "无活跃时间戳"}
    days_inactive = (time.time() * 1000 - modify_time_ms) / (1000 * 3600 * 24)
    if days_inactive > 3:
        return {"is_recommended": False, "probability": 10, "reason": "超3天未活跃"}
    if (post_count + like_count) < 5 or follower_count < 20:
        return {"is_recommended": False, "probability": 10, "reason": "数据极低疑似脚本"}

    # 漏斗三：明暗意图判定
    if any(kw in bio for kw in MUTUAL_FOLLOW_KEYWORDS):
        return {"is_recommended": True, "probability": 100, "reason": "T0明牌承诺互关"}

    ratio = follow_count / follower_count
    if not (2.0 <= ratio <= 15.0) or follow_count <= 200:
        return {"is_recommended": False, "probability": 40, "reason": "比例畸形或主动关注基数低"}

    has_holding = bool(user_info.get('publicHoldingScope'))
    has_tags = bool(user_info.get('userTags'))
    has_tipping = user_info.get('tippingControl', 0) == 1
    if not (has_holding or has_tags or has_tipping):
        return {"is_recommended": False, "probability": 40, "reason": "缺乏生态深度绑定"}

    if 3.0 <= ratio <= 10.0:
        return {"is_recommended": True, "probability": 95, "reason": "T1隐性互惠(黄金比例)"}
    return {"is_recommended": True, "probability": 85, "reason": "T1隐性互惠(次优比例)"}


def extract_mutual_follow_users(posts, target_time_str):
    """解析帖子及评论流，正则碰撞提取目标用户UID"""
    try:
        target_timestamp = int(datetime.strptime(target_time_str, "%Y-%m-%d %H:%M:%S").timestamp())
    except ValueError as e:
        logger.error(
            f"[解析模块/时间转换] 格式化时间戳失败 | 关键参数: 【{target_time_str}】 | 结果: 终止抽取 | 可能原因: 传入的时间格式并非 YYYY-MM-DD HH:MM:SS")
        return set()

    extracted_uids = set()
    for post in posts:
        if post.get("publish_time", 0) < target_timestamp:
            continue

        content = post.get("content", {})
        text = ' '.join(f"{content.get('title', '')} {content.get('text_content', '')}".lower().split())

        if any(kw in text for kw in MUTUAL_FOLLOW_KEYWORDS):
            if post.get("author_id"):
                extracted_uids.add(post.get("author_id"))
            for comment in post.get("comments", []):
                if comment.get("author_uid"):
                    extracted_uids.add(comment.get("author_uid"))

    logger.info(
        f"[解析模块/线索抽取] 提取帖子文本线索完成 | 关键参数: 帖子基数【{len(posts)}】, 去重UID【{len(extracted_uids)}】 | 结果: 成功收集公共池基底")
    return extracted_uids


def _get_current_relations(user_name, max_count=10):
    """安全拉取目标用户的关注/粉丝映射表"""
    following_list = fetch_binance_relations(target_username=user_name, relation_type="following",
                                             required_count=max_count)
    following_map = {u.get('username'): u.get('squareUid') for u in following_list if
                     u.get('username') and u.get('squareUid')}

    followers_list = fetch_binance_relations(target_username=user_name, relation_type="followers",
                                             required_count=max_count)
    follower_map = {u.get('username'): u.get('squareUid') for u in followers_list if
                    u.get('username') and u.get('squareUid')}

    return following_map, follower_map


# =====================================================================
# [核心业务层] BFS网络裂变与多账号执行逻辑
# =====================================================================

def _analyze_user_task(user_name, user_info_map, stop_event):
    """
    单用户画像评估与关系链扩散任务
    通过锁机制确保高频并发下的全局缓存一致性
    【修改说明】: 在所有 return 语句末尾追加了 is_new 布尔值，用于区分是否是新请求 (True为新请求，False为读缓存)
    """
    if stop_event.is_set():
        return user_name, None, None, None, None, False

    with profile_cache_lock:
        user_data = user_info_map.get(user_name, {})

    # 1. 命中缓存层评估
    if 'predict_info' in user_data:
        predict_info = user_data['predict_info']
        if not (_is_high_value_target(predict_info) or _is_worth_fission(predict_info)):
            return user_name, user_data.get('squareUid'), predict_info, {}, {}, False

        if 'following' in user_data and 'followers' in user_data:
            return user_name, user_data.get('squareUid'), predict_info, user_data['following'], user_data[
                'followers'], False

        if user_data.get('squareUid'):
            square_uid = user_data.get('squareUid')
            following_map, follower_map = _get_current_relations(user_name, max_count=1000)
            with profile_cache_lock:
                user_info_map[user_name]['following'] = following_map
                user_info_map[user_name]['followers'] = follower_map
            return user_name, square_uid, predict_info, following_map, follower_map, False

    # 2. 网络I/O与漏斗评估
    raw_profile = fetch_binance_user_profile(user_name)
    if not raw_profile:
        predict_info = {"is_recommended": False, "probability": 0, "reason": "接口失效或用户注销"}
        with profile_cache_lock:
            user_info_map[user_name] = {'predict_info': predict_info}
        return user_name, None, predict_info, {}, {}, True

    user_info = {
        'squareUid': raw_profile.get('squareUid'),
        'totalFollowCount': raw_profile.get('totalFollowCount', 0),
        'totalFollowerCount': raw_profile.get('totalFollowerCount', 0),
        'totalListedPostCount': raw_profile.get('totalListedPostCount', 0),
        'totalLikeCount': raw_profile.get('totalLikeCount', 0),
        'lowQuality': raw_profile.get('lowQuality', False),
        'userStatus': raw_profile.get('userStatus', 1),
        'accountStatus': raw_profile.get('accountStatus', 0),
        'blockType': raw_profile.get('blockType', 0),
        'modifyTime': raw_profile.get('modifyTime', 0),
        'biography': raw_profile.get('biography', ''),
        'verificationType': raw_profile.get('verificationType', 0),
        'hasCopyTradingEntrance': raw_profile.get('hasCopyTradingEntrance', False),
        'publicHoldingScope': raw_profile.get('publicHoldingScope', []),
        'userTags': raw_profile.get('userTags', []),
        'tippingControl': raw_profile.get('tippingControl', 0)
    }

    predict_info = predict_follow_back(user_info)
    square_uid = user_info.get('squareUid')

    with profile_cache_lock:
        if user_name not in user_info_map:
            user_info_map[user_name] = {}
        user_info_map[user_name].update(user_info)
        user_info_map[user_name]['predict_info'] = predict_info

        if not (_is_high_value_target(predict_info) or _is_worth_fission(predict_info)):
            preserved_keys = {'predict_info', 'squareUid'}
            keys_to_delete = [k for k in user_info_map[user_name] if k not in preserved_keys]
            for k in keys_to_delete:
                del user_info_map[user_name][k]

    if not (_is_high_value_target(predict_info) or _is_worth_fission(predict_info)):
        return user_name, square_uid, predict_info, {}, {}, True

    # 3. 对达标者获取深层关系链供下游使用
    following_map, follower_map = _get_current_relations(user_name, max_count=1000)
    with profile_cache_lock:
        user_info_map[user_name]['following'] = following_map
        user_info_map[user_name]['followers'] = follower_map

    return user_name, square_uid, predict_info, following_map, follower_map, True


def get_worth_following_list(initial_user_name_list, target_count):
    """广度优先裂变引擎：基于种子用户的关系链网，高并发筛出强意愿互关目标群体"""
    if not initial_user_name_list:
        logger.warning(
            "[裂变引擎/启动校验] 缺少种子数据 | 关键参数: 【种子数=0】 | 结果: 终止裂变 | 可能原因: 矩阵账号无关注无粉丝")
        return []

    logger.info(
        f"[裂变引擎/初始化] 启动拓扑扩散挖掘 | 关键参数: 初始种子【{len(initial_user_name_list)}】, 目标获取量【{target_count}】 | 结果: 引擎挂载就绪")

    user_info_map = read_json(PROFILE_CACHE_PATH) or {}
    valid_square_uids = set()
    evaluated_users = set()
    pending_user_names = list(initial_user_name_list)
    turn_count = 1

    # 【修改说明】: 新增两个计数器，统计来源数据
    cached_valid_count = 0
    new_valid_count = 0

    while len(valid_square_uids) < target_count and pending_user_names:
        next_turn_user_names = []
        stop_event = threading.Event()

        with ThreadPoolExecutor(max_workers=30) as executor:
            future_to_user = {}
            for uname in pending_user_names:
                if uname in evaluated_users:
                    continue
                evaluated_users.add(uname)
                future_to_user[executor.submit(_analyze_user_task, uname, user_info_map, stop_event)] = uname

            for future in as_completed(future_to_user):
                try:
                    # 【修改说明】: 拆包时增加接收 is_new 标识
                    uname, uid, predict_info, following_map, follower_map, is_new = future.result()
                    if predict_info is None:
                        continue

                    if uid:
                        if following_map or follower_map:
                            next_turn_user_names.extend(following_map.keys())
                            next_turn_user_names.extend(follower_map.keys())

                        if _is_high_value_target(predict_info):
                            # 【修改说明】: 确保只对首次加入集合的目标进行统计
                            if str(uid) not in valid_square_uids:
                                valid_square_uids.add(str(uid))

                                if is_new:
                                    new_valid_count += 1
                                else:
                                    cached_valid_count += 1

                                logger.info(
                                    f"[裂变引擎/命中拦截] 挖掘到高潜用户 | 关键参数: 用户【{uname}】, 评分【{_normalize_probability(predict_info.get('probability', 0)):.0f}】, 来源【{'新提取' if is_new else '本地缓存'}】 | 结果: 已达标进度【{len(valid_square_uids)}/{target_count}】")

                                if len(valid_square_uids) >= target_count:
                                    logger.info(
                                        "[裂变引擎/边界控制] 已达成获取额度 | 关键参数: 【目标阈值触发】 | 结果: 阻断当前轮次所有剩余并发任务")
                                    stop_event.set()
                                    break
                except Exception as e:
                    logger.error(
                        f"[裂变引擎/并发流转] 用户画像评估发生雪崩 | 关键参数: 并发任务报错 | 结果: 跳过当前任务 | 可能原因: API风控或内部网络中断 [{e}]")

        with profile_cache_lock:
            save_json(PROFILE_CACHE_PATH, user_info_map)

        pending_user_names = list(set(next_turn_user_names) - evaluated_users)
        turn_count += 1

    # 【修改说明】: 任务完结的日志中打印出新挖掘与本地缓存的数量对比
    logger.info(
        f"[裂变引擎/任务完结] 图层扩散搜索完毕 | 关键参数: 产出量【{len(valid_square_uids)}】(其中新挖掘: {new_valid_count}个, 本地缓存: {cached_valid_count}个), 过滤总计【{len(evaluated_users)}】 | 结果: 返回高优UID集合")
    return list(valid_square_uids)


def _sync_single_account_logic(user_key, global_fans_uids, allocated_wild_uids):
    """
    单账号闭环：提取全局粉丝与专属探路流量，先保证VIP粉丝全覆盖，再执行剩余份额的新客探索。
    """
    my_name = get_config(f"{user_key}_name")
    browser_session_dir = get_config(f"{user_key}_browser_session_dir")
    current_time = time.time()
    update_interval_seconds = COOKIE_UPDATE_INTERVAL_DAYS * 24 * 3600

    with auth_cache_lock:
        cache_data = ACCOUNT_AUTH_CACHE.get(user_key, {})
        needs_update = not cache_data or (current_time - cache_data.get('last_update', 0) > update_interval_seconds)

    if needs_update:
        logger.info(
            f"[身份认证/凭证刷新] 账号凭证为空或过期 | 关键参数: 账号【{user_key}】 | 结果: 调用无头浏览器重置鉴权状态")
        my_cookies, csrf_token = get_auth_tokens_robust(browser_session_dir)
        with auth_cache_lock:
            ACCOUNT_AUTH_CACHE[user_key] = {
                'cookies': my_cookies,
                'csrf_token': csrf_token,
                'last_update': current_time
            }
    else:
        with auth_cache_lock:
            my_cookies = ACCOUNT_AUTH_CACHE[user_key]['cookies']
            csrf_token = ACCOUNT_AUTH_CACHE[user_key]['csrf_token']

    if not all([my_cookies, csrf_token, my_name]):
        with auth_cache_lock:
            if user_key in ACCOUNT_AUTH_CACHE:
                del ACCOUNT_AUTH_CACHE[user_key]
        logger.error(
            f"[业务中断/配置断档] 执行参数不完整 | 关键参数: 账号【{user_key}】 | 结果: 强制退出该账号逻辑 | 可能原因: 环境变量未配置或无头浏览器抓取Cookie失败")
        return

    # 拉取当前账号自身最新的关注状态
    following_map, _ = _get_current_relations(my_name, max_count=10000)
    my_following_uids = set(following_map.values())

    # ---------------- 核心装填逻辑：绝对优先级排序 ----------------
    # 1. 提取全矩阵粉丝中的未关注对象作为【优先队列】（保证所有粉丝必须先被关注）
    vip_queue = global_fans_uids - my_following_uids

    # 2. 提取分发给该账号的独有野生线索作为【探索队列】（防撞车双重保险）
    explore_queue = set(allocated_wild_uids) - my_following_uids

    # 3. 严格按照先后顺序合并列表，截取前 100 个名额，完美实现粉丝绝对优先策略
    final_uids_to_follow = (list(vip_queue) + list(explore_queue))[:100]
    # -----------------------------------------------------------

    logger.info(
        f"[聚合调度/队列装填] 单账号待关注分配盘点 | 关键参数: 账号【{user_key}】 VIP必回关粉丝【{len(vip_queue)}】, 专属探索线索【{len(explore_queue)}】 | 结果: 截取并锁定 【{len(final_uids_to_follow)}】 个指标")

    if not final_uids_to_follow:
        return

    success_count = 0
    total = len(final_uids_to_follow)
    for index, uid in enumerate(final_uids_to_follow, 1):
        is_success = toggle_binance_follow(uid, "follow", my_cookies, csrf_token)
        if is_success:
            success_count += 1

        logger.info(
            f"[网络交互/行为执行] 触发账号关注动作 | 关键参数: 账号【{user_key}】, 进度【{index}/{total}】, UID【{uid}】 | 结果: 【{'成功' if is_success else '失败'}】")

        if index < total:
            sleep_time = random.uniform(60, 90)
            time.sleep(sleep_time)

    logger.info(
        f"[调度流转/账号完结] 单账号执行流闭环完毕 | 关键参数: 账号【{user_key}】, 触达总量【{total}】, 成功【{success_count}】 | 结果: 释放线程资源")


def consumer_auto_sync_main(accounts=None):
    """
    消费者总干线：构建全局去重基座，提取VIP矩阵粉丝流，并隔离切割野生流量下发执行。
    """
    if accounts is None:
        accounts = ["dahao", "nana"]

    post_manager = UniversalPostManager(gen_db_object())
    logger.info(f"[总控系统/进程派发] 消费者集群系统就绪 | 关键参数: 激活账号【{accounts}】 | 结果: 进入持续性清剿循环")

    while True:
        try:
            # 1. 获取基础野生线索 (发帖+裂变)
            posts_list = post_manager.find_posts_by_source("biance", limit=50000)
            target_time_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
            shared_post_uids = extract_mutual_follow_users(posts_list, target_time_str)

            # 2. 初始化全局数据基座
            seed_user_names = set()
            global_following_uids = set() # 矩阵所有账号的已关注池(用于绝对隔离去重)
            global_fans_uids = set()      # 矩阵所有账号的粉丝大集合(最高优先级VIP)

            for acc in accounts:
                my_name = get_config(f"{acc}_name")
                if my_name:
                    f_map, follower_map = _get_current_relations(my_name, max_count=10000)
                    seed_user_names.update(f_map.keys())
                    global_following_uids.update(f_map.values())
                    global_fans_uids.update(follower_map.values())

            # 获取裂变池
            global_worth_uids = get_worth_following_list(list(seed_user_names), target_count=1000)
            global_wild_uids = shared_post_uids.union(set(global_worth_uids))

            # 3. 核心清洗：野生池 减去 所有账号已关注的 减去 所有粉丝，留下纯净且未开垦的荒地
            pure_wild_uids = list(global_wild_uids - global_following_uids - global_fans_uids)

            logger.info(
                f"[资源调度/全量统筹] 聚合公网线索与矩阵资产 | 关键参数: 帖子线索【{len(shared_post_uids)}】, 裂变输血【{len(global_worth_uids)}】, 矩阵全网粉丝池【{len(global_fans_uids)}】, 全局去重后纯净探索池【{len(pure_wild_uids)}】 | 结果: 分发至各节点执行")

            if not global_fans_uids and not pure_wild_uids:
                logger.info(
                    "[总控系统/流量干涸] 未嗅探到可用靶点数据 | 关键参数: 【全局池=0】 | 结果: 休眠10分钟后启动补偿机制")
                time.sleep(600)
                continue

            # 4. 隔离派发任务
            with ThreadPoolExecutor(max_workers=len(accounts)) as executor:
                futures = []
                for i, acc in enumerate(accounts):
                    # 通过切片法 (slice) 实现流量均分且绝对不重叠
                    allocated_wild_uids = pure_wild_uids[i::len(accounts)]
                    futures.append(
                        executor.submit(_sync_single_account_logic, acc, global_fans_uids, allocated_wild_uids)
                    )
                for f in as_completed(futures):
                    f.result()

            logger.info(
                "[总控系统/安全休眠] 平台并发执行策略本轮落幕 | 关键参数: 【全量账号归零】 | 结果: 休眠 3600 秒防强风控追踪")
            time.sleep(3600)

        except Exception as e:
            logger.error(
                f"[系统崩溃/主干线阻断] 消费者顶层心跳意外终止 | 关键参数: 【循环抛错】 | 结果: 短休眠60秒后复活 | 可能原因: 币安网关拒绝响应或数据库连接断裂 [{e}]",
                exc_info=True)
            time.sleep(60)


# =====================================================================
# [生产者] 增量抓取与DB投递持久化
# =====================================================================

def producer_fetch_content_main():
    """
    生产者死循环：不间断通过广场接口打捞最新动向，物理解耦下放至DB。
    """
    post_manager = UniversalPostManager(gen_db_object())
    logger.info("[总控系统/进程派发] 生产者探针系统就绪 | 关键参数: 关键词字典【加载完成】 | 结果: 进入持续性探活抓取")

    while True:
        try:
            time.sleep(PRODUCER_SWEEP_INTERVAL)
            existing_posts = post_manager.find_posts_by_source("biance", limit=50000)
            existing_ids = {str(_extract_post_id(p)) for p in existing_posts if _extract_post_id(p)}

            total_new = 0
            for search_key in SEARCH_KEYWORDS:
                search_data = fetch_binance_feed(keyword=search_key, count=1000, existing_ids=existing_ids)
                fetched = len(search_data) if search_data else 0
                total_new += fetched

                if search_data:
                    post_manager.upsert_posts(search_data)

                logger.info(
                    f"[网络交互/资源下放] 单关键词定向打捞完毕 | 关键参数: 搜索词【{search_key}】, 新增入库【{fetched}】条 | 结果: 落盘并硬休眠5秒防断流")
                time.sleep(5)

            logger.info(
                f"[调度流转/轮次交替] 探针全覆盖扫描完成 | 关键参数: 记忆库【{len(existing_ids)}】, 增量下放【{total_new}】 | 结果: 休眠【{PRODUCER_SWEEP_INTERVAL}s】等待下一轮触发")

        except Exception as e:
            logger.error(
                f"[系统崩溃/主干线阻断] 生产者探针作业意外脱轨 | 关键参数: 【抓取/入库阶段】 | 结果: 短休眠10秒后重拨 | 可能原因: 币安API封禁当前机器IP或MongoDB进程假死 [{e}]",
                exc_info=True)
            time.sleep(10)


# =====================================================================
# 程序引擎启动入口
# =====================================================================
if __name__ == "__main__":
    try:
        logger.info(
            "[运行时框架/引导激活] 主程序内存空间分配完毕 | 关键参数: 【生产者挂载/消费者挂载】 | 结果: 双擎并发点火")

        t_producer = threading.Thread(target=producer_fetch_content_main, name="ProducerThread", daemon=True)
        t_consumer = threading.Thread(target=consumer_auto_sync_main, kwargs={"accounts": ["dahao", "nana", "jie", "mama"]},
                                      name="ConsumerThread", daemon=True)

        t_producer.start()
        t_consumer.start()

        t_producer.join()
        t_consumer.join()

    except KeyboardInterrupt:
        logger.info("[运行时框架/降维打击] 接收到终端阻断信号 | 关键参数: 【Ctrl+C】 | 结果: 释放内存安全关机")
    except Exception as e:
        logger.error(
            f"[系统崩溃/全局灾难] 底层守护引擎被击穿 | 关键参数: 【致命核心抛错】 | 结果: 进程彻底死亡 | 可能原因: 系统环境资源耗尽或Python解释器异常 [{e}]",
            exc_info=True)