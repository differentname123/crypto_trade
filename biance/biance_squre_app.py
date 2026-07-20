# -*- coding: utf-8 -*-
# =====================================================================
# [功能摘要]
#   币安广场"互关裂变"自动化系统。生产者持续抓帖入库，消费者从库中挖掘
#   高回关概率用户并多账号并行自动关注，二者经 MongoDB 物理解耦。
#
# [输入数据]
#   1. 币安广场 API：帖子流(fetch_binance_feed)、用户画像(fetch_binance_user_profile)、
#      关系链(fetch_binance_relations)，均为 dict / list[dict] 结构。
#   2. 本地缓存 user_profile.json：{username: {squareUid, predict_info, following, followers, ...}}。
#   3. 配置项 get_config：各账号的 cookie / csrf / name。
#
# [数据流转/交互]
#   生产者:  SEARCH_KEYWORDS ─fetch_feed→ 新帖(以DB existing_ids去重) ─upsert→ MongoDB
#   消费者:  MongoDB帖子 ─互关提取→ 池A(shared_post_uids)
#            账号种子 ─BFS裂变(predict_follow_back判定+关系链扩散)→ 池B(global_worth_uids)
#            池A∪池B ─各账号差集运算(减去已关注/补回未回关)→ 待关注UID
#            待关注UID ─toggle_binance_follow→ 执行关注(带随机休眠防风控)
#
# [输出数据]
#   副作用: 在币安平台对目标用户执行"关注"动作; 帖子数据持久化至 MongoDB;
#           用户画像与预测结果缓存至 user_profile.json。
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

# ---------------------------- 全局常量 ----------------------------
COOKIE_UPDATE_INTERVAL_DAYS = 1  # [新增配置] Cookie 更新间隔天数，后续可在此任意调整

MIN_PROBABILITY = 90  # 严格判定为高价值目标的概率下限(需 > 此值)
MIN_FISSION_PROBABILITY = 85  # 裂变中继跳板阈值(需 >= 此值，允许挖掘其关系链)
PROFILE_CACHE_PATH = "user_profile.json"
PRODUCER_SWEEP_INTERVAL = 10000  # 生产者两轮全量抓取之间的休眠间隔(秒)

# 账号 Cookie 与 CSRF 全局缓存 (结构: {user_key: {'cookies': ..., 'csrf_token': ..., 'last_update': timestamp}})
ACCOUNT_AUTH_CACHE = {}

# 帖子/评论区互关线索提取关键词(高覆盖版)
MUTUAL_FOLLOW_KEYWORDS = [
    "互关", "必回", "互粉", '回关', "互赞", "互评", "互fo", "互助互关", "互关互粉", "互赞互评", "互粉互赞", "互关互赞",
    "互换关注",
    "关注必回", "点赞必回", "评论必回", "留下评论必回", "必回关", "关必回", "秒回关", "互关秒回", "粉必回",
    "有粉必回", "必须回关",
    "点赞互关", "诚信互关", "关注报数", "赚积分互助", "广场互关", "广场互粉", "币安互关", "币安互粉", "币圈互关",
    "币圈互粉", "加密货币互粉",
    "f4f", "follow for follow", "follow4follow", "followback", "follow back", "mutual follow", "mutuals",
    "follow each other", "l4l", "like for like", "like4like", "sub4sub", "binance follow for follow",
    "binance f4f", "binance mutual follow", "follow back binance", "crypto follow for follow", "crypto f4f",
    "crypto mutual follow", "follow back crypto"
]

# 生产者按词抓取的关键词
SEARCH_KEYWORDS = [
    "互关", "互粉", "互赞", "互评", "互fo", "涨粉", "粉丝互助", "互关互粉", "关注必回", "必回关",
    "秒回关", "点赞互关", "赚积分互助", "广场互关", "币安互关", "f4f", "follow for follow",
    "followback", "mutual follow", "follow me Binance"
]


# =====================================================================
# [基础组件层] 判定引擎、线索提取与关系链获取
# =====================================================================

def _normalize_probability(prob):
    """兼容历史缓存中概率可能为 '95%' 字符串的情况，统一归一为浮点数，防止比较报错。"""
    if isinstance(prob, str):
        try:
            return float(prob.replace('%', ''))
        except ValueError:
            return 0.0
    return prob or 0.0


def _is_high_value_target(predict_info):
    """高价值判定：被推荐且概率严格大于阈值，判定为最终需要关注的目标。"""
    return bool(predict_info.get('is_recommended', False)) and \
        _normalize_probability(predict_info.get('probability', 0)) > MIN_PROBABILITY


def _is_worth_fission(predict_info):
    return bool(predict_info.get('is_recommended', False)) and \
        _normalize_probability(predict_info.get('probability', 0)) >= MIN_FISSION_PROBABILITY


def predict_follow_back(user_info):
    """
    极高概率回关预测引擎 (极致 Precision 版)
    原则: 宁可漏掉大量普通用户，也绝不误判一个低意愿/假活人用户。
    """
    # --- 基础数据提取 ---
    follow_count = user_info.get('totalFollowCount', 0)
    follower_count = user_info.get('totalFollowerCount', 0)
    post_count = user_info.get('totalListedPostCount', 0)
    like_count = user_info.get('totalLikeCount', 0)
    modify_time_ms = user_info.get('modifyTime', 0)
    bio = (user_info.get('biography') or '').lower()

    # =======================================================
    # 漏斗一：风控与物理死亡线 (绝对否决，斩杀异常/高姿态)
    # =======================================================
    # 1. 官方状态风控
    if user_info.get('lowQuality', False):
        return {"is_recommended": False, "probability": 0, "reason": "VETO: 官方标记低质量/降权号"}
    if user_info.get('userStatus', 1) != 1 or user_info.get('accountStatus', 0) != 0 or user_info.get('blockType',
                                                                                                      0) != 0:
        return {"is_recommended": False, "probability": 0, "reason": "VETO: 账号状态异常(受限/封禁/静默)"}

    # 2. 高姿态与带单节点过滤
    if user_info.get('verificationType', 0) > 0:
        return {"is_recommended": False, "probability": 0, "reason": "VETO: 已认证机构/大V，高姿态无回关意愿"}
    if user_info.get('hasCopyTradingEntrance', False):
        return {"is_recommended": False, "probability": 0, "reason": "VETO: 拥有带单入口，属于KOL吸粉节点"}

    # 3. 物理上限与脱离饥渴期过滤
    if follow_count > 4200:
        return {"is_recommended": False, "probability": 0,
                "reason": f"VETO: 关注数({follow_count})逼近系统上限，丧失回关能力"}
    if follower_count > 1000:
        return {"is_recommended": False, "probability": 0,
                "reason": f"VETO: 粉丝数({follower_count})已脱离冷启动饥渴期"}

    # =======================================================
    # 漏斗二：僵尸与脚本死亡线 (必须证明是活人且能看到通知)
    # =======================================================
    # 1. 沉寂度过滤 (极端严格：超3天未活跃直接抛弃)
    if modify_time_ms == 0:
        return {"is_recommended": False, "probability": 10, "reason": "REJECT: 缺乏活跃时间戳"}
    days_inactive = (time.time() * 1000 - modify_time_ms) / (1000 * 3600 * 24)
    if days_inactive > 3:
        return {"is_recommended": False, "probability": 10,
                "reason": f"REJECT: 距今超 {days_inactive:.1f} 天未活跃，通知送达率无保障"}

    # 2. 纯工具号防伪验证
    if (post_count + like_count) < 5:
        return {"is_recommended": False, "probability": 10,
                "reason": "REJECT: 发帖+点赞总和极低，疑似纯刷关注工具号/不看内容"}

    # 3. 社会化防反噬机制
    if follower_count < 20:
        return {"is_recommended": False, "probability": 10,
                "reason": f"REJECT: 粉丝数过低({follower_count})，缺乏账号经营意识或被降权"}

    # =======================================================
    # 漏斗三：意图锁定层 (最终目标分流判定)
    # =======================================================
    # 路径 A: T0 级明牌意图直通车
    if any(kw in bio for kw in MUTUAL_FOLLOW_KEYWORDS):
        return {"is_recommended": True, "probability": 100,
                "reason": "PASS(T0): 近3天活跃真人，无风控异常且未达关注上限，简介明牌承诺互关。"}

    # 路径 B: 隐性强意图 (数据画像一致性)
    # 计算比例 (前面已拦截 follower_count < 20，故不会除零)
    ratio = follow_count / follower_count

    # 1. 比例畸形拦截
    if not (2.0 <= ratio <= 15.0):
        return {"is_recommended": False, "probability": 20,
                "reason": f"REJECT: 关注/粉丝比({ratio:.1f})不在[2.0, 15.0]有效区间，疑似脚本/黑户或意愿不足"}

    # 2. 主动关注基数拦截
    if follow_count <= 200:
        return {"is_recommended": False, "probability": 40,
                "reason": f"REJECT: 主动关注基数({follow_count})不足200，缺乏主动互惠习惯"}

    # 3. 生态深度防伪 (必须证明是高频打开APP的用户)
    has_holding = bool(user_info.get('publicHoldingScope'))
    has_tags = bool(user_info.get('userTags'))
    has_tipping = user_info.get('tippingControl', 0) == 1

    if not (has_holding or has_tags or has_tipping):
        return {"is_recommended": False, "probability": 40,
                "reason": "REJECT: 比例虽达标，但缺乏公开持仓/系统标签/打赏功能等深度生态绑定，活人一致性未达标"}

    # 通过全部苛刻条件，判定为隐性强互关目标，按比例分配极致分数
    if 3.0 <= ratio <= 10.0:
        return {"is_recommended": True, "probability": 95,
                "reason": f"PASS(T1): 深度生态活人，关注基数充足({follow_count})，比例({ratio:.1f})处于黄金互惠区间。"}
    else:
        return {"is_recommended": True, "probability": 85,
                "reason": f"PASS(T1): 深度生态活人，关注基数充足({follow_count})，比例({ratio:.1f})处于次优互惠区间。"}


def extract_mutual_follow_users(posts, target_time_str):
    """从帖子流中提取互关线索 UID：命中关键词的作者及其评论区用户，并输出结构化统计。"""
    try:
        target_timestamp = int(datetime.strptime(target_time_str, "%Y-%m-%d %H:%M:%S").timestamp())
    except ValueError as e:
        logger.error(f"❌ [互关提取] 时间格式错误(应为 'YYYY-MM-DD HH:MM:SS'): {e}")
        return set()

    extracted_uids = set()
    time_filtered = content_filtered = matched_posts = comment_uids = 0

    for post in posts:
        if post.get("publish_time", 0) < target_timestamp:
            time_filtered += 1
            continue

        content = post.get("content", {})
        text = ' '.join(f"{content.get('title', '')} {content.get('text_content', '')}".lower().split())

        if not any(kw in text for kw in MUTUAL_FOLLOW_KEYWORDS):
            content_filtered += 1
            continue

        matched_posts += 1
        if post.get("author_id"):
            extracted_uids.add(post.get("author_id"))
        for comment in post.get("comments", []):
            if comment.get("author_uid"):
                extracted_uids.add(comment.get("author_uid"))
                comment_uids += 1

    logger.info(
        f"🔍 [互关提取] 完成 | 扫描帖子:{len(posts)} | 时间过滤:{time_filtered} | 无关键词过滤:{content_filtered} | "
        f"命中帖子:{matched_posts} | 评论区抽取:{comment_uids} | 去重后总UID:{len(extracted_uids)}"
    )
    return extracted_uids


def _get_current_relations(user_name, max_count=10):
    """拉取指定用户当前的关注集与粉丝集，返回两个 {username: squareUid} 映射。"""
    following_list = fetch_binance_relations(target_username=user_name, relation_type="following",
                                             required_count=max_count)
    following_map = {u.get('username'): u.get('squareUid') for u in following_list
                     if u.get('username') and u.get('squareUid')}

    followers_list = fetch_binance_relations(target_username=user_name, relation_type="followers",
                                             required_count=max_count)
    follower_map = {u.get('username'): u.get('squareUid') for u in followers_list
                    if u.get('username') and u.get('squareUid')}

    return following_map, follower_map


def _get_uids_from_recent_posts(post_manager, days_ago=7, limit=50000):
    """从近 days_ago 天的库存帖子中提取互关线索 UID 集合。"""
    posts_list = post_manager.find_posts_by_source("biance", limit=limit)
    target_time_str = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    return extract_mutual_follow_users(posts_list, target_time_str)


# =====================================================================
# [核心业务层] 扁平化单任务闭环与裂变引擎
# =====================================================================

def _analyze_user_task(user_name, user_info_map, stop_event):
    """单用户分析闭环：修复历史缓存击穿引发的二次降权Bug"""
    if stop_event.is_set():
        return user_name, None, None, None, None

    user_data = user_info_map.get(user_name, {})

    # 1. 命中缓存：复用历史预测，避免重复网络请求
    if 'predict_info' in user_data:
        predict_info = user_data['predict_info']
        is_target = _is_high_value_target(predict_info)
        is_bridge = _is_worth_fission(predict_info)

        # 既不是关注目标，也不是裂变跳板，直接返回
        if not (is_target or is_bridge):
            return user_name, user_data.get('squareUid'), predict_info, {}, {}

        # 如果是目标或跳板，且已经拥有完整关系链，直接复用返回
        if 'following' in user_data and 'followers' in user_data:
            return user_name, user_data.get('squareUid'), predict_info, \
                user_data['following'], user_data['followers']

        # 命中目标/跳板，但缺少关系链（通常是历史跑过的 85 分数据被清除了）
        # 此时只需直接拉关系链即可，千万不可让残缺的 user_data 掉入下方的预测函数被洗成 10 分！
        if user_data.get('squareUid'):
            square_uid = user_data.get('squareUid')
            following_map, follower_map = _get_current_relations(user_name, max_count=1000)
            user_info_map[user_name]['following'] = following_map
            user_info_map[user_name]['followers'] = follower_map
            return user_name, square_uid, predict_info, following_map, follower_map

    # 2. 网络层：完全未评估过的新用户，正常拉画像
    if 'squareUid' not in user_data:
        raw_profile = fetch_binance_user_profile(user_name)
        if not raw_profile:
            predict_info = {"is_recommended": False, "probability": 0, "reason": "API拉取失败或用户注销"}
            user_info_map[user_name] = {'predict_info': predict_info}
            return user_name, None, predict_info, {}, {}

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
        user_info_map.setdefault(user_name, {}).update(user_info)
    else:
        user_info = user_data

    # 3. 纯血引擎预测并落缓存
    predict_info = predict_follow_back(user_info)
    square_uid = user_info.get('squareUid')
    user_info_map[user_name]['predict_info'] = predict_info

    is_target = _is_high_value_target(predict_info)
    is_bridge = _is_worth_fission(predict_info)

    if not (is_target or is_bridge):
        preserved = {'predict_info', 'squareUid'}
        for k in [key for key in user_info_map[user_name] if key not in preserved]:
            del user_info_map[user_name][k]
        return user_name, square_uid, predict_info, {}, {}

    # 4. 高价值或优质跳板，投入网络 I/O 拉取关系链深挖
    following_map, follower_map = _get_current_relations(user_name, max_count=1000)
    user_info_map[user_name]['following'] = following_map
    user_info_map[user_name]['followers'] = follower_map

    return user_name, square_uid, predict_info, following_map, follower_map


def get_worth_following_list(initial_user_name_list, target_count):
    """广度优先裂变引擎：以种子用户为起点，通过高价值号的关系链逐轮扩散，直至凑够目标数量的优质 UID。"""
    if not initial_user_name_list:
        logger.error("❌ [裂变引擎] 初始种子列表为空，无法起盘。")
        return []

    logger.info(f"🕸️ [裂变引擎] 启动 | 目标高价值UID:{target_count} | 初始种子:{len(initial_user_name_list)}")

    user_info_map = read_json(PROFILE_CACHE_PATH)
    valid_square_uids = set()
    evaluated_users = set()
    pending_user_names = list(initial_user_name_list)
    turn_count = 1

    while len(valid_square_uids) < target_count and pending_user_names:
        logger.info(f"🚀 [裂变引擎] 第 {turn_count} 轮 | 待评估:{len(pending_user_names)} | "
                    f"已达标:{len(valid_square_uids)}/{target_count}")

        next_turn_user_names = []
        stop_event = threading.Event()

        # 全局唯一线程池，网络与逻辑操作均内聚于单任务，杜绝嵌套线程饥饿
        with ThreadPoolExecutor(max_workers=30) as executor:
            future_to_user = {}
            for uname in pending_user_names:
                if uname in evaluated_users:
                    continue
                evaluated_users.add(uname)
                future_to_user[executor.submit(_analyze_user_task, uname, user_info_map, stop_event)] = uname

            for future in as_completed(future_to_user):
                try:
                    uname, uid, predict_info, following_map, follower_map = future.result()
                    if predict_info is None:
                        continue  # 任务已被 stop_event 终止

                    if uid:
                        # 无论高价值还是跳板，只要有关系链(前面拦截了无价值的号返回空字典)，就加入下一轮种子池
                        if following_map or follower_map:
                            next_turn_user_names.extend(following_map.keys())
                            next_turn_user_names.extend(follower_map.keys())

                        # 只有真正满足 95 分的高价值号，才会被塞入关注池并打印命中日志
                        if _is_high_value_target(predict_info):
                            valid_square_uids.add(str(uid))
                            logger.info(
                                f"🌟 [裂变引擎] 命中高潜:{uname} | "
                                f"概率:{_normalize_probability(predict_info.get('probability', 0)):.0f} | "
                                f"累计:{len(valid_square_uids)}/{target_count}"
                            )
                            if len(valid_square_uids) >= target_count:
                                logger.info("🎉 [裂变引擎] 目标达标，广播阻断信号终止本轮剩余任务。")
                                stop_event.set()
                                break
                except Exception as e:
                    logger.error(f"❌ [裂变引擎] 用户分析任务异常: {e}")

        save_json(PROFILE_CACHE_PATH, user_info_map)
        pending_user_names = list(set(next_turn_user_names) - evaluated_users)
        turn_count += 1

    logger.info(f"🏁 [裂变引擎] 结束 | 产出高价值UID:{len(valid_square_uids)} | 累计评估用户:{len(evaluated_users)}")
    return list(valid_square_uids)


# =====================================================================
# [消费者] 提取公共优质池并多账号并行执行关注
# =====================================================================

def _sync_single_account_logic(user_key, global_potential_uids, all_accounts_followers_uids):
    """单账号关注闭环：以自身关系链做差集，实行严格的三级梯队优先关注策略，并带随机休眠地执行关注。"""
    global ACCOUNT_AUTH_CACHE

    my_name = get_config(f"{user_key}_name")
    browser_session_dir = get_config(f"{user_key}_browser_session_dir")

    current_time = time.time()
    update_interval_seconds = COOKIE_UPDATE_INTERVAL_DAYS * 24 * 3600

    # 按天检查是否需要重新获取 Cookie 缓存
    if (user_key not in ACCOUNT_AUTH_CACHE) or \
            (current_time - ACCOUNT_AUTH_CACHE[user_key]['last_update'] > update_interval_seconds):
        logger.info(f"🔄 [账号:{user_key}] Cookie 缓存为空或已过期(> {COOKIE_UPDATE_INTERVAL_DAYS} 天)，正在重新获取...")
        my_cookies, csrf_token = get_auth_tokens_robust(browser_session_dir)

        ACCOUNT_AUTH_CACHE[user_key] = {
            'cookies': my_cookies,
            'csrf_token': csrf_token,
            'last_update': current_time
        }
    else:
        logger.info(f"⚡ [账号:{user_key}] 使用缓存的 Cookie 信息 (未超过 {COOKIE_UPDATE_INTERVAL_DAYS} 天)")
        my_cookies = ACCOUNT_AUTH_CACHE[user_key]['cookies']
        csrf_token = ACCOUNT_AUTH_CACHE[user_key]['csrf_token']

    if not all([my_cookies, csrf_token, my_name]):
        if user_key in ACCOUNT_AUTH_CACHE:
            del ACCOUNT_AUTH_CACHE[user_key]  # 如果失败则清除缓存
        logger.error(f"❌ [账号:{user_key}] 缺少配置(cookie/csrf/name)，任务终止。")
        return

    logger.info(f"========== 🚀 [账号:{user_key}] 开始执行关注逻辑 ==========")

    following_map, follower_map = _get_current_relations(my_name, max_count=10000)
    following_uids = set(following_map.values())
    followers_uids = set(follower_map.values())

    # ================= 核心优先级分层逻辑 =================

    # 梯队 1：自己的粉丝中还没回关的
    tier_1_own_fans = followers_uids - following_uids

    # 梯队 2：矩阵其他号的粉丝 (所有号粉丝 - 自己的粉丝) 中我还没关注的
    other_accounts_fans = all_accounts_followers_uids - followers_uids
    tier_2_other_fans = other_accounts_fans - following_uids

    # 梯队 3：纯粹在外部抓取和裂变的散客用户 (大池 - 所有号粉丝) 中我还没关注的
    mined_users = global_potential_uids - all_accounts_followers_uids
    tier_3_mined_users = mined_users - following_uids

    # 严格按照顺序拼接：自己粉丝优先，其他号粉丝其次，最后才是外部挖掘，然后截取前 100 名
    final_uids_to_follow = (list(tier_1_own_fans) + list(tier_2_other_fans) + list(tier_3_mined_users))[:100]

    logger.info(
        f"📊 [账号:{user_key}] 差集运算分层 | [梯队1]自己待回关:{len(tier_1_own_fans)} | "
        f"[梯队2]其它账号粉丝待关注:{len(tier_2_other_fans)} | [梯队3]挖掘的新用户待关注:{len(tier_3_mined_users)} | 截取后实际准备关注:{len(final_uids_to_follow)}"
    )

    if not final_uids_to_follow:
        logger.info(f"🎉 [账号:{user_key}] 无需关注的新用户，本轮跳过。")
        return

    success_count = 0
    fail_count = 0
    total = len(final_uids_to_follow)
    for index, uid in enumerate(final_uids_to_follow, 1):
        is_success = toggle_binance_follow(uid, "follow", my_cookies, csrf_token)
        if is_success:
            success_count += 1
        else:
            fail_count += 1
        logger.info(f"[账号:{user_key}] [{index}/{total}] 关注 UID:{uid} → {'成功' if is_success else '失败'}")

        if index < total:
            sleep_time = random.uniform(60, 90)
            logger.info(f"⏳ [账号:{user_key}] 休眠 {sleep_time:.2f}s 防风控...")
            time.sleep(sleep_time)

    logger.info(f"🏁 [账号:{user_key}] 完毕 | 成功:{success_count} | 失败:{fail_count}")


def consumer_auto_sync_main(accounts=None):
    """消费者主循环：以 MongoDB 为唯一线索中转站，聚合公共优质池后多账号并行执行关注，周期性清剿。"""
    if accounts is None:
        accounts = ["dahao", "nana"]

    logger.info("========== 🚀 [消费者] 系统启动(多账号并发关注) ==========")
    post_manager = UniversalPostManager(gen_db_object())

    while True:
        try:
            # 1. 从近 30 天库存帖子提取公共互关线索
            shared_post_uids = _get_uids_from_recent_posts(post_manager, days_ago=30, limit=50000)

            # 2. 聚合各账号关注列表作为裂变种子，产出全网高质量目标池
            logger.info("🌱 [消费者] 聚合各账号初始种子...")
            seed_user_names = set()
            all_accounts_followers_uids = set()  # 搜集所有账号的粉丝

            for acc in accounts:
                my_name = get_config(f"{acc}_name")
                if my_name:
                    f_map, follower_map = _get_current_relations(my_name, max_count=10000)
                    seed_user_names.update(f_map.keys())
                    all_accounts_followers_uids.update(follower_map.values())  # 汇总各个账户的粉丝UID

            global_worth_uids = get_worth_following_list(
                initial_user_name_list=list(seed_user_names), target_count=1000
            )

            # 3. 合成公共大池(帖子线索 ∪ 裂变高价值 ∪ 所有账号粉丝)
            global_potential_uids = shared_post_uids.union(set(global_worth_uids)).union(all_accounts_followers_uids)
            logger.info(f"🧩 [消费者] 公共池合成 | 帖子线索:{len(shared_post_uids)} | "
                        f"裂变高价值:{len(global_worth_uids)} | 所有粉丝:{len(all_accounts_followers_uids)} | 合并去重:{len(global_potential_uids)}")

            if not global_potential_uids:
                logger.info("🤷 [消费者] 暂无任何潜在线索，休眠 10 分钟后重试...")
                time.sleep(600)
                continue

            # 4. 多账号并行执行，各自运算差集与休眠
            logger.info(f"🚦 [消费者] 启动多账号并行引擎 | 并发数:{len(accounts)}")
            with ThreadPoolExecutor(max_workers=len(accounts)) as executor:
                # 将 all_accounts_followers_uids 作为参数传递给各个线程
                futures = [
                    executor.submit(_sync_single_account_logic, acc, global_potential_uids, all_accounts_followers_uids)
                    for acc in accounts]
                for f in as_completed(futures):
                    f.result()  # 显式捕获内部异常

            logger.info("♻️ [消费者] 本轮完毕，休眠 1 小时后再次清剿...")
            time.sleep(3600)

        except Exception as e:
            logger.error(f"❌ [消费者] 主循环异常: {e}", exc_info=True)
            time.sleep(60)


# =====================================================================
# [生产者] 按关键词抓帖并 Upsert 入库
# =====================================================================

def producer_fetch_content_main():
    """生产者死循环：以 DB 现存 ID 为增量边界，逐关键词抓取新帖并 Upsert 入 MongoDB，词间硬休眠防 API 断流。"""
    post_manager = UniversalPostManager(gen_db_object())
    logger.info("========== 📡 [生产者] 抓取引擎启动(MongoDB 管道就绪) ==========")

    while True:
        try:
            time.sleep(PRODUCER_SWEEP_INTERVAL)

            # 构建增量记忆库：DB 现存帖子 ID(兼容对象与 dict 两种返回结构)
            existing_posts = post_manager.find_posts_by_source("biance", limit=50000)
            existing_ids = {str(getattr(post, "post_id", post.get("post_id")))
                            for post in existing_posts if getattr(post, "post_id", post.get("post_id"))}
            logger.info(f"🧠 [生产者] 记忆库构建完成 | DB 历史帖子:{len(existing_ids)} 条")

            total_new = 0
            for search_key in SEARCH_KEYWORDS:
                search_data = fetch_binance_feed(keyword=search_key, count=1000, existing_ids=existing_ids)
                fetched = len(search_data) if search_data else 0
                total_new += fetched
                if search_data:
                    post_manager.upsert_posts(search_data)
                logger.info(f"📥 [生产者] 关键词『{search_key}』新帖 {fetched} 条 → 已 Upsert 入库")
                time.sleep(5)  # 词间硬休眠，防触发 API 风控断流

            logger.info(f"✅ [生产者] 本轮全量抓取完成 | 新增总计:{total_new} 条 | "
                        f"休眠 {PRODUCER_SWEEP_INTERVAL}s 后进入下一轮")

        except Exception as e:
            logger.error(f"❌ [生产者] 运行异常: {e}", exc_info=True)
            time.sleep(10)


# =====================================================================
# 统一启动入口(生产者与消费者以守护线程各自挂载)
# =====================================================================
if __name__ == "__main__":
    try:
        logger.info("💥 初始化启动... 生产者与消费者已各自装载")

        t_producer = threading.Thread(target=producer_fetch_content_main, name="ProducerThread", daemon=True)
        t_consumer = threading.Thread(target=consumer_auto_sync_main, kwargs={"accounts": ["dahao", "nana"]},
                                      name="ConsumerThread", daemon=True)
        t_producer.start()
        t_consumer.start()

        t_producer.join()
        t_consumer.join()

    except KeyboardInterrupt:
        logger.info("⚠️ 收到退出信号，系统安全中止。")
    except Exception as e:
        logger.error(f"🚨 系统致命错误: {e}", exc_info=True)