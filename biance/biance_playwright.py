# -*- coding: utf-8 -*-
"""
===============================================================================
[功能摘要]：币安广场全自动评论与交互 RPA 脚本（图片 + 正文 + 超链接一体化注入）。
[输入数据]：
    - post_url    : 目标帖子 URL (str)
    - comment     : 评论正文 (str)
    - image_path  : 图片本地物理路径 (str)
    - url_info_list: 超链接注入清单，Shape: [{"text": str, "url": str}, ...]
[数据流转/交互]：
    1. 凭证挂载：读取本地 User Data 目录恢复浏览器上下文，接管本地 Cookie/CSRF 登录态。
    2. 局部锁定：页面加载后，模拟 PageDown 步进探底，锁定并隔离富文本编辑器 DOM 作用域。
    3. 数据注入：在隔离作用域内依次触发 [图片上传] -> [文本键入] -> [动态菜单唤醒+超链接拼接]。
    4. 结果校验：触发 [发送]，优先挂载网络监听捕获 `pgc/content/add` 接口状态；
                若超时，降级比对发送前后编辑器 DOM 的清空状态以判定成败。
[输出数据]：向终端输出结构化执行日志；最终返回 Tuple: (错误信息|None, 是否成功|bool, 评论ID|None)。
===============================================================================
"""

import os
import re
import shutil
import sys
import time
import traceback

from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeoutError

# ==============================================================================
# 全局配置、正则常量与自定义异常
# ==============================================================================
USER_DATA_DIR = r"W:\temp\biance_nana"
LOGIN_URL = 'https://www.binance.com/zh-CN/login'

# 前端文案多语言/多变体的统一匹配规则，避免选择器中重复书写正则
RE_MORE = re.compile(r"更多|More|Options|Expand", re.IGNORECASE)
RE_ADD_LINK = re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
RE_CONFIRM = re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)
RE_SEND = re.compile(r"回复|发送|Reply|Comment", re.IGNORECASE)
RE_SEND_EXACT = re.compile(r"^回复$|^发送$|^Reply$|^Comment$", re.IGNORECASE)


class PageCrashedException(Exception):
    """页面崩溃/死机（如内存溢出触发的重新加载）"""
    pass


class BusinessErrorException(Exception):
    """发送请求被服务端业务规则拦截"""
    pass


# ==============================================================================
# 底层通用工具
# ==============================================================================

def clean_browser_cache(user_data_dir):
    """瘦身浏览器缓存，仅清理冗余目录、保留核心登录凭证。单项删除失败按原设计静默忽略（尽力而为）。"""
    if not os.path.exists(user_data_dir):
        return

    print(f"\n[Cache/Clean] 启动浏览器数据瘦身 | 目标目录: <{user_data_dir}> | 结果: [执行中]")
    garbage = ["Cache", "Code Cache", "GPUCache", "ShaderCache", "GrShaderCache", "Service Worker", "CacheStorage"]
    deleted = 0

    for base in [user_data_dir, os.path.join(user_data_dir, "Default")]:
        for target in garbage:
            tp = os.path.join(base, target)
            if not os.path.exists(tp):
                continue
            try:
                shutil.rmtree(tp, ignore_errors=True) if os.path.isdir(tp) else os.remove(tp)
                deleted += 1
            except Exception:
                pass  # 缓存清理为尽力而为，单项失败不影响主流程

    print(f"[Cache/Clean] 瘦身完成 | 结果: [清理了 【{deleted}】 个冗余项]\n")


def check_for_crash(page):
    """探测页面是否崩溃：在 500ms 窗口内出现【重新加载】按钮即判定 DOM 渲染崩溃。"""
    try:
        page.get_by_role("button", name="重新加载").first.wait_for(state="visible", timeout=500)
        raise PageCrashedException("页面 DOM 渲染崩溃，检测到【重新加载】按钮")
    except PlaywrightTimeoutError:
        pass  # 未出现崩溃按钮，属正常情况


def human_intervention_pause(error_msg):
    """挂起程序并蜂鸣，等待人工在浏览器中排查后回车续跑。"""
    sys.stdout.write('\a')
    sys.stdout.flush()
    print(f"\n{'=' * 50}")
    print(f"[System/Halt] 🚨 触发人工介入机制 | 失败原因: 【{error_msg}】")
    print(f"[System/Halt] 请在浏览器中排查问题，完成后按 [Enter] 键继续...")
    print(f"{'=' * 50}")
    input()


def _interact_fallback_locators(locators, action="wait", timeout=5000, desc="目标元素"):
    """
    为对抗多变前端结构而设计的核心健壮性机制：
    轮询后备选择器清单，将长阻塞打散为 200ms 时间片，避免单一选择器失效导致整体长时间卡顿。
    action="click" 时命中即点击并返回；否则命中即返回该 locator。全部超时则抛出聚合异常。
    """
    end_time = time.time() + (timeout / 1000.0)
    last_err = None

    while time.time() < end_time:
        for loc in locators:
            try:
                loc.wait_for(state="visible", timeout=200)
                if action == "click":
                    loc.click(timeout=1500)
                return loc
            except Exception as e:
                last_err = e
                continue

    raise Exception(f"在 {timeout}ms 内未能 {action} 【{desc}】 | 底层最后错误: {str(last_err)[:100]}")


def _robust_click(locator):
    """三段降级点击（常规 -> 强制穿透遮挡 -> JS 原生），前两段失败静默降级，末段失败则如实抛出。"""
    try:
        locator.click(timeout=1500)
        return
    except Exception:
        pass
    try:
        locator.click(force=True, timeout=1500)
        return
    except Exception:
        pass
    locator.evaluate("node => node.click()")


def _focus_editor_end(page, editor_node):
    """将光标聚焦到富文本末尾，为后续键入/菜单唤醒做准备。"""
    try:
        editor_node.click(timeout=2000)
    except Exception:
        pass
    page.keyboard.press("End")
    page.wait_for_timeout(120)


def _snapshot_editor(editor):
    """
    安全读取编辑器当前状态快照，供发送前后比对是否清空。
    返回: (文本字符数, 媒体元素数[img/a])；元素不可见或异常时返回 (0, 0)。
    """
    try:
        if not editor.is_visible():
            return 0, 0
        return len(editor.inner_text().strip()), editor.locator("img, a").count()
    except Exception:
        return 0, 0


# ==============================================================================
# 核心业务模块
# ==============================================================================

def login_and_save_session():
    """打开可见浏览器供手动登录，并将会话固化到本地 User Data。"""
    print(f"[Auth/Login] 准备手动登录 | 存储路径: <{USER_DATA_DIR}> | 结果: [启动中]")
    clean_browser_cache(USER_DATA_DIR)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            channel="chrome", user_data_dir=USER_DATA_DIR, headless=False,
            args=['--disable-blink-features=AutomationControlled', '--start-maximized']
        )
        page = context.new_page()
        page.goto(LOGIN_URL)
        input("\n[Auth/Login] 等待操作 | 动作: 【登录成功后，请按 Enter 键关闭并保存】")
        context.close()
        print(f"[Auth/Login] 会话保存完毕 | 状态: [Success]")


def _smart_scroll_to_editor(page, max_scrolls=20):
    """步进式 PageDown 探底，锁定评论区富文本编辑器容器并回滚至可视范围。"""
    print(f"[DOM/Locate] 开始向下探索评论区 | 最大尝试: <{max_scrolls}次> | 结果: [扫描中]")
    editor_container = page.locator("div.feed-post-editor").first

    for i in range(max_scrolls):
        if editor_container.is_visible():
            print(f"[DOM/Locate] 探索成功 | 滚动次数: 【{i}】 | 结果: [已锁定局部作用域]")
            editor_container.scroll_into_view_if_needed()
            return editor_container
        page.keyboard.press("PageDown")
        time.sleep(0.5)

    raise Exception("向下滚动探底失败，未能找到评论输入区，疑似死链或风控滑块拦截。")


def _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx):
    """
    在富文本末尾唤起动态菜单，注入单条超链接并校验上屏。
    数据形貌: link_text/link_url 均为已清洗的非空字符串（link_url 已补全协议头）。
    成功返回 True；任一环节失败则闭窗清理并返回 False —— 按原设计不阻断主流程。
    """
    print(f"[Editor/Link] 注入节点 [{idx + 1}] | 文本: 【{link_text}】 -> URL: <{link_url}> | 结果: [执行中]")
    try:
        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(150)

        # 唤醒"更多"菜单：多级后备选择器抵御图标 DOM 结构变动
        more_cands = [
            editor_container.locator('#post-editor-more-icon').first,
            editor_container.locator("svg").filter(has=page.locator('path[d^="M12 16.5"]')).first,
            editor_container.locator("div.icon-box").filter(has=page.locator('svg')).last,
            editor_container.get_by_role("button", name=RE_MORE).first,
            editor_container.locator('button[aria-label*="更多"], button[aria-label*="More" i]').first,
        ]
        _interact_fallback_locators(more_cands, action="click", timeout=4000, desc="更多按钮")
        page.wait_for_timeout(350)

        # 点击"添加链接"选项
        add_link_cands = [
            page.locator('.menu-item').filter(has_text=RE_ADD_LINK).first,
            page.get_by_role("menuitem", name=RE_ADD_LINK).first,
            page.locator('[role="menuitem"], [class*="menu-item"]').filter(has_text=RE_ADD_LINK).first,
        ]
        _interact_fallback_locators(add_link_cands, action="click", timeout=4000, desc="添加链接选项")

        # 锁定注入弹窗作用域（无弹窗则退化为整页）
        dialog = page
        try:
            dlg = page.get_by_role("dialog").last
            dlg.wait_for(state="visible", timeout=2000)
            dialog = dlg
        except Exception:
            pass

        # 定位正文/地址输入框与确认按钮（data-bn-type 为币安专有属性，优先嗅探）
        name_input = _interact_fallback_locators([
            dialog.locator('input[name="name"][data-bn-type="input"]').first,
            dialog.locator('input[name="name"]').first,
            dialog.get_by_placeholder(re.compile(r"正文|名称|标题|text|name|title", re.IGNORECASE)).first,
        ], action="wait", timeout=6000, desc="链接正文输入框")

        link_input = _interact_fallback_locators([
            dialog.locator('input[name="link"][data-bn-type="input"]').first,
            dialog.locator('input[name="link"]').first,
            dialog.get_by_placeholder(re.compile(r"链接|地址|link|url|address", re.IGNORECASE)).first,
        ], action="wait", timeout=6000, desc="链接地址输入框")

        confirm_btn = _interact_fallback_locators([
            dialog.locator('button[type="submit"][data-bn-type="button"]').filter(has_text=RE_CONFIRM).first,
            dialog.locator('button[type="submit"]').filter(has_text=RE_CONFIRM).first,
            dialog.get_by_role("button", name=RE_CONFIRM).first,
        ], action="wait", timeout=6000, desc="链接确认按钮")

        name_input.fill(link_text)
        page.wait_for_timeout(200)
        link_input.fill(link_url)
        page.wait_for_timeout(200)

        expect(confirm_btn).to_be_enabled(timeout=6000)
        confirm_btn.click(timeout=6000)
        expect(name_input).to_be_hidden(timeout=6000)

        # 校验链接确已上屏
        link_locator = real_editor.locator("a").filter(has_text=re.compile(re.escape(link_text), re.IGNORECASE))
        expect(link_locator.first).to_be_visible(timeout=5000)

        print(f"[Editor/Link] 注入节点 [{idx + 1}] 成功 | 状态: [Success]")
        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(200)
        return True

    except Exception as e:
        # 单条链接失败按原设计跳过：关闭可能残留的弹窗，继续注入下一条
        print(f"[Editor/Link] 注入节点 [{idx + 1}] 失败，执行跳过 | 可能原因: 【菜单未唤醒/弹窗结构变动/校验超时: {e}】 | 结果: [Skipped]")
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
        return False


def _submit_comment(page, editor_container, comment, image_path=None, url_info_list=None):
    """
    在隔离的局部作用域内完成发帖全链路：唤醒编辑器 -> 图片 -> 正文 -> 超链接 -> 发送校验。
    入参 url_info_list 形貌: [{"text": str, "url": str}, ...]。
    返回: 成功时的评论ID(str) 或 None。
    """
    comment = str(comment) if comment else ""
    comment_id = None

    # ---- 步骤 1：唤醒富文本编辑器 ----
    print(f"[Editor/Wakeup] 尝试唤醒富文本框 | 目标: <div.ProseMirror> | 结果: [执行中]")
    editor_container.locator('input[type="text"], input[placeholder]').first.click()
    real_editor = editor_container.locator('div[contenteditable="true"].ProseMirror').first
    expect(real_editor).to_be_editable(timeout=8000)
    print(f"[Editor/Wakeup] 唤醒成功 | 状态: [可编辑]")

    # ---- 步骤 2：注入图片（失败则降级为纯文本，不中断） ----
    if image_path and os.path.exists(image_path):
        print(f"[Editor/Image] 开始上传图片 | 路径: <{image_path}> | 结果: [执行中]")
        try:
            editor_container.locator('input[type="file"]').first.set_input_files(image_path)
            page.wait_for_timeout(3500)
            print(f"[Editor/Image] 图片挂载完毕 | 状态: [Success]")
        except Exception as e:
            print(f"[Editor/Image] 图片上传失败，自动降级为纯文本 | 可能原因: 【文件损坏或上传控件不可用: {e}】 | 结果: [Warning]")

    # ---- 步骤 3：注入正文（含框架静默清空的补录兜底） ----
    if comment.strip():
        print(f"[Editor/Text] 填入正文内容 | 长度: <{len(comment)}> | 结果: [输入中]")
        real_editor.click()
        page.wait_for_timeout(800)
        real_editor.press_sequentially(comment, delay=60)
        page.wait_for_timeout(500)

        if not real_editor.inner_text().strip():  # 防前端框架拦截导致静默清空
            print(f"[Editor/Text] 检测到文本被静默清空，触发重试补录 | 动作: [Retry]")
            real_editor.click()
            real_editor.press_sequentially(comment, delay=60)
        print(f"[Editor/Text] 文本输入完成 | 状态: [Success]")
    else:
        real_editor.click()
        page.wait_for_timeout(500)

    # ---- 步骤 4：注入超链接（逐条注入，单条失败自动跳过） ----
    if isinstance(url_info_list, list) and url_info_list:
        print(f"[Editor/Link] 检测到超链接任务 | 数量: <{len(url_info_list)}> | 结果: [启动注入流]")
        for idx, url_info in enumerate(url_info_list):
            if not isinstance(url_info, dict):
                continue
            link_text = str(url_info.get("text", "")).strip()
            link_url = str(url_info.get("url", "")).strip()
            if not link_text or not link_url:
                continue
            if not re.match(r"^https?://", link_url, re.IGNORECASE):
                link_url = "https://" + link_url
            _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx)

    # ---- 步骤 5：发送 + API 监听校验 ----
    print(f"[Editor/Submit] 定位发送按钮 | 结果: [执行中]")
    send_btn_cands = [
        editor_container.locator("button").filter(has_text=RE_SEND_EXACT).first,
        editor_container.get_by_role("button", name=RE_SEND).first,
    ]
    try:
        send_button = _interact_fallback_locators(send_btn_cands, action="wait", timeout=10000, desc="发送按钮")
        expect(send_button).to_be_enabled(timeout=10000)
    except Exception as e:
        print(f"[Editor/Submit] 发送按钮定位/状态异常，强制回退首选候选 | 可能原因: 【按钮未渲染或处于禁用态: {e}】 | 状态: [Warning]")
        send_button = send_btn_cands[0]

    # 记录发送前编辑器快照，供 DOM 兜底比对
    text_before, media_before = _snapshot_editor(real_editor)
    api_success = False

    print(f"[Editor/Submit] 触发发送并挂载 API 监听器 | 接口特征: <pgc/content/add> | 结果: [执行中]")
    try:
        with page.expect_response(
                lambda r: "pgc/content/add" in r.url and r.request.method == "POST", timeout=10000
        ) as response_info:
            _robust_click(send_button)

        # 健壮解析：兼容 502 等返回 HTML 而非 JSON 的场景，避免解析崩溃
        try:
            raw_json = response_info.value.json()
            json_data = raw_json if isinstance(raw_json, dict) else {"raw": str(raw_json)}
        except Exception:
            json_data = {}

        if str(json_data.get("code", "")) == "000000" or json_data.get("success") is True:
            api_success = True
            data = json_data.get("data")
            comment_id = data.get("id") if isinstance(data, dict) else None
            print(f"[Editor/Verify] 底层接口校验通过 | 响应码: 【000000】 | 评论ID: 【{comment_id}】 | 结果: [Success]")
        elif json_data:
            err_msg = json_data.get("message", "未知业务拦截")
            print(f"[Editor/Verify] 底层接口拒绝请求 | 原因: 【{err_msg}】 | 结果: [Failed]")
            raise BusinessErrorException(f"业务发送被服务器拦截，原因: {err_msg}")

    except PlaywrightTimeoutError:
        print(f"[Editor/Verify] API 监听超时，降级启用 DOM 状态机校验 | 状态: [Warning]")

    # ---- 步骤 6：DOM 兜底校验（编辑器被大幅清空即视为发送成功） ----
    if not api_success:
        time.sleep(3)
        text_after, media_after = _snapshot_editor(real_editor)
        text_cleared = text_before > 0 and text_after < (text_before / 3)
        media_cleared = media_before > 0 and media_after < media_before

        if text_cleared or media_cleared:
            print(f"[Editor/Verify] DOM 兜底比对通过 | 现象: 【输入框已被大幅清空】 | 结果: [Success]")
        else:
            raise Exception("发送已执行但输入框未清空且 API 无成功响应，疑似发送按钮失效或网络堵塞。")

    return comment_id


def comment_on_binance_post(post_url, comment, image_path=None, user_data_dir=USER_DATA_DIR, url_info_list=None):
    """
    主控入口：调度浏览器加载帖子并执行评论全流程。
    返回: Tuple(错误信息(str|None), 是否成功(bool), 评论ID(str|None))。
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    print(f"\n{'=' * 60}")
    print(f"[Main/Task] 启动自动化发帖任务 | 目标URL: <{post_url}> | 结果: [初始化]")
    print(f"{'=' * 60}")

    # sync_playwright 提到最外层，确保底层引擎崩溃与业务异常分层捕获、上下文不被误销毁
    try:
        with sync_playwright() as p:
            context = None
            try:
                context = p.chromium.launch_persistent_context(
                    channel="chrome", user_data_dir=user_data_dir, headless=False,
                    args=["--disable-blink-features=AutomationControlled", "--start-maximized"],
                    ignore_default_args=["--enable-automation"]
                )
                context.set_default_timeout(60000)
                context.set_default_navigation_timeout(60000)
                page = context.pages[0] if context.pages else context.new_page()

                print(f"[Main/Nav] 导航至目标页面 | 动作: [等待 DOM 加载]")
                page.goto(post_url, timeout=60000)
                page.wait_for_load_state("domcontentloaded", timeout=60000)

                # 登录态嗅探：出现 login 链接即判定 Cookie 过期
                try:
                    page.locator("a[href*='login']").first.wait_for(state="visible", timeout=3000)
                    return "页面探测到 Login 按钮，本地 Cookie 可能已过期失效。", False, None
                except PlaywrightTimeoutError:
                    pass

                check_for_crash(page)
                editor_container = _smart_scroll_to_editor(page)
                check_for_crash(page)

                comment_id = _submit_comment(page, editor_container, comment, image_path, url_info_list)
                return None, True, comment_id

            except BusinessErrorException as biz_e:
                error_info = f"[业务拦截] {str(biz_e)}"
                print(f"[Main/Task] 发帖被服务端业务规则阻断 | 原因: 【{error_info}】 | 结果: [Failed]")
                return error_info, False, None

            except PlaywrightTimeoutError as pt_e:
                error_info = f"[元素/网络超时] {str(pt_e)}"
                print(f"[Main/Task] 元素等待或网络请求超时 | 可能原因: 【页面卡顿/选择器失效/网络抖动】 | 详情: 【{error_info[:200]}...】 | 结果: [Failed]")
                human_intervention_pause(error_info)
                return error_info, False, None

            except Exception as e:
                error_info = f"[{type(e).__name__}] {str(e)}\n[Traceback]:\n{traceback.format_exc()}"
                print(f"[Main/Task] 执行过程发生未预期异常 | 摘要: 【{str(e)[:200]}...】 | 结果: [Failed]")

                # 上下文仍在，保留故障现场（截图 + HTML）以便离线复盘
                if context and context.pages:
                    try:
                        ts = int(time.time())
                        context.pages[0].screenshot(path=f"error_screenshot_{ts}.png")
                        with open(f"error_html_{ts}.html", "w", encoding="utf-8") as f:
                            f.write(context.pages[0].content())
                        print(f"[Main/Debug] 故障现场已保留 | 产物时间戳: 【{ts}】 | 结果: [Saved]")
                    except Exception as s_e:
                        print(f"[Main/Debug] 现场保留失败 | 可能原因: 【磁盘不可写或页面已销毁: {s_e}】")

                human_intervention_pause(error_info)
                return error_info, False, None

            finally:
                if context:
                    try:
                        context.close()
                    except Exception:
                        pass

    except Exception as core_e:
        error_info = f"[CoreEngineCrash] Playwright 底层启动/运行发生系统级崩溃:\n{core_e}\n\n[Traceback]:\n{traceback.format_exc()}"
        print(f"[Main/Task] Playwright 核心框架崩溃，无法启动浏览器引擎 | 结果: [Failed]")
        return error_info, False, None


def get_auth_tokens_robust(user_data_dir):
    """
    脱机运行前提取登录凭证（无头拦截接口请求头）。
    返回: Tuple(Cookie串(str|None), CSRF Token(str|None))。
    """
    if not os.path.exists(user_data_dir):
        print(f"[Auth/Extract] 环境不存在，终止提取 | 目录: <{user_data_dir}> | 结果: [Failed]")
        return None, None

    visit_url = "https://www.binance.com/zh-CN/square/profile/insights_anchor"
    target_api_keyword = "pgc/user/client"
    print(f"[Auth/Extract] 启动无头浏览器提取凭证 | 拦截目标: <{target_api_keyword}> | 结果: [执行中]")

    with sync_playwright() as p:
        context = None
        try:
            context = p.chromium.launch_persistent_context(
                channel="chrome", user_data_dir=user_data_dir, headless=True,
                args=['--disable-blink-features=AutomationControlled', '--headless=new']
            )
            page = context.pages[0] if context.pages else context.new_page()

            with page.expect_request(lambda req: target_api_keyword in req.url, timeout=15000) as first_req:
                page.goto(visit_url)

            extracted_csrf = first_req.value.headers.get("csrftoken")
            raw_cookies = context.cookies(urls=["https://www.binance.com", visit_url])
            extracted_cookie = "; ".join(f"{c['name']}={c['value']}" for c in raw_cookies)

            if extracted_cookie and extracted_csrf:
                print(f"[Auth/Extract] 提取成功 | CSRF: 【{extracted_csrf[:8]}...】 | Cookie长度: 【{len(extracted_cookie)}】 | 结果: [Success]")
                return extracted_cookie, extracted_csrf

            print(f"[Auth/Extract] 提取失败 | 原因: 【捕获到的凭据为空，可能未登录】 | 结果: [Failed]")
            return None, None

        except PlaywrightTimeoutError:
            print(f"[Auth/Extract] 提取失败 | 原因: 【15 秒内未捕获到目标接口请求，疑似登录态失效】 | 结果: [Timeout]")
            return None, None
        except Exception as e:
            print(f"[Auth/Extract] 提取失败 | 原因: 【未知异常: {e}】 | 结果: [Error]")
            return None, None
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass


def open_browser_for_manual_use(user_data_dir):
    """启动可见浏览器交由人工自由操作，关闭窗口后自动收回控制权并释放资源。"""
    print(f"\n{'=' * 50}\n[System/Manual] 启动本地浏览器进行人工接管 | 目录: <{user_data_dir}>\n{'=' * 50}")
    with sync_playwright() as p:
        context = None
        try:
            context = p.chromium.launch_persistent_context(
                channel="chrome", user_data_dir=user_data_dir, headless=False,
                args=['--disable-blink-features=AutomationControlled', '--start-maximized'],
                ignore_default_args=["--enable-automation"]
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto('https://www.binance.com/zh-CN')

            print("\n[System/Manual] ✅ 浏览器已就绪，控制权已交接。")
            print("[System/Manual] 🛑 退出方式: 直接关闭浏览器窗口，程序将自动结束。")
            page.wait_for_event("close", timeout=0)
        except Exception as e:
            print(f"\n[System/Manual] 浏览器运行异常 | 可能原因: 【环境损坏或被手动强制杀死: {e}】")
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass
            print("[System/Manual] 👋 窗口已关闭，控制权收回，系统资源已释放。\n")


# ==============================================================================
# 启动入口
# ==============================================================================
if __name__ == '__main__':
    # # 其他可选入口（按需取消注释）:
    # get_auth_tokens_robust(USER_DATA_DIR)   # 提取脱机 API 凭证
    # login_and_save_session()                # 初次手动登录并固化 Session
    # open_browser_for_manual_use(USER_DATA_DIR)  # 人工接管调试

    test_url = "https://www.binance.com/zh-CN/square/post/309692475255842"
    test_msg = "少即是多，慢即是快。同频共振！🚀"
    test_img = r"C:\Users\zxh\Desktop\temp\a6c98436-42f9-4aa9-bab8-.png"
    my_urls = [
        {"text": "带单", "url": "https://www.binance.com/zh-CN/square/post/309692475255842"},
        {"text": "带单高手", "url": "https://www.binance.com/zh-CN/square/post/309692475255842"},
    ]

    err, success, c_id = comment_on_binance_post(
        post_url=test_url, comment=test_msg, image_path=test_img, url_info_list=my_urls
    )

    if success:
        print(f"\n[Final/Result] 🎉 ======== 自动评论任务圆满成功 ======== | 评论ID: 【{c_id}】")
    else:
        print(f"\n[Final/Result] ❌ ======== 任务失败 ======== | 最终追溯:\n{err}")