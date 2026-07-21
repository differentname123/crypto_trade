# -*- coding: utf-8 -*-
"""
===============================================================================
[功能摘要]：币安广场全自动评论与交互 RPA 脚本。
[输入数据]：目标帖子的 URL、评论正文文本、图片本地物理路径、结构化的超链接注入列表。
[数据流转/交互]：
    1. 凭证挂载：读取本地 USER_DATA_DIR 恢复浏览器上下文，接管用户登录态。
    2. 局部锁定：页面加载后，通过模拟按键向下探索，锁定并隔离富文本编辑器的 DOM 作用域。
    3. 数据注入：在隔离作用域内，依次触发[图片上传] -> [文本键入] -> [动态菜单唤醒与超链接拼接]。
    4. 结果校验：触发发送动作后，优先挂载网络监听器捕获 `pgc/content/add` 接口状态；若超时，则降级比对 DOM 渲染前后的元素清空状态。
[输出数据]：向终端输出标准化的执行进度日志，并最终返回业务执行的布尔结果与具体错误追溯信息。
===============================================================================
"""

import json
import os
import re
import shutil
import time
import sys
import traceback
from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeoutError
from typing import Tuple, Optional
from playwright.sync_api import sync_playwright, Page, expect, TimeoutError as PlaywrightTimeoutError

# ==============================================================================
# 全局配置
# ==============================================================================
USER_DATA_DIR = r"W:\temp\biance_dahao"
LOGIN_URL = 'https://www.binance.com/zh-CN/login'


# ==============================================================================
# 底层工具模块
# ==============================================================================
def _get_dir_size(start_path='.'):
    """递归计算目录物理大小"""
    total_size = 0
    try:
        for dirpath, _, filenames in os.walk(start_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    try:
                        total_size += os.path.getsize(fp)
                    except Exception:
                        pass
    except Exception:
        pass
    return total_size


def _format_size(size):
    """格式化字节大小为人类可读格式"""
    power, n = 1024, size
    power_labels = {0: '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    count = 0
    while n > power:
        n /= power
        count += 1
    return f"{n:.2f} {power_labels.get(count, 'B')}"


def clean_browser_cache(user_data_dir):
    """清理无用缓存，维持核心登录凭证"""
    if not os.path.exists(user_data_dir):
        return

    print(f"\n[Cache/Clean] 启动浏览器数据瘦身 | 目标目录: <{user_data_dir}> | 结果: [执行中]")
    size_before = _get_dir_size(user_data_dir)
    garbage_targets = ["Cache", "Code Cache", "GPUCache", "ShaderCache", "GrShaderCache", "Service Worker",
                       "CacheStorage"]
    deleted_count = 0

    for base_path in [user_data_dir, os.path.join(user_data_dir, "Default")]:
        if not os.path.exists(base_path):
            continue
        for target in garbage_targets:
            tp = os.path.join(base_path, target)
            if os.path.exists(tp):
                try:
                    shutil.rmtree(tp, ignore_errors=True) if os.path.isdir(tp) else os.remove(tp)
                    deleted_count += 1
                except Exception:
                    pass

    freed_size = size_before - _get_dir_size(user_data_dir)
    print(
        f"[Cache/Clean] 瘦身完成 | 释放空间: 【{_format_size(freed_size)}】 | 结果: [清理了 {deleted_count} 个冗余项]\n")


# 自定义业务异常类
class PageCrashedException(Exception): pass


class BusinessErrorException(Exception): pass


def check_for_crash(page):
    """探测页面是否崩溃或死机"""
    try:
        if page.get_by_role("button", name="重新加载").is_visible(timeout=500):
            raise PageCrashedException("检测到 '重新加载' 按钮，页面已崩溃")
    except PlaywrightTimeoutError:
        pass


def human_intervention_pause(error_msg):
    """挂起程序，蜂鸣警报，等待人工排查"""
    sys.stdout.write('\a')
    sys.stdout.flush()
    print(f"\n{'=' * 50}")
    print(f"[System/Halt] 🚨 触发人工介入机制 | 失败原因: 【{error_msg}】")
    print(f"[System/Halt] 请在弹出的浏览器中排查问题。排查完毕后，按 [Enter] 键继续流转...")
    print(f"{'=' * 50}")
    input()


# ==============================================================================
# 核心业务模块
# ==============================================================================

def login_and_save_session():
    """打开浏览器供人工登录并固化会话"""
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
    """步进式向下滚动，扫描并锁定评论区DOM"""
    print(f"[DOM/Locate] 开始向下探索评论区 | 最大尝试次数: <{max_scrolls}> | 结果: [扫描中]")
    editor_container = page.locator("div.feed-post-editor").first

    for i in range(max_scrolls):
        if editor_container.is_visible():
            print(f"[DOM/Locate] 探索成功 | 滚动次数: 【{i}】 | 结果: [已锁定局部作用域]")
            editor_container.scroll_into_view_if_needed()
            return editor_container

        page.keyboard.press("PageDown")
        time.sleep(0.5)

    raise Exception("已达到最大滚动次数，未能找到评论输入区，疑似死链或风控滑块拦截")


def _submit_comment(
        page: Page,
        editor_container,
        comment: str,
        image_path: Optional[str] = None,
        url_info_list: Optional[list] = None
):
    """
    在已锁定的局部作用域内，执行发帖的全链路操作。
    包含：唤醒编辑器、传图、传文本、插链接、点击发送、API/DOM双重校验。
    """
    comment = "" if comment is None else str(comment)

    # --- 内部工具闭包，隔离琐碎的DOM操作逻辑 ---
    def _wait_first(locators, timeout=5000, desc="元素"):
        start_time = time.time()
        end_time = start_time + (timeout / 1000.0)
        last_err = None

        # 将长阻塞打散为时间片轮询，避免单选择器失效导致的长时间卡顿
        while time.time() < end_time:
            for loc in locators:
                try:
                    # 使用极短超时探查，如果在 200ms 内出现则命中，否则立即进入下一个候选者
                    loc.wait_for(state="visible", timeout=200)
                    return loc
                except Exception as e:
                    last_err = e
                    continue
        raise Exception(f"无法找到可见的 {desc}，总耗时超限 ({timeout}ms) | 底层原因: {last_err}")

    def _click_first(locators, timeout=5000, desc="元素"):
        start_time = time.time()
        end_time = start_time + (timeout / 1000.0)
        last_err = None

        while time.time() < end_time:
            for loc in locators:
                try:
                    loc.wait_for(state="visible", timeout=200)
                    loc.click(timeout=1500)
                    return loc
                except Exception as e:
                    last_err = e
                    continue
        raise Exception(f"无法点击 {desc}，总耗时超限 ({timeout}ms) | 底层原因: {last_err}")

    def _focus_end(editor_node):
        try:
            editor_node.click(timeout=2000)
        except Exception:
            pass
        page.keyboard.press("End")
        page.wait_for_timeout(120)

    def _close_overlay():
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass

    # =======================
    # 步骤 1：唤醒编辑器
    # =======================
    print(f"[Editor/Wakeup] 尝试唤醒富文本框 | 目标: <div.ProseMirror> | 结果: [执行中]")
    fake_input = editor_container.locator('input[type="text"], input[placeholder]').first
    fake_input.click()

    real_editor = editor_container.locator('div[contenteditable="true"].ProseMirror').first
    expect(real_editor).to_be_editable(timeout=8000)
    print(f"[Editor/Wakeup] 唤醒成功 | 状态: [可编辑]")

    # =======================
    # 步骤 2：注入图片
    # =======================
    if image_path and os.path.exists(image_path):
        print(f"[Editor/Image] 开始上传图片 | 路径: <{image_path}> | 结果: [上传中]")
        try:
            file_input = editor_container.locator('input[type="file"]').first
            file_input.set_input_files(image_path)
            page.wait_for_timeout(3500)
            print(f"[Editor/Image] 图片挂载完毕 | 状态: [Success]")
        except Exception as e:
            print(f"[Editor/Image] 图片上传异常，降级为纯文本 | 原因: 【{e}】 | 结果: [Warning]")

    # =======================
    # 步骤 3：注入文本
    # =======================
    if comment.strip():
        print(f"[Editor/Text] 填入正文内容 | 长度: <{len(comment)}> | 结果: [输入中]")
        real_editor.click()
        page.wait_for_timeout(800)
        real_editor.press_sequentially(comment, delay=60)
        page.wait_for_timeout(500)

        # 兜底：防止前端框架静默清空
        if not real_editor.inner_text().strip():
            print(f"[Editor/Text] 检测到文本被静默清空，触发重试补录 | 动作: [Retry]")
            real_editor.click()
            real_editor.press_sequentially(comment, delay=60)

        print(f"[Editor/Text] 文本输入完成 | 状态: [Success]")
    else:
        real_editor.click()
        page.wait_for_timeout(500)

    # =======================
    # 步骤 4：注入超链接 (复杂DOM操作) - 极速无卡顿版本
    # =======================
    if url_info_list and isinstance(url_info_list, (list, tuple)):
        print(f"[Editor/Link] 检测到超链接任务 | 数量: <{len(url_info_list)}> | 结果: [启动注入流]")

        for idx, url_info in enumerate(url_info_list):
            if not isinstance(url_info, dict):
                continue

            link_text = str(url_info.get("text", "")).strip()
            link_url = str(url_info.get("url", "")).strip()
            if not link_text or not link_url:
                continue

            # 协议补全，防止前端校验按钮置灰
            if not re.match(r"^https?://", link_url, re.IGNORECASE):
                link_url = "https://" + link_url

            print(f"[Editor/Link] 注入节点 [{idx + 1}] | 文本: 【{link_text}】 -> URL: <{link_url}> | 结果: [执行中]")
            try:
                _focus_end(real_editor)
                page.keyboard.press("Space")
                page.wait_for_timeout(150)

                # 点开“更多” - 根据现有HTML结构加入精准首发，配合轮询降维打击卡顿
                more_cands = [
                    editor_container.locator('#post-editor-more-icon').first,
                    editor_container.locator("svg").filter(has=page.locator('path[d^="M12 16.5"]')).first,
                    editor_container.locator("div.icon-box").filter(has=page.locator('svg')).last,
                    editor_container.get_by_role("button",
                                                 name=re.compile(r"更多|More|Options|Expand", re.IGNORECASE)).first,
                    editor_container.locator('button[aria-label*="更多"], button[aria-label*="More" i]').first,
                ]
                _click_first(more_cands, timeout=4000, desc="更多按钮")
                page.wait_for_timeout(350)

                # 点击“添加链接”
                add_link_cands = [
                    page.locator("div.menu-item").filter(
                        has_text=re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)).first,
                    page.locator('.menu-item').filter(
                        has_text=re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)).first,
                    page.get_by_role("menuitem",
                                     name=re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)).first,
                    page.locator('[role="menuitem"], .menu-item, [class*="menu-item"]').filter(
                        has_text=re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)).first,
                ]
                _click_first(add_link_cands, timeout=4000, desc="添加链接选项")

                # 锁定弹窗
                dialog = page
                try:
                    dlg = page.get_by_role("dialog").last
                    dlg.wait_for(state="visible", timeout=2000)
                    dialog = dlg
                except Exception:
                    pass

                # 填写正文与地址
                name_input = _wait_first([
                    dialog.locator('input[name="name"][data-bn-type="input"]').first,
                    dialog.locator('input[name="name"]').first,
                    dialog.get_by_placeholder(re.compile(r"正文|名称|标题|text|name|title", re.IGNORECASE)).first
                ], timeout=6000, desc="链接正文输入框")

                link_input = _wait_first([
                    dialog.locator('input[name="link"][data-bn-type="input"]').first,
                    dialog.locator('input[name="link"]').first,
                    dialog.get_by_placeholder(re.compile(r"链接|地址|link|url|address", re.IGNORECASE)).first
                ], timeout=6000, desc="链接地址输入框")

                # 确认按钮
                confirm_btn = _wait_first([
                    dialog.locator('button[type="submit"][data-bn-type="button"]').filter(
                        has_text=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)).first,
                    dialog.locator('button[type="submit"]').filter(
                        has_text=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)).first,
                    dialog.get_by_role("button", name=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)).first,
                    dialog.locator('button[data-bn-type="button"]').filter(
                        has_text=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)).first,
                ], timeout=6000, desc="链接确认按钮")

                name_input.fill(link_text)
                page.wait_for_timeout(200)
                link_input.fill(link_url)
                page.wait_for_timeout(200)

                # 确认并验证插入结果
                expect(confirm_btn).to_be_enabled(timeout=6000)
                confirm_btn.click(timeout=6000)
                expect(name_input).to_be_hidden(timeout=6000)

                link_locator = real_editor.locator("a").filter(has_text=re.compile(re.escape(link_text), re.IGNORECASE))
                expect(link_locator.first).to_be_visible(timeout=5000)

                print(f"[Editor/Link] 注入节点 [{idx + 1}] 成功 | 状态: [Success]")

                _focus_end(real_editor)
                page.keyboard.press("Space")
                page.wait_for_timeout(200)

            except Exception as e:
                print(f"[Editor/Link] 注入节点 [{idx + 1}] 失败，执行跳过 | 原因: 【{e}】 | 结果: [Skipped]")
                _close_overlay()

    # =======================
    # 步骤 5：发送并监听结果
    # =======================
    print(f"[Editor/Submit] 获取发送按钮 | 结果: [定位中]")
    send_btn_cands = [
        editor_container.locator("button").filter(
            has_text=re.compile(r"^回复$|^发送$|^Reply$|^Comment$", re.IGNORECASE)).first,
        editor_container.get_by_role("button", name=re.compile(r"回复|发送|Reply|Comment", re.IGNORECASE)).first,
    ]

    try:
        send_button = _wait_first(send_btn_cands, timeout=10000, desc="发送按钮")
    except Exception:
        print(f"[Editor/Submit] 未能稳定定位发送按钮，将使用默认第一个候选按钮继续 | 状态: [Warning]")
        send_button = send_btn_cands[0]

    try:
        expect(send_button).to_be_enabled(timeout=10000)
    except Exception:
        print(f"[Editor/Submit] 发送按钮预期状态未达标，但将尝试强制执行流... | 状态: [Warning]")

    try:
        text_before = real_editor.inner_text().strip()
    except Exception:
        text_before = ""

    try:
        media_before = real_editor.locator("img, a").count()
    except Exception:
        media_before = 0

    api_success = False
    print(f"[Editor/Submit] 触发发送并挂载 API 监听器 | 接口特征: <pgc/content/add> | 结果: [执行中]")

    try:
        with page.expect_response(
                lambda response: "pgc/content/add" in response.url and response.request.method == "POST",
                timeout=10000
        ) as response_info:

            # 三级降级点击策略
            try:
                send_button.click(timeout=1500)
            except Exception:
                try:
                    send_button.click(force=True, timeout=1500)
                except Exception:
                    send_button.evaluate("node => node.click()")

        resp = response_info.value
        try:
            raw_json = resp.json()
            json_data = raw_json if isinstance(raw_json, dict) else {"raw": str(raw_json)}
        except Exception:
            json_data = {}

        if str(json_data.get("code", "")) == "000000" or json_data.get("success") is True:
            api_success = True
            print(f"[Editor/Verify] 底层接口校验通过 | 响应码: 【000000】 | 结果: [Success]")
        elif json_data:
            err_msg = json_data.get("message") or str(json_data)
            print(f"[Editor/Verify] 底层接口拒绝请求 | 原因: 【{err_msg}】 | 结果: [Failed]")
            raise BusinessErrorException(f"业务发送被服务器拦截，原因: {err_msg}")

    except PlaywrightTimeoutError:
        print(f"[Editor/Verify] API 监听超时，降级启用 DOM 状态机校验 | 状态: [Warning]")

    # =======================
    # 步骤 6：DOM 兜底校验
    # =======================
    if not api_success:
        time.sleep(3)

        try:
            editor_visible = real_editor.is_visible()
        except Exception:
            editor_visible = False

        if editor_visible:
            try:
                text_after = real_editor.inner_text().strip()
            except Exception:
                text_after = ""
            try:
                media_after = real_editor.locator("img, a").count()
            except Exception:
                media_after = 0
        else:
            text_after = ""
            media_after = 0

        # 判断文本或媒体是否被大幅度清空
        text_cleared = (len(text_before) > 0 and len(text_after) < (len(text_before) / 3))
        media_cleared = (media_before > 0 and media_after < media_before)

        if text_cleared or media_cleared:
            print(f"[Editor/Verify] DOM 兜底比对通过 | 现象: 【输入框已清空】 | 结果: [Success]")
        else:
            raise Exception("发送操作已执行，但输入框未清空且 API 无明确成功响应，疑似发送按钮未激活或网络堵塞。")


def comment_on_binance_post(post_url, comment, image_path=None, user_data_dir=USER_DATA_DIR, url_info_list=None):
    """
    主控入口：调度浏览器加载帖子并执行评论。
    返回: (错误信息字符串或 None, 是否成功的布尔值)
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False

    error_info = None
    is_success = False
    context = None

    print(f"\n{'=' * 60}")
    print(f"[Main/Task] 启动自动化发帖任务 | 目标URL: <{post_url}> | 结果: [初始化]")
    print(f"{'=' * 60}")

    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                channel="chrome",
                user_data_dir=user_data_dir,
                headless=False,
                args=["--disable-blink-features=AutomationControlled", "--start-maximized"],
                ignore_default_args=["--enable-automation"]
            )
            page = context.pages[0] if context.pages else context.new_page()

            print(f"[Main/Nav] 导航至目标页面 | 动作: [等待 DOM 加载]")
            page.goto(post_url)
            page.wait_for_load_state("domcontentloaded")

            # 登录状态探活
            try:
                page.locator("a[href*='login']").first.wait_for(state="visible", timeout=3000)
                raise Exception("页面探测到 Login 按钮，本地 Cookie 可能已过期失效。")
            except PlaywrightTimeoutError:
                pass

            check_for_crash(page)

            # 寻找编辑器
            editor_container = _smart_scroll_to_editor(page)
            check_for_crash(page)

            # 执行评论全流
            _submit_comment(
                page=page,
                editor_container=editor_container,
                comment=comment,
                image_path=image_path,
                url_info_list=url_info_list
            )
            is_success = True

        except BusinessErrorException as biz_e:
            error_info = str(biz_e)
            print(f"[Main/Task] 业务阻断异常 | 原因: 【{error_info}】 | 结果: [Failed]")

        except Exception as e:
            error_info = str(e)
            print(f"[Main/Task] 系统执行异常 | 原因: 【{error_info[:200]}...】 | 结果: [Failed]")

            if context and context.pages:
                try:
                    ts = int(time.time())
                    context.pages[0].screenshot(path=f"error_screenshot_{ts}.png")
                    with open(f"error_html_{ts}.html", "w", encoding="utf-8") as f:
                        f.write(context.pages[0].content())
                    print(f"[Main/Debug] 现场保留完毕 | 产物: 【截图与HTML源码时间戳: {ts}】 | 结果: [Saved]")
                except Exception:
                    pass

            human_intervention_pause(error_info)

        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass

    return error_info, is_success


def get_auth_tokens_robust(user_data_dir):
    """提取核心凭证 (CSRF & Cookies) 供脱机 API 调用"""
    if not os.path.exists(user_data_dir):
        print(f"[Auth/Extract] 环境不存在，终止提取 | 目录: <{user_data_dir}> | 结果: [Failed]")
        return None, None

    visit_url = "https://www.binance.com/zh-CN/square/profile/insights_anchor"
    target_api_keyword = "pgc/user/client"

    print(f"[Auth/Extract] 启动无头浏览器提取凭证 | 拦截目标: <{target_api_keyword}> | 结果: [执行中]")

    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                channel="chrome",
                user_data_dir=user_data_dir,
                headless=True,
                args=['--disable-blink-features=AutomationControlled', '--headless=new']
            )
            page = context.pages[0] if context.pages else context.new_page()

            extracted_cookie = None
            extracted_csrf = None

            with page.expect_request(lambda request: target_api_keyword in request.url, timeout=15000) as first_req:
                page.goto(visit_url)

            # 获取 CSRF
            req = first_req.value
            extracted_csrf = req.headers.get("csrftoken")

            # 强行抽离 Cookie
            raw_cookies = context.cookies(urls=["https://www.binance.com"])
            extracted_cookie = "; ".join([f"{c['name']}={c['value']}" for c in raw_cookies])

            context.close()

            if extracted_cookie and extracted_csrf:
                print(
                    f"[Auth/Extract] 提取成功 | CSRF: 【{extracted_csrf[:8]}...】 | Cookie长度: 【{len(extracted_cookie)}】 | 结果: [Success]")
                return extracted_cookie, extracted_csrf
            else:
                print(f"[Auth/Extract] 提取异常 | 数据为空 | 结果: [Failed]")
                return None, None

        except PlaywrightTimeoutError:
            print(f"[Auth/Extract] 拦截超时 | 在 15 秒内未捕获到目标请求 | 结果: [Timeout]")
            return None, None
        except Exception as e:
            print(f"[Auth/Extract] 未知异常 | 原因: 【{e}】 | 结果: [Error]")
            return None, None


def open_browser_for_manual_use(user_data_dir):
    """启动浏览器交接给人类操作，直至关闭"""
    print(f"\n{'=' * 50}")
    print(f"[System/Manual] 启动本地浏览器进行人工接管 | 目录: <{user_data_dir}>")
    print(f"{'=' * 50}")

    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                channel="chrome",
                user_data_dir=user_data_dir,
                headless=False,
                args=['--disable-blink-features=AutomationControlled', '--start-maximized'],
                ignore_default_args=["--enable-automation"]
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.goto('https://www.binance.com/zh-CN')

            print("\n[System/Manual] ✅ 浏览器启动完毕，控制权已交接。")
            print("[System/Manual] 🛑 退出方式: 直接点击右上角关闭浏览器窗口，程序将自动结束。")

            page.wait_for_event("close", timeout=0)
        except Exception as e:
            print(f"\n[System/Manual] 浏览器运行异常 | 原因: 【{e}】")
        finally:
            print("[System/Manual] 👋 窗口已关闭，控制权收回，系统资源已释放。\n")


# ==============================================================================
# 启动入口
# ==============================================================================
if __name__ == '__main__':

    # # 1. 获取凭证 (演示)
    # cookie_str, csrf_token = get_auth_tokens_robust(USER_DATA_DIR)

    # 2. 人工登录 (演示)
    # login_and_save_session()

    # 3. 自由接管 (演示)
    # open_browser_for_manual_use(USER_DATA_DIR)

    # 4. 执行发帖主流程
    test_url = "https://www.binance.com/zh-CN/square/post/309692475255842"
    test_msg = "少即是多，慢即是快。同频共振！🚀"
    test_img = r"C:\Users\zxh\Desktop\temp\a6c98436-42f9-4aa9-bab8-.png"

    my_urls = [
        {
            "text": "带单",
            "url": "https://www.binance.com/zh-CN/square/post/309692475255842"
        }
    ]

    err, success = comment_on_binance_post(
        post_url=test_url,
        comment=test_msg,
        image_path=test_img,
        url_info_list=my_urls
    )

    if success:
        print("\n[Final/Result] 🎉 ======== 自动评论任务圆满成功 ========")
    else:
        print(f"\n[Final/Result] ❌ ======== 失败记录 ======== | 最终追溯: 【{err}】")