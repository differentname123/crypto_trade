# -*- coding: utf-8 -*-
""":authors:
    zhuxiaohu (Refactored to Enterprise RPA Level)
:description:
    币安广场自动化评论脚本。
    核心机制：局部作用域链式定位、API与DOM双重成功校验、无缝降级兜底点击、动态智能等待。
"""
import json
import os
import re
import shutil
import time
import sys
from typing import Tuple, Optional
from playwright.sync_api import sync_playwright, Page, expect, TimeoutError as PlaywrightTimeoutError
import traceback

# ==============================================================================
# 配置区域
# ==============================================================================
USER_DATA_DIR = r"W:\temp\biance_dahao"
LOGIN_URL = 'https://www.binance.com/zh-CN/login'


# ==============================================================================
# 核心功能函数 (底层逻辑保留)
# ==============================================================================
def _get_dir_size(start_path='.'):
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(start_path):
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
    power, n = 1024, size
    power_labels = {0: '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    count = 0
    while n > power:
        n /= power
        count += 1
    return f"{n:.2f} {power_labels.get(count, 'B')}"


def clean_browser_cache(user_data_dir: str):
    """清理浏览器缓存，保留 Cookies 以维持登录"""
    if not os.path.exists(user_data_dir):
        return
    print("=" * 40 + "\n🚀 正在执行浏览器数据瘦身...")
    size_before = _get_dir_size(user_data_dir)
    garbage_targets = ["Cache", "Code Cache", "GPUCache", "ShaderCache", "GrShaderCache", "Service Worker",
                       "CacheStorage"]
    deleted_count = 0
    for base_path in [user_data_dir, os.path.join(user_data_dir, "Default")]:
        if not os.path.exists(base_path): continue
        for target in garbage_targets:
            tp = os.path.join(base_path, target)
            if os.path.exists(tp):
                try:
                    shutil.rmtree(tp, ignore_errors=True) if os.path.isdir(tp) else os.remove(tp)
                    deleted_count += 1
                except:
                    pass
    freed_size = size_before - _get_dir_size(user_data_dir)
    print(f"[+] 成功释放空间: {_format_size(freed_size)} (清理 {deleted_count} 项)\n" + "=" * 40)


class PageCrashedException(Exception): pass


class BusinessErrorException(Exception): pass


def check_for_crash(page: Page):
    """快速检查页面是否崩溃"""
    try:
        if page.get_by_role("button", name="重新加载").is_visible(timeout=500):
            raise PageCrashedException("页面已崩溃 (检测到 '重新加载' 按钮)")
    except PlaywrightTimeoutError:
        pass


def human_intervention_pause(msg: str):
    """触发警报并挂起程序，等待人工介入"""
    sys.stdout.write('\a')  # 触发系统蜂鸣声报警
    sys.stdout.flush()
    print(
        f"\n{'=' * 50}\n🚨 【需要人工介入】\n{msg}\n请在弹出的浏览器中排查问题。排查完毕后，在终端按 [Enter] 键继续执行...\n{'=' * 50}")
    input()


def login_and_save_session():
    print(f"--- 启动浏览器进行手动登录 ---\n保存在: {USER_DATA_DIR}")
    clean_browser_cache(USER_DATA_DIR)
    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            channel="chrome", user_data_dir=USER_DATA_DIR, headless=False,
            args=['--disable-blink-features=AutomationControlled', '--start-maximized']
        )
        page = context.new_page()
        page.goto(LOGIN_URL)
        input("\n登录成功后，请按 Enter 键继续...")
        context.close()


# ==============================================================================
# 币安业务相关函数 (架构级重构版本)
# ==============================================================================

def _smart_scroll_to_editor(page: Page, max_scrolls: int = 20):
    """
    智能探索滚动：边向下滚边寻找评论大容器，找到即停。
    """
    print("[*] 正在向下滚动探索评论区...")

    # [特征锚点 1 - 容器大框]: 如果找不到，去F12找包裹整个输入框的 div 类名
    editor_container = page.locator("div.feed-post-editor").first

    for i in range(max_scrolls):
        if editor_container.is_visible():
            print(f"[+] 第 {i} 次滚动找到评论区，锁定局部作用域。")
            editor_container.scroll_into_view_if_needed()
            return editor_container

        # 模拟按 PageDown 键，比鼠标滚轮更稳定真实
        page.keyboard.press("PageDown")
        time.sleep(0.5)

    raise Exception("向下滚动了最大次数，依然没有找到评论输入区，疑似死链或被风控滑块拦截。")


def _submit_comment(
    page: Page,
    editor_container,
    comment: str,
    image_path: Optional[str] = None,
    url_info_list: Optional[list] = None
):
    """
    在锁定的局部作用域内执行：
    唤醒编辑器 -> 上传图片 -> 输入正文 -> 插入超链接 -> 发送 -> API/DOM 校验
    """

    comment = "" if comment is None else str(comment)

    # =======================
    # 内部小工具
    # =======================

    def focus_editor_end():
        """把光标切回编辑器末尾"""
        try:
            real_editor.click(timeout=2000)
        except Exception:
            pass
        page.keyboard.press("End")
        page.wait_for_timeout(120)

    def close_overlay():
        """尝试关闭弹窗 / 下拉菜单，避免失败后卡住后续流程"""
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass

    def wait_first_visible(locators, timeout=5000, desc="元素"):
        """等待第一个可见元素，并返回它"""
        last_err = None
        for loc in locators:
            try:
                loc.wait_for(state="visible", timeout=timeout)
                return loc
            except Exception as e:
                last_err = e
                continue
        raise Exception(f"无法找到可见的{desc}: {last_err}")

    def click_first_visible(locators, timeout=5000, desc="元素"):
        """点击第一个可见元素"""
        last_err = None
        for loc in locators:
            try:
                loc.wait_for(state="visible", timeout=timeout)
                loc.click(timeout=timeout)
                return loc
            except Exception as e:
                last_err = e
                continue
        raise Exception(f"无法点击{desc}: {last_err}")

    # =======================
    # 第 1 步：唤醒真实编辑器
    # =======================
    print("[*] 正在唤醒编辑器...")
    fake_input = editor_container.locator('input[type="text"], input[placeholder]').first
    fake_input.click()

    real_editor = editor_container.locator('div[contenteditable="true"].ProseMirror').first
    expect(real_editor).to_be_editable(timeout=8000)

    # =======================
    # 第 2 步：先上传图片
    # =======================
    if image_path and os.path.exists(image_path):
        print(f"[*] 准备上传图片: {image_path}")
        try:
            file_input = editor_container.locator('input[type="file"]').first
            file_input.set_input_files(image_path)
            print("[*] 图片已注入，等待前端渲染机制稳定...")
            page.wait_for_timeout(3500)
            print("[+] 图片加载及页面状态机刷新完成。")
        except Exception as e:
            print(f"[!] 警告: 图片上传环节发生异常，将尝试降级发送纯文本。原因: {e}")

    # =======================
    # 第 3 步：输入正文
    # =======================
    print("[*] 编辑器就绪，开始填入文字...")

    if comment.strip():
        real_editor.click()
        page.wait_for_timeout(800)

        real_editor.press_sequentially(comment, delay=60)

        page.wait_for_timeout(500)
        if not real_editor.inner_text().strip():
            print("[!] 检测到文字被前端框架静默清空，触发重试补录...")
            real_editor.click()
            real_editor.press_sequentially(comment, delay=60)

        print("[+] 文字填写并校验挂载完毕。")
    else:
        real_editor.click()
        page.wait_for_timeout(500)
        print("[*] 未提供正文文本，将只处理图片 / 链接等内容。")

    # =======================
    # 第 3.5 步：插入超链接
    # =======================
    if url_info_list and isinstance(url_info_list, (list, tuple)):
        print(f"[*] 检测到需要插入 {len(url_info_list)} 个超链接，启动注入流...")

        for idx, url_info in enumerate(url_info_list):
            if not isinstance(url_info, dict):
                print(f"[!] 警告: 第 {idx + 1} 个链接数据结构不合法，已跳过: {url_info}")
                continue

            link_text = str(url_info.get("text", "")).strip()
            link_url = str(url_info.get("url", "")).strip()

            if not link_text or not link_url:
                print(f"[!] 警告: 第 {idx + 1} 个链接缺少 text 或 url，已跳过。")
                continue

            # 自动补全协议，避免前端 URL 校验不通过导致确认按钮不激活
            if not re.match(r"^https?://", link_url, re.IGNORECASE):
                link_url = "https://" + link_url

            print(f"[*] 正在注入第 {idx + 1} 个超链接: [{link_text}]({link_url})")

            try:
                # 1. 光标切到编辑器末尾，并补一个空格，隔离正文和链接
                focus_editor_end()
                page.keyboard.press("Space")
                page.wait_for_timeout(150)

                # 2. 点击“三个点 / 更多”
                # 优先使用语义化选择器，SVG path 作为兜底
                more_candidates = [
                    editor_container.get_by_role(
                        "button",
                        name=re.compile(r"更多|More|Options|Expand", re.IGNORECASE)
                    ).first,
                    editor_container.locator(
                        'button[aria-label*="更多"], '
                        'button[aria-label*="More" i], '
                        'button[title*="更多"], '
                        'button[title*="More" i]'
                    ).first,
                    editor_container.locator("svg").filter(
                        has=page.locator('path[d^="M12 16.5"]')
                    ).first,
                ]
                click_first_visible(more_candidates, timeout=4000, desc="更多按钮")
                page.wait_for_timeout(350)

                # 3. 点击“添加链接”
                add_link_candidates = [
                    page.get_by_role(
                        "menuitem",
                        name=re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
                    ).first,
                    page.locator("div.menu-item").filter(
                        has_text=re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
                    ).first,
                    page.locator('[role="menuitem"], .menu-item, [class*="menu-item"]').filter(
                        has_text=re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
                    ).first,
                ]
                click_first_visible(add_link_candidates, timeout=4000, desc="添加链接选项")

                # 4. 尽量锁定弹窗作用域
                dialog = page
                try:
                    dlg = page.get_by_role("dialog").last
                    dlg.wait_for(state="visible", timeout=2000)
                    dialog = dlg
                except Exception:
                    dialog = page

                # 5. 查找链接正文输入框
                name_candidates = [
                    dialog.locator('input[name="name"][data-bn-type="input"]').first,
                    dialog.locator('input[name="name"]').first,
                    dialog.get_by_placeholder(
                        re.compile(r"正文|名称|标题|text|name|title", re.IGNORECASE)
                    ).first,
                ]
                name_input = wait_first_visible(
                    name_candidates,
                    timeout=6000,
                    desc="链接正文输入框"
                )

                # 6. 查找链接地址输入框
                link_candidates = [
                    dialog.locator('input[name="link"][data-bn-type="input"]').first,
                    dialog.locator('input[name="link"]').first,
                    dialog.get_by_placeholder(
                        re.compile(r"链接|地址|link|url|address", re.IGNORECASE)
                    ).first,
                ]
                link_input = wait_first_visible(
                    link_candidates,
                    timeout=6000,
                    desc="链接地址输入框"
                )

                # 7. 查找确认按钮
                confirm_candidates = [
                    dialog.locator('button[type="submit"][data-bn-type="button"]').filter(
                        has_text=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)
                    ).first,
                    dialog.locator('button[type="submit"]').filter(
                        has_text=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)
                    ).first,
                    dialog.get_by_role(
                        "button",
                        name=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)
                    ).first,
                    dialog.locator('button[data-bn-type="button"]').filter(
                        has_text=re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)
                    ).first,
                ]
                confirm_btn = wait_first_visible(
                    confirm_candidates,
                    timeout=6000,
                    desc="链接确认按钮"
                )

                # 8. 填写链接信息
                name_input.fill(link_text)
                page.wait_for_timeout(200)

                link_input.fill(link_url)
                page.wait_for_timeout(200)

                # 9. 等待前端校验通过，确认按钮激活
                expect(confirm_btn).to_be_enabled(timeout=6000)

                # 10. 点击确认
                confirm_btn.click(timeout=6000)

                # 11. 等待弹窗关闭，而不是固定 sleep
                expect(name_input).to_be_hidden(timeout=6000)

                # 12. 校验链接是否真的插入到富文本编辑器中
                link_locator = real_editor.locator("a").filter(
                    has_text=re.compile(re.escape(link_text), re.IGNORECASE)
                )
                expect(link_locator.first).to_be_visible(timeout=5000)

                print(f"[+] 链接 [{link_text}] 插入成功。")

                # 13. 再次把光标移到末尾，并补空格，避免下一个链接粘连
                focus_editor_end()
                page.keyboard.press("Space")
                page.wait_for_timeout(200)

            except Exception as e:
                print(f"[!] 警告: 注入链接 [{link_text}] 时发生异常，尝试关闭弹层并跳过。原因: {e}")
                close_overlay()

                # 如果弹层已经彻底卡死，继续执行可能也会失败。
                # 如果你希望更保守，可以这里直接 break。
                # break

    # =======================
    # 第 4 步：检查发送按钮
    # =======================
    print("[*] 检查发送按钮状态...")

    send_button_candidates = [
        editor_container.locator("button").filter(
            has_text=re.compile(r"^回复$|^发送$|^Reply$|^Comment$", re.IGNORECASE)
        ).first,
        editor_container.get_by_role(
            "button",
            name=re.compile(r"回复|发送|Reply|Comment", re.IGNORECASE)
        ).first,
    ]

    try:
        send_button = wait_first_visible(send_button_candidates, timeout=10000, desc="发送按钮")
    except Exception:
        print("[!] 警告: 未能稳定定位发送按钮，将使用默认第一个候选按钮继续。")
        send_button = send_button_candidates[0]

    try:
        expect(send_button).to_be_enabled(timeout=10000)
    except Exception:
        print("[!] 警告: 发送按钮预期状态未达标，但将尝试强制执行流...")

    # 发送前记录编辑器状态，用于后续 DOM 兜底校验
    try:
        text_before_send = real_editor.inner_text().strip()
    except Exception:
        text_before_send = ""

    try:
        media_before_send = real_editor.locator("img, a").count()
    except Exception:
        media_before_send = 0

    # =======================
    # 第 5 步：发送 + API 监听 + DOM 兜底
    # =======================
    print("[*] 执行发送操作 (API网络监听护航中)...")

    api_success = False
    api_error_msg = ""

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
            json_data = resp.json()
            if not isinstance(json_data, dict):
                json_data = {"raw": str(json_data)}
        except Exception:
            json_data = {}

        if str(json_data.get("code", "")) == "000000" or json_data.get("success") is True:
            api_success = True
            print("[+] API 返回底层确认，评论 100% 发送成功！")
        elif json_data:
            api_error_msg = json_data.get("message") or str(json_data)
            print(f"[-] API 抓取到明确报错(风控/频率限制): {api_error_msg}")
            raise BusinessErrorException(f"被币安服务器拒绝: {api_error_msg}")
        else:
            print("[*] API 响应无法解析为 JSON，将依赖 DOM 兜底校验。")

    except PlaywrightTimeoutError:
        print("[*] API 监听超时(可能接口改版或网络极卡)，启动 DOM 兜底校验...")

    # DOM 兜底校验
    if not api_success:
        time.sleep(3)

        try:
            editor_visible = real_editor.is_visible()
        except Exception:
            editor_visible = False

        if editor_visible:
            try:
                text_after_send = real_editor.inner_text().strip()
            except Exception:
                text_after_send = ""

            try:
                media_after_send = real_editor.locator("img, a").count()
            except Exception:
                media_after_send = 0
        else:
            text_after_send = ""
            media_after_send = 0

        text_cleared = (
            len(text_before_send) > 0 and
            len(text_after_send) < (len(text_before_send) / 3)
        )

        media_cleared = (
            media_before_send > 0 and
            media_after_send < media_before_send
        )

        if text_cleared or media_cleared:
            print("[+] DOM 兜底校验通过：编辑器内容已成功清空，判定为成功！")
        else:
            raise Exception(
                "发送操作已执行，但输入框未清空且 API 无明确成功响应，"
                "疑似发送失败 (或者按钮根本没激活)。"
            )


def comment_on_binance_post(
    post_url: str,
    comment: str,
    image_path: Optional[str] = None,
    user_data_dir=USER_DATA_DIR,
    url_info_list: Optional[list] = None
) -> Tuple[Optional[str], bool]:
    """
    币安帖子评论入口函数。

    参数:
        post_url: 帖子 URL
        comment: 评论正文
        image_path: 可选，图片路径
        user_data_dir: 浏览器用户数据目录
        url_info_list: 可选，超链接列表，格式示例：
            [
                {"text": "链接标题", "url": "https://example.com"},
                {"text": "第二个链接", "url": "https://example.org"}
            ]

    返回:
        (error_info, is_success)
    """

    if not os.path.isdir(user_data_dir):
        return f"用户数据目录不存在: {user_data_dir}\n请先运行登录流程获取 Cookie。", False

    error_info = None
    is_success = False
    context = None

    print(f"\n{'=' * 60}\n🚀 开始评论任务: {post_url}\n{'=' * 60}")

    with sync_playwright() as p:
        try:
            # 启动持久化环境
            context = p.chromium.launch_persistent_context(
                channel="chrome",
                user_data_dir=user_data_dir,
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--start-maximized"
                ],
                ignore_default_args=["--enable-automation"]
            )

            page = context.pages[0] if context.pages else context.new_page()

            # =======================
            # 加载帖子
            # =======================
            print("[*] 正在加载目标帖子...")
            page.goto(post_url)
            page.wait_for_load_state("domcontentloaded")

            # =======================
            # 登录状态探活
            # =======================
            try:
                page.locator("a[href*='login']").first.wait_for(state="visible", timeout=3000)
                raise Exception("检测到登录失效 (页面存在 Login 按钮)，请重新执行 login_and_save_session。")
            except PlaywrightTimeoutError:
                pass

            check_for_crash(page)

            # =======================
            # 定位编辑器
            # =======================
            editor_container = _smart_scroll_to_editor(page)
            check_for_crash(page)

            # =======================
            # 执行评论流
            # =======================
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

        except Exception as e:
            error_info = str(e)
            print(f"\n[!] 执行过程中发生严重异常: {error_info[:500]}")

            if context and context.pages:
                try:
                    ts = int(time.time())
                    context.pages[0].screenshot(path=f"error_screenshot_{ts}.png")

                    with open(f"error_html_{ts}.html", "w", encoding="utf-8") as f:
                        f.write(context.pages[0].content())

                    print(f"[*] 现场截图和HTML已保存，时间戳: {ts}")
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
    """
    终极健壮版：
    - CSRF Token: 通过真实 API 拦截动态获取。
    - Cookie String: 从浏览器底层 Context 强行提取并拼接。
    """
    if not os.path.exists(user_data_dir):
        print(f"[-] 用户数据目录不存在，请先执行登录: {user_data_dir}")
        return None, None

    visit_url = "https://www.binance.com/zh-CN/square/profile/insights_anchor"
    target_api_keyword = "pgc/user/client"

    print(f"[*] 启动浏览器，准备执行综合凭证提取...")

    with sync_playwright() as p:
        try:
            context = p.chromium.launch_persistent_context(
                channel="chrome",
                user_data_dir=user_data_dir,
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--headless=new'
                ]
            )

            page = context.pages[0] if context.pages else context.new_page()

            extracted_cookie = None
            extracted_csrf = None

            print(f"[*] 正在挂载网络监听网兜，目标特征: {target_api_keyword}")

            # 【步骤一】：拦截前端请求，抓取 JS 动态生成的 csrftoken
            with page.expect_request(lambda request: target_api_keyword in request.url, timeout=15000) as first_request:
                page.goto(visit_url)

            req = first_request.value

            # Playwright 会将 headers 的 key 全转为小写
            extracted_csrf = req.headers.get("csrftoken")

            # 【步骤二】：直接绕过请求头，从浏览器上下文中强抽 Cookie 并标准化拼接
            raw_cookies = context.cookies(urls=["https://www.binance.com"])
            cookie_parts = [f"{c['name']}={c['value']}" for c in raw_cookies]
            extracted_cookie = "; ".join(cookie_parts)

            context.close()

            # 校验最终成果
            if extracted_cookie and extracted_csrf:
                print(f"[+] 提取成功！CSRF: {extracted_csrf[:8]}... | Cookie 长度: {len(extracted_cookie)}")
                return extracted_cookie, extracted_csrf
            else:
                print(
                    f"[-] 提取结果异常。Cookie获取状态: {bool(extracted_cookie)}, CSRF获取状态: {bool(extracted_csrf)}")
                return extracted_cookie, extracted_csrf

        except PlaywrightTimeoutError:
            print(f"[-] 拦截超时！在 15 秒内未能捕获到包含 '{target_api_keyword}' 的网络请求。")
            return None, None
        except Exception as e:
            print(f"[!] 提取过程发生未知的严重异常: {e}")
            return None, None


def open_browser_for_manual_use(user_data_dir: str):
    """
    启动并挂起浏览器，将控制权完全交给用户进行任意手动操作。
    关闭浏览器窗口后，程序才会结束。

    :param user_data_dir: 浏览器用户数据目录路径
    """
    print(f"{'=' * 50}")
    print(f"🚀 正在启动本地浏览器环境...")
    print(f"📁 配置目录: {user_data_dir}")
    print(f"{'=' * 50}")

    with sync_playwright() as p:
        try:
            # 启动持久化上下文，和你的主脚本保持一致的防风控参数
            context = p.chromium.launch_persistent_context(
                channel="chrome",  # 强制使用本地安装的 Chrome (更真实)
                user_data_dir=user_data_dir,
                headless=False,  # 必须为 False，显示界面
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--start-maximized'
                ],
                ignore_default_args=["--enable-automation"]
            )

            # 获取当前存在的第一个页面，如果没有则新建
            page = context.pages[0] if context.pages else context.new_page()

            # 默认打开币安主页（你也可以改成其他任意网址，或直接注释掉这一行）
            page.goto('https://www.binance.com/zh-CN')

            print("\n✅ 浏览器已成功启动！")
            print("🕹️  现在你可以任意在浏览器内进行手动操作 (点赞、浏览、交易等)。")
            print("💡 提示: 你的所有登录状态和操作都会被保存在该目录中。")
            print("🛑 退出方式: 直接点击右上角关闭浏览器窗口，程序将自动结束运行。")

            # 【核心逻辑】：无限期挂起主线程，直到该页面被用户手动关闭
            # timeout=0 表示永不超时
            page.wait_for_event("close", timeout=0)

        except Exception as e:
            print(f"\n[!] 浏览器运行中出现异常: {e}")
        finally:
            print("\n👋 浏览器已关闭，结束接管，释放系统资源。")

# ==============================================================================
# 启动入口
# ==============================================================================
if __name__ == '__main__':

    # # 获取cookie方便api调用凭证
    cookie_str, csrf_token = get_auth_tokens_robust(USER_DATA_DIR)

    login_and_save_session()

    open_browser_for_manual_use(USER_DATA_DIR)

    test_url = "https://www.binance.com/zh-CN/square/post/309692475255842"
    test_msg = "少即是多，慢即是快。同频共振！🚀"

    # 无图测试设为 None，有图填绝对路径
    test_img = r"C:\Users\zxh\Desktop\temp\a6c98436-42f9-4aa9-bab8-.png"

    my_urls = [
        {
            "text": "带单主页",
            "url": "https://www.binance.com/zh-CN/square/post/309692475255842"
        }
    ]

    # 将 my_urls 传入主控函数
    err, success = comment_on_binance_post(
        post_url=test_url,
        comment=test_msg,
        image_path=test_img,
        url_info_list=my_urls  # 传入新增参数
    )

    if success:
        print("\n🎉 ======== 自动评论任务圆满成功 ========")
    else:
        print(f"\n❌ ======== 失败记录 ========\n原因: {err}")