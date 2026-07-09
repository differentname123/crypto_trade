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
USER_DATA_DIR = r"W:\temp\dahao"
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


def _submit_comment(page: Page, editor_container, comment: str, image_path: Optional[str] = None):
    """在锁定的局部作用域内执行输入、传图、发送、校验全流程"""

    # --- 第 1 步：点击伪装框唤醒真实编辑器 ---
    print("[*] 正在唤醒编辑器...")
    # [特征锚点 2 - 伪装框]: 容器里第一个 input
    fake_input = editor_container.locator('input[type="text"], input[placeholder]').first
    fake_input.click()

    # [特征锚点 3 - 真编辑器]: 带有 contenteditable="true" 和 ProseMirror 的 div
    real_editor = editor_container.locator('div[contenteditable="true"].ProseMirror').first
    expect(real_editor).to_be_editable(timeout=8000)

    # --- 第 2 步：模拟真人逐字输入 ---
    print("[*] 编辑器就绪，开始填入文字...")
    real_editor.click()
    # 弃用 keyboard.type，使用更底层的 press_sequentially 模拟真实物理敲击
    real_editor.press_sequentially(comment, delay=30)
    print("[+] 文字填写完毕。")

    # --- 第 3 步：确定性传图 (告别 sleep) ---
    if image_path and os.path.exists(image_path):
        print(f"[*] 准备上传图片: {image_path}")
        try:
            # 直接暴力向底层 input 注入文件，最稳
            file_input = editor_container.locator('input[type="file"]').first
            file_input.set_input_files(image_path)

            # [特征锚点 4 - 图片预览]: 传图后出现的缩略图大框
            preview_box = editor_container.locator('div.images-box-item').first
            print("[*] 图片已注入，等待服务器返回缩略图...")

            # 见图行事：最多等30秒。如果网快 0.5秒出来，代码就0.5秒放行
            preview_box.wait_for(state="visible", timeout=30000)
            print("[+] 图片渲染成功！")
        except Exception as e:
            print(f"[!] 警告: 图片上传似乎失败或超时，将降级发送纯文本。原因: {e}")

    # --- 第 4 步：防抖拦截与按钮就绪检查 ---
    print("[*] 检查发送按钮状态...")
    # [特征锚点 5 - 发送按钮]: 容器内的发送按钮
    send_button = editor_container.locator("button").filter(
        has_text=re.compile(r"^回复$|^发送$|^Reply$|^Comment$", re.IGNORECASE)
    ).first

    # [特征锚点 6 - 按钮不可用特征]: aria-disabled="true"
    # 等待按钮脱离 disabled 状态（等待图片传完后台防抖结束）
    expect(send_button).not_to_have_attribute("aria-disabled", "true", timeout=15000)

    # --- 第 5 步：API 状态机 + 降级点击 + DOM 兜底 (核心神技) ---
    print("[*] 执行发送操作 (API网络监听护航中)...")

    api_success = False
    api_error_msg = ""

    try:
        # 挂起 API 监听网兜: 盯死 pgc/content/add 接口，限时 10 秒
        # [特征锚点 7 - 发送 API]: URL 包含 /pgc/content/add
        with page.expect_response(
                lambda response: "pgc/content/add" in response.url and response.request.method == "POST",
                timeout=10000) as response_info:

            # 【三级降级点击策略】
            try:
                send_button.click(timeout=1500)  # 1. 模拟真人点
            except:
                try:
                    send_button.click(force=True, timeout=1500)  # 2. 穿透图层挡版点
                except:
                    send_button.evaluate("node => node.click()")  # 3. JS 底层强杀点

        # 捕获到了 API 响应，进行解析
        resp = response_info.value
        json_data = resp.json()

        # [特征锚点 8 - 成功标志]: code 为 "000000" 或 success 为 true
        if json_data.get("code") == "000000" or json_data.get("success") is True:
            api_success = True
            print(f"[+] API 返回底层确认，评论 100% 发送成功！(耗时极短)")
        else:
            api_error_msg = json_data.get("message") or str(json_data)
            print(f"[-] API 抓取到明确报错(风控/频率限制): {api_error_msg}")
            raise BusinessErrorException(f"被币安服务器拒绝: {api_error_msg}")

    except PlaywrightTimeoutError:
        print("[*] API 监听超时(可能接口改版或网络极卡)，启动老版本 DOM 兜底校验...")
        pass  # 不报错，交给下面的 DOM 兜底处理

    # 【DOM 兜底检验】如果 API 没抓到，用原来的方法看字数变没变少
    if not api_success:
        time.sleep(2)  # 给前端一点渲染收起的时间
        current_text = real_editor.inner_text().strip() if real_editor.is_visible() else ""
        if len(current_text) < (len(comment.strip()) / 3):
            print("[+] DOM 兜底校验通过：输入框文字已清空，判定为成功！")
        else:
            raise Exception("发送操作已执行，但输入框未清空且API无响应，疑似发送失败。")


def comment_on_binance_post(post_url: str, comment: str, image_path: Optional[str] = None,
                            user_data_dir=USER_DATA_DIR) -> Tuple[Optional[str], bool]:
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
                channel="chrome", user_data_dir=user_data_dir, headless=False,
                args=['--disable-blink-features=AutomationControlled', '--start-maximized'],
                ignore_default_args=["--enable-automation"]
            )
            page = context.pages[0] if context.pages else context.new_page()

            # --- 前置校验与加载 ---
            print("[*] 正在加载目标帖子...")
            page.goto(post_url)
            page.wait_for_load_state("domcontentloaded")

            # 【前置探活】：检查是否处于登录状态 (通过右下角有没有全局的发帖按钮等特征，这里用松散校验)
            if page.locator("a[href*='login']").is_visible(timeout=3000):
                raise Exception("检测到登录失效 (页面存在 Login 按钮)，请重新执行 login_and_save_session。")

            check_for_crash(page)

            # --- 智能探索与作用域锁定 ---
            editor_container = _smart_scroll_to_editor(page)
            check_for_crash(page)

            # --- 执行评论流 ---
            _submit_comment(page, editor_container, comment, image_path)

            is_success = True

        except BusinessErrorException as biz_e:
            # 明确的业务报错（如太频繁），不需要人工挂起，直接返回给上层调度记录
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
                except:
                    pass

            # 【终极防线】：人工接管替代 sleep(1000)
            human_intervention_pause(error_info)

        finally:
            if context:
                try:
                    context.close()
                except:
                    pass

    return error_info, is_success


# ==============================================================================
# 启动入口
# ==============================================================================
if __name__ == '__main__':
    # login_and_save_session()

    test_url = "https://www.binance.com/zh-CN/square/post/309692475255842"
    test_msg = "少即是多，慢即是快。同频共振！🚀"

    # 无图测试设为 None，有图填绝对路径
    test_img = None

    err, success = comment_on_binance_post(test_url, test_msg, test_img)

    if success:
        print("\n🎉 ======== 自动评论任务圆满成功 ========")
    else:
        print(f"\n❌ ======== 失败记录 ========\n原因: {err}")