# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 1:12
:last_date:
    2026/4/7 1:12
:description:
    
"""
import json
import os
import re
import shutil
import time
import argparse
import sys
from typing import Tuple, Optional
from playwright.sync_api import sync_playwright, Page, expect, Locator
import datetime
import traceback

# ==============================================================================
# 配置区域
# ==============================================================================
# 用于保存浏览器登录状态的目录，请确保该目录可写
USER_DATA_DIR = r"W:\temp\binance_browser_data"
# 登录目标地址
LOGIN_URL = 'https://www.binance.com/zh-CN/login'


# ==============================================================================
# 核心功能函数 (底层逻辑保留，未涉及业务处严禁修改)
# ==============================================================================

def _get_dir_size(start_path='.'):
    """计算目录总大小 (返回字节数)"""
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
    """将字节转换为易读的格式 (MB, GB)"""
    power = 1024
    n = size
    power_labels = {0: '', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    count = 0
    while n > power:
        n /= power
        count += 1
    return f"{n:.2f} {power_labels.get(count, 'B')}"


def clean_browser_cache(user_data_dir: str):
    """
    深度清理 Chromium 用户目录缓存，并显示清理前后的体积变化。
    保留 Cookies, LocalStorage 以维持登录状态。
    """
    if not os.path.exists(user_data_dir):
        print(f"[-] 目录不存在，无需清理: {user_data_dir}")
        return

    print("=" * 40)
    print("🚀 正在执行浏览器数据瘦身...")

    size_before = _get_dir_size(user_data_dir)
    print(f"[*] 清理前占用空间: {_format_size(size_before)}")

    garbage_targets = [
        "Cache", "Code Cache", "GPUCache", "ShaderCache", "GrShaderCache",
        "Service Worker", "CacheStorage", "ScriptCache", "Crashpad",
        "BrowserMetrics", "Safe Browsing", "blob_storage", "OptimizationGuidePredictionModels",
    ]

    scan_paths = [user_data_dir, os.path.join(user_data_dir, "Default")]
    deleted_count = 0

    for base_path in scan_paths:
        if not os.path.exists(base_path):
            continue
        for target in garbage_targets:
            target_full_path = os.path.join(base_path, target)
            if os.path.exists(target_full_path):
                try:
                    if os.path.isdir(target_full_path):
                        shutil.rmtree(target_full_path, ignore_errors=True)
                    else:
                        os.remove(target_full_path)
                    deleted_count += 1
                except Exception:
                    pass

    size_after = _get_dir_size(user_data_dir)
    freed_size = size_before - size_after

    print(f"[*] 清理后占用空间: {_format_size(size_after)}")
    print(f"[+] 成功释放空间:   {_format_size(freed_size)} (清理了 {deleted_count} 个项目)")
    print("=" * 40)


class PageCrashedException(Exception):
    """自定义异常，用于表示页面已崩溃。"""
    pass


def check_for_crash_and_abort(page: Page):
    """
    (内部调用) 快速检查页面是否崩溃。如果崩溃，则立即抛出异常以终止任务。
    """
    try:
        reload_button = page.get_by_role("button", name="重新加载")
        if reload_button.is_visible(timeout=1000):
            error_msg = "页面已崩溃 (检测到 '重新加载' 按钮)，任务终止。"
            print(f"[!] {error_msg}")
            raise PageCrashedException(error_msg)
    except Exception as e:
        if isinstance(e, PageCrashedException):
            raise
        pass


def login_and_save_session():
    """
    启动浏览器，让用户手动登录，并将登录会话保存到 USER_DATA_DIR。
    """
    print("--- 启动浏览器进行手动登录 ---")
    print(f"会话信息将保存在: {USER_DATA_DIR}")
    clean_browser_cache(USER_DATA_DIR)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            channel="chrome",
            user_data_dir=USER_DATA_DIR,
            headless=False,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--start-maximized',
                '--disable-gpu',
                '--disk-cache-size=1',
                '--window-position=0,0',
                '--media-cache-size=1',
                '--disable-application-cache',
                '--disable-component-update'
            ],
            ignore_default_args=["--enable-automation"]
        )

        page = context.new_page()
        page.goto(LOGIN_URL)

        print("\n" + "=" * 60)
        print("浏览器已打开。请在浏览器窗口中手动完成币安的登录操作。")
        print("登录成功后，请回到本命令行窗口，然后按 Enter 键继续...")
        print("=" * 60)

        input()
        context.close()
        print("\n[+] 登录会话信息已成功保存。现在可以使用评论功能运行任务了。")


# ==============================================================================
# 币安业务相关函数 (完全重写以适配 Binance Square)
# ==============================================================================

def _scroll_page_to_bottom(page: Page, steps: int = 10, step_px: int = 800, delay: float = 0.1):
    """向下滚动页面以触发评论区加载"""
    for _ in range(steps):
        try:
            vp = page.viewport_size or {"width": 1280, "height": 720}
            page.mouse.move(vp["width"] / 2, vp["height"] / 2)
            page.mouse.wheel(0, step_px)
        except:
            pass
        time.sleep(delay)


def _submit_comment(page: Page, comment: str):
    """(内部调用) 在币安广场帖子中定位并发送评论"""
    print("[*] 正在定位评论输入框...")

    try:
        # --- 第 1 步：点击伪装的占位输入框 ---
        # 兼容中英文占位符，且兼容 <input> 元素
        trigger_input = page.locator(
            'input[placeholder*="回复"], input[placeholder*="Reply"], input[placeholder*="发布"]').first

        # 如果正则表达式找不到，尝试用类名特征做备用方案
        if not trigger_input.is_visible(timeout=5000):
            trigger_input = page.locator("input.bg-transparent").first

        expect(trigger_input).to_be_visible(timeout=10000)
        trigger_input.scroll_into_view_if_needed()
        print("[*] 找到初始占位输入框，正在点击以唤醒真实编辑器...")
        trigger_input.click()

        # 给予前端 React 动态渲染 DOM 的时间
        time.sleep(1)

        # --- 第 2 步：操作真实的富文本编辑器 ---
        # 根据你提供的结构: <div class="ProseMirror" contenteditable="true">
        real_editor = page.locator('div.ProseMirror[contenteditable="true"], div[contenteditable="true"]').last
        expect(real_editor).to_be_editable(timeout=10000)

        print("[*] 编辑器已就绪，正在输入评论内容...")
        real_editor.click()
        time.sleep(0.5)

        # 重点：对于复杂的富文本编辑器，避免用 .fill() 覆盖节点结构
        # 改用 keyboard.type 逐字模拟敲击，能最大概率触发前端的字数统计和状态校验
        page.keyboard.type(comment, delay=30)
        print("[+] 评论内容已填入。")

        # 再次等待，确保前端状态机捕获到文字变化并解禁“发送”按钮
        time.sleep(1.5)

        # --- 第 3 步：点击提交按钮 ---
        # 严格匹配按钮上的文字，同时兼容中英文
        send_button = page.locator("button").filter(
            has_text=re.compile(r"^回复$|^发送$|^Reply$|^Comment$", re.IGNORECASE)
        ).last

        expect(send_button).to_be_enabled(timeout=10000)
        print("[*] 发送按钮已就绪，正在点击...")
        send_button.click()

        # 等待接口响应完成
        time.sleep(3)

    except Exception as e:
        raise Exception(f"提交评论时遇到障碍: {e}")

def comment_on_binance_post(post_url: str, comment: str, user_data_dir=USER_DATA_DIR) -> Tuple[Optional[str], bool]:
    """
    使用已保存的登录会话启动浏览器，访问币安广场帖子并自动发送评论。

    Returns:
        Tuple[str, bool]: (error_info, is_success)
    """
    if not os.path.isdir(user_data_dir):
        error_msg = f"用户数据目录不存在: {user_data_dir}\n请先运行登录流程获取 Cookie。"
        return error_msg, False

    error_info = None
    is_success = False
    context = None

    print(f"--- 开始评论任务: URL='{post_url}', 内容='{comment[:20]}...' ---")

    # 【核心调整】：让 with 语句包裹整个错误捕获流程
    with sync_playwright() as p:
        try:
            if (15 <= datetime.datetime.now().hour < 15):
                context = p.chromium.launch_persistent_context(
                    channel="chrome",
                    user_data_dir=user_data_dir,
                    headless=False,
                    viewport={'width': 1920, 'height': 1080},
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-gpu',
                        '--window-position=-10000,-10000',
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-renderer-backgrounding',
                        '--disable-background-timer-throttling',
                        '--disable-backgrounding-occluded-windows',
                        '--disable-features=CalculateNativeWinOcclusion',
                        '--disable-breakpad',
                    ],
                    ignore_default_args=["--enable-automation"]
                )
            else:
                context = p.chromium.launch_persistent_context(
                    channel="chrome",
                    user_data_dir=user_data_dir,
                    headless=False,
                    args=['--disable-blink-features=AutomationControlled', '--start-maximized', '--disable-gpu',
                          '--window-position=0,0'],
                    ignore_default_args=["--enable-automation"]
                )

            page = context.pages[0] if context.pages else context.new_page()
            page.set_default_timeout(60000)

            print("[*] 正在加载目标帖子...")
            page.goto(post_url)

            # 等待网络空闲，确保 React 组件加载完毕
            page.wait_for_load_state("networkidle")
            check_for_crash_and_abort(page)

            # 稍微滚动页面，让评论框出现在视野范围内（懒加载触发）
            _scroll_page_to_bottom(page, steps=5)
            check_for_crash_and_abort(page)

            # 执行填写和提交
            _submit_comment(page, comment)

            check_for_crash_and_abort(page)
            print("[+] 评论流执行完毕！")
            is_success = True

        except PageCrashedException as crash_e:
            error_info = str(crash_e)
            if context and context.pages:
                try:
                    screenshot_path = f"crash_screenshot_{int(time.time())}.png"
                    if context.pages:
                        context.pages[0].screenshot(path=screenshot_path)
                        print(f"[*] 崩溃截图已保存至: {screenshot_path}")
                except Exception:
                    pass
            print("[!] 页面崩溃，暂停 1000 秒供排查...")
            time.sleep(1000)

        except Exception as e:
            error_info = str(e)
            print(f"[!] 执行过程中发生错误: {error_info[:1000]}")
            if context and context.pages:
                try:
                    screenshot_path = f"error_screenshot_{int(time.time())}.png"
                    if context.pages:
                        context.pages[0].screenshot(path=screenshot_path)
                        print(f"[*] 错误截图已保存至: {screenshot_path}")
                except Exception:
                    pass

            # 【新增】：报错后在这里死等，此时浏览器处于存活状态
            print("[!] 出现错误，已暂停 1000 秒。请去弹出的浏览器窗口中排查元素...")
            time.sleep(1000)

        finally:
            if context:
                try:
                    context.close()
                    print("[*] 浏览器环境已关闭。")
                except Exception:
                    pass

    return error_info, is_success

# ==============================================================================
# 程序主入口和使用示例
# ==============================================================================
if __name__ == '__main__':
    # ==================================
    # 步骤 1：首次使用请取消下面这行的注释进行登录
    # ==================================
    login_and_save_session()

    # ==================================
    # 步骤 2：登录成功后，使用自动化评论功能
    # ==================================
    test_post_url = "https://www.binance.com/zh-CN/square/post/309671050623009"
    test_comment_text = "支持！非常深度的分析，感谢博主分享！"

    err, success = comment_on_binance_post(post_url=test_post_url, comment=test_comment_text)

    if err:
        print("\n======== ❌ 失败 ========")
        print(f"错误信息: {err}")
    else:
        print("\n======== ✅ 成功 ========")
        print("自动评论任务已顺利完成。")
