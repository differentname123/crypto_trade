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
import json
import os
import re
import shutil
import sys
import time
import traceback
import urllib
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeoutError

from biance.biance_squre_api import toggle_binance_follow
from common.common_utils import setup_logger

logger = setup_logger(app_name="biance_playwright")

# ==============================================================================
# 全局配置、正则常量与自定义异常
# ==============================================================================
USER_DATA_DIR = r"W:\temp\biance_jie"
LOGIN_URL = 'https://www.binance.com/zh-CN/login'

# 前端文案多语言/多变体的统一匹配规则，避免选择器中重复书写正则
RE_MORE = re.compile(r"更多|More|Options|Expand", re.IGNORECASE)
RE_ADD_LINK = re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
RE_CONFIRM = re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)
RE_SEND = re.compile(r"回复|发送|评论|Reply|Comment|Send|Post", re.IGNORECASE)
RE_SEND_EXACT = re.compile(r"^(回复|发送|评论|Reply|Comment|Send|Post)$", re.IGNORECASE)


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

    logger.info(f"\n[Cache/Clean] 启动浏览器数据瘦身 | 目标目录: <{user_data_dir}> | 结果: [执行中]")
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

    logger.info(f"[Cache/Clean] 瘦身完成 | 结果: [清理了 【{deleted}】 个冗余项]\n")


def check_for_crash(page):
    """探测页面是否崩溃：在 500ms 窗口内出现【重新加载】按钮即判定 DOM 渲染崩溃。"""
    try:
        page.get_by_role("button", name="重新加载").first.wait_for(state="visible", timeout=500)
        raise PageCrashedException("页面 DOM 渲染崩溃，检测到【重新加载】按钮")
    except PlaywrightTimeoutError:
        pass  # 未出现崩溃按钮，属正常情况




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
    logger.info(f"[Auth/Login] 准备手动登录 | 存储路径: <{USER_DATA_DIR}> | 结果: [启动中]")
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
        logger.info(f"[Auth/Login] 会话保存完毕 | 状态: [Success]")


def _smart_scroll_to_editor(page, max_scrolls=20):
    """步进式 PageDown 探底，锁定评论区富文本编辑器容器并回滚至可视范围。"""
    logger.info(f"[DOM/Locate] 开始向下探索评论区 | 最大尝试: <{max_scrolls}次> | 结果: [扫描中]")
    editor_container = page.locator("div.feed-post-editor").first

    for i in range(max_scrolls):
        if editor_container.is_visible():
            logger.info(f"[DOM/Locate] 探索成功 | 滚动次数: 【{i}】 | 结果: [已锁定局部作用域]")
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
    logger.info(f"[Editor/Link] 注入节点 [{idx + 1}] | 文本: 【{link_text}】 -> URL: <{link_url}> | 结果: [执行中]")
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

        logger.info(f"[Editor/Link] 注入节点 [{idx + 1}] 成功 | 状态: [Success]")
        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(200)
        return True

    except Exception as e:
        # 单条链接失败按原设计跳过：关闭可能残留的弹窗，继续注入下一条
        logger.info(f"[Editor/Link] 注入节点 [{idx + 1}] 失败，执行跳过 | 可能原因: 【菜单未唤醒/弹窗结构变动/校验超时: {e}】 | 结果: [Skipped]")
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
        return False


def human_intervention_pause(error_msg):
    """记录错误并阻断，不再使用 input() 挂起程序，实现全自动化报错退出。"""
    sys.stdout.write('\a')
    sys.stdout.flush()
    logger.info(f"\n{'=' * 50}")
    logger.info(f"[System/Halt] 🚨 触发异常中断机制 | 失败原因: 【{error_msg}】")
    logger.info(f"[System/Halt] 已自动保存故障现场，不再阻塞等待人工，程序即将直接返回结果...")
    logger.info(f"{'=' * 50}")
    # 🚀 修复核心：已移除 input()，彻底消除程序无期限卡死的问题 🚀


# ⚠️ 换行安全模式：
#   False（默认，与原版完全一致）—— 正文中的 \n 直接作为 Enter 键送出。
#   True （可选加固）—— 正文中的 \n 改用 Shift+Enter 送出，防止"Enter=发送"导致内容截断提前发出。
#   ！！这属于会改变键序的行为，故默认关闭。若你的正文含多行且曾遇到"内容被截断发出"，再置 True。
NEWLINE_SAFE_MODE = False

# 正文分块长度（仅影响 press_sequentially 的调用切分，键序与 delay 完全不变）
TYPE_CHUNK_SIZE = 80
TYPE_DELAY_MS = 60          # 与原版 delay=60 一致

# API 监听超时（与原版 expect_response 的 10000ms 一致）
API_WAIT_TIMEOUT_MS = 10000

# JS 看门狗轮询间隔
GUARD_INTERVAL_MS = 700


# ==============================================================================
#                              文案正则 / 选择器常量
# ==============================================================================

# 引导浮层"确认关闭"类按钮文案（刻意不含"取消/Cancel"，避免误取消业务弹窗）
RE_DISMISS = re.compile(
    r"^\s*(好的|好|知道了|我知道了|明白了|明白|了解|开始使用|立即体验|马上体验|下一步|"
    r"完成|跳过|不再提示|不再显示|以后再说|稍后|关闭|"
    r"OK|Okay|Got it|Got It|I see|Understood|Skip|Next|Done|Continue|Close|Dismiss|Later|Maybe later)\s*$",
    re.IGNORECASE,
)

# Cookie 同意（页面 HTML 中确实存在 OneTrust「隐私偏好中心/全部允许/确认我的选择」）
COOKIE_SELECTORS = [
    "#onetrust-accept-btn-handler",
    "#onetrust-close-btn-container button",
    "button:has-text('全部允许')",
    "button:has-text('确认我的选择')",
    "button:has-text('Accept All')",
    "button:has-text('Allow All')",
]

# 发送按钮黑名单：页面底部存在「立即回复」这类"打开编辑器"的跳转按钮，必须排除
RE_SEND_BLACKLIST = re.compile(
    r"(立即回复|去回复|查看|更多|展开|取消|Cancel|View|More)", re.IGNORECASE
)

# 编辑器保护选择器：任何清障动作都不允许碰到含这些元素的容器
EDITOR_GUARD_SELECTOR = (
    '.ProseMirror,[contenteditable="true"],input[type="file"],textarea'
)


# ==============================================================================
#                                  存证工具
# ==============================================================================

def _forensics(page, tag, extra=None):
    """
    统一存证：截图 + HTML + 结构化 JSON。
    任何降级 / 失败路径都调用，杜绝"死无对证"。
    """
    try:
        ts = int(time.time() * 1000)
        base = f"forensic_{tag}_{ts}"
        try:
            page.screenshot(path=f"{base}.png", full_page=False)
        except Exception:
            pass
        try:
            with open(f"{base}.html", "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            pass
        if extra:
            try:
                with open(f"{base}.json", "w", encoding="utf-8") as f:
                    json.dump(extra, f, ensure_ascii=False, indent=2, default=str)
            except Exception:
                pass
        logger.info(f"[Forensic] 现场已存证 | 前缀: <{base}> | 结果: [Saved]")
        return base
    except Exception as e:
        logger.info(f"[Forensic] 存证失败 | 可能原因: 【{e}】 | 结果: [Warning]")
        return None


# ==============================================================================
#                          命中测试：到底是谁挡住了目标元素
# ==============================================================================

_HIT_TEST_JS = r"""
(el) => {
    const r = el.getBoundingClientRect();
    if (r.width === 0 || r.height === 0) return {ok:false, reason:'zero-size'};
    const cx = r.left + r.width / 2;
    const cy = r.top + r.height / 2;
    if (cx < 0 || cy < 0 || cx > window.innerWidth || cy > window.innerHeight) {
        return {ok:false, reason:'outside-viewport',
                rect:{x:Math.round(r.left), y:Math.round(r.top),
                      w:Math.round(r.width), h:Math.round(r.height)}};
    }
    const top = document.elementFromPoint(cx, cy);
    if (!top) return {ok:false, reason:'no-element-at-point'};
    if (top === el || el.contains(top) || top.contains(el)) return {ok:true};

    // 向上追溯遮挡者，取其最外层 fixed 高层级祖先，便于整层处理
    let blocker = top, hop = 0;
    while (blocker.parentElement && hop < 12) {
        const st = window.getComputedStyle(blocker);
        if (st.position === 'fixed' && (parseInt(st.zIndex || '0', 10) > 0)) break;
        blocker = blocker.parentElement;
        hop++;
    }
    const br = blocker.getBoundingClientRect();
    return {
        ok: false,
        reason: 'intercepted',
        blockerTag: blocker.tagName,
        blockerClass: (blocker.className || '').toString().slice(0, 200),
        blockerId: blocker.id || '',
        blockerText: (blocker.innerText || '').replace(/\s+/g, ' ').slice(0, 200),
        blockerHtml: (blocker.outerHTML || '').slice(0, 1500),
        coverRatio: +(((br.width * br.height) /
                      (window.innerWidth * window.innerHeight)) || 0).toFixed(3),
        bodyOverflow: window.getComputedStyle(document.body).overflow,
        htmlOverflow: window.getComputedStyle(document.documentElement).overflow
    };
}
"""


def _hit_test(page, locator):
    """返回 dict：{ok: bool, ...遮挡者详情}。既是判断依据，也是最有价值的日志。"""
    try:
        return locator.evaluate(_HIT_TEST_JS)
    except Exception as e:
        return {"ok": False, "reason": f"eval-error:{str(e)[:120]}"}


# ==============================================================================
#                          浮层歼灭器（分级 + 编辑器白名单保护）
# ==============================================================================

_NUKE_OVERLAY_JS = r"""
(guardSelector) => {
    const killed = [];
    const isProtected = (n) => {
        try { return n.querySelector(guardSelector) !== null; } catch (e) { return false; }
    };

    // 1) 解除 scroll-lock（引导 / modal 几乎必配 body{overflow:hidden}）
    for (const el of [document.body, document.documentElement]) {
        try {
            const st = window.getComputedStyle(el);
            if (st.overflow === 'hidden' || st.overflowY === 'hidden') {
                el.style.setProperty('overflow', 'auto', 'important');
                el.style.setProperty('overflow-y', 'auto', 'important');
            }
            if (st.position === 'fixed') el.style.removeProperty('position');
            Array.from(el.classList).forEach(c => {
                if (/modal|dialog|lock|no-?scroll|overflow-hidden|popup-open/i.test(c)) {
                    el.classList.remove(c);
                }
            });
        } catch (e) {}
    }

    // 2) 清理覆盖视口中心的高层级浮层
    const cx = window.innerWidth / 2, cy = window.innerHeight / 2;
    const total = window.innerWidth * window.innerHeight;
    document.querySelectorAll('body *').forEach(n => {
        try {
            const st = window.getComputedStyle(n);
            if (st.position !== 'fixed' && st.position !== 'absolute') return;
            if (st.display === 'none' || st.visibility === 'hidden' || st.opacity === '0') return;
            const r = n.getBoundingClientRect();
            if (r.width < 40 || r.height < 40) return;
            const ratio = (r.width * r.height) / total;
            const coversCenter = (r.left <= cx && r.right >= cx && r.top <= cy && r.bottom >= cy);
            const z = parseInt(st.zIndex || '0', 10);
            // 判定：大面积遮罩(>=55%) 或 覆盖视口中心且层级很高
            if ((ratio >= 0.55 || (coversCenter && z >= 100)) && !isProtected(n)) {
                killed.push({
                    tag: n.tagName,
                    cls: (n.className || '').toString().slice(0, 80),
                    text: (n.innerText || '').replace(/\s+/g, ' ').slice(0, 60),
                    ratio: +ratio.toFixed(2), z: z
                });
                n.style.setProperty('pointer-events', 'none', 'important');
                n.style.setProperty('display', 'none', 'important');
            }
        } catch (e) {}
    });
    return killed;
}
"""


def _dismiss_overlays(page, aggressive=False, desc=""):
    """
    分级清障（不触碰任何含编辑器的容器）：
      L1 —— 按语义点掉（最安全，让前端正确写入 localStorage，后续不再弹）
      L2 —— 点弹窗关闭图标 / 按 Escape
      L3 —— aggressive=True 时物理移除遮罩 + 解 scroll-lock（兜底）
    返回：是否执行过任何清障动作。
    """
    acted = False

    # ---- L1-a：Cookie 横幅（OneTrust z-index 极高，必须先吃掉）----
    for sel in COOKIE_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=400):
                loc.click(timeout=2000, no_wait_after=True, force=True)
                acted = True
                logger.info(f"[Overlay/L1] 已处理 Cookie 横幅 | 选择器: <{sel}> | 场景: <{desc}> | 结果: [Closed]")
                page.wait_for_timeout(250)
        except Exception:
            pass

    # ---- L1-b：引导浮层「好的 / 知道了 / Got it」----
    makers = (
        lambda: page.get_by_role("button", name=RE_DISMISS),
        lambda: page.locator(
            "div[role='dialog'],[class*='modal'],[class*='mask'],[class*='guide'],"
            "[class*='onboard'],[class*='popup'],[class*='tooltip'],[class*='tour']"
        ).locator("button,[role='button'],div[class*='btn'],span[class*='btn'],a[class*='btn']"
                  ).filter(has_text=RE_DISMISS),
        lambda: page.locator("button,[role='button'],div[class*='btn'],span[class*='btn']"
                             ).filter(has_text=RE_DISMISS),
    )
    for maker in makers:
        try:
            loc = maker()
            cnt = min(loc.count(), 3)
            for i in range(cnt):
                item = loc.nth(i)
                try:
                    if not item.is_visible(timeout=300):
                        continue
                    # 白名单保护：绝不点击编辑器内部的按钮
                    inside_editor = item.evaluate(
                        """(el, gs) => {
                            const box = el.closest("div[role='dialog'],[class*='modal'],[class*='mask'],"
                                                   + "[class*='guide'],[class*='popup']") || el.parentElement;
                            return !!(box && box.querySelector(gs));
                        }""",
                        EDITOR_GUARD_SELECTOR,
                    )
                    if inside_editor:
                        continue
                    txt = (item.inner_text() or "").strip()[:20]
                    item.click(timeout=2500, no_wait_after=True, force=True)
                    acted = True
                    logger.info(f"[Overlay/L1] 已点掉引导浮层 | 文案: <{txt}> | 场景: <{desc}> | 结果: [Closed]")
                    page.wait_for_timeout(350)
                except Exception:
                    continue
        except Exception:
            pass

    # ---- L2：关闭图标 / Escape ----
    if not acted:
        try:
            close_ic = page.locator(
                "div[role='dialog'] [aria-label*='lose'],div[role='dialog'] [class*='close'],"
                "div[role='dialog'] svg[class*='close'],[class*='modal'] [class*='close']"
            ).first
            if close_ic.count() > 0 and close_ic.is_visible(timeout=300):
                close_ic.click(timeout=2000, force=True, no_wait_after=True)
                acted = True
                logger.info(f"[Overlay/L2] 已点击弹窗关闭图标 | 场景: <{desc}> | 结果: [Closed]")
                page.wait_for_timeout(250)
        except Exception:
            pass
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(150)
        except Exception:
            pass

    # ---- L3：物理歼灭 + 解锁滚动 ----
    if aggressive:
        try:
            killed = page.evaluate(_NUKE_OVERLAY_JS, EDITOR_GUARD_SELECTOR)
            if killed:
                acted = True
                logger.info(f"[Overlay/L3] 强制移除遮罩层 | 数量: <{len(killed)}> | 明细: 【{killed}】 | 结果: [Nuked]")
            else:
                logger.info(f"[Overlay/L3] 未发现可移除的遮罩层 | 场景: <{desc}> | 结果: [NoOp]")
        except Exception as e:
            logger.info(f"[Overlay/L3] 强制移除失败 | 可能原因: 【{str(e)[:120]}】 | 结果: [Warning]")

    return acted


# ==============================================================================
#            常驻守卫：init_script 预置 flag + JS 看门狗 + locator_handler
# ==============================================================================

_GUARD_INIT_JS = r"""
(() => {
  // ---- A. 预置常见引导"已读"flag（命中即彻底不弹；未命中也无副作用）----
  try {
    const HINT = /guide|tip|tour|onboard|popup|first|newbie|intro|welcome|banner/i;
    const SCOPE = /square|thread|post|short|feed|bibi|social/i;
    for (const k of Object.keys(localStorage)) {
      try {
        if (HINT.test(k) && (SCOPE.test(k) || HINT.test(k))) localStorage.setItem(k, 'true');
      } catch (e) {}
    }
    ['square_thread_guide_shown','square_guide_shown','bn_square_guide','square_guide_v2',
     'shortpost_guide','square_short_post_guide','thread_scroll_guide','square_onboarding']
      .forEach(k => { try { localStorage.setItem(k, 'true'); } catch (e) {} });
  } catch (e) {}

  // ---- B. 常驻看门狗：见引导按钮就点、见 scroll-lock 就解 ----
  const RE = /^\s*(好的|好|知道了|我知道了|明白了|明白|了解|开始使用|立即体验|下一步|完成|跳过|不再提示|不再显示|以后再说|稍后|OK|Okay|Got it|I see|Understood|Skip|Next|Continue|Dismiss|Later)\s*$/i;
  const GUARD_SEL = '.ProseMirror,[contenteditable="true"],input[type="file"],textarea';

  const tick = () => {
    try {
      // B1. 解 scroll-lock
      for (const el of [document.body, document.documentElement]) {
        const st = getComputedStyle(el);
        if (st.overflow === 'hidden' || st.overflowY === 'hidden') {
          el.style.setProperty('overflow', 'auto', 'important');
          el.style.setProperty('overflow-y', 'auto', 'important');
        }
      }
      // B2. 点掉引导确认按钮（跳过一切含编辑器的容器）
      const nodes = document.querySelectorAll(
        "button,[role='button'],div[class*='btn'],span[class*='btn'],a[class*='btn']");
      for (const n of nodes) {
        const t = (n.innerText || '').trim();
        if (!t || t.length > 12 || !RE.test(t)) continue;
        const box = n.closest("div[role='dialog'],[class*='modal'],[class*='mask'],"
                              + "[class*='guide'],[class*='popup'],[class*='tour']") || n.parentElement;
        if (box && box.querySelector(GUARD_SEL)) continue;   // 白名单保护
        const r = n.getBoundingClientRect();
        if (r.width > 0 && r.height > 0) {
          n.click();
          window.__bnGuardHits = (window.__bnGuardHits || 0) + 1;
          window.__bnGuardLast = t;
        }
      }
    } catch (e) {}
  };

  if (!window.__bnGuardTimer) {
    window.__bnGuardTimer = setInterval(tick, __GUARD_INTERVAL__);
    tick();
  }
})();
"""


def install_overlay_guard(page):
    """
    ⚠️ 必须在 page.goto() 之前调用（init_script 只对之后的导航生效）。
    三重保险：init_script 预置 + locator_handler 原生自愈 + 立即清一次。
    """
    try:
        page.add_init_script(_GUARD_INIT_JS.replace("__GUARD_INTERVAL__", str(GUARD_INTERVAL_MS)))
        logger.info("[Guard] 引导浮层 init_script 已注入（预置flag + JS看门狗） | 结果: [Armed]")
    except Exception as e:
        logger.info(f"[Guard] init_script 注入失败 | 可能原因: 【{str(e)[:120]}】 | 结果: [Warning]")

    # Playwright >= 1.42：任何 action 被浮层挡住前自动执行 handler 并重试
    try:
        page.add_locator_handler(
            page.locator(
                "div[role='dialog'],[class*='mask'],[class*='overlay'],[class*='backdrop'],[class*='guide']"
            ).filter(has_text=RE_DISMISS).first,
            lambda: _dismiss_overlays(page, aggressive=False, desc="locator_handler"),
            no_wait_after=True,
        )
        logger.info("[Guard] add_locator_handler 已挂载（遮挡自愈） | 结果: [Armed]")
    except TypeError:
        # 老版本签名不支持 no_wait_after
        try:
            page.add_locator_handler(
                page.locator("div[role='dialog'],[class*='mask'],[class*='guide']")
                    .filter(has_text=RE_DISMISS).first,
                lambda: _dismiss_overlays(page, aggressive=False, desc="locator_handler"),
            )
            logger.info("[Guard] add_locator_handler 已挂载（兼容模式） | 结果: [Armed]")
        except Exception as e2:
            logger.info(f"[Guard] locator_handler 不可用，退化为手动清障 | 详情: 【{str(e2)[:120]}】")
    except Exception as e:
        logger.info(f"[Guard] locator_handler 不可用(Playwright<1.42?)，退化为手动清障 | 详情: 【{str(e)[:120]}】")


def report_guard_hits(page, stage=""):
    """读取 JS 看门狗战果，便于事后定位到底自动关掉了什么。"""
    try:
        info = page.evaluate(
            "() => ({hits: window.__bnGuardHits || 0, last: window.__bnGuardLast || ''})")
        if info and info.get("hits"):
            logger.info(f"[Guard] 看门狗累计自动关闭浮层 | 次数: <{info['hits']}> "
                        f"| 最后文案: <{info['last']}> | 阶段: <{stage}>")
    except Exception:
        pass


# ==============================================================================
#                        步骤1 辅助：唤醒目标解析 + 4 级降级唤醒
# ==============================================================================

def _resolve_wake_target(editor_container):
    """
    唤醒目标候选（保持原版首选选择器，仅排除搜索框，避免点到右上角「搜索」input）。
    """
    cands = [
        # 原版首选：input[type=text] / input[placeholder]，但显式排除搜索类
        editor_container.locator(
            'input[type="text"]:not([type="search"]):not([placeholder*="搜索"])'
            ':not([placeholder*="Search"]):not([aria-label*="搜索"]):not([aria-label*="Search"]),'
            'input[placeholder]:not([type="search"]):not([placeholder*="搜索"]):not([placeholder*="Search"])'
        ).first,
        # 语义候选：回复框占位符
        editor_container.get_by_placeholder(
            re.compile(r"(发布您的回复|发布你的回复|写下你的|说点什么|发表评论|回复|评论|Reply|Comment|Write|Post)")
        ).first,
        # 直接点 ProseMirror 本体
        editor_container.locator('div[contenteditable="true"].ProseMirror').first,
        editor_container.locator('div[contenteditable="true"]').first,
        editor_container.locator('[class*="placeholder"]').first,
    ]
    for c in cands:
        try:
            if c.count() > 0:
                return c
        except Exception:
            continue
    return editor_container.locator('input, div[contenteditable="true"]').first


def _wake_editor(page, editor_container, max_round=4):
    """
    唤醒富文本编辑器（业务动作不变：点击输入区使 ProseMirror 变为可编辑）。
    加固：每轮先清障，点击方式逐级升级，失败时 hit-test 存证。
    返回 real_editor(Locator)。
    """
    target = _resolve_wake_target(editor_container)
    real_editor = editor_container.locator('div[contenteditable="true"].ProseMirror').first
    last_hit = None

    for rnd in range(1, max_round + 1):
        # 第 2 轮起启用物理歼灭
        _dismiss_overlays(page, aggressive=(rnd >= 2), desc=f"wake-r{rnd}")
        try:
            target.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass
        page.wait_for_timeout(200)

        hit = _hit_test(page, target)
        last_hit = hit
        if not hit.get("ok"):
            logger.info(
                f"[Editor/Wakeup] 第{rnd}轮命中测试未通过 | 原因: 【{hit.get('reason')}】 "
                f"| 遮挡者: 【<{hit.get('blockerTag')}> class={hit.get('blockerClass')} "
                f"text='{hit.get('blockerText')}' 覆盖率={hit.get('coverRatio')} "
                f"bodyOverflow={hit.get('bodyOverflow')}】 | 状态: [Blocked]"
            )

        try:
            if rnd == 1:
                target.click(timeout=8000)                       # 原版行为
            elif rnd == 2:
                target.click(timeout=5000, force=True)           # 跳过 hit-test
            elif rnd == 3:
                target.evaluate("""(el) => {
                    el.scrollIntoView({block:'center'});
                    const o = {bubbles:true, cancelable:true, view:window};
                    try { el.dispatchEvent(new PointerEvent('pointerdown', o)); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mousedown', o)); } catch(e) {}
                    if (el.focus) el.focus();
                    try { el.dispatchEvent(new PointerEvent('pointerup', o)); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('mouseup', o)); } catch(e) {}
                    try { el.dispatchEvent(new MouseEvent('click', o)); } catch(e) {}
                }""")
            else:
                box = target.bounding_box()
                if box:
                    page.mouse.click(box["x"] + box["width"] / 2,
                                     box["y"] + min(box["height"] / 2, 20))
                page.keyboard.press("Tab")
        except Exception as e:
            logger.info(f"[Editor/Wakeup] 第{rnd}轮点击异常 | 详情: 【{str(e)[:150]}】 | 状态: [Retry]")

        # 校验：ProseMirror 可编辑（原版判据）+ 焦点确实落在编辑器内
        try:
            expect(real_editor).to_be_editable(timeout=8000 if rnd == 1 else 4000)
            focused = page.evaluate("""() => {
                const a = document.activeElement;
                return !!(a && (a.isContentEditable ||
                                (a.classList && a.classList.contains('ProseMirror')) ||
                                (a.closest && a.closest('.ProseMirror'))));
            }""")
            if not focused:
                try:
                    real_editor.evaluate("el => el.focus()")
                except Exception:
                    pass
            logger.info(f"[Editor/Wakeup] 唤醒成功 | 状态: [可编辑] | 轮次: <{rnd}> | 焦点在编辑器: 【{focused}】")
            return real_editor
        except Exception:
            logger.info(f"[Editor/Wakeup] 第{rnd}轮唤醒未生效，升级策略重试 | 状态: [Retry]")

    _forensics(page, "wake_fail", {"last_hit_test": last_hit})
    raise Exception(f"编辑器唤醒失败（4级降级全部失效）。最后命中测试: {last_hit}")


# ==============================================================================
#        步骤4 辅助：光标绝对置顶（业务意图不变，仅修 stale handle + PM 同步）
# ==============================================================================

_CARET_TO_HEAD_JS = r"""
(element) => {
    // 与原版完全一致的 Range 置顶逻辑
    element.focus();
    if (typeof window.getSelection !== "undefined" && typeof document.createRange !== "undefined") {
        const range = document.createRange();
        range.selectNodeContents(element);
        range.collapse(true);          // true = 折叠到头部
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
    }
    // 回报当前光标是否真的在绝对头部，用于日志校验
    try {
        const sel = window.getSelection();
        if (!sel || sel.rangeCount === 0) return {atHead:false, reason:'no-range'};
        const probe = document.createRange();
        probe.selectNodeContents(element);
        probe.setEnd(sel.getRangeAt(0).startContainer, sel.getRangeAt(0).startOffset);
        return {atHead: probe.toString().length === 0, offsetChars: probe.toString().length};
    } catch (e) {
        return {atHead:null, reason:String(e)};
    }
}
"""


def _force_caret_to_head(page, real_editor, tag=""):
    """
    业务目标不变：把光标锁定到编辑器文本的绝对头部（保证正文顶在超链接之前）。
    加固点：
      1) 用 locator.evaluate 替代 page.evaluate(element_handle())，避免框架重渲染后 handle 变 stale；
      2) Range 置顶后追加 Ctrl+Home，让 ProseMirror 内部 state.selection 与浏览器 selection 同步；
      3) JS 回报偏移量，日志可核验是否真的在头部。
    """
    logger.info(f"[Editor/Focus] 锁定光标到文本绝对头部 | 场景: <{tag}> | 结果: [执行中]")
    res = None
    try:
        res = real_editor.evaluate(_CARET_TO_HEAD_JS)
    except Exception as e:
        logger.info(f"[Editor/Focus] Range 置顶执行异常，降级键盘方案 | 详情: 【{str(e)[:120]}】")

    # 让 ProseMirror 走自己的 selection 通道再确认一次（结果与 Range 置顶一致：文档起点）
    for combo in ("Control+Home", "Home"):
        try:
            real_editor.press(combo, timeout=3000)
            break
        except Exception:
            continue

    page.wait_for_timeout(300)   # 与原版一致的 300ms 稳定等待

    try:
        verify = real_editor.evaluate("""(element) => {
            const sel = window.getSelection();
            if (!sel || sel.rangeCount === 0) return {atHead:false};
            const probe = document.createRange();
            probe.selectNodeContents(element);
            probe.setEnd(sel.getRangeAt(0).startContainer, sel.getRangeAt(0).startOffset);
            return {atHead: probe.toString().length === 0, offsetChars: probe.toString().length};
        }""")
        logger.info(f"[Editor/Focus] 光标置顶完成 | 校验: 【在头部={verify.get('atHead')}, "
                    f"距头部字符数={verify.get('offsetChars')}】 | 首次Range回报: 【{res}】 | 结果: [Success]")
    except Exception:
        logger.info(f"[Editor/Focus] 光标置顶完成（校验不可用） | Range回报: 【{res}】 | 结果: [Success]")


# ==============================================================================
#                步骤4 辅助：正文输入（键序与原版一致，仅分块 + 落字校验）
# ==============================================================================

def _type_body(page, real_editor, comment):
    """
    输入正文。
    - 键序、delay 与原版 press_sequentially(comment, delay=60) 完全一致；
    - 仅按 TYPE_CHUNK_SIZE 切分调用，避免超长文本单次 action 撞总超时；
    - 每块后校验是否落字，未落字降级 keyboard.insert_text 补录（不改变最终文本）。
    - NEWLINE_SAFE_MODE=True 时才把 \\n 改为 Shift+Enter（默认 False，保持原行为）。
    """
    if NEWLINE_SAFE_MODE and "\n" in comment:
        segments = comment.split("\n")
        for li, line in enumerate(segments):
            _press_chunks(page, real_editor, line)
            if li < len(segments) - 1:
                try:
                    real_editor.press("Shift+Enter", timeout=3000)
                except Exception:
                    page.keyboard.press("Shift+Enter")
        return

    _press_chunks(page, real_editor, comment)


def _press_chunks(page, real_editor, text):
    if not text:
        return
    for i in range(0, len(text), TYPE_CHUNK_SIZE):
        seg = text[i:i + TYPE_CHUNK_SIZE]
        try:
            before = len(real_editor.inner_text() or "")
        except Exception:
            before = -1
        try:
            real_editor.press_sequentially(seg, delay=TYPE_DELAY_MS, timeout=60000)
        except AttributeError:
            # 兼容旧版 Playwright（type 已废弃但仍可用）
            real_editor.type(seg, delay=TYPE_DELAY_MS, timeout=60000)
        except Exception as e:
            logger.info(f"[Editor/Text] 分块输入异常 | 片段序号: <{i // TYPE_CHUNK_SIZE}> "
                        f"| 详情: 【{str(e)[:120]}】 | 动作: [Fallback]")
        if before >= 0:
            try:
                after = len(real_editor.inner_text() or "")
            except Exception:
                after = before
            if after <= before:
                logger.info(f"[Editor/Text] 分块未落字，降级 insert_text 补录 | 片段长度: <{len(seg)}> | 动作: [Fallback]")
                try:
                    page.keyboard.insert_text(seg)
                except Exception:
                    pass


# ==============================================================================
#                      步骤5 辅助：发送按钮解析 + API 监听器
# ==============================================================================

def _pick_trusted_send_button(cands):
    """
    保持原版候选与顺序，仅追加黑名单过滤：
    页面底部存在「立即回复」这类跳转按钮，误点会导致正文根本没提交。
    """
    for c in cands:
        try:
            if c.count() == 0:
                continue
            try:
                txt = (c.inner_text(timeout=1500) or "").strip()
            except Exception:
                txt = ""
            if txt and RE_SEND_BLACKLIST.search(txt):
                logger.info(f"[Editor/Submit] 候选按钮命中黑名单，跳过 | 文案: <{txt[:20]}> | 结果: [Skipped]")
                continue
            return c
        except Exception:
            continue
    return None


# 主判据：与原版完全一致，只认 pgc/content/add + POST
RE_SUBMIT_API_PRIMARY = re.compile(r"pgc/content/add", re.IGNORECASE)
# 辅助观测：仅用于"主接口没抓到时"的成功识别与日志，不用于抛业务异常
RE_SUBMIT_API_SECONDARY = re.compile(
    r"(content/comment|comment/add|comment/create|/reply|square/.*(publish|post/add))", re.IGNORECASE
)


class _ApiWatcher:
    """
    点击前挂监听，点击后轮询取结果。
    相较 expect_response 的优势：响应早于监听建立 / 多次请求 / 点击本身抛异常时都不会漏。
    判据与原版一致。
    """

    def __init__(self, page):
        self.page = page
        self.primary = []
        self.secondary = []
        self._closed = False
        self.page.on("response", self._on_response)

    def _on_response(self, resp):
        try:
            if resp.request.method != "POST":
                return
            url = resp.url or ""
            if RE_SUBMIT_API_PRIMARY.search(url):
                self.primary.append(resp)
            elif RE_SUBMIT_API_SECONDARY.search(url):
                self.secondary.append(resp)
        except Exception:
            pass

    def wait_primary(self, timeout_ms):
        end = time.time() + timeout_ms / 1000.0
        while time.time() < end:
            if self.primary:
                return self.primary[-1]
            self.page.wait_for_timeout(200)
        return None

    def latest_secondary(self):
        return self.secondary[-1] if self.secondary else None

    def close(self):
        if self._closed:
            return
        self._closed = True
        try:
            self.page.remove_listener("response", self._on_response)
        except Exception:
            pass


def _parse_api_json(resp):
    """兼容 502 等返回 HTML 而非 JSON 的场景（原版逻辑保留）。"""
    try:
        raw = resp.json()
        return raw if isinstance(raw, dict) else {"raw": str(raw)}
    except Exception:
        return {}


# ==============================================================================
#                                核心：提交评论
# ==============================================================================

def _submit_comment(page, editor_container, comment, image_path=None, url_info_list=None):
    """
    在隔离的局部作用域内完成发帖全链路：
    唤醒编辑器 -> 图片 -> 超链接 -> 光标置顶 -> 正文 -> 发送校验。
    ⚠️ 业务顺序与原版完全一致（超链接先注入垫底，随后光标置顶，正文顶在超链接之前）。
    入参 url_info_list 形貌: [{"text": str, "url": str}, ...]。
    返回: 成功时的评论ID(str) 或 None。
    """
    comment = str(comment) if comment else ""
    comment_id = None

    # ---- 步骤 0：进场清障（新增：浮层免疫，不改变任何业务动作）----
    _dismiss_overlays(page, aggressive=False, desc="pre-submit")
    report_guard_hits(page, "pre-submit")

    # ---- 步骤 1：唤醒富文本编辑器 ----
    logger.info("[Editor/Wakeup] 尝试唤醒富文本框 | 目标: <div.ProseMirror> | 结果: [执行中]")
    real_editor = _wake_editor(page, editor_container)

    # ---- 步骤 2：注入图片（失败则降级为纯文本，不中断）----
    if image_path and os.path.exists(image_path):
        logger.info(f"[Editor/Image] 开始上传图片 | 路径: <{image_path}> | 结果: [执行中]")
        try:
            editor_container.locator('input[type="file"]').first.set_input_files(
                image_path, timeout=15000)
            # 优先等缩略图真正出现；等不到再退回原版的固定 3.5s
            mounted = False
            try:
                expect(
                    editor_container.locator(
                        "img[src^='blob'],img[src^='http'],img[src^='data:'],"
                        "[class*='thumb'],[class*='preview'] img"
                    ).first
                ).to_be_visible(timeout=12000)
                mounted = True
            except Exception:
                page.wait_for_timeout(3500)
            logger.info(f"[Editor/Image] 图片挂载完毕 | 缩略图可见: 【{mounted}】 | 状态: [Success]")
        except Exception as e:
            logger.info(f"[Editor/Image] 图片上传失败，自动降级为纯文本 | "
                        f"可能原因: 【文件损坏或上传控件不可用: {str(e)[:150]}】 | 结果: [Warning]")

    # ---- 步骤 3：注入超链接（保持原设计：先注入，让超链接垫底）----
    if isinstance(url_info_list, list) and url_info_list:
        logger.info(f"[Editor/Link] 检测到超链接任务 | 数量: <{len(url_info_list)}> | 结果: [启动注入流]")
        for idx, url_info in enumerate(url_info_list):
            if not isinstance(url_info, dict):
                continue
            link_text = str(url_info.get("text", "")).strip()
            link_url = str(url_info.get("url", "")).strip()
            if not link_text or not link_url:
                continue
            if not re.match(r"^https?://", link_url, re.IGNORECASE):
                link_url = "https://" + link_url
            # 新增：链接注入弹窗常伴浮层，注入前做一次轻量清障（不改变注入逻辑本身）
            _dismiss_overlays(page, aggressive=False, desc=f"pre-link-{idx}")
            _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx)

    # ---- 核心保留：强制光标往前（置顶），防止在超链接后输入 ----
    _force_caret_to_head(page, real_editor, tag="before-body")

    # ---- 步骤 4：注入正文（光标已前置，输入的文本会顶在超链接前面）----
    if comment.strip():
        logger.info(f"[Editor/Text] 填入正文内容 | 长度: <{len(comment)}> | 结果: [输入中]")
        # ⚠️ 与原版一致：此处不做 real_editor.click()，避免光标跳回已注入的超链接中。
        page.wait_for_timeout(800)
        _type_body(page, real_editor, comment)
        page.wait_for_timeout(500)

        try:
            current_text = (real_editor.inner_text() or "").strip()
        except Exception:
            current_text = ""

        if not current_text:   # 防前端框架拦截导致静默清空
            logger.info("[Editor/Text] 检测到文本被静默清空，触发重试补录 | 动作: [Retry]")
            _dismiss_overlays(page, aggressive=True, desc="text-retry")
            try:
                expect(real_editor).to_be_editable(timeout=4000)
            except Exception:
                real_editor = _wake_editor(page, editor_container, max_round=2)
            # 重试前再次保证光标绝对置顶（与原版一致）
            _force_caret_to_head(page, real_editor, tag="text-retry")
            _type_body(page, real_editor, comment)
            page.wait_for_timeout(500)

        logger.info("[Editor/Text] 文本输入完成 | 状态: [Success]")

    # ---- 步骤 5：发送 + API 监听校验 ----
    logger.info("[Editor/Submit] 定位发送按钮 | 结果: [执行中]")
    _dismiss_overlays(page, aggressive=False, desc="pre-send")

    send_btn_cands = [
        editor_container.locator("button").filter(has_text=RE_SEND_EXACT).first,
        editor_container.get_by_role("button", name=RE_SEND).first,
    ]
    send_button = None
    try:
        trusted = _pick_trusted_send_button(send_btn_cands)
        if trusted is not None:
            send_button = trusted
            expect(send_button).to_be_enabled(timeout=10000)
        else:
            send_button = _interact_fallback_locators(
                send_btn_cands, action="wait", timeout=10000, desc="发送按钮")
            expect(send_button).to_be_enabled(timeout=10000)
    except Exception as e:
        logger.info(f"[Editor/Submit] 发送按钮定位/状态异常，强制回退首选候选 | "
                    f"可能原因: 【按钮未渲染或处于禁用态: {str(e)[:150]}】 | 状态: [Warning]")
        send_button = send_btn_cands[0]

    # 记录发送前编辑器快照，供 DOM 兜底比对（原版逻辑）
    text_before, media_before = _snapshot_editor(real_editor)
    api_success = False

    logger.info("[Editor/Submit] 触发发送并挂载 API 监听器 | 接口特征: <pgc/content/add> | 结果: [执行中]")
    watcher = _ApiWatcher(page)
    try:
        # ---- 点击：3 级降级（业务动作仍是"点一次发送"）----
        clicked = False
        for attempt in range(3):
            hit = _hit_test(page, send_button)
            if not hit.get("ok"):
                logger.info(f"[Editor/Submit] 发送按钮被遮挡/不可命中 | 原因: 【{hit.get('reason')}】 "
                            f"| 遮挡者: 【<{hit.get('blockerTag')}> text='{hit.get('blockerText')}'】 | 动作: [清障重试]")
                _dismiss_overlays(page, aggressive=(attempt >= 1), desc=f"send-blocked-{attempt}")
            try:
                if attempt == 0:
                    _robust_click(send_button)          # 原版首选点击方式
                elif attempt == 1:
                    send_button.click(timeout=5000, force=True)
                else:
                    send_button.evaluate("el => el.click()")
                clicked = True
                break
            except Exception as ce:
                logger.info(f"[Editor/Submit] 第{attempt + 1}次点击失败，升级重试 | 详情: 【{str(ce)[:150]}】")
        if not clicked:
            _forensics(page, "send_click_fail", {"hit": _hit_test(page, send_button)})
            raise Exception("发送按钮点击 3 级降级全部失败（疑似被浮层持续拦截或按钮已失效）。")

        # ---- 等待主接口响应（判据与原版一致）----
        resp = watcher.wait_primary(API_WAIT_TIMEOUT_MS)
        if resp is None:
            logger.info("[Editor/Verify] API 监听超时，降级启用 DOM 状态机校验 | 状态: [Warning]")
            sec = watcher.latest_secondary()
            if sec is not None:
                # 仅作观测/成功识别，绝不据此抛业务异常（保持原版打击面）
                sec_json = _parse_api_json(sec)
                logger.info(f"[Editor/Verify] 观测到疑似评论接口（非主判据） | URL: <{sec.url}> "
                            f"| HTTP: 【{sec.status}】 | body: 【{str(sec_json)[:200]}】")
                if str(sec_json.get("code", "")) == "000000" or sec_json.get("success") is True:
                    api_success = True
                    d = sec_json.get("data")
                    if isinstance(d, dict):
                        comment_id = d.get("id") or d.get("commentId") or d.get("contentId")
                    logger.info(f"[Editor/Verify] 辅助接口判定发送成功 | 评论ID: 【{comment_id}】 | 结果: [Success]")
        else:
            json_data = _parse_api_json(resp)
            if str(json_data.get("code", "")) == "000000" or json_data.get("success") is True:
                api_success = True
                data = json_data.get("data")
                comment_id = data.get("id") if isinstance(data, dict) else None
                logger.info(f"[Editor/Verify] 底层接口校验通过 | 响应码: 【000000】 "
                            f"| 评论ID: 【{comment_id}】 | 结果: [Success]")
            elif json_data:
                err_msg = json_data.get("message", "未知业务拦截")
                logger.info(f"[Editor/Verify] 底层接口拒绝请求 | 原因: 【{err_msg}】 | 结果: [Failed]")
                _forensics(page, "biz_reject", {"url": resp.url, "status": resp.status, "body": json_data})
                raise BusinessErrorException(f"业务发送被服务器拦截，原因: {err_msg}")
            else:
                logger.info(f"[Editor/Verify] 主接口响应非 JSON（HTTP {resp.status}），降级 DOM 校验 | 状态: [Warning]")

    except PlaywrightTimeoutError:
        logger.info("[Editor/Verify] API 监听超时，降级启用 DOM 状态机校验 | 状态: [Warning]")
    finally:
        watcher.close()

    # ---- 步骤 6：DOM 兜底校验（编辑器被大幅清空即视为发送成功）----
    if not api_success:
        page.wait_for_timeout(3000)
        text_after, media_after = _snapshot_editor(real_editor)
        text_cleared = text_before > 0 and text_after < (text_before / 3)
        media_cleared = media_before > 0 and media_after < media_before

        if text_cleared or media_cleared:
            logger.info("[Editor/Verify] DOM 兜底比对通过 | 现象: 【输入框已被大幅清空】 | 结果: [Success]")
        else:
            report_guard_hits(page, "send-fail")
            _forensics(page, "send_no_effect", {
                "text_before": text_before, "text_after": text_after,
                "media_before": media_before, "media_after": media_after,
                "send_btn_hit_test": _hit_test(page, send_button),
            })
            raise Exception("发送已执行但输入框未清空且 API 无成功响应，疑似发送按钮失效或网络堵塞。")

    return comment_id


# ==============================================================================
#                                  URL / 帖子ID
# ==============================================================================

def extract_binance_post_id(url):
    if not url or not isinstance(url, str):
        return None
    try:
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        # 放宽到 binance 各域/镜像（原版只认 www.binance.com / binance.com）
        if not re.search(r"(^|\.)binance\.(com|info|me)$", host):
            return None

        # 兼容 /square/post/123、/zh-CN/square/post/123、/square/post/history/123
        match = re.search(r'/square/post/(?:[a-zA-Z\-]+/)?(\d+)', parsed.path)
        if not match:
            match = re.search(r'/post/(\d+)', parsed.path)
        return match.group(1) if match else None
    except Exception:
        return None


def _fallback_post_id(post_url):
    """URL 解析兜底：只取纯数字段，避免把 slug / 语言前缀当作 ID。"""
    try:
        m = re.search(r"/post/(?:[a-zA-Z\-]+/)?(\d+)", str(post_url))
        if m:
            return m.group(1)
        tail = str(post_url).split('?')[0].strip('/').split('/')[-1]
        return tail if tail.isdigit() else None
    except Exception:
        return None


# ==============================================================================
#                                  主控入口
# ==============================================================================

def comment_on_binance_post(post_url, comment, image_path=None, user_data_dir=USER_DATA_DIR,
                            url_info_list=None, debug=True):
    """
    主控入口：调度浏览器加载帖子并执行评论全流程。
    返回: Tuple(错误信息(str|None), 是否成功(bool), 评论ID(str|None))。
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    logger.info(f"\n{'=' * 60}")
    logger.info(f"[Main/Task] 启动自动化发帖任务 | 目标URL: <{post_url}> | 结果: [初始化]")
    logger.info(f"{'=' * 60}")

    # 帖子ID：统一用严谨解析（后续不再被覆盖）
    post_id = extract_binance_post_id(post_url) or _fallback_post_id(post_url)

    try:
        with sync_playwright() as p:
            context = None
            try:
                if not debug:
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
                            '--force-device-scale-factor=1',   # 新增：防离屏窗口 DPI 漂移致坐标点击偏移
                            '--hide-scrollbars',
                        ],
                        ignore_default_args=["--enable-automation"]
                    )
                else:
                    context = p.chromium.launch_persistent_context(
                        channel="chrome",
                        user_data_dir=user_data_dir,
                        headless=False,
                        args=[
                            '--disable-blink-features=AutomationControlled',
                            '--start-maximized',
                            '--disable-gpu',
                            '--window-position=0,0'
                        ],
                        ignore_default_args=["--enable-automation"]
                    )

                context.set_default_timeout(60000)
                context.set_default_navigation_timeout(60000)
                page = context.pages[0] if context.pages else context.new_page()

                # 🚀 关键新增：必须在 goto 之前武装浮层守卫（init_script 只对之后的导航生效）
                install_overlay_guard(page)

                logger.info("[Main/Nav] 导航至目标页面 | 动作: [等待 DOM 加载]")
                response = page.goto(post_url, timeout=60000)
                page.wait_for_load_state("domcontentloaded", timeout=60000)

                # 落地后立即清障（Cookie 层 + 引导层），再做任何 UI 探测
                _dismiss_overlays(page, aggressive=False, desc="post-nav")
                report_guard_hits(page, "post-nav")

                # 1. 拦截标准 HTTP 错误
                if response and response.status >= 400:
                    error_info = f"页面加载异常或帖子已被删除 (HTTP 状态码: {response.status})"
                    logger.info(f"[Main/Nav] 嗅探到无效页面 | 原因: 【{error_info}】 | 结果: [Failed]")
                    return error_info, False, None

                # 2. 拦截 301/302 导致的 URL 漂移（判定逻辑保留；仅加一次 SPA 重试 + 编辑器复核，避免误杀）
                if post_id:
                    current_url_decoded = urllib.parse.unquote(page.url)
                    if post_id not in current_url_decoded:
                        page.wait_for_timeout(2000)   # 给 SPA 的 replaceState 一点滞后余量
                        current_url_decoded = urllib.parse.unquote(page.url)
                    if post_id not in current_url_decoded:
                        editor_alive = False
                        try:
                            page.locator(
                                "div[contenteditable='true'].ProseMirror,"
                                "[placeholder*='回复'],[placeholder*='评论']"
                            ).first.wait_for(state="attached", timeout=5000)
                            editor_alive = True
                        except Exception:
                            pass
                        if not editor_alive:
                            error_info = (f"页面发生重定向，目标帖子已被删除或失效 | 原帖子ID: {post_id} "
                                          f"| 现落地URL: {current_url_decoded}")
                            logger.info(f"[Main/Nav] 拦截到重定向漂移 | 原因: 【{error_info}】 | 结果: [Failed]")
                            return error_info, False, None
                        logger.info(f"[Main/Nav] URL 漂移但评论编辑器存在，判定为前端路由行为，放行 "
                                    f"| 落地URL: <{current_url_decoded}> | 结果: [Pass]")

                # 登录态嗅探：出现 login 链接即判定 Cookie 过期
                try:
                    page.locator("a[href*='login']").first.wait_for(state="visible", timeout=3000)
                    return "页面探测到 Login 按钮，本地 Cookie 可能已过期失效。", False, None
                except PlaywrightTimeoutError:
                    pass

                # 软拦截（拉黑 / 权限）嗅探
                try:
                    block_notice = page.locator("text=您无法查看此内容")
                    block_notice.wait_for(state="visible", timeout=2500)
                    error_info = "触发平台软拦截（账号已被该创作者拉黑或设置了权限），安全跳过当前任务。"
                    logger.info(f"[Main/Nav] 嗅探到拉黑阻断 | 原因: 【{error_info}】 | 结果: [Skip]")
                    return error_info, False, None
                except PlaywrightTimeoutError:
                    pass

                check_for_crash(page)

                # 定位编辑器前再清一次障：引导浮层会给 body 加 scroll-lock，导致滚动指令失效
                _dismiss_overlays(page, aggressive=False, desc="pre-scroll")
                try:
                    editor_container = _smart_scroll_to_editor(page)
                except PlaywrightTimeoutError:
                    logger.info("[Main/Nav] 编辑器定位超时，疑似 scroll-lock 未解除，启用强制清障后重试 | 动作: [Retry]")
                    _dismiss_overlays(page, aggressive=True, desc="scroll-lock-retry")
                    page.wait_for_timeout(500)
                    editor_container = _smart_scroll_to_editor(page)

                check_for_crash(page)

                comment_id = _submit_comment(page, editor_container, comment, image_path, url_info_list)
                return None, True, comment_id

            except BusinessErrorException as biz_e:
                error_info = f"[业务拦截] {str(biz_e)}"
                logger.info(f"[Main/Task] 发帖被服务端业务规则阻断 | 原因: 【{error_info}】 | 结果: [Failed]")
                return error_info, False, None

            except PlaywrightTimeoutError as pt_e:
                error_info = f"[元素/网络超时] {str(pt_e)}"
                logger.info(f"[Main/Task] 元素等待或网络请求超时 | 可能原因: 【引导浮层拦截/帖子软删除/页面卡顿/网络抖动】 "
                            f"| 详情: 【{error_info[:200]}...】 | 结果: [Failed]")

                if context and context.pages:
                    try:
                        pg = context.pages[0]
                        report_guard_hits(pg, "timeout")
                        ts = int(time.time())
                        pg.screenshot(path=f"timeout_screenshot_{ts}.png")
                        with open(f"timeout_html_{ts}_{post_id}.html", "w", encoding="utf-8") as f:
                            f.write(pg.content())
                        # 附带遮挡诊断，便于直接看出"是谁挡住了"
                        try:
                            diag = pg.evaluate("""() => {
                                const cx = innerWidth/2, cy = innerHeight/2;
                                const top = document.elementFromPoint(cx, cy);
                                return {
                                    bodyOverflow: getComputedStyle(document.body).overflow,
                                    centerTag: top ? top.tagName : null,
                                    centerClass: top ? (top.className||'').toString().slice(0,200) : null,
                                    centerText: top ? (top.innerText||'').replace(/\\s+/g,' ').slice(0,200) : null,
                                    guardHits: window.__bnGuardHits || 0
                                };
                            }""")
                            with open(f"timeout_diag_{ts}_{post_id}.json", "w", encoding="utf-8") as f:
                                json.dump(diag, f, ensure_ascii=False, indent=2)
                            logger.info(f"[Main/Debug] 遮挡诊断: 【{diag}】")
                        except Exception:
                            pass
                        logger.info(f"[Main/Debug] 超时现场已保留 | 产物时间戳: 【{ts}】 | 结果: [Saved]")
                    except Exception as s_e:
                        logger.info(f"[Main/Debug] 超时现场保留失败 | 可能原因: 【磁盘不可写或页面已销毁: {s_e}】")

                human_intervention_pause(error_info)
                return error_info, False, None

            except Exception as e:
                error_info = f"[{type(e).__name__}] {str(e)}\n[Traceback]:\n{traceback.format_exc()}"
                logger.info(f"[Main/Task] 执行过程发生未预期异常 | 摘要: 【{str(e)[:200]}...】 | 结果: [Failed]")

                if context and context.pages:
                    try:
                        pg = context.pages[0]
                        report_guard_hits(pg, "error")
                        ts = int(time.time())
                        pg.screenshot(path=f"error_screenshot_{ts}.png")
                        with open(f"error_html_{ts}_{post_id}.html", "w", encoding="utf-8") as f:
                            f.write(pg.content())
                        logger.info(f"[Main/Debug] 故障现场已保留 | 产物时间戳: 【{ts}】 | 结果: [Saved]")
                    except Exception as s_e:
                        logger.info(f"[Main/Debug] 现场保留失败 | 可能原因: 【磁盘不可写或页面已销毁: {s_e}】")

                human_intervention_pause(error_info)
                return error_info, False, None

            finally:
                if context:
                    try:
                        context.close()
                    except Exception:
                        pass

    except Exception as core_e:
        error_info = (f"[CoreEngineCrash] Playwright 底层启动/运行发生系统级崩溃:\n{core_e}\n\n"
                      f"[Traceback]:\n{traceback.format_exc()}")
        logger.info("[Main/Task] Playwright 核心框架崩溃，无法启动浏览器引擎 | 结果: [Failed]")
        return error_info, False, None

def get_auth_tokens_robust(user_data_dir):
    if not os.path.exists(user_data_dir):
        logger.info(f"[Auth/Extract] 环境不存在，终止提取 | 目录: <{user_data_dir}>")
        return None, None

    visit_url = "https://www.binance.com/zh-CN/square/profile/insights_anchor"
    target_api_keyword = "pgc/user/client"
    logger.info(f"[Auth/Extract] 启动浏览器提取凭证 (Headed模式) | 拦截目标: <{target_api_keyword}>")

    with sync_playwright() as p:
        context = None
        try:
            context = p.chromium.launch_persistent_context(
                channel="chrome",
                user_data_dir=user_data_dir,
                headless=False,  # 必须保持 False
                viewport={'width': 1280, 'height': 720},
                args=['--disable-blink-features=AutomationControlled']
            )
            page = context.pages[0] if context.pages else context.new_page()

            logger.info("[Auth/Extract] 正在等待页面加载及目标接口调用...")

            # 【核心修改 1】：过滤 OPTIONS 请求，只抓 GET 或 POST 真实请求
            with page.expect_request(
                    lambda req: target_api_keyword in req.url and req.method != "OPTIONS",
                    timeout=20000
            ) as first_req_info:
                page.goto(visit_url, wait_until="networkidle")

            first_req = first_req_info.value
            headers = first_req.headers

            # 打印调试信息，看看到底抓到了什么
            logger.info(f"--- 调试信息 ---")
            logger.info(f"拦截到请求: {first_req.method} {first_req.url}")
            logger.info(f"请求头 Keys: {list(headers.keys())}")
            logger.info(f"----------------")

            # Playwright 获取的 headers key 默认全是小写
            extracted_csrf = headers.get("csrftoken")
            extracted_cookie = headers.get("cookie")

            # 【核心修改 2】：如果 Headers 里没取到，尝试用全局 Cookie 兜底拼装
            if not extracted_cookie:
                logger.info("[Auth/Extract] Request headers 中无 Cookie，尝试从浏览器上下文中提取指定域名 Cookie...")

                # 修复核心点：强制指定域名 URL，防止获取到 Google/YouTube 等全站庞大 Cookie 导致 400 报错
                binance_url = "https://www.binance.com"
                raw_cookies = context.cookies(binance_url)

                # 拼接指定域名的干净 Cookie
                extracted_cookie = "; ".join(f"{c['name']}={c['value']}" for c in raw_cookies)

            if extracted_cookie:
                has_p20t = "p20t=" in extracted_cookie

                # 清理一下两端可能的空白符或换行符，增加健壮性
                extracted_cookie = extracted_cookie.strip()
                if extracted_csrf:
                    extracted_csrf = extracted_csrf.strip()

                logger.info(
                    f"[Auth/Extract] 提取完成 | CSRF: 【{str(extracted_csrf)[:8]}...】 | Cookie长度: {len(extracted_cookie)} | 包含p20t: {has_p20t}")

                if has_p20t and extracted_csrf:
                    return extracted_cookie, extracted_csrf
                else:
                    logger.info("[Auth/Extract] 警告: 提取到了Cookie，但可能缺失核心 p20t 或 CSRF。")
                    return extracted_cookie, extracted_csrf

            logger.info(f"[Auth/Extract] 提取失败 | 捕获到的请求未携带合法凭据")
            return None, None

        except PlaywrightTimeoutError:
            logger.info(f"[Auth/Extract] 提取失败 | 原因: 超时未捕获到目标接口。请检查浏览器打开时是否处于登录状态！")
            return None, None
        except Exception as e:
            logger.info(f"[Auth/Extract] 提取失败 | 原因: 【未知异常: {e}】")
            return None, None
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass

def open_browser_for_manual_use(user_data_dir, home_url='https://www.binance.com/zh-CN'):
    """启动可见浏览器交由人工自由操作，关闭窗口后自动收回控制权并释放资源。"""
    logger.info(f"\n{'=' * 50}\n[System/Manual] 启动本地浏览器进行人工接管 | 目录: <{user_data_dir}>\n{'=' * 50}")
    with sync_playwright() as p:
        context = None
        try:
            # 修改点 1：在 args 列表中追加 '--window-position=0,0' 以覆盖之前屏幕外的历史坐标缓存
            context = p.chromium.launch_persistent_context(
                channel="chrome", user_data_dir=user_data_dir, headless=False,
                args=['--disable-blink-features=AutomationControlled', '--start-maximized', '--window-position=0,0'],
                ignore_default_args=["--enable-automation"]
            )
            page = context.pages[0] if context.pages else context.new_page()

            # 修改点 2：显式将该页面/窗口唤醒至操作系统最前端（正面显示）
            page.bring_to_front()

            page.goto(home_url)

            logger.info("\n[System/Manual] ✅ 浏览器已就绪，控制权已交接。")
            logger.info("[System/Manual] 🛑 退出方式: 直接关闭浏览器窗口，程序将自动结束。")
            page.wait_for_event("close", timeout=0)
        except Exception as e:
            logger.info(f"\n[System/Manual] 浏览器运行异常 | 可能原因: 【环境损坏或被手动强制杀死: {e}】")
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass
            logger.info("[System/Manual] 👋 窗口已关闭，控制权收回，系统资源已释放。\n")



# ==============================================================================
# 启动入口
# ==============================================================================
if __name__ == '__main__':
    # # # 其他可选入口（按需取消注释）:
    # my_cookies, csrf_token = get_auth_tokens_robust(USER_DATA_DIR)   # 提取脱机 API 凭证
    # my_cookies = """bnc-uuid=f53197d0-e2ad-43bb-b43b-5161cb030a4b; se_gd=1MQFVVQINTKFgcFBQUg4gZZA1CBsDBTVlFXVdVUNlVSUQCVNWWVW1; se_gsd=cDM2ChFxMCknMCstJDI1Uy40BRUNBwpSVl5LUVZXW1VUAlNT1; BNC_FV_KEY=3351d68efd68acd5a71a1ff808d967f0baf26f5a; OptanonAlertBoxClosed=2026-07-21T08:29:53.202Z; r20t=web.1243072957.8E5BB294B939C6C157F67D64CFED42D4; r30t=1; cr00=86336EFE58AD741731FAB162CA606CF1; d1og=web.1243072957.B7AE0C43C84AAEA96A2F1884ACFB1369; r2o1=web.1243072957.1D2CF44FE2130BFEB88A3CD6F538992C; f30l=web.1243072957.42627429D66FA9273C83285996BC3F96; currentAccount=; logined=y; BNC-Location=CN; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221243072957%22%2C%22first_id%22%3A%2219f83cb6c4f1c50-08e14edf337b1e8-26071951-921600-19f83cb6c50290e%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTlmODNjYjZjNGYxYzUwLTA4ZTE0ZWRmMzM3YjFlOC0yNjA3MTk1MS05MjE2MDAtMTlmODNjYjZjNTAyOTBlIiwiJGlkZW50aXR5X2xvZ2luX2lkIjoiMTI0MzA3Mjk1NyJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%24identity_login_id%22%2C%22value%22%3A%221243072957%22%7D%7D; userPreferredCurrency=USD_USD; _gcl_au=1.1.755273243.1784622841; _uetvid=ee84368084de11f1bab6f3dd0938120c; aws-waf-token=6957a2c3-542a-4819-8ca4-129a00800b51:AQoAfOpYIn4FAAAA:LMuuHAwfjEqQ5yRaGPYQACZ1SynT2a+FLdwhL1KWA2/CDrCkqMtodWZMV5FGeykwQTu+T/e4mYWHRnQ/WkAcZARZxgPia4u32mC3436686vcRd6WUe25FrouHf4zBi33l15+xab1fPWfX0tuytlDXjK5WwlGsqSv57EwejDvAUakb/SrszZD75C0ff+bkVMpSORew43lZ69wmfbP4PELrhHDg9cwHX1wpqRVULYJwyMv5z5eZz+za1lmtGMjND+sTm6TIDpIdvXj; _gid=GA1.2.168006475.1784896831; _ga_3WP50LGEEC=deleted; _ga_3WP50LGEEC=deleted; g_state={"i_l":0,"i_ll":1784930857994,"i_b":"4Jy+KY8TkPWMy29knfYSzbEYQUDgDNehQ12eSiYgTwY","i_e":{"enable_itp_optimization":24},"i_et":1784930857994}; BNC_FV_KEY_T=101-TS2vIZ4AxtwOfheK3b7yo9DXYannOutll1fkjOchOeor7Rriqx6KEoDtOOcNIHqUvgLDU2%2B7RtP5PgBiZEZxTg%3D%3D-1D0GuGjHiIyVSsCJznHMbA%3D%3D-6d; BNC_FV_KEY_EXPIRE=1785025950438; theme=dark; p20t=web.1243072957.14122033BCFA181F144BC572017CF8C7; _ga=GA1.1.2076591487.1784622596; OptanonConsent=isGpcEnabled=0&datestamp=Sun+Jul+26+2026+04%3A50%3A09+GMT%2B0800+(%E4%B8%AD%E5%9B%BD%E6%A0%87%E5%87%86%E6%97%B6%E9%97%B4)&version=202604.2.0&browserGpcFlag=0&isDntEnabled=0&isIABGlobal=false&hosts=&consentId=c6ec02be-dae4-4461-bc5f-344aa724fcd7&interactionCount=1&isAnonUser=1&prevHadToken=0&landingPath=NotLandingPage&groups=C0001%3A1%2CC0003%3A1%2CC0004%3A1%2CC0002%3A1&fclco=&lastConsentTs=1784622593&intType=1&crTime=1784622596175&geolocation=KR%3B11&AwaitingReconsent=false; _ga_3WP50LGEEC=GS2.1.s1785011798$o6$g1$t1785012610$j57$l0$h0"""
    # csrf_token = "8f974eb25ea628f9c9f9bc47dd5bcf8f"
    # is_success = toggle_binance_follow("CfexsWwIVYYbr1N5GJXlVQ", "follow", my_cookies, csrf_token)

    # login_and_save_session()                # 初次手动登录并固化 Session
    # open_browser_for_manual_use(USER_DATA_DIR)  # 人工接管调试

    test_url = "https://www.binance.com/zh-CN/square/post/309692475255842"
    test_msg = "少即是多，慢即是快。同频共振！🚀"
    test_img = r"C:\Users\zxh\Desktop\temp\a6c98436-42f9-4aa9-bab8-.png"
    my_urls = [
        {"text": "带单", "url": "https://www.binance.com/zh-CN/square/post/309692475255842"},
    ]

    err, success, c_id = comment_on_binance_post(
        post_url=test_url, comment=test_msg, image_path=test_img, url_info_list=my_urls,debug=True
    )

    if success:
        logger.info(f"\n[Final/Result] 🎉 ======== 自动评论任务圆满成功 ======== | 评论ID: 【{c_id}】")
    else:
        logger.info(f"\n[Final/Result] ❌ ======== 任务失败 ======== | 最终追溯:\n{err}")