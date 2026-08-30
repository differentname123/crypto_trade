# -*- coding: utf-8 -*-
"""
=========================================================================================
[功能摘要]
    币安广场「全自动评论」RPA：复用本地 Chrome 登录态打开帖子，在评论区一次性注入
    【图片 + 超链接 + 正文】并发送，最终以「提交接口响应」为主判据、「编辑器被清空」为兜底
    判据来确认成败，失败自动落地故障现场。

[输入数据]
    - post_url      : 目标帖子 URL(str)，用于解析 post_id 做删帖/重定向校验
    - comment       : 评论正文(str)，允许多行
    - image_path    : 本地图片物理路径(str|None)，文件缺失或上传失败自动降级为纯文本
    - url_info_list : 超链接清单，Shape: [{"text": <锚文本>, "url": <地址>}, ...]
    - user_data_dir : Chrome 持久化目录，承载 Cookie / CSRF 登录态

[数据流转/交互]
    1. 凭证挂载：launch_persistent_context 复用 User Data 目录 → 免登录浏览器上下文
    2. 浮层免疫：goto 之前武装守卫(init_script 预置引导flag + JS看门狗 + locator_handler)
    3. 落地体检：HTTP 状态 → post_id 是否漂移(重定向/删帖) → 登录态 → 平台软拦截 → 崩溃探测
    4. 作用域隔离：PageDown 步进探底锁定 div.feed-post-editor，之后所有操作只在该容器内进行
    5. 数据注入：唤醒 ProseMirror →[图片]→[逐条超链接(弹窗填写)]→ 光标置顶 →[正文分块键入]
    6. 结果判定：点击发送 → _ApiWatcher 抓 POST `pgc/content/add`；超时则比对编辑器清空程度
    7. 失败留证：_forensics 落地 png / html / json(含视口中心遮挡诊断)，供人工复盘

[输出数据]
    - 返回 Tuple(错误信息|None, 是否成功|bool, 评论ID|None)
    - 副作用：结构化终端日志；失败现场文件 forensic_*.png/.html/.json；浏览器上下文关闭
=========================================================================================
"""
import json
import os
import re
import shutil
import sys
import time
import traceback
from urllib.parse import urlparse, unquote

from playwright.sync_api import sync_playwright, expect, TimeoutError as PlaywrightTimeoutError

from common.common_utils import setup_logger

logger = setup_logger(app_name="biance_playwright")

# ==============================================================================
#                                   运行配置
# ==============================================================================
USER_DATA_DIR = r"W:\temp\biance_jie"
LOGIN_URL = "https://www.binance.com/zh-CN/login"

TYPE_CHUNK_SIZE = 80            # 正文分块长度：仅切分 press_sequentially 调用，键序与延迟不变
TYPE_DELAY_MS = 60              # 逐字键入延迟(ms)，模拟真人输入
API_WAIT_TIMEOUT_MS = 10000     # 发送后等待提交接口响应的上限
GUARD_INTERVAL_MS = 700         # 页面内 JS 看门狗轮询间隔

# ⚠️ 换行安全模式：False(默认，线上行为) —— 正文中的 \n 直接作 Enter 送出；
#    True(可选加固) —— 改用 Shift+Enter，防止"Enter=发送"导致内容截断提前发出。
#    这会改变键序，仅在确认遇到"多行正文被截断发出"时才开启。
NEWLINE_SAFE_MODE = False

# ==============================================================================
#                             文案正则 / 选择器常量
# ==============================================================================
RE_MORE = re.compile(r"更多|More|Options|Expand", re.IGNORECASE)
RE_ADD_LINK = re.compile(r"添加链接|Add link|Insert link", re.IGNORECASE)
RE_CONFIRM = re.compile(r"确认|Confirm|OK|Save|Add", re.IGNORECASE)
RE_SEND = re.compile(r"回复|发送|评论|Reply|Comment|Send|Post", re.IGNORECASE)
RE_SEND_EXACT = re.compile(r"^(回复|发送|评论|Reply|Comment|Send|Post)$", re.IGNORECASE)

# 引导浮层"确认关闭"类按钮：刻意不含「取消/Cancel」，避免误取消业务弹窗
RE_DISMISS = re.compile(
    r"^\s*(好的|好|知道了|我知道了|明白了|明白|了解|开始使用|立即体验|马上体验|下一步|"
    r"完成|跳过|不再提示|不再显示|以后再说|稍后|关闭|"
    r"OK|Okay|Got it|Got It|I see|Understood|Skip|Next|Done|Continue|Close|Dismiss|Later|Maybe later)\s*$",
    re.IGNORECASE,
)

# 发送按钮黑名单：页面底部存在「立即回复」这类"打开编辑器"的跳转按钮，误点会导致正文根本没提交
RE_SEND_BLACKLIST = re.compile(r"(立即回复|去回复|查看|更多|展开|取消|Cancel|View|More)", re.IGNORECASE)

# Cookie 同意（页面内确实存在 OneTrust 隐私偏好中心）
COOKIE_SELECTORS = (
    "#onetrust-accept-btn-handler",
    "#onetrust-close-btn-container button",
    "button:has-text('全部允许')",
    "button:has-text('确认我的选择')",
    "button:has-text('Accept All')",
    "button:has-text('Allow All')",
)

# 编辑器保护白名单：任何清障动作都不许碰到含这些元素的容器
EDITOR_GUARD_SELECTOR = '.ProseMirror,[contenteditable="true"],input[type="file"],textarea'

# 提交接口主判据（与线上一致，只认这一个）；辅助判据仅用于观测与"主接口没抓到"时的成功识别
RE_SUBMIT_API_PRIMARY = re.compile(r"pgc/content/add", re.IGNORECASE)
RE_SUBMIT_API_SECONDARY = re.compile(
    r"(content/comment|comment/add|comment/create|/reply|square/.*(publish|post/add))", re.IGNORECASE
)


class PageCrashedException(Exception):
    """页面崩溃/死机（如内存溢出触发的重新加载）"""


class BusinessErrorException(Exception):
    """发送请求被服务端业务规则拦截"""


# ==============================================================================
#                            浏览器 / 通用底层工具
# ==============================================================================

def _launch_persistent(p, user_data_dir, args, viewport=None, hide_automation=True):
    """统一的持久化上下文启动口，收敛四处重复的 launch 配置（各调用方原有差异逐参保留）。"""
    kwargs = {"channel": "chrome", "user_data_dir": user_data_dir, "headless": False, "args": args}
    if viewport:
        kwargs["viewport"] = viewport
    if hide_automation:
        kwargs["ignore_default_args"] = ["--enable-automation"]
    return p.chromium.launch_persistent_context(**kwargs)


def clean_browser_cache(user_data_dir):
    """清理浏览器冗余缓存目录、保留登录凭证。单项删除失败按原设计静默忽略（尽力而为，不阻断主流程）。"""
    if not os.path.exists(user_data_dir):
        return

    garbage = ("Cache", "Code Cache", "GPUCache", "ShaderCache", "GrShaderCache", "Service Worker", "CacheStorage")
    deleted = 0
    for base in (user_data_dir, os.path.join(user_data_dir, "Default")):
        for name in garbage:
            path = os.path.join(base, name)
            if not os.path.exists(path):
                continue
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    os.remove(path)
                deleted += 1
            except Exception:
                pass
    logger.info(f"[缓存/Clean] 浏览器数据瘦身完成 | 目录: <{user_data_dir}> | 清理冗余项: 【{deleted}】")


def check_for_crash(page):
    """探测页面渲染崩溃：500ms 窗口内出现【重新加载】按钮即判定崩溃。"""
    try:
        page.get_by_role("button", name="重新加载").first.wait_for(state="visible", timeout=500)
    except PlaywrightTimeoutError:
        return
    raise PageCrashedException("页面 DOM 渲染崩溃，检测到【重新加载】按钮")


def _interact_fallback_locators(locators, action="wait", timeout=5000, desc="目标元素"):
    """
    对抗前端结构多变的核心健壮性机制：轮询后备选择器清单，把长阻塞打散为 200ms 时间片，
    避免单一选择器失效造成整体长时间卡顿。action="click" 命中即点击返回，否则命中即返回 locator。
    """
    end_time = time.time() + timeout / 1000.0
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
    raise Exception(f"在 {timeout}ms 内未能 {action} 【{desc}】 | 底层最后错误: {str(last_err)[:100]}")


def _robust_click(locator):
    """三段降级点击：常规 → 强制穿透遮挡 → JS 原生。前两段失败静默降级，末段失败如实抛出。"""
    for attempt in ("normal", "force"):
        try:
            locator.click(timeout=1500, force=(attempt == "force"))
            return
        except Exception:
            continue
    locator.evaluate("node => node.click()")


def _focus_editor_end(page, editor_node):
    """把光标聚焦到富文本末尾，为后续键入 / 唤醒菜单做准备。"""
    try:
        editor_node.click(timeout=2000)
    except Exception:
        pass
    page.keyboard.press("End")
    page.wait_for_timeout(120)


def _snapshot_editor(editor):
    """
    读取编辑器状态快照，用于发送前后比对是否清空。
    返回 (文本字符数, 媒体元素数[img/a])；元素不可见或异常时返回 (0, 0)。
    """
    try:
        if not editor.is_visible():
            return 0, 0
        return len(editor.inner_text().strip()), editor.locator("img, a").count()
    except Exception:
        return 0, 0


# ==============================================================================
#                            存证 / 遮挡命中测试
# ==============================================================================

_PAGE_DIAG_JS = r"""
() => {
    const cx = innerWidth / 2, cy = innerHeight / 2;
    const top = document.elementFromPoint(cx, cy);
    return {
        url: location.href,
        bodyOverflow: getComputedStyle(document.body).overflow,
        centerTag: top ? top.tagName : null,
        centerClass: top ? (top.className || '').toString().slice(0, 200) : null,
        centerText: top ? (top.innerText || '').replace(/\s+/g, ' ').slice(0, 200) : null,
        guardHits: window.__bnGuardHits || 0,
        guardLast: window.__bnGuardLast || ''
    };
}
"""


def _forensics(page, tag, extra=None):
    """
    统一存证：截图 + HTML + JSON（自动附带"视口中心是谁挡住的"诊断与看门狗战果）。
    所有降级 / 失败路径都应调用，杜绝"死无对证"。extra 形貌: 任意可 JSON 序列化的 dict。
    """
    # 统一存证目录名称，可根据需要修改
    save_dir = "forensics_logs"
    try:
        os.makedirs(save_dir, exist_ok=True)
    except Exception:
        pass

    base_name = f"forensic_{tag}_{int(time.time() * 1000)}"
    base_path = os.path.join(save_dir, base_name)

    payload = dict(extra or {})
    try:
        payload["page_diag"] = page.evaluate(_PAGE_DIAG_JS)
    except Exception as e:
        payload["page_diag"] = f"unavailable:{str(e)[:80]}"

    try:
        page.screenshot(path=f"{base_path}.png", full_page=False)
    except Exception:
        pass
    try:
        with open(f"{base_path}.html", "w", encoding="utf-8") as f:
            f.write(page.content())
    except Exception:
        pass
    try:
        with open(f"{base_path}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    except Exception:
        pass

    logger.warning(f"[存证/Forensic] 故障现场已落盘 | 文件前缀: <{base_path}> | 页面诊断: 【{payload.get('page_diag')}】")
    return base_path

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
        coverRatio: +(((br.width * br.height) /
                      (window.innerWidth * window.innerHeight)) || 0).toFixed(3),
        bodyOverflow: window.getComputedStyle(document.body).overflow
    };
}
"""


def _hit_test(page, locator):
    """返回 {ok: bool, reason, blockerTag/blockerText/coverRatio...}：既是判断依据，也是最有价值的排查日志。"""
    try:
        return locator.evaluate(_HIT_TEST_JS)
    except Exception as e:
        return {"ok": False, "reason": f"eval-error:{str(e)[:120]}"}


def _fmt_hit(hit):
    """把命中测试结果压成一行人话，供日志直接引用。"""
    if hit.get("ok"):
        return "可点击"
    return (f"{hit.get('reason')} | 遮挡者: <{hit.get('blockerTag')}> "
            f"class={hit.get('blockerClass')} text='{hit.get('blockerText')}' "
            f"覆盖率={hit.get('coverRatio')} bodyOverflow={hit.get('bodyOverflow')}")


# ==============================================================================
#                    浮层清障（分级歼灭 + 编辑器白名单保护）
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

_INSIDE_EDITOR_JS = """
(el, gs) => {
    const box = el.closest("div[role='dialog'],[class*='modal'],[class*='mask'],[class*='guide'],[class*='popup']")
                || el.parentElement;
    return !!(box && box.querySelector(gs));
}
"""


def _dismiss_overlays(page, aggressive=False, desc=""):
    """
    分级清障（绝不触碰任何含编辑器的容器）：
      L1 —— 按语义点掉 Cookie 横幅 / 引导浮层（最安全，让前端正确写 localStorage，后续不再弹）
      L2 —— 点弹窗关闭图标 / 按 Escape
      L3 —— aggressive=True 时物理移除遮罩 + 解 scroll-lock（兜底）
    返回：本次是否执行过任何清障动作(bool)。
    """
    acted = False

    # ---- L1-a：Cookie 横幅（OneTrust z-index 极高，必须先吃掉）----
    for sel in COOKIE_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0 and loc.is_visible(timeout=400):
                loc.click(timeout=2000, no_wait_after=True, force=True)
                acted = True
                logger.info(f"[清障/L1] 已关闭 Cookie 横幅 | 选择器: <{sel}> | 场景: <{desc}>")
                page.wait_for_timeout(250)
        except Exception:
            pass

    # ---- L1-b：引导浮层「好的 / 知道了 / Got it」，由宽泛到兜底三级候选 ----
    makers = (
        lambda: page.get_by_role("button", name=RE_DISMISS),
        lambda: page.locator(
            "div[role='dialog'],[class*='modal'],[class*='mask'],[class*='guide'],"
            "[class*='onboard'],[class*='popup'],[class*='tooltip'],[class*='tour']"
        ).locator(
            "button,[role='button'],div[class*='btn'],span[class*='btn'],a[class*='btn']"
        ).filter(has_text=RE_DISMISS),
        lambda: page.locator(
            "button,[role='button'],div[class*='btn'],span[class*='btn']"
        ).filter(has_text=RE_DISMISS),
    )
    for maker in makers:
        try:
            loc = maker()
            for i in range(min(loc.count(), 3)):
                item = loc.nth(i)
                try:
                    if not item.is_visible(timeout=300):
                        continue
                    if item.evaluate(_INSIDE_EDITOR_JS, EDITOR_GUARD_SELECTOR):
                        continue  # 白名单保护：绝不点击编辑器所在容器内的按钮
                    txt = (item.inner_text() or "").strip()[:20]
                    item.click(timeout=2500, no_wait_after=True, force=True)
                    acted = True
                    logger.info(f"[清障/L1] 已点掉引导浮层 | 文案: <{txt}> | 场景: <{desc}>")
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
                logger.info(f"[清障/L2] 已点击弹窗关闭图标 | 场景: <{desc}>")
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
            acted = acted or bool(killed)
            logger.info(f"[清障/L3] 强制歼灭遮罩层 | 场景: <{desc}> | 数量: 【{len(killed)}】 | 明细: 【{killed}】")
        except Exception as e:
            logger.warning(f"[清障/L3] 强制歼灭失败，页面可能仍被浮层锁死 | 场景: <{desc}> | 原因: 【{str(e)[:120]}】")

    return acted


# ==============================================================================
#          常驻守卫：init_script 预置 flag + JS 看门狗 + locator_handler 自愈
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
      for (const el of [document.body, document.documentElement]) {
        const st = getComputedStyle(el);
        if (st.overflow === 'hidden' || st.overflowY === 'hidden') {
          el.style.setProperty('overflow', 'auto', 'important');
          el.style.setProperty('overflow-y', 'auto', 'important');
        }
      }
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
    武装浮层免疫（⚠️ 必须在 page.goto() 之前调用，init_script 只对之后的导航生效）。
    三重保险：init 预置引导已读 flag + 页面内 JS 看门狗常驻点击 + locator_handler 被挡自愈。
    """
    try:
        page.add_init_script(_GUARD_INIT_JS.replace("__GUARD_INTERVAL__", str(GUARD_INTERVAL_MS)))
    except Exception as e:
        logger.warning(f"[守卫/Guard] init_script 注入失败，引导浮层可能反复弹出并拦截点击 | 原因: 【{str(e)[:120]}】")

    trigger = page.locator(
        "div[role='dialog'],[class*='mask'],[class*='overlay'],[class*='backdrop'],[class*='guide']"
    ).filter(has_text=RE_DISMISS).first

    def _on_overlay():
        # 🚀 [核心修复] 兜底防死锁：如果常规点击没能关掉浮层，直接用 JS 强制将其物理隐藏
        acted = _dismiss_overlays(page, aggressive=False, desc="locator_handler")
        if not acted:
            try:
                trigger.evaluate("el => el.style.display = 'none'")
            except Exception:
                pass

    try:
        page.add_locator_handler(trigger, _on_overlay, no_wait_after=True)
        logger.info("[守卫/Guard] 浮层免疫已武装 | 机制: 【预置flag + JS看门狗 + locator_handler自愈】")
        return
    except TypeError:
        pass  # 老版本 Playwright 签名不支持 no_wait_after，退化为兼容模式
    except Exception as e:
        logger.warning(f"[守卫/Guard] locator_handler 不可用(需 Playwright>=1.42)，退化为手动清障 | 原因: 【{str(e)[:120]}】")
        return

    try:
        page.add_locator_handler(trigger, _on_overlay)
        logger.info("[守卫/Guard] 浮层免疫已武装（兼容模式：无 no_wait_after）")
    except Exception as e:
        logger.warning(f"[守卫/Guard] locator_handler 挂载失败，退化为手动清障 | 原因: 【{str(e)[:120]}】")

def report_guard_hits(page, stage=""):
    """读取 JS 看门狗战果，便于事后定位"到底自动关掉了什么浮层"。"""
    try:
        info = page.evaluate("() => ({hits: window.__bnGuardHits || 0, last: window.__bnGuardLast || ''})")
        if info and info.get("hits"):
            logger.info(f"[守卫/Guard] 看门狗累计自动关闭浮层 | 阶段: <{stage}> | 次数: 【{info['hits']}】 "
                        f"| 最后文案: <{info['last']}>")
    except Exception:
        pass


# ==============================================================================
#                            编辑器：定位 / 唤醒 / 光标
# ==============================================================================

def _smart_scroll_to_editor(page, max_scrolls=20):
    """步进式 PageDown 探底，锁定评论区富文本容器并滚入可视范围（后续所有操作的作用域根）。"""
    editor_container = page.locator("div.feed-post-editor").first
    for i in range(max_scrolls):
        if editor_container.is_visible():
            editor_container.scroll_into_view_if_needed()
            logger.info(f"[定位/DOM] 已锁定评论区局部作用域 | 滚动次数: 【{i}】 | 选择器: <div.feed-post-editor>")
            return editor_container
        page.keyboard.press("PageDown")
        time.sleep(0.5)
    raise Exception(f"向下滚动 {max_scrolls} 次仍未找到评论输入区，疑似死链、风控滑块拦截或 body 被 scroll-lock 锁死。")


def _resolve_wake_target(editor_container):
    """
    解析"点哪里能唤醒编辑器"的候选（保持原首选选择器，仅显式排除右上角搜索框）。
    """
    cands = (
        editor_container.locator(
            'input[type="text"]:not([type="search"]):not([placeholder*="搜索"])'
            ':not([placeholder*="Search"]):not([aria-label*="搜索"]):not([aria-label*="Search"]),'
            'input[placeholder]:not([type="search"]):not([placeholder*="搜索"]):not([placeholder*="Search"])'
        ).first,
        editor_container.get_by_placeholder(
            re.compile(r"(发布您的回复|发布你的回复|写下你的|说点什么|发表评论|回复|评论|Reply|Comment|Write|Post)")
        ).first,
        editor_container.locator('div[contenteditable="true"].ProseMirror').first,
        editor_container.locator('div[contenteditable="true"]').first,
        editor_container.locator('[class*="placeholder"]').first,
    )
    for c in cands:
        try:
            if c.count() > 0:
                return c
        except Exception:
            continue
    return editor_container.locator('input, div[contenteditable="true"]').first


def _wake_editor(page, editor_container, max_round=4):
    """
    唤醒富文本编辑器（业务动作不变：点击输入区让 ProseMirror 变为可编辑）。
    每轮先清障，点击方式逐级升级：常规 → force → JS 事件序列 → 鼠标坐标+Tab。返回 real_editor(Locator)。
    """
    target = _resolve_wake_target(editor_container)
    real_editor = editor_container.locator('div[contenteditable="true"].ProseMirror').first
    last_hit = None

    for rnd in range(1, max_round + 1):
        _dismiss_overlays(page, aggressive=(rnd >= 2), desc=f"wake-r{rnd}")
        try:
            target.scroll_into_view_if_needed(timeout=3000)
        except Exception:
            pass
        page.wait_for_timeout(200)

        last_hit = _hit_test(page, target)
        if not last_hit.get("ok"):
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮输入区不可命中，先清障再强点 | 详情: 【{_fmt_hit(last_hit)}】")

        try:
            if rnd == 1:
                target.click(timeout=8000)
            elif rnd == 2:
                target.click(timeout=5000, force=True)
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
                    page.mouse.click(box["x"] + box["width"] / 2, box["y"] + min(box["height"] / 2, 20))
                page.keyboard.press("Tab")
        except Exception as e:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮点击动作抛错，升级策略重试 | 详情: 【{str(e)[:150]}】")

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
            logger.info(f"[编辑器/唤醒] 唤醒成功 | 轮次: 【{rnd}】 | 状态: [可编辑] | 焦点在编辑器: 【{focused}】")
            return real_editor
        except Exception:
            logger.warning(f"[编辑器/唤醒] 第{rnd}轮唤醒未生效（ProseMirror 仍不可编辑），升级策略重试")

    _forensics(page, "wake_fail", {"last_hit_test": last_hit})
    raise Exception(f"编辑器唤醒失败（{max_round}级降级全部失效），疑似浮层持续拦截或前端结构变更。最后命中测试: {last_hit}")


_CARET_PROBE_JS = r"""
(element) => {
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

_CARET_TO_HEAD_JS = r"""
(element) => {
    element.focus();
    if (typeof window.getSelection !== "undefined" && typeof document.createRange !== "undefined") {
        const range = document.createRange();
        range.selectNodeContents(element);
        range.collapse(true);           // true = 折叠到头部
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(range);
    }
    return true;
}
"""


def _force_caret_to_head(page, real_editor, tag=""):
    """
    把光标锁定到编辑器文本的绝对头部——这是保证「正文顶在已注入超链接之前」的关键潜规则。
    加固：用 locator.evaluate 规避框架重渲染导致的 stale handle；再补 Ctrl+Home 让 ProseMirror
    内部 selection 与浏览器 selection 同步；最后回报偏移量便于日志核验。
    """
    try:
        real_editor.evaluate(_CARET_TO_HEAD_JS)
    except Exception as e:
        logger.warning(f"[编辑器/光标] Range 置顶执行异常，降级为键盘方案 | 场景: <{tag}> | 详情: 【{str(e)[:120]}】")

    for combo in ("Control+Home", "Home"):
        try:
            real_editor.press(combo, timeout=3000)
            break
        except Exception:
            continue

    page.wait_for_timeout(300)
    try:
        verify = real_editor.evaluate(_CARET_PROBE_JS)
    except Exception:
        verify = {"atHead": "unknown"}
    logger.info(f"[编辑器/光标] 已锁定光标到文本头部 | 场景: <{tag}> | 校验: 【在头部={verify.get('atHead')}, "
                f"距头部字符数={verify.get('offsetChars')}】")


# ==============================================================================
#                          编辑器：超链接注入 / 正文键入
# ==============================================================================

def _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx):
    """
    在富文本末尾唤起「更多 → 添加链接」弹窗，注入单条超链接并校验上屏。
    入参形貌: link_text / link_url 均为已清洗非空字符串（link_url 已补全协议头）。
    成功返回 True；任一环节失败按原设计只跳过该条、不阻断主流程（返回 False）。
    """
    logger.info(f"[编辑器/链接] 开始注入第 【{idx + 1}】 条 | 锚文本: 【{link_text}】 | URL: <{link_url}>")
    try:
        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(150)

        # 唤醒"更多"菜单：多级后备选择器抵御图标 DOM 结构变动
        _interact_fallback_locators([
            editor_container.locator('#post-editor-more-icon').first,
            editor_container.locator("svg").filter(has=page.locator('path[d^="M12 16.5"]')).first,
            editor_container.locator("div.icon-box").filter(has=page.locator('svg')).last,
            editor_container.get_by_role("button", name=RE_MORE).first,
            editor_container.locator('button[aria-label*="更多"], button[aria-label*="More" i]').first,
        ], action="click", timeout=4000, desc="更多按钮")
        page.wait_for_timeout(350)

        _interact_fallback_locators([
            page.locator('.menu-item').filter(has_text=RE_ADD_LINK).first,
            page.get_by_role("menuitem", name=RE_ADD_LINK).first,
            page.locator('[role="menuitem"], [class*="menu-item"]').filter(has_text=RE_ADD_LINK).first,
        ], action="click", timeout=4000, desc="添加链接选项")

        # 锁定弹窗作用域（无 dialog 角色时退化为整页）
        dialog = page
        try:
            dlg = page.get_by_role("dialog").last
            dlg.wait_for(state="visible", timeout=2000)
            dialog = dlg
        except Exception:
            pass

        # data-bn-type 为币安专有属性，优先嗅探
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
        expect(real_editor.locator("a").filter(
            has_text=re.compile(re.escape(link_text), re.IGNORECASE)).first).to_be_visible(timeout=5000)

        _focus_editor_end(page, real_editor)
        page.keyboard.press("Space")
        page.wait_for_timeout(200)
        logger.info(f"[编辑器/链接] 第 【{idx + 1}】 条注入成功并已上屏 | 结果: [Success]")
        return True

    except Exception as e:
        logger.warning(f"[编辑器/链接] 第 【{idx + 1}】 条注入失败，按设计跳过继续下一条 | 锚文本: 【{link_text}】 "
                       f"| 可能原因: 【更多菜单未唤醒 / 弹窗结构变动 / 上屏校验超时: {str(e)[:150]}】 | 结果: [Skipped]")
        try:
            page.keyboard.press("Escape")
            page.wait_for_timeout(250)
        except Exception:
            pass
        return False


def _type_body(page, real_editor, text):
    """
    键入正文：键序与延迟与线上一致，仅按 TYPE_CHUNK_SIZE 切分调用避免超长文本撞总超时；
    每块后校验是否真落字，未落字用 keyboard.insert_text 补录（不改变最终文本）。
    NEWLINE_SAFE_MODE=True 时才把 \\n 改为 Shift+Enter。
    """
    if NEWLINE_SAFE_MODE and "\n" in text:
        lines = text.split("\n")
        for li, line in enumerate(lines):
            _type_body(page, real_editor, line)
            if li < len(lines) - 1:
                try:
                    real_editor.press("Shift+Enter", timeout=3000)
                except Exception:
                    page.keyboard.press("Shift+Enter")
        return

    for start in range(0, len(text), TYPE_CHUNK_SIZE):
        seg = text[start:start + TYPE_CHUNK_SIZE]
        try:
            before = len(real_editor.inner_text() or "")
        except Exception:
            before = -1

        try:
            real_editor.press_sequentially(seg, delay=TYPE_DELAY_MS, timeout=60000)
        except AttributeError:
            real_editor.type(seg, delay=TYPE_DELAY_MS, timeout=60000)  # 兼容旧版 Playwright
        except Exception as e:
            logger.warning(f"[编辑器/正文] 第 【{start // TYPE_CHUNK_SIZE}】 块键入抛错，转入落字校验补录 "
                           f"| 详情: 【{str(e)[:120]}】")

        if before < 0:
            continue
        try:
            after = len(real_editor.inner_text() or "")
        except Exception:
            after = before
        if after <= before:
            logger.warning(f"[编辑器/正文] 第 【{start // TYPE_CHUNK_SIZE}】 块未落字，降级 insert_text 补录 "
                           f"| 片段长度: 【{len(seg)}】 | 可能原因: 【前端框架吞键 / 焦点被抢】")
            try:
                page.keyboard.insert_text(seg)
            except Exception:
                pass


# ==============================================================================
#                        发送：按钮解析 / 接口监听 / 结果判定
# ==============================================================================

def _pick_trusted_send_button(cands):
    """按原候选顺序挑选发送按钮，仅追加黑名单过滤（「立即回复」等跳转按钮误点会导致正文根本没提交）。"""
    for c in cands:
        try:
            if c.count() == 0:
                continue
            try:
                txt = (c.inner_text(timeout=1500) or "").strip()
            except Exception:
                txt = ""
            if txt and RE_SEND_BLACKLIST.search(txt):
                logger.info(f"[发送/按钮] 候选命中黑名单已跳过 | 文案: <{txt[:20]}>")
                continue
            return c
        except Exception:
            continue
    return None


class _ApiWatcher:
    """
    点击前挂 response 监听、点击后轮询取结果。相较 expect_response 的优势：
    响应早于监听建立、同一动作多次请求、点击本身抛异常等场景都不会漏抓。判据与线上一致。
    """

    def __init__(self, page):
        self.page = page
        self.primary = []
        self.secondary = []
        self._closed = False
        page.on("response", self._on_response)

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
    """兼容 502/风控返回 HTML 而非 JSON 的场景，恒返回 dict（不可解析时为空 dict）。"""
    try:
        raw = resp.json()
        return raw if isinstance(raw, dict) else {"raw": str(raw)}
    except Exception:
        return {}


def _read_api_verdict(page, watcher):
    """
    解读发送结果。主判据(与线上一致)：POST pgc/content/add 且 code=000000 或 success=True。
    返回 (是否成功bool, 评论ID|None)；服务端明确业务拒绝时抛 BusinessErrorException。
    辅助接口仅用于"主接口没抓到"时识别成功，绝不据此抛业务异常（避免误判扩大打击面）。
    """
    resp = watcher.wait_primary(API_WAIT_TIMEOUT_MS)
    if resp is not None:
        body = _parse_api_json(resp)
        if str(body.get("code", "")) == "000000" or body.get("success") is True:
            data = body.get("data")
            cid = data.get("id") if isinstance(data, dict) else None
            logger.info(f"[发送/校验] 主接口确认发送成功 | 响应码: 【000000】 | 评论ID: 【{cid}】 | 结果: [Success]")
            return True, cid
        if body:
            err = body.get("message", "未知业务拦截")
            logger.error(f"[发送/校验] 服务端业务规则拒收本条评论 | HTTP: 【{resp.status}】 | 原因: 【{err}】 "
                         f"| 排查方向: 【内容违规 / 发送频率限制 / 该帖已关闭评论 / 账号权限不足】")
            _forensics(page, "biz_reject", {"url": resp.url, "status": resp.status, "body": body})
            raise BusinessErrorException(f"业务发送被服务器拦截，原因: {err}")
        logger.warning(f"[发送/校验] 主接口响应非 JSON(HTTP 【{resp.status}】)，转入 DOM 兜底校验 "
                       f"| 可能原因: 【网关 502 / 风控返回 HTML 页面】")
        return False, None

    sec = watcher.latest_secondary()
    if sec is None:
        logger.warning(f"[发送/校验] 【{API_WAIT_TIMEOUT_MS}ms】 内未捕获到提交接口，转入 DOM 兜底校验 "
                       f"| 可能原因: 【点击未真正生效 / 请求被前端校验拦下 / 网络极慢】")
        return False, None

    body = _parse_api_json(sec)
    logger.info(f"[发送/校验] 观测到疑似评论接口（非主判据） | URL: <{sec.url}> | HTTP: 【{sec.status}】 "
                f"| body: 【{str(body)[:200]}】")
    if str(body.get("code", "")) == "000000" or body.get("success") is True:
        data = body.get("data")
        cid = (data.get("id") or data.get("commentId") or data.get("contentId")) if isinstance(data, dict) else None
        logger.info(f"[发送/校验] 辅助接口判定发送成功 | 评论ID: 【{cid}】 | 结果: [Success]")
        return True, cid
    return False, None


# ==============================================================================
#                              核心：提交一条评论
# ==============================================================================

def _submit_comment(page, editor_container, comment, image_path=None, url_info_list=None):
    """
    在隔离作用域内跑完发帖全链路：唤醒 →[图片]→[超链接]→ 光标置顶 →[正文]→ 发送校验。
    ⚠️ 业务顺序保持线上潜规则：超链接先注入垫底，随后强制光标置顶，让正文顶在超链接之前。
    入参形貌: url_info_list = [{"text": str, "url": str}, ...]。返回评论ID(str) 或 None。
    """
    comment = str(comment) if comment else ""

    _dismiss_overlays(page, aggressive=False, desc="pre-submit")
    report_guard_hits(page, "pre-submit")

    # ---- 步骤 1：唤醒富文本编辑器 ----
    real_editor = _wake_editor(page, editor_container)

    # ---- 步骤 2：注入图片（失败降级为纯文本，不中断）----
    if image_path and os.path.exists(image_path):
        try:
            editor_container.locator('input[type="file"]').first.set_input_files(image_path, timeout=15000)
            mounted = False
            try:
                expect(editor_container.locator(
                    "img[src^='blob'],img[src^='http'],img[src^='data:'],[class*='thumb'],[class*='preview'] img"
                ).first).to_be_visible(timeout=12000)
                mounted = True
            except Exception:
                page.wait_for_timeout(3500)  # 等不到缩略图则退回固定等待
            logger.info(
                f"[编辑器/图片] 图片挂载完毕 | 路径: <{image_path}> | 缩略图可见: 【{mounted}】 | 结果: [Success]")
        except Exception as e:
            logger.warning(f"[编辑器/图片] 图片上传失败，本条评论自动降级为纯文本 | 路径: <{image_path}> "
                           f"| 可能原因: 【文件损坏/格式不支持/上传控件未渲染: {str(e)[:150]}】")

    # ---- 步骤 3：注入超链接（先注入，让超链接垫底）----
    links = [u for u in (url_info_list or []) if isinstance(u, dict)] if isinstance(url_info_list, list) else []
    if links:
        logger.info(f"[编辑器/链接] 检测到超链接任务 | 数量: 【{len(links)}】 | 结果: [启动注入流]")
    for idx, url_info in enumerate(links):
        link_text = str(url_info.get("text", "")).strip()
        link_url = str(url_info.get("url", "")).strip()
        if not link_text or not link_url:
            continue
        if not re.match(r"^https?://", link_url, re.IGNORECASE):
            link_url = "https://" + link_url
        _dismiss_overlays(page, aggressive=False, desc=f"pre-link-{idx}")
        _inject_single_link(page, editor_container, real_editor, link_text, link_url, idx)

    # ---- 核心潜规则：强制光标置顶，防止正文写进超链接之后 ----
    _force_caret_to_head(page, real_editor, tag="before-body")

    # ---- 步骤 4：注入正文 ----
    if comment.strip():
        logger.info(f"[编辑器/正文] 开始键入正文 | 字符数: 【{len(comment)}】 | 分块: 【{TYPE_CHUNK_SIZE}】")
        # ⚠️ 与线上一致：此处绝不再 click 编辑器，否则光标会跳回已注入的超链接内部
        page.wait_for_timeout(800)
        _type_body(page, real_editor, comment)
        page.wait_for_timeout(500)

        try:
            current_text = (real_editor.inner_text() or "").strip()
        except Exception:
            current_text = ""

        if not current_text:
            logger.warning("[编辑器/正文] 正文被静默清空，触发一次全量补录 | 可能原因: 【前端框架重渲染 / 浮层抢焦点】")
            _dismiss_overlays(page, aggressive=True, desc="text-retry")
            try:
                expect(real_editor).to_be_editable(timeout=4000)
            except Exception:
                real_editor = _wake_editor(page, editor_container, max_round=2)
            _force_caret_to_head(page, real_editor, tag="text-retry")
            _type_body(page, real_editor, comment)
            page.wait_for_timeout(500)

        # ==============================================================================
        # 🚀 [核心修复]: 强制状态同步 (State Resync)
        # 应对 ProseMirror + React 状态脱帧导致“发送按钮置灰”的终极杀招。
        # 将光标切到末尾，模拟一次真实的交互触发 onChange。
        # ==============================================================================
        logger.info("[编辑器/同步] 执行强制状态唤醒 (State Resync)...")
        try:
            _focus_editor_end(page, real_editor)
            real_editor.press("Space")
            page.wait_for_timeout(100)
            real_editor.press("Backspace")
            page.wait_for_timeout(300)
        except Exception as sync_e:
            logger.warning(f"[编辑器/同步] 状态唤醒动作异常，但继续主流程: {str(sync_e)[:100]}")

        logger.info("[编辑器/正文] 正文输入与状态同步完成 | 结果: [Success]")

    # ---- 步骤 5：定位并点击发送 ----
    _dismiss_overlays(page, aggressive=False, desc="pre-send")
    send_btn_cands = [
        editor_container.locator("button").filter(has_text=RE_SEND_EXACT).first,
        editor_container.get_by_role("button", name=RE_SEND).first,
    ]

    # ==============================================================================
    # 🚀 [核心修复]: 严格的按钮状态断言
    # 绝不用 JS 原生去点一个 Disabled 的按钮，那会制造假希望并导致幽灵超时。
    # ==============================================================================
    try:
        send_button = _pick_trusted_send_button(send_btn_cands) or _interact_fallback_locators(
            send_btn_cands, action="wait", timeout=5000, desc="发送按钮")

        # 必须等待它变成 enable（亮黄色）
        expect(send_button).to_be_enabled(timeout=8000)
    except PlaywrightTimeoutError:
        # 如果依然是 disabled，立即存证并终止，不要去点
        _forensics(page, "btn_disabled_fatal", {
            "btn_html": send_button.evaluate("el => el.outerHTML") if send_button else "none",
            "editor_text": real_editor.inner_text() if real_editor else "none"
        })
        raise Exception(
            "发送按钮被找到，但持续处于禁用(Disabled)状态。前端组件未能识别输入内容。已阻断强制点击，防止死等接口。")
    except Exception as e:
        logger.warning(f"[发送/按钮] 发生意外的定位错误: {str(e)[:150]}")
        send_button = send_btn_cands[0]

    text_before, media_before = _snapshot_editor(real_editor)
    api_success, comment_id = False, None

    logger.info(f"[发送/提交] 触发发送并挂载接口监听 | 主判据: <pgc/content/add> "
                f"| 发送前编辑器: 【文本{text_before}字 / 媒体{media_before}个】")
    watcher = _ApiWatcher(page)
    try:
        clicked = False
        for attempt in range(3):
            hit = _hit_test(page, send_button)
            if not hit.get("ok"):
                logger.warning(
                    f"[发送/提交] 发送按钮不可命中，先清障再重试 | 第【{attempt + 1}】次 | 详情: 【{_fmt_hit(hit)}】")
                _dismiss_overlays(page, aggressive=(attempt >= 1), desc=f"send-blocked-{attempt}")
            try:
                if attempt == 0:
                    _robust_click(send_button)
                elif attempt == 1:
                    send_button.click(timeout=5000, force=True)
                else:
                    send_button.evaluate("el => el.click()")
                clicked = True
                break
            except Exception as ce:
                logger.warning(f"[发送/提交] 第【{attempt + 1}】次点击失败，升级点击方式重试 | 详情: 【{str(ce)[:150]}】")

        if not clicked:
            _forensics(page, "send_click_fail", {"hit": _hit_test(page, send_button)})
            raise Exception("发送按钮点击 3 级降级全部失败（疑似被浮层持续拦截或按钮已失效）。")

        # ==============================================================================
        # 🚀 [新增加固]: 嗅探并处理「关注以回复」权限拦截弹窗
        # ==============================================================================
        try:
            # 宽泛正则：兼容中英文环境下的“关注并回复”按钮
            RE_FOLLOW_AND_REPLY = re.compile(r"关注并回复|Follow and [R|r]eply", re.IGNORECASE)
            follow_reply_btn = page.locator("div[role='dialog'], [class*='modal']").get_by_role(
                "button", name=RE_FOLLOW_AND_REPLY
            ).first

            # 给弹窗 1.5 秒的渲染时间，如果没有弹窗会平滑超时 pass
            if follow_reply_btn.is_visible(timeout=1500):
                logger.info("[发送/权限] 触发了「仅限关注者评论」限制，正在自动点击【关注并回复】")

                # ⚠️ 核心细节：清空 Watcher 之前捕获的 70007 失败响应，防止 _read_api_verdict 误判
                watcher.primary.clear()
                watcher.secondary.clear()

                # 点击关注并回复，这会触发真正的发帖请求
                follow_reply_btn.click(timeout=3000)
                page.wait_for_timeout(500)  # 给予接口一点缓冲时间
        except Exception as bypass_e:
            # 没找到弹窗或点击失败都不阻塞，交由后续的 API 判据来决定生死
            pass
        # ==============================================================================

        api_success, comment_id = _read_api_verdict(page, watcher)
    except PlaywrightTimeoutError as e:
        logger.warning(f"[发送/校验] 等待接口响应超时，转入 DOM 兜底校验 | 详情: 【{str(e)[:120]}】")
    finally:
        watcher.close()

    # ---- 步骤 6：DOM 兜底校验（编辑器被大幅清空即视为发送成功）----
    if api_success:
        return comment_id

    page.wait_for_timeout(3000)
    text_after, media_after = _snapshot_editor(real_editor)
    if (text_before > 0 and text_after < text_before / 3) or (media_before > 0 and media_after < media_before):
        logger.info(f"[发送/校验] DOM 兜底比对通过（输入框已被大幅清空） | 文本: 【{text_before}→{text_after}】 "
                    f"| 媒体: 【{media_before}→{media_after}】 | 结果: [Success]")
        return comment_id

    report_guard_hits(page, "send-fail")
    _forensics(page, "send_no_effect", {
        "text_before": text_before, "text_after": text_after,
        "media_before": media_before, "media_after": media_after,
        "send_btn_hit_test": _hit_test(page, send_button),
    })
    raise Exception(f"发送已点击但输入框未清空且接口无成功响应（文本 {text_before}→{text_after}，"
                    f"媒体 {media_before}→{media_after}），疑似发送按钮失效、内容被前端校验拦下或网络堵塞。")


# ==============================================================================
#                              URL / 帖子ID 解析
# ==============================================================================

def extract_binance_post_id(post_url):
    """
    从帖子 URL 提取纯数字 post_id（严格路径优先，失败退化为宽松匹配再退化为数字尾段）。
    兼容 /square/post/123、/zh-CN/square/post/123、/square/post/history/123 及各 binance 镜像域。
    """
    raw = str(post_url or "").strip()
    if not raw:
        return None
    try:
        url = raw if raw.startswith(("http://", "https://")) else "https://" + raw
        parsed = urlparse(url)
        if re.search(r"(^|\.)binance\.(com|info|me)$", (parsed.hostname or "").lower()):
            m = (re.search(r"/square/post/(?:[a-zA-Z\-]+/)?(\d+)", parsed.path)
                 or re.search(r"/post/(\d+)", parsed.path))
            if m:
                return m.group(1)

        m = re.search(r"/post/(?:[a-zA-Z\-]+/)?(\d+)", raw)
        if m:
            return m.group(1)
        tail = raw.split("?")[0].strip("/").split("/")[-1]
        return tail if tail.isdigit() else None
    except Exception:
        return None


# ==============================================================================
#                                  主控入口
# ==============================================================================

def _alert_failure_scene(error_msg):
    """失败终局提示：响铃 + 聚合打印失败原因。刻意不再 input() 挂起，保证全自动化不卡死。"""
    sys.stdout.write("\a")
    sys.stdout.flush()
    logger.error(f"\n{'=' * 60}\n[中断/Halt] 🚨 任务异常终止（已保存故障现场，不阻塞等待人工）"
                 f"\n[中断/Halt] 失败原因: 【{error_msg}】\n{'=' * 60}")


def _verify_page_ready(page, response, post_id):
    """
    落地页四重体检，返回错误描述(str) 或 None(通过)。顺序即优先级：
    HTTP 状态 → post_id 漂移(重定向/删帖) → 登录态 → 平台软拦截(拉黑/权限)。
    """
    if response and response.status >= 400:
        return f"页面加载异常或帖子已被删除 (HTTP 状态码: {response.status})"

    if post_id:
        landed = unquote(page.url)
        if post_id not in landed:
            page.wait_for_timeout(2000)   # 给 SPA 的 replaceState 一点滞后余量
            landed = unquote(page.url)
        if post_id not in landed:
            editor_alive = True
            try:
                page.locator(
                    "div[contenteditable='true'].ProseMirror,[placeholder*='回复'],[placeholder*='评论']"
                ).first.wait_for(state="attached", timeout=5000)
            except Exception:
                editor_alive = False
            if not editor_alive:
                return (f"页面发生重定向，目标帖子已被删除或失效 | 原帖子ID: {post_id} | 现落地URL: {landed}")
            logger.info(f"[导航/Nav] URL 漂移但评论编辑器存在，判定为前端路由行为，放行 | 落地URL: <{landed}>")

    try:
        page.locator("a[href*='login']").first.wait_for(state="visible", timeout=3000)
        return "页面探测到 Login 按钮，本地 Cookie 可能已过期失效。"
    except PlaywrightTimeoutError:
        pass

    try:
        page.locator("text=您无法查看此内容").wait_for(state="visible", timeout=2500)
        return "触发平台软拦截（账号已被该创作者拉黑或设置了权限），安全跳过当前任务。"
    except PlaywrightTimeoutError:
        pass

    return None


def comment_on_binance_post(post_url, comment, image_path=None, user_data_dir=USER_DATA_DIR,
                            url_info_list=None, debug=False):
    """
    主控入口：调度浏览器打开帖子并执行评论全流程。
    入参形貌: url_info_list = [{"text": str, "url": str}, ...]。
    返回 Tuple(错误信息(str|None), 是否成功(bool), 评论ID(str|None))。
    """
    if not os.path.isdir(user_data_dir):
        return f"缺少用户环境: {user_data_dir}，请先执行登录", False, None

    post_id = extract_binance_post_id(post_url)
    logger.info(f"\n{'=' * 70}\n[任务/Main] 启动自动化评论 | 帖子ID: 【{post_id}】 | URL: <{post_url}> "
                f"| 正文: 【{len(str(comment or ''))}字】 | 图片: <{image_path or '无'}> "
                f"| 链接数: 【{len(url_info_list or [])}】 | 模式: 【{'debug可见' if debug else '离屏后台'}】"
                f"\n{'=' * 70}")

    # 🚀 [核心修复] 彻底禁止 Chrome 恢复上次崩溃/未关闭的会话，从底层斩断假死
    anti_freeze_args = ['--disable-restore-session-state', '--no-default-browser-check']

    offscreen_args = [
                         '--disable-blink-features=AutomationControlled', '--disable-gpu',
                         '--window-position=-10000,-10000', '--no-sandbox', '--disable-dev-shm-usage',
                         '--disable-renderer-backgrounding', '--disable-background-timer-throttling',
                         '--disable-backgrounding-occluded-windows', '--disable-features=CalculateNativeWinOcclusion',
                         '--disable-breakpad',
                         '--force-device-scale-factor=1',  # 防离屏窗口 DPI 漂移导致坐标点击偏移
                         '--hide-scrollbars',
                     ] + anti_freeze_args

    # 🚀 [核心修复] 去掉可见模式下的 --disable-gpu，防止 Windows 有头模式下渲染死锁白屏
    debug_args = [
                     '--disable-blink-features=AutomationControlled', '--start-maximized',
                     '--window-position=0,0'
                 ] + anti_freeze_args

    try:
        with sync_playwright() as p:
            context = None
            try:
                context = _launch_persistent(
                    p, user_data_dir,
                    args=debug_args if debug else offscreen_args,
                    viewport=None if debug else {'width': 1920, 'height': 1080},
                )
                context.set_default_timeout(60000)
                context.set_default_navigation_timeout(60000)

                # 🚀 [核心修复] 绝对不复用 context.pages[0]，强制创建崭新页面，并关闭所有历史垃圾页面
                page = context.new_page()
                for old_page in context.pages:
                    if old_page != page:
                        try:
                            old_page.close()
                        except Exception:
                            pass
                page.bring_to_front()

                # 🚀 必须在 goto 之前武装守卫：init_script 只对之后的导航生效
                install_overlay_guard(page)

                # 🚀 [核心修复] 等待级别降级为 domcontentloaded，避免被第三方死链卡死
                response = page.goto(post_url, timeout=60000, wait_until="domcontentloaded")

                _dismiss_overlays(page, aggressive=False, desc="post-nav")
                report_guard_hits(page, "post-nav")

                page_err = _verify_page_ready(page, response, post_id)
                if page_err:
                    logger.warning(f"[导航/Nav] 落地页体检未通过，安全跳过本任务 | 帖子ID: 【{post_id}】 "
                                   f"| 原因: 【{page_err}】 | 结果: [Failed]")
                    return page_err, False, None

                check_for_crash(page)

                # 定位编辑器前再清障：引导浮层会给 body 加 scroll-lock 让 PageDown 失效
                _dismiss_overlays(page, aggressive=False, desc="pre-scroll")
                try:
                    editor_container = _smart_scroll_to_editor(page)
                except PlaywrightTimeoutError:
                    logger.warning("[导航/Nav] 编辑器定位超时，疑似 scroll-lock 未解除，强制清障后重试一次")
                    _dismiss_overlays(page, aggressive=True, desc="scroll-lock-retry")
                    page.wait_for_timeout(500)
                    editor_container = _smart_scroll_to_editor(page)

                check_for_crash(page)

                comment_id = _submit_comment(page, editor_container, comment, image_path, url_info_list)
                logger.info(
                    f"[任务/Main] 评论发送成功 | 帖子ID: 【{post_id}】 | 评论ID: 【{comment_id}】 | 结果: [Success]")
                return None, True, comment_id

            except BusinessErrorException as biz_e:
                error_info = f"[业务拦截] {biz_e}"
                logger.error(f"[任务/Main] 发帖被服务端业务规则阻断，无需重试 | 帖子ID: 【{post_id}】 "
                             f"| 原因: 【{error_info}】 | 结果: [Failed]")
                return error_info, False, None

            except Exception as e:
                is_timeout = isinstance(e, PlaywrightTimeoutError)
                if is_timeout:
                    error_info = f"[元素/网络超时] {e}"
                    logger.error(f"[任务/Main] 元素等待或网络请求超时 | 帖子ID: 【{post_id}】 "
                                 f"| 可能原因: 【引导浮层拦截 / 帖子软删除 / 页面卡顿 / 网络抖动】 "
                                 f"| 详情: 【{error_info[:200]}】 | 结果: [Failed]")
                else:
                    error_info = f"[{type(e).__name__}] {e}\n[Traceback]:\n{traceback.format_exc()}"
                    logger.error(f"[任务/Main] 执行中发生未预期异常 | 帖子ID: 【{post_id}】 "
                                 f"| 摘要: 【{str(e)[:200]}】 | 结果: [Failed]")

                if context and context.pages:
                    try:
                        pg = context.pages[0]
                        report_guard_hits(pg, "timeout" if is_timeout else "error")
                        _forensics(pg, ("timeout" if is_timeout else "error") + f"_{post_id}",
                                   {"post_url": post_url, "error": error_info[:2000]})
                    except Exception as s_e:
                        logger.warning(f"[任务/Debug] 故障现场保存失败 | 可能原因: 【磁盘不可写或页面已销毁: {s_e}】")

                _alert_failure_scene(error_info)
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
        logger.error(f"[任务/Main] Playwright 核心框架崩溃，浏览器引擎未能启动 "
                     f"| 可能原因: 【Chrome 版本不匹配 / User Data 目录被其它进程占用 / 磁盘权限不足】 "
                     f"| 详情: 【{str(core_e)[:200]}】")
        return error_info, False, None


# ==============================================================================
#                          会话管理 / 凭证提取 / 人工接管
# ==============================================================================

def login_and_save_session():
    """打开可见浏览器供人工手动登录，回车后关闭并把会话固化到本地 User Data 目录。"""
    logger.info(f"[登录/Auth] 准备手动登录 | 存储路径: <{USER_DATA_DIR}>")
    clean_browser_cache(USER_DATA_DIR)

    with sync_playwright() as p:
        context = None
        try:
            context = _launch_persistent(
                p, USER_DATA_DIR,
                args=['--disable-blink-features=AutomationControlled', '--start-maximized'],
                hide_automation=False,
            )
            page = context.new_page()
            page.goto(LOGIN_URL)
            input("\n[登录/Auth] 等待操作 | 动作: 【登录成功后，请按 Enter 键关闭并保存会话】")
            logger.info("[登录/Auth] 会话已固化到本地 | 结果: [Success]")
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass


def get_auth_tokens_robust(user_data_dir):
    """
    以真实浏览器请求为样本提取脱机 API 凭证。
    流程：Headed 打开广场作者页 → 拦截首个非 OPTIONS 的 `pgc/user/client` 请求 → 取其 csrftoken/cookie；
    请求头无 Cookie 时，回退用 context.cookies("https://www.binance.com") 拼装（限定域名，避免带上
    全站巨型 Cookie 触发 400）。返回 (cookie|None, csrf|None)。
    """
    if not os.path.exists(user_data_dir):
        logger.warning(f"[凭证/Auth] 环境目录不存在，无法提取 | 目录: <{user_data_dir}>")
        return None, None

    visit_url = "https://www.binance.com/zh-CN/square/profile/insights_anchor"
    api_keyword = "pgc/user/client"
    logger.info(f"[凭证/Auth] 启动浏览器提取凭证(Headed 必须可见) | 拦截目标: <{api_keyword}>")

    with sync_playwright() as p:
        context = None
        try:
            context = _launch_persistent(
                p, user_data_dir,
                args=['--disable-blink-features=AutomationControlled'],
                viewport={'width': 1280, 'height': 720},
                hide_automation=False,
            )
            page = context.pages[0] if context.pages else context.new_page()

            with page.expect_request(
                    lambda req: api_keyword in req.url and req.method != "OPTIONS", timeout=20000
            ) as req_info:
                page.goto(visit_url, wait_until="networkidle")

            req = req_info.value
            headers = req.headers  # Playwright 返回的 header key 恒为小写
            csrf = (headers.get("csrftoken") or "").strip() or None
            cookie = (headers.get("cookie") or "").strip()
            source = "request-header"

            if not cookie:
                raw_cookies = context.cookies("https://www.binance.com")
                cookie = "; ".join(f"{c['name']}={c['value']}" for c in raw_cookies).strip()
                source = "context-cookies(binance域)"

            if not cookie:
                logger.warning(f"[凭证/Auth] 提取失败：捕获到请求但无任何合法凭据 | 请求: 【{req.method} {req.url}】 "
                               f"| 排查方向: 【浏览器当前是否处于登录态】")
                return None, None

            has_p20t = "p20t=" in cookie
            level = logger.info if (has_p20t and csrf) else logger.warning
            level(f"[凭证/Auth] 凭证提取完成 | 来源: <{source}> | CSRF: 【{str(csrf)[:8]}...】 "
                  f"| Cookie长度: 【{len(cookie)}】 | 含核心 p20t: 【{has_p20t}】"
                  f"{'' if (has_p20t and csrf) else ' | 提醒: 缺失 p20t 或 CSRF，后续 API 很可能 401/400'}")
            return cookie, csrf

        except PlaywrightTimeoutError:
            logger.warning(f"[凭证/Auth] 提取失败：20s 内未捕获到目标接口 <{api_keyword}> "
                           f"| 排查方向: 【浏览器打开时是否已登录 / 页面是否被风控拦截】")
            return None, None
        except Exception as e:
            logger.error(f"[凭证/Auth] 提取过程发生未预期异常 | 详情: 【{e}】")
            return None, None
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass


def open_browser_for_manual_use(user_data_dir, home_url="https://www.binance.com/zh-CN"):
    """启动可见浏览器交由人工自由操作（含 window-position 归零 + 置顶，防历史屏幕外坐标缓存）。"""
    logger.info(f"\n{'=' * 60}\n[人工/Manual] 启动本地浏览器交接控制权 | 目录: <{user_data_dir}>\n{'=' * 60}")
    with sync_playwright() as p:
        context = None
        try:
            context = _launch_persistent(
                p, user_data_dir,
                args=['--disable-blink-features=AutomationControlled', '--start-maximized', '--window-position=0,0'],
            )
            page = context.pages[0] if context.pages else context.new_page()
            page.bring_to_front()
            page.goto(home_url)
            logger.info("[人工/Manual] ✅ 浏览器已就绪，控制权已交接 | 🛑 退出方式: 【直接关闭浏览器窗口，程序自动结束】")
            page.wait_for_event("close", timeout=0)
        except Exception as e:
            logger.warning(f"[人工/Manual] 浏览器运行异常 | 可能原因: 【环境损坏或窗口被手动强杀: {e}】")
        finally:
            if context:
                try:
                    context.close()
                except Exception:
                    pass
            logger.info("[人工/Manual] 👋 窗口已关闭，控制权收回，系统资源已释放。\n")


# ==============================================================================
#                                   启动入口
# ==============================================================================
if __name__ == "__main__":
    # 其他可选入口（按需取消注释）：
    # login_and_save_session()                                  # 初次手动登录并固化 Session
    # open_browser_for_manual_use(USER_DATA_DIR)                # 人工接管调试
    # cookies, csrf = get_auth_tokens_robust(USER_DATA_DIR)     # 提取脱机 API 凭证

    test_url = "https://www.binance.com/zh-CN/square/post/309692475255842"
    test_msg = "少即是多，慢即是快。同频共振！🚀"
    test_img = r"C:\Users\zxh\Desktop\temp\a6c98436-42f9-4aa9-bab8-.png"
    test_links = [{"text": "带单", "url": "https://www.binance.com/zh-CN/square/post/309692475255842"}]

    err, success, c_id = comment_on_binance_post(
        post_url=test_url, comment=test_msg, image_path=test_img, url_info_list=test_links, debug=True
    )

    if success:
        logger.info(f"\n[结果/Final] 🎉 ======== 自动评论任务圆满成功 ======== | 评论ID: 【{c_id}】")
    else:
        logger.error(f"\n[结果/Final] ❌ ======== 任务失败 ======== | 最终追溯:\n{err}")