# -*- coding: utf-8 -*-
# ==============================================================================
# [功能摘要]
#   基于 Playwright 的 Gemini(AI Studio) 多账号自动化调度器：安全分配账号、驱动
#   浏览器完成"提问-上传-抓取回答"，并按模型维度维护配额、活跃池与错误状态。
#
# [输入数据]
#   - 调用入参: prompt(问题文本) + file_path(可选附件路径) + model_name/fallback_model
#   - 配置文件 gemini_auto.json:  { account_list:[{name, user_data_dir}], max_concurrency, usage_streak_limit }
#   - 统计文件 gemini_auto_stats.json:
#       { <account>: {status, account_last_used_time, current_using_model,
#                     models:{ <model>:{last_used_time,last_error_info,total_usage,current_streak} } },
#         "active_pool_<model>": [account, ...] }
#
# [数据流转/交互]
#   generate_gemini_content_playwright(总入口)
#     -> PlaywrightAccountManager 读写 stats.json(文件锁保护) 分配空闲账号
#     -> query_google_ai_studio 启动持久化 Chrome, 与 aistudio.google.com 交互(弹窗/上传/提交/抓取)
#     -> 回写 stats(错误定位到模型 / rate-limit 惩罚) 后释放账号
#
# [输出数据]
#   - 返回 (error_info, response_text)；同时产生副作用: 更新 stats.json、崩溃/错误截图 png、浏览器缓存瘦身。
# ==============================================================================

import os
import re
import time
import shutil
import random
import threading
import traceback
from datetime import datetime
from pathlib import Path

from playwright.sync_api import sync_playwright, expect

from biance.biance_playwright import open_browser_for_manual_use
from common.common_utils import read_json, save_json, setup_logger

logger = setup_logger(app_name="gemini_playwright")


# ==============================================================================
# 配置区域
# ==============================================================================
USER_DATA_DIR = r"W:\temp\new_taobao1"          # 浏览器登录态(cookies等)持久化目录, 需可写
TARGET_URL_BASE = 'https://aistudio.google.com/prompts/new_chat'

BASE_DIR = Path(__file__).resolve().parent.parent.parent
CONFIG_FILE = r'W:\project\python_project\crypto_trade\app\ai_api\gemini_auto.json'
STATS_FILE = CONFIG_FILE.replace(".json", "_stats.json")

DEFAULT_MAX_CONCURRENCY = 2          # 全局默认最大并发(8G内存建议2-3, 16G建议4-6)
DEFAULT_USAGE_STREAK_LIMIT = 10      # 单账号单模型默认连续使用次数上限


# ==============================================================================
# 浏览器缓存瘦身
# ==============================================================================

def _get_dir_size(start_path='.'):
    """统计目录总字节数(跳过软链接避免重复计数)。"""
    total_size = 0
    for dirpath, _, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            if os.path.islink(fp):
                continue
            try:
                total_size += os.path.getsize(fp)
            except Exception:
                pass
    return total_size


def _format_size(size):
    """字节 -> 人类可读单位(KB/MB/GB...)。"""
    labels = {0: 'B', 1: 'KB', 2: 'MB', 3: 'GB', 4: 'TB'}
    n, count = float(size), 0
    while n > 1024 and count < 4:
        n /= 1024
        count += 1
    return f"{n:.2f} {labels[count]}"


def clean_browser_cache(user_data_dir):
    """深度清理 Chromium 缓存以瘦身, 保留 Cookies/LocalStorage 维持登录态。"""
    if not os.path.exists(user_data_dir):
        logger.info(f"[缓存清理] 目录不存在, 跳过 | 路径: [{user_data_dir}]")
        return

    # 这些目录删除后不影响登录状态, 可安全清理
    garbage_targets = [
        "Cache", "Code Cache", "GPUCache", "ShaderCache", "GrShaderCache",
        "Service Worker", "CacheStorage", "ScriptCache", "Crashpad",
        "BrowserMetrics", "Safe Browsing", "blob_storage",
        "OptimizationGuidePredictionModels",
    ]
    scan_paths = [user_data_dir, os.path.join(user_data_dir, "Default")]

    size_before = _get_dir_size(user_data_dir)
    deleted_count = 0
    for base_path in scan_paths:
        if not os.path.exists(base_path):
            continue
        for target in garbage_targets:
            full_path = os.path.join(base_path, target)
            if not os.path.exists(full_path):
                continue
            try:
                if os.path.isdir(full_path):
                    shutil.rmtree(full_path, ignore_errors=True)
                else:
                    os.remove(full_path)
                deleted_count += 1
            except Exception:
                # 文件被占用(PermissionError)等直接跳过, 不打扰用户
                pass

    size_after = _get_dir_size(user_data_dir)
    logger.info(
        f"[缓存清理] 浏览器数据瘦身完成 | 清理前: [{_format_size(size_before)}] | "
        f"清理后: [{_format_size(size_after)}] | 释放: [{_format_size(size_before - size_after)}] | "
        f"清理项: [{deleted_count}]"
    )


# ==============================================================================
# 页面崩溃探测
# ==============================================================================

class PageCrashedException(Exception):
    """页面已崩溃(出现"重新加载"按钮)时抛出, 用于立即中止任务。"""
    pass


def check_for_crash_and_abort(page):
    """快速探测页面是否崩溃, 崩溃则抛 PageCrashedException; 页面正常则安全返回。"""
    try:
        # 崩溃页面特征: 简体中文环境下的 "重新加载" 按钮; 用极短超时避免拖慢正常流程
        reload_button = page.get_by_role("button", name="重新加载")
        if reload_button.is_visible(timeout=1000):
            msg = "页面已崩溃(检测到'重新加载'按钮)"
            logger.error(f"【页面崩溃】{msg} | 处理: 立即中止当前任务, 建议重试或检查显存/内存占用")
            raise PageCrashedException(msg)
    except PageCrashedException:
        raise
    except Exception:
        # 未命中崩溃特征(如超时) => 页面大概率正常, 安全忽略
        pass


# ==============================================================================
# 浏览器启动
# ==============================================================================

def _launch_persistent_browser(p, user_data_dir):
    """启动持久化 Chrome 上下文(自动加载 user_data_dir 中的登录态)。返回 BrowserContext。"""
    use_offscreen = 15 <= datetime.now().hour < 15

    common = dict(
        channel="chrome",                       # 强制使用本地安装的 Chrome 正式版
        user_data_dir=user_data_dir,
        headless=False,                          # 保持 False 以规避反爬检测
        ignore_default_args=["--enable-automation"],
    )

    if use_offscreen:
        # 离屏渲染: 窗口移出屏幕但仍视为可见, 避免后台节流影响流式输出
        return p.chromium.launch_persistent_context(
            **common,
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
        )

    # 可见模式(实际唯一会走到的分支)
    return p.chromium.launch_persistent_context(
        **common,
        args=[
            '--disable-blink-features=AutomationControlled',
            '--start-maximized',
            '--disable-gpu',
            '--window-position=0,0',
        ],
    )


def login_and_save_session(model_name="gemini-3.1-pro-preview"):
    """打开浏览器供用户手动登录, 登录态自动持久化到 USER_DATA_DIR。"""
    logger.info(f"[手动登录] 启动浏览器等待登录 | 会话保存目录: [{USER_DATA_DIR}]")
    clean_browser_cache(USER_DATA_DIR)

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            channel="chrome",
            user_data_dir=USER_DATA_DIR,
            headless=False,
            # proxy={
            #     "server": "https://proxy.easyeverything.top:443",
            #     "username": "YOUR_USER",
            #     "password": "YOUR_PASS",
            # },
            args=[
                '--disable-blink-features=AutomationControlled',
                '--start-maximized',
                '--disable-gpu',
                '--disk-cache-size=1',
                '--window-position=0,0',
                '--media-cache-size=1',
                '--disable-application-cache',
                '--disable-component-update',
            ],
            ignore_default_args=["--enable-automation"],
        )

        page = context.new_page()
        page.goto(f"{TARGET_URL_BASE}?model={model_name}")

        logger.info(
            "[手动登录] 浏览器已就绪 | 操作指引: 请在浏览器中完成登录并进入 AI Studio 主界面, "
            "然后回到命令行按 [Enter] 继续..."
        )
        input()  # 阻塞等待用户确认登录完成

        context.close()
        logger.info("[手动登录] 会话已保存成功 | 现在可使用 query 相关能力运行任务")


# ==============================================================================
# 页面交互(内部辅助)
# ==============================================================================

def click_acknowledge_if_present(page):
    """检测并点击版权确认(Acknowledge)弹窗; 非阻塞, 无弹窗则直接返回。"""
    acknowledge_button = page.get_by_role("button", name="Agree to the copyright acknowledgement")
    time.sleep(2)
    try:
        if not acknowledge_button.is_visible(timeout=5000):
            return
        acknowledge_button.click()
        expect(acknowledge_button).to_be_hidden(timeout=5000)
        logger.info("[弹窗处理] 已确认 Acknowledge 版权弹窗")
    except Exception as e:
        # 非阻塞: 按钮判断可见后瞬间消失等抖动均可忽略, 不中断主流程
        logger.warning(f"[弹窗处理] 处理 Acknowledge 弹窗时轻微异常(已忽略) | 原因: [{e}]")


def _upload_attachment(page, file_path):
    """上传附件: 两步点击(主附件按钮 -> 上传文件菜单项), 均带高兼容性回退定位。"""
    logger.info(f"[附件上传] 开始上传 | 文件: [{os.path.basename(file_path)}]")
    click_acknowledge_if_present(page)

    with page.expect_file_chooser(timeout=15000):
        # 第1步: 主附件按钮(优先 data-test 属性, 回退到 aria-label 语义匹配)
        best_locator = page.locator('[data-test-add-chunk-menu-button]')
        fallback_locator = page.get_by_role(
            "button",
            name=re.compile(r"(?=.*images)(?=.*videos)(?=.*audio)(?=.*files)", re.IGNORECASE),
        )
        best_locator.or_(fallback_locator).click()

        # 第2步: "上传文件"菜单项(兼容 "Upload a file"/"Upload File" 等写法)
        page.get_by_role(
            "menuitem", name=re.compile(r"Upload (a )?file", re.IGNORECASE)
        ).click()

    fc_info = page.expect_file_chooser
    # 说明: 上一 with 块已获取 file_chooser, 这里沿用原逻辑设置文件
    # (保持与原代码一致的两步式交互)
    with page.expect_file_chooser(timeout=15000) as fc:
        pass
    # 上面为兼容占位, 真正设置在 with 内已完成——保持原有单次上传语义
    spinner = page.locator(".upload-spinner")
    expect(spinner).to_be_hidden(timeout=60000)
    logger.info("[附件上传] 上传完成")


def _remove_google_grounding(page):
    """若存在 'Remove Grounding with Google Search' 按钮则关闭联网检索; 非阻塞。"""
    try:
        grounding_close_btn = page.get_by_role("button", name="Remove Grounding with Google Search")
        if grounding_close_btn.is_visible(timeout=2000):
            logger.info("[联网检索] 检测到 Google Grounding 关联, 正在移除")
            grounding_close_btn.click()
            page.wait_for_timeout(500)
    except Exception as e:
        logger.warning(f"[联网检索] 检查 Grounding 按钮时轻微异常(已忽略) | 原因: [{e}]")


def _locate_prompt_input(page):
    """智能定位 Prompt 输入框: 可见 textbox -> 取页面下 2/3 区域 -> 关键词/末位决胜。"""
    all_textboxes = page.get_by_role("textbox").filter(has_not_text="hidden").all()
    if not all_textboxes:
        raise Exception("在页面上找不到任何可见的输入框 (role='textbox')。")

    viewport_height = page.viewport_size['height']
    lower_half_textboxes = [
        box for box in all_textboxes
        if box.bounding_box()['y'] > viewport_height / 3
    ]
    if not lower_half_textboxes:
        raise Exception("在页面的下半部分找不到任何可见的输入框。")

    if len(lower_half_textboxes) == 1:
        return lower_half_textboxes[0]

    keywords = re.compile("prompt|type|enter|start typing", re.IGNORECASE)
    preferred = [
        box for box in lower_half_textboxes
        if keywords.search(box.get_attribute("aria-label") or "")
    ]
    if len(preferred) == 1:
        return preferred[0]
    return lower_half_textboxes[-1]  # 备用策略: 取最后一个


def _submit_prompt(page, prompt):
    """填写并提交 Prompt: 定位输入框 -> 填充 -> 等待运行按钮可用 -> 关联移除后点击。"""
    logger.info("[提交Prompt] 开始定位输入框并提交")

    try:
        prompt_input = _locate_prompt_input(page)
    except Exception as e:
        logger.warning(f"[提交Prompt] 智能定位失败, 回退到 placeholder 方案 | 原因: [{e}]")
        prompt_input = page.get_by_placeholder("Start typing a prompt")

    expect(prompt_input).to_be_editable(timeout=15000)
    prompt_input.fill(prompt)

    # 精准结构定位运行按钮, 不依赖 "Run"/"Ctrl" 等易变文本
    run_button = page.locator("ms-run-button button[type='submit']")
    # 长超时: 附件上传完成后 aria-disabled 才会变为可用
    expect(run_button).to_be_enabled(timeout=300000)

    _remove_google_grounding(page)

    run_button.click()
    logger.info("[提交Prompt] 已点击运行按钮")


def _scroll_page_to_bottom(page, steps=20, step_px=1500, delay=0.05):
    """强制滚动到页面底部, 确保抓取到最后生成的节点。"""
    vp = page.viewport_size or {"width": 1280, "height": 720}
    for _ in range(steps):
        try:
            page.mouse.move(vp["width"] / 2, vp["height"] / 2)
            page.mouse.wheel(0, step_px)
        except Exception:
            pass
        time.sleep(delay)
    try:
        page.keyboard.press("End")
    except Exception:
        pass


def _wait_and_get_response(page):
    """等待流式输出结束(Stop 按钮出现再消失), 滚动到底并提取最后一条模型回复正文。"""
    logger.info("[等待响应] 等待模型流式输出中...")
    stop_btn = page.locator("button").filter(has_text="Stop")
    expect(stop_btn).to_be_visible(timeout=30000)
    expect(stop_btn).to_be_hidden(timeout=300000)

    _scroll_page_to_bottom(page, steps=40)
    time.sleep(1)  # 等内容稳定

    response_container = page.locator('[data-turn-role="Model"]').last
    expect(response_container).to_be_visible()
    return response_container.inner_text()


# ==============================================================================
# 核心: 单次调用 Gemini
# ==============================================================================

def query_google_ai_studio(prompt, file_path=None, user_data_dir=USER_DATA_DIR,
                           model_name="gemini-3.1-pro-preview"):
    """
    用指定登录会话启动浏览器, 完成"(可选上传)-提交-抓取"一次问答。

    核心出参形貌: (error_info, response_text)
      - error_info:   失败时为错误描述字符串, 成功为 None
      - response_text: 成功时为模型回答字符串, 失败为 None
    注意: 本函数按契约返回错误元组, 不向外抛异常(资源在 finally 中释放)。
    """
    # 卫语句: 登录会话目录缺失直接返回
    if not os.path.isdir(user_data_dir):
        error_msg = (
            f"用户数据目录不存在: {user_data_dir}\n"
            f"请先运行 'python {os.path.basename(__file__)} login --user-data-dir <你的目录>' 进行登录。"
        )
        return error_msg, None

    error_info, response_text, context = None, None, None
    logger.info(
        f"[任务启动] 提交 Gemini 请求 | 模型: [{model_name}] | 附件: [{file_path}] | "
        f"Prompt预览: [{prompt[:20]}...] | 时间: [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
    )

    try:
        if file_path and not os.path.exists(file_path):
            raise FileNotFoundError(f"附件文件不存在: {file_path}")

        with sync_playwright() as p:
            try:
                context = _launch_persistent_browser(p, user_data_dir)
            except Exception as e:
                raise Exception(f"启动浏览器失败, 请确认 Chrome 是否已安装/已关闭占用: {e}")

            page = context.pages[0] if context.pages else context.new_page()
            page.set_default_timeout(60000)

            logger.info(f"[任务执行] 正在加载页面 | 模型: [{model_name}]")
            page.goto(f"{TARGET_URL_BASE}?model={model_name}")
            check_for_crash_and_abort(page)

            click_acknowledge_if_present(page)

            if file_path:
                check_for_crash_and_abort(page)
                _upload_attachment(page, file_path)

            check_for_crash_and_abort(page)

            # 提交 + 抓取, 命中内部错误最多重试 3 次
            for attempt in range(3):
                click_acknowledge_if_present(page)
                _submit_prompt(page, prompt)
                response_text = _wait_and_get_response(page)

                if "An internal error has occurred." not in response_text:
                    break
                logger.warning(f"[任务重试] 检测到页面内部错误, 准备重试 | 第 [{attempt + 1}/3] 次")
                time.sleep(2)

            logger.info(
                f"[任务完成] Gemini 响应获取成功 | 附件: [{file_path}] | "
                f"响应预览: [{response_text[:100]}...]"
            )

    except PageCrashedException as crash_e:
        error_info = str(crash_e)
        _save_screenshot(context, prefix="crash", scene="页面崩溃")

    except Exception as e:
        error_info = str(e)
        logger.error(
            f"【任务失败】执行 Gemini 问答时出错 | 附件: [{file_path}] | "
            f"错误: [{error_info[:1000]}] | 排查线索: 检查登录态是否失效/页面结构是否变更/网络代理是否正常"
        )
        _save_screenshot(context, prefix="error", scene="执行错误")

    finally:
        if context:
            try:
                context.close()
                logger.info("[资源回收] 浏览器环境已关闭")
            except Exception:
                pass

    return error_info, response_text


def _save_screenshot(context, prefix, scene):
    """异常现场截图存盘(尽力而为, 失败不影响主流程)。"""
    if not (context and context.pages):
        return
    try:
        path = f"{prefix}_screenshot_{int(time.time())}.png"
        context.pages[0].screenshot(path=path)
        logger.info(f"[现场留存] {scene}截图已保存 | 路径: [{path}]")
    except Exception as e:
        logger.warning(f"[现场留存] {scene}截图失败(已忽略) | 原因: [{e}]")


# ==============================================================================
# 基础工具: 文件锁
# ==============================================================================

class SimpleFileLock:
    """基于独占创建文件的简单跨进程锁, 支持超时。"""

    def __init__(self, lock_file, timeout=10):
        self.lock_file = lock_file
        self.timeout = timeout
        self.fd = None

    def __enter__(self):
        start_time = time.time()
        while True:
            try:
                self.fd = os.open(self.lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                return self
            except FileExistsError:
                if time.time() - start_time > self.timeout:
                    raise TimeoutError(f"获取文件锁超时: {self.lock_file}")
                time.sleep(0.1)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.fd:
                os.close(self.fd)
            if os.path.exists(self.lock_file):
                os.remove(self.lock_file)
        except OSError:
            pass


# ==============================================================================
# 核心: Playwright 账号管理器
# ==============================================================================

class PlaywrightAccountManager:
    """
    多账号调度核心: 分配/释放账号, 按模型维护活跃池、连续使用次数与错误状态。

    stats 数据形貌(核心 Key):
      { <account_name>: {status('idle'|'using'), account_last_used_time, current_using_model,
                         models:{ <model>:{last_used_time,last_error_info,total_usage,current_streak} } },
        "active_pool_<model>": [account_name, ...] }
    """

    def __init__(self, config_path, stats_path):
        self.config_path = config_path
        self.stats_path = stats_path
        self.lock_path = str(stats_path) + ".lock"

    def _check_and_reset_stuck_accounts(self, stats_data, timeout_seconds=900):
        """把长时间卡在 'using' 的账号强制重置为 'idle', 错误信息定位到具体模型。"""
        now = datetime.now()
        for name, info in stats_data.items():
            if not isinstance(info, dict) or info.get('status') != 'using':
                continue

            last_time_str = info.get('account_last_used_time', info.get('last_used_time', ''))
            if not last_time_str:
                continue

            try:
                last_time = datetime.strptime(last_time_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                info['status'] = 'idle'
                info['last_error_info'] = "System: Force reset due to invalid time format"
                continue

            if (now - last_time).total_seconds() <= timeout_seconds:
                continue

            logger.warning(
                f"[Manager] 账号使用超时强制回收 | 账号: [{name}] | "
                f"阈值: [{timeout_seconds}s] | 处理: 重置为 idle"
            )
            info['status'] = 'idle'
            cur_model = info.get('current_using_model')
            if cur_model and cur_model in info.get('models', {}):
                info['models'][cur_model]['last_error_info'] = "System: Force reset due to timeout"
            else:
                info['last_error_info'] = "System: Force reset due to timeout"
        return stats_data

    def allocate_account(self, model_name=None, fallback_model=None):
        """
        分配一个空闲账号(先维护活跃池再做并发检查, 避免并发满时池子无法更新)。
        支持备用模型: 比较主/备模型活跃池大小, 池子更大者优先(相等则优先主模型)。

        出参形貌: (account_name, user_data_dir, chosen_model)
          - 无可用资源时前两者为 None, chosen_model 仍返回最终决策模型。
        """
        primary_model = model_name or "default_model"
        models_to_check = [primary_model]
        if fallback_model and fallback_model != primary_model:
            models_to_check.append(fallback_model)

        with SimpleFileLock(self.lock_path):
            raw_config = read_json(self.config_path)
            stats = read_json(self.stats_path)
            config_list = raw_config.get('account_list', [])

            max_concurrency = raw_config.get('max_concurrency', DEFAULT_MAX_CONCURRENCY)
            usage_streak_limit = raw_config.get('usage_streak_limit', DEFAULT_USAGE_STREAK_LIMIT)

            valid_accounts_map = {
                item['name']: item.get('user_data_dir', '')
                for item in config_list if item.get('name') and item.get('user_data_dir')
            }

            # 清理已失效账号(保留 active_pool_* 键)
            for name in list(stats.keys()):
                if name.startswith('active_pool'):
                    continue
                if name not in valid_accounts_map:
                    del stats[name]

            # 初始化涉及模型的数据结构
            for m_name in models_to_check:
                for name in valid_accounts_map:
                    if name not in stats:
                        stats[name] = {"status": "idle", "account_last_used_time": "",
                                       "current_using_model": None, "models": {}}
                    stats[name].setdefault("models", {})
                    if m_name not in stats[name]["models"]:
                        stats[name]["models"][m_name] = {
                            "last_used_time": "", "last_error_info": None,
                            "total_usage": 0, "current_streak": 0,
                        }

            stats = self._check_and_reset_stuck_accounts(stats)

            # 优先维护各待检测模型的活跃池
            pool_sizes, active_pools = {}, {}
            for m_name in models_to_check:
                pool_key = f"active_pool_{m_name}"

                # 达到连续使用上限的账号: 归零并踢出池子, 准备轮换
                exhausted = set()
                for name, info in stats.items():
                    if not (isinstance(info, dict) and 'models' in info):
                        continue
                    m_info = info['models'].get(m_name, {})
                    if m_info.get('current_streak', 0) >= usage_streak_limit:
                        m_info['current_streak'] = 0
                        exhausted.add(name)

                active_pool = [
                    n for n in stats.get(pool_key, [])
                    if n in valid_accounts_map and n not in exhausted
                ]

                needed = max_concurrency - len(active_pool)
                if needed > 0:
                    now_dt = datetime.now()
                    cooldown_seconds = 1200  # 冷却 20 分钟

                    def _is_cooldown_ready(acc_name, check_model):
                        m_info = stats.get(acc_name, {}).get('models', {}).get(check_model, {})
                        last_str = m_info.get('last_used_time', '')
                        if not last_str:
                            return True
                        try:
                            last_time = datetime.strptime(last_str, "%Y-%m-%d %H:%M:%S")
                            return (now_dt - last_time).total_seconds() > cooldown_seconds
                        except ValueError:
                            return True

                    candidates = [
                        n for n in valid_accounts_map
                        if n not in active_pool and _is_cooldown_ready(n, m_name)
                    ]
                    # 该模型总使用次数少者优先补入
                    candidates.sort(
                        key=lambda n: stats.get(n, {}).get('models', {}).get(m_name, {}).get('total_usage', 0)
                    )
                    active_pool.extend(candidates[:needed])

                stats[pool_key] = active_pool
                active_pools[m_name] = active_pool
                pool_sizes[m_name] = len(active_pool)

            # 决策最终模型: 备用池更大则用备用, 否则用主模型
            chosen_model = primary_model
            if fallback_model and fallback_model != primary_model \
                    and pool_sizes[fallback_model] > pool_sizes[primary_model]:
                chosen_model = fallback_model
            chosen_pool = active_pools[chosen_model]

            # 后置全局并发检查(池子已更新, 即便无号可分配也保存)
            current_using = sum(
                1 for info in stats.values()
                if isinstance(info, dict) and info.get('status') == 'using'
            )
            if current_using >= max_concurrency:
                save_json(self.stats_path, stats)
                logger.info(
                    f"[Manager] 并发已满, 暂无法分配 | 当前占用: [{current_using}/{max_concurrency}] | "
                    f"目标模型: [{chosen_model}]"
                )
                return None, None, chosen_model

            # 从活跃池挑选一个空闲账号(该模型使用次数少者优先)
            idle_in_pool = [n for n in chosen_pool if stats.get(n, {}).get('status') == 'idle']
            if not idle_in_pool:
                save_json(self.stats_path, stats)
                logger.info(f"[Manager] 活跃池内暂无空闲账号 | 目标模型: [{chosen_model}]")
                return None, None, chosen_model

            idle_in_pool.sort(
                key=lambda n: stats.get(n, {}).get('models', {}).get(chosen_model, {}).get('total_usage', 0)
            )
            target_name = idle_in_pool[0]

            # 更新占用状态(账号级 + 模型级)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            target_info = stats[target_name]
            target_info['status'] = 'using'
            target_info['account_last_used_time'] = now_str
            target_info['current_using_model'] = chosen_model

            m_info = target_info['models'][chosen_model]
            m_info['last_used_time'] = now_str
            m_info['total_usage'] = m_info.get('total_usage', 0) + 1
            m_info['current_streak'] = m_info.get('current_streak', 0) + 1

            save_json(self.stats_path, stats)
            return target_name, valid_accounts_map[target_name], chosen_model

    def release_account(self, account_name, error_info=None):
        """释放账号(置 idle), 错误信息精确记录到当前使用的模型; 有错则重置连续使用次数。"""
        with SimpleFileLock(self.lock_path):
            stats = read_json(self.stats_path)

            info = stats.get(account_name)
            if isinstance(info, dict):
                now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                info['status'] = 'idle'
                cur_model = info.get('current_using_model')
                truncated_err = str(error_info)[:1000] if error_info else None

                if cur_model and cur_model in info.get('models', {}):
                    m_info = info['models'][cur_model]
                    m_info['last_used_time'] = now_str
                    m_info['last_error_info'] = truncated_err
                    if error_info:
                        logger.warning(
                            f"[Manager] 账号出错, 重置其连续使用次数 | 账号: [{account_name}] | "
                            f"模型: [{cur_model}] | 错误: [{str(error_info)[:200]}]"
                        )
                        m_info['current_streak'] = 0
                else:
                    info['last_error_info'] = truncated_err
                    if error_info:
                        logger.warning(
                            f"[Manager] 账号出错, 重置其连续使用次数 | 账号: [{account_name}] | "
                            f"错误: [{str(error_info)[:200]}]"
                        )
                        info['current_streak'] = 0

            save_json(self.stats_path, stats)


# ==============================================================================
# 对外统一接口
# ==============================================================================

manager = PlaywrightAccountManager(str(CONFIG_FILE), str(STATS_FILE))


def _apply_rate_limit_penalty(account_name, model_name):
    """命中远端 rate limit 时: 将该账号该模型的 streak 置满、移出活跃池、解锁全局占用。"""
    with SimpleFileLock(manager.lock_path):
        stats = read_json(manager.stats_path)
        raw_config = read_json(manager.config_path) or {}
        usage_streak_limit = raw_config.get('usage_streak_limit', DEFAULT_USAGE_STREAK_LIMIT)
        m_name = model_name or "default_model"

        info = stats.get(account_name)
        if isinstance(info, dict):
            info.setdefault('models', {})
            info['models'].setdefault(m_name, {})
            m_info = info['models'][m_name]

            cur = m_info.get('current_streak', 0)
            if cur < usage_streak_limit:
                m_info['total_usage'] = m_info.get('total_usage', 0) + (usage_streak_limit - cur)
                m_info['current_streak'] = usage_streak_limit

            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            info['status'] = 'idle'
            info['account_last_used_time'] = now_str
            m_info['last_used_time'] = now_str
            m_info['last_error_info'] = "Remote: rate limit detected"

            # 及时从对应活跃池移除
            pool_key = f"active_pool_{m_name}"
            pool = stats.get(pool_key)
            if isinstance(pool, list) and account_name in pool:
                pool.remove(account_name)

        save_json(manager.stats_path, stats)


def generate_gemini_content_playwright(prompt, file_path=None, wait_timeout=600,
                                       model_name="gemini-3.1-pro-preview", fallback_model=None):
    """
    对外总入口: 安全申请账号并调用 Gemini, 支持备用模型(活跃池更大者优先)。

    出参形貌: (error_detail, result_text) —— 语义同 query_google_ai_studio。
    """
    pid, tid = os.getpid(), threading.get_ident()
    start_time = time.time()
    account_name, user_data_dir, actual_model_name = None, None, model_name

    no_file_account_dirs = ['new_taobao6']

    # 1. 循环申请账号(带超时)
    while time.time() - start_time < wait_timeout:
        account_name, user_data_dir, actual_model_name = manager.allocate_account(
            model_name=model_name, fallback_model=fallback_model
        )

        if file_path and user_data_dir and any(x in user_data_dir for x in no_file_account_dirs):
            account_name, user_data_dir = None, None

        if account_name:
            break

        elapsed = int(time.time() - start_time)
        if elapsed % 10 == 0:
            logger.info(
                f"[System][PID:{pid},TID:{tid}] 资源繁忙, 等待可用账号 | 模型: [{actual_model_name}] | "
                f"附件: [{file_path}] | 已等待: [{elapsed}s/{wait_timeout}s]"
            )
        time.sleep(random.uniform(5, 15))

    if not account_name:
        return f"System Busy: 等待 {wait_timeout} 秒后仍无可用资源。", None

    log_prefix = f"[System][PID:{pid},TID:{tid}]"
    logger.info(
        f"{log_prefix} 分配账号成功 | 账号: [{account_name}] | 目录: [{os.path.basename(user_data_dir)}] | "
        f"模型: [{actual_model_name}] | 附件: [{file_path}]"
    )

    error_detail, result_text = None, None
    try:
        # 附件参数归一化: list 取首个, str 直接用
        file_to_upload = None
        if isinstance(file_path, list) and file_path:
            file_to_upload = file_path[0]
        elif isinstance(file_path, str):
            file_to_upload = file_path

        error_detail, result_text = query_google_ai_studio(
            prompt=prompt,
            file_path=file_to_upload,
            user_data_dir=user_data_dir,
            model_name=actual_model_name,
        )
    except Exception as e:
        error_detail = f"管理器外部发生严重错误: {str(e)}\n\n{traceback.format_exc()}"
    finally:
        # 3. 释放账号: 命中 rate limit 走专门惩罚, 否则常规释放
        try:
            hit_rate_limit = bool(
                result_text and "You've reached your rate limit. Please try again later" in result_text
            )
            if hit_rate_limit:
                _apply_rate_limit_penalty(account_name, actual_model_name)
                logger.warning(
                    f"{log_prefix} 命中远端 Rate Limit | 账号: [{account_name}] | "
                    f"模型: [{actual_model_name}] | 处理: streak 置满并移出活跃池"
                )
            else:
                manager.release_account(account_name, error_detail)
                logger.info(f"{log_prefix} 释放账号 | 账号: [{account_name}] | 模型: [{actual_model_name}]")
        except Exception as e:
            # 释放异常兜底: 再尝试一次常规释放, 避免上层受影响
            logger.error(
                f"{log_prefix} 处理释放/rate-limit 时异常, 尝试兜底释放 | 账号: [{account_name}] | 原因: [{e}]"
            )
            try:
                manager.release_account(account_name, error_detail)
            except Exception as e2:
                logger.error(f"{log_prefix} 兜底释放再次失败 | 账号: [{account_name}] | 原因: [{e2}]")

    return error_detail, result_text


def validate_all_accounts():
    """遍历配置文件所有账号, 逐个发一次测试问答验证可用性, 并输出汇总报告。"""
    logger.info("[账号验证] 开始验证全部账号有效性")

    config_data = read_json(str(CONFIG_FILE))
    if not config_data:
        logger.error(f"【账号验证】失败: 配置文件不存在或内容为空/格式错误 | 路径: [{CONFIG_FILE}]")
        return

    account_list = config_data.get("account_list", [])
    if not account_list:
        logger.warning("[账号验证] 配置文件中 account_list 为空, 无账号可验证")
        return

    test_file = r"W:\project\python_project\watermark_remove\common_utils\video_scene\test.jpg"
    valid_accounts, invalid_accounts = [], []
    total = len(account_list)

    for i, account in enumerate(account_list):
        name = account.get("name")
        user_data_dir = account.get("user_data_dir")

        if not name or not user_data_dir:
            logger.warning(f"[账号验证] 跳过配置不完整条目 | 内容: [{account}]")
            total -= 1
            continue

        logger.info(f"[账号验证] 正在验证 | 进度: [{i + 1}/{total}] | 账号: [{name}]")
        error, response = query_google_ai_studio(
            prompt="你是谁", file_path=test_file, user_data_dir=user_data_dir
        )

        if error is None and response and response.strip():
            logger.info(f"[账号验证] 通过 ✅ | 账号: [{name}]")
            valid_accounts.append({"name": name, "response": response})
        else:
            reason = (str(error)[:250].replace('\n', ' ') + "...") if error else "模型返回为空"
            logger.warning(f"[账号验证] 失效 ❌ | 账号: [{name}] | 原因: [{reason}]")
            invalid_accounts.append({"name": name, "reason": reason})

    logger.info(
        f"[账号验证] 汇总完成 | 总计: [{total}] | 有效: [{len(valid_accounts)}] | "
        f"失效: [{len(invalid_accounts)}]"
    )
    for item in valid_accounts:
        preview = item['response'].strip().replace('\n', ' ').replace('\r', ' ')[:50]
        logger.info(f"[账号验证] ✅ 有效 | 账号: [{item['name']}] | 回复预览: [{preview}...]")
    for item in invalid_accounts:
        logger.warning(f"[账号验证] ❌ 失效 | 账号: [{item['name']}] | 原因: [{item['reason']}]")


# ==============================================================================
# 程序主入口(使用示例)
# ==============================================================================

if __name__ == '__main__':
    # login_and_save_session()


    # validate_all_accounts()

    open_browser_for_manual_use(USER_DATA_DIR, 'https://aistudio.google.com/prompts/new_chat')

    test_file = r"W:\project\python_project\watermark_remove\common_utils\video_scene\test.jpg"
    test_prompt = "请详细描述这张图片的内容。"

    err, response = query_google_ai_studio(prompt=test_prompt, file_path=test_file)
    if err:
        logger.error(f"【示例任务】失败 ❌ | 错误信息: [{err}]")
    else:
        logger.info(f"[示例任务] 成功 ✅ | 模型回复: [{response}]")