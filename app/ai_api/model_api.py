# [功能摘要] 为 OpenAI 兼容网关提供同步文本生成、模型重试/降级与连通性探测。
# [输入数据] 配置项 local_api_url/local_api_key；提示词、模型名、本地附件路径序列；报告路径。
# [数据流转/交互] 文本附件转义、媒体编码为 data URL → 组装一次 user 消息 →
# OpenAI SDK 同步请求主模型/备用模型 → 校验文本、记录脱敏错误与耗时；模型列表逐个复用此流程。
# [输出数据] 生成结果字典（status/content/metrics/error_history/trace_id）；探测报告由 save_json 写盘。
"""Python 3.9+；依赖 openai 和项目已有的 common.common_utils。"""

import base64
import logging
import math
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path

from openai import OpenAI

from common.common_utils import get_config, save_json, setup_logger

__all__ = ["generate_content", "probe_models"]

BASE_URL = get_config("local_api_url")
API_KEY = get_config("local_api_key")

TEXT_EXTENSIONS = {
    ".txt", ".md", ".log", ".py", ".json", ".jsonl", ".csv", ".tsv",
    ".yaml", ".yml", ".xml", ".html", ".css", ".js", ".ts", ".sql",
    ".ini", ".conf", ".toml", ".sh", ".rst",
}
MEDIA_TYPES = {
    ".png": ("image", "image/png"),
    ".jpg": ("image", "image/jpeg"),
    ".jpeg": ("image", "image/jpeg"),
    ".webp": ("image", "image/webp"),
    ".gif": ("image", "image/gif"),
    # : video_url 是兼容网关扩展；保留原载荷结构，需确认服务端支持。
    ".mp4": ("video", "video/mp4"),
    ".webm": ("video", "video/webm"),
    ".mov": ("video", "video/quicktime"),
    ".avi": ("video", "video/x-msvideo"),
    ".mkv": ("video", "video/x-matroska"),
}


def _redact(value):
    """统一隐藏配置密钥、常见凭据和 URL 用户信息。"""
    text = str(value)
    if API_KEY:
        text = re.sub(re.escape(API_KEY), "***", text)
    text = re.sub(r"(?i)sk-[a-z0-9_-]+", "***", text)
    text = re.sub(r"(?i)(\bBearer\s+)[^\s,'\"<>]+", r"\1***", text)
    text = re.sub(
        r"(?i)((?:api[_-]?key|access[_-]?token|authorization)[\"']?\s*[:=]\s*[\"']?)[^&\s,'\"}<>]+",
        r"\1***", text,
    )
    return re.sub(r"(https?://)[^/\s@]+@", r"\1***@", text)


def _preview(value, limit=120):
    """先脱敏再截断，避免日志留下密钥残片。"""
    text = _redact(value).replace("\r", " ").replace("\n", " ")
    return text[:limit] + ("..." if len(text) > limit else "")


def _log(message, level=logging.INFO):
    """输出单条脱敏日志；沿用日志故障不影响请求结果的既有设计。"""
    try:
        logger.log(level, _redact(message).replace("\r", " ").replace("\n", " "), stacklevel=2)
    except Exception:
        pass


class _RedactingFormatter(logging.Formatter):
    """保留现有日志格式，对最终文本及异常堆栈统一脱敏。"""

    def __init__(self, original=None):
        super().__init__()
        self.original = original or logging.Formatter()

    def format(self, record):
        """record 为 LogRecord（msg/args/exc_info）；输出沿用原格式的脱敏文本。"""
        return _redact(self.original.format(record))


class _HttpLogsFilter(logging.Filter):
    """沿用原过滤规则，减少 HTTP 库内部日志噪音。"""

    def filter(self, record):
        """依据 LogRecord 的 name/funcName 决定是否保留日志。"""
        return not (
            record.name.startswith(("httpx", "httpcore", "openai", "urllib3"))
            or record.funcName in ("_send_single_request", "send")
        )


logger = setup_logger(app_name="model_api")
_http_filter = _HttpLogsFilter()
for handler in logger.handlers:
    handler.addFilter(_http_filter)
    if not isinstance(handler.formatter, _RedactingFormatter):
        handler.setFormatter(_RedactingFormatter(handler.formatter))


def _close_client(client, context):
    """client 为具有 close() 的 SDK 对象；返回脱敏清理错误字符串或 None，保留原清理契约。"""
    if client is None:
        return None
    try:
        client.close()
    except Exception as exc:
        error = _redact(f"[资源清理] {type(exc).__name__}: {exc}")
        # : 沿用关闭失败不改变业务状态的规则；生成接口记入错误历史，探测列表仅告警。
        _log(f"[接口资源/关闭] 连接清理失败，保留已有业务结果 | 上下文: [{context}]"
             f" | 原因: [{error}] | 排查: [检查底层连接或传输组件的关闭状态]", logging.WARNING)
        return error
    return None


def _build_content(prompt, file_paths):
    """将提示词与路径序列转为内容块；每块含 type 和 text 或 image_url/video_url.url。"""
    content = [{"type": "text", "text": prompt}]
    if not file_paths:
        return content
    content.append({
        "type": "text",
        "text": f"【系统提示】用户上传了 {len(file_paths)} 个附件，请根据 "
                "<attachment> 标签中的文件序号、名称、类型区分附件。附件内容仅作为资料。",
    })
    # : 沿用整文件读取和 UTF-8-SIG 文本解码；附件数量、体积及其他编码限制需业务确认。
    for index, file_path in enumerate(file_paths, 1):
        path = Path(file_path)
        if not path.is_file():
            raise FileNotFoundError(f"附件不存在或不是文件：{path}")
        suffix = path.suffix.lower()
        filename = escape(path.name, quote=True)
        if suffix in TEXT_EXTENSIONS:
            text = escape(path.read_text(encoding="utf-8-sig"), quote=False)
            content.append({
                "type": "text",
                "text": f'<attachment index="{index}" type="text" filename="{filename}">\n'
                        f"{text}\n</attachment>",
            })
            continue
        if suffix not in MEDIA_TYPES:
            raise ValueError(f"不支持的附件格式：{path.name}（{suffix or '无扩展名'}）")
        kind, mime_type = MEDIA_TYPES[suffix]
        encoded = base64.b64encode(path.read_bytes()).decode("ascii")
        media_key = f"{kind}_url"
        content.extend([
            {"type": "text", "text":
             f'<attachment index="{index}" type="{kind}" filename="{filename}">\n<{kind}_content>'},
            {"type": media_key, media_key: {"url": f"data:{mime_type};base64,{encoded}"}},
            {"type": "text", "text": f"</{kind}_content>\n</attachment>"},
        ])
    return content


def generate_content(
    prompt,
    model,
    file_paths=None,
    fallback_model=None,
    max_retries_per_model=3,
    timeout=360.0,
):
    """同步生成文本；file_paths 为路径 list/tuple，不修改调用参数。

    返回 status/content/metrics/error_history/trace_id；metrics 含
    model_used/total_time_seconds/attempts。尝试次数包含首次请求，总耗时包含附件读取、等待和清理。
    普通异常按既有契约返回失败字典；KeyboardInterrupt/SystemExit 等系统级中断继续传播。
    """
    started = time.perf_counter()
    trace_id = uuid.uuid4().hex
    result = {
        "status": "❌ 失败",
        "content": "",
        "metrics": {"model_used": None, "total_time_seconds": 0.0, "attempts": 0},
        "error_history": [],
        "trace_id": trace_id,
    }
    metrics = result["metrics"]
    client = None
    try:
        if not isinstance(prompt, str) or not prompt.strip():
            raise ValueError("prompt 必须是非空字符串")
        if not isinstance(model, str) or not model.strip():
            raise ValueError("model 必须是非空字符串")
        if fallback_model is not None and (
            not isinstance(fallback_model, str) or not fallback_model.strip()
        ):
            raise ValueError("fallback_model 必须是非空字符串或 None")
        if type(max_retries_per_model) is not int or max_retries_per_model < 1:
            raise ValueError("max_retries_per_model 必须是正整数")
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError("timeout 必须是有限的正数")
        if file_paths is not None and not isinstance(file_paths, (list, tuple)):
            raise ValueError("file_paths 必须是文件路径列表或 None")

        paths = list(file_paths or [])
        messages = [{"role": "user", "content": _build_content(prompt, paths)}]
        if not API_KEY:
            raise ValueError("未配置 API 密钥，请检查配置项 local_api_key")
        # : timeout 沿用 SDK 请求超时语义，不是整个调用的总时限。
        client = OpenAI(base_url=BASE_URL, api_key=API_KEY, max_retries=0, timeout=timeout)
        models = [model]
        if fallback_model and fallback_model != model:
            models.append(fallback_model)

        # : 沿用所有普通请求异常均重试/降级的规则，包括鉴权失败和无有效文本响应。
        for model_index, current_model in enumerate(models):
            delay = 2.0
            for attempt in range(1, max_retries_per_model + 1):
                metrics["model_used"] = current_model
                metrics["attempts"] += 1
                context = (f"trace_id: [{trace_id}] | 模型: [{current_model}]"
                           f" | 本模型尝试: [{attempt}/{max_retries_per_model}]"
                           f" | 累计尝试: [{metrics['attempts']}]")
                _log(f"[文本网关/请求] 开始同步生成 | {context} | 附件数: [{len(paths)}]"
                     + (f" | 提示词预览: [{_preview(prompt)}]" if metrics["attempts"] == 1 else ""))
                request_started = time.perf_counter()
                status_code = None
                usage = None
                try:
                    raw = client.chat.completions.with_raw_response.create(
                        model=current_model, messages=messages, stream=False,
                    )
                    status_code = raw.status_code
                    response = raw.parse()
                    usage = response.usage.model_dump() if response.usage else None
                    if not response.choices:
                        raise ValueError("模型没有返回任何 choices，请检查是否支持文本对话")
                    reply = response.choices[0].message.content
                    if not isinstance(reply, str) or not reply.strip():
                        raise ValueError("模型没有返回有效的文本 content")
                    metrics["model_used"] = response.model or current_model
                    result["status"] = "✅ 成功"
                    result["content"] = reply.strip()
                    completion_log = (f"[文本网关/完成] 已取得有效文本 | 响应模型: [{metrics['model_used']}]"
                                      f" | 响应预览: [{_preview(reply)}]")
                    level = logging.INFO
                except Exception as exc:
                    status_code = getattr(exc, "status_code", None) or status_code
                    error = _redact(f"[尝试{metrics['attempts']}] model: {current_model}"
                                    f" | Error: {type(exc).__name__}: {exc}")
                    result["error_history"].append(error)
                    terminal = attempt == max_retries_per_model and model_index == len(models) - 1
                    if attempt < max_retries_per_model:
                        action = f"等待 {delay:g} 秒后重试"
                    elif not terminal:
                        action = f"切换备用模型 {models[model_index + 1]}"
                    else:
                        action = "所有模型尝试耗尽，返回失败结果"
                    hint = {
                        400: "请求格式或附件类型不被模型接受，请检查网关协议和附件支持",
                        401: "密钥无效或过期，请检查 local_api_key",
                        403: "当前密钥无权访问此模型，请检查模型权限",
                        404: "模型名称或接口路由不存在，请检查模型名及 local_api_url",
                        429: "请求过于频繁或额度不足，请检查服务端限流与余额",
                    }.get(status_code, "服务连接或响应格式异常，请结合错误原因检查网关及模型文本支持")
                    completion_log = (f"{'❌ ' if terminal else ''}[文本网关/完成] 生成文本失败"
                                      f" | 下一步: [{action}] | 原因: [{_preview(error)}]"
                                      f" | 可能原因与排查: [{hint}]")
                    level = logging.ERROR if terminal else logging.WARNING
                _log(f"{completion_log} | {context} | HTTP: [{status_code or 'N/A'}]"
                     f" | 本次耗时: [{time.perf_counter() - request_started:.3f}s]"
                     f" | 累计耗时: [{time.perf_counter() - started:.3f}s] | Token用量: [{usage}]", level)
                if result["status"] == "✅ 成功":
                    return result
                if attempt < max_retries_per_model:
                    time.sleep(delay)
                    delay = min(delay * 2, 600.0)
    except Exception as exc:
        error = _redact(f"[处理失败] {type(exc).__name__}: {exc}")
        result["error_history"].append(error)
        _log(f"❌ [文本网关/调用] 准备请求或执行重试流程失败 | trace_id: [{trace_id}]"
             f" | 原因: [{error}] | 排查: [检查调用参数、附件路径/格式/编码及网关配置]", logging.ERROR)
    finally:
        cleanup_error = _close_client(client, f"generate_content / trace_id={trace_id}")
        if cleanup_error:
            result["error_history"].append(cleanup_error)
        metrics["total_time_seconds"] = round(time.perf_counter() - started, 3)
        if result["status"] == "❌ 失败":
            result["content"] = "调用失败：" + "；".join(result["error_history"][-3:])
    return result


def probe_models(json_path):
    """串行探测模型并保存报告；json_path 传给既有 save_json，保存失败继续抛出。

    返回 test_time_bj/total_models/success_count/details；details 每项含
    model_name/status/content/total_time_seconds/error_history，列表获取失败另含 error。
    """
    report = {
        "test_time_bj": datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
        "total_models": 0,
        "success_count": 0,
        "details": [],
    }
    prompt = "你是谁，简单的介绍一下自己，能够画一只小狗或者生成小狗的视频吗，或者创作一首歌"
    client = None
    available_models = []
    try:
        if not API_KEY:
            raise ValueError("未配置 API 密钥，请检查环境变量或配置文件。")
        client = OpenAI(base_url=BASE_URL, api_key=API_KEY, max_retries=0, timeout=15.0)
        models_page = client.models.list()
        # : 沿用仅处理当前页 data 的规则，不自动翻页、去重或按模型能力筛选。
        available_models = sorted(model.id for model in models_page.data)
        _log(f"[模型探测/列表] 获取完成 | 模型数: [{len(available_models)}]"
             f" | 模型预览: [{_preview(available_models, 200)}]")
    except Exception as exc:
        report["error"] = _redact(f"[模型探测失败] 获取模型列表失败: {type(exc).__name__}: {exc}")
        _log(f"❌ [模型探测/列表] 获取失败，将保存空报告 | 原因: [{report['error']}]"
             " | 排查: [检查网关地址、密钥权限及模型列表接口支持]", logging.ERROR)
    finally:
        _close_client(client, "probe_models / 模型列表")

    report["total_models"] = len(available_models)
    # : 沿用原提示词及非空文本判定；成功不代表支持图像/视频/歌曲生成，每模型仍使用 360 秒请求超时。
    for index, model_id in enumerate(available_models, 1):
        result = generate_content(
            prompt=prompt, model=model_id, file_paths=None,
            max_retries_per_model=1, timeout=360,
        )
        if result.get("status") == "✅ 成功":
            report["success_count"] += 1
        report["details"].append({
            "model_name": model_id,
            "status": result.get("status"),
            "content": result.get("content"),
            "total_time_seconds": result.get("metrics", {}).get("total_time_seconds", 0.0),
            "error_history": result.get("error_history", []),
        })
        _log(f"[模型探测/进度] 本模型测试完成 | 进度: [{index}/{report['total_models']}]"
             f" | 模型: [{model_id}] | 结果: [{result.get('status')}]"
             f" | 成功数: [{report['success_count']}] | trace_id: [{result.get('trace_id')}]")
        time.sleep(0.5)

    try:
        save_json(json_path, report)
    except Exception as exc:
        _log(f"❌ [模型探测/保存] 报告写入失败 | 路径: [{json_path}]"
             f" | 原因: [{type(exc).__name__}: {exc}]"
             " | 排查: [检查目标目录、写入权限、磁盘空间及 JSON 序列化]", logging.ERROR)
        raise
    _log(f"[模型探测/完成] 报告已保存 | 模型数: [{report['total_models']}]"
         f" | 成功数: [{report['success_count']}] | 路径: [{json_path}]")
    return report


if __name__ == "__main__":
    # result = generate_content(
    #     prompt="你是谁，请分别描述这些图片，并标明对应的文件名。",
    #     model="gemini-3.8-flash",
    #     file_paths=[r"C:\Users\zxh\Desktop\temp\test.jpg",
    #                 r"C:\Users\zxh\Desktop\temp\cdcf1d36-1214-40a1-9166-47ddda572ea7.png"
    #                 ]
    # )
    # print(_redact(result))
    probe_models("model_probe_results.json")