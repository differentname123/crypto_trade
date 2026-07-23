# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:32
:last_date:
    2026/4/7 0:32
:description:
    
"""
import ast
import asyncio
import hashlib
import json
import os
import re
import socket
from pathlib import Path
from urllib.parse import urlparse

import aiohttp
import aiofiles
import httpx

from filelock import FileLock, Timeout

# common/log_config.py
import logging
import os
from logging.handlers import TimedRotatingFileHandler


def setup_logger(log_dir="logs", app_name="BinanceBot"):
    """
    初始化全局日志配置。
    支持在多个文件中重复调用，但实际只会初始化一次。
    """
    os.makedirs(log_dir, exist_ok=True)

    # 获取根记录器
    logger = logging.getLogger()

    # 【核心安全阀】：如果已经有 Handler，说明被其他文件初始化过了，直接跳过！
    # 这一步极其重要，否则你在 10 个文件里调用，一行日志就会被打印 10 次。
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    # 包含 文件名.函数名:行号 的终极溯源格式
    formatter = logging.Formatter(
        '%(asctime)s,%(msecs)03d | %(levelname)s | [%(funcName)s] | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 按天切割日志
    log_file_path = os.path.join(log_dir, f"{app_name}.log")
    file_handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    # 控制台输出
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.propagate = False

    return logger

def read_json(json_path):
    """
    读取 JSON 文件并返回内容。

    Args:
        json_path (str): JSON 文件的路径。

    Returns:
        dict: 解析后的 JSON 内容。
    """
    if not os.path.exists(json_path):
        return {}

    with open(json_path, 'r', encoding='utf-8') as f:
        try:
            data = json.load(f)
            return data
        except json.JSONDecodeError as e:
            raise ValueError(f"无法解析 JSON 文件 '{json_path}': {e}")


def save_json(json_path, data):
    dir_path = os.path.dirname(json_path)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    # 锁文件通常以 .lock 结尾
    lock_path = json_path + ".lock"

    # FileLock 会在文件系统层面创建锁，支持多进程和多线程安全
    with FileLock(lock_path):
        # 原子写入
        tmp_path = json_path + ".tmp"
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4, default=str)
        os.replace(tmp_path, json_path)


def read_file_to_str(filepath,
                     encoding: str = "utf-8",
                     errors: str = "strict") -> str:
    """
    读取文件并返回整个内容的字符串。

    参数:
        filepath: 文件路径（str 或 pathlib.Path）。
        encoding: 文本编码（默认 'utf-8'）。
        errors: 解码错误处理策略（'strict'|'replace'|'ignore' 等，默认 'strict'）。
                'strict' 会在遇到无法解码的字节时抛出 UnicodeDecodeError，
                'replace' 会用替代字符替换无法解码的字节，'ignore' 则忽略它们。

    返回:
        文件内容（str）。

    抛出:
        FileNotFoundError 如果文件不存在。
        UnicodeDecodeError 如果 decoding 失败且 errors='strict'。
    """
    p = Path(filepath)
    with p.open("r", encoding=encoding, errors=errors) as f:
        return f.read()


def string_to_object(input_str: str):
    """
    从字符串中提取并解析出 Python 列表或字典对象，设计得更加健壮。

    该函数增强了对不规范格式的容忍度，特别适合处理来自 LLM 的输出。

    核心功能：
    1.  **智能提取**: 自动在整个字符串中定位 JSON/Python 对象的边界（从第一个 '{' 或 '[' 到最后一个 '}' 或 ']），
        忽略前导和尾随的无关文本（例如 "当然，这是您要的JSON："）。
    2.  **兼容 Markdown**: 能够处理被 ```json ... ``` 代码块包裹的内容。
    3.  **错误修正**:
        - 自动移除常见的行内 (//) 和块级 (/* */) 注释。
        - 自动移除导致 JSON 解析失败的尾随逗号 (trailing commas)。
    4.  **双引擎解析**:
        - 首先尝试使用 `json.loads`，因为它更符合标准，速度更快。
        - 如果失败，则回退到 `ast.literal_eval`，以支持 Python 特有的字面量
          （如 `None`, `True`, `False` 以及单引号字符串）。

    如果无法找到或解析出有效的对象，则抛出 ValueError 异常。

    :param input_str: 包含列表或字典的输入字符串。
    :return: 解析后的 Python 列表或字典。
    :raises ValueError: 如果无法从字符串中找到或解析出有效的对象。
    :raises TypeError: 如果输入不是字符串。
    """
    # 0. 输入校验：处理 None 或非字符串输入
    if not isinstance(input_str, str):
        # 抛出 TypeError 更符合 Python 语义，但根据您的要求统一为 ValueError 也可以
        raise TypeError(f"输入必须是字符串，但收到了 {type(input_str).__name__}。")

    # 创建一个统一的错误信息生成器
    def _create_error_message(reason: str) -> str:
        # 预览原始输入的前50个字符
        preview = (input_str[:50] + '...') if len(input_str) > 50 else input_str
        return f"{reason} | 输入内容预览: '{preview}'"

    # 1. 智能提取：在字符串中寻找对象边界 (重构后，逻辑更清晰)
    first_bracket = input_str.find('[')
    first_curly = input_str.find('{')

    # 确定第一个开括号的位置
    if first_bracket == -1 and first_curly == -1:
        raise ValueError(_create_error_message("输入字符串中未找到疑似列表或字典的起始符号 '[' 或 '{'"))

    if first_bracket == -1:
        start_pos = first_curly
    elif first_curly == -1:
        start_pos = first_bracket
    else:
        start_pos = min(first_bracket, first_curly)

    # 确定最后一个闭括号的位置
    end_pos = max(input_str.rfind(']'), input_str.rfind('}'))

    if end_pos <= start_pos:
        raise ValueError(_create_error_message("未找到与起始括号匹配的结束括号 ']' 或 '}'"))

    # 提取出最可能包含对象的子字符串
    potential_obj_str = input_str[start_pos: end_pos + 1]

    # 2. 错误修正：清理提取出的字符串
    # 移除 JavaScript/JSONC 风格的注释
    potential_obj_str = re.sub(r"//.*", "", potential_obj_str)
    potential_obj_str = re.sub(r"/\*[\s\S]*?\*/", "", potential_obj_str, flags=re.MULTILINE)
    # 移除尾随逗号 (例如, [1, 2,])
    potential_obj_str = re.sub(r",\s*([}\]])", r"\1", potential_obj_str)
    cleaned_str = potential_obj_str.strip()

    # 3. 双引擎解析
    try:
        # 首先尝试使用 json.loads (更标准，通常更快)
        return json.loads(cleaned_str)
    except json.JSONDecodeError:
        # 如果 json.loads 失败，回退到 ast.literal_eval (更宽容，支持 Python 语法)
        try:
            return ast.literal_eval(cleaned_str)
        except (ValueError, SyntaxError, MemoryError) as e:
            # 如果两种方法都失败，则抛出最终的异常，并提供丰富的上下文信息
            cleaned_preview = (cleaned_str[:150] + '...') if len(cleaned_str) > 150 else cleaned_str
            error_reason = f"无法将提取的内容解析为列表或字典，解析器错误: {e}"
            # 最终的错误信息包含：原因，原始输入预览，以及尝试解析的内容预览
            raise ValueError(f"{_create_error_message(error_reason)}\n"
                             f"尝试解析的内容 (清理后): '''{cleaned_preview}'''")


def get_config(key):
    """
    从 config.json 文件中获取指定字段的值
    :param key: 配置字段名
    :return: 配置字段值
    """
    # 获取当前脚本所在目录
    base_dir = Path(os.path.dirname(os.path.abspath(__file__))).resolve().parent
    # 拼接 config.json 文件的绝对路径
    config_file = os.path.join(base_dir, 'config/config.json')

    # 检查 config.json 文件是否存在
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"配置文件 '{config_file}' 不存在，请检查文件路径。")

    # 读取配置文件
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValueError(f"配置文件 '{config_file}' 格式错误: {e}")

    # 获取指定字段的值
    if key not in config_data:
        raise KeyError(f"配置文件中缺少字段: {key}")

    return config_data[key]

async def _download_media_async(url: str, save_dir: str, proxy: str = None) -> str:
    """异步核心：使用 MD5 作为安全文件名下载文件，返回绝对路径。"""

    # 提取后缀名 (例如 .jpg, .mp4)
    suffix = Path(urlparse(url).path).suffix

    # 直接对整个 URL 进行 MD5 哈希，绝对安全，不会有任何非法字符
    safe_name = f"{hashlib.md5(url.encode('utf-8')).hexdigest()}{suffix}"

    save_path = Path(save_dir) / safe_name
    abs_path = str(save_path.resolve())

    # 文件已存在则直接返回路径
    if save_path.exists():
        return abs_path

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        async with httpx.AsyncClient(proxy=proxy, verify=False) as client:
            async with client.stream('GET', url, headers=headers, timeout=30.0) as response:
                response.raise_for_status()

                save_path.parent.mkdir(parents=True, exist_ok=True)

                async with aiofiles.open(save_path, 'wb') as f:
                    async for chunk in response.aiter_bytes():
                        await f.write(chunk)

        return abs_path

    except Exception as e:
        print(f"[ERROR] 下载失败 {url}: {e}")
        return None


def download_web_media(url: str, save_dir: str, proxy: str = None) -> str:
    """同步入口：下载网络文件并返回本地绝对路径。"""
    return asyncio.run(_download_media_async(url, save_dir, proxy=proxy))
