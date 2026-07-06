# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:32
:last_date:
    2026/4/7 0:32
:description:
    
"""
import asyncio
import hashlib
import json
import os
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
