# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/4/7 0:32
:last_date:
    2026/4/7 0:32
:description:
    
"""
import json
import os
from pathlib import Path

from filelock import FileLock, Timeout

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
