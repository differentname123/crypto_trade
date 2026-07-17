# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/6/13 7:16
:last_date:
    2026/6/13 7:16
:description:
    
"""
import json
from pathlib import Path

import pandas as pd
import os
import logging
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

# 提供一个辅助函数，仅用于日志打印时将毫秒时间戳转为人类可读的北京时间
def format_ts_to_bj(ms_timestamp):
    return pd.to_datetime(ms_timestamp, unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai').strftime(
        '%Y-%m-%d %H:%M:%S')


def get_config(key):
    """
    从 config.json 文件中获取指定字段的值
    :param key: 配置字段名
    :return: 配置字段值
    """
    # 获取当前脚本所在目录
    base_dir = Path(os.path.dirname(os.path.abspath(__file__))).resolve()
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
