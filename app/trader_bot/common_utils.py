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

def setup_logger(log_dir="logs", app_name="BinanceBot", force_reset=False):
    """
    初始化全局日志配置。
    force_reset=True: 专为多进程子进程设计，强制清除从父进程继承的 Handler，重新绑定独立文件。
    """
    os.makedirs(log_dir, exist_ok=True)

    # 获取根记录器
    logger = logging.getLogger()

    # 【核心修改】：如果是子进程强制接管，清除原有的 Handler 以免重复打印或写错文件
    if force_reset:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()

    # 【核心安全阀】
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s,%(msecs)03d | %(levelname)s | [%(funcName)s] | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 按天切割日志 (此时每个进程拥有独立的文件，切割不再冲突)
    log_file_path = os.path.join(log_dir, f"{app_name}.log")
    file_handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when="midnight",
        interval=1,
        backupCount=30,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False

    # 【附加优化】：屏蔽 CCXT / requests 等底层库的烦人调试日志
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("ccxt").setLevel(logging.WARNING)

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
