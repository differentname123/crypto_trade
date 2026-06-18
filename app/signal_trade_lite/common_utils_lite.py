# -- coding: utf-8 --
""":authors:
    zhuxiaohu
:create_date:
    2026/6/13 7:16
:last_date:
    2026/6/13 7:16
:description:
    
"""
import pandas as pd
import os
import logging
from logging.handlers import TimedRotatingFileHandler
def setup_logger(log_dir="logs"):
    """
    初始化并返回按天切割的日志记录器。

    :param log_dir: 日志文件存储的相对或绝对目录 (默认: "logs")
    :return: logging.Logger 实例
    """
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("QuantBot")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s,%(msecs)03d | %(levelname)s | [BOT] %(message)s',
                                      datefmt='%Y-%m-%d %H:%M:%S')

        log_file_path = os.path.join(log_dir, "trading_bot.log")
        file_handler = TimedRotatingFileHandler(
            filename=log_file_path, when="midnight", interval=1, backupCount=30, encoding='utf-8'
        )
        file_handler.setFormatter(formatter)

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