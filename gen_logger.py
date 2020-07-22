# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/4/21 11:12
@Author   : Jarvis
"""
import logging
from logging.handlers import RotatingFileHandler
from logging import StreamHandler
import os


def get_logger(log_file_name, console_print=False):
    path = os.path.join('./', 'logs')
    if not os.path.exists(path):
        os.makedirs(path)
    log_file_name = os.path.join(path, log_file_name + '.log')
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:  # 避免重复添加handler
        console = StreamHandler()
        handler = RotatingFileHandler(log_file_name, maxBytes=2*1024*1024, backupCount=5)
        formatter = logging.Formatter("%(asctime)s %(filename)15s %(lineno)4s"
                                      " %(levelname)7s| %(message)s ",
                                      datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        console.setFormatter(formatter)
        logger.addHandler(handler)
        if console_print:
            logger.addHandler(console)  # 注释掉此行，以避免在控制台打印日志信息
    return logger
