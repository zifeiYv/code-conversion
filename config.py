# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/9 5:06 下午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
db_info = {
    "ori_db_info": {
        "type": "oracle",
        "info": {
            "host": "191.168.7.150",
            "port": 1521,
            "user": "tc",
            "password": "tc",
            "db": "bj"
        }
    },
    "tar_db_info": {
        "type": "oracle",
        "info": {
            "host": "191.168.7.150",
            "port": 1521,
            "user": "tc",
            "password": "tc",
            "db": "bj"
        }
    }
}

# 使用正则表达式发现编码值与业务含义的映射
USE_RE = True

# 由于发现编码转换规则的过程可能耗时过长，因此在收到计算转换规则的请求后使用另外的进程进行计算，
# 计算完成后，以GET的方式调用这个接口，提示后端已经完成计算。
finish_url = 'http://localhost:12345'

# sqlite数据库，用于存储发现的映射规则「临时方案」。
sqlite_db = './mapping.db'
