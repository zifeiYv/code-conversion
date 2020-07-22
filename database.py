# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/3 10:42 上午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
from config import db_info
ori_db_info = db_info['ori_db_info']
tar_db_info = db_info['tar_db_info']
ori_db_type = db_info['ori_db_info']['type']
ori_db = db_info['ori_db_info']['info']['db']

tar_db_type = db_info['tar_db_info']['type']


def get_conn():
    """根据配置文件中的信息，获取两个连接对象。

    Returns:
        ori_conn: 待转换的编码值所在的数据库
        tar_conn: 目标值所在的数据库，如果与ori_conn相同，则说明是同系统转换（编码值->业务含义）

    """
    if ori_db_info['type'].upper() == 'ORACLE':
        import cx_Oracle
        info = ori_db_info['info']
        url = f"{info['host']}:{info['port']}/{info['db']}"
        user = info['user']
        password = info['password']
        ori_conn = cx_Oracle.connect(user, password, url)
    else:
        # todo: add more database support
        ori_conn = None

    if tar_db_info == {} or tar_db_info == ori_db_info:
        tar_conn = ori_conn
    else:
        # todo: add more database support
        tar_conn = None
    return ori_conn, tar_conn
