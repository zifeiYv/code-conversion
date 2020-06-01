# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/26 10:37
@Author   : Jarvis
"""
from config import target_db_info
import sqlite3
import datetime

db_filename = './mapping.db'


class FindCodingTable:
    def __init__(self):
        if target_db_info['type'].upper() == 'ORACLE':
            import cx_Oracle
            info = target_db_info['info']
            url = f"{info['host']}:{info['port']}/{info['db']}"
            user = info['user']
            password = info['password']
            try:
                self.conn = cx_Oracle.connect(user, password, url)
            except Exception as exc:
                print(exc)
                print("Can not connect to database")
                self.conn = None
        elif target_db_info['type'].upper() != 'ORACLE':
            # todo: add more database support
            self.conn = None

    def find_coding_table(self):
        if not self.conn:
            print("Unsupported database")
            return
        sqlite_conn = sqlite3.connect(db_filename)
        self._calc(sqlite_conn)

    def _calc(self, sqlite_conn):
        # todo: 完成编码表的识别代码
        pass


class Translation:
    def __init__(self, conn):
        self.sqlite_conn = sqlite3.connect(db_filename)
        self.conn = conn

    def trans(self, table, col, val):
        cr = self.sqlite_conn.cursor()
        sql = f"select tar_table, tar_column, value_column, explain_col from mapping_detail " \
            f"where ori_table='{table}' and ori_column='{col}'"
        cr.execute(sql)
        tab, val1, val2, val3 = cr.fetchone()

        with self.conn.cursor() as cr:
            # todo: 待转换的值如果不是字符类或时间类，应该更加精细化处理
            if isinstance(val, str) or isinstance(val, datetime.datetime):
                cr.execute(f"select {val3} from {tab} where {val1}='{col}' and {val2}='{val}'")
            else:
                cr.execute(f"select {val3} from {tab} where {val1}='{col}' and {val2}={val}")
            return cr.fetchone()[0]
