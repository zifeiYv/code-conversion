# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/3 11:42 上午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
import sqlite3
from config import USE_RE, finish_url, sqlite_db
from database import get_conn, ori_db_type, ori_db
from gen_logger import get_logger
import os
import requests

# Add this to display Chinese normally
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

ori_conn, tar_conn = get_conn()
coded_logger = get_logger('coded', 1)


class FindCodingTable:
    def __init__(self):
        self.db_type = ori_db_type
        self.db_name = ori_db
        self.logger = coded_logger

    def find_coding_table(self):
        if USE_RE:
            self.logger.info(">>>Using regular expresion to extract mapping...")
            self.__extract_from_comments()
        else:
            self.logger.info(">>>Can't extract mapping now")
            # todo: add other method to find mapping rules
        self.__callback()

    def __extract_from_comments(self):
        self.logger.info(f'   Extracting process ID: {os.getpid()}')
        if self.db_type.upper() == 'ORACLE':
            self.logger.info("   An oracle database found")
            res = []
            with ori_conn.cursor() as cr:
                self.logger.info("   Getting the column comments...")
                try:
                    cr.execute(f"select * from user_col_comments")
                except Exception as e:
                    self.logger.error(f"   {e}")
                    self.logger.error("<<<Failed")
                    return
                self.logger.info("   Done")
                self.logger.info("   Extracting mapping information...")
                comments = cr.fetchall()
                if not comments:
                    self.logger.error("   ERROR:No available information in `user_col_comments`")
                    self.logger.error("<<<Failed")
                    return
                for i in comments:
                    if not i[2]:  # have no comments
                        continue
                    expn = self.__findall(i[2])
                    if expn:
                        for j in expn:
                            res.append([i[0], i[1], j, expn[j]])
            if not res:
                self.logger.warning("   Find nothing available")
                self.logger.warning("<<<Finished")
                return
            self.logger.info("   Done")
            self.logger.info("   Inserting mapping information into sqlite...")
            self.__insert_into_db(res)
            self.logger.info("   Done")
            self.logger.info("<<<Finished")
            return
        elif self.db_type.upper() == 'MYSQL':
            self.logger.info("   A mysql database found")
            res = []
            with ori_conn.cursor() as cr:
                self.logger.info("   Getting the column comments...")
                cr.execute(f"select table_name, column_name, column_comment from "
                           f"information_schema.columns "
                           f"where table_schema='{self.db_name}' order by table_name, column_name ")
                self.logger.info("   Done")
                self.logger.info("   Extracting mapping information...")
                for i in cr.fetchall():
                    if not i[2]:
                        continue
                    expn = self.__findall(i[2])
                    if expn:
                        for j in expn:
                            res.append([i[0], i[1], j, expn[j]])
            if not res:
                self.logger.info("   Find nothing available")
                self.logger.warning("<<<Finished")
                return
            self.logger.info("   Done")
            self.logger.info("   Inserting mapping information into sqlite...")
            self.__insert_into_db(res)
            self.logger.info("   Done")
            self.logger.info("<<<Finished")
            return
        else:
            # todo: add more database support
            pass

    @staticmethod
    def __findall(string):
        expn = {}
        from regexes import patterns
        for p in patterns:
            groups = p.findall(string)
            if len(groups) >= 2:
                for i in groups:
                    expn[i[0]] = i[2]
            return expn

    @staticmethod
    def __insert_into_db(res):
        sqlite_conn = sqlite3.connect(sqlite_db)
        cr = sqlite_conn.cursor()
        # 由于`sqlite3.connect()`没有实现上下文管理，因此不能用`with`
        sql = "insert into virtual_coding_table (tab_name, column, value, explain) " \
              "values (?, ?, ?, ?)"
        cr.executemany(sql, res)
        sqlite_conn.commit()
        sqlite_conn.close()

    @staticmethod
    def __callback():
        requests.get(finish_url)
