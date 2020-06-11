# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/9 2:15 下午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from config import sqlite_db
from gen_logger import get_logger
from database import get_conn
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

trans_logger = get_logger('trans', 1)
ori_conn, tar_conn = get_conn()

assert ori_conn is not None, 'Must specify an available database for original database/system'
assert tar_conn is not None, 'Must specify an available database for target database/system'


class SemanticTranslator:
    def __init__(self):
        self.ori_conn = ori_conn
        self.tar_conn = tar_conn
        self.logger = trans_logger
        self.sqlite_conn = sqlite3.connect(sqlite_db, check_same_thread=False)

    def translate(self, table, col, val):
        """Open two thread to translate basing on both `mapping_detail` and `virtual_coding_table`"""
        executor = ThreadPoolExecutor(max_workers=2)
        self.logger.info(">>>Start translating...")
        #
        # Since all results in two threads need to be merged together, so block brought by calling
        # the `result()` method of `Future` is no problem
        #
        f1 = executor.submit(self.__trans_from_virtual, table, col, val)
        f2 = executor.submit(self.__trans_from_physical, table, col, val)
        dict1 = f1.result()
        dict2 = f2.result()
        #
        # The translated results will be returned in dict, which contain 'state', 'msg' and
        # 'value'. The value of 'state' could be:
        #      -1: means failed translation and return original value
        #       0: means failed translation and return nothing
        #       1: means successful translation and return translated value
        #
        self.logger.info("   Merge all results together...")
        res = self.__merge(dict1, dict2)
        self.logger.info("<<<Finished")
        return res

    def __trans_from_physical(self, table, col, val):
        """Translate basing on `mapping_detail`"""
        self.logger.info("   PHY: Translating from `mapping_detail`...")
        val_ = self.__phy_single_trans(table, col, val)
        self.logger.info("   PHY: Done")
        if val_ is None:
            return {'state': 0, 'msg': 'Translation failed, return nothing', 'value': None}
        elif val_ == val:
            return {'state': -1, 'msg': 'Translation failed, original value returned', 'value': val_}
        else:
            return {'state': 1, 'msg': 'Translation success', 'value': val_}

    def __trans_from_virtual(self, table, col, val):
        """Translate basing on `virtual_coding_table`"""
        self.logger.info("   VIR: Translation from `virtual_coding_table`...")
        val_ = self.__vir_single_trans(table, col, val)
        self.logger.info("   VIR: Done")
        if val_ is None:
            return {'state': 0, 'msg': 'Translation failed, return nothing', 'value': None}
        elif val_ == val:
            return {'state': -1, 'msg': 'Translation failed, original value returned', 'value': val}
        else:
            return {'state': 1, 'msg': 'Translation success', 'value': val_}

    def __phy_single_trans(self, table, col, val):
        cr = self.sqlite_conn.cursor()
        sql = f"""
            select tar_table, tar_column, value_column, explain_col from mapping_detail
            where ori_table='{table}' and ori_column='{col}'
            """
        cr.execute(sql)
        try:
            tab, col_name, col_val, exp_col = cr.fetchone()
        except Exception as e:  # Return original value if can't get the mapping rule,
            self.logger.warning(f"   PHY: {e}")
            return val

        if self.ori_conn is self.tar_conn:  # Coded value to semantic value
            return self.__trans_single_value(self.ori_conn, tab, col_name, col_val, exp_col, col, val)
        else:  # Coded value to another coded value
            # todo
            self.logger.warning("   PHY: Can't translate it to another coded value")
            return None

    @staticmethod
    def __trans_single_value(conn, tab, col_name, col_val, exp_col, ori_col, ori_value):
        with conn.cursor() as cr:
            if isinstance(ori_value, str):
                sql = f"select {exp_col} from {tab} where {col_name}='{ori_col}' and {col_val}='{ori_value}'"
            elif isinstance(ori_value, int) or isinstance(ori_value, float):
                sql = f"select {exp_col} from {tab} where {col_name}='{ori_col}' and {col_val}={ori_value}"
            else:
                return ori_value
            cr.execute(sql)
            try:
                return cr.fetchone()[0]
            except TypeError:  # Return original value if can't be translated into explain values
                return ori_value

    def __vir_single_trans(self, table, col, val):
        cr = self.sqlite_conn.cursor()
        sql = f"select explain from virtual_coding_table where tab_name='{table}' " \
              f"and column='{col}' and value='{val}'"
        cr.execute(sql)
        try:
            val_ = cr.fetchone()[0]
        except Exception as e:  # Return original value if can't get the translated value from `virtual_coding_table`
            self.logger.warning(f"   VIR: {e}")
            return val

        if self.ori_conn is self.tar_conn:  # Coded value to semantic value
            return val_
        else:  # Coded value to another coded value
            # todo
            self.logger.warning("   PHY: Can't translate it to another coded value")
            return None

    @staticmethod
    def __merge(dict1, dict2):
        if dict1['state'] == 1 and dict2['state'] != 1:
            return dict1
        elif dict1['state'] != 1 and dict2['state'] == 1:
            return dict2
        elif dict1['state'] == -1 and dict2['state'] == 0:
            return dict1
        elif dict1['state'] == 0 and dict2['state'] == -1:
            return dict2
        elif dict1['state'] == 0 and dict2['state'] == 0:
            return dict1
        elif dict1['state'] == -1 and dict2['state'] == -1:
            return dict1
        else:  # dict1['state'] == 1 and dict2['state'] == 1
            val1, val2 = dict1['value'], dict2['value']
            if val1 == val2:
                return dict1
            else:
                return {'state': 1, 'msg': 'Multiple results', 'value': [val1, val2]}

    # def __vir_multi_trans(self, table, col):
    #     with self.ori_conn.cursor() as cr:
    #         cr.execute(f"select {col} from {table}")
    #         try:
    #             values = list(map(lambda x: x[0], cr.fetchall()))
    #         except TypeError:
    #             return None
    #     res = []
    #     with self.sqlite_conn.cursor() as cr:
    #         for v in values:
    #             cr.execute(f"select explain from virtual_coding_table where tab_name='{table}' "
    #                        f"and column='{col}' and value='{v}'")
    #             try:
    #                 val_ = cr.fetchone()[0]
    #                 if self.ori_conn is self.tar_conn:
    #                     res.append((v, val_))
    #                 else:
    #                     pass  # Coded value to another coded value
    #             except TypeError:
    #                 res.append((v, v))
    #     return res

    # def __phy_multi_trans(self, table, col):
    #     cr = self.sqlite_conn.cursor()
    #     sql = f"""
    #                 select tar_table, tar_column, value_column, explain_col from mapping_detail
    #                 where ori_table='{table}' and ori_column='{col}'
    #         """
    #     cr.execute(sql)
    #     try:
    #         tab, col_name, col_val, exp_col = cr.fetchone()
    #     except TypeError:  # Return nothing if can't get the mapping rule,
    #         return None
    #
    #     if self.ori_conn is self.tar_conn:  # Coded value to semantic value
    #         cr = self.ori_conn.cursor()
    #         cr.execute(f"select {col} from {table}")
    #         try:
    #             values = list(map(lambda x: x[0], cr.fetchall()))
    #         except TypeError:
    #             return None
    #         res = []
    #         for v in values:
    #             res.append((v, self.__trans_single_value(self.ori_conn, tab, col_name, col_val, exp_col, col, v)))
    #         return res