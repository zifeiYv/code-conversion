# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/9 2:15 下午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from config import sqlite_db


class SemanticTranslator:
    def __init__(self, ori_conn, logger):
        self.ori_conn = ori_conn
        self.logger = logger
        self.sqlite_conn = sqlite3.connect(sqlite_db)

    def translate(self, table, col, val):
        """Open two thread to translate basing on both `mapping_detail` and `virtual_coding_table`"""
        executor = ThreadPoolExecutor(max_workers=2)
        f1 = executor.submit(self.__trans_from_virtual, table, col, val)
        f2 = executor.submit(self.__trans_from_physical, table, col, val)
        # The translated results will be returned in dict, which contain 'state', 'msg' and
        # 'value'. The value of 'state' could be:
        #      -1: means failed translation and return original value
        #       0: means failed translation and return nothing
        #       1: means successful translation and return translated value
        #
        r1 = f1.result()
        r2 = f2.result()
        if r1['state'] == 1 and r2['state'] != 1:
            return r1
        elif r1['state'] != 1 and r2['state'] == 1:
            return r2
        elif r1['state'] == -1 and r2['state'] == 0:
            return r1
        elif r1['state'] == 0 and r2['state'] == -1:
            return r2
        elif r1['state'] == 0 and r2['state'] == 0:
            return r1
        elif r1['state'] == -1 and r2['state'] == -1:
            return r1
        else:  # r1['state'] == 1 and r2['state'] == 1
            if val:
                val1, val2 = r1['value'], r2['value']
                if val1 == val2:
                    return r1
                else:
                    return {'state': 1, 'msg': 'Multiple results', 'value': [val1, val2]}
            else:
                val1, val2 = r1['value'], r2['value']
                if len(val1) != len(val2):
                    return {'state': 0, 'msg': 'inconsistent results', 'value': None}
                else:
                    final_val = []
                    for i in range(len(val1)):
                        if val1[i][1] == val2[1]:
                            final_val.append(val1[i])
                        elif val1[i][1] != val1[i][0]:
                            final_val.append(val1[i])
                        else:
                            final_val.append(val2[i])
                    return {'state': 1, 'msg': 'Translation success', 'value': final_val}
                
    def __trans_from_physical(self, table, col, val):
        """Translate basing on `mapping_detail`"""
        if val:
            val_ = self.__phy_single_trans(table, col, val)
            if val_ is None:
                return {'state': 0, 'msg': 'Translation failed, return nothing', 'value': None}
            elif val_ == val:
                return {'state': -1, 'msg': 'Translation failed, original value returned', 'value': val_}
            else:
                return {'state': 1, 'msg': 'Translation success', 'value': val_}
        else:
            values = self.__phy_multi_trans(table, col)
            if not values:
                return {'state': 0, 'msg': 'Translation failed, nothing returned', 'value': None}
            else:
                if all(map(lambda x: x[0] is x[1], values)):
                    return {'state': -1, 'msg': 'Translation failed, original values returned', 'value': values}
                else:
                    return {'state': 1, 'msg': 'Translation success', 'value': values}

    def __trans_from_virtual(self, table, col, val):
        """Translate basing on `virtual_coding_table`"""
        if val:
            val_ = self.__vir_single_trans(table, col, val)
            if val_ is None:
                return {'state': 0, 'msg': 'Translation failed, return nothing', 'value': None}
            elif val_ == val:
                return {'state': -1, 'msg': 'Translation failed, original value returned', 'value': val}
            else:
                return {'state': 1, 'msg': 'Translation success', 'value': val_}
        else:
            values = self.__vir_multi_trans(table, col)
            if values is None or not values:
                return {'state': 0, 'msg': 'Translation failed, return nothing', 'value': None}
            else:
                if all(map(lambda x: x[0] is x[1], values)):
                    return {'state': -1, 'msg': 'Translation failed, original values returned', 'value': values}
                else:
                    return {'state': 1, 'msg': 'Translation success', 'value': values}

    def __phy_single_trans(self, table, col, val):
        cr = self.sqlite_conn.cursor()
        sql = f"""
            select tar_table, tar_column, value_column, explain_col from mapping_detail
            where ori_table='{table}' and ori_column='{col}'
            """
        cr.execute(sql)
        try:
            tab, col_name, col_val, exp_col = cr.fetchone()
        except TypeError:  # Return original value if can't get the mapping rule,
            return val

        if self.ori_conn is self.tar_conn:  # Coded value to semantic value
            return self.__trans_single_value(self.ori_conn, tab, col_name, col_val, exp_col, col, val)
        else:  # Coded value to another coded value
            # todo
            return None

    def __phy_multi_trans(self, table, col):
        cr = self.sqlite_conn.cursor()
        sql = f"""
                    select tar_table, tar_column, value_column, explain_col from mapping_detail
                    where ori_table='{table}' and ori_column='{col}'
            """
        cr.execute(sql)
        try:
            tab, col_name, col_val, exp_col = cr.fetchone()
        except TypeError:  # Return nothing if can't get the mapping rule,
            return None

        if self.ori_conn is self.tar_conn:  # Coded value to semantic value
            cr = self.ori_conn.cursor()
            cr.execute(f"select {col} from {table}")
            try:
                values = list(map(lambda x: x[0], cr.fetchall()))
            except TypeError:
                return None
            res = []
            for v in values:
                res.append((v, self.__trans_single_value(self.ori_conn, tab, col_name, col_val, exp_col, col, v)))
            return res

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
        except TypeError:  # Return original value if can't get the translated value from `virtual_coding_table`
            return val

        if self.ori_conn is self.tar_conn:  # Coded value to semantic value
            return val_
        else:  # Coded value to another coded value todo
            return None

    def __vir_multi_trans(self, table, col):
        with self.ori_conn.cursor() as cr:
            cr.execute(f"select {col} from {table}")
            try:
                values = list(map(lambda x: x[0], cr.fetchall()))
            except TypeError:
                return None
        res = []
        with self.sqlite_conn.cursor() as cr:
            for v in values:
                cr.execute(f"select explain from virtual_coding_table where tab_name='{table}' "
                           f"and column='{col}' and value='{v}'")
                try:
                    val_ = cr.fetchone()[0]
                    if self.ori_conn is self.tar_conn:
                        res.append((v, val_))
                    else:
                        pass  # Coded value to another coded value
                except TypeError:
                    res.append((v, v))
        return res
