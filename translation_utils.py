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
    """定义一个用于转换编码值的类"""
    def __init__(self):
        self.ori_conn = ori_conn
        self.tar_conn = tar_conn
        self.logger = trans_logger
        self.sqlite_conn = sqlite3.connect(sqlite_db, check_same_thread=False)

    def translate(self, table, col, val):
        """开启两个线程进行编码转换。

        涉及到两个概念：虚拟表与物理表。

        物理表指的是存储从客观字典表中提取到的编码映射规则信息的表；
        虚拟表指的是存储从注释中提取的编码映射规则信息的表，是相对于物理表而言的。
        """
        executor = ThreadPoolExecutor(max_workers=2)
        self.logger.info(">>>开始转换...")
        # 向第一个线程中提交从虚拟表中获取翻译结果的任务
        f1 = executor.submit(self.__trans_from_virtual, table, col, val)
        # 向第二个线程中提交从物理表中获取翻译结果的任务
        f2 = executor.submit(self.__trans_from_physical, table, col, val)
        dict1 = f1.result()
        dict2 = f2.result()
        #
        # 翻译的结果以字典的形式返回，其键为'state'/'msg'/'value'。其中，'state'的取值及其含义为：
        #   -1: 翻译失败，返回原始值
        #    0: 翻译失败，返回None
        #    1: 翻译成功，返回翻译后的值
        self.logger.info("   融合转换结果...")
        res = self.__merge(dict1, dict2)
        self.logger.info("<<<完成")
        return res

    def __trans_from_physical(self, table, col, val):
        """基于物理表，对给定的数据尝试进行翻译"""
        self.logger.info("   PHY: 从`mapping_detail`表进行转换...")
        val_ = self.__phy_single_trans(table, col, val)
        self.logger.info("   PHY: 完成")
        if val_ is None:
            return {'state': 0, 'msg': 'Translation failed, return nothing', 'value': None}
        elif val_ == val:
            return {'state': -1, 'msg': 'Translation failed, original value returned', 'value': val_}
        else:
            return {'state': 1, 'msg': 'Translation success', 'value': val_}

    def __trans_from_virtual(self, table, col, val):
        """基于虚拟表，对给定的数据尝试进行翻译"""
        self.logger.info("   VIR: 从`virtual_coding_table`表进行转换...")
        val_ = self.__vir_single_trans(table, col, val)
        self.logger.info("   VIR: 完成")
        if val_ is None:
            return {'state': 0, 'msg': 'Translation failed, return nothing', 'value': None}
        elif val_ == val:
            return {'state': -1, 'msg': 'Translation failed, original value returned', 'value': val}
        else:
            return {'state': 1, 'msg': 'Translation success', 'value': val_}

    def __phy_single_trans(self, table, col, val):
        """对单个值进行翻译"""
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
            return self.__trans_single_value(self.ori_conn, tab, col_name, col_val, exp_col, col,
                                             val)
        else:  # Coded value to another coded value
            # todo
            self.logger.warning("   PHY: Can't translate it to another coded value")
            return None

    @staticmethod
    def __trans_single_value(conn, tab, col_name, col_val, exp_col, ori_col, ori_value):
        """从原始库中获取到原始值，对照物理表进行翻译"""
        with conn.cursor() as cr:
            if isinstance(ori_value, str):
                sql = f"select {exp_col} from {tab} where {col_name}='{ori_col}' and " \
                      f"{col_val}='{ori_value}'"
            elif isinstance(ori_value, int) or isinstance(ori_value, float):
                sql = f"select {exp_col} from {tab} where {col_name}='{ori_col}' and " \
                      f"{col_val}={ori_value}"
            else:
                return ori_value
            cr.execute(sql)
            try:
                return cr.fetchone()[0]
            except TypeError:  # Return original value if can't be translated into explain values
                return ori_value

    def __vir_single_trans(self, table, col, val):
        """对单个值进行翻译"""
        cr = self.sqlite_conn.cursor()
        sql = f"select explain from virtual_coding_table where tab_name='{table}' " \
              f"and column='{col}' and value='{val}'"
        cr.execute(sql)
        try:
            val_ = cr.fetchone()[0]
        except Exception as e:
            # Return original value if can't get the translated value from `virtual_coding_table`
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
        """将从物理表获取的结果与从虚拟表获取的结果进行融合"""
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
