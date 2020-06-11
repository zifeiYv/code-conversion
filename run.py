# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/3 10:42 上午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
from flask import Flask, jsonify, request
from concurrent.futures import ProcessPoolExecutor
from finding_utils import FindCodingTable
from translation_utils import SemanticTranslator
from gen_logger import get_logger
from database import get_conn

# 添加以下代码，以防止中文显示为"???"
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
###
trans_logger = get_logger('trans', 1)

ori_conn, tar_conn = get_conn()
assert ori_conn is not None, 'Must specify an available database for original database/system'
assert tar_conn is not None, 'Must specify an available database for target database/system'

app = Flask(__name__)

executor = ProcessPoolExecutor(1)


@app.route('/translate-to-semantic/', methods=['POST'])
def translate_to_semantic():
    """单系统编码值向标准语义转换，即解释编码值
        入参格式：
        {
            "value": {
                "table": <"待转换的表名称">,
                "column": <"待转换的表的字段">,
                "value": <"待转换字段下的值" or "">
            }
        }

    :return: A jsonify dict.
    """
    json = request.json
    tab, col, val = list(json['value'].values())
    translator = SemanticTranslator(ori_conn, trans_logger)
    res = translator.translate(tab, col, val)
    return jsonify(res)


@app.route('/find-coding-table/')
def find_coding_table():
    """另外开启一个进程来查找编码表，如果该进程中已经存在正在执行的编码查找任务，则不允许再次调用该接口"""
    fct = FindCodingTable()
    executor.submit(fct.find_coding_table)
    return jsonify({"state": 1, "msg": "正在后台进行计算编码表..."})


if __name__ == '__main__':
    app.run()
