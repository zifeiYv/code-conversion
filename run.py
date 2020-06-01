# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/25 10:49
@Author   : Jarvis
"""
from flask import Flask, jsonify, request
from concurrent.futures import ProcessPoolExecutor
from utils import FindCodingTable, Translation
from config import target_db_info

if target_db_info['type'].upper() == 'ORACLE':
    import cx_Oracle
    info = target_db_info['info']
    url = f"{info['host']}:{info['port']}/{info['db']}"
    user = info['user']
    password = info['password']
    orcl_conn = cx_Oracle.connect(user, password, url)

app = Flask(__name__)

executor = ProcessPoolExecutor(1)


@app.route('/translate/', methods=['POST'])
def translate():
    """单系统编码值向标准语义转换
        入参格式：
            {
                "table": "<str, 待转换的表名称>",
                "column": "<str, 待转换的表的字段>",
                "value": "<待转换字段下的值>"
            }
    """
    json = request.json
    tab, col, val = list(json.values())
    translator = Translation(orcl_conn)
    val = translator.trans(tab, col, val)
    return jsonify({'state': 1, 'msg': val})


@app.route('/find-code-table/')
def find_coding_tab():
    """另外开启一个进程来查找编码表"""
    fct = FindCodingTable()
    executor.submit(fct.find_coding_table)
    return jsonify({"state": 1, "msg": "正在后台进行计算编码表..."})


if __name__ == '__main__':
    app.run()
