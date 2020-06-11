# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/26 10:55
@Author   : Jarvis
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

# Turn on this to use regular expression to find explanation
USE_RE = True

# When finding task finished, request this url using GET method to tell Java
finish_url = 'http://localhost:12345'

# A sqlite database contains all data for finding mapping rules and translating
sqlite_db = './mapping.db'
