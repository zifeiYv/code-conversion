# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/26 9:14
@Author   : Jarvis
"""
import os
import sqlite3

db_filename = './mapping.db'
schema_filename = './mapping_schema.sql'

db_is_new = not os.path.exists(db_filename)

conn = sqlite3.connect(db_filename)

if db_is_new:
    print("Need to create schema")
else:
    print("Database exists")
    with open(schema_filename, 'rt', encoding='utf-8') as f:
        schema = f.read()
    conn.executescript(schema)

conn.close()
