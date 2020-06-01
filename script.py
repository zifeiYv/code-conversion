# -*- coding: utf-8 -*-
"""
@Statement: Sorry for this shit code 
@Time     : 2020/5/25 9:03
@Author   : Jarvis
"""
import cx_Oracle

"""
仅用于测试，后续将集成到代码表发现的代码中去
"""

# Connect to database
url = '191.168.7.150:1521/bj'
user = 'tc'
password = 'tc'
conn = cx_Oracle.connect(user, password, url)
cr = conn.cursor()

# Get all contents in `code_sort_id` of `p_code`
col_name = 'code_sort_id'
cr.execute(f"select distinct {col_name} from p_code")
code_names = list(map(lambda x: x[0], cr.fetchall()))

# Get all tables and their columns except `p_code`
cr.execute(f"select table_name,column_name from user_tab_columns order by table_name")
res = cr.fetchall()
tab_col_dict = {}
for i in res:
    tab = i[0]
    if tab.upper() == 'P_CODE':
        continue
    if tab in tab_col_dict:
        tab_col_dict[tab].append(i[1])
    else:
        tab_col_dict[tab] = [i[1]]

# Use `for` loop to find coding fields
matching = {}
for i in code_names:
    for tab in tab_col_dict:
        if i.upper() in tab_col_dict[tab]:
            if tab in matching:
                matching[tab].append(i.upper())
            else:
                matching[tab] = [i.upper()]
