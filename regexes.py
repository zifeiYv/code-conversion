# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/9 3:06 下午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
# 如果字段的注释中存在该字段取值含义的内容，则使用正则表达式来进行匹配
#

import re

strings = [
    r'(\d{1,3})([\s:：\-\=]+)([^,;，；。、\s]+)'
]

patterns = [re.compile(s) for s in strings]
