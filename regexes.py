# -*- coding: utf-8 -*-
"""
@Time   : 2020/6/9 3:06 下午
@Author : sunjiawei
@E-mail : j.w.sun1992@gmail.com
"""
# Use Regular Expression to match specific parts in fields' comments which
# may contain explanation of its values. The effect depends on the generality
# of the regular expression(s).
#

import re

strings = [
    r'(\d{1,3})([\s:：\-\=]+)([^,;，；。、\s]+)'
]

patterns = [re.compile(s) for s in strings]
