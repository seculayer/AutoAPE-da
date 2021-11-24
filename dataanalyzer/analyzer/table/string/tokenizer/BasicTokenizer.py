# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import re
from typing import List


class BasicTokenizer(object):
    def __init__(self):
        pass

    def tokenize(self, value: str) -> List[str]:
        data = value.lower()
        key_set = list(set(re.findall(r'[^a-zA-z0-9]|[_]', data)))
        for key in key_set:
            if key != " ":
                data = data.replace(str(key), ' ' + str(key) + ' ').replace("  ", " ")
        data = data.strip()
        data = data.split(" ")
        return data
