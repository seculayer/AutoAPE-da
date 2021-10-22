# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

from typing import Dict
import json


# class : JSONUtils
class JSONUtils(object):
    @staticmethod
    def read_json_from_file(filename: str) -> Dict:
        result_dict = dict()
        with open(filename, "r") as f:
            result_dict = json.loads(f.read())
        return result_dict
