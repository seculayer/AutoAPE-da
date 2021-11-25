# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import List, Dict

from dataanalyzer.common.Constants import Constants


class DatasetMeta(object):
    def __init__(self):
        self.n_rows = 0
        self.meta_dataset = dict()
        self.meta_list: List[Dict] = list()

    def initialize(self, **kwargs):
        raise NotImplementedError

    def apply(self, data):
        raise NotImplementedError

    @staticmethod
    def _field_type(data: str):
        if len(data) == 0:
            return None, Constants.FIELD_TYPE_NULL
        try:
            return int(data), Constants.FIELD_TYPE_INT
        except ValueError as ve:
            try:
                return float(data), Constants.FIELD_TYPE_FLOAT
            except ValueError as ve2:
                return data, Constants.FIELD_TYPE_STRING
