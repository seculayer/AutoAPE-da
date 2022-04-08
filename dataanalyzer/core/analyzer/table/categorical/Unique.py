# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.core.analyzer.table.Analyzer import Analyzer


class Unique(Analyzer):
    def __init__(self, num_instances: int):
        self.unique_dict = dict()
        self.unique_count = 0
        self.num_instances = num_instances
        self._is_category = True

    def apply(self, val):
        if self._is_category:
            if self.unique_dict.get(val, None) is None:
                self.unique_dict[val] = 1
                self.unique_count += 1
            else:
                self.unique_dict[val] += 1
            if self.unique_count / self.num_instances > 0.4:
                self._is_category = False

    def calculate(self):
        if not self._is_category:
            self.unique_dict = {}
            self.unique_count = 0

    def to_dict(self):
        if self._is_category:
            return {"unique": self.unique_dict, "unique_count": self.unique_count}
        else:
            return {}

    def __str__(self):
        return str(self.unique_dict)

    def is_category(self):
        return self._is_category
