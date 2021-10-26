# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.analyzer.table.Analyzer import Analyzer


class Unique(Analyzer):
    def __init__(self):
        self.unique_dict = dict()

    def apply(self, val):
        if self.unique_dict.get(val, None) is None:
            self.unique_dict[val] = 1
        else:
            self.unique_dict[val] += 1

    def calculate(self):
        pass

    def to_dict(self):
        return self.unique_dict

    def __str__(self):
        return str(self.unique_dict)
