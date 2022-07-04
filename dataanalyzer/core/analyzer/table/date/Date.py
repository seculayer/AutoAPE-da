# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

from datetime import datetime
from typing import Union

from dataanalyzer.core.analyzer.table.Analyzer import Analyzer


class Date(Analyzer):
    def __init__(self):
        self.is_date = False
        self.start: Union[datetime, None] = None
        self.end: Union[datetime, None] = None
        self.date_format_list = ["%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
        self.date_format_idx = None

    def apply(self, val):
        rst: Union[datetime, None] = None

        for idx, date_format in enumerate(self.date_format_list):
            try:
                rst = datetime.strptime(val, date_format)
                self.date_format_idx = idx
                break
            except ValueError:
                pass

        if self.date_format_idx is not None:
            if self.start is None and self.end is None:
                self.start = rst
                self.end = rst
            elif self.start > rst:
                self.start = rst
            elif self.end < rst:
                self.end = rst

    def calculate(self):
        pass

    def to_dict(self):
        _start = None if not self.start else self.start.strftime(self.date_format_list[self.date_format_idx])
        _end = None if not self.end else self.end.strftime(self.date_format_list[self.date_format_idx])

        return {
            "date": {
                "start": _start,
                "end": _end
            }
        }
