# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import math
from typing import Dict

from dataanalyzer.core.analyzer.table.Analyzer import Analyzer


class GlobalStatistics(Analyzer):
    def __init__(self):
        self.diff_n = 0.0
        self.variance = 0.0
        self.stddev = 0.0

    def apply(self, val) -> None:
        self.diff_n += val

    def calculate(self) -> None:
        self.variance = self.diff_n
        self.stddev = math.sqrt(self.variance)

    def to_dict(self) -> Dict:
        return {
            "global": {
                "variance": self.variance,
                "std_dev": self.stddev
            }
        }
