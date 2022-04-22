# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import math
from typing import Dict

from dataanalyzer.core.analyzer.table.Analyzer import Analyzer


class LocalStatistics(Analyzer):
    def __init__(self):
        self.diff = 0.0
        self.instances = 0.0
        self.mean = 0.0
        self.local_var = 0.0

    def initialize(self, instances, mean):
        self.instances = instances
        self.mean = mean

    def apply(self, val) -> None:
        self.diff += math.pow((val - self.mean), 2)

    def calculate(self) -> None:
        self.local_var = self.diff / self.instances

    def to_dict(self) -> Dict:
        return {
            "local": {
                "local_var": self.local_var,
            }
        }
