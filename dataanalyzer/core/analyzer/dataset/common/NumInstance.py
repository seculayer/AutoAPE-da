# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.core.analyzer.dataset.DatasetMetaFeatureAbstract import DatasetMetaFeatureAbstract


class NumInstance(DatasetMetaFeatureAbstract):
    def __init__(self):
        DatasetMetaFeatureAbstract.__init__(self)
        self.instance = 0

    def initialize(self, instances):
        self.instance = instances

    def apply(self, data):
        self.instance += 1

    def to_dict(self):
        return {"instances": self.instance}
