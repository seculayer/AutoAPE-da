# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.analyzer.dataset.DatasetMetaFeatureAbstract import DatasetMetaAbstract


class NumFeature(DatasetMetaAbstract):
    def __init__(self):
        DatasetMetaAbstract.__init__(self)
        self.features = 0

    def initialize(self, features):
        self.features = features

    def apply(self, data):
        self.features += 1

    def to_dict(self):
        return {"features": self.features}
