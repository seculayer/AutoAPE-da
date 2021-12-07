# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.analyzer.DatasetMeta import DatasetMeta
from dataanalyzer.info.DAJobInfo import DAJobInfo


class ImageDatasetMetaChief(DatasetMeta):
    COMMON_KEYS = ["label"]

    def __init__(self):
        DatasetMeta.__init__(self)

    def initialize(self, job_info: DAJobInfo) -> None:
        pass

    def apply(self, data):
        pass
