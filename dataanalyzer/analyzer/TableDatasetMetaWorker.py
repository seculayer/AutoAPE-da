# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from dataanalyzer.analyzer.DatasetMeta import DatasetMeta
from dataanalyzer.analyzer.table.categorical.Unique import Unique
from dataanalyzer.analyzer.table.numeric.BasicStatistics import BasicStatistics
from dataanalyzer.analyzer.table.numeric.LocalStatistics import LocalStatistics
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo


class TableDatasetMetaWorker(DatasetMeta):
    COMMON_KEYS = ["unique"]
    LOCAL_KEYS = ["local"]

    def __init__(self):
        DatasetMeta.__init__(self)
        self.meta_list: List[Dict] = list()

    def initialize(self, meta_json: Dict, job_info: DAJobInfo):
        self.meta_list: List[Dict] = meta_json.get("meta", list())

        for _ in self.meta_list:
            if _.get("field_type") == Constants.FIELD_TYPE_INT or _.get("field_type") == Constants.FIELD_TYPE_FLOAT:
                local_statistic = LocalStatistics()
                local_statistic.initialize(
                    job_info.get_instances(), float(_.get("statistics").get("basic").get("mean")))
                _["statistics"] = {
                    "local": local_statistic
                }
            else:
                _["statistics"] = {}

    def apply(self, data):
        for idx, fd in enumerate(self.meta_list):
            result, f_type = DatasetMeta._field_type(data.get(fd.get("field_nm")))

            # numeric
            if fd.get("field_type") == Constants.FIELD_TYPE_INT or fd.get("field_type") == Constants.FIELD_TYPE_FLOAT:
                if f_type is Constants.FIELD_TYPE_INT or f_type is Constants.FIELD_TYPE_FLOAT:
                    for _key in self.LOCAL_KEYS:
                        fd.get("statistics").get(_key).apply(result)

    def calculate(self):
        for meta in self.meta_list:
            if meta.get("field_type") == Constants.FIELD_TYPE_INT or \
                    meta.get("field_type") == Constants.FIELD_TYPE_FLOAT:
                for _key in self.LOCAL_KEYS:
                    meta.get("statistics").get(_key).calculate()

    def get_meta_list(self) -> List[dict]:
        for meta in self.meta_list:
            for _ in meta.get("statistics").keys():
                meta.get("statistics")[_] = meta.get("statistics").get(_).to_dict()
        return self.meta_list
