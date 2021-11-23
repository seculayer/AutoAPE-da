# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from dataanalyzer.analyzer.DatasetMeta import DatasetMeta
from dataanalyzer.analyzer.table.categorical.Unique import Unique
from dataanalyzer.analyzer.table.numeric.BasicStatistics import BasicStatistics
from dataanalyzer.analyzer.table.numeric.GlobalStatistics import GlobalStatistics
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo


class TableDatasetMeta(DatasetMeta):
    COMMON_KEYS = ["unique"]
    NUMERIC_KEYS = ["basic"]
    GLOBAL_KEYS = ["global"]
    STRING_KEYS = ["word"]

    def __init__(self):
        DatasetMeta.__init__(self)

        self.meta_list: List[Dict] = list()
        self.field_list = list()

    def initialize(self, job_info: DAJobInfo) -> None:
        self.field_list = job_info.get_field_list()

        for idx, field_nm in enumerate(self.field_list):
            self.meta_list.append(
                {
                    "field_nm": field_nm,
                    "field_idx": idx,
                    "field_type": None,
                    "type_stat": {
                        Constants.FIELD_TYPE_NULL: 0,
                        Constants.FIELD_TYPE_INT: 0,
                        Constants.FIELD_TYPE_FLOAT: 0,
                        Constants.FIELD_TYPE_STRING: 0,
                    },
                    "statistics": {
                        "basic": BasicStatistics(),
                        "unique": Unique()
                    }
                }
            )

    def apply(self, data) -> None:
        for idx, field_nm in enumerate(self.field_list):
            result, f_type = DatasetMeta._field_type(data.get(field_nm))
            self.meta_list[idx].get("type_stat")[f_type] += 1

            # numeric
            if f_type is Constants.FIELD_TYPE_INT or f_type is Constants.FIELD_TYPE_FLOAT:
                for _ in self.NUMERIC_KEYS:
                    self.meta_list[idx].get("statistics").get(_).apply(result)

            # string
            if f_type is Constants.FIELD_TYPE_STRING:
                for _ in self.STRING_KEYS:
                    pass

            # common
            for _ in self.COMMON_KEYS:
                self.meta_list[idx].get("statistics").get(_).apply(result)

    def calculate(self) -> None:
        for meta in self.meta_list:
            meta["field_type"] = self.determine_type(meta.get("type_stat"))

            if meta.get("field_type") == Constants.FIELD_TYPE_FLOAT:
                del meta.get("statistics")["unique"]

            if meta.get("field_type") == Constants.FIELD_TYPE_STRING:
                del meta.get("statistics")["basic"]

            for _ in meta.get("statistics"):
                meta.get("statistics").get(_).calculate()

            # end
            for _ in meta.get("statistics").keys():
                meta.get("statistics")[_] = meta.get("statistics").get(_).to_dict()

    @staticmethod
    def determine_type(type_stat: Dict) -> str:
        ft_int = type_stat.get(Constants.FIELD_TYPE_INT)
        ft_float = type_stat.get(Constants.FIELD_TYPE_FLOAT)
        ft_string = type_stat.get(Constants.FIELD_TYPE_STRING)

        if ft_int > ft_float and ft_int > ft_string:
            return Constants.FIELD_TYPE_INT
        if ft_float > ft_int and ft_float > ft_string:
            return Constants.FIELD_TYPE_FLOAT
        if ft_string > ft_int and ft_string > ft_float:
            return Constants.FIELD_TYPE_STRING
        return Constants.FIELD_TYPE_NULL

    def get_meta_list(self) -> List[dict]:
        return self.meta_list

    def calculate_global_meta(self, local_meta_list: List[List[Dict]]) -> None:
        # make global
        for idx, meta in enumerate(self.meta_list):
            ft = meta.get("field_type")
            if ft is Constants.FIELD_TYPE_INT or ft is Constants.FIELD_TYPE_FLOAT:
                gs = GlobalStatistics()
                for local_meta in local_meta_list:
                    gs.apply(local_meta[idx].get("statistics").get("local").get("local_var"))
                gs.calculate()
                meta.get("statistics")["global"] = gs.to_dict()
