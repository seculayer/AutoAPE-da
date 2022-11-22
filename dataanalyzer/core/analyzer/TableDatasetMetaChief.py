# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
from dataanalyzer.core.analyzer.dataset.common.NumFeature import NumFeature
from dataanalyzer.core.analyzer.dataset.common.NumInstance import NumInstance
from dataanalyzer.core.analyzer.table.numeric.GlobalStatistics import GlobalStatistics
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo


class TableDatasetMetaChief(DatasetMetaAbstract):
    COMMON_KEYS = ["unique"]
    NUMERIC_KEYS = ["basic"]
    GLOBAL_KEYS = ["global"]
    STRING_KEYS = ["word"]
    DATE_KEYS = ["date"]

    def __init__(self):
        DatasetMetaAbstract.__init__(self)

    def initialize(self, job_info: DAJobInfo, meta_json: Dict = None) -> None:
        super().initialize(job_info)

        self.field_list = job_info.get_field_list()
        for idx, field_nm in enumerate(self.field_list):
            self.meta_list.append(self._initialize_metadata(idx, field_nm))
            self.meta_func_list.append(self._initialize_meta_functions(job_info, None))

    def _initialize_basic_dataset_meta(self, job_info: DAJobInfo) -> None:
        num_features = NumFeature()
        num_features.initialize(job_info.get_features())
        self.meta_dataset.update(num_features.to_dict())

        num_instances = NumInstance()
        num_instances.initialize(job_info.get_instances())
        self.meta_dataset.update(num_instances.to_dict())

    def apply(self, data) -> None:
        # dataset meta data
        self._apply_dataset_meta(data)

        # field meta data
        self._apply_field_meta(data)

    def _apply_dataset_meta(self, data) -> None:
        pass

    def _apply_field_meta(self, data) -> None:
        for idx, field_nm in enumerate(self.field_list):
            result, f_type = DatasetMetaAbstract.field_type(data.get(field_nm))
            self.meta_list[idx].get("type_stat")[f_type] += 1

            if f_type is not Constants.FIELD_TYPE_NULL:
                # numeric
                if f_type is Constants.FIELD_TYPE_INT or f_type is Constants.FIELD_TYPE_FLOAT:
                    for _ in self.NUMERIC_KEYS:
                        self.meta_func_list[idx].get(_).apply(result)

                # date
                if f_type is Constants.FIELD_TYPE_DATE:
                    for _ in self.DATE_KEYS:
                        self.meta_func_list[idx].get(_).apply(result)

                # string
                if f_type is Constants.FIELD_TYPE_STRING:
                    for _ in self.STRING_KEYS:
                        self.meta_func_list[idx].get(_).apply(result)

                # common
                for _ in self.COMMON_KEYS:
                    self.meta_func_list[idx].get(_).apply(result)

    # One-Cycle Statistics
    def calculate(self) -> None:
        for idx, meta in enumerate(self.meta_list):
            meta["field_type"] = self.determine_type(meta.get("type_stat"))

            if meta.get("field_type") == Constants.FIELD_TYPE_FLOAT:
                for _ in self.STRING_KEYS:
                    del self.meta_func_list[idx][_]
                del self.meta_func_list[idx]["unique"]
                del self.meta_func_list[idx]["date"]
            elif meta.get("field_type") == Constants.FIELD_TYPE_INT:
                for _ in self.STRING_KEYS:
                    del self.meta_func_list[idx][_]
                del self.meta_func_list[idx]["date"]
            elif meta.get("field_type") == Constants.FIELD_TYPE_STRING:
                del self.meta_func_list[idx]["basic"]
                del self.meta_func_list[idx]["date"]
            elif meta.get("field_type") == Constants.FIELD_TYPE_DATE:
                del self.meta_func_list[idx]["basic"]
                del self.meta_func_list[idx]["unique"]
            elif meta.get("field_type") == Constants.FIELD_TYPE_NULL:
                for _ in self.STRING_KEYS:
                    del self.meta_func_list[idx][_]
                del self.meta_func_list[idx]["basic"]
                del self.meta_func_list[idx]["unique"]

            self._statistic_calculate(idx, meta)

            # tag
            meta["field_tag"] = self.calculate_field_tag(meta)

    # Chief-Worker Statistics
    def calculate_global_meta(self, local_meta_list: List[List[Dict]]) -> None:
        # make global
        for idx, meta in enumerate(self.meta_list):
            ft = meta.get("field_type")
            if ft is Constants.FIELD_TYPE_INT or ft is Constants.FIELD_TYPE_FLOAT:
                gs = GlobalStatistics()
                for local_meta in local_meta_list:
                    gs.apply(local_meta[idx].get("statistics").get("local").get("local_var"))
                gs.calculate()
                meta.get("statistics").update(gs.to_dict())

    @staticmethod
    def determine_type(type_stat: Dict) -> str:
        ft_int = type_stat.get(Constants.FIELD_TYPE_INT)
        ft_float = type_stat.get(Constants.FIELD_TYPE_FLOAT)
        ft_string = type_stat.get(Constants.FIELD_TYPE_STRING)
        ft_date = type_stat.get(Constants.FIELD_TYPE_DATE)

        if ft_int > ft_float and ft_int > ft_string and ft_int > ft_date:
            return Constants.FIELD_TYPE_INT
        if ft_float > ft_int and ft_float > ft_string and ft_float > ft_date:
            return Constants.FIELD_TYPE_FLOAT
        if ft_string > ft_int and ft_string > ft_float and ft_string > ft_date:
            return Constants.FIELD_TYPE_STRING
        if ft_date > ft_int and ft_date > ft_float and ft_date > ft_string:
            return Constants.FIELD_TYPE_DATE
        return Constants.FIELD_TYPE_NULL

    @staticmethod
    def calculate_field_tag(meta) -> List[str]:
        tag_list = list()
        # Categorical
        if meta.get("statistics").get("unique", None):
            if meta.get("statistics").get("unique", None) is not None:
                tag_list.append(Constants.TAG_CATEGORY)
        return tag_list
