# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from dataanalyzer.analyzer.DatasetMeta import DatasetMeta
from dataanalyzer.analyzer.dataset.common.NumFeature import NumFeature
from dataanalyzer.analyzer.dataset.common.NumInstance import NumInstance
from dataanalyzer.analyzer.table.categorical.Unique import Unique
from dataanalyzer.analyzer.table.numeric.BasicStatistics import BasicStatistics
from dataanalyzer.analyzer.table.numeric.GlobalStatistics import GlobalStatistics
from dataanalyzer.analyzer.table.string.Word import Word
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo


class TableDatasetMetaChief(DatasetMeta):
    COMMON_KEYS = ["unique"]
    NUMERIC_KEYS = ["basic"]
    GLOBAL_KEYS = ["global"]
    STRING_KEYS = ["word"]

    def __init__(self):
        DatasetMeta.__init__(self)
        self.field_list = list()
        self.meta_func_list: List[Dict] = list()

    def initialize(self, job_info: DAJobInfo) -> None:
        self.field_list = job_info.get_field_list()
        self._initialize_basic_dataset_meta(job_info)

        for idx, field_nm in enumerate(self.field_list):
            self.meta_list.append(self._initialize_metadata(idx, field_nm))
            self.meta_func_list.append(self._initialize_meta_functions(job_info))

    @staticmethod
    def _initialize_metadata(idx, field_nm) -> Dict:
        return {
            "field_nm": field_nm,
            "field_idx": idx,
            "field_type": None,
            "type_stat": {
                Constants.FIELD_TYPE_NULL: 0,
                Constants.FIELD_TYPE_INT: 0,
                Constants.FIELD_TYPE_FLOAT: 0,
                Constants.FIELD_TYPE_STRING: 0,
            },
            "statistics": dict(),
        }

    @staticmethod
    def _initialize_meta_functions(job_info: DAJobInfo) -> Dict:
        return {
            "basic": BasicStatistics(),
            "unique": Unique(job_info.get_instances()),
            "word": Word(),
        }

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
            result, f_type = DatasetMeta._field_type(data.get(field_nm))
            self.meta_list[idx].get("type_stat")[f_type] += 1

            if f_type is not Constants.FIELD_TYPE_NULL:
                # numeric
                if f_type is Constants.FIELD_TYPE_INT or f_type is Constants.FIELD_TYPE_FLOAT:
                    for _ in self.NUMERIC_KEYS:
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

            if meta.get("field_type") == Constants.FIELD_TYPE_FLOAT \
                    or meta.get("field_type") == Constants.FIELD_TYPE_INT \
                    or meta.get("field_type") == Constants.FIELD_TYPE_NULL:
                for _ in self.STRING_KEYS:
                    del self.meta_func_list[idx][_]

            if meta.get("field_type") == Constants.FIELD_TYPE_FLOAT \
                    or meta.get("field_type") == Constants.FIELD_TYPE_NULL:
                del self.meta_func_list[idx]["unique"]

            if meta.get("field_type") == Constants.FIELD_TYPE_STRING \
                    or meta.get("field_type") == Constants.FIELD_TYPE_NULL:
                del self.meta_func_list[idx]["basic"]

            for _ in meta.get("statistics"):
                self.meta_func_list[idx].get(_).calculate()

            # end
            for _ in self.meta_func_list[idx].keys():
                result_dict = self.meta_func_list[idx].get(_).to_dict()
                if len(result_dict) > 0:
                    meta.get("statistics").update(result_dict)

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
                meta.get("statistics")["global"] = gs.to_dict()

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

    def get_meta_dataset(self) -> Dict:
        return self.meta_dataset

    def calculate_field_tag(self, meta) -> List[str]:
        tag_list = list()
        # Categorical
        if meta.get("statistics").get("unique", None):
            if meta.get("statistics").get("unique", None) is not None:
                tag_list.append(Constants.TAG_CATEGORY)
        return tag_list
