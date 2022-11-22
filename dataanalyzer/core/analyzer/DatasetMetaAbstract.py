# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import List, Dict
from datetime import datetime

from dataanalyzer.core.analyzer.dataset.common.NumInstance import NumInstance
from dataanalyzer.core.analyzer.image.ImageShape import ImageShape
from dataanalyzer.core.analyzer.table.categorical.Unique import Unique
from dataanalyzer.core.analyzer.table.numeric.BasicStatistics import BasicStatistics
from dataanalyzer.core.analyzer.table.date.Date import Date
from dataanalyzer.core.analyzer.table.string.Word import Word
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo


class DatasetMetaAbstract(object):
    LOCAL_KEYS = []

    def __init__(self):
        self.n_rows = 0
        self.meta_dataset = dict()
        self.field_list = list()

        self.meta_list: List[Dict] = list()
        self.meta_func_list: List[Dict] = list()

    def initialize(self, job_info: DAJobInfo, meta_json: Dict = None):
        self._initialize_basic_dataset_meta(job_info)

    def _initialize_basic_dataset_meta(self, job_info: DAJobInfo) -> None:
        num_instances = NumInstance()
        num_instances.initialize(job_info.get_instances())
        self.meta_dataset.update(num_instances.to_dict())

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
                Constants.FIELD_TYPE_DATE: 0,
                Constants.FIELD_TYPE_LIST: 0,
            },
            "statistics": dict(),
        }

    @staticmethod
    def _initialize_image_metadata(idx, field_nm) -> Dict:
        return {
            "field_nm": field_nm,
            "field_idx": idx,
            "field_type": Constants.FIELD_TYPE_IMAGE,
            "statistics": dict(),
        }

    def _initialize_meta_functions(self, job_info: DAJobInfo, meta) -> Dict:
        return {
            "basic": BasicStatistics(),
            "unique": Unique(job_info.get_instances()),
            "word": Word(),
            "date": Date()
        }

    @staticmethod
    def _initialize_image_meta_functions(job_info: DAJobInfo) -> Dict:
        return {
            "size": ImageShape(),
        }

    @staticmethod
    def _initialize_label_meta_functions(job_info: DAJobInfo) -> Dict:
        return {
            "unique": Unique(job_info.get_instances()),
        }

    def apply(self, data):
        raise NotImplementedError

    def calculate(self):
        raise NotImplementedError

    def _statistic_calculate(self, idx, meta):
        # end
        for _ in self.meta_func_list[idx].keys():
            self.meta_func_list[idx].get(_).calculate()
            result_dict = self.meta_func_list[idx].get(_).to_dict()
            if len(result_dict) > 0:
                meta.get("statistics").update(result_dict)

    def get_meta_list(self) -> List[dict]:
        return self.meta_list

    def get_meta_dataset(self) -> Dict:
        return self.meta_dataset

    @staticmethod
    def field_type(data: str):
        date_format_list = ["%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]
        if data is None or len(data) == 0:
            return None, Constants.FIELD_TYPE_NULL

        # date
        date_flag = False
        for date_format in date_format_list:
            try:
                datetime.strptime(data, date_format)
                date_flag = True
                break
            except ValueError:
                pass
        # list
        list_flag = False
        if data[0] == "[" and data[-1] == "]":
            list_flag = True

        if date_flag:
            return data, Constants.FIELD_TYPE_DATE
        elif list_flag:
            return data, Constants.FIELD_TYPE_LIST
        else:
            try:
                return int(data), Constants.FIELD_TYPE_INT
            except ValueError:
                try:
                    return float(data), Constants.FIELD_TYPE_FLOAT
                except ValueError:
                    return data, Constants.FIELD_TYPE_STRING

    def get_meta_list_for_worker(self) -> List[dict]:
        for idx, meta in enumerate(self.meta_list):
            if meta.get("field_type") == Constants.FIELD_TYPE_INT or \
               meta.get("field_type") == Constants.FIELD_TYPE_FLOAT:
                for _ in self.LOCAL_KEYS:
                    result_dict = self.meta_func_list[idx].get(_).to_dict()
                    meta.get("statistics").update(result_dict)
        return self.meta_list

    def calculate_global_meta(self, local_meta_list: List[List[Dict]]) -> None:
        return None
