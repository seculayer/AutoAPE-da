# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
from dataanalyzer.core.analyzer.dataset.common.NumFeature import NumFeature
from dataanalyzer.core.analyzer.dataset.common.NumInstance import NumInstance
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo
from eda.core.analyze.FunctionInterface import FunctionInterface
from eda.core.analyze.FunctionsAbstract import FunctionsAbstract


class TableDatasetMetaChief(DatasetMetaAbstract):
    def __init__(self):
        DatasetMetaAbstract.__init__(self)

    def initialize(self, job_info: DAJobInfo, meta_json: Dict = None) -> None:
        super().initialize(job_info)

        self.field_list = job_info.get_field_list()
        for idx, field_nm in enumerate(self.field_list):
            self.meta_list.append(self._initialize_metadata(idx, field_nm))

    def _initialize_basic_dataset_meta(self, job_info: DAJobInfo) -> None:
        num_features = NumFeature()
        num_features.initialize(job_info.get_features())
        self.meta_dataset.update(num_features.to_dict())

        num_instances = NumInstance()
        num_instances.initialize(job_info.get_instances())
        self.meta_dataset.update(num_instances.to_dict())

    def apply(self, data, curr_cycle) -> None:
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

    def set_field_type(self) -> None:
        for idx, meta in enumerate(self.meta_list):
            meta["field_type"] = self.determine_type(meta.get("type_stat"))

    def set_meta_func(self) -> None:
        if len(self.meta_func_list) > 0:
            self.meta_func_list.clear()

        for idx, meta in enumerate(self.meta_list):
            field_type = meta["field_type"]

            func_cls_list = FunctionInterface.get_func_cls_list(self.eda_func_list)
            cls_dict = FunctionInterface.get_available_func_dict(func_cls_list, field_type)
            for key in cls_dict.keys():
                cls_dict[key] = cls_dict[key](num_instances=self.meta_dataset["instances"])
            self.meta_func_list.append(cls_dict)

    # Chief-Worker Statistics
    def calculate_global_meta(self, local_meta_list: List[List[Dict]], curr_cycle) -> bool:
        self.set_meta_func()

        continue_cycle_flag = False

        for idx, field_func in enumerate(self.meta_func_list):
            tmp_list = list()
            rst_dict = dict()
            for worker_meta_list in local_meta_list:
                tmp_list.append(worker_meta_list[idx])
            for _key in field_func.keys():
                meta_func_cls: FunctionsAbstract = field_func.get(_key)
                # 중복 계산 방지
                if curr_cycle > meta_func_cls.get_n_cycle():
                    continue
                meta_func_cls.global_calc(tmp_list)
                rst_dict.update(meta_func_cls.global_to_dict())
                if meta_func_cls.get_n_cycle() > curr_cycle:
                    continue_cycle_flag = True

            self.meta_list[idx]["statistics"].update(rst_dict)

        return not continue_cycle_flag

    @staticmethod
    def determine_type(type_stat: Dict) -> str:
        types = [
            Constants.FIELD_TYPE_INT,
            Constants.FIELD_TYPE_FLOAT,
            Constants.FIELD_TYPE_STRING,
            Constants.FIELD_TYPE_DATE,
            Constants.FIELD_TYPE_LIST,
        ]

        max_val = 0
        max_type = Constants.FIELD_TYPE_NULL
        for const_type in types:
            value = type_stat.get(const_type)
            if max_val < value:
                max_val = value
                max_type = const_type
        return max_type
