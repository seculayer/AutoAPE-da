# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import List, Dict
from datetime import datetime

from dataanalyzer.core.analyzer.dataset.common.NumInstance import NumInstance
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo
from eda.core.analyze.FunctionInterface import FunctionInterface
from eda.core.analyze.FunctionsAbstract import FunctionsAbstract


class DatasetMetaAbstract(object):
    def __init__(self):
        self.n_rows = 0
        self.meta_dataset = dict()
        self.field_list = list()

        self.meta_list: List[Dict] = list()
        self.meta_func_list: List[Dict] = list()
        self.eda_func_list = FunctionInterface.get_func_name_list()

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

    def apply(self, data, curr_cycle) -> None:
        raise NotImplementedError

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
        if (data[0] == "[" and data[-1] == "]") or isinstance(data, list):
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
            local_meta_func = self.meta_func_list[idx]
            for _key in local_meta_func:
                result_dict = local_meta_func.get(_key).local_to_dict()
                meta.get("statistics").update(result_dict)
        return self.meta_list

    # for chief
    def set_meta_func(self):
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
                # 필요값이 없을 경우(필요 cycle에 도달하지 못한 경우) 및 중복 계산 방지
                if not curr_cycle == meta_func_cls.get_n_cycle():
                    pass
                else:
                    # import datetime
                    # print(f"{_key}")
                    # start_time = datetime.datetime.now()
                    meta_func_cls.global_calc(tmp_list)
                    # print(f"{curr_cycle} - {meta_func_cls.__class__} 걸린 시간 : {datetime.datetime.now() - start_time}")
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
