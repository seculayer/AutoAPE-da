# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
# from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo
from eda.core.analyze.FunctionInterface import FunctionInterface
from eda.core.analyze.FunctionsAbstract import FunctionsAbstract


class TableDatasetMetaWorker(DatasetMetaAbstract):
    def __init__(self, worker_idx):
        DatasetMetaAbstract.__init__(self)
        self.worker_idx = int(worker_idx)

    def initialize(self, job_info: DAJobInfo, meta_json: Dict = None):
        self.meta_list: List[Dict] = meta_json.get("meta", list())
        self.dataset_meta_list: List[Dict] = meta_json.get("dataset_meta_list", list())
        if len(self.dataset_meta_list) == 0:
            for _ in range(len(self.meta_list)):
                self.dataset_meta_list.append({})

        field_type_list = list()
        for meta in self.meta_list:
            field_type_list.append(meta.get("field_type"))

        if len(self.meta_func_list) > 0:
            self.meta_func_list.clear()

        for idx, _ in enumerate(self.meta_list):
            self.meta_func_list.append(self._initialize_meta_functions(job_info, _))

        if len(self.dataset_func_list) > 0:
            self.dataset_func_list.clear()

        for idx, _ in enumerate(self.meta_list):
            self.dataset_func_list.append(self._initialize_dataset_functions(job_info, _, field_type_list))

    def _initialize_meta_functions(self, job_info: DAJobInfo, meta) -> Dict:
        field_type = meta.get("field_type")
        # worker_n_instances = self._get_worker_n_instance(job_info)

        func_cls_list = FunctionInterface.get_func_cls_list("eda.core.analyze.functions.monomial", self.monomial_func_list)
        cls_dict = FunctionInterface.get_available_func_dict(func_cls_list, field_type)
        for key in cls_dict.keys():
            # cls_dict[key] = cls_dict[key](num_instances=worker_n_instances)
            cls_dict[key] = cls_dict[key](num_instances=job_info.get_instances())

        return cls_dict

    def _initialize_dataset_functions(self, job_info: DAJobInfo, meta, field_type_list) -> Dict:
        target_field_type = meta.get("field_type")

        func_cls_list = FunctionInterface.get_func_cls_list("eda.core.analyze.functions.binomial", self.binomial_func_list)
        cls_dict = FunctionInterface.get_available_func_dict(func_cls_list, target_field_type)
        for key in cls_dict.keys():
            cls_list = list()
            for field_type in field_type_list:
                tmp_cls_dict = FunctionInterface.get_available_func_dict(func_cls_list, field_type)
                if tmp_cls_dict.__contains__(key):
                    cls_list.append(tmp_cls_dict[key](num_instances=job_info.get_instances()))
                else:
                    cls_list.append(None)
            cls_dict[key] = cls_list

        return cls_dict

    # def _get_worker_n_instance(self, job_info: DAJobInfo):
    #     total_n_instances = job_info.get_instances()
    #     n_workers = total_n_instances // Constants.DISTRIBUTE_INSTANCES_TABLE + 1
    #
    #     workers_instance = total_n_instances // n_workers
    #     if total_n_instances % n_workers >= self.worker_idx + 1:
    #         workers_instance += 1
    #
    #     return workers_instance

    def apply(self, data, curr_cycle):
        field_list = [field_dict.get("field_nm") for field_dict in self.meta_list]
        # import datetime
        for idx, fd in enumerate(self.meta_list):
            # print(f'{fd.get("field_nm")}')
            for _key in self.meta_func_list[idx].keys():
                meta_func_cls: FunctionsAbstract = self.meta_func_list[idx].get(_key)
                # 필요값이 없을 경우(필요 cycle에 도달하지 못한 경우) 및 중복 계산 방지
                if not curr_cycle == meta_func_cls.get_n_cycle() - 1:
                    continue
                # start_time = datetime.datetime.now()
                meta_func_cls.local_calc(data.get(fd.get("field_nm")), self.meta_list[idx].get("statistics", {}))
                # print(f"{curr_cycle} - {meta_func_cls.__class__} 걸린 시간 : {datetime.datetime.now() - start_time}")

            for dataset_func_key in self.dataset_func_list[idx].keys():
                for sub_idx, dataset_func in enumerate(self.dataset_func_list[idx][dataset_func_key]):
                    # 필요값이 없을 경우(필요 cycle에 도달하지 못한 경우) 및 중복 계산 방지
                    if dataset_func is None or not curr_cycle == dataset_func.get_n_cycle() - 1:
                        continue

                    dataset_func.local_calc(
                        [data.get(fd.get("field_nm")), data.get(field_list[sub_idx])],
                        [dict(self.meta_list[idx].get("statistics", {}), **self.dataset_meta_list[idx]),
                         dict(self.meta_list[sub_idx].get("statistics", {}), **self.dataset_meta_list[sub_idx])],
                        sub_idx
                    )

    def check_end(self, curr_cycle) -> bool:
        is_continue = False
        for idx, fd in enumerate(self.meta_list):
            for _key in self.meta_func_list[idx].keys():
                n_cycle = self.meta_func_list[idx].get(_key).get_n_cycle()
                if curr_cycle + 1 < n_cycle:
                    is_continue = True

        return not is_continue

    def get_meta_list(self) -> List[dict]:
        self.set_meta_list_for_worker()

        return self.meta_list

    def get_dataset_meta_list(self) -> List[Dict]:
        self.set_dataset_meta_list()

        return self.dataset_meta_list

    def set_dataset_meta_list(self) -> None:
        if len(self.dataset_meta_list) == 0:
            for _ in range(len(self.meta_list)):
                self.dataset_meta_list.append({})

        for idx, meta in enumerate(self.meta_list):
            local_datset_func_dict = self.dataset_func_list[idx]

            for _key, _value in local_datset_func_dict.items():
                rst_list = list()
                for local_dataset_func in _value:
                    if local_dataset_func is not None:
                        tmp_dict = local_dataset_func.local_to_dict()
                        if len(tmp_dict) == 0:
                            tmp_val = None
                        else:
                            tmp_val = list(tmp_dict.values())[0]
                        rst_list.append(tmp_val)
                    else:
                        rst_list.append(None)
                self.dataset_meta_list[idx][_key] = rst_list
