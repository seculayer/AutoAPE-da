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
        self.meta_func_list: List[Dict] = list()
        self.worker_idx = int(worker_idx)

    def initialize(self, job_info: DAJobInfo, meta_json: Dict = None):
        self.meta_list: List[Dict] = meta_json.get("meta", list())

        if len(self.meta_func_list) > 0:
            self.meta_func_list.clear()

        for idx, _ in enumerate(self.meta_list):
            self.meta_func_list.append(self._initialize_meta_functions(job_info, _))

    def _initialize_meta_functions(self, job_info: DAJobInfo, meta) -> Dict:
        field_type = meta.get("field_type")
        # worker_n_instances = self._get_worker_n_instance(job_info)

        func_cls_list = FunctionInterface.get_func_cls_list(self.eda_func_list)
        cls_dict = FunctionInterface.get_available_func_dict(func_cls_list, field_type)
        for key in cls_dict.keys():
            # cls_dict[key] = cls_dict[key](num_instances=worker_n_instances)
            cls_dict[key] = cls_dict[key](num_instances=job_info.get_instances())

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
        import datetime
        for idx, fd in enumerate(self.meta_list):
            print(f'{fd.get("field_nm")}')
            for _key in self.meta_func_list[idx].keys():
                meta_func_cls: FunctionsAbstract = self.meta_func_list[idx].get(_key)
                # 필요값이 없을 경우(필요 cycle에 도달하지 못한 경우) 및 중복 계산 방지
                if not curr_cycle == meta_func_cls.get_n_cycle() - 1:
                    continue
                start_time = datetime.datetime.now()
                meta_func_cls.local_calc(data.get(fd.get("field_nm")), self.meta_list[idx].get("statistics", {}))
                print(f"{curr_cycle} - {meta_func_cls.__class__} 걸린 시간 : {datetime.datetime.now() - start_time}")

    def check_end(self, curr_cycle) -> bool:
        is_continue = False
        for idx, fd in enumerate(self.meta_list):
            for _key in self.meta_func_list[idx].keys():
                n_cycle = self.meta_func_list[idx].get(_key).get_n_cycle()
                if curr_cycle + 1 < n_cycle:
                    is_continue = True

        return not is_continue

    def get_meta_list(self) -> List[dict]:
        return self.get_meta_list_for_worker()
