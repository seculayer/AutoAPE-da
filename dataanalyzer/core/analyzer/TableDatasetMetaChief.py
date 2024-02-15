# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from eda.core.analyze.FunctionsAbstract import FunctionsAbstract
from eda.core.analyze.FunctionInterface import FunctionInterface
from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
from dataanalyzer.core.analyzer.dataset.common.NumFeature import NumFeature
from dataanalyzer.core.analyzer.dataset.common.NumInstance import NumInstance
from dataanalyzer.info.DAJobInfo import DAJobInfo


class TableDatasetMetaChief(DatasetMetaAbstract):
    def __init__(self, target_field):
        DatasetMetaAbstract.__init__(self)
        self.target_field = target_field

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

    def set_dataset_func(self):
        if len(self.dataset_func_list) > 0:
            self.dataset_func_list.clear()

        field_type_list = list()
        target_field_idx = -1
        for idx, meta in enumerate(self.meta_list):
            field_type_list.append(meta.get("field_type"))
            if self.target_field == meta.get("field_nm"):
                target_field_idx = idx

        if len(self.dataset_meta_list) == 0:
            for _ in range(len(self.meta_list)):
                self.dataset_meta_list.append({})

        for idx, meta in enumerate(self.meta_list):
            target_field_type = meta["field_type"]

            func_cls_list = FunctionInterface.get_func_cls_list("eda.core.analyze.functions.binomial",
                                                                self.binomial_func_list)
            cls_dict = FunctionInterface.get_available_func_dict(func_cls_list, target_field_type)
            for key in cls_dict.keys():
                cls_list = list()
                for field_idx, field_type in enumerate(field_type_list):
                    tmp_cls_dict = FunctionInterface.get_available_func_dict(func_cls_list, field_type)
                    if tmp_cls_dict.__contains__(key):
                        if tmp_cls_dict[key].DATASET_META_RST_TYPE == "1" \
                                or (tmp_cls_dict[key].DATASET_META_RST_TYPE == "2" and field_idx == target_field_idx):
                            cls_list.append(tmp_cls_dict[key](num_instances=self.meta_dataset["instances"]))
                        else:
                            cls_list.append(None)
                    else:
                        cls_list.append(None)
                cls_dict[key] = cls_list
            self.dataset_func_list.append(cls_dict)

    # Chief-Worker Statistics
    def calculate_global_meta(self, local_meta_list: List[List[Dict]], curr_cycle) -> bool:
        self.set_meta_func()
        self.set_dataset_func()

        continue_cycle_flag = False

        for idx, field_func in enumerate(self.meta_func_list):
            workers_meta_list = list()
            meta_rst_dict = dict()
            for meta_list in local_meta_list:
                workers_meta_list.append(meta_list[idx])
            for _key in field_func.keys():
                meta_func_cls: FunctionsAbstract = field_func.get(_key)
                # 필요값이 없을 경우(필요 cycle에 도달하지 못한 경우) 및 중복 계산 방지
                if not curr_cycle == meta_func_cls.get_n_cycle():
                    pass
                else:
                    # import datetime
                    # print(f"{_key}")
                    # start_time = datetime.datetime.now()
                    meta_func_cls.global_calc(workers_meta_list)
                    # print(f"{curr_cycle} - {meta_func_cls.__class__} 걸린 시간 : {datetime.datetime.now() - start_time}")
                    meta_rst_dict.update(meta_func_cls.global_to_dict())
                if meta_func_cls.get_n_cycle() > curr_cycle:
                    continue_cycle_flag = True

            meta_rst_dict = self.statistics_exception(meta_rst_dict)
            self.meta_list[idx]["statistics"].update(meta_rst_dict)

            dataset_meta_rst_dict = dict()
            for _key, _dataset_fns in self.dataset_func_list[idx].items():
                not_none_idx = None
                for i, cls in enumerate(_dataset_fns):
                    if cls is None:
                        continue
                    else:
                        not_none_idx = i
                        break
                # 필요값이 없을 경우(필요 cycle에 도달하지 못한 경우) 및 중복 계산 방지
                if not_none_idx is None or not curr_cycle == _dataset_fns[not_none_idx].get_n_cycle():
                    continue
                else:
                    dataset_meta_rst_dict.update({_key: list()})
                    for sub_idx in range(len(_dataset_fns)):
                        if _dataset_fns[sub_idx] is None:
                            dataset_meta_rst_dict[_key].append(None)
                            continue
                        comparison_meta_list = list()

                        for meta_list in local_meta_list:
                            comparison_meta_list.append(meta_list[sub_idx])

                        _dataset_fns[sub_idx].global_calc([workers_meta_list, comparison_meta_list],
                                                          comparison_idx=sub_idx)
                        tmp_global_dict = _dataset_fns[sub_idx].global_to_dict()
                        if len(tmp_global_dict) == 0:
                            tmp_global_val = None
                        else:
                            tmp_global_val = list(tmp_global_dict.values())[0]
                        dataset_meta_rst_dict[_key].append(tmp_global_val)

                if _dataset_fns[not_none_idx].get_n_cycle() > curr_cycle:
                    continue_cycle_flag = True

            dataset_meta_rst_dict = self.statistics_exception(dataset_meta_rst_dict)
            self.dataset_meta_list[idx].update(dataset_meta_rst_dict)

        return not continue_cycle_flag

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
