# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict

from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
from dataanalyzer.core.analyzer.dataset.common.NumFeature import NumFeature
from dataanalyzer.core.analyzer.dataset.common.NumInstance import NumInstance
from dataanalyzer.info.DAJobInfo import DAJobInfo


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
