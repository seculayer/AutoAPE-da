# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

import numpy as np

from dataanalyzer.core.analyzer import DatasetMetaAbstract
from dataanalyzer.info.DAJobInfo import DAJobInfo


class ImageDatasetMetaChief(DatasetMetaAbstract):
    IMAGE_KEYS = ["size"]
    COMMON_KEYS = [("unique", "label")]

    def __init__(self):
        DatasetMetaAbstract.__init__(self)

    def initialize(self, job_info: DAJobInfo, meta_json: Dict = None):
        super().initialize(job_info)

        self.field_list = ["image"] + job_info.get_field_list()

        self.meta_list.append(self._initialize_image_metadata(0, "image"))
        self.meta_func_list.append(self._initialize_image_meta_functions(job_info))

        for idx, _tup in enumerate(self.COMMON_KEYS):
            self.meta_list.append(self._initialize_metadata(idx + 1, _tup[1]))
            self.meta_func_list.append(self._initialize_label_meta_functions(job_info))

    def _initialize_basic_dataset_meta(self, job_info: DAJobInfo) -> None:
        super()._initialize_basic_dataset_meta(job_info)

    def apply(self, data: np.array):
        for _ in self.IMAGE_KEYS:
            self.meta_func_list[0].get(_).apply(data)

    def apply_annotation(self, json_data: Dict):
        for idx, _tup in enumerate(self.COMMON_KEYS):
            self.meta_func_list[idx + 1].get(_tup[0]).apply(json_data.get(_tup[1]))

    def calculate(self):
        for idx, meta in enumerate(self.meta_list):
            if meta["field_nm"] == "label":
                meta["field_type"] = "string"
            self._statistic_calculate(idx, meta)

    # Chief-Worker Statistics
    def calculate_global_meta(self, local_meta_list: List[List[Dict]]) -> None:
        pass

