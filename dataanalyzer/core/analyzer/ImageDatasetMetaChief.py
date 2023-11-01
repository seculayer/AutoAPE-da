# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

import numpy as np

from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
from dataanalyzer.info.DAJobInfo import DAJobInfo


class ImageDatasetMetaChief(DatasetMetaAbstract):
    IMAGE_KEYS = ["size"]
    COMMON_KEYS = [("unique", "label")]

    def __init__(self, target_field):
        DatasetMetaAbstract.__init__(self)
        self.target_field = target_field

    def initialize(self, job_info: DAJobInfo, meta_json: Dict = None):
        super().initialize(job_info)

        self.field_list = ["image"] + job_info.get_field_list()

        for idx, field_nm in enumerate(self.field_list):
            if field_nm == "image":
                self.meta_list.append(self._initialize_image_metadata(0, Constants.FIELD_TYPE_IMAGE))
            else:
                self.meta_list.append(self._initialize_metadata(idx, field_nm))

    def _initialize_basic_dataset_meta(self, job_info: DAJobInfo) -> None:
        super()._initialize_basic_dataset_meta(job_info)

    def apply(self, data: np.array, curr_cycle) -> None:
        for idx, field_nm in enumerate(self.field_list):
            if field_nm == "image":
                continue
            result, f_type = DatasetMetaAbstract.field_type(data.get(field_nm))
            self.meta_list[idx].get("type_stat")[f_type] += 1

    def set_field_type(self) -> None:
        for idx, meta in enumerate(self.meta_list):
            if meta.get("field_nm") == "image":
                continue
            meta["field_type"] = self.determine_type(meta.get("type_stat"))
