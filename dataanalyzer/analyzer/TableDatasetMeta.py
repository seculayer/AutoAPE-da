# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict, List

from dataanalyzer.analyzer.DatasetMeta import DatasetMeta
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo


class TableDatasetMeta(DatasetMeta):
    def __init__(self):
        DatasetMeta.__init__(self)

        self.meta_list: List[Dict] = list()
        self.field_list = list()

    def initialize(self, job_info: DAJobInfo):
        self.field_list = job_info.get_field_list()

        for idx, field_nm in enumerate(self.field_list):
            self.meta_list.append(
                {
                    "field_nm": field_nm,
                    "field_idx": idx,
                    "field_type": {
                        Constants.FIELD_TYPE_NULL: 0,
                        Constants.FIELD_TYPE_INT: 0,
                        Constants.FIELD_TYPE_FLOAT: 0,
                        Constants.FIELD_TYPE_STRING: 0,
                    }
                }
            )

    def apply(self, data):
        for idx, field_nm in enumerate(self.field_list):
            result, f_type = DatasetMeta._field_type(data.get(field_nm))
            self.meta_list[idx].get("field_type")[f_type] += 1
