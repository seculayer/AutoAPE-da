# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict

from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.dataloader.distributor.DataDistributorImage import DataDistributorImage
from dataanalyzer.core.dataloader.distributor.DataDistributorTable import DataDistributorTable
from dataanalyzer.info.DAJobInfo import DAJobInfo


class VDataDistributorFactory(object):
    @staticmethod
    def create(job_info: DAJobInfo, num_worker: int) -> DataDistributorTable:
        class_dict: Dict = {
            Constants.DATASET_FORMAT_TABLE: DataDistributorTable,
            Constants.DATASET_FORMAT_IMAGE: DataDistributorImage
        }
        class_nm = class_dict.get(job_info.get_dataset_format())
        if class_nm is not None:
            return class_nm(job_info, num_worker)
        else:
            raise NotImplementedError
