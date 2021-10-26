# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.dataloader.DataLoaderTable import DataLoaderTable
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoaderFactory(object):
    @staticmethod
    def make_data_loader(job_info: DAJobInfo, sftp_client: PySFTPClient) -> DataLoader:
        return {
            Constants.DATASET_FORMAT_TABLE: DataLoaderTable(job_info, sftp_client)
        }.get(job_info.get_dataset_format())
