# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.dataloader.DataLoaderTable import DataLoaderTable
from dataanalyzer.dataloader.DataLoaderTableWorker import DataLoaderTableWorker
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoaderFactory(object):
    @staticmethod
    def make_data_loader(job_type: str, job_info: DAJobInfo, job_idx: str, sftp_client: PySFTPClient,
                         mrms_sftp_client: PySFTPClient) -> DataLoader:
        return {
            Constants.DATASET_FORMAT_TABLE: {
                Constants.JOB_TYPE_CHIEF: DataLoaderTable(job_info, sftp_client, mrms_sftp_client),
                Constants.JOB_TYPE_WORKER: DataLoaderTableWorker(job_info, sftp_client, mrms_sftp_client, job_idx),
            }
        }.get(job_info.get_dataset_format()).get(job_type)
