# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.dataloader.DataLoaderImage import DataLoaderImage
from dataanalyzer.dataloader.DataLoaderImageWorker import DataLoaderImageWorker
from dataanalyzer.dataloader.DataLoaderTable import DataLoaderTable
from dataanalyzer.dataloader.DataLoaderTableWorker import DataLoaderTableWorker
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.sftp.PySFTPClient import PySFTPClient


class DataLoaderFactory(object):
    @staticmethod
    def create(job_type: str, job_info: DAJobInfo, job_idx: str, sftp_client: PySFTPClient,
               mrms_sftp_client: PySFTPClient) -> DataLoader:
        class_dict = {
            Constants.DATASET_FORMAT_TABLE: {
                Constants.JOB_TYPE_CHIEF: DataLoaderTable,
                Constants.JOB_TYPE_WORKER: DataLoaderTableWorker,
            },
            Constants.DATASET_FORMAT_IMAGE: {
                Constants.JOB_TYPE_CHIEF: DataLoaderImage,
                Constants.JOB_TYPE_WORKER: DataLoaderImageWorker,
            }
        }
        class_nm = class_dict.get(job_info.get_dataset_format()).get(job_type)
        if class_nm is not None:
            if job_type == Constants.JOB_TYPE_CHIEF:
                return class_nm(job_info, sftp_client, mrms_sftp_client)
            else:
                return class_nm(job_info, sftp_client, mrms_sftp_client, job_idx)
        else:
            raise NotImplementedError
