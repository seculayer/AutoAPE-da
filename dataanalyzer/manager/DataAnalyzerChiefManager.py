# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import requests as rq

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.dataloader.DataLoaderFactory import DataLoaderFactory
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.manager.SFTPClientManager import SFTPClientManager
from dataanalyzer.util.Singleton import Singleton
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataAnalyzerChiefManager(object, metaclass=Singleton):
    # class : DataAnalyzerChiefManager
    def __init__(self):
        self.logger = Common.LOGGER.get_logger()
        self.mrms_sftp_manager: SFTPClientManager = None
        self.storage_sftp_manager: SFTPClientManager = None
        self.job_info: DAJobInfo = None
        self.loader: DataLoader = None
        self.job_type = Constants.JOB_TYPE_CHIEF
        self.rest_root_url = f"http://{Constants.MRMS_SVC}:{Constants.MRMS_REST_PORT}"

    def initialize(self, job_id: str, job_idx: str):
        self.mrms_sftp_manager = SFTPClientManager(
            "{}:{}".format(Constants.MRMS_SVC, Constants.MRMS_SFTP_PORT),
            Constants.SSH_USER, Constants.SSH_PASSWD)

        self.storage_sftp_manager = SFTPClientManager(
            "{}:{}".format(Constants.STORAGE_SVC, Constants.STORAGE_SFTP_PORT),
            Constants.SSH_USER, Constants.SSH_PASSWD)

        self.job_info = self.load_job_info(job_id, job_idx)
        self.logger.info(str(self.job_info))

        self.loader = DataLoaderFactory.make_data_loader(
            self.job_type, self.job_info, job_idx,
            self.storage_sftp_manager.get_client(),
            self.mrms_sftp_manager.get_client()
        )
        self.logger.info("DataAnalyzerManager initialized.")

    def load_job_info(self, job_id: str, job_idx: str):
        filename = Constants.DIR_JOB_PATH + f"/{job_id}/DA_{job_id}.job"
        self.logger.info("load file name : {}".format(filename))
        return DAJobInfo(self.mrms_sftp_manager.get_client(), filename)

    def get_job_info(self):
        return self.job_info

    def get_storage_sftp_client(self) -> PySFTPClient:
        return self.storage_sftp_manager.get_client()

    def get_mrms_sftp_client(self):
        return self.mrms_sftp_manager.get_client()

    def data_loader(self):
        self.loader.load()

    def request_worker_create(self):
        response = rq.get("{}/mrms/request_da_worker?id={}&num_worker={}".format(
            self.rest_root_url, self.job_info.get_job_id(), self.loader.get_num_worker())
        )
        self.logger.info("create da worker : {} {} {}".format(response.status_code, response.reason, response.text))

    def monitor_worker_end(self) -> bool:
        return self.loader.worker_monitor()

    def calculate_global_meta(self):
        self.loader.global_meta()

    def request_da_terminate(self):
        response = rq.get("{}/mrms/insert_data_anls_info?dataset_id={}".format(
            self.rest_root_url, self.job_info.get_job_id())
        )
        self.logger.info("insert data anls info : {} {} {}".format(response.status_code, response.reason, response.text))

    def request_update_dataset_status(self, job_id, status):
        body_data = {
            "dataset_id": job_id, "status_cd": status
        }
        response = rq.post("{}/mrms/update_dataset_status".format(self.rest_root_url),
                           json=body_data
                           )
        self.logger.info("update dataset status : {} {} {}".format(response.status_code, response.reason, response.text))

    def terminate(self):
        if self.mrms_sftp_manager is not None:
            self.mrms_sftp_manager.close()
        if self.storage_sftp_manager is not None:
            self.storage_sftp_manager.close()


if __name__ == '__main__':
    dam = DataAnalyzerChiefManager("ID", "0")
