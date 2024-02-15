# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import requests as rq
from typing import Union
import json

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.dataloader.DataLoader import DataLoader
from dataanalyzer.core.dataloader.DataLoaderFactory import DataLoaderFactory
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.sftp.SFTPClientManager import SFTPClientManager
from pycmmn.Singleton import Singleton
from pycmmn.sftp.PySFTPClient import PySFTPClient


class DataAnalyzerChiefManager(object, metaclass=Singleton):
    # class : DataAnalyzerChiefManager
    def __init__(self):
        self.logger = Common.LOGGER.getLogger()
        self.mrms_sftp_manager: Union[SFTPClientManager, None] = None
        self.storage_sftp_manager: Union[SFTPClientManager, None] = None
        self.job_info: Union[DAJobInfo, None] = None
        self.loader: Union[DataLoader, None] = None
        self.job_type = Constants.JOB_TYPE_CHIEF
        self.rest_root_url = f"http://{Constants.MRMS_SVC}:{Constants.MRMS_REST_PORT}"

    def initialize(self, job_id: str, job_idx: str):
        self.mrms_sftp_manager = SFTPClientManager(
            "{}:{}".format(Constants.MRMS_SVC, Constants.MRMS_SFTP_PORT),
            Constants.MRMS_SSH_USER, Constants.MRMS_SSH_PASSWD, self.logger)

        self.storage_sftp_manager = SFTPClientManager(
            "{}:{}".format(Constants.STORAGE_SVC, Constants.STORAGE_SFTP_PORT),
            Constants.STORAGE_SSH_USER, Constants.STORAGE_SSH_PASSWD, self.logger)

        self.job_info = self.load_job_info(job_id)
        self.logger.info(str(self.job_info))
        response = rq.post(f"{self.rest_root_url}/mrms/get_dataset_info", json={"dataset_id": job_id})
        response_json = json.loads(response.text)
        target_field = response_json.get("target_field")
        self.logger.info(f"get target field: {response.status_code} {response.reason} {target_field}")

        self.loader = DataLoaderFactory.create(
            self.job_type, self.job_info, job_idx,
            self.storage_sftp_manager.get_client(),
            self.mrms_sftp_manager.get_client(),
            target_field
        )
        self.logger.info("DataAnalyzerManager initialized.")

    def load_job_info(self, job_id: str):
        filename = Constants.DIR_JOB_PATH + f"/{job_id}/DA_{job_id}.job"
        self.logger.info("load file name : {}".format(filename))
        return DAJobInfo(self.mrms_sftp_manager.get_client(), filename)

    def get_job_info(self):
        return self.job_info

    def get_storage_sftp_client(self) -> PySFTPClient:
        return self.storage_sftp_manager.get_client()

    def get_mrms_sftp_client(self):
        return self.mrms_sftp_manager.get_client()

    def data_loader(self, **kwargs):
        self.loader.load(**kwargs)

    def check_end(self) -> bool:
        return self.loader.check_end()

    def request_worker_create(self):
        response = rq.get("{}/mrms/request_da_worker?id={}&num_worker={}".format(
            self.rest_root_url, self.job_info.get_job_id(), self.loader.get_num_worker())
        )
        self.logger.info("create da worker : {} {} {}".format(response.status_code, response.reason, response.text))

    def monitor_worker_end(self, curr_cycle) -> bool:
        return self.loader.worker_monitor(curr_cycle)

    def calculate_global_meta(self, curr_cycle) -> None:
        self.loader.global_meta(curr_cycle)

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
    dam = DataAnalyzerChiefManager()
