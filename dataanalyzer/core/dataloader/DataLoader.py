# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from logging import Logger
from typing import Dict, List

from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.sftp.PySFTPClient import PySFTPClient


class DataLoader(object):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        self.logger: Logger = Common.LOGGER.getLogger()
        self.job_info: DAJobInfo = job_info
        self.sftp_client: PySFTPClient = sftp_client
        self.mrms_sftp_client: PySFTPClient = mrms_sftp_client
        self.dataset_meta: DatasetMetaAbstract = DatasetMetaAbstract()
        self.num_worker = self.determine_n_workers()
        self.is_end = False

    def determine_n_workers(self):
        return 1

    def load(self, **kwargs):
        raise NotImplementedError

    def generate_meta(self) -> Dict:
        raise NotImplementedError

    def global_meta(self, curr_cycle) -> None:
        # load local meta info
        local_meta_list: List = list()
        for idx in range(self.get_num_worker()):
            local_meta_list.append(self.load_local_meta(idx, curr_cycle))

        self.is_end = self.dataset_meta.calculate_global_meta(local_meta_list, curr_cycle)
        if self.is_end:
            self.write_meta(f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_META_{self.job_info.get_job_id()}.info")
        else:
            self.write_meta(f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_CHIEF_{self.job_info.get_job_id()}_{curr_cycle}.meta")

    def get_num_worker(self) -> int:
        return self.num_worker

    def get_meta(self) -> DatasetMetaAbstract:
        return self.dataset_meta

    def worker_monitor(self, curr_cycle) -> bool:
        for idx in range(self.num_worker):
            if not self.mrms_sftp_client.is_exist(
                f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_WORKER_{self.job_info.get_job_id()}_{idx}_{curr_cycle-1}.meta"
            ):
                return False
        return True

    def chief_monitor(self, curr_cycle) -> bool:
        if self.mrms_sftp_client.is_exist(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_CHIEF_{self.job_info.get_job_id()}_{curr_cycle}.meta"
        ):
            return True
        else:
            return False

    def load_local_meta(self, idx, curr_cycle):
        f = self.mrms_sftp_client.open(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_WORKER_{self.job_info.get_job_id()}_{idx}_{curr_cycle-1}.meta",
            "r"
        )
        meta = json.loads(f.read()).get("meta")
        f.close()
        return meta

    def write_meta(self, filename) -> None:
        f = self.mrms_sftp_client.open(filename, "w")
        f.write(json.dumps(self.generate_meta(), indent=2))
        f.close()

    def check_end(self) -> bool:
        return self.is_end
