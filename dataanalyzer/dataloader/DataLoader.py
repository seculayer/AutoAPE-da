# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict

from dataanalyzer.analyzer.TableDatasetMeta import TableDatasetMeta
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient
from dataanalyzer.analyzer.DatasetMeta import DatasetMeta


class DataLoader(object):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        self.job_info: DAJobInfo = job_info
        self.sftp_client: PySFTPClient = sftp_client
        self.mrms_sftp_client: PySFTPClient = mrms_sftp_client
        self.dataset_meta: DatasetMeta = DatasetMeta()
        self.num_worker: int = 1

    def load(self):
        raise NotImplementedError

    def generate_meta(self) -> Dict:
        raise NotImplementedError

    def global_meta(self) -> None:
        raise NotImplementedError

    def get_num_worker(self) -> int:
        return self.num_worker

    def get_meta(self) -> DatasetMeta:
        return self.dataset_meta

    def worker_monitor(self) -> bool:
        for idx in range(self.num_worker):
            if not self.mrms_sftp_client.is_exist(
                    "{}/DA_WORKER_{}_{}.meta".format(
                        Constants.DIR_DIVISION_PATH, self.job_info.get_job_id(), idx)):
                return False
        return True

    def load_local_meta(self, idx):
        f = self.mrms_sftp_client.open(
            "{}/DA_WORKER_{}_{}.meta".format(Constants.DIR_DIVISION_PATH, self.job_info.get_job_id(), idx), "r")
        meta = json.loads(f.read()).get("meta")
        f.close()
        return meta

    def write_meta(self, filename) -> None:
        f = self.mrms_sftp_client.open(filename, "w")
        f.write(json.dumps(self.generate_meta(), indent=2))
        f.close()
