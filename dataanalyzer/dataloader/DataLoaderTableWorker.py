# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict

from dataanalyzer.analyzer.TableDatasetMetaWorker import TableDatasetMetaWorker
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoaderTableWorker(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient, job_idx: str):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.worker_idx = job_idx

    def load_meta(self):
        f = self.mrms_sftp_client.open("{}/DA_CHIEF_{}.meta".format(
            Constants.DIR_DIVISION_PATH, self.job_info.get_job_id()))
        result: Dict = json.loads(f.read())
        f.close()
        return result

    def load(self) -> None:
        f = self.mrms_sftp_client.open("{}/{}_{}.done".format(
            Constants.DIR_DIVISION_PATH, self.job_info.get_job_id(), self.worker_idx), "r")
        self.dataset_meta: TableDatasetMetaWorker = TableDatasetMetaWorker()
        self.dataset_meta.initialize(self.load_meta(), self.job_info)

        while True:
            line = f.readline()
            if not line:
                break
            json_data = json.loads(line)
            self.dataset_meta.apply(json_data)

        self.dataset_meta.calculate()
        f.close()
        self.write_meta()

    def generate_meta(self) -> Dict:
        return {
            "meta": self.dataset_meta.get_meta_list()
        }

    def write_meta(self) -> None:
        f = self.mrms_sftp_client.open("{}/DA_WORKER_{}_{}.meta".format(
            Constants.DIR_DIVISION_PATH, self.job_info.get_job_id(), self.worker_idx), "w")
        f.write(json.dumps(self.generate_meta(), indent=2))
        f.close()

    def global_meta(self):
        raise NotImplementedError