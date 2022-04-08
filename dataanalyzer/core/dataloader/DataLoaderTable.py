# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict

from dataanalyzer.core.analyzer import TableDatasetMetaChief
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.dataloader.distributor import DataDistributorTable
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.sftp.PySFTPClient import PySFTPClient


class DataLoaderTable(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.data_dist = DataDistributorTable(job_info, self.num_worker)
        self.data_dist.initialize(mrms_sftp_client)

    def determine_n_workers(self):
        try:
            instances = self.job_info.get_instances()
            n_workers = int(instances / Constants.DISTRIBUTE_INSTANCES_TABLE)
            if instances % Constants.DISTRIBUTE_INSTANCES_TABLE == 0:
                return n_workers
            else:
                return n_workers + 1
        except Exception:
            return DataLoader.determine_n_workers(self)

    def load(self) -> None:
        f = self.sftp_client.open("{}/{}".format(self.job_info.get_filepath(), self.job_info.get_filename()), "r")
        self.dataset_meta: TableDatasetMetaChief = TableDatasetMetaChief()
        self.dataset_meta.initialize(self.job_info)

        self.data_dist.make_fileline_list()

        while True:
            line = f.readline()
            if not line:
                break
            json_data = json.loads(line)
            self.dataset_meta.apply(json_data)
            self.data_dist.write(json_data)

        self.dataset_meta.calculate()
        f.close()
        self.data_dist.close()
        self.write_meta(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_CHIEF_{self.job_info.get_job_id()}.meta"
        )

    def generate_meta(self) -> Dict:
        return {
            "file_list": self.data_dist.get_file_list(),
            "file_num_line": self.data_dist.get_fileline_list(),
            "meta": self.dataset_meta.get_meta_list(),
            "dataset_meta": self.dataset_meta.get_meta_dataset()
        }
