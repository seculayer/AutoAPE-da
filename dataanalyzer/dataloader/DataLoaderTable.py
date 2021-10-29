# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict, List

from dataanalyzer.analyzer.TableDatasetMeta import TableDatasetMeta
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataDistributor import DataDistributor
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoaderTable(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.num_worker = self.determine_n_workers(self.job_info.get_instances())
        self.data_dist = DataDistributor(job_info, self.num_worker)
        self.data_dist.initialize(mrms_sftp_client)

    @staticmethod
    def determine_n_workers(instances):
        n_workers = int(instances / Constants.DISTRIBUTE_INSTANCES)
        if instances % Constants.DISTRIBUTE_INSTANCES == 0:
            return n_workers
        else:
            return n_workers + 1

    def load(self) -> None:
        f = self.sftp_client.open("{}/{}".format(self.job_info.get_filepath(), self.job_info.get_filename()), "r")
        self.dataset_meta: TableDatasetMeta = TableDatasetMeta()
        self.dataset_meta.initialize(self.job_info)
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
        self.write_meta("{}/DA_CHIEF_{}.meta".format(Constants.DIR_DIVISION_PATH, self.job_info.get_job_id()))

    def generate_meta(self) -> Dict:
        return {
            "file_list": self.data_dist.get_file_list(),
            "meta": self.dataset_meta.get_meta_list()
        }

    def global_meta(self) -> None:
        # load local meta info
        local_meta_list: List = list()
        for idx in range(self.get_num_worker()):
            local_meta_list.append(self.load_local_meta(idx))

        self.dataset_meta.calculate_global_meta(local_meta_list)
        self.write_meta("{}/DA_META_{}.info".format(Constants.DIR_DIVISION_PATH, self.job_info.get_job_id()))
