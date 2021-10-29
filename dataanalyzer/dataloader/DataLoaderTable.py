# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json

from dataanalyzer.analyzer.TableDatasetMeta import TableDatasetMeta
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataDistributor import DataDistributor
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoaderTable(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        DataLoader.__init__(self, job_info, sftp_client)
        self.num_worker = self.determine_n_workers(self.job_info.get_instances())
        self.data_dist = DataDistributor(job_info, self.num_worker)
        self.data_dist.initialize(mrms_sftp_client)

    def determine_n_workers(self, instances):
        n_workers = int(self.job_info.get_instances() / Constants.DISTRIBUTE_INSTANCES)
        if self.job_info.get_instances() % Constants.DISTRIBUTE_INSTANCES == 0:
            return n_workers
        else:
            return n_workers + 1

    def load(self):
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

        # for meta in self.dataset_meta.meta_list:
        #     for _ in meta.get("statistics").keys():
        #         print(str(meta.get("statistics").get(_)))
        f.close()
        self.data_dist.close()
