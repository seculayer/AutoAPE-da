# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json

from dataanalyzer.analyzer.TableDatasetMeta import TableDatasetMeta
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoaderTable(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient):
        DataLoader.__init__(self, job_info, sftp_client)

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

        print(self.dataset_meta.meta_list)
        f.close()
