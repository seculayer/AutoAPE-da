# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoader(object):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient):
        self.job_info: DAJobInfo = job_info
        self.sftp_client: PySFTPClient = sftp_client

    def load(self):
        return {
            Constants.DATASET_FORMAT_TABLE: self.load_table()
        }.get(self.job_info.get_dataset_format())

    def load_table(self):
        f = self.sftp_client.open(self.job_info.get_filepath(), "r")
        while True:
            line = f.readline()
            print(line)
            if not line:
                break
        f.close()
