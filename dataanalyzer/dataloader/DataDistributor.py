# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict, List

import paramiko

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataDistributor(object):
    def __init__(self, job_info: DAJobInfo, num_worker=1):
        self.logger = Common.LOGGER.get_logger()

        self.job_info: DAJobInfo = job_info
        self.num_worker: int = num_worker
        self.mrms_sftp_client: PySFTPClient = None
        self.max_rows, self.mod = self.determine_max_rows(self.job_info.get_instances())
        self.current: int = 0
        self.current_worker_n: int = 0
        self.writer: paramiko.SFTPFile = None
        self.filename_list: List[str] = list()

    def initialize(self, client):
        self.mrms_sftp_client = client
        self.writer = self.open()

    def determine_max_rows(self, instances) -> (int, int):
        return int(instances / self.num_worker), int(instances % self.num_worker)

    def get_filename(self) -> str:
        return "{}/{}_{}".format(
            Constants.DIR_DIVISION_PATH, self.job_info.job_id, self.current_worker_n)

    def open(self) -> paramiko.SFTPFile:
        self.filename_list.append(self.get_filename() + ".done")
        return self.mrms_sftp_client.open(self.get_filename() + ".tmp", "w")

    def close(self):
        if self.writer is not None:
            self.writer.close()
            self.writer = None
            filename = self.get_filename()
            if self.mrms_sftp_client.is_exist(filename + ".done"):
                self.mrms_sftp_client.remove(filename + ".done")
            self.mrms_sftp_client.rename(filename + ".tmp", filename + ".done")

    def write(self, data: Dict):
        self.writer.write(json.dumps(data) + "\n")
        self.current += 1
        if self.current >= self.max_rows:
            if self.mod == 0 or self.mod < self.current_worker_n or self.current >= self.max_rows + 1:
                self.close()
                self.current = 0
                self.current_worker_n += 1
                if self.current_worker_n < self.num_worker:
                    self.writer = self.open()

    def get_file_list(self):
        return self.filename_list
