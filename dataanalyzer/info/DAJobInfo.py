# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 Service Model Team, R&D Center.
from typing import List

from pycmmn.sftp.PySFTPClient import PySFTPClient
from pycmmn.utils.JSONUtils import JSONUtils


class DAJobInfo(object):
    def __init__(self, sftp_client: PySFTPClient, filename: str):
        self.job_info_dict = JSONUtils.read_sftp_json_from_file(sftp_client, filename)
        self.job_id = self.job_info_dict.get("dataset_id")
        self.dataset_format = self.job_info_dict.get("dataset_format")
        self.file_path = self.job_info_dict.get("format_json").get("file_path")

    def get_filepath(self) -> str:
        return self.file_path

    def get_dataset_format(self) -> str:
        return self.dataset_format

    def get_filename(self) -> str:
        return self.job_info_dict.get("format_json").get("file_name")

    def get_field_list(self) -> List:
        return self.job_info_dict.get("format_json").get("field_list")

    def get_instances(self) -> int:
        return int(self.job_info_dict.get("n_rows"))

    def __str__(self) -> str:
        return "DataAnalyzerJobInfo = {}".format(self.job_info_dict)

    def get_job_id(self) -> str:
        return self.job_id

    def get_features(self):
        return int(self.job_info_dict.get("n_cols"))
