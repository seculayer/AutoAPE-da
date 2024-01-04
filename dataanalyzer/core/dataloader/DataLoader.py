# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
"""
module-docstring
"""
import fnmatch
import json
from logging import Logger
from typing import Dict, List

from pycmmn.sftp.PySFTPClient import PySFTPClient

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.analyzer.DatasetMetaAbstract import DatasetMetaAbstract
from dataanalyzer.info.DAJobInfo import DAJobInfo


class DataLoader:
    """
    class-docstring
    """

    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        self.logger: Logger = Common.LOGGER.getLogger()
        self.job_info: DAJobInfo = job_info
        self.sftp_client: PySFTPClient = sftp_client
        self.mrms_sftp_client: PySFTPClient = mrms_sftp_client
        self.dataset_meta: DatasetMetaAbstract = DatasetMetaAbstract()
        self.num_worker = self.determine_n_workers()
        self.is_end = False

    def determine_n_workers(self):
        """
        function-docstring
        """
        return 1

    def load(self, **kwargs):
        """
        function-docstring
        """
        raise NotImplementedError

    def generate_meta(self) -> Dict:
        """
        function-docstring
        """
        raise NotImplementedError

    def global_meta(self, curr_cycle) -> None:
        """
        function-docstring
        """
        # load local meta info
        local_meta_list: List = []
        for idx in range(self.get_num_worker()):
            local_meta_list.append(self.load_local_meta(idx, curr_cycle))

        self.is_end = self.dataset_meta.calculate_global_meta(local_meta_list, curr_cycle)
        if self.is_end:
            self.write_meta(
                f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/" f"DA_META_{self.job_info.get_job_id()}.info"
            )
            if not Constants.WRITE_CYCLE_HISTORY:
                self._remove_cycle_history()
        else:
            self.write_meta(
                (
                    f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/"
                    f"DA_CHIEF_{self.job_info.get_job_id()}_{curr_cycle}.meta"
                )
            )

    def get_num_worker(self) -> int:
        """
        function-docstring
        """
        return self.num_worker

    def get_meta(self) -> DatasetMetaAbstract:
        """
        function-docstring
        """
        return self.dataset_meta

    def worker_monitor(self, curr_cycle) -> bool:
        """
        function-docstring
        """
        for idx in range(self.num_worker):
            if not self.mrms_sftp_client.is_exist(
                (
                    f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/"
                    f"DA_WORKER_{self.job_info.get_job_id()}_{idx}_{curr_cycle-1}.meta"
                )
            ):
                return False
        return True

    def chief_monitor(self, curr_cycle) -> bool:
        """
        function-docstring
        """
        if self.mrms_sftp_client.is_exist(
            (
                f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/"
                f"DA_CHIEF_{self.job_info.get_job_id()}_{curr_cycle}.meta"
            )
        ):
            return True
        return False

    def load_local_meta(self, idx, curr_cycle):
        """
        function-docstring
        """
        f = self.mrms_sftp_client.open(
            (
                f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/"
                f"DA_WORKER_{self.job_info.get_job_id()}_{idx}_{curr_cycle-1}.meta"
            ),
            "r",
        )
        meta = json.loads(f.read()).get("meta")
        f.close()
        return meta

    def write_meta(self, filename) -> None:
        """
        function-docstring
        """
        filename, extension = filename.split(".")
        f = self.mrms_sftp_client.open(f"{filename}.tmp", "w")
        f.write(json.dumps(self.generate_meta(), indent=2))
        f.close()
        self.mrms_sftp_client.rename(f"{filename}.tmp", f"{filename}.{extension}")

    def check_end(self) -> bool:
        """
        function-docstring
        """
        return self.is_end

    def _remove_cycle_history(self):
        """
        function-docstring
        """
        folder_path = f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}"
        for file_name in self.mrms_sftp_client.get_file_list(folder_path):
            if fnmatch.fnmatch(file_name, "DA_CHIEF*") or fnmatch.fnmatch(file_name, "DA_WORKER*"):
                self.mrms_sftp_client.delete_file(f"{folder_path}/{file_name}")
