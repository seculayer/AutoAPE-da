# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict, Union

from dataanalyzer.core.analyzer.TableDatasetMetaChief import TableDatasetMetaChief
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.dataloader.distributor.DataDistributorTable import DataDistributorTable
from dataanalyzer.core.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.sftp.PySFTPClient import PySFTPClient
from pycmmn.exceptions.FileLoadError import FileLoadError
from pycmmn.exceptions.ETCException import ETCException


class DataLoaderTable(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient, target_field: str):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.data_dist = DataDistributorTable(job_info, self.num_worker)
        self.data_dist.initialize(mrms_sftp_client)
        self.target_field = target_field
        self.dataset_meta: TableDatasetMetaChief = TableDatasetMetaChief(target_field=self.target_field)

    def determine_n_workers(self):
        try:
            instances = self.job_info.get_instances()
            n_workers = int(instances / Constants.DISTRIBUTE_INSTANCES_TABLE)
            if instances % Constants.DISTRIBUTE_INSTANCES_TABLE == 0:
                return n_workers
            else:
                return n_workers + 1
        except Exception:
            return super().determine_n_workers()

    def load_local_meta(self, idx, curr_cycle):
        f = self.mrms_sftp_client.open(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_WORKER_{self.job_info.get_job_id()}_{idx}_{curr_cycle-1}.meta",
            "r"
        )
        json_f = json.loads(f.read())
        meta_list = json_f.get("meta")
        dataset_meta_list = json_f.get("dataset_meta_list")
        for idx, _meta in enumerate(meta_list):
            _meta.get("statistics").update(dataset_meta_list[idx])
        f.close()
        return meta_list

    def load(self, **kwargs) -> None:
        self.dataset_meta.initialize(self.job_info)

        try:
            f = self.sftp_client.open("{}/{}".format(self.job_info.get_filepath(), self.job_info.get_filename()), "r")
        except FileNotFoundError as e:
            self.logger.error(e, exc_info=True)
            raise FileLoadError(f"{self.job_info.get_filepath()}/{self.job_info.get_filename()}")
        except Exception as e:
            self.logger.error(e, exc_info=True)
            raise ETCException

        self.data_dist.make_fileline_list()

        while True:
            line = f.readline()
            if not line:
                break
            json_data = json.loads(line)
            self.dataset_meta.apply(json_data, 0)
            self.data_dist.write(json_data)
        f.close()
        self.data_dist.close()

        self.dataset_meta.set_field_type()
        self.write_meta(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_CHIEF_{self.job_info.get_job_id()}_0.meta"
        )

    def generate_meta(self) -> Dict:
        return {
            "file_list": self.data_dist.get_file_list(),
            "file_num_line": self.data_dist.get_fileline_list(),
            "meta": self.dataset_meta.get_meta_list(),
            "dataset_meta": self.dataset_meta.get_meta_dataset(),
            "dataset_meta_list": self.dataset_meta.get_dataset_meta_list()
        }
