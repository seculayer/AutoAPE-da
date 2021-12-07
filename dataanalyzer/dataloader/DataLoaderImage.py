# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict, List

import numpy as np

from dataanalyzer.analyzer.ImageDatasetMetaChief import ImageDatasetMetaChief
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataDistributor import DataDistributor
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.util.ImageUtils import ImageUtils
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataLoaderImage(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.num_worker = self.determine_n_workers(self.job_info.get_instances())
        self.data_dist = DataDistributor(job_info, self.num_worker)
        self.data_dist.initialize(mrms_sftp_client)

    @staticmethod
    def determine_n_workers(instances):
        n_workers = int(instances / Constants.DISTRIBUTE_INSTANCES_IMAGE)
        if instances % Constants.DISTRIBUTE_INSTANCES_IMAGE == 0:
            return n_workers
        else:
            return n_workers + 1

    def load(self) -> None:
        f = self.sftp_client.open("{}/{}".format(self.job_info.get_filepath(), self.job_info.get_filename()), "r")
        self.dataset_meta: ImageDatasetMetaChief = ImageDatasetMetaChief()
        self.dataset_meta.initialize(self.job_info)
        idx = 0
        while True:
            line = f.readline()
            if not line:
                self.logger.info("file : {} / {} loaded...".format(idx, self.job_info.get_instances()))
                break
            json_data = json.loads(line)
            # print(json_data)

            img_data: np.array = self._read_image(json_data)
            self.dataset_meta.apply(img_data)
            self.dataset_meta.apply_annotation(json_data)
            # print(json_data)
            # break
            # self.data_dist.write(json_data)
            idx += 1
            if idx % 100 == 0:
                self.logger.info("file : {} / {} loaded...".format(idx, self.job_info.get_instances()))

        # self.data_dist.make_fileline_list()

        self.dataset_meta.calculate()
        f.close()
        self.data_dist.close()
        self.write_meta("{}/DA_CHIEF_{}.meta".format(Constants.DIR_DIVISION_PATH, self.job_info.get_job_id()))

    def _read_image(self, annotation_data: Dict) -> np.array:
        filepath = annotation_data.get("file_path")
        filename = annotation_data.get("file_conv_nm")
        img_f = self.sftp_client.open("{}/{}".format(filepath, filename, "rb"))
        return ImageUtils.load(img_f.read())

    def generate_meta(self) -> Dict:
        return {
            "folder_list": self.data_dist.get_file_list(),
            "file_num": self.data_dist.get_fileline_list(),
            "meta": self.dataset_meta.get_meta_list(),
            "dataset_meta": self.dataset_meta.get_meta_dataset()
        }

    def global_meta(self) -> None:
        # load local meta info
        local_meta_list: List = list()
        for idx in range(self.get_num_worker()):
            local_meta_list.append(self.load_local_meta(idx))

        self.dataset_meta.calculate_global_meta(local_meta_list)
        self.write_meta("{}/DA_META_{}.info".format(Constants.DIR_DIVISION_PATH, self.job_info.get_job_id()))
