# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict

import numpy as np

from dataanalyzer.core.analyzer.ImageDatasetMetaChief import ImageDatasetMetaChief
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.dataloader.DataLoader import DataLoader
from dataanalyzer.core.dataloader.distributor.DataDistributorImage import DataDistributorImage
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.utils.ImageUtils import ImageUtils
from pycmmn.sftp.PySFTPClient import PySFTPClient


class DataLoaderImage(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.data_dist = DataDistributorImage(self.job_info, self.num_worker)
        self.data_dist.initialize(mrms_sftp_client)

    def determine_n_workers(self):
        try:
            instances = self.job_info.get_instances() * self._get_pixels()
            n_workers = int(instances / Constants.DISTRIBUTE_INSTANCES_IMAGE)
            if int(instances % Constants.DISTRIBUTE_INSTANCES_IMAGE) != 0:
                n_workers += 1
            self.logger.info(f"n_worker : {n_workers}")
            return n_workers
        except Exception:
            return super().determine_n_workers()

    def _get_pixels(self) -> int:
        f = self.sftp_client.open("{}/{}".format(self.job_info.get_filepath(), self.job_info.get_filename()), "r")

        while True:
            line = f.readline()
            json_data = json.loads(line)
            pixels = int(json_data.get("img_width")) * int(json_data.get("img_height"))
            break
        f.close()
        return pixels

    def load(self) -> None:
        f = self.sftp_client.open("{}/{}".format(self.job_info.get_filepath(), self.job_info.get_filename()), "r")
        self.dataset_meta: ImageDatasetMetaChief = ImageDatasetMetaChief()
        self.dataset_meta.initialize(self.job_info)

        self.data_dist.make_fileline_list()

        idx = 0
        while True:
            line = f.readline()
            if not line:
                self.logger.info("file : {} / {} loaded...".format(idx, self.job_info.get_instances()))
                break
            json_data = json.loads(line)

            img_byte: bytes = self._read_image_binary(json_data)
            img_data: np.array = ImageUtils.load(img_byte)
            assert img_data is not None, \
                f"{json_data['file_path']/json_data['file_conv_nm']} file is not jpeg format, \
                 orginal file nm : {json_data['file_ori_nm']}"

            self.dataset_meta.apply(img_data)
            self.dataset_meta.apply_annotation(json_data)

            self.data_dist.write_image(json_data, img_byte)
            self.data_dist.write(json_data)

            idx += 1
            if idx % 100 == 0:
                self.logger.info("file : {} / {} loaded...".format(idx, self.job_info.get_instances()))

        self.dataset_meta.calculate()
        f.close()
        self.data_dist.close()
        self.write_meta(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_CHIEF_{self.job_info.get_job_id()}.meta"
        )

    def _read_image_binary(self, annotation_data: Dict) -> bytes:
        filepath = annotation_data.get("file_path")
        filename = annotation_data.get("file_conv_nm")
        img_f = self.sftp_client.open("{}/{}".format(filepath, filename, "rb"))
        img_array = img_f.read()
        img_f.close()
        return img_array

    def generate_meta(self) -> Dict:
        return {
            "file_list": self.data_dist.get_file_list(),
            "file_num": self.data_dist.get_fileline_list(),
            "meta": self.dataset_meta.get_meta_list(),
            "dataset_meta": self.dataset_meta.get_meta_dataset()
        }
