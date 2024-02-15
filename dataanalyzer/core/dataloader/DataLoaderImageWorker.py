# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict, Union
import numpy as np

from dataanalyzer.core.analyzer.ImageDatasetMetaWorker import ImageDatasetMetaWorker
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.utils.ImageUtils import ImageUtils
from pycmmn.sftp.PySFTPClient import PySFTPClient
from pycmmn.exceptions.ETCException import ETCException
from pycmmn.exceptions.FileLoadError import FileLoadError
from pycmmn.exceptions.JsonParsingError import JsonParsingError


class DataLoaderImageWorker(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient, job_idx: str):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.worker_idx = job_idx
        self.dataset_meta: ImageDatasetMetaWorker = ImageDatasetMetaWorker()

    def load_meta(self, curr_cycle):
        f = None
        result: Union[Dict, None] = None
        try:
            f = self.mrms_sftp_client.open(
                f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_CHIEF_{self.job_info.get_job_id()}_{curr_cycle}.meta"
            )
            result: Dict = json.loads(f.read())
        except FileNotFoundError as e:
            self.logger.error(e, exc_info=True)
            raise FileLoadError
        except json.JSONDecodeError as e:
            self.logger.error(e, exc_info=True)
            raise JsonParsingError
        except Exception as e:
            self.logger.error(e, exc_info=True)
            raise ETCException
        finally:
            if f is not None:
                f.close()

        return result

    def load(self, **kwargs) -> None:
        self.dataset_meta.initialize(self.job_info, self.load_meta(kwargs["curr_cycle"]))

        f = self.mrms_sftp_client.open(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/{self.worker_idx}/{self.job_info.get_job_id()}_{self.worker_idx}.done",
            "r"
        )

        while True:
            line = f.readline()
            if not line:
                break
            json_data = json.loads(line)
            img_byte: bytes = self._read_image_binary(json_data)
            img_data: np.array = ImageUtils.load(img_byte)
            self.dataset_meta.apply((img_data, json_data), kwargs["curr_cycle"])

        self.is_end = self.dataset_meta.check_end(kwargs["curr_cycle"])

        f.close()
        self.write_meta(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_WORKER_{self.job_info.get_job_id()}_{self.worker_idx}_{kwargs['curr_cycle']}.meta"
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
            "meta": self.dataset_meta.get_meta_list()
        }
