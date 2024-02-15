# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict, Union

from dataanalyzer.core.analyzer.TableDatasetMetaWorker import TableDatasetMetaWorker
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.dataloader.DataLoader import DataLoader
from dataanalyzer.info.DAJobInfo import DAJobInfo
from pycmmn.sftp.PySFTPClient import PySFTPClient
from pycmmn.exceptions.ETCException import ETCException
from pycmmn.exceptions.FileLoadError import FileLoadError
from pycmmn.exceptions.JsonParsingError import JsonParsingError


class DataLoaderTableWorker(DataLoader):
    def __init__(self, job_info: DAJobInfo, sftp_client: PySFTPClient, mrms_sftp_client: PySFTPClient, job_idx: str):
        DataLoader.__init__(self, job_info, sftp_client, mrms_sftp_client)
        self.worker_idx = job_idx
        self.dataset_meta: TableDatasetMetaWorker = TableDatasetMetaWorker(worker_idx=job_idx)

    def load_meta(self, curr_cycle):
        f = None
        result: Union[Dict, None] = None
        try:
            file_name = f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_CHIEF_{self.job_info.get_job_id()}_{curr_cycle}.meta"
            f = self.mrms_sftp_client.open(
                file_name
            )
            result = json.loads(f.read())
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
        self.dataset_meta.initialize(self.job_info, meta_json=self.load_meta(kwargs["curr_cycle"]))

        f = self.mrms_sftp_client.open(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/{self.worker_idx}/{self.job_info.get_job_id()}_{self.worker_idx}.done",
            "r"
        )

        while True:
            line = f.readline()
            if not line:
                break
            json_data = json.loads(line)
            self.dataset_meta.apply(json_data, kwargs["curr_cycle"])

        self.is_end = self.dataset_meta.check_end(kwargs["curr_cycle"])

        f.close()
        self.write_meta(
            f"{Constants.DIR_DA_PATH}/{self.job_info.get_job_id()}/DA_WORKER_{self.job_info.get_job_id()}_{self.worker_idx}_{kwargs['curr_cycle']}.meta"
        )

    def generate_meta(self) -> Dict:
        return {
            "meta": self.dataset_meta.get_meta_list(),
            "dataset_meta_list": self.dataset_meta.get_dataset_meta_list()
        }

    def global_meta(self, n_cycle):
        raise NotImplementedError
