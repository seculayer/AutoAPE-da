# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
import json
from typing import Dict

import paramiko

from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.distributor.DataDistributor import DataDistributor
from dataanalyzer.info.DAJobInfo import DAJobInfo


class DataDistributorImage(DataDistributor):
    def __init__(self, job_info: DAJobInfo, num_worker=1):
        DataDistributor.__init__(self, job_info, num_worker)

    def write_image(self, data: Dict, buffer):
        filename = data.get("file_conv_nm")
        dst_f = self.mrms_sftp_client.open("{}/{}".format(self.get_folder(), filename), "wb")
        try:
            dst_f.write(buffer)
        except Exception:
            self.logger.error("file error: {}/{}".format(self.get_folder(), filename))
        finally:
            dst_f.close()

