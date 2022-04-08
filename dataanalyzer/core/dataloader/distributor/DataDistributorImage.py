# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict

from dataanalyzer.core.dataloader.distributor import DataDistributorTable
from dataanalyzer.info.DAJobInfo import DAJobInfo


class DataDistributorImage(DataDistributorTable):
    def __init__(self, job_info: DAJobInfo, num_worker=1):
        DataDistributorTable.__init__(self, job_info, num_worker)

    def write_image(self, data: Dict, buffer):
        filename = data.get("file_conv_nm")
        dst_f = self.mrms_sftp_client.open("{}/{}".format(self.get_folder(), filename), "wb")
        try:
            dst_f.write(buffer)
        except Exception:
            self.logger.error("file error: {}/{}".format(self.get_folder(), filename))
        finally:
            dst_f.close()

