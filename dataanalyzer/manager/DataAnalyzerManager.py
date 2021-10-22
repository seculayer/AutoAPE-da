# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.common.Constants import Constants
from dataanalyzer.common.Common import Common
from dataanalyzer.manager.SFTPClientManager import SFTPClientManager


class DataAnalyzerManager(object):
    # class : DataAnalyzerManager
    def __init__(self, job_id: str, job_idx: str):
        self.logger = Common.LOGGER.get_logger()

        self.mrms_sftp_manager: SFTPClientManager = SFTPClientManager(
            "{}:{}".format(Constants.MRMS_SVC, Constants.MRMS_SFTP_PORT),
            Constants.SSH_USER, Constants.SSH_PASSWD)

        self.storage_sftp_manager: SFTPClientManager = SFTPClientManager(
            "{}:{}".format(Constants.STORAGE_SVC, Constants.STORAGE_SFTP_PORT),
            Constants.SSH_USER, Constants.SSH_PASSWD)

        self.job_info = self.load_job_info(job_id, job_idx)
        self.logger.info(str(self.job_info))

        self.logger.info("DataAnalyzerManager initialized.")

    @staticmethod
    def load_job_info(job_id: str, job_idx: str):
        filename = Constants.DIR_DATA_ROOT + "/{}_{}.job".format(job_id, job_idx)
        return DAJobInfo(filename)


if __name__ == '__main__':
    dam = DataAnalyzerManager("ID", "0")
