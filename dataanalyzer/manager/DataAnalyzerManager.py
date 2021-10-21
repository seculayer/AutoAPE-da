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
    def __init__(self, job_id, job_idx):
        self.logger = Common.LOGGER.get_logger()

        filename = Constants.DIR_DATA_ROOT + "/{}_{}.job".format(job_id, job_idx)
        self.job_info = DAJobInfo(filename)
        self.logger.info(str(self.job_info))

        self.mrms_sftp_manager: SFTPClientManager = SFTPClientManager(Constants.MRMS_SVC, Constants.MRMS_USER, Constants.MRMS_PASSWD)

        self.logger.info("DataAnalyzerManager initialized.")


if __name__ == '__main__':
    dam = DataAnalyzerManager("ID", "0")
