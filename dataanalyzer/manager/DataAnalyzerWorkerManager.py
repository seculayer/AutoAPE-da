# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.dataloader.DataLoader import DataLoader
from dataanalyzer.dataloader.DataLoaderFactory import DataLoaderFactory
from dataanalyzer.info.DAJobInfo import DAJobInfo
from dataanalyzer.manager.DataAnalyzerChiefManager import DataAnalyzerChiefManager
from dataanalyzer.manager.SFTPClientManager import SFTPClientManager
from dataanalyzer.util.Singleton import Singleton
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class DataAnalyzerWorkerManager(DataAnalyzerChiefManager, metaclass=Singleton):
    # class : DataAnalyzerWorkerManager
    def __init__(self):
        DataAnalyzerChiefManager.__init__(self)
        self.job_type = Constants.JOB_TYPE_WORKER


if __name__ == '__main__':
    dam = DataAnalyzerWorkerManager("ID", "0")
