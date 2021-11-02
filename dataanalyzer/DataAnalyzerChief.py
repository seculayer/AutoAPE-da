# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2020 AI Service Model Team, R&D Center.

# ---- python base packages
import time

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.manager.DataAnalyzerChiefManager import DataAnalyzerChiefManager
from dataanalyzer.util.KubePodSafetyTermThread import KubePodSafetyTermThread
from dataanalyzer.util.Singleton import Singleton


# class : DataAnalyzer
class DataAnalyzerChief(KubePodSafetyTermThread, metaclass=Singleton):
    def __init__(self, job_id: str):
        KubePodSafetyTermThread.__init__(self)
        self.logger = Common.LOGGER.get_logger()

        self.da_manager = DataAnalyzerChiefManager()
        self.da_manager.initialize(job_id, "0")
        self.job_id = job_id

        self.logger.info("DataAnalyzer Initialized!")

    def run(self) -> None:
        self.da_manager.data_loader()

        # request to mrms for worker create
        self.da_manager.request_worker_create()

        # monitoring worker end
        while not self.da_manager.monitor_worker_end():
            time.sleep(1)

        # calculate to global meta feature
        self.da_manager.calculate_global_meta()

        # update status
        self.da_manager.request_update_dataset_status(self.job_id, Constants.STATUS_DATASET_COMPLETE)

        # bye
        self.da_manager.request_da_terminate()
        self.da_manager.terminate()
        self.logger.info("DataAnalyzer terminate!")


# ---- main function
if __name__ == '__main__':
    import sys

    j_id = sys.argv[1]

    data_analyzer = DataAnalyzerChief(j_id)
    data_analyzer.start()
    data_analyzer.join()
