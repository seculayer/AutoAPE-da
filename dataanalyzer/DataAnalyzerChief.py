# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2020 AI Service Model Team, R&D Center.

# ---- python base packages
import time
from datetime import datetime

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.manager.DataAnalyzerChiefManager import DataAnalyzerChiefManager
from pycmmn.KubePodSafetyTermThread import KubePodSafetyTermThread
from pycmmn.Singleton import Singleton


# class : DataAnalyzer
class DataAnalyzerChief(KubePodSafetyTermThread, metaclass=Singleton):
    def __init__(self, job_id: str):
        KubePodSafetyTermThread.__init__(self)
        self.logger = Common.LOGGER.getLogger()

        self.da_manager = DataAnalyzerChiefManager()
        self.job_id = job_id

        try:
            self.da_manager.initialize(job_id, "0")
            self.logger.info("DataAnalyzer Initialized!")
        except Exception as e:
            self.logger.error(e, exc_info=True)

    def run(self) -> None:
        try:
            self.da_manager.data_loader()

            # request to mrms for worker create
            self.da_manager.request_worker_create()

            # monitoring worker end
            start_time = datetime.now()
            while not self.da_manager.monitor_worker_end():
                time.sleep(1)
                if (datetime.now() - start_time).total_seconds() >= Constants.WORKER_WAITING_TIMEOUT:
                    break

            # calculate to global meta feature
            self.da_manager.calculate_global_meta()

            self.da_manager.request_update_dataset_status(self.job_id, Constants.STATUS_DA_RM_REQ)

        except Exception as e:
            self.logger.error(e, exc_info=True)
            self.da_manager.request_update_dataset_status(self.job_id, Constants.STATUS_ERROR)

        finally:
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
