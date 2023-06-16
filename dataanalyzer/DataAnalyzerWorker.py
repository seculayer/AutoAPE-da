# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2020 AI Service Model Team, R&D Center.

# ---- python base packages
from datetime import datetime
import time

from dataanalyzer.common.Common import Common
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.manager.DataAnalyzerWorkerManager import DataAnalyzerWorkerManager
from pycmmn.KubePodSafetyTermThread import KubePodSafetyTermThread
from pycmmn.Singleton import Singleton


# class : DataAnalyzer
class DataAnalyzerWorker(KubePodSafetyTermThread, metaclass=Singleton):
    def __init__(self, job_id: str, job_idx: str):
        KubePodSafetyTermThread.__init__(self)
        self.logger = Common.LOGGER.getLogger()

        self.da_manager = DataAnalyzerWorkerManager()
        try:
            self.da_manager.initialize(job_id, job_idx)
            self.logger.info("DataAnalyzer Initialized!")
        except Exception as e:
            self.logger.error(e, exc_info=True)

    def run(self) -> None:
        try:
            curr_cycle = 0
            timeout_flag = False
            while True:
                # monitoring cheif end
                start_time = datetime.now()
                while not self.da_manager.monitor_chief_end(curr_cycle):
                    time.sleep(1)
                    if (datetime.now() - start_time).total_seconds() >= Constants.WAITING_TIMEOUT:
                        self.logger.error(f"CHIEF Waiting Time Out...")
                        timeout_flag = True
                        break
                self.da_manager.data_loader(curr_cycle=curr_cycle)

                assert not timeout_flag

                if self.da_manager.check_end():
                    break
                curr_cycle += 1
        except Exception as e:
            self.logger.error(e, exc_info=True)
        finally:
            self.da_manager.terminate()
            self.logger.info("DataAnalyzer terminate!")


# ---- main function
if __name__ == '__main__':
    import sys

    j_id = sys.argv[1]
    j_idx = sys.argv[2]

    data_analyzer = DataAnalyzerWorker(j_id, j_idx)
    data_analyzer.start()
    data_analyzer.join()
