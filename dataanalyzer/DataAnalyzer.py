# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2020 AI Service Model Team, R&D Center.

# ---- python base packages
import time

from dataanalyzer.common.Common import Common
from dataanalyzer.manager.DataAnalyzerManager import DataAnalyzerManager
from dataanalyzer.util.KubePodSafetyTermThread import KubePodSafetyTermThread
# ---- automl packages
from dataanalyzer.util.Singleton import Singleton


# class : DataAnalyzer
class DataAnalyzer(KubePodSafetyTermThread, metaclass=Singleton):
    def __init__(self, job_id: str, job_idx: str):
        KubePodSafetyTermThread.__init__(self)
        self.logger = Common.LOGGER.get_logger()

        self.da_manager = DataAnalyzerManager(job_id, job_idx)

        self.logger.info("DataAnalyzer Initialized!")

    def run(self) -> None:
        while not self._is_exit():
            time.sleep(1)

        self.logger.info("DataAnalyzer terminate!")


# ---- main function
if __name__ == '__main__':
    import sys

    j_id = sys.argv[1]
    j_idx = sys.argv[2]

    data_analyzer = DataAnalyzer(j_id, j_idx)
    data_analyzer.start()
    data_analyzer.join()
