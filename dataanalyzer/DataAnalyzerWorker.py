# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2020 AI Service Model Team, R&D Center.

# ---- python base packages

from dataanalyzer.common.Common import Common
from dataanalyzer.manager.DataAnalyzerWorkerManager import DataAnalyzerWorkerManager
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
            self.da_manager.data_loader()
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
