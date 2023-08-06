# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.common.Constants import Constants
from dataanalyzer.core.manager.DataAnalyzerChiefManager import DataAnalyzerChiefManager
from pycmmn.Singleton import Singleton


class DataAnalyzerWorkerManager(DataAnalyzerChiefManager, metaclass=Singleton):
    # class : DataAnalyzerWorkerManager
    def __init__(self):
        DataAnalyzerChiefManager.__init__(self)
        self.job_type = Constants.JOB_TYPE_WORKER

    def monitor_chief_end(self, curr_cycle) -> bool:
        return self.loader.chief_monitor(curr_cycle)


if __name__ == '__main__':
    dam = DataAnalyzerWorkerManager()
