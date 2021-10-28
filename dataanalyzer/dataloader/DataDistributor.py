# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from dataanalyzer.info.DAJobInfo import DAJobInfo


class DataDistributor(object):
    def __init__(self, job_info: DAJobInfo, num_worker=1):
        self.job_info = job_info
        self.num_worker = num_worker
