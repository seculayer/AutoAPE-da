# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 Service Model Team, R&D Center.
from typing import List

from dataanalyzer.info.JobInfo import JobInfo


# class : DAJobInfo
class DAJobInfo(JobInfo):
    def __init__(self, filename: str):
        JobInfo.__init__(self, filename)

        self.job_id = self.job_info_dict.get("job_id")
        self.job_idx = self.job_info_dict.get("job_idx")
        self.job_type = self.job_info_dict.get("job_type")
        self.cluster: List = self.job_info_dict.get("cluster")

    def __str__(self) -> str:
        return "DataAnalyzerJobInfo = {" + \
               "job_id : {}, job_idx : {}, job_type: {}, cluster: {}".format(
                   self.job_id, self.job_idx, self.job_type, self.cluster
               ) + \
               "}"
