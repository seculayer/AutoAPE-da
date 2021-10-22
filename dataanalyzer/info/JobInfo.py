# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

from dataanalyzer.util.JSONUtils import JSONUtils


class JobInfo(object):
    def __init__(self, filename):
        self.job_info_dict = JSONUtils.read_json_from_file(filename)
