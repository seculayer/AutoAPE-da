# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

from dataanalyzer.util.JSONUtils import JSONUtils
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class JobInfo(object):
    def __init__(self, sftp_client: PySFTPClient, filename: str):
        self.job_info_dict = JSONUtils.read_sftp_json_from_file(sftp_client, filename)
