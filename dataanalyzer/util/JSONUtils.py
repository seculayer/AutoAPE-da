# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

from typing import Dict
import json


# class : JSONUtils
from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient


class JSONUtils(object):
    @staticmethod
    def read_json_from_file(filename: str) -> Dict:
        result_dict = dict()
        with open(filename, "r") as f:
            result_dict = json.loads(f.read())
        return result_dict

    @staticmethod
    def read_sftp_json_from_file(sftp_client: PySFTPClient, filename: str):
        f = sftp_client.open(filename, "r")
        result_dict = json.loads(f.read())
        f.close()
        return result_dict