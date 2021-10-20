# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 Service Model Team, R&D Center. 

import os

from dataanalyzer.util.Singleton import Singleton
from dataanalyzer.util.ConfigUtils import ConfigUtils
from dataanalyzer.util.FileUtils import FileUtils

import os
os.chdir(FileUtils.get_realpath(__file__) + "/../../")


# class : Constants
class Constants(metaclass=Singleton):
    # load config xml file
    _CONFIG = ConfigUtils.load(os.getcwd() + "/conf/da-conf.xml")

    # Directories
    DIR_DATA_ROOT = _CONFIG.get("dir_data_root")

    # Logs
    DIR_LOG = _CONFIG.get("dir_log", "./logs")
    LOG_LEVEL = _CONFIG.get("log_level", "INFO")
    LOG_NAME = _CONFIG.get("log_name", "DataAnalyzer")

    # Hosts
    MRMS_SVC = _CONFIG.get("mrms_svc")
    MRMS_USER = _CONFIG.get("mrms_username")
    MRMS_PASSWD = _CONFIG.get("mrms_password")


if __name__ == '__main__':
    print(Constants.DIR_DATA_ROOT)

