# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 Service Model Team, R&D Center.

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
    DIR_DATA_ROOT = _CONFIG.get("dir_data_root", "/eyeCloudAI/data")
    DIR_DATA_ANALYZER = DIR_DATA_ROOT + _CONFIG.get("dir_data_analyzer")
    DIR_DIVISION_PATH = "/eyeCloudAI/data/processing/ape/division"

# Logs
    DIR_LOG = _CONFIG.get("dir_log", "/eyeCloudAI/logs")
    LOG_LEVEL = _CONFIG.get("log_level", "INFO")
    LOG_NAME = _CONFIG.get("log_name", "DataAnalyzer")

    # Hosts
    STORAGE_SVC = _CONFIG.get("storage_svc")
    STORAGE_SFTP_PORT = _CONFIG.get("storage_sftp_port")

    MRMS_SVC = _CONFIG.get("mrms_svc")
    MRMS_SFTP_PORT = _CONFIG.get("mrms_sftp_port")
    MRMS_REST_PORT = int(_CONFIG.get("mrms_rest_port"))

    SSH_USER = _CONFIG.get("ssh_username")
    SSH_PASSWD = _CONFIG.get("ssh_password")

    # DATASET FORMAT
    DATASET_FORMAT_TABLE = "1"

    # TABLE FIELD TYPE
    FIELD_TYPE_NULL = "null"
    FIELD_TYPE_INT = "int"
    FIELD_TYPE_FLOAT = "float"
    FIELD_TYPE_STRING = "string"

    JOB_TYPE_CHIEF = "chief"
    JOB_TYPE_WORKER = "worker"
    DISTRIBUTE_INSTANCES = 10000


if __name__ == '__main__':
    print(Constants.DIR_DATA_ROOT)
