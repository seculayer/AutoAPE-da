# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 Service Model Team, R&D Center.

from pycmmn.Singleton import Singleton
from pycmmn.utils.ConfUtils import ConfUtils
from pycmmn.utils.FileUtils import FileUtils

import os
os.chdir(FileUtils.get_realpath(__file__) + "/../../")


# class : Constants
class Constants(metaclass=Singleton):
    # load config xml file
    _CONFIG = ConfUtils.load(filename=os.getcwd() + "/conf/da-conf.xml")

    # Directories
    DIR_DATA_ROOT = _CONFIG.get("dir_data_root", "/eyeCloudAI/data")
    DIR_DIVISION_PATH = DIR_DATA_ROOT + "/processing/ape/division"
    DIR_JOB_PATH = DIR_DATA_ROOT + "/processing/ape/jobs"
    DIR_DA_PATH = DIR_DATA_ROOT + "/processing/ape/da"

# Logs
    DIR_LOG = _CONFIG.get("dir_log", "/eyeCloudAI/logs")
    LOG_LEVEL = _CONFIG.get("log_level", "INFO")
    LOG_NAME = _CONFIG.get("log_name", "DataAnalyzer")

    # Hosts
    STORAGE_SVC = _CONFIG.get("storage_svc")
    STORAGE_SFTP_PORT = _CONFIG.get("storage_sftp_port")

    STORAGE_SSH_USER = _CONFIG.get("storage_ssh_username")
    STORAGE_SSH_PASSWD = _CONFIG.get("storage_ssh_password")

    MRMS_SVC = _CONFIG.get("mrms_svc")
    MRMS_SFTP_PORT = _CONFIG.get("mrms_sftp_port")
    MRMS_REST_PORT = int(_CONFIG.get("mrms_rest_port"))

    MRMS_SSH_USER = _CONFIG.get("mrms_ssh_username")
    MRMS_SSH_PASSWD = _CONFIG.get("mrms_ssh_password")

    # DATASET FORMAT
    DATASET_FORMAT_TEXT = "1"
    DATASET_FORMAT_IMAGE = "2"
    DATASET_FORMAT_TABLE = "3"

    # DATASET STATUS
    STATUS_DA_RM_REQ = "9"
    STATUS_ERROR = "8"

    # TABLE FIELD TYPE
    FIELD_TYPE_NULL = "null"
    FIELD_TYPE_INT = "int"
    FIELD_TYPE_FLOAT = "float"
    FIELD_TYPE_STRING = "string"
    FIELD_TYPE_IMAGE = "image"
    FIELD_TYPE_DATE = "date"
    FIELD_TYPE_LIST = "list"

    JOB_TYPE_CHIEF = "chief"
    JOB_TYPE_WORKER = "worker"
    DISTRIBUTE_INSTANCES_TABLE = int(_CONFIG.get("text_distribute_instances", "100000"))
    DISTRIBUTE_INSTANCES_IMAGE = int(_CONFIG.get("image_distribute_instances", "1024")) * 1024 * 1024

    # FIELD TAG
    TAG_CATEGORY = "Categorical"

    WORKER_WAITING_TIMEOUT = int(_CONFIG.get("worker_waiting_timeout", "86400"))  # 1 day


if __name__ == '__main__':
    print(Constants.DIR_DATA_ROOT)
