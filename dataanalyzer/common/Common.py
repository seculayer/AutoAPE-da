# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 Service Model Team, R&D Center.

# ---- automl packages
from dataanalyzer.util.Singleton import Singleton
from dataanalyzer.util.logger.MPLogger import MPLogger
from dataanalyzer.util.FileUtils import FileUtils
from dataanalyzer.common.Constants import Constants


# class : class_name
class Common(metaclass=Singleton):
    # make directories
    FileUtils.mkdir(Constants.DIR_DATA_ROOT)
    FileUtils.mkdir(Constants.DIR_LOG)
    FileUtils.mkdir(Constants.DIR_DA_PATH)

    # LOGGER
    LOGGER: MPLogger = MPLogger(log_dir=Constants.DIR_LOG, log_level=Constants.LOG_LEVEL,
                      log_name=Constants.LOG_NAME)
