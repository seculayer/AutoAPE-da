# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 Service Model Team, R&D Center. 

from typing import List

from dataanalyzer.util.sftp.PySFTPClient import PySFTPClient
from dataanalyzer.common.Constants import Constants
from dataanalyzer.common.Common import Common


class SFTPClientManager(object):
    # class : SFTPClientManager

    def __init__(self, service: str, username: str, password: str):
        self.logger = Common.LOGGER.get_logger()
        self.service: List[str] = service.split(":")
        self.username = username
        self.password = password

        self.sftp_client = PySFTPClient(self.service[0], int(self.service[1]),
                                        self.username, self.password)

        self.logger.info("initialized service - [{}] SFTP Client Initialized.".format(service))

    def close(self):
        self.sftp_client.close()


if __name__ == '__main__':
    SFTPClientManager(Constants.MRMS_SVC, Constants.MRMS_USER, Constants.MRMS_PASSWD)
