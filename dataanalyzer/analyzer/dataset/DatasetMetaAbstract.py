# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

class DatasetMetaAbstract(object):
    def __init__(self):
        pass

    def apply(self, data):
        raise NotImplementedError
