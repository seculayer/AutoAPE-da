# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

class Analyzer(object):
    def apply(self, val):
        raise NotImplementedError

    def calculate(self):
        raise NotImplementedError

    def to_dict(self):
        raise NotImplementedError

    def __str__(self) -> str:
        return str(self.to_dict())
