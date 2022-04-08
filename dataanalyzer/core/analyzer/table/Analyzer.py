# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict


class Analyzer(object):
    def apply(self, val) -> None:
        raise NotImplementedError

    def calculate(self) -> None:
        raise NotImplementedError

    def to_dict(self) -> Dict:
        raise NotImplementedError

    def __str__(self) -> str:
        return str(self.to_dict())
