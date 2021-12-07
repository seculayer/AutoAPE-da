# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict

import numpy as np

from dataanalyzer.analyzer.table.Analyzer import Analyzer


class ImageShape(Analyzer):
    KEY = "shape"

    def __init__(self):
        self.width: Dict = {"min": None, "max": None, "mean": 0, "sum": 0}
        self.height: Dict = {"min": None, "max": None, "mean": 0, "sum": 0}
        self.channel: Dict = {"min": None, "max": None, "mean": 0, "sum": 0}

        self.instances = 0

    def apply(self, val: np.array) -> None:
        self.instances += 1
        img_shape = val.shape
        channel = 1
        if len(img_shape) != 2:
            channel = img_shape[2]
        self.width = self._calc_dict(img_shape[0], self.width)
        self.height = self._calc_dict(img_shape[1], self.height)
        self.channel = self._calc_dict(channel, self.channel)

    def calculate(self) -> None:
        self.width["mean"] = self.width["sum"] / self.instances
        self.height["mean"] = self.height["sum"] / self.instances
        self.channel["mean"] = self.channel["sum"] / self.instances

    def to_dict(self) -> Dict:
        return {
            self.KEY: {
                "width": self.width, "height": self.height, "channel": self.channel
            }
        }

    @staticmethod
    def _calc_dict(val: int, data_dict) -> Dict:
        if data_dict.get("min") is None:
            data_dict["min"] = val
            data_dict["max"] = val
        else:
            if data_dict["min"] < val:
                data_dict["min"] = val
            if data_dict["max"] > val:
                data_dict["max"] = val
        data_dict["sum"] += val

        return data_dict
