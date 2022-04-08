# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict

import numpy as np

from dataanalyzer.core.analyzer.table.Analyzer import Analyzer


class ImageShape(Analyzer):
    KEY = "shape"

    def __init__(self):
        self.width: Dict = {"min": None, "max": None, "mean": 0, "sum": 0}
        self.height: Dict = {"min": None, "max": None, "mean": 0, "sum": 0}
        self.channel: Dict = {"min": None, "max": None, "mean": 0, "sum": 0}
        self.value: Dict = {"min": None, "max": None, "mean": 0, "sum": 0}

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
        self.value = self._calc_val_dict(val, self.value)

    def calculate(self) -> None:
        self.width["mean"] = self.width["sum"] / self.instances
        self.height["mean"] = self.height["sum"] / self.instances
        self.channel["mean"] = self.channel["sum"] / self.instances

    def to_dict(self) -> Dict:
        rst: dict = {
            # self.KEY: {
            #     "width": self.width, "height": self.height, "channel": self.channel
            # }
            "width": self.width, "height": self.height, "channel": self.channel,
            "sum": self.value.get("sum"), "mean": self.value.get("mean"),
            "max": self.value.get("max"), "min": self.value.get("min"),
        }
        return rst

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

    @staticmethod
    def _calc_val_dict(image_arr: np.array, data_dict) -> Dict:
        im_shape: tuple = image_arr.shape
        n_pixel: int = int(np.prod(im_shape))
        data_dict["sum"] = int(np.sum(image_arr))
        data_dict["mean"] = data_dict["sum"] / n_pixel
        data_dict["max"] = int(np.max(image_arr))
        data_dict["min"] = int(np.min(image_arr))

        return data_dict
