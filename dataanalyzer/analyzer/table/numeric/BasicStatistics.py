# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.

class BasicStatistics(object):
    def __init__(self):
        self.min = None
        self.max = None
        self.mean = 0
        self.instances = 0
        self.sum = 0

    def apply(self, val):
        if self.min is None:
            self.min = val
        else:
            if self.min > val:
                self.min = val
        if self.max is None:
            self.max = val
        else:
            if self.max < val:
                self.max = val

        self.instances += 1
        self.sum += val

    def calculate(self):
        if self.instances == 0:
            self.mean = 0.0
            return
        self.mean = self.sum / self.instances

    def __str__(self):
        return "{" + "min: {}, max: {}, mean: {}".format(
            self.min, self.max, self.mean) \
               + "}"
