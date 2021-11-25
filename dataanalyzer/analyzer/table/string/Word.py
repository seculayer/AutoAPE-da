# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jin.kim@seculayer.com
# Powered by Seculayer © 2021 AI Service Model Team, R&D Center.
from typing import Dict

from dataanalyzer.analyzer.table.Analyzer import Analyzer
from dataanalyzer.analyzer.table.string.tokenizer.BasicTokenizer import BasicTokenizer


class Word(Analyzer):
    def __init__(self):
        self.word_dict = dict()
        self.tokenizer = BasicTokenizer()

    def apply(self, val) -> None:
        word_list = self.tokenizer.tokenize(val)

        for word in word_list:
            if self.word_dict.get(word, None) is None:
                self.word_dict[word] = 1
            else:
                self.word_dict[word] += 1

    def calculate(self) -> None:
        pass

    def to_dict(self) -> Dict:
        return {"word": self.word_dict}
