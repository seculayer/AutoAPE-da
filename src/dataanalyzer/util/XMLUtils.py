# -*- coding: utf-8 -*-
# Author : Jin Kim
# e-mail : jinkim@seculayer.co.kr
# Powered by Seculayer © 2017 AI-TF Team

from typing import List, Optional
from xml.etree.ElementTree import ElementTree, parse, fromstring, Element


# class: XMLUtils
class XMLUtils(object):
    @staticmethod
    def xml_load(filename: str) -> ElementTree:
        tree = parse(filename)
        return tree

    @staticmethod
    def xml_load_str(xml_str: str) -> ElementTree:
        tree = ElementTree(fromstring(xml_str))
        return tree

    @classmethod
    def xml_write(cls, filename: str, element: Optional[Element]) -> None:
        ElementTree(cls.indent(element), 4).write(filename, encoding="utf-8", xml_declaration=True)

    @staticmethod
    def xml_parse(root: ElementTree, _key: str) -> List[Element]:
        if root is None:
            return []
        else:
            root_keys = root.findall(_key)
            return root_keys

    @staticmethod
    def indent(elem: Element, level=0):
        i = "\n" + level * "\t"
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "\t"
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
            for elem in elem:
                XMLUtils.indent(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i
        return elem

    @classmethod
    def xml2dict_list(cls, xml_data: List[Element], keys):
        res_dict_list = list()
        for data in xml_data:
            res_dict_list.append(cls._xml2dict(xml_data=data, keys=keys))

        return res_dict_list

    @staticmethod
    def _xml2dict(xml_data: Element, keys):
        res_dict = dict()
        for key in keys:
            try:
                res_dict[key] = xml_data.attrib[key]
            except Exception as e:
                res_dict[key] = None
        return res_dict

    @staticmethod
    def find(xml_data, key):
        return xml_data.find(key).text


if __name__ == '__main__':
    XMLUtils.xml_load(filename="./example.xml")
