#!/usr/bin/env python
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-24

import logging
from xml.dom import minidom
from xml.etree import ElementTree

logger = logging.getLogger(__name__)


class XMLDictionary:
    """
    This is a helper class that is used to create the dictionary which is converted to the special XML files for
    Nordugrid pilots.
    Example dictionary:
      dictionary = { "outfiles": [ { "file": { "surl": "some_surl", "size": "123", "ad32": "aaaaaaa",
                                               "guid": "ababa22", "lfn": "some_lfn", "dataset": "some_dataset",
                                               "date": "11/11/11" } },
                                    {}, {}, ..
                                 ]
                    }

    Usage:
      xmldic = XMLDictionary()
      xmldic.add_to_list({"surl": "some_surl1", "size": "123", "ad32": "aaaaaaa", "guid": "ababa22", "lfn": "some_lfn",
                          "dataset": "some_dataset", "date": "11/11/11"})
      dictionary = xmldic.get_dictionary()
    """

    _dictionary = None

    def __init__(self, rootname: str = "outfiles"):
        """
        Initialize the dictionary with the root key.

        :param rootname: name of the root key. There is only one root key in the Nordugrid XML file ('outfiles') (str).
        """
        self._dictionary = {}
        self._dictionary[rootname] = []

    def add_to_list(self, dictionary: dict, rootname: str = "outfiles", itemname: str = "file"):
        """
        Add dictionary to itemname key.

        See example in class header.

        :param dictionary: dictionary to add to itemname key (dict)
        :param rootname: name of the root key. There is only one root key in the Nordugrid XML file ('outfiles') (str)
        :param itemname: name of the item key. In the Nordugrid XML it should be called 'file' (str).
        """
        if isinstance(self._dictionary, dict):
            if isinstance(self._dictionary[rootname], list):
                _dic = {itemname: dictionary}
                self._dictionary[rootname].append(_dic)
            else:
                pass
        else:
            logger.info(f"not a dictionary: {self._dictionary}")

    def get_dictionary(self) -> dict:
        """
        Return the dictionary to be converted to XML.

        It should be populated with the dictionary added to it in add_to_list().

        :return: dictionary (dict).
        """
        return self._dictionary


def convert_to_xml(dictionary: dict) -> str:
    """
    Convert a dictionary to XML.
    The dictionary is expected to follow the Nordugrid format. See the XMLDictionary helper class.

    Example of XML (OutputFiles.xml):

    <?xml version="1.0" ?>
    <outfiles>
    <file>
      <ad32>aaaaaaa</ad32>
      <surl>some_surl1</surl>
      <lfn>some_lfn</lfn>
      <dataset>some_dataset</dataset>
      <date>11/11/11</date>
      <guid>ababa22</guid>
      <size>123</size>
    </file>
    </outfiles>

    :param dictionary: dictionary created with XMLDictionary.
    :return: pretty printed xml (str).
    """
    failed = False

    single_file_tag = list(dictionary.keys())
    if len(single_file_tag) != 1:
        logger.warning(f"unexpected format - expected single entry, got {len(single_file_tag)} entries")
        logger.warning(f'dictionary = {dictionary}')

        return None

    file_tag = single_file_tag[0]
    root = ElementTree.Element(file_tag)

    file_list = dictionary[file_tag]
    if isinstance(file_list, list):
        for file_entry in file_list:
            if isinstance(file_entry, dict) and len(file_entry) == 1:
                single_entry = list(file_entry.keys())[0]  # Python 2/3

                # add the 'file' element
                file_element = ElementTree.Element(single_entry)
                root.append(file_element)

                file_dictionary = file_entry[single_entry]
                if isinstance(file_dictionary, dict):
                    for dictionary_entry in list(file_dictionary.keys()):  # Python 2/3
                        # convert all entries to xml elements
                        entry = ElementTree.SubElement(file_element, dictionary_entry)
                        entry.text = file_dictionary[dictionary_entry]
                else:
                    logger.warning(f"unexpected format - expected a dictionary, got {file_dictionary}")
                    failed = True
            else:
                logger.warning(f"unexpected format - expected a length 1 dictionary, got {file_entry}")
                failed = True
    else:
        logger.warning(f"unexpected format - expected a list, got {file_list}")
        failed = True

    if failed:
        return None

    # generate pretty print
    return minidom.parseString(ElementTree.tostring(root)).toprettyxml(indent="   ")
