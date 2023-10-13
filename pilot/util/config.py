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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23

import os
import re
import configparser
from typing import Any

_default_path = os.path.join(os.path.dirname(__file__), 'default.cfg')
_path = os.environ.get('HARVESTER_PILOT_CONFIG', _default_path)
_default_cfg = _path if os.path.exists(_path) else _default_path


class _ConfigurationSection():
    """
    Keep the settings for a section of the configuration file
    """

    def __getitem__(self, item: Any) -> Any:
        return getattr(self, item)

    def __repr__(self) -> str:
        return str(tuple(list(self.__dict__.keys())))

    def __getattr__(self, attr: Any) -> Any:
        if attr in self.__dict__:
            return self.__dict__[attr]
        raise AttributeError(f'setting \"{attr}\" does not exist in the section; __dict__={self.__dict__}')


def read(config_file: Any) -> Any:
    """
    Read the settings from file and return a dot notation object

    :param config_file: file
    :return: attribute object.
    """

    _config = configparser.ConfigParser()
    _config.read(config_file)

    obj = _ConfigurationSection()

    for section in _config.sections():

        settings = _ConfigurationSection()
        for key, value in _config.items(section):
            # handle environmental variables
            if value.startswith('$'):
                tmpmatch = re.search(r'\$\{*([^\}]+)\}*', value)
                envname = tmpmatch.group(1)
                if envname not in os.environ:
                    raise KeyError(f'{envname} in the cfg is an undefined environment variable')
                value = os.environ.get(envname)
            # convert to proper types
            if value in {'True', 'true'}:
                value = True
            elif value in {'False', 'false'}:
                value = False
            elif value in {'None', 'none'}:
                value = None
            elif re.match(r'^\d+$', value):
                value = int(value)
            setattr(settings, key, value)

        setattr(obj, section, settings)

    return obj


config = read(_default_cfg)
