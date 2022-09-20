#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-2022

import os
import re
import configparser

_default_path = os.path.join(os.path.dirname(__file__), 'default.cfg')
_path = os.environ.get('HARVESTER_PILOT_CONFIG', _default_path)
_default_cfg = _path if os.path.exists(_path) else _default_path


class _ConfigurationSection(object):
    """
    Keep the settings for a section of the configuration file
    """

    def __getitem__(self, item):
        return getattr(self, item)

    def __repr__(self):
        return str(tuple(list(self.__dict__.keys())))

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        else:
            raise AttributeError(f'setting \"{attr}\" does not exist in the section; __dict__={self.__dict__}')


def read(config_file):
    """
    Read the settings from file and return a dot notation object
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
            if value == 'True' or value == 'true':
                value = True
            elif value == 'False' or value == 'false':
                value = False
            elif value == 'None' or value == 'none':
                value = None
            elif re.match(r'^\d+$', value):
                value = int(value)
            setattr(settings, key, value)

        setattr(obj, section, settings)

    return obj


config = read(_default_cfg)
