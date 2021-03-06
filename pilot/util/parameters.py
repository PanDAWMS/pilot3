#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2021

# This module contains functions that are used with the get_parameters() function defined in the information module.

# WARNING: IN GENERAL, NEEDS TO USE PLUG-IN MANAGER

from pilot.info import infosys

import logging
logger = logging.getLogger(__name__)


def get_maximum_input_sizes():
    """
    This function returns the maximum allowed size for all input files. The sum of all input file sizes should not
    exceed this value.

    :return: maxinputsizes (integer value in MB).
    """

    try:
        _maxinputsizes = infosys.queuedata.maxwdir  # normally 14336+2000 MB
    except TypeError as exc:
        from pilot.util.config import config
        _maxinputsizes = config.Pilot.maximum_input_file_sizes  # MB
        logger.warning(f'could not convert schedconfig value for maxwdir: {exc} (will use default value instead - {_maxinputsizes})')

        if isinstance(_maxinputsizes, str) and ' MB' in _maxinputsizes:
            _maxinputsizes = _maxinputsizes.replace(' MB', '')

    try:
        _maxinputsizes = int(_maxinputsizes)
    except Exception as exc:
        _maxinputsizes = 14336 + 2000
        logger.warning(f'failed to convert maxinputsizes to int: {exc} (using value: {_maxinputsizes} MB)')

    return _maxinputsizes


def convert_to_int(parameter, default=None):
    """
    Try to convert a given parameter to an integer value.
    The default parameter can be used to force the function to always return a given value in case the integer
    conversion, int(parameter), fails.

    :param parameter: parameter (any type).
    :param default: None by default (if set, always return an integer; the given value will be returned if
    conversion to integer fails).
    :return: converted integer.
    """

    try:
        value = int(parameter)
    except (ValueError, TypeError):
        value = default

    return value
