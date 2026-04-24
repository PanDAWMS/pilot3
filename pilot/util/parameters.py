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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-24

"""Parameter resolution helpers used with the information module's get_parameters()."""

# This module contains functions that are used with the get_parameters() function defined in the information module.

# WARNING: IN GENERAL, NEEDS TO USE PLUG-IN MANAGER

import logging
from typing import Any

from pilot.info import infosys
from pilot.util.config import config

logger = logging.getLogger(__name__)


def get_maximum_input_sizes() -> int:
    """Return the maximum allowed total size for all input files in MB.

    Fetches the value from ``infosys.queuedata.maxwdir``. Falls back to
    ``config.Pilot.maximum_input_file_sizes`` if the queue data is unavailable,
    and finally defaults to 16336 MB if all conversions fail.

    Returns:
        Maximum combined input file size in MB.
    """
    try:
        _maxinputsizes = infosys.queuedata.maxwdir  # normally 14336+2000 MB
    except TypeError as exc:
        _maxinputsizes = config.Pilot.maximum_input_file_sizes  # MB
        logger.warning(f'could not convert schedconfig value for maxwdir: {exc} (will use default value instead - {_maxinputsizes})')

        if isinstance(_maxinputsizes, str) and ' MB' in _maxinputsizes:
            _maxinputsizes = _maxinputsizes.replace(' MB', '')

    try:
        _maxinputsizes = int(_maxinputsizes)
    except (ValueError, TypeError) as exc:
        _maxinputsizes = 14336 + 2000
        logger.warning(f'failed to convert maxinputsizes to int: {exc} (using value: {_maxinputsizes} MB)')

    return _maxinputsizes


def convert_to_int(parameter: Any, default: Any = None) -> Any:
    """Try to convert *parameter* to an integer.

    Args:
        parameter: Value to convert.
        default: Value returned when conversion raises ``ValueError`` or
            ``TypeError``. Defaults to ``None``.

    Returns:
        Integer conversion of *parameter*, or *default* if conversion fails.
    """
    try:
        value = int(parameter)
    except (ValueError, TypeError):
        value = default

    return value
