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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

# This module contains functions related to logging.

import logging
import sys
from time import gmtime
from pilot.util.config import config

logger = logging.getLogger(__name__)


def establish_logging(debug: bool = True, nopilotlog: bool = False, filename: str = config.Pilot.pilotlog, loglevel: int = 0, redirectstdout: str = ""):
    """
    Setup and establish logging.

    Option loglevel can be used to decide which (predetermined) logging format to use.
    Example:
      loglevel=0: '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
      loglevel=1: 'ts=%(asctime)s level=%(levelname)-8s event=%(name)-32s.%(funcName)-25s msg="%(message)s"'

    All stdout can be redirected to /dev/null (or to a file). Basically required in prompt processing, or there
    will be too much stdout. If to a file, it is recommended to then also set an appropriate max pilot lifetime
    to prevent it from creating too much stdout.

    :param debug: debug mode (Boolean),
    :param nopilotlog: True when pilot log is not known (Boolean).
    :param filename: name of log file (string).
    :param loglevel: selector for logging level (int).
    :param redirectstdout: file name, or /dev/null (string).
    """

    if redirectstdout:
        with open(redirectstdout, 'w', encoding="utf-8") as sys.stdout:
            pass  # use with open to prevent pylint complaint

    if loglevel:
        pass

    _logger = logging.getLogger('')
    _logger.handlers = []
    _logger.propagate = False

    console = logging.StreamHandler(sys.stdout)
    console.name = 'stream_handler'
    if debug:
        format_str = '%(asctime)s | %(levelname)-8s | %(name)-32s | %(funcName)-25s | %(message)s'
        level = logging.DEBUG
    else:
        format_str = '%(asctime)s | %(levelname)-8s | %(message)s'
        level = logging.INFO
    #rank, maxrank = get_ranks_info()
    #if rank is not None:
    #    format_str = 'Rank {0} |'.format(rank) + format_str
    if nopilotlog:
        logging.basicConfig(level=level, format=format_str, filemode='w')
    else:
        logging.basicConfig(filename=filename, level=level, format=format_str, filemode='w')
    console.setLevel(level)
    console.setFormatter(logging.Formatter(format_str))
    logging.Formatter.converter = gmtime
    _logger.addHandler(console)


def flush_handler(name: str = ""):
    """
    Flush the stdout buffer for the given handler.
    Useful e.g. in case of time-out exceptions.

    :param name: name of handler (string)
    """

    if not name:
        return
    for handler in logging.getLogger().handlers:
        if handler.name == name:
            handler.flush()  # make sure that stdout buffer gets flushed
