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
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-24
# - Tadashi Maeno, tadashi.maeno@cern.ch, 2020

import logging
import os

from pilot.info.jobdata import JobData
from pilot.util.config import config
from pilot.util.filehandling import read_file, tail

logger = logging.getLogger(__name__)


def interpret(job: JobData) -> int:
    """
    Interpret the payload, look for specific errors in the stdout.

    :param job: job object (JobData)
    :return: exit code (payload) (int).
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    if os.path.exists(stdout):
        message = 'payload stdout dump\n'
        message += read_file(stdout)
        logger.debug(message)
    else:
        logger.warning('payload produced no stdout')
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
    if os.path.exists(stderr):
        message = 'payload stderr dump\n'
        message += read_file(stderr)
        logger.debug(message)
    else:
        logger.info('payload produced no stderr')

    return 0


def get_log_extracts(job: JobData, state: str) -> str:
    """
    Extract special warnings and other info from special logs.

    This function also discovers if the payload had any outbound connections.

    :param job: job object (JobData)
    :param state: job state (str)
    :return: log extracts (str).
    """
    logger.info("building log extracts (sent to the server as \'pilotLog\')")

    # for failed/holding jobs, add extracts from the pilot log file, but always add it to the pilot log itself
    extracts = ""
    _extracts = get_pilot_log_extracts(job)
    if _extracts != "":
        logger.warning(f'detected the following tail of warning/fatal messages in the pilot log:\n{_extracts}')
        if state in {'failed', 'holding'}:
            extracts += _extracts

    return extracts


def get_pilot_log_extracts(job: JobData) -> str:
    """
    Get the extracts from the pilot log (warning/fatal messages, as well as tail of the log itself).

    :param job: job object (JobData)
    :return: tail of pilot log (str).
    """
    extracts = ""

    path = os.path.join(job.workdir, config.Pilot.pilotlog)
    if os.path.exists(path):
        # get the last 20 lines of the pilot log in case it contains relevant error information
        _tail = tail(path, nlines=20)
        if _tail != "":
            if extracts != "":
                extracts += "\n"
            extracts += f"- Log from {config.Pilot.pilotlog} -\n"
            extracts += _tail
    else:
        logger.warning(f'pilot log file does not exist: {path}')

    return extracts
