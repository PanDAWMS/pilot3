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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023-24

"""Functions related to heartbeat messages. It is especially needed for the pilot to know if it has been suspended."""

import logging
import os
import threading
import time

# from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    PilotException,
    FileHandlingFailure,
    ConversionFailure
)
from pilot.util.config import config
from pilot.util.filehandling import (
    read_json,
    write_json
)

lock = threading.Lock()
logger = logging.getLogger(__name__)
# errors = ErrorCodes()


def update_pilot_heartbeat(update_time: float, detected_job_suspension: bool = False, time_since_detection: int = 0, name: str = 'pilot') -> bool:
    """
    Update the pilot heartbeat file.

    Dictionary = {last_pilot_heartbeat: <int>, last_server_update: <int>, ( last_looping_check: {job_id: <int>: <int>}, .. ) }
    (optionally add looping job info later).

    :param update_time: time of last update (float)
    :param detected_job_suspension: True if a job suspension was detected, False otherwise (bool)
    :param time_since_detection: time since the job suspension was detected, in seconds (int)
    :param name: name of the heartbeat to update, 'pilot' or 'server' (str)
    :return: True if successfully updated heartbeat file, False otherwise (bool).
    """
    path = os.path.join(os.getenv('PILOT_HOME', os.getcwd()), config.Pilot.pilot_heartbeat_file)
    dictionary = read_pilot_heartbeat(path)
    if not dictionary:  # redundancy
        dictionary = {}

    with lock:
        # add the diff time (time between updates) to the dictionary if not present (ie the first time)
        if not dictionary.get('max_diff_time', None):
            # ie add the new field
            dictionary['max_diff_time'] = 0
        if not dictionary.get(f'last_{name}_update', None):
            # ie add the new field
            dictionary[f'last_{name}_update'] = int(update_time)
        max_diff_time = int(update_time) - dictionary.get(f'last_{name}_update', 0)
        if max_diff_time >= dictionary.get('max_diff_time', 0):
            dictionary['max_diff_time'] = max_diff_time
        dictionary[f'last_{name}_update'] = int(update_time)
        dictionary['time_since_detection'] = time_since_detection if detected_job_suspension else 0
        if detected_job_suspension:
            logger.warning(f'job suspension detected: time since detection: {time_since_detection} seconds')
        else:
            logger.debug('no job suspension detected')

        status = write_json(path, dictionary)
        if not status:
            logger.warning(f'failed to update heartbeat file: {path}')
            return False
        else:
            logger.debug(f'updated pilot heartbeat file: {path}')

    return True


def read_pilot_heartbeat(path: str) -> dict:
    """
    Read the pilot heartbeat file.

    :param path: path to heartbeat file (str)
    :return: dictionary with pilot heartbeat info (dict).
    """
    dictionary = {}

    with lock:
        if os.path.exists(path):
            try:
                dictionary = read_json(path)
            except (PilotException, FileHandlingFailure, ConversionFailure) as exc:
                logger.warning(f'failed to read heartbeat file: {exc}')

    return dictionary


def time_since_suspension() -> int:
    """
    Return the time since the pilot detected a job suspension.

    If non-zero, reset the time since detection to zero.

    :return: time since the pilot detected a job suspension (int).
    """
    path = os.path.join(os.getenv('PILOT_HOME', os.getcwd()), config.Pilot.pilot_heartbeat_file)
    dictionary = read_pilot_heartbeat(path)
    if dictionary:
        time_since_detection = dictionary.get('time_since_detection', 0)
        if time_since_detection:
            # reset the time since detection to zero
            update_pilot_heartbeat(time.time(), False, 0)
            logger.info('reset time since detection to zero')
        return time_since_detection

    return 0
