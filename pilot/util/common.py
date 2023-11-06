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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

import os
import logging
from typing import Any

from pilot.util.config import config
from pilot.util.constants import PILOT_KILL_SIGNAL
from pilot.util.timing import get_time_since

logger = logging.getLogger(__name__)


def should_abort(args: Any, limit: int = 30, label: str = '') -> bool:
    """
    Abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set).

    :param args: pilot arguments object
    :param limit: optional time limit (int)
    :param label: optional label prepending log messages (string)
    :return: True if graceful_stop has been set (and less than optional time limit has passed since maxtime) or False (bool)
    """

    abort = False
    if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
        if os.environ.get('REACHED_MAXTIME', None) and limit:
            # was the pilot killed?
            was_killed = was_pilot_killed(args.timing)
            time_since = get_time_since('0', PILOT_KILL_SIGNAL, args)
            if time_since < limit and was_killed:
                logger.warning(f'{label}:received graceful stop - {time_since} s ago, continue for now')
            else:
                abort = True
        else:
            logger.warning(f'{label}:received graceful stop - abort after this iteration')
            abort = True

    return abort


def was_pilot_killed(timing: dict) -> bool:
    """
    Was the pilot killed by a KILL signal?

    :param timing: args.timing dictionary (dict)
    :return: True if pilot was killed by KILL signal (bool).
    """

    return any(PILOT_KILL_SIGNAL in timing[i] for i in timing)


def is_pilot_check(check: str = '') -> bool:
    """
    Should the given pilot check be run?

    Consult config.Pilot.checks if the given check is listed.

    :param check: name of check (string)
    :return: True if check is present in config.Pilot.checks (and if config is outdated), False othersise (bool).
    """

    status = False
    if not check:
        return status

    try:
        if check in config.Pilot.checks:
            status = True
    except AttributeError as exc:
        logger.warning(f'attribute Pilot.checks not present in config file - please update: exc={exc}')
        status = True  # to allow check to proceed when config file is outdated

    return status
