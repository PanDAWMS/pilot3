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

"""Common functions."""

import os
import logging
from typing import Any

from pilot.util.config import config
from pilot.util.constants import PILOT_KILL_SIGNAL
from pilot.util.timing import get_time_since

logger = logging.getLogger(__name__)


def should_abort(args: Any, limit: int = 30, label: str = '') -> bool:
    """Abort if graceful_stop has been set and the optional grace period has not expired.

    Args:
        args: Pilot arguments object with ``graceful_stop`` event and ``timing`` dict.
        limit: Grace period in seconds after ``REACHED_MAXTIME`` is set before aborting.
            Pass 0 to abort immediately.
        label: Optional prefix appended to log messages (used to identify the caller).

    Returns:
        True if ``graceful_stop`` is set and the grace period has expired (or was not
        applicable), False otherwise.
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
    """Check if the pilot was killed by a KILL signal.

    Args:
        timing: The ``args.timing`` dictionary mapping event labels to timing data.

    Returns:
        True if a ``PILOT_KILL_SIGNAL`` entry is present in any timing record,
        False otherwise.
    """
    return any(PILOT_KILL_SIGNAL in timing[i] for i in timing)


def is_pilot_check(check: str = '') -> bool:
    """Determine whether the given pilot check should be run.

    Consults ``config.Pilot.checks`` to decide if *check* is enabled. When
    the config attribute is absent (outdated config file), returns ``True``
    to allow the check to proceed.

    Args:
        check: Name of the pilot check to look up.

    Returns:
        True if the check is listed in ``config.Pilot.checks``, or if the
        config attribute is missing; False if *check* is empty or not listed.
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
