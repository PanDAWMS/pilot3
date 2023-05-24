#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2021

import os
import logging

from pilot.util.config import config
from pilot.util.constants import PILOT_KILL_SIGNAL
from pilot.util.timing import get_time_since

logger = logging.getLogger(__name__)


def should_abort(args, limit=30, label=''):
    """
    Abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set).

    :param args: pilot arguments object.
    :param limit: optional time limit (int).
    :param label: optional label prepending log messages (string).
    :return: True if graceful_stop has been set (and less than optional time limit has passed since maxtime) or False
    """

    abort = False
    if args.graceful_stop.wait(1) or args.graceful_stop.is_set():  # 'or' added for 2.6 compatibility reasons
        if os.environ.get('REACHED_MAXTIME', None) and limit:
            # was the pilot killed?
            was_killed = was_pilot_killed(args.timing)
            time_since = get_time_since('0', PILOT_KILL_SIGNAL, args)
            if time_since < limit and was_killed:
                logger.warning('%s:received graceful stop - %d s ago, continue for now', label, time_since)
            else:
                abort = True
        else:
            logger.warning('%s:received graceful stop - abort after this iteration', label)
            abort = True

    return abort


def was_pilot_killed(timing):
    """
    Was the pilot killed by a KILL signal?

    :param timing: args.timing dictionary.
    :return: Boolean, True if pilot was killed by KILL signal.
    """

    was_killed = False
    for i in timing:
        if PILOT_KILL_SIGNAL in timing[i]:
            was_killed = True
    return was_killed


def is_pilot_check(check=''):
    """
    Should the given pilot check be run?

    Consult config.Pilot.checks if the given check is listed.

    :param check: name of check (string)
    :return: True if check is present in config.Pilot.checks (and if config is outdated), False othersise (Boolean)
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
