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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-26

"""Timing module for the pilot."""

# Note: The Pilot modules that need to record timing measurements, can do so using the add_to_pilot_timing() function.
# When the timing measurements need to be recorded, the high-level functions, e.g. get_getjob_time(), can be used.

# Structure of pilot timing dictionary:
#     { job_id: { <timing_constant_1>: <time measurement in seconds since epoch>, .. }
# job_id = 0 means timing information from wrapper. Timing constants are defined in pilot.util.constants.
# Time measurement are time.time() values. The float value will be converted to an int as a last step.

from __future__ import annotations
import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

from pilot.util.config import config
from pilot.util.constants import (
    PILOT_END_TIME,
    PILOT_MULTIJOB_START_TIME,
    PILOT_POST_FINAL_UPDATE,
    PILOT_POST_GETJOB,
    PILOT_POST_LOG_TAR,
    PILOT_POST_PAYLOAD,
    PILOT_POST_SETUP,
    PILOT_POST_STAGEIN,
    PILOT_POST_STAGEOUT,
    PILOT_PRE_GETJOB,
    PILOT_PRE_LOG_TAR,
    PILOT_PRE_PAYLOAD,
    PILOT_PRE_SETUP,
    PILOT_PRE_STAGEIN,
    PILOT_PRE_STAGEOUT,
    PILOT_PRE_FINAL_UPDATE,
    PILOT_START_TIME,
    PILOT_PRE_REMOTEIO,
    PILOT_POST_REMOTEIO
)
from pilot.util.filehandling import (
    read_json,
    write_json
)

logger = logging.getLogger(__name__)


def read_pilot_timing() -> dict:
    """Read the pilot timing dictionary from file.

    Returns:
        Pilot timing dictionary.
    """
    pilot_timing_dictionary = {}

    path = os.path.join(os.environ.get('PILOT_HOME', ''), config.Pilot.timing_file)
    if os.path.exists(path):
        pilot_timing_dictionary = read_json(path)

    return pilot_timing_dictionary


def write_pilot_timing(pilot_timing_dictionary: dict) -> None:
    """Write the given pilot timing dictionary to file.

    Args:
        pilot_timing_dictionary: Pilot timing dictionary to persist.
    """
    timing_file = config.Pilot.timing_file
    #rank, max_ranks = get_ranks_info()
    #if rank is not None:
    #    timing_file += '_{0}'.format(rank)
    path = os.path.join(os.environ.get('PILOT_HOME', ''), timing_file)
    if write_json(path, pilot_timing_dictionary):
        logger.debug(f'updated pilot timing dictionary: {path}')
    else:
        logger.warning(f'failed to update pilot timing dictionary: {path}')


def add_to_pilot_timing(job_id: int, timing_constant: str, time_measurement: float, args: object, store: bool = False) -> None:
    """Add the given timing constant and measurement for job_id to the pilot timing dictionary.

    Args:
        job_id: PanDA job id.
        timing_constant: Timing constant.
        time_measurement: Time measurement.
        args: Pilot arguments.
        store: If True, write the timing dictionary to file immediately.
    """
    if args.timing == {}:
        args.timing[job_id] = {timing_constant: time_measurement}
    else:
        if job_id not in args.timing:
            args.timing[job_id] = {}
        args.timing[job_id][timing_constant] = time_measurement

    # update the file
    if store:
        write_pilot_timing(args.timing)


def get_initial_setup_time(job_id: int, args: object) -> int:
    """Return the time for the initial setup.

    The initial setup time is measured from ``PILOT_START_TIME`` to
    ``PILOT_PRE_GETJOB``.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_MULTIJOB_START_TIME, PILOT_PRE_GETJOB, args)


def get_getjob_time(job_id: int, args: object) -> int:
    """Return the time for the getjob operation.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_GETJOB, PILOT_POST_GETJOB, args)


def get_setup_time(job_id: int, args: object) -> int:
    """Return the time for the setup operation.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_SETUP, PILOT_POST_SETUP, args)


def get_stagein_time(job_id: int, args: object) -> int:
    """Return the time for the stage-in operation.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_STAGEIN, PILOT_POST_STAGEIN, args)


def get_stageout_time(job_id: int, args: object) -> int:
    """Return the time for the stage-out operation.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_STAGEOUT, PILOT_POST_STAGEOUT, args)


def get_log_creation_time(job_id: int, args: object) -> int:
    """Return the time for creating the job log.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_LOG_TAR, PILOT_POST_LOG_TAR, args)


def get_payload_execution_time(job_id: int, args: object) -> int:
    """Return the time for the payload execution.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_PAYLOAD, PILOT_POST_PAYLOAD, args)


def get_final_update_time(job_id: int, args: object) -> int:
    """Return the time for the final update.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_FINAL_UPDATE, PILOT_POST_FINAL_UPDATE, args)


def get_total_pilot_time(job_id: int, args: object) -> int:
    """Return the total pilot time for the given job_id.

    Returns the wall time elapsed from the start of the pilot until after
    the last job update.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_START_TIME, PILOT_END_TIME, args)


def get_total_remoteio_time(job_id: int, args: object) -> int:
    """Return the total time to verify remote i/o files for the given job_id.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_difference(job_id, PILOT_PRE_REMOTEIO, PILOT_POST_REMOTEIO, args)


def get_postgetjob_time(job_id: int, args: object) -> Optional[int]:
    """Return the post getjob time.

    Args:
        job_id: PanDA job id.
        args: Pilot arguments.

    Returns:
        Post getjob time measurement in seconds, or None on failure.
    """
    time_measurement = None
    timing_constant = PILOT_POST_GETJOB

    if job_id in args.timing:
        # extract time measurements
        time_measurement_dictionary = args.timing.get(job_id, None)
        if time_measurement_dictionary:
            time_measurement = time_measurement_dictionary.get(timing_constant, None)

        if not time_measurement:
            logger.warning(f'failed to extract time measurement {timing_constant} from {time_measurement_dictionary} (no such key)')

    return time_measurement


def get_time_measurement(timing_constant: str, time_measurement_dictionary: dict, timing_dictionary: dict) -> Optional[float]:
    """Return a requested time measurement from the time measurement dictionary.

    The dictionary is read from the pilot timing file.

    Args:
        timing_constant: Timing constant (e.g. ``PILOT_MULTIJOB_START_TIME``).
        time_measurement_dictionary: Time measurement dictionary extracted from
            the pilot timing dictionary.
        timing_dictionary: Full timing dictionary from the pilot timing file.

    Returns:
        Time measurement in seconds, or None if not found.
    """
    time_measurement = time_measurement_dictionary.get(timing_constant, None)
    if not time_measurement:
        # try to get the measurement for the PILOT_MULTIJOB_START_TIME dictionary
        i = '0' if timing_constant == PILOT_START_TIME else '1'
        time_measurement_dictionary_0 = timing_dictionary.get(i, None)
        if time_measurement_dictionary_0:
            time_measurement = time_measurement_dictionary_0.get(timing_constant, None)
        else:
            logger.warning(f'failed to extract time measurement {timing_constant} from {time_measurement_dictionary} (no such key)')

    return time_measurement


def get_time_since_start(args: object) -> int:
    """Return the amount of time that has passed since the pilot was launched.

    Args:
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_since('0', PILOT_START_TIME, args)


def get_time_since_start_or_none(args: object) -> Optional[int]:
    """Return the time passed since the pilot was launched, or None if it cannot be determined.

    get_time_since_start() returns 0 both when the pilot has only just started and when the
    PILOT_START_TIME measurement is missing altogether. Callers that would draw a different
    conclusion from those two cases -- notably anything subtracting the result from a ceiling
    such as PQ.maxtime, where a missing measurement would silently yield the full ceiling --
    should use this function instead.

    Args:
        args: Pilot arguments.

    Returns:
        Time in seconds since the pilot was launched, or None if no PILOT_START_TIME
            measurement is available.
    """
    time_measurement_dictionary = args.timing.get('0', None) if getattr(args, 'timing', None) else None
    if not time_measurement_dictionary:
        logger.warning('failed to extract time measurement dictionary for job id 0')
        return None

    time_measurement = get_time_measurement(PILOT_START_TIME, time_measurement_dictionary, args.timing)
    if not time_measurement:
        return None

    return int(time.time() - time_measurement)


def get_time_since_multijob_start(args: object) -> int:
    """Return the amount of time that has passed since the last multi job was launched.

    Args:
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    return get_time_since('1', PILOT_MULTIJOB_START_TIME, args)


def get_time_since(job_id: int, timing_constant: str, args: object) -> int:
    """Return the amount of time that has passed since the time measurement of timing_constant.

    Args:
        job_id: PanDA job id.
        timing_constant: Timing constant.
        args: Pilot arguments.

    Returns:
        Time in seconds.
    """
    diff = 0

    if job_id in args.timing:

        # extract time measurements
        time_measurement_dictionary = args.timing.get(job_id, None)
        if time_measurement_dictionary:
            time_measurement = get_time_measurement(timing_constant, time_measurement_dictionary, args.timing)
            if time_measurement:
                diff = int(time.time() - time_measurement)
        else:
            logger.warning(f'failed to extract time measurement dictionary from {args.timing}')
    else:
        logger.warning(f'job id {job_id} not found in timing dictionary')

    return diff


def get_time_difference(job_id: int, timing_constant_1: str, timing_constant_2: str, args: object) -> int:
    """Return the positive time difference between the given constants.

    The order is not important — a positive difference is always returned.
    Collects the time measurements corresponding to the given timing constants
    from the pilot timing file. The job_id is used internally as a dictionary
    key.

    Timing dictionary structure::

        { job_id: { <timing_constant_1>: <time measurement in seconds since epoch>, .. }

    ``job_id = 0`` means timing information from the wrapper. Timing constants
    are defined in ``pilot.util.constants``. Time measurements are
    ``time.time()`` values converted to int as a last step.

    Args:
        job_id: PanDA job id.
        timing_constant_1: First timing constant.
        timing_constant_2: Second timing constant.
        args: Pilot arguments.

    Returns:
        Time difference in seconds.
    """
    diff = 0

    if job_id in args.timing:

        # extract time measurements
        time_measurement_dictionary = args.timing.get(job_id, None)
        if time_measurement_dictionary:

            time_measurement_1 = get_time_measurement(timing_constant_1, time_measurement_dictionary, args.timing)
            time_measurement_2 = get_time_measurement(timing_constant_2, time_measurement_dictionary, args.timing)

            if time_measurement_1 and time_measurement_2:
                diff = time_measurement_2 - time_measurement_1
        else:
            logger.warning(f'failed to extract time measurement dictionary from {args.timing}')
    else:
        logger.warning(f'job id {job_id} not found in timing dictionary')

    # always return a positive number
    if diff < 0:
        diff = -diff

    # convert to int as a last step
    try:
        diff = int(diff)
    except Exception as exc:
        logger.warning(f'failed to convert {diff} to int: {exc} (will reset to 0)')
        diff = 0

    return diff


def timing_report(job_id: int, args: object) -> tuple[int, int, int, int, int, int, int]:
    """Write a timing report to the job log and return relevant timing measurements.

    Args:
        job_id: Job id.
        args: Pilot arguments.

    Returns:
        Tuple of (getjob, stagein, payload, stageout, initial_setup, setup,
        log_creation) times in seconds.
    """
    # collect pilot timing data
    time_getjob = get_getjob_time(job_id, args)
    time_initial_setup = get_initial_setup_time(job_id, args)
    time_setup = get_setup_time(job_id, args)
    #time_total_setup = time_initial_setup + time_setup
    time_stagein = get_stagein_time(job_id, args)
    time_payload = get_payload_execution_time(job_id, args)
    time_stageout = get_stageout_time(job_id, args)
    time_log_creation = get_log_creation_time(job_id, args)
    time_remoteio = get_total_remoteio_time(job_id, args)

    # correct the setup and stagein times if remote i/o verification was done
    if time_remoteio > 0:
        logger.info("correcting setup and stagein times since remote i/o verification was done")
        logger.debug(f"original setup time: {time_setup} s")
        logger.debug(f"original stagein time: {time_stagein} s")
        time_setup -= time_remoteio
        time_stagein += time_remoteio
        logger.debug(f"corrected setup time: {time_setup} s")
        logger.debug(f"corrected stagein time: {time_stagein} s")

    logger.info('.' * 30)
    logger.info('. Timing measurements:')
    logger.info(f'. get job = {time_getjob} s')
    logger.info(f'. initial setup = {time_initial_setup} s')
    logger.info(f'. payload setup = {time_setup} s')
    #logger.info(f'. total setup = {time_total_setup} s')
    logger.info(f'. stage-in = {time_stagein} s')
    logger.info(f'. payload execution = {time_payload} s')
    logger.info(f'. stage-out = {time_stageout} s')
    logger.info(f'. log creation = {time_log_creation} s')
    logger.info('.' * 30)

    return time_getjob, time_stagein, time_payload, time_stageout, time_initial_setup, time_setup, time_log_creation


def time_stamp() -> str:
    """Return the current UTC timestamp in ISO-8601 format.

    Returns:
        A UTC timestamp string such as ``2026-03-20T08:15:30Z``.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def time_stamp_old() -> str:
    """Return ISO-8601 compliant date/time format using local time.

    Returns:
        Timestamp string with UTC offset, e.g. ``'2026-03-20T08:15:30+02:00'``.
    """
    tmptz = time.timezone
    sign_str = '+'
    if tmptz > 0:
        sign_str = '-'
    tmptz_hours = int(tmptz / 3600)

    return str("%s%s%02d:%02d" % (time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime()), sign_str, abs(tmptz_hours),
                                  int(tmptz / 60 - tmptz_hours * 60)))


def get_elapsed_real_time(t0: tuple = None) -> int:
    """Return a time stamp corresponding to the elapsed real time.

    Uses ``os.times()`` to obtain the current time stamp. If t0 is provided,
    the returned value is relative to t0. t0 is assumed to be an ``os.times()``
    tuple.

    Args:
        t0: ``os.times()`` tuple for the reference time stamp. If None,
            returns the absolute elapsed real time.

    Returns:
        Elapsed real time in seconds.
    """
    if t0 and isinstance(t0, tuple):
        try:
            _t0 = int(t0[4])
        except (IndexError, ValueError, TypeError) as exc:
            logger.warning(f'unknown timing format for t0: {exc}')
            _t0 = 0
    else:
        _t0 = 0

    return int(os.times()[4]) - _t0
