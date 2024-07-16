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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-24

import logging

from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import set_pilot_state
from pilot.util.config import config
from pilot.util.processes import kill_processes
from .utilities import get_memory_values

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def allow_memory_usage_verifications() -> bool:
    """
    Return True if memory usage verifications should be performed.

    :return: True for ATLAS jobs (bool).
    """
    return True


def get_ucore_scale_factor(job):
    """
    Get the correction/scale factor for SCORE/4CORE/nCORE jobs on UCORE queues/

    :param job: job object.
    :return: scale factor (int).
    """

    try:
        job_corecount = float(job.corecount)
    except (ValueError, TypeError) as exc:
        logger.warning(f'exception caught: {exc} (job.corecount={job.corecount})')
        job_corecount = None

    try:
        schedconfig_corecount = float(job.infosys.queuedata.corecount)
    except (ValueError, TypeError) as exc:
        logger.warning(f'exception caught: {exc} (job.infosys.queuedata.corecount={job.infosys.queuedata.corecount})')
        schedconfig_corecount = None

    if job_corecount and schedconfig_corecount:
        try:
            scale = job_corecount / schedconfig_corecount
            logger.debug(f'scale: job_corecount / schedconfig_corecount={scale}')
        except (ZeroDivisionError, TypeError) as exc:
            logger.warning(f'exception caught: {exc} (using scale factor 1)')
            scale = 1
    else:
        logger.debug('will use scale factor 1')
        scale = 1

    return scale


def get_memkillgrace(memkillgrace: int) -> float:
    """
    Return a proper memkillgrace value.

    Convert from percentage to integer if necessary.

    :param memkillgrace: memkillgrace value (int)
    :return: memkillgrace value (float).
    """
    return memkillgrace / 100 if memkillgrace > 1 else 1.0


def get_memory_limit(resource_type: str) -> int:
    """
    Get the memory limit for the relevant resource type.

    :param resource_type: resource type (str)
    :return: memory limit in MB (int).
    """
    try:
        memory_limits = config.Payload.memory_limits
    except AttributeError as e:
        logger.warning(f"memory_limits not set in config, using defaults: {e}")
        memory_limits = {'MCORE': 1001,
                         'MCORE_HIMEM': 2001,
                         'MCORE_LOMEM': None,
                         'SCORE': 1001,
                         'SCORE_HIMEM': 2001,
                         'SCORE_LOMEM': None}
    memory_limit = memory_limits.get(resource_type, None)
    if not memory_limit:
        logger.warning(f"memory limit not set for resource type {resource_type} - using default 4001")
        memory_limit = 4001

    return memory_limit


def memory_usage(job: object, resource_type: str) -> (int, str):
    """
    Perform memory usage verification.

    :param job: job object (object)
    :param resource_type: resource type (str)
    :return: exit code (int), diagnostics (str).
    """
    exit_code = 0
    diagnostics = ""

    # Get the maxPSS value from the memory monitor
    summary_dictionary = get_memory_values(job.workdir, name=job.memorymonitor)

    if not summary_dictionary:
        exit_code = errors.BADMEMORYMONITORJSON
        diagnostics = "Memory monitor output could not be read"
        return exit_code, diagnostics

    maxdict = summary_dictionary.get('Max', {})
    maxpss_int = maxdict.get('maxPSS', -1)

    memory_limit = get_memory_limit(resource_type)
    logger.debug(f'memory_limit for {resource_type}: {memory_limit} MB')

    # Only proceed if values are set
    if maxpss_int != -1:
        maxrss = job.infosys.queuedata.maxrss
        memkillgrace = get_memkillgrace(job.infosys.queuedata.memkillgrace)
        logger.debug(f'memkillgrace: {memkillgrace}')
        if maxrss:
            # correction for SCORE/4CORE/nCORE jobs on UCORE queues
            scale = get_ucore_scale_factor(job)
            try:
                maxrss_int = 2 * int(maxrss * scale) * 1024  # Convert to int and kB
            except (ValueError, TypeError) as exc:
                logger.warning(f"unexpected value for maxRSS: {exc}")
            else:
                # Compare the maxRSS with the maxPSS from memory monitor
                if maxrss_int > 0 and maxpss_int > 0:
                    if maxpss_int > maxrss_int:
                        diagnostics = f"job has exceeded the memory limit {maxpss_int} kB > {maxrss_int} kB " \
                                      f"(2 * queuedata.maxrss)"
                        logger.warning(diagnostics)

                        # Create a lockfile to let RunJob know that it should not restart the memory monitor after it has been killed
                        #pUtil.createLockFile(False, self.__env['jobDic'][k][1].workdir, lockfile="MEMORYEXCEEDED")

                        # Kill the job
                        set_pilot_state(job=job, state="failed")
                        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXCEEDMAXMEM)
                        kill_processes(job.pid)
                    else:
                        logger.info(f"max memory (maxPSS) used by the payload is within the allowed limit: "
                                    f"{maxpss_int} B (2 * maxRSS = {maxrss_int} B, memkillgrace = {job.infosys.queuedata.memkillgrace}%)")
        else:
            if maxrss == 0 or maxrss == "0":
                logger.info("queuedata.maxrss set to 0 (no memory checks will be done)")
            else:
                logger.warning("queuedata.maxrss is not set")

    return exit_code, diagnostics
