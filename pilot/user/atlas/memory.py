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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-25

import ast
import logging
import math

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.info.jobdata import JobData
from pilot.util.auxiliary import set_pilot_state
from pilot.util.cgroups import set_memory_limit
from pilot.util.config import config
from pilot.util.processes import kill_processes

from .utilities import get_memory_values

logger = logging.getLogger(__name__)
errors = ErrorCodes()
pilot_cache = get_pilot_cache()


def allow_memory_usage_verifications() -> bool:
    """
    Return True if memory usage verifications should be performed.

    :return: True for ATLAS jobs (bool).
    """
    return True


def get_ucore_scale_factor(job: JobData) -> int:
    """
    Get the correction/scale factor for SCORE/4CORE/nCORE jobs on UCORE queues.

    :param job: job object (JobData)
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


def get_memory_limit_old(resource_type: str) -> int:
    """
    Get the memory limit for the relevant resource type.

    :param resource_type: resource type (str)
    :return: memory limit in MB (int).
    """
    try:
        memory_limits = ast.literal_eval(config.Payload.memory_limits)
    except AttributeError as e:
        logger.warning(f"memory limits not set in config, using defaults: {e}")
        memory_limits = {'MCORE': 1001,
                         'MCORE_HIMEM': 2001,
                         'MCORE_LOMEM': None,
                         'SCORE': 1001,
                         'SCORE_HIMEM': 2001,
                         'SCORE_LOMEM': None}
    try:
        memory_limit = memory_limits.get(resource_type, None)
    except AttributeError as e:
        logger.warning(f"memory limit not set for resource type {resource_type}: {e}")
        memory_limit = None
    if not memory_limit:
        logger.warning(f"memory limit not set for resource type {resource_type} - using default 4001")
        memory_limit = 4001

    return memory_limit


def get_memory_limit(resource_type: str) -> int:
    """
    Get the memory limit for the relevant resource type.

    :param resource_type: resource type (str)
    :return: memory limit in MB (int), 0 if not set.
    """
    try:
        memory_limits = pilot_cache.resource_types
        logger.debug(f"memory limits from pilot_cache: {memory_limits}")
        limit_dict = memory_limits.get(resource_type)
    except AttributeError as e:
        logger.warning(f"memory limits not set in config, using defaults: {e}")
        limits = {'MCORE': 2000,
                  'MCORE_HIMEM': 3000,
                  'MCORE_LOMEM': 1000,
                  'MCORE_VHIMEM': 0,
                  'SCORE': 2000,
                  'SCORE_HIMEM': 3000,
                  'SCORE_LOMEM': 1000,
                  'SCORE_VHIMEM': 0}
        return limits.get(resource_type, 4001)
    logger.debug(f"memory limits for resource type {resource_type}: {limit_dict}")
    if not limit_dict:
        logger.warning(f"memory limit not set for resource type {resource_type} - using default 4001")
        return 4001
    limit = limit_dict.get("maxrampercore")
    logger.debug(f"maxrampercore for resource type {resource_type}: {limit}")
    if not limit:
        logger.warning(f"maxrampercore not set for resource type {resource_type} - using default 4001")
        return 4001

    return limit


def calculate_memory_limit_kb(job: JobData, resource_type: str, memory_limit_panda: int) -> int or None:
    """
    Calculate the memory kill threshold in kB based on resource type.

    Args:
        job (JobData): job object containing job information.
        resource_type (str): subresource type string (e.g. SCORE_HIMEM, MCORE_LOMEM).
        memory_limit_panda (int): memory limit from PanDA in MB.

    Returns:
        int or None: memory limit in kB, or None if it cannot be determined.
    """
    pilot_rss_grace = float(job.infosys.queuedata.pilot_rss_grace or 2.0)
    score_resource_types = {"SCORE", "SCORE_LOMEM", "SCORE_HIMEM", "SCORE_VHIMEM"}
    mcore_resource_types = {"MCORE", "MCORE_LOMEM", "MCORE_HIMEM", "MCORE_VHIMEM"}

    try:
        maxrss = int(job.infosys.queuedata.maxrss)
        pq_corecount = int(job.infosys.queuedata.corecount or 1)
        job_corecount = int(job.corecount or 1)

        if "VHIMEM" not in resource_type:
            scaled_maxrss = memory_limit_panda * job_corecount
            logger.debug(f"logic: scaled_maxrss = {memory_limit_panda} * {job_corecount} = {scaled_maxrss}")
        elif resource_type in score_resource_types:
            scaled_maxrss = (maxrss / pq_corecount) * job_corecount
            logger.debug(f"SCORE VHIMEM logic: scaled_maxrss = ({maxrss} / {pq_corecount}) * {job_corecount} = {scaled_maxrss}")
        elif resource_type in mcore_resource_types:
            scaled_maxrss = maxrss
            logger.debug(f"MCORE logic: full maxrss = {scaled_maxrss}")
        else:
            scaled_maxrss = None  # trigger fallback
    except (ValueError, TypeError) as exc:
        logger.warning(f"error determining maxrss or corecount: {exc}")
        scaled_maxrss = None

    if scaled_maxrss:
        memory_limit_kb = pilot_rss_grace * scaled_maxrss * 1024
        logger.debug(f"memory limit using maxrss-based calculation: pilot_rss_grace * scaled_maxrss * 1024 = "
                     f"{pilot_rss_grace} * {scaled_maxrss} * 1024 = {memory_limit_kb} kB")
        return int(memory_limit_kb)

    # fallback to job.minramcount
    if hasattr(job, "minramcount") and job.minramcount:
        is_push_queue = pilot_cache.harvester_submitmode == "push"
        minram = job.minramcount
        if not is_push_queue:
            minram = int(math.ceil(minram / 1000.0)) * 1000  # Round up for pull PQs
        memory_limit_kb = pilot_rss_grace * minram * 1024
        logger.debug(f"fallback using job.minramcount ({minram} MB): {memory_limit_kb} kB")
        logger.debug(f"(pilot_rss_grace * minramcount * 1024 = "
                     f"{pilot_rss_grace} * {minram} * 1024) = {memory_limit_kb} kB)")
        logger.debug(f"(where minramcount = int(math.ceil({job.minramcount} / 1000.0)) * 1000)")

        return int(memory_limit_kb)

    logger.warning("no valid memory limit source found (maxrss or minramcount)")
    return None


def set_cgroups_limit(memory_limit_kb: int):
    """
    Set the cgroups memory limit for a given process ID.

    Args:
        memory_limit_kb (int): Memory limit in kB.
    """
    # cgroup_path = pilot_cache.get_cgroup("payload")
    cgroup_path = pilot_cache.get_cgroup("subprocesses")  # use subprocesses cgroup which should include the payload
    if not cgroup_path:
        logger.warning("no cgroup found for subprocesses cgroup - cannot set memory limit")
        return

    if pilot_cache.set_memory_limits and cgroup_path in pilot_cache.set_memory_limits:
        logger.debug(f"memory limit already set for cgroup {cgroup_path}")
        return

    try:
        set_memory_limit(cgroup_path, memory_limit_kb * 1024)  # convert to bytes
    except (ValueError, FileNotFoundError, PermissionError, OSError) as exc:
        logger.warning(f"could not set cgroup memory limit: {exc}")
    else:
        pilot_cache.set_memory_limits.append(cgroup_path)
        logger.info(f"memory limit set for cgroup {cgroup_path}: {memory_limit_kb} kB")


def memory_usage(job: object, resource_type: str) -> tuple[int, str]:
    """
    Perform memory usage verification.

    Args:
        job (JobData): job object containing job information.
        resource_type (str): subresource type string (e.g. SCORE_HIMEM)

    Returns:
        tuple: exit code (int), diagnostics (str).
    """
    exit_code = 0
    diagnostics = ""

    summary_dictionary = get_memory_values(job.workdir, name=job.memorymonitor)
    if not summary_dictionary:
        exit_code = errors.BADMEMORYMONITORJSON
        diagnostics = "memory monitor output could not be read"
        return exit_code, diagnostics

    # get the memory limit from PanDA
    memory_limit_panda = get_memory_limit(resource_type)
    if memory_limit_panda:
        logger.debug(f'memory_limit for {resource_type}: {memory_limit_panda} MB')
    else:
        logger.debug(f'memory_limit for {resource_type}: {memory_limit_panda}' '(not set)')

    maxdict = summary_dictionary.get("Max", {})
    maxpss_int = maxdict.get("maxPSS", -1)
    memory_limit_kb = calculate_memory_limit_kb(job, resource_type, memory_limit_panda)

    # set the cgroups memory limit for the payload process if applicable and in case it is not set already
    if pilot_cache.use_cgroups and memory_limit_kb:
        set_cgroups_limit(memory_limit_kb)

    if maxpss_int != -1 and memory_limit_kb:
        if maxpss_int > memory_limit_kb:
            diagnostics = (
                f"job has exceeded the memory limit {maxpss_int} kB > {memory_limit_kb} kB "
                f"(subresource={resource_type}, corecount={job.corecount})"
            )
            logger.warning(diagnostics)
            set_pilot_state(job=job, state="failed")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXCEEDMAXMEM)

            kill_processes(job.pid)  # explicitly kill also when using cgroups?
        else:
            logger.info(
                f"max memory (maxPSS) used by the payload is within the allowed limit: "
                f"{maxpss_int} kB <= {memory_limit_kb} kB (subresource={resource_type})"
            )
    elif memory_limit_kb is None:
        logger.warning("could not determine memory limit - memory check skipped")
    elif maxpss_int == -1:
        logger.warning("maxPSS not found in memory monitor output")

    return exit_code, diagnostics


def memory_usage_old(job: object, resource_type: str) -> tuple[int, str]:
    """
    Perform memory usage verification.

    :param job: job object (JobData)
    :param resource_type: resource type (str)
    :return: exit code (int), diagnostics (str) (tuple).
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
        pilot_rss_grace = job.infosys.queuedata.pilot_rss_grace
        memkillgrace = get_memkillgrace(job.infosys.queuedata.memkillgrace)
        logger.debug(f'memkillgrace: {memkillgrace} pilot_rss_grace: {pilot_rss_grace}')
        if maxrss:
            # correction for SCORE/4CORE/nCORE jobs on UCORE queues
            scale = get_ucore_scale_factor(job)
            try:
                maxrss_int = pilot_rss_grace * int(maxrss * scale) * 1024  # Convert to int and kB
            except (ValueError, TypeError) as exc:
                logger.warning(f"unexpected value for maxRSS: {exc}")
            else:
                # Compare the maxRSS with the maxPSS from memory monitor
                if maxrss_int > 0 and maxpss_int > 0:
                    if maxpss_int > maxrss_int:
                        diagnostics = f"job has exceeded the memory limit {maxpss_int} kB > {maxrss_int} kB " \
                                      f"({pilot_rss_grace}(queuedata.pilot_rss_grace) * {maxrss} (queuedata.maxrss) * {scale} (scale))"
                        logger.warning(diagnostics)

                        # Create a lockfile to let RunJob know that it should not restart the memory monitor after it has been killed
                        #pUtil.createLockFile(False, self.__env['jobDic'][k][1].workdir, lockfile="MEMORYEXCEEDED")

                        # Kill the job
                        set_pilot_state(job=job, state="failed")
                        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXCEEDMAXMEM)
                        kill_processes(job.pid)
                    else:
                        logger.info(f"max memory (maxPSS) used by the payload is within the allowed limit: "
                                    f"{maxpss_int} B ({pilot_rss_grace} (queuedata.pilot_rss_grace) * {maxrss} "
                                    f"(queuedata.maxrss) * {scale} (scale) = {maxrss_int} B, memkillgrace = {job.infosys.queuedata.memkillgrace}%)")
        elif maxrss in {0, "0"}:
            logger.info("queuedata.maxrss set to 0 (no memory checks will be done)")
        else:
            logger.warning("queuedata.maxrss is not set")

    return exit_code, diagnostics
