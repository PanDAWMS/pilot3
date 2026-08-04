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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-26

"""CPU usage monitoring for the sPHENIX experiment plugin."""

from __future__ import annotations
import logging
from typing import Any

from pilot.info.jobdata import JobData
from pilot.util.math import float_to_rounded_string

from .utilities import get_memory_values

logger = logging.getLogger(__name__)


def get_core_count(job: JobData) -> int:
    """Return the core count.

    Args:
        job: job object.

    Returns:
        int: core count.
    """
    if not job:  # to bypass pylint warning
        pass

    return 0


def add_core_count(corecount: int, core_counts: list = None) -> list:
    """Add a core count measurement to the list of core counts.

    Args:
        corecount: current actual core count.
        core_counts: list of core counts.

    Returns:
        list: updated list of core counts.
    """
    if core_counts is None:
        core_counts = []
    core_counts.append(corecount)

    return core_counts


def set_core_counts(**kwargs: Any) -> None:
    """Set the number of used cores.

    Relies on the memory monitor (prmon) to estimate the number of actual cores used by the payload,
    (utime+stime)/walltime, the same technique used by the ATLAS plugin. This is a cumulative,
    time-averaged estimate rather than an instantaneous process snapshot, so it is not skewed by a
    payload that has not yet ramped up to its full parallelism (e.g. right after start-up).

    Note: job.corecount (the number of cores requested for/allocated to the job) is never modified
    here - only job.actualcorecount and job.corecounts are updated. Downstream code (e.g. the work
    directory size check) relies on job.corecount continuing to reflect the requested/allocated value.

    Args:
        **kwargs: keyword arguments including job and walltime.
    """
    job = kwargs.get('job', None)
    walltime = kwargs.get('walltime', None)

    if job and walltime:
        try:
            summary_dictionary = get_memory_values(job.workdir, name=job.memorymonitor)
        except ValueError as exc:
            logger.warning(f'failed to parse memory monitor output: {exc}')
            summary_dictionary = None

        if summary_dictionary:
            time_dictionary = summary_dictionary.get('Time', None)
            if time_dictionary:
                stime = time_dictionary.get('stime', None)
                utime = time_dictionary.get('utime', None)
                if stime and utime:
                    logger.debug(f'stime={stime}')
                    logger.debug(f'utime={utime}')
                    logger.debug(f'walltime={walltime}')
                    cores = float(stime + utime) / float(walltime)
                    logger.debug(f'number of cores={cores}')
                    job.actualcorecount = float_to_rounded_string(cores, precision=2)
                    # note: the numeric value (not the rounded string stored in job.actualcorecount)
                    # is appended, since job.corecounts is averaged downstream by
                    # control/job.py::get_data_structure() to produce mean_core_count
                    job.corecounts = add_core_count(cores, job.corecounts)
                    logger.debug(f'current core counts list: {job.corecounts}')
                else:
                    logger.debug('no stime/utime')
            else:
                logger.debug('no time dictionary')
        else:
            logger.debug('no summary dictionary')
    else:
        logger.debug(f'failed to calculate number of cores (walltime={walltime})')
