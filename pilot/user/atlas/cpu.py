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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-24

""" CPU related functionality."""

import logging
import os

# from .utilities import get_memory_values
#from pilot.util.container import execute
from pilot.info.jobdata import JobData
from pilot.util.math import float_to_rounded_string
from .utilities import get_memory_values

logger = logging.getLogger(__name__)


def get_core_count(job: JobData) -> int:
    """
    Return the core count from ATHENA_PROC_NUMBER.

    :param job: job object (JobData)
    :return: core count (int).
    """
    if "HPC_HPC" in job.infosys.queuedata.catchall:
        if job.corecount is None:
            job.corecount = 0
    elif job.corecount:
        # Always use the ATHENA_PROC_NUMBER first, if set
        if 'ATHENA_PROC_NUMBER' in os.environ:
            try:
                job.corecount = int(os.environ.get('ATHENA_PROC_NUMBER'))
            except (ValueError, TypeError) as exc:
                logger.warning(f"ATHENA_PROC_NUMBER is not properly set: {exc} "
                               f"(will use existing job.corecount value)")
    else:
        try:
            job.corecount = int(os.environ.get('ATHENA_PROC_NUMBER'))
        except (ValueError, TypeError):
            logger.warning("environment variable ATHENA_PROC_NUMBER is not set. corecount is not set")

    return job.corecount


def add_core_count(corecount: int, core_counts: list = None) -> list:
    """
    Add a core count measurement to the list of core counts.

    :param corecount: current actual core count (int)
    :param core_counts: list of core counts (list)
    :return: updated list of core counts (list).
    """
    if core_counts is None:
        core_counts = []
    core_counts.append(corecount)

    return core_counts


def set_core_counts(**kwargs: dict):
    """
    Set the number of used cores.

    :param kwargs: kwargs (dict).
    """
    # something like this could be used if prmon also gave info about ncores
    # (change nprocs -> ncores and add ncores to list in utilities module, get_average_summary_dictionary_prmon())

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
                else:
                    logger.debug('no stime/utime')
            else:
                logger.debug('no time dictionary')
        else:
            logger.debug('no summary dictionary')
    else:
        logger.debug(f'failed to calculate number of cores (walltime={walltime})')
