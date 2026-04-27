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

"""Queue handling utilities for pilot job and data queues."""

from __future__ import annotations
import logging
import os
import signal
import time
from collections import namedtuple
from queue import Queue
from typing import Optional

from pilot.common.errorcodes import ErrorCodes
from pilot.info import JobData
from pilot.util.auxiliary import (
    set_pilot_state,
    is_string
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def get_signal_name(sig_num: int) -> str:
    """Return the signal name for the given signal number.

    Args:
        sig_num: Signal number.

    Returns:
        Signal name string, or None if the signal number is not valid.
    """
    try:
        # Convert signal number to its enumeration equivalent and then to string
        return signal.Signals(sig_num).name
    except ValueError:
        # If the signal number is not a valid signal, return None or handle as needed
        return None


def declare_failed_by_kill(job: object, queue: Queue, signal_name: str) -> None:
    """Declare the job failed by a kill signal and put it in a suitable failed queue.

    E.g. ``queue=queues.failed_data_in`` if the kill signal was received during stage-in.

    Args:
        job: Job object.
        queue: Queue object to place the failed job into.
        signal_name: Detected kill signal name (e.g. ``'SIGTERM'``).
    """
    set_pilot_state(job=job, state="failed")
    error_code = errors.get_kill_signal_error_code(signal_name)
    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code)

    #queue.put(job)
    put_in_queue(job, queue)


def scan_for_jobs(queues: namedtuple) -> list:
    """Scan queues until at least one queue has a job object, aborting after 30 seconds.

    Args:
        queues: Named tuple of queue objects.

    Returns:
        List of job objects found, or None if none were found in time.
    """
    _t0 = time.time()
    found_job = False
    jobs = None

    while time.time() - _t0 < 30:
        for queue in queues._fields:
            # ignore queues with no job objects
            if queue in {'completed_jobids', 'messages'}:
                continue
            _queue = getattr(queues, queue)
            jobs = list(_queue.queue)
            if len(jobs) > 0:
                logger.debug(f'found {len(jobs)} job(s) in queue {queue} after {time.time() - _t0} s - will begin queue monitoring')
                found_job = True
                break
        if found_job:
            break
        time.sleep(0.1)

    return jobs


def get_timeinfo_from_job(queues: namedtuple, params: dict) -> tuple[Optional[int], Optional[int]]:
    """Return the maxwalltime and starttime from the job object.

    Requires the ``PANDAID`` environment variable to be set in order to find
    the correct walltime.

    Args:
        queues: Named tuple of queue objects.
        params: ``queuedata.params`` dictionary.

    Returns:
        Tuple of ``(maxwalltime, starttime)``, each an int or None.
    """
    maxwalltime = None
    starttime = None
    use_job_maxwalltime = False
    current_job_id = os.environ.get('PANDAID', None)
    if not current_job_id:
        return None, None

    # on push queues, one can set params.use_job_maxwalltime to decide if job.maxwalltime should be used to check
    # job running time
    if params:
        use_job_maxwalltime = params.get('job_maxwalltime', False)
        logger.debug(f'use_job_maxwalltime={use_job_maxwalltime} (type={type(use_job_maxwalltime)}, current job id={current_job_id})')

    # extract jobs from the queues
    jobs = scan_for_jobs(queues)
    if jobs:
        for job in jobs:
            if current_job_id == job.jobid:
                maxwalltime = job.maxwalltime if job.maxwalltime and use_job_maxwalltime else None
                # make sure maxwalltime is an int (might be 'NULL')
                if not isinstance(maxwalltime, int):
                    maxwalltime = None
                starttime = job.starttime
                if not isinstance(starttime, int):
                    starttime = None
                break

    return maxwalltime, starttime


def get_queuedata_from_job(queues: namedtuple) -> Optional[object]:
    """Return the queuedata object from a job in the given queues.

    Useful when queuedata is needed from a context that does not have direct
    access to the job object (e.g. the pilot monitor).

    Args:
        queues: Named tuple of queue objects.

    Returns:
        The queuedata object extracted from the first available job, or None.
    """
    queuedata = None

    # extract jobs from the queues
    jobs = scan_for_jobs(queues)
    if jobs:
        for job in jobs:
            queuedata = job.infosys.queuedata
            break

    return queuedata


def abort_jobs_in_queues(queues: namedtuple, sig: str) -> None:
    """Find all jobs in the queues and abort them.

    Args:
        queues: Named tuple of queue objects.
        sig: Detected kill signal name (e.g. ``'SIGTERM'``).
    """
    jobs_list = []

    # loop over all queues and find all jobs
    for queue in queues._fields:
        _queue = getattr(queues, queue)
        jobs = list(_queue.queue)
        for job in jobs:
            # completed_jobids can contain strings or ints, and other non-job sentinels might appear
            if is_string(job) or not hasattr(job, 'jobid'):
                continue
            if job not in jobs_list:
                jobs_list.append(job)

    logger.info(f'found {len(jobs_list)} job(s) in {len(queues._fields)} queues')
    for job in jobs_list:
        logger.info(f'aborting job {job.jobid}')
        declare_failed_by_kill(job, queues.failed_jobs, sig)


def queue_report(queues: namedtuple, purge: bool = False) -> None:
    """Report on how many jobs are in the various queues.

    Can also empty the queues (except ``completed_jobids``).

    Args:
        queues: Named tuple of queue objects.
        purge: If True, clear all queues (except ``completed_jobids``).
    """
    exceptions_list = ['completed_jobids']
    for queue in queues._fields:
        _queue = getattr(queues, queue)
        jobs = list(_queue.queue)
        if queue not in exceptions_list:
            tag = '[purged]' if purge else ''
            logger.info(f'queue {queue} had {len(jobs)} job(s) {tag}')
            with _queue.mutex:
                _queue.queue.clear()
        else:
            logger.info(f'queue {queue} has {len(jobs)} job(s)')


def put_in_queue(obj: object, queue: Queue) -> None:
    """Put the given object in the given queue, skipping duplicates.

    Args:
        obj: Object to put in the queue.
        queue: Queue object to receive the object.
    """
    # update job object size (currently not used)
    if isinstance(obj, JobData):
        obj.add_size(obj.get_size())

    # only put the object in the queue if it is not there already
    if obj not in list(queue.queue):
        queue.put(obj)


def purge_queue(queue: Queue) -> None:
    """Empty the given queue.

    Args:
        queue: Queue object to purge.
    """
    while not queue.empty():
        try:
            queue.get(False)
        except queue.Empty:
            continue
        queue.task_done()

    logger.debug('queue purged')
