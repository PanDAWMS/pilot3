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
import threading
import time
from collections import namedtuple
from queue import Queue
from typing import Any, Optional

from pilot.common.errorcodes import ErrorCodes
from pilot.info import JobData
from pilot.util.auxiliary import (
    set_pilot_state,
    is_string
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()

# Scan duration above which a queue scan is reported even when its outcome has
# not changed, in seconds. A scan that finds a job immediately costs tens of
# microseconds; one that takes measurable time means the queues were empty and
# had to be waited for, which is worth a line every time it happens.
SLOW_SCAN_THRESHOLD = 1.0

# Last reported outcome of scan_for_jobs() and of the maxwalltime decision in
# get_timeinfo_from_job(). Both functions are called from the pilot monitoring
# loop, which runs every couple of seconds for the entire lifetime of the job:
# logging them unconditionally produced one identical pair of lines per
# iteration, thousands of them per job, for a result that changes at most a
# handful of times. Only a change is reported.
_report_state: dict[str, Any] = {"scan": None, "timeinfo": None}

# Guards _report_state. The monitoring thread and the job control thread both
# reach these functions, and without the lock a change can be reported twice or
# (worse) swallowed by an interleaved write from the other thread.
_report_lock = threading.Lock()


def reset_queue_report_state() -> None:
    """Reset the change-detection state of the repeated queue messages.

    Exposed for tests, which must not see a message suppressed because an
    earlier test in the same process already reported the same value.
    """
    with _report_lock:
        _report_state["scan"] = None
        _report_state["timeinfo"] = None


def report_when_changed(key: str, value: Any, message: str, force: bool = False) -> bool:
    """Log *message* at debug level only when *value* differs from the last one.

    Args:
        key: Entry in the module level report state, e.g. ``'scan'``.
        value: Value identifying what is being reported.
        message: Message to log when the value has changed.
        force: Log regardless of whether the value has changed.

    Returns:
        True if the message was logged.
    """
    with _report_lock:
        changed = _report_state.get(key) != value
        _report_state[key] = value

    if changed or force:
        logger.debug(message)

    return changed or force


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

    The outcome is reported at debug level only when it differs from the last
    reported one, or when the scan itself took a measurable time
    (:data:`SLOW_SCAN_THRESHOLD`). This function is called once per iteration of
    the pilot monitoring loop, i.e. every couple of seconds for the whole life
    of the job, and the answer is the same every time: reporting it
    unconditionally filled the pilot log with thousands of identical lines and
    pushed the messages that do carry information out of sight. A change - a
    job appearing, disappearing, or moving to another queue - is still reported
    the moment it happens, and so is a scan that had to wait.

    Args:
        queues: Named tuple of queue objects.

    Returns:
        List of job objects found, or None if none were found in time.
    """
    _t0 = time.time()
    found_job = False
    jobs = None
    found_in = ''

    while time.time() - _t0 < 30:
        for queue in queues._fields:
            # ignore queues with no job objects
            if queue in {'completed_jobids', 'messages'}:
                continue
            _queue = getattr(queues, queue)
            jobs = list(_queue.queue)
            if len(jobs) > 0:
                found_job = True
                found_in = queue
                break
        if found_job:
            break
        time.sleep(0.1)

    duration = time.time() - _t0
    if found_job:
        report_when_changed(
            'scan', (found_in, len(jobs)),
            f'found {len(jobs)} job(s) in queue {found_in} after {duration:.3f} s - will begin queue monitoring',
            force=duration > SLOW_SCAN_THRESHOLD
        )
    else:
        report_when_changed('scan', None, f'found no jobs in any queue after {duration:.3f} s')

    return jobs


def get_timeinfo_from_job(queues: namedtuple, params: dict, harvester_submitmode: str = '') -> tuple[Optional[int], Optional[int]]:
    """Return the maxwalltime and starttime from the job object.

    Requires the ``PANDAID`` environment variable to be set in order to find
    the correct walltime.

    ``job.maxwalltime`` is only used when the pilot is running in Harvester
    push mode (ARC CEs, OBS), where the batch system uses the job-definition
    ``maxWalltime`` field as its actual kill limit.  In all other cases the
    PQ-level ``queuedata.maxtime`` from CRIC drives the time check.

    Args:
        queues: Named tuple of queue objects.
        params: ``queuedata.params`` dictionary (kept for API compatibility).
        harvester_submitmode: Harvester submit mode string (``'push'`` or ``'pull'``).

    Returns:
        Tuple of ``(maxwalltime, starttime)``, each an int or None.
    """
    maxwalltime = None
    starttime = None
    current_job_id = os.environ.get('PANDAID', None)
    if not current_job_id:
        return None, None

    # job.maxwalltime is only meaningful when the pilot is running in push mode
    # (ARC CE / OBS), where the batch system enforces maxWalltime from the job
    # definition as the hard wall-clock limit.  In pull mode, maxWalltime in the
    # job definition is task-level metadata and should not override the PQ limit.
    # reported only on a change: this is called once per monitoring loop
    # iteration, and the decision depends on the submit mode and the job, both
    # of which are fixed for the duration of a job
    use_job_maxwalltime = harvester_submitmode.lower() == 'push'
    report_when_changed(
        'timeinfo', (use_job_maxwalltime, harvester_submitmode, current_job_id),
        f'use_job_maxwalltime={use_job_maxwalltime} (harvester_submitmode={harvester_submitmode!r}, '
        f'current job id={current_job_id})'
    )

    # extract jobs from the queues
    jobs = scan_for_jobs(queues)
    if jobs:
        for job in jobs:
            if current_job_id == job.jobid:
                if use_job_maxwalltime and job.maxwalltime and isinstance(job.maxwalltime, int):
                    maxwalltime = job.maxwalltime
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
