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

import logging
import os
import signal
import time
from collections import namedtuple
from queue import Queue

from pilot.common.errorcodes import ErrorCodes
from pilot.info import JobData
from pilot.util.auxiliary import (
    set_pilot_state,
    is_string
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def get_signal_name(sig_num: int) -> str:
    """
    Return the signal name for the given signal number.

    :param sig_num: signal number (int)
    :return: signal name (str).
    """
    try:
        # Convert signal number to its enumeration equivalent and then to string
        return signal.Signals(sig_num).name
    except ValueError:
        # If the signal number is not a valid signal, return None or handle as needed
        return None


def declare_failed_by_kill(job: object, queue: Queue, sig: int):
    """
    Declare the job failed by a kill signal and put it in a suitable failed queue.

    E.g. queue=queues.failed_data_in, if the kill signal was received during stage-in.

    :param job: job object (object)
    :param queue: queue object (Queue)
    :param sig: signal (int).
    """
    set_pilot_state(job=job, state="failed")
    signal_name = get_signal_name(sig)
    if not signal_name:
        logger.warning(f'could not find signal name for signal number {sig} - using SIGTERM')
        signal_name = 'SIGTERM'
    error_code = errors.get_kill_signal_error_code(signal_name)
    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code)

    #queue.put(job)
    put_in_queue(job, queue)


def scan_for_jobs(queues: namedtuple) -> list:
    """
    Scan queues until at least one queue has a job object. abort if it takes too long time

    :param queues:
    :return: found jobs (list of job objects).
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


def get_timeinfo_from_job(queues: namedtuple, params: dict) -> tuple[int or None, int or None]:
    """
    Return the maxwalltime and starttime from the job object.

    The algorithm requires a set PANDAID environmental variable, in order to find the correct walltime.

    :param queues: queues object (namedtuple)
    :param params: queuedata.params (dict)
    :return: maxwalltime (int or None), starttime (int or None) (tuple).
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


def get_queuedata_from_job(queues: namedtuple) -> object or None:
    """
    Return the queuedata object from a job in the given queues object.

    This function is useful if queuedata is needed from a function that does not know about the job object.
    E.g. the pilot monitor does not know about the job object, but still knows
    about the queues from which a job object can be extracted and therefore the queuedata.

    :param queues: queues object (namedtuple)
    :return: queuedata (object or None).
    """
    queuedata = None

    # extract jobs from the queues
    jobs = scan_for_jobs(queues)
    if jobs:
        for job in jobs:
            queuedata = job.infosys.queuedata
            break

    return queuedata


def abort_jobs_in_queues(queues: namedtuple, sig: int):
    """
    Find all jobs in the queues and abort them.

    :param queues: queues object (namedtuple)
    :param sig: detected kill signal (int)
    """
    jobs_list = []

    # loop over all queues and find all jobs
    for queue in queues._fields:
        _queue = getattr(queues, queue)
        jobs = list(_queue.queue)
        for job in jobs:
            if is_string(job):  # this will be the case for the completed_jobids queue
                continue
            if job not in jobs_list:
                jobs_list.append(job)

    logger.info(f'found {len(jobs_list)} job(s) in {len(queues._fields)} queues')
    for job in jobs_list:
        logger.info(f'aborting job {job.jobid}')
        declare_failed_by_kill(job, queues.failed_jobs, sig)


def queue_report(queues: namedtuple, purge: bool = False):
    """
    Report on how many jobs are till in the various queues.

    This function can also empty the queues (except completed_jobids).

    :param queues: queues object (namedtuple)
    :param purge: clean up queues if True (bool).
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


def put_in_queue(obj: object, queue: Queue):
    """
    Put the given object in the given queue.

    :param obj: object to put in the queue (object)
    :param queue: queue object (Queue).
    """
    # update job object size (currently not used)
    if isinstance(obj, JobData):
        obj.add_size(obj.get_size())

    # only put the object in the queue if it is not there already
    if obj not in list(queue.queue):
        queue.put(obj)


def purge_queue(queue: Queue):
    """
    Empty given queue.

    :param queue: queue object (Queue).
    """
    while not queue.empty():
        try:
            queue.get(False)
        except queue.Empty:
            continue
        queue.task_done()

    logger.debug('queue purged')
