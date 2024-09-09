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

# Note: leave this module for now - the code might be useful for reuse

"""Interceptor module, currently unused."""

import time
import queue
import logging

from pilot.common.exception import ExcThread
from pilot.util.processes import threads_aborted

logger = logging.getLogger(__name__)


def run(args: object):
    """
    Set up all interceptor threads.

    Main execution function for the interceptor communication layer.

    :param args: pilot arguments (object)
    """
    targets = {'receive': receive, 'send': send}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'args': args},
                         name=name) for name, target in list(targets.items())]  # Python 2/3

    _ = [thread.start() for thread in threads]

    # if an exception is thrown, the graceful_stop will be set by the ExcThread class run() function
    while not args.graceful_stop.is_set():
        for thread in threads:
            bucket = thread.get_bucket()
            try:
                exc = bucket.get(block=False)
            except queue.Empty:
                pass
            else:
                _, exc_obj, _ = exc
                logger.warning("thread \'%s\' received an exception from bucket: %s", thread.name, exc_obj)

                # deal with the exception
                # ..

            thread.join(0.1)
            time.sleep(0.1)

        time.sleep(0.5)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[interceptor] run thread has finished')


def receive(args: object):
    """
    Look for interceptor messages.

    :param args: Pilot args object (object).
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[interceptor] receive thread has finished')


def send(args: object):
    """
    Send message to interceptor.

    :param args: Pilot args object (Any).
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.debug('[interceptor] receive send has finished')


# implement if necessary
# def interceptor(queues: namedtuple, traces: Any, args: object):
#    """
#
#    :param queues: internal queues for job handling (namedtuple)
#    :param traces: tuple containing internal pilot states (tupl)
#    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object).
#    """
#    # overall loop counter (ignoring the fact that more than one job may be running)
#    counter = 0
#    while not args.graceful_stop.is_set():
#        time.sleep(0.1)
#
#        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
#        # (abort at the end of the loop)
#        abort = should_abort(args, label='job:interceptor')
#
#        # check for any abort_job requests
#        abort_job = check_for_abort_job(args, caller='interceptor')
#        if not abort_job:
#            # peek at the jobs in the validated_jobs queue and send the running ones to the heartbeat function
#            jobs = queues.monitored_payloads.queue
#            if jobs:
#                for _ in range(len(jobs)):
#                    logger.info(f'interceptor loop {counter}: looking for communication file')
#            time.sleep(30)
#
#        counter += 1
#
#        if abort or abort_job:
#            break
#
#    # proceed to set the job_aborted flag?
#    if threads_aborted(caller='interceptor'):
#        logger.debug('will proceed to set job_aborted')
#        args.job_aborted.set()
#
#    logger.info('[job] interceptor thread has finished')
