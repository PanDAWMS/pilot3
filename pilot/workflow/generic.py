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
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-25
# - Shuwei Ye, yesw@bnl.gov, 2021

import functools
import logging
import signal
import threading
import traceback
import queue

from collections import namedtuple
from os import getpid
from shutil import rmtree
from sys import stderr
from time import (
    time,
    sleep
)
from types import FrameType

from pilot.common.exception import ExcThread
from pilot.util.constants import (
    MAX_KILL_WAIT_TIME,
    PILOT_KILL_SIGNAL,
    SUCCESS,
    FAILURE
)
from pilot.control import (
    data,
    job,
    monitor,
    payload,
)
from pilot.util.processes import (
    kill_processes,
    threads_aborted
)
from pilot.util.timing import add_to_pilot_timing

logger = logging.getLogger(__name__)
# Define Traces namedtuple at the module level
Traces = namedtuple("Traces", ["pilot"])


def interrupt(args: object, signum: int, frame: FrameType):
    """
    Handle signals for graceful exit.

    This function is forwarded any incoming signals (SIGINT, SIGTERM, etc) and will set abort_job which instructs
    the threads to abort the job.

    :param args: pilot arguments (object)
    :param signum: signal number (int)
    :param frame: stack/execution frame pointing to the frame that was interrupted by the signal (object).
    """
    sig = [v for v, k in list(signal.__dict__.items()) if k == signum][0]

    # ignore SIGUSR1 since that will be aimed at a child process
    #if str(sig) == 'SIGUSR1':
    #    logger.info('ignore intercepted SIGUSR1 aimed at child process')
    #    return
    if not hasattr(args, 'signal_counter'):
        args.signal_counter = 0
    args.signal_counter += 1

    # keep track of when first kill signal arrived, any stuck loops should abort at a defined cut off time
    if args.kill_time == 0:
        args.kill_time = int(time())

    max_kill_wait_time = MAX_KILL_WAIT_TIME + 60  # add another minute of grace to let threads finish
    current_time = int(time())
    if args.kill_time and current_time - args.kill_time > max_kill_wait_time:
        logger.warning('passed maximum waiting time after first kill signal - will commit suicide - farewell')
        try:
            if hasattr(args, 'sourcedir'):
                rmtree(args.sourcedir)
        except (TypeError, OSError) as e:
            logger.warning(e)
        logging.shutdown()
        kill_processes(getpid())

    add_to_pilot_timing('0', PILOT_KILL_SIGNAL, time(), args)
    add_to_pilot_timing('1', PILOT_KILL_SIGNAL, time(), args)
    logger.warning('caught signal: %s in FRAME=\n%s', sig, '\n'.join(traceback.format_stack(frame)))

    args.signal = sig
    logger.warning('will instruct threads to abort and update the server')

    if not hasattr(args, 'abort_job'):
        args.abort_job = threading.Event()
    args.abort_job.set()

    logger.warning('setting graceful stop (in case it was not set already)')

    if not hasattr(args, 'graceful_stop'):
        args.graceful_stop = threading.Event()
    args.graceful_stop.set()

    logger.warning('waiting for threads to finish')

    if not hasattr(args, 'job_aborted'):
        args.job_aborted = threading.Event()
    args.job_aborted.wait(timeout=180)


def register_signals(signals: list, args: object):
    """
    Register kill signals for intercept function.

    :param signals: list of signals (list)
    :param args: pilot arguments object (object).
    """
    for sig in signals:
        signal.signal(sig, functools.partial(interrupt, args))


def run(args: object) -> Traces or None:
    """
    Main execution function for the generic workflow.

    The function sets up the internal queues which handle the flow of jobs.

    :param args: pilot arguments object (object)
    :returns: traces object (Traces namedtuple or None)
    """
    logger.info('setting up signal handling')

    register_signals([signal.SIGINT,
                      signal.SIGTERM,
                      signal.SIGQUIT,
                      signal.SIGSEGV,
                      signal.SIGXCPU,
                      signal.SIGUSR1,
                      signal.SIGBUS],
                     args)

    queues = namedtuple('queues', ['jobs', 'payloads', 'data_in', 'data_out', 'current_data_in',
                                   'validated_jobs', 'validated_payloads', 'monitored_payloads',
                                   'finished_jobs', 'finished_payloads', 'finished_data_in', 'finished_data_out',
                                   'failed_jobs', 'failed_payloads', 'failed_data_in', 'failed_data_out',
                                   'completed_jobs', 'completed_jobids', 'realtimelog_payloads', 'messages'])

    queues.jobs = queue.Queue()
    queues.payloads = queue.Queue()
    queues.data_in = queue.Queue()
    queues.data_out = queue.Queue()

    queues.current_data_in = queue.Queue()
    queues.validated_jobs = queue.Queue()
    queues.validated_payloads = queue.Queue()
    queues.monitored_payloads = queue.Queue()

    queues.finished_jobs = queue.Queue()
    queues.finished_payloads = queue.Queue()
    queues.finished_data_in = queue.Queue()
    queues.finished_data_out = queue.Queue()

    queues.failed_jobs = queue.Queue()
    queues.failed_payloads = queue.Queue()
    queues.failed_data_in = queue.Queue()
    queues.failed_data_out = queue.Queue()

    queues.completed_jobs = queue.Queue()
    queues.completed_jobids = queue.Queue()
    queues.realtimelog_payloads = queue.Queue()
    queues.messages = queue.Queue()

    # queues.interceptor_messages = queue.Queue()

    # Initialize traces with default values
    traces = Traces(pilot={"state": SUCCESS, "nr_jobs": 0, "error_code": 0, "command": None})

    # initial sanity check defined by pilot user
    try:
        if not hasattr(args, 'pilot_user'):
            logger.warning('pilot_user not defined - setting generic user')
            args.pilot_user = 'generic'
        user = __import__(f'pilot.user.{args.pilot_user.lower()}.common', globals(), locals(),
                          [args.pilot_user.lower()], 0)
        exit_code = user.sanity_check()
    except Exception as exc:
        logger.info(f'skipping sanity check since: {exc}')
    else:
        if exit_code != 0:
            logger.info('aborting workflow since sanity check failed')
            # Update traces using _replace for immutable update
            traces = traces._replace(pilot={"state": FAILURE,
                                            "nr_jobs": traces.pilot["nr_jobs"],
                                            "error_code": exit_code})
            #traces.pilot['error_code'] = exit_code
            return traces

    # define the threads
    targets = {
        'job': job.control,
        'payload': payload.control,
        'data': data.control,
        'monitor': monitor.control,
        'cgroup_monitor': monitor.cgroup_control
    }
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in list(targets.items())]

    _ = [thread.start() for thread in threads]

    logger.info('waiting for interrupts')

    # the active_count() is the total number of threads, not just the ExcThreads above
    abort = False
    try:
        while threading.active_count() > 1 or not abort:
            # Note: this loop only includes at ExcThreads, not MainThread or Thread
            # threading.activeCount() will also include MainThread and any daemon threads (will be ignored)
            for thread in threads:
                bucket = thread.get_bucket()
                try:
                    exc = bucket.get(block=False)
                except queue.Empty:
                    pass
                else:
                    _, exc_obj, _ = exc
                    # deal with the exception
                    print(f'received exception from bucket queue in generic workflow: {exc_obj}', file=stderr)

                thread.join(0.1)

            # have all threads finished?
            abort = threads_aborted(caller='run')
            if abort:
                logger.debug('will proceed to set job_aborted')

                if not hasattr(args, 'job_aborted'):
                    args.job_aborted = threading.Event()
                args.job_aborted.set()

                sleep(5)  # allow monitor thread to finish (should pick up job_aborted within 1 second)
                logger.debug(f'all relevant threads have aborted (thread count={threading.active_count()})')
                break

            sleep(1)
    except Exception as exc:
        logger.warning(f"exception caught while handling threads: {exc}")
    finally:
        logger.info('all workflow threads have been joined')

    logger.info(f'end of generic workflow (traces error code: {traces.pilot["error_code"]})')

    return traces
