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
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2023

# NOTE: this module should deal with non-job related monitoring, such as thread monitoring. Job monitoring is
#       a task for the job_monitor thread in the Job component.

import logging
import threading
import time
import re
from os import environ, getpid, getuid
from subprocess import Popen, PIPE

from pilot.common.exception import PilotException, ExceededMaxWaitTime
from pilot.util.auxiliary import check_for_final_server_update, set_pilot_state
from pilot.util.common import is_pilot_check
from pilot.util.config import config
from pilot.util.constants import MAX_KILL_WAIT_TIME
# from pilot.util.container import execute
from pilot.util.features import MachineFeatures
from pilot.util.queuehandling import get_queuedata_from_job, get_maxwalltime_from_job, abort_jobs_in_queues
from pilot.util.timing import get_time_since_start

logger = logging.getLogger(__name__)


# Monitoring of threads functions

def control(queues, traces, args):  # noqa: C901
    """
    Main control function, run from the relevant workflow module.

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    t_0 = time.time()
    traces.pilot['lifetime_start'] = t_0  # ie referring to when pilot monitoring began
    traces.pilot['lifetime_max'] = t_0

    threadchecktime = int(config.Pilot.thread_check)

    # for CPU usage debugging
    cpuchecktime = int(config.Pilot.cpu_check)
    tcpu = t_0
    last_minute_check = t_0

    queuedata = get_queuedata_from_job(queues)
    push = args.harvester and args.harvester_submitmode.lower() == 'push'
    try:
        # overall loop counter (ignoring the fact that more than one job may be running)
        niter = 0

        max_running_time_old = 0
        while not args.graceful_stop.is_set():
            # every few seconds, run the monitoring checks
            if args.graceful_stop.wait(1) or args.graceful_stop.is_set():
                logger.warning('aborting monitor loop since graceful_stop has been set (timing out remaining threads)')
                run_checks(queues, args)
                break

            # abort if kill signal arrived too long time ago, ie loop is stuck
            if args.kill_time and int(time.time()) - args.kill_time > MAX_KILL_WAIT_TIME:
                logger.warning('loop has run for too long time - will abort')
                args.graceful_stop.set()
                break

            # check if the pilot has run out of time (stop ten minutes before PQ limit)
            time_since_start = get_time_since_start(args)
            grace_time = 3 * 60
            if time_since_start - grace_time < 0:
                grace_time = 0
            # get the current max_running_time (can change with job)
            try:
                max_running_time = get_max_running_time(args.lifetime, queuedata, queues, push, args.pod)
            except Exception as exc:
                logger.warning(f'caught exception: {exc}')
                max_running_time = args.lifetime
            else:
                if max_running_time != max_running_time_old:
                    max_running_time_old = max_running_time
                    logger.info(f'using max running time = {max_running_time}s')

            # for testing: max_running_time = 4 * 60
            if time_since_start > max_running_time - grace_time:
                logger.fatal(f'max running time ({max_running_time}s) minus grace time ({grace_time}s) has been '
                             f'exceeded - time to abort pilot')
                reached_maxtime_abort(args)
                break
            else:
                if niter % 60 == 0:
                    logger.info(f'{time_since_start}s have passed since pilot start')

            # every minute run the following check
            if is_pilot_check(check='machinefeatures'):
                if time.time() - last_minute_check > 60:
                    reached_maxtime = run_shutdowntime_minute_check(time_since_start)
                    if reached_maxtime:
                        reached_maxtime_abort(args)
                        break
                    last_minute_check = time.time()

            # take a nap
            time.sleep(1)

            # time to check the CPU usage?
            if is_pilot_check(check='cpu_usage'):
                if int(time.time() - tcpu) > cpuchecktime and False:  # for testing only
                    processes = get_process_info('python3 pilot3/pilot.py', pid=getpid())
                    if processes:
                        logger.info(f'PID={getpid()} has CPU usage={processes[0]}% CMD={processes[2]}')
                        nproc = processes[3]
                        if nproc > 1:
                            logger.info(f'.. there are {nproc} such processes running')
                    tcpu = time.time()

            # proceed with running the other checks
            run_checks(queues, args)

            # thread monitoring
            if is_pilot_check(check='threads'):
                if int(time.time() - traces.pilot['lifetime_start']) % threadchecktime == 0:
                    # get all threads
                    for thread in threading.enumerate():
                        # logger.info('thread name: %s', thread.name)
                        if not thread.is_alive():
                            logger.fatal(f'thread \'{thread.name}\' is not alive')
                            # args.graceful_stop.set()

            niter += 1

    except Exception as error:
        print((f"monitor: exception caught: {error}"))
        raise PilotException(error)

    logger.info('[monitor] control thread has ended')


def run_shutdowntime_minute_check(time_since_start):
    """
    Run checks on machine features shutdowntime once a minute.

    :param time_since_start: how many seconds have lapsed since the pilot started (int).
    :return: True if reached max time, False it not (or if shutdowntime not known) (Boolean).
    """

    # check machine features if present for shutdowntime
    machinefeatures = MachineFeatures().get()
    if machinefeatures:
        grace_time = 10 * 60
        try:
            now = int(time.time())
        except (TypeError, ValueError) as exc:
            logger.warning(f'failed to read current time: {exc}')
            return False  # will be ignored

        # ignore shutdowntime if not known
        shutdowntime = None
        _shutdowntime = machinefeatures.get('shutdowntime', None)
        if _shutdowntime:
            try:
                shutdowntime = int(_shutdowntime)
            except (TypeError, ValueError):  # as exc:
                #logger.debug(f'failed to convert shutdowntime: {exc}')
                return False  # will be ignored
            else:
                logger.debug(f'machinefeatures shutdowntime={shutdowntime} - now={now}')
        if not shutdowntime:
            logger.debug('ignoring shutdowntime since it is not set')
            return False  # will be ignored

        # ignore shutdowntime if in the past (= set before the pilot started)
        if shutdowntime < (now - time_since_start):
            logger.debug(f'shutdowntime ({shutdowntime}) was set before pilot started - ignore it '
                         f'(now - time since start = {now - time_since_start})')
            return False  # will be ignored

        # did we pass, or are we close to the shutdowntime?
        if now > shutdowntime - grace_time:
            logger.fatal(f'now={now}s - shutdowntime ({shutdowntime}s) minus grace time ({grace_time}s) has been '
                         f'exceeded - time to abort pilot')
            return True

    return False


def reached_maxtime_abort(args):
    """
    Max time has been reached, set REACHED_MAXTIME and graceful_stop, close any ActiveMQ connections.
    Wait for final server update before setting graceful_stop.

    :param args: pilot args.
    :return:
    """

    logger.info('setting REACHED_MAXTIME and graceful stop')
    environ['REACHED_MAXTIME'] = 'REACHED_MAXTIME'  # TODO: use singleton instead
    if args.amq:
        logger.debug('closing ActiveMQ connections')
        args.amq.close_connections()
    else:
        logger.debug('No ActiveMQ connections to close')

    # do not set graceful stop if pilot has not finished sending the final job update
    # i.e. wait until SERVER_UPDATE is FINAL_DONE
    # note: if args.update_server is False, the function will return immediately. In that case, make sure
    # that the heartbeat file has been updated (write_heartbeat_to_file() is called from job::send_state())

    # args.update_server = False
    set_pilot_state(state='failed')
    check_for_final_server_update(args.update_server)
    args.graceful_stop.set()


#def log_lifetime(sig, frame, traces):
#    logger.info('lifetime: %i used, %i maximum', int(time.time() - traces.pilot['lifetime_start']), traces.pilot['lifetime_max'])


def get_process_info(cmd, user=None, args='aufx', pid=None):
    """
    Return process info for given command.
    The function returns a list with format [cpu, mem, command, number of commands] as returned by 'ps -u user args' for
    a given command (e.g. python3 pilot3/pilot.py).

    Example
      get_processes_for_command('sshd:')

      nilspal   1362  0.0  0.0 183424  2528 ?        S    12:39   0:00 sshd: nilspal@pts/28
      nilspal   1363  0.0  0.0 136628  2640 pts/28   Ss   12:39   0:00  _ -tcsh
      nilspal   8603  0.0  0.0  34692  5072 pts/28   S+   12:44   0:00      _ python monitor.py
      nilspal   8604  0.0  0.0  62036  1776 pts/28   R+   12:44   0:00          _ ps -u nilspal aufx --no-headers

      -> ['0.0', '0.0', 'sshd: nilspal@pts/28', 1]

    :param cmd: command (string).
    :param user: user (string).
    :param args: ps arguments (string).
    :param pid: process id (int).
    :return: list with process info (l[0]=cpu usage(%), l[1]=mem usage(%), l[2]=command(string)).
    """

    processes = []
    num = 0
    if not user:
        user = str(getuid())
    pattern = re.compile(r"\S+|[-+]?\d*\.\d+|\d+")
    arguments = ['ps', '-u', user, args, '--no-headers']

    process = Popen(arguments, stdout=PIPE, stderr=PIPE, encoding='utf-8')
    stdout, _ = process.communicate()
    for line in stdout.splitlines():
        found = re.findall(pattern, line)
        if found is not None:
            processid = found[1]
            cpu = found[2]
            mem = found[3]
            command = ' '.join(found[10:])
            if cmd in command:
                num += 1
                if processid == str(pid):
                    processes = [cpu, mem, command]

    if processes:
        processes.append(num)
    return processes


def run_checks(queues, args):
    """
    Perform non-job related monitoring checks.

    :param queues:
    :param args:
    :return:
    """

    # check how long time has passed since last successful heartbeat
    if is_pilot_check(check='last_heartbeat'):
        last_heartbeat = time.time() - args.last_heartbeat
        if last_heartbeat > config.Pilot.lost_heartbeat and args.update_server:
            diagnostics = f'too much time has passed since last successful heartbeat ({last_heartbeat} s)'
            logger.warning(diagnostics)
            logger.warning('aborting pilot - no need to wait for job to finish - kill everything')
            args.job_aborted.set()
            args.graceful_stop.set()
            args.abort_job.clear()
            raise ExceededMaxWaitTime(diagnostics)

    if args.graceful_stop.is_set():
        # find all running jobs and stop them, find all jobs in queues relevant to this module
        abort_jobs_in_queues(queues, args.signal)

        t_max = 5 * 60
        logger.warning('pilot monitor received instruction that args.graceful_stop has been set')
        logger.warning(f'will wait for a maximum of {t_max} s for threads to finish')
        t_0 = time.time()
        ret = False
        while time.time() - t_0 < t_max:
            if args.job_aborted.is_set():
                logger.warning('job_aborted has been set - aborting pilot monitoring')
                #args.abort_job.clear()
                ret = True
                break
            time.sleep(1)
        if ret:
            return

        diagnostics = 'reached maximum waiting time - threads should have finished (ignore exception)'
        #args.abort_job.clear()
        args.job_aborted.set()
        raise ExceededMaxWaitTime(diagnostics)

#        if not args.job_aborted.is_set():
#            t_max = 180
#            logger.warning(f'will wait for a maximum of {t_max} s for graceful_stop to take effect')
#            t_0 = time.time()
#            ret = False
#            while time.time() - t_0 < t_max:
#                if args.job_aborted.is_set():
#                    logger.warning('job_aborted has been set - aborting pilot monitoring')
#                    #args.abort_job.clear()
#                    ret = True
#                    break
#                time.sleep(1)
#            if ret:
#                return

#            diagnostics = 'reached maximum waiting time - threads should have finished'
#            args.abort_job.clear()
#            args.job_aborted.set()
#            raise ExceededMaxWaitTime(diagnostics)


def get_max_running_time(lifetime, queuedata, queues, push, pod):
    """
    Return the maximum allowed running time for the pilot.
    The max time is set either as a pilot option or via the schedconfig.maxtime for the PQ in question.
    If running in a Kubernetes pod, always use the args.lifetime as maxtime (it will be determined by the harvester submitter).

    :param lifetime: optional pilot option time in seconds (int).
    :param queuedata: queuedata object
    :param queues:
    :param push: push mode (boolean)
    :param pod: pod mode (boolean)
    :return: max running time in seconds (int)
    """

    if pod:
        return lifetime

    max_running_time = lifetime

    if not queuedata:
        #logger.warning(f'queuedata could not be extracted from queues, will use default for max running time '
        #               f'({max_running_time}s)')
        return max_running_time

    # for push queues: try to get the walltime from the job object first, in case it exists and is set
    if push:
        try:
            _max_running_time = get_maxwalltime_from_job(queues, queuedata.params)
        except Exception as exc:
            logger.warning(f'caught exception: {exc}')
        else:
            if _max_running_time:
                #logger.debug(f'using max running time from job: {_max_running_time}s')
                return _max_running_time

    # use the schedconfig value if set, otherwise use the pilot option lifetime value
    if queuedata.maxtime:
        try:
            max_running_time = int(queuedata.maxtime)
        except Exception as error:
            logger.warning(f'exception caught: {error}')
            logger.warning(f'failed to convert maxtime from queuedata, will use default value for max running time '
                           f'({max_running_time}s)')
        else:
            if max_running_time == 0:
                max_running_time = lifetime  # fallback to default value
            #    logger.debug(f'will use default value for max running time: {max_running_time}s')
            #else:
            #    logger.debug(f'will use queuedata.maxtime value for max running time: {max_running_time}s')

    return max_running_time
