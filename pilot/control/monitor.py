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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-25

# NOTE: this module should deal with non-job related monitoring, such as thread monitoring. Job monitoring is
#       a task for the job_monitor thread in the Job component.

"""Functions for monitoring of pilot and threads."""

import logging
import os
import threading
import time
import re

from collections import namedtuple
from os import environ, getuid, getpid
from subprocess import (
    Popen,
    PIPE
)
from typing import Any

from pilot.common.exception import PilotException, ExceededMaxWaitTime
from pilot.common.pilotcache import get_pilot_cache
from pilot.util.auxiliary import (
    check_for_final_server_update,
    set_pilot_state
)
from pilot.util.cgroups import monitor_cgroup
from pilot.util.common import is_pilot_check
from pilot.util.config import config
from pilot.util.constants import MAX_KILL_WAIT_TIME
# from pilot.util.container import execute
from pilot.util.features import MachineFeatures
from pilot.util.heartbeat import update_pilot_heartbeat
from pilot.util.https import (
    get_local_oidc_token_info,
    update_local_oidc_token_info
)
from pilot.util.psutils import get_process_info
from pilot.util.queuehandling import (
    abort_jobs_in_queues,
    get_timeinfo_from_job,
    get_queuedata_from_job,
)
from pilot.util.timing import get_time_since_start

pilot_cache = get_pilot_cache()
logger = logging.getLogger(__name__)


def cgroup_control(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Control function for the cgroup monitor.

    This function is called from the main control thread to set up the cgroup monitor task.

    Args:
        queues: internal queues for job handling (namedtuple)
        traces: tuple containing internal pilot states (Any)
        args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    if queues or traces:  # to bypass pylint warning
        pass

    # set up the periodic cgroup monitor task
    while not args.graceful_stop.is_set():
        pilot_cgroup_path = pilot_cache.get_cgroup(os.getpid())
        logger.debug(f"monitoring pilot cgroup at path: {pilot_cgroup_path}")
        if pilot_cgroup_path:
            monitor_cgroup(pilot_cgroup_path)

        subprocesses_cgroup_path = pilot_cache.get_cgroup('subprocesses')
        logger.debug(f"monitoring subprocesses cgroup at path: {subprocesses_cgroup_path}")
        if subprocesses_cgroup_path:
            monitor_cgroup(subprocesses_cgroup_path)

        time.sleep(60)

    logger.info("[monitor] cgroup control has ended")


def control(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Monitor threads.

    Main control function, run from the relevant workflow module.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    t_0 = time.time()
    traces.pilot['lifetime_start'] = t_0  # ie referring to when pilot monitoring began
    traces.pilot['lifetime_max'] = t_0

    threadchecktime = int(config.Pilot.thread_check)
    # if OIDC tokens are used, define the time interval for checking the token
    # otherwise the following variable is None
    tokendownloadchecktime = get_oidc_check_time()
    last_token_check = t_0

    # for CPU usage debugging
    cpuchecktime = int(config.Pilot.cpu_check)
    tcpu = t_0
    last_minute_check = t_0

    queuedata = pilot_cache.queuedata
    if not queuedata:
        logger.warning("no queuedata in pilot cache, will try to extract it from queues")
        queuedata = get_queuedata_from_job(queues)
    if not queuedata:
        logger.warning('queuedata could not be extracted from queues either')

    try:
        # overall loop counter (ignoring the fact that more than one job may be running)
        n_iterations = 0

        max_running_time_old = 0
        while not args.graceful_stop.is_set():
            # every few seconds, run the monitoring checks
            if args.graceful_stop.wait(1) or args.graceful_stop.is_set():
                logger.warning('aborting monitor loop since graceful_stop has been set (timing out remaining threads)')
                run_checks(queues, args)
                break

            # check if the OIDC token needs to be refreshed
            if tokendownloadchecktime and queuedata:
                if int(time.time() - last_token_check) > tokendownloadchecktime:
                    last_token_check = time.time()
                    if 'no_token_renewal' in queuedata.catchall or args.token_renewal is False:
                        logger.info("OIDC token will not be renewed by the pilot")
                    else:
                        update_local_oidc_token_info(args.url, args.port)

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
                max_running_time, start_time = get_timeinfo(args.lifetime, queuedata, queues, args.pod)
            except Exception as exc:
                logger.warning(f'caught exception: {exc}')
                max_running_time = args.lifetime
            else:
                if max_running_time != max_running_time_old:
                    max_running_time_old = max_running_time
                    logger.info(f'using max running time = {max_running_time}s')

            # if start_time for the current job is known (push queues), a more detailed check can be performed
            start_time_ok = False
            if start_time and queuedata:  # in epoch seconds
                time_since_job_start = int(time.time()) - start_time
                # in this case, max_running_time is the max job walltime
                limit = max_running_time * queuedata.pilot_walltime_grace  # queuedata.pilot_walltime_grace = 1 + PQ.pilot_walltime_grace/100
                if time_since_job_start > limit:
                    logger.fatal(f"time since job start ({time_since_job_start}s) has exceeded the limit ({limit}s) - time to abort pilot")
                    logger.fatal(f'limit = max running time ({max_running_time}s) * pilot walltime grace ({queuedata.pilot_walltime_grace})')
                    reached_maxtime_abort(args)
                    break
                else:
                    logger.info(f'time since job start ({time_since_job_start}s) is within the limit ({limit}s)')
                    logger.debug(f'max running time = {max_running_time}s, queuedata.pilot_walltime_grace = {queuedata.pilot_walltime_grace}')
                    start_time_ok = True

            # fallback to max_running_time if start_time is not known
            if (time_since_start > max_running_time - grace_time) and not start_time_ok:
                logger.fatal(f'max running time ({max_running_time}s) minus grace time ({grace_time}s) has been '
                             f'exceeded - time to abort pilot')
                reached_maxtime_abort(args)
                break

            if n_iterations % 60 == 0:
                logger.info(f"{time_since_start}s have passed since pilot start - server update state is \'{environ['SERVER_UPDATE']}\'")
                logger.debug(f"args.update_server={args.update_server}")

            # every minute run the following check
            if is_pilot_check(check='machinefeatures'):
                if time.time() - last_minute_check > 60:
                    reached_maxtime = run_shutdowntime_minute_check(time_since_start)
                    if reached_maxtime:
                        reached_maxtime_abort(args)
                        break
                    last_minute_check = time.time()

            # test max
            #time.sleep(120)
            #reached_maxtime_abort(args)
            # take a nap
            time.sleep(1)

            # time to check the CPU usage?
            if is_pilot_check(check='cpu_usage'):
                if int(time.time() - tcpu) > cpuchecktime:
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

            n_iterations += 1

    except Exception as error:
        print((f"monitor: exception caught: {error}"))
        raise PilotException(error) from error

    # shut down the cgroups monitoring task
    # logger.info("[monitor] waiting for cgroup monitor task to finish")
    # await task

    logger.info('[monitor] control thread has ended')


def get_oidc_check_time() -> int or None:
    """
    Return the time interval for checking the OIDC token.

    :return: time interval for checking the OIDC token (int or None).
    """
    auth_token, auth_origin = get_local_oidc_token_info()
    use_oidc_token = True if auth_token and auth_origin else False
    if use_oidc_token:
        try:
            token_check = int(config.Token.download_check)
        except (AttributeError, ValueError):
            token_check = None
    else:
        token_check = None

    return token_check


def run_shutdowntime_minute_check(time_since_start: int) -> bool:
    """
    Run checks on machine features shutdowntime once a minute.

    Args:
        time_since_start (int): how many seconds have lapsed since the pilot started

    Returns:
        True if reached max time, False otherwise (also if shutdowntime not known) (bool).
    """
    # check machine features if present for shutdowntime
    machinefeatures = MachineFeatures().get()
    logger.debug(f"MachineFeatures().get()={machinefeatures}")
    logger.debug(f"type(machinefeatures)={type(machinefeatures)}")

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
            except (TypeError, ValueError) as exc:
                logger.warning(f'failed to convert shutdowntime: {exc}')
                return False  # will be ignored

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


def reached_maxtime_abort(args: Any):
    """
    Set REACHED_MAXTIME and graceful_stop, since max time has been reached.

    Also close any ActiveMQ connections
    Wait for final server update before setting graceful_stop.

    :param args: Pilot arguments object (Any).
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


def get_process_info_old(cmd: str, user: str = "", args: str = 'aufx', pid: int = 0) -> list:
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

    :param cmd: command (str)
    :param user: user (str)
    :param args: ps arguments (str)
    :param pid: process id (int)
    :return: list with process info (l[0]=cpu usage(%), l[1]=mem usage(%), l[2]=command(string)) (list).
    """
    processes = []
    num = 0
    if not user:
        user = str(getuid())
    pattern = re.compile(r"\S+|[-+]?\d*\.\d+|\d+")
    arguments = ['ps', '-u', user, args, '--no-headers']

    with Popen(arguments, stdout=PIPE, stderr=PIPE, encoding='utf-8') as process:
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


def get_proper_pilot_heartbeat() -> int:
    """
    Return the proper pilot heartbeat time limit from config.

    :return: pilot heartbeat time limit (int).
    """

    try:
        return int(config.Pilot.pilot_heartbeat)
    except Exception as exc:
        logger.warning(f'detected outdated config file: please update default.cfg: {exc}')
        return 60


def run_checks(queues: namedtuple, args: object) -> None:
    """
    Perform non-job related monitoring checks.

    :param queues: queues object (namedtuple)
    :param args: Pilot arguments object (object)
    :raises: ExceedMaxWaitTime.
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

    # note: active update rather than a check (every ten minutes)
    if is_pilot_check(check='pilot_heartbeat'):
        last_heartbeat = time.time() - args.pilot_heartbeat
        _pilot_heartbeat = get_proper_pilot_heartbeat()

        if last_heartbeat > _pilot_heartbeat:
            detected_job_suspension = last_heartbeat > 10 * 60
            if detected_job_suspension:
                logger.warning(f'detected job suspension (last heartbeat was updated more than 10 minutes ago: {last_heartbeat} s)')
            else:
                logger.debug(f'pilot heartbeat file was last updated {last_heartbeat} s ago (time to update)')

            # if the pilot heartbeat file can be updated, update the args object
            _time = time.time()
            if update_pilot_heartbeat(_time, detected_job_suspension=detected_job_suspension, time_since_detection=last_heartbeat):
                args.pilot_heartbeat = _time

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


def get_timeinfo(lifetime: int, queuedata: Any, queues: namedtuple, pod: bool) -> tuple[int or None, int or None]:
    """
    Return the maximum allowed running time for the pilot and any start time for the running job.

    The max time is set either as a pilot option or via the schedconfig.maxtime for the PQ in question.
    If running in a Kubernetes pod, always use the args.lifetime as maxtime (it will be determined by the harvester submitter).

    :param lifetime: optional pilot option time in seconds (int)
    :param queuedata: queuedata object (Any)
    :param queues: queues object (namedtuple)
    :param pod: pod mode (bool)
    :return: max running time in seconds (int or None), start time in seconds (int or None) (tuple).
    """
    if pod:
        return lifetime, None

    max_running_time = lifetime
    start_time = None

    if not queuedata:
        #logger.warning(f'queuedata could not be extracted from queues, will use default for max running time '
        #               f'({max_running_time}s)')
        return max_running_time, start_time

    try:
        _max_running_time, start_time = get_timeinfo_from_job(queues, queuedata.params)
    except Exception as exc:
        logger.warning(f'caught exception: {exc}')
    else:
        if _max_running_time:
            logger.debug(f'using max running time from job: {_max_running_time}s and start time: {start_time}')
            return _max_running_time, start_time

    # use the schedconfig value if set, otherwise use the pilot option lifetime value
    if queuedata.maxtime:
        try:
            max_running_time = int(queuedata.maxtime)
        except Exception as error:
            logger.warning(f'exception caught: {error}')
            logger.warning(f'failed to convert maxtime from queuedata, will use default value for max running time '
                           f'({max_running_time}s)')
        else:
            max_running_time = lifetime if max_running_time == 0 else max_running_time
            #    logger.debug(f'will use default value for max running time: {max_running_time}s')
            #else:
            #    logger.debug(f'will use queuedata.maxtime value for max running time: {max_running_time}s')

    return max_running_time, start_time
