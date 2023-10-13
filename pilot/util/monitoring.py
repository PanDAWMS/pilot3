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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

# This module contains implementations of job monitoring tasks

import os
import time
import subprocess
from glob import glob
from typing import Any
from signal import SIGKILL

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException, MiddlewareImportFailure
from pilot.util.auxiliary import set_pilot_state  #, show_memory_usage
from pilot.util.config import config
from pilot.util.constants import PILOT_PRE_PAYLOAD
from pilot.util.container import execute
from pilot.util.filehandling import (
    get_disk_usage,
    remove_files,
    get_local_file_size,
    read_file,
    zip_files
)
from pilot.util.loopingjob import looping_job
from pilot.util.math import (
    convert_mb_to_b,
    human2bytes
)
from pilot.util.parameters import (
    convert_to_int,
    get_maximum_input_sizes
)
from pilot.util.processes import (
    get_current_cpu_consumption_time,
    kill_processes,
    get_number_of_child_processes,
    reap_zombies
)
from pilot.util.psutils import (
    is_process_running,
    get_pid,
    get_subprocesses
)
from pilot.util.timing import get_time_since
from pilot.util.workernode import (
    get_local_disk_space,
    check_hz
)
from pilot.info import infosys

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def job_monitor_tasks(job, mt, args):  # noqa: C901
    """
    Perform the tasks for the job monitoring.
    The function is called once a minute. Individual checks will be performed at any desired time interval (>= 1
    minute).

    :param job: job object.
    :param mt: `MonitoringTime` object.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    # verify that the process is still alive
    if not still_running(job.pid):
        return 0, ""

    current_time = int(time.time())

    # update timing info for running jobs (to avoid an update after the job has finished)
    if job.state == 'running':

        # make sure that any utility commands are still running (and determine pid of memory monitor- as early as possible)
        if job.utilities != {}:
            utility_monitor(job)

        # confirm that the worker node has a proper SC_CLK_TCK (problems seen on MPPMU)
        check_hz()

        try:
            cpuconsumptiontime = get_current_cpu_consumption_time(job.pid)
        except Exception as error:
            diagnostics = f"Exception caught: {error}"
            logger.warning(diagnostics)
            exit_code = get_exception_error_code(diagnostics)
            return exit_code, diagnostics
        else:
            _cpuconsumptiontime = int(round(cpuconsumptiontime))
            if _cpuconsumptiontime > 0:
                job.cpuconsumptiontime = int(round(cpuconsumptiontime))
                job.cpuconversionfactor = 1.0
                logger.info(f'(instant) CPU consumption time for pid={job.pid}: {cpuconsumptiontime} (rounded to {job.cpuconsumptiontime})')
            else:
                logger.warning(f'process {job.pid} is no longer using CPU - aborting')
                return 0, ""

        # keep track of the subprocesses running (store payload subprocess PIDs)
        store_subprocess_pids(job)

        # check how many cores the payload is using
        time_since_start = get_time_since(job.jobid, PILOT_PRE_PAYLOAD, args)  # payload walltime
        set_number_used_cores(job, time_since_start)

        # check memory usage (optional) for jobs in running state
        exit_code, diagnostics = verify_memory_usage(current_time, mt, job, debug=args.debug)
        if exit_code != 0:
            return exit_code, diagnostics

        # display OOM process info
        display_oom_info(job.pid)

    # should the pilot abort the payload?
    exit_code, diagnostics = should_abort_payload(current_time, mt)
    if exit_code != 0:
        return exit_code, diagnostics

    # check lease time in stager/pod mode on Kubernetes
    if args.workflow == 'stager':
        exit_code, diagnostics = check_lease_time(current_time, mt, args.leasetime)
        if exit_code != 0:
            return exit_code, diagnostics

    # is it time to verify the pilot running time?
#    exit_code, diagnostics = verify_pilot_running_time(current_time, mt, job)
#    if exit_code != 0:
#        return exit_code, diagnostics

    # should the proxy be verified?
    if args.verify_proxy:
        exit_code, diagnostics = verify_user_proxy(current_time, mt)
        if exit_code != 0:
            return exit_code, diagnostics

    # is it time to check for looping jobs?
    exit_code, diagnostics = verify_looping_job(current_time, mt, job, args)
    if exit_code != 0:
        return exit_code, diagnostics

    # is the job using too much space?
    exit_code, diagnostics = verify_disk_usage(current_time, mt, job)
    if exit_code != 0:
        return exit_code, diagnostics

    # is it time to verify the number of running processes?
    if job.pid:
        exit_code, diagnostics = verify_running_processes(current_time, mt, job.pid)
        if exit_code != 0:
            return exit_code, diagnostics

    logger.debug(f'job monitor tasks loop took {int(time.time()) - current_time} s to complete')

    return exit_code, diagnostics


def still_running(pid):
    # verify that the process is still alive

    running = False
    try:
        if pid:
            if not is_process_running(pid):
                logger.warning(f'aborting job monitor tasks since payload process {pid} is not running')
            else:
                running = True
                logger.debug(f'payload process {pid} is running')
    except MiddlewareImportFailure as exc:
        logger.warning(f'exception caught: {exc}')

    return running


def display_oom_info(payload_pid):
    """
    Display OOM process info.

    :param payload_pid: payload pid (int).
    """

    payload_score = get_score(payload_pid) if payload_pid else 'UNKNOWN'
    pilot_score = get_score(os.getpid())
    logger.info(f'oom_score(pilot) = {pilot_score}, oom_score(payload) = {payload_score}')


def get_score(pid):
    """
    Get the OOM process score.

    :param pid: process id (int).
    :return: score (string).
    """

    try:
        score = '%s' % read_file('/proc/%d/oom_score' % pid)
    except Exception as error:
        logger.warning(f'caught exception reading oom_score: {error}')
        score = 'UNKNOWN'
    else:
        if score.endswith('\n'):
            score = score[:-1]

    return score


def get_exception_error_code(diagnostics):
    """
    Identify a suitable error code to a given exception.

    :param diagnostics: exception diagnostics (string).
    :return: exit_code
    """

    import traceback
    logger.warning(traceback.format_exc())
    if "Resource temporarily unavailable" in diagnostics:
        exit_code = errors.RESOURCEUNAVAILABLE
    elif "No such file or directory" in diagnostics:
        exit_code = errors.STATFILEPROBLEM
    elif "No such process" in diagnostics:
        exit_code = errors.NOSUCHPROCESS
    else:
        exit_code = errors.GENERALCPUCALCPROBLEM

    return exit_code


def set_number_used_cores(job, walltime):
    """
    Set the number of cores used by the payload.
    The number of actual used cores is reported with job metrics (if set).
    The walltime can be used to estimate the number of used cores in combination with memory monitor output,
    (utime+stime)/walltime. If memory momitor information is not available, a ps command is used (not reliable for
    multi-core jobs).

    :param job: job object.
    :param walltime: wall time for payload in seconds (int).
    :return:
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    cpu = __import__('pilot.user.%s.cpu' % pilot_user, globals(), locals(), [pilot_user], 0)

    kwargs = {'job': job, 'walltime': walltime}
    cpu.set_core_counts(**kwargs)


def verify_memory_usage(current_time, mt, job, debug=False):
    """
    Verify the memory usage (optional).
    Note: this function relies on a stand-alone memory monitor tool that may be executed by the Pilot.

    :param current_time: current time at the start of the monitoring loop (int)
    :param mt: measured time object
    :param job: job object
    :param debug: True for args.debug==True (Boolean)
    :return: exit code (int), error diagnostics (string).
    """

    #if debug:
    #    show_memory_usage()

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    memory = __import__('pilot.user.%s.memory' % pilot_user, globals(), locals(), [pilot_user], 0)

    if not memory.allow_memory_usage_verifications():
        return 0, ""

    # is it time to verify the memory usage?
    memory_verification_time = convert_to_int(config.Pilot.memory_usage_verification_time, default=60)
    if current_time - mt.get('ct_memory') > memory_verification_time:
        # is the used memory within the allowed limit?
        try:
            exit_code, diagnostics = memory.memory_usage(job)
        except Exception as error:
            logger.warning(f'caught exception: {error}')
            exit_code = -1
        if exit_code != 0:
            logger.warning('ignoring failure to parse memory monitor output')
            #return exit_code, diagnostics
        else:
            # update the ct_proxy with the current time
            mt.update('ct_memory')

    return 0, ""


def should_abort_payload(current_time, mt):
    """
    Should the pilot abort the payload?
    In the case of Raythena, the Driver is monitoring the time to end jobs and may decide
    that the pilot should abort the payload. Internally, this is achieved by letting the Actors
    know it's time to end, and they in turn contacts the pilot by placing a 'pilot_kill_payload' file
    in the run directory.

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :return: exit code (int), error diagnostics (string).
    """

    # is it time to look for the kill instruction file?
    killing_time = convert_to_int(config.Pilot.kill_instruction_time, default=600)
    if current_time - mt.get('ct_kill') > killing_time:
        path = os.path.join(os.environ.get('PILOT_HOME'), config.Pilot.kill_instruction_filename)
        if os.path.exists(path):
            logger.info('pilot encountered payload kill instruction file - will abort payload')
            return errors.KILLPAYLOAD, ""  # note, this is not an error

    return 0, ""


def verify_user_proxy(current_time, mt):
    """
    Verify the user proxy.
    This function is called by the job_monitor_tasks() function.

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :return: exit code (int), error diagnostics (string).
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    userproxy = __import__('pilot.user.%s.proxy' % pilot_user, globals(), locals(), [pilot_user], 0)

    # is it time to verify the proxy?
    # test bad proxy
    #proxy_verification_time = 30  # convert_to_int(config.Pilot.proxy_verification_time, default=600)
    proxy_verification_time = convert_to_int(config.Pilot.proxy_verification_time, default=600)
    if current_time - mt.get('ct_proxy') > proxy_verification_time:
        # is the proxy still valid?
        exit_code, diagnostics = userproxy.verify_proxy(test=False)  # use test=True to test expired proxy
        if exit_code != 0:
            return exit_code, diagnostics
        else:
            # update the ct_proxy with the current time
            mt.update('ct_proxy')

    return 0, ""


def verify_looping_job(current_time, mt, job, args):
    """
    Verify that the job is not looping.

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :param job: job object.
    :param args: pilot args object.
    :return: exit code (int), error diagnostics (string).
    """

    # only perform looping job check if desired and enough time has passed since start
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    loopingjob_definitions = __import__('pilot.user.%s.loopingjob_definitions' % pilot_user, globals(), locals(), [pilot_user], 0)

    runcheck = loopingjob_definitions.allow_loopingjob_detection()
    if not job.looping_check and runcheck:
        logger.debug('looping check not desired')
        return 0, ""

    time_since_start = get_time_since(job.jobid, PILOT_PRE_PAYLOAD, args)  # payload walltime
    looping_verification_time = convert_to_int(config.Pilot.looping_verification_time, default=600)

    if time_since_start < looping_verification_time:
        logger.debug(f'no point in running looping job algorithm since time since last payload start={time_since_start} s < '
                     f'looping verification time={looping_verification_time} s')
        return 0, ""

    if current_time - mt.get('ct_looping') > looping_verification_time:

        # remove any lingering defunct processes
        try:
            reap_zombies()
        except Exception as exc:
            logger.warning(f'reap_zombies threw an exception: {exc}')

        # is the job looping?
        try:
            exit_code, diagnostics = looping_job(job, mt)
        except Exception as error:
            diagnostics = f'exception caught in looping job algorithm: {error}'
            logger.warning(diagnostics)
            if "No module named" in diagnostics:
                exit_code = errors.BLACKHOLE
            else:
                exit_code = errors.UNKNOWNEXCEPTION
            return exit_code, diagnostics
        else:
            if exit_code != 0:
                return exit_code, diagnostics

        # update the ct_proxy with the current time
        mt.update('ct_looping')

    return 0, ""


def check_lease_time(current_time, mt, leasetime):
    """
    Check the lease time in stager mode.

    :param current_time: current time at the start of the monitoring loop (int)
    :param mt: measured time object
    :param leasetime: lease time in seconds (int)
    :return: exit code (int), error diagnostics (string).
    """

    exit_code = 0
    diagnostics = ''
    if current_time - mt.get('ct_lease') > 10:
        # time to check the lease time

        logger.debug(f'checking lease time (lease time={leasetime})')
        if current_time - mt.get('ct_start') > leasetime:
            diagnostics = f"lease time is up: {current_time - mt.get('ct_start')} s has passed since start - abort stager pilot"
            logger.warning(diagnostics)
            exit_code = errors.LEASETIME

        # update the ct_lease with the current time
        mt.update('ct_lease')

    return exit_code, diagnostics


def verify_disk_usage(current_time, mt, job):
    """
    Verify the disk usage.
    The function checks 1) payload stdout size, 2) local space, 3) work directory size, 4) output file sizes.

    :param current_time: current time at the start of the monitoring loop (int)
    :param mt: measured time object
    :param job: job object
    :return: exit code (int), error diagnostics (string).
    """

    disk_space_verification_time = convert_to_int(config.Pilot.disk_space_verification_time, default=300)
    if current_time - mt.get('ct_diskspace') > disk_space_verification_time:
        # time to check the disk space

        # check the size of the payload stdout
        try:
            exit_code, diagnostics = check_payload_stdout(job)
        except Exception as exc:
            logger.warning(f'caught exception: {exc}')
        else:
            if exit_code != 0:
                return exit_code, diagnostics

        # check the local space, if it's enough left to keep running the job
        exit_code, diagnostics = check_local_space(initial=False)
        if exit_code != 0:
            return exit_code, diagnostics

        # check the size of the workdir
        exit_code, diagnostics = check_work_dir(job)
        if exit_code != 0:
            return exit_code, diagnostics

        # check the output file sizes
        exit_code, diagnostics = check_output_file_sizes(job)
        if exit_code != 0:
            return exit_code, diagnostics

        # update the ct_diskspace with the current time
        mt.update('ct_diskspace')

    return 0, ""


def verify_running_processes(current_time, mt, pid):
    """
    Verify the number of running processes.
    The function sets the environmental variable PILOT_MAXNPROC to the maximum number of found (child) processes
    corresponding to the main payload process id.
    The function does not return an error code (always returns exit code 0).

    :param current_time: current time at the start of the monitoring loop (int).
    :param mt: measured time object.
    :param pid: payload process id (int).
    :return: exit code (int), error diagnostics (string).
    """

    nproc_env = 0

    process_verification_time = convert_to_int(config.Pilot.process_verification_time, default=300)
    if current_time - mt.get('ct_process') > process_verification_time:
        # time to check the number of processes
        nproc = get_number_of_child_processes(pid)
        try:
            nproc_env = int(os.environ.get('PILOT_MAXNPROC', 0))
        except Exception as error:
            logger.warning(f'failed to convert PILOT_MAXNPROC to int: {error}')
        else:
            if nproc > nproc_env:
                # set the maximum number of found processes
                os.environ['PILOT_MAXNPROC'] = str(nproc)

        if nproc_env > 0:
            logger.info(f'maximum number of monitored processes: {nproc_env}')

    return 0, ""


def utility_monitor(job):  # noqa: C901
    """
    Make sure that any utility commands are still running.
    In case a utility tool has crashed, this function may restart the process.
    The function is used by the job monitor thread.

    :param job: job object.
    :return:
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    usercommon = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)

    # loop over all utilities
    for utcmd in list(job.utilities.keys()):  # E.g. utcmd = MemoryMonitor

        utproc = job.utilities[utcmd][0]

        if utcmd == 'MemoryMonitor':
            if len(job.utilities[utcmd]) < 4:  # only proceed if the pid has not been appended to the list already
                pid = get_pid(job.pid)
                if pid:
                    logger.info(f'memory monitor command has pid {pid} (appending to cmd dictionary)')
                    job.utilities[utcmd].append(pid)
                else:
                    logger.info('could not find any pid for memory monitor command')

        # make sure the subprocess is still running
        if not utproc.poll() is None:

            # clean up the process
            kill_process(utproc)

            if job.state == 'finished' or job.state == 'failed' or job.state == 'stageout':
                logger.debug('no need to restart utility command since payload has finished running')
                continue

            # if poll() returns anything but None it means that the subprocess has ended - which it
            # should not have done by itself
            utility_subprocess_launches = job.utilities[utcmd][1]
            if utility_subprocess_launches <= 5:
                logger.warning('detected crashed utility subprocess - will restart it')
                utility_command = job.utilities[utcmd][2]

                try:
                    proc1 = execute(utility_command, workdir=job.workdir, returnproc=True, usecontainer=False,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=job.workdir, queuedata=job.infosys.queuedata)
                except Exception as error:
                    logger.error(f'could not execute: {error}')
                else:
                    # store process handle in job object, and keep track on how many times the
                    # command has been launched
                    job.utilities[utcmd] = [proc1, utility_subprocess_launches + 1, utility_command]
            else:
                logger.warning(f'detected crashed utility subprocess - too many restarts, will not restart {utcmd} again')
        else:  # check the utility output (the selector option adds a substring to the output file name)
            filename = usercommon.get_utility_command_output_filename(utcmd, selector=True)
            path = os.path.join(job.workdir, filename)
            if not os.path.exists(path):
                logger.warning(f'file: {path} does not exist')

            time.sleep(10)


def kill_process(process: Any):
    """
    Kill process before restart to get rid of defunct processes.

    :param process: process object
    """

    diagnostics = ''
    try:
        logger.warning('killing lingering subprocess')
        process.kill()
    except ProcessLookupError as exc:
        diagnostics += f'\n(kill process group) ProcessLookupError={exc}'
    try:
        logger.warning('killing lingering process')
        os.kill(process.pid, SIGKILL)
    except ProcessLookupError as exc:
        diagnostics += f'\n(kill process) ProcessLookupError={exc}'
    logger.warning(f'sent hard kill signal - final stderr: {diagnostics}')


def get_local_size_limit_stdout(bytes=True):
    """
    Return a proper value for the local size limit for payload stdout (from config file).

    :param bytes: boolean (if True, convert kB to Bytes).
    :return: size limit (int).
    """

    try:
        localsizelimit_stdout = int(config.Pilot.local_size_limit_stdout)
    except Exception as error:
        localsizelimit_stdout = 2097152
        logger.warning(f'bad value in config for local_size_limit_stdout: {error} (will use value: {localsizelimit_stdout} kB)')

    # convert from kB to B
    if bytes:
        localsizelimit_stdout *= 1024

    return localsizelimit_stdout


def check_payload_stdout(job):
    """
    Check the size of the payload stdout.

    :param job: job object.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    # get list of log files
    file_list = glob(os.path.join(job.workdir, 'log.*'))

    # is this a multi-trf job?
    n_jobs = job.jobparams.count("\n") + 1
    for _i in range(n_jobs):
        # get name of payload stdout file created by the pilot
        _stdout = config.Payload.payloadstdout
        if n_jobs > 1:
            _stdout = _stdout.replace(".txt", "_%d.txt" % (_i + 1))

        # add the primary stdout file to the fileList
        file_list.append(os.path.join(job.workdir, _stdout))

    tmp_list = glob(os.path.join(job.workdir, 'workDir/tmp.stdout.*'))
    if tmp_list:
        file_list += tmp_list
    logger.debug(f'file list={file_list}')

    # now loop over all files and check each individually (any large enough file will fail the job)
    to_be_zipped = []
    for filename in file_list:

        if "job.log.tgz" in filename:
            logger.debug(f"skipping file size check of file ({filename}) since it is a special log file")
            continue

        if os.path.exists(filename):
            _exit_code, to_be_zipped = check_log_size(filename, to_be_zipped=to_be_zipped)
            if _exit_code:  # do not break loop so that other logs can get zipped if necessary
                exit_code = _exit_code
        else:
            logger.info(f"skipping file size check of payload stdout file ({filename}) since it has not been created yet")

    if exit_code:
        # remove any lingering input files from the work dir
        lfns, guids = job.get_lfns_and_guids()
        if lfns:
            # remove any lingering input files from the work dir
            remove_files(lfns, workdir=job.workdir)

    if to_be_zipped:
        logger.warning(f'the following files will be zipped: {to_be_zipped}')
        archivename = os.path.join(job.workdir, 'oversized_files.zip')
        status = zip_files(archivename, to_be_zipped)
        if status:
            logger.info(f'created archive {archivename}')
            # verify that the new file size is not too big (ignore exit code, should already be set above)
            _exit_code, _ = check_log_size(archivename, to_be_zipped=None, archive=True)
            if _exit_code:
                logger.warning('also the archive was too large - will be removed')
                remove_files([archivename])

        # remove logs
        remove_files(to_be_zipped)

        # kill the job
        set_pilot_state(job=job, state="failed")
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)
        kill_processes(job.pid)  # will not return

    return exit_code, diagnostics


def check_log_size(filename, to_be_zipped=None, archive=False):
    """
    Check the payload log file size.
    The log will be added to the list of files to be zipped, if too large.

    :param filename: file path (string)
    :param to_be_zipped: list of files to be zipped
    :param archive: is this file an archive? (boolean)
    :return: exit code (int), to_be_zipped (list)
    """

    exit_code = 0

    try:
        # get file size in bytes
        fsize = os.path.getsize(filename)
    except Exception as error:
        logger.warning(f"could not read file size of {filename}: {error}")
    else:
        # is the file too big?
        localsizelimit_stdout = get_local_size_limit_stdout()
        if fsize > localsizelimit_stdout:
            exit_code = errors.STDOUTTOOBIG
            label = 'archive' if archive else 'log file'
            diagnostics = f"{label} {filename} is too big: {fsize} B (larger than limit {localsizelimit_stdout} B) [will be zipped]"
            logger.warning(diagnostics)
            if to_be_zipped is not None:
                to_be_zipped.append(filename)
        else:
            logger.info(
                f"payload log ({os.path.basename(filename)}) within allowed size limit ({localsizelimit_stdout} B): {fsize} B")

    return exit_code, to_be_zipped


def check_local_space(initial=True):
    """
    Do we have enough local disk space left to run the job?
    For the initial local space check, the Pilot will require 2 GB of free space, but during running
    this can be lowered to 1 GB.

    :param initial: True means a 2 GB limit, False means a 1 GB limit (optional Boolean)
    :return: pilot error code (0 if success, NOLOCALSPACE if failure)
    """

    ec = 0
    diagnostics = ""

    # is there enough local space to run a job?
    cwd = os.getcwd()
    logger.debug(f'checking local space on {cwd}')
    try:
        local_space = get_local_disk_space(cwd)
    except PilotException as exc:
        diagnostics = exc.get_detail()
        logger.warning(f'exception caught while executing df: {diagnostics} (ignoring)')
        return ec, diagnostics

    if local_space:
        spaceleft = convert_mb_to_b(local_space)  # B (diskspace is in MB)
        free_space_limit = human2bytes(config.Pilot.free_space_limit) if initial else human2bytes(config.Pilot.free_space_limit_running)

        if spaceleft <= free_space_limit:
            diagnostics = f'too little space left on local disk to run job: {spaceleft} B (need > {free_space_limit} B)'
            ec = errors.NOLOCALSPACE
            logger.warning(diagnostics)
        else:
            logger.info(f'sufficient remaining disk space ({spaceleft} B)')
    else:
        diagnostics = 'get_local_disk_space() returned None'
        logger.warning(diagnostics)

    return ec, diagnostics


def check_work_dir(job):
    """
    Check the size of the work directory.
    The function also updates the workdirsizes list in the job object.

    :param job: job object.
    :return: exit code (int), error diagnostics (string)
    """

    exit_code = 0
    diagnostics = ""

    if os.path.exists(job.workdir):
        # get the limit of the workdir
        maxwdirsize = get_max_allowed_work_dir_size()

        if os.path.exists(job.workdir):
            workdirsize = get_disk_usage(job.workdir)

            # is user dir within allowed size limit?
            if workdirsize > maxwdirsize:
                exit_code = errors.USERDIRTOOLARGE
                diagnostics = f'work directory ({job.workdir}) is too large: {workdirsize} B (must be < {maxwdirsize} B)'
                logger.fatal(diagnostics)

                cmd = 'ls -altrR %s' % job.workdir
                _ec, stdout, stderr = execute(cmd, mute=True)
                logger.info(f'{cmd}:\n{stdout}')

                # kill the job
                set_pilot_state(job=job, state="failed")
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)
                kill_processes(job.pid)

                # remove any lingering input files from the work dir
                lfns, guids = job.get_lfns_and_guids()
                if lfns:
                    remove_files(lfns, workdir=job.workdir)

                    # remeasure the size of the workdir at this point since the value is stored below
                    workdirsize = get_disk_usage(job.workdir)
            else:
                logger.info(f'size of work directory {job.workdir}: {workdirsize} B (within {maxwdirsize} B limit)')

            # Store the measured disk space (the max value will later be sent with the job metrics)
            if workdirsize > 0:
                job.add_workdir_size(workdirsize)
        else:
            logger.warning(f'job work dir does not exist: {job.workdir}')
    else:
        logger.warning('skipping size check of workdir since it has not been created yet')

    return exit_code, diagnostics


def get_max_allowed_work_dir_size():
    """
    Return the maximum allowed size of the work directory.
    Note: input sizes need not be added to [..] when copytool=mv (ie on storm/NDGF).

    :return: max allowed work dir size in Bytes (int).
    """

    try:
        maxwdirsize = convert_mb_to_b(get_maximum_input_sizes())  # from MB to B, e.g. 16336 MB -> 17,129,537,536 B
    except Exception as error:
        max_input_size = get_max_input_size()
        maxwdirsize = max_input_size + config.Pilot.local_size_limit_stdout * 1024
        logger.info(f"work directory size check will use {maxwdirsize} B as a max limit (maxinputsize [{max_input_size}"
                    f"B] + local size limit for stdout [{config.Pilot.local_size_limit_stdout * 1024} B])")
        logger.warning(f'conversion caught exception: {error}')
    else:
        # grace margin, as discussed in https://its.cern.ch/jira/browse/ATLASPANDA-482
        margin = 10.0  # percent, read later from somewhere
        maxwdirsize = int(maxwdirsize * (1 + margin / 100.0))
        logger.info(f"work directory size check will use {maxwdirsize} B as a max limit (10% grace limit added)")

    return maxwdirsize


def get_max_input_size(megabyte=False):
    """
    Return a proper maxinputsize value.

    :param megabyte: return results in MB (Boolean).
    :return: max input size (int).
    """

    _maxinputsize = infosys.queuedata.maxwdir  # normally 14336+2000 MB
    max_input_file_sizes = 14 * 1024 * 1024 * 1024  # 14 GB, 14336 MB (pilot default)
    max_input_file_sizes_mb = 14 * 1024  # 14336 MB (pilot default)
    if _maxinputsize != "":
        try:
            if megabyte:  # convert to MB int
                _maxinputsize = int(_maxinputsize)  # MB
            else:  # convert to B int
                _maxinputsize = int(_maxinputsize) * 1024 * 1024  # MB -> B
        except Exception as error:
            logger.warning(f"schedconfig.maxinputsize: {error}")
            if megabyte:
                _maxinputsize = max_input_file_sizes_mb
            else:
                _maxinputsize = max_input_file_sizes
    else:
        if megabyte:
            _maxinputsize = max_input_file_sizes_mb
        else:
            _maxinputsize = max_input_file_sizes

    _label = 'MB' if megabyte else 'B'
    logger.info(f"max input size = {_maxinputsize} {_label} (pilot default)")

    return _maxinputsize


def check_output_file_sizes(job):
    """
    Are the output files within the allowed size limits?

    :param job: job object.
    :return: exit code (int), error diagnostics (string)
    """

    exit_code = 0
    diagnostics = ""

    # loop over all known output files
    for fspec in job.outdata:
        path = os.path.join(job.workdir, fspec.lfn)
        if os.path.exists(path):
            # get the current file size
            fsize = get_local_file_size(path)
            max_fsize = human2bytes(config.Pilot.maximum_output_file_size)
            if fsize and fsize < max_fsize:
                logger.info(f'output file {path} is within allowed size limit ({fsize} B < {max_fsize} B)')
            elif fsize == 0:
                exit_code = errors.EMPTYOUTPUTFILE
                diagnostics = f'zero size output file detected: {path}'
                logger.warning(diagnostics)
            else:
                exit_code = errors.OUTPUTFILETOOLARGE
                diagnostics = f'output file {path} is not within allowed size limit ({fsize} B > {max_fsize} B)'
                logger.warning(diagnostics)
        else:
            logger.info(f'output file size check: skipping output file {path} since it does not exist')

    return exit_code, diagnostics


def store_subprocess_pids(job):
    """
    Keep track of all running subprocesses.

    :param job: job object.
    :return:
    """

    # only store the pid once
    if job.subprocesses:
        return

    # is the payload running?
    if job.pid:
        # get all subprocesses
        _subprocesses = get_subprocesses(job.pid)
        # merge lists without duplicates
        job.subprocesses = list(set(job.subprocesses + _subprocesses))
        logger.debug(f'payload subprocesses: {job.subprocesses}')
    else:
        logger.debug('payload not running (no subprocesses)')
