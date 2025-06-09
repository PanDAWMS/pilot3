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
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2024
# - Wen Guan, wen.guan@cern.ch, 2017-2018

"""Functions for handling the payload."""

import logging
import os
import time
import traceback
import queue
from collections import namedtuple
from re import (
    findall,
    split,
    search
)
from typing import (
    Any,
    TextIO
)

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    ExcThread,
    PilotException
)
from pilot.control.payloads import (
    eventservice,
    eventservicemerge,
    generic,
)
from pilot.control.job import send_state
from pilot.info import JobData
from pilot.util.auxiliary import set_pilot_state
from pilot.util.container import execute
from pilot.util.config import config
from pilot.util.filehandling import (
    extract_lines_from_file,
    find_file,
    get_guid,
    read_file,
    read_json,
    remove_core_dumps
)
from pilot.util.processes import (
    get_cpu_consumption_time,
    threads_aborted
)
from pilot.util.queuehandling import put_in_queue
from pilot.util.realtimelogger import get_realtime_logger

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def control(queues: namedtuple, traces: Any, args: object):
    """
    Set up payload threads.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    targets = {'validate_pre': validate_pre, 'execute_payloads': execute_payloads, 'validate_post': validate_post,
               'failed_post': failed_post, 'run_realtimelog': run_realtimelog}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in list(targets.items())]

    _ = [thread.start() for thread in threads]

    # if an exception is thrown, the graceful_stop will be set by the ExcThread class run() function
    try:
        while not args.graceful_stop.is_set():
            for thread in threads:
                bucket = thread.get_bucket()
                try:
                    exc = bucket.get(block=False)
                except queue.Empty:
                    pass
                else:
                    # exc_type, exc_obj, exc_trace = exc
                    _, exc_obj, _ = exc
                    logger.warning(f"thread \'{thread.name}\' received an exception from bucket: {exc_obj}")

                    # deal with the exception
                    # ..

                thread.join(0.1)
                time.sleep(0.1)

            time.sleep(0.5)
    except Exception as exc:
        logger.warning(f"exception caught while handling threads: {exc}")
    finally:
        logger.info('all payload control threads have been joined')

    logger.debug('payload control ending since graceful_stop has been set')
    if args.abort_job.is_set():
        if traces.pilot['command'] == 'aborting':
            logger.warning('jobs are aborting')
        elif traces.pilot['command'] == 'abort':
            logger.warning('data control detected a set abort_job (due to a kill signal)')
            traces.pilot['command'] = 'aborting'

            # find all running jobs and stop them, find all jobs in queues relevant to this module
            #abort_jobs_in_queues(queues, args.signal)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='control'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[payload] control thread has finished')


def validate_pre(queues: namedtuple, traces: Any, args: object):
    """
    Get a Job object from the "payloads" queue and validate it.

    Thread.

    If the payload is successfully validated (user defined), the Job object is placed in the "validated_payloads" queue,
    otherwise it is placed in the "failed_payloads" queue.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.payloads.get(block=True, timeout=1)
        except queue.Empty:
            continue

        if _validate_payload(job):
            put_in_queue(job, queues.realtimelog_payloads)
            put_in_queue(job, queues.validated_payloads)
        else:
            put_in_queue(job, queues.failed_payloads)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='validate_pre'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[payload] validate_pre thread has finished')


def _validate_payload(job: JobData) -> bool:
    """
    Perform user validation tests for the payload.

    :param job: job object (JobData)
    :return: boolean (bool).
    """
    status = True

    # perform user specific validation
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
    try:
        status = user.validate(job)
    except Exception as error:
        logger.fatal(f'failed to execute user validate() function: {error}')
        status = False

    return status


def get_payload_executor(args: object, job: JobData, out: TextIO, err: TextIO, traces: Any) -> Any:
    """
    Get payload executor function for different payload.

    :param args: Pilot arguments object (object)
    :param job: job object (JobData)
    :param out: stdout file object (TextIO)
    :param err: stderr file object (TextIO)
    :param traces: traces object (Any)
    :return: instance of a payload executor (Any).
    """
    if job.is_eventservice:  # True for native HPO workflow as well
        payload_executor = eventservice.Executor(args, job, out, err, traces)
    elif job.is_eventservicemerge:
        payload_executor = eventservicemerge.Executor(args, job, out, err, traces)
    else:
        payload_executor = generic.Executor(args, job, out, err, traces)

    return payload_executor


def execute_payloads(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Execute queued payloads.

    Extract a Job object from the "validated_payloads" queue and put it in the "monitored_jobs" queue. The payload
    stdout/err streams are opened and the pilot state is changed to "starting". A payload executor is selected (for
    executing a normal job, an event service job or event service merge job). After the payload (or rather its executor)
    is started, the thread will wait for it to finish and then check for any failures. A successfully completed job is
    placed in the "finished_payloads" queue, and a failed job will be placed in the "failed_payloads" queue.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    job = None
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.validated_payloads.get(block=True, timeout=1)
            q_snapshot = list(queues.finished_data_in.queue)
            peek = [s_job for s_job in q_snapshot if job.jobid == s_job.jobid]
            if len(peek) == 0:
                put_in_queue(job, queues.validated_payloads)
                for _ in range(10):
                    if args.graceful_stop.is_set():
                        break
                    time.sleep(1)
                continue

            # this job is now to be monitored, so add it to the monitored_payloads queue
            put_in_queue(job, queues.monitored_payloads)

            logger.debug(f'job {job.jobid} added to monitored payloads queue')

            try:
                if job.is_eventservice or job.is_eventservicemerge:
                    out = open(os.path.join(job.workdir, config.Payload.payloadstdout), 'ab')
                    err = open(os.path.join(job.workdir, config.Payload.payloadstderr), 'ab')
                else:
                    out = open(os.path.join(job.workdir, config.Payload.payloadstdout), 'wb')
                    err = open(os.path.join(job.workdir, config.Payload.payloadstderr), 'wb')
            except Exception as error:
                logger.warning(f'failed to open payload stdout/err: {error}')
                out = None
                err = None
            send_state(job, args, 'starting')

            # note: when sending a state change to the server, the server might respond with 'tobekilled'
            if job.state == 'failed':
                logger.warning('job state is \'failed\' - abort execute_payloads()')
                set_pilot_state(job=job, state="failed")
                continue

            payload_executor = get_payload_executor(args, job, out, err, traces)
            logger.info(f"will use payload executor: {payload_executor}")

            # run the payload and measure the execution time
            job.t0 = os.times()
            exit_code, diagnostics = payload_executor.run()
            if exit_code and exit_code > 1000:  # pilot error code, add to list
                logger.warning(f'pilot error code received (code={exit_code}, diagnostics=\n{diagnostics})')
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code, msg=diagnostics)
            else:
                if exit_code >= 0:
                    job.transexitcode = exit_code % 255
                else:
                    logger.warning(f'pilot error code received a negative transform exit code={exit_code} - will not set transexitcode')

            logger.debug(f'run() returned exit_code={exit_code}')
            set_cpu_consumption_time(job)
            if out:
                out.close()
            if err:
                err.close()

            # some HPO jobs will produce new output files (following lfn name pattern), discover those and replace the job.outdata list
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            if job.is_hpo:
                user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
                try:
                    user.update_output_for_hpo(job)
                except Exception as error:
                    logger.warning(f'exception caught by update_output_for_hpo(): {error}')
                else:
                    for dat in job.outdata:
                        if not dat.guid:
                            dat.guid = get_guid()
                            logger.warning(f'guid not set: generated guid={dat.guid} for lfn={dat.lfn}')

            #if traces.pilot['nr_jobs'] == 1:
            #    logger.debug('faking job failure in first multi-job')
            #    job.transexitcode = 1
            #    exit_code = 1

            # analyze and interpret the payload execution output
            perform_initial_payload_error_analysis(job, exit_code)

            # was an error already found?
            user = __import__(f'pilot.user.{pilot_user}.diagnose', globals(), locals(), [pilot_user], 0)
            try:
                exit_code_interpret = user.interpret(job)
            except (Exception, PilotException) as error:
                logger.warning(f'exception caught: {error}')
                if isinstance(error, PilotException):
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error._error_code, msg=error._message)
                elif isinstance(error, str):
                    if 'error code:' in error and 'message:' in error:
                        error_code, diagnostics = extract_error_info(error)
                        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code, msg=diagnostics)
                    else:
                        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.INTERNALPILOTPROBLEM, msg=error)
                else:
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.INTERNALPILOTPROBLEM, msg=error)

            if exit_code < 0 and not job.piloterrorcodes:
                # exit code < 0 means that the payload was killed, e.g. by a signal
                logger.warning("it seems the payload was killed but no error code was assigned yet - setting SIGTERM error")
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.SIGTERM)

            if job.piloterrorcodes:
                exit_code_interpret = 1

            if exit_code_interpret == 0 and exit_code == 0:
                logger.info('main payload error analysis completed - did not find any errors')

                # update output lists if zipmaps were used
                #job.add_archives_to_output_lists()

                # queues.finished_payloads.put(job)
                put_in_queue(job, queues.finished_payloads)
            else:
                logger.debug('main payload error analysis completed - adding job to failed_payloads queue')
                #queues.failed_payloads.put(job)
                put_in_queue(job, queues.failed_payloads)

        except queue.Empty:
            continue
        except Exception as error:
            logger.fatal(f'execute payloads caught an exception (cannot recover): {error}, {traceback.format_exc()}')
            if job:
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXECUTIONEXCEPTION)
                put_in_queue(job, queues.failed_payloads)
            while not args.graceful_stop.is_set():
                # let stage-out of log finish, but stop running payloads as there should be a problem with the pilot
                time.sleep(5)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='execute_payloads'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[payload] execute_payloads thread has finished')


def extract_error_info(error: str) -> (int, str):
    """
    Extract the error code and diagnostics from an error exception.

    :param error: exception string (str)
    :return: error code (int), diagnostics (str).
    """
    error_code = errors.INTERNALPILOTPROBLEM
    diagnostics = f'full exception: {error}'

    pattern = r'error\ code\:\ ([0-9]+)\,\ message\:\ (.+)'
    errinfo = findall(pattern, error)
    if errinfo:  # e.g. [('1303', 'Failed during file handling')]
        try:
            error_code = errinfo[0][0]
            diagnostics = errinfo[0][1]
        except Exception as exc:
            logger.warning(f'failed to extract error info from exception error={error}, exc={exc}')

    return error_code, diagnostics


def get_rtlogging(catchall: str) -> str:
    """
    Return the proper rtlogging value from PQ.catchall, the experiment specific plug-in or the config file.

    :param catchall: catchall field from queuedata (str)
    :return: rtlogging (str).
    """
    if catchall:
        _rtlogging = findall(r'logging=([^,]+)', catchall)
        if _rtlogging and ";" in _rtlogging[0]:
            logger.info(f"found rtlogging in catchall: {_rtlogging[0]}")
            return _rtlogging[0]

    rtlogging = None

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    try:
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
        rtlogging = user.get_rtlogging()
    except Exception as exc:
        rtlogging = config.Pilot.rtlogging
        logger.warning(f'found no experiment specific rtlogging, using config value ({rtlogging}): {exc}')

    return rtlogging


def get_logging_info(job: JobData, args: object) -> dict:
    """
    Extract the logging type/protocol/url/port from catchall if present, or from args fields.

    Returns a dictionary with the format: {'logging_type': .., 'protocol': .., 'url': .., 'port': .., 'logname': ..}

    If the provided debug_command contains a tail instruction ('tail log_file_name'), the pilot will locate
    the log file and use that for RT logging (full path).

    Note: the returned dictionary can be built with either args (has priority) or catchall info.

    :param job: job object (JobData)
    :param args: Pilot arguments object (object)
    :return: info dictionary (logging_type (string), protocol (string), url (string), port (int)) (dict).
    """
    info_dic = {}

    if not job.realtimelogging and "loggingfile" not in job.infosys.queuedata.catchall:
        logger.info("job.realtimelogging is not enabled")
        return {}

    # args handling
    info_dic['logname'] = args.realtime_logname if args.realtime_logname else "pilot-log"
    logserver = args.realtime_logging_server if args.realtime_logging_server else ""

    try:
        catchall = job.infosys.queuedata.catchall
    except Exception as exc:
        logger.warning(f'exception caught: {exc}')
        catchall = ""

    info = findall(r'(\S+)\;(\S+)\:\/\/(\S+)\:(\d+)', get_rtlogging(catchall))
    if not logserver and not info:
        logger.warning(f"not enough info available for activating real-time logging (info='{info}', logserver='{logserver}')")
        return {}

    if len(logserver) > 0:
        if ';' not in logserver:
            logger.warning(f'wrong format of logserver: does not contain a \';\' character: {logserver}')
            logger.info("correct logserver formal: logging_type;protocol://hostname:port")
            return {}

        regex = r"logserver=(?P<logging_type>[^;]+);(?P<protocol>[^:]+)://(?P<hostname>[^:]+):(?P<port>\d+)"
        match = search(regex, logserver)
        if match:
            logging_type = match.group('logging_type')
            protocol = match.group('protocol')
            hostname = match.group('hostname')
            port = match.group('port')

            # Print the extracted values
            logger.debug(f"extracted logging_type='{logging_type}', protocol='{protocol}', hostname='{hostname}',"
                         f"port='{port}' from logserver='{logserver}'")

            info_dic['logging_type'] = logging_type
            info_dic['protocol'] = protocol
            info_dic['url'] = hostname
            info_dic['port'] = port
        else:
            logger.warning(f"no match found in logserver='{logserver}' for pattern=r'{regex}'")
            return {}
    elif info:
        try:
            info_dic['logging_type'] = info[0][0]
            info_dic['protocol'] = info[0][1]
            info_dic['url'] = info[0][2]
            info_dic['port'] = info[0][3]
        except IndexError as exc:
            logger.warning(f'exception caught: {exc}')
            return {}

        # find the log file to tail
        path = find_log_to_tail(job.debug_command, job.workdir, args, job.is_analysis(), job.infosys.queuedata.catchall)
        logger.info(f'using {path} for real-time logging')
        info_dic['logfiles'] = [path]

    if 'logfiles' not in info_dic:
        # Rubin
        logfiles = os.environ.get('REALTIME_LOGFILES', None)
        if logfiles is not None:
            info_dic['logfiles'] = split('[:,]', logfiles)

    return info_dic


def get_catchall_loggingfile(catchall: str) -> str:
    """
    Extract the logging file from the catchall field if present.

    :param catchall: catchall field from queuedata (str)
    :return: logging file name (str).
    """
    filename = ""
    if catchall and "loggingfile" in catchall:
        _filename = findall(r'loggingfile=([^,]+)', catchall)
        if _filename:
            filename = _filename[0]
            logger.debug(f'found filename in catchall: {filename}')

    return filename


def find_log_to_tail(debug_command: str, workdir: str, args: object, is_analysis: bool, catchall: str) -> str:
    """
    Find the log file to tail in the RT logging.

    :param debug_command: requested debug command (str)
    :param workdir: job working directory (str)
    :param args: Pilot arguments object (object)
    :param is_analysis: True for user jobs, False otherwise (bool)
    :param catchall: catchall field from queuedata (str)
    :return: path to log file (str).
    """
    path = ""
    filename = ""
    counter = 0
    maxwait = 5 * 60

    # get filename from env or from catchall if present
    filename_env = os.environ.get('REALTIME_LOGFILE', None)
    filename_catchall = get_catchall_loggingfile(catchall) if not filename_env else None

    # .. otherwise get it from the debug command or use default for analysis jobs
    if 'tail' in debug_command:
        filename = debug_command.split(' ')[-1]
    elif is_analysis and not filename_env and not filename_catchall:
        filename = 'tmp.stdout*'

    if filename:
        logger.debug(f'filename={filename}')
        while counter < maxwait and not args.graceful_stop.is_set():
            path = find_file(filename, workdir)
            if not path:
                logger.debug(f'file {filename} not found, waiting for max {maxwait} s')
                time.sleep(10)
            else:
                break
            counter += 10

    if not path and filename_env:
        # extract the path from the env variable
        path = filename_env

    if not path and filename_catchall:
        # extract the path from the catchall "..,loggingfile=path,.."
        path = filename_catchall

    # fallback to known log file if no other file could be found
    logf = path if path else config.Payload.payloadstdout
    if not path and filename:
        logger.warning(f'file {filename} was not found for {maxwait} s, using default')

    return logf


def run_realtimelog(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Validate finished payloads.

    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out (in failed_post() below).

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    info_dic = None
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.realtimelog_payloads.get(block=True, timeout=1)
        except queue.Empty:
            continue

        # wait with proceeding until the job is running
        abort_loops = False
        first1 = True
        first2 = True
        gotonextjob = False
        while not args.graceful_stop.is_set():

            # note: in multi-job mode, the real-time logging will be switched off at the end of the job
            while not args.graceful_stop.is_set():
                if job.state == 'running':
                    if first1:
                        logger.debug(f'job is running, check if real-time logger is needed '
                                     f'(job.debug={job.debug}, job.debug_command={job.debug_command})')
                        first1 = False
                    break
                if job.state in {'stageout', 'failed', 'holding'}:
                    if first2:
                        logger.debug(f'job is in state {job.state}, continue to next job or abort (wait for graceful stop)')
                        first1 = True
                        break
                    time.sleep(10)
                    continue
                time.sleep(1)

            if first1 and first2:
                logger.debug('continue to next job (1)')
                gotonextjob = True
                break

            if args.use_realtime_logging:
                # always do real-time logging
                job.realtimelogging = True
                job.debug = True
                abort_loops = True
            elif job.debug and \
                    (not job.debug_command or job.debug_command == 'debug' or 'tail' in job.debug_command) and \
                    not args.use_realtime_logging:
                job.realtimelogging = True
                abort_loops = True

            if abort_loops:
                logger.info('time to start real-time logger')
                break
            time.sleep(10)

        if gotonextjob:
            logger.debug('continue to next job (2)')
            gotonextjob = False
            continue

        # only set info_dic once per job (the info will not change)
        info_dic = get_logging_info(job, args)
        if info_dic:
            args.use_realtime_logging = True
            logger.debug("Going to get realtime_logger")
            realtime_logger = get_realtime_logger(args, info_dic, job.workdir, job.pilotsecrets)
        else:
            logger.debug('real-time logging not needed at this point')
            realtime_logger = None

        # If no realtime logger is found, do nothing and exit
        if realtime_logger is None:
            logger.debug('realtime logger was not found, waiting ..')
            continue

        logger.debug('realtime logger was found')
        realtime_logger.sending_logs(args, job)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='run_realtimelog'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[payload] run_realtimelog thread has finished')


def set_cpu_consumption_time(job: JobData):
    """
    Set the CPU consumption time.

    :param job: job object (JobData).
    """
    cpuconsumptiontime = get_cpu_consumption_time(job.t0)
    job.cpuconsumptiontime = int(round(cpuconsumptiontime))
    job.cpuconsumptionunit = "s"
    job.cpuconversionfactor = 1.0
    logger.info(f'CPU consumption time: {cpuconsumptiontime} {job.cpuconsumptionunit} (rounded to {job.cpuconsumptiontime} {job.cpuconsumptionunit})')


def perform_initial_payload_error_analysis(job: JobData, exit_code: int):  # noqa: C901
    """
    Perform an initial analysis of the payload.

    Singularity/apptainer errors are caught here.

    :param job: job object (JobData)
    :param exit_code: exit code from payload execution (int).
    """
    if exit_code != 0:
        logger.warning(f'main payload execution returned non-zero exit code: {exit_code}')
    if exit_code < 0:
        logger.warning("payload was killed (negative exit code)")

    # check if the transform has produced an error report
    path = os.path.join(job.workdir, config.Payload.error_report)
    if os.path.exists(path):
        error_report = read_json(path)
        error_code = error_report.get('error_code')
        error_diag = error_report.get('error_diag')
        if error_code:
            logger.warning(f'{config.Payload.error_report} contained error code: {error_code}')
            logger.warning(f'{config.Payload.error_report} contained error diag: {error_diag}')
            job.exeerrorcode = error_code
            job.exeerrordiag = error_report.get('error_diag')
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXECUTIONFAILURE, msg=error_diag)
            return
        logger.info(f'{config.Payload.error_report} exists but did not contain any non-zero error code')
    else:
        logger.debug(f'{config.Payload.error_report} does not exist')

    # look for singularity/apptainer errors (the exit code can be zero in this case)
    path = os.path.join(job.workdir, config.Payload.payloadstderr)
    if os.path.exists(path):
        stderr = read_file(path)
        _exit_code, error_message = errors.resolve_transform_error(exit_code, stderr)
        if error_message:
            logger.warning(f"found apptainer error in stderr: {error_message}")
            if exit_code == 0 and _exit_code != 0:
                logger.warning("will overwrite trf exit code 0 due to previous error")
        exit_code = _exit_code

    else:
        stderr = ''
        logger.info(f'file does not exist: {path}')

    # check for memory errors first
    if exit_code != 0 and job.subprocesses:
        # scan for memory errors in dmesg messages
        msg = scan_for_memory_errors(job.subprocesses)
        if msg:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADOUTOFMEMORY, msg=msg)
    if exit_code != 0:
        msg = get_critical_error_from_stdout(job.workdir)  # if any

        if stderr != "" and not msg:
            msg = errors.extract_stderr_error(stderr)
            if msg == "":
                # look for warning messages instead (might not be fatal so do not set UNRECOGNIZEDTRFSTDERR)
                msg = errors.extract_stderr_warning(stderr)

        # note: msg should be constructed either from stderr or stdout
        if msg:
            msg = errors.format_diagnostics(exit_code, msg)

        if exit_code < 1000:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXECUTIONFAILURE,
                                                                             msg=msg)
        else:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code, msg=msg)
    else:
        logger.info('main payload execution returned zero exit code')

    # check if core dumps exist, if so remove them
    if not job.debug:  # do not shorten these if-statements
        if remove_core_dumps(job.workdir, pid=job.pid):
            # COREDUMP error will only be set if the core dump belongs to the payload (ie 'core.<payload pid>')
            logger.warning('setting COREDUMP error')
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.COREDUMP)


def get_critical_error_from_stdout(workdir: str) -> str:
    """
    Get any critical error messages from the payload stdout.

    :param workdir: payload work dir (str)
    :return: error message (str).
    """
    msg = ""

    # are there any critical errors in the stdout?
    path = os.path.join(workdir, config.Payload.payloadstdout)
    if os.path.exists(path):
        lines = extract_lines_from_file('CRITICAL', path)
        if lines:
            logger.warning(f'found CRITICAL errors in {config.Payload.payloadstdout}:\n{lines}')
            msg = lines.split('\n')[0]
    else:
        logger.warning('found no payload stdout')

    return msg


def scan_for_memory_errors(subprocesses: list) -> str:
    """
    Scan for memory errors in dmesg messages.

    :param subprocesses: list of payload subprocesses (list)
    :return: error diagnostics (str).
    """
    diagnostics = ""
    search_str = 'Memory cgroup out of memory'
    for pid in subprocesses:
        logger.info(f'scanning dmesg message for subprocess={pid} for memory errors')
        cmd = f'dmesg|grep {pid}'
        _, out, _ = execute(cmd)
        if search_str in out:
            for line in out.split('\n'):
                if search_str in line:
                    diagnostics = line[line.find(search_str):]
                    logger.warning(f'found memory error: {diagnostics}')

                    # make sure that this message is for a true subprocess of the pilot
                    # extract the pid from the message and compare it to the subprocesses list
                    match = search(r'Killed process (\d+)', diagnostics)
                    if match:
                        try:
                            found_pid = int(match.group(1))
                            logger.info(f"extracted PID: {found_pid}")

                            # is it a known subprocess?
                            if found_pid in subprocesses:
                                logger.info("PID found in the list of subprocesses")
                                break
                            else:
                                logger.warning("the extracted PID is not a known subprocess of the payload")
                                diagnostics = ""
                                # is the extracted PID a subprocess of the main pilot process itself?

                        except (ValueError, TypeError, AttributeError) as e:
                            logger.warning(f"failed to extract PID from the message: {e}")
                            diagnostics = ""
                    else:
                        logger.warning("PID could not be extracted from the message")
                        diagnostics = ""

        if diagnostics:
            break

    return diagnostics


def set_error_code_from_stderr(msg: str, fatal: bool) -> int:
    """
    Identify specific errors in stderr and set the corresponding error code.

    The function returns 0 if no error is recognized.

    :param msg: stderr (str)
    :param fatal: boolean flag if fatal error among warning messages in stderr (bool)
    :return: error code (int).
    """
    exit_code = 0
    error_map = {errors.SINGULARITYNEWUSERNAMESPACE: "Failed invoking the NEWUSER namespace runtime",
                 errors.SINGULARITYFAILEDUSERNAMESPACE: "Failed to create user namespace",
                 errors.SINGULARITYRESOURCEUNAVAILABLE: "resource temporarily unavailable",
                 errors.SINGULARITYNOTINSTALLED: "Singularity is not installed",
                 errors.TRANSFORMNOTFOUND: "command not found",
                 errors.UNSUPPORTEDSL5OS: "SL5 is unsupported",
                 errors.UNRECOGNIZEDTRFARGUMENTS: "unrecognized arguments"}

    for key, value in error_map.items():
        if value in msg:
            exit_code = key
            break

    if fatal and not exit_code:
        exit_code = errors.UNRECOGNIZEDTRFSTDERR

    return exit_code


def validate_post(queues: namedtuple, traces: Any, args: object):
    """
    Validate finished payloads.

    Thread.

    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out (in failed_post() below).

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        # finished payloads
        try:
            job = queues.finished_payloads.get(block=True, timeout=1)
        except queue.Empty:
            time.sleep(0.1)
            continue

        # by default, both output and log should be staged out
        job.stageout = 'all'
        logger.debug('adding job to data_out queue')
        #queues.data_out.put(job)
        set_pilot_state(job=job, state='stageout')
        put_in_queue(job, queues.data_out)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='validate_post'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[payload] validate_post thread has finished')


def failed_post(queues: namedtuple, traces: Any, args: object):
    """
    Handle failed jobs.

    Thread.

    Get a Job object from the "failed_payloads" queue. Set the pilot state to "stageout" and the stageout field to
    "log", and add the Job object to the "data_out" queue.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        # finished payloads
        try:
            job = queues.failed_payloads.get(block=True, timeout=1)
        except queue.Empty:
            time.sleep(0.1)
            continue

        logger.debug('adding log for log stageout')

        job.stageout = 'log'  # only stage-out log file
        #queues.data_out.put(job)
        set_pilot_state(job=job, state='stageout')
        put_in_queue(job, queues.data_out)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='failed_post'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[payload] failed_post thread has finished')
