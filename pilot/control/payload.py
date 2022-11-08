#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2022
# - Wen Guan, wen.guan@cern.ch, 2017-2018

import os
import time
import traceback
import queue
from re import findall, split

from pilot.control.payloads import generic, eventservice, eventservicemerge
from pilot.control.job import send_state
from pilot.util.auxiliary import set_pilot_state
from pilot.util.container import execute
from pilot.util.processes import get_cpu_consumption_time
from pilot.util.config import config
from pilot.util.filehandling import read_file, remove_core_dumps, get_guid, extract_lines_from_file, find_file
from pilot.util.processes import threads_aborted
from pilot.util.queuehandling import put_in_queue
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import ExcThread
from pilot.util.realtimelogger import get_realtime_logger

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def control(queues, traces, args):
    """
    (add description)

    :param queues:
    :param traces:
    :param args:
    :return:
    """

    targets = {'validate_pre': validate_pre, 'execute_payloads': execute_payloads, 'validate_post': validate_post,
               'failed_post': failed_post, 'run_realtimelog': run_realtimelog}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in list(targets.items())]

    [thread.start() for thread in threads]

    # if an exception is thrown, the graceful_stop will be set by the ExcThread class run() function
    while not args.graceful_stop.is_set():
        for thread in threads:
            bucket = thread.get_bucket()
            try:
                exc = bucket.get(block=False)
            except queue.Empty:
                pass
            else:
                exc_type, exc_obj, exc_trace = exc
                logger.warning(f"thread \'{thread.name}\' received an exception from bucket: {exc_obj}")

                # deal with the exception
                # ..

            thread.join(0.1)
            time.sleep(0.1)

        time.sleep(0.5)

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
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] control thread has finished')


def validate_pre(queues, traces, args):
    """
    Get a Job object from the "payloads" queue and validate it.

    If the payload is successfully validated (user defined), the Job object is placed in the "validated_payloads" queue,
    otherwise it is placed in the "failed_payloads" queue.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
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
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] validate_pre thread has finished')


def _validate_payload(job):
    """
    Perform validation tests for the payload.

    :param job: job object.
    :return: boolean.
    """

    status = True

    # perform user specific validation
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)
    try:
        status = user.validate(job)
    except Exception as error:
        logger.fatal(f'failed to execute user validate() function: {error}')
        status = False

    return status


def get_payload_executor(args, job, out, err, traces):
    """
    Get payload executor function for different payload.

    :param args: args object.
    :param job: job object.
    :param out:
    :param err:
    :param traces: traces object.
    :return: instance of a payload executor
    """
    if job.is_eventservice:  # True for native HPO workflow as well
        payload_executor = eventservice.Executor(args, job, out, err, traces)
    elif job.is_eventservicemerge:
        payload_executor = eventservicemerge.Executor(args, job, out, err, traces)
    else:
        payload_executor = generic.Executor(args, job, out, err, traces)
    return payload_executor


def execute_payloads(queues, traces, args):  # noqa: C901
    """
    Execute queued payloads.

    Extract a Job object from the "validated_payloads" queue and put it in the "monitored_jobs" queue. The payload
    stdout/err streams are opened and the pilot state is changed to "starting". A payload executor is selected (for
    executing a normal job, an event service job or event service merge job). After the payload (or rather its executor)
    is started, the thread will wait for it to finish and then check for any failures. A successfully completed job is
    placed in the "finished_payloads" queue, and a failed job will be placed in the "failed_payloads" queue.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
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
            exit_code = payload_executor.run()

            set_cpu_consumption_time(job)
            job.transexitcode = exit_code % 255

            out.close()
            err.close()

            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()

            # some HPO jobs will produce new output files (following lfn name pattern), discover those and replace the job.outdata list
            if job.is_hpo:
                user = __import__('pilot.user.%s.common' % pilot_user, globals(), locals(), [pilot_user], 0)
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
            user = __import__('pilot.user.%s.diagnose' % pilot_user, globals(), locals(), [pilot_user], 0)
            try:
                exit_code_interpret = user.interpret(job)
            except Exception as error:
                logger.warning(f'exception caught: {error}')
                if 'error code:' in error and 'message:' in error:
                    error_code, diagnostics = extract_error_info(error)
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error_code, msg=diagnostics)
                else:
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.INTERNALPILOTPROBLEM, msg=error)

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
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] execute_payloads thread has finished')


def extract_error_info(error):
    """
    Extract the error code and diagnostics from an error exception.

    :param error: exception string.
    :return: error code (int), diagnostics (string).
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


def get_transport(catchall):
    """
    Extract the transport/protocol from catchall if present.

    :param catchall: PQ.catchall field (string).
    :return: transport (string).
    """

    transport = ''

    return transport


def get_logging_info(job, args):
    """
    Extract the logging type/protocol/url/port from catchall if present, or from args fields.
    Returns a dictionary with the format: {'logging_type': .., 'protocol': .., 'url': .., 'port': .., 'logname': ..}

    If the provided debug_command contains a tail instruction ('tail log_file_name'), the pilot will locate
    the log file and use that for RT logging (full path).

    Note: the returned dictionary can be built with either args (has priority) or catchall info.

    :param job: job object.
    :param args: args object.
    :return: info dictionary (logging_type (string), protocol (string), url (string), port (int)).
    """

    info_dic = {}

    if not job.realtimelogging:
        logger.info("job.realtimelogging is not enabled")
        return {}

    # args handling
    info_dic['logname'] = args.realtime_logname if args.realtime_logname else "pilot-log"
    logserver = args.realtime_logging_server if args.realtime_logging_server else ""

    pattern = r'(\S+)\;(\S+)\:\/\/(\S+)\:(\d+)'
    info = findall(pattern, config.Pilot.rtlogging)

    if not logserver and not info:
        logger.warning('not enough info available for activating real-time logging')
        return {}

    if len(logserver) > 0:
        items = logserver.split(':')
        info_dic['logging_type'] = items[0].lower()
        pattern = r'(\S+)\:\/\/(\S+)'
        if len(items) > 2:
            _address = findall(pattern, items[1])
            info_dic['port'] = items[2]
        else:
            _address = None
            info_dic['port'] = 24224
        if _address:
            info_dic['protocol'] = _address[0][0]
            info_dic['url'] = _address[0][1]
        else:
            logger.warning(f'protocol/url could not be extracted from {items}')
            info_dic['protocol'] = ''
            info_dic['url'] = ''
    elif info:
        try:
            info_dic['logging_type'] = info[0][0]
            info_dic['protocol'] = info[0][1]
            info_dic['url'] = info[0][2]
            info_dic['port'] = info[0][3]
        except IndexError as exc:
            logger.warning(f'exception caught: {exc}')
            return {}
        else:
            # find the log file to tail
            path = find_log_to_tail(job.debug_command, job.workdir, args, job.is_analysis())
            logger.info(f'using {path} for real-time logging')
            info_dic['logfiles'] = [path]

    if 'logfiles' not in info_dic:
        # Rubin
        logfiles = os.environ.get('REALTIME_LOGFILES', None)
        if logfiles is not None:
            info_dic['logfiles'] = split('[:,]', logfiles)

    return info_dic


def find_log_to_tail(debug_command, workdir, args, is_analysis):
    """
    Find the log file to tail in the RT logging.

    :param debug_command: requested debug command (string).
    :param workdir: job working directory (string).
    :param args: pilot args object.
    :param is_analysis: True for user jobs (Bool).
    :return: path to log file (string).
    """

    path = ""
    filename = ""
    counter = 0
    maxwait = 5 * 60

    if 'tail' in debug_command:
        filename = debug_command.split(' ')[-1]
    elif is_analysis:
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

    # fallback to known log file if no other file could be found
    if not path:
        logger.warning(f'file {filename} was not found for {maxwait} s, using default')
    logf = path if path else config.Payload.payloadstdout

    return logf


def run_realtimelog(queues, traces, args):  # noqa: C901
    """
    Validate finished payloads.
    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out (in failed_post() below).

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
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
        while not args.graceful_stop.is_set():

            # note: in multi-job mode, the real-time logging will be switched off at the end of the job
            while not args.graceful_stop.is_set():
                if job.state == 'running':
                    if first1:
                        logger.debug('job is running, check if real-time logger is needed')
                        first1 = False
                    break
                if job.state == 'stageout' or job.state == 'failed' or job.state == 'holding':
                    if first2:
                        logger.debug(f'job is in state {job.state}, continue to next job or abort (wait for graceful stop)')
                        first2 = False
                    time.sleep(10)
                    continue
                time.sleep(1)

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
        else:
            logger.debug('realtime logger was found')

        realtime_logger.sending_logs(args, job)

    # proceed to set the job_aborted flag?
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] run_realtimelog thread has finished')


def set_cpu_consumption_time(job):
    """
    Set the CPU consumption time.
    :param job: job object.
    :return:
    """

    cpuconsumptiontime = get_cpu_consumption_time(job.t0)
    job.cpuconsumptiontime = int(round(cpuconsumptiontime))
    job.cpuconsumptionunit = "s"
    job.cpuconversionfactor = 1.0
    logger.info(f'CPU consumption time: {cpuconsumptiontime} {job.cpuconsumptionunit} (rounded to {job.cpuconsumptiontime} {job.cpuconsumptionunit})')


def perform_initial_payload_error_analysis(job, exit_code):
    """
    Perform an initial analysis of the payload.
    Singularity/apptainer errors are caught here.

    :param job: job object.
    :param exit_code: exit code from payload execution.
    :return:
    """

    if exit_code != 0:
        logger.warning(f'main payload execution returned non-zero exit code: {exit_code}')

    # look for singularity/apptainer errors (the exit code can be zero in this case)
    path = os.path.join(job.workdir, config.Payload.payloadstderr)
    if os.path.exists(path):
        stderr = read_file(path)
        exit_code = errors.resolve_transform_error(exit_code, stderr)
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
        msg = ""

        # are there any critical errors in the stdout?
        path = os.path.join(job.workdir, config.Payload.payloadstdout)
        if os.path.exists(path):
            lines = extract_lines_from_file('CRITICAL', path)
            if lines:
                logger.warning(f'found CRITICAL errors in {config.Payload.payloadstdout}:\n{lines}')
                msg = lines.split('\n')[0]
        else:
            logger.warning('found no payload stdout')

        if stderr != "" and not msg:
            msg = errors.extract_stderr_error(stderr)
            if msg == "":
                # look for warning messages instead (might not be fatal so do not set UNRECOGNIZEDTRFSTDERR)
                msg = errors.extract_stderr_warning(stderr)
            #    fatal = False
            #else:
            #    fatal = True
            #if msg != "":  # redundant since resolve_transform_error is used above
            #    logger.warning("extracted message from stderr:\n%s", msg)
            #    exit_code = set_error_code_from_stderr(msg, fatal)

        # note: msg should be constructed either from stderr or stdout
        if msg:
            msg = errors.format_diagnostics(exit_code, msg)

        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code, msg=msg)

        '''
        if exit_code != 0:
            if msg:
                msg = errors.format_diagnostics(exit_code, msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code, msg=msg)
        else:
            if job.piloterrorcodes:
                logger.warning('error code(s) already set: %s', str(job.piloterrorcodes))
            else:
                # check if core dumps exist, if so remove them and return True
                if remove_core_dumps(job.workdir) and not job.debug:
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.COREDUMP)
                else:
                    logger.warning('initial error analysis did not resolve the issue (and core dumps were not found)')
        '''
    else:
        logger.info('main payload execution returned zero exit code')

    # check if core dumps exist, if so remove them and return True
    if not job.debug:  # do not shorten these if-statements
        # only return True if found core dump belongs to payload
        if remove_core_dumps(job.workdir, pid=job.pid):
            # COREDUMP error will only be set if the core dump belongs to the payload (ie 'core.<payload pid>')
            logger.warning('setting COREDUMP error')
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.COREDUMP)


def scan_for_memory_errors(subprocesses):
    """
    Scan for memory errors in dmesg messages.

    :param subprocesses: list of payload subprocesses.
    :return: error diagnostics (string).
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
                    break

        if diagnostics:
            break

    return diagnostics


def set_error_code_from_stderr(msg, fatal):
    """
    Identify specific errors in stderr and set the corresponding error code.
    The function returns 0 if no error is recognized.

    :param msg: stderr (string).
    :param fatal: boolean flag if fatal error among warning messages in stderr.
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


def validate_post(queues, traces, args):
    """
    Validate finished payloads.
    If payload finished correctly, add the job to the data_out queue. If it failed, add it to the data_out queue as
    well but only for log stage-out (in failed_post() below).

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
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
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] validate_post thread has finished')


def failed_post(queues, traces, args):
    """
    Get a Job object from the "failed_payloads" queue. Set the pilot state to "stakeout" and the stageout field to
    "log", and add the Job object to the "data_out" queue.

    :param queues: internal queues for job handling.
    :param traces: tuple containing internal pilot states.
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc).
    :return:
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
    if threads_aborted():
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()
    else:
        logger.debug('will not set job_aborted yet')

    logger.info('[payload] failed_post thread has finished')
