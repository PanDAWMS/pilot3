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
# - Wen Guan, wen.guan@cern.ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018

"""Control interface to data API."""

import logging
import os
import time
import traceback
import queue
from collections import namedtuple
from typing import Any
from pathlib import Path

from pilot.api.data import (
    StageInClient,
    StageOutClient
)
from pilot.api.es_data import StageInESClient
from pilot.control.job import send_state
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    ExcThread,
    FileHandlingFailure,
    LogFileCreationFailure,
    NoSuchFile,
    PilotException,
)
from pilot.info import JobData
from pilot.util.auxiliary import (
    set_pilot_state,
    check_for_final_server_update
)
from pilot.util.common import should_abort
from pilot.util.config import config
from pilot.util.constants import (
    LOG_TRANSFER_DONE,
    LOG_TRANSFER_FAILED,
    LOG_TRANSFER_IN_PROGRESS,
    LOG_TRANSFER_NOT_DONE,
    MAX_KILL_WAIT_TIME,
    PILOT_POST_LOG_TAR,
    PILOT_POST_STAGEIN,
    PILOT_POST_STAGEOUT,
    PILOT_PRE_LOG_TAR,
    PILOT_PRE_STAGEIN,
    PILOT_PRE_STAGEOUT,
    SERVER_UPDATE_RUNNING,
    UTILITY_BEFORE_STAGEIN
)
from pilot.util.container import execute
from pilot.util.filehandling import (
    copy,
    find_files_with_pattern,
    get_directory_size,
    remove,
    rename_xrdlog,
    write_file,
)
from pilot.util.https import get_base_urls
from pilot.util.middleware import (
    containerise_middleware,
    use_middleware_script
)
from pilot.util.processes import threads_aborted
from pilot.util.queuehandling import (
    declare_failed_by_kill,
    put_in_queue
)
from pilot.util.timing import add_to_pilot_timing
from pilot.util.tracereport import TraceReport

logger = logging.getLogger(__name__)

errors = ErrorCodes()


def control(queues: namedtuple, traces: Any, args: object):
    """
    Set up data control threads.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    targets = {'copytool_in': copytool_in, 'copytool_out': copytool_out, 'queue_monitoring': queue_monitoring}
    threads = [ExcThread(bucket=queue.Queue(), target=target, kwargs={'queues': queues, 'traces': traces, 'args': args},
                         name=name) for name, target in list(targets.items())]  # Python 2/3

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
                    logger.warning("thread \'%s\' received an exception from bucket: %s", thread.name, exc_obj)

                    # deal with the exception
                    # ..

                thread.join(0.1)
                time.sleep(0.1)

            time.sleep(0.5)
    except Exception as exc:
        logger.warning(f"exception caught while handling threads: {exc}")
    finally:
        logger.info('all data control threads have been joined')

    logger.debug('data control ending since graceful_stop has been set')
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

    logger.info('[data] control thread has finished')


def skip_special_files(job: JobData):
    """
    Consult user defined code if any files should be skipped during stage-in.

    ATLAS code will skip DBRelease files e.g. as they should already be available in CVMFS.

    :param job: job object (JobData).
    """
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
    try:
        user.update_stagein(job)
    except Exception as error:
        logger.warning('caught exception: %s', error)


def update_indata(job: JobData):
    """
    Remove files marked as no_transfer files from stage-in.

    :param job: job object (JobData).
    """
    toberemoved = []
    for fspec in job.indata:
        if fspec.status == 'no_transfer':
            toberemoved.append(fspec)
    for fspec in toberemoved:
        logger.info('removing fspec object (lfn=%s) from list of input files', fspec.lfn)
        job.indata.remove(fspec)


def get_trace_report_variables(job: JobData, label: str = 'stage-in') -> (str, str, str):
    """
    Get some of the variables needed for creating the trace report.

    :param job: job object (JobData)
    :param label: 'stage-[in|out]' (str)
    :return: event_type (str), localsite (str), remotesite (str).
    """
    event_type = "get_sm" if label == 'stage-in' else "put_sm"
    if job.is_analysis():
        event_type += "_a"
    data = job.indata if label == 'stage-in' else job.outdata
    localsite = remotesite = get_rse(data)

    return event_type, localsite, remotesite


def create_trace_report(job: JobData, label: str = 'stage-in') -> Any:
    """
    Create the trace report object.

    :param job: job object (JobData)
    :param label: 'stage-[in|out]' (str)
    :return: trace report object (Any).
    """
    event_type, localsite, remotesite = get_trace_report_variables(job, label=label)
    trace_report = TraceReport(pq=os.environ.get('PILOT_SITENAME', ''), localSite=localsite, remoteSite=remotesite,
                               dataset="", eventType=event_type, workdir=job.workdir)
    trace_report.init(job)

    return trace_report


def get_stagein_client(job: JobData, args: object, label: str = 'stage-in') -> (Any, str, list):
    """
    Return the proper stage-in client.

    :param job: job object (JobData)
    :param args: pilot args object (object)
    :param label: 'stage-in' (str)
    :return: stage-in client (StageInClient), copytool_activities, storage_activities
    """
    # create the trace report
    trace_report = create_trace_report(job, label=label)

    if job.is_eventservicemerge:
        client = StageInESClient(job.infosys, logger=logger, trace_report=trace_report)
        activity = 'es_events_read'
        storage_activities = ['read_lan']
    else:
        client = StageInClient(job.infosys, logger=logger, trace_report=trace_report,
                               ipv=args.internet_protocol_version, workdir=job.workdir)
        activity = 'read_lan'

        is_unified = job.infosys.queuedata.type == 'unified' if job.infosys else None
        is_analysis = job.is_analysis()
        storage_activities = ['read_lan_analysis', 'read_lan'] if is_unified and is_analysis else ['read_lan']

    return client, activity, storage_activities


def _stage_in(args: object, job: JobData) -> bool:
    """
    Call the stage-in client.

    :param args: pilot args object (object)
    :param job: job object (JobData)
    :return: True in case of success, False otherwise (bool).
    """
    # tested ok:
    #logger.info('testing sending SIGUSR1')
    #import signal
    #os.kill(os.getpid(), signal.SIGUSR1)

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEIN, time.time(), args)

    # any DBRelease files should not be staged in
    skip_special_files(job)

    # now that the trace report has been created, remove any files that are not to be transferred (DBRelease files) from the indata list
    update_indata(job)

    label = 'stage-in'

    # should stage-in be done by a script (for containerisation) or by invoking the API (ie classic mode)?
    use_container = use_middleware_script(job.infosys.queuedata.container_type.get("middleware"))
    if use_container:
        logger.info('stage-in will be done in a container')
        try:
            eventtype, localsite, remotesite = get_trace_report_variables(job, label=label)
            containerise_middleware(job, args, job.indata, eventtype, localsite, remotesite,
                                    label=label,
                                    container_type=job.infosys.queuedata.container_type.get("middleware"))
        except PilotException as error:
            logger.warning('stage-in containerisation threw a pilot exception: %s', error)
        except Exception as error:
            logger.warning('stage-in containerisation threw an exception: %s', error)
            logger.error(traceback.format_exc())
    else:
        try:
            logger.info('stage-in will not be done in a container')

            client, activity, storage_activities = get_stagein_client(job, args, label)
            logger.info(f'activity={activity}, storage_activities={storage_activities}')
            use_pcache = job.infosys.queuedata.use_pcache
            logger.debug(f'use_pcache={use_pcache}')
            # get the proper input file destination (normally job.workdir unless stager workflow)
            jobworkdir = job.workdir  # there is a distinction for mv copy tool on ND vs non-ATLAS
            workdir = get_proper_input_destination(job.workdir, args.input_destination_dir)
            logger.debug(f'workdir={workdir}')
            kwargs = {'workdir': workdir,
                      'cwd': job.workdir,
                      'usecontainer': False,
                      'use_pcach': use_pcache,
                      'use_bulk': False,
                      'use_vp': job.use_vp,
                      'catchall': job.infosys.queuedata.catchall,
                      'checkinputsize': True,
                      'rucio_host': args.rucio_host,
                      'jobworkdir': jobworkdir,
                      'args': args}
            logger.debug(f'kwargs={kwargs}')
            client.prepare_sources(job.indata)
            client.prepare_inputddms(job.indata, storage_activities)
            logger.info('prepared sources - will now transfer files')
            client.transfer(job.indata, activity=activity, **kwargs)
        except PilotException as error:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            msg = errors.format_diagnostics(error.get_error_code(), error_msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code(), msg=msg)
        except Exception as error:
            logger.error('failed to stage-in: error=%s', error)
        else:
            # only the data API will know if the input file sizes should be included in size checks
            for fspec in job.indata:
                if not fspec.checkinputsize:
                    job.checkinputsize = False
                    break  # it's enough to check one file
            logger.debug(f'checkinputsize={job.checkinputsize}')

    logger.info('summary of transferred files:')
    for infile in job.indata:
        status = infile.status if infile.status else "(not transferred)"
        logger.info(" -- lfn=%s, status_code=%s, status=%s", infile.lfn, infile.status_code, status)

    # write time stamps to pilot timing file

    # MOVE THIS TO AFTER REMOTE FILE OPEN HAS BEEN VERIFIED (actually just before the payload starts)
    add_to_pilot_timing(job.jobid, PILOT_POST_STAGEIN, time.time(), args)

    remain_files = [infile for infile in job.indata if infile.status not in ['remote_io', 'transferred', 'no_transfer']]
    if not remain_files:
        logger.info("stage-in finished")
    else:
        logger.info("stage-in failed")
    os.environ['PILOT_JOB_STATE'] = 'stageincompleted'

    return not remain_files


def get_proper_input_destination(workdir: str, input_destination_dir: str) -> str:
    """
    Return the proper input file destination.

    Normally this would be the job.workdir, unless an input file destination has been set with pilot
    option --input-file-destination (which should be set for stager workflow).

    :param workdir: job work directory (str)
    :param input_destination_dir: optional input file destination (str)
    :return: input file destination (str).
    """
    if input_destination_dir:
        if not os.path.exists(input_destination_dir):
            logger.warning(f'input file destination does not exist: {input_destination_dir} (defaulting to {workdir})')
            destination = workdir
        else:
            destination = input_destination_dir
    else:
        destination = workdir

    logger.info(f'will use input file destination: {destination}')

    return destination


def get_rse(data: Any, lfn: str = "") -> str:
    """
    Return the ddmEndPoint corresponding to the given lfn.

    If lfn is not provided, the first ddmEndPoint will be returned.

    :param data: FileSpec list object (Any)
    :param lfn: local file name (str)
    :return: rse (str).
    """
    rse = ""

    if lfn == "":
        try:
            return data[0].ddmendpoint
        except Exception as error:
            logger.warning("exception caught: %s", error)
            logger.warning("end point is currently unknown")
            return "unknown"

    for fspec in data:
        if fspec.lfn == lfn:
            rse = fspec.ddmendpoint

    if rse == "":
        logger.warning("end point is currently unknown")
        rse = "unknown"

    return rse


def write_output(filename: str, output: str):
    """
    Write command output to file.

    :param filename: file name (str)
    :param output: command stdout/stderr (str).
    """
    try:
        write_file(filename, output, unique=True)
    except PilotException as error:
        logger.warning('failed to write utility output to file: %s, %s', error, output)
    else:
        logger.debug('wrote %s', filename)


def write_utility_output(workdir: str, step: str, stdout: str, stderr: str):
    """
    Write the utility command output to stdout, stderr files to the job.workdir for the current step.

    -> <step>_stdout.txt, <step>_stderr.txt
    Example of step: xcache.

    :param workdir: job workdir (str)
    :param step: utility step (str)
    :param stdout: command stdout (str)
    :param stderr: command stderr (str).
    """
    # dump to files
    write_output(os.path.join(workdir, step + '_stdout.txt'), stdout)
    write_output(os.path.join(workdir, step + '_stderr.txt'), stderr)


def copytool_in(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Call the stage-in function and put the job object in the proper queue.

    Main stage-in thread.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    abort = False
    while not args.graceful_stop.is_set() and not abort:
        time.sleep(0.5)
        try:
            # abort if kill signal arrived too long time ago, ie loop is stuck
            current_time = int(time.time())
            if args.kill_time and current_time - args.kill_time > MAX_KILL_WAIT_TIME:
                logger.warning('loop has run for too long time after first kill signal - will abort')
                break

            # extract a job to stage-in its input
            job = queues.data_in.get(block=True, timeout=1)

            # convert the base URLs for trf downloads to a list (most likely from an empty string)
            base_urls = get_base_urls(args.baseurls)

            # does the user want to execute any special commands before stage-in?
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)  # Python 2/3
            cmd = user.get_utility_commands(job=job, order=UTILITY_BEFORE_STAGEIN, base_urls=base_urls)
            if cmd:
                _, stdout, stderr = execute(cmd.get('command'))
                logger.debug('stdout=%s', stdout)
                logger.debug('stderr=%s', stderr)

                # perform any action necessary after command execution (e.g. stdout processing)
                kwargs = {'label': cmd.get('label', 'utility'), 'output': stdout}
                user.post_prestagein_utility_command(**kwargs)

                # write output to log files
                write_utility_output(job.workdir, cmd.get('label', 'utility'), stdout, stderr)

            # place it in the current stage-in queue (used by the jobs' queue monitoring)
            put_in_queue(job, queues.current_data_in)

            # ready to set the job in running state
            send_state(job, args, 'running')

            # note: when sending a state change to the server, the server might respond with 'tobekilled'
            if job.state == 'failed':
                logger.warning('job state is \'failed\' - order log transfer and abort copytool_in()')
                job.stageout = 'log'  # only stage-out log file
                put_in_queue(job, queues.data_out)
                break

            os.environ['SERVER_UPDATE'] = SERVER_UPDATE_RUNNING

            if args.abort_job.is_set():
                traces.pilot['command'] = 'abort'
                logger.warning('copytool_in detected a set abort_job pre stage-in (due to a kill signal)')
                declare_failed_by_kill(job, queues.failed_data_in, args.signal)
                if args.graceful_stop.is_set():
                    break

            if _stage_in(args, job):
                if args.abort_job.is_set():
                    traces.pilot['command'] = 'abort'
                    logger.warning('copytool_in detected a set abort_job post stage-in (due to a kill signal)')
                    declare_failed_by_kill(job, queues.failed_data_in, args.signal)
                    if args.graceful_stop.is_set():
                        break

                put_in_queue(job, queues.finished_data_in)
                # remove the job from the current stage-in queue
                _job = queues.current_data_in.get(block=True, timeout=1)
                if _job:
                    logger.debug('job %s has been removed from the current_data_in queue', _job.jobid)

                # now create input file metadata if required by the payload
                if os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') == 'generic':
                    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
                    try:
                        user = __import__(f'pilot.user.{pilot_user}.metadata', globals(), locals(), [pilot_user], 0)
                        file_dictionary = get_input_file_dictionary(job.indata)
                        xml = user.create_input_file_metadata(file_dictionary, job.workdir)
                        logger.info('created input file metadata:\n%s', xml)
                    except ModuleNotFoundError as exc:
                        logger.warning(f'no such module: {exc} (will not create input file metadata)')

                if args.pod and args.workflow == 'stager':
                    # files can now be moved to init dir, which will be the same as the PANDA_WORKDIR for the jupyter user
                    for fspec in job.indata:
                        path = os.path.join(job.workdir, fspec.lfn)
                        if os.path.exists(path):
                            dest = os.environ.get('PILOT_WORKDIR')
                            cmd = f"mv {path} {dest}"
                            ec, _, stderr = execute(cmd)
                            if ec:
                                logger.warning(f'move failed: {stderr}')
                            else:
                                logger.info(f"moved file {path} to {dest}")
                        else:
                            logger.warning(f'path does not exist: {path}')

                    # stage-out log file
                    #job.stageout = "log"
                    #if not _stage_out_new(job, args):
                    #    logger.info(f"job {job.jobid} failed during stage-out of log, adding job object to failed_data_outs queue")
                    #    put_in_queue(job, queues.failed_data_out)
                    #else:
                    #    logger.info(f"job {job.jobid} has finished")
                    #    put_in_queue(job, queues.finished_jobs)

                    # this job is now to be monitored, so add it to the monitored_payloads queue
                    put_in_queue(job, queues.monitored_payloads)

                    logger.info('stage-in thread is no longer needed - terminating')
                    abort = True
                    break
                    #args.job_aborted.set()
                    #args.graceful_stop.set()
            else:
                # remove the job from the current stage-in queue
                _job = queues.current_data_in.get(block=True, timeout=1)
                if _job:
                    logger.debug('job %s has been removed from the current_data_in queue', _job.jobid)
                logger.warning('stage-in failed, adding job object to failed_data_in queue')
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINFAILED)
                set_pilot_state(job=job, state="failed")
                traces.pilot['error_code'] = job.piloterrorcodes[0]
                put_in_queue(job, queues.failed_data_in)
                # do not set graceful stop if pilot has not finished sending the final job update
                # i.e. wait until SERVER_UPDATE is DONE_FINAL
                check_for_final_server_update(args.update_server)
                args.graceful_stop.set()

        except queue.Empty:
            continue

    if abort:
        logger.debug('an abort was received - finishing stage-in thread')

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='copytool_in') and args.workflow != 'stager':  # only finish this thread in stager mode
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[data] copytool_in thread has finished')


def copytool_out(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Perform stage-out as soon as a job object can be extracted from the data_out queue.

    Main stage-out thread.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    cont = True
    if args.graceful_stop.is_set():
        logger.debug('graceful_stop already set - do not start copytool_out thread')

    processed_jobs = []
    while cont:

        time.sleep(0.5)

        # abort if kill signal arrived too long time ago, ie loop is stuck
        current_time = int(time.time())
        if args.kill_time and current_time - args.kill_time > MAX_KILL_WAIT_TIME:
            logger.warning('loop has run for too long time after first kill signal - will abort')
            break

        # check for abort, print useful messages and include a 1 s sleep
        abort = should_abort(args, label='data:copytool_out')
        try:
            job = queues.data_out.get(block=True, timeout=1)
            if job:
                # hack to prevent stage-out to be called more than once for same job object (can apparently happen
                # in multi-output jobs)
                # should not be necessary unless job object is added to queues.data_out more than once - check this
                # for multiple output files
                if processed_jobs:
                    if is_already_processed(queues, processed_jobs):
                        continue

                logger.info('will perform stage-out for job id=%s', job.jobid)

                if args.abort_job.is_set():
                    traces.pilot['command'] = 'abort'
                    logger.warning('copytool_out detected a set abort_job pre stage-out (due to a kill signal)')
                    declare_failed_by_kill(job, queues.failed_data_out, args.signal)
                    if abort:
                        break

                if _stage_out_new(job, args):
                    if args.abort_job.is_set():
                        traces.pilot['command'] = 'abort'
                        logger.warning('copytool_out detected a set abort_job post stage-out (due to a kill signal)')
                        #declare_failed_by_kill(job, queues.failed_data_out, args.signal)
                        if args.graceful_stop.is_set():
                            break

                    #queues.finished_data_out.put(job)
                    processed_jobs.append(job.jobid)
                    put_in_queue(job, queues.finished_data_out)
                    logger.debug('job object added to finished_data_out queue')
                else:
                    #queues.failed_data_out.put(job)
                    put_in_queue(job, queues.failed_data_out)
                    logger.debug('job object added to failed_data_out queue')
            else:
                logger.debug('no returned job - why no exception?')
        except queue.Empty:
            if abort:
                cont = False
                break
            continue

        if abort:
            cont = False
            break

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='copytool_out'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[data] copytool_out thread has finished')


def is_already_processed(queues: namedtuple, processed_jobs: list) -> bool:
    """
    Skip stage-out in case the job has already been processed.

    This should not be necessary so this is a fail-safe but it seems there is a case when a job with multiple output
    files enters the stage-out more than once.

    :param queues: queues object (namedtuple)
    :param processed_jobs: list of already processed jobs (list)
    :return: True if stage-out queues contain a job object that has already been processed, False otherwise (bool).
    """
    snapshots = list(queues.finished_data_out.queue) + list(queues.failed_data_out.queue)
    jobids = [obj.jobid for obj in snapshots]
    found = False

    for jobid in processed_jobs:
        if jobid in jobids:
            logger.warning('output from job %s has already been staged out', jobid)
            found = True
            break
    if found:
        time.sleep(5)

    return found


def get_input_file_dictionary(indata: list) -> dict:
    """
    Return an input file dictionary.

    Format: {'guid': 'pfn', ..}

    Normally use_turl would be set to True if direct access is used.
    Note: any environment variables in the turls will be expanded

    :param indata: list of FileSpec objects (list)
    :return: file dictionary (dict).
    """
    ret = {}

    for fspec in indata:
        ret[fspec.guid] = fspec.turl if fspec.status == 'remote_io' else fspec.lfn
        ret[fspec.guid] = os.path.expandvars(ret[fspec.guid])

        # correction for ND and mv
        # in any case use the lfn instead of pfn since there are trf's that have problems with pfn's
        if not ret[fspec.guid]:   # this case never works (turl/lfn is always non empty), deprecated code?
            ret[fspec.guid] = fspec.lfn

    return ret


def remove_input_output_files(workdir: str, files: list):
    """
    Remove input/output files.

    :param workdir: work directory (str)
    :param files: list of files to remove (list).
    """
    for fname in files:
        path = os.path.join(workdir, fname)
        if os.path.exists(path):
            logger.info(f'removing file: {path}')
            remove(path)


def create_log(workdir: str, logfile_name: str, tarball_name: str, cleanup: bool,  # noqa: C901
               input_files: list = None, output_files: list = None, piloterrors: list = None, debugmode: bool = False):  # noqa: C901
    """
    Create the tarball for the job.

    :param workdir: work directory for the job (str)
    :param logfile_name: log file name (str)
    :param tarball_name: tarball name (str)
    :param cleanup: perform cleanup (bool)
    :param input_files: list of input files to remove (list)
    :param output_files: list of output files to remove (list)
    :param piloterrors: list of Pilot assigned error codes (list)
    :param debugmode: True if debug mode has been switched on (bool)
    :raises LogFileCreationFailure: in case of log file creation problem.
    """
    if input_files is None:
        input_files = []
    if output_files is None:
        output_files = []
    if piloterrors is None:
        piloterrors = []
    logger.debug(f'preparing to create log file (debug mode={debugmode})')

    # PILOT_HOME is the launch directory of the pilot (or the one specified in pilot options as pilot workdir)
    pilot_home = os.environ.get('PILOT_HOME', os.getcwd())
    current_dir = os.getcwd()
    if pilot_home != current_dir:
        os.chdir(pilot_home)

    # copy special files if they exist (could be made experiment specific if there's a need for it)
    copy_special_files(workdir)

    # perform special cleanup (user specific) prior to log file creation
    if cleanup:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)  # Python 2/3
        user.remove_redundant_files(workdir, piloterrors=piloterrors, debugmode=debugmode)

    # remove any present input/output files before tarring up workdir
    remove_input_output_files(workdir, input_files + output_files)

    if logfile_name is None or len(logfile_name.strip('/ ')) == 0:
        logger.info('skipping tarball creation, since the logfile_name is empty')
        return

    newworkdir = os.path.join(os.path.dirname(workdir), tarball_name)
    orgworkdir = workdir
    os.rename(workdir, newworkdir)
    workdir = newworkdir
    dirsize = get_directory_size(workdir)
    timeout = get_tar_timeout(dirsize)
    fullpath = os.path.join(current_dir, logfile_name)  # /some/path/to/dirname/log.tgz
    logger.info(f'will create archive {fullpath} using timeout={timeout} s for directory size={dirsize} MB')

    try:
        # add e.g. sleep 200; before tar command to test time-out
        cmd = f"pwd;tar cvfz {fullpath} {tarball_name} --dereference --one-file-system; echo $?"
        exit_code, stdout, stderr = execute(cmd, timeout=timeout)
    except Exception as error:
        raise LogFileCreationFailure(error) from error
    if pilot_home != current_dir:
        os.chdir(pilot_home)
    logger.debug(f'stdout: {stdout}')

    try:
        os.rename(workdir, orgworkdir)
    except OSError as error:
        logger.debug(f'exception caught when renaming workdir: {error} (ignore)')

    if exit_code:
        diagnostics = f'tarball creation failed with exit code: {exit_code}, stdout={stdout}, stderr={stderr}'
        logger.warning(diagnostics)
        if exit_code == errors.COMMANDTIMEDOUT:
            exit_code = errors.LOGCREATIONTIMEOUT
        raise PilotException(diagnostics, code=exit_code)

    # final step, copy the log file into the workdir - otherwise containerized stage-out won't work
    try:
        copy(fullpath, orgworkdir)
    except (NoSuchFile, FileHandlingFailure) as exc:
        logger.warning(f'caught exception when copying tarball: {exc}')


def copy_special_files(tardir: str):
    """
    Copy any special files into the directory to be tarred up.

    :param tardir: path to tar directory (str).
    """
    # general pattern, typically xrdlog.txt. The pilot might produce multiple files, xrdlog.txt-LFN1..N
    xrd_logfile = os.environ.get('XRD_LOGFILE', None)
    pilot_home = os.environ.get('PILOT_HOME', None)

    if xrd_logfile:
        # xrootd is then expected to have produced a corresponding log file
        if pilot_home:
            #suffix = Path(xrd_logfile).suffix  # .txt
            stem = Path(xrd_logfile).stem  # xrdlog

            # in case the payload also produced an xrdlog.txt file, rename it
            rename_xrdlog('payload')

            # find all log files
            matching_files = find_files_with_pattern(pilot_home, f'{stem}*')
            for logfile in matching_files:
                path = os.path.join(pilot_home, logfile)
                try:
                    copy(path, tardir)
                except (NoSuchFile, FileHandlingFailure) as exc:
                    logger.warning(f'caught exception when copying {logfile}: {exc}')
        else:
            logger.warning(f'cannot look for {xrd_logfile} since PILOT_HOME was not set')

    path = os.path.join(pilot_home, config.Pilot.pilot_heartbeat_file)
    if os.path.exists(path):
        try:
            copy(path, tardir)
        except (NoSuchFile, FileHandlingFailure) as exc:
            logger.warning(f'caught exception when copying {path}: {exc}')
    else:
        logger.warning(f'cannot find pilot heartbeat file: {path}')

    # store the workernode map
    try:
        path = os.path.join(pilot_home, config.Workernode.map)
        if os.path.exists(path):
            copy(path, tardir)
        path = os.path.join(pilot_home, config.Workernode.gpu_map)
        if os.path.exists(path):
            copy(path, tardir)
    except (NoSuchFile, FileHandlingFailure) as exc:
        logger.warning(f'failed to copy workernode map: {exc}')


def get_tar_timeout(dirsize: float) -> int:
    """
    Get a proper time-out limit based on the directory size.

    It should also handle the case dirsize=None and return the max timeout.

    :param dirsize: directory size (float)
    :return: time-out in seconds (int).
    """
    timeout_max = 3 * 3600  # 3 hours
    timeout_min = 30
    timeout = timeout_min + int(60.0 + dirsize / 5.0) if dirsize else timeout_max

    return min(timeout, timeout_max)


def _do_stageout(job: JobData, args: object, xdata: list, activity: list, title: str, ipv: str = 'IPv6') -> bool:
    """
    Use the `StageOutClient` in the Data API to perform stage-out.

    The rucio host is internally set by Rucio via the client config file. This can be set directly as a pilot option
    --rucio-host.

    :param job: job object (JobData)
    :param args: pilot args object (object)
    :param xdata: list of FileSpec objects (list)
    :param activity: copytool activity or preferred list of activities to resolve copytools (list)
    :param title: type of stage-out (output, log) (str)
    :param ipv: internet protocol version (str)
    :return: True in case of success transfers, False otherwise (bool).
    """
    logger.info('prepare to stage-out %d %s file(s)', len(xdata), title)
    label = 'stage-out'

    # should stage-in be done by a script (for containerisation) or by invoking the API (ie classic mode)?
    use_container = use_middleware_script(job.infosys.queuedata.container_type.get("middleware"))
    # use_container = use_middleware_script(job.infosys.queuedata.container_type.get("middleware")) if title != 'log' else False

    # switch the X509_USER_PROXY on unified dispatch queues (restore later in this function)
    x509_unified_dispatch = os.environ.get('X509_UNIFIED_DISPATCH', '')
    x509_org = os.environ.get('X509_USER_PROXY', '')
    if x509_unified_dispatch and os.path.exists(x509_unified_dispatch):
        os.environ['X509_USER_PROXY'] = x509_unified_dispatch
        logger.info(f'switched proxy on unified dispatch queue: X509_USER_PROXY={x509_unified_dispatch}')
    else:
        logger.debug(f'will not switch proxy since X509_UNIFIED_DISPATCH={x509_unified_dispatch}, '
                     f'os.path.exists(x509_unified_dispatch)={os.path.exists(x509_unified_dispatch)}, '
                     f'X509_USER_PROXY={x509_org}')

    if use_container:
        logger.info('stage-out will be done in a container')
        try:
            eventtype, localsite, remotesite = get_trace_report_variables(job, label=label)
            containerise_middleware(job, args, xdata, eventtype, localsite, remotesite,
                                    label=label,
                                    container_type=job.infosys.queuedata.container_type.get("middleware"))
        except PilotException as error:
            logger.warning('stage-out containerisation threw a pilot exception: %s', error)
        except Exception as error:
            logger.warning('stage-out containerisation threw an exception: %s', error)
    else:
        try:
            logger.info('stage-out will not be done in a container')

            # create the trace report
            trace_report = create_trace_report(job, label=label)

            client = StageOutClient(job.infosys, logger=logger, trace_report=trace_report, ipv=ipv, workdir=job.workdir)
            kwargs = {'workdir': job.workdir, 'cwd': job.workdir, 'usecontainer': False, 'job': job,
                      'output_dir': args.output_dir, 'catchall': job.infosys.queuedata.catchall,
                      'rucio_host': args.rucio_host}  #, mode='stage-out')
            #is_unified = job.infosys.queuedata.type == 'unified'
            # prod analy unification: use destination preferences from PanDA server for unified queues
            #if not is_unified:
            #    client.prepare_destinations(xdata, activity)  ## FIX ME LATER: split activities: for astorages and for copytools (to unify with ES workflow)

            ## FIX ME LATER: split activities: for `astorages` and `copytools` (to unify with ES workflow)
            client.prepare_destinations(xdata, activity, alt_exclude=list(filter(None, [job.nucleus])))

            altstageout = job.allow_altstageout()
            client.transfer(xdata, activity, raise_exception=not altstageout, **kwargs)
            remain_files = [entry for entry in xdata if entry.require_transfer()]
            # check if alt stageout can be applied (all remain files must have alt storage declared ddmendpoint_alt)
            has_altstorage = all(entry.ddmendpoint_alt and entry.ddmendpoint != entry.ddmendpoint_alt for entry in remain_files)

            logger.info('alt stage-out settings: %s, allow_altstageout=%s, remain_files=%s, has_altstorage=%s',
                        activity, altstageout, len(remain_files), has_altstorage)

            if altstageout and remain_files and has_altstorage:  # apply alternative stageout for failed transfers
                for entry in remain_files:
                    entry.ddmendpoint = entry.ddmendpoint_alt
                    entry.ddmendpoint_alt = None
                    entry.is_altstaged = True

                logger.info('alt stage-out will be applied for remain=%s files (previously failed)', len(remain_files))
                f = [entry.lfn for entry in remain_files]
                job.piloterrordiags.append(f'Alternative stage-out for {f}')
                client.transfer(xdata, activity, **kwargs)

        except PilotException as error:
            error_msg = traceback.format_exc()
            logger.error(error_msg)
            msg = errors.format_diagnostics(error.get_error_code(), error_msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code(), msg=msg)
        except Exception:
            logger.error(traceback.format_exc())
            # do not raise the exception since that will prevent also the log from being staged out
            # error = PilotException("stageOut failed with error=%s" % e, code=ErrorCodes.STAGEOUTFAILED)
        else:
            logger.debug('stage-out client completed')

    if x509_unified_dispatch and os.path.exists(x509_unified_dispatch):
        os.environ['X509_USER_PROXY'] = x509_org
        os.environ['X509_UNIFIED_DISPATCH'] = ''
        remove(x509_unified_dispatch)
        logger.info(f'switched back proxy on unified dispatch queue: X509_USER_PROXY={x509_org} (reset X509_UNIFIED_DISPATCH)')

    logger.info('summary of transferred files:')
    for entry in xdata:
        logger.info(" -- lfn=%s, status_code=%s, status=%s", entry.lfn, entry.status_code, entry.status or "(not transferred)")

    remain_files = [entry for entry in xdata if entry.status not in ['transferred']]

    return not remain_files


def _stage_out_new(job: JobData, args: object) -> bool:
    """
    Stage out all output files.

    If job.stageout=log then only log files will be transferred.

    :param job: job object (JobData)
    :param args: pilot args object (object)
    :return: True in case of success, False otherwise (bool).
    """
    #logger.info('testing sending SIGUSR1')
    #import signal
    #os.kill(os.getpid(), signal.SIGUSR1)

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_PRE_STAGEOUT, time.time(), args)

    is_success = True

    if not job.outdata or job.is_eventservice:
        logger.info('this job does not have any output files, only stage-out log file')
        job.stageout = 'log'

    is_unified = job.infosys.queuedata.type == 'unified'
    is_analysis = job.is_analysis()
    activities = ['write_lan_analysis', 'write_lan', 'w'] if is_unified and is_analysis else ['write_lan', 'w']

    if job.stageout != 'log':  ## do stage-out output files
        if not _do_stageout(job, args, job.outdata, activities, title='output',
                            ipv=args.internet_protocol_version):
            is_success = False
            logger.warning('transfer of output file(s) failed')

    if job.stageout in {'log', 'all'} and job.logdata:  ## do stage-out log files
        # prepare log file, consider only 1st available log file
        status = job.get_status('LOG_TRANSFER')
        if status != LOG_TRANSFER_NOT_DONE:
            logger.warning('log transfer already attempted')
            return False

        job.status['LOG_TRANSFER'] = LOG_TRANSFER_IN_PROGRESS
        logfile = job.logdata[0]

        # write time stamps to pilot timing file
        current_time = time.time()
        add_to_pilot_timing(job.jobid, PILOT_PRE_LOG_TAR, current_time, args)

        try:
            tarball_name = f'tarball_PandaJob_{job.jobid}_{job.infosys.pandaqueue}'
            create_log(job.workdir, logfile.lfn, tarball_name, args.cleanup,
                       input_files=[fspec.lfn for fspec in job.indata],
                       output_files=[fspec.lfn for fspec in job.outdata],
                       piloterrors=job.piloterrorcodes, debugmode=job.debug)
        except LogFileCreationFailure as error:
            logger.warning(f'failed to create tar file: {error}')
            set_pilot_state(job=job, state="failed")
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.LOGFILECREATIONFAILURE)
            return False
        except PilotException as error:
            logger.warning(f'failed to create tar file: {error}')
            set_pilot_state(job=job, state="failed")
            if 'timed out' in error.get_detail():
                delta = int(time.time() - current_time)
                msg = f'tar command for log file creation timed out after {delta} s: {error.get_detail()}'
            else:
                msg = error.get_detail()
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code(), msg=msg)
            return False

        # write time stamps to pilot timing file
        add_to_pilot_timing(job.jobid, PILOT_POST_LOG_TAR, time.time(), args)

        if not _do_stageout(job, args, [logfile], ['pl'] + activities, title='log',
                            ipv=args.internet_protocol_version):
            is_success = False
            logger.warning('log transfer failed')
            job.status['LOG_TRANSFER'] = LOG_TRANSFER_FAILED
        else:
            job.status['LOG_TRANSFER'] = LOG_TRANSFER_DONE
    elif not job.logdata:
        logger.info('no log was defined - will not create log file')
        job.status['LOG_TRANSFER'] = LOG_TRANSFER_DONE

    # write time stamps to pilot timing file
    add_to_pilot_timing(job.jobid, PILOT_POST_STAGEOUT, time.time(), args)

    # generate fileinfo details to be sent to Panda
    job.fileinfo = generate_fileinfo(job)

    # WARNING THE FOLLOWING RESETS ANY PREVIOUS STAGEOUT ERRORS
    if not is_success:
        # set error code + message (a more precise error code might have been set already)
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTFAILED)
        set_pilot_state(job=job, state="failed")
        logger.warning('stage-out failed')
        return False

    logger.info('stage-out finished correctly')

    if not job.state or (job.state and job.state == 'stageout'):  # is the job state already set? if so, don't change the state (unless it's the stageout state)
        logger.debug(f'changing job state from {job.state} to finished')
        set_pilot_state(job=job, state="finished")

    return is_success


def generate_fileinfo(job: JobData) -> dict:
    """
    Generate fileinfo details to be sent to Panda.

    :param job: job object (JobData)
    :return: file info (dict).
    """
    fileinfo = {}
    checksum_type = config.File.checksum_type if config.File.checksum_type == 'adler32' else 'md5sum'
    for entry in job.outdata + job.logdata:
        if entry.status in {'transferred'}:
            dat = {
                'guid': entry.guid,
                'fsize': entry.filesize,
                f'{checksum_type}': entry.checksum.get(config.File.checksum_type),
                'surl': entry.turl
            }
            if True or entry.is_altstaged:  # always report output RSE (ATLASPANDA-604)
                dat['endpoint'] = entry.ddmendpoint

            fileinfo[entry.lfn] = dat

    return fileinfo


def queue_monitoring(queues: namedtuple, traces: Any, args: object):
    """
    Monitor data queues.

    Thread.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    while True:  # will abort when graceful_stop has been set
        time.sleep(0.5)
        if traces.pilot['command'] == 'abort':
            logger.warning('data queue monitor saw the abort instruction')
            args.graceful_stop.set()

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        # (abort at the end of the loop)
        abort = should_abort(args, label='data:queue_monitoring')

        # monitor the failed_data_in queue
        try:
            job = queues.failed_data_in.get(block=True, timeout=1)
        except queue.Empty:
            pass
        else:
            # stage-out log file then add the job to the failed_jobs queue
            job.stageout = "log"
            if not _stage_out_new(job, args):
                logger.info("job %s failed during stage-in and stage-out of log, adding job object to failed_data_outs queue", job.jobid)
                put_in_queue(job, queues.failed_data_out)
            else:
                logger.info("job %s failed during stage-in, adding job object to failed_jobs queue", job.jobid)
                put_in_queue(job, queues.failed_jobs)

        # monitor the finished_data_out queue
        try:
            job = queues.finished_data_out.get(block=True, timeout=1)
        except queue.Empty:
            pass
        else:
            # use the payload/transform exitCode from the job report if it exists
            if job.transexitcode == 0 and job.exitcode == 0 and job.piloterrorcodes == []:
                logger.info('finished stage-out for finished payload, adding job to finished_jobs queue')
                #queues.finished_jobs.put(job)
                put_in_queue(job, queues.finished_jobs)
            else:
                logger.info('finished stage-out (of log) for failed payload')
                #queues.failed_jobs.put(job)
                put_in_queue(job, queues.failed_jobs)

        # monitor the failed_data_out queue
        try:
            job = queues.failed_data_out.get(block=True, timeout=1)
        except queue.Empty:
            pass
        else:
            # attempt to upload the log in case the previous stage-out failure was not an SE error
            job.stageout = "log"
            set_pilot_state(job=job, state="failed")
            if not _stage_out_new(job, args):
                logger.info("job %s failed during stage-out", job.jobid)

            put_in_queue(job, queues.failed_jobs)

        if abort:
            break

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='queue_monitoring'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[data] queue_monitor thread has finished')
