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
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-17
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-25
# - Wen Guan, wen.guan@cern.ch, 2018

"""Job module with functions for job handling."""

import os
import time
import hashlib
import logging
import queue
import random
from collections import namedtuple
from json import dumps
from glob import glob
from typing import Any
from urllib.parse import parse_qsl

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    ExcThread,
    FileHandlingFailure,
    PilotException,
)
from pilot.common.pilotcache import get_pilot_cache
from pilot.info import (
    infosys,
    InfoService,
    JobData,
    JobInfoProvider
)
from pilot.util import https
from pilot.util.activemq import ActiveMQ
from pilot.util.auxiliary import (
    check_for_final_server_update,
    encode_globaljobid,
    get_batchsystem_jobid,
    # get_display_info,
    get_job_scheduler_id,
    get_pilot_state,
    has_instruction_sets,
    is_virtual_machine,
    locate_core_file,
    pilot_version_banner,
    set_pilot_state,
)
from pilot.util.config import config
from pilot.util.common import (
    should_abort,
    was_pilot_killed
)
from pilot.util.constants import (
    PILOT_MULTIJOB_START_TIME,
    PILOT_PRE_GETJOB,
    PILOT_POST_GETJOB,
    PILOT_KILL_SIGNAL,
    LOG_TRANSFER_NOT_DONE,
    LOG_TRANSFER_IN_PROGRESS,
    LOG_TRANSFER_DONE,
    LOG_TRANSFER_FAILED,
    SERVER_UPDATE_TROUBLE,
    SERVER_UPDATE_FINAL,
    SERVER_UPDATE_UPDATING,
    SERVER_UPDATE_NOT_DONE
)
from pilot.util.container import execute
from pilot.util.filehandling import (
    copy,
    create_symlink,
    find_text_files,
    get_total_input_size,
    is_json,
    remove,
    tail,
    write_file,
    write_json,
)
from pilot.util.harvester import (
    is_harvester_mode,
    get_event_status_file,
    get_worker_attributes_file,
    parse_job_definition_file,
    publish_job_report,
    publish_stageout_files,
    publish_work_report,
    remove_job_request_file,
    request_new_jobs,
)
from pilot.util.jobmetrics import get_job_metrics
from pilot.util.loggingsupport import establish_logging
from pilot.util.math import mean, float_to_rounded_string
from pilot.util.middleware import containerise_general_command
from pilot.util.monitoring import (
    check_local_space,
    job_monitor_tasks,
)
from pilot.util.monitoringtime import MonitoringTime
from pilot.util.processes import (
    cleanup,
    kill_defunct_children,
    kill_process,
    kill_processes,
    threads_aborted,
)
from pilot.util.proxy import get_distinguished_name
from pilot.util.queuehandling import (
    purge_queue,
    put_in_queue,
    queue_report,
    scan_for_jobs,
)
from pilot.util.realtimelogger import cleanup as rtcleanup
from pilot.util.timing import (
    add_to_pilot_timing,
    get_postgetjob_time,
    get_time_since,
    get_time_since_start,
    time_stamp,
    timing_report,
)
from pilot.util.workernode import (
    collect_workernode_info,
    get_cpu_info,
    get_cpu_model,
    get_disk_space,
    get_node_name,
    update_modelstring,
    extract_site_and_schedd
)

errors = ErrorCodes()
logger = logging.getLogger(__name__)
pilot_cache = get_pilot_cache()


def control(queues: namedtuple, traces: Any, args: object):
    """
    Set up job control threads.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    targets = {'validate': validate, 'retrieve': retrieve, 'create_data_payload': create_data_payload,
               'queue_monitor': queue_monitor, 'job_monitor': job_monitor, 'fast_job_monitor': fast_job_monitor,
               'message_listener': message_listener}
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
        logger.info('all job control threads have been joined')

    logger.debug('job control ending since graceful_stop has been set')
    if args.abort_job.is_set():
        if traces.pilot['command'] == 'aborting':
            logger.warning('jobs are aborting')
        elif traces.pilot['command'] == 'abort':
            logger.warning('job control detected a set abort_job (due to a kill signal)')
            traces.pilot['command'] = 'aborting'

            # find all running jobs and stop them, find all jobs in queues relevant to this module
            #abort_jobs_in_queues(queues, args.signal)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='control'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[job] control thread has finished')
    # test kill signal during end of generic workflow
    #import signal
    #os.kill(os.getpid(), signal.SIGBUS)


def _validate_job(job: Any) -> bool:
    """
    Verify job parameters for specific problems.

    :param job: job object (Any)
    :return: True if job has been verified, False otherwise (bool).
    """
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
    container = __import__(f'pilot.user.{pilot_user}.container', globals(), locals(), [user], 0)

    # should a container be used for the payload?
    try:
        kwargs = {'job': job}
        job.usecontainer = container.do_use_container(**kwargs)
    except Exception as error:
        logger.warning(f'exception caught: {error}')

    return user.verify_job(job)


def verify_error_code(job: Any):
    """
    Make sure an error code is properly set.

    This makes sure that job.piloterrorcode is always set for a failed/holding job, that not only
    job.piloterrorcodes are set but not job.piloterrorcode. This function also negates the sign of the error code
    and sets job state 'holding' (instead of 'failed') if the error is found to be recoverable by a later job (user
    jobs only).

    :param job: job object (Any).
    """
    if job.piloterrorcode == 0 and len(job.piloterrorcodes) > 0:
        logger.warning(f'piloterrorcode set to first piloterrorcodes list entry: {job.piloterrorcodes}')
        job.piloterrorcode = job.piloterrorcodes[0]

    if job.piloterrorcode != 0 and job.is_analysis():
        if errors.is_recoverable(code=job.piloterrorcode):
            job.piloterrorcode = -abs(job.piloterrorcode)
            job.state = 'failed'
            logger.info(f'failed user job is recoverable (error code={job.piloterrorcode})')
        else:
            logger.info('failed user job is not recoverable')
    else:
        logger.info('verified error code')


def get_proper_state(job: Any, state: str) -> str:
    """
    Return a proper job state to send to server.

    This function should only return 'starting', 'running', 'finished', 'holding' or 'failed'.
    If the internal job.serverstate is not yet set, it means it is the first server update, ie 'starting' should be
    sent.

    :param job: job object (Any)
    :param state: internal pilot state (str)
    :return: valid server state (str).
    """
    if job.serverstate in {'finished', 'failed'}:
        pass
    elif job.serverstate == "" and state != "finished" and state != "failed":
        job.serverstate = 'starting'
    elif state in {'finished', 'failed', 'holding'}:
        job.serverstate = state
    else:
        job.serverstate = 'running'

    return job.serverstate


def publish_harvester_reports(state: str, args: Any, data: dict, job: Any, final: bool) -> bool:
    """
    Publish all reports needed by Harvester.

    :param state: job state (str)
    :param args: pilot args object (Any)
    :param data: data structure for server update (dict)
    :param job: job object (Any)
    :param final: is this the final update? (bool)
    :return: True if successful, False otherwise (bool).
    """
    # write part of the heartbeat message to worker attributes files needed by Harvester
    path = get_worker_attributes_file(args)

    # add jobStatus (state) for Harvester
    data['jobStatus'] = state

    # publish work report
    if not publish_work_report(data, path):
        logger.debug(f'failed to write to workerAttributesFile: {path}')
        return False

    # check if we are in final state then write out information for output files
    if final:
        # Use the job information to write Harvester event_status.dump file
        event_status_file = get_event_status_file(args)
        if publish_stageout_files(job, event_status_file):
            logger.debug(f'wrote log and output files to file: {event_status_file}')
        else:
            logger.warning(f'could not write log and output files to file: {event_status_file}')
            return False

        # publish job report
        _path = os.path.join(job.workdir, config.Payload.jobreport)
        if os.path.exists(_path):
            if publish_job_report(job, args, config.Payload.jobreport):
                logger.debug('wrote job report file')
                return True

            logger.warning('failed to write job report file')
            return False
    else:
        logger.info('finished writing various report files in Harvester mode')

    return True


def write_heartbeat_to_file(data: dict) -> bool:
    """
    Write heartbeat dictionary to file.

    This is only done when server updates are not wanted.

    :param data: server data (dict)
    :return: True if successful, False otherwise (bool).
    """
    path = os.path.join(os.environ.get('PILOT_HOME'), config.Pilot.heartbeat_message)
    if write_json(path, data):
        logger.debug(f'heartbeat dictionary: {data}')
        logger.debug(f'wrote heartbeat to file: {path}')
        return True

    return False


def is_final_update(job: Any, state: str, tag: str = 'sending') -> bool:
    """
    Determine if it will be the final server update.

    :param job: job object (Any)
    :param state: job state (str)
    :param tag: optional tag ('sending'/'writing') (str)
    :return: final state (bool).
    """
    # make sure that the log transfer has been attempted
    log_transfer = get_job_status(job, 'LOG_TRANSFER')
    actual_state = state
    if log_transfer in {LOG_TRANSFER_DONE, LOG_TRANSFER_FAILED}:
        logger.info(f'log transfer has been attempted: {log_transfer}')
    elif not job.logdata:
        # make sure that there should actually be a log transfer (i.e. is there a known log file defined in the job def)
        logger.info('no logdata defined in job definition - no log transfer will be attempted')
    else:
        logger.info(f'log transfer has not been attempted: {log_transfer}')
        state = 'not_ready_for_final_state'

    if state in {'finished', 'failed', 'holding'}:
        final = True
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_UPDATING
        logger.info(f'job {job.jobid} has {state} - {tag} final server update')

        # make sure that job.state is 'failed' if there's a set error code
        if job.piloterrorcode or job.piloterrorcodes:
            logger.warning('making sure that job.state is set to failed since a pilot error code is set')
            state = 'failed'
            job.state = state
        # make sure an error code is properly set
        elif state != 'finished':
            verify_error_code(job)
    else:
        final = False
        logger.info(f'job {job.jobid} has state \'{actual_state}\' - {tag} heartbeat')

    return final


def send_state(job: Any, args: Any, state: str, xml: str = "", metadata: str = "",
               test_tobekilled: bool = False) -> bool:
    """
    Update the server (send heartbeat message).

    Interpret and handle any server instructions arriving with the updateJob back channel.

    :param job: job object (Any)
    :param args: Pilot arguments (e.g. containing queue name, queuedata dictionary, etc) (Any)
    :param state: job state (str)
    :param xml: optional metadata xml (str)
    :param metadata: job report metadata read as a string (str)
    :param test_tobekilled: emulate a tobekilled command (bool)
    :return: True if successful, False otherwise (bool).
    """
    # insert out of batch time error code if MAXTIME has been reached
    if os.environ.get('REACHED_MAXTIME', None):
        msg = 'the max batch system time limit has been reached'
        logger.warning(msg)
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.REACHEDMAXTIME, msg=msg)
        state = 'failed'
        job.state = state

    state = get_proper_state(job, state)
    if state in {'finished', 'holding', 'failed'}:
        logger.info(f'this job has now completed (state={state})')
        # job.completed = True  - do not set that here (only after the successful final server update)
    elif args.pod and args.workflow == 'stager':
        state = 'running'  # stager pods should only send 'running' since harvester already has set the 'starting' state
        job.state = state

    # should the pilot make any server updates?
    if not args.update_server:
        logger.info('pilot will not update the server (heartbeat message will be written to file)')

    # will it be the final update?
    final = is_final_update(job, state, tag='sending' if args.update_server else 'writing')

    # build the data structure needed for updateJob
    data = get_data_structure(job, state, args, xml=xml, metadata=metadata)
    logger.debug(f'data={data}')

    # write the heartbeat message to file if the server is not to be updated by the pilot (Nordugrid mode)
    if not args.update_server:
        # if in harvester mode write to files required by harvester
        if is_harvester_mode(args):
            return publish_harvester_reports(state, args, data, job, final)

        # store the file in the main workdir
        return write_heartbeat_to_file(data)

    if config.Pilot.pandajob != 'real':
        logger.info('skipping job update for fake test job')
        return True

    res = https.send_update('updateJob', data, args.url, args.port, job=job, ipv=args.internet_protocol_version)
    if res is not None:
        # update the last heartbeat time
        args.last_heartbeat = time.time()

        # to test 'tobekilled' server instruction - pilot will abort after stage-in
        #if state == 'running':
        #    test_tobekilled = True

        # does the server update contain any backchannel information? if so, update the job object
        handle_backchannel_command(res, job, args, test_tobekilled=test_tobekilled)

        if final:  # and os.path.exists(job.workdir):  # ignore if workdir doesn't exist - might be a delayed jobUpdate
            os.environ['SERVER_UPDATE'] = SERVER_UPDATE_FINAL

        if final and state in {'finished', 'holding', 'failed'}:
            logger.info(f'setting job as completed (state={state})')
            job.completed = True

        return True

    if final:
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_TROUBLE

    return False


def get_job_status_from_server(job_id: int, url: str, port: int) -> (str, int, int):
    """
    Return the current status of job <jobId> from the dispatcher.

    typical dispatcher response: 'status=finished&StatusCode=0'
    StatusCode  0: succeeded
               10: time-out
               20: general error
               30: failed
    In the case of time-out, the dispatcher will be asked one more time after 10 s.

    :param job_id: PanDA job id (int)
    :param url: PanDA server URL (int)
    :param port: PanDA server port (str)
    :return: status (string; e.g. holding), attempt_nr (int), status_code (int).
    """
    status = 'unknown'
    attempt_nr = 0
    status_code = 0
    if config.Pilot.pandajob == 'fake':
        return status, attempt_nr, status_code

    data = {}
    data['ids'] = job_id

    # get the URL for the PanDA server from pilot options or from config
    pandaserver = https.get_panda_server(url, port)

    # ask dispatcher about lost job status
    trial = 1
    max_trials = 2

    while trial <= max_trials:
        try:
            # open connection
            ret = https.request2(f'{pandaserver}/server/panda/getStatus', data=data)
            logger.debug(f"request2 response: {ret}")
            if not ret or isinstance(ret, str):
                ret = https.request(f'{pandaserver}/server/panda/getStatus', data=data)

            if isinstance(ret, str):  # string response in case of time-out
                logger.warning(f"dispatcher did not return allowed values: {ret}")
                status = "unknown"
                attempt_nr = -1
                status_code = 20
                continue

            response = ret[1]
            logger.info(f"response: {response}")
            if response:
                try:
                    # decode the response
                    # eg. var = ['status=notfound', 'attemptNr=0', 'StatusCode=0']
                    # = response

                    status = response['status']  # e.g. 'holding'
                    attempt_nr = int(response['attemptNr'])  # e.g. '0'
                    status_code = int(response['StatusCode'])  # e.g. '0'
                except Exception as error:
                    logger.warning(
                        f"exception: dispatcher did not return allowed values: {ret}, {error}")
                    status = "unknown"
                    attempt_nr = -1
                    status_code = 20
                else:
                    logger.debug(f'server job status={status}, attempt_nr={attempt_nr}, status_code={status_code}')
            else:
                logger.warning(f"dispatcher did not return allowed values: {ret}")
                status = "unknown"
                attempt_nr = -1
                status_code = 20
        except Exception as error:
            logger.warning(f"could not interpret job status from dispatcher: {error}")
            status = 'unknown'
            attempt_nr = -1
            status_code = -1
            break
        else:
            if status_code == 0:  # success
                break
            if status_code == 10:  # time-out
                trial += 1
                time.sleep(10)
                continue
            if status_code == 20:  # other error
                if ret[0] == 13056 or ret[0] == '13056':
                    logger.warning(f"wrong certificate used with curl operation? (encountered error {ret[0]})")
                break

            # general error
            break

    return status, attempt_nr, status_code


def get_debug_command(cmd: str) -> (bool, str):
    """
    Identify and filter the given debug command.

    Note: only a single command will be allowed from a predefined list: tail, ls, gdb, ps, du.

    :param cmd: raw debug command from job definition (str)
    :return: True if command is deemed ok, False otherwise (bool), debug_command (str).
    """
    debug_mode = False
    debug_command = ""

    allowed_commands = ['tail', 'ls', 'ps', 'gdb', 'du']
    forbidden_commands = ['rm']

    # remove any 'debug,' command that the server might send redundantly
    if ',' in cmd and 'debug' in cmd:
        cmd = cmd.replace('debug,', '').replace(',debug', '')
    try:
        tmp = cmd.split(' ')
        com = tmp[0]
    except Exception as error:
        logger.warning(f'failed to identify debug command: {error}')
    else:
        if com not in allowed_commands:
            logger.warning(f'command={com} is not in the list of allowed commands: {allowed_commands}')
        elif ';' in cmd or '&#59' in cmd:
            logger.warning(f'debug command cannot contain \';\': \'{cmd}\'')
        elif com in forbidden_commands:
            logger.warning(f'command={com} is not allowed')
        else:
            debug_mode = True
            debug_command = cmd

    return debug_mode, debug_command


def handle_backchannel_command(res: dict, job: Any, args: Any, test_tobekilled: bool = False) -> None:
    """
    Checkk if the server update contain any backchannel information. If so, update the job object.

    :param res: server response (dict)
    :param job: job object (Any)
    :param args: pilot args object (Any)
    :param test_tobekilled: emulate a tobekilled command (bool)
    """
    if test_tobekilled:
        logger.info('faking a \'tobekilled\' command')
        res['command'] = 'tobekilled'

    if 'pilotSecrets' in res:
        try:
            job.pilotsecrets = res.get('pilotSecrets')
        except Exception as exc:
            logger.warning(f'failed to parse pilotSecrets: {exc}')

    if 'command' in res and res.get('command') != 'NULL':
        # warning: server might return comma-separated string, 'debug,tobekilled'
        cmd = res.get('command')

        # for testing debug command: cmd = 'tail pilotlog.txt'

        # is it a 'command options'-type? debug_command=tail .., ls .., gdb .., ps .., du ..
        if ' ' in cmd and 'tobekilled' not in cmd:
            job.debug, job.debug_command = get_debug_command(cmd)
        elif 'tobekilled' in cmd:
            logger.info(f'pilot received a panda server signal to kill job {job.jobid} at {time_stamp()}')
            if not os.path.exists(job.workdir):  # jobUpdate might be delayed - do not cause problems for new downloaded job
                logger.warning(f'job.workdir ({job.workdir}) does not exist - ignore kill instruction')
                return
            if args.workflow == 'stager':
                logger.info('will set interactive job to finished (server will override this, but harvester will not)')
                set_pilot_state(job=job, state="finished")  # this will let pilot finish naturally and report exit code 0 to harvester
            else:
                set_pilot_state(job=job, state="failed")
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PANDAKILL)
            if job.pid:
                logger.debug('killing payload process')
                kill_process(job.pid)
            args.abort_job.set()
        elif 'softkill' in cmd:
            logger.info(f'pilot received a panda server signal to softkill job {job.jobid} at {time_stamp()}')
            # event service kill instruction
            job.debug_command = 'softkill'
        elif 'debug' in cmd:
            logger.info('pilot received a command to turn on standard debug mode from the server')
            job.debug = True
            job.debug_command = 'debug'
        elif 'debugoff' in cmd:
            logger.info('pilot received a command to turn off debug mode from the server')
            job.debug = False
            job.debug_command = 'debugoff'
        elif 'nocleanup' in cmd:
            logger.info('pilot received a command to turn off workdir cleanup')
            args.cleanup = False
        else:
            logger.warning(f'received unknown server command via backchannel: {cmd}')

    # for testing debug mode
    # job.debug = True
    # job.debug_command = 'du -sk'
    # job.debug_command = 'tail -30 payload.stdout'
    # job.debug_command = 'ls -ltr workDir'  # not really tested
    # job.debug_command = 'ls -ltr %s' % job.workdir
    # job.debug_command = 'ps -ef'
    # job.debug_command = 'ps axo pid,ppid,pgid,args'
    # job.debug_command = 'gdb --pid % -ex \'generate-core-file\''


def add_data_structure_ids(data: dict, version_tag: str, job: Any) -> dict:
    """
    Add pilot, batch and scheduler ids to the data structure for getJob, updateJob.

    :param data: data structure (dict)
    :param version_tag: Pilot version tag (str)
    :param job: job object (Any)
    :return: updated data structure (dict).
    """
    schedulerid = get_job_scheduler_id()
    if schedulerid:
        data['schedulerID'] = schedulerid

    # update the jobid in the pilotid if necessary (not for ATLAS since there should be one batch log for all multi-jobs)
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
    pilotid = user.get_pilot_id(data['jobId'])
    if pilotid:
        pilotversion = os.environ.get('PILOT_VERSION')
        # report the batch system job id, if available
        if not job.batchid:
            job.batchtype, job.batchid = get_batchsystem_jobid()
        if job.batchtype and job.batchid:
            data['pilotID'] = f"{pilotid}|{job.batchtype}|{version_tag}|{pilotversion}"
            data['batchID'] = job.batchid
        else:
            data['pilotID'] = f"{pilotid}|{version_tag}|{pilotversion}"
    else:
        logger.warning('pilotid not available')

    return data


def get_data_structure(job: Any, state: str, args: Any, xml: str = "", metadata: str = "") -> dict:  # noqa: C901
    """
    Build the data structure needed for updateJob.

    :param job: job object (Any)
    :param state: state of the job (str)
    :param args: Pilot args object (Any)
    :param xml: optional XML string (str)
    :param metadata: job report metadata read as a string (str)
    :return: data structure (dict).
    """
    data = {'jobId': job.jobid,
            'state': state,
            'timestamp': time_stamp(),
            'siteName': os.environ.get('PILOT_SITENAME'),  # args.site,
            'node': get_node_name(),
            'attemptNr': job.attemptnr}

    # add pilot, batch and scheduler ids to the data structure
    data = add_data_structure_ids(data, args.version_tag, job)

    starttime = get_postgetjob_time(job.jobid, args)
    if starttime:
        data['startTime'] = starttime

    if xml is not None:
        data['xml'] = xml
    if metadata is not None:
        data['metaData'] = metadata

    # in debug mode, also send a tail of the latest log file touched by the payload
    if job.debug and job.debug_command:
        data['stdout'] = process_debug_mode(job)

    # add the core count
    if job.corecount and job.corecount != 'null' and job.corecount != 'NULL':
        data['coreCount'] = job.corecount
    if job.corecounts:
        _mean = mean(job.corecounts)
        logger.info(f'mean actualcorecount: {_mean}')
        data['meanCoreCount'] = _mean

    # get the number of events, should report in heartbeat in case of preempted.
    if job.nevents != 0:
        data['nEvents'] = job.nevents
        logger.info(f"total number of processed events: {job.nevents} (read)")
    else:
        logger.info("payload/TRF did not report the number of read events")

    # get the CPU consumption time
    constime = get_cpu_consumption_time(job.cpuconsumptiontime)
    if constime and constime != -1:
        data['cpuConsumptionTime'] = constime
        data['cpuConversionFactor'] = job.cpuconversionfactor
    number_of_cores, ht, sockets, cpu_mhz, _, _, _, cpu_arch_level = get_cpu_info()  # get from a cache
    cpumodel = get_cpu_model()  # ARM info will be corrected below if necessary (otherwise cpumodel will contain UNKNOWN)
    if number_of_cores:
        cpumodel = update_modelstring(cpumodel, number_of_cores, ht, sockets)  # add the CPU cores if not present
    data['cpuConsumptionUnit'] = job.cpuconsumptionunit + "+" + cpumodel
    logger.debug(f"got CPU MHz: {cpu_mhz}")
    if cpu_mhz:
        job.cpufrequencies.append(cpu_mhz)

    # CPU instruction set
    instruction_sets = has_instruction_sets(['AVX2'])
    # if the product and vendor info is needed, better to cache it since it is expensive to get
    # product, vendor = get_display_info()
    if instruction_sets:
        if 'cpuConsumptionUnit' in data:
            data['cpuConsumptionUnit'] += '+' + instruction_sets
        else:
            data['cpuConsumptionUnit'] = instruction_sets
        #if product and vendor:
        #    logger.debug(f'cpuConsumptionUnit: could have added: product={product}, vendor={vendor}')

    # CPU architecture level
    if cpu_arch_level:
        data['cpu_architecture_level'] = cpu_arch_level
        # correct the cpuConsumptionUnit on ARM since cpumodel and cache won't be reported
        if cpu_arch_level.startswith('ARM') and 'UNKNOWN' in data['cpuConsumptionUnit']:
            data['cpuConsumptionUnit'] = data['cpuConsumptionUnit'].replace('UNKNOWN', 'ARM')

    # add memory information if available
    add_memory_info(data, job.workdir, name=job.memorymonitor)

    # job metrics

    # add read_bytes from memory monitor to job metrics if available
    extra = {}
    if 'totRBYTES' in data:
        _totalsize = get_total_input_size(job.indata, nolib=True)
        logger.debug(f'_totalsize={_totalsize}')
        logger.debug(f"current max read_bytes: {data.get('totRBYTES')}")
        try:
            readfrac = data.get('totRBYTES') / _totalsize
        except (TypeError, ZeroDivisionError) as exc:
            logger.warning(f"failed to calculate totRBYTES / total size of input files = {data.get('totRBYTES')}/{_totalsize}: {exc}")
            logger.warning('will not report readbyterate')
            readfrac = None
        else:
            readfrac = float_to_rounded_string(readfrac, precision=2)
            logger.debug(f'readbyterate={readfrac}')
        if readfrac:
            extra = {'readbyterate': readfrac}
    else:
        logger.debug('read_bytes info not yet available')

    # extract and remove any GPU info from data since it will be reported with job metrics
    add_gpu_info(data, extra)

    # add the lsetup time if set
    if job.lsetuptime:
        extra['lsetup_time'] = job.lsetuptime

    job_metrics = get_job_metrics(job, extra=extra)
    if job_metrics:
        data['jobMetrics'] = job_metrics

    # add timing info if finished or failed
    if state in {'finished', 'failed'}:
        add_timing_and_extracts(data, job, state, args)
        https.add_error_codes(data, job)

    # glidein information, currently only relevant for EIC and generic pilots
    if args.pilot_user.lower() == 'eic' or args.pilot_user.lower() == 'generic':
        glidein_site, remote_schedd_name = extract_site_and_schedd()
        if glidein_site and remote_schedd_name:
            data['source_site'] = remote_schedd_name
            data['destination_site'] = glidein_site

    return data


def add_gpu_info(data: dict, extra: dict):
    """
    Add GPU info to the extra dictionary.

    :param data: data dictionary (dict)
    :param extra: extra dictionary (dict)
    """
    if 'GPU' in data:
        ngpu = 0
        name = ""
        try:
            logger.debug(f'data[GPU]={data["GPU"]}')
            for key in data["GPU"]:
                if key.startswith('gpu_0'):  # ignore any further GPU info, ie assume they are all the same
                    name = data["GPU"][key]['name'].replace(' ', '_')  # NVIDIA A100-SXM4-40GB -> NVIDIA_A100-SXM4-40GB
                elif key == 'nGPU':
                    ngpu = data["GPU"][key]
            if name:
                extra['GPU_name'] = name
            if ngpu:
                extra['nGPU'] = ngpu
            del data['GPU']
        except Exception as exc:
            logger.warning(f'exception caught: {exc}')


def process_debug_mode(job: Any) -> str:
    """
    Handle debug mode - preprocess debug command, get the output and kill the payload in case of gdb.

    :param job: job object (Any)
    :return: stdout from debug command (str).
    """
    # for gdb commands, use the proper gdb version (the system one may be too old)
    if job.debug_command.startswith('gdb '):
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
        user.preprocess_debug_command(job)

    if job.debug_command:
        stdout = get_debug_stdout(job)
        if stdout:
            # in case gdb was successfully used, the payload can now be killed
            if job.debug_command.startswith('gdb ') and job.pid:
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PANDAKILL,
                                                                                 msg='payload was killed after gdb produced requested core file')
                logger.debug('will proceed to kill payload processes')
                kill_processes(job.pid)
    else:
        stdout = ''

    return stdout


def get_debug_stdout(job: Any) -> str:
    """
    Return the requested output from a given debug command.

    :param job: job object (Any)
    :return: output (str).
    """
    if job.debug_command == 'debug':
        return get_payload_log_tail(job.workdir, job.jobid)
    if 'tail ' in job.debug_command:
        return get_requested_log_tail(job.debug_command, job.workdir)
    if 'ls ' in job.debug_command:
        return get_ls(job.debug_command, job.workdir)
    if 'ps ' in job.debug_command or 'gdb ' in job.debug_command:
        return get_general_command_stdout(job)

    # general command, execute and return output
    _, stdout, _ = execute(job.debug_command)
    logger.info(f'debug_command: {job.debug_command}:\n\n{stdout}\n')

    return stdout


def get_general_command_stdout(job: Any):
    """
    Return the output from the requested debug command.

    :param job: job object (Any)
    :return: output (str).
    """
    stdout = ''

    # for gdb, we might have to process the debug command (e.g. to identify the proper pid to debug)
    if 'gdb ' in job.debug_command and '--pid %' in job.debug_command:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
        job.debug_command = user.process_debug_command(job.debug_command, job.jobid)

    if job.debug_command:
        _containerisation = False  # set this with some logic instead - not used for now
        if _containerisation:
            try:
                containerise_general_command(job,
                                             label='general',
                                             container_type='container')
            except PilotException as error:
                logger.warning(f'general containerisation threw a pilot exception: {error}')
            except Exception as error:
                logger.warning(f'general containerisation threw an exception: {error}')
        else:
            _, stdout, stderr = execute(job.debug_command)
            logger.debug(f"{job.debug_command} (stdout):\n\n{stdout}\n\n")
            logger.debug(f"{job.debug_command} (stderr):\n\n{stderr}\n\n")

        # in case a core file was produced, locate it
        path = locate_core_file(cmd=job.debug_command) if 'gdb ' in job.debug_command else ''
        if path:
            # copy it to the working directory (so it will be saved in the log)
            try:
                copy(path, job.workdir)
            except Exception:
                pass

    return stdout


def get_ls(debug_command: str, workdir: str) -> str:
    """
    Return the requested ls debug command.

    :param debug_command: full debug command (str)
    :param workdir: job work directory (str)
    :return: output (str).
    """
    items = debug_command.split(' ')
    # cmd = items[0]
    options = ' '.join(items[1:])
    path = options.rsplit(' ', maxsplit=1)[-1] if ' ' in options else options
    if path.startswith('-'):
        path = '.'
    finalpath = os.path.join(workdir, path)
    debug_command = debug_command.replace(path, finalpath)

    _, stdout, _ = execute(debug_command)
    logger.debug(f"{debug_command}:\n\n{stdout}\n\n")

    return stdout


def get_requested_log_tail(debug_command: str, workdir: str) -> str:
    """
    Return the tail of the requested debug log.

    Examples
      tail workdir/tmp.stdout* <- pilot finds the requested log file in the specified relative path
      tail log.RAWtoALL <- pilot finds the requested log file

    :param debug_command: full debug command (str)
    :param workdir: job work directory (str)
    :return: output (str).
    """
    _tail = ""
    items = debug_command.split(' ')
    cmd = items[0]
    options = ' '.join(items[1:])
    logger.debug(f'debug command: {cmd}')
    logger.debug(f'debug options: {options}')

    # assume that the path is the last of the options; <some option> <some path>
    path = options.rsplit(' ', maxsplit=1)[-1] if ' ' in options else options
    fullpath = os.path.join(workdir, path)

    # find all files with the given pattern and pick the latest updated file (if several)
    files = glob(fullpath)
    if files:
        logger.info(f'files found: {files}')
        _tail = get_latest_log_tail(files)
    else:
        logger.warning(f'did not find \'{path}\' in path {fullpath}')

    if _tail:
        logger.debug(f'tail =\n\n{_tail}\n\n')

    return _tail


def get_cpu_consumption_time(cpuconsumptiontime: int) -> int:
    """
    Get the CPU consumption time.

    The function makes sure that the value exists and is within allowed limits (< 10^9).

    :param cpuconsumptiontime: CPU consumption time (int)
    :return: properly set CPU consumption time (int).
    """
    try:
        constime = int(cpuconsumptiontime)
    except Exception:
        constime = 0
    if constime and constime > 10 ** 9:
        logger.warning(f"unrealistic cpuconsumptiontime: {constime} (reset to -1)")
        constime = -1

    return constime


def add_timing_and_extracts(data: dict, job: Any, state: str, args: Any):
    """
    Add timing info and log extracts to data structure for a completed job (finished or failed) to be sent to server.

    Note: this function updates the data dictionary.

    :param data: data structure (dict)
    :param job: job object (Any)
    :param state: state of the job (str)
    :param args: pilot args object (Any)
    """
    time_getjob, time_stagein, time_payload, time_stageout, time_initial_setup, time_setup, time_log_creation = timing_report(job.jobid, args)
    data['pilotTiming'] = f"{time_getjob}|{time_stagein}|{time_payload}|{time_stageout}|{time_initial_setup}|{time_setup}"
    logger.debug(f'could have reported time_log_creation={time_log_creation} s')

    # add log extracts (for failed/holding jobs or for jobs with outbound connections)
    extracts = ""
    if state in {'failed', 'holding'}:
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.diagnose', globals(), locals(), [pilot_user], 0)
        extracts = user.get_log_extracts(job, state)
        if extracts != "":
            logger.warning(f'\n[begin log extracts]\n{extracts}\n[end log extracts]')
    data['pilotLog'] = extracts[:1024]
    data['endTime'] = time.time()


def add_memory_info(data: dict, workdir: str, name: str = ""):
    """
    Add memory information (if available) to the data structure that will be sent to the server with job updates.

    Note: this function updates the data dictionary.

    :param data: data structure (dict)
    :param workdir: working directory of the job (str)
    :param name: name of memory monitor (str).
    """
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    utilities = __import__(f'pilot.user.{pilot_user}.utilities', globals(), locals(), [pilot_user], 0)
    try:
        utility_node = utilities.get_memory_monitor_info(workdir, name=name)
        data.update(utility_node)
    except Exception as error:
        logger.info(f'memory information not available: {error}')


def remove_pilot_logs_from_list(list_of_files: list, jobid: str) -> list:
    """
    Remove any pilot logs from the list of last updated files.

    :param list_of_files: list of last updated files (list)
    :param jobid: PanDA job id (str)
    :return: list of files (list).
    """
    # note: better to move experiment specific files to user area
    # ignore the pilot log files
    try:
        to_be_removed = [config.Pilot.pilotlog, config.Pilot.stageinlog, config.Pilot.stageoutlog,
                         config.Pilot.timing_file, config.Pilot.remotefileverification_dictionary,
                         config.Pilot.remotefileverification_log, config.Pilot.base_trace_report,
                         config.Container.container_script, config.Container.release_setup,
                         config.Container.stagein_status_dictionary, config.Container.stagein_replica_dictionary,
                         'eventLoopHeartBeat.txt', 'memory_monitor_output.txt', 'memory_monitor_summary.json_snapshot',
                         f'curl_updateJob_{jobid}.config', config.Pilot.pilot_heartbeat_file]
    except Exception as error:
        logger.warning(f'exception caught: {error}')
        to_be_removed = []

    new_list_of_files = []
    for filename in list_of_files:
        if os.path.basename(filename) not in to_be_removed and '/pilot/' not in filename and 'prmon' not in filename:
            new_list_of_files.append(filename)

    return new_list_of_files


def get_payload_log_tail(workdir: str, jobid: str) -> str:
    """
    Return the tail of the payload stdout or its latest updated log file.

    :param workdir: job work directory (str)
    :param jobid: PanDA job id (str)
    :return: tail of stdout (str).
    """
    # find the latest updated log file
    # list_of_files = get_list_of_log_files()
    # find the latest updated text file
    list_of_files = find_text_files()
    list_of_files = remove_pilot_logs_from_list(list_of_files, jobid)

    if not list_of_files:
        logger.info(f'no log files were found (will use default {config.Payload.payloadstdout})')
        list_of_files = [os.path.join(workdir, config.Payload.payloadstdout)]

    return get_latest_log_tail(list_of_files)


def get_latest_log_tail(files: list) -> str:
    """
    Get the tail of the latest updated file from the given file list.

    :param files: files (list)
    :return: tail (str).
    """
    stdout_tail = ""

    try:
        latest_file = max(files, key=os.path.getmtime)
        logger.info(f'tail of file {latest_file} will be added to heartbeat')

        # now get the tail of the found log file and protect against potentially large tails
        stdout_tail = latest_file + "\n" + tail(latest_file)
        stdout_tail = stdout_tail[-2048:]
    except OSError as exc:
        logger.warning(f'failed to get payload stdout tail: {exc}')

    return stdout_tail


def validate(queues: namedtuple, traces: Any, args: object):
    """
    Perform validation of job.

    Thread.

    :param queues: queues object (namedtuple)
    :param traces: traces object (Any)
    :param args: args object (object).
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.jobs.get(block=True, timeout=1)
        except queue.Empty:
            continue

        traces.pilot['nr_jobs'] += 1

        # set the environmental variable for the task id
        os.environ['PanDA_TaskID'] = str(job.taskid)
        logger.info(f'processing PanDA job {job.jobid} from task {job.taskid}')

        if _validate_job(job):

            # Define a new parent group
            os.setpgrp()

            job_dir = os.path.join(args.mainworkdir, f'PanDA_Pilot-{job.jobid}')
            logger.debug(f'creating job working directory: {job_dir}')
            try:
                os.mkdir(job_dir)
                os.chmod(job_dir, 0o770)
                job.workdir = job_dir
            except (FileExistsError, PermissionError, FileNotFoundError) as error:
                logger.debug(f'cannot create working directory: {error}')
                traces.pilot['error_code'] = errors.MKDIR
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(traces.pilot['error_code'])
                job.piloterrordiag = error
                put_in_queue(job, queues.failed_jobs)
                break
            else:
                create_k8_link(job_dir)

#            try:
#                # stream the job object to file
#                job_dict = job.to_json()
#                write_json(os.path.join(job.workdir, 'job.json'), job_dict)
#            except Exception as error:
#                logger.debug(f'exception caught: {error}')
#            else:
#                try:
#                    _job_dict = read_json(os.path.join(job.workdir, 'job.json'))
#                    job_dict = loads(_job_dict)
#                    _job = JobData(job_dict, use_kmap=False)
#                except Exception as error:
#                    logger.warning(f'exception caught: {error}')

            # hide any secrets
            hide_secrets(job)

            create_symlink(from_path=f'../{config.Pilot.pilotlog}', to_path=os.path.join(job_dir, config.Pilot.pilotlog))

            # handle proxy in unified dispatch
            if args.verify_proxy:
                handle_proxy(job)
            else:
                logger.debug(
                    f'will skip unified dispatch proxy handling since verify_proxy={args.verify_proxy} '
                    f'(job.infosys.queuedata.type={job.infosys.queuedata.type})')

            # pre-cleanup
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            utilities = __import__(f'pilot.user.{pilot_user}.utilities', globals(), locals(), [pilot_user], 0)
            try:
                utilities.precleanup()
            except Exception as error:
                logger.warning(f'exception caught: {error}')

            # store the PanDA job id for the wrapper to pick up
            if not args.pod:
                store_jobid(job.jobid, args.sourcedir)

                # make sure that ctypes is available (needed at the end by orphan killer)
                verify_ctypes()

            # run the delayed space check now
            delayed_space_check(queues, traces, args, job)

            # make sure the queue is correctly configured for containers if needed
            if job.usecontainer:
                if "pilot" in pilot_cache.queuedata.container_type:
                    pass
                else:
                    logger.debug(f"pilot_cache.queuedata={pilot_cache.queuedata}")
                    msg = "container_type must be set in CRIC"
                    logger.error(msg)
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.QUEUENOTSETUPFORCONTAINERS,
                                                                                     msg=msg)
                    job.usecontainer = False
                    put_in_queue(job, queues.failed_jobs)

        else:
            logger.debug(f'failed to validate job={job.jobid}')
            put_in_queue(job, queues.failed_jobs)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='validate'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[job] validate thread has finished')


def hide_secrets(job: Any):
    """
    Hide any user secrets.

    The function hides any user secrets arriving with the job definition. It places them in a JSON file (panda_secrets.json)
    and updates the job.pandasecrets string to 'hidden'. The JSON file is removed before the job log is created. The
    contents of job.pandasecrets is not dumped to the log.

    :param job: job object (Any).
    """
    if job.pandasecrets:
        try:
            path = os.path.join(job.workdir, config.Pilot.pandasecrets)
            _ = write_file(path, job.pandasecrets)
        except FileHandlingFailure as exc:
            logger.warning(f'failed to store user secrets: {exc}')
        logger.info('user secrets saved to file')
        #job.pandasecrets = 'hidden'
    else:
        logger.debug('no user secrets for this job')


def verify_ctypes():
    """Verify ctypes and make sure all subprocess are parented."""
    try:
        import ctypes
    except ImportError as error:
        diagnostics = f'ctypes python module could not be imported: {error}'
        logger.warning(diagnostics)
        #job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOCTYPES, msg=diagnostics)
        #logger.debug('Failed to validate job=%s', job.jobid)
        #put_in_queue(job, queues.failed_jobs)
    else:
        logger.debug('ctypes python module imported')

        # make sure all children are parented by the pilot
        # specifically, this will include any 'orphans', i.e. if the pilot kills all subprocesses at the end,
        # 'orphans' will be included (orphans seem like the wrong name)
        libc = ctypes.CDLL('libc.so.6')
        pr_set_child_subreaper = 36
        libc.prctl(pr_set_child_subreaper, 1)
        logger.debug('all child subprocesses will be parented')


def delayed_space_check(queues: namedtuple, traces: Any, args: object, job: object):
    """
    Run the delayed space check if necessary.

    :param queues: queues object (namedtuple)
    :param traces: traces object (Any)
    :param args: args object (object)
    :param job: job object (object).
    """
    proceed_with_local_space_check = args.harvester_submitmode.lower() == 'push' and args.update_server
    if proceed_with_local_space_check:
        logger.debug('pilot will now perform delayed space check')
        exit_code, diagnostics = check_local_space()
        if exit_code != 0:
            traces.pilot['error_code'] = errors.NOLOCALSPACE
            # set the corresponding error code
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOLOCALSPACE, msg=diagnostics)
            logger.debug(f'failed to validate job={job.jobid}')
            put_in_queue(job, queues.failed_jobs)
        else:
            put_in_queue(job, queues.validated_jobs)
    else:
        put_in_queue(job, queues.validated_jobs)


def create_k8_link(job_dir: str):
    """
    Create a soft link to the payload workdir on Kubernetes if SHARED_DIR exists.

    :param job_dir: payload workdir (str).
    """
    shared_dir = os.environ.get('SHARED_DIR', None)
    if shared_dir:
        #create_symlink(from_path=os.path.join(shared_dir, 'payload_workdir'), to_path=job_dir)
        create_symlink(from_path=job_dir, to_path=os.path.join(shared_dir, 'payload_workdir'))
    else:
        logger.debug('will not create symlink in SHARED_DIR')


def store_jobid(jobid: int, init_dir: str):
    """
    Store the PanDA job id in a file that can be picked up by the wrapper for other reporting.

    :param jobid: job id (int)
    :param init_dir: pilot init dir (str).
    """
    pilot_source_dir = os.environ.get('PANDA_PILOT_SOURCE', '')
    if pilot_source_dir:
        path = os.path.join(pilot_source_dir, config.Pilot.jobid_file)
    else:
        path = os.path.join(os.path.join(init_dir, 'pilot3'), config.Pilot.jobid_file)
        path = path.replace('pilot3/pilot3', 'pilot3')  # dirty fix for bad paths

    try:
        mode = 'a' if os.path.exists(path) else 'w'
        write_file(path, f"{jobid}\n", mode=mode, mute=False)
    except Exception as error:
        logger.warning(f'exception caught while trying to store job id: {error}')


def create_data_payload(queues: namedtuple, traces: Any, args: object):
    """
    Get a Job object from the "validated_jobs" queue.

    If the job has defined input files, move the Job object to the "data_in" queue and put the internal pilot state to
    "stagein". In case there are no input files, place the Job object in the "finished_data_in" queue. For either case,
    the thread also places the Job object in the "payloads" queue (another thread will retrieve it and wait for any
    stage-in to finish).

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    while not args.graceful_stop.is_set():
        time.sleep(0.5)
        try:
            job = queues.validated_jobs.get(block=True, timeout=1)
        except queue.Empty:
            continue

        if job.indata:
            # if the job has input data, put the job object in the data_in queue which will trigger stage-in
            set_pilot_state(job=job, state='stagein')
            put_in_queue(job, queues.data_in)

        else:
            # if the job does not have any input data, then pretend that stage-in has finished and put the job
            # in the finished_data_in queue
            put_in_queue(job, queues.finished_data_in)
            # for stager jobs in pod mode, let the server know the job is running, then terminate the pilot as it is no longer needed
            if args.pod and args.workflow == 'stager':
                set_pilot_state(job=job, state='running')
                ret = send_state(job, args, 'running')
                if not ret:
                    traces.pilot['error_code'] = errors.COMMUNICATIONFAILURE
                logger.info('pilot is no longer needed - terminating')
                args.job_aborted.set()
                args.graceful_stop.set()

        # only in normal workflow; in the stager workflow there is no payloads queue
        if not args.workflow == 'stager':
            put_in_queue(job, queues.payloads)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='create_data_payload'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[job] create_data_payload thread has finished')


def get_task_id() -> str:
    """
    Return the task id for the current job.

    Note: currently the implementation uses an environmental variable to store this number (PanDA_TaskID).

    :return: task id. Returns empty string in case of error (str)
    """
    if "PanDA_TaskID" in os.environ:
        taskid = os.environ["PanDA_TaskID"]
    else:
        logger.warning('PanDA_TaskID not set in environment')
        taskid = ""

    return taskid


def get_job_label(args: Any) -> str:
    """
    Return a proper job label.

    The function returns a job label that corresponds to the actual pilot version, ie if the pilot is a development
    version (ptest or rc_test2) or production version (managed or user).
    Example: -i RC -> job_label = rc_test2.
    NOTE: it should be enough to only use the job label, -j rc_test2 (and not specify -i RC at all).

    :param args: pilot args object (Any)
    :return: job_label (str).
    """
    # PQ status
    status = infosys.queuedata.status

    if args.version_tag == 'RC' and args.job_label == 'rc_test2':
        job_label = 'rc_test2'
    elif args.version_tag == 'RC' and args.job_label == 'ptest':
        job_label = args.job_label
    elif args.version_tag == 'RCM' and args.job_label == 'ptest':
        job_label = 'rcm_test2'
    elif args.version_tag == 'ALRB':
        job_label = 'rc_alrb'
    elif status == 'test' and args.job_label != 'ptest':
        logger.warning('PQ status set to test - will use job label / prodSourceLabel test')
        job_label = 'test'
    else:
        job_label = args.job_label

    return job_label


def get_remaining_time(args: Any) -> int:
    """
    Return the remaining time for the pilot.

    The remaining time is taken as the minimum of the remaining proxy lifetime and the remaining time
    before the time limit set by the site kills the job.

    Args:
        args (Any): Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc)

    Returns:
        int: remaining time in seconds.
    """
    # get the remaining time from the proxy
    remaining_time = pilot_cache.proxy_lifetime
    logger.info(f"remaining proxy life time = {pilot_cache.proxy_lifetime} s")
    if not remaining_time:
        logger.warning('failed to get remaining time from proxy')
        return 0

    # get the remaining time from the site
    # e.g. maxtime = 345600, i.e. pilot is allowed to run for a maximum of 345600 s
    # the remaining time is therefore 345600 - time since pilot started
    site_remaining_time = infosys.queuedata.maxtime - get_time_since_start(args)
    logger.info(f"remaining time (PQ.maxtime - time since start) = {site_remaining_time} s")
    if not site_remaining_time:
        logger.warning('failed to get remaining time from site')
        return 0

    # return the minimum of the two
    return min(remaining_time, site_remaining_time)


def get_dispatcher_dictionary(args: Any, taskid: str = "") -> dict:
    """
    Return a dictionary with required fields for the dispatcher getJob operation.

    The dictionary should contain the following fields: siteName, computingElement (queue name),
    prodSourceLabel (e.g. user, test, ptest), diskSpace (available disk space for a job in MB),
    workingGroup, countryGroup, cpu (float), mem (float) and node (worker node name).

    workingGroup, countryGroup and allowOtherCountry
    we add a new pilot setting allowOtherCountry=True to be used in conjunction with countryGroup=us for
    US pilots. With these settings, the Panda server will produce the desired behaviour of dedicated X% of
    the resource exclusively (so long as jobs are available) to countryGroup=us jobs. When allowOtherCountry=false
    this maintains the behavior relied on by current users of the countryGroup mechanism -- to NOT allow
    the resource to be used outside the privileged group under any circumstances.

    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (Any)
    :param taskid: task id from message broker, if any (str)
    :returns: dictionary prepared for the dispatcher getJob operation (str).
    """
    _diskspace = get_disk_space(infosys.queuedata)
    _mem, _cpu, _ = collect_workernode_info(os.getcwd())
    _nodename = get_node_name()
    #_remaining_time = get_remaining_time(args)

    data = {
        'siteName': infosys.queuedata.resource,
        'computingElement': args.queue,
        'prodSourceLabel': get_job_label(args),
        'diskSpace': _diskspace,
        'workingGroup': args.working_group,
        'cpu': _cpu,
        'mem': _mem,
        'node': _nodename
    }

    # include remaining time
    #if _remaining_time:
    #    data['remaining_time'] = _remaining_time

    if args.jobtype != "":
        data['jobType'] = args.jobtype

    if args.allow_other_country != "":
        data['allowOtherCountry'] = args.allow_other_country

    if args.country_group != "":
        data['countryGroup'] = args.country_group

    if args.job_label == 'self':
        dn = get_distinguished_name()
        data['prodUserID'] = dn

    # special handling for task id from message broker
    if taskid:
        data['taskID'] = taskid
        if args.allow_same_user:
            data['viaTopic'] = True
        logger.info(f"will download a new job belonging to task id: {data['taskID']}")
    else:  # task id from env var
        taskid = get_task_id()
        if taskid != "" and args.allow_same_user:
            data['taskID'] = taskid
            logger.info(f"will download a new job belonging to task id: {data['taskID']}")

    if args.resource_type != "":
        data['resourceType'] = args.resource_type

    # add harvester fields
    if 'HARVESTER_ID' in os.environ:
        data['harvester_id'] = os.environ.get('HARVESTER_ID')
    if 'HARVESTER_WORKER_ID' in os.environ:
        data['worker_id'] = os.environ.get('HARVESTER_WORKER_ID')

    return data


def proceed_with_getjob(timefloor: int, starttime: int, jobnumber: int, getjob_requests: int, max_getjob_requests: int,  # noqa: C901
                        should_update_server: bool, submitmode: str, harvester: bool, verify_proxy: bool, traces: Any) -> bool:  # noqa: C901
    """
    Check if we can proceed with getJob.

    We may not proceed if we have run out of time (timefloor limit), if the proxy is too short, if disk space is too
    small or if we have already proceed enough jobs.

    :param timefloor: timefloor limit (s) (int)
    :param starttime: start time of retrieve() (s) (int)
    :param jobnumber: number of downloaded jobs (int)
    :param getjob_requests: number of getjob requests (int)
    :param max_getjob_requests: max getjob requests (int)
    :param should_update_server: should pilot update server? (bool)
    :param submitmode: Harvester submit mode, PULL or PUSH (str)
    :param harvester: True if Harvester is used, False otherwise. Affects the max number of getjob reads from file (bool)
    :param verify_proxy: True if the proxy should be verified. False otherwise (bool)
    :param traces: traces object (to be able to propagate a proxy error all the way back to the wrapper) (Any)
    :return: True if pilot should proceed with getJob (bool).
    """
    # use for testing thread exceptions. the exception will be picked up by ExcThread run() and caught in job.control()
    # raise NoLocalSpace('testing exception from proceed_with_getjob')

    #timefloor = 600
    currenttime = time.time()

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    common = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
    if not common.allow_timefloor(submitmode):
        timefloor = 0

    # should the proxy be verified?
    if verify_proxy:
        userproxy = __import__(f'pilot.user.{pilot_user}.proxy', globals(), locals(), [pilot_user], 0)

        # is the proxy still valid? at pilot startup, the proxy lifetime must be at least 72h
        exit_code, diagnostics = userproxy.verify_proxy(test=False, pilotstartup=True)
        if traces.pilot['error_code'] == 0:  # careful so we don't overwrite another error code
            # only set the traces error code if there is still time to run more jobs
            if currenttime - starttime > timefloor and exit_code:
                # we have run out of time to start new jobs, do not report any error (which would have been returned to the
                # wrapper and Harvester would pick it up)
                logger.warning("proxy verification failed, but we will not report it since we have anyway run out of time to start new jobs")
            else:
                traces.pilot['error_code'] = exit_code
        if exit_code == errors.ARCPROXYLIBFAILURE:
            logger.warning("currently ignoring arcproxy library failure")
        if exit_code in {errors.NOPROXY, errors.NOVOMSPROXY, errors.CERTIFICATEHASEXPIRED, errors.PROXYTOOSHORT}:
            logger.warning(diagnostics)
            return False

    # is there enough local space to run a job?
    # note: do not run this test at this point if submit mode=PUSH and we are in truePilot mode on ARC
    # (available local space will in this case be checked after the job definition has been read from file, so the
    # pilot can report the error with a server update)
    proceed_with_local_space_check = not (submitmode.lower() == 'push' and should_update_server)
    if proceed_with_local_space_check:
        exit_code, diagnostics = check_local_space()
        if exit_code != 0:
            traces.pilot['error_code'] = errors.NOLOCALSPACE
            return False
    else:
        logger.debug('pilot will delay local space check until after job definition has been read from file')

    maximum_getjob_requests = 60 if harvester else max_getjob_requests  # 1 s apart (if harvester)
    if getjob_requests > int(maximum_getjob_requests):
        return wrap_up_quickly(f'reached maximum number of getjob requests ({maximum_getjob_requests}) -- will abort pilot')

    if timefloor == 0 and jobnumber > 0:
        return wrap_up_quickly("since timefloor is set to 0, pilot was only allowed to run one job")

    if (currenttime - starttime > timefloor) and jobnumber > 0:
        return wrap_up_quickly(f"the pilot has run out of time (timefloor={timefloor} has been passed)")

    # timefloor not relevant for the first job
    if jobnumber > 0:
        logger.info(f'since timefloor={timefloor} s and only {currenttime - starttime} s has passed since launch, pilot can run another job')

    if harvester and jobnumber > 0:
        # unless it's the first job (which is preplaced in the init dir), instruct Harvester to place another job
        # in the init dir
        logger.info('asking Harvester for another job')
        status = request_new_jobs()
        if not status:
            return False

    if os.environ.get('SERVER_UPDATE', '') == SERVER_UPDATE_UPDATING:
        logger.info('still updating previous job, will not ask for a new job yet')
        return False

    os.environ['SERVER_UPDATE'] = SERVER_UPDATE_NOT_DONE
    return True


def wrap_up_quickly(message: str) -> bool:
    """
    Wrap up quickly.

    Helper function to reduce complexity of proceed_with_getjob().

    :param message: message to log (str)
    :return: False.
    """
    logger.warning(message)
    os.environ['PILOT_WRAP_UP'] = 'QUICKLY'
    return False


def get_job_definition_from_file(path: str, harvester: bool, pod: bool) -> dict:
    """
    Get a job definition from a pre-placed file.

    In Harvester mode, also remove any existing job request files since it is no longer needed/wanted.

    :param path: path to job definition file (str)
    :param harvester: True if Harvester is being used (determined from args.harvester), otherwise False (bool)
    :param pod: True if pilot is running in a pod, otherwise False (bool)
    :return: job definition (dict).
    """
    # remove any existing Harvester job request files (silent in non-Harvester mode) and read the JSON
    if harvester or pod:
        if harvester:
            remove_job_request_file()
        if is_json(path):
            job_definition_list = parse_job_definition_file(path)
            if not job_definition_list:
                logger.warning(f'no jobs were found in Harvester job definitions file: {path}')
                return {}

            # remove the job definition file from the original location, place a renamed copy in the pilot dir
            new_path = os.path.join(os.environ.get('PILOT_HOME'), 'job_definition.json')
            copy(path, new_path)
            remove(path)

            # note: the pilot can only handle one job at the time from Harvester
            return job_definition_list[0]

    # old style
    res = {}
    with open(path, 'r', encoding='utf-8') as jobdatafile:
        response = jobdatafile.read()
        if len(response) == 0:
            logger.fatal(f'encountered empty job definition file: {path}')
            res = None  # this is a fatal error, no point in continuing as the file will not be replaced
        else:
            # parse response message
            datalist = parse_qsl(response, keep_blank_values=True)

            # convert to dictionary
            for data in datalist:
                res[data[0]] = data[1]

    if os.path.exists(path):
        remove(path)

    return res


def get_job_definition_from_server(args: Any, taskid: str = "") -> str:
    """
    Get a job definition from a server.

    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (Any)
    :param taskid: task id from message broker, if any (str)
    :return: job definition (dict).
    """
    res = {}

    # get the job dispatcher dictionary
    data = get_dispatcher_dictionary(args, taskid=taskid)

    # get the getJob server command
    cmd = https.get_server_command(args.url, args.port)
    if cmd != "":
        logger.info(f'executing server command: {cmd}')
        if "curlgetjob" in infosys.queuedata.catchall:
            res = https.request(cmd, data=data)
        else:
            res = https.request2(cmd, data=data, panda=True)  # will be a dictionary
            logger.debug(f"request2 response: {res}")  # should be StatusCode=0 if all is ok
            if not res or isinstance(res, str):  # fallback to curl solution
                res = https.request(cmd, data=data)

    return res


def locate_job_definition(args: Any) -> str:
    """
    Locate the job definition file among standard locations.

    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (Any)
    :return: path (str).
    """
    if args.harvester_datadir:
        paths = [os.path.join(args.harvester_datadir, config.Pilot.pandajobdata)]
    else:
        paths = [os.path.join(f"{args.sourcedir}/..", config.Pilot.pandajobdata),
                 os.path.join(args.sourcedir, config.Pilot.pandajobdata),
                 os.path.join(os.environ['PILOT_WORK_DIR'], config.Pilot.pandajobdata)]

    if args.harvester_workdir:
        paths.append(os.path.join(args.harvester_workdir, config.Harvester.pandajob_file))
    if 'HARVESTER_WORKDIR' in os.environ:
        paths.append(os.path.join(os.environ['HARVESTER_WORKDIR'], config.Harvester.pandajob_file))

    path = ""
    for _path in paths:
        if os.path.exists(_path):
            path = _path
            break

    if path == "":
        logger.info('did not find any local job definition file')

    # make sure there are no secondary job definition copies
    _path = os.path.join(os.environ.get('PILOT_HOME'), config.Pilot.pandajobdata)
    if _path != path and os.path.exists(_path):
        logger.info(f'removing useless secondary job definition file: {_path}')
        remove(_path)

    return path


def get_job_definition(queues: namedtuple, args: object) -> dict or str:
    """
    Get a job definition from a source (server or pre-placed local file).

    :param queues: queues object (namedtuple)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object)
    :return: job definition (dict or str).
    """
    res = {}
    path = locate_job_definition(args)

    # should we run a normal 'real' job or a 'fake' job?
    if config.Pilot.pandajob == 'fake':
        logger.info('will use a fake PanDA job')
        res = get_fake_job()
    elif os.path.exists(path):
        logger.info(f'will read job definition from file: {path}')
        res = get_job_definition_from_file(path, args.harvester, args.pod)
    elif args.harvester and args.harvester_submitmode.lower() == 'push':
        pass  # local job definition file not found (go to sleep)
    else:
        # get the task id from a message broker if requested
        taskid = None
        abort = False
        if args.subscribe_to_msgsvc:
            message = None
            while not args.graceful_stop.is_set():
                try:  # look for graceful stop every ten seconds, otherwise block the queue
                    message = queues.messages.get(block=True, timeout=10)
                except queue.Empty:
                    continue
                else:
                    break

#            message = get_message_from_mb(args)
            if message and message['msg_type'] == 'get_job':
                taskid = message['taskid']
            elif message and message['msg_type'] == 'kill_task':
                # abort immediately
                logger.warning('received instruction to kill task (abort pilot)')
                abort = True
            elif message and message['msg_type'] == 'finish_task':
                # abort gracefully - let job finish, but no job is downloaded so ignore this?
                logger.warning('received instruction to finish task (abort pilot)')
                abort = True
            elif args.graceful_stop.is_set():
                logger.warning('graceful_stop is set, will abort getJob')
                abort = True
            if taskid:
                logger.info(f'will download job definition from server using taskid={taskid}')
        else:
            logger.info('will download job definition from server')
        if abort:
            res = None  # None will trigger 'fatal' error and will finish the pilot
        else:
            if delay_to_get_job():
                res = None
            else:
                res = get_job_definition_from_server(args, taskid=taskid)

    return res


def get_load_factor() -> float:
    """
    Get the pilot load factor.
    """
    try:
        load_factor = os.environ.get('PILOT_LOAD_FACTOR', None)
        if load_factor:
            try:
                load_factor = float(load_factor)
            except Exception as exc:      # NOQA F841
                load_factor = None
        if not load_factor:
            load_factor = float(config.Pilot.load_factor)
        return load_factor
    except Exception as exc:
        logger.warning(f'delay_to_get_job caught exception: {exc}')
    return 1.2


def is_delay_to_get_job_enabled() -> bool:
    """
    To check if it's enabled to delay to get jobs based on load.
    """
    try:
        enabled = os.environ.get('PILOT_ENABLE_DELAY_TO_GET_JOB', 'false').lower() == 'true'
        if enabled:
            return enabled
        enabled = bool(config.Pilot.enable_delay_to_get_job)
        return enabled
    except Exception as exc:
        logger.warning(f'is_delay_to_get_job_enabled caught exception: {exc}')
    return False


def delay_to_get_job() -> bool:
    """
    Delay to get job if the machine load is too high.

    This code checks the system's load average and compares it to a calculated maximum load based on the number
    of CPU cores and a configurable load factor. If the 1-minute load average exceeds the maximum load, it logs
    the condition and delays job retrieval.

    :return: bool.
    """
    try:
        if not is_delay_to_get_job_enabled():
            return False

        load1, load5, load15 = os.getloadavg()
        num_cores = os.cpu_count()
        load_factor = get_load_factor()
        max_load = num_cores * load_factor
        logger.info(f"num_cores: {num_cores}, load_factor: {load_factor}, load1: {load1}, load5: {load5}, load15: {load15}")
        if load1 > max_load:
            logger.info(f"load1 ({load1}) > max_load ({max_load}) (num_cores * load_factor). delay to getjob.")
            return True
        return False
    except Exception as exc:
        logger.warning(f'delay_to_get_job caught exception: {exc}')
    return False


def get_message_from_mb(args: Any) -> dict:
    """
    Get a message from a message broker.

    Wait maximum args.lifetime s, then abort.
    Note that this might also be interrupted by args.graceful_stop (checked for each ten seconds).

    :param args: Pilot arguments object (Any)
    :return: message (dict).
    """
    if args.graceful_stop.is_set():
        logger.debug('will not start ActiveMQ since graceful_stop is set')
        return {}

    # do not put this import at the top since it can possibly interfere with some modules (esp. Google Cloud Logging modules)
    import multiprocessing

    ctx = multiprocessing.get_context('spawn')
    message_queue = ctx.Queue()
    #amq_queue = ctx.Queue()

    proc = multiprocessing.Process(target=get_message, args=(args, message_queue,))
    proc.start()

    _t0 = time.time()  # basically this should be PILOT_START_TIME, but any large time will suffice for the loop below
    maxtime = args.lifetime
    while not args.graceful_stop.is_set() and time.time() - _t0 < maxtime:
        proc.join(10)  # wait for ten seconds, then check graceful_stop and that we are within the allowed running time
        if proc.is_alive():
            continue

        break  # ie abort 'infinite' loop when the process has finished

    if proc.is_alive():
        # still running after max time/graceful_stop: kill it
        proc.terminate()

    try:
        message = message_queue.get(timeout=1)
    except Exception:
        message = {}
    if not message:
        logger.debug('not returning any messages')

    return message


def get_message(args: Any, message_queue: Any):
    """
    Get a message from ActiveMQ and put it in the given message queue.

    :param args: Pilot arguments object (Any)
    :param message_queue: message queue (Any).
    """
    queues = namedtuple('queues', ['mbmessages'])
    queues.mbmessages = queue.Queue()
    kwargs = get_kwargs_for_mb(queues, args.url, args.port, args.allow_same_user, args.debug)
    # start connections
    amq = ActiveMQ(**kwargs)
    args.amq = amq

    # wait for messages
    message = None
    while not args.graceful_stop.is_set() and not os.environ.get('REACHED_MAXTIME', None):
        time.sleep(0.5)
        try:
            message = queues.mbmessages.get(block=True, timeout=10)
        except queue.Empty:
            continue
        else:
            break

    if args.graceful_stop.is_set() or os.environ.get('REACHED_MAXTIME', None):
        logger.debug('closing connections')
        amq.close_connections()
        logger.debug('get_message() ended - the pilot has finished')

    if message:
        # message = {'msg_type': 'get_job', 'taskid': taskid}
        message_queue.put(message)


def get_kwargs_for_mb(queues: namedtuple, url: str, port: str, allow_same_user: bool, debug: bool):
    """
    Get the kwargs dictinoary for the message broker.

    :param queues: queues object (namedtuple)
    :param url: PanDA server URL (str)
    :param port: PanDA server port (str)
    :param allow_same_user: allow the same user or not (bool)
    :param debug: True for pilot debug mode, False otherwise (bool)
    :return: kwargs dictionary (dict).
    """
    topic = f'/{"topic" if allow_same_user else "queue"}/panda.pilot'
    kwargs = {
        'broker': config.Message_broker.url,  # 'atlas-test-mb.cern.ch',
        'receiver_port': config.Message_broker.receiver_port,  # 61013,
        # 'port': config.Message_broker.port,  # 61013,
        'topic': topic,
        'receive_topics': [topic],
        'username': 'X',
        'password': 'X',
        'queues': queues,
        'pandaurl': url,
        'pandaport': port,
        'debug': debug
    }

    return kwargs


def now() -> str:
    """
    Return the current epoch as a UTF-8 encoded string.

    :return: current time as encoded string (str).
    """
    return str(time.time()).encode('utf-8')


def get_fake_job(inpt: bool = True) -> dict:
    """
    Return a job definition for internal pilot testing.

    Note: this function is only used for testing purposes. The job definitions below are ATLAS specific.

    :param inpt: True when there are input files, set to False if no input files are wanted (bool)
    :return: job definition (dict).
    """
    res = {}

    # create hashes
    _hash = hashlib.md5()
    _hash.update(now())
    log_guid = _hash.hexdigest()
    _hash.update(now())
    guid = _hash.hexdigest()
    _hash.update(now())
    job_name = _hash.hexdigest()

    if config.Pilot.testjobtype == 'production':
        logger.info('creating fake test production job definition')
        res = {'jobsetID': 'NULL',
               'logGUID': log_guid,
               'cmtConfig': 'x86_64-slc6-gcc48-opt',
               'prodDBlocks': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
               'dispatchDBlockTokenForOut': 'NULL,NULL',
               'destinationDBlockToken': 'NULL,NULL',
               'destinationSE': 'AGLT2_TEST',
               'realDatasets': job_name,
               'prodUserID': 'no_one',
               'GUID': guid,
               'realDatasetsIn': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
               'nSent': 0,
               'cloud': 'US',
               'StatusCode': 0,
               'homepackage': 'AtlasProduction/20.1.4.14',
               'inFiles': 'HITS.06828093._000096.pool.root.1',
               'processingType': 'pilot-ptest',
               'ddmEndPointOut': 'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
               'fsize': '94834717',
               'fileDestinationSE': 'AGLT2_TEST,AGLT2_TEST',
               'scopeOut': 'panda',
               'minRamCount': 0,
               'jobDefinitionID': 7932,
               'maxWalltime': 'NULL',
               'scopeLog': 'panda',
               'transformation': 'Reco_tf.py',
               'maxDiskCount': 0,
               'coreCount': 1,
               'prodDBlockToken': 'NULL',
               'transferType': 'NULL',
               'destinationDblock': job_name,
               'dispatchDBlockToken': 'NULL',
               'jobPars': f'--maxEvents=1 --inputHITSFile HITS.06828093._000096.pool.root.1 --outputRDOFile RDO_{job_name}.root',
               'attemptNr': 0,
               'swRelease': 'Atlas-20.1.4',
               'nucleus': 'NULL',
               'maxCpuCount': 0,
               'outFiles': f'RDO_{job_name}.root,{job_name}.job.log.tgz',
               'currentPriority': 1000,
               'scopeIn': 'mc15_13TeV',
               'PandaID': '0',
               'sourceSite': 'NULL',
               'dispatchDblock': 'NULL',
               'prodSourceLabel': 'ptest',
               'checksum': 'ad:5d000974',
               'jobName': job_name,
               'ddmEndPointIn': 'UTA_SWT2_DATADISK',
               'taskID': 'NULL',
               'logFile': f'{job_name}.job.log.tgz'}
    elif config.Pilot.testjobtype == 'user':
        logger.info('creating fake test user job definition')
        res = {'jobsetID': 'NULL',
               'logGUID': log_guid,
               'cmtConfig': 'x86_64-slc6-gcc49-opt',
               'prodDBlocks': 'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               'dispatchDBlockTokenForOut': 'NULL,NULL',
               'destinationDBlockToken': 'NULL,NULL',
               'destinationSE': 'ANALY_SWT2_CPB',
               'realDatasets': job_name,
               'prodUserID': 'None',
               'GUID': guid,
               'realDatasetsIn': 'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               'nSent': '0',
               'cloud': 'US',
               'StatusCode': 0,
               'homepackage': 'AnalysisTransforms-AtlasDerivation_20.7.6.4',
               'inFiles': 'AOD.07709524._000050.pool.root.1',
               'processingType': 'pilot-ptest',
               'ddmEndPointOut': 'SWT2_CPB_SCRATCHDISK,SWT2_CPB_SCRATCHDISK',
               'fsize': '1564780952',
               'fileDestinationSE': 'ANALY_SWT2_CPB,ANALY_SWT2_CPB',
               'scopeOut': 'user.gangarbt',
               'minRamCount': '0',
               'jobDefinitionID': '9445',
               'maxWalltime': 'NULL',
               'scopeLog': 'user.gangarbt',
               'transformation': 'http://pandaserver.cern.ch:25080/trf/user/runAthena-00-00-11',
               'maxDiskCount': '0',
               'coreCount': '1',
               'prodDBlockToken': 'NULL',
               'transferType': 'NULL',
               'destinationDblock': job_name,
               'dispatchDBlockToken': 'NULL',
               'jobPars': '-a sources.20115461.derivation.tgz -r ./ -j "Reco_tf.py '
                          '--inputAODFile AOD.07709524._000050.pool.root.1 --outputDAODFile test.pool.root '
                          '--reductionConf HIGG3D1" -i "[\'AOD.07709524._000050.pool.root.1\']" -m "[]" -n "[]" --trf'
                          ' --useLocalIO --accessmode=copy -o '
                          '"{\'IROOT\': [(\'DAOD_HIGG3D1.test.pool.root\', \'%s.root\')]}" '
                          '--sourceURL https://aipanda012.cern.ch:25443' % (job_name),
               'attemptNr': '0',
               'swRelease': 'Atlas-20.7.6',
               'nucleus': 'NULL',
               'maxCpuCount': '0',
               'outFiles': f'{job_name}.root,{job_name}.job.log.tgz',
               'currentPriority': '1000',
               'scopeIn': 'data15_13TeV',
               'PandaID': '0',
               'sourceSite': 'NULL',
               'dispatchDblock': 'data15_13TeV:data15_13TeV.00276336.physics_Main.merge.AOD.r7562_p2521_tid07709524_00',
               'prodSourceLabel': 'ptest',
               'checksum': 'ad:b11f45a7',
               'jobName': job_name,
               'ddmEndPointIn': 'SWT2_CPB_SCRATCHDISK',
               'taskID': 'NULL',
               'logFile': f'{job_name}.job.log.tgz'}
    else:
        logger.warning(f'unknown test job type: {config.Pilot.testjobtype}')

    if res:
        if not inpt:
            res['inFiles'] = 'NULL'
            res['GUID'] = 'NULL'
            res['scopeIn'] = 'NULL'
            res['fsize'] = 'NULL'
            res['realDatasetsIn'] = 'NULL'
            res['checksum'] = 'NULL'

        if config.Pilot.testtransfertype in {'NULL', 'direct'}:
            res['transferType'] = config.Pilot.testtransfertype
        else:
            logger.warning(f'unknown test transfer type: {config.Pilot.testtransfertype} (ignored)')

        if config.Pilot.testjobcommand == 'sleep':
            res['transformation'] = 'sleep'
            res['jobPars'] = '1'
            res['inFiles'] = ''
            res['outFiles'] = ''

    return res


def get_job_retrieval_delay(harvester: bool) -> int:
    """
    Return the proper delay between job retrieval attempts.

    In Harvester mode, the pilot will look once per second for a job definition file.

    :param harvester: True if Harvester is being used (determined from args.harvester), otherwise False (bool)
    :return: sleep (s) (int)
    """
    return 10 if harvester else 60


def retrieve(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Retrieve all jobs from the proper source.

    Thread.

    The job definition is a json dictionary that is either present in the launch
    directory (preplaced) or downloaded from a server specified by `args.url`.

    The function retrieves the job definition from the proper source and places
    it in the `queues.jobs` queue.

    WARNING: this function is nearly too complex. Be careful with adding more lines as flake8 will fail it.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object)
    :raises PilotException: if create_job fails (e.g. because queuedata could not be downloaded).
    """
    timefloor = infosys.queuedata.timefloor
    starttime = time.time()

    jobnumber = 0  # number of downloaded jobs
    getjob_requests = 0
    getjob_failures = 0
    print_node_info()

    while not args.graceful_stop.is_set():

        time.sleep(0.5)
        getjob_requests += 1

        if not proceed_with_getjob(timefloor, starttime, jobnumber, getjob_requests, args.getjob_requests,
                                   args.update_server, args.harvester_submitmode, args.harvester, args.verify_proxy, traces):
            # do not set graceful stop if pilot has not finished sending the final job update
            # i.e. wait until SERVER_UPDATE is DONE_FINAL
            check_for_final_server_update(args.update_server)
            logger.warning('setting graceful_stop since proceed_with_getjob() returned False (pilot will end)')
            args.graceful_stop.set()
            args.abort_job.set()
            break

        # store time stamp
        time_pre_getjob = time.time()

        # get a job definition from a source (file or server)
        res = get_job_definition(queues, args)
        if isinstance(res, str):
            logger.warning(f"get_job_definition() returned a string (setting it to None): {res}")
            res = None

        #res['debug'] = True
        if res:
            dump_job_definition(res)

        # only ATLAS wants to abort immediately in this case
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        jobdata = __import__(f'pilot.user.{pilot_user}.jobdata', globals(), locals(), [pilot_user], 0)
        fail_at_none = jobdata.fail_at_getjob_none()
        if res is None and fail_at_none:
            logger.fatal('fatal error in job download loop - cannot continue')
            # do not set graceful stop if pilot has not finished sending the final job update
            # i.e. wait until SERVER_UPDATE is DONE_FINAL
            check_for_final_server_update(args.update_server)
            logger.warning('setting graceful_stop since no job definition could be received (pilot will end)')
            args.graceful_stop.set()
            break

        if not res:
            getjob_failures += 1
            if getjob_failures >= get_nr_getjob_failures(args.getjob_failures, args.harvester_submitmode):
                logger.warning(f'did not get a job -- max number of job request failures reached: {getjob_failures} (setting graceful_stop)')
                if getjob_failures >= 5 and pilot_cache.queuedata.resource_type.lower() == 'hpc':
                    logger.warning('setting error code to NOJOBSINPANDA on HPC resource')
                    traces.pilot['error_code'] = errors.NOJOBSINPANDA

                args.graceful_stop.set()
                break

            delay = get_job_retrieval_delay(args.harvester)
            if not args.harvester:
                logger.warning(f'did not get a job -- sleep {delay} s and repeat')
            for _ in range(delay):
                if args.graceful_stop.is_set():
                    break
                time.sleep(1)
        elif ((isinstance(res, str) and res.startswith('StatusCode') and not res.startswith('StatusCode=0') or
               (isinstance(res, dict) and 'StatusCode' in res and res['StatusCode'] != '0' and res['StatusCode'] != 0))):
            # it seems the PanDA server returns StatusCode as an int, but the aCT returns it as a string
            # note: StatusCode keyword is not available in job definition files from Harvester (not needed)
            getjob_failures += 1
            if getjob_failures >= get_nr_getjob_failures(args.getjob_failures, args.harvester_submitmode):
                logger.warning(f'did not get a job -- max number of job request failures reached: {getjob_failures}')
                if getjob_failures >= 5 and pilot_cache.queuedata.resource_type.lower() == 'hpc':
                    logger.warning('setting error code to NOJOBSINPANDA on HPC resource')
                    traces.pilot['error_code'] = errors.NOJOBSINPANDA

                args.graceful_stop.set()
                break

            delay = random.randint(60, 180)
            logger.warning(f"did not get a job -- sleep {delay}s and repeat -- status: {res}")
            for _ in range(delay):
                if args.graceful_stop.is_set():
                    break
                time.sleep(1)
        else:
            # create the job object out of the raw dispatcher job dictionary
            try:
                job = create_job(res, queuename=args.queue)
            except PilotException as error:
                raise error

            # keep track of start time
            job.starttime = int(time.time())
            logger.info(f'job {job.jobid} has start time={job.starttime}')

            # inform the server if this job should be in debug mode (real-time logging), decided by queuedata
            if "setdebugmode" in job.infosys.queuedata.catchall:
                set_debug_mode(job.jobid, args.url, args.port)

            # logger.info('resetting any existing errors')
            job.reset_errors()

            #else:
            # verify the job status on the server
            #try:
            #    job_status, job_attempt_nr, job_status_code = get_job_status_from_server(job.jobid, args.url, args.port)
            #    if job_status == "running":
            #        pilot_error_diag = "job %s is already running elsewhere - aborting" % job.jobid
            #        logger.warning(pilot_error_diag)
            #        raise JobAlreadyRunning(pilot_error_diag)
            #except Exception as error:
            #    logger.warning(f"{error}")
            # write time stamps to pilot timing file
            # note: PILOT_POST_GETJOB corresponds to START_TIME in Pilot 1
            add_to_pilot_timing(job.jobid, PILOT_PRE_GETJOB, time_pre_getjob, args)
            add_to_pilot_timing(job.jobid, PILOT_POST_GETJOB, time.time(), args)

            # for debugging on HTCondor purposes, set special env var
            # (only proceed if there is a condor class ad)
            if os.environ.get('_CONDOR_JOB_AD', None):
                htcondor_envvar(job.jobid)

            # add the job definition to the jobs queue and increase the job counter,
            # and wait until the job has finished
            put_in_queue(job, queues.jobs)

            jobnumber += 1
            while not args.graceful_stop.is_set():
                if has_job_completed(queues, args):
                    # make sure there are no lingering defunct subprocesses
                    kill_defunct_children(job.pid)

                    # purge queue(s) that retains job object
                    set_pilot_state(state='')
                    purge_queue(queues.finished_data_in)

                    args.job_aborted.clear()
                    args.abort_job.clear()
                    logger.info('ready for new job')

                    # re-establish logging
                    logging.info('pilot has finished with previous job - re-establishing logging')
                    logging.handlers = []
                    logging.shutdown()
                    establish_logging(debug=args.debug, nopilotlog=args.nopilotlog)
                    pilot_version_banner()
                    getjob_requests = 0
                    add_to_pilot_timing('1', PILOT_MULTIJOB_START_TIME, time.time(), args)
                    args.signal = None
                    break
                time.sleep(0.5)

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='retrieve'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[job] retrieve thread has finished')


def set_debug_mode(jobid: int, url: str, port: int):
    """
    Inform the server that the given job should be in debug mode.

    Note, this is decided by queuedata.catchall.

    :param jobid: job id (int)
    :param url: server url (str)
    :param port: server port (int).
    """
    # worker node structure to be sent to the server
    data = {}
    data["pandaID"] = jobid
    data["modeOn"] = True

    # attempt to send the info to the server
    res = https.send_update("setDebugMode", data, url, port)
    if not res:
        logger.warning('could not inform server to set job in debug mode')


def get_nr_getjob_failures(getjob_failures: int, harvester_submitmode: str) -> int:
    """
    Return the number of max getjob failures.

    Note: the default max number of getjob failures is set to 5 in pilot.py. However, for PUSH mode, it makes more
    sense to have a larger max attempt number since Harvester only checks for job requests once per five minutes.
    So, if the pilot is started in PUSH mode, the max number of getjob failures is set to a higher number unless
    args.getjob_failures is set (to a number not equal to five).

    :param getjob_failures: max getjob failures (int)
    :param harvester_submitmode: Harvester submit mode, PUSH or PULL (str)
    :return: max getjob failures (int).
    """
    if harvester_submitmode.lower() == 'push':
        if getjob_failures == 5:
            return 12
        else:
            return getjob_failures
    else:
        return getjob_failures


def htcondor_envvar(jobid: str):
    """
    On HTCondor nodes, set special env var (HTCondor_PANDA) for debugging Lustre.

    :param jobid: PanDA job id (str).
    """
    try:
        globaljobid = encode_globaljobid(jobid)
        if globaljobid:
            os.environ['HTCondor_Job_ID'] = globaljobid
            logger.info(f'set env var HTCondor_Job_ID={globaljobid}')
    except Exception as exc:
        logger.warning(f'caught exception: {exc}')


def handle_proxy(job: Any):
    """
    Handle the proxy in unified dispatch.

    In unified dispatch, the pilot is started with the production proxy, but in case the job is a user job, the
    production proxy is too powerful. A user proxy is then downloaded instead.

    :param job: job object (Any).
    """
    if job.is_analysis() and job.infosys.queuedata.type == 'unified' and not job.prodproxy:
        logger.info('the production proxy will be replaced by a user proxy (to be downloaded)')
        ec = download_new_proxy(role='user', proxy_type='unified', workdir=job.workdir)
        if ec:
            logger.warning(f'failed to download proxy for unified dispatch - will continue with X509_USER_PROXY={os.environ.get("X509_USER_PROXY")}')
        if ec == errors.CERTIFICATEHASEXPIRED:
            logger.warning('the certificate has expired - cannot fail right now, should be picked up by the job later on')
    else:
        logger.debug(f'will not download a new proxy since job.is_analysis()={job.is_analysis()}, '
                     f'job.infosys.queuedata.type={job.infosys.queuedata.type}, job.prodproxy={job.prodproxy}')


def dump_job_definition(res: dict):
    """
    Dump the job definition to the log, but hide any sensitive information.

    :param res: raw job definition (dict).
    """
    if 'secrets' in res:
        _pandasecrets = res['secrets']
        res['secrets'] = '********'
    else:
        _pandasecrets = ''
    if 'pilotSecrets' in res:
        _pilotsecrets = res['pilotSecrets']
        res['pilotSecrets'] = '********'
    else:
        _pilotsecrets = ''
    logger.info(f'job definition = {res}')
    if _pandasecrets:
        res['secrets'] = _pandasecrets
    if _pilotsecrets:
        res['pilotSecrets'] = _pilotsecrets


def print_node_info():
    """Print information about the local node to the log."""
    if is_virtual_machine():
        logger.info("pilot is running in a virtual machine")
    else:
        logger.info("pilot is not running in a virtual machine")


def create_job(dispatcher_response: dict, queuename: str) -> Any:
    """
    Create a job object out of the dispatcher response.

    :param dispatcher_response: raw job dictionary from the dispatcher (dict)
    :param queuename: queue name (str)
    :return: job object (Any)
    """
    # initialize (job specific) InfoService instance
    job = JobData(dispatcher_response)
    jobinfosys = InfoService()
    jobinfosys.init(queuename, infosys.confinfo, infosys.extinfo, JobInfoProvider(job))
    job.init(jobinfosys)

    logger.info(f'received job: {job.jobid} (sleep until the job has finished)')

    # payload environment wants the PANDAID to be set, also used below
    os.environ['PANDAID'] = job.jobid

    # reset pilot errors at the beginning of each new job
    errors.reset_pilot_errors()

    return job


def has_job_completed(queues: namedtuple, args: object) -> bool:
    """
    Check if the current job has completed (finished or failed).

    Note: the job object was extracted from monitored_payloads queue before this function was called.

    :param queues: Pilot queues object (namedtuple)
    :param args: Pilot arguments object (object)
    :return: True is the payload has finished or failed, False otherwise (bool).
    """
    # check if the job has finished
    try:
        job = queues.completed_jobs.get(block=True, timeout=1)
    except queue.Empty:
        # logger.info("(job still running)")
        pass
    else:
        make_job_report(job)
        cmd = f"ls -lF {os.environ.get('PILOT_HOME')}"
        logger.debug(f'{cmd}:\n')
        _, stdout, _ = execute(cmd)
        logger.debug(stdout)

        # empty the job queues
        queue_report(queues, purge=True)
        job.reset_errors()
        logger.info(f"job {job.jobid} has completed (purged errors)")

        # reset any running real-time logger
        rtcleanup()

        # reset proxy on unified queues for user jobs
        if job.prodproxy:
            os.environ['X509_USER_PROXY'] = job.prodproxy
            job.prodproxy = ''

        # cleanup of any remaining processes
        if job.pid and job.pid not in job.zombies:
            job.zombies.append(job.pid)
        cleanup(job, args)

        return True

    # is there anything in the finished_jobs queue?
    #finished_queue_snapshot = list(queues.finished_jobs.queue)
    #peek = [obj for obj in finished_queue_snapshot if jobid == obj.jobid]
    #if peek:
    #    logger.info(f"job {jobid} has completed (finished)")
    #    return True

    # is there anything in the failed_jobs queue?
    #failed_queue_snapshot = list(queues.failed_jobs.queue)
    #peek = [obj for obj in failed_queue_snapshot if jobid == obj.jobid]
    #if peek:
    #    logger.info(f"job {jobid} has completed (failed)")
    #    return True

    return False


def get_job_from_queue(queues: namedtuple, state: str) -> object or None:
    """
    Check if the job has finished or failed and if so return it.

    :param queues: Pilot queues object (namedtuple)
    :param state: job state (e.g. finished/failed) (str)
    :return: job object (object or None).
    """
    try:
        if state == "finished":
            job = queues.finished_jobs.get(block=True, timeout=1)
        elif state == "failed":
            job = queues.failed_jobs.get(block=True, timeout=1)
        else:
            job = None
    except queue.Empty:
        job = None
    else:
        # make sure that state=failed
        set_pilot_state(job=job, state=state)
        logger.info(f"job {job.jobid} has state=%s", job.state)

    return job


def is_queue_empty(queues: namedtuple, queuename: str) -> bool:
    """
    Check if the given queue is empty (without pulling).

    :param queues: Pilot queues object (namedtuple)
    :param queuename: queue name (str)
    :return: True if queue is empty, False otherwise (bool)
    """
    status = False
    if queuename in queues._fields:
        _queue = getattr(queues, queuename)
        jobs = list(_queue.queue)
        if len(jobs) > 0:
            logger.info(f'queue {queuename} not empty: found {len(jobs)} job(s)')
        else:
            logger.info(f'queue {queuename} is empty')
            status = True
    else:
        logger.warning(f'queue {queuename} not present in {queues._fields}')

    return status


def order_log_transfer(queues: namedtuple, job: object):
    """
    Order a log transfer for a failed job.

    :param queues: Pilot queues object (namedtuple)
    :param job: job object (object).
    """
    # add the job object to the data_out queue to have it staged out
    job.stageout = 'log'  # only stage-out log file
    #set_pilot_state(job=job, state='stageout')
    put_in_queue(job, queues.data_out)

    logger.debug('job added to data_out queue')

    # wait for the log transfer to finish
    counter = 0
    nmax = 60
    while counter < nmax:
        # refresh the log_transfer since it might have changed
        log_transfer = job.get_status('LOG_TRANSFER')
        logger.info(f'waiting for log transfer to finish (#{counter + 1}/#{nmax}): {log_transfer}')
        if is_queue_empty(queues, 'data_out') and log_transfer in {LOG_TRANSFER_DONE, LOG_TRANSFER_FAILED}:
            logger.info('stage-out of log has completed')
            break

        if log_transfer == LOG_TRANSFER_IN_PROGRESS:  # set in data component, job object is singleton
            logger.info('log transfer is in progress')
        time.sleep(2)
        counter += 1

    logger.info('proceeding with server update')


def wait_for_aborted_job_stageout(args: object, queues: namedtuple, job: object):
    """
    Wait for stage-out to finish for aborted job.

    :param args: Pilot arguments object (object)
    :param queues: Pilot queues object (namedtuple)
    :param job: job object (object).
    """
    # if the pilot received a kill signal, how much time has passed since the signal was intercepted?
    try:
        time_since_kill = get_time_since('1', PILOT_KILL_SIGNAL, args)
        was_killed = was_pilot_killed(args.timing)
        if was_killed:
            logger.info(f'{time_since_kill} s passed since kill signal was intercepted - make sure that stage-out has finished')
    except Exception as error:
        logger.warning('exception caught: %s', error)
        time_since_kill = 60

    if time_since_kill > 60 or time_since_kill < 0:  # fail-safe
        logger.warning('reset time_since_kill to 60 since value is out of allowed limits')
        time_since_kill = 60

    # if stage-out has not finished, we need to wait (less than two minutes or the batch system will issue
    # a hard SIGKILL)
    max_wait_time = 2 * 60 - time_since_kill - 5
    logger.debug(f'using max_wait_time = {max_wait_time} s')
    t0 = time.time()
    while time.time() - t0 < max_wait_time:
        if job in queues.finished_data_out.queue or job in queues.failed_data_out.queue:
            logger.info('stage-out has finished, proceed with final server update')
            break

        time.sleep(0.5)

    logger.info('proceeding with final server update')


def get_job_status(job: Any, key: str) -> str:
    """
    Return the job status corresponding to the given key.

    Wrapper function around job.get_status().
    If key = 'LOG_TRANSFER' but job object is not defined, the function will return value = LOG_TRANSFER_NOT_DONE.

    :param job: job object (Any)
    :param key: key name (str)
    :return: value (str).
    """
    value = ""
    if job:
        value = job.get_status(key)
    elif key == 'LOG_TRANSFER':
        value = LOG_TRANSFER_NOT_DONE

    return value


def queue_monitor(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Monitor queue activity.

    Thread.

    This function monitors queue activity, specifically if a job has finished or failed and then reports to the server.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object).
    """
    # scan queues until at least one queue has a job object. abort if it takes too long time
    if not scan_for_jobs(queues):
        logger.warning('queues are still empty of jobs - will begin queue monitoring anyway')

    job = None
    while True:  # will abort when graceful_stop has been set or if enough time has passed after kill signal
        time.sleep(1)

        if traces.pilot['command'] == 'abort':
            logger.warning('job queue monitor received an abort instruction')
            args.graceful_stop.set()

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        # (abort at the end of the loop)
        abort_thread = should_abort(args, label='job:queue_monitor')
        if abort_thread and os.environ.get('PILOT_WRAP_UP', '') == 'NORMAL':
            pause_queue_monitor(20)

        # check if the job has finished
        imax = 20
        i = 0
        while i < imax and os.environ.get('PILOT_WRAP_UP', '') == 'NORMAL':
            job = get_finished_or_failed_job(args, queues)
            if job:
                logger.debug(f'returned job has job.state={job.state} and job.completed={job.completed}')
                #if job.state == 'failed':
                #    logger.warning('will abort failed job (should prepare for final server update)')
                break
            i += 1
            state = get_pilot_state()  # the job object is not available, but the state is also kept in PILOT_JOB_STATE
            if state != 'stage-out':
                # logger.info("no need to wait since job state=\'%s\'", state)
                break
            if not abort_thread:
                pause_queue_monitor(1)
            else:
                pause_queue_monitor(10)

        # job has not been defined if it's still running
        if not job and not abort_thread:
            continue

        completed_jobids = queues.completed_jobids.queue if queues.completed_jobids else []
        if job and job.jobid not in completed_jobids:
            logger.info("preparing for final server update for job %s in state=\'%s\'", job.jobid, job.state)

            if args.job_aborted.is_set():
                # wait for stage-out to finish for aborted job
                wait_for_aborted_job_stageout(args, queues, job)

            # send final server update
            update_server(job, args)
            logger.debug(f'job.completed={job.completed}')
            # we can now stop monitoring this job, so remove it from the monitored_payloads queue and add it to the
            # completed_jobs queue which will tell retrieve() that it can download another job
            try:
                _job = queues.monitored_payloads.get(block=True, timeout=1) if args.workflow != 'stager' else None
            except queue.Empty:
                logger.warning('failed to dequeue job: queue is empty (did job fail before job monitor started?)')
                make_job_report(job)
            else:
                # now ready for the next job (or quit)
                put_in_queue(job.jobid, queues.completed_jobids)
                put_in_queue(job, queues.completed_jobs)
                if _job:
                    del _job

        if abort_thread:
            break

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='queue_monitor'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[job] queue monitor thread has finished')


def update_server(job: Any, args: Any) -> None:
    """
    Update the server (wrapper for send_state() that also prepares the metadata).

    :param job: job object (Any)
    :param args: Pilot arguments object (Any).
    """
    if job.completed:
        logger.warning('job has already completed - cannot send another final update')
        return

    # user specific actions
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
    metadata = user.get_metadata(job.workdir)
    try:
        user.update_server(job)
    except Exception as error:
        logger.warning('exception caught in update_server(): %s', error)
    if job.fileinfo:
        send_state(job, args, job.state, xml=dumps(job.fileinfo), metadata=metadata)
    else:
        send_state(job, args, job.state, metadata=metadata)


def pause_queue_monitor(delay: int):
    """
    Pause the queue monitor to let log transfer complete.

    Note: this function should use globally available object. Use sleep for now.

    :param delay: sleep time in seconds (int).
    """
    logger.warning(f'since job:queue_monitor is responsible for sending job updates, we sleep for {delay} s')
    time.sleep(delay)


def get_finished_or_failed_job(args: object, queues: namedtuple) -> Any:
    """
    Check if the job has either finished or failed and if so return it.

    If failed, order a log transfer. If the job is in state 'failed' and abort_job is set, set job_aborted.

    :param args: Pilot arguments object (object)
    :param queues: Pilot queues object (namedtuple)
    :return: job object (Any).
    """
    job = get_job_from_queue(queues, "finished")
    if job:
        # logger.debug('get_finished_or_failed_job: job has finished')
        pass
    else:
        # logger.debug('check_job: job has not finished')
        job = get_job_from_queue(queues, "failed")
        if job:
            logger.debug('get_finished_or_failed_job: job has failed')
            job.state = 'failed'
            args.job_aborted.set()

            # get the current log transfer status
            log_transfer = get_job_status(job, 'LOG_TRANSFER')
            if log_transfer == LOG_TRANSFER_NOT_DONE:
                # order a log transfer for a failed job
                order_log_transfer(queues, job)

    # check if the job has failed
    if job and job.state == 'failed':
        # set job_aborted in case of kill signals
        if args.abort_job.is_set():
            logger.warning('queue monitor detected a set abort_job (due to a kill signal)')
            # do not set graceful stop if pilot has not finished sending the final job update
            # i.e. wait until SERVER_UPDATE is DONE_FINAL
            #check_for_final_server_update(args.update_server)
            #args.job_aborted.set()

    return job


def get_heartbeat_period(debug: bool = False) -> int:
    """
    Return the proper heartbeat period, as determined by normal or debug mode.

    In normal mode, the heartbeat period is 30*60 s, while in debug mode it is 5*60 s. Both values are defined in the
    config file.

    :param debug: Boolean, True for debug mode. False otherwise (bool)
    :return: heartbeat period (int).
    """
    try:
        return int(config.Pilot.heartbeat if not debug else config.Pilot.debug_heartbeat)
    except Exception as error:
        logger.warning('bad config data for heartbeat period: %s (will use default 1800 s)', error)
        return 1800


def check_for_abort_job(args: Any, caller: str = '') -> bool:
    """
    Check if args.abort_job.is_set().

    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (Any)
    :param caller: function name of caller (str)
    :return: True if args_job.is_set(), False otherwise (bool).
    """
    abort_job = False
    if args.abort_job.is_set():
        logger.warning(f'{caller} detected an abort_job request (signal=args.signal)')
        abort_job = True

    return abort_job


def fast_monitor_tasks(job: Any) -> int:
    """
    Perform user specific fast monitoring tasks.

    :param job: job object (Any)
    :return: exit code (int).
    """
    exit_code = 0

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.monitoring', globals(), locals(), [pilot_user], 0)
    try:
        exit_code = user.fast_monitor_tasks(job)
    except Exception as exc:
        logger.warning('caught exception: %s', exc)

    return exit_code


def message_listener(queues: namedtuple, traces: Any, args: object):
    """
    Listen for messages from ActiveMQ.

    Thread.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    while not args.graceful_stop.is_set() and args.subscribe_to_msgsvc:

        # listen for a message and add it to the messages queue
        message = get_message_from_mb(args)  # in blocking mode
        if args.graceful_stop.is_set():
            break

        # if kill_task or finish_task instructions are received, abort this thread as it will not be needed any longer
        if message and (message['msg_type'] == 'kill_task' or message['msg_type'] == 'finish_task'):
            put_in_queue(message, queues.messages)  # will only be put in the queue if not there already
            if message['kill_task']:
                args.graceful_stop.set()
                # kill running job?
            break
        if message and message['msg_type'] == 'get_job':
            put_in_queue(message, queues.messages)  # will only be put in the queue if not there already
            continue  # wait for the next message

        if args.amq:
            logger.debug('got the amq instance')
        else:
            logger.debug('no amq instance')
        time.sleep(1)

    if args.amq:
        logger.debug('got the amq instance 2')
    else:
        logger.debug('no amq instance 2')

    # proceed to set the job_aborted flag?
    if args.subscribe_to_msgsvc:
        if threads_aborted(caller='message_listener'):
            logger.debug('will proceed to set job_aborted')
            args.job_aborted.set()

    if args.amq:
        logger.debug('closing ActiveMQ connections')
        args.amq.close_connections()

    logger.info('[job] message listener thread has finished')


def fast_job_monitor(queues: namedtuple, traces: Any, args: object) -> None:
    """
    Fast monitoring of job parameters.

    Thread.

    This function can be used for monitoring processes below the one minute threshold of the normal job_monitor thread.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    # peeking and current time; peeking_time gets updated if and when jobs are being monitored, update_time is only
    # used for sending the heartbeat and is updated after a server update
    #peeking_time = int(time.time())
    #update_time = peeking_time

    # end thread immediately if this pilot should never use realtime logging
    if not args.use_realtime_logging:
        logger.warning('fast monitoring not required by pilot option - ending thread')
        return

    if True:
        logger.info('fast monitoring thread disabled')
        return

    while not args.graceful_stop.is_set():
        time.sleep(10)

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        # (abort at the end of the loop)
        abort = should_abort(args, label='job:fast_job_monitor')
        if abort:
            break

        if traces.pilot.get('command') == 'abort':
            logger.warning('fast job monitor received an abort command')
            break

        # check for any abort_job requests
        abort_job = check_for_abort_job(args, caller='fast job monitor')
        if abort_job:
            break

        # peek at the jobs in the validated_jobs queue and send the running ones to the heartbeat function
        jobs = queues.monitored_payloads.queue
        if jobs:
            for i in range(len(jobs)):
                #current_id = jobs[i].jobid
                if jobs[i].state in {'finished', 'failed'}:
                    logger.info('will abort fast job monitoring soon since job state=%s (job is still in queue)', jobs[i].state)
                    break

                # perform the monitoring tasks
                exit_code = fast_monitor_tasks(jobs[i])
                if exit_code:
                    logger.debug(f'fast monitoring reported an error: {exit_code}')

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='fast_job_monitor'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    logger.info('[job] fast job monitor thread has finished')


def job_monitor(queues: namedtuple, traces: Any, args: object):  # noqa: C901
    """
    Monitor job parameters.

    Thread.

    This function monitors certain job parameters, such as job looping, at various time intervals. The main loop
    is executed once a minute, while individual verifications may be executed at any time interval (>= 1 minute). E.g.
    looping jobs are checked once every ten minutes (default) and the heartbeat is sent once every 30 minutes. Memory
    usage is checked once a minute.

    :param queues: internal queues for job handling (namedtuple)
    :param traces: tuple containing internal pilot states (Any)
    :param args: Pilot arguments object (e.g. containing queue name, queuedata dictionary, etc) (object)
    """
    # initialize the monitoring time object
    mt = MonitoringTime()

    # peeking and current time; peeking_time gets updated if and when jobs are being monitored, update_time is only
    # used for sending the heartbeat and is updated after a server update
    start_time = int(time.time())
    peeking_time = start_time
    update_time = peeking_time

    # keep track of jobs we don't want to continue monitoring
    # no_monitoring = {}  # { job:id: time.time(), .. }

    # fail-safe to be able to report a job that is still running after the pilot has been ordered to abort
    final_job = None

    # overall loop counter (ignoring the fact that more than one job may be running)
    n = 0
    cont = True
    while cont:

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        abort = should_abort(args, label='job:job_monitor')
        if abort:
            logger.info('aborting loop')
            cont = False
            break

        time.sleep(0.5)

        if traces.pilot.get('command') == 'abort':
            logger.warning('job monitor received an abort command')

        # check for any abort_job requests (either kill signal or tobekilled command)
        abort_job = check_for_abort_job(args, caller='job monitor')
        if not abort_job:
            if not queues.current_data_in.empty():
                # make sure to send heartbeat regularly if stage-in takes a long time
                jobs = queues.current_data_in.queue
                if jobs:
                    for i in range(len(jobs)):
                        # send heartbeat if it is time (note that the heartbeat function might update the job object, e.g.
                        # by turning on debug mode, ie we need to get the heartbeat period in case it has changed)
                        try:
                            update_time = send_heartbeat_if_time(jobs[i], args, update_time)
                        except (ValueError, TypeError) as exc:
                            logger.warning(f'exception caught during send_heartbeat_if_time: {exc}')

                        # note: when sending a state change to the server, the server might respond with 'tobekilled'
                        try:
                            jobs[i]
                            final_job = jobs[i]
                        except Exception as error:
                            logger.warning('detected stale jobs[i] object in job_monitor: %s', error)
                        else:
                            if jobs[i].state == 'failed':
                                logger.warning('job state is \'failed\' - order log transfer and abort job_monitor() (1)')
                                jobs[i].stageout = 'log'  # only stage-out log file
                                put_in_queue(jobs[i], queues.data_out)

                    # sleep for a while if stage-in has not completed
                    time.sleep(1)
                    continue
            elif queues.finished_data_in.empty():
                # sleep for a while if stage-in has not completed
                time.sleep(1)
                continue

        # peek at the jobs in the validated_jobs queue and send the running ones to the heartbeat function
        jobs = queues.monitored_payloads.queue
        if jobs:
            # update the peeking time
            peeking_time = int(time.time())

            # stop_monitoring = False  # continue with the main loop (while cont)
            for i in range(len(jobs)):
                try:
                    current_id = jobs[i].jobid
                    error_code = None
                    if abort_job and args.signal:
                        # if abort_job and a kill signal was set
                        error_code = get_signal_error(args.signal)
                    elif abort_job:  # i.e. no kill signal
                        logger.info('tobekilled seen by job_monitor (error code should already be set) - abort job only')
                        # set error code so the server can be informed
                        error_code = errors.PANDAKILL
                    elif os.environ.get('REACHED_MAXTIME', None):
                        # the batch system max time has been reached, time to abort (in the next step)
                        logger.info('REACHED_MAXTIME seen by job monitor - sleeping up to 30 s before aborting job')
                        counter = 0
                        while os.environ['SERVER_UPDATE'] != SERVER_UPDATE_FINAL and counter < 30:
                            time.sleep(1)
                            counter += 1

                        if not args.graceful_stop.is_set():
                            logger.info('setting graceful_stop since it was not set already')
                            args.graceful_stop.set()
                        error_code = errors.REACHEDMAXTIME

                    if error_code:
                        jobs[i].state = 'failed'
                        jobs[i].piloterrorcodes, jobs[i].piloterrordiags = errors.add_error_code(error_code)
                        jobs[i].completed = True
                        #if not jobs[i].completed:  # job.completed gets set to True after a successful final server update:
                        send_state(jobs[i], args, jobs[i].state)
                        if jobs[i].pid:
                            logger.debug('killing payload processes')
                            kill_processes(jobs[i].pid)

                    logger.info(f"monitor loop #{n}: job {i}:{current_id} is in state \'{jobs[i].state}\'")
                    if jobs[i].state in {'finished', 'failed'}:
                        logger.info('will abort job monitoring soon since job state=%s (job is still in queue)', jobs[i].state)
                        if args.workflow == 'stager':  # abort interactive stager pilot, this will trigger an abort of all threads
                            set_pilot_state(job=jobs[i], state="finished")
                            logger.info('ordering log transfer')
                            jobs[i].stageout = 'log'  # only stage-out log file
                            put_in_queue(jobs[i], queues.data_out)
                            cont = False
                        # no_monitoring[current_id] = int(time.time())
                        break

                    # perform the monitoring tasks
                    exit_code, diagnostics = job_monitor_tasks(jobs[i], mt, args)
                    logger.debug(f'job_monitor_tasks returned {exit_code}, {diagnostics}')
                    if exit_code != 0:
                        # do a quick server update with the error diagnostics only
                        preliminary_server_update(jobs[i], args, diagnostics)
                        if exit_code == errors.VOMSPROXYABOUTTOEXPIRE:
                            # attempt to download a new proxy since it is about to expire
                            ec = download_new_proxy(role='production')
                            exit_code = ec if ec != 0 else 0  # reset the exit_code if success
                        if exit_code in {errors.KILLPAYLOAD, errors.NOVOMSPROXY, errors.CERTIFICATEHASEXPIRED}:
                            jobs[i].piloterrorcodes, jobs[i].piloterrordiags = errors.add_error_code(exit_code)
                            logger.debug('killing payload process')
                            kill_process(jobs[i].pid)
                            break
                        if exit_code == errors.LEASETIME:  # stager mode, order log stage-out
                            set_pilot_state(job=jobs[i], state="finished")
                            logger.info('ordering log transfer')
                            jobs[i].stageout = 'log'  # only stage-out log file
                            put_in_queue(jobs[i], queues.data_out)
                        elif exit_code == 0:
                            # ie if download of new proxy was successful
                            diagnostics = ""
                            break
                        else:
                            try:
                                fail_monitored_job(jobs[i], exit_code, diagnostics, queues, traces)
                            except Exception as error:
                                logger.warning('(1) exception caught: %s (job id=%s)', error, current_id)
                            break

                    # run this check again in case job_monitor_tasks() takes a long time to finish (and the job object
                    # has expired in the meantime)
                    try:
                        _job = jobs[i]
                    except (IndexError, TypeError):
                        logger.info('aborting job monitoring since job object (job id=%s) has expired', current_id)
                        break

                    # send heartbeat if it is time (note that the heartbeat function might update the job object, e.g.
                    # by turning on debug mode, ie we need to get the heartbeat period in case it has changed)
                    try:
                        update_time = send_heartbeat_if_time(_job, args, update_time)
                    except (ValueError, TypeError) as error:
                        logger.warning('exception caught: %s (job id=%s)', error, current_id)
                        break
                    else:
                        # note: when sending a state change to the server, the server might respond with 'tobekilled'
                        # only if combined with tobekilled, in which case errors.PANDAKILL is set
                        if _job.state == 'failed' and errors.PANDAKILL in _job.piloterrorcodes:
                            logger.warning('job state is \'failed\' and errors.PANDAKILL is set - order log transfer and abort job_monitor() (2)')
                            _job.stageout = 'log'  # only stage-out log file
                            put_in_queue(_job, queues.data_out)
                            #abort = True
                            break

                except Exception as exc:
                    # did the job object expire?
                    logger.warning(f'exception caught: {exc}')
                    continue

            #if stop_monitoring:
            #    continue

        elif os.environ.get('PILOT_JOB_STATE') == 'stagein':
            logger.info('job monitoring is waiting for stage-in to finish')

        n += 1

        if abort_job:
            logger.warning('cannot recover job monitoring - aborting pilot')
            args.graceful_stop.set()

        # abort in case graceful_stop has been set, and less than 30 s has passed since MAXTIME was reached (if set)
        abort = should_abort(args, label='job:job_monitor')
        if abort:
            logger.info('will abort loop')
            cont = False

    # proceed to set the job_aborted flag?
    if threads_aborted(caller='job_monitor'):
        logger.debug('will proceed to set job_aborted')
        args.job_aborted.set()

    # fail-safe
    if os.environ.get('REACHED_MAXTIME', None):
        if not final_job:
            logger.warning('REACHED_MAXTIME seen by job monitor - but final job object not set, cannot report')
        else:
            logger.warning('REACHED_MAXTIME seen by job monitor - will report final job')
            try:
                fail_monitored_job(final_job, errors.REACHEDMAXTIME, "Reached maxtime", queues, traces)
            except Exception as error:
                logger.warning('(1) exception caught: %s (job id=%s)', error, current_id)

    logger.info('[job] job monitor thread has finished')


def preliminary_server_update(job: Any, args: Any, diagnostics: str):
    """
    Send a quick job update to the server (do not send any error code yet) for a failed job.

    :param job: job object (Any)
    :param args: Pilot arguments object (Any)
    :param diagnostics: error diagnostics (str).
    """
    logger.warning(f'will send preliminary diagnostics (and pretend job is still running)={diagnostics}')
    piloterrorcode = job.piloterrorcode
    piloterrorcodes = job.piloterrorcodes
    piloterrordiags = job.piloterrordiags
    job.piloterrorcode = 0
    job.piloterrorcodes = []
    job.piloterrordiags = [diagnostics]
    send_state(job, args, 'running')
    job.piloterrorcode = piloterrorcode
    job.piloterrorcodes = piloterrorcodes
    job.piloterrordiags = piloterrordiags


def get_signal_error(sig: Any) -> int:
    """
    Return a corresponding pilot error code for the given signal.

    :param sig: signal (Any)
    :return: pilot error code (int).
    """
    try:
        _sig = str(sig)  # e.g. 'SIGTERM'
    except ValueError:
        ret = errors.KILLSIGNAL
    else:
        codes = {'SIGBUS': errors.SIGBUS,
                 'SIGQUIT': errors.SIGQUIT,
                 'SIGSEGV': errors.SIGSEGV,
                 'SIGTERM': errors.SIGTERM,
                 'SIGXCPU': errors.SIGXCPU,
                 'SIGUSR1': errors.SIGUSR1,
                 'USERKILL': errors.USERKILL}
        ret = codes.get(_sig) if _sig in codes else errors.KILLSIGNAL

    return ret


def download_new_proxy(role: str = 'production', proxy_type: str = '', workdir: str = '') -> int:
    """
    Download a new production proxy, since it has expired.

    If it fails to download and verify a new proxy, return the NOVOMSPROXY error.

    :param role: role, 'production' or 'user' (str)
    :param proxy_type: proxy type, e.g. unified (str)
    :param workdir: payload work directory (str)
    :return: exit code (int).
    """
    exit_code = 0
    x509 = os.environ.get('X509_USER_PROXY', '')
    logger.info(f'attempt to download a new proxy (proxy_type={proxy_type})')

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.proxy', globals(), locals(), [pilot_user], 0)

    voms_role = user.get_voms_role(role=role)
    ec, _, new_x509 = user.get_and_verify_proxy(x509, voms_role=voms_role, proxy_type=proxy_type, workdir=workdir)
    if ec != 0:  # do not return non-zero exit code if only download fails
        logger.warning('failed to download/verify new proxy')
        if ec == errors.ARCPROXYLIBFAILURE:
            logger.warning("currently ignoring arcproxy library failure")
        else:
            exit_code = errors.CERTIFICATEHASEXPIRED if ec == errors.CERTIFICATEHASEXPIRED else errors.NOVOMSPROXY
    elif new_x509 and new_x509 != x509 and 'unified' in new_x509 and os.path.exists(new_x509):
        os.environ['X509_UNIFIED_DISPATCH'] = new_x509
        logger.debug(f'set X509_UNIFIED_DISPATCH to {new_x509}')
        # already dumped right after proxy download:
        #cmd = f'export X509_USER_PROXY={os.environ.get("X509_UNIFIED_DISPATCH")};echo $X509_USER_PROXY; voms-proxy-info -all'
        #_, stdout, _ = execute(cmd)
        #logger.debug(f'cmd={cmd}:\n{stdout}')
    else:
        logger.debug(f'will not set X509_UNIFIED_DISPATCH since new_x509={new_x509}, x509={x509}, os.path.exists(new_x509)={os.path.exists(new_x509)}')

    return exit_code


def send_heartbeat_if_time(job: Any, args: Any, update_time: float) -> int:
    """
    Send a heartbeat to the server if it is time to do so.

    :param job: job object (Any)
    :param args: Pilot arguments object (Any)
    :param update_time: last update time (from time.time()) (float)
    :return: possibly updated update_time, converted to int (from time.time()) (int).
    """
    if job.completed:
        logger.info('job already completed - will not send any further updates')
    elif int(time.time()) - update_time >= get_heartbeat_period(job.debug and job.debug_command):
        # check for state==running here, and send explicit 'running' in send_state, rather than sending job.state
        # since the job state can actually change in the meantime by another thread
        # job.completed will anyway be checked in https::send_update()
        if job.serverstate not in {'finished', 'failed'}:
            logger.info(f'will send heartbeat for job in \'{job.state}\' state')
            logger.info("note: will only send \'running\' state to server to prevent sending any final state too early")
            send_state(job, args, "running")
            update_time = time.time()
    else:
        logger.debug('will not send any job update')

    return int(update_time)


def fail_monitored_job(job: object, exit_code: int, diagnostics: str, queues: namedtuple, traces: Any):
    """
    Fail a monitored job.

    :param job: job object (object)
    :param exit_code: exit code from job_monitor_tasks (int)
    :param diagnostics: pilot error diagnostics (str)
    :param queues: queues object (namedtuple)
    :param traces: traces object (Any).
    """
    set_pilot_state(job=job, state="failed")
    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code, msg=diagnostics)
    job.piloterrordiag = diagnostics
    traces.pilot['error_code'] = exit_code
    put_in_queue(job, queues.failed_payloads)
    logger.info('aborting job monitoring since job state=%s', job.state)


def make_job_report(job: Any):
    """
    Make a summary report for the given job.

    This function is called when the job has completed.

    :param job: job object (Any).
    """
    logger.info('')
    logger.info('job summary report')
    logger.info('--------------------------------------------------')
    logger.info(f'PanDA job id: {job.jobid}')
    logger.info(f'task id: {job.taskid}')
    n = len(job.piloterrorcodes)
    if n > 0:
        for i in range(n):
            logger.info(f'error {i + 1}/{n}: {job.piloterrorcodes[i]}: {job.piloterrordiags[i]}')
    else:
        logger.info('errors: (none)')
    if job.piloterrorcode != 0:
        logger.info(f'pilot error code: {job.piloterrorcode}')
        logger.info(f'pilot error diag: {job.piloterrordiag}')
    info = ""
    for key in job.status:
        info += key + " = " + job.status[key] + " "
    logger.info(f'status: {info}')
    s = ""
    if job.is_analysis() and job.state != 'finished':
        s = '(user job is recoverable)' if errors.is_recoverable(code=job.piloterrorcode) else '(user job is not recoverable)'
    logger.info(f'pilot state: {job.state} {s}')
    logger.info(f'transexitcode: {job.transexitcode}')
    logger.info(f'exeerrorcode: {job.exeerrorcode}')
    logger.info(f'exeerrordiag: {job.exeerrordiag}')
    logger.info(f'exitcode: {job.exitcode}')
    logger.info(f'exitmsg: {job.exitmsg}')
    logger.info(f'cpuconsumptiontime: {job.cpuconsumptiontime} {job.cpuconsumptionunit}')
    logger.info(f'nevents: {job.nevents}')
    logger.info(f'neventsw: {job.neventsw}')
    logger.info(f'pid: {job.pid}')
    logger.info(f'pgrp: {job.pgrp}')
    logger.info(f'corecount: {job.corecount}')
    logger.info(f'event service: {job.is_eventservice}')
    logger.info(f'sizes: {job.sizes}')
    logger.info('--------------------------------------------------')
    logger.info('')
