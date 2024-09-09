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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-24

"""Functions for interactiving with Harvester."""

import logging
import os
import os.path
import socket
from typing import Any

from pilot.util.config import config
from pilot.util.filehandling import write_json, touch, remove, read_json, get_checksum_value
from pilot.util.timing import time_stamp

logger = logging.getLogger(__name__)


def dump(obj: Any):
    """
    Dump given object to stdout.

    :param obj: object (Any).
    """
    for attr in dir(obj):
        print(f"obj.{attr} = {getattr(obj, attr)}")


def is_harvester_mode(args: Any) -> bool:
    """
    Determine if the pilot is running in Harvester mode.

    :param args: Pilot arguments object (Any)
    :return: True if Harvester mode, False otherwise (bool).
    """
    if (args.harvester_workdir != '' or args.harvester_datadir != '') and not args.update_server:
        harvester = True
    elif (args.harvester_eventstatusdump != '' or args.harvester_workerattributes != '') and not args.update_server:
        harvester = True
    elif ('HARVESTER_ID' in os.environ or 'HARVESTER_WORKER_ID' in os.environ) and args.harvester_submitmode.lower() == 'push':
        harvester = True
    else:
        harvester = False

    return harvester


def get_job_request_file_name() -> str:
    """
    Return the name of the job request file as defined in the pilot config file.

    :return: job request file name (str).
    """
    return os.path.join(os.environ.get('PILOT_HOME'), config.Harvester.job_request_file)


def remove_job_request_file():
    """Remove an old job request file when it is no longer needed."""
    path = get_job_request_file_name()
    if os.path.exists(path):
        if remove(path) == 0:
            logger.info(f'removed {path}')
    else:
        logger.debug('there is no job request file')


def request_new_jobs(njobs: int = 1) -> bool:
    """
    Inform Harvester that the pilot is ready to process new jobs by creating a job request file.

    The request file will contain the desired number of jobs.

    :param njobs: Number of jobs. Default is 1 since on grids and clouds the pilot does not know how many jobs it can
    process before it runs out of time (int)
    :return: True if the job request file was successfully created, False otherwise (bool).
    """
    path = get_job_request_file_name()
    if os.path.exists(path):
        logger.warning(f'job request file already exists: {path}')
        return True

    dictionary = {'nJobs': njobs}
    logger.info(f'requesting {njobs} new job(s) by creating {path}')
    # write it to file
    status = write_json(path, dictionary)
    if not status:
        logger.warning("failed to request new job from Harvester")
        return False

    return True


def kill_worker():
    """
    Create (touch) a kill_worker file in the pilot launch directory.

    This file will let Harverster know that the pilot has finished.
    """
    touch(os.path.join(os.environ['PILOT_HOME'], config.Harvester.kill_worker_file))


def get_initial_work_report() -> dict:
    """
    Prepare the work report dictionary.

    Note: the work_report should also contain all fields defined in parse_jobreport_data().

    :return: work report dictionary (dict).
    """
    # set a timeout of 10 seconds to prevent potential hanging due to problems with DNS resolution, or if the DNS
    # server is slow to respond
    socket.setdefaulttimeout(10)
    try:
        hostname = socket.gethostname()
    except socket.herror as exc:
        logger.warning(f'failed to get hostname: {exc}')
        hostname = 'localhost'

    return {'jobStatus': 'starting',
            'messageLevel': logging.getLevelName(logger.getEffectiveLevel()),
            'cpuConversionFactor': 1.0,
            'cpuConsumptionTime': '',
            'node': os.environ.get('PANDA_HOSTNAME', hostname),
            'workdir': '',
            'timestamp': time_stamp(),
            'endTime': '',
            'transExitCode': 0,
            'pilotErrorCode': 0,  # only add this in case of failure?
            }


def get_event_status_file(args: Any) -> str:
    """
    Return the name of the event_status.dump file.

    :param args: Pilot arguments object (Any)
    :return: event staus file name (str).
    """
    logger.debug(f'config.Harvester.__dict__ : {config.Harvester.__dict__}')

    if args.harvester_workdir != '':
        work_dir = args.harvester_workdir
    else:
        work_dir = os.environ['PILOT_HOME']
    event_status_file = config.Harvester.stageoutn_file
    event_status_file = os.path.join(work_dir, event_status_file)
    logger.debug(f'event_status_file = {event_status_file}')

    return event_status_file


def get_worker_attributes_file(args: Any):
    """
    Return the name of the worker attributes file.

    :param args: Pilot arguments object (Any)
    :return: worker attributes file name (str).
    """
    if args.harvester_workdir != '':
        work_dir = args.harvester_workdir
    else:
        work_dir = os.environ['PILOT_HOME']

    return os.path.join(work_dir, config.Harvester.workerattributes_file)


def findfile(path: str, name: str) -> str:
    """
    Find the first instance of file in the directory tree.

    :param path: directory tree to search (str)
    :param name: name of the file to search (str)
    :return: the path to the first instance of the file (str).
    """
    filename = ""
    for root, dirs, files in os.walk(path):
        if name in files:
            filename = os.path.join(root, name)
            break

    return filename


def publish_stageout_files(job: Any, event_status_file: str) -> bool:
    """
    Publish the work report for stageout.

    The work report dictionary should contain the fields defined in get_initial_work_report().

    :param job: job object (Any)
    :param event_status_file: file ane (str)
    :return: status of writing the file information to a json (bool).
    """
    # get the harvester workdir from the event_status_file
    work_dir = os.path.dirname(event_status_file)

    out_file_report = {}
    out_file_report[job.jobid] = []

    # first look at the logfile information (logdata) from the FileSpec objects
    for fspec in job.logdata:
        logger.debug(f"file {fspec.lfn} will be checked and declared for stage out")
        # find the first instance of the file
        filename = os.path.basename(fspec.surl)
        path = findfile(work_dir, filename)
        logger.debug(f"found File {fspec.lfn} at path - {path}")
        #
        file_desc = {}
        file_desc['type'] = fspec.filetype
        file_desc['path'] = path
        file_desc['guid'] = fspec.guid
        file_desc['fsize'] = fspec.filesize
        file_desc['chksum'] = get_checksum_value(fspec.checksum)
        logger.debug(f"file description - {file_desc} ")
        out_file_report[job.jobid].append(file_desc)

    # Now look at the output file(s) information (outdata) from the FileSpec objects
    for fspec in job.outdata:
        logger.debug(f"file {fspec.lfn} will be checked and declared for stage out")
        if fspec.status != 'transferred':
            logger.debug('will not add the output file to the json since it was not produced or transferred')
        else:
            # find the first instance of the file
            filename = os.path.basename(fspec.surl)
            path = findfile(work_dir, filename)
            if not path:
                logger.warning(f'file {path} was not found - will not be added to json')
            else:
                logger.debug(f"found File {fspec.lfn} at {path}")
                #
                file_desc = {}
                file_desc['type'] = fspec.filetype
                file_desc['path'] = path
                file_desc['guid'] = fspec.guid
                file_desc['fsize'] = fspec.filesize
                file_desc['chksum'] = get_checksum_value(fspec.checksum)
                logger.debug(f"File description - {file_desc} ")
                out_file_report[job.jobid].append(file_desc)

    if out_file_report[job.jobid]:
        if write_json(event_status_file, out_file_report):
            logger.debug(f'stagout declared in: {event_status_file}')
            logger.debug(f'report for stageout: {out_file_report}')
            return True
        else:
            logger.debug(f'failed to declare stagout in: {event_status_file}')
            return False
    else:
        logger.debug('no report for stageout')
        return False


def publish_work_report(work_report: dict = None, worker_attributes_file: str = "worker_attributes.json") -> bool:
    """
    Publish the work report.

    The work report dictionary should contain the fields defined in get_initial_work_report().

    :param work_report: work report dictionary (dict)
    :param worker_attributes_file: file name (str)
    :raises FileHandlingFailure: in case of IOError
    :return: True if successfully published, False otherwise (bool).
    """
    if work_report is None:
        work_report = {}
    if work_report:
        work_report['timestamp'] = time_stamp()
        if "outputfiles" in work_report:
            del (work_report["outputfiles"])
        if "inputfiles" in work_report:
            del (work_report["inputfiles"])
        if "xml" in work_report:
            del (work_report["xml"])

        status = write_json(worker_attributes_file, work_report)
        if not status:
            logger.error(f"work report publish failed: {work_report}")
            return False
        else:
            logger.info(f"work report published: {work_report}")
            return True
    else:
        # No work_report return False
        return False


def publish_job_report(job: Any, args: Any, job_report_file: str = "jobReport.json") -> str:
    """
    Publish the job report.

    Copy job report file to make it accessible by Harvester. Shrink job report file.

    :param job: job object (Any)
    :param args: Pilot arguments object (Any)
    :param job_report_file: name of job report (str)
    :raises FileHandlingFailure: in case of IOError
    :return True if successfully published, False otherwise (bool).
    """
    src_file = os.path.join(job.workdir, job_report_file)
    dst_file = os.path.join(args.harvester_workdir, job_report_file)

    try:
        logger.info(f"copy of payload report [{job_report_file}] to access point: {args.harvester_workdir}")
        # shrink jobReport
        job_report = read_json(src_file)
        if 'executor' in job_report:
            for executor in job_report['executor']:
                if 'logfileReport' in executor:
                    executor['logfileReport'] = {}

        if write_json(dst_file, job_report):
            return True
        else:
            return False

    except IOError:
        logger.error("job report copy failed")
        return False


def parse_job_definition_file(filename: str) -> list:
    """
    Parse the Harvester job definition file and re-package the job definition dictionaries.

    The format of the Harvester job definition dictionary is:
    dict = { job_id: { key: value, .. }, .. }
    The function returns a list of these dictionaries each re-packaged as
    dict = { key: value } (where the job_id is now one of the key-value pairs: 'jobid': job_id)

    :param filename: file name (str)
    :return: list of job definition dictionaries (list).
    """
    job_definitions_list = []

    # re-package dictionaries
    job_definitions_dict = read_json(filename)
    if job_definitions_dict:
        for job_id in job_definitions_dict:
            res = {'jobid': job_id}
            res.update(job_definitions_dict[job_id])
            job_definitions_list.append(res)

    return job_definitions_list
