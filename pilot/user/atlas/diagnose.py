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

import json
import os
import re
import logging
from glob import glob

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    BadXML,
    FileHandlingFailure,
    NoSuchFile,
    PilotException,
)
from pilot.info.jobdata import JobData
from pilot.util.config import config
from pilot.util.filehandling import (
    copy,
    get_guid,
    grep,
    open_file,
    read_file,
    scan_file,
    tail,
    write_json,
)
from pilot.util.math import convert_mb_to_b
from pilot.util.workernode import get_local_disk_space

from .common import (
    update_job_data,
    parse_jobreport_data
)
from .metadata import (
    get_guid_from_xml,
    get_metadata_from_xml,
    get_total_number_of_events,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def interpret(job: JobData) -> int:
    """
    Interpret the payload, look for specific errors in the stdout.

    :param job: job object (JobData)
    :return: exit code (payload) (int).
    """
    exit_code = 0

    # extract errors from job report
    process_job_report(job)
    if job.piloterrorcodes:
        # ignore metadata error if trf exit code is non-zero
        if len(job.piloterrorcodes) == 1 and errors.NOPAYLOADMETADATA in job.piloterrorcodes and job.transexitcode != 0:
            logger.warning('ignore metadata error for now')
        if job.piloterrorcodes[0] < 1000:
            logger.warning(f"recorded error code is not a pilot error code: {job.piloterrorcodes[0]} - resetting to UNKNOWNTRFFAILURE")
            job.piloterrorcodes[0] = errors.UNKNOWNTRFFAILURE
        else:
            logger.warning(f'aborting payload error diagnosis since an error has already been set: {job.piloterrorcodes}')
            return -1

    if job.exitcode != 0:
        exit_code = job.exitcode

    # check for special errors
    if exit_code == 146:
        logger.warning(f'user tarball was not downloaded (payload exit code {exit_code})')
        set_error_nousertarball(job)
    elif exit_code == 160:
        logger.info(f'ignoring harmless preprocess exit code {exit_code}')
        job.transexitcode = 0
        job.exitcode = 0
        exit_code = 0

    # extract special information, e.g. number of events
    try:
        extract_special_information(job)
    except PilotException as exc:
        logger.error(f'PilotException caught while extracting special job information: {exc}')
        exit_code = exc.get_error_code()
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)

    # interpret the exit info from the payload
    try:
        interpret_payload_exit_info(job)
    except Exception as exc:
        logger.warning(f'exception caught while interpreting payload exit info: {exc}')

    return exit_code


def interpret_payload_exit_info(job: JobData):
    """
    Interpret the exit info from the payload.

    :param job: job object (JobData).
    """
    # try to identify out of memory errors in the stderr
    if is_out_of_memory(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADOUTOFMEMORY, priority=True)
        return

    # look for specific errors in the stdout (tail)
    if is_installation_error(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGINSTALLATION, priority=True)
        return

    # did AtlasSetup fail?
    if is_atlassetup_error(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.SETUPFATAL, priority=True)
        return

    # did the payload run out of space?
    if is_out_of_space(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOLOCALSPACE, priority=True)

        # double check local space
        try:
            disk_space = get_local_disk_space(os.getcwd())
        except PilotException as exc:
            diagnostics = exc.get_detail()
            logger.warning(f'exception caught while executing df: {diagnostics} (ignoring)')
        else:
            if disk_space:
                spaceleft = convert_mb_to_b(disk_space)  # B (diskspace is in MB)
                logger.info(f'remaining local space: {spaceleft} B')
            else:
                logger.warning('get_local_disk_space() returned None')
        return

    # look for specific errors in the stdout (full)
    if is_nfssqlite_locking_problem(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NFSSQLITE, priority=True)
        return

    # is the user tarball missing on the server?
    if is_user_code_missing(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGUSERCODE, priority=True)
        return

    # set a general Pilot error code if the payload error could not be identified
    if job.transexitcode == 0 and job.exitcode != 0:
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.UNKNOWNPAYLOADFAILURE, priority=True)


def is_out_of_memory(job: JobData) -> bool:
    """
    Check of the payload ran out of memory.

    :param job: job object (JobData)
    :return: True means the error was found (bool).
    """
    out_of_memory = False

    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)

    files = {stderr: ["FATAL out of memory: taking the application down"], stdout: ["St9bad_alloc", "std::bad_alloc"]}
    for path, patterns in files.items():
        if os.path.exists(path):
            logger.info(f'looking for out-of-memory errors in {os.path.basename(path)}')
            if os.path.getsize(path) > 0:
                matched_lines = grep(patterns, path)
                if matched_lines:
                    logger.warning(f"identified an out of memory error in {job.payload}")
                    for line in matched_lines:
                        logger.info(line)
                    out_of_memory = True
        else:
            logger.warning(f'file does not exist: {path} (cannot look for out-of-memory error in it)')

    return out_of_memory


def is_user_code_missing(job: JobData) -> bool:
    """
    Check if the user code (tarball) is missing on the server.

    :param job: job object (JobData)
    :return: True means the user code was found (bool).
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    error_messages = ["ERROR: unable to fetch source tarball from web"]

    return scan_file(stdout,
                     error_messages,
                     warning_message=f"identified an '{error_messages[0]}' message in {os.path.basename(stdout)}")


def is_out_of_space(job: JobData):
    """
    Check if the disk ran out of space.

    :param job: job object (JobData)
    :return: True means the error was found (bool).
    """
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
    error_messages = ["No space left on device"]

    return scan_file(stderr,
                     error_messages,
                     warning_message=f"identified a '{error_messages[0]}' message in {os.path.basename(stderr)}")


def is_installation_error(job: JobData) -> bool:
    """
    Check if the payload failed to run due to faulty/missing installation.

    :param job: job object (JobData)
    :return: BTrue means the error was found bool).
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(stdout)
    res_tmp = _tail[:1024]

    return res_tmp[0:3] == "sh:" and 'setup.sh' in res_tmp and 'No such file or directory' in res_tmp


def is_atlassetup_error(job: JobData) -> bool:
    """
    Check if AtlasSetup failed with a fatal error.

    :param job: job object (JobData)
    :return: True means the error was found (bool).
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(stdout)
    res_tmp = _tail[:2048]

    return "AtlasSetup(FATAL): Fatal exception" in res_tmp


def is_nfssqlite_locking_problem(job: JobData) -> bool:
    """
    Check if there were any NFS SQLite locking problems.

    :param job: job object (JobData)
    :return: True means the error was found (bool).
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    error_messages = ["prepare 5 database is locked", "Error SQLiteStatement"]

    return scan_file(stdout,
                     error_messages,
                     warning_message=f"identified an NFS/Sqlite locking problem in {os.path.basename(stdout)}")


def extract_special_information(job: JobData):
    """
    Extract special information from different sources, such as number of events and database fields.

    :param job: job object (JobData).
    """
    # try to find the number(s) of processed events (will be set in the relevant job fields)
    find_number_of_events(job)

    # get the DB info from the jobReport
    try:
        find_db_info(job)
    except Exception as exc:
        logger.warning(f'detected problem with parsing job report (in find_db_info()): {exc}')


def find_number_of_events(job: JobData):
    """
    Find the number of events.

    :param job: job object (JobData).
    """
    if job.nevents:
        logger.info(f'number of events already known: {job.nevents}')
        return

    logger.info('looking for number of processed events (source #1: jobReport.json)')
    find_number_of_events_in_jobreport(job)
    if job.nevents > 0:
        logger.info(f'found {job.nevents} processed events')
        return

    logger.info('looking for number of processed events (source #2: metadata.xml)')
    find_number_of_events_in_xml(job)
    if job.nevents > 0:
        logger.info(f'found {job.nevents} processed events')
        return

    logger.info('looking for number of processed events (source #3: athena summary file(s)')
    nev1, nev2 = process_athena_summary(job)
    if nev1 > 0:
        job.nevents = nev1
        logger.info(f'found {job.nevents} processed (read) events')
    if nev2 > 0:
        job.neventsw = nev2
        logger.info(f'found {nev2} processed (written) events')


def find_number_of_events_in_jobreport(job: JobData):
    """
    Look for the number of events in the jobReport.json file.

    :param job: job object (JobData).
    """
    try:
        work_attributes = parse_jobreport_data(job.metadata)
    except Exception as exc:
        logger.warning(f'exception caught while parsing job report: {exc}')
        return

    if 'nEvents' in work_attributes:
        try:
            n_events = work_attributes.get('nEvents')
            if n_events:
                job.nevents = int(n_events)
        except ValueError as exc:
            logger.warning(f'failed to convert number of events to int: {exc}')


def find_number_of_events_in_xml(job: JobData):
    """
    Look for the number of events in the metadata.xml file.

    :param job: job object (JobData)
    :raises: BadXML exception if metadata cannot be parsed.
    """
    try:
        metadata = get_metadata_from_xml(job.workdir)
    except Exception as exc:
        msg = f"exception caught while interpreting XML: {exc}"
        raise BadXML(msg) from exc

    if metadata:
        nevents = get_total_number_of_events(metadata)
        if nevents > 0:
            job.nevents = nevents


def process_athena_summary(job: JobData) -> tuple[int, int]:
    """
    Look for the number of events in the Athena summary file.

    :param job: job object (JobData)
    :return: number of read events (int), number of written events (int) (tuple).
    """
    nev1 = 0
    nev2 = 0
    file_pattern_list = ['AthSummary*', 'AthenaSummary*']

    file_list = []
    # loop over all patterns in the list to find all possible summary files
    for file_pattern in file_pattern_list:
        # get all the summary files for the current file pattern
        files = glob(os.path.join(job.workdir, file_pattern))
        # append all found files to the file list
        for summary_file in files:
            file_list.append(summary_file)

    if file_list in ([], ['']):
        logger.info("did not find any athena summary files")
    else:
        # find the most recent and the oldest files
        recent_summary_file, recent_time, oldest_summary_file, oldest_time = \
            find_most_recent_and_oldest_summary_files(file_list)
        if oldest_summary_file == recent_summary_file:
            logger.info(f"summary file {os.path.basename(oldest_summary_file)} will be processed for errors and number of events")
        else:
            logger.info(f"most recent summary file {os.path.basename(recent_summary_file)} "
                        f"(updated at {recent_time}) will be processed for errors [to be implemented]")
            logger.info(f"oldest summary file {os.path.basename(oldest_summary_file)} "
                        f"(updated at {oldest_time}) will be processed for number of events")

        # Get the number of events from the oldest summary file
        nev1, nev2 = get_number_of_events_from_summary_file(oldest_summary_file)
        logger.info(f"number of events: {nev1} (read)")
        logger.info(f"number of events: {nev2} (written)")

    return nev1, nev2


def find_most_recent_and_oldest_summary_files(file_list: list) -> tuple[str, int, str, int]:
    """
    Find the most recent and the oldest athena summary files.

    :param file_list: list of athena summary files (list)
    :return: most recent summary file (str), recent time (int), oldest summary file (str), oldest time (int) (tuple).
    """
    oldest_summary_file = ""
    recent_summary_file = ""
    oldest_time = 9999999999
    recent_time = 0
    if len(file_list) > 1:
        for summary_file in file_list:
            # get the modification time
            try:
                st_mtime = os.path.getmtime(summary_file)
            except OSError as exc:
                logger.warning(f"could not read modification time of file {summary_file}: {exc}")
            else:
                if st_mtime > recent_time:
                    recent_time = st_mtime
                    recent_summary_file = summary_file
                if st_mtime < oldest_time:
                    oldest_time = st_mtime
                    oldest_summary_file = summary_file
    else:
        oldest_summary_file = file_list[0]
        recent_summary_file = oldest_summary_file
        try:
            oldest_time = os.path.getmtime(oldest_summary_file)
        except OSError as exc:
            logger.warning(f"could not read modification time of file {oldest_summary_file}: {exc}")
        else:
            recent_time = oldest_time

    return recent_summary_file, recent_time, oldest_summary_file, oldest_time


def get_number_of_events_from_summary_file(oldest_summary_file: str) -> tuple[int, int]:
    """
    Get the number of events from the oldest summary file.

    :param oldest_summary_file: athena summary file name (str)
    :return: number of read events (int), number of written events (int) (tuple).
    """
    nev1 = 0
    nev2 = 0

    _file = open_file(oldest_summary_file, 'r')
    if _file:
        lines = _file.readlines()
        _file.close()

        if lines:
            for line in lines:
                if "Events Read:" in line:
                    try:
                        nev1 = int(re.match(r'Events Read\: *(\d+)', line).group(1))
                    except ValueError as exc:
                        logger.warning(f'failed to convert number of read events to int: {exc}')
                if "Events Written:" in line:
                    try:
                        nev2 = int(re.match(r'Events Written\: *(\d+)', line).group(1))
                    except ValueError as exc:
                        logger.warning(f'failed to convert number of written events to int: {exc}')
                if nev1 > 0 and nev2 > 0:
                    break
        else:
            logger.warning('failed to get number of events from empty summary file')

    # Get the errors from the most recent summary file
    # ...

    return nev1, nev2


def find_db_info(job: JobData):
    """
    Find the DB info in the jobReport.

    :param job: job object (JobData).
    """
    work_attributes = parse_jobreport_data(job.metadata)

    if '__db_time' in work_attributes:
        try:
            job.dbtime = int(work_attributes.get('__db_time'))
        except ValueError as exc:
            logger.warning(f'failed to convert dbtime to int: {exc}')
        logger.info(f'dbtime (total): {job.dbtime}')

    if '__db_data' in work_attributes:
        try:
            job.dbdata = work_attributes.get('__db_data')
        except ValueError as exc:
            logger.warning(f'failed to convert dbdata to int: {exc}')
        logger.info(f'dbdata (total): {job.dbdata}')


def set_error_nousertarball(job: JobData):
    """
    Set error code for NOUSERTARBALL.

    :param job: job object (JobData).
    """
    # get the tail of the stdout since it will contain the URL of the user log
    filename = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(filename)
    _tail += 'http://someurl.se/path'
    if _tail:
        # try to extract the tarball url from the tail
        tarball_url = extract_tarball_url(_tail)

        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOUSERTARBALL)
        job.piloterrorcode = errors.NOUSERTARBALL
        job.piloterrordiag = f"User tarball {tarball_url} cannot be downloaded from PanDA server"


def extract_tarball_url(payload_tail: str) -> str:
    """
    Extract the tarball URL for missing user code if possible from stdout tail.

    :param payload_tail: tail of payload stdout (str)
    :return: url (str).
    """
    tarball_url = "(source unknown)"

    if "https://" in payload_tail or "http://" in payload_tail:
        pattern = r"(https?\:\/\/.+)"
        found = re.findall(pattern, payload_tail)
        if found:
            tarball_url = found[0]

    return tarball_url


def process_metadata_from_xml(job: JobData):
    """
    Extract necessary metadata from XML when job report is not available.

    :param job: job object (JobData).
    """
    # get the metadata from the xml file instead, which must exist for most production transforms
    path = os.path.join(job.workdir, config.Payload.metadata)
    if os.path.exists(path):
        job.metadata = read_file(path)
    elif not job.is_analysis() and job.transformation != 'Archive_tf.py':
        diagnostics = f'metadata does not exist: {path}'
        logger.warning(diagnostics)
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.NOPAYLOADMETADATA)
        job.piloterrorcode = errors.NOPAYLOADMETADATA
        job.piloterrordiag = diagnostics

    # add missing guids
    for dat in job.outdata:
        if not dat.guid:
            # try to read it from the metadata before the last resort of generating it
            metadata = None
            try:
                metadata = get_metadata_from_xml(job.workdir)
            except Exception as exc:
                msg = f"Exception caught while interpreting XML: {exc} (ignoring it, but guids must now be generated)"
                logger.warning(msg)
            if metadata:
                dat.guid = get_guid_from_xml(metadata, dat.lfn)
                logger.info(f'read guid for lfn={dat.lfn} from xml: {dat.guid}')
            else:
                dat.guid = get_guid()
                logger.info(f'generated guid for lfn={dat.lfn}: {dat.guid}')


def process_job_report(job: JobData):
    """
    Process the job report produced by the payload/transform if it exists.

    Payload error codes and diagnostics, as well as payload metadata (for output files) and stageout type will be
    extracted. The stageout type is either "all" (i.e. stage-out both output and log files) or "log" (i.e. only log file
    will be staged out).
    Note: some fields might be experiment specific. A call to a user function is therefore also done.

    :param job: job dictionary will be updated by the function and several fields set (JobData).
    """
    # get the job report
    path = os.path.join(job.workdir, config.Payload.jobreport)
    if not os.path.exists(path):
        logger.warning(f'job report does not exist: {path}')

        # get the metadata from the xml file instead, which must exist for most production transforms
        process_metadata_from_xml(job)
    else:
        _metadata = {}  # used to overwrite original metadata file in case of changes
        with open(path, encoding="utf-8") as data_file:
            # compulsory field; the payload must produce a job report (see config file for file name), attach it to the
            # job object
            job.metadata = json.load(data_file)

            # truncate warnings if necessary (note: _metadata will remain unset if there are no changes)
            _metadata = truncate_metadata(job.metadata)

            # update job data if necessary
            update_job_data(job)

            # compulsory fields
            try:
                job.exitcode = job.metadata['exitCode']
            except KeyError as exc:
                logger.warning(f'could not find compulsory payload exitCode in job report: {exc} (will be set to 0)')
                job.exitcode = 0
            else:
                logger.info(f'extracted exit code from job report: {job.exitcode}')
            try:
                job.exitmsg = job.metadata['exitMsg']
            except KeyError as exc:
                logger.warning(f'could not find compulsory payload exitMsg in job report: {exc} '
                               f'(will be set to empty string)')
                job.exitmsg = ""
            else:
                # assign special payload error code
                if "got a SIGSEGV signal" in job.exitmsg:
                    diagnostics = f'Invalid memory reference or a segmentation fault in payload: ' \
                                  f'{job.exitmsg} (job report)'
                    logger.warning(diagnostics)
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADSIGSEGV, msg=diagnostics)
                    job.piloterrorcode = errors.PAYLOADSIGSEGV
                    job.piloterrordiag = diagnostics
                else:
                    # extract Frontier errors
                    errmsg = get_frontier_details(job.metadata)
                    if errmsg:
                        msg = f'Frontier error: {errmsg}'
                        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.FRONTIER, msg=msg)
                        job.piloterrorcode = errors.FRONTIER
                        job.piloterrordiag = msg

                    logger.info(f'extracted exit message from job report: {job.exitmsg}')
                    if job.exitmsg != 'OK':
                        job.exeerrordiag = job.exitmsg
                        job.exeerrorcode = job.exitcode

            if job.exitcode != 0:
                # get list with identified errors in job report
                job_report_errors = get_job_report_errors(job.metadata)

                # is it a bad_alloc failure?
                bad_alloc, diagnostics = is_bad_alloc(job_report_errors)
                if bad_alloc:
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.BADALLOC)
                    job.piloterrorcode = errors.BADALLOC
                    job.piloterrordiag = diagnostics

        if _metadata:
            # overwrite job.metadata since it was updated and overwrite the json file
            job.metadata = _metadata
            overwrite_metadata(_metadata, path)


def truncate_metadata(job_report_dictionary: dict) -> dict:
    """
    Truncate the metadata if necessary.

    This function will truncate the job.metadata if some fields are too large. This can at least happen with the 'WARNINGS'
    field.

    :param job_report_dictionary: original job.metadata (dict)
    :return: updated metadata, empty if no updates (dict).
    """
    _metadata = {}

    limit = 25
    if 'executor' in job_report_dictionary:
        try:
            warnings = job_report_dictionary['executor'][0]['logfileReport']['details']['WARNING']
        except KeyError as exc:
            logger.debug(f"jobReport has no such key: {exc} (ignore)")
        except (TypeError, IndexError) as exc:
            logger.warning(f"caught exception (aborting jobReport scan): {exc}")
        else:
            if isinstance(warnings, list) and len(warnings) > limit:
                job_report_dictionary['executor'][0]['logfileReport']['details']['WARNING'] = warnings[0:limit]
                _metadata = job_report_dictionary
                logger.warning(f'truncated jobReport WARNING field to length: {limit}')
    else:
        logger.warning("jobReport does not have the executor key (aborting)")

    return _metadata


def overwrite_metadata(metadata: dict, path: str):
    """
    Overwrite the original metadata with updated info.

    Also make a backup of the original file.

    :param metadata: updated metadata (dict)
    :param path: path to the metadata file (str).
    """
    # make a backup of the original metadata file
    try:
        copy(path, path + '.original')
    except (IOError, FileHandlingFailure, NoSuchFile) as exc:
        logger.warning(f'failed to make a backup of {path} (ignore): {exc}')
    else:
        logger.info(f'backed up original metadata file: {path}')

    # store the updated metadata
    status = write_json(path, metadata)
    if status:
        logger.info(f'overwrote {path} with updated metadata')
    else:
        logger.warning(f'failed to overwrite {path} with updated metadata (ignore)')


def get_frontier_details(job_report_dictionary: dict) -> str:  # noqa: C901
    """
    Extract special Frontier related errors from the job report.

    :param job_report_dictionary: job report (dict)
    :return: extracted error message (str).
    """
    try:
        error_details = job_report_dictionary['executor'][0]['logfileReport']['details']
    except KeyError as exc:
        logger.warning(f'key error: {exc} (ignore detailed Frontier analysis)')
        return ""

    patterns = {'abnormalLines': r'Cannot\sfind\sa\svalid\sfrontier\sconnection(.*)',
                'lastNormalLine': r'Using\sfrontier\sconnection\sfrontier(.*)'}

    def extract_message_from_entry(entry, pattern_name, pattern):
        if 'moreDetails' in entry:
            dic = entry['moreDetails'].get(pattern_name, None)
            if dic:
                for item in dic:
                    if 'message' in item:
                        message = dic[item]
                        if re.findall(pattern, message):
                            return message
        return None

    def extract_message_from_entries(entries, pattern_name, pattern):
        for entry in entries:
            message = extract_message_from_entry(entry, pattern_name, pattern)
            if message:
                return message
        return None

    def find_error_message(patterns, error_details):
        for pattern_name, pattern in patterns.items():
            for _, entries in error_details.items():  # _=level='FATAL','ERROR'
                message = extract_message_from_entries(entries, pattern_name, pattern)
                if message:
                    return message
        return ""

    errmsg = find_error_message(patterns, error_details)
    try:
        msg = re.split(r'INFO\ |WARNING\ ', errmsg)[1]
    except (IndexError, TypeError):
        msg = errmsg

    return msg


def get_job_report_errors(job_report_dictionary: dict) -> list[str]:
    """
    Extract the error list from the jobReport.json dictionary.

    The returned list is scanned for special errors.

    :param job_report_dictionary: job report (dict)
    :return: job_report_errors (list).
    """
    job_report_errors = []
    if 'reportVersion' in job_report_dictionary:
        logger.info(f"scanning jobReport (v {job_report_dictionary.get('reportVersion')}) for error info")
    else:
        logger.warning("jobReport does not have the reportVersion key")

    if 'executor' in job_report_dictionary:
        try:
            error_details = job_report_dictionary['executor'][0]['logfileReport']['details']['ERROR']
        except (KeyError, TypeError, IndexError) as exc:
            logger.warning(f"WARNING: aborting jobReport scan: {exc}")
        else:
            if isinstance(error_details, list):
                for msg in error_details:
                    job_report_errors.append(msg['message'])
            else:
                logger.warning(f"did not get a list object: {type(error_details)}")
    else:
        logger.warning("jobReport does not have the executor key (aborting)")

    return job_report_errors


def is_bad_alloc(job_report_errors: list[str]) -> tuple[bool, str]:
    """
    Check for bad_alloc errors.

    :param job_report_errors: errors extracted from the job report (list)
    :return: bad_alloc (bool), diagnostics (str) (tuple).
    """
    bad_alloc = False
    diagnostics = ""
    for err in job_report_errors:
        if "bad_alloc" in err:
            logger.warning(f"encountered a bad_alloc error: {err}")
            bad_alloc = True
            diagnostics = err
            break

    return bad_alloc, diagnostics


def get_log_extracts(job: JobData, state: str) -> str:
    """
    Extract special warnings and other info from special logs.

    This function also discovers if the payload had any outbound connections.

    :param job: job object (JobData)
    :param state: job state (str)
    :return: log extracts (str).
    """
    logger.info("building log extracts (sent to the server as \'pilotLog\')")

    # did the job have any outbound connections?
    # look for the pandatracerlog.txt file, produced if the user payload attempted any outgoing connections
    extracts = get_panda_tracer_log(job)

    # for failed/holding jobs, add extracts from the pilot log file, but always add it to the pilot log itself
    _extracts = get_pilot_log_extracts(job)
    if _extracts != "":
        logger.warning(f'detected the following tail of warning/fatal messages in the pilot log:\n{_extracts}')
        if state in {'failed', 'holding'}:
            extracts += _extracts

    # add extracts from payload logs
    # (see buildLogExtracts in Pilot 1)

    return extracts


def get_panda_tracer_log(job: JobData) -> str:
    """
    Return the contents of the PanDA tracer log if it exists.

    This file will contain information about outbound connections.

    :param job: job object (JobData)
    :return: log extracts from pandatracerlog.txt (str).
    """
    extracts = ""

    tracerlog = os.path.join(job.workdir, "pandatracerlog.txt")
    if os.path.exists(tracerlog):
        # only add if file is not empty
        if os.path.getsize(tracerlog) > 0:
            message = f"PandaID={job.jobid} had outbound connections: "
            extracts += message
            message = read_file(tracerlog)
            extracts += message
            logger.warning(message)
        else:
            logger.info(f"PanDA tracer log ({tracerlog}) has zero size (no outbound connections detected)")
    else:
        logger.debug(f"PanDA tracer log does not exist: {tracerlog} (ignoring)")

    return extracts


def get_pilot_log_extracts(job: JobData) -> str:
    """
    Get the extracts from the pilot log (warning/fatal messages, as well as tail of the log itself).

    :param job: job object (JobData)
    :return: tail of pilot log (str).
    """
    extracts = ""

    path = os.path.join(job.workdir, config.Pilot.pilotlog)
    if os.path.exists(path):
        # get the last 20 lines of the pilot log in case it contains relevant error information
        _tail = tail(path, nlines=20)
        if _tail != "":
            if extracts != "":
                extracts += "\n"
            extracts += f"- Log from {config.Pilot.pilotlog} -\n"
            extracts += _tail
    else:
        logger.warning(f'pilot log file does not exist: {path}')

    return extracts
