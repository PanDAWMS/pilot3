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

"""Payload stdout/stderr interpretation for the ATLAS experiment plugin."""

from __future__ import annotations
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

# XRootD and ROOT file-open error patterns that can appear in payload stdout when
# direct-access (remoteIO) file reads fail after the pilot's pre-flight check passed.
_DIRECT_ACCESS_ERROR_PATTERNS: list[str] = [
    r'TNetXNGFile::Open\s+ERROR',
    r'Unable to open ROOT file',
    r'\[ERROR\] Operation expired',
    r'\[ERROR\] No servers are available',
    r'\[ERROR\] Server responded with an error',
    r'XrdCl::\S+\s+Error',
    r'ERROR\s+.*root://',
    r'FileReadError.*root://',
]

# Cling JIT "Cannot allocate memory" appears in payload stdout/stderr when the worker
# node exhausts its 64k VMA limit.  Increasing the memory request does not help; the
# correct retry action is to reduce the number of input files per job (action 5).
_CLING_JIT_ERROR_PATTERNS: list[str] = [
    r'cling JIT session error: Cannot allocate memory',
]

# Matches the first file path token in an error line.
# Covers: scheme-based URLs (root://, file://, …), absolute POSIX paths with at least
# one interior slash, and long simple absolute paths.  A minimum length is required to
# avoid matching bare hostnames or single-component paths that are not file names.
_FILE_PATH_RE = re.compile(
    r'(?<!\S)'                               # preceded by whitespace or start of string
    r'('
    r'(?:[a-z][a-z0-9+\-.]*://)\S{10,}'     # scheme-based URL (e.g. root://host/path)
    r'|'
    r'/\S*/\S{3,}'                           # absolute path with at least one inner slash
    r'|'
    r'/\S{10,}'                              # simple absolute path >=10 chars
    r')'
)


def interpret(job: JobData) -> int:
    """Interpret the payload, looking for specific errors in the stdout.

    Args:
        job: Job object whose stdout and metadata will be examined.

    Returns:
        Payload exit code, or -1 if diagnosis was aborted because an error
        code had already been assigned.
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
        elif job.piloterrorcodes[0] == errors.PAYLOADEXECUTIONFAILURE and job.has_remoteio():
            # PAYLOADEXECUTIONFAILURE (1305) is a generic placeholder set before the
            # more specific direct-access error scan runs.  For remoteIO jobs allow
            # interpret_payload_exit_info() to proceed so it can replace 1305 with the
            # more informative STAGEINFAILED (1099) when XRootD patterns are detected.
            logger.info('allowing direct-access error scan to proceed despite PAYLOADEXECUTIONFAILURE already set (remoteIO job)')
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
    """Interpret the exit information from the payload and set the appropriate error code.

    Checks for out-of-memory, installation, AtlasSetup, disk-space, NFS/SQLite,
    missing user code, and direct-access errors in that order. The first matching
    condition sets the pilot error code with priority and returns. If none match and
    the payload exited non-zero without a transform error, ``UNKNOWNPAYLOADFAILURE``
    is set as a catch-all.

    Args:
        job: Job object whose error codes and diagnostics will be updated in place.
    """
    # try to identify out of memory errors in the stderr
    if is_out_of_memory(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADOUTOFMEMORY, priority=True)
        return

    # check for cling JIT "Cannot allocate memory" — distinct from a true OOM: caused by
    # the worker node hitting its 64k VMA limit; retry with fewer input files (action 5)
    if is_cling_jit_error(job):
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.ALLOCATIONERROR, priority=True)
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

    # did a direct-access (remoteIO) file open fail inside the payload?
    if job.has_remoteio():
        _diag = is_direct_access_error(job)
        if _diag:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINFAILED, priority=True, msg=_diag)
            return

    # set a general Pilot error code if the payload error could not be identified
    if job.transexitcode == 0 and job.exitcode != 0:
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.UNKNOWNPAYLOADFAILURE, priority=True)


def is_out_of_memory(job: JobData) -> bool:
    """Check whether the payload ran out of memory.

    Searches ``payload.stderr`` for Athena fatal OOM messages and
    ``payload.stdout`` for C++ ``bad_alloc`` signatures.

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        True if an out-of-memory error pattern was found, False otherwise.
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
    """Check whether the user code tarball could not be fetched from the server.

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        True if the tarball-fetch error message was found in payload stdout,
        False otherwise.
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    error_messages = ["ERROR: unable to fetch source tarball from web"]

    return scan_file(stdout,
                     error_messages,
                     warning_message=f"identified an '{error_messages[0]}' message in {os.path.basename(stdout)}")


def is_cling_jit_error(job: JobData) -> bool:
    """Check whether the payload hit the 64k VMA limit via a cling JIT allocation failure.

    Scans both ``payload.stdout`` and ``payload.stderr`` for the cling JIT
    ``"Cannot allocate memory"`` message.  This error is caused by the worker
    node exhausting its kernel VMA limit (typically 65536 mappings) and is
    distinct from a genuine out-of-memory condition: increasing the memory
    allocation will not help.  The appropriate retry action is to reduce the
    number of input files per job (retryModule action 5).

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        True if the cling JIT allocation-failure pattern was found in either
        payload stdout or stderr, False otherwise.
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)

    for path in (stdout, stderr):
        if not os.path.exists(path):
            logger.warning(f'file does not exist: {path} (cannot scan for cling JIT allocation error)')
            continue
        if scan_file(path,
                     _CLING_JIT_ERROR_PATTERNS,
                     warning_message=f"identified a cling JIT allocation failure in {os.path.basename(path)}"):
            return True

    return False


def is_direct_access_error(job: JobData) -> str:
    """Check whether a direct-access (remoteIO) file-open error occurred inside the payload.

    Scans the full payload stdout for XRootD and ROOT file-open error patterns that are
    only visible inside the payload log, not at the pilot stage-in layer. The function
    should only be called for jobs that used direct access (``job.has_remoteio()``).

    Among all matched lines, the first one that contains a recognisable file path (a
    scheme-based URL such as ``root://…`` or an absolute POSIX path) is preferred as the
    diagnostics string, because it identifies the specific file that could not be read.
    If no line contains a path, the first matched line is used as a fallback. Up to five
    matched lines are also logged at WARNING level.

    The returned string is intended to be passed directly to
    ``errors.add_error_code(..., msg=<return value>)`` so it reaches the server as the
    human-readable error diagnostics rather than the generic STAGEINFAILED message.

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        The best diagnostics line (stripped) if a direct-access error pattern was found,
        otherwise an empty string.
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    if not os.path.exists(stdout):
        logger.warning(f'payload stdout does not exist, cannot scan for direct-access errors: {stdout}')
        return ""

    matched_lines = grep(_DIRECT_ACCESS_ERROR_PATTERNS, stdout)
    if not matched_lines:
        return ""

    logger.warning('detected direct-access (remoteIO) error pattern(s) in payload stdout:')
    for line in matched_lines[:5]:  # cap output to avoid flooding the pilot log
        logger.warning(f'  {line.rstrip()}')

    # Prefer the first line that contains a recognisable file path so the diagnostics
    # string identifies the specific file that could not be read.
    for line in matched_lines:
        m = _FILE_PATH_RE.search(line)
        if m:
            return line.strip()

    # Fallback: no line contained a path — return the first matched line as-is.
    return matched_lines[0].strip()


def is_out_of_space(job: JobData) -> bool:
    """Check whether the payload ran out of local disk space.

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        True if a "No space left on device" message was found in payload stderr,
        False otherwise.
    """
    stderr = os.path.join(job.workdir, config.Payload.payloadstderr)
    error_messages = ["No space left on device"]

    return scan_file(stderr,
                     error_messages,
                     warning_message=f"identified a '{error_messages[0]}' message in {os.path.basename(stderr)}")


def is_installation_error(job: JobData) -> bool:
    """Check whether the payload failed due to a faulty or missing installation.

    Inspects the tail of payload stdout for a ``sh: … setup.sh: No such file
    or directory`` signature.

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        True if an installation error pattern was found, False otherwise.
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(stdout)
    res_tmp = _tail[:1024]

    return res_tmp[0:3] == "sh:" and 'setup.sh' in res_tmp and 'No such file or directory' in res_tmp


def is_atlassetup_error(job: JobData) -> bool:
    """Check whether AtlasSetup failed with a fatal exception.

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        True if an ``AtlasSetup(FATAL): Fatal exception`` message was found in
        the tail of payload stdout, False otherwise.
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    _tail = tail(stdout)
    res_tmp = _tail[:2048]

    return "AtlasSetup(FATAL): Fatal exception" in res_tmp


def is_nfssqlite_locking_problem(job: JobData) -> bool:
    """Check whether an NFS SQLite locking problem occurred in the payload.

    Args:
        job: Job object containing workdir and payload file path configuration.

    Returns:
        True if an NFS/SQLite locking error pattern was found in payload stdout,
        False otherwise.
    """
    stdout = os.path.join(job.workdir, config.Payload.payloadstdout)
    error_messages = ["prepare 5 database is locked", "Error SQLiteStatement"]

    return scan_file(stdout,
                     error_messages,
                     warning_message=f"identified an NFS/Sqlite locking problem in {os.path.basename(stdout)}")


def extract_special_information(job: JobData):
    """Extract special information from the job report and related sources.

    Populates event-count fields (``job.nevents``, ``job.neventsw``) and
    database-usage fields (``job.dbtime``, ``job.dbdata``) on the job object.

    Args:
        job: Job object whose fields will be updated in place.
    """
    # try to find the number(s) of processed events (will be set in the relevant job fields)
    find_number_of_events(job)

    # get the DB info from the jobReport
    try:
        find_db_info(job)
    except Exception as exc:
        logger.warning(f'detected problem with parsing job report (in find_db_info()): {exc}')


def find_number_of_events(job: JobData):
    """Find the number of processed events and store it on the job object.

    Tries three sources in order: ``jobReport.json``, ``metadata.xml``, and
    Athena summary files. Stops as soon as a non-zero value is found. Both the
    read count (``job.nevents``) and the write count (``job.neventsw``) may be
    set by the Athena summary path.

    Args:
        job: Job object whose ``nevents`` and ``neventsw`` fields will be updated.
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
    """Look for the number of processed events in ``jobReport.json``.

    Sets ``job.nevents`` if the ``nEvents`` key is present and non-zero.

    Args:
        job: Job object whose ``nevents`` field may be updated.
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
    """Look for the number of processed events in ``metadata.xml``.

    Sets ``job.nevents`` if a non-zero count is found.

    Args:
        job: Job object whose ``nevents`` field may be updated.

    Raises:
        BadXML: If the XML metadata file exists but cannot be parsed.
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
    """Look for the number of processed events in Athena summary files.

    Searches the job work directory for ``AthSummary*`` and ``AthenaSummary*``
    files. When multiple files are found, the oldest is used for event counts
    and the most recent would be used for error extraction (not yet
    implemented).

    Args:
        job: Job object providing the work directory to search.

    Returns:
        A tuple of ``(n_read, n_written)`` event counts. Either value is zero
        if it could not be determined.
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
    """Find the most recently and least recently modified Athena summary files.

    Args:
        file_list: Paths of candidate summary files to examine.

    Returns:
        A tuple of ``(recent_file, recent_mtime, oldest_file, oldest_mtime)``
        where modification times are Unix timestamps. When only one file is
        provided both slots refer to that file.
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
    """Extract the read and written event counts from an Athena summary file.

    Args:
        oldest_summary_file: Path to the Athena summary file to parse.

    Returns:
        A tuple of ``(n_read, n_written)`` event counts. Either value is zero
        if the corresponding line was absent or could not be parsed.
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
    """Find database usage information in the job report and store it on the job object.

    Reads ``__db_time`` and ``__db_data`` from the parsed job report and sets
    ``job.dbtime`` and ``job.dbdata`` respectively when present.

    Args:
        job: Job object whose ``dbtime`` and ``dbdata`` fields may be updated.
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
    """Set the NOUSERTARBALL error code and extract the tarball URL from payload stdout.

    Reads the tail of payload stdout to find the URL of the tarball that could
    not be downloaded, then stores the NOUSERTARBALL error code and a
    descriptive diagnostic message on the job object.

    Args:
        job: Job object whose error code and diagnostic fields will be updated.
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
    """Extract the tarball URL from the tail of payload stdout.

    Args:
        payload_tail: Tail of the payload stdout as a plain string.

    Returns:
        The first ``http://`` or ``https://`` URL found in the tail, or the
        string ``"(source unknown)"`` if no URL could be extracted.
    """
    tarball_url = "(source unknown)"

    if "https://" in payload_tail or "http://" in payload_tail:
        pattern = r"(https?\:\/\/.+)"
        found = re.findall(pattern, payload_tail)
        if found:
            tarball_url = found[0]

    return tarball_url


def process_metadata_from_xml(job: JobData):
    """Extract payload metadata from ``metadata.xml`` when no job report is available.

    Reads the XML file into ``job.metadata`` and sets NOPAYLOADMETADATA on the
    job if the file is absent and the job is a non-analysis production transform.
    Also fills any missing GUIDs on output file specs, first by reading them
    from the XML and falling back to generating them.

    Args:
        job: Job object whose ``metadata`` field and output file GUIDs will be
            updated in place.
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
    """Process the job report produced by the payload transform, if it exists.

    Extracts payload exit codes and diagnostics, output file metadata, and
    stageout type (``"all"`` or ``"log"``). When the job report is absent,
    falls back to ``process_metadata_from_xml()``. Truncates oversized WARNING
    fields in the report and overwrites the file if any changes were made.
    Handles SIGSEGV, Frontier, and bad_alloc error signatures found inside the
    report. Some fields are experiment-specific and are handled via
    ``update_job_data()``.

    Args:
        job: Job object whose metadata, exit code, error code, and stageout
            fields will be updated in place.
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
    """Truncate oversized fields in the job report metadata.

    Currently caps the ``executor[0].logfileReport.details.WARNING`` list at
    25 entries to prevent excessively large metadata payloads.

    Args:
        job_report_dictionary: The raw ``job.metadata`` dictionary as loaded
            from ``jobReport.json``.

    Returns:
        The updated metadata dictionary if any field was truncated, otherwise
        an empty dict signalling that no changes were made.
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
    """Overwrite the original metadata file with updated content.

    A backup of the original file is created at ``<path>.original`` before
    writing. Failures at either step are logged as warnings but do not raise.

    Args:
        metadata: Updated metadata dictionary to serialise as JSON.
        path: Absolute path to the metadata file to overwrite.
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
    """Extract Frontier-related error details from the job report.

    Searches the ``executor[0].logfileReport.details`` section for lines
    matching known Frontier connection failure and configuration patterns,
    then strips the leading log-level prefix (``INFO`` / ``WARNING``) from
    the returned message.

    Args:
        job_report_dictionary: The raw ``job.metadata`` dictionary as loaded
            from ``jobReport.json``.

    Returns:
        The extracted Frontier error message, or an empty string if none was
        found or the expected keys were absent.
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
    """Extract the ERROR-level message list from the job report dictionary.

    Navigates to ``executor[0].logfileReport.details.ERROR`` and returns each
    entry's ``message`` value. The returned list is typically passed on to
    specialised checkers such as ``is_bad_alloc()``.

    Args:
        job_report_dictionary: The raw ``job.metadata`` dictionary as loaded
            from ``jobReport.json``.

    Returns:
        List of error message strings found in the report. Empty if the
        expected keys were absent or the details were not a list.
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
    """Check whether any job report error message indicates a C++ bad_alloc failure.

    Args:
        job_report_errors: List of error message strings extracted from the
            job report by ``get_job_report_errors()``.

    Returns:
        A tuple of ``(found, diagnostics)`` where ``found`` is True if a
        ``bad_alloc`` message was detected and ``diagnostics`` is the
        offending message string, or an empty string when not found.
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
    """Build a log-extract string to be sent to the server as ``pilotLog``.

    Always includes the PanDA tracer log content when present. For failed or
    holding jobs, also appends a tail of the pilot log file.

    Args:
        job: Job object providing the work directory and job ID.
        state: Current job state string (e.g. ``"failed"``, ``"holding"``).

    Returns:
        Concatenated log extracts as a plain string, or an empty string if
        nothing relevant was found.
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
    """Return the contents of the PanDA tracer log, if it exists and is non-empty.

    The tracer log (``pandatracerlog.txt``) is produced when the payload
    attempts outbound network connections. Its presence is reported as a
    warning.

    Args:
        job: Job object providing the work directory and job ID.

    Returns:
        The full contents of the tracer log prefixed with a PandaID header,
        or an empty string if the file does not exist or is empty.
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
    """Return the last 20 lines of the pilot log file.

    Args:
        job: Job object providing the work directory.

    Returns:
        A formatted string containing the pilot log tail, or an empty string
        if the log file does not exist or is empty.
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
