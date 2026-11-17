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
# specific language governing permissions and limitations
# under the License.
#
# Authors:
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-18
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-26
# - Tomas Javurek, tomas.javurek@cern.ch, 2019
# - Tomas Javurek, tomas.javurek@cern.ch, 2019
# - David Cameron, david.cameron@cern.ch, 2019

"""Rucio copy tool."""

from __future__ import annotations
import json
import logging
import os
import traceback
from copy import deepcopy
from time import time
from typing import Any

from pilot.common.exception import (
    PilotException,
    ErrorCodes,
)
# from pilot.common.pilotcache import get_pilot_cache - do not use pilot cache here (complicated container if used)
from pilot.util.timer import (
    timeout,
    TimedThread,
)
from pilot.util.config import config
from pilot.util.filehandling import rename_xrdlog
from pilot.util.tracereport import (
    TraceReport,
    send_traces
)
from .common import (
    resolve_common_transfer_errors,
    verify_catalog_checksum,
    get_timeout
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# can be disabled for Rucio if allowed to use all RSE for input
require_replicas = True    ## indicates if given copytool requires input replicas to be resolved
require_protocols = False  ## indicates if given copytool requires protocols to be resolved first for stage-out
tracing_rucio = False      ## should Rucio send the trace?
FAILED_ATTEMPT_STATES = ('FAILED', 'FAIL_VALIDATE')  # Rucio client states of a failed download attempt
STAGEIN_ATTEMPT_FAILED = 'STAGEIN_ATTEMPT_FAILED'  # clientState of the trace of a failed download attempt
STAGEOUT_ATTEMPT_FAILED = 'STAGEOUT_ATTEMPT_FAILED'  # clientState of the trace of a failed upload attempt


def is_valid_for_copy_in(files: list) -> bool:
    """
    Determine if this copytool is valid for input for the given file list.

    Placeholder.

    Args:
        files: list of FileSpec objects.

    Returns:
        Always True (for now).
    """
    if files:  # to get rid of pylint warning
        pass
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files: list) -> bool:
    """
    Determine if this copytool is valid for output for the given file list.

    Placeholder.

    Args:
        files: list of FileSpec objects.

    Returns:
        Always True (for now).
    """
    if files:  # to get rid of pylint warning
        pass
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


#@timeout(seconds=10800)
def copy_in(files: list, **kwargs: dict) -> list:
    """
    Download given files using rucio copytool.

    Args:
        files: list of FileSpec objects.
        kwargs: kwargs dictionary.

    Returns:
        Updated list of files.

    Raises:
        PilotException: in case of controlled error.
    """
    ignore_errors = kwargs.get('ignore_errors')
    trace_report = kwargs.get('trace_report')
    use_pcache = kwargs.get('use_pcache')
    rucio_host = kwargs.get('rucio_host', '')
    pilot_args = kwargs.get('args')

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    # note, env vars might be unknown inside middleware contrainers, if so get the value already in the trace report
    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', trace_report.get_value('localSite'))
    for fspec in files:
        # check if we should abort
        if pilot_args and pilot_args.graceful_stop and pilot_args.graceful_stop.is_set():
            msg = f'copytool has detected graceful stop - will abort stage-in ({pilot_args.mainworkdir})'
            logger.warning(msg)
            raise PilotException(msg)
        if not pilot_args:
            logger.warning('pilot_args not set, cannot check for graceful stop')

        logger.info(f'rucio copytool, downloading file with scope:{fspec.scope} lfn:{fspec.lfn}')
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
        trace_report.update(url=fspec.turl if fspec.turl else fspec.surl)
        trace_report.start_file()  # every file's trace has its own start time stamps
        fspec.status_code = 0
        dst = fspec.workdir or kwargs.get('workdir') or '.'

        trace_report_out = []
        attempts = []  # failed download attempts inside rucio (rucio retries internally)
        transfer_timeout = get_timeout(fspec.filesize)
        ctimeout = transfer_timeout + 10  # give the API a chance to do the time-out first
        logger.info(f'overall transfer timeout={ctimeout}')

        error_msg = ""
        ec = 0
        try:
            ec, trace_report_out = timeout(ctimeout, timer=TimedThread)(_stage_in_api)(dst, fspec, trace_report,
                                                                                       trace_report_out, transfer_timeout,
                                                                                       use_pcache, rucio_host, attempts=attempts)
        except Exception as error:
            error_msg = str(error)
            logger.warning(f"exception caught during stage-in: {error_msg}")
            error_details = handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=True)
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            if not ignore_errors:
                send_stagein_traces(trace_report, attempts, rucio_failed=True)
                msg = f"{fspec.scope}:{fspec.lfn} from {fspec.ddmendpoint}, {error_details.get('error')}"
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state')) from error
        else:
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            rename_xrdlog(fspec.lfn)
        rucio_failed = bool(error_msg or ec)  # the download failed inside rucio (after its own retries)

        # make sure there was no missed failure (only way to deal with this until rucio API has been fixed)
        # (using the timeout decorator prevents the trace_report_out from being updated - rucio API should return
        # the proper error immediately instead of encoding it into a dictionary)
        state_reason = None if not trace_report_out else trace_report_out[0].get('stateReason')
        if ec and state_reason and not error_msg:
            error_details = handle_rucio_error(state_reason, trace_report, trace_report_out, fspec, stagein=True)

            if not ignore_errors:
                send_stagein_traces(trace_report, attempts, rucio_failed)
                msg = f"{fspec.scope}:{fspec.lfn} from {fspec.ddmendpoint}, {error_details.get('error')}"
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        destination = os.path.join(dst, fspec.lfn)
        if os.path.exists(destination):
            trace_report.update(validateStart=time())
            state, diagnostics = verify_catalog_checksum(fspec, destination)
            if diagnostics != "" and not ignore_errors:
                trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                    timeEnd=time())
                send_stagein_traces(trace_report, attempts, rucio_failed)
                raise PilotException(diagnostics, code=fspec.status_code, state=state)
        else:
            diagnostics = f'file does not exist: {destination} (cannot verify catalog checksum)'
            logger.warning(diagnostics)
            state = 'STAGEIN_ATTEMPT_FAILED'
            fspec.status_code = ErrorCodes.STAGEINFAILED
            trace_report.update(clientState=state, stateReason=diagnostics,
                                timeEnd=time())
            send_stagein_traces(trace_report, attempts, rucio_failed)
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        send_stagein_traces(trace_report, attempts, rucio_failed)

    return files


def get_protocol(trace_report_out: dict) -> str:
    """
    Extract the protocol used for the transfer from the dictionary returned by rucio.

    Args:
        trace_report_out: returned rucio transfer dictionary.

    Returns:
        Protocol string.
    """
    try:
        protocol = trace_report_out[0].get('protocol')
    except Exception as error:
        logger.warning(f'exception caught: {error}')
        protocol = ''

    return protocol


def capture_failed_attempts(download_client: Any, attempts: list | None) -> bool:
    """Record the failed download attempts made inside the Rucio download client.

    The Rucio download client retries a failed download internally, and only the outcome of the
    last attempt reaches the pilot. After each failed attempt, the client calls its private
    ``_send_trace()`` method, which does nothing since Rucio's own tracing is turned off. The
    method is wrapped on this client instance to keep a copy of each failed attempt (see
    :func:`get_attempt_snapshot`); the original method is still called.

    ``_send_trace()`` is present in the Rucio clients checked (1.29 to 41.5). If it is missing,
    nothing is recorded and only the final trace is sent, as before.

    Args:
        download_client: Rucio download client.
        attempts: List to append the failed attempts to (nothing is recorded if None).

    Returns:
        True if the failed attempts will be recorded.
    """
    if attempts is None:
        return False

    original = getattr(download_client, '_send_trace', None)
    if not callable(original):
        logger.info('rucio download client has no _send_trace() method - failed download attempts will not be reported')
        return False

    def _send_trace(trace: dict, *args: Any, **kwargs: Any) -> Any:
        """Keep a copy of a failed attempt, then call the original method."""
        try:
            snapshot = get_attempt_snapshot(trace)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning(f'failed to record download attempt: {exc}')
        else:
            if snapshot:
                attempts.append(snapshot)

        return original(trace, *args, **kwargs)

    download_client._send_trace = _send_trace  # pylint: disable=protected-access

    return True


def get_attempt_snapshot(trace: dict) -> dict | None:
    """Return a copy of the relevant fields of a Rucio trace for a failed download attempt.

    The Rucio trace dictionary is changed in place by later attempts, so the fields are copied.

    Args:
        trace: Rucio trace dictionary.

    Returns:
        The copied fields with the time of the failure, or None if the trace is not for a failed attempt.
    """
    state = trace.get('clientState')
    state = getattr(state, 'value', state)  # an enum in recent Rucio clients
    if state not in FAILED_ATTEMPT_STATES:
        return None

    return {'clientState': state, 'stateReason': trace.get('stateReason'), 'protocol': trace.get('protocol'),
            'remoteSite': trace.get('remoteSite'), 'timeEnd': time()}


def get_upload_attempt_snapshot(trace_report_out: list, error: BaseException) -> dict:
    """Return the relevant fields of a failed upload attempt.

    The Rucio upload client creates one trace per file and call, and does not send it since Rucio's own
    tracing is turned off. Its reason is used if there is one (e.g. the error of the last protocol tried, or
    'File already exists' if the file was left on the storage by the previous attempt), otherwise the error
    raised by the client (e.g. when it failed before creating the trace).

    Args:
        trace_report_out: Rucio traces of the attempt (empty if the client failed before creating one).
        error: Exception raised by the upload client.

    Returns:
        The fields, with the time of the failure.
    """
    trace = trace_report_out[-1] if trace_report_out else {}
    reason = trace.get('stateReason') or f'{type(error).__name__}: {error}'

    return {'stateReason': reason, 'protocol': trace.get('protocol'), 'remoteSite': trace.get('remoteSite'),
            'timeEnd': time()}


def get_attempt_traces(trace_report: Any, attempts: list, rucio_failed: bool,
                       state: str = STAGEIN_ATTEMPT_FAILED) -> list:
    """Return one trace report per failed transfer attempt.

    Each trace is a copy of the file's trace report, with ``clientState`` set to ``state`` (by default
    STAGEIN_ATTEMPT_FAILED) and the reason, protocol and remote site of the attempt. If the transfer failed
    overall, the last failed attempt is the failure that the file's own trace reports, so it gets no extra trace.

    Each attempt has its own start time stamps: the first attempt starts when the file's transfer started,
    and every later attempt when the previous one failed (the client does not report when an attempt
    started, but the attempts follow each other). ``validateStart`` is cleared, since a failed attempt
    is not verified by the pilot.

    Args:
        trace_report: The file's trace report.
        attempts: Failed attempts (see :func:`get_attempt_snapshot` and :func:`get_upload_attempt_snapshot`).
        rucio_failed: True if the last attempt is among the attempts, i.e. the transfer failed inside Rucio.
        state: clientState of the attempt traces.

    Returns:
        List of trace reports.
    """
    snapshots = list(attempts)
    if rucio_failed and snapshots:
        snapshots = snapshots[:-1]

    traces = []
    start = trace_report.get('transferStart')
    for snapshot in snapshots:
        trace = TraceReport(trace_report, ipv=getattr(trace_report, 'ipv', 'IPv6'),
                            workdir=getattr(trace_report, 'workdir', ''))
        if start is not None:  # always set by the copytool, but never use the current time instead
            trace.start_file(start)
        trace.update(clientState=state, stateReason=snapshot.get('stateReason'),
                     timeEnd=snapshot.get('timeEnd'), validateStart=None)
        start = snapshot.get('timeEnd') or start  # the next attempt starts when this one failed
        for key in ('protocol', 'remoteSite'):
            if snapshot.get(key):
                trace[key] = snapshot[key]
        traces.append(trace)

    return traces


def send_stagein_traces(trace_report: Any, attempts: list, rucio_failed: bool) -> None:
    """Send the file's trace, together with the traces of its failed download attempts.

    Without failed attempts, the file's trace is sent on its own, as before. Otherwise all traces are
    sent together (see :func:`~pilot.util.tracereport.send_traces`): in one request, or through the
    spool file or the job's background sender like any other trace.

    Args:
        trace_report: The file's trace report.
        attempts: Failed attempts (see :func:`get_attempt_snapshot`).
        rucio_failed: True if the download failed inside Rucio.
    """
    attempt_traces = get_attempt_traces(trace_report, attempts, rucio_failed)
    if not attempt_traces:
        trace_report.send()
        return

    logger.info(f'sending the trace of {trace_report.get("filename")} with {len(attempt_traces)} trace(s) for failed download attempts')
    send_traces(attempt_traces + [trace_report], job_id=trace_report.get('appid'), workdir=getattr(trace_report, 'workdir', ''))


def send_stageout_traces(trace_report: Any, attempts: list) -> None:
    """Send the file's trace, together with the traces of its failed upload attempts.

    The attempts are the failed upload attempts that were followed by another attempt (see
    :func:`_stage_out_api`), so the last attempt is never among them: its outcome is what the file's own
    trace reports. Without failed attempts, the file's trace is sent on its own, as before.

    Args:
        trace_report: The file's trace report.
        attempts: Failed upload attempts (see :func:`get_upload_attempt_snapshot`).
    """
    attempt_traces = get_attempt_traces(trace_report, attempts, False, state=STAGEOUT_ATTEMPT_FAILED)
    if not attempt_traces:
        trace_report.send()
        return

    logger.info(f'sending the trace of {trace_report.get("filename")} with {len(attempt_traces)} trace(s) for failed upload attempts')
    send_traces(attempt_traces + [trace_report], job_id=trace_report.get('appid'), workdir=getattr(trace_report, 'workdir', ''))


def handle_rucio_error(error_msg: str, trace_report: dict, trace_report_out: list, fspec: Any,
                       stagein: bool = True) -> dict:
    """
    Handle any error from Rucio.

    Args:
        error_msg: error message.
        trace_report: trace report dictionary.
        trace_report_out: trace report from Rucio.
        fspec: FileSpec object.
        stagein: True for stage-in, False for stage-out.

    Returns:
        Error details dictionary of the form {'rcode': rcode, 'state': state, 'error': error_msg}.
    """
    # try to get a better error message from the traces
    error_msg_org = error_msg
    if trace_report_out:
        logger.debug(f'reading stateReason from trace_report_out: {trace_report_out}')
        error_msg = trace_report_out[0].get('stateReason', '')
        if not error_msg or error_msg == 'OK':
            logger.warning('could not extract error message from trace report - reverting to original error message')
            error_msg = error_msg_org
    else:
        logger.debug('no trace_report_out')
    logger.info(f'rucio returned an error: \"{error_msg}\"')

    error_details = resolve_common_transfer_errors(error_msg, is_stagein=stagein)
    fspec.status = 'failed'
    fspec.status_code = error_details.get('rcode')

    msg = 'STAGEIN_ATTEMPT_FAILED' if stagein else 'STAGEOUT_ATTEMPT_FAILED'
    trace_report.update(clientState=error_details.get('state', msg),
                        stateReason=error_details.get('error'), timeEnd=time())

    return error_details


def copy_in_bulk(files: list, **kwargs: dict) -> list:
    """
    Download given files using rucio copytool.

    Args:
        files: list of FileSpec objects.
        kwargs: kwargs dictionary.

    Returns:
        List of successfully transferred files.

    Raises:
        PilotException: in case of controlled error.
    """
    #allow_direct_access = kwargs.get('allow_direct_access')
    ignore_errors = kwargs.get('ignore_errors')
    trace_common_fields = kwargs.get('trace_report')
    rucio_host = kwargs.get('rucio_host', '')

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    dst = kwargs.get('workdir') or '.'

    # THE DOWNLOAD
    trace_report_out = []
    try:
        # transfer_timeout = get_timeout(fspec.filesize, add=10)  # give the API a chance to do the time-out first
        # timeout(transfer_timeout)(_stage_in_api)(dst, fspec, trace_report, trace_report_out)
        _stage_in_bulk(dst, files, trace_report_out, trace_common_fields, rucio_host)
    except Exception as error:
        error_msg = str(error)
        # Fill and sned the traces, if they are not received from Rucio, abortion of the download process
        # If there was Exception from Rucio, but still some traces returned, we continue to VALIDATION section
        if not trace_report_out:
            trace_report = deepcopy(trace_common_fields)
            localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', None)
            diagnostics = f'none of the traces received from rucio. response from rucio: {error_msg}'
            code = 0
            for fspec in files:
                localsite = localsite if localsite else fspec.ddmendpoint
                trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                trace_report.update('STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics, timeEnd=time())
                trace_report.send()
                code = fspec.status_code  # just get one of them for later
            logger.error(diagnostics)
            raise PilotException(diagnostics, code=code, state='STAGEIN_ATTEMPT_FAILED') from error

    # VALIDATION AND TERMINATION
    files_done = []
    for fspec in files:

        # getting the trace for given file
        # if one trace is missing, the whould stagin gets failed
        trace_candidates = _get_trace(fspec, trace_report_out)
        protocol = get_protocol(trace_report_out)  # note this is probably not correct (using [0])
        trace_report.update(protocol=protocol)
        trace_report = None
        diagnostics = 'unknown'
        if len(trace_candidates) == 0:
            diagnostics = 'no trace retrieved for given file: {fspec.lfn}'
            logger.error(diagnostics)
        elif len(trace_candidates) != 1:
            diagnostics = f'too many traces for given file: {fspec.lfn}'
            logger.error(diagnostics)
        else:
            trace_report = trace_candidates[0]

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        destination = os.path.join(dst, fspec.lfn)
        if os.path.exists(destination):
            state, diagnostics = verify_catalog_checksum(fspec, destination)
            if diagnostics != "" and not ignore_errors and trace_report:  # caution, validation against empty string
                trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                    timeEnd=time())
                logger.error(diagnostics)
        elif trace_report:
            diagnostics = f'file does not exist: {destination} (cannot verify catalog checksum)'
            state = 'STAGEIN_ATTEMPT_FAILED'
            fspec.status_code = ErrorCodes.STAGEINFAILED
            trace_report.update(clientState=state, stateReason=diagnostics, timeEnd=time())
            logger.error(diagnostics)
        else:
            fspec.status_code = ErrorCodes.STAGEINFAILED

        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
            files_done.append(fspec)

        # updating the trace and sending it
        if not trace_report:
            logger.error(f'an unknown error occurred when handling the traces for {fspec.lfn}')
            logger.warning('trace not sent')
        trace_report.update(guid=fspec.guid.replace('-', ''))
        trace_report.send()

    if len(files_done) != len(files):
        raise PilotException('not all files downloaded', code=ErrorCodes.STAGEINFAILED, state='STAGEIN_ATTEMPT_FAILED')

    return files_done


def _get_trace(fspec: Any, traces: list) -> list:
    """
    Get the trace candidates corresponding to the given file.

    Traces returned by Rucio are not orderred the same as input files from pilot.
    This method finds the proper trace.

    Args:
        fspec: the file that is wanted.
        traces: list of all traces that are received by Rucio.

    Returns:
        Trace candidates that correspond to the given file.
    """
    try:
        trace_candidates = list(t for t in traces if t['filename'] == fspec.lfn and t['scope'] == fspec.scope)
        if trace_candidates:
            return trace_candidates
        logger.warning(f'file does not match to any trace received from Rucio: {fspec.lfn} {fspec.scope}')
    except Exception as error:
        logger.warning(f'traces from pilot and rucio could not be merged: {error}')
    return []


def get_json_summary(filename: str) -> str:
    """
    Return the content of the JSON summary file created by Rucio.

    Args:
        filename: path to the JSON summary file.

    Returns:
        Parsed JSON content of the summary file, or empty string if the file does not exist.
    """
    summary_json = ""
    if os.path.exists(filename):
        with open(filename, 'rb') as f:
            summary_json = json.load(f)
    else:
        logger.error(f'Failed to resolve Rucio summary JSON, wrong path? file={filename}')

    return summary_json


#@timeout(seconds=10800)
def copy_out(files: list, **kwargs: dict) -> list:  # noqa: C901
    """
    Upload given files using rucio copytool.

    Args:
        files: list of FileSpec objects.
        kwargs: kwargs dictionary.

    Returns:
        Updated list of files.

    Raises:
        PilotException: in case of controlled error.
    """
    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'
    logger.info(f'rucio stage-out: X509_USER_PROXY={os.environ.get("X509_USER_PROXY", "")}')

    summary = kwargs.pop('summary', True)
    ignore_errors = kwargs.pop('ignore_errors', False)
    trace_report = kwargs.get('trace_report')
    rucio_host = kwargs.get('rucio_host', '')

    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', None)
    for fspec in files:
        logger.info(f'rucio copytool, uploading file with scope: {fspec.scope} and lfn: {fspec.lfn}')
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint)
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset, url=fspec.surl, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.start_file()  # every file's trace has its own start time stamps
        fspec.status_code = 0

        summary_file_path = None
        cwd = fspec.workdir or kwargs.get('workdir') or '.'
        if summary:
            summary_file_path = os.path.join(cwd, 'rucio_upload.json')

        logger.info(f'the file will be uploaded to {fspec.ddmendpoint}')
        trace_report_out = []
        attempts = []  # failed upload attempts that were followed by another attempt
        transfer_timeout = get_timeout(fspec.filesize)
        ctimeout = transfer_timeout + 10  # give the API a chance to do the time-out first
        stageout_attempts = get_number_of_attempts()
        if stageout_attempts > 1:
            ctimeout *= stageout_attempts

        logger.info(f'overall transfer timeout={ctimeout} (stageout attempts: {stageout_attempts})')

        error_msg = ""
        ec = 0
        try:
            ec, trace_report_out = timeout(ctimeout, TimedThread)(_stage_out_api)(fspec, summary_file_path, trace_report,
                                                                                  trace_report_out, transfer_timeout,
                                                                                  rucio_host, attempts=attempts)
        except PilotException as error:
            logger.warning(f"PilotException caught during stage-out: {str(error)}")
            error_details = handle_rucio_error(str(error), trace_report, trace_report_out, fspec, stagein=False)
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            if not ignore_errors:
                send_stageout_traces(trace_report, attempts)
                msg = f" {fspec.scope}:{fspec.lfn} to {fspec.ddmendpoint}, {error_details.get('error')}"
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state')) from error
        except Exception as error:
            logger.warning(f"Exception caught during stage-out: {str(error)}")
            error_details = handle_rucio_error(str(error), trace_report, trace_report_out, fspec, stagein=False)
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            if not ignore_errors:
                send_stageout_traces(trace_report, attempts)
                msg = f" {fspec.scope}:{fspec.lfn} to {fspec.ddmendpoint}, {error_details.get('error')}"
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state')) from error

        if summary:
            summary_json = get_json_summary(summary_file_path)
            # {'panda:RDO_f22aae82-6317-4f79-9118-eacc73532f70.root': {'adler32': '7951df23', 'bytes': 1114317,
            # 'guid': 'c0dab595739d40cfafee0e5e6fba6b53', 'md5': '714b77553a718ab7dd0733408c76dd9f', '
            # name': 'RDO_f22aae82-6317-4f79-9118-eacc73532f70.root',
            # 'pfn': 'root://eosatlas.cern.ch:1094//eos/atlas/atlasdatadisk/rucio/panda/f6/48/RDO_f22aae82-6317-4f79-9118-eacc73532f70.root',
            # 'rse': 'CERN-PROD_DATADISK', 'scope': 'panda'}}

            # extract pfn already here in case of trouble with the state reason, so that the trace report at least contains the pfn (turl) and protocol
            dat = summary_json.get(f"{fspec.scope}:{fspec.lfn}") or {}
            fspec.turl = dat.get('pfn')
            trace_report.update(url=fspec.turl)
        else:
            summary_json = ""
            dat = {}
        protocol = get_protocol(trace_report_out)
        trace_report.update(protocol=protocol)
        rename_xrdlog(fspec.lfn)

        # make sure there was no missed failure (only way to deal with this until rucio API has been fixed)
        # (using the timeout decorator prevents the trace_report_out from being updated - rucio API should return
        # the proper error immediately instead of encoding it into a dictionary)
        state_reason = None if not trace_report_out else trace_report_out[0].get('stateReason')
        if ec and state_reason and not error_msg:
            error_details = handle_rucio_error(state_reason, trace_report, trace_report_out, fspec, stagein=False)

            if not ignore_errors:
                send_stageout_traces(trace_report, attempts)
                msg = f" {fspec.scope}:{fspec.lfn} from {fspec.ddmendpoint}, {error_details.get('error')}"
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        # continue
        # resolve final pfn (turl) from the summary JSON
        if summary and dat:
            trace_report.update(validateStart=time())
            checksum = dat.get(config.File.checksum_type)
            local_checksum = fspec.checksum.get(config.File.checksum_type)
            if local_checksum and checksum and local_checksum != checksum:
                msg = f'checksum verification failed: local {local_checksum} != remote {checksum}'
                logger.warning(msg)
                fspec.status = 'failed'
                fspec.status_code = ErrorCodes.PUTADMISMATCH if config.File.checksum_type == 'adler32' else ErrorCodes.PUTMD5MISMATCH
                state = 'AD_MISMATCH' if config.File.checksum_type == 'adler32' else 'MD_MISMATCH'
                trace_report.update(clientState=state, stateReason=msg, timeEnd=time())
                send_stageout_traces(trace_report, attempts)
                if not ignore_errors:
                    raise PilotException("Failed to stageout: CRC mismatched", code=fspec.status_code, state=state)
            elif local_checksum and checksum and local_checksum == checksum:
                logger.info(f'local checksum ({local_checksum}) = remote checksum ({checksum})')
            else:
                logger.warning(f'checksum could not be verified: local checksum ({local_checksum}), remote checksum ({checksum})')
        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        send_stageout_traces(trace_report, attempts)

    return files


def record_upload_attempt(attempts: list | None, trace_report_out: list, error: BaseException) -> None:
    """Record a failed upload attempt that will be followed by another attempt.

    Args:
        attempts: List to append the attempt to (nothing is recorded if None).
        trace_report_out: Rucio traces of the attempt.
        error: Exception raised by the upload client.
    """
    snapshot = get_upload_attempt_snapshot(trace_report_out, error)
    logger.warning(f'stage-out attempt failed and will be retried: {snapshot.get("stateReason")}')
    if attempts is not None:
        attempts.append(snapshot)


def _stage_in_api(dst: str, fspec: Any, trace_report: dict, trace_report_out: list, transfer_timeout: int,
                  use_pcache: bool, rucio_host: str, *, attempts: list | None = None) -> tuple[int, list]:
    """
    Stage-in files using the Rucio API.

    Args:
        dst: destination directory.
        fspec: FileSpec object.
        trace_report: trace report.
        trace_report_out: list of trace reports from Rucio.
        transfer_timeout: transfer timeout in seconds.
        use_pcache: True if pcache should be used, False otherwise.
        rucio_host: Rucio host URL.
        attempts: list to record the failed download attempts in (see capture_failed_attempts()).

    Returns:
        Tuple of (exit_code, trace_report_out).
    """
    ec = 0

    # init. download client
    from rucio.client import Client
    from rucio.client.downloadclient import DownloadClient
    # for ATLAS: rucio_host = 'https://voatlasrucio-server-prod.cern.ch:443'
    if rucio_host:
        logger.debug(f'using rucio_host={rucio_host}')
        rucio_client = Client(rucio_host=rucio_host)
        download_client = DownloadClient(client=rucio_client, logger=logger)
    else:
        download_client = DownloadClient(logger=logger)
    if use_pcache:
        download_client.check_pcache = True

    # traces are switched off
    if hasattr(download_client, 'tracing'):
        download_client.tracing = tracing_rucio

    # keep the failed download attempts (rucio retries internally), to report them in the traces
    capture_failed_attempts(download_client, attempts)

    # file specifications before the actual download
    _file = {}
    _file['did_scope'] = fspec.scope
    _file['did_name'] = fspec.lfn
    _file['did'] = f'{fspec.scope}:{fspec.lfn}'
    _file['rse'] = fspec.ddmendpoint
    _file['base_dir'] = dst
    _file['no_subdir'] = True
    if fspec.turl:
        _file['pfn'] = fspec.turl

    if transfer_timeout:
        _file['transfer_timeout'] = transfer_timeout
    _file['connection_timeout'] = 60 * 60

    # proceed with the download
    logger.info(f'rucio API stage-in dictionary: {_file}')
    trace_pattern = {}
    if trace_report:
        trace_pattern = trace_report

    # download client raises an exception if any file failed
    try:
        logger.info('*** rucio API downloading file (taking over logging) ***')
        if fspec.turl:
            result = download_client.download_pfns([_file], 1, trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
        else:
            result = download_client.download_dids([_file], trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
    except Exception as error:
        logger.warning('*** rucio API download client failed ***')
        logger.warning(f'caught exception: {error}')
        logger.debug(f'trace_report_out={trace_report_out}')
        # only raise an exception if the error info cannot be extracted
        if not trace_report_out:
            raise error
        if not trace_report_out[0].get('stateReason'):
            raise error
        ec = -1
    else:
        logger.info('*** rucio API download client finished ***')
        logger.debug(f'client returned {result}')

    logger.debug(f'trace_report_out={trace_report_out}')

    return ec, trace_report_out


def _stage_in_bulk(dst: str, files: list, trace_report_out: list = None, trace_common_fields: dict = None,
                   rucio_host: str = '') -> None:
    """
    Stage-in files in bulk using the Rucio API.

    Args:
        dst: destination directory.
        files: list of fspec objects.
        trace_report_out: List that will be populated with trace reports from Rucio.
        trace_common_fields: trace report common fields.
        rucio_host: optional Rucio host URL.

    Raises:
        Exception: download_client.download_pfns exception.
    """
    if trace_report_out is None:
        trace_report_out = []
    if trace_common_fields is None:
        trace_common_fields = {}
    # init. download client
    from rucio.client import Client
    from rucio.client.downloadclient import DownloadClient
    # for ATLAS: rucio_host = 'https://voatlasrucio-server-prod.cern.ch:443'
    if rucio_host:
        logger.debug(f'using rucio_host={rucio_host}')
        rucio_client = Client(rucio_host=rucio_host)
        download_client = DownloadClient(client=rucio_client, logger=logger)
    else:
        download_client = DownloadClient(logger=logger)

    # traces are switched off
    if hasattr(download_client, 'tracing'):
        download_client.tracing = tracing_rucio

    # build the list of file dictionaries before calling the download function
    file_list = []

    for fspec in files:
        fspec.status_code = 0

        # file specifications before the actual download
        _file = {}
        _file['did_scope'] = fspec.scope
        _file['did_name'] = fspec.lfn
        _file['did'] = f'{fspec.scope}:{fspec.lfn}'
        _file['rse'] = fspec.ddmendpoint
        _file['base_dir'] = fspec.workdir or dst
        _file['no_subdir'] = True
        if fspec.turl:
            _file['pfn'] = fspec.turl
        else:
            logger.warning('cannot perform bulk download since fspec.turl is not set (required by download_pfns()')
            # fail somehow

        if fspec.filesize:
            _file['transfer_timeout'] = get_timeout(fspec.filesize)
        _file['connection_timeout'] = 60 * 60

        file_list.append(_file)

    # proceed with the download
    trace_pattern = trace_common_fields if trace_common_fields else {}

    # download client raises an exception if any file failed
    num_threads = len(file_list)
    logger.info('*** rucio API downloading files (taking over logging) ***')
    try:
        result = download_client.download_pfns(file_list, num_threads, trace_custom_fields=trace_pattern, traces_copy_out=trace_report_out)
    except Exception as error:
        logger.warning('*** rucio API download client failed ***')
        logger.warning(f'caught exception: {error}')
        logger.debug(f'trace_report_out={trace_report_out}')
        # only raise an exception if the error info cannot be extracted
        if not trace_report_out:
            raise error
        if not trace_report_out[0].get('stateReason'):
            raise error
    else:
        logger.info('*** rucio API download client finished ***')
        logger.debug(f'client returned {result}')


def get_number_of_attempts() -> int:
    """
    Get the number of stage-out attempts.

    Returns:
        Number of stage-out attempts.
    """
    try:
        stageout_attempts = int(os.environ.get('PILOT_STAGEOUT_ATTEMPTS', 1))
    except ValueError as exc:
        logger.warning(f'failed to parse PILOT_STAGEOUT_ATTEMPTS, defaulting to 1: {exc}')
        stageout_attempts = 1

    return stageout_attempts


def _stage_out_api(fspec: Any, summary_file_path: str, trace_report: dict, trace_report_out: list,  # noqa: C901
                   transfer_timeout: int, rucio_host: str, *, attempts: list | None = None) -> tuple[int, list]:
    """
    Stage-out files using the Rucio API.

    The upload is attempted up to ``PILOT_STAGEOUT_ATTEMPTS`` times. Each attempt is counted in
    ``fspec.stageout_attempts`` before it is made (so that an attempt cut short by the overall time-out is
    counted too). ``trace_report_out`` only holds the Rucio traces of the latest attempt, so that the error and
    protocol read from it after the call belong to the attempt that decided the outcome. A failed attempt that is
    followed by another attempt is recorded in ``attempts`` (see :func:`get_upload_attempt_snapshot`).

    Args:
        fspec: FileSpec object.
        summary_file_path: path to summary file.
        trace_report: trace report.
        trace_report_out: trace report from Rucio.
        transfer_timeout: transfer time-out in seconds.
        rucio_host: Rucio host URL.
        attempts: list to record the failed upload attempts in that were followed by another attempt.

    Returns:
        Tuple of (exit_code, trace_report_out).
    """
    ec = 0

    # init. download client
    from rucio.client import Client
    from rucio.client.uploadclient import UploadClient
    # for ATLAS: rucio_host = 'https://voatlasrucio-server-prod.cern.ch:443'
    if rucio_host:
        logger.debug(f'using rucio_host={rucio_host}')
        rucio_client = Client(rucio_host=rucio_host)
        upload_client = UploadClient(_client=rucio_client, logger=logger)
    else:
        upload_client = UploadClient(logger=logger)

    # traces are turned off
    if hasattr(upload_client, 'tracing'):
        upload_client.tracing = tracing_rucio
    if tracing_rucio:
        upload_client.trace = trace_report

    # file specifications before the upload
    _file = {}
    _file['path'] = fspec.surl or getattr(fspec, 'pfn', None) or os.path.join(fspec.workdir, fspec.lfn)
    _file['rse'] = fspec.ddmendpoint
    _file['did_scope'] = fspec.scope
    _file['no_register'] = True

    if transfer_timeout:
        _file['transfer_timeout'] = transfer_timeout
    _file['connection_timeout'] = 60 * 60

    # if fspec.storageId and int(fspec.storageId) > 0:
    #     if fspec.turl and fspec.is_nondeterministic:
    #         f['pfn'] = fspec.turl
    # elif fspec.lfn and '.root' in fspec.lfn:
    #     f['guid'] = fspec.guid
    if fspec.lfn and '.root' in fspec.lfn:
        _file['guid'] = fspec.guid

    logger.info(f'rucio API stage-out dictionary: {_file}')
    logger.info('*** rucio API uploading file (taking over logging) ***')
    logger.debug(f'trace_report_out={trace_report_out}')
    stageout_attempts = get_number_of_attempts()

    # upload client raises an exception if any file failed
    for attempt in range(stageout_attempts):
        logger.info(f'stage-out attempt {attempt + 1}/{stageout_attempts}')
        fspec.stageout_attempts = (getattr(fspec, 'stageout_attempts', 0) or 0) + 1
        # the upload client adds one trace per call; keep only those of this attempt (the list object is kept,
        # since the caller holds a reference to it)
        del trace_report_out[:]

        try:
            result = upload_client.upload([_file], summary_file_path=summary_file_path, traces_copy_out=trace_report_out, ignore_availability=True)
        except UnboundLocalError as error:
            logger.warning('*** rucio API upload client failed ***')
            logger.warning('rucio still needs a bug fix of the summary in the uploadclient')
            if attempt == stageout_attempts - 1:  # last attempt
                ec = -1
            else:
                record_upload_attempt(attempts, trace_report_out, error)
                continue
        except Exception as error:
            logger.warning('*** rucio API upload client failed ***')
            logger.warning(f'caught exception: {error}')
            logger.error(traceback.format_exc())
            logger.debug(f'trace_report_out={trace_report_out}')

            if attempt == stageout_attempts - 1:  # last attempt
                if not trace_report_out:
                    raise error
                if not trace_report_out[0].get('stateReason'):
                    raise error
                if "None of the given files have been uploaded" in str(error):
                    raise PilotException(f"Rucio upload failed: {error}")
                ec = -1
            else:
                record_upload_attempt(attempts, trace_report_out, error)
                continue
        else:
            logger.warning('*** rucio API upload client finished ***')
            logger.debug(f'client returned {result}')
            break

    return ec, trace_report_out
