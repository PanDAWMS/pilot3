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
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2023
# - Tomas Javurek, tomas.javurek@cern.ch, 2019
# - Tomas Javurek, tomas.javurek@cern.ch, 2019
# - David Cameron, david.cameron@cern.ch, 2019

from __future__ import absolute_import  # Python 2 (2to3 complains about this)

import os
import json
import logging
from time import time
from copy import deepcopy

from pilot.common.exception import (
    PilotException,
    ErrorCodes,
)
from pilot.util.timer import (
    timeout,
    TimedThread,
)
from .common import (
    resolve_common_transfer_errors,
    verify_catalog_checksum,
    get_timeout
)
from pilot.util.config import config
from pilot.util.filehandling import rename_xrdlog

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# can be disabled for Rucio if allowed to use all RSE for input
require_replicas = True    ## indicates if given copytool requires input replicas to be resolved
require_protocols = False  ## indicates if given copytool requires protocols to be resolved first for stage-out
tracing_rucio = False      ## should Rucio send the trace?


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files):
    return True  ## FIX ME LATER


#@timeout(seconds=10800)
def copy_in(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
    """

    ignore_errors = kwargs.get('ignore_errors')
    trace_report = kwargs.get('trace_report')
    use_pcache = kwargs.get('use_pcache')
    rucio_host = kwargs.get('rucio_host', '')

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    # note, env vars might be unknown inside middleware contrainers, if so get the value already in the trace report
    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', trace_report.get_value('localSite'))
    for fspec in files:
        logger.info(f'rucio copytool, downloading file with scope:{fspec.scope} lfn:{fspec.lfn}')
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
        trace_report.update(url=fspec.turl if fspec.turl else fspec.surl)
        trace_report.update(catStart=time())  ## is this metric still needed? LFC catalog
        fspec.status_code = 0
        dst = fspec.workdir or kwargs.get('workdir') or '.'

        trace_report_out = []
        transfer_timeout = get_timeout(fspec.filesize)
        ctimeout = transfer_timeout + 10  # give the API a chance to do the time-out first
        logger.info(f'overall transfer timeout={ctimeout}')

        error_msg = ""
        ec = 0
        try:
            ec, trace_report_out = timeout(ctimeout, timer=TimedThread)(_stage_in_api)(dst, fspec, trace_report,
                                                                                       trace_report_out, transfer_timeout,
                                                                                       use_pcache, rucio_host)
        except Exception as error:
            error_msg = str(error)
            error_details = handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=True)
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            if not ignore_errors:
                trace_report.send()
                msg = f"{fspec.scope}:{fspec.lfn} from {fspec.ddmendpoint}, {error_details.get('error')}"
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))
        else:
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            rename_xrdlog(fspec.lfn)

        # make sure there was no missed failure (only way to deal with this until rucio API has been fixed)
        # (using the timeout decorator prevents the trace_report_out from being updated - rucio API should return
        # the proper error immediately instead of encoding it into a dictionary)
        state_reason = None if not trace_report_out else trace_report_out[0].get('stateReason')
        if ec and state_reason and not error_msg:
            error_details = handle_rucio_error(state_reason, trace_report, trace_report_out, fspec, stagein=True)

            if not ignore_errors:
                trace_report.send()
                msg = f"{fspec.scope}:{fspec.lfn} from {fspec.ddmendpoint}, {error_details.get('error')}"
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        destination = os.path.join(dst, fspec.lfn)
        if os.path.exists(destination):
            state, diagnostics = verify_catalog_checksum(fspec, destination)
            if diagnostics != "" and not ignore_errors:
                trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                    timeEnd=time())
                trace_report.send()
                raise PilotException(diagnostics, code=fspec.status_code, state=state)
        else:
            diagnostics = f'file does not exist: {destination} (cannot verify catalog checksum)'
            logger.warning(diagnostics)
            state = 'STAGEIN_ATTEMPT_FAILED'
            fspec.status_code = ErrorCodes.STAGEINFAILED
            trace_report.update(clientState=state, stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        trace_report.send()

    return files


def get_protocol(trace_report_out):
    """
    Extract the protocol used for the transfer from the dictionary returned by rucio.

    :param trace_report_out: returned rucio transfer dictionary (dictionary).
    :return: protocol (string).
    """

    try:
        protocol = trace_report_out[0].get('protocol')
    except Exception as error:
        logger.warning(f'exception caught: {error}')
        protocol = ''

    return protocol


def handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=True):
    """

    :param error_msg:
    :param trace_report:
    :param trace_report_out:
    :param fspec:
    :return:
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


def copy_in_bulk(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
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
            for fspec in files:
                localsite = localsite if localsite else fspec.ddmendpoint
                trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                trace_report.update('STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics, timeEnd=time())
                trace_report.send()
            logger.error(diagnostics)
            raise PilotException(diagnostics, code=fspec.status_code, state='STAGEIN_ATTEMPT_FAILED')

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


def _get_trace(fspec, traces):
    """
    Traces returned by Rucio are not orderred the same as input files from pilot.
    This method finds the proper trace.

    :param: fspec: the file that is seeked
    :param: traces: all traces that are received by Rucio

    :return: trace_candiates that correspond to the given file
    """
    try:
        try:
            trace_candidates = list(filter(lambda t: t['filename'] == fspec.lfn and t['scope'] == fspec.scope, traces))  # Python 2
        except Exception:
            trace_candidates = list([t for t in traces if t['filename'] == fspec.lfn and t['scope'] == fspec.scope])  # Python 3
        if trace_candidates:
            return trace_candidates
        else:
            logger.warning(f'file does not match to any trace received from Rucio: {fspec.lfn} {fspec.scope}')
    except Exception as error:
        logger.warning(f'traces from pilot and rucio could not be merged: {error}')
        return []


#@timeout(seconds=10800)
def copy_out(files, **kwargs):  # noqa: C901
    """
        Upload given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :param ignore_errors: boolean, if specified then transfer failures will be ignored
        :raise: PilotException in case of controlled error
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
        logger.info('rucio copytool, uploading file with scope: %s and lfn: %s' % (str(fspec.scope), str(fspec.lfn)))
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint)
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset, url=fspec.surl, filesize=fspec.filesize)
        trace_report.update(catStart=time(), filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        fspec.status_code = 0

        summary_file_path = None
        cwd = fspec.workdir or kwargs.get('workdir') or '.'
        if summary:
            summary_file_path = os.path.join(cwd, 'rucio_upload.json')

        logger.info('the file will be uploaded to %s' % str(fspec.ddmendpoint))
        trace_report_out = []
        transfer_timeout = get_timeout(fspec.filesize)
        ctimeout = transfer_timeout + 10  # give the API a chance to do the time-out first
        logger.info('overall transfer timeout=%s' % ctimeout)

        error_msg = ""
        ec = 0
        try:
            ec, trace_report_out = timeout(ctimeout, TimedThread)(_stage_out_api)(fspec, summary_file_path, trace_report,
                                                                                  trace_report_out, transfer_timeout,
                                                                                  rucio_host)
            #_stage_out_api(fspec, summary_file_path, trace_report, trace_report_out)
        except PilotException as error:
            error_msg = str(error)
            error_details = handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=False)
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s to %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))
        except Exception as error:
            error_msg = str(error)
            error_details = handle_rucio_error(error_msg, trace_report, trace_report_out, fspec, stagein=False)
            protocol = get_protocol(trace_report_out)
            trace_report.update(protocol=protocol)
            if not ignore_errors:
                trace_report.send()
                msg = ' %s:%s to %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))
        else:
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
                trace_report.send()
                msg = ' %s:%s from %s, %s' % (fspec.scope, fspec.lfn, fspec.ddmendpoint, error_details.get('error'))
                raise PilotException(msg, code=error_details.get('rcode'), state=error_details.get('state'))

        if summary:  # resolve final pfn (turl) from the summary JSON
            if not os.path.exists(summary_file_path):
                logger.error('Failed to resolve Rucio summary JSON, wrong path? file=%s' % summary_file_path)
            else:
                with open(summary_file_path, 'rb') as f:
                    summary_json = json.load(f)
                    dat = summary_json.get(f"{fspec.scope}:{fspec.lfn}") or {}
                    fspec.turl = dat.get('pfn')
                    # quick transfer verification:
                    # the logic should be unified and moved to base layer shared for all the movers
                    checksum = dat.get(config.File.checksum_type)
                    local_checksum = fspec.checksum.get(config.File.checksum_type)
                    if local_checksum and checksum and local_checksum != checksum:
                        msg = f'checksum verification failed: local {local_checksum} != remote {checksum}'
                        logger.warning(msg)
                        fspec.status = 'failed'
                        fspec.status_code = ErrorCodes.PUTADMISMATCH if config.File.checksum_type == 'adler32' else ErrorCodes.PUTMD5MISMATCH
                        state = 'AD_MISMATCH' if config.File.checksum_type == 'adler32' else 'MD_MISMATCH'
                        trace_report.update(clientState=state, stateReason=msg, timeEnd=time())
                        trace_report.send()
                        if not ignore_errors:
                            raise PilotException("Failed to stageout: CRC mismatched",
                                                 code=fspec.status_code, state=state)
                    else:
                        if local_checksum and checksum and local_checksum == checksum:
                            logger.info(f'local checksum ({local_checksum}) = remote checksum ({checksum})')
                        else:
                            logger.warning(f'checksum could not be verified: local checksum ({local_checksum}), '
                                           f'remote checksum ({checksum})')
        if not fspec.status_code:
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())

        trace_report.send()

    return files


def _stage_in_api(dst, fspec, trace_report, trace_report_out, transfer_timeout, use_pcache, rucio_host):
    """
    Stage-in files using the Rucio API.
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


def _stage_in_bulk(dst, files, trace_report_out=None, trace_common_fields=None, rucio_host=''):
    """
    Stage-in files in bulk using the Rucio API.

    :param dst: destination (string).
    :param files: list of fspec objects.
    :param trace_report_out:
    :param trace_common_fields:
    :param rucio_host: optional rucio host (string).
    :return:
    """
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
        _file['did'] = '%s:%s' % (fspec.scope, fspec.lfn)
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


def _stage_out_api(fspec, summary_file_path, trace_report, trace_report_out, transfer_timeout, rucio_host):
    """
    Stage-out files using the Rucio API.
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

    # upload client raises an exception if any file failed
    try:
        logger.info('*** rucio API uploading file (taking over logging) ***')
        logger.debug(f'summary_file_path={summary_file_path}')
        logger.debug(f'trace_report_out={trace_report_out}')
        result = upload_client.upload([_file], summary_file_path=summary_file_path, traces_copy_out=trace_report_out, ignore_availability=True)
    except Exception as error:
        logger.warning('*** rucio API upload client failed ***')
        logger.warning(f'caught exception: {error}')
        import traceback
        logger.error(traceback.format_exc())
        logger.debug(f'trace_report_out={trace_report_out}')
        if not trace_report_out:
            raise error
        if not trace_report_out[0].get('stateReason'):
            raise error
        ec = -1
    except UnboundLocalError:
        logger.warning('*** rucio API upload client failed ***')
        logger.warning('rucio still needs a bug fix of the summary in the uploadclient')
    else:
        logger.warning('*** rucio API upload client finished ***')
        logger.debug(f'client returned {result}')

    return ec, trace_report_out
