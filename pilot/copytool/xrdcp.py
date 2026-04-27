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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2024
# - Alexey Anisenkov, anisyonk@cern.ch, 2017

"""Xrdcp copy tool."""

from __future__ import annotations
import logging
import os
import re
from time import time
from typing import Optional

from pilot.util.container import execute
from pilot.common.exception import PilotException, ErrorCodes
#from pilot.util.timer import timeout
from .common import resolve_common_transfer_errors, verify_catalog_checksum  #, get_timeout

logger = logging.getLogger(__name__)

require_replicas = True  ## indicate if given copytool requires input replicas to be resolved
allowed_schemas = ['root']  # prioritized list of supported schemas for transfers by given copytool

copy_command = 'xrdcp'


def is_valid_for_copy_in(files: list) -> bool:
    """
    Determine if this copytool is valid for input for the given file list.

    Placeholder.

    Args:
        files: list of FileSpec objects.

    Returns:
        Always True (for now).
    """
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    if files:  # to get rid of pylint warning
        pass
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
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    if files:  # to get rid of pylint warning
        pass
    return True  ## FIX ME LATER


def _resolve_checksum_option(setup: str, **kwargs: dict) -> str:
    """
    Resolve which checksum option to use.

    Args:
        setup: setup string.
        kwargs: kwargs dictionary.

    Returns:
        Checksum option string.
    """
    cmd = f"{copy_command} -h"
    if setup:
        cmd = f"source {setup}; {cmd}"

    logger.info(f"execute command ({cmd}) to decide which option should be used to calc/verify file checksum..")

    rcode, stdout, stderr = execute(cmd, **kwargs)
    output = stdout + stderr

    coption = ""
    checksum_type = 'adler32'  ## consider only adler32 for now

    if rcode:
        logger.error(f'FAILED to execute command={cmd}: {output}')
    elif "--cksum" in output:
        coption = f"--cksum {checksum_type}:print"
    elif "-adler" in output and checksum_type == 'adler32':
        coption = "-adler"
    elif "-md5" in output and checksum_type == 'md5':
        coption = "-md5"

    if coption:
        logger.info(f"use {coption} option to get the checksum for {copy_command} command")

    return coption


#@timeout(seconds=10800)
def _stagefile(coption: str, source: str, destination: str, filesize: int, is_stagein: bool, setup: str = None,
               **kwargs: dict) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Stage the given file (stagein or stageout).

    Args:
        coption: checksum option.
        source: file source path.
        destination: file destination path.
        filesize: file size.
        is_stagein: True for stage-in, False for stage-out.
        setup: setup string.
        kwargs: kwargs dictionary.

    Returns:
        Tuple of (filesize, checksum, checksum_type), or (None, None, None) if coption is empty or on error.

    Raises:
        PilotException: in case of controlled error.
    """
    if filesize:  # to get rid of pylint warning - could be useful
        pass
    filesize_cmd, checksum_cmd, checksum_type = None, None, None

    cmd = f'{copy_command} -np -f {coption} {source} {destination}'
    if setup:
        cmd = f"source {setup}; {cmd}"

    #timeout = get_timeout(filesize)
    rcode, stdout, stderr = execute(cmd, **kwargs)
    logger.info(f'rcode={rcode}, stdout={stdout}, stderr={stderr}')

    if rcode:  ## error occurred
        error = resolve_common_transfer_errors(stdout + stderr, is_stagein=is_stagein)

        #rcode = error.get('rcode')  ## TO BE IMPLEMENTED
        #if not is_stagein and rcode == PilotErrors.ERR_CHKSUMNOTSUP: ## stage-out, on fly checksum verification is not supported .. ignore
        #    logger.info('stage-out: ignore ERR_CHKSUMNOTSUP error .. will explicitly verify uploaded file')
        #    return None, None, None

        raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

    # extract filesize and checksum values from output
    if coption != "":
        filesize_cmd, checksum_cmd, checksum_type = get_file_info_from_output(stdout + stderr)

    ## verify transfer by returned checksum or call remote checksum calculation
    ## to be moved at the base level

    is_verified = True   ## TO BE IMPLEMENTED LATER

    if not is_verified:
        rcode = ErrorCodes.GETADMISMATCH if is_stagein else ErrorCodes.PUTADMISMATCH
        raise PilotException("Copy command failed", code=rcode, state='AD_MISMATCH')

    return filesize_cmd, checksum_cmd, checksum_type


# @timeout(seconds=10800)
def copy_in(files: list, **kwargs: dict) -> list:
    """
    Download given files using xrdcp command.

    Args:
        files: list of FileSpec objects.
        kwargs: kwargs dictionary.

    Returns:
        Updated list of files.

    Raises:
        PilotException: in case of controlled error.
    """
    #allow_direct_access = kwargs.get('allow_direct_access') or False
    setup = kwargs.pop('copytools', {}).get('xrdcp', {}).get('setup')
    coption = _resolve_checksum_option(setup, **kwargs)
    trace_report = kwargs.get('trace_report')

    # note, env vars might be unknown inside middleware contrainers, if so get the value already in the trace report
    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', trace_report.get_value('localSite'))
    for fspec in files:
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)

        # continue loop for files that are to be accessed directly  ## TOBE DEPRECATED (anisyonk)
        #if fspec.is_directaccess(ensure_replica=False) and allow_direct_access and fspec.accessmode == 'direct':
        #    fspec.status_code = 0
        #    fspec.status = 'remote_io'
        #    trace_report.update(url=fspec.turl, clientState='FOUND_ROOT', stateReason='direct_access')
        #    trace_report.send()
        #    continue

        trace_report.update(catStart=time())

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        destination = os.path.join(dst, fspec.lfn)
        try:
            _, checksum_cmd, checksum_type = _stagefile(coption, fspec.turl, destination, fspec.filesize,
                                                        is_stagein=True, setup=setup, **kwargs)
            fspec.status_code = 0
            fspec.status = 'transferred'
        except PilotException as error:
            fspec.status = 'failed'
            fspec.status_code = error.get_error_code()
            diagnostics = error.get_detail()
            state = 'STAGEIN_ATTEMPT_FAILED'
            trace_report.update(clientState=state, stateReason=diagnostics, timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state) from error

        # compare checksums
        fspec.checksum[checksum_type] = checksum_cmd  # remote checksum
        state, diagnostics = verify_catalog_checksum(fspec, destination)
        if diagnostics != "":
            trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

        trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
        trace_report.send()

    return files


# @timeout(seconds=10800)
def copy_out(files: list, **kwargs: dict) -> list:
    """
    Upload given files using xrdcp command.

    Args:
        files: list of FileSpec objects.
        kwargs: kwargs dictionary.

    Returns:
        Updated list of files.

    Raises:
        PilotException: in case of controlled error.
    """
    setup = kwargs.pop('copytools', {}).get('xrdcp', {}).get('setup')
    coption = _resolve_checksum_option(setup, **kwargs)
    trace_report = kwargs.get('trace_report')

    for fspec in files:
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset, url=fspec.surl, filesize=fspec.filesize)
        trace_report.update(catStart=time(), filename=fspec.lfn, guid=fspec.guid.replace('-', ''))

        try:
            _, checksum_cmd, checksum_type = _stagefile(coption, fspec.surl, fspec.turl, fspec.filesize,
                                                        is_stagein=False, setup=setup, **kwargs)
            fspec.status_code = 0
            fspec.status = 'transferred'
            trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
            trace_report.send()
        except PilotException as error:
            fspec.status = 'failed'
            fspec.status_code = error.get_error_code()
            state = 'STAGEOUT_ATTEMPT_FAILED'
            diagnostics = error.get_detail()
            trace_report.update(clientState=state, stateReason=diagnostics, timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state) from error

        # compare checksums
        fspec.checksum[checksum_type] = checksum_cmd  # remote checksum
        state, diagnostics = verify_catalog_checksum(fspec, fspec.surl)
        if diagnostics != "":
            trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

    return files


def get_file_info_from_output(output: str) -> tuple[Optional[int], Optional[str], Optional[str]]:
    """
    Extract file size, checksum value from the xrdcp --chksum command output.

    Args:
        output: combined stdout and stderr output from xrdcp command.

    Returns:
        Tuple of (filesize, checksum, checksum_type), or (None, None, None) on failure.
    """
    if not output:
        return None, None, None

    if not ("xrootd" in output or "XRootD" in output or "adler32" in output):
        logger.warning(f"WARNING: Failed to extract checksum: Unexpected output: {output}")
        return None, None, None

    pattern = r"(?P<type>md5|adler32):\ (?P<checksum>[a-zA-Z0-9]+)\ \S+\ (?P<filesize>[0-9]+)"  # Python 3 (added r)
    filesize, checksum, checksum_type = None, None, None

    m = re.search(pattern, output)
    if m:
        checksum_type = m.group('type')
        checksum = m.group('checksum')
        checksum = checksum.zfill(8)  # make it 8 chars length (adler32 xrdcp fix)
        filesize = m.group('filesize')
        if filesize:
            try:
                filesize = int(filesize)
            except ValueError as error:
                logger.warning(f'failed to convert filesize to int: {error}')
                filesize = None
    else:
        logger.warning(f"WARNING: Checksum/file size info not found in output: "
                       f"failed to match pattern={pattern} in output={output}")

    return filesize, checksum, checksum_type
