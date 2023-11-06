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
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2023
# - Mario Lassnig, mario.lassnig@cern.ch, 2020

import logging
import os
import re
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.util.filehandling import (
    calculate_checksum,
    get_checksum_type,
    get_checksum_value,
)

logger = logging.getLogger(__name__)


def get_timeout(filesize: int, add: int = 0) -> int:
    """
    Get a proper time-out limit based on the file size.

    :param filesize: file size (int).
    :param add: optional additional time to be added [s] (int)
    :return: time-out in seconds (int).
    """

    timeout_max = 3 * 3600  # 3 hours
    timeout_min = 300  # self.timeout

    timeout = timeout_min + int(filesize / 0.1e7) + add  # approx < 1 Mb/sec

    return min(timeout, timeout_max)


def verify_catalog_checksum(fspec: Any, path: str) -> (str, str):
    """
    Verify that the local and remote (fspec) checksum values are the same.
    The function will update the fspec object.

    :param fspec: FileSpec object for a given file.
    :param path: path to local file (string).
    :return: state (string), diagnostics (string).
    """

    diagnostics = ""
    state = ""

    checksum_type = get_checksum_type(fspec.checksum)
    checksum_catalog = get_checksum_value(fspec.checksum)
    if checksum_type == 'unknown':
        diagnostics = f'unknown checksum type for checksum(catalog): {fspec.checksum}'
        logger.warning(diagnostics)
        fspec.status_code = ErrorCodes.UNKNOWNCHECKSUMTYPE
        fspec.status = 'failed'
        state = 'UNKNOWN_CHECKSUM_TYPE'
    else:
        try:
            checksum_local = calculate_checksum(path, algorithm=checksum_type)
        except (Exception) as exc:
            diagnostics = f'caught exception during checksum calculation: {exc}'
            logger.warning(diagnostics)
            fspec.status_code = ErrorCodes.CHECKSUMCALCFAILURE
            fspec.status = 'failed'
            state = 'CHECKSUMCALCULATIONFAILURE'
            return state, diagnostics

        if checksum_type == 'ad32':
            checksum_type = 'adler32'
        logger.info(f'checksum (catalog): {checksum_catalog} (type: {checksum_type})')
        logger.info(f'checksum (local): {checksum_local}')
        if checksum_local and checksum_local != '' and checksum_local != checksum_catalog:
            diagnostics = f'checksum verification failed for LFN={fspec.lfn}: ' \
                          f'checksum (catalog)={checksum_catalog} != checksum (local)={checksum_local}'
            logger.warning(diagnostics)
            fspec.status_code = ErrorCodes.GETADMISMATCH if checksum_type == 'adler32' else ErrorCodes.GETMD5MISMATCH
            fspec.status = 'failed'
            state = 'AD_MISMATCH' if checksum_type == 'ad32' else 'MD_MISMATCH'
        else:
            logger.info('catalog and local checksum values are the same')

    return state, diagnostics


def merge_destinations(files: list) -> dict:
    """
    Converts the file-with-destination dict to a destination-with-files dict

    :param files: files to merge (list)
    :return: destination-with-files dictionary.
    """
    destinations = {}
    # ensure type(files) == list
    for _file in files:
        if not os.path.exists(_file['destination']):
            _file['status'] = 'failed'
            _file['errmsg'] = f"Destination directory does not exist: {_file['destination']}"
            _file['errno'] = 1
        else:
            # ensure scope, name in f
            _file['status'] = 'running'
            _file['errmsg'] = 'File not yet successfully downloaded.'
            _file['errno'] = 2
            lfn = f"{_file['scope']}:{_file['name']}"
            dst = destinations.setdefault(_file['destination'], {'lfns': set(), 'files': []})
            dst['lfns'].add(lfn)
            dst['files'].append(_file)
    return destinations


def get_copysetup(copytools: list, copytool_name: str) -> str:
    """
    Return the copysetup for the given copytool.

    :param copytools: copytools list from infosys.
    :param copytool_name: name of copytool (string).
    :return: copysetup (string).
    """
    copysetup = ""

    if not copytools:
        return ""

    for ct in list(copytools.keys()):  # Python 2/3
        if copytool_name == ct:
            copysetup = copytools[ct].get('setup')
            break

    return copysetup


def get_error_info(rcode: int, state: str, error_msg: str) -> dict:
    """
    Return an error info dictionary specific to transfer errors.
    Helper function to resolve_common_transfer_errors().

    :param rcode: return code (int).
    :param state: state string used in Rucio traces.
    :param error_msg: transfer command stdout (string).
    :return: dictionary with format {'rcode': rcode, 'state': state, 'error': error_msg}.
    """

    return {'rcode': rcode, 'state': state, 'error': error_msg}


def output_line_scan(ret: dict, output: str) -> dict:
    """
    Do some reg exp on the transfer command output to search for special errors.
    Helper function to resolve_common_transfer_errors().

    :param ret: pre-filled error info dictionary with format {'rcode': rcode, 'state': state, 'error': error_msg}
    :param output: transfer command stdout (string).
    :return: updated error info dictionary.
    """

    for line in output.split('\n'):
        match = re.search(r"[Dd]etails\s*:\s*(?P<error>.*)", line)  # Python 3 (added r)
        if match:
            ret['error'] = match.group('error')
        elif 'service_unavailable' in line:
            ret['error'] = 'service_unavailable'
            ret['rcode'] = ErrorCodes.RUCIOSERVICEUNAVAILABLE

    return ret


def resolve_common_transfer_errors(output: str, is_stagein: bool = True) -> dict:  # noqa: C901
    """
    Resolve any common transfer related errors.

    :param output: stdout from transfer command (string).
    :param is_stagein: optional (boolean).
    :return: dict {'rcode': rcode, 'state': state, 'error': error_msg}.
    """

    # default to make sure dictionary exists and all fields are populated (some of which might be overwritten below)
    ret = get_error_info(ErrorCodes.STAGEINFAILED if is_stagein else ErrorCodes.STAGEOUTFAILED, 'COPY_ERROR', output)
    if not output:
        return ret

    if "timeout" in output:
        ret = get_error_info(ErrorCodes.STAGEINTIMEOUT if is_stagein else ErrorCodes.STAGEOUTTIMEOUT,
                             'CP_TIMEOUT', f'copy command timed out: {output}')
    elif "failed xrdadler32" in output:
        ret = get_error_info(ErrorCodes.GETADMISMATCH if is_stagein else ErrorCodes.PUTADMISMATCH,
                             'AD_MISMATCH', output)
    elif "does not match the checksum" in output and 'adler32' in output:
        ret = get_error_info(ErrorCodes.GETADMISMATCH if is_stagein else ErrorCodes.PUTADMISMATCH,
                             'AD_MISMATCH', output)
    elif "does not match the checksum" in output and 'adler32' not in output:
        ret = get_error_info(ErrorCodes.GETMD5MISMATCH if is_stagein else ErrorCodes.PUTMD5MISMATCH,
                             'MD5_MISMATCH', output)
    elif "globus_xio:" in output:
        ret = get_error_info(ErrorCodes.GETGLOBUSSYSERR if is_stagein else ErrorCodes.PUTGLOBUSSYSERR,
                             'GLOBUS_FAIL', f"Globus system error: {output}")
    elif "File exists" in output or 'SRM_FILE_BUSY' in output or 'file already exists' in output:
        ret = get_error_info(ErrorCodes.FILEEXISTS, 'FILE_EXISTS',
                             f"File already exists in the destination: {output}")
    elif "No such file or directory" in output and is_stagein:
        ret = get_error_info(ErrorCodes.MISSINGINPUTFILE, 'MISSING_INPUT', output)
    elif "query chksum is not supported" in output or "Unable to checksum" in output:
        ret = get_error_info(ErrorCodes.CHKSUMNOTSUP, 'CHKSUM_NOTSUP', output)
    elif "Could not establish context" in output:
        error_msg = f"Could not establish context: Proxy / VO extension of proxy has probably expired: {output}"
        ret = get_error_info(ErrorCodes.NOPROXY, 'CONTEXT_FAIL', error_msg)
    elif "No space left on device" in output:
        ret = get_error_info(ErrorCodes.NOLOCALSPACE if is_stagein else ErrorCodes.NOREMOTESPACE,
                             'NO_SPACE', f"No available space left on disk: {output}")
    elif "No such file or directory" in output:
        ret = get_error_info(ErrorCodes.NOSUCHFILE, 'NO_FILE', output)
    elif "service is not available at the moment" in output:
        ret = get_error_info(ErrorCodes.SERVICENOTAVAILABLE, 'SERVICE_ERROR', output)
    elif "Network is unreachable" in output:
        ret = get_error_info(ErrorCodes.UNREACHABLENETWORK, 'NETWORK_UNREACHABLE', output)
    elif "Run: [ERROR] Server responded with an error" in output:
        ret = get_error_info(ErrorCodes.XRDCPERROR, 'XRDCP_ERROR', output)
    elif "Unable to locate credentials" in output:
        ret = get_error_info(ErrorCodes.MISSINGCREDENTIALS, 'S3_ERROR', output)

    # reg exp the output to get real error message
    return output_line_scan(ret, output)
