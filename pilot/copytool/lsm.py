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
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2024

"""Local site mover copy tool."""

import logging
import errno
import os
from time import time

from pilot.common.exception import (
    ErrorCodes,
    PilotException
)
from pilot.util.container import execute
#from pilot.util.timer import timeout
from .common import (
    get_copysetup,
    verify_catalog_checksum,
    resolve_common_transfer_errors  #, get_timeout
)


logger = logging.getLogger(__name__)

require_replicas = True  ## indicate if given copytool requires input replicas to be resolved

allowed_schemas = ['srm', 'gsiftp', 'root']  # prioritized list of supported schemas for transfers by given copytool


def is_valid_for_copy_in(files: list) -> bool:
    """
    Determine if this copytool is valid for input for the given file list.

    Placeholder.

    :param files: list of FileSpec objects (list)
    :return: always True (for now) (bool).
    """
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files: list) -> bool:
    """
    Determine if this copytool is valid for output for the given file list.

    Placeholder.

    :param files: list of FileSpec objects (list)
    :return: always True (for now) (bool).
    """
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


def copy_in(files: list, **kwargs: dict) -> list:
    """
    Download given files using the lsm-get command.

    :param files: list of `FileSpec` objects (list)
    :param kwargs: kwargs dictionary (dict)
    :return: updated files (list)
    :raises: PilotException in case of controlled error.
    """
    copytools = kwargs.get('copytools') or []
    copysetup = get_copysetup(copytools, 'lsm')
    trace_report = kwargs.get('trace_report')
    #allow_direct_access = kwargs.get('allow_direct_access')

    # note, env vars might be unknown inside middleware contrainers, if so get the value already in the trace report
    localsite = os.environ.get('RUCIO_LOCAL_SITE_ID', trace_report.get_value('localSite'))

    for fspec in files:
        # update the trace report
        localsite = localsite if localsite else fspec.ddmendpoint
        trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
        trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset)

        # continue loop for files that are to be accessed directly  ## TO BE DEPRECATED (anisyonk)
        #if fspec.is_directaccess(ensure_replica=False) and allow_direct_access and fspec.accessmode == 'direct':
        #    fspec.status_code = 0
        #    fspec.status = 'remote_io'
        #    trace_report.update(url=fspec.turl, clientState='FOUND_ROOT', stateReason='direct_access')
        #    trace_report.send()
        #    continue

        trace_report.update(catStart=time())

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        #timeout = get_timeout(fspec.filesize)
        source = fspec.turl
        destination = os.path.join(dst, fspec.lfn)

        logger.info(f"transferring file {fspec.lfn} from {source} to {destination}")

        exit_code, stdout, stderr = move(source, destination, dst_in=True, copysetup=copysetup)

        if exit_code != 0:
            logger.warning(f"transfer failed: exit code = {exit_code}, stdout = {stdout}, stderr = {stderr}")

            error = resolve_common_transfer_errors(stderr, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            trace_report.update(clientState=error.get('state') or 'STAGEIN_ATTEMPT_FAILED',
                                stateReason=error.get('error'), timeEnd=time())
            trace_report.send()
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        # verify checksum; compare local checksum with catalog value (fspec.checksum), use same checksum type
        state, diagnostics = verify_catalog_checksum(fspec, destination)
        if diagnostics != "":
            trace_report.update(clientState=state or 'STAGEIN_ATTEMPT_FAILED', stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=fspec.status_code, state=state)

        fspec.status_code = 0
        fspec.status = 'transferred'
        trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
        trace_report.send()

    # for testing kill signals
    #import signal
    #os.kill(os.getpid(), signal.SIGSEGV)

    return files


def copy_out(files: list, **kwargs: dict) -> list:
    """
    Upload given files using lsm copytool.

    :param files: list of `FileSpec` objects (list)
    :param kwargs: kwargs dictionary (dict)
    :return: updated files (list)
    :raises: PilotException in case of controlled error.
    """
    copytools = kwargs.get('copytools') or []
    copysetup = get_copysetup(copytools, 'lsm')
    trace_report = kwargs.get('trace_report')
    ddmconf = kwargs.get('ddmconf', None)
    if not ddmconf:
        raise PilotException("copy_out() failed to resolve ddmconf from function arguments",
                             code=ErrorCodes.STAGEOUTFAILED,
                             state='COPY_ERROR')

    for fspec in files:
        trace_report.update(scope=fspec.scope, dataset=fspec.dataset, url=fspec.surl, filesize=fspec.filesize)
        trace_report.update(catStart=time(), filename=fspec.lfn, guid=fspec.guid.replace('-', ''))

        # resolve token value from fspec.ddmendpoint
        ddm = ddmconf.get(fspec.ddmendpoint)
        token = ddm.token
        if not token:
            diagnostics = f"copy_out() failed to resolve token value for ddmendpoint={fspec.ddmendpoint}"
            trace_report.update(clientState='STAGEOUT_ATTEMPT_FAILED',
                                stateReason=diagnostics,
                                timeEnd=time())
            trace_report.send()
            raise PilotException(diagnostics, code=ErrorCodes.STAGEOUTFAILED, state='COPY_ERROR')

        src = fspec.workdir or kwargs.get('workdir') or '.'
        #timeout = get_timeout(fspec.filesize)
        source = os.path.join(src, fspec.lfn)
        destination = fspec.turl

        # checksum has been calculated in the previous step - transfer_files() in api/data
        # note: pilot is handing over checksum to the command - which will/should verify it after the transfer
        checksum = f"adler32:{fspec.checksum.get('adler32')}"

        # define the command options
        _opts = {'--size': fspec.filesize,
                 '-t': token,
                 '--checksum': checksum,
                 '--guid': fspec.guid}
        opts = " ".join([f"{k} {v}" for (k, v) in list(_opts.items())])

        logger.info(f"transferring file {fspec.lfn} from {source} to {destination}")

        nretries = 1  # input parameter to function?
        for _ in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=False, copysetup=copysetup, options=opts)

            if exit_code != 0:
                if stderr == "":
                    stderr = stdout
                error = resolve_common_transfer_errors(stderr, is_stagein=False)
                fspec.status = 'failed'
                fspec.status_code = error.get('exit_code')
                trace_report.update(clientState=error.get('state', None) or 'STAGEOUT_ATTEMPT_FAILED',
                                    stateReason=error.get('error', 'unknown error'),
                                    timeEnd=time())
                trace_report.send()
                raise PilotException(error.get('error'), code=error.get('exit_code'), state=error.get('state'))

            logger.info('all successful')
            break

        fspec.status_code = 0
        fspec.status = 'transferred'
        trace_report.update(clientState='DONE', stateReason='OK', timeEnd=time())
        trace_report.send()

    return files


def move_all_files_in(files: list, nretries: int = 1) -> (int, str, str):
    """
    Move all inout files.

    :param files: list of FileSpec objects (list)
    :param nretries: number of retries; sometimes there can be a timeout copying, but the next attempt may succeed (int)
    :return: exit code (int), stdout (str), stderr (str).
    """
    exit_code = 0
    stdout = ""
    stderr = ""

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info(f"transferring file {entry['name']} from {entry['source']} to {entry['destination']}")

        source = entry['source'] + '/' + entry['name']
        destination = os.path.join(entry['destination'], entry['name'])
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=True)

            if exit_code != 0:
                if ((exit_code not in (errno.ETIMEDOUT, errno.ETIME)) or ((retry + 1) == nretries)):
                    logger.warning(f"transfer failed: exit code = {exit_code}, stdout = {stdout}, stderr = {stderr}")
                    return exit_code, stdout, stderr
            else:  # all successful
                break

    return exit_code, stdout, stderr


def move_all_files_out(files: list, nretries: int = 1) -> (int, str, str):
    """
    Move all output files.

    :param files: list of FileSPec objects (list)
    :param nretries: number of retries; sometimes there can be a timeout copying, but the next attempt may succeed (int)
    :return: exit code (int), stdout (str), stderr (str).
    """
    exit_code = 0
    stdout = ""
    stderr = ""

    for entry in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}
        logger.info(f"transferring file {entry['name']} from {entry['source']} to {entry['destination']}")

        destination = entry['destination'] + '/' + entry['name']
        source = os.path.join(entry['source'], entry['name'])
        for retry in range(nretries):
            exit_code, stdout, stderr = move(source, destination, dst_in=False)

            if exit_code != 0:
                if ((exit_code not in (errno.ETIMEDOUT, errno.ETIME)) or ((retry + 1) == nretries)):
                    logger.warning(f"transfer failed: exit code = {exit_code}, stdout = {stdout}, stderr = {stderr}")
                    return exit_code, stdout, stderr
            else:  # all successful
                break

    return exit_code, stdout, stderr


#@timeout(seconds=10800)
def move(source: str, destination: str, dst_in: bool = True, copysetup: str = "", options: str = "") -> (int, str, str):
    """
    Use lsm-get or lsm-put to transfer the file.

    :param source: path to source (str)
    :param destination: path to destination (str)
    :param dst_in: True for stage-in, False for stage-out (bool)
    :param copysetup: path to copysetup (str)
    :param options: additional options (str)
    :return: exit code (int), stdout (str), stderr (str).
    """
    # copysetup = '/osg/mwt2/app/atlas_app/atlaswn/setup.sh'
    if copysetup != "":
        cmd = f'source {copysetup};'
    else:
        cmd = ''

    args = f"{source} {destination}"
    if options:
        args = f"{options} {args}"

    if dst_in:
        cmd += f"lsm-get {args}"
    else:
        cmd += f"lsm-put {args}"

    try:
        exit_code, stdout, stderr = execute(cmd, usecontainer=False, copytool=True)  #, timeout=get_timeout(fspec.filesize))
    except Exception as error:
        if dst_in:
            exit_code = ErrorCodes.STAGEINFAILED
        else:
            exit_code = ErrorCodes.STAGEOUTFAILED
        stdout = f'exception caught: {error}'
        stderr = ''
        logger.warning(stdout)

    logger.info(f'exit_code={exit_code}, stdout={stdout}, stderr={stderr}')
    return exit_code, stdout, stderr


def check_for_lsm(dst_in: bool = True) -> bool:
    """
    Check if lsm-get / lsm-put are locally available.

    :param dst_in: True for stage-in, False for stage-out (bool)
    :return: True if command is available (bool).
    """
    if dst_in:
        cmd = 'which lsm-get'
    else:
        cmd = 'which lsm-put'
    exit_code, _, _ = execute(cmd)
    return exit_code == 0
