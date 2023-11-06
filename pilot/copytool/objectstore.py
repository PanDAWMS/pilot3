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
# - Wen Guan, wen.guan@cern.ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-2023

import os
import json
import logging

from pilot.util.container import execute
from pilot.common.exception import (
    PilotException,
    ErrorCodes,
)
from pilot.util.ruciopath import get_rucio_path
from pilot.util.config import config
from .common import resolve_common_transfer_errors

logger = logging.getLogger(__name__)

# can be disabled for Rucio if allowed to use all RSE for input
require_replicas = False    ## indicates if given copytool requires input replicas to be resolved

require_input_protocols = True    ## indicates if given copytool requires input protocols and manual generation of input replicas
require_protocols = True  ## indicates if given copytool requires protocols to be resolved first for stage-out

allowed_schemas = ['srm', 'gsiftp', 'https', 'davs', 'root', 's3', 's3+rucio']


def is_valid_for_copy_in(files: list) -> bool:
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files: list) -> bool:
    return True  ## FIX ME LATER


def resolve_surl(fspec, protocol, ddmconf, **kwargs):
    """
        Get final destination SURL for file to be transferred to Objectstore
        Can be customized at the level of specific copytool

        :param protocol: suggested protocol
        :param ddmconf: full ddm storage data
        :param fspec: file spec data
        :return: dictionary {'surl': surl}
    """
    ddm = ddmconf.get(fspec.ddmendpoint)
    if not ddm:
        raise PilotException(f'failed to resolve ddmendpoint by name={fspec.ddmendpoint}')

    if ddm.is_deterministic:
        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), get_rucio_path(fspec.scope, fspec.lfn))
    elif ddm.type in ['OS_ES', 'OS_LOGS']:
        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), fspec.lfn)
        fspec.protocol_id = protocol.get('id')
    else:
        raise PilotException(f'resolve_surl(): failed to construct SURL for non deterministic ddm={fspec.ddmendpoint}: NOT IMPLEMENTED')

    return {'surl': surl}


def copy_in(files, **kwargs):
    """
        Download given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    ddmconf = kwargs.pop('ddmconf', {})

    for fspec in files:

        cmd = []
        logger.info("To transfer file: %s", fspec)
        if fspec.protocol_id:
            ddm = ddmconf.get(fspec.ddmendpoint)
            if ddm:
                ddm_special_setup = ddm.get_special_setup(fspec.protocol_id)
                if ddm_special_setup:
                    cmd = [ddm_special_setup]

        # temporary hack
        rses_option = '--rses' if is_new_rucio_version() else '--rse'

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        cmd += ['/usr/bin/env', 'rucio', '-v', 'download', '--no-subdir', '--dir', dst]
        if require_replicas:
            cmd += [rses_option, fspec.replicas[0]['ddmendpoint']]

        # a copytool module should consider fspec.turl for transfers, and could failback to fspec.surl,
        # but normally fspec.turl (transfer url) is mandatory and already populated by the top workflow
        turl = fspec.turl or fspec.surl
        if turl:
            if fspec.ddmendpoint:
                cmd.extend([rses_option, fspec.ddmendpoint])
            cmd.extend(['--pfn', turl])
        cmd += [f'{fspec.scope}:{fspec.lfn}']

        rcode, _, stderr = execute(" ".join(cmd), **kwargs)

        if rcode:  ## error occurred
            error = resolve_common_transfer_errors(stderr, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def is_new_rucio_version() -> bool:
    """
    Check if --rses RSES option is supported in Rucio.

    :return: True if new rucio version (bool).
    """

    _, stdout, _ = execute('rucio download -h')
    return True if '--rses RSES' in stdout else False


def copy_out(files, **kwargs):
    """
        Upload given files using rucio copytool.

        :param files: list of `FileSpec` objects
        :raise: PilotException in case of controlled error
    """

    # don't spoil the output, we depend on stderr parsing
    os.environ['RUCIO_LOGGING_FORMAT'] = '%(asctime)s %(levelname)s [%(message)s]'

    no_register = kwargs.pop('no_register', True)
    summary = kwargs.pop('summary', False)
    ddmconf = kwargs.pop('ddmconf', {})
    # trace_report = kwargs.get('trace_report')

    for fspec in files:
        cmd = []
        if fspec.protocol_id:
            ddm = ddmconf.get(fspec.ddmendpoint)
            if ddm:
                ddm_special_setup = ddm.get_special_setup(fspec.protocol_id)
                if ddm_special_setup:
                    cmd = [ddm_special_setup]

        cmd += ['/usr/bin/env', 'rucio', '-v', 'upload']
        cmd += ['--rse', fspec.ddmendpoint]

        if fspec.scope:
            cmd.extend(['--scope', fspec.scope])
        if fspec.guid:
            cmd.extend(['--guid', fspec.guid])

        if no_register:
            cmd.append('--no-register')

        if summary:
            cmd.append('--summary')

        if fspec.turl:
            cmd.extend(['--pfn', fspec.turl])

        cmd += [fspec.surl]

        rcode, _, stderr = execute(" ".join(cmd), **kwargs)

        if rcode:  ## error occurred
            error = resolve_common_transfer_errors(stderr, is_stagein=False)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        if summary:  # resolve final pfn (turl) from the summary JSON
            cwd = fspec.workdir or kwargs.get('workdir') or '.'
            path = os.path.join(cwd, 'rucio_upload.json')
            if not os.path.exists(path):
                logger.error(f'failed to resolve Rucio summary JSON, wrong path? file={path}')
            else:
                with open(path, 'rb') as f:
                    summary = json.load(f)
                    dat = summary.get(f"{fspec.scope}:{fspec.lfn}") or {}
                    fspec.turl = dat.get('pfn')
                    # quick transfer verification:
                    # the logic should be unified and moved to base layer shared for all the movers
                    checksum = dat.get(config.File.checksum_type)
                    logger.debug(f'extracted checksum: {checksum}')
                    if fspec.checksum.get(config.File.checksum_type) and checksum and fspec.checksum.get(config.File.checksum_type) != checksum:
                        state = 'AD_MISMATCH' if config.File.checksum_type == 'adler32' else 'MD_MISMATCH'
                        code = ErrorCodes.PUTADMISMATCH if config.File.checksum_type == 'adler32' else ErrorCodes.PUTMD5MISMATCH
                        raise PilotException("failed to stageout: CRC mismatched", code=code, state=state)

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files
