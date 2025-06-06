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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-24

import logging
import os
import traceback

from pilot.common.exception import FileHandlingFailure
from pilot.util import https
from pilot.util.config import config
from pilot.util.container import (
    execute,
    execute_nothreads
)
from pilot.util.filehandling import write_file

logger = logging.getLogger(__name__)


def get_distinguished_name() -> str:
    """
    Get the user DN.

    Note: the DN is also sent by the server to the pilot in the job description (produserid).

    :return: User DN (str).
    """
    dn = ""
    executable = 'arcproxy -i subject'
    exit_code, stdout, stderr = execute(executable)
    if exit_code != 0 or "ERROR:" in stderr:
        logger.warning(f"arcproxy failed: ec={exit_code}, stdout={stdout}, stderr={stderr}")

        if "command not found" in stderr or "Can not find certificate file" in stderr:
            logger.warning("arcproxy experienced a problem (will try voms-proxy-info instead)")

            # Default to voms-proxy-info
            exit_code, stdout, _ = vomsproxyinfo(options='-subject', mute=True)

    if exit_code == 0:
        dn = stdout
        logger.info(f'DN = {dn}')
        cn = "/CN=proxy"
        if not dn.endswith(cn):
            logger.info(f"DN does not end with {cn} (will be added)")
            dn += cn

    else:
        logger.warning(f"user=self set but cannot get proxy: {exit_code}, {stdout}")

    return dn


def vomsproxyinfo(options: str = '-all', mute: bool = False, path: str = '') -> tuple[int, str, str]:
    """
    Execute voms-proxy-info with the given options.

    :param options: command options (str)
    :param mute: should command output be printed (mute=False) or not (mute=True) (bool)
    :param path: use given path if specified for proxy (str)
    :return: exit code (int), stdout (string), stderr (str) (tuple).
    """
    executable = f'voms-proxy-info {options}'
    if path:
        executable += f' --file={path}'
    exit_code, stdout, stderr = execute_nothreads(executable)
    if not mute:
        logger.info(stdout + stderr)

    return exit_code, stdout, stderr


def get_proxy(proxy_outfile_name: str, voms_role: str) -> tuple[bool, str]:
    """
    Download and store a proxy.

    E.g. on read-only file systems (K8), the default path is not available, in which case the new proxy
    will be stored in the workdir (return the updated path).

    :param proxy_outfile_name: specify the file to store proxy (str)
    :param voms_role: what proxy (role) to request, e.g. 'atlas' (str)
    :return: result (Boolean), updated proxy path (str) (tuple).
    """
    try:
        # it assumes that https_setup() was done already
        url = os.environ.get('PANDA_SERVER_URL', config.Pilot.pandaserver)

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.proxy', globals(), locals(), [pilot_user], 0)
        data = user.getproxy_dictionary(voms_role)

        res = https.request2(f'{url}/server/panda/getProxy', data=data)
        if res is None:
            logger.error(f"unable to get proxy with role '{voms_role}' from panda server using urllib method")
            res = https.request('{url}/server/panda/getProxy', data=data)
            if res is None:
                logger.error(f"unable to get proxy with role '{voms_role}' from panda server using curl method")
                return False, proxy_outfile_name

        if isinstance(res, str):
            logger.error(f"panda server returned a string instead of a dictionary: {res}")
            return False, proxy_outfile_name

        if res['StatusCode'] != 0:
            logger.error(f"panda server returned: \'{res['errorDialog']}\' for proxy role \'{voms_role}\'")
            return False, proxy_outfile_name

        proxy_contents = res['userProxy']

    except Exception as exc:
        logger.error(f"Get proxy from panda server failed: {exc}, {traceback.format_exc()}")
        return False, proxy_outfile_name

    def create_file(filename: str, contents: str) -> bool:
        """Create a file with the given contents."""
        _file = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.close(_file)

        return write_file(filename, contents, mute=False)  # returns True on success

    result = False
    try:
        # pre-create empty proxy file with secure permissions. Prepare it for write_file() which can not
        # set file permission mode, it will write to the existing file with correct permissions.
        result = create_file(proxy_outfile_name, proxy_contents)
    except (OSError, FileHandlingFailure) as exc:
        logger.error(f"exception caught:\n{exc},\ntraceback: {traceback.format_exc()}")
        if 'Read-only file system' in exc:
            proxy_outfile_name = os.path.join(os.getenv('PILOT_HOME'), os.path.basename(proxy_outfile_name))  # e.g. '/path/x509up_u25606_prod-unified.proxy'
            logger.info(f'attempting writing proxy to alternative path: {proxy_outfile_name}')
            try:  # can we bypass a problem with read-only file systems by writing the proxy to the pilot home dir instead?
                result = create_file(proxy_outfile_name, proxy_contents)
            except (OSError, FileHandlingFailure) as e:
                logger.error(f"exception caught:\n{e},\ntraceback: {traceback.format_exc()}")
            else:
                logger.debug('updating X509_USER_PROXY to alternative path {path} (valid until end of current job)')
                os.environ['X509_USER_PROXY'] = proxy_outfile_name
    else:
        # dump voms-proxy-info -all to log
        _, _, _ = vomsproxyinfo(options='-all', path=proxy_outfile_name)

    return result, proxy_outfile_name


def create_cert_files(from_proxy: str, workdir: str) -> tuple[str, str]:
    """
    Create cert/key pem files from given proxy and store in workdir.

    These files are needed for communicating with logstash server.

    :param from_proxy: path to proxy file (str)
    :param workdir: work directory (str)
    :return: path to crt.pem (string), path to key.pem (string) (tuple).
    """
    _files = [os.path.join(workdir, 'crt.pem'), os.path.join(workdir, 'key.pem')]
    if os.path.exists(_files[0]) and os.path.exists(_files[1]):
        return _files[0], _files[1]

    cmds = [f'openssl pkcs12 -in {from_proxy} -out {_files[0]} -clcerts -nokeys',
            f'openssl pkcs12 -in {from_proxy} -out {_files[1]} -nocerts -nodes']

    counter = 0
    for cmd in cmds:
        ec, stdout, stderr = execute(cmd)
        if ec:
            logger.warning(f'cert command failed: {stdout}, {stderr}')
            return '', ''

        logger.debug(f'produced key/cert file: {_files[counter]}')
        counter += 1

    return _files[0], _files[1]
