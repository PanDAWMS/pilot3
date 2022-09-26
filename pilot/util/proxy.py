#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2022

import logging
import os
import traceback

from pilot.common.exception import FileHandlingFailure
from pilot.util import https
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import write_file

logger = logging.getLogger(__name__)


def get_distinguished_name():
    """
    Get the user DN.
    Note: the DN is also sent by the server to the pilot in the job description (produserid).

    :return: User DN (string).
    """

    dn = ""
    executable = 'arcproxy -i subject'
    exit_code, stdout, stderr = execute(executable)
    if exit_code != 0 or "ERROR:" in stderr:
        logger.warning("arcproxy failed: ec=%d, stdout=%s, stderr=%s" % (exit_code, stdout, stderr))

        if "command not found" in stderr or "Can not find certificate file" in stderr:
            logger.warning("arcproxy experienced a problem (will try voms-proxy-info instead)")

            # Default to voms-proxy-info
            executable = 'voms-proxy-info -subject'
            exit_code, stdout, stderr = execute(executable)

    if exit_code == 0:
        dn = stdout
        logger.info('DN = %s' % dn)
        cn = "/CN=proxy"
        if not dn.endswith(cn):
            logger.info("DN does not end with %s (will be added)" % cn)
            dn += cn

    else:
        logger.warning("user=self set but cannot get proxy: %d, %s" % (exit_code, stdout))

    return dn


def get_proxy(proxy_outfile_name, voms_role):
    """
    Download and store a proxy.
    E.g. on read-only file systems (K8), the default path is not available, in which case the new proxy
    will be stored in the workdir (return the updated path).

    :param proxy_outfile_name: specify the file to store proxy (string).
    :param voms_role: what proxy (role) to request, e.g. 'atlas' (string).
    :return: result (Boolean), updated proxy path (string).
    """
    try:
        # it assumes that https_setup() was done already
        url = os.environ.get('PANDA_SERVER_URL', config.Pilot.pandaserver)
        res = https.request('{pandaserver}/server/panda/getProxy'.format(pandaserver=url), data={'role': voms_role})

        if res is None:
            logger.error(f"unable to get proxy with role '{voms_role}' from panda server")
            return False, proxy_outfile_name

        if res['StatusCode'] != 0:
            logger.error(f"panda server returned: \'{res['errorDialog']}\' for proxy role \'{voms_role}\'")
            return False, proxy_outfile_name

        proxy_contents = res['userProxy']

    except Exception as exc:
        logger.error(f"Get proxy from panda server failed: {exc}, {traceback.format_exc()}")
        return False, proxy_outfile_name

    def create_file(filename, contents):
        """
        Internally used helper function to create proxy file.
        """
        _file = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.close(_file)
        return write_file(filename, contents, mute=False)  # returns True on success

    result = False
    try:
        # pre-create empty proxy file with secure permissions. Prepare it for write_file() which can not
        # set file permission mode, it will writes to the existing file with correct permissions.
        result = create_file(proxy_outfile_name, proxy_contents)
    except (IOError, OSError, FileHandlingFailure) as exc:
        logger.error(f"exception caught:\n{exc},\ntraceback: {traceback.format_exc()}")
        if 'Read-only file system' in exc:
            proxy_outfile_name = os.path.join(os.getenv('PILOT_HOME'), os.path.basename(proxy_outfile_name))  # e.g. '/path/x509up_u25606_prod-unified.proxy'
            logger.info(f'attempting writing proxy to alternative path: {proxy_outfile_name}')
            try:  # can we bypass a problem with read-only file systems by writing the proxy to the pilot home dir instead?
                result = create_file(proxy_outfile_name, proxy_contents)
            except (IOError, OSError, FileHandlingFailure) as exc:
                logger.error(f"exception caught:\n{exc},\ntraceback: {traceback.format_exc()}")
            else:
                logger.debug('updating X509_USER_PROXY to alternative path {path} (valid until end of current job)')
                os.environ['X509_USER_PROXY'] = proxy_outfile_name

    return result, proxy_outfile_name


def create_cert_files(from_proxy, workdir):
    """
    Create cert/key pem files from given proxy and store in workdir.
    These files are needed for communicating with logstash server.

    :param from_proxy: path to proxy file (string).
    :param workdir: work directory (string).
    :return: path to crt.pem (string), path to key.pem (string).
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
        else:
            logger.debug(f'produced key/cert file: {_files[counter]}')
            counter += 1

    return _files[0], _files[1]
