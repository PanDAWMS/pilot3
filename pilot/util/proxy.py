#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2022

from pilot.util.container import execute

import os
import logging
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


def get_proxy(server, path):
    """
    Download proxy from given server and store it in given path.

    :param server: server URL (string).
    :return:
    """

    pass


def create_cert_files(from_proxy, workdir):
    """
    Create cert/key pem files from given proxy and store in workdir.
    These files are needed for communicating with logstash server.

    :param from_proxy: path to proxy file (string).
    :param workdir: work directory (string).
    :return: path to crt.pem (string), path to key.pem (string).
    """

#    return os.path.join(workdir, os.environ.get("X509_USER_PROXY")),\
#           os.path.join(workdir, os.environ.get("X509_USER_PROXY"))

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
