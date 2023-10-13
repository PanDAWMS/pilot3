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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

# from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)


def verify_proxy(limit=None, x509=None, proxy_id="pilot", test=False):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int).
    :param test: free Boolean test parameter.
    :return: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """

    return 0, ""


def get_voms_role(role='production'):
    """
    Return the proper voms role.

    :param role: proxy role, 'production' or 'user' (string).
    :return: voms role (string).
    """

    return ''


def get_and_verify_proxy(x509, voms_role='', proxy_type='', workdir=''):
    """
    Download a payload proxy from the server and verify it.

    :param x509: X509_USER_PROXY (string).
    :param voms_role: role, e.g. 'rubin' (string).
    :param proxy_type: proxy type ('payload' for user payload proxy, blank for prod/user proxy) (string).
    :param workdir: payload work directory (string).
    :return:  exit code (int), diagnostics (string), updated X509_USER_PROXY (string).
    """

    exit_code = 0
    diagnostics = ""

    return exit_code, diagnostics, x509


def getproxy_dictionary(voms_role):
    """
    Prepare the dictionary for the getProxy call.

    :param voms_role: VOMS role (string).
    """

    return {'role': voms_role}
