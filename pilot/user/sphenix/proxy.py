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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-24

"""Functions related to proxy handling for sPHENIX."""

# from pilot.util.container import execute

import logging

logger = logging.getLogger(__name__)


def verify_proxy(limit: int = None, x509: bool = None, proxy_id: str = "pilot", test: bool = False, pilotstartup: bool = False) -> (int, str):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int)
    :param x509: points to the proxy file. If not set (=None) - get proxy file from X509_USER_PROXY environment (bool)
    :param proxy_id: proxy id (str)
    :param test: free Boolean test parameter (bool)
    :param pilotstartup: free Boolean pilotstartup parameter (bool)
    :return: exit code (NOPROXY or NOVOMSPROXY) (int), diagnostics (error diagnostics string) (str).
    """
    if limit or x509 or proxy_id or test:  # to bypass pylint score 0
        pass

    return 0, ""


def get_voms_role(role: str = 'production') -> str:
    """
    Return the proper voms role.

    :param role: proxy role, 'production' or 'user' (str).
    :return: voms role (str).
    """
    if role:  # to bypass pylint score 0
        pass

    return ''


def get_and_verify_proxy(x509: str, voms_role: str = '', proxy_type: str = '', workdir: str = '') -> (int, str, str):
    """
    Download a payload proxy from the server and verify it.

    :param x509: X509_USER_PROXY (str)
    :param voms_role: role, e.g. 'sphenix' (str)
    :param proxy_type: proxy type ('payload' for user payload proxy, blank for prod/user proxy) (str)
    :param workdir: payload work directory (str)
    :return:  exit code (int), diagnostics (str), updated X509_USER_PROXY (str).
    """
    if voms_role or proxy_type or workdir:  # to bypass pylint score 0
        pass

    exit_code = 0
    diagnostics = ""

    return exit_code, diagnostics, x509


def getproxy_dictionary(voms_role: str) -> dict:
    """
    Prepare the dictionary with the VOMS role for the getProxy call.

    :param voms_role: VOMS role (str)
    :return: getProxy dictionary (dict).
    """
    return {'role': voms_role}
