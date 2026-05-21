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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025

"""Functions related to proxy handling for epic user."""

# from pilot.util.container import execute

from __future__ import annotations
import logging
logger = logging.getLogger(__name__)


def verify_proxy(limit: int = None, x509: str = None, proxy_id: str = "pilot", test: bool = False, pilotstartup: bool = False) -> tuple[int, str]:
    """Check for a valid voms/grid proxy longer than N hours.

    Use `limit` to set required time limit.

    Args:
        limit: time limit in hours.
        x509: points to the proxy file. If not set (=None) - get proxy file from X509_USER_PROXY environment.
        proxy_id: proxy id.
        test: free Boolean test parameter.
        pilotstartup: free Boolean pilotstartup parameter.

    Returns:
        tuple[int, str]: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """
    if limit or x509 or proxy_id or test:  # to bypass pylint score 0
        pass

    return 0, ""


def get_voms_role(role: str = 'production') -> str:
    """Return the proper voms role.

    Args:
        role: proxy role, 'production' or 'user'.

    Returns:
        str: voms role.
    """
    if role:  # to bypass pylint score 0
        pass

    return ''


def get_and_verify_proxy(x509: str, voms_role: str = '', proxy_type: str = '', workdir: str = '') -> tuple[int, str, str]:
    """Download a payload proxy from the server and verify it.

    Args:
        x509: X509_USER_PROXY.
        voms_role: role, e.g. 'atlas'.
        proxy_type: proxy type ('payload' for user payload proxy, blank for prod/user proxy).
        workdir: payload work directory.

    Returns:
        tuple[int, str, str]: exit code, diagnostics, updated X509_USER_PROXY.
    """
    if voms_role or proxy_type or workdir:  # to bypass pylint score 0
        pass

    exit_code = 0
    diagnostics = ""

    return exit_code, diagnostics, x509


def getproxy_dictionary(voms_role: str) -> dict:
    """Prepare the dictionary for the getProxy call.

    Args:
        voms_role: VOMS role.

    Returns:
        dict: getProxy dictionary.
    """
    return {'role': voms_role}
