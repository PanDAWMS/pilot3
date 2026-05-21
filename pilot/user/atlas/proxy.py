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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-25
# - Alexander Bogdanchikov, alexander.bogdanchikov@cern.ch, 2020

"""Functions related to proxy handling for ATLAS."""

from __future__ import annotations
import logging
import os
import re

from time import time
from typing import (
    Any,
    Optional,
)

# from pilot.user.atlas.setup import get_file_system_root_path
from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.util.container import (
    execute,
    execute_nothreads
)
from pilot.util.proxy import get_proxy

errors = ErrorCodes()
logger = logging.getLogger(__name__)
pilot_cache = get_pilot_cache()


def get_voms_role(role: str = 'production') -> str:
    """Return the proper voms role.

    Args:
        role: proxy role, 'production' or 'user'.

    Returns:
        str: voms role.
    """
    return 'atlas:/atlas/Role=production' if role == 'production' else 'atlas'


def get_and_verify_proxy(x509: str, voms_role: str = '', proxy_type: str = '', workdir: str = '') -> tuple[int, str, str]:
    """Download a payload proxy from the server and verify it.

    Args:
        x509: X509_USER_PROXY.
        voms_role: role, e.g. 'atlas' for user jobs in unified dispatch, 'atlas:/atlas/Role=production' for production jobs.
        proxy_type: proxy type ('unified' on unified dispatch queues, otherwise blank).
        workdir: payload work directory.

    Returns:
        tuple[int, str, str]: exit code, diagnostics, updated x509.
    """
    exit_code = 0
    diagnostics = ""

    x509_payload = re.sub('.proxy$', '', x509) + f'-{proxy_type}.proxy' if proxy_type else x509
    # remove the .proxy suffix if it is not present in the original x509
    if not x509.endswith('.proxy'):
        x509_payload = re.sub('.proxy$', '', x509_payload)

    # for unified proxies, store it in the workdir
    if proxy_type == 'unified':
        x509_payload = os.path.join(workdir, os.path.basename(x509_payload))

    # try to receive payload proxy and update x509
    logger.info(f"download proxy from server (type=\'{proxy_type}\', x509_payload={x509_payload})")
    res, x509_payload = get_proxy(x509_payload, voms_role)  # note that x509_payload might be updated
    logger.debug(f'get_proxy() returned {x509_payload}')
    if res:
        logger.debug("server returned proxy (verifying)")
        exit_code, diagnostics = verify_proxy(x509=x509_payload, proxy_id=None, test=False)
        # if all verifications fail, verify_proxy()  returns exit_code=0 and last failure in diagnostics
        if exit_code != 0 or (exit_code == 0 and diagnostics != ''):
            logger.warning(diagnostics)
            logger.info(f"proxy verification failed (proxy type=\'{proxy_type}\')")
        else:
            logger.info(f"proxy verified (proxy type=\'{proxy_type}\')")
            # is commented: no user proxy should be in the command the container will execute
            x509 = x509_payload
    else:
        logger.warning(f"failed to get proxy for role=\'{voms_role}\'")

    return exit_code, diagnostics, x509


def verify_proxy(limit: int = None, x509: bool = None, proxy_id: str = "pilot", test: bool = False, pilotstartup: bool = False) -> tuple[int, str]:
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
    if pilotstartup:
        limit = 72  # 3 days
    if limit is None:
        limit = 1

    # add setup for arcproxy if it exists
    if x509 is None:
        x509 = os.environ.get('X509_USER_PROXY', '')
    if x509 != '':
        envsetup = f'export X509_USER_PROXY={x509};'
    else:
        envsetup = ''

    return verify_arcproxy(envsetup, limit, proxy_id=proxy_id, test=test)  # exit_code, diagnostics


def verify_arcproxy(envsetup: str, limit: int, proxy_id: str = "pilot", test: bool = False) -> tuple[int, str]:  # noqa: C901
    """Verify the proxy using arcproxy.

    Args:
        envsetup: general setup string for proxy commands.
        limit: time limit in hours.
        proxy_id: proxy unique id name. The verification result will be cached for this id. If None the result will not be cached.
        test: free Boolean test parameter.

    Returns:
        tuple[int, str]: exit code, error diagnostics.
    """
    exit_code = 0
    diagnostics = ""
    proxies = ['cert', 'proxy']

    if test:
        return errors.VOMSPROXYABOUTTOEXPIRE, 'dummy test'

    if proxy_id is not None:
        if not hasattr(verify_arcproxy, "cache"):
            verify_arcproxy.cache = {}

        if proxy_id in verify_arcproxy.cache:  # if exists, then calculate result from current cache
            validity_end_cert = verify_arcproxy.cache[proxy_id][0]
            validity_end = verify_arcproxy.cache[proxy_id][1]
            if validity_end < 0:  # previous validity check failed, do not try to re-check
                exit_code = -1
                diagnostics = "arcproxy verification failed (cached result)"
            else:
                #
                validities = [validity_end_cert, validity_end]
                for proxyname, validity in list(zip(proxies, validities)):
                    exit_code, diagnostics = check_time_left(proxyname, validity, limit)
                    if exit_code == errors.VOMSPROXYABOUTTOEXPIRE:
                        # remove the proxy_id from the dictionary to trigger a new entry after a new proxy has been downloaded
                        del verify_arcproxy.cache[proxy_id]

            return exit_code, diagnostics

    # options and options' sequence are important for parsing, do not change it
    # -i validityEnd -i validityLeft: time left for the certificate
    # -i vomsACvalidityEnd -i vomsACvalidityLeft: time left for the proxy
    #   validityEnd - timestamp when proxy validity ends.
    #   validityLeft - duration of proxy validity left in seconds.
    #   vomsACvalidityEnd - timestamp when VOMS attribute validity ends.
    #   vomsACvalidityLeft - duration of VOMS attribute validity left in seconds.
    cmd = f"{envsetup}arcproxy -i validityEnd -i validityLeft -i vomsACvalidityEnd -i vomsACvalidityLeft"
    _exit_code, stdout, stderr = execute_nothreads(cmd, shell=True)  # , usecontainer=True, copytool=True)
    if stdout is not None:
        if 'command not found' in stdout:
            logger.warning(f"arcproxy is not available on this queue,"
                           f"this can lead to memory issues with voms-proxy-info on SL6: {stdout}")
            exit_code = -1
        else:
            exit_code, diagnostics, validity_end_cert, validity_end = interpret_proxy_info(_exit_code, stdout, stderr, limit)
            # validity_end = int(time()) + 71 * 3600  # 71 hours test

            if proxy_id and validity_end:  # setup cache if requested
                if exit_code == 0:
                    logger.info(f"caching the validity ends from arcproxy: cache[\'{proxy_id}\'] = [{validity_end_cert}, {validity_end}]")
                    verify_arcproxy.cache[proxy_id] = [validity_end_cert, validity_end]
                else:
                    logger.warning('cannot store validity ends from arcproxy in cache')
                    verify_arcproxy.cache[proxy_id] = [-1, -1]  # -1 in cache means any error in prev validation
            if exit_code == 0:
                endtimes = [validity_end_cert, validity_end] if not proxy_id else verify_arcproxy.cache[proxy_id]
                for proxyname, validity in list(zip(proxies, endtimes)):
                    exit_code, diagnostics = check_time_left(proxyname, validity, limit)
                    if exit_code == errors.VOMSPROXYABOUTTOEXPIRE:
                        # remove the proxy_id from the dictionary to trigger a new entry after a new proxy has been downloaded
                        if proxy_id:
                            del verify_arcproxy.cache[proxy_id]
                    if exit_code == errors.CERTIFICATEHASEXPIRED:
                        logger.debug('certificate has expired')
                        break
                    if exit_code == errors.PROXYTOOSHORT:
                        # logger.debug('proxy is too short - aborting')
                        break
            if exit_code == errors.ARCPROXYLIBFAILURE:
                logger.warning("currenly ignoring arcproxy library failure")
                exit_code = 0
                diagnostics = ""
    else:
        logger.warning('command execution failed')

    return exit_code, diagnostics


def check_time_left(proxyname: str, validity: int, limit: int) -> tuple[int, str]:
    """Check the time left for the proxy.

    Args:
        proxyname: cert or proxy.
        validity: validity time.
        limit: time limit in hours.

    Returns:
        tuple[int, str]: exit code, diagnostics.
    """
    exit_code = 0
    diagnostics = ''
    tnow = int(time() + 0.5)  # round to seconds
    seconds_left = validity - tnow

    # test bad proxy
    #if proxyname == 'proxy':
    #    seconds_left = 1000
    logger.info(f"cache: check {proxyname} validity: wanted={limit}h ({limit * 3600 - 20 * 60}s with grace) "
                f"left={float(seconds_left) / 3600:.2f}h (now={tnow} validity={validity} left={seconds_left}s)")

    # special case for limit=72h (3 days) for pilot startup
    if limit == 72 and seconds_left < limit * 3600 - 20 * 60:
        diagnostics = f'proxy is too short for pilot startup: {float(seconds_left) / 3600:.2f}h'
        logger.warning(diagnostics)
        exit_code = errors.PROXYTOOSHORT
    elif seconds_left < limit * 3600 - 20 * 60:
        diagnostics = f'cert/proxy is about to expire: {float(seconds_left) / 3600:.2f}h'
        logger.warning(diagnostics)
        exit_code = errors.CERTIFICATEHASEXPIRED if proxyname == 'cert' else errors.VOMSPROXYABOUTTOEXPIRE
    else:
        logger.info(f"{proxyname} validity time is verified")

    return exit_code, diagnostics


def verify_vomsproxy(envsetup: str, limit: int) -> tuple[int, str]:
    """Verify proxy using voms-proxy-info command.

    Args:
        envsetup: general setup string for proxy commands.
        limit: time limit in hours.

    Returns:
        tuple[int, str]: exit code, error diagnostics.
    """
    exit_code = 0
    diagnostics = ""

    if os.environ.get('X509_USER_PROXY', '') != '':
        cmd = f"{envsetup}voms-proxy-info -actimeleft --timeleft --file $X509_USER_PROXY"
        logger.info(f'executing command: {cmd}')
        _exit_code, stdout, stderr = execute_nothreads(cmd, shell=True)
        if stdout is not None:
            if "command not found" in stdout:
                logger.info("skipping voms proxy check since command is not available")
            else:
                exit_code, diagnostics, _, _ = interpret_proxy_info(_exit_code, stdout, stderr, limit)
                if exit_code == 0:
                    logger.info("voms proxy verified using voms-proxy-info")
                    return 0, diagnostics
        else:
            logger.warning('command execution failed')
    else:
        logger.warning('X509_USER_PROXY is not set')

    return exit_code, diagnostics


def verify_gridproxy(envsetup: str, limit: int) -> tuple[int, str]:
    """Verify proxy using grid-proxy-info command.

    Args:
        envsetup: general setup string for proxy commands.
        limit: time limit in hours.

    Returns:
        tuple[int, str]: exit code, error diagnostics.
    """
    ec = 0
    diagnostics = ""

    if limit:
        # next clause had problems: grid-proxy-info -exists -valid 0.166666666667:00
        # more accurate calculation of HH:MM
        limit_hours = int(limit * 60) / 60
        limit_minutes = int(limit * 60 + .999) - limit_hours * 60
        cmd = f"{envsetup}grid-proxy-info -exists -valid {limit_hours}:{limit_minutes:02}"
    else:
        cmd = f"{envsetup}grid-proxy-info -exists -valid 24:00"

    logger.info(f'executing command: {cmd}')
    exit_code, stdout, _ = execute(cmd, shell=True)
    if stdout is not None:
        if exit_code != 0:
            if stdout.find("command not found") > 0:
                logger.info("skipping grid proxy check since command is not available")
            else:
                # Analyze exit code / stdout
                diagnostics = f"grid proxy certificate does not exist or is too short: {exit_code}, {stdout}"
                logger.warning(diagnostics)
                return errors.NOPROXY, diagnostics
        else:
            logger.info("grid proxy verified")
    else:
        logger.warning('command execution failed')

    return ec, diagnostics


def interpret_proxy_info(proxy_ec: Any, stdout: str, stderr: str, limit: int) -> tuple[int, str, Optional[int], Optional[int]]:
    """Interpret the output from arcproxy.

    Args:
        proxy_ec: exit code from proxy command.
        stdout: stdout from proxy command.
        stderr: stderr from proxy command.
        limit: time limit in hours.

    Returns:
        tuple[int, str, Optional[int], Optional[int]]: exit code, diagnostics, validity end cert, validity end in seconds if detected or None if not detected.
    """
    exitcode = 0
    diagnostics = ""
    validity_end = None  # not detected
    validity_end_cert = None  # not detected

    logger.debug(f'stdout = {stdout}')
    logger.debug(f'stderr = {stderr}')

    if proxy_ec != 0:
        if "Unable to verify signature! Server certificate possibly not installed" in stdout:
            logger.warning(f"skipping voms proxy check: {stdout}")
        # test for command errors
        elif "arcproxy: error while loading shared libraries" in stderr:
            diagnostics = stderr
            logger.warning(diagnostics)
            exitcode = errors.ARCPROXYLIBFAILURE
        elif "arcproxy:" in stdout:
            diagnostics = f"arcproxy failed: {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.ARCPROXYFAILURE
        else:
            # Analyze exit code / output
            diagnostics = f"voms proxy certificate check failure: {proxy_ec}, {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.NOVOMSPROXY
    else:
        if "\n" in stdout:
            # try to extract the time left from the command output
            validity_end_cert, validity_end, stdout = extract_time_left(stdout)
            if validity_end:
                return exitcode, diagnostics, validity_end_cert, validity_end

            diagnostics = f"arcproxy failed: {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR

            return exitcode, diagnostics, validity_end_cert, validity_end

        # test for command errors
        if "arcproxy:" in stdout:
            diagnostics = f"arcproxy failed: {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR
        else:
            # on EMI-3 the time output is different (HH:MM:SS as compared to SS on EMI-2)
            if ":" in stdout:
                ftr = [3600, 60, 1]
                stdout = sum(a * b for a, b in zip(ftr, [int(x) for x in stdout.split(':')]))
            try:
                validity = int(stdout)
                if validity >= limit * 3600:
                    logger.info(f"voms proxy verified ({validity} s)")
                else:
                    diagnostics = f"voms proxy certificate does not exist or is too short (lifetime {validity} s)"
                    logger.warning(diagnostics)
                    exitcode = errors.NOVOMSPROXY
            except ValueError as exc:
                diagnostics = f"failed to evaluate command stdout: {stdout}, stderr: {stderr}, exc={exc}"
                logger.warning(diagnostics)
                exitcode = errors.GENERALERROR

    return exitcode, diagnostics, validity_end_cert, validity_end


def extract_time_left(stdout: str) -> tuple[Optional[int], Optional[int], str]:
    """Extract the time left for the cert and proxy from the proxy command.

    Some processing on the stdout is done.

    Args:
        stdout: stdout.

    Returns:
        tuple[Optional[int], Optional[int], str]: validity_end_cert, validity_end, stdout.
    """
    validity_end_cert = None
    validity_end = None

    # remove the last \n in case there is one
    if stdout[-1] == '\n':
        stdout = stdout[:-1]
    stdout_split = stdout.split('\n')
    # give up if there are not four entries
    if len(stdout_split) != 4:
        print(f'cannot extract validity_end from: {stdout}')
        return None, None, stdout

    try:
        validity_end_cert = int(stdout_split[-4])
        validity_end = int(stdout_split[-2])
    except (ValueError, TypeError):
        # try to get validity_end in penultimate line
        try:
            validity_end_cert = None  # unknown in this case
            validity_end_str = stdout_split[-1]  # may raise exception IndexError if stdout is too short
            logger.debug(f"try to get validity_end from the line: \"{validity_end_str}\"")
            validity_end = int(
                validity_end_str)  # may raise ValueError if not string
        except (IndexError, ValueError) as exc:
            logger.warning(f"validity_end not found in stdout: {exc}")

    if validity_end_cert:
        logger.info(f"validity_end_cert = {validity_end_cert}")
    if validity_end:
        logger.info(f"validity_end = {validity_end}")
        pilot_cache.proxy_lifetime = validity_end - int(time())  # remaining time in seconds

    return validity_end_cert, validity_end, stdout


def extract_time_left_old(stdout: str) -> tuple[Optional[int], str]:
    """Extract the time left from the proxy command.

    Some processing on the stdout is done.

    Args:
        stdout: stdout.

    Returns:
        tuple[Optional[int], str]: validity_end, stdout.
    """
    validity_end = None

    # remove the last \n in case there is one
    if stdout[-1] == '\n':
        stdout = stdout[:-1]
    stdout_split = stdout.split('\n')
    try:
        validity_end = int(stdout_split[-2])
    except (ValueError, TypeError):
        # try to get validity_end in penultimate line
        try:
            validity_end_str = stdout_split[-1]  # may raise exception IndexError if stdout is too short
            logger.debug(f"try to get validity_end from the line: \"{validity_end_str}\"")
            validity_end = int(validity_end_str)  # may raise ValueError if not string
        except (IndexError, ValueError) as exc:
            logger.info(f"validity_end not found in stdout: {exc}")
        #validity_end = None

    if validity_end:
        logger.info(f"validity_end = {validity_end}")

    return validity_end, stdout


def getproxy_dictionary(voms_role: str) -> dict:
    """Prepare the dictionary with the VOMS role and DN for the getProxy call.

    Args:
        voms_role: VOMS role.

    Returns:
        dict: getProxy dictionary.
    """
    return {'role': voms_role, 'dn': 'atlpilo2'} if voms_role == 'atlas' else {'role': voms_role}
