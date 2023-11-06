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
# - Alexander Bogdanchikov, alexander.bogdanchikov@cern.ch, 2020

import os
import logging
import re
from time import time

# from pilot.user.atlas.setup import get_file_system_root_path
from pilot.util.container import execute
from pilot.common.errorcodes import ErrorCodes
from pilot.util.proxy import get_proxy

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def get_voms_role(role='production'):
    """
    Return the proper voms role.

    :param role: proxy role, 'production' or 'user' (string).
    :return: voms role (string).
    """

    return 'atlas:/atlas/Role=production' if role == 'production' else 'atlas'


def get_and_verify_proxy(x509, voms_role='', proxy_type='', workdir=''):
    """
    Download a payload proxy from the server and verify it.

    :param x509: X509_USER_PROXY (string).
    :param voms_role: role, e.g. 'atlas' for user jobs in unified dispatch, 'atlas:/atlas/Role=production' for production jobs (string).
    :param proxy_type: proxy type ('unified' on unified dispatch queues, otherwise blank) (string).
    :param workdir: payload work directory (string).
    :return:  exit code (int), diagnostics (string), updated x509 (string).
    """

    exit_code = 0
    diagnostics = ""

    x509_payload = re.sub('.proxy$', '', x509) + f'-{proxy_type}.proxy' if proxy_type else x509
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
            # cmd = cmd.replace("export X509_USER_PROXY=%s;" % x509, "export X509_USER_PROXY=%s;" % x509_payload)
            x509 = x509_payload
    else:
        logger.warning(f"failed to get proxy for role=\'{voms_role}\'")

    return exit_code, diagnostics, x509


def verify_proxy(limit=None, x509=None, proxy_id="pilot", test=False):
    """
    Check for a valid voms/grid proxy longer than N hours.
    Use `limit` to set required time limit.

    :param limit: time limit in hours (int).
    :param x509: points to the proxy file. If not set (=None) - get proxy file from X509_USER_PROXY environment
    :param test: free Boolean test parameter.
    :return: exit code (NOPROXY or NOVOMSPROXY), diagnostics (error diagnostics string).
    """

    if limit is None:
        limit = 1

    # add setup for arcproxy if it exists
    #arcproxy_setup = "%s/atlas.cern.ch/repo/sw/arc/client/latest/slc6/x86_64/setup.sh" % get_file_system_root_path()
    if x509 is None:
        x509 = os.environ.get('X509_USER_PROXY', '')
    if x509 != '':
        envsetup = f'export X509_USER_PROXY={x509};'
    else:
        envsetup = ''

    # envsetup += ". %s/atlas.cern.ch/repo/ATLASLocalRootBase/user/atlasLocalSetup.sh --quiet;" % get_file_system_root_path()
    #if os.environ.get('ALRB_noGridMW', '').lower() != "yes":
    #    envsetup += "lsetup emi;"
    #else:
    #    logger.warning('Skipping "lsetup emi" as ALRB_noGridMW=YES')

    # first try to use arcproxy since voms-proxy-info is not working properly on SL6
    #  (memory issues on queues with limited memory)

    exit_code, diagnostics = verify_arcproxy(envsetup, limit, proxy_id=proxy_id, test=test)
    if exit_code != 0 and exit_code != -1:
        return exit_code, diagnostics
    elif exit_code == -1:
        pass  # go to next test
    else:
        return 0, diagnostics

    #exit_code, diagnostics = verify_vomsproxy(envsetup, limit)
    #if exit_code != 0:
    #    return exit_code, diagnostics
    #else:
    #    return 0, diagnostics

    return 0, diagnostics


def verify_arcproxy(envsetup, limit, proxy_id="pilot", test=False):  # noqa: C901
    """
    Verify the proxy using arcproxy.

    :param envsetup: general setup string for proxy commands (string).
    :param limit: time limit in hours (int).
    :param  proxy_id: proxy unique id name. The verification result will be cached for this id. If None the result will not be cached (string)
    :return: exit code (int), error diagnostics (string).
    """
    exit_code = 0
    diagnostics = ""
    proxies = ['cert', 'proxy']

    if test:
        return errors.VOMSPROXYABOUTTOEXPIRE, 'dummy test'
        #return errors.NOVOMSPROXY, 'dummy test'

    try:
        logger.debug(f'proxy_id={proxy_id}')
        logger.debug(f'verify_arcproxy.cache={verify_arcproxy.cache}')
        logger.debug(f'verify_arcproxy.cache[proxy_id]={verify_arcproxy.cache[proxy_id]}')
    except Exception as exc:
        logger.debug(f'exc={exc}')

    if proxy_id is not None:
        if not hasattr(verify_arcproxy, "cache"):
            verify_arcproxy.cache = {}

        if proxy_id in verify_arcproxy.cache:  # if exist, then calculate result from current cache
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
    cmd = f"{envsetup}arcproxy -i subject"
    _exit_code, stdout, stderr = execute(cmd, shell=True)  # , usecontainer=True, copytool=True)
    logger.info(f'subject={stdout}')

    cmd = f"{envsetup}arcproxy -i validityEnd -i validityLeft -i vomsACvalidityEnd -i vomsACvalidityLeft"
    _exit_code, stdout, stderr = execute(cmd, shell=True)  # , usecontainer=True, copytool=True)
    if stdout is not None:
        if 'command not found' in stdout:
            logger.warning(f"arcproxy is not available on this queue,"
                           f"this can lead to memory issues with voms-proxy-info on SL6: {stdout}")
            exit_code = -1
        else:
            exit_code, diagnostics, validity_end_cert, validity_end = interpret_proxy_info(_exit_code, stdout, stderr, limit)

            if proxy_id and validity_end:  # setup cache if requested
                if exit_code == 0:
                    logger.info(f"caching the validity ends from arcproxy: cache[\'{proxy_id}\'] = [{validity_end_cert}, {validity_end}]")
                    verify_arcproxy.cache[proxy_id] = [validity_end_cert, validity_end]
                else:
                    logger.warning('cannot store validity ends from arcproxy in cache')
                    verify_arcproxy.cache[proxy_id] = [-1, -1]  # -1 in cache means any error in prev validation
            if exit_code == 0:

                #if proxy_id in verify_arcproxy.cache:
                #    logger.debug('getting validity ends from arcproxy cache')
                #else:
                #    logger.debug('using validity ends from arcproxy (cache not available)')
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
                return exit_code, diagnostics
            elif exit_code == -1:  # skip to next proxy test
                return exit_code, diagnostics
            elif exit_code == errors.NOVOMSPROXY:
                return exit_code, diagnostics
            else:
                logger.info("will try voms-proxy-info instead")
                exit_code = -1
    else:
        logger.warning('command execution failed')

    return exit_code, diagnostics


def check_time_left(proxyname, validity, limit):
    """

    :param proxyname: cert or proxy (string).
    :param validity:
    :param limit: time limit in hours (int).
    return exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ''
    tnow = int(time() + 0.5)  # round to seconds
    seconds_left = validity - tnow

    # test bad proxy
    #if proxyname == 'proxy':
    #    seconds_left = 1000
    logger.info("cache: check %s validity: wanted=%dh (%ds with grace) left=%.2fh (now=%d validity=%d left=%d)",
                proxyname, limit, limit * 3600 - 20 * 60, float(seconds_left) / 3600, tnow, validity, seconds_left)
    if seconds_left < limit * 3600 - 20 * 60:
        diagnostics = 'cert/proxy is about to expire: %.2fh' % (float(seconds_left) / 3600)
        logger.warning(diagnostics)
        exit_code = errors.CERTIFICATEHASEXPIRED if proxyname == 'cert' else errors.VOMSPROXYABOUTTOEXPIRE
    else:
        logger.info(f"{proxyname} validity time is verified")

    return exit_code, diagnostics


def verify_vomsproxy(envsetup, limit):
    """
    Verify proxy using voms-proxy-info command.

    :param envsetup: general setup string for proxy commands (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), error diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    if os.environ.get('X509_USER_PROXY', '') != '':
        cmd = f"{envsetup}voms-proxy-info -actimeleft --timeleft --file $X509_USER_PROXY"
        logger.info(f'executing command: {cmd}')
        _exit_code, stdout, stderr = execute(cmd, shell=True)
        if stdout is not None:
            if "command not found" in stdout:
                logger.info("skipping voms proxy check since command is not available")
            else:
                exit_code, diagnostics, validity_end_cert, validity_end = interpret_proxy_info(_exit_code, stdout, stderr, limit)
                if exit_code == 0:
                    logger.info("voms proxy verified using voms-proxy-info")
                    return 0, diagnostics
        else:
            logger.warning('command execution failed')
    else:
        logger.warning('X509_USER_PROXY is not set')

    return exit_code, diagnostics


def verify_gridproxy(envsetup, limit):
    """
    Verify proxy using grid-proxy-info command.

    :param envsetup: general setup string for proxy commands (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), error diagnostics (string).
    """

    ec = 0
    diagnostics = ""

    if limit:
        # next clause had problems: grid-proxy-info -exists -valid 0.166666666667:00
        #cmd = "%sgrid-proxy-info -exists -valid %s:00" % (envsetup, str(limit))
        # more accurate calculation of HH:MM
        limit_hours = int(limit * 60) / 60
        limit_minutes = int(limit * 60 + .999) - limit_hours * 60
        cmd = "%sgrid-proxy-info -exists -valid %d:%02d" % (envsetup, limit_hours, limit_minutes)
    else:
        cmd = f"{envsetup}grid-proxy-info -exists -valid 24:00"

    logger.info(f'executing command: {cmd}')
    exit_code, stdout, stderr = execute(cmd, shell=True)
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


def interpret_proxy_info(_ec, stdout, stderr, limit):
    """
    Interpret the output from arcproxy or voms-proxy-info.

    :param _ec: exit code from proxy command (int).
    :param stdout: stdout from proxy command (string).
    :param stderr: stderr from proxy command (string).
    :param limit: time limit in hours (int).
    :return: exit code (int), diagnostics (string). validity end cert, validity end in seconds if detected, None if not detected (int).
    """

    exitcode = 0
    diagnostics = ""
    validity_end = None  # not detected
    validity_end_cert = None  # not detected

    logger.debug('stdout = %s', stdout)
    logger.debug('stderr = %s', stderr)

    if _ec != 0:
        if "Unable to verify signature! Server certificate possibly not installed" in stdout:
            logger.warning(f"skipping voms proxy check: {stdout}")
        # test for command errors
        elif "arcproxy: error while loading shared libraries" in stderr:
            exitcode = -1
            logger.warning('skipping arcproxy test')
        elif "arcproxy:" in stdout:
            diagnostics = f"arcproxy failed: {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.GENERALERROR
        else:
            # Analyze exit code / output
            diagnostics = f"voms proxy certificate check failure: {_ec}, {stdout}"
            logger.warning(diagnostics)
            exitcode = errors.NOVOMSPROXY
    else:
        if "\n" in stdout:
            # try to extract the time left from the command output
            validity_end_cert, validity_end, stdout = extract_time_left(stdout)
            if validity_end:
                return exitcode, diagnostics, validity_end_cert, validity_end
            else:
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
                stdout = sum([a * b for a, b in zip(ftr, list(map(int, stdout.split(':'))))])
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


def extract_time_left(stdout):
    """
    Extract the time left for the cert and proxy from the proxy command.
    Some processing on the stdout is done.

    :param stdout: stdout (string).
    :return: validity_end_cert, validity_end, stdout (int, string))
    """

    validity_end_cert = None
    validity_end = None

    # remove the last \n in case there is one
    if stdout[-1] == '\n':
        stdout = stdout[:-1]
    stdout_split = stdout.split('\n')
    # give up if there not four entries
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

    return validity_end_cert, validity_end, stdout


def extract_time_left_old(stdout):
    """
    Extract the time left from the proxy command.
    Some processing on the stdout is done.

    :param stdout: stdout (string).
    :return: validity_end, stdout (int, string))
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


def getproxy_dictionary(voms_role):
    """
    Prepare the dictionary for the getProxy call.

    :param voms_role: VOMS role (string).
    """

    return {'role': voms_role, 'dn': 'atlpilo2'} if voms_role == 'atlas' else {'role': voms_role}
