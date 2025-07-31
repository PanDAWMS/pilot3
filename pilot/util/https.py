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
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-25

"""Functions for https interactions."""

import ast
try:
    import certifi
except ImportError:
    certifi = None
import http.client as http_client
import json
import logging
import os
import platform
import random
try:
    import requests
except ImportError:
    requests = None
import shlex
import socket
import ssl
import sys
import urllib.request
import urllib.error
import urllib.parse

from collections.abc import Callable
from collections import namedtuple
from gzip import GzipFile
from io import BytesIO
from re import (
    findall,
    sub
)
from time import (
    sleep,
    time
)
from typing import Any
from urllib.parse import parse_qs

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import FileHandlingFailure
from pilot.info.jobdata import JobData

from .auxiliary import is_kubernetes_resource
from .config import config
from .constants import get_pilot_version
from .container import execute
from .filehandling import (
    read_file,
    rename,
    write_file,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()

_ctx = namedtuple('_ctx', 'ssl_context user_agent capath cacert')
_ctx.ssl_context = None
_ctx.user_agent = "Pilot3"  # default value (must be string not None)
_ctx.capath = None
_ctx.cacert = None

# anisyonk: public copy of `_ctx` to avoid logic break since ssl_context is reset inside the request() -- FIXME
# anisyonk: public instance, should be properly initialized by `https_setup()`
# anisyonk: use lightweight class definition instead of namedtuple since tuple is immutable and we don't need/use any tuple features here
ctx = type('ctx', (object,), {'ssl_context': None, 'user_agent': 'Pilot3 client', 'capath': None, 'cacert': None})


def _tester(func: Callable[..., Any], *args: Any) -> Any:
    """
    Test function ``func`` on the given arguments and return the first positive.

    >>> _tester(lambda x: x%3 == 0, 1, 2, 3, 4, 5, 6)
    3
    >>> _tester(lambda x: x%3 == 0, 1, 2)
    None

    :param func: the function to be tested (Callable)
    :param args: other arguments (Any)
    :return: something or none (Any).
    """
    for arg in args:
        if arg is not None and func(arg):
            return arg

    return None


def capath(args: object = None) -> Any:
    """
    Try to get :abbr:`CA (Certification Authority)` path with certificates.

    Tries
    1. :option:`--capath` from arguments
    2. :envvar:`X509_CERT_DIR` from env
    3. Path ``/etc/grid-security/certificates``

    :param args: arguments, parsed by argparse (object)
    :returns: directory path (str), or None.
    """
    return _tester(os.path.isdir,
                   args and args.capath,
                   os.environ.get('X509_CERT_DIR'),
                   '/etc/grid-security/certificates')


def cacert_default_location() -> str or None:
    """
    Try to get current user ID through `os.getuid`, and get the posix path for x509 certificate.

    :returns: `str` -- posix default x509 path, or `None` (str or None).
    """
    try:
        return f'/tmp/x509up_u{os.getuid()}'
    except AttributeError:
        logger.warning('no UID available? System not POSIX-compatible... trying to continue')

    return None


def cacert(args: object = None) -> str:
    """
    Try to get :abbr:`CA (Certification Authority)` certificate or X509.

    Checks that it is a regular file.
    Tries
    1. :option:`--cacert` from arguments
    2. :envvar:`X509_USER_PROXY` from env
    3. Path ``/tmp/x509up_uXXX``, where ``XXX`` refers to ``UID``

    :param args: arguments, parsed by argparse (object)
    :return: certificate file path (str).
    """
    cert_path = _tester(os.path.isfile,
                        args and args.cacert,
                        os.environ.get('X509_USER_PROXY'),
                        cacert_default_location())

    return cert_path if cert_path else ""


def https_setup(args: object = None, version: str = ""):
    """
    Set up the context for HTTPS requests.

    1. Selects the certificate paths
    2. Sets up :mailheader:`User-Agent`
    3. Tries to create `ssl.SSLContext` for future use (falls back to :command:`curl` if fails)

    :param args: arguments, parsed by argparse (object)
    :param version: pilot version string (for :mailheader:`User-Agent`) (str).
    """
    version = version or get_pilot_version()

    _ctx.user_agent = f'pilot/{version} (Python {sys.version.split()[0]}; {platform.system()} {platform.machine()})'
    _ctx.capath = capath(args)
    _ctx.cacert = cacert(args)

    try:
        _ctx.ssl_context = ssl.create_default_context(capath=_ctx.capath,
                                                      cafile=_ctx.cacert)
    except Exception as exc:
        logger.info(f"capath={_ctx.capath}, cacert={_ctx.cacert}")
        logger.warning(f'SSL communication is impossible due to SSL error: {exc}')
        _ctx.ssl_context = None

    # anisyonk: clone `_ctx` to avoid logic break since ssl_context is reset inside the request() -- FIXME
    ctx.capath = _ctx.capath
    ctx.cacert = _ctx.cacert
    ctx.user_agent = _ctx.user_agent

    try:
        ctx.ssl_context = ssl.create_default_context(capath=ctx.capath, cafile=ctx.cacert)
        ctx.ssl_context.load_cert_chain(ctx.cacert)
    except Exception as exc:
        logger.warning(f'Failed to initialize SSL context .. skipped, error: {exc}')


def request(url: str, data: dict = None, plain: bool = False, secure: bool = True, ipv: str = 'IPv6') -> Any:
    """
    Send a request using HTTPS.

    Sends :mailheader:`User-Agent` and certificates previously being set up by `https_setup`.
    If `ssl.SSLContext` is available, uses `urllib2` as a request processor. Otherwise, uses :command:`curl`.

    If ``data`` is provided, encodes it as a URL form data and sends it to the server.

    Treats the request as JSON unless a parameter ``plain`` is `True`.
    If JSON is expected, sends ``Accept: application/json`` header.

    Usage:

    .. code-block:: python
        :emphasize-lines: 2

        https_setup(args, PILOT_VERSION)  # sets up ssl and other stuff
        response = request('https://some.url', {'some':'data'})

    :param url: the URL of the resource (str)
    :param data: data to send (dict)
    :param plain: if true, treats the response as a plain text (bool)
    :param secure: default: True, i.e. use certificates (bool)
    :param ipv: internet protocol version (str)
    :returns:
        - :keyword:`dict` -- if everything went OK
        - `str` -- if ``plain`` parameter is `True`
        - `None` -- if something went wrong.
    """
    if data is None:
        data = {}
    _ctx.ssl_context = None  # certificates are not available on the grid, use curl

    # note that X509_USER_PROXY might change during running (in the case of proxy downloads), so
    # we might have to update _ctx
    update_ctx()

    logger.debug(f'server update dictionary = \n{data}')

    # get the filename and strdata for the curl config file
    filename, strdata = get_vars(url, data)
    # write the strdata to file
    try:
        writestatus = write_file(filename, strdata)
    except FileHandlingFailure:
        writestatus = None

    # get the config option for the curl command
    dat = get_curl_config_option(writestatus, url, data, filename)

    # loop over internet protocol versions since proper value might be known yet (ie before downloading queuedata)
    ipvs = ['IPv6', 'IPv4'] if ipv == 'IPv6' else ['IPv4']
    if _ctx.ssl_context is None and secure:
        failed = False
        for _ipv in ipvs:
            req, obscure = get_curl_command(plain, dat, _ipv)
            if not req:
                logger.warning('failed to construct valid curl command')
                failed = True
                break
            try:
                status, output, stderr = execute(req, obscure=obscure, timeout=config.Pilot.http_maxtime)
            except Exception as exc:
                logger.warning(f'exception: {exc}')
                failed = True
                break
            else:
                if status == 0:
                    break
                logger.warning(f'request failed for IPv={_ipv} ({status}): stdout={output}, stderr={stderr}')
                continue
        if failed:
            return None

        # return output if plain otherwise return json.loads(output)
        if plain:
            return output
        try:
            ret = json.loads(output)
        except Exception as exc:
            logger.warning(f'json.loads() failed to parse output={output}: {exc}')
            return None
        return ret

    req = execute_urllib(url, data, plain, secure)
    context = _ctx.ssl_context if secure else None

    ec, output = get_urlopen_output(req, context)
    if ec:
        return None

    return output.read() if plain else json.load(output)


def update_ctx():
    """Update the ctx object in case X509_USER_PROXY has been updated."""
    cert = str(_ctx.cacert)  # to bypass pylint W0143 warning
    x509 = os.environ.get('X509_USER_PROXY', cert)
    if x509 != cert and os.path.exists(x509):
        _ctx.cacert = x509

    path = str(_ctx.capath)  # to bypass pylint W0143 warning
    certdir = os.environ.get('X509_CERT_DIR', path)
    if certdir != path and os.path.exists(certdir):
        _ctx.capath = certdir


def get_local_oidc_token_info() -> tuple[str or None, str or None]:
    """
    Get the OIDC token locally.

    :return: token (str), token origin (str) (tuple).
    """
    # first check if there is a token that was downloaded by the pilot
    refreshed_auth_token = os.environ.get('OIDC_REFRESHED_AUTH_TOKEN')
    if refreshed_auth_token and os.path.exists(refreshed_auth_token):
        auth_token = refreshed_auth_token
    else:  # no refreshed token, try to get the initial longlasting token
        auth_token = os.environ.get('OIDC_AUTH_TOKEN', os.environ.get('PANDA_AUTH_TOKEN'))

    # origin of the token (panda_dev.pilot, ..)
    auth_origin = os.environ.get('OIDC_AUTH_ORIGIN', os.environ.get('PANDA_AUTH_ORIGIN'))

    return auth_token, auth_origin


def get_curl_command(plain: bool, dat: str, ipv: str) -> tuple[Any, str]:
    """
    Get the curl command.

    :param plain: if true, treats the response as a plain text (bool)
    :param dat: curl config option (str)
    :param ipv: internet protocol version (str)
    :return: curl command (str or None), sensitive string to be obscured before dumping to log (str).
    """
    auth_token_content = ''
    auth_token, auth_origin = get_local_oidc_token_info()

    command = 'curl'
    if ipv == 'IPv4':
        command += ' -4'
    if auth_token and auth_origin:
        # curl --silent --capath
        # /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/etc/grid-security-emi/certificates --compressed
        # -H "Authorization: Bearer <contents of PANDA_AUTH_TOKEN>" -H "Origin: <PANDA_AUTH_VO>"
        path = locate_token(auth_token)
        if os.path.exists(path):
            auth_token_content = read_file(path)
            if not auth_token_content:
                logger.warning(f'failed to read file {path}')
                return None, ''
        else:
            logger.warning(f'path does not exist: {path}')
            return None, ''
        if not auth_token_content:
            logger.warning('OIDC_AUTH_TOKEN/PANDA_AUTH_TOKEN content could not be read')
            return None, ''

        req = f'{command} -sS --compressed --connect-timeout {config.Pilot.http_connect_timeout} ' \
              f'--max-time {config.Pilot.http_maxtime} '\
              f'--capath {shlex.quote(_ctx.capath or "")} ' \
              f'-H "Authorization: Bearer {shlex.quote(auth_token_content)}" ' \
              f'-H {shlex.quote("Accept: application/json") if not plain else ""} ' \
              f'-H "Origin: {shlex.quote(auth_origin)}" {dat}'
    else:
        req = f'{command} -sS --compressed --connect-timeout {config.Pilot.http_connect_timeout} ' \
              f'--max-time {config.Pilot.http_maxtime} '\
              f'--capath {shlex.quote(_ctx.capath or "")} ' \
              f'--cert {shlex.quote(_ctx.cacert or "")} ' \
              f'--cacert {shlex.quote(_ctx.cacert or "")} ' \
              f'--key {shlex.quote(_ctx.cacert or "")} '\
              f'-H {shlex.quote(f"User-Agent: {_ctx.user_agent}")} ' \
              f'-H {shlex.quote("Accept: application/json") if not plain else ""} {dat}'

    return req, auth_token_content


def locate_token(auth_token: str, key: bool = False) -> str:
    """
    Locate the OIDC token file.

    Primary means the original token file, not the refreshed one.
    The primary token is needed for downloading new tokens (i.e. 'refreshed' ones).

    Note that auth_token is only the file name for the primary token, but has the full path for any
    refreshed token.

    :param auth_token: file name of token (str)
    :param key: if true, token key is used (bool)
    :return: path to token (str).
    """
    primary_basedir = os.path.dirname(os.environ.get('OIDC_AUTH_DIR', os.environ.get('PANDA_AUTH_DIR', os.environ.get('X509_USER_PROXY', ''))))
    paths = [os.path.join(primary_basedir, auth_token),
             os.path.join(os.environ.get('PILOT_SOURCE_DIR', ''), auth_token),
             os.path.join(os.environ.get('PILOT_WORK_DIR', ''), auth_token),
             os.path.join(os.environ.get('HOME', ''), auth_token)]

    # if the refreshed token exists, prepend it to the paths list and use it first
    if not key:
        _refreshed = os.environ.get('OIDC_REFRESHED_AUTH_TOKEN')  # full path to any refreshed token
        if _refreshed and os.path.exists(_refreshed):
            paths.insert(0, _refreshed)

    # remove duplicates
    paths = list(set(paths))

    path = ""
    for _path in paths:
        if os.path.exists(_path):
            logger.debug(f'found {_path}')
            path = _path
            break

    if path == "":
        logger.info(f'did not find any local token file ({auth_token}) in paths={paths}')

    return path


def get_vars(url: str, data: dict) -> tuple[str, str]:
    """
    Get the filename and strdata for the curl config file.

    :param url: URL (str)
    :param data: data to be written to file (dict)
    :return: filename (str), strdata (str) (tuple).
    """
    strdata = ""
    for key in data:
        strdata += f'data="{urllib.parse.urlencode({key: data[key]})}"\n'
    jobid = f"_{data['jobId']}" if 'jobId' in list(data.keys()) else ""

    # write data to temporary config file
    filename = f"{os.getenv('PILOT_HOME')}/curl_{os.path.basename(url)}{jobid}.config"

    return filename, strdata


def get_curl_config_option(writestatus: bool, url: str, data: dict, filename: str) -> str:
    """
    Get the curl config option.

    :param writestatus: status of write_file call (bool)
    :param url: URL (str)
    :param data: data structure (dict)
    :param filename: file name of config file (str)
    :return: config option (str).
    """
    if not writestatus:
        logger.warning('failed to create curl config file (will attempt to urlencode data directly)')
        dat = shlex.quote(url + '?' + urllib.parse.urlencode(data) if data else '')
    else:
        dat = f'--config {filename} {url}'

    return dat


def execute_urllib(url: str, data: dict, plain: bool, secure: bool) -> urllib.request.Request:
    """
    Execute the request using urllib.

    :param url: URL (str)
    :param data: data structure (dict)
    :param plain: if true, treats the response as a plain text (bool)
    :param secure: default: True, i.e. use certificates (bool)
    :return: urllib request structure (Any).
    """
    req = urllib.request.Request(url, urllib.parse.urlencode(data).encode('ascii'))
    if not plain:
        req.add_header('Accept', 'application/json')
    if secure:
        req.add_header('User-Agent', _ctx.user_agent)

    return req


def get_urlopen_output(req: urllib.request.Request, context: ssl.SSLContext) -> tuple[int, str]:
    """
    Get the output from the urlopen request.

    :param req: urllib request structure (urllib.request.Request)
    :param context: ssl context (ssl.SSLContext)
    :return: exit code (int), output (str) (tuple).
    """
    exitcode = -1
    output = ""
    logger.debug('ok about to open url')
    try:
        output = urllib.request.urlopen(req, context=context)
    except urllib.error.HTTPError as exc:
        logger.warning(f'server error ({exc.code}): {exc.read()}')
    except (urllib.error.URLError, http_client.RemoteDisconnected, ssl.SSLError) as exc:
        logger.warning(f'connection error: {exc.reason}')
    else:
        exitcode = 0
    logger.debug(f'ok url opened: exitcode={exitcode}')

    return exitcode, output


def send_update(update_function: str, data: dict, url: str, port: int, job: JobData = None, ipv: str = 'IPv6', max_attempts: int = 2) -> dict:
    """
    Send the update to the server using the given function and data.

    :param update_function: 'updateJob' or 'updateWorkerPilotStatus' (str)
    :param data: data (dict)
    :param url: server url (str)
    :param port: server port (int)
    :param job: job object (JobData)
    :param ipv: internet protocol version, IPv4 or IPv6 (str)
    :param max_attempts: maximum number of attempts to send the update (int)
    :return: server response (dict).
    """
    attempt = 0
    done = False
    res = None

    if os.environ.get('REACHED_MAXTIME', None) and update_function == 'updateJob':
        data['state'] = 'failed'
        if job:
            job.state = 'failed'
            job.completed = True
            msg = 'the max batch system time limit has been reached'
            logger.warning(msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.REACHEDMAXTIME, msg=msg)
            add_error_codes(data, job)

    # do not allow any delayed heartbeat messages for running state, if the job has completed (ie another call to this
    # function was already made by another thread for finished/failed state)
    if job:  # ignore for updateWorkerPilotStatus calls
        if job.completed and job.state in {'running', 'starting'}:
            logger.warning(f'will not send job update for {job.state} state since the job has already completed')
            return None  # should be ignored

    while attempt < max_attempts and not done:
        logger.info(f'server update attempt {attempt + 1}/{max_attempts}')

        # get the URL for the PanDA server from pilot options or from config
        try:
            pandaserver = get_panda_server(url, port)
        except Exception as exc:
            logger.warning(f'exception caught in get_panda_server(): {exc}')
            sleep(5)
            attempt += 1
            continue
        # send the heartbeat
        res = send_request(pandaserver, update_function, data, job, ipv)
        if res is not None:
            done = True
        attempt += 1
        if not done:
            sleep(config.Pilot.update_sleep)

    return res


def send_request(pandaserver: str, update_function: str, data: dict, job: JobData, ipv: str) -> dict or None:
    """
    Send the request to the server using the appropriate method.

    :param pandaserver: PanDA server URL (str)
    :param update_function: update function (str)
    :param data: data dictionary (dict)
    :param job: job object (JobData)
    :param ipv: internet protocol version (str)
    :return: server response (dict or None).
    """
    res = None
    time_before = int(time())

    # adjust the server path if the new server API is being used
    if "api/v" in update_function:  # e.g. api/v1
        path = f"{pandaserver}/{update_function}"
    else:
        path = f'{pandaserver}/server/panda/{update_function}'

    logger.debug(f"update_function = {update_function}, path = {path}")
    # first try the new request2 method based on urllib. If that fails, revert to the old request method using curl
    try:
        res = request2(f'{path}', data=data, panda=True)
    except Exception as exc:
        logger.warning(f'exception caught in https.request(): {exc}')

    if not res:
        logger.warning('failed to send request using urllib based request2(), will try curl based request()')
        try:
            res = request(f'{pandaserver}/server/panda/{update_function}', data=data, ipv=ipv)
        except Exception as exc:
            logger.warning(f'exception caught in https.request(): {exc}')

    if isinstance(res, str):
        logger.warning(f"panda server returned a string instead of a dictionary: {res}")
        return None

    if res:
        txt = f'server {update_function} request completed in {int(time()) - time_before}s'
        if job:
            txt += f' for job {job.jobid}'
        logger.info(txt)
        # hide sensitive info
        pilotsecrets = ''
        if res and 'pilotSecrets' in res:
            pilotsecrets = res['pilotSecrets']
            res['pilotSecrets'] = '********'
        logger.info(f'server responded with: res = {res}')
        if pilotsecrets:
            res['pilotSecrets'] = pilotsecrets
    else:
        logger.warning(f'server {update_function} request failed both with urllib and curl')

    return res


def get_panda_server(url: str, port: int, update_server: bool = True) -> str:
    """
    Get the URL for the PanDA server.

    The URL will be randomized if the server can be contacted (otherwise fixed).

    :param url: URL string, if set in pilot option (port not included) (str)
    :param port: port number, if set in pilot option (int)
    :param update_server: True if the server can be contacted, False otherwise (bool)
    :return: full URL (either from pilot options or from config file) (str).
    """
    if url != '':
        parsedurl = url.split('://')
        scheme = None
        if len(parsedurl) == 2:
            scheme = parsedurl[0]
            loc = parsedurl[1]
        else:
            loc = parsedurl[0]

        parsedloc = loc.split(':')
        loc = parsedloc[0]

        # if a port is provided in the url, then override the port argument
        if len(parsedloc) == 2:
            port = parsedloc[1]
        # default scheme to https
        if not scheme:
            scheme = "https"
        portstr = f":{port}" if port else ""
        pandaserver = f"{scheme}://{loc}{portstr}"
    else:
        pandaserver = config.Pilot.pandaserver
        if not pandaserver.startswith('http'):
            pandaserver = 'https://' + pandaserver

    if not update_server:
        return pandaserver

    # set a timeout to prevent potential hanging due to problems with DNS resolution, or if the DNS
    # server is slow to respond
    socket.setdefaulttimeout(config.Pilot.http_maxtime)

    # add randomization for PanDA server
    default = 'pandaserver.cern.ch'
    if default in pandaserver:
        try:
            rnd = random.choice([socket.getfqdn(vv) for vv in set([v[-1][0] for v in socket.getaddrinfo(default, 25443, socket.AF_INET)])])
        except (socket.herror, socket.gaierror) as exc:
            logger.warning(f'failed to get address from socket: {exc} - will use default server ({pandaserver})')
        else:
            pandaserver = pandaserver.replace(default, rnd)
            logger.debug(f'updated {default} to {pandaserver}')

    return pandaserver


def add_error_codes(data: dict, job: JobData):
    """
    Add error codes to data structure.

    :param data: data dictionary (dict)
    :param job: job object (JobData).
    """
    # error codes
    pilot_error_code = job.piloterrorcode
    pilot_error_codes = job.piloterrorcodes
    if pilot_error_codes != []:
        logger.warning(f'pilotErrorCodes = {pilot_error_codes} (will report primary/first error code)')
        data['pilotErrorCode'] = pilot_error_codes[0]
    else:
        data['pilotErrorCode'] = pilot_error_code

    def remove_timestamp(log_entry: str) -> str:
        """
        Removes the timestamp in the format 'YYYY-MM-DD HH:MM:SS,mmm' from a log entry.

        :param log_entry: The log entry string containing a timestamp (str)
        :return: The log entry without the timestamp (str).
        """
        return sub(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{1,3})?', '', log_entry).strip()

    # add error info
    pilot_error_diag = job.piloterrordiag
    pilot_error_diags = job.piloterrordiags
    if pilot_error_diags != []:
        # filter out any timestamps that might mess up monitoring (https://its.cern.ch/jira/browse/ATLASPANDA-1324)
        # pilot_error_diags = [remove_timestamp(diag) for diag in pilot_error_diags]
        pilot_error_diags_cleaned = []
        for diag in pilot_error_diags:
            if isinstance(diag, str):
                pilot_error_diags_cleaned.append(remove_timestamp(diag))
            else:
                # Optionally log or convert to string
                pilot_error_diags_cleaned.append(remove_timestamp(str(diag)))
                logger.warning(f'pilotErrorDiags contains non-string value: {diag} (converted to string)')
        pilot_error_diags = pilot_error_diags_cleaned

        logger.warning(f'pilotErrorDiags = {pilot_error_diags} (will report primary/first error diag)')
        data['pilotErrorDiag'] = pilot_error_diags[0]
    else:
        data['pilotErrorDiag'] = pilot_error_diag

    # special case for SIGTERM failures on Kubernetes resources
    if data.get('pilotErrorCode') == errors.SIGTERM:
        if is_kubernetes_resource():
            logger.warning('resetting SIGTERM error to PREEMPTION for Kubernetes resource')
            data['pilotErrorCode'] = errors.PREEMPTION
            data['pilotErrorDiag'] = errors.get_error_code(errors.PREEMPTION)

    data['transExitCode'] = job.transexitcode
    data['exeErrorCode'] = job.exeerrorcode
    data['exeErrorDiag'] = job.exeerrordiag


def get_server_command(url: str, port: int, cmd: str = 'getJob') -> str:
    """
    Prepare the getJob server command.

    :param url: PanDA server URL (str)
    :param port: PanDA server port (int)
    :param cmd: command (str)
    :return: full server command (str).
    """
    if url != "":
        port_pattern = '.:([0-9]+)'
        if not findall(port_pattern, url):
            url = url + f':{port}'
        else:
            logger.debug(f'URL already contains port: {url}')
    else:
        url = config.Pilot.pandaserver
    if url == "":
        logger.fatal('PanDA server url not set (either as pilot option or in config file)')
    elif not url.startswith("http"):
        url = 'https://' + url
        logger.warning('detected missing protocol in server url (added)')

    # randomize server name
    url = get_panda_server(url, port)

    return f'{url}/server/panda/{cmd}'


def get_headers(use_oidc_token: bool, auth_token_content: str = None, auth_origin: str = None,
                content_type: str = "application/json", accept: bool = False) -> dict:
    """
    Get the headers for the request.

    :param use_oidc_token: True if OIDC token should be used (bool)
    :param auth_token_content: token content (str)
    :param auth_origin: token origin (str)
    :param content_type: content type (str)
    :param accept: True if accept header should be added (bool)
    :return: headers (dict).
    """
    if use_oidc_token:
        headers = {
            "Authorization": f"Bearer {shlex.quote(auth_token_content)}",
            # "Accept": "application/json",  # what is the difference with "Content-Type"? See else: below
            "Origin": shlex.quote(auth_origin),
        }
    else:
        headers = {}

    # always add the user agent
    headers["User-Agent"] = _ctx.user_agent

    # only add the content type if there is a body to send (that is of type application/json)
    if content_type:
        headers["Content-Type"] = content_type
        if accept:
            headers["Accept"] = content_type

    return headers


def get_ssl_context() -> ssl.SSLContext:
    """
    Get the SSL context.

    :return: SSL context (ssl.SSLContext).
    """
    # should be
    # ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
    # but it doesn't work, so use this for now even if it throws a deprecation warning
    # logger.info(f'ssl.OPENSSL_VERSION_INFO={ssl.OPENSSL_VERSION_INFO}')
    try:  # for ssl version 3.0 and python 3.10+
        # ssl_context = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_CLIENT)
        ssl_context = ssl.SSLContext(protocol=None)
    except Exception:  # for ssl version 1.0
        ssl_context = ssl.SSLContext()

    return ssl_context


def get_auth_token_content(auth_token: str, key: bool = False) -> str:
    """
    Get the content of the auth token.

    :param auth_token: token name (str)
    :param key: if true, token key is used (bool)
    :return: token content (str).
    """
    path = locate_token(auth_token, key=key)
    if os.path.exists(path):
        auth_token_content = read_file(path)
        if not auth_token_content:
            logger.warning(f'failed to read file {path}')
            return ""
        else:
            logger.info(f'read contents from file {path} (length = {len(auth_token_content)})')
    else:
        if not path:
            logger.warning('token could not be located (path is not set - make sure OIDC env vars are set)')
        else:
            logger.warning(f'path does not exist: {path}')
        return ""

    return auth_token_content


class IPv4HTTPHandler(urllib.request.HTTPHandler):
    def http_open(self, req):
        return self.do_open(self._create_connection, req)

    def _create_connection(self, host, port=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None):
        return socket.create_connection((host, port), timeout, source_address, family=socket.AF_INET)


def request2(url: str = "", data: dict = None, secure: bool = True, compressed: bool = True, panda: bool = False) -> str or dict:  # noqa: C901
    """
    Send a request using HTTPS (using urllib module).

    :param url: the URL of the resource (str)
    :param data: data to send (dict)
    :param secure: use secure connection (bool)
    :param compressed: compress data (bool)
    :param panda: True for panda server interactions (bool)
    :return: server response (str or dict).
    """
    if data is None:
        data = {}

    ipv = os.environ.get("PILOT_IP_VERSION")

    # https might not have been set up if running in a [middleware] container
    if not _ctx.cacert:
        https_setup(None, get_pilot_version())

    # should tokens be used?
    auth_token, auth_origin = get_local_oidc_token_info()
    use_oidc_token = auth_token and auth_origin and panda
    auth_token_content = get_auth_token_content(auth_token) if use_oidc_token else ""
    if not auth_token_content and use_oidc_token:
        logger.warning('OIDC_AUTH_TOKEN/PANDA_AUTH_TOKEN content could not be read')
        return ""

    # only add Accept to headers if new API is used
    accept = True if "api/v" in url else False

    # get the relevant headers
    headers = get_headers(use_oidc_token, auth_token_content, auth_origin, accept=accept)
    logger.info(f'data = {data}')

    # encode data as compressed JSON
    if compressed:
        if "api/v" in url:
            headers['Content-Encoding'] = 'gzip'
        rdata_out = BytesIO()
        with GzipFile(fileobj=rdata_out, mode="w") as f_gzip:
            f_gzip.write(json.dumps(data).encode())
        data_json = rdata_out.getvalue()
    else:
        data_json = json.dumps(data).encode('utf-8')
    logger.info(f'headers = {hide_token(headers.copy())}')

    # set up the request
    req = urllib.request.Request(url, data_json, headers=headers)

    # create a context with certificate verification
    ssl_context = get_ssl_context()
    #ssl_context.verify_mode = ssl.CERT_REQUIRED
    if not use_oidc_token:
        ssl_context.load_cert_chain(certfile=_ctx.cacert, keyfile=_ctx.cacert)

    if not secure:
        ssl_context.verify_mode = False
        ssl_context.check_hostname = False

    if ipv == 'IPv4':
        logger.info("will use IPv4 in server communication")
        install_ipv4_opener()
    else:
        logger.info("will use IPv6 in server communication")

    # ssl_context = ssl.create_default_context(capath=_ctx.capath, cafile=_ctx.cacert)
    # Send the request securely
    try:
        logger.debug('sending data to server')
        with urllib.request.urlopen(req, context=ssl_context, timeout=config.Pilot.http_maxtime) as response:
            # Handle the response here
            logger.info(f"response.status={response.status}, response.reason={response.reason}")
            ret = response.read().decode('utf-8')
            if 'getProxy' not in url:
                logger.info(f"response={ret}")
        logger.debug('sent request to server')
    except (urllib.error.URLError, urllib.error.HTTPError, http_client.RemoteDisconnected, TimeoutError, ssl.SSLError) as exc:
        ret = f"failed to send request: {exc}"
        logger.warning(ret)
    else:
        if secure and isinstance(ret, str):
            if ret == 'Succeeded':  # this happens for sending modeOn (debug mode)
                ret = {'StatusCode': '0'}
            elif ret.startswith('{') and ret.endswith('}'):
                try:
                    ret = json.loads(ret)
                except json.JSONDecodeError as e:
                    logger.warning(f'failed to parse response: {e}')
            else:  # response="StatusCode=_some number_"
                # Parse the query string into a dictionary
                query_dict = parse_qs(ret)

                # Convert lists to single values
                ret = {k: v[0] if len(v) == 1 else v for k, v in query_dict.items()}

    return ret


def install_ipv4_opener():
    """Install the IPv4 opener."""
    http_proxy = os.environ.get("http_proxy")
    all_proxy = os.environ.get("all_proxy")
    if http_proxy and all_proxy:
        logger.info(f"using http_proxy={http_proxy}, all_proxy={all_proxy}")
        proxy_handler = urllib.request.ProxyHandler({
            'http': http_proxy,
            'https': http_proxy,
            'all': all_proxy
        })
        opener = urllib.request.build_opener(proxy_handler, IPv4HTTPHandler())
    else:
        logger.info("no http_proxy found, will use IPv4 without proxy")
        opener = urllib.request.build_opener(IPv4HTTPHandler())
    urllib.request.install_opener(opener)


def hide_token(headers: dict) -> dict:
    """
    Hide the token in the headers.

    :param headers: Copy of headers (dict)
    :return: headers with token hidden (dict).
    """
    if 'Authorization' in headers:
        headers['Authorization'] = 'Bearer ********'

    return headers


def request3(url: str, data: dict = None) -> str:
    """
    Send a request using HTTPS (using requests module).

    :param url: the URL of the resource (str)
    :param data: data to send (dict)
    :return: server response (str).
    """
    if data is None:
        data = {}
    if not requests:
        logger.warning('cannot use requests module (not available)')
        return ""
    if not certifi:
        logger.warning('cannot use certifi module (not available)')
        return ""

        # https might not have been set up if running in a [middleware] container
    if not _ctx.cacert:
        logger.debug('setting up unset https')
        https_setup(None, get_pilot_version())

    # define additional headers
    headers = {
        "Content-Type": "application/json",
        "User-Agent": _ctx.user_agent,
    }

    # Convert the dictionary to a JSON string
    data_json = json.dumps(data)

    # Use the requests module to make the HTTP request
    try:
        # certifi.where() = /cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase/x86_64/python/3.11.7-x86_64-el9/
        #                    lib/python3.11/site-packages/certifi/cacert.pem
        # _ctx.cacert = /alrb/x509up_u25606_prod
        response = requests.post(url, data=data_json, headers=headers, verify=_ctx.cacert, cert=certifi.where(), timeout=120)
        response.raise_for_status()  # Raise an error for bad responses (4xx and 5xx)

        # Handle the response as needed
        ret = response.text
    except requests.exceptions.RequestException as exc:
        logger.warning(f'failed to send request: {exc}')
        ret = ""

    return ret


def upload_file(url: str, path: str) -> bool:
    """
    Upload the contents of the given JSON file to the given URL.

    :param url: server URL (str)
    :param path: path to the file (str)
    :return: True if success, False otherwise (bool).
    """
    status = False
    # Define headers
    headers = {
        "Content-Type": "application/json"
    }

    # Read file contents
    with open(path, 'rb') as file:
        file_content = file.read()

    # Define request object
    req = urllib.request.Request(url, data=file_content, headers=headers, method='POST')

    # Set timeouts
    req.timeout = 20
    req.socket_timeout = 120

    # Perform the request
    ret = 'notok'
    try:
        with urllib.request.urlopen(req) as response:
            response_data = response.read()
            # Handle response
            ret = response_data.decode('utf-8')
    except (urllib.error.URLError, http_client.RemoteDisconnected, ssl.SSLError) as e:
        # Handle URL errors
        logger.warning(f"exception caught in urlopen: {e}")
        ret = str(e)

    if ret == 'ok':
        status = True
    else:
        logger.warning(f'failed to send data to {url}: response={ret}')

    return status


def download_file(url: str, timeout: int = 20, headers: dict = None) -> str:
    """
    Download url content.

    The optional headers should in fact be used for downloading OIDC tokens.

    :param url: url (str)
    :param timeout: optional timeout (int)
    :param headers: optional headers (dict)
    :return: url content (str).
    """
    # define the request headers
    if headers is None:
        headers = {"User-Agent": _ctx.user_agent}
    logger.debug(f"headers = {hide_token(headers.copy())}")

    req = urllib.request.Request(url, headers=headers)

    # download the file
    try:
        with urllib.request.urlopen(req, context=ctx.ssl_context, timeout=timeout) as response:
            content = response.read()
    except (urllib.error.URLError, http_client.RemoteDisconnected, ssl.SSLError) as exc:
        logger.warning(f"error occurred with urlopen: {exc.reason}")
        # Handle the error, set content to None or handle as needed
        content = ""

    return content


def refresh_oidc_token(auth_token: str, auth_origin: str, url: str, port: int) -> bool:
    """
    Refresh the OIDC token.

    :param auth_token: token name (str)
    :param auth_origin: token origin (str)
    :param url: server URL (str)
    :param port: server port (int)
    :return: True if success, False otherwise (bool).
    """
    status = False

    # first get the token key
    token_key = os.environ.get("PANDA_AUTH_TOKEN_KEY")
    if not token_key:
        logger.warning('PANDA_AUTH_TOKEN_KEY is not set - will not be able to download a new token')
        return False

    panda_token_key = get_auth_token_content(token_key, key=True)
    if panda_token_key:
        logger.info(f'read token key: {token_key}')
    else:
        logger.warning('failed to get panda_token_key - will not be able to download a new token')
        return status

    # now get the actual token
    auth_token_content = get_auth_token_content(auth_token)
    if not auth_token_content:
        logger.warning(f'failed to get auth token content for {auth_token}')
        return status

    headers = get_headers(True, auth_token_content, auth_origin, content_type=None)
    server_command = get_server_command(url, port, cmd='get_access_token')

    # the client name and token key should be added to the URL as parameters
    server_command += f'?client_name=pilot_server&token_key={panda_token_key}'

    content = download_file(server_command, headers=headers)
    if content:
        status = handle_file_content(content, auth_token)
    else:
        logger.warning(f'failed to download data from \"{url}\" resource')

    return status


def handle_file_content(content: bytes or str, auth_token: str) -> bool:
    """
    Handle the content of the downloaded file.

    The original token is overwritten with the new token.

    :param content: file content (bytes or str)
    :param auth_token: token name (str)
    :return: True if success, False otherwise (bool).
    """
    status = False

    # define the path if it does not exist already
    path = os.environ.get('OIDC_REFRESHED_AUTH_TOKEN')
    if path is None:
        path = os.path.join(os.environ.get('PILOT_HOME'), 'tmp_refreshed_token')

    if isinstance(content, bytes):
        content = content.decode('utf-8')

    # convert the string to a dictionary
    _content = ast.literal_eval(content)

    # check for errors
    statuscode = _content.get('StatusCode', 0)
    diagnostics = _content.get('ErrorDialog', '')
    if statuscode != 0:
        logger.warning(f"failed to get new token: StatusCode={statuscode}, ErrorDialog={diagnostics}")
    else:
        token = _content.get('userProxy')
        if not token:
            logger.warning(f'failed to find userProxy in content: {content}')
        else:
            # write the content to the file
            try:
                with open(path, "w", encoding='utf-8') as _file:
                    _file.write(token)
            except IOError as exc:
                logger.warning(f'failed to write data to file {path}: {exc}')
            else:
                # proceed with renaming the refreshed token to that of the original one (i.e. overwrite)
                status = rename(path, auth_token)
                if status:
                    logger.info(f'saved token data in file {path}, length={len(content) / 1024.:.1f} kB')
                    os.environ['OIDC_REFRESHED_AUTH_TOKEN'] = auth_token
                else:
                    logger.warning(f'failed to rename {path} to {auth_token}')

    return status


def update_local_oidc_token_info(url: str, port: int):
    """
    Update the local OIDC token info.

    :param url: URL (str)
    :param port: port number (int).
    """
    auth_token, auth_origin = get_local_oidc_token_info()
    if auth_token and auth_origin:
        logger.debug('updating OIDC token info')
        status = refresh_oidc_token(auth_token, auth_origin, url, port)
        if not status:
            logger.warning('failed to refresh OIDC token')
        else:
            logger.info('OIDC token has been refreshed')
    else:
        logger.debug('no OIDC token info to update')


def get_base_urls(args_base_urls: str) -> list:
    """
    Get base URLs for transform download.

    :param args_base_urls: base URLs (str)
    :return: list of base URLs (list).
    """
    base_urls = args_base_urls.split(",") if args_base_urls else []
    if not base_urls:
        # try to get the list from an environmental variable instead
        urls = os.getenv("PANDA_BASE_URLS", None)
        if urls:
            base_urls = urls.split(",") if urls else []

    return base_urls


def get_memory_limits(url: str, port: int) -> dict:
    """
    Get the resource types from the server.

    Args:
        url (str): The URL of the server.
        port (int): The port number of the server.

    Returns:
        dict: A dictionary of resource types.
    """
    cmd = get_server_command(url, port, cmd="getResourceTypes")
    try:
        response = request2(cmd, panda=True)  # will be a dictionary
    except Exception as exc:
        logger.warning(f'exception caught in request2() while getting resource types: {exc}')
        return {}
    logger.debug(f"response from {cmd} = {response}")

    if not response:
        logger.warning(f'failed to get memory limits from {cmd}')
        return {}

    # convert the response to a dictionary in case it is a string
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError as exc:
            logger.warning(f'failed to parse response as JSON: {exc}')
            return {}

    resource_types_pre = response.get('ResourceTypes', {})
    # Handle if resource_types_pre is a string (legacy server response)
    if isinstance(resource_types_pre, str):
        resource_types_str = resource_types_pre.replace("None", "null").replace("'", '"')
        try:
            resource_types_list = json.loads(resource_types_str)
        except json.JSONDecodeError as exc:
            logger.warning(f'failed to parse ResourceTypes as JSON: {exc}')
            resource_types_list = []
    elif isinstance(resource_types_pre, dict):
        resource_types_list = resource_types_pre.get('ResourceTypes', [])
    else:
        resource_types_list = []

    # create the final dictionary
    resource_types = {}
    try:
        for entry in resource_types_list:
            resource_name = entry.get('resource_name', '')
            mincore = entry.get('mincore', 0)
            maxcore = entry.get('maxcore', 0)
            minrampercore = entry.get('minrampercore', 0)
            maxrampercore = entry.get('maxrampercore', 0)
            resource_types[resource_name] = {
                'mincore': mincore,
                'maxcore': maxcore,
                'minrampercore': minrampercore,
                'maxrampercore': maxrampercore
            }
    except Exception as exc:
        logger.warning(f'failed to parse resource types: {exc}')
        resource_types = {}

    return resource_types
