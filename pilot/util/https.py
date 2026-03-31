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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-26


"""HTTPS communication layer for the PanDA Pilot.

This module provides all network I/O between the pilot and the PanDA server,
including:

- SSL/TLS context management (:func:`https_setup`, :func:`get_ssl_context`)
- Low-level HTTP request helpers (:func:`request`, :func:`request2`,
  :func:`request3`) supporting both curl and urllib backends
- OIDC token management (:func:`get_local_oidc_token_info`,
  :func:`refresh_oidc_token`, :func:`locate_token`)
- Server update dispatch (:func:`send_update`, :func:`send_request`)
- Job and worker-pilot status queries (:func:`get_job_status_from_server`)
- Tracing and memory-limit endpoint helpers
"""

from __future__ import annotations

import ast
import base64
try:
    import certifi
except ImportError:
    certifi = None
import datetime
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
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import urllib.request
import urllib.error
import urllib.parse

from collections.abc import Callable
from collections import namedtuple
from dataclasses import dataclass
from gzip import GzipFile
from http import client as http_client
from io import BytesIO
from re import (
    findall,
    sub
)
from time import (
    ctime,
    sleep,
    time
)
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union
)

from pilot.common.errorcodes import ErrorCodes
from pilot.info.jobdata import JobData

from .auxiliary import (
    is_kubernetes_resource,
    mask_sensitive_response,
    set_pilot_state
)
from .config import config
from .constants import get_pilot_version
from .filehandling import (
    get_modification_time,
    read_file,
    rename,
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
    """Apply *func* to each argument and return the first one for which it is truthy.

    >>> _tester(lambda x: x%3 == 0, 1, 2, 3, 4, 5, 6)
    3
    >>> _tester(lambda x: x%3 == 0, 1, 2)

    Args:
        func: Predicate to test each argument against.
        *args: Candidate values to test in order; ``None`` entries are skipped.

    Returns:
        The first non-``None`` argument for which ``func`` returns a truthy
        value, or ``None`` if no argument passes.
    """
    for arg in args:
        if arg is not None and func(arg):
            return arg

    return None


def capath(args: object = None) -> Optional[str]:
    """Return the CA certificate directory path.

    Tries the following sources in order, returning the first that is an
    existing directory:

    1. ``--capath`` command-line argument (``args.capath``)
    2. ``X509_CERT_DIR`` environment variable
    3. ``/etc/grid-security/certificates``

    Args:
        args: Parsed argparse namespace, or ``None``.

    Returns:
        Path to an existing CA directory, or ``None`` if none is found.
    """
    return _tester(os.path.isdir,
                   args and args.capath,
                   os.environ.get('X509_CERT_DIR'),
                   '/etc/grid-security/certificates')


def cacert_default_location() -> Optional[str]:
    """Return the default POSIX path for the X.509 user proxy certificate.

    Constructs the conventional path ``/tmp/x509up_u<uid>`` using
    :func:`os.getuid`.  On non-POSIX systems where ``getuid`` is not
    available the function logs a warning and returns ``None``.

    Returns:
        Default proxy path string, or ``None`` on non-POSIX systems.
    """
    try:
        return f'/tmp/x509up_u{os.getuid()}'
    except AttributeError:
        logger.warning('no UID available? System not POSIX-compatible... trying to continue')

    return None


def cacert(args: object = None) -> str:
    """Return the path to the CA certificate or X.509 user proxy.

    Tries the following sources in order, returning the first that is an
    existing regular file:

    1. ``--cacert`` command-line argument (``args.cacert``)
    2. ``X509_USER_PROXY`` environment variable
    3. Default POSIX proxy path ``/tmp/x509up_u<uid>``

    Args:
        args: Parsed argparse namespace, or ``None``.

    Returns:
        Path to an existing certificate file, or an empty string if none
        is found.
    """
    cert_path = _tester(os.path.isfile,
                        args and args.cacert,
                        os.environ.get('X509_USER_PROXY'),
                        cacert_default_location())

    return cert_path if cert_path else ""


def https_setup(args: object = None, version: str = "") -> None:
    """Set up the global SSL/TLS context for all subsequent HTTPS requests.

    Performs three steps:

    1. Resolves CA directory and certificate paths via :func:`capath` and
       :func:`cacert`, storing them on the module-level ``_ctx`` and ``ctx``
       objects.
    2. Builds the ``User-Agent`` header string from the pilot version and
       Python/OS information.
    3. Attempts to create an :class:`ssl.SSLContext`; on failure the context
       is set to ``None`` and the curl fallback path is used instead.

    Args:
        args: Parsed argparse namespace used to resolve certificate paths, or
            ``None`` to rely solely on environment variables and defaults.
        version: Pilot version string embedded in the ``User-Agent`` header.
            Defaults to the value returned by :func:`~pilot.util.constants.get_pilot_version`.
    """
    logger.debug('https_setup: resolving certificate paths and user-agent string')
    version = version or get_pilot_version()

    _ctx.user_agent = f'pilot/{version} (Python {sys.version.split()[0]}; {platform.system()} {platform.machine()})'
    _ctx.capath = capath(args)
    _ctx.cacert = cacert(args)
    logger.debug('https_setup: resolving CA directory and user-agent string')
    logger.debug(f'_ctx.capath={_ctx.capath}, _ctx.cacert={_ctx.cacert}, _ctx.user_agent={_ctx.user_agent}')
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


def request(url: str,
            data: Dict[str, Any] | None = None,
            plain: bool = False,
            secure: bool = True,
            ipv: str = "IPv6") -> Any:
    """
    Curl-based HTTP request fallback which uses a curl config file that references
    a JSON payload file (data = @payload.json) and sets `request = POST`.

    This function delegates to send_request_with_token_via_curl_config() which:
      - writes the JSON payload to a temporary file,
      - writes a curl config file that sets headers and `request = POST`,
      - executes curl with --config <file>,
      - returns either a parsed dict (if response was JSON), a raw string (if curl succeeded but response not JSON),
        or a string beginning with "failed to send request:" on error.

    Args:
        url: Full URL to call (including scheme and port).
        data: Payload dictionary to send (JSON body). If None, an empty dict is used.
        plain: If True, return the raw string response (or JSON-dumped string for dict).
        secure: Kept for API compatibility; currently only logged.
        ipv: Preferred IP family ("IPv4" or "IPv6"); currently only logged.

    Returns:
        Parsed dict on JSON success, raw string on parse failure, or failure string.
    """
    # Ensure payload exists
    payload = data or {}

    logger.info("curl fallback request: url=%s, secure=%s, ipv=%s", url, secure, ipv)
    logger.debug("curl fallback payload preview (first 1024 chars): %s", json.dumps(payload)[:1024])

    # Gather certificate / key and user-agent from module context (_ctx) and config
    # NOTE: these names must match the ones used in your module:
    #   _ctx.cacert  -> path to cert/cacert/key (used for cert/key/cacert)
    #   _ctx.user_agent -> user agent string
    #try:
    #    certfile = _ctx.cacert
    #    cacertfile = _ctx.cacert
    #    keyfile = _ctx.cacert
    #    user_agent = _ctx.user_agent
    #except Exception as exc:
    #    logger.warning("request() curl fallback: missing _ctx cert or user-agent: %s", exc)
    #    # Fallback to sensible defaults (may still fail later)
    #    certfile = cacertfile = keyfile = "/etc/ssl/certs/ca-bundle.crt"
    #    user_agent = "pilot/unknown"

    # Timeouts from config (fallback to sane defaults if not present)
    try:
        connect_timeout = int(config.Pilot.http_connect_timeout)
    except Exception:
        connect_timeout = 100
    try:
        total_timeout = int(config.Pilot.http_maxtime)
    except Exception:
        total_timeout = 120

    # Call the curl-config-based sender
    try:
        # get token content as your code already does
        auth_token, _ = get_local_oidc_token_info()
        auth_token_content = get_auth_token_content(auth_token)  # whatever function you have

        res = send_request_with_token_via_curl_config(
            url=url,
            payload=data,
            cacertfile=_ctx.capath or "/etc/grid-security/certificates",
            use_capath=bool(_ctx.capath),
            token=auth_token_content,
            extra_headers={"Origin": "atlas.pilot"},
            use_token_only=False,  # important
            certfile=_ctx.cacert,
            keyfile=_ctx.cacert,
            connect_timeout=connect_timeout,
            total_timeout=total_timeout,
            verify=True,
        )
    except Exception as exc:
        logger.exception("curl fallback raised unexpected exception: %s", exc)
        return f"failed to send request: {exc}"

    # If caller explicitly wants a plain string, return it (stringify dict if needed)
    if plain:
        if isinstance(res, dict):
            try:
                return json.dumps(res)
            except Exception:
                return str(res)
        return res

    # Normal behaviour: prefer dict, else attempt to json.loads() string response,
    # otherwise return the raw string (previously you attempted json.loads and warned)
    if isinstance(res, dict):
        return res

    if isinstance(res, str):
        try:
            parsed = json.loads(res)
            return parsed
        except Exception:
            logger.warning("json.loads() failed to parse curl output=%r", res[:2000])
            return res

    # Unexpected type from helper: return string repr
    logger.warning("curl fallback returned unexpected type %s", type(res))
    return str(res)


def update_ctx() -> None:
    """Refresh ``_ctx`` certificate paths from the environment.

    Re-reads ``X509_USER_PROXY`` and ``X509_CERT_DIR`` and updates
    ``_ctx.cacert`` / ``_ctx.capath`` if the environment variables point to
    paths that now exist and differ from the stored values.  Called at the
    start of each :func:`request` invocation to pick up proxy renewals.
    """
    cert = str(_ctx.cacert)  # to bypass pylint W0143 warning
    x509 = os.environ.get('X509_USER_PROXY', cert)
    if x509 != cert and os.path.exists(x509):
        _ctx.cacert = x509

    path = str(_ctx.capath)  # to bypass pylint W0143 warning
    certdir = os.environ.get('X509_CERT_DIR', path)
    if certdir != path and os.path.exists(certdir):
        _ctx.capath = certdir


def get_local_oidc_token_info() -> tuple[Optional[str], Optional[str]]:
    """Return the local OIDC auth token path and its origin string.

    Checks for a refreshed token first (``OIDC_REFRESHED_AUTH_TOKEN``), then
    falls back to the initial long-lasting token (``OIDC_AUTH_TOKEN`` or
    ``PANDA_AUTH_TOKEN``).  The origin is read from ``OIDC_AUTH_ORIGIN`` or
    ``PANDA_AUTH_ORIGIN``.

    Returns:
        A two-element tuple ``(auth_token, auth_origin)`` where each element
        is either a string value or ``None`` if the corresponding environment
        variable is not set.
    """
    # first check if there is a token that was downloaded by the pilot
    logger.debug('checking for refreshed OIDC token in environment variable OIDC_REFRESHED_AUTH_TOKEN')
    refreshed_auth_token = os.environ.get('OIDC_REFRESHED_AUTH_TOKEN')
    if refreshed_auth_token and os.path.exists(refreshed_auth_token):
        auth_token = refreshed_auth_token
    else:  # no refreshed token, try to get the initial longlasting token
        auth_token = os.environ.get('OIDC_AUTH_TOKEN', os.environ.get('PANDA_AUTH_TOKEN'))

    # origin of the token (panda_dev.pilot, ..)
    auth_origin = os.environ.get('OIDC_AUTH_ORIGIN', os.environ.get('PANDA_AUTH_ORIGIN'))

    return auth_token, auth_origin


def _format_curl_headers(plain: bool,
                         use_oidc: bool,
                         auth_token_content: str,
                         auth_origin: str,
                         user_agent: str,
                         capath: str,
                         cacert: str) -> Tuple[str, str]:
    """
    Return a safe shell fragment of curl header and cert flags, and the
    bearer token content for redaction. Uses shlex.quote on each header.
    """
    parts = []
    redaction_token = ''

    # common capath
    parts.append(f'--capath {shlex.quote(capath or "")}')

    if use_oidc:
        # Authorization header (quote whole header string)
        parts.append('-H ' + shlex.quote(f'Authorization: Bearer {auth_token_content}'))
        redaction_token = auth_token_content
        if not plain:
            parts.append('-H ' + shlex.quote('Accept: application/json'))
            parts.append('-H ' + shlex.quote('Content-Type: application/json'))
        parts.append('-H ' + shlex.quote(f'Origin: {auth_origin}'))
    else:
        # client certs + UA
        parts.append(f'--cert {shlex.quote(cacert or "")}')
        parts.append(f'--cacert {shlex.quote(cacert or "")}')
        parts.append(f'--key {shlex.quote(cacert or "")}')
        parts.append('-H ' + shlex.quote(f'User-Agent: {user_agent}'))
        if not plain:
            parts.append('-H ' + shlex.quote('Accept: application/json'))
            parts.append('-H ' + shlex.quote('Content-Type: application/json'))

    return ' '.join(parts), redaction_token


def get_curl_command(plain: bool, dat: str, ipv: str) -> Tuple[object, str]:
    """
    Build the curl command safely by delegating header/cert construction
    to _format_curl_headers to avoid nested f-strings and quoting issues.
    Returns (command_str_or_None, auth_token_content_for_redaction).
    """
    auth_token_content = ''
    auth_token, auth_origin = get_local_oidc_token_info()

    command = 'curl'
    if ipv == 'IPv4':
        command += ' -4'

    use_oidc = bool(auth_token and auth_origin)
    if use_oidc:
        path = locate_token(auth_token)
        if os.path.exists(path):
            auth_token_content = read_file(path)
            if not auth_token_content:
                logger.warning(f'failed to read file {path}')
                return None, ''
        else:
            logger.warning(f'path does not exist: {path}')
            return None, ''

    headers_fragment, redact_token = _format_curl_headers(
        plain=plain,
        use_oidc=use_oidc,
        auth_token_content=auth_token_content,
        auth_origin=auth_origin or "",
        user_agent=_ctx.user_agent,
        capath=_ctx.capath or "",
        cacert=_ctx.cacert or ""
    )

    req = (
        f'{command} -sS --compressed --connect-timeout {config.Pilot.http_connect_timeout} '
        f'--max-time {config.Pilot.http_maxtime} {headers_fragment} {dat}'
    )

    return req, redact_token


def locate_token2(auth_token: str, key: bool = False) -> str:
    """Find the filesystem path for an OIDC token file.

    Searches a prioritised list of candidate directories for a file named
    *auth_token*.  When *key* is ``False`` (default) the list is prepended
    with the ``OIDC_REFRESHED_AUTH_TOKEN`` path if it exists, ensuring the
    most recently refreshed token is preferred.

    Candidate directories (in order):

    1. ``OIDC_REFRESHED_AUTH_TOKEN`` (if *key* is ``False`` and the file
       exists)
    2. ``OIDC_AUTH_DIR`` / ``PANDA_AUTH_DIR`` / ``X509_USER_PROXY`` dirname
    3. ``PILOT_SOURCE_DIR``
    4. ``PILOT_WORK_DIR``
    5. ``HOME``

    Args:
        auth_token: File name (primary token) or full path (refreshed token)
            to locate.
        key: If ``True``, search for the token *key* file and skip the
            refreshed-token prepend step.

    Returns:
        Absolute path to the first matching file, or an empty string if no
        candidate exists.
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

    # remove duplicates while preserving insertion-order priority
    paths = list(dict.fromkeys(paths))

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
    """Build the curl config file path and its URL-encoded content string.

    Args:
        url: Target URL; its basename is used as part of the config filename.
        data: Key/value payload to encode as ``data="key=value"`` lines.

    Returns:
        A two-element tuple ``(filename, strdata)`` where *filename* is the
        absolute path for the temporary curl config file under ``PILOT_HOME``
        and *strdata* is the file content to write.
    """
    strdata = ""
    for key in data:
        strdata += f'data="{urllib.parse.urlencode({key: data[key]})}"\n'
    jobid = f"_{data['job_id']}" if 'job_id' in list(data.keys()) else ""

    # write data to temporary config file
    filename = f"{os.getenv('PILOT_HOME')}/curl_{os.path.basename(url)}{jobid}.config"

    return filename, strdata


def get_curl_config_option(writestatus: bool, url: str, data: dict, filename: str) -> str:
    """Return the curl data option string for use in the curl command.

    When *writestatus* is truthy (the config file was written successfully),
    returns ``--config <filename> <url>``.  Otherwise falls back to inlining
    the URL-encoded data directly in the command string.

    Args:
        writestatus: ``True`` if the curl config file was written successfully.
        url: Target URL.
        data: Payload dict used for the URL-encoded fallback.
        filename: Path to the temporary curl config file.

    Returns:
        The ``dat`` string to append to the curl command.
    """
    if not writestatus:
        logger.warning('failed to create curl config file (will attempt to send JSON body inline)')
        try:
            json_payload = json.dumps(data) if data else ''
            dat = f"--data-raw {shlex.quote(json_payload)} {url}"
        except Exception:
            dat = shlex.quote(url + ('?' + urllib.parse.urlencode(data) if data else ''))
    else:
        dat = f'--config {filename} {url}'

    return dat


def execute_urllib(url: str, data: dict, plain: bool, secure: bool) -> urllib.request.Request:
    """Build a :class:`urllib.request.Request` object for the given URL and data.

    Args:
        url: Target URL.
        data: Payload dict to URL-encode and attach as the request body.
        plain: If ``True``, omit the ``Accept: application/json' \
              f'-H {shlex.quote("Content-Type: application/json")}`` header.
        secure: If ``True``, attach the ``User-Agent`` header from ``_ctx``.

    Returns:
        Configured :class:`urllib.request.Request` ready to be passed to
        :func:`~urllib.request.urlopen`.
    """
    req = urllib.request.Request(url, urllib.parse.urlencode(data).encode('ascii'))
    if not plain:
        req.add_header('Accept', 'application/json')
    if secure:
        req.add_header('User-Agent', _ctx.user_agent)

    return req


def get_urlopen_output(req: urllib.request.Request, context: ssl.SSLContext) -> tuple[int, str]:
    """Open *req* with :func:`~urllib.request.urlopen` and return the response.

    Args:
        req: Prepared :class:`urllib.request.Request` to send.
        context: SSL context to use, or ``None`` for no TLS verification.

    Returns:
        A two-element tuple ``(exitcode, output)`` where *exitcode* is ``0``
        on success or ``-1`` on any network/HTTP error, and *output* is the
        open response object (or an empty string on error).
    """
    exitcode = -1
    output = ""
    logger.debug('ok about to open url')
    try:
        output = urllib.request.urlopen(req, context=context)
    except urllib.error.HTTPError as exc:
        logger.warning(f'server error ({exc.code}): {exc.read()}')
    except (urllib.error.URLError, http_client.RemoteDisconnected, ssl.SSLError) as exc:
        logger.warning(f'connection error: {getattr(exc, "reason", exc)}')
    else:
        exitcode = 0
    logger.debug(f'ok url opened: exitcode={exitcode}')

    return exitcode, output


@dataclass(frozen=True)
class UpdateResult:
    """Normalized result from a server update attempt."""
    ok: bool
    attempts: int
    response: Optional[Dict[str, Any]]
    success: Optional[bool]
    status_code: Optional[int]
    command: Optional[str]
    message: str
    error_type: Optional[str] = None  # e.g. "transport", "protocol", "server"


def _parse_update_response(res: Dict[str, Any]) -> Tuple[bool, Optional[int], Optional[str], str]:
    """Parse a PanDA server update response into a normalised tuple.

    Args:
        res: Response dict from the server (must be a ``dict``).

    Returns:
        A four-element tuple ``(ok, status_code, command, message)`` where
        *ok* is ``True`` only when ``success`` is ``True`` and
        ``StatusCode`` is absent or zero.
    """
    success = res.get("success")
    message = res.get("message") or ""
    data = res.get("data", {})

    if success and not data:
        logger.debug(f"Server response success=True, message={message!r} (data not returned)")
        return True, None, None, message

    if not isinstance(data, dict):
        return False, None, None, f"Malformed response: data is {type(data)}"

    status_code = data.get("StatusCode")
    command = data.get("command")

    # Application-level success rule:
    # - success must be True
    # - StatusCode should be 0 (if present)
    if success is True and (status_code is None or status_code == 0):
        return True, status_code, command, message

    # Construct a meaningful message for caller/logging
    if success is False:
        return False, status_code, command, message or "Server returned success=False"
    if status_code is not None and status_code != 0:
        return False, status_code, command, message or f"Server returned StatusCode={status_code}"
    return False, status_code, command, message or "Server response indicated failure"


def send_update(update_function: str, data: Dict[str, Any], url: str, port: int, job: Optional[Any] = None, ipv: str = "IPv6", max_attempts: int = 2) -> UpdateResult:  # noqa
    """Send an update to the PanDA server and validate the response.

    This function distinguishes:
      - transport success: got a response dict back
      - application success: response has success=True and StatusCode==0 (if present)

    It also preserves the existing behavior around REACHED_MAXTIME and delayed
    heartbeats after completion.

    Args:
        update_function: Server endpoint/function name, e.g. 'api/v1/pilot/update_job'.
        data: Payload dict to send.
        url: Server URL (host or base).
        port: Server port.
        job: Job object (optional; used for completion logic and error annotations).
        ipv: 'IPv4' or 'IPv6'.
        max_attempts: Maximum number of attempts for retryable failures.

    Returns:
        UpdateResult with ok=True only when the server accepted the update.
    """
    attempt = 0
    last_res: Optional[Dict[str, Any]] = None

    # Preserve REACHED_MAXTIME behavior (as in your current code)
    if os.environ.get("REACHED_MAXTIME") and update_function.endswith("update_job"):
        data["job_status"] = "failed"
        if job:
            set_pilot_state(job=job, state="failed")
            job.completed = True
            msg = "the max batch system time limit has been reached"
            logger.warning(msg)
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.REACHEDMAXTIME, msg=msg)
            add_error_codes(data, job)

    # Prevent delayed heartbeats after completion (as in your current code)
    if job:
        if getattr(job, "completed", False) and getattr(job, "state", None) in {"running", "starting"}:
            logger.warning(f"Will not send job update for {job.state} since the job has already completed")
            return UpdateResult(
                ok=False,
                attempts=0,
                response=None,
                success=None,
                status_code=None,
                command=None,
                message="Job already completed; dropping delayed heartbeat",
                error_type="client",
            )

    while attempt < max_attempts:
        attempt += 1
        logger.info(f"Server update attempt {attempt}/{max_attempts}")

        try:
            pandaserver = get_panda_server(url, port)
        except Exception as exc:
            logger.warning(f"Exception in get_panda_server(): {exc}")
            # retryable (local/transient)
            if attempt < max_attempts:
                sleep(5)
            continue

        # Transport call
        res = send_request(pandaserver, update_function, data, job, ipv)
        last_res = res

        if res is None:
            # transport failed; retry
            logger.warning("No server response (None)")
            if attempt < max_attempts:
                sleep(config.Pilot.update_sleep)
            continue

        if not isinstance(res, dict):
            logger.warning(f"Malformed server response type: {type(res)}")
            if attempt < max_attempts:
                sleep(config.Pilot.update_sleep)
            continue

        ok, status_code, command, msg = _parse_update_response(res)
        if ok:
            return UpdateResult(
                ok=True,
                attempts=attempt,
                response=res,
                success=res.get("success"),
                status_code=status_code,
                command=command if isinstance(command, str) else None,
                message=msg or "",
                error_type=None,
            )

        # Not ok: decide retry behavior
        # If server explicitly says success=False or StatusCode!=0, usually retrying
        # may or may not help; we keep current behavior and retry up to max_attempts.
        logger.warning(
            f"Server update rejected: success={res.get('success')} StatusCode={status_code} message={msg!r}"
        )
        if attempt < max_attempts:
            sleep(config.Pilot.update_sleep)

    # Exhausted attempts
    # If we have a dict response, include parsed fields for caller
    if isinstance(last_res, dict):
        ok, status_code, command, msg = _parse_update_response(last_res)
        return UpdateResult(
            ok=False,
            attempts=max_attempts,
            response=last_res,
            success=last_res.get("success"),
            status_code=status_code,
            command=command if isinstance(command, str) else None,
            message=msg,
            error_type="server" if last_res.get("success") is False or (status_code not in (None, 0)) else "protocol",
        )

    return UpdateResult(
        ok=False,
        attempts=max_attempts,
        response=None,
        success=None,
        status_code=None,
        command=None,
        message="No valid server response after retries",
        error_type="transport",
    )


def _write_payload_file(payload: Dict[str, Any], tmpdir: str) -> str:
    """Write JSON payload to a temporary file.

    Args:
        payload: JSON-serializable dictionary.
        tmpdir: Directory where the file should be created.

    Returns:
        Path to the created payload file.
    """
    payload_path = os.path.join(tmpdir, "payload.json")
    with open(payload_path, "w", encoding="utf-8") as pf:
        json.dump(payload, pf, ensure_ascii=False)
        pf.flush()
        os.fsync(pf.fileno())
    return payload_path


def _build_curl_config(
    url: str,
    payload_path: str,
    user_agent: str,
    token: Optional[str],
    extra_headers: Optional[Dict[str, str]],
    tmpdir: str,
) -> str:
    """Create a curl config file for JSON POST.

    Args:
        url: Target URL.
        payload_path: Path to JSON payload file.
        user_agent: User-Agent string.
        token: Optional OIDC bearer token.
        extra_headers: Additional HTTP headers.
        tmpdir: Directory for config file.

    Returns:
        Path to the curl config file.
    """
    config_lines: list[str] = [f'url = "{url}"']
    config_lines.append(f'header = "User-Agent: {user_agent}"')
    config_lines.append('header = "Accept: application/json"')
    config_lines.append('header = "Content-Type: application/json"')

    headers = (extra_headers.copy() if extra_headers else {})

    if token:
        headers.setdefault("Authorization", f"Bearer {token}")

    for key, value in headers.items():
        safe_value = value.replace('"', '\\"')
        config_lines.append(f'header = "{key}: {safe_value}"')

    config_lines.append(f'data = @{payload_path}')
    config_lines.append("request = POST")

    config_path = os.path.join(tmpdir, "curl_request.config")

    with open(config_path, "w", encoding="utf-8") as cf:
        cf.write("\n".join(config_lines))
        cf.flush()
        os.fsync(cf.fileno())

    # Log config preview (mask token)
    try:
        with open(config_path, "r", encoding="utf-8", errors="replace") as f:
            preview = f.read()[:4096]
        if token:
            preview = preview.replace(token, "<TOKEN_REDACTED>")
        logger.debug(
            "curl config file (%s) contents (first 4096 chars): %s",
            config_path,
            preview,
        )
    except Exception:
        logger.debug("could not read back curl config file for logging")

    return config_path


def _build_curl_command(
    config_path: str,
    cacertfile: str,
    certfile: Optional[str],
    keyfile: Optional[str],
    connect_timeout: int,
    total_timeout: int,
    verify: bool,
    use_capath: bool,
    use_token_only: bool,
) -> list[str]:
    """Construct curl command list.

    Args:
        config_path: Path to curl config file.
        cacertfile: CA bundle file or directory.
        certfile: Client certificate file.
        keyfile: Client key file.
        connect_timeout: Curl connection timeout.
        total_timeout: Curl total timeout.
        verify: Whether to verify server certificate.
        use_capath: Use --capath instead of --cacert.
        use_token_only: If True, omit client certificate.

    Returns:
        List of curl command arguments.
    """
    cmd: list[str] = [
        "curl",
        "-sS",
        "--compressed",
        "--connect-timeout",
        str(connect_timeout),
        "--max-time",
        str(total_timeout),
    ]

    if not verify:
        cmd.append("--insecure")

    if use_capath:
        cmd.extend(["--capath", cacertfile])
    else:
        cmd.extend(["--cacert", cacertfile])

    if not use_token_only and certfile:
        cmd.extend(["--cert", certfile])

    if not use_token_only and keyfile:
        cmd.extend(["--key", keyfile])

    cmd.extend(["--config", config_path])

    return cmd


def _run_curl(cmd: list[str]) -> Tuple[int, str, str]:
    """Execute curl command.

    Args:
        cmd: Curl command as list.

    Returns:
        Tuple of (return_code, stdout, stderr).
    """
    try:
        logger.info(
            "executing curl fallback: %s",
            " ".join(shlex.quote(x) for x in cmd),
        )
    except Exception:
        logger.info("executing curl fallback (command hidden)")

    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    stdout = proc.stdout or ""
    stderr = proc.stderr or ""

    logger.debug("curl stdout (truncated): %s", stdout[:8192])
    logger.debug("curl stderr (truncated): %s", stderr[:8192])

    return proc.returncode, stdout, stderr


def _parse_curl_json(stdout: str) -> Union[str, Dict[str, Any]]:
    """Parse JSON output from curl, handling concatenated JSON.

    Args:
        stdout: Raw stdout string from curl.

    Returns:
        Parsed dictionary if JSON detected, otherwise raw string.
    """
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        try:
            decoder = json.JSONDecoder()
            obj, idx = decoder.raw_decode(stdout)
            leftover = stdout[idx:].strip()
            if leftover:
                logger.debug(
                    "extra data after first JSON object: %s",
                    leftover[:512],
                )
            return obj
        except Exception as exc:
            logger.warning(
                "failed to parse curl output as JSON: %s",
                exc,
            )
            return stdout


def send_request_with_token_via_curl_config(  # noqa: C901
    url: str,
    payload: Dict[str, Any],
    *,
    token: Optional[str] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    use_token_only: bool = False,
    certfile: Optional[str] = None,
    keyfile: Optional[str] = None,
    cacertfile: Optional[str] = None,
    use_capath: bool = False,
    connect_timeout: int = 100,
    total_timeout: int = 120,
    config_dir: Optional[str] = None,
    verify: bool = True,
) -> Union[str, Dict[str, Any]]:
    """Send JSON via curl using a curl config file and return parsed JSON or raw stdout.

    This function writes a payload.json and a curl_request.config in a temporary directory,
    invokes curl with `--config` (so the config contains the URL), and returns either:
      * a parsed dict (the first JSON object), or
      * the raw stdout string if parsing fails, or
      * an error string starting with "failed to send request:" on failure.

    Notes:
      - Do NOT add a default `Origin` header here; callers may add it if required by their server.
      - If `use_token_only` is True we do not add client cert/key options to the curl command.
      - If `token` is supplied it is added as Authorization header. Ensure the token is valid
        for the server and the expected VO (some servers require specific VO information).

    Args:
        url: Full target URL (including scheme and port).
        payload: JSON-serializable dict to send as body.
        token: Optional OIDC bearer token. If provided an 'Authorization: Bearer ...' header is added.
        extra_headers: Optional extra headers to include (dict).
        use_token_only: If True, do NOT pass client cert/key to curl (token-only mode).
        certfile: Path to client certificate (if using X.509).
        keyfile: Path to client key (if separate).
        cacertfile: Path to CA bundle file to use with curl (or directory if use_capath True).
        use_capath: If True, use `--capath` instead of `--cacert`.
        connect_timeout: curl --connect-timeout seconds.
        total_timeout: curl --max-time seconds.
        config_dir: Directory where temporary files should be created (defaults to PILOT_HOME or tmp dir).
        verify: Whether to verify server certificate. If False, pass --insecure to curl.

    Returns:
        dict parsed from server JSON, raw stdout string on parse failure, or error string
        beginning with "failed to send request:" on errors.
    """
    tmpdir = None
    payload_path = None
    config_path = None

    try:
        base_tmpdir = config_dir or os.getenv("PILOT_HOME") or tempfile.gettempdir()
        tmpdir = tempfile.mkdtemp(prefix="curl_cfg_", dir=base_tmpdir)

        # Write payload JSON
        payload_path = os.path.join(tmpdir, "payload.json")
        with open(payload_path, "w", encoding="utf-8") as pf:
            json.dump(payload, pf, ensure_ascii=False)
            pf.flush()
            os.fsync(pf.fileno())

        # Determine user agent (prefer _ctx if present, then config.Pilot)
        try:
            user_agent = getattr(_ctx, "user_agent", None) or getattr(config.Pilot, "user_agent", "pilot/unknown")
        except Exception:
            user_agent = "pilot/unknown"

        # Build headers dictionary (avoid duplicates; extra_headers takes precedence)
        headers: Dict[str, str] = {}
        headers["User-Agent"] = user_agent
        headers["Accept"] = "application/json"
        headers["Content-Type"] = "application/json"
        # NOTE: Do NOT add a default Origin header here. If the server requires an Origin header,
        # the caller should pass it explicitly in extra_headers.

        if extra_headers:
            # Normalize header keys and values to strings
            for k, v in extra_headers.items():
                if v is None:
                    continue
                headers[str(k)] = str(v)

        if token:
            # Authorization header added, but caller must ensure token is valid for the server
            headers["Authorization"] = f"Bearer {token}"

        # Build curl config lines (use --config so url is inside the config)
        config_lines: List[str] = [f'url = "{url}"']
        for k, v in headers.items():
            # Escape any double quotes in header values for the config file
            safe_val = v.replace('"', '\\"')
            config_lines.append(f'header = "{k}: {safe_val}"')

        config_lines.append(f'data = @{payload_path}')
        config_lines.append("request = POST")

        # Write curl config
        config_path = os.path.join(tmpdir, "curl_request.config")
        with open(config_path, "w", encoding="utf-8") as cf:
            cf.write("\n".join(config_lines))
            cf.flush()
            os.fsync(cf.fileno())

        # Log preview of config (redact token)
        try:
            cfg_preview = open(config_path, "r", encoding="utf-8", errors="replace").read()[:4096]
            if token:
                cfg_preview = cfg_preview.replace(token, "<TOKEN_REDACTED>")
            logger.debug(f"curl config file ({config_path}) contents (first 4096 chars): {cfg_preview}")
        except Exception:
            logger.debug("could not read back curl config file for logging")

        # Construct curl command: use --config only
        curl_cmd = [
            "curl",
            "-sS",
            "--compressed",
            "--connect-timeout",
            str(connect_timeout),
            "--max-time",
            str(total_timeout),
        ]

        if not verify:
            curl_cmd.append("--insecure")

        # CA verification choice
        if use_capath and cacertfile:
            curl_cmd.extend(["--capath", cacertfile])
        elif cacertfile:
            curl_cmd.extend(["--cacert", cacertfile])

        # Include client cert/key unless explicitly token-only
        if not use_token_only and certfile:
            curl_cmd.extend(["--cert", certfile])
        if not use_token_only and keyfile:
            curl_cmd.extend(["--key", keyfile])

        curl_cmd.extend(["--config", config_path])

        # Log command (redact token)
        try:
            safe_cmd = " ".join(shlex.quote(s) for s in curl_cmd)
            if token:
                safe_cmd = safe_cmd.replace(token, "<TOKEN_REDACTED>")
            logger.info(f"executing curl fallback: {safe_cmd}")
        except Exception:
            logger.info("executing curl fallback (command hidden)")

        proc = subprocess.run(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        retcode = proc.returncode

        logger.debug(f"curl stdout (truncated): {stdout[:8192]}")
        logger.debug(f"curl stderr (truncated): {stderr[:8192]}")

        if retcode != 0:
            if verify and ("self-signed" in stderr.lower() or "certificate" in stderr.lower() or "ssl certificate" in stderr.lower()):
                logger.warning(f"curl failed with certificate verification error. stderr (head): {'; '.join(stderr.splitlines()[:8])}")
            return f"failed to send request: curl exit {retcode}: {stderr[:2000]!s}"

        # Parse first JSON object robustly
        try:
            return json.loads(stdout)
        except json.JSONDecodeError:
            try:
                decoder = json.JSONDecoder()
                obj, idx = decoder.raw_decode(stdout)
                leftover = stdout[idx:].strip()
                if leftover:
                    logger.debug(f"extra data after first JSON object (first 512 chars): {leftover[:512]}")
                return obj
            except Exception as exc:
                logger.warning(f"json.loads() failed to parse output={stdout[:2000]!s}; raw_decode also failed: {exc}")
                return stdout

    except Exception as exc:
        logger.exception(f"unexpected exception while running curl config fallback: {exc}")
        return f"failed to send request: {exc}"

    finally:
        # Clean up temporary files/dir
        try:
            if payload_path and os.path.exists(payload_path):
                os.remove(payload_path)
        except Exception:
            pass
        try:
            if config_path and os.path.exists(config_path):
                os.remove(config_path)
        except Exception:
            pass
        try:
            if tmpdir and os.path.isdir(tmpdir):
                shutil.rmtree(tmpdir, ignore_errors=True)
        except Exception:
            pass


def make_cacert_with_server_cert(host: str, port: int, orig_cacert: str, out_dir: Optional[str] = None) -> str:
    """
    Create a new CA bundle by copying orig_cacert and appending the server certificate(s)
    obtained from openssl s_client -showcerts. Returns the path to the new CA file.

    Notes:
      - Does NOT modify orig_cacert.
      - Requires `openssl` to be available on the host.
      - Caller should inspect/log the created file for security/audit.
    """
    base_tmp = out_dir or tempfile.gettempdir()
    tmpdir = tempfile.mkdtemp(prefix="cacert_append_", dir=base_tmp)
    server_chain = os.path.join(tmpdir, "server-chain.pem")
    new_cacert = os.path.join(tmpdir, "cacert_with_server.pem")

    # Run openssl s_client to get the server chain
    cmd = f"openssl s_client -connect {shlex.quote(host)}:{int(port)} -showcerts"
    proc = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, input="")
    stdout = proc.stdout or ""
    stderr = proc.stderr or ""
    if not stdout:
        # raise so caller can log and decide
        raise RuntimeError(f"openssl s_client produced no output: {stderr[:2000]!s}")

    # Extract PEM blocks
    pem_blocks = []
    in_block = False
    block_lines = []
    for line in stdout.splitlines():
        if "-----BEGIN CERTIFICATE-----" in line:
            in_block = True
            block_lines = [line]
        elif "-----END CERTIFICATE-----" in line and in_block:
            block_lines.append(line)
            pem_blocks.append("\n".join(block_lines) + "\n")
            in_block = False
        elif in_block:
            block_lines.append(line)

    if not pem_blocks:
        raise RuntimeError("no certificates found in server chain output")

    # Write server-chain.pem
    with open(server_chain, "w", encoding="utf-8") as f:
        for b in pem_blocks:
            f.write(b)

    # Copy orig cacert and append the server chain (binary append)
    shutil.copy(orig_cacert, new_cacert)
    with open(new_cacert, "ab") as out_f, open(server_chain, "rb") as in_f:
        out_f.write(b"\n")
        out_f.write(in_f.read())

    # Secure permissions
    try:
        os.chmod(new_cacert, 0o600)
    except Exception:
        pass

    # Log path for inspection by caller
    logger.info("created temporary cacert with server cert appended: %s", new_cacert)
    return new_cacert


def send_request(pandaserver: str, update_function: str, data: dict, job: JobData, ipv: str) -> Optional[dict]:
    """Send a single request to the PanDA server and return the response.

    Tries :func:`request2` (urllib) first; falls back to :func:`request`
    (curl) for legacy endpoints.  Sensitive fields in the response are masked
    before logging.

    Args:
        pandaserver: Fully-qualified PanDA server URL (e.g.
            ``'https://pandaserver.cern.ch'``).
        update_function: Endpoint path, e.g. ``'api/v1/pilot/update_job'``.
        data: Payload dict to send.
        job: Job object used to annotate log messages, or ``None``.
        ipv: Internet-protocol version passed to the curl fallback.

    Returns:
        Server response dict, or ``None`` if both backends failed.
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
        res = request2(f'{path}', json_body=data, panda=True)
    except Exception as exc:
        logger.warning(f'exception caught in https.request2(): {exc}')

    # test fallback to curl
    #if "update_job" in update_function:
    #    res = None
    if not res:
        #if "api/v" in update_function:
        #    return None
        logger.warning('failed to send request using urllib based request2(), will try curl based request()')
        try:
            res = request(f'{pandaserver}/{update_function}', data=data, ipv=ipv)
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
        # Determine the nested dict that contains 'pilotSecrets'
        #logger.debug(f"res={res}")
        log_res, pilot_secrets = mask_sensitive_response(res)
        logger.info(f'server responded with: res = {log_res}')
    else:
        logger.warning(f'server {update_function} request failed both with urllib and curl')

    return res


def get_panda_server(url: str, port: int, update_server: bool = True) -> str:
    """Resolve and optionally randomise the PanDA server URL.

    When *url* is non-empty it is parsed and normalised; otherwise the value
    from the pilot config is used.  If *update_server* is ``True`` (default)
    the hostname ``pandaserver.cern.ch`` is replaced with a randomly chosen
    IP address resolved via DNS, providing basic load balancing.

    Args:
        url: Server URL from pilot options (port may be embedded or supplied
            separately); pass an empty string to use the config default.
        port: Port number to embed in the URL when not already present in
            *url*.
        update_server: If ``True``, attempt DNS randomisation of the default
            PanDA server address.

    Returns:
        Fully-qualified server URL string ready for use in requests.
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


def add_error_codes(data: dict, job: JobData) -> None:
    """Populate *data* with error-code fields from *job*.

    Extracts pilot error codes/diagnostics, transaction exit code, and
    executable error code from the job object and writes them into *data*
    ready for submission to the PanDA server.  Timestamps are stripped from
    diagnostic strings.  SIGTERM errors on Kubernetes resources are remapped
    to ``PREEMPTION``.

    Args:
        data: Mutable payload dict to update in-place.
        job: Job object providing error codes and diagnostics.
    """
    # error codes
    pilot_error_code = job.piloterrorcode
    pilot_error_codes = job.piloterrorcodes
    if pilot_error_codes != []:
        logger.warning(f'pilot_error_code(s) = {pilot_error_codes} (will report primary/first error code)')
        data['pilot_error_code'] = int(pilot_error_codes[0])
    else:
        data['pilot_error_code'] = int(pilot_error_code)

    def remove_timestamp(log_entry: str) -> str:
        """Strip a ``YYYY-MM-DD HH:MM:SS[,mmm]`` timestamp from a log entry.

        Args:
            log_entry: Log entry string that may contain a leading timestamp.

        Returns:
            The entry with the timestamp removed and surrounding whitespace
            stripped.
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
                logger.warning(f'pilot_error_diag(s) contains non-string value: {diag} (converted to string)')
        pilot_error_diags = pilot_error_diags_cleaned

        logger.warning(f'pilot_error_diag(s) = {pilot_error_diags} (will report primary/first error diag)')
        data['pilot_error_diag'] = pilot_error_diags[0]
    else:
        data['pilot_error_diag'] = pilot_error_diag

    # special case for SIGTERM failures on Kubernetes resources
    if data.get('pilot_error_code') == errors.SIGTERM:
        if is_kubernetes_resource():
            logger.warning('resetting SIGTERM error to PREEMPTION for Kubernetes resource')
            data['pilot_error_code'] = errors.PREEMPTION
            data['pilot_error_diag'] = errors.get_error_code(errors.PREEMPTION)

    data['trans_exit_code'] = job.transexitcode
    data['exe_error_code'] = job.exeerrorcode
    data['exe_error_diag'] = job.exeerrordiag


def get_server_command(url: str, port: int, cmd: str = 'api/v1/pilot/acquire_jobs') -> str:
    """Build the full URL for a PanDA server API command.

    Appends *port* to *url* when it is not already present, ensures an
    ``https://`` scheme, applies DNS randomisation via :func:`get_panda_server`,
    and returns the endpoint URL.

    Args:
        url: PanDA server base URL from pilot options, or empty string to use
            the config default.
        port: Server port number.
        cmd: API path to append (default ``'api/v1/pilot/acquire_jobs'``).

    Returns:
        Fully-qualified URL string for the requested API endpoint.
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

    if "api/v" in cmd:
        return f'{url}/{cmd}'
    return f'{url}/server/panda/{cmd}'


def get_headers(use_oidc_token: bool, auth_token_content: Optional[str] = None,
                auth_origin: Optional[str] = None,
                content_type: str = "application/json", accept: bool = False) -> dict:
    """Build the HTTP request headers dict.

    Always includes ``User-Agent``.  When *use_oidc_token* is ``True``, adds
    ``Authorization: Bearer <token>`` and ``Origin`` headers.  When
    *content_type* is non-empty, adds ``Content-Type`` (and optionally
    ``Accept`` when *accept* is ``True``).

    Args:
        use_oidc_token: If ``True``, include OIDC bearer-token auth headers.
        auth_token_content: Raw bearer token string (required when
            *use_oidc_token* is ``True``).
        auth_origin: Token origin string for the ``Origin`` header (required
            when *use_oidc_token* is ``True``).
        content_type: Value for the ``Content-Type`` header; pass an empty
            string to omit it.
        accept: If ``True`` and *content_type* is non-empty, also set
            ``Accept`` to *content_type*.

    Returns:
        Dict of HTTP header name/value pairs ready to pass to
        :class:`urllib.request.Request`.
    """
    if use_oidc_token:
        headers = {
            "Authorization": f"Bearer {auth_token_content}",
            # "Accept": "application/json",  # what is the difference with "Content-Type"? See else: below
            "Origin": auth_origin,
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
    """Create and return a bare :class:`ssl.SSLContext`.

    Uses ``ssl.SSLContext(protocol=None)`` on Python 3.10+ and falls back to
    the no-argument form on older ssl versions.  The caller is responsible for
    configuring certificate verification and loading cert chains.

    Returns:
        A new, unconfigured :class:`ssl.SSLContext`.
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


def get_auth_token_content2(auth_token: str, key: bool = False) -> str:
    """Read and return the content of an OIDC token file.

    Locates the token via :func:`locate_token` and reads its content.  Returns
    an empty string and logs a warning if the file cannot be found or read.

    Args:
        auth_token: Token file name or path to locate and read.
        key: If ``True``, locate the token key file rather than the token
            itself.

    Returns:
        File content as a string, or an empty string on failure.
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
    """urllib HTTP handler that forces connections to use IPv4 (``AF_INET``).

    Install via :func:`install_ipv4_opener` to override the default opener
    when the environment variable ``PILOT_IP_VERSION`` is ``'IPv4'``.
    """

    def http_open(self, req):
        return self.do_open(self._create_connection, req)

    def _create_connection(self, host, port=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, source_address=None):
        return socket.create_connection((host, port), timeout, source_address, family=socket.AF_INET)


def _merge_query(url: str, params: Dict[str, Any]) -> str:
    """Merge *params* into the query string of *url*.

    Existing query parameters are preserved; *params* values override them.
    ``None`` values are skipped.  Lists and tuples produce repeated query
    arguments (``doseq=True``).

    Args:
        url: Base URL that may already contain a query string.
        params: Additional query parameters to merge in.

    Returns:
        URL with the merged query string.
    """
    if not params:
        return url

    parts = urllib.parse.urlsplit(url)
    existing = urllib.parse.parse_qs(parts.query, keep_blank_values=True)

    # Add/override with new params
    for k, v in params.items():
        if v is None:
            continue
        if isinstance(v, (list, tuple)):
            existing[k] = [str(x) for x in v]
        else:
            existing[k] = [str(v)]

    new_query = urllib.parse.urlencode(existing, doseq=True)
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


# --- small helpers to reduce complexity in request2() ---

def _decide_method(json_body: Optional[Dict[str, Any]], method: Optional[str]) -> str:
    """Return the HTTP method to use for a request.

    Args:
        json_body: Request body dict; its presence implies ``POST``.
        method: Caller-supplied method string, or ``None`` to infer.

    Returns:
        Upper-cased HTTP method string: ``'GET'`` when *json_body* is ``None``
        and *method* is unset, ``'POST'`` when *json_body* is provided, or the
        upper-cased *method* string if explicitly given.
    """
    if method:
        return method.upper()
    return "GET" if json_body is None else "POST"


def _get_auth_and_headers(url: str, panda: bool) -> Tuple[Dict[str, str], bool]:
    """Build request headers and determine whether OIDC auth is active.

    Reads the local OIDC token info, optionally reads the token content, and
    delegates to :func:`get_headers` to assemble the final header dict.  Never
    raises; logs a warning if the token content cannot be read.

    Args:
        url: Target URL; used to decide whether to include ``Accept`` header
            (present when the path contains ``'api/v'``).
        panda: If ``True``, OIDC auth is enabled when a token and origin are
            both available.

    Returns:
        A two-element tuple ``(headers, use_oidc_token)`` where *headers* is
        ready for use in a :class:`urllib.request.Request` and
        *use_oidc_token* indicates whether OIDC auth was applied.
    """
    auth_token, auth_origin = get_local_oidc_token_info()

    if "CERN-PTEST" in os.environ.get('PILOT_SITENAME', ''):
        logger.debug('switched off OIDC tokens for CERN-PTEST (2)')
        use_oidc_token = False
    else:
        use_oidc_token = bool(auth_token and auth_origin and panda)

    auth_token_content = get_auth_token_content(auth_token) if use_oidc_token else ""
    if not auth_token_content and use_oidc_token:
        logger.warning("OIDC_AUTH_TOKEN/PANDA_AUTH_TOKEN content could not be read")
        # calling function will decide what to do (we return headers but caller may bail)
    accept = True if "api/v" in url else False
    headers = get_headers(use_oidc_token, auth_token_content, auth_origin, accept=accept)
    return headers, use_oidc_token


def _prepare_body_and_headers(url: str, json_body: Optional[Dict[str, Any]], compressed: bool, headers: Dict[str, str]) -> Optional[bytes]:
    """
    Prepare request body bytes and adjust headers.

    - If json_body is None -> returns None.
    - If 'api/v' in URL and compressed=True -> gzip JSON and set Content-Encoding.
    - Otherwise returns UTF-8 encoded JSON bytes.
    """
    if json_body is None:
        return None

    headers.setdefault("Content-Type", "application/json")
    payload = json.dumps(json_body).encode("utf-8")
    if compressed and "api/v" in url:
        headers["Content-Encoding"] = "gzip"
        rdata_out = BytesIO()
        with GzipFile(fileobj=rdata_out, mode="w") as f_gzip:
            f_gzip.write(payload)
        return rdata_out.getvalue()
    return payload


def _get_ssl_context(use_oidc_token: bool) -> ssl.SSLContext:
    """
    Create an SSLContext configured with the module CA cert if available.

    If OIDC is used we still want to verify server certificates; client cert
    loading is handled separately by the caller when needed.
    """
    if use_oidc_token:
        pass  # to bypass pylint error
    capath_val = getattr(_ctx, "capath", None)
    # cacert_val = getattr(_ctx, "cacert", None)

    try:
        # Pass capath (grid CA dir) AND cafile together — matches https_setup()
        context = ssl.create_default_context(
            capath=capath_val or None,
            cafile=None,  # user proxy is NOT a CA file; don't pass it here
        )
        # Optionally also load certifi as a fallback if the grid CA dir is absent
        if not capath_val:
            try:
                if certifi:
                    context.load_verify_locations(cafile=certifi.where())
            except Exception:
                pass
    except Exception as exc:
        logger.warning(f"failed to create SSL context with capath={capath_val}: {exc}, falling back to default")
        context = ssl.create_default_context()

    return context


def _parse_response_text(text: str) -> Union[str, Dict[str, Any]]:
    """
    Parse a textual server response into a dict where possible, otherwise return string.

    Attempts JSON, query-string style, then Python-literal dict parsing as fallbacks.
    """
    text = (text or "").strip()
    if text == "Succeeded":
        return {"StatusCode": "0"}

    # Try JSON first (most common)
    if text.startswith("{") and text.endswith("}"):
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass

    # Try query-string style: "a=1&b=2"
    try:
        from urllib.parse import parse_qs

        query_dict = parse_qs(text)
        if query_dict:
            return {k: v[0] if len(v) == 1 else v for k, v in query_dict.items()}
    except Exception:
        pass

    # Try to salvage python dict-like text
    try:
        import ast

        maybe = ast.literal_eval(text)
        if isinstance(maybe, dict):
            return maybe
    except Exception:
        pass

    # Last resort: return raw string
    return text


def _ensure_numeric_fields(payload: Optional[Dict[str, Any]]) -> None:
    """
    Normalize common numeric fields to integers if they are strings.

    This is a best-effort helper to avoid server-side type errors for fields
    like 'memory' or 'disk_space' that sometimes arrive as strings.
    """
    if not isinstance(payload, dict):
        return
    for key in ("memory", "disk_space", "core_count", "attempt_nr", "attempt_number", "worker_id"):
        if key in payload:
            v = payload.get(key)
            if isinstance(v, str):
                try:
                    # try to remove commas and plus signs if present
                    cleaned = v.replace(",", "").split("+")[0]
                    payload[key] = int(cleaned)
                except Exception:
                    # leave as-is if conversion fails
                    pass


def request2(url: str = "", *, params: Optional[Dict[str, Any]] = None, json_body: Optional[Dict[str, Any]] = None, secure: bool = True, compressed: bool = True, panda: bool = False, method: Optional[str] = None) -> Union[str, Dict[str, Any]]:  # noqa
    """
    Send an HTTPS request that supports both OIDC token and X.509 client cert auth.

    This function:
    - Decides method (GET if no body else POST) unless overridden
    - Merges query params into the URL
    - Prepares headers via `get_headers()` (uses OIDC token if panda=True)
    - Optionally gzips JSON bodies for API endpoints (and retries once w/o gzip on 5xx)
    - Loads X.509 client certificate into the SSL context when OIDC token is not used
    - Parses response text into dicts where possible

    Args:
        url: Target URL.
        params: Query-string parameters merged into URL.
        json_body: Dict to JSON-serialize as request body.
        secure: Whether to verify server TLS certificates. If False, TLS verification is disabled.
        compressed: If True, gzip bodies for API endpoints where applicable.
        panda: If True, attempt to use OIDC token authentication (if available).
        method: Optional HTTP method override.

    Returns:
        Parsed response (dict) when possible, otherwise raw string. On network issues returns
        an error string starting with "failed to send request:".
    """
    params = params or {}
    method = _decide_method(json_body, method)

    ipv = os.environ.get("PILOT_IP_VERSION")
    logger.info(f"url = {url}, secure = {secure}, compressed = {compressed}, ipv = {ipv}, method = {method}")

    # Ensure HTTPS setup is available
    if not getattr(_ctx, "cacert", None):
        logger.debug('calling https_setup to ensure SSL context is configured')
        https_setup(None, get_pilot_version())

    # Determine OIDC usage
    auth_token_name, auth_origin = get_local_oidc_token_info()
    if "CERN-PTEST" in os.environ.get('PILOT_SITENAME', ''):
        logger.debug('switched off OIDC tokens for CERN-PTEST')
        use_oidc_token = False
    else:
        use_oidc_token = bool(auth_token_name and auth_origin and panda)
    logger.info("will use OIDC token authentication" if use_oidc_token else "will not use OIDC token authentication")

    auth_token_content = ""
    if use_oidc_token:
        auth_token_content = get_auth_token_content(auth_token_name)
        if not auth_token_content:
            logger.warning(f"OIDC token requested but token content unreadable for token '{auth_token_name}'")
            return ""

    # Only add Accept if new API is used
    accept = True if "api/v" in url else False
    headers = get_headers(use_oidc_token, auth_token_content, auth_origin, accept=accept)

    # Merge params into URL
    url = _merge_query(url, params)

    # Disallow GET with body: explicit error (prevents silent misuse)
    if method and method.upper() == "GET" and json_body is not None:
        raise ValueError("GET requests must not include json_body; use 'params=' instead")

    # Normalize some numeric fields to ints when possible (best-effort)
    try:
        _ensure_numeric_fields(json_body)
    except Exception:
        logger.debug("numeric field normalization skipped or failed")

    # Log preview of outgoing JSON body for debugging
    if json_body is not None:
        try:
            logger.debug(f"outgoing JSON payload preview: {json.dumps(json_body, ensure_ascii=False)[:1024]}")
        except Exception:
            logger.debug("failed to create outgoing payload preview")

    # Prepare body bytes and headers (helper will set Content-Encoding if gzipped)
    data_bytes = _prepare_body_and_headers(url, json_body, compressed, headers)

    logger.info(f"params = {params}")
    logger.info(f"json_body = {json_body}")
    logger.info(f"headers = {hide_token(headers.copy())}")
    logger.debug(f"data_bytes length = {len(data_bytes) if data_bytes is not None else 0}")

    # Build urllib request
    req = urllib.request.Request(url, data=data_bytes, headers=headers, method=method)

    # SSL context and certificate handling
    ssl_context = _get_ssl_context(use_oidc_token)

    # If not using OIDC token, attempt to load X.509 client cert (mTLS) into context
    if not use_oidc_token:
        try:
            if getattr(_ctx, "cacert", None):
                ssl_context.load_cert_chain(certfile=_ctx.cacert, keyfile=_ctx.cacert)
                logger.debug(f"loaded X.509 client cert/key from '{_ctx.cacert}'")
        except Exception as exc:
            logger.warning(f"failed to load X.509 client cert/key ('{getattr(_ctx, 'cacert', None)}'): {exc}")

    if not secure:
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    # IP family handling
    if ipv == "IPv4":
        logger.info("will use IPv4 in server communication")
        install_ipv4_opener()
    else:
        logger.info("will use IPv6 in server communication")

    # Send request
    ret_text: Optional[str] = None
    try:
        logger.debug("sending request to server")
        with urllib.request.urlopen(req, context=ssl_context, timeout=config.Pilot.http_maxtime) as response:
            logger.info(f"response.status={response.status}, response.reason={response.reason}")
            ret_text = response.read().decode("utf-8")
        logger.debug("sent request to server")
    except urllib.error.HTTPError as http_exc:
        # HTTPError may include body - attempt to read it for diagnostics
        try:
            body = http_exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = "<no body available>"

        code = getattr(http_exc, "code", "<no-code>")
        reason = getattr(http_exc, "reason", "<no-reason>")
        logger.warning(f"failed to send request: HTTP Error {code}: {reason}. Server response body: {body[:2048]}")

        # Retry once without gzip if server-side 5xx and we originally sent gzip
        try:
            code_int = int(code)
        except Exception:
            code_int = 0

        if 500 <= code_int < 600 and json_body is not None and compressed:
            logger.info("server returned 5xx -> retrying once without gzip compression")
            try:
                headers_retry = dict(headers)
                headers_retry.pop("Content-Encoding", None)
                payload_retry = json.dumps(json_body).encode("utf-8")
                logger.debug(f"retry JSON payload preview: {payload_retry.decode('utf-8', errors='replace')[:1024]}")
                req_retry = urllib.request.Request(url, data=payload_retry, headers=headers_retry, method=method)
                with urllib.request.urlopen(req_retry, context=ssl_context, timeout=config.Pilot.http_maxtime) as response:
                    logger.info(f"[retry no-gzip] response.status={response.status}, response.reason={response.reason}")
                    ret_text = response.read().decode("utf-8")
                logger.debug("sent retry (no-gzip) to server")
            except Exception as retry_exc:
                logger.warning(f"retry without gzip failed: {retry_exc}")
                return f"failed to send request: HTTP Error {code}: {reason}"
        else:
            return f"failed to send request: HTTP Error {code}: {reason}"

    except (urllib.error.URLError, http_client.RemoteDisconnected, TimeoutError, ssl.SSLError) as exc:
        ret = f"failed to send request: {exc}"
        logger.warning(ret)
        return ret

    # Parse textual response into dict when possible
    if secure and isinstance(ret_text, str):
        return _parse_response_text(ret_text)

    return ret_text


def install_ipv4_opener() -> None:
    """Install a urllib opener that forces all connections to use IPv4.

    Builds an opener with :class:`IPv4HTTPHandler` (and a proxy handler if
    ``http_proxy``/``all_proxy`` are set in the environment) and registers it
    as the global default opener via :func:`urllib.request.install_opener`.
    """
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
    """Replace the bearer token value in *headers* with a redaction placeholder.

    Modifies the dict in-place and also returns it so callers can use it
    inline (e.g. ``logger.info(f"headers={hide_token(headers.copy())}")``.
    Always pass a *copy* of the real headers to avoid permanently redacting
    the live dict.

    Args:
        headers: Copy of the request headers dict.

    Returns:
        The same dict with ``Authorization`` replaced by
        ``'Bearer ********'`` if present.
    """
    if 'Authorization' in headers:
        headers['Authorization'] = 'Bearer ********'

    return headers


def request3(url: str, data: dict = None) -> str:
    """Send an HTTPS POST request using the ``requests`` library.

    Requires both the ``requests`` and ``certifi`` optional packages; returns
    an empty string with a warning if either is unavailable.  Uses
    ``_ctx.cacert`` for the server certificate and ``certifi.where()`` for the
    client certificate.

    Args:
        url: Target URL.
        data: Payload dict to serialise as JSON.

    Returns:
        Response text on success, or an empty string on failure.
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
    """Upload the contents of a local file to *url* via HTTP POST.

    Reads the file as raw bytes and sends it with ``Content-Type:
    application/json``.  Returns ``True`` only when the server responds with
    the literal string ``'ok'``.

    Args:
        url: Destination URL for the POST request.
        path: Local filesystem path of the file to upload.

    Returns:
        ``True`` if the server responded with ``'ok'``, ``False`` otherwise.
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
    """Download the content at *url* and return it as a string.

    Uses the ``ctx`` SSL context (which includes client certificate chain).
    Primarily used to download OIDC tokens from the PanDA server.

    Args:
        url: URL to fetch.
        timeout: Socket timeout in seconds (default 20).
        headers: Optional HTTP headers dict; defaults to ``{'User-Agent': …}``
            when ``None``.

    Returns:
        Response body as a string, or an empty string on failure.
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
        logger.warning(f"error occurred with urlopen: {getattr(exc, 'reason', exc)}")
        # Handle the error, set content to None or handle as needed
        content = ""

    return content


def refresh_oidc_token(auth_token: str, auth_origin: str, url: str, port: int) -> bool:
    """
    Refresh the OIDC access token by downloading a new token from the PanDA server
    and overwriting the existing token file.

    The token key is expected to be provided in the environment variable
    PANDA_AUTH_TOKEN_KEY and the token itself in `auth_token`.

    Args:
        auth_token: Path to the local auth token file to overwrite (or token identifier
            used by `get_auth_token_content()` and `rename()` in the pilot codebase).
        auth_origin: Token origin string used for authentication headers.
        url: PanDA server base URL.
        port: PanDA server port.

    Returns:
        True if a new token was downloaded and stored successfully, otherwise False.
    """
    # first get the token key
    token_key = os.environ.get("PANDA_AUTH_TOKEN_KEY")
    if not token_key:
        logger.warning("PANDA_AUTH_TOKEN_KEY is not set - will not be able to download a new token")
        return False

    panda_token_key = get_auth_token_content(token_key, key=True)
    if not panda_token_key:
        logger.warning("failed to get panda_token_key - will not be able to download a new token")
        return False

    logger.info(f"read token key: {token_key}")

    # now get the actual token content (used to authenticate the refresh call)
    auth_token_content = get_auth_token_content(auth_token)
    if not auth_token_content:
        logger.warning(f"failed to get auth token content for {auth_token}")
        return False

    headers = get_headers(True, auth_token_content, auth_origin, content_type=None)
    server_command = get_server_command(url, port, cmd="api/v1/creds/get_access_token")

    # the client name and token key should be added to the URL as parameters
    server_command += f"?client_name=pilot_server&token_key={panda_token_key}"
    logger.info(f"server_command: {server_command}")
    content = download_file(server_command, headers=headers)
    if not content:
        logger.warning(f'failed to download data from "{url}" resource')
        return False

    return handle_file_content(content, auth_token)


def _extract_token_from_refresh_response(payload: Dict[str, Any]) -> str:
    """
    Extract the refreshed token from either new-style or old-style response payloads.

    New-style example:
        {"success": true, "message": "", "data": {"access_token": "<TOKEN>"}}

    Old-style example (historical pilot convention):
        {"StatusCode": 0, "ErrorDialog": "", "userProxy": "<TOKEN>"}

    Returns:
        The token string.

    Raises:
        ValueError: if the response indicates an error or token cannot be found.
    """
    # Old-style
    if "StatusCode" in payload:
        statuscode = payload.get("StatusCode", 0)
        diagnostics = payload.get("ErrorDialog", "")
        if statuscode != 0:
            raise ValueError(f"failed to get new token: StatusCode={statuscode}, ErrorDialog={diagnostics!r}")
        token = payload.get("userProxy") or payload.get("access_token") or payload.get("token")
        if not token:
            raise ValueError("old-style response missing token field (expected userProxy)")
        return str(token)

    # New-style
    if "success" in payload:
        if payload.get("success") is not True:
            msg = payload.get("message", "unknown error")
            raise ValueError(f"failed to get new token: success=False, message={msg!r}")

        data = payload.get("data") or {}
        if not isinstance(data, dict):
            raise ValueError(f"unexpected 'data' type in response: {type(data)}")

        token = data.get("access_token") or data.get("token") or data.get("user_proxy") or data.get("userProxy")
        if not token:
            raise ValueError("new-style response missing token field in data (expected access_token/token)")
        return str(token)

    raise ValueError("unrecognized token refresh response format")


def _resolve_token_path(auth_token: str) -> str:
    """Resolve a token name or path to an absolute filesystem path.

    Uses :func:`locate_token` so the same candidate-directory search used
    when *reading* a token is also used when *writing* one.  This prevents
    a bare filename (e.g. ``"panda_token"``) from being resolved relative to
    the pilot's CWD rather than the directory that actually holds the token.

    Args:
        auth_token: Token filename or path as supplied by the caller.

    Returns:
        Absolute path if found, otherwise the original value unchanged.
    """
    resolved = locate_token(auth_token)
    if resolved:
        if resolved != auth_token:
            logger.debug(f'resolved token path: {auth_token!r} -> {resolved!r}')
        return resolved
    # locate_token already logs a warning; fall back to the original value
    # (may still work if auth_token is already a valid absolute path)
    logger.warning(f'could not resolve token path for {auth_token!r} - will use as-is')
    return auth_token


def _parse_token_response(content: Union[bytes, str]) -> Optional[str]:
    """Parse a PanDA token-refresh response and return the raw token string.

    Accepts both the new JSON API format and the legacy Python-dict-string
    format returned by very old endpoints.

    Args:
        content: Raw response bytes or text from the PanDA server.

    Returns:
        The token string on success, or ``None`` if parsing fails.
    """
    text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content

    payload: Optional[Dict[str, Any]] = None
    try:
        payload = json.loads(text)
    except Exception:
        try:
            import ast
            maybe = ast.literal_eval(text)
            if isinstance(maybe, dict):
                payload = maybe
        except Exception:
            pass

    if not isinstance(payload, dict):
        logger.warning(f"failed to parse token refresh response as dict; raw={text!r}")
        return None

    try:
        return _extract_token_from_refresh_response(payload)
    except ValueError as exc:
        logger.warning(str(exc))
        return None


def _atomic_write_token(token: str, auth_token: str) -> bool:
    """Write *token* to disk, atomically replacing *auth_token*.

    The token is written to a sibling temp file first and then renamed over
    the destination so readers never see a partial write.  The temp file is
    placed in the same directory as the destination to guarantee both are on
    the same filesystem (a requirement for ``os.rename`` atomicity).

    On success, sets ``OIDC_REFRESHED_AUTH_TOKEN`` to *auth_token* and logs
    the file size and modification time.

    Args:
        token: Token string to persist.
        auth_token: Absolute path of the token file to overwrite.

    Returns:
        ``True`` on success, ``False`` otherwise.
    """
    token_dir = os.path.dirname(auth_token) or os.environ.get("PILOT_HOME", "")
    tmp_path = os.path.join(token_dir, "tmp_refreshed_token")

    try:
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(token)
    except IOError as exc:
        logger.warning(f"failed to write data to file {tmp_path}: {exc}")
        return False

    if not rename(tmp_path, auth_token):
        logger.warning(f"failed to rename {tmp_path} to {auth_token}")
        return False

    logger.info(f"saved token data in file {auth_token}, length={len(token) / 1024.0:.1f} kB")
    os.environ["OIDC_REFRESHED_AUTH_TOKEN"] = auth_token

    mtime = get_modification_time(auth_token)
    if mtime:
        logger.info(f"{os.path.basename(auth_token)} modification time: {ctime(mtime)}")
    else:
        logger.warning(f"failed to get modification time for {auth_token}")

    return True


def handle_file_content(content: Union[bytes, str], auth_token: str) -> bool:
    """Handle the content of the downloaded token payload and overwrite the existing token.

    The refreshed token is written to a temporary file first and then renamed over
    the original `auth_token` (overwrite).

    Args:
        content: The raw response content (bytes or str) returned from the server.
        auth_token: Path/name of the token file to overwrite.

    Returns:
        True if the token was parsed and saved successfully, otherwise False.
    """
    auth_token = _resolve_token_path(auth_token)
    token = _parse_token_response(content)
    if token is None:
        return False
    return _atomic_write_token(token, auth_token)


def refresh_oidc_token_old(auth_token: str, auth_origin: str, url: str, port: int) -> bool:
    """Refresh the OIDC token using the legacy server endpoint (old version).

    Args:
        auth_token: Token name/path to refresh.
        auth_origin: Token origin string used in the auth header.
        url: PanDA server base URL.
        port: PanDA server port.

    Returns:
        ``True`` if the token was refreshed and saved successfully, ``False``
        otherwise.
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


def handle_file_content_old(content: Union[bytes, str], auth_token: str) -> bool:
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
                    logger.info(f'saved token data in file {auth_token}, length={len(content) / 1024.:.1f} kB')
                    os.environ['OIDC_REFRESHED_AUTH_TOKEN'] = auth_token
                else:
                    logger.warning(f'failed to rename {path} to {auth_token}')

                mtime = get_modification_time(auth_token)
                if mtime:
                    logger.info(f'{os.path.basename(auth_token)} modification time: {ctime(mtime)}')
                else:
                    logger.warning(f'failed to get modification time for {auth_token}')

    return status


def update_local_oidc_token_info(url: str, port: int) -> None:
    """Refresh the local OIDC token if one is configured.

    Reads the current token info via :func:`get_local_oidc_token_info` and,
    when both token and origin are available, calls :func:`refresh_oidc_token`
    to download a fresh token from the PanDA server.  On success the new
    token's ``iat`` / ``exp`` fields are logged via :func:`decode_jwt_payload`.

    Args:
        url: PanDA server base URL.
        port: PanDA server port.
    """
    auth_token, auth_origin = get_local_oidc_token_info()
    if auth_token and auth_origin:
        logger.debug('updating OIDC token info')
        status = refresh_oidc_token(auth_token, auth_origin, url, port)
        if not status:
            logger.warning('failed to refresh OIDC token')
        else:
            logger.info('OIDC token has been refreshed')

            # print out the token expiry time and issue time
            path = locate_token(auth_token)
            try:
                _ = decode_jwt_payload(path, return_times=True)
            except ValueError as exc:
                logger.warning(f'failed to decode JWT payload: {exc}')
    else:
        logger.debug('no OIDC token info to update')


def decode_jwt_payload(token_or_path: str, return_times: bool = True) -> dict:
    """Decode and return the payload section of a JWT (OIDC token).

    Accepts either a raw JWT string or a filesystem path to a file containing
    one.  Optionally logs the ``iat`` (issued-at) and ``exp`` (expiry) times
    in UTC.

    Args:
        token_or_path: JWT string (``header.payload.signature``) or path to a
            file containing the token.
        return_times: If ``True``, log the ``iat`` and ``exp`` timestamps from
            the payload.

    Returns:
        Decoded payload as a Python dict.

    Raises:
        ValueError: If the token does not have exactly three segments, or if
            base64 decoding or JSON parsing fails.
    """
    # Load token from file if needed
    if os.path.exists(token_or_path):
        with open(token_or_path, "r") as f:
            token = f.read().strip()
    else:
        token = token_or_path.strip()

    # Split into header.payload.signature
    try:
        parts = token.split('.')
        if len(parts) != 3:
            raise ValueError("invalid JWT format (expecting 3 segments)")
        payload_b64url = parts[1]
    except Exception as e:
        raise ValueError(f"failed to split JWT token: {e}")

    # Convert base64url to base64
    payload_b64 = payload_b64url.replace('-', '+').replace('_', '/')
    padding = '=' * ((4 - len(payload_b64) % 4) % 4)
    payload_b64 += padding

    # Decode the payload
    try:
        decoded_bytes = base64.b64decode(payload_b64)
        decoded_str = decoded_bytes.decode('utf-8')
        payload = json.loads(decoded_str)
    except Exception as e:
        raise ValueError(f"failed to decode JWT payload: {e}")

    # Optionally print iat and exp
    if return_times:
        if 'iat' in payload:
            iat = datetime.datetime.utcfromtimestamp(payload['iat'])
            logger.info(f"Token was issued at (iat):   {iat} UTC")
        else:
            logger.info("no 'iat' field found in token")

        if 'exp' in payload:
            exp = datetime.datetime.utcfromtimestamp(payload['exp'])
            logger.info(f"Token expires at (exp):  {exp} UTC")
        else:
            logger.info("No 'exp' field found in token")

    return payload


def get_base_urls(args_base_urls: str) -> list:
    """Return a list of base URLs for transform download.

    Splits *args_base_urls* on commas if it is non-empty; otherwise falls back
    to the ``PANDA_BASE_URLS`` environment variable.

    Args:
        args_base_urls: Comma-separated URL string from command-line arguments,
            or an empty string to use the environment variable.

    Returns:
        List of URL strings, or an empty list if neither source is set.
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
    cmd = get_server_command(url, port, cmd="api/v1/metaconfig/get_resource_types")

    try:
        response = request2(cmd, panda=True)
    except Exception as exc:
        logger.warning(f'exception caught in request2() while getting resource types: {exc}')
        return {}

    logger.debug(f"response from {cmd} = {response}")

    if not response:
        logger.warning(f'failed to get memory limits from {cmd}')
        return {}

    # Convert to dict if needed
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError as exc:
            logger.warning(f'failed to parse response as JSON: {exc}')
            return {}

    if not isinstance(response, dict):
        logger.warning("unexpected response format (not a dict)")
        return {}

    # --- NEW SERVER FORMAT HANDLING ---
    success = response.get("success", False)
    if not success:
        message = response.get("message", "unknown error")
        logger.warning(f'PanDA server returned failure: {message}')
        return {}

    resource_types_list = response.get("data", [])
    if not isinstance(resource_types_list, list):
        logger.warning("unexpected data format in server response")
        return {}

    # Build final dictionary
    resource_types = {}

    try:
        for entry in resource_types_list:
            if not isinstance(entry, dict):
                continue

            resource_name = entry.get("resource_name")
            if not resource_name:
                continue

            resource_types[resource_name] = {
                "mincore": entry.get("mincore"),
                "maxcore": entry.get("maxcore"),
                "minrampercore": entry.get("minrampercore"),
                "maxrampercore": entry.get("maxrampercore"),
            }

    except Exception as exc:
        logger.warning(f'failed to parse resource types: {exc}')
        return {}

    return resource_types


def extract_protocol(url: str) -> Optional[str]:
    """Extract the protocol (scheme) from a URL.

    This function uses ``urllib.parse.urlparse`` to safely parse the URL
    and return its scheme component (e.g. ``http``, ``https``, ``root``).

    Args:
        url: The URL string to parse.

    Returns:
        The protocol (scheme) if present, otherwise ``None``.
    """
    parsed = urllib.parse.urlparse(url)
    return parsed.scheme or None


# --- helper: parse dispatcher response (module-level, small and simple) ---
def _parse_get_job_status_response(resp: Any, job_id: int) -> Tuple[str, int, int]:
    """Parse the dispatcher ``get_job_status`` response into a normalised tuple.

    Looks for an exact ``job_id`` match in the ``data`` list; falls back to
    the first record if no exact match is found.

    Args:
        resp: Response value from :func:`request2`; must be a dict with
            ``success`` and ``data`` fields.
        job_id: PanDA job ID used to select the correct record from the
            ``data`` list.

    Returns:
        A three-element tuple ``(status, attempt_nr, status_code)`` where
        *status* is the job status string, *attempt_nr* is the attempt number
        (``-1`` on parse failure), and *status_code* is always ``0`` on
        success.

    Raises:
        ValueError: If the response is not a dict, ``success`` is ``False``,
            or the ``data`` list cannot be parsed.
    """
    if not isinstance(resp, dict):
        raise ValueError("response is not a dict")

    if resp.get("success") is not True:
        raise ValueError("dispatcher returned success=False")

    data = resp.get("data")
    if not isinstance(data, list) or len(data) == 0:
        # success True but no data -> notfound (not an error)
        return "notfound", 0, 0

    # look for exact match first
    for item in data:
        if isinstance(item, dict) and item.get("job_id") == job_id:
            record = item
            break
    else:
        # fallback to first dict-like record
        first = data[0]
        if not isinstance(first, dict):
            raise ValueError("data[0] is not a dict")
        record = first

    status = str(record.get("status", "unknown"))
    attempt_raw = record.get("attempt_number", 0)
    try:
        attempt_nr = int(attempt_raw)
    except (TypeError, ValueError):
        attempt_nr = -1

    return status, attempt_nr, 0


# --- main function (refactored to be low complexity) ---
def get_job_status_from_server(job_id: int, url: str, port: int) -> Tuple[str, int, int]:
    """
    Fetch the current status of a PanDA job from the dispatcher (pilot API).

    Queries:
        GET /api/v1/pilot/get_job_status?job_ids=<...>

    Expected response:
        {
          "success": true,
          "message": "",
          "data": [
            {"job_id": 7037255444, "status": "sent", "attempt_number": 0}
          ]
        }

    Return status codes:
        0  : Success (parsed OK)
        10 : Transient error / timeout (may be retried)
        20 : Response parsing error or API returned success=False
        -1 : Unexpected exception

    Args:
        job_id: PanDA job ID.
        url: PanDA server URL/alias for `get_panda_server()`.
        port: PanDA server port.

    Returns:
        Tuple of (status, attempt_nr, status_code).
    """
    # defaults
    status: str = "unknown"
    attempt_nr: int = 0
    status_code: int = 0

    if config.Pilot.pandajob == "fake":
        return status, attempt_nr, status_code

    pandaserver = get_panda_server(url, port)
    params = {"job_ids": str(job_id)}
    max_trials = 2

    for trial in range(1, max_trials + 1):
        ret: Optional[Any] = None
        try:
            ret = request2(f"{pandaserver}/api/v1/pilot/get_job_status", params=params, method="GET")
            status, attempt_nr, status_code = _parse_get_job_status_response(ret, job_id)
            return status, attempt_nr, status_code

        except ValueError as parse_err:
            # Response shape or semantic error -> map to 20
            logger.warning(f"parse error getting job status (trial {trial}): {parse_err} ret={ret}")
            return "unknown", -1, 20

        except (TimeoutError, ssl.SSLError, http_client.RemoteDisconnected, urllib.error.URLError, urllib.error.HTTPError) as transient_exc:
            # Transient: map to 10 and retry if allowed
            logger.warning(f"transient error contacting dispatcher (trial {trial}/{max_trials}): {transient_exc}")
            if trial < max_trials:
                sleep(10)
                continue
            return "unknown", -1, 10

        except Exception as exc:
            logger.warning(f"unexpected error interpreting job status: {exc} ret={ret}")
            return "unknown", -1, -1

    # fallback (shouldn't be reached, but safe default)
    return "unknown", -1, -1


def locate_token(auth_token: str, key: bool = False) -> str:
    """Find the filesystem path for an OIDC token file.

    Defensive: if auth_token is falsy, return empty string immediately to avoid TypeErrors.
    """
    if not auth_token:
        logger.debug("locate_token(): no auth_token provided -> returning empty path")
        return ""

    primary_basedir = os.path.dirname(
        os.environ.get('OIDC_AUTH_DIR',
                       os.environ.get('PANDA_AUTH_DIR',
                                      os.environ.get('X509_USER_PROXY', '')))
    )
    paths = []
    if primary_basedir:
        paths.append(os.path.join(primary_basedir, auth_token))
    # common candidate locations
    for envvar in ('PILOT_SOURCE_DIR', 'PILOT_WORK_DIR', 'HOME'):
        base = os.environ.get(envvar, '')
        if base:
            paths.append(os.path.join(base, auth_token))

    # if the refreshed token exists, prepend it to the paths list and use it first (unless looking for key)
    if not key:
        _refreshed = os.environ.get('OIDC_REFRESHED_AUTH_TOKEN')  # full path to any refreshed token
        if _refreshed and os.path.exists(_refreshed):
            paths.insert(0, _refreshed)

    # remove duplicates while preserving order
    seen = set()
    uniq_paths = []
    for p in paths:
        if p and p not in seen:
            seen.add(p)
            uniq_paths.append(p)
    paths = uniq_paths

    path = ""
    for _path in paths:
        try:
            if _path and os.path.exists(_path):
                logger.debug(f'found {_path}')
                path = _path
                break
        except Exception:
            logger.debug(f'failed to stat candidate path: {_path}', exc_info=True)
            continue

    if path == "":
        logger.info(f'did not find any local token file ({auth_token}) in paths={paths}')

    return path


def get_auth_token_content(auth_token: str, key: bool = False) -> str:
    """Read and return the content of an OIDC token file.

    Defensive: return empty string immediately if auth_token is falsy.
    """
    if not auth_token:
        logger.debug("get_auth_token_content(): no auth_token provided -> returning empty content")
        return ""

    path = locate_token(auth_token, key=key)
    if not path:
        logger.warning('token could not be located (path is not set - make sure OIDC env vars are set)')
        return ""

    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                auth_token_content = f.read().strip()
            if not auth_token_content:
                logger.warning(f'failed to read file {path} or file is empty')
                return ""
            logger.info(f'read contents from file {path} (length = {len(auth_token_content)})')
            return auth_token_content
        else:
            logger.warning(f'path does not exist: {path}')
            return ""
    except Exception as exc:
        logger.warning(f'failed to read token file {path}: {exc}')
        return ""
