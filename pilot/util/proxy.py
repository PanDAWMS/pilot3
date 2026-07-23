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

"""Proxy certificate handling and verification utilities."""

from __future__ import annotations
import logging
import os
import traceback
from time import sleep
from typing import Any, Dict

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
    """Get the user DN.

    The DN is also sent by the server to the pilot in the job description
    (``produserid``).

    Returns:
        User DN string, or an empty string if it could not be retrieved.
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
    """Execute voms-proxy-info with the given options.

    Args:
        options: Command options.
        mute: If True, suppress printing of command output.
        path: Path to the proxy file. If set, ``--file=<path>`` is appended.

    Returns:
        Tuple of (exit code, stdout, stderr).
    """
    executable = f'voms-proxy-info {options}'
    if path:
        executable += f' --file={path}'
    exit_code, stdout, stderr = execute_nothreads(executable)
    if not mute:
        logger.info(stdout + stderr)

    return exit_code, stdout, stderr


def _extract_proxy_from_response(res: Any, voms_role: str) -> str:
    """
    Extract proxy contents from either the new or old PanDA proxy API response.

    New-style response example:
        {
          "success": true,
          "message": "",
          "data": {"user_proxy": "-----BEGIN CERTIFICATE-----..."}
        }

    Old-style response example:
        {
          "StatusCode": 0,
          "errorDialog": "",
          "userProxy": "-----BEGIN CERTIFICATE-----..."
        }

    Raises:
        ValueError: if the response indicates an error or has an unexpected format.
    """
    if res is None:
        raise ValueError(f"empty response when requesting proxy for role='{voms_role}'")

    if isinstance(res, str):
        raise ValueError(f"string response when requesting proxy for role='{voms_role}': {res}")

    if not isinstance(res, dict):
        raise ValueError(f"unexpected proxy response type={type(res)} for role='{voms_role}': {res!r}")

    # Old-style: StatusCode + userProxy
    if "StatusCode" in res:
        status_code = res.get("StatusCode")
        if status_code != 0:
            err = res.get("errorDialog", "unknown error")
            raise ValueError(
                f"panda server returned: {err!r} for proxy role '{voms_role}' (StatusCode={status_code})"
            )
        proxy_contents = res.get("userProxy")
        if not proxy_contents:
            raise ValueError(f"missing 'userProxy' in proxy response for role='{voms_role}'")
        return proxy_contents

    # New-style: success + data.user_proxy
    if "success" in res:
        if res.get("success") is not True:
            msg = res.get("message", "unknown error")
            raise ValueError(
                f"panda server returned success=False for proxy role '{voms_role}': {msg!r}"
            )

        data = res.get("data") or {}
        if not isinstance(data, dict):
            raise ValueError(f"unexpected 'data' format in proxy response for role='{voms_role}': {data!r}")

        proxy_contents = data.get("user_proxy") or data.get("userProxy")
        if not proxy_contents:
            raise ValueError(f"missing 'user_proxy' in proxy response data for role='{voms_role}'")
        return proxy_contents

    # Unknown format
    raise ValueError(f"unrecognised proxy response format for role='{voms_role}': {res!r}")


def get_proxy(proxy_outfile_name: str, voms_role: str) -> tuple[bool, str]:
    """
    Download and store a proxy.

    On read-only file systems (e.g. K8s), the default output path may not be writable.
    In that case, the proxy will be stored in the pilot workdir instead and the path
    will be updated in the return value (and X509_USER_PROXY environment variable).

    Args:
        proxy_outfile_name: Path to the file where the proxy should be stored.
        voms_role: VOMS role / VO name to request, e.g. "atlas".

    Returns:
        A tuple (result, proxy_path):
            result: True on success, False on failure.
            proxy_path: Path to the written proxy file (or the original path on failure).
    """
    _max_attempts = 3
    _retry_sleep = 30  # seconds between attempts for transient network failures
    proxy_contents = None

    for attempt in range(1, _max_attempts + 1):
        try:
            # It assumes that https_setup() was done already
            url = os.environ.get("PANDA_SERVER_URL", config.Pilot.pandaserver)

            pilot_user = os.environ.get("PILOT_USER", "generic").lower()
            user = __import__(f"pilot.user.{pilot_user}.proxy", globals(), locals(), [pilot_user], 0)
            data: Dict[str, Any] = user.getproxy_dictionary(voms_role)

            # New API endpoint
            res = https.request2(f"{url}/api/v1/creds/get_proxy", params=data)

            if res is None:
                logger.error(f"unable to get proxy with role '{voms_role}' from panda server using urllib method")
                # Fallback to old endpoint via curl-based helper (if still available)
                res = https.request(f"{url}/server/panda/getProxy", data=data)
                if res is None:
                    logger.error(
                        f"unable to get proxy with role '{voms_role}' from panda server using curl method"
                    )
                    return False, proxy_outfile_name

            # Extract proxy from either new-style or old-style response.
            # _extract_proxy_from_response raises ValueError for string responses
            # (which request2 returns on network errors) and for server-side failures.
            proxy_contents = _extract_proxy_from_response(res, voms_role)
            break  # success

        except ValueError as exc:
            exc_str = str(exc)
            # Distinguish transient network errors from definitive server-side failures.
            # request2() returns a string like "failed to send request: <urlopen error …>"
            # which _extract_proxy_from_response wraps in a ValueError.  Server-side
            # failures (StatusCode != 0, success=False) are definitive and should not
            # be retried.
            is_transient = "failed to send request:" in exc_str
            if is_transient and attempt < _max_attempts:
                logger.warning(
                    f"transient error downloading proxy for role='{voms_role}' "
                    f"(attempt {attempt}/{_max_attempts}): {exc_str} — retrying in {_retry_sleep}s"
                )
                sleep(_retry_sleep)
                continue
            logger.error(f"Get proxy from panda server failed: {exc_str}, {traceback.format_exc()}")
            return False, proxy_outfile_name

        except Exception as exc:
            logger.error(f"Get proxy from panda server failed: {exc}, {traceback.format_exc()}")
            return False, proxy_outfile_name

    if proxy_contents is None:
        return False, proxy_outfile_name

    def create_file(filename: str, contents: str) -> bool:
        """Create a file with the given contents and secure permissions."""
        _file = os.open(filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.close(_file)
        # write_file() returns True on success
        return write_file(filename, contents, mute=False)

    result = False
    try:
        # Pre-create proxy file with secure permissions and then write into it.
        result = create_file(proxy_outfile_name, proxy_contents)
    except (OSError, FileHandlingFailure) as exc:
        logger.error(f"exception caught:\n{exc},\ntraceback: {traceback.format_exc()}")
        # Handle read-only FS by writing to PILOT_HOME instead
        if "Read-only file system" in str(exc):
            proxy_outfile_name = os.path.join(
                os.getenv("PILOT_HOME", "."), os.path.basename(proxy_outfile_name)
            )
            logger.info(f"attempting writing proxy to alternative path: {proxy_outfile_name}")
            try:
                result = create_file(proxy_outfile_name, proxy_contents)
            except (OSError, FileHandlingFailure) as e:
                logger.error(f"exception caught:\n{e},\ntraceback: {traceback.format_exc()}")
            else:
                logger.debug(
                    f"updating X509_USER_PROXY to alternative path {proxy_outfile_name} "
                    "(valid until end of current job)"
                )
                os.environ["X509_USER_PROXY"] = proxy_outfile_name
    else:
        # On success, dump voms-proxy-info -all to log
        _, _, _ = vomsproxyinfo(options="-all", path=proxy_outfile_name)

    return result, proxy_outfile_name


def get_proxy_old(proxy_outfile_name: str, voms_role: str) -> tuple[bool, str]:
    """Download and store a proxy (legacy implementation).

    On read-only file systems (e.g. K8s), the default path may not be
    writable. In that case the new proxy will be stored in the workdir and
    the updated path is returned.

    Args:
        proxy_outfile_name: Path to the file where the proxy should be stored.
        voms_role: VOMS role / VO name to request, e.g. ``'atlas'``.

    Returns:
        Tuple of (result, proxy_path): result is True on success, proxy_path
        is the path to the written proxy file.
    """
    try:
        # it assumes that https_setup() was done already
        url = os.environ.get('PANDA_SERVER_URL', config.Pilot.pandaserver)

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.proxy', globals(), locals(), [pilot_user], 0)
        data = user.getproxy_dictionary(voms_role)

        # res = https.request2(f'{url}/server/panda/getProxy', json_body=data)
        res = https.request2(f'{url}/api/v1/creds/get_proxy', params=data)
        if res is None:
            logger.error(f"unable to get proxy with role '{voms_role}' from panda server using urllib method")
            res = https.request('{url}/server/panda/getProxy', data=data)
            if res is None:
                logger.error(f"unable to get proxy with role '{voms_role}' from panda server using curl method")
                return False, proxy_outfile_name

        if isinstance(res, str):
            logger.error(f"panda server returned a string instead of a dictionary: {res}")
            return False, proxy_outfile_name

        logger.debug(f'get_proxy server response: {res}')
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
    """Create cert/key pem files from given proxy and store in workdir.

    These files are needed for communicating with the logstash server.

    Args:
        from_proxy: Path to proxy file.
        workdir: Work directory where the pem files will be stored.

    Returns:
        Tuple of (path to crt.pem, path to key.pem). Both are empty strings
        on failure.
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
