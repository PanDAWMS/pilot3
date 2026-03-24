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
# - Alexey Anisenkov, alexey.anisenkov@cern.ch, 2017
# - Pavlo Svirin, pavlo.svirin@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-25

"""Rucio tracing report.

This module provides :class:`TraceReport`, a dict subclass that accumulates
metadata about a single file transfer and submits it to the Rucio tracing
server at the end of the operation.

Each instance is pre-populated with default field values matching the Rucio
tracing schema (see the Tracing report document in the Pilot GitHub wiki).
Callers update fields via the standard :meth:`dict.update` / item-assignment
interface and then call :meth:`TraceReport.send` when the transfer is
complete.

Two special keys — ``ipv`` and ``workdir`` — are accepted at construction
time but are stored as instance attributes rather than dict entries so they
are never serialised into the payload sent to the server.
"""

import hashlib
import logging
import os
import socket
import time

from io import TextIOWrapper
from json import (
    dumps,
    loads
)
from os import (
    environ,
    getuid
)
from sys import exc_info
from typing import (
    Any,
    Optional,
)

from pilot.common.exception import FileHandlingFailure
from pilot.info import JobData
from pilot.util.auxiliary import (
    correct_none_types,
    uuidgen_t
)
from pilot.util.config import config
from pilot.util.constants import (
    get_pilot_version,
    get_rucio_client_version
)
from pilot.util.container import execute2
from pilot.util.filehandling import (
    append_to_file,
    write_file
)
from pilot.util.https import (
    extract_protocol,
    request2
)
logger = logging.getLogger(__name__)


class TraceReport(dict):
    """A dict-based container for a single Rucio file-transfer trace.

    Inherits from :class:`dict` so that callers can update individual report
    fields with standard dict operations.  :meth:`send` serialises the dict
    and delivers it to the Rucio tracing endpoint.

    Class attributes:
        ipv (str): Internet-protocol version used to select the ``-4`` curl
            flag (default ``'IPv6'``).  Overridden per-instance.
        workdir (str): Pilot working directory used to resolve paths for curl
            output files (default ``''``).  Overridden per-instance.
    """

    ipv = 'IPv6'
    workdir = ''

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the trace report.

        Populates the report dict with default field values drawn from the
        Rucio tracing schema, then merges any caller-supplied fields via
        *args* and **kwargs**.  Two keys receive special treatment and are
        **not** included in the serialised payload sent to the server:

        - ``ipv`` (str): Internet-protocol version string used to select the
          ``-4`` flag when calling curl (default ``'IPv6'``).
        - ``workdir`` (str): Pilot working directory used to resolve paths for
          curl output files (default ``''``).

        Args:
            *args: Optional positional dicts whose items are merged into the
                report after the defaults are applied.
            **kwargs: Optional keyword arguments merged into the report.
        """
        event_version = f"{get_pilot_version()}+{get_rucio_client_version()}"
        defs = {  # for reference, see Tracing report document in wiki area of Pilot GitHub repository
            'eventType': '',
            'eventVersion': event_version,  # Pilot+Rucio client version
            'protocol': None,               # set by specific copy tool
            'clientState': 'INIT_REPORT',
            'localSite': environ.get('RUCIO_LOCAL_SITE_ID', ''),
            'remoteSite': '',
            'timeStart': None,
            'catStart': None,
            'relativeStart': None,
            'transferStart': None,
            'validateStart': None,
            'timeEnd': None,
            'dataset': '',
            'version': None,
            'duid': None,
            'filename': None,
            'guid': None,
            'filesize': None,
            'usr': None,
            'appid': None,
            'hostname': '',
            'ip': '',
            'suspicious': '0',
            'usrdn': '',
            'url': None,
            'stateReason': None,
            'uuid': None,
            'taskid': '',
            'pq': environ.get('PILOT_SITENAME', '')
        }

        super().__init__(defs)
        self.update(dict(*args, **kwargs))  # apply extra input
        # pop ipv and workdir from the dict — they are for internal use only and must not appear in the sent report
        self.ipv = self.pop('ipv', 'IPv6')  # ipv (internet protocol version) is needed below for the curl command, but should not be included in the report
        self.workdir = self.pop('workdir', '')  # workdir is needed for streaming the curl output, but should not be included in the report

    # sitename, dsname, eventType
    def init(self, job: JobData) -> None:
        """Populate report fields that depend on the running job.

        Sets ``clientState``, ``usr`` (an anonymised MD5 hash of the producer
        user ID), ``appid``, ``usrdn``, ``taskid``, ``timeStart``,
        ``hostname``, ``ip``, and ``uuid`` from the supplied *job*.  All DNS
        look-ups are performed with a 10-second socket timeout to prevent
        hanging when a DNS server is unreachable.

        Args:
            job: Job object whose metadata should be recorded in the report.
        """
        data = {
            'clientState': 'INIT_REPORT',
            'usr': hashlib.md5(job.produserid.encode('utf-8')).hexdigest(),  # anonymise user and pilot id's
            'appid': job.jobid,
            'usrdn': job.produserid,
            'taskid': job.taskid
        }
        self.update(data)
        self['timeStart'] = time.time()

        # set a timeout of 10 seconds to prevent potential hanging due to problems with DNS resolution, or if the DNS
        # server is slow to respond
        socket.setdefaulttimeout(10)

        try:
            hostname = os.environ.get('PANDA_HOSTNAME', socket.gethostname())
        except (socket.gaierror, socket.herror) as exc:
            logger.warning(f'unable to detect hostname for trace report: {exc}')
            hostname = os.environ.get('PANDA_HOSTNAME', 'unknown')

        try:
            self['hostname'] = socket.gethostbyaddr(hostname)[0]
        except (socket.gaierror, socket.herror) as exc:
            logger.warning(f'unable to detect hostname by address for trace report: {exc}')
            self['hostname'] = 'unknown'

        try:
            self['ip'] = socket.gethostbyname(hostname)
        except (socket.gaierror, socket.herror) as exc:
            logger.debug(f"unable to detect host IP for trace report: {exc}")
            self['ip'] = '0.0.0.0'

        if job.jobdefinitionid:
            s = f'ppilot_{job.jobdefinitionid}'
            self['uuid'] = hashlib.md5(s.encode('utf-8')).hexdigest()  # hash_pilotid, Python 2/3
        else:
            _uuid = uuidgen_t()  # 'uuidgen -t 2> /dev/null'
            self['uuid'] = _uuid.replace('-', '')

    def get_value(self, key: str) -> Any:
        """Return the trace report value for a given key.

        Args:
            key: Report field name to look up.

        Returns:
            The value stored under *key*, or ``None`` if the key is absent.
        """
        return self.get(key, None)

    def verify_trace(self) -> bool:
        """Verify that all required trace fields are populated.

        Strips backslash escape characters from ``stateReason`` if present,
        and re-applies ``RUCIO_LOCAL_SITE_ID`` from the environment if the
        variable is set (overriding any previously stored value).

        Returns:
            ``True`` if ``eventType``, ``localSite``, and ``remoteSite`` are
            all non-empty; ``False`` otherwise.
        """
        # remove any escape characters that might be present in the stateReason field
        state_reason = self.get('stateReason', '')
        if not state_reason:
            state_reason = ''
        self.update(stateReason=state_reason.replace('\\', ''))

        # overwrite any localSite if RUCIO_LOCAL_SITE_ID is set
        localsite = environ.get('RUCIO_LOCAL_SITE_ID', '')
        if localsite:
            self['localSite'] = localsite

        if not self['eventType'] or not self['localSite'] or not self['remoteSite']:
            return False

        return True

    def send(self) -> bool:  # noqa: C901
        """Send the trace report to the Rucio tracing server.

        First attempts delivery via :func:`~pilot.util.https.request2`
        (urllib-based).  If that call returns a falsy value, the method falls
        back to a ``curl`` subprocess, streaming its stdout and stderr to
        files under :attr:`workdir` to avoid overwhelming
        ``subprocess.communicate()``.  Any unhandled exception is caught and
        logged so that a tracing failure never aborts the calling code.

        Sending can be disabled globally by setting the environment variable
        ``PILOT_USE_RUCIO_TRACES`` to ``'False'``.

        Returns:
            ``True`` if the trace was sent successfully or if sending was
            disabled; ``False`` if :meth:`verify_trace` failed or the server
            returned an unexpected response type.
        """
        # only send trace if it is actually required (can be turned off with pilot option)
        if environ.get('PILOT_USE_RUCIO_TRACES', 'True') == 'False':
            logger.debug('rucio trace does not need to be sent')
            return True

        url = config.Rucio.url
        logger.info(f"tracing server: {url}")

        # determine protocol in case it is not set (to prevent None values sent to server)
        if not self['protocol'] and self['url']:
            self['protocol'] = extract_protocol(self['url'])
            logger.debug(f'setting protocol to {self["protocol"]}')
        logger.info(f"sending tracing report: {self}")

        if not self.verify_trace():
            logger.warning('cannot send trace since not all fields are set')
            return False

        out = None
        err = None
        try:
            # take care of the encoding (ipv/workdir are instance attrs, not in the dict)
            data = dumps(self).replace('"', '\\"')  # for curl
            data_urllib = dumps(self)  # for urllib

            # must convert data to a dictionary and make sure None values are kept
            data_str_urllib = data_urllib.replace('None', '\"None\"')
            data_str_urllib = data_str_urllib.replace('null', '\"None\"')

            data_dict = loads(data_str_urllib)  # None values will now be 'None'-strings
            data_dict = correct_none_types(data_dict)
            logger.debug(f'data_dict={data_dict}')
            ret = request2(url=url, json_body=data_dict, secure=False, compressed=False)
            if isinstance(ret, str):
                logger.warning(f"tracing server returned a string instead of a dictionary: {ret}")
                return False

            logger.info(f'received: {ret}')

            if ret:
                logger.info("tracing report sent")
                return True

            logger.warning("failed to send tracing report - using old curl command")

            ssl_certificate = self.get_ssl_certificate()

            # create the command
            command = 'curl'
            if self.ipv == 'IPv4':
                command += ' -4'

            # stream the output to files to prevent massive reponses that could overwhelm subprocess.communicate() in execute()
            outname, errname = self.get_trace_curl_filenames(name='trace_curl_last')
            out, err = self.get_trace_curl_files(outname, errname)
            logger.debug(f'using {outname} and {errname} to store curl output')
            if out is None or err is None:
                logger.warning('failed to open curl output files; curl fallback cannot run')
            else:
                cmd = f'{command} --connect-timeout 100 --max-time 120 --cacert {ssl_certificate} -v -k -d \"{data}\" {url}'
                exit_code = execute2(cmd, out, err, 300)
                logger.debug(f'exit_code={exit_code}')

                # always append the output to trace_curl.std{out|err}
                outname_final, errname_final = self.get_trace_curl_filenames(name='trace_curl')
                _ = append_to_file(outname, outname_final)
                _ = append_to_file(errname, errname_final)
                self.close(out, err)

                # handle errors that only appear in stdout/err (curl)
                if not exit_code:
                    out, err = self.get_trace_curl_files(outname, errname, mode='r')
                    if out:
                        exit_code = self.assign_error(out)
                        if not exit_code and err:
                            exit_code = self.assign_error(err)
                        logger.debug(f'curl exit_code from stdout/err={exit_code}')
                        self.close(out, err)
                    else:
                        logger.warning(f'failed to open curl stdout file: {outname}')
                if not exit_code:
                    logger.info('no errors were detected from curl operation')
                else:
                    # better to store exit code in file since env var will not be seen outside container in case middleware
                    # container is used
                    path = os.path.join(self.workdir, config.Rucio.rucio_trace_error_file)
                    try:
                        write_file(path, str(exit_code))
                    except FileHandlingFailure as exc:
                        logger.warning(f'failed to store curl exit code to file: {exc}')
                    else:
                        logger.info(f'wrote rucio trace exit code {exit_code} to file {path}')
                    logger.debug(f"setting env var RUCIO_TRACE_ERROR to \'{exit_code}\' to be sent with job metrics")
                    os.environ['RUCIO_TRACE_ERROR'] = str(exit_code)

        except Exception:
            # if something fails, log it but ignore
            logger.error(f'tracing failed: {exc_info()}')

        return True

    def close(self, out: Optional[TextIOWrapper], err: Optional[TextIOWrapper]) -> None:
        """Close open file streams, ignoring ``None`` handles.

        Args:
            out: Open stdout file to close, or ``None``.
            err: Open stderr file to close, or ``None``.
        """
        if out:
            out.close()
        if err:
            err.close()

    def assign_error(self, out: TextIOWrapper) -> int:
        """Scan a curl output file line by line for server-side errors.

        Reads *out* until EOF or until a line containing ``'ExceptionClass'``
        is found, which indicates a server-side failure reported by the Rucio
        tracing endpoint.

        Args:
            out: Open text file containing curl output to scan.

        Returns:
            ``1`` if an ``'ExceptionClass'`` line was found; ``0`` otherwise.
        """
        exit_code = 0
        count = 0
        while True:
            count += 1

            # Get next line from file
            line = out.readline()

            # if line is empty
            # end of file is reached
            if not line:
                break
            if 'ExceptionClass' in line.strip():
                logger.warning(f'curl failure: {line.strip()}')
                exit_code = 1
                break

        return exit_code

    def get_trace_curl_filenames(self, name: str = 'trace_curl') -> tuple[str, str]:
        """Return paths for the curl stdout and stderr files.

        Paths are rooted in :attr:`workdir` when that attribute is non-empty,
        keeping all pilot-generated files in one directory.

        Args:
            name: Base name stem for both files (default ``'trace_curl'``).

        Returns:
            A two-element tuple ``(stdout_path, stderr_path)``.
        """
        base = os.path.join(self.workdir, name) if self.workdir else name
        return f"{base}.stdout", f"{base}.stderr"

    def get_trace_curl_files(self, outpath: str, errpath: str, mode: str = 'w') -> tuple[Optional[TextIOWrapper], Optional[TextIOWrapper]]:
        """Open the curl stdout and stderr files.

        Opens both files in the requested mode.  If either open fails, any
        already-open handle is closed before returning so there is no
        file-descriptor leak.

        Args:
            outpath: Path for the stdout file.
            errpath: Path for the stderr file.
            mode: :func:`open` mode string (default ``'w'`` for write-text).

        Returns:
            A two-element tuple ``(out, err)`` with the open
            :class:`~io.TextIOWrapper` objects, or ``(None, None)`` if
            either file could not be opened.
        """
        out = None
        err = None
        try:
            out = open(outpath, mode=mode, encoding='utf-8')  # pylint: disable=consider-using-with
            err = open(errpath, mode=mode, encoding='utf-8')  # pylint: disable=consider-using-with
        except IOError as error:
            logger.warning(f'failed to open curl stdout/err: {error}')
            if out is not None:
                out.close()
            out = None
            err = None

        return out, err

    def get_ssl_certificate(self) -> str:
        """Return the path to the X.509 user proxy certificate.

        Reads ``X509_USER_PROXY`` from the environment.  If it is not set,
        the conventional default ``/tmp/x509up_u<uid>`` is returned.

        Returns:
            Absolute path to the SSL certificate file.
        """
        return environ.get('X509_USER_PROXY', f'/tmp/x509up_u{getuid()}')
