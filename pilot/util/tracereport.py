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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-26

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

Several traces can be sent in one request with :func:`send_bulk` (the Rucio
trace server accepts a list of traces since Rucio 37). See also
:mod:`pilot.util.tracesender` for sending traces in the background.

Where a trace goes (:meth:`TraceReport.send` and :func:`send_traces`):

1. to the spool file, in a middleware container script (see
   :func:`enable_trace_spool`); the pilot sends these traces after the
   container has finished;
2. to the background sender of the job, if there is one (it registers
   itself with :func:`set_background_sender`);
3. otherwise, the traces are sent at once, as before.
"""

from __future__ import annotations
import hashlib
import logging
import os
import re
import socket
import threading
import time

from collections.abc import (
    Callable,
    Iterable
)
from dataclasses import dataclass

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
from pilot.util.networking import (
    get_host_ip,
    get_local_ip
)

logger = logging.getLogger(__name__)


DEFAULT_TRACE_BATCH_SIZE = 100
DEFAULT_TRACE_TIMEOUT = 30
TRACE_ACCEPTED_REPLY = 'Created'  # reply text of the Rucio trace server (HTTP 201) for an accepted request
GENERIC_TRACE_ERROR = 1  # stored when a failed trace request has no HTTP status code
DEFAULT_TRACE_SPOOL_FILE = 'rucio_traces.jsonl'


def traces_disabled() -> bool:
    """Return True if sending of Rucio traces has been turned off.

    Traces can be turned off with ``PILOT_USE_RUCIO_TRACES=False``.

    Returns:
        ``True`` if traces should not be sent.
    """
    return environ.get('PILOT_USE_RUCIO_TRACES', 'True') == 'False'


def get_trace_setting(name: str, default: int, minimum: int = 1) -> int:
    """Return an integer trace setting from the ``[Rucio]`` config section.

    The default is used if the setting is missing (e.g. in a custom config file) or invalid.

    Args:
        name: Setting name.
        default: Value used if the setting is missing, not an integer, or below ``minimum``.
        minimum: Smallest valid value (default 1, i.e. the setting must be positive).

    Returns:
        The setting value.
    """
    try:
        value = int(getattr(config.Rucio, name))
    except (AttributeError, TypeError, ValueError):
        return default

    return value if value >= minimum else default


def is_trace_accepted(reply: Any) -> bool:
    """Return True if the reply from the trace server means that the request was accepted.

    The Rucio trace server replies ``Created`` (HTTP 201). A dictionary (a JSON reply) is also
    accepted. Anything else, including the ``failed to send request`` messages from
    :func:`~pilot.util.https.request2`, is a failure.

    Args:
        reply: Return value of :func:`~pilot.util.https.request2`.

    Returns:
        ``True`` if the request was accepted.
    """
    if isinstance(reply, dict):
        return True

    return isinstance(reply, str) and reply.strip() == TRACE_ACCEPTED_REPLY


def get_trace_error_code(reply: Any) -> int:
    """Return the error code to store for a failed trace request.

    The HTTP status code is used when the reply contains one (e.g. 502), otherwise
    :data:`GENERIC_TRACE_ERROR`. HTTP status codes cannot be confused with curl exit codes,
    which are below 100.

    Args:
        reply: Return value of :func:`~pilot.util.https.request2`.

    Returns:
        Error code.
    """
    match = re.search(r'HTTP Error (\d{3})', reply) if isinstance(reply, str) else None

    return int(match.group(1)) if match else GENERIC_TRACE_ERROR


def store_trace_error(workdir: str, exit_code: int) -> None:
    """Store a trace sending error, to be reported with the job metrics as ``rucioTraceError``.

    The error is written to a file in the work directory, since an environment variable is not
    seen outside a middleware container, and is also set as ``RUCIO_TRACE_ERROR``.

    Args:
        workdir: Job work directory.
        exit_code: curl exit code or HTTP status code.
    """
    path = os.path.join(workdir, config.Rucio.rucio_trace_error_file)
    try:
        write_file(path, str(exit_code))
    except FileHandlingFailure as exc:
        logger.warning(f'failed to store rucio trace error code to file: {exc}')
    else:
        logger.info(f'wrote rucio trace error code {exit_code} to file {path}')
    logger.debug(f"setting env var RUCIO_TRACE_ERROR to \'{exit_code}\' to be sent with job metrics")
    os.environ['RUCIO_TRACE_ERROR'] = str(exit_code)


def get_host_identity(hostname: str) -> tuple[str, str]:
    """Return the full host name and IP address to put in the trace report.

    The host name is looked up with ``socket.gethostbyaddr()``; if that fails,
    the given name is reported as is. The IP address is resolved for both IPv4
    and IPv6 (IPv4 preferred); if the name does not resolve, e.g. a Kubernetes
    pod name without a DNS entry, the address of the outgoing network
    interface is used instead, and ``0.0.0.0`` only if there is none.

    These values are informative only: they do not affect any transfer.

    Args:
        hostname: Local host name (``PANDA_HOSTNAME`` or ``socket.gethostname()``).

    Returns:
        tuple[str, str]: (host name, IP address).
    """
    try:
        fqdn = socket.gethostbyaddr(hostname)[0]
    except (OSError, UnicodeError) as exc:
        fqdn = hostname or 'unknown'
        logger.info(f'could not look up the full name of host {hostname!r} ({exc}) - using {fqdn!r} in the trace report')

    try:
        ip = get_host_ip(hostname)
    except (OSError, UnicodeError) as exc:
        ip = get_local_ip() or '0.0.0.0'
        logger.info(f'could not resolve host {hostname!r} ({exc}) - using the outgoing interface address {ip} in the trace report')

    return fqdn, ip


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
        ``hostname``, ``ip``, and ``uuid`` from the supplied *job*. See
        :func:`get_host_identity` for how ``hostname`` and ``ip`` are found.

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

        self['hostname'], self['ip'] = get_host_identity(hostname)

        if job.jobdefinitionid:
            s = f'ppilot_{job.jobdefinitionid}'
            self['uuid'] = hashlib.md5(s.encode('utf-8')).hexdigest()  # hash_pilotid, Python 2/3
        else:
            _uuid = uuidgen_t()  # 'uuidgen -t 2> /dev/null'
            self['uuid'] = _uuid.replace('-', '')

    def start_file(self, start: float | None = None) -> None:
        """Set the start time stamps for the transfer of a new file.

        The copytools use one trace report for all the files of a stage-in or stage-out, so every file's
        trace gets its own start time stamps here. ``timeStart``, ``catStart``, ``relativeStart`` and
        ``transferStart`` are all set to the start of the transfer. ``validateStart`` is cleared; it is set
        when the pilot verifies the file's checksum (it stays empty if the pilot does not verify it).

        Args:
            start: Start time (default: now).
        """
        start = time.time() if start is None else start
        self.update(timeStart=start, catStart=start, relativeStart=start, transferStart=start, validateStart=None)

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

    def prepare(self) -> dict[str, Any] | None:
        """Prepare the report for sending and return a JSON-safe copy of it.

        Sets ``protocol`` from ``url`` when it is missing, verifies the report
        (see :meth:`verify_trace`, which also applies ``RUCIO_LOCAL_SITE_ID``)
        and returns a plain copy in which the strings ``'None'`` and
        ``'null'`` are sent as null values. The copy is independent of the
        report, so it can be queued while the report itself is reused.

        Returns:
            The prepared report, or ``None`` if the report could not be
            verified or serialised.
        """
        # determine protocol in case it is not set (to prevent None values sent to server)
        if not self.get('protocol') and self.get('url'):
            self['protocol'] = extract_protocol(self['url'])
            logger.debug(f'setting protocol to {self["protocol"]}')

        if not self.verify_trace():
            logger.warning(f'cannot send trace since not all fields are set (filename={self.get("filename")})')
            return None

        try:
            data = loads(dumps(self))
        except (TypeError, ValueError) as exc:
            logger.warning(f'cannot send trace since it cannot be serialised: {exc}')
            return None

        return correct_none_types(data)

    def send(self) -> bool:
        """Send the trace report to the Rucio tracing server.

        The report is delivered via :func:`~pilot.util.https.request2`. A
        failed request is logged and not retried. The ``curl`` fallback is
        only used if the request returns neither a reply nor an error message.
        Any unhandled exception is caught and logged so that a tracing
        failure never aborts the calling code.

        Inside a middleware container script, the report is written to the
        spool file instead, and if the job has a background sender, the
        report is queued there (see :func:`defer_traces`). In both cases the
        call returns at once.

        Sending can be disabled globally by setting the environment variable
        ``PILOT_USE_RUCIO_TRACES`` to ``'False'``.

        Returns:
            ``True`` if the trace was sent successfully, spooled or queued, or
            if sending was disabled; ``False`` if :meth:`prepare` failed or the
            server did not accept the trace.
        """
        # only send trace if it is actually required (can be turned off with pilot option)
        if traces_disabled():
            logger.debug('rucio trace does not need to be sent')
            return True

        logger.info(f"sending tracing report: {self}")
        data_dict = self.prepare()
        if data_dict is None:
            return False

        if defer_traces([data_dict], self.get('appid')):
            return True

        url = config.Rucio.url
        logger.info(f"tracing server: {url}")
        try:
            logger.debug(f'data_dict={data_dict}')
            ret = request2(url=url, json_body=data_dict, secure=False, compressed=False,
                           timeout=get_trace_setting('trace_timeout', DEFAULT_TRACE_TIMEOUT))
        except Exception:  # pylint: disable=broad-exception-caught
            # if something fails, log it but ignore
            logger.error(f'tracing failed: {exc_info()}')
            return True

        if is_trace_accepted(ret):
            logger.info(f'tracing report sent (server reply: {ret})')
            return True

        if isinstance(ret, str):
            logger.warning(f'failed to send tracing report: {ret}')
            return False

        logger.warning("failed to send tracing report - using old curl command")
        self.send_with_curl(url)

        return True

    def send_with_curl(self, url: str) -> None:
        """Send the trace report with ``curl`` (legacy fallback).

        The curl output is streamed to files under :attr:`workdir` to avoid
        overwhelming ``subprocess.communicate()``. A curl error is stored with
        :func:`store_trace_error` so that it can be reported with the job
        metrics, also when a middleware container is used.

        Args:
            url: Tracing server URL.
        """
        try:
            # take care of the encoding (ipv/workdir are instance attrs, not in the dict)
            data = dumps(self).replace('"', '\\"')  # for curl
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
                return

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
                exit_code = self.get_curl_output_error(outname, errname)
            if not exit_code:
                logger.info('no errors were detected from curl operation')
            else:
                store_trace_error(self.workdir, exit_code)
        except Exception:  # pylint: disable=broad-exception-caught
            # if something fails, log it but ignore
            logger.error(f'tracing failed: {exc_info()}')

    def get_curl_output_error(self, outname: str, errname: str) -> int:
        """Return the error found in the curl stdout or stderr files.

        Args:
            outname: Path of the curl stdout file.
            errname: Path of the curl stderr file.

        Returns:
            ``1`` if a server-side error was found; ``0`` otherwise.
        """
        exit_code = 0
        out, err = self.get_trace_curl_files(outname, errname, mode='r')
        if out:
            exit_code = self.assign_error(out)
            if not exit_code and err:
                exit_code = self.assign_error(err)
            logger.debug(f'curl exit_code from stdout/err={exit_code}')
            self.close(out, err)
        else:
            logger.warning(f'failed to open curl stdout file: {outname}')

        return exit_code

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


@dataclass
class BulkResult:
    """Outcome of :func:`send_bulk`.

    Attributes:
        sent: Number of traces in accepted requests.
        failed: Number of traces in failed requests.
        invalid: Number of traces dropped since they could not be verified.
        requests: Number of requests sent.
        elapsed: Total time in seconds.
    """

    sent: int = 0
    failed: int = 0
    invalid: int = 0
    requests: int = 0
    elapsed: float = 0.0


def prepare_traces(traces: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Prepare traces for sending, dropping the ones that cannot be verified.

    Args:
        traces: Trace reports, or plain dictionaries with trace report fields.

    Returns:
        A tuple ``(prepared, invalid)`` with the prepared traces and the number of dropped traces.
    """
    prepared, rejected = split_traces(traces)

    return prepared, len(rejected)


def split_traces(traces: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Prepare traces for sending, and return the ones that cannot be verified separately.

    Args:
        traces: Trace reports, or plain dictionaries with trace report fields.

    Returns:
        A tuple ``(prepared, rejected)`` with the prepared traces and the traces that could not be verified.
    """
    prepared = []
    rejected = []
    for trace in traces:
        report = trace if isinstance(trace, TraceReport) else TraceReport(trace)
        data = report.prepare()
        if data is None:
            rejected.append(report)
        else:
            prepared.append(data)
    if rejected:
        logger.warning(f'dropped {len(rejected)} trace(s) that could not be verified')

    return prepared, rejected


def post_traces(batch: list[dict[str, Any]], timeout: int) -> tuple[bool, Any]:
    """Send one request with a list of prepared traces to the Rucio trace server.

    The server accepts a list of traces since Rucio 37. This function never raises.

    Args:
        batch: Prepared traces (see :func:`prepare_traces`).
        timeout: Request time-out in seconds.

    Returns:
        A tuple ``(accepted, reply)``.
    """
    try:
        reply = request2(url=config.Rucio.url, json_body=batch, secure=False, compressed=False, timeout=timeout)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        reply = f'failed to send request: {exc}'

    return is_trace_accepted(reply), reply


def send_bulk(traces: Iterable[dict[str, Any]], workdir: str = '') -> BulkResult:
    """Send traces in as few requests as possible, in the calling thread.

    The traces are sent in requests of at most ``[Rucio] trace_batch_size`` traces. A failed
    request is not retried, and the error is stored for the job metrics (see
    :func:`store_trace_error`).

    Args:
        traces: Trace reports, or plain dictionaries with trace report fields.
        workdir: Job work directory (for the trace error file).

    Returns:
        The outcome.
    """
    result = BulkResult()
    if traces_disabled():
        logger.debug('rucio traces do not need to be sent')
        return result

    t0 = time.time()
    prepared, result.invalid = prepare_traces(traces)
    batch_size = get_trace_setting('trace_batch_size', DEFAULT_TRACE_BATCH_SIZE)
    timeout = get_trace_setting('trace_timeout', DEFAULT_TRACE_TIMEOUT)
    for start in range(0, len(prepared), batch_size):
        batch = prepared[start:start + batch_size]
        accepted, reply = post_traces(batch, timeout)
        result.requests += 1
        if accepted:
            result.sent += len(batch)
        else:
            result.failed += len(batch)
            logger.warning(f'failed to send {len(batch)} trace(s): {reply}')
            store_trace_error(workdir, get_trace_error_code(reply))
    result.elapsed = time.time() - t0
    logger.info(f'sent traces in {result.requests} request(s): sent={result.sent}, failed={result.failed}, '
                f'invalid={result.invalid}, time={result.elapsed:.1f} s')

    return result


# ---------------------------------------------------------------------------
# routing: spool file (middleware containers), background sender, or at once
# ---------------------------------------------------------------------------


class _TraceRouting:  # pylint: disable=too-few-public-methods
    """Where traces go instead of being sent at once (module state, see :func:`reset_trace_routing`)."""

    def __init__(self) -> None:
        """Start with no spool file and no background sender."""
        self.spool_path = ''  # set in middleware container scripts
        self.background: Callable[[list, Any], bool] | None = None  # set by pilot.util.tracesender
        self.lock = threading.Lock()  # serialises the spool file writes


_routing = _TraceRouting()


def get_trace_spool_path(workdir: str) -> str:
    """Return the path of the trace spool file in a work directory.

    Args:
        workdir: Job work directory.

    Returns:
        Path of the spool file (``[Rucio] trace_spool_file``, or its default if the setting is missing).
    """
    name = getattr(config.Rucio, 'trace_spool_file', '') or DEFAULT_TRACE_SPOOL_FILE

    return os.path.join(workdir, name)


def enable_trace_spool(workdir: str) -> str:
    """Write traces to the spool file in the work directory instead of sending them.

    This is used by the middleware container scripts: the pilot sends the traces after the
    container has finished (see :func:`read_trace_spool`), so that they do not delay stage-in
    or stage-out.

    Args:
        workdir: Job work directory (as seen by the script).

    Returns:
        Path of the spool file.
    """
    _routing.spool_path = get_trace_spool_path(workdir)
    logger.info(f'rucio traces will be written to {_routing.spool_path} and sent by the pilot')

    return _routing.spool_path


def set_background_sender(handler: Callable[[list, Any], bool] | None) -> None:
    """Register the function that hands traces to the background sender of a job.

    The function is called with the traces and the job id, and returns True if it took the
    traces. It is registered by :mod:`pilot.util.tracesender`, which imports this module.

    Args:
        handler: The function, or None to remove it.
    """
    _routing.background = handler


def reset_trace_routing() -> None:
    """Turn the spool file off (for tests). The background sender function is kept."""
    _routing.spool_path = ''


def spool_traces(traces: Iterable[dict[str, Any]]) -> bool:
    """Append traces to the spool file, one JSON line per trace.

    Traces that cannot be verified are dropped (see :func:`prepare_traces`).

    Args:
        traces: Trace reports, or plain dictionaries with trace report fields.

    Returns:
        True if the traces were written (or there was nothing to write), False if the file could not be written.
    """
    prepared, _ = prepare_traces(traces)
    if not prepared:
        return True

    path = _routing.spool_path
    lines = ''.join(dumps(trace) + '\n' for trace in prepared)
    try:
        with _routing.lock, open(path, 'a', encoding='utf-8') as spool:
            spool.write(lines)
    except OSError as exc:
        logger.warning(f'failed to write {len(prepared)} trace(s) to {path}: {exc} - will send them now')
        return False
    logger.info(f'wrote {len(prepared)} trace(s) to {path}')

    return True


def read_trace_spool(workdir: str) -> list[dict[str, Any]]:
    """Read the traces written to the spool file by a middleware container script, and remove the file.

    Damaged lines, e.g. a last line cut short when the container was killed, are skipped.

    Args:
        workdir: Job work directory.

    Returns:
        The traces (empty if there is no spool file).
    """
    path = get_trace_spool_path(workdir)
    if not os.path.exists(path):
        return []

    try:
        with open(path, encoding='utf-8', errors='replace') as spool:
            lines = spool.read().splitlines()
    except OSError as exc:
        logger.warning(f'failed to read {path}: {exc}')
        return []

    traces, damaged = parse_spool_lines(lines)
    if damaged:
        logger.warning(f'skipped {damaged} damaged line(s) in {path}')

    try:
        os.remove(path)
    except OSError as exc:
        logger.warning(f'failed to remove {path}: {exc}')
    logger.info(f'read {len(traces)} trace(s) from {path}')

    return traces


def parse_spool_lines(lines: Iterable[str]) -> tuple[list[dict[str, Any]], int]:
    """Return the traces in the lines of a spool file.

    Blank lines are ignored. A line that is not a JSON object is damaged.

    Args:
        lines: Lines of the spool file.

    Returns:
        A tuple ``(traces, damaged)`` with the traces and the number of damaged lines.
    """
    traces = []
    damaged = 0
    for line in lines:
        if not line.strip():
            continue
        try:
            trace = loads(line)
        except ValueError:
            trace = None
        if isinstance(trace, dict):
            traces.append(trace)
        else:
            damaged += 1

    return traces, damaged


def defer_traces(traces: list[dict[str, Any]], job_id: Any = None) -> bool:
    """Write traces to the spool file, or queue them in the job's background sender, instead of sending them now.

    Args:
        traces: Trace reports, or plain dictionaries with trace report fields.
        job_id: PanDA job id (``appid`` of the traces).

    Returns:
        True if the traces were taken care of, False if the caller should send them now.
    """
    if _routing.spool_path:
        return spool_traces(traces)

    handler = _routing.background
    if handler is None:
        return False

    try:
        return bool(handler(traces, job_id))
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f'failed to queue {len(traces)} trace(s) for sending in the background: {exc}')
        return False


def send_traces(traces: Iterable[dict[str, Any]], job_id: Any = None, workdir: str = '') -> None:
    """Send several traces: to the spool file or the job's background sender if possible, otherwise at once.

    Sending at once uses :func:`send_bulk`.

    Args:
        traces: Trace reports, or plain dictionaries with trace report fields.
        job_id: PanDA job id (``appid`` of the traces).
        workdir: Job work directory (for the trace error file).
    """
    traces = list(traces)  # the traces are used twice if they cannot be deferred
    if traces_disabled():
        logger.debug('rucio traces do not need to be sent')
        return

    if not defer_traces(traces, job_id):
        send_bulk(traces, workdir=workdir)
