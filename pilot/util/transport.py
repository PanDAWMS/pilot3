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
# PanDA authors:
# - Aleksandr Alekseev, aleksandr.alekseev@cern.ch, 2022
# - Paul Nilsson, paul.nilsson@cern.ch, 2022-23

from abc import ABC, abstractmethod
from typing import Iterator, Union
import json
import logging
import socket
import ssl

try:
    from requests.auth import HTTPBasicAuth
except ImportError:
    HTTPBasicAuth = None
try:
    import requests
    import pylogbeat
    from logstash_async.utils import ichunked
except ImportError:
    pass

logger = logging.getLogger(__name__)


class TimeoutNotSet:
    pass


class Transport(ABC):
    """Abstract base class for all transport protocols.

    Args:
        host: Hostname of the remote endpoint.
        port: TCP/UDP port number.
        timeout: Connection timeout, or None for no timeout.
        ssl_enable: If True, activates TLS.
        ssl_verify: If True, enables TLS certificate verification. A string
            may be passed as a path to a CA certificate file.
        use_logging: If True, use logging for debugging output.
    """

    def __init__(
            self,
            host: str,
            port: int,
            timeout: Union[None, float],
            ssl_enable: bool,
            ssl_verify: Union[bool, str],
            use_logging: bool,
    ):
        self._host = host
        self._port = port
        self._timeout = None if timeout is TimeoutNotSet else timeout
        self._ssl_enable = ssl_enable
        self._ssl_verify = ssl_verify
        self._use_logging = use_logging
        super().__init__()

    @abstractmethod
    def send(self, events: list, **kwargs):
        pass

    @abstractmethod
    def close(self):
        pass


class UdpTransport:

    _keep_connection = False

    # ----------------------------------------------------------------------
    # pylint: disable=unused-argument
    def __init__(self, host, port, timeout=TimeoutNotSet, **kwargs):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._sock = None

    # ----------------------------------------------------------------------
    def send(self, events, use_logging=False):  # pylint: disable=unused-argument
        # Ideally we would keep the socket open but this is risky because we might not notice
        # a broken TCP connection and send events into the dark.
        # On UDP we push into the dark by design :)
        self._create_socket()
        try:
            self._send(events)
        finally:
            self._close()

    # ----------------------------------------------------------------------
    def _create_socket(self):
        if self._sock is not None:
            return

        # from logging.handlers.DatagramHandler
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if self._timeout is not TimeoutNotSet:
            self._sock.settimeout(self._timeout)

    # ----------------------------------------------------------------------
    def _send(self, events):
        for event in events:
            self._send_via_socket(event)

    # ----------------------------------------------------------------------
    def _send_via_socket(self, data):
        data_to_send = self._convert_data_to_send(data)
        self._sock.sendto(data_to_send, (self._host, self._port))

    # ----------------------------------------------------------------------
    def _convert_data_to_send(self, data):
        if not isinstance(data, bytes):
            return bytes(data, 'utf-8')

        return data

    # ----------------------------------------------------------------------
    def _close(self, force=False):
        if not self._keep_connection or force:
            if self._sock:
                self._sock.close()
                self._sock = None

    # ----------------------------------------------------------------------
    def close(self):
        self._close(force=True)


class TcpTransport(UdpTransport):

    # ----------------------------------------------------------------------
    def __init__(  # pylint: disable=too-many-arguments
            self,
            host,
            port,
            ssl_enable,
            ssl_verify,
            keyfile,
            certfile,
            ca_certs,
            timeout=TimeoutNotSet,
            **kwargs):
        super().__init__(host, port)
        self._ssl_enable = ssl_enable
        self._ssl_verify = ssl_verify
        self._keyfile = keyfile
        self._certfile = certfile
        self._ca_certs = ca_certs
        self._timeout = timeout

    # ----------------------------------------------------------------------
    def _create_socket(self):
        if self._sock is not None:
            return

        # from logging.handlers.SocketHandler
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if self._timeout is not TimeoutNotSet:
            self._sock.settimeout(self._timeout)

        try:
            self._sock.connect((self._host, self._port))
            # non-SSL
            if not self._ssl_enable:
                return
            # SSL
            cert_reqs = ssl.CERT_REQUIRED
            ssl_context = ssl.create_default_context(cafile=self._ca_certs)
            if not self._ssl_verify:
                if self._ca_certs:
                    cert_reqs = ssl.CERT_OPTIONAL
                else:
                    cert_reqs = ssl.CERT_NONE

            ssl_context.verify_mode = cert_reqs
            ssl_context.check_hostname = False
            ssl_context.load_cert_chain(self._certfile, self._keyfile)
            self._sock = ssl_context.wrap_socket(self._sock, server_side=False)
        except socket.error:
            self._close()
            raise

    # ----------------------------------------------------------------------
    def _send_via_socket(self, data):
        data_to_send = self._convert_data_to_send(data)
        self._sock.sendall(data_to_send)


class BeatsTransport:

    _batch_size = 10

    # ----------------------------------------------------------------------
    def __init__(  # pylint: disable=too-many-arguments
            self,
            host,
            port,
            ssl_enable,
            ssl_verify,
            keyfile,
            certfile,
            ca_certs,
            timeout=TimeoutNotSet,
            **kwargs):
        timeout_ = None if timeout is TimeoutNotSet else timeout
        self._client_arguments = dict(
            host=host,
            port=port,
            timeout=timeout_,
            ssl_enable=ssl_enable,
            ssl_verify=ssl_verify,
            keyfile=keyfile,
            certfile=certfile,
            ca_certs=ca_certs,
            **kwargs)

    # ----------------------------------------------------------------------
    def close(self):
        pass  # nothing to do

    # ----------------------------------------------------------------------
    def send(self, events, use_logging=False):
        try:
            client = pylogbeat.PyLogBeatClient(use_logging=use_logging, **self._client_arguments)
        except Exception as exc:
            logger.warning(f'caught exception in send(): {exc}')
            return
        with client:
            for events_subset in ichunked(events, self._batch_size):
                try:
                    client.send(events_subset)
                except Exception:
                    pass


class HttpTransport(Transport):
    """HTTP transport client for the logstash ``inputs_http`` plugin.

    Args:
        host: Hostname of the logstash HTTP server.
        port: TCP port of the logstash HTTP server.
        timeout: Connection timeout in seconds. Defaults to None (no timeout).
        ssl_enable: If True, activates TLS.
        ssl_verify: If True, verifies the TLS certificate with certifi. A
            string path to a CA certificate file is also accepted.
        use_logging: If True, use logging for debugging output.
        username: Username for HTTP basic authorization.
        password: Password for HTTP basic authorization.
        max_content_length: Maximum HTTP request body size in bytes.
            Defaults to 100 MB.
    """

    def __init__(
            self,
            host: str,
            port: int,
            timeout: Union[None, float] = TimeoutNotSet,
            ssl_enable: bool = True,
            ssl_verify: Union[bool, str] = True,
            use_logging: bool = False,
            #keyfile: Union[bool, str] = True,
            #certfile: Union[bool, str] = True,
            **kwargs
    ):
        super().__init__(host, port, timeout, ssl_enable, ssl_verify, use_logging)
        self._username = kwargs.get('username', None)
        self._password = kwargs.get('password', None)
        self._max_content_length = kwargs.get('max_content_length', 100 * 1024 * 1024)
        self.__session = None
        self._cert = kwargs.get('cert', None)

    @property
    def url(self) -> str:
        """The URL of the logstash HTTP pipeline.

        Returns:
            URL string built from hostname, port, and TLS setting.
        """
        protocol = 'http'
        if self._ssl_enable:
            protocol = 'https'
        return f'{protocol}://{self._host}:{self._port}'

    def __batches(self, events: list) -> Iterator[list]:
        """Generate dynamically-sized batches based on the max content length.

        Args:
            events: List of JSON-encoded event strings.

        Returns:
            Iterator that yields lists of event objects fitting within
            ``_max_content_length``.
        """
        current_batch = []
        event_iter = iter(events)
        while True:
            try:
                current_event = next(event_iter)
            except StopIteration:
                current_event = None
                if not current_batch:
                    return
                yield current_batch
            if current_event is None:
                return
            if len(current_event) > self._max_content_length:
                msg = 'The event size <%s> is greater than the max content length <%s>.'
                msg += 'Skipping event.'
                if self._use_logging:
                    logger.warning(msg, len(current_event), self._max_content_length)
                continue
            obj = json.loads(current_event)
            content_length = len(json.dumps(current_batch + [obj]).encode('utf8'))
            if content_length > self._max_content_length:
                batch = current_batch
                current_batch = [obj]
                yield batch
            else:
                current_batch += [obj]

    def __auth(self) -> HTTPBasicAuth:
        """Return an HTTP basic auth object for the logstash pipeline.

        Returns:
            An ``HTTPBasicAuth`` instance, or None if the username or password
            is not set.
        """
        if self._username is None or self._password is None:
            return None
        try:
            return HTTPBasicAuth(self._username, self._password)
        except TypeError as exc:
            logger.warning(f'failed to execute HTTPBasicAuth: {exc}')
            return None

    def close(self) -> None:
        """Close the HTTP session.
        """
        if self.__session is not None:
            self.__session.close()

    def send(self, events: list, **kwargs) -> None:
        """Send events to the logstash pipeline.

        Batches events so that each POST body does not exceed
        ``_max_content_length``. Oversized individual events are skipped.

        Args:
            events: List of JSON-encoded event strings to send.
        """
        try:
            self.__session = requests.Session()
        except Exception:
            logger.warning('no requests module')
            return

        #print(self._cert)
        for batch in self.__batches(events):
            if self._use_logging:
                logger.debug('Batch length: %s, Batch size: %s',
                             len(batch), len(json.dumps(batch).encode('utf8')))
            response = self.__session.post(
                self.url,
                headers={'Content-Type': 'application/json'},
                json=batch,
                verify=self._ssl_verify,
                timeout=self._timeout,
                auth=self.__auth(),
                cert=self._cert)
            #print(response)
            if response.status_code != 200:
                self.close()
                response.raise_for_status()
        self.close()
