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
# - Paul Nilsson, paul.nilsson@cern.ch, 2022-23, 2026

"""Functions for using ActiveMQ."""

from __future__ import annotations

import socket
import json
import random
import logging
import re
import sys
from collections.abc import Sequence
from typing import Any

try:
    import stomp
    connectionlistener = stomp.ConnectionListener
except ModuleNotFoundError:
    #from types import SimpleNamespace
    #_stomp = {'ConnectionListener': print}
    #stomp = SimpleNamespace(**_stomp)
    connectionlistener = object

from pilot.common.errorcodes import ErrorCodes
#from pilot.common.exception import PilotException
from pilot.util import https

logger = logging.getLogger(__name__)
errors = ErrorCodes()

# API path serving the user secrets under the new PanDA credentials API. Note that
# get_access_token serves tokens rather than secrets and must not be used here.
CREDENTIALS_ENDPOINT = 'api/v1/creds/get_user_secrets'

# Names of the message broker secrets to request, username first. They travel as a list
# in the query string: _merge_query() renders it as keys=MB_USERNAME&keys=MB_PASSWORD.
MB_CREDENTIAL_KEYS = ('MB_USERNAME', 'MB_PASSWORD')

# Prefix that request2() returns in place of a response when the request could not be sent.
REQUEST_FAILURE_MARKER = 'failed to send request'

# Substrings that mark a field name as carrying a secret, matched case-insensitively
# against the whole field name, so MB_PASSWORD, userSecret and access_token are all caught.
SECRET_KEY_MARKERS = ('password', 'passwd', 'pwd', 'secret', 'token', 'credential')

# Maximum number of characters of an unparseable response quoted in a diagnostic message.
PREVIEW_LENGTH = 200


def scrub_text(text: str) -> str:
    """Mask secret-looking values in text that could not be parsed as JSON.

    Covers both the ``"KEY": "value"`` and the ``KEY=value`` forms, so that neither an
    HTML error page nor a query-string echo can carry a credential into the log.

    Args:
        text: Raw response text.

    Returns:
        The text with any secret-looking value replaced by an ellipsis.
    """
    pattern = '|'.join(re.escape(marker) for marker in SECRET_KEY_MARKERS)

    text = re.sub(
        rf'("[^"]*(?:{pattern})[^"]*"\s*:\s*")([^"]*)(")',
        lambda match: match.group(1) + '......' + match.group(3),
        text,
        flags=re.IGNORECASE,
    )

    return re.sub(
        rf'([A-Za-z0-9_]*(?:{pattern})[A-Za-z0-9_]*=)([^&\s"]+)',
        lambda match: match.group(1) + '......',
        text,
        flags=re.IGNORECASE,
    )


def _coerce_mapping(value: Any) -> dict[str, Any]:
    """Coerce a value into a dictionary where possible.

    The server returns the secrets as a JSON-encoded *string* rather than as a nested
    object, so the string form is the one that matters; the dictionary form is accepted
    defensively in case that changes.

    Args:
        value: Candidate value from the response payload.

    Returns:
        A dictionary, or an empty dictionary if the value cannot be coerced.
    """
    if isinstance(value, dict):
        return value

    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (ValueError, TypeError):
            return {}
        if isinstance(decoded, dict):
            return decoded

    return {}


def _as_dict(res: Any) -> dict[str, Any]:
    """Normalise a server response into a dictionary.

    :func:`pilot.util.https.request2` returns a dictionary when the response parses, the
    raw text when it does not, and a ``failed to send request`` marker string when the
    request could not be sent at all. All three are handled here.

    Args:
        res: Response as returned by :func:`pilot.util.https.request2`.

    Returns:
        The parsed response dictionary.

    Raises:
        ValueError: If the request failed, or the response is empty, unparseable, or not
            a dictionary.
    """
    if not res:
        raise ValueError('empty response from server')

    if isinstance(res, (bytes, str)):
        text = res.decode('utf-8', errors='replace') if isinstance(res, bytes) else res
        if text.startswith(REQUEST_FAILURE_MARKER):
            raise ValueError(text)
        try:
            res = json.loads(text)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f'response is not valid JSON: {exc}: {scrub_text(text[:PREVIEW_LENGTH])!r}'
            ) from exc

    if not isinstance(res, dict):
        raise ValueError(f'unexpected response type: {type(res).__name__}')

    return res


def extract_credentials(res: Any, keys: Sequence[str] = MB_CREDENTIAL_KEYS) -> tuple[str, str]:
    r"""Extract MB_USERNAME and MB_PASSWORD from a get_user_secrets response.

    The observed response shape is::

        {"success": true, "message": "", "data": "{\"MB_USERNAME\": \"...\", ...}"}

    i.e. ``data`` is a JSON-encoded *string*, not a nested object. Note that
    ``success: true`` does not by itself mean that the secrets are present: the server
    answers ``success=true`` with ``data="{}"`` when the caller's identity has no
    secrets bound to it, which must be treated as a failure rather than as valid but
    empty credentials.

    No part of the response is included in the raised messages beyond the server's own
    ``message`` field and the available key names, since the response carries the
    password and the pilot log is uploaded.

    Args:
        res: Response as returned by :func:`pilot.util.https.request2`.
        keys: Secret names, username first.

    Returns:
        A two-element tuple ``(username, password)``.

    Raises:
        ValueError: If the response is malformed, reports failure, or does not contain
            both credentials.
    """
    names = [str(part) for part in keys]
    if len(names) < 2:
        raise ValueError(f'need two key names (username, password), got {names}')

    payload = _as_dict(res)

    if 'success' in payload:
        if payload.get('success') is not True:
            raise ValueError(f"server returned success=False: {payload.get('message', 'no message')!r}")
        data = payload.get('data')
    else:  # defensive: a bare secrets mapping without the status envelope
        data = payload

    secrets = _coerce_mapping(data)
    if not secrets:
        raise ValueError('no secrets in response (server reported success but returned no data)')

    try:
        username = str(secrets[names[0]])
        password = str(secrets[names[1]])
    except KeyError as exc:
        raise ValueError(f'missing key {exc} in secrets (available: {sorted(secrets)})') from exc

    if not username or not password:
        raise ValueError('username and/or password is empty')

    return username, password


class Listener(connectionlistener):
    """Messaging listener."""

    messages = []

    def __init__(self, broker: Any = None, queues: Any = None) -> None:
        """Initialize variables.

        Args:
            broker: Message broker connection object.
            queues: Queues object for storing incoming messages.
        """
        self.__broker = broker
        self.__queues = queues
        self.logger = logging.getLogger(self.__class__.__name__)

    def set_broker(self, broker: Any) -> None:
        """Set the broker for internal use.

        Args:
            broker: Message broker connection object.
        """
        self.__broker = broker

    def on_error(self, frame: Any) -> None:
        """Handle errors received from the message broker.

        Args:
            frame: STOMP frame object containing error details.
        """
        self.logger.warning(f'received an error "{frame}"')
        # store error in messages?

    def on_message(self, frame: Any) -> None:
        """Handle incoming messages from the message broker.

        Deserializes the frame body from JSON and enqueues it if not already present.

        Args:
            frame: STOMP frame object with a ``body`` attribute containing JSON-encoded data.
        """
        self.logger.info(f'received a message "{frame.body}"')
        body = json.loads(frame.body)
        if body not in [_obj for _obj in list(self.__queues.mbmessages.queue)]:
            self.__queues.mbmessages.put(body)
        #if body not in self.messages:
        #    self.messages.append(body)

    def get_messages(self) -> list:
        """Return stored messages to the caller.

        Returns:
            List of stored message objects.
        """
        return self.messages


class ActiveMQ:
    """
    ActiveMQ class.

    Note: the class can be used for either topic or queue messages.
    E.g. 'topic': '/queue/panda.pilot' or '/topic/panda.pilot'
    X.509 authentication using SSL not possible since key+cert cannot easily be reached from WNs.
    """

    broker = '128.0.0.1'
    brokers_resolved = []
    receiver_port = 0
    port = 0
    pandaport = 0
    pandaurl = ''
    topic = ''
    receive_topics = [topic]
    username = ''
    password = ''
    listener = None
    queues = None

    def __init__(self, **kwargs: Any) -> None:
        """Initialize variables and set up all broker connections and the listener.

        Args:
            **kwargs: Keyword arguments for configuration. Supported keys:

                - ``broker`` (str): Hostname of the ActiveMQ broker.
                - ``receiver_port`` (int): Port used to receive messages.
                - ``topic`` (str): Destination topic or queue for outgoing messages.
                - ``receive_topics`` (list): List of topics to subscribe to.
                - ``pandaurl`` (str): PanDA server URL for credential retrieval.
                - ``pandaport`` (int): PanDA server port for credential retrieval.
                - ``queues``: Queues object used by the listener to store messages.
                - ``debug`` (bool): Enable debug logging if ``True``.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.broker = kwargs.get('broker', '')
        self.receiver_port = kwargs.get('receiver_port', '')
        # self.port = kwargs.get('port', '')
        self.topic = kwargs.get('topic', '')
        self.receive_topics = kwargs.get('receive_topics', '')
        self.username = None
        self.password = None
        self.connections = []
        self.pandaurl = kwargs.get('pandaurl', '')
        self.pandaport = kwargs.get('pandaport', 0)
        self.queues = kwargs.get('queues', None)
        self.debug = kwargs.get('debug', False)

        _ = logging.StreamHandler(sys.stdout)

        # get credentials from the PanDA server, abort if not returned
        self.get_credentials()
        if not self.username or not self.password:
            self.logger.warning('cannot continue without message broker credentials')
            return

        # prevent stomp from exposing credentials in stdout (in case pilot is running in debug mode)
        logging.getLogger('stomp').setLevel(logging.INFO)

        # set a timeout of 10 seconds to prevent potential hanging due to problems with DNS resolution, or if the DNS
        # server is slow to respond
        socket.setdefaulttimeout(10)
        try:
            # get the list of brokers to use
            _addrinfos = socket.getaddrinfo(self.broker, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
        except socket.herror as exc:
            logger.warning(f'failed get address from socket: {exc}')
            return
        self.brokers_resolved = [_ai[4][0] for _ai in _addrinfos]

        receive_topic = self.receive_topics[0]
        self.logger.debug(f'receive topic: {self.receive_topics[0]}')

        # prepare the connections
        self.logger.debug(f'brokers={self.brokers_resolved}')
        for broker in self.brokers_resolved:
            try:
                # self.logger.debug(f'broker={broker}, port={self.receiver_port}')
                conn = stomp.Connection12(host_and_ports=[(broker, int(self.receiver_port))],
                                          keepalive=True)
            except Exception as exc:  # primarily used to avoid interpreted problem with stomp is not available
                self.logger.warning(f'exception caught: {exc}')
            else:
                if conn not in self.connections:
                    self.connections.append(conn)

        self.logger.debug(f'setup connections: {self.connections}')
        self.listener = Listener(queues=self.queues)
        # setup the connections (once setup, the listener will wait for messages)
        for conn in self.connections:
            if not conn.is_connected():
                self.listener.set_broker(conn.transport._Transport__host_and_ports[0])
                conn.set_listener('message-receiver', self.listener)
                conn.connect(self.username, self.password, wait=True)
                conn.subscribe(destination=receive_topic,
                               id='atlas-pilot-messaging',
                               ack='auto')

                self.logger.debug('subscribed')

    def get_messages(self) -> list:
        """Return messages received by the listener.

        Returns:
            List of received message objects, or an empty list if no listener is set.
        """
        self.logger.debug(f'getting messages from {self.listener}')
        return self.listener.get_messages() if self.listener else []

    def send_message(self, message: str) -> None:
        """Send a message to the configured topic or queue.

        Selects a random connection from the pool and publishes the message as JSON.

        Args:
            message: Message payload to send. Will be serialized to JSON.
        """
        conn = random.choice(self.connections)
        self.logger.debug(f'sending to {conn} topic/queue={self.topic}')
        conn.send(destination=self.topic, body=json.dumps(message), id='atlas-pilot-messaging', ack='auto',
                  headers={'persistent': 'true', 'vo': 'atlas'})
        self.logger.debug('sent message')

    def close_connections(self) -> None:
        """Disconnect and close all open broker connections."""
        for conn in self.connections:
            try:
                conn.disconnect()
            except Exception as exc:
                self.logger.warning(f'exception caught while closing connections: {exc}')
            else:
                self.logger.debug(f'closed connection to {conn}')

    def get_credentials(self) -> None:
        """Download ActiveMQ credentials from the PanDA server.

        Fetches ``MB_USERNAME`` and ``MB_PASSWORD`` from the ``get_user_secrets``
        endpoint and stores them in ``self.username`` and ``self.password``. Does
        nothing if ``pandaurl`` or ``pandaport`` are not configured.
        """
        if not self.pandaurl or self.pandaport == 0:
            self.logger.warning('PanDA server URL and/or port not set - cannot get ActiveMQ credentials')
            return

        cmd = https.get_server_command(self.pandaurl, self.pandaport, cmd=CREDENTIALS_ENDPOINT)
        if not cmd:
            return

        self.logger.info(f'executing server command: {cmd}')
        # note: no OIDC token - the secrets are bound to the proxy identity, so panda=False
        # (the default) is required; with panda=True the call succeeds but returns data="{}"
        res = https.request2(cmd, params={'keys': list(MB_CREDENTIAL_KEYS)}, method='GET')

        try:
            self.username, self.password = extract_credentials(res)
        except ValueError as exc:
            # never log res itself: it carries the password
            self.logger.warning(f'failed to get ActiveMQ credentials: {exc}')
            return

        self.logger.info(f'got ActiveMQ credentials for {self.username} '
                         f'(password length {len(self.password)})')
