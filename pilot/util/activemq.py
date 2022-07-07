#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2022

import time
import socket
import json
import random
import logging
# import threading

try:
    import stomp
except ModuleNotFoundError:
    pass

from pilot.common.errorcodes import ErrorCodes
#from pilot.common.exception import PilotException

logger = logging.getLogger(__name__)
errors = ErrorCodes()

#graceful_stop = threading.Event()


class Listener(stomp.ConnectionListener):
    """
    Messaging listener.
    """

    messages = []

    def __init__(self, broker=None):
        """
        Init function.

        :param broker:
        """

        self.__broker = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def set_broker(self, broker):
        """
        Define broker for internal use.

        :param broker:
        """

        self.__broker = broker

    def on_error(self, frame):
        """
        Error handler.

        :param frame:
        """

        self.logger.warning('received an error "%s"' % frame)
        # store error in messages?

    def on_message(self, frame):
        """
        Message handler.

        :param frame:
        """

        self.logger.info('received a message "%s"' % frame.body)
        body = json.loads(frame.body)
        if body not in self.messages:
            self.messages.append(body)

    def get_messages(self):
        """
        Return stored messages to user.
        """

        return self.messages


class ActiveMQ(object):
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
    topic = ''
    receive_topics = [topic]
    username = ''
    password = ''
    listener = None

    def __init__(self, **kwargs):
        """
        Init function.
        Note: the init function sets up all connections and starts the listener.

        :param kwargs: kwargs dictionary.
        """

        self.logger = logging.getLogger(self.__class__.__name__)
        self.broker = kwargs.get('broker', '')
        self.receiver_port = kwargs.get('receiver_port', '')
        self.port = kwargs.get('port', '')
        self.topic = kwargs.get('topic', '')
        self.receive_topics = kwargs.get('receive_topics', '')
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.connections = []

        _addrinfos = socket.getaddrinfo(self.broker, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
        self.brokers_resolved = [_ai[4][0] for _ai in _addrinfos]

        receive_topic = self.receive_topics[0]

        for broker in self.brokers_resolved:
            conn = stomp.Connection12(host_and_ports=[(broker, self.receiver_port)],
                                      keepalive=True)
            if not conn in self.connections:
                self.connections.append(conn)

        self.listener = Listener()
        for conn in self.connections:
            self.logger.debug(f'conn={conn}')
            if not conn.is_connected():
                self.listener.set_broker(conn.transport._Transport__host_and_ports[0])
                conn.set_listener('message-receiver', self.listener)
                conn.connect(self.username, self.password, wait=True)
                self.logger.debug(f'topic={receive_topic}')
                conn.subscribe(destination=receive_topic,
                               id='atlas-pilot-messaging',
                               ack='auto')
                self.logger.debug('subscribed')

    def get_messages(self):
        """
        Return messages to user.
        """
        self.logger.debug(f'getting messages from {self.listener}')
        return self.listener.get_messages() if self.listener else []

    def send_message(self, message):
        """
        Send a message to a topic or queue.
        """

        conn = random.choice(self.connections)
        self.logger.debug(f'sending to {conn} topic/queue={self.topic}')
        conn.send(destination=self.topic, body=json.dumps(message), id='atlas-pilot-messaging', ack='auto',
                  headers={'persistent': 'true', 'vo': 'atlas'})
        self.logger.debug('sent message')

    def close_connections(self):
        """
        Close all open connections.
        """

        for conn in self.connections:
            try:
                conn.disconnect()
            except Exception as exc:
                self.logger.warning(f'exception caught while closing connections: {exc}')
            else:
                self.logger.debug('closed connection')
