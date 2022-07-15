#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2022

import socket
import json
import random
import logging

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


class Listener(connectionlistener):
    """
    Messaging listener.
    """

    messages = []

    def __init__(self, broker=None, queues=None):
        """
        Init function.

        :param broker:
        :param queues: queues
        """

        self.__broker = None
        self.__queues = queues
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
        if body not in [_obj for _obj in list(self.__queues.mbmessages.queue)]:
            self.__queues.mbmessages.put(body)
        #if body not in self.messages:
        #    self.messages.append(body)

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
    pandaport = 0
    pandaurl = ''
    topic = ''
    receive_topics = [topic]
    username = ''
    password = ''
    listener = None
    queues = None

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
        self.username = None
        self.password = None
        #self.username = kwargs.get('username', '')
        #self.password = kwargs.get('password', '')
        self.connections = []
        self.pandaurl = kwargs.get('pandaurl', '')
        self.pandaport = kwargs.get('pandaport', 0)
        self.queues = kwargs.get('queues', None)

        # get credentials from the PanDA server, abort if not returned
        res = self.get_credentials()
        self.logger.debug(f'secrets res={res}')
        return

        # get the list of brokers to use
        _addrinfos = socket.getaddrinfo(self.broker, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
        self.brokers_resolved = [_ai[4][0] for _ai in _addrinfos]

        receive_topic = self.receive_topics[0]

        # prepare the connections
        for broker in self.brokers_resolved:
            try:
                conn = stomp.Connection12(host_and_ports=[(broker, self.receiver_port)],
                                          keepalive=True)
            except Exception:  # primarily used to avoid interpreted problem with stomp is not available
                pass
            else:
                if conn not in self.connections:
                    self.connections.append(conn)

        self.listener = Listener(queues=self.queues)
        # setup the connections (once setup, the listener will wait for messages)
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

    def get_credentials(self):
        """
        Download username+password from the PanDA server for ActiveMQ authentication.

        :return: credentials dictionary.
        """

        res = {}
        if not self.pandaurl or self.pandaport == 0:
            self.logger.warning('PanDA server URL and/or port not set - cannot get ActiveMQ credentials')
            return {}

        data = {'get_json': True, 'keys': 'MB_USERNAME,MB_PASSWORD'}
        cmd = https.get_server_command(self.pandaurl, self.pandaport, cmd='get_user_secrets')
        if cmd != "":
            logger.info(f'executing server command: {cmd}')
            res = https.request(cmd, data=data)

        return res
