#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wguan.icedew@gmail.com, 2022
# - Paul Nilsson, paul.nilsson@cern.ch, 2022

import json
import logging
import socket
import sys
import time

try:
    import stomp
except ModuleNotFoundError:
    pass

from pilot.common.errorcodes import ErrorCodes
#from pilot.common.exception import PilotException

logger = logging.getLogger(__name__)
errors = ErrorCodes()


class MessagingListener(stomp.ConnectionListener):
    """
    Messaging Listener
    """
    def __init__(self, broker):
        """
        Init function.

        :param broker:
        """

        self.__broker = broker
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, frame):
        """
        Error handler.

        :param frame:
        """
        self.logger.error('[broker] [%s]: %s' % (self.__broker, frame))

    def on_message(self, frame):
        """
        on_message

        :param frame:
        """
        self.logger.info("received message: %s" % str(frame))


class Receiver(stomp.ConnectionListener):
    """
    Messaging Receiver
    """
    def __init__(self, broker):
        """
        Init function.

        :param broker:
        """

        self.__broker = broker
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, frame):
        """
        Error handler.

        :param frame:
        """

        self.logger.error('[broker] [%s]: %s', self.__broker, frame)

    def on_message(self, frame):
        """
        on_message

        :param frame:
        """

        body = frame
        self.logger.info("received message: %s", body)
        body = json.loads(body.body)
        if 'msg_type' in body and body['msg_type'] == 'health_heartbeat':
            self.logger.info("received message: %s", body)


class ActiveMQ(object):
    """
    ActiveMQ class
    """

    broker = '128.0.0.1'
    brokers_resolved = []
    receiver_port = 61613
    port = 61613
    topic = '/topic/pandaidds'
    receive_topics = ['/topic/pandaidds']
    username = 'pilot'
    password = 'trustno1'
    ssl_key = ''
    ssl_cert = ''

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs: kwargs dictionary.
        """

        self.broker = kwargs.get('broker', '')
        self.receiver_port = kwargs.get('receiver_port', '')
        self.port = kwargs.get('port', '')
        self.topic = kwargs.get('topic', '')
        self.receive_topics = kwargs.get('receive_topics', '')
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')
        self.ssl_key = kwargs.get('ssl_key', '')
        self.ssl_cert = kwargs.get('ssl_cert', '')

        _addrinfos = socket.getaddrinfo(self.broker, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
        self.brokers_resolved = [_ai[4][0] for _ai in _addrinfos]

    def send_message(self):
        """
        Send a test message to the ActiveMQ queue.
        """

        for broker in self.brokers_resolved:
            if self.ssl_key and self.ssl_cert:
                conn = stomp.Connection12(host_and_ports=[(broker, self.port)],
                                          keepalive=True,
                                          ssl_key_file=self.ssl_key,
                                          ssl_cert_file=self.ssl_cert,
                                          use_ssl=True)
                conn.set_listener('message-sender', MessagingListener(conn.transport._Transport__host_and_ports[0]))
            else:
                conn = stomp.Connection12(host_and_ports=[(broker, self.port)],
                                          keepalive=True,
                                          use_ssl=False)
                conn.set_listener('message-sender', MessagingListener(conn.transport._Transport__host_and_ports[0]))
                conn.connect(self.username, self.password, wait=True)

            # conn.start()

            payload = {"files": [
                {"adler32": "0cc737eb", "scope": "mock", "bytes": 1, "name": "file_ac6db48094974e039aa9d1ae52873596"}],
                       "rse": "WJ-XROOTD", "operation": "add_replicas", "lifetime": 2}

            logger.debug("sending message")
            try:
                for i in range(10):
                    conn.send(destination=self.topic, body=json.dumps(payload), id='atlas-idds-messaging', ack='auto',
                              headers={'persistent': 'true', 'vo': 'atlas'})
            except Exception as ex:
                logger.warning(ex)

            logger.debug("sent message")

    def receive_message(self):
        """
        Receive a message from ActiveMQ.
        """

        for receive_topic in self.receive_topics:
            print('start receiver: %s' % receive_topic)
            if self.ssl_key and self.ssl_cert:
                conn = stomp.Connection12(host_and_ports=[(self.broker, self.receiver_port)],
                                          keepalive=True,
                                          ssl_key_file=self.ssl_key,
                                          ssl_cert_file=self.ssl_cert,
                                          use_ssl=True)
            else:
                conn = stomp.Connection12(host_and_ports=[(self.broker, self.receiver_port)],
                                          keepalive=True,
                                          use_ssl=False)
                conn.connect(self.username, self.password, wait=True)
            conn.set_listener('message-receiver', Receiver(conn.transport._Transport__host_and_ports[0]))

            # conn.start()
            conn.subscribe(destination=receive_topic, id='atlas-idds-messaging', ack='auto')
