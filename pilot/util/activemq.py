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
import random
import threading

try:
    import stomp
except ModuleNotFoundError:
    pass

from pilot.common.errorcodes import ErrorCodes
#from pilot.common.exception import PilotException

logger = logging.getLogger(__name__)
errors = ErrorCodes()

graceful_stop = threading.Event()


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
        self.logger.error(f'[broker] [{self.__broker}]: {frame}')

    def on_message(self, frame):
        """
        on_message

        :param frame:
        """
        self.logger.info(f"received message: {frame}")


class Receiver(stomp.ConnectionListener):
    """
    Messaging Receiver
    """

    messages = []

    def __init__(self):
        """
        Init function.
        """

        self.__broker = None
        self.logger = logging.getLogger(self.__class__.__name__)

    def on_error(self, frame):
        """
        Error handler.

        :param frame:
        """

        self.messages.append(f'ERROR: {frame.body}')

    def on_message(self, frame):
        """
        on_message

        :param frame:
        """

        body = json.loads(frame.body)
        self.logger.info(f"received message: {body} (broker={self.__broker})")
        if body not in self.messages:
            self.messages.append(body)

        graceful_stop.set()


class ActiveMQ(object):
    """
    ActiveMQ class
    """

    broker = '128.0.0.1'
    brokers_resolved = []
    receiver_port = 0
    port = 0
    topic = '/topic/doma.panda_idds'
    receive_topics = [topic]
    username = 'atlpndpilot'
    password = ''

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
        self.connections = []
        self.cid = kwargs.get('id', None)
        self.vo = kwargs.get('vo', None)
        _addrinfos = socket.getaddrinfo(self.broker, 0, socket.AF_INET, 0, socket.IPPROTO_TCP)
        self.brokers_resolved = [_ai[4][0] for _ai in _addrinfos]

        self.logger = logging.getLogger(self.__class__.__name__)

    def send_message(self):
        """
        Send a test message to the ActiveMQ queue.
        """

        broker = random.choice(self.brokers_resolved)
        self.logger.info(f'will send message to random broker: {broker}')
        conn = stomp.Connection12(host_and_ports=[(broker, self.port)], keepalive=True)
        conn.set_listener('message-sender', MessagingListener(conn.transport._Transport__host_and_ports[0]))
        conn.connect(self.username, self.password, wait=True)

        message = {'msg_type': 'get_job', 'taskid': 1234, 'jobid': 123456}

        self.logger.info("sending message")
        try:
            conn.send(destination=self.topic,
                      body=json.dumps(message),
                      id=self.cid,
                      ack='auto',
                      headers={'persistent': 'true', 'vo': self.vo})
        except Exception as exc:
            self.logger.error(exc)
        else:
            self.logger.info("sent message")

    def receive_message(self):
        """
        Receive a message from ActiveMQ.
        """

        receive_topic = self.receive_topics[0]

        for broker in self.brokers_resolved:
            conn = stomp.Connection12(host_and_ports=[(broker, self.receiver_port)],
                                      keepalive=True)

            if not conn in self.connections:
                self.connections.append(conn)

        receiver = Receiver()
        messages = []
        while not graceful_stop.is_set():
            for conn in self.connections:
                if not conn.is_connected():
                    self.logger.info(f'connecting to {conn.transport._Transport__host_and_ports[0]}')
                    receiver.set_broker(conn.transport._Transport__host_and_ports[0])
                    conn.set_listener('message-receiver', receiver)
                    conn.connect(self.username, self.password, wait=True)
                    conn.subscribe(destination=receive_topic,
                                   id=self.cid,
                                   ack='auto')
                    messages = receiver.get_messages()
                    if messages:
                        break

            time.sleep(1)

        self.close_connections()
        return messages