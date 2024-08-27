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
# - Wen Guan, wen.guan@cern.ch, 2024

"""Loki Real-time logger."""

import atexit
import gzip
import json
import logging
import os
import queue
import requests
import threading
import time

logger = logging.getLogger(__name__)


class PilotLokiLoggerFormatter:
    def __init__(self):
        pass

    def format(self, record):
        formatted = {
            "timestamp": record.created,
            "process": record.process,
            "thread": record.thread,
            "function": record.funcName,
            "module": record.module,
            "name": record.name,
            "level": record.levelname,
        }

        record_keys = set(record.__dict__.keys())
        for key in record_keys:
            if key not in formatted and key not in ['msg']:
                formatted[key] = getattr(record, key)

        message = record.msg
        try:
            if type(message) in [dict]:
                msg = message.copy()
            elif type(message) in [str]:
                try:
                    msg = json.loads(message)
                except Exception:
                    msg = message

            if type(msg) in [dict]:
                origin_message = msg.get('message', None)
                if origin_message is not None:
                    formatted['message'] = origin_message
                    msg.pop('message')
                    for key in msg:
                        formatted[key] = msg[key]
                else:
                    formatted['message'] = message
            else:
                formatted['message'] = message
        except Exception as ex:
            logger.warn(f"Format exception: {ex}")
        return formatted


class PilotLokiLoggerHandler(logging.Handler):
    _success_response_code = 204

    def __init__(
        self,
        url,
        label_keys=None,
        timeout=10,
        compressed=True,
        name='pilot',
        formatter=PilotLokiLoggerFormatter(),
        verbose=False
    ):
        super().__init__()

        self.url = url
        self.label_keys = label_keys
        self.compressed = compressed
        self.timeout = timeout
        self.formatter = formatter
        self.verbose = verbose

        self.default_keys = {"namespace": "usdf-panda", "app": name, "env": "production"}
        self.queue = queue.Queue()
        self._lock = threading.RLock()
        self._graceful_stop = threading.Event()
        self.session = requests.Session()
        self._thread = threading.Thread(target=self._runner, daemon=True)
        self._thread.start()

    def emit(self, record):
        msg = self.formatter.format(record)
        self.queue.put(msg)

    def _sleep(self, timeout=10):
        time_start = time.time()
        while not self._graceful_stop.is_set():
            if time.time() - time_start > timeout:
                break
            time.sleep(1)

    def stop(self):
        self._graceful_stop.set()
        self._send()

    def _send(self, data):
        response = None
        try:
            headers = {"Content-type": "application/json"}
            if self.compressed:
                headers["Content-Encoding"] = "gzip"
                data = gzip.compress(bytes(data, "utf-8"))

            if self.verbose:
                logger.warn(f"url: {self.url}, headers: {headers}, data: {data}")
            response = self.session.post(self.url, data=data, headers=headers)
            if response.status_code != self._success_response_code:
                err = f"Failed to send logs: {response.status_code}, {response.text}"
                raise Exception(err)
        except Exception as e:
            raise Exception(f"Error while sending logs: {e}")

        finally:
            if response:
                response.close()

    def format_stream_messages(self, msgs):
        streams = {}
        for msg in msgs:
            for k in self.default_keys:
                if k not in msg:
                    msg[k] = self.default_keys[k]

            ts = str(int(msg.get("timestamp") * 1e9))
            msg.pop("timestamp")

            if self.label_keys:
                # only allowed labels will be put into the stream
                keys = {}
                others = {}
                for k in msg:
                    if k in self.label_keys:
                        keys[k] = msg[k]
                    else:
                        others[k] = msg[k]
                message = others
            else:
                message = msg['message']
                keys = msg.copy()
                keys.pop('message')

            sorted_keys = dict(sorted(keys.items()))
            key = ",".join(f'{key}:{value}' for key, value in sorted_keys.items())
            if key not in streams:
                stream = {k: str(msg[k]) for k in sorted_keys}
                streams[key] = {'stream': stream, 'values': []}
            if type(message) in [dict] and len(message.keys()) == 1 and list(message.keys())[0] == "message":
                message = message["message"]
            elif type(message) not in [str]:
                message = json.dumps(message)
            streams[key]['values'].append([ts, message])

        data = {"streams": []}
        for key in streams:
            stream = streams[key]
            data['streams'].append(stream)
        return json.dumps(data)

    def _flush(self):
        msgs = []
        while not self.queue.empty():
            msg = self.queue.get()
            msgs.append(msg)

        try:
            if msgs:
                stream_msgs = self.format_stream_messages(msgs)
                self._send(stream_msgs)
        except Exception as ex:
            logger.warn(f"Failed for sending message: {ex}")
            # put messages back
            for msg in msgs:
                self.queue.put(msg)

    def _runner(self):
        atexit.register(self._flush)

        while not self._graceful_stop.is_set():
            self._flush()
            self._sleep(self.timeout)


def setup_loki_handler(name):
    """Setup the Loki logger handler."""

    loki_labelkeys = None
    try:
        label_keys = os.environ.get('LOKI_LABELKEYS', None)
        if label_keys:
            label_keys = json.loads(label_keys)
            loki_labelkeys = label_keys
    except Exception as ex:
        logger.warning(f'failed to load LOKI_LABELKEYS from environment: {ex}')

    try:
        loki_period = int(os.environ.get('LOKI_PERIOD', 30))
    except Exception as ex:
        logger.warning(f'failed to load LOKI_PERIOD from environment: {ex}')
        loki_period = 30

    try:
        loki_verbose = os.environ.get('LOKI_VERBOSE', False)
        if loki_verbose and loki_verbose.lower() == 'true':
            loki_verbose = True
    except Exception as ex:
        logger.warning(f'failed to load LOKI_VERBOSE from environment: {ex}')
        loki_verbose = False

    _handler = PilotLokiLoggerHandler(
        url=os.environ["LOKI_URL"],
        label_keys=loki_labelkeys,
        timeout=loki_period,
        formatter=PilotLokiLoggerFormatter(),
        compressed=False,
        name=name,
        verbose=loki_verbose
    )
    return _handler