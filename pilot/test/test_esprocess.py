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
# - Wen Guan, wen.guan@cern.ch, 2017-18
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23

"""Unit tests for the esprocess package."""

import json
import logging
import os
import queue
import subprocess
import sys
import threading
import time
import unittest

from pilot.eventservice.esprocess.eshook import ESHook
from pilot.eventservice.esprocess.esmanager import ESManager
from pilot.eventservice.esprocess.esmessage import MessageThread
from pilot.eventservice.esprocess.esprocess import ESProcess

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def check_env() -> bool:
    """
    Check whether cvmfs is available.

    To be used to decide whether to skip some test functions.

    :return: True if cvmfs is available, otherwise False (bool).
    """
    return os.path.exists('/cvmfs/')


class TestESHook(ESHook):
    """A class implemented ESHook, to be used to test eventservice."""

    def __init__(self):
        """
        Initialize the hook class for tests.

        Read payload and event ranges from a file.
        Download evgen files which are needed to run payload.
        """
        with open('pilot/test/resource/eventservice_job.txt', 'r', encoding='utf-8') as job_file:
            job = json.load(job_file)
            self.__payload = job['payload']
            self.__event_ranges = job['event_ranges']  # doesn't exit

        if check_env():
            with subprocess.Popen('pilot/test/resource/download_test_es_evgen.sh', shell=True, stdout=subprocess.PIPE) as process:
                process.wait()
                if process.returncode != 0:
                    raise Exception(f'failed to download input files for es test: {process.communicate()}')
        else:
            logging.info("no CVMFS. skip downloading files.")

        self.__injected_event_ranges = []
        self.__outputs = []

    def get_payload(self) -> dict:
        """
        Get payload hook function for tests.

        :returns: dict {'executable': <cmd string>, 'output_file': <filename or without it>, 'error_file': <filename or without it>}
        """
        return self.__payload

    def get_event_ranges(self, num_ranges=1) -> list:
        """
        Get event ranges hook function for tests.

        :returns: list of event ranges (list).
        """
        ret = []
        for _ in range(num_ranges):
            if len(self.__event_ranges) > 0:
                event_range = self.__event_ranges.pop(0)
                ret.append(event_range)
                self.__injected_event_ranges.append(event_range)

        return ret

    def handle_out_message(self, message: dict):
        """
        Handle ES output or error messages hook function for tests.

        :param message: a dict of parsed message (dict).
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'failed', 'message': <full message>}.
        """

        print(message)
        self.__outputs.append(message)

    def get_injected_event_ranges(self) -> list:
        """
        Get event ranges injected to payload for test assertion.

        :returns: List of injected event ranges (list).
        """
        return self.__injected_event_ranges

    def get_outputs(self) -> list:
        """
        Get outputs for test assertion.

        :returns: List of outputs.
        """
        return self.__outputs


class TestESMessageThread(unittest.TestCase):
    """
    Unit tests for event service message thread.
    """

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_msg_thread(self):
        """Make sure that the es message thread works as expected."""
        _queue = queue.Queue()
        msg_thread = MessageThread(_queue, socket_name='test', context='local')
        self.assertIsInstance(msg_thread, threading.Thread)

        msg_thread.start()
        time.sleep(1)
        self.assertTrue(msg_thread.is_alive())

        msg_thread.send('test')
        msg_thread.stop()
        self.assertTrue(msg_thread.is_stopped())
        time.sleep(1)
        self.assertFalse(msg_thread.is_alive())


@unittest.skipIf(not check_env(), "No CVMFS")
class TestESProcess(unittest.TestCase):
    """Unit tests for event service process functions."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls._test_hook = TestESHook()
        cls._esProcess = ESProcess(cls._test_hook.get_payload())

    def test_set_get_event_ranges_hook(self):
        """Make sure that no exceptions to set get_event_ranges hook."""
        self._esProcess.set_get_event_ranges_hook(self._test_hook.get_event_ranges)
        self.assertEqual(self._test_hook.get_event_ranges, self._esProcess.get_get_event_ranges_hook())

    def test_set_handle_out_message_hook(self):
        """Make sure that no exceptions to set handle_out_message hook."""
        self._esProcess.set_handle_out_message_hook(self._test_hook.handle_out_message)
        self.assertEqual(self._test_hook.handle_out_message, self._esProcess.get_handle_out_message_hook())

    def test_parse_out_message(self):
        """Make sure to parse messages from payload correctly."""
        output_msg = '/tmp/HITS.12164365._000300.pool.root.1.12164365-3616045203-10980024041-4138-8,ID:12164365-3616045203-10980024041-4138-8,CPU:288,WALL:303'
        ret = self._esProcess.parse_out_message(output_msg)
        self.assertEqual(ret['status'], 'finished')
        self.assertEqual(ret['id'], '12164365-3616045203-10980024041-4138-8')

        error_msg1 = 'ERR_ATHENAMP_PROCESS 130-2068634812-21368-1-4: Failed to process event range'
        ret = self._esProcess.parse_out_message(error_msg1)
        self.assertEqual(ret['status'], 'failed')
        self.assertEqual(ret['id'], '130-2068634812-21368-1-4')

        error_msg2 = "ERR_ATHENAMP_PARSE \"u'LFN': u'eta0-25.evgen.pool.root',u'eventRangeID': u'130-2068634812-21368-1-4', u'startEvent': 5\": Wrong format"
        ret = self._esProcess.parse_out_message(error_msg2)
        self.assertEqual(ret['status'], 'failed')
        self.assertEqual(ret['id'], '130-2068634812-21368-1-4')


class TestEventService(unittest.TestCase):
    """Unit tests for event service functions."""

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_init_esmanager(self):
        """Make sure that no exceptions to init ESManager."""
        test_hook = TestESHook()
        es_manager = ESManager(test_hook)
        self.assertIsInstance(es_manager, ESManager)

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_run_es(self):
        """Make sure that ES produced all events that injected."""
        test_hook = TestESHook()
        es_manager = ESManager(test_hook)
        es_manager.run()
        injected_event = test_hook.get_injected_event_ranges()
        outputs = test_hook.get_outputs()

        self.assertEqual(len(injected_event), len(outputs))
        self.assertNotEqual(len(outputs), 0)
