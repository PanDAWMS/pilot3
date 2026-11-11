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

"""Unit tests for the dmesg memory scan's payload window.

Production symptom: on a node where process ids wrap within hours, a payload subprocess
(here a ``sleep 60``) carried the id of a python process the kernel had OOM-killed for an
earlier job. The scan matched the old kernel message by id alone and reported
PAYLOADOUTOFMEMORY (1212) on a job that had failed for an unrelated reason. The scan now
counts only messages logged after the kernel log's newest message at payload start.
"""

import unittest
from unittest import mock

from pilot.control import payload as payload_module
from pilot.util import processes

STALE = ('[1803211.402117] Memory cgroup out of memory: Killed process 939046 (python) '
         'total-vm:4794092kB, anon-rss:3630844kB, file-rss:323060kB, shmem-rss:0kB, UID:13200140')
FRESH = ('[1931502.118204] Memory cgroup out of memory: Killed process 939046 (python) '
         'total-vm:4794092kB, anon-rss:3630844kB, file-rss:323060kB, shmem-rss:0kB, UID:13200140')
UNSTAMPED = ('Memory cgroup out of memory: Killed process 939046 (python) '
             'total-vm:4794092kB, anon-rss:3630844kB')
MARK = 1930000.000001  # the newest kernel message when the payload started


def fake_execute(output):
    """An execute() that answers every command with ``output``."""
    return mock.patch.object(payload_module, 'execute', return_value=(0, output, ''))


class TestKernelLogTime(unittest.TestCase):
    """Reading the dmesg time stamp."""

    def test_stamp_is_read(self):
        self.assertEqual(processes.get_kernel_log_time(STALE), 1803211.402117)

    def test_padded_stamp_is_read(self):
        self.assertEqual(processes.get_kernel_log_time('[   12.500000] usb 1-1: new device'), 12.5)

    def test_line_without_stamp(self):
        self.assertIsNone(processes.get_kernel_log_time(UNSTAMPED))

    def test_mark_is_the_newest_stamp(self):
        out = '[  10.000000] a\n[  20.000000] b\nno stamp\n'
        with mock.patch.object(processes, 'execute', return_value=(0, out, '')):
            self.assertEqual(processes.get_kernel_log_mark(), 20.0)

    def test_mark_is_none_when_dmesg_is_unreadable(self):
        with mock.patch.object(processes, 'execute', return_value=(1, '', 'dmesg: read kernel buffer failed')):
            self.assertIsNone(processes.get_kernel_log_mark())


class TestScanWindow(unittest.TestCase):
    """Only kills logged while the payload ran are counted."""

    subprocesses = [939034, 939035, 939046]

    def test_kill_logged_before_the_payload_is_ignored(self):
        with fake_execute(STALE):
            self.assertEqual(payload_module.scan_for_memory_errors(self.subprocesses, since=MARK), '')

    def test_kill_logged_during_the_payload_is_counted(self):
        with fake_execute(STALE + '\n' + FRESH):
            diagnostics = payload_module.scan_for_memory_errors(self.subprocesses, since=MARK)
        self.assertTrue(diagnostics.startswith('Memory cgroup out of memory: Killed process 939046'))

    def test_without_a_mark_every_kill_counts_as_before(self):
        with fake_execute(STALE):
            diagnostics = payload_module.scan_for_memory_errors(self.subprocesses)
        self.assertTrue(diagnostics.startswith('Memory cgroup out of memory: Killed process 939046'))

    def test_unstamped_message_counts_as_before(self):
        with fake_execute(UNSTAMPED):
            diagnostics = payload_module.scan_for_memory_errors(self.subprocesses, since=MARK)
        self.assertTrue(diagnostics.startswith('Memory cgroup out of memory: Killed process 939046'))

    def test_kill_of_an_unknown_process_is_still_ignored(self):
        with fake_execute(FRESH.replace('939046', '2939046')):
            self.assertEqual(payload_module.scan_for_memory_errors(self.subprocesses, since=MARK), '')


if __name__ == '__main__':
    unittest.main()
