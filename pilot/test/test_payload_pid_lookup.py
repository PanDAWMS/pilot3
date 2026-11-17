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
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Regression tests for PanDA job 7379005140 (CERN-PROD_UCORE_2, lxplus941).

The payload had been started in a container when the pilot looked up its process id for the
memory monitor. Reading ``/proc/1/cmdline`` was not permitted (``/proc`` mounted with hidepid=1,
reasoned), psutil raised ``AccessDenied``, and the exception went up to ``execute_payloads()``,
which failed the job while the payload kept running. The final server update was then delayed by
115 s, although the log had already been transferred.

Fixed here:

- unreadable processes are skipped when looking up processes by job id or command;
- a failure to start the utility after the payload (memory monitor) no longer stops the payload
  execution;
- the wait for the stage-out of an aborted job ends once the log transfer has been attempted;
- ``find_lingering_processes()`` caught the non-existent ``psutil.FileNotFoundError``.

psutil is optional for the pilot (see pilot.util.psutils) and is not installed in the GitHub CI, so the tests that
need it are skipped when it cannot be imported.
"""

import queue
import types
import unittest
from unittest.mock import MagicMock, patch

from pilot.control import job as job_control
from pilot.control.payloads.generic import Executor
from pilot.user.atlas import common as atlas_common
from pilot.user.atlas import utilities as atlas_utilities
from pilot.util import psutils
from pilot.util.constants import (
    LOG_TRANSFER_DONE,
    LOG_TRANSFER_FAILED,
    LOG_TRANSFER_IN_PROGRESS,
)

try:
    import psutil
except ImportError:
    psutil = None

JOB_ID = 7379005140
NO_PSUTIL = 'psutil is not installed'
PAYLOAD_PID = 3242352


class FakeProcess:  # pylint: disable=too-few-public-methods
    """A process whose command line is given, or raises the given exception."""

    def __init__(self, pid: int, cmdline=None, error: Exception = None):
        """Keep the process data."""
        self.pid = pid
        self._cmdline = cmdline or []
        self._error = error

    def cmdline(self) -> list:
        """Return the command line, or raise the error."""
        if self._error:
            raise self._error
        return self._cmdline


def processes_of_job_7379005140() -> list:
    """Return the process table seen on lxplus941: pid 1 cannot be read."""
    return [
        FakeProcess(1, error=psutil.AccessDenied(1)),
        FakeProcess(2, error=psutil.ZombieProcess(2)),
        FakeProcess(3, error=psutil.NoSuchProcess(3)),
        FakeProcess(4, [f'xrootd.{JOB_ID}.log']),
        FakeProcess(PAYLOAD_PID, ['/bin/bash', '-c', f'export PandaID={JOB_ID};source ...']),
    ]


@unittest.skipUnless(psutil, NO_PSUTIL)
class TestFindProcessByJobid(unittest.TestCase):
    """find_process_by_jobid() skips processes that cannot be read."""

    def test_job_7379005140(self):
        """The payload is found although pid 1 cannot be read."""
        with patch.object(psutils.psutil, 'process_iter', return_value=processes_of_job_7379005140()):
            self.assertEqual(psutils.find_process_by_jobid(JOB_ID), PAYLOAD_PID)

    def test_no_match(self):
        """None is returned when no readable process has the job id."""
        processes = [FakeProcess(1, error=psutil.AccessDenied(1)), FakeProcess(5, ['sleep', '10'])]
        with patch.object(psutils.psutil, 'process_iter', return_value=processes):
            self.assertIsNone(psutils.find_process_by_jobid(JOB_ID))

    def test_get_pid_for_jobid(self):
        """The ATLAS lookup used for the memory monitor finds the payload."""
        with patch.object(psutils.psutil, 'process_iter', return_value=processes_of_job_7379005140()):
            self.assertEqual(atlas_utilities.get_pid_for_jobid(JOB_ID), PAYLOAD_PID)


@unittest.skipUnless(psutil, NO_PSUTIL)
class TestGetCommandByPid(unittest.TestCase):
    """get_command_by_pid() returns None for processes that cannot be read."""

    def check(self, process, expected):
        """Check the command returned for a fake process."""
        with patch.object(psutils.psutil, 'Process', return_value=process):
            self.assertEqual(psutils.get_command_by_pid(process.pid), expected)

    def test_readable(self):
        """The command line is joined."""
        self.check(FakeProcess(7, ['python3', 'pilot.py']), 'python3 pilot.py')

    def test_access_denied(self):
        """An unreadable process gives None."""
        self.check(FakeProcess(1, error=psutil.AccessDenied(1)), None)

    def test_zombie(self):
        """A zombie process gives None."""
        self.check(FakeProcess(2, error=psutil.ZombieProcess(2)), None)

    def test_gone(self):
        """A process that has exited gives None."""
        self.check(FakeProcess(3, error=psutil.NoSuchProcess(3)), None)


@patch.object(psutils, '_is_psutil_available', True)  # the process helpers are mocked, so psutil is not needed
class TestFindActualPayloadPid(unittest.TestCase):
    """find_actual_payload_pid() skips children whose command is unknown."""

    def test_unreadable_child_is_skipped(self):
        """A child without a command does not stop the search."""
        commands = {10: None, 11: 'python Reco_tf.py --maxEvents=1'}
        with patch.object(psutils, 'get_subprocesses', return_value=[10, 11]), \
                patch.object(psutils, 'get_command_by_pid', side_effect=commands.get):
            self.assertEqual(psutils.find_actual_payload_pid(9, 'Reco_tf.py'), 11)

    def test_no_command_at_all(self):
        """None is returned if no child command can be read."""
        with patch.object(psutils, 'get_subprocesses', return_value=[10]), \
                patch.object(psutils, 'get_command_by_pid', return_value=None):
            self.assertIsNone(psutils.find_actual_payload_pid(9, 'Reco_tf.py'))


@unittest.skipUnless(psutil, NO_PSUTIL)
class TestFindLingeringProcesses(unittest.TestCase):
    """find_lingering_processes() handles errors (it caught the non-existent psutil.FileNotFoundError)."""

    def test_file_not_found(self):
        """A FileNotFoundError gives an empty list instead of an AttributeError."""
        with patch.object(psutils.psutil, 'Process', side_effect=FileNotFoundError('/proc/9/stat')):
            self.assertEqual(psutils.find_lingering_processes(9), [])

    def test_no_such_process(self):
        """A parent that has exited gives an empty list."""
        with patch.object(psutils.psutil, 'Process', side_effect=psutil.NoSuchProcess(9)):
            self.assertEqual(psutils.find_lingering_processes(9), [])


class TestUtilityAfterPayloadStarted(unittest.TestCase):
    """A failure to start the memory monitor does not stop the payload execution."""

    def executor(self) -> Executor:
        """Return an executor with only the attributes used here."""
        executor = Executor.__new__(Executor)
        executor._Executor__args = types.SimpleNamespace(baseurls='')  # pylint: disable=attribute-defined-outside-init
        return executor

    @unittest.skipUnless(psutil, NO_PSUTIL)
    def test_job_7379005140(self):
        """AccessDenied from the pid lookup is logged, not raised, and no utility is stored."""
        job = MagicMock(jobid=str(JOB_ID), workdir='/tmp', utilities={})
        command = {'command': 'MemoryMonitor', 'args': '', 'label': 'MemoryMonitor'}
        with patch.dict('os.environ', {'PILOT_USER': 'atlas'}), \
                patch.object(atlas_common, 'get_utility_commands', return_value=command), \
                patch.object(atlas_common, 'get_utility_command_setup', side_effect=psutil.AccessDenied(1)), \
                self.assertLogs('pilot.control.payloads.generic', level='ERROR') as logs:
            self.executor().utility_after_payload_started(job)
        self.assertEqual(job.utilities, {})
        self.assertTrue([line for line in logs.output if 'payload continues without it' in line])

    def test_no_error(self):
        """Without an error the utility is started as before."""
        executor = self.executor()
        with patch.object(Executor, '_start_utility_after_payload') as start:
            executor.utility_after_payload_started('job')
        start.assert_called_once_with('job')


class FakeClock:
    """Replacement for the time module: sleep() advances time() without waiting."""

    def __init__(self):
        """Start the clock."""
        self.now = 1000.0
        self.sleeps = 0

    def time(self) -> float:
        """Return the current fake time."""
        return self.now

    def sleep(self, seconds: float) -> None:
        """Advance the fake time."""
        self.now += seconds
        self.sleeps += 1


class TestWaitForAbortedJobStageout(unittest.TestCase):
    """wait_for_aborted_job_stageout() stops waiting once the log transfer has been attempted."""

    def setUp(self):
        """Use a fake clock and no kill signal."""
        self.clock = FakeClock()
        self.queues = types.SimpleNamespace(finished_data_out=queue.Queue(), failed_data_out=queue.Queue())
        self.args = types.SimpleNamespace(timing={})
        self.patches = [patch.object(job_control, 'time', self.clock),
                        patch.object(job_control, 'get_time_since', return_value=0),
                        patch.object(job_control, 'was_pilot_killed', return_value=False)]
        for _patch in self.patches:
            _patch.start()

    def tearDown(self):
        """Stop the patches."""
        for _patch in reversed(self.patches):
            _patch.stop()

    def wait(self, log_transfer: str) -> float:
        """Run the wait for a job with the given log transfer status, and return the waited time."""
        job = MagicMock()
        job.get_status.side_effect = lambda key: log_transfer if key == 'LOG_TRANSFER' else ''
        start = self.clock.now
        job_control.wait_for_aborted_job_stageout(self.args, self.queues, job)
        return self.clock.now - start

    def test_job_7379005140(self):
        """The log was already transferred and the job had left the data_out queues: no wait."""
        self.assertEqual(self.wait(LOG_TRANSFER_DONE), 0)

    def test_log_transfer_failed(self):
        """A failed log transfer has also been attempted: no wait."""
        self.assertEqual(self.wait(LOG_TRANSFER_FAILED), 0)

    def test_log_transfer_in_progress(self):
        """While the log transfer is in progress, the wait is bounded (115 s without a kill signal)."""
        waited = self.wait(LOG_TRANSFER_IN_PROGRESS)
        self.assertGreaterEqual(waited, 115)
        self.assertLess(waited, 116)

    def test_job_in_data_out_queue(self):
        """A job in a data_out queue has finished stage-out, as before."""
        job = MagicMock()
        job.get_status.return_value = LOG_TRANSFER_IN_PROGRESS
        self.queues.failed_data_out.put(job)
        job_control.wait_for_aborted_job_stageout(self.args, self.queues, job)
        self.assertEqual(self.clock.sleeps, 0)


if __name__ == '__main__':
    unittest.main()
