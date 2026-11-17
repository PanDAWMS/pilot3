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
# Authors
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Unit tests for the serialisation of the final server update (update_server() in pilot.control.job).

Job 7378803325 (grid.40547911.7.out, 2026-10-23): job_monitor sent the final update after an abort, and the server
answered with 'tobekilled'. While job_monitor was killing the (already finished) payload, including a 3 s sleep inside
send_state() and before job.completed was set, queue_monitor passed the job.completed check in update_server() and
sent a second final update. Both threads then sent the pilot telemetry, and the server rejected the second document.
"""

import os
import shutil
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.control import job as job_module
from pilot.util import telemetry

JOB_ID = '7378803325'
WAIT = 5  # maximum time (s) to wait for a thread event, so that a regression fails instead of hanging


def make_job(workdir: str, **kwargs) -> SimpleNamespace:
    """Return a minimal failed job object suitable for update_server().

    Args:
        workdir: Job work directory.
        **kwargs: Attributes to add or override.

    Returns:
        Job object.
    """
    values = {'jobid': JOB_ID, 'state': 'failed', 'workdir': workdir, 'fileinfo': {}, 'indata': [], 'outdata': [],
              'logdata': [], 'lsetuptime': 0, 'completed': False}
    values.update(kwargs)
    return SimpleNamespace(**values)


def make_args(**kwargs) -> SimpleNamespace:
    """Return a minimal pilot arguments object.

    Args:
        **kwargs: Attributes to add or override.

    Returns:
        Arguments object.
    """
    values = {'timing': {}, 'update_server': True, 'url': 'https://pandaserver.example.org', 'port': 25443,
              'internet_protocol_version': 'IPv4'}
    values.update(kwargs)
    return SimpleNamespace(**values)


class TestJob7378803325(unittest.TestCase):
    """Concurrent update_server() calls for the same job send one final update and one telemetry document."""

    def setUp(self):
        """Use the generic user plugin, and reset the pilot singletons."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []
        self._cache_snapshot = dict(vars(get_pilot_cache()))
        self.env = patch.dict(os.environ, {'PILOT_USER': 'generic'}, clear=False)
        self.env.start()
        self.workdir = tempfile.mkdtemp()
        self.first_inside = threading.Event()  # the first thread is inside send_state()
        self.second_waiting = threading.Event()  # the second thread is waiting for the lock
        self.release_first = threading.Event()  # let the first thread finish send_state()
        self.errors = []

    def tearDown(self):
        """Restore the environment and the pilot singletons."""
        self.release_first.set()
        self.env.stop()
        shutil.rmtree(self.workdir, ignore_errors=True)
        cache = get_pilot_cache()
        vars(cache).clear()
        vars(cache).update(self._cache_snapshot)
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []

    def _fake_send_state(self, accepted: bool):
        """Return a send_state() replacement where the first call blocks, like the 'tobekilled' kill did.

        Args:
            accepted: Whether the first call is accepted (and completes the job).

        Returns:
            Replacement function.
        """
        calls = []

        def fake_send_state(job, _args, _state, **_kwargs):
            calls.append(job.jobid)
            if len(calls) == 1:
                self.first_inside.set()
                self.release_first.wait(WAIT)
                if accepted:
                    job.completed = True
                return accepted
            job.completed = True
            return True

        return fake_send_state

    def _run(self, job: SimpleNamespace, args: SimpleNamespace):
        """Run update_server() for the job in a thread, catching any exception.

        Args:
            job: Job object.
            args: Arguments object.
        """
        try:
            job_module.update_server(job, args)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.errors.append(exc)

    def _race(self, accepted: bool) -> tuple:
        """Let a second thread call update_server() while the first one is inside send_state().

        Args:
            accepted: Whether the first final update is accepted.

        Returns:
            (job, args, send_state mock, send_pilot_telemetry mock).
        """
        job, args = make_job(self.workdir), make_args()
        real_info = job_module.logger.info

        def info(msg, *a, **kw):
            if 'another thread is updating the server' in str(msg):
                self.second_waiting.set()
            return real_info(msg, *a, **kw)

        with patch.object(job_module, 'send_state', side_effect=self._fake_send_state(accepted)) as send_state, \
                patch.object(telemetry, 'send_pilot_telemetry') as send, \
                patch.object(job_module.logger, 'info', side_effect=info):
            first = threading.Thread(target=self._run, args=(job, args), name='job_monitor')
            first.start()
            self.assertTrue(self.first_inside.wait(WAIT))
            second = threading.Thread(target=self._run, args=(job, args), name='queue_monitor')
            second.start()
            waited = self.second_waiting.wait(WAIT)
            self.release_first.set()
            first.join(WAIT)
            second.join(WAIT)

        self.assertFalse(first.is_alive())
        self.assertFalse(second.is_alive())
        self.assertEqual(self.errors, [])
        self.assertTrue(waited, 'the second thread did not wait for the first one')
        return job, args, send_state, send

    def test_one_final_update_and_one_telemetry(self):
        """The second thread waits, finds the job completed, and sends nothing."""
        job, args, send_state, send = self._race(accepted=True)
        self.assertEqual(send_state.call_count, 1)
        send.assert_called_once_with(job, args, trace_snapshot=None)
        self.assertTrue(job.completed)

    def test_second_thread_retries_when_first_update_was_rejected(self):
        """If the first final update was not accepted, the second thread still sends it (and the telemetry)."""
        job, args, send_state, send = self._race(accepted=False)
        self.assertEqual(send_state.call_count, 2)
        send.assert_called_once_with(job, args, trace_snapshot=None)
        self.assertTrue(job.completed)

    def test_lock_released_after_exception(self):
        """An exception in the final update does not leave the lock held."""
        job, args = make_job(self.workdir), make_args()
        with patch.object(job_module, 'send_state', side_effect=RuntimeError('boom')), \
                patch.object(telemetry, 'send_pilot_telemetry'):
            with self.assertRaises(RuntimeError):
                job_module.update_server(job, args)
        # check from another thread, since the reentrant lock can always be acquired by the thread that holds it
        acquired = []

        def try_lock():
            if job_module.FINAL_UPDATE_LOCK.acquire(blocking=False):  # pylint: disable=consider-using-with
                acquired.append(True)
                job_module.FINAL_UPDATE_LOCK.release()

        thread = threading.Thread(target=try_lock)
        thread.start()
        thread.join(WAIT)
        self.assertEqual(acquired, [True])

    def test_uncontended_call_does_not_log_waiting(self):
        """A single caller does not report waiting for another thread."""
        job, args = make_job(self.workdir), make_args()

        def complete(_job, *_args, **_kwargs):
            _job.completed = True
            return True

        with patch.object(job_module, 'send_state', side_effect=complete), \
                patch.object(telemetry, 'send_pilot_telemetry') as send, \
                patch.object(job_module.logger, 'info') as info:
            job_module.update_server(job, args)
        send.assert_called_once_with(job, args, trace_snapshot=None)
        self.assertFalse(any('another thread' in str(call.args[0]) for call in info.call_args_list if call.args))


if __name__ == '__main__':
    unittest.main()
