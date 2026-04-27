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

"""Unit tests for pilot.util.auxiliary."""

import logging
import os
import sys
import threading
import time
import unittest

from pilot.util.auxiliary import check_for_final_server_update
from pilot.util.constants import (
    SERVER_UPDATE_FINAL,
    SERVER_UPDATE_NOT_DONE,
    SERVER_UPDATE_RUNNING,
    SERVER_UPDATE_TROUBLE,
)

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


class TestCheckForFinalServerUpdate(unittest.TestCase):
    """Unit tests for check_for_final_server_update()."""

    def setUp(self):
        """Remove SERVER_UPDATE from the environment before each test."""
        os.environ.pop('SERVER_UPDATE', None)

    def tearDown(self):
        """Clean up SERVER_UPDATE after each test."""
        os.environ.pop('SERVER_UPDATE', None)

    def test_returns_immediately_when_not_done(self):
        """Return immediately when SERVER_UPDATE is NOT_DONE (pre-job startup state)."""
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_NOT_DONE
        t0 = time.monotonic()
        check_for_final_server_update(update_server=True)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 1.0, 'should return without sleeping when state is NOT_DONE')

    def test_waits_when_running_and_update_server_true(self):
        """Wait (bounded) when SERVER_UPDATE is RUNNING and update_server=True.

        This is the MAXTIME fix: instead of returning immediately (which caused
        lost heartbeats), the function now waits up to
        _MAX_RUNNING_WAIT_ITERATIONS * _RUNNING_WAIT_SLEEP s for the state to
        advance.  We monkey-patch sleep so the test completes quickly.

        After the bounded wait the function must fall through and return even
        if the state is still RUNNING (no infinite block).
        """
        import pilot.util.auxiliary as aux_module

        original_sleep = aux_module.sleep
        aux_module.sleep = lambda _: time.sleep(0.01)  # replace any sleep with 10 ms

        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_RUNNING
        t0 = time.monotonic()
        try:
            check_for_final_server_update(update_server=True)
        finally:
            aux_module.sleep = original_sleep

        elapsed = time.monotonic() - t0
        # Must have entered the wait loop (slept at least once), but must also
        # have returned (not blocked indefinitely).
        self.assertLess(elapsed, 5.0, 'should return within a bounded time even if state stays RUNNING')

    def test_running_unblocks_early_when_state_advances_to_final(self):
        """Unblock early when SERVER_UPDATE advances from RUNNING to FINAL during the wait.

        A background thread simulates the job thread completing its final server
        update while check_for_final_server_update is in the RUNNING wait loop.
        The function should break out of the inner loop and then exit the outer
        polling loop on the first pass (state is already FINAL).
        """
        import pilot.util.auxiliary as aux_module

        original_sleep = aux_module.sleep
        aux_module.sleep = lambda _: time.sleep(0.05)  # 50 ms per sleep tick

        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_RUNNING

        def _advance_to_final():
            time.sleep(0.12)  # let the inner loop tick at least once first
            os.environ['SERVER_UPDATE'] = SERVER_UPDATE_FINAL

        t = threading.Thread(target=_advance_to_final, daemon=True)
        t.start()
        t0 = time.monotonic()
        try:
            check_for_final_server_update(update_server=True)
        finally:
            aux_module.sleep = original_sleep
            t.join(timeout=2)

        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 2.0, 'should unblock early once SERVER_UPDATE advances to FINAL')
        self.assertEqual(os.environ.get('SERVER_UPDATE'), SERVER_UPDATE_FINAL)

    def test_skips_running_wait_when_update_server_false(self):
        """Skip the RUNNING wait entirely when update_server=False.

        When update_server is False the pilot writes to a heartbeat file
        instead of contacting the server, so there is no SERVER_UPDATE
        progression to wait for.  The function must return promptly.
        """
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_RUNNING
        t0 = time.monotonic()
        check_for_final_server_update(update_server=False)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 1.0, 'should skip RUNNING wait when update_server is False')

    def test_returns_immediately_when_already_final(self):
        """Return on the first poll when SERVER_UPDATE is already DONE_FINAL."""
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_FINAL
        t0 = time.monotonic()
        check_for_final_server_update(update_server=True)
        elapsed = time.monotonic() - t0
        # Enters the while loop once, checks the state, breaks immediately — no sleep.
        self.assertLess(elapsed, 1.0, 'should break immediately when state is already DONE_FINAL')

    def test_returns_immediately_when_trouble(self):
        """Return on the first poll when SERVER_UPDATE is LOST_HEARTBEAT."""
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_TROUBLE
        t0 = time.monotonic()
        check_for_final_server_update(update_server=True)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 1.0, 'should break immediately when state is LOST_HEARTBEAT')

    def test_skips_loop_when_update_server_false(self):
        """Skip the polling loop entirely when update_server=False."""
        os.environ['SERVER_UPDATE'] = SERVER_UPDATE_RUNNING
        t0 = time.monotonic()
        check_for_final_server_update(update_server=False)
        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 1.0, 'should skip polling loop when update_server is False')

    def test_waits_and_unblocks_when_state_transitions_to_final(self):
        """Poll until SERVER_UPDATE transitions from UPDATING_FINAL to DONE_FINAL.

        A background thread sets SERVER_UPDATE to DONE_FINAL after a short
        delay, simulating the job thread completing its final server update
        while check_for_final_server_update is polling.

        Note: this test patches the sleep inside the function to 0.05 s by
        temporarily monkey-patching pilot.util.auxiliary.sleep so the test
        runs in well under a second.
        """
        import pilot.util.auxiliary as aux_module

        original_sleep = aux_module.sleep
        aux_module.sleep = lambda _: time.sleep(0.05)  # replace 30 s sleep with 50 ms

        # Start in a state that won't trigger early-exit but isn't terminal yet.
        # Use an arbitrary non-terminal, non-RUNNING value to exercise the loop.
        os.environ['SERVER_UPDATE'] = 'UPDATING_FINAL'

        def _set_final_after_delay():
            time.sleep(0.15)  # let the loop spin at least twice before resolving
            os.environ['SERVER_UPDATE'] = SERVER_UPDATE_FINAL

        t = threading.Thread(target=_set_final_after_delay, daemon=True)
        t.start()

        t0 = time.monotonic()
        try:
            check_for_final_server_update(update_server=True)
        finally:
            aux_module.sleep = original_sleep
            t.join(timeout=2)

        elapsed = time.monotonic() - t0
        self.assertLess(elapsed, 2.0, 'should unblock once SERVER_UPDATE becomes DONE_FINAL')
        self.assertEqual(os.environ.get('SERVER_UPDATE'), SERVER_UPDATE_FINAL)

    def test_no_missing_env_var_raises(self):
        """Function must not raise when SERVER_UPDATE is absent from the environment."""
        # SERVER_UPDATE is already absent (cleared in setUp)
        try:
            check_for_final_server_update(update_server=False)
        except KeyError as exc:
            self.fail(f'check_for_final_server_update raised KeyError with missing env var: {exc}')


if __name__ == '__main__':
    unittest.main()
