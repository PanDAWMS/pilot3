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

"""Unit tests for the MachineFeatures shutdowntime check in proceed_with_getjob().

Covers:
- _time_until_shutdown(): missing/empty MachineFeatures, missing/unparsable
  shutdowntime, stale (pre-pilot-start) shutdowntime, and the normal case.
- proceed_with_getjob(): refusal to fetch a new job when shutdowntime is
  imminent (regression test for ATLASPANDA-MAXTIME premature-abort bug),
  acceptance when enough time remains, the first-job exemption (jobnumber=0),
  and backward compatibility when args is not supplied.
"""

import logging
import os
import sys
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pilot.control import job as job_module

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def _make_args(pilot_start_time: float) -> SimpleNamespace:
    """Build a minimal args-like object sufficient for get_time_since_start().

    Args:
        pilot_start_time: epoch seconds to record as PILOT_START_TIME.

    Returns:
        SimpleNamespace: object with a `.timing` dict shaped like the real
            pilot args.timing structure.
    """
    return SimpleNamespace(timing={'0': {'PILOT_START_TIME': pilot_start_time}})


class TestTimeUntilShutdown(unittest.TestCase):
    """Tests for job._time_until_shutdown()."""

    @patch('pilot.control.job.MachineFeatures')
    def test_no_machinefeatures(self, mock_mf):
        """Return None when MachineFeatures().get() is empty/falsy."""
        mock_mf.return_value.get.return_value = {}
        args = _make_args(time.time() - 3600)
        self.assertIsNone(job_module._time_until_shutdown(args))

    @patch('pilot.control.job.MachineFeatures')
    def test_shutdowntime_not_set(self, mock_mf):
        """Return None when the shutdowntime key is present but empty."""
        mock_mf.return_value.get.return_value = {
            'hs06': '', 'shutdowntime': '', 'total_cpu': '', 'grace_secs': ''
        }
        args = _make_args(time.time() - 3600)
        self.assertIsNone(job_module._time_until_shutdown(args))

    @patch('pilot.control.job.MachineFeatures')
    def test_shutdowntime_unparsable(self, mock_mf):
        """Return None (and warn) when shutdowntime cannot be converted to int."""
        mock_mf.return_value.get.return_value = {'shutdowntime': 'not-a-number'}
        args = _make_args(time.time() - 3600)
        self.assertIsNone(job_module._time_until_shutdown(args))

    @patch('pilot.control.job.MachineFeatures')
    def test_shutdowntime_stale_before_pilot_start(self, mock_mf):
        """Return None when shutdowntime predates pilot start (stale value)."""
        now = time.time()
        pilot_start = now - 3600  # pilot started 1h ago
        stale_shutdowntime = pilot_start - 7200  # shutdowntime set 2h before pilot start
        mock_mf.return_value.get.return_value = {'shutdowntime': str(int(stale_shutdowntime))}
        args = _make_args(pilot_start)
        self.assertIsNone(job_module._time_until_shutdown(args))

    @patch('pilot.control.job.MachineFeatures')
    def test_shutdowntime_imminent(self, mock_mf):
        """Return a small positive remaining time when shutdown is close (the bug scenario)."""
        now = time.time()
        pilot_start = now - 6 * 3600  # pilot has been running 6h (multijob pilot)
        shutdowntime = now + 1107  # matches the ~18.5 min remaining seen in the incident log
        mock_mf.return_value.get.return_value = {'shutdowntime': str(int(shutdowntime))}
        args = _make_args(pilot_start)
        remaining = job_module._time_until_shutdown(args)
        self.assertIsNotNone(remaining)
        self.assertTrue(900 < remaining <= 1107)

    @patch('pilot.control.job.MachineFeatures')
    def test_shutdowntime_far_away(self, mock_mf):
        """Return a large remaining time when shutdown is not imminent."""
        now = time.time()
        pilot_start = now - 3600
        shutdowntime = now + 20000
        mock_mf.return_value.get.return_value = {'shutdowntime': str(int(shutdowntime))}
        args = _make_args(pilot_start)
        remaining = job_module._time_until_shutdown(args)
        self.assertIsNotNone(remaining)
        self.assertTrue(19000 < remaining <= 20000)


class TestTimeUntilShutdownNoMachineFeatures(unittest.TestCase):
    """Regression tests using the real (unmocked) MachineFeatures class.

    Most PanDA queues do not provide MachineFeatures at all (no MACHINEFEATURES
    env var, or it points to a path that doesn't exist). These tests exercise
    pilot.util.features.MachineFeatures directly -- not a mock -- to guarantee
    that _time_until_shutdown() degrades to None on such queues, and that
    proceed_with_getjob() therefore behaves exactly as it did before this fix
    was introduced.
    """

    def setUp(self):
        """Save and clear MACHINEFEATURES so each test starts from a known state."""
        self._saved_env = os.environ.pop('MACHINEFEATURES', None)

    def tearDown(self):
        """Restore the original MACHINEFEATURES environment, if any."""
        if self._saved_env is not None:
            os.environ['MACHINEFEATURES'] = self._saved_env

    def test_no_env_var_set(self):
        """The common case: MACHINEFEATURES is simply not set in the environment."""
        args = _make_args(time.time() - 3600)
        self.assertIsNone(job_module._time_until_shutdown(args))

    def test_env_var_points_to_missing_path(self):
        """A misconfigured queue: MACHINEFEATURES set but the path doesn't exist."""
        os.environ['MACHINEFEATURES'] = '/nonexistent/path/that/does/not/exist'
        args = _make_args(time.time() - 3600)
        self.assertIsNone(job_module._time_until_shutdown(args))

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_proceed_with_getjob_unaffected_when_no_machinefeatures(self, mock_space):
        """proceed_with_getjob() must behave exactly as before this fix when
        MachineFeatures is unavailable -- i.e. it must still accept jobs based
        purely on the pre-existing timefloor/proxy/disk-space checks.
        """
        args = _make_args(time.time() - 3600)
        proceed = job_module.proceed_with_getjob(
            timefloor=86400,
            starttime=time.time(),
            jobnumber=5,
            getjob_requests=1,
            max_getjob_requests=150,
            should_update_server=True,
            submitmode='PULL',
            harvester=False,
            verify_proxy=False,
            traces=MagicMock(pilot={'error_code': 0}),
            args=args,
        )
        self.assertTrue(proceed)


class TestProceedWithGetjobShutdowntime(unittest.TestCase):
    """Tests for the shutdowntime guard inside proceed_with_getjob().

    These tests stub out the unrelated checks in proceed_with_getjob() (proxy
    verification is skipped via verify_proxy=False, and local disk space via
    patching check_local_space) so that only the shutdowntime logic under test
    determines the outcome.
    """

    def _common_kwargs(self, args):
        """Build the common kwargs shared across proceed_with_getjob() calls in this test class.

        Args:
            args: the args-like object to pass through to proceed_with_getjob().

        Returns:
            dict: kwargs for proceed_with_getjob(), with proxy verification disabled
                and a generous timefloor so only the shutdowntime check is exercised.
        """
        return {
            'timefloor': 86400,
            'starttime': time.time(),
            'getjob_requests': 1,
            'max_getjob_requests': 150,
            'should_update_server': True,
            'submitmode': 'PULL',
            'harvester': False,
            'verify_proxy': False,
            'traces': MagicMock(pilot={'error_code': 0}),
            'args': args,
        }

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    @patch('pilot.control.job._time_until_shutdown', return_value=500)
    def test_refuses_job_when_shutdown_imminent(self, mock_remaining, mock_space):
        """proceed_with_getjob() must refuse a new job when shutdown is too close.

        Regression test for the bug where the pilot accepted job
        7193562359 with only ~1107s left before MachineFeatures shutdowntime,
        leading to an avoidable REACHED_MAXTIME abort 564s later.
        """
        args = _make_args(time.time() - 6 * 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1  # not the first job -- shutdowntime check applies
        proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertFalse(proceed)
        mock_remaining.assert_called_once()

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    @patch('pilot.control.job._time_until_shutdown', return_value=7200)
    def test_accepts_job_when_shutdown_not_imminent(self, mock_remaining, mock_space):
        """proceed_with_getjob() must proceed normally when ample time remains."""
        args = _make_args(time.time() - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1
        proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)
        mock_remaining.assert_called_once()

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    @patch('pilot.control.job._time_until_shutdown', return_value=None)
    def test_accepts_job_when_shutdowntime_unknown(self, mock_remaining, mock_space):
        """proceed_with_getjob() must proceed when shutdowntime is not known (None)."""
        args = _make_args(time.time() - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1
        proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    @patch('pilot.control.job._time_until_shutdown', return_value=10)
    def test_first_job_exempt_from_shutdowntime_check(self, mock_remaining, mock_space):
        """The shutdowntime check must not apply to the first job (jobnumber=0).

        The first job is special-cased throughout proceed_with_getjob() (e.g. the
        timefloor checks), since it is typically preplaced by the batch system /
        wrapper rather than actively fetched by a long-running pilot.
        """
        args = _make_args(time.time() - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 0
        proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)
        mock_remaining.assert_not_called()

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    @patch('pilot.control.job._time_until_shutdown')
    def test_no_args_skips_shutdowntime_check(self, mock_remaining, mock_space):
        """Backward compatibility: omitting args must skip the shutdowntime check entirely."""
        kwargs = self._common_kwargs(args=None)
        kwargs['jobnumber'] = 1
        proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)
        mock_remaining.assert_not_called()


if __name__ == '__main__':
    unittest.main()
