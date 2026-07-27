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

"""Unit tests for the remaining-time check in proceed_with_getjob().

The pilot refuses to fetch a new job when too little time remains to run it. The limit can
come from the proxy lifetime, the site time limit (PQ.maxtime) or the MachineFeatures
shutdowntime, whichever is most constraining.

Covers:
- _time_until_shutdown(): missing/empty MachineFeatures, missing/unparsable shutdowntime,
  stale (pre-pilot-start) shutdowntime, and the normal case.
- proceed_with_getjob(): refusal when the remaining time is below MIN_TIME_FOR_NEW_JOB,
  acceptance when enough time remains or when no source is available, the combined
  MIN_TIME_FOR_NEW_JOB threshold across several sources, and backward compatibility when
  args is not supplied.
- The error code reported on refusal: set only when the pilot ends without having run any
  job (jobnumber == 0), and chosen according to the binding source.
"""

import logging
import os
import sys
import time
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.control import job as job_module

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()


def _make_args(pilot_start_time: float) -> SimpleNamespace:
    """Build a minimal args-like object sufficient for get_time_since_start().

    Args:
        pilot_start_time: epoch seconds to record as PILOT_START_TIME.

    Returns:
        SimpleNamespace: object with a `.timing` dict shaped like the real
            pilot args.timing structure.
    """
    return SimpleNamespace(timing={'0': {'PILOT_START_TIME': pilot_start_time}})


@contextmanager
def remaining_time_sources(proxy_validity_end: int = 0, queuedata: object = None,
                           shutdown: object = None):
    """Control all three remaining-time sources for the duration of a test.

    Anything not passed is left unavailable, which is the common real-world case: most
    queues have no MachineFeatures installed, PQ.maxtime is frequently unset, and queues
    using OIDC tokens rather than VOMS proxies never cache a proxy validity.

    Args:
        proxy_validity_end: absolute epoch time (s) at which the proxy expires, or 0 for
            "no proxy information cached".
        queuedata: object exposing a `maxtime` attribute, or None to simulate infosys not
            having been initialised.
        shutdown: value for _time_until_shutdown() -- seconds until shutdown, or None for
            "no MachineFeatures shutdowntime available".

    Yields:
        None: the patches are active inside the with-block.
    """
    with patch.object(job_module.pilot_cache, 'proxy_validity_end', proxy_validity_end), \
            patch.object(job_module.infosys, 'queuedata', queuedata), \
            patch('pilot.control.job._time_until_shutdown', return_value=shutdown):
        yield


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
    proceed_with_getjob() therefore behaves exactly as it did before the
    remaining-time check was introduced.
    """

    def setUp(self):
        """Save and clear MACHINEFEATURES so each test starts from a known state."""
        self._saved_env = os.environ.pop('MACHINEFEATURES', None)
        self._saved_wrap_up = os.environ.pop('PILOT_WRAP_UP', None)

    def tearDown(self):
        """Restore the original environment, if any."""
        os.environ.pop('PILOT_WRAP_UP', None)
        if self._saved_env is not None:
            os.environ['MACHINEFEATURES'] = self._saved_env
        if self._saved_wrap_up is not None:
            os.environ['PILOT_WRAP_UP'] = self._saved_wrap_up

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
    def test_proceed_with_getjob_unaffected_when_no_source_available(self, mock_space):
        """proceed_with_getjob() must accept jobs when no remaining-time source exists.

        With no MachineFeatures, no cached proxy validity and no queuedata, the pilot must
        fall back on the pre-existing timefloor/proxy/disk-space checks alone. The queuedata
        being None also exercises the guard against infosys not having been initialised --
        without it this raises AttributeError, which the caller of proceed_with_getjob()
        silently turns into "stop fetching jobs".
        """
        args = _make_args(time.time() - 3600)
        with remaining_time_sources(proxy_validity_end=0, queuedata=None, shutdown=None):
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


class TestProceedWithGetjobRemainingTime(unittest.TestCase):
    """Tests for the remaining-time gate inside proceed_with_getjob().

    These tests stub out the unrelated checks in proceed_with_getjob() (proxy
    verification is skipped via verify_proxy=False, and local disk space via
    patching check_local_space) so that only the remaining-time logic under test
    determines the outcome.
    """

    def setUp(self):
        """Start each test without a leaked PILOT_WRAP_UP from an earlier refusal."""
        self._saved_wrap_up = os.environ.pop('PILOT_WRAP_UP', None)

    def tearDown(self):
        """Restore PILOT_WRAP_UP so refusals do not leak into other tests."""
        os.environ.pop('PILOT_WRAP_UP', None)
        if self._saved_wrap_up is not None:
            os.environ['PILOT_WRAP_UP'] = self._saved_wrap_up

    def _common_kwargs(self, args, traces=None):
        """Build the common kwargs shared across proceed_with_getjob() calls in this test class.

        Args:
            args: the args-like object to pass through to proceed_with_getjob().
            traces: optional traces object; a fresh one with no error code is used by default.

        Returns:
            dict: kwargs for proceed_with_getjob(), with proxy verification disabled
                and a generous timefloor so only the remaining-time check is exercised.
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
            'traces': traces if traces is not None else MagicMock(pilot={'error_code': 0}),
            'args': args,
        }

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_refuses_job_when_shutdown_imminent(self, mock_space):
        """proceed_with_getjob() must refuse a new job when shutdown is too close.

        Regression test for the bug where the pilot accepted job 7193562359 with only
        ~1107s left before MachineFeatures shutdowntime, leading to an avoidable
        REACHED_MAXTIME abort 564s later.
        """
        args = _make_args(time.time() - 6 * 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1  # not the first job
        with remaining_time_sources(shutdown=500):
            proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertFalse(proceed)

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_accepts_job_when_shutdown_not_imminent(self, mock_space):
        """proceed_with_getjob() must proceed normally when ample time remains."""
        args = _make_args(time.time() - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1
        with remaining_time_sources(shutdown=7200):
            proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_accepts_job_when_no_source_available(self, mock_space):
        """proceed_with_getjob() must proceed when no source of remaining time exists."""
        args = _make_args(time.time() - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1
        with remaining_time_sources():
            proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_refuses_first_job_when_time_already_gone(self, mock_space):
        """The gate now applies to the first job too (jobnumber == 0).

        Previously the first job was exempt, on the grounds that it is the batch system's
        or wrapper's responsibility. An already-passed shutdowntime is just as wasteful to
        request against on the first job as on the fifth.
        """
        args = _make_args(time.time() - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 0
        with remaining_time_sources(shutdown=10):
            proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertFalse(proceed)

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_no_args_skips_remaining_time_check(self, mock_space):
        """Backward compatibility: omitting args must skip the remaining-time check entirely."""
        kwargs = self._common_kwargs(args=None)
        kwargs['jobnumber'] = 1
        with patch('pilot.control.job._compute_remaining_time') as mock_compute:
            proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)
        mock_compute.assert_not_called()

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_threshold_applies_to_combined_value_not_single_source(self, mock_space):
        """MIN_TIME_FOR_NEW_JOB must be compared against the most constraining source.

        Each source on its own leaves plenty of time; only the shutdowntime is below the
        threshold, and that is enough to refuse.
        """
        now = int(time.time())
        args = _make_args(now - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1
        with remaining_time_sources(proxy_validity_end=now + 72 * 3600,
                                    queuedata=SimpleNamespace(maxtime=90000),
                                    shutdown=600):
            proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertFalse(proceed)

    @patch('pilot.control.job.check_local_space', return_value=(0, ''))
    def test_accepts_when_every_source_is_above_threshold(self, mock_space):
        """All three sources available and all comfortably above the threshold."""
        now = int(time.time())
        args = _make_args(now - 3600)
        kwargs = self._common_kwargs(args)
        kwargs['jobnumber'] = 1
        with remaining_time_sources(proxy_validity_end=now + 72 * 3600,
                                    queuedata=SimpleNamespace(maxtime=90000),
                                    shutdown=7200):
            proceed = job_module.proceed_with_getjob(**kwargs)
        self.assertTrue(proceed)


class TestProceedWithGetjobErrorCode(unittest.TestCase):
    """Tests for the error code reported when the remaining-time gate refuses a job.

    An error code is only set when the pilot ends without having run a single job. A
    multijob pilot that declines a further job has ended normally and must not report an
    error, matching the existing convention elsewhere in proceed_with_getjob() that having
    run out of time is not an error worth propagating to the wrapper and Harvester.
    """

    def setUp(self):
        """Start each test without a leaked PILOT_WRAP_UP from an earlier refusal."""
        self._saved_wrap_up = os.environ.pop('PILOT_WRAP_UP', None)

    def tearDown(self):
        """Restore PILOT_WRAP_UP so refusals do not leak into other tests."""
        os.environ.pop('PILOT_WRAP_UP', None)
        if self._saved_wrap_up is not None:
            os.environ['PILOT_WRAP_UP'] = self._saved_wrap_up

    def _refuse(self, jobnumber, initial_error_code=0, **sources):
        """Drive proceed_with_getjob() to a refusal and return the resulting traces.

        Args:
            jobnumber: number of jobs already downloaded.
            initial_error_code: error code already present in traces before the call.
            **sources: keyword arguments forwarded to remaining_time_sources().

        Returns:
            tuple: (proceed, traces) from the proceed_with_getjob() call.
        """
        traces = MagicMock(pilot={'error_code': initial_error_code})
        args = _make_args(time.time() - 3600)
        with remaining_time_sources(**sources):
            with patch('pilot.control.job.check_local_space', return_value=(0, '')):
                proceed = job_module.proceed_with_getjob(
                    timefloor=86400,
                    starttime=time.time(),
                    jobnumber=jobnumber,
                    getjob_requests=1,
                    max_getjob_requests=150,
                    should_update_server=True,
                    submitmode='PULL',
                    harvester=False,
                    verify_proxy=False,
                    traces=traces,
                    args=args,
                )
        return proceed, traces

    def test_error_code_set_when_no_job_was_ever_run(self):
        """jobnumber == 0: the pilot did nothing, so the wrapper must be told why."""
        proceed, traces = self._refuse(jobnumber=0, shutdown=10)
        self.assertFalse(proceed)
        self.assertEqual(traces.pilot['error_code'], errors.NOTIMELEFTFORNEWJOB)

    def test_no_error_code_when_a_job_has_already_run(self):
        """jobnumber > 0: a multijob pilot declining a further job has ended normally."""
        proceed, traces = self._refuse(jobnumber=3, shutdown=10)
        self.assertFalse(proceed)
        self.assertEqual(traces.pilot['error_code'], 0)

    def test_proxy_bound_refusal_reports_proxy_too_short(self):
        """A refusal bound by the proxy must report the dedicated proxy error code."""
        now = int(time.time())
        proceed, traces = self._refuse(
            jobnumber=0,
            proxy_validity_end=now + 60,  # proxy expires in a minute
            queuedata=SimpleNamespace(maxtime=90000),
            shutdown=7200,
        )
        self.assertFalse(proceed)
        self.assertEqual(traces.pilot['error_code'], errors.PROXYTOOSHORT)

    def test_maxtime_bound_refusal_reports_no_time_left(self):
        """A refusal bound by PQ.maxtime must report the general no-time-left code."""
        now = int(time.time())
        proceed, traces = self._refuse(
            jobnumber=0,
            proxy_validity_end=now + 72 * 3600,
            queuedata=SimpleNamespace(maxtime=3700),  # pilot started 3600s ago -> 100s left
            shutdown=7200,
        )
        self.assertFalse(proceed)
        self.assertEqual(traces.pilot['error_code'], errors.NOTIMELEFTFORNEWJOB)

    def test_existing_error_code_is_not_overwritten(self):
        """An error code set earlier in the pilot must survive the refusal."""
        proceed, traces = self._refuse(
            jobnumber=0, initial_error_code=errors.NOLOCALSPACE, shutdown=10
        )
        self.assertFalse(proceed)
        self.assertEqual(traces.pilot['error_code'], errors.NOLOCALSPACE)

    def test_no_error_code_when_the_job_is_accepted(self):
        """No error code may be set when the gate lets the job through."""
        proceed, traces = self._refuse(jobnumber=0, shutdown=7200)
        self.assertTrue(proceed)
        self.assertEqual(traces.pilot['error_code'], 0)


if __name__ == '__main__':
    unittest.main()
