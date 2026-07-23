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

"""Regression tests for a missing return statement in atlas.memory.memory_usage().

Bug summary: memory_usage() only had an explicit `return exit_code, diagnostics`
for the early "monitor output could not be read" path. The main `if/elif/elif`
block that handles the within-limit, over-limit, limit-undetermined and
maxPSS-not-found cases logged a message in every branch but never returned
anything, so Python implicitly returned None in all of those cases.

The sole caller, pilot.util.monitoring.verify_memory_usage(), does::

    exit_code, _ = memory.memory_usage(job, resource_type)

Unpacking None raises "TypeError: cannot unpack non-iterable NoneType object".
That TypeError is caught by a broad except clause and logged as:

    WARNING | verify_memory_usage | caught exception: cannot unpack non-iterable NoneType object
    WARNING | verify_memory_usage | ignoring failure to parse memory monitor output

on every single monitoring loop iteration -- observed live even when the
payload's memory usage was well within its allowed limit (maxPSS 21662 kB
<= limit 4096000 kB), which should never be treated as a parsing failure.

These tests confirm memory_usage() always returns a proper (int, str) tuple,
and that verify_memory_usage() no longer logs the spurious exception.
"""

import logging
import os
import sys
import time
import unittest
from unittest.mock import patch

from pilot.common.errorcodes import ErrorCodes
from pilot.user.atlas import memory as atlas_memory
from pilot.util.monitoringtime import MonitoringTime

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()

# The exact summary_dictionary shape observed in the live pilot log that
# triggered the bug (maxPSS well within the computed limit).
WITHIN_LIMIT_SUMMARY = {
    'Max': {'maxVMEM': 1764220, 'maxPSS': 21662, 'maxRSS': 50664, 'maxSwap': 0},
    'Avg': {'avgVMEM': 1764220, 'avgPSS': 21662, 'avgRSS': 50664, 'avgSwap': 0},
    'Other': {'rchar': 84483769, 'wchar': 4545749, 'read_bytes': 65536, 'write_bytes': 397312, 'nprocs': 9},
    'Time': {'stime': 2, 'utime': 1},
}


class FakeJob:
    """Minimal job-like object sufficient for atlas.memory.memory_usage()."""

    def __init__(self):
        """Initialize a fake job with the attributes memory_usage() touches."""
        self.workdir = '/tmp/does-not-need-to-exist'
        self.memorymonitor = 'prmon'
        self.corecount = 1
        self.pid = None
        self.state = 'running'
        self.piloterrorcodes = []
        self.piloterrordiags = []


class TestMemoryUsageAlwaysReturnsTuple(unittest.TestCase):
    """Regression tests: memory_usage() must return (int, str) on every branch."""

    def setUp(self):
        """Disable cgroup enforcement so tests don't touch the real filesystem."""
        self._orig_use_cgroups = atlas_memory.pilot_cache.use_cgroups
        atlas_memory.pilot_cache.use_cgroups = False

    def tearDown(self):
        """Restore the shared pilot_cache singleton's use_cgroups flag."""
        atlas_memory.pilot_cache.use_cgroups = self._orig_use_cgroups

    @patch('pilot.user.atlas.memory.calculate_memory_limit_kb', return_value=4096000)
    @patch('pilot.user.atlas.memory.get_memory_limit', return_value=2000)
    @patch('pilot.user.atlas.memory.get_memory_values', return_value=WITHIN_LIMIT_SUMMARY)
    def test_within_limit_returns_tuple(self, _mock_values, _mock_limit, _mock_calc):
        """Test the exact live scenario from the bug report: usage within limit."""
        result = atlas_memory.memory_usage(FakeJob(), 'SCORE')
        self.assertIsNotNone(result, "memory_usage() returned None instead of a tuple")
        self.assertIsInstance(result, tuple)
        exit_code, diagnostics = result  # must not raise
        self.assertEqual(exit_code, 0)
        self.assertEqual(diagnostics, "")

    @patch('pilot.user.atlas.memory.kill_processes')
    @patch('pilot.user.atlas.memory.set_pilot_state')
    @patch('pilot.user.atlas.memory.calculate_memory_limit_kb', return_value=1000)
    @patch('pilot.user.atlas.memory.get_memory_limit', return_value=2000)
    @patch('pilot.user.atlas.memory.get_memory_values', return_value=WITHIN_LIMIT_SUMMARY)
    def test_exceeds_limit_returns_tuple_and_kills(
        self, _mock_values, _mock_limit, _mock_calc, mock_set_state, mock_kill
    ):
        """Test that exceeding the memory limit still returns a tuple and kills the payload."""
        job = FakeJob()
        exit_code, diagnostics = atlas_memory.memory_usage(job, 'SCORE')
        self.assertEqual(exit_code, 0)
        self.assertIn("exceeded the memory limit", diagnostics)
        mock_set_state.assert_called_once_with(job=job, state="failed")
        mock_kill.assert_called_once_with(job.pid)
        self.assertTrue(job.piloterrorcodes, "expected PAYLOADEXCEEDMAXMEM to be recorded")

    @patch('pilot.user.atlas.memory.calculate_memory_limit_kb', return_value=None)
    @patch('pilot.user.atlas.memory.get_memory_limit', return_value=2000)
    @patch('pilot.user.atlas.memory.get_memory_values', return_value=WITHIN_LIMIT_SUMMARY)
    def test_limit_undetermined_returns_tuple(self, _mock_values, _mock_limit, _mock_calc):
        """Test that an undetermined memory limit still returns a tuple, not None."""
        result = atlas_memory.memory_usage(FakeJob(), 'SCORE')
        self.assertIsInstance(result, tuple)
        exit_code, _ = result
        self.assertEqual(exit_code, 0)

    @patch('pilot.user.atlas.memory.calculate_memory_limit_kb', return_value=4096000)
    @patch('pilot.user.atlas.memory.get_memory_limit', return_value=2000)
    @patch('pilot.user.atlas.memory.get_memory_values',
           return_value={'Max': {}, 'Avg': {}, 'Other': {}, 'Time': {}})
    def test_maxpss_not_found_returns_tuple(self, _mock_values, _mock_limit, _mock_calc):
        """Test that a summary missing 'maxPSS' still returns a tuple, not None."""
        result = atlas_memory.memory_usage(FakeJob(), 'SCORE')
        self.assertIsInstance(result, tuple)
        exit_code, _ = result
        self.assertEqual(exit_code, 0)

    @patch('pilot.user.atlas.memory.get_memory_values', return_value=None)
    def test_unreadable_monitor_output_returns_error_tuple(self, _mock_values):
        """Test the pre-existing early-return path still works unchanged."""
        exit_code, diagnostics = atlas_memory.memory_usage(FakeJob(), 'SCORE')
        self.assertEqual(exit_code, errors.BADMEMORYMONITORJSON)
        self.assertEqual(diagnostics, "memory monitor output could not be read")


class TestVerifyMemoryUsageIntegration(unittest.TestCase):
    """End-to-end regression test through pilot.util.monitoring.verify_memory_usage()."""

    def setUp(self):
        """Force the atlas plugin and disable cgroup enforcement."""
        self._orig_pilot_user = os.environ.get('PILOT_USER')
        os.environ['PILOT_USER'] = 'atlas'
        self._orig_use_cgroups = atlas_memory.pilot_cache.use_cgroups
        atlas_memory.pilot_cache.use_cgroups = False

    def tearDown(self):
        """Restore PILOT_USER and the pilot_cache singleton's use_cgroups flag."""
        if self._orig_pilot_user is None:
            os.environ.pop('PILOT_USER', None)
        else:
            os.environ['PILOT_USER'] = self._orig_pilot_user
        atlas_memory.pilot_cache.use_cgroups = self._orig_use_cgroups

    @patch('pilot.user.atlas.memory.calculate_memory_limit_kb', return_value=4096000)
    @patch('pilot.user.atlas.memory.get_memory_limit', return_value=2000)
    @patch('pilot.user.atlas.memory.get_memory_values', return_value=WITHIN_LIMIT_SUMMARY)
    def test_no_spurious_exception_logged(self, _mock_values, _mock_limit, _mock_calc):
        """Test that verify_memory_usage() no longer logs the unpacking TypeError.

        Reproduces the reported log sequence end to end: a healthy,
        within-limit job should never produce a "caught exception" or
        "ignoring failure to parse memory monitor output" warning.
        """
        # local import: must happen after PILOT_USER=atlas is set (in setUp())
        # so pilot.util.monitoring resolves the atlas plugin, not a module-load-time default
        from pilot.util import monitoring  # pylint: disable=import-outside-toplevel

        job = FakeJob()
        mt = MonitoringTime()
        original_ct_memory = int(time.time()) - 61
        mt.ct_memory = original_ct_memory  # force the verification window to be due

        captured = []

        class _CaptureHandler(logging.Handler):
            def emit(self, record):
                captured.append(record.getMessage())

        monitoring_logger = logging.getLogger('pilot.util.monitoring')
        handler = _CaptureHandler()
        monitoring_logger.addHandler(handler)
        try:
            monitoring.verify_memory_usage(int(time.time()), mt, job, 'SCORE')
        finally:
            monitoring_logger.removeHandler(handler)

        for message in captured:
            self.assertNotIn('caught exception', message, f"unexpected warning logged: {message}")
            self.assertNotIn('ignoring failure to parse', message, f"unexpected warning logged: {message}")

        # ct_memory should have been updated, confirming memory_usage() returned
        # exit_code == 0 and the success path (not the exception path) was taken.
        self.assertGreater(mt.get('ct_memory'), original_ct_memory)


if __name__ == '__main__':
    unittest.main()
