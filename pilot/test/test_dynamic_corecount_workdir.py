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

"""Unit tests for pilot.util.monitoring.get_max_allowed_work_dir_size().

Covers a production incident on an mcore Rubin/LSSTCam queue (usdf) whose PQ
declares 'corecount': -1 in CRIC - a sentinel meaning "this PQ accepts jobs
with a dynamic number of cores" - combined with job.corecount transiently
reading 1 (see pilot.user.rubin.cpu.set_core_counts(), which - unlike ATLAS's
implementation - overwrites job.corecount with a single ps-based
instantaneous core measurement). The pre-fix code only guarded against
pq_corecount being None/0 (`if not pq_corecount`), so a -1 sailed through as
a divisor:

    maxwdirsize // -1 -> negative
    negative * grace_margin -> still negative

...producing a CRITICAL 'work directory too large' failure where the
reported 'max limit' was a large negative number (e.g. -20,555,445,043 B)
that any non-negative workdirsize (e.g. 938,304 B) would always exceed.

These tests reproduce the exact figures from that incident and confirm the
fix (treating any non-positive pq_corecount as invalid, not just falsy)
without changing behaviour for the existing, valid use cases.
"""

import logging
import sys
import unittest
from unittest.mock import patch

from pilot.util.monitoring import get_max_allowed_work_dir_size

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# PQ.maxwdir = 16336 MB, as in the incident's pilot log and in the
# get_maximum_input_sizes() docstring example (16336 MB -> 17,129,537,536 B).
_MAXWDIR_MB = 16336
_GRACE = 1.2


class TestDynamicCorecountWorkDirSize(unittest.TestCase):
    """Tests for get_max_allowed_work_dir_size() with non-positive PQ.corecount values."""

    def setUp(self):
        """Patch get_maximum_input_sizes() to return a fixed, known PQ.maxwdir value."""
        patcher = patch('pilot.util.monitoring.get_maximum_input_sizes', return_value=_MAXWDIR_MB)
        self.addCleanup(patcher.stop)
        patcher.start()

    def test_incident_reproduction_dynamic_corecount_single_core_reading(self):
        """corecount=1 (transiently misreported) + PQ.corecount=-1 (dynamic) must not go negative."""
        # pre-fix this returned -20555445043 (reproducing the exact incident value)
        maxwdirsize = get_max_allowed_work_dir_size('MCORE', 1, -1, _GRACE)
        self.assertGreater(maxwdirsize, 0)
        # falls back to divider=1 -> same as the full, undivided mcore limit
        self.assertEqual(maxwdirsize, int(_MAXWDIR_MB * 1024 ** 2 * _GRACE))

    def test_dynamic_corecount_with_correct_mcore_reading(self):
        """corecount=10 (correctly reported mcore job) + PQ.corecount=-1 was already unaffected."""
        # divider only ever depends on pq_corecount when corecount == 1, so this path was never
        # broken - included here as a belt-and-suspenders regression guard
        maxwdirsize = get_max_allowed_work_dir_size('MCORE', 10, -1, _GRACE)
        self.assertGreater(maxwdirsize, 0)
        self.assertEqual(maxwdirsize, int(_MAXWDIR_MB * 1024 ** 2 * _GRACE))

    def test_pq_corecount_zero_still_handled(self):
        """PQ.corecount=0 (falsy) must still fall back to 1, as before this fix."""
        maxwdirsize = get_max_allowed_work_dir_size('SCORE', 1, 0, _GRACE)
        self.assertGreater(maxwdirsize, 0)
        self.assertEqual(maxwdirsize, int(_MAXWDIR_MB * 1024 ** 2 * _GRACE))

    def test_pq_corecount_none_still_handled(self):
        """PQ.corecount=None must still fall back to 1, as before this fix."""
        maxwdirsize = get_max_allowed_work_dir_size('SCORE', 1, None, _GRACE)
        self.assertGreater(maxwdirsize, 0)
        self.assertEqual(maxwdirsize, int(_MAXWDIR_MB * 1024 ** 2 * _GRACE))

    def test_valid_positive_pq_corecount_still_divides_normally(self):
        """A genuine single-core job on a fixed-corecount PQ must still divide by PQ.corecount."""
        maxwdirsize = get_max_allowed_work_dir_size('SCORE', 1, 8, _GRACE)
        expected_full = int(_MAXWDIR_MB * 1024 ** 2 * _GRACE)
        expected_divided = int((_MAXWDIR_MB * 1024 ** 2 // 8) * _GRACE)
        self.assertGreater(maxwdirsize, 0)
        self.assertEqual(maxwdirsize, expected_divided)
        self.assertLess(maxwdirsize, expected_full)

    def test_other_negative_pq_corecount_values_also_handled(self):
        """Any non-positive PQ.corecount (not just -1) must fall back to divider=1."""
        for bad_value in (-1, -2, -100):
            with self.subTest(pq_corecount=bad_value):
                maxwdirsize = get_max_allowed_work_dir_size('MCORE', 1, bad_value, _GRACE)
                self.assertGreater(maxwdirsize, 0)
                self.assertEqual(maxwdirsize, int(_MAXWDIR_MB * 1024 ** 2 * _GRACE))

    def test_log_message_for_missing_pq_corecount_does_not_call_it_negative(self):
        """The None/0 fallback log message must describe a missing value, not a negative one."""
        with self.assertLogs('pilot.util.monitoring', level='WARNING') as cm:
            get_max_allowed_work_dir_size('SCORE', 1, None, _GRACE)
        self.assertTrue(any('not set' in msg for msg in cm.output))
        self.assertFalse(any('negative' in msg for msg in cm.output))

    def test_log_message_for_negative_pq_corecount_does_not_call_it_invalid(self):
        """The negative fallback log message must name the dynamic-corecount PQ meaning, not just 'invalid'."""
        with self.assertLogs('pilot.util.monitoring', level='WARNING') as cm:
            get_max_allowed_work_dir_size('MCORE', 1, -1, _GRACE)
        self.assertTrue(any('negative' in msg and 'dynamic' in msg for msg in cm.output))
        self.assertFalse(any('invalid' in msg for msg in cm.output))


if __name__ == '__main__':
    unittest.main()
