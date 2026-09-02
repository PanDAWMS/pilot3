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

"""Unit tests for the mean_core_count heartbeat field.

Background: when sphenix, epic and rubin were switched from the ps-based
core count snapshot to ATLAS's prmon (stime+utime)/walltime estimate, the value
appended to job.corecounts became the *formatted* string produced by
float_to_rounded_string() (e.g. '0.17') rather than a number. job.corecounts is
consumed by control/job.py::get_data_structure(), which averages it with
pilot.util.math.mean() to produce the 'mean_core_count' heartbeat field.
mean() computes sum(data) / len(data), so a list of strings raised

    TypeError: unsupported operand type(s) for +: 'int' and 'str'

That exception is caught in job_monitor() around send_heartbeat_if_time(). The
handler breaks out of the per-job 'for' loop only, not the outer 'while cont'
loop, so job_monitor_tasks() (memory checks, looping check, disk checks) keeps
running. What is lost is the heartbeat itself: update_time is never advanced, so
every subsequent monitoring cycle re-attempts the heartbeat and re-raises, and no
update is ever delivered to the server again for that job. A warning is logged
each cycle, but it carries only the exception text ("unsupported operand type(s)
for +: 'int' and 'str'") and not the exception class name, so grepping a pilotlog
for "TypeError" will not find it.

Only sphenix/epic/rubin were affected: ATLAS never calls add_core_count(), and
ska/darkside/generic still record ints from the ps-based measurement.

Two layers are covered here:
  1. The three prmon-based plugins append a numeric value to job.corecounts.
  2. get_mean_core_count() coerces entries to float and skips unusable ones, so
     no single malformed measurement can ever reach the heartbeat path as an
     exception.
"""

import logging
import sys
import unittest
from unittest.mock import patch

from pilot.control.job import get_mean_core_count
from pilot.info.jobdata import JobData
from pilot.util.math import mean

logger = logging.getLogger(__name__)

PRMON_PLUGINS = ('sphenix', 'epic', 'rubin')


def _make_job(corecount: int = 1) -> JobData:
    """Return a minimal JobData object with per-instance corecounts list."""
    job = JobData({}, use_kmap=False)
    job.corecount = corecount
    job.workdir = '/tmp'
    job.memorymonitor = 'prmon'

    return job


class TestGetMeanCoreCount(unittest.TestCase):
    """Tests for control/job.py::get_mean_core_count()."""

    def test_integers(self):
        """Ints (as recorded by the ps-based plugins) are averaged and truncated."""
        self.assertEqual(get_mean_core_count([1, 2, 6]), 3)

    def test_floats(self):
        """Floats (as recorded by the prmon-based plugins) are averaged and truncated."""
        self.assertEqual(get_mean_core_count([1.0, 9.5]), 5)

    def test_numeric_strings_do_not_raise(self):
        """Legacy string measurements must be coerced, not propagated as a TypeError."""
        self.assertEqual(get_mean_core_count(['1.00', '9.50']), 5)

    def test_mean_would_have_raised_on_strings(self):
        """Pin the underlying failure mode this guard exists to absorb."""
        with self.assertRaises(TypeError):
            mean(['1.00', '9.50'])

    def test_unusable_entries_are_skipped(self):
        """Non-numeric entries are dropped; the remaining measurements still report."""
        self.assertEqual(get_mean_core_count(['0.17', 2, None, 'not-a-number', 4.0]), 2)

    def test_all_entries_unusable_returns_none(self):
        """With nothing usable, no mean_core_count should be reported at all."""
        self.assertIsNone(get_mean_core_count([None, 'x']))

    def test_empty_list_returns_none(self):
        """An empty measurement list reports nothing rather than dividing by zero."""
        self.assertIsNone(get_mean_core_count([]))


class TestPrmonPluginsRecordNumericCoreCounts(unittest.TestCase):
    """The three prmon-based plugins must append numbers to job.corecounts."""

    def _run_plugin(self, vo: str, stime: int, utime: int, walltime: int) -> JobData:
        """Import the VO plugin and run set_core_counts() with a patched summary dictionary."""
        module = __import__(f'pilot.user.{vo}.cpu', globals(), locals(), ['cpu'], 0)
        job = _make_job(corecount=1)
        summary = {'Time': {'stime': stime, 'utime': utime}}
        with patch.object(module, 'get_memory_values', return_value=summary):
            module.set_core_counts(job=job, walltime=walltime)

        return job

    def test_corecounts_entries_are_numeric(self):
        """job.corecounts must not contain the rounded display string."""
        for vo in PRMON_PLUGINS:
            with self.subTest(vo=vo):
                job = self._run_plugin(vo, 45, 50, 10)
                self.assertEqual(job.actualcorecount, '9.50')
                self.assertEqual(job.corecounts, [9.5])
                for entry in job.corecounts:
                    self.assertNotIsInstance(entry, str)

    def test_heartbeat_mean_survives_plugin_measurements(self):
        """The full plugin -> get_mean_core_count() path must not raise."""
        for vo in PRMON_PLUGINS:
            with self.subTest(vo=vo):
                job = self._run_plugin(vo, 45, 50, 10)
                self.assertEqual(get_mean_core_count(job.corecounts), 9)

    def test_epic_gke_incident_numbers(self):
        """Reproduce the ePIC/GKE report (pandaid 1685720, 2026): ~0.17 cores, not 6.

        The pod was limited to 1 CPU and the job reported cpuefficiency=16.53%, i.e. the
        payload used ~0.17 cores on average. The prmon estimate must agree with that,
        and must not raise on the way to the heartbeat.
        """
        job = self._run_plugin('epic', 20, 80, 600)
        self.assertEqual(job.actualcorecount, '0.17')
        self.assertAlmostEqual(job.corecounts[0], 100.0 / 600.0)
        self.assertEqual(get_mean_core_count(job.corecounts), 0)


class TestPsBasedPluginsUnaffected(unittest.TestCase):
    """ska/darkside/generic still record ints - confirm the heartbeat path is fine."""

    def test_int_measurements_average_cleanly(self):
        """A ps-based history of ints averages without coercion warnings."""
        self.assertEqual(get_mean_core_count([6, 6, 6]), 6)


def suite():
    """Return the test suite (for the pilot's own test runner)."""
    return unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])


if __name__ == '__main__':
    unittest.main()
