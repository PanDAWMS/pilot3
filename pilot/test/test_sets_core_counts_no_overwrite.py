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

"""Unit tests for set_core_counts() across the non-ATLAS experiment plugins.

Background: JobData documents a clear split between two attributes -
'corecount' ("number of cores as requested by the task") and 'actualcorecount'
("number of cores actually used by the payload"). ATLAS's own cpu.py never
touches job.corecount from set_core_counts() - only job.actualcorecount.

However, six sibling plugins (sphenix, ska, darkside, epic, generic, rubin)
previously did `job.corecount = job.actualcorecount`, permanently overwriting
the requested/allocated core count with whatever was measured at that instant.
This caused a production incident on an mcore Rubin/usdf queue: a transient
single-core reading early in the job's life (before the payload had spawned
its worker processes) got baked into job.corecount, which downstream code
(pilot.util.monitoring.get_max_allowed_work_dir_size()) then combined with the
PQ's CRIC-declared dynamic corecount (-1) to compute a negative work dir size
limit, killing a healthy job.

This module verifies, for all six plugins:
  1. job.corecount is never modified by set_core_counts().
  2. job.actualcorecount is updated as before, and job.corecounts now correctly
     accumulates a running history across calls (previously each call passed
     only the new reading to add_core_count(), discarding prior history and
     silently reducing job.py's reported 'mean_core_count' to just the latest
     single measurement - fixed here by passing the existing job.corecounts
     list through on every call).

For sphenix, epic, and rubin - which already have prmon support via their own
get_memory_values() - set_core_counts() was additionally switched from an
instantaneous `ps`-based process-group snapshot to ATLAS's cumulative,
time-averaged (stime+utime)/walltime estimate, which is not skewed by a
payload that has not yet ramped up to its full parallelism. Those three
plugins get extra coverage of that calculation.

ska, darkside, and generic have no prmon parsing implemented, so they keep
the ps-based measurement; only the job.corecount overwrite is removed there.
"""

import logging
import sys
import unittest
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import patch

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# The two mixins below are never run standalone - they only ever provide test
# methods to a concrete subclass that also inherits unittest.TestCase (see
# each Test*SetCoreCounts class). At runtime they inherit from plain `object`
# so they are not themselves collected and run as (broken) test cases, but
# under TYPE_CHECKING (as seen by pylint/mypy) they present as TestCase
# subclasses so static analysis can resolve self.assertEqual() etc. and does
# not flag every assertion as a false-positive no-member error.
if TYPE_CHECKING:
    _CoreCountTestsBase = unittest.TestCase
else:
    _CoreCountTestsBase = object


def _make_job(corecount=10, actualcorecount=0, pgrp=4242, workdir='/tmp/wd', memorymonitor='prmon'):
    """Build a minimal job-like stand-in with only the attributes set_core_counts() touches."""
    return SimpleNamespace(
        corecount=corecount,
        actualcorecount=actualcorecount,
        corecounts=[],
        pgrp=pgrp,
        workdir=workdir,
        memorymonitor=memorymonitor,
    )


class _PsBasedPluginTestsMixin(_CoreCountTestsBase):
    """Shared tests for the three plugins that still use a ps-based process-group snapshot."""

    module = None  # set by subclasses, e.g. pilot.user.ska.cpu

    def test_corecount_not_overwritten_when_actual_reading_is_lower(self):
        """A 10-core job transiently measured as 1 active core must keep corecount == 10."""
        job = _make_job(corecount=10)
        with patch.object(self.module, 'execute', return_value=(0, '1', '')):
            self.module.set_core_counts(job=job, walltime=30)

        self.assertEqual(job.corecount, 10, 'job.corecount (requested/allocated) must be untouched')
        self.assertEqual(job.actualcorecount, 1)
        self.assertEqual(job.corecounts, [1])

    def test_actualcorecount_and_corecounts_still_tracked(self):
        """actualcorecount/corecounts must accumulate across repeated measurements."""
        job = _make_job(corecount=10)
        with patch.object(self.module, 'execute', return_value=(0, '3', '')):
            self.module.set_core_counts(job=job, walltime=30)
        with patch.object(self.module, 'execute', return_value=(0, '10', '')):
            self.module.set_core_counts(job=job, walltime=60)

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.actualcorecount, 10)
        self.assertEqual(job.corecounts, [3, 10])

    def test_no_pgrp_leaves_corecount_and_actualcorecount_untouched(self):
        """No process group set - nothing should be measured or overwritten."""
        job = _make_job(corecount=10, actualcorecount=0, pgrp=None)
        with patch.object(self.module, 'execute') as mock_execute:
            self.module.set_core_counts(job=job, walltime=30)
            mock_execute.assert_not_called()

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.actualcorecount, 0)
        self.assertEqual(job.corecounts, [])

    def test_unparsable_ps_output_leaves_corecount_untouched(self):
        """A non-numeric ps result must not crash and must not touch job.corecount."""
        job = _make_job(corecount=10)
        with patch.object(self.module, 'execute', return_value=(0, 'not-a-number', '')):
            self.module.set_core_counts(job=job, walltime=30)

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.corecounts, [])


class TestSkaSetCoreCounts(_PsBasedPluginTestsMixin, unittest.TestCase):
    """set_core_counts() tests for the ska plugin."""

    @classmethod
    def setUpClass(cls):
        """Import the plugin module once for the whole test class."""
        import pilot.user.ska.cpu as module  # pylint: disable=import-outside-toplevel
        cls.module = module


class TestDarksideSetCoreCounts(_PsBasedPluginTestsMixin, unittest.TestCase):
    """set_core_counts() tests for the darkside plugin."""

    @classmethod
    def setUpClass(cls):
        """Import the plugin module once for the whole test class."""
        import pilot.user.darkside.cpu as module  # pylint: disable=import-outside-toplevel
        cls.module = module


class TestGenericSetCoreCounts(_PsBasedPluginTestsMixin, unittest.TestCase):
    """set_core_counts() tests for the generic plugin."""

    @classmethod
    def setUpClass(cls):
        """Import the plugin module once for the whole test class."""
        import pilot.user.generic.cpu as module  # pylint: disable=import-outside-toplevel
        cls.module = module


class _PrmonBasedPluginTestsMixin(_CoreCountTestsBase):
    """Shared tests for the three plugins now using ATLAS's prmon CPU-time estimate."""

    module = None  # set by subclasses, e.g. pilot.user.rubin.cpu

    def _summary(self, stime, utime):
        return {'Time': {'stime': stime, 'utime': utime}}

    def test_corecount_not_overwritten_when_actual_usage_is_low(self):
        """A 10-core job with low cumulative CPU usage so far must keep corecount == 10."""
        job = _make_job(corecount=10)
        # stime+utime == walltime -> ~1 core used on average so far
        with patch.object(self.module, 'get_memory_values', return_value=self._summary(5, 5)):
            self.module.set_core_counts(job=job, walltime=10)

        self.assertEqual(job.corecount, 10, 'job.corecount (requested/allocated) must be untouched')
        self.assertEqual(job.actualcorecount, '1.00')
        self.assertEqual(job.corecounts, ['1.00'])

    def test_actualcorecount_reflects_multicore_usage(self):
        """Cumulative CPU time close to corecount*walltime should read as close to full parallelism."""
        job = _make_job(corecount=10)
        # stime+utime == 95 over walltime=10 -> 9.5 cores
        with patch.object(self.module, 'get_memory_values', return_value=self._summary(45, 50)):
            self.module.set_core_counts(job=job, walltime=10)

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.actualcorecount, '9.50')
        self.assertEqual(job.corecounts, ['9.50'])

    def test_corecounts_accumulates_across_calls(self):
        """job.corecounts must grow across repeated measurements, not reset each call."""
        job = _make_job(corecount=10)
        with patch.object(self.module, 'get_memory_values', return_value=self._summary(5, 5)):
            self.module.set_core_counts(job=job, walltime=10)
        with patch.object(self.module, 'get_memory_values', return_value=self._summary(45, 50)):
            self.module.set_core_counts(job=job, walltime=10)

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.corecounts, ['1.00', '9.50'])

    def test_missing_time_dictionary_leaves_state_untouched(self):
        """No 'Time' entry in the summary dictionary - nothing should be recorded."""
        job = _make_job(corecount=10)
        with patch.object(self.module, 'get_memory_values', return_value={'Max': {}}):
            self.module.set_core_counts(job=job, walltime=10)

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.actualcorecount, 0)
        self.assertEqual(job.corecounts, [])

    def test_memory_monitor_parse_failure_leaves_state_untouched(self):
        """get_memory_values() raising ValueError must not crash or touch job.corecount."""
        job = _make_job(corecount=10)
        with patch.object(self.module, 'get_memory_values', side_effect=ValueError('bad prmon output')):
            self.module.set_core_counts(job=job, walltime=10)

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.actualcorecount, 0)
        self.assertEqual(job.corecounts, [])

    def test_no_walltime_leaves_state_untouched(self):
        """Missing walltime - nothing should be measured."""
        job = _make_job(corecount=10)
        with patch.object(self.module, 'get_memory_values') as mock_get_memory_values:
            self.module.set_core_counts(job=job, walltime=None)
            mock_get_memory_values.assert_not_called()

        self.assertEqual(job.corecount, 10)
        self.assertEqual(job.actualcorecount, 0)


class TestSphenixSetCoreCounts(_PrmonBasedPluginTestsMixin, unittest.TestCase):
    """set_core_counts() tests for the sphenix plugin."""

    @classmethod
    def setUpClass(cls):
        """Import the plugin module once for the whole test class."""
        import pilot.user.sphenix.cpu as module  # pylint: disable=import-outside-toplevel
        cls.module = module


class TestEpicSetCoreCounts(_PrmonBasedPluginTestsMixin, unittest.TestCase):
    """set_core_counts() tests for the epic plugin."""

    @classmethod
    def setUpClass(cls):
        """Import the plugin module once for the whole test class."""
        import pilot.user.epic.cpu as module  # pylint: disable=import-outside-toplevel
        cls.module = module


class TestRubinSetCoreCounts(_PrmonBasedPluginTestsMixin, unittest.TestCase):
    """set_core_counts() tests for the rubin plugin (the plugin involved in the reported incident)."""

    @classmethod
    def setUpClass(cls):
        """Import the plugin module once for the whole test class."""
        import pilot.user.rubin.cpu as module  # pylint: disable=import-outside-toplevel
        cls.module = module


if __name__ == '__main__':
    unittest.main()
