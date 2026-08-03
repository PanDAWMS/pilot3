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

"""Unit tests for cgroup v2 memory.events based OOM-kill detection.

Production symptom that motivated this code: payloads were being SIGKILLed with no
memory-specific pilot error code, because the pilot relied on grepping dmesg for the
payload PIDs and the OOM kill message was not present there. The cgroup memory.events
counters are always present on a cgroups v2 node and, crucially, survive the death of
every process in the cgroup, which makes them a reliable post-mortem source.

Covers:
- read_memory_events(): a real-world counter dump, malformed content, missing file.
- monitor_cgroup(): the counters are still returned when the cgroup holds no processes
  (regression: the previous implementation early-returned zeros in exactly the state
  left behind by an OOM kill).
- check_for_cgroup_oom_kill(): non-zero exit code sets the error, zero exit code only
  warns, and the counters are compared against the payload-start baseline so a kill from
  an earlier job of a multi-job pilot is not attributed to the current payload.
- set_error_from_cgroup_oom_kill(): PAYLOADOUTOFMEMORY reported first with PAYLOADOOMKILL
  as a secondary code, and prmon's PAYLOADEXCEEDMAXMEM left untouched when already set.
- _get_current_job(): reads the monitored_payloads queue (regression: it previously
  looked for a non-existent running_jobs queue and therefore always returned None).
"""

import logging
import os
import queue
import sys
import tempfile
import unittest
from types import SimpleNamespace

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.control import payload as payload_module
from pilot.control.monitor import _get_current_job
from pilot.util.cgroups import (
    check_for_cgroup_oom_kill,
    get_oom_deltas,
    monitor_cgroup,
    read_memory_events,
    store_oom_baseline
)

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()
pilot_cache = get_pilot_cache()

# The counter dump reported in the ticket: the limit was reached 5605 times, but no
# process was ever killed.
NO_KILL_EVENTS = """low 0
high 0
max 5605
oom 0
oom_kill 0
oom_group_kill 0
"""

# The same cgroup after the kernel OOM killer fired with memory.oom.group enabled:
# one group kill, and one process killed as part of it.
KILL_EVENTS = """low 0
high 0
max 5606
oom 1
oom_kill 1
oom_group_kill 1
"""


class TestCgroupOomKill(unittest.TestCase):
    """Tests for the memory.events OOM detection path."""

    def setUp(self):
        """Create a fake cgroup directory and reset all shared singleton state."""
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.cgroup_path = self.tmpdir.name

        # the error code lists are class-level on ErrorCodes and leak between tests
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []

        # the pilot cache is a process-wide singleton
        self._saved_cgroups = dict(pilot_cache.cgroups)
        self._saved_baselines = dict(pilot_cache.oom_baselines)
        pilot_cache.cgroups = {}
        pilot_cache.oom_baselines = {}

    def tearDown(self):
        """Restore the singleton state and remove the fake cgroup directory."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []
        pilot_cache.cgroups = self._saved_cgroups
        pilot_cache.oom_baselines = self._saved_baselines
        self.tmpdir.cleanup()

    def _write(self, filename: str, content: str):
        """Write a fake cgroup interface file.

        Args:
            filename: name of the file inside the fake cgroup directory.
            content: file content.
        """
        with open(os.path.join(self.cgroup_path, filename), "w", encoding="utf-8") as fh:
            fh.write(content)

    @staticmethod
    def _make_job():
        """Return a minimal job-like object with the fields the code under test touches."""
        return SimpleNamespace(jobid="1234567890", piloterrorcodes=[], piloterrordiags=[])

    # ---------------------------------------------------------------- parsing

    def test_read_memory_events_parses_all_counters(self):
        """All memory.events counters are returned as integers."""
        self._write("memory.events", NO_KILL_EVENTS)
        events = read_memory_events(self.cgroup_path)

        self.assertEqual(events["max"], 5605)
        self.assertEqual(events["oom_kill"], 0)
        self.assertEqual(events["oom_group_kill"], 0)
        self.assertIsInstance(events["max"], int)

    def test_read_memory_events_missing_file(self):
        """A missing memory.events file (cgroups v1 or cgroups unused) returns an empty dict."""
        self.assertEqual(read_memory_events(self.cgroup_path), {})

    def test_read_memory_events_malformed_content(self):
        """Malformed lines are skipped without raising."""
        self._write("memory.events", "low 0\noom_kill abc\n\n   \nmax 7\ngarbage\n")
        events = read_memory_events(self.cgroup_path)

        self.assertEqual(events, {"low": 0, "max": 7})

    def test_monitor_cgroup_reports_counters_for_empty_cgroup(self):
        """The counters are returned even when the cgroup holds no processes.

        Regression test: an OOM kill (especially with memory.oom.group enabled) leaves
        the cgroup empty, and the previous implementation returned zeros in that case
        without ever reading memory.events.
        """
        self._write("memory.events", KILL_EVENTS)
        self._write("cgroup.procs", "")

        events = monitor_cgroup(self.cgroup_path)

        self.assertEqual(events.get("oom_kill"), 1)
        self.assertEqual(events.get("oom_group_kill"), 1)

    # ------------------------------------------------------------- detection

    def test_no_kill_with_zero_exit_code(self):
        """The ticket's counter dump with a successful payload sets no error code."""
        self._write("memory.events", NO_KILL_EVENTS)
        store_oom_baseline(self.cgroup_path)

        error_code, diagnostics, deltas = check_for_cgroup_oom_kill(0, self.cgroup_path)

        self.assertEqual(error_code, 0)
        self.assertEqual(diagnostics, "")
        self.assertEqual(deltas.get("oom_kill"), 0)

    def test_no_kill_with_non_zero_exit_code(self):
        """A failed payload with no OOM kill is not turned into a memory error."""
        self._write("memory.events", NO_KILL_EVENTS)
        store_oom_baseline(self.cgroup_path)

        error_code, _, _ = check_for_cgroup_oom_kill(137, self.cgroup_path)

        self.assertEqual(error_code, 0)

    def test_kill_with_non_zero_exit_code_sets_error(self):
        """An OOM kill plus a non-zero exit code returns PAYLOADOUTOFMEMORY."""
        self._write("memory.events", NO_KILL_EVENTS)
        store_oom_baseline(self.cgroup_path)
        self._write("memory.events", KILL_EVENTS)
        self._write("memory.max", str(4096 * 1024 * 1024))
        self._write("memory.peak", str(4093 * 1024 * 1024))

        error_code, diagnostics, deltas = check_for_cgroup_oom_kill(137, self.cgroup_path)

        self.assertEqual(error_code, errors.PAYLOADOUTOFMEMORY)
        self.assertEqual(deltas.get("oom_kill"), 1)
        self.assertIn("memory.events", diagnostics)
        self.assertIn("4096 MB", diagnostics)
        self.assertIn("4093 MB", diagnostics)

    def test_kill_with_zero_exit_code_warns_only(self):
        """An OOM kill with a zero exit code produces diagnostics but no error code."""
        self._write("memory.events", NO_KILL_EVENTS)
        store_oom_baseline(self.cgroup_path)
        self._write("memory.events", KILL_EVENTS)

        error_code, diagnostics, _ = check_for_cgroup_oom_kill(0, self.cgroup_path)

        self.assertEqual(error_code, 0)
        self.assertNotEqual(diagnostics, "")

    def test_missing_memory_events_is_a_noop(self):
        """No memory.events file means no check and no error, on any exit code."""
        error_code, diagnostics, deltas = check_for_cgroup_oom_kill(137, self.cgroup_path)

        self.assertEqual((error_code, diagnostics, deltas), (0, "", {}))

    def test_no_cgroup_path_is_a_noop(self):
        """With no subprocesses cgroup in the pilot cache the check is skipped."""
        error_code, diagnostics, deltas = check_for_cgroup_oom_kill(137)

        self.assertEqual((error_code, diagnostics, deltas), (0, "", {}))

    # -------------------------------------------------------------- baseline

    def test_kill_from_previous_job_is_not_attributed(self):
        """A kill already present at payload start does not fail the current payload.

        The subprocesses cgroup is created once per pilot and shared by every payload of
        a multi-job pilot, so the counters are cumulative and must be compared against a
        baseline taken at payload start.
        """
        self._write("memory.events", KILL_EVENTS)
        store_oom_baseline(self.cgroup_path)  # baseline already has oom_kill=1

        error_code, _, deltas = check_for_cgroup_oom_kill(137, self.cgroup_path)

        self.assertEqual(error_code, 0)
        self.assertEqual(deltas.get("oom_kill"), 0)

    def test_second_kill_after_baseline_is_detected(self):
        """A further kill on top of the baseline is attributed to the current payload."""
        self._write("memory.events", KILL_EVENTS)
        store_oom_baseline(self.cgroup_path)
        self._write("memory.events", KILL_EVENTS.replace("oom_kill 1", "oom_kill 2"))

        error_code, _, deltas = check_for_cgroup_oom_kill(137, self.cgroup_path)

        self.assertEqual(error_code, errors.PAYLOADOUTOFMEMORY)
        self.assertEqual(deltas.get("oom_kill"), 1)

    def test_deltas_without_baseline_use_absolute_values(self):
        """Without a baseline the absolute counters are used (fail towards reporting)."""
        self._write("memory.events", KILL_EVENTS)
        events = read_memory_events(self.cgroup_path)

        self.assertEqual(get_oom_deltas(self.cgroup_path, events).get("oom_kill"), 1)

    def test_store_oom_baseline_uses_pilot_cache_path(self):
        """store_oom_baseline() falls back to the subprocesses cgroup in the pilot cache."""
        self._write("memory.events", NO_KILL_EVENTS)
        pilot_cache.add_cgroup("subprocesses", self.cgroup_path)

        events = store_oom_baseline()

        self.assertEqual(events.get("max"), 5605)
        self.assertEqual(pilot_cache.get_oom_baseline(self.cgroup_path).get("max"), 5605)

    # ------------------------------------------------------- error reporting

    def test_error_codes_set_on_job(self):
        """PAYLOADOUTOFMEMORY is reported first, with PAYLOADOOMKILL as secondary code."""
        self._write("memory.events", NO_KILL_EVENTS)
        pilot_cache.add_cgroup("subprocesses", self.cgroup_path)
        store_oom_baseline(self.cgroup_path)
        self._write("memory.events", KILL_EVENTS)

        job = self._make_job()
        found = payload_module.set_error_from_cgroup_oom_kill(job, 137)

        self.assertTrue(found)
        # only the first error code is reported to the server, so 1212 must come first
        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADOUTOFMEMORY)
        self.assertIn(errors.PAYLOADOOMKILL, job.piloterrorcodes)
        self.assertIn("memory.events", job.piloterrordiags[0])

    def test_prmon_error_is_not_overwritten(self):
        """An existing PAYLOADEXCEEDMAXMEM from prmon takes precedence."""
        self._write("memory.events", NO_KILL_EVENTS)
        pilot_cache.add_cgroup("subprocesses", self.cgroup_path)
        store_oom_baseline(self.cgroup_path)
        self._write("memory.events", KILL_EVENTS)

        job = self._make_job()
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXCEEDMAXMEM)

        found = payload_module.set_error_from_cgroup_oom_kill(job, 137)

        self.assertFalse(found)
        self.assertEqual(job.piloterrorcodes, [errors.PAYLOADEXCEEDMAXMEM])

    def test_no_error_set_when_no_kill(self):
        """No error code is set on the job when there was no OOM kill."""
        self._write("memory.events", NO_KILL_EVENTS)
        pilot_cache.add_cgroup("subprocesses", self.cgroup_path)
        store_oom_baseline(self.cgroup_path)

        job = self._make_job()
        found = payload_module.set_error_from_cgroup_oom_kill(job, 137)

        self.assertFalse(found)
        self.assertEqual(job.piloterrorcodes, [])

    # ------------------------------------------------------------ job lookup

    def test_get_current_job_reads_monitored_payloads(self):
        """The running job is found on the monitored_payloads queue.

        Regression test: the previous implementation looked for a running_jobs queue,
        which the workflow never creates, so every in-flight OOM detection ended with
        "no running job found in queues" and no error code was ever set.
        """
        queues = SimpleNamespace(monitored_payloads=queue.Queue())
        job = self._make_job()
        queues.monitored_payloads.put(job)

        self.assertIs(_get_current_job(queues), job)
        # the job must not be consumed from the queue
        self.assertEqual(queues.monitored_payloads.qsize(), 1)

    def test_get_current_job_returns_none_when_empty(self):
        """No running job yields None rather than an exception."""
        queues = SimpleNamespace(monitored_payloads=queue.Queue())

        self.assertIsNone(_get_current_job(queues))


if __name__ == '__main__':
    unittest.main()
