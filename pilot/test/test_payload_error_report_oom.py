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

"""Unit tests for the memory error checks on the transform error-report path.

Production symptom that motivated this code (job 7056870203): the payload was SIGKILLed,
runGen wrote ``payload_error_report.json`` with ``error_code 5318`` and ``error_diag
"payload execution failed with 137"``, and the pilot reported PAYLOADEXECUTIONFAILURE
(1305) with no memory diagnosis at all. The error-report branch of
``perform_initial_payload_error_analysis()`` returned before reaching the memory checks, so
the cgroup ``memory.events`` detection was unreachable on precisely the path where a
SIGKILL - and therefore an OOM kill - is most likely.

runGen only writes this file for user analysis jobs; production ``*_tf`` jobs report through
``jobReport.json`` and never enter this branch.

Covers:
- perform_initial_payload_error_analysis(): the memory checks now run before the branch
  returns, so a confirmed OOM kill yields PAYLOADOUTOFMEMORY first while the transform's own
  code is preserved in exeErrorCode.
- the no-regression case: with no OOM kill the outcome is byte-identical to the previous
  behaviour, i.e. PAYLOADEXECUTIONFAILURE alone.
- prmon's PAYLOADEXCEEDMAXMEM is still reported first when it won the race.
- get_effective_exit_code(): the real payload exit code is recovered from the transform's
  free-text diagnostics so that a pilot-observed exit code of 0 does not downgrade a
  confirmed kill to a warning.
- a truncated or empty error report no longer raises AttributeError (memory.oom.group=1 can
  take runGen down mid-write, and the call site is unguarded).
"""

import json
import logging
import os
import sys
import tempfile
import unittest
from types import SimpleNamespace
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.control import payload as payload_module
from pilot.test.test_cgroup_oom_kill import KILL_EVENTS, NO_KILL_EVENTS
from pilot.util.cgroups import store_oom_baseline
from pilot.util.config import config

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()
pilot_cache = get_pilot_cache()

# the error report written by runGen for the job in the ticket
ERROR_REPORT = {"error_code": 5318, "error_diag": "payload execution failed with 137"}


class TestPayloadErrorReportOom(unittest.TestCase):
    """Tests for the memory checks reached through the transform error report."""

    def setUp(self):
        """Create a fake job workdir and cgroup, and reset all shared singleton state."""
        self.tmpdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = os.path.join(self.tmpdir.name, "workdir")
        self.cgroup_path = os.path.join(self.tmpdir.name, "cgroup")
        os.makedirs(self.workdir)
        os.makedirs(self.cgroup_path)

        # the error code lists are class-level on ErrorCodes and leak between tests
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []

        # the pilot cache is a process-wide singleton
        self._saved_cgroups = dict(pilot_cache.cgroups)
        self._saved_baselines = dict(pilot_cache.oom_baselines)
        pilot_cache.cgroups = {}
        pilot_cache.oom_baselines = {}

    def tearDown(self):
        """Restore the singleton state and remove the temporary directories."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []
        pilot_cache.cgroups = self._saved_cgroups
        pilot_cache.oom_baselines = self._saved_baselines
        self.tmpdir.cleanup()

    # ---------------------------------------------------------------- helpers

    def _make_job(self):
        """Return a minimal job-like object with the fields the code under test touches."""
        return SimpleNamespace(
            jobid="7056870203",
            workdir=self.workdir,
            piloterrorcodes=[],
            piloterrordiags=[],
            exeerrorcode=0,
            exeerrordiag="",
            subprocesses=[],  # empty, so the dmesg fallback is not consulted
            debug=False,
            pid=None
        )

    def _write_error_report(self, content: Any = None):
        """Write the transform error report into the fake job workdir.

        Args:
            content: dict to serialise as JSON, or a raw string written verbatim. Defaults to
                the report written by runGen for the job in the ticket.
        """
        if content is None:
            content = dict(ERROR_REPORT)
        path = os.path.join(self.workdir, config.Payload.error_report)
        with open(path, "w", encoding="utf-8") as fh:
            if isinstance(content, str):
                fh.write(content)
            else:
                json.dump(content, fh)

    def _write_events(self, content: str):
        """Write a fake memory.events file into the fake cgroup directory.

        Args:
            content: file content.
        """
        with open(os.path.join(self.cgroup_path, "memory.events"), "w", encoding="utf-8") as fh:
            fh.write(content)

    def _arm_cgroup(self, killed: bool):
        """Register the fake cgroup and set its counters relative to the payload baseline.

        Args:
            killed: whether the OOM killer should appear to have fired since payload start.
        """
        pilot_cache.add_cgroup("subprocesses", self.cgroup_path)
        self._write_events(NO_KILL_EVENTS)
        store_oom_baseline(self.cgroup_path)
        if killed:
            self._write_events(KILL_EVENTS)

    # ------------------------------------------------------- the ticket's job

    def test_oom_kill_is_detected_despite_error_report(self):
        """A SIGKILLed payload with a transform error report gets a memory error code.

        Regression test for the ticket: the branch used to return before the memory checks,
        so 1305 was all the server ever saw for job 7056870203.
        """
        self._write_error_report()
        self._arm_cgroup(killed=True)

        job = self._make_job()
        payload_module.perform_initial_payload_error_analysis(job, 137)

        # only the first error code is reported to the server, so 1212 must come first
        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADOUTOFMEMORY)
        self.assertIn(errors.PAYLOADOOMKILL, job.piloterrorcodes)
        self.assertIn(errors.PAYLOADEXECUTIONFAILURE, job.piloterrorcodes)
        self.assertIn("memory.events", job.piloterrordiags[0])

        # the transform's own diagnosis must survive untouched
        self.assertEqual(job.exeerrorcode, 5318)
        self.assertEqual(job.exeerrordiag, ERROR_REPORT["error_diag"])

    def test_no_oom_kill_keeps_previous_behaviour(self):
        """With no OOM kill the outcome is identical to the behaviour before the fix.

        This is the key no-regression test: a genuine transform error must not acquire a
        memory error code just because the memory checks now run on this path.
        """
        self._write_error_report()
        self._arm_cgroup(killed=False)

        job = self._make_job()
        payload_module.perform_initial_payload_error_analysis(job, 137)

        self.assertEqual(job.piloterrorcodes, [errors.PAYLOADEXECUTIONFAILURE])
        self.assertEqual(job.piloterrordiags, [ERROR_REPORT["error_diag"]])
        self.assertEqual(job.exeerrorcode, 5318)

    def test_prmon_error_is_still_reported_first(self):
        """PAYLOADEXCEEDMAXMEM from prmon takes precedence over the post-mortem check."""
        self._write_error_report()
        self._arm_cgroup(killed=True)

        job = self._make_job()
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PAYLOADEXCEEDMAXMEM)

        payload_module.perform_initial_payload_error_analysis(job, 137)

        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADEXCEEDMAXMEM)
        self.assertNotIn(errors.PAYLOADOUTOFMEMORY, job.piloterrorcodes)

    def test_zero_exit_code_with_error_report_still_detects_kill(self):
        """A pilot-observed exit code of 0 does not downgrade a confirmed kill to a warning.

        The transform reported a non-zero error code, so the payload did not survive and the
        "exit code 0 means warn only" rule must not apply. The real exit code (137) is
        recovered from the transform's diagnostics.
        """
        self._write_error_report()
        self._arm_cgroup(killed=True)

        job = self._make_job()
        payload_module.perform_initial_payload_error_analysis(job, 0)

        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADOUTOFMEMORY)

    def test_no_cgroup_is_a_noop_on_this_path(self):
        """On a site without cgroups the behaviour is unchanged (dmesg is the only detector)."""
        self._write_error_report()

        job = self._make_job()
        payload_module.perform_initial_payload_error_analysis(job, 137)

        self.assertEqual(job.piloterrorcodes, [errors.PAYLOADEXECUTIONFAILURE])

    # ------------------------------------------------------- malformed report

    def test_truncated_error_report_does_not_raise(self):
        """A truncated error report falls through to the normal path instead of raising.

        memory.oom.group=1 kills every process in the cgroup atomically, so runGen can be
        taken down while writing this file. read_json() returns None in that case, and the
        call site is unguarded, so this used to raise AttributeError inside the payload
        thread. Falling through is also the desired behaviour, since the normal path runs
        the memory checks anyway.
        """
        self._write_error_report('{"error_code": 5318, "error_diag": "payload exec')
        self._arm_cgroup(killed=True)

        job = self._make_job()
        payload_module.perform_initial_payload_error_analysis(job, 137)

        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADOUTOFMEMORY)

    def test_empty_error_report_does_not_raise(self):
        """An empty error report is handled in the same way as a truncated one."""
        self._write_error_report("")
        self._arm_cgroup(killed=False)

        job = self._make_job()
        payload_module.perform_initial_payload_error_analysis(job, 137)

        # no OOM kill, so the normal path sets the generic failure code
        self.assertEqual(job.piloterrorcodes, [errors.PAYLOADEXECUTIONFAILURE])

    # -------------------------------------------------- effective exit code

    def test_effective_exit_code_prefers_observed_code(self):
        """A non-zero exit code observed by the pilot is used as it is."""
        self.assertEqual(payload_module.get_effective_exit_code(9, ERROR_REPORT), 9)

    def test_effective_exit_code_recovered_from_diagnostics(self):
        """The payload exit code is parsed out of the transform's free-text diagnostics."""
        self.assertEqual(payload_module.get_effective_exit_code(0, ERROR_REPORT), 137)

    def test_effective_exit_code_falls_back_to_error_code(self):
        """Without a parsable exit code the transform's own error code is used."""
        report = {"error_code": 5318, "error_diag": "something else went wrong"}

        self.assertEqual(payload_module.get_effective_exit_code(0, report), 5318)

    def test_effective_exit_code_handles_missing_fields(self):
        """An empty report yields zero rather than raising."""
        self.assertEqual(payload_module.get_effective_exit_code(0, {}), 0)

    def test_effective_exit_code_handles_string_error_code(self):
        """An error code serialised as a string is still usable."""
        report = {"error_code": "5318", "error_diag": None}

        self.assertEqual(payload_module.get_effective_exit_code(0, report), 5318)


if __name__ == '__main__':
    unittest.main()
