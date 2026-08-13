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

"""Unit tests for the opt-in GPU/PID visibility diagnostic.

The diagnostic exists to answer whether the PID the pilot hands to prmon has
the actual GPU-bound process among its descendants, on the queues where prmon
reports all-zero GPU statistics (ngpus/gpufbmem/gpusmpct/gpumempct) while the
payload's own monitoring shows real GPU activity.

Covers:
- gating: inert unless PILOT_GPU_DEBUG is set *and* nvidia-smi is present;
- resolution of the monitored PID from the stored memory monitor command
  (which is where the container-resolved '--pid <pid>' actually lives), with
  the job.pid fallback;
- parsing of '--query-compute-apps' csv output, including the correct
  'used_gpu_memory' field name;
- the three-way classification (in the monitored tree / visible but outside it
  / not visible at all) and the verdict derived from it;
- the snapshot schedule (bounded number of snapshots, spaced by the configured
  offsets);
- that a failure anywhere in the diagnostic can never propagate into the job
  monitoring loop.
"""

import logging
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

from pilot.util import gpudiagnostics
from pilot.util.gpudiagnostics import (
    GPU_DEBUG_ENV_VAR,
    SNAPSHOT_OFFSETS,
    classify_gpu_pid,
    extract_pids_from_compute_apps,
    get_monitored_pid,
    get_verdict,
    is_gpu_diagnostics_enabled,
    is_snapshot_due,
    report_gpu_pid_visibility,
    reset_gpu_diagnostics_state,
    run_nvidia_smi,
)

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Realistic '--query-compute-apps=pid,process_name,used_gpu_memory' output.
COMPUTE_APPS_OUTPUT = (
    "1234567, python, 512 MiB\n"
    "1234599, athena.py, 2048 MiB"
)


class MockJob:
    """Minimal job-like object sufficient for the diagnostic."""

    def __init__(self, pid: int = 0, utilities: dict = None):
        """Set the payload pid and the utilities dictionary."""
        self.pid = pid
        self.utilities = utilities if utilities is not None else {}


class TestGpuDiagnosticsGating(unittest.TestCase):
    """Tests for the opt-in gating of the diagnostic."""

    def setUp(self):
        """Reset module-level snapshot state before each test."""
        reset_gpu_diagnostics_state()

    def tearDown(self):
        """Reset module-level snapshot state after each test."""
        reset_gpu_diagnostics_state()

    def test_disabled_when_env_var_not_set(self):
        """Verify the diagnostic is inert when the env var is absent."""
        with patch.dict(os.environ, {}, clear=True):
            self.assertFalse(is_gpu_diagnostics_enabled())

    def test_disabled_when_env_var_falsy(self):
        """Verify a falsy env var value does not enable the diagnostic."""
        for value in ("0", "false", "no", ""):
            with patch.dict(os.environ, {GPU_DEBUG_ENV_VAR: value}, clear=True):
                self.assertFalse(is_gpu_diagnostics_enabled())

    def test_disabled_without_nvidia_smi(self):
        """Verify the diagnostic is a no-op on queues without nvidia-smi."""
        with patch.dict(os.environ, {GPU_DEBUG_ENV_VAR: "1"}, clear=True):
            with patch("pilot.util.gpudiagnostics.which", return_value=None):
                self.assertFalse(is_gpu_diagnostics_enabled())

    def test_enabled_when_env_var_set_and_nvidia_smi_present(self):
        """Verify the diagnostic is enabled when both conditions hold."""
        for value in ("1", "true", "YES", "On"):
            with patch.dict(os.environ, {GPU_DEBUG_ENV_VAR: value}, clear=True):
                with patch("pilot.util.gpudiagnostics.which", return_value="/usr/bin/nvidia-smi"):
                    self.assertTrue(is_gpu_diagnostics_enabled())

    def test_no_snapshot_taken_when_disabled(self):
        """Verify no snapshot is collected while the diagnostic is disabled."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("pilot.util.gpudiagnostics.log_gpu_pid_snapshot") as mock_snapshot:
                report_gpu_pid_visibility(MockJob(pid=4321))
        mock_snapshot.assert_not_called()


class TestMonitoredPidResolution(unittest.TestCase):
    """Tests for resolving the PID that prmon was actually given."""

    def test_pid_taken_from_memory_monitor_command(self):
        """Verify the pid is read from the stored memory monitor command.

        get_utility_command_setup() resolves the payload pid (which differs
        from the Popen pid when the payload runs in a container) and does not
        store it on the job object, but the full command it built is kept in
        job.utilities and carries '--pid <pid>'.
        """
        cmd = (
            "cd /srv/workdir;source /cvmfs/atlas.cern.ch/setup.sh;lsetup prmon;"
            "prmon --pid 99887 --filename memory_monitor_output.txt "
            "--json-summary memory_monitor_summary.json --interval 60"
        )
        job = MockJob(pid=12345, utilities={"MemoryMonitor": ["proc", 1, cmd]})
        pid, source = get_monitored_pid(job)
        self.assertEqual(pid, 99887)
        self.assertNotEqual(pid, job.pid)
        self.assertIn("memory monitor", source)

    def test_pid_taken_from_prmon_override_command(self):
        """Verify the pid is also found in a PILOT_PRMON_CMD override command."""
        cmd = "/opt/site/wrap_prmon.sh --pid 55555"
        job = MockJob(pid=12345, utilities={"MemoryMonitor": ["proc", 1, cmd]})
        pid, _ = get_monitored_pid(job)
        self.assertEqual(pid, 55555)

    def test_fallback_to_job_pid(self):
        """Verify job.pid is used before the memory monitor has started."""
        job = MockJob(pid=12345)
        pid, source = get_monitored_pid(job)
        self.assertEqual(pid, 12345)
        self.assertIn("job.pid", source)

    def test_zero_when_no_pid_known(self):
        """Verify a missing pid is reported as 0 rather than raising."""
        pid, _ = get_monitored_pid(MockJob())
        self.assertEqual(pid, 0)


class TestComputeAppsParsing(unittest.TestCase):
    """Tests for parsing nvidia-smi compute-apps output."""

    def test_pids_extracted(self):
        """Verify GPU-active pids are extracted from csv output."""
        self.assertEqual(
            extract_pids_from_compute_apps(COMPUTE_APPS_OUTPUT), [1234567, 1234599]
        )

    def test_empty_output(self):
        """Verify no pids are returned when nvidia-smi reports none."""
        self.assertEqual(extract_pids_from_compute_apps(""), [])

    def test_unparsable_lines_ignored(self):
        """Verify noise in the output does not break pid extraction."""
        output = "No running processes found\n1234567, python, 512 MiB"
        self.assertEqual(extract_pids_from_compute_apps(output), [1234567])

    def test_correct_query_field_used(self):
        """Verify the valid 'used_gpu_memory' query field is used.

        'used_memory' is not a valid --query-compute-apps field and makes
        nvidia-smi fail, which would have produced an empty diagnostic.
        """
        with patch("pilot.util.gpudiagnostics.run_nvidia_smi", return_value="") as mock_run:
            gpudiagnostics.get_gpu_compute_apps()
        options = mock_run.call_args[0][0]
        self.assertIn("--query-compute-apps=pid,process_name,used_gpu_memory", options)
        self.assertNotIn("used_memory,", " ".join(options))


class TestNvidiaSmiExecution(unittest.TestCase):
    """Tests for the nvidia-smi wrapper."""

    def test_failure_returns_empty_string(self):
        """Verify a failing nvidia-smi call does not raise."""
        error = subprocess.CalledProcessError(1, "nvidia-smi", stderr="boom")
        with patch("pilot.util.gpudiagnostics.subprocess.run", side_effect=error):
            self.assertEqual(run_nvidia_smi(["pmon", "-c", "1"]), "")

    def test_timeout_returns_empty_string(self):
        """Verify a hanging nvidia-smi call is bounded and does not raise."""
        error = subprocess.TimeoutExpired("nvidia-smi", 60)
        with patch("pilot.util.gpudiagnostics.subprocess.run", side_effect=error):
            self.assertEqual(run_nvidia_smi(["pmon", "-c", "1"]), "")


class TestClassificationAndVerdict(unittest.TestCase):
    """Tests for the three-way classification and the resulting verdict."""

    def test_pid_in_monitored_tree(self):
        """Verify a GPU pid inside the monitored tree is classified as such."""
        self.assertEqual(classify_gpu_pid(4242, {1111, 4242}), "descendant")

    def test_pid_visible_but_outside_tree(self):
        """Verify a GPU pid that exists but is outside the tree is detected."""
        with patch("pilot.util.gpudiagnostics.os.path.exists", return_value=True):
            self.assertEqual(classify_gpu_pid(4242, {1111}), "visible")

    def test_pid_not_visible_at_all(self):
        """Verify a GPU pid absent from /proc is flagged as invisible.

        This is the PID-namespace case: nvidia-smi reports host pids, so if
        the pilot cannot see them at all, prmon can never match them.
        """
        with patch("pilot.util.gpudiagnostics.os.path.exists", return_value=False):
            self.assertEqual(classify_gpu_pid(4242, {1111}), "invisible")

    def test_verdict_no_gpu_processes_is_inconclusive(self):
        """Verify the absence of GPU processes is not reported as a problem."""
        verdict, is_problem = get_verdict({}, [])
        self.assertFalse(is_problem)
        self.assertIn("inconclusive", verdict)

    def test_verdict_descendant_points_away_from_pid_visibility(self):
        """Verify an in-tree GPU process clears the PID visibility theory."""
        verdict, is_problem = get_verdict({42: "descendant"}, [42])
        self.assertFalse(is_problem)
        self.assertIn("IS in the monitored process tree", verdict)

    def test_verdict_visible_but_outside_tree_is_a_problem(self):
        """Verify an escaped GPU process is reported as a problem."""
        verdict, is_problem = get_verdict({42: "visible"}, [42])
        self.assertTrue(is_problem)
        self.assertIn("escaped", verdict)

    def test_verdict_invisible_reports_namespace_mismatch(self):
        """Verify an invisible GPU process yields the namespace verdict."""
        verdict, is_problem = get_verdict({42: "invisible"}, [42])
        self.assertTrue(is_problem)
        self.assertIn("PID namespace", verdict)

    def test_descendant_wins_over_other_categories(self):
        """Verify a single in-tree match dominates the verdict."""
        classifications = {1: "invisible", 2: "visible", 3: "descendant"}
        _, is_problem = get_verdict(classifications, [1, 2, 3])
        self.assertFalse(is_problem)


class TestSnapshotSchedule(unittest.TestCase):
    """Tests for the bounded, spaced snapshot schedule."""

    def setUp(self):
        """Reset module-level snapshot state before each test."""
        reset_gpu_diagnostics_state()

    def tearDown(self):
        """Reset module-level snapshot state after each test."""
        reset_gpu_diagnostics_state()

    def test_first_call_is_due(self):
        """Verify the first call always takes a snapshot."""
        self.assertTrue(is_snapshot_due(now=1000))

    def test_second_call_not_due_immediately(self):
        """Verify snapshots are spaced, not taken on every monitoring loop."""
        self.assertTrue(is_snapshot_due(now=1000))
        self.assertFalse(is_snapshot_due(now=1001))
        self.assertFalse(is_snapshot_due(now=1000 + SNAPSHOT_OFFSETS[1] - 1))

    def test_second_snapshot_due_after_offset(self):
        """Verify the next snapshot is taken once its offset has elapsed.

        The GPU-touching process may be forked long after prmon starts, so a
        single snapshot at payload start could be a false negative.
        """
        self.assertTrue(is_snapshot_due(now=1000))
        self.assertTrue(is_snapshot_due(now=1000 + SNAPSHOT_OFFSETS[1]))

    def test_schedule_is_bounded(self):
        """Verify the diagnostic stops after the configured snapshot count."""
        now = 1000
        taken = 0
        for _ in range(100):
            if is_snapshot_due(now=now):
                taken += 1
            now += 60
        self.assertEqual(taken, len(SNAPSHOT_OFFSETS))


class TestSnapshotOutput(unittest.TestCase):
    """End-to-end tests for the logged snapshot."""

    def setUp(self):
        """Reset module-level snapshot state before each test."""
        reset_gpu_diagnostics_state()

    def tearDown(self):
        """Reset module-level snapshot state after each test."""
        reset_gpu_diagnostics_state()

    def _run_snapshot(self, compute_apps: str) -> str:
        """Collect one snapshot with mocked nvidia-smi output and return the log.

        Returns:
            All log records emitted by the diagnostic, joined by newlines.
        """
        job = MockJob(
            pid=os.getpid(),
            utilities={"MemoryMonitor": ["proc", 1, f"lsetup prmon;prmon --pid {os.getpid()}"]},
        )
        with patch.object(gpudiagnostics, "get_gpu_device_info", return_value="0, A100, 610.43.02, Disabled, Disabled, Enabled"), \
                patch.object(gpudiagnostics, "get_gpu_compute_apps", return_value=compute_apps), \
                patch.object(gpudiagnostics, "get_gpu_pmon_sample", return_value="# gpu pid type sm mem command"):
            with self.assertLogs("pilot.util.gpudiagnostics", level="INFO") as captured:
                gpudiagnostics.log_gpu_pid_snapshot(job, 1)

        return "\n".join(captured.output)

    def test_snapshot_reports_namespace_mismatch(self):
        """Verify a host pid unknown to the pilot yields the namespace verdict.

        This reproduces the reported failure shape (CERN-GPU, SLAC_GPU): real
        GPU activity on the node, but nothing prmon can attribute by pid.
        """
        output = self._run_snapshot("9999999, python, 512 MiB")
        self.assertIn("NOT visible in this pilot's /proc", output)
        self.assertIn("different PID namespace", output)

    def test_snapshot_reports_in_tree_process(self):
        """Verify a GPU pid inside the monitored tree is reported as such."""
        output = self._run_snapshot(f"{os.getpid()}, python, 512 MiB")
        self.assertIn("IN the monitored process tree", output)

    def test_snapshot_survives_no_gpu_processes(self):
        """Verify an empty compute-apps list is reported as inconclusive."""
        output = self._run_snapshot("")
        self.assertIn("no GPU-active compute processes", output)


class TestFailureIsolation(unittest.TestCase):
    """Tests that the diagnostic cannot disturb the job monitoring loop."""

    def setUp(self):
        """Reset module-level snapshot state before each test."""
        reset_gpu_diagnostics_state()

    def tearDown(self):
        """Reset module-level snapshot state after each test."""
        reset_gpu_diagnostics_state()

    def test_exception_is_swallowed(self):
        """Verify an unexpected failure is logged and not re-raised."""
        with patch.dict(os.environ, {GPU_DEBUG_ENV_VAR: "1"}, clear=True):
            with patch("pilot.util.gpudiagnostics.which", return_value="/usr/bin/nvidia-smi"):
                with patch(
                    "pilot.util.gpudiagnostics.log_gpu_pid_snapshot",
                    side_effect=RuntimeError("unexpected"),
                ):
                    report_gpu_pid_visibility(MockJob(pid=4321))  # must not raise

    def test_snapshot_runs_when_enabled(self):
        """Verify a snapshot is collected when the diagnostic is enabled."""
        with patch.dict(os.environ, {GPU_DEBUG_ENV_VAR: "1"}, clear=True):
            with patch("pilot.util.gpudiagnostics.which", return_value="/usr/bin/nvidia-smi"):
                with patch("pilot.util.gpudiagnostics.log_gpu_pid_snapshot") as mock_snapshot:
                    report_gpu_pid_visibility(MockJob(pid=4321))
        mock_snapshot.assert_called_once()


if __name__ == "__main__":
    unittest.main()
