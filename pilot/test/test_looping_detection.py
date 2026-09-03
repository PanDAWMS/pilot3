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

"""Unit tests for looping job detection.

The production failure being fixed: after the looping job diagnostics were
added, the new pilot did not report a single looping job (error code 1150) in a
full day at 5% of production, while the pilot it is replacing reported them
continuously.

The looping algorithm decides that a payload is alive from the modification
time of the most recently modified file in the job work directory. The
diagnostic snapshot series is written into that same directory, as
``looping_snapshots.log``, and is not filtered out by ``remove_unwanted_files()``
in any experiment plugin, so the pilot's own write looked exactly like payload
activity. Since the series starts at ``looping_snapshot_fraction`` (0.5) of the
looping limit and is written at every looping verification, the time since the
last touch was pinned at roughly half the limit plus one verification interval
and could never exceed the limit. Detection was not degraded, it was impossible.

Covers:
- the artifacts of the diagnostics are recognised as pilot output;
- the plugin filter composed with the central filter drops them for every
  experiment, which is where the plugin filters alone failed;
- the reported scenario end to end: a payload that stopped touching its files
  is still reported as looping after the pilot has written a snapshot, with
  each of the two guards (the central filter, the pinned modification time)
  tested on its own so that neither can quietly stop working;
- the detection is recorded before the diagnostics run, so that a failing core
  dump or kill cannot cancel it - the error code used to be assigned after
  both, inside the same try block;
- the check requires the job and the experiment plugin to agree, which the
  inverted gate in verify_looping_job() did not enforce.
"""

import logging
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.util import loopingdumps, loopingjob, monitoring
from pilot.util.loopingdumps import (
    SNAPSHOT_FILENAME,
    is_looping_diagnostic_file,
    remove_diagnostic_files,
    reset_looping_dump_state,
    take_looping_snapshot,
)
from pilot.util.loopingjob import (
    get_time_for_last_touch,
    kill_looping_job,
    looping_job,
)
from pilot.util.monitoringtime import MonitoringTime

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

errors = ErrorCodes()

EXPERIMENTS = ("atlas", "generic", "epic", "sphenix", "darkside", "rubin", "ska")

# the looping limit and verification time from util/default.cfg
LOOPING_LIMIT = 7200

# time since the payload last touched a file: past the snapshot threshold
# (0.5 * 7200 s) but not yet past the limit, i.e. the state in which the
# snapshot is written
SINCE_TOUCH = 3700


class FakeJob:
    """Minimal stand-in for JobData carrying only what the looping path reads."""

    def __init__(self, workdir="", pid=1000, jobid="6789012345", state="running"):
        """Initialise the fake job.

        Args:
            workdir: Job work directory.
            pid: Payload process id.
            jobid: PanDA job id.
            state: Job state.
        """
        self.workdir = workdir
        self.pid = pid
        self.jobid = jobid
        self.state = state
        self.debug = False
        self.debug_command = ""
        self.looping_check = True
        self.zombies = []
        self.piloterrorcodes = []
        self.piloterrordiags = []
        self.swrelease = "Athena-24.0.41"
        self.homepackage = "AthGeneration/24.0.41"
        self.platform = "x86_64-el9-gcc13-opt"
        self.transformation = "Generate_tf.py"
        self.imagename = ""

    def collect_zombies(self, depth=10):
        """Do nothing.

        Args:
            depth: Recursion depth, ignored.
        """

    @staticmethod
    def get_lfns_and_guids():
        """Return no input files.

        Returns:
            Tuple of two empty lists.
        """
        return [], []


def snapshot_patches():
    """Return the patches that make a snapshot cheap and deterministic.

    Returns:
        Tuple of context managers.
    """
    return (
        patch.object(loopingdumps, "get_descendants", return_value=[
            (5001, "/usr/bin/python3 /cvmfs/sw/bin/athena.py runargs.py")
        ]),
        patch.object(loopingdumps, "get_payload_process_names", return_value=[]),
        patch.object(loopingdumps, "get_cpu_time", return_value=100.0),
        patch.object(loopingdumps, "get_rss", return_value=1024 * 1024),
        patch.object(loopingdumps, "get_stack_tool", return_value=""),
        patch.object(loopingdumps, "get_stack_trace", return_value="frame0\nframe1"),
    )


class TestDiagnosticFiles(unittest.TestCase):
    """The diagnostics write into the directory the algorithm measures."""

    def test_the_artifacts_are_recognised(self):
        """Every file the diagnostics write must be recognised as pilot output."""
        for name in (SNAPSHOT_FILENAME, "core.12345", "core.12345.analysis.txt"):
            self.assertTrue(is_looping_diagnostic_file(f"/srv/workdir/{name}"), msg=name)

    def test_payload_files_are_not_recognised(self):
        """A payload file wrongly dropped here would hide real progress."""
        for name in ("payload.stdout", "log.generate", "EVNT.root", "core_dump_settings.txt",
                     "corefile.txt", "analysis.txt"):
            self.assertFalse(is_looping_diagnostic_file(f"/srv/workdir/{name}"), msg=name)

    def test_empty_path(self):
        """An empty path is not an artifact rather than an error."""
        self.assertFalse(is_looping_diagnostic_file(""))

    def test_the_filters_together_drop_them_for_every_experiment(self):
        """The plugin filter alone did not, which is why the central filter exists."""
        workdir = "/srv/workdir"
        files = [
            workdir,
            os.path.join(workdir, "payload.stdout"),
            os.path.join(workdir, SNAPSHOT_FILENAME),
            os.path.join(workdir, "core.12345"),
            os.path.join(workdir, "core.12345.analysis.txt"),
        ]
        for experiment in EXPERIMENTS:
            definitions = __import__(f"pilot.user.{experiment}.loopingjob_definitions",
                                     globals(), locals(), [experiment], 0)
            kept = remove_diagnostic_files(definitions.remove_unwanted_files(workdir, files))
            self.assertEqual(kept, [os.path.join(workdir, "payload.stdout")],
                             msg=f"{experiment} keeps a looping diagnostic file")


class TestSnapshotCannotResetTheClock(unittest.TestCase):
    """The reported scenario: a written snapshot must not look like payload activity."""

    def setUp(self):
        """Reset the snapshot bookkeeping and set the experiment."""
        reset_looping_dump_state()
        self.environ = patch.dict(os.environ, {"PILOT_USER": "atlas"})
        self.environ.start()

    def tearDown(self):
        """Reset the snapshot bookkeeping."""
        self.environ.stop()
        reset_looping_dump_state()

    @staticmethod
    def _stuck_job(workdir):
        """Create a work directory whose payload output is SINCE_TOUCH seconds old.

        Args:
            workdir: Job work directory.

        Returns:
            FakeJob instance.
        """
        path = os.path.join(workdir, "payload.stdout")
        with open(path, "w", encoding="utf-8") as _file:
            _file.write("stuck\n")
        stale = time.time() - SINCE_TOUCH
        os.utime(path, (stale, stale))

        return FakeJob(workdir=workdir)

    def _since_touch(self, job, montime):
        """Return the time since the payload last touched a file.

        Args:
            job: Job object.
            montime: MonitoringTime object.

        Returns:
            Seconds since the last touch.
        """
        time_last_touched, _ = get_time_for_last_touch(job, montime, LOOPING_LIMIT)
        self.assertIsNotNone(time_last_touched, msg="no measurement was made at all")

        return int(time.time()) - time_last_touched

    def _run(self, workdir):
        """Measure, take a snapshot as the algorithm does, and measure again.

        Args:
            workdir: Job work directory.

        Returns:
            Tuple of (since_touch before the snapshot, since_touch after it).
        """
        job = self._stuck_job(workdir)
        montime = MonitoringTime()

        before = self._since_touch(job, montime)
        take_looping_snapshot(job, before, LOOPING_LIMIT)
        self.assertTrue(os.path.exists(os.path.join(workdir, SNAPSHOT_FILENAME)),
                        msg="the snapshot was not written, so nothing is being tested")

        return before, self._since_touch(job, montime)

    def test_the_clock_is_not_reset(self):
        """The production failure: the snapshot reset the clock it was triggered by."""
        patches = snapshot_patches()
        for _patch in patches:
            _patch.start()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                before, after = self._run(workdir)
        finally:
            for _patch in patches:
                _patch.stop()

        self.assertGreaterEqual(before, SINCE_TOUCH)
        self.assertGreaterEqual(after, before, msg="the snapshot reset the looping clock")

    def test_the_central_filter_is_enough_on_its_own(self):
        """With the modification time not pinned, the filter must still hold."""
        patches = snapshot_patches() + (
            patch.object(loopingdumps, "pin_diagnostic_mtime"),  # do nothing
        )
        for _patch in patches:
            _patch.start()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                before, after = self._run(workdir)
        finally:
            for _patch in patches:
                _patch.stop()

        self.assertGreaterEqual(after, before, msg="the central filter does not hold on its own")

    def test_the_pinned_modification_time_is_enough_on_its_own(self):
        """With the artifacts not filtered, the pinned modification time must still hold."""
        patches = snapshot_patches() + (
            patch.object(loopingjob, "remove_diagnostic_files", side_effect=lambda files: files),
        )
        for _patch in patches:
            _patch.start()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                before, after = self._run(workdir)
        finally:
            for _patch in patches:
                _patch.stop()

        self.assertGreaterEqual(after, before, msg="the pinned modification time does not hold on its own")

    def test_the_series_still_accumulates(self):
        """Pinning the modification time must not cost the diagnostics themselves."""
        patches = snapshot_patches()
        for _patch in patches:
            _patch.start()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                job = self._stuck_job(workdir)
                path = os.path.join(workdir, SNAPSHOT_FILENAME)
                for since in (4000, 5000, 6000):
                    take_looping_snapshot(job, since, LOOPING_LIMIT)
                    self.assertLessEqual(os.path.getmtime(path), time.time() - SINCE_TOUCH + 1,
                                         msg="the snapshot file is newer than the last payload touch")
                with open(path, encoding="utf-8") as _file:
                    contents = _file.read()
        finally:
            for _patch in patches:
                _patch.stop()

        for index in (1, 2, 3):
            self.assertIn(f"snapshot #{index}", contents)


class TestDetectionSurvivesFailingDiagnostics(unittest.TestCase):
    """A diagnostic must not be able to cancel the detection it documents."""

    def setUp(self):
        """Reset the shared error code lists."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []

    def tearDown(self):
        """Reset the shared error code lists."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []

    @staticmethod
    def _looping_patches():
        """Return the patches placing the job past the looping limit.

        Returns:
            Tuple of context managers.
        """
        return (
            patch.object(loopingjob, "get_time_for_last_touch",
                         return_value=(int(time.time()) - 2 * LOOPING_LIMIT, [])),
            patch.object(loopingjob, "time_since_suspension", return_value=0),
            patch.object(loopingjob, "get_looping_job_limit", return_value=LOOPING_LIMIT),
        )

    def _run(self, extra):
        """Run looping_job() with the given extra patches.

        Args:
            extra: Tuple of additional context managers.

        Returns:
            Tuple of (exit code, diagnostics).
        """
        patches = self._looping_patches() + extra
        for _patch in patches:
            _patch.start()
        try:
            return looping_job(FakeJob(), MonitoringTime())
        finally:
            for _patch in patches:
                _patch.stop()

    def test_a_looping_job_is_reported(self):
        """The baseline: the loop is detected and the kill is attempted."""
        kill = MagicMock()
        exit_code, diagnostics = self._run((
            patch.object(loopingjob, "create_core_dump"),
            patch.object(loopingjob, "kill_looping_job", kill),
        ))

        self.assertEqual(exit_code, errors.LOOPINGJOB)
        self.assertIn("looping", diagnostics)
        self.assertTrue(kill.called)

    def test_a_failing_core_dump_does_not_cancel_the_detection(self):
        """The core dump is a diagnostic; the kill and the error code are not."""
        kill = MagicMock()
        exit_code, _ = self._run((
            patch.object(loopingjob, "create_core_dump", side_effect=RuntimeError("gdb blew up")),
            patch.object(loopingjob, "kill_looping_job", kill),
        ))

        self.assertEqual(exit_code, errors.LOOPINGJOB)
        self.assertTrue(kill.called, msg="a failing core dump skipped the kill")

    def test_a_failing_kill_still_reports_the_looping_job(self):
        """The server must be told even if the local kill went wrong."""
        exit_code, diagnostics = self._run((
            patch.object(loopingjob, "create_core_dump"),
            patch.object(loopingjob, "kill_looping_job", side_effect=RuntimeError("kill failed")),
        ))

        self.assertEqual(exit_code, errors.LOOPINGJOB)
        self.assertIn("looping", diagnostics)

    def test_the_job_is_failed_before_the_diagnostics_run(self):
        """Slow or failing diagnostics must not leave the job unmarked."""
        job = FakeJob(workdir="/srv/nonexistent")
        with patch.object(loopingjob, "_dump_payload_stack_traces", side_effect=RuntimeError("no eu-stack")), \
             patch.object(loopingjob, "get_pilot_process_tree", side_effect=RuntimeError("no tree")), \
             patch.object(loopingjob, "get_child_processes", return_value=[]), \
             patch.object(loopingjob, "reap_zombies"):
            try:
                kill_looping_job(job)
            except RuntimeError:
                pass  # the failing diagnostic itself is not what is under test here

        self.assertIn(errors.LOOPINGJOB, job.piloterrorcodes)
        self.assertEqual(job.state, "failed")

    def test_the_child_processes_are_killed_despite_failing_diagnostics(self):
        """The kill is the point of the function and must not be skipped."""
        job = FakeJob(workdir="/srv/nonexistent")
        kill_process = MagicMock()
        with patch.object(loopingjob, "_dump_payload_stack_traces"), \
             patch.object(loopingjob, "get_pilot_process_tree", side_effect=RuntimeError("no tree")), \
             patch.object(loopingjob, "get_process_details", side_effect=RuntimeError("no details")), \
             patch.object(loopingjob, "get_child_processes", return_value=[(4242, "python athena.py")]), \
             patch.object(loopingjob, "kill_process", kill_process), \
             patch.object(loopingjob, "reap_zombies"):
            kill_looping_job(job)

        kill_process.assert_called_once_with(4242)


class TestLoopingCheckGate(unittest.TestCase):
    """The check requires the job and the experiment plugin to agree."""

    def setUp(self):
        """Set the experiment and a fake set of pilot arguments."""
        self.environ = patch.dict(os.environ, {"PILOT_USER": "atlas"})
        self.environ.start()
        self.args = MagicMock()

    def tearDown(self):
        """Restore the environment."""
        self.environ.stop()

    def _verify(self, looping_check=True, allowed=True):
        """Run verify_looping_job() and report whether the algorithm ran.

        Args:
            looping_check: Value of job.looping_check.
            allowed: What the experiment plugin allows.

        Returns:
            True if the looping job algorithm was called.
        """
        job = FakeJob()
        job.looping_check = looping_check
        algorithm = MagicMock(return_value=(0, ""))
        with patch("pilot.user.atlas.loopingjob_definitions.allow_loopingjob_detection",
                   return_value=allowed), \
             patch.object(monitoring, "get_time_since", return_value=10 * LOOPING_LIMIT), \
             patch.object(monitoring, "reap_zombies"), \
             patch.object(monitoring, "looping_job", algorithm):
            montime = MonitoringTime()
            montime.ct_looping = int(time.time()) - 10 * LOOPING_LIMIT
            exit_code, _ = monitoring.verify_looping_job(int(time.time()), montime, job, self.args)

        self.assertEqual(exit_code, 0)

        return algorithm.called

    def test_the_check_runs_when_both_agree(self):
        """The default for an ATLAS job."""
        self.assertTrue(self._verify(looping_check=True, allowed=True))

    def test_a_plugin_that_disallows_detection_is_respected(self):
        """'not a and b' let the plugin be overruled; both must now agree."""
        self.assertFalse(self._verify(looping_check=True, allowed=False))

    def test_a_job_that_disables_the_check_is_respected(self):
        """loopingCheck=False comes from the task definition and is not negotiable."""
        self.assertFalse(self._verify(looping_check=False, allowed=True))

    def test_neither_allows_it(self):
        """Both disabled is also not a reason to run it."""
        self.assertFalse(self._verify(looping_check=False, allowed=False))


if __name__ == "__main__":
    unittest.main()
