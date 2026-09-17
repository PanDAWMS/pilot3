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

"""Unit tests for the looping job process diagnostics.

The production failure being fixed: when the looping job algorithm decided that
a payload had stopped touching its files, the core dump was taken from
``get_subprocesses(job.pid)[-1]``, the last entry of a depth-first walk of the
descendant tree in ascending PID order. For an ATLAS job that tree also holds
the transform's own prmon instance, ALRB setup scripts, container and shell
wrappers, and xrootd helpers, so the core file was frequently taken from a
process that cannot explain a loop - and the 10 s timeout made that outcome
likely rather than merely possible, since only a small helper can write its
whole address space that fast.

Covers:
- the denylist: prmon and the other non-payload helpers are rejected, while a
  payload running through an interpreter ('python .../Sim_tf.py') is kept;
- candidate ranking: a plugin-declared payload name beats a higher-CPU
  anonymous process, and CPU time is the tie-breaker among the rest, since a
  looping payload spins whereas a hung one does not;
- the fallback to job.pid when every descendant is filtered out;
- the snapshot series: nothing recorded below the configured fraction of the
  looping limit, recorded above it, appended to a file in the work directory,
  and reset per job so a multijob pilot samples every job;
- the delta summary: CPU advancing is reported as a loop, CPU frozen as a hang;
- the core file analysis block: the executable, command line and release are
  recorded under a greppable marker together with the gdb invocation, so the
  core file can still be read after the worker node is gone;
- that the diagnostics never raise into the job monitoring loop.
"""

import logging
import os
import re
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

from pilot.common.errorcodes import ErrorCodes
from pilot.util import loopingdumps
from pilot.util.loopingdumps import (
    CLEAN_ENVIRONMENT,
    CORE_WRITTEN_MARKER,
    CORE_INFO_MARKER,
    CORE_INFO_SUFFIX,
    GDB_OUTPUT_SUFFIX,
    INVENTORY_MARKER,
    SNAPSHOT_FILENAME,
    build_gdb_invocation,
    build_phase_command,
    create_core_dump,
    format_snapshot,
    get_core_analysis_info,
    get_core_dump_max_size,
    get_expected_message_info,
    get_process_name,
    has_python_startup_failure,
    is_denylisted,
    is_looping_diagnostic_file,
    log_process_inventory,
    rank_candidate,
    reset_looping_dump_state,
    select_dump_candidates,
    STARTUP_MARKER,
    store_core_analysis_info,
    summarise_snapshots,
    take_looping_snapshot,
)

errors = ErrorCodes()

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class FakeJob:
    """Minimal stand-in for JobData carrying only what the diagnostics read."""

    def __init__(self, pid=1000, workdir="", jobid="6789012345"):
        """Initialise the fake job.

        Args:
            pid: Payload process id.
            workdir: Job work directory.
            jobid: PanDA job id.
        """
        self.pid = pid
        self.workdir = workdir
        self.jobid = jobid
        self.swrelease = "Athena-24.0.41"
        self.homepackage = "AthGeneration/24.0.41"
        self.platform = "x86_64-el9-gcc13-opt"
        self.transformation = "Generate_tf.py"
        self.imagename = ""


# the descendant tree of a looping ATLAS job, as reported by
# get_child_processes(): the transform, its athena child, the transform's own
# prmon, a container wrapper and an xrootd helper
ATLAS_TREE = [
    (1001, "/bin/bash -c export PandaID=6789012345; asetup Athena,24.0.41; Generate_tf.py --outputEVNTFile=x"),
    (1002, "python /cvmfs/atlas.cern.ch/repo/sw/software/24.0/AthGeneration/Generate_tf.py --outputEVNTFile=x"),
    (1003, "/usr/bin/python3 /cvmfs/atlas.cern.ch/repo/sw/software/24.0/bin/athena.py runargs.Generate.py"),
    (1004, "prmon --pid 1002 --filename prmon.txt --json-summary prmon.json --interval 60"),
    (1005, "/usr/bin/apptainer exec -B /cvmfs /srv/image.sif /srv/containerScript.sh"),
    (1006, "xrdcp root://eos.cern.ch//eos/atlas/file.root ."),
]


class TestDenylist(unittest.TestCase):
    """Non-payload helpers must never be selected as dump targets."""

    def test_prmon_is_rejected(self):
        """prmon, the process the core dump was actually being taken from, is rejected."""
        self.assertTrue(is_denylisted(
            "prmon --pid 1002 --filename prmon.txt --json-summary prmon.json --interval 60"
        ))

    def test_prmon_through_an_interpreter_is_rejected(self):
        """A prmon invoked through a wrapper is rejected on the full command line."""
        self.assertTrue(is_denylisted("/usr/bin/python3 /cvmfs/sw/prmon/prmon_wrapper.py --pid 1002"))

    def test_attested_helpers_are_rejected(self):
        """Only what the pilot demonstrably puts in the payload tree is dropped."""
        for cmdline in (
            "/bin/bash -c asetup Athena,24.0.41; Generate_tf.py",
            "/usr/bin/apptainer exec -B /cvmfs /srv/image.sif /srv/containerScript.sh",
            "/usr/bin/singularity exec /srv/image.sif /srv/containerScript.sh",
            "asetup Athena,24.0.41",
            "lsetup prmon",
        ):
            self.assertTrue(is_denylisted(cmdline), msg=cmdline)

    def test_unattested_processes_are_not_dropped_on_suspicion(self):
        """A process wrongly dropped vanishes silently; one wrongly kept is visible.

        These all looked like obvious helpers, but none of them is known to appear
        inside the payload tree, so the denylist must not remove them - the logged
        inventory is what should settle whether they belong there.
        """
        for cmdline in (
            "xrdcp root://eos.cern.ch//eos/atlas/file.root .",
            "nvidia-smi --query-compute-apps=pid --format=csv",
            "ps axo pid,ppid,args",
            "tar czf payload.tgz outputs/",
            "find /srv/workdir -mmin -120",
        ):
            self.assertFalse(is_denylisted(cmdline), msg=cmdline)

    def test_empty_cmdline_is_rejected(self):
        """A process with no readable command line is a kernel thread or gone."""
        self.assertTrue(is_denylisted(""))

    def test_payload_through_an_interpreter_is_kept(self):
        """The ATLAS payload runs as 'python .../Sim_tf.py', so interpreters are kept."""
        for cmdline in (
            "python /cvmfs/atlas.cern.ch/repo/sw/software/24.0/AthGeneration/Generate_tf.py --x=1",
            "/usr/bin/python3 /cvmfs/atlas.cern.ch/repo/sw/software/24.0/bin/athena.py runargs.py",
        ):
            self.assertFalse(is_denylisted(cmdline), msg=cmdline)


class TestCandidateSelection(unittest.TestCase):
    """Selection must produce the payload, not an arbitrary tree entry."""

    def setUp(self):
        """Reset the snapshot bookkeeping (module level singleton state)."""
        reset_looping_dump_state()

    def tearDown(self):
        """Reset the snapshot bookkeeping (module level singleton state)."""
        reset_looping_dump_state()

    @staticmethod
    def _cpu_time(pid):
        """Return a fake CPU time: the xrootd helper burns the most CPU.

        Args:
            pid (int): Process id.

        Returns:
            float: CPU time in seconds.
        """
        return {1001: 0.5, 1002: 12.0, 1003: 900.0, 1004: 3.0, 1005: 0.2, 1006: 4000.0}.get(pid, 0.0)

    def test_prmon_is_never_the_target(self):
        """The regression itself: prmon must not be selected as the dump target."""
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             patch.object(loopingdumps, "get_payload_process_names", return_value=["athena.py", "_tf.py"]), \
             patch.object(loopingdumps, "get_cpu_time", side_effect=self._cpu_time), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"):
            candidates = select_dump_candidates(FakeJob())

        self.assertTrue(candidates)
        selected_pids = [pid for pid, _ in candidates]
        self.assertNotIn(1004, selected_pids)
        for _, cmdline in candidates:
            self.assertNotIn("prmon", cmdline)

    def test_last_tree_entry_is_not_selected(self):
        """The old code took the last entry (the xrootd helper here) - it must not win."""
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             patch.object(loopingdumps, "get_payload_process_names", return_value=["athena.py", "_tf.py"]), \
             patch.object(loopingdumps, "get_cpu_time", side_effect=self._cpu_time), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"):
            candidates = select_dump_candidates(FakeJob())

        self.assertNotEqual(candidates[0][0], ATLAS_TREE[-1][0])
        self.assertEqual(candidates[0][0], 1003)  # athena.py, the highest CPU name match

    def test_reported_scenario_prmon_is_the_last_tree_entry(self):
        """The reported failure: prmon was the last entry, so the old code dumped it."""
        tree = [
            (1001, "/bin/bash -c export PandaID=6789012345; asetup Athena,24.0.41; Generate_tf.py"),
            (1002, "python /cvmfs/sw/AthGeneration/Generate_tf.py --outputEVNTFile=x"),
            (1003, "/usr/bin/python3 /cvmfs/sw/bin/athena.py runargs.Generate.py"),
            (1004, "prmon --pid 1002 --filename prmon.txt --json-summary prmon.json"),
        ]
        self.assertIn("prmon", tree[-1][1])  # what the old '[-1]' selection would have picked

        with patch.object(loopingdumps, "get_descendants", return_value=tree), \
             patch.object(loopingdumps, "get_payload_process_names", return_value=["athena.py", "_tf.py"]), \
             patch.object(loopingdumps, "get_cpu_time", side_effect=self._cpu_time), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"):
            candidates = select_dump_candidates(FakeJob())

        self.assertEqual(candidates[0][0], 1003)
        self.assertNotIn(1004, [pid for pid, _ in candidates])

    def test_name_match_beats_higher_cpu_time(self):
        """A plugin declared payload name wins over an anonymous higher-CPU process."""
        tree = [
            (2001, "/cvmfs/sw/bin/somehelper --spin"),
            (2002, "/usr/bin/python3 /cvmfs/sw/bin/athena.py runargs.py"),
        ]
        with patch.object(loopingdumps, "get_descendants", return_value=tree), \
             patch.object(loopingdumps, "get_payload_process_names", return_value=["athena.py"]), \
             patch.object(loopingdumps, "get_cpu_time", side_effect=lambda pid: 9999.0 if pid == 2001 else 1.0), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"):
            candidates = select_dump_candidates(FakeJob())

        self.assertEqual(candidates[0][0], 2002)

    def test_cpu_time_breaks_the_tie(self):
        """Without a name match, the spinning process is preferred over the idle one."""
        tree = [
            (3001, "/cvmfs/sw/bin/workerA"),
            (3002, "/cvmfs/sw/bin/workerB"),
        ]
        with patch.object(loopingdumps, "get_descendants", return_value=tree), \
             patch.object(loopingdumps, "get_payload_process_names", return_value=[]), \
             patch.object(loopingdumps, "get_cpu_time", side_effect=lambda pid: 500.0 if pid == 3002 else 1.0), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"):
            candidates = select_dump_candidates(FakeJob())

        self.assertEqual(candidates[0][0], 3002)

    def test_fallback_to_the_payload_process(self):
        """When every descendant is a helper, the payload process itself is used."""
        tree = [
            (4001, "prmon --pid 4000 --filename prmon.txt"),
            (4002, "/bin/bash -c asetup; sleep 1"),
        ]
        with patch.object(loopingdumps, "get_descendants", return_value=tree), \
             patch.object(loopingdumps, "get_payload_process_names", return_value=[]), \
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c payload"), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_cpu_time", return_value=0.0), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"):
            candidates = select_dump_candidates(FakeJob(pid=4000))

        self.assertEqual([pid for pid, _ in candidates], [4000])

    def test_no_pid_returns_nothing(self):
        """No payload pid means there is nothing to dump."""
        self.assertEqual(select_dump_candidates(FakeJob(pid=None)), [])

    def test_rank_is_ordered_by_name_then_cpu(self):
        """The sort key orders on the name match first and the CPU time second."""
        with patch.object(loopingdumps, "get_cpu_time", return_value=10.0), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"):
            matched = rank_candidate("python athena.py", 1, ["athena.py"])
            unmatched = rank_candidate("python other.py", 2, ["athena.py"])

        self.assertGreater(matched, unmatched)


class TestSnapshotSeries(unittest.TestCase):
    """The snapshot series is what actually diagnoses a loop."""

    def setUp(self):
        """Reset the module level snapshot bookkeeping."""
        reset_looping_dump_state()

    def tearDown(self):
        """Reset the module level snapshot bookkeeping."""
        reset_looping_dump_state()

    @staticmethod
    def _patches():
        """Return the patch context managers used by the snapshot tests.

        Returns:
            Tuple of context managers.
        """
        return (
            patch.object(loopingdumps, "get_descendants", return_value=[
                (5001, "/usr/bin/python3 /cvmfs/sw/bin/athena.py runargs.py")
            ]),
            patch.object(loopingdumps, "get_payload_process_names", return_value=["athena.py"]),
            patch.object(loopingdumps, "get_cpu_time", return_value=100.0),
            patch.object(loopingdumps, "get_rss", return_value=1024 * 1024),
            patch.object(loopingdumps, "get_stack_tool", return_value=""),
            patch.object(loopingdumps, "get_stack_trace", return_value="frame0\nframe1"),
        )

    def test_nothing_recorded_below_the_threshold(self):
        """A healthy job must not pay for the diagnostics."""
        patches = self._patches()
        for _patch in patches:
            _patch.start()
        try:
            take_looping_snapshot(FakeJob(), since_touch=100, looping_limit=7200)
        finally:
            for _patch in patches:
                _patch.stop()

        self.assertEqual(loopingdumps._snapshot_state["snapshots"], [])

    def test_recorded_above_the_threshold(self):
        """Past half the looping limit the snapshots start."""
        patches = self._patches()
        for _patch in patches:
            _patch.start()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                job = FakeJob(workdir=workdir)
                take_looping_snapshot(job, since_touch=4000, looping_limit=7200)
                self.assertEqual(len(loopingdumps._snapshot_state["snapshots"]), 1)
                self.assertTrue(os.path.exists(os.path.join(workdir, SNAPSHOT_FILENAME)))
        finally:
            for _patch in patches:
                _patch.stop()

    def test_series_is_appended_not_overwritten(self):
        """Consecutive snapshots must accumulate - the deltas are the point."""
        patches = self._patches()
        for _patch in patches:
            _patch.start()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                job = FakeJob(workdir=workdir)
                for since in (4000, 5000, 6000):
                    take_looping_snapshot(job, since_touch=since, looping_limit=7200)
                self.assertEqual(len(loopingdumps._snapshot_state["snapshots"]), 3)
                with open(os.path.join(workdir, SNAPSHOT_FILENAME), encoding="utf-8") as _file:
                    contents = _file.read()
                for index in (1, 2, 3):
                    self.assertIn(f"snapshot #{index}", contents)
        finally:
            for _patch in patches:
                _patch.stop()

    def test_series_is_reset_per_job(self):
        """A multijob pilot must not carry the first job's series into the second."""
        patches = self._patches()
        for _patch in patches:
            _patch.start()
        try:
            with tempfile.TemporaryDirectory() as workdir:
                take_looping_snapshot(FakeJob(workdir=workdir, jobid="111"), 4000, 7200)
                take_looping_snapshot(FakeJob(workdir=workdir, jobid="222"), 4000, 7200)
                self.assertEqual(len(loopingdumps._snapshot_state["snapshots"]), 1)
                self.assertEqual(loopingdumps._snapshot_state["jobid"], "222")
        finally:
            for _patch in patches:
                _patch.stop()

    def test_a_failure_never_propagates(self):
        """A diagnostic must not be able to break the job monitoring loop."""
        with patch.object(loopingdumps, "select_dump_candidates", side_effect=RuntimeError("boom")):
            take_looping_snapshot(FakeJob(), since_touch=4000, looping_limit=7200)  # must not raise


class TestSnapshotSummary(unittest.TestCase):
    """The deltas separate a genuine loop from a hang."""

    def setUp(self):
        """Reset the module level snapshot bookkeeping."""
        reset_looping_dump_state()

    def tearDown(self):
        """Reset the module level snapshot bookkeeping."""
        reset_looping_dump_state()

    @staticmethod
    def _snapshot(index, stamp, cpu_time, rss, backtrace):
        """Return a minimal snapshot dictionary.

        Args:
            index (int): Snapshot index.
            stamp (int): Snapshot time.
            cpu_time (float): Accumulated CPU time.
            rss (int): Resident set size in bytes.
            backtrace (str): Backtrace text.

        Returns:
            dict: Snapshot dictionary.
        """
        return {
            "index": index,
            "time": stamp,
            "since_touch": 4000,
            "processes": [{
                "pid": 5001,
                "cmdline": "python athena.py",
                "state": "R",
                "cpu_time": cpu_time,
                "rss": rss,
                "threads": "8",
                "wchan": "0",
                "syscall": "running",
                "cwd": "/srv/workdir",
                "exe": "/usr/bin/python3",
                "backtrace": backtrace,
            }],
        }

    def test_no_snapshots(self):
        """With nothing recorded the summary says so rather than inventing a verdict."""
        self.assertIn("no looping snapshots", summarise_snapshots())

    def test_single_snapshot_has_no_deltas(self):
        """One snapshot cannot produce a delta."""
        loopingdumps._snapshot_state["snapshots"] = [self._snapshot(1, 1000, 100.0, 0, "f0")]
        self.assertIn("only one looping snapshot", summarise_snapshots())

    def test_advancing_cpu_is_reported_as_a_loop(self):
        """CPU time advancing over the interval means the payload is spinning."""
        loopingdumps._snapshot_state["snapshots"] = [
            self._snapshot(1, 1000, 100.0, 1024, "f0"),
            self._snapshot(2, 2000, 1000.0, 1024, "f0"),
        ]
        summary = summarise_snapshots()
        self.assertIn("spinning", summary)
        self.assertIn("stack unchanged", summary)

    def test_frozen_cpu_is_reported_as_a_hang(self):
        """CPU time frozen over the interval means a hang, not a loop."""
        loopingdumps._snapshot_state["snapshots"] = [
            self._snapshot(1, 1000, 100.0, 1024, "f0"),
            self._snapshot(2, 2000, 100.0, 1024, "f0"),
        ]
        self.assertIn("hang, not a loop", summarise_snapshots())

    def test_snapshot_formatting_is_complete(self):
        """Every collected field reaches the snapshot file."""
        text = format_snapshot(self._snapshot(1, 1000, 100.0, 2 * 1024 * 1024, "frame0\nframe1"))
        for fragment in ("snapshot #1", "pid=5001", "cpu_time=100.0s", "rss=2MB",
                         "syscall: running", "frame0", "frame1"):
            self.assertIn(fragment, text)


class TestProcessInventory(unittest.TestCase):
    """The inventory is what a payload name list should later be derived from."""

    _PPID = {1001: 1000, 1002: 1001, 1003: 1002, 1004: 1002, 1005: 1001, 1006: 1003}

    def test_inventory_includes_the_dropped_processes(self):
        """Nothing is filtered out - the denylist itself has to be checkable."""
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             patch.object(loopingdumps, "get_ppid", side_effect=lambda pid: self._PPID.get(pid, 0)), \
             patch.object(loopingdumps, "get_cpu_time", return_value=1.0), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="S"), \
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c payload"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            returned = log_process_inventory(FakeJob(), label="at kill time")

        text = "\n".join(captured.output)
        self.assertEqual(text.count(INVENTORY_MARKER), 2)  # opens and closes the block
        self.assertIn("at kill time", text)
        for pid, _ in ATLAS_TREE:
            self.assertIn(str(pid), text)
        self.assertIn("prmon", text)  # present, and flagged as dropped
        self.assertEqual(returned, ATLAS_TREE)

    def test_inventory_records_names_depth_and_drop_status(self):
        """The columns a name list is built from are all present."""
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             patch.object(loopingdumps, "get_ppid", side_effect=lambda pid: self._PPID.get(pid, 0)), \
             patch.object(loopingdumps, "get_cpu_time", return_value=900.0), \
             patch.object(loopingdumps, "get_rss", return_value=3800 * 1024 * 1024), \
             patch.object(loopingdumps, "get_process_state", return_value="R"), \
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c payload"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            log_process_inventory(FakeJob())

        text = "\n".join(captured.output)
        for fragment in ("depth", "ppid", "name", "cpu_s", "rss_MB", "drop",
                         "athena.py", "Generate_tf.py", "3800"):
            self.assertIn(fragment, text)

    def test_empty_tree_is_reported_not_hidden(self):
        """An empty tree is itself a finding and must be visible in the log."""
        with patch.object(loopingdumps, "get_descendants", return_value=[]), \
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c payload"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            log_process_inventory(FakeJob())

        self.assertIn("no descendants found", "\n".join(captured.output))


class TestProcessName(unittest.TestCase):
    """Name extraction feeds the inventory column a name list is harvested from."""

    def test_interpreted_payload_reports_the_script(self):
        """The interpreter is not the interesting name for an ATLAS payload."""
        self.assertEqual(
            get_process_name("python /cvmfs/sw/24.0/AthGeneration/Generate_tf.py --outputEVNTFile=x"),
            "Generate_tf.py"
        )
        self.assertEqual(
            get_process_name("/usr/bin/python3 /cvmfs/sw/bin/athena.py runargs.Generate.py"),
            "athena.py"
        )

    def test_shells_are_not_unwrapped(self):
        """The first word of a 'bash -c' string is setup noise, not the payload."""
        self.assertEqual(
            get_process_name("/bin/bash -c export PandaID=1; asetup Athena; Generate_tf.py"),
            "bash"
        )

    def test_plain_binary_reports_its_basename(self):
        """A non-interpreted process reports argv[0]."""
        self.assertEqual(get_process_name("prmon --pid 1002 --filename prmon.txt"), "prmon")
        self.assertEqual(get_process_name("xrdcp root://eos//f.root ."), "xrdcp")

    def test_unreadable_cmdline(self):
        """An unreadable command line yields no name rather than raising."""
        self.assertEqual(get_process_name(""), "")


class TestNameMatchingIsInert(unittest.TestCase):
    """No experiment declares payload names yet - that is deliberate."""

    def test_no_plugin_declares_names(self):
        """A guessed name would promote the wrong process; an empty list cannot."""
        for experiment in ("atlas", "generic", "epic", "sphenix", "darkside", "rubin", "ska"):
            with patch.dict(os.environ, {"PILOT_USER": experiment}):
                self.assertEqual(loopingdumps.get_payload_process_names(), [],
                                 msg=f"{experiment} declares payload names")

    def test_every_plugin_defines_the_hook(self):
        """The hook is part of the plugin interface, not an optional extra."""
        for experiment in ("atlas", "generic", "epic", "sphenix", "darkside", "rubin", "ska"):
            module = __import__(f"pilot.user.{experiment}.loopingjob_definitions",
                                globals(), locals(), [experiment], 0)
            self.assertTrue(callable(getattr(module, "get_payload_process_names", None)),
                            msg=f"{experiment} does not define get_payload_process_names()")

    def test_ranking_falls_back_to_cpu_time(self):
        """With no names declared, the spinning process wins on CPU time alone."""
        tree = [
            (7001, "/cvmfs/sw/bin/workerA"),
            (7002, "/cvmfs/sw/bin/workerB"),
        ]
        with patch.dict(os.environ, {"PILOT_USER": "atlas"}), \
             patch.object(loopingdumps, "get_descendants", return_value=tree), \
             patch.object(loopingdumps, "get_ppid", return_value=0), \
             patch.object(loopingdumps, "get_process_state", return_value="R"), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_cpu_time",
                          side_effect=lambda pid: 800.0 if pid == 7002 else 2.0):
            candidates = select_dump_candidates(FakeJob())

        self.assertEqual(candidates[0][0], 7002)


class TestCoreAnalysisInfo(unittest.TestCase):
    """A core file is useless without knowing which binary produced it."""

    def test_block_records_the_binary_and_the_gdb_invocation(self):
        """The executable, command line, release and gdb command are all recorded."""
        with patch.object(loopingdumps, "read_proc_link",
                          side_effect=lambda pid, name: {
                              "exe": "/cvmfs/atlas.cern.ch/repo/sw/software/24.0/bin/python",
                              "cwd": "/srv/workdir",
                          }.get(name, "")), \
             patch.object(loopingdumps, "get_rss", return_value=4096 * 1024 * 1024), \
             patch.object(loopingdumps, "get_shared_libraries",
                          return_value=["/cvmfs/atlas.cern.ch/repo/sw/software/24.0/lib/libAthenaKernel.so"]):
            info = get_core_analysis_info(
                FakeJob(), 1003, "/usr/bin/python3 /cvmfs/sw/bin/athena.py runargs.py",
                "/srv/workdir/core.1003"
            )

        self.assertEqual(info.count(CORE_INFO_MARKER), 2)  # opens and closes the block
        self.assertIn("core.1003", info)
        self.assertIn("pid: 1003", info)
        self.assertIn("/cvmfs/atlas.cern.ch/repo/sw/software/24.0/bin/python", info)
        self.assertIn("athena.py", info)
        self.assertIn("swRelease: Athena-24.0.41", info)
        self.assertIn("homePackage: AthGeneration/24.0.41", info)
        self.assertIn("gdb /cvmfs/atlas.cern.ch/repo/sw/software/24.0/bin/python core.1003", info)
        self.assertIn("libAthenaKernel.so", info)

    def test_no_gdb_line_when_no_core_file_was_written(self):
        """Without a core file the identity is still recorded, but not a bogus gdb line."""
        with patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/python"), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]):
            info = get_core_analysis_info(FakeJob(), 1003, "python athena.py",
                                          "/srv/workdir/core.1003", with_core=False)

        self.assertIn("backtraces only", info)
        self.assertNotIn("gdb /cvmfs/sw/bin/python core.1003", info)

    def test_companion_file_is_written_next_to_the_core_file(self):
        """The information travels in the log tarball, not only in the pilot log."""
        with tempfile.TemporaryDirectory() as workdir:
            core_path = os.path.join(workdir, "core.1003")
            with patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/python"), \
                 patch.object(loopingdumps, "get_rss", return_value=0), \
                 patch.object(loopingdumps, "get_shared_libraries", return_value=[]):
                store_core_analysis_info(FakeJob(workdir=workdir), 1003, "python athena.py", core_path)

            path = f"{core_path}{CORE_INFO_SUFFIX}"
            self.assertTrue(os.path.exists(path))
            with open(path, encoding="utf-8") as _file:
                self.assertIn(CORE_INFO_MARKER, _file.read())


class GdbStub:
    """Stand-in for execute() that behaves like a gdb writing to its output file.

    The production defect being covered is that
    :func:`pilot.util.container.execute` discards stdout when a command times
    out, so a stub that returns its output through the return value cannot
    reproduce the failure. This one writes to the redirect target parsed out of
    the command, exactly as the real shell redirection does, which is what makes
    a timeout test meaningful.
    """

    REDIRECT = re.compile(r'>> "([^"]+)" 2>&1')

    def __init__(self, responses):
        """Initialise the stub.

        Args:
            responses (list): One ``(exit_code, output, core_file)`` tuple per
                expected call; *core_file* is a path to create, or ``None``.
        """
        self.responses = list(responses)
        self.calls = []

    def __call__(self, command, **kwargs):
        """Record the call, write the output and return the canned result.

        Args:
            command (str): Command that would have been executed.
            **kwargs: Keyword arguments passed to execute().

        Returns:
            tuple: (exit_code, stdout, stderr).
        """
        self.calls.append((command, kwargs))
        exit_code, output, core_file = self.responses.pop(0)

        match = self.REDIRECT.search(command)
        if match and output:
            with open(match.group(1), "a", encoding="utf-8") as _file:
                _file.write(output + "\n")
        if core_file:
            with open(core_file, "wb") as _file:
                _file.write(b"\x7fELF" + b"\x00" * 1024)

        stderr = "subprocess communicate sent TimeoutExpired" if exit_code == errors.COMMANDTIMEDOUT else ""

        return exit_code, "", stderr


# the exact signature seen in production: gdb's own embedded interpreter failing
# during Py_Initialize() because the release setup exported a PYTHONHOME that
# does not match the Python gdb is linked against. gdb exits 1 before running a
# single -ex command, so neither the core file nor the backtraces are produced
ENCODINGS_FAILURE = (
    "Fatal Python error: init_fs_encoding: failed to get the Python codec of the filesystem encoding\n"
    "Python runtime state: core initialized\n"
    "ModuleNotFoundError: No module named 'encodings'\n"
    "Current thread 0x00007fb9e01810c0 (most recent call first):\n"
    "<no Python frame>"
)

# what gdb had printed when the 300 s timeout struck in production: the phase
# had started and produced output, all of which execute() then discarded
PARTIAL_BACKTRACE = (
    "Thread 1 (Thread 0x2b0e7c0d5f40 (LWP 28179)):\n"
    "#0  0x00002b0e7befb740 in __read_nocancel () from /lib64/libpthread.so.0\n"
    "#1  0x00002b0e7bafcd56 in _Py_read () from libpython3.9.so.1.0"
)


class TestDumpPhases(unittest.TestCase):
    """The two dump phases, and the two ways they failed in production."""

    def setUp(self):
        """Create a work directory and reset the snapshot bookkeeping."""
        reset_looping_dump_state()
        self._tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = self._tmp.name

    def tearDown(self):
        """Remove the work directory and reset the snapshot bookkeeping."""
        self._tmp.cleanup()
        reset_looping_dump_state()

    def _run(self, responses, rss=100 * 1024 * 1024):
        """Run create_core_dump() against the stub and return it with the log.

        Args:
            responses (list): Canned responses for the stub.
            rss (int): Resident set size reported for the dump candidate.

        Returns:
            tuple: (GdbStub, captured log text, core file path).
        """
        job = FakeJob(pid=1000, workdir=self.workdir)
        core_path = os.path.join(self.workdir, "core.1003")
        stub = GdbStub([
            (code, output, core_path if core else None) for code, output, core in responses
        ])
        with patch.object(loopingdumps, "execute", stub), \
             patch.object(loopingdumps, "select_dump_candidates",
                          return_value=[(1003, "python athena.py runargs.py")]), \
             patch.object(loopingdumps, "get_rss", return_value=rss), \
             patch.object(loopingdumps, "has_room_for_core", return_value=True), \
             patch.object(loopingdumps, "get_gdb_setup", return_value="asetup AthGeneration,23.6.11; "), \
             patch.object(loopingdumps, "get_payload_container_image", return_value=""), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
             patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/python"), \
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c source atlasLocalSetup.sh -c x86_64"), \
             patch.object(loopingdumps, "resume_process"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            create_core_dump(job)

        return stub, "\n".join(captured.output), core_path

    def test_a_timeout_says_how_far_gdb_got(self):
        """The diagnosis has to be wired into the timeout path, not just exist.

        Output with no startup marker means gdb never reached the commands,
        which is the signature of it still reading symbols.
        """
        _, log, _ = self._run([
            (errors.COMMANDTIMEDOUT, "Attaching to process 1003", False),
            (0, PARTIAL_BACKTRACE, False),
        ])

        self.assertIn("never reached the requested commands", log)

    def test_the_core_phase_asks_gdb_to_mark_completion(self):
        """Otherwise a truncated core file is indistinguishable from a complete one."""
        stub, _, _ = self._run([(0, "Saved corefile", True), (0, PARTIAL_BACKTRACE, False)])

        self.assertIn(CORE_WRITTEN_MARKER, stub.calls[0][0])
        self.assertIn(STARTUP_MARKER, stub.calls[0][0])

    def test_core_file_is_written_before_the_backtraces(self):
        """The cheap artifact must not be lost to the expensive one.

        generate-core-file needs no symbols; the backtraces need the symbol
        table of every mapped object. Running them the other way round is what
        cost the core file in production.
        """
        stub, _, core_path = self._run([(0, "Saved corefile", True), (0, PARTIAL_BACKTRACE, False)])

        self.assertEqual(len(stub.calls), 2)
        self.assertIn("generate-core-file", stub.calls[0][0])
        self.assertNotIn("thread apply all bt", stub.calls[0][0])
        self.assertIn("thread apply all bt", stub.calls[1][0])
        self.assertTrue(os.path.exists(core_path))

    def test_core_phase_runs_without_the_release_setup(self):
        """Phase A pays neither the asetup cost nor its environment failure mode."""
        stub, _, _ = self._run([(0, "Saved corefile", True), (0, PARTIAL_BACKTRACE, False)])

        self.assertNotIn("asetup", stub.calls[0][0])
        self.assertIn("asetup", stub.calls[1][0])

    def test_timeout_keeps_the_output_produced_so_far(self):
        """A timed out phase must not lose what gdb had already printed.

        execute() returns an empty stdout on timeout, so the output has to come
        from the file. Before this fix the pilot logged that the backtraces had
        been captured while having captured nothing at all.
        """
        _, log, _ = self._run([
            (0, "Saved corefile", True),
            (errors.COMMANDTIMEDOUT, PARTIAL_BACKTRACE, False),
        ])

        path = os.path.join(self.workdir, f"core.1003{GDB_OUTPUT_SUFFIX}")
        with open(path, encoding="utf-8") as _file:
            contents = _file.read()

        self.assertIn("__read_nocancel", contents)
        self.assertIn("__read_nocancel", log)  # and it reached the pilot log
        self.assertIn("timed out", log)

    def test_encodings_failure_is_identified_and_retried(self):
        """gdb's own interpreter failing must be named as such, not as a payload error.

        Production signature: 'ModuleNotFoundError: No module named encodings'
        with exit code 1. It refers to gdb's embedded Python, not the payload's,
        and it aborts gdb before any -ex command runs.
        """
        stub, log, core_path = self._run([
            (1, ENCODINGS_FAILURE, False),      # phase A, poisoned environment
            (0, "Saved corefile", True),        # phase A retry, clean environment
            (0, PARTIAL_BACKTRACE, False),      # phase B
        ])

        self.assertEqual(len(stub.calls), 3)
        self.assertNotIn(CLEAN_ENVIRONMENT, stub.calls[0][0])
        self.assertIn(CLEAN_ENVIRONMENT, stub.calls[1][0])
        self.assertIn("embedded interpreter", log)
        self.assertIn("not to the payload", log)
        self.assertTrue(os.path.exists(core_path))

    def test_backtrace_phase_retries_without_the_release_setup(self):
        """When the setup is what breaks gdb, the backtraces are still worth trying."""
        stub, log, _ = self._run([
            (0, "Saved corefile", True),
            (1, ENCODINGS_FAILURE, False),      # phase B with the setup
            (0, PARTIAL_BACKTRACE, False),      # phase B retry without it
        ])

        self.assertEqual(len(stub.calls), 3)
        self.assertIn("asetup", stub.calls[1][0])
        self.assertNotIn("asetup", stub.calls[2][0])
        self.assertIn("retrying without the release setup", log)

    def test_every_phase_strips_the_python_environment(self):
        """The variables that break gdb are unset in every invocation.

        The pilot itself runs under an ALRB Python, so the poisoned environment
        can reach gdb even when no release setup is prepended.
        """
        stub, _, _ = self._run([(0, "Saved corefile", True), (0, PARTIAL_BACKTRACE, False)])

        for command, _ in stub.calls:
            self.assertIn("unset PYTHONHOME PYTHONPATH", command)

    def test_execute_is_muted_so_the_pid_is_not_redacted(self):
        """print_executable() redacts the value after '-p', i.e. the pid itself.

        It matches '-p <token>' as a credential and then replaces every
        occurrence of that string, so the log showed 'gdb -p ********' and
        'core.********' and the dump could not be tied to a process.
        """
        stub, _, _ = self._run([(0, "Saved corefile", True), (0, PARTIAL_BACKTRACE, False)])

        for _, kwargs in stub.calls:
            self.assertTrue(kwargs.get("mute"), msg="execute() must be muted or the pid is redacted")

        # phase A attaches, so it carries the option that triggers the redaction
        self.assertIn("-p 1003", stub.calls[0][0])

    def test_gdb_runs_outside_the_job_work_directory(self):
        """asetup writes .asetup.save into its working directory.

        The looping algorithm measures that directory for payload activity, so a
        diagnostic writing there looks exactly like the payload doing work.
        """
        stub, _, _ = self._run([(0, "Saved corefile", True), (0, PARTIAL_BACKTRACE, False)])

        for _, kwargs in stub.calls:
            cwd = kwargs.get("cwd")
            self.assertTrue(cwd, msg="no working directory was given to execute()")
            self.assertFalse(os.path.abspath(cwd).startswith(os.path.abspath(self.workdir)))

    def test_gdb_never_reaches_the_network_or_a_user_gdbinit(self):
        """debuginfod has no route from a worker node and would stall the phase."""
        invocation = build_gdb_invocation(1003, ["-ex bt"])

        self.assertIn("--nx", invocation)
        self.assertIn("set debuginfod enabled off", invocation)
        self.assertIn("set index-cache enabled off", invocation)

    def test_missing_core_file_is_reported(self):
        """A phase that reports success without producing a core file is still a failure."""
        _, log, _ = self._run([(0, "", False), (0, PARTIAL_BACKTRACE, False)])

        self.assertIn("no core file was produced", log)

    def test_oversized_payload_keeps_the_backtraces_only(self):
        """The core file travels in the log tarball, so its size bounds the log file."""
        stub, log, _ = self._run([(0, PARTIAL_BACKTRACE, False)], rss=8 * 1024 * 1024 * 1024)

        self.assertEqual(len(stub.calls), 1)  # phase B only
        self.assertNotIn("generate-core-file", stub.calls[0][0])
        self.assertIn("exceeds the configured maximum", log)

    def test_budget_is_bounded(self):
        """The default core dump limit is what bounds the looping job log file size."""
        self.assertEqual(get_core_dump_max_size(), 2 * 1024 * 1024 * 1024)
        # and the fallback used when the setting is absent from the configuration
        self.assertEqual(loopingdumps.DEFAULT_CORE_DUMP_MAX_SIZE, "2 GB")
        self.assertGreater(loopingdumps.get_diagnostics_budget(), 0)
        self.assertEqual(loopingdumps.get_remaining_budget(0.0), 0)

    def test_phase_is_skipped_when_the_budget_is_spent(self):
        """Everything here runs between the decision to kill and the kill itself."""
        job = FakeJob(pid=1000, workdir=self.workdir)
        stub = GdbStub([])
        with patch.object(loopingdumps, "execute", stub), \
             patch.object(loopingdumps, "select_dump_candidates",
                          return_value=[(1003, "python athena.py")]), \
             patch.object(loopingdumps, "get_rss", return_value=1024), \
             patch.object(loopingdumps, "has_room_for_core", return_value=True), \
             patch.object(loopingdumps, "get_gdb_setup", return_value=""), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
             patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/python"), \
             patch.object(loopingdumps, "get_cmdline", return_value="bash -c payload"), \
             patch.object(loopingdumps, "get_diagnostics_budget", return_value=0), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            create_core_dump(job)

        self.assertEqual(stub.calls, [])
        self.assertIn("budget is spent", "\n".join(captured.output))


class TestPythonFailureDetection(unittest.TestCase):
    """The signature has to be recognised wherever it appears."""

    def test_both_fragments_are_recognised(self):
        """Either half of the production message identifies the failure."""
        self.assertTrue(has_python_startup_failure(ENCODINGS_FAILURE))
        self.assertTrue(has_python_startup_failure("init_fs_encoding blah"))
        self.assertTrue(has_python_startup_failure("ModuleNotFoundError: No module named 'encodings'"))

    def test_ordinary_output_is_not_mistaken_for_it(self):
        """A normal backtrace must not trigger a retry."""
        self.assertFalse(has_python_startup_failure(PARTIAL_BACKTRACE))
        self.assertFalse(has_python_startup_failure(""))


class TestDiagnosticFileFiltering(unittest.TestCase):
    """The diagnostics must not look like payload activity - nor mask it."""

    def test_gdb_output_file_is_recognised(self):
        """The phases write it into the directory the looping algorithm measures."""
        self.assertTrue(is_looping_diagnostic_file(f"/srv/workdir/core.1003{GDB_OUTPUT_SUFFIX}"))

    def test_the_existing_artifacts_are_still_recognised(self):
        """The snapshot series, the core files and the analysis companions."""
        self.assertTrue(is_looping_diagnostic_file(f"/srv/workdir/{SNAPSHOT_FILENAME}"))
        self.assertTrue(is_looping_diagnostic_file("/srv/workdir/core.1003"))
        self.assertTrue(is_looping_diagnostic_file(f"/srv/workdir/core.1003{CORE_INFO_SUFFIX}"))

    def test_asetup_save_is_not_filtered(self):
        """Filtering it by name would hide real activity from a multi-step transform.

        The setup is kept out of the job work directory instead, by running the
        gdb phases elsewhere.
        """
        self.assertFalse(is_looping_diagnostic_file("/srv/workdir/.asetup.save"))

    def test_payload_files_are_not_filtered(self):
        """Only the pilot's own output is dropped from the activity measurement."""
        for name in ("log.generate", "payload.stdout", "sherpa.log", "core_dump_analysis.py"):
            self.assertFalse(is_looping_diagnostic_file(f"/srv/workdir/{name}"), msg=name)


class TestContainerAnalysisInfo(unittest.TestCase):
    """A core file has to be read in the environment that produced it."""

    SIF = "/cvmfs/atlas.cern.ch/repo/containers/images/apptainer/x86_64-el9.img"

    def setUp(self):
        """Create a directory standing in for an unpacked container image."""
        self._tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.image = self._tmp.name

    def tearDown(self):
        """Remove it."""
        self._tmp.cleanup()

    def _analysis(self, image):
        """Build the analysis block for a payload running in the given image.

        Args:
            image (str): Container image path, or "" for no container.

        Returns:
            str: Analysis block.
        """
        with patch.object(loopingdumps, "read_proc_link",
                          side_effect=lambda pid, name: {"exe": "/usr/bin/python3.9",
                                                         "cwd": "/srv/workDir"}.get(name, "")), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_cmdline", return_value="apptainer exec ..."), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
             patch.object(loopingdumps, "get_payload_container_image", return_value=image):
            return loopingdumps.get_core_analysis_info(
                FakeJob(), 1003, "python x.py", "/srv/core.1003", setup="")

    def test_the_quoted_recipe_carries_the_sysroot(self):
        """The reader must not be handed the recipe that produces '?? ()'.

        A core file read without a sysroot gives unnamed frames and a backtrace
        truncated at the first of them, which is the output this whole change
        exists to stop producing.
        """
        info = self._analysis(self.image)

        self.assertIn(f"gdb -iex 'set sysroot {self.image}' /usr/bin/python3.9 core.1003", info)

    def test_a_sif_image_still_sends_the_reader_into_a_container(self):
        """No sysroot is possible there, so the old advice is still the best there is."""
        info = self._analysis(self.SIF)

        self.assertNotIn("set sysroot", info)
        self.assertIn("run gdb inside a container of the same platform", info)

    def test_the_sysroot_note_replaces_the_container_instruction(self):
        """Two different instructions for the same reader would be worse than one."""
        info = self._analysis(self.image)

        self.assertNotIn("run gdb inside a container of the same platform", info)
        self.assertIn("truncated at the first one", info)

    def _info(self, setup="asetup AthGeneration,23.6.11; "):
        """Return an analysis block built with the given setup.

        Args:
            setup (str): Experiment setup string.

        Returns:
            str: Analysis block.
        """
        container = ("/bin/bash -c export ALRB_CONT_CHOME=/srv; source atlasLocalSetup.sh "
                     "-c x86_64-centos7-gcc11-opt -s /srv/my_release_setup.sh")
        with patch.object(loopingdumps, "read_proc_link",
                          side_effect=lambda pid, name: {"exe": "/cvmfs/sw/bin/python3.9",
                                                         "cwd": "/srv"}.get(name, "")), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_cmdline", return_value=container), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]):
            return get_core_analysis_info(FakeJob(), 1003, "python athena.py",
                                          "/srv/workdir/core.1003", setup=setup)

    def test_the_container_invocation_is_recorded_verbatim(self):
        """Taken from the payload process, so the bind mounts stay right."""
        info = self._info()
        self.assertIn("atlasLocalSetup.sh -c x86_64-centos7-gcc11-opt", info)
        self.assertIn("the payload container was started with", info)

    def test_the_host_gdb_pitfall_is_stated(self):
        """The payload's libc comes from the image, not from the worker node."""
        info = self._info()
        self.assertIn("run gdb inside a container of the same platform", info)
        self.assertIn("not from the worker node", info)

    def test_the_release_setup_is_recorded(self):
        """Whoever opens the core file needs the same release."""
        info = self._info()
        self.assertIn("release setup (as used by the pilot)", info)
        self.assertIn("asetup AthGeneration,23.6.11", info)

    def test_the_encodings_pitfall_is_stated(self):
        """The same failure will hit an interactive gdb after the same setup."""
        self.assertIn("No module named 'encodings'", self._info())
        self.assertIn("unset both before starting gdb", self._info())

    def test_the_gdb_output_file_is_pointed_at(self):
        """The backtraces travel in the log tarball next to the core file."""
        self.assertIn(f"core.1003{GDB_OUTPUT_SUFFIX}", self._info())

    def test_the_block_survives_a_missing_setup(self):
        """A plugin without a setup hook must not break the block."""
        info = self._info(setup="")
        self.assertIn(CORE_INFO_MARKER, info)
        self.assertNotIn("release setup (as used by the pilot)", info)


class TestPhaseCommandConstruction(unittest.TestCase):
    """The output has to come from a file, not from the return value."""

    def test_output_is_redirected_to_the_named_file(self):
        """execute() discards stdout on timeout, which is when it matters most."""
        cmd = build_phase_command(build_gdb_invocation(7, ["-ex bt"]),
                                  "/srv/workdir/core.7.gdb.txt", "=== phase B ===")
        self.assertIn('>> "/srv/workdir/core.7.gdb.txt" 2>&1', cmd)

    def test_the_gdb_identity_is_recorded(self):
        """Which gdb was picked up is the first question asked when a dump fails."""
        cmd = build_phase_command(build_gdb_invocation(7, ["-ex bt"]),
                                  "/srv/workdir/core.7.gdb.txt", "=== phase B ===")
        self.assertIn("command -v gdb", cmd)
        self.assertIn("gdb --version", cmd)

    def test_the_setup_precedes_the_environment_sanitising(self):
        """Unsetting has to happen after the setup that exports the variables."""
        cmd = build_phase_command(build_gdb_invocation(7, ["-ex bt"]),
                                  "/srv/workdir/core.7.gdb.txt", "=== phase B ===",
                                  setup="asetup Athena; ")
        self.assertLess(cmd.index("asetup Athena"), cmd.index("unset PYTHONHOME"))


class TestSymbolLoading(unittest.TestCase):
    """gdb reads symbols during the attach, before any -ex command runs.

    This is what defeated the first two attempts at the core dump. The phase
    that needs no symbols still paid for all of them: measured against gdb 15.1
    on a process mapping 176 shared objects, the attach reads 176 symbol tables
    with the default setting and 2 with auto-solib-add off, and the core file
    is byte-identical either way. On CVMFS, with several hundred objects and a
    cold cache, those reads exhausted a 300 s timeout on a 989 MB payload
    before generate-core-file was ever reached.
    """

    def test_the_core_phase_disables_symbol_loading(self):
        """The setting must be an -iex: by the time an -ex runs, the reading is done."""
        invocation = build_gdb_invocation(1003, ["-ex bt"], symbols=False)

        self.assertIn("-iex 'set auto-solib-add off'", invocation)
        self.assertIn("-iex 'set auto-load no'", invocation)
        self.assertLess(invocation.index("auto-solib-add"), invocation.index("-ex bt"))

    def test_the_backtrace_phase_keeps_symbol_loading(self):
        """Phase B is the one that actually needs the symbols."""
        invocation = build_gdb_invocation(1003, ["-ex bt"])

        self.assertNotIn("auto-solib-add", invocation)

    def test_the_dump_uses_each_setting_in_the_right_phase(self):
        """Phase A without symbols, phase B with them."""
        job = FakeJob(pid=1000, workdir=self.workdir)
        stub = GdbStub([(0, "Saved corefile", os.path.join(self.workdir, "core.1003")),
                        (0, PARTIAL_BACKTRACE, None)])
        with patch.object(loopingdumps, "execute", stub), \
             patch.object(loopingdumps, "select_dump_candidates", return_value=[(1003, "athena.py")]), \
             patch.object(loopingdumps, "get_rss", return_value=1024), \
             patch.object(loopingdumps, "has_room_for_core", return_value=True), \
             patch.object(loopingdumps, "get_gdb_setup", return_value="asetup Athena; "), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
             patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/python"), \
             patch.object(loopingdumps, "get_cmdline", return_value="bash -c payload"), \
             patch.object(loopingdumps, "resume_process"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO"):
            create_core_dump(job)

        self.assertIn("auto-solib-add off", stub.calls[0][0])
        self.assertNotIn("auto-solib-add", stub.calls[1][0])

    def setUp(self):
        """Create a work directory."""
        reset_looping_dump_state()
        self._tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = self._tmp.name

    def tearDown(self):
        """Remove the work directory."""
        self._tmp.cleanup()
        reset_looping_dump_state()


class TestStageMarkers(unittest.TestCase):
    """A timeout leaves no exit status, so the markers are the only evidence."""

    def test_a_stall_during_startup_is_named(self):
        """No marker at all means gdb never reached the commands."""
        with self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.log_stall_diagnosis("Attaching to process 1003", "phase A")

        text = "\n".join(captured.output)
        self.assertIn("never reached the requested commands", text)
        self.assertIn("symbol table of every mapped shared object", text)

    def test_a_stall_during_the_commands_is_named(self):
        """The startup marker means gdb got as far as running them."""
        with self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.log_stall_diagnosis(f"blah\n{STARTUP_MARKER}\n", "phase A")

        self.assertIn("stopped while running them", "\n".join(captured.output))

    def test_a_complete_core_file_is_recognised(self):
        """gdb can be killed after the core file is already on disk."""
        with self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.log_stall_diagnosis(
                f"{STARTUP_MARKER}\nSaved corefile\n{CORE_WRITTEN_MARKER}\n", "phase A")

        self.assertIn("core file was complete", "\n".join(captured.output))

    def test_the_startup_marker_does_not_claim_the_attach_succeeded(self):
        """gdb in batch mode carries on after an error, so the echo runs anyway.

        Verified against gdb 15.1: a failed attach prints 'ptrace: No such
        process.' and then the marker. The attach failure has to be detected
        from gdb's own error text instead.
        """
        failed = "ptrace: No such process.\n=== gdb ready ===\nYou can't do that without a process to debug."

        self.assertIn(STARTUP_MARKER, failed)  # the marker is present ...
        self.assertTrue(loopingdumps.has_attach_failure(failed))  # ... and means nothing here

    def test_a_successful_attach_is_not_flagged(self):
        """The real gdb output of a working attach must stay clean."""
        succeeded = (f"[Thread debugging using libthread_db enabled]\n{STARTUP_MARKER}\n"
                     f"Saved corefile /tmp/core.944\n{CORE_WRITTEN_MARKER}\n"
                     f"[Inferior 1 (process 944) detached]")

        self.assertFalse(loopingdumps.has_attach_failure(succeeded))


class TestPerPhaseLogging(unittest.TestCase):
    """A phase logged only after the next one has finished is invisible for minutes."""

    def test_output_is_logged_with_its_phase_label(self):
        """And truncated to a bounded number of lines."""
        with self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.log_gdb_output(PARTIAL_BACKTRACE, "phase B (backtraces)", "/srv/core.7.gdb.txt")

        text = "\n".join(captured.output)
        self.assertIn("phase B (backtraces)", text)
        self.assertIn("__read_nocancel", text)

    def test_a_long_output_is_truncated_and_says_so(self):
        """The full text is in the log tarball either way."""
        with self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.log_gdb_output("\n".join(f"#{i} frame" for i in range(500)),
                                        "phase B", "/srv/core.7.gdb.txt")

        text = "\n".join(captured.output)
        self.assertIn("earlier lines in core.7.gdb.txt", text)
        self.assertNotIn("#0 frame", text)

    def test_empty_output_is_reported(self):
        """gdb producing nothing at all is itself a finding."""
        with self.assertLogs("pilot.util.loopingdumps", level="WARNING") as captured:
            loopingdumps.log_gdb_output("", "phase A", "/srv/core.7.gdb.txt")

        self.assertIn("no output at all", "\n".join(captured.output))


class TestCvmfsFailureDetection(unittest.TestCase):
    """gdb failing on a CVMFS path is not by itself a CVMFS failure.

    gdb defaults to fetching files through the inferior's mount namespace, and
    reaching into an unprivileged Apptainer image from the host fails with an
    I/O error whatever the file is. Since most payload executables live under
    /cvmfs, taking gdb's word for it would mislabel almost every containerised
    looping job. A path only counts if gdb named it without the target: prefix
    and the pilot, which runs outside the container, cannot read it either.
    """

    # verbatim from the wuppertal log. Note the target: prefix - this is gdb
    # reaching through the container, not a statement about CVMFS
    WUPPERTAL_LINE = (
        'warning: "target:/cvmfs/atlas.cern.ch/repo/sw/software/25.2/AnalysisBase/25.2.97/'
        'InstallArea/x86_64-el9-gcc14-opt/bin/eventloop_run_grid_job": could not open as an '
        'executable file: Input/output error.'
    )

    # verbatim from ANALY_CERN-PTEST, where CVMFS was demonstrably healthy: the
    # same error, on a path that has nothing to do with CVMFS
    PTEST_LINE = (
        'warning: "target:/usr/bin/python3.9": could not open as an executable file: '
        'Input/output error.'
    )

    # what a real CVMFS failure looks like: no target: prefix, so gdb read it
    # from the worker node's own filesystem
    HOST_LINE = (
        'warning: "/cvmfs/atlas.cern.ch/repo/sw/software/25.2/bin/athena": could not open as '
        'an executable file: Input/output error.'
    )

    def test_a_target_prefixed_failure_is_not_evidence_about_cvmfs(self):
        """The wuppertal line, which this code was originally built around."""
        self.assertEqual(loopingdumps.get_cvmfs_failure_paths(self.WUPPERTAL_LINE), [])
        self.assertFalse(loopingdumps.has_cvmfs_io_failure(self.WUPPERTAL_LINE))

    def test_the_same_error_occurs_with_cvmfs_healthy(self):
        """ANALY_CERN-PTEST produced it on a non-CVMFS path with CVMFS working."""
        self.assertFalse(loopingdumps.has_cvmfs_io_failure(self.PTEST_LINE))

    def test_a_host_path_is_extracted_for_checking(self):
        """Without the prefix, the path is gdb's own view of the node."""
        paths = loopingdumps.get_cvmfs_failure_paths(self.HOST_LINE)

        self.assertEqual(paths, ["/cvmfs/atlas.cern.ch/repo/sw/software/25.2/bin/athena"])

    def test_the_pilot_confirming_the_failure_is_evidence(self):
        """Two independent readers failing on the same path is a broken node."""
        with patch.object(loopingdumps, "is_path_unreadable", return_value=True), \
             self.assertLogs("pilot.util.loopingdumps", level="WARNING") as captured:
            observed = loopingdumps.has_cvmfs_io_failure(self.HOST_LINE)

        self.assertTrue(observed)
        self.assertIn("CVMFS is broken on this node", "\n".join(captured.output))

    def test_the_pilot_reading_it_fine_settles_the_matter(self):
        """gdb could not, the pilot could, so it was the mount namespace."""
        with patch.object(loopingdumps, "is_path_unreadable", return_value=False), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            observed = loopingdumps.has_cvmfs_io_failure(self.HOST_LINE)

        self.assertFalse(observed)
        self.assertIn("mount namespace", "\n".join(captured.output))

    def test_a_cvmfs_path_without_a_failure_is_not_one(self):
        """gdb mentions CVMFS paths constantly when things are working."""
        self.assertEqual(loopingdumps.get_cvmfs_failure_paths(
            "0x1 in main () from /cvmfs/atlas.cern.ch/repo/sw/lib/libAthenaKernel.so"), [])

    def test_an_io_error_on_a_local_path_is_not_one_either(self):
        """Both halves have to be on the same line, and the path must be CVMFS."""
        self.assertEqual(loopingdumps.get_cvmfs_failure_paths(
            'warning: "/tmp/scratch/payload": Input/output error.'), [])

    def test_the_two_halves_on_separate_lines_do_not_combine(self):
        """An unrelated CVMFS line next to an unrelated error must not pair up."""
        output = ("Reading symbols from /cvmfs/atlas.cern.ch/repo/sw/bin/athena\n"
                  "warning: /tmp/x: Input/output error.")

        self.assertEqual(loopingdumps.get_cvmfs_failure_paths(output), [])

    def test_ordinary_backtraces_are_clean(self):
        """No false positive on the common case."""
        self.assertFalse(loopingdumps.has_cvmfs_io_failure(PARTIAL_BACKTRACE))
        self.assertFalse(loopingdumps.has_cvmfs_io_failure(""))

    def test_a_blocked_read_counts_as_unreadable(self):
        """open() blocks indefinitely on a hung mount."""
        with patch.object(loopingdumps, "PATH_CHECK_TIMEOUT", 0.2), \
             patch.object(loopingdumps, "call_with_timeout", return_value=None), \
             self.assertLogs("pilot.util.loopingdumps", level="WARNING"):
            self.assertTrue(loopingdumps.is_path_unreadable("/cvmfs/atlas.cern.ch/x"))

    def test_a_readable_path_is_reported_as_readable(self):
        """The check has to be able to clear a path, or it proves nothing."""
        with tempfile.NamedTemporaryFile() as handle:
            handle.write(b"x")
            handle.flush()
            self.assertFalse(loopingdumps.is_path_unreadable(handle.name))

    def test_a_missing_path_is_reported_as_unreadable(self):
        """A path gdb saw that the pilot cannot open at all."""
        with self.assertLogs("pilot.util.loopingdumps", level="WARNING"):
            self.assertTrue(loopingdumps.is_path_unreadable("/cvmfs/does/not/exist/anywhere"))

    def test_the_dump_reports_a_confirmed_failure_to_its_caller(self):
        """The caller uses this to pick the error code, so it must propagate."""
        with tempfile.TemporaryDirectory() as workdir:
            job = FakeJob(pid=1000, workdir=workdir)
            stub = GdbStub([(1, self.HOST_LINE, None), (0, PARTIAL_BACKTRACE, None)])
            with patch.object(loopingdumps, "execute", stub), \
                 patch.object(loopingdumps, "is_path_unreadable", return_value=True), \
                 patch.object(loopingdumps, "select_dump_candidates", return_value=[(1003, "athena")]), \
                 patch.object(loopingdumps, "get_rss", return_value=1024), \
                 patch.object(loopingdumps, "has_room_for_core", return_value=True), \
                 patch.object(loopingdumps, "get_gdb_setup", return_value=""), \
                 patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
                 patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/athena"), \
                 patch.object(loopingdumps, "get_cmdline", return_value="bash -c payload"), \
                 patch.object(loopingdumps, "resume_process"), \
                 self.assertLogs("pilot.util.loopingdumps", level="INFO"):
                observed = create_core_dump(job)

        self.assertTrue(observed)

    def test_the_dump_reports_nothing_for_a_namespace_artifact(self):
        """The wuppertal case must not reach the caller as a CVMFS failure."""
        with tempfile.TemporaryDirectory() as workdir:
            job = FakeJob(pid=1000, workdir=workdir)
            stub = GdbStub([(0, self.WUPPERTAL_LINE, os.path.join(workdir, "core.1003")),
                            (0, PARTIAL_BACKTRACE, None)])
            with patch.object(loopingdumps, "execute", stub), \
                 patch.object(loopingdumps, "select_dump_candidates", return_value=[(1003, "athena")]), \
                 patch.object(loopingdumps, "get_rss", return_value=1024), \
                 patch.object(loopingdumps, "has_room_for_core", return_value=True), \
                 patch.object(loopingdumps, "get_gdb_setup", return_value=""), \
                 patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
                 patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/athena"), \
                 patch.object(loopingdumps, "get_cmdline", return_value="bash -c payload"), \
                 patch.object(loopingdumps, "resume_process"), \
                 self.assertLogs("pilot.util.loopingdumps", level="INFO"):
                observed = create_core_dump(job)

        self.assertFalse(observed)


class TestExecutableArgument(unittest.TestCase):
    """gdb must be told which binary it is looking at, without going through the container."""

    def test_the_core_file_is_read_instead_of_attaching(self):
        """A second attach can be refused; reading a core file needs no ptrace."""
        invocation = build_gdb_invocation(1003, ["-ex bt"], core_path="/srv/core.1003")

        self.assertIn("-c /srv/core.1003", invocation)
        self.assertNotIn("-p 1003", invocation)

    def test_the_core_file_invocation_does_not_detach(self):
        """Nothing is attached, so a detach only prints noise into the frames."""
        invocation = build_gdb_invocation(1003, ["-ex bt"], core_path="/srv/core.1003")

        self.assertNotIn("detach", invocation)
        self.assertIn("-ex quit", invocation)

    def test_a_live_attach_still_detaches(self):
        """The payload is killed by the caller, not left stopped by the diagnostic."""
        invocation = build_gdb_invocation(1003, ["-ex bt"])

        self.assertIn("-p 1003", invocation)
        self.assertIn("-ex detach", invocation)

    def test_the_proc_exe_link_is_used(self):
        """A magic symlink to the inode, so the mount namespace is irrelevant."""
        argument = loopingdumps.get_executable_argument(os.getpid())

        self.assertEqual(argument, f"-se /proc/{os.getpid()}/exe")

    def test_a_dead_process_yields_no_argument(self):
        """The link disappears with the process; gdb has to cope on its own."""
        with self.assertLogs("pilot.util.loopingdumps", level="WARNING"):
            self.assertEqual(loopingdumps.get_executable_argument(999999999), "")

    def test_the_invocation_carries_the_executable(self):
        """Otherwise gdb resolves the path inside the container and fails."""
        invocation = build_gdb_invocation(os.getpid(), ["-ex bt"])

        self.assertIn(f"-se /proc/{os.getpid()}/exe", invocation)


class TestEmptySetupHandling(unittest.TestCase):
    """A job without a software release gets a setup that is only a separator."""

    def test_a_separator_only_setup_is_normalised_away(self):
        """The plugin returns '; ' for a job with no software release.

        Observed at ANALY_CERN-PTEST, where swRelease was NULL. Passing that on
        as a real setup is what produced the bare leading ';' in the gdb
        command and the empty section in the analysis file.
        """
        class _Plugin:  # pylint: disable=too-few-public-methods
            """Stand-in for a plugin whose setup comes back empty."""

            @staticmethod
            def preprocess_debug_command(scratch):
                """Leave the command as the separator alone."""
                scratch.debug_command = "; "

        with patch.object(loopingdumps, "__import__", create=True, return_value=_Plugin):
            setup = loopingdumps.get_gdb_setup(FakeJob())

        self.assertEqual(setup, "")

    def test_a_real_setup_is_passed_through(self):
        """The normalisation must not swallow a setup that has content."""
        class _Plugin:  # pylint: disable=too-few-public-methods
            """Stand-in for a plugin returning a real setup."""

            @staticmethod
            def preprocess_debug_command(scratch):
                """Set a setup with actual content."""
                scratch.debug_command = "asetup Athena,24.0.41; "

        with patch.object(loopingdumps, "__import__", create=True, return_value=_Plugin):
            setup = loopingdumps.get_gdb_setup(FakeJob())

        self.assertEqual(setup, "asetup Athena,24.0.41; ")

    def test_an_empty_setup_leaves_no_stray_separator(self):
        """The phase B command used to start with a bare ';'."""
        cmd = build_phase_command(build_gdb_invocation(7, ["-ex bt"]),
                                  "/srv/core.7.gdb.txt", "=== phase B ===", setup="")

        self.assertFalse(cmd.startswith(";"))

    def test_an_empty_setup_leaves_no_empty_analysis_section(self):
        """An empty 'release setup:' heading reads as though something went missing."""
        with patch.object(loopingdumps, "read_proc_link", return_value="/usr/bin/python3"), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_cmdline", return_value="bash -c payload"), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]):
            info = get_core_analysis_info(FakeJob(), 1003, "python x.py", "/srv/core.1003", setup="")

        self.assertNotIn("release setup (as used by the pilot)", info)


class TestPostMortemBacktraces(unittest.TestCase):
    """Phase B reads the core file, and resolves libraries against the image.

    The first design ran gdb inside a second instance of the payload's own
    container image, so that the library paths recorded in the inferior would
    mean what the payload meant by them. That cannot work: unprivileged
    Apptainer puts the second gdb in a new user namespace, where it holds no
    capability over a process in the parent namespace, and the attach is
    refused with "ptrace: Operation not permitted" even though the payload is
    visible in the shared PID namespace. Observed in production on job
    7305590981 at ANALY_CERN-PTEST.

    Reading the core file instead needs no ptrace at all, and pointing gdb's
    sysroot at the image directory resolves the same libraries from the host.
    """

    IMAGE = "/cvmfs/atlas.cern.ch/repo/containers/fs/singularity/x86_64-almalinux9"
    SIF = "/cvmfs/atlas.cern.ch/repo/containers/images/apptainer/x86_64-el9.img"

    def setUp(self):
        """Create a work directory."""
        reset_looping_dump_state()
        self._tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = self._tmp.name

    def tearDown(self):
        """Remove the work directory."""
        self._tmp.cleanup()
        reset_looping_dump_state()

    def test_the_image_comes_from_the_payload_itself(self):
        """Not inferred from the platform: this is the image actually in use."""
        environment = {"APPTAINER_CONTAINER": self.IMAGE, "HOME": "/srv"}
        with patch.object(loopingdumps, "get_process_environment", return_value=environment):
            self.assertEqual(loopingdumps.get_payload_container_image(1003), self.IMAGE)

    def test_the_singularity_variable_is_honoured_too(self):
        """Older sites still export the Singularity name."""
        with patch.object(loopingdumps, "get_process_environment",
                          return_value={"SINGULARITY_CONTAINER": self.IMAGE}):
            self.assertEqual(loopingdumps.get_payload_container_image(1003), self.IMAGE)

    def test_an_uncontainerised_payload_yields_no_image(self):
        """Nothing to resolve against, so the host's own libraries it is."""
        with patch.object(loopingdumps, "get_process_environment", return_value={"HOME": "/srv"}):
            self.assertEqual(loopingdumps.get_payload_container_image(1003), "")

    def test_the_environment_is_read_from_proc(self):
        """The pilot runs as the same user, so the payload's environ is readable."""
        environment = loopingdumps.get_process_environment(os.getpid())

        # every process has PATH; the point is that /proc/<pid>/environ parsed at all
        self.assertIn("PATH", environment)

    def test_the_image_is_looked_up_once_per_pid(self):
        """Four callers need it, and each one used to log the same line.

        The analysis notes, the sysroot, the Python stack check and the
        decision not to trace a containerised payload all ask. Job 7311944106
        carried four identical 'the payload container image is ...' lines.
        """
        environment = {"APPTAINER_CONTAINER": self.IMAGE}
        with patch.object(loopingdumps, "get_process_environment",
                          return_value=environment) as lookup, \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            first = loopingdumps.get_payload_container_image(1003)
            second = loopingdumps.get_payload_container_image(1003)

        self.assertEqual((first, second), (self.IMAGE, self.IMAGE))
        self.assertEqual(lookup.call_count, 1)
        announcements = [line for line in captured.output if "container image is" in line]
        self.assertEqual(len(announcements), 1)

    def test_an_unreadable_environment_is_not_fatal(self):
        """The process may have exited between the ranking and the dump."""
        self.assertEqual(loopingdumps.get_process_environment(999999999), {})

    def test_a_directory_image_becomes_the_sysroot(self):
        """The ATLAS images on CVMFS are unpacked directories the host can read.

        Measured against gdb 15.1: without this the frames are '?? ()' and gdb
        reports "Unable to find dynamic linker breakpoint function", which is
        exactly the production symptom; with it the frames are named and carry
        source lines.
        """
        with patch.object(loopingdumps, "get_payload_container_image", return_value=self.workdir):
            options = loopingdumps.get_sysroot_options(1003)

        self.assertIn(f"-iex 'set sysroot {self.workdir}'", " ".join(options))

    def test_a_sif_image_yields_no_sysroot(self):
        """gdb cannot look inside a single-file image, so pointing at it would lie."""
        with patch.object(loopingdumps, "get_payload_container_image", return_value=self.SIF):
            options = loopingdumps.get_sysroot_options(1003)

        self.assertEqual(options, [])

    def test_an_unreadable_image_yields_no_sysroot(self):
        """A path that cannot be read is not a sysroot, whatever it names."""
        with patch.object(loopingdumps, "get_payload_container_image", return_value=self.IMAGE), \
             patch.object(loopingdumps.os.path, "isdir", return_value=True), \
             patch.object(loopingdumps.os, "access", return_value=False):
            options = loopingdumps.get_sysroot_options(1003)

        self.assertEqual(options, [])

    def test_an_uncontainerised_payload_yields_no_sysroot(self):
        """Nothing to point at, and gdb's own defaults are then correct."""
        with patch.object(loopingdumps, "get_payload_container_image", return_value=""):
            self.assertEqual(loopingdumps.get_sysroot_options(1003), [])

    def _run_phase_b(self, responses, core_path="", sysroot_options=None, python_options=None):
        """Run the backtrace phase against the stub.

        Args:
            responses (list): Canned (exit_code, output) pairs.
            core_path (str): Core file handed to the phase.
            sysroot_options (list): Options the image resolves to.
            python_options (tuple): Python stack options the image resolves to.

        Returns:
            tuple: (GdbStub, captured log text, resume_process mock).
        """
        stub = GdbStub([(code, output, None) for code, output in responses])
        output_path = os.path.join(self.workdir, "core.1003.gdb.txt")
        with patch.object(loopingdumps, "execute", stub), \
             patch.object(loopingdumps, "get_sysroot_options",
                          return_value=sysroot_options or []), \
             patch.object(loopingdumps, "get_python_stack_options",
                          return_value=python_options or ([], [])), \
             patch.object(loopingdumps, "resume_process") as resume, \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.run_backtrace_phase(FakeJob(pid=1000, workdir=self.workdir), 1003,
                                             output_path, "/tmp/scratch", "asetup Athena; ",
                                             deadline=time.monotonic() + 600,
                                             core_path=core_path)

        return stub, "\n".join(captured.output), resume

    def _core(self):
        """Write a core file for the phase to read.

        Returns:
            str: Path to the core file.
        """
        core_path = os.path.join(self.workdir, "core.1003")
        with open(core_path, "wb") as _file:
            _file.write(b"\x7fELF" + b"\x00" * 1024)

        return core_path

    def test_the_core_file_is_used_when_there_is_one(self):
        """And nothing else runs when it works."""
        stub, _, _ = self._run_phase_b([(0, PARTIAL_BACKTRACE)], core_path=self._core())

        self.assertEqual(len(stub.calls), 1)
        self.assertIn("core.1003", stub.calls[0][0])
        self.assertNotIn("-p 1003", stub.calls[0][0])
        self.assertIn("asetup Athena", stub.calls[0][0])

    def test_a_payload_without_a_core_file_is_attached_to(self):
        """An oversized payload keeps the backtraces, so they must still be taken."""
        stub, _, _ = self._run_phase_b([(0, PARTIAL_BACKTRACE)])

        self.assertEqual(len(stub.calls), 1)
        self.assertIn("-p 1003", stub.calls[0][0])

    def test_a_core_file_that_was_never_written_is_not_read(self):
        """report_core_file() says so, but the phase must not take its word for it."""
        stub, _, _ = self._run_phase_b([(0, PARTIAL_BACKTRACE)],
                                       core_path=os.path.join(self.workdir, "core.1003"))

        self.assertIn("-p 1003", stub.calls[0][0])

    def test_an_unreadable_core_file_falls_back_to_the_live_process(self):
        """Some backtraces are worth more than none."""
        stub, log, _ = self._run_phase_b(
            [(1, '"core.1003" is not a core dump: file format not recognized'),
             (0, PARTIAL_BACKTRACE)],
            core_path=self._core()
        )

        self.assertEqual(len(stub.calls), 2)
        self.assertNotIn("-p 1003", stub.calls[0][0])
        self.assertIn("-p 1003", stub.calls[1][0])
        self.assertIn("the core file could not be read", log)

    def test_a_timed_out_core_read_is_not_retried_against_the_process(self):
        """A slow read is not an unreadable one, and the attach is slower still.

        The symbol tables are the cost either way, and the fallback would pay
        for them a second time out of what is left of the budget.
        """
        stub, _, _ = self._run_phase_b([(errors.COMMANDTIMEDOUT, PARTIAL_BACKTRACE)],
                                       core_path=self._core())

        self.assertEqual(len(stub.calls), 1)

    def test_the_sysroot_reaches_the_command(self):
        """The option is useless unless the phase actually passes it to gdb."""
        stub, _, _ = self._run_phase_b([(0, PARTIAL_BACKTRACE)], core_path=self._core(),
                                       sysroot_options=[f"-iex 'set sysroot {self.IMAGE}'"])

        self.assertIn(f"set sysroot {self.IMAGE}", stub.calls[0][0])

    def test_a_refused_attach_is_a_failure_even_though_gdb_exits_zero(self):
        """gdb in batch mode carries on after an error and still exits 0.

        In production the in-container phase B printed "ptrace: Operation not
        permitted", produced no frames, exited 0 and was logged as having
        finished in 1 s. The fallback that should have followed never ran.
        """
        self.assertTrue(loopingdumps.phase_failed(0, "ptrace: Operation not permitted.\nNo stack."))
        self.assertFalse(loopingdumps.phase_failed(0, PARTIAL_BACKTRACE))

    def test_a_refused_attach_leaves_the_payload_running(self):
        """A phase killed mid-attach leaves the payload stopped; exit 0 hides it."""
        _, _, resume = self._run_phase_b([(0, "ptrace: Operation not permitted.\nNo stack.")])

        resume.assert_called_once_with(1003)

    def test_py_bt_does_not_reach_the_command_without_a_helper(self):
        """The decision is useless unless the phase honours it."""
        stub, _, _ = self._run_phase_b([(0, PARTIAL_BACKTRACE)], core_path=self._core())

        self.assertNotIn("py-bt", stub.calls[0][0])
        self.assertNotIn("auto-load safe-path", stub.calls[0][0])
        self.assertIn("thread apply all bt", stub.calls[0][0])

    def test_py_bt_reaches_the_command_when_there_is_a_helper(self):
        """And the auto-load option with it, or the script would be refused."""
        stub, _, _ = self._run_phase_b(
            [(0, PARTIAL_BACKTRACE)], core_path=self._core(),
            python_options=(["-iex 'set auto-load safe-path /'"], ["-ex 'py-bt'"]))

        self.assertIn("py-bt", stub.calls[0][0])
        self.assertIn("set auto-load safe-path /", stub.calls[0][0])

    def test_the_retry_without_the_setup_keeps_asking_for_the_python_stack(self):
        """It is the most valuable artefact for a looping transform."""
        stub, _, _ = self._run_phase_b(
            [(1, ENCODINGS_FAILURE), (0, PARTIAL_BACKTRACE)], core_path=self._core(),
            python_options=(["-iex 'set auto-load safe-path /'"], ["-ex 'py-bt'"]))

        self.assertEqual(len(stub.calls), 2)
        self.assertIn("py-bt", stub.calls[1][0])

    def test_the_python_environment_is_stripped(self):
        """The pilot itself runs under an ALRB Python, so it can poison gdb."""
        stub, _, _ = self._run_phase_b([(0, PARTIAL_BACKTRACE)], core_path=self._core())

        self.assertIn("unset PYTHONHOME PYTHONPATH", stub.calls[0][0])


class TestPythonStack(unittest.TestCase):
    """py-bt is a script shipped with CPython, not a gdb built-in.

    Asking for it where no such script exists ends every dump with
    'Undefined command: "py-bt"', which reads as a fault in the pilot's gdb
    rather than as an absent script in the image. Confirmed absent from the
    ATLAS almalinux9 image.
    """

    def setUp(self):
        """Create a directory standing in for an unpacked container image."""
        reset_looping_dump_state()
        self._tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = self._tmp.name

    def tearDown(self):
        """Remove it."""
        self._tmp.cleanup()
        reset_looping_dump_state()

    def _python_options(self, libraries, helper_name=""):
        """Ask for the Python stack options for a payload mapping the given libraries.

        Args:
            libraries (list): Object files mapped by the payload.
            helper_name (str): Helper script to create under the image, if any.

        Returns:
            tuple: (early options, commands).
        """
        if helper_name:
            path = os.path.join(self.workdir, helper_name.lstrip(os.sep))
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as _file:
                _file.write("# helper\n")
        with patch.object(loopingdumps, "get_shared_libraries", return_value=libraries), \
             patch.object(loopingdumps, "read_proc_link", return_value=""):
            return loopingdumps.get_python_stack_options(1003, self.workdir)

    def test_py_bt_is_not_requested_when_the_image_has_no_helper(self):
        """Otherwise every dump ends in 'Undefined command: "py-bt"'.

        Which reads as a fault in the pilot's gdb rather than as an absent
        script in the image. Confirmed absent from the ATLAS almalinux9 image.
        """
        early, commands = self._python_options(["/usr/lib64/libpython3.9.so.1.0"])

        self.assertEqual((early, commands), ([], []))

    def test_py_bt_is_requested_when_the_helper_sits_beside_the_library(self):
        """The first of the two places gdb looks."""
        _, commands = self._python_options(
            ["/usr/lib64/libpython3.9.so.1.0"],
            helper_name="/usr/lib64/libpython3.9.so.1.0-gdb.py")

        self.assertEqual(commands, ["-ex 'py-bt'"])

    def test_py_bt_is_requested_when_the_helper_is_under_auto_load(self):
        """The second place, which is where a distribution usually ships it."""
        _, commands = self._python_options(
            ["/usr/lib64/libpython3.9.so.1.0"],
            helper_name="/usr/share/gdb/auto-load/usr/lib64/libpython3.9.so.1.0-gdb.py")

        self.assertEqual(commands, ["-ex 'py-bt'"])

    def test_auto_load_is_only_widened_when_there_is_a_script_to_load(self):
        """It lets gdb execute a script out of the image, so it is not free."""
        early, _ = self._python_options(
            ["/usr/lib64/libpython3.9.so.1.0"],
            helper_name="/usr/lib64/libpython3.9.so.1.0-gdb.py")

        self.assertEqual(early, ["-iex 'set auto-load safe-path /'"])
        self.assertEqual(self._python_options(["/usr/lib64/libc.so.6"])[0], [])

    def test_a_payload_that_is_not_python_is_not_asked_for_a_python_stack(self):
        """The same check covers it, with no separate test for the payload type."""
        early, commands = self._python_options(["/usr/lib64/libc.so.6"])

        self.assertEqual((early, commands), ([], []))


class TestHeaderQuoting(unittest.TestCase):
    """The phase header is shell input, so it has to be quoted as such."""

    def test_an_apostrophe_in_the_header_does_not_break_the_command(self):
        """The header "in the payload's container" killed phase B in production.

        The apostrophe closed the single-quoted echo early and the following
        parenthesis became a syntax error, so bash rejected the whole command
        and gdb never ran. Constraining the header by convention was not
        enough; it has to be quoted.
        """
        cmd = build_phase_command("gdb -p 7", "/tmp/out.txt",
                                  "=== phase B: backtraces (in the payload's container) ===")

        self.assertEqual(subprocess.run(["bash", "-n", "-c", cmd], check=False).returncode, 0)

    def test_the_headers_actually_used_all_parse(self):
        """Every header this module emits, checked against a real shell."""
        headers = [
            "=== phase A: core file (bare gdb, no release setup, no symbols) ===",
            "=== phase A (retry): core file (clean environment) ===",
            "=== phase B: backtraces (release setup) ===",
            "=== phase B: backtraces (in the payload's container) ===",
            "=== phase B (retry): backtraces (no release setup) ===",
        ]
        for header in headers:
            cmd = build_phase_command("gdb -p 7", "/tmp/out.txt", header)
            self.assertEqual(
                subprocess.run(["bash", "-n", "-c", cmd], check=False).returncode, 0, msg=header
            )

    def test_the_header_still_reaches_the_output_file(self):
        """Quoting must not mangle what the reader sees."""
        with tempfile.TemporaryDirectory() as workdir:
            path = os.path.join(workdir, "out.txt")
            header = "=== phase B: backtraces (in the payload's container) ==="
            cmd = build_phase_command("true", path, header)
            subprocess.run(["bash", "-c", cmd], check=False)
            with open(path, encoding="utf-8") as _file:
                self.assertIn(header, _file.read())


class TestImageExecutable(unittest.TestCase):
    """The executable gdb is given must be the one the payload is running.

    ``-se /proc/<pid>/exe`` opens the right inode, but gdb then records the
    object file under the path that symlink *resolves* to - a path that names a
    file inside the container image and a different file on the worker node.
    Measured against gdb 15.1. In production (job 7313656511) gdb re-read the
    worker node's copy during the attach:

        `/usr/bin/python3.9' has changed; re-reading symbols.

    and the core file was then written with the wrong executable loaded.
    """

    def setUp(self):
        """Give the image a real executable and clear the per pid caches."""
        reset_looping_dump_state()
        self._image = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.image = self._image.name
        os.makedirs(os.path.join(self.image, "usr", "bin"))
        self.in_container = "/usr/bin/python3.9"
        self.in_image = os.path.join(self.image, "usr/bin/python3.9")
        with open(self.in_image, "wb") as _file:
            _file.write(b"\x7fELF")

    def tearDown(self):
        """Remove the image and clear the caches for the next test."""
        self._image.cleanup()
        reset_looping_dump_state()

    def _with_image(self, image=None, executable=None):
        """Patch the container image and the executable link.

        Args:
            image (str): Container image directory, defaulting to the real one.
            executable (str): In-container executable path.

        Returns:
            Context manager applying both patches.
        """
        return patch.multiple(
            loopingdumps,
            get_payload_container_image=lambda pid: self.image if image is None else image,
            read_proc_link=lambda pid, name: executable or self.in_container,
        )

    def test_the_executable_is_named_inside_the_image(self):
        """The same build as the running process, and a real host path."""
        with self._with_image():
            self.assertEqual(loopingdumps.get_image_executable(1003), self.in_image)

    def test_the_gdb_option_names_the_image_copy(self):
        """Not /proc/<pid>/exe, which gdb resolves to the worker node's file."""
        with self._with_image():
            argument = loopingdumps.get_executable_argument(1003)

        self.assertEqual(argument, f"-se {self.in_image}")
        self.assertNotIn("/proc/1003/exe", argument)

    def test_an_executable_outside_the_image_falls_back(self):
        """A binary from a bind mount cannot be reached by prefixing the image."""
        with self._with_image(executable="/srv/workDir/private/python3"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            argument = loopingdumps.get_executable_argument(os.getpid())

        self.assertEqual(argument, f"-se /proc/{os.getpid()}/exe")
        self.assertIn("was not found inside the image", "\n".join(captured.output))

    def test_an_uncontainerised_payload_uses_the_proc_link(self):
        """Nothing to prefix with, and the recorded path is already a host path."""
        with self._with_image(image=""):
            self.assertEqual(loopingdumps.get_image_executable(1003), "")
            self.assertEqual(loopingdumps.get_executable_argument(os.getpid()),
                             f"-se /proc/{os.getpid()}/exe")

    def test_the_answer_is_cached_and_announced_once(self):
        """Three callers need it; production carried four identical lines."""
        with self._with_image(), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            first = loopingdumps.get_image_executable(1003)
            second = loopingdumps.get_image_executable(1003)

        self.assertEqual((first, second), (self.in_image, self.in_image))
        announcements = [line for line in captured.output if "executable inside the image" in line]
        self.assertEqual(len(announcements), 1)

    def test_gdb_is_told_not_to_replace_the_executable(self):
        """Its default is to load the target's file instead, silently.

        Measured against gdb 15.1: attaching with an executable whose build id
        differs from the target's produces "Build ID mismatch ...
        exec-file-mismatch handling is currently "ask"" and, in batch mode, the
        target's file is loaded. With the option off the given file is kept.
        """
        with self._with_image():
            invocation = build_gdb_invocation(1003, ["-ex bt"])

        self.assertIn("-iex 'set exec-file-mismatch off'", invocation)

    def test_the_option_is_absent_without_an_image_executable(self):
        """It does not exist before gdb 10, so it is not emitted for nothing."""
        with self._with_image(image=""):
            invocation = build_gdb_invocation(os.getpid(), ["-ex bt"])

        self.assertNotIn("exec-file-mismatch", invocation)

    def test_the_core_writing_phase_gets_it_too(self):
        """Phase A is where the substitution happened, so it is the one that matters."""
        with self._with_image():
            invocation = build_gdb_invocation(1003, ["-ex 'generate-core-file /srv/core.1003'"],
                                              symbols=False)

        self.assertIn(f"-se {self.in_image}", invocation)
        self.assertIn("-iex 'set exec-file-mismatch off'", invocation)


class TestPayloadLibrariesOutsideTheImage(unittest.TestCase):
    """A library the payload brought itself is covered by neither sysroot nor host.

    Production (job 7313656511) mapped
    /srv/workDir/<uuid>/lib64/wrapper.so, which is in the job work directory and
    not in the image, and gdb said so twice. Measured against gdb 15.1:
    'solib-search-path' removes the "Could not load shared library symbols"
    warning and is *not* searched recursively, so the exact directory has to be
    named; the "file-backed mapping note processing" warning is emitted earlier
    and survives either way.
    """

    def setUp(self):
        """Build a work directory holding a library, and clear the caches."""
        reset_looping_dump_state()
        self._workdir = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = self._workdir.name
        self.libdir = os.path.join(self.workdir, "b296d528", "lib64")
        os.makedirs(self.libdir)
        self.library = os.path.join(self.libdir, "wrapper.so")
        with open(self.library, "wb") as _file:
            _file.write(b"\x7fELF")
        self.job = FakeJob(pid=1000, workdir=self.workdir)

    def tearDown(self):
        """Remove the work directory and clear the caches."""
        self._workdir.cleanup()
        reset_looping_dump_state()

    def test_a_library_in_the_image_is_resolved_already(self):
        """The sysroot covers it, so naming a search path would say nothing."""
        image = os.path.join(self.workdir, "image")
        os.makedirs(os.path.join(image, "lib64"))
        with open(os.path.join(image, "lib64", "libc.so.6"), "wb") as _file:
            _file.write(b"\x7fELF")
        with patch.object(loopingdumps, "get_shared_libraries", return_value=["/lib64/libc.so.6"]):
            self.assertEqual(loopingdumps.classify_mapped_libraries(1003, image), ([], []))

    def test_a_library_the_sysroot_hides_is_named_by_its_own_directory(self):
        """Measured against gdb 15.1: a sysroot stops the fallback to the host path.

        libc.so.6 existed on the worker node at exactly the path recorded in the
        core file and gdb still reported "Could not load shared library
        symbols". This is the normal case for an ATLAS release, since /cvmfs is
        bind-mounted into the container at the same path, so every release
        library is readable on the node and invisible to gdb once the sysroot
        points at the image.
        """
        with patch.object(loopingdumps, "get_shared_libraries", return_value=[self.library]):
            directories, basenames = loopingdumps.classify_mapped_libraries(1003, "/cvmfs/image")

        self.assertEqual(directories, [self.libdir])
        self.assertEqual(basenames, [])

    def test_a_library_present_on_the_worker_node_needs_nothing_without_a_sysroot(self):
        """An uncontainerised payload records host paths, which gdb opens itself."""
        with patch.object(loopingdumps, "get_shared_libraries", return_value=[self.library]):
            self.assertEqual(loopingdumps.classify_mapped_libraries(1003, ""), ([], []))

    def test_a_library_in_neither_is_looked_for_by_basename(self):
        """Its recorded path names a bind mount and cannot be inverted."""
        with patch.object(loopingdumps, "get_shared_libraries",
                          return_value=["/srv/workDir/b296d528/lib64/wrapper.so"]):
            directories, basenames = loopingdumps.classify_mapped_libraries(1003, "/cvmfs/image")

        self.assertEqual(directories, [])
        self.assertEqual(basenames, ["wrapper.so"])

    def test_a_directory_is_named_once(self):
        """A release contributes many libraries from the same few directories."""
        second = os.path.join(self.libdir, "other.so")
        with open(second, "wb") as _file:
            _file.write(b"\x7fELF")
        with patch.object(loopingdumps, "get_shared_libraries",
                          return_value=[self.library, second]):
            directories, _ = loopingdumps.classify_mapped_libraries(1003, "/cvmfs/image")

        self.assertEqual(directories, [self.libdir])

    def test_the_exact_directory_is_found_not_the_work_directory(self):
        """gdb does not search solib-search-path recursively (measured).

        Passing job.workdir would look correct and resolve nothing.
        """
        directories = loopingdumps.find_library_directories(self.workdir, ["wrapper.so"])

        self.assertEqual(directories, [self.libdir])
        self.assertNotIn(self.workdir, directories)

    def test_a_library_that_is_not_there_yields_nothing(self):
        """Better than a directory that does not hold it."""
        self.assertEqual(loopingdumps.find_library_directories(self.workdir, ["nothere.so"]), [])

    def test_the_walk_is_bounded(self):
        """This runs between the decision to kill and the kill itself."""
        with patch.object(loopingdumps, "MAX_LIBRARY_SCAN_DIRECTORIES", 1), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            directories = loopingdumps.find_library_directories(self.workdir, ["wrapper.so"])

        self.assertEqual(directories, [])
        self.assertIn("stopped looking for payload libraries", "\n".join(captured.output))

    def test_a_library_below_the_depth_limit_is_not_searched_for(self):
        """The bound has to hold, or a deep tree costs the kill its budget."""
        with patch.object(loopingdumps, "MAX_LIBRARY_SCAN_DEPTH", 1):
            self.assertEqual(
                loopingdumps.find_library_directories(self.workdir, ["wrapper.so"]), []
            )

    def test_the_number_of_directories_is_capped(self):
        """A long option is a gdb command line that no longer parses."""
        libraries = [f"/cvmfs/release/lib{index}/lib{index}.so" for index in range(30)]
        for library in libraries:
            os.makedirs(os.path.dirname(library.replace("/cvmfs", self.workdir)), exist_ok=True)
            with open(library.replace("/cvmfs", self.workdir), "wb") as _file:
                _file.write(b"\x7fELF")
        with patch.object(loopingdumps, "get_shared_libraries",
                          return_value=[library.replace("/cvmfs", self.workdir)
                                        for library in libraries]):
            search_path = loopingdumps.get_solib_search_path(self.job, 1003, "/cvmfs/image")

        self.assertEqual(len(search_path.split(":")),
                         loopingdumps.MAX_SOLIB_SEARCH_DIRECTORIES)

    def test_the_option_names_the_directory(self):
        """And it is an -iex, since gdb reads the mappings while it starts up."""
        with patch.object(loopingdumps, "get_shared_libraries",
                          return_value=["/srv/workDir/b296d528/lib64/wrapper.so"]):
            options = loopingdumps.get_solib_search_path_options(self.job, 1003, "/cvmfs/image")

        self.assertEqual(options, [f"-iex 'set solib-search-path {self.libdir}'"])

    def test_nothing_is_searched_for_when_everything_resolves(self):
        """The walk is the cost here, so it must not happen for nothing."""
        with patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
             patch.object(loopingdumps, "find_library_directories") as walk:
            options = loopingdumps.get_solib_search_path_options(self.job, 1003, "/cvmfs/image")

        self.assertEqual(options, [])
        walk.assert_not_called()

    def test_the_search_path_is_cached(self):
        """Both the analysis notes and phase B ask for it."""
        with patch.object(loopingdumps, "get_shared_libraries",
                          return_value=["/srv/workDir/b296d528/lib64/wrapper.so"]), \
             patch.object(loopingdumps, "find_library_directories",
                          return_value=[self.libdir]) as walk:
            first = loopingdumps.get_solib_search_path(self.job, 1003, "/cvmfs/image")
            second = loopingdumps.get_solib_search_path(self.job, 1003, "/cvmfs/image")

        self.assertEqual((first, second), (self.libdir, self.libdir))
        self.assertEqual(walk.call_count, 1)

    def test_a_library_that_cannot_be_located_is_reported(self):
        """Silence would leave the reader with gdb's unanswerable question."""
        with patch.object(loopingdumps, "get_shared_libraries",
                          return_value=["/srv/workDir/gone/lib64/missing.so"]), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            self.assertEqual(
                loopingdumps.get_solib_search_path(self.job, 1003, "/cvmfs/image"), ""
            )

        self.assertIn("could not be found under the job work directory",
                      "\n".join(captured.output))


class TestAnalysisInstructions(unittest.TestCase):
    """The recipe in the analysis file has to work on the reader's machine.

    Measured against gdb 15.1: 'set sysroot' does not apply to the executable
    named on the command line. gdb opened the host's copy of the path, and
    failed outright when the host had no such path although the sysroot did. So
    the in-container path is the one thing that must not be quoted there.
    """

    def setUp(self):
        """Build an image holding the payload executable."""
        reset_looping_dump_state()
        self._image = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.image = self._image.name
        os.makedirs(os.path.join(self.image, "usr", "bin"))
        self.in_image = os.path.join(self.image, "usr/bin/python3.9")
        with open(self.in_image, "wb") as _file:
            _file.write(b"\x7fELF")

    def tearDown(self):
        """Remove the image and clear the caches."""
        self._image.cleanup()
        reset_looping_dump_state()

    def _info(self, libraries=None, search_path="", with_core=True):
        """Return the analysis block for a containerised payload.

        Args:
            libraries (list): Libraries reported as mapped.
            search_path (str): Search path the work directory scan resolves to.
            with_core (bool): Whether a core file was written.

        Returns:
            str: The analysis block.
        """
        with patch.object(loopingdumps, "get_payload_container_image", return_value=self.image), \
             patch.object(loopingdumps, "read_proc_link",
                          side_effect=lambda pid, name: {"exe": "/usr/bin/python3.9",
                                                         "cwd": "/srv/workDir"}.get(name, "")), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             patch.object(loopingdumps, "get_solib_search_path", return_value=search_path), \
             patch.object(loopingdumps, "get_shared_libraries", return_value=libraries or []):
            return get_core_analysis_info(FakeJob(), 1003, "/usr/bin/python3 -u ./LoopingJob.py",
                                          "/srv/workDir/core.1003", with_core=with_core)

    def test_the_command_names_the_executable_inside_the_image(self):
        """Naming the in-container path opens the reader's own interpreter."""
        info = self._info()

        self.assertIn(f"gdb -iex 'set sysroot {self.image}' {self.in_image} core.1003", info)

    def test_the_in_container_path_is_still_recorded_and_marked(self):
        """It is what the core file and the backtraces refer to."""
        info = self._info()

        self.assertIn("executable: /usr/bin/python3.9 (path inside the container)", info)
        self.assertIn(f"executable as seen from the worker node: {self.in_image}", info)

    def test_the_reader_is_told_why_the_two_are_not_interchangeable(self):
        """Otherwise the shorter path is the obvious thing to type."""
        info = self._info()

        self.assertIn("applies to the libraries, not to the executable", info)
        self.assertIn("core file may", info)

    def test_the_command_carries_the_library_search_path(self):
        """The reader gets the same resolution the pilot's own phase B had."""
        info = self._info(search_path="/pool/condor/dir/PanDA_Pilot-1/b296/lib64")

        self.assertIn("-iex 'set solib-search-path /pool/condor/dir/PanDA_Pilot-1/b296/lib64'", info)

    def test_the_expected_messages_are_listed(self):
        """Both have cost time to investigate on a dump that was correct."""
        info = self._info()

        self.assertIn("0xffffffffff600000", info)
        self.assertIn("file-backed mapping note processing", info)

    def test_nothing_is_said_about_reading_a_core_file_that_was_not_written(self):
        """An oversized payload keeps the backtraces and gets no core file."""
        info = self._info(with_core=False)

        self.assertNotIn("0xffffffffff600000", info)


class TestQuietRanking(unittest.TestCase):
    """The ranking is needed before the kill; its twenty-eight lines are not."""

    def setUp(self):
        """Clear the caches."""
        reset_looping_dump_state()

    def tearDown(self):
        """Clear the caches."""
        reset_looping_dump_state()

    def test_the_quiet_ranking_is_the_same_ranking(self):
        """A quieter answer must not be a different one."""
        cpu = {1002: 10.0, 1003: 900.0, 1006: 1.0}
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             patch.object(loopingdumps, "get_cpu_time", side_effect=lambda pid: cpu.get(pid, 0.0)), \
             patch.object(loopingdumps, "get_rss", return_value=0):
            with self.assertLogs("pilot.util.loopingdumps", level="INFO"):
                loud = select_dump_candidates(FakeJob(), label="before diagnostics")
            quiet = select_dump_candidates(FakeJob(), label="before kill", verbose=False)

        self.assertEqual(loud, quiet)
        self.assertEqual(quiet[0][0], 1003)  # the athena process, ranked on CPU time

    def test_the_quiet_ranking_logs_nothing(self):
        """assertNoLogs is what pins this; the inventory is the bulk of it.

        unittest.TestCase.assertNoLogs() was only added in Python 3.10; this
        codebase's floor is 3.9 (see vermin config), so use the documented
        assertLogs() behaviour instead: it raises AssertionError if no
        matching record was logged inside the with-block, which is exactly
        the condition we want to assert here (see test_gpu_nvidia_smi_parsing.py
        for the same idiom).
        """
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             patch.object(loopingdumps, "get_cpu_time", return_value=1.0), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             self.assertRaises(AssertionError):
            with self.assertLogs("pilot.util.loopingdumps", level="INFO"):
                select_dump_candidates(FakeJob(), label="before kill", verbose=False)

    def test_the_fallback_is_also_quiet(self):
        """Every descendant filtered out is still not a reason to log twice."""
        denylisted = [(1004, "prmon --pid 1002")]
        with patch.object(loopingdumps, "get_descendants", return_value=denylisted), \
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c payload"), \
             self.assertRaises(AssertionError):
            with self.assertLogs("pilot.util.loopingdumps", level="INFO"):
                candidates = select_dump_candidates(FakeJob(), label="before kill", verbose=False)

        self.assertEqual(candidates, [(1000, "/bin/bash -c payload")])

    def test_the_inventory_is_still_collected(self):
        """The caller needs the tree; it just does not need it in the log."""
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             self.assertRaises(AssertionError):
            with self.assertLogs("pilot.util.loopingdumps", level="INFO"):
                descendants = log_process_inventory(FakeJob(), label="before kill", verbose=False)

        self.assertEqual(descendants, ATLAS_TREE)

    def test_the_verbose_inventory_still_carries_the_marker(self):
        """Which is what the payload name list is meant to be derived from."""
        with patch.object(loopingdumps, "get_descendants", return_value=ATLAS_TREE), \
             patch.object(loopingdumps, "get_cpu_time", return_value=1.0), \
             patch.object(loopingdumps, "get_rss", return_value=0), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            log_process_inventory(FakeJob(), label="before kill")

        self.assertIn(INVENTORY_MARKER, "\n".join(captured.output))


class TestExpectedMessages(unittest.TestCase):
    """A successful dump must not read like a failed one.

    Phase A of job 7315111321 - the first with the executable fix in place -
    still carried six pairs of

        Error while mapping shared library sections:
        Could not open `target:/lib64/libc.so.6' as an executable file: Input/output error

    Measured against gdb 15.1 with 'set auto-solib-add off' in force: an
    unreadable sysroot still produces a complaint about every mapped library,
    while 'info sharedlibrary' reports 'Syms Read: No' for all of them. The
    opens build the section table and are not symbol reading, so the flag does
    not govern them and nothing in the pilot's control removes them. They are
    explained instead.
    """

    # phase A output of job 7315111321, trimmed to the relevant lines
    PHASE_A = (
        "=== phase A: core file (bare gdb, no release setup, no symbols) ===\n"
        "GNU gdb (Red Hat Enterprise Linux) 16.3-3.el9\n"
        "[New LWP 10704]\n"
        "Error while mapping shared library sections:\n"
        "Could not open `target:/lib64/libc.so.6' as an executable file: Input/output error\n"
        "0x00001512d460f8dd in ?? ()\n"
        "=== gdb ready ===\n"
        "Saved corefile /pool/condor/dir_3615630/core.10703\n"
    )

    def test_the_lines_are_recognised(self):
        """Both halves are required: the message and a container path."""
        self.assertTrue(loopingdumps.has_section_mapping_failure(self.PHASE_A))

    def test_a_failure_under_gdbs_own_root_is_not_this(self):
        """gdb failing to open a library it can see is a different situation."""
        output = self.PHASE_A.replace("target:/lib64/libc.so.6", "/lib64/libc.so.6")

        self.assertFalse(loopingdumps.has_section_mapping_failure(output))

    def test_a_container_path_alone_is_not_this(self):
        """gdb names 'target:' paths in other warnings too.

        The executable warning that the CVMFS classification has to ignore is
        one of them, and it is not a section mapping failure.
        """
        output = (
            "=== phase A ===\n"
            'warning: "target:/usr/bin/python3.9": could not open as an executable file: '
            "Input/output error.\n"
            "Saved corefile /tmp/core.1\n"
        )

        self.assertFalse(loopingdumps.has_section_mapping_failure(output))

    def test_a_clean_phase_is_not_flagged(self):
        """An uncontainerised payload produces none of this."""
        self.assertFalse(loopingdumps.has_section_mapping_failure(
            "=== phase A ===\nSaved corefile /tmp/core.1\n"))
        self.assertFalse(loopingdumps.has_section_mapping_failure(""))

    def test_the_pilot_says_they_are_expected(self):
        """Immediately after the output, where the reader meets them."""
        with patch.object(loopingdumps, "get_file_size", return_value=0), \
             patch.object(loopingdumps, "execute", return_value=(0, "", "")), \
             patch.object(loopingdumps, "read_gdb_output", return_value=self.PHASE_A), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.run_gdb_phase("gdb ...", "/tmp/out.txt", 300, "/tmp", "phase A")

        captured = "\n".join(captured.output)
        self.assertIn("are expected for a containerised payload", captured)
        self.assertIn("auto-solib-add", captured)

    def test_a_clean_phase_gets_no_explanation(self):
        """An explanation of something that did not happen is noise."""
        with patch.object(loopingdumps, "get_file_size", return_value=0), \
             patch.object(loopingdumps, "execute", return_value=(0, "", "")), \
             patch.object(loopingdumps, "read_gdb_output",
                          return_value="=== phase A ===\nSaved corefile /tmp/core.1\n"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            loopingdumps.run_gdb_phase("gdb ...", "/tmp/out.txt", 300, "/tmp", "phase A")

        self.assertNotIn("are expected for a containerised payload", "\n".join(captured.output))

    def test_the_notes_cover_all_four_messages(self):
        """The analysis file is what survives in the log tarball."""
        info = "\n".join(get_expected_message_info())

        self.assertIn("0xffffffffff600000", info)
        self.assertIn("file-backed mapping note processing", info)
        self.assertIn("Error while mapping shared library sections", info)
        self.assertIn("core file may not match specified executable file", info)

    def test_the_mismatch_note_says_what_to_check_instead(self):
        """Job 7315111321 resolved all sixteen frames while warning about this."""
        info = "\n".join(get_expected_message_info())

        self.assertIn("name comparison", info)
        self.assertIn("if they", info)

    def test_nothing_is_said_when_no_core_file_was_written(self):
        """These are messages about writing and reading one."""
        self.assertEqual(get_expected_message_info(with_core=False), [])


if __name__ == "__main__":
    unittest.main()
