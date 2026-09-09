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
import sys
import tempfile
import unittest
from unittest.mock import patch

from pilot.common.errorcodes import ErrorCodes
from pilot.util import loopingdumps
from pilot.util.loopingdumps import (
    CLEAN_ENVIRONMENT,
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
    get_process_name,
    has_python_startup_failure,
    is_denylisted,
    is_looping_diagnostic_file,
    log_process_inventory,
    rank_candidate,
    reset_looping_dump_state,
    select_dump_candidates,
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
             patch.object(loopingdumps, "get_shared_libraries", return_value=[]), \
             patch.object(loopingdumps, "read_proc_link", return_value="/cvmfs/sw/bin/python"), \
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c source atlasLocalSetup.sh -c x86_64"), \
             patch.object(loopingdumps, "resume_process"), \
             self.assertLogs("pilot.util.loopingdumps", level="INFO") as captured:
            create_core_dump(job)

        return stub, "\n".join(captured.output), core_path

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

        for command, kwargs in stub.calls:
            self.assertTrue(kwargs.get("mute"), msg="execute() must be muted or the pid is redacted")
            self.assertIn("-p 1003", command)

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


if __name__ == "__main__":
    unittest.main()
