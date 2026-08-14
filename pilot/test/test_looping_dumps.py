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
import sys
import tempfile
import unittest
from unittest.mock import patch

from pilot.util import loopingdumps
from pilot.util.loopingdumps import (
    CORE_INFO_MARKER,
    CORE_INFO_SUFFIX,
    SNAPSHOT_FILENAME,
    format_snapshot,
    get_core_analysis_info,
    is_denylisted,
    rank_candidate,
    reset_looping_dump_state,
    select_dump_candidates,
    store_core_analysis_info,
    summarise_snapshots,
    take_looping_snapshot,
)

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

    def test_wrappers_and_helpers_are_rejected(self):
        """Shell, container, setup and transfer helpers are all rejected."""
        for cmdline in (
            "/bin/bash -c asetup Athena,24.0.41; Generate_tf.py",
            "/usr/bin/apptainer exec -B /cvmfs /srv/image.sif /srv/containerScript.sh",
            "xrdcp root://eos.cern.ch//eos/atlas/file.root .",
            "nvidia-smi --query-compute-apps=pid --format=csv",
            "ps axo pid,ppid,args",
        ):
            self.assertTrue(is_denylisted(cmdline), msg=cmdline)

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
             patch.object(loopingdumps, "get_rss", return_value=0):
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
             patch.object(loopingdumps, "get_rss", return_value=0):
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
             patch.object(loopingdumps, "get_rss", return_value=0):
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
             patch.object(loopingdumps, "get_rss", return_value=0):
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
             patch.object(loopingdumps, "get_rss", return_value=0):
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
             patch.object(loopingdumps, "get_cmdline", return_value="/bin/bash -c payload"):
            candidates = select_dump_candidates(FakeJob(pid=4000))

        self.assertEqual([pid for pid, _ in candidates], [4000])

    def test_no_pid_returns_nothing(self):
        """No payload pid means there is nothing to dump."""
        self.assertEqual(select_dump_candidates(FakeJob(pid=None)), [])

    def test_rank_is_ordered_by_name_then_cpu(self):
        """The sort key orders on the name match first and the CPU time second."""
        with patch.object(loopingdumps, "get_cpu_time", return_value=10.0), \
             patch.object(loopingdumps, "get_rss", return_value=0):
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


if __name__ == "__main__":
    unittest.main()
