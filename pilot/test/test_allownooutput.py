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

"""Unit tests for output files listed in allowNoOutput that the payload did not produce."""

import json
import os
import tempfile
import unittest

from pilot.info.filespec import FileSpec
from pilot.user.atlas.common import get_allowed_no_output, remove_no_output_files
from pilot.user.atlas.diagnose import process_job_report
from pilot.util.config import config

# ATLASPANDA-1867, job 7352661760: the payload (runAthena) reported that this file was
# "missing but allowed by --allowNoOutput", and the pilot then failed the job with
# "Local output file is missing" since the file was still in the stage-out list
LFN = "user.tmaeno.52919790.EXT0._000001.out.pool"
OTHER = "user.tmaeno.52919790.log.tgz"

# the part of an athena job report that the pilot reads besides the output files
EXECUTOR = [{"logfileReport": {"details": {}}}]

# ATLASPANDA-1867, job 7358358336 (D. Barberis): a text file that exists, is listed in allowNoOutput and is
# listed in the job report without an event count, which is normal for files that are not ROOT files
TXT_LFN = "user.dario.52977211.EXT0._000151.txt"
TXT_GUID = "DE823ACA-1D42-4654-B020-EEBC4F3C434B"


class FakeJob:  # pylint: disable=too-many-instance-attributes
    """Minimal stand-in for JobData."""

    def __init__(self, workdir, allownooutput=LFN, lfns=(LFN,)):
        """Build an analysis job.

        Args:
            workdir (str): Job working directory.
            allownooutput (str): allowNoOutput as sent by the server.
            lfns (tuple): Output file names in the job definition.
        """
        self.workdir = workdir
        self.metadata = {}
        self.allownooutput = allownooutput
        self.is_eventservice = False
        self.transformation = "runAthena-00-00-11"
        self.outdata = [FileSpec(filetype="output", lfn=lfn, guid="") for lfn in lfns]
        self.nevents = 0
        self.stageout = "all"
        self.exitcode = 0
        self.exitmsg = ""
        self.exeerrordiag = ""
        self.exeerrorcode = 0
        self.piloterrorcodes = []
        self.piloterrordiags = []
        self.piloterrorcode = 0
        self.piloterrordiag = ""

    def is_analysis(self):
        """Return True, these are user jobs."""
        return True


class TestAllowNoOutput(unittest.TestCase):
    """A missing file that is listed in allowNoOutput must not be staged out, whatever the job report says."""

    def setUp(self):
        """Create a job working directory."""
        self._tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = self._tmp.name

    def tearDown(self):
        """Remove it."""
        self._tmp.cleanup()

    def _write_report(self, report):
        """Write a job report in the working directory."""
        with open(os.path.join(self.workdir, config.Payload.jobreport), "w", encoding="utf-8") as fout:
            json.dump(report, fout)

    def _touch(self, lfn):
        """Create a non-empty output file."""
        with open(os.path.join(self.workdir, lfn), "w", encoding="utf-8") as fout:
            fout.write("data")

    def _lfns(self, job):
        """Return the lfns left in the stage-out list."""
        return [fspec.lfn for fspec in job.outdata]

    def test_report_without_files_section(self):
        """The job report has no 'files' key at all."""
        self._write_report({"exitCode": 0, "exitMsg": "OK", "executor": EXECUTOR})
        job = FakeJob(self.workdir)

        process_job_report(job)

        self.assertEqual(self._lfns(job), [])

    def test_report_with_empty_files_section(self):
        """The job report has 'files' but no 'output' in it."""
        self._write_report({"exitCode": 0, "exitMsg": "OK", "executor": EXECUTOR, "files": {"input": []}})
        job = FakeJob(self.workdir)

        process_job_report(job)

        self.assertEqual(self._lfns(job), [])

    def test_report_with_empty_output_list(self):
        """The job report lists no output file (this case was handled before)."""
        self._write_report({"exitCode": 0, "exitMsg": "OK", "executor": EXECUTOR, "files": {"output": []}})
        job = FakeJob(self.workdir)

        process_job_report(job)

        self.assertEqual(self._lfns(job), [])

    def test_no_job_report(self):
        """The transform wrote no job report (runGen does not create one)."""
        job = FakeJob(self.workdir)

        process_job_report(job)

        self.assertEqual(self._lfns(job), [])

    def test_existing_file_is_kept(self):
        """A file that is listed but was produced is staged out as usual."""
        self._write_report({"exitCode": 0, "exitMsg": "OK", "executor": EXECUTOR, "files": {}})
        self._touch(LFN)
        job = FakeJob(self.workdir)

        process_job_report(job)

        self.assertEqual(self._lfns(job), [LFN])

    def test_empty_listed_file_is_dropped(self):
        """A listed file of size zero is not staged out (it would fail with ZEROFILESIZE)."""
        with open(os.path.join(self.workdir, LFN), "w", encoding="utf-8"):
            pass
        job = FakeJob(self.workdir)

        remove_no_output_files(job)

        self.assertEqual(self._lfns(job), [])

    def test_empty_file_that_is_not_listed_is_kept(self):
        """Not listed means the empty file must still fail the job in stage-out."""
        with open(os.path.join(self.workdir, LFN), "w", encoding="utf-8"):
            pass
        job = FakeJob(self.workdir, allownooutput="")

        remove_no_output_files(job)

        self.assertEqual(self._lfns(job), [LFN])

    def test_listed_directory_is_kept(self):
        """Size zero means nothing for a directory, so a listed directory is left alone."""
        os.mkdir(os.path.join(self.workdir, LFN))
        job = FakeJob(self.workdir)

        remove_no_output_files(job)

        self.assertEqual(self._lfns(job), [LFN])

    def _run_with_report_entry(self, entry, allownooutput=TXT_LFN, content="payload"):
        """Process a job report that lists TXT_LFN with the given entry; return the lfns left to stage out.

        Args:
            entry (dict): The sub-file entry in the job report.
            allownooutput (str): allowNoOutput as sent by the server.
            content (str or None): Content of the file in the working directory (None: no file).

        Returns:
            list: The lfns left in the stage-out list.
        """
        self._write_report({"exitCode": 0, "exitMsg": "OK", "executor": EXECUTOR,
                            "files": {"output": [{"subFiles": [entry]}]}})
        if content is not None:
            with open(os.path.join(self.workdir, TXT_LFN), "w", encoding="utf-8") as fout:
                fout.write(content)
        job = FakeJob(self.workdir, allownooutput=allownooutput, lfns=(TXT_LFN,))

        process_job_report(job)

        return self._lfns(job)

    def test_existing_file_without_nentries_is_staged_out(self):
        """No event count in the job report says nothing about the file (ATLASPANDA-1867, Dario's case)."""
        entry = {"name": TXT_LFN, "guid": TXT_GUID}

        self.assertEqual(self._run_with_report_entry(entry), [TXT_LFN])

    def test_existing_file_with_null_nentries_is_staged_out(self):
        """The same with an explicit null."""
        entry = {"name": TXT_LFN, "guid": TXT_GUID, "nentries": None}

        self.assertEqual(self._run_with_report_entry(entry), [TXT_LFN])

    def test_existing_file_with_events_is_staged_out(self):
        """A file with events is staged out whether it is listed or not."""
        entry = {"name": TXT_LFN, "guid": TXT_GUID, "nentries": 5}

        self.assertEqual(self._run_with_report_entry(entry), [TXT_LFN])
        self.assertEqual(self._run_with_report_entry(entry, allownooutput=""), [TXT_LFN])

    def test_unlisted_file_without_nentries_is_staged_out(self):
        """Not listed in allowNoOutput: no change in behaviour."""
        entry = {"name": TXT_LFN, "guid": TXT_GUID}

        self.assertEqual(self._run_with_report_entry(entry, allownooutput=""), [TXT_LFN])

    def test_missing_file_without_nentries_is_dropped(self):
        """The job report lists the file but it is not there: the allowNoOutput list decides."""
        entry = {"name": TXT_LFN, "guid": TXT_GUID}

        self.assertEqual(self._run_with_report_entry(entry, content=None), [])

    def test_empty_file_without_nentries_is_dropped(self):
        """The job report lists the file, which is empty: the allowNoOutput list decides."""
        entry = {"name": TXT_LFN, "guid": TXT_GUID}

        self.assertEqual(self._run_with_report_entry(entry, content=""), [])

    def test_listed_file_with_zero_events_is_dropped(self):
        """An explicit zero event count for a listed file is still taken to mean no output (unchanged)."""
        entry = {"name": TXT_LFN, "guid": TXT_GUID, "nentries": 0}

        self.assertEqual(self._run_with_report_entry(entry), [])

    def test_missing_file_that_is_not_listed_is_kept(self):
        """Not listed means the stage-out check must still fail the job, so the file stays."""
        job = FakeJob(self.workdir, allownooutput="")

        remove_no_output_files(job)

        self.assertEqual(self._lfns(job), [LFN])

    def test_only_the_listed_file_is_dropped(self):
        """A job with two outputs, one of them allowed to be missing."""
        self._touch(OTHER)
        job = FakeJob(self.workdir, lfns=(LFN, OTHER))

        remove_no_output_files(job)

        self.assertEqual(self._lfns(job), [OTHER])

    def test_a_substring_is_not_a_match(self):
        """allowNoOutput is a list of names, not a string to search in."""
        job = FakeJob(self.workdir, allownooutput=f"prefix.{LFN}.suffix")

        remove_no_output_files(job)

        self.assertEqual(self._lfns(job), [LFN])

    def test_allowed_names_from_server_string(self):
        """The server sends a comma-separated string."""
        job = FakeJob(self.workdir, allownooutput=f"{LFN}, {OTHER}")

        self.assertEqual(get_allowed_no_output(job), {LFN, OTHER})

    def test_allowed_names_when_unset(self):
        """Unset values (empty string, None, False) mean nothing is allowed."""
        for value in ("", None, False):
            job = FakeJob(self.workdir, allownooutput=value)
            self.assertEqual(get_allowed_no_output(job), set())


if __name__ == "__main__":
    unittest.main()
