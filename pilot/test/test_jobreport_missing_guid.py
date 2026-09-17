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

"""Unit tests for a job report that does not carry a guid for an output file."""

import json
import os
import tempfile
import unittest
from unittest.mock import patch

from pilot.info.filespec import FileSpec
from pilot.user.atlas.common import (
    assign_missing_guids,
    extract_output_file_guids,
    get_outfiles_records,
    update_job_data,
)

# job 7316132471 at WEIZMANN: the payload exited 0 and the job was failed anyway,
# because this report has no 'file_guid' and two places read that key directly
LFN = "output.1.3ef5c681-29ad-4d07-90d6-b53f905a0656_33240.pool.root"
JOB_REPORT = json.loads(
    '{"files": {"output": [{"subFiles": [{"name": "%s", "nentries": 1, '
    '"file_size": 10148}]}]}, "reportVersion": "1.0.0"}' % LFN
)

# the guid an XML would carry, to check it is preferred over a generated one
XML_GUID = "11111111-2222-3333-4444-555555555555"


class FakeJob:
    """Minimal stand-in for JobData."""

    def __init__(self, workdir, metadata=None, lfn=LFN, guid=""):
        """Build a job with a single output file.

        Args:
            workdir (str): Job working directory.
            metadata (dict): Job report, or None.
            lfn (str): Output file name.
            guid (str): Guid already known for it, if any.
        """
        self.workdir = workdir
        self.metadata = metadata
        self.allownooutput = False
        self.outdata = [FileSpec(filetype="output", lfn=lfn, guid=guid)]


class TestJobReportWithoutGuid(unittest.TestCase):
    """A job report may legitimately omit the guid for an output file.

    extract_output_file_guids() says so itself: "Use job report value if
    present, otherwise generate the guid. Note: guid generation is done later,
    not in this function". Only the subscript disagreed, and it turned a
    payload that exited zero into a failed job.
    """

    def setUp(self):
        """Create a job working directory."""
        self._tmp = tempfile.TemporaryDirectory()  # pylint: disable=consider-using-with
        self.workdir = self._tmp.name

    def tearDown(self):
        """Remove it."""
        self._tmp.cleanup()

    def test_the_records_parser_survives_a_missing_guid(self):
        """This path only cost job.nevents, since its caller catches the error."""
        records = get_outfiles_records(JOB_REPORT["files"]["output"][0]["subFiles"])

        self.assertEqual(records[LFN]["guid"], "")
        self.assertEqual(records[LFN]["size"], 10148)

    def test_the_records_parser_survives_a_missing_size(self):
        """The same key read the same way, for the field beside it."""
        records = get_outfiles_records([{"name": LFN, "nentries": 1}])

        self.assertEqual(records[LFN]["size"], 0)

    def test_extraction_does_not_raise_on_a_missing_guid(self):
        """The failure in job 7316132471: KeyError out of update_job_data()."""
        job = FakeJob(self.workdir, metadata=JOB_REPORT)

        extract_output_file_guids(job)

        self.assertEqual(job.outdata[0].guid, "")

    def test_a_guid_in_the_report_is_still_used(self):
        """The report remains the preferred source when it has one."""
        report = json.loads(json.dumps(JOB_REPORT))
        report["files"]["output"][0]["subFiles"][0]["file_guid"] = XML_GUID
        job = FakeJob(self.workdir, metadata=report)

        extract_output_file_guids(job)

        self.assertEqual(job.outdata[0].guid, XML_GUID)

    def test_a_missing_guid_is_generated_as_if_there_were_no_report(self):
        """Rod's requirement: behave exactly as for a transform that wrote none."""
        job = FakeJob(self.workdir, metadata=JOB_REPORT)

        extract_output_file_guids(job)
        assign_missing_guids(job)

        self.assertTrue(job.outdata[0].guid)

    def test_the_xml_is_preferred_over_generating(self):
        """Production transforms write the real guid there.

        Generating one without looking would replace a true guid with an
        invented one, which is worse than the crash it replaces.
        """
        job = FakeJob(self.workdir)

        with patch("pilot.user.atlas.common.get_metadata_from_xml",
                   return_value={LFN: {"guid": XML_GUID}}), \
             patch("pilot.user.atlas.common.get_guid_from_xml", return_value=XML_GUID):
            assign_missing_guids(job)

        self.assertEqual(job.outdata[0].guid, XML_GUID)

    def test_a_guid_is_generated_when_there_is_no_xml(self):
        """Last resort, and the only behaviour the old code had."""
        job = FakeJob(self.workdir)

        assign_missing_guids(job)

        self.assertTrue(job.outdata[0].guid)
        self.assertFalse(os.path.exists(os.path.join(self.workdir, "metadata.xml")))

    def test_an_existing_guid_is_not_replaced(self):
        """Whatever the source, a guid already assigned is authoritative."""
        job = FakeJob(self.workdir, guid=XML_GUID)

        assign_missing_guids(job)

        self.assertEqual(job.outdata[0].guid, XML_GUID)

    def test_update_job_data_assigns_the_guid_for_this_report(self):
        """End to end: the wiring, not just the two functions in isolation.

        This is the behaviour job 7316132471 needed. validate_output_data() is
        patched out so that the guid can only have come from the new call, and
        verify_output_files() because it reaches the filesystem.
        """
        job = FakeJob(self.workdir, metadata=JOB_REPORT)
        job.is_eventservice = False
        job.stageout = ""
        job.nevents = 0

        with patch("pilot.user.atlas.common.get_stageout_label", return_value="all"), \
             patch("pilot.user.atlas.common.verify_output_files", return_value=True), \
             patch("pilot.user.atlas.common.validate_output_data"):
            update_job_data(job)

        self.assertTrue(job.outdata[0].guid, msg="update_job_data() must assign the missing guid")

    def test_a_broken_xml_does_not_stop_the_guid_being_assigned(self):
        """The payload exited zero; nothing here may fail the job."""
        job = FakeJob(self.workdir)

        with patch("pilot.user.atlas.common.get_metadata_from_xml",
                   side_effect=ValueError("malformed")):
            assign_missing_guids(job)

        self.assertTrue(job.outdata[0].guid)


if __name__ == "__main__":
    unittest.main()
