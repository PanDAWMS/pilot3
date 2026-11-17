#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
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

"""Tests for the per-file time stamps of Rucio traces.

The copytools use one trace report for all the files of a stage-in or stage-out. Before this change only
``catStart`` and ``timeEnd`` were set per file, while ``timeStart``, ``relativeStart``, ``transferStart``
and ``validateStart`` were set once. In the bulk trace request of job 7389762158 (MWT2_TEST, pilot
3.14.6.14), the last of 10 input files therefore had ``timeEnd - transferStart`` = 91.0 s, while its own
download and checksum check took 12.9 s. The Rucio team asked for every trace to carry its own file's
time stamps.
"""

import json
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import (
    MagicMock,
    patch
)

from pilot.copytool import gfal as gfal_copytool
from pilot.copytool import lsm as lsm_copytool
from pilot.copytool import rucio as rucio_copytool
from pilot.copytool import xrdcp as xrdcp_copytool
from pilot.util import tracereport as tracereport_module
from pilot.util.tracereport import TraceReport

START_FIELDS = ('timeStart', 'catStart', 'relativeStart', 'transferStart')

# job 7389762158: catStart and timeEnd of the 10 stage-in traces (seconds after 1793305400)
JOB_7389762158 = [(32.034, 36.789), (36.792, 40.892), (40.894, 45.056), (45.058, 48.982), (48.982, 53.130),
                  (53.130, 65.978), (65.979, 78.093), (78.094, 90.556), (90.557, 107.956), (107.957, 120.860)]
JOB_7389762158_TRANSFER_START = 29.873  # the single transferStart of all 10 traces before the change


class _Clock:
    """Controllable clock, used instead of time.time() in the copytools and the trace report."""

    def __init__(self, now: float = 1000.0):
        """Set the start time."""
        self.now = now

    def __call__(self) -> float:
        """Return the current time."""
        return self.now

    def advance(self, seconds: float) -> None:
        """Move the clock forward."""
        self.now += seconds


class _FileSpec:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """Minimal FileSpec stand-in."""

    def __init__(self, lfn: str, workdir: str):
        """Create the file spec."""
        self.scope = 'mc16_13TeV'
        self.lfn = lfn
        self.ddmendpoint = 'MWT2_DATADISK'
        self.turl = f'root://fax.mwt2.org:1094//atlasdatadisk/{lfn}'
        self.surl = os.path.join(workdir, lfn)
        self.filesize = 4
        self.guid = 'abcd-1234'
        self.dataset = 'mc16_13TeV.dataset'
        self.workdir = workdir
        self.checksum = {'adler32': '01234567'}
        self.status_code = 0
        self.status = None


class _CopytoolTestCase(unittest.TestCase):
    """Common set-up: a controllable clock, a work directory and captured traces."""

    def setUp(self):
        """Patch the clock and capture the traces."""
        self.workdir = tempfile.mkdtemp()
        self.clock = _Clock()
        self.sent = []
        self.patches = [
            patch.object(tracereport_module, 'time', SimpleNamespace(time=self.clock)),
            patch.object(TraceReport, 'send', autospec=True, side_effect=lambda report: self.sent.append(dict(report))),
        ]
        for _patch in self.patches:
            _patch.start()

    def tearDown(self):
        """Remove the patches and the work directory."""
        for _patch in reversed(self.patches):
            _patch.stop()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def start_patch(self, *args, **kwargs):
        """Start a patch that is stopped in tearDown."""
        _patch = patch.object(*args, **kwargs)
        _patch.start()
        self.patches.append(_patch)

    def files(self, count: int) -> list:
        """Return file specs for files that exist in the work directory."""
        files = []
        for index in range(count):
            fspec = _FileSpec(f'HITS.53149443._{index:06d}.pool.root.1', self.workdir)
            with open(fspec.surl, 'w', encoding='utf-8') as _file:
                _file.write('data')
            files.append(fspec)
        return files

    def trace_report(self, event_type: str) -> TraceReport:
        """Return a trace report as created at the start of stage-in or stage-out."""
        report = TraceReport(eventType=event_type, localSite='MWT2_UC_SCRATCHDISK', remoteSite='MWT2_DATADISK',
                             workdir=self.workdir)
        report.update(timeStart=self.clock(), relativeStart=self.clock(), transferStart=self.clock(),
                      validateStart=self.clock())  # what TraceReport.init(), transfer() and resolve_replicas() set
        return report

    def advance(self, seconds: float, result=None):
        """Return a side effect that moves the clock forward and returns the result."""
        def _side_effect(*_args, **_kwargs):
            self.clock.advance(seconds)
            return result
        return _side_effect

    def assert_own_start(self, traces: list, starts: list) -> None:
        """Assert that each trace's start fields are its own file's start."""
        self.assertEqual(len(traces), len(starts))
        for trace, start in zip(traces, starts):
            for field in START_FIELDS:
                self.assertEqual(trace[field], start, (trace['filename'], field))


class TestStartFile(unittest.TestCase):
    """TraceReport.start_file()."""

    def test_start_fields(self):
        """All start fields get the start time and validateStart is cleared."""
        report = TraceReport(validateStart=5.0, timeEnd=9.0)
        report.start_file(12.5)
        self.assertEqual([report[field] for field in START_FIELDS], [12.5] * 4)
        self.assertIsNone(report['validateStart'])
        self.assertEqual(report['timeEnd'], 9.0)

    def test_default_is_now(self):
        """Without a start time, the current time is used."""
        report = TraceReport()
        with patch.object(tracereport_module, 'time', SimpleNamespace(time=lambda: 42.0)):
            report.start_file()
        self.assertEqual(report['transferStart'], 42.0)


class TestRucioStageIn(_CopytoolTestCase):
    """Rucio copytool, stage-in."""

    def setUp(self):
        """Replace the download and the checksum check."""
        super().setUp()
        self.durations = []
        self.start_patch(rucio_copytool, 'time', self.clock)
        self.start_patch(rucio_copytool, '_stage_in_api', side_effect=self.download)
        self.start_patch(rucio_copytool, 'verify_catalog_checksum', side_effect=self.checksum)
        self.start_patch(rucio_copytool, 'rename_xrdlog')

    def download(self, *_args, **_kwargs):
        """Download a file (time is spent here, minus one second for the checksum check)."""
        self.clock.advance(self.durations.pop(0) - 1.0)
        return 0, []

    def checksum(self, *_args, **_kwargs):
        """Check the checksum (one second)."""
        self.clock.advance(1.0)
        return '', ''

    def test_job_7389762158(self):
        """Every trace has its own file's start; the last file's duration is 12.9 s, not 91.0 s."""
        self.clock.now = JOB_7389762158_TRANSFER_START
        report = self.trace_report('get_sm')
        files = self.files(len(JOB_7389762158))

        starts = []
        for index, (start, end) in enumerate(JOB_7389762158):
            starts.append(start)
            self.durations.append(end - start)
            if index + 1 < len(JOB_7389762158):
                self.durations[-1] += JOB_7389762158[index + 1][0] - end  # the short gap before the next file

        self.clock.advance(JOB_7389762158[0][0] - JOB_7389762158_TRANSFER_START)  # replica lookup etc.
        rucio_copytool.copy_in(files, trace_report=report, workdir=self.workdir)

        self.assert_own_start(self.sent, starts)
        last = self.sent[-1]
        self.assertAlmostEqual(last['timeEnd'] - last['transferStart'], 12.9, places=1)
        self.assertGreater(JOB_7389762158[-1][1] - JOB_7389762158_TRANSFER_START, 90.9)  # the old value
        for trace in self.sent:
            self.assertAlmostEqual(trace['timeEnd'] - trace['validateStart'], 1.0, places=6)

    def test_retried_download_attempts(self):
        """Each failed attempt starts when the previous one failed."""
        report = self.trace_report('get_sm')

        def _download(*_args, **kwargs):
            for _ in range(2):
                self.clock.advance(10.0)
                kwargs['attempts'].append({'clientState': 'FAILED', 'stateReason': 'failed', 'timeEnd': self.clock()})
            self.clock.advance(10.0)
            return 0, []

        self.start_patch(rucio_copytool, '_stage_in_api', side_effect=_download)
        bulk = []
        self.start_patch(rucio_copytool, 'send_traces',
                         side_effect=lambda traces, **_kwargs: bulk.append([dict(trace) for trace in traces]))
        start = self.clock()
        rucio_copytool.copy_in(self.files(1), trace_report=report, workdir=self.workdir)

        traces = bulk[0]
        self.assertEqual([trace['clientState'] for trace in traces], ['STAGEIN_ATTEMPT_FAILED'] * 2 + ['DONE'])
        self.assert_own_start(traces, [start, start + 10.0, start])
        self.assertEqual([trace['timeEnd'] for trace in traces], [start + 10.0, start + 20.0, start + 31.0])
        self.assertEqual([trace['validateStart'] for trace in traces], [None, None, start + 30.0])


class TestAttemptTraces(unittest.TestCase):
    """Start time stamps of the traces of failed attempts."""

    def test_attempt_starts(self):
        """The first attempt starts at the file's start, the next one when the previous one failed."""
        report = TraceReport(transferStart=100.0, validateStart=150.0)
        attempts = [{'stateReason': 'a', 'timeEnd': 110.0}, {'stateReason': 'b', 'timeEnd': 125.0},
                    {'stateReason': 'c', 'timeEnd': 140.0}]
        traces = rucio_copytool.get_attempt_traces(report, attempts, rucio_failed=True)
        self.assertEqual([trace['transferStart'] for trace in traces], [100.0, 110.0])
        self.assertEqual([trace['timeStart'] for trace in traces], [100.0, 110.0])
        self.assertEqual([trace['validateStart'] for trace in traces], [None, None])
        self.assertEqual(report['transferStart'], 100.0)  # the file's own trace is not changed

    def test_missing_end_keeps_the_previous_start(self):
        """An attempt without an end time does not move the start of the next attempt."""
        report = TraceReport(transferStart=100.0)
        traces = rucio_copytool.get_attempt_traces(report, [{'timeEnd': None}, {'timeEnd': 120.0}], rucio_failed=False)
        self.assertEqual([trace['transferStart'] for trace in traces], [100.0, 100.0])

    def test_missing_file_start(self):
        """Without a file start, the start fields are left as they are, never set to the current time."""
        report = TraceReport(validateStart=150.0)
        traces = rucio_copytool.get_attempt_traces(report, [{'timeEnd': 110.0}, {'timeEnd': 120.0}], rucio_failed=False)
        self.assertEqual([trace['transferStart'] for trace in traces], [None, 110.0])
        self.assertEqual([trace['validateStart'] for trace in traces], [None, None])


class TestRucioStageOut(_CopytoolTestCase):
    """Rucio copytool, stage-out."""

    def setUp(self):
        """Replace the upload."""
        super().setUp()
        self.start_patch(rucio_copytool, 'time', self.clock)
        self.start_patch(rucio_copytool, '_stage_out_api', side_effect=self.advance(20.0, (0, [])))
        self.start_patch(rucio_copytool, 'rename_xrdlog')
        self.start_patch(rucio_copytool, 'get_number_of_attempts', return_value=1)

    def test_own_start_and_checksum_comparison(self):
        """Every trace has its own start; validateStart is when the checksums were compared."""
        files = self.files(2)
        summary = {f'{fspec.scope}:{fspec.lfn}': {'adler32': '01234567', 'pfn': fspec.turl} for fspec in files}
        with open(os.path.join(self.workdir, 'rucio_upload.json'), 'w', encoding='utf-8') as _file:
            json.dump(summary, _file)
        start = self.clock()
        rucio_copytool.copy_out(files, trace_report=self.trace_report('put_sm'), workdir=self.workdir)
        self.assert_own_start(self.sent, [start, start + 20.0])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [start + 20.0, start + 40.0])

    def test_no_checksum_comparison(self):
        """Without the upload summary, the checksums are not compared and validateStart stays empty."""
        start = self.clock()
        rucio_copytool.copy_out(self.files(2), trace_report=self.trace_report('put_sm'), workdir=self.workdir,
                                summary=False)
        self.assert_own_start(self.sent, [start, start + 20.0])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [None, None])


class TestGfal(_CopytoolTestCase):
    """gfal copytool: the checksum is checked by gfal-copy, so validateStart stays empty."""

    def setUp(self):
        """Replace the copy command."""
        super().setUp()
        self.start_patch(gfal_copytool, 'time', self.clock)
        self.start_patch(gfal_copytool, 'check_for_gfal', return_value=True)
        self.start_patch(gfal_copytool, 'execute', side_effect=self.advance(5.0, (0, '', '')))

    def test_stage_in(self):
        """Every trace has its own start."""
        start = self.clock()
        gfal_copytool.copy_in(self.files(2), trace_report=self.trace_report('get_sm'), workdir=self.workdir)
        self.assert_own_start(self.sent, [start, start + 5.0])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [None, None])

    def test_stage_out(self):
        """Every trace has its own start."""
        start = self.clock()
        gfal_copytool.copy_out(self.files(2), trace_report=self.trace_report('put_sm'), workdir=self.workdir)
        self.assert_own_start(self.sent, [start, start + 5.0])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [None, None])


class TestLsm(_CopytoolTestCase):
    """lsm copytool."""

    def setUp(self):
        """Replace the copy command and the checksum check."""
        super().setUp()
        self.start_patch(lsm_copytool, 'time', self.clock)
        self.start_patch(lsm_copytool, 'move', side_effect=self.advance(5.0, (0, '', '')))
        self.start_patch(lsm_copytool, 'verify_catalog_checksum', side_effect=self.advance(1.0, ('', '')))

    def test_stage_in(self):
        """Every trace has its own start, and validateStart is when the checksum check started."""
        start = self.clock()
        lsm_copytool.copy_in(self.files(2), trace_report=self.trace_report('get_sm'), workdir=self.workdir)
        self.assert_own_start(self.sent, [start, start + 6.0])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [start + 5.0, start + 11.0])

    def test_stage_out(self):
        """Every trace has its own start; lsm does not check the checksum after the upload."""
        start = self.clock()
        ddmconf = {'MWT2_DATADISK': MagicMock(token='ATLASDATADISK')}
        lsm_copytool.copy_out(self.files(2), trace_report=self.trace_report('put_sm'), workdir=self.workdir,
                              ddmconf=ddmconf)
        self.assert_own_start(self.sent, [start, start + 5.0])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [None, None])


class TestXrdcp(_CopytoolTestCase):
    """xrdcp copytool."""

    def setUp(self):
        """Replace the copy command and the checksum check."""
        super().setUp()
        self.start_patch(xrdcp_copytool, 'time', self.clock)
        self.start_patch(xrdcp_copytool, '_resolve_checksum_option', return_value='')
        self.start_patch(xrdcp_copytool, '_stagefile', side_effect=self.advance(5.0, (4, '01234567', 'adler32')))
        self.checksum = MagicMock(side_effect=self.advance(1.0, ('', '')))
        self.start_patch(xrdcp_copytool, 'verify_catalog_checksum', self.checksum)

    def test_stage_in(self):
        """Every trace has its own start, and validateStart is when the checksum check started."""
        start = self.clock()
        xrdcp_copytool.copy_in(self.files(2), trace_report=self.trace_report('get_sm'), workdir=self.workdir)
        self.assert_own_start(self.sent, [start, start + 6.0])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [start + 5.0, start + 11.0])

    def test_stage_out_checksum_failure(self):
        """The trace of a failed checksum check after an upload has the time the check started."""
        self.checksum.side_effect = self.advance(1.0, ('AD_MISMATCH', 'checksum mismatch'))
        start = self.clock()
        with self.assertRaises(Exception):
            xrdcp_copytool.copy_out(self.files(1), trace_report=self.trace_report('put_sm'), workdir=self.workdir)
        # the DONE trace is sent before the checksum check, the mismatch trace after it
        self.assertEqual([trace['clientState'] for trace in self.sent], ['DONE', 'AD_MISMATCH'])
        self.assert_own_start(self.sent, [start, start])
        self.assertEqual([trace['validateStart'] for trace in self.sent], [None, start + 5.0])


if __name__ == '__main__':
    unittest.main()
