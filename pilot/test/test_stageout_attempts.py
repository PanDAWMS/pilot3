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
# Authors
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Tests for stage-out retries, the traces of failed upload attempts and pilot telemetry schema version 2.

With ``--stageout-attempts`` defaulting to 2, a file whose first upload fails and whose second one succeeds
looked like a clean success everywhere. The pilot now:

- counts the upload attempts per file (also in middleware containers);
- sends a STAGEOUT_ATTEMPT_FAILED trace for every failed attempt that was followed by another one, as for
  retried downloads;
- reports only the error of the attempt that decided the outcome (the Rucio traces of earlier attempts used to
  stay in front of it);
- reports the retried files and a summary of the job's Rucio traces in the pilot telemetry (schema version 2).

The fake upload client follows Rucio's UploadClient.upload() (master, ecae535): one trace per file and call,
appended to traces_copy_out before the transfer, FAILED with the error of the last protocol tried, and
'None of the given files have been uploaded.' when no file was uploaded.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pilot.common.exception import PilotException
from pilot.control import job as job_module
from pilot.copytool import rucio as rucio_copytool
from pilot.info import FileSpec
from pilot.scripts import stageout as stageout_script
from pilot.util import middleware, telemetry, tracesender
from pilot.util.config import config
from pilot.util.constants import PILOT_ATTRIBUTES_SCHEMA_VERSION
from pilot.util.tracereport import GENERIC_TRACE_ERROR, TraceReport
from pilot.util.tracesender import (
    BatchResult,
    TraceSender,
    TraceSettings,
    TraceSnapshot,
    TraceSummary,
    get_or_create_sender,
    get_sender,
    get_trace_snapshot,
    reset_registry,
)
from pilot.util.tracesummary import TraceRecord, TraceStats

NO_FILES_UPLOADED = 'None of the given files have been uploaded.'
INCIDENT_REASON = ('Error on XrdCl::CopyProcess::Run(): [ERROR] Server responded with an error: [3012] '
                   'Internal timeout (destination)')


# ---------------------------------------------------------------------------
# fake rucio upload client
# ---------------------------------------------------------------------------


def failed(reason: str, protocol: str = 'root', message: str = NO_FILES_UPLOADED) -> tuple:
    """Return a script step: the upload fails after Rucio created the trace.

    Args:
        reason: stateReason set by Rucio.
        protocol: Last protocol tried.
        message: Message of the exception raised by the client.

    Returns:
        Script step.
    """
    return ('failed', reason, protocol, message)


def done(protocol: str = 'davs', adler32: str = '') -> tuple:
    """Return a script step: the upload succeeds.

    Args:
        protocol: Protocol used.
        adler32: Remote checksum written to the summary file (if one is asked for).

    Returns:
        Script step.
    """
    return ('done', protocol, adler32)


def early(error: BaseException) -> tuple:
    """Return a script step: the client raises before creating the trace.

    Args:
        error: Exception to raise.

    Returns:
        Script step.
    """
    return ('early', error)


class _FakeUploadClient:  # pylint: disable=too-few-public-methods
    """Rucio upload client playing a script, one step per upload() call."""

    script: list = []
    calls: list = []

    def __init__(self, *args, **kwargs):
        """Create the client (arguments are ignored)."""
        del args, kwargs
        self.tracing = True

    def upload(self, items, summary_file_path=None, traces_copy_out=None, ignore_availability=False):
        """Play the next script step."""
        del ignore_availability
        step = _FakeUploadClient.script[len(_FakeUploadClient.calls)]
        item = items[0]
        _FakeUploadClient.calls.append(dict(item))
        if step[0] == 'early':
            raise step[1]
        trace = {'clientState': None, 'stateReason': '', 'remoteSite': item['rse'], 'protocol': None}
        traces_copy_out.append(trace)
        if step[0] == 'done':
            trace.update(clientState='DONE', protocol=step[1])
            if summary_file_path:
                name = os.path.basename(item['path'])
                summary = {f"{item['did_scope']}:{name}": {'pfn': f'{step[1]}://se/{name}', 'adler32': step[2]}}
                with open(summary_file_path, 'w', encoding='utf-8') as _file:
                    json.dump(summary, _file)
            return 0
        trace.update(clientState='FAILED', stateReason=step[1], protocol=step[2])
        raise RuntimeError(step[3])


class _RucioModulesTestCase(unittest.TestCase):
    """Test case with fake rucio client modules, a work directory and the number of stage-out attempts."""

    attempts = '2'

    def setUp(self):
        """Install the fake rucio modules and set PILOT_STAGEOUT_ATTEMPTS."""
        self.workdir = tempfile.mkdtemp()
        _FakeUploadClient.script = []
        _FakeUploadClient.calls = []
        client_module = types.ModuleType('rucio.client')
        client_module.Client = MagicMock()
        upload_module = types.ModuleType('rucio.client.uploadclient')
        upload_module.UploadClient = _FakeUploadClient
        self.modules = patch.dict(sys.modules, {'rucio': types.ModuleType('rucio'), 'rucio.client': client_module,
                                                'rucio.client.uploadclient': upload_module})
        self.modules.start()
        self.env = patch.dict(os.environ, {'PILOT_STAGEOUT_ATTEMPTS': self.attempts})
        self.env.start()

    def tearDown(self):
        """Remove the fake modules and the work directory."""
        self.env.stop()
        self.modules.stop()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def fspec(self, lfn: str = 'log.123._000001.job.log.tgz.1') -> FileSpec:
        """Return an output file spec.

        Args:
            lfn: Logical file name.

        Returns:
            File spec.
        """
        fspec = FileSpec(filetype='output')
        fspec.lfn = lfn
        fspec.scope = 'mc23_13p6TeV'
        fspec.ddmendpoint = 'BNL-OSG2_DATADISK'
        fspec.guid = 'abcd-1234'
        fspec.filesize = 1000
        fspec.workdir = self.workdir
        return fspec


# ---------------------------------------------------------------------------
# _stage_out_api(): attempts, per-attempt traces, recorded failures
# ---------------------------------------------------------------------------


class TestStageOutApi(_RucioModulesTestCase):
    """The upload attempts made by _stage_out_api()."""

    def stage_out(self, script: list, fspec: FileSpec = None, attempts: list = None,
                  trace_report_out: list = None) -> tuple:
        """Run _stage_out_api() with the given upload script.

        Args:
            script: One step per upload() call.
            fspec: File spec (default: a new one).
            attempts: List for the failed attempts (default: a new list).
            trace_report_out: List for the Rucio traces (default: a new list).

        Returns:
            (exit code, Rucio traces, recorded attempts, file spec).
        """
        _FakeUploadClient.script = script
        _FakeUploadClient.calls = []
        fspec = fspec or self.fspec()
        attempts = [] if attempts is None else attempts
        trace_report_out = [] if trace_report_out is None else trace_report_out
        ec, traces = rucio_copytool._stage_out_api(fspec, '', {}, trace_report_out, 100, '',
                                                   attempts=attempts)
        return ec, traces, attempts, fspec

    def test_success_at_first_attempt(self):
        """One attempt, nothing recorded."""
        ec, traces, attempts, fspec = self.stage_out([done()])
        self.assertEqual((ec, fspec.stageout_attempts, attempts), (0, 1, []))
        self.assertEqual([trace['clientState'] for trace in traces], ['DONE'])

    def test_success_after_failed_attempt(self):
        """The failed attempt is recorded with Rucio's reason, and only the last attempt's trace is kept."""
        ec, traces, attempts, fspec = self.stage_out([failed(INCIDENT_REASON, 'root'), done('davs')])
        self.assertEqual((ec, fspec.stageout_attempts, len(_FakeUploadClient.calls)), (0, 2, 2))
        self.assertEqual(len(attempts), 1)
        self.assertEqual((attempts[0]['stateReason'], attempts[0]['protocol'], attempts[0]['remoteSite']),
                         (INCIDENT_REASON, 'root', 'BNL-OSG2_DATADISK'))
        self.assertIsNotNone(attempts[0]['timeEnd'])
        self.assertEqual([(trace['clientState'], trace['protocol']) for trace in traces], [('DONE', 'davs')])

    def test_failure_reports_the_last_attempt(self):
        """After all attempts failed, the error read from the traces is the last one, not the first one."""
        trace_report_out = []
        with self.assertRaises(PilotException):
            self.stage_out([failed('first error'), failed('second error', 'davs')], trace_report_out=trace_report_out)
        self.assertEqual([trace['stateReason'] for trace in trace_report_out], ['second error'])
        self.assertEqual(rucio_copytool.get_protocol(trace_report_out), 'davs')

    def test_last_attempt_is_not_recorded(self):
        """Only failed attempts followed by another attempt are recorded."""
        attempts = []
        fspec = self.fspec()
        with self.assertRaises(PilotException):
            self.stage_out([failed('first error'), failed('second error')], fspec=fspec, attempts=attempts)
        self.assertEqual([attempt['stateReason'] for attempt in attempts], ['first error'])
        self.assertEqual(fspec.stageout_attempts, 2)

    def test_last_attempt_without_trace_raises_its_own_error(self):
        """A last attempt that fails before Rucio creates its trace is not mistaken for the previous attempt."""
        trace_report_out = []
        with self.assertRaises(ValueError) as ctx:
            self.stage_out([failed('first error'), early(ValueError('RSE not found'))], trace_report_out=trace_report_out)
        self.assertEqual(str(ctx.exception), 'RSE not found')
        self.assertEqual(trace_report_out, [])

    def test_attempt_without_trace(self):
        """Without a Rucio trace, the reason is the exception raised by the client."""
        _, _, attempts, _ = self.stage_out([early(ValueError('RSE not found')), done()])
        self.assertEqual(attempts[0]['stateReason'], 'ValueError: RSE not found')
        self.assertIsNone(attempts[0]['protocol'])

    def test_unbound_local_error_is_retried(self):
        """The known summary bug of the upload client counts as a failed attempt too."""
        _, _, attempts, fspec = self.stage_out([early(UnboundLocalError('summary')), done()])
        self.assertEqual(attempts[0]['stateReason'], 'UnboundLocalError: summary')
        self.assertEqual(fspec.stageout_attempts, 2)

    def test_unbound_local_error_at_last_attempt(self):
        """The known summary bug at the last attempt gives exit code -1, as before."""
        ec, _, attempts, _ = self.stage_out([early(UnboundLocalError('a')), early(UnboundLocalError('b'))])
        self.assertEqual(ec, -1)
        self.assertEqual(len(attempts), 1)

    def test_count_accumulates(self):
        """A second stage-out of the same file (alternative stage-out) adds to the count."""
        fspec = self.fspec()
        with self.assertRaises(PilotException):
            self.stage_out([failed('a'), failed('b')], fspec=fspec)
        self.stage_out([done()], fspec=fspec)
        self.assertEqual(fspec.stageout_attempts, 3)

    def test_without_attempt_list(self):
        """Failed attempts are still retried and logged when there is no list to record them in."""
        _FakeUploadClient.script = [failed('a'), done()]
        with self.assertLogs(rucio_copytool.logger, level='WARNING') as logs:
            ec, _ = rucio_copytool._stage_out_api(self.fspec(), '', {}, [], 100, '')
        self.assertEqual(ec, 0)
        self.assertTrue(any('will be retried: a' in line for line in logs.output))

    def test_trace_list_object_is_kept(self):
        """The caller's list is emptied in place, not replaced."""
        trace_report_out = [{'stateReason': 'left over'}]
        _, traces, _, _ = self.stage_out([failed('a'), done()], trace_report_out=trace_report_out)
        self.assertIs(traces, trace_report_out)
        self.assertEqual([trace['clientState'] for trace in trace_report_out], ['DONE'])


class TestSingleAttempt(_RucioModulesTestCase):
    """With one attempt, nothing is recorded and the behaviour is unchanged."""

    attempts = '1'

    def test_failure(self):
        """The only attempt is the final failure."""
        _FakeUploadClient.script = [failed('only error')]
        attempts = []
        fspec = self.fspec()
        with self.assertRaises(PilotException):
            rucio_copytool._stage_out_api(fspec, '', {}, [], 100, '', attempts=attempts)
        self.assertEqual((attempts, fspec.stageout_attempts), ([], 1))


class TestUploadAttemptSnapshot(unittest.TestCase):
    """get_upload_attempt_snapshot()."""

    def test_rucio_reason_wins(self):
        """Rucio's reason is preferred to the exception text."""
        snapshot = rucio_copytool.get_upload_attempt_snapshot(
            [{'stateReason': 'File already exists', 'protocol': 'root', 'remoteSite': 'RSE'}], RuntimeError(NO_FILES_UPLOADED))
        self.assertEqual((snapshot['stateReason'], snapshot['protocol'], snapshot['remoteSite']),
                         ('File already exists', 'root', 'RSE'))

    def test_empty_reason_falls_back_to_the_error(self):
        """A trace without a reason gives the exception text."""
        snapshot = rucio_copytool.get_upload_attempt_snapshot([{'stateReason': '', 'protocol': 'root'}], OSError('boom'))
        self.assertEqual((snapshot['stateReason'], snapshot['protocol']), ('OSError: boom', 'root'))

    def test_latest_trace_is_used(self):
        """The trace of the attempt is the latest one in the list."""
        snapshot = rucio_copytool.get_upload_attempt_snapshot([{'stateReason': 'old'}, {'stateReason': 'new'}], OSError())
        self.assertEqual(snapshot['stateReason'], 'new')

    def test_snapshot_is_a_copy(self):
        """Later changes to Rucio's trace dictionary do not change a recorded attempt."""
        trace = {'stateReason': 'first'}
        snapshot = rucio_copytool.get_upload_attempt_snapshot([trace], OSError())
        trace['stateReason'] = 'second'
        self.assertEqual(snapshot['stateReason'], 'first')


# ---------------------------------------------------------------------------
# copy_out(): attempt traces sent with the file's trace
# ---------------------------------------------------------------------------


class _CopyOutTestCase(_RucioModulesTestCase):
    """Test case running copy_out() with the traces captured instead of sent."""

    def setUp(self):
        """Capture the traces instead of sending them."""
        super().setUp()
        self.sent_single = []
        self.sent_bulk = []
        self.patches = [
            patch.object(TraceReport, 'send', autospec=True, side_effect=lambda report: self.sent_single.append(dict(report))),
            patch.object(rucio_copytool, 'send_traces',
                         side_effect=lambda traces, job_id=None, workdir='': self.sent_bulk.append([dict(t) for t in traces])),
            patch.object(rucio_copytool, 'rename_xrdlog'),
        ]
        for _patch in self.patches:
            _patch.start()

    def tearDown(self):
        """Stop capturing."""
        for _patch in reversed(self.patches):
            _patch.stop()
        super().tearDown()

    def copy_out(self, script: list, summary: bool = False) -> FileSpec:
        """Run the rucio copytool stage-out for one file.

        Args:
            script: One step per upload() call.
            summary: Whether the upload client writes a summary file (with the remote checksum).

        Returns:
            The file spec.
        """
        _FakeUploadClient.script = script
        fspec = self.fspec()
        fspec.surl = os.path.join(self.workdir, fspec.lfn)
        fspec.checksum = {'adler32': '0a0b0c0d'}
        trace_report = TraceReport(eventType='put_sm', localSite='BNL-OSG2', remoteSite='BNL-OSG2_DATADISK',
                                   appid='7380000000', workdir=self.workdir)
        rucio_copytool.copy_out([fspec], trace_report=trace_report, workdir=self.workdir, summary=summary)
        return fspec


class TestCopyOutTraces(_CopyOutTestCase):
    """Failed upload attempts get a STAGEOUT_ATTEMPT_FAILED trace, sent with the file's own trace."""

    def test_no_retry(self):
        """Without failed attempts, the file's trace is sent on its own, as before."""
        fspec = self.copy_out([done()])
        self.assertEqual([trace['clientState'] for trace in self.sent_single], ['DONE'])
        self.assertEqual(self.sent_bulk, [])
        self.assertEqual(fspec.status, 'transferred')

    def test_success_after_retry(self):
        """The failed attempt and the final DONE trace are sent together."""
        fspec = self.copy_out([failed(INCIDENT_REASON, 'root'), done('davs')])
        self.assertEqual(self.sent_single, [])
        self.assertEqual(len(self.sent_bulk), 1)
        traces = self.sent_bulk[0]
        self.assertEqual([(trace['clientState'], trace['stateReason']) for trace in traces],
                         [('STAGEOUT_ATTEMPT_FAILED', INCIDENT_REASON), ('DONE', 'OK')])
        self.assertEqual([trace['protocol'] for trace in traces], ['root', 'davs'])
        self.assertEqual([trace['eventType'] for trace in traces], ['put_sm', 'put_sm'])
        self.assertEqual(traces[0]['filename'], fspec.lfn)
        self.assertIsNotNone(traces[0]['timeEnd'])
        self.assertEqual((fspec.status, fspec.stageout_attempts), ('transferred', 2))

    def test_failure_after_all_attempts(self):
        """The first attempt gets its own trace; the last one is the file's failure trace."""
        with self.assertRaises(PilotException) as ctx:
            self.copy_out([failed('first error'), failed('second error', 'davs')])
        self.assertIn('second error', str(ctx.exception))
        self.assertNotIn('first error', str(ctx.exception))
        self.assertEqual(len(self.sent_bulk), 1)
        traces = self.sent_bulk[0]
        self.assertEqual([trace['clientState'] for trace in traces][0], 'STAGEOUT_ATTEMPT_FAILED')
        self.assertEqual([trace['stateReason'] for trace in traces], ['first error', 'second error'])
        self.assertEqual(traces[1]['protocol'], 'davs')

    def test_last_attempt_fails_before_rucio_trace(self):
        """The attempt traces are also sent when the last attempt fails without a Rucio trace."""
        with self.assertRaises(PilotException) as ctx:
            self.copy_out([failed('first error'), early(ValueError('RSE not found'))])
        self.assertIn('RSE not found', str(ctx.exception))
        self.assertEqual(len(self.sent_bulk), 1)
        self.assertEqual([trace['clientState'] for trace in self.sent_bulk[0]][0], 'STAGEOUT_ATTEMPT_FAILED')
        self.assertEqual(len(self.sent_bulk[0]), 2)

    def test_last_attempt_with_other_error(self):
        """A last attempt with a Rucio reason and another error goes through the missed-failure check."""
        with self.assertRaises(PilotException) as ctx:
            self.copy_out([failed('first error'), failed('connection reset', 'davs', message='upload broke')])
        self.assertIn('connection reset', str(ctx.exception))
        self.assertEqual([trace['stateReason'] for trace in self.sent_bulk[0]], ['first error', 'connection reset'])

    def test_checksum_mismatch_after_retry(self):
        """A checksum mismatch after a retry sends the attempt trace with the mismatch trace."""
        with self.assertRaises(PilotException):
            self.copy_out([failed('first error'), done('davs', adler32='ffffffff')], summary=True)
        self.assertEqual(len(self.sent_bulk), 1)
        self.assertEqual([trace['clientState'] for trace in self.sent_bulk[0]], ['STAGEOUT_ATTEMPT_FAILED', 'AD_MISMATCH'])

    def test_summary_after_retry(self):
        """With a summary file and a matching checksum, the retried file is transferred."""
        fspec = self.copy_out([failed('first error'), done('davs', adler32='0a0b0c0d')], summary=True)
        self.assertEqual((fspec.status, fspec.turl), ('transferred', f'davs://se/{fspec.lfn}'))
        self.assertEqual([trace['clientState'] for trace in self.sent_bulk[0]], ['STAGEOUT_ATTEMPT_FAILED', 'DONE'])


class TestCopyOutSingleAttempt(_CopyOutTestCase):
    """With one attempt, the traces are sent as before."""

    attempts = '1'

    def test_success(self):
        """The file's trace is sent on its own."""
        self.copy_out([done()])
        self.assertEqual([trace['clientState'] for trace in self.sent_single], ['DONE'])
        self.assertEqual(self.sent_bulk, [])

    def test_failure(self):
        """One failed attempt that is also the final failure: only the final trace."""
        with self.assertRaises(PilotException):
            self.copy_out([failed('only error')])
        self.assertEqual(self.sent_bulk, [])
        self.assertEqual([trace['stateReason'] for trace in self.sent_single], ['only error'])


class TestAttemptTraceState(unittest.TestCase):
    """get_attempt_traces() sets the given state."""

    def test_states(self):
        """Stage-in keeps its state by default; stage-out passes its own."""
        trace_report = TraceReport(eventType='put_sm', localSite='A', remoteSite='B')
        attempts = [{'stateReason': 'x', 'protocol': 'root', 'remoteSite': 'B', 'timeEnd': 1}]
        self.assertEqual(rucio_copytool.get_attempt_traces(trace_report, attempts, False)[0]['clientState'],
                         'STAGEIN_ATTEMPT_FAILED')
        traces = rucio_copytool.get_attempt_traces(trace_report, attempts, False, state='STAGEOUT_ATTEMPT_FAILED')
        self.assertEqual(traces[0]['clientState'], 'STAGEOUT_ATTEMPT_FAILED')
        self.assertIsNot(traces[0], trace_report)


# ---------------------------------------------------------------------------
# middleware containers: the count is passed back to the pilot
# ---------------------------------------------------------------------------


class TestContainerisedCount(unittest.TestCase):
    """The number of attempts made in the stage-out container reaches the pilot's file spec."""

    def setUp(self):
        """Create a work directory."""
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Remove the work directory."""
        shutil.rmtree(self.workdir, ignore_errors=True)

    @staticmethod
    def entry(attempts: int | None = None) -> list:
        """Return a stage-out status dictionary entry.

        Args:
            attempts: Number of attempts (no 7th value if None).

        Returns:
            Entry.
        """
        return stageout_script.add_to_dictionary({}, 'f', 'transferred', 0, 'surl', 'turl', 'ad', 10,
                                                 attempts=attempts)['f']

    def test_script_adds_the_count_for_files_only(self):
        """The script adds the count to file entries; the error entry keeps six values."""
        self.assertEqual(self.entry(2), ['transferred', 0, 'surl', 'turl', 'ad', 10, 2])
        self.assertEqual(len(self.entry()), 6)
        self.assertEqual(self.entry(0)[6], 0)

    def test_script_reports_the_file(self):
        """The script reports the file's status with its number of attempts (0 if not counted)."""
        fspec = FileSpec(filetype='output')
        fspec.lfn, fspec.status, fspec.surl, fspec.turl, fspec.filesize = 'f', 'transferred', 'surl', 'turl', 10
        fspec.checksum = {'adler32': 'ad'}
        fspec.stageout_attempts = 2
        dictionary = stageout_script.add_file_to_dictionary({}, fspec)
        self.assertEqual(dictionary, {'f': ['transferred', 0, 'surl', 'turl', 'ad', 10, 2]})
        fspec.stageout_attempts = None
        self.assertEqual(stageout_script.add_file_to_dictionary({}, fspec)['f'][6], 0)

    def test_count_added(self):
        """The count is added to the file's count."""
        fspec = SimpleNamespace(lfn='f', stageout_attempts=1)
        middleware.add_stageout_attempts(fspec, self.entry(2))
        self.assertEqual(fspec.stageout_attempts, 3)

    def test_old_script(self):
        """An entry from an older script leaves the count unchanged."""
        fspec = SimpleNamespace(lfn='f', stageout_attempts=0)
        middleware.add_stageout_attempts(fspec, self.entry())
        self.assertEqual(fspec.stageout_attempts, 0)

    def test_malformed_count(self):
        """A malformed count is logged and ignored."""
        fspec = SimpleNamespace(lfn='f', stageout_attempts=0)
        entry = self.entry(2)
        entry[6] = 'two'
        with self.assertLogs(middleware.logger, level='WARNING'):
            middleware.add_stageout_attempts(fspec, entry)
        self.assertEqual(fspec.stageout_attempts, 0)

    def test_missing_count_is_zero(self):
        """None counts as no attempts, and a file spec without the attribute starts from zero."""
        fspec = SimpleNamespace(lfn='f')
        middleware.add_stageout_attempts(fspec, self.entry(None) + [None])
        self.assertEqual(fspec.stageout_attempts, 0)

    def test_read_from_status_file(self):
        """handle_updated_job_object() reads the count from the stage-out status file."""
        fspec = FileSpec(filetype='output')
        fspec.lfn = 'f'
        dictionary = {'f': self.entry(2), 'error': ['', 0, None, None, None, None]}
        with open(os.path.join(self.workdir, config.Container.stageout_status_dictionary), 'w', encoding='utf-8') as _file:
            json.dump(dictionary, _file)
        job = SimpleNamespace(workdir=self.workdir)
        middleware.handle_updated_job_object(job, [fspec], label='stage-out')
        self.assertEqual((fspec.status, fspec.turl, fspec.stageout_attempts), ('transferred', 'turl', 2))

    def test_stage_in_ignores_the_count(self):
        """Stage-in entries are not read for a count."""
        fspec = FileSpec(filetype='input')
        fspec.lfn = 'f'
        dictionary = {'f': ['transferred', 0, 'turl', 'RSE', 'x', 1, 5], 'error': ['', 0]}
        with open(os.path.join(self.workdir, config.Container.stagein_status_dictionary), 'w', encoding='utf-8') as _file:
            json.dump(dictionary, _file)
        middleware.handle_updated_job_object(SimpleNamespace(workdir=self.workdir), [fspec], label='stage-in')
        self.assertEqual(fspec.stageout_attempts, 0)


# ---------------------------------------------------------------------------
# trace sender: request errors and snapshot
# ---------------------------------------------------------------------------


def make_sender(workdir: str) -> TraceSender:
    """Return a trace sender with test settings.

    Args:
        workdir: Work directory.

    Returns:
        Sender.
    """
    settings = TraceSettings(batch_size=100, max_parallel=2, timeout=10, slow_threshold=5, final_wait=10, batch_wait=0)
    return TraceSender('7380000000', workdir, settings=settings)


class TestSenderSnapshot(unittest.TestCase):
    """Request errors per code and the snapshot for the telemetry."""

    def setUp(self):
        """Create a work directory and clear the registry."""
        self.workdir = tempfile.mkdtemp()
        os.environ.pop('RUCIO_TRACE_ERROR', None)
        reset_registry()

    def tearDown(self):
        """Clean up."""
        reset_registry()
        os.environ.pop('RUCIO_TRACE_ERROR', None)
        shutil.rmtree(self.workdir, ignore_errors=True)

    @staticmethod
    def batch(n: int) -> list:
        """Return a batch of n (trace, record) pairs.

        Args:
            n: Number of traces.

        Returns:
            Batch.
        """
        return [({}, TraceRecord('get_sm', 'DONE', f'f{i}', 'RSE')) for i in range(n)]

    def test_request_errors_counted_per_code(self):
        """Each failed request is counted under its HTTP status code, or the generic code without one."""
        sender = make_sender(self.workdir)
        results = [BatchResult(2, False, 0.1, 'HTTP Error 502: Bad Gateway'),
                   BatchResult(3, False, 0.1, 'HTTP Error 502: Bad Gateway'),
                   BatchResult(1, False, 0.1, 'timed out'),
                   BatchResult(4, True, 0.1, 'Created')]
        with self.assertLogs(tracesender.logger, level='WARNING'):
            sender._record(results, [self.batch(2), self.batch(3), self.batch(1), self.batch(4)])
        snapshot = sender.snapshot()
        self.assertEqual(snapshot.request_errors, {502: 2, GENERIC_TRACE_ERROR: 1})
        self.assertEqual((snapshot.summary.failed, snapshot.summary.sent, snapshot.stats.requests), (6, 4, 4))

    def test_snapshot_is_a_copy(self):
        """Changing a snapshot does not change the sender."""
        sender = make_sender(self.workdir)
        sender._records.append(TraceRecord('put_sm', 'DONE', 'f', 'RSE'))
        snapshot = sender.snapshot()
        snapshot.records[0].result = 'changed'
        snapshot.request_errors[1] = 5
        snapshot.summary.sent = 99
        again = sender.snapshot()
        self.assertEqual((again.records[0].result, again.request_errors, again.summary.sent), ('queued', {}, 0))

    def test_registry_snapshot_only_when_finished(self):
        """get_trace_snapshot() returns nothing before finish() or without a sender."""
        job = SimpleNamespace(jobid='7380000000', workdir=self.workdir)
        self.assertIsNone(get_trace_snapshot(job.jobid))
        sender = get_or_create_sender(job)
        self.assertIsNone(get_trace_snapshot(job.jobid))
        sender.finish()
        snapshot = get_trace_snapshot(job.jobid)
        self.assertIsInstance(snapshot, TraceSnapshot)
        self.assertEqual(snapshot.records, ())


# ---------------------------------------------------------------------------
# telemetry schema version 2
# ---------------------------------------------------------------------------


def make_snapshot(records: list = (), request_errors: dict = None, **summary) -> TraceSnapshot:
    """Return a trace snapshot.

    Args:
        records: Trace records.
        request_errors: Failed requests per code.
        **summary: TraceSummary fields.

    Returns:
        Snapshot.
    """
    stats = TraceStats(requests=2, sending_time=60.6, final_wait=0.4, parallel=1)
    return TraceSnapshot(summary=TraceSummary(**summary), stats=stats, records=tuple(records),
                         request_errors=dict(request_errors or {}))


def record(event: str = 'get_sm', state: str = 'DONE', reason: str = '') -> TraceRecord:
    """Return a trace record.

    Args:
        event: eventType.
        state: clientState.
        reason: Shortened stateReason ('' for OK).

    Returns:
        Record.
    """
    return TraceRecord(event, state, 'file', 'RSE', reason=reason, result='sent')


class TestTelemetryCounters(unittest.TestCase):
    """The stage-out counters of schema version 2."""

    @staticmethod
    def counters(outdata: list = (), logdata: list = (), args: SimpleNamespace | None = None) -> dict:
        """Return the counters of a job.

        Args:
            outdata: Output files.
            logdata: Log files.
            args: Pilot arguments.

        Returns:
            Counters.
        """
        job = SimpleNamespace(indata=[], outdata=list(outdata), logdata=list(logdata))
        return telemetry.get_counters(job, args)

    def test_schema_version(self):
        """The schema version is 2."""
        self.assertEqual(PILOT_ATTRIBUTES_SCHEMA_VERSION, 2)

    def test_retried_files(self):
        """Output and log files with more than one attempt are counted."""
        outdata = [SimpleNamespace(stageout_attempts=2), SimpleNamespace(stageout_attempts=1),
                   SimpleNamespace(stageout_attempts=0), SimpleNamespace(), SimpleNamespace(stageout_attempts='x')]
        logdata = [SimpleNamespace(stageout_attempts=3)]
        self.assertEqual(self.counters(outdata, logdata)['n_stageout_retried'], 2)

    def test_attempts_max(self):
        """The configured number of attempts is reported when known."""
        self.assertEqual(self.counters(args=SimpleNamespace(stageout_attempts=2))['stageout_attempts_max'], 2)
        self.assertNotIn('stageout_attempts_max', self.counters(args=SimpleNamespace()))
        self.assertNotIn('stageout_attempts_max', self.counters())

    def test_input_files_are_not_counted(self):
        """Input files are never counted as retried."""
        job = SimpleNamespace(indata=[SimpleNamespace(stageout_attempts=5)], outdata=[], logdata=[])
        self.assertEqual(telemetry.get_counters(job)['n_stageout_retried'], 0)


class TestTelemetryTraces(unittest.TestCase):
    """The traces section of schema version 2."""

    def test_no_sender(self):
        """Without a trace sender, the section is empty."""
        self.assertEqual(telemetry.get_traces(None), {})

    def test_counts_and_requests(self):
        """Delivery counts and request statistics; times are rounded to seconds."""
        traces = telemetry.get_traces(make_snapshot([record()], sent=4, failed=2, abandoned=1, invalid=3, degraded=True))
        expected = {'sent': 4, 'failed': 2, 'abandoned': 1, 'invalid': 3, 'requests': 2, 'parallel': 1,
                    'degraded': True, 'sending_time': 61, 'final_wait': 0}
        self.assertEqual({key: traces[key] for key in expected}, expected)
        self.assertIs(traces['degraded'], True)

    def test_no_traces(self):
        """A sender without traces gives the counts only."""
        traces = telemetry.get_traces(make_snapshot())
        self.assertEqual(set(traces), {'sent', 'failed', 'abandoned', 'invalid', 'requests', 'parallel', 'degraded',
                                       'sending_time', 'final_wait'})

    def test_request_errors(self):
        """Failed requests per code, with string keys in code order."""
        traces = telemetry.get_traces(make_snapshot(request_errors={502: 2, 1: 1}))
        self.assertEqual(list(traces['request_errors'].items()), [('1', 1), ('502', 2)])

    def test_states(self):
        """Traces are counted per eventType and clientState."""
        records = [record('get_sm', 'DONE'), record('get_sm', 'DONE'), record('get_sm', 'STAGEIN_ATTEMPT_FAILED', 'x'),
                   record('put_sm', 'STAGEOUT_ATTEMPT_FAILED', 'y'), record('put_sm', 'DONE')]
        self.assertEqual(telemetry.get_traces(make_snapshot(records))['states'],
                         {'get_sm': {'DONE': 2, 'STAGEIN_ATTEMPT_FAILED': 1},
                          'put_sm': {'STAGEOUT_ATTEMPT_FAILED': 1, 'DONE': 1}})

    def test_reasons(self):
        """Non-OK reasons are counted per event, state and reason, most frequent first."""
        records = [record('put_sm', 'STAGEOUT_ATTEMPT_FAILED', 'File already exists'),
                   record('get_sm', 'STAGEIN_ATTEMPT_FAILED', 'checksum'),
                   record('get_sm', 'STAGEIN_ATTEMPT_FAILED', 'checksum'),
                   record('get_sm', 'DONE')]
        traces = telemetry.get_traces(make_snapshot(records))
        self.assertEqual(traces['reasons'], [
            {'event': 'get_sm', 'state': 'STAGEIN_ATTEMPT_FAILED', 'reason': 'checksum', 'count': 2},
            {'event': 'put_sm', 'state': 'STAGEOUT_ATTEMPT_FAILED', 'reason': 'File already exists', 'count': 1}])
        self.assertEqual(traces['n_reasons'], 2)

    def test_same_reason_in_different_states(self):
        """The same reason in another state is another combination."""
        records = [record('put_sm', 'STAGEOUT_ATTEMPT_FAILED', 'r'), record('put_sm', 'FAILED', 'r')]
        self.assertEqual(telemetry.get_traces(make_snapshot(records))['n_reasons'], 2)

    def test_reasons_are_capped(self):
        """At most MAX_TRACE_REASONS reasons are listed; n_reasons tells how many there were."""
        records = [record('get_sm', 'FAILED', f'reason {i}') for i in range(telemetry.MAX_TRACE_REASONS + 5)]
        records += [record('get_sm', 'FAILED', 'reason 14')] * 3
        traces = telemetry.get_traces(make_snapshot(records))
        self.assertEqual(len(traces['reasons']), telemetry.MAX_TRACE_REASONS)
        self.assertEqual(traces['reasons'][0], {'event': 'get_sm', 'state': 'FAILED', 'reason': 'reason 14', 'count': 4})
        self.assertEqual(traces['n_reasons'], telemetry.MAX_TRACE_REASONS + 5)

    def test_no_reasons(self):
        """Without non-OK reasons, there is no reasons list."""
        traces = telemetry.get_traces(make_snapshot([record()]))
        self.assertNotIn('reasons', traces)
        self.assertNotIn('n_reasons', traces)

    def test_document_includes_the_section(self):
        """The snapshot ends up in the document."""
        job = SimpleNamespace(jobid=1, indata=[], outdata=[], logdata=[], lsetuptime=0)
        document = telemetry.get_pilot_telemetry(job, SimpleNamespace(timing={}), make_snapshot([record()], sent=1))
        self.assertEqual(document['traces']['sent'], 1)
        self.assertEqual(telemetry.get_pilot_telemetry(job, SimpleNamespace(timing={}))['traces'], {})

    def test_large_job_fits(self):
        """Thousands of traces with distinct reasons stay far below the size limit."""
        records = [record('get_sm', 'FAILED', 'x' * 100 + str(i)) for i in range(5000)]
        size = len(json.dumps(telemetry.get_traces(make_snapshot(records))))
        self.assertLess(size, 3000)

    def test_traces_dropped_first(self):
        """The traces section is the first to go when the size limit is hit."""
        document = {'schema_version': 2, 'timestamps': {'a': 1}, 'durations': {'b': 2}, 'counters': {'c': 3},
                    'traces': {'reasons': ['x' * 500]}}
        limit = telemetry.attributes_size({'schema_version': 2, 'timestamps': {'a': 1}, 'durations': {'b': 2},
                                           'counters': {'c': 3}, 'truncated': ['traces']})
        with self.assertLogs(telemetry.logger, level='WARNING'):
            result = telemetry.apply_size_limit(document, limit)
        self.assertEqual(result['truncated'], ['traces'])
        self.assertIn('counters', result)


class TestSendWithSnapshot(unittest.TestCase):
    """send_pilot_telemetry() puts the trace snapshot in the request body."""

    def test_body_has_the_traces(self):
        """The traces section of the body comes from the snapshot."""
        job = SimpleNamespace(jobid=1, indata=[], outdata=[], logdata=[], lsetuptime=0)
        args = SimpleNamespace(timing={}, url='u', port=1, internet_protocol_version='IPv4')
        result = SimpleNamespace(ok=True, message='')
        with patch.object(telemetry, 'send_update', return_value=result) as send:
            telemetry.send_pilot_telemetry(job, args, trace_snapshot=make_snapshot([record()], sent=7))
        body = send.call_args[0][1]
        self.assertEqual(body['pilot_attributes']['traces']['sent'], 7)
        self.assertEqual(body['pilot_attributes']['schema_version'], 2)


class TestUpdateServerTraceSnapshot(unittest.TestCase):
    """update_server() takes the trace snapshot before the sender is removed, and passes it to the telemetry."""

    def setUp(self):
        """Use the generic user plugin and a clean registry."""
        self.workdir = tempfile.mkdtemp()
        self.env = patch.dict(os.environ, {'PILOT_USER': 'generic'}, clear=False)
        self.env.start()
        reset_registry()

    def tearDown(self):
        """Clean up."""
        reset_registry()
        self.env.stop()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def test_snapshot_reaches_the_telemetry(self):
        """The telemetry gets the finished sender's snapshot although the sender is removed after the update."""
        job = SimpleNamespace(jobid='7380000000', state='finished', workdir=self.workdir, fileinfo={}, indata=[],
                              outdata=[], logdata=[], lsetuptime=0, completed=False)
        args = SimpleNamespace(timing={}, update_server=True, url='https://pandaserver.example.org', port=25443,
                               internet_protocol_version='IPv4')
        sender = get_or_create_sender(job)
        sender._records.append(TraceRecord('put_sm', 'DONE', 'f', 'RSE'))

        def complete(_job, _args, _state, **_kwargs):
            _job.completed = True
            return True

        with patch.object(job_module, 'send_state', side_effect=complete), \
                patch.object(telemetry, 'send_pilot_telemetry') as send:
            job_module.update_server(job, args)
        snapshot = send.call_args[1]['trace_snapshot']
        self.assertIsInstance(snapshot, TraceSnapshot)
        self.assertEqual([rec.event for rec in snapshot.records], ['put_sm'])
        self.assertIsNone(get_sender(job.jobid))

    def test_without_sender(self):
        """A job without a trace sender passes no snapshot."""
        job = SimpleNamespace(jobid='7380000001', state='failed', workdir=self.workdir, fileinfo={}, indata=[],
                              outdata=[], logdata=[], lsetuptime=0, completed=False)
        args = SimpleNamespace(timing={}, update_server=True, url='u', port=1, internet_protocol_version='IPv4')

        def complete(_job, _args, _state, **_kwargs):
            _job.completed = True
            return True

        with patch.object(job_module, 'send_state', side_effect=complete), \
                patch.object(telemetry, 'send_pilot_telemetry') as send:
            job_module.update_server(job, args)
        self.assertIsNone(send.call_args[1]['trace_snapshot'])


class TestDefaultAttempts(unittest.TestCase):
    """The pilot makes two stage-out attempts by default."""

    def test_default(self):
        """--stageout-attempts defaults to 2."""
        import argparse  # pylint: disable=import-outside-toplevel
        from arguments import add_queue_args  # pylint: disable=import-outside-toplevel
        parser = argparse.ArgumentParser()
        add_queue_args(parser)
        self.assertEqual(parser.get_default('stageout_attempts'), 2)


if __name__ == '__main__':
    unittest.main()
