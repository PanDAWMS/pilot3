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

"""Regression tests for the traces sent after a remote file open verification time-out.

Production incident (job 7136531874): the remote file open verification timed out after the
first input file had been opened and while the second one was still being opened. The pilot
failed the job with REMOTEFILEOPENTIMEDOUT (1368), with the apptainer start-up banner as the
error diagnostics, and then sent successful (FOUND_ROOT) traces for *all* input files, since
an empty list of unopened files was returned on the time-out path and the trace code treated
every file not in that list as opened.

Expected behaviour: one FOUND_ROOT trace for the opened file, one TIMEOUT_REMOTE_OPEN trace for
the file that was being opened, and no trace for the files that were never checked. Whenever
the per-file outcome is unknown, no traces are sent at all.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.scripts import open_remote_file
from pilot.scripts.open_remote_file import ProgressRecorder, get_args
from pilot.user.atlas import common
from pilot.user.atlas.common import (
    REMOTEIO_FAILED,
    REMOTEIO_OPENED,
    REMOTEIO_PER_FILE_TIMEOUT,
    REMOTEIO_TIMED_OUT,
    get_file_open_command,
    get_outcomes_from_dictionary,
    get_outcomes_from_progress,
    get_remote_file_trace_fields,
    get_remote_open_summary,
    get_remotefileverification_progress_path,
    process_remote_file_traces,
    resolve_remote_open_result,
)
from pilot.util.config import config

errors = ErrorCodes()

PILOT_SOURCE = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class _FileSpec:
    """Minimal FileSpec stand-in for a remote i/o input file."""

    def __init__(self, index: int, status: str = 'remote_io'):
        """Create input file number index."""
        self.lfn = f'EVNT.12345678._00000{index}.pool.root.1'
        self.turl = f'root://eosatlas.cern.ch//eos/atlas/{self.lfn}'
        self.surl = self.turl
        self.status = status
        self.ddmendpoint = 'CERN-PROD_DATADISK'
        self.filesize = 1000 + index
        self.guid = f'0000-{index}'
        self.scope = 'mc21_13p6TeV'
        self.dataset = 'mc21_13p6TeV.dataset'


def _indata(n: int = 4) -> list:
    """Return n remote i/o input files."""
    return [_FileSpec(i) for i in range(1, n + 1)]


def _write_json(path: str, content) -> None:
    """Write content as JSON to path."""
    with open(path, 'w', encoding='utf-8') as _file:
        json.dump(content, _file)


class _WorkdirTestCase(unittest.TestCase):
    """Test case with a temporary work directory."""

    def setUp(self):
        """Create the work directory."""
        self.workdir = tempfile.mkdtemp()
        self.progress_path = get_remotefileverification_progress_path(self.workdir)
        self.dictionary_path = os.path.join(self.workdir, config.Pilot.remotefileverification_dictionary)

    def tearDown(self):
        """Remove the work directory."""
        shutil.rmtree(self.workdir, ignore_errors=True)


# ---------------------------------------------------------------------------
# open_remote_file.py: progress recording
# ---------------------------------------------------------------------------


class TestProgressRecorder(_WorkdirTestCase):
    """The script must record the per-file progress while the files are being opened."""

    def _read(self) -> dict:
        """Return the content of the progress file."""
        with open(self.progress_path, encoding='utf-8') as _file:
            return json.load(_file)

    def test_started_file_has_start_time_but_no_end_time(self):
        """A file being opened is recorded as started, with its start time."""
        recorder = ProgressRecorder(self.progress_path)
        before = time.time()
        recorder.update('root://a', open_remote_file.STATE_STARTED)
        entry = self._read()['root://a']
        self.assertEqual(entry['state'], 'started')
        self.assertGreaterEqual(entry['start'], before)
        self.assertNotIn('end', entry)

    def test_finished_file_keeps_start_time(self):
        """A finished file keeps its start time and gets an end time."""
        recorder = ProgressRecorder(self.progress_path)
        recorder.update('root://a', open_remote_file.STATE_STARTED)
        recorder.update('root://a', open_remote_file.STATE_OPENED)
        entry = self._read()['root://a']
        self.assertEqual(entry['state'], 'opened')
        self.assertIn('start', entry)
        self.assertIn('end', entry)

    def test_all_files_are_kept(self):
        """Every update rewrites the whole progress, not only the latest file."""
        recorder = ProgressRecorder(self.progress_path)
        recorder.update('root://a', open_remote_file.STATE_STARTED)
        recorder.update('root://a', open_remote_file.STATE_FAILED)
        recorder.update('root://b', open_remote_file.STATE_STARTED)
        progress = self._read()
        self.assertEqual(progress['root://a']['state'], 'failed')
        self.assertEqual(progress['root://b']['state'], 'started')

    def test_no_temporary_file_is_left_behind(self):
        """The progress file is moved into place, so the temporary file must not remain."""
        ProgressRecorder(self.progress_path).update('root://a', open_remote_file.STATE_STARTED)
        self.assertFalse(os.path.exists(f'{self.progress_path}.tmp'))

    def test_write_failure_does_not_raise(self):
        """A progress file that cannot be written must not break the verification itself."""
        recorder = ProgressRecorder(os.path.join(self.workdir, 'missing_dir', 'progress.json'))
        with patch.object(open_remote_file, 'message') as mock_message:
            recorder.update('root://a', open_remote_file.STATE_STARTED)
        self.assertIn('failed to write progress file', mock_message.call_args[0][0])


class TestTryOpenFileProgress(_WorkdirTestCase):
    """try_open_file() must record the start and the outcome of every file open."""

    def setUp(self):
        """Patch ROOT and message()."""
        super().setUp()
        self._real_root = open_remote_file.ROOT
        self._real_message = open_remote_file.message
        open_remote_file.message = lambda msg: None
        self.recorder = ProgressRecorder(self.progress_path)
        self.recorder.update = MagicMock(wraps=self.recorder.update)

    def tearDown(self):
        """Restore the patched module attributes."""
        open_remote_file.ROOT = self._real_root
        open_remote_file.message = self._real_message
        super().tearDown()

    def _run(self, turl: str, openable: set):
        """Run try_open_file() against a fake ROOT."""
        fake_file = MagicMock()
        fake_file.IsOpen.return_value = True
        fake_root = MagicMock()
        fake_root.TFile.Open.side_effect = lambda path: fake_file if path in openable else None
        open_remote_file.ROOT = fake_root
        queues = MagicMock()
        open_remote_file.try_open_file(turl, queues, progress=self.recorder)

    def test_opened_file(self):
        """An opened file is recorded as started, then as opened."""
        self._run('root://a', {'root://a'})
        states = [call.args for call in self.recorder.update.call_args_list]
        self.assertEqual(states, [('root://a', 'started'), ('root://a', 'opened')])

    def test_raw_mode_file_is_recorded_under_the_original_turl(self):
        """A file opened in raw mode must be recorded under the turl the pilot knows."""
        self._run('root://a', {'root://a?filetype=raw'})
        states = [call.args for call in self.recorder.update.call_args_list]
        self.assertEqual(states, [('root://a', 'started'), ('root://a', 'opened')])

    def test_failed_file(self):
        """A file that could not be opened is recorded as started, then as failed."""
        self._run('root://a', set())
        states = [call.args for call in self.recorder.update.call_args_list]
        self.assertEqual(states, [('root://a', 'started'), ('root://a', 'failed')])

    def test_no_recorder(self):
        """The progress recorder is optional."""
        open_remote_file.ROOT = MagicMock()
        open_remote_file.try_open_file('root://a', MagicMock())


class TestProgressFileArgument(unittest.TestCase):
    """The pilot passes the name of the progress file to the script."""

    def test_script_accepts_progress_file(self):
        """The script must parse --progress-file."""
        args = get_args(['--turls=root://a', '--progress-file=progress.json'])
        self.assertEqual(args.progress_file, 'progress.json')

    def test_progress_file_is_optional(self):
        """Without --progress-file no progress is recorded."""
        self.assertIsNone(get_args(['--turls=root://a']).progress_file)

    def test_command_includes_progress_file(self):
        """The command built by the pilot must pass the progress file name."""
        cmd = get_file_open_command('./open_remote_file.py', 'root://a', 1, workdir='/tmp',
                                    progress_file='progress.json')
        self.assertIn('--progress-file=progress.json', cmd)

    def test_command_without_progress_file(self):
        """No option is added when no progress file is given."""
        cmd = get_file_open_command('./open_remote_file.py', 'root://a', 1, workdir='/tmp')
        self.assertNotIn('--progress-file', cmd)


class TestScriptKilledWhileOpening(_WorkdirTestCase):
    """Run the real script, with a fake ROOT that hangs on the second file, and kill it (job 7136531874)."""

    def test_progress_survives_the_kill(self):
        """After the kill, the progress file must show opened, in-flight and unchecked files."""
        fake_root_dir = os.path.join(self.workdir, 'fakeroot')
        os.makedirs(fake_root_dir)
        with open(os.path.join(fake_root_dir, 'ROOT.py'), 'w', encoding='utf-8') as _file:
            _file.write(textwrap.dedent('''
                import time

                class _File:
                    def IsOpen(self):
                        return True

                    def Close(self):
                        pass

                class TFile:
                    @staticmethod
                    def SetOpenTimeout(_ms):
                        return 0

                    @staticmethod
                    def Open(path):
                        if 'hang' in path:
                            time.sleep(600)
                        return _File()
            '''))
        turls = ['root://se//good.root', 'root://se//hang.root', 'root://se//unchecked.root']
        script = os.path.join(PILOT_SOURCE, 'pilot', 'scripts', 'open_remote_file.py')
        env = dict(os.environ, PYTHONPATH=os.pathsep.join([fake_root_dir, PILOT_SOURCE]))
        cmd = [sys.executable, script, f'--turls={",".join(turls)}', '-w', self.workdir, '-t', '1',
               f'--progress-file={os.path.basename(self.progress_path)}']
        with subprocess.Popen(cmd, cwd=self.workdir, env=env,  # pylint: disable=consider-using-with
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) as process:
            try:
                progress = {}
                deadline = time.time() + 60
                while time.time() < deadline:
                    if os.path.exists(self.progress_path):
                        with open(self.progress_path, encoding='utf-8') as _file:
                            progress = json.load(_file)
                        if turls[1] in progress:
                            break
                    time.sleep(0.2)
            finally:
                process.kill()

        self.assertEqual(progress[turls[0]]['state'], 'opened')
        self.assertEqual(progress[turls[1]]['state'], 'started')
        self.assertNotIn(turls[2], progress)
        self.assertFalse(os.path.exists(self.dictionary_path),
                         'the final dictionary is only written when all files have been processed')


# ---------------------------------------------------------------------------
# pilot: per-file outcomes
# ---------------------------------------------------------------------------


class TestOutcomesFromProgress(_WorkdirTestCase):
    """Outcomes of a verification that was killed on a time-out."""

    def test_job_7136531874(self):
        """One file opened, one hung beyond its budget, two never attempted."""
        indata = _indata(4)
        now = 10000.0
        _write_json(self.progress_path, {
            indata[0].turl: {'state': 'opened', 'start': now - 1000, 'end': now - 995},
            indata[1].turl: {'state': 'started', 'start': now - 995},
        })
        outcomes = get_outcomes_from_progress(self.workdir, indata, now=now)
        self.assertEqual(outcomes, {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_TIMED_OUT})

    def test_failed_file(self):
        """A file that could not be opened before the kill is reported as failed."""
        indata = _indata(1)
        _write_json(self.progress_path, {indata[0].turl: {'state': 'failed', 'start': 1, 'end': 2}})
        self.assertEqual(get_outcomes_from_progress(self.workdir, indata), {indata[0].turl: REMOTEIO_FAILED})

    def test_file_started_just_before_the_kill_is_not_blamed(self):
        """A file that had not used up its own budget gets no outcome."""
        indata = _indata(1)
        now = 10000.0
        _write_json(self.progress_path, {indata[0].turl: {'state': 'started', 'start': now - REMOTEIO_PER_FILE_TIMEOUT + 1}})
        self.assertEqual(get_outcomes_from_progress(self.workdir, indata, now=now), {})

    def test_file_at_exactly_its_budget_is_timed_out(self):
        """The per-file budget is inclusive."""
        indata = _indata(1)
        now = 10000.0
        _write_json(self.progress_path, {indata[0].turl: {'state': 'started', 'start': now - REMOTEIO_PER_FILE_TIMEOUT}})
        self.assertEqual(get_outcomes_from_progress(self.workdir, indata, now=now), {indata[0].turl: REMOTEIO_TIMED_OUT})

    def test_started_file_without_start_time(self):
        """A malformed entry gets no outcome."""
        indata = _indata(1)
        _write_json(self.progress_path, {indata[0].turl: {'state': 'started'}})
        self.assertEqual(get_outcomes_from_progress(self.workdir, indata), {})

    def test_malformed_entries_are_ignored(self):
        """Unexpected entry types or states get no outcome."""
        indata = _indata(2)
        _write_json(self.progress_path, {indata[0].turl: 'opened', indata[1].turl: {'state': 'weird'}})
        self.assertEqual(get_outcomes_from_progress(self.workdir, indata), {})

    def test_missing_progress_file(self):
        """Without a progress file the outcome is unknown."""
        self.assertEqual(get_outcomes_from_progress(self.workdir, _indata(2)), {})

    def test_unreadable_progress_file(self):
        """A corrupt progress file is treated as missing."""
        with open(self.progress_path, 'w', encoding='utf-8') as _file:
            _file.write('{not json')
        self.assertEqual(get_outcomes_from_progress(self.workdir, _indata(2)), {})

    def test_non_dictionary_progress_file(self):
        """A progress file that is not a dictionary is treated as missing."""
        _write_json(self.progress_path, ['a', 'b'])
        self.assertEqual(get_outcomes_from_progress(self.workdir, _indata(2)), {})

    def test_copy_to_scratch_files_are_ignored(self):
        """Only remote i/o files get an outcome."""
        indata = [_FileSpec(1, status='transferred')]
        _write_json(self.progress_path, {indata[0].turl: {'state': 'opened', 'start': 1, 'end': 2}})
        self.assertEqual(get_outcomes_from_progress(self.workdir, indata), {})


class TestOutcomesFromDictionary(_WorkdirTestCase):
    """Outcomes of a verification that ran to completion."""

    def test_opened_failed_and_absent(self):
        """True means opened, False means failed and an absent file gets no outcome."""
        indata = _indata(3)
        _write_json(self.dictionary_path, {indata[0].turl: True, indata[1].turl: False})
        self.assertEqual(get_outcomes_from_dictionary(self.workdir, indata),
                         {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_FAILED})

    def test_raw_mode_file_is_opened(self):
        """A file opened in raw mode is listed with filetype=raw appended to its turl."""
        indata = _indata(1)
        _write_json(self.dictionary_path, {indata[0].turl + '?filetype=raw': True})
        self.assertEqual(get_outcomes_from_dictionary(self.workdir, indata), {indata[0].turl: REMOTEIO_OPENED})

    def test_raw_mode_file_with_query_string(self):
        """The raw-mode turl of a turl with a query string uses '&' (same rule as the script)."""
        indata = _indata(1)
        indata[0].turl += '?token=x'
        _write_json(self.dictionary_path, {indata[0].turl + '&filetype=raw': True})
        self.assertEqual(get_outcomes_from_dictionary(self.workdir, indata), {indata[0].turl: REMOTEIO_OPENED})

    def test_missing_dictionary(self):
        """Without a dictionary the outcome is unknown."""
        self.assertEqual(get_outcomes_from_dictionary(self.workdir, _indata(2)), {})


class TestRemoteOpenSummary(unittest.TestCase):
    """The time-out diagnostics must say what happened to the files."""

    def test_job_7136531874(self):
        """One opened, one timed out, two not checked."""
        indata = _indata(4)
        summary = get_remote_open_summary(indata, {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_TIMED_OUT})
        self.assertEqual(summary, f'1/4 file(s) opened, timed out: {indata[1].lfn}, 2 not checked')

    def test_failed_files_are_listed(self):
        """Files that could not be opened are listed."""
        indata = _indata(2)
        summary = get_remote_open_summary(indata, {indata[0].turl: REMOTEIO_FAILED, indata[1].turl: REMOTEIO_OPENED})
        self.assertEqual(summary, f'1/2 file(s) opened, failed: {indata[0].lfn}')

    def test_nothing_known(self):
        """Without outcomes, all files are reported as not checked."""
        self.assertEqual(get_remote_open_summary(_indata(3), {}), '0/3 file(s) opened, 3 not checked')

    def test_long_lists_are_capped(self):
        """At most three file names are listed per category."""
        indata = _indata(5)
        summary = get_remote_open_summary(indata, {fspec.turl: REMOTEIO_TIMED_OUT for fspec in indata})
        self.assertIn('(+2 more)', summary)
        self.assertNotIn(indata[3].lfn, summary)
        self.assertNotIn('not checked', summary)

    def test_formatted_diagnostics_keep_the_standard_message(self):
        """The payload code relies on 'Remote file open timed out' being in the error diagnostics."""
        indata = _indata(2)
        diag = errors.format_diagnostics(errors.REMOTEFILEOPENTIMEDOUT, get_remote_open_summary(indata, {}))
        self.assertTrue(diag.startswith('Remote file open timed out:'))
        self.assertNotIn('apptainer', diag.lower())


class TestResolveRemoteOpenResult(_WorkdirTestCase):
    """The exit code of the verification script decides where the per-file outcomes come from."""

    banner = ('Info: Migrating env variable SINGULARITY_CACHEDIR to APPTAINER_CACHEDIR\n'
              'Apptainer: 1.4.4\nLSETUP_COMPLETED\n')

    def _write_progress(self, indata: list) -> None:
        """Write the progress of job 7136531874 (file 1 opened, file 2 hung)."""
        now = time.time()
        _write_json(self.progress_path, {
            indata[0].turl: {'state': 'opened', 'start': now - 1000, 'end': now - 995},
            indata[1].turl: {'state': 'started', 'start': now - 995},
        })

    def test_python_timeout_uses_progress(self):
        """Exit code 3 (python phase timed out): REMOTEFILEOPENTIMEDOUT with per-file outcomes."""
        indata = _indata(4)
        self._write_progress(indata)
        exitcode, diagnostics, outcomes = resolve_remote_open_result(3, self.banner, self.workdir, indata)
        self.assertEqual(exitcode, errors.REMOTEFILEOPENTIMEDOUT)
        self.assertEqual(outcomes, {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_TIMED_OUT})
        self.assertEqual(diagnostics, f'1/4 file(s) opened, timed out: {indata[1].lfn}, 2 not checked')

    def test_python_timeout_without_progress(self):
        """Without progress, the time-out still fails the job but no outcome is known."""
        exitcode, diagnostics, outcomes = resolve_remote_open_result(3, self.banner, self.workdir, _indata(2))
        self.assertEqual(exitcode, errors.REMOTEFILEOPENTIMEDOUT)
        self.assertEqual(outcomes, {})
        self.assertEqual(diagnostics, '0/2 file(s) opened, 2 not checked')

    def test_lsetup_timeout_uses_progress(self):
        """Exit code 2 (lsetup phase timed out) is handled in the same way."""
        indata = _indata(2)
        self._write_progress(indata)
        exitcode, _, outcomes = resolve_remote_open_result(2, self.banner, self.workdir, indata)
        self.assertEqual(exitcode, errors.LSETUPTIMEDOUT)
        self.assertEqual(outcomes[indata[0].turl], REMOTEIO_OPENED)

    def test_container_failure_has_no_outcomes(self):
        """Exit code 1 (e.g. container did not start): no outcome, container output as diagnostics."""
        indata = _indata(2)
        self._write_progress(indata)  # must not be used
        exitcode, diagnostics, outcomes = resolve_remote_open_result(1, self.banner, self.workdir, indata)
        self.assertEqual(exitcode, errors.PAYLOADEXECUTIONFAILURE)
        self.assertEqual(diagnostics, self.banner)
        self.assertEqual(outcomes, {})

    def test_apptainer_error_has_no_outcomes(self):
        """A recognised apptainer error keeps its error code and gets no outcome."""
        exitcode, _, outcomes = resolve_remote_open_result(3, 'Failed to mount image', self.workdir, _indata(1))
        self.assertEqual(exitcode, errors.SINGULARITYIMAGEMOUNTFAILURE)
        self.assertEqual(outcomes, {})

    def test_completed_check_uses_dictionary(self):
        """Exit code 0: outcomes come from the dictionary, not from the progress file."""
        indata = _indata(2)
        self._write_progress(indata)
        _write_json(self.dictionary_path, {indata[0].turl: True, indata[1].turl: True})
        exitcode, _, outcomes = resolve_remote_open_result(0, '', self.workdir, indata)
        self.assertEqual(exitcode, 0)
        self.assertEqual(outcomes, {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_OPENED})

    def test_completed_check_with_failed_file(self):
        """Exit code 0 with an unopened file: REMOTEFILECOULDNOTBEOPENED and a failed outcome."""
        indata = _indata(2)
        _write_json(self.dictionary_path, {indata[0].turl: True, indata[1].turl: False})
        exitcode, _, outcomes = resolve_remote_open_result(0, '', self.workdir, indata)
        self.assertEqual(exitcode, errors.REMOTEFILECOULDNOTBEOPENED)
        self.assertEqual(outcomes[indata[1].turl], REMOTEIO_FAILED)

    def test_completed_check_without_dictionary(self):
        """Exit code 0 without a dictionary: REMOTEFILEDICTDOESNOTEXIST and no outcome."""
        exitcode, _, outcomes = resolve_remote_open_result(0, '', self.workdir, _indata(2))
        self.assertEqual(exitcode, errors.REMOTEFILEDICTDOESNOTEXIST)
        self.assertEqual(outcomes, {})

    def test_command_timeout_without_dictionary(self):
        """COMMANDTIMEDOUT without a dictionary is still converted to REMOTEFILEOPENTIMEDOUT."""
        exitcode, diagnostics, outcomes = resolve_remote_open_result(errors.COMMANDTIMEDOUT, '', self.workdir, _indata(1))
        self.assertEqual(exitcode, errors.REMOTEFILEOPENTIMEDOUT)
        self.assertIn('timed-out', diagnostics)
        self.assertEqual(outcomes, {})

    def test_command_timeout_with_failed_file(self):
        """COMMANDTIMEDOUT with an unopened file is converted to REMOTEFILEOPENTIMEDOUT."""
        indata = _indata(1)
        _write_json(self.dictionary_path, {indata[0].turl: False})
        exitcode, _, outcomes = resolve_remote_open_result(errors.COMMANDTIMEDOUT, '', self.workdir, indata)
        self.assertEqual(exitcode, errors.REMOTEFILEOPENTIMEDOUT)
        self.assertEqual(outcomes, {indata[0].turl: REMOTEIO_FAILED})

    def test_command_timeout_with_all_files_opened(self):
        """COMMANDTIMEDOUT while all files could be opened keeps the original code."""
        indata = _indata(1)
        _write_json(self.dictionary_path, {indata[0].turl: True})
        exitcode, _, outcomes = resolve_remote_open_result(errors.COMMANDTIMEDOUT, '', self.workdir, indata)
        self.assertEqual(exitcode, errors.COMMANDTIMEDOUT)
        self.assertEqual(outcomes, {indata[0].turl: REMOTEIO_OPENED})

    def test_command_timeout_with_missing_input_file(self):
        """Any other dictionary error code is passed through."""
        indata = _indata(1)
        _write_json(self.dictionary_path, {indata[0].turl: False})
        with open(os.path.join(self.workdir, config.Pilot.remotefileverification_log), 'w', encoding='utf-8') as _file:
            _file.write('No such file or directory')
        exitcode, _, _ = resolve_remote_open_result(errors.COMMANDTIMEDOUT, '', self.workdir, indata)
        self.assertEqual(exitcode, errors.MISSINGINPUTFILE)


# ---------------------------------------------------------------------------
# pilot: traces
# ---------------------------------------------------------------------------


class _SentTraces(list):
    """Collects the trace reports that would have been sent.

    The traces are handed to the job's background trace sender, so they are recorded when they
    are queued there.
    """

    def factory(self, **fields):
        """Return a fake TraceReport that remembers its fields."""
        report = MagicMock()
        report.__bool__ = lambda _self: True
        report.fields = fields
        report.send.side_effect = AssertionError('remote i/o traces must not be sent in the foreground')
        return report

    def sender(self, _job):
        """Return a fake trace sender that records the fields of the queued traces."""
        sender = MagicMock()
        sender.enqueue.side_effect = lambda traces: self.extend(trace.fields for trace in traces)
        return sender


class TestProcessRemoteFileTraces(_WorkdirTestCase):
    """Only files with a known outcome may get a trace."""

    def setUp(self):
        """Write the base trace report."""
        super().setUp()
        self.path = os.path.join(self.workdir, 'base_trace_report.json')
        _write_json(self.path, {'eventType': 'get_sm_a', 'stateReason': None})
        self.sent = _SentTraces()

    def _process(self, indata: list, outcomes: dict) -> list:
        """Run process_remote_file_traces() and return the sent traces."""
        job = MagicMock(indata=indata, workdir=self.workdir)
        with patch.object(common, 'TraceReport', side_effect=self.sent.factory), \
                patch.object(common, 'get_or_create_sender', side_effect=self.sent.sender):
            process_remote_file_traces(self.path, job, outcomes)
        return self.sent

    def test_job_7136531874(self):
        """One good trace, one time-out trace, nothing for the unchecked files."""
        indata = _indata(4)
        sent = self._process(indata, {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_TIMED_OUT})
        self.assertEqual([(t['filename'], t['clientState']) for t in sent],
                         [(indata[0].lfn, 'FOUND_ROOT'), (indata[1].lfn, 'TIMEOUT_REMOTE_OPEN')])
        self.assertEqual(sent[1]['stateReason'], 'remote file open did not complete in time')

    def test_unknown_outcome_sends_no_traces(self):
        """The previous behaviour (all FOUND_ROOT when nothing was known) must not come back."""
        self.assertEqual(self._process(_indata(3), {}), [])

    def test_failed_file(self):
        """A file that could not be opened gets FAILED_REMOTE_OPEN."""
        indata = _indata(1)
        sent = self._process(indata, {indata[0].turl: REMOTEIO_FAILED})
        self.assertEqual(sent[0]['clientState'], 'FAILED_REMOTE_OPEN')
        self.assertEqual(sent[0]['stateReason'], 'remote file could not be opened')

    def test_all_opened(self):
        """The normal case is unchanged: FOUND_ROOT for every file."""
        indata = _indata(3)
        sent = self._process(indata, {fspec.turl: REMOTEIO_OPENED for fspec in indata})
        self.assertEqual([t['clientState'] for t in sent], ['FOUND_ROOT'] * 3)
        self.assertEqual([t['protocol'] for t in sent], ['root'] * 3)

    def test_no_field_leaks_between_files(self):
        """The protocol of an opened file must not end up in the trace of a failed one, and vice versa."""
        indata = _indata(3)
        sent = self._process(indata, {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_FAILED,
                                      indata[2].turl: REMOTEIO_OPENED})
        self.assertNotEqual(sent[1].get('protocol'), 'root')
        self.assertIsNone(sent[2]['stateReason'])

    def test_workdir_is_passed_to_the_trace(self):
        """The trace report needs the work directory for the curl output."""
        indata = _indata(1)
        sent = self._process(indata, {indata[0].turl: REMOTEIO_OPENED})
        self.assertEqual(sent[0]['workdir'], self.workdir)

    def test_copy_to_scratch_files_get_no_trace(self):
        """Only remote i/o files are reported here."""
        indata = [_FileSpec(1, status='transferred')]
        self.assertEqual(self._process(indata, {indata[0].turl: REMOTEIO_OPENED}), [])

    def test_missing_base_trace_report(self):
        """Without the base trace report no traces can be sent."""
        indata = _indata(1)
        os.remove(self.path)
        self.assertEqual(self._process(indata, {indata[0].turl: REMOTEIO_OPENED}), [])

    def test_empty_base_trace_report(self):
        """An empty base trace report cannot be used."""
        indata = _indata(1)
        _write_json(self.path, {})
        self.assertEqual(self._process(indata, {indata[0].turl: REMOTEIO_OPENED}), [])

    def test_trace_fields_use_a_copy_of_the_base(self):
        """The base trace report itself must not be modified."""
        base = {'eventType': 'get_sm_a'}
        get_remote_file_trace_fields(base, _FileSpec(1), REMOTEIO_FAILED)
        self.assertEqual(base, {'eventType': 'get_sm_a'})


# ---------------------------------------------------------------------------
# pilot: open_remote_files() end-to-end (execute_remote_file_open mocked)
# ---------------------------------------------------------------------------


class TestOpenRemoteFilesTimeout(_WorkdirTestCase):
    """open_remote_files() must return the per-file outcomes on a time-out (job 7136531874)."""

    def _open_remote_files(self, indata: list, execute):
        """Run open_remote_files() with the container execution mocked."""
        home = os.path.join(self.workdir, 'home')
        scripts = os.path.join(home, 'pilot3', 'pilot', 'scripts')
        os.makedirs(scripts)
        for name in ('open_remote_file.py', 'open_file.sh'):
            with open(os.path.join(scripts, name), 'w', encoding='utf-8') as _file:
                _file.write('REPLACE_ME_FOR_CMD')
        env = {'PILOT_HOME': home, 'PYTHONPATH': os.environ.get('PYTHONPATH', '')}
        with patch.dict('os.environ', env, clear=False), \
             patch.object(common, 'copy_pilot_source', return_value=''), \
             patch.object(common, 'create_root_container_command', return_value='container cmd'), \
             patch.object(common, 'execute_remote_file_open', execute), \
             patch.object(common, 'remove_container_proxies'):
            return common.open_remote_files(indata, self.workdir, 1)

    def test_timeout_returns_outcomes(self):
        """A python-phase time-out returns the outcomes recorded before the kill."""
        indata = _indata(4)

        def execute(_path, _timeout):
            """Simulate the script: file 1 opened, file 2 hung, then the kill."""
            now = time.time()
            _write_json(self.progress_path, {
                indata[0].turl: {'state': 'opened', 'start': now - 1000, 'end': now - 995},
                indata[1].turl: {'state': 'started', 'start': now - 995},
            })
            return 3, 'Info: Migrating env variable SINGULARITY_CACHEDIR to APPTAINER_CACHEDIR\n', 12

        exitcode, diagnostics, outcomes, lsetup_time = self._open_remote_files(indata, execute)
        self.assertEqual(exitcode, errors.REMOTEFILEOPENTIMEDOUT)
        self.assertEqual(outcomes, {indata[0].turl: REMOTEIO_OPENED, indata[1].turl: REMOTEIO_TIMED_OUT})
        self.assertNotIn('APPTAINER', diagnostics)
        self.assertEqual(lsetup_time, 12)

    def test_progress_file_name_is_passed_to_the_script(self):
        """The command written for the container must ask the script to record its progress."""
        indata = _indata(1)
        written = {}
        with patch.object(common, 'create_root_container_command',
                          side_effect=lambda _workdir, cmd, _content: written.setdefault('cmd', cmd)):
            home = os.path.join(self.workdir, 'home')
            os.makedirs(os.path.join(home, 'pilot3', 'pilot', 'scripts'))
            for name in ('open_remote_file.py', 'open_file.sh'):
                with open(os.path.join(home, 'pilot3', 'pilot', 'scripts', name), 'w', encoding='utf-8') as _file:
                    _file.write('x')
            with patch.dict('os.environ', {'PILOT_HOME': home, 'PYTHONPATH': ''}, clear=False), \
                 patch.object(common, 'copy_pilot_source', return_value=''), \
                 patch.object(common, 'execute_remote_file_open', return_value=(1, '', 0)), \
                 patch.object(common, 'remove_container_proxies'):
                common.open_remote_files(indata, self.workdir, 1)
        self.assertIn(f'--progress-file={os.path.basename(self.progress_path)}', written['cmd'])

    def test_stale_progress_file_is_removed(self):
        """A progress file left over from before the check must not be mistaken for its progress."""
        indata = _indata(1)
        _write_json(self.progress_path, {indata[0].turl: {'state': 'opened', 'start': 1, 'end': 2}})
        _, _, outcomes, _ = self._open_remote_files(indata, MagicMock(return_value=(3, '', 0)))
        self.assertEqual(outcomes, {})

    def test_missing_script_has_no_outcomes(self):
        """When the check cannot even be prepared, no outcome is known (previously: all FOUND_ROOT)."""
        indata = _indata(1)
        with patch.dict('os.environ', {'PILOT_HOME': self.workdir, 'PYTHONPATH': ''}, clear=False), \
             patch.object(common, 'copy_pilot_source', return_value=''):
            exitcode, diagnostics, outcomes, _ = common.open_remote_files(indata, self.workdir, 1)
        self.assertEqual(exitcode, 0)
        self.assertIn('script path does not exist', diagnostics)
        self.assertEqual(outcomes, {})

    def test_nothing_to_verify(self):
        """Without remote i/o files, nothing is verified and no outcome is returned."""
        self.assertEqual(common.open_remote_files([_FileSpec(1, status='transferred')], self.workdir, 1),
                         (0, '', {}, 0))


if __name__ == '__main__':
    unittest.main()
