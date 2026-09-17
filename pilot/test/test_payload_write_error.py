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

"""Unit tests for local ROOT file write-error detection in pilot.util.rootio."""

import logging
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.util.filehandling import grep
from pilot.util.rootio import check_root_write_error, get_root_write_error

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()

# The cascade as it appears in a real Athena payload log (JIRA report, CYFRONET analysis
# job): the message service reformats the ROOT error handler output and truncates the
# originating method name to 22 characters.
ATHENA_CASCADE = (
    "TFile::Flush                                          ERROR   error flushing file "
    "tree.root (Input/output error)\n" +
    "TBranchElement::WriteB...                             ERROR   basket's WriteBuffer failed.\n" * 9
)

# The same failure as printed by bare ROOT, without the Athena message service.
RAW_ROOT_CASCADE = (
    "SysError in <TFile::Flush>: error flushing file tree.root (Input/output error)\n"
    "Error in <TBranchElement::WriteBasketImpl>: basket's WriteBuffer failed.\n"
)


def _write_stdout(tmpdir: str, content: str, name: str = 'payload.stdout') -> str:
    """Write payload stdout content to a temporary directory.

    Args:
        tmpdir: Directory to write into.
        content: Text to write.
        name: File name to use.

    Returns:
        str: Full path to the written file.
    """
    path = os.path.join(tmpdir, name)
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(content)

    return path


class TestGetRootWriteError(unittest.TestCase):
    """Unit tests for get_root_write_error()."""

    def setUp(self):
        """Reset the class-level error lists before each test."""
        errors.reset_pilot_errors()

    def tearDown(self):
        """Reset the class-level error lists after each test."""
        errors.reset_pilot_errors()

    def test_returns_nothing_when_stdout_missing(self):
        """Return (0, '') when the payload stdout file does not exist."""
        code, diag = get_root_write_error('/nonexistent/path/payload.stdout')
        self.assertEqual(code, 0)
        self.assertEqual(diag, '')

    def test_returns_nothing_for_clean_log(self):
        """Return (0, '') when stdout contains no ROOT write errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, 'AthenaMP finished successfully\ntree.root written\n')
            code, diag = get_root_write_error(path)
        self.assertEqual(code, 0)
        self.assertEqual(diag, '')

    def test_detects_athena_formatted_flush_error(self):
        """Detect the Athena-reformatted TFile::Flush failure from the JIRA report."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, ATHENA_CASCADE)
            code, diag = get_root_write_error(path)
        self.assertEqual(code, errors.PAYLOADWRITEFAILURE)
        self.assertIn('error flushing file', diag)
        self.assertIn('tree.root', diag)

    def test_detects_raw_root_flush_error(self):
        """Detect the bare-ROOT SysError rendering of the same failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, RAW_ROOT_CASCADE)
            code, diag = get_root_write_error(path)
        self.assertEqual(code, errors.PAYLOADWRITEFAILURE)
        self.assertIn('SysError in <TFile::Flush>', diag)

    def test_detects_writebuffer_error(self):
        """Detect the TFile::WriteBuffer system error."""
        content = ("SysError in <TFile::WriteBuffer>: error writing to file tree.root (65536) "
                   "(Input/output error)\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, content)
            code, diag = get_root_write_error(path)
        self.assertEqual(code, errors.PAYLOADWRITEFAILURE)
        self.assertIn('error writing to file', diag)

    def test_detects_short_write_error(self):
        """Detect the TFile::WriteBuffer short-write error."""
        content = ("Error in <TFile::WriteBuffer>: error writing all requested bytes to file "
                   "tree.root, wrote 1024 of 65536\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, content)
            code, diag = get_root_write_error(path)
        self.assertEqual(code, errors.PAYLOADWRITEFAILURE)
        self.assertIn('wrote 1024 of 65536', diag)

    def test_cascade_alone_does_not_trigger(self):
        """Do not report a write failure when only cascade lines are present.

        TBasket::WriteBuffer() also returns -1 when the compressed buffer cannot be
        allocated, so the cascade on its own is not evidence of a node I/O problem.
        """
        content = ("Warning in <TBasket::WriteBuffer>: Unable to allocate the compressed buffer\n"
                   "Error in <TBranchElement::WriteBasketImpl>: basket's WriteBuffer failed.\n" * 5)
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, content)
            code, diag = get_root_write_error(path)
        self.assertEqual(code, 0)
        self.assertEqual(diag, '')

    def test_message_text_without_tfile_does_not_trigger(self):
        """Do not match the bare message text outside a TFile error line.

        Every primary pattern is exercised: the message texts are ordinary English and
        would otherwise match unrelated payload output.
        """
        messages = [
            'INFO my analysis code reports: error flushing file buffer.dat',
            'INFO my analysis code reports: error writing to file summary.txt',
            'INFO error writing all requested bytes to file summary.txt, wrote 1 of 2',
        ]
        for message in messages:
            with self.subTest(message=message):
                with tempfile.TemporaryDirectory() as tmpdir:
                    path = _write_stdout(tmpdir, message + '\n')
                    code, diag = get_root_write_error(path)
                self.assertEqual(code, 0)
                self.assertEqual(diag, '')

    def test_every_primary_pattern_requires_tfile(self):
        """Match each primary message text once it appears in a TFile error line."""
        lines = [
            'SysError in <TFile::Flush>: error flushing file tree.root (Input/output error)',
            'SysError in <TFile::WriteBuffer>: error writing to file tree.root (4096) (Input/output error)',
            'Error in <TFile::WriteBuffer>: error writing all requested bytes to file tree.root, wrote 1 of 2',
        ]
        for line in lines:
            with self.subTest(line=line):
                with tempfile.TemporaryDirectory() as tmpdir:
                    path = _write_stdout(tmpdir, line + '\n')
                    code, diag = get_root_write_error(path)
                self.assertEqual(code, errors.PAYLOADWRITEFAILURE)
                self.assertNotEqual(diag, '')

    def test_read_errors_do_not_trigger(self):
        """Do not match ROOT read errors, which are handled by the direct-access scan."""
        content = ("SysError in <TFile::ReadBuffer>: error reading from file tree.root "
                   "(Input/output error)\n"
                   "Error in <TFile::ReadBuffer>: error reading all requested bytes from file "
                   "tree.root, got 10 of 100\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, content)
            code, diag = get_root_write_error(path)
        self.assertEqual(code, 0)
        self.assertEqual(diag, '')

    def test_enospc_maps_to_nolocalspace(self):
        """Map a full file system to NOLOCALSPACE rather than PAYLOADWRITEFAILURE."""
        content = ("SysError in <TFile::WriteBuffer>: error writing to file tree.root (65536) "
                   "(No space left on device)\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, content)
            code, _diag = get_root_write_error(path)
        self.assertEqual(code, errors.NOLOCALSPACE)

    def test_quota_maps_to_nolocalspace(self):
        """Map an exceeded disk quota to NOLOCALSPACE."""
        content = ("SysError in <TFile::Flush>: error flushing file tree.root "
                   "(Disk quota exceeded)\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, content)
            code, _diag = get_root_write_error(path)
        self.assertEqual(code, errors.NOLOCALSPACE)

    def test_diagnostics_are_truncated(self):
        """Truncate the diagnostics string so the monitor message stays within limits."""
        long_name = 'a' * 500
        content = f"SysError in <TFile::Flush>: error flushing file {long_name}.root (Input/output error)\n"
        with tempfile.TemporaryDirectory() as tmpdir:
            path = _write_stdout(tmpdir, content)
            _code, diag = get_root_write_error(path)
        self.assertLessEqual(len(diag), 200)

    def test_handles_non_utf8_payload_stdout(self):
        """Detect the error even when the log contains undecodable bytes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'payload.stdout')
            with open(path, 'wb') as fh:
                fh.write(b'some binary junk: \xff\xfe\x80\n')
                fh.write(b'SysError in <TFile::Flush>: error flushing file tree.root '
                         b'(Input/output error)\n')
            code, diag = get_root_write_error(path)
        self.assertEqual(code, errors.PAYLOADWRITEFAILURE)
        self.assertIn('error flushing file', diag)


class TestCheckRootWriteError(unittest.TestCase):
    """Unit tests for check_root_write_error()."""

    def setUp(self):
        """Reset the class-level error lists before each test."""
        errors.reset_pilot_errors()

    def tearDown(self):
        """Reset the class-level error lists after each test."""
        errors.reset_pilot_errors()

    def _make_job(self, workdir: str, outdata_lfns: list = None) -> MagicMock:
        """Build a minimal mock job object.

        Args:
            workdir: Job working directory.
            outdata_lfns: Optional list of declared output LFNs.

        Returns:
            MagicMock: Object behaving like a JobData instance for these tests.
        """
        job = MagicMock()
        job.workdir = workdir
        job.piloterrorcodes = []
        job.piloterrordiags = []
        job.outdata = []
        for lfn in outdata_lfns or []:
            fspec = MagicMock()
            fspec.lfn = lfn
            job.outdata.append(fspec)

        return job

    def test_sets_error_code_on_write_failure(self):
        """Set PAYLOADWRITEFAILURE with priority when a write failure is found."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_stdout(tmpdir, ATHENA_CASCADE)
            job = self._make_job(tmpdir)
            with patch('pilot.util.rootio.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                found = check_root_write_error(job)
        self.assertTrue(found)
        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADWRITEFAILURE)
        self.assertIn('error flushing file', job.piloterrordiags[0])

    def test_error_code_is_added_with_priority(self):
        """Report the write failure first when another error code is already registered.

        Only the first entry in piloterrorcodes is reported to the server, so a
        non-priority insert would leave the write failure invisible on the monitor.
        """
        errors.add_error_code(errors.NOPAYLOADMETADATA)
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_stdout(tmpdir, ATHENA_CASCADE)
            job = self._make_job(tmpdir)
            with patch('pilot.util.rootio.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                check_root_write_error(job)
        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADWRITEFAILURE)
        self.assertIn(errors.NOPAYLOADMETADATA, job.piloterrorcodes)

    def test_sets_no_error_code_on_clean_log(self):
        """Leave the job untouched when stdout contains no write failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_stdout(tmpdir, 'all good\n')
            job = self._make_job(tmpdir)
            with patch('pilot.util.rootio.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                found = check_root_write_error(job)
        self.assertFalse(found)
        self.assertEqual(job.piloterrorcodes, [])

    def test_fires_for_zero_exit_code(self):
        """Report the failure even though the payload exited zero.

        This is the whole point of the check: ROOT truncates the output file without the
        transform necessarily noticing, so gating on the exit code would miss the case
        reported in the JIRA ticket.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_stdout(tmpdir, ATHENA_CASCADE)
            job = self._make_job(tmpdir)
            job.exitcode = 0
            job.transexitcode = 0
            with patch('pilot.util.rootio.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                found = check_root_write_error(job)
        self.assertTrue(found)
        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADWRITEFAILURE)

    def test_matches_declared_output_file(self):
        """Report a write failure that concerns a declared output file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_stdout(tmpdir, ATHENA_CASCADE)
            job = self._make_job(tmpdir, outdata_lfns=['tree.root'])
            with patch('pilot.util.rootio.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                with self.assertLogs('pilot.util.rootio', level='WARNING') as captured:
                    found = check_root_write_error(job)
        self.assertTrue(found)
        self.assertTrue(any('declared output file tree.root' in line for line in captured.output))


class TestInterpretPayloadExitInfoOrdering(unittest.TestCase):
    """Verify the placement of the write check in the ATLAS diagnosis chain."""

    def setUp(self):
        """Reset the class-level error lists before each test."""
        errors.reset_pilot_errors()

    def tearDown(self):
        """Reset the class-level error lists after each test."""
        errors.reset_pilot_errors()

    def test_write_error_reported_for_successful_payload(self):
        """Set the error through the ATLAS chain even when the payload exited zero.

        The check must not be gated on the payload exit code: ROOT truncates the output
        file without the transform necessarily noticing, which is exactly the case
        reported in the JIRA ticket.
        """
        from pilot.user.atlas.diagnose import interpret_payload_exit_info

        with tempfile.TemporaryDirectory() as tmpdir:
            _write_stdout(tmpdir, ATHENA_CASCADE)
            _write_stdout(tmpdir, '', name='payload.stderr')
            job = MagicMock()
            job.workdir = tmpdir
            job.piloterrorcodes = []
            job.piloterrordiags = []
            job.exitcode = 0
            job.transexitcode = 0
            job.outdata = []
            job.has_remoteio.return_value = False
            with patch('pilot.util.rootio.config') as mock_cfg, \
                    patch('pilot.user.atlas.diagnose.config') as mock_cfg2:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                mock_cfg2.Payload.payloadstdout = 'payload.stdout'
                mock_cfg2.Payload.payloadstderr = 'payload.stderr'
                interpret_payload_exit_info(job)

        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADWRITEFAILURE)

    def test_write_error_wins_over_direct_access(self):
        """Report a local write failure rather than a stage-in error on a remoteIO job.

        A node with a failing local disk can produce both a ROOT write failure and
        XRootD read errors. Blaming stage-in would send the shifter to the wrong place.
        """
        from pilot.user.atlas.diagnose import interpret_payload_exit_info

        content = (ATHENA_CASCADE +
                   "[ERROR] Operation expired root://xrd.example.com//store/data/input.root\n")
        with tempfile.TemporaryDirectory() as tmpdir:
            _write_stdout(tmpdir, content)
            _write_stdout(tmpdir, '', name='payload.stderr')
            job = MagicMock()
            job.workdir = tmpdir
            job.piloterrorcodes = []
            job.piloterrordiags = []
            job.exitcode = 65
            job.transexitcode = 0
            job.outdata = []
            job.has_remoteio.return_value = True
            with patch('pilot.util.rootio.config') as mock_cfg, \
                    patch('pilot.user.atlas.diagnose.config') as mock_cfg2:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                mock_cfg2.Payload.payloadstdout = 'payload.stdout'
                mock_cfg2.Payload.payloadstderr = 'payload.stderr'
                interpret_payload_exit_info(job)

        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADWRITEFAILURE)


class TestAllPluginsDetectWriteError(unittest.TestCase):
    """Verify that every experiment plugin performs the write check.

    ROOT write failures are not ATLAS specific, so each plugin must call the shared
    check. A plugin that silently drops it would report the affected jobs as successful.
    """

    PLUGINS = ('atlas', 'darkside', 'epic', 'generic', 'rubin', 'ska', 'sphenix')

    def setUp(self):
        """Reset the class-level error lists before each test."""
        errors.reset_pilot_errors()

    def tearDown(self):
        """Reset the class-level error lists after each test."""
        errors.reset_pilot_errors()

    def test_every_plugin_sets_the_error_code(self):
        """Set PAYLOADWRITEFAILURE from interpret() in every experiment plugin."""
        for plugin in self.PLUGINS:
            with self.subTest(plugin=plugin):
                errors.reset_pilot_errors()
                module = __import__(f'pilot.user.{plugin}.diagnose', globals(), locals(), [plugin], 0)
                with tempfile.TemporaryDirectory() as tmpdir:
                    _write_stdout(tmpdir, ATHENA_CASCADE)
                    _write_stdout(tmpdir, '', name='payload.stderr')
                    job = MagicMock()
                    job.workdir = tmpdir
                    job.piloterrorcodes = []
                    job.piloterrordiags = []
                    job.exitcode = 0
                    job.transexitcode = 0
                    job.outdata = []
                    job.metadata = None
                    job.has_remoteio.return_value = False
                    with patch('pilot.util.rootio.config') as mock_cfg, \
                            patch(f'pilot.user.{plugin}.diagnose.config') as mock_cfg2, \
                            patch.object(module, 'update_job_data', create=True):
                        mock_cfg.Payload.payloadstdout = 'payload.stdout'
                        mock_cfg2.Payload.payloadstdout = 'payload.stdout'
                        mock_cfg2.Payload.payloadstderr = 'payload.stderr'
                        if plugin == 'atlas':
                            module.interpret_payload_exit_info(job)
                        else:
                            module.interpret(job)
                self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADWRITEFAILURE)


class TestGrepHardening(unittest.TestCase):
    """Verify that grep() tolerates undecodable bytes in payload logs."""

    def test_grep_survives_invalid_utf8(self):
        """Return matches instead of raising UnicodeDecodeError on a corrupt log."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'payload.stdout')
            with open(path, 'wb') as fh:
                fh.write(b'\xff\xfe corrupted prefix\n')
                fh.write(b'St9bad_alloc\n')
            matched = grep(['St9bad_alloc'], path)
        self.assertEqual(len(matched), 1)


if __name__ == '__main__':
    unittest.main()
