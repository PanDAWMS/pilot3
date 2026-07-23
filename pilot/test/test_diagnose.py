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

"""Unit tests for direct-access (remoteIO) error classification in pilot.user.atlas.diagnose."""

import logging
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.user.atlas.diagnose import interpret_payload_exit_info, is_direct_access_error

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()


def _make_job(stdout_content: str, has_remoteio: bool = True, workdir: str = None) -> MagicMock:
    """Build a minimal mock job object for diagnose tests.

    Args:
        stdout_content: Text to write into the payload stdout file.
        has_remoteio: Whether the job should report remote I/O input files.
        workdir: Directory to use; a fresh tempdir is created when None.

    Returns:
        A MagicMock configured to behave like a JobData instance for the
        purposes of the diagnose functions under test.
    """
    if workdir is None:
        workdir = tempfile.mkdtemp()

    stdout_path = os.path.join(workdir, 'payload.stdout')
    with open(stdout_path, 'w', encoding='utf-8') as fh:
        fh.write(stdout_content)

    job = MagicMock()
    job.workdir = workdir
    job.piloterrorcodes = []
    job.piloterrordiags = []
    job.transexitcode = 0
    job.exitcode = 1
    job.has_remoteio.return_value = has_remoteio

    def _add_error(code, priority=False, msg=''):
        job.piloterrorcodes = [code]
        job.piloterrordiags = [msg or errors.get_error_message(code)]
        return job.piloterrorcodes, job.piloterrordiags

    job_errors = ErrorCodes()
    with patch('pilot.user.atlas.diagnose.errors', job_errors):
        pass
    return job, workdir


class TestIsDirectAccessError(unittest.TestCase):
    """Unit tests for is_direct_access_error()."""

    def test_returns_empty_when_stdout_missing(self):
        """Return empty string when payload stdout does not exist."""
        job = MagicMock()
        job.workdir = '/nonexistent/path'
        with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
            mock_cfg.Payload.payloadstdout = 'payload.stdout'
            _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertEqual(diag, '')

    def test_returns_empty_when_no_pattern_matches(self):
        """Return empty string when stdout contains no XRootD error patterns."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('AthenaMP finished successfully\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertEqual(diag, '')

    def test_detects_tnetxngfile_error(self):
        """Detect TNetXNGFile::Open ERROR pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('Error in <TNetXNGFile::Open ERROR> Cannot open file root://dcache-door.example.com//pnfs/data/file.root\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertNotEqual(diag, '')
        self.assertIn('TNetXNGFile', diag)

    def test_detects_unable_to_open_root_file(self):
        """Detect 'Unable to open ROOT file' pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('Unable to open ROOT file root://atlas-xrd.example.com//store/data/file.root\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertNotEqual(diag, '')
        self.assertIn('root://', diag)

    def test_detects_operation_expired(self):
        """Detect '[ERROR] Operation expired' pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('[ERROR] Operation expired after 30 seconds\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertNotEqual(diag, '')

    def test_detects_no_servers_available(self):
        """Detect '[ERROR] No servers are available' pattern."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('[ERROR] No servers are available to serve your request\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertNotEqual(diag, '')

    def test_prefers_line_with_file_path(self):
        """When multiple lines match, prefer the one containing a file path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('[ERROR] Operation expired after 30 seconds\n')
                f.write('[ERROR] No servers are available\n')
                f.write('Unable to open ROOT file root://atlas-xrd.example.com//store/data/file.root\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertIn('root://', diag, 'line with file URL should be preferred as diagnostics')

    def test_fallback_to_first_matched_line_when_no_path(self):
        """When no matched line contains a path, return the first matched line."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('[ERROR] Operation expired after 30 seconds\n')
                f.write('[ERROR] No servers are available to handle request\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                _ecode, diag, _lfns = is_direct_access_error(job)
        self.assertIn('Operation expired', diag)

    def test_xrd3010_fullyrestricted_returns_xrdaccessrestricted_code(self):
        """XRootD [3010] FullyRestricted must return XRDACCESSRESTRICTED, not STAGEINFAILED."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write(
                    'TNetXNGFile::Open ERROR [ERROR] Server responded with an error: '
                    '[3010] Restriction FullyRestricted denied access for [READ_DATA] on '
                    '/pnfs/uchicago.edu/atlasdatadisk/rucio/data17_13TeV/c6/21/'
                    'DAOD_PHYS.49561597._000010.pool.root.1\n'
                )
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                ecode, diag, _lfns = is_direct_access_error(job)
        self.assertEqual(ecode, errors.XRDACCESSRESTRICTED,
                         f'[3010] FullyRestricted must return XRDACCESSRESTRICTED (1388), got {ecode}')
        self.assertNotEqual(diag, '')

    def test_xrd3010_restriction_denied_returns_xrdaccessrestricted_code(self):
        """XRootD [3010] Restriction denied variant must return XRDACCESSRESTRICTED."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write(
                    '[ERROR] Server responded with an error: [3010] Restriction denied access\n'
                )
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                ecode, diag, _lfns = is_direct_access_error(job)
        self.assertEqual(ecode, errors.XRDACCESSRESTRICTED)
        self.assertNotEqual(diag, '')

    def test_generic_xrootd_error_still_returns_stageinfailed_code(self):
        """Generic XRootD errors (no [3010]) must still return STAGEINFAILED, not XRDACCESSRESTRICTED."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout, 'w') as f:
                f.write('[ERROR] No servers are available to serve your request\n')
            job = MagicMock()
            job.workdir = tmpdir
            with patch('pilot.user.atlas.diagnose.config') as mock_cfg:
                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                ecode, diag, _lfns = is_direct_access_error(job)
        self.assertEqual(ecode, errors.STAGEINFAILED,
                         f'generic error must return STAGEINFAILED (1099), got {ecode}')


class TestInterpretPayloadExitInfoDirectAccess(unittest.TestCase):
    """Integration tests: interpret_payload_exit_info() sets STAGEINFAILED for remoteIO jobs."""

    def _run(self, stdout_content: str, has_remoteio: bool = True):
        """Run interpret_payload_exit_info with a temporary stdout file.

        Args:
            stdout_content: Content for the payload stdout file.
            has_remoteio: Whether to simulate a remoteIO job.

        Returns:
            Tuple of (piloterrorcodes, piloterrordiags) after the call.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout_path, 'w', encoding='utf-8') as fh:
                fh.write(stdout_content)

            job = MagicMock()
            job.workdir = tmpdir
            job.piloterrorcodes = []
            job.piloterrordiags = []
            job.transexitcode = 0
            job.exitcode = 1
            job.has_remoteio.return_value = has_remoteio

            captured = {}

            def _add_error_code(code, priority=False, msg=''):
                captured['codes'] = [code]
                captured['diags'] = [msg or '']
                return [code], [msg or '']

            with patch('pilot.user.atlas.diagnose.errors') as mock_errors, \
                 patch('pilot.user.atlas.diagnose.config') as mock_cfg, \
                 patch('pilot.user.atlas.diagnose.is_out_of_memory', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_cling_jit_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_installation_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_atlassetup_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_out_of_space', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_nfssqlite_locking_problem', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_user_code_missing', return_value=False):

                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                mock_errors.STAGEINFAILED = errors.STAGEINFAILED
                mock_errors.XRDACCESSRESTRICTED = errors.XRDACCESSRESTRICTED
                mock_errors.UNKNOWNPAYLOADFAILURE = errors.UNKNOWNPAYLOADFAILURE
                mock_errors.add_error_code.side_effect = _add_error_code

                interpret_payload_exit_info(job)

            return captured.get('codes', []), captured.get('diags', [])

    def test_remoteio_xrootd_error_sets_stageinfailed(self):
        """A remoteIO job with XRootD errors in stdout must be classified as STAGEINFAILED (1099).

        This is the core regression test: before the fix, PAYLOADEXECUTIONFAILURE (1305)
        was set by perform_initial_payload_error_analysis and the early return in interpret()
        prevented the direct-access scan from ever running.
        """
        stdout = (
            'AthenaMP starting\n'
            '[ERROR] No servers are available to serve root://atlas-xrd.example.com//data/file.root\n'
            'AthenaMP exiting with code 1\n'
        )
        codes, diags = self._run(stdout, has_remoteio=True)
        self.assertEqual(codes, [errors.STAGEINFAILED],
                         f'expected STAGEINFAILED (1099), got {codes}')
        self.assertTrue(diags[0], 'diagnostics string should not be empty')

    def test_non_remoteio_job_does_not_set_stageinfailed(self):
        """A non-remoteIO job with XRootD patterns must NOT be classified as STAGEINFAILED."""
        stdout = '[ERROR] No servers are available\n'
        codes, diags = self._run(stdout, has_remoteio=False)
        self.assertNotIn(errors.STAGEINFAILED, codes,
                         'STAGEINFAILED must not be set for non-remoteIO jobs')

    def test_remoteio_clean_stdout_does_not_set_stageinfailed(self):
        """A remoteIO job with no XRootD patterns must not be classified as STAGEINFAILED."""
        stdout = 'AthenaMP finished successfully\n'
        codes, _ = self._run(stdout, has_remoteio=True)
        self.assertNotIn(errors.STAGEINFAILED, codes)

    def test_writetobject_error_zero_exit_does_not_set_stageinfailed(self):
        """A remoteIO job with a WriteTObject ERROR in stdout but zero payload exit must not be classified as STAGEINFAILED.

        This is the regression test for the false-positive reported by Rod: ROOT emits
        'TNetXNGFile::WriteTObject ERROR  Directory root://... is not writable' as a
        diagnostic even on successful direct-access jobs.  The pattern matches
        _DIRECT_ACCESS_ERROR_PATTERNS but the payload exited zero, so no error should
        be set.
        """
        stdout = (
            'AthenaMP starting\n'
            'TNetXNGFile::WriteTObject ERROR   Directory '
            'root://xrootd.data.net2.mghpcc.org//USATLAS/atlasdatadisk/rucio/mc23_13p6TeV/'
            'ed/95/DAOD_PHYSLITE.46831501._000007.pool.root.1 is not writable\n'
            'AthenaMP exiting with code 0\n'
        )
        # Override exitcode to 0 by running _run then patching exitcode directly.
        # _run() sets exitcode=1 by default so we need a local variant here.
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, 'payload.stdout')
            with open(stdout_path, 'w', encoding='utf-8') as fh:
                fh.write(stdout)

            job = MagicMock()
            job.workdir = tmpdir
            job.piloterrorcodes = []
            job.piloterrordiags = []
            job.transexitcode = 0
            job.exitcode = 0  # payload succeeded
            job.has_remoteio.return_value = True

            captured = {}

            def _add_error_code(code, priority=False, msg=''):
                captured['codes'] = [code]
                captured['diags'] = [msg or '']
                return [code], [msg or '']

            with patch('pilot.user.atlas.diagnose.errors') as mock_errors, \
                 patch('pilot.user.atlas.diagnose.config') as mock_cfg, \
                 patch('pilot.user.atlas.diagnose.is_out_of_memory', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_cling_jit_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_installation_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_atlassetup_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_out_of_space', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_nfssqlite_locking_problem', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_user_code_missing', return_value=False):

                mock_cfg.Payload.payloadstdout = 'payload.stdout'
                mock_errors.STAGEINFAILED = errors.STAGEINFAILED
                mock_errors.XRDACCESSRESTRICTED = errors.XRDACCESSRESTRICTED
                mock_errors.UNKNOWNPAYLOADFAILURE = errors.UNKNOWNPAYLOADFAILURE
                mock_errors.add_error_code.side_effect = _add_error_code

                interpret_payload_exit_info(job)

        codes = captured.get('codes', [])
        self.assertNotIn(errors.STAGEINFAILED, codes,
                         'WriteTObject ERROR on a zero-exit remoteIO job must not set STAGEINFAILED')

    def test_writetobject_error_nonzero_exit_sets_stageinfailed(self):
        """A remoteIO job with a WriteTObject ERROR and non-zero exit must still be classified as STAGEINFAILED.

        The zero-exit guard must not suppress error detection when the payload genuinely
        failed: if exitcode != 0 the stdout scan must still run and the xrootd pattern
        must still be picked up.
        """
        stdout = (
            'AthenaMP starting\n'
            'TNetXNGFile::WriteTObject ERROR   Directory '
            'root://xrootd.data.net2.mghpcc.org//USATLAS/atlasdatadisk/rucio/mc23_13p6TeV/'
            'ed/95/DAOD_PHYSLITE.46831501._000007.pool.root.1 is not writable\n'
            'AthenaMP exiting with code 1\n'
        )
        codes, _ = self._run(stdout, has_remoteio=True)
        self.assertIn(errors.STAGEINFAILED, codes,
                      'WriteTObject ERROR on a non-zero-exit remoteIO job must still set STAGEINFAILED')

    def test_diagnostics_contains_matched_line(self):
        """The diagnostics string recorded with STAGEINFAILED must contain the matched text."""
        stdout = 'TNetXNGFile::Open ERROR opening root://xrd.example.com//store/data/mc.pool.root\n'
        codes, diags = self._run(stdout, has_remoteio=True)
        if errors.STAGEINFAILED in codes:
            self.assertTrue(any('TNetXNGFile' in d or 'root://' in d for d in diags),
                            f'diagnostics should reference the error line, got: {diags}')


class TestInterpretEarlyReturnBehaviour(unittest.TestCase):
    """Verify that the early-return logic in interpret() is not broken by the new exception."""

    def _make_job_with_error(self, error_code: int, has_remoteio: bool = False) -> MagicMock:
        """Make a mock job that already has a specific error code set.

        Args:
            error_code: The pilot error code already set on the job.
            has_remoteio: Whether the job has remoteIO input files.

        Returns:
            Mock job object with the error code pre-set.
        """
        job = MagicMock()
        job.piloterrorcodes = [error_code]
        job.piloterrordiags = ['pre-set error']
        job.transexitcode = 1
        job.exitcode = 1
        job.has_remoteio.return_value = has_remoteio
        return job

    def _call_interpret(self, job):
        """Call interpret() with all the heavy dependencies mocked away.

        Args:
            job: Mock job object.

        Returns:
            Return value of interpret().
        """
        with patch('pilot.user.atlas.diagnose.process_job_report'), \
             patch('pilot.user.atlas.diagnose.extract_special_information'), \
             patch('pilot.user.atlas.diagnose.interpret_payload_exit_info') as mock_exit, \
             patch('pilot.user.atlas.diagnose.errors') as mock_errors:

            mock_errors.NOPAYLOADMETADATA = errors.NOPAYLOADMETADATA
            mock_errors.PAYLOADEXECUTIONFAILURE = errors.PAYLOADEXECUTIONFAILURE
            mock_errors.UNKNOWNTRFFAILURE = errors.UNKNOWNTRFFAILURE

            from pilot.user.atlas.diagnose import interpret
            result = interpret(job)

        return result, mock_exit.called

    def test_specific_error_non_remoteio_aborts_diagnosis(self):
        """A non-remoteIO job with PAYLOADEXECUTIONFAILURE must abort diagnosis (return -1)."""
        job = self._make_job_with_error(errors.PAYLOADEXECUTIONFAILURE, has_remoteio=False)
        result, exit_info_called = self._call_interpret(job)
        self.assertEqual(result, -1)
        self.assertFalse(exit_info_called,
                         'interpret_payload_exit_info should not be called when aborting early')

    def test_payloadexecutionfailure_remoteio_proceeds(self):
        """A remoteIO job with PAYLOADEXECUTIONFAILURE must NOT abort — diagnosis must proceed."""
        job = self._make_job_with_error(errors.PAYLOADEXECUTIONFAILURE, has_remoteio=True)
        result, exit_info_called = self._call_interpret(job)
        self.assertNotEqual(result, -1,
                            'interpret() must not return -1 for remoteIO+PAYLOADEXECUTIONFAILURE')
        self.assertTrue(exit_info_called,
                        'interpret_payload_exit_info must be called for remoteIO jobs')

    def test_oom_error_always_aborts_diagnosis(self):
        """PAYLOADOUTOFMEMORY (already set) must always abort regardless of remoteIO."""
        for remoteio in (True, False):
            with self.subTest(remoteio=remoteio):
                job = self._make_job_with_error(errors.PAYLOADOUTOFMEMORY, has_remoteio=remoteio)
                result, _ = self._call_interpret(job)
                self.assertEqual(result, -1,
                                 f'PAYLOADOUTOFMEMORY should abort diagnosis (remoteio={remoteio})')


if __name__ == '__main__':
    unittest.main()
