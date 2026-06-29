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

"""Unit tests for cling JIT VMA-limit error detection (ALLOCATIONERROR)."""

import logging
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.user.atlas.diagnose import interpret_payload_exit_info, is_cling_jit_error

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


class TestAllocationErrorCode(unittest.TestCase):
    """Tests for the ALLOCATIONERROR constant and its message in ErrorCodes."""

    def test_error_code_value(self):
        """ALLOCATIONERROR must be assigned the value 1387."""
        errors = ErrorCodes()
        self.assertEqual(errors.ALLOCATIONERROR, 1387)

    def test_error_message_present(self):
        """ALLOCATIONERROR must have a non-empty entry in _error_messages."""
        errors = ErrorCodes()
        msg = errors.get_error_message(errors.ALLOCATIONERROR)
        self.assertIsInstance(msg, str)
        self.assertTrue(len(msg) > 0)
        self.assertNotIn("unknown error code", msg)

    def test_error_code_sequential(self):
        """ALLOCATIONERROR must directly follow PANDAQUEUENOTONLINE (1386)."""
        errors = ErrorCodes()
        self.assertEqual(errors.ALLOCATIONERROR, errors.PANDAQUEUENOTONLINE + 1)


class TestIsClingJitError(unittest.TestCase):
    """Tests for is_cling_jit_error() in pilot.user.atlas.diagnose."""

    def _make_job(self, workdir, stdout_name='payload.stdout', stderr_name='payload.stderr'):
        """Return a minimal mock job object pointing at workdir."""
        job = MagicMock()
        job.workdir = workdir

        mock_config = MagicMock()
        mock_config.Payload.payloadstdout = stdout_name
        mock_config.Payload.payloadstderr = stderr_name
        return job, mock_config

    def test_detected_in_stdout(self):
        """is_cling_jit_error returns True when the pattern is in payload stdout."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, 'payload.stdout')
            stderr_path = os.path.join(tmpdir, 'payload.stderr')
            with open(stdout_path, 'w') as f:
                f.write("some preamble\n")
                f.write("cling JIT session error: Cannot allocate memory\n")
                f.write("Traceback (most recent call last):\n")
            with open(stderr_path, 'w') as f:
                f.write("security protocol 'ztn' disallowed for non-TLS connections.\n")

            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertTrue(result)

    def test_detected_in_stderr(self):
        """is_cling_jit_error returns True when the pattern is in payload stderr."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, 'payload.stdout')
            stderr_path = os.path.join(tmpdir, 'payload.stderr')
            with open(stdout_path, 'w') as f:
                f.write("normal payload output\n")
            with open(stderr_path, 'w') as f:
                f.write("cling JIT session error: Cannot allocate memory\n")

            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertTrue(result)

    def test_detected_repeated_occurrences(self):
        """is_cling_jit_error returns True when the pattern appears multiple times (as seen in real jobs)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, 'payload.stdout')
            stderr_path = os.path.join(tmpdir, 'payload.stderr')
            with open(stderr_path, 'w') as f:
                for _ in range(11):
                    f.write("cling JIT session error: Cannot allocate memory\n")
            with open(stdout_path, 'w') as f:
                f.write("")

            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertTrue(result)

    def test_not_detected_when_absent(self):
        """is_cling_jit_error returns False when the pattern is absent from both files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, 'payload.stdout')
            stderr_path = os.path.join(tmpdir, 'payload.stderr')
            with open(stdout_path, 'w') as f:
                f.write("St9bad_alloc\n")
                f.write("std::bad_alloc\n")
            with open(stderr_path, 'w') as f:
                f.write("FATAL out of memory: taking the application down\n")

            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertFalse(result)

    def test_not_detected_on_empty_files(self):
        """is_cling_jit_error returns False when both payload files are empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            for name in ('payload.stdout', 'payload.stderr'):
                open(os.path.join(tmpdir, name), 'w').close()

            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertFalse(result)

    def test_partial_match_not_triggered(self):
        """is_cling_jit_error does not trigger on messages that merely contain 'allocate' or 'memory'."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout_path = os.path.join(tmpdir, 'payload.stdout')
            stderr_path = os.path.join(tmpdir, 'payload.stderr')
            with open(stdout_path, 'w') as f:
                f.write("Cannot allocate memory\n")          # missing 'cling JIT session error:' prefix
                f.write("JIT session error: something else\n")
            with open(stderr_path, 'w') as f:
                f.write("")

            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertFalse(result)

    def test_missing_stdout_file(self):
        """is_cling_jit_error returns False gracefully when payload stdout does not exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stderr_path = os.path.join(tmpdir, 'payload.stderr')
            with open(stderr_path, 'w') as f:
                f.write("normal stderr output\n")
            # deliberately do not create payload.stdout

            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertFalse(result)

    def test_missing_both_files(self):
        """is_cling_jit_error returns False gracefully when neither payload file exists."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # create no files at all
            job, mock_config = self._make_job(tmpdir)
            with patch('pilot.user.atlas.diagnose.config', mock_config):
                result = is_cling_jit_error(job)
            self.assertFalse(result)


class TestClingJitPriorityOverOOM(unittest.TestCase):
    """Regression tests: ALLOCATIONERROR must take priority over PAYLOADOUTOFMEMORY.

    When a cling JIT VMA-exhaustion event occurs, a secondary ``std::bad_alloc`` is
    emitted to payload.stdout by the C++ exception handler.  The OOM scanner
    (``is_out_of_memory``) matches that ``std::bad_alloc`` and would previously set
    ``PAYLOADOUTOFMEMORY`` (action: raise memory), defeating the retryModule's action 5
    (reduce input count) that ``ALLOCATIONERROR`` triggers.  After the fix,
    ``is_cling_jit_error`` is evaluated first so that the root-cause code wins.
    """

    def setUp(self):
        """Reset the class-level ErrorCodes lists before each test."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []

    def _make_job(self, workdir: str) -> MagicMock:
        """Return a minimal mock job with both payload files in workdir."""
        job = MagicMock()
        job.workdir = workdir
        job.piloterrorcodes = []
        job.piloterrordiags = []
        job.transexitcode = 0
        job.exitcode = 1
        job.has_remoteio.return_value = False
        return job

    def _make_config(self) -> MagicMock:
        cfg = MagicMock()
        cfg.Payload.payloadstdout = 'payload.stdout'
        cfg.Payload.payloadstderr = 'payload.stderr'
        return cfg

    def test_cling_jit_wins_when_both_patterns_present(self):
        """ALLOCATIONERROR must be set when stdout contains both the cling JIT pattern
        and std::bad_alloc (the real-world co-occurrence seen in job 7197402797)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            stderr = os.path.join(tmpdir, 'payload.stderr')
            with open(stdout, 'w') as fh:
                # real sequence from job 7197402797: cling JIT errors followed by
                # LLVM giving up, then EventLoop catching std::bad_alloc
                for _ in range(9):
                    fh.write("cling JIT session error: Cannot allocate memory\n")
                fh.write("LLVM ERROR: out of memory\n")
                fh.write("caught exception: std::bad_alloc\n")
            with open(stderr, 'w') as fh:
                fh.write("")

            errors = ErrorCodes()
            job = self._make_job(tmpdir)
            cfg = self._make_config()

            with patch('pilot.user.atlas.diagnose.config', cfg), \
                 patch('pilot.user.atlas.diagnose.errors', errors), \
                 patch('pilot.user.atlas.diagnose.is_installation_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_atlassetup_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_out_of_space', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_nfssqlite_locking_problem', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_user_code_missing', return_value=False):
                interpret_payload_exit_info(job)

            self.assertEqual(job.piloterrorcodes[0], errors.ALLOCATIONERROR,
                             f"expected ALLOCATIONERROR ({errors.ALLOCATIONERROR}), "
                             f"got {job.piloterrorcodes}")
            self.assertNotIn(errors.PAYLOADOUTOFMEMORY, job.piloterrorcodes,
                             "PAYLOADOUTOFMEMORY must not appear when cling JIT error is present")

    def test_oom_still_fires_without_cling_jit_pattern(self):
        """PAYLOADOUTOFMEMORY is set normally when std::bad_alloc appears without
        any cling JIT session error (genuine OOM, no VMA exhaustion)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            stdout = os.path.join(tmpdir, 'payload.stdout')
            stderr = os.path.join(tmpdir, 'payload.stderr')
            with open(stdout, 'w') as fh:
                fh.write("caught exception: std::bad_alloc\n")
            with open(stderr, 'w') as fh:
                fh.write("")

            errors = ErrorCodes()
            job = self._make_job(tmpdir)
            cfg = self._make_config()

            with patch('pilot.user.atlas.diagnose.config', cfg), \
                 patch('pilot.user.atlas.diagnose.errors', errors), \
                 patch('pilot.user.atlas.diagnose.is_installation_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_atlassetup_error', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_out_of_space', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_nfssqlite_locking_problem', return_value=False), \
                 patch('pilot.user.atlas.diagnose.is_user_code_missing', return_value=False):
                interpret_payload_exit_info(job)

            self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADOUTOFMEMORY,
                             f"expected PAYLOADOUTOFMEMORY ({errors.PAYLOADOUTOFMEMORY}), "
                             f"got {job.piloterrorcodes}")
            self.assertNotIn(errors.ALLOCATIONERROR, job.piloterrorcodes,
                             "ALLOCATIONERROR must not appear for genuine OOM without cling JIT pattern")


if __name__ == '__main__':
    unittest.main()
