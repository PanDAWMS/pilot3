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
from pilot.user.atlas.diagnose import is_cling_jit_error

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


if __name__ == '__main__':
    unittest.main()
