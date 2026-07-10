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

"""Unit tests for nvidia-smi CUDA version parsing in pilot.util.workernode.

Covers:
- _extract_cuda_version(): legacy header format, the newer KMD/UMD-split
  header format (nvidia-smi >= 610.x), and the case where neither known
  pattern matches.
- get_gpu_info(): end-to-end behaviour for both known formats, and the
  fallback/logging behaviour when the format is unrecognised (framework_version
  falls back to "Unknown" and the full nvidia-smi output is logged so the new
  format can be diagnosed and a new pattern added).
"""

import logging
import subprocess
import sys
import unittest
from unittest.mock import patch

from pilot.util.workernode import (
    CUDA_VERSION_PATTERNS,
    _extract_cuda_version,
    get_gpu_info,
)

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# Legacy nvidia-smi header (e.g. driver 535.x series).
LEGACY_OUTPUT = """
----- nvidia-smi -----
Thu Jul  9 23:23:10 2026
+-----------------------------------------------------------------------------------+
| NVIDIA-SMI 535.129.03             Driver Version: 535.129.03   CUDA Version: 12.2 |
|-----------------------------------------+----------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id        Disp.A | Volatile Uncorr. ECC |
+-----------------------------------------------------------------------------------+
"""

# New nvidia-smi header (KMD/CUDA-UMD split), as seen in the reported case.
NEW_FORMAT_OUTPUT = """
----- nvidia-smi -----
Thu Jul  9 23:23:10 2026
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 610.43.02              KMD Version: 610.43.02     CUDA UMD Version: 13.3     |
+-----------------------------------------+------------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|                                         |                        |               MIG M. |
|=========================================+========================+======================|
|   0  Tesla T4                       On  |   00000000:5E:00.0 Off |                    0 |
| N/A   30C    P8             13W /   70W |       3MiB /  15360MiB |      0%   E. Process |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+
"""

# A hypothetical future format that doesn't match any known pattern at all,
# to exercise the "unknown format" fallback/logging path.
UNKNOWN_FORMAT_OUTPUT = """
----- nvidia-smi -----
Thu Jul  9 23:23:10 2026
+-----------------------------------------------------------------------------------+
| NVIDIA-SMI 720.10.01              Kernel Module: 720.10.01   Runtime: 14.0        |
+-----------------------------------------------------------------------------------+
"""

# A hypothetical future relabeling that doesn't match either exact pattern, but
# is still recognisable by the generic "CUDA ... Version:" fallback pattern.
GENERIC_FALLBACK_OUTPUT = """
----- nvidia-smi -----
Thu Jul  9 23:23:10 2026
+-----------------------------------------------------------------------------------+
| NVIDIA-SMI 720.10.01              Driver Version: 720.10.01   CUDA Driver Version: 14.0 |
+-----------------------------------------------------------------------------------+
"""


class TestExtractCudaVersion(unittest.TestCase):
    """Tests for _extract_cuda_version()."""

    def test_legacy_format(self):
        """The legacy 'CUDA Version:' pattern must still be matched."""
        self.assertEqual(_extract_cuda_version(LEGACY_OUTPUT), "12.2")

    def test_new_kmd_umd_format(self):
        """The new 'CUDA UMD Version:' pattern (nvidia-smi >= 610.x) must be matched."""
        self.assertEqual(_extract_cuda_version(NEW_FORMAT_OUTPUT), "13.3")

    def test_unknown_format_returns_none(self):
        """An unrecognised header format must return None (not raise, not guess)."""
        self.assertIsNone(_extract_cuda_version(UNKNOWN_FORMAT_OUTPUT))

    def test_generic_fallback_matches_unseen_relabeling(self):
        """A plausible future relabeling ('CUDA Driver Version:') not covered by
        either exact pattern must still be caught by the generic fallback pattern.
        """
        self.assertEqual(_extract_cuda_version(GENERIC_FALLBACK_OUTPUT), "14.0")

    def test_exact_patterns_take_precedence_over_generic_fallback(self):
        """When an exact pattern matches, it must be used rather than falling
        through to the generic pattern (both would in fact agree here, but this
        pins down the intended trying order for future maintainers).
        """
        self.assertEqual(CUDA_VERSION_PATTERNS.index(r'CUDA Version:\s+([\d.]+)'), 0)
        self.assertEqual(CUDA_VERSION_PATTERNS.index(r'CUDA UMD Version:\s+([\d.]+)'), 1)
        self.assertEqual(_extract_cuda_version(LEGACY_OUTPUT), "12.2")
        self.assertEqual(_extract_cuda_version(NEW_FORMAT_OUTPUT), "13.3")

    def test_empty_output_returns_none(self):
        """Empty input must return None gracefully."""
        self.assertIsNone(_extract_cuda_version(""))


class TestGetGpuInfo(unittest.TestCase):
    """Tests for get_gpu_info(), mocking the two nvidia-smi subprocess calls."""

    @staticmethod
    def _completed(stdout: str) -> subprocess.CompletedProcess:
        """Build a CompletedProcess-like object carrying the given stdout."""
        return subprocess.CompletedProcess(args=['nvidia-smi'], returncode=0, stdout=stdout, stderr='')

    @patch('pilot.util.workernode.subprocess.run')
    def test_legacy_format_reports_cuda_version(self, mock_run):
        """get_gpu_info() must report the CUDA version parsed from a legacy-format header."""
        mock_run.side_effect = [
            self._completed(LEGACY_OUTPUT),
            self._completed('Tesla T4, 15360, 535.129.03\n'),
        ]
        info = get_gpu_info(site='TEST_SITE')
        self.assertEqual(info['framework_version'], '12.2')
        self.assertEqual(info['driver_version'], '535.129.03')
        self.assertEqual(info['model'], 'Tesla T4')

    @patch('pilot.util.workernode.subprocess.run')
    def test_new_format_reports_cuda_version(self, mock_run):
        """get_gpu_info() must report the CUDA version parsed from the new KMD/UMD-split header."""
        mock_run.side_effect = [
            self._completed(NEW_FORMAT_OUTPUT),
            self._completed('Tesla T4, 15360, 610.43.02\n'),
        ]
        info = get_gpu_info(site='TEST_SITE')
        self.assertEqual(info['framework_version'], '13.3')
        self.assertEqual(info['driver_version'], '610.43.02')
        self.assertEqual(info['model'], 'Tesla T4')

    @patch('pilot.util.workernode.subprocess.run')
    def test_generic_fallback_format_reports_cuda_version(self, mock_run):
        """get_gpu_info() must report the CUDA version via the generic fallback
        pattern when a future relabeling isn't covered by an exact pattern.
        """
        mock_run.side_effect = [
            self._completed(GENERIC_FALLBACK_OUTPUT),
            self._completed('Tesla T4, 15360, 720.10.01\n'),
        ]
        info = get_gpu_info(site='TEST_SITE')
        self.assertEqual(info['framework_version'], '14.0')
        self.assertEqual(info['driver_version'], '720.10.01')

    @patch('pilot.util.workernode.subprocess.run')
    def test_unknown_format_falls_back_and_logs_full_output(self, mock_run):
        """get_gpu_info() must fall back to 'Unknown' and log the full nvidia-smi
        output (not just "Unknown") when no known CUDA version pattern matches,
        so a future format change can be diagnosed directly from the pilot log.
        """
        mock_run.side_effect = [
            self._completed(UNKNOWN_FORMAT_OUTPUT),
            self._completed('Tesla T4, 15360, 720.10.01\n'),
        ]
        with self.assertLogs('pilot.util.workernode', level='WARNING') as cm:
            info = get_gpu_info(site='TEST_SITE')

        self.assertEqual(info['framework_version'], 'Unknown')
        # the full raw nvidia-smi output must be present in the warning for diagnosis
        logged = '\n'.join(cm.output)
        self.assertIn('720.10.01', logged)
        self.assertIn('Kernel Module: 720.10.01', logged)

    @patch('pilot.util.workernode.subprocess.run')
    def test_known_format_does_not_log_warning(self, mock_run):
        """No warning should be logged when a known CUDA version pattern matches."""
        mock_run.side_effect = [
            self._completed(NEW_FORMAT_OUTPUT),
            self._completed('Tesla T4, 15360, 610.43.02\n'),
        ]
        with self.assertNoLogs('pilot.util.workernode', level='WARNING'):
            get_gpu_info(site='TEST_SITE')

    @patch('pilot.util.workernode.subprocess.run')
    def test_called_process_error_returns_empty_dict(self, mock_run):
        """get_gpu_info() must return {} (not raise) if nvidia-smi itself fails."""
        mock_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd=['nvidia-smi'], stderr='no devices found')
        info = get_gpu_info(site='TEST_SITE')
        self.assertEqual(info, {})


if __name__ == '__main__':
    unittest.main()
