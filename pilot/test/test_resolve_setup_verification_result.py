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

"""Unit tests for Executor.resolve_setup_verification_result().

Covers the regression reported 2026-07-22: ALRB's apptainer 'buildcfg'
version-probe failing with "unknown shorthand flag" / "unknown flag:" was
leaking through as a non-zero exit code for the *whole* setup-verification
wrapper, even though the setup script itself (a different apptainer
subcommand entirely) ran to completion inside the container and reached its
own "Done." completion marker. Genuine setup failures (no "Done." marker, or
a non-ambiguous apptainer/singularity pattern) must still be reported as
failures.
"""

import logging
import sys
import unittest

from pilot.common.errorcodes import ErrorCodes
from pilot.control.payloads.generic import Executor

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()

# The exact production stderr reported for job 7229988025 (praguelcg2/LUMI, 2026-07-22):
# ALRB's apptainer buildcfg version-probe failing, unrelated to the setup itself.
_BUILDCFG_PROBE_STDERR = (
    'apptainerFunctions.sh: line 92: let: alrb_contVerN=Error:: syntax error '
    'in expression (error token is ":")\n'
    'Error for command "buildcfg": unknown shorthand flag: \'B\' in -B\n'
    'Options for buildcfg command:\n'
    '  -h, --help   help for buildcfg\n'
    "Run 'singularity --help' for more detailed usage information.\n"
)

# stdout as it would appear once the setup script (asetup etc.) ran fully and
# reached the 'echo "Done."' marker appended by run().
_SUCCESSFUL_SETUP_STDOUT = (
    'Using AthGeneration/23.6.26 [cmake] with platform x86_64-el9-gcc13-opt\n'
    'Unchanged: COOL_ORA_ENABLE_ADAPTIVE_OPT=Y\n'
    '19:57:45 2026/07/22\n'
    'Done.\n'
)


class TestResolveSetupVerificationResultBuildcfgFalsePositive(unittest.TestCase):
    """Regression: buildcfg-probe noise must not fail a setup that actually completed."""

    def test_buildcfg_probe_with_done_marker_is_ignored(self):
        """Exact production case: exit_code=1, buildcfg probe noise, 'Done.' present -> success."""
        diagnostics = _BUILDCFG_PROBE_STDERR + _SUCCESSFUL_SETUP_STDOUT
        exit_code, diagnostics_out = Executor.resolve_setup_verification_result(
            1, _SUCCESSFUL_SETUP_STDOUT, diagnostics
        )
        self.assertEqual(exit_code, 0)
        self.assertEqual(diagnostics_out, "")

    def test_unknown_flag_with_done_marker_is_ignored(self):
        """The 'unknown flag:' variant is also ignored when 'Done.' is present."""
        stdout = "some setup output\nDone.\n"
        diagnostics = "Error: unknown flag: --bind\n" + stdout
        exit_code, diagnostics_out = Executor.resolve_setup_verification_result(1, stdout, diagnostics)
        self.assertEqual(exit_code, 0)
        self.assertEqual(diagnostics_out, "")

    def test_done_marker_must_be_its_own_line(self):
        """A 'Done.' substring embedded in other text does not count as the marker."""
        stdout = "SomeToolDone.SomethingElse\n"
        diagnostics = _BUILDCFG_PROBE_STDERR + stdout
        exit_code, _ = Executor.resolve_setup_verification_result(1, stdout, diagnostics)
        # no genuine 'Done.' line -> buildcfg pattern is authoritative -> failure
        self.assertEqual(exit_code, errors.SINGULARITYGENERALFAILURE)


class TestResolveSetupVerificationResultGenuineFailures(unittest.TestCase):
    """Genuine setup failures must still be reported, even with similar-looking input."""

    def test_buildcfg_probe_without_done_marker_still_fails(self):
        """If the container never reached 'Done.', the buildcfg pattern is trusted as usual."""
        stdout = "some partial output, container did not finish\n"
        diagnostics = _BUILDCFG_PROBE_STDERR + stdout
        exit_code, diagnostics_out = Executor.resolve_setup_verification_result(1, stdout, diagnostics)
        self.assertEqual(exit_code, errors.SINGULARITYGENERALFAILURE)
        self.assertNotEqual(diagnostics_out, "")

    def test_genuine_mount_failure_still_fails_even_with_done_marker(self):
        """A real mount-type apptainer failure is not exempted by the 'Done.' safety net.

        (Only the two ambiguous buildcfg-probe patterns get the 'Done.'
        exemption; other apptainer/singularity patterns remain authoritative.)
        """
        stdout = "Done.\n"
        diagnostics = "Failed to mount image /cvmfs/atlas.cern.ch\n" + stdout
        exit_code, diagnostics_out = Executor.resolve_setup_verification_result(1, stdout, diagnostics)
        self.assertEqual(exit_code, errors.SINGULARITYIMAGEMOUNTFAILURE)
        self.assertNotEqual(diagnostics_out, "")

    def test_unrecognised_error_with_no_pattern_falls_back(self):
        """No pattern match at all falls back to PAYLOADEXECUTIONFAILURE as before."""
        stdout = "some unrelated setup problem\n"
        exit_code, _ = Executor.resolve_setup_verification_result(1, stdout, stdout)
        self.assertEqual(exit_code, errors.PAYLOADEXECUTIONFAILURE)

    def test_empty_stdout_does_not_crash(self):
        """Empty/None-like stdout must not raise and must not be treated as the 'Done.' marker."""
        exit_code, _ = Executor.resolve_setup_verification_result(1, "", _BUILDCFG_PROBE_STDERR)
        self.assertEqual(exit_code, errors.SINGULARITYGENERALFAILURE)


if __name__ == "__main__":
    unittest.main()
