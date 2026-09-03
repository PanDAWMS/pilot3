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

Also covers the regression reported 2026-09-02 (job 7291003889,
UNI-SIEGEN-HEP): the containerised setup verification hung and was killed by
execute() after 600 s (COMMANDTIMEDOUT). The container had written its
output to setup.stdout only, leaving setup.stderr empty, so re-reading both
files overwrote the in-memory stderr holding the TimeoutExpired reason. The
resulting placeholder ("General payload setup verification error ...") was
itself an error_map pattern, so the pilot matched its own text, logged
"found apptainer error in stderr" and reclassified 1367 -> 1110. A timed-out
setup verification must now be reported as SETUPTIMEDOUT with the time-out
reason preserved in the diagnostics.
"""

import logging
import os
import shutil
import sys
import tempfile
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


# The exact production stderr returned by execute() for job 7291003889
# (UNI-SIEGEN-HEP, 2026-09-02) when the setup verification container hung.
_TIMEOUT_STDERR = (
    "subprocess communicate sent TimeoutExpired: Command '['/bin/bash', '-c', "
    "'export X509_USER_PROXY=/var/lib/condor/execute/dir_18031/x509up_u25606_prod;"
    "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c $thePlatform "
    "-s /srv/my_release_setup.sh -r /srv/container_script.sh']' timed out after "
    "599.9998625442386 seconds"
)

# setup.stdout as written by the container before it stalled: the ALRB banner and
# the start of asetup, but no 'Done.' marker.
_TRUNCATED_SETUP_STDOUT = (
    "Info: /cvmfs mounted; do 'setupATLAS -d -c ...' to skip default mounts.\n"
    "Apptainer: 1.2.2\n"
    " sourcing /srv/my_release_setup.sh \n"
    "Using AthGeneration/23.6.11 [cmake] with platform x86_64-centos7-gcc11-opt\n"
)


class TestResolveSetupVerificationResultTimeout(unittest.TestCase):
    """A timed-out setup verification is reported as a time-out, not a pattern match."""

    def test_timeout_is_reported_as_setuptimedout(self):
        """Exact production case: COMMANDTIMEDOUT -> SETUPTIMEDOUT."""
        diagnostics = _TIMEOUT_STDERR + _TRUNCATED_SETUP_STDOUT
        exit_code, diagnostics_out = Executor.resolve_setup_verification_result(
            errors.COMMANDTIMEDOUT, _TRUNCATED_SETUP_STDOUT, diagnostics, stderr=_TIMEOUT_STDERR
        )
        self.assertEqual(exit_code, errors.SETUPTIMEDOUT)
        self.assertIn("Payload setup verification timed out", diagnostics_out)
        self.assertIn("600 s", diagnostics_out)
        self.assertLessEqual(len(diagnostics_out), 256)

    def test_timeout_diagnostics_falls_back_to_combined_output(self):
        """With no separate stderr the combined diagnostics is still used."""
        exit_code, diagnostics_out = Executor.resolve_setup_verification_result(
            errors.COMMANDTIMEDOUT, _TRUNCATED_SETUP_STDOUT, _TRUNCATED_SETUP_STDOUT
        )
        self.assertEqual(exit_code, errors.SETUPTIMEDOUT)
        self.assertNotEqual(diagnostics_out, "")

    def test_placeholder_is_not_reclassified_as_setupfailure(self):
        """The pilot's own placeholder text must not be pattern-matched.

        This is what turned the time-out into SETUPFAILURE (1110) with the
        misleading "found apptainer error in stderr" warning.
        """
        placeholder = "setup verification failed without any output (check setup logs)"
        exit_code, _ = Executor.resolve_setup_verification_result(
            errors.COMMANDTIMEDOUT, "", placeholder, stderr=""
        )
        self.assertEqual(exit_code, errors.SETUPTIMEDOUT)
        self.assertNotEqual(exit_code, errors.SETUPFAILURE)


class TestCollectSetupDiagnostics(unittest.TestCase):
    """The in-memory output from execute() must not be lost to an empty file."""

    def setUp(self):
        """Create a temporary work directory with setup.stdout/setup.stderr paths."""
        self.workdir = tempfile.mkdtemp()
        self.stdout_filename = os.path.join(self.workdir, "setup.stdout")
        self.stderr_filename = os.path.join(self.workdir, "setup.stderr")

    def tearDown(self):
        """Remove the temporary work directory."""
        shutil.rmtree(self.workdir, ignore_errors=True)

    def _write(self, path: str, content: str):
        """Write content to the given path."""
        with open(path, "w", encoding="utf-8") as _file:
            _file.write(content)

    def test_timeout_stderr_survives_empty_stderr_file(self):
        """Exact production case: stdout only on disk, time-out reason only in memory."""
        self._write(self.stdout_filename, _TRUNCATED_SETUP_STDOUT)
        self._write(self.stderr_filename, "")  # the container wrote everything to stdout

        stdout, stderr, diagnostics = Executor.collect_setup_diagnostics(
            "", _TIMEOUT_STDERR, self.stdout_filename, self.stderr_filename
        )
        self.assertEqual(stderr, _TIMEOUT_STDERR)
        self.assertIn("AthGeneration/23.6.11", stdout)
        self.assertIn("timed out after", diagnostics)
        self.assertIn("AthGeneration/23.6.11", diagnostics)

    def test_stdout_only_is_enough_for_diagnostics(self):
        """A failure with output on stdout alone does not fall back to a placeholder."""
        self._write(self.stdout_filename, "asetup: release not found\n")
        self._write(self.stderr_filename, "")

        _, _, diagnostics = Executor.collect_setup_diagnostics(
            "", "", self.stdout_filename, self.stderr_filename
        )
        self.assertEqual(diagnostics, "asetup: release not found\n")

    def test_placeholder_only_when_there_is_no_output_at_all(self):
        """With nothing anywhere, a placeholder is used - and it matches no pattern."""
        _, _, diagnostics = Executor.collect_setup_diagnostics(
            "", "", self.stdout_filename, self.stderr_filename
        )
        self.assertNotEqual(diagnostics, "")
        _exit_code, error_message = errors.resolve_transform_error(
            errors.COMMANDTIMEDOUT, diagnostics
        )
        self.assertEqual(error_message, "")
        self.assertEqual(_exit_code, errors.COMMANDTIMEDOUT)

    def test_in_memory_output_takes_precedence_over_files(self):
        """What execute() returned is kept; the files only fill in what is missing."""
        self._write(self.stdout_filename, "from file\n")
        self._write(self.stderr_filename, "from file\n")

        stdout, stderr, _ = Executor.collect_setup_diagnostics(
            "in memory out", "in memory err", self.stdout_filename, self.stderr_filename
        )
        self.assertEqual(stdout, "in memory out")
        self.assertEqual(stderr, "in memory err")

    def test_missing_files_do_not_raise(self):
        """Absent setup.stdout/setup.stderr must not raise."""
        stdout, stderr, diagnostics = Executor.collect_setup_diagnostics(
            "", "", os.path.join(self.workdir, "nosuch.stdout"),
            os.path.join(self.workdir, "nosuch.stderr")
        )
        self.assertEqual(stdout, "")
        self.assertEqual(stderr, "")
        self.assertNotEqual(diagnostics, "")


if __name__ == "__main__":
    unittest.main()
