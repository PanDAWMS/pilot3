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

"""Unit tests for ErrorCodes.resolve_transform_error().

Covers:
- Pre-existing apptainer/singularity pattern matching (authoritative
  regardless of exit_code, since these indicate the container itself
  failed to mount/start).
- Ambiguous patterns (unknown shorthand flag, unknown flag:, No such file
  or directory), which are only authoritative when exit_code != 0 (see
  class docstring).
- Regression: these ambiguous patterns must NOT override an already
  successful (exit_code=0) transform result:
    * 2026-07-22: an ALRB apptainer buildcfg version-probe failed with
      "unknown shorthand flag" but the job's actual container and payload
      succeeded.
    * 2026-07-30: a batch-system 'mktemp' call failed with "No such file
      or directory" (an environment setup script, unrelated to the
      container) while the payload transform completed with exit code 0.
- The loop-guard fix: non-ambiguous patterns are returned for *any* exit
  code, not only when exit_code == 0.
- Numeric exit-code fallbacks (2, 3, 251, -1, COMMANDTIMEDOUT).
- Regression: no-match path still returns PAYLOADEXECUTIONFAILURE for
  unrecognised non-zero exit codes.
- Regression (job 7291003889, 2026-09-02): nothing may be returned as an
  "error message found in stderr" unless it was really found in stderr, and
  an exit code that is already a pilot error code must be passed through
  instead of being replaced by PAYLOADEXECUTIONFAILURE.
"""

import logging
import sys
import unittest

from pilot.common.errorcodes import ErrorCodes

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()


class TestResolveTransformErrorExistingPatterns(unittest.TestCase):
    """Pre-existing pattern entries in error_map."""

    def test_bind_point_failure(self):
        """'Not mounting requested bind point' maps to SINGULARITYBINDPOINTFAILURE."""
        ec, msg = errors.resolve_transform_error(1, "Not mounting requested bind point /tmp")
        self.assertEqual(ec, errors.SINGULARITYBINDPOINTFAILURE)
        self.assertIn("Not mounting requested bind point", msg)

    def test_no_loop_devices(self):
        """'No more available loop devices' maps to SINGULARITYNOLOOPDEVICES."""
        ec, msg = errors.resolve_transform_error(1, "No more available loop devices")
        self.assertEqual(ec, errors.SINGULARITYNOLOOPDEVICES)

    def test_failed_to_mount_image(self):
        """'Failed to mount image' maps to SINGULARITYIMAGEMOUNTFAILURE."""
        ec, msg = errors.resolve_transform_error(1, "Failed to mount image /cvmfs/atlas.cern.ch")
        self.assertEqual(ec, errors.SINGULARITYIMAGEMOUNTFAILURE)

    def test_error_while_mounting(self):
        """'error: while mounting' maps to SINGULARITYIMAGEMOUNTFAILURE."""
        ec, msg = errors.resolve_transform_error(1, "error: while mounting overlay")
        self.assertEqual(ec, errors.SINGULARITYIMAGEMOUNTFAILURE)

    def test_operation_not_permitted(self):
        """'Operation not permitted' maps to SINGULARITYGENERALFAILURE."""
        ec, msg = errors.resolve_transform_error(1, "Operation not permitted")
        self.assertEqual(ec, errors.SINGULARITYGENERALFAILURE)

    def test_failed_to_create_user_namespace(self):
        """'Failed to create user namespace' maps to SINGULARITYFAILEDUSERNAMESPACE."""
        ec, msg = errors.resolve_transform_error(1, "Failed to create user namespace")
        self.assertEqual(ec, errors.SINGULARITYFAILEDUSERNAMESPACE)

    def test_singularity_not_installed(self):
        """'Singularity is not installed' maps to SINGULARITYNOTINSTALLED."""
        ec, msg = errors.resolve_transform_error(1, "Singularity is not installed")
        self.assertEqual(ec, errors.SINGULARITYNOTINSTALLED)

    def test_apptainer_not_installed(self):
        """'Apptainer is not installed' maps to APPTAINERNOTINSTALLED."""
        ec, msg = errors.resolve_transform_error(1, "Apptainer is not installed")
        self.assertEqual(ec, errors.APPTAINERNOTINSTALLED)

    def test_cannot_create_directory(self):
        """'cannot create directory' maps to MKDIR."""
        ec, msg = errors.resolve_transform_error(1, "cannot create directory /tmp/foo")
        self.assertEqual(ec, errors.MKDIR)


class TestResolveTransformErrorAmbiguousPatterns(unittest.TestCase):
    """Patterns that are only authoritative when exit_code != 0.

    'unknown shorthand flag' / 'unknown flag:' arise when ALRB's
    apptainerFunctions.sh probes the apptainer binary with 'apptainer
    buildcfg' and the binary does not recognise a CLI flag (e.g. -B). This
    probe is a *different* apptainer subcommand from the one that actually
    launches the payload container ('apptainer exec'), which commonly
    accepts flags that 'buildcfg' rejects. Confirmed in production
    (ATLASPANDA report, 2026-07-22): the buildcfg probe failed with this
    exact message while the job's container started normally and the payload
    completed with trf exit code 0.

    'No such file or directory' is included here for the same reason, and is
    even more prone to false positives since it is a generic OS error
    message rather than something apptainer/singularity-specific. Confirmed
    in production (2026-07-30): a batch-system 'mktemp' call in an
    environment setup script failed with this exact message, unrelated to
    the container or the payload, while the transform completed with exit
    code 0.

    Because of this, these patterns are only trusted as a failure signal
    when exit_code is already non-zero; they must not override an
    already-successful (exit_code=0) result.
    """

    # --- unknown shorthand flag ---

    def test_unknown_shorthand_flag_exit1(self):
        """'unknown shorthand flag' with exit_code=1 maps to SINGULARITYGENERALFAILURE."""
        stderr = (
            'alrb_contVerN=Error: syntax error in expression (error token is ":")\n'
            "Error for command \"buildcfg\": unknown shorthand flag: 'B' in -B\n"
            "Options for buildcfg command:\n"
            "  -h, --help   help for\n"
        )
        ec, msg = errors.resolve_transform_error(1, stderr)
        self.assertEqual(ec, errors.SINGULARITYGENERALFAILURE)
        self.assertIn("unknown shorthand flag", msg)

    def test_unknown_shorthand_flag_minimal(self):
        """Minimal 'unknown shorthand flag' string with exit_code=1 maps to SINGULARITYGENERALFAILURE."""
        ec, msg = errors.resolve_transform_error(1, "unknown shorthand flag: 'B' in -B")
        self.assertEqual(ec, errors.SINGULARITYGENERALFAILURE)

    def test_unknown_shorthand_flag_not_partial_match(self):
        """'unknown shorthand' without 'flag' does not trigger the pattern."""
        # Should fall through to numeric fallback (exit_code=1 → PAYLOADEXECUTIONFAILURE)
        ec, _ = errors.resolve_transform_error(1, "unknown shorthand option specified")
        self.assertEqual(ec, errors.PAYLOADEXECUTIONFAILURE)

    def test_unknown_shorthand_flag_exit0_does_not_override_success(self):
        """Regression: buildcfg-probe noise must not override a successful (exit_code=0) transform.

        This reproduces the production false-positive: the ALRB buildcfg
        version-probe fails with this exact message, but the job's actual
        container invocation succeeded and the transform reported exit
        code 0 (output file validated, event count passed). The pilot must
        not reclassify this as SINGULARITYGENERALFAILURE.
        """
        stderr = (
            'alrb_contVerN=Error: syntax error in expression (error token is ":")\n'
            "Error for command \"buildcfg\": unknown shorthand flag: 'B' in -B\n"
            "Options for buildcfg command:\n"
            "  -h, --help   help for\n"
            "Run 'singularity --help' for more detailed usage information.\n"
        )
        ec, msg = errors.resolve_transform_error(0, stderr)
        self.assertEqual(ec, 0, "exit_code=0 must be preserved despite the buildcfg probe noise")
        self.assertEqual(msg, "")

    # --- unknown flag: ---

    def test_unknown_flag_exit1(self):
        """'unknown flag:' with exit_code=1 maps to SINGULARITYGENERALFAILURE."""
        ec, msg = errors.resolve_transform_error(1, "Error: unknown flag: --bind")
        self.assertEqual(ec, errors.SINGULARITYGENERALFAILURE)
        self.assertIn("unknown flag:", msg)

    def test_unknown_flag_minimal(self):
        """Minimal 'unknown flag:' string with exit_code=1 maps to SINGULARITYGENERALFAILURE."""
        ec, msg = errors.resolve_transform_error(1, "unknown flag: --some-option")
        self.assertEqual(ec, errors.SINGULARITYGENERALFAILURE)

    def test_unknown_flag_without_colon_does_not_match(self):
        """'unknown flag' without trailing colon does not trigger the pattern."""
        ec, _ = errors.resolve_transform_error(1, "unknown flag passed to function")
        self.assertEqual(ec, errors.PAYLOADEXECUTIONFAILURE)

    def test_unknown_flag_exit0_does_not_override_success(self):
        """Regression: 'unknown flag:' noise must not override a successful (exit_code=0) transform."""
        ec, msg = errors.resolve_transform_error(0, "Error: unknown flag: --bind")
        self.assertEqual(ec, 0)
        self.assertEqual(msg, "")

    # --- No such file or directory ---

    def test_no_such_file_exit1(self):
        """'No such file or directory' with exit_code=1 maps to NOSUCHFILE."""
        ec, msg = errors.resolve_transform_error(1, "No such file or directory: /cvmfs/atlas")
        self.assertEqual(ec, errors.NOSUCHFILE)
        self.assertIn("No such file or directory", msg)

    def test_no_such_file_exit0_does_not_override_success(self):
        """Regression: benign 'No such file or directory' noise must not override a successful (exit_code=0) transform.

        Reproduces the production false-positive reported 2026-07-30: an
        asetup-related 'mktemp' call in payload.stderr failed with "No such
        file or directory" while payload.stdout confirmed the transform
        finished normally (trf exit code 0). The pilot must not reclassify
        this as NOSUCHFILE.
        """
        stderr = (
            "mktemp: failed to create file via template "
            "'/tmp_local/condor/execute/dir_2556354/asetup_XXXXXX.sh': "
            "No such file or directory\n"
        )
        ec, msg = errors.resolve_transform_error(0, stderr)
        self.assertEqual(ec, 0, "exit_code=0 must be preserved despite the benign mktemp failure")
        self.assertEqual(msg, "")


class TestResolveTransformErrorLoopGuardFix(unittest.TestCase):
    """Verify the loop guard fix: patterns are matched for any exit code.

    Regression for the old behaviour where exit_code != 0 caused the loop
    to 'continue' past every match, falling through to PAYLOADEXECUTIONFAILURE.
    """

    def test_pattern_matched_with_nonzero_exit_code(self):
        """A pattern match overrides exit_code=1 (was previously silently ignored)."""
        ec, msg = errors.resolve_transform_error(1, "Not mounting requested bind point /mnt")
        self.assertNotEqual(ec, errors.PAYLOADEXECUTIONFAILURE,
                            "Should not fall through to PAYLOADEXECUTIONFAILURE when a pattern matches")
        self.assertEqual(ec, errors.SINGULARITYBINDPOINTFAILURE)

    def test_pattern_matched_with_exit_code_zero(self):
        """A pattern match is also returned when exit_code=0 (apptainer wrote to stderr but exited 0)."""
        ec, msg = errors.resolve_transform_error(0, "Apptainer is not installed")
        self.assertEqual(ec, errors.APPTAINERNOTINSTALLED)

    def test_pattern_matched_with_large_exit_code(self):
        """A pattern match overrides any non-zero exit code, including large ones."""
        ec, _ = errors.resolve_transform_error(127, "Failed to create user namespace")
        self.assertEqual(ec, errors.SINGULARITYFAILEDUSERNAMESPACE)

    def test_no_match_nonzero_still_payloadexecutionfailure(self):
        """Unrecognised non-zero exit code with no matching pattern returns PAYLOADEXECUTIONFAILURE."""
        ec, _ = errors.resolve_transform_error(1, "some completely unrelated error message")
        self.assertEqual(ec, errors.PAYLOADEXECUTIONFAILURE)


class TestResolveTransformErrorNumericFallbacks(unittest.TestCase):
    """Numeric exit-code fallbacks (no stderr pattern match)."""

    def test_exit_code_2_returns_lsetuptimedout(self):
        """exit_code=2 with no pattern match returns LSETUPTIMEDOUT."""
        ec, _ = errors.resolve_transform_error(2, "")
        self.assertEqual(ec, errors.LSETUPTIMEDOUT)

    def test_exit_code_3_returns_remotefileopentimedout(self):
        """exit_code=3 with no pattern match returns REMOTEFILEOPENTIMEDOUT."""
        ec, _ = errors.resolve_transform_error(3, "")
        self.assertEqual(ec, errors.REMOTEFILEOPENTIMEDOUT)

    def test_exit_code_251_returns_unknowntrffailure(self):
        """exit_code=251 with no pattern match returns UNKNOWNTRFFAILURE."""
        ec, _ = errors.resolve_transform_error(251, "")
        self.assertEqual(ec, errors.UNKNOWNTRFFAILURE)

    def test_exit_code_minus1_returns_unknowntrffailure(self):
        """exit_code=-1 with no pattern match returns UNKNOWNTRFFAILURE."""
        ec, _ = errors.resolve_transform_error(-1, "")
        self.assertEqual(ec, errors.UNKNOWNTRFFAILURE)

    def test_exit_code_commandtimedout_passthrough(self):
        """exit_code=COMMANDTIMEDOUT with no pattern match is passed through unchanged."""
        ec, _ = errors.resolve_transform_error(errors.COMMANDTIMEDOUT, "")
        self.assertEqual(ec, errors.COMMANDTIMEDOUT)

    def test_exit_code_0_no_match_returns_zero(self):
        """exit_code=0 with no pattern match returns 0 (success)."""
        ec, _ = errors.resolve_transform_error(0, "")
        self.assertEqual(ec, 0)

    def test_exit_code_1_no_match_returns_payloadexecutionfailure(self):
        """exit_code=1 with no pattern match returns PAYLOADEXECUTIONFAILURE."""
        ec, _ = errors.resolve_transform_error(1, "")
        self.assertEqual(ec, errors.PAYLOADEXECUTIONFAILURE)

    def test_return_value_is_tuple_of_int_and_str(self):
        """resolve_transform_error always returns (int, str)."""
        for ec_in, stderr in [(0, ""), (1, ""), (1, "unknown shorthand flag: x")]:
            result = errors.resolve_transform_error(ec_in, stderr)
            self.assertIsInstance(result, tuple)
            self.assertEqual(len(result), 2)
            self.assertIsInstance(result[0], int)
            self.assertIsInstance(result[1], str)


class TestResolveTransformErrorNoFabricatedMessage(unittest.TestCase):
    """Regression (job 7291003889, 2026-09-02): no invented stderr messages."""

    def test_no_error_message_without_a_stderr_match(self):
        """Regression: no error message may be reported when stderr matched nothing.

        The callers log the returned message as "found apptainer error in
        stderr: ...". A reverse look-up of the pattern maps by error code used
        to fabricate one for every exit code that happened to equal a mapped
        pilot error code - e.g. an empty payload.stderr with
        exit_code=SETUPFAILURE (1110) produced "found apptainer error in
        stderr: General payload setup verification error" (job 7291003889).
        """
        for code in (0, 1, 2, 3, -1, 251, errors.COMMANDTIMEDOUT, errors.SETUPFAILURE,
                     errors.NOSUCHFILE, errors.MKDIR, errors.SINGULARITYNOTINSTALLED,
                     errors.SINGULARITYFAILEDUSERNAMESPACE, errors.APPTAINERNOTINSTALLED):
            _, msg = errors.resolve_transform_error(code, "")
            self.assertEqual(msg, "", f"no message may be reported for exit code {code}")

    def test_placeholder_diagnostics_is_not_a_pattern(self):
        """Regression: the pilot's own setup-failure placeholder is not matched.

        "General payload setup verification error" was an error_map key while
        also being the text the pilot itself substituted when a setup
        verification produced no output, so the pilot pattern-matched its own
        placeholder and reclassified COMMANDTIMEDOUT to SETUPFAILURE.
        """
        placeholder = "General payload setup verification error (check setup logs)"
        ec, msg = errors.resolve_transform_error(errors.COMMANDTIMEDOUT, placeholder)
        self.assertEqual(ec, errors.COMMANDTIMEDOUT)
        self.assertEqual(msg, "")

    def test_pilot_error_code_is_passed_through(self):
        """An exit code that is already a pilot error code is not rewritten."""
        for code in (errors.SETUPFAILURE, errors.SETUPTIMEDOUT, errors.NOSUCHFILE,
                     errors.SINGULARITYBINDPOINTFAILURE, errors.MKDIR):
            ec, _ = errors.resolve_transform_error(code, "")
            self.assertEqual(ec, code, f"pilot error code {code} must not be rewritten")


if __name__ == "__main__":
    unittest.main()
