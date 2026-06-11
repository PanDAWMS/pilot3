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

"""Unit tests for MISSINGINPUTFILE (1331) routing.

Covers two paths introduced to address Philippe Ganz's ATLASPANDA report:

1. ``resolve_common_transfer_errors()`` in ``pilot/copytool/common.py``:
   XRootD error string "No such file (source)" must now route to
   MISSINGINPUTFILE (1331), not fall through to STAGEINFAILED (1099).

2. ``parse_remotefileverification_dictionary()`` in ``pilot/user/atlas/common.py``:
   When unopened files are present and the remote file open log contains a
   "No such file" pattern, the function must return MISSINGINPUTFILE (1331)
   instead of REMOTEFILECOULDNOTBEOPENED (1361).
"""

import json
import os
import tempfile
import unittest

from pilot.common.errorcodes import ErrorCodes
from pilot.copytool.common import resolve_common_transfer_errors
from pilot.user.atlas.common import parse_remotefileverification_dictionary

errors = ErrorCodes()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_json(path: str, data: dict) -> None:
    """Write *data* as JSON to *path*."""
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(data, fh)


def _write_log(path: str, content: str) -> None:
    """Write *content* to *path*."""
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(content)


# ---------------------------------------------------------------------------
# Tests for resolve_common_transfer_errors()
# ---------------------------------------------------------------------------

class TestResolveCommonTransferErrorsMissingInput(unittest.TestCase):
    """resolve_common_transfer_errors() must map missing-file strings to MISSINGINPUTFILE."""

    # -- POSIX "No such file or directory" (pre-existing behaviour) -----------

    def test_posix_no_such_file_stagein_returns_missinginputfile(self):
        """Pre-existing: POSIX "No such file or directory" during stage-in -> 1331."""
        output = 'ERROR: No such file or directory for root://xrootd.cern.ch//atlas/file.root'
        ret = resolve_common_transfer_errors(output, is_stagein=True)
        self.assertEqual(ret['rcode'], errors.MISSINGINPUTFILE)

    def test_posix_no_such_file_stageout_returns_nosuchfile(self):
        """Pre-existing: POSIX "No such file or directory" during stage-out -> 1103 (not 1331)."""
        output = 'ERROR: No such file or directory for /tmp/output.root'
        ret = resolve_common_transfer_errors(output, is_stagein=False)
        self.assertEqual(ret['rcode'], errors.NOSUCHFILE)

    # -- XRootD [3011] "No such file (source)" (new behaviour) ---------------

    def test_xrootd_no_such_file_source_stagein_returns_missinginputfile(self):
        """NEW: XRootD [3011] "No such file (source)" during stage-in -> 1331."""
        output = ('[ERROR] Server responded with an error: '
                  '[3011] No such file (source) '
                  'root://eosatlas.cern.ch//eos/atlas/file.root')
        ret = resolve_common_transfer_errors(output, is_stagein=True)
        self.assertEqual(ret['rcode'], errors.MISSINGINPUTFILE)

    def test_xrootd_no_such_file_source_stageout_returns_stageoutfailed(self):
        """NEW: XRootD [3011] "No such file (source)" during stage-out -> STAGEOUTFAILED (not 1331)."""
        output = '[ERROR] Server responded with an error: [3011] No such file (source)'
        ret = resolve_common_transfer_errors(output, is_stagein=False)
        # is_stagein=False means the MISSINGINPUTFILE branch is not entered;
        # falls through to the default STAGEOUTFAILED.
        self.assertEqual(ret['rcode'], errors.STAGEOUTFAILED)

    def test_xrootd_no_such_file_source_state_is_missing_input(self):
        """NEW: state string for XRootD missing-file stage-in must be MISSING_INPUT."""
        output = '[ERROR] Server responded with an error: [3011] No such file (source)'
        ret = resolve_common_transfer_errors(output, is_stagein=True)
        self.assertEqual(ret['state'], 'MISSING_INPUT')

    def test_xrootd_no_such_file_source_not_confused_with_stageinfailed(self):
        """NEW: XRootD missing-file must NOT return STAGEINFAILED (1099)."""
        output = '[ERROR] Server responded with an error: [3011] No such file (source)'
        ret = resolve_common_transfer_errors(output, is_stagein=True)
        self.assertNotEqual(ret['rcode'], errors.STAGEINFAILED)

    # -- Unrelated errors must not be affected --------------------------------

    def test_timeout_returns_stageintimeout(self):
        """Timeout errors must still return STAGEINTIMEOUT, not be affected by our change."""
        output = 'ERROR: timeout while transferring root://xrootd.cern.ch//atlas/file.root'
        ret = resolve_common_transfer_errors(output, is_stagein=True)
        self.assertEqual(ret['rcode'], errors.STAGEINTIMEOUT)

    def test_checksum_mismatch_returns_getadmismatch(self):
        """Adler32 mismatch must still return GETADMISMATCH."""
        output = 'ERROR: failed xrdadler32 checksum verification'
        ret = resolve_common_transfer_errors(output, is_stagein=True)
        self.assertEqual(ret['rcode'], errors.GETADMISMATCH)

    def test_empty_output_returns_stageinfailed(self):
        """Empty output must still return the default STAGEINFAILED."""
        ret = resolve_common_transfer_errors('', is_stagein=True)
        self.assertEqual(ret['rcode'], errors.STAGEINFAILED)

    def test_generic_error_returns_stageinfailed(self):
        """An unrecognised error string must still return STAGEINFAILED."""
        ret = resolve_common_transfer_errors('some completely unknown error', is_stagein=True)
        self.assertEqual(ret['rcode'], errors.STAGEINFAILED)


# ---------------------------------------------------------------------------
# Tests for parse_remotefileverification_dictionary()
# ---------------------------------------------------------------------------

class TestParseRemotefileverificationDictionary(unittest.TestCase):
    """parse_remotefileverification_dictionary() must promote to MISSINGINPUTFILE when log shows absent file."""

    def setUp(self):
        """Create a temporary work directory for each test."""
        self.workdir = tempfile.mkdtemp()
        self.dict_path = os.path.join(self.workdir, 'remotefileverification_dictionary.json')
        self.log_path = os.path.join(self.workdir, 'remotefileslog.txt')

    # -- No files failed to open (all good) -----------------------------------

    def test_all_opened_returns_zero(self):
        """All files opened -> exit code 0, empty not_opened list."""
        _write_json(self.dict_path, {
            'root://a.cern.ch//f1.root': True,
            'root://a.cern.ch//f2.root': True,
        })
        ec, _, not_opened = parse_remotefileverification_dictionary(self.workdir)
        self.assertEqual(ec, 0)
        self.assertEqual(not_opened, [])

    # -- Files failed, no log present (fallback behaviour) -------------------

    def test_unopened_no_log_returns_remotefilecouldnotbeopened(self):
        """Unopened files + no log file -> fallback to REMOTEFILECOULDNOTBEOPENED (1361)."""
        _write_json(self.dict_path, {
            'root://a.cern.ch//f1.root': False,
        })
        # do not write a log file
        ec, _, not_opened = parse_remotefileverification_dictionary(self.workdir)
        self.assertEqual(ec, errors.REMOTEFILECOULDNOTBEOPENED)
        self.assertEqual(len(not_opened), 1)

    # -- Files failed, log present but no "No such file" pattern -------------

    def test_unopened_log_no_pattern_returns_remotefilecouldnotbeopened(self):
        """Unopened files + log with no missing-file pattern -> REMOTEFILECOULDNOTBEOPENED (1361)."""
        _write_json(self.dict_path, {'root://a.cern.ch//f1.root': False})
        _write_log(self.log_path, 'opening root://a.cern.ch//f1.root\nconnection refused\n')
        ec, _, _ = parse_remotefileverification_dictionary(self.workdir)
        self.assertEqual(ec, errors.REMOTEFILECOULDNOTBEOPENED)

    # -- Files failed, log contains POSIX "No such file or directory" --------

    def test_unopened_log_posix_pattern_returns_missinginputfile(self):
        """NEW: Unopened files + log "No such file or directory" -> MISSINGINPUTFILE (1331)."""
        _write_json(self.dict_path, {'root://a.cern.ch//f1.root': False})
        _write_log(self.log_path,
                   'opening root://a.cern.ch//f1.root\n'
                   'Error in <TNetXNGFile::Open>: [ERROR] No such file or directory\n')
        ec, _, _ = parse_remotefileverification_dictionary(self.workdir)
        self.assertEqual(ec, errors.MISSINGINPUTFILE)

    # -- Files failed, log contains XRootD "No such file (source)" -----------

    def test_unopened_log_xrootd_pattern_returns_missinginputfile(self):
        """NEW: Unopened files + log "No such file (source)" -> MISSINGINPUTFILE (1331)."""
        _write_json(self.dict_path, {'root://a.cern.ch//f1.root': False})
        _write_log(self.log_path,
                   'opening root://a.cern.ch//f1.root\n'
                   '[ERROR] Server responded with an error: [3011] No such file (source)\n')
        ec, _, _ = parse_remotefileverification_dictionary(self.workdir)
        self.assertEqual(ec, errors.MISSINGINPUTFILE)

    def test_unopened_log_xrootd_pattern_not_remotefilecouldnotbeopened(self):
        """NEW: Confirmed absent file must NOT return REMOTEFILECOULDNOTBEOPENED (1361)."""
        _write_json(self.dict_path, {'root://a.cern.ch//f1.root': False})
        _write_log(self.log_path,
                   '[ERROR] Server responded with an error: [3011] No such file (source)\n')
        ec, _, _ = parse_remotefileverification_dictionary(self.workdir)
        self.assertNotEqual(ec, errors.REMOTEFILECOULDNOTBEOPENED)

    def test_diagnostics_preserved_when_promoted(self):
        """Diagnostics string must still list the unopened TURLs after promotion."""
        turl = 'root://a.cern.ch//f1.root'
        _write_json(self.dict_path, {turl: False})
        _write_log(self.log_path, 'No such file (source)\n')
        _, diagnostics, _ = parse_remotefileverification_dictionary(self.workdir)
        self.assertIn(turl, diagnostics)

    def test_multiple_unopened_files_promoted(self):
        """Multiple unopened files must all appear in not_opened even after promotion."""
        _write_json(self.dict_path, {
            'root://a.cern.ch//f1.root': False,
            'root://a.cern.ch//f2.root': False,
            'root://a.cern.ch//f3.root': True,
        })
        _write_log(self.log_path, 'No such file (source)\n')
        ec, _, not_opened = parse_remotefileverification_dictionary(self.workdir)
        self.assertEqual(ec, errors.MISSINGINPUTFILE)
        self.assertEqual(len(not_opened), 2)

    # -- Dictionary missing (pre-existing behaviour must be unchanged) --------

    def test_missing_dictionary_returns_remotefiledictdoesnotexist(self):
        """Pre-existing: absent dictionary -> REMOTEFILEDICTDOESNOTEXIST (unrelated to our change)."""
        ec, _, _ = parse_remotefileverification_dictionary(self.workdir)
        self.assertEqual(ec, errors.REMOTEFILEDICTDOESNOTEXIST)


if __name__ == '__main__':
    unittest.main()
