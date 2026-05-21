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

"""Unit tests for ``--forceDirectIO`` / ``prodDBlockToken`` handling.

When a user submits with ``--forceDirectIO``, PanDA/JEDI omits the ``'local'``
``prodDBlockToken`` for RAW input files that would otherwise carry it.  The
pilot must use ``storage_token`` (the internal name for ``prodDBlockToken``) as
the per-file copy/direct signal rather than relying on hard-coded filename
patterns.

Covers:
- ``JobData.prepare_infiles`` accessmode assignment for production jobs:
  ``storage_token='local'`` forces copy; ``storage_token`` not ``'local'``
  permits direct access for non-lib data files.
- ``FileSpec.is_directaccess``: RAW files honour ``accessmode='direct'`` while
  lib tarballs remain unconditionally excluded.
"""

import unittest
from unittest.mock import MagicMock

from pilot.info.filespec import FileSpec
from pilot.info.jobdata import JobData


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_filespec(lfn: str, accessmode: str = '', turl: str = '') -> FileSpec:
    """Return a ``FileSpec`` with the given lfn, accessmode and optional turl.

    Args:
        lfn: Logical file name.
        accessmode: Access mode string ('direct', 'copy', or '').
        turl: Transfer URL (used when ensure_replica=True).

    Returns:
        FileSpec: Configured instance.
    """
    fspec = FileSpec.__new__(FileSpec)
    fspec.lfn = lfn
    fspec.accessmode = accessmode
    fspec.turl = turl
    return fspec


def _accessmode_for(lfn: str, storage_token: str,
                    is_analysis: bool = False, transfertype: str = '',
                    job_accessmode: str = None) -> str:
    """Drive ``JobData.prepare_infiles`` for a single file and return its accessmode.

    Constructs the minimal JobData state and data dict that prepare_infiles
    needs, then returns the accessmode assigned to the single resulting FileSpec.

    ``prepare_infiles`` calls ``set_accessmode()`` internally, which derives
    ``job.accessmode`` from ``job.jobparams``.  To test the override path we
    therefore encode the desired accessmode as a jobparams string rather than
    setting ``job.accessmode`` directly.

    Args:
        lfn: Logical file name for the input file.
        storage_token: prodDBlockToken value for the file.
        is_analysis: Whether the job should be treated as an analysis job.
        transfertype: Job-level transfertype string.
        job_accessmode: Desired job-level accessmode override ('direct', 'copy',
            or None for no override).  Encoded into jobparams so that
            set_accessmode() derives the right value.

    Returns:
        str: The accessmode assigned to the FileSpec by prepare_infiles.
    """
    _jobparams_map = {
        'direct': '--accessmode=direct',
        'copy': '--accessmode=copy',
        None: '',
    }
    job = JobData.__new__(JobData)
    job.transfertype = transfertype
    job.accessmode = None  # will be set by set_accessmode() inside prepare_infiles
    job.infosys = None  # skip queuedata branch
    job.jobparams = _jobparams_map[job_accessmode]

    # is_analysis() is called in prepare_infiles
    job.is_analysis = MagicMock(return_value=is_analysis)

    data = {
        'inFiles': lfn,
        'realDatasetsIn': 'dataset',
        'GUID': 'guid-0001',
        'fsize': '1000',
        'checksum': 'ad:abcd1234',
        'scopeIn': 'scope',
        'prodDBlockToken': storage_token,
        'ddmEndPointIn': 'ddm',
    }

    files = job.prepare_infiles(data)
    assert len(files) == 1, f'expected 1 FileSpec, got {len(files)}'
    return files[0].accessmode


# ---------------------------------------------------------------------------
# Tests for prepare_infiles accessmode assignment via storage_token
# ---------------------------------------------------------------------------

class TestPrepareInfilesStorageToken(unittest.TestCase):
    """Tests for per-file accessmode assignment in ``JobData.prepare_infiles``.

    Validates that ``prodDBlockToken`` (``storage_token``) is the authoritative
    signal controlling copy vs direct access for data files.
    """

    # --- production job, no transfertype, storage_token=local (default RAW behaviour) ---

    def test_prod_raw_local_token_is_copy(self):
        """RAW file with storage_token='local' on a prod job must get accessmode='copy'."""
        mode = _accessmode_for(
            lfn='data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            storage_token='local',
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'copy')

    def test_prod_raw_no_token_is_direct(self):
        """RAW file without 'local' token on a prod job must get accessmode='direct' (--forceDirectIO path)."""
        mode = _accessmode_for(
            lfn='data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            storage_token='',   # JEDI omitted 'local' token due to --forceDirectIO
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'direct')

    def test_prod_raw_real_token_is_direct(self):
        """RAW file with a real DDM token on a prod job must get accessmode='direct'."""
        mode = _accessmode_for(
            lfn='data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            storage_token='CERN-PROD_DATADISK',
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'direct')

    # --- lib tarballs must always be copy regardless of token ---

    def test_prod_lib_tgz_no_token_is_copy(self):
        """Lib tarball without 'local' token must still get accessmode='copy'."""
        mode = _accessmode_for(
            lfn='AtlasProduction.21.0.15.lib.tgz',
            storage_token='',
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'copy')

    def test_prod_lib_tgz_real_token_is_copy(self):
        """Lib tarball with a real DDM token must still get accessmode='copy'."""
        mode = _accessmode_for(
            lfn='AtlasProduction.21.0.15.lib.tgz',
            storage_token='CERN-PROD_DATADISK',
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'copy')

    def test_prod_tar_gz_no_token_is_copy(self):
        """A .tar.gz sandbox without 'local' token must still get accessmode='copy'."""
        mode = _accessmode_for(
            lfn='inputsandbox.tar.gz',
            storage_token='',
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'copy')

    # --- analysis job: always direct when storage_token != local ---

    def test_analy_raw_local_token_is_copy(self):
        """Analysis job RAW file with storage_token='local' must get accessmode='copy'."""
        mode = _accessmode_for(
            lfn='data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            storage_token='local',
            is_analysis=True,
            transfertype='',
        )
        self.assertEqual(mode, 'copy')

    def test_analy_raw_no_token_is_direct(self):
        """Analysis job RAW file without 'local' token must get accessmode='direct'."""
        mode = _accessmode_for(
            lfn='data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            storage_token='',
            is_analysis=True,
            transfertype='',
        )
        self.assertEqual(mode, 'direct')

    # --- job_accessmode (from jobparams --accessmode=) overrides storage_token signal ---

    def test_job_accessmode_copy_overrides_no_token(self):
        """job.accessmode='copy' must win over no-token signal."""
        mode = _accessmode_for(
            lfn='data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            storage_token='',
            is_analysis=False,
            transfertype='',
            job_accessmode='copy',
        )
        self.assertEqual(mode, 'copy')

    def test_job_accessmode_direct_overrides_local_token(self):
        """job.accessmode='direct' must override even a 'local' storage_token."""
        mode = _accessmode_for(
            lfn='data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            storage_token='local',
            is_analysis=False,
            transfertype='',
            job_accessmode='direct',
        )
        self.assertEqual(mode, 'direct')

    # --- regular root file, no token ---

    def test_prod_root_file_no_token_is_direct(self):
        """Regular ROOT file on a prod job without 'local' token must get accessmode='direct'."""
        mode = _accessmode_for(
            lfn='myfile.pool.root.1',
            storage_token='',
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'direct')

    def test_prod_root_file_local_token_is_copy(self):
        """Regular ROOT file on a prod job with storage_token='local' must get accessmode='copy'."""
        mode = _accessmode_for(
            lfn='myfile.pool.root.1',
            storage_token='local',
            is_analysis=False,
            transfertype='',
        )
        self.assertEqual(mode, 'copy')


# ---------------------------------------------------------------------------
# Tests for FileSpec.is_directaccess()
# ---------------------------------------------------------------------------

class TestIsDirectaccess(unittest.TestCase):
    """Tests for ``FileSpec.is_directaccess`` with RAW and lib files."""

    # --- normal root file behaviour (baseline, unaffected by the fix) ---

    def test_root_file_direct_mode_no_replica_check(self):
        """A regular ROOT file with accessmode=direct must return True (no replica check)."""
        fspec = _make_filespec('myfile.pool.root.1', accessmode='direct')
        self.assertTrue(fspec.is_directaccess(ensure_replica=False))

    def test_root_file_copy_mode_no_replica_check(self):
        """A regular ROOT file with accessmode=copy must return False."""
        fspec = _make_filespec('myfile.pool.root.1', accessmode='copy')
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    def test_root_file_no_accessmode_returns_false(self):
        """A regular ROOT file with no accessmode must return False (default copy)."""
        fspec = _make_filespec('myfile.pool.root.1', accessmode='')
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    # --- lib tarball: ALWAYS excluded, even with accessmode=direct ---

    def test_lib_tgz_direct_mode_still_excluded(self):
        """A .lib.tgz file must never be direct-accessed, even with accessmode=direct."""
        fspec = _make_filespec('AtlasProduction.21.0.15.lib.tgz', accessmode='direct')
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    def test_tar_gz_direct_mode_still_excluded(self):
        """A .tar.gz file must never be direct-accessed, even with accessmode=direct."""
        fspec = _make_filespec('inputsandbox.tar.gz', accessmode='direct')
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    def test_lib_tgz_copy_mode_excluded(self):
        """A .lib.tgz file with accessmode=copy must also return False."""
        fspec = _make_filespec('AtlasProduction.21.0.15.lib.tgz', accessmode='copy')
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    # --- RAW files: excluded when copy, allowed when direct ---

    def test_raw_file_copy_mode_excluded(self):
        """RAW file with accessmode=copy must return False."""
        fspec = _make_filespec(
            'data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            accessmode='copy',
        )
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    def test_raw_file_no_accessmode_excluded(self):
        """RAW file with no accessmode must return False."""
        fspec = _make_filespec(
            'data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            accessmode='',
        )
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    def test_raw_file_direct_mode_allowed(self):
        """RAW file with accessmode=direct (storage_token was not 'local') must return True."""
        fspec = _make_filespec(
            'data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            accessmode='direct',
        )
        self.assertTrue(fspec.is_directaccess(ensure_replica=False))

    def test_raw_dot_prefix_direct_mode_allowed(self):
        """Files starting with 'raw.' with accessmode=direct must return True."""
        fspec = _make_filespec('raw.00123456.physics_Main.daq._lb0001.data', accessmode='direct')
        self.assertTrue(fspec.is_directaccess(ensure_replica=False))

    def test_raw_dot_prefix_no_accessmode_excluded(self):
        """Files starting with 'raw.' without accessmode=direct must return False."""
        fspec = _make_filespec('raw.00123456.physics_Main.daq._lb0001.data', accessmode='')
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    # --- ensure_replica gate still applies for RAW + direct ---

    def test_raw_direct_no_turl_excluded_by_replica_check(self):
        """RAW + accessmode=direct but no turl must return False (replica gate)."""
        fspec = _make_filespec(
            'data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            accessmode='direct', turl='',
        )
        self.assertFalse(fspec.is_directaccess(ensure_replica=True))

    def test_raw_direct_root_turl_passes_replica_check(self):
        """RAW + accessmode=direct + valid root:// turl must return True."""
        fspec = _make_filespec(
            'data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            accessmode='direct',
            turl='root://atlas-xrootd.example.com//path/file.data',
        )
        self.assertTrue(fspec.is_directaccess(ensure_replica=True))

    def test_raw_direct_unknown_schema_turl_excluded_by_replica_check(self):
        """RAW + accessmode=direct but unknown schema turl must be blocked by replica check."""
        fspec = _make_filespec(
            'data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data',
            accessmode='direct',
            turl='gsiftp://gridftp.example.com//path/file.data',
        )
        self.assertFalse(fspec.is_directaccess(ensure_replica=True))

    # --- case insensitivity ---

    def test_raw_uppercase_in_lfn_no_accessmode_excluded(self):
        """Upper-case RAW pattern in LFN without accessmode=direct must be excluded."""
        fspec = _make_filespec(
            'data22_13p6TeV.00440543.physics_Main.merge.RAW._lb1234._0001.data',
            accessmode='',
        )
        self.assertFalse(fspec.is_directaccess(ensure_replica=False))

    def test_raw_uppercase_in_lfn_direct_mode_allowed(self):
        """Upper-case RAW pattern in LFN with accessmode=direct must be allowed."""
        fspec = _make_filespec(
            'data22_13p6TeV.00440543.physics_Main.merge.RAW._lb1234._0001.data',
            accessmode='direct',
        )
        self.assertTrue(fspec.is_directaccess(ensure_replica=False))


if __name__ == '__main__':
    unittest.main()
