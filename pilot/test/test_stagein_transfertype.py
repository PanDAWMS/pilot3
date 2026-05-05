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

"""Unit tests for transfertype-aware direct-I/O protocol selection.

Covers:
- ``is_directio_transfertype``: classification of transfertype values.
- ``get_directio_preferred_schemas``: protocol ordering for direct I/O.
- ``StageInClient.get_direct_access_variables``: production-job gate.
"""

import logging
import unittest
from unittest.mock import MagicMock

from pilot.api.data import (
    StageInClient,
    get_directio_preferred_schemas,
    is_directio_transfertype,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_job(is_analysis: bool = False, transfertype: str = '') -> MagicMock:
    """Return a minimal mock job object.

    Args:
        is_analysis: Whether the job should report itself as an analysis job.
        transfertype: Value for ``job.transfertype``.

    Returns:
        MagicMock: Configured mock job.
    """
    job = MagicMock()
    job.is_analysis.return_value = is_analysis
    job.transfertype = transfertype
    return job


def _make_client(direct_access_lan: bool = True, direct_access_wan: bool = False) -> StageInClient:
    """Return a ``StageInClient`` wired to a minimal mock infosys.

    Args:
        direct_access_lan: Value for ``queuedata.direct_access_lan``.
        direct_access_wan: Value for ``queuedata.direct_access_wan``.

    Returns:
        StageInClient: Configured client instance.
    """
    queuedata = MagicMock()
    queuedata.direct_access_lan = direct_access_lan
    queuedata.direct_access_wan = direct_access_wan

    infosys = MagicMock()
    infosys.queuedata = queuedata

    logger = logging.getLogger('test.null')
    logger.disabled = True

    client = StageInClient.__new__(StageInClient)
    client.logger = logger
    client.infosys = infosys
    return client


# ---------------------------------------------------------------------------
# Tests for is_directio_transfertype()
# ---------------------------------------------------------------------------

class TestIsDirectioTransfertype(unittest.TestCase):
    """Tests for ``is_directio_transfertype``."""

    def test_empty_string_returns_false(self):
        """Empty transfertype must not be treated as direct I/O."""
        self.assertFalse(is_directio_transfertype(''))

    def test_none_returns_false(self):
        """None transfertype must not be treated as direct I/O."""
        self.assertFalse(is_directio_transfertype(None))

    def test_direct_returns_true(self):
        """'direct' is the canonical direct-I/O keyword."""
        self.assertTrue(is_directio_transfertype('direct'))

    def test_root_returns_true(self):
        """'root' requests direct I/O via root:// protocol."""
        self.assertTrue(is_directio_transfertype('root'))

    def test_davs_returns_true(self):
        """'davs' requests direct I/O via davs:// protocol."""
        self.assertTrue(is_directio_transfertype('davs'))

    def test_file_returns_false(self):
        """'file' means Rucio copy via POSIX link, not direct I/O."""
        self.assertFalse(is_directio_transfertype('file'))

    def test_null_string_returns_false(self):
        """'Null' (PanDA sentinel) must not enable direct I/O."""
        self.assertFalse(is_directio_transfertype('Null'))

    def test_davs_root_comma_list_returns_true(self):
        """Comma-separated list of valid directio types must return True."""
        self.assertTrue(is_directio_transfertype('davs,root'))

    def test_root_davs_comma_list_returns_true(self):
        """Order of comma-separated valid types should not matter."""
        self.assertTrue(is_directio_transfertype('root,davs'))

    def test_direct_root_comma_list_returns_true(self):
        """'direct,root' is a valid directio combination."""
        self.assertTrue(is_directio_transfertype('direct,root'))

    def test_comma_list_containing_file_returns_false(self):
        """Any list containing 'file' must return False — 'file' is copy, not I/O."""
        self.assertFalse(is_directio_transfertype('file,root'))

    def test_comma_list_containing_unknown_returns_false(self):
        """Unknown tokens in a list must cause the check to return False."""
        self.assertFalse(is_directio_transfertype('root,xrootd'))

    def test_uppercase_input_is_normalised(self):
        """Input is normalised to lower-case before comparison."""
        self.assertTrue(is_directio_transfertype('DAVS'))
        self.assertTrue(is_directio_transfertype('Root'))
        self.assertFalse(is_directio_transfertype('FILE'))

    def test_whitespace_around_tokens_is_tolerated(self):
        """Spaces around comma-separated tokens must be stripped."""
        self.assertTrue(is_directio_transfertype(' davs , root '))


# ---------------------------------------------------------------------------
# Tests for get_directio_preferred_schemas()
# ---------------------------------------------------------------------------

class TestGetDirectioPreferredSchemas(unittest.TestCase):
    """Tests for ``get_directio_preferred_schemas``."""

    # Default schema lists mirroring the class-level constants in data.py
    _LOCAL_SCHEMAS = ['root', 'davs', 'dcache', 'dcap', 'file', 'https']
    _REMOTE_SCHEMAS = ['root', 'davs', 'https']

    def test_empty_transfertype_returns_default(self):
        """Empty transfertype must leave the schema list unchanged."""
        result = get_directio_preferred_schemas('', self._LOCAL_SCHEMAS)
        self.assertEqual(result, self._LOCAL_SCHEMAS)

    def test_none_transfertype_returns_default(self):
        """None transfertype must leave the schema list unchanged."""
        result = get_directio_preferred_schemas(None, self._LOCAL_SCHEMAS)
        self.assertEqual(result, self._LOCAL_SCHEMAS)

    def test_direct_returns_default_unchanged(self):
        """'direct' is the legacy default-order keyword; list must be unchanged."""
        result = get_directio_preferred_schemas('direct', self._LOCAL_SCHEMAS)
        self.assertEqual(result, self._LOCAL_SCHEMAS)

    def test_root_keeps_root_first_in_local(self):
        """'root' with a list that already starts with root must be unchanged."""
        result = get_directio_preferred_schemas('root', self._LOCAL_SCHEMAS)
        self.assertEqual(result[0], 'root')
        self.assertEqual(result, self._LOCAL_SCHEMAS)

    def test_davs_moves_davs_to_front_of_remote(self):
        """'davs' must place davs:// first, followed by the original entries."""
        result = get_directio_preferred_schemas('davs', self._REMOTE_SCHEMAS)
        self.assertEqual(result[0], 'davs')
        # Original entries not in the preferred list follow in order
        self.assertIn('root', result)
        self.assertIn('https', result)

    def test_davs_moves_davs_to_front_of_local(self):
        """'davs' on the local schema list must place davs first."""
        result = get_directio_preferred_schemas('davs', self._LOCAL_SCHEMAS)
        self.assertEqual(result[0], 'davs')
        for schema in self._LOCAL_SCHEMAS:
            self.assertIn(schema, result)

    def test_davs_root_orders_davs_then_root(self):
        """'davs,root' must produce [davs, root, ...remaining...]."""
        result = get_directio_preferred_schemas('davs,root', self._REMOTE_SCHEMAS)
        self.assertEqual(result[0], 'davs')
        self.assertEqual(result[1], 'root')

    def test_root_davs_orders_root_then_davs(self):
        """'root,davs' must produce [root, davs, ...remaining...]."""
        result = get_directio_preferred_schemas('root,davs', self._REMOTE_SCHEMAS)
        self.assertEqual(result[0], 'root')
        self.assertEqual(result[1], 'davs')

    def test_no_duplicates_in_result(self):
        """Schema names must not appear more than once in the result."""
        result = get_directio_preferred_schemas('davs,root', self._LOCAL_SCHEMAS)
        self.assertEqual(len(result), len(set(result)))

    def test_all_original_schemas_present_in_result(self):
        """Every schema from the default list must appear in the result."""
        result = get_directio_preferred_schemas('davs', self._LOCAL_SCHEMAS)
        for schema in self._LOCAL_SCHEMAS:
            self.assertIn(schema, result)

    def test_file_transfertype_returns_default_unchanged(self):
        """'file' is not a directio type; default list must be returned as-is."""
        result = get_directio_preferred_schemas('file', self._LOCAL_SCHEMAS)
        self.assertEqual(result, self._LOCAL_SCHEMAS)

    def test_unknown_transfertype_returns_default_unchanged(self):
        """Unrecognised transfertype must not alter the schema list."""
        result = get_directio_preferred_schemas('xrootd', self._LOCAL_SCHEMAS)
        self.assertEqual(result, self._LOCAL_SCHEMAS)

    def test_preferred_schema_not_in_default_list_is_prepended(self):
        """A valid directio keyword not already in the default list is prepended."""
        # Use a minimal custom list that does not contain 'davs' to verify
        # that the schema is still prepended when absent from the default.
        result = get_directio_preferred_schemas('davs', ['root', 'https'])
        self.assertEqual(result[0], 'davs')
        self.assertIn('root', result)
        self.assertIn('https', result)

    def test_davs_already_in_local_schemas_no_duplicate(self):
        """'davs' in default list must appear once and remain first after reorder."""
        result = get_directio_preferred_schemas('davs', self._LOCAL_SCHEMAS)
        self.assertEqual(result[0], 'davs')
        self.assertEqual(result.count('davs'), 1)


# ---------------------------------------------------------------------------
# Tests for StageInClient.get_direct_access_variables()
# ---------------------------------------------------------------------------

class TestGetDirectAccessVariables(unittest.TestCase):
    """Tests for ``StageInClient.get_direct_access_variables``."""

    def test_analysis_job_always_allows_direct_access(self):
        """Analysis jobs must always be allowed direct access regardless of transfertype."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=True, transfertype='')
        allow, dtype = client.get_direct_access_variables(job)
        self.assertTrue(allow)
        self.assertEqual(dtype, 'LAN')

    def test_production_job_direct_allows_direct_access(self):
        """Production job with transfertype='direct' must enable direct access."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=False, transfertype='direct')
        allow, _ = client.get_direct_access_variables(job)
        self.assertTrue(allow)

    def test_production_job_root_allows_direct_access(self):
        """Production job with transfertype='root' must enable direct access."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=False, transfertype='root')
        allow, _ = client.get_direct_access_variables(job)
        self.assertTrue(allow)

    def test_production_job_davs_allows_direct_access(self):
        """Production job with transfertype='davs' must enable direct access."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=False, transfertype='davs')
        allow, _ = client.get_direct_access_variables(job)
        self.assertTrue(allow)

    def test_production_job_davs_root_allows_direct_access(self):
        """Production job with transfertype='davs,root' must enable direct access."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=False, transfertype='davs,root')
        allow, _ = client.get_direct_access_variables(job)
        self.assertTrue(allow)

    def test_production_job_empty_transfertype_disables_direct_access(self):
        """Production job with no transfertype must have direct access disabled."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=False, transfertype='')
        allow, _ = client.get_direct_access_variables(job)
        self.assertFalse(allow)

    def test_production_job_file_transfertype_disables_direct_access(self):
        """Production job with transfertype='file' (POSIX copy) must not get direct access."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=False, transfertype='file')
        allow, _ = client.get_direct_access_variables(job)
        self.assertFalse(allow)

    def test_production_job_null_transfertype_disables_direct_access(self):
        """Production job with transfertype='Null' must not get direct access."""
        client = _make_client(direct_access_lan=True)
        job = _make_job(is_analysis=False, transfertype='Null')
        allow, _ = client.get_direct_access_variables(job)
        self.assertFalse(allow)

    def test_lan_type_reported_correctly(self):
        """direct_access_type must be 'LAN' when direct_access_lan is set."""
        client = _make_client(direct_access_lan=True, direct_access_wan=False)
        job = _make_job(is_analysis=True)
        _, dtype = client.get_direct_access_variables(job)
        self.assertEqual(dtype, 'LAN')

    def test_wan_type_reported_correctly(self):
        """direct_access_type must be 'WAN' when only direct_access_wan is set."""
        client = _make_client(direct_access_lan=False, direct_access_wan=True)
        job = _make_job(is_analysis=True)
        _, dtype = client.get_direct_access_variables(job)
        self.assertEqual(dtype, 'WAN')

    def test_no_queuedata_disables_direct_access(self):
        """Uninitialised queuedata must result in direct access being disabled."""
        infosys = MagicMock()
        infosys.queuedata = None
        logger = logging.getLogger('test.null')
        logger.disabled = True
        client = StageInClient.__new__(StageInClient)
        client.logger = logger
        client.infosys = infosys

        job = _make_job(is_analysis=True)
        allow, dtype = client.get_direct_access_variables(job)
        self.assertFalse(allow)
        self.assertEqual(dtype, '')

    def test_none_job_does_not_raise(self):
        """Passing job=None must not raise any exception."""
        client = _make_client(direct_access_lan=True)
        try:
            allow, dtype = client.get_direct_access_variables(None)
        except Exception as exc:  # pylint: disable=broad-except
            self.fail(f'get_direct_access_variables(None) raised {exc!r}')
        self.assertTrue(allow)


# ---------------------------------------------------------------------------
# Tests for FileSpec.is_directaccess() with davs:// turl
# ---------------------------------------------------------------------------

class TestIsDirectaccessDavsTurl(unittest.TestCase):
    """Tests that is_directaccess() accepts davs:// turls for direct I/O."""

    def _make_fspec(self, turl: str, accessmode: str = 'direct') -> object:
        """Return a minimal FileSpec-like object.

        Args:
            turl: The transport URL to assign to the file.
            accessmode: The access mode ('direct' or 'copy').

        Returns:
            MagicMock: Configured mock FileSpec.
        """
        from pilot.info.filespec import FileSpec
        fspec = FileSpec.__new__(FileSpec)
        fspec.lfn = 'data18_13TeV.00359541.physics_Main.daq.RAW._lb0192._SFO-6._0004.data'
        fspec.turl = turl
        fspec.accessmode = accessmode
        return fspec

    def test_davs_turl_accepted_by_local_schemas(self):
        """davs:// turl must pass is_directaccess when davs is in local schemas."""
        from pilot.api.data import StagingClient
        fspec = self._make_fspec('davs://dcache-atlas-webdav-job.desy.de:2880/path/to/file.data')
        schemas = StagingClient.direct_localinput_allowed_schemas
        self.assertIn('davs', schemas,
                      "direct_localinput_allowed_schemas must include 'davs'")
        self.assertTrue(
            fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=schemas),
            "is_directaccess must return True for a davs:// turl"
        )

    def test_davs_turl_accepted_by_remote_schemas(self):
        """davs:// turl must pass is_directaccess when davs is in remote schemas."""
        from pilot.api.data import StagingClient
        fspec = self._make_fspec('davs://dcache-atlas-webdav.desy.de:2880/path/to/file.data')
        schemas = StagingClient.direct_remoteinput_allowed_schemas
        self.assertIn('davs', schemas,
                      "direct_remoteinput_allowed_schemas must include 'davs'")
        self.assertTrue(
            fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=schemas),
            "is_directaccess must return True for a davs:// turl on remote schemas"
        )

    def test_root_turl_still_accepted_by_local_schemas(self):
        """root:// turl must continue to pass is_directaccess (no regression)."""
        from pilot.api.data import StagingClient
        fspec = self._make_fspec('root://dcache-atlas-xrootd-job.desy.de:1094//path/file.data')
        schemas = StagingClient.direct_localinput_allowed_schemas
        self.assertTrue(
            fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=schemas),
            "root:// turl must remain accepted after adding davs to the schema list"
        )

    def test_davs_turl_rejected_when_accessmode_copy(self):
        """davs:// turl must be rejected when accessmode is 'copy'."""
        from pilot.api.data import StagingClient
        fspec = self._make_fspec(
            'davs://dcache-atlas-webdav-job.desy.de:2880/path/to/file.data',
            accessmode='copy'
        )
        schemas = StagingClient.direct_localinput_allowed_schemas
        self.assertFalse(
            fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=schemas),
            "is_directaccess must be False when accessmode='copy' regardless of turl schema"
        )


if __name__ == '__main__':
    unittest.main()
