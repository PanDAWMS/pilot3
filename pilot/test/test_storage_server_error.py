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

"""Regression tests for STORAGESERVERERROR (1395) and TimedThread exception propagation.

Incident: EIC canary job on BNL OSG, 2026-11-02 (condor grid.45883.0), log file
group.EIC.canary.bnl_osg_panda_pilotest.20261102012012.000001.log.tgz. The destination
XRootD endpoint answered "[3012] Internal timeout (destination)" (kXR_ServerError).

Two stacked defects turned this storage-side failure into a pilot-side looking one:

1. ``TimedThread.execute()`` only caught TypeError/ValueError/AttributeError/KeyError. The
   PilotException raised by ``_stage_out_api()`` escaped the worker thread ("Exception in
   thread Thread-3 (execute)" in the batch stderr), was lost, and ``run()`` raised a
   ``TimeoutException("Unknown time-out related error ...")`` instead.
2. ``resolve_common_transfer_errors()`` matched the word "timeout" before any XRootD server
   error, so the server-side "[3012] Internal timeout" became STAGEOUTTIMEOUT (1152).
"""

import os
import shutil
import tempfile
import threading
import traceback
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.copytool import rucio
from pilot.copytool.common import resolve_common_transfer_errors
from pilot.info.filespec import FileSpec
from pilot.util.auxiliary import TimeoutException
from pilot.util.timer import TimedThread, timeout

errors = ErrorCodes()

# trace stateReason of the incident: str(RSEOperationNotSupported) as set by rucio 35.6.0 UploadClient.upload()
INCIDENT_STATE_REASON = (
    "RSE does not support requested operation.\n"
    "Details: The requested service is not available at the moment.\n"
    "Details: An unknown exception occurred.\n"
    "Details: Error on XrdCl::CopyProcess::Run(): [ERROR] Server responded with an error: [3012] Internal timeout (destination)"
)
INCIDENT_XRD_LINE = "Error on XrdCl::CopyProcess::Run(): [ERROR] Server responded with an error: [3012] Internal timeout (destination)"


class RucioLikeError(Exception):
    """Stand-in for a rucio exception (not one of the four types TimedThread used to catch)."""


def _return_value(value):
    """Return the given value."""
    return value


def _raise_pilot_exception():
    """Raise a PilotException with an explicit error code."""
    raise PilotException("storage said no", code=errors.STAGEOUTFAILED)


def _exit_thread():
    """End the worker thread without storing a result."""
    raise SystemExit(1)


def _raise_rucio_like():
    """Raise an exception of a type that TimedThread did not catch before the fix."""
    raise RucioLikeError("Cannot download file: no replicas available")


class TestTimedThreadPropagation(unittest.TestCase):
    """Exceptions raised inside a TimedThread must reach the caller unchanged."""

    def setUp(self):
        """Record uncaught thread exceptions instead of printing them."""
        self.thread_errors = []
        self._orig_hook = threading.excepthook
        threading.excepthook = self.thread_errors.append

    def tearDown(self):
        """Restore the thread exception hook."""
        threading.excepthook = self._orig_hook

    def test_return_value_is_passed_through(self):
        """A normal return value is still returned."""
        self.assertEqual(timeout(5, timer=TimedThread)(_return_value)(42), 42)

    def test_pilot_exception_propagates_with_code(self):
        """A PilotException keeps its type and error code (incident: became TimeoutException)."""
        with self.assertRaises(PilotException) as ctx:
            timeout(5, timer=TimedThread)(_raise_pilot_exception)()
        self.assertEqual(ctx.exception.get_error_code(), errors.STAGEOUTFAILED)
        self.assertNotIsInstance(ctx.exception, TimeoutException)

    def test_other_exception_types_propagate(self):
        """An exception type outside the old four is re-raised, not turned into a time-out."""
        with self.assertRaises(RucioLikeError) as ctx:
            timeout(5, timer=TimedThread)(_raise_rucio_like)()
        self.assertIn("no replicas available", str(ctx.exception))

    def test_traceback_points_at_the_raising_function(self):
        """The re-raised exception carries the traceback from inside the thread."""
        try:
            timeout(5, timer=TimedThread)(_raise_rucio_like)()
        except RucioLikeError as exc:
            frames = [frame.name for frame in traceback.extract_tb(exc.__traceback__)]
            self.assertIn('_raise_rucio_like', frames)
        else:
            self.fail('RucioLikeError was not raised')

    def test_thread_ending_without_result_is_reported(self):
        """A thread that ends without a result (SystemExit is not caught) still raises TimeoutException."""
        with self.assertRaises(TimeoutException):
            timeout(5, timer=TimedThread)(_exit_thread)()

    def test_no_uncaught_exception_in_worker_thread(self):
        """The worker thread must not die with an uncaught exception (the stderr dump in the incident)."""
        with self.assertRaises(PilotException):
            timeout(5, timer=TimedThread)(_raise_pilot_exception)()
        self.assertEqual(self.thread_errors, [])


class TestStorageServerErrorResolution(unittest.TestCase):
    """XRootD [3012] on stage-out must resolve to STORAGESERVERERROR, not STAGEOUTTIMEOUT."""

    def test_incident_state_reason_is_storage_server_error(self):
        """The exact trace stateReason of the incident resolves to 1395."""
        ret = resolve_common_transfer_errors(INCIDENT_STATE_REASON, is_stagein=False)
        self.assertEqual(ret['rcode'], errors.STORAGESERVERERROR)
        self.assertEqual(ret['state'], 'STORAGE_SERVER_ERROR')

    def test_incident_diagnostics_keep_the_xrootd_line(self):
        """The diagnostics are reduced to the innermost (XRootD) Details line."""
        ret = resolve_common_transfer_errors(INCIDENT_STATE_REASON, is_stagein=False)
        self.assertEqual(ret['error'], INCIDENT_XRD_LINE)

    def test_bare_xrdcl_line_is_storage_server_error(self):
        """The XrdCl line on its own is enough."""
        ret = resolve_common_transfer_errors(INCIDENT_XRD_LINE, is_stagein=False)
        self.assertEqual(ret['rcode'], errors.STORAGESERVERERROR)

    def test_client_timeout_is_still_stageout_timeout(self):
        """A genuine copy time-out (no [3012]) still gives STAGEOUTTIMEOUT."""
        ret = resolve_common_transfer_errors("copy command failed: timeout after 600 s", is_stagein=False)
        self.assertEqual(ret['rcode'], errors.STAGEOUTTIMEOUT)

    def test_other_xrootd_server_errors_unchanged(self):
        """Other XRootD server errors keep their previous classification."""
        output = "Run: [ERROR] Server responded with an error: [3011] No such file or directory"
        ret = resolve_common_transfer_errors(output, is_stagein=False)
        self.assertNotEqual(ret['rcode'], errors.STORAGESERVERERROR)

    def test_stagein_scope_unchanged(self):
        """Stage-in is out of scope: [3012] at the source keeps the existing (time-out) classification."""
        ret = resolve_common_transfer_errors(INCIDENT_XRD_LINE.replace('destination', 'source'), is_stagein=True)
        self.assertEqual(ret['rcode'], errors.STAGEINTIMEOUT)


class TestStorageServerErrorCode(unittest.TestCase):
    """The new error code is defined, described and recoverable for user jobs."""

    def setUp(self):
        """Snapshot the class-level code lists."""
        self._put = list(ErrorCodes.put_error_codes)
        self._recoverable = list(ErrorCodes.recoverable_error_codes)

    def tearDown(self):
        """Restore the class-level code lists."""
        ErrorCodes.put_error_codes = self._put
        ErrorCodes.recoverable_error_codes = self._recoverable

    def test_code_value_and_message(self):
        """STORAGESERVERERROR is 1395 with its own message."""
        self.assertEqual(errors.STORAGESERVERERROR, 1395)
        self.assertEqual(errors.get_error_message(errors.STORAGESERVERERROR),
                         "Storage endpoint reported an internal server error during stage-out")

    def test_code_is_recoverable_like_stageout_timeout(self):
        """Reclassifying away from 1152 must not make failed user jobs unrecoverable."""
        self.assertTrue(errors.is_recoverable(code=errors.STAGEOUTTIMEOUT))
        self.assertTrue(errors.is_recoverable(code=errors.STORAGESERVERERROR))
        self.assertIn(errors.STORAGESERVERERROR, ErrorCodes.put_error_codes)


class TestRucioCopytoolEndToEnd(unittest.TestCase):
    """The rucio copytool reports the incident as 1395 and other rucio errors without fake time-outs."""

    def setUp(self):
        """Create a work directory and a file spec."""
        self.workdir = tempfile.mkdtemp()
        self.fspec = FileSpec(filetype='output')
        self.fspec.lfn = 'group.EIC.canary.bnl_osg_panda_pilotest.20261102012012.000001.log.tgz'
        self.fspec.scope = 'group.EIC'
        self.fspec.ddmendpoint = 'BNL_EIC_TEST'
        self.fspec.guid = '0f7c-11ee'
        self.fspec.filesize = 1000
        self.fspec.workdir = self.workdir
        self.trace_report = MagicMock()
        self.trace_report.get_value.return_value = ''
        self.trace_report.get.return_value = ''
        self._env = os.environ.get('RUCIO_LOGGING_FORMAT')

    def tearDown(self):
        """Remove the work directory and restore the environment."""
        shutil.rmtree(self.workdir, ignore_errors=True)
        if self._env is None:
            os.environ.pop('RUCIO_LOGGING_FORMAT', None)
        else:
            os.environ['RUCIO_LOGGING_FORMAT'] = self._env

    @staticmethod
    def _incident_upload(*args, **_kwargs):
        """Behave like _stage_out_api() in the incident: rucio trace filled in, then PilotException.

        Same arguments as _stage_out_api(); the fourth positional one is the trace_report_out list.
        """
        trace_report_out = args[3]
        trace_report_out.append({'clientState': 'FAILED', 'stateReason': INCIDENT_STATE_REASON, 'protocol': 'root'})
        raise PilotException("Rucio upload failed: None of the given files have been uploaded.")

    @staticmethod
    def _download_without_trace(*args, **kwargs):
        """Behave like _stage_in_api() when rucio raises before filling in a trace stateReason."""
        raise RucioLikeError("Cannot download file: no replicas available")

    def test_copy_out_incident_reports_storage_server_error(self):
        """The incident ends as STORAGESERVERERROR (was STAGEOUTTIMEOUT)."""
        with patch.object(rucio, '_stage_out_api', self._incident_upload):
            with self.assertRaises(PilotException) as ctx:
                rucio.copy_out([self.fspec], trace_report=self.trace_report, workdir=self.workdir)
        self.assertEqual(ctx.exception.get_error_code(), errors.STORAGESERVERERROR)
        self.assertEqual(self.fspec.status_code, errors.STORAGESERVERERROR)
        self.assertIn('[3012] Internal timeout (destination)', str(ctx.exception))

    def test_copy_in_rucio_error_is_not_a_timeout(self):
        """A rucio error without trace info is no longer reported as STAGEINTIMEOUT."""
        with patch.object(rucio, '_stage_in_api', self._download_without_trace):
            with self.assertRaises(PilotException) as ctx:
                rucio.copy_in([self.fspec], trace_report=self.trace_report, workdir=self.workdir, args=MagicMock(graceful_stop=None))
        self.assertEqual(ctx.exception.get_error_code(), errors.STAGEINFAILED)
        self.assertIn('no replicas available', str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
