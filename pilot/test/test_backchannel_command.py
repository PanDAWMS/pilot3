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

"""Unit tests for extract_backchannel_data() and handle_backchannel_command().

Regression tests for a bug where the pilot silently ignored the server's
'tobekilled' backchannel command (and, by the same root cause, 'debug',
'softkill', 'nocleanup' and 'pilotSecrets') whenever the server responded
via the api/v1/pilot/update_job endpoint.

That endpoint nests backchannel fields under response['data'], e.g.:
    {'success': True, 'message': '', 'data': {'StatusCode': 0, 'command': 'tobekilled'}}
while handle_backchannel_command() looked for 'command' at the top level of
the response, which only the legacy (pre-api/v1) response shape provided.
As a result a job that the server wanted killed just kept running to its
own natural completion (observed live in an ATLAS Rubin job log: the same
'tobekilled' instruction was sent twice, 30 minutes apart, and never acted
upon).

extract_backchannel_data() normalizes both response shapes into a single
flat dict before handle_backchannel_command() looks at it.
"""

import logging
import os
import sys
import threading
import unittest
from unittest.mock import patch

from pilot.control import job as job_module
from pilot.common.errorcodes import ErrorCodes

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

errors = ErrorCodes()


class FakeJob:
    """Minimal job-like object sufficient for handle_backchannel_command()."""

    def __init__(self, workdir: str):
        """Initialize a fake job with a real, existing workdir.

        Args:
            workdir: path to an existing directory to stand in for job.workdir.
        """
        self.jobid = '90460452'
        self.workdir = workdir
        self.pid = None
        self.state = 'running'
        self.piloterrorcodes = []
        self.piloterrordiags = []
        self.pilotsecrets = None
        self.debug = False
        self.debug_command = ''


class FakeArgs:
    """Minimal args-like object sufficient for handle_backchannel_command()."""

    def __init__(self, workflow: str = 'generic'):
        """Initialize a fake args object.

        Args:
            workflow: pilot workflow name (e.g. 'generic' or 'stager').
        """
        self.workflow = workflow
        self.abort_job = threading.Event()
        self.cleanup = True


# The exact server response observed in the ATLAS Rubin pilot log for job
# 90460452 (SLAC_Rubin_4G), sent twice (00:25:52 and 00:55:52) and never acted upon.
REAL_API_V1_TOBEKILLED_RESPONSE = {
    'success': True,
    'message': '',
    'data': {'StatusCode': 0, 'command': 'tobekilled'},
}


class TestExtractBackchannelData(unittest.TestCase):
    """Tests for job.extract_backchannel_data()."""

    def test_nested_api_v1_response_is_flattened(self):
        """Test that fields nested under 'data' are hoisted to the top level."""
        flat = job_module.extract_backchannel_data(REAL_API_V1_TOBEKILLED_RESPONSE)
        self.assertEqual(flat.get('command'), 'tobekilled')
        self.assertEqual(flat.get('StatusCode'), 0)

    def test_legacy_flat_response_is_unchanged(self):
        """Test that an already-flat (legacy) response still works."""
        legacy = {'StatusCode': 0, 'command': 'tobekilled'}
        flat = job_module.extract_backchannel_data(legacy)
        self.assertEqual(flat.get('command'), 'tobekilled')

    def test_nested_data_wins_over_top_level_on_collision(self):
        """Test that nested 'data' fields take precedence over stale top-level fields."""
        res = {'command': 'debug', 'data': {'command': 'tobekilled'}}
        flat = job_module.extract_backchannel_data(res)
        self.assertEqual(flat.get('command'), 'tobekilled')

    def test_malformed_non_dict_data_does_not_crash(self):
        """Test that a non-dict 'data' value is ignored rather than raising."""
        res = {'success': True, 'data': 'oops'}
        flat = job_module.extract_backchannel_data(res)
        self.assertNotIn('command', flat)

    def test_missing_data_key(self):
        """Test a response with no 'data' key at all (e.g. plain heartbeat ack)."""
        res = {'success': True, 'message': ''}
        flat = job_module.extract_backchannel_data(res)
        self.assertNotIn('command', flat)

    def test_non_dict_input_returns_empty_dict(self):
        """Test that non-dict input is handled gracefully."""
        self.assertEqual(job_module.extract_backchannel_data(None), {})
        self.assertEqual(job_module.extract_backchannel_data('oops'), {})

    def test_nested_pilot_secrets(self):
        """Test that pilotSecrets nested under 'data' is also hoisted."""
        res = {'success': True, 'data': {'pilotSecrets': {'key': 'value'}}}
        flat = job_module.extract_backchannel_data(res)
        self.assertEqual(flat.get('pilotSecrets'), {'key': 'value'})


class TestHandleBackchannelCommandTobekilled(unittest.TestCase):
    """Regression tests for the 'tobekilled' backchannel command."""

    def setUp(self):
        """Create a fresh, existing workdir for each test."""
        self.workdir = os.getcwd()

    def test_tobekilled_ignored_without_normalization(self):
        """Test the pre-fix behaviour: raw api/v1 response is silently ignored.

        This documents the original bug: passing the enveloped response directly
        (as the buggy code used to do) means the top-level 'command' in check
        never matches, so no kill action is taken.
        """
        job = FakeJob(self.workdir)
        args = FakeArgs()

        job_module.handle_backchannel_command(REAL_API_V1_TOBEKILLED_RESPONSE, job, args)

        self.assertFalse(args.abort_job.is_set())
        self.assertEqual(job.state, 'running')
        self.assertEqual(job.piloterrorcodes, [])

    def test_tobekilled_acted_on_after_normalization(self):
        """Test the fix: normalized api/v1 response triggers the kill path."""
        job = FakeJob(self.workdir)
        args = FakeArgs()

        flat = job_module.extract_backchannel_data(REAL_API_V1_TOBEKILLED_RESPONSE)
        job_module.handle_backchannel_command(flat, job, args)

        self.assertTrue(args.abort_job.is_set())
        self.assertEqual(job.state, 'failed')
        self.assertIn(errors.PANDAKILL, job.piloterrorcodes)

    def test_tobekilled_legacy_flat_response_still_works(self):
        """Test backward compatibility with the legacy (pre-api/v1) flat response shape."""
        job = FakeJob(self.workdir)
        args = FakeArgs()

        legacy = {'StatusCode': 0, 'command': 'tobekilled'}
        job_module.handle_backchannel_command(legacy, job, args)

        self.assertTrue(args.abort_job.is_set())
        self.assertEqual(job.state, 'failed')
        self.assertIn(errors.PANDAKILL, job.piloterrorcodes)

    def test_tobekilled_stager_workflow_sets_finished_not_failed(self):
        """Test that a stager workflow job is set to 'finished' rather than 'failed'."""
        job = FakeJob(self.workdir)
        args = FakeArgs(workflow='stager')

        flat = job_module.extract_backchannel_data(REAL_API_V1_TOBEKILLED_RESPONSE)
        job_module.handle_backchannel_command(flat, job, args)

        self.assertTrue(args.abort_job.is_set())
        self.assertEqual(job.state, 'finished')

    def test_null_command_is_ignored(self):
        """Test that a 'NULL' command string (server's no-op sentinel) is not acted on."""
        job = FakeJob(self.workdir)
        args = FakeArgs()

        res = {'success': True, 'data': {'StatusCode': 0, 'command': 'NULL'}}
        flat = job_module.extract_backchannel_data(res)
        job_module.handle_backchannel_command(flat, job, args)

        self.assertFalse(args.abort_job.is_set())
        self.assertEqual(job.state, 'running')

    def test_tobekilled_missing_workdir_is_ignored_safely(self):
        """Test that a kill instruction is ignored (not crashed on) if job.workdir does not yet exist."""
        job = FakeJob('/no/such/workdir/should/exist/hopefully')
        args = FakeArgs()

        flat = job_module.extract_backchannel_data(REAL_API_V1_TOBEKILLED_RESPONSE)
        job_module.handle_backchannel_command(flat, job, args)

        self.assertFalse(args.abort_job.is_set())
        self.assertEqual(job.state, 'running')

    def test_debug_command_still_works_after_normalization(self):
        """Test that the (also previously broken) 'debug' command path is unaffected by the fix."""
        job = FakeJob(self.workdir)
        args = FakeArgs()

        res = {'success': True, 'data': {'StatusCode': 0, 'command': 'tail pilotlog.txt'}}
        flat = job_module.extract_backchannel_data(res)
        job_module.handle_backchannel_command(flat, job, args)

        self.assertTrue(job.debug)
        self.assertEqual(job.debug_command, 'tail pilotlog.txt')


class TestSendStateBackchannelIntegration(unittest.TestCase):
    """End-to-end test that send_state() correctly wires the normalization into the real update path."""

    def setUp(self):
        """Create a fresh, existing workdir for each test."""
        self.workdir = os.getcwd()

    def test_send_state_acts_on_tobekilled_from_api_v1_response(self):
        """Test that send_state() triggers the kill path given a real api/v1 server response.

        This is the regression test for the live failure: server sends 'tobekilled'
        via api/v1/pilot/update_job and the running job must be marked failed/aborted
        rather than being allowed to continue running.
        """
        job = FakeJob(self.workdir)
        job.completed = False
        job.fileinfo = {}
        job.serverstate = ''
        args = FakeArgs()
        args.update_server = True
        args.url = 'https://fake-panda-server.example.org'
        args.port = 25443
        args.internet_protocol_version = 'IPv6'
        args.last_heartbeat = 0
        args.pod = False

        fake_result = job_module.https.UpdateResult(
            ok=True,
            attempts=1,
            response=REAL_API_V1_TOBEKILLED_RESPONSE,
            success=True,
            status_code=0,
            command='tobekilled',
            message='',
        )

        with patch('pilot.control.job.https.send_update', return_value=fake_result), \
                patch('pilot.control.job.get_data_structure', return_value={}), \
                patch('pilot.control.job.is_final_update', return_value=False), \
                patch('pilot.control.job.config') as mock_config:
            mock_config.Pilot.pandajob = 'real'
            ok = job_module.send_state(job, args, 'running')

        self.assertTrue(ok)
        self.assertTrue(args.abort_job.is_set())
        self.assertEqual(job.state, 'failed')
        self.assertIn(errors.PANDAKILL, job.piloterrorcodes)


if __name__ == '__main__':
    unittest.main()
