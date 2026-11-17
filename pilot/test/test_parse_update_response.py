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
# Authors
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Unit tests for the parsing of PanDA server update responses (pilot.util.https)."""

import unittest
from unittest.mock import patch

from pilot.util import https

# the server responses seen for job 7378803325 (grid.40547911.7.out, 2026-10-23)
ALREADY_INSERTED = {'success': False,
                    'message': 'Pilot attributes for PandaID=7378803325 were already inserted and cannot be updated.',
                    'data': None}
INSERTED = {'success': True, 'message': 'Inserted pilot attributes.', 'data': None}
TOBEKILLED = {'success': True, 'message': '', 'data': {'StatusCode': 0, 'command': 'tobekilled'}}


class TestParseUpdateResponse(unittest.TestCase):
    """_parse_update_response() turns a server response into (ok, status_code, command, message)."""

    def test_rejected_with_null_data_keeps_server_message(self):
        """Job 7378803325: success=False with data=None reports the server message, not a malformed response."""
        self.assertEqual(https._parse_update_response(ALREADY_INSERTED),  # pylint: disable=protected-access
                         (False, None, None, ALREADY_INSERTED['message']))

    def test_rejected_with_null_data_and_no_message(self):
        """success=False with data=None and no message gets the generic server failure message."""
        res = {'success': False, 'message': '', 'data': None}
        self.assertEqual(https._parse_update_response(res),  # pylint: disable=protected-access
                         (False, None, None, 'Server returned success=False'))

    def test_accepted_with_null_data(self):
        """success=True with data=None is accepted (as before)."""
        self.assertEqual(https._parse_update_response(INSERTED),  # pylint: disable=protected-access
                         (True, None, None, 'Inserted pilot attributes.'))

    def test_rejected_without_data_field(self):
        """A missing data field behaves like data=None."""
        res = {'success': False, 'message': 'nope'}
        self.assertEqual(https._parse_update_response(res),  # pylint: disable=protected-access
                         (False, None, None, 'nope'))

    def test_accepted_with_command(self):
        """The backchannel command and status code are returned for an accepted update."""
        self.assertEqual(https._parse_update_response(TOBEKILLED),  # pylint: disable=protected-access
                         (True, 0, 'tobekilled', ''))

    def test_non_zero_status_code(self):
        """A non-zero StatusCode is a rejection even when success=True."""
        res = {'success': True, 'message': '', 'data': {'StatusCode': 30}}
        self.assertEqual(https._parse_update_response(res),  # pylint: disable=protected-access
                         (False, 30, None, 'Server returned StatusCode=30'))

    def test_non_dict_data_is_still_malformed(self):
        """Data that is neither a dictionary nor None is still reported as malformed."""
        for data in ('text', [], 0):
            with self.subTest(data=data):
                ok, status_code, command, message = https._parse_update_response(  # pylint: disable=protected-access
                    {'success': False, 'message': 'm', 'data': data})
                self.assertFalse(ok)
                self.assertIsNone(status_code)
                self.assertIsNone(command)
                self.assertEqual(message, f'Malformed response: data is {type(data)}')


class TestSendUpdateMessage(unittest.TestCase):
    """send_update() passes the server message on to the caller."""

    def _send(self, res: dict) -> https.UpdateResult:
        """Send a pilot attributes update with the given server response.

        Args:
            res: Server response.

        Returns:
            Update result.
        """
        with patch.object(https, 'get_panda_server', return_value='https://pandaserver.example.org:25443'), \
                patch.object(https, 'send_request', return_value=res):
            return https.send_update('api/v1/pilot/update_pilot_attributes', {'job_id': 7378803325},
                                     'https://pandaserver.example.org', 25443, job=None, max_attempts=1)

    def test_job_7378803325_rejection(self):
        """The rejection of a repeated telemetry document carries the server message."""
        with self.assertLogs(https.logger, level='WARNING') as logs:
            result = self._send(ALREADY_INSERTED)
        self.assertFalse(result.ok)
        self.assertFalse(result.success)
        self.assertEqual(result.error_type, 'server')
        self.assertEqual(result.message, ALREADY_INSERTED['message'])
        self.assertNotIn('Malformed', '\n'.join(logs.output))
        self.assertIn('were already inserted', '\n'.join(logs.output))

    def test_accepted(self):
        """An accepted telemetry document carries the server message."""
        result = self._send(INSERTED)
        self.assertTrue(result.ok)
        self.assertEqual(result.message, 'Inserted pilot attributes.')


if __name__ == '__main__':
    unittest.main()
