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

"""Unit tests for the ActiveMQ credentials retrieval.

The message broker credentials come from the new PanDA credentials API. The call shape
was established empirically on the grid on 2026-08-07 and is fixed by these tests:

    GET api/v1/creds/get_user_secrets?keys=MB_USERNAME&keys=MB_PASSWORD

with no OIDC token, since the secrets are bound to the X.509 proxy identity rather than
to the pilot's token identity.

Covers:
- extract_credentials(): the response shape actually observed on the grid (``data`` as a
  JSON-encoded string), the server's false-positive success (``success=true`` with
  ``data="{}"``), reported failures, transport failures, unparseable text, partial and
  empty secrets, and the defensive dictionary path.
- get_credentials(): the request is built as a GET with ``keys`` as a list and no OIDC
  token, the credentials are stored on success, and nothing is stored on failure.
- the guarantee that no failure path writes the password into the log.
"""

import json
import logging
import sys
import unittest
from typing import Any
from unittest.mock import patch

from pilot.util.activemq import (
    ActiveMQ,
    CREDENTIALS_ENDPOINT,
    MB_CREDENTIAL_KEYS,
    extract_credentials,
    scrub_text,
)

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# A recognisable stand-in for the real password. If this string ever turns up in a log
# record or in an exception message, a real password would have leaked in production.
SECRET_PW = 'sup3rs3cr3t-do-not-log'

# The response shape observed on the grid: 'data' is a JSON-encoded string.
GRID_RESPONSE = {
    'success': True,
    'message': '',
    'data': json.dumps({'MB_USERNAME': 'atlpndpilot', 'MB_PASSWORD': SECRET_PW}),
}


def make_activemq() -> ActiveMQ:
    """Build an ActiveMQ instance without running __init__().

    ``ActiveMQ.__init__()`` resolves brokers and opens STOMP connections, none of which
    is wanted here. Only the attributes that ``get_credentials()`` touches are set.

    Returns:
        An ActiveMQ instance ready for a get_credentials() call.
    """
    amq = ActiveMQ.__new__(ActiveMQ)
    amq.logger = logging.getLogger('ActiveMQ')
    amq.pandaurl = 'https://pandaserver.cern.ch'
    amq.pandaport = 25443
    amq.username = None
    amq.password = None

    return amq


class TestExtractCredentials(unittest.TestCase):
    """Tests for extract_credentials()."""

    def test_json_encoded_data_is_the_real_shape(self):
        """The observed grid response, with 'data' as a JSON-encoded string, is parsed."""
        self.assertEqual(extract_credentials(GRID_RESPONSE), ('atlpndpilot', SECRET_PW))

    def test_data_as_dict_is_accepted_defensively(self):
        """A nested object in 'data' is parsed too, in case the server shape changes."""
        payload = {
            'success': True,
            'message': '',
            'data': {'MB_USERNAME': 'mbuser', 'MB_PASSWORD': 'mbpass'},
        }
        self.assertEqual(extract_credentials(payload), ('mbuser', 'mbpass'))

    def test_raw_text_response_is_parsed(self):
        """request2() falls back to raw text when the response will not parse for it."""
        payload = '{"success": true, "message": "", "data": "{\\"MB_USERNAME\\": \\"u\\", \\"MB_PASSWORD\\": \\"p\\"}"}'
        self.assertEqual(extract_credentials(payload), ('u', 'p'))

    def test_bytes_response_is_decoded(self):
        """Byte responses are decoded before parsing."""
        payload = b'{"success": true, "data": {"MB_USERNAME": "u", "MB_PASSWORD": "p"}}'
        self.assertEqual(extract_credentials(payload), ('u', 'p'))

    def test_success_true_with_empty_data_raises(self):
        """The server's false-positive success must not become empty credentials.

        This is the shape returned when the call is authenticated with the OIDC token
        instead of the X.509 proxy: success=true, but data is an empty JSON object.
        """
        payload = {'success': True, 'message': '', 'data': '{}'}
        with self.assertRaises(ValueError):
            extract_credentials(payload)

    def test_success_true_with_empty_dict_data_raises(self):
        """The same, with 'data' as an empty object rather than an empty string."""
        with self.assertRaises(ValueError):
            extract_credentials({'success': True, 'message': '', 'data': {}})

    def test_failure_surfaces_the_server_message(self):
        """success=False raises and preserves the server's explanation."""
        payload = {'success': False, 'message': 'expecting GET, received POST', 'data': ''}
        with self.assertRaises(ValueError) as ctx:
            extract_credentials(payload)
        self.assertIn('expecting GET, received POST', str(ctx.exception))

    def test_transport_failure_marker_raises(self):
        """The 'failed to send request' marker from request2() is reported as-is."""
        payload = 'failed to send request: HTTP Error 503: Service Unavailable'
        with self.assertRaises(ValueError) as ctx:
            extract_credentials(payload)
        self.assertIn('503', str(ctx.exception))

    def test_empty_response_raises(self):
        """An empty response raises rather than yielding empty credentials."""
        with self.assertRaises(ValueError):
            extract_credentials('')

    def test_non_json_text_raises(self):
        """A proxy error page raises rather than crashing."""
        with self.assertRaises(ValueError):
            extract_credentials('<html><body>502 Bad Gateway</body></html>')

    def test_unexpected_type_raises(self):
        """A response that parses to something other than a dict raises."""
        with self.assertRaises(ValueError):
            extract_credentials('[1, 2, 3]')

    def test_missing_password_key_names_the_key(self):
        """A partial secrets mapping raises and names the missing key."""
        payload = {'success': True, 'message': '', 'data': '{"MB_USERNAME": "u"}'}
        with self.assertRaises(ValueError) as ctx:
            extract_credentials(payload)
        self.assertIn('MB_PASSWORD', str(ctx.exception))

    def test_empty_credential_value_raises(self):
        """An empty credential value is rejected rather than returned."""
        payload = {'success': True, 'data': {'MB_USERNAME': 'u', 'MB_PASSWORD': ''}}
        with self.assertRaises(ValueError):
            extract_credentials(payload)

    def test_bare_secrets_mapping_is_accepted(self):
        """A response without the status envelope is treated as the secrets mapping."""
        payload = {'MB_USERNAME': 'mbuser', 'MB_PASSWORD': 'mbpass'}
        self.assertEqual(extract_credentials(payload), ('mbuser', 'mbpass'))

    def test_custom_keys_are_honoured(self):
        """Alternative secret names can be requested."""
        payload = {'success': True, 'data': {'USER': 'u', 'PASS': 'p'}}
        self.assertEqual(extract_credentials(payload, keys=('USER', 'PASS')), ('u', 'p'))


class TestGetCredentialsRequestShape(unittest.TestCase):
    """The call shape confirmed on the grid must not regress."""

    def test_request_is_a_get_with_keys_as_a_list(self):
        """GET, keys as a list in params, no json_body, no OIDC token."""
        amq = make_activemq()
        with patch('pilot.util.activemq.https.get_server_command') as mock_cmd, \
                patch('pilot.util.activemq.https.request2') as mock_request:
            mock_cmd.return_value = f'https://aipanda097.cern.ch:25443/{CREDENTIALS_ENDPOINT}'
            mock_request.return_value = GRID_RESPONSE
            amq.get_credentials()

        self.assertEqual(mock_cmd.call_args.kwargs['cmd'], CREDENTIALS_ENDPOINT)

        kwargs = mock_request.call_args.kwargs
        self.assertEqual(kwargs['method'], 'GET')
        self.assertEqual(kwargs['params'], {'keys': ['MB_USERNAME', 'MB_PASSWORD']})
        self.assertIsInstance(kwargs['params']['keys'], list)
        self.assertNotIn('json_body', kwargs)
        self.assertNotIn('data', kwargs)
        # the secrets are bound to the proxy identity: panda must stay at its default (False)
        self.assertFalse(kwargs.get('panda', False))
        # the removed legacy payload must not come back
        self.assertNotIn('get_json', kwargs['params'])

    def test_credentials_are_stored_on_success(self):
        """A successful call populates username and password."""
        amq = make_activemq()
        with patch('pilot.util.activemq.https.get_server_command', return_value='https://x/y'), \
                patch('pilot.util.activemq.https.request2', return_value=GRID_RESPONSE):
            amq.get_credentials()

        self.assertEqual(amq.username, 'atlpndpilot')
        self.assertEqual(amq.password, SECRET_PW)

    def test_nothing_is_stored_when_the_server_returns_no_secrets(self):
        """success=true with empty data leaves the credentials unset."""
        amq = make_activemq()
        empty = {'success': True, 'message': '', 'data': '{}'}
        with patch('pilot.util.activemq.https.get_server_command', return_value='https://x/y'), \
                patch('pilot.util.activemq.https.request2', return_value=empty):
            amq.get_credentials()

        self.assertFalse(amq.username)
        self.assertFalse(amq.password)

    def test_no_request_without_server_url(self):
        """An unconfigured server URL or port aborts before any request is sent."""
        amq = make_activemq()
        amq.pandaport = 0
        with patch('pilot.util.activemq.https.request2') as mock_request:
            amq.get_credentials()
        mock_request.assert_not_called()


class TestNothingLeaksToTheLog(unittest.TestCase):
    """The pilot log is uploaded, so no failure path may carry the password into it."""

    def _records(self, response: Any) -> str:
        """Run get_credentials() against a response and return the emitted log text.

        Args:
            response: Value to be returned by the patched request2().

        Returns:
            All log records emitted during the call, joined into one string.
        """
        amq = make_activemq()
        with patch('pilot.util.activemq.https.get_server_command', return_value='https://x/y'), \
                patch('pilot.util.activemq.https.request2', return_value=response):
            with self.assertLogs(amq.logger, level=logging.DEBUG) as captured:
                amq.get_credentials()

        return '\n'.join(captured.output)

    def test_success_logs_username_and_length_only(self):
        """The success path logs the username and the password length, not the password."""
        output = self._records(GRID_RESPONSE)
        self.assertIn('atlpndpilot', output)
        self.assertIn(f'password length {len(SECRET_PW)}', output)
        self.assertNotIn(SECRET_PW, output)

    def test_partial_response_failure_does_not_log_the_password(self):
        """A response missing the username still must not echo the password."""
        payload = {'success': True, 'message': '', 'data': json.dumps({'MB_PASSWORD': SECRET_PW})}
        self.assertNotIn(SECRET_PW, self._records(payload))

    def test_unparseable_response_does_not_log_the_password(self):
        """A response that is not JSON is scrubbed before being quoted."""
        payload = f'ERROR MB_PASSWORD={SECRET_PW} could not be served'
        self.assertNotIn(SECRET_PW, self._records(payload))

    def test_server_failure_does_not_log_the_password(self):
        """A reported failure surfaces the message without the rest of the response."""
        payload = {'success': False, 'message': 'expecting GET, received POST', 'data': SECRET_PW}
        output = self._records(payload)
        self.assertIn('expecting GET, received POST', output)
        self.assertNotIn(SECRET_PW, output)


class TestScrubText(unittest.TestCase):
    """Tests for scrub_text()."""

    def test_masks_json_style_values(self):
        """A "KEY": "value" pair with a secret-looking name is masked."""
        self.assertNotIn(SECRET_PW, scrub_text(f'{{"MB_PASSWORD": "{SECRET_PW}"}}'))

    def test_masks_query_style_values(self):
        """A KEY=value pair with a secret-looking name is masked."""
        self.assertNotIn(SECRET_PW, scrub_text(f'user=u&MB_PASSWORD={SECRET_PW}'))

    def test_leaves_harmless_text_alone(self):
        """Text without a secret-looking field name is returned unchanged."""
        self.assertEqual(scrub_text('502 Bad Gateway'), '502 Bad Gateway')


class TestConstants(unittest.TestCase):
    """The endpoint and key names are part of the confirmed call shape."""

    def test_endpoint_is_the_new_api_path(self):
        """The endpoint is the new API path, not the old dispatcher command."""
        self.assertEqual(CREDENTIALS_ENDPOINT, 'api/v1/creds/get_user_secrets')
        self.assertNotIn('get_access_token', CREDENTIALS_ENDPOINT)

    def test_keys_are_username_first(self):
        """extract_credentials() relies on the username coming first."""
        self.assertEqual(tuple(MB_CREDENTIAL_KEYS), ('MB_USERNAME', 'MB_PASSWORD'))


if __name__ == '__main__':
    unittest.main()
