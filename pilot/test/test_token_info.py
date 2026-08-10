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

"""Unit tests for the OIDC token inspection helpers in pilot.util.https.

The pilot's token identity resolves to a different PanDA owner than its X.509 proxy does,
which is why a get_user_secrets call authenticated with the token returns no secrets. The
owner is therefore worth logging whenever a token is decoded, so that it does not have to
be recovered from the server-side log.

Covers:
- get_token_owner(): claim precedence, absent claims, non-dict input.
- log_token_info(): the owner line, the identity claims, the validity window, expired
  tokens, list-valued and over-long claims, and the guarantee that the token itself is
  never logged.
- decode_jwt_payload(): decoding from a string and from a file, malformed input, and the
  return_times switch.
- get_local_token_owner(): the happy path and every way it can come up empty.
"""

import base64
import datetime
import json
import logging
import os
import sys
import tempfile
import unittest
from time import time
from unittest.mock import patch

from pilot.util.https import (
    TOKEN_OWNER_CLAIMS,
    decode_jwt_payload,
    get_local_token_owner,
    get_token_owner,
    log_token_info,
)

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# The owner observed in the PanDA server log for the token-authenticated call, alongside
# 'atlpilo1' for the proxy-authenticated one.
TOKEN_OWNER = 'Robot Pilot'

# A signature-like segment that must never reach the log.
FAKE_SIGNATURE = 'do-not-log-this-signature'


def make_jwt(payload: dict) -> str:
    """Build a JWT-shaped string carrying *payload*.

    The signature is not verified anywhere in the pilot, so a placeholder suffices.

    Args:
        payload: Claims to encode into the payload segment.

    Returns:
        A ``header.payload.signature`` string.
    """
    def encode(part: dict) -> str:
        raw = json.dumps(part).encode('utf-8')
        return base64.urlsafe_b64encode(raw).decode('utf-8').rstrip('=')

    return f"{encode({'alg': 'RS256', 'typ': 'JWT'})}.{encode(payload)}.{FAKE_SIGNATURE}"


def make_payload(**overrides) -> dict:
    """Build a realistic token payload.

    Args:
        **overrides: Claims to add to or replace in the default payload.

    Returns:
        A payload dict shaped like an IAM-issued robot token.
    """
    payload = {
        'sub': '1a2b3c4d-0000-0000-0000-abcdefabcdef',
        'name': TOKEN_OWNER,
        'preferred_username': 'atlpilo1',
        'client_id': 'pilot_server',
        'iss': 'https://atlas-auth.cern.ch/',
        'aud': ['https://pandaserver.cern.ch', 'panda_dev'],
        'scope': 'openid profile',
        'wlcg.groups': ['/atlas/production', '/atlas'],
        'jti': 'ffffffff-1111-2222-3333-444444444444',
        'iat': int(time()) - 60,
        'exp': int(time()) + 3600,
    }
    payload.update(overrides)

    return payload


class TestGetTokenOwner(unittest.TestCase):
    """Tests for get_token_owner()."""

    def test_name_claim_wins(self):
        """'name' is tried first, because that is what the server logs as the owner."""
        self.assertEqual(get_token_owner(make_payload()), TOKEN_OWNER)

    def test_falls_back_through_the_claim_order(self):
        """Each claim in turn takes over as the earlier ones are removed."""
        payload = make_payload()
        for claim in TOKEN_OWNER_CLAIMS:
            with self.subTest(claim=claim):
                self.assertEqual(get_token_owner(payload), str(payload[claim]))
            del payload[claim]
        self.assertEqual(get_token_owner(payload), '')

    def test_empty_claims_are_skipped(self):
        """An empty claim value does not count as an owner."""
        payload = make_payload(name='', preferred_username='')
        self.assertEqual(get_token_owner(payload), 'pilot_server')

    def test_no_identity_claims_yields_empty(self):
        """A payload with no identity claims at all yields an empty string."""
        self.assertEqual(get_token_owner({'iat': 0, 'exp': 1}), '')

    def test_non_dict_input_yields_empty(self):
        """A non-dict payload does not raise."""
        self.assertEqual(get_token_owner(None), '')
        self.assertEqual(get_token_owner('not a payload'), '')


class TestLogTokenInfo(unittest.TestCase):
    """Tests for log_token_info()."""

    def _log(self, payload: dict) -> str:
        """Capture the log output emitted for a payload.

        Args:
            payload: Decoded JWT payload.

        Returns:
            All log records emitted, joined into one string.
        """
        with self.assertLogs('pilot.util.https', level=logging.DEBUG) as captured:
            log_token_info(payload)

        return '\n'.join(captured.output)

    def test_owner_is_logged(self):
        """The owner is reported explicitly, not left to be inferred from the claims."""
        self.assertIn(f'token owner: {TOKEN_OWNER}', self._log(make_payload()))

    def test_unknown_owner_is_stated(self):
        """A token with no identity claim says so rather than logging an empty owner."""
        self.assertIn('unknown', self._log({'iat': int(time())}))

    def test_identity_claims_are_logged(self):
        """The curated identity claims are all reported when present."""
        output = self._log(make_payload())
        for expected in ('token sub:', 'token client_id:', 'token iss:', 'token jti:'):
            self.assertIn(expected, output)

    def test_list_claims_are_joined(self):
        """List-valued claims such as aud and wlcg.groups are rendered readably."""
        output = self._log(make_payload())
        self.assertIn('/atlas/production, /atlas', output)

    def test_long_claim_is_truncated(self):
        """An over-long claim value is truncated rather than flooding the log."""
        output = self._log(make_payload(scope='storage.read:/ ' * 500))
        self.assertIn('truncated', output)

    def test_validity_window_is_logged(self):
        """iat and exp are rendered as UTC timestamps with the remaining lifetime."""
        output = self._log(make_payload())
        self.assertIn('issued at (iat)', output)
        self.assertIn('expires at (exp)', output)
        self.assertIn('UTC', output)
        self.assertIn('remaining lifetime', output)

    def test_expired_token_is_flagged(self):
        """An already-expired token produces a warning rather than a lifetime."""
        output = self._log(make_payload(exp=int(time()) - 120))
        self.assertIn('expired', output)
        self.assertNotIn('remaining lifetime', output)

    def test_missing_times_are_reported(self):
        """Absent iat and exp are noted; absent nbf is not, being routinely absent."""
        output = self._log({'name': TOKEN_OWNER})
        self.assertIn("no 'iat' field found", output)
        self.assertIn("no 'exp' field found", output)
        self.assertNotIn("no 'nbf' field found", output)

    def test_unusable_timestamp_does_not_raise(self):
        """A non-numeric time claim is reported rather than raising."""
        self.assertIn('not a valid timestamp', self._log(make_payload(exp='never')))

    def test_nbf_is_logged_when_present(self):
        """nbf is reported when the issuer includes it."""
        self.assertIn('not valid before (nbf)', self._log(make_payload(nbf=int(time()) - 60)))

    def test_timestamp_is_utc(self):
        """The rendered timestamp matches the claim, in UTC."""
        moment = int(datetime.datetime(2026, 8, 7, 19, 12, 26, tzinfo=datetime.timezone.utc).timestamp())
        self.assertIn('2026-08-07 19:12:26 UTC', self._log(make_payload(iat=moment)))

    def test_email_claim_is_logged_when_present(self):
        """The email claim is included in the curated list."""
        self.assertIn('token email: pilot@cern.ch', self._log(make_payload(email='pilot@cern.ch')))


class TestDecodeJwtPayload(unittest.TestCase):
    """Tests for decode_jwt_payload()."""

    def test_decodes_from_string(self):
        """A raw JWT string is decoded to its payload."""
        payload = make_payload()
        self.assertEqual(decode_jwt_payload(make_jwt(payload), return_times=False), payload)

    def test_decodes_from_file(self):
        """A path to a token file is read and decoded."""
        payload = make_payload()
        with tempfile.NamedTemporaryFile('w', suffix='.token', delete=False) as handle:
            handle.write(f'{make_jwt(payload)}\n')
            path = handle.name
        try:
            self.assertEqual(decode_jwt_payload(path, return_times=False), payload)
        finally:
            os.unlink(path)

    def test_logs_the_owner_when_times_requested(self):
        """The default logging path reports the owner."""
        with self.assertLogs('pilot.util.https', level=logging.DEBUG) as captured:
            decode_jwt_payload(make_jwt(make_payload()), return_times=True)
        self.assertIn(f'token owner: {TOKEN_OWNER}', '\n'.join(captured.output))

    def test_logs_nothing_when_times_not_requested(self):
        """return_times=False stays silent, as get_local_token_owner() relies on."""
        with self.assertRaises(AssertionError):
            with self.assertLogs('pilot.util.https', level=logging.DEBUG):
                decode_jwt_payload(make_jwt(make_payload()), return_times=False)

    def test_the_token_itself_is_never_logged(self):
        """No log record carries the token or its signature."""
        token = make_jwt(make_payload())
        with self.assertLogs('pilot.util.https', level=logging.DEBUG) as captured:
            decode_jwt_payload(token, return_times=True)
        output = '\n'.join(captured.output)
        self.assertNotIn(FAKE_SIGNATURE, output)
        self.assertNotIn(token, output)

    def test_wrong_segment_count_raises(self):
        """A string that is not a three-segment JWT raises ValueError."""
        with self.assertRaises(ValueError):
            decode_jwt_payload('not.a.jwt.at.all', return_times=False)

    def test_undecodable_payload_raises(self):
        """A three-segment string whose payload is not JSON raises ValueError."""
        with self.assertRaises(ValueError):
            decode_jwt_payload('aaa.bbb.ccc', return_times=False)


class TestGetLocalTokenOwner(unittest.TestCase):
    """Tests for get_local_token_owner()."""

    def test_returns_the_owner_of_the_local_token(self):
        """The local token is located, decoded and its owner returned."""
        payload = make_payload()
        with tempfile.NamedTemporaryFile('w', suffix='.token', delete=False) as handle:
            handle.write(make_jwt(payload))
            path = handle.name
        try:
            with patch('pilot.util.https.get_local_oidc_token_info', return_value=(path, 'atlas.pilot')), \
                    patch('pilot.util.https.locate_token', return_value=path):
                self.assertEqual(get_local_token_owner(), TOKEN_OWNER)
        finally:
            os.unlink(path)

    def test_no_token_configured_yields_empty(self):
        """No token in the environment yields an empty string rather than raising."""
        with patch('pilot.util.https.get_local_oidc_token_info', return_value=(None, None)):
            self.assertEqual(get_local_token_owner(), '')

    def test_unlocatable_token_yields_empty(self):
        """A configured token that cannot be found yields an empty string."""
        with patch('pilot.util.https.get_local_oidc_token_info', return_value=('token', 'atlas.pilot')), \
                patch('pilot.util.https.locate_token', return_value=''):
            self.assertEqual(get_local_token_owner(), '')

    def test_undecodable_token_yields_empty(self):
        """A token that will not decode yields an empty string rather than raising."""
        with tempfile.NamedTemporaryFile('w', suffix='.token', delete=False) as handle:
            handle.write('this is not a jwt')
            path = handle.name
        try:
            with patch('pilot.util.https.get_local_oidc_token_info', return_value=(path, 'atlas.pilot')), \
                    patch('pilot.util.https.locate_token', return_value=path):
                self.assertEqual(get_local_token_owner(), '')
        finally:
            os.unlink(path)


if __name__ == '__main__':
    unittest.main()
