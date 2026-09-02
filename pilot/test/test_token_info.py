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
token is therefore worth logging whenever it is decoded, so that a lookup returning
nothing does not have to be diagnosed from the server-side log.

The pilot's real token, observed on 2026-08-10, is a client-credentials token: it carries
a client UUID in both 'sub' and 'client_id' and no user identity claim whatsoever. The
owner the server reported for the same identity, 'Robot Pilot', appears in none of its
claims, so the owner name is resolved server-side and cannot be derived by the pilot -
which is why these helpers report the subject and say so explicitly.

Covers:
- get_token_subject(): the real client-credentials token, claim precedence, absent claims,
  non-dict input.
- log_token_info(): the subject line, the caveat about the owner name, the logged claims,
  the validity window, expired tokens, list-valued and over-long claims, and the guarantee
  that the token itself is never logged.
- decode_jwt_payload(): decoding from a string and from a file, malformed input, and the
  return_times switch.
- get_local_token_subject(): the happy path and every way it can come up empty.
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
    TOKEN_SUBJECT_CLAIMS,
    decode_jwt_payload,
    get_local_token_subject,
    get_token_subject,
    log_token_info,
)

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# The owner observed in the PanDA server log for the token-authenticated call, alongside
# 'atlpilo1' for the proxy-authenticated one. It is not a claim of the token: no part of
# the pilot's real token contains it, which is what these tests pin down.
SERVER_REPORTED_OWNER = 'Robot Pilot'

# The client UUID carried by the pilot's real token, in both 'sub' and 'client_id'.
CLIENT_UUID = '2d1fa96c-5e70-4e67-b57d-0d28257b2795'

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
    """Build a payload matching the pilot's real token, observed on 2026-08-10.

    A client-credentials token: the client UUID appears in both 'sub' and 'client_id',
    and there is no 'name', 'preferred_username' or 'email' claim at all.

    Args:
        **overrides: Claims to add to or replace in the default payload.

    Returns:
        A payload dict shaped like the token the pilot actually receives.
    """
    payload = {
        'sub': CLIENT_UUID,
        'client_id': CLIENT_UUID,
        'iss': 'https://atlas-auth.cern.ch/',
        'aud': 'https://wlcg.cern.ch/jwt/v1/any',
        'scope': 'wlcg wlcg.groups',
        'jti': '6f596ef0-1dc2-466a-951c-a5632ddce3dd',
        'iat': int(time()) - 27,
        'nbf': int(time()) - 87,
        'exp': int(time()) + 341972,
    }
    payload.update(overrides)

    return payload


def make_user_payload(**overrides) -> dict:
    """Build a user-style token payload carrying human-readable identity claims.

    The pilot is not expected to see one of these, but the helpers must handle it.

    Args:
        **overrides: Claims to add to or replace in the default payload.

    Returns:
        A payload dict with 'name', 'preferred_username' and list-valued claims.
    """
    payload = {
        'sub': '1a2b3c4d-0000-0000-0000-abcdefabcdef',
        'name': SERVER_REPORTED_OWNER,
        'preferred_username': 'atlpilo1',
        'email': 'pilot@cern.ch',
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


class TestGetTokenSubject(unittest.TestCase):
    """Tests for get_token_subject()."""

    def test_real_token_yields_the_client_uuid_from_sub(self):
        """The pilot's real token has no identity claim beyond the client UUID."""
        self.assertEqual(get_token_subject(make_payload()), (CLIENT_UUID, 'sub'))

    def test_real_token_carries_no_owner_name(self):
        """The owner the server reports appears in no claim of the real token.

        This is the finding the helpers are built around: were the owner name present,
        the pilot could report it directly instead of reporting the subject.
        """
        self.assertNotIn(SERVER_REPORTED_OWNER, json.dumps(make_payload()))

    def test_sub_is_preferred_over_a_human_readable_claim(self):
        """A user token still reports 'sub': the subject is the stable identifier."""
        self.assertEqual(get_token_subject(make_user_payload())[1], 'sub')

    def test_falls_back_through_the_claim_order(self):
        """Each claim in turn takes over as the earlier ones are removed."""
        payload = make_user_payload()
        for claim in TOKEN_SUBJECT_CLAIMS:
            with self.subTest(claim=claim):
                self.assertEqual(get_token_subject(payload), (str(payload[claim]), claim))
            del payload[claim]
        self.assertEqual(get_token_subject(payload), ('', ''))

    def test_empty_claims_are_skipped(self):
        """An empty claim value does not count as a subject."""
        payload = make_payload(sub='')
        self.assertEqual(get_token_subject(payload), (CLIENT_UUID, 'client_id'))

    def test_no_subject_claims_yields_empty(self):
        """A payload with no subject claim at all yields an empty string."""
        self.assertEqual(get_token_subject({'iat': 0, 'exp': 1}), ('', ''))

    def test_non_dict_input_yields_empty(self):
        """A non-dict payload does not raise."""
        self.assertEqual(get_token_subject(None), ('', ''))
        self.assertEqual(get_token_subject('not a payload'), ('', ''))


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

    def test_subject_is_logged(self):
        """The subject is reported explicitly, not left to be read off the claim list."""
        self.assertIn(f'token subject: {CLIENT_UUID}', self._log(make_payload()))

    def test_subject_line_names_the_claim_it_came_from(self):
        """The claim is named, so the value can be traced back to its source."""
        self.assertIn("from the 'sub' claim", self._log(make_payload()))

    def test_subject_line_names_a_fallback_claim(self):
        """The named claim follows the fallback rather than being hardcoded."""
        self.assertIn("from the 'client_id' claim", self._log(make_payload(sub='')))

    def test_owner_name_is_not_claimed_to_be_known(self):
        """The log states that the owner name is resolved server-side, not by the pilot."""
        output = self._log(make_payload())
        self.assertIn('not a token claim', output)
        self.assertNotIn(SERVER_REPORTED_OWNER, output)

    def test_unknown_subject_is_stated(self):
        """A token with no subject claim says so rather than logging an empty value."""
        self.assertIn('unknown', self._log({'iat': int(time())}))

    def test_claims_of_the_real_token_are_logged(self):
        """Every claim the real token carries is reported."""
        output = self._log(make_payload())
        for expected in ('token sub:', 'token client_id:', 'token iss:', 'token aud:',
                         'token scope:', 'token jti:'):
            self.assertIn(expected, output)

    def test_absent_claims_are_skipped_silently(self):
        """The real token has no name or email claim; neither is mentioned."""
        output = self._log(make_payload())
        self.assertNotIn('token name:', output)
        self.assertNotIn('token email:', output)

    def test_list_claims_are_joined(self):
        """List-valued claims such as aud and wlcg.groups are rendered readably."""
        output = self._log(make_user_payload())
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
        output = self._log({'sub': CLIENT_UUID})
        self.assertIn("no 'iat' field found", output)
        self.assertIn("no 'exp' field found", output)
        self.assertNotIn("no 'nbf' field found", output)

    def test_unusable_timestamp_does_not_raise(self):
        """A non-numeric time claim is reported rather than raising."""
        self.assertIn('not a valid timestamp', self._log(make_payload(exp='never')))

    def test_nbf_is_logged_when_present(self):
        """nbf is reported; the real token carries one."""
        self.assertIn('not valid before (nbf)', self._log(make_payload()))

    def test_timestamp_is_utc(self):
        """The rendered timestamp matches the claim, in UTC."""
        moment = int(datetime.datetime(2026, 8, 7, 19, 12, 26, tzinfo=datetime.timezone.utc).timestamp())
        self.assertIn('2026-08-07 19:12:26 UTC', self._log(make_payload(iat=moment)))

    def test_email_claim_is_logged_when_present(self):
        """The email claim is included in the curated list."""
        self.assertIn('token email: pilot@cern.ch', self._log(make_user_payload()))

    def test_long_lifetime_is_reported_in_hours(self):
        """The real token has a four-day lifetime, which is reported in hours."""
        self.assertIn('h)', self._log(make_payload()))


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

    def test_logs_the_subject_when_times_requested(self):
        """The default logging path reports the subject."""
        with self.assertLogs('pilot.util.https', level=logging.DEBUG) as captured:
            decode_jwt_payload(make_jwt(make_payload()), return_times=True)
        self.assertIn(f'token subject: {CLIENT_UUID}', '\n'.join(captured.output))

    def test_logs_nothing_when_times_not_requested(self):
        """return_times=False stays silent, as get_local_token_subject() relies on."""
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


class TestGetLocalTokenSubject(unittest.TestCase):
    """Tests for get_local_token_subject()."""

    def test_returns_the_subject_of_the_local_token(self):
        """The local token is located, decoded and its subject returned."""
        payload = make_payload()
        with tempfile.NamedTemporaryFile('w', suffix='.token', delete=False) as handle:
            handle.write(make_jwt(payload))
            path = handle.name
        try:
            with patch('pilot.util.https.get_local_oidc_token_info', return_value=(path, 'atlas.pilot')), \
                    patch('pilot.util.https.locate_token', return_value=path):
                self.assertEqual(get_local_token_subject(), CLIENT_UUID)
        finally:
            os.unlink(path)

    def test_no_token_configured_yields_empty(self):
        """No token in the environment yields an empty string rather than raising."""
        with patch('pilot.util.https.get_local_oidc_token_info', return_value=(None, None)):
            self.assertEqual(get_local_token_subject(), '')

    def test_unlocatable_token_yields_empty(self):
        """A configured token that cannot be found yields an empty string."""
        with patch('pilot.util.https.get_local_oidc_token_info', return_value=('token', 'atlas.pilot')), \
                patch('pilot.util.https.locate_token', return_value=''):
            self.assertEqual(get_local_token_subject(), '')

    def test_undecodable_token_yields_empty(self):
        """A token that will not decode yields an empty string rather than raising."""
        with tempfile.NamedTemporaryFile('w', suffix='.token', delete=False) as handle:
            handle.write('this is not a jwt')
            path = handle.name
        try:
            with patch('pilot.util.https.get_local_oidc_token_info', return_value=(path, 'atlas.pilot')), \
                    patch('pilot.util.https.locate_token', return_value=path):
                self.assertEqual(get_local_token_subject(), '')
        finally:
            os.unlink(path)


if __name__ == '__main__':
    unittest.main()
