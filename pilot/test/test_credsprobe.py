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

"""Unit tests for the temporary credentials API probe.

TEMPORARY: delete together with pilot/util/credsprobe.py. When
parse_credentials_response() moves into pilot/util/activemq.py, these tests
should move to test_activemq.py with the import updated.

The probe itself is not unit tested: it runs unconditionally at pilot start-up
and reports over the network. What is tested here is everything that has to be
correct before the results in pilotlog.txt can be trusted - the response parser
and the request builders - plus the guarantee that the probe can never affect
the pilot.

The parser must cope with the new PanDA API response envelope

    {"success": true, "message": "string", "data": {}}

as well as the legacy shapes, because the message broker credentials call is
migrating from the old dispatcher endpoint (get_access_token under
/server/panda/) to api/v1/creds/get_user_secrets. It is not yet established
which shape the 'data' field takes for user secrets, so all three plausible
placements are covered: flat in 'data', nested under 'data.secrets', and
JSON-encoded as a string (which is a real possibility given that the request
carries get_json=true).
"""

import logging
import unittest
from types import SimpleNamespace

from pilot.util import credsprobe
from pilot.util.credsprobe import (
    ENDPOINTS,
    KEYS,
    VARIANTS,
    build_body,
    build_params,
    check_prerequisites,
    extract_secrets,
    mask_secret,
    parse_credentials_response,
    redact,
    run_probe,
    safe_preview,
    scrub_text,
)


class TestParseCredentialsResponse(unittest.TestCase):
    """Tests for parse_credentials_response()."""

    def test_new_style_flat_data(self):
        """Secrets sitting directly in the 'data' field are extracted."""
        payload = {
            "success": True,
            "message": "OK",
            "data": {"MB_USERNAME": "mbuser", "MB_PASSWORD": "mbpass"},
        }
        self.assertEqual(parse_credentials_response(payload), ("mbuser", "mbpass"))

    def test_new_style_nested_container(self):
        """Secrets nested under 'data.secrets' are extracted."""
        payload = {
            "success": True,
            "message": "",
            "data": {"secrets": {"MB_USERNAME": "mbuser", "MB_PASSWORD": "mbpass"}},
        }
        self.assertEqual(parse_credentials_response(payload), ("mbuser", "mbpass"))

    def test_new_style_json_encoded_data(self):
        """A JSON-encoded string in 'data' is decoded before extraction."""
        payload = {
            "success": True,
            "message": "",
            "data": '{"MB_USERNAME": "mbuser", "MB_PASSWORD": "mbpass"}',
        }
        self.assertEqual(parse_credentials_response(payload), ("mbuser", "mbpass"))

    def test_new_style_raw_text(self):
        """Raw response text is parsed (request2() falls back to text on parse failure)."""
        payload = '{"success": true, "message": "", "data": {"MB_USERNAME": "u", "MB_PASSWORD": "p"}}'
        self.assertEqual(parse_credentials_response(payload), ("u", "p"))

    def test_new_style_bytes(self):
        """Byte responses are decoded before parsing."""
        payload = b'{"success": true, "data": {"MB_USERNAME": "u", "MB_PASSWORD": "p"}}'
        self.assertEqual(parse_credentials_response(payload), ("u", "p"))

    def test_new_style_failure_reports_message(self):
        """success=False raises, and the server message is preserved."""
        payload = {"success": False, "message": "invalid token", "data": {}}
        with self.assertRaises(ValueError) as ctx:
            parse_credentials_response(payload)
        self.assertIn("invalid token", str(ctx.exception))

    def test_new_style_empty_data(self):
        """success=True with an empty 'data' field is still an error."""
        with self.assertRaises(ValueError):
            parse_credentials_response({"success": True, "message": "", "data": {}})

    def test_missing_password_key(self):
        """A partial secrets mapping raises and names the missing key."""
        payload = {"success": True, "message": "", "data": {"MB_USERNAME": "u"}}
        with self.assertRaises(ValueError) as ctx:
            parse_credentials_response(payload)
        self.assertIn("MB_PASSWORD", str(ctx.exception))

    def test_empty_password_value(self):
        """An empty credential value is rejected rather than returned."""
        payload = {"success": True, "data": {"MB_USERNAME": "u", "MB_PASSWORD": ""}}
        with self.assertRaises(ValueError):
            parse_credentials_response(payload)

    def test_legacy_bare_dict(self):
        """A bare secrets dict (no envelope) is still accepted."""
        payload = {"MB_USERNAME": "mbuser", "MB_PASSWORD": "mbpass"}
        self.assertEqual(parse_credentials_response(payload), ("mbuser", "mbpass"))

    def test_legacy_statuscode_envelope(self):
        """The legacy StatusCode envelope is accepted."""
        payload = {"StatusCode": 0, "MB_USERNAME": "mbuser", "MB_PASSWORD": "mbpass"}
        self.assertEqual(parse_credentials_response(payload), ("mbuser", "mbpass"))

    def test_legacy_statuscode_failure(self):
        """A non-zero StatusCode raises and preserves the error dialog."""
        payload = {"StatusCode": 20, "ErrorDialog": "no permission"}
        with self.assertRaises(ValueError) as ctx:
            parse_credentials_response(payload)
        self.assertIn("no permission", str(ctx.exception))

    def test_html_error_page(self):
        """A non-JSON response (e.g. a proxy error page) raises rather than crashing."""
        with self.assertRaises(ValueError):
            parse_credentials_response("<html><body>502 Bad Gateway</body></html>")

    def test_custom_keys(self):
        """Alternative secret names are honoured."""
        payload = {"success": True, "data": {"USER": "u", "PASS": "p"}}
        self.assertEqual(parse_credentials_response(payload, keys="USER,PASS"), ("u", "p"))


class TestExtractSecrets(unittest.TestCase):
    """Tests for extract_secrets()."""

    def test_returns_empty_for_unusable_input(self):
        """Non-mapping input yields an empty dict rather than raising."""
        self.assertEqual(extract_secrets(None), {})
        self.assertEqual(extract_secrets([1, 2, 3]), {})
        self.assertEqual(extract_secrets("not json"), {})

    def test_ignores_empty_container(self):
        """An empty nested container falls through to the outer mapping."""
        data = {"secrets": {}, "MB_USERNAME": "u", "MB_PASSWORD": "p"}
        self.assertEqual(extract_secrets(data), data)


class TestRequestConstruction(unittest.TestCase):
    """Tests for the variant request builders."""

    def test_get_variant_sends_lowercase_true(self):
        """get_json is sent as the JSON spelling, since query values go through str()."""
        params = build_params(VARIANTS["get-token"], KEYS, "")
        self.assertEqual(params["get_json"], "true")
        self.assertEqual(params["keys"], KEYS)

    def test_get_variant_has_no_body(self):
        """GET variants must not carry a body: request2() rejects that combination."""
        self.assertIsNone(build_body(VARIANTS["get-token"], KEYS))

    def test_post_variant_sends_native_bool(self):
        """POST variants send a real boolean in the JSON body."""
        body = build_body(VARIANTS["post-token"], KEYS)
        self.assertIs(body["get_json"], True)

    def test_tokenkey_variant_adds_client_name(self):
        """The token-key variants mirror refresh_oidc_token()'s query parameters."""
        params = build_params(VARIANTS["get-tokenkey"], KEYS, "thekey")
        self.assertEqual(params["client_name"], "pilot_server")
        self.assertEqual(params["token_key"], "thekey")

    def test_plain_variant_has_no_auth_params(self):
        """The no-token variants send neither client_name nor token_key."""
        params = build_params(VARIANTS["get-plain"], KEYS, "thekey")
        self.assertNotIn("client_name", params)
        self.assertNotIn("token_key", params)


class TestMatrix(unittest.TestCase):
    """Tests for the shape of the probe matrix itself."""

    def test_gzip_dimension_is_covered(self):
        """Both gzip settings are probed, so gzip can be ruled in or out."""
        compressed = {spec.compressed for spec in VARIANTS.values() if spec.style == "json"}
        self.assertEqual(compressed, {True, False})

    def test_secrets_endpoint_is_probed(self):
        """The secrets endpoint under the new API is get_user_secrets."""
        self.assertIn("api/v1/creds/get_user_secrets", ENDPOINTS)

    def test_token_endpoint_is_not_probed(self):
        """get_access_token serves tokens, not secrets, so it is not probed."""
        self.assertNotIn("api/v1/creds/get_access_token", ENDPOINTS)

    def test_get_variants_carry_no_body(self):
        """No GET variant may define a JSON body: request2() raises on that."""
        for name, spec in VARIANTS.items():
            if spec.method == "GET":
                self.assertIsNone(build_body(spec, KEYS), f"{name} must not carry a body")


class TestPrerequisites(unittest.TestCase):
    """Tests for check_prerequisites()."""

    def test_plain_variant_needs_nothing(self):
        """A no-token variant can always run."""
        self.assertEqual(check_prerequisites(VARIANTS["get-plain"], "", ""), "")

    def test_token_variant_needs_token(self):
        """A token variant is skipped when no token is available."""
        self.assertIn("token", check_prerequisites(VARIANTS["get-token"], "", ""))

    def test_tokenkey_variant_needs_key(self):
        """A token-key variant is skipped when the key is missing."""
        reason = check_prerequisites(VARIANTS["get-tokenkey"], "tok", "")
        self.assertIn("token key", reason)

    def test_satisfied_prerequisites(self):
        """A token-key variant runs when both token and key are present."""
        self.assertEqual(check_prerequisites(VARIANTS["get-tokenkey"], "tok", "key"), "")


class TestRunProbeIsHarmless(unittest.TestCase):
    """The probe runs in every pilot, so it must never affect one."""

    def setUp(self):
        """Reset the run-once guard so each test starts from a clean state."""
        credsprobe._PROBE_HAS_RUN = False  # pylint: disable=protected-access

    def tearDown(self):
        """Leave the guard reset for any later test in the same process."""
        credsprobe._PROBE_HAS_RUN = False  # pylint: disable=protected-access

    def test_swallows_exceptions(self):
        """An args object missing the expected fields must not raise."""
        self.assertIsNone(run_probe(object()))

    def test_runs_only_once_per_process(self):
        """A second call is a no-op, so a multi-job pilot probes once."""
        calls = []

        def fake_matrix(*_args, **_kwargs):
            calls.append(1)
            return [("endpoint", "variant", False, "no")]

        original = credsprobe.run_matrix
        credsprobe.run_matrix = fake_matrix
        try:
            args = SimpleNamespace(url="https://example.org", port=25443)
            run_probe(args)
            run_probe(args)
        finally:
            credsprobe.run_matrix = original

        self.assertEqual(len(calls), 1)


# The value used throughout the redaction tests. If this string ever appears
# verbatim in a log line, a real password would have leaked in production.
SECRET_PW = "s3cret-pw-value"


class TestRedaction(unittest.TestCase):
    """pilotlog.txt is uploaded, so no response value may reach it unmasked."""

    def test_mask_keeps_short_prefix(self):
        """Masking leaves a short prefix, enough to recognise the value."""
        self.assertEqual(mask_secret("abcdefgh"), "ab......")

    def test_mask_hides_short_values_entirely(self):
        """A value too short for a safe prefix is masked completely."""
        self.assertEqual(mask_secret("ab"), "......")
        self.assertEqual(mask_secret("abc"), "......")

    def test_flat_password_is_masked(self):
        """A password directly in 'data' does not survive into the preview."""
        payload = {"success": True, "data": {"MB_USERNAME": "u", "MB_PASSWORD": SECRET_PW}}
        preview = safe_preview(payload, KEYS)
        self.assertNotIn(SECRET_PW, preview)
        self.assertIn("MB_PASSWORD", preview)

    def test_nested_password_is_masked(self):
        """A password under 'data.secrets' does not survive either."""
        payload = {"success": True, "data": {"secrets": {"MB_PASSWORD": SECRET_PW}}}
        self.assertNotIn(SECRET_PW, safe_preview(payload, KEYS))

    def test_json_encoded_data_is_masked(self):
        """A password inside a JSON-encoded 'data' string is decoded and masked."""
        payload = {"success": True, "data": '{"MB_PASSWORD": "%s"}' % SECRET_PW}
        self.assertNotIn(SECRET_PW, safe_preview(payload, KEYS))

    def test_scalars_inside_secret_container_are_masked(self):
        """Every scalar under a secret-named container is masked, whatever its key."""
        payload = {"success": True, "data": {"credentials": ["bobuser", SECRET_PW]}}
        self.assertNotIn(SECRET_PW, safe_preview(payload, KEYS))

    def test_token_fields_are_masked(self):
        """Fields named after tokens are masked as well as passwords."""
        payload = {"success": True, "data": {"access_token": SECRET_PW}}
        self.assertNotIn(SECRET_PW, safe_preview(payload, KEYS))

    def test_key_names_survive(self):
        """Masking preserves structure: only values are hidden."""
        payload = {"success": True, "message": "OK", "data": {"MB_USERNAME": "pilotmb"}}
        preview = safe_preview(payload, KEYS)
        for expected in ("success", "message", "OK", "data", "MB_USERNAME"):
            self.assertIn(expected, preview)

    def test_scrub_text_handles_unparseable_response(self):
        """A non-JSON response is scrubbed rather than logged verbatim."""
        text = '<html>{"MB_PASSWORD": "%s"}</html>' % SECRET_PW
        self.assertNotIn(SECRET_PW, scrub_text(text))

    def test_scrub_text_handles_query_string_form(self):
        """A query-string echo is scrubbed too."""
        self.assertNotIn("hunter2", scrub_text("MB_PASSWORD=hunter2&other=fine"))

    def test_custom_password_key_is_masked(self):
        """A password field whose name contains no marker is still masked."""
        payload = {"success": True, "data": {"USER": "u", "PHRASE": SECRET_PW}}
        self.assertNotIn(SECRET_PW, safe_preview(payload, "USER,PHRASE"))

    def test_redact_does_not_mutate_input(self):
        """Redaction returns a copy: the caller still has the real values."""
        data = {"MB_PASSWORD": SECRET_PW}
        redact(data, "MB_PASSWORD")
        self.assertEqual(data["MB_PASSWORD"], SECRET_PW)

    def test_parse_error_does_not_leak_envelope(self):
        """The 'no secrets found' error masks the envelope it quotes."""
        # secrets present at the top level, with an unusable 'data' field:
        # this is the shape that makes the error message quote the envelope
        payload = {"success": True, "data": None, "MB_PASSWORD": SECRET_PW}
        with self.assertRaises(ValueError) as ctx:
            parse_credentials_response(payload, keys=KEYS)
        self.assertNotIn(SECRET_PW, str(ctx.exception))


class TestNothingLeaksToTheLog(unittest.TestCase):
    """End-to-end check on the log records the probe actually emits."""

    def test_successful_probe_logs_no_password(self):
        """A successful variant logs the response without the password in it."""
        payload = {"success": True, "message": "OK",
                   "data": {"MB_USERNAME": "pilotmb", "MB_PASSWORD": SECRET_PW}}

        def fake_send(*_args, **_kwargs):
            return True, payload

        original = credsprobe.send
        credsprobe.send = fake_send
        try:
            with self.assertLogs(credsprobe.logger, level=logging.DEBUG) as captured:
                success, summary = credsprobe.probe_variant(
                    "get-token", "https://example.org", 25443,
                    endpoint=ENDPOINTS[0], token="tok", token_key="key"
                )
        finally:
            credsprobe.send = original

        self.assertTrue(success)
        self.assertEqual(summary, "OK")
        for record in captured.output:
            self.assertNotIn(SECRET_PW, record)

    def test_failed_parse_logs_no_password(self):
        """A response the parser rejects is still scrubbed before logging."""
        # 'data' is unusable, so the parser quotes the envelope in its error
        payload = {"success": True, "data": None, "MB_PASSWORD": SECRET_PW}

        def fake_send(*_args, **_kwargs):
            return True, payload

        original = credsprobe.send
        credsprobe.send = fake_send
        try:
            with self.assertLogs(credsprobe.logger, level=logging.DEBUG) as captured:
                success, summary = credsprobe.probe_variant(
                    "get-token", "https://example.org", 25443,
                    endpoint=ENDPOINTS[0], token="tok", token_key="key"
                )
        finally:
            credsprobe.send = original

        self.assertFalse(success)
        self.assertNotIn(SECRET_PW, summary)
        for record in captured.output:
            self.assertNotIn(SECRET_PW, record)


if __name__ == "__main__":
    unittest.main()
