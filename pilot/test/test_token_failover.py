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

"""Regression tests for the OIDC token failover and lifecycle (PanDA ID 7367714395).

In that job, the token downloaded at pilot startup had 891 s left (issued 2026-10-13 12:31:48 UTC,
expiring 2026-10-17 12:31:48 UTC). The pilot took a job anyway; every update_job after the token
expired was rejected with ``HTTP Error 403: Forbidden ... ERROR : Token authentication failed.
Invalid token. Signature has expired``, the curl fallback resent the same token, the hourly refresh
was rejected as well since it authenticated with the expired token, and the pilot exited 0 although
the server never received the final update.
"""

import base64
import json
import os
import tempfile
import time
import unittest
import urllib.error
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.control import job as job_module
from pilot.util import https, tokenlifetime
from pilot.util.auxiliary import convert_to_pilot_error_code, shell_exit_code

# server response body observed for PanDA ID 7367714395
EXPIRED_BODY = "ERROR : Token authentication failed. Invalid token. Signature has expired"
URL = "https://aipanda092.cern.ch:25443/api/v1/pilot/update_job"
OK_BODY = b'{"success": true, "message": "", "data": {"StatusCode": 0}}'
errors = ErrorCodes()


def make_jwt(exp: float) -> str:
    """Build an unsigned JWT carrying the given expiry.

    Args:
        exp: Expiry (epoch seconds).

    Returns:
        The token string.
    """
    def _segment(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{_segment({'alg': 'RS256'})}.{_segment({'sub': 'pilot', 'exp': int(exp)})}.signature"


def http_error(code: int, body: str) -> urllib.error.HTTPError:
    """Build an HTTPError with a readable body.

    Args:
        code: HTTP status code.
        body: Response body.

    Returns:
        The exception.
    """
    return urllib.error.HTTPError(URL, code, "Forbidden", {}, BytesIO(body.encode()))


def ok_response(body: bytes = OK_BODY) -> MagicMock:
    """Build a context-manager response object as returned by urlopen().

    Args:
        body: Response body.

    Returns:
        The mocked response context manager.
    """
    response = MagicMock(status=200, reason="OK")
    response.read.return_value = body
    manager = MagicMock()
    manager.__enter__.return_value = response
    return manager


def reset_token_state() -> None:
    """Reset the token-related pilot cache fields to their defaults."""
    cache = get_pilot_cache()
    cache.token_x509_mode = False
    cache.token_rejected = False
    cache.token_next_refresh = 0
    cache.token_failed_final_updates = set()
    cache.proxy_validity_end = 0


class TokenTestCase(unittest.TestCase):
    """Base class: resets the singletons and provides a proxy file."""

    def setUp(self):
        """Reset the pilot cache and error codes, and create a proxy file."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []
        reset_token_state()
        handle, self.proxy = tempfile.mkstemp(prefix="x509up_u")
        os.close(handle)
        self.patches = [
            patch.object(https._ctx, "cacert", self.proxy, create=True),
            patch.object(https._ctx, "capath", "/etc/grid-security/certificates", create=True),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        """Stop the patches, remove the proxy file and reset the singletons."""
        for item in self.patches:
            item.stop()
        os.remove(self.proxy)
        reset_token_state()
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []


class TestTokenAuthFailure(unittest.TestCase):
    """Classification of HTTP errors as authentication failures."""

    def test_observed_expired_token_is_auth_failure(self):
        """The 403 observed for PanDA ID 7367714395 is an authentication failure."""
        self.assertTrue(https.is_token_auth_failure(403, EXPIRED_BODY))

    def test_401_is_auth_failure(self):
        """A 401 is always an authentication failure."""
        self.assertTrue(https.is_token_auth_failure(401, ""))

    def test_unrelated_403_is_not_auth_failure(self):
        """A 403 that does not mention the token is not treated as an authentication failure."""
        self.assertFalse(https.is_token_auth_failure(403, "ERROR : job is not allowed"))
        self.assertFalse(https.is_token_auth_failure(403, ""))
        self.assertFalse(https.is_token_auth_failure(403, None))

    def test_server_error_is_not_auth_failure(self):
        """A 5xx is not an authentication failure, even when the body mentions a token."""
        self.assertFalse(https.is_token_auth_failure(500, EXPIRED_BODY))

    def test_is_token_rejection(self):
        """Only the marked error string is a token rejection."""
        self.assertTrue(https.is_token_rejection(f"failed to send request: HTTP Error 403: Forbidden ({https.TOKEN_REJECTED})"))
        self.assertFalse(https.is_token_rejection("failed to send request: HTTP Error 403: Forbidden"))
        self.assertFalse(https.is_token_rejection({"success": True}))
        self.assertFalse(https.is_token_rejection(None))


class TestX509FailoverAvailable(TokenTestCase):
    """Availability of the X.509 proxy for a failover."""

    def test_existing_proxy_without_known_lifetime(self):
        """An existing, unverified proxy is assumed to be usable."""
        self.assertTrue(https.x509_failover_available(1920))

    def test_proxy_lifetime_is_respected(self):
        """A verified proxy must outlast the requested lifetime."""
        get_pilot_cache().proxy_validity_end = int(time.time()) + 1000
        self.assertTrue(https.x509_failover_available(900))
        self.assertFalse(https.x509_failover_available(1920))

    def test_missing_proxy(self):
        """A missing proxy file means no failover."""
        with patch.object(https._ctx, "cacert", "/nonexistent/x509up", create=True):
            self.assertFalse(https.x509_failover_available())
        with patch.object(https._ctx, "cacert", None, create=True):
            self.assertFalse(https.x509_failover_available())


class TestRequest2Failover(TokenTestCase):
    """request2() retries with X.509 when the server rejects the token."""

    def _request2(self, side_effect: list):
        """Call request2() for update_job with a token configured and urlopen mocked.

        Args:
            side_effect: Side effect list for urlopen.

        Returns:
            (response, urlopen mock, ssl context mock).
        """
        ssl_context = MagicMock()
        urlopen = MagicMock(side_effect=side_effect)
        with patch.object(https, "get_local_oidc_token_info", return_value=("panda_token", "atlas.pilot")), \
                patch.object(https, "get_auth_token_content", return_value="expired-token"), \
                patch.object(https, "_get_ssl_context", return_value=ssl_context), \
                patch.object(https.urllib.request, "urlopen", urlopen):
            res = https.request2(URL, json_body={"job_id": 7367714395, "job_status": "running"}, panda=True)
        return res, urlopen, ssl_context

    def test_x509_retry_succeeds(self):
        """The rejected update is resent with the proxy and without the token, and X.509 becomes sticky."""
        res, urlopen, ssl_context = self._request2([http_error(403, EXPIRED_BODY), ok_response()])
        self.assertEqual(res["success"], True)
        self.assertEqual(urlopen.call_count, 2)
        first, retry = urlopen.call_args_list[0][0][0], urlopen.call_args_list[1][0][0]
        self.assertEqual(first.get_header("Authorization"), "Bearer expired-token")
        self.assertIsNone(retry.get_header("Authorization"))
        self.assertIsNone(retry.get_header("Origin"))
        self.assertEqual(retry.data, first.data)
        self.assertEqual(retry.get_method(), "POST")
        ssl_context.load_cert_chain.assert_called_once_with(certfile=self.proxy, keyfile=self.proxy)
        self.assertTrue(get_pilot_cache().token_x509_mode)
        self.assertFalse(get_pilot_cache().token_rejected)

    def test_x509_retry_fails(self):
        """If X.509 is rejected as well, the rejection is reported and recorded."""
        res, urlopen, _ = self._request2([http_error(403, EXPIRED_BODY), http_error(403, "ERROR : X.509 not accepted")])
        self.assertTrue(https.is_token_rejection(res))
        self.assertEqual(urlopen.call_count, 2)
        self.assertFalse(get_pilot_cache().token_x509_mode)
        self.assertTrue(get_pilot_cache().token_rejected)

    def test_no_proxy_no_retry(self):
        """Without a proxy, the rejection is reported without a retry."""
        with patch.object(https._ctx, "cacert", None, create=True), \
                self.assertLogs(https.logger, level="WARNING") as logs:
            res, urlopen, _ = self._request2([http_error(403, EXPIRED_BODY)])
        self.assertIn("no X.509 proxy is available for a failover", "\n".join(logs.output))
        self.assertTrue(https.is_token_rejection(res))
        self.assertEqual(urlopen.call_count, 1)
        self.assertTrue(get_pilot_cache().token_rejected)

    def test_unrelated_403_is_not_retried(self):
        """A 403 for another reason is returned as before, without failover or rejection marker."""
        res, urlopen, _ = self._request2([http_error(403, "ERROR : not allowed")])
        self.assertTrue(res.startswith("failed to send request: HTTP Error 403"))
        self.assertFalse(https.is_token_rejection(res))
        self.assertEqual(urlopen.call_count, 1)
        self.assertFalse(get_pilot_cache().token_rejected)

    def test_sticky_x509_mode(self):
        """After a successful failover, requests go straight to X.509."""
        get_pilot_cache().token_x509_mode = True
        res, urlopen, ssl_context = self._request2([ok_response()])
        self.assertEqual(res["success"], True)
        self.assertIsNone(urlopen.call_args[0][0].get_header("Authorization"))
        ssl_context.load_cert_chain.assert_called_once_with(certfile=self.proxy, keyfile=self.proxy)

    def test_sticky_x509_rejected_reverts_to_token(self):
        """If X.509 is rejected in sticky mode, the next request tries the token again."""
        get_pilot_cache().token_x509_mode = True
        res, urlopen, _ = self._request2([http_error(401, "")])
        self.assertTrue(https.is_token_rejection(res))
        self.assertEqual(urlopen.call_count, 1)
        self.assertFalse(get_pilot_cache().token_x509_mode)
        self.assertTrue(get_pilot_cache().token_rejected)

    def test_success_clears_rejection(self):
        """An accepted request clears an earlier rejection."""
        get_pilot_cache().token_rejected = True
        self._request2([ok_response()])
        self.assertFalse(get_pilot_cache().token_rejected)

    def test_no_token_configured_is_unchanged(self):
        """Without a token, a 403 is not handled as a token rejection."""
        with patch.object(https, "get_local_oidc_token_info", return_value=(None, None)), \
                patch.object(https, "_get_ssl_context", return_value=MagicMock()), \
                patch.object(https.urllib.request, "urlopen", MagicMock(side_effect=[http_error(403, EXPIRED_BODY)])):
            res = https.request2(URL, json_body={"job_id": 1}, panda=True)
        self.assertFalse(https.is_token_rejection(res))


class TestSendRequestSkipsCurl(TokenTestCase):
    """send_request() does not resend a rejected token through the curl fallback."""

    def test_curl_skipped_on_token_rejection(self):
        """A rejected token returns None without calling curl."""
        rejected = f"failed to send request: HTTP Error 403: Forbidden ({https.TOKEN_REJECTED})"
        with patch.object(https, "request2", return_value=rejected), \
                patch.object(https, "request") as curl:
            res = https.send_request("https://pandaserver.cern.ch:25443", "api/v1/pilot/update_job", {}, None, "IPv6")
        self.assertIsNone(res)
        curl.assert_not_called()

    def test_curl_used_on_transport_failure(self):
        """Other request2() failures still fall back to curl."""
        with patch.object(https, "request2", return_value="failed to send request: timed out"), \
                patch.object(https, "request", return_value={"success": True}) as curl:
            res = https.send_request("https://pandaserver.cern.ch:25443", "api/v1/pilot/update_job", {}, None, "IPv6")
        self.assertEqual(res, {"success": True})
        curl.assert_called_once()


class TestSendX509Request(TokenTestCase):
    """send_x509_request() authenticates with the proxy only."""

    def test_token_headers_removed_case_insensitively(self):
        """Authorization and Origin are removed whatever their capitalisation."""
        urlopen = MagicMock(return_value=ok_response(b"{}"))
        with patch.object(https, "_get_ssl_context", return_value=MagicMock()), \
                patch.object(https.urllib.request, "urlopen", urlopen):
            ok, _ = https.send_x509_request(URL, b"{}", {"authorization": "Bearer x", "ORIGIN": "atlas.pilot",
                                                         "User-agent": "pilot/3"}, "POST")
        self.assertTrue(ok)
        req = urlopen.call_args[0][0]
        self.assertIsNone(req.get_header("Authorization"))
        self.assertIsNone(req.get_header("Origin"))
        self.assertEqual(req.get_header("User-agent"), "pilot/3")

    def test_http_error_is_reported(self):
        """An HTTP error is returned as a failure with its code."""
        with patch.object(https, "_get_ssl_context", return_value=MagicMock()), \
                patch.object(https.urllib.request, "urlopen", MagicMock(side_effect=http_error(403, "denied"))):
            ok, text = https.send_x509_request(URL)
        self.assertFalse(ok)
        self.assertIn("HTTP Error 403", text)

    def test_unloadable_proxy(self):
        """A proxy that cannot be loaded is a failure, not an exception."""
        context = MagicMock()
        context.load_cert_chain.side_effect = OSError("bad proxy")
        with patch.object(https, "_get_ssl_context", return_value=context):
            ok, text = https.send_x509_request(URL)
        self.assertFalse(ok)
        self.assertIn("bad proxy", text)

    def test_no_proxy(self):
        """Without a proxy, nothing is sent."""
        with patch.object(https._ctx, "cacert", None, create=True):
            ok, _ = https.send_x509_request(URL)
        self.assertFalse(ok)


class TestRefreshWithX509(TokenTestCase):
    """The token refresh falls back to X.509 when the token cannot authenticate it."""

    def _refresh(self, token_download: str, x509_result: tuple):
        """Run refresh_oidc_token() with the downloads mocked.

        Args:
            token_download: Return value of the token-authenticated download.
            x509_result: Return value of send_x509_request().

        Returns:
            (status, handle_file_content mock, send_x509_request mock).
        """
        with patch.object(https, "get_token_refresh_command", return_value="https://server/api/v1/creds/get_access_token?x"), \
                patch.object(https, "get_auth_token_content", return_value="expired-token"), \
                patch.object(https, "download_file", return_value=token_download), \
                patch.object(https, "send_x509_request", return_value=x509_result) as x509, \
                patch.object(https, "handle_file_content", return_value=True) as handle:
            status = https.refresh_oidc_token("panda_token", "atlas.pilot", "https://server", 25443)
        return status, handle, x509

    def test_expired_token_refreshed_with_x509(self):
        """The 403 on the refresh (13:17:02 for PanDA ID 7367714395) is followed by an X.509 download."""
        status, handle, x509 = self._refresh("", (True, '{"success": true, "data": {"access_token": "new"}}'))
        self.assertTrue(status)
        x509.assert_called_once()
        self.assertNotIn("Authorization", x509.call_args[1]["headers"])
        handle.assert_called_once_with('{"success": true, "data": {"access_token": "new"}}', "panda_token")

    def test_token_refresh_does_not_use_x509(self):
        """A successful token-authenticated refresh does not use X.509."""
        status, handle, x509 = self._refresh('{"data": {}}', (True, ""))
        self.assertTrue(status)
        x509.assert_not_called()
        handle.assert_called_once()

    def test_both_fail(self):
        """If both downloads fail, the refresh fails."""
        status, handle, _ = self._refresh("", (False, "HTTP Error 403"))
        self.assertFalse(status)
        handle.assert_not_called()

    def test_no_token_key(self):
        """Without a token key, nothing is downloaded."""
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PANDA_AUTH_TOKEN_KEY", None)
            self.assertEqual(https.get_token_refresh_command("https://server", 25443), "")
            with patch.object(https, "download_file") as download:
                self.assertFalse(https.refresh_oidc_token("panda_token", "atlas.pilot", "https://server", 25443))
            download.assert_not_called()


class TestTokenExpiry(unittest.TestCase):
    """get_token_expiry() reads the exp claim quietly."""

    def test_expiry_read_from_file(self):
        """The exp claim of the token file is returned."""
        with tempfile.NamedTemporaryFile("w", suffix="panda_token", delete=False) as handle:
            handle.write(make_jwt(1792240308))  # 2026-10-17 12:31:48 UTC (PanDA ID 7367714395)
        try:
            with patch.object(https, "get_local_oidc_token_info", return_value=(handle.name, "atlas.pilot")), \
                    patch.object(https, "locate_token", return_value=handle.name):
                self.assertEqual(https.get_token_expiry(), 1792240308)
        finally:
            os.remove(handle.name)

    def test_no_token(self):
        """Without a token, the expiry is unknown."""
        with patch.object(https, "get_local_oidc_token_info", return_value=(None, None)):
            self.assertIsNone(https.get_token_expiry())

    def test_unreadable_token(self):
        """An undecodable token has no known expiry."""
        with patch.object(https, "get_local_oidc_token_info", return_value=("panda_token", "atlas.pilot")), \
                patch.object(https, "locate_token", return_value="not-a-jwt"):
            self.assertIsNone(https.get_token_expiry())

    def test_update_local_oidc_token_info_reports_status(self):
        """The refresh status is returned."""
        with patch.object(https, "get_local_oidc_token_info", return_value=(None, None)):
            self.assertFalse(https.update_local_oidc_token_info("https://server", 25443))
        with patch.object(https, "get_local_oidc_token_info", return_value=("panda_token", "atlas.pilot")), \
                patch.object(https, "refresh_oidc_token", return_value=False):
            self.assertFalse(https.update_local_oidc_token_info("https://server", 25443))
        with patch.object(https, "get_local_oidc_token_info", return_value=("panda_token", "atlas.pilot")), \
                patch.object(https, "refresh_oidc_token", return_value=True), \
                patch.object(https, "locate_token", return_value=""), \
                patch.object(https, "decode_jwt_payload", side_effect=ValueError("no token")):
            self.assertTrue(https.update_local_oidc_token_info("https://server", 25443))


class TestRefreshSchedule(unittest.TestCase):
    """The refresh is scheduled from the token's expiry."""

    def setUp(self):
        """Reset the token state."""
        reset_token_state()

    def tearDown(self):
        """Reset the token state."""
        reset_token_state()

    def test_unknown_expiry_uses_interval(self):
        """Without a known expiry, the hourly schedule is kept."""
        self.assertEqual(tokenlifetime.compute_next_refresh(1000, None, 3600, 900, 300), 4600)

    def test_long_lived_token_uses_interval(self):
        """A token that lives longer than interval + margin is refreshed after the interval."""
        self.assertEqual(tokenlifetime.compute_next_refresh(1000, 1000 + 4 * 86400, 3600, 900, 300), 4600)

    def test_short_token_refreshed_before_expiry(self):
        """A token that expires within interval + margin is refreshed margin seconds before expiry."""
        self.assertEqual(tokenlifetime.compute_next_refresh(1000, 1000 + 2000, 3600, 900, 300), 1000 + 1100)

    def test_boundary_between_interval_and_deadline(self):
        """At exactly interval + margin the two rules agree."""
        self.assertEqual(tokenlifetime.compute_next_refresh(0, 4500, 3600, 900, 300), 3600)
        self.assertEqual(tokenlifetime.compute_next_refresh(0, 4499, 3600, 900, 300), 3599)

    def test_observed_891_s_token_is_retried(self):
        """The token with 891 s left (PanDA ID 7367714395) is retried after the retry interval."""
        self.assertEqual(tokenlifetime.compute_next_refresh(1000, 1000 + 891, 3600, 900, 300), 1300)

    def test_deadline_exactly_now_is_retried(self):
        """A token expiring exactly margin seconds from now is retried."""
        self.assertEqual(tokenlifetime.compute_next_refresh(1000, 1900, 3600, 900, 300), 1300)

    def test_expired_token_is_retried(self):
        """An expired token is retried after the retry interval."""
        self.assertEqual(tokenlifetime.compute_next_refresh(1000, 500, 3600, 900, 300), 1300)

    def test_schedule_is_stored(self):
        """The schedule is stored in the pilot cache."""
        next_refresh = tokenlifetime.schedule_next_refresh(10000 + 891, now=10000)
        self.assertEqual(next_refresh, 10300)
        self.assertEqual(get_pilot_cache().token_next_refresh, 10300)

    def test_missing_config_uses_defaults(self):
        """Missing [Token] settings fall back to the defaults."""
        with patch.object(tokenlifetime, "config", SimpleNamespace(Token=SimpleNamespace())):
            self.assertEqual(tokenlifetime.schedule_next_refresh(None, now=0), tokenlifetime.DEFAULT_DOWNLOAD_CHECK)
            self.assertEqual(tokenlifetime.schedule_next_refresh(100, now=0), tokenlifetime.DEFAULT_REFRESH_RETRY)

    def test_is_refresh_due_follows_schedule(self):
        """Once scheduled, the schedule decides, not the elapsed time."""
        get_pilot_cache().token_next_refresh = 2000
        self.assertFalse(tokenlifetime.is_refresh_due(0, 3600, now=1999))
        self.assertTrue(tokenlifetime.is_refresh_due(0, 3600, now=2000))
        self.assertTrue(tokenlifetime.is_refresh_due(1990, 3600, now=2001))

    def test_is_refresh_due_fallback(self):
        """Without a schedule, the fixed interval is used."""
        self.assertFalse(tokenlifetime.is_refresh_due(0, 3600, now=3600))
        self.assertTrue(tokenlifetime.is_refresh_due(0, 3600, now=3601))


class TestRefreshToken(TokenTestCase):
    """refresh_token() wraps the refresh and reschedules."""

    def _refresh(self, expiries: list, status: object = True):
        """Run refresh_token() with the expiry before and after the refresh mocked.

        Args:
            expiries (list): Expiry before and after the refresh.
            status (bool | Exception): Return value of update_local_oidc_token_info(), or an exception to raise.

        Returns:
            (status, captured log output).
        """
        side_effect = status if isinstance(status, Exception) else None
        with patch.object(tokenlifetime, "get_token_expiry", side_effect=expiries), \
                patch.object(tokenlifetime, "update_local_oidc_token_info", return_value=status,
                             side_effect=side_effect), \
                self.assertLogs(tokenlifetime.logger, level="INFO") as logs:
            result = tokenlifetime.refresh_token("https://server", 25443)
        return result, "\n".join(logs.output)

    def test_unchanged_token_is_reported_and_retried(self):
        """The server returning the same short-lived token is reported and retried soon."""
        expiry = int(time.time()) + 891
        result, output = self._refresh([expiry, expiry])
        self.assertTrue(result)
        self.assertIn("did not return a newer token", output)
        self.assertLessEqual(get_pilot_cache().token_next_refresh - time.time(), tokenlifetime.DEFAULT_REFRESH_RETRY + 1)

    def test_newer_token_ends_x509_mode(self):
        """A newer token switches back from X.509 to token authentication."""
        get_pilot_cache().token_x509_mode = True
        now = int(time.time())
        result, output = self._refresh([now - 100, now + 4 * 86400])
        self.assertTrue(result)
        self.assertFalse(get_pilot_cache().token_x509_mode)
        self.assertIn("will use token authentication again", output)

    def test_failed_refresh_keeps_x509_mode(self):
        """A failed refresh keeps X.509 and is still rescheduled."""
        get_pilot_cache().token_x509_mode = True
        now = int(time.time())
        result, _ = self._refresh([now - 100, now - 100], status=False)
        self.assertFalse(result)
        self.assertTrue(get_pilot_cache().token_x509_mode)
        self.assertGreater(get_pilot_cache().token_next_refresh, now)

    def test_exception_is_contained_and_rescheduled(self):
        """An exception does not escape, and the refresh is still rescheduled."""
        result, output = self._refresh([None, None], status=RuntimeError("boom"))
        self.assertFalse(result)
        self.assertIn("boom", output)
        self.assertGreater(get_pilot_cache().token_next_refresh, time.time())


class TestCheckTokenForNewJob(TokenTestCase):
    """The token must outlast the next heartbeat before a job is fetched."""

    def _check(self, lifetimes: list, renewal_allowed: bool = True, x509: bool = False):
        """Run check_token_for_new_job() with the remaining lifetime mocked.

        Args:
            lifetimes: Remaining lifetime before (and after) a refresh.
            renewal_allowed: Passed on.
            x509: Whether X.509 failover is available.

        Returns:
            (proceed, diagnostics, refresh mock).
        """
        with patch.object(tokenlifetime, "get_token_remaining_lifetime", side_effect=lifetimes), \
                patch.object(tokenlifetime, "refresh_token") as refresh, \
                patch.object(tokenlifetime, "x509_failover_available", return_value=x509):
            proceed, diagnostics = tokenlifetime.check_token_for_new_job("https://server", 25443, renewal_allowed)
        return proceed, diagnostics, refresh

    def test_required_lifetime(self):
        """The token must outlast the heartbeat plus grace."""
        self.assertEqual(tokenlifetime.get_required_token_lifetime(), 1920)

    def test_no_token(self):
        """Without a token, the check does not apply."""
        proceed, _, refresh = self._check([None])
        self.assertTrue(proceed)
        refresh.assert_not_called()

    def test_long_token(self):
        """A long-lived token is accepted without refresh."""
        proceed, _, refresh = self._check([4 * 86400])
        self.assertTrue(proceed)
        refresh.assert_not_called()

    def test_observed_891_s_token_without_failover_is_refused(self):
        """The situation of PanDA ID 7367714395 without failover: no job is fetched."""
        proceed, diagnostics, refresh = self._check([891, 885])
        self.assertFalse(proceed)
        refresh.assert_called_once()
        self.assertIn("885 s", diagnostics)
        self.assertIn("no X.509 failover", diagnostics)

    def test_observed_891_s_token_with_failover_proceeds(self):
        """With X.509 failover available, the pilot proceeds (with a warning)."""
        proceed, _, _ = self._check([891, 885], x509=True)
        self.assertTrue(proceed)

    def test_refresh_rescues_short_token(self):
        """A refresh that delivers a long-lived token lets the pilot proceed."""
        proceed, _, refresh = self._check([891, 4 * 86400])
        self.assertTrue(proceed)
        refresh.assert_called_once()

    def test_no_refresh_when_renewal_disabled(self):
        """With token renewal disabled, no refresh is attempted."""
        proceed, _, refresh = self._check([891], renewal_allowed=False)
        self.assertFalse(proceed)
        refresh.assert_not_called()

    def test_threshold_is_inclusive(self):
        """A token lasting exactly the required lifetime is accepted."""
        proceed, _, _ = self._check([tokenlifetime.get_required_token_lifetime()])
        self.assertTrue(proceed)
        proceed, _, _ = self._check([tokenlifetime.get_required_token_lifetime() - 1, 0])
        self.assertFalse(proceed)

    def test_failover_needs_proxy_for_required_lifetime(self):
        """The X.509 failover must cover the required lifetime."""
        with patch.object(tokenlifetime, "get_token_remaining_lifetime", side_effect=[891, 891]), \
                patch.object(tokenlifetime, "refresh_token"), \
                patch.object(tokenlifetime, "x509_failover_available", return_value=False) as available:
            tokenlifetime.check_token_for_new_job("https://server", 25443, True)
        available.assert_called_once_with(tokenlifetime.get_required_token_lifetime())


class TestTokenExitCode(TokenTestCase):
    """A final update lost to a rejected token ends with TOKENTOOSHORT."""

    def test_batch_exit_code(self):
        """TOKENTOOSHORT maps to batch exit code 85 and back."""
        self.assertEqual(errors.TOKENTOOSHORT, 1394)
        self.assertEqual(shell_exit_code(errors.TOKENTOOSHORT), 85)
        self.assertEqual(convert_to_pilot_error_code(85), errors.TOKENTOOSHORT)
        self.assertEqual(errors.get_error_message(errors.TOKENTOOSHORT), "OIDC token lifetime too short for server updates")

    def test_final_update_lost_to_token(self):
        """The final update of PanDA ID 7367714395 was rejected - exit code 1394 instead of 0."""
        get_pilot_cache().token_rejected = True
        tokenlifetime.record_final_update("7367714395", False)
        self.assertEqual(tokenlifetime.apply_token_exit_code(0), errors.TOKENTOOSHORT)

    def test_final_update_failed_for_other_reason(self):
        """A final update that failed for another reason does not change the exit code."""
        tokenlifetime.record_final_update("7367714395", False)
        self.assertEqual(tokenlifetime.apply_token_exit_code(0), 0)

    def test_later_success_clears(self):
        """A later accepted final update for the same job clears the failure."""
        get_pilot_cache().token_rejected = True
        tokenlifetime.record_final_update("7367714395", False)
        tokenlifetime.record_final_update("7367714395", True)
        self.assertEqual(tokenlifetime.apply_token_exit_code(0), 0)

    def test_existing_error_code_is_kept(self):
        """An existing exit code is not overwritten."""
        get_pilot_cache().token_rejected = True
        tokenlifetime.record_final_update("7367714395", False)
        self.assertEqual(tokenlifetime.apply_token_exit_code(errors.STAGEOUTFAILED), errors.STAGEOUTFAILED)


class TestJobControlIntegration(TokenTestCase):
    """The job control uses the token checks."""

    def setUp(self):
        """Save PILOT_WRAP_UP."""
        super().setUp()
        self._saved_wrap_up = os.environ.pop("PILOT_WRAP_UP", None)

    def tearDown(self):
        """Restore PILOT_WRAP_UP."""
        os.environ.pop("PILOT_WRAP_UP", None)
        if self._saved_wrap_up is not None:
            os.environ["PILOT_WRAP_UP"] = self._saved_wrap_up
        super().tearDown()

    def _check(self, jobnumber: int, result: tuple, error_code: int = 0, catchall: str = ""):
        """Run _check_token_lifetime() with the token decision mocked.

        Args:
            jobnumber: Number of jobs already downloaded.
            result: Return value of check_token_for_new_job().
            error_code: Initial traces error code.
            catchall: PQ.catchall.

        Returns:
            (proceed, traces, check mock).
        """
        traces = MagicMock(pilot={"error_code": error_code})
        args = SimpleNamespace(url="https://pandaserver.cern.ch", port=25443, token_renewal=True)
        with patch.object(job_module, "check_token_for_new_job", return_value=result) as check, \
                patch.object(job_module.infosys, "queuedata", SimpleNamespace(catchall=catchall)):
            proceed = job_module._check_token_lifetime(args, jobnumber, traces)
        return proceed, traces, check

    def test_first_job_refused_sets_error_code(self):
        """A pilot that ends without a job reports TOKENTOOSHORT."""
        proceed, traces, _ = self._check(0, (False, "too short"))
        self.assertFalse(proceed)
        self.assertEqual(traces.pilot["error_code"], errors.TOKENTOOSHORT)
        self.assertEqual(os.environ.get("PILOT_WRAP_UP"), "QUICKLY")

    def test_later_job_refused_without_error_code(self):
        """A multijob pilot declining a further job does not report an error."""
        proceed, traces, _ = self._check(1, (False, "too short"))
        self.assertFalse(proceed)
        self.assertEqual(traces.pilot["error_code"], 0)

    def test_existing_error_code_is_kept(self):
        """An existing error code is not overwritten."""
        _, traces, _ = self._check(0, (False, "too short"), error_code=errors.NOLOCALSPACE)
        self.assertEqual(traces.pilot["error_code"], errors.NOLOCALSPACE)

    def test_accepted(self):
        """An accepted token lets the pilot proceed."""
        proceed, traces, check = self._check(0, (True, ""))
        self.assertTrue(proceed)
        self.assertEqual(traces.pilot["error_code"], 0)
        check.assert_called_once_with("https://pandaserver.cern.ch", 25443, True)

    def test_no_token_renewal_catchall(self):
        """PQ.catchall no_token_renewal disables the refresh in the check."""
        _, _, check = self._check(0, (True, ""), catchall="no_token_renewal")
        check.assert_called_once_with("https://pandaserver.cern.ch", 25443, False)

    def test_send_state_records_lost_final_update(self):
        """send_state() records a final update rejected because of the token."""
        get_pilot_cache().token_rejected = True
        job = MagicMock(jobid="7367714395", state="finished")
        args = SimpleNamespace(update_server=True, pod=False, workflow="generic", url="https://pandaserver.cern.ch",
                               port=25443, internet_protocol_version="IPv6")
        result = https.UpdateResult(ok=False, attempts=2, response=None, success=None, status_code=None,
                                    command=None, message="no response")
        with patch.object(job_module, "get_proper_state", return_value="finished"), \
                patch.object(job_module, "is_final_update", return_value=True), \
                patch.object(job_module, "get_data_structure", return_value={}), \
                patch.object(job_module.config.Pilot, "pandajob", "real", create=True), \
                patch.object(job_module.https, "send_update", return_value=result):
            self.assertFalse(job_module.send_state(job, args, "finished"))
        self.assertEqual(get_pilot_cache().token_failed_final_updates, {"7367714395"})
        self.assertEqual(tokenlifetime.apply_token_exit_code(0), errors.TOKENTOOSHORT)

    def test_getjob_curl_skipped_on_token_rejection(self):
        """getJob does not resend a rejected token through curl."""
        rejected = f"failed to send request: HTTP Error 403: Forbidden ({https.TOKEN_REJECTED})"
        with patch.object(job_module, "get_dispatcher_dictionary", return_value={}), \
                patch.object(job_module.infosys, "queuedata", SimpleNamespace(catchall="")), \
                patch.object(job_module.https, "request2", return_value=rejected), \
                patch.object(job_module.https, "request") as curl:
            res = job_module.get_job_definition_from_server(SimpleNamespace(url="https://pandaserver.cern.ch", port=25443))
        self.assertEqual(res, rejected)
        curl.assert_not_called()


if __name__ == "__main__":
    unittest.main()
