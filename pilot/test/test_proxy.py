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

"""Unit tests for proxy download retry and error-propagation logic.

Covers two changes made in response to the ATLASPANDA ticket on silent proxy
download failures:

1. ``get_proxy()`` in ``pilot/util/proxy.py``:
   Transient network errors (``request2()`` returning a "failed to send
   request:" string) must be retried up to 3 times with a 30-second sleep
   between attempts.  Definitive server-side failures must NOT be retried.

2. ``get_and_verify_proxy()`` in ``pilot/user/atlas/proxy.py``:
   A download failure (``get_proxy()`` returning ``False``) must now
   propagate as ``exit_code = NOPROXY`` rather than returning 0.
"""

import sys
import unittest
from unittest.mock import patch, MagicMock

from pilot.common.errorcodes import ErrorCodes

errors = ErrorCodes()


def _make_user_proxy_module():
    """Return a fake pilot.user.atlas.proxy module for sys.modules injection."""
    mod = MagicMock()
    mod.getproxy_dictionary.return_value = {'role': 'atlas', 'dn': 'atlpilo2'}
    return mod


# ---------------------------------------------------------------------------
# Tests for get_proxy() retry behaviour
# ---------------------------------------------------------------------------

class TestGetProxyRetry(unittest.TestCase):
    """get_proxy() must retry on transient network errors and not on server errors."""

    def _run_get_proxy(self, request2_side_effect, write_file_ok=True):
        """Drive get_proxy() with the given request2 side-effects.

        Args:
            request2_side_effect: Value or list of values returned by successive
                calls to ``https.request2``.
            write_file_ok: Whether the proxy file write should succeed.

        Returns:
            Tuple of (result, path, n_request2_calls, n_sleep_calls).
        """
        from pilot.util.proxy import get_proxy

        if not isinstance(request2_side_effect, list):
            request2_side_effect = [request2_side_effect]

        fake_user_mod = _make_user_proxy_module()
        # Inject the fake module so the bare __import__ call in get_proxy() finds it.
        fake_mod_name = 'pilot.user.atlas.proxy'
        original = sys.modules.get(fake_mod_name)
        sys.modules[fake_mod_name] = fake_user_mod

        try:
            with patch('pilot.util.proxy.https') as mock_https, \
                 patch('pilot.util.proxy.config') as mock_cfg, \
                 patch.dict('os.environ', {'PILOT_USER': 'atlas', 'PANDA_SERVER_URL': 'https://pandaserver.cern.ch'}), \
                 patch('pilot.util.proxy.sleep') as mock_sleep, \
                 patch('pilot.util.proxy.write_file', return_value=write_file_ok), \
                 patch('pilot.util.proxy.os.open', return_value=3), \
                 patch('pilot.util.proxy.os.close'), \
                 patch('pilot.util.proxy.vomsproxyinfo', return_value=(0, '', '')):

                mock_cfg.Pilot.pandaserver = 'https://pandaserver.cern.ch'
                mock_https.request2.side_effect = request2_side_effect

                result, path = get_proxy('/tmp/x509up_u0.proxy', 'atlas')
                return result, path, mock_https.request2.call_count, mock_sleep.call_count
        finally:
            if original is None:
                sys.modules.pop(fake_mod_name, None)
            else:
                sys.modules[fake_mod_name] = original

    # -- Transient failure: all three attempts fail ---------------------------

    def test_all_transient_failures_returns_false(self):
        """Three consecutive transient failures -> get_proxy() returns False."""
        transient = 'failed to send request: <urlopen error [Errno 104] Connection reset by peer>'
        result, _, n_calls, n_sleeps = self._run_get_proxy([transient, transient, transient])
        self.assertFalse(result)
        self.assertEqual(n_calls, 3, 'request2 must be called exactly 3 times')
        self.assertEqual(n_sleeps, 2, 'sleep must be called between attempts (2 gaps for 3 tries)')

    def test_transient_then_success_returns_true(self):
        """One transient failure followed by a successful response -> returns True."""
        transient = 'failed to send request: <urlopen error [Errno 104] Connection reset by peer>'
        good = {'StatusCode': 0, 'userProxy': '-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n'}
        result, _, n_calls, n_sleeps = self._run_get_proxy([transient, good])
        self.assertTrue(result)
        self.assertEqual(n_calls, 2)
        self.assertEqual(n_sleeps, 1)

    # -- Definitive server error: must NOT retry ------------------------------

    def test_server_side_failure_no_retry(self):
        """A definitive server error (StatusCode != 0) must not trigger a retry."""
        server_error = {'StatusCode': 1, 'errorDialog': 'No proxy available'}
        result, _, n_calls, n_sleeps = self._run_get_proxy([server_error])
        self.assertFalse(result)
        self.assertEqual(n_calls, 1, 'request2 must be called only once for a server-side failure')
        self.assertEqual(n_sleeps, 0, 'no sleep should occur for a definitive failure')

    def test_success_first_attempt_no_sleep(self):
        """Immediate success must not call sleep at all."""
        good = {'StatusCode': 0, 'userProxy': '-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n'}
        result, _, n_calls, n_sleeps = self._run_get_proxy([good])
        self.assertTrue(result)
        self.assertEqual(n_calls, 1)
        self.assertEqual(n_sleeps, 0)

    def test_retry_sleep_duration(self):
        """Each retry must sleep for exactly 30 seconds."""
        from pilot.util.proxy import get_proxy

        transient = 'failed to send request: timeout'
        good = {'StatusCode': 0, 'userProxy': '-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----\n'}

        fake_user_mod = _make_user_proxy_module()
        fake_mod_name = 'pilot.user.atlas.proxy'
        original = sys.modules.get(fake_mod_name)
        sys.modules[fake_mod_name] = fake_user_mod

        try:
            with patch('pilot.util.proxy.https') as mock_https, \
                 patch('pilot.util.proxy.config') as mock_cfg, \
                 patch.dict('os.environ', {'PILOT_USER': 'atlas', 'PANDA_SERVER_URL': 'https://pandaserver.cern.ch'}), \
                 patch('pilot.util.proxy.sleep') as mock_sleep, \
                 patch('pilot.util.proxy.write_file', return_value=True), \
                 patch('pilot.util.proxy.os.open', return_value=3), \
                 patch('pilot.util.proxy.os.close'), \
                 patch('pilot.util.proxy.vomsproxyinfo', return_value=(0, '', '')):

                mock_cfg.Pilot.pandaserver = 'https://pandaserver.cern.ch'
                mock_https.request2.side_effect = [transient, good]
                get_proxy('/tmp/x509up_u0.proxy', 'atlas')

            mock_sleep.assert_called_once_with(30)
        finally:
            if original is None:
                sys.modules.pop(fake_mod_name, None)
            else:
                sys.modules[fake_mod_name] = original


# ---------------------------------------------------------------------------
# Tests for get_and_verify_proxy() error propagation
# ---------------------------------------------------------------------------

class TestGetAndVerifyProxyErrorPropagation(unittest.TestCase):
    """get_and_verify_proxy() must propagate download failure as NOPROXY."""

    def test_download_failure_returns_noproxy_exit_code(self):
        """get_proxy() returning False must cause exit_code == NOPROXY."""
        from pilot.user.atlas.proxy import get_and_verify_proxy

        with patch('pilot.user.atlas.proxy.get_proxy', return_value=(False, '/tmp/x509up_u0.proxy')):
            exit_code, diagnostics, x509 = get_and_verify_proxy(
                '/tmp/x509up_u0.proxy', voms_role='atlas', proxy_type='payload'
            )

        self.assertEqual(exit_code, errors.NOPROXY,
                         f'download failure must return NOPROXY (1163), got {exit_code}')

    def test_download_failure_sets_diagnostics(self):
        """get_proxy() returning False must set a non-empty diagnostics string."""
        from pilot.user.atlas.proxy import get_and_verify_proxy

        with patch('pilot.user.atlas.proxy.get_proxy', return_value=(False, '/tmp/x509up_u0.proxy')):
            _, diagnostics, _ = get_and_verify_proxy(
                '/tmp/x509up_u0.proxy', voms_role='atlas', proxy_type='payload'
            )

        self.assertTrue(diagnostics, 'diagnostics must be non-empty on download failure')
        self.assertIn('atlas', diagnostics)

    def test_download_failure_preserves_original_x509(self):
        """On download failure, the original x509 path must be returned unchanged."""
        from pilot.user.atlas.proxy import get_and_verify_proxy

        original = '/tmp/x509up_u12345.proxy'
        with patch('pilot.user.atlas.proxy.get_proxy', return_value=(False, original)):
            _, _, x509_out = get_and_verify_proxy(
                original, voms_role='atlas', proxy_type='payload'
            )

        self.assertEqual(x509_out, original)

    def test_download_success_returns_zero_exit_code(self):
        """Successful proxy download and verification must return exit_code == 0."""
        from pilot.user.atlas.proxy import get_and_verify_proxy

        with patch('pilot.user.atlas.proxy.get_proxy', return_value=(True, '/tmp/x509up_u0-payload.proxy')), \
             patch('pilot.user.atlas.proxy.verify_proxy', return_value=(0, '')):
            exit_code, _, _ = get_and_verify_proxy(
                '/tmp/x509up_u0.proxy', voms_role='atlas', proxy_type='payload'
            )

        self.assertEqual(exit_code, 0)


if __name__ == '__main__':
    unittest.main()
