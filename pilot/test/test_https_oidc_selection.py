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

"""Unit tests for the choice between OIDC token and X.509 authentication in server requests.

The choice only depends on whether a token and origin are configured and whether the request goes to the PanDA
server. In particular, it does not depend on the site (it used to be forced to X.509 on CERN-PTEST).
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.util import https

URL = 'https://pandaserver.example.org:25443/api/v1/pilot/update_pilot_attributes'
PROXY = '/tmp/x509up_u12345'
SITES = ('CERN-PTEST', 'ANALY_CERN-PTEST', 'BNL', '')


class OidcSelectionTestCase(unittest.TestCase):
    """Base class: resets the pilot singletons and mocks the token and TLS environment."""

    def setUp(self):
        """Reset the error codes, snapshot the pilot cache and mock the token lookup."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []
        self._cache_snapshot = dict(vars(get_pilot_cache()))
        self.token_info = patch.object(https, 'get_local_oidc_token_info', return_value=('token', 'atlas.pilot'))
        self.token_content = patch.object(https, 'get_auth_token_content', return_value='secret-token')
        self.token_info.start()
        self.token_content.start()

    def tearDown(self):
        """Stop the mocks, restore the pilot cache and reset the error codes."""
        self.token_content.stop()
        self.token_info.stop()
        cache = get_pilot_cache()
        vars(cache).clear()
        vars(cache).update(self._cache_snapshot)
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []


class TestRequest2AuthSelection(OidcSelectionTestCase):
    """request2() uses the token when one is configured, on every site."""

    def _request2(self, site: str, panda: bool = True) -> tuple:
        """Call request2() with the network and TLS layers mocked.

        Args:
            site: Value of PILOT_SITENAME.
            panda: Passed on to request2().

        Returns:
            (response, the urllib Request that was sent, the mocked SSL context).
        """
        ssl_context = MagicMock()
        response = MagicMock(status=200, reason='OK')
        response.read.return_value = b'{"success": true, "message": "ok", "data": null}'
        urlopen = MagicMock()
        urlopen.return_value.__enter__.return_value = response
        with patch.dict(os.environ, {'PILOT_SITENAME': site}), \
                patch.object(https._ctx, 'cacert', PROXY, create=True), \
                patch.object(https._ctx, 'capath', '/etc/grid-security/certificates', create=True), \
                patch.object(https, '_get_ssl_context', return_value=ssl_context), \
                patch.object(https.urllib.request, 'urlopen', urlopen):
            res = https.request2(URL, json_body={'job_id': 1}, panda=panda)
        return res, urlopen.call_args[0][0], ssl_context

    def test_token_used_on_every_site(self):
        """With a token and origin, the token is used and no client certificate is loaded."""
        for site in SITES:
            with self.subTest(site=site):
                res, req, ssl_context = self._request2(site)
                self.assertEqual(res['success'], True)
                self.assertEqual(req.get_header('Authorization'), 'Bearer secret-token')
                ssl_context.load_cert_chain.assert_not_called()

    def test_decision_is_logged(self):
        """The log states that token authentication is used, and nothing about switching it off."""
        with self.assertLogs(https.logger, level='DEBUG') as logs:
            self._request2('CERN-PTEST')
        self.assertIn('will use OIDC token authentication', '\n'.join(logs.output))
        self.assertFalse(any('switched off' in entry for entry in logs.output))

    def test_x509_without_token(self):
        """Without a token, the X.509 proxy is used as client certificate."""
        with patch.object(https, 'get_local_oidc_token_info', return_value=(None, None)):
            _, req, ssl_context = self._request2('CERN-PTEST')
        self.assertIsNone(req.get_header('Authorization'))
        ssl_context.load_cert_chain.assert_called_once_with(certfile=PROXY, keyfile=PROXY)

    def test_x509_without_origin(self):
        """A token without origin is not used."""
        with patch.object(https, 'get_local_oidc_token_info', return_value=('token', None)):
            _, req, ssl_context = self._request2('BNL')
        self.assertIsNone(req.get_header('Authorization'))
        ssl_context.load_cert_chain.assert_called_once_with(certfile=PROXY, keyfile=PROXY)

    def test_x509_without_token_but_with_origin(self):
        """An origin without a token is not enough."""
        with patch.object(https, 'get_local_oidc_token_info', return_value=(None, 'atlas.pilot')):
            _, req, ssl_context = self._request2('BNL')
        self.assertIsNone(req.get_header('Authorization'))
        ssl_context.load_cert_chain.assert_called_once_with(certfile=PROXY, keyfile=PROXY)

    def test_x509_for_non_panda_request(self):
        """The token is only used for requests to the PanDA server."""
        _, req, ssl_context = self._request2('CERN-PTEST', panda=False)
        self.assertIsNone(req.get_header('Authorization'))
        ssl_context.load_cert_chain.assert_called_once_with(certfile=PROXY, keyfile=PROXY)


class TestGetAuthAndHeaders(OidcSelectionTestCase):
    """_get_auth_and_headers() makes the same choice as request2()."""

    def test_token_used_on_every_site(self):
        """With a token and origin, the headers carry the token on every site."""
        for site in SITES:
            with self.subTest(site=site), patch.dict(os.environ, {'PILOT_SITENAME': site}):
                headers, use_oidc = https._get_auth_and_headers(URL, panda=True)
                self.assertTrue(use_oidc)
                self.assertEqual(headers['Authorization'], 'Bearer secret-token')

    def test_no_token(self):
        """Without a token, no token headers are added."""
        with patch.object(https, 'get_local_oidc_token_info', return_value=(None, None)), \
                patch.dict(os.environ, {'PILOT_SITENAME': 'CERN-PTEST'}):
            headers, use_oidc = https._get_auth_and_headers(URL, panda=True)
        self.assertFalse(use_oidc)
        self.assertNotIn('Authorization', headers)

    def test_token_without_origin(self):
        """A token without origin is not used."""
        with patch.object(https, 'get_local_oidc_token_info', return_value=('token', None)):
            headers, use_oidc = https._get_auth_and_headers(URL, panda=True)
        self.assertFalse(use_oidc)
        self.assertNotIn('Authorization', headers)

    def test_origin_without_token(self):
        """An origin without a token is not enough."""
        with patch.object(https, 'get_local_oidc_token_info', return_value=(None, 'atlas.pilot')):
            headers, use_oidc = https._get_auth_and_headers(URL, panda=True)
        self.assertFalse(use_oidc)
        self.assertNotIn('Authorization', headers)

    def test_non_panda_request(self):
        """The token is only used for requests to the PanDA server."""
        headers, use_oidc = https._get_auth_and_headers(URL, panda=False)
        self.assertFalse(use_oidc)
        self.assertNotIn('Authorization', headers)


if __name__ == '__main__':
    unittest.main()
