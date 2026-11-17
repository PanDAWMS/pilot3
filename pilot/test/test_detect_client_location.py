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

"""Regression tests for the client location sent to rucio.list_replicas().

Production context (CA-VICTORIA-K8S-T2, Oct 2026, IPv6-only Kubernetes worker nodes): the
local IP was only looked up over IPv4, so on an IPv6-only node the connect() failed, the pilot
logged a warning and sent ip='0.0.0.0'. The Rucio server replaces the IP with the request's
source address before geoip sorting, so the replica order was not affected - only the warning
and the logged value were wrong.

Expected behaviour: IPv6-only nodes report their IPv6 address without a warning; dual-stack
nodes report the same IPv4 address as before; with no route at all, 0.0.0.0 and a diagnostic.
"""

import logging
import unittest
from unittest.mock import patch

from pilot.api.data import StagingClient


class _Client(StagingClient):
    """StagingClient stand-in that skips the full constructor (infosys, copytools)."""

    def __init__(self):  # pylint: disable=super-init-not-called
        """Only set up the logger used by detect_client_location()."""
        self.logger = logging.getLogger('test_detect_client_location')


class TestDetectClientLocation(unittest.TestCase):
    """Tests for StagingClient.detect_client_location()."""

    def setUp(self):
        """Create the client."""
        self.client = _Client()

    def test_ipv6_only_node(self):
        """IPv6-only node: the IPv6 address is reported, with no diagnostics."""
        with patch('pilot.api.data.get_local_ip', return_value='2001:db8::5'), \
                patch.dict('os.environ', {'PILOT_RUCIO_SITENAME': 'CA-VICTORIA-WESTGRID-T2'}):
            location, diagnostics = self.client.detect_client_location()
        self.assertEqual(location, {'ip': '2001:db8::5', 'site': 'CA-VICTORIA-WESTGRID-T2'})
        self.assertEqual(diagnostics, '')

    def test_ipv4_node_unchanged(self):
        """IPv4 or dual-stack node: the IPv4 address is reported, as before."""
        with patch('pilot.api.data.get_local_ip', return_value='192.0.2.10'):
            location, diagnostics = self.client.detect_client_location()
        self.assertEqual(location['ip'], '192.0.2.10')
        self.assertEqual(diagnostics, '')

    def test_no_route(self):
        """No route at all: 0.0.0.0 and a diagnostic (warned about by list_replicas())."""
        with patch('pilot.api.data.get_local_ip', return_value=''):
            with self.assertLogs('test_detect_client_location', level='WARNING'):
                location, diagnostics = self.client.detect_client_location()
        self.assertEqual(location['ip'], '0.0.0.0')
        self.assertIn('no outgoing IPv4 or IPv6 route', diagnostics)

    def test_site_default(self):
        """Without PILOT_RUCIO_SITENAME the site is 'unknown', as before."""
        with patch('pilot.api.data.get_local_ip', return_value='192.0.2.10'), \
                patch.dict('os.environ', {}, clear=True):
            location, _ = self.client.detect_client_location()
        self.assertEqual(location['site'], 'unknown')

    def test_vp_coordinates_from_environment(self):
        """VP path is unchanged: coordinates come from RUCIO_LATITUDE/RUCIO_LONGITUDE."""
        env = {'RUCIO_LATITUDE': '48.42', 'RUCIO_LONGITUDE': '-123.37', 'PILOT_RUCIO_SITENAME': 'X'}
        with patch('pilot.api.data.get_local_ip', return_value='2001:db8::5'), \
                patch.dict('os.environ', env, clear=True):
            location, diagnostics = self.client.detect_client_location(use_vp=True)
        self.assertEqual(location, {'ip': '2001:db8::5', 'site': 'X', 'latitude': 48.42, 'longitude': -123.37})
        self.assertEqual(diagnostics, '')


if __name__ == '__main__':
    unittest.main()
