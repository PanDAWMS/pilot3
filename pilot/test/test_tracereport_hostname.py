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

"""Regression tests for the host name and IP address in the Rucio trace report.

Production incident (CA-VICTORIA-K8S-T2, job 7373453055, Oct 2026): on an IPv6-only
Kubernetes worker node the pilot logged

    unable to detect hostname by address for trace report: [Errno -2] Name or service not known
    unable to detect host IP for trace report: [Errno -2] Name or service not known

and sent hostname='unknown' and ip='0.0.0.0'. Two separate causes:

1. socket.gethostbyname() only resolves IPv4, so on an IPv6-only node the IP could never be
   found, even when the host name resolved.
2. The pod name had no DNS or /etc/hosts entry, so neither look-up could succeed.

Expected behaviour: IPv6-only names resolve to their IPv6 address (IPv4 still preferred on
dual-stack nodes), an unresolvable name is reported as is, and the IP address falls back to
the address of the outgoing network interface.
"""

import socket
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pilot.util import networking
from pilot.util.networking import get_host_ip, get_local_ip
from pilot.util.tracereport import TraceReport, get_host_identity

POD = 'grid-job-44582156-h852d'
NONAME = socket.gaierror(socket.EAI_NONAME, 'Name or service not known')


def _addrinfo(*addresses):
    """Return a getaddrinfo()-style list for the given (family, address) pairs."""
    result = []
    for family, address in addresses:
        sockaddr = (address, 0) if family == socket.AF_INET else (address, 0, 0, 0)
        result.append((family, socket.SOCK_STREAM, 6, '', sockaddr))
    return result


class TestGetHostIp(unittest.TestCase):
    """Tests for get_host_ip()."""

    def test_ipv6_only_name_resolves(self):
        """An IPv6-only host name returns its IPv6 address (gethostbyname() failed here)."""
        with patch.object(networking.socket, 'getaddrinfo', return_value=_addrinfo((socket.AF_INET6, 'fd00::1234'))):
            self.assertEqual(get_host_ip(POD), 'fd00::1234')

    def test_dual_stack_prefers_ipv4(self):
        """On a dual-stack node the IPv4 address is reported, as before the fix."""
        info = _addrinfo((socket.AF_INET6, 'fd00::1234'), (socket.AF_INET, '10.0.0.7'))
        with patch.object(networking.socket, 'getaddrinfo', return_value=info):
            self.assertEqual(get_host_ip(POD), '10.0.0.7')

    def test_lookup_is_family_agnostic(self):
        """The look-up must ask for any address family, not only IPv4."""
        with patch.object(networking.socket, 'getaddrinfo', return_value=_addrinfo((socket.AF_INET6, 'fd00::1'))) as mock:
            get_host_ip(POD)
        self.assertEqual(mock.call_args[0][2], socket.AF_UNSPEC)

    def test_unresolvable_name_raises(self):
        """An unresolvable name raises socket.gaierror."""
        with patch.object(networking.socket, 'getaddrinfo', side_effect=NONAME):
            with self.assertRaises(socket.gaierror):
                get_host_ip(POD)

    def test_no_usable_family_raises(self):
        """Only non-IP results (e.g. AF_UNIX) raise socket.gaierror instead of returning junk."""
        info = [(socket.AF_UNIX, socket.SOCK_STREAM, 0, '', '/tmp/sock')]
        with patch.object(networking.socket, 'getaddrinfo', return_value=info):
            with self.assertRaises(socket.gaierror):
                get_host_ip(POD)


class TestGetLocalIp(unittest.TestCase):
    """Tests for get_local_ip()."""

    @staticmethod
    def _socket_factory(working_families, address='2001:db8::5'):
        """Return a socket.socket replacement that only has a route for the given families."""
        def factory(family, _type):
            sock = MagicMock()
            sock.__enter__.return_value = sock
            if family in working_families:
                sock.getsockname.return_value = (address, 12345, 0, 0) if family == socket.AF_INET6 else (address, 12345)
            else:
                sock.connect.side_effect = OSError(101, 'Network is unreachable')
            return sock
        return factory

    def test_ipv6_only_node(self):
        """With no IPv4 route, the IPv6 interface address is returned."""
        with patch.object(networking.socket, 'socket', side_effect=self._socket_factory({socket.AF_INET6})):
            self.assertEqual(get_local_ip(), '2001:db8::5')

    def test_ipv4_tried_first(self):
        """With an IPv4 route, the IPv4 address is returned without trying IPv6."""
        factory = MagicMock(side_effect=self._socket_factory({socket.AF_INET, socket.AF_INET6}, '192.0.2.10'))
        with patch.object(networking.socket, 'socket', factory):
            self.assertEqual(get_local_ip(), '192.0.2.10')
        self.assertEqual(factory.call_count, 1)
        self.assertEqual(factory.call_args[0][0], socket.AF_INET)

    def test_no_route_returns_empty(self):
        """With no route at all, an empty string is returned."""
        with patch.object(networking.socket, 'socket', side_effect=self._socket_factory(set())):
            self.assertEqual(get_local_ip(), '')

    def test_udp_socket_is_used(self):
        """A UDP socket is used, so that connect() sends no packets."""
        factory = MagicMock(side_effect=self._socket_factory({socket.AF_INET}, '192.0.2.10'))
        with patch.object(networking.socket, 'socket', factory):
            get_local_ip()
        self.assertEqual(factory.call_args[0][1], socket.SOCK_DGRAM)


class TestGetHostIdentity(unittest.TestCase):
    """Tests for get_host_identity()."""

    def test_victoria_pod_unresolvable(self):
        """The Victoria case: pod name not resolvable -> name as is, outgoing interface address."""
        with patch('pilot.util.tracereport.socket.gethostbyaddr', side_effect=NONAME), \
                patch('pilot.util.tracereport.get_host_ip', side_effect=NONAME), \
                patch('pilot.util.tracereport.get_local_ip', return_value='2001:db8::5'):
            self.assertEqual(get_host_identity(POD), (POD, '2001:db8::5'))

    def test_nothing_available(self):
        """No resolution and no route -> 0.0.0.0, as before."""
        with patch('pilot.util.tracereport.socket.gethostbyaddr', side_effect=NONAME), \
                patch('pilot.util.tracereport.get_host_ip', side_effect=NONAME), \
                patch('pilot.util.tracereport.get_local_ip', return_value=''):
            self.assertEqual(get_host_identity(POD), (POD, '0.0.0.0'))

    def test_empty_name_reported_as_unknown(self):
        """An empty host name is still reported as 'unknown'."""
        with patch('pilot.util.tracereport.socket.gethostbyaddr', side_effect=NONAME), \
                patch('pilot.util.tracereport.get_host_ip', side_effect=NONAME), \
                patch('pilot.util.tracereport.get_local_ip', return_value=''):
            self.assertEqual(get_host_identity('')[0], 'unknown')

    def test_resolvable_host(self):
        """A resolvable host gets its full name and resolved address; no fallback is used."""
        local = MagicMock(return_value='192.0.2.99')
        with patch('pilot.util.tracereport.socket.gethostbyaddr', return_value=('wn1.example.org', [], ['10.0.0.7'])), \
                patch('pilot.util.tracereport.get_host_ip', return_value='10.0.0.7'), \
                patch('pilot.util.tracereport.get_local_ip', local):
            self.assertEqual(get_host_identity('wn1'), ('wn1.example.org', '10.0.0.7'))
        local.assert_not_called()

    def test_lookup_timeout_does_not_raise(self):
        """A resolver time-out (an OSError) must not escape into trace report initialisation."""
        with patch('pilot.util.tracereport.socket.gethostbyaddr', side_effect=socket.timeout('timed out')), \
                patch('pilot.util.tracereport.get_host_ip', side_effect=socket.timeout('timed out')), \
                patch('pilot.util.tracereport.get_local_ip', return_value='2001:db8::5'):
            self.assertEqual(get_host_identity(POD), (POD, '2001:db8::5'))

    def test_invalid_name_does_not_raise(self):
        """A name the IDNA codec rejects (UnicodeError) must not escape either."""
        bad = 'a' * 64 + '.example.org'  # label longer than 63 characters
        with patch('pilot.util.tracereport.get_local_ip', return_value=''):
            self.assertEqual(get_host_identity(bad), (bad, '0.0.0.0'))


class TestTraceReportInit(unittest.TestCase):
    """End-to-end check that TraceReport.init() fills hostname and ip via the new look-up."""

    def test_init_on_ipv6_only_pod(self):
        """IPv6-only pod whose name resolves: the IPv6 address is reported, not 0.0.0.0."""
        job = SimpleNamespace(produserid='user', jobid='7373453055', taskid='1', jobdefinitionid='42')
        report = TraceReport(pq='CA-VICTORIA-K8S-T2')
        with patch.dict('os.environ', {'PANDA_HOSTNAME': POD}), \
                patch('pilot.util.tracereport.socket.gethostbyaddr', return_value=(POD, [], ['fd00::1234'])), \
                patch.object(networking.socket, 'getaddrinfo', return_value=_addrinfo((socket.AF_INET6, 'fd00::1234'))):
            report.init(job)
        self.assertEqual(report['hostname'], POD)
        self.assertEqual(report['ip'], 'fd00::1234')


if __name__ == '__main__':
    unittest.main()
