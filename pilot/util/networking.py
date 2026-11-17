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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023-26

"""Functions related to networking."""

import ipaddress
import logging
import re
import socket

from pilot.util.auxiliary import is_command_available
from pilot.util.container import execute

logger = logging.getLogger(__name__)


def dump_ipv6_info() -> None:
    """Dump the IPv6 info to the log."""
    cmd = 'ifconfig'
    if not is_command_available(cmd):
        _cmd = '/usr/sbin/ifconfig -a'
        if not is_command_available(_cmd):
            logger.warning(f'command {cmd} is not available - this WN might not support IPv6')
            return
        cmd = _cmd

    _, stdout, stderr = execute(cmd, timeout=10)
    if stdout:
        ipv6 = extract_ipv6_addresses(stdout)
        if ipv6:
            logger.info(f'IPv6 addresses: {ipv6}')
        else:
            logger.warning('no IPv6 addresses were found')
    else:
        logger.warning(f'failed to run ifconfig: {stderr}')


def extract_ipv6_addresses(ifconfig_output: str) -> list:
    """Extracts IPv6 addresses from ifconfig output.

    Args:
        ifconfig_output: The output of the ifconfig command.

    Returns:
        A list of IPv6 addresses.
    """
    ipv6_addresses = []
    for line in ifconfig_output.splitlines():
        line = line.strip().replace("\t", " ").replace("\r", "").replace("\n", "")
        match = re.search(r"inet6 (.*?)\s", line)
        if match and match.group(1) != "::1":  # skip loopback address
            ipv6_addresses.append(match.group(1))

    return ipv6_addresses


def extract_ipv6(ifconfig: str) -> str:
    """Extract IPv6 addresses from ifconfig output.

    Args:
        ifconfig: Output of the ifconfig command.

    Returns:
        List of valid IPv6 address strings.
    """
    # Regular expression pattern to match MAC addresses
    mac_pattern = r'([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})'

    # Extract MAC addresses from the text
    # mac_addresses = re.findall(mac_pattern, ifconfig)

    # Replace MAC addresses with placeholders
    placeholder = '__MAC_ADDRESS__'
    text_without_mac = re.sub(mac_pattern, placeholder, ifconfig)

    # Regular expression pattern to match potential IPv6 addresses
    ipv6_pattern = r'\b[0-9a-fA-F:]+\b'

    # Extract potential addresses from the text without MAC addresses
    potential_addresses = re.findall(ipv6_pattern, text_without_mac)

    # Filter and collect valid IPv6 addresses
    valid_ipv6_addresses = [addr for addr in potential_addresses if ':' in addr]

    # Validate if addresses are IPv6
    try:
        ipv6_addresses = [str(ipaddress.IPv6Address(addr)) for addr in valid_ipv6_addresses if
                          ipaddress.ip_address(addr).version == 6]
    except ValueError:
        ipv6_addresses = []

    return ipv6_addresses


def get_host_ip(hostname: str) -> str:
    """Return an IP address for the given host name, IPv4 preferred.

    Unlike ``socket.gethostbyname()``, which only knows about IPv4, this also
    resolves names that only have an IPv6 address, as on IPv6-only worker
    nodes. IPv4 is preferred when both exist, so that dual-stack nodes report
    the same address as before.

    Args:
        hostname: Host name to resolve.

    Returns:
        The first IPv4 address of the host, or else its first IPv6 address.

    Raises:
        OSError: If the name cannot be resolved (``socket.gaierror``).
    """
    infos = socket.getaddrinfo(hostname, None, socket.AF_UNSPEC, socket.SOCK_STREAM)
    for family in (socket.AF_INET, socket.AF_INET6):
        for info in infos:
            if info[0] == family:
                return info[4][0]

    raise socket.gaierror(socket.EAI_NONAME, f'no IPv4 or IPv6 address found for {hostname}')


def get_local_ip() -> str:
    """Return the address of the local interface used for outgoing traffic.

    Connecting a UDP socket only selects a route; no packets are sent. IPv4 is
    tried first, then IPv6, so that IPv6-only worker nodes also get a real
    address.

    Returns:
        The local IP address, or an empty string if there is no route.
    """
    targets = (('IPv4', socket.AF_INET, ('8.8.8.8', 80)),
               ('IPv6', socket.AF_INET6, ('2001:4860:4860::8888', 80)))
    for label, family, target in targets:
        try:
            with socket.socket(family, socket.SOCK_DGRAM) as sock:
                sock.connect(target)
                return sock.getsockname()[0]
        except OSError as exc:
            logger.debug(f'no outgoing {label} route: {exc}')

    return ''
