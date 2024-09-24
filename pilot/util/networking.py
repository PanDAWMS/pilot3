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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

"""Functions related to networking."""

import ipaddress
import logging
import re

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
    """
    Extract the IPv6 address from the ifconfig output.

    :param ifconfig: ifconfig output (str)
    :return: IPv6 address (str).
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
