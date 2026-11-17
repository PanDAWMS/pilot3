#!/usr/bin/env python3
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

"""Check whether the client IP sent to rucio.list_replicas() changes the geoip replica order.

The pilot sends a client_location with its local IP address. The Rucio server replaces that
IP with the request's source address before geoip sorting, so the order is expected to be
the same whatever IP is sent. This script lists the replica order for the same files with
several client IPs and reports whether the orders differ.

Usage (after setupATLAS; lsetup rucio; voms-proxy-init -voms atlas):

    python3 verify_geoip_sorting.py <scope:name> [<scope:name> ...]

Use files with replicas at sites on several continents, otherwise the comparison shows little.
Exit code: 0 if the order is identical for all IPs, 1 if it differs, 2 on usage or setup errors.

Test confirmed to work:
    python3 verify_geoip_sorting.py mc15_13TeV:HITS.06828093._000096.pool.root.1
"""

import sys
from typing import Any

VARIANTS = {
    'old pilot on IPv6-only node': '0.0.0.0',
    'IPv6 pod address (new pilot)': '2001:db8::5',
    'far-away IPv4 (AARNet, AU)': '139.130.4.5',
}


def rse_order(client: Any, scope: str, name: str, ip: str) -> list:
    """Return the RSEs in the order the server sorted them for the given client IP.

    Args:
        client: Rucio client.
        scope: File scope.
        name: File name.
        ip: Client IP address to send in client_location.

    Returns:
        list: RSE names in priority order, without duplicates.
    """
    location = {'ip': ip, 'site': 'CA-VICTORIA-WESTGRID-T2'}
    replicas = client.list_replicas([{'scope': scope, 'name': name}], schemes=['root', 'davs', 'https'],
                                    sort='geoip', client_location=location)
    order = []
    for replica in replicas:
        for _, pfn in sorted(replica['pfns'].items(), key=lambda item: item[1]['priority']):
            if pfn['rse'] not in order:
                order.append(pfn['rse'])

    return order


def main(dids: list) -> int:
    """Compare the replica order for each client IP variant.

    Args:
        dids: List of DIDs in scope:name format.

    Returns:
        int: 0 if the order is identical for all IPs, 1 if it differs, 2 on usage or setup errors.
    """
    if not dids or any(':' not in did for did in dids):
        print(__doc__)
        return 2

    try:
        from rucio.client import Client  # pylint: disable=import-outside-toplevel
    except ImportError:
        print("Rucio client has not been setup, please run 'lsetup rucio' first")
        return 2

    client = Client()
    same = True
    for did in dids:
        scope, name = did.split(':', 1)
        orders = {label: rse_order(client, scope, name, ip) for label, ip in VARIANTS.items()}
        print(f'\n{did}')
        for label, order in orders.items():
            print(f'  {label:32s} {order}')
        if len({tuple(order) for order in orders.values()}) > 1:
            same = False
            print('  -> ORDER DIFFERS')
        elif len(next(iter(orders.values()))) < 2:
            print('  (fewer than two replicas - this file shows nothing about sorting)')

    print('\nresult:', 'identical order for all IPs' if same else 'order depends on the client IP')
    return 0 if same else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
