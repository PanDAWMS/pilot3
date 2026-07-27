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

"""Unit tests for the cached proxy validity end.

extract_time_left() caches the proxy validity in pilot_cache so that the rest of the
pilot can work out how much proxy time is left. It used to cache a *relative* lifetime
(validity_end - now). Because verify_arcproxy() caches its verification result per
proxy_id, that code path is normally only reached once per pilot, so the relative value
was frozen at its start-up reading and never decreased -- a six-hour-old multijob pilot
still reported its original proxy lifetime.

An absolute epoch timestamp is cached instead, from which a live remaining time can be
derived at any later point.

Covers:
- extract_time_left(): caches the absolute validity end, unchanged by the passage of time.
- the derived remaining time decreases as the pilot runs (the actual regression).
- extract_time_left(): leaves the cache alone when no validity end can be parsed.
"""

import logging
import sys
import unittest
from time import time
from unittest.mock import patch

from pilot.common.pilotcache import get_pilot_cache
from pilot.user.atlas.proxy import extract_time_left

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

pilot_cache = get_pilot_cache()


def _arcproxy_stdout(validity_end_cert: int, validity_end: int) -> str:
    """Build arcproxy stdout in the four-line form extract_time_left() expects.

    The command is `arcproxy -i validityEnd -i validityLeft -i vomsACvalidityEnd
    -i vomsACvalidityLeft`, so the lines are, in order: certificate validity end,
    certificate validity left, VOMS AC validity end, VOMS AC validity left.

    Args:
        validity_end_cert: epoch time (s) at which the certificate expires.
        validity_end: epoch time (s) at which the VOMS attribute expires.

    Returns:
        str: four newline-separated values as arcproxy would print them.
    """
    now = int(time())
    return (
        f"{validity_end_cert}\n"
        f"{validity_end_cert - now}\n"
        f"{validity_end}\n"
        f"{validity_end - now}\n"
    )


class TestProxyValidityEndIsAbsolute(unittest.TestCase):
    """extract_time_left() must cache an absolute validity end, not a relative lifetime."""

    def setUp(self):
        """Save the cached validity end so each test starts from a known state."""
        self._saved = pilot_cache.proxy_validity_end
        pilot_cache.proxy_validity_end = 0

    def tearDown(self):
        """Restore the cached validity end."""
        pilot_cache.proxy_validity_end = self._saved

    def test_caches_absolute_validity_end(self):
        """The cached value must be the epoch timestamp itself, not seconds remaining."""
        now = int(time())
        validity_end = now + 72 * 3600  # a fresh 72h proxy, as required at pilot start-up

        extract_time_left(_arcproxy_stdout(now + 90 * 3600, validity_end))

        self.assertEqual(pilot_cache.proxy_validity_end, validity_end)

    def test_derived_remaining_time_decreases_over_time(self):
        """The regression: remaining proxy time must shrink as the pilot runs.

        verify_arcproxy() caches its result per proxy_id, so extract_time_left() is
        normally only reached once per pilot. Reading the cache six hours later must
        still yield a correct remaining time, roughly six hours lower than at start-up.
        """
        now = int(time())
        validity_end = now + 72 * 3600

        extract_time_left(_arcproxy_stdout(now + 90 * 3600, validity_end))

        remaining_at_start = pilot_cache.proxy_validity_end - now

        # no second extract_time_left() call -- the cached verification result means the
        # pilot never re-reads the proxy; only the wall clock has moved on
        six_hours_later = now + 6 * 3600
        remaining_later = pilot_cache.proxy_validity_end - six_hours_later

        self.assertEqual(remaining_at_start, 72 * 3600)
        self.assertEqual(remaining_later, 66 * 3600)
        self.assertLess(remaining_later, remaining_at_start)

    def test_expired_proxy_yields_negative_remaining_time(self):
        """An already-expired proxy must be distinguishable from a healthy one."""
        now = int(time())
        validity_end = now - 600  # expired ten minutes ago

        extract_time_left(_arcproxy_stdout(now + 3600, validity_end))

        self.assertEqual(pilot_cache.proxy_validity_end, validity_end)
        self.assertLess(pilot_cache.proxy_validity_end - now, 0)

    def test_cache_untouched_when_validity_end_unparsable(self):
        """Leave the cache at its default when arcproxy output cannot be parsed."""
        with patch('builtins.print'):  # extract_time_left() prints on the short-output path
            extract_time_left("garbage output\n")

        self.assertEqual(pilot_cache.proxy_validity_end, 0)


if __name__ == '__main__':
    unittest.main()
