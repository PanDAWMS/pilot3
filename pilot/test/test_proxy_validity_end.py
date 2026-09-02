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

"""Unit tests for the cached pilot proxy validity end.

verify_arcproxy() records when the pilot's own proxy expires in pilot_cache, so that the
rest of the pilot can work out how much proxy time is left. Two properties matter and are
easy to get wrong:

- The value must be an absolute epoch timestamp, not a relative lifetime. verify_arcproxy()
  serves repeat calls from its own per-proxy_id cache, so this code path is normally only
  reached once per pilot; a relative value would be frozen at its start-up reading and never
  decrease, leaving a six-hour-old multijob pilot still reporting its original proxy lifetime.

- Only the pilot's own proxy may be recorded. get_and_verify_proxy() verifies the downloaded
  *payload* proxy with proxy_id=None, so a write placed deeper down in extract_time_left() --
  which cannot tell the two apart -- would overwrite the pilot's validity with the payload
  proxy's.

Covers:
- verify_arcproxy(): caches the pilot proxy's absolute validity end.
- the derived remaining time decreases as the pilot runs.
- verify_arcproxy(): leaves the pilot's cached validity alone for a payload proxy.
- verify_arcproxy(): records an already-expired proxy as such.
- extract_time_left(): parses without touching the cache.
"""

import logging
import sys
import unittest
from time import time
from unittest.mock import patch

from pilot.common.pilotcache import get_pilot_cache
from pilot.user.atlas.proxy import extract_time_left, verify_arcproxy

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

pilot_cache = get_pilot_cache()


def _arcproxy_stdout(validity_end_cert: int, validity_end: int) -> str:
    """Build arcproxy stdout in the four-line form the pilot parses.

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


class TestPilotProxyValidityEndCache(unittest.TestCase):
    """verify_arcproxy() must cache the pilot proxy's absolute validity end, and only that."""

    def setUp(self):
        """Start from a known state: empty caches, no leftover validity."""
        self._saved = pilot_cache.proxy_validity_end
        pilot_cache.proxy_validity_end = 0
        if hasattr(verify_arcproxy, 'cache'):
            self._saved_cache = dict(verify_arcproxy.cache)
            verify_arcproxy.cache.clear()
        else:
            self._saved_cache = None

    def tearDown(self):
        """Restore both caches."""
        pilot_cache.proxy_validity_end = self._saved
        if self._saved_cache is not None:
            verify_arcproxy.cache.clear()
            verify_arcproxy.cache.update(self._saved_cache)

    @staticmethod
    def _run(validity_end, proxy_id='pilot', validity_end_cert=None, limit=72):
        """Drive verify_arcproxy() with a stubbed arcproxy invocation.

        Args:
            validity_end: epoch time (s) at which the VOMS attribute expires.
            proxy_id: proxy id to verify under; 'pilot' for the pilot's own proxy, None for
                the downloaded payload proxy.
            validity_end_cert: epoch time (s) at which the certificate expires; defaults to
                well beyond the proxy so it is never the limiting factor.
            limit: time limit in hours passed through to verify_arcproxy().

        Returns:
            tuple[int, str]: exit code and diagnostics from verify_arcproxy().
        """
        if validity_end_cert is None:
            validity_end_cert = validity_end + 90 * 3600
        stdout = _arcproxy_stdout(validity_end_cert, validity_end)
        with patch('pilot.user.atlas.proxy.execute_nothreads', return_value=(0, stdout, '')):
            return verify_arcproxy('', limit, proxy_id=proxy_id)

    def test_caches_absolute_validity_end_for_pilot_proxy(self):
        """The cached value must be the epoch timestamp itself, not seconds remaining."""
        validity_end = int(time()) + 72 * 3600  # a fresh 72h proxy, as required at pilot start-up

        self._run(validity_end)

        self.assertEqual(pilot_cache.proxy_validity_end, validity_end)

    def test_derived_remaining_time_decreases_over_time(self):
        """The regression: remaining proxy time must shrink as the pilot runs.

        verify_arcproxy() serves repeat calls from its own cache, so this code path is
        normally only reached once per pilot. Reading the cached value six hours later must
        still yield a correct remaining time, roughly six hours lower than at start-up.
        """
        now = int(time())
        validity_end = now + 72 * 3600

        self._run(validity_end)

        # no second verification -- the per-proxy_id cache means the pilot never re-reads the
        # proxy; only the wall clock has moved on
        remaining_at_start = pilot_cache.proxy_validity_end - now
        remaining_later = pilot_cache.proxy_validity_end - (now + 6 * 3600)

        self.assertEqual(remaining_at_start, 72 * 3600)
        self.assertEqual(remaining_later, 66 * 3600)
        self.assertLess(remaining_later, remaining_at_start)

    def test_payload_proxy_does_not_overwrite_pilot_validity(self):
        """A payload proxy verification must leave the pilot's cached validity untouched.

        get_and_verify_proxy() verifies the downloaded payload proxy with proxy_id=None. That
        proxy is a different credential with its own, typically much shorter, validity, and
        must not be mistaken for the pilot's own.
        """
        now = int(time())
        pilot_validity_end = now + 72 * 3600
        self._run(pilot_validity_end)
        self.assertEqual(pilot_cache.proxy_validity_end, pilot_validity_end)

        payload_validity_end = now + 3600  # short-lived payload proxy
        self._run(payload_validity_end, proxy_id=None, limit=1)

        self.assertEqual(pilot_cache.proxy_validity_end, pilot_validity_end)

    def test_named_non_pilot_proxy_does_not_overwrite_pilot_validity(self):
        """Only the 'pilot' proxy id may update the cached pilot validity."""
        now = int(time())
        pilot_validity_end = now + 72 * 3600
        self._run(pilot_validity_end)

        self._run(now + 1800, proxy_id='payload', limit=1)

        self.assertEqual(pilot_cache.proxy_validity_end, pilot_validity_end)

    def test_expired_proxy_yields_negative_remaining_time(self):
        """An already-expired proxy must be distinguishable from a healthy one."""
        now = int(time())
        validity_end = now - 600  # expired ten minutes ago

        self._run(validity_end, limit=1)

        self.assertEqual(pilot_cache.proxy_validity_end, validity_end)
        self.assertLess(pilot_cache.proxy_validity_end - now, 0)

    def test_short_proxy_is_still_recorded(self):
        """A proxy that fails the limit check still has a knowable validity end.

        The validity is a parsed property of the proxy, independent of whether it passed
        verification, and the remaining-time calculation needs it either way.
        """
        validity_end = int(time()) + 900  # far below the 72h start-up requirement

        exit_code, _ = self._run(validity_end)

        self.assertNotEqual(exit_code, 0)
        self.assertEqual(pilot_cache.proxy_validity_end, validity_end)

    def test_extract_time_left_does_not_touch_the_cache(self):
        """extract_time_left() must stay a pure parser with no cache side effect."""
        now = int(time())

        validity_end_cert, validity_end, _ = extract_time_left(
            _arcproxy_stdout(now + 90 * 3600, now + 72 * 3600)
        )

        self.assertEqual(validity_end, now + 72 * 3600)
        self.assertEqual(validity_end_cert, now + 90 * 3600)
        self.assertEqual(pilot_cache.proxy_validity_end, 0)


if __name__ == '__main__':
    unittest.main()
