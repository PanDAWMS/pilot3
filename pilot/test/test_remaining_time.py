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

"""Unit tests for the pilot's remaining-time calculation and its use in acquire_jobs.

The pilot reports how much time it has left in the acquire_jobs payload so that the server
can avoid handing out jobs that cannot finish. The value is the most constraining of three
optional sources: the proxy lifetime, the site time limit (PQ.maxtime) and the
MachineFeatures shutdowntime.

Covers:
- _get_remaining_time_candidates(): each source in isolation, all three together, none at
  all, and the guard against infosys not having been initialised.
- _compute_remaining_time(): selection of the most constraining source, its reported name,
  tie-breaking, and None when nothing is available.
- get_remaining_time(): the public value-only contract, including None.
- get_dispatcher_dictionary(): remaining_time present in the payload when positive, and
  absent when unavailable, zero or negative.
"""

import logging
import sys
import time
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from pilot.control import job as job_module

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def _make_args(pilot_start_time: float) -> SimpleNamespace:
    """Build a minimal args-like object sufficient for get_time_since_start().

    Args:
        pilot_start_time: epoch seconds to record as PILOT_START_TIME.

    Returns:
        SimpleNamespace: object with a `.timing` dict shaped like the real pilot
            args.timing structure.
    """
    return SimpleNamespace(timing={'0': {'PILOT_START_TIME': pilot_start_time}})


@contextmanager
def remaining_time_sources(proxy_validity_end: int = 0, queuedata: object = None,
                           shutdown: object = None):
    """Control all three remaining-time sources for the duration of a test.

    Anything not passed is left unavailable, which is the common real-world case: most
    queues have no MachineFeatures installed, PQ.maxtime is frequently unset, and queues
    using OIDC tokens rather than VOMS proxies never cache a proxy validity.

    Args:
        proxy_validity_end: absolute epoch time (s) at which the proxy expires, or 0 for
            "no proxy information cached".
        queuedata: object exposing a `maxtime` attribute, or None to simulate infosys not
            having been initialised.
        shutdown: value for _time_until_shutdown() -- seconds until shutdown, or None for
            "no MachineFeatures shutdowntime available".

    Yields:
        None: the patches are active inside the with-block.
    """
    with patch.object(job_module.pilot_cache, 'proxy_validity_end', proxy_validity_end), \
            patch.object(job_module.infosys, 'queuedata', queuedata), \
            patch('pilot.control.job._time_until_shutdown', return_value=shutdown):
        yield


class TestGetRemainingTimeCandidates(unittest.TestCase):
    """Tests for job._get_remaining_time_candidates()."""

    def test_no_source_available(self):
        """Return an empty dictionary when nothing can be determined."""
        args = _make_args(time.time() - 3600)
        with remaining_time_sources():
            self.assertEqual(job_module._get_remaining_time_candidates(args), {})

    def test_proxy_only(self):
        """Derive a live remaining lifetime from the cached absolute proxy validity end."""
        now = int(time.time())
        args = _make_args(now - 3600)
        with remaining_time_sources(proxy_validity_end=now + 7200):
            candidates = job_module._get_remaining_time_candidates(args)
        self.assertEqual(list(candidates), ['proxy'])
        self.assertTrue(7100 < candidates['proxy'] <= 7200)

    def test_pq_maxtime_only(self):
        """Subtract the time already spent from the site time limit."""
        now = time.time()
        args = _make_args(now - 3600)  # pilot started an hour ago
        with remaining_time_sources(queuedata=SimpleNamespace(maxtime=10000)):
            candidates = job_module._get_remaining_time_candidates(args)
        self.assertEqual(list(candidates), ['pq_maxtime'])
        self.assertTrue(6300 < candidates['pq_maxtime'] <= 6400)

    def test_shutdowntime_only(self):
        """Take the MachineFeatures shutdowntime as-is when it is the only source."""
        args = _make_args(time.time() - 3600)
        with remaining_time_sources(shutdown=4200):
            candidates = job_module._get_remaining_time_candidates(args)
        self.assertEqual(candidates, {'shutdowntime': 4200})

    def test_all_three_sources(self):
        """Collect every available source, in the documented insertion order."""
        now = int(time.time())
        args = _make_args(now - 3600)
        with remaining_time_sources(proxy_validity_end=now + 20000,
                                    queuedata=SimpleNamespace(maxtime=10000),
                                    shutdown=4200):
            candidates = job_module._get_remaining_time_candidates(args)
        self.assertEqual(list(candidates), ['proxy', 'pq_maxtime', 'shutdowntime'])

    def test_queuedata_none_does_not_raise(self):
        """infosys.queuedata is None until infosys is initialised -- this must not raise.

        Regression test: an unguarded infosys.queuedata.maxtime raises AttributeError, and
        the caller of proceed_with_getjob() silently converts any exception into "stop
        fetching jobs", which would strand the pilot.
        """
        args = _make_args(time.time() - 3600)
        with remaining_time_sources(queuedata=None, shutdown=4200):
            candidates = job_module._get_remaining_time_candidates(args)
        self.assertEqual(candidates, {'shutdowntime': 4200})

    def test_maxtime_zero_is_treated_as_unset(self):
        """PQ.maxtime defaults to 0, meaning "no limit configured", not "no time left"."""
        args = _make_args(time.time() - 3600)
        with remaining_time_sources(queuedata=SimpleNamespace(maxtime=0)):
            self.assertEqual(job_module._get_remaining_time_candidates(args), {})

    def test_expired_proxy_gives_a_negative_candidate(self):
        """An already-expired limit must be reported as negative, not dropped."""
        now = int(time.time())
        args = _make_args(now - 3600)
        with remaining_time_sources(proxy_validity_end=now - 600):
            candidates = job_module._get_remaining_time_candidates(args)
        self.assertLess(candidates['proxy'], 0)


class TestComputeRemainingTime(unittest.TestCase):
    """Tests for job._compute_remaining_time()."""

    def test_returns_none_when_no_source_available(self):
        """Return None rather than a misleading zero when nothing is known."""
        args = _make_args(time.time() - 3600)
        with remaining_time_sources():
            self.assertIsNone(job_module._compute_remaining_time(args))

    def test_picks_the_most_constraining_source(self):
        """The smallest candidate wins, and its name is reported alongside it."""
        now = int(time.time())
        args = _make_args(now - 3600)
        with remaining_time_sources(proxy_validity_end=now + 20000,
                                    queuedata=SimpleNamespace(maxtime=10000),
                                    shutdown=4200):
            remaining_time, source = job_module._compute_remaining_time(args)
        self.assertEqual(source, 'shutdowntime')
        self.assertEqual(remaining_time, 4200)

    def test_proxy_can_be_the_binding_source(self):
        """Source attribution must follow the values, not a fixed priority."""
        now = int(time.time())
        args = _make_args(now - 3600)
        with remaining_time_sources(proxy_validity_end=now + 900,
                                    queuedata=SimpleNamespace(maxtime=90000),
                                    shutdown=7200):
            remaining_time, source = job_module._compute_remaining_time(args)
        self.assertEqual(source, 'proxy')
        self.assertTrue(800 < remaining_time <= 900)

    def test_pq_maxtime_can_be_the_binding_source(self):
        """The site time limit must be reported when it is the most constraining."""
        now = int(time.time())
        args = _make_args(now - 3600)
        with remaining_time_sources(proxy_validity_end=now + 72 * 3600,
                                    queuedata=SimpleNamespace(maxtime=5000),
                                    shutdown=7200):
            remaining_time, source = job_module._compute_remaining_time(args)
        self.assertEqual(source, 'pq_maxtime')
        self.assertTrue(1300 < remaining_time <= 1400)

    def test_tie_between_sources_is_resolved_deterministically(self):
        """On an exact tie the first-inserted source wins: proxy, then pq_maxtime.

        Arbitrary but deterministic; pinned here so the behaviour cannot change silently.
        """
        args = _make_args(time.time() - 3600)
        with patch('pilot.control.job._get_remaining_time_candidates',
                   return_value={'proxy': 4200, 'pq_maxtime': 4200, 'shutdowntime': 9000}):
            _, source = job_module._compute_remaining_time(args)
        self.assertEqual(source, 'proxy')

        with patch('pilot.control.job._get_remaining_time_candidates',
                   return_value={'pq_maxtime': 4200, 'shutdowntime': 4200}):
            _, source = job_module._compute_remaining_time(args)
        self.assertEqual(source, 'pq_maxtime')

    def test_negative_value_is_returned_not_clamped(self):
        """A passed limit must surface as a negative value so callers can act on it."""
        args = _make_args(time.time() - 3600)
        with remaining_time_sources(shutdown=-500):
            remaining_time, source = job_module._compute_remaining_time(args)
        self.assertEqual((remaining_time, source), (-500, 'shutdowntime'))


class TestGetRemainingTime(unittest.TestCase):
    """Tests for the public job.get_remaining_time()."""

    def test_returns_value_only(self):
        """The public entry point returns the value without the source name."""
        args = _make_args(time.time() - 3600)
        with remaining_time_sources(shutdown=4200):
            self.assertEqual(job_module.get_remaining_time(args), 4200)

    def test_returns_none_when_unavailable(self):
        """None distinguishes "unknown" from a real zero, unlike the previous contract."""
        args = _make_args(time.time() - 3600)
        with remaining_time_sources():
            self.assertIsNone(job_module.get_remaining_time(args))


class TestDispatcherDictionaryRemainingTime(unittest.TestCase):
    """Tests for the remaining_time key in the acquire_jobs payload."""

    def _build(self, remaining_time):
        """Build the dispatcher dictionary with get_remaining_time() stubbed.

        Everything unrelated to remaining_time is stubbed out so the test does not depend
        on the worker node it happens to run on.

        Args:
            remaining_time: value that get_remaining_time() should return.

        Returns:
            dict: the dispatcher dictionary prepared for the acquire_jobs operation.
        """
        args = SimpleNamespace(
            queue='TEST_QUEUE',
            jobtype='',
            job_label='ptest',
            resource_type='',
            allow_same_user=False,
        )
        with patch('pilot.control.job.get_disk_space', return_value=100000), \
                patch('pilot.control.job.collect_workernode_info', return_value=(8000.0, 0, 0)), \
                patch('pilot.control.job.get_node_name', return_value='testnode'), \
                patch('pilot.control.job.get_job_label', return_value='ptest'), \
                patch('pilot.control.job.get_task_id', return_value=''), \
                patch.object(job_module.infosys, 'queuedata', SimpleNamespace(resource='TEST')), \
                patch('pilot.control.job.get_remaining_time', return_value=remaining_time):
            return job_module.get_dispatcher_dictionary(args)

    def test_included_when_positive(self):
        """A usable remaining time is sent so the server can filter on it."""
        data = self._build(12345)
        self.assertEqual(data['remaining_time'], 12345)

    def test_omitted_when_unavailable(self):
        """Nothing is sent when the remaining time cannot be determined."""
        self.assertNotIn('remaining_time', self._build(None))

    def test_omitted_when_zero(self):
        """A literal 0 must not leak into the payload -- omission means "no information"."""
        self.assertNotIn('remaining_time', self._build(0))

    def test_omitted_when_negative(self):
        """A passed limit must not be sent as a negative number either."""
        self.assertNotIn('remaining_time', self._build(-500))

    def test_other_payload_fields_are_unaffected(self):
        """Adding remaining_time must not disturb the rest of the payload."""
        data = self._build(12345)
        self.assertEqual(data['site_name'], 'TEST')
        self.assertEqual(data['computing_element'], 'TEST_QUEUE')
        self.assertEqual(data['node'], 'testnode')


if __name__ == '__main__':
    unittest.main()
