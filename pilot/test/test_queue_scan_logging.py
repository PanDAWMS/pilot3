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

"""Tests for the repeated queue scan messages in the pilot monitoring loop.

The pilot monitor iterates roughly every two seconds for the whole life of a
job, and every iteration calls get_timeinfo_from_job(), which logs the
maxwalltime decision and then calls scan_for_jobs(), which logged its result.
Both answers are the same on every iteration, so a job running for hours
produced thousands of identical pairs of lines:

    use_job_maxwalltime=False (harvester_submitmode='PULL', current job id=...)
    found 1 job(s) in queue monitored_payloads after 2.2649765014648438e-05 s
    - will begin queue monitoring

Observed on job 7313656511 at ANALY_CERN-PTEST, where the pair repeated well
over a thousand times and pushed the messages that do carry information out of
sight. Reported here:

- an unchanged result is reported once, not once per iteration;
- a change - the count, the queue, or jobs disappearing - is reported when it
  happens, which is the whole reason the line exists;
- a scan that had to wait is reported every time, since the waiting is itself
  the information;
- the maxwalltime decision follows the same rule, keyed on the submit mode and
  the job, so a multijob pilot reports it once per job.
"""

import logging
import os
import sys
import unittest
from collections import namedtuple
from queue import Queue
from unittest.mock import patch

from pilot.util import queuehandling
from pilot.util.queuehandling import (
    get_timeinfo_from_job,
    reset_queue_report_state,
    scan_for_jobs,
)

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class FakeJob:
    """Minimal stand-in for JobData carrying only what the time check reads."""

    def __init__(self, jobid="7313656511", maxwalltime=0, starttime=0):
        """Initialise the fake job.

        Args:
            jobid (str): PanDA job id.
            maxwalltime (int): Job definition maxWalltime in seconds.
            starttime (int): Job start time in epoch seconds.
        """
        self.jobid = jobid
        self.maxwalltime = maxwalltime
        self.starttime = starttime


def make_queues(**contents):
    """Return a queues named tuple holding the given job objects.

    Args:
        **contents (list): Queue name to list of job objects.

    Returns:
        Named tuple of Queue objects.
    """
    names = ["monitored_payloads", "validated_jobs", "completed_jobids", "messages"]
    holder = namedtuple("queues", names)
    queues = holder(**{name: Queue() for name in names})
    for name, jobs in contents.items():
        for job in jobs:
            getattr(queues, name).put(job)

    return queues


def fake_clock(values):
    """Return a time.time() stand-in walking through *values*.

    The last value is repeated, so a caller that reads the clock more often than
    expected gets a stable answer rather than a StopIteration.

    Args:
        values (list): Successive values to return, in seconds.

    Returns:
        Callable taking no arguments.
    """
    remaining = list(values)

    def _clock():
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    return _clock


class TestQueueScanReporting(unittest.TestCase):
    """An answer that never changes must not be logged once per loop iteration."""

    def setUp(self):
        """Clear the change detection state left by any earlier test."""
        reset_queue_report_state()

    def tearDown(self):
        """Leave no state behind for the next test."""
        reset_queue_report_state()

    def _scan_messages(self, queues, iterations=1):
        """Run the scan and return the messages it logged.

        Args:
            queues (namedtuple): Queues named tuple.
            iterations (int): Number of times the monitoring loop calls the scan.

        Returns:
            list: Logged messages.
        """
        with self.assertLogs("pilot.util.queuehandling", level="DEBUG") as captured:
            for _ in range(iterations):
                scan_for_jobs(queues)
            logging.getLogger("pilot.util.queuehandling").debug("end of test")

        return [line for line in captured.output if "end of test" not in line]

    def test_an_unchanged_result_is_reported_once(self):
        """Twenty iterations of the monitoring loop, one line."""
        queues = make_queues(monitored_payloads=[FakeJob()])

        messages = self._scan_messages(queues, iterations=20)

        self.assertEqual(len(messages), 1)
        self.assertIn("found 1 job(s) in queue monitored_payloads", messages[0])

    def test_a_changed_count_is_reported(self):
        """A second job appearing is exactly what the line is there to say."""
        queues = make_queues(monitored_payloads=[FakeJob()])
        first = self._scan_messages(queues, iterations=5)
        queues.monitored_payloads.put(FakeJob(jobid="7313656512"))
        second = self._scan_messages(queues, iterations=5)

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertIn("found 2 job(s)", second[0])

    def test_a_changed_queue_is_reported(self):
        """The same count in a different queue is a different state."""
        self._scan_messages(make_queues(monitored_payloads=[FakeJob()]), iterations=3)
        messages = self._scan_messages(make_queues(validated_jobs=[FakeJob()]), iterations=3)

        self.assertEqual(len(messages), 1)
        self.assertIn("in queue validated_jobs", messages[0])

    def test_jobs_disappearing_is_reported(self):
        """The old code never logged this case at all."""
        self._scan_messages(make_queues(monitored_payloads=[FakeJob()]), iterations=2)
        with patch.object(queuehandling.time, "sleep"), \
             patch.object(queuehandling.time, "time", side_effect=fake_clock([1000.0, 1000.0, 1031.0])):
            messages = self._scan_messages(make_queues(), iterations=1)

        self.assertEqual(len(messages), 1)
        self.assertIn("found no jobs", messages[0])

    def test_a_slow_scan_is_reported_even_when_unchanged(self):
        """A scan that had to wait means the queues were empty; that is news."""
        queues = make_queues(monitored_payloads=[FakeJob()])
        self._scan_messages(queues, iterations=1)
        slow = 1000.0 + queuehandling.SLOW_SCAN_THRESHOLD + 1
        with patch.object(queuehandling.time, "time",
                          side_effect=fake_clock([1000.0, 1000.0, slow])):
            messages = self._scan_messages(queues, iterations=1)

        self.assertEqual(len(messages), 1)

    def test_a_fast_unchanged_scan_stays_quiet(self):
        """The mutant of the test above: without the threshold it would log anyway."""
        queues = make_queues(monitored_payloads=[FakeJob()])
        self._scan_messages(queues, iterations=1)
        with patch.object(queuehandling.time, "time",
                          side_effect=fake_clock([1000.0, 1000.0, 1000.1])):
            messages = self._scan_messages(queues, iterations=1)

        self.assertEqual(messages, [])

    def test_the_duration_is_readable(self):
        """'after 2.2649765014648438e-05 s' is not a number anyone reads."""
        messages = self._scan_messages(make_queues(monitored_payloads=[FakeJob()]))

        self.assertIn("after 0.000 s", messages[0])
        self.assertNotIn("e-05", messages[0])

    def test_the_scan_still_returns_the_jobs(self):
        """Reporting less must not find less."""
        job = FakeJob()

        self.assertEqual(scan_for_jobs(make_queues(monitored_payloads=[job])), [job])


class TestMaxwalltimeReporting(unittest.TestCase):
    """The decision depends on the submit mode and the job, both fixed per job."""

    def setUp(self):
        """Clear the change detection state and pin the job id."""
        reset_queue_report_state()
        self._pandaid = os.environ.get("PANDAID")
        os.environ["PANDAID"] = "7313656511"

    def tearDown(self):
        """Restore the environment and the state."""
        if self._pandaid is None:
            os.environ.pop("PANDAID", None)
        else:
            os.environ["PANDAID"] = self._pandaid
        reset_queue_report_state()

    def _timeinfo_messages(self, queues, mode, iterations=1):
        """Run the time check and return the decision lines it logged.

        Args:
            queues (namedtuple): Queues named tuple.
            mode (str): Harvester submit mode.
            iterations (int): Number of monitoring loop iterations.

        Returns:
            list: Logged messages mentioning the decision.
        """
        with self.assertLogs("pilot.util.queuehandling", level="DEBUG") as captured:
            for _ in range(iterations):
                get_timeinfo_from_job(queues, {}, mode)
            logging.getLogger("pilot.util.queuehandling").debug("end of test")

        return [line for line in captured.output if "use_job_maxwalltime" in line]

    def test_the_decision_is_reported_once(self):
        """It cannot change while the pilot runs the same job in the same mode."""
        queues = make_queues(monitored_payloads=[FakeJob()])

        messages = self._timeinfo_messages(queues, "PULL", iterations=20)

        self.assertEqual(len(messages), 1)
        self.assertIn("use_job_maxwalltime=False", messages[0])

    def test_a_different_job_is_reported_again(self):
        """A multijob pilot must report the decision for every job it runs."""
        queues = make_queues(monitored_payloads=[FakeJob()])
        self._timeinfo_messages(queues, "PULL", iterations=3)
        os.environ["PANDAID"] = "7313656512"
        messages = self._timeinfo_messages(queues, "PULL", iterations=3)

        self.assertEqual(len(messages), 1)
        self.assertIn("7313656512", messages[0])

    def test_push_mode_is_reported_as_its_own_state(self):
        """The mode decides whether job.maxwalltime is used at all."""
        queues = make_queues(monitored_payloads=[FakeJob()])
        self._timeinfo_messages(queues, "PULL", iterations=2)
        messages = self._timeinfo_messages(queues, "push", iterations=2)

        self.assertEqual(len(messages), 1)
        self.assertIn("use_job_maxwalltime=True", messages[0])

    def test_the_walltime_is_still_returned_in_push_mode(self):
        """Reporting less must not decide differently."""
        queues = make_queues(monitored_payloads=[FakeJob(maxwalltime=3600, starttime=1000)])

        self.assertEqual(get_timeinfo_from_job(queues, {}, "push"), (3600, 1000))

    def test_the_walltime_is_not_used_in_pull_mode(self):
        """There maxWalltime is task level metadata, not the batch system's limit."""
        queues = make_queues(monitored_payloads=[FakeJob(maxwalltime=3600, starttime=1000)])

        self.assertEqual(get_timeinfo_from_job(queues, {}, "PULL"), (None, 1000))


class TestReportState(unittest.TestCase):
    """The state is module level, so it has to be resettable and thread safe."""

    def setUp(self):
        """Clear the change detection state."""
        reset_queue_report_state()

    def tearDown(self):
        """Clear the change detection state."""
        reset_queue_report_state()

    def test_a_reset_makes_the_next_report_happen(self):
        """Otherwise a test would see a message suppressed by an earlier one."""
        self.assertTrue(queuehandling.report_when_changed("scan", ("q", 1), "first"))
        self.assertFalse(queuehandling.report_when_changed("scan", ("q", 1), "second"))
        reset_queue_report_state()
        self.assertTrue(queuehandling.report_when_changed("scan", ("q", 1), "third"))

    def test_the_two_keys_do_not_shadow_each_other(self):
        """Both functions report into the same state and must not interfere."""
        self.assertTrue(queuehandling.report_when_changed("scan", ("q", 1), "scan"))
        self.assertTrue(queuehandling.report_when_changed("timeinfo", ("q", 1), "timeinfo"))

    def test_a_forced_report_happens_without_a_change(self):
        """This is what keeps a slow scan visible every time it is slow."""
        queuehandling.report_when_changed("scan", ("q", 1), "first")

        self.assertTrue(queuehandling.report_when_changed("scan", ("q", 1), "again", force=True))


if __name__ == "__main__":
    unittest.main()
