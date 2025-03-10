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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025

"""Unit test functions for time-outs."""

import unittest
from time import sleep
from pilot.util.auxiliary import TimeoutException
from pilot.util.timer import (
    timeout,
    TimedThread
)


def spend_time(t):
    """Function that simulates work by sleeping."""
    sleep(t)


class TestTimeoutFunction(unittest.TestCase):
    def test_function_times_out(self):
        """Test that the function times out correctly."""
        ctimeout = 1  # Timeout duration
        with self.assertRaises(TimeoutException):
            timeout(ctimeout, timer=TimedThread)(spend_time)(2)  # Exceeds timeout

    def test_function_completes_within_time(self):
        """Test that the function completes if within timeout limit."""
        ctimeout = 3  # Longer timeout
        try:
            timeout(ctimeout, timer=TimedThread)(spend_time)(1)  # Should not time out
        except TimeoutException:
            self.fail("TimeoutException was raised unexpectedly.")


if __name__ == "__main__":
    unittest.main()
