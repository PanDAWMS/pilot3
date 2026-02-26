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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

"""Unit test functions for the Analytics package."""

import unittest
import os

from pilot.api import analytics


class TestAnalytics(unittest.TestCase):
    """Unit tests for the Analytics package."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = analytics.Analytics()

    def test_linear_fit(self):
        """Make sure that a linear fit works."""
        self.assertIsInstance(self.client, analytics.Analytics)  # python 2.7

        x = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
        y = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

        fit = self.client.fit(x, y)
        slope = fit.slope()
        intersect = fit.intersect()
        self.assertEqual(type(slope), float)
        self.assertEqual(slope, 1.0)
        self.assertEqual(type(intersect), float)
        # intersect is the y value at the center of the x range (x_offset = mean(x) = 4.5),
        # not at x=0; for y=x this equals 4.5
        self.assertEqual(intersect, 4.5)
        # verify the model evaluates correctly at arbitrary points
        self.assertAlmostEqual(fit.value(0.0), 0.0)
        self.assertAlmostEqual(fit.value(4.5), 4.5)
        self.assertAlmostEqual(fit.value(9.0), 9.0)

        y = [0, -1, -2, -3, -4, -5, -6, -7, -8, -9]

        fit = self.client.fit(x, y)
        slope = fit.slope()

        self.assertEqual(slope, -1.0)

    def test_linear_fit_large_x_offset(self):
        """Verify intersect stability when x values are large (e.g. Unix timestamps)."""
        # Simulate 10 memory-monitor samples one second apart starting from a Unix timestamp.
        # The true relationship is y = 1.0 * x + C, but expressed in original coordinates.
        # With centering the fit should still recover slope=1.0 and value() should agree.
        x_offset = 1_700_000_000
        x = [x_offset + i for i in range(10)]
        y = [float(i) for i in range(10)]  # y = x - x_offset, slope=1, intersect(centered)=4.5

        fit = self.client.fit(x, y)
        self.assertAlmostEqual(fit.slope(), 1.0, places=10)
        # intersect is the y value at the center of the x range
        self.assertAlmostEqual(fit.intersect(), 4.5, places=10)
        # value() must give correct results in original coordinates
        self.assertAlmostEqual(fit.value(x_offset + 0), 0.0, places=10)
        self.assertAlmostEqual(fit.value(x_offset + 4.5), 4.5, places=10)
        self.assertAlmostEqual(fit.value(x_offset + 9), 9.0, places=10)

    def test_zero_slope(self):
        """Verify that a flat dataset (zero slope) is handled correctly."""
        x = [1, 2, 3, 4, 5]
        y = [7.0, 7.0, 7.0, 7.0, 7.0]  # perfectly flat: slope should be 0, intersect=7

        fit = self.client.fit(x, y)
        # slope must be 0.0, not None
        self.assertIsNotNone(fit.slope())
        self.assertEqual(fit.slope(), 0.0)
        self.assertAlmostEqual(fit.intersect(), 7.0)
        self.assertAlmostEqual(fit.value(3.0), 7.0)

    def est_parsing_memory_monitor_data(self):
        """Read and fit PSS vs Time from memory monitor output file."""
        # old MemoryMonitor format
        filename = 'pilot/test/resource/memory_monitor_output.txt'
        self.assertEqual(os.path.exists(filename), True)

        table = self.client.get_table(filename)

        self.assertEqual(type(table), dict)

        x = table['Time']
        y = table['PSS']  # old MemoryMonitor format
        fit = self.client.fit(x, y)

        slope = fit.slope()

        self.assertEqual(type(slope), float)
        self.assertGreater(slope, 0)


if __name__ == '__main__':
    unittest.main()
