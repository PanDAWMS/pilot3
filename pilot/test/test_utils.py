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

"""Unit tests for pilot utils."""

import os
import unittest

from pilot.info import infosys
from pilot.util.workernode import (
    collect_workernode_info,
    get_disk_space
)


def check_env() -> bool:
    """
    Check whether cvmfs is available.

    To be used to decide whether to skip some test functions.

    :returns: True if not a Mac, otherwise False (bool).
    """
    is_mac = os.environ.get('MACOSX') == 'true' or not os.path.exists('/proc/meminfo')
    return not is_mac
    # return os.path.exists('/cvmfs/atlas.cern.ch/repo/')


@unittest.skipIf(not check_env(), "This unit test is broken")
class TestUtils(unittest.TestCase):
    """Unit tests for utils functions."""

    def setUp(self):
        """Set up test fixtures."""
        infosys.init("CERN")

    def test_collect_workernode_info(self):
        """Make sure that collect_workernode_info() returns the proper types (float, float, float)."""
        mem, cpu, disk = collect_workernode_info(path=os.getcwd())

        self.assertEqual(type(mem), float)
        self.assertEqual(type(cpu), float)
        self.assertEqual(type(disk), float)

        self.assertNotEqual(mem, 0.0)
        self.assertNotEqual(cpu, 0.0)
        self.assertNotEqual(disk, 0.0)

    def test_get_disk_space(self):
        """Verify that get_disk_space() returns the proper type (int)."""
        #queuedata = {'maxwdir': 123456789}

        diskspace = get_disk_space(infosys.queuedata)  ## FIX ME LATER

        self.assertEqual(type(diskspace), int)


if __name__ == '__main__':
    unittest.main()
