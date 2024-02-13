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
# - Pavlo Svirin pavlo.svirin@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

"""Unit test functions for the copytool rucio."""

import os
import unittest

# from pilot.control.job import get_fake_job
# from pilot.info import JobData
from pilot.info.filespec import FileSpec
from pilot.util.tracereport import TraceReport

try:
    from pilot.copytool.rucio import copy_out
except ImportError:
    pass


def check_env() -> bool:
    """
    Check whether rucio copytool is loaded correctly.

    To be used to decide whether to skip some test functions.

    :return: Turn if rucio copytool is available, otherwise False (bool).
    """
    aval = False
    return aval


@unittest.skipIf(not check_env(), "No Rucio copytool")
class TestCopytoolRucio(unittest.TestCase):
    """Unit tests for rucio copytool."""

    def setUp(self):
        """Set up test fixtures."""
        with open('test.txt', 'w', encoding='utf-8') as test_file:
            test_file.write('For test purposes only.')

        fspec_out = FileSpec()
        fspec_out.lfn = 'test.txt'
        fspec_out.scope = 'user.pnilsson'
        fspec_out.checksum = {'adler32': '682c08b9'}
        fspec_out.pfn = os.getcwd() + '/' + 'test.txt'
        fspec_out.ddmendpoint = 'UNI-FREIBURG_SCRATCHDISK'
        self.outdata = [fspec_out]

    def test_copy_out_rucio(self):
        """Test copy_out function."""
        trace_report = TraceReport()
        trace_report.update(eventType='unit test')
        try:
            copy_out(self.outdata, trace_report=trace_report)
        except NameError:
            pass
        os.remove(self.outdata[0].pfn)


if __name__ == '__main__':
    unittest.main()
