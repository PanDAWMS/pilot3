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

"""Unit tests for the target_architecture field in the acquire_jobs payload.

The GPU brokerage only guarantees that some worker node in a queue has the requested GPU
vendor, model and microarchitecture, so a job could be dispatched to an incompatible node
of a mixed queue (the V100 vs A100 case) and fail there. The pilot therefore reports the
GPU specifications of the node it is running on, taken from the GPU map it collected at
startup, and the server matches them against the hardware requirements of the task.

Covers:
- get_target_architecture(): the GPU map is passed through unchanged, wrapped in the gpus
  list; nothing is reported when the map is missing, empty, unreadable or malformed, or
  when PILOT_HOME is not set.
- get_dispatcher_dictionary(): target_architecture present in the payload on a GPU worker
  node and absent otherwise, and the rest of the payload unaffected.
- The payload shape agreed with the server side (pandaserver pilot_api_tests.py).
"""

import logging
import os
import sys
import unittest
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from pilot.common.exception import FileHandlingFailure
from pilot.control import job as job_module
from pilot.util.config import config
from pilot.util.filehandling import write_json
from pilot.util.workernode import get_target_architecture

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)

# a real gpu map, as written by the pilot on a CERN-PROD H100 worker node
GPU_MAP = {
    "architecture": "Hopper",
    "count": 1,
    "driver_version": "610.43.02",
    "framework": "CUDA",
    "framework_version": "13.3",
    "host_name": "b9pgpun001.cern.ch",
    "model": "NVIDIA H100 NVL",
    "site": "CERN-PROD",
    "vendor": "NVIDIA",
    "vram": 95830,
}


class TestGetTargetArchitecture(unittest.TestCase):
    """Tests for get_target_architecture()."""

    def setUp(self):
        """Give each test its own PILOT_HOME so that no gpu map leaks between tests."""
        self._tmpdir = TemporaryDirectory()  # pylint: disable=consider-using-with
        self._previous_pilot_home = os.environ.get('PILOT_HOME')
        os.environ['PILOT_HOME'] = self._tmpdir.name
        self.path = os.path.join(self._tmpdir.name, config.Workernode.gpu_map)

    def tearDown(self):
        """Restore PILOT_HOME and remove the temporary directory."""
        if self._previous_pilot_home is None:
            os.environ.pop('PILOT_HOME', None)
        else:
            os.environ['PILOT_HOME'] = self._previous_pilot_home
        self._tmpdir.cleanup()

    def test_gpu_map_is_reported_unchanged(self):
        """The keys and values must be identical to those of the GPU worker node map.

        The server matches the reported specifications against what it already has on
        record from update_worker_node_gpu, so the pilot must not rewrite or filter them.
        """
        write_json(self.path, GPU_MAP)
        self.assertEqual(get_target_architecture(), {'gpus': [GPU_MAP]})

    def test_nothing_is_reported_without_a_gpu_map(self):
        """A worker node with no GPU has no map, and an absent key means "not reported".

        This is not the same as an empty gpus list, which would assert that the node has
        no GPU and would make the server refuse GPU jobs for it.
        """
        self.assertEqual(get_target_architecture(), {})

    def test_nothing_is_reported_for_an_empty_gpu_map(self):
        """An empty map means the information was lost, not that there is no GPU."""
        write_json(self.path, {})
        self.assertEqual(get_target_architecture(), {})

    def test_nothing_is_reported_for_an_unreadable_gpu_map(self):
        """A read failure must not propagate: job acquisition has to continue."""
        write_json(self.path, GPU_MAP)
        with patch('pilot.util.workernode.read_json', side_effect=FileHandlingFailure('denied')):
            self.assertEqual(get_target_architecture(), {})

    def test_nothing_is_reported_for_a_corrupt_gpu_map(self):
        """read_json() returns None for a file that does not parse."""
        with open(self.path, 'w', encoding='utf-8') as _file:
            _file.write('{not json')
        self.assertEqual(get_target_architecture(), {})

    def test_nothing_is_reported_for_an_unexpected_gpu_map_format(self):
        """A map that is not a dictionary must not be wrapped and sent on."""
        write_json(self.path, [GPU_MAP])
        self.assertEqual(get_target_architecture(), {})

    def test_nothing_is_reported_without_pilot_home(self):
        """The gpu map cannot be located if PILOT_HOME is not set."""
        del os.environ['PILOT_HOME']
        self.assertEqual(get_target_architecture(), {})


class TestDispatcherDictionaryTargetArchitecture(unittest.TestCase):
    """Tests for the target_architecture key in the acquire_jobs payload."""

    @staticmethod
    def _build(target_architecture):
        """Build the dispatcher dictionary with get_target_architecture() stubbed.

        Everything unrelated to the target architecture is stubbed out so that the test
        does not depend on the worker node it happens to run on.

        Args:
            target_architecture (dict): value that get_target_architecture() should return.

        Returns:
            dict: the dispatcher dictionary prepared for the acquire_jobs operation.
        """
        args = SimpleNamespace(
            queue='CERN-PROD_GPU',
            jobtype='unified',
            job_label='unified',
            resource_type='SCORE',
            allow_same_user=False,
            send_remaining_time=False,
        )
        with patch('pilot.control.job.get_disk_space', return_value=160000), \
                patch('pilot.control.job.collect_workernode_info', return_value=(471616.0, 0, 0)), \
                patch('pilot.control.job.get_node_name', return_value='b9pgpun001.cern.ch'), \
                patch('pilot.control.job.get_job_label', return_value='unified'), \
                patch('pilot.control.job.get_task_id', return_value=''), \
                patch.object(job_module.infosys, 'queuedata', SimpleNamespace(resource='CERN-PROD')), \
                patch('pilot.control.job.get_target_architecture', return_value=target_architecture):
            return job_module.get_dispatcher_dictionary(args)

    def test_included_on_a_gpu_worker_node(self):
        """The dispatcher needs the specifications of the node that is asking for a job."""
        data = self._build({'gpus': [GPU_MAP]})
        self.assertEqual(data['target_architecture'], {'gpus': [GPU_MAP]})

    def test_omitted_when_no_gpu_information_is_available(self):
        """Omission leaves the server's GPU requirement check disabled, as before."""
        self.assertNotIn('target_architecture', self._build({}))

    def test_empty_gpu_list_is_never_sent(self):
        """An empty list would claim the node has no GPU, which the pilot cannot know."""
        self.assertNotIn('target_architecture', self._build({'gpus': []}))

    def test_payload_shape_matches_the_server_api(self):
        """The nesting must match what acquire_jobs() expects, i.e. gpus is a list of dicts.

        Mirrors pandaserver/api/v1/tests/pilot_api_tests.py, where target_architecture is a
        dictionary whose gpus key holds one dictionary per GPU.
        """
        target_architecture = self._build({'gpus': [GPU_MAP]})['target_architecture']
        self.assertIsInstance(target_architecture, dict)
        self.assertIsInstance(target_architecture['gpus'], list)
        gpu = target_architecture['gpus'][0]
        for key in ('vendor', 'model', 'vram', 'architecture', 'framework_version', 'driver_version'):
            self.assertIn(key, gpu)
        self.assertEqual(gpu['vendor'], 'NVIDIA')
        self.assertIsInstance(gpu['vram'], int)

    def test_other_payload_fields_are_unaffected(self):
        """Adding target_architecture must not disturb the rest of the payload."""
        data = self._build({'gpus': [GPU_MAP]})
        self.assertEqual(data['site_name'], 'CERN-PROD')
        self.assertEqual(data['computing_element'], 'CERN-PROD_GPU')
        self.assertEqual(data['node'], 'b9pgpun001.cern.ch')
        self.assertEqual(data['resource_type'], 'SCORE')


if __name__ == '__main__':
    unittest.main()
