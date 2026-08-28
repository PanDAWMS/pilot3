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
- _target_architecture_was_rejected(): the refusals in which the server names the field, as
  opposed to a plain "no jobs" answer, which is not by itself proof of anything.
- get_job_definition_from_server(): the retry without the field on any response that carries
  no job, and when the refusal is remembered for the remainder of the pilot's lifetime.
- no_target_architecture in PQ.catchall as the per-queue off switch.
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

    def setUp(self):
        """Start each test with the rejection flag cleared, since it is class-level state."""
        job_module.pilot_cache.target_architecture_rejected = False

    def tearDown(self):
        """Leave the flag cleared for the tests that follow."""
        job_module.pilot_cache.target_architecture_rejected = False

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

    def test_omitted_after_the_server_has_rejected_it(self):
        """Once refused, the field is not sent again for the rest of the pilot's lifetime."""
        job_module.pilot_cache.target_architecture_rejected = True
        self.assertNotIn('target_architecture', self._build({'gpus': [GPU_MAP]}))

    def test_omitted_when_disabled_for_the_queue(self):
        """no_target_architecture in PQ.catchall switches the field off without a release."""
        args = SimpleNamespace(
            queue='CERN-PROD_GPU',
            jobtype='unified',
            job_label='unified',
            resource_type='SCORE',
            allow_same_user=False,
            send_remaining_time=False,
        )
        queuedata = SimpleNamespace(resource='CERN-PROD', catchall='no_target_architecture')
        with patch('pilot.control.job.get_disk_space', return_value=160000), \
                patch('pilot.control.job.collect_workernode_info', return_value=(471616.0, 0, 0)), \
                patch('pilot.control.job.get_node_name', return_value='b9pgpun001.cern.ch'), \
                patch('pilot.control.job.get_job_label', return_value='unified'), \
                patch('pilot.control.job.get_task_id', return_value=''), \
                patch.object(job_module.infosys, 'queuedata', queuedata), \
                patch('pilot.control.job.get_target_architecture', return_value={'gpus': [GPU_MAP]}):
            data = job_module.get_dispatcher_dictionary(args)
        self.assertNotIn('target_architecture', data)


class TestTargetArchitectureWasRejected(unittest.TestCase):
    """Tests for the classification of acquire_jobs responses.

    A failed request and an empty queue have the same response shape, so the pilot has to tell
    them apart by the message. Retrying without the field on an empty queue would obtain
    exactly the job that the field exists to avoid.
    """

    def test_argument_error_from_a_server_without_support(self):
        """The API validates the request against its own signature before running it."""
        response = {
            'success': False,
            'message': "Argument error: got an unexpected keyword argument 'target_architecture'",
            'data': None,
        }
        self.assertTrue(job_module._target_architecture_was_rejected(response))

    def test_parse_error_reported_by_the_server(self):
        """acquire_jobs() rejects a target architecture it cannot parse."""
        response = {'success': False, 'message': 'failed to parse target_architecture with ...'}
        self.assertTrue(job_module._target_architecture_was_rejected(response))

    def test_type_error_reported_by_the_server(self):
        """The type check of the API decorator is reported the same way."""
        response = {'success': False, 'message': "Type error: 'target_architecture' with value ..."}
        self.assertTrue(job_module._target_architecture_was_rejected(response))

    def test_no_jobs_is_not_an_explicit_rejection(self):
        """The server did not name the field, so a job has to prove that it was the cause."""
        response = {'success': False, 'message': 'No jobs in PanDA'}
        self.assertFalse(job_module._target_architecture_was_rejected(response))

    def test_unrelated_failure_is_not_an_explicit_rejection(self):
        """A timeout says nothing about the target architecture."""
        response = {'success': False, 'message': 'Timed out'}
        self.assertFalse(job_module._target_architecture_was_rejected(response))

    def test_successful_response_is_not_a_rejection(self):
        """A job was returned, so nothing has to be retried."""
        response = {'success': True, 'message': '', 'data': {'StatusCode': 0, 'jobs': [{}]}}
        self.assertFalse(job_module._target_architecture_was_rejected(response))

    def test_non_dictionary_response_is_not_a_rejection(self):
        """A transport failure is handled by the existing curl fallback, not by this one."""
        self.assertFalse(job_module._target_architecture_was_rejected('failed to send request: ...'))
        self.assertFalse(job_module._target_architecture_was_rejected(None))


class TestAcquireJobsFallback(unittest.TestCase):
    """Tests for the retry without the target architecture in get_job_definition_from_server()."""

    def setUp(self):
        """Start each test with the rejection flag cleared, since it is class-level state."""
        job_module.pilot_cache.target_architecture_rejected = False

    def tearDown(self):
        """Leave the flag cleared for the tests that follow."""
        job_module.pilot_cache.target_architecture_rejected = False

    @staticmethod
    def _run(responses, payload):
        """Call get_job_definition_from_server() with a scripted sequence of responses.

        Args:
            responses (list): responses that _acquire_jobs() should return, in order.
            payload (dict): dispatcher dictionary that get_dispatcher_dictionary() should return.

        Returns:
            tuple: (final response, list of payloads that _acquire_jobs() was called with).
        """
        sent = []

        def _fake_acquire_jobs(_cmd, data):
            sent.append(dict(data))
            return responses[len(sent) - 1]

        args = SimpleNamespace(url='https://pandaserver.cern.ch', port=25443)
        with patch('pilot.control.job.get_dispatcher_dictionary', return_value=payload), \
                patch('pilot.control.job.https.get_server_command',
                      return_value='https://pandaserver.cern.ch:25443/api/v1/pilot/acquire_jobs'), \
                patch('pilot.control.job._acquire_jobs', side_effect=_fake_acquire_jobs):
            res = job_module.get_job_definition_from_server(args)

        return res, sent

    @staticmethod
    def _payload():
        """Return a dispatcher dictionary carrying a target architecture.

        Returns:
            dict: minimal payload for a GPU worker node.
        """
        return {'site_name': 'CERN-PROD', 'target_architecture': {'gpus': [GPU_MAP]}}

    def test_retried_without_the_field_when_explicitly_rejected(self):
        """The queue must not be left without jobs by a server that refuses the field."""
        rejection = {
            'success': False,
            'message': "Argument error: got an unexpected keyword argument 'target_architecture'",
        }
        job = {'success': True, 'message': '', 'data': {'StatusCode': 0, 'jobs': [{'PandaID': 1}]}}
        res, sent = self._run([rejection, job], self._payload())
        self.assertEqual(res, job)
        self.assertEqual(len(sent), 2)
        self.assertIn('target_architecture', sent[0])
        self.assertNotIn('target_architecture', sent[1])
        self.assertEqual(sent[1]['site_name'], 'CERN-PROD')

    def test_retried_without_the_field_when_no_job_is_returned(self):
        """A server can also withhold jobs while reporting nothing but an empty queue.

        This is the case the GPU queues ran into: the response is indistinguishable from an
        idle queue, so the pilot has to try without the field to find out.
        """
        no_jobs = {'success': False, 'message': 'No jobs in PanDA'}
        job = {'success': True, 'message': '', 'data': {'StatusCode': 0, 'jobs': [{'PandaID': 1}]}}
        res, sent = self._run([no_jobs, job], self._payload())
        self.assertEqual(res, job)
        self.assertEqual(len(sent), 2)
        self.assertIn('target_architecture', sent[0])
        self.assertNotIn('target_architecture', sent[1])

    def test_explicit_rejection_is_remembered(self):
        """A refusal that names the field settles the matter without further evidence."""
        rejection = {'success': False, 'message': 'failed to parse target_architecture with ...'}
        no_jobs = {'success': False, 'message': 'No jobs in PanDA'}
        self._run([rejection, no_jobs], self._payload())
        self.assertTrue(job_module.pilot_cache.target_architecture_rejected)

    def test_field_abandoned_once_a_job_arrives_without_it(self):
        """A job that appears as soon as the field is dropped proves what withheld it."""
        no_jobs = {'success': False, 'message': 'No jobs in PanDA'}
        job = {'success': True, 'message': '', 'data': {'StatusCode': 0, 'jobs': [{'PandaID': 1}]}}
        self._run([no_jobs, job], self._payload())
        self.assertTrue(job_module.pilot_cache.target_architecture_rejected)

    def test_field_kept_when_the_queue_is_simply_empty(self):
        """No job either way means nothing was learned, so the field is reported again."""
        no_jobs = {'success': False, 'message': 'No jobs in PanDA'}
        res, sent = self._run([no_jobs, no_jobs], self._payload())
        self.assertEqual(res, no_jobs)
        self.assertEqual(len(sent), 2)
        self.assertFalse(job_module.pilot_cache.target_architecture_rejected)

    def test_no_retry_when_the_payload_has_no_target_architecture(self):
        """Nothing to strip on a worker node that does not report GPUs."""
        no_jobs = {'success': False, 'message': 'No jobs in PanDA'}
        res, sent = self._run([no_jobs], {'site_name': 'CERN-PROD'})
        self.assertEqual(res, no_jobs)
        self.assertEqual(len(sent), 1)

    def test_no_retry_when_a_job_was_returned(self):
        """The field did no harm, so it must keep being reported."""
        job = {'success': True, 'message': '', 'data': {'StatusCode': 0, 'jobs': [{'PandaID': 1}]}}
        res, sent = self._run([job], self._payload())
        self.assertEqual(res, job)
        self.assertEqual(len(sent), 1)
        self.assertFalse(job_module.pilot_cache.target_architecture_rejected)

    def test_no_retry_on_a_transport_failure(self):
        """A failure to reach the server says nothing about the payload."""
        res, sent = self._run(['failed to send request: timeout'], self._payload())
        self.assertEqual(res, 'failed to send request: timeout')
        self.assertEqual(len(sent), 1)

    def test_no_request_without_a_server_command(self):
        """An empty command means no server to ask."""
        args = SimpleNamespace(url='', port=25443)
        with patch('pilot.control.job.get_dispatcher_dictionary', return_value={}), \
                patch('pilot.control.job.https.get_server_command', return_value=''), \
                patch('pilot.control.job._acquire_jobs') as mock_acquire:
            self.assertEqual(job_module.get_job_definition_from_server(args), {})
        mock_acquire.assert_not_called()


if __name__ == '__main__':
    unittest.main()
