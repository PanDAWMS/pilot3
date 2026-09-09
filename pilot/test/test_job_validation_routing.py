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

"""Unit tests for queue routing in pilot.control.job.validate().

A job must end up in exactly one of the validated_jobs and failed_jobs queues. Two defects are
covered:

1. A failed payload proxy download used to leave no trace in validate() at all - the download
   happened much later, inside the container command builder - so the job proceeded to
   validated_jobs and the payload ran under the pilot's own proxy.

2. The QUEUENOTSETUPFORCONTAINERS check ran *after* delayed_space_check(), which is the function
   that places the job in validated_jobs. A job on a queue without container_type set therefore
   ended up in both queues.

The tests drive validate() itself rather than a helper, since both defects were ordering and
control-flow problems that a helper-level test cannot see.
"""

import queue
import unittest
from collections import namedtuple
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes

errors = ErrorCodes()

Queues = namedtuple('Queues', ['jobs', 'validated_jobs', 'failed_jobs'])


class _OneShotStop:
    """graceful_stop stand-in that lets the validate() loop run exactly one iteration."""

    def __init__(self):
        self.calls = 0

    def is_set(self) -> bool:
        """Return False on the first call only."""
        self.calls += 1

        return self.calls > 1

    def set(self):
        """No-op."""


class _FakeQueuedata:
    """Minimal queuedata stand-in."""

    def __init__(self, queue_type: str = 'production', container_type: dict = None):
        self.type = queue_type
        self.container_type = container_type if container_type is not None else {'pilot': 'apptainer'}


class _FakeJob:
    """Minimal job stand-in exposing only what validate() reads."""

    def __init__(self, usecontainer: bool = True, container_type: dict = None):
        self.jobid = '7299340608'
        self.taskid = '2'
        self.workdir = ''
        self.usecontainer = usecontainer
        self.prodproxy = ''
        self.piloterrorcodes = []
        self.piloterrordiags = []
        self.piloterrordiag = ''
        self.infosys = MagicMock()
        self.infosys.queuedata = _FakeQueuedata(container_type=container_type)

    def is_analysis(self) -> bool:
        """Return True for a user analysis job."""
        return True


def _run_validate(job, prerequisites_result, queuedata=None):
    """Run one iteration of validate() and return the (validated, failed) queue sizes.

    Args:
        job: job object to feed into the jobs queue.
        prerequisites_result: value verify_job_prerequisites() should return.
        queuedata: queuedata to expose through the pilot cache.

    Returns:
        tuple[int, int, object]: number of jobs in validated_jobs, in failed_jobs, and traces.
    """
    from pilot.control import job as job_module

    queues = Queues(jobs=queue.Queue(), validated_jobs=queue.Queue(), failed_jobs=queue.Queue())
    queues.jobs.put(job)

    args = MagicMock()
    args.graceful_stop = _OneShotStop()
    args.verify_proxy = True
    args.pod = True
    args.mainworkdir = '/tmp'
    args.harvester_submitmode = 'pull'
    args.update_server = False

    traces = MagicMock()
    traces.pilot = {'nr_jobs': 0, 'error_code': 0}

    cache = MagicMock()
    cache.queuedata = queuedata if queuedata is not None else _FakeQueuedata()

    errors.reset_pilot_errors()

    with patch.object(job_module, '_validate_job', return_value=True), \
         patch.object(job_module, 'verify_job_prerequisites', return_value=prerequisites_result), \
         patch.object(job_module, 'hide_secrets'), \
         patch.object(job_module, 'create_symlink'), \
         patch.object(job_module, 'create_k8_link'), \
         patch.object(job_module, 'threads_aborted', return_value=False), \
         patch.object(job_module, 'pilot_cache', cache), \
         patch.object(job_module.os, 'setpgrp'), \
         patch.object(job_module.os, 'mkdir'), \
         patch.object(job_module.os, 'chmod'), \
         patch.object(job_module.time, 'sleep'):
        job_module.validate(queues, traces, args)

    return queues.validated_jobs.qsize(), queues.failed_jobs.qsize(), traces


class TestValidateRouting(unittest.TestCase):
    """validate() must place a job in exactly one queue."""

    def tearDown(self):
        """Reset the class-level error code lists."""
        errors.reset_pilot_errors()

    def test_healthy_job_goes_to_validated_only(self):
        """With all prerequisites met, the job must go to validated_jobs only."""
        validated, failed, _ = _run_validate(_FakeJob(), (0, ''))

        self.assertEqual(validated, 1)
        self.assertEqual(failed, 0)

    def test_proxy_failure_goes_to_failed_only(self):
        """A payload proxy failure must fail the job without reaching validated_jobs.

        This is the reported case: the payload must not be executed.
        """
        diagnostics = "failed to download proxy from server for role='atlas'"
        validated, failed, _ = _run_validate(
            _FakeJob(), (errors.PAYLOADPROXYDOWNLOADFAILURE, diagnostics)
        )

        self.assertEqual(failed, 1)
        self.assertEqual(validated, 0, 'the job must not be queued for payload execution')

    def test_proxy_failure_records_the_error_code(self):
        """The dedicated error code must be recorded on the job and in traces."""
        diagnostics = "failed to download proxy from server for role='atlas'"
        job = _FakeJob()
        _, _, traces = _run_validate(job, (errors.PAYLOADPROXYDOWNLOADFAILURE, diagnostics))

        self.assertEqual(job.piloterrorcodes[0], errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertEqual(traces.pilot['error_code'], errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertEqual(job.piloterrordiag, diagnostics)

    def test_reported_error_code_is_an_int(self):
        """Only integers may be reported to the server."""
        job = _FakeJob()
        _run_validate(job, (errors.PAYLOADPROXYDOWNLOADFAILURE, 'diag'))

        self.assertIsInstance(job.piloterrorcodes[0], int)

    def test_container_misconfiguration_goes_to_failed_only(self):
        """A queue without container_type must fail the job and not also validate it.

        Previously this check ran after delayed_space_check(), so the job was placed in both
        validated_jobs and failed_jobs.
        """
        msg = 'container_type must be set in CRIC'
        validated, failed, _ = _run_validate(
            _FakeJob(container_type={}), (errors.QUEUENOTSETUPFORCONTAINERS, msg)
        )

        self.assertEqual(failed, 1)
        self.assertEqual(validated, 0, 'the job must not be placed in both queues')


class TestVerifyJobPrerequisites(unittest.TestCase):
    """verify_job_prerequisites() must gate on both the proxy and the container setup."""

    def tearDown(self):
        """Reset the class-level error code lists."""
        errors.reset_pilot_errors()

    def _args(self, verify_proxy: bool = True):
        """Return a minimal args stand-in."""
        args = MagicMock()
        args.verify_proxy = verify_proxy

        return args

    def test_proxy_failure_is_propagated(self):
        """A non-zero exit code from handle_proxy() must be returned unchanged."""
        from pilot.control import job as job_module

        diagnostics = "failed to download proxy from server for role='atlas'"
        with patch.object(job_module, 'handle_proxy',
                          return_value=(errors.PAYLOADPROXYDOWNLOADFAILURE, diagnostics)):
            exit_code, diag = job_module.verify_job_prerequisites(_FakeJob(), self._args())

        self.assertEqual(exit_code, errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertEqual(diag, diagnostics)

    def test_proxy_handling_skipped_when_verification_disabled(self):
        """--no-verify-proxy must skip the proxy handling entirely."""
        from pilot.control import job as job_module

        cache = MagicMock()
        cache.queuedata = _FakeQueuedata()
        with patch.object(job_module, 'handle_proxy') as mock_handle, \
             patch.object(job_module, 'pilot_cache', cache):
            exit_code, _ = job_module.verify_job_prerequisites(
                _FakeJob(), self._args(verify_proxy=False)
            )

        self.assertEqual(exit_code, 0)
        mock_handle.assert_not_called()

    def test_missing_container_type_is_caught(self):
        """A container job on a queue without container_type must be rejected."""
        from pilot.control import job as job_module

        cache = MagicMock()
        cache.queuedata = _FakeQueuedata(container_type={})
        with patch.object(job_module, 'handle_proxy', return_value=(0, '')), \
             patch.object(job_module, 'pilot_cache', cache):
            exit_code, diagnostics = job_module.verify_job_prerequisites(
                _FakeJob(), self._args()
            )

        self.assertEqual(exit_code, errors.QUEUENOTSETUPFORCONTAINERS)
        self.assertIn('CRIC', diagnostics)

    def test_non_container_job_is_not_rejected(self):
        """A job that does not use a container must not be affected by container_type."""
        from pilot.control import job as job_module

        cache = MagicMock()
        cache.queuedata = _FakeQueuedata(container_type={})
        with patch.object(job_module, 'handle_proxy', return_value=(0, '')), \
             patch.object(job_module, 'pilot_cache', cache):
            exit_code, _ = job_module.verify_job_prerequisites(
                _FakeJob(usecontainer=False), self._args()
            )

        self.assertEqual(exit_code, 0)

    def test_healthy_job_passes(self):
        """With a working proxy and a configured queue, the job must pass."""
        from pilot.control import job as job_module

        cache = MagicMock()
        cache.queuedata = _FakeQueuedata()
        with patch.object(job_module, 'handle_proxy', return_value=(0, '')), \
             patch.object(job_module, 'pilot_cache', cache):
            exit_code, diagnostics = job_module.verify_job_prerequisites(
                _FakeJob(), self._args()
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(diagnostics, '')


class TestHandleProxy(unittest.TestCase):
    """handle_proxy() must cover both the unified and the payload proxy paths."""

    def tearDown(self):
        """Reset the class-level error code lists."""
        errors.reset_pilot_errors()

    def test_payload_proxy_failure_is_propagated(self):
        """A payload proxy failure from the user plugin must be returned."""
        from pilot.control import job as job_module

        job = _FakeJob()
        job.infosys.queuedata = _FakeQueuedata(queue_type='production')
        diagnostics = "failed to download proxy from server for role='atlas'"

        with patch.dict('os.environ', {'PILOT_USER': 'atlas'}, clear=False), \
             patch('pilot.user.atlas.proxy.handle_payload_proxy',
                   return_value=(errors.PAYLOADPROXYDOWNLOADFAILURE, diagnostics)):
            exit_code, diag = job_module.handle_proxy(job)

        self.assertEqual(exit_code, errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertEqual(diag, diagnostics)

    def test_payload_proxy_success_passes(self):
        """A successful payload proxy download must let the job proceed."""
        from pilot.control import job as job_module

        job = _FakeJob()
        job.infosys.queuedata = _FakeQueuedata(queue_type='production')

        with patch.dict('os.environ', {'PILOT_USER': 'atlas'}, clear=False), \
             patch('pilot.user.atlas.proxy.handle_payload_proxy', return_value=(0, '')):
            exit_code, diagnostics = job_module.handle_proxy(job)

        self.assertEqual(exit_code, 0)
        self.assertEqual(diagnostics, '')

    def test_unified_dispatch_failure_is_now_fatal(self):
        """A failed unified dispatch proxy download must no longer be ignored.

        It previously logged a warning and continued with the production proxy.
        """
        from pilot.control import job as job_module

        job = _FakeJob()
        job.infosys.queuedata = _FakeQueuedata(queue_type='unified')

        with patch.object(job_module, 'download_new_proxy', return_value=errors.NOVOMSPROXY):
            exit_code, diagnostics = job_module.handle_proxy(job)

        self.assertEqual(exit_code, errors.NOVOMSPROXY)
        self.assertTrue(diagnostics)

    def test_plugin_exception_does_not_fail_the_job(self):
        """A broken plugin hook must not fail an otherwise healthy job."""
        from pilot.control import job as job_module

        job = _FakeJob()
        job.infosys.queuedata = _FakeQueuedata(queue_type='production')

        with patch.dict('os.environ', {'PILOT_USER': 'atlas'}, clear=False), \
             patch('pilot.user.atlas.proxy.handle_payload_proxy',
                   side_effect=AttributeError('no such hook')):
            exit_code, _ = job_module.handle_proxy(job)

        self.assertEqual(exit_code, 0)


if __name__ == '__main__':
    unittest.main()
