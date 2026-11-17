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

"""Tests for the routing of Rucio traces (bulk traces, phase 2).

Every trace sent by a copytool now goes, in this order:

1. to the spool file, inside a middleware container script; the pilot sends these traces after the
   container has finished;
2. to the background sender of the job, if there is one (created at the stage-in and stage-out
   entry points);
3. otherwise to the trace server at once, as before.

A trace arriving after the sender has finished (final server update) is sent at once instead of
being dropped. The stub trace server of ``test_bulk_traces`` is used.
"""

import ast
import json
import os
import types
import unittest
from unittest.mock import MagicMock, patch

from pilot.common.exception import PilotException
from pilot.control import data as data_control
from pilot.copytool import rucio as rucio_copytool
from pilot.test.test_bulk_traces import _StubServerTestCase
from pilot.util import middleware, tracereport
from pilot.util.config import config
from pilot.util.tracereport import (
    DEFAULT_TRACE_SPOOL_FILE,
    TraceReport,
    defer_traces,
    enable_trace_spool,
    get_trace_spool_path,
    read_trace_spool,
    reset_trace_routing,
    send_traces,
    spool_traces,
)
from pilot.util.tracesender import (
    collect_spooled_traces,
    get_or_create_sender,
    get_sender,
)

JOB_ID = '7380000001'


class _RoutingTestCase(_StubServerTestCase):
    """Stub server test case with a job, and with the spool file turned off before and after each test."""

    def setUp(self):
        """Create the job and turn the spool file off."""
        super().setUp()
        reset_trace_routing()
        self.job = MagicMock(jobid=JOB_ID, workdir=self.workdir)
        self.job.infosys.queuedata.catchall = ''

    def tearDown(self):
        """Turn the spool file off."""
        reset_trace_routing()
        super().tearDown()

    def job_traces(self, n: int, job_id: str = JOB_ID) -> list:
        """Return n valid trace reports of a job."""
        traces = self.traces(n)
        for trace in traces:
            trace['appid'] = job_id
        return traces

    @property
    def spool_path(self) -> str:
        """Return the path of the spool file in the work directory."""
        return get_trace_spool_path(self.workdir)

    def spooled_lines(self) -> list:
        """Return the lines of the spool file ([] if there is none)."""
        if not os.path.exists(self.spool_path):
            return []
        with open(self.spool_path, encoding='utf-8') as spool:
            return spool.read().splitlines()


# ---------------------------------------------------------------------------
# routing order: spool file, background sender, at once
# ---------------------------------------------------------------------------


class TestRoutingOrder(_RoutingTestCase):
    """TraceReport.send() routes a trace to the spool file, the job's sender, or the server."""

    def test_no_sender_sends_at_once(self):
        """Without a sender (e.g. the HPC workflow) the trace is sent at once, as before."""
        self.assertTrue(self.job_traces(1)[0].send())
        self.assertEqual(len(self.state.requests), 1)

    def test_sender_takes_the_trace(self):
        """With a sender, send() only queues the trace."""
        sender = get_or_create_sender(self.job)
        self.state.delay = 0.5
        self.assertTrue(self.job_traces(1)[0].send())
        self.assertEqual(self.state.requests, [])
        summary = sender.finish()
        self.assertEqual((summary.sent, summary.lost), (1, 0))
        self.assertEqual(len(self.received), 1)

    def test_sender_of_another_job_is_not_used(self):
        """Only the sender of the trace's own job (appid) is used."""
        sender = get_or_create_sender(self.job)
        self.job_traces(1, job_id='999')[0].send()
        self.assertEqual(len(self.state.requests), 1)
        self.assertEqual(sender.finish().sent, 0)

    def test_spool_comes_first(self):
        """In a middleware container script the trace goes to the spool file, even if there is a sender."""
        sender = get_or_create_sender(self.job)
        enable_trace_spool(self.workdir)
        self.assertTrue(self.job_traces(1)[0].send())
        self.assertEqual(len(self.spooled_lines()), 1)
        self.assertEqual(self.state.requests, [])
        self.assertEqual(sender.finish().sent, 0)

    def test_finished_sender_sends_at_once(self):
        """A trace arriving after the final update (sender finished, not yet removed) is sent, not dropped."""
        sender = get_or_create_sender(self.job)
        sender.finish()
        self.assertTrue(self.job_traces(1)[0].send())
        self.assertEqual(len(self.state.requests), 1)
        self.assertEqual(sender.summary.sent, 0)

    def test_without_background_function(self):
        """If no background function is registered, the trace is sent at once."""
        get_or_create_sender(self.job)
        with patch.object(tracereport._routing, 'background', None):  # pylint: disable=protected-access
            self.job_traces(1)[0].send()
        self.assertEqual(len(self.state.requests), 1)

    def test_background_function_error(self):
        """An error in the background function does not lose the trace."""
        with patch.object(tracereport._routing, 'background', side_effect=RuntimeError('boom')):  # pylint: disable=protected-access
            self.job_traces(1)[0].send()
        self.assertEqual(len(self.state.requests), 1)

    def test_invalid_trace_is_not_routed(self):
        """A trace that cannot be verified is reported as not sent and is neither spooled nor queued."""
        sender = get_or_create_sender(self.job)
        trace = self.job_traces(1)[0]
        trace['remoteSite'] = ''
        self.assertFalse(trace.send())
        enable_trace_spool(self.workdir)
        self.assertFalse(trace.send())
        self.assertEqual(self.spooled_lines(), [])
        self.assertEqual(sender.finish().invalid, 0)

    def test_disabled(self):
        """Nothing is spooled or queued when traces are turned off."""
        os.environ['PILOT_USE_RUCIO_TRACES'] = 'False'
        enable_trace_spool(self.workdir)
        self.assertTrue(self.job_traces(1)[0].send())
        send_traces(self.job_traces(2), job_id=JOB_ID, workdir=self.workdir)
        self.assertEqual(self.spooled_lines(), [])
        self.assertEqual(self.state.requests, [])

    def test_send_traces_without_sender_sends_one_request(self):
        """send_traces() falls back to one bulk request."""
        send_traces(self.job_traces(3), job_id=JOB_ID, workdir=self.workdir)
        self.assertEqual([len(request) for request in self.state.requests], [3])

    def test_send_traces_to_sender(self):
        """send_traces() hands the traces to the job's sender."""
        sender = get_or_create_sender(self.job)
        self.state.delay = 0.5
        send_traces(self.job_traces(3), job_id=JOB_ID, workdir=self.workdir)
        self.assertEqual(self.state.requests, [])
        self.assertEqual(sender.finish().sent, 3)

    def test_send_traces_with_nothing(self):
        """send_traces() with no traces does nothing."""
        get_or_create_sender(self.job)
        send_traces([], job_id=JOB_ID)
        self.assertEqual(get_sender(JOB_ID).finish().sent, 0)
        self.assertEqual(self.state.requests, [])

    def test_defer_without_spool_or_sender(self):
        """defer_traces() reports that the caller must send the traces itself."""
        self.assertFalse(defer_traces(self.job_traces(1), JOB_ID))


class TestTryEnqueue(_RoutingTestCase):
    """TraceSender.try_enqueue() never drops traces silently."""

    def test_accepted_while_running(self):
        """While running, the traces are queued and invalid ones are counted."""
        sender = get_or_create_sender(self.job)
        traces = self.job_traces(3)
        traces[0]['eventType'] = ''
        self.assertTrue(sender.try_enqueue(traces))
        summary = sender.finish()
        self.assertEqual((summary.sent, summary.invalid), (2, 1))

    def test_refused_after_finish(self):
        """After finish() the traces are refused (and not counted), so the caller can send them."""
        sender = get_or_create_sender(self.job)
        first = sender.finish()
        traces = self.job_traces(2)
        traces[0]['eventType'] = ''
        self.assertFalse(sender.try_enqueue(traces))
        self.assertEqual(sender.summary, first)

    def test_refused_after_stop(self):
        """A stopped sender also refuses traces."""
        sender = get_or_create_sender(self.job)
        sender.stop()
        self.assertFalse(sender.try_enqueue(self.job_traces(1)))

    def test_disabled(self):
        """With traces turned off, the traces are taken care of without being queued."""
        os.environ['PILOT_USE_RUCIO_TRACES'] = 'False'
        sender = get_or_create_sender(self.job)
        self.assertTrue(sender.try_enqueue(self.job_traces(2)))
        self.assertEqual(self.state.requests, [])
        os.environ['PILOT_USE_RUCIO_TRACES'] = 'True'
        self.assertEqual(sender.finish().sent, 0)


# ---------------------------------------------------------------------------
# spool file
# ---------------------------------------------------------------------------


class TestSpoolFile(_RoutingTestCase):
    """The spool file written in middleware containers and read by the pilot."""

    def test_round_trip(self):
        """Spooled traces are read back as written, and the file is removed."""
        enable_trace_spool(self.workdir)
        traces = self.job_traces(3)
        expected = [trace.prepare() for trace in traces]
        self.assertTrue(spool_traces(traces))
        self.assertEqual(len(self.spooled_lines()), 3)
        reset_trace_routing()
        self.assertEqual(read_trace_spool(self.workdir), expected)
        self.assertFalse(os.path.exists(self.spool_path))

    def test_appends(self):
        """Several writes (several files, retries) go to the same file."""
        enable_trace_spool(self.workdir)
        self.job_traces(1)[0].send()
        send_traces(self.job_traces(2), job_id=JOB_ID)
        self.assertEqual(len(self.spooled_lines()), 3)

    def test_state_reason_with_none(self):
        """A stateReason containing 'None' survives the spool file."""
        enable_trace_spool(self.workdir)
        trace = self.job_traces(1)[0]
        trace['stateReason'] = "'NoneType' object has no attribute 'get' (null)"
        trace.send()
        traces = read_trace_spool(self.workdir)
        self.assertEqual(traces[0]['stateReason'], "'NoneType' object has no attribute 'get' (null)")
        self.assertIsNone(traces[0]['guid'])

    def test_damaged_last_line(self):
        """A last line cut short (container killed during the write) is skipped."""
        enable_trace_spool(self.workdir)
        spool_traces(self.job_traces(2))
        with open(self.spool_path, 'a', encoding='utf-8') as spool:
            spool.write('{"eventType": "get_sm", "filen')
        with self.assertLogs(tracereport.logger, level='WARNING') as logs:
            traces = read_trace_spool(self.workdir)
        self.assertEqual([trace['filename'] for trace in traces], ['file0', 'file1'])
        self.assertTrue([line for line in logs.output if 'skipped 1 damaged line' in line])

    def test_damaged_and_blank_lines(self):
        """Damaged lines anywhere, and lines that are not a trace, are skipped; blank lines are ignored."""
        with open(self.spool_path, 'w', encoding='utf-8') as spool:
            spool.write('{"filename": "a"}\n\nnot json\n[1, 2]\n"text"\n{"filename": "b"}\n')
        with self.assertLogs(tracereport.logger, level='WARNING') as logs:
            traces = read_trace_spool(self.workdir)
        self.assertEqual(traces, [{'filename': 'a'}, {'filename': 'b'}])
        self.assertTrue([line for line in logs.output if 'skipped 3 damaged line' in line])

    def test_no_spool_file(self):
        """Nothing to read when the container wrote no traces."""
        self.assertEqual(read_trace_spool(self.workdir), [])

    def test_unreadable_spool_file(self):
        """A spool file that cannot be read gives no traces and no exception."""
        os.mkdir(self.spool_path)
        self.assertEqual(read_trace_spool(self.workdir), [])

    def test_spool_file_that_cannot_be_removed(self):
        """The traces are still returned if the file cannot be removed."""
        enable_trace_spool(self.workdir)
        spool_traces(self.job_traces(1))
        with patch.object(tracereport.os, 'remove', side_effect=OSError('read-only')):
            self.assertEqual(len(read_trace_spool(self.workdir)), 1)

    def test_write_failure_sends_at_once(self):
        """If the spool file cannot be written, the trace is sent at once."""
        enable_trace_spool(os.path.join(self.workdir, 'missing'))
        self.assertTrue(self.job_traces(1)[0].send())
        self.assertEqual(len(self.state.requests), 1)
        send_traces(self.job_traces(2), job_id=JOB_ID, workdir=self.workdir)
        self.assertEqual([len(request) for request in self.state.requests], [1, 2])

    def test_only_invalid_traces(self):
        """Nothing is written for traces that cannot be verified."""
        enable_trace_spool(self.workdir)
        trace = self.job_traces(1)[0]
        trace['eventType'] = ''
        self.assertTrue(spool_traces([trace]))
        self.assertFalse(os.path.exists(self.spool_path))

    def test_preparing_again_changes_nothing(self):
        """A spooled trace is prepared again by the pilot; the result is the same."""
        trace = self.job_traces(1)[0]
        trace.update(url='root://eosatlas.cern.ch//eos/atlas/file0', stateReason='a\\b None', guid=None)
        prepared = trace.prepare()
        self.assertEqual(TraceReport(json.loads(json.dumps(prepared))).prepare(), prepared)

    def test_spool_path(self):
        """The file name comes from the config, with a default for config files without the setting."""
        self.assertEqual(get_trace_spool_path('/srv'), os.path.join('/srv', config.Rucio.trace_spool_file))
        with patch.object(config.Rucio, 'trace_spool_file', ''):
            self.assertEqual(get_trace_spool_path('/srv'), os.path.join('/srv', DEFAULT_TRACE_SPOOL_FILE))

    def test_reset(self):
        """reset_trace_routing() turns the spool file off."""
        enable_trace_spool(self.workdir)
        reset_trace_routing()
        self.job_traces(1)[0].send()
        self.assertEqual(self.spooled_lines(), [])
        self.assertEqual(len(self.state.requests), 1)


# ---------------------------------------------------------------------------
# pick-up after a middleware container run
# ---------------------------------------------------------------------------


class TestCollectSpooledTraces(_RoutingTestCase):
    """The pilot sends the traces of a middleware container after it has finished."""

    def run_fake_container(self, n: int) -> None:
        """Write n traces to the spool file, as stagein.py and stageout.py do."""
        enable_trace_spool(self.workdir)
        for trace in self.job_traces(n):
            trace.send()
        reset_trace_routing()

    def test_traces_go_to_the_sender(self):
        """The spooled traces are queued in the job's sender, and the spool file is removed."""
        sender = get_or_create_sender(self.job)
        self.state.delay = 0.5
        self.run_fake_container(3)
        self.assertEqual(collect_spooled_traces(self.job), 3)
        self.assertEqual(self.state.requests, [])
        self.assertFalse(os.path.exists(self.spool_path))
        self.assertEqual(sender.finish().sent, 3)
        self.assertEqual([trace['filename'] for trace in self.received], ['file0', 'file1', 'file2'])

    def test_without_sender(self):
        """Without a sender the spooled traces are sent at once, in one request."""
        self.run_fake_container(3)
        self.assertEqual(collect_spooled_traces(self.job), 3)
        self.assertEqual([len(request) for request in self.state.requests], [3])

    def test_finished_sender(self):
        """After the final update the spooled traces are sent at once."""
        get_or_create_sender(self.job).finish()
        self.run_fake_container(2)
        collect_spooled_traces(self.job)
        self.assertEqual([len(request) for request in self.state.requests], [2])

    def test_nothing_spooled(self):
        """Nothing happens if the container wrote no traces."""
        self.assertEqual(collect_spooled_traces(self.job), 0)
        self.assertEqual(self.state.requests, [])

    def test_never_raises(self):
        """Errors are logged, not raised."""
        self.assertEqual(collect_spooled_traces(MagicMock()), 0)
        self.run_fake_container(1)
        with patch.object(tracereport, 'send_bulk', side_effect=RuntimeError('boom')):
            self.assertEqual(collect_spooled_traces(self.job), 0)


class TestContainerisedStageIn(_RoutingTestCase):
    """containerise_middleware() sends the spooled traces however the container run ended."""

    def setUp(self):
        """Create the pilot arguments."""
        super().setUp()
        self.args = types.SimpleNamespace(input_dir='', output_dir='', queue='CERN-PROD', rucio_host='', stageout_attempts=1)
        self.sender = get_or_create_sender(self.job)
        self.state.delay = 0.2

    def run_containerise(self, execute):
        """Run containerise_middleware() with a fake script run."""
        with patch.object(middleware, 'get_command', return_value='true'), \
                patch.object(middleware, 'execute', side_effect=execute):
            middleware.containerise_middleware(self.job, self.args, [], 'get_sm', 'CERN-PROD', 'CERN-PROD',
                                               label='stage-in', container_type='bash')

    def fake_script(self, *_args, **_kwargs):
        """Write traces and the stage-in status file, as stagein.py does."""
        enable_trace_spool(self.workdir)
        for trace in self.job_traces(2):
            trace.send()
        reset_trace_routing()
        path = os.path.join(self.workdir, config.Container.stagein_status_dictionary)
        with open(path, 'w', encoding='utf-8') as status:
            json.dump({'error': ['', 0]}, status)
        return 0, '', ''

    def test_successful_run(self):
        """The traces of a successful run reach the sender."""
        self.run_containerise(self.fake_script)
        self.assertFalse(os.path.exists(self.spool_path))
        self.assertEqual(self.sender.finish().sent, 2)

    def test_missing_status_file(self):
        """Traces are sent even if the status file is missing, which raises an exception (FileHandlingFailure)."""
        def script(*args, **kwargs):
            self.fake_script(*args, **kwargs)
            os.remove(os.path.join(self.workdir, config.Container.stagein_status_dictionary))
            return 1, '', 'killed'

        with self.assertRaises(PilotException):
            self.run_containerise(script)
        self.assertEqual(self.sender.finish().sent, 2)

    def test_failed_execution(self):
        """Traces are sent even if running the script raised an exception."""
        def script(*args, **kwargs):
            self.fake_script(*args, **kwargs)
            raise RuntimeError('container runtime failed')

        self.run_containerise(script)
        self.assertEqual(self.sender.finish().sent, 2)


# ---------------------------------------------------------------------------
# rucio stage-in: file trace with the traces of failed download attempts
# ---------------------------------------------------------------------------


class TestStageInTraces(_RoutingTestCase):
    """send_stagein_traces() routes the attempt traces like any other trace."""

    def report(self) -> TraceReport:
        """Return the file's trace report after a successful download."""
        trace = self.job_traces(1)[0]
        trace.update(clientState='DONE', stateReason='OK')
        return trace

    def attempts(self) -> list:
        """Return two failed download attempts."""
        return [{'clientState': 'FAILED', 'stateReason': f'attempt {i} failed', 'protocol': 'root',
                 'remoteSite': 'CERN-PROD_DATADISK', 'timeEnd': 1.0} for i in (1, 2)]

    def test_to_sender(self):
        """With a sender, the attempt traces and the file's trace are queued."""
        sender = get_or_create_sender(self.job)
        self.state.delay = 0.5
        rucio_copytool.send_stagein_traces(self.report(), self.attempts(), rucio_failed=False)
        self.assertEqual(self.state.requests, [])
        self.assertEqual(sender.finish().sent, 3)
        self.assertEqual([trace['clientState'] for trace in self.received], ['STAGEIN_ATTEMPT_FAILED'] * 2 + ['DONE'])

    def test_to_spool(self):
        """In a middleware container, the traces are spooled."""
        enable_trace_spool(self.workdir)
        rucio_copytool.send_stagein_traces(self.report(), self.attempts(), rucio_failed=False)
        self.assertEqual(len(self.spooled_lines()), 3)
        self.assertEqual(self.state.requests, [])

    def test_at_once(self):
        """Without a sender, the traces are sent in one request, as in phase 1."""
        rucio_copytool.send_stagein_traces(self.report(), self.attempts(), rucio_failed=False)
        self.assertEqual([len(request) for request in self.state.requests], [3])

    def test_without_attempts(self):
        """Without failed attempts, the file's trace goes through TraceReport.send()."""
        sender = get_or_create_sender(self.job)
        rucio_copytool.send_stagein_traces(self.report(), [], rucio_failed=False)
        self.assertEqual(sender.finish().sent, 1)


# ---------------------------------------------------------------------------
# stage-in and stage-out entry points
# ---------------------------------------------------------------------------


class _FakeStageInClient:
    """Stage-in client whose transfer sends the trace of a copytool (in-process stage-in)."""

    def __init__(self, trace: TraceReport):
        """Keep the trace to send."""
        self.trace = trace

    def prepare_sources(self, *_args, **_kwargs):
        """Do nothing."""

    def prepare_inputddms(self, *_args, **_kwargs):
        """Do nothing."""

    def transfer(self, *_args, **_kwargs):
        """Send the trace, as a copytool does after each file."""
        self.trace.send()


class TestEntryPoints(_RoutingTestCase):
    """The stage-in and stage-out entry points create the job's sender before any transfer."""

    def setUp(self):
        """Mock everything around the transfers."""
        super().setUp()
        self.job.indata = []
        self.job.outdata = []
        self.args = types.SimpleNamespace(input_destination_dir='', rucio_host='', output_dir='')
        self.patches = [patch.object(data_control, 'add_to_pilot_timing'),
                        patch.object(data_control, 'skip_special_files'),
                        patch.object(data_control, 'update_indata'),
                        patch.object(data_control, 'refresh_job_proxies_before_stageout'),
                        patch.object(data_control, 'get_trace_report_variables', return_value=('get_sm', 'A', 'A')),
                        patch.dict(os.environ, {'X509_UNIFIED_DISPATCH': ''})]
        for _patch in self.patches:
            _patch.start()
        self.sender_seen = []

    def tearDown(self):
        """Stop the patches."""
        for _patch in reversed(self.patches):
            _patch.stop()
        super().tearDown()

    def containerise(self, *_args, **_kwargs):
        """Record whether the sender existed when the container was started."""
        self.sender_seen.append(get_sender(JOB_ID))

    def test_containerised_stage_in(self):
        """The sender exists before the stage-in container runs."""
        with patch.object(data_control, 'use_middleware_script', return_value=True), \
                patch.object(data_control, 'containerise_middleware', side_effect=self.containerise):
            data_control._stage_in(self.args, self.job)  # pylint: disable=protected-access
        self.assertIsNotNone(self.sender_seen[0])

    def test_containerised_stage_out(self):
        """The sender exists before the stage-out container runs."""
        with patch.object(data_control, 'use_middleware_script', return_value=True), \
                patch.object(data_control, 'containerise_middleware', side_effect=self.containerise):
            data_control._do_stageout(self.job, self.args, [], ['pw'], 'output')  # pylint: disable=protected-access
        self.assertIsNotNone(self.sender_seen[0])

    def test_in_process_stage_in(self):
        """A copytool trace sent during an in-process stage-in goes to the job's sender."""
        self.state.delay = 0.5
        client = _FakeStageInClient(self.job_traces(1)[0])
        with patch.object(data_control, 'use_middleware_script', return_value=False), \
                patch.object(data_control, 'get_stagein_client', return_value=(client, 'read_lan', ['read_lan'])):
            data_control._stage_in(self.args, self.job)  # pylint: disable=protected-access
        self.assertEqual(self.state.requests, [])
        self.assertEqual(get_sender(JOB_ID).finish().sent, 1)

    def test_initialised_report_goes_to_the_sender(self):
        """A trace report initialised for the job (appid = job id) goes to the job's sender."""
        sender = get_or_create_sender(self.job)
        job = types.SimpleNamespace(produserid='user', jobid=JOB_ID, taskid='1', jobdefinitionid=0)
        trace = TraceReport(eventType='put_sm', localSite='CERN-PROD', remoteSite='CERN-PROD_DATADISK', workdir=self.workdir)
        with patch.object(tracereport, 'get_host_identity', return_value=('host.cern.ch', '127.0.0.1')):
            trace.init(job)
        self.state.delay = 0.5
        trace.send()
        self.assertEqual(self.state.requests, [])
        self.assertEqual(sender.finish().sent, 1)


class TestContainerScripts(unittest.TestCase):
    """stagein.py and stageout.py write their traces to the spool file."""

    def check_script(self, name: str) -> None:
        """Check that the script turns the spool file on before it creates the trace report."""
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'scripts', name)
        with open(path, encoding='utf-8') as script:
            tree = ast.parse(script.read())
        main = [node for node in tree.body if isinstance(node, ast.If) and 'name' in ast.unparse(node.test)][0]
        calls = [ast.unparse(node.func) for statement in main.body for node in ast.walk(statement) if isinstance(node, ast.Call)]
        self.assertIn('enable_trace_spool', calls)
        self.assertLess(calls.index('enable_trace_spool'), calls.index('TraceReport'))
        spool_call = [node for node in ast.walk(main) if isinstance(node, ast.Call) and ast.unparse(node.func) == 'enable_trace_spool'][0]
        self.assertEqual(ast.unparse(spool_call.args[0]), 'args.workdir')

    def test_stagein(self):
        """stagein.py turns the spool file on."""
        self.check_script('stagein.py')

    def test_stageout(self):
        """stageout.py turns the spool file on."""
        self.check_script('stageout.py')


if __name__ == '__main__':
    unittest.main()
