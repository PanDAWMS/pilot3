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

"""Tests for bulk and background Rucio traces.

Production incident (PanDA job 7372130338): a user job with 200 remote i/o input files had a low
CPU efficiency. The remote i/o traces were sent one at a time before the payload started, and 41
of them failed with 502 Bad Gateway after about a minute each, which delayed the payload start by
about half an hour. The traces are now sent in bulk by a background sender that never delays the
payload. Failed Rucio-internal download attempts now also get a trace.

A local stub server stands in for the Rucio 41 trace server: it parses the request body as JSON,
wraps a single trace in a list, and replies 201 ``Created``.
"""

import enum
import json
import os
import shutil
import sys
import tempfile
import threading
import time
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import MagicMock, patch

from pilot.common.exception import PilotException
from pilot.copytool import rucio as rucio_copytool
from pilot.user.atlas import jobmetrics as atlas_jobmetrics
from pilot.user.atlas.common import REMOTEIO_OPENED, process_remote_file_traces
from pilot.user.epic import jobmetrics as epic_jobmetrics
from pilot.util import tracereport, tracesender
from pilot.util.config import config
from pilot.util.https import request2
from pilot.util.tracereport import (
    TraceReport,
    get_trace_error_code,
    get_trace_setting,
    is_trace_accepted,
    prepare_traces,
    send_bulk,
)
from pilot.util.tracesender import (
    TraceSender,
    TraceSettings,
    finish_traces,
    get_lost_traces,
    get_or_create_sender,
    get_parallel_traces,
    get_sender,
    remove_sender,
    reset_registry,
)


# ---------------------------------------------------------------------------
# stub trace server
# ---------------------------------------------------------------------------


class _StubState:  # pylint: disable=too-few-public-methods
    """Behaviour and records of the stub trace server."""

    def __init__(self):
        """Start in 'accept' mode."""
        self.mode = 'accept'  # 'accept' (201 Created) or 'fail' (502)
        self.delay = 0.0
        self.requests = []  # list of traces per request
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()


class _StubHandler(BaseHTTPRequestHandler):
    """Mimics the Rucio 41 trace handler."""

    state: _StubState = None

    def do_POST(self):  # noqa: N802  # pylint: disable=invalid-name
        """Handle a trace request."""
        state = self.state
        with state.lock:
            state.active += 1
            state.max_active = max(state.max_active, state.active)
        try:
            body = self.rfile.read(int(self.headers.get('Content-Length', 0)))
            traces = json.loads(body)
            if isinstance(traces, dict):
                traces = [traces]
            time.sleep(state.delay)
            with state.lock:
                state.requests.append(traces)
            if state.mode == 'fail':
                self.send_response(502, 'Bad Gateway')
                self.end_headers()
                self.wfile.write(b'Bad Gateway')
            else:
                self.send_response(201, 'Created')
                self.end_headers()
                self.wfile.write(b'Created')
        finally:
            with state.lock:
                state.active -= 1

    def log_message(self, format, *args):  # noqa: A002
        """Keep the test output quiet."""


class _StubServerTestCase(unittest.TestCase):
    """Test case with a stub trace server, a work directory and a clean trace sender registry."""

    def setUp(self):
        """Start the stub server and point the trace URL at it."""
        self.state = _StubState()
        handler = type('Handler', (_StubHandler,), {'state': self.state})
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), handler)
        self.server.daemon_threads = True
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        url = f'http://127.0.0.1:{self.server.server_address[1]}/traces/'
        self.workdir = tempfile.mkdtemp()
        self.patches = [patch.object(config.Rucio, 'url', url),
                        patch.dict(os.environ, {'PILOT_USE_RUCIO_TRACES': 'True'})]
        for _patch in self.patches:
            _patch.start()
        os.environ.pop('RUCIO_TRACE_ERROR', None)
        os.environ.pop('RUCIO_LOCAL_SITE_ID', None)
        reset_registry()

    def tearDown(self):
        """Stop the stub server and clean up."""
        reset_registry()
        for _patch in reversed(self.patches):
            _patch.stop()
        self.server.shutdown()
        self.server.server_close()
        shutil.rmtree(self.workdir, ignore_errors=True)

    @property
    def received(self) -> list:
        """Return all traces received by the stub server."""
        with self.state.lock:
            return [trace for request in self.state.requests for trace in request]

    def traces(self, n: int) -> list:
        """Return n valid trace reports."""
        return [TraceReport(eventType='get_sm', localSite='CERN-PROD', remoteSite='CERN-PROD_DATADISK',
                            filename=f'file{i}', workdir=self.workdir) for i in range(n)]

    def sender(self, parallel: int = 1, **settings) -> TraceSender:
        """Return a trace sender with test settings (traces sent at once unless batch_wait is given)."""
        defaults = {'batch_size': 100, 'max_parallel': 2, 'timeout': 10, 'slow_threshold': 5, 'final_wait': 10,
                    'batch_wait': 0}
        defaults.update(settings)
        return TraceSender('1234', self.workdir, parallel=parallel, settings=TraceSettings(**defaults))

    def wait_for(self, condition, timeout: float = 5.0):
        """Wait until condition() is true."""
        deadline = time.time() + timeout
        while not condition():
            if time.time() > deadline:
                self.fail('condition not met in time')
            time.sleep(0.01)

    def stored_error(self) -> str:
        """Return the stored trace error code, or '' if none."""
        path = os.path.join(self.workdir, config.Rucio.rucio_trace_error_file)
        if not os.path.exists(path):
            return ''
        with open(path, encoding='utf-8') as _file:
            return _file.read().strip()


# ---------------------------------------------------------------------------
# tracereport: success detection, preparation, single and bulk sending
# ---------------------------------------------------------------------------


class TestSuccessDetection(unittest.TestCase):
    """The Rucio trace server replies 'Created', which is a success."""

    def test_created_is_accepted(self):
        """The real Rucio reply."""
        self.assertTrue(is_trace_accepted('Created'))
        self.assertTrue(is_trace_accepted(' Created\n'))

    def test_dictionary_is_accepted(self):
        """A JSON reply."""
        self.assertTrue(is_trace_accepted({'status': 'ok'}))
        self.assertTrue(is_trace_accepted({}))

    def test_failures(self):
        """Error messages and anything unexpected are failures."""
        for reply in ('failed to send request: HTTP Error 502: Bad Gateway', 'failed to send request: timed out',
                      '', 'created', 'OK', 'Created and more', None, 201, ['Created']):
            self.assertFalse(is_trace_accepted(reply), reply)


class TestTraceErrorCode(unittest.TestCase):
    """The error code stored for a failed request."""

    def test_http_status(self):
        """The HTTP status code is used when present."""
        self.assertEqual(get_trace_error_code('failed to send request: HTTP Error 502: Bad Gateway'), 502)

    def test_no_http_status(self):
        """Other failures get the generic code."""
        self.assertEqual(get_trace_error_code('failed to send request: <urlopen error timed out>'), 1)
        self.assertEqual(get_trace_error_code(None), 1)


class TestTraceSetting(unittest.TestCase):
    """Config settings fall back to defaults."""

    def test_missing_setting(self):
        """A setting missing from a custom config file."""
        self.assertEqual(get_trace_setting('no_such_trace_setting', 7), 7)

    def test_invalid_setting(self):
        """Non-positive or non-numeric values."""
        for value in (0, -3, 'abc', None):
            with patch.object(config.Rucio, 'trace_batch_size', value):
                self.assertEqual(get_trace_setting('trace_batch_size', 100), 100)

    def test_valid_setting(self):
        """A positive value is used."""
        with patch.object(config.Rucio, 'trace_batch_size', 25):
            self.assertEqual(get_trace_setting('trace_batch_size', 100), 25)

    def test_minimum(self):
        """With minimum 0, zero is valid but a negative value is not."""
        with patch.object(config.Rucio, 'trace_batch_wait', 0):
            self.assertEqual(get_trace_setting('trace_batch_wait', 60, minimum=0), 0)
        with patch.object(config.Rucio, 'trace_batch_wait', -1):
            self.assertEqual(get_trace_setting('trace_batch_wait', 60, minimum=0), 60)
        with patch.object(config.Rucio, 'trace_batch_wait', 5):
            self.assertEqual(get_trace_setting('trace_batch_wait', 60, minimum=6), 60)
            self.assertEqual(get_trace_setting('trace_batch_wait', 60, minimum=5), 5)

    def test_batch_wait(self):
        """trace_batch_wait may be 0 (send at once); missing or invalid values give the default."""
        with patch.object(config.Rucio, 'trace_batch_wait', 0):
            self.assertEqual(TraceSettings.from_config().batch_wait, 0)
        with patch.object(config.Rucio, 'trace_batch_wait', 'abc'):
            self.assertEqual(TraceSettings.from_config().batch_wait, 300)
        with patch.object(config.Rucio, 'trace_batch_wait', 15):
            self.assertEqual(TraceSettings.from_config().batch_wait, 15)

    def test_default_config(self):
        """The values in default.cfg."""
        settings = TraceSettings.from_config()
        self.assertEqual((settings.batch_size, settings.max_parallel, settings.timeout, settings.slow_threshold,
                          settings.final_wait, settings.batch_wait), (100, 2, 30, 20, 600, 300))


class TestPrepare(unittest.TestCase):
    """Preparation of a trace for sending."""

    def setUp(self):
        """Clear the local site override."""
        self.env = patch.dict(os.environ)
        self.env.start()
        os.environ.pop('RUCIO_LOCAL_SITE_ID', None)

    def tearDown(self):
        """Restore the environment."""
        self.env.stop()

    @staticmethod
    def report(**fields) -> TraceReport:
        """Return a valid trace report."""
        values = {'eventType': 'get_sm', 'localSite': 'SITE', 'remoteSite': 'RSE', 'workdir': '/tmp/w', 'ipv': 'IPv4'}
        values.update(fields)
        return TraceReport(**values)

    def test_none_values_are_sent_as_null(self):
        """None values (and the strings 'None' and 'null') are sent as null, as before."""
        data = self.report(dataset='None', version='null').prepare()
        self.assertIsNone(data['filesize'])
        self.assertIsNone(data['dataset'])
        self.assertIsNone(data['version'])

    def test_state_reason_containing_none(self):
        """A reason containing 'None' or 'null' used to make the trace invalid JSON, and it was lost."""
        reason = "'NoneType' object has no attribute 'get' (nullable)"
        data = self.report(stateReason=reason).prepare()
        self.assertEqual(data['stateReason'], reason)

    def test_protocol_from_url(self):
        """The protocol is taken from the URL when it is missing."""
        data = self.report(url='root://eosatlas.cern.ch//eos/file').prepare()
        self.assertEqual(data['protocol'], 'root')

    def test_local_site_override(self):
        """RUCIO_LOCAL_SITE_ID overrides the local site."""
        os.environ['RUCIO_LOCAL_SITE_ID'] = 'OVERRIDE'
        self.assertEqual(self.report().prepare()['localSite'], 'OVERRIDE')

    def test_incomplete_trace(self):
        """A trace without a remote site cannot be sent."""
        self.assertIsNone(self.report(remoteSite='').prepare())

    def test_not_serialisable(self):
        """A trace that cannot be serialised is dropped instead of raising."""
        self.assertIsNone(self.report(filename=object()).prepare())

    def test_internal_attributes_are_not_sent(self):
        """workdir and ipv are not part of the trace."""
        data = self.report().prepare()
        self.assertNotIn('workdir', data)
        self.assertNotIn('ipv', data)

    def test_copy_is_independent(self):
        """The prepared trace does not change when the report is reused for the next file."""
        report = self.report(filename='a')
        data = report.prepare()
        report['filename'] = 'b'
        self.assertEqual(data['filename'], 'a')

    def test_invalid_traces_are_dropped(self):
        """Traces that cannot be verified are dropped and counted."""
        prepared, invalid = prepare_traces([self.report(), self.report(remoteSite=''),
                                            {'eventType': 'get_sm', 'localSite': 'S', 'remoteSite': 'R'}])
        self.assertEqual((len(prepared), invalid), (2, 1))


class TestSingleSend(_StubServerTestCase):
    """TraceReport.send() against the stub server."""

    def test_created_is_a_success(self):
        """A trace accepted by the server was reported as a failure ('returned a string instead of a dictionary')."""
        with self.assertLogs(tracereport.logger, level='INFO') as logs:
            self.assertTrue(self.traces(1)[0].send())
        self.assertFalse([line for line in logs.output if 'instead of a dictionary' in line])
        self.assertEqual(len(self.received), 1)

    def test_server_error_is_a_failure(self):
        """A 502 reply is a failure and is not retried."""
        self.state.mode = 'fail'
        self.assertFalse(self.traces(1)[0].send())
        self.assertEqual(len(self.state.requests), 1)

    def test_trace_timeout_is_used(self):
        """Single traces use the trace time-out, not the general one."""
        with patch.object(tracereport, 'request2', return_value='Created') as mocked_request2:
            self.traces(1)[0].send()
        self.assertEqual(mocked_request2.call_args.kwargs['timeout'], 30)

    def test_disabled(self):
        """Nothing is sent when traces are turned off."""
        os.environ['PILOT_USE_RUCIO_TRACES'] = 'False'
        self.assertTrue(self.traces(1)[0].send())
        self.assertEqual(self.state.requests, [])


class TestSendBulk(_StubServerTestCase):
    """send_bulk() sends traces in requests of at most 100 traces."""

    def test_chunking(self):
        """250 traces are sent in three requests."""
        result = send_bulk(self.traces(250), workdir=self.workdir)
        self.assertEqual([len(request) for request in self.state.requests], [100, 100, 50])
        self.assertEqual((result.sent, result.failed, result.invalid, result.requests), (250, 0, 0, 3))
        self.assertEqual(len({trace['filename'] for trace in self.received}), 250)

    def test_exactly_one_batch(self):
        """100 traces fit in one request."""
        send_bulk(self.traces(100), workdir=self.workdir)
        self.assertEqual([len(request) for request in self.state.requests], [100])

    def test_invalid_traces(self):
        """Invalid traces are dropped, the others are sent."""
        traces = self.traces(3)
        traces[1]['remoteSite'] = ''
        result = send_bulk(traces, workdir=self.workdir)
        self.assertEqual((result.sent, result.invalid), (2, 1))

    def test_failure_is_stored_for_the_job_metrics(self):
        """A failed bulk request sets the rucio trace error (decision: bulk failures are reported like curl errors)."""
        self.state.mode = 'fail'
        result = send_bulk(self.traces(3), workdir=self.workdir)
        self.assertEqual((result.sent, result.failed), (0, 3))
        self.assertEqual(self.stored_error(), '502')
        self.assertEqual(os.environ.get('RUCIO_TRACE_ERROR'), '502')

    def test_success_stores_no_error(self):
        """No error is stored when all requests are accepted."""
        send_bulk(self.traces(3), workdir=self.workdir)
        self.assertEqual(self.stored_error(), '')
        self.assertIsNone(os.environ.get('RUCIO_TRACE_ERROR'))

    def test_disabled(self):
        """Nothing is sent when traces are turned off."""
        os.environ['PILOT_USE_RUCIO_TRACES'] = 'False'
        self.assertEqual(send_bulk(self.traces(3)).requests, 0)
        self.assertEqual(self.state.requests, [])

    def test_request_exception(self):
        """An exception in the request is a failure, not a crash."""
        with patch.object(tracereport, 'request2', side_effect=RuntimeError('boom')):
            result = send_bulk(self.traces(2), workdir=self.workdir)
        self.assertEqual(result.failed, 2)
        self.assertEqual(self.stored_error(), '1')


class TestRequestTimeout(_StubServerTestCase):
    """request2() uses the given time-out instead of the general one (120 s)."""

    def test_timeout_is_used(self):
        """A slow server makes the request fail after the given time-out."""
        self.state.delay = 3.0
        t0 = time.time()
        reply = request2(url=config.Rucio.url, json_body=[{'a': 1}], secure=False, compressed=False, timeout=1)
        self.assertLess(time.time() - t0, 2.5)
        self.assertFalse(is_trace_accepted(reply))

    def test_default_timeout(self):
        """Without a time-out, config.Pilot.http_maxtime is used."""
        self.state.delay = 3.0
        with patch.object(config.Pilot, 'http_maxtime', 1):
            t0 = time.time()
            reply = request2(url=config.Rucio.url, json_body=[{'a': 1}], secure=False, compressed=False)
        self.assertLess(time.time() - t0, 2.5)
        self.assertFalse(is_trace_accepted(reply))

    def test_list_body(self):
        """A list of traces arrives as one request."""
        reply = request2(url=config.Rucio.url, json_body=[{'a': 1}, {'a': 2}], secure=False, compressed=False, timeout=5)
        self.assertTrue(is_trace_accepted(reply))
        self.assertEqual(self.state.requests, [[{'a': 1}, {'a': 2}]])


# ---------------------------------------------------------------------------
# tracesender
# ---------------------------------------------------------------------------


class TestParallelTraces(unittest.TestCase):
    """PARALLEL_TRACES in the catchall field: default 1, capped at trace_max_parallel."""

    def test_default(self):
        """No key means one request at a time."""
        for catchall in (None, '', 'nopenfiles=4 remoteio_test=false'):
            self.assertEqual(get_parallel_traces(catchall, 2), 1, catchall)

    def test_value(self):
        """A valid value is used up to the cap."""
        self.assertEqual(get_parallel_traces('PARALLEL_TRACES=2', 2), 2)
        self.assertEqual(get_parallel_traces('nopenfiles=4 PARALLEL_TRACES=2 other', 4), 2)
        self.assertEqual(get_parallel_traces('PARALLEL_TRACES=1', 2), 1)

    def test_cap(self):
        """The value is capped at trace_max_parallel."""
        self.assertEqual(get_parallel_traces('PARALLEL_TRACES=5', 2), 2)
        self.assertEqual(get_parallel_traces('PARALLEL_TRACES=5', 0), 1)

    def test_invalid(self):
        """An invalid value means one request at a time."""
        for value in ('0', '-1', 'abc', '', '2.5'):
            self.assertEqual(get_parallel_traces(f'PARALLEL_TRACES={value}', 2), 1, value)

    def test_case_insensitive(self):
        """The key is matched case-insensitively."""
        self.assertEqual(get_parallel_traces('parallel_traces=2', 2), 2)


class TestTraceSender(_StubServerTestCase):
    """Background sending, parallel requests, back-pressure and finish()."""

    def test_enqueue_does_not_block(self):
        """Queuing returns before the slow server replies."""
        self.state.delay = 0.5
        sender = self.sender()
        t0 = time.time()
        self.assertEqual(sender.enqueue(self.traces(5)), 5)
        self.assertLess(time.time() - t0, 0.4)
        self.assertEqual(sender.finish().sent, 5)

    def test_healthy_finish_sends_everything(self):
        """A healthy sender sends the remaining traces before finishing."""
        self.state.delay = 0.1
        sender = self.sender()
        sender.enqueue(self.traces(250))
        summary = sender.finish()
        self.assertEqual((summary.sent, summary.failed, summary.abandoned, summary.lost), (250, 0, 0, 0))
        self.assertFalse(summary.degraded)
        self.assertEqual([len(request) for request in self.state.requests], [100, 100, 50])

    def test_parallel_requests(self):
        """Two requests are sent at the same time when allowed."""
        self.state.delay = 0.3
        sender = self.sender(parallel=2)
        sender.enqueue(self.traces(400))
        sender.finish()
        self.assertEqual(self.state.max_active, 2)
        self.assertEqual(len(self.received), 400)

    def test_one_request_at_a_time_by_default(self):
        """Without PARALLEL_TRACES, one request at a time."""
        self.state.delay = 0.2
        sender = self.sender(parallel=1)
        sender.enqueue(self.traces(300))
        sender.finish()
        self.assertEqual(self.state.max_active, 1)

    def test_failure_degrades(self):
        """After a failed request, one request at a time, and every trace is lost."""
        self.state.mode = 'fail'
        self.state.delay = 0.1
        sender = self.sender(parallel=2)
        sender.enqueue(self.traces(600))
        self.wait_for(lambda: sender.degraded)
        self.assertEqual(sender.parallel, 1)
        summary = sender.finish()
        self.assertTrue(summary.degraded)
        self.assertEqual(summary.lost, 600)
        self.assertEqual(summary.failed + summary.abandoned, 600)
        self.assertEqual(self.stored_error(), '502')

    def test_slow_request_degrades(self):
        """A slow (but accepted) request also degrades the sender."""
        self.state.delay = 0.3
        sender = self.sender(parallel=2, slow_threshold=0.1)
        sender.enqueue(self.traces(100))
        self.wait_for(lambda: sender.degraded)
        self.assertEqual(sender.parallel, 1)
        self.assertEqual(sender.summary.sent, 100)
        self.assertEqual(self.stored_error(), '')

    def test_degraded_finish_waits_at_most_the_timeout(self):
        """A degraded sender waits at most trace_timeout seconds, then abandons the rest."""
        self.state.mode = 'fail'
        sender = self.sender(timeout=1, final_wait=10)
        sender.enqueue(self.traces(100))
        self.wait_for(lambda: sender.degraded)
        self.state.delay = 0.6
        sender.enqueue(self.traces(300))
        t0 = time.time()
        summary = sender.finish()
        elapsed = time.time() - t0
        self.assertGreater(elapsed, 0.9)
        self.assertLess(elapsed, 1.6)
        self.assertEqual(summary.lost, 400)
        self.assertGreater(summary.abandoned, 0)

    def test_degraded_finish_sends_late_traces(self):
        """Traces queued at the end of the job (stage-out) are still sent by a degraded sender."""
        self.state.delay = 0.2
        sender = self.sender(slow_threshold=0.1, timeout=5, final_wait=10)
        sender.enqueue(self.traces(10))
        self.wait_for(lambda: sender.degraded)
        sender.enqueue(self.traces(30))  # e.g. the output and log traces
        summary = sender.finish()
        self.assertTrue(summary.degraded)
        self.assertEqual((summary.sent, summary.lost), (40, 0))

    def test_degraded_wait_is_not_longer_than_the_final_wait(self):
        """A degraded sender never waits longer than trace_final_wait."""
        self.state.mode = 'fail'
        sender = self.sender(timeout=5, final_wait=0.5)
        sender.enqueue(self.traces(100))
        self.wait_for(lambda: sender.degraded)
        self.state.delay = 2.0
        sender.enqueue(self.traces(100))
        t0 = time.time()
        sender.finish()
        self.assertLess(time.time() - t0, 1.2)

    def test_degradation_during_final_wait(self):
        """When requests fail during the final wait, the wait ends after trace_timeout seconds (job 7372130338)."""
        self.state.mode = 'fail'
        self.state.delay = 0.5
        sender = self.sender(timeout=1, final_wait=30)
        sender.enqueue(self.traces(500))
        t0 = time.time()
        summary = sender.finish()
        self.assertLess(time.time() - t0, 2.0)
        self.assertLessEqual(len(self.state.requests), 3)
        self.assertEqual(summary.lost, 500)

    def test_counts_are_final_after_finish(self):
        """A request that replies after finish() does not change the counts."""
        self.state.mode = 'fail'
        self.state.delay = 1.0
        sender = self.sender(final_wait=0.3)
        sender.enqueue(self.traces(100))
        summary = sender.finish()
        self.assertEqual(summary.abandoned, 100)
        self.wait_for(lambda: len(self.state.requests) == 1)
        time.sleep(0.2)
        self.assertEqual(sender.summary, summary)
        self.assertEqual(sender.summary.lost, 100)

    def test_final_wait_limit(self):
        """A healthy sender waits at most trace_final_wait seconds."""
        self.state.delay = 3.0
        sender = self.sender(final_wait=1)
        sender.enqueue(self.traces(150))
        t0 = time.time()
        summary = sender.finish()
        self.assertLess(time.time() - t0, 2.0)
        self.assertEqual(summary.abandoned, 150)  # the request in flight is not confirmed

    def test_graceful_stop_already_set(self):
        """No wait when graceful stop has been set."""
        self.state.delay = 2.0
        sender = self.sender()
        sender.enqueue(self.traces(10))
        event = threading.Event()
        event.set()
        t0 = time.time()
        summary = sender.finish(event)
        self.assertLess(time.time() - t0, 0.5)
        self.assertEqual(summary.abandoned, 10)

    def test_graceful_stop_during_wait(self):
        """The wait ends when graceful stop is set."""
        self.state.delay = 5.0
        sender = self.sender(final_wait=30)
        sender.enqueue(self.traces(10))
        event = threading.Event()
        threading.Timer(0.3, event.set).start()
        t0 = time.time()
        sender.finish(event)
        self.assertLess(time.time() - t0, 3.0)

    def test_finish_twice(self):
        """A repeated finish returns the same counts."""
        sender = self.sender()
        sender.enqueue(self.traces(3))
        first = sender.finish()
        self.assertEqual(sender.finish(), first)

    def test_nothing_queued_after_finish(self):
        """Traces arriving after the final update are not queued."""
        sender = self.sender()
        sender.finish()
        self.assertEqual(sender.enqueue(self.traces(3)), 0)

    def test_invalid_traces_are_counted(self):
        """Invalid traces are counted but not lost."""
        traces = self.traces(3)
        traces[0]['eventType'] = ''
        sender = self.sender()
        sender.enqueue(traces)
        summary = sender.finish()
        self.assertEqual((summary.sent, summary.invalid, summary.lost), (2, 1, 0))

    def test_disabled(self):
        """Nothing is queued when traces are turned off."""
        os.environ['PILOT_USE_RUCIO_TRACES'] = 'False'
        sender = self.sender()
        self.assertEqual(sender.enqueue(self.traces(3)), 0)
        self.assertEqual(sender.finish().sent, 0)


class TestHeldTraces(_StubServerTestCase):
    """Traces are held so that several go in one request (``[Rucio] trace_batch_wait``).

    Production job 7386436637 (pilot 3.14.6.12, MWT2_TEST) downloaded its 10 input files one at a time. Each
    trace was queued 3-10 s after the previous one, and the worker sent it before the next one arrived, so the
    job's 12 traces went in 12 requests. The tests use a hold time of 1 s and gaps of 0.1 s to mimic this.
    """

    def requests(self) -> list:
        """Return the number of traces in each request received so far."""
        with self.state.lock:
            return [len(request) for request in self.state.requests]

    def test_job_7386436637(self):
        """Traces queued a little apart go in one request after the hold time."""
        sender = self.sender(batch_wait=1)
        t0 = time.time()
        for trace in self.traces(5):
            sender.enqueue([trace])
            time.sleep(0.15)
        self.assertEqual(self.requests(), [])  # still held
        self.wait_for(lambda: self.requests() == [5])
        self.assertLess(time.time() - t0, 1.4)  # the hold time counts from the first trace, not the last
        self.assertEqual(sender.finish().sent, 5)

    def test_held_for_at_most_the_batch_wait(self):
        """A single trace is sent after the hold time, not before and not much later."""
        sender = self.sender(batch_wait=1)
        t0 = time.time()
        sender.enqueue(self.traces(1))
        self.wait_for(lambda: self.requests() == [1])
        self.assertGreaterEqual(time.time() - t0, 0.9)
        self.assertLess(time.time() - t0, 2.0)

    def test_full_request_is_sent_at_once(self):
        """Enough traces for a full request are not held."""
        sender = self.sender(batch_wait=60, batch_size=3)
        sender.enqueue(self.traces(2))
        time.sleep(0.3)
        self.assertEqual(self.requests(), [])
        sender.enqueue(self.traces(1))
        self.wait_for(lambda: self.requests() == [3], timeout=2.0)

    def test_full_round_of_parallel_requests(self):
        """With parallel requests, the traces are held until all requests of a round are full."""
        self.state.delay = 0.3
        sender = self.sender(parallel=2, batch_wait=60, batch_size=2)
        sender.enqueue(self.traces(3))
        time.sleep(0.3)
        self.assertEqual(self.requests(), [])
        sender.enqueue(self.traces(1))
        self.wait_for(lambda: self.requests() == [2, 2], timeout=2.0)
        self.assertEqual(self.state.max_active, 2)

    def test_remaining_traces_keep_the_hold_time(self):
        """A trace left over after a full request is sent when the oldest trace's hold time is up."""
        sender = self.sender(batch_wait=1, batch_size=2)
        t0 = time.time()
        sender.enqueue(self.traces(3))
        self.wait_for(lambda: self.requests() == [2], timeout=0.5)
        self.wait_for(lambda: self.requests() == [2, 1], timeout=3.0)
        self.assertGreaterEqual(time.time() - t0, 0.9)
        self.assertLess(time.time() - t0, 1.6)

    def test_new_hold_after_sending(self):
        """A trace queued after the waiting traces were sent is held again."""
        sender = self.sender(batch_wait=1)
        sender.enqueue(self.traces(1))
        self.wait_for(lambda: self.requests() == [1])
        sender.enqueue(self.traces(1))
        time.sleep(0.5)
        self.assertEqual(self.requests(), [1])
        self.wait_for(lambda: self.requests() == [1, 1])

    def test_finish_sends_held_traces(self):
        """Held traces are sent at once at the end of the job."""
        sender = self.sender(batch_wait=60)
        sender.enqueue(self.traces(2))
        t0 = time.time()
        summary = sender.finish()
        self.assertLess(time.time() - t0, 2.0)
        self.assertEqual((summary.sent, summary.abandoned), (2, 0))
        self.assertEqual(self.requests(), [2])

    def test_graceful_stop_abandons_held_traces(self):
        """Held traces are abandoned when graceful stop has been set."""
        sender = self.sender(batch_wait=60)
        sender.enqueue(self.traces(2))
        event = threading.Event()
        event.set()
        summary = sender.finish(event)
        self.assertEqual((summary.sent, summary.abandoned, summary.lost), (0, 2, 2))
        self.assertEqual(self.requests(), [])

    def test_stop_drops_held_traces(self):
        """A stopped sender does not send held traces."""
        sender = self.sender(batch_wait=1)
        sender.enqueue(self.traces(2))
        sender.stop()
        time.sleep(1.3)
        self.assertEqual(self.requests(), [])

    def test_zero_sends_at_once(self):
        """With batch_wait 0, each trace is sent as soon as it is queued."""
        sender = self.sender(batch_wait=0)
        sender.enqueue(self.traces(1))
        self.wait_for(lambda: self.requests() == [1], timeout=0.5)

    def test_send_reason_is_logged(self):
        """The reason for sending held traces is logged."""
        sender = self.sender(batch_wait=60, batch_size=2)
        with self.assertLogs('pilot.util.tracesender', level='INFO') as logs:
            sender.enqueue(self.traces(2))
            self.wait_for(lambda: self.requests() == [2], timeout=2.0)
            sender.enqueue(self.traces(1))
            sender.finish()
        output = '\n'.join(logs.output)
        self.assertIn('sending 2 trace(s) in 1 request(s) (requests full)', output)
        self.assertIn('sending 1 trace(s) in 1 request(s) (end of job)', output)

    def test_creation_log(self):
        """The log line for a new sender says how traces are sent."""
        job = MagicMock(jobid='11', workdir=self.workdir)
        job.infosys.queuedata.catchall = ''
        with self.assertLogs('pilot.util.tracesender', level='INFO') as logs:
            get_or_create_sender(job)
        self.assertIn('traces held for at most 300 s', '\n'.join(logs.output))
        job.jobid = '12'
        with patch.object(config.Rucio, 'trace_batch_wait', 0), self.assertLogs('pilot.util.tracesender', level='INFO') as logs:
            get_or_create_sender(job)
        self.assertIn('traces sent at once', '\n'.join(logs.output))


class TestRegistry(_StubServerTestCase):
    """One sender per job."""

    @staticmethod
    def job(jobid: str, catchall: str = '', workdir: str = '/tmp') -> MagicMock:
        """Return a job object."""
        job = MagicMock(jobid=jobid, workdir=workdir)
        job.infosys.queuedata.catchall = catchall
        return job

    def test_one_sender_per_job(self):
        """Two jobs get separate senders; the same job gets the same one."""
        first = get_or_create_sender(self.job('1'))
        second = get_or_create_sender(self.job('2'))
        self.assertIsNot(first, second)
        self.assertIs(get_or_create_sender(self.job('1')), first)
        self.assertIs(get_sender(1), first)

    def test_jobs_are_isolated(self):
        """Finishing one job does not affect the other."""
        self.state.mode = 'fail'
        get_or_create_sender(self.job('1', workdir=self.workdir)).enqueue(self.traces(5))
        get_or_create_sender(self.job('2', workdir=self.workdir))
        finish_traces(self.job('1'))
        self.assertEqual(get_lost_traces('1'), 5)
        self.assertEqual(get_lost_traces('2'), 0)
        self.assertFalse(get_sender('2').finished)

    def test_parallel_from_catchall(self):
        """PARALLEL_TRACES is read from the queue's catchall field."""
        self.assertEqual(get_or_create_sender(self.job('1', 'PARALLEL_TRACES=2')).parallel, 2)
        self.assertEqual(get_or_create_sender(self.job('2', 'PARALLEL_TRACES=9')).parallel, 2)
        self.assertEqual(get_or_create_sender(self.job('3')).parallel, 1)

    def test_no_queuedata(self):
        """A job without queue data gets the default."""
        job = types.SimpleNamespace(jobid='4', workdir='/tmp', infosys=None)
        self.assertEqual(get_or_create_sender(job).parallel, 1)

    def test_finish_without_sender(self):
        """Nothing happens for a job without traces."""
        self.assertIsNone(finish_traces(self.job('5'), MagicMock()))
        self.assertEqual(get_lost_traces('5'), 0)

    def test_lost_traces_only_after_finish(self):
        """The lost count is only known once the sender has finished."""
        self.state.mode = 'fail'
        with patch.object(config.Rucio, 'trace_batch_wait', 0):  # send at once
            sender = get_or_create_sender(self.job('6', workdir=self.workdir))
        sender.enqueue(self.traces(2))
        self.wait_for(lambda: sender.summary.failed == 2)
        self.assertEqual(get_lost_traces('6'), 0)
        finish_traces(self.job('6'), types.SimpleNamespace(graceful_stop=threading.Event()))
        self.assertEqual(get_lost_traces('6'), 2)

    def test_remove_sender(self):
        """A removed sender is gone."""
        get_or_create_sender(self.job('7'))
        remove_sender('7')
        self.assertIsNone(get_sender('7'))
        remove_sender('7')  # no error


# ---------------------------------------------------------------------------
# remote i/o traces: regression test for PanDA job 7372130338
# ---------------------------------------------------------------------------


class _RemoteFile:  # pylint: disable=too-few-public-methods
    """Minimal FileSpec stand-in for a remote i/o input file."""

    def __init__(self, index: int):
        """Create input file number index."""
        self.lfn = f'DAOD_PHYS.12345678._{index:06d}.pool.root.1'
        self.turl = f'root://eosatlas.cern.ch//eos/atlas/{self.lfn}'
        self.surl = self.turl
        self.status = 'remote_io'
        self.ddmendpoint = 'CERN-PROD_DATADISK'
        self.filesize = 1000 + index
        self.guid = f'0000-{index}'
        self.scope = 'data18_13TeV'
        self.dataset = 'data18_13TeV.dataset'


class TestJob7372130338(_StubServerTestCase):
    """200 remote i/o traces against a trace server that fails with 502 after a delay."""

    def setUp(self):
        """Write the base trace report and create the job."""
        super().setUp()
        self.path = os.path.join(self.workdir, 'base_trace_report.json')
        with open(self.path, 'w', encoding='utf-8') as _file:
            json.dump({'eventType': 'get_sm_a', 'localSite': 'CERN-PROD', 'stateReason': None}, _file)
        self.indata = [_RemoteFile(i) for i in range(200)]
        self.job = MagicMock(jobid='7372130338', workdir=self.workdir, indata=self.indata)
        self.job.infosys.queuedata.catchall = ''
        self.args = types.SimpleNamespace(graceful_stop=threading.Event())

    def test_payload_is_not_delayed_and_traces_are_reported_lost(self):
        """The traces no longer delay the payload; the failed ones are counted as lost."""
        self.state.mode = 'fail'
        self.state.delay = 0.5
        outcomes = {fspec.turl: REMOTEIO_OPENED for fspec in self.indata}
        t0 = time.time()
        process_remote_file_traces(self.path, self.job, outcomes)
        self.assertLess(time.time() - t0, 0.4)  # returns before the first reply
        self.assertEqual(self.state.requests, [])

        t0 = time.time()
        summary = finish_traces(self.job, self.args)
        self.assertLess(time.time() - t0, 5.0)  # degraded after the first failure: the wait is bounded by trace_timeout
        self.assertEqual(summary.lost, 200)
        self.assertEqual(get_lost_traces(self.job.jobid), 200)
        self.assertLessEqual(len(self.state.requests), 2)  # bulk: at most two requests, not 200
        self.assertEqual(self.stored_error(), '502')

    def test_healthy_server(self):
        """With a working server all traces arrive in two requests."""
        outcomes = {fspec.turl: REMOTEIO_OPENED for fspec in self.indata}
        process_remote_file_traces(self.path, self.job, outcomes)
        summary = finish_traces(self.job, self.args)
        self.assertEqual((summary.sent, summary.lost), (200, 0))
        self.assertEqual([len(request) for request in self.state.requests], [100, 100])
        self.assertEqual({trace['clientState'] for trace in self.received}, {'FOUND_ROOT'})


# ---------------------------------------------------------------------------
# final server update and job metrics
# ---------------------------------------------------------------------------


class TestFinalUpdateHook(unittest.TestCase):
    """The traces are finished before the final update, and the sender is removed after it."""

    def run_update_server(self, completed: bool) -> list:
        """Run update_server() with everything around it mocked, and return the call order."""
        from pilot.control import job as job_module  # pylint: disable=import-outside-toplevel
        calls = []
        job = MagicMock(jobid='42', completed=False, fileinfo=None, workdir='/tmp', state='finished')

        def _send_state(*_args, **_kwargs):
            calls.append('send_state')
            job.completed = completed
            return completed

        args = MagicMock(update_server=True)
        with patch.object(job_module, 'finish_traces', side_effect=lambda *_: calls.append('finish_traces')), \
                patch.object(job_module, 'remove_sender', side_effect=lambda *_: calls.append('remove_sender')), \
                patch.object(job_module, 'add_to_pilot_timing', side_effect=lambda _jobid, name, *_: calls.append(name)), \
                patch.object(job_module, 'send_state', side_effect=_send_state), \
                patch.object(job_module, 'handle_pilot_telemetry'), \
                patch.dict(os.environ, {'PILOT_USER': 'generic'}):
            job_module.update_server(job, args)
        return calls

    def test_order(self):
        """finish_traces() comes before the final update timing, remove_sender() after an accepted update."""
        from pilot.util.constants import PILOT_POST_FINAL_UPDATE, PILOT_PRE_FINAL_UPDATE  # pylint: disable=import-outside-toplevel
        self.assertEqual(self.run_update_server(completed=True),
                         ['finish_traces', PILOT_PRE_FINAL_UPDATE, 'send_state', PILOT_POST_FINAL_UPDATE, 'remove_sender'])

    def test_sender_kept_if_update_not_accepted(self):
        """The sender is kept for a repeated final update."""
        self.assertNotIn('remove_sender', self.run_update_server(completed=False))


class TestLostTracesInJobMetrics(_StubServerTestCase):
    """rucioTracesLost is logged, but not added to the job metrics yet (ATLAS and ePIC)."""

    def test_logged_not_reported(self):
        """The lost count is logged but not part of the job metrics string."""
        self.state.mode = 'fail'
        job = MagicMock(jobid='77', workdir=self.workdir)
        job.infosys.queuedata.catchall = ''
        get_or_create_sender(job).enqueue(self.traces(3))
        finish_traces(job)
        for module in (atlas_jobmetrics, epic_jobmetrics):
            with self.assertLogs(module.logger, level='WARNING') as logs:
                self.assertEqual(module.log_lost_traces('77'), 3)
            self.assertIn('rucioTracesLost=3', '\n'.join(logs.output))

    def test_not_in_job_metrics_string(self):
        """get_job_metrics_string() does not contain rucioTracesLost."""
        job = MagicMock(jobid='78', workdir=self.workdir, actualcorecount=0, nevents=0, neventsw=0, metadata=None,
                        dbtime=None, dbdata=None, resimevents=None, state='running', processingtype='',
                        dask_scheduler_ip=None, jupyter_session_ip=None, cpufrequencies=[])
        sender = get_or_create_sender(job)
        sender.finish()
        with patch.object(tracesender.TraceSender, 'summary', new_callable=lambda: property(lambda _self: tracesender.TraceSummary(failed=4))):
            for module in (atlas_jobmetrics, epic_jobmetrics):
                with patch.object(module, 'get_core_count', return_value=1), \
                        patch.object(module, 'add_features', side_effect=lambda metrics, *_args, **_kwargs: metrics), \
                        patch.object(module, 'add_analytics_data', side_effect=lambda metrics, *_args: metrics), \
                        patch.object(module, 'add_event_number', side_effect=lambda metrics, *_args: metrics), \
                        self.assertLogs(module.logger, level='WARNING') as logs:
                    metrics = module.get_job_metrics_string(job)
                self.assertIn('rucioTracesLost=4', '\n'.join(logs.output))
                self.assertNotIn('rucioTracesLost', metrics)


# ---------------------------------------------------------------------------
# retried downloads (rucio copytool)
# ---------------------------------------------------------------------------


class _FileDownloadState(str, enum.Enum):
    """Download states as in the Rucio 41 client."""

    PROCESSING = 'PROCESSING'
    DOWNLOAD_ATTEMPT = 'DOWNLOAD_ATTEMPT'
    DONE = 'DONE'
    FAILED = 'FAILED'
    FAIL_VALIDATE = 'FAIL_VALIDATE'
    FILE_NOT_FOUND = 'FILE_NOT_FOUND'


class _ClientWithoutSendTrace:
    """Mimics the retry loop of the Rucio download client (rucio 41.5 downloadclient.py), for a client version without _send_trace()."""

    failures = 0  # number of failed attempts before success
    succeed = True

    def __init__(self, client=None, logger=None):
        """Create the client."""
        del client, logger
        self.tracing = True
        self.sent = []

    def _notify(self, trace):
        """Call _send_trace() after an attempt, as Rucio does, if this client version has it."""
        send_trace = getattr(self, '_send_trace', None)
        if send_trace:
            send_trace(trace)

    def download_pfns(self, items, num_threads, trace_custom_fields=None, traces_copy_out=None):
        """Download with internal retries, changing one trace dictionary in place."""
        del num_threads
        item = items[0]
        trace = dict(trace_custom_fields or {})
        trace.setdefault('stateReason', 'UNKNOWN')
        traces_copy_out.append(trace)
        trace['remoteSite'] = item['rse']
        trace['protocol'] = 'davs'
        trace['clientState'] = _FileDownloadState.DOWNLOAD_ATTEMPT
        for attempt in range(self.failures):
            trace['protocol'] = 'root' if attempt == 0 else 'davs'  # rucio moves on to the next source (scheme)
            trace['clientState'] = _FileDownloadState.FAIL_VALIDATE if attempt == 1 else _FileDownloadState.FAILED
            trace['stateReason'] = f'attempt {attempt + 1} failed'
            self._notify(trace)
        trace['clientState'] = _FileDownloadState.FILE_NOT_FOUND  # not a failed attempt: must be ignored
        self._notify(trace)
        if not self.succeed:
            trace['clientState'] = _FileDownloadState.FAILED
            raise RuntimeError('NotAllFilesDownloaded')
        with open(os.path.join(item['base_dir'], item['did_name']), 'w', encoding='utf-8') as _file:
            _file.write('data')
        trace['clientState'] = _FileDownloadState.DONE
        trace['stateReason'] = 'OK'
        self._notify(trace)
        return [item]

    download_dids = download_pfns


class _FakeDownloadClient(_ClientWithoutSendTrace):
    """Rucio download client with _send_trace(), as in all client versions checked (1.29 to 41.5)."""

    def _send_trace(self, trace):
        """Rucio's own trace sending (off in the pilot)."""
        if self.tracing:
            self.sent.append(dict(trace))


class _InputFile:  # pylint: disable=too-few-public-methods
    """Minimal FileSpec stand-in for a file to download."""

    def __init__(self, workdir: str):
        """Create the file spec."""
        self.scope = 'mc23_13p6TeV'
        self.lfn = 'EVNT.123._000001.pool.root.1'
        self.ddmendpoint = 'BNL-OSG2_DATADISK'
        self.turl = 'davs://dcdoor.usatlas.bnl.gov/pnfs/file'
        self.surl = self.turl
        self.filesize = 4
        self.guid = 'abcd-1234'
        self.dataset = 'mc23_13p6TeV.dataset'
        self.workdir = workdir
        self.status_code = 0
        self.status = None


class TestRetriedDownloads(unittest.TestCase):
    """Failed Rucio-internal download attempts get a trace (clientState=STAGEIN_ATTEMPT_FAILED)."""

    def setUp(self):
        """Install the fake rucio client modules."""
        self.workdir = tempfile.mkdtemp()
        _ClientWithoutSendTrace.failures = 0
        _ClientWithoutSendTrace.succeed = True
        rucio_module = types.ModuleType('rucio')
        client_module = types.ModuleType('rucio.client')
        client_module.Client = MagicMock()
        self.download_module = types.ModuleType('rucio.client.downloadclient')
        self.download_module.DownloadClient = _FakeDownloadClient
        self.modules = patch.dict(sys.modules, {'rucio': rucio_module, 'rucio.client': client_module,
                                                'rucio.client.downloadclient': self.download_module})
        self.modules.start()
        self.sent_single = []
        self.sent_bulk = []
        self.patches = [
            patch.object(TraceReport, 'send', autospec=True, side_effect=lambda report: self.sent_single.append(dict(report))),
            patch.object(rucio_copytool, 'send_traces',
                         side_effect=lambda traces, job_id=None, workdir='': self.sent_bulk.append([dict(t) for t in traces])),
            patch.object(rucio_copytool, 'verify_catalog_checksum', return_value=('', '')),
            patch.object(rucio_copytool, 'rename_xrdlog'),
        ]
        for _patch in self.patches:
            _patch.start()

    def tearDown(self):
        """Remove the fake modules."""
        for _patch in reversed(self.patches):
            _patch.stop()
        self.modules.stop()
        shutil.rmtree(self.workdir, ignore_errors=True)

    def copy_in(self):
        """Run the rucio copytool stage-in for one file."""
        trace_report = TraceReport(eventType='get_sm', localSite='BNL-OSG2', remoteSite='BNL-OSG2_DATADISK',
                                   workdir=self.workdir)
        return rucio_copytool.copy_in([_InputFile(self.workdir)], trace_report=trace_report, workdir=self.workdir)

    def test_no_retries(self):
        """Without failed attempts, the file's trace is sent on its own, as before."""
        self.copy_in()
        self.assertEqual([trace['clientState'] for trace in self.sent_single], ['DONE'])
        self.assertEqual(self.sent_bulk, [])

    def test_success_after_retries(self):
        """Every failed attempt gets a trace, sent with the final DONE trace in one request."""
        _ClientWithoutSendTrace.failures = 2
        self.copy_in()
        self.assertEqual(self.sent_single, [])
        self.assertEqual(len(self.sent_bulk), 1)
        states = [(trace['clientState'], trace['stateReason']) for trace in self.sent_bulk[0]]
        self.assertEqual(states, [('STAGEIN_ATTEMPT_FAILED', 'attempt 1 failed'),
                                  ('STAGEIN_ATTEMPT_FAILED', 'attempt 2 failed'),
                                  ('DONE', 'OK')])
        self.assertEqual([trace['protocol'] for trace in self.sent_bulk[0]], ['root', 'davs', 'davs'])
        attempt = self.sent_bulk[0][0]
        self.assertEqual((attempt['remoteSite'], attempt['filename']),
                         ('BNL-OSG2_DATADISK', 'EVNT.123._000001.pool.root.1'))
        self.assertIsNotNone(attempt['timeEnd'])

    def test_failure_after_all_retries(self):
        """The last failed attempt is the final failure, so it gets no extra trace."""
        _ClientWithoutSendTrace.failures = 2
        _ClientWithoutSendTrace.succeed = False
        with self.assertRaises(PilotException):
            self.copy_in()
        self.assertEqual(len(self.sent_bulk), 1)
        traces = self.sent_bulk[0]
        self.assertEqual(len(traces), 2)
        self.assertEqual((traces[0]['clientState'], traces[0]['stateReason']), ('STAGEIN_ATTEMPT_FAILED', 'attempt 1 failed'))
        self.assertNotEqual(traces[1]['clientState'], 'DONE')

    def test_single_failed_attempt(self):
        """One failed attempt that is also the final failure: only the final trace."""
        _ClientWithoutSendTrace.failures = 1
        _ClientWithoutSendTrace.succeed = False
        with self.assertRaises(PilotException):
            self.copy_in()
        self.assertEqual(self.sent_bulk, [])
        self.assertEqual(len(self.sent_single), 1)

    def test_rucio_tracing_stays_off(self):
        """The wrapper calls Rucio's method, which still sends nothing since tracing is off."""
        client = _FakeDownloadClient()
        client.tracing = False
        attempts = []
        self.assertTrue(rucio_copytool.capture_failed_attempts(client, attempts))
        client._send_trace({'clientState': _FileDownloadState.FAILED, 'stateReason': 'x'})
        self.assertEqual((len(attempts), client.sent), (1, []))

    def test_client_without_send_trace(self):
        """A client version without _send_trace(): only the final trace, as before."""
        _ClientWithoutSendTrace.failures = 2
        self.download_module.DownloadClient = _ClientWithoutSendTrace
        self.copy_in()
        self.assertEqual([trace['clientState'] for trace in self.sent_single], ['DONE'])
        self.assertEqual(self.sent_bulk, [])

    def test_no_attempt_list(self):
        """Nothing is wrapped without a list to record into."""
        client = _FakeDownloadClient()
        original = client._send_trace
        self.assertFalse(rucio_copytool.capture_failed_attempts(client, None))
        self.assertEqual(client._send_trace, original)

    def test_snapshot_is_a_copy(self):
        """Later changes to Rucio's trace dictionary do not change a recorded attempt."""
        trace = {'clientState': _FileDownloadState.FAILED, 'stateReason': 'first', 'protocol': 'root', 'remoteSite': 'RSE'}
        snapshot = rucio_copytool.get_attempt_snapshot(trace)
        trace['stateReason'] = 'second'
        self.assertEqual((snapshot['clientState'], snapshot['stateReason']), ('FAILED', 'first'))

    def test_other_states_are_ignored(self):
        """Only FAILED and FAIL_VALIDATE are failed attempts."""
        for state in ('DONE', 'FILE_NOT_FOUND', 'FOUND_IN_PCACHE', 'ALREADY_DONE', None):
            self.assertIsNone(rucio_copytool.get_attempt_snapshot({'clientState': state}), state)
        self.assertIsNotNone(rucio_copytool.get_attempt_snapshot({'clientState': 'FAIL_VALIDATE'}))

    def test_recording_error_does_not_break_the_download(self):
        """An error while recording an attempt is logged, and Rucio's method is still called."""
        client = _FakeDownloadClient()
        attempts = []
        rucio_copytool.capture_failed_attempts(client, attempts)
        with patch.object(rucio_copytool, 'get_attempt_snapshot', side_effect=KeyError('x')):
            client._send_trace({'clientState': 'FAILED'})
        self.assertEqual(attempts, [])
        self.assertEqual(len(client.sent), 1)


if __name__ == '__main__':
    unittest.main()
