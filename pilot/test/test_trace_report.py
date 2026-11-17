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

"""Tests for the per-job Rucio trace report (pilot.util.tracesummary and its use in the trace sender)."""

import enum
import os
import time
import unittest
from unittest.mock import patch

from pilot.test.test_bulk_traces import _StubServerTestCase
from pilot.util import tracesender
from pilot.util.tracesummary import (
    BANNER,
    MAX_FILENAME,
    MAX_REASON,
    MAX_ROWS,
    TraceRecord,
    TraceStats,
    format_trace_report,
    shorten,
)

COUNTS = 'rucio traces of job 42: sent=1, failed=0, abandoned=0, invalid=0, time=1.0 s'


class FileDownloadState(enum.Enum):
    """Like the Rucio 35+ client state enum."""

    DONE = 'DONE'


def record(i: int = 0, result: str = 'sent', **fields) -> TraceRecord:
    """Return the record of a trace with some default fields."""
    trace = {'eventType': 'get_sm', 'clientState': 'DONE', 'filename': f'file{i}', 'remoteSite': 'CERN-PROD_DATADISK',
             'stateReason': 'OK'}
    trace.update(fields)
    return TraceRecord.from_trace(trace, result)


def body(lines: list) -> list:
    """Return the report lines without the banners and the prefix."""
    return [line[4:] for line in lines[1:-1]]


class TestShorten(unittest.TestCase):
    """Shortening of file names and reasons."""

    def test_short_text(self):
        """A short text is kept."""
        self.assertEqual(shorten('abc', 10), 'abc')
        self.assertEqual(shorten('a' * 10, 10), 'a' * 10)

    def test_end(self):
        """By default the end is cut."""
        self.assertEqual(shorten('abcdefghijkl', 10), 'abcdefg...')

    def test_middle(self):
        """File names are cut in the middle, keeping both ends."""
        self.assertEqual(shorten('abcdefghijkl', 10, middle=True), 'abc...ijkl')
        self.assertEqual(len(shorten('x' * 200, MAX_FILENAME, middle=True)), MAX_FILENAME)


class TestTraceRecord(unittest.TestCase):
    """What the report keeps of a trace."""

    def test_fields(self):
        """The fields come from the trace."""
        rec = record(3, result='queued', eventType='put_sm', clientState='SERVICE_ERROR', stateReason='HTTP 403')
        self.assertEqual((rec.event, rec.state, rec.filename, rec.rse, rec.reason, rec.result),
                         ('put_sm', 'SERVICE_ERROR', 'file3', 'CERN-PROD_DATADISK', 'HTTP 403', 'queued'))

    def test_ok_reasons_are_hidden(self):
        """A reason that only says OK or DONE (or nothing) is not shown."""
        for reason in ('OK', 'ok', 'DONE', '', None, 'None', '  '):
            self.assertEqual(record(stateReason=reason).reason, '', reason)

    def test_long_reason(self):
        """A long reason is cut, with its white space (newlines) collapsed."""
        rec = record(stateReason='Copy failed\n  Details: ' + 'x' * 300)
        self.assertEqual(len(rec.reason), MAX_REASON)
        self.assertTrue(rec.reason.startswith('Copy failed Details: x'))
        self.assertTrue(rec.reason.endswith('...'))

    def test_long_filename(self):
        """A long file name keeps its beginning and its end (e.g. the attempt number and extension)."""
        name = 'a' * 40 + 'b' * 40 + '.job.log.tgz'
        rec = record(filename=name)
        self.assertEqual(len(rec.filename), MAX_FILENAME)
        self.assertTrue(rec.filename.startswith('aaaa'))
        self.assertTrue(rec.filename.endswith('bbbb.job.log.tgz'))

    def test_enum_state(self):
        """An enum clientState (Rucio 35+) is shown by its value."""
        self.assertEqual(record(clientState=FileDownloadState.DONE).state, 'DONE')

    def test_rse_fallback(self):
        """Without remoteSite, localSite is used."""
        self.assertEqual(record(remoteSite='', localSite='CERN-PROD').rse, 'CERN-PROD')

    def test_missing_fields(self):
        """Missing fields are shown as '?'."""
        rec = TraceRecord.from_trace({})
        self.assertEqual((rec.event, rec.state, rec.filename, rec.rse), ('?', '?', '?', '?'))

    def test_sent(self):
        """Only the result 'sent' counts as sent."""
        self.assertTrue(record(result='sent').sent)
        for result in ('queued', 'failed (HTTP 502)', 'abandoned', 'invalid'):
            self.assertFalse(record(result=result).sent)


class TestFormatTraceReport(unittest.TestCase):
    """Layout of the report."""

    def test_banner_and_prefix(self):
        """The report is framed by banners and every line in between starts with '***'."""
        lines = format_trace_report('42', COUNTS, [record()], TraceStats(requests=1))
        self.assertEqual((lines[0], lines[-1]), (BANNER, BANNER))
        self.assertTrue(all(line.startswith('***') for line in lines[1:-1]))
        self.assertEqual(lines[1], '*** RUCIO TRACE REPORT - job 42')
        self.assertEqual(lines[2], f'*** {COUNTS}')

    def test_statistics_line(self):
        """Requests, parallelism, degradation, sending time and final wait are shown."""
        lines = format_trace_report('42', COUNTS, [record()], TraceStats(3, 1.234, 0.5, 2, True))
        self.assertEqual(body(lines)[2], 'requests: 3 (up to 2 at a time, degraded), time sending: 1.2 s, final wait: 0.5 s')
        lines = format_trace_report('42', COUNTS, [record()], TraceStats(1, 0.0, 0.0, 1, False))
        self.assertIn('(up to 1 at a time, not degraded)', body(lines)[2])

    def test_table(self):
        """One aligned row per trace, in order, with a reason line where there is one."""
        records = [record(0), record(1, result='failed (HTTP 502)', eventType='put_sm', clientState='SERVICE_ERROR',
                                     stateReason='Permission refused', filename='a_much_longer_file_name.root')]
        lines = body(format_trace_report('42', COUNTS, records, TraceStats()))
        self.assertEqual(lines[3], '')
        self.assertEqual(lines[4].split(), ['#', 'event', 'state', 'file', 'rse', 'result'])
        self.assertEqual(lines[5].split(), ['1', 'get_sm', 'DONE', 'file0', 'CERN-PROD_DATADISK', 'sent'])
        self.assertEqual(lines[6].split()[:5], ['2', 'put_sm', 'SERVICE_ERROR', 'a_much_longer_file_name.root', 'CERN-PROD_DATADISK'])
        self.assertTrue(lines[6].endswith('failed (HTTP 502)'))
        self.assertEqual(lines[7], '   reason: Permission refused')  # under the event column
        self.assertEqual(len(lines), 8)
        # the columns line up
        self.assertEqual(lines[4].index('rse'), lines[5].index('CERN-PROD_DATADISK'))
        self.assertEqual(lines[5].index('CERN-PROD_DATADISK'), lines[6].index('CERN-PROD_DATADISK'))

    def test_reason_indent_with_two_digit_numbers(self):
        """With ten or more rows the reason lines are still under the event column."""
        records = [record(i) for i in range(9)] + [record(9, stateReason='slow')]
        lines = body(format_trace_report('42', COUNTS, records, TraceStats()))
        self.assertEqual(lines[-1].index('reason'), lines[-2].index('get_sm'))

    def test_no_traces(self):
        """A job without traces says so."""
        lines = body(format_trace_report('42', COUNTS, [], TraceStats()))
        self.assertEqual(lines[-1], 'no traces were sent for this job')
        lines = body(format_trace_report('42', COUNTS, [], TraceStats(), disabled=True))
        self.assertEqual(lines[-1], 'no traces were sent for this job (trace sending is turned off)')

    def test_row_limit(self):
        """Up to MAX_ROWS traces are listed one per row."""
        lines = body(format_trace_report('42', COUNTS, [record(i) for i in range(MAX_ROWS)], TraceStats()))
        self.assertEqual(len(lines), 5 + MAX_ROWS)

    def test_grouped(self):
        """Beyond MAX_ROWS the traces are counted per event, state and result, and the unsent ones listed."""
        records = [record(i) for i in range(MAX_ROWS)] + [record(100, result='abandoned', eventType='put_sm')]
        lines = body(format_trace_report('42', COUNTS, records, TraceStats()))
        self.assertEqual(lines[4], f'{MAX_ROWS + 1} traces, counted per event, state and result:')
        self.assertEqual(lines[5].split(), ['count', 'event', 'state', 'result'])
        self.assertEqual(lines[6].split(), [str(MAX_ROWS), 'get_sm', 'DONE', 'sent'])
        self.assertEqual(lines[7].split(), ['1', 'put_sm', 'DONE', 'abandoned'])
        self.assertEqual(lines[9], 'traces that were not sent (1):')
        self.assertEqual(lines[11].split(), [str(MAX_ROWS + 1), 'put_sm', 'DONE', 'file100', 'CERN-PROD_DATADISK', 'abandoned'])
        self.assertEqual(len(lines), 12)

    def test_grouped_order(self):
        """The groups are listed with the most frequent first."""
        records = [record(0, result='abandoned', eventType='put_sm')] + [record(i) for i in range(1, MAX_ROWS + 1)]
        lines = body(format_trace_report('42', COUNTS, records, TraceStats()))
        self.assertEqual(lines[6].split(), [str(MAX_ROWS), 'get_sm', 'DONE', 'sent'])
        self.assertEqual(lines[7].split(), ['1', 'put_sm', 'DONE', 'abandoned'])

    def test_grouped_all_sent(self):
        """Without unsent traces there is no list of them."""
        lines = body(format_trace_report('42', COUNTS, [record(i) for i in range(MAX_ROWS + 1)], TraceStats()))
        self.assertFalse([line for line in lines if 'not sent' in line])
        self.assertEqual(len(lines), 7)

    def test_unsent_limit(self):
        """At most MAX_ROWS unsent traces are listed."""
        records = [record(i, result='failed (HTTP 502)') for i in range(MAX_ROWS + 10)]
        lines = body(format_trace_report('42', COUNTS, records, TraceStats()))
        self.assertIn(f'traces that were not sent (first {MAX_ROWS} of {MAX_ROWS + 10}):', lines)
        start = lines.index(f'traces that were not sent (first {MAX_ROWS} of {MAX_ROWS + 10}):')
        self.assertEqual(len(lines) - start - 2, MAX_ROWS)


class TestSenderReport(_StubServerTestCase):
    """The sender logs the report once, when it finishes."""

    def finish(self, sender) -> tuple:
        """Finish the sender and return the summary and the logged report lines."""
        with self.assertLogs(tracesender.logger, level='INFO') as logs:
            summary = sender.finish()
        lines = [entry.getMessage() for entry in logs.records]
        start = lines.index(BANNER)
        return summary, lines[start:], logs.records[start:]

    def rows(self, lines: list) -> list:
        """Return the table rows of a report (split into columns)."""
        start = [line.split()[1:] for line in lines].index(['#', 'event', 'state', 'file', 'rse', 'result'])
        return [line.split()[1:] for line in lines[start + 1:-1] if 'reason:' not in line]

    def test_sent(self):
        """Sent traces are listed as sent, with the request statistics."""
        sender = self.sender()
        sender.enqueue(self.traces(3))
        self.wait_for(lambda: len(self.received) == 3)
        _, lines, _ = self.finish(sender)
        self.assertEqual([row[0] for row in self.rows(lines)], ['1', '2', '3'])
        self.assertEqual({row[-1] for row in self.rows(lines)}, {'sent'})
        self.assertIn('rucio traces of job 1234: sent=3, failed=0, abandoned=0, invalid=0', lines[2])
        self.assertTrue(lines[3].startswith('*** requests: 1 (up to 1 at a time, not degraded), time sending: '))

    def test_failed(self):
        """Traces in a failed request show the HTTP status, and the counts line is a warning."""
        self.state.mode = 'fail'
        sender = self.sender()
        sender.enqueue(self.traces(2))
        self.wait_for(lambda: sender.degraded)
        _, lines, records = self.finish(sender)
        self.assertEqual([row[-3:] for row in self.rows(lines)], [['failed', '(HTTP', '502)']] * 2)
        self.assertEqual(records[2].levelname, 'WARNING')
        self.assertEqual({entry.levelname for entry in records[:2] + records[3:]}, {'INFO'})
        self.assertIn('at a time, degraded)', lines[3])

    def test_failed_without_status(self):
        """A failed request without an HTTP status shows 'failed'."""
        sender = self.sender()
        self.server.shutdown()
        self.server.server_close()
        sender.enqueue(self.traces(1))
        self.wait_for(lambda: sender.degraded)
        _, lines, _ = self.finish(sender)
        self.assertEqual(self.rows(lines)[0][-1], 'failed')

    def test_abandoned(self):
        """Traces not sent by the end of the final wait are listed as abandoned."""
        self.state.delay = 1.0
        sender = self.sender(final_wait=0.3)
        sender.enqueue(self.traces(2))
        summary, lines, _ = self.finish(sender)
        self.assertEqual(summary.abandoned, 2)
        self.assertEqual([row[-1] for row in self.rows(lines)], ['abandoned', 'abandoned'])
        time.sleep(1.0)  # the late reply does not change the report data
        self.assertEqual(sender.finish().abandoned, 2)

    def test_invalid(self):
        """Traces that could not be verified are listed as invalid, in queue order."""
        sender = self.sender()
        traces = self.traces(3)
        traces[1]['eventType'] = ''
        sender.enqueue(traces[:2])
        sender.try_enqueue(traces[2:])
        self.wait_for(lambda: len(self.received) == 2)
        _, lines, _ = self.finish(sender)
        rows = self.rows(lines)
        self.assertEqual(sorted(row[-1] for row in rows), ['invalid', 'sent', 'sent'])
        self.assertEqual([row[0] for row in rows], ['1', '2', '3'])

    def test_reason(self):
        """A failed transfer attempt shows its reason."""
        sender = self.sender()
        trace = self.traces(1)[0]
        trace.update(clientState='SERVICE_ERROR', stateReason='HTTP 403 : Permission refused')
        sender.enqueue([trace])
        self.wait_for(lambda: len(self.received) == 1)
        _, lines, _ = self.finish(sender)
        self.assertIn('***    reason: HTTP 403 : Permission refused', lines)

    def test_times(self):
        """The time spent sending and the time spent waiting for the remaining traces are shown."""
        self.state.delay = 0.5
        sender = self.sender()
        sender.enqueue(self.traces(1))
        _, lines, _ = self.finish(sender)
        sending = float(lines[3].split('time sending: ')[1].split(' s')[0])
        final_wait = float(lines[3].split('final wait: ')[1].split(' s')[0])
        self.assertGreaterEqual(sending, 0.4)
        self.assertGreater(final_wait, 0.2)

    def test_pending_and_in_flight_abandoned(self):
        """Both the traces of a request in flight and the traces still queued are listed as abandoned."""
        self.state.delay = 1.0
        sender = self.sender(batch_size=1, final_wait=0.3)
        sender.enqueue(self.traces(2))
        self.wait_for(lambda: self.state.active == 1)
        summary, lines, _ = self.finish(sender)
        self.assertEqual(summary.abandoned, 2)
        self.assertEqual([row[-1] for row in self.rows(lines)], ['abandoned', 'abandoned'])

    def test_logged_once(self):
        """Calling finish() again (repeated final update) does not log the report again."""
        sender = self.sender()
        self.finish(sender)
        # (assertNoLogs needs Python 3.10)
        with patch.object(tracesender.logger, 'info') as info, patch.object(tracesender.logger, 'warning') as warning:
            sender.finish()
        info.assert_not_called()
        warning.assert_not_called()

    def test_no_traces(self):
        """A job without traces gets a short report."""
        _, lines, _ = self.finish(self.sender())
        self.assertEqual(lines[-2], '*** no traces were sent for this job')

    def test_disabled(self):
        """With trace sending turned off, the report says so."""
        os.environ['PILOT_USE_RUCIO_TRACES'] = 'False'
        sender = self.sender()
        sender.enqueue(self.traces(2))
        _, lines, _ = self.finish(sender)
        self.assertEqual(lines[-2], '*** no traces were sent for this job (trace sending is turned off)')

    def test_parallel_requests(self):
        """With parallel requests every trace still gets its own result."""
        sender = self.sender(parallel=2, batch_size=2)
        sender.enqueue(self.traces(6))
        self.wait_for(lambda: len(self.received) == 6)
        summary, lines, _ = self.finish(sender)
        self.assertEqual(summary.sent, 6)
        self.assertEqual([row[-1] for row in self.rows(lines)], ['sent'] * 6)
        self.assertIn('requests: 3 (up to 2 at a time', lines[3])


if __name__ == '__main__':
    unittest.main()
