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

"""Rucio trace report of a job: one block in the pilot log listing every trace and what happened to it.

The background trace sender (:mod:`pilot.util.tracesender`) keeps a :class:`TraceRecord` per trace
and logs the report with :func:`format_trace_report` once the last upload attempt has been made,
just before the final server update. Example::

    ******************************************************************************************
    *** RUCIO TRACE REPORT - job 7379084940
    *** rucio traces of job 7379084940: sent=4, failed=0, abandoned=0, invalid=0, time=114.2 s
    *** requests: 4 (up to 1 at a time, not degraded), time sending: 0.2 s, final wait: 0.0 s
    ***
    ***  #  event   state          file                               rse                 result
    ***  1  get_sm  DONE           HITS.06828093._000096.pool.root.1  CERN-PROD_DATADISK  sent
    ***  ...
    ******************************************************************************************

Up to :data:`MAX_ROWS` traces are listed one per row. Beyond that, the traces are counted per
event, state and result, followed by the traces that were not sent (at most :data:`MAX_ROWS`).
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

MAX_ROWS = 50  # maximum number of trace rows in the report
MAX_FILENAME = 60  # longer file names are shortened in the middle
MAX_REASON = 100  # longer reasons are cut
BANNER = '*' * 90
PREFIX = '***'
OK_REASONS = ('', 'OK', 'DONE', 'NONE', 'NULL')

# results
QUEUED = 'queued'
SENT = 'sent'
FAILED = 'failed'
ABANDONED = 'abandoned'
INVALID = 'invalid'


def _text(value: Any) -> str:
    """Return a field value as text (enum values are unpacked, None is empty).

    Args:
        value: Field value.

    Returns:
        The text.
    """
    value = getattr(value, 'value', value)

    return '' if value is None else str(value)


def shorten(text: str, limit: int, middle: bool = False) -> str:
    """Shorten a text to at most limit characters.

    Args:
        text: The text.
        limit: Maximum length (at least 5).
        middle: Cut the middle (for file names) instead of the end.

    Returns:
        The text, with '...' where it was cut.
    """
    if len(text) <= limit:
        return text
    if middle:
        head = (limit - 3) // 2
        tail = limit - 3 - head
        return f'{text[:head]}...{text[-tail:]}'

    return f'{text[:limit - 3]}...'


@dataclass
class TraceRecord:
    """What the report needs to know about one trace.

    Attributes:
        event: eventType (e.g. get_sm, put_sm).
        state: clientState (e.g. DONE, SERVICE_ERROR).
        filename: File name.
        rse: RSE the trace is about (remoteSite, or localSite if there is none).
        reason: stateReason, if it says more than OK/DONE (shortened).
        result: What happened to the trace (queued, sent, failed (...), abandoned, invalid).
    """

    event: str
    state: str
    filename: str
    rse: str
    reason: str = ''
    result: str = QUEUED

    @classmethod
    def from_trace(cls, trace: dict[str, Any], result: str = QUEUED) -> TraceRecord:
        """Create the record of a trace.

        Args:
            trace: Trace report or prepared trace.
            result: Initial result.

        Returns:
            The record.
        """
        reason = _text(trace.get('stateReason')).strip()
        if reason.upper() in OK_REASONS:
            reason = ''

        return cls(event=_text(trace.get('eventType')) or '?',
                   state=_text(trace.get('clientState')) or '?',
                   filename=shorten(_text(trace.get('filename')) or '?', MAX_FILENAME, middle=True),
                   rse=_text(trace.get('remoteSite')) or _text(trace.get('localSite')) or '?',
                   reason=shorten(' '.join(reason.split()), MAX_REASON),
                   result=result)

    @property
    def sent(self) -> bool:
        """Return True if the trace reached the trace server.

        Returns:
            True if sent.
        """
        return self.result == SENT


@dataclass
class TraceStats:
    """Request statistics of a trace sender, for the report.

    Attributes:
        requests: Number of requests sent.
        sending_time: Sum of the request durations in seconds.
        final_wait: Time in seconds spent waiting for the remaining traces before the final update.
        parallel: Number of requests sent at the same time while healthy.
        degraded: True if requests failed or were slow.
        started: Time (epoch seconds) the sender was created.
    """

    requests: int = 0
    sending_time: float = 0.0
    final_wait: float = 0.0
    parallel: int = 1
    degraded: bool = False
    started: float = 0.0


def _columns(header: tuple[str, ...], rows: list[tuple[str, ...]]) -> list[str]:
    """Return aligned lines: the first column right-aligned (a number), the others left-aligned.

    Args:
        header: Column names.
        rows: Rows with one text per column.

    Returns:
        One line for the header and one per row.
    """
    widths = [max(len(row[i]) for row in [header] + rows) for i in range(len(header))]

    return ['  '.join([row[0].rjust(widths[0])] + [cell.ljust(width) for cell, width in zip(row[1:], widths[1:])]).rstrip()
            for row in [header] + rows]


def _table(records: list[tuple[int, TraceRecord]]) -> list[str]:
    """Return the table for numbered records, with a reason line under a row where there is one.

    Args:
        records: (number, record) pairs.

    Returns:
        Table lines (header included).
    """
    rows = [(str(number), rec.event, rec.state, rec.filename, rec.rse, rec.result) for number, rec in records]
    lines = _columns(('#', 'event', 'state', 'file', 'rse', 'result'), rows)
    indent = ' ' * (len(lines[0]) - len(lines[0].lstrip()) + 3)  # under the event column
    table = [lines[0]]
    for line, (_, rec) in zip(lines[1:], records):
        table.append(line)
        if rec.reason:
            table.append(f'{indent}reason: {rec.reason}')

    return table


def _grouped(records: list[TraceRecord]) -> list[str]:
    """Return the counts per event, state and result.

    Args:
        records: The records.

    Returns:
        One line per group, most frequent first (header included).
    """
    groups = Counter((rec.event, rec.state, rec.result) for rec in records)
    rows = [(str(count), event, state, result) for (event, state, result), count in groups.most_common()]

    return _columns(('count', 'event', 'state', 'result'), rows)


def format_trace_report(job_id: str, counts_line: str, records: list[TraceRecord], stats: TraceStats,
                        disabled: bool = False) -> list[str]:
    """Return the lines of the trace report of a job.

    Args:
        job_id: PanDA job id.
        counts_line: The one-line summary (``rucio traces of job ...``), kept for log searches.
        records: One record per trace, in the order the traces were queued.
        stats: Request statistics.
        disabled: True if trace sending is turned off.

    Returns:
        The report lines, including the banner lines.
    """
    state = 'degraded' if stats.degraded else 'not degraded'
    body = [f'RUCIO TRACE REPORT - job {job_id}',
            counts_line,
            f'requests: {stats.requests} (up to {stats.parallel} at a time, {state}), '
            f'time sending: {stats.sending_time:.1f} s, final wait: {stats.final_wait:.1f} s']

    if not records:
        body.append('no traces were sent for this job' + (' (trace sending is turned off)' if disabled else ''))
    elif len(records) <= MAX_ROWS:
        body.append('')
        body.extend(_table(list(enumerate(records, start=1))))
    else:
        body.append('')
        body.append(f'{len(records)} traces, counted per event, state and result:')
        body.extend(f'  {line}' for line in _grouped(records))
        unsent = [(number, rec) for number, rec in enumerate(records, start=1) if not rec.sent]
        if unsent:
            body.append('')
            shown = f'first {MAX_ROWS} of {len(unsent)}' if len(unsent) > MAX_ROWS else f'{len(unsent)}'
            body.append(f'traces that were not sent ({shown}):')
            body.extend(_table(unsent[:MAX_ROWS]))

    return [BANNER] + [f'{PREFIX} {line}'.rstrip() for line in body] + [BANNER]
