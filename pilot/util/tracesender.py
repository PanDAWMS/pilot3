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

"""Background sender for Rucio traces.

Traces handed to a :class:`TraceSender` are sent by a daemon thread, in
requests of at most ``[Rucio] trace_batch_size`` traces, so that sending them
never delays the payload. There is one sender per job, kept in a registry
keyed by the job id (see :func:`get_or_create_sender`).

Traces are held for a while so that several can go in one request, since
copytools that download one file at a time queue one trace every few seconds.
The waiting traces are sent when the first of them:

- has waited ``[Rucio] trace_batch_wait`` seconds;
- is joined by enough traces to fill one round of requests
  (``trace_batch_size`` traces per request);
- is still waiting when the job's traces are finished (:meth:`TraceSender.finish`).

With ``trace_batch_wait: 0`` the traces are sent as soon as they are queued, so
that a request only carries several traces when they were queued together, or
piled up while a request was in flight.

Number of requests sent at the same time:

- ``PARALLEL_TRACES=<n>`` in the queue's catchall field asks for ``n``
  requests, capped at ``[Rucio] trace_max_parallel``. Without the key, or with
  an invalid value, one request at a time is sent.
- After the first failed request, or a request slower than
  ``[Rucio] trace_slow_threshold`` seconds, the sender is *degraded* and sends
  one request at a time for the rest of the job.

The traces must be sent before the final server update, since the job
metrics are built there. :func:`finish_traces` is therefore called just
before the final update, and the remaining traces are sent first, bounded by:

- ``[Rucio] trace_final_wait`` seconds if sending is healthy;
- ``[Rucio] trace_timeout`` seconds if it is degraded (also when it degrades
  during the wait), so that the traces queued at the end of the job, e.g. by
  stage-out, still get one chance;
- graceful stop (no wait if it is already set).

The traces that have not been sent then are abandoned. Abandoned traces, and
traces in failed requests, are counted as lost. ``finish()`` then logs the
job's trace report, which lists every trace and what happened to it (see
:mod:`pilot.util.tracesummary`).

Copytools send their traces with :meth:`TraceReport.send`, which hands them
to the job's sender when there is one (this module registers itself with
:func:`~pilot.util.tracereport.set_background_sender`). The stage-in and
stage-out entry points create the sender with :func:`get_or_create_sender`.
Traces that a middleware container script wrote to the spool file are sent
with :func:`collect_spooled_traces` after the container has finished.
"""

from __future__ import annotations
import logging
import threading
import time

from collections import (
    Counter,
    deque
)
from collections.abc import Iterable
from dataclasses import (
    dataclass,
    replace
)
from typing import Any

from pilot.util.auxiliary import get_key_value
from pilot.util.tracereport import (
    DEFAULT_TRACE_BATCH_SIZE,
    DEFAULT_TRACE_TIMEOUT,
    get_trace_error_code,
    get_trace_setting,
    post_traces,
    read_trace_spool,
    send_traces,
    set_background_sender,
    split_traces,
    store_trace_error,
    traces_disabled
)
from pilot.util.tracesummary import (
    ABANDONED,
    FAILED,
    INVALID,
    SENT,
    TraceRecord,
    TraceStats,
    format_trace_report
)

logger = logging.getLogger(__name__)

DEFAULT_PARALLEL_TRACES = 1
DEFAULT_TRACE_MAX_PARALLEL = 2
DEFAULT_TRACE_SLOW_THRESHOLD = 20
DEFAULT_TRACE_FINAL_WAIT = 600
DEFAULT_TRACE_BATCH_WAIT = 300
PARALLEL_TRACES_KEY = 'parallel_traces'  # catchall key PARALLEL_TRACES=<n> (matched case-insensitively)
WAIT_INTERVAL = 1.0  # seconds between graceful stop checks while waiting for the remaining traces
JOIN_MARGIN = 30  # extra seconds to wait for a parallel request beyond its time-out

# sender states
RUNNING = 'running'
STOPPED = 'stopped'  # stopped without counting the remaining traces (see TraceSender.stop())
FINISHED = 'finished'  # stopped by TraceSender.finish(), the counts are final


@dataclass(frozen=True)
class TraceSettings:
    """Settings of a trace sender (from the ``[Rucio]`` config section).

    Attributes:
        batch_size: Maximum number of traces per request.
        max_parallel: Maximum number of requests sent at the same time.
        timeout: Request time-out in seconds; also the maximum time to wait for the remaining traces
            before the final update when the sender is degraded.
        slow_threshold: A request taking longer than this (seconds) degrades the sender.
        final_wait: Maximum time in seconds to wait for the remaining traces before the final update.
        batch_wait: Maximum time in seconds a trace is held for more traces to join it (0: send at once).
    """

    batch_size: int = DEFAULT_TRACE_BATCH_SIZE
    max_parallel: int = DEFAULT_TRACE_MAX_PARALLEL
    timeout: int = DEFAULT_TRACE_TIMEOUT
    slow_threshold: int = DEFAULT_TRACE_SLOW_THRESHOLD
    final_wait: int = DEFAULT_TRACE_FINAL_WAIT
    batch_wait: int = DEFAULT_TRACE_BATCH_WAIT

    @classmethod
    def from_config(cls) -> TraceSettings:
        """Return the settings from the config file, with defaults for missing or invalid values.

        Returns:
            The settings.
        """
        return cls(batch_size=get_trace_setting('trace_batch_size', DEFAULT_TRACE_BATCH_SIZE),
                   max_parallel=get_trace_setting('trace_max_parallel', DEFAULT_TRACE_MAX_PARALLEL),
                   timeout=get_trace_setting('trace_timeout', DEFAULT_TRACE_TIMEOUT),
                   slow_threshold=get_trace_setting('trace_slow_threshold', DEFAULT_TRACE_SLOW_THRESHOLD),
                   final_wait=get_trace_setting('trace_final_wait', DEFAULT_TRACE_FINAL_WAIT),
                   batch_wait=get_trace_setting('trace_batch_wait', DEFAULT_TRACE_BATCH_WAIT, minimum=0))


@dataclass
class TraceSummary:
    """Counts of a trace sender.

    Attributes:
        sent: Traces in accepted requests.
        failed: Traces in failed requests.
        invalid: Traces dropped since they could not be verified.
        abandoned: Traces not sent (or not confirmed) when the sender finished.
        elapsed: Seconds from the creation of the sender until it finished.
        degraded: True if requests failed or were slow.
    """

    sent: int = 0
    failed: int = 0
    invalid: int = 0
    abandoned: int = 0
    elapsed: float = 0.0
    degraded: bool = False

    @property
    def lost(self) -> int:
        """Return the number of traces that did not reach the trace server.

        Returns:
            Failed plus abandoned traces.
        """
        return self.failed + self.abandoned


@dataclass(frozen=True)
class TraceSnapshot:
    """Final state of the trace sender of a job, for the pilot telemetry (see :func:`get_trace_snapshot`).

    Attributes:
        summary: Final counts.
        stats: Request statistics.
        records: One record per trace, in the order the traces were queued.
        request_errors: Number of failed requests per error code (HTTP status code, or
            :data:`~pilot.util.tracereport.GENERIC_TRACE_ERROR` if the reply had none).
    """

    summary: TraceSummary
    stats: TraceStats
    records: tuple[TraceRecord, ...]
    request_errors: dict[int, int]


@dataclass(frozen=True)
class BatchResult:
    """Outcome of one trace request.

    Attributes:
        size: Number of traces in the request.
        accepted: True if the server accepted the request.
        elapsed: Duration of the request in seconds.
        reply: Server reply or error message.
    """

    size: int
    accepted: bool
    elapsed: float
    reply: Any


def get_parallel_traces(catchall: str | None, max_parallel: int) -> int:
    """Return the number of trace requests to send at the same time.

    Args:
        catchall: Queue catchall field, possibly containing ``PARALLEL_TRACES=<n>``.
        max_parallel: Upper limit (``[Rucio] trace_max_parallel``).

    Returns:
        ``min(n, max_parallel)`` for a positive integer ``n``, otherwise 1.
    """
    value = get_key_value(catchall.lower(), key=PARALLEL_TRACES_KEY) if catchall else None
    try:
        parallel = int(value)
    except (TypeError, ValueError):
        return DEFAULT_PARALLEL_TRACES
    if parallel < 1:
        return DEFAULT_PARALLEL_TRACES

    return min(parallel, max(max_parallel, 1))


def _is_set(event: threading.Event | None) -> bool:
    """Return True if the event exists and is set.

    Args:
        event: Event, or None.

    Returns:
        True if set.
    """
    return bool(event is not None and event.is_set())


class TraceSender:  # pylint: disable=too-many-instance-attributes  # queue, hold and request state of one job
    """Sends the traces of one job in the background."""

    def __init__(self, job_id: str, workdir: str, parallel: int = DEFAULT_PARALLEL_TRACES,
                 settings: TraceSettings | None = None) -> None:
        """Create the sender. The worker thread is started when the first traces arrive.

        Args:
            job_id: PanDA job id.
            workdir: Job work directory (for the trace error file).
            parallel: Number of requests to send at the same time while healthy.
            settings: Sender settings (default: from the config file).
        """
        self.job_id = job_id
        self.workdir = workdir
        self.settings = settings or TraceSettings.from_config()
        self._pending: deque = deque()  # (prepared trace, record) pairs
        self._in_flight: list[tuple[dict[str, Any], TraceRecord]] = []
        self._records: list[TraceRecord] = []  # one per trace, for the trace report
        self._stats = TraceStats(parallel=max(parallel, 1), started=time.time())  # request statistics and start time
        self._cond = threading.Condition()
        self._thread: threading.Thread | None = None
        self._state = RUNNING
        self._summary = TraceSummary()  # summary.degraded is also the live degraded flag
        self._request_errors: Counter = Counter()  # number of failed requests per error code
        self._hold_start = 0.0  # when the oldest waiting trace was queued (set when a trace is added to an empty queue)
        self._flushing = False  # set by finish(): send the waiting traces at once

    @property
    def parallel(self) -> int:
        """Return the current number of requests sent at the same time.

        Returns:
            1 if degraded, otherwise the number set at creation.
        """
        return 1 if self._summary.degraded else self._stats.parallel

    @property
    def degraded(self) -> bool:
        """Return True if a request failed or was slow.

        Returns:
            True if degraded.
        """
        return self._summary.degraded

    @property
    def finished(self) -> bool:
        """Return True once :meth:`finish` has been called.

        Returns:
            True if finished.
        """
        return self._state == FINISHED

    @property
    def summary(self) -> TraceSummary:
        """Return a copy of the current counts.

        Returns:
            The counts.
        """
        with self._cond:
            return replace(self._summary)

    def snapshot(self) -> TraceSnapshot:
        """Return a copy of the counts, request statistics, trace records and request errors.

        Returns:
            The snapshot.
        """
        with self._cond:
            return TraceSnapshot(summary=replace(self._summary),
                                 stats=replace(self._stats),
                                 records=tuple(replace(record) for record in self._records),
                                 request_errors=dict(self._request_errors))

    def enqueue(self, traces: Iterable[dict[str, Any]]) -> int:
        """Prepare traces and queue them for sending.

        Traces that cannot be verified are dropped and counted as invalid. Nothing is queued after
        :meth:`finish`.

        Args:
            traces: Trace reports, or plain dictionaries with trace report fields.

        Returns:
            Number of queued traces.
        """
        if traces_disabled():
            logger.debug('rucio traces do not need to be sent')
            return 0

        prepared, rejected = split_traces(traces)
        with self._cond:
            self._summary.invalid += len(rejected)
            if self._state != RUNNING:
                logger.warning(f'{len(prepared)} trace(s) arrived after the trace sender was stopped (final update) and will not be sent')
                return 0
            waiting = self._add(prepared, rejected)
        self._log_queued(len(prepared), waiting)

        return len(prepared)

    def try_enqueue(self, traces: Iterable[dict[str, Any]]) -> bool:
        """Prepare traces and queue them for sending, if the sender still takes traces.

        Unlike :meth:`enqueue`, nothing is dropped when the sender has been stopped: the caller is
        told to send the traces itself.

        Args:
            traces: Trace reports, or plain dictionaries with trace report fields.

        Returns:
            True if the traces were taken care of (queued, or dropped as invalid), False if the
            sender has been stopped.
        """
        if traces_disabled():
            logger.debug('rucio traces do not need to be sent')
            return True

        prepared, rejected = split_traces(traces)
        with self._cond:
            if self._state != RUNNING:
                return False
            self._summary.invalid += len(rejected)
            waiting = self._add(prepared, rejected)
        self._log_queued(len(prepared), waiting)

        return True

    def _add(self, prepared: list[dict[str, Any]], rejected: list[dict[str, Any]]) -> int:
        """Queue prepared traces, record all traces for the report, and wake up the worker.

        Must be called with the lock held.

        Args:
            prepared: Prepared traces.
            rejected: Traces that could not be verified (recorded as invalid).

        Returns:
            Number of traces waiting to be sent.
        """
        self._records.extend(TraceRecord.from_trace(trace, INVALID) for trace in rejected)
        if prepared and not self._pending:
            self._hold_start = time.time()
        for trace in prepared:
            record = TraceRecord.from_trace(trace)
            self._records.append(record)
            self._pending.append((trace, record))
        if prepared:
            self._start_worker()
            self._cond.notify_all()

        return len(self._pending)

    def _log_queued(self, queued: int, waiting: int) -> None:
        """Log the number of queued traces.

        Args:
            queued: Number of traces just queued.
            waiting: Number of traces waiting to be sent.
        """
        if queued:
            logger.info(f'queued {queued} trace(s) for sending in the background ({waiting} waiting, job {self.job_id})')

    def finish(self, graceful_stop: threading.Event | None = None) -> TraceSummary:
        """Stop sending, after the remaining traces have been sent (bounded, see :meth:`_wait_for_remaining`).

        Calling it again returns the same counts.

        Args:
            graceful_stop: Graceful stop event (no wait if set).

        Returns:
            The final counts.
        """
        with self._cond:
            if self._state == FINISHED:
                return replace(self._summary)
            t0 = time.time()
            self._flushing = True  # stop holding traces
            self._cond.notify_all()
            self._wait_for_remaining(graceful_stop)  # returns at once if graceful stop has been set
            self._stats.final_wait = time.time() - t0
            # traces of a request still in flight are counted as abandoned, since the outcome is unknown
            unsent = list(self._pending) + self._in_flight
            self._summary.abandoned += len(unsent)
            for _, record in unsent:
                record.result = ABANDONED
            self._pending.clear()
            self._state = FINISHED
            self._summary.elapsed = time.time() - self._stats.started
            self._stats.degraded = self._summary.degraded
            self._cond.notify_all()
            summary = replace(self._summary)
            records = list(self._records)
            stats = replace(self._stats)

        self._log_report(summary, records, stats)

        return summary

    def _log_report(self, summary: TraceSummary, records: list[TraceRecord], stats: TraceStats) -> None:
        """Log the trace report of the job (see :mod:`pilot.util.tracesummary`).

        The one-line summary in the report is logged as a warning if traces were lost.

        Args:
            summary: Final counts.
            records: One record per trace.
            stats: Request statistics.
        """
        counts = (f'rucio traces of job {self.job_id}: sent={summary.sent}, failed={summary.failed}, '
                  f'abandoned={summary.abandoned}, invalid={summary.invalid}, time={summary.elapsed:.1f} s')
        for line in format_trace_report(self.job_id, counts, records, stats, disabled=traces_disabled()):
            if summary.lost and line.endswith(counts):
                logger.warning(line)
            else:
                logger.info(line)

    def stop(self) -> None:
        """Stop the worker without sending the remaining traces or counting them."""
        with self._cond:
            self._pending.clear()
            if self._state == RUNNING:
                self._state = STOPPED
            self._cond.notify_all()

    def _wait_limit(self) -> float:
        """Return the maximum time to wait for the remaining traces.

        Returns:
            ``final_wait`` seconds while healthy, at most ``timeout`` seconds once degraded.
        """
        if self._summary.degraded:
            return min(self.settings.timeout, self.settings.final_wait)

        return self.settings.final_wait

    def _wait_for_remaining(self, graceful_stop: threading.Event | None) -> None:
        """Wait until all traces are sent, the time is up, or graceful stop is set.

        The time limit is ``final_wait`` seconds while the sender is healthy, and ``timeout`` seconds
        (counted from the start of the wait) once it is degraded. Must be called with the lock held.

        Args:
            graceful_stop: Graceful stop event.
        """
        t0 = time.time()
        if self._summary.degraded and (self._pending or self._in_flight):
            logger.info(f'trace sending is degraded - will wait at most {self._wait_limit()} s for the remaining traces')
        while self._pending or self._in_flight:
            limit = self._wait_limit()
            remaining = t0 + limit - time.time()
            if remaining <= 0:
                logger.warning(f'traces were not sent within {limit} s')
                return
            if _is_set(graceful_stop):
                logger.warning('graceful stop has been set - will not wait for the remaining traces')
                return
            self._cond.wait(min(WAIT_INTERVAL, remaining))

    def _start_worker(self) -> None:
        """Start the daemon worker thread if it is not running. Must be called with the lock held."""
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name=f'tracesender-{self.job_id}', daemon=True)
            self._thread.start()

    def _run(self) -> None:
        """Send queued traces until the sender is stopped (worker thread)."""
        while True:
            with self._cond:
                reason = self._send_reason()
                while reason is None and self._state == RUNNING:
                    self._cond.wait(self._hold_time())
                    reason = self._send_reason()
                if self._state != RUNNING:
                    return
                batches = self._next_batches()
                self._in_flight = [item for batch in batches for item in batch]
                if self.settings.batch_wait > 0:
                    logger.info(f'sending {len(self._in_flight)} trace(s) in {len(batches)} request(s) ({reason})')

            results = self._send_batches(batches)

            with self._cond:
                self._in_flight = []
                if self._state == RUNNING:  # otherwise finish() has already counted these traces
                    self._record(results, batches)
                self._cond.notify_all()

    def _send_reason(self) -> str | None:
        """Return why the waiting traces should be sent now, or None to keep waiting. Must be called with the lock held.

        Returns:
            The reason (for the log), or None if no traces are waiting or they are still held.
        """
        if not self._pending:
            return None
        if self._flushing:
            return 'end of job'
        if len(self._pending) >= self.settings.batch_size * self.parallel:
            return 'requests full'
        held = time.time() - self._hold_start
        if held >= self.settings.batch_wait:
            return f'held for {held:.0f} s'

        return None

    def _hold_time(self) -> float | None:
        """Return how long the worker may sleep before the waiting traces are due. Must be called with the lock held.

        Returns:
            Seconds until the oldest waiting trace has been held for ``batch_wait`` seconds, or None (no time limit)
            if no traces are waiting.
        """
        if not self._pending:
            return None

        return max(self._hold_start + self.settings.batch_wait - time.time(), 0.0)

    def _next_batches(self) -> list[list[tuple[dict[str, Any], TraceRecord]]]:
        """Take the traces for the next round of requests off the queue. Must be called with the lock held.

        Returns:
            One list of (trace, record) pairs per request.
        """
        batches = []
        while self._pending and len(batches) < self.parallel:
            size = min(self.settings.batch_size, len(self._pending))
            batches.append([self._pending.popleft() for _ in range(size)])

        return batches

    def _send_batches(self, batches: list[list[tuple[dict[str, Any], TraceRecord]]]) -> list[BatchResult]:
        """Send one request per batch, at the same time if there is more than one.

        Args:
            batches: One list of (trace, record) pairs per request.

        Returns:
            One result per request.
        """
        if len(batches) == 1:
            return [self._post(batches[0])]

        results: list[BatchResult | None] = [None] * len(batches)
        threads = []
        for index, batch in enumerate(batches):
            thread = threading.Thread(target=self._post_into, args=(batch, results, index),
                                      name=f'tracesender-{self.job_id}-{index}', daemon=True)
            thread.start()
            threads.append(thread)
        for thread in threads:
            thread.join(self.settings.timeout + JOIN_MARGIN)

        return [result if result is not None else BatchResult(len(batch), False, float(self.settings.timeout), 'no reply')
                for result, batch in zip(results, batches)]

    def _post(self, batch: list[tuple[dict[str, Any], TraceRecord]]) -> BatchResult:
        """Send one request.

        Args:
            batch: (prepared trace, record) pairs.

        Returns:
            The outcome.
        """
        t0 = time.time()
        accepted, reply = post_traces([trace for trace, _ in batch], self.settings.timeout)

        return BatchResult(len(batch), accepted, time.time() - t0, reply)

    def _post_into(self, batch: list[tuple[dict[str, Any], TraceRecord]], results: list[BatchResult | None], index: int) -> None:
        """Send one request and store the outcome (target of a request thread).

        Args:
            batch: (prepared trace, record) pairs.
            results: Result list.
            index: Position of this request in the result list.
        """
        results[index] = self._post(batch)

    def _record(self, results: list[BatchResult], batches: list[list[tuple[dict[str, Any], TraceRecord]]]) -> None:
        """Count the outcome of a round of requests and degrade on failure or slowness. Must be called with the lock held.

        Args:
            results: Outcomes, one per request.
            batches: The (trace, record) pairs of each request.
        """
        for result, batch in zip(results, batches):
            self._stats.requests += 1
            self._stats.sending_time += result.elapsed
            if result.accepted:
                self._summary.sent += result.size
                outcome = SENT
            else:
                self._summary.failed += result.size
                logger.warning(f'failed to send {result.size} trace(s): {result.reply}')
                code = get_trace_error_code(result.reply)
                self._request_errors[code] += 1
                store_trace_error(self.workdir, code)
                outcome = f'{FAILED} (HTTP {code})' if code >= 100 else FAILED
                self._degrade('a trace request failed')
            for _, record in batch:
                record.result = outcome
            if result.elapsed > self.settings.slow_threshold:
                self._degrade(f'a trace request took {result.elapsed:.1f} s')

    def _degrade(self, reason: str) -> None:
        """Send one request at a time from now on. Must be called with the lock held.

        Args:
            reason: Reason, for the log.
        """
        if not self._summary.degraded:
            self._summary.degraded = True
            logger.warning(f'{reason} - will send one trace request at a time, and wait at most {self._wait_limit()} s for the '
                           f'remaining traces at the end of the job')


_registry: dict[str, TraceSender] = {}
_registry_lock = threading.Lock()


def _get_catchall(job: Any) -> str:
    """Return the catchall field of the job's queue.

    Args:
        job: Job object.

    Returns:
        The catchall field, or an empty string if not available.
    """
    try:
        return job.infosys.queuedata.catchall or ''
    except AttributeError:
        return ''


def get_or_create_sender(job: Any) -> TraceSender:
    """Return the trace sender of a job, creating it if needed.

    Args:
        job: Job object.

    Returns:
        The sender.
    """
    job_id = str(job.jobid)
    with _registry_lock:
        sender = _registry.get(job_id)
        if sender is None:
            settings = TraceSettings.from_config()
            parallel = get_parallel_traces(_get_catchall(job), settings.max_parallel)
            sender = TraceSender(job_id, job.workdir, parallel=parallel, settings=settings)
            _registry[job_id] = sender
            hold = f'traces held for at most {settings.batch_wait} s' if settings.batch_wait > 0 else 'traces sent at once'
            logger.info(f'created background trace sender for job {job_id} ({parallel} trace request(s) at a time, {hold})')

    return sender


def get_sender(job_id: Any) -> TraceSender | None:
    """Return the trace sender of a job, if there is one.

    Args:
        job_id: PanDA job id.

    Returns:
        The sender, or None.
    """
    with _registry_lock:
        return _registry.get(str(job_id))


def finish_traces(job: Any, args: Any = None) -> TraceSummary | None:
    """Finish sending the traces of a job before its final server update.

    Does nothing if the job has no trace sender.

    Args:
        job: Job object.
        args: Pilot arguments (for graceful stop).

    Returns:
        The final counts, or None if the job has no trace sender.
    """
    sender = get_sender(job.jobid)
    if sender is None:
        return None

    return sender.finish(getattr(args, 'graceful_stop', None))


def get_trace_snapshot(job_id: Any) -> TraceSnapshot | None:
    """Return the final state of the trace sender of a job, for the pilot telemetry.

    Must be called after :func:`finish_traces` and before :func:`remove_sender`.

    Args:
        job_id: PanDA job id.

    Returns:
        The snapshot, or None if the job has no finished trace sender.
    """
    sender = get_sender(job_id)
    if sender is None or not sender.finished:
        return None

    return sender.snapshot()


def get_lost_traces(job_id: Any) -> int:
    """Return the number of lost traces of a job whose trace sender has finished.

    Args:
        job_id: PanDA job id.

    Returns:
        The number of lost traces (0 if there is no finished sender).
    """
    sender = get_sender(job_id)
    if sender is None or not sender.finished:
        return 0

    return sender.summary.lost


def remove_sender(job_id: Any) -> None:
    """Remove the trace sender of a job from the registry (after the final server update).

    Args:
        job_id: PanDA job id.
    """
    with _registry_lock:
        sender = _registry.pop(str(job_id), None)
    if sender is not None:
        sender.stop()


def reset_registry() -> None:
    """Stop and remove all trace senders (for tests)."""
    with _registry_lock:
        senders = list(_registry.values())
        _registry.clear()
    for sender in senders:
        sender.stop()


def _enqueue_for_job(traces: list[dict[str, Any]], job_id: Any) -> bool:
    """Queue traces in the background sender of a job (see :func:`~pilot.util.tracereport.defer_traces`).

    Args:
        traces: Trace reports, or plain dictionaries with trace report fields.
        job_id: PanDA job id.

    Returns:
        True if the sender took the traces, False if the job has no sender or it has been stopped.
    """
    sender = get_sender(job_id)

    return sender is not None and sender.try_enqueue(traces)


def collect_spooled_traces(job: Any) -> int:
    """Send the traces written to the spool file by a middleware container script.

    The traces go to the job's background sender, or are sent at once if there is none. This
    function never raises, so that it can be called however the container run ended.

    Args:
        job: Job object.

    Returns:
        Number of traces read from the spool file.
    """
    try:
        traces = read_trace_spool(job.workdir)
        if traces:
            send_traces(traces, job_id=job.jobid, workdir=job.workdir)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f'failed to send the rucio traces of the middleware container: {exc}')
        return 0

    return len(traces)


set_background_sender(_enqueue_for_job)
