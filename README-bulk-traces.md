# Sending Rucio Traces in Bulk (`trace_batch_wait`)

The pilot sends a Rucio trace for every file it reads or writes. A trace server request can carry a list of traces, so the pilot tries to put several traces in one request. This page describes the two ways the pilot can do that, and how to choose between them.

---

## How traces are sent

Each job has a background **trace sender** (`pilot/util/tracesender.py`), created by stage-in or stage-out. Copytools hand their traces to it with `TraceReport.send()`, and a worker thread sends them, so that sending never delays the payload.

- Every request is a JSON **list** of traces, at most `trace_batch_size` (100) per request. A single trace is a one-element list.
- Up to `PARALLEL_TRACES=<n>` requests are sent at the same time (queue catchall, default 1, capped by `trace_max_parallel`).
- Before the final server update, `finish()` sends the remaining traces (at most `trace_final_wait` seconds, or `trace_timeout` seconds once sending is degraded) and logs the job's trace report.

What differs between the two alternatives is **when** the worker sends the traces that are waiting.

---

## Alternative 1: hold traces (default, `trace_batch_wait: 300`)

A trace is held for at most `trace_batch_wait` seconds so that more traces can join it. The waiting traces are sent when the first of these happens:

| Trigger | Log reason |
|---|---|
| The oldest waiting trace has waited `trace_batch_wait` seconds | `held for N s` |
| Enough traces are waiting to fill a round of requests (`trace_batch_size` × the number of parallel requests) | `requests full` |
| The job's traces are finished, before the final update | `end of job` |

The hold time counts from the oldest waiting trace, so no trace waits longer than `trace_batch_wait` seconds (plus the time to send the requests in front of it).

**Gains:**
- Bulk requests for every job type, including copy-to-scratch jobs whose copytool downloads one file at a time.
- Fewer requests to the trace server: typically one or two for stage-in and one for stage-out.

**Costs:**
- Traces reach Rucio up to `trace_batch_wait` seconds later. The trace timestamps are not affected.
- Traces held when the job ends abnormally can be lost:
  - If graceful stop is set before the final update (for example, the pilot was told to stop), `finish()` does not wait, and held traces are counted as **abandoned**. They appear in the trace report and in `rucioTracesLost`.
  - If the pilot is killed outright, held traces are lost without being counted.
  - In both cases, at most the traces of the last `trace_batch_wait` seconds are at risk.
- The final wait before the final update now includes sending the held traces, typically under a second.

## Alternative 2: send at once (`trace_batch_wait: 0`)

The worker sends every trace as soon as it is queued. This is the behaviour of pilot versions before `trace_batch_wait` was added.

A request then carries several traces only when they were queued together, or piled up while a request was in flight:

- remote I/O (direct access): the traces of all remote files are queued in one call after the payload;
- middleware containers: the stage-in or stage-out traces are read from the spool file and queued in one call;
- a failed download attempt: the attempt traces and the file's own trace are queued in one call;
- a backlog behind a slow request.

Copy-to-scratch jobs whose copytool downloads one file at a time get **one request per trace**: each download takes seconds, while a request takes well under a second, so only one trace is ever waiting.

**Gains:** traces reach Rucio as soon as possible, and nothing is held when the job ends abnormally.

**Costs:** one request per file for most production jobs.

---

## Examples from production jobs

All three jobs ran on MWT2_TEST. The first two ran with a pilot that sends at once (alternative 2), the third with a pilot that holds traces for 300 s (alternative 1).

| Job | Traces | Requests, sent at once | Requests, held for 300 s |
|---|---|---|---|
| 7381062698 (pile-up, 11 input files) | 11 stage-in + 2 stage-out | 13 (measured) | 2 (worked out) |
| 7386436637 (pile-up reco, 10 input files) | 10 stage-in + 2 stage-out | 12 (measured) | 2 (worked out) |
| 7389762158 (pile-up reco, 10 input files) | 10 stage-in + 3 stage-out | — | 2 (measured) |

The worked-out numbers come from the times the traces were queued. In job 7389762158, stage-in took 84 s, so its 10 stage-in traces were sent in one request 300 s after the first one, and the 3 stage-out traces in one request at the end of the job.

A hold time shorter than stage-in splits the stage-in traces. With the earlier test value of 60 s, job 7388118915 sent its 2 stage-in traces in 2 requests: downloading and checksumming its second, 22.6 GB, input file took 81 s.

---

## Configuration

All settings are in the `[Rucio]` section of `pilot/util/default.cfg`:

| Setting | Default | Meaning |
|---|---|---|
| `trace_batch_wait` | 300 | Maximum time (s) a trace is held for more traces; 0 sends at once (alternative 2). A negative or non-numeric value gives the default |
| `trace_batch_size` | 100 | Maximum number of traces per request |
| `trace_max_parallel` | 2 | Cap for `PARALLEL_TRACES` |
| `trace_timeout` | 30 | Request time-out (s) |
| `trace_slow_threshold` | 20 | A slower request makes the sender send one request at a time |
| `trace_final_wait` | 600 | Maximum wait (s) for the remaining traces before the final update |

To change a setting:
- edit `default.cfg` in the pilot tarball; or
- point `HARVESTER_PILOT_CONFIG` at a complete copy of the config file. The copy replaces `default.cfg`, it is not merged with it.

There is no per-queue switch for `trace_batch_wait`. Per queue, only the number of parallel requests (`PARALLEL_TRACES` in the catchall) can be set.

`PILOT_USE_RUCIO_TRACES=False` turns all traces off. The HPC workflow has no background sender and sends every trace at once, whatever `trace_batch_wait` is.

---

## Reading the log

When the sender is created, the log says which alternative is used:

```
created background trace sender for job 7389762158 (1 trace request(s) at a time, traces held for at most 300 s)
created background trace sender for job 7389762158 (1 trace request(s) at a time, traces sent at once)
```

With holding, each round of requests is logged with its reason:

```
queued 1 trace(s) for sending in the background (1 waiting, job 7389762158)
queued 1 trace(s) for sending in the background (2 waiting, job 7389762158)
…
sending 10 trace(s) in 1 request(s) (held for 300 s)
…
sending 3 trace(s) in 1 request(s) (end of job)
```

The trace report at the end of the job shows the result. Bulk sending happened when there are fewer requests than sent traces:

```
*** rucio traces of job 7389762158: sent=13, failed=0, abandoned=0, invalid=0, time=… s
*** requests: 2 (up to 1 at a time, not degraded), time sending: … s, final wait: … s
```

---

## Time stamps of a trace

Every trace carries the time stamps of its own file, or of its own failed transfer attempt (`TraceReport.start_file()`, called by the copytools at the start of every file):

- `timeStart`, `catStart`, `relativeStart` and `transferStart`: the start of the file's transfer. For a failed attempt inside the Rucio client or a failed stage-out attempt, the start of that attempt, i.e. the time the previous attempt failed.
- `validateStart`: the start of the pilot's checksum verification of the file. Empty (null) where the pilot does not verify the checksum itself (gfal, lsm stage-out, the Rucio copytool without an upload summary) and for failed attempts.
- `timeEnd`: the end of the transfer, including the checksum verification.

Older pilot versions set `timeStart`, `relativeStart`, `transferStart` and `validateStart` once per stage-in or stage-out. In job 7389762158, the last of 10 input files therefore had `timeEnd - transferStart` = 91.0 s, while its own download and checksum verification took 12.9 s.
