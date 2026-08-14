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

"""Process diagnostics for looping payloads.

Background: when the looping job algorithm decides that a payload has stopped
touching its files, the pilot produced a single core dump before killing the
job. The dump target was ``get_subprocesses(job.pid)[-1]``, i.e. the last entry
of a depth-first walk of the payload's descendants in ascending PID order. That
is not "the youngest child" and has no relation to which process is actually
stuck: for an ATLAS job the descendant tree also holds the transform's own
prmon instance, asetup/apptainer/bash wrappers, and xrootd helpers, any of
which can end up last.

The 10 s timeout used for the dump made the selection actively harmful rather
than merely arbitrary. ``generate-core-file`` writes the whole address space,
so a multi-GB payload cannot finish inside the window while a few-MB helper
always can - the one candidate guaranteed to produce a usable core file was
the uninteresting one.

This module replaces that with two things.

**A ranked candidate list** (:func:`select_dump_candidates`). Known-uninteresting
processes are removed by command line (prmon, nvidia-smi, shell and container
wrappers, ALRB setup scripts, xrootd clients, ...), and what survives is ranked
by how likely it is to be the looping payload: processes whose names the
experiment plugin declares interesting come first, then the process burning the
most CPU time - a *looping* payload normally spins, which is exactly what
separates a loop from a hang - then the largest resident set, then tree depth.

**A series of cheap snapshots** (:func:`take_looping_snapshot`). A core file is
one instant, whereas a loop is characterised by what changes and what does not
between samples, so once the job is a configurable fraction of the way to the
looping limit the pilot starts recording, at every looping verification, the
accumulated CPU time, process state, ``wchan``, current syscall, resident set
and a truncated backtrace of every candidate. That is kilobytes per snapshot
against gigabytes for a core file, and the deltas between consecutive snapshots
are summarised at kill time (:func:`summarise_snapshots`).

At kill time at most one core file is still produced
(:func:`create_core_dump`), but with the experiment's own gdb (the system one
is frequently too old, which is why the server-driven debug path already
prepends a setup), with an explicit working directory, with a timeout that a
real payload can meet, and with the backtraces requested *before* the expensive
core write so that a timeout still leaves something behind.

Because a core file is useless without knowing which binary produced it, the
executable identity is recorded twice: as a greppable block in the pilot log
marked with :data:`CORE_INFO_MARKER`, and as a companion file next to the core
file in the job work directory, so that whoever picks up the log tarball later
can run gdb against the right binary and the right software release.
"""

from __future__ import annotations

import logging
import os
import re
import signal
import time
from shutil import (
    disk_usage,
    which
)
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import write_file
from pilot.util.math import human2bytes
from pilot.util.parameters import convert_to_int
from pilot.util.psutils import get_child_processes

logger = logging.getLogger(__name__)
errors = ErrorCodes()

# Prefix on every log line from this module so the diagnostics can be grepped
# out of a pilot log.
LOG_PREFIX = "looping-dump"

# Marker on the log block that records which binary a core file belongs to.
# Deliberately verbose and unique: it is what someone analysing the log tarball
# weeks later will grep for in order to find out how to open the core file.
CORE_INFO_MARKER = "CORE FILE ANALYSIS INFO"

# Name of the companion file written next to the core file, holding the same
# information as the CORE_INFO_MARKER log block. The core file and this file
# travel together in the log tarball.
CORE_INFO_SUFFIX = ".analysis.txt"

# Name of the file in the job work directory holding the snapshot series.
SNAPSHOT_FILENAME = "looping_snapshots.log"

# Command line fragments identifying processes that are never the looping
# payload. Matched case-insensitively against the basename of argv[0] and, for
# the entries in DENYLISTED_ARGS, against the full command line.
DENYLISTED_NAMES = (
    "prmon",
    "memorymonitor",
    "nvidia-smi",
    "ps",
    "du",
    "df",
    "find",
    "tail",
    "ls",
    "sh",
    "bash",
    "dash",
    "csh",
    "tcsh",
    "zsh",
    "apptainer",
    "singularity",
    "runc",
    "crun",
    "conmon",
    "asetup",
    "lsetup",
    "atlaslocalsetup.sh",
    "setupatlas",
    "xrootd",
    "xrdcp",
    "xrdcopy",
    "curl",
    "wget",
    "gfal-copy",
    "rucio",
    "gdb",
    "eu-stack",
    "pstack",
    "sleep",
    "cat",
    "grep",
    "awk",
    "sed",
    "tar",
    "gzip",
    "zstd",
)

# Full-command-line fragments that disqualify a process regardless of argv[0],
# for helpers invoked through an interpreter (e.g. 'python .../prmon_wrapper').
# Kept deliberately short: a fragment that can occur inside a legitimate payload
# command line would silently drop the very process we are looking for.
DENYLISTED_ARGS = (
    "prmon",
    "nvidia-smi",
)

# Maximum number of candidates carried through to snapshotting. Bounds both the
# snapshot size and the number of stack tool invocations per snapshot.
MAX_CANDIDATES = 5

# Maximum number of backtrace lines kept per process per snapshot.
MAX_BACKTRACE_LINES = 40

# Timeout for a single stack trace invocation, in seconds.
STACK_TOOL_TIMEOUT = 60

# Stack tools in order of preference. eu-stack is the cheapest and does not need
# a full gdb; pstack is the traditional fallback; gdb is used last because it is
# the slowest to start.
STACK_TOOLS = ("eu-stack", "pstack")

# Fraction of the looping limit after which the snapshot series starts. With the
# default 7200 s limit and a 900 s verification time this yields roughly four
# samples before the payload is killed.
DEFAULT_SNAPSHOT_FRACTION = 0.5

# Default timeout for the core dump, in seconds. 'generate-core-file' writes the
# whole address space, so this has to be generous; a multi-GB athena needs far
# more than the 10 s previously allowed.
DEFAULT_CORE_DUMP_TIMEOUT = 300

# Default upper bound on the resident set of a process for which a core file is
# still attempted. Above this the backtraces are kept and the core file skipped,
# since the log tarball has no size guard of its own.
DEFAULT_CORE_DUMP_MAX_SIZE = "4 GB"

# Multiplier applied to the resident set when checking free disk space, to cover
# the difference between RSS and the size of the written core file.
CORE_SIZE_SAFETY_FACTOR = 1.5

# Ticks per second used to convert utime/stime from /proc/<pid>/stat. Kept as a
# constant rather than read via os.sysconf() on every snapshot; it is 100 on
# every platform the pilot runs on.
CLOCK_TICKS = 100.0

# Snapshot bookkeeping, keyed on job id so that every job of a multijob pilot
# gets its own series.
_snapshot_state: dict[str, Any] = {"jobid": None, "snapshots": []}


def reset_looping_dump_state() -> None:
    """Reset the snapshot bookkeeping.

    Exposed for tests and for the multijob case; the state is otherwise reset
    automatically when a new job id is seen.
    """
    _snapshot_state["jobid"] = None
    _snapshot_state["snapshots"] = []


def read_proc_file(pid: int, name: str) -> str:
    """Return the contents of a file under ``/proc/<pid>/``.

    Args:
        pid: Process id.
        name: File name relative to ``/proc/<pid>/``, e.g. ``"stat"``.

    Returns:
        File contents, or an empty string if the file could not be read (the
        process may have exited, or the kernel may not provide the file).
    """
    try:
        with open(f"/proc/{pid}/{name}", "r", encoding="utf-8", errors="replace") as _file:
            return _file.read().strip()
    except OSError:
        return ""


def read_proc_link(pid: int, name: str) -> str:
    """Return the target of a symlink under ``/proc/<pid>/``.

    Args:
        pid: Process id.
        name: Link name relative to ``/proc/<pid>/``, e.g. ``"exe"``.

    Returns:
        Link target, or an empty string if it could not be resolved.
    """
    try:
        return os.readlink(f"/proc/{pid}/{name}")
    except OSError:
        return ""


def get_cmdline(pid: int) -> str:
    """Return the full command line of a process.

    Args:
        pid: Process id.

    Returns:
        Space separated command line, or an empty string if unavailable.
    """
    raw = read_proc_file(pid, "cmdline")

    return raw.replace("\x00", " ").strip() if raw else ""


def get_cpu_time(pid: int) -> float:
    """Return the accumulated CPU time (user + system) of a process.

    Args:
        pid: Process id.

    Returns:
        CPU time in seconds, or 0.0 if it could not be determined.
    """
    stat = read_proc_file(pid, "stat")
    if not stat:
        return 0.0

    # the comm field is parenthesised and can itself contain spaces, so split
    # after the closing parenthesis: fields 14 and 15 (1-based) are utime/stime
    try:
        fields = stat[stat.rindex(")") + 1:].split()
        return (int(fields[11]) + int(fields[12])) / CLOCK_TICKS
    except (ValueError, IndexError):
        return 0.0


def get_process_state(pid: int) -> str:
    """Return the single-letter process state from ``/proc/<pid>/stat``.

    Args:
        pid: Process id.

    Returns:
        Process state (``R``, ``S``, ``D``, ``Z``, ``T``, ...), or an empty
        string if it could not be determined.
    """
    stat = read_proc_file(pid, "stat")
    if not stat:
        return ""

    try:
        return stat[stat.rindex(")") + 1:].split()[0]
    except (ValueError, IndexError):
        return ""


def get_status_value(pid: int, key: str) -> str:
    """Return a single field from ``/proc/<pid>/status``.

    Args:
        pid: Process id.
        key: Field name without the colon, e.g. ``"VmRSS"``.

    Returns:
        Field value with surrounding whitespace stripped, or an empty string.
    """
    status = read_proc_file(pid, "status")
    if not status:
        return ""

    for line in status.split("\n"):
        if line.startswith(f"{key}:"):
            return line.split(":", 1)[1].strip()

    return ""


def get_rss(pid: int) -> int:
    """Return the resident set size of a process in bytes.

    Args:
        pid: Process id.

    Returns:
        Resident set size in bytes, or 0 if it could not be determined.
    """
    value = get_status_value(pid, "VmRSS")
    if not value:
        return 0

    try:
        # value looks like '4194304 kB'
        return int(value.split()[0]) * 1024
    except (ValueError, IndexError):
        return 0


def get_current_syscall(pid: int) -> str:
    """Return the syscall a process is currently executing.

    ``/proc/<pid>/syscall`` is extremely cheap to read and immediately
    separates a process spinning in user space (``running``) from one blocked
    in a syscall, which is the single most useful discriminator between a
    genuine loop and a hang on I/O.

    Args:
        pid: Process id.

    Returns:
        Raw ``/proc/<pid>/syscall`` contents, or an empty string if the kernel
        does not expose it (it requires ``CONFIG_HAVE_ARCH_TRACEHOOK``).
    """
    return read_proc_file(pid, "syscall")


def is_denylisted(cmdline: str) -> bool:
    """Return True if the given command line belongs to a non-payload helper.

    Args:
        cmdline: Full command line of the process.

    Returns:
        True if the process should never be considered as a dump target.
    """
    if not cmdline:
        # a process with an unreadable command line is a kernel thread or has
        # already exited - either way it is not a dump target
        return True

    lowered = cmdline.lower()
    for fragment in DENYLISTED_ARGS:
        if fragment in lowered:
            return True

    # note: interpreters (python, python3, ...) are deliberately not denylisted,
    # since the ATLAS payload itself runs as 'python .../Sim_tf.py ...'
    argv0 = os.path.basename(lowered.split()[0])

    return argv0 in DENYLISTED_NAMES


def get_payload_process_names() -> list:
    """Return the process names the experiment plugin considers interesting.

    Returns:
        List of lowercase name fragments, empty if the plugin does not define
        any (in which case selection falls back to the generic ranking).
    """
    pilot_user = os.environ.get("PILOT_USER", "generic").lower()
    try:
        definitions = __import__(
            f"pilot.user.{pilot_user}.loopingjob_definitions",
            globals(), locals(), [pilot_user], 0
        )
        names = definitions.get_payload_process_names()
    except (ImportError, AttributeError) as exc:
        logger.debug(f"{LOG_PREFIX}: no payload process names from the {pilot_user} plugin: {exc}")
        return []

    return [name.lower() for name in names or []]


def get_descendants(pid: int) -> list:
    """Return the descendants of a process as ``(pid, cmdline)`` tuples.

    Args:
        pid: Root process id.

    Returns:
        List of ``(pid, cmdline)`` tuples; empty on failure.
    """
    try:
        descendants = get_child_processes(pid)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: failed to walk the process tree of pid={pid}: {exc}")
        return []

    normalised = []
    for entry in descendants or []:
        try:
            _pid, cmdline = entry
        except (TypeError, ValueError):
            continue
        if isinstance(cmdline, list):
            cmdline = " ".join([str(item) for item in cmdline])
        normalised.append((_pid, (cmdline or "").strip()))

    return normalised


def rank_candidate(cmdline: str, pid: int, payload_names: list) -> tuple:
    """Return the sort key ranking a candidate process as a dump target.

    Higher is better. A name declared interesting by the experiment plugin wins
    outright; among the rest the process burning the most CPU time comes first,
    since a looping payload normally spins, followed by the largest resident
    set.

    Args:
        cmdline: Full command line of the process.
        pid: Process id.
        payload_names: Name fragments declared interesting by the plugin.

    Returns:
        Tuple of ``(name_match, cpu_time, rss)`` used as a sort key.
    """
    lowered = cmdline.lower()
    name_match = 1 if any(name in lowered for name in payload_names) else 0

    return name_match, get_cpu_time(pid), get_rss(pid)


def select_dump_candidates(job: Any) -> list:
    """Return the payload processes ranked by how likely they are to be looping.

    The full descendant tree of ``job.pid`` is logged together with the reason
    each entry was kept or dropped, so that a wrong choice can be diagnosed
    from the log afterwards rather than guessed at.

    Args:
        job: Job object; ``job.pid`` must be set.

    Returns:
        List of ``(pid, cmdline)`` tuples, best candidate first, truncated to
        :data:`MAX_CANDIDATES`. Falls back to ``[(job.pid, <cmdline>)]`` when
        every descendant was filtered out.
    """
    if not job.pid:
        logger.warning(f"{LOG_PREFIX}: cannot select a dump candidate - job.pid is not set")
        return []

    payload_names = get_payload_process_names()
    descendants = get_descendants(job.pid)

    lines = [f"{LOG_PREFIX}: descendants of the payload process (pid={job.pid}):"]
    kept = []
    for pid, cmdline in descendants:
        if is_denylisted(cmdline):
            lines.append(f"  pid={pid} DROPPED (non-payload helper): {cmdline}")
            continue
        kept.append((pid, cmdline))
        lines.append(f"  pid={pid} kept: {cmdline}")
    if not descendants:
        lines.append("  (none)")
    logger.info("\n".join(lines))

    if not kept:
        cmdline = get_cmdline(job.pid)
        logger.info(
            f"{LOG_PREFIX}: every descendant was filtered out - falling back to the payload "
            f"process itself (pid={job.pid})"
        )
        return [(job.pid, cmdline)]

    kept.sort(key=lambda entry: rank_candidate(entry[1], entry[0], payload_names), reverse=True)
    candidates = kept[:MAX_CANDIDATES]

    lines = [f"{LOG_PREFIX}: candidate ranking (best first):"]
    for pid, cmdline in candidates:
        name_match, cpu_time, rss = rank_candidate(cmdline, pid, payload_names)
        lines.append(
            f"  pid={pid} name_match={name_match} cpu_time={cpu_time:.1f}s "
            f"rss={rss // (1024 * 1024)}MB: {cmdline}"
        )
    logger.info("\n".join(lines))

    return candidates


def get_stack_tool() -> str:
    """Return the first available stack trace tool.

    Returns:
        Name of the tool (``"eu-stack"`` or ``"pstack"``), or an empty string
        if neither is available (gdb is then used instead).
    """
    for tool in STACK_TOOLS:
        if which(tool):
            return tool

    return ""


def get_stack_trace(pid: int, tool: str = "") -> str:
    """Return a truncated backtrace for the given process.

    Args:
        pid: Process id.
        tool: Stack tool to use; resolved automatically when not given.

    Returns:
        Backtrace text truncated to :data:`MAX_BACKTRACE_LINES` lines, or a
        short explanatory string when no backtrace could be obtained.
    """
    if not tool:
        tool = get_stack_tool()
    if not tool:
        return "(no stack trace tool available)"

    cmd = f"{tool} -p {pid}" if tool == "eu-stack" else f"{tool} {pid}"
    try:
        _, stdout, stderr = execute(cmd, mute=True, timeout=STACK_TOOL_TIMEOUT)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        return f"({tool} failed: {exc})"

    output = (stdout or stderr or "").strip()
    if not output:
        return f"({tool} returned no output)"

    lines = output.split("\n")
    if len(lines) > MAX_BACKTRACE_LINES:
        remaining = len(lines) - MAX_BACKTRACE_LINES
        lines = lines[:MAX_BACKTRACE_LINES] + [f"... ({remaining} more lines)"]

    return "\n".join(lines)


def get_snapshot_fraction() -> float:
    """Return the fraction of the looping limit after which snapshots start.

    Returns:
        Fraction between 0 and 1; the default is used when the configuration
        value is missing or unusable.
    """
    try:
        fraction = float(config.Pilot.looping_snapshot_fraction)
    except (AttributeError, ValueError, TypeError):
        return DEFAULT_SNAPSHOT_FRACTION

    return fraction if 0 < fraction < 1 else DEFAULT_SNAPSHOT_FRACTION


def get_core_dump_timeout() -> int:
    """Return the timeout for the core dump in seconds.

    Returns:
        Timeout in seconds.
    """
    try:
        return convert_to_int(config.Pilot.looping_core_dump_timeout, default=DEFAULT_CORE_DUMP_TIMEOUT)
    except AttributeError:
        return DEFAULT_CORE_DUMP_TIMEOUT


def get_core_dump_max_size() -> int:
    """Return the resident set size above which the core dump is skipped.

    Returns:
        Maximum size in bytes.
    """
    try:
        value = config.Pilot.looping_core_dump_max_size
    except AttributeError:
        value = DEFAULT_CORE_DUMP_MAX_SIZE

    try:
        return human2bytes(value)
    except ValueError:
        return human2bytes(DEFAULT_CORE_DUMP_MAX_SIZE)


def is_core_dump_wanted() -> bool:
    """Return True if a core dump should be produced for a looping job.

    Returns:
        True unless the configuration disables it.
    """
    try:
        value = str(config.Pilot.looping_core_dump).lower()
    except AttributeError:
        return True

    return value not in ("false", "0", "no", "off")


def take_process_snapshot(pid: int, cmdline: str, tool: str) -> dict:
    """Return a cheap diagnostic snapshot of a single process.

    Args:
        pid: Process id.
        cmdline: Full command line of the process.
        tool: Stack trace tool to use.

    Returns:
        Dictionary with the collected fields.
    """
    return {
        "pid": pid,
        "cmdline": cmdline,
        "state": get_process_state(pid),
        "cpu_time": get_cpu_time(pid),
        "rss": get_rss(pid),
        "threads": get_status_value(pid, "Threads"),
        "wchan": read_proc_file(pid, "wchan"),
        "syscall": get_current_syscall(pid),
        "cwd": read_proc_link(pid, "cwd"),
        "exe": read_proc_link(pid, "exe"),
        "backtrace": get_stack_trace(pid, tool=tool),
    }


def format_snapshot(snapshot: dict) -> str:
    """Return a snapshot rendered for the snapshot file.

    Args:
        snapshot: Snapshot dictionary as returned by :func:`take_looping_snapshot`.

    Returns:
        Formatted multi-line text.
    """
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(snapshot["time"]))
    lines = [
        "=" * 78,
        f"snapshot #{snapshot['index']} at {stamp} "
        f"(files last touched {snapshot['since_touch']} s ago)",
        "=" * 78,
    ]
    for process in snapshot["processes"]:
        lines += [
            "",
            f"pid={process['pid']} state={process['state']} "
            f"cpu_time={process['cpu_time']:.1f}s "
            f"rss={process['rss'] // (1024 * 1024)}MB threads={process['threads']}",
            f"  cmdline: {process['cmdline']}",
            f"  exe:     {process['exe']}",
            f"  cwd:     {process['cwd']}",
            f"  wchan:   {process['wchan']}",
            f"  syscall: {process['syscall']}",
            "  backtrace:",
        ]
        lines += [f"    {line}" for line in process["backtrace"].split("\n")]

    return "\n".join(lines) + "\n"


def take_looping_snapshot(job: Any, since_touch: int, looping_limit: int) -> None:
    """Record a diagnostic snapshot if the job is approaching the looping limit.

    Called from the looping job algorithm on every verification. No-op until
    the time since the last file touch exceeds
    ``looping_limit * looping_snapshot_fraction``, so that healthy jobs never
    pay for it. Never raises: a diagnostic must not be able to disturb the job
    monitoring loop.

    Args:
        job: Job object.
        since_touch: Seconds since the payload last touched a file.
        looping_limit: Looping detection limit in seconds.
    """
    try:
        threshold = int(looping_limit * get_snapshot_fraction())
        if since_touch < threshold:
            return

        jobid = str(getattr(job, "jobid", "") or "")
        if _snapshot_state["jobid"] != jobid:
            _snapshot_state["jobid"] = jobid
            _snapshot_state["snapshots"] = []

        candidates = select_dump_candidates(job)
        if not candidates:
            return

        tool = get_stack_tool()
        index = len(_snapshot_state["snapshots"]) + 1
        snapshot = {
            "index": index,
            "time": int(time.time()),
            "since_touch": since_touch,
            "processes": [take_process_snapshot(pid, cmdline, tool) for pid, cmdline in candidates],
        }
        _snapshot_state["snapshots"].append(snapshot)

        logger.info(
            f"{LOG_PREFIX}: recorded snapshot #{index} "
            f"({since_touch} s since last file touch, threshold={threshold} s, "
            f"looping limit={looping_limit} s)"
        )
        store_snapshot(job, snapshot)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: snapshot failed (ignored): {exc}")


def store_snapshot(job: Any, snapshot: dict) -> None:
    """Append a snapshot to the snapshot file in the job work directory.

    Args:
        job: Job object.
        snapshot: Snapshot dictionary.
    """
    if not job.workdir:
        return

    path = os.path.join(job.workdir, SNAPSHOT_FILENAME)
    try:
        write_file(path, format_snapshot(snapshot), mode="a")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: failed to append to {path}: {exc}")


def summarise_snapshots() -> str:
    """Return a summary of the deltas between the recorded snapshots.

    The deltas are the actual diagnosis: whether CPU time is advancing (a
    genuine loop) or frozen (a hang), whether the backtrace is unchanged
    (stuck in one place) or moving, and whether the resident set is still
    growing.

    Returns:
        Multi-line summary, or an explanatory string when there is nothing to
        compare.
    """
    snapshots = _snapshot_state["snapshots"]
    if not snapshots:
        return "no looping snapshots were recorded"
    if len(snapshots) == 1:
        return "only one looping snapshot was recorded - no deltas available"

    lines = [f"looping snapshot summary ({len(snapshots)} snapshots):"]
    first = snapshots[0]
    last = snapshots[-1]
    elapsed = last["time"] - first["time"]

    previous = {process["pid"]: process for process in first["processes"]}
    for process in last["processes"]:
        pid = process["pid"]
        if pid not in previous:
            lines.append(f"  pid={pid} appeared after the first snapshot: {process['cmdline']}")
            continue
        was = previous[pid]
        cpu_delta = process["cpu_time"] - was["cpu_time"]
        rss_delta = process["rss"] - was["rss"]
        same_stack = process["backtrace"] == was["backtrace"]
        same_syscall = process["syscall"] == was["syscall"]
        if elapsed > 0:
            cpu_fraction = 100.0 * cpu_delta / elapsed
            verdict = "spinning (CPU advancing)" if cpu_fraction > 5.0 else "not consuming CPU (hang, not a loop)"
        else:
            cpu_fraction = 0.0
            verdict = "unknown (no elapsed time between snapshots)"
        lines.append(
            f"  pid={pid} {verdict}: cpu +{cpu_delta:.1f}s over {elapsed}s ({cpu_fraction:.0f}%), "
            f"rss {rss_delta // (1024 * 1024):+d}MB, "
            f"stack {'unchanged' if same_stack else 'changed'}, "
            f"syscall {'unchanged' if same_syscall else 'changed'}"
        )

    return "\n".join(lines)


def get_hostname() -> str:
    """Return the worker node host name.

    Returns:
        Host name, or ``"unknown"`` if it could not be determined.
    """
    host = os.environ.get("PANDA_HOSTNAME", "")
    if not host and hasattr(os, "uname"):
        host = os.uname()[1]

    return host or "unknown"


def get_release_info(job: Any) -> list:
    """Return the software release fields of a job, for core file analysis.

    Args:
        job: Job object.

    Returns:
        List of ``"key: value"`` strings for the fields that are set.
    """
    fields = (
        ("swrelease", "swRelease"),
        ("homepackage", "homePackage"),
        ("platform", "cmtConfig/platform"),
        ("transformation", "transformation"),
        ("imagename", "container image"),
    )
    info = []
    for attribute, label in fields:
        value = getattr(job, attribute, "")
        if value:
            info.append(f"{label}: {value}")

    return info


def get_shared_libraries(pid: int, maximum: int = 40) -> list:
    """Return the shared libraries mapped by a process.

    gdb needs the libraries as well as the main executable in order to resolve
    a core file, and on a grid worker node they come from CVMFS paths that are
    not reconstructable from the release name alone.

    Args:
        pid: Process id.
        maximum: Maximum number of paths to return.

    Returns:
        List of library paths truncated to *maximum* entries, with libraries
        from outside the system directories listed first - those are the release
        libraries that gdb will not find on its own.
    """
    maps = read_proc_file(pid, "maps")
    if not maps:
        return []

    paths = set()
    for line in maps.split("\n"):
        match = re.search(r"\s(/\S+\.so(?:\.\S+)?)$", line)
        if match:
            paths.add(match.group(1))

    system_prefixes = ("/usr/", "/lib/", "/lib64/", "/bin/", "/sbin/")
    ordered = sorted(paths, key=lambda path: (path.startswith(system_prefixes), path))

    return ordered[:maximum]


def get_core_analysis_info(job: Any, pid: int, cmdline: str, core_path: str, with_core: bool = True) -> str:
    """Return the block describing how to analyse a core file.

    gdb cannot open a core file without being told which binary produced it,
    and that information is only available while the job is still running. It
    is therefore recorded here, both in the pilot log (behind
    :data:`CORE_INFO_MARKER`) and in a companion file next to the core file.

    Args:
        job: Job object.
        pid: Process id the core file was taken from.
        cmdline: Full command line of that process.
        core_path: Path to the core file.
        with_core: Whether a core file was actually requested. When False the
            block still records the executable identity, since the backtraces
            in the pilot log need it too.

    Returns:
        Multi-line text block.
    """
    executable = read_proc_link(pid, "exe")
    cwd = read_proc_link(pid, "cwd")
    core_name = os.path.basename(core_path)

    lines = [
        CORE_INFO_MARKER,
        f"core file: {core_name if with_core else '(none - backtraces only)'}",
        f"PanDA job id: {getattr(job, 'jobid', 'unknown')}",
        f"pid: {pid}",
        f"executable: {executable or 'unknown'}",
        f"command line: {cmdline or 'unknown'}",
        f"working directory: {cwd or 'unknown'}",
        f"resident set at dump time: {get_rss(pid) // (1024 * 1024)} MB",
        f"host: {get_hostname()}",
        f"queue: {os.environ.get('PILOT_SITENAME', 'unknown')}",
    ]
    lines += get_release_info(job)

    if not executable:
        lines += [
            "",
            "the executable could not be resolved from /proc - use the command line",
            "above to identify the binary within the software release",
        ]
    elif with_core:
        lines += [
            "",
            "to analyse:",
            f"  gdb {executable} {core_name}",
        ]

    libraries = get_shared_libraries(pid)
    if libraries:
        lines += ["", "shared libraries mapped at dump time:"]
        lines += [f"  {library}" for library in libraries]

    lines.append(CORE_INFO_MARKER)

    return "\n".join(lines)


def store_core_analysis_info(job: Any, pid: int, cmdline: str, core_path: str, with_core: bool = True) -> None:
    """Log the core file analysis information and write it next to the core file.

    Args:
        job: Job object.
        pid: Process id the core file was taken from.
        cmdline: Full command line of that process.
        core_path: Path to the core file in the job work directory.
        with_core: Whether a core file was actually requested.
    """
    info = get_core_analysis_info(job, pid, cmdline, core_path, with_core=with_core)
    logger.info(f"\n{info}")

    path = f"{core_path}{CORE_INFO_SUFFIX}"
    try:
        write_file(path, info + "\n")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: failed to write {path}: {exc}")
    else:
        logger.info(f"{LOG_PREFIX}: wrote core file analysis information to {os.path.basename(path)}")


def get_gdb_setup(job: Any) -> str:
    """Return the experiment setup needed to get a usable gdb.

    The system gdb on a worker node is frequently too old to read a core file
    from a current software release, which is why the server-driven debug path
    already prepends a setup before running gdb. The same hook is reused here.

    Args:
        job: Job object.

    Returns:
        Setup command ending in ``'; '``, or an empty string when the plugin
        does not provide one.
    """
    pilot_user = os.environ.get("PILOT_USER", "generic").lower()
    try:
        user = __import__(f"pilot.user.{pilot_user}.common", globals(), locals(), [pilot_user], 0)
    except ImportError as exc:
        logger.warning(f"{LOG_PREFIX}: cannot import the {pilot_user} plugin: {exc}")
        return ""

    # reuse the debug command preprocessing, which prepends the setup to
    # job.debug_command; do it on a scratch object so that job.debug_command
    # (which the looping algorithm uses as a marker) is left alone
    class _Scratch:  # pylint: disable=too-few-public-methods
        """Minimal stand-in carrying the fields preprocess_debug_command() touches."""

        def __init__(self, _job: Any):
            self.debug_command = ""
            self.noexecstrcnv = getattr(_job, "noexecstrcnv", False)
            self.jobparams = getattr(_job, "jobparams", "")
            self.infosys = getattr(_job, "infosys", None)
            self.swrelease = getattr(_job, "swrelease", "")
            self.homepackage = getattr(_job, "homepackage", "")
            self.platform = getattr(_job, "platform", "")
            self.jobid = getattr(_job, "jobid", "")
            self.workdir = getattr(_job, "workdir", "")

    scratch = _Scratch(job)
    try:
        user.preprocess_debug_command(scratch)
    except AttributeError:
        logger.debug(f"{LOG_PREFIX}: the {pilot_user} plugin has no preprocess_debug_command()")
        return ""
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: failed to build the gdb setup: {exc}")
        return ""

    return scratch.debug_command


def has_room_for_core(workdir: str, rss: int) -> bool:
    """Return True if there is enough free space in the work directory.

    Args:
        workdir: Job work directory.
        rss: Resident set size of the process to be dumped, in bytes.

    Returns:
        True if the core file is expected to fit.
    """
    needed = int(rss * CORE_SIZE_SAFETY_FACTOR)
    try:
        free = disk_usage(workdir).free
    except OSError as exc:
        logger.warning(f"{LOG_PREFIX}: cannot determine free space in {workdir}: {exc}")
        return True

    if free < needed:
        logger.warning(
            f"{LOG_PREFIX}: skipping core dump - {needed // (1024 * 1024)} MB needed but only "
            f"{free // (1024 * 1024)} MB free in {workdir}"
        )
        return False

    return True


def resume_process(pid: int) -> None:
    """Send ``SIGCONT`` to a process.

    A gdb that is killed while attached can leave its inferior group-stopped,
    so the payload is explicitly resumed after a failed or timed out dump. The
    payload is about to be killed anyway, but a stopped process cannot be
    killed cleanly.

    Args:
        pid: Process id.
    """
    try:
        os.kill(pid, signal.SIGCONT)
    except OSError as exc:
        logger.debug(f"{LOG_PREFIX}: could not resume pid={pid}: {exc}")
    else:
        logger.info(f"{LOG_PREFIX}: sent SIGCONT to pid={pid} in case gdb left it stopped")


def build_gdb_command(pid: int, core_path: str, setup: str, with_core: bool) -> str:
    """Return the gdb command used to dump a process.

    The backtraces are requested before ``generate-core-file`` so that a
    timeout during the (expensive) core write still leaves the (cheap and often
    sufficient) backtraces behind. ``py-bt`` is included because for a looping
    transform the Python stack usually identifies the algorithm directly; it is
    silently ignored by a gdb without the Python extension.

    Args:
        pid: Process id to attach to.
        core_path: Absolute path of the core file to write.
        setup: Experiment setup prepended to the command.
        with_core: Whether to include the ``generate-core-file`` step.

    Returns:
        Full command string.
    """
    options = [
        "-batch",
        "-ex 'set confirm off'",
        "-ex 'set pagination off'",
        "-ex 'thread apply all bt'",
        "-ex 'py-bt'",
    ]
    if with_core:
        options.append(f"-ex 'generate-core-file {core_path}'")
    options += ["-ex detach", "-ex quit"]

    return f"{setup}gdb -p {pid} {' '.join(options)}"


def create_core_dump(job: Any) -> None:
    """Create a core dump of the looping payload and record how to analyse it.

    Targets the best candidate from :func:`select_dump_candidates` rather than
    an arbitrary descendant, uses the experiment's gdb, writes directly into
    the job work directory, and captures the backtraces before the core file so
    that a timeout is not a total loss. The executable identity is recorded in
    the pilot log and in a companion file so that the core file can still be
    opened long after the worker node is gone.

    Args:
        job: Job object. Must have ``pid`` and ``workdir`` set.
    """
    if not job.pid or not job.workdir:
        logger.warning(f"{LOG_PREFIX}: cannot create a core file since pid or workdir is unknown")
        return

    logger.info(summarise_snapshots())

    candidates = select_dump_candidates(job)
    if not candidates:
        logger.warning(f"{LOG_PREFIX}: no dump candidate could be identified")
        return

    pid, cmdline = candidates[0]
    logger.info(f"{LOG_PREFIX}: selected pid={pid} for the core dump: {cmdline}")

    rss = get_rss(pid)
    with_core = is_core_dump_wanted()
    if with_core and rss > get_core_dump_max_size():
        logger.warning(
            f"{LOG_PREFIX}: not dumping a core file for pid={pid} - its resident set "
            f"({rss // (1024 * 1024)} MB) exceeds the configured maximum "
            f"({get_core_dump_max_size() // (1024 * 1024)} MB); keeping the backtraces only"
        )
        with_core = False
    if with_core and not has_room_for_core(job.workdir, rss):
        with_core = False

    core_path = os.path.join(job.workdir, f"core.{pid}")
    setup = get_gdb_setup(job)
    cmd = build_gdb_command(pid, core_path, setup, with_core)
    timeout = get_core_dump_timeout()

    # the analysis information must be collected while the process still exists,
    # since /proc/<pid>/exe and /proc/<pid>/maps disappear with it
    store_core_analysis_info(job, pid, cmdline, core_path, with_core=with_core)

    logger.info(f"{LOG_PREFIX}: running gdb on pid={pid} (timeout={timeout} s, core={with_core})")
    exit_code, stdout, stderr = execute(cmd, cwd=job.workdir, timeout=timeout)
    output = (stdout or "") + (stderr or "")
    if output:
        logger.info(f"{LOG_PREFIX}: gdb output for pid={pid}:\n{output}")

    if exit_code != 0:
        if exit_code == errors.COMMANDTIMEDOUT:
            logger.warning(
                f"{LOG_PREFIX}: gdb timed out after {timeout} s - any core file will be truncated "
                f"or missing, but the backtraces above were captured first"
            )
        else:
            logger.warning(f"{LOG_PREFIX}: gdb failed with exit code {exit_code}")
        resume_process(pid)

    if with_core:
        if os.path.exists(core_path):
            size = os.path.getsize(core_path)
            logger.info(f"{LOG_PREFIX}: core file written: {os.path.basename(core_path)} ({size // (1024 * 1024)} MB)")
        else:
            logger.warning(f"{LOG_PREFIX}: no core file was produced at {core_path}")
