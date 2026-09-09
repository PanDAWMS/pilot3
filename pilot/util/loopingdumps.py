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

This module replaces that with three things.

**An unfiltered inventory** (:func:`log_process_inventory`). What an ATLAS payload
tree actually contains during a loop has not been established, so the complete
tree is logged - depth, pid, ppid, process name, state, CPU time, resident set,
drop status and full command line for every descendant, including the ones that
were dropped - bracketed by :data:`INVENTORY_MARKER` so the inventories of
several looping jobs can be grepped out and compared. Any refinement of the
selection should come from those, not from assumptions made here.

**A minimal denylist and a ranked candidate list**
(:func:`select_dump_candidates`). Only processes the pilot demonstrably puts into
the payload tree are rejected: prmon, shells, the container runtimes and the ALRB
setup calls. What survives is ranked by accumulated CPU time - a *looping*
payload normally spins, which is exactly what separates a loop from a hang - then
by resident set. A per-experiment hook for payload process names exists and is
consulted first, but is empty for every experiment: a name that matched the wrong
process would promote it above the real payload, which is worse than declaring
nothing, so the hook is left for the inventories to fill in.

**A series of cheap snapshots** (:func:`take_looping_snapshot`). A core file is
one instant, whereas a loop is characterised by what changes and what does not
between samples, so once the job is a configurable fraction of the way to the
looping limit the pilot starts recording, at every looping verification, the
accumulated CPU time, process state, ``wchan``, current syscall, resident set
and a truncated backtrace of every candidate. That is kilobytes per snapshot
against gigabytes for a core file, and the deltas between consecutive snapshots
are summarised at kill time (:func:`summarise_snapshots`). Because the snapshots
cover the top :data:`MAX_CANDIDATES` processes rather than only the winner, a
mis-ranked first choice still leaves the real payload sampled.

At kill time at most one core file is still produced
(:func:`create_core_dump`), in two independent phases with independent
timeouts. The first ordering tried - all of it in one gdb invocation, with the
backtraces requested before the core write "so that a timeout still leaves
something behind" - was wrong in production, and wrong in three separate ways:

* it assumed the core write was the expensive step. It is not. ``generate-core-file``
  only walks the inferior's mappings and needs no symbols at all, whereas
  ``thread apply all bt`` needs the symbol table of every mapped object, which
  for an athena process means several hundred shared libraries read over CVMFS.
  On a real looping job the backtraces had not finished after four minutes and
  the core write was never reached, so the cheap artifact was lost to the
  expensive one;
* it prepended the experiment setup to the invocation that writes the core
  file, which cost about a minute of asetup before gdb even started, wrote
  ``.asetup.save`` into the job work directory, and exported ``PYTHONHOME`` and
  ``PYTHONPATH`` pointing at the release's Python. A gdb linked against a
  different libpython honours those, cannot find the ``encodings`` module and
  aborts inside ``Py_Initialize()`` before executing a single ``-ex`` command
  (see :data:`PYTHON_ENVIRONMENT_VARIABLES`), losing both artifacts at once;
* it relied on the return value of :func:`pilot.util.container.execute` to
  carry the output, and that function discards stdout when a command times out.
  Everything gdb had printed before the timeout was thrown away, and the log
  claimed the backtraces had been captured when nothing had been.

So: phase A writes the core file with a bare gdb, no experiment setup, and a
sanitised environment. Phase B collects the backtraces with the experiment
setup, on its own timeout, and is allowed to fail - the stacks are in the core
file too. Both phases redirect to a file in the job work directory
(:data:`GDB_OUTPUT_SUFFIX`) rather than relying on the return value, so a
timeout keeps whatever was produced up to that point, and both run with a
working directory *outside* the job work directory so that the setup cannot
write there. The total is bounded by a single diagnostics budget
(:func:`get_diagnostics_budget`), because everything here happens between the
decision to kill and the kill itself.

The release gdb is only needed to *read* a core file, and that happens offline,
long after the worker node is gone. What it needs in order to be possible at
all is recorded next to the core file by :func:`get_core_analysis_info`,
including the container the payload ran in: the payload's system libraries come
from the container image, not from the worker node, so a gdb running on the
host resolves the system frames against the wrong binaries.

Because a core file is useless without knowing which binary produced it, the
executable identity is recorded twice: as a greppable block in the pilot log
marked with :data:`CORE_INFO_MARKER`, and as a companion file next to the core
file in the job work directory, so that whoever picks up the log tarball later
can run gdb against the right binary and the right software release.

**These files must never look like payload activity.** The looping algorithm
decides that a payload is alive by taking the modification time of the most
recently modified file in the job work directory, and everything this module
writes lands in that same directory. A diagnostic write therefore looks exactly
like the payload doing work, which resets the very clock that triggered the
diagnostic: the snapshot series starts at a fraction of the looping limit, so
the time since the last touch was pinned just below that fraction and could
never reach the limit. No looping job could be detected at all. Two independent
guards prevent that, and both are needed - the first covers all artifacts, the
second holds even if a caller forgets to apply the first:

* :func:`is_looping_diagnostic_file` names every artifact written here, and the
  looping algorithm drops those paths from the file list it measures
  (:func:`pilot.util.loopingjob.get_time_for_last_touch`), centrally rather
  than in each experiment plugin - the file names belong to this module, and
  seven separate plugin filters are what failed to catch this;
* :func:`store_snapshot` pins the modification time of the snapshot file to the
  payload's own last touch, so that the file cannot be the newest file in the
  work directory no matter who looks at it.
"""

from __future__ import annotations

import logging
import os
import re
import signal
import tempfile
import time
from shutil import (
    disk_usage,
    rmtree,
    which
)
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import (
    get_modification_time,
    write_file
)
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

# Name of the file the gdb phases redirect their output to, written next to the
# core file. The output cannot be taken from the return value of execute():
# that function discards stdout when a command times out, which is precisely
# the case in which the partial output matters most.
GDB_OUTPUT_SUFFIX = ".gdb.txt"

# Marker bracketing the unfiltered inventory of the payload process tree. The
# selection heuristics rest on assumptions about what that tree contains during a
# loop, so the inventory is logged in full - including the processes that were
# dropped - to let a payload name list be derived from real jobs rather than
# guessed at. Grep this out of the logs of several looping jobs to build it.
INVENTORY_MARKER = "PAYLOAD PROCESS INVENTORY"

# Name of the file in the job work directory holding the snapshot series.
SNAPSHOT_FILENAME = "looping_snapshots.log"

# Core files written by create_core_dump() are named 'core.<pid>'. Matched
# against the basename so that the looping algorithm can recognise them as its
# own output rather than as payload progress.
CORE_FILE_PATTERN = re.compile(r"^core\.\d+$")

# Command line fragments identifying processes that are known not to be the
# looping payload. Matched case-insensitively against the basename of argv[0]
# and, for DENYLISTED_ARGS, against the full command line.
#
# Deliberately minimal. Every entry here is one the pilot itself demonstrably
# puts inside the payload tree:
#
# * prmon - the memory monitor, and the process the core dump was in fact being
#   taken from; 'memorymonitor' is the pilot's own internal name for it
#   (config.Pilot.utility_after_payload_started);
# * sh/bash - execute() runs every command as '/bin/bash -c <...>', so job.pid
#   is itself a shell and the transform's own wrappers are shells too;
# * apptainer/singularity - the container runtimes used by the container plugin;
# * asetup/lsetup/atlasLocalSetup.sh/setupATLAS - the ALRB setup calls embedded
#   in the payload command string.
#
# Nothing is added on suspicion. An over-broad denylist fails the same way a
# guessed payload name list does, only more quietly: dropping the real payload
# leaves the ranking to pick something worse, and the log would show the entry
# as filtered rather than as chosen wrongly. Anything else that turns out to
# live in the tree should be added on the evidence of the logged inventories
# (see INVENTORY_MARKER), not in advance.
DENYLISTED_NAMES = (
    "prmon",
    "memorymonitor",
    "sh",
    "bash",
    "apptainer",
    "singularity",
    "asetup",
    "lsetup",
    "atlaslocalsetup.sh",
    "setupatlas",
)

# Full-command-line fragments that disqualify a process regardless of argv[0],
# for a helper invoked through an interpreter (e.g. a prmon wrapper script).
# Kept to prmon alone: a fragment that can occur inside a legitimate payload
# command line would silently drop the very process being looked for.
DENYLISTED_ARGS = (
    "prmon",
)

# Maximum number of candidates carried through to snapshotting. Bounds both the
# snapshot size and the number of stack tool invocations per snapshot.
MAX_CANDIDATES = 5

# Maximum number of candidates stack traced at kill time. pstack is a gdb
# wrapper and costs up to its full timeout per process on a large payload, so
# this bounds the time between the decision to kill and the log upload. The
# candidates are ranked, so the ones dropped are the least likely to explain the
# loop, and the best candidate has already been dumped in more detail.
MAX_STACK_TRACE_CANDIDATES = 2

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

# Default timeout for phase A, the core file, in seconds. 'generate-core-file'
# writes the whole address space but needs no symbols, so the cost is bounded by
# the resident set and the disk: at DEFAULT_CORE_DUMP_MAX_SIZE this is ample.
DEFAULT_CORE_DUMP_TIMEOUT = 300

# Default timeout for phase B, the backtraces, in seconds. This is the phase
# that needs the symbol table of every mapped object, several hundred of them
# over CVMFS for an athena process, and is the one that timed out in production.
# It is allowed to: the stacks are in the core file from phase A as well.
DEFAULT_BACKTRACE_TIMEOUT = 300

# Default upper bound on the total time the diagnostics may take, in seconds.
# Everything in this module runs between the decision to kill a looping payload
# and the kill itself, so an unbounded sum of per-step timeouts delays the log
# upload on a worker node that may be close to its wall clock limit.
DEFAULT_DIAGNOSTICS_BUDGET = 900

# Default upper bound on the resident set of a process for which a core file is
# still attempted. Above this the backtraces are kept and the core file skipped.
# The core file is deliberately kept in the log tarball so that it can be
# analysed afterwards, which is what bounds this: the value is a limit on the
# size of every looping job's log file, not just on the dump.
DEFAULT_CORE_DUMP_MAX_SIZE = "2 GB"

# Maximum number of gdb frames requested per thread in phase B.
MAX_BACKTRACE_FRAMES = 100

# Maximum number of lines of gdb output echoed into the pilot log. The full
# output is in the file next to the core file either way; this only makes the
# common case greppable without unpacking the log tarball.
MAX_GDB_LOG_LINES = 200

# Environment variables that must not reach gdb. The experiment setup exports
# PYTHONHOME and PYTHONPATH pointing at the release's Python. A gdb linked
# against a different libpython honours them, fails to find the 'encodings'
# module and aborts during Py_Initialize() with
#
#   Fatal Python error: init_fs_encoding: failed to get the Python codec of the
#   filesystem encoding
#   ModuleNotFoundError: No module named 'encodings'
#
# before executing a single -ex command, so neither the core file nor the
# backtraces are produced and gdb exits 1. Observed in production. Note that the
# pilot's own environment can carry these too, since the pilot itself normally
# runs under an ALRB Python, so they are stripped in every phase and not only
# after the release setup.
PYTHON_ENVIRONMENT_VARIABLES = (
    "PYTHONHOME",
    "PYTHONPATH",
    "PYTHONSTARTUP",
    "PYTHONNOUSERSITE",
)

# Fragments identifying the failure above in the gdb output. Used to decide
# whether a retry is worth attempting and, more importantly, to say so in the
# log: the message names the payload's Python, not gdb's, and is easy to
# misread as a payload failure.
PYTHON_FAILURE_SIGNATURES = (
    "init_fs_encoding",
    "No module named 'encodings'",
)

# Environment used for the last-resort retry, when even the sanitised
# environment leaves gdb unable to start its interpreter.
CLEAN_ENVIRONMENT = "env -i PATH=/usr/bin:/bin:/usr/local/bin TERM=dumb HOME=/tmp "

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


def is_looping_diagnostic_file(path: str) -> bool:
    """Return True if the given path is a file the looping diagnostics wrote.

    The looping algorithm must not measure the pilot's own diagnostic output as
    if it were payload activity; see the module docstring. Everything this
    module writes into the job work directory is listed here:

    * the snapshot series (:data:`SNAPSHOT_FILENAME`);
    * the core files (``core.<pid>``, :data:`CORE_FILE_PATTERN`);
    * the core file analysis companions (``*.analysis.txt``,
      :data:`CORE_INFO_SUFFIX`);
    * the gdb output of the two dump phases (``*.gdb.txt``,
      :data:`GDB_OUTPUT_SUFFIX`).

    Note what is deliberately *not* listed: ``.asetup.save``. The experiment
    setup writes it into its working directory, which is why the gdb phases run
    outside the job work directory (:func:`create_scratch_directory`) rather
    than being filtered by name here. A multi-step transform legitimately
    rewrites ``.asetup.save`` between steps, so filtering it would hide real
    payload activity and could turn a healthy job into a looping one.

    Args:
        path: File path, absolute or relative.

    Returns:
        True if the path is a looping diagnostic artifact.
    """
    name = os.path.basename(path or "")
    if not name:
        return False

    if name == SNAPSHOT_FILENAME:
        return True

    if name.endswith(CORE_INFO_SUFFIX) or name.endswith(GDB_OUTPUT_SUFFIX):
        return True

    return bool(CORE_FILE_PATTERN.match(name))


def remove_diagnostic_files(files: list) -> list:
    """Return the given file list without the looping diagnostic artifacts.

    Called by the looping algorithm on the list of recently modified files
    before their modification times are used to decide whether the payload is
    still alive.

    Args:
        files: File paths.

    Returns:
        The paths that are not looping diagnostic artifacts.
    """
    kept = [_file for _file in files or [] if not is_looping_diagnostic_file(_file)]

    dropped = len(files or []) - len(kept)
    if dropped:
        logger.debug(
            f"{LOG_PREFIX}: ignoring {dropped} looping diagnostic file(s) in the work directory - "
            f"they are pilot output, not payload activity"
        )

    return kept


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
    """Return True if the given command line belongs to a known non-payload helper.

    Only processes the pilot demonstrably puts into the payload tree are
    rejected (see :data:`DENYLISTED_NAMES`). Everything else is kept, on the
    principle that a process wrongly dropped here disappears from the ranking
    silently, whereas a process wrongly kept is at worst outranked - and is
    visible either way in the logged inventory.

    Args:
        cmdline: Full command line of the process.

    Returns:
        True if the process should not be considered as a dump target.
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


def get_ppid(pid: int) -> int:
    """Return the parent process id from ``/proc/<pid>/stat``.

    Args:
        pid: Process id.

    Returns:
        Parent process id, or 0 if it could not be determined.
    """
    stat = read_proc_file(pid, "stat")
    if not stat:
        return 0

    try:
        return int(stat[stat.rindex(")") + 1:].split()[1])
    except (ValueError, IndexError):
        return 0


def get_process_name(cmdline: str) -> str:
    """Return the process name that a payload name list would be made of.

    For an interpreted payload the interpreter is not the interesting name, so
    the first argument that looks like a script is preferred over ``argv[0]``
    (``python .../Generate_tf.py`` gives ``Generate_tf.py``, not ``python3``).
    Shells are deliberately not unwrapped: the first word of a ``bash -c``
    string is usually a variable assignment or a setup call rather than the
    payload, and the inventory carries the full command line anyway.

    Args:
        cmdline: Full command line of the process.

    Returns:
        Process name, or an empty string when the command line is unreadable.
    """
    if not cmdline:
        return ""

    parts = cmdline.split()
    argv0 = os.path.basename(parts[0])
    if argv0.startswith("python") or argv0.startswith("perl"):
        for part in parts[1:]:
            if part.startswith("-") or "=" in part:
                continue
            candidate = os.path.basename(part)
            if candidate:
                return candidate

    return argv0


def get_tree_depth(pid: int, root_pid: int, ppids: dict, maximum: int = 25) -> int:
    """Return the depth of a process below the payload process.

    Args:
        pid: Process id.
        root_pid: Payload process id, i.e. the root of the tree.
        ppids: Mapping of pid to parent pid for the known descendants.
        maximum: Guard against a cycle in a pathological ``/proc``.

    Returns:
        Depth below *root_pid* (1 for a direct child), or 0 if it could not be
        established.
    """
    depth = 0
    current = pid
    while current and current != root_pid and depth < maximum:
        current = ppids.get(current) or get_ppid(current)
        depth += 1

    return depth if current == root_pid else 0


def log_process_inventory(job: Any, label: str = "") -> list:
    """Log every process in the payload tree, whether or not it is a candidate.

    This is the observational half of the diagnostics and is deliberately
    unfiltered: the denylist and the ranking are both built on assumptions about
    what an ATLAS payload tree actually contains during a loop, and nobody has
    yet looked at one. Logging the complete inventory - names, depth, state, CPU
    time and resident set for every descendant, including the ones that were
    dropped - makes it possible to derive a payload name list from real jobs
    instead of guessing at one, and to check the denylist against reality.

    The block is bracketed by :data:`INVENTORY_MARKER` so that the inventories
    from many jobs can be grepped out of their logs and compared.

    Args:
        job: Job object; ``job.pid`` must be set.
        label: Optional context added to the header, e.g. a snapshot number.

    Returns:
        List of ``(pid, cmdline)`` tuples for the whole tree, as collected.
    """
    descendants = get_descendants(job.pid)
    ppids = {pid: get_ppid(pid) for pid, _ in descendants}

    header = f"{INVENTORY_MARKER}"
    if label:
        header += f" ({label})"
    lines = [
        header,
        f"payload process: pid={job.pid} name={get_process_name(get_cmdline(job.pid))!r}",
        f"descendants: {len(descendants)}",
        "",
        f"{'depth':>5}  {'pid':>7}  {'ppid':>7}  {'name':<28}  {'st':<2}  "
        f"{'cpu_s':>9}  {'rss_MB':>7}  {'drop':<4}  cmdline",
    ]
    rows = []
    for pid, cmdline in descendants:
        rows.append((
            get_tree_depth(pid, job.pid, ppids),
            pid,
            ppids.get(pid, 0),
            get_process_name(cmdline),
            get_process_state(pid),
            get_cpu_time(pid),
            get_rss(pid),
            is_denylisted(cmdline),
            cmdline,
        ))
    for depth, pid, ppid, name, state, cpu, rss, dropped, cmdline in sorted(rows):
        lines.append(
            f"{depth:>5}  {pid:>7}  {ppid:>7}  {name[:28]:<28}  {state:<2}  "
            f"{cpu:>9.1f}  {rss // (1024 * 1024):>7}  {'yes' if dropped else 'no':<4}  {cmdline}"
        )
    if not descendants:
        lines.append("  (no descendants found)")
    lines.append(INVENTORY_MARKER)

    logger.info("\n".join(lines))

    return descendants


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


def select_dump_candidates(job: Any, label: str = "") -> list:
    """Return the payload processes ranked by how likely they are to be looping.

    The complete tree is logged first by :func:`log_process_inventory`, including
    the processes that are dropped, so that a wrong choice can be diagnosed from
    the log afterwards rather than guessed at.

    Note that positive name matching is currently inert: no plugin except a
    deliberately configured one declares any payload names, so in practice the
    ranking is decided by accumulated CPU time and then resident set. That is on
    purpose - the name list is meant to be derived from the logged inventories of
    real looping jobs, not assumed in advance.

    Args:
        job: Job object; ``job.pid`` must be set.
        label: Optional context passed through to the inventory header.

    Returns:
        List of ``(pid, cmdline)`` tuples, best candidate first, truncated to
        :data:`MAX_CANDIDATES`. Falls back to ``[(job.pid, <cmdline>)]`` when
        every descendant was filtered out.
    """
    if not job.pid:
        logger.warning(f"{LOG_PREFIX}: cannot select a dump candidate - job.pid is not set")
        return []

    payload_names = get_payload_process_names()
    descendants = log_process_inventory(job, label=label)

    kept = [(pid, cmdline) for pid, cmdline in descendants if not is_denylisted(cmdline)]

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
    if not payload_names:
        lines.append(
            "  (no payload names declared for this experiment - ranked on CPU time and "
            "resident set only; see the inventory above to derive a name list)"
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
    """Return the timeout for phase A, the core file, in seconds.

    Returns:
        Timeout in seconds.
    """
    try:
        return convert_to_int(config.Pilot.looping_core_dump_timeout, default=DEFAULT_CORE_DUMP_TIMEOUT)
    except AttributeError:
        return DEFAULT_CORE_DUMP_TIMEOUT


def get_backtrace_timeout() -> int:
    """Return the timeout for phase B, the backtraces, in seconds.

    Returns:
        Timeout in seconds.
    """
    try:
        return convert_to_int(config.Pilot.looping_backtrace_timeout, default=DEFAULT_BACKTRACE_TIMEOUT)
    except AttributeError:
        return DEFAULT_BACKTRACE_TIMEOUT


def get_diagnostics_budget() -> int:
    """Return the total time the looping diagnostics may take, in seconds.

    Bounds the sum of the dump phases rather than each of them individually,
    since they all run between the decision to kill the payload and the kill.

    Returns:
        Budget in seconds.
    """
    try:
        return convert_to_int(config.Pilot.looping_diagnostics_budget, default=DEFAULT_DIAGNOSTICS_BUDGET)
    except AttributeError:
        return DEFAULT_DIAGNOSTICS_BUDGET


def get_remaining_budget(deadline: float) -> int:
    """Return the time left before the diagnostics deadline, in seconds.

    Args:
        deadline: Deadline as a :func:`time.monotonic` value.

    Returns:
        Seconds remaining, never negative.
    """
    return max(0, int(deadline - time.monotonic()))


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

        index = len(_snapshot_state["snapshots"]) + 1
        candidates = select_dump_candidates(job, label=f"snapshot #{index}")
        if not candidates:
            return

        tool = get_stack_tool()
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


def pin_diagnostic_mtime(path: str, mtime: int) -> None:
    """Set the modification time of a diagnostic file to the given time.

    The snapshot file lives in the job work directory, which is exactly what
    the looping algorithm scans for payload activity, so writing it would
    otherwise make the payload look alive and reset the looping clock. Pinning
    the modification time to the payload's own last touch makes the file
    incapable of being the newest file in the work directory, independently of
    any name based filtering.

    Args:
        path: File path.
        mtime: Modification time to set, in seconds since the Unix epoch.
    """
    try:
        os.utime(path, (mtime, mtime))
    except OSError as exc:
        # the central filter in the looping algorithm still covers this file
        logger.warning(f"{LOG_PREFIX}: could not pin the modification time of {path}: {exc}")


def store_snapshot(job: Any, snapshot: dict) -> None:
    """Append a snapshot to the snapshot file in the job work directory.

    The modification time of the file is pinned to the time of the payload's
    last file touch (see :func:`pin_diagnostic_mtime`), which for the first
    snapshot is derived from the snapshot itself and afterwards is simply the
    time already carried by the file.

    Args:
        job: Job object.
        snapshot: Snapshot dictionary.
    """
    if not job.workdir:
        return

    path = os.path.join(job.workdir, SNAPSHOT_FILENAME)

    # the time the payload last touched a file: never later than this, so that the
    # snapshot file cannot look like payload activity
    pinned = get_modification_time(path)
    if pinned is None:
        pinned = int(snapshot.get("time", time.time())) - int(snapshot.get("since_touch", 0))

    try:
        write_file(path, format_snapshot(snapshot), mode="a")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: failed to append to {path}: {exc}")
        return

    pin_diagnostic_mtime(path, pinned)


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


def get_container_analysis_info(job: Any, setup: str) -> list:
    """Return the notes explaining how to reproduce the payload's environment.

    A core file has to be read by a gdb running in the same environment as the
    payload produced it in. The payload's system libraries - libc, libpthread,
    the dynamic loader - come from the container image and not from the worker
    node, so a gdb running on the host resolves those frames against the wrong
    binaries even though the release libraries on CVMFS resolve correctly.

    The container invocation is taken verbatim from the payload process rather
    than reconstructed from the job description, so that it stays right for
    whatever the pilot actually did, including the bind mounts.

    Args:
        job: Job object.
        setup: Experiment setup string as returned by :func:`get_gdb_setup`.

    Returns:
        List of lines, empty when nothing could be established.
    """
    lines = [
        "",
        "IMPORTANT: run gdb inside a container of the same platform as the payload.",
        "The payload's system libraries (libc, libpthread, the dynamic loader) come from",
        "the container image, not from the worker node, so a gdb running on the host will",
        "resolve the system frames against the wrong binaries. The working directory given",
        "above is the one seen inside the container.",
    ]

    container_command = get_cmdline(job.pid)
    if container_command:
        lines += [
            "",
            "the payload container was started with:",
            f"  {container_command}",
        ]

    if setup:
        lines += [
            "",
            "release setup (as used by the pilot):",
            f"  {setup.strip().rstrip(';')}",
        ]

    lines += [
        "",
        "if gdb aborts with \"ModuleNotFoundError: No module named 'encodings'\", the",
        "release setup has exported a PYTHONHOME/PYTHONPATH that does not match the Python",
        "that gdb itself is linked against - unset both before starting gdb.",
    ]

    return lines


def get_core_analysis_info(job: Any, pid: int, cmdline: str, core_path: str,
                           *, with_core: bool = True, setup: str = "") -> str:
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
        setup: Experiment setup string, recorded so that the release can be set
            up again when the core file is opened.

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

    lines.append(f"gdb output from the dump phases: {os.path.basename(core_path)}{GDB_OUTPUT_SUFFIX}")
    lines += get_container_analysis_info(job, setup)

    libraries = get_shared_libraries(pid)
    if libraries:
        lines += ["", "shared libraries mapped at dump time:"]
        lines += [f"  {library}" for library in libraries]

    lines.append(CORE_INFO_MARKER)

    return "\n".join(lines)


def store_core_analysis_info(job: Any, pid: int, cmdline: str, core_path: str,
                             *, with_core: bool = True, setup: str = "") -> None:
    """Log the core file analysis information and write it next to the core file.

    Args:
        job: Job object.
        pid: Process id the core file was taken from.
        cmdline: Full command line of that process.
        core_path: Path to the core file in the job work directory.
        with_core: Whether a core file was actually requested.
        setup: Experiment setup string, recorded in the block.
    """
    info = get_core_analysis_info(job, pid, cmdline, core_path, with_core=with_core, setup=setup)
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


def get_environment_prefix() -> str:
    """Return the shell fragment that strips the Python environment before gdb.

    See :data:`PYTHON_ENVIRONMENT_VARIABLES` for why this is needed.

    Returns:
        Shell fragment ending in ``'; '``.
    """
    return "unset " + " ".join(PYTHON_ENVIRONMENT_VARIABLES) + "; "


def has_python_startup_failure(output: str) -> bool:
    """Return True if the gdb output shows its interpreter failed to start.

    Args:
        output: Captured gdb output.

    Returns:
        True if the failure signature is present.
    """
    return any(signature in (output or "") for signature in PYTHON_FAILURE_SIGNATURES)


def build_gdb_invocation(pid: int, commands: list, environment: str = "") -> str:
    """Return the gdb invocation attaching to a process and running commands.

    ``--nx`` keeps a stray ``.gdbinit`` out of the way, and debuginfod and the
    index cache are disabled before the inferior is loaded: a worker node has no
    route to a debuginfod server, so leaving it enabled risks a stall inside a
    step that is already the slowest one here.

    Args:
        pid: Process id to attach to.
        commands: gdb ``-ex`` options to run, in order.
        environment: Optional command prefix, e.g. :data:`CLEAN_ENVIRONMENT`.

    Returns:
        gdb command string.
    """
    options = [
        "--nx",
        f"-p {pid}",
        "-batch",
        "-iex 'set debuginfod enabled off'",
        "-iex 'set index-cache enabled off'",
        "-ex 'set confirm off'",
        "-ex 'set pagination off'",
    ]
    options += commands
    options += ["-ex detach", "-ex quit"]

    return f"{environment}gdb {' '.join(options)}"


def build_phase_command(invocation: str, output_path: str, header: str, setup: str = "") -> str:
    """Return the full shell command for one dump phase.

    Everything is redirected to *output_path* rather than read from the return
    value of :func:`pilot.util.container.execute`, which discards stdout on a
    timeout. The identity of the gdb that was actually used is recorded in the
    same file: whether the release gdb or the system one was picked up is the
    first question asked when a dump fails, and it is not answerable from any
    log the pilot currently writes.

    Args:
        invocation: gdb command as returned by :func:`build_gdb_invocation`.
        output_path: File the phase appends its output to.
        header: Single line marking the phase in the output file; must not
            contain a single quote.
        setup: Experiment setup prepended to the command.

    Returns:
        Full shell command string.
    """
    identity = "echo \"gdb: $(command -v gdb)\"; gdb --version 2>&1 | head -1"
    inner = f"echo '{header}'; date -u '+%Y-%m-%dT%H:%M:%SZ'; {identity}; {invocation}"

    return f'{setup}{get_environment_prefix()}{{ {inner}; }} >> "{output_path}" 2>&1'


def get_file_size(path: str) -> int:
    """Return the size of a file, or 0 if it does not exist.

    Args:
        path: File path.

    Returns:
        Size in bytes.
    """
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def read_gdb_output(path: str, offset: int = 0) -> str:
    """Return the gdb output written from *offset* onwards.

    Args:
        path: Output file path.
        offset: Byte offset to read from, so that one phase does not read the
            output of the previous one.

    Returns:
        File contents from *offset*, or an empty string.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as _file:
            _file.seek(offset)
            return _file.read()
    except OSError as exc:
        logger.warning(f"{LOG_PREFIX}: could not read {os.path.basename(path)}: {exc}")
        return ""


def create_scratch_directory(job: Any) -> str:
    """Return a working directory for the gdb phases, outside the job work directory.

    The experiment setup writes ``.asetup.save`` into its working directory, and
    the job work directory is exactly what the looping algorithm scans for
    payload activity, so running gdb there makes the pilot's own diagnostic look
    like the payload doing work. Keeping the setup out of that directory is
    preferred over filtering the file by name, since a multi-step transform
    rewrites ``.asetup.save`` legitimately between steps.

    Args:
        job: Job object.

    Returns:
        Path to a new directory, or an empty string if none could be created.
    """
    parent = os.path.dirname(os.path.abspath(job.workdir)) if job.workdir else ""
    for directory in (parent, None):
        try:
            return tempfile.mkdtemp(prefix="looping-dump-", dir=directory or None)
        except OSError as exc:
            logger.debug(f"{LOG_PREFIX}: could not create a scratch directory in {directory}: {exc}")

    logger.warning(f"{LOG_PREFIX}: no scratch directory could be created - gdb will run in the current directory")

    return ""


def remove_scratch_directory(path: str) -> None:
    """Remove the scratch directory used by the gdb phases.

    Args:
        path: Directory path; ignored when empty.
    """
    if path:
        rmtree(path, ignore_errors=True)


def log_gdb_output(output_path: str, label: str) -> None:
    """Echo a bounded amount of gdb output into the pilot log.

    The full output travels in the log tarball next to the core file; this only
    makes the common case greppable without unpacking it.

    Args:
        output_path: Output file path.
        label: Phase label used in the log message.
    """
    output = read_gdb_output(output_path).strip()
    if not output:
        logger.warning(f"{LOG_PREFIX}: {label}: gdb produced no output")
        return

    lines = output.split("\n")
    if len(lines) > MAX_GDB_LOG_LINES:
        remaining = len(lines) - MAX_GDB_LOG_LINES
        lines = lines[-MAX_GDB_LOG_LINES:]
        lines.insert(0, f"... ({remaining} earlier lines in {os.path.basename(output_path)})")

    text = "\n".join(lines)
    logger.info(f"{LOG_PREFIX}: {label}: gdb output:\n{text}")


def run_gdb_phase(cmd: str, output_path: str, timeout: int, scratch: str, label: str) -> tuple:
    """Run one gdb phase and return its exit code and its own output.

    Args:
        cmd: Full shell command as returned by :func:`build_phase_command`.
        output_path: File the phase appends its output to.
        timeout: Timeout in seconds.
        scratch: Working directory for the command.
        label: Phase label used in the log messages.

    Returns:
        Tuple of ``(exit_code, output)`` where *output* is what this phase
        appended, so that a phase never inspects the previous phase's result.
    """
    offset = get_file_size(output_path)

    # execute() is muted here on purpose: print_executable() redacts the value
    # following '-p', so it would log 'gdb -p ********' and mask the pid
    # everywhere else in the command as well
    logger.info(f"{LOG_PREFIX}: {label}: timeout={timeout} s, command: {cmd}")
    start = time.time()
    exit_code, _, stderr = execute(cmd, cwd=scratch or None, timeout=timeout, mute=True)
    elapsed = int(time.time() - start)

    output = read_gdb_output(output_path, offset=offset)
    if exit_code == errors.COMMANDTIMEDOUT:
        logger.warning(
            f"{LOG_PREFIX}: {label}: gdb timed out after {elapsed} s - whatever it had produced "
            f"by then was kept in {os.path.basename(output_path)}"
        )
    elif exit_code != 0:
        logger.warning(f"{LOG_PREFIX}: {label}: gdb failed with exit code {exit_code} after {elapsed} s")
        if stderr:
            logger.warning(f"{LOG_PREFIX}: {label}: {stderr}")
    else:
        logger.info(f"{LOG_PREFIX}: {label}: gdb finished in {elapsed} s")

    if has_python_startup_failure(output):
        logger.warning(
            f"{LOG_PREFIX}: {label}: gdb's own embedded interpreter failed to start "
            f"(the 'encodings' error refers to gdb's Python, not to the payload's) - "
            f"a PYTHONHOME/PYTHONPATH in the environment does not match the Python gdb is "
            f"linked against, and gdb aborted before running any command"
        )

    return exit_code, output


def run_core_dump_phase(pid: int, core_path: str, output_path: str, scratch: str, deadline: float) -> bool:
    """Run phase A: write the core file with a bare gdb.

    No experiment setup is used. ``generate-core-file`` only walks the
    inferior's mappings, so it needs no symbols at all, and the setup would add
    about a minute of asetup plus its own failure modes for nothing. The release
    gdb is only needed to *read* the core file, which happens offline.

    Args:
        pid: Process id to dump.
        core_path: Absolute path of the core file to write.
        output_path: File the phase appends its output to.
        scratch: Working directory for the command.
        deadline: Diagnostics deadline as a :func:`time.monotonic` value.

    Returns:
        True if gdb reported success.
    """
    timeout = min(get_core_dump_timeout(), get_remaining_budget(deadline))
    if timeout <= 0:
        logger.warning(f"{LOG_PREFIX}: phase A (core file): skipped - the diagnostics budget is spent")
        return False

    commands = [f"-ex 'generate-core-file {core_path}'"]
    cmd = build_phase_command(
        build_gdb_invocation(pid, commands),
        output_path,
        "=== phase A: core file (bare gdb, no release setup) ===",
    )
    exit_code, output = run_gdb_phase(cmd, output_path, timeout, scratch, "phase A (core file)")

    if exit_code != 0 and has_python_startup_failure(output):
        timeout = min(get_core_dump_timeout(), get_remaining_budget(deadline))
        if timeout > 0:
            logger.info(f"{LOG_PREFIX}: phase A (core file): retrying with a clean environment")
            cmd = build_phase_command(
                build_gdb_invocation(pid, commands, environment=CLEAN_ENVIRONMENT),
                output_path,
                "=== phase A (retry): core file (clean environment) ===",
            )
            exit_code, _ = run_gdb_phase(cmd, output_path, timeout, scratch, "phase A retry (core file)")

    if exit_code != 0:
        resume_process(pid)

    return exit_code == 0


def run_backtrace_phase(pid: int, output_path: str, scratch: str, setup: str, deadline: float) -> None:
    """Run phase B: collect the backtraces with the experiment setup.

    This is the expensive phase, since every mapped object's symbol table has to
    be read, several hundred of them over CVMFS for an athena process. It is
    therefore run last and is allowed to fail: the same stacks are in the core
    file that phase A already wrote.

    ``py-bt`` is requested because for a looping transform the Python stack
    usually identifies the algorithm directly; a gdb without the Python
    extension ignores it.

    Args:
        pid: Process id to attach to.
        output_path: File the phase appends its output to.
        scratch: Working directory for the command.
        setup: Experiment setup prepended to the command.
        deadline: Diagnostics deadline as a :func:`time.monotonic` value.
    """
    timeout = min(get_backtrace_timeout(), get_remaining_budget(deadline))
    if timeout <= 0:
        logger.warning(
            f"{LOG_PREFIX}: phase B (backtraces): skipped - the diagnostics budget is spent "
            f"(the stacks are in the core file)"
        )
        return

    commands = [
        f"-ex 'set backtrace limit {MAX_BACKTRACE_FRAMES}'",
        "-ex bt",
        "-ex 'thread apply all bt'",
        "-ex 'py-bt'",
    ]
    invocation = build_gdb_invocation(pid, commands)
    cmd = build_phase_command(
        invocation, output_path, "=== phase B: backtraces (release setup) ===", setup=setup
    )
    exit_code, output = run_gdb_phase(cmd, output_path, timeout, scratch, "phase B (backtraces)")

    if exit_code != 0 and has_python_startup_failure(output):
        timeout = min(get_backtrace_timeout(), get_remaining_budget(deadline))
        if timeout > 0:
            logger.info(
                f"{LOG_PREFIX}: phase B (backtraces): retrying without the release setup - "
                f"the frames will be less well resolved, but the thread and Python stacks are "
                f"worth more than nothing"
            )
            cmd = build_phase_command(
                invocation, output_path, "=== phase B (retry): backtraces (no release setup) ==="
            )
            exit_code, _ = run_gdb_phase(cmd, output_path, timeout, scratch, "phase B retry (backtraces)")

    if exit_code != 0:
        resume_process(pid)


def is_core_file_wanted_for(job: Any, pid: int) -> bool:
    """Return True if a core file should be written for the given process.

    Args:
        job: Job object.
        pid: Process id to dump.

    Returns:
        True if the core file is wanted and expected to fit.
    """
    if not is_core_dump_wanted():
        return False

    rss = get_rss(pid)
    maximum = get_core_dump_max_size()
    if rss > maximum:
        logger.warning(
            f"{LOG_PREFIX}: not dumping a core file for pid={pid} - its resident set "
            f"({rss // (1024 * 1024)} MB) exceeds the configured maximum "
            f"({maximum // (1024 * 1024)} MB); keeping the backtraces only. The core file is "
            f"kept in the log tarball, so this limit bounds the log file size as well"
        )
        return False

    return has_room_for_core(job.workdir, rss)


def report_core_file(core_path: str) -> None:
    """Log whether the core file was written, and how large it is.

    Args:
        core_path: Path to the core file.
    """
    name = os.path.basename(core_path)
    size = get_file_size(core_path)
    if size:
        logger.info(f"{LOG_PREFIX}: core file written: {name} ({size // (1024 * 1024)} MB)")
    else:
        logger.warning(f"{LOG_PREFIX}: no core file was produced at {core_path}")


def create_core_dump(job: Any) -> None:
    """Create a core dump of the looping payload and record how to analyse it.

    Targets the best candidate from :func:`select_dump_candidates` rather than
    an arbitrary descendant, and runs in two phases with independent timeouts
    under one overall budget: phase A writes the core file with a bare gdb,
    phase B collects the backtraces with the experiment setup and is allowed to
    fail. Both redirect to a file next to the core file, so a timeout keeps
    whatever was produced. The executable identity, the container the payload
    ran in and the release setup are recorded in the pilot log and in a
    companion file, so that the core file can still be opened long after the
    worker node is gone.

    Args:
        job: Job object. Must have ``pid`` and ``workdir`` set.
    """
    if not job.pid or not job.workdir:
        logger.warning(f"{LOG_PREFIX}: cannot create a core file since pid or workdir is unknown")
        return

    logger.info(summarise_snapshots())

    candidates = select_dump_candidates(job, label="before diagnostics")
    if not candidates:
        logger.warning(f"{LOG_PREFIX}: no dump candidate could be identified")
        return

    pid, cmdline = candidates[0]
    logger.info(f"{LOG_PREFIX}: selected pid={pid} for the core dump: {cmdline}")

    with_core = is_core_file_wanted_for(job, pid)
    core_path = os.path.join(job.workdir, f"core.{pid}")
    output_path = f"{core_path}{GDB_OUTPUT_SUFFIX}"
    setup = get_gdb_setup(job)

    # the analysis information must be collected while the process still exists,
    # since /proc/<pid>/exe, /proc/<pid>/cwd and /proc/<pid>/maps disappear with it
    store_core_analysis_info(job, pid, cmdline, core_path, with_core=with_core, setup=setup)

    budget = get_diagnostics_budget()
    deadline = time.monotonic() + budget
    logger.info(f"{LOG_PREFIX}: diagnostics budget for pid={pid}: {budget} s (core file={with_core})")

    scratch = create_scratch_directory(job)
    try:
        if with_core:
            run_core_dump_phase(pid, core_path, output_path, scratch, deadline)
            report_core_file(core_path)
        run_backtrace_phase(pid, output_path, scratch, setup, deadline)
    finally:
        remove_scratch_directory(scratch)

    log_gdb_output(output_path, f"pid={pid}")
