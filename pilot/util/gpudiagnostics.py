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

"""Opt-in diagnostics for zero GPU statistics in the memory monitor output.

Background: prmon reports all-zero GPU statistics
(``ngpus``/``gpufbmem``/``gpusmpct``/``gpumempct``) for jobs on some GPU
queues, while the payload's own monitoring shows real GPU activity. The pilot
has been cleared as the source of the zeroed values (it copies the prmon JSON
summary verbatim and never touches the GPU fields), so the remaining question
is whether prmon is *able* to attribute the GPU-active process to the process
tree it has been asked to monitor.

prmon matches GPU-active processes by PID against the descendants of the PID
it was given with ``--pid``. This module answers, from the pilot side, why
that match can fail, by comparing two host-side PID sets:

* set A: the monitored PID and all of its descendants, as the pilot sees them;
* set B: the PIDs of GPU-active compute processes as reported by
  ``nvidia-smi``.

Each PID in set B is then classified into one of three buckets, which
discriminate between the competing explanations:

* ``descendant`` - the GPU-active process *is* in the monitored tree, so prmon
  had everything it needed and the zeroes point at prmon itself;
* ``visible`` - the process exists in this pilot's ``/proc`` but is not a
  descendant of the monitored PID, i.e. it has escaped the monitored tree
  (``setsid``, re-parenting, a detached container supervisor, ...);
* ``invisible`` - the process does not exist in this pilot's ``/proc`` at all,
  which means the pilot (and hence prmon) is in a different PID namespace than
  the PIDs reported by ``nvidia-smi``, and no PID-based match can ever succeed.

No ``nsenter`` is needed for this: the NVIDIA driver operates at host kernel
level, so ``nvidia-smi`` always reports the real host PID of a GPU-active
process regardless of any PID-namespace isolation on the payload side.

Two further one-shot queries are made because they can explain zeroes without
any PID problem at all: if MIG mode is enabled or GPU accounting is disabled,
per-process attribution is unavailable no matter which process tree is
monitored; and a raw ``nvidia-smi pmon`` sample shows whether the utilisation
percentages that end up as ``gpusmpct``/``gpumempct`` are reported by
``nvidia-smi`` in the first place.

Sampling is driven by device activity rather than by a fixed clock. A payload
with a setup phase (installing packages, building a benchmark suite, staging a
model) can be minutes away from its first kernel launch, so a purely
time-scheduled burst of snapshots early in the payload is the one strategy that
can never observe the signature this module exists to catch - a busy device with
nothing enumerated. Two scheduled snapshots therefore establish the baseline
(PID namespace, ``NSpid``, process tree) even for a job that never touches the
GPU, while a cheap device-level utilisation poll on every monitoring iteration
triggers the remaining snapshots at the moment the device actually becomes
active. The poll also logs idle/busy transitions and the peak activity seen so
far, so a job that ends without warning still leaves behind the answer to
"was this GPU ever used at all" - the absence of any transition line means it
was not.

This is a temporary diagnostic intended to be removed once the root cause is
established. It activates by itself on GPU queues (any queue name containing
``GPU``) so that no configuration or pilot argument change is needed to collect
data, and is inert everywhere else unless ``PILOT_GPU_DEBUG`` is set - which
covers a GPU queue whose name does not carry the marker. Either way
``nvidia-smi`` must be present. The gating decision itself is logged once per
job, so a disabled diagnostic can be told apart from one that was never
reached.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import time
from shutil import which

from pilot.util.psutils import (
    _is_psutil_available,
    get_child_processes,
    get_parent_pid,
    get_pilot_process_tree,
)

logger = logging.getLogger(__name__)

# Environment variable used to force the diagnostic on, for a GPU queue whose
# name does not contain GPU_QUEUE_MARKER (e.g. CERN-PROD).
GPU_DEBUG_ENV_VAR = "PILOT_GPU_DEBUG"

# Substring in the PanDA queue name that activates the diagnostic. Matched
# case-insensitively against the queue name, so SLAC_GPU, CERN-GPU,
# UKI-LT2-QMUL_GPU etc. all activate it without any configuration change.
GPU_QUEUE_MARKER = "GPU"

# Prefix on every log line so the diagnostic can be grepped out of a pilot log.
LOG_PREFIX = "gpu-debug"

# Values accepted as "enabled" for GPU_DEBUG_ENV_VAR.
TRUTHY_VALUES = ("1", "true", "yes", "on")

# Reason fragment marking the one disabled case that deserves a warning: the
# diagnostic was activated but the node cannot support it.
GPU_DEBUG_UNUSABLE = "nvidia-smi was not found"

# Elapsed times (in seconds, relative to the first snapshot) at which the
# baseline snapshots are taken. These exist to record the PID namespace, NSpid
# and process tree even for a payload that never uses the GPU, so they are kept
# early and few: the snapshots that can actually observe the failure signature
# are triggered by device activity instead (see ACTIVITY_SNAPSHOT_BUDGET).
SNAPSHOT_OFFSETS = (0, 30)

# Maximum number of additional snapshots taken because the device was found to
# be active. Bounded so that a long GPU job cannot fill its pilot log: the
# signature is either present in the first few active snapshots or it is not.
ACTIVITY_SNAPSHOT_BUDGET = 3

# Minimum interval (in seconds) between two activity-triggered snapshots, so
# that a sustained workload spreads its budget over the run instead of spending
# it on three consecutive monitoring iterations.
ACTIVITY_SNAPSHOT_SPACING = 30

# Thresholds above which a device counts as active. A bare non-zero test is not
# usable as a trigger: with persistence mode or MIG enabled, memory.used is not
# reliably 0 MiB on an idle device, which would report activity (and spend the
# whole snapshot budget) before the payload has started. utilization.gpu and
# utilization.memory are percentages of sample-period occupancy, where any
# non-zero value is real work; memory.used needs a floor above driver overhead.
BUSY_UTILIZATION_PERCENT = 1
BUSY_MEMORY_MIB = 64

# Labels distinguishing why a snapshot was taken, used in its header line.
SCHEDULED_LABEL = "scheduled"
ACTIVITY_LABEL = "activity-triggered"

# Timeout for any single nvidia-smi invocation, in seconds. 'pmon -c 1' samples
# for about a second, the others return immediately.
NVIDIA_SMI_TIMEOUT = 60

# Conventional inode of the initial (host) PID namespace on Linux. A pilot in
# any other PID namespace cannot match the host PIDs that the NVIDIA driver
# reports, which is the failure mode this diagnostic exists to detect.
INITIAL_PID_NAMESPACE = "pid:[4026531836]"

# Maximum number of ancestors reported for a GPU-active process that is visible
# but outside the monitored tree (guards against a pathological /proc).
MAX_ANCESTORS = 25

# Absolute paths tried when nvidia-smi is not on PATH (the pilot's PATH is set
# by the wrapper and does not always include it, even on GPU worker nodes).
NVIDIA_SMI_FALLBACK_PATHS = (
    "/usr/bin/nvidia-smi",
    "/bin/nvidia-smi",
    "/usr/local/nvidia/bin/nvidia-smi",
)

# Snapshot bookkeeping. The schedule is per job: the diagnostic is called from
# the job monitoring loop, which has no per-job object to hang state on, and a
# multijob pilot must not have its second and later jobs silently skipped
# because the first job used up the snapshot budget.
_snapshot_state = {
    "jobid": None,
    "first_snapshot_time": None,
    "snapshots_taken": 0,
    "total_snapshots": 0,
}

# Device activity bookkeeping, also per job for the same reason. 'active' is the
# last observed state and is None before the first poll, so that the very first
# observation of an active device registers as a transition rather than being
# mistaken for the initial condition.
_activity_state = {
    "jobid": None,
    "active": None,
    "peak_gpu_percent": 0,
    "peak_memory_mib": 0,
    "activity_snapshots": 0,
    "last_activity_time": None,
}

# Whether the one-time announcement of the gating decision has been logged for
# the current job.
_announced_state = {"jobid": None}

# nvidia-smi option signatures already warned about for the current job. The
# activity poll runs on every monitoring iteration, so an unhealthy nvidia-smi
# would otherwise emit the same warning for the whole job.
_warned_commands: set = set()


def reset_gpu_diagnostics_state() -> None:
    """Reset the snapshot, activity and announcement bookkeeping.

    Called automatically when a new job is seen, and by the tests.
    """
    _snapshot_state["jobid"] = None
    _snapshot_state["first_snapshot_time"] = None
    _snapshot_state["snapshots_taken"] = 0
    _snapshot_state["total_snapshots"] = 0
    _activity_state["jobid"] = None
    _activity_state["active"] = None
    _activity_state["peak_gpu_percent"] = 0
    _activity_state["peak_memory_mib"] = 0
    _activity_state["activity_snapshots"] = 0
    _activity_state["last_activity_time"] = None
    _announced_state["jobid"] = None
    _warned_commands.clear()


def announce_once(jobid: str, enabled: bool, reason: str, announce: bool) -> None:
    """Log the gating decision once per job.

    Without this, a disabled diagnostic is completely silent and cannot be
    told apart from one that was never reached - which is exactly the
    ambiguity seen when ``PILOT_GPU_DEBUG`` does not make it into the pilot's
    own environment. One line per job is negligible in a pilot log, but the
    line is suppressed altogether on nodes without a GPU, where it would be
    pure noise on every job at every queue.

    Args:
        jobid: PanDA job id (used to announce once per job).
        enabled: Whether the diagnostic is enabled.
        reason: Short explanation of the decision.
        announce: Whether the decision is worth logging at all.
    """
    if not announce or _announced_state["jobid"] == jobid:
        return

    _announced_state["jobid"] = jobid
    if enabled:
        logger.info(f"{LOG_PREFIX}: diagnostic enabled ({reason})")
    elif GPU_DEBUG_UNUSABLE in reason:
        # the diagnostic was activated but the node cannot support it - worth a warning
        logger.warning(f"{LOG_PREFIX}: diagnostic disabled ({reason})")
    else:
        logger.info(f"{LOG_PREFIX}: diagnostic disabled ({reason})")


def find_nvidia_smi() -> str:
    """Locate the nvidia-smi executable.

    Falls back to a small list of well-known absolute paths when nvidia-smi is
    not on PATH, since the pilot's PATH is set by the wrapper and does not
    always include it.

    Returns:
        Path to nvidia-smi, or an empty string if it could not be found.
    """
    path = which("nvidia-smi")
    if path:
        return path

    for candidate in NVIDIA_SMI_FALLBACK_PATHS:
        if os.path.exists(candidate):
            return candidate

    return ""


def is_gpu_diagnostics_enabled(queue: str) -> tuple:
    """Return whether the GPU diagnostic should run, why, and whether to say so.

    The diagnostic activates by itself on GPU queues - any queue whose name
    contains :data:`GPU_QUEUE_MARKER` - so that no configuration or pilot
    argument change is needed to collect data. ``PILOT_GPU_DEBUG`` remains
    available to force it on for a GPU queue whose name does not carry the
    marker. Either way ``nvidia-smi`` is required, which makes the diagnostic a
    cheap no-op if a non-GPU node is ever picked up by a GPU queue.

    Args:
        queue: PanDA queue name.

    Returns:
        Tuple of (enabled, reason, announce), where reason is a short
        explanation suitable for logging and announce is False for the
        uninteresting case of a non-GPU queue on a node without a GPU.
    """
    forced = os.environ.get(GPU_DEBUG_ENV_VAR, "").strip().lower() in TRUTHY_VALUES
    is_gpu_queue = GPU_QUEUE_MARKER in (queue or "").upper()
    has_nvidia_smi = bool(find_nvidia_smi())

    if not is_gpu_queue and not forced:
        # only worth mentioning on a node that could have run the diagnostic, which
        # is also the interesting case of a GPU node behind a queue not named *GPU*
        return (
            False,
            f"queue name '{queue}' does not contain '{GPU_QUEUE_MARKER}' and "
            f"{GPU_DEBUG_ENV_VAR} is not set",
            has_nvidia_smi,
        )

    trigger = f"{GPU_DEBUG_ENV_VAR} is set" if forced and not is_gpu_queue else f"GPU queue '{queue}'"
    if not has_nvidia_smi:
        return False, f"{trigger} but {GPU_DEBUG_UNUSABLE} on this node", True

    offsets = ", ".join(str(offset) for offset in SNAPSHOT_OFFSETS)
    return (
        True,
        f"{trigger}, baseline snapshots at {offsets} s after the first monitoring iteration "
        f"plus up to {ACTIVITY_SNAPSHOT_BUDGET} more, at least {ACTIVITY_SNAPSHOT_SPACING} s "
        f"apart, while the device is active",
        True,
    )


def warn_once(key: str, message: str) -> None:
    """Log a warning the first time it is seen for the current job.

    The activity poll runs on every monitoring iteration, so a persistently
    failing nvidia-smi would otherwise repeat the same warning for the whole
    job. One line per distinct failure is enough to diagnose it.

    Args:
        key: Identifier deduplicating the warning.
        message: Warning text.
    """
    if key in _warned_commands:
        return

    _warned_commands.add(key)
    logger.warning(message)


def run_nvidia_smi(options: list) -> str:
    """Run nvidia-smi with the given options and return its stdout.

    Args:
        options: nvidia-smi options, e.g. ``['pmon', '-c', '1']``.

    Returns:
        Stripped stdout, or an empty string if the command failed or timed out.
    """
    executable = find_nvidia_smi()
    if not executable:
        return ""

    try:
        result = subprocess.run(
            [executable] + options,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            timeout=NVIDIA_SMI_TIMEOUT,
            universal_newlines=True,
        )
    except subprocess.CalledProcessError as exc:
        warn_once(
            f"failed:{options}",
            f"{LOG_PREFIX}: nvidia-smi {' '.join(options)} failed: {exc.stderr}",
        )
        return ""
    except (subprocess.TimeoutExpired, OSError) as exc:
        warn_once(
            f"unexecutable:{options}",
            f"{LOG_PREFIX}: nvidia-smi {' '.join(options)} could not be executed: {exc}",
        )
        return ""

    return result.stdout.strip()


def get_gpu_device_info() -> str:
    """Return per-device GPU mode information relevant to per-process accounting.

    MIG mode and accounting mode are queried because either can prevent
    per-process GPU attribution entirely, independently of any PID visibility
    problem.

    Returns:
        Raw csv output (one line per device), or an empty string on failure.
    """
    return run_nvidia_smi(
        [
            "--query-gpu=index,name,driver_version,mig.mode.current,accounting.mode,persistence_mode",
            "--format=csv,noheader",
        ]
    )


def get_gpu_utilization() -> str:
    """Return device-level GPU utilisation.

    This is the control for the per-process query: device-level counters are
    readable from inside a PID namespace, whereas per-process enumeration is
    not. A busy device with no compute processes listed therefore means
    enumeration is unavailable, not that the payload is off the GPU - which is
    the difference between an inconclusive snapshot and a conclusive one.

    Returns:
        Raw csv output (one line per device), or an empty string on failure.
    """
    return run_nvidia_smi(
        [
            "--query-gpu=index,utilization.gpu,utilization.memory,memory.used",
            "--format=csv,noheader",
        ]
    )


def parse_gpu_utilization(output: str) -> list:
    """Parse ``--query-gpu`` utilisation csv output into numeric tuples.

    Fields that nvidia-smi reports as unsupported (``[N/A]``) are read as zero:
    an unreadable counter must not be able to claim the device is active.

    Args:
        output: Raw csv output from :func:`get_gpu_utilization`.

    Returns:
        List of (index, gpu percent, memory percent, memory MiB) tuples, one per
        device, skipping any line that does not carry all four fields.
    """
    devices = []
    for line in output.split("\n"):
        fields = [field.strip() for field in line.split(",")]
        if len(fields) < 4:
            continue

        values = []
        for field in fields[:4]:
            match = re.match(r"\d+", field)
            values.append(int(match.group()) if match else 0)
        devices.append(tuple(values))

    return devices


def is_gpu_busy(output: str) -> bool:
    """Return True if any device is doing work, by threshold rather than by zero.

    Args:
        output: Raw csv output from :func:`get_gpu_utilization`.

    Returns:
        True if at least one device exceeds :data:`BUSY_UTILIZATION_PERCENT` on
        either utilisation counter, or :data:`BUSY_MEMORY_MIB` of used memory.
    """
    for _, gpu_percent, memory_percent, memory_mib in parse_gpu_utilization(output):
        busy = (
            gpu_percent >= BUSY_UTILIZATION_PERCENT or
            memory_percent >= BUSY_UTILIZATION_PERCENT or
            memory_mib >= BUSY_MEMORY_MIB
        )
        if busy:
            return True

    return False


def get_peak_activity(output: str) -> tuple:
    """Return the highest utilisation and used memory across all devices.

    Args:
        output: Raw csv output from :func:`get_gpu_utilization`.

    Returns:
        Tuple of (max gpu percent, max memory MiB); (0, 0) for empty output.
    """
    devices = parse_gpu_utilization(output)
    if not devices:
        return 0, 0

    return (
        max(device[1] for device in devices),
        max(device[3] for device in devices),
    )


def describe_activity_peak() -> str:
    """Describe the highest GPU activity seen so far for the current job.

    Returns:
        Short human-readable summary of the peak counters.
    """
    return (
        f"gpu {_activity_state['peak_gpu_percent']} %, "
        f"memory {_activity_state['peak_memory_mib']} MiB"
    )


def get_gpu_compute_apps() -> str:
    """Return the list of GPU-active compute processes reported by nvidia-smi.

    Returns:
        Raw csv output (one line per compute process), or an empty string if
        there are none or the query failed.
    """
    return run_nvidia_smi(
        ["--query-compute-apps=pid,process_name,used_gpu_memory", "--format=csv,noheader"]
    )


def get_gpu_pmon_sample() -> str:
    """Return a single ``nvidia-smi pmon`` sample.

    ``pmon`` is the source of the per-process utilisation percentages that end
    up as ``gpusmpct``/``gpumempct``, so its raw output shows whether those
    numbers exist at all on this host.

    Returns:
        Raw pmon output, or an empty string on failure.
    """
    return run_nvidia_smi(["pmon", "-c", "1"])


def extract_pids_from_compute_apps(output: str) -> list:
    """Extract the PIDs from ``--query-compute-apps`` csv output.

    Args:
        output: Raw csv output from :func:`get_gpu_compute_apps`.

    Returns:
        List of PIDs (ints), in the order they appear.
    """
    pids = []
    for line in output.split("\n"):
        field = line.split(",")[0].strip()
        try:
            pids.append(int(field))
        except ValueError:
            if field:
                logger.debug(f"{LOG_PREFIX}: ignoring unparsable compute-apps line: {line}")

    return pids


def get_monitored_pid(job: object) -> tuple:
    """Return the PID that the memory monitor has actually been asked to watch.

    The PID handed to prmon is resolved when the memory monitor command is
    built (it can differ from ``job.pid`` when the payload runs in a
    container) and is not stored on the job object, but the full command is
    kept in ``job.utilities`` and contains ``--pid <pid>``. That is therefore
    the authoritative source; ``job.pid`` is used as a fallback so that the
    diagnostic still reports something before the memory monitor has started.

    Args:
        job: Job object.

    Returns:
        Tuple of (pid, source description). The pid is 0 if no PID could be
        determined at all.
    """
    utilities = getattr(job, "utilities", None) or {}
    command = ""
    for value in utilities.values():
        # the stored value is [process handle, launch count, full command]
        if isinstance(value, list) and len(value) > 2 and isinstance(value[2], str):
            if "--pid" in value[2]:
                command = value[2]
                break

    if command:
        match = re.search(r"--pid\s+(\d+)", command)
        if match:
            return int(match.group(1)), "memory monitor command"

    return getattr(job, "pid", 0) or 0, "job.pid (memory monitor not started yet?)"


def get_pid_namespace(pid: int) -> str:
    """Return the PID namespace identifier for the given process.

    Args:
        pid: Process ID.

    Returns:
        The namespace identifier (e.g. ``pid:[4026531836]``), or a short
        notice if it could not be read.
    """
    try:
        return os.readlink(f"/proc/{pid}/ns/pid")
    except OSError as exc:
        return f"(unavailable: {exc})"


def get_ancestors(pid: int) -> list:
    """Return the ancestor PID chain for the given process, up to PID 1.

    Args:
        pid: Process ID to walk upwards from.

    Returns:
        List of ancestor PIDs, closest ancestor first.
    """
    ancestors = []
    current = pid
    while len(ancestors) < MAX_ANCESTORS:
        parent = get_parent_pid(current)
        if not parent or parent in ancestors:
            break
        ancestors.append(parent)
        if parent == 1:
            break
        current = parent

    return ancestors


def get_descendant_pids(pid: int) -> set:
    """Return the given PID together with all of its descendants.

    Args:
        pid: Root PID of the process tree (the PID given to the memory monitor).

    Returns:
        Set of PIDs, including the root PID itself.
    """
    pids = {pid}
    for entry in get_child_processes(pid):
        try:
            pids.add(entry[0])
        except (IndexError, TypeError):
            continue

    return pids


def classify_gpu_pid(pid: int, monitored_pids: set) -> str:
    """Classify a GPU-active PID with respect to the monitored process tree.

    Args:
        pid: PID of a GPU-active process, as reported by nvidia-smi.
        monitored_pids: The monitored PID and all of its descendants.

    Returns:
        One of ``'descendant'``, ``'visible'`` or ``'invisible'``.
    """
    if pid in monitored_pids:
        return "descendant"

    if os.path.exists(f"/proc/{pid}"):
        return "visible"

    return "invisible"


def get_verdict(classifications: dict, gpu_pids: list, gpu_busy: bool = False) -> tuple:
    """Summarise what the classification of the GPU-active PIDs implies.

    Args:
        classifications: Mapping of GPU-active PID to classification.
        gpu_pids: The GPU-active PIDs reported by nvidia-smi.
        gpu_busy: Whether device-level counters show the GPU doing work.

    Returns:
        Tuple of (verdict text, is_problem) where is_problem is True when the
        result should be logged as a warning.
    """
    if not gpu_pids and gpu_busy:
        return (
            "the GPU is busy at device level but nvidia-smi reports no compute processes "
            "at all - per-process enumeration is unavailable in this context, so prmon has "
            "nothing it can attribute to the monitored tree and reports ngpus=0 no matter "
            "which process tree it is given (check the pilot PID namespace above)",
            True,
        )

    if not gpu_pids:
        return (
            "nvidia-smi reports no GPU-active compute processes and the device is idle - "
            "inconclusive, the payload is not on the GPU at this point; a snapshot labelled "
            f"'{ACTIVITY_LABEL}' follows once it is, and if no 'device activity' line ever "
            "reports busy then this job never used the GPU at all",
            False,
        )

    categories = set(classifications.values())

    if "descendant" in categories:
        return (
            "at least one GPU-active process IS in the monitored process tree - prmon had "
            "the information it needed, so zero GPU statistics cannot be explained by PID "
            "visibility on this host",
            False,
        )

    if "visible" in categories:
        return (
            "GPU-active process(es) are visible to the pilot but are NOT in the monitored "
            "process tree - the payload's GPU process has escaped the tree that prmon was "
            "given (re-parenting, setsid, or a detached container supervisor); prmon cannot "
            "attribute it",
            True,
        )

    return (
        "GPU-active process(es) do not exist in this pilot's /proc at all - the pilot (and "
        "therefore prmon) is in a different PID namespace than the PIDs reported by "
        "nvidia-smi, so no PID-based match can ever succeed",
        True,
    )


def get_nspid(pid: int) -> str:
    """Return the ``NSpid`` field from ``/proc/<pid>/status``.

    ``NSpid`` lists the PID of a process at each PID-namespace level it belongs
    to, leftmost being the level of whichever namespace mounted this procfs.
    More than one value therefore proves namespace nesting outright, and the
    leftmost value is the outer (host) PID - which is what the NVIDIA driver
    reports, and hence exactly the translation prmon would need in order to
    match GPU-active processes from inside a namespace.

    Args:
        pid: Process ID.

    Returns:
        The NSpid values, or a short notice if unavailable.
    """
    try:
        with open(f"/proc/{pid}/status", encoding="utf-8") as status:
            for line in status:
                if line.startswith("NSpid:"):
                    values = line.split()[1:]
                    if len(values) > 1:
                        return (
                            f"{' '.join(values)} (nested: outer/host pid {values[0]}, "
                            f"innermost {values[-1]})"
                        )
                    return f"{' '.join(values)} (single level as seen from this procfs)"
    except OSError as exc:
        return f"(unavailable: {exc})"

    return "(not reported by this kernel)"


def describe_pid_namespace(pid: int) -> str:
    """Describe the PID namespace of the given process.

    The bare inode is the single most decisive datum in this diagnostic, so it
    is reported together with what it means rather than left to be looked up.

    Args:
        pid: Process ID.

    Returns:
        The namespace identifier followed by its interpretation.
    """
    namespace = get_pid_namespace(pid)
    if namespace == INITIAL_PID_NAMESPACE:
        note = "initial/host namespace, host PIDs are directly matchable"
    elif namespace.startswith("pid:"):
        note = (
            "NOT the initial/host namespace - the PIDs reported by the NVIDIA driver are "
            "host PIDs and cannot be matched from here"
        )
    else:
        note = "could not be determined"

    return f"{namespace} ({note})"


def log_gpu_pid_snapshot(
    job: object,
    snapshot: int,
    label: str = SCHEDULED_LABEL,
    utilization: str | None = None,
) -> None:
    """Collect and log a single GPU/PID visibility snapshot.

    Args:
        job: Job object.
        snapshot: 1-based snapshot number, counted across both snapshot kinds.
        label: Why this snapshot was taken (:data:`SCHEDULED_LABEL` or
            :data:`ACTIVITY_LABEL`).
        utilization: Utilisation sample already taken by the caller, reused so
            that the activity poll and the snapshot do not query the device
            twice within the same monitoring iteration. Queried here if absent.
    """
    logger.info(f"{LOG_PREFIX}: ---------- snapshot {snapshot} ({label}) ----------")

    if snapshot == 1:
        # device mode does not change during a job, so query it only once
        device_info = get_gpu_device_info()
        logger.info(
            f"{LOG_PREFIX}: GPU devices (index, name, driver, mig mode, accounting, persistence):"
            f"\n{device_info or '(no output)'}"
        )
        if not _is_psutil_available:
            logger.warning(
                f"{LOG_PREFIX}: psutil is not available - only direct children can be discovered, "
                f"so a 'not in the monitored tree' result may be a false negative"
            )

    monitored_pid, pid_source = get_monitored_pid(job)
    if not monitored_pid:
        logger.warning(f"{LOG_PREFIX}: no PID available to inspect - skipping snapshot")
        return

    logger.info(
        f"{LOG_PREFIX}: monitored pid={monitored_pid} (from {pid_source}), job.pid="
        f"{getattr(job, 'pid', None)}, pilot pid={os.getpid()}"
    )
    logger.info(f"{LOG_PREFIX}: pilot PID namespace={describe_pid_namespace(os.getpid())}")
    logger.info(
        f"{LOG_PREFIX}: NSpid pilot={get_nspid(os.getpid())}, "
        f"monitored={get_nspid(monitored_pid)}"
    )

    monitored_pids = get_descendant_pids(monitored_pid)
    logger.info(
        f"{LOG_PREFIX}: monitored tree contains {len(monitored_pids)} pid(s): {sorted(monitored_pids)}"
    )

    if utilization is None:
        utilization = get_gpu_utilization()
    logger.info(
        f"{LOG_PREFIX}: GPU utilisation (index, gpu %, memory %, memory used):"
        f"\n{utilization or '(no output)'}"
    )
    logger.info(f"{LOG_PREFIX}: peak activity so far this job: {describe_activity_peak()}")

    compute_apps = get_gpu_compute_apps()
    logger.info(
        f"{LOG_PREFIX}: nvidia-smi compute apps (pid, process name, used GPU memory):"
        f"\n{compute_apps or '(none reported)'}"
    )

    gpu_pids = extract_pids_from_compute_apps(compute_apps)
    classifications = {pid: classify_gpu_pid(pid, monitored_pids) for pid in gpu_pids}
    for pid, classification in classifications.items():
        if classification == "descendant":
            logger.info(f"{LOG_PREFIX}: GPU pid {pid}: IN the monitored process tree")
        elif classification == "visible":
            logger.info(
                f"{LOG_PREFIX}: GPU pid {pid}: visible but NOT in the monitored tree "
                f"(namespace={get_pid_namespace(pid)}, ancestors={get_ancestors(pid)})"
            )
        else:
            logger.info(
                f"{LOG_PREFIX}: GPU pid {pid}: NOT visible in this pilot's /proc "
                f"(host pid outside the pilot's PID namespace)"
            )

    verdict, is_problem = get_verdict(classifications, gpu_pids, is_gpu_busy(utilization))
    if is_problem:
        logger.warning(f"{LOG_PREFIX}: verdict: {verdict}")
    else:
        logger.info(f"{LOG_PREFIX}: verdict: {verdict}")

    logger.info(f"{LOG_PREFIX}: nvidia-smi pmon sample:\n{get_gpu_pmon_sample() or '(no output)'}")

    tree = get_pilot_process_tree(monitored_pid)
    if tree:
        logger.info(f"{LOG_PREFIX}: monitored process tree:\n{tree}")


def is_snapshot_due(jobid: str, now: int | None = None) -> bool:
    """Return True if a snapshot is due, updating the snapshot bookkeeping.

    The schedule is per job: seeing a new job id resets it, so that every job
    in a multijob pilot gets its own set of snapshots. The first call for a job
    schedules and claims snapshot one; later calls claim the next snapshot once
    the corresponding offset in :data:`SNAPSHOT_OFFSETS` has elapsed. Once all
    snapshots have been taken for that job, this always returns False.

    Args:
        jobid: PanDA job id.
        now: Current time in seconds since the epoch (defaults to now).

    Returns:
        True if the caller should take a snapshot.
    """
    if now is None:
        now = int(time.time())

    if _snapshot_state["jobid"] != jobid:
        _snapshot_state["jobid"] = jobid
        _snapshot_state["first_snapshot_time"] = None
        _snapshot_state["snapshots_taken"] = 0
        _snapshot_state["total_snapshots"] = 0

    taken = _snapshot_state["snapshots_taken"]
    if taken >= len(SNAPSHOT_OFFSETS):
        return False

    if _snapshot_state["first_snapshot_time"] is None:
        _snapshot_state["first_snapshot_time"] = now
    elif now - _snapshot_state["first_snapshot_time"] < SNAPSHOT_OFFSETS[taken]:
        return False

    _snapshot_state["snapshots_taken"] = taken + 1

    return True


def track_gpu_activity(jobid: str, utilization: str) -> bool:
    """Record device activity, logging every idle/busy transition.

    This runs on every monitoring iteration and is the diagnostic's only
    whole-job observation: a job that ends between snapshots still leaves a
    record of whether its GPU was ever used, and to what extent. Only
    transitions are logged, so a whole job costs a handful of lines.

    Args:
        jobid: PanDA job id (a new id resets the tracking).
        utilization: Raw csv output from :func:`get_gpu_utilization`.

    Returns:
        True if the device is active in this sample.
    """
    if _activity_state["jobid"] != jobid:
        _activity_state["jobid"] = jobid
        _activity_state["active"] = None
        _activity_state["peak_gpu_percent"] = 0
        _activity_state["peak_memory_mib"] = 0
        _activity_state["activity_snapshots"] = 0
        _activity_state["last_activity_time"] = None
        _warned_commands.clear()

    active = is_gpu_busy(utilization)
    gpu_percent, memory_mib = get_peak_activity(utilization)
    _activity_state["peak_gpu_percent"] = max(_activity_state["peak_gpu_percent"], gpu_percent)
    _activity_state["peak_memory_mib"] = max(_activity_state["peak_memory_mib"], memory_mib)

    if active != _activity_state["active"]:
        previous = _activity_state["active"]
        _activity_state["active"] = active
        transition = "idle -> busy" if active else "busy -> idle"
        if previous is None:
            transition = "busy at first observation" if active else "idle at first observation"
        logger.info(
            f"{LOG_PREFIX}: device activity {transition} (now gpu {gpu_percent} %, "
            f"memory {memory_mib} MiB; peak this job: {describe_activity_peak()})"
        )

    return active


def is_activity_snapshot_due(active: bool, now: int | None = None) -> bool:
    """Return True if an activity-triggered snapshot is due, updating bookkeeping.

    The trigger is the device *being* active rather than the idle -> busy edge
    alone: a single edge would yield a single snapshot, whereas the point of the
    budget is to sample a live workload more than once, in case enumeration only
    becomes possible (or only fails) after the payload has settled.

    Args:
        active: Whether the device is active in the current sample.
        now: Current time in seconds since the epoch (defaults to now).

    Returns:
        True if the caller should take a snapshot.
    """
    if not active or _activity_state["activity_snapshots"] >= ACTIVITY_SNAPSHOT_BUDGET:
        return False

    if now is None:
        now = int(time.time())

    last = _activity_state["last_activity_time"]
    if last is not None and now - last < ACTIVITY_SNAPSHOT_SPACING:
        return False

    _activity_state["activity_snapshots"] += 1
    _activity_state["last_activity_time"] = now

    return True


def report_gpu_pid_visibility(job: object, queue: str) -> None:
    """Log a GPU/PID visibility snapshot if the diagnostic is enabled and due.

    This is the only entry point used by the pilot. It is a no-op unless the
    queue is a GPU queue (or ``PILOT_GPU_DEBUG`` is set) and ``nvidia-smi`` is
    available, and it never raises - a diagnostic must not be able to disturb
    the job monitoring loop.

    Args:
        job: Job object.
        queue: PanDA queue name.
    """
    try:
        jobid = str(getattr(job, "jobid", "") or "")
        enabled, reason, announce = is_gpu_diagnostics_enabled(queue)
        announce_once(jobid, enabled, reason, announce)
        if not enabled:
            return

        # the one query made on every monitoring iteration (tens of milliseconds);
        # its result is reused by any snapshot taken below
        utilization = get_gpu_utilization()
        active = track_gpu_activity(jobid, utilization)

        if is_snapshot_due(jobid):
            label = SCHEDULED_LABEL
        elif is_activity_snapshot_due(active):
            label = ACTIVITY_LABEL
        else:
            return

        _snapshot_state["total_snapshots"] += 1
        log_gpu_pid_snapshot(job, _snapshot_state["total_snapshots"], label, utilization)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: snapshot failed (ignored): {exc}")
