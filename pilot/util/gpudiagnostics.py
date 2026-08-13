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

This is a temporary, opt-in diagnostic intended to be removed once the root
cause is established. It is inert unless ``PILOT_GPU_DEBUG`` is set to a
truthy value and ``nvidia-smi`` is present.
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

# Environment variable used to opt in to the diagnostic (per queue/job).
GPU_DEBUG_ENV_VAR = "PILOT_GPU_DEBUG"

# Prefix on every log line so the diagnostic can be grepped out of a pilot log.
LOG_PREFIX = "gpu-debug"

# Values accepted as "enabled" for GPU_DEBUG_ENV_VAR.
TRUTHY_VALUES = ("1", "true", "yes", "on")

# Elapsed times (in seconds, relative to the first snapshot) at which snapshots
# are taken. The GPU-touching process may be forked well after prmon starts, so
# a single snapshot at payload start would risk a false negative.
SNAPSHOT_OFFSETS = (0, 300, 900)

# Timeout for any single nvidia-smi invocation, in seconds. 'pmon -c 1' samples
# for about a second, the others return immediately.
NVIDIA_SMI_TIMEOUT = 60

# Maximum number of ancestors reported for a GPU-active process that is visible
# but outside the monitored tree (guards against a pathological /proc).
MAX_ANCESTORS = 25

# Snapshot bookkeeping: time of the first snapshot and the number taken so far.
_snapshot_state = {"first_snapshot_time": None, "snapshots_taken": 0}


def reset_gpu_diagnostics_state() -> None:
    """Reset the snapshot bookkeeping.

    The snapshot schedule is module-level state (the diagnostic is called from
    the job monitoring loop and has no object of its own to live on), so it
    must be reset between tests.
    """
    _snapshot_state["first_snapshot_time"] = None
    _snapshot_state["snapshots_taken"] = 0


def is_gpu_diagnostics_enabled() -> bool:
    """Return True if the GPU diagnostic should run.

    The diagnostic is opt-in via the ``PILOT_GPU_DEBUG`` environment variable
    and additionally requires ``nvidia-smi`` to be present, which makes it a
    cheap no-op on non-GPU queues even if the variable is set globally.

    Returns:
        True if the diagnostic is enabled and usable on this worker node.
    """
    if os.environ.get(GPU_DEBUG_ENV_VAR, "").strip().lower() not in TRUTHY_VALUES:
        return False

    if not which("nvidia-smi"):
        logger.debug(f"{LOG_PREFIX}: {GPU_DEBUG_ENV_VAR} is set but nvidia-smi was not found - skipping")
        return False

    return True


def run_nvidia_smi(options: list) -> str:
    """Run nvidia-smi with the given options and return its stdout.

    Args:
        options: nvidia-smi options, e.g. ``['pmon', '-c', '1']``.

    Returns:
        Stripped stdout, or an empty string if the command failed or timed out.
    """
    try:
        result = subprocess.run(
            ["nvidia-smi"] + options,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            timeout=NVIDIA_SMI_TIMEOUT,
            universal_newlines=True,
        )
    except subprocess.CalledProcessError as exc:
        logger.warning(f"{LOG_PREFIX}: nvidia-smi {' '.join(options)} failed: {exc.stderr}")
        return ""
    except (subprocess.TimeoutExpired, OSError) as exc:
        logger.warning(f"{LOG_PREFIX}: nvidia-smi {' '.join(options)} could not be executed: {exc}")
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


def get_verdict(classifications: dict, gpu_pids: list) -> tuple:
    """Summarise what the classification of the GPU-active PIDs implies.

    Args:
        classifications: Mapping of GPU-active PID to classification.
        gpu_pids: The GPU-active PIDs reported by nvidia-smi.

    Returns:
        Tuple of (verdict text, is_problem) where is_problem is True when the
        result should be logged as a warning.
    """
    if not gpu_pids:
        return (
            "nvidia-smi reports no GPU-active compute processes at this moment - "
            "inconclusive (either the payload is not on the GPU yet, or per-process "
            "accounting is unavailable on this host - see the device info above)",
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


def log_gpu_pid_snapshot(job: object, snapshot: int) -> None:
    """Collect and log a single GPU/PID visibility snapshot.

    Args:
        job: Job object.
        snapshot: 1-based snapshot number.
    """
    total = len(SNAPSHOT_OFFSETS)
    logger.info(f"{LOG_PREFIX}: ---------- snapshot {snapshot}/{total} ----------")

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
    logger.info(f"{LOG_PREFIX}: pilot PID namespace={get_pid_namespace(os.getpid())}")

    monitored_pids = get_descendant_pids(monitored_pid)
    logger.info(
        f"{LOG_PREFIX}: monitored tree contains {len(monitored_pids)} pid(s): {sorted(monitored_pids)}"
    )

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

    verdict, is_problem = get_verdict(classifications, gpu_pids)
    if is_problem:
        logger.warning(f"{LOG_PREFIX}: verdict: {verdict}")
    else:
        logger.info(f"{LOG_PREFIX}: verdict: {verdict}")

    logger.info(f"{LOG_PREFIX}: nvidia-smi pmon sample:\n{get_gpu_pmon_sample() or '(no output)'}")

    tree = get_pilot_process_tree(monitored_pid)
    if tree:
        logger.info(f"{LOG_PREFIX}: monitored process tree:\n{tree}")


def is_snapshot_due(now: int | None = None) -> bool:
    """Return True if a snapshot is due, updating the snapshot bookkeeping.

    The first call schedules and claims snapshot one; later calls claim the
    next snapshot once the corresponding offset in :data:`SNAPSHOT_OFFSETS`
    has elapsed. Once all snapshots have been taken, this always returns
    False.

    Args:
        now: Current time in seconds since the epoch (defaults to now).

    Returns:
        True if the caller should take a snapshot.
    """
    if now is None:
        now = int(time.time())

    taken = _snapshot_state["snapshots_taken"]
    if taken >= len(SNAPSHOT_OFFSETS):
        return False

    if _snapshot_state["first_snapshot_time"] is None:
        _snapshot_state["first_snapshot_time"] = now
    elif now - _snapshot_state["first_snapshot_time"] < SNAPSHOT_OFFSETS[taken]:
        return False

    _snapshot_state["snapshots_taken"] = taken + 1

    return True


def report_gpu_pid_visibility(job: object) -> None:
    """Log a GPU/PID visibility snapshot if the diagnostic is enabled and due.

    This is the only entry point used by the pilot. It is a no-op unless
    ``PILOT_GPU_DEBUG`` is set and ``nvidia-smi`` is available, and it never
    raises - a diagnostic must not be able to disturb the job monitoring loop.

    Args:
        job: Job object.
    """
    try:
        if not is_gpu_diagnostics_enabled() or not is_snapshot_due():
            return

        log_gpu_pid_snapshot(job, _snapshot_state["snapshots_taken"])
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f"{LOG_PREFIX}: snapshot failed (ignored): {exc}")
