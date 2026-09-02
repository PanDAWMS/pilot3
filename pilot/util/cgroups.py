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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025-26

"""Code for interacting with cgroups."""

from __future__ import annotations

import logging
import os
try:
    import psutil
except ImportError:
    print('FAILED; psutil module could not be imported')
    _is_psutil_available = False
else:
    _is_psutil_available = True
import subprocess
from pathlib import Path
from typing import Optional, Union

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache

errors = ErrorCodes()
logger = logging.getLogger(__name__)
pilot_cache = get_pilot_cache()
CGROUP_PATH = "/sys/fs/cgroup"
PROC_CGROUP_PATH = "/proc/self/cgroup"


def get_cgroup_version() -> str:
    """Determine if the system is using cgroups version 1 or 2.

    Returns:
        ``'v1'`` if cgroups version 1, ``'v2'`` if version 2, or None if unable to determine.
    """
    try:
        output = subprocess.check_output(
            ['mount'], encoding='utf-8'
        )

        if 'type cgroup2' in output:
            return 'v2'
        if 'type cgroup' in output:
            return 'v1'
        return None

    except subprocess.CalledProcessError as e:
        logger.warning(f"Error occurred while determining cgroup version: {e}")
        return None


def add_process_to_cgroup(pid: int, group_name: str = 'panda_pilot') -> bool:
    """Create a cgroup with the given name (if it does not exist) and add the specified PID to it.

    Args:
        pid: The process ID to add to the cgroup.
        group_name: Name of the cgroup to create and use.

    Returns:
        True if successfully added, False otherwise.
    """
    paths = get_process_cgroups(str(pid))
    if not paths:
        return False
    cgroup_path = paths[0]

    try:
        if not os.path.exists(cgroup_path):
            subprocess.run(['mkdir', cgroup_path], check=True, capture_output=True, text=True)
            logger.info(f"cgroup '{group_name}' created.")
        else:
            logger.info(f"cgroup '{group_name}' already exists.")
    except subprocess.CalledProcessError as e:
        logger.warning(f"failed to create cgroup '{group_name}': {e}. stdout: {e.stdout}, stderr: {e.stderr}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False
    except PermissionError as e:
        logger.warning(f"permission denied when creating cgroup '{group_name}': {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False

    try:
        with open(os.path.join(cgroup_path, 'cgroup.procs'), 'w', encoding='utf-8') as f:
            f.write(str(pid))
        logger.info(f"process {pid} added to cgroup '{group_name}'.")
        return True
    except FileNotFoundError as e:
        logger.warning(f"cgroup file not found: {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False
    except PermissionError as e:
        logger.warning(f"permission denied when adding PID {pid} to cgroup '{group_name}': {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False


def get_process_cgroups(pid: str = "self") -> list:
    """Return the cgroup paths for a given process ID.

    Args:
        pid: Process ID as a string. Default is ``'self'`` for the current process.

    Returns:
        List of cgroup path strings.
    """
    cgroups = []
    path = f"/proc/{pid}/cgroup"

    try:
        with open(path, "r", encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(":")
                if len(parts) == 3:
                    _, _, cgroup_path = parts
                    cgroups.append(cgroup_path)
    except FileNotFoundError:
        logger.warning(f"process {pid} does not exist")
    except PermissionError as e:
        logger.warning(f"error reading cgroup info for PID {pid}: {e}")

    return cgroups


def parse_cgroup_path(size: int) -> str:
    """Parse the cgroup path from ``/proc/self/cgroup``.

    Reads ``/proc/self/cgroup`` and extracts the path associated with the
    cgroup v2 entry (hierarchy id 0, empty controllers field). Returns None
    if the entry is not found or parsing fails.

    Args:
        size: Maximum allowed length of the returned path, simulating a buffer size.

    Returns:
        The parsed cgroup path, truncated to ``(size - 1)`` characters if needed,
        or None if parsing fails.
    """
    try:
        with open(PROC_CGROUP_PATH, "r", encoding='utf-8') as f_cgroup:
            logger.debug(f"Contents of {PROC_CGROUP_PATH}:")
            for line in f_cgroup:
                logger.debug(line.strip())
                parts = line.strip().split(":")
                if len(parts) == 3:
                    hierarchy_id, controllers, path = parts
                    if hierarchy_id == '0' and controllers == '':
                        return path[:size - 1]
    except IOError as e:
        logger.warning(f"Failed to open {PROC_CGROUP_PATH}: {e}")
        return None

    logger.warning(f"Failed to parse cgroup path from {PROC_CGROUP_PATH}")
    return None


def create_cgroup(pid: int = os.getpid(), controller: str = "controller") -> bool:  # noqa: C901
    """Create a cgroup for the current process.

    Creates a controller subgroup and moves the current process into it. Also
    moves all processes in the parent cgroup to the control subgroup and enables
    memory and pid controllers in the parent cgroup. Additionally creates a
    subprocesses cgroup so that child processes can be monitored and controlled.

    Args:
        pid: Process ID to create the cgroup for. Defaults to the current process ID.
        controller: Name of the controller subgroup to create.

    Returns:
        True if the cgroup was successfully created, False otherwise.
    """
    # make sure that the cgroup was not already created for this pid
    if pilot_cache:
        pids = pilot_cache.get_pids()
        if pid in pids:
            logger.debug(f"cgroup already created for pid {pid}")
            return True

    # Parse the current cgroup path this process is running in
    current_cgroup_path = parse_cgroup_path(1024)  # ad hoc size
    if not current_cgroup_path:
        logger.warning(f"failed to parse cgroup path from {PROC_CGROUP_PATH}")
        return False
    logger.debug(f"current_cgroup_path= {current_cgroup_path}")

    # Construct the full path to the (raw) parent cgroup under /sys/fs/cgroup
    # current_cgroup_path starts with '/', so skip the first char
    raw_parent_cgroup_path = os.path.join(CGROUP_PATH, current_cgroup_path[1:])
    logger.debug(f"raw_parent_cgroup_path= {raw_parent_cgroup_path}")

    # 🟢 NEW: normalize .scope → .slice so we use the writable parent
    parent_cgroup_path = get_writable_cgroup_parent(raw_parent_cgroup_path)
    logger.debug(f"parent_cgroup_path (writable)= {parent_cgroup_path}")

    # Create a "controller" cgroup for the parent process
    controller_cgroup_path = create_subgroup(parent_cgroup_path, controller)
    if not controller_cgroup_path:
        logger.warning(f"failed to create controller cgroup at {parent_cgroup_path}")
        return False

    status = move_process_to_cgroup(controller_cgroup_path, os.getpid())
    if not status:
        logger.warning(
            f"failed to move process to controller_cgroup_path: {controller_cgroup_path}"
        )
        return False

    # move all processes in the parent cgroup to the control subgroup
    _ = move_procs_to_control_subgroup(parent_cgroup_path)

    # create a new cgroup for future subprocesses
    subprocesses_cgroup_path = create_subgroup(parent_cgroup_path, "subprocesses")
    if not subprocesses_cgroup_path:
        logger.warning(f"failed to create subprocesses cgroup at {parent_cgroup_path}")
        return False

    # also create a new cgroup for the payload (still optional / commented)
    # payload_cgroup_path = create_subgroup(parent_cgroup_path, "payload")
    # if not payload_cgroup_path:
    #     logger.warning(f"failed to create payload cgroup at {parent_cgroup_path}")
    #     return False

    # enable memory and pid controllers in the parent cgroup (the .slice)
    status = enable_controllers(parent_cgroup_path, "+memory +pids")
    if not status:
        logger.warning(f"failed to enable controllers in cgroup: {parent_cgroup_path}")
        return False

    # Keep track of the cgroup path in the pilot cache
    if pilot_cache:
        pilot_cache.add_cgroup(str(pid), controller_cgroup_path)
        pilot_cache.add_cgroup("subprocesses", subprocesses_cgroup_path)
        # pilot_cache.add_cgroup("payload", payload_cgroup_path)

    return True


def get_writable_cgroup_parent(raw_cgroup_path: Union[str, Path]) -> Path:
    """Return the cgroup path where a child cgroup may be created.

    Normalizes the provided path so that if the process is placed inside a
    ``.scope`` node (common in systemd/HTCondor layouts) it returns the parent
    ``.slice`` directory, which is writable for creating child cgroups. If the
    supplied path is already writable (e.g. a ``.slice`` or a legacy layout),
    it is returned unchanged.

    Args:
        raw_cgroup_path: Path to the job's cgroup. May be a string or a
            ``pathlib.Path``, and may refer to a ``.scope``, ``.slice``, or
            other layout.

    Returns:
        A ``pathlib.Path`` pointing to the writable parent cgroup where child
        subgroups may be created, or the original path if no normalization is
        required.
    """
    p = Path(raw_cgroup_path)

    # If we are inside the .scope node, go up to the .slice parent
    if p.name.endswith(".scope"):
        slice_path = p.parent
        logger.debug(f"Detected .scope cgroup; using parent slice {slice_path}")
        return slice_path

    # If we’re already at the .slice (or some older layout), just return it
    return p


def create_subgroup(parent_path: Union[str, Path], subgroup_name: str) -> str:
    """Create a cgroup v2 subgroup for the pilot / controller.

    Normalizes the provided ``parent_path`` to a writable parent (handles
    ``.scope`` → ``.slice``), creates the requested subgroup directory under
    that writable parent, and returns the path to the created subgroup. On
    failure logs a warning and returns an empty string.

    Args:
        parent_path: Path to the parent cgroup (may be a job cgroup or a
            ``.scope`` node).
        subgroup_name: Name of the subgroup to create.

    Returns:
        Absolute path to the created subgroup on success, or an empty string
        on failure.

    Note:
        Uses :func:`get_writable_cgroup_parent` to determine where creation is
        permitted. Failure cases (permission denied, OS errors) are handled by
        logging and returning an empty string.
    """
    # Normalize to the writable parent (handles .scope → .slice)
    writable_parent = get_writable_cgroup_parent(parent_path)

    subgroup_path = Path(writable_parent) / subgroup_name

    try:
        subgroup_path.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        logger.warning(
            "failed to create cgroup %s at %s (permission denied): %s",
            subgroup_name,
            writable_parent,
            e,
        )
        return ""
    except OSError as e:
        logger.warning(
            "failed to create cgroup %s at %s: %s",
            subgroup_name,
            writable_parent,
            e,
        )
        return ""

    logger.info("created cgroup at: %s", subgroup_path)
    return str(subgroup_path)


def move_procs_to_control_subgroup(parent_cgroup_path: str, control_name: str = "control") -> list:
    """Move all PIDs from the parent cgroup's cgroup.procs file to a control subgroup.

    Args:
        parent_cgroup_path: Path to the parent cgroup directory (e.g.,
            ``/sys/fs/cgroup/system.slice/htcondor/condor_...``).
        control_name: Name of the control subgroup to create and move PIDs into.

    Returns:
        List of PID strings that were moved to the control subgroup.
    """
    parent_path = Path(parent_cgroup_path)
    procs_file = parent_path / "cgroup.procs"
    control_path = parent_path / control_name
    control_procs_file = control_path / "cgroup.procs"

    # Create control subgroup if it doesn't exist
    if not control_path.exists():
        control_path.mkdir(parents=True)

    # Read PIDs from the parent cgroup.procs
    try:
        with open(procs_file, "r", encoding='utf-8') as f:
            pids = [line.strip() for line in f if line.strip()]
    except (PermissionError, FileNotFoundError) as e:
        logger.warning(f"Failed to read {procs_file}: {e}")
        pids = []

    # Move each PID to control subgroup
    for pid in pids:
        try:
            with open(control_procs_file, "w", encoding='utf-8') as f:
                f.write(pid)
        except (PermissionError, FileNotFoundError) as e:
            logger.warning(f"Failed to move PID {pid} to {control_procs_file}: {e}")
            pids = []

    return pids


def move_procs_to_parent(path: str):
    """Move all PIDs listed in the specified cgroup.procs file to its parent cgroup.

    Args:
        path: Full path to the ``cgroup.procs`` file
            (e.g. ``os.path.join(parent_cgroup_path, "cgroup.procs")``).

    Returns:
        List of PID strings that were moved.

    Raises:
        RuntimeError: If any PID fails to move.
        FileNotFoundError: If the specified ``cgroup.procs`` file does not exist.
    """
    procs_file = Path(path)
    cgroup_path = procs_file.parent
    parent_procs_file = cgroup_path.parent / "cgroup.procs"

    if not procs_file.exists():
        raise FileNotFoundError(f"{procs_file} does not exist")

    logger.debug(f"Moving PIDs to parent cgroup: {parent_procs_file}")
    try:
        logger.debug(f"cat {str(procs_file)}:")
        result = subprocess.run(["cat", str(procs_file)], check=True, capture_output=True, text=True)
        logger.debug(f"result={result.stdout}")
        pids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        logger.debug(f"pids={pids}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to read {procs_file}: {e}") from e

    for pid in pids:
        try:
            subprocess.run(["bash", "-c", f"echo {pid} > {parent_procs_file}"], check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to move PID {pid} to {parent_procs_file}: {e}") from e

    return pids


def move_process_to_cgroup(cgroup_path: str, pid: int) -> bool:
    """Move the given process to the specified cgroup by writing its PID to cgroup.procs.

    Args:
        cgroup_path: Filesystem path to the cgroup directory.
        pid: PID of the process to move into the cgroup.

    Returns:
        True if the process was successfully moved, False otherwise.
    """
    procs_path = os.path.join(cgroup_path, "cgroup.procs")

    try:
        with open(procs_path, "a", encoding='utf-8') as f:
            f.write(f"{pid}")
    except IOError as e:
        logger.warning(f"Failed to move process to cgroup: {e}")
        try:
            result = subprocess.run([f'echo {pid} > {procs_path}'], check=True, capture_output=True, text=True)
            logger.debug(f"Command output: {result.stdout}")
            return True
        except subprocess.CalledProcessError as exc:
            logger.warning(f"failed to run command: {exc}")

        return False

    logger.debug(f"added process {pid} to cgroup {cgroup_path}")
    return True


def move_process_and_descendants_to_cgroup(cgroup_path: str, root_pid: int) -> bool:
    """Move the given PID and all of its descendants into the specified cgroup.

    Args:
        cgroup_path: Path to the cgroup directory (e.g. ``/sys/fs/cgroup/mygroup``).
        root_pid: PID of the root process to move.

    Returns:
        True if all processes were successfully moved, False otherwise.
    """
    if not _is_psutil_available:
        logger.warning("psutil module is not available, cannot move processes to cgroup.")
        return False

    procs_file = f"{cgroup_path}/cgroup.procs"
    root_process = psutil.Process(root_pid)
    all_pids = [root_process.pid] + [p.pid for p in root_process.children(recursive=True)]

    for pid in all_pids:
        try:
            with open(procs_file, "a", encoding='utf-8') as f:
                f.write(f"{pid}")
        except IOError as e:
            logger.warning(f"failed to move process to cgroup: {e}")
            cmd = f"echo {pid} > {procs_file}"
            try:
                subprocess.run(cmd, shell=True, check=True, executable="/bin/bash")
            except subprocess.CalledProcessError as exc:
                logger.warning(f"failed to move PID {pid} to cgroup: {exc}")
                return False

    logger.info(f"moved process {root_pid} to cgroup {cgroup_path} (process list= {all_pids})")
    # test test BS kills
    #if "subprocesses" in str(cgroup_path):
    #    try:
    #        set_memory_limit(cgroup_path, 100000)
    #    except (OSError, FileNotFoundError, PermissionError, ValueError) as e:
    #        logger.warning(f"failed to set memory limit for cgroup {cgroup_path}: {e}")

    return True


def enable_controllers(cgroup_path: str, controllers: str) -> bool:
    """Enable specified controllers in the cgroup's subtree_control file.

    Args:
        cgroup_path: Filesystem path to the cgroup directory.
        controllers: Space-separated controllers to enable, prefixed with ``+``
            (e.g. ``"+cpu +memory"``).

    Returns:
        True if the controllers were successfully enabled, False otherwise.
    """
    subtree_control_path = os.path.join(cgroup_path, "cgroup.subtree_control")
    try:
        with open(subtree_control_path, "w", encoding='utf-8') as f:
            f.write(f"{controllers}")
    except IOError as e:
        logger.warning(f"Failed to enable controllers: {e}")
    else:
        logger.debug(f"Enabled controllers {controllers} in cgroup {cgroup_path}")
        return True

    try:
        cmd = f'echo \"{controllers}\" > {subtree_control_path}'
        logger.debug(f"Executing command: {cmd}")
        result = subprocess.run(cmd,
                                shell=True, check=True, executable="/bin/bash", capture_output=True, text=True)
        #cmd = f"echo '{controllers}' | sudo tee {cgroup_path}/cgroup.subtree_control > /dev/null"
        #result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            logger.warning(f"Failed to enable controllers at {cgroup_path}")
            return False
        logger.debug(f"Command output: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.warning(f"failed to run command: {e}")
        #return False
        cmd = f'ls -l {subtree_control_path}'
        logger.debug(f"Executing command: {cmd}")
        result = subprocess.run(cmd,
                                shell=True, check=True, executable="/bin/bash", capture_output=True, text=True)
        if result.returncode != 0:
            logger.debug(f"Failed to execute ls command: {result.stderr}")
        else:
            logger.debug(f"Command output: {result.stdout}")
        return False

    return True


def get_pids_for_cgroup(cgroup_path: str) -> list:
    """Return the PIDs of all processes in the specified cgroup.

    Args:
        cgroup_path: Path to the cgroup directory (e.g. ``/sys/fs/cgroup/mygroup``).

    Returns:
        List of integer PIDs in the cgroup.
    """
    procs_file = os.path.join(cgroup_path, "cgroup.procs")
    try:
        with open(procs_file, "r", encoding='utf-8') as f:
            pids = [int(line.strip()) for line in f if line.strip()]
        return pids
    except IOError as e:
        logger.warning(f"Failed to read {procs_file}: {e}")
        return []


def read_memory_events(cgroup_path: str, local: bool = False) -> dict:
    """Read and parse the ``memory.events`` file of a cgroup v2 cgroup.

    The file is a flat ``key value`` listing. All keys are returned as
    integers, typically ``low``, ``high``, ``max``, ``oom``, ``oom_kill`` and
    ``oom_group_kill``. Note the meaning of the two most relevant counters:

    - ``max``: number of times the cgroup exceeded ``memory.max`` *without*
      a kill (memory was reclaimed instead). A high value means the payload
      repeatedly ran up against the limit and survived.
    - ``oom_kill``: number of processes killed by the cgroup OOM killer. Any
      increase means the kernel terminated a process on memory grounds.

    The counters are cumulative for the lifetime of the cgroup and remain
    readable after all processes in the cgroup have exited, which is what
    makes this a reliable post-mortem source (unlike ``dmesg``).

    This function never raises. A missing file means cgroups are not in use
    or the node runs cgroups v1, both of which are legitimate states.

    Args:
        cgroup_path: Path to the cgroup directory (e.g. ``/sys/fs/cgroup/mygroup``).
        local: If True, read ``memory.events.local`` (events attributed to this
            cgroup only) instead of ``memory.events`` (hierarchical, i.e. this
            cgroup and its descendants).

    Returns:
        dict mapping event name to integer count, or an empty dict if the file
        does not exist or cannot be parsed.
    """
    filename = "memory.events.local" if local else "memory.events"
    path = os.path.join(cgroup_path, filename)
    events = {}

    try:
        with open(path, "r", encoding="utf-8") as fh:
            content = fh.read()
    except FileNotFoundError:
        logger.debug(f"{path} does not exist (cgroups v1 or cgroups not in use)")
        return {}
    except OSError as exc:  # includes PermissionError
        logger.warning(f"failed to read {path}: {exc}")
        return {}

    for line in content.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        try:
            events[parts[0]] = int(parts[1])
        except (ValueError, TypeError):
            logger.warning(f"ignoring malformed line in {path}: {line!r}")

    return events


def get_memory_limit_and_peak(cgroup_path: str) -> tuple[Optional[int], Optional[int]]:
    """Read ``memory.max`` and ``memory.peak`` for a cgroup v2 cgroup.

    Both values are used for diagnostics only, so a missing or unreadable file
    is not an error. ``memory.peak`` requires Linux 5.19 or later and is often
    absent. ``memory.max`` contains the string ``"max"`` when no limit is set,
    which is reported as None.

    Args:
        cgroup_path: Path to the cgroup directory.

    Returns:
        tuple of (memory.max in bytes, memory.peak in bytes); either element is
        None if the corresponding value is unavailable or unlimited.
    """
    def _read_int(filename: str) -> Optional[int]:
        path = os.path.join(cgroup_path, filename)
        try:
            with open(path, "r", encoding="utf-8") as fh:
                content = fh.read().strip()
        except OSError as exc:  # includes FileNotFoundError and PermissionError
            logger.debug(f"could not read {path}: {exc}")
            return None
        if content == "max":  # no limit set
            return None
        try:
            return int(content)
        except (ValueError, TypeError):
            logger.debug(f"unexpected content in {path}: {content!r}")
            return None

    return _read_int("memory.max"), _read_int("memory.peak")


def monitor_cgroup(cgroup_path: str) -> dict:
    """Monitor the specified cgroup by logging its PIDs and memory usage.

    Reads ``memory.current``, ``memory.events`` and ``pids.current`` from the
    cgroup and logs a formatted summary. The parsed ``memory.events`` contents
    are returned so that upstream code can react to kernel-initiated kills.

    ``memory.events`` is read even when the cgroup no longer holds any
    processes: the counters persist after the processes are gone, and an
    emptied cgroup is precisely the state left behind by an OOM kill (all the
    more so when ``memory.oom.group`` is enabled).

    Args:
        cgroup_path: Path to the cgroup directory (e.g. ``/sys/fs/cgroup/mygroup``).

    Returns:
        dict mapping ``memory.events`` names to their cumulative counts, e.g.
        ``{'low': 0, 'high': 0, 'max': 5605, 'oom': 0, 'oom_kill': 0,
        'oom_group_kill': 0}``. Empty if the file could not be read. Callers
        should use ``.get(name, 0)`` since the available keys are kernel
        version dependent.
    """
    # read the OOM counters first - they are valid regardless of whether the
    # cgroup still holds any processes
    events = read_memory_events(cgroup_path)

    pids = get_pids_for_cgroup(cgroup_path)
    if not pids:
        events_str = " ".join([f"{key}={value}" for key, value in events.items()]) or "<unavailable>"
        logger.info(f"[cgroup: {cgroup_path}]\n  No processes found.\n  Memory Events: {events_str}")
        return events

    output_lines = [f"[cgroup: {cgroup_path}]", f"  PIDs: {', '.join([str(pid) for pid in pids])}"]

    files_to_read = {
        "Memory Usage": f"{cgroup_path}/memory.current",
        "Memory Events": f"{cgroup_path}/memory.events",
        "Process Count": f"{cgroup_path}/pids.current"
    }

    for label, filepath in files_to_read.items():
        try:
            with open(filepath, "r", encoding="utf-8") as fh:
                content = fh.read().strip()
        except OSError as exc:  # includes FileNotFoundError and PermissionError
            output_lines.append(f"  {label}: <error reading {filepath}> ({exc})")
            continue

        if '\n' in content:
            indented = "\n    ".join(content.splitlines())
            output_lines.append(f"  {label}:\n    {indented}")
        else:
            output_lines.append(f"  {label}: {content}")

    logger.info("\n%s", "\n".join(output_lines))
    return events


def get_oom_deltas(cgroup_path: str, events: dict) -> dict:
    """Return the increase in each memory.events counter since the baseline.

    The baseline is the snapshot stored in the pilot cache at payload start.
    If no baseline is available the current values are returned unchanged,
    i.e. the check fails towards reporting rather than towards silence.

    Args:
        cgroup_path: Path to the cgroup directory (also the baseline key).
        events: Current parsed ``memory.events`` contents.

    Returns:
        dict mapping event name to the delta since the baseline.
    """
    baseline = pilot_cache.get_oom_baseline(cgroup_path) if pilot_cache else {}
    if not baseline:
        logger.warning(
            f"no memory.events baseline stored for {cgroup_path} - "
            "will interpret the absolute counters as belonging to the current payload"
        )

    return {key: value - baseline.get(key, 0) for key, value in events.items()}


def store_oom_baseline(cgroup_path: str = None) -> dict:
    """Snapshot the current memory.events counters as the OOM baseline.

    Called at payload start. The "subprocesses" cgroup is created once per
    pilot and is shared by every payload of a multi-job pilot as well as by
    all commands launched through ``execute()``, so its OOM counters cannot be
    attributed to the current payload without a baseline to subtract.

    Args:
        cgroup_path: Path to the cgroup directory. Defaults to the
            "subprocesses" cgroup recorded in the pilot cache.

    Returns:
        The snapshotted ``memory.events`` contents (empty dict if unavailable).
    """
    if not cgroup_path:
        cgroup_path = pilot_cache.get_cgroup("subprocesses") if pilot_cache else None
    if not cgroup_path:
        logger.debug("no subprocesses cgroup known - cannot store an OOM baseline")
        return {}

    events = read_memory_events(cgroup_path)
    if pilot_cache:
        pilot_cache.set_oom_baseline(cgroup_path, events)

    if events:
        logger.debug(f"stored memory.events baseline for {cgroup_path}: {events}")
    else:
        logger.debug(f"no memory.events available for {cgroup_path} - stored an empty OOM baseline")

    return events


def format_oom_diagnostics(cgroup_path: str, deltas: dict) -> str:
    """Build a human readable diagnostics string for a cgroup OOM kill.

    The message names ``memory.events`` explicitly so that it cannot be
    confused with the ``dmesg`` based scan or with the prmon soft limit, and
    it states the action needed so that it is directly usable in the job
    monitor.

    Args:
        cgroup_path: Path to the cgroup directory.
        deltas: Per-counter increases since the payload-start baseline.

    Returns:
        str: diagnostics message.
    """
    limit, peak = get_memory_limit_and_peak(cgroup_path)
    extras = []
    if limit is not None:
        extras.append(f"cgroup limit {limit // (1024 * 1024)} MB")
    if peak is not None:
        extras.append(f"peak {peak // (1024 * 1024)} MB")
    extras_str = f"; {', '.join(extras)}" if extras else ""

    return (
        f"Payload killed by the kernel cgroup OOM killer "
        f"(memory.events: oom_kill={deltas.get('oom_kill', 0)}, "
        f"oom_group_kill={deltas.get('oom_group_kill', 0)}){extras_str}; "
        f"increase the memory request"
    )


def check_for_cgroup_oom_kill(exit_code: int, cgroup_path: str = None) -> tuple[int, str, dict]:
    """Check whether the kernel OOM-killed the payload, using memory.events.

    This is the authoritative post-mortem OOM check. ``dmesg`` is not always
    readable in an unprivileged container and does not always retain the kill
    message, whereas the cgroup ``memory.events`` counters are always present
    on a cgroups v2 node and survive the death of the processes.

    The counters are compared against the baseline taken at payload start, so
    a kill belonging to an earlier job of a multi-job pilot is not attributed
    to the current payload.

    Following the reporting rules requested in the JIRA ticket:

    - non-zero payload exit code and an OOM kill: return an error code
    - zero payload exit code and an OOM kill: warn only, return 0 (a payload
      that survived a partial kill should not be failed on this evidence)
    - no OOM kill but a large ``max`` delta: log that the payload repeatedly
      reached the limit without being killed, return 0

    Args:
        exit_code: Exit code from the payload execution.
        cgroup_path: Path to the cgroup directory. Defaults to the
            "subprocesses" cgroup recorded in the pilot cache.

    Returns:
        tuple of (error code, diagnostics, deltas). The error code is 0 when no
        error should be set. The deltas dict is empty when no ``memory.events``
        information was available.
    """
    if not cgroup_path:
        cgroup_path = pilot_cache.get_cgroup("subprocesses") if pilot_cache else None
    if not cgroup_path:
        logger.debug("no subprocesses cgroup known - skipping the memory.events OOM check")
        return 0, "", {}

    events = read_memory_events(cgroup_path)
    if not events:
        logger.debug("no memory.events information available - skipping the memory.events OOM check")
        return 0, "", {}

    deltas = get_oom_deltas(cgroup_path, events)
    logger.info(f"memory.events for {cgroup_path}: {events} (change since payload start: {deltas})")

    oom_kill = deltas.get("oom_kill", 0)
    oom_group_kill = deltas.get("oom_group_kill", 0)

    if oom_kill <= 0 and oom_group_kill <= 0:
        # no kill, but did the payload repeatedly hit the limit and survive?
        if deltas.get("max", 0) > 0:
            logger.warning(
                f"payload reached the cgroup memory limit {deltas.get('max')} times without being killed "
                f"(memory.events max counter) - the memory request is close to insufficient"
            )
        else:
            logger.info("no cgroup OOM kill detected for the payload")
        return 0, "", deltas

    diagnostics = format_oom_diagnostics(cgroup_path, deltas)

    if exit_code == 0:
        # per the ticket: a warning is better than nothing, but do not fail the job
        logger.warning(f"{diagnostics} (payload exit code is zero - not setting an error code)")
        return 0, diagnostics, deltas

    logger.warning(diagnostics)
    return errors.PAYLOADOUTOFMEMORY, diagnostics, deltas


def set_memory_limit(cgroup_path: str, memory_bytes: int):
    """Set the maximum memory usage limit for a given cgroup v2.

    Writes the specified limit to the cgroup's ``memory.max`` file. If the
    limit is exceeded, the kernel will trigger an OOM kill.

    Args:
        cgroup_path: Full path to the cgroup (e.g. ``/sys/fs/cgroup/mygroup``).
        memory_bytes: Maximum allowed memory in bytes. Use ``-1`` to disable
            the limit (writes ``"max"``).

    Raises:
        FileNotFoundError: If the ``memory.max`` file is missing.
        PermissionError: If the process lacks permission to write to the cgroup.
        ValueError: If ``memory_bytes`` is invalid or less than -1.
        OSError: For other OS-level errors.
    """
    memory_max_path = os.path.join(cgroup_path, "memory.max")

    if memory_bytes < -1:
        raise ValueError(f"Invalid memory limit: {memory_bytes}")

    # cgroup expects "max" for unlimited
    value = "max" if memory_bytes == -1 else str(memory_bytes)

    try:
        with open(memory_max_path, "w", encoding='utf-8') as f:
            f.write(value)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"{memory_max_path} does not exist.") from e
    except PermissionError as e:
        raise PermissionError(f"Permission denied to write to {memory_max_path}. Are you root or delegated?") from e
    except OSError as e:
        raise OSError(f"Error writing memory limit to {memory_max_path}: {e}") from e

    logger.info(f"[cgroup: {cgroup_path}]\n  Max memory usage: {value}")


def set_oom_group(cgroup_path: str) -> bool:
    """Enable atomic OOM-group killing for a cgroup v2.

    Writes ``1`` to ``memory.oom.group`` so that when any process in the
    cgroup exceeds the memory limit, the kernel sends SIGKILL to *all*
    processes in the cgroup atomically, preventing half-killed payloads.

    Args:
        cgroup_path: Full path to the cgroup (e.g. ``/sys/fs/cgroup/mygroup``).

    Returns:
        True if the file was written successfully, False otherwise.
    """
    oom_group_path = os.path.join(cgroup_path, "memory.oom.group")
    try:
        with open(oom_group_path, "w", encoding='utf-8') as f:
            f.write("1")
    except OSError as e:
        logger.warning(f"failed to set memory.oom.group for {cgroup_path}: {e}")
        return False

    logger.info(f"memory.oom.group enabled for cgroup: {cgroup_path}")
    return True
