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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023-25

import logging
import os
import subprocess
try:
    import psutil
except ImportError:
    print('FAILED; psutil module could not be imported')
    _is_psutil_available = False
else:
    _is_psutil_available = True
from re import findall
from typing import Optional

# from pilot.common.exception import MiddlewareImportFailure

logger = logging.getLogger(__name__)


def is_process_running_by_pid(pid: int) -> bool:
    """Check whether the given process is still running via /proc.

    Args:
        pid: Process ID to check.

    Returns:
        True if the process is still running, False otherwise.
    """
    return os.path.exists(f"/proc/{pid}")


def is_process_running(pid: int) -> bool:
    """Check whether the given process is still running.

    Uses psutil when available, falling back to ``/proc/{pid}`` existence check
    if psutil is not importable.

    Args:
        pid: Process ID to check.

    Returns:
        True if the process is still running, False otherwise.
    """
    if not _is_psutil_available:
        is_running = is_process_running_by_pid(pid)
        logger.warning(f'using /proc/{pid} instead of psutil (is_running={is_running})')
        return is_running
        # raise MiddlewareImportFailure("required dependency could not be imported: psutil")

    return psutil.pid_exists(pid)


def get_pid(jobpid: int) -> int:
    """Return the PID of the memory monitoring tool (prmon) for the given job.

    Uses psutil when available, falling back to parsing ``ps aux`` output if not.

    Args:
        jobpid: PID of the job process (``job.pid``).

    Returns:
        PID of the prmon process, or None if not found.
    """
    pid = None

    if _is_psutil_available:
        pid = find_pid_by_command_and_ppid('prmon', jobpid)
    else:
        try:
            _ps = subprocess.run(['ps', 'aux', str(os.getpid())], stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, text=True, check=True, encoding='utf-8')
            prmon = f'prmon --pid {jobpid}'
            pid = None
            pattern = r'\b\d+\b'
            for line in _ps.stdout.split('\n'):
                # line=atlprd55  16451  0.0  0.0   2944  1148 ?        SN   17:42   0:00 prmon --pid 13096 ..
                if prmon in line and f';{prmon}' not in line:  # ignore the line that includes the setup
                    matches = findall(pattern, line)
                    if matches:
                        pid = matches[0]
                        logger.info(f'extracting prmon pid from line: {line}')
                        break

        except subprocess.CalledProcessError as exc:
            logger.warning(f"error: {exc}")

    return pid


def find_pid_by_command_and_ppid(command: str, payload_pid: int) -> int:
    """Find the PID of a process by command name, verifying it belongs to the given payload.

    Args:
        command: Command name to search for (e.g. ``"prmon"``).
        payload_pid: Expected payload process ID (used to verify ownership via cmdline).

    Returns:
        PID of the matching process, or None if not found.
    """
    if not _is_psutil_available:
        logger.warning('find_pid_by_command_and_ppid(): psutil not available - aborting')
        return None

    for process in psutil.process_iter(['pid', 'name', 'cmdline', 'ppid']):
        try:
            # Check if the process has a cmdline attribute (command-line arguments)
            # cmdline = cmdline=['prmon', '--pid', '46258', '--filename', 'memory_monitor_output.txt', '--json-summary',
            # 'memory_monitor_summary.json', '--interval', '60'] pid=54481 ppid=46487 name=prmon parent_pid=2840
            if process.info['cmdline'] and (command in process.info['cmdline'][0] and process.info['cmdline'][2] == str(payload_pid)):
                logger.debug(f"command={command} is in {process.info['cmdline'][0]}")
                logger.debug(f"ok returning pid={process.info['pid']}")
                return process.info['pid']
        except (psutil.AccessDenied, psutil.ZombieProcess, KeyError):
            pass

    return None


def get_parent_pid(pid: int) -> Optional[int]:
    """Return the parent process ID for the given PID.

    Args:
        pid: Process ID to query.

    Returns:
        Parent process ID, or None if the process does not exist.
    """
    try:
        process = psutil.Process(pid)
        parent_pid = process.ppid()
        return parent_pid
    except psutil.NoSuchProcess:
        return None


def get_child_processes(parent_pid: int) -> list:
    """Return all child processes of the given parent PID as a list of (pid, cmdline) tuples.

    Uses psutil when available, falling back to the legacy ``/proc``-based
    implementation if not.

    Args:
        parent_pid: Parent process ID.

    Returns:
        List of ``(pid, cmdline)`` tuples for all descendant processes.
    """
    if not _is_psutil_available:
        logger.warning('get_child_processes(): psutil not available - using legacy code as a fallback')
        return get_child_processes_legacy(parent_pid)

    return get_all_descendant_processes(parent_pid)


def get_all_descendant_processes(parent_pid: int, top_pid: int = os.getpid()) -> list:
    """Recursively find all descendant processes of the given parent PID.

    Args:
        parent_pid: PID to use as the root of the search.
        top_pid: PID to exclude from results (defaults to the current process).

    Returns:
        List of ``(pid, cmdline)`` tuples for all descendants.
    """
    def find_descendant_processes(pid: int, top_pid: int) -> list:
        """Recursively collect all descendant (pid, cmdline) pairs for the given pid.

        Returns:
            List of ``(pid, cmdline)`` tuples for all descendants.
        """
        try:
            descendants = []
            for process in psutil.process_iter(attrs=['pid', 'ppid', 'cmdline']):
                process_info = process.info
                child_pid = process_info['pid']
                ppid = process_info['ppid']
                cmdline = process_info['cmdline']
                if ppid == pid and child_pid != top_pid:
                    descendants.append((child_pid, cmdline))
                    descendants.extend(find_descendant_processes(child_pid, top_pid))
            return descendants
        except (psutil.AccessDenied, psutil.ZombieProcess, KeyError):
            return []
    all_descendant_processes = find_descendant_processes(parent_pid, top_pid)

    return all_descendant_processes


def get_child_processes_legacy(parent_pid: int) -> list:
    """Return child processes of the given parent PID using /proc (legacy fallback).

    This implementation scans ``/proc`` directly and is less efficient than the
    psutil-based recursive approach. It should be removed once psutil is
    available everywhere.

    Args:
        parent_pid: Parent process ID.

    Returns:
        List of ``(pid, cmdline)`` tuples for direct child processes.
    """
    child_processes = []

    # Iterate through all directories in /proc
    for _pid in os.listdir('/proc'):
        if not _pid.isdigit():
            continue  # Skip non-numeric directories

        try:
            pid = int(_pid)
        except ValueError as exc:
            logger.warning(f'exception caught: got an unexpected value for pid={_pid}: {exc}')
            continue

        try:
            # Read the command line of the process
            with open(f'/proc/{pid}/cmdline', 'rb') as cmdline_file:
                cmdline = cmdline_file.read().decode().replace('\x00', ' ')

            # Read the parent PID of the process
            with open(f'/proc/{pid}/stat', 'rb') as stat_file:
                stat_info = stat_file.read().decode()
                parts = stat_info.split()
                ppid = int(parts[3])  # can throw a ValueError

            # Check if the parent PID matches the specified parent process
            if ppid == parent_pid:
                child_processes.append((pid, cmdline))

        except (ValueError, FileNotFoundError, PermissionError):
            continue  # Process may have terminated or we don't have permission

    return child_processes


def get_subprocesses(pid: int, debug: bool = False) -> list:
    """Return the PIDs of all subprocesses belonging to the given PID.

    Args:
        pid: Main process PID.
        debug: If True, log the child process list at INFO level (used for looping
            job diagnostics); otherwise log at DEBUG level.

    Returns:
        List of child process PIDs.
    """
    pids = get_child_processes(pid)
    if debug:  # always dump for looping jobs e.g.
        logger.info(f'child processes for pid={pid}: {pids}')
    else:  # otherwise, only in debug mode
        logger.debug(f'child processes for pid={pid}: {pids}')

    return [pid[0] for pid in pids]
    #cmd = f'ps -opid --no-headers --ppid {pid}'
    #_, out, _ = execute(cmd)
    #return [int(line) for line in out.splitlines()] if out else []


def get_command_by_pid(pid: int) -> Optional[str]:
    """Return the full command line for the given process ID.

    Args:
        pid: Process ID to query.

    Returns:
        Full command line string, or None if psutil is unavailable or the
        process no longer exists.
    """
    try:
        process = psutil.Process(pid)
        command = " ".join(process.cmdline())
        return command
    except NameError:
        logger.warning('psutil module not available - aborting')
        return None
    except psutil.NoSuchProcess:
        logger.warning(f"process with PID {pid} not found")
        return None


def find_process_by_jobid(jobid: int) -> Optional[int]:
    """Find the PID of a process whose command arguments contain the given job ID.

    Args:
        jobid: PanDA job ID to search for.

    Returns:
        PID of the matching process, or None if not found.
    """
    if not _is_psutil_available:
        logger.warning('find_process_by_jobid(): psutil not available - aborting')
        return None

    for proc in psutil.process_iter():
        try:
            cmd_line = proc.cmdline()
        except psutil.NoSuchProcess:
            continue

        for arg in cmd_line:
            if str(jobid) in arg and 'xrootd' not in arg:
                return proc.pid

    return None


def find_actual_payload_pid(bash_pid: int, payload_cmd: str) -> Optional[int]:
    """Find the PID of the actual payload process launched under the given bash PID.

    Walks the subprocesses of ``bash_pid`` looking for one whose command line
    contains ``payload_cmd``.

    Args:
        bash_pid: PID of the bash wrapper process.
        payload_cmd: Partial command string to match against (e.g. ``"Reco_tf.py"``).

    Returns:
        PID of the matching payload process, or ``bash_pid`` if no children are
        found, or None if psutil is unavailable.
    """
    if not _is_psutil_available:
        logger.warning('find_actual_payload_pid(): psutil not available - aborting')
        return None

    children = get_subprocesses(bash_pid)
    if not children:
        logger.warning(f'no children found for bash PID {bash_pid}')
        return bash_pid

    for pid in children:
        cmd = get_command_by_pid(pid)
        logger.debug(f'pid={pid} cmd={cmd}')
        if payload_cmd in cmd:
            logger.info(f'found payload PID={pid} for bash PID={bash_pid}')
            return pid

    logger.warning(f'could not find payload PID for bash PID {bash_pid}')
    return None


def find_lingering_processes(parent_pid: int) -> list:
    """Find non-zombie child processes still running after the parent has terminated.

    Args:
        parent_pid: PID of the (terminated) parent process.

    Returns:
        List of PIDs of lingering child processes.
    """
    if not _is_psutil_available:
        logger.warning('psutil not available, cannot find lingering processes - aborting')
        return []

    lingering_processes = []
    try:
        parent_process = psutil.Process(parent_pid)
        for child in parent_process.children(recursive=True):
            try:
                if child.status() != psutil.STATUS_ZOMBIE:
                    lingering_processes.append(child.pid)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                logger.warning(f"[harmless] failed to get status for child process {child.pid}: {e}")
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, psutil.FileNotFoundError) as e:
        logger.warning(f"[harmless] failed to get parent process {parent_pid}: {e}")

    return lingering_processes


def check_cpu_load():
    """Check whether the system is under heavy CPU load (above 80%).

    Returns:
        True if the system CPU usage exceeds 80%, False otherwise.
    """
    if not _is_psutil_available:
        logger.warning('psutil not available, cannot check CPU load (pretending it is normal)')
        return False

    try:
        cpu_percent = psutil.cpu_percent(interval=0.5)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
        logger.warning(f"Failed to read CPU percent: {e}")
        logger.info("system is under heavy CPU load (assumed)")
        return True
    if cpu_percent > 80:
        logger.info("system is under heavy CPU load")
        return True
    else:
        logger.info("system load is normal")
        return False


def get_process_info(cmd: str, user: str = "", pid: int = 0) -> list:
    """Return CPU and memory usage for a process matching the given command.

    Args:
        cmd: Command string to search for (e.g. ``"python3 pilot3/pilot.py"``).
        user: If non-empty, restrict the search to processes owned by this user.
        pid: If non-zero, return detailed info only for this specific PID.

    Returns:
        List of ``[cpu_percent, memory_percent, cmdline, count]`` for the
        matching process, or an empty list if not found or psutil is unavailable.
    """
    if not _is_psutil_available:
        logger.warning('psutil not available, cannot check pilot CPU load')
        return []

    processes = []
    num = 0

    for proc in psutil.process_iter(['pid', 'username', 'cpu_percent', 'memory_percent', 'cmdline']):
        try:
            if user and proc.info['username'] != user:
                continue
            cmdline = proc.info['cmdline']
            if cmdline and cmd in ' '.join(cmdline):
                num += 1
                if proc.info['pid'] == pid:
                    processes = [proc.info['cpu_percent'], proc.info['memory_percent'], ' '.join(cmdline)]
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess, KeyError):
            continue

    if processes:
        processes.append(num)

    return processes


def list_processes_and_threads() -> list:
    """List all processes and threads owned by the current user.

    Filters by the real UID of the current process (``os.getuid()``) rather
    than by username, so this function works correctly on worker nodes where
    the pilot's UID has no entry in ``/etc/passwd`` (e.g. Kubernetes pods or
    HPC nodes with numeric UIDs). This avoids the ``KeyError`` raised by
    ``getpass.getuser()`` / ``pwd.getpwuid()`` on such systems.

    The output format mimics ``ps -eo pid,ppid -m``, restricted to processes
    owned by the current UID.

    Returns:
        List of formatted PID/PPID strings, one entry per process plus an
        extra dash-row for each process that has additional threads. Returns
        an empty list if psutil is not available.
    """
    if not _is_psutil_available:
        logger.warning('psutil not available, cannot list processes and threads')
        return []

    current_uid = os.getuid()
    processes = []
    for proc in psutil.process_iter(attrs=['pid', 'ppid', 'uids']):
        try:
            info = proc.info
            uids = info.get('uids')
            if uids is None or uids.real != current_uid:
                continue
            if info['pid'] == 1:
                continue
            processes.append((info['pid'], info['ppid'], proc))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    processes.sort(key=lambda x: x[0])

    lines = [f"{'PID':>6} {'PPID':>6}"]
    for pid, ppid, proc in processes:
        ppid_str = str(ppid) if ppid is not None else '-'
        lines.append(f"{pid:6} {ppid_str:6}")
        try:
            threads = proc.threads()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            threads = []
        extra_threads = [t for t in threads if t.id != pid]
        if extra_threads:
            lines.append(f"{'-':6} {'-':6}")

    return lines


def get_clock_speed() -> Optional[float]:
    """Return the current CPU clock speed in MHz.

    Returns:
        Current clock speed in MHz, 0.0 if the frequency cannot be read, or
        None if psutil is not available.
    """
    if not _is_psutil_available:
        logger.warning('get_clock_speed(): psutil not available - aborting')
        return None

    freq = psutil.cpu_freq()  # scpufreq(current=2300, min=2300, max=2300)
    return freq.current if freq is not None else 0.0


def get_pilot_process_tree(root_pid: int) -> str:
    """Return a formatted snapshot of the pilot process tree rooted at ``root_pid``.

    Walks the descendant tree via psutil so only processes belonging to this
    pilot are included, avoiding the noise of unrelated jobs running on the
    same worker node. Falls back to an empty string when psutil is unavailable
    rather than raising an exception.

    Args:
        root_pid: PID of the process to use as the root of the tree.

    Returns:
        A formatted multi-line string with one row per process, indented to
        reflect parent/child depth, or an empty string if psutil is not
        available.
    """
    if not _is_psutil_available:
        logger.warning('get_pilot_process_tree(): psutil not available - skipping')
        return ''

    lines = [f"{'PID':>7} {'PPID':>7} {'STAT':>6}  COMMAND"]

    def _walk(pid: int, depth: int = 0) -> None:
        """Walk the process tree recursively, appending one line per process."""
        try:
            proc = psutil.Process(pid)
            with proc.oneshot():
                ppid = proc.ppid()
                status = proc.status()
                cmdline = ' '.join(proc.cmdline()) or proc.name()
            indent = '  ' * depth
            lines.append(f"{pid:>7} {ppid:>7} {status:>6}  {indent}{cmdline}")
            for child in proc.children(recursive=False):
                _walk(child.pid, depth + 1)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    _walk(root_pid)
    return '\n'.join(lines)


def get_process_details(pid: int) -> str:
    """Return a single-line summary of the given process.

    Includes the PID, parent PID, status, and full command line. Falls back
    gracefully when psutil is unavailable or the process no longer exists.

    Args:
        pid: Process ID to describe.

    Returns:
        A formatted string of the form
        ``"PID <pid> (ppid=<ppid>, status=<status>): <cmdline>"``, or a
        short unavailability notice if the process cannot be inspected.
    """
    if not _is_psutil_available:
        logger.warning('get_process_details(): psutil not available - skipping')
        return f'PID {pid}: (psutil not available)'

    try:
        proc = psutil.Process(pid)
        with proc.oneshot():
            ppid = proc.ppid()
            status = proc.status()
            cmdline = ' '.join(proc.cmdline()) or proc.name()
        return f"PID {pid} (ppid={ppid}, status={status}): {cmdline}"
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as exc:
        return f"PID {pid}: (unavailable: {exc})"
