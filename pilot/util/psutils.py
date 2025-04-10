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

import getpass
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

# from pilot.common.exception import MiddlewareImportFailure

logger = logging.getLogger(__name__)


def is_process_running_by_pid(pid: int) -> bool:
    """
    Is the given process still running?

    :param pid: process id (int)
    :return: True (process still running), False (process not running).
    """
    return os.path.exists(f"/proc/{pid}")


def is_process_running(pid: int) -> bool:
    """
    Is the given process still running?

    Note: if psutil module is not available, this function will raise an exception.

    :param pid: process id (int)
    :return: True (process still running), False (process not running)
    :raises: MiddlewareImportFailure if psutil module is not available.
    """
    if not _is_psutil_available:
        is_running = is_process_running_by_pid(pid)
        logger.warning(f'using /proc/{pid} instead of psutil (is_running={is_running})')
        return is_running
        # raise MiddlewareImportFailure("required dependency could not be imported: psutil")

    return psutil.pid_exists(pid)


def get_pid(jobpid: int) -> int:
    """
    Try to figure out the pid for the memory monitoring tool.

    Attempt to use psutil, but use a fallback to ps-command based code if psutil is not available.

    :param jobpid: job.pid (int)
    :return: pid (int|None).
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
    """
    Find the process id corresponding to the given command, and ensure that it belongs to the given payload.

    :param command: command (str)
    :param payload_pid: payload process id (int)
    :return: process id (int) or None.
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


def get_parent_pid(pid: int) -> int or None:
    """
    Return the parent process id for the given pid.

    :param pid: process id (int)
    :return: parent process id (int or None).
    """
    try:
        process = psutil.Process(pid)
        parent_pid = process.ppid()
        return parent_pid
    except psutil.NoSuchProcess:
        return None


def get_child_processes(parent_pid: int) -> list:
    """
    Return a list of all child processes belonging to the same parent process id.

    Uses a fallback to /proc/{pid} in case psutil is not available.

    :param parent_pid: parent process id (int)
    :return: child processes (list).
    """
    if not _is_psutil_available:
        logger.warning('get_child_processes(): psutil not available - using legacy code as a fallback')
        return get_child_processes_legacy(parent_pid)

    return get_all_descendant_processes(parent_pid)


def get_all_descendant_processes(parent_pid: int, top_pid: int = os.getpid()) -> list:
    """
    Recursively find child processes using the given parent pid as a starting point.

    :param parent_pid: parent process id (int)
    :param top_pid: do not include os.getpid() in the list (int)
    :return: descendant process ids and cmdline (list).
    """
    def find_descendant_processes(pid: int, top_pid: int) -> list:
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
    """
    Return a list of all child processes belonging to the same parent process id.

    Note: this approach is not efficient if one is to find all child processes using
    the parent pid as a starting point. Better to use a recursive function using psutil.
    This method should be removed once psutil is available everywhere.

    :param parent_pid: parent process id (int)
    :return: child processes (list).
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
    """
    Return the subprocesses belonging to the given PID as a list.

    :param pid: main process PID (int)
    :param debug: control debug mode (bool)
    :return: list of subprocess PIDs.
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


def get_command_by_pid(pid: int) -> str or None:
    """
    Return the command corresponding to the given process id.

    :param pid: process id (int)
    :return: command (str or None).
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


def find_process_by_jobid(jobid: int) -> int or None:
    """
    Find the process ID of a process whose command arguments contain the given job ID.

    :param jobid: the job ID to search for (int)
    :return: the process ID of the matching process, or None if no match is found (int or None).
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


def find_actual_payload_pid(bash_pid: int, payload_cmd: str) -> int or None:
    """
    Find the actual payload PID.

    Identify all subprocesses of the given bash PID and search for the payload command. Return its PID.

    :param bash_pid: bash PID (int)
    :param payload_cmd: payload command (partial) (str)
    :return: payload PID (int or None).
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
    """
    Find processes that are still running after the specified parent process has terminated.

    :param parent_pid: The PID of the parent process (int)
    :return: A list of lingering process PIDs (list).
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
    """
    Check if the system is under heavy CPU load.

    High CPU load is here defined to be above 80%.

    :return: True (system is under heavy CPU load), False (system load is normal).
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
    """
    Return process info for given command.

    The function returns a list with format [cpu, mem, command, number of commands] for
    a given command (e.g. python3 pilot3/pilot.py).

    :param cmd: command (str)
    :param user: user (str)
    :param pid: process id (int)
    :return: list with process info (l[0]=cpu usage(%), l[1]=mem usage(%), l[2]=command(string)) (list).
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
    """
    List all processes and threads owned by the current user.

    This function corresponds to the command "ps -eo pid,ppid -m".

    :return: list of processes and threads (list).
    """
    if not _is_psutil_available:
        logger.warning('psutil not available, cannot check pilot CPU load')
        return []

    current_user = getpass.getuser()
    processes = []
    # Gather only processes owned by the current user (and skip PID 1)
    for proc in psutil.process_iter(attrs=['pid', 'ppid', 'username']):
        try:
            info = proc.info
            if info.get('username') != current_user:
                continue
            if info['pid'] == 1:
                continue
            processes.append((info['pid'], info['ppid'], proc))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    # Sort by PID so the output order roughly matches ps
    processes.sort(key=lambda x: x[0])

    lines = []
    lines.append(f"{'PID':>6} {'PPID':>6}")
    for pid, ppid, proc in processes:
        ppid_str = str(ppid) if ppid is not None else '-'
        # Print the main process line
        lines.append(f"{pid:6} {ppid_str:6}")
        # Try to fetch threads (if available)
        try:
            threads = proc.threads()
        except (psutil.AccessDenied, psutil.NoSuchProcess):
            threads = []
        # Filter out the main thread (whose id equals the process id)
        extra_threads = [t for t in threads if t.id != pid]
        if extra_threads:
            # Mimic ps -m: print one extra line with dashes for threads
            lines.append(f"{'-':6} {'-':6}")

    return lines


def get_clock_speed() -> float or None:
    """
    Return the clock speed in MHz.

    :return: clock speed (float or None).
    """
    if not _is_psutil_available:
        logger.warning('get_clock_speed(): psutil not available - aborting')
        return None

    freq = psutil.cpu_freq()  # scpufreq(current=2300, min=2300, max=2300)
    return freq.current if freq is not None else 0.0
