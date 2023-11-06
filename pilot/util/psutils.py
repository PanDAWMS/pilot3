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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

from re import findall
import os
import subprocess
try:
    import psutil
except ImportError:
    print('FAILED; psutil module could not be imported')
    _is_psutil_available = False
else:
    _is_psutil_available = True

# from pilot.common.exception import MiddlewareImportFailure

import logging
logger = logging.getLogger(__name__)


def is_process_running_by_pid(pid):
    return os.path.exists(f"/proc/{pid}")


def is_process_running(pid):
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
    else:
        return psutil.pid_exists(pid)


def get_pid(jobpid):
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


def find_pid_by_command_and_ppid(command, payload_pid):
    """
    Find the process id corresponding to the given command, and ensure that it belongs to the given payload.

    :param command: command (string)
    :param payload_pid: payload process id (int)
    :return: process id (int) or None
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
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return None


def get_parent_pid(pid):
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


def get_child_processes(parent_pid):
    """
    Return a list of all child processes belonging to the same parent process id.
    Using a fallback to /proc/{pid} in case psutil is not available.

    :param parent_pid: parent process id (int)
    :return: child processes (list).
    """

    if not _is_psutil_available:
        logger.warning('get_child_processes(): psutil not available - using legacy code as a fallback')
        return get_child_processes_legacy(parent_pid)
    else:
        return get_all_descendant_processes(parent_pid)


def get_all_descendant_processes(parent_pid, top_pid=os.getpid()):
    """
    Recursively find child processes using the given parent pid as a starting point.

    :param parent_pid: parent process id (int)
    :param top_pid: do not include os.getpid() in the list (int)
    :return: descendant process ids and cmdline (list).
    """

    def find_descendant_processes(pid, top_pid):
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
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return []

    all_descendant_processes = find_descendant_processes(parent_pid, top_pid)
    return all_descendant_processes


def get_child_processes_legacy(parent_pid):
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


def get_subprocesses(pid, debug=False):
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
