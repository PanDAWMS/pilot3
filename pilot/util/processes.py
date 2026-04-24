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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-25

"""Process management utilities: monitoring, killing, and resource-usage tracking."""

import logging
import os
import time
import signal
import re
import threading
from typing import Optional, Union

from pilot.info import JobData
from pilot.util.container import execute
from pilot.util.auxiliary import (
    whoami,
    grep_str
)
from pilot.util.filehandling import (
    read_file,
    remove_dir_tree
)
from pilot.util.processgroups import kill_process_group
from pilot.util.psutils import list_processes_and_threads
from pilot.util.timer import timeout

logger = logging.getLogger(__name__)


def find_processes_in_group(cpids: list, pid: int, ps_cache: str = ""):
    """Find all processes that belong to the same group using the given ps command output.

    Search for the children processes belonging to pid and return their pid's.
    pid is the parent pid and cpids is a list that has to be initialized before
    calling this function and it contains the pids of the children AND the parent.

    ps_cache is expected to be the output from the command "ps -eo pid,ppid -m".

    The cpids input parameter list gets updated in the function.

    Args:
        cpids: List of pid's for all child processes to the parent pid, as well
            as the parent pid itself.
        pid: Parent process id.
        ps_cache: ps command output.
    """
    visited = set()
    stack = [pid]

    while stack:
        current_pid = stack.pop()
        if current_pid in visited:
            continue
        visited.add(current_pid)
        cpids.append(current_pid)
        lines = grep_str([str(current_pid)], ps_cache)

        if lines and lines != ['']:
            for line in lines:
                try:
                    thispid, thisppid = [int(x) for x in line.split()[:2]]
                except Exception as error:
                    logger.warning(f'exception caught: {error}')
                else:
                    if thisppid == current_pid:
                        stack.append(thispid)


def find_processes_in_group_old(cpids: list, pid: int, ps_cache: str = ""):
    """Find all processes that belong to the same group (recursive version).

    Recursively search for the children processes belonging to pid and return
    their pid's. pid is the parent pid and cpids is a list that has to be
    initialized before calling this function and it contains the pids of the
    children AND the parent.

    ps_cache is expected to be the output from the command "ps -eo pid,ppid -m".

    The cpids input parameter list gets updated in the function.

    Args:
        cpids: List of pid's for all child processes to the parent pid, as well
            as the parent pid itself.
        pid: Parent process id.
        ps_cache: ps command output.
    """
    if pid:
        cpids.append(pid)
        lines = grep_str([str(pid)], ps_cache)

        if lines and lines != ['']:
            for _, line in enumerate(lines):
                try:
                    thispid, thisppid = [int(x) for x in line.split()[:2]]
                except Exception as error:
                    logger.warning(f'exception caught: {error}')
                else:
                    if thisppid == pid:
                        find_processes_in_group(cpids, thispid, ps_cache)


def is_zombie(pid: int) -> bool:
    """Check if the given process is a zombie process.

    Args:
        pid: Process id.

    Returns:
        True if process is defunct, False otherwise.
    """
    status = False

    cmd = f"ps aux | grep {pid}"
    _, stdout, _ = execute(cmd, mute=True)
    if "<defunct>" in stdout:
        status = True

    return status


def get_process_commands(euid: int, pids: list) -> list:
    """Return a list of process commands corresponding to a pid list for user euid.

    Args:
        euid: User id.
        pids: List of process id's.

    Returns:
        List of process commands.
    """
    cmd = f'ps u -u {euid}'
    process_commands = []
    exit_code, stdout, stderr = execute(cmd, mute=True)

    if exit_code != 0 or stdout == '':
        logger.warning(f'ps command failed: {exit_code}, \"{stdout}\", \"{stderr}\"')
    else:
        # extract the relevant processes
        p_commands = stdout.split('\n')
        first = True
        for p_command in p_commands:
            if first:
                # get the header info line
                process_commands.append(p_command)
                first = False
            else:
                # remove extra spaces
                _p_command = p_command
                while "  " in _p_command:
                    _p_command = _p_command.replace("  ", " ")
                items = _p_command.split(" ")
                for pid in pids:
                    # items = username pid ...
                    if items[1] == str(pid):
                        process_commands.append(p_command)
                        break

    return process_commands


def dump_stack_trace(pid: int) -> None:
    """Execute the stack trace command (pstack <pid>).

    Args:
        pid: Process id.
    """
    # make sure that the process is not in a zombie state
    if not is_zombie(pid):
        cmd = f"pstack {pid}"
        _, stdout, _ = execute(cmd, mute=True, timeout=60)
        logger.info(stdout or "(pstack returned empty string)")
    else:
        logger.info("skipping pstack dump for zombie process")


def get_ps_cache() -> str:
    """Return the "ps -eo pid,ppid -m" command output.

    The psutil alternative is preferred when available.

    Returns:
        ps command output.
    """
    _ps_cache = list_processes_and_threads()
    if _ps_cache:
        ps_cache = "\n".join(_ps_cache)
    else:
        _, ps_cache, _ = execute("ps -eo pid,ppid -m", mute=True)

    return ps_cache


def kill_processes(pid: int, korphans: bool = True, ps_cache: str = None, nap: int = 10) -> None:
    """Kill processes belonging to the process group that the given pid belongs to.

    Args:
        pid: Process id.
        korphans: Kill orphans.
        ps_cache: ps command output.
        nap: Napping time between kill signals in seconds.
    """
    # if there is a known subprocess pgrp, then it should be enough to kill the group in one go
    status = False
    try:
        pgrp = os.getpgid(pid)
    except ProcessLookupError:
        pgrp = 0
    if pgrp != 0:
        status = kill_process_group(pgrp, nap=nap)

    if not status:
        # firstly find all the children process IDs to be killed
        children = []
        if not ps_cache:
            ps_cache = get_ps_cache()
        find_processes_in_group(children, pid, ps_cache)

        # reverse the process order so that the athena process is killed first (otherwise the stdout will be truncated)
        if not children:
            return

        children.reverse()
        logger.info("process IDs to be killed: %s (in reverse order)", str(children))

        # find which commands are still running
        try:
            cmds = get_process_commands(os.geteuid(), children)
        except Exception as error:
            logger.warning("get_process_commands() threw an exception: %s", error)
        else:
            if len(cmds) <= 1:
                logger.warning("found no corresponding commands to process id(s)")
            else:
                logger.info("found commands still running:")
                for cmd in cmds:
                    logger.info(cmd)

                # loop over all child processes
                for i in children:
                    # dump the stack trace before killing it
                    dump_stack_trace(i)

                    # kill the process gracefully
                    kill_process(i)

    # kill any remaining orphan processes
    # note: this should no longer be necessary since ctypes has made sure all subprocesses are parented
    # if orphan process killing is not desired, set env var PILOT_NOKILL
    if korphans:
        kill_orphans()

    # kill any lingering defunct processes
    try:
        kill_defunct_children(pid)
    except Exception as exc:
        logger.warning(f'exception caught: {exc}')


def kill_defunct_children(pid: int) -> None:
    """Kill any defunct child processes of the specified process ID.

    Args:
        pid: Process id.
    """
    defunct_children = []
    for proc in os.listdir("/proc"):
        if proc.isdigit():
            try:
                cmdline = os.readlink(f"/proc/{proc}/cmdline")
            except OSError:
                # ignore lines that do not have cmdline and proc 1
                continue
            if not cmdline or cmdline.startswith("/bin/init"):
                continue
            pinfo = os.readlink(f"/proc/{proc}/status")
            if pinfo.startswith("Z") and os.readlink(f"/proc/{proc}/parent") == str(pid):
                defunct_children.append(int(proc))

    if defunct_children:
        logger.info(f'will now remove defunct processes: {defunct_children}')
    else:
        logger.info(f'did not find any defunct processes belonging to {pid}')
    for child_pid in defunct_children:
        try:
            os.kill(child_pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def kill_child_processes(pid: int, ps_cache: str = None) -> None:
    """Kill child processes.

    Args:
        pid: Process id.
        ps_cache: ps command output.
    """
    # firstly find all the children process IDs to be killed
    children = []
    if not ps_cache:
        ps_cache = get_ps_cache()
    find_processes_in_group(children, pid, ps_cache)

    # reverse the process order so that the athena process is killed first (otherwise the stdout will be truncated)
    children.reverse()
    logger.info(f"process IDs to be killed: {children} (in reverse order)")

    # find which commands are still running
    try:
        cmds = get_process_commands(os.geteuid(), children)
    except Exception as error:
        logger.warning(f"get_process_commands() threw an exception: {error}")
    else:
        if len(cmds) <= 1:
            logger.warning("found no corresponding commands to process id(s)")
        else:
            logger.info("found commands still running:")
            for cmd in cmds:
                logger.info(cmd)

            # loop over all child processes
            for i in children:
                # dump the stack trace before killing it
                dump_stack_trace(i)

                # kill the process gracefully
                kill_process(i)


def kill_process(pid: int, hardkillonly: bool = False) -> bool:
    """Kill process.

    Args:
        pid: Process id.
        hardkillonly: Only execute the hard kill.

    Returns:
        True if successful (SIGKILL), False otherwise.
    """
    # start with soft kill (ignore any returned status)
    if not hardkillonly:
        kill(pid, signal.SIGTERM)

        _t = 3
        logger.info(f"sleeping {_t} s to allow process to exit")
        time.sleep(_t)

    # now do a hard kill just in case some processes haven't gone away
    status = kill(pid, signal.SIGKILL)

    return status


def kill(pid: int, sig: int) -> bool:
    """Kill the given process with the given signal.

    Args:
        pid: Process id.
        sig: Signal.

    Returns:
        True when successful, False otherwise.
    """
    status = False
    try:
        os.kill(pid, sig)
    except OSError as error:
        logger.warning(f"exception thrown when killing process {pid} with signal={sig}: {error}")
    else:
        logger.info(f"killed process {pid} with signal={sig}")
        status = True

    return status


# called checkProcesses() in Pilot 1, used by process monitoring
def get_number_of_child_processes(pid: int) -> int:
    """Get the number of child processes for a given parent process.

    Args:
        pid: Parent process id.

    Returns:
        Number of child processes.
    """
    children = []
    n = 0
    try:
        ps_cache = get_ps_cache()
        find_processes_in_group(children, pid, ps_cache)
    except Exception as error:
        logger.warning(f"exception caught in find_processes_in_group: {error}")
    else:
        if pid:
            n = len(children)
            logger.info(f"number of running child processes to parent process {pid}: {n}")
        else:
            logger.debug("pid not yet set")
    return n


def killpg(pid: Union[int, str], sig: int) -> None:
    """Kill given process group with given signal.

    Args:
        pid: Process group id.
        sig: Signal.
    """
    try:
        _pid = int(pid) if isinstance(pid, str) else pid
        os.killpg(_pid, sig)
    except (ProcessLookupError, PermissionError, ValueError) as error:
        logger.warning(f"failed to execute killpg(): {error}")
        cmd = f'kill -{sig} {pid}'
        exit_code, rs, _ = execute(cmd)
        if exit_code != 0:
            logger.warning(rs)
        else:
            logger.info(f"killed orphaned process {pid}")
    else:
        logger.info(f"killed orphaned process group {pid}")


def get_pilot_pid_from_processes(ps_processes: str, pattern: re.Pattern) -> Optional[int]:
    """Identify the pilot pid from the list of processes.

    Args:
        ps_processes: ps output.
        pattern: Regex pattern.

    Returns:
        Pilot pid, or None if not found.
    """
    pilot_pid = None
    for line in ps_processes.split('\n'):
        ids = pattern.search(line)
        if ids:
            _pid = ids.group(1)
            args = ids.group(3)
            try:
                pid = int(_pid)
            except (ValueError, TypeError) as error:
                logger.warning(f'failed to convert pid to int: {error}')
                continue
            if 'pilot.py' in args and 'python' in args:
                pilot_pid = pid
                break

    return pilot_pid


def kill_orphans() -> None:
    """Find and kill all orphan processes belonging to current pilot user."""
    # exception for BOINC
    if 'BOINC' in os.environ.get('PILOT_SITENAME', ''):
        logger.info("Do not look for orphan processes in BOINC jobs")
        return

    if 'PILOT_NOKILL' in os.environ:
        return

    logger.info("searching for orphan processes")

    cmd = f"ps -o pid,ppid,args -u {whoami()}"
    _, _processes, _ = execute(cmd)
    pattern = re.compile(r'(\d+)\s+(\d+)\s+([\S\s]+)')

    count = 0
    for line in _processes.split('\n'):
        ids = pattern.search(line)
        if ids:
            _pid = ids.group(1)
            ppid = ids.group(2)
            args = ids.group(3)
            try:
                pid = int(_pid)
            except (ValueError, TypeError) as error:
                logger.warning(f'failed to convert pid to int: {error}')
                continue
            if 'cvmfs2' in args:
                logger.info(f"ignoring possible orphan process running cvmfs2: pid={pid}, ppid={ppid}, args='{args}'")
            elif 'pilots_starter.py' in args or 'runpilot2-wrapper.sh' in args or 'runpilot3-wrapper.sh' in args:
                logger.info(f"ignoring pilot launcher: pid={pid}, ppid={ppid}, args='{args}'")
            elif ppid == '1':
                count += 1
                logger.info(f"found orphan process: pid={pid}, ppid={ppid}, args='{args}'")
                if 'bash' in args or ('python' in args and 'pilot.py' in args):
                    logger.info("will not kill bash process")
                else:
                    killpg(pid, signal.SIGTERM)
                    _t = 10
                    logger.info(f"sleeping {_t} s to allow processes to exit")
                    time.sleep(_t)
                    killpg(pid, signal.SIGKILL)

    if count == 0:
        logger.info("did not find any orphan processes")
    else:
        logger.info(f"found {count} orphan process" + "es" if count > 1 else "")


def get_max_memory_usage_from_cgroups() -> Optional[int]:
    """Read the max memory from the CGROUPS file memory.max_usage_in_bytes.

    Returns:
        Max memory in bytes, or None if not available.
    """
    max_memory = None

    # Get the CGroups max memory using the pilot pid
    pid = os.getpid()
    path = f"/proc/{pid}/cgroup"
    if os.path.exists(path):
        cmd = f"grep memory {path}"
        _, out, _ = execute(cmd)
        if out == "":
            logger.info("(command did not return anything)")
        else:
            logger.info(out)
            if ":memory:" in out:
                pos = out.find('/')
                path = out[pos:]
                logger.info(f"extracted path {path}")

                pre = get_cgroups_base_path()
                if pre != "":
                    path = pre + os.path.join(path, "memory.max_usage_in_bytes")
                    logger.info(f"path to CGROUPS memory info: {path}")
                    max_memory = read_file(path)
                else:
                    logger.info("CGROUPS base path could not be extracted - not a CGROUPS site")
            else:
                logger.warning(f"invalid format: {out} (expected ..:memory:[path])")
    else:
        logger.info(f"path {path} does not exist (not a CGROUPS site)")

    return max_memory


def get_cgroups_base_path() -> str:
    """Return the base path for CGROUPS.

    Returns:
        Base path for CGROUPS.
    """
    cmd = "grep \'^cgroup\' /proc/mounts|grep memory| awk \'{print $2}\'"
    _, base_path, _ = execute(cmd, mute=True)

    return base_path


def get_cpu_consumption_time(t0: tuple) -> float:
    """Return the CPU consumption time for child processes.

    Measured by system+user time from os.times(). The os.times() tuple contains:
    user time, system time, s user time, s system time, and elapsed real time
    since a fixed point in the past.

    Args:
        t0: Initial os.times() tuple prior to measurement.

    Returns:
        System+user time for child processes.
    """
    t1 = os.times()
    user_time = t1[2] - t0[2]
    system_time = t1[3] - t0[3]

    return user_time + system_time


def get_instant_cpu_consumption_time(pid: int) -> float:
    """Return the CPU consumption time (system+user time) for a given process.

    Parses /proc/pid/stat. Returns 0.0 if the pid is not set. Sums up all the
    user+system times for both the main process (pid) and the child processes,
    since the main process is most likely spawning new processes.

    Args:
        pid: Process id.

    Returns:
        System+user time for a given pid.
    """
    utime = None
    stime = None
    cutime = None
    cstime = None

    hz = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
    if not isinstance(hz, int):
        logger.warning(f'unknown SC_CLK_TCK: {hz}')
        return 0.0

    if pid and hz and hz > 0:
        path = f"/proc/{pid}/stat"
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as fp:
                    fields = fp.read().split(' ')[13:17]
                    utime, stime, cutime, cstime = [(float(f) / hz) for f in fields]
            except IOError as exc:
                logger.warning(f'exception caught: {exc} (ignoring process {pid})')
        else:
            logger.debug(f"{path} no longer exist (ignoring terminated process {pid})")

    if utime and stime and cutime and cstime:
        # sum up all the user+system times for both the main process (pid) and the child processes
        cpu_consumption_time = utime + stime + cutime + cstime
    else:
        cpu_consumption_time = 0.0

    return cpu_consumption_time


def get_current_cpu_consumption_time(pid: int) -> float:
    """Get the current CPU consumption time (system+user time) for a given process.

    Loops over all child processes to accumulate the total.

    Args:
        pid: Process id.

    Returns:
        System+user time for a given pid.
    """
    # get all the child processes
    children = []
    ps_cache = get_ps_cache()
    if ps_cache:
        find_processes_in_group(children, pid, ps_cache)
    else:
        logger.warning('failed to get ps_cache')
        return -1

    cpuconsumptiontime = 0
    for _pid in children:
        _cpuconsumptiontime = get_instant_cpu_consumption_time(_pid)
        if _cpuconsumptiontime:
            cpuconsumptiontime += _cpuconsumptiontime

    return cpuconsumptiontime


def is_process_running(process_id: int) -> bool:
    """Check whether process is still running.

    Args:
        process_id: Process id.

    Returns:
        True if process is running, False otherwise.
    """
    try:
        # note that this kill function call will not kill the process
        os.kill(process_id, 0)
        return True
    except OSError:
        return False


def cleanup(job: JobData, args: object) -> None:
    """Cleanup called after completion of job.

    Args:
        job: Job object.
        args: Pilot args object.
    """
    logger.info("overall cleanup function is called")

    # make sure the workdir is deleted
    if args.cleanup:
        if remove_dir_tree(job.workdir):
            logger.info(f'removed {job.workdir}')

        if os.path.exists(job.workdir):
            logger.warning(f'work directory still exists: {job.workdir}')
        else:
            logger.debug(f'work directory was removed: {job.workdir}')
    else:
        logger.info(f'workdir not removed {job.workdir}')

    # collect any zombie processes
    job.collect_zombies(depth=10)
    logger.info("collected zombie processes")

    if job.pid:
        logger.info(f"will attempt to kill all subprocesses of pid={job.pid}")
        kill_processes(job.pid)
    else:
        logger.warning('cannot kill any subprocesses since job.pid is not set')
    #logger.info("deleting job object")
    #del job


def threads_aborted(caller: str = '') -> bool:
    """Check if the Pilot threads have been aborted.

    Counts all threads still running, but only returns True if all threads
    started by the Pilot's main thread have finished — not including the main
    thread itself or any daemon threads (which might be created by Rucio or
    Google Logging).

    Args:
        caller: Caller name.

    Returns:
        True if number of running pilot threads is zero, False otherwise.
    """
    abort = False
    #thread_count = threading.activeCount()
    pilot_thread_count = 0
    daemon_threads = 0
    main_thread_count = 0

    # count all threads still alive
    names = []
    time.sleep(1)
    for thread in threading.enumerate():
        if thread.isDaemon():  # ignore any daemon threads, they will be aborted when python ends
            daemon_threads += 1
            #tag = 'daemon'
        elif thread == threading.main_thread():
            main_thread_count += 1
            #tag = 'main'
            names.append(f'{thread}')
        else:  # only count threads spawned by the main thread, no the main thread itself or any daemon threads
            pilot_thread_count += 1
            #tag = 'pilot?'
            names.append(f'{thread}')
        #logger.debug(f'thread={thread},'
        #             f'caller={caller}, '
        #             f'pilot_thread_count={pilot_thread_count}, '
        #             f'daemon_thread_count={daemon_threads}, '
        #             f'main_thread_count={main_thread_count}, '
        #             f'names={names}, '
        #             f'tag={tag}')
    #if pilot_thread_count == 0:
    #    logger.debug(f'caller={caller}, main_thread_count={main_thread_count}')
    #    logger.debug(f'aborting since only the main Pilot thread is still running '
    #                 f'(total thread count={thread_count} with {daemon_threads} daemon thread(s): names={names}')
    #    abort = True
    if pilot_thread_count == 0 and caller:  # and caller != 'run':
        if caller in names[0] or caller == 'run':
            logger.info(f'caller={caller} is remaining thread - safe to abort (names={names})')
            abort = True
    elif pilot_thread_count == 0:
        logger.info(f'safe to abort? (names={names})')
        abort = True
    elif pilot_thread_count == 1:
        mon = [thread for thread in names if ('monitor' in thread and '_monitor' not in thread)]  # exclude job_monitor and queue_monitor(ing)
        if mon:
            logger.info(f'only monitor.control thread still running - safe to abort: {names}')
            abort = True
        else:
            logger.info(f'waiting for thread to finish: {names}')

    return abort


def convert_ps_to_dict(output: str, pattern: str = r'(\d+) (\d+) (\d+) (.+)') -> dict:
    """Convert output from a ps command to a dictionary.

    Example::

        ps axo pid,ppid,pgid,cmd
          PID  PPID  PGID COMMAND
          22091  6672 22091 bash
          32581 22091 32581 ps something;sdfsdfds/athena.py ddfg

        -> {'PID': [22091, 32581], 'PPID': [6672, 22091], ..., 'COMMAND': ['bash', 'ps something;...']}

    Args:
        output: ps stdout.
        pattern: Regex pattern matching the ps output.

    Returns:
        Dictionary with ps output.
    """
    dictionary = {}
    first_line = []  # e.g. PID PPID PGID COMMAND

    for line in output.split('\n'):
        try:
            # remove leading and trailing spaces
            line = line.strip()
            # remove multiple spaces inside the line
            _l = re.sub(' +', ' ', line)

            if not first_line:
                _l = [_f for _f in _l.split(' ') if _f]
                first_line = _l
                for i, item in enumerate(_l):
                    dictionary[item] = []
            else:  # e.g. 22091 6672 22091 bash
                match = re.search(pattern, _l)
                if match:
                    for i, key in enumerate(first_line):
                        try:
                            var = int(match.group(i + 1))
                        except (ValueError, TypeError):
                            var = match.group(i + 1)
                        dictionary[key].append(var)

        except (ValueError, IndexError, KeyError, AttributeError, re.error) as error:
            print(f"unexpected format of utility output: {error}")

    return dictionary


def get_trimmed_dictionary(keys: list, dictionary: dict) -> dict:
    """Return a sub-dictionary with only the given keys.

    Args:
        keys: Keys to keep.
        dictionary: Full dictionary.

    Returns:
        Trimmed dictionary containing only the specified keys.
    """
    subdictionary = {}
    for key in keys:
        if key in dictionary:
            subdictionary[key] = dictionary[key]

    return subdictionary


def find_cmd_pids(cmd: str, ps_dictionary: dict) -> list:
    """Find all pids for the given command.

    Example: ``cmd = 'athena.py'`` returns ``[1234, 2267]`` if two pilots are
    running on the worker node.

    Args:
        cmd: Command.
        ps_dictionary: Converted ps output.

    Returns:
        List of pids.
    """
    pids = []
    i = -1
    for _cmd in ps_dictionary.get('COMMAND'):
        i += 1
        if cmd in _cmd:
            pids.append(ps_dictionary.get('PID')[i])

    return pids


def find_pid(pandaid: int, ps_dictionary: dict) -> int:
    """Find the process id for the command that contains 'export PandaID=<pandaid>'.

    Args:
        pandaid: PanDA ID.
        ps_dictionary: ps output dictionary.

    Returns:
        Process id, or -1 if not found.
    """
    pid = -1
    i = -1
    pandaid_cmd = f'export PandaID={pandaid}'
    for _cmd in ps_dictionary.get('COMMAND'):
        i += 1
        if pandaid_cmd in _cmd:
            pid = ps_dictionary.get('PID')[i]
            break

    return pid


def is_child(pid: int, pandaid_pid: int, dictionary: dict) -> bool:
    """Check if the given pid is a child process of the pandaid_pid.

    Proceeds recursively until the parent pandaid_pid has been found, or
    returns False if it fails to find it.

    Args:
        pid: Process id.
        pandaid_pid: Parent process id.
        dictionary: ps output dictionary.

    Returns:
        True if process is a child, False otherwise.
    """
    try:
        # where are we at in the PID list?
        index = dictionary.get('PID').index(pid)
    except ValueError:
        # not in the list
        return False

    # get the corresponding ppid
    ppid = dictionary.get('PPID')[index]

    # logger.info(f'checking pid={pid} ppid={ppid} pandaid_pid={pandaid_pid}')
    # is the current parent the same as the pandaid_pid? if yes, we are done
    if ppid == pandaid_pid:
        return True
    # try another pid
    return is_child(ppid, pandaid_pid, dictionary)


def identify_numbers_and_strings(s: str) -> list:
    """Identify numbers and strings in a given string.

    Args:
        s: The string to be processed.

    Returns:
        A list of tuples, where each tuple contains the matched numbers and strings.
    """
    return re.findall(r'(\d+)\s+(\d+)\s+([A-Za-z]+)\s+([A-Za-z]+)', s)


def find_zombies(parent_pid: int) -> dict:
    """Find all zombie/defunct processes under the given parent pid.

    Args:
        parent_pid: Parent pid.

    Returns:
        Dictionary mapping parent pid to list of zombie process info.
    """
    zombies = {}
    cmd = 'ps -eo pid,ppid,stat,comm'
    _, stdout, _ = execute(cmd)
    for line in stdout.split('\n'):
        matches = identify_numbers_and_strings(line)
        if matches:
            pid = int(matches[0][0])
            ppid = int(matches[0][1])
            stat = matches[0][2]
            comm = matches[0][3]
            #print(f'pid={pid} ppid={ppid} stat={stat} comm={comm}')
            if ppid == parent_pid and stat.startswith('Z'):
                if not zombies.get(parent_pid):
                    zombies[parent_pid] = []
                zombies[parent_pid].append([pid, stat, comm])

    return zombies


def handle_zombies(zombies: list, job: JobData = None) -> None:
    """Dump some info about the given zombies.

    Args:
        zombies: List of zombies.
        job: If provided, the zombie pid will be added to the job.zombies list.
    """
    for parent in zombies:
        #logger.info(f'sending SIGCHLD to ppid={parent}')
        #kill(parent, signal.SIGCHLD)
        for zombie in zombies.get(parent):
            pid = zombie[0]
            # stat = zombie[1]
            comm = zombie[2]
            logger.info(f'zombie process {pid} (comm={comm}, ppid={parent})')
            # kill_process(pid, hardkillonly=True)  # useless for zombies - they are already dead
            if job:
                job.zombies.append(pid)


def reap_zombies(pid: int = -1) -> None:
    """Check for and reap zombie processes.

    This function can be called by the monitoring loop. Using PID -1 in
    os.waitpid() means that the request pertains to any child of the current
    process.

    Args:
        pid: Process id. Use -1 for any child of the current process.
    """
    max_timeout = 20

    @timeout(seconds=max_timeout)
    def waitpid(pid: int = -1):
        try:
            while True:
                _pid, status = os.waitpid(pid, os.WNOHANG)
                if _pid == 0:
                    break
                # Handle the terminated process here
                if os.WIFEXITED(status):
                    exit_code = os.WEXITSTATUS(status)
                    logger.info(f'pid={_pid} exited with {exit_code}')
        except ChildProcessError:
            pass
    logger.info(f'reaping zombies for max {max_timeout} seconds')
    waitpid(pid)


def check_proc_access() -> bool:
    """Verify that /proc/self/statm can be accessed.

    Returns:
        True if /proc/self/statm can be accessed, False otherwise.
    """
    try:
        with open('/proc/self/statm', 'r') as f:
            _ = f.read()
        return True
    except (FileNotFoundError, PermissionError) as e:
        logger.warning(f"error accessing /proc/self/statm: {e} (CPU consumption time will be discarded)")
        return False
