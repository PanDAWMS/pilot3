#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

import os
try:
    import psutil
except ImportError:
    print('FAILED; psutil module could not be imported')
    is_psutil_available = False
else:
    is_psutil_available = True

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

    if not is_psutil_available:
        is_running = is_process_running_by_pid(pid)
        logger.warning(f'using /proc/{pid} instead of psutil (is_running={is_running})')
        return is_running
        # raise MiddlewareImportFailure("required dependency could not be imported: psutil")
    else:
        return psutil.pid_exists(pid)


def find_pid_by_command_and_ppid(command, payload_pid):
    """
    Find the process id corresponding to the given command, and ensure that it belongs to the given payload.

    :param command: command (string)
    :param payload_pid: payload process id (int)
    :return: process id (int) or None
    """

    if not is_psutil_available:
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
