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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

import os
import subprocess
import logging
import shlex
import threading

from os import environ, getcwd, getpgid, kill  #, setpgrp, getpgid  #setsid
from time import sleep
from signal import SIGTERM, SIGKILL
from typing import Any, TextIO

from pilot.common.errorcodes import ErrorCodes
#from pilot.util.loggingsupport import flush_handler
from pilot.util.processgroups import kill_process_group

logger = logging.getLogger(__name__)
errors = ErrorCodes()

# Define a global lock for synchronization
execute_lock = threading.Lock()


def execute(executable: Any, **kwargs: dict) -> Any:
    """
    Execute the command and its options in the provided executable list.
    The function also determines whether the command should be executed within a container.

    :param executable: command to be executed (string or list).
    :param kwargs: kwargs (dict)
    :return: exit code (int), stdout (str) and stderr (str) (or process if requested via returnproc argument)
    """

    usecontainer = kwargs.get('usecontainer', False)
    job = kwargs.get('job')
    #shell = kwargs.get("shell", False)
    obscure = kwargs.get('obscure', '')  # if this string is set, hide it in the log message

    # convert executable to string if it is a list
    if isinstance(executable, list):
        executable = ' '.join(executable)

    # switch off pilot controlled containers for user defined containers
    if job and job.imagename != "" and "runcontainer" in executable:
        usecontainer = False
        job.usecontainer = usecontainer

    # Import user specific code if necessary (in case the command should be executed in a container)
    # Note: the container.wrapper() function must at least be declared
    if usecontainer:
        executable, diagnostics = containerise_executable(executable, **kwargs)
        if not executable:
            return None if kwargs.get('returnproc', False) else -1, "", diagnostics

    if not kwargs.get('mute', False):
        print_executable(executable, obscure=obscure)

    # always use a timeout to prevent stdout buffer problem in nodes with lots of cores
    timeout = get_timeout(kwargs.get('timeout', None))

    exe = ['/usr/bin/python'] + executable.split() if kwargs.get('mode', 'bash') == 'python' else ['/bin/bash', '-c', executable]

    # try: intercept exception such as OSError -> report e.g. error.RESOURCEUNAVAILABLE: "Resource temporarily unavailable"
    exit_code = 0
    stdout = ''
    stderr = ''

    # Acquire the lock before creating the subprocess
    with execute_lock:
        process = subprocess.Popen(exe,
                                   bufsize=-1,
                                   stdout=kwargs.get('stdout', subprocess.PIPE),
                                   stderr=kwargs.get('stderr', subprocess.PIPE),
                                   cwd=kwargs.get('cwd', getcwd()),
                                   preexec_fn=os.setsid,    # setpgrp
                                   encoding='utf-8',
                                   errors='replace')
        if kwargs.get('returnproc', False):
            return process

        try:
            logger.debug(f'subprocess.communicate() will use timeout={timeout} s')
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            # make sure that stdout buffer gets flushed - in case of time-out exceptions
            # flush_handler(name="stream_handler")
            stderr += f'subprocess communicate sent TimeoutExpired: {exc}'
            logger.warning(stderr)
            exit_code = errors.COMMANDTIMEDOUT
            stderr = kill_all(process, stderr)
        except Exception as exc:
            logger.warning(f'exception caused when executing command: {executable}: {exc}')
            exit_code = errors.UNKNOWNEXCEPTION
            stderr = kill_all(process, str(exc))
        else:
            exit_code = process.poll()

    # wait for the process to finish
    # (not strictly necessary when process.communicate() is used)
    try:
        # wait for the process to complete with a timeout of 60 seconds
        process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        # Handle the case where the process did not complete within the timeout
        print("process did not complete within the timeout of 60s - terminating")
        process.terminate()

    # remove any added \n
    if stdout and stdout.endswith('\n'):
        stdout = stdout[:-1]

    return exit_code, stdout, stderr


def execute2(executable: Any, stdout_file: TextIO, stderr_file: TextIO, timeout_seconds: int, **kwargs: dict) -> int:

    exit_code = None

    def _timeout_handler():
        # This function is called when the timeout occurs
        nonlocal exit_code  # Use nonlocal to modify the outer variable
        logger.warning("subprocess execution timed out")
        exit_code = -2
        process.terminate()  # Terminate the subprocess if it's still running
        logger.info(f'process terminated after {timeout_seconds}s')

    obscure = kwargs.get('obscure', '')  # if this string is set, hide it in the log message
    if not kwargs.get('mute', False):
        print_executable(executable, obscure=obscure)

    exe = ['/usr/bin/python'] + executable.split() if kwargs.get('mode', 'bash') == 'python' else ['/bin/bash', '-c', executable]

    # Create the subprocess with stdout and stderr redirection to files
    process = subprocess.Popen(exe,
                               stdout=stdout_file,
                               stderr=stderr_file,
                               cwd=kwargs.get('cwd', os.getcwd()),
                               preexec_fn=os.setsid,
                               encoding='utf-8',
                               errors='replace')

    # Set up a timer for the timeout
    timeout_timer = threading.Timer(timeout_seconds, _timeout_handler)

    try:
        # Start the timer
        timeout_timer.start()

        # wait for the process to finish
        try:
            # wait for the process to complete with a timeout (this will likely never happen since a timer is used)
            process.wait(timeout=timeout_seconds + 10)
        except subprocess.TimeoutExpired:
            # Handle the case where the process did not complete within the timeout
            timeout_seconds = timeout_seconds + 10
            logger.warning(f"process wait did not complete within the timeout of {timeout_seconds}s - terminating")
            exit_code = -2
            process.terminate()
    except Exception as exc:
        logger.warning(f'execution caught: {exc}')
    finally:
        # Cancel the timer to avoid it firing after the subprocess has completed
        timeout_timer.cancel()

    if exit_code == -2:
        # the process was terminated due to a time-out
        exit_code = errors.COMMANDTIMEDOUT
    else:
        # get the exit code after a normal finish
        exit_code = process.returncode

    return exit_code


def get_timeout(requested_timeout: int) -> int:
    """
    Define the timeout to be used with subprocess.communicate().

    If no timeout was requested by the execute() caller, a large default 10 days timeout will be returned.
    It is better to give a really large timeout than no timeout at all, since the subprocess python module otherwise
    can get stuck processing stdout on nodes with many cores.

    :param requested_timeout: timeout in seconds set by execute() caller (int)
    :return: timeout in seconds (int).
    """

    return requested_timeout if requested_timeout else 10 * 24 * 60 * 60  # using a ridiculously large default timeout


def execute_command(command: str) -> str:
    """
    Executes a command using subprocess without using the shell.

    :param command: The command to execute.

    :return: The output of the command (string).
    """

    try:
        logger.info(f'executing command: {command}')
        command = shlex.split(command)
        proc = subprocess.Popen(command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        proc.wait()
        #output, err = proc.communicate()
        exit_code = proc.returncode
        logger.info(f'command finished with exit code: {exit_code}')
        # output = subprocess.check_output(command, text=True)
    except subprocess.CalledProcessError as exc:
        logger.warning(f"error executing command:\n{command}\nexit code: {exc.returncode}\nStderr: {exc.stderr}")
        exit_code = exc.returncode
    return exit_code


def kill_all(process: Any, stderr: str) -> str:
    """
    Kill all processes after a time-out exception in process.communication().

    :param process: process object
    :param stderr: stderr (string)
    :return: stderr (str).
    """

    try:
        logger.warning('killing lingering subprocess and process group')
        sleep(1)
        # process.kill()
        kill_process_group(getpgid(process.pid))
    except ProcessLookupError as exc:
        stderr += f'\n(kill process group) ProcessLookupError={exc}'
    except Exception as exc:
        stderr += f'\n(kill_all 1) exception caught: {exc}'
    try:
        logger.warning('killing lingering process')
        sleep(1)
        kill(process.pid, SIGTERM)
        logger.warning('sleeping a bit before sending SIGKILL')
        sleep(10)
        kill(process.pid, SIGKILL)
    except ProcessLookupError as exc:
        stderr += f'\n(kill process) ProcessLookupError={exc}'
    except Exception as exc:
        stderr += f'\n(kill_all 2) exception caught: {exc}'
    logger.warning(f'sent soft kill signals - final stderr: {stderr}')
    return stderr


def print_executable(executable: str, obscure: str = '') -> None:
    """
    Print out the command to be executed, omitting any secrets.
    Any S3_SECRET_KEY=... parts will be removed.

    :param executable: executable (string).
    :param obscure: sensitive string to be obscured before dumping to log (string)
    """

    executable_readable = executable
    for sub_cmd in executable_readable.split(";"):
        if 'S3_SECRET_KEY=' in sub_cmd:
            secret_key = sub_cmd.split('S3_SECRET_KEY=')[1]
            secret_key = 'S3_SECRET_KEY=' + secret_key
            executable_readable = executable_readable.replace(secret_key, 'S3_SECRET_KEY=********')
    if obscure:
        executable_readable = executable_readable.replace(obscure, '********')

    logger.info(f'executing command: {executable_readable}')


def containerise_executable(executable: str, **kwargs: dict) -> (Any, str):
    """
    Wrap the containerisation command around the executable.

    :param executable: command to be wrapper (string)
    :param kwargs: kwargs dictionary
    :return: containerised executable (list or None), diagnostics (str).
    """

    job = kwargs.get('job')

    user = environ.get('PILOT_USER', 'generic').lower()  # TODO: replace with singleton
    container = __import__(f'pilot.user.{user}.container', globals(), locals(), [user], 0)
    if container:
        # should a container really be used?
        do_use_container = job.usecontainer if job else container.do_use_container(**kwargs)
        # overrule for event service
        if job and job.is_eventservice and do_use_container and environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') != 'raythena':
            logger.info('overruling container decision for event service grid job')
            do_use_container = False

        if do_use_container:
            diagnostics = ""
            try:
                executable = container.wrapper(executable, **kwargs)
            except Exception as exc:
                diagnostics = f'failed to execute wrapper function: {exc}'
                logger.fatal(diagnostics)
            else:
                if executable == "":
                    diagnostics = 'failed to prepare container command (error code should have been set)'
                    logger.fatal(diagnostics)
            if diagnostics != "":
                return None, diagnostics
        else:
            logger.info('pilot user container module has decided to not use a container')
    else:
        logger.warning('container module could not be imported')

    return executable, ""
