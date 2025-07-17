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

"""Functions for executing commands."""

import errno
import os
import subprocess
import logging
import queue
import re
import select
import shlex
import signal
import threading

from os import environ, getcwd, getpgid, kill  #, setpgrp, getpgid  #setsid
from queue import Queue
from signal import SIGTERM, SIGKILL
from time import sleep
from typing import Any, TextIO

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
#from pilot.util.loggingsupport import flush_handler
from pilot.util.cgroups import move_process_and_descendants_to_cgroup
from pilot.util.processgroups import kill_process_group

errors = ErrorCodes()
logger = logging.getLogger(__name__)
pilot_cache = get_pilot_cache()

# Define a global lock for synchronization
execute_lock = threading.Lock()


def execute(executable: Any, **kwargs: dict) -> Any:  # noqa: C901
    """
    Executes the command with its options in the provided executable list using subprocess time-out handler.
    :param executable: command to be executed (str or list)
    :param kwargs: kwargs (dict)
    :return: exit code (int), stdout (str) and stderr (str) (or process if requested via returnproc argument).
    """
    usecontainer = kwargs.get('usecontainer', False)
    job = kwargs.get('job')
    obscure = kwargs.get('obscure', '')  # if this string is set, hide it in the log message

    # Convert executable to string if it is a list
    if isinstance(executable, list):
        executable = ' '.join(executable)

    if job and job.imagename != "" and "runcontainer" in executable:
        usecontainer = False
        job.usecontainer = usecontainer

    if usecontainer:
        executable, diagnostics = containerise_executable(executable, **kwargs)
        if not executable:
            return None if kwargs.get('returnproc', False) else -1, "", diagnostics

    if not kwargs.get('mute', False):
        print_executable(executable, obscure=obscure)

    timeout = get_timeout(kwargs.get('timeout', None))
    exe = ['/usr/bin/python'] + executable.split() if kwargs.get('mode', 'bash') == 'python' else ['/bin/bash', '-c', executable]

    process = None
    try:
        with execute_lock:
            process = subprocess.Popen(
                exe,
                bufsize=-1,
                stdout=kwargs.get('stdout', subprocess.PIPE),
                stderr=kwargs.get('stderr', subprocess.PIPE),
                cwd=kwargs.get('cwd', getcwd()),
                start_new_session=True,
                encoding='utf-8',
                errors='replace'
            )
            # should we create a cgroup for the process and add the pid?
            #if pilot_cache.use_cgroups:  leads to circular import
            #    status = add_process_to_cgroup(process.pid)
            #    if not status:
            #        logger.warning('failed to add process to cgroup')
            #        pilot_cache.use_cgroups = False

            if kwargs.get('returnproc', False):
                return process

        # move the process to the cgroup if cgroups are used
        try:
            if pilot_cache.use_cgroups:
                cgroup_path = pilot_cache.get_cgroup("subprocesses")
                if cgroup_path:
                    logger.info(
                        f"moving process (pid={process.pid}) to cgroup: {cgroup_path}"
                    )
                    _ = move_process_and_descendants_to_cgroup(cgroup_path, process.pid)
                else:
                    logger.warning("cannot move process to cgroup - no cgroup path found")
        except Exception as e:
            logger.warning(f"exception caught when moving process to cgroup: {e}")

        # use communicate() to read stdout and stderr reliably
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            # Timeout handling
            stderr = f'subprocess communicate sent TimeoutExpired: {exc}'
            logger.warning(stderr)
            exit_code = errors.COMMANDTIMEDOUT
            stderr = kill_all(process, stderr)
            return exit_code, "", stderr
        except Exception as exc:
            logger.warning(f'exception caused when executing command: {executable}: {exc}')
            exit_code = errors.UNKNOWNEXCEPTION
            stderr = kill_all(process, str(exc))
            return exit_code, "", stderr

        exit_code = process.poll()
        if stdout and stdout.endswith('\n'):
            stdout = stdout[:-1]

        return exit_code, stdout, stderr

    finally:
        # Ensure the process is cleaned up
        if process and not kwargs.get('returnproc', False):
            try:
                process.wait(timeout=60)
                process.stdout.close()
                process.stderr.close()
            except Exception:
                pass


def execute_old3(executable: Any, **kwargs: dict) -> Any:  # noqa: C901
    """
    Executes the command with its options in the provided executable list using subprocess time-out handler.

    The function also determines whether the command should be executed within a container.

    :param executable: command to be executed (str or list)
    :param kwargs: kwargs (dict)
    :return: exit code (int), stdout (str) and stderr (str) (or process if requested via returnproc argument).
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
    process = None
    with execute_lock:
        process = subprocess.Popen(exe,
                                   bufsize=-1,
                                   stdout=kwargs.get('stdout', subprocess.PIPE),
                                   stderr=kwargs.get('stderr', subprocess.PIPE),
                                   cwd=kwargs.get('cwd', getcwd()),
                                   start_new_session=True,  # alternative to use os.setsid
                                   # preexec_fn=os.setsid,    # setpgrp
                                   encoding='utf-8',
                                   errors='replace')
        if kwargs.get('returnproc', False):
            return process

    # Create threads to read stdout and stderr asynchronously
    stdout_queue = Queue()
    stderr_queue = Queue()

    def read_output(stream, queue):
        while True:
            try:
                # Use select to wait for the stream to be ready for reading
                ready, _, _ = select.select([stream], [], [], 1.0)
                if ready:
                    line = stream.readline()
                    if not line:
                        break
                    try:
                        queue.put_nowait(line)
                    except queue.Full:
                        pass  # Handle the case where the queue is full
                else:
                    sleep(0.01)  # Sleep for a short interval to avoid busy waiting
            except (AttributeError, ValueError):
                break
            except OSError as e:
                if e.errno == errno.EBADF:
                    break
                else:
                    raise

    stdout_thread = threading.Thread(target=read_output, args=(process.stdout, stdout_queue))
    stderr_thread = threading.Thread(target=read_output, args=(process.stderr, stderr_queue))

    # start the threads and use thread synchronization
    with threading.Lock():
        stdout_thread.start()
        stderr_thread.start()

    try:
        logger.debug(f'subprocess.communicate() will use timeout {timeout} s')
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
        #exit_code = process.poll()
        exit_code = process.returncode

    # Wait for the threads to finish reading
    try:
        stdout_thread.join()
        stderr_thread.join()
    except Exception as e:
        logger.warning(f'exception caught in execute: {e}')

    # Read the remaining output from the queues
    while not stdout_queue.empty():
        stdout += stdout_queue.get()
    while not stderr_queue.empty():
        stderr += stderr_queue.get()

    # wait for the process to finish
    # (not strictly necessary when process.communicate() is used)
    try:
        # wait for the process to complete with a timeout of 60 seconds
        if process:
            process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        # Handle the case where the process did not complete within the timeout
        if process:
            logger.warning("process did not complete within the timeout of 60s - terminating")
            process.terminate()

    # remove any added \n
    if stdout and stdout.endswith('\n'):
        stdout = stdout[:-1]

    return exit_code, stdout, stderr


def execute_nothreads(executable: Any, **kwargs: dict) -> Any:
    """
    Execute the command with its options in the provided executable list using subprocess time-out handler.

    The function also determines whether the command should be executed within a container.

    This variant of execute() is not using threads to read stdout and stderr. This is required for some use-cases like
    executing arcproxy where the stdout is time-ordered.

    :param executable: command to be executed (str or list)
    :param kwargs: kwargs (dict)
    :return: exit code (int), stdout (str) and stderr (str) (or process if requested via returnproc argument).
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
    process = None
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
            logger.debug(f'subprocess.communicate() will use timeout {timeout} s')
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
        if process:
            process.wait(timeout=60)
    except subprocess.TimeoutExpired:
        # Handle the case where the process did not complete within the timeout
        if process:
            logger.warning("process did not complete within the timeout of 60s - terminating")
            process.terminate()

    # remove any added \n
    if stdout and stdout.endswith('\n'):
        stdout = stdout[:-1]

    return exit_code, stdout, stderr


def execute2(executable: Any, stdout_file: TextIO, stderr_file: TextIO, timeout_seconds: int, **kwargs: dict) -> int:
    """
    Execute the command with its options in the provided executable list using an internal timeout handler.

    The function also determines whether the command should be executed within a container.

    :param executable: command to be executed (string or list)
    :param kwargs: kwargs (dict)
    :return: exit code (int), stdout (str) and stderr (str) (or process if requested via returnproc argument).
    """
    exit_code = None

    def _timeout_handler():
        # This function is called when the timeout occurs
        nonlocal exit_code  # Use nonlocal to modify the outer variable
        logger.warning("subprocess execution timed out")
        exit_code = -2
        if process:
            process.terminate()  # Terminate the subprocess if it's still running
            logger.info(f'process terminated after {timeout_seconds}s')

    obscure = kwargs.get('obscure', '')  # if this string is set, hide it in the log message
    if not kwargs.get('mute', False):
        print_executable(executable, obscure=obscure)

    exe = ['/usr/bin/python'] + executable.split() if kwargs.get('mode', 'bash') == 'python' else ['/bin/bash', '-c', executable]

    # Create the subprocess with stdout and stderr redirection to files
    # Acquire the lock before creating the subprocess
    process = None
    with execute_lock:
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
        if process:
            exit_code = process.returncode
        else:
            exit_code = -1

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
    Execute a command using subprocess without using the shell.

    :param command: The command to execute (str)
    :return: The output of the command (str).
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

    :param process: process object (Any)
    :param stderr: stderr (str)
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

    :param executable: executable (str)
    :param obscure: sensitive string to be obscured before dumping to log (str).
    """
    executable_readable = executable
    for sub_cmd in executable_readable.split(";"):
        if 'S3_SECRET_KEY=' in sub_cmd:
            secret_key = sub_cmd.split('S3_SECRET_KEY=')[1]
            secret_key = 'S3_SECRET_KEY=' + secret_key
            executable_readable = executable_readable.replace(secret_key, 'S3_SECRET_KEY=********')
    if obscure:
        executable_readable = executable_readable.replace(obscure, '********')

    # also make sure there is no user token present. If so, obscure it as well
    executable_readable = obscure_token(executable_readable)

    logger.info(f'executing command: {executable_readable}')


def containerise_executable(executable: str, **kwargs: dict) -> (Any, str):
    """
    Wrap the containerisation command around the executable.

    :param executable: command to be wrapper (str)
    :param kwargs: kwargs dictionary (dict)
    :return: containerised executable (list or None), diagnostics (str).
    """
    job = kwargs.get('job')
    logger.debug(f'containerising executable called for exe={executable}')

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


def obscure_token(cmd: str) -> str:
    """
    Obscure any user token from the payload command.

    :param cmd: payload command (str)
    :return: updated command (str).
    """
    try:
        match = re.search(r'-p (\S+)\ ', cmd)
        if match:
            cmd = cmd.replace(match.group(1), '********')
    except (re.error, AttributeError, IndexError):
        logger.warning('an exception was thrown while trying to obscure the user token')
        cmd = ''

    return cmd


def execute_command_with_timeout2(command, timeout=30):
    """Executes a command with a timeout.

    Args:
        command: The command to execute as a list of strings.
        timeout: The maximum execution time in seconds.

    Returns:
        A tuple containing the return code of the command and the output.
    """

    # convert to list if necessary
    _command = shlex.split(command) if isinstance(command, str) else command
    process = subprocess.Popen(_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def timeout_handler(signum, frame):
        logger.warning(f"command timed out after {timeout} seconds (cmd={command})")
        process.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(timeout)

    try:
        output, _ = process.communicate()
        return_code = process.returncode
    except KeyboardInterrupt:
        logger.warning("command interrupted")
        process.send_signal(signal.SIGTERM)
        return -1, None
    finally:
        signal.alarm(0)  # Disable the alarm to prevent unexpected behavior

    return return_code, output.decode()


def execute_command_with_timeout(command, timeout=30):
    """Executes a command with a timeout.

    Args:
        command: The command to execute as a list of strings.
        timeout: The maximum execution time in seconds.

    Returns:
        A tuple containing the return code of the command and the output.
    """
    result_queue = queue.Queue()

    def _execute_command():
        _command = shlex.split(command) if isinstance(command, str) else command
        process = subprocess.Popen(_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        try:
            output, errors = process.communicate(timeout=timeout)
            return_code = process.returncode
            result_queue.put((return_code, output.decode()))
        except subprocess.TimeoutExpired:
            process.kill()
            result_queue.put((-1, "Command timed out"))
        except KeyboardInterrupt:
            process.kill()
            result_queue.put((-1, "Command interrupted"))

    # Create a thread to execute the command
    thread = threading.Thread(target=_execute_command)
    thread.start()

    # Wait for the thread to finish or time out
    try:
        return_code, output = result_queue.get(timeout=timeout)
    except queue.Empty:
        thread.join()  # Wait for the thread to finish
        return_code, output = result_queue.get()

    return return_code, output
