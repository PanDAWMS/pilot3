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

"""Subprocess execution utilities.

This module is the single entry point for running external commands within the
pilot.  It provides several variants of a subprocess wrapper, each suited to
different execution contexts:

- :func:`execute` — the primary wrapper used throughout the pilot.  Launches a
  command under ``/bin/bash -c``, optionally wraps it in a container via
  :func:`containerise_executable`, enforces a configurable timeout through
  ``subprocess.communicate()``, and optionally moves the child process into a
  cgroup.  Returns ``(exit_code, stdout, stderr)`` or a bare
  :class:`subprocess.Popen` object when ``returnproc=True`` is passed.

- :func:`execute_nothreads` — a simpler variant that does *not* use background
  reader threads.  Required for commands (e.g. ``arcproxy``) whose stdout must
  be consumed in time order.

- :func:`execute2` — a file-redirect variant that writes stdout/stderr directly
  to caller-supplied file objects and uses an internal
  :class:`threading.Timer`-based timeout.  Returns only an exit code.

- :func:`execute_command` / :func:`execute_command_with_timeout` /
  :func:`execute_command_with_timeout2` — lightweight helpers for simple
  one-shot commands.

Helper functions include :func:`containerise_executable` (which delegates to
the user plugin's ``container.wrapper()``), :func:`kill_all` (SIGTERM/SIGKILL
cleanup after a timeout), :func:`print_executable` (log-safe command
printing), and :func:`obscure_token` (redacts credentials from log output).
"""

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


def execute(executable: Any, **kwargs: Any) -> Any:  # noqa: C901
    """Execute a shell command, optionally inside a container.

    The primary subprocess wrapper used throughout the pilot.  Normalises
    *executable* to a string, optionally wraps it with the user container
    plugin, spawns a ``/bin/bash -c`` child process, and optionally moves it
    to a cgroup.  ``subprocess.communicate()`` is used with a configurable
    timeout to collect output.

    Keyword Args:
        usecontainer (bool): Wrap the command in a container. Default ``False``.
        job: Job object; used to check ``imagename`` and ``usecontainer``.
        obscure (str): Sensitive substring to redact in log output.
        mute (bool): Suppress the pre-execution log line. Default ``False``.
        timeout (int): Timeout in seconds passed to ``communicate()``.
            Defaults to 10 days if not set.
        mode (str): ``'python'`` to prefix with ``/usr/bin/python``;
            otherwise ``/bin/bash -c`` is used.
        stdout: File-like object for child stdout; defaults to
            ``subprocess.PIPE``.
        stderr: File-like object for child stderr; defaults to
            ``subprocess.PIPE``.
        cwd (str): Working directory for the child process.
        returnproc (bool): When ``True``, return the bare
            :class:`subprocess.Popen` object instead of
            ``(exit_code, stdout, stderr)``.

    Args:
        executable: Command string or list of strings to execute.

    Returns:
        A 3-tuple ``(exit_code, stdout, stderr)`` on normal completion, or a
        :class:`subprocess.Popen` instance when ``returnproc=True``.
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


def execute_old3(executable: Any, **kwargs: Any) -> Any:  # noqa: C901
    """Execute a command using background reader threads for stdout/stderr.

    .. deprecated::
        This is a legacy implementation superseded by :func:`execute`.  It uses
        background :class:`threading.Thread` workers and ``select``-based I/O
        to read stdout/stderr asynchronously.  Retained for reference only.

    Args:
        executable: Command string or list of strings to execute.
        **kwargs: Same keyword arguments as :func:`execute`.

    Returns:
        A 3-tuple ``(exit_code, stdout, stderr)`` on normal completion, or a
        :class:`subprocess.Popen` instance when ``returnproc=True``.
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


def execute_nothreads(executable: Any, **kwargs: Any) -> Any:
    """Execute a command without background reader threads.

    A thread-free variant of :func:`execute` required for commands (such as
    ``arcproxy``) whose stdout is consumed in strict time order.  Uses
    ``preexec_fn=os.setsid`` to create a new process group and calls
    ``process.communicate()`` directly without auxiliary reader threads.

    Args:
        executable: Command string or list of strings to execute.
        **kwargs: Same keyword arguments as :func:`execute`.

    Returns:
        A 3-tuple ``(exit_code, stdout, stderr)`` on normal completion, or a
        :class:`subprocess.Popen` instance when ``returnproc=True``.
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


def execute2(executable: Any, stdout_file: TextIO, stderr_file: TextIO, timeout_seconds: int, **kwargs: Any) -> int:
    """Execute a command redirecting output to files, with a timer-based timeout.

    Launches the command under ``/bin/bash -c`` and writes stdout/stderr
    directly to the supplied file objects.  A :class:`threading.Timer` fires
    after *timeout_seconds* and calls ``process.terminate()``; a secondary
    ``process.wait()`` guard catches any additional delay.

    Args:
        executable: Command string or list of strings to execute.
        stdout_file: Open file object receiving the child's standard output.
        stderr_file: Open file object receiving the child's standard error.
        timeout_seconds: Maximum execution time in seconds before the process
            is terminated.
        **kwargs: Additional keyword arguments (e.g. ``mute``, ``obscure``,
            ``mode``, ``cwd``).

    Returns:
        Exit code of the subprocess, or ``errors.COMMANDTIMEDOUT`` if it was
        killed due to a timeout.
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
    """Return the effective timeout for ``subprocess.communicate()``.

    A large fallback (10 days) is used when no timeout is requested so that
    the subprocess module never hangs indefinitely on nodes with many cores.

    Args:
        requested_timeout: Caller-supplied timeout in seconds, or ``None``/
            ``0`` to use the default.

    Returns:
        Timeout in seconds; either *requested_timeout* if truthy, or
        ``864000`` (10 days).
    """
    return requested_timeout if requested_timeout else 10 * 24 * 60 * 60  # using a ridiculously large default timeout


def execute_command(command: str) -> int:
    """Execute a command using subprocess without invoking a shell.

    Args:
        command: The command string to execute (split internally with
            :func:`shlex.split`).

    Returns:
        The exit code of the subprocess.
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
    """Kill a timed-out subprocess and its entire process group.

    Sends SIGTERM to the process group via
    :func:`~pilot.util.processgroups.kill_process_group`, then individually
    sends SIGTERM and (after 10 s) SIGKILL to the process PID.  Any
    ``ProcessLookupError`` (process already gone) is silently appended to
    *stderr* as context.

    Args:
        process: The :class:`subprocess.Popen` object to kill.
        stderr: Accumulated stderr text; error details are appended and the
            updated string is returned.

    Returns:
        The updated *stderr* string with any kill-related error details
        appended.
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
    """Log the command to be executed with all secrets redacted.

    Removes ``S3_SECRET_KEY=<value>`` substrings, replaces any string
    matching *obscure* with ``'********'``, and calls
    :func:`obscure_token` to strip ``-p <token>`` patterns before writing
    to the logger at INFO level.

    Args:
        executable: The full command string about to be executed.
        obscure: An additional sensitive substring to redact, e.g. a
            password or token passed via a ``--password`` flag.
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


def containerise_executable(executable: str, **kwargs: Any) -> tuple:
    """Wrap a command with the user-plugin container invocation.

    Imports the ``pilot.user.<user>.container`` module and calls its
    ``wrapper()`` function to prepend the container runtime command
    (e.g. Singularity/Apptainer arguments) to *executable*.  The container
    is skipped for event-service grid jobs and when ``do_use_container``
    resolves to ``False``.

    Args:
        executable: The bare command string to containerise.
        **kwargs: Forwarded to the user container plugin; typically includes
            ``job``, ``workdir``, etc.

    Returns:
        A 2-tuple ``(containerised_executable, diagnostics)`` where
        *containerised_executable* is the wrapped command string, or
        ``None`` on failure, and *diagnostics* is an empty string on success
        or an error message on failure.
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
    """Redact a ``-p <token>`` credential from a command string.

    Uses a regex to find the first ``-p <non-whitespace>`` token and replaces
    the value with ``'********'``.  Returns an empty string if the regex
    raises an exception.

    Args:
        cmd: The command string that may contain a ``-p <token>`` argument.

    Returns:
        The command string with the token value replaced, or an empty string
        if a regex error occurred.
    """
    try:
        match = re.search(r'-p (\S+)\ ', cmd)
        if match:
            cmd = cmd.replace(match.group(1), '********')
    except (re.error, AttributeError, IndexError):
        logger.warning('an exception was thrown while trying to obscure the user token')
        cmd = ''

    return cmd


def execute_command_with_timeout2(command: Any, timeout: int = 30) -> tuple:
    """Execute a command with a ``SIGALRM``-based timeout.

    Uses :func:`signal.alarm` to send ``SIGALRM`` after *timeout* seconds,
    which triggers SIGTERM on the child process.  Note: ``SIGALRM`` is only
    available on Unix.

    Args:
        command: The command to execute; either a string (split with
            :func:`shlex.split`) or a list of strings.
        timeout: Maximum execution time in seconds. Default ``30``.

    Returns:
        A 2-tuple ``(return_code, output)`` where *output* is the decoded
        stdout string, or ``(-1, None)`` if the command was interrupted.
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


def execute_command_with_timeout(command: Any, timeout: int = 30) -> tuple:
    """Execute a command in a thread with a queue-based timeout.

    Runs the command in a dedicated :class:`threading.Thread` and collects
    the result via a :class:`queue.Queue`.  Unlike
    :func:`execute_command_with_timeout2` this does not rely on ``SIGALRM``
    and is therefore safe to call from non-main threads.

    Args:
        command: The command to execute; either a string (split with
            :func:`shlex.split`) or a list of strings.
        timeout: Maximum execution time in seconds. Default ``30``.

    Returns:
        A 2-tuple ``(return_code, output)`` where *output* is the decoded
        stdout string, ``"Command timed out"`` on timeout, or
        ``"Command interrupted"`` on keyboard interrupt.
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
