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
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-24

"""Standalone implementation of time-out check on function call.

Timer stops execution of wrapped function if it reaches the limit of provided time. Supports decorator feature.

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: March 2018
"""

from __future__ import print_function  # Python 2 (2to3 complains about this)

import os
import signal
import sys

import traceback
import threading

from queue import Empty
from functools import wraps
from pilot.util.auxiliary import TimeoutException


class TimedThread:
    """Thread-based timer implementation using the ``threading`` module.

    Operates in shared memory space; subject to GIL limitations and cannot
    forcefully kill a running thread. Compatible with Windows.
    """

    def __init__(self, _timeout):
        """Initialize the timer.

        Args:
            _timeout: Timeout value for the operation in seconds.
        """
        self.timeout = _timeout
        self.is_timeout = False
        self.result = None

    def execute(self, func, args, kwargs):
        """Execute the function and store its return value or exception info.

        Args:
            func: Callable to execute.
            args: Positional arguments for ``func``.
            kwargs: Keyword arguments for ``func``.

        Returns:
            Tuple of (success_bool, result_or_exc_info).
        """
        try:
            ret = (True, func(*args, **kwargs))
        except (TypeError, ValueError, AttributeError, KeyError):
            ret = (False, sys.exc_info())

        self.result = ret

        return ret

    def run(self, func, args, kwargs, _timeout=None):
        """Run the given function in a daemon thread with a timeout.

        Raises:
            TimeoutException: If the timeout value is reached before the
                function finishes.
        """
        thread = threading.Thread(target=self.execute, args=(func, args, kwargs))
        thread.daemon = True

        thread.start()

        _timeout = _timeout if _timeout is not None else self.timeout

        try:
            thread.join(_timeout)
        except (RuntimeError, KeyboardInterrupt) as exc:
            print(f'exception caught while joining timer thread: {exc}')

        if thread.is_alive():
            self.is_timeout = True
            raise TimeoutException("Timeout reached", timeout=_timeout)

        if self.result:
            ret = self.result
            if ret[0]:
                return ret[1]

            try:
                _r = ret[1][0](ret[1][1]).with_traceback(ret[1][2])
            except AttributeError:
                exec("raise ret[1][0], ret[1][1], ret[1][2]")
            raise _r
        else:
            raise TimeoutException("Unknown time-out related error, see batch log for more info")


class TimedProcess:
    """Process-based timer implementation using the ``multiprocessing`` module.

    Uses a shared Queue to return the result from a completely isolated memory
    space. Python's default multiprocessing uses pickle for serialization, which
    cannot properly handle local or decorated functions (affects Windows only).
    Traceback data is printed to stderr.
    """

    def __init__(self, _timeout):
        """Initialize the timer.

        Args:
            _timeout: Timeout value for the operation in seconds.
        """
        self.timeout = _timeout
        self.is_timeout = False

    def run(self, func, args, kwargs, _timeout=None):
        """Run the given function in a daemon process with a timeout.

        Args:
            func: Callable to execute in a subprocess.
            args: Positional arguments for ``func``.
            kwargs: Keyword arguments for ``func``.
            _timeout: Timeout override in seconds; falls back to ``self.timeout``.

        Returns:
            Return value of ``func``.

        Raises:
            TimeoutException: If the timeout is reached before ``func`` returns.
        """

        def _execute(func, args, kwargs, queue):
            try:
                ret = func(*args, **kwargs)
                queue.put((True, ret))
            except (TypeError, ValueError, AttributeError, KeyError) as e:
                print(f'exception occurred while executing {func}', file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                queue.put((False, e))

        # do not put this import at the top since it can possibly interfere with some modules (esp. Google Cloud Logging modules)
        import multiprocessing

        queue = multiprocessing.Queue(1)
        process = multiprocessing.Process(target=_execute, args=(func, args, kwargs, queue))
        process.daemon = True
        process.start()

        _timeout = _timeout if _timeout is not None else self.timeout

        try:
            ret = queue.get(block=True, timeout=_timeout)
        except Empty as exc:
            self.is_timeout = True
            process.terminate()
            raise TimeoutException("Timeout reached", timeout=_timeout) from exc
        finally:
            while process.is_alive():
                process.join(1)
                if process.is_alive():  ## still alive, force terminate
                    process.terminate()
                    process.join(1)
                if process.is_alive() and process.pid:  ## still alive, hard kill
                    os.kill(process.pid, signal.SIGKILL)

            multiprocessing.active_children()

        if ret[0]:
            return ret[1]
        raise ret[1]


Timer = TimedProcess


def timeout(seconds: int, timer: Timer = None):
    """Decorator that causes a function to time out after a given number of seconds.

    Args:
        seconds: Timeout value in seconds.
        timer: Timer class to use. Defaults to ``Timer`` (``TimedProcess``).

    Raises:
        TimeoutException: If the timeout is reached before the function finishes.
    """
    timer = timer or Timer

    def decorate(function):

        @wraps(function)
        def wrapper(*args, **kwargs):
            return timer(seconds).run(function, args, kwargs)

        return wrapper

    return decorate
