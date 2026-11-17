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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-26

"""Standalone implementation of time-out check on function call.

Timer stops execution of wrapped function if it reaches the limit of provided time. Supports decorator feature.

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: March 2018
"""

from __future__ import print_function  # Python 2 (2to3 complains about this)

import os
import pickle
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

        Every exception raised by ``func`` is captured and handed back to the
        caller through :meth:`run`. An exception escaping this method would
        end the worker thread (dumping its traceback to stderr), be lost, and
        leave :meth:`run` able to report only a misleading time-out, which the
        stage-in/out error resolution then classified as a transfer time-out.

        Args:
            func (Callable): Callable to execute.
            args (tuple): Positional arguments for ``func``.
            kwargs (dict): Keyword arguments for ``func``.

        Returns:
            tuple: (success_bool, result_or_exc_info).
        """
        try:
            ret = (True, func(*args, **kwargs))
        except Exception:  # any failure is the caller's to see, never a time-out
            ret = (False, sys.exc_info())

        self.result = ret

        return ret

    def run(self, func, args, kwargs, _timeout=None):
        """Run the given function in a daemon thread with a timeout.

        A ``_timeout`` value (seconds) overrides ``self.timeout``.

        Args:
            func (Callable): Callable to execute in the thread.
            args (tuple): Positional arguments for ``func``.
            kwargs (dict): Keyword arguments for ``func``.

        Returns:
            Any: Return value of ``func``.

        Raises:
            TimeoutException: If the timeout value is reached before the
                function finishes.
            Exception: Whatever ``func`` raised, re-raised unchanged.
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

            raise ret[1][1].with_traceback(ret[1][2])
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

        A ``_timeout`` value (seconds) overrides ``self.timeout``.

        Args:
            func (Callable): Callable to execute in a subprocess.
            args (tuple): Positional arguments for ``func``.
            kwargs (dict): Keyword arguments for ``func``.

        Returns:
            Any: Return value of ``func``.

        Raises:
            TimeoutException: If the timeout is reached before ``func`` returns.
        """

        def _execute(func, args, kwargs, queue):
            try:
                ret = func(*args, **kwargs)
                queue.put((True, ret))
            except Exception as e:  # any failure is the caller's to see, never a time-out
                print(f'exception occurred while executing {func}', file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                try:
                    pickle.loads(pickle.dumps(e))
                except Exception:  # the queue pickles; keep the type and message of one that cannot be
                    e = RuntimeError(f'{type(e).__name__}: {e}')
                queue.put((False, e))

        # do not put this import at the top since it can possibly interfere with some modules (esp. Google Cloud Logging modules)
        import multiprocessing  # pylint: disable=import-outside-toplevel

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
