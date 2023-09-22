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
