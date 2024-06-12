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
# - Paul Nilsson, paul.nilsson@cern.ch, 2024

"""Functions related to CVMFS operations."""

import logging
import os
import signal
import time
import types

from pilot.util.container import execute

logger = logging.getLogger(__name__)


class TimeoutException(Exception):
    """Timeout exception."""
    pass


def timeout_handler(signum: int, frame: types.FrameType) -> None:
    """Timeout handler."""
    raise TimeoutException


signal.signal(signal.SIGALRM, timeout_handler)


def is_cvmfs_available() -> bool or None:
    """
    Check if CVMFS is available.

    :return: True if CVMFS is available, False if not available, None if user cvmfs module not implemented.
    """
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    try:
        user = __import__(f'pilot.user.{pilot_user}.cvmfs', globals(), locals(), [pilot_user], 0)
    except ImportError:
        logger.warning('user cvmfs module does not exist - skipping cvmfs checks')
        return None

    mount_points = getattr(user, 'cvmfs_mount_points', None)
    if mount_points:
        found_bad_mount_point = False
        for mount_point in mount_points:
            # update any base path
            get_base_path = getattr(user, 'get_cvmfs_base_path', None)
            if get_base_path:
                mount_point = mount_point.replace('CVMFS_BASE', get_base_path())
            if os.path.exists(mount_point):
                # verify that the file can be opened
                if 'lastUpdate' not in mount_point:  # skip directories
                    logger.info(f'CVMFS is available at {mount_point}')
                    continue
                try:
                    with open(mount_point, 'r'):
                        pass
                except Exception as exc:
                    logger.warning(f'failed to open file {mount_point}: {exc}')
                    found_bad_mount_point = True
                    break
                else:
                    logger.info(f'CVMFS is available at {mount_point} (and could be opened)')
            else:
                logger.warning(f'CVMFS is not available at {mount_point}')
                found_bad_mount_point = True
                break
        if found_bad_mount_point:
            return False
        else:
            return True
    else:
        logger.warning('cvmfs_mount_points not defined in user cvmfs module')
        return None


def get_last_update() -> int:
    """
    Check the last update time from the last update file.

    :return: last update time (int).
    """
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.cvmfs', globals(), locals(), [pilot_user], 0)
    try:
        last_update_file = user.get_last_update_file()
    except AttributeError:
        last_update_file = None

    timestamp = None
    if last_update_file:
        if os.path.exists(last_update_file):
            try:
                timestamp = extract_timestamp(last_update_file)
            except Exception as exc:
                logger.warning(f'failed to read last update file: {exc}')
            if timestamp:
                now = int(time.time())
                logger.info(f'last cvmfs update on '
                            f'{time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(timestamp))} '
                            f'{now - timestamp} seconds ago ({timestamp})')
        else:
            logger.warning(f'last update file does not exist: {last_update_file}')
    else:
        logger.warning('last_update_file not defined in user cvmfs module')

    return timestamp


def extract_timestamp(filename: str) -> int:
    """
    Extract the timestamp from the last update file.

    The function will wait a maximum of 5 minutes for the file to be read. If the timeout is thrown, the function will
    return -1.

    :param filename: last update file name (str).
    :return: timestamp (int).
    """
    signal.alarm(300)  # Set the timeout to 5 minutes
    timestamp = 0
    try:
        with open(filename, 'r') as file:
            line = file.readline()  # e.g. "2024-03-18 19:43:47 | lxcvmfs145.cern.ch | 1710787427"
            parts = line.split("|")
            if len(parts) >= 3:
                timestamp = int(parts[2].strip())  # strip() is used to remove leading/trailing white spaces
    except TimeoutException:
        logger.warning("timeout caught while reading last update file")
        return -1
    finally:
        signal.alarm(0)  # Disable the alarm

    return timestamp


def cvmfs_diagnostics():
    """Run cvmfs_diagnostics."""
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.cvmfs', globals(), locals(), [pilot_user], 0)
    try:
        cmds = user.get_cvmfs_diagnostics_commands()
    except AttributeError:
        logger.warning('get_cvmfs_diagnostics_commands not defined in user cvmfs module')
        return

    if cmds:
        for cmd in cmds:
            timeout = 60
            logger.info(f'running cvmfs diagnostics command using timeout={timeout}s')
            exit_code, stdout, stderr = execute(cmd, timeout=timeout)
            if exit_code == 0:
                logger.info(f'cvmfs diagnostics completed successfully:\n{stdout}')
            else:
                logger.warning(f'cvmfs diagnostics failed: {stderr}')
    else:
        logger.warning('cvmfs diagnostics commands not defined in user cvmfs module')
