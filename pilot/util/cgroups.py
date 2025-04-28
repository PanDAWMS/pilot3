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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025

"""Code for interacting with cgroups."""

import os
import subprocess
import logging

from pilot.common.pilotcache import get_pilot_cache

logger = logging.getLogger(__name__)
pilot_cache = get_pilot_cache()


def get_cgroup_version() -> str:
    """
    Determine if the system is using cgroups version 1 or 2.

    Returns:
        str: 'v1' if cgroups version 1, 'v2' if version 2, or None if unable to determine.
    """
    try:
        output = subprocess.check_output(
            ['mount'], encoding='utf-8'
        )

        if 'type cgroup2' in output:
            return 'v2'
        if 'type cgroup' in output:
            return 'v1'
        return None

    except subprocess.CalledProcessError as e:
        print(f"Error occurred while determining cgroup version: {e}")
        return None


def add_process_to_cgroup(pid: int, group_name: str = 'panda_pilot') -> bool:
    """
    Create a cgroup with the given name (if it does not exist) and adds the specified process ID (PID) to it.

    Args:
        pid (int): The process ID to add to the cgroup.
        group_name (str): Name of the cgroup to create and use.

    Returns:
        bool: True if successfully added, False otherwise.
    """
    paths = get_process_cgroups(pid)
    if not paths:
        return False
    cgroup_path = os.path.join(paths[0], group_name)  # f'/sys/fs/cgroup/{group_name}'

    try:
        if not os.path.exists(cgroup_path):
            subprocess.run(['sudo', 'mkdir', cgroup_path], check=True)
            logger.info(f"cgroup '{group_name}' created.")
        else:
            logger.info(f"cgroup '{group_name}' already exists.")
    except subprocess.CalledProcessError as e:
        logger.warning(f"failed to create cgroup '{group_name}': {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False
    except PermissionError as e:
        logger.warning(f"permission denied when creating cgroup '{group_name}': {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False

    try:
        with open(os.path.join(cgroup_path, 'cgroup.procs'), 'w', encoding='utf-8') as f:
            f.write(str(pid))
        logger.info(f"process {pid} added to cgroup '{group_name}'.")
        return True
    except FileNotFoundError as e:
        logger.warning(f"cgroup file not found: {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False
    except PermissionError as e:
        logger.warning(f"permission denied when adding PID {pid} to cgroup '{group_name}': {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False
    except Exception as e:
        logger.warning(f"unexpected error adding PID {pid} to cgroup '{group_name}': {e}")
        logger.info(f"cgroup version: {get_cgroup_version()}")
        return False


def get_process_cgroups(pid="self"):
    """
    Gets the cgroup paths for a given process ID (default is 'self' for the current process).

    :param pid: Process ID as a string or integer. Default is 'self'.
    :return: List of cgroup paths.
    """
    cgroups = []
    path = f"/proc/{pid}/cgroup"

    try:
        with open(path, "r") as f:
            for line in f:
                parts = line.strip().split(":")
                if len(parts) == 3:
                    _, _, cgroup_path = parts
                    cgroups.append(cgroup_path)
    except FileNotFoundError:
        print(f"Process {pid} does not exist.")
    except Exception as e:
        print(f"Error reading cgroup info for PID {pid}: {e}")

    return cgroups
