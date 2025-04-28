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

from pilot.util.auxiliary import is_version_sufficient

logger = logging.getLogger(__name__)


def get_htcondor_version() -> str or None:
    """
    Get the HTCondor version.

    Returns:
        str or None: The HTCondor version number.
    """
    try:
        result = subprocess.check_output(['condor_version'], encoding='utf-8')
        version_line = result.splitlines()[0]
        version_number = version_line.split()[1]
        return version_number
    except subprocess.CalledProcessError as e:
        logger.warning(f"Failed to run condor_version: {e}")
        return None


def is_htcondor_version_sufficient() -> bool:
    """
    Check if the HTCondor version is sufficient for cgroup support.

    For the new cgroups support, HTCondor version 24.0.7 or higher is required.

    Returns:
        bool: True if the version is sufficient, False otherwise.
    """
    current_version = get_htcondor_version()
    if current_version is None:
        logger.warning("unable to determine HTCondor version")
        return False

    return is_version_sufficient(current_version, '24.0.7')


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


def add_process_to_cgroup(pid: int, group_name: str = 'mygroup') -> bool:
    """
    Create a cgroup with the given name (if it does not exist) and adds the specified process ID (PID) to it.

    Args:
        pid (int): The process ID to add to the cgroup.
        group_name (str): Name of the cgroup to create and use.

    Returns:
        bool: True if successfully added, False otherwise.
    """
    cgroup_path = f'/sys/fs/cgroup/{group_name}'

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
