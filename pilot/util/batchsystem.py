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

"""Function library for batch system interaction."""

import subprocess
import logging

logger = logging.getLogger(__name__)


def is_version_sufficient(current_version: str, required_version: str) -> bool:
    """
    Compare two version strings without any external libraries.

    Args:
        current_version (str): Current version string (e.g., '23.0.2').
        required_version (str): Required version string (e.g., '24.0.7').

    Returns:
        bool: True if current_version >= required_version, else False.
    """

    def version_tuple(v):
        return tuple(map(int, v.split('.')))

    return version_tuple(current_version) >= version_tuple(required_version)


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
    except FileNotFoundError:
        # logger.error(f"command 'condor_version' not found: {e}")  # this is of course normal on a non-condor system, ignore
        return None


def is_htcondor_version_sufficient() -> bool:
    """
    Check if the HTCondor version is sufficient for cgroup support.

    For the new cgroups support, HTCondor version 24.0.7 or higher is required.

    Returns:
        bool: True if the version is sufficient, False otherwise.
    """
    required_htcondor_version = "24.0.7"
    htcondor_version = get_htcondor_version()
    if not htcondor_version:
        logger.warning("unable to determine HTCondor version (will not use cgroups for process management)")
        return False
    else:
        logger.info(f"HTCondor version: {htcondor_version} (version required for cgroups support: {required_htcondor_version})")

    return is_version_sufficient(htcondor_version, required_htcondor_version)
