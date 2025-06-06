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

import logging
import os
import psutil
import subprocess
from pathlib import Path

from pilot.common.pilotcache import get_pilot_cache
from pilot.util.filehandling import mkdirs

logger = logging.getLogger(__name__)
pilot_cache = get_pilot_cache()
CGROUP_PATH = "/sys/fs/cgroup"
PROC_CGROUP_PATH = "/proc/self/cgroup"


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
    #cgroup_path = os.path.join(paths[0], group_name)  # f'/sys/fs/cgroup/{group_name}'
    cgroup_path = paths[0]

    try:
        if not os.path.exists(cgroup_path):
            subprocess.run(['mkdir', cgroup_path], check=True, capture_output=True, text=True)
            #subprocess.run(['sudo', 'mkdir', cgroup_path], check=True)
            logger.info(f"cgroup '{group_name}' created.")
        else:
            logger.info(f"cgroup '{group_name}' already exists.")
    except subprocess.CalledProcessError as e:
        logger.warning(f"failed to create cgroup '{group_name}': {e}. stdout: {e.stdout}, stderr: {e.stderr}")
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


def parse_cgroup_path(size: int) -> str:
    """
    Parses the cgroup path from /proc/self/cgroup.

    Reads the contents of /proc/self/cgroup and extracts the path associated
    with the cgroup v2 entry (entry with id 0 and empty controllers field).
    If not found, returns None.

    Args:
        size (int): The maximum allowed length of the returned path, simulating a buffer size.

    Returns:
        str: The parsed cgroup path, truncated to (size - 1) characters if needed.
             Returns None if parsing fails.
    """
    try:
        with open(PROC_CGROUP_PATH, "r") as f_cgroup:
            logger.debug(f"Contents of {PROC_CGROUP_PATH}:")
            for line in f_cgroup:
                logger.debug(line.strip())
                parts = line.strip().split(":")
                if len(parts) == 3:
                    hierarchy_id, controllers, path = parts
                    if hierarchy_id == '0' and controllers == '':
                        return path[:size - 1]
    except IOError as e:
        logger.warning(f"Failed to open {PROC_CGROUP_PATH}: {e}")
        return None

    logger.warning(f"Failed to parse cgroup path from {PROC_CGROUP_PATH}")
    return None


def parse_cgroup_path_old(size: int) -> str:
    """
    Parse the cgroup v2 path from /proc/self/cgroup.

    Reads the contents of /proc/self/cgroup and extracts the path associated
    with the cgroup v2 entry (entry with id 0 and empty controller field).

    This function mimics the behavior of a C function using a fixed-size buffer.
    It prints the contents of the file for debugging and returns the parsed path,
    truncated to the given size (minus one character to allow for null-termination in C).

    Translated from C code: https://github.com/arosberg/memory_allocator/blob/main/memory_allocator.c

    Args:
        size (int): The maximum allowed length of the returned path, simulating a buffer size.

    Returns:
        str: The parsed cgroup v2 path, truncated to (size - 1) characters if needed.
    """
    try:
        with open(PROC_CGROUP_PATH, "r") as f_cgroup:
            logger.debug(f"parent: Contents of {PROC_CGROUP_PATH}:")
            for line in f_cgroup:
                logger.debug(f"parent: {line.strip()}")

                # Attempt to parse line using the expected format: <id>::<path>
                parts = line.strip().split("::")
                if len(parts) == 2:
                    try:
                        id_ = int(parts[0])
                        path = parts[1]
                        if id_ == 0:
                            # Ensure the path does not exceed the size limit
                            return path[:size - 1]
                    except ValueError:
                        continue
    except IOError:
        logger.warning(f"failed to open {PROC_CGROUP_PATH}")
        return None

    logger.warning(f"error: failed to parse cgroup path from {PROC_CGROUP_PATH}")
    return None


def create_cgroup(pid: int = os.getpid(), controller: str = "controller0") -> bool:  # noqa: C901
    """
    Create a cgroup for the current process.

    This function creates a cgroup for the current process and returns its path.

    Args:
        pid (int): The process ID to create the cgroup for. Default is the current process ID.
        controller (str, optional): The controller to create the cgroup for. Default is "controller0".

    Returns:
        bool: True if the cgroup was successfully created, False otherwise.
    """
    # First make sure that the cgroup was not already created for this pid
    if pilot_cache:
        pids = pilot_cache.get_pids()
        if pid in pids:
            logger.debug(f"cgroup already created for pid {pid}")
            return True

    # Parse the current cgroup path this process is running in
    current_cgroup_path = parse_cgroup_path(1024)  # ad hoc size
    if not current_cgroup_path:
        logger.warning(f"failed to parse cgroup path from {PROC_CGROUP_PATH}")
        return ""
    logger.debug(f"current_cgroup_path= {current_cgroup_path}")

    # Construct the full path to the parent cgroup
    parent_cgroup_path = os.path.join(CGROUP_PATH, current_cgroup_path[1:])  # remove the initial / from current_cgroup_path

    logger.debug(f"parent_cgroup_path= {parent_cgroup_path}")

    try:
        logger.debug(f"ls -lF {parent_cgroup_path}")
        result = subprocess.run(['ls', '-lF', parent_cgroup_path], check=True, capture_output=True, text=True)
        logger.debug(f"Command output: {result.stdout}")
    except Exception as e:
        logger.warning(f"failed to run command: {e}")
        return False
    try:
        path = os.path.join(parent_cgroup_path, "cgroup.procs")
        #cmd = f"cat {path}"
        #logger.debug(f"Executing command: {cmd}")
        #result = subprocess.run(cmd, shell=True, check=True, executable="/bin/bash",
        #                        capture_output=True, text=True)
        #logger.debug(f"Command output: {result.stdout}")
        moved = move_procs_to_parent(path)
        print(f"Moved PIDs: {moved}")
    except Exception as e:
        logger.warning(f"failed to run command: {e}")
        return False
    else:
        if not moved:
            logger.warning(f"failed to move processes to parent cgroup: {parent_cgroup_path}")
            return False
    # should be here; but it fails since there are already processes added to the cgroup
    # Enable memory and pid controllers in the parent cgroup
    #status = enable_controllers(parent_cgroup_path, "+memory +pids")
    #if not status:
    #    logger.warning(f"failed to enable controllers in cgroup: {parent_cgroup_path}")
    #    return False

    # Create a "controller" cgroup for the parent process
    controller_cgroup_path = os.path.join(parent_cgroup_path, controller)
    logger.info(f"Creating controller cgroup directory at: {controller_cgroup_path}")
    try:
        mkdirs(controller_cgroup_path, chmod=0o755)
    except Exception as e:
        logger.warning(f"failed to create cgroup: {e}")
        return False

    #
    try:
        logger.debug(f"ls -lF {controller_cgroup_path}")
        result = subprocess.run(['ls', '-lF', controller_cgroup_path], check=True, capture_output=True, text=True)
        logger.debug(f"Command output: {result.stdout}")
    except Exception as e:
        logger.warning(f"failed to run command: {e}")
        return False
    try:
        path = os.path.join(controller_cgroup_path, "cgroup.procs")
        cmd = f"cat {path}"
        logger.debug(f"Executing command: {cmd}")
        result = subprocess.run(cmd, shell=True, check=True, executable="/bin/bash",
                                capture_output=True, text=True)
        logger.debug(f"Command output: {result.stdout}")
    except Exception as e:
        logger.warning(f"failed to run command: {e}")

    # Enable memory and pid controllers in the parent controller cgroup
    status = enable_controllers(parent_cgroup_path, "+memory +pids")
    if not status:
        logger.warning(f"failed to enable controllers in cgroup: {parent_cgroup_path}")
        return False

    # Move the parent process (and any existing child processes) to the controller cgroup
    status = move_process_and_descendants_to_cgroup(controller_cgroup_path, pid)
    if not status:
        logger.warning(f"failed to move process to cgroup: {controller_cgroup_path}")
        return False

    # Keep track of the cgroup path in the pilot cache
    if pilot_cache:
        pilot_cache.add_cgroup(pid, controller_cgroup_path)

    return True


def move_procs_to_parent(path: str):
    """
    Moves all PIDs listed in the specified cgroup.procs file to its parent cgroup.

    Args:
        path (str): Full path to the cgroup.procs file (e.g., os.path.join(parent_cgroup_path, "cgroup.procs")).

    Returns:
        list: List of PIDs that were moved.

    Raises:
        RuntimeError: If any PID fails to move.
    """
    procs_file = Path(path)
    cgroup_path = procs_file.parent
    parent_procs_file = cgroup_path.parent / "cgroup.procs"

    if not procs_file.exists():
        raise FileNotFoundError(f"{procs_file} does not exist")

    logger.debug("Moving PIDs to parent cgroup: {parent_procs_file}")
    try:
        result = subprocess.run(["cat", str(procs_file)], check=True, capture_output=True, text=True)
        pids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to read {procs_file}: {e}")

    for pid in pids:
        try:
            subprocess.run(["bash", "-c", f"echo {pid} > {parent_procs_file}"], check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to move PID {pid} to {parent_procs_file}: {e}")

    return pids


def move_process_to_cgroup(cgroup_path: str, pid: int) -> bool:
    """
    Moves the given process to the specified cgroup by writing its PID to cgroup.procs.

    Constructs the path to the `cgroup.procs` file inside the given cgroup
    directory and writes the process ID to it. This is how processes are
    assigned to cgroups in cgroup v2.

    Args:
        cgroup_path (str): The filesystem path to the cgroup directory.
        pid (int): The PID of the process to move into the cgroup.

    Returns:
        bool: True if the process was successfully moved, False otherwise.
    """
    procs_path = os.path.join(cgroup_path, "cgroup.procs")

    try:
        with open(procs_path, "a") as f:
            f.write(f"{pid}")
    except IOError as e:
        logger.warning(f"Failed to move process to cgroup: {e}")
        try:
            result = subprocess.run([f'echo {pid} > {procs_path}'], check=True, capture_output=True, text=True)
            logger.debug(f"Command output: {result.stdout}")
            return True
        except Exception as e:
            logger.warning(f"failed to run command: {e}")

        return False

    logger.debug(f"added process {pid} to cgroup {cgroup_path}")
    return True


def move_process_and_descendants_to_cgroup(cgroup_path: str, root_pid: int):
    """
    Moves the given PID and all of its descendants into the specified cgroup
    by invoking 'echo <pid> > cgroup.procs' using a subprocess shell command.

    Args:
        cgroup_path (str): Path to the cgroup directory (e.g., /sys/fs/cgroup/mygroup).
        root_pid (int): The PID of the root process to move.

    Raises:
        RuntimeError: If moving any PID fails.
    """
    procs_file = f"{cgroup_path}/cgroup.procs"
    root_process = psutil.Process(root_pid)
    all_pids = [root_process.pid] + [p.pid for p in root_process.children(recursive=True)]

    for pid in all_pids:
        try:
            with open(procs_file, "a") as f:
                f.write(f"{pid}")
        except IOError as e:
            logger.warning(f"Failed to move process to cgroup: {e}")

            cmd = f"echo {pid} > {procs_file}"
            try:
                subprocess.run(cmd, shell=True, check=True, executable="/bin/bash")
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to move PID {pid} to cgroup: {e}")
                return False

        cmd = f"ls -l {procs_file}"
        logger.debug(f"Executing command: {cmd}")
        result = subprocess.run(cmd, shell=True, check=True, executable="/bin/bash",
                                capture_output=True, text=True)
        if result:
            logger.debug(result.stdout)
        else:
            logger.warning("failed to run command: ls? no output")

    logger.info(f"moved process {root_pid} to cgroup {cgroup_path} (process list= {all_pids})")
    return True


def enable_controllers(cgroup_path: str, controllers: str) -> bool:
    """
    Enable specified controllers in the cgroup's subtree_control file.

    Constructs the full path to the `cgroup.subtree_control` file and writes
    the given controller names (e.g., "+cpu +memory") to it. This is necessary
    to activate controllers in a cgroup v2 hierarchy.

    Args:
        cgroup_path (str): The filesystem path to the cgroup directory.
        controllers (str): A space-separated string of controllers to enable,
            prefixed with "+" (e.g., "+cpu +memory").

    Returns:
        bool: True if the controllers were successfully enabled, False otherwise.
    """
    subtree_control_path = os.path.join(cgroup_path, "cgroup.subtree_control")
    try:
        with open(subtree_control_path, "a") as f:
            f.write(f"{controllers}")
    except IOError as e:
        logger.warning(f"Failed to enable controllers: {e}")
    else:
        logger.debug(f"Enabled controllers {controllers} in cgroup {cgroup_path}")
        return True

    try:
        cmd = f'echo \"{controllers}\" > {subtree_control_path}'
        logger.debug(f"Executing command: {cmd}")
        result = subprocess.run(cmd,
                                shell=True, check=True, executable="/bin/bash", capture_output=True, text=True)
        #cmd = f"echo '{controllers}' | sudo tee {cgroup_path}/cgroup.subtree_control > /dev/null"
        #result = subprocess.run(cmd, shell=True)
        if result.returncode != 0:
            logger.warning(f"Failed to enable controllers at {cgroup_path}")
            return False
        logger.debug(f"Command output: {result.stdout}")
    except Exception as e:
        logger.warning(f"failed to run command: {e}")
        #return False
        cmd = f'ls -l {subtree_control_path}'
        logger.debug(f"Executing command: {cmd}")
        result = subprocess.run(cmd,
                                shell=True, check=True, executable="/bin/bash", capture_output=True, text=True)
        if result.returncode != 0:
            logger.debug(f"Failed to execute ls command: {result.stderr}")
        else:
            logger.debug(f"Command output: {result.stdout}")
        return False

    return True
