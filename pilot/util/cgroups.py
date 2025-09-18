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
try:
    import psutil
except ImportError:
    print('FAILED; psutil module could not be imported')
    _is_psutil_available = False
else:
    _is_psutil_available = True
import subprocess
from pathlib import Path

from pilot.common.pilotcache import get_pilot_cache

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
        logger.warning(f"Error occurred while determining cgroup version: {e}")
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
    paths = get_process_cgroups(str(pid))
    if not paths:
        return False
    cgroup_path = paths[0]

    try:
        if not os.path.exists(cgroup_path):
            subprocess.run(['mkdir', cgroup_path], check=True, capture_output=True, text=True)
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


def get_process_cgroups(pid: str = "self") -> list:
    """
    Gets the cgroup paths for a given process ID (default is 'self' for the current process).

    Args:
        pid (str): Process ID as a string. Default is 'self'.

    Returns:
        list: cgroup paths.
    """
    cgroups = []
    path = f"/proc/{pid}/cgroup"

    try:
        with open(path, "r", encoding='utf-8') as f:
            for line in f:
                parts = line.strip().split(":")
                if len(parts) == 3:
                    _, _, cgroup_path = parts
                    cgroups.append(cgroup_path)
    except FileNotFoundError:
        logger.warning(f"process {pid} does not exist")
    except PermissionError as e:
        logger.warning(f"error reading cgroup info for PID {pid}: {e}")

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
        with open(PROC_CGROUP_PATH, "r", encoding='utf-8') as f_cgroup:
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
        with open(PROC_CGROUP_PATH, "r", encoding='utf-8') as f_cgroup:
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


def create_cgroup(pid: int = os.getpid(), controller: str = "controller") -> bool:  # noqa: C901
    """
    Create a cgroup for the current process.

    This function creates a cgroup for the current process and returns its path. It also creates a controller
    and moves the current process into that cgroup. Additionally, it moves all processes in the parent cgroup
    to the control subgroup. Finally, it enables memory and pid controllers in the parent cgroup.

    It also creates a cgroup for the subprocess that will be created by the main process, so that the subprocess
    can be monitored and controlled as well.

    Args:
        pid (int): The process ID to create the cgroup for. Default is the current process ID.
        controller (str, optional): The controller to create the cgroup for. Default is "controller0".

    Returns:
        bool: True if the cgroup was successfully created, False otherwise.
    """
    # make sure that the cgroup was not already created for this pid
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

    # Create a "controller" cgroup for the parent process
    controller_cgroup_path = create_subgroup(parent_cgroup_path, controller)
    if not controller_cgroup_path:
        return False

    status = move_process_to_cgroup(controller_cgroup_path, os.getpid())
    if not status:
        logger.warning(f"failed to move process to controller_cgroup_path: {controller_cgroup_path}")
        return False

    # move all processes in the parent cgroup to the control subgroup
    _ = move_procs_to_control_subgroup(parent_cgroup_path)

    # create a new cgroup for future subprocesses
    subprocesses_cgroup_path = create_subgroup(parent_cgroup_path, "subprocesses")
    if not subprocesses_cgroup_path:
        logger.warning(f"failed to create subprocesses cgroup at {parent_cgroup_path}")
        return False

    # also create a new cgroup for the payload
    # payload_cgroup_path = create_subgroup(parent_cgroup_path, "payload")
    # if not payload_cgroup_path:
    #     logger.warning(f"failed to create payload cgroup at {parent_cgroup_path}")
    #     return False

    # enable memory and pid controllers in the parent cgroup
    status = enable_controllers(parent_cgroup_path, "+memory +pids")
    if not status:
        logger.warning(f"failed to enable controllers in cgroup: {parent_cgroup_path}")
        return False

    # Keep track of the cgroup path in the pilot cache
    if pilot_cache:
        pilot_cache.add_cgroup(str(pid), controller_cgroup_path)
        pilot_cache.add_cgroup("subprocesses", subprocesses_cgroup_path)
        # pilot_cache.add_cgroup("payload", payload_cgroup_path)

    return True


def create_subgroup(parent_path: str, subgroup_name: str) -> str:
    """
    Creates an additional cgroup for subprocesses under the specified parent cgroup path.

    Args:
        parent_path (str): Path to the parent cgroup directory.
        subgroup_name (str): Name of the subgroup to create.

    Returns:
        str: The path to the created subgroup, or an empty string if creation failed.
    """
    subgroup_path = Path(parent_path) / subgroup_name
    try:
        subgroup_path.mkdir(parents=True)
        # mkdirs(subgroup_path, chmod=0o755)
        logger.info(f"created cgroup at: {subgroup_path}")
        return subgroup_path
    except (FileExistsError, PermissionError) as e:
        logger.warning(f"failed to create cgroup {subgroup_name} at {parent_path}: {e}")
        return ""


def move_procs_to_control_subgroup(parent_cgroup_path: str, control_name: str = "control") -> list:
    """
    Moves all PIDs from the parent cgroup's cgroup.procs file to a control subgroup.

    Args:
        parent_cgroup_path (str): Path to the parent cgroup directory (e.g.,
            /sys/fs/cgroup/system.slice/htcondor/condor_var_lib_condor_execute_slot1_23@...).
        control_name (str): Name of the control subgroup to create and move PIDs into.

    Returns:
        list: List of PIDs that were moved to the control subgroup.
    """
    parent_path = Path(parent_cgroup_path)
    procs_file = parent_path / "cgroup.procs"
    control_path = parent_path / control_name
    control_procs_file = control_path / "cgroup.procs"

    # Create control subgroup if it doesn't exist
    if not control_path.exists():
        control_path.mkdir(parents=True)

    # Read PIDs from the parent cgroup.procs
    try:
        with open(procs_file, "r", encoding='utf-8') as f:
            pids = [line.strip() for line in f if line.strip()]
    except (PermissionError, FileNotFoundError) as e:
        logger.warning(f"Failed to read {procs_file}: {e}")
        pids = []

    # Move each PID to control subgroup
    for pid in pids:
        try:
            with open(control_procs_file, "w", encoding='utf-8') as f:
                f.write(pid)
        except (PermissionError, FileNotFoundError) as e:
            logger.warning(f"Failed to move PID {pid} to {control_procs_file}: {e}")
            pids = []

    return pids


def move_procs_to_parent(path: str):
    """
    Moves all PIDs listed in the specified cgroup.procs file to its parent cgroup.

    Args:
        path (str): Full path to the cgroup.procs file (e.g., os.path.join(parent_cgroup_path, "cgroup.procs")).

    Returns:
        list: List of PIDs that were moved.

    Raises:
        RuntimeError: If any PID fails to move.
        FileNotFoundError: If the specified cgroup.procs file does not exist.
    """
    procs_file = Path(path)
    cgroup_path = procs_file.parent
    parent_procs_file = cgroup_path.parent / "cgroup.procs"

    if not procs_file.exists():
        raise FileNotFoundError(f"{procs_file} does not exist")

    logger.debug(f"Moving PIDs to parent cgroup: {parent_procs_file}")
    try:
        logger.debug(f"cat {str(procs_file)}:")
        result = subprocess.run(["cat", str(procs_file)], check=True, capture_output=True, text=True)
        logger.debug(f"result={result.stdout}")
        pids = [line.strip() for line in result.stdout.splitlines() if line.strip()]
        logger.debug(f"pids={pids}")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to read {procs_file}: {e}") from e

    for pid in pids:
        try:
            subprocess.run(["bash", "-c", f"echo {pid} > {parent_procs_file}"], check=True)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to move PID {pid} to {parent_procs_file}: {e}") from e

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
        with open(procs_path, "a", encoding='utf-8') as f:
            f.write(f"{pid}")
    except IOError as e:
        logger.warning(f"Failed to move process to cgroup: {e}")
        try:
            result = subprocess.run([f'echo {pid} > {procs_path}'], check=True, capture_output=True, text=True)
            logger.debug(f"Command output: {result.stdout}")
            return True
        except subprocess.CalledProcessError as exc:
            logger.warning(f"failed to run command: {exc}")

        return False

    logger.debug(f"added process {pid} to cgroup {cgroup_path}")
    return True


def move_process_and_descendants_to_cgroup(cgroup_path: str, root_pid: int) -> bool:
    """
    Moves the given PID and all of its descendants into the specified cgroup
    by invoking 'echo <pid> > cgroup.procs' using a subprocess shell command.

    Args:
        cgroup_path (str): Path to the cgroup directory (e.g., /sys/fs/cgroup/mygroup).
        root_pid (int): The PID of the root process to move.

    Returns:
        bool: True if all processes were successfully moved, False otherwise.
    """
    if not _is_psutil_available:
        logger.warning("psutil module is not available, cannot move processes to cgroup.")
        return False

    procs_file = f"{cgroup_path}/cgroup.procs"
    root_process = psutil.Process(root_pid)
    all_pids = [root_process.pid] + [p.pid for p in root_process.children(recursive=True)]

    for pid in all_pids:
        try:
            with open(procs_file, "a", encoding='utf-8') as f:
                f.write(f"{pid}")
        except IOError as e:
            logger.warning(f"failed to move process to cgroup: {e}")
            cmd = f"echo {pid} > {procs_file}"
            try:
                subprocess.run(cmd, shell=True, check=True, executable="/bin/bash")
            except subprocess.CalledProcessError as exc:
                logger.warning(f"failed to move PID {pid} to cgroup: {exc}")
                return False

    logger.info(f"moved process {root_pid} to cgroup {cgroup_path} (process list= {all_pids})")
    # test test BS kills
    #if "subprocesses" in str(cgroup_path):
    #    try:
    #        set_memory_limit(cgroup_path, 100000)
    #    except (OSError, FileNotFoundError, PermissionError, ValueError) as e:
    #        logger.warning(f"failed to set memory limit for cgroup {cgroup_path}: {e}")

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
        with open(subtree_control_path, "w", encoding='utf-8') as f:
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
    except subprocess.CalledProcessError as e:
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


def get_pids_for_cgroup(cgroup_path: str) -> list:
    """
    Get the PIDs of all processes in the specified cgroup.

    Args:
        cgroup_path (str): Path to the cgroup directory (e.g., /sys/fs/cgroup/mygroup).

    Returns:
        list: List of PIDs in the cgroup.
    """
    procs_file = os.path.join(cgroup_path, "cgroup.procs")
    try:
        with open(procs_file, "r", encoding='utf-8') as f:
            pids = [int(line.strip()) for line in f if line.strip()]
        return pids
    except IOError as e:
        logger.warning(f"Failed to read {procs_file}: {e}")
        return []


def monitor_cgroup(cgroup_path: str) -> None:
    """
    Monitor the specified cgroup by printing its PIDs and memory usage.

    Args:
        cgroup_path (str): Path to the cgroup directory (e.g., /sys/fs/cgroup/mygroup).
    """
    pids = get_pids_for_cgroup(cgroup_path)
    if not pids:
        logger.info(f"[cgroup: {cgroup_path}]\n  No processes found.")
        return

    output_lines = [f"[cgroup: {cgroup_path}]", f"  PIDs: {', '.join([str(pid) for pid in pids])}"]

    files_to_read = {
        "Memory Usage": f"{cgroup_path}/memory.current",
        "Memory Events": f"{cgroup_path}/memory.events",
        "Process Count": f"{cgroup_path}/pids.current"
    }

    for label, filepath in files_to_read.items():
        try:
            result = subprocess.run(f"cat {filepath}", shell=True, check=True, capture_output=True, text=True)
            content = result.stdout.strip()
            # Indent multi-line output for readability
            if '\n' in content:
                indented = "\n    ".join(content.splitlines())
                output_lines.append(f"  {label}:\n    {indented}")
            else:
                output_lines.append(f"  {label}: {content}")
        except subprocess.CalledProcessError as e:
            output_lines.append(f"  {label}: <error reading {filepath}> ({e})")

    logger.info("\n%s", "\n".join(output_lines))


def set_memory_limit(cgroup_path: str, memory_bytes: int):
    """
    Set the maximum memory usage limit for a given cgroup v2.

    This function writes the specified memory limit (in bytes) to the cgroup's
    `memory.max` file. If the limit is exceeded by processes in the cgroup,
    the kernel will trigger an out-of-memory (OOM) kill.

    Args:
        cgroup_path (str): Full path to the cgroup (e.g., /sys/fs/cgroup/mygroup).
        memory_bytes (int): The maximum allowed memory usage in bytes. Use -1 or
            a large value (e.g., 9223372036854771712) to disable the limit.

    Raises:
        FileNotFoundError: If the memory.max file is missing.
        PermissionError: If the script lacks permission to write to the cgroup.
        ValueError: If the memory_bytes is invalid or negative (other than -1).
        OSError: For other OS-level errors.
    """
    memory_max_path = os.path.join(cgroup_path, "memory.max")

    if memory_bytes < -1:
        raise ValueError(f"Invalid memory limit: {memory_bytes}")

    # cgroup expects "max" for unlimited
    value = "max" if memory_bytes == -1 else str(memory_bytes)

    try:
        with open(memory_max_path, "w", encoding='utf-8') as f:
            f.write(value)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"{memory_max_path} does not exist.") from e
    except PermissionError as e:
        raise PermissionError(f"Permission denied to write to {memory_max_path}. Are you root or delegated?") from e
    except OSError as e:
        raise OSError(f"Error writing memory limit to {memory_max_path}: {e}") from e

    logger.info(f"[cgroup: {cgroup_path}]\n  Max memory usage: {value}")
