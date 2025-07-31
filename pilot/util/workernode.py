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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-25

import logging
import os
import re
import socket
import subprocess
from shutil import which
from typing import (
    Optional,
    Tuple
)

#from subprocess import getoutput

from pilot.common.exception import (
    PilotException,
    ErrorCodes
)
from pilot.info import infosys
from pilot.util.auxiliary import sort_words
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.disk import disk_usage
from pilot.util.filehandling import (
    read_json,
    write_json
)
from pilot.util.math import convert_b_to_gb
from pilot.util.psutils import get_clock_speed

logger = logging.getLogger(__name__)


def get_local_disk_space(path):
    """
    Return remaining disk space for the disk in the given path.
    Unit is MB.

    :param path: path to disk (string). Can be None, if call to collect_workernode_info() doesn't specify it.
    :return: disk space (float).
    :raises: PilotException in case of failure to convert df output to float, or if getoutput() returns an empty string.
    """

    # -mP = blocks of 1024*1024 (MB) and POSIX format
    cmd = f"df -mP {path}"
    #disks = getoutput(cmd)
    _, stdout, stderr = execute(cmd)
    if stdout:
        logger.debug(f'stdout={stdout}')
        logger.debug(f'stderr={stderr}')
        try:
            disk = float(stdout.splitlines()[1].split()[3])
        except (IndexError, ValueError, TypeError, AttributeError) as error:
            msg = f'exception caught while trying to convert disk info: {error}'
            logger.warning(msg)
            raise PilotException(msg, code=ErrorCodes.UNKNOWNEXCEPTION)
    else:
        msg = f'no stdout+stderr from command: {cmd}'
        logger.warning(msg)
        raise PilotException(msg, code=ErrorCodes.UNKNOWNEXCEPTION)

    return disk


def get_total_memory() -> float:
    """
    Return the total memory (in MB) from /proc/meminfo.

    :return: total memory in MB (float).
    """
    try:
        with open('/proc/meminfo') as f:
            for line in f:
                if 'MemTotal:' in line:
                    mem_kb = int(line.split()[1])
                    return round(mem_kb / 1024, 2)
    except (FileNotFoundError, IOError, ValueError) as error:
        logger.warning(f"exception caught when trying to read /proc/meminfo: {error}")

    return 0.0


def get_cpu_flags(_sorted: bool = True) -> str:
    """
    Return the CPU flags.

    :param _sorted: should the CPU flags be sorted? (Boolean)
    :return: cpu flags (string).
    """

    flags = ''
    with open("/proc/cpuinfo", "r") as _fd:
        lines = _fd.readlines()
        for line in lines:
            if line.find("flags") != -1:
                try:
                    flags = line.split(":")[1].strip()
                except ValueError as error:
                    logger.warning(f'exception caught while trying to convert cpuinfo: {error}')
                break  # command info is the same for all cores, so break here

    if flags and _sorted:
        flags = sort_words(flags)

    return flags


def get_cpu_arch_internal():
    """
    Return the CPU architecture string (using internal script).

    The CPU architecture string is determined by a script (pilot/scripts/cpu_arch.py), run by the pilot.
    For details about this script, see: https://its.cern.ch/jira/browse/ATLINFR-4844

    :return: CPU arch (string).
    """

    cpu_arch = ''

    # copy pilot source into container directory, unless it is already there
    script = 'cpu_arch.py'
    srcdir = os.path.join(os.environ.get('PILOT_SOURCE_DIR', '.'), 'pilot3')
    script_dir = os.path.join(srcdir, 'pilot/scripts')

    if script_dir not in os.environ['PYTHONPATH']:
        os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH') + ':' + script_dir

    # CPU arch script has now been copied, time to execute it
    ec, stdout, stderr = execute(f'python3 {script_dir}/{script} --alg gcc')
    if ec or stderr:
        logger.debug(f'ec={ec}, stdout={stdout}, stderr={stderr}')
    else:
        cpu_arch = stdout
        logger.debug(f'CPU arch script returned: {cpu_arch}')

    return cpu_arch


def get_cpu_arch():
    """
    Return the CPU architecture string.

    The CPU architecture string is determined by a script (cpu_arch.py), run by the pilot but setup with lsetup.
    For details about this script, see: https://its.cern.ch/jira/browse/ATLINFR-4844

    :return: CPU arch (string).
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.utilities' % pilot_user, globals(), locals(), [pilot_user], 0)
    cpu_arch = user.get_cpu_arch()
    if not cpu_arch:
        logger.info('no CPU architecture reporting')
        # cpu_arch = get_cpu_arch_internal()

    return cpu_arch


def collect_workernode_info(path=None):
    """
    Collect node information (cpu, memory and disk space).
    The disk space (in MB) is return for the disk in the given path.

    :param path: path to disk (string).
    :return: memory (float), cpu (float), disk space (float).
    """

    mem = get_total_memory()
    cpu = get_cpu_frequency()
    try:
        disk = get_local_disk_space(path)
    except PilotException as exc:
        diagnostics = exc.get_detail()
        logger.warning(f'exception caught while executing df: {diagnostics} (ignoring)')
        disk = None

    return mem, cpu, disk


def get_disk_space(queuedata):
    """
    Get the disk space from the queuedata that should be available for running the job;
    either what is actually locally available or the allowed size determined by the site (value from queuedata). This
    value is only to be used internally by the job dispatcher.

    :param queuedata: infosys object.
    :return: disk space that should be available for running the job (int).
    """

    # --- non Job related queue data
    # jobinfo provider is required to consider overwriteAGIS data coming from Job
    _maxinputsize = infosys.queuedata.maxwdir
    logger.debug(f'resolved value from global infosys.queuedata instance: infosys.queuedata.maxwdir={_maxinputsize} B')
    _maxinputsize = queuedata.maxwdir
    logger.debug(f'resolved value: queuedata.maxwdir={_maxinputsize} B')

    try:
        _du = disk_usage(os.path.abspath("."))
        _diskspace = int(_du[2] / (1024 * 1024))  # need to convert from B to MB
    except ValueError as error:
        logger.warning(f"failed to extract disk space: {error} (will use schedconfig default)")
        _diskspace = _maxinputsize
    else:
        logger.info(f'available WN disk space: {_diskspace} MB')

    _diskspace = min(_diskspace, _maxinputsize)
    logger.info(f'sending disk space {_diskspace} MB to dispatcher')

    return _diskspace


def get_node_name():
    """
    Return the local node name.

    :return: node name (string)
    """
    if 'PANDA_HOSTNAME' in os.environ:
        host = os.environ.get('PANDA_HOSTNAME')
    elif hasattr(os, 'uname'):
        host = os.uname()[1]
    else:
        host = socket.gethostname()

    return get_condor_node_name(host)


def get_condor_node_name(nodename):
    """
    On a condor system, add the SlotID to the nodename

    :param nodename:
    :return:
    """

    if "_CONDOR_SLOT" in os.environ:
        nodename = "%s@%s" % (os.environ.get("_CONDOR_SLOT"), nodename)

    return nodename


def get_cpu_model() -> str:
    """
    Get cpu model and cache size from /proc/cpuinfo.

    If the cpu model is not found, the function will attempt to use lscpu instead.

    Example.
      model name      : Intel(R) Xeon(TM) CPU 2.40GHz
      cache size      : 512 KB

    gives the return string "Intel(R) Xeon(TM) CPU 2.40GHz 512 KB".

    :return: cpu model (str).
    """

    cpumodel = ""
    cpucache = ""
    modelstring = ""

    re_model = re.compile(r'^model name\s+:\s+(\w.+)')  # Python 3 (added r)
    re_cache = re.compile(r'^cache size\s+:\s+(\d+ KB)')  # Python 3 (added r)

    with open("/proc/cpuinfo", "r") as _fp:

        # loop over all lines in cpuinfo
        for line in _fp.readlines():
            # try to grab cpumodel from current line
            model = re_model.search(line)
            if model:
                # found cpu model
                cpumodel = model.group(1)

            # try to grab cache size from current line
            cache = re_cache.search(line)
            if cache:
                # found cache size
                cpucache = cache.group(1)

            # stop after 1st pair found - can be multiple cpus
            if cpumodel and cpucache:
                # create return string
                modelstring = cpumodel + " " + cpucache
                break

    # default return string if no info was found
    if not modelstring:
        modelstring = "UNKNOWN"

    if modelstring == "UNKNOWN":
        # try to get the model string from lscpu instead
        _, stdout = lscpu()
        if stdout:
            # extract the model string from the lscpu output
            for line in stdout.split('\n'):
                if line.find("Model name") != -1:
                    modelstring = line.split(":")[1].strip()
                    break

    logger.debug(f"cpu model: {modelstring}")

    return modelstring


def lscpu():
    """
    Execute lscpu command.

    :return: exit code (int), stdout (string).
    """

    cmd = 'lscpu'
    if not which(cmd):
        logger.warning(f'command={cmd} does not exist - cannot check number of available cores')
        return 1, ""

    ec, stdout, _ = execute(cmd)
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8")

    logger.debug(f'lscpu:\n{stdout}')

    return ec, stdout


def get_partials_from_workernode_map() -> tuple[int, int, int, int, str, str]:
    """
    Get numbers from a cache (the worker node map json) if it exists, otherwise reset variables to 0.

    :return: cores per socket (int), threads per core (int), clock_speed (int), sockets (int), architecture (str), architecture level (str).
    """
    try:
        filename = os.path.join(os.getcwd(), config.Workernode.map)
        if os.path.exists(filename):
            workernode_map = read_json(filename)
            cores_per_socket = workernode_map.get('cores_per_socket', 0)
            threads_per_core = workernode_map.get('threads_per_core', 0)
            clock_speed = workernode_map.get('clock_speed', 0)
            sockets = workernode_map.get('n_sockets', 0)
            architecture = workernode_map.get('cpu_architecture', '')
            architecture_level = workernode_map.get('cpu_architecture_level', '')
            return cores_per_socket, threads_per_core, clock_speed, sockets, architecture, architecture_level
    except Exception as e:
        logger.warning(f'cannot read workernode map: {e}')

    return 0, 0, 0, 0, "", ""


def get_cpu_info() -> tuple[int, str, int, float, int, int, str, str]:
    """
    Get CPU information.

    :return: number of cores (int), ht (str), sockets (int), clock speed (float), threads per core (int),
    cores per socket (int), archictecture (str), architecture level (str).
    """
    # get numbers from a cache (the worker node map json) if it exists, otherwise reset variables to 0
    cores_per_socket, threads_per_core, clock_speed, sockets, architecture, architecture_level = get_partials_from_workernode_map()
    if cores_per_socket:
        number_of_cores = cores_per_socket * sockets
        ht = "HT" if threads_per_core == 2 else ""
        return number_of_cores, ht, sockets, clock_speed, threads_per_core, cores_per_socket, architecture, architecture_level

    ec, stdout = lscpu()
    if ec:
        return 0, "", 0, 0.0, 0, 0, "", ""

    # get the architecture level
    architecture_level = get_cpu_arch()

    def get_number_for_pattern(pattern: str, line: str) -> int:
        number = None
        try:
            _number = re.findall(pattern, line)
            if _number:
                number = int(_number[0])
        except Exception as exc:
            logger.warning(f'exception caught: {exc}')
            logger.warning(f'failed to extract number for pattern: {pattern} from line: {line}')

        return number

    for line in stdout.split('\n'):
        match = re.search(r"^Architecture:\s+(\S+)", line)
        if match:
            architecture = match.group(1)
            continue
        n = get_number_for_pattern(r'Thread\(s\)\ per\ core\:\ +(\d+)', line)
        if n:
            threads_per_core = n
            continue
        n = get_number_for_pattern(r'Core\(s\)\ per\ socket\:\ +(\d+)', line)
        if n:
            cores_per_socket = n
            continue
        m = get_number_for_pattern(r'CPU\ MHz\:\ +(\d+)', line)
        if m:
            clock_speed = m
            continue
        n = get_number_for_pattern(r'Socket\(s\)\:\ +(\d+)', line)
        if n:
            sockets = n
            break

    # if the CPU frequency was not found in the command output, try to get it from psutil instead or from /proc/cpuinfo
    if not clock_speed:
        clock_speed = get_clock_speed() or get_cpu_frequency() or 0.0

    ht = "HT" if threads_per_core == 2 else ""
    if cores_per_socket and sockets:
        number_of_cores = cores_per_socket * sockets
        _cores_per_socket = '1 core' if cores_per_socket == 1 else f'{cores_per_socket} cores'
        _sockets = '1 socket' if sockets == 1 else f'{sockets} sockets'
        logger.info(f'found {number_of_cores} cores ({_cores_per_socket} per socket, {_sockets}) {ht}, CPU MHz: {clock_speed}')
    else:
        number_of_cores = 0

    return number_of_cores, ht, sockets, clock_speed, threads_per_core, cores_per_socket, architecture, architecture_level


def update_modelstring(modelstring: str, number_of_cores: int, ht: str, sockets: int) -> str:
    """
    Update the model string with the number of cores, hyperthreading info and number of sockets.

    E.g. modelstring = 'Intel Xeon Processor (Skylake, IBRS) 16384 KB'
         -> updated modelstring = 'Intel Xeon 10-Core Processor (Skylake, IBRS) 16384 KB'

    :param modelstring: CPU model info (str)
    :param number_of_cores: number of cores (int)
    :param ht: hyperthreading info (str)
    :param sockets: number of sockets (int)
    :return: updated CPU model info (str).
    """
    logger.debug(f'current model string: {modelstring}')
    if number_of_cores > 0:
        if '-Core Processor' in modelstring:  # NN-Core info already in string - update it
            pattern = r'(\d+)\-Core Processor'
            _nn = re.findall(pattern, modelstring)
            if _nn:
                modelstring = modelstring.replace(f'{_nn[0]}-Core', f'{number_of_cores}-Core')
        elif 'Core Processor' in modelstring:
            modelstring = modelstring.replace('Core', '%d-Core' % number_of_cores)
        elif 'Processor' in modelstring:
            modelstring = modelstring.replace('Processor', '%d-Core Processor' % number_of_cores)
        else:
            modelstring += ' %d-Core Processor' % number_of_cores

        if ht:
            modelstring += " " + ht
        modelstring += f' {sockets}-Socket'
        if sockets > 1:
            modelstring += 's'
        logger.debug(f'updated model string: {modelstring}')

    return modelstring


def check_hz():
    """
    Try to read the SC_CLK_TCK and write it to the log.

    :return:
    """

    try:
        _ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
    except Exception:
        import traceback
        logger.fatal('failed to read SC_CLK_TCK - will not be able to perform CPU consumption calculation')
        logger.warning(traceback.format_exc())


def get_hepspec_per_core() -> str:
    """
    Get the published hepspec value per core.

    On HTCondor only.

    :return: hepspec value (str).
    """
    condor_machine_ad = os.environ.get('CONDOR_MACHINE_AD', '')
    if not condor_machine_ad:
        logger.warning('CONDOR_MACHINE_AD not set - cannot determine hepspec value')
        return ''

    cmd = f"cat {condor_machine_ad}"
    _, stdout, _ = execute(cmd)
    logger.debug(f"cmd: {cmd}, stdout:\n{stdout}")

    cmd = f"condor_status -ads {condor_machine_ad} -af HEPSPEC_PER_CORE"
    _, stdout, _ = execute(cmd)
    logger.debug(f"cmd: {cmd}, stdout:\n{stdout}")

    return stdout


def extract_site_and_schedd() -> Tuple[Optional[str], Optional[str]]:
    """
    Extract the values of 'GLIDEIN_Site' and 'RemoteScheddName' from the .machine.ad file.

    Returns:
        Tuple[Optional[str], Optional[str]]: A tuple containing the GLIDEIN_Site
        and RemoteScheddName values, respectively. Each will be None if not found.
    """
    ad_path = os.environ.get('_CONDOR_MACHINE_AD')
    if not ad_path:
        logger.warning("Environment variable _CONDOR_MACHINE_AD is not set.")
        return None, None

    site = None
    schedd = None

    site_pattern = re.compile(r'^GLIDEIN_Site\s*=\s*"([^"]+)"')
    schedd_pattern = re.compile(r'^RemoteScheddName\s*=\s*"([^"]+)"')

    try:
        with open(ad_path, 'r') as file:
            for line in file:
                stripped = line.strip()
                if site is None:
                    site_match = site_pattern.match(stripped)
                    if site_match:
                        site = site_match.group(1)
                if schedd is None:
                    schedd_match = schedd_pattern.match(stripped)
                    if schedd_match:
                        schedd = schedd_match.group(1)
                if site and schedd:
                    break
    except FileNotFoundError:
        logger.warning(f"File not found: {ad_path}")
    except IOError as e:
        logger.warning(f"Error reading file {ad_path}: {e}")

    if site is None:
        logger.warning("failed to extract GLIDEIN_Site from .machine.ad file")
    if schedd is None:
        logger.warning("failed to extract RemoteScheddName from .machine.ad file")

    return site, schedd


def get_total_local_disk_size() -> int:
    """
    Run the lsblk command and capture the output.

    The lsblk will only report local disks and not any mounted disks.

    :return: total disk size in bytes (int).
    """
    result = subprocess.run(['lsblk', '-d', '-o', 'NAME,SIZE'], capture_output=True, text=True)

    # Regular expression to match disk size (supports K,M,G,T)
    size_pattern = re.compile(r'(\d+(\.\d+)?)([KMGTP])')

    size_units = {'K': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12, 'P': 1e15}
    total_size_bytes = 0

    try:
        for line in result.stdout.strip().split('\n')[1:]:
            parts = line.split()
            if len(parts) == 2:
                size_str = parts[1]
                match = size_pattern.match(size_str)
                if match:
                    size_val = float(match.group(1))
                    unit = match.group(3)
                    total_size_bytes += size_val * size_units[unit]
    except Exception:  # ignore any exceptions
        pass

    return total_size_bytes


def get_cpu_frequency() -> float:
    """
    Get the CPU frequency (in MHz) from /proc/cpuinfo.

    This function is only used if psutil cannot provice the clock speed.

    :return: CPU speed (float).
    """
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if "cpu MHz" in line:
                    return float(line.strip().split(":")[1])
    except (FileNotFoundError, IOError, ValueError, KeyError):
        pass

    return 0.0


def detect_architecture(model_name: str) -> str:
    """
    Try to infer architecture from known mappings; fallback to 'Unknown'.

    Args:
        model_name (str): The GPU model name to check.

    Returns:
        str: The architecture name if found, otherwise 'Unknown'.
    """
    # Extensible architecture mapping
    architecture_map = {
        'K80': 'Kepler',
        'P100': 'Pascal',
        'V100': 'Volta',
        'T4': 'Turing',
        'A100': 'Ampere',
        'L40': 'Ada Lovelace',
        'RTX 6000': 'Ada Lovelace',
        'H100': 'Hopper',
        'GH200': 'Grace Hopper'
    }

    for key, arch in architecture_map.items():
        if key in model_name:
            return arch

    logger.warning(f"Unknown architecture for GPU model: '{model_name}'")
    return "Unknown"


def get_gpu_info(site: str) -> dict:
    """
    Get GPU information using nvidia-smi command.

    This function will return a dictionary with the GPU information found on the system.

    Args:
        site (str): ATLAS site name from PQ.resource.

    Returns:
        dict: A dictionary containing GPU information such as vendor, model, architecture, VRAM, CUDA version, driver version, and count.
    """
    try:
        # Full nvidia-smi output for CUDA version
        full_output = subprocess.run(
            ['nvidia-smi'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True
        ).stdout

        cuda_match = re.search(r'CUDA Version:\s+([\d.]+)', full_output)
        cuda_version = cuda_match.group(1) if cuda_match else "Unknown"

        # Query key GPU parameters
        result = subprocess.run(
            ['nvidia-smi', '--query-gpu=name,memory.total,driver_version', '--format=csv,noheader,nounits'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            universal_newlines=True
        )

        lines = result.stdout.strip().split('\n')
        count = len(lines)
        name, vram, driver_version = lines[0].split(', ')
        architecture = detect_architecture(name)

        return {
            "site": site,
            "host_name": socket.gethostname(),
            "vendor": "NVIDIA",
            "model": name,
            "architecture": architecture,
            "vram": int(vram),  # MB
            "framework": "CUDA",
            "framework_version": cuda_version,
            "driver_version": driver_version,
            "count": count
        }

    except subprocess.CalledProcessError as e:
        logger.warning(f"failed to run nvidia-smi: {e.stderr}")
        return {}


def has_gpu() -> bool:
    """
    Check whether the system has a GPU recognized as a '3D controller' by lspci.

    Returns:
        bool: True if a '3D controller' is found in the lspci output, False otherwise.
    """
    if not which('lspci'):
        logger.warning('lspci command not found - cannot check for GPU presence')
        return False
    try:
        result = subprocess.run(
            ["lspci", "-nn"],
            capture_output=True,
            text=True,
            check=True
        )
        return any("3D controller" in line for line in result.stdout.splitlines())
    except (subprocess.SubprocessError, FileNotFoundError):
        return False


def get_workernode_gpu_map(site: str, cache: bool = True) -> dict:
    """
    Return a dictionary with the GPU map.

    The GPU map is a dictionary with the local GPU specs collected by the pilot.
    It gets reported to the PanDA server with the getJob call.

    The dictionary is to be sent to {api_url_ssl}/pilot/update_gpu_map.

    :param site: Site name from PQ.resource (str)
    :param cache: should the gpu map be cached? (bool)
    :return: gpu map (dict).
    """
    # first confirm that the workernode actually has a GPU (relies on lspci)
    has_any_gpu = has_gpu()
    if not has_any_gpu:
        logger.info('no GPU detected via lspci')
        return {}
    else:
        logger.info('GPU detected via lspci')
    if not which('nvidia-smi'):
        logger.warning('nvidia-smi command not found - can currently only handle NVIDIA GPUs')
        return {}

    gpu_info = get_gpu_info(site)

    # store the gpu map for caching
    if cache and gpu_info:
        try:
            filename = os.path.join(os.getcwd(), config.Workernode.gpu_map)
            write_json(filename, gpu_info)
        except Exception as exc:
            logger.warning(f'failed to write gpu map: {exc}')

    return gpu_info


def get_workernode_map(site: str, cache: bool = True) -> dict:
    """
    Return a dictionary with the worker node map.

    The worker node map is a dictionary with the local hardware specs collected by the pilot.

    The dictionary is to be sent to {api_url_ssl}/pilot/update_worker_node.

    :param site: ATLAS site name from PQ.resource (str)
    :param cache: should the workernode map be cached? (bool)
    :return: worker node map (dict).
    """
    number_of_cores, ht, sockets, clock_speed, threads_per_core, cores_per_socket, cpu_architecture, cpu_architecture_level = get_cpu_info()
    logical_cpus = number_of_cores * (2 if ht else 1)
    mem = int(get_total_memory())
    try:
        total_local_disk = convert_b_to_gb(get_total_local_disk_size())
    except ValueError:
        total_local_disk = 0

    data = {
        "site": site,
        "host_name": get_node_name(),  # "slot1@wn1.cern.ch",
        "cpu_model": get_cpu_model(),  # "AMD EPYC 7B12",
        "n_logical_cpus": logical_cpus,
        "n_sockets": sockets,
        "cores_per_socket": cores_per_socket,
        "threads_per_core": threads_per_core,
        "cpu_architecture": cpu_architecture,   # "x86_64",
        "cpu_architecture_level": cpu_architecture_level,  # "x86_64-v3",
        "total_memory": mem,
        "total_local_disk": total_local_disk,
    }

    # the clock speed is optional since it is not available for ARM
    if clock_speed and clock_speed > 0.0:
        data["clock_speed"] = clock_speed

    # store the workernode map for caching
    if cache:
        try:
            filename = os.path.join(os.getcwd(), config.Workernode.map)
            write_json(filename, data)
        except Exception as exc:
            logger.warning(f'failed to write workernode map: {exc}')

    return data
