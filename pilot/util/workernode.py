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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-26

"""Worker-node information collection utilities (memory, CPU, disk, architecture)."""

from __future__ import annotations
import logging
import os
import re
import socket
import subprocess
from shutil import which
from typing import (
    Any,
    Optional,
    Tuple
)

#from subprocess import getoutput

from pilot.common.exception import (
    PilotException,
    ErrorCodes
)
from pilot.util.auxiliary import sort_words
from pilot.util.condor import get_condor_node_name
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


def get_local_disk_space(path: Optional[str]) -> float:
    """
    Return remaining disk space for the disk in the given path.

    Uses `df -mP` to obtain the available space for the filesystem containing
    *path* and returns the available space in megabytes (MB).

    Args:
        path (Optional[str]): Path to a file or directory on the target filesystem.
            If ``None`` or empty, the current working directory is used.

    Returns:
        float: Available disk space in MB.

    Raises:
        PilotException: If the command produces no output or the output cannot be
            parsed into a float value. The exception uses ``ErrorCodes.UNKNOWNEXCEPTION``.
    """
    # first check for the new env vars (storageLimitMiB and storageRequestMiB) and, if present,
    # use min(storageLimitMiB, site_heuristic) where site_heuristic=maxwdir * nCores * site_scale
    # use heuristic as the fallback when the env vars aren’t set
    storage_limit_mib = os.environ.get('storageLimitMiB')
    storage_request_mib = os.environ.get('storageRequestMiB')
    if storage_limit_mib and storage_request_mib:
        try:
            storage_limit_mib = float(storage_limit_mib)
            storage_limit_mb = storage_limit_mib * 1.048576  # convert MiB to MB
            logger.debug(f"storage_limit_mib={storage_limit_mib} MiB converted to storage_limit_mb={storage_limit_mb} MB")
            return storage_limit_mb
            #storage_scale = 1  # for now, could be made configurable later (site specific)
            #site_heuristic = infosys.queuedata.maxwdir * infosys.queuedata.ncores * storage_scale
            #disk = min(storage_limit_mib, site_heuristic)
            #logger.debug(f"storage_limit_mib={storage_limit_mib} MiB, storage_limit_mb={storage_limit_mb} MB, "
            #             f"site_heuristic={site_heuristic} MB")
            #logger.info(f'using disk space from env vars: storageLimitMiB={storage_limit_mib} MiB, '
            #            f'-> using {disk} MiB')
            # return disk
        except ValueError as error:
            logger.warning(f'exception caught while converting storageLimitMiB/storageRequestMiB to float: {error} '
                           f'(will use df command instead)')
    else:
        logger.debug('storageLimitMiB/storageRequestMiB not set - will use df command to get disk space')

    # Default to current directory when no path is provided
    if not path:
        path = "."

    # -mP = blocks of 1024*1024 (MB) and POSIX format
    cmd = f"df -mP {path}"
    _, stdout, stderr = execute(cmd, timeout=60)

    # Ensure stdout is a string
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", errors="ignore")

    if stdout:
        logger.debug(f'stdout={stdout}')
        logger.debug(f'stderr={stderr}')
        try:
            lines = stdout.splitlines()
            if len(lines) < 2:
                raise ValueError("unexpected df output")
            # Available field is the 4th column (index 3) on the second line
            disk = float(lines[1].split()[3])
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
    """Return the total memory in MB read from /proc/meminfo.

    Returns:
        Total memory in MB, or 0.0 if the file cannot be read or parsed.
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
    """Return the CPU flags string from /proc/cpuinfo.

    Args:
        _sorted: If True, flag tokens are sorted alphabetically before
            being returned.

    Returns:
        Space-separated CPU flags string, or an empty string if unavailable.
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


def get_cpu_arch_internal() -> str:
    """Return the CPU architecture string using the internal cpu_arch.py script.

    The script lives at ``pilot/scripts/cpu_arch.py``. For background see
    https://its.cern.ch/jira/browse/ATLINFR-4844.

    Returns:
        CPU architecture string, or an empty string if the script fails.
    """
    cpu_arch = ''

    # copy pilot source into container directory, unless it is already there
    script = 'cpu_arch.py'
    srcdir = os.path.join(os.environ.get('PILOT_SOURCE_DIR', '.'), 'pilot3')
    script_dir = os.path.join(srcdir, 'pilot/scripts')

    pythonpath = os.environ.get('PYTHONPATH', '')
    if script_dir not in pythonpath:
        os.environ['PYTHONPATH'] = pythonpath + ':' + script_dir if pythonpath else script_dir

    # CPU arch script has now been copied, time to execute it
    ec, stdout, stderr = execute(f'python3 {script_dir}/{script}')
    if ec or stderr:
        logger.debug(f'ec={ec}, stdout={stdout}, stderr={stderr}')
    else:
        cpu_arch = stdout.strip()
        logger.debug(f'CPU arch script returned: {cpu_arch}')

    return cpu_arch


def get_cpu_arch() -> str:
    """Return the CPU architecture string via the experiment-specific plugin.

    Delegates to ``pilot.user.<PILOT_USER>.utilities.get_cpu_arch()``. For
    background see https://its.cern.ch/jira/browse/ATLINFR-4844.

    If the plugin returns an empty string, falls back to
    :func:`get_cpu_arch_internal` which invokes the bundled
    ``pilot/scripts/cpu_arch.py`` directly.

    Returns:
        CPU architecture string, or an empty string when not reported.
    """
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__('pilot.user.%s.utilities' % pilot_user, globals(), locals(), [pilot_user], 0)
    cpu_arch = user.get_cpu_arch()
    if not cpu_arch:
        logger.info('no CPU architecture reported by plugin, trying internal fallback')
        cpu_arch = get_cpu_arch_internal()

    return cpu_arch


def collect_workernode_info(path: Optional[str] = None) -> tuple:
    """Collect worker-node information: memory, CPU frequency, and disk space.

    Args:
        path: Path used to measure available disk space. Defaults to the
            current working directory when ``None``.

    Returns:
        Tuple of ``(memory_mb, cpu_freq_mhz, disk_mb)`` where *disk_mb* may
        be ``None`` if the disk-space query raises a ``PilotException``.
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


def get_disk_space(queuedata: Any) -> int:
    """Return the disk space in MB that should be available for running the job.

    Compares the locally available disk space with the site-configured maximum
    (``queuedata.maxwdir``) and returns the smaller of the two. Used internally
    by the job dispatcher.

    Args:
        queuedata: Queue data object with a ``maxwdir`` attribute giving the
            site-configured disk limit in MB.

    Returns:
        Available disk space in MB (the lesser of local availability and the
        site maximum).
    """
    # --- non Job related queue data
    # jobinfo provider is required to consider overwriteAGIS data coming from Job
    _maxinputsize = queuedata.maxwdir
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


def get_node_name() -> str:
    """Return the local worker-node hostname.

    Checks ``PANDA_HOSTNAME`` first, then ``os.uname()``, then
    ``socket.gethostname()``. The result is further processed by
    :func:`get_condor_node_name` to handle HTCondor slot names.

    Returns:
        Worker-node hostname string.
    """
    if 'PANDA_HOSTNAME' in os.environ:
        host = os.environ.get('PANDA_HOSTNAME')
    elif hasattr(os, 'uname'):
        host = os.uname()[1]
    else:
        host = socket.gethostname()

    return get_condor_node_name(host)


def get_cpu_model() -> str:
    """Return the CPU model name and cache size.

    Reads ``/proc/cpuinfo`` for the ``model name`` and ``cache size`` fields.
    Falls back to ``lscpu`` when those fields are not present.

    Example return value: ``"Intel(R) Xeon(TM) CPU 2.40GHz 512 KB"``.

    Returns:
        CPU model string combining model name and cache size, or ``'UNKNOWN'``
        if neither source provides the information.
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


def lscpu() -> tuple:
    """Execute the ``lscpu`` command and return its output.

    Returns:
        Tuple of ``(exit_code, stdout)`` where *exit_code* is non-zero on
        failure. Returns ``(1, "")`` when ``lscpu`` is not on ``PATH``.
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
    """Read cached CPU metrics from the worker-node map JSON file.

    Returns zeroed/empty values when the cache file does not exist or cannot
    be read.

    Returns:
        Tuple of ``(cores_per_socket, threads_per_core, clock_speed, sockets,
        architecture, architecture_level)``.
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
    """Return detailed CPU topology information.

    Uses the worker-node map cache when available, otherwise falls back to
    ``lscpu`` output.

    Returns:
        Tuple of ``(number_of_cores, ht, sockets, clock_speed, threads_per_core,
        cores_per_socket, architecture, architecture_level)`` where *ht* is
        ``"HT"`` when hyperthreading is active, or an empty string otherwise.
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
    """Append core count, hyperthreading, and socket info to the CPU model string.

    Example::

        'Intel Xeon Processor (Skylake, IBRS) 16384 KB'
        → 'Intel Xeon 10-Core Processor (Skylake, IBRS) 16384 KB HT 2-Sockets'

    Args:
        modelstring: Existing CPU model description string.
        number_of_cores: Total number of physical CPU cores.
        ht: Hyperthreading marker string (``"HT"`` or ``""``).
        sockets: Number of CPU sockets.

    Returns:
        Updated CPU model string, or the original string unchanged when
        *number_of_cores* is 0.
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


def check_hz() -> None:
    """Attempt to read SC_CLK_TCK and log any failure.

    A missing ``SC_CLK_TCK`` sysconf value prevents CPU consumption
    calculations. Any exception is caught and logged at fatal/warning level.
    """
    try:
        _ = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
    except Exception:
        import traceback
        logger.fatal('failed to read SC_CLK_TCK - will not be able to perform CPU consumption calculation')
        logger.warning(traceback.format_exc())


def get_hepspec_per_core() -> str:
    """Return the published HEPSPEC value per core from the HTCondor machine ad.

    Only applicable when running under HTCondor. Requires the
    ``CONDOR_MACHINE_AD`` environment variable to be set.

    Returns:
        HEPSPEC per core as a string, or an empty string if the value cannot
        be determined.
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
    """Return the total size of all local (non-mounted) block devices in bytes.

    Uses ``lsblk -d`` to enumerate local disks. Parsing errors are silently
    ignored and that disk is skipped.

    Returns:
        Aggregate disk size in bytes, or 0 if ``lsblk`` output cannot be parsed.
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
    """Return the CPU frequency in MHz read from /proc/cpuinfo.

    Used as a fallback when psutil cannot provide the clock speed.

    Returns:
        CPU frequency in MHz, or 0.0 if the value cannot be read.
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


# Known nvidia-smi header formats for the CUDA version field, tried in order.
# nvidia-smi occasionally changes its plain-text header layout across driver
# releases, e.g.:
#   legacy:  "... Driver Version: 535.129.03   CUDA Version: 12.2 ..."
#   >=610.x: "... KMD Version: 610.43.02   CUDA UMD Version: 13.3 ..."
# Add new *exact* patterns here as future formats are encountered, ahead of
# the generic fallback below - an exact match documents the format we've
# actually seen in the wild, whereas the fallback is a safety net for formats
# we haven't (e.g. a hypothetical "CUDA Driver Version:" relabeling).
CUDA_VERSION_PATTERNS = (
    r'CUDA Version:\s+([\d.]+)',        # legacy nvidia-smi header
    r'CUDA UMD Version:\s+([\d.]+)',    # nvidia-smi >= 610.x header (KMD/UMD split)
    r'CUDA[A-Za-z\s]*Version:\s+([\d.]+)',  # generic fallback: any "CUDA ... Version:" label
)


def _extract_cuda_version(full_output: str) -> Optional[str]:
    """
    Extract the CUDA version from plain-text nvidia-smi output.

    Tries each pattern in :data:`CUDA_VERSION_PATTERNS` in turn, so that a
    driver/nvidia-smi update which renames or moves the field does not by
    itself break CUDA version reporting - the previous pattern is kept as a
    fallback and a new one can be added alongside it. The last pattern in
    the list is a generic "CUDA ... Version:" fallback that catches label
    variants not yet seen (and not worth a dedicated exact pattern for).

    Args:
        full_output (str): Full stdout from a plain ``nvidia-smi`` invocation.

    Returns:
        Optional[str]: The matched CUDA version string, or None if no known
            pattern matched.
    """
    for pattern in CUDA_VERSION_PATTERNS:
        cuda_match = re.search(pattern, full_output)
        if cuda_match:
            return cuda_match.group(1)

    return None


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

        cuda_version = _extract_cuda_version(full_output)
        if cuda_version is None:
            # None of the known patterns matched - nvidia-smi's output format has
            # likely changed again. Log the full output (rather than just "Unknown")
            # so the new format is captured in the pilot log and a new pattern can
            # be added to CUDA_VERSION_PATTERNS without having to reproduce the issue.
            logger.warning(
                'failed to extract CUDA version from nvidia-smi output using known patterns '
                '- nvidia-smi format may have changed; full output follows for diagnosis:\n'
                f'{full_output}'
            )
            cuda_version = "Unknown"

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
    # make sure that /usr/sbin is in the PATH
    if ':/usr/sbin' not in os.environ.get('PATH', ''):
        os.environ['PATH'] += os.pathsep + '/usr/sbin'

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
    """Return a dictionary of local GPU specifications collected by the pilot.

    Checks for GPU presence via ``lspci``, then queries ``nvidia-smi``. The
    result is reported to the PanDA server with the ``getJob`` call and
    optionally persisted to a local JSON file.

    Args:
        site: Site name from ``PQ.resource``, included in the returned dict.
        cache: If True, the GPU map is written to the configured JSON file.

    Returns:
        Dictionary of GPU info (vendor, model, architecture, VRAM, CUDA
        version, driver version, count), or an empty dict when no GPU is
        detected or ``nvidia-smi`` is unavailable.
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
            filename = os.path.join(os.environ.get('PILOT_HOME'), config.Workernode.gpu_map)
            write_json(filename, gpu_info)
        except Exception as exc:
            logger.warning(f'failed to write gpu map: {exc}')
        else:
            logger.info(f'gpu map written to {filename}')

    return gpu_info


def get_target_architecture() -> dict:
    """Return the worker-node hardware description for the job dispatcher.

    The dispatcher uses this to avoid handing a job to a worker node whose
    hardware cannot run it. Brokerage only guarantees that *some* worker node in
    the queue satisfies the GPU vendor, model and microarchitecture requested by
    the task, so a queue holding both e.g. V100 and A100 nodes could previously
    receive a job on either of them.

    The GPU specifications are taken from the GPU map that the pilot collected
    at startup, i.e. the same keys and values that are reported with the
    ``update_worker_node_gpu`` call, wrapped in the ``gpus`` list expected by
    the ``acquire_jobs`` API. Reading the cached file rather than probing the
    hardware again keeps what the dispatcher matches against identical to what
    the server already has on record, and avoids running ``nvidia-smi`` once per
    job request.

    Nothing is returned unless the GPU map exists and has content: an absent
    ``gpus`` key tells the server that the worker node does not report GPU
    information, which leaves the previous, unchecked behaviour in place. An
    empty ``gpus`` list would instead assert that the node has no GPU at all,
    which the pilot must not claim on the basis of a missing or unreadable file.

    Returns:
        Dictionary of the form ``{"gpus": [{...}]}``, or an empty dict when no
        GPU information is available.
    """
    pilot_home = os.environ.get('PILOT_HOME', '')
    if not pilot_home:
        logger.warning('PILOT_HOME is not set - cannot look for the gpu map')
        return {}

    path = os.path.join(pilot_home, config.Workernode.gpu_map)
    if not os.path.exists(path):
        logger.info(f'no gpu map at {path} - target architecture will not be reported')
        return {}

    try:
        gpu_map = read_json(path)
    except PilotException as exc:
        logger.warning(f'failed to read gpu map from {path}: {exc}')
        return {}

    if not gpu_map:
        # the map is only written when nvidia-smi returned GPU specifications, so an empty
        # file means the pilot has lost the information rather than that there is no GPU
        logger.warning(f'gpu map at {path} is empty - target architecture will not be reported')
        return {}

    if not isinstance(gpu_map, dict):
        logger.warning(f'unexpected gpu map format in {path} ({type(gpu_map).__name__}) - '
                       f'target architecture will not be reported')
        return {}

    return {'gpus': [gpu_map]}


def get_workernode_map(site: str, queue: str, cache: bool = True) -> dict:
    """Return a dictionary of local worker-node hardware specifications.

    Collects CPU topology, memory, disk, and architecture information. The
    result is reported to the PanDA server and optionally written to a local
    cache JSON file.

    Args:
        site: Site name from ``PQ.resource``.
        queue: PanDA queue name from ``PQ.name``.
        cache: If True, the worker-node map is persisted to the configured
            JSON file.

    Returns:
        Dictionary with hardware keys such as ``cpu_model``, ``n_logical_cpus``,
        ``total_memory``, ``total_local_disk``, and others.
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
        "panda_queue": queue,
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
            filename = os.path.join(os.environ.get('PILOT_HOME'), config.Workernode.map)
            write_json(filename, data)
        except Exception as exc:
            logger.warning(f'failed to write workernode map: {exc}')
        else:
            logger.info(f'worker node map written to: {filename}')

    return data
