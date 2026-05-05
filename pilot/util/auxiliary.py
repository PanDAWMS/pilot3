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

"""Auxiliary functions."""

import logging
import os
import re
import shlex
import sys

from collections.abc import Set, Mapping
from collections import deque, OrderedDict
from copy import deepcopy
from numbers import Number
from time import sleep
from typing import Any, Dict, Optional, Tuple
from uuid import uuid4

from pilot.util.constants import (
    SUCCESS,
    FAILURE,
    SERVER_UPDATE_FINAL,
    SERVER_UPDATE_NOT_DONE,
    SERVER_UPDATE_RUNNING,
    SERVER_UPDATE_TROUBLE,
    get_pilot_version,
)
from pilot.common.errorcodes import ErrorCodes
from pilot.util.condor import (
    get_globaljobid,
    # update_condor_classad
)
from pilot.util.container import execute
from pilot.util.filehandling import (
    dump,
    grep
)

zero_depth_bases = (str, bytes, Number, range, bytearray)
iteritems = 'items'
logger = logging.getLogger(__name__)
errors = ErrorCodes()


def pilot_version_banner() -> None:
    """Print a pilot version banner."""
    version = f'***  PanDA Pilot version {get_pilot_version()}  ***'
    logger.info('*' * len(version))
    logger.info(version)
    logger.info('*' * len(version))
    logger.info('')

    if is_virtual_machine():
        logger.info('pilot is running in a VM')

    display_architecture_info()
    logger.info('*' * len(version))


def get_pilot_id(version_tag: str) -> str:
    """Return a unique pilot id.

    Used by CondorHT ClassAd.

    Args:
        version_tag: pilot version tag.

    Returns:
        Unique pilot id string.
    """
    unique_id = os.environ.get("GTAG", "unknown")
    pilotversion = os.environ.get('PILOT_VERSION')
    return f'{pilotversion}-{version_tag}-{unique_id}'


def is_virtual_machine() -> bool:
    """Determine if we are running in a virtual machine.

    If we are running inside a VM, then linux will put 'hypervisor' in cpuinfo.
    This function looks for the presence of that.

    Returns:
        True if running in a virtual machine, False otherwise.
    """
    status = False

    # look for 'hypervisor' in cpuinfo
    with open("/proc/cpuinfo", "r", encoding='utf-8') as _fd:
        lines = _fd.readlines()
        for line in lines:
            if "hypervisor" in line:
                status = True
                break

    return status


def display_architecture_info() -> None:
    """Display OS/architecture information from /etc/os-release."""
    logger.info("architecture information:")
    dump("/etc/os-release")


def get_batchsystem_jobid() -> tuple[Optional[str], str]:
    """Identify and return the batch system job id (will be reported to the server).

    Returns:
        A tuple of (batch_system_name, batch_system_job_id). The name is None
        if no known batch system is detected.
    """
    # BQS (e.g. LYON)
    batchsystem_dict = {'QSUB_REQNAME': 'BQS',
                        'BQSCLUSTER': 'BQS',  # BQS alternative
                        'PBS_JOBID': 'Torque',
                        'LSB_JOBID': 'LSF',
                        'JOB_ID': 'Grid Engine',  # Sun's Grid Engine
                        'clusterid': 'Condor',  # Condor (variable sent through job submit file)
                        'SLURM_JOB_ID': 'SLURM',
                        'K8S_JOB_ID': 'Kubernetes'}

    for key, value in list(batchsystem_dict.items()):
        if key in os.environ:
            return value, os.environ.get(key, '')

    # Condor (get jobid from classad file)
    if '_CONDOR_JOB_AD' in os.environ:
        try:
            ret = get_globaljobid()
        except OSError as exc:
            logger.warning(f"failed to read HTCondor job classAd: {exc}")
        else:
            return "Condor", ret
    return None, ""


def get_job_scheduler_id() -> str:
    """Get the job scheduler id from the environment variable PANDA_JSID.

    Returns:
        Job scheduler id, or 'unknown' if the environment variable is not set.
    """
    return os.environ.get("PANDA_JSID", "unknown")


def whoami() -> str:
    """Return the name of the pilot user.

    Returns:
        Output of the whoami command.
    """
    _, who_am_i, _ = execute('whoami', mute=True)

    return who_am_i


def get_error_code_translation_dictionary() -> dict:
    """Define the error code translation dictionary.

    Returns:
        Populated error code translation dictionary mapping pilot error codes
        to [shell_exit_code, meaning] pairs.
    """
    error_code_translation_dictionary = {
        -1: [64, "Site offline"],
        errors.CVMFSISNOTALIVE: [64, "CVMFS is not responding"],  # same exit code as site offline
        errors.GENERALERROR: [65, "General pilot error, consult batch log"],  # added to traces object
        errors.MKDIR: [66, "Could not create directory"],  # added to traces object
        errors.NOSUCHFILE: [67, "No such file or directory"],  # added to traces object
        errors.NOVOMSPROXY: [68, "Voms proxy not valid"],  # added to traces object
        errors.NOPROXY: [68, "Proxy not valid"],  # added to traces object
        errors.CERTIFICATEHASEXPIRED: [68, "Proxy not valid"],
        errors.NOLOCALSPACE: [69, "No space left on local disk"],  # added to traces object
        errors.UNKNOWNEXCEPTION: [70, "Exception caught by pilot"],  # added to traces object
        errors.QUEUEDATA: [71, "Pilot could not download queuedata"],  # tested
        errors.QUEUEDATANOTOK: [72, "Pilot found non-valid queuedata"],  # not implemented yet, error code added
        errors.NOSOFTWAREDIR: [73, "Software directory does not exist"],  # added to traces object
        errors.JSONRETRIEVALTIMEOUT: [74, "JSON retrieval timed out"],  # ..
        errors.BLACKHOLE: [75, "Black hole detected in file system"],  # ..
        errors.MIDDLEWAREIMPORTFAILURE: [76, "Failed to import middleware module"],  # added to traces object
        errors.MISSINGINPUTFILE: [77, "Missing input file in SE"],  # should pilot report this type of error to wrapper?
        errors.PANDAQUEUENOTACTIVE: [78, "PanDA queue is not active"],
        errors.COMMUNICATIONFAILURE: [79, "PanDA server communication failure"],
        errors.PROXYTOOSHORT: [80, "Proxy too short"],  # added to traces object
        errors.REACHEDMAXTIME: [81, "Reached maximum time limit"],  # added to traces object
        errors.NOJOBSINPANDA: [82, "No jobs in PanDA"],  # added to traces object
        errors.PANDAQUEUENOTONLINE: [83, "Site offline"],
        errors.KILLSIGNAL: [137, "General kill signal"],  # Job terminated by unknown kill signal
        errors.SIGTERM: [143, "Job killed by signal: SIGTERM"],  # 128+15
        errors.SIGQUIT: [131, "Job killed by signal: SIGQUIT"],  # 128+3
        errors.SIGSEGV: [139, "Job killed by signal: SIGSEGV"],  # 128+11
        errors.SIGXCPU: [152, "Job killed by signal: SIGXCPU"],  # 128+24
        errors.SIGUSR1: [138, "Job killed by signal: SIGUSR1"],  # 128+10
        errors.SIGINT: [130, "Job killed by signal: SIGINT"],  # 128+2
        errors.SIGBUS: [135, "Job killed by signal: SIGBUS"]   # 128+7
    }

    return error_code_translation_dictionary


def convert_signal_to_exit_code(signal: str) -> int:
    """Convert a signal name to an exit code.

    Args:
        signal: Signal name (e.g. 'SIGTERM').

    Returns:
        Corresponding pilot error code.
    """
    if signal == "SIGINT":
        exitcode = errors.SIGINT
    elif signal == "SIGTERM":
        exitcode = errors.SIGTERM
    elif signal == "SIGQUIT":
        exitcode = errors.SIGQUIT
    elif signal == "SIGSEGV":
        exitcode = errors.SIGSEGV
    elif signal == "SIGXCPU":
        exitcode = errors.SIGXCPU
    elif signal == "SIGUSR1":
        exitcode = errors.SIGUSR1
    elif signal == "SIGBUS":
        exitcode = errors.SIGBUS
    else:
        exitcode = errors.KILLSIGNAL

    return exitcode


def shell_exit_code(exit_code: int) -> int:
    """Translate the pilot exit code to a proper exit code for the shell (wrapper).

    Any error code that is to be converted by this function should be added to the
    traces object like: ``traces.pilot['error_code'] = errors.<ERRORCODE>``.
    The traces object will be checked by the pilot module.

    Restricts user (pilot) exit codes to the range 64–113, as suggested by
    http://tldp.org/LDP/abs/html/exitcodes.html. Uses exit code 137 for kill
    signal error codes.

    Args:
        exit_code: Pilot error code.

    Returns:
        Standard shell exit code.
    """
    error_code_translation_dictionary = get_error_code_translation_dictionary()

    ret = FAILURE
    if exit_code in error_code_translation_dictionary:
        ret = error_code_translation_dictionary.get(exit_code)[0]  # Only return the shell exit code, not the error meaning
    elif exit_code != 0:
        print(f"no translation to shell exit code for error code {exit_code}")
    else:
        ret = SUCCESS
    return ret


def convert_to_pilot_error_code(exit_code: int) -> int:
    """Revert a batch system exit code back to a pilot error code.

    Note: this function is used by Harvester.

    Args:
        exit_code: Batch system exit code.

    Returns:
        Corresponding pilot error code.
    """
    error_code_translation_dictionary = get_error_code_translation_dictionary()

    list_of_keys = [key for (key, value) in error_code_translation_dictionary.items() if value[0] == exit_code]
    # note: do not use logging object as this function is used by Harvester
    if not list_of_keys:
        print(f'unknown exit code: {exit_code} (no matching pilot error code)')
        list_of_keys = [-1]
    elif len(list_of_keys) > 1:
        print(f'found multiple pilot error codes: {list_of_keys}')

    return list_of_keys[0]


def get_size(obj_0: Any) -> int:
    """Recursively iterate to sum size of object and members.

    Note: for size measurement to work, the object must have set the data
    members in the ``__init__()``.

    Args:
        obj_0: Object to be measured.

    Returns:
        Size of the object in bytes.
    """
    _seen_ids = set()

    def inner(obj):
        obj_id = id(obj)
        if obj_id in _seen_ids:
            return 0

        _seen_ids.add(obj_id)
        size = sys.getsizeof(obj)
        if isinstance(obj, zero_depth_bases):
            pass  # bypass remaining control flow and return
        elif isinstance(obj, OrderedDict):
            pass  # can currently not handle this
        elif isinstance(obj, (tuple, list, Set, deque)):
            size += sum(inner(i) for i in obj)
        elif isinstance(obj, Mapping) or hasattr(obj, iteritems):
            try:
                size += sum(inner(k) + inner(v) for k, v in getattr(obj, iteritems)())
            except Exception:  # as exc
                pass
                # <class 'collections.OrderedDict'>: unbound method iteritems() must be called
                # with OrderedDict instance as first argument (got nothing instead)
                #logger.debug('exception caught for obj=%s: %s', (str(obj), exc))

        # Check for custom object instances - may subclass above too
        if hasattr(obj, '__dict__'):
            size += inner(vars(obj))
        if hasattr(obj, '__slots__'):  # can have __slots__ with __dict__
            size += sum(inner(getattr(obj, s)) for s in obj.__slots__ if hasattr(obj, s))

        return size

    return inner(obj_0)


def get_pilot_state(job: Any = None) -> str:
    """Return the current pilot (job) state.

    If the job object does not exist, the environmental variable
    PILOT_JOB_STATE will be queried instead.

    Args:
        job: Optional job object.

    Returns:
        Current pilot (job) state string.
    """
    return job.state if job else os.environ.get('PILOT_JOB_STATE', 'unknown')


def set_pilot_state(job: Any = None, state: str = '') -> None:
    """Set the internal pilot state.

    Note: this function should update the global/singleton object but currently
    uses an environmental variable (PILOT_JOB_STATE). The function does not
    update ``job.state`` if it is already set to finished or failed. The
    environmental variable PILOT_JOB_STATE will always be set, in case the job
    object does not exist.

    Args:
        job: Optional job object.
        state: Internal pilot state to set.
    """
    os.environ['PILOT_JOB_STATE'] = state

    if job and job.state != 'failed':
        job.state = state
    # update_condor_classad(state=state)


def check_for_final_server_update(update_server: bool) -> None:
    """Check for the final server update.

    Do not set graceful stop if pilot has not finished sending the final job
    update. This function sleeps for a maximum of 20*30 s until the
    SERVER_UPDATE env variable has been set to SERVER_UPDATE_FINAL.

    When SERVER_UPDATE is RUNNING (job was mid-execution when max time was
    reached), wait up to _MAX_RUNNING_WAIT_ITERATIONS * _RUNNING_WAIT_SLEEP s
    for the state to advance to a terminal value before returning. This
    prevents graceful_stop from being set before the final server update has
    had a chance to fire, which would otherwise result in a lost heartbeat on
    the server side.

    Args:
        update_server: Whether the pilot should update the server (args.update_server).
    """
    _MAX_RUNNING_WAIT_ITERATIONS = 5
    _RUNNING_WAIT_SLEEP = 10  # s
    max_i = 20
    counter = 0

    # abort if in startup stage or if in final update stage
    server_update = os.environ.get('SERVER_UPDATE', '')
    logger.info(f'current server update state: {server_update}')
    logger.info(f'update_server={update_server}')
    if server_update == SERVER_UPDATE_NOT_DONE:
        return

    # If the server update is still in the RUNNING state (i.e. the job was still
    # executing when max time was reached), wait a bounded time for the state to
    # advance before falling through to the main polling loop. Without this wait,
    # graceful_stop is set immediately and every downstream thread (job_monitor,
    # failed_post, queue_monitor) exits before the final PanDA updateJob call is
    # issued, causing a lost heartbeat on the server.
    if server_update == SERVER_UPDATE_RUNNING and update_server:
        logger.info('server update is still in RUNNING state - waiting for it to advance')
        running_counter = 0
        while running_counter < _MAX_RUNNING_WAIT_ITERATIONS:
            sleep(_RUNNING_WAIT_SLEEP)
            running_counter += 1
            server_update = os.environ.get('SERVER_UPDATE', '')
            logger.info(f'server update state after {running_counter * _RUNNING_WAIT_SLEEP}s: {server_update}')
            if server_update != SERVER_UPDATE_RUNNING:
                break
        if server_update == SERVER_UPDATE_RUNNING:
            logger.warning('server update is still in RUNNING state after waiting - proceeding anyway')
            # The job_monitor will have sent (or is about to send) the update and will set
            # SERVER_UPDATE=FINAL directly. Running the full 20*30 s outer loop when SERVER_UPDATE
            # is still RUNNING means no UPDATING_FINAL transition is in progress, so there is
            # nothing to wait for. Return here to avoid a ~10-minute unnecessary stall.
            return

    while counter < max_i and update_server:
        server_update = os.environ.get('SERVER_UPDATE', '')
        if server_update in (SERVER_UPDATE_FINAL, SERVER_UPDATE_TROUBLE):
            logger.info('server update done, finishing')
            break
        logger.info(f'server update not finished (#{counter + 1}/#{max_i})')
        sleep(30)
        counter += 1


def get_resource_name() -> str:
    """Return the name of the resource.

    Only set for HPC resources (e.g. Cori); returns 'grid' otherwise.

    Returns:
        Resource name string.
    """
    resource_name = os.environ.get('PILOT_RESOURCE_NAME', '').lower()
    if not resource_name:
        resource_name = 'grid'
    return resource_name


def get_object_size(obj: Any, seen: Optional[set] = None) -> int:
    """Recursively find the size of any object.

    Args:
        obj: Object to measure.
        seen: Set of already-visited object ids used to handle self-referential
            objects. Pass ``None`` (the default) to start a fresh traversal.

    Returns:
        Total size of the object in bytes.
    """
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0

    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_object_size(v, seen) for v in obj.values()])
        size += sum([get_object_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_object_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_object_size(i, seen) for i in obj])

    return size


def show_memory_usage() -> None:
    """Display the current memory usage by the pilot process."""
    _, _stdout, _ = get_memory_usage(os.getpid())
    _value = extract_memory_usage_value(_stdout)
    logger.debug(f'current pilot memory usage:\n\n{_stdout}\n\nusage: {_value} kB\n')


def get_memory_usage(pid: int) -> tuple[int, str, str]:
    """Return the memory usage string for the given process.

    Executes ``ps aux -q <pid>`` to obtain usage information.

    Args:
        pid: Process id.

    Returns:
        A tuple of (exit_code, stdout, stderr) from the ps command.
    """
    return execute(f'ps aux -q {pid}', timeout=60)


def extract_memory_usage_value(output: str) -> str:
    """Extract the memory usage value from the ps output (in kB).

    Example ps output::

        USER       PID %CPU %MEM    VSZ   RSS TTY      STAT START   TIME COMMAND
        usatlas1 13917  1.5  0.0 1324968 152832 ?      Sl   09:33   2:55 /bin/python2 ..

    The RSS column (index 5) contains the value in kB, e.g. 152832.

    Args:
        output: Raw ps command output.

    Returns:
        Memory value in kB as a string, or '(unknown)' if parsing fails.
    """
    memory_usage = "(unknown)"
    for row in output.split('\n'):
        try:
            memory_usage = " ".join(row.split()).split(' ')[5]
        except (IndexError, ValueError):
            memory_usage = "(unknown)"
        else:
            break

    return memory_usage


def cut_output(txt: str, cutat: int = 1024, separator: str = '\n[...]\n') -> str:
    """Cut the given string if longer than 2 * cutat characters.

    Args:
        txt: Text to be cut.
        cutat: Maximum length of each retained head/tail segment.
        separator: Text inserted between the head and tail segments.

    Returns:
        Possibly truncated text with separator inserted in the middle.
    """
    if len(txt) > 2 * cutat:
        txt = txt[:cutat] + separator + txt[-cutat:]

    return txt


def has_instruction_sets(instruction_sets: list) -> str:
    """Determine whether a given list of CPU instruction sets is available.

    Uses grep to search in /proc/cpuinfo (both upper and lower case).

    Example::

        has_instruction_sets(['AVX', 'AVX2', 'SSE4_2', 'XXX']) -> "AVX|AVX2|SSE4_2"

    Args:
        instruction_sets: List of instruction set names to check (e.g. ['AVX2']).

    Returns:
        Pipe-separated string of detected instruction sets.
    """
    ret = ""

    for instr in instruction_sets:
        pattern = re.compile(fr'{instr.lower()}[^ ]*', re.IGNORECASE)
        out = grep(patterns=[pattern], file_name="/proc/cpuinfo")

        for stdout in out:
            if instr.upper() not in ret and (instr.lower() in stdout.split() or instr.upper() in stdout.split()):
                ret += f'|{instr.upper()}' if ret else instr.upper()

    return ret


def has_instruction_sets_old(instruction_sets: list) -> str:
    """Determine whether a given list of CPU instruction sets is available (legacy implementation).

    Uses grep to search in /proc/cpuinfo (both upper and lower case).

    Example::

        has_instruction_sets_old(['AVX', 'AVX2', 'SSE4_2', 'XXX']) -> "AVX|AVX2|SSE4_2"

    Args:
        instruction_sets: List of instruction set names to check (e.g. ['AVX2']).

    Returns:
        Pipe-separated string of detected instruction sets.
    """
    ret = ""
    pattern = ""

    for instr in instruction_sets:
        pattern += fr'\|{instr.lower()}[^ ]*\|{instr.upper()}[^ ]*' if pattern else fr'{instr.lower()}[^ ]*\|{instr.upper()}[^ ]*'
    cmd = f"grep -o \'{pattern}\' /proc/cpuinfo"

    exit_code, stdout, stderr = execute(cmd)
    if not exit_code and not stderr:
        for instr in instruction_sets:
            if instr.lower() in stdout.split() or instr.upper() in stdout.split():
                ret += f'|{instr.upper()}' if ret else instr.upper()

    return ret


def locate_core_file(cmd: str = '', pid: int = 0) -> str:
    """Locate the core file produced by gdb.

    Args:
        cmd: Optional command string containing the pid corresponding to the
            core file.
        pid: Optional pid to use with core file (core.<pid>).

    Returns:
        Path to the core file, or None if it could not be located.
    """
    path = None
    if not pid and cmd:
        pid = get_pid_from_command(cmd)
    if pid:
        filename = f'core.{pid}'
        path = os.path.join(os.environ.get('PILOT_HOME', '.'), filename)
        if os.path.exists(path):
            logger.debug(f'found core file at: {path}')

        else:
            logger.debug(f'did not find {filename} in {path}')
    else:
        logger.warning('cannot locate core file since pid could not be extracted from command')

    return path


def get_pid_from_command(cmd: str, pattern: str = r'gdb --pid (\d+)') -> int:
    r"""Identify an explicit process id in the given command.

    Example::

        cmd = "gdb --pid 19114 -ex 'generate-core-file'"
        get_pid_from_command(cmd)  # -> 19114

    Args:
        cmd: Command string containing a pid.
        pattern: Regex pattern used to extract the pid. Must contain a capture
            group for the numeric pid.

    Returns:
        Extracted pid as an integer, or None if no match was found.
    """
    pid = None
    match = re.search(pattern, cmd)
    if match:
        try:
            pid = int(match.group(1))
        except (IndexError, ValueError):
            pid = None
    else:
        logger.warning(f"no match for pattern \'{pattern}\' in command=\'{cmd}\'")

    return pid


def list_hardware() -> str:
    """Execute lshw to list local hardware.

    Returns:
        Output of ``lshw -numeric -C display``, or an empty string if the
        command is not available.
    """
    _, stdout, stderr = execute('lshw -numeric -C display', mute=True)
    if 'command not found' in stdout or 'command not found' in stderr:
        stdout = ''
    return stdout


def get_display_info() -> tuple[str, str]:
    """Extract the product and vendor from the lshw command output.

    Example lshw output::

           product: GD 5446 [1013:B8]
           vendor: Cirrus Logic [1013]

    Returns:
        A tuple of (product, vendor) strings. Both are empty strings if lshw
        is unavailable or produces no relevant output.
    """
    vendor = ''
    product = ''
    stdout = list_hardware()
    if stdout:
        vendor_pattern = re.compile(r'vendor\:\ (.+)\ .')
        product_pattern = re.compile(r'product\:\ (.+)\ .')

        for line in stdout.split('\n'):
            if 'vendor' in line:
                result = re.findall(vendor_pattern, line)
                if result:
                    vendor = result[0]
            elif 'product' in line:
                result = re.findall(product_pattern, line)
                if result:
                    product = result[0]

    return product, vendor


def get_key_value(catchall: str, key: str = 'SOMEKEY') -> str:
    """Return the value corresponding to key in a catchall string.

    Args:
        catchall: Free-form string containing zero or more ``key=value`` pairs.
        key: Key name to look up.

    Returns:
        Value associated with the key, or None if the key is not present.
    """
    # ignore any non-key-value pairs that might be present in the catchall string
    _dic = dict(_str.split('=', 1) for _str in catchall.split() if '=' in _str)

    return _dic.get(key)


def is_string(obj: Any) -> bool:
    """Determine if the passed object is a string.

    Args:
        obj: Object to test.

    Returns:
        True if obj is a string, False otherwise.
    """
    return isinstance(obj, str)


def find_pattern_in_list(input_list: list, pattern: str) -> str:
    """Search for the given pattern in the input list.

    Args:
        input_list: List of strings to search.
        pattern: Regular expression pattern to match against each line.

    Returns:
        First matched substring, or None if no match was found.
    """
    found = None
    for line in input_list:
        out = re.search(pattern, line)
        if out:
            found = out[0]
            break

    return found


def sort_words(input_str: str) -> str:
    """Sort the words in the given string.

    Example::

        sort_words('bbb fff aaa')  # -> 'aaa bbb fff'

    Args:
        input_str: Input string whose whitespace-separated words will be sorted.

    Returns:
        String with words sorted alphabetically. Returns the original string
        unchanged if sorting fails.
    """
    output_str = input_str
    try:
        tmp = output_str.split()
        tmp.sort()
        output_str = ' '.join(tmp)
    except (AttributeError, TypeError) as exc:
        logger.warning(f'failed to sort input string: {input_str}, exc={exc}')

    return output_str


def grep_str(patterns: list, stdout: str) -> list:
    """Search for the patterns in the given stdout string.

    For expected large stdout, prefer ``FileHandling.grep()`` instead.

    Args:
        patterns: List of regexp pattern strings.
        stdout: Text to search.

    Returns:
        List of lines from stdout that match any of the given patterns.
    """
    matched_lines = []
    _pats = []
    for pattern in patterns:
        _pats.append(re.compile(pattern))

    lines = stdout.split('\n')
    for line in lines:
        # can the search pattern be found?
        for _cp in _pats:
            if re.search(_cp, line):
                matched_lines.append(line)

    return matched_lines


class TimeoutException(Exception):
    """Timeout exception."""

    def __init__(self, message: str, timeout: int = None, *args: Any):
        """Initialize the TimeoutException.

        Args:
            message: Human-readable description of the timeout.
            timeout: Timeout duration in seconds.
            *args: Additional positional arguments forwarded to Exception.
        """
        self.timeout = timeout
        self.message = message
        self._error_code = 1334
        super(TimeoutException, self).__init__(*args)

    def __str__(self):
        """Return a string representation of this exception."""
        tmp = f' : {repr(self.args)}' if self.args else ''
        return f"{self.__class__.__name__}: {self.message}, timeout={self.timeout} seconds{tmp}"


def correct_none_types(data_dict: dict) -> dict:
    """Correct None types in the given dictionary.

    Replaces string values ``'None'`` and ``'null'`` with the Python ``None``
    singleton.

    Args:
        data_dict: Dictionary potentially containing ``'None'`` or ``'null'``
            string values.

    Returns:
        The same dictionary with corrected None types.
    """
    for key, value in data_dict.items():
        if value == 'None' or value == 'null':
            data_dict[key] = None
    return data_dict


def is_command_available(command: str) -> bool:
    """Check if the given command is available on the system.

    Args:
        command: Command string to check (may include arguments).

    Returns:
        True if the command executable is accessible and executable, False otherwise.
    """
    args = shlex.split(command)

    return os.access(args[0], os.X_OK)


def is_kubernetes_resource() -> bool:
    """Determine if the pilot is running on a Kubernetes resource.

    Returns:
        True if running on Kubernetes, False otherwise.
    """
    if os.environ.get('K8S_JOB_ID'):
        return True
    else:
        return False


def uuidgen_t() -> str:
    """Generate a UUID string in the same format as ``uuidgen -t``.

    Returns:
        UUID string in the format ``'00000000-0000-0000-0000-000000000000'``.
    """
    return str(uuid4())


def list_items(items: list) -> None:
    """List the items in the given list as a numbered log entry.

    Args:
        items: List of items to log.
    """
    for i, item in enumerate(items):
        logger.info(f'{i + 1}: {item}')


def mask_sensitive_response(res: Dict[str, Any], key: str = "pilotSecrets", mask: str = "********") -> Tuple[Dict[str, Any], Optional[Any]]:
    """Return a masked copy of ``res`` for logging and the extracted sensitive value.

    Does not mutate the original ``res``. Looks first in ``res.get('data')``,
    then at the top-level ``res``.

    Args:
        res: Response dictionary that may contain sensitive data.
        key: Dictionary key whose value should be masked.
        mask: Replacement string used to obscure the sensitive value.

    Returns:
        A tuple of (masked_copy, extracted_value_or_None).
    """
    if not isinstance(res, dict):
        return res, None

    log_res = deepcopy(res)
    extracted = None

    # prefer nested 'data' container if present
    data_node = log_res.get('data') or {}
    if isinstance(data_node, dict) and key in data_node:
        extracted = res.get('data', {}).get(key)
        data_node[key] = mask
        # ensure the masked node is placed back if it was a copy
        log_res['data'] = data_node
    elif key in log_res:
        extracted = res.get(key)
        log_res[key] = mask

    return log_res, extracted
