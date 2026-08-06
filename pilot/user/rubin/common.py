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

"""Common functions for Rubin."""

import logging
import os
from re import findall
from signal import SIGTERM
from typing import Any

from pilot.common.exception import TrfDownloadFailure
from pilot.info.jobdata import JobData
from pilot.util.config import config
from pilot.util.constants import UTILITY_BEFORE_PAYLOAD, UTILITY_AFTER_PAYLOAD_STARTED
from pilot.util.filehandling import read_file
from pilot.util.https import get_base_urls
from .setup import get_analysis_trf

logger = logging.getLogger(__name__)


def sanity_check() -> int:
    """Perform an initial sanity check before doing anything else in a given workflow.

    This function can be used to verify importing of modules that are otherwise used much later, but it is better to abort
    the pilot if a problem is discovered early.

    Returns:
        int: exit code (0 if all is ok, otherwise non-zero exit code).
    """
    return 0


def validate(job: Any) -> bool:
    """Perform user specific payload/job validation.

    Args:
        job: job object.

    Returns:
        bool: True if validation is successful.
    """
    if job:
        pass
    return True


def get_payload_command(job: JobData, args: object = None) -> str:
    """Return the full command for executing the payload.

    The returned string includes the sourcing of all setup files and setting of environment variables.
    By default, the full payload command is assumed to be in the job.jobparams.

    Args:
        job: job object.
        args: pilot arguments.

    Returns:
        str: command.
    """
    # Try to download the trf
    # if job.imagename != "" or "--containerImage" in job.jobparams:
    #    job.transformation = os.path.join(os.path.dirname(job.transformation), "runcontainer")
    #    logger.warning('overwrote job.transformation, now set to: %s' % job.transformation)
    # convert the base URLs for trf downloads to a list (most likely from an empty string)
    base_urls = get_base_urls(args.baseurls)

    ec, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir, base_urls)
    if ec != 0:
        raise TrfDownloadFailure(diagnostics)
    logger.debug(f'user analysis trf: {trf_name}')

    return get_analysis_run_command(job, trf_name)


def get_analysis_run_command(job: object, trf_name: str) -> str:
    """Return the proper run command for the user job.

    Example output: export X509_USER_PROXY=<..>;./runAthena <job parameters> --usePFCTurl --directIn

    Args:
        job: job object.
        trf_name: name of the transform that will run the job. Used when containers are not used.

    Returns:
        str: command.
    """
    cmd = ""

    # add the user proxy
    if 'X509_USER_PROXY' in os.environ and not job.imagename:
        cmd += f"export X509_USER_PROXY={os.environ.get('X509_USER_PROXY')};"

    # set up trfs
    if job.imagename == "":  # user jobs with no imagename defined
        cmd += f'./{trf_name} {job.jobparams}'
    elif trf_name:
        cmd += f'./{trf_name} {job.jobparams}'
    else:
        cmd += f'python {trf_name} {job.jobparams}'

    return cmd


def update_job_data(job: object) -> None:
    """Update/add data to the job object.

    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metaData field and added to other job object fields.

    Args:
        job: job object.
    """
    if job:  # to bypass pylint score 0
        pass


def remove_external_symlinks(workdir: str) -> None:
    """Remove symlinks whose resolved targets lie outside the work directory.

    The ``tar --dereference`` flag causes tar to open and read the target of
    every symlink it encounters.  If any target resolves to a path on a stalled
    remote filesystem (dCache, NFS, CVMFS), tar will enter an uninterruptible
    kernel wait (D state) and cannot be killed until the mount recovers.  This
    function removes such symlinks before the log archive is created so that
    tar never needs to touch a remote filesystem.

    Args:
        workdir: absolute path to the job work directory.
    """
    workdir_real = os.path.realpath(workdir)
    removed = []
    for root, _, files in os.walk(workdir):
        for filename in files:
            path = os.path.join(root, filename)
            if not os.path.islink(path):
                continue
            target = os.readlink(path)
            # Resolve relative symlinks relative to the directory containing the link
            if not os.path.isabs(target):
                target = os.path.join(os.path.dirname(path), target)
            target_real = os.path.realpath(target)
            if not target_real.startswith(workdir_real + os.sep) and target_real != workdir_real:
                try:
                    os.remove(path)
                    removed.append(path)
                except OSError as exc:
                    logger.warning(f'failed to remove external symlink {path}: {exc}')
    if removed:
        logger.info(f'removed {len(removed)} external symlink(s) prior to log creation: {removed}')


def remove_redundant_files(workdir: str, outputfiles: list = None, piloterrors: list = None, debugmode: bool = False) -> None:
    """Remove redundant files and directories prior to creating the log file.

    Removes symlinks whose targets lie outside the work directory before the
    log archive is created.  The ``tar --dereference`` flag used during log
    creation would otherwise cause tar to open those targets, which can trigger
    an uninterruptible kernel wait if the remote mount is stalled.

    Args:
        workdir: working directory.
        outputfiles: list of output files (unused, reserved for future use).
        piloterrors: list of Pilot assigned error codes (unused, reserved for future use).
        debugmode: True if debug mode has been switched on (unused, reserved for future use).
    """
    if outputfiles or piloterrors or debugmode:  # reserved for future use
        pass

    # warning: removing the external symlinks leads to problems with logs (the GTAG won't get updated in the gs copytool)
    # workdir = os.path.abspath(workdir)
    # remove_external_symlinks(workdir)


def get_utility_commands(order: int = None, job: JobData = None, base_urls: list = None) -> dict:
    """Return a dictionary of utility commands and arguments to be executed in parallel with the payload.

    This could e.g. be memory and network monitor commands. A separate function can be used to determine the
    corresponding command setups using the utility command name.
    If the optional order parameter is set, the function should return the list of corresponding commands.
    E.g. if order=UTILITY_BEFORE_PAYLOAD, the function should return all commands that are to be executed before the
    payload. If order=UTILITY_WITH_PAYLOAD, the corresponding commands will be prepended to the payload execution
    string. If order=UTILITY_AFTER_PAYLOAD_STARTED, the commands that should be executed after the payload has been started
    should be returned.

    FORMAT: {'command': <command>, 'args': <args>}

    Args:
        order: optional sorting order (see pilot.util.constants).
        job: optional job object.
        base_urls: optional list of base URLs.

    Returns:
        dict: dictionary of utilities to be executed in parallel with the payload.
    """
    if order or job or base_urls:  # to bypass pylint score 0
        pass

    return {}


def get_utility_command_setup(name: str, job: object, setup: str = None) -> str:
    """Return the proper setup for the given utility command.

    If a payload setup is specified, it will be taken into account.

    Args:
        name: utility name.
        job: job object.
        setup: optional setup string.

    Returns:
        str: setup string.
    """
    if name or job or setup:  # to bypass pylint score 0
        pass

    return ""


def get_utility_command_execution_order(name: str) -> int:
    """Determine whether the given utility command should be executed before or after the payload.

    Args:
        name: utility name.

    Returns:
        int: execution order constant (UTILITY_BEFORE_PAYLOAD or UTILITY_AFTER_PAYLOAD_STARTED).
    """
    # example implementation
    if name == 'monitor':
        return UTILITY_BEFORE_PAYLOAD

    return UTILITY_AFTER_PAYLOAD_STARTED


def post_utility_command_action(name: str, job: object) -> None:
    """Perform post action for given utility command.

    Args:
        name: name of utility command.
        job: job object.
    """
    if name or job:  # to bypass pylint score 0
        pass


def get_utility_command_kill_signal(name: str) -> int:
    """Return the proper kill signal used to stop the utility command.

    Args:
        name: utility name.

    Returns:
        int: kill signal.
    """
    if name:  # to bypass pylint score 0
        pass

    return SIGTERM


def get_utility_command_output_filename(name: str, selector: bool = None) -> str:
    """Return the filename to the output of the utility command.

    Args:
        name: utility name.
        selector: optional special conditions flag.

    Returns:
        str: filename.
    """
    if name or selector:  # to bypass pylint score 0
        pass

    return ""


def verify_job(job: object) -> bool:
    """Verify job parameters for specific errors.

    Note:
      in case of problem, the function should set the corresponding pilot error code using
      job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())

    Args:
        job: job object.

    Returns:
        bool: True if job parameters are verified.
    """
    status = True

    # ..

    # make sure there were no earlier problems
    if status and job.piloterrorcodes:
        logger.warning(f'job has errors: {job.piloterrorcodes}')
        status = False

    return status


def update_stagein(job: object) -> None:
    """Update stage-in information if necessary.

    In case special files need to be skipped during stage-in, the job.indata list can be updated here.
    See ATLAS code for an example.

    Args:
        job: job object.
    """
    if job:  # to bypass pylint score 0
        pass


def get_metadata(workdir: str):
    """Return the metadata from file.

    Args:
        workdir: work directory.

    Returns:
        metadata (dict or None).
    """
    path = os.path.join(workdir, config.Payload.jobreport)
    metadata = read_file(path) if os.path.exists(path) else None

    return metadata


def update_server(job: object) -> None:
    """Perform any user specific server actions.

    E.g. this can be used to send special information to a logstash.

    Args:
        job: job object.
    """
    if job:  # to bypass pylint score 0
        pass


def post_prestagein_utility_command(**kwargs: Any) -> None:
    """Execute any post pre-stage-in utility commands.

    Args:
        **kwargs: keyword arguments.
    """
    if kwargs:  # to bypass pylint score 0
        pass
    # label = kwargs.get('label', 'unknown_label')
    # stdout = kwargs.get('output', None)


def process_debug_command(debug_command: str, pandaid: int) -> str:
    """Process the debug command.

    In debug mode, the server can send a special debug command to the pilot via the updateJob backchannel.
    This function can be used to process that command, i.e. to identify a proper pid to debug (which is unknown
    to the server).

    Args:
        debug_command: debug command.
        pandaid: PanDA job id.

    Returns:
        str: updated debug command.
    """
    if pandaid:  # to bypass pylint score 0
        pass

    return debug_command


def allow_timefloor(submitmode: str) -> bool:
    """Check if the timefloor mechanism (multi-jobs) is allowed for the given submit mode.

    Args:
        submitmode: submit mode.

    Returns:
        bool: True if multi-jobs are allowed.
    """
    allow = True
    if submitmode.lower() == 'push':
        logger.info('Since the submitmode=push, override timefloor with zero manually')
        allow = False

    return allow


def get_pilot_id(data: dict) -> str:
    """Get the pilot id from the environment variable GTAG.

    Update for each job to get a unique pilot id per job.

    Args:
        data: data dictionary.

    Returns:
        str: Pilot id.
    """
    pilotid = os.environ.get("GTAG", "unknown")
    regex = r'PandaJob\_(\d+)+'
    _id = findall(regex, pilotid)
    if _id:
        jobid = data.get("job_id", "unknown")
        pilotid = pilotid.replace(_id[0], str(jobid))

    return pilotid


def allow_send_workernode_map() -> bool:
    """Return True if the workernode map should be sent to the server.

    Returns:
        bool: False unless requested.
    """
    return False


def allow_send_remaining_time() -> bool:
    """Return True if the remaining time should be sent to the server in the acquire_jobs payload.

    The remaining_time field lets the dispatcher filter out jobs that cannot finish in the time
    the pilot has left. It is currently only supported by the ATLAS server side, so it is not
    sent here.

    Returns:
        bool: False unless the server side adds support for the field.
    """
    return False
