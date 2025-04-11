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

"""Generic user specific functionality."""

import logging
import os
import re

from signal import SIGTERM

from pilot.common.exception import TrfDownloadFailure
from pilot.info.jobdata import JobData
from pilot.util.config import config
from pilot.util.constants import (
    UTILITY_BEFORE_PAYLOAD,
    UTILITY_AFTER_PAYLOAD_STARTED
)
from pilot.util.filehandling import read_file
from pilot.util.https import get_base_urls

from .setup import get_analysis_trf

logger = logging.getLogger(__name__)


def sanity_check() -> int:
    """
    Perform an initial sanity check before doing anything else in a given workflow.

    This function can be used to verify importing of modules that are otherwise used much later, but it is better to abort
    the pilot if a problem is discovered early.

    :return: exit code (0 if all is ok, otherwise non-zero exit code) (int).
    """
    return 0


def validate(job: object) -> bool:
    """
    Perform user specific payload/job validation.

    :param job: job object (Any)
    :return: True if validation is successful (bool).
    """
    if job:  # to bypass pylint score 0
        pass

    return True


def get_payload_command(job: JobData, args: object = None) -> str:
    """
    Return the full command for executing the payload.

    The returned command string includes the sourcing of all setup files and setting of
    environment variables.
    By default, the full payload command is assumed to be in the job.jobparams.

    :param job: job object (object)
    :param args: pilot arguments (object)
    :return: command (str).
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
    """
    Return the proper run command for the user job.

    Example output: export X509_USER_PROXY=<..>;./runAthena <job parameters> --usePFCTurl --directIn

    :param job: job object (object)
    :param trf_name: name of the transform that will run the job (string). Used when containers are not used (str)
    :return: command (str).
    """
    cmd = ""

    # add the user proxy
    if 'X509_USER_PROXY' in os.environ and not job.imagename:
        cmd += f"export X509_USER_PROXY={os.environ.get('X509_USER_PROXY')};"

    # set up trfs
    if job.imagename == "":  # user jobs with no imagename defined
        cmd += f'./{trf_name} {job.jobparams}'
    elif job.imagename:
        # to run a script in a container, assume that the script name is contained in the jobparams
        # but ignore everything else
        match = re.search(r'\b[\w\-.]+\.sh\b', job.jobparams)
        if match:
            shell_script = match.group()
            cmd += f'apptainer exec {job.imagename} {shell_script}'
        else:
            # if no script name is found, assume that the jobparams contain the full command
            cmd += f'apptainer exec {job.imagename} {job.jobparams}'
    elif trf_name:
        cmd += f'./{trf_name} {job.jobparams}'
    else:
        cmd += f'python3 {trf_name} {job.jobparams}'

    return cmd


def update_job_data(job: object):
    """
    Update/add data to the job object.

    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metaData field and added to other job object fields.

    :param job: job object (object).
    """
    if job:  # to bypass pylint score 0
        pass


def remove_redundant_files(workdir: str, outputfiles: list = None, piloterrors: list = None, debugmode: bool = False):
    """
    Remove redundant files and directories prior to creating the log file.

    :param workdir: working directory (str)
    :param outputfiles: list of output files (list)
    :param piloterrors: list of Pilot assigned error codes (list)
    :param debugmode: debug mode (bool).
    """
    if workdir or outputfiles or piloterrors or debugmode:  # to bypass pylint score 0
        pass
    #if outputfiles is None:
    #    outputfiles = []
    #if piloterrors is None:
    #    piloterrors = []


def get_utility_commands(order: int = None, job: object = None, base_urls: list = None) -> dict:
    """
    Return a dictionary of utility commands and arguments to be executed in parallel with the payload.

    This could e.g. be memory and network monitor commands. A separate function can be used to determine the
    corresponding command setups using the utility command name.
    If the optional order parameter is set, the function should return the list of corresponding commands.
    E.g. if order=UTILITY_BEFORE_PAYLOAD, the function should return all commands that are to be executed before the
    payload. If order=UTILITY_WITH_PAYLOAD, the corresponding commands will be prepended to the payload execution
    string. If order=UTILITY_AFTER_PAYLOAD_STARTED, the commands that should be executed after the payload has been started
    should be returned.

    FORMAT: {'command': <command>, 'args': <args>}

    :param order: optional sorting order (see pilot.util.constants) (int)
    :param job: optional job object (object)
    :param base_urls: optional list of base URLs (list)
    :return: dictionary of utilities to be executed in parallel with the payload (dict).
    """
    if order or job or base_urls:  # to bypass pylint score 0
        pass

    return {}


def get_utility_command_setup(name: str, job: object, setup: str = None) -> str:
    """
    Return the proper setup for the given utility command.

    :param name: name of utility command (str)
    :param job: job object (object)
    :param setup: setup string (str)
    :return: full setup string of the utility command (str).
    """
    if name or job or setup:  # to bypass pylint score 0
        pass

    return ""


def get_utility_command_execution_order(name: str) -> int:
    """
    Should the given utility command be executed before or after the payload?

    :param name: utility name (str)
    :return: execution order constant (UTILITY_BEFORE_PAYLOAD or UTILITY_AFTER_PAYLOAD_STARTED) (int).
    """
    # example implementation
    if name == 'monitor':
        return UTILITY_BEFORE_PAYLOAD

    return UTILITY_AFTER_PAYLOAD_STARTED


def post_utility_command_action(name: str, job: object):
    """
    Perform post action for given utility command.

    :param name: name of utility command (str)
    :param job: job object (object).
    """
    if name or job:  # to bypass pylint score 0
        pass


def get_utility_command_kill_signal(name: str) -> int:
    """
    Return the proper kill signal used to stop the utility command.

    :param name: utility command name (str)
    :return: kill signal (int).
    """
    if name:  # to bypass pylint score 0
        pass

    return SIGTERM


def get_utility_command_output_filename(name: str, selector: bool = None) -> str:
    """
    Return the filename to the output of the utility command.

    :param name: utility name (str)
    :param selector: optional special conditions flag (bool)
    :return: filename (str).
    """
    if name or selector:  # to bypass pylint score 0
        pass

    return ""


def verify_job(job: object) -> bool:
    """
    Verify job parameters for specific errors.

    Note:
      in case of problem, the function should set the corresponding pilot error code using
      job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())

    :param job: job object (object)
    :return: True if job is verified (bool).
    """
    if job:  # to bypass pylint score 0
        pass

    return True


def update_stagein(job: object):
    """
    Update the job.indata list with any special files that need to be skipped during stage-in.

    See ATLAS code for an example.

    :param job: job object (object).
    """
    if job:  # to bypass pylint score 0
        pass


def get_metadata(workdir: str) -> str:
    """
    Return the metadata from file.

    :param workdir: work directory (str)
    :return: metadata (str).
    """
    path = os.path.join(workdir, config.Payload.jobreport)
    metadata = read_file(path) if os.path.exists(path) else None

    return metadata


def update_server(job: object):
    """
    Perform any user specific server actions.

    E.g. this can be used to send special information to a logstash.

    :param job: job object (object)
    """
    if job:  # to bypass pylint score 0
        pass


def post_prestagein_utility_command(**kwargs: dict):
    """
    Execute any post pre-stage-in utility commands.

    :param kwargs: kwargs (dict).
    """
    # label = kwargs.get('label', 'unknown_label')
    # stdout = kwargs.get('output', None)
    if kwargs:  # to bypass pylint score 0
        pass


def process_debug_command(debug_command: str, pandaid: str) -> str:
    """
    Process a debug command.

    In debug mode, the server can send a special debug command to the pilot via the updateJob backchannel.

    This function can be used to process that command, i.e. to identify a proper pid to debug (which is unknown
    to the server).

    :param debug_command: debug command (str)
    :param pandaid: PanDA id (str)
    :return: updated debug command (str).
    """
    if pandaid:  # to bypass pylint score 0
        pass

    return debug_command


def allow_timefloor(submitmode: str) -> bool:
    """
    Check if the timefloor mechanism is allowed for the given submit mode.

    :param submitmode: submit mode (str)
    :return: True if timefloor is allowed (bool).
    """
    if submitmode:  # to bypass pylint score 0
        pass

    return True


def get_pilot_id(jobid: str) -> str:
    """
    Get the pilot id from the environment variable GTAG.

    Update if necessary (do not used if you want the same pilot id for all multi-jobs).

    :param jobid: PanDA job id - UNUSED (str)
    :return: pilot id (str).
    """
    if jobid:  # to bypass pylint score 0
        pass

    return os.environ.get("GTAG", "unknown")
