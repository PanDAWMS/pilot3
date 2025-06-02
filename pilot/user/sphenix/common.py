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
from signal import SIGTERM

from pilot.common.exception import (
    TrfDownloadFailure,
    FileHandlingFailure
)
from pilot.info import FileSpec
from pilot.info.jobdata import JobData
from pilot.util.config import config
from pilot.util.constants import (
    UTILITY_AFTER_PAYLOAD_FINISHED,
    UTILITY_AFTER_PAYLOAD_FINISHED2,
    UTILITY_AFTER_PAYLOAD_STARTED,
    UTILITY_AFTER_PAYLOAD_STARTED2,
    UTILITY_BEFORE_PAYLOAD,
    UTILITY_BEFORE_STAGEIN,
    UTILITY_WITH_PAYLOAD,
)
from pilot.util.filehandling import read_file
from pilot.util.https import get_base_urls
from .setup import get_analysis_trf
from .utilities import (
    get_memory_monitor_setup,
    get_memory_monitor_summary_filename,
    post_memory_monitor_action,
)

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

    :param job: job object (object)
    :return: True if validation is successful (bool).
    """
    if job:  # to bypass pylint score 0
        pass

    return True


def get_payload_command(job: JobData, args: object = None) -> str:
    """
    Return the full command for executing the payload.

    This includes the sourcing of all setup files and setting of environment variables.
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
    :param trf_name: name of the transform that will run the job (str)
    :return: command (str).
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


def update_job_data(job: object):
    """
    Update job data with user specific information.

    This function can be used to update/add data to the job object.
    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metaData field and added to other job object fields.

    :param job: job object (object).
    """
    # in case the job was created with --outputs="regex|DST_.*\.root", we can now look for the corresponding
    # output files and add them to the output file list
    outfiles = []
    scope = ''
    dataset = ''
    ddmendpoint = ''
    for fspec in job.outdata:
        if fspec.lfn.startswith('regex|'):  # if this is true, job.outdata will be overwritten
            regex_pattern = fspec.lfn.split('regex|')[1]  # "DST_.*.root"
            logger.info(f'found regular expression {regex_pattern} - looking for the corresponding files in {job.workdir}')

            # extract needed info for the output files for later
            scope = fspec.scope
            dataset = fspec.dataset
            ddmendpoint = fspec.ddmendpoint

            # now locate the corresponding files in the work dir
            outfiles = [_file for _file in os.listdir(job.workdir) if re.search(regex_pattern, _file)]
            logger.debug(f'outfiles={outfiles}')
            if not outfiles:
                logger.warning(f'no output files matching {regex_pattern} were found')
            break

    if outfiles:
        new_outfiles = []
        for outfile in outfiles:
            new_file = {'scope': scope,
                        'lfn': outfile,
                        'workdir': job.workdir,
                        'dataset': dataset,
                        'ddmendpoint': ddmendpoint,
                        'ddmendpoint_alt': None}
            new_outfiles.append(new_file)

        # create list of FileSpecs and overwrite the old job.outdata
        _xfiles = [FileSpec(filetype='output', **_file) for _file in new_outfiles]
        logger.info(f'overwriting old outdata list with new output file info (size={len(_xfiles)})')
        job.outdata = _xfiles
    else:
        logger.debug('no regex found in outdata file list')


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

    # example implementation
    # remove all files except the log file
    # for _file in os.listdir(workdir):
    #     if _file != 'pilotlog.txt':
    #         try:
    #             os.remove(os.path.join(workdir, _file))
    #         except Exception as e:
    #             logger.warning(f'failed to remove {_file}: {e}')


def get_utility_commands(order: int = None, job: JobData = None, base_urls: list = None) -> dict or None:
    """
    Return a dictionary of utility commands and arguments to be executed in parallel with the payload.

    This could e.g. be memory and network monitor commands. A separate function can be used to determine the
    corresponding command setups using the utility command name. If the optional order parameter is set, the
    function should return the list of corresponding commands.

    For example:

    If order=UTILITY_BEFORE_PAYLOAD, the function should return all
    commands that are to be executed before the payload.

    If order=UTILITY_WITH_PAYLOAD, the corresponding commands will be
    prepended to the payload execution string.

    If order=UTILITY_AFTER_PAYLOAD_STARTED, the commands that should be
    executed after the payload has been started should be returned.

    If order=UTILITY_WITH_STAGEIN, the commands that should be executed
    parallel with stage-in will be returned.

    FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>, 'ignore_failure': <Boolean>}

    :param order: optional sorting order (see pilot.util.constants) (int)
    :param job: optional job object (JobData)
    :param base_urls: optional list of base URLs (list)
    :return: dictionary of utilities to be executed in parallel with the payload (dict or None).
    """
    if base_urls:  # to bypass pylint score 0
        pass

    if order == UTILITY_BEFORE_PAYLOAD and job.preprocess:
        return {}

    if order == UTILITY_WITH_PAYLOAD:
        return {}

    if order == UTILITY_AFTER_PAYLOAD_STARTED:
        return get_utility_after_payload_started()

    if order == UTILITY_AFTER_PAYLOAD_STARTED2 and job.coprocess:
        return {}

    if order == UTILITY_AFTER_PAYLOAD_FINISHED:
        return {}

    if order == UTILITY_AFTER_PAYLOAD_FINISHED2 and job.postprocess:
        return {}

    if order == UTILITY_BEFORE_STAGEIN:
        return {}

    return None


def get_utility_after_payload_started() -> dict:
    """
    Return the command dictionary for the utility after the payload has started.

    Command FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    :return: command (dict).
    """
    com = {}
    try:
        cmd = config.Pilot.utility_after_payload_started
    except AttributeError:
        pass
    else:
        if cmd:
            com = {'command': cmd, 'args': '', 'label': cmd.lower(), 'ignore_failure': True}

    return com


def get_utility_command_setup(name: str, job: object, setup: str = None) -> str:
    """
    Return the proper setup for the given utility command.

    If a payload setup is specified, then the utility command string should be prepended to it.

    :param name: name of utility (str)
    :param job: job object (object)
    :param setup: optional payload setup string (str)
    :return: utility command setup (str).
    """
    if setup:  # to bypass pylint score 0
        pass
    if name == 'MemoryMonitor':
        # must know if payload is running in a container or not
        # (enables search for pid in ps output)
        use_container = False  #job.usecontainer or 'runcontainer' in job.transformation

        setup, pid = get_memory_monitor_setup(
            job.pid,
            job.jobid,
            job.workdir,
            setup=job.command,
            use_container=use_container
        )

        _pattern = r"([\S]+)\ ."
        pattern = re.compile(_pattern)
        _name = re.findall(pattern, setup.split(';')[-1])
        if _name:
            job.memorymonitor = _name[0]
        else:
            logger.warning('trf name could not be identified in setup string')

        # update the pgrp if the pid changed
        if pid not in (job.pid, -1):
            logger.debug(f'updating pgrp={job.pgrp} for pid {pid}')
            try:
                job.pgrp = os.getpgid(pid)
            except ProcessLookupError as exc:
                logger.warning(f'os.getpgid({pid}) failed with: {exc}', pid, exc)
        return setup

    return ""


def get_utility_command_execution_order(name: str) -> int:
    """
    Decide the execution order for the given utility command.

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
    if name == 'MemoryMonitor':
        post_memory_monitor_action(job)


def get_utility_command_kill_signal(name: str) -> int:
    """
    Return the proper kill signal used to stop the utility command.

    :param name: utility name (str)
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
    if name == 'MemoryMonitor':
        filename = get_memory_monitor_summary_filename(selector=selector)
    else:
        filename = ""

    return filename


def verify_job(job: object) -> bool:
    """
    Verify job parameters for specific errors.

    Note:
      in case of problem, the function should set the corresponding pilot error code using
      job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())

    :param job: job object (object)
    :return: True if job parameters are verified (bool).
    """
    status = True

    # ..

    # make sure there were no earlier problems
    if status and job.piloterrorcodes:
        logger.warning(f'job has errors: {job.piloterrorcodes}')
        status = False

    return status


def update_stagein(job: object):
    """
    Update the stage-in list if necessary.

    In case special files need to be skipped during stage-in, the job.indata list can be updated here.
    See ATLAS code for an example.

    :param job: job object (object).
    """
    if job:  # to bypass pylint score 0
        pass


def get_metadata(workdir: str) -> str or None:
    """
    Return the metadata from file.

    :param workdir: work directory (str)
    :return: metadata (str or None).
    """
    path = os.path.join(workdir, config.Payload.jobreport)
    try:
        metadata = read_file(path) if os.path.exists(path) else None
    except FileHandlingFailure as exc:
        logger.warning(f'exception caught while opening file: {exc}')
        metadata = None

    return metadata


def update_server(job: object):
    """
    Perform any user specific server actions.

    E.g. this can be used to send special information to a logstash.

    :param job: job object (object).
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
    Decide if the timefloor mechanism should be allowed for the given submit mode.

    :param submitmode: submit mode (str).
    :return: True (bool).
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


def get_rtlogging() -> str:
    """
    Return the proper rtlogging value.

    :return: rtlogging (str).
    """
    return 'logstash;http://splogstash.sdcc.bnl.gov:8080'


def get_rtlogging_ssl() -> (bool, bool):
    """
    Return the proper ssl_enable and ssl_verify for real-time logging.

    :return: ssl_enable (bool), ssl_verify (bool).
    """
    return False, False
