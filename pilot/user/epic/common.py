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
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""User specific functionality for ePIC."""

import fnmatch
import logging
import os
import re

from glob import glob
from signal import SIGTERM
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import TrfDownloadFailure
from pilot.info.jobdata import JobData
from pilot.util.auxiliary import get_resource_name
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
from pilot.util.container import execute
from pilot.util.filehandling import (
    get_guid,
    read_file,
    remove,
    remove_dir_tree,
)
from pilot.util.https import get_base_urls

from .setup import get_analysis_trf
from .utilities import (
    get_memory_monitor_setup,
    post_memory_monitor_action,
    get_memory_monitor_summary_filename,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def sanity_check() -> int:
    """Perform an initial sanity check before doing anything else in a given workflow.

    This function can be used to verify importing of modules that are otherwise used much later, but it is better to abort
    the pilot if a problem is discovered early.

    Returns:
        int: exit code (0 if all is ok, otherwise non-zero exit code).
    """
    return 0


def validate(job: object) -> bool:
    """Perform user specific payload/job validation.

    Args:
        job: job object.

    Returns:
        bool: True if validation is successful.
    """
    status = True

    if job.imagename and job.imagename.startswith('/'):
        if os.path.exists(job.imagename):
            logger.info(f'verified that image exists: {job.imagename}')
        else:
            status = False
            logger.warning(f'image does not exist: {job.imagename}')
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.IMAGENOTFOUND)

    return status


def get_payload_command(job: JobData, args: object = None) -> str:
    """Return the full command for executing the payload.

    The returned command string includes the sourcing of all setup files and setting of
    environment variables.
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

    # Is it a user job or not?
    userjob = job.is_analysis()
    tmp = 'user analysis' if userjob else 'production'
    logger.info(f'pilot is running a {tmp} job')

    ec, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir, base_urls)
    if ec != 0:
        raise TrfDownloadFailure(diagnostics)
    logger.debug(f'user analysis trf: {trf_name}')

    try:
        resource_name = get_resource_name()  # 'grid' if no hpc_resource is set
        resource = __import__(f'pilot.user.epic.resource.{resource_name}', globals(), locals(), [resource_name], 0)

        # get the general setup command
        cmd = resource.get_setup_command(job, False)
    except Exception:
        logger.info("Not using a resource specific setup command")
    else:
        return cmd + get_analysis_run_command(job, trf_name)

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
    elif job.imagename:
        # to run a script in a container, assume that the script name is contained in the jobparams
        # but ignore everything else

        #match = re.search(r'\b[\w\-.]+\.sh\b', job.jobparams)
        #if match:
        #    shell_script = match.group()
        #    cmd += f'apptainer exec {job.imagename} {shell_script}'
        #else:
        #    # if no script name is found, assume that the jobparams contain the full command
        #    cmd += f'apptainer exec {job.imagename} {job.jobparams}'

        cmd += f'./{trf_name} {job.jobparams}'

    elif trf_name:
        cmd += f'./{trf_name} {job.jobparams}'
    else:
        cmd += f'python3 {trf_name} {job.jobparams}'

    logger.info(f'payload run command: {cmd}')

    return cmd


def update_job_data(job: object) -> None:
    """Update/add data to the job object.

    E.g. user specific information can be extracted from other job object fields. In the case of ATLAS, information
    is extracted from the metaData field and added to other job object fields.

    Args:
        job: job object.
    """
    validate_output_data(job)


def validate_output_data(job: JobData) -> None:
    """Validate output data.

    Set any missing GUIDs in the output file list.

    Args:
        job: job object.
    """
    for dat in job.outdata:
        if not dat.guid:
            dat.guid = get_guid()
            logger.warning(f'guid not set: generated guid={dat.guid} for lfn={dat.lfn}')


def get_redundant_path() -> str:
    """Return the path to the file containing the redundant files and directories to be removed prior to log file creation.

    Returns:
        str: file path.
    """
    filename = config.Pilot.redundant

    # correct /cvmfs if necessary
    if filename.startswith('/cvmfs') and os.environ.get('ATLAS_SW_BASE', False):
        filename = filename.replace('/cvmfs', os.environ.get('ATLAS_SW_BASE'))

    return filename


def get_redundants() -> list:
    """Get list of redundant files and directories (to be removed).

    The function will return the content of an external file. It that
    can't be read, then a list defined in this function will be returned instead.
    Any updates to the external file must be propagated to this function.

    Returns:
        list: files and directories.
    """
    # try to read the list from the external file
    filename = get_redundant_path()

    # do not use the cvmfs file since it is not being updated
    # If you uncomment this block, need to also uncomment the read_list import
    # if os.path.exists(filename) and False:
    #    dir_list = read_list(filename)
    #    if dir_list:
    #        return dir_list

    logger.debug(f'list of redundant files could not be read from external file: {filename} (will use internal list)')

    # else return the following
    dir_list = [".asetup.save",
                "AtlasProduction*",
                "AtlasPoint1",
                "AtlasTier0",
                "buildJob*",
                "CDRelease*",
                "ckpt*",
                "csc*.log",
                "DBRelease*",
                "EvgenJobOptions",
                "external",
                "fort.*",
                "geant4",
                "geomDB",
                "geomDB_sqlite",
                "home",
                "LICENSE",
                "madevent",
                "o..pacman..o",
                "pacman-*",
                "python*",
                "requirements.txt",
                "runAthena*",
                "runGen-*",
                "scratch",
                "setup.cfg",
                "share",
                "sources.*",
                "sqlite*",
                "sw",
                "stage*.sh",
                "tcf_*",
                "triggerDB",
                "trusted.caches",
                "workdir",
                "*.data*",
                "*.events",
                "*.py",
                "*.pyc",
                "*.root*",
                "tmp*",
                "*.tmp",
                "*.TMP",
                "*.writing",
                "pwg*",
                "pwhg*",
                "*PROC*",
                "*proxy",
                "*runcontainer*",
                "*job.log.tgz",
                "pandawnutil",
                "src",
                "singularity_cachedir",
                "apptainer_cachedir",
                "_joproxy15",
                "HAHM_*",
                "Process",
                "merged_lhef._0.events-new",
                "panda_secrets.json",
                "singularity",
                "apptainer",
                "work",
                "PILOTVERSION",
                "README*",
                "CLAUDE.md",
                "MANIFEST*",
                "*.part*",
                "__pycache__*",
                "x509*",
                "docs",
                "venv",
                "usr",
                "%1",
                "open_remote_file_cmd.sh"]

    return dir_list


def remove_special_files(workdir: str, dir_list: list) -> None:
    """Remove list of special files from the workdir.

    Args:
        workdir: work directory.
        dir_list: list of special files.
    """
    # note: these should be partial file/dir names, not containing any wildcards
    exceptions_list = ["runargs", "runwrapper", "jobReport", "log.", "xrdlog"]

    to_delete = []
    for _dir in dir_list:
        files = glob(os.path.join(workdir, _dir))
        if not files:
            continue

        exclude = []
        for exc in exceptions_list:
            for item in files:
                if exc in item:
                    exclude.append(os.path.abspath(item))

        _files = [os.path.abspath(item) for item in files if item not in exclude]
        to_delete += _files

    for item in to_delete:
        if os.path.isfile(item):
            remove(item)
        else:
            remove_dir_tree(item)


def cleanup_looping_payload(workdir: str) -> None:
    """Run a special cleanup for looping payloads.

    Remove any root and tmp files.

    Args:
        workdir: working directory.
    """
    for (root, _, files) in os.walk(workdir):
        for filename in files:
            if 'pool.root' in filename:
                path = os.path.join(root, filename)
                path = os.path.abspath(path)
                remove(path)


def cleanup_broken_links(workdir: str) -> None:
    """Run a second pass to clean up any broken links prior to log file creation.

    Args:
        workdir: working directory.
    """
    broken = []
    for root, _, files in os.walk(workdir):
        for filename in files:
            path = os.path.join(root, filename)
            if not os.path.islink(path):
                continue

            target_path = os.readlink(path)
            # Resolve relative symlinks
            if not os.path.isabs(target_path):
                target_path = os.path.join(os.path.dirname(path), target_path)
            if not os.path.exists(target_path):
                broken.append(path)

    for brok in broken:
        remove(brok)


def remove_root_files(workdir: str, outputfiles: list = None) -> None:
    """Recursively remove all .root files from the work directory prior to log file creation.

    Files that are listed as protected output files are skipped.  The walk covers all
    subdirectories so that deeply-nested simulation/reconstruction output (e.g.
    ``./41/FULL/.../run001.edm4hep.root``) is caught regardless of depth.

    Args:
        workdir: work directory to search recursively.
        outputfiles: list of protected output file basenames that must not be removed.
    """
    if outputfiles is None:
        outputfiles = []
    protected = {os.path.basename(f) for f in outputfiles}

    for dirpath, _, filenames in os.walk(workdir):
        for filename in fnmatch.filter(filenames, '*.root*'):
            if filename in protected:
                logger.debug(f'skipping protected output file: {filename}')
                continue
            path = os.path.abspath(os.path.join(dirpath, filename))
            logger.debug(f'removing root file: {path}')
            remove(path)


def remove_redundant_files(workdir: str, outputfiles: list = None, piloterrors: list = None, debugmode: bool = False) -> None:
    """Remove redundant files and directories prior to creating the log file.

    Note: in debug mode, any core files should not be removed before creating the log.

    Args:
        workdir: working directory.
        outputfiles: list of protected output files.
        piloterrors: list of Pilot assigned error codes.
        debugmode: True if debug mode has been switched on.
    """
    if outputfiles is None:
        outputfiles = []
    if piloterrors is None:
        piloterrors = []
    logger.debug("removing redundant files prior to log creation")
    workdir = os.path.abspath(workdir)

    # remove special files
    # get list of redundant files and directories (to be removed)
    dir_list = get_redundants()

    remove_special_files(workdir, dir_list)

    # recursively remove any .root files left in subdirectories (e.g. simulation/reco output
    # nested under run-number subdirs) that the flat glob in remove_special_files misses
    remove_root_files(workdir, outputfiles)

    # verify_container_script(os.path.join(workdir, config.Container.container_script))

    # run a second pass to clean up any broken links
    cleanup_broken_links(workdir)

    # remove any present user workDir
    path = os.path.join(workdir, 'workDir')
    if os.path.exists(path):
        # remove at least root files from workDir (ie also in the case of looping job)
        cleanup_looping_payload(path)
        islooping = errors.LOOPINGJOB in piloterrors
        ismemerror = errors.PAYLOADEXCEEDMAXMEM in piloterrors
        if not islooping and not ismemerror:
            logger.debug(f'removing \'workDir\' from workdir={workdir}')
            remove_dir_tree(path)

    # remove additional dirs
    additionals = ['singularity', 'pilot', 'cores']
    for additional in additionals:
        path = os.path.join(workdir, additional)
        if os.path.exists(path):
            logger.debug(f"removing \'{additional}\' from workdir={workdir}")
            remove_dir_tree(path)

    list_work_dir(workdir)


def list_work_dir(workdir: str) -> None:
    """Execute ls -lF for the given directory and dump to log.

    Args:
        workdir: directory name.
    """
    cmd = f'ls -lF {workdir}'
    _, stdout, stderr = execute(cmd)
    logger.debug(f'{stdout}:\n' + stderr)


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
    if base_urls:  # to bypass pylint score 0
        pass

    if order == UTILITY_BEFORE_PAYLOAD and job.preprocess:
        return get_precopostprocess_command(job.preprocess, job.workdir, 'preprocess', base_urls)

    if order == UTILITY_WITH_PAYLOAD:
        return {}

    if order == UTILITY_AFTER_PAYLOAD_STARTED:
        return get_utility_after_payload_started()

    if order == UTILITY_AFTER_PAYLOAD_STARTED2 and job.coprocess:
        return {}

    if order == UTILITY_AFTER_PAYLOAD_FINISHED:
        return {}

    if order == UTILITY_AFTER_PAYLOAD_FINISHED2 and job.postprocess:
        return get_precopostprocess_command(job.postprocess, job.workdir, 'postprocess', base_urls)

    if order == UTILITY_BEFORE_STAGEIN:
        return {}

    return None


def get_utility_after_payload_started() -> dict:
    """Return the command dictionary for the utility after the payload has started.

    Command FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    Returns:
        dict: command.
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


def get_precopostprocess_command(process: dict, workdir: str, label: str, base_urls: list) -> dict:
    """Return the pre/co/post-process command dictionary.

    Command FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    The returned command has the structure: { 'command': <string>, }

    Args:
        process: pre/co/post-process.
        workdir: working directory.
        label: label.
        base_urls: base URLs for trf download.

    Returns:
        dict: command.
    """
    com = {}
    if process.get('command', ''):
        com = download_command(process, workdir, base_urls)
        com['label'] = label
        com['ignore_failure'] = False

    return com


def get_utility_command_setup(name: str, job: JobData, setup: str = None) -> str:
    """Return the proper setup for the given utility command.

    If a payload setup is specified, then the utility command string should be prepended to it.

    Args:
        name: name of utility.
        job: job object.
        setup: optional payload setup string.

    Returns:
        str: utility command setup.
    """
    if name == 'MemoryMonitor':
        # must know if payload is running in a container or not
        # (enables search for pid in ps output)
        use_container = job.usecontainer or 'runcontainer' in job.transformation

        setup, pid = get_memory_monitor_setup(
            job.pid,
            job.jobid,
            job.workdir,
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
            logger.debug(f'updating pgrp={job.pgrp} for pid={pid}')
            try:
                job.pgrp = os.getpgid(pid)
            except ProcessLookupError as exc:
                logger.warning(f'os.getpgid({pid}) failed with: {exc}')
        return setup

    return ""


def get_utility_command_execution_order(name: str) -> int:
    """Should the given utility command be executed before or after the payload?

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
    if name == 'MemoryMonitor':
        post_memory_monitor_action(job)


def get_utility_command_kill_signal(name: str) -> int:
    """Return the proper kill signal used to stop the utility command.

    Args:
        name: utility command name.

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
    return get_memory_monitor_summary_filename(selector=selector) if name == 'MemoryMonitor' else ""


def verify_job(job: object) -> bool:
    """Verify job parameters for specific errors.

    Note:
      in case of problem, the function should set the corresponding pilot error code using
      job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())

    Args:
        job: job object.

    Returns:
        bool: True if job is verified.
    """
    status = True

    # ..

    # make sure there were no earlier problems
    if status and job.piloterrorcodes:
        logger.warning(f'job has errors: {job.piloterrorcodes}')
        status = False

    return status


def update_stagein(job: object) -> None:
    """Update the job.indata list with any special files that need to be skipped during stage-in.

    See ATLAS code for an example.

    Args:
        job: job object.
    """
    if job:  # to bypass pylint score 0
        pass


def get_metadata(workdir: str) -> str:
    """Return the metadata from file.

    Args:
        workdir: work directory.

    Returns:
        str: metadata.
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
    # label = kwargs.get('label', 'unknown_label')
    # stdout = kwargs.get('output', None)
    if kwargs:  # to bypass pylint score 0
        pass


def process_debug_command(debug_command: str, pandaid: int) -> str:
    """Process a debug command.

    In debug mode, the server can send a special debug command to the pilot via the updateJob backchannel.

    This function can be used to process that command, i.e. to identify a proper pid to debug (which is unknown
    to the server).

    Args:
        debug_command: debug command.
        pandaid: PanDA id.

    Returns:
        str: updated debug command.
    """
    if pandaid:  # to bypass pylint score 0
        pass

    return debug_command


def allow_timefloor(submitmode: str) -> bool:
    """Check if the timefloor mechanism is allowed for the given submit mode.

    Args:
        submitmode: submit mode.

    Returns:
        bool: True if timefloor is allowed.
    """
    if submitmode:  # to bypass pylint score 0
        pass

    return True


def get_pilot_id(data: dict) -> str:
    """Get the pilot id from the environment variable GTAG.

    Update if necessary (not for ATLAS since we want the same pilot id for all multi-jobs).

    Args:
        data: data dictionary.

    Returns:
        str: pilot id.
    """
    base_url = os.environ.get("GTAG", "unknown")
    jobid = data.get("job_id")
    site_name = data.get("site_name", "unknown")

    # If GTAG is not set or not a URL, return as-is
    if base_url == "unknown" or not base_url.startswith("http"):
        return base_url

    # Append PandaID to construct job-specific log directory URL
    try:
        # This points to the directory containing all logs for this specific job
        if "perlmutter" in site_name.lower():
            return f"{base_url}/{jobid}"
        else:
            return base_url
    except Exception:
        # Fall back to base URL if URL construction fails
        return base_url


def download_command(process: dict, workdir: str, base_urls: list) -> dict:
    """Download the pre/postprocess commands if necessary.

    Process FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    Args:
        process: pre/postprocess dictionary.
        workdir: job workdir.
        base_urls: list of base URLs.

    Returns:
        dict: updated pre/postprocess dictionary.
    """
    cmd = process.get('command', '')

    # download the command if necessary
    if cmd.startswith('http'):
        # Try to download the trf (skip when user container is to be used)
        exitcode, _, cmd = get_analysis_trf(cmd, workdir, base_urls)
        if exitcode != 0:
            logger.warning(f'cannot execute command due to previous error: {cmd}')
            return {}

        # update the preprocess command (the URL should be stripped)
        process['command'] = './' + cmd

    return process


def allow_send_workernode_map() -> bool:
    """Return True if the workernode map should be sent to the server.

    Returns:
        bool: False unless requested.
    """
    return True


def allow_send_remaining_time() -> bool:
    """Return True if the remaining time should be sent to the server in the acquire_jobs payload.

    The remaining_time field lets the dispatcher filter out jobs that cannot finish in the time
    the pilot has left. It is currently only supported by the ATLAS server side, so it is not
    sent here.

    Returns:
        bool: False unless the server side adds support for the field.
    """
    return False
