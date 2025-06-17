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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-24
# - Wen Guan, wen.guan@cern.ch, 2018

"""Common functions for ATLAS."""

import fnmatch
import logging
import os
import re
import time

from collections import defaultdict
from functools import reduce
from glob import glob
from random import randint
from signal import SIGTERM, SIGUSR1
from typing import Any

# from tarfile import ExFileObject

from pilot.util.auxiliary import (
    get_resource_name,
    get_key_value,
)
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    TrfDownloadFailure,
    PilotException,
    FileHandlingFailure
)
from pilot.info.filespec import FileSpec
from pilot.info.jobdata import JobData
from pilot.util.config import config
from pilot.util.constants import (
    UTILITY_BEFORE_PAYLOAD,
    UTILITY_WITH_PAYLOAD,
    UTILITY_AFTER_PAYLOAD_STARTED,
    UTILITY_AFTER_PAYLOAD_FINISHED,
    UTILITY_AFTER_PAYLOAD_STARTED2,
    UTILITY_BEFORE_STAGEIN,
    UTILITY_AFTER_PAYLOAD_FINISHED2,
    PILOT_PRE_REMOTEIO,
    PILOT_POST_REMOTEIO
)
from pilot.util.container import execute
from pilot.util.filehandling import (
    copy,
    copy_pilot_source,
    calculate_checksum,
    get_disk_usage,
    get_guid,
    get_local_file_size,
    remove,
    remove_dir_tree,
    remove_core_dumps,
    read_file,
    read_json,
    update_extension,
    write_file,
)
from pilot.util.https import (
    upload_file,
    get_base_urls
)
from pilot.util.processes import (
    convert_ps_to_dict,
    find_pid, find_cmd_pids,
    get_trimmed_dictionary,
    is_child
)
from pilot.util.timing import add_to_pilot_timing
from pilot.util.tracereport import TraceReport
from .container import (
    create_root_container_command,
    execute_remote_file_open
)
from .dbrelease import get_dbrelease_version, create_dbrelease
from .setup import (
    should_pilot_prepare_setup,
    is_standard_atlas_job,
    get_asetup,
    set_inds,
    get_analysis_trf,
    get_payload_environment_variables,
    replace_lfns_with_turls,
)
from .utilities import (
    get_memory_monitor_setup,
    get_network_monitor_setup,
    post_memory_monitor_action,
    get_memory_monitor_summary_filename,
    get_memory_monitor_output_filename,
    get_metadata_dict_from_txt,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def sanity_check() -> int:
    """
    Perform an initial sanity check before doing anything else in a given workflow.

    This function can be used to verify importing of modules that are otherwise used much later, but it is better to
    abort the pilot if a problem is discovered early.

    Note: currently this function does not do anything.

    :return: exit code (0 if all is ok, otherwise non-zero exit code) (int).
    """
    #try:
    #    from rucio.client.downloadclient import DownloadClient
    #    from rucio.client.uploadclient import UploadClient
    #    # note: must do something with Download/UploadClients or flake8
    # will complain - but do not instantiate
    #except Exception as exc:
    #    logger.warning(f'sanity check failed: {exc}')
    #    exit_code = errors.MIDDLEWAREIMPORTFAILURE

    return 0


def validate(job: JobData) -> bool:
    """
    Perform user specific payload/job validation.

    This function will produce a local DBRelease file if necessary (old releases).

    :param job: job object (JobData)
    :return: True if validation is successful, False otherwise (bool).
    """
    status = True

    if 'DBRelease' in job.jobparams:
        logger.debug((
            'encountered DBRelease info in job parameters - '
            'will attempt to create a local DBRelease file'))
        version = get_dbrelease_version(job.jobparams)
        if version:
            status = create_dbrelease(version, job.workdir)

    # assign error in case of DBRelease handling failure
    if not status:
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.DBRELEASEFAILURE)

    # make sure that any given images actually exist
    if status:
        if job.imagename and job.imagename.startswith('/'):
            if os.path.exists(job.imagename):
                logger.info(f'verified that image exists: {job.imagename}')
            else:
                status = False
                logger.warning(f'image does not exist: {job.imagename}')
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.IMAGENOTFOUND)

    # cleanup job parameters if only copy-to-scratch
    #if job.only_copy_to_scratch():
    #    logger.debug(f'job.params={job.jobparams}')
    #    if ' --usePFCTurl' in job.jobparams:
    #        logger.debug('cleaning up --usePFCTurl from job parameters
    #         since all input is copy-to-scratch')
    #        job.jobparams = job.jobparams.replace(' --usePFCTurl', '')
    #    if ' --directIn' in job.jobparams:
    #        logger.debug('cleaning up --directIn from job parameters
    #           since all input is copy-to-scratch')
    #        job.jobparams = job.jobparams.replace(' --directIn', '')

    return status


def open_remote_files(indata: list, workdir: str, nthreads: int) -> tuple[int, str, list, int]:  # noqa: C901
    """
    Verify that direct i/o files can be opened.

    :param indata: list of FileSpec (list)
    :param workdir: working directory (str)
    :param nthreads: number of concurrent file open threads (int)
    :return: exit code (int), diagnostics (str), not opened files (list), lsetup time (int) (tuple).
    :raises PilotException: in case of pilot error.
    """
    exitcode = 0
    diagnostics = ""
    not_opened = []
    lsetup_time = 0

    # extract direct i/o files from indata (string of comma-separated turls)
    turls = extract_turls(indata)
    if turls:
        # execute file open script which will attempt to open each file

        # copy pilot source into container directory, unless it is already there
        diagnostics = copy_pilot_source(workdir)
        if diagnostics:
            raise PilotException(diagnostics)

        os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH') + ':' + workdir

        # first copy all scripts that are needed
        scripts = ['open_remote_file.py', 'open_file.sh']
        final_paths = {}
        for script in scripts:

            final_script_path = os.path.join(workdir, script)
            script_path = os.path.join('pilot/scripts', script)
            dir1 = os.path.join(os.path.join(os.environ['PILOT_HOME'], 'pilot3'), script_path)
            dir2 = os.path.join(workdir, script_path)
            full_script_path = dir1 if os.path.exists(dir1) else dir2
            if not os.path.exists(full_script_path):
                # do not set ec since this will be a pilot issue rather than site issue
                diagnostics = (
                    f'cannot perform file open test - script path does not exist: {full_script_path}'
                )
                logger.warning(diagnostics)
                logger.warning(f'tested both path={dir1} and path={dir2} (none exists)')
                return exitcode, diagnostics, not_opened, lsetup_time

            try:
                copy(full_script_path, final_script_path)
            except PilotException as exc:
                # do not set ec since this will be a pilot issue rather than site issue
                diagnostics = f'cannot perform file open test - pilot source copy failed: {exc}'
                logger.warning(diagnostics)
                return exitcode, diagnostics, not_opened, lsetup_time

            # correct the path when containers have been used
            if "open_remote_file.py" in script:
                final_script_path = os.path.join('.', script)

            final_paths[script] = final_script_path
            logger.debug(f'final path={final_script_path}')

        logger.debug(f'reading file: {final_paths["open_file.sh"]}')
        script_content = read_file(final_paths['open_file.sh'])
        if not script_content:
            diagnostics = (f'cannot perform file open test - failed to read script content from path '
                           f'{final_paths["open_file.sh"]}')
            logger.warning(diagnostics)
            return exitcode, diagnostics, not_opened, lsetup_time

        logger.debug(f'creating file open command from path: {final_paths["open_remote_file.py"]}')
        _cmd = get_file_open_command(final_paths['open_remote_file.py'], turls, nthreads)
        if not _cmd:
            diagnostics = (f'cannot perform file open test - failed to create file open command from path '
                           f'{final_paths["open_remote_file.py"]}')
            logger.warning(diagnostics)
            return exitcode, diagnostics, not_opened, lsetup_time

        timeout = get_timeout_for_remoteio(indata)
        cmd = create_root_container_command(workdir, _cmd, script_content)
        path = os.path.join(workdir, 'open_remote_file_cmd.sh')
        logger.info(f'executing file open verification script (path={path}, timeout={timeout}):\n\n\'{cmd}\'\n\n')
        try:
            write_file(path, cmd)
        except FileHandlingFailure as exc:
            diagnostics = f'failed to write file: {exc}'
            logger.warning(diagnostics)
            return 11, diagnostics, not_opened, lsetup_time

        # if execute_remote_file_open() returns exit code 1, it means general error.
        # exit code 2 means that lsetup timed out, while 3 means that the python script (actual file open) timed out
        try:
            exitcode, stdout, lsetup_time = execute_remote_file_open(path, timeout)
        except PilotException as exc:
            logger.warning(f'caught pilot exception: {exc}')
            exitcode = 11
            stdout = exc
#        exitcode, stdout, stderr = execute(cmd, usecontainer=False, timeout=timeout)
#        if config.Pilot.remotefileverification_log:
#            fpath = os.path.join(workdir, config.Pilot.remotefileverification_log)
#            write_file(fpath, stdout + stderr, mute=False)

        logger.info(f'remote file open finished with ec={exitcode}')
        if lsetup_time > 0:
            logger.info(f"lsetup completed after {lsetup_time} seconds")
        else:
            logger.info("lsetup did not finish correctly")

        # error handling
        if exitcode:
            # first check for apptainer errors
            _exitcode, error_message = errors.resolve_transform_error(exitcode, stdout)
            if _exitcode != exitcode:  # a better error code was found (COMMANDTIMEDOUT error will be passed through)
                if error_message:
                    logger.warning(f"found apptainer error in stderr: {error_message}")
                    logger.warning(f"will overwrite trf exit code {exitcode} due to previous error")
                return _exitcode, stdout, not_opened, lsetup_time

            # note: if the remote files could still be opened the reported error should not be REMOTEFILEOPENTIMEDOUT
            _exitcode, diagnostics, not_opened = parse_remotefileverification_dictionary(workdir)
            if not _exitcode:
                logger.info('remote file could still be opened in spite of previous error')
            elif _exitcode:
                if exitcode == errors.COMMANDTIMEDOUT and _exitcode == errors.REMOTEFILECOULDNOTBEOPENED:
                    exitcode = errors.REMOTEFILEOPENTIMEDOUT
                elif exitcode == errors.COMMANDTIMEDOUT and _exitcode == errors.REMOTEFILEDICTDOESNOTEXIST:
                    exitcode = errors.REMOTEFILEOPENTIMEDOUT
                    diagnostics = f'remote file open command was timed-out and: {diagnostics}'  # cannot give further info
                else:  # REMOTEFILECOULDNOTBEOPENED
                    exitcode = _exitcode
        else:
            exitcode, diagnostics, not_opened = parse_remotefileverification_dictionary(workdir)
    else:
        logger.info('nothing to verify (for remote files)')

    if exitcode:
        logger.warning(f'remote file open exit code: {exitcode}')

    return exitcode, diagnostics, not_opened, lsetup_time


def get_timeout_for_remoteio(indata: list) -> int:
    """
    Calculate a proper timeout to be used for remote i/o files.

    :param indata: list of FileSpec objects (list)
    :return: timeout in seconds (int).
    """
    remote_io = [fspec.status == 'remote_io' for fspec in indata]

    return len(remote_io) * 30 + 900


def parse_remotefileverification_dictionary(workdir: str) -> tuple[int, str, list]:
    """
    Verify that all files could be remotely opened.

    Note: currently ignoring if remote file dictionary doesn't exist.

    :param workdir: work directory needed for opening remote file dictionary (str)
    :return: exit code (int), diagnostics (str), not opened files (list) (tuple).
    """
    exitcode = 0
    diagnostics = ""
    not_opened = []

    dictionary_path = os.path.join(
        workdir,
        config.Pilot.remotefileverification_dictionary
    )

    if not os.path.exists(dictionary_path):
        diagnostics = f'file {dictionary_path} does not exist'
        logger.warning(diagnostics)
        return errors.REMOTEFILEDICTDOESNOTEXIST, diagnostics, not_opened

    file_dictionary = read_json(dictionary_path)
    if not file_dictionary:
        diagnostics = f'could not read dictionary from {dictionary_path}'
        logger.warning(diagnostics)
    else:
        for turl in file_dictionary:
            opened = file_dictionary[turl]
            if not opened:
                logger.info(f'turl could not be opened: {turl}')
                not_opened.append(turl)
            else:
                logger.info(f'turl could be opened: {turl}')

    if not_opened:
        exitcode = errors.REMOTEFILECOULDNOTBEOPENED
        diagnostics = f"Remote file(s) could not be opened: {not_opened}"

    return exitcode, diagnostics, not_opened


def get_file_open_command(script_path: str, turls: str, nthreads: int,
                          stdout: str = 'remote_open.stdout', stderr: str = 'remote_open.stderr') -> str:
    """
    Return the command for opening remote files.

    :param script_path: path to script (str)
    :param turls: comma-separated turls (str)
    :param nthreads: number of concurrent file open threads (int)
    :param stdout: stdout file name (str)
    :param stderr: stderr file name (str)
    :return: comma-separated list of turls (str).
    """
    cmd = f"{script_path} --turls=\'{turls}\' -w {os.path.dirname(script_path)} -t {nthreads}"
    if stdout and stderr:
        cmd += f' 1>{stdout} 2>{stderr}'

    return cmd


def extract_turls(indata: list) -> str:
    """
    Extract TURLs from indata for direct i/o files.

    :param indata: list of FileSpec (list)
    :return: comma-separated list of turls (str).
    """
    # turls = ""
    # for filespc in indata:
    # if filespc.status == 'remote_io':
    # turls += filespc.turl if not turls else f",{filespc.turl}"
    # return turls

    return ",".join(
        fspec.turl for fspec in indata if fspec.status == 'remote_io'
    )


def process_remote_file_traces(path: str, job: JobData, not_opened_turls: list):
    """
    Report traces for remote files.

    The function reads back the base trace report (common part of all traces)
    and updates it per file before reporting it to the Rucio server.

    :param path: path to base trace report (str)
    :param job: job object (JobData)
    :param not_opened_turls: list of turls that could not be opened (list)
    """
    try:
        base_trace_report = read_json(path)
    except PilotException as exc:
        logger.warning(f'failed to open base trace report (cannot send trace reports): {exc}')
    else:
        if not base_trace_report:
            logger.warning('failed to read back base trace report (cannot send trace reports)')
        else:
            # update and send the trace info
            if 'workdir' not in base_trace_report:
                base_trace_report['workdir'] = job.workdir
            for fspec in job.indata:
                if fspec.status == 'remote_io':
                    base_trace_report.update(url=fspec.turl)
                    base_trace_report.update(remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                    base_trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                    base_trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                    if fspec.turl in not_opened_turls:
                        base_trace_report.update(clientState='FAILED_REMOTE_OPEN')
                    else:
                        protocol = get_protocol(fspec.surl, base_trace_report.get('eventType', ''))
                        logger.debug(f'protocol={protocol}')
                        if protocol:
                            base_trace_report.update(protocol=protocol)
                            logger.debug(f'added protocol={protocol} to trace report')
                        base_trace_report.update(clientState='FOUND_ROOT')

                    # copy the base trace report (only a dictionary) into a real trace report object
                    trace_report = TraceReport(**base_trace_report)
                    if trace_report:
                        trace_report.send()
                    else:
                        logger.warning(f'failed to create trace report for turl={fspec.turl}')


def get_protocol(surl: str, event_type: str) -> str:
    """
    Extract the protocol from the surl for event type get_sm_a.

    :param surl: SURL (str)
    :param event_type: event type (str)
    :return: protocol (str).
    """
    protocol = ''
    if event_type != 'get_sm_a':
        return ''
    if surl:
        protocols = re.findall(r'(\w+)://', surl)  # use raw string to avoid flake8 warning W605 invalid escape sequence '\w'
        if protocols:
            protocol = protocols[0]

    return protocol


def get_nthreads(catchall: str) -> int:
    """
    Extract number of concurrent file open threads from catchall.

    Return nthreads=1 if nopenfiles=.. is not present in catchall.

    :param catchall: queuedata catchall (str)
    :return: number of threads (int).
    """
    _nthreads = get_key_value(catchall, key='nopenfiles')
    return _nthreads if _nthreads else 1


def get_payload_command(job: JobData, args: object = None) -> str:
    """
    Return the full command for executing the payload, including the sourcing of all setup files and setting of environment variables.

    :param job: job object (JobData)
    :param args: pilot arguments (object)
    :return: command (str)
    :raises TrfDownloadFailure: in case of download failure.
    """
    # Should the pilot do the setup or does jobPars already contain the information?
    preparesetup = should_pilot_prepare_setup(job.noexecstrcnv, job.jobparams)

    # convert the base URLs for trf downloads to a list (most likely from an empty string)
    base_urls = get_base_urls(args.baseurls)

    # Get the platform value
    # platform = job.infosys.queuedata.platform

    # Is it a user job or not?
    userjob = job.is_analysis()
    tmp = 'user analysis' if userjob else 'production'
    logger.info(f'pilot is running a {tmp} job')

    resource_name = get_resource_name()  # 'grid' if no hpc_resource is set
    resource = __import__(f'pilot.user.atlas.resource.{resource_name}', globals(), locals(), [resource_name], 0)

    # make sure that remote file can be opened before executing payload
    catchall = job.infosys.queuedata.catchall.lower() if job.infosys.queuedata.catchall else ''
    if config.Pilot.remotefileverification_log and 'remoteio_test=false' not in catchall:
        exitcode = 0
        diagnostics = ""

        t0 = int(time.time())
        add_to_pilot_timing(job.jobid, PILOT_PRE_REMOTEIO, t0, args)
        try:
            exitcode, diagnostics, not_opened_turls, lsetup_time = open_remote_files(job.indata, job.workdir, get_nthreads(catchall))
        except Exception as exc:
            logger.warning(f'caught std exception: {exc}')
        else:
            # store the lsetup time for later reporting with job metrics
            if lsetup_time:
                job.lsetuptime = lsetup_time

            # read back the base trace report
            path = os.path.join(job.workdir, config.Pilot.base_trace_report)
            if not os.path.exists(path):
                logger.warning(f'base trace report does not exist ({path}) - '
                               f'input file traces should already have been sent')
            else:
                process_remote_file_traces(path, job, not_opened_turls)  # ignore PyCharm warning, path is str

            t1 = int(time.time())
            add_to_pilot_timing(job.jobid, PILOT_POST_REMOTEIO, t1, args)
            dt = t1 - t0
            logger.info(f'remote file verification finished in {dt} s')

            # fail the job if the remote files could not be verified
            if exitcode != 0:
                # improve the error diagnostics
                diagnostics = errors.format_diagnostics(exitcode, diagnostics)
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exitcode, msg=diagnostics)
                raise PilotException(diagnostics, code=exitcode)
    else:
        logger.debug('no remote file open verification')

    os.environ['INDS'] = 'unknown'  # reset in case set by earlier job

    # get the general setup command
    cmd = resource.get_setup_command(job, preparesetup)
    logger.debug(f'get_setup_command: cmd={cmd}')

    # do not verify the command at this point, since it is best to run in a container (ie do it further down)

    # move this until the final command is ready to prevent double work and complications
    #if cmd:
    #    # containerise command for payload setup verification
    #    _cmd = create_middleware_container_command(job, cmd, label='setup', proxy=False)
    #    exitcode, diagnostics = resource.verify_setup_command(_cmd)
    #    if exitcode != 0:
    #        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exitcode, msg=diagnostics)
    #        raise PilotException(diagnostics, code=exitcode)
    #    else:
    #        logger.info('payload setup verified (in a container)')
    #########################

    if is_standard_atlas_job(job.swrelease):
        # Normal setup (production and user jobs)
        logger.info("preparing normal production/analysis job setup command")
        cmd = get_normal_payload_command(cmd, job, preparesetup, userjob, base_urls)
    else:
        # Generic, non-ATLAS specific jobs, or at least a job with undefined swRelease
        logger.info("generic job (non-ATLAS specific or with undefined swRelease)")
        cmd = get_generic_payload_command(cmd, job, preparesetup, userjob, base_urls)

    # add any missing trailing ;
    if not cmd.endswith(';'):
        cmd += '; '

    site = os.environ.get('PILOT_SITENAME', '')
    variables = get_payload_environment_variables(cmd, job.jobid, job.taskid, job.attemptnr, job.processingtype, site, userjob)
    cmd = ''.join(variables) + cmd

    # prepend payload command with environment variables from PQ.environ if set
    cmd = prepend_env_vars(job.infosys.queuedata.environ, cmd)

    # prepend PanDA job id in case it is not there already (e.g. runcontainer jobs)
    if 'export PandaID' not in cmd:
        cmd = f"export PandaID={job.jobid};" + cmd

    cmd = cmd.replace(';;', ';')

    ## ported from old logic
    if not userjob and not job.is_build_job() and job.has_remoteio():
        ## ported from old logic but still it looks strange (anisyonk)
        ## the "PoolFileCatalog.xml" should already contains proper TURLs
        ## values as it created by create_input_file_metadata() if the case
        ## is just to patch `writetofile` file, than logic should be cleaned
        ## and decoupled anyway, instead of parsing the file, it's much easier
        ## to generate properly `writetofile` content from the beginning
        ##  with TURL data
        lfns = job.get_lfns_and_guids()[0]
        cmd = replace_lfns_with_turls(
            cmd,
            job.workdir,
            "PoolFileCatalog.xml",
            lfns,
            writetofile=job.writetofile
        )

    # Explicitly add the ATHENA_PROC_NUMBER (or JOB value)
    cmd = add_athena_proc_number(cmd)
    if job.dask_scheduler_ip:
        cmd += f'export DASK_SCHEDULER_IP={job.dask_scheduler_ip}; ' + cmd

    logger.info(f'payload run command: {cmd}')

    return cmd


def prepend_env_vars(environ: str, cmd: str) -> str:
    """
    Prepend the payload command with environmental variables from PQ.environ if set.

    :param environ: PQ.environ (str)
    :param cmd: payload command (str)
    :return: updated payload command (str).
    """
    exports = get_exports(environ)
    exports_to_add = ''.join(exports)

    # add the UTC time zone
    exports_to_add += "export TZ=\'UTC\'; "
    cmd = exports_to_add + cmd
    logger.debug(f'prepended exports to payload command: {exports_to_add}')

    return cmd


def get_key_values(from_string: str) -> list:
    """
    Return a list of key value tuples from given string.

    Example: from_string = 'KEY1=VALUE1 KEY2=VALUE2' -> [('KEY1','VALUEE1'), ('KEY2', 'VALUE2')]

    :param from_string: string containing key-value pairs (str)
    :return: list of key-pair tuples (list).
    """
    return re.findall(re.compile(r"\b(\w+)=(.*?)(?=\s\w+=\s*|$)"), from_string)


def get_exports(from_string: str) -> list:
    """
    Return list of exports from given string.

    :param from_string: string containing key-value pairs (str)
    :return: list of export commands (list).
    """
    exports = []
    key_values = get_key_values(from_string)
    logger.debug(f'extracted key-values: {key_values}')
    if key_values:
        for _, raw_val in enumerate(key_values):
            _key = raw_val[0]
            _value = raw_val[1]
            key_value = ''
            if not _key.startswith('export '):
                key_value = 'export ' + _key + '=' + _value
            if not _value.endswith(';'):
                key_value += ';'
            exports.append(key_value)

    return exports


def get_normal_payload_command(cmd: str, job: JobData, preparesetup: bool, userjob: bool, base_urls: list) -> str:
    """
    Return the payload command for a normal production/analysis job.

    :param cmd: any preliminary command setup (str)
    :param job: job object (JobData)
    :param preparesetup: True if the pilot should prepare the setup, False if already in the job parameters (bool)
    :param userjob: True for user analysis jobs, False otherwise (bool)
    :param base_urls: list of base URLs for trf downloads (list)
    :return: normal payload command (str).
    """
    # set the INDS env variable
    # (used by runAthena but also for EventIndex production jobs)
    set_inds(job.datasetin)  # realDatasetsIn

    if userjob:
        # Try to download the trf (skip when user container is to be used)
        exitcode, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir, base_urls)
        if exitcode != 0:
            raise TrfDownloadFailure(diagnostics)

        logger.debug(f'user analysis trf: {trf_name}')

        if preparesetup:
            _cmd = get_analysis_run_command(job, trf_name)
        else:
            _cmd = job.jobparams

        # Correct for multi-core if necessary (especially important in
        # case coreCount=1 to limit parallel make)
        cmd += "; " + add_makeflags(job.corecount, "") + _cmd
    else:
        # Add Database commands if they are set by the local site
        cmd += os.environ.get('PILOT_DB_LOCAL_SETUP_CMD', '')

        if job.is_eventservice:
            if job.corecount:
                cmd += f'; export ATHENA_PROC_NUMBER={job.corecount}'
                cmd += f'; export ATHENA_CORE_NUMBER={job.corecount}'
            else:
                cmd += '; export ATHENA_PROC_NUMBER=1'
                cmd += '; export ATHENA_CORE_NUMBER=1'

        # Add the transform and the job parameters (production jobs)
        if preparesetup:
            cmd += f"; {job.transformation} {job.jobparams}"
        else:
            cmd += "; " + job.jobparams

    return cmd


def get_generic_payload_command(cmd: str, job: JobData, preparesetup: bool, userjob: bool, base_urls: list) -> str:
    """
    Return the payload command for a generic job.

    :param cmd: any preliminary command setup (str)
    :param job: job object (JobData)
    :param preparesetup: True if the pilot should prepare the setup, False if already in the job parameters (bool)
    :param userjob: True for user analysis jobs, False otherwise (bool)
    :param base_urls: list of base URLs for trf downloads (list)
    :return: generic job command (str).
    """
    if userjob:
        # Try to download the trf
        #if job.imagename != "" or "--containerImage" in job.jobparams:
        #    job.transformation = os.path.join(os.path.dirname(job.transformation), "runcontainer")
        #    logger.warning(f'overwrote job.transformation, now set to: {job.transformation}')
        exitcode, diagnostics, trf_name = get_analysis_trf(job.transformation, job.workdir, base_urls)
        if exitcode != 0:
            raise TrfDownloadFailure(diagnostics)

        logger.debug(f'user analysis trf: {trf_name}')

        if preparesetup:
            _cmd = get_analysis_run_command(job, trf_name)
        else:
            _cmd = job.jobparams

        # correct for multi-core if necessary (especially important in case
        # coreCount=1 to limit parallel make), only if not using a user container
        if not job.imagename:
            cmd += "; " + add_makeflags(job.corecount, "") + _cmd
        else:
            cmd += _cmd

    elif verify_release_string(job.homepackage) != 'NULL' and job.homepackage != ' ':
        if preparesetup:
            cmd = f"python {job.homepackage}/{job.transformation} {job.jobparams}"
        else:
            cmd = job.jobparams
    elif preparesetup:
        cmd = f"python {job.transformation} {job.jobparams}"
    else:
        cmd = job.jobparams

    return cmd


def add_athena_proc_number(cmd: str) -> str:
    """
    Add the ATHENA_PROC_NUMBER and ATHENA_CORE_NUMBER to the payload command if necessary.

    :param cmd: payload execution command (str)
    :return: updated payload execution command (str).
    """
    # get the values if they exist
    try:
        value1 = int(os.environ['ATHENA_PROC_NUMBER_JOB'])
    except (TypeError, KeyError, ValueError) as exc:
        logger.warning(f'failed to convert ATHENA_PROC_NUMBER_JOB to int: {exc}')
        value1 = None
    try:
        value2 = int(os.environ['ATHENA_CORE_NUMBER'])
    except (TypeError, KeyError, ValueError) as exc:
        logger.warning(f'failed to convert ATHENA_CORE_NUMBER to int:{exc}')
        value2 = None

    if "ATHENA_PROC_NUMBER" not in cmd:
        if "ATHENA_PROC_NUMBER" in os.environ:
            cmd = f"export ATHENA_PROC_NUMBER={os.environ['ATHENA_PROC_NUMBER']};" + cmd
        elif "ATHENA_PROC_NUMBER_JOB" in os.environ and value1:
            if value1 > 1:
                cmd = f'export ATHENA_PROC_NUMBER={value1};' + cmd
            else:
                logger.info(f"will not add ATHENA_PROC_NUMBER to cmd since the value is {value1}")
        else:
            logger.warning((
                "don't know how to set ATHENA_PROC_NUMBER "
                "(could not find it in os.environ)"))
    else:
        logger.info("ATHENA_PROC_NUMBER already in job command")

    if 'ATHENA_CORE_NUMBER' in os.environ and value2:
        if value2 > 1:
            cmd = f'export ATHENA_CORE_NUMBER={value2};' + cmd
        else:
            logger.info(f"will not add ATHENA_CORE_NUMBER to cmd since the value is {value2}")
    else:
        logger.warning((
            'there is no ATHENA_CORE_NUMBER in os.environ '
            '(cannot add it to payload command)'))

    return cmd


def verify_release_string(release: str or None) -> str:
    """
    Verify that the release (or homepackage) string is set.

    :param release: release or homepackage string that might or might not be set (str or None)
    :return: release (str).
    """
    if release is None:
        release = ""
    release = release.upper()
    if release == "":
        release = "NULL"
    if release == "NULL":
        logger.info("detected unset (NULL) release/homepackage string")

    return release


def add_makeflags(job_core_count: int, cmd: str) -> str:
    """
    Correct for multicore if necessary (especially important in case coreCount=1 to limit parallel make).

    :param job_core_count: core count from the job definition (int)
    :param cmd: payload execution command (str)
    :return: updated payload execution command (str).
    """
    # ATHENA_PROC_NUMBER is set in Node.py using the schedconfig value
    try:
        core_count = int(os.environ.get('ATHENA_PROC_NUMBER'))
    except (TypeError, KeyError, ValueError):
        core_count = -1

    if core_count == -1:
        try:
            core_count = int(job_core_count)
        except (TypeError, ValueError):
            pass
        else:
            if core_count >= 1:
                # Note: the original request (AF) was to use j%d
                # and not -j%d, now using the latter
                cmd += f"export MAKEFLAGS=\'-j{core_count} QUICK=1 -l1\';"

    # make sure that MAKEFLAGS is always set
    if "MAKEFLAGS=" not in cmd:
        cmd += "export MAKEFLAGS=\'-j1 QUICK=1 -l1\';"

    return cmd


def get_analysis_run_command(job: JobData, trf_name: str) -> str:  # noqa: C901
    """
    Return the proper run command for the user job.

    Example output:
    export X509_USER_PROXY=<..>;./runAthena <job parameters> --usePFCTurl --directIn

    :param job: job object (JobData)
    :param trf_name: name of the transform that will run the job (str)
    :return: command (str).
    """
    cmd = ""

    # add the user proxy
    if 'X509_USER_PROXY' in os.environ and not job.imagename:
        x509 = os.environ.get('X509_UNIFIED_DISPATCH', os.environ.get('X509_USER_PROXY', ''))
        cmd += f'export X509_USER_PROXY={x509};'

    env_vars_to_unset = [
        'OIDC_AUTH_TOKEN',
        'OIDC_AUTH_ORIGIN',
        'PANDA_AUTH_TOKEN',
        'PANDA_AUTH_ORIGIN',
        'OIDC_REFRESHED_AUTH_TOKEN'
    ]

    for var in env_vars_to_unset:
        if var in os.environ:
            cmd += f'unset {var};'

    # set up trfs
    if job.imagename == "":  # user jobs with no imagename defined
        cmd += f'./{trf_name} {job.jobparams}'
    else:
        if job.is_analysis() and job.imagename:
            cmd += f'./{trf_name} {job.jobparams}'
        else:
            cmd += f'python {trf_name} {job.jobparams}'

        imagename = job.imagename
        # check if image is on disk as defined by envar PAYLOAD_CONTAINER_LOCATION
        payload_container_location = os.environ.get('PAYLOAD_CONTAINER_LOCATION')
        if payload_container_location is not None:
            logger.debug(f"$PAYLOAD_CONTAINER_LOCATION = {payload_container_location}")
            # get container name
            containername = imagename.rsplit('/')[-1]
            image_location = os.path.join(payload_container_location, containername)
            if os.path.exists(image_location):
                logger.debug(f"image exists at {image_location}")
                imagename = image_location

        # restore the image name if necessary
        if 'containerImage' not in cmd and 'runcontainer' in trf_name:
            cmd += f' --containerImage={imagename}'

    # add control options for PFC turl and direct access
    #if job.indata:   ## DEPRECATE ME (anisyonk)
    #    if use_pfc_turl and '--usePFCTurl' not in cmd:
    #        cmd += ' --usePFCTurl'
    #    if use_direct_access and '--directIn' not in cmd:
    #        cmd += ' --directIn'

    if job.has_remoteio():
        logger.debug((
            'direct access (remoteio) is used to access some input files: '
            '--usePFCTurl and --directIn will be added to payload command'))
        if '--usePFCTurl' not in cmd:
            cmd += ' --usePFCTurl'
        if '--directIn' not in cmd:
            cmd += ' --directIn'

    # update the payload command for forced accessmode
    ## -- REDUNDANT logic, since it should be done from the beginning at
    ## the step of FileSpec initialization (anisyonk)
    #cmd = update_forced_accessmode(log, cmd, job.transfertype,
    # job.jobparams, trf_name)  ## DEPRECATE ME (anisyonk)

    # add guids when needed
    # get the correct guids list (with only the direct access files)
    if not job.is_build_job():
        lfns, guids = job.get_lfns_and_guids()
        _guids = get_guids_from_jobparams(job.jobparams, lfns, guids)
        if _guids:
            cmd += f' --inputGUIDs "{str(_guids)}"'

    return cmd


def get_guids_from_jobparams(jobparams: str, infiles: list, infilesguids: list) -> list:
    """
    Extract the correct guid from the input file list.

    The guids list is used for direct reading.
    1. extract input file list for direct reading from job parameters
    2. for each input file in this list, find the corresponding guid from
    the input file guid list.
    Since the job parameters string is entered by a human, the order of
    the input files might not be the same.

    :param jobparams: job parameters (str)
    :param infiles: input file list (list)
    :param infilesguids: input file guids list (list)
    :return: guids (list).
    """
    guidlist = []
    jobparams = jobparams.replace("'", "")
    jobparams = jobparams.replace(", ", ",")

    pattern = re.compile(r'\-i \"\[([A-Za-z0-9.,_-]+)\]\"')
    directreadinginputfiles = re.findall(pattern, jobparams)
    _infiles = []
    if directreadinginputfiles != []:
        _infiles = directreadinginputfiles[0].split(",")
    else:
        match = re.search(r"-i ([A-Za-z0-9.\[\],_-]+) ", jobparams)
        if match is not None:
            compactinfiles = match.group(1)
            match = re.search(r'(.*)\[(.+)\](.*)\[(.+)\]', compactinfiles)
            if match is not None:
                infiles = []
                head = match.group(1)
                tail = match.group(3)
                body = match.group(2).split(',')
                attr = match.group(4).split(',')

                for idx, item in enumerate(body):
                    lfn = f'{head}{item}{tail}{attr[idx]}'
                    infiles.append(lfn)
            else:
                infiles = [compactinfiles]

    for infile in _infiles:
        # get the corresponding index from the inputFiles list,
        # which has the same order as infilesguids
        try:
            index = infiles.index(infile)
        except ValueError as exc:
            logger.warning(f"exception caught: {exc} (direct reading will fail)")
        else:
            # add the corresponding guid to the list
            guidlist.append(infilesguids[index])

    return guidlist


def test_job_data(job: JobData):
    """
    Test function to verify that the job object contains the expected data.

    :param job: job object (JobData).
    """
    # in case the job was created with --outputs="regex|DST_.*\.root", we can now look for the corresponding
    # output files and add them to the output file list
    # add a couple of files to replace current output
    filesizeinbytes = 1024
    outputfiles = ['DST_.random1.root', 'DST_.random2.root', 'DST_.random3.root']
    for outputfile in outputfiles:
        with open(os.path.join(job.workdir, outputfile), 'wb') as fout:
            fout.write(os.urandom(filesizeinbytes))  # replace 1024 with a size in kilobytes if it is not unreasonably large

    outfiles = []
    scope = ''
    dataset = ''
    ddmendpoint = ''
    for fspec in job.outdata:

        fspec.lfn = 'regex|DST_.*.root'
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
                        'guid': get_guid(),
                        'workdir': job.workdir,
                        'dataset': dataset,
                        'ddmendpoint': ddmendpoint,
                        'ddmendpoint_alt': None}
            new_outfiles.append(new_file)
        logger.debug(f'new_outfiles={new_outfiles}')
        # create list of FileSpecs and overwrite the old job.outdata
        _xfiles = [FileSpec(filetype='output', **_file) for _file in new_outfiles]
        logger.info(f'overwriting old outdata list with new output file info (size={len(_xfiles)})')
        job.outdata = _xfiles
    else:
        logger.debug('no regex found in outdata file list')


def update_job_data(job: JobData):
    """
    Update the job object.

    This function can be used to update/add data to the job object.
    E.g. user specific information can be extracted from other job object fields.
    In the case of ATLAS, information is extracted from the metadata field and
    added to other job object fields.

    :param job: job object (JobData).
    """
    ## comment from Alexey:
    ## it would be better to reallocate this logic (as well as parse
    ## metadata values)directly to Job object since in general it's Job
    ## related part. Later on once we introduce VO specific Job class
    ## (inherited from JobData) this can be easily customized

    # test_job_data(job)

    # get label "all" or "log"
    stageout = get_stageout_label(job)

    if 'exeErrorDiag' in job.metadata:
        job.exeerrordiag = job.metadata['exeErrorDiag']
        if job.exeerrordiag:
            logger.warning(f'payload failed: exeErrorDiag={job.exeerrordiag}')

    # determine what should be staged out
    job.stageout = stageout  # output and log file or only log file

    try:
        work_attributes = parse_jobreport_data(job.metadata)
    except Exception as exc:
        logger.warning(f'failed to parse job report (cannot set job.nevents): {exc}')
    else:
        # note: the number of events can be set already at this point
        # if the value was extracted from the job report (a more thorough
        # search for this value is done later unless it was set here)
        nevents = work_attributes.get('nEvents', 0)
        if nevents:
            job.nevents = nevents

    # extract output files from the job report if required, in case the trf
    # has created additional (overflow) files. Also make sure all guids are
    # assigned (use job report value if present, otherwise generate the guid)
    is_raythena = os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') == 'raythena'
    if not is_raythena:
        if job.metadata and not job.is_eventservice:
            # keep this for now, complicated to merge with verify_output_files?
            extract_output_file_guids(job)
            try:
                verify_output_files(job)
            except Exception as exc:
                logger.warning(f'exception caught while trying verify output files: {exc}')
        elif not job.allownooutput:  # i.e. if it's an empty list/string, do nothing
            logger.debug((
                "will not try to extract output files from jobReport "
                "for user job (and allowNoOut list is empty)"))
        else:
            # remove the files listed in allowNoOutput if they don't exist
            remove_no_output_files(job)

        validate_output_data(job)


def validate_output_data(job: JobData):
    """
    Validate output data.

    Set any missing GUIDs and make sure the output file names follow the ATLAS naming convention - if not, set the
    error code.

    :param job: job object (JobData).
    """
    ## validate output data (to be moved into the JobData)
    ## warning: do no execute this code unless guid lookup in job report
    # has failed - pilot should only generate guids
    ## if they are not present in job report

    pattern = re.compile(naming_convention_pattern())
    bad_files = []
    for dat in job.outdata:
        if not dat.guid:
            dat.guid = get_guid()
            logger.warning(f'guid not set: generated guid={dat.guid} for lfn={dat.lfn}')
        # is the output file following the naming convention?
        found = re.findall(pattern, dat.lfn)
        if found:
            logger.info(f'verified that {dat.lfn} follows the naming convention')
        else:
            bad_files.append(dat.lfn)

    # make sure there are no illegal characters in the file names
    for bad_file_name in bad_files:
        diagnostic = f'{bad_file_name} does not follow the naming convention: {naming_convention_pattern()}'
        try:
            bad_file_name.encode('ascii')
        except UnicodeEncodeError as exc:
            diagnostic += f' and contains illegal characters: {exc}'
            # only fail the job in this case (otherwise test jobs would fail!), and only report on the first file
            if errors.BADOUTPUTFILENAME not in job.piloterrorcodes:
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.BADOUTPUTFILENAME, msg=diagnostic)
        logger.warning(diagnostic)

    if not bad_files:
        logger.debug('verified that all output files follow the ATLAS naming convention')


def naming_convention_pattern() -> str:
    """
    Return a regular expression pattern in case the output file name should be verified.

    Pattern as below in the return statement will match the following file names:
    re.findall(pattern, 'AOD.29466419._001462.pool.root.1')
    ['AOD.29466419._001462.pool.root.1']

    :return: raw string (str).
    """
    max_filename_size = 250

    # pydocstyle does not like the backslash in the following line, but it is needed
    return fr"^[A-Za-z0-9][A-Za-z0-9.\-_]{{1,{max_filename_size}}}$"


def get_stageout_label(job: JobData):
    """
    Get a proper stage-out label.

    :param job: job object (JobData)
    :return: "all"/"log" depending on stage-out type (str).
    """
    stageout = "all"

    if job.is_eventservice:
        logger.info('event service payload, will only stage-out log')
        stageout = "log"
    elif 'exeErrorCode' in job.metadata:
        # handle any error codes
        job.exeerrorcode = job.metadata['exeErrorCode']
        if job.exeerrorcode == 0:
            stageout = "all"
        else:
            logger.info(f'payload failed: exeErrorCode={job.exeerrorcode}')
            stageout = "log"

    return stageout


def update_output_for_hpo(job: JobData):
    """
    Update the output (outdata) for HPO jobs.

    :param job: job object (JobData).
    """
    try:
        new_outdata = discover_new_outdata(job)
    except Exception as exc:
        logger.warning(f'exception caught while discovering new outdata: {exc}')
    else:
        if new_outdata:
            logger.info(f'replacing job outdata with discovered output ({len(new_outdata)} file(s))')
            job.outdata = new_outdata


def discover_new_outdata(job: JobData) -> list:
    """
    Discover new outdata created by HPO job.

    :param job: job object (JobData)
    :return: new_outdata (list of FileSpec objects) (list).
    """
    new_outdata = []

    for outdata_file in job.outdata:
        new_output = discover_new_output(outdata_file.lfn, job.workdir)
        if new_output:
            # create new FileSpec objects out of the new output
            for outfile, file_info in new_output.items():
                # note: guid will be taken from job report
                # after this function has been called
                files = [{
                    'scope': outdata_file.scope,
                    'lfn': outfile,
                    'workdir': job.workdir,
                    'dataset': outdata_file.dataset,
                    'ddmendpoint': outdata_file.ddmendpoint,
                    'ddmendpoint_alt': None,
                    'filesize': file_info['filesize'],
                    'checksum': file_info['checksum'],
                    'guid': ''
                }]

                # do not abbreviate the following two lines as otherwise
                # the content of xfiles will be a list of generator objects
                _xfiles = [FileSpec(filetype='output', **f) for f in files]
                new_outdata += _xfiles

    return new_outdata


def discover_new_output(name_pattern: str, workdir: str) -> dict:
    """
    Discover new output created by HPO job in the given work directory.

    name_pattern for known 'filename' is 'filename_N' (N = 0, 1, 2, ..).
    Example: name_pattern = 23578835.metrics.000001.tgz
             should discover files with names 23578835.metrics.000001.tgz_N (N = 0, 1, ..)

    new_output = { lfn: {'path': path, 'size': size, 'checksum': checksum}, .. }

    :param name_pattern: assumed name pattern for file to discover (str)
    :param workdir: work directory (str)
    :return: new_output (dict).
    """
    new_output = {}
    outputs = glob(f"{workdir}/{name_pattern}_*")
    if outputs:
        lfns = [os.path.basename(path) for path in outputs]
        for lfn, path in list(zip(lfns, outputs)):
            # get file size
            filesize = get_local_file_size(path)
            # get checksum
            try:
                checksum = calculate_checksum(path, algorithm=config.File.checksum_type)
            except (FileHandlingFailure, NotImplementedError) as exc:
                logger.warning(f'failed to create file info (filesize={filesize}) for lfn={lfn}: {exc}')
            else:
                if filesize and checksum:
                    new_output[lfn] = {'path': path, 'filesize': filesize, 'checksum': checksum}
                else:
                    logger.warning(f'failed to create file info (filesize={filesize}, checksum={checksum}) for lfn={lfn}')

    return new_output


def extract_output_file_guids(job: JobData):
    """
    Extract output file info from the job report and make sure all guids are assigned.

    Use job report value if present, otherwise generate the guid.
    Note: guid generation is done later, not in this function since
    this function might not be called if metadata info is not found prior
    to the call.

    :param job: job object (JobData).
    """
    # make sure there is a defined output file list in the job report -
    # unless it is allowed by task parameter allowNoOutput
    if not job.allownooutput:
        output = job.metadata.get('files', {}).get('output', [])
        if output:
            logger.info(f'verified that job report contains metadata for {len(output)} file(s)')
        else:
            #- will fail job since allowNoOutput is not set')
            logger.warning('job report contains no output files and allowNoOutput is not set')
            #job.piloterrorcodes, job.piloterrordiags =
            # errors.add_error_code(errors.NOOUTPUTINJOBREPORT)
            return

    # extract info from metadata (job report JSON)
    data = dict([out.lfn, out] for out in job.outdata)
    #extra = []
    for dat in job.metadata.get('files', {}).get('output', []):
        for fdat in dat.get('subFiles', []):
            lfn = fdat['name']

            # verify the guid if the lfn is known
            # only extra guid if the file is known by the
            # job definition (March 18 change, v 2.5.2)
            if lfn in data:
                data[lfn].guid = fdat['file_guid']
                logger.info(f'set guid={data[lfn].guid} for lfn={lfn} (value taken from job report)')
            else:  # found new entry
                logger.warning(f'pilot no longer considers output files not mentioned in job definition (lfn={lfn})')
                continue

                #if job.outdata:
                #    kw = {'lfn': lfn,
                # .         # take value from 1st output file?
                #          'scope': job.outdata[0].scope,
                #          'guid': fdat['file_guid'],
                #          'filesize': fdat['file_size'],
                #           # take value from 1st output file?
                #          'dataset': dat.get('dataset') or job.outdata[0].dataset
                #          }
                #    spec = FileSpec(filetype='output', **kw)
                #    extra.append(spec)

    # make sure the output list has set guids from job report
    for fspec in job.outdata:
        if fspec.guid != data[fspec.lfn].guid:
            fspec.guid = data[fspec.lfn].guid
            logger.debug(f'reset guid={fspec.guid} for lfn={fspec.lfn}')
        elif fspec.guid:
            logger.debug(f'verified guid={fspec.guid} for lfn={fspec.lfn}')
        else:
            logger.warning(f'guid not set for lfn={fspec.lfn}')
    #if extra:
        #logger.info('found extra output files in job report,
        # will overwrite output file list: extra=%s' % extra)
        #job.outdata = extra


def verify_output_files(job: JobData) -> bool:
    """
    Verify that the output files from the job definition are listed in the job report.

    Also make sure that the number of processed events is greater than zero.

    If the output file is not listed in the job report, then if the file is
    listed in allowNoOutput remove it from stage-out, otherwise fail the job.

    Note from Rod: fail scenario: The output file is not in output:[] or is
    there with zero events. Then if allownooutput is not set - fail the job.
    If it is set, then do not store the output, and finish ok.

    :param job: job object (JobData)
    :return: True if output files were validated correctly, False otherwise (bool).
    """
    failed = False

    # get list of output files from the job definition
    lfns_jobdef = []
    for fspec in job.outdata:
        lfns_jobdef.append(fspec.lfn)
    if not lfns_jobdef:
        logger.debug('empty output file list from job definition (nothing to verify)')
        return True

    # get list of output files from job report
    # (if None is returned, it means the job report is from an old release
    # and does not contain an output list)
    output = job.metadata.get('files', {}).get('output', None)
    if not output and output is not None:
        # ie empty list, output=[] - are all known output files in allowNoOutput?
        logger.warning('encountered an empty output file list in job report, consulting allowNoOutput list')
        failed = False
        for lfn in lfns_jobdef:
            if lfn not in job.allownooutput:
                if job.is_analysis():
                    logger.warning(f'lfn {lfn} is not in allowNoOutput list - ignore for user job')
                else:
                    failed = True
                    logger.warning(f'lfn {lfn} is not in allowNoOutput list - job will fail')
                    job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGOUTPUTFILE)
                    break
            else:
                logger.info(f'lfn {lfn} listed in allowNoOutput - will be removed from stage-out')
                remove_from_stageout(lfn, job)

    elif output is None:
        # ie job report is ancient / output could not be extracted
        logger.warning('output file list could not be extracted from job report (nothing to verify)')
    else:
        verified, nevents = verify_extracted_output_files(output, lfns_jobdef, job)
        failed = (not verified)
        if nevents > 0 and not failed and job.nevents == 0:
            job.nevents = nevents
            logger.info(f'number of events from summed up output files: {nevents}')
        else:
            logger.info(f'number of events previously set to {job.nevents}')

    status = (not failed)

    if status:
        logger.info('output file verification succeeded')
    else:
        logger.warning('output file verification failed')

    return status


def verify_extracted_output_files(output: list, lfns_jobdef: list, job: JobData) -> tuple[bool, int]:
    """
    Make sure all output files extracted from the job report are listed.

    Grab the number of events if possible.

    :param output: list of FileSpecs (list)
    :param lfns_jobdef: list of lfns strings from job definition (list)
    :param job: job object (JobData)
    :return: True if successful, False if failed (bool), number of events (int) (tuple).
    """
    failed = False
    nevents = 0
    output_jobrep = {}  # {lfn: nentries, ..}
    logger.debug((
        'extracted output file list from job report - '
        'make sure all known output files are listed'))

    # first collect the output files from the job report
    for dat in output:
        for fdat in dat.get('subFiles', []):
            # get the lfn
            name = fdat.get('name', None)

            # get the number of processed events and add the output file info to the dictionary
            output_jobrep[name] = fdat.get('nentries', None)

    # now make sure that the known output files are in the job report dictionary
    for lfn in lfns_jobdef:
        if lfn not in output_jobrep and lfn not in job.allownooutput:
            if job.is_analysis():
                logger.warning(f'output file {lfn} from job definition is not present in job report and '
                               f'is not listed in allowNoOutput')
            else:
                logger.warning(f'output file {lfn} from job definition is not present in job report and '
                               f'is not listed in allowNoOutput - job will fail')
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.MISSINGOUTPUTFILE)
                failed = True
                break
        if lfn not in output_jobrep and lfn in job.allownooutput:
            logger.warning(f'output file {lfn} from job definition is not present in job report but '
                           f'is listed in allowNoOutput - remove from stage-out')
            remove_from_stageout(lfn, job)
        else:
            nentries = output_jobrep[lfn]
            if nentries == "UNDEFINED":
                logger.warning(f'encountered file with nentries=UNDEFINED - will ignore {lfn}')

            elif nentries is None:
                if lfn not in job.allownooutput:
                    logger.warning(f'output file {lfn} is listed in job report, but has no events and '
                                   f'is not listed in allowNoOutput - will ignore')
                else:
                    logger.warning(f'output file {lfn} is listed in job report, nentries is None and is listed in '
                                   f'allowNoOutput - remove from stage-out')
                    remove_from_stageout(lfn, job)

            elif nentries == 0:
                if lfn not in job.allownooutput:
                    logger.warning(f'output file {lfn} is listed in job report, has zero events and '
                                   f'is not listed in allowNoOutput - will ignore')
                else:
                    logger.warning(f'output file {lfn} is listed in job report, has zero events and is listed in '
                                   f'allowNoOutput - remove from stage-out')
                    remove_from_stageout(lfn, job)

            elif isinstance(nentries, int) and nentries:
                logger.info(f'output file {lfn} has {nentries} event(s)')
                nevents += nentries
            else:  # should not reach this step
                logger.warning(f'case not handled for output file {lfn} with {nentries} event(s) (ignore)')

    status = (not failed)

    return status, nevents


def remove_from_stageout(lfn: str, job: JobData):
    """
    Remove the given lfn from the stage-out list.

    :param lfn: local file name (str)
    :param job: job object (JobData).
    """
    outdata = []
    for fspec in job.outdata:
        if fspec.lfn == lfn:
            logger.info(f'removing {lfn} from stage-out list')
        else:
            outdata.append(fspec)
    job.outdata = outdata


def remove_no_output_files(job: JobData):
    """
    Remove files from output file list if they are listed in allowNoOutput and do not exist.

    :param job: job object (JobData).
    """
    # first identify the files to keep
    _outfiles = []
    for fspec in job.outdata:
        filename = fspec.lfn
        path = os.path.join(job.workdir, filename)

        if filename in job.allownooutput:
            if os.path.exists(path):
                logger.info(f"file {filename} is listed in allowNoOutput but exists (will not be removed from "
                            f"list of files to be staged-out)")
                _outfiles.append(filename)
            else:
                logger.info(f"file {filename} is listed in allowNoOutput and does not exist (will be removed from list of files to be staged-out)")
        else:
            if os.path.exists(path):
                logger.info(f"file {filename} is not listed in allowNoOutput (will be staged-out)")
            else:
                logger.warning(f"file {filename} is not listed in allowNoOutput and does not exist (job will fail)")
            _outfiles.append(filename)

    # now remove the unwanted fspecs
    if len(_outfiles) != len(job.outdata):
        outdata = []
        for fspec in job.outdata:
            if fspec.lfn in _outfiles:
                outdata.append(fspec)
        job.outdata = outdata


def get_outfiles_records(subfiles: list) -> dict:
    """
    Extract file info from job report JSON subfiles entry.

    :param subfiles: list of subfiles (list)
    :return: file info dictionary with format { 'guid': .., 'size': .., 'nentries': .. (optional)} (dict).
    """
    res = {}
    for subfile in subfiles:
        res[subfile['name']] = {
            'guid': subfile['file_guid'],
            'size': subfile['file_size']
        }

        nentries = subfile.get('nentries', 'UNDEFINED')
        if isinstance(nentries, int):
            res[subfile['name']]['nentries'] = nentries
        else:
            logger.warning("nentries is undefined in job report")

    return res


class DictQuery(dict):
    """Helper class for parsing the job report."""

    def get(self, path: str, dst_dict: dict, dst_key: str):
        """
        Get value from dictionary.

        Updates dst_dict[dst_key] with the value from the dictionary.

        :param path: path to the value (str)
        :param dst_dict: destination dictionary (dict)
        :param dst_key: destination key (str)
        """
        keys = path.split("/")
        if len(keys) == 0:
            return
        last_key = keys.pop()
        me_ = self
        for key in keys:
            if not (key in me_ and isinstance(me_[key], dict)):
                return

            me_ = me_[key]

        if last_key in me_:
            dst_dict[dst_key] = me_[last_key]


def parse_jobreport_data(job_report: dict) -> dict:  # noqa: C901
    """
    Parse a job report and extract relevant fields.

    :param job_report: job report dictionary (dict)
    :return: work_attributes (dict).
    """
    work_attributes = {}
    if job_report is None or not any(job_report):
        return work_attributes

    # these are default values for job metrics
    core_count = ""
    work_attributes["nEvents"] = 0
    work_attributes["dbTime"] = ""
    work_attributes["dbData"] = ""
    work_attributes["inputfiles"] = []
    work_attributes["outputfiles"] = []

    if "ATHENA_PROC_NUMBER" in os.environ:
        logger.debug(f"ATHENA_PROC_NUMBER: {os.environ['ATHENA_PROC_NUMBER']}")
        work_attributes['core_count'] = int(os.environ['ATHENA_PROC_NUMBER'])
        core_count = os.environ['ATHENA_PROC_NUMBER']

    dictq = DictQuery(job_report)
    dictq.get("resource/transform/processedEvents", work_attributes, "nEvents")
    dictq.get("resource/transform/cpuTimeTotal", work_attributes, "cpuConsumptionTime")
    dictq.get("resource/machine/node", work_attributes, "node")
    dictq.get("resource/machine/model_name", work_attributes, "cpuConsumptionUnit")
    dictq.get("resource/dbTimeTotal", work_attributes, "dbTime")
    dictq.get("resource/dbDataTotal", work_attributes, "dbData")
    dictq.get("exitCode", work_attributes, "transExitCode")
    dictq.get("exitMsg", work_attributes, "exeErrorDiag")
    dictq.get("files/input", work_attributes, "inputfiles")
    dictq.get("files/output", work_attributes, "outputfiles")

    outputfiles_dict = {}
    for opf in work_attributes['outputfiles']:
        outputfiles_dict.update(get_outfiles_records(opf['subFiles']))
    work_attributes['outputfiles'] = outputfiles_dict

    if work_attributes['inputfiles']:
        work_attributes['nInputFiles'] = reduce(lambda a, b: a + b, [len(inpfiles['subFiles']) for inpfiles in
                                                                     work_attributes['inputfiles']])
    if 'resource' in job_report and 'executor' in job_report['resource']:
        j = job_report['resource']['executor']

        fin_report = defaultdict(int)
        for value in j.values():
            mem = value.get('memory', {})
            for key in ('Avg', 'Max'):
                for subk, subv in mem.get(key, {}).items():
                    fin_report[subk] += subv

        work_attributes.update(fin_report)

    workdir_size = get_disk_usage('.')
    work_attributes['jobMetrics'] = f"coreCount={core_count} " \
                                    f"nEvents={work_attributes['nEvents']} " \
                                    f"dbTime={work_attributes['dbTime']} " \
                                    f"dbData={work_attributes['dbData']} " \
                                    f"workDirSize={workdir_size}"
    del work_attributes["dbData"]
    del work_attributes["dbTime"]

    return work_attributes


def get_executor_dictionary(jobreport_dictionary: dict) -> dict:
    """
    Extract the 'executor' dictionary from with a job report.

    :param jobreport_dictionary: job report dictionary (dict)
    :return: executor_dictionary (dict).
    """
    executor_dictionary = {}
    if jobreport_dictionary != {}:

        if 'resource' in jobreport_dictionary:
            resource_dictionary = jobreport_dictionary['resource']
            if 'executor' in resource_dictionary:
                executor_dictionary = resource_dictionary['executor']
            else:
                logger.warning("no such key: executor")
        else:
            logger.warning("no such key: resource")

    return executor_dictionary


def get_resimevents(jobreport_dictionary: dict) -> int or None:
    """
    Extract and add up the resimevents from the job report.

    This information is reported with the jobMetrics.

    :param jobreport_dictionary: job report dictionary (dict)
    :return: resimevents (int or None).
    """
    resimevents = None

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for fmt in list(executor_dictionary.keys()):  # "ReSim"
            if 'resimevents' in executor_dictionary[fmt]:
                try:
                    resimevents = int(executor_dictionary[fmt]['resimevents'])
                except (KeyError, ValueError, TypeError):
                    pass
                else:
                    break

    return resimevents


def get_db_info(jobreport_dictionary: dict) -> tuple[int, int]:
    """
    Extract and add up the DB info from the job report.

    This information is reported with the jobMetrics.
    Note: this function adds up the different dbData and dbTime's in
    the different executor steps. In modern job reports this might have
    been done already by the transform and stored in dbDataTotal and dbTimeTotal.

    :param jobreport_dictionary: job report dictionary (dict)
    :return: db_time (int), db_data (int) (tuple).
    """
    db_time = 0
    db_data = 0

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for fmt in list(executor_dictionary.keys()):  # "RAWtoESD", ..,
            if 'dbData' in executor_dictionary[fmt]:
                try:
                    db_data += executor_dictionary[fmt]['dbData']
                except Exception:
                    pass
            else:
                logger.warning(f"format {fmt} has no such key: dbData")
            if 'dbTime' in executor_dictionary[fmt]:
                try:
                    db_time += executor_dictionary[fmt]['dbTime']
                except Exception:
                    pass
            else:
                logger.warning(f"format {fmt} has no such key: dbTime")

    return db_time, db_data


def get_db_info_str(db_time: int, db_data: int) -> (str, str):
    """
    Convert db_time, db_data to strings.

    E.g. dbData="105077960", dbTime="251.42".

    :param db_time: time in seconds (int)
    :param db_data: long integer (int)
    :return: db_time_s (str), db_data_s (str).
    """
    zero = 0

    db_data_s = ""
    if db_data != zero:
        db_data_s = f"{db_data}"

    db_time_s = ""
    if db_time != 0:
        db_time_s = f"{db_time:.2f}"

    return db_time_s, db_data_s


def get_cpu_times(jobreport_dictionary: dict) -> tuple[str, int, float]:
    """
    Extract and add up the total CPU times from the job report.

    E.g. ('s', 5790L, 1.0).

    Note: this function is used with Event Service jobs

    :param jobreport_dictionary: job report dictionary (dict)
    :return: cpu_conversion_unit (str), total_cpu_time (int), conversion_factor (output consistent with set_time_consumed()) (float) (tuple).
    """
    total_cpu_time = 0

    executor_dictionary = get_executor_dictionary(jobreport_dictionary)
    if executor_dictionary != {}:
        for fmt in list(executor_dictionary.keys()):  # "RAWtoESD", ..,
            try:
                total_cpu_time += executor_dictionary[fmt]['cpuTime']
            except KeyError:
                logger.warning(f"format {fmt} has no such key: cpuTime")
            except Exception:
                pass

    conversion_factor = 1.0
    cpu_conversion_unit = "s"

    return cpu_conversion_unit, total_cpu_time, conversion_factor


def get_exit_info(jobreport_dictionary: dict) -> tuple[int, str]:
    """
    Return the exit code (exitCode) and exit message (exitMsg).

    E.g. (0, 'OK').

    :param jobreport_dictionary:
    :return: exit_code (int), exit_message (str) (tuple).
    """
    return jobreport_dictionary.get('exitCode'), jobreport_dictionary.get('exitMsg')


def cleanup_looping_payload(workdir: str):
    """
    Run a special cleanup for looping payloads.

    Remove any root and tmp files.

    :param workdir: working directory (str).
    """
    for (root, _, files) in os.walk(workdir):
        for filename in files:
            if 'pool.root' in filename:
                path = os.path.join(root, filename)
                path = os.path.abspath(path)
                remove(path)


def cleanup_payload(workdir: str, outputfiles: list = None, removecores: bool = True):
    """
    Clean up payload (specifically AthenaMP) sub-directories prior to log file creation.

    Also remove core dumps.

    :param workdir: working directory (str)
    :param outputfiles: list of output files (list)
    :param removecores: remove core files if True (bool).
    """
    if outputfiles is None:
        outputfiles = []

    if removecores:
        remove_core_dumps(workdir)

    for ampdir in glob(f'{workdir}/athenaMP-workers-*'):
        for (root, _, files) in os.walk(ampdir):
            for filename in files:
                path = os.path.abspath(os.path.join(root, filename))

                core_file = ('core' in filename and removecores)
                pool_root_file = 'pool.root' in filename
                tmp_file = 'tmp.' in filename

                if core_file or pool_root_file or tmp_file:
                    remove(path)

                for outfile in outputfiles:
                    if outfile in filename:
                        remove(path)


def get_redundant_path() -> str:
    """
    Return the path to the file containing the redundant files and directories to be removed prior to log file creation.

    :return: file path (str).
    """
    filename = config.Pilot.redundant

    # correct /cvmfs if necessary
    if filename.startswith('/cvmfs') and os.environ.get('ATLAS_SW_BASE', False):
        filename = filename.replace('/cvmfs', os.environ.get('ATLAS_SW_BASE'))

    return filename


def get_redundants() -> list:
    """
    Get list of redundant files and directories (to be removed).

    The function will return the content of an external file. It that
    can't be read, then a list defined in this function will be returned instead.
    Any updates to the external file must be propagated to this function.

    :return: files and directories (list).
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
                "/cores",
                "/panda_pilot*",
                "/work",
                "README*",
                "MANIFEST*",
                "*.part*",
                "docs",
                "/venv",
                "/pilot3",
                "usr",
                "%1",
                "open_remote_file_cmd.sh"]

    return dir_list


def remove_archives(workdir: str):
    """
    Explicitly remove any soft linked archives (.a files) since they will be dereferenced by the tar command.

    (--dereference option).

    :param workdir: working directory (str).
    """
    matches = []
    for root, _, filenames in os.walk(workdir):
        for filename in fnmatch.filter(filenames, '*.a'):
            matches.append(os.path.join(root, filename))
    for root, _, filenames in os.walk(os.path.dirname(workdir)):
        for filename in fnmatch.filter(filenames, 'EventService_premerge_*.tar'):
            matches.append(os.path.join(root, filename))

    for match in matches:
        remove(match)


def cleanup_broken_links(workdir: str):
    """
    Run a second pass to clean up any broken links prior to log file creation.

    :param workdir: working directory (str).
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


def list_work_dir(workdir: str):
    """
    Execute ls -lF for the given directory and dump to log.

    :param workdir: directory name (str).
    """
    cmd = f'ls -lF {workdir}'
    _, stdout, stderr = execute(cmd)
    logger.debug(f'{stdout}:\n' + stderr)


def remove_special_files(workdir: str, dir_list: list):
    """
    Remove list of special files from the workdir.

    :param workdir: work directory (str)
    :param dir_list: list of special files (list)
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


def remove_redundant_files(workdir: str, outputfiles: list = None, piloterrors: list = None, debugmode: bool = False):
    """
    Remove redundant files and directories prior to creating the log file.

    Note: in debug mode, any core files should not be removed before creating the log.

    :param workdir: working directory (str)
    :param outputfiles: list of protected output files (list)
    :param piloterrors: list of Pilot assigned error codes (list)
    :param debugmode: True if debug mode has been switched on (bool).
    """
    if outputfiles is None:
        outputfiles = []
    if piloterrors is None:
        piloterrors = []
    logger.debug("removing redundant files prior to log creation")
    workdir = os.path.abspath(workdir)

    # remove core and pool.root files from AthenaMP subdirectories
    try:
        cleanup_payload(workdir, outputfiles, removecores=not debugmode)
    except OSError as exc:
        logger.warning(f"failed to execute cleanup_payload(): {exc}")

    # explicitly remove any soft linked archives (.a files)
    # since they will be dereferenced by the tar command (--dereference option)
    remove_archives(workdir)

    # remove special files
    # get list of redundant files and directories (to be removed)
    dir_list = get_redundants()

    remove_special_files(workdir, dir_list)

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


def download_command(process: dict, workdir: str, base_urls: list) -> dict:
    """
    Download the pre/postprocess commands if necessary.

    Process FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    :param process: pre/postprocess dictionary (dict)
    :param workdir: job workdir (str)
    :param base_urls: list of base URLs (list)
    :return: updated pre/postprocess dictionary (dict).
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
    :param base_urls: list of base URLs (list)
    :return: dictionary of utilities to be executed in parallel with the payload (dict or None).
    """
    if order == UTILITY_BEFORE_PAYLOAD and job.preprocess:
        return get_precopostprocess_command(job.preprocess, job.workdir, 'preprocess', base_urls)

    if order == UTILITY_WITH_PAYLOAD:
        return {'command': 'NetworkMonitor', 'args': '', 'label': 'networkmonitor', 'ignore_failure': True}

    if order == UTILITY_AFTER_PAYLOAD_STARTED:
        return get_utility_after_payload_started()

    if order == UTILITY_AFTER_PAYLOAD_STARTED2 and job.coprocess:
        return get_precopostprocess_command(job.coprocess, job.workdir, 'coprocess', base_urls)

    if order == UTILITY_AFTER_PAYLOAD_FINISHED:
        return get_xcache_command(
            job.infosys.queuedata.catchall,
            job.workdir,
            job.jobid,
            'xcache_kill',
            xcache_deactivation_command,
        )

    if order == UTILITY_AFTER_PAYLOAD_FINISHED2 and job.postprocess:
        return get_precopostprocess_command(job.postprocess, job.workdir, 'postprocess', base_urls)

    if order == UTILITY_BEFORE_STAGEIN:
        return get_xcache_command(
            job.infosys.queuedata.catchall,
            job.workdir,
            job.jobid,
            'xcache_start',
            xcache_activation_command,
        )

    return None


def get_precopostprocess_command(process: dict, workdir: str, label: str, base_urls: list) -> dict:
    """
    Return the pre/co/post-process command dictionary.

    Command FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    The returned command has the structure: { 'command': <string>, }

    :param process: pre/co/post-process (dict)
    :param workdir: working directory (str)
    :param label: label (str)
    :param base_urls: base URLs for trf download (list)
    :return: command (dict).
    """
    com = {}
    if process.get('command', ''):
        com = download_command(process, workdir, base_urls)
        com['label'] = label
        com['ignore_failure'] = False

    return com


def get_utility_after_payload_started() -> dict:
    """
    Return the command dictionary for the utility after the payload has started.

    Command FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    :return: command (dict).
    """
    com = {}
    try:
        cmd = config.Pilot.utility_after_payload_started
    except Exception:
        pass
    else:
        if cmd:
            com = {'command': cmd, 'args': '', 'label': cmd.lower(), 'ignore_failure': True}

    return com


def get_xcache_command(catchall: str, workdir: str, jobid: str, label: str, xcache_function: Any) -> dict:
    """
    Return the proper xcache command for either activation or deactivation.

    Command FORMAT: {'command': <command>, 'args': <args>, 'label': <some name>}

    :param catchall: queuedata catchall field (str)
    :param workdir: job working directory (str)
    :param jobid: PanDA job id (str)
    :param label: label (str)
    :param xcache_function: activation/deactivation function name (Any)
    :return: command (dict).
    """
    com = {}
    if 'pilotXcache' in catchall:
        com = xcache_function(jobid=jobid, workdir=workdir)
        com['label'] = label
        com['ignore_failure'] = True

    return com


def post_prestagein_utility_command(**kwargs: dict):
    """
    Execute any post pre-stage-in utility commands.

    :param kwargs: kwargs (dict).
    """
    label = kwargs.get('label', 'unknown_label')
    stdout = kwargs.get('output', None)

    if stdout:
        logger.debug(f'processing stdout for label={label}')
        xcache_proxy(stdout)
    else:
        logger.warning(f'no output for label={label}')

    alrb_xcache_files = os.environ.get('ALRB_XCACHE_FILES', '')
    if alrb_xcache_files:
        cmd = 'cat $ALRB_XCACHE_FILES/settings.sh'
        _, _stdout, _ = execute(cmd)
        logger.debug(f'cmd={cmd}:\n\n{_stdout}\n\n')


def xcache_proxy(output: str):
    """
    Extract env vars from xcache stdout and set them.

    :param output: command output (str).
    """
    # loop over each line in the xcache stdout and identify the needed environmental variables
    for line in output.split('\n'):
        if 'ALRB_XCACHE_PROXY' in line:
            suffix = '_REMOTE' if 'REMOTE' in line else ''
            name = f'ALRB_XCACHE_PROXY{suffix}'
            pattern = fr'\ export\ ALRB_XCACHE_PROXY{suffix}\=\"(.+)\"'
            set_xcache_var(line, name=name, pattern=pattern)

        elif 'ALRB_XCACHE_MYPROCESS' in line:
            set_xcache_var(
                line,
                name='ALRB_XCACHE_MYPROCESS',
                pattern=r'\ ALRB_XCACHE_MYPROCESS\=(.+)'
            )

        elif 'Messages logged in' in line:
            set_xcache_var(
                line,
                name='ALRB_XCACHE_LOG',
                pattern=r'xcache\ started\ successfully.\ \ Messages\ logged\ in\ (.+)'
            )

        elif 'ALRB_XCACHE_FILES' in line:
            set_xcache_var(
                line,
                name='ALRB_XCACHE_FILES',
                pattern=r'\ ALRB_XCACHE_FILES\=(.+)'
            )


def set_xcache_var(line: str, name: str = '', pattern: str = ''):
    """
    Extract the value of a given environmental variable from a given stdout line.

    :param line: line from stdout to be investigated (str)
    :param name: name of env var (str)
    :param pattern: regular expression pattern (str).
    """
    pattern = re.compile(pattern)
    result = re.findall(pattern, line)
    if result:
        os.environ[name] = result[0]


def xcache_activation_command(workdir: str = '', jobid: str = '') -> dict:
    """
    Return the xcache service activation command.

    Note: the workdir is not used here, but the function prototype
    needs it in the called (xcache_activation_command needs it).

    :param workdir: unused work directory - do not remove (str)
    :param jobid: PanDA job id to guarantee that xcache process is unique (int)
    :return: xcache command (str).
    """
    if workdir:  # to bypass pylint warning
        pass
    # a successful startup will set ALRB_XCACHE_PROXY and ALRB_XCACHE_PROXY_REMOTE
    # so any file access with root://...  should be replaced with one of
    # the above (depending on whether you are on the same machine or not)
    # example:
    # ${ALRB_XCACHE_PROXY}root://atlasxrootd-kit.gridka.de:1094//pnfs/gridka.de/../DAOD_FTAG4.24348858._000020.pool.root.1
    command = f"{get_asetup(asetup=False)} "

    # add 'xcache list' which will also kill any
    # orphaned processes lingering in the system
    command += (
        f"lsetup xcache; xcache list; xcache start -d $PWD/{jobid}/xcache -C centos7 --disklow 4g --diskhigh 5g -b 4"
    )

    return {'command': command, 'args': ''}


def xcache_deactivation_command(workdir: str = '', jobid: str = '') -> dict:
    """
    Return the xcache service deactivation command.

    This service should be stopped after the payload has finished.
    Copy the messages log before shutting down.

    Note: the job id is not used here, but the function prototype
    needs it in the called (xcache_activation_command needs it).

    :param workdir: payload work directory (str)
    :param jobid: unused job id - do not remove (str)
    :return: xcache command (dict).
    """
    if jobid:  # to bypass pylint warning
        pass
    path = os.environ.get('ALRB_XCACHE_LOG', None)
    if path and os.path.exists(path):
        logger.debug(f'copying xcache messages log file ({path}) to work dir ({workdir})')
        dest = os.path.join(workdir, 'xcache-messages.log')
        try:
            copy(path, dest)
        except Exception as exc:
            logger.warning(f'exception caught copying xcache log: {exc}')
    else:
        if not path:
            logger.warning('ALRB_XCACHE_LOG is not set')
        if path and not os.path.exists(path):
            logger.warning(f'path does not exist: {path}')
    command = f"{get_asetup(asetup=False)} "
    command += "lsetup xcache; xcache kill"  # -C centos7

    return {'command': command, 'args': '-p $ALRB_XCACHE_MYPROCESS'}


def get_utility_command_setup(name: str, job: JobData, setup: str = None) -> str:
    """
    Return the proper setup for the given utility command.

    If a payload setup is specified, then the utility command string should be prepended to it.

    :param name: name of utility (str)
    :param job: job object (JobData)
    :param setup: optional payload setup string (str)
    :return: utility command setup (str).
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

    if name == 'NetworkMonitor' and setup:
        return get_network_monitor_setup(setup, job)

    return ""


def get_utility_command_execution_order(name: str) -> int:
    """
    Determine if the given utility command should be executed before or after the payload.

    :param name: utility name (str)
    :return: execution order constant (int).
    """
    # example implementation
    if name == 'NetworkMonitor':
        return UTILITY_WITH_PAYLOAD

    if name == 'MemoryMonitor':
        return UTILITY_AFTER_PAYLOAD_STARTED

    logger.warning(f'unknown utility name: {name}')

    return UTILITY_AFTER_PAYLOAD_STARTED


def post_utility_command_action(name: str, job: JobData):
    """
    Perform post action for given utility command.

    :param name: name of utility command (str)
    :param job: job object (JobData).
    """
    if name == 'NetworkMonitor':
        pass
    elif name == 'MemoryMonitor':
        post_memory_monitor_action(job)


def get_utility_command_kill_signal(name: str) -> int:
    """
    Return the proper kill signal used to stop the utility command.

    :param name: name of utility command (str)
    :return: kill signal (int).
    """
    # note that the NetworkMonitor does not require killing (to be confirmed)
    return SIGUSR1 if name == 'MemoryMonitor' else SIGTERM


def get_utility_command_output_filename(name: str, selector: bool = None) -> str:
    """
    Return the filename to the output of the utility command.

    :param name: utility name (str)
    :param selector: optional special conditions flag (bool)
    :return: filename (str).
    """
    return get_memory_monitor_summary_filename(selector=selector) if name == 'MemoryMonitor' else ""


def verify_lfn_length(outdata: list) -> tuple[int, str]:
    """
    Make sure that the LFNs are all within the allowed length.

    :param outdata: list of FileSpec objects (list)
    :return: error code (int), diagnostics (str) (tuple).
    """
    exitcode = 0
    diagnostics = ""
    max_length = 255

    # loop over all output files
    for fspec in outdata:
        if len(fspec.lfn) > max_length:
            diagnostics = f"LFN too long (length: {len(fspec.lfn)}, " \
                          f"must be less than {max_length} characters): {fspec.lfn}"
            exitcode = errors.LFNTOOLONG
            break

    return exitcode, diagnostics


def verify_ncores(corecount: int):
    """
    Verify that nCores settings are correct.

    :param corecount: number of cores (int).
    """
    try:
        del os.environ['ATHENA_PROC_NUMBER_JOB']
        logger.debug("unset existing ATHENA_PROC_NUMBER_JOB")
    except Exception:
        pass

    try:
        athena_proc_number = int(os.environ.get('ATHENA_PROC_NUMBER', None))
    except Exception:
        athena_proc_number = None

    # Note: if ATHENA_PROC_NUMBER is set (by the wrapper), then do not
    # overwrite it. Otherwise, set it to the value of job.coreCount
    # (actually set ATHENA_PROC_NUMBER_JOB and use it if it exists,
    # otherwise use ATHENA_PROC_NUMBER directly; ATHENA_PROC_NUMBER_JOB
    # will always be the value from the job definition)
    if athena_proc_number:
        logger.info(f"encountered a set ATHENA_PROC_NUMBER ({athena_proc_number}), will not overwrite it")
        logger.info('set ATHENA_CORE_NUMBER to same value as ATHENA_PROC_NUMBER')
        os.environ['ATHENA_CORE_NUMBER'] = str(athena_proc_number)
    else:
        os.environ['ATHENA_PROC_NUMBER_JOB'] = str(corecount)
        os.environ['ATHENA_CORE_NUMBER'] = str(corecount)
        logger.info(f"set ATHENA_PROC_NUMBER_JOB and ATHENA_CORE_NUMBER to {corecount} "
                    f"(ATHENA_PROC_NUMBER will not be overwritten)")


def verify_job(job: JobData) -> bool:
    """
    Verify job parameters for specific errors.

    Note:
      in case of problem, the function should set the corresponding pilot error code using:
      job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())

    :param job: job object (JobData)
    :return: True if verified, False otherwise (bool).
    """
    status = False

    # are LFNs of correct lengths?
    exitcode, diagnostics = verify_lfn_length(job.outdata)
    if exitcode != 0:
        logger.fatal(diagnostics)
        job.piloterrordiag = diagnostics
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exitcode)
    else:
        status = True

    # check the ATHENA_PROC_NUMBER settings
    verify_ncores(job.corecount)

    # make sure there were no earlier problems
    if status and job.piloterrorcodes:
        logger.warning(f'job has errors: {job.piloterrorcodes}')
        status = False

    return status


def update_stagein(job: JobData):
    """
    Skip DBRelease files during stage-in.

    :param job: job object (JobData).
    """
    for fspec in job.indata:
        if 'DBRelease' in fspec.lfn:
            fspec.status = 'no_transfer'


def get_metadata(workdir: str) -> dict or None:
    """
    Return the metadata from file.

    :param workdir: work directory (str)
    :return: metadata (dict).
    """
    path = os.path.join(workdir, config.Payload.jobreport)
    metadata = read_file(path) if os.path.exists(path) else None
    logger.debug(f'metadata={metadata}')

    return metadata


def should_update_logstash(frequency: int = 10) -> bool:
    """
    Determine if logstash should be updated with prmon dictionary.

    :param frequency: update frequency (int)
    :return: return True once per 'frequency' times (bool).
    """
    return randint(0, frequency - 1) == 0


def update_server(job: JobData) -> None:
    """
    Perform any user specific server actions.

    E.g. this can be used to send special information to a logstash.

    :param job: job object (JobData).
    """
    # attempt to read memory_monitor_output.txt and convert it to json
    if not should_update_logstash():
        logger.debug('no need to update logstash for this job')
        return

    path = os.path.join(job.workdir, get_memory_monitor_output_filename())
    if not os.path.exists(path):
        logger.warning(f'path does not exist: {path}')
        return

    # convert memory monitor text output to json and return the selection
    # (don't store it, log has already been created)
    metadata_dictionary = get_metadata_dict_from_txt(path, storejson=True, jobid=job.jobid)
    if metadata_dictionary:
        # the output was previously written to file,
        # update the path and tell curl to send it
        new_path = update_extension(path=path, extension='json')

        # out = read_json(new_path)
        # logger.debug(f'prmon json=\n{out}')
        # logger.debug(f'final logstash prmon dictionary: {metadata_dictionary}')
        url = 'https://pilot.atlas-ml.org'  # 'http://collector.atlas-ml.org:80'
        status = upload_file(url, new_path)
        if status:
            logger.info('sent prmon JSON dictionary to logstash server (urllib method)')
        else:
            cmd = (
                f"curl --connect-timeout 20 --max-time 120 -H \"Content-Type: application/json\" -X POST "
                f"--upload-file {new_path} {url}"
            )
            # send metadata to logstash
            try:
                _, stdout, stderr = execute(cmd, usecontainer=False)
            except Exception as exc:
                logger.warning(f'exception caught: {exc}')
            else:
                logger.info('sent prmon JSON dictionary to logstash server (curl method)')
                logger.debug(f'stdout: {stdout}')
                logger.debug(f'stderr: {stderr}')
    else:
        msg = 'no prmon json available - cannot send anything to logstash server'
        logger.warning(msg)

    return


def preprocess_debug_command(job: JobData):
    """
    Pre-process the debug command in debug mode.

    :param job: Job object (JobData).
    """
    # Should the pilot do the setup or does jobPars already contain the information?
    preparesetup = should_pilot_prepare_setup(job.noexecstrcnv, job.jobparams)
    # get the general setup command and then verify it if required
    resource_name = get_resource_name()  # 'grid' if no hpc_resource is set

    resource = __import__(f'pilot.user.atlas.resource.{resource_name}', globals(), locals(), [resource_name], 0)

    cmd = resource.get_setup_command(job, preparesetup)
    if not cmd.endswith(';'):
        cmd += '; '
    if cmd not in job.debug_command:
        job.debug_command = cmd + job.debug_command


def process_debug_command(debug_command: str, pandaid: str) -> str:
    """
    Process the debug command in debug mode.

    In debug mode, the server can send a special debug command to the piloti
    via the updateJob backchannel. This function can be used to process that
    command, i.e. to identify a proper pid to debug (which is unknown
    to the server).

    For gdb, the server might send a command with gdb option --pid %.
    The pilot need to replace the % with the proper pid. The default
    (hardcoded) process will be that of athena.py. The pilot will find the
    corresponding pid.

    :param debug_command: debug command (str)
    :param pandaid: PanDA id (str)
    :return: updated debug command (str).
    """
    if '--pid %' not in debug_command:
        return debug_command

    # replace the % with the pid for athena.py
    # note: if athena.py is not yet running, the --pid % will remain.
    # Otherwise the % will be replaced by the pid first find the pid
    # (if athena.py is running)
    cmd = 'ps axo pid,ppid,pgid,args'
    _, stdout, _ = execute(cmd)
    if stdout:
        # convert the ps output to a dictionary
        dictionary = convert_ps_to_dict(stdout)

        # trim this dictionary to reduce the size
        # (only keep the PID and PPID lists)
        trimmed_dictionary = get_trimmed_dictionary(['PID', 'PPID'], dictionary)

        # what is the pid of the trf?
        pandaid_pid = find_pid(pandaid, dictionary)

        # find all athena processes
        pids = find_cmd_pids('athena.py', dictionary)

        # which of the found pids are children of the trf?
        # (which has an export PandaID=.. attached to it)
        for pid in pids:
            try:
                child = is_child(pid, pandaid_pid, trimmed_dictionary)
            except RuntimeError as rte:
                logger.warning(f'too many recursions: {rte} (cannot identify athena process)')
            else:
                if child:
                    logger.info(f'pid={pid} is a child process of the trf of this job')
                    debug_command = debug_command.replace('--pid %', f'--pid {pid}')
                    logger.info(f'updated debug command: {debug_command}')
                    break
                logger.info(f'pid={pid} is not a child process of the trf of this job')

        if not pids or '--pid %' in debug_command:
            logger.debug('athena is not yet running (no corresponding pid)')

            # reset the command to prevent the payload from being killed
            # (will be killed when gdb has run)
            debug_command = ''

    return debug_command


def allow_timefloor(submitmode: str) -> bool:
    """
    Decide if the timefloor mechanism (for multi-jobs) should be allowed for the given submit mode.

    :param submitmode: submit mode (str)
    :return: always True for ATLAS (bool).
    """
    if submitmode:  # to bypass pylint score 0
        pass

    return True


def get_pilot_id(jobid: str) -> str:
    """
    Get the pilot id from the environment variable GTAG.

    Update if necessary (not for ATLAS since we want the same pilot id for all multi-jobs).

    :param jobid: PanDA job id - UNUSED (str)
    :return: pilot id (str).
    """
    if jobid:  # to bypass pylint score 0
        pass

    return os.environ.get("GTAG", "unknown")
