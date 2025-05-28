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

"""Functions related to containerisation for generic user."""

import json
import logging
import os
import re
from collections.abc import Callable
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    #PilotException,
    FileHandlingFailure
)
from pilot.info import (
    #InfoService,
    #infosys,
    JobData
)
from pilot.user.generic.setup import (
    get_asetup,
    #get_file_system_root_path
)
from pilot.util.config import config
from pilot.util.container import obscure_token
from pilot.util.filehandling import (
    #grep,
    #remove,
    write_file
)
errors = ErrorCodes()
logger = logging.getLogger(__name__)


def do_use_container(**kwargs: dict) -> bool:
    """
    Decide whether to use a container or not.

    :param kwargs: dictionary of key-word arguments (dict)
    :return: True is function has decided that a container should be used, False otherwise (bool).
    """
    # to force no container use: return False
    use_container = False

    job = kwargs.get('job', False)
    copytool = kwargs.get('copytool', False)
    if job:
        # for user jobs, TRF option --containerImage must have been used, ie imagename must be set
        if job.imagename and job.imagename != 'NULL':
            use_container = True
        elif not (job.platform or job.alrbuserplatform):
            use_container = False
        else:
            queuedata = job.infosys.queuedata
            container_name = queuedata.container_type.get("pilot")
            if container_name:
                use_container = True
    elif copytool:
        # override for copytools - use a container for stage-in/out
        use_container = True

    return use_container


def wrapper(executable: str, **kwargs: dict) -> Callable[..., Any]:
    """
    Wrap given function for any container specific usage.

    This function will be called by pilot.util.container.execute() and prepends the executable with a container command.

    :param executable: command to be executed (str)
    :param kwargs: dictionary of key-word arguments (dict)
    :return: executable wrapped with container command (Callable).
    """
    workdir = kwargs.get('workdir', '.')
    pilot_home = os.environ.get('PILOT_HOME', '')
    job = kwargs.get('job', None)

    if workdir == '.' and pilot_home != '':
        workdir = pilot_home

    # if job.imagename (from --containerimage <image>) is set, then always use raw singularity/apptainer
    if config.Container.setup_type == "ALRB":  # and job and not job.imagename:
        fctn = alrb_wrapper
    else:
        fctn = alrb_wrapper  # no support for container_wrapper

    return fctn(executable, workdir, job=job)


def fix_asetup(asetup: str) -> str:
    """
    Make sure that the command returned by get_asetup() contains a trailing ;-sign.

    :param asetup: asetup (str)
    :return: updated asetup (str).
    """
    if asetup and not asetup.strip().endswith(';'):
        asetup += '; '

    return asetup


def extract_atlas_setup(asetup: str, swrelease: str) -> tuple[str, str]:
    """
    Extract the asetup command from the full setup command for jobs that have a defined release.

    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
      source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;source $AtlasSetup/scripts/asetup.sh
    -> $AtlasSetup/scripts/asetup.sh, export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; source
         ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;

    :param asetup: full asetup command (str).
    :param swrelease: ATLAS release (str).
    :return: extracted asetup command (str), cleaned up full asetup command without asetup.sh (str) (tuple).
    """
    if not swrelease:
        return '', ''

    try:
        # source $AtlasSetup/scripts/asetup.sh
        asetup = asetup.strip()
        atlas_setup = asetup.split(';')[-1] if not asetup.endswith(';') else asetup.split(';')[-2]
        cleaned_atlas_setup = asetup.replace(atlas_setup, '').replace(';;', ';')
        atlas_setup = atlas_setup.replace('source ', '')
    except AttributeError as exc:
        logger.debug(f'exception caught while extracting asetup command: {exc}')
        atlas_setup = ''
        cleaned_atlas_setup = ''

    return atlas_setup, cleaned_atlas_setup


def get_full_asetup(cmd: str, atlas_setup: str) -> str:
    """
    Extract the full asetup command from the payload execution command.

    (Easier that generating it again). We need to remove this command for stand-alone containers.
    Alternatively: do not include it in the first place (but this seems to trigger the need for further changes).
    atlas_setup is "source $AtlasSetup/scripts/asetup.sh", which is extracted in a previous step.
    The function typically returns: "source $AtlasSetup/scripts/asetup.sh 21.0,Athena,2020-05-19T2148,notest --makeflags='$MAKEFLAGS';".

    :param cmd: payload execution command (str)
    :param atlas_setup: extracted atlas setup (str)
    :return: full atlas setup (str).
    """
    pos = cmd.find(atlas_setup)
    cmd = cmd[pos:]  # remove everything before 'source $AtlasSetup/..'
    pos = cmd.find(';')
    cmd = cmd[:pos + 1]  # remove everything after the first ;, but include the trailing ;

    return cmd


def update_for_user_proxy(setup_cmd: str, cmd: str, is_analysis: bool = False, queue_type: str = '') -> tuple[int, str, str, str]:
    """
    Add the X509 user proxy to the container sub command string if set, and remove it from the main container command.

    Try to receive payload proxy and update X509_USER_PROXY in container setup command
    In case payload proxy from server is required, this function will also download and verify this proxy.

    :param setup_cmd: container setup command (str)
    :param cmd: command the container will execute (str)
    :param is_analysis: True for user job (bool)
    :param queue_type: queue type (e.g. 'unified') (str)
    :return: exit_code (int), diagnostics (str), updated _cmd (str), updated cmd (str) (tuple).
    """
    exit_code = 0
    diagnostics = ""

    #x509 = os.environ.get('X509_USER_PROXY', '')
    x509 = os.environ.get('X509_UNIFIED_DISPATCH', os.environ.get('X509_USER_PROXY', ''))
    if x509 != "":
        # do not include the X509_USER_PROXY in the command the container will execute
        cmd = cmd.replace(f"export X509_USER_PROXY={x509};", '')
        # add it instead to the container setup command:

        # not support yet, see atlas code if needed
        # download and verify payload proxy from the server if desired
        #proxy_verification = os.environ.get('PILOT_PROXY_VERIFICATION') == 'True' and os.environ.get('PILOT_PAYLOAD_PROXY_VERIFICATION') == 'True'
        #if proxy_verification and config.Pilot.payload_proxy_from_server and is_analysis and queue_type != 'unified':
        #    voms_role = get_voms_role(role='user')
        #    exit_code, diagnostics, x509 = get_and_verify_proxy(x509, voms_role=voms_role, proxy_type='payload')
        #    if exit_code != 0:  # do not return non-zero exit code if only download fails
        #        logger.warning('payload proxy verification failed')

        # add X509_USER_PROXY setting to the container setup command
        setup_cmd = f"export X509_USER_PROXY={x509};" + setup_cmd

    return exit_code, diagnostics, setup_cmd, cmd


def extract_full_atlas_setup(cmd: str, atlas_setup: str) -> tuple[str, str]:
    """
    Extract the full asetup (including options) from the payload setup command.

    atlas_setup is typically '$AtlasSetup/scripts/asetup.sh'.

    :param cmd: full payload setup command (str)
    :param atlas_setup: asetup command (str)
    :return: extracted full asetup command (str), updated full payload setup command without asetup part (str) (tuple).
    """
    updated_cmds = []
    extracted_asetup = ""

    if not atlas_setup:
        return extracted_asetup, cmd

    try:
        _cmd = cmd.split(';')
        for subcmd in _cmd:
            if atlas_setup in subcmd:
                extracted_asetup = subcmd
            else:
                updated_cmds.append(subcmd)
        updated_cmd = ';'.join(updated_cmds)
    except AttributeError as exc:
        logger.warning(f'exception caught while extracting full atlas setup: {exc}')
        updated_cmd = cmd

    return extracted_asetup, updated_cmd


def create_release_setup(cmd: str, atlas_setup: str, full_atlas_setup: str, release: str, workdir: str, is_cvmfs: bool) -> tuple[str, str]:
    """
    Get the proper release setup script name, and create the script if necessary.

    This function also updates the cmd string (removes full asetup from payload command).

    :param cmd: Payload execution command (str)
    :param atlas_setup: asetup command (str)
    :param full_atlas_setup: full asetup command (str)
    :param release: software release, needed to determine Athena environment (str)
    :param workdir: job workdir (str)
    :param is_cvmfs: does the queue have cvmfs? (bool)
    :return: proper release setup name (str), updated cmd (str).
    """
    release_setup_name = '/srv/my_release_setup.sh'

    # extracted_asetup should be written to 'my_release_setup.sh' and cmd to 'container_script.sh'
    content = f'echo \"INFO: sourcing {release_setup_name} inside the container. ' \
              f'This should not run if it is a ATLAS standalone container\"'
    if is_cvmfs and release and release != 'NULL':
        content, cmd = extract_full_atlas_setup(cmd, atlas_setup)
        if not content:
            content = full_atlas_setup
        content = 'retCode=0\n' + content

    content += '\nretCode=$?'
    # add timing info (hours:minutes:seconds in UTC)
    # this is used to get a better timing info about setup
    content += '\ndate +\"%H:%M:%S %Y/%m/%d\"'  # e.g. 07:36:27 2022/06/29
    content += '\nif [ $? -ne 0 ]; then'
    content += '\n    retCode=$?'
    content += '\nfi'
    content += '\nreturn $retCode'

    logger.debug(f'command to be written to release setup file:\n\n{release_setup_name}:\n\n{content}\n')
    try:
        write_file(os.path.join(workdir, os.path.basename(release_setup_name)), content, mute=False)
    except FileHandlingFailure as exc:
        logger.warning(f'exception caught: {exc}')

    return release_setup_name, cmd.replace(';;', ';')


def replace_last_command(cmd: str, replacement: str) -> str:
    """
    Replace the last command in cmd with given replacement.

    :param cmd: command (str)
    :param replacement: replacement (str)
    :return: updated command (str).
    """
    cmd = cmd.strip('; ')
    last_bit = cmd.split(';')[-1]
    cmd = cmd.replace(last_bit.strip(), replacement)

    return cmd


## DEPRECATED, remove after verification with user container job
def remove_container_string(job_params: str) -> tuple[str, str]:
    """
    Retrieve the container string from the job parameters.

    :param job_params: job parameters (str)
    :return: updated job parameters (str), extracted container path (str) (tuple).
    """
    pattern = r" \'?\-\-containerImage\=?\ ?([\S]+)\ ?\'?"
    compiled_pattern = re.compile(pattern)

    # remove any present ' around the option as well
    job_params = re.sub(r'\'\ \'', ' ', job_params)

    # extract the container path
    found = re.findall(compiled_pattern, job_params)
    container_path = found[0] if found else ""

    # Remove the pattern and update the job parameters
    job_params = re.sub(pattern, ' ', job_params)

    return job_params, container_path


def get_container_options(container_options: str) -> str:
    """
    Get the container options from AGIS for the container execution command.

    For Raythena ES jobs, replace the -C with "" (otherwise IPC does not work, needed by yampl).

    :param container_options: container options from AGIS (str)
    :return: updated container command (str).
    """
    is_raythena = os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') == 'raythena'

    opts = ''
    # Set the singularity/apptainer options
    if container_options:
        # the event service payload cannot use -C/--containall since it will prevent yampl from working
        if is_raythena:
            if '-C' in container_options:
                container_options = container_options.replace('-C', '')
            if '--containall' in container_options:
                container_options = container_options.replace('--containall', '')
        if container_options:
            opts += f'-e "{container_options}"'
    # consider using options "-c -i -p" instead of "-C". The difference is that the latter blocks all environment
    # variables by default and the former does not
    # update: skip the -i to allow IPC, otherwise yampl won't work
    elif is_raythena:
        pass
        # opts += 'export ALRB_CONT_CMDOPTS=\"$ALRB_CONT_CMDOPTS -c -i -p\";'
    else:
        #opts += '-e \"-C\"'
        opts += '-e \"-c -i\"'

    return opts


def add_asetup(job: JobData, alrb_setup: str, is_cvmfs: bool, release_setup: str, container_script: str, container_options: str) -> str:
    """
    Add atlasLocalSetup and options to form the final payload command.

    :param job: job object (JobData)
    :param alrb_setup: ALRB setup (str)
    :param is_cvmfs: True for cvmfs sites (bool)
    :param release_setup: release setup (str)
    :param container_script: container script name (str)
    :param container_options: container options (str)
    :return: final payload command (str).
    """
    # this should not be necessary after the extract_container_image() in JobData update
    # containerImage should have been removed already
    if '--containerImage' in job.jobparams:
        job.jobparams, container_path = remove_container_string(job.jobparams)
        if job.alrbuserplatform:
            if not is_cvmfs:
                alrb_setup += f'source ${{ATLAS_LOCAL_ROOT_BASE}}/user/atlasLocalSetup.sh -c {job.alrbuserplatform}'
        elif container_path != "":
            alrb_setup += f'source ${{ATLAS_LOCAL_ROOT_BASE}}/user/atlasLocalSetup.sh -c {container_path}'
        else:
            logger.warning(f'failed to extract container path from {job.jobparams}')
            alrb_setup = ""
        if alrb_setup and not is_cvmfs:
            alrb_setup += ' -d'
    else:
        alrb_setup += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --shell bash '
        if job.platform or job.alrbuserplatform or job.imagename:
            alrb_setup += '-c $thePlatform'
            if not is_cvmfs:
                alrb_setup += ' -d'

    # update the ALRB setup command
    alrb_setup += f' -s {release_setup}'
    alrb_setup += ' -r /srv/' + container_script
    alrb_setup = alrb_setup.replace('  ', ' ').replace(';;', ';')

    # add container options
    alrb_setup += ' ' + get_container_options(container_options)
    alrb_setup = alrb_setup.replace('  ', ' ')
    cmd = alrb_setup

    # correct full payload command in case preprocess command are used (ie replace trf with setupATLAS -c ..)
    #if job.preprocess and job.containeroptions:
    #    logger.debug(f'will update cmd={cmd}')
    #    cmd = replace_last_command(cmd, 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c $thePlatform')
    #    logger.debug('updated cmd with containerImage')

    return cmd


def has_docker_pattern(line: str, pattern: str = None) -> bool:
    """
    Check if the given line contains a docker pattern.

    :param line: panda secret (str)
    :param pattern: regular expression pattern (str)
    :return: True or False (bool).
    """
    found = False

    if line:
        # if no given pattern, look for a general docker registry URL
        url_pattern = get_url_pattern() if not pattern else pattern
        match = re.search(url_pattern, line)
        if match:
            logger.warning('the given line contains a docker token')
            found = True

    return found


def get_docker_pattern() -> str:
    """
    Return the docker login URL pattern for secret verification.

    Examples:
     docker login <registry URL> -u <username> -p <token>
     apptainer remote login -u <username> -p <lxplus password> <registry URL>

    :return: pattern (str).
    """
    return (
        # fr"docker\ login\ {get_url_pattern()}\ \-u\ \S+\ \-p\ \S+;"
        fr"apptainer\ remote\ login\ \-u\ \S+\ \-p\ \S+\ {get_url_pattern()};"
    )


def get_url_pattern() -> str:
    """
    Return the URL pattern for secret verification.

    :return: pattern (str).
    """
    return (
        r"docker?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\."
        r"[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*)"
    )


def add_docker_login(cmd: str, pandasecrets: dict) -> dict:
    """
    Add docker login to user command.

    The pandasecrets dictionary was found to contain login information (username + token). This function
    will add it to the payload command that will be run in the user container.

    :param cmd: payload command (str)
    :param pandasecrets: panda secrets (dict)
    :return: updated payload command (str).
    """
    pattern = r'docker://[^/]+/'
    tmp = json.loads(pandasecrets)
    docker_tokens = tmp.get('DOCKER_TOKENS', None)
    if docker_tokens:
        try:
            docker_token = json.loads(docker_tokens)
            if docker_token:
                token_dict = docker_token[0]
                username = token_dict.get('username', None)
                token = token_dict.get('token', None)
                registry_path = token_dict.get('registry_path', None)
                if username and token and registry_path:
                    # extract the registry (e.g. docker://gitlab-registry.cern.ch/) from the path
                    try:
                        match = re.search(pattern, registry_path)
                        if match:
                            # cmd = f'docker login {match.group(0)} -u {username} -p {token}; ' + cmd
                            cmd = f'apptainer remote login -u {username} -p {token} {match.group(0)}; ' + cmd
                        else:
                            logger.warning(f'failed to extract registry from {registry_path}')
                    except re.error as regex_error:
                        err = str(regex_error)
                        entry = err.find('token')
                        err = err[:entry]  # cut away any token
                        logger.warning(f'error in regular expression: {err}')
                else:
                    logger.warning(
                        'either username, token, or registry_path was not set in DOCKER_TOKENS dictionary')
            else:
                logger.warning('failed to convert DOCKER_TOKENS str to dict')
        except json.JSONDecodeError as json_error:
            err = str(json_error)
            entry = err.find('token')
            err = err[:entry]  # cut away any token
            logger.warning(f'error decoding JSON data: {err}')
    else:
        logger.warning('failed to read DOCKER_TOKENS key from panda secrets')

    return cmd


def alrb_wrapper(cmd: str, workdir: str, job: JobData = None) -> str:
    """
    Wrap the given command with the special ALRB setup for containers
    E.g. cmd = /bin/bash hello_world.sh
    ->
    export thePlatform="x86_64-slc6-gcc48-opt"
    export ALRB_CONT_RUNPAYLOAD="cmd'
    setupATLAS -c $thePlatform

    :param cmd: command to be executed in a container (str)
    :param workdir: (not used) (str)
    :param job: job object (JobData)
    :return: prepended command with singularity/apptainer execution command (str).
    """
    if workdir:  # bypass pylint warning
        pass
    if not job:
        logger.warning('the ALRB wrapper did not get a job object - cannot proceed')
        return cmd

    queuedata = job.infosys.queuedata
    container_name = queuedata.container_type.get("pilot")  # resolve container name for user=pilot
    if container_name:
        # first get the full setup, which should be removed from cmd (or ALRB setup won't work)
        _asetup = get_asetup()
        _asetup = fix_asetup(_asetup)
        # get_asetup()
        # -> export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
        #     --quiet;source $AtlasSetup/scripts/asetup.sh
        # atlas_setup = $AtlasSetup/scripts/asetup.sh
        # clean_asetup = export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source
        #                   ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        atlas_setup, clean_asetup = extract_atlas_setup(_asetup, job.swrelease)
        full_atlas_setup = get_full_asetup(cmd, 'source ' + atlas_setup) if atlas_setup and clean_asetup else ''

        # do not include 'clean_asetup' in the container script
        if clean_asetup and full_atlas_setup:
            cmd = cmd.replace(clean_asetup, '')
            # for stand-alone containers, do not include the full atlas setup either
            if job.imagename:
                cmd = cmd.replace(full_atlas_setup, '')

        # get_asetup(asetup=False)
        # -> export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;

        # get simplified ALRB setup (export)
        alrb_setup = get_asetup(alrb=True, add_if=True)
        alrb_setup = fix_asetup(alrb_setup)

        # get_asetup(alrb=True)
        # -> export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
        # get_asetup(alrb=True, add_if=True)
        # -> if [ -z "$ATLAS_LOCAL_ROOT_BASE" ]; then export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; fi;

        # add user proxy if necessary (actually it should also be removed from cmd)
        exit_code, diagnostics, alrb_setup, cmd = update_for_user_proxy(alrb_setup, cmd, is_analysis=job.is_analysis(), queue_type=job.infosys.queuedata.type)
        if exit_code:
            job.piloterrordiag = diagnostics
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(exit_code)
        # set the platform info
        alrb_setup = set_platform(job, alrb_setup)

        # add the jobid to be used as an identifier for the payload running inside the container
        # it is used to identify the pid for the process to be tracked by the memory monitor
        if 'export PandaID' not in alrb_setup:
            alrb_setup += f"export PandaID={job.jobid};"

        # add TMPDIR
        cmd = "export TMPDIR=/srv;export GFORTRAN_TMPDIR=/srv;" + cmd
        cmd = cmd.replace(';;', ';')

        # get the proper release setup script name, and create the script if necessary
        release_setup, cmd = create_release_setup(cmd, atlas_setup, full_atlas_setup, job.swrelease,
                                                  job.workdir, queuedata.is_cvmfs)

        # correct full payload command in case preprocess command are used (ie replace trf with setupATLAS -c ..)
        if job.preprocess and job.containeroptions:
            cmd = replace_last_command(cmd, job.containeroptions.get('containerExec'))

        # write the full payload command to a script file
        container_script = config.Container.container_script
        if cmd:
            logger.info(f'command to be written to container script file:\n\n{container_script}:\n\n{cmd}\n')
        else:
            logger.warning('will not show container script file since the user token could not be obscured')
        try:
            write_file(os.path.join(job.workdir, container_script), cmd, mute=False)
            os.chmod(os.path.join(job.workdir, container_script), 0o755)
        except (FileHandlingFailure, OSError) as exc:
            logger.warning(f'exception caught: {exc}')
            return ""

        # also store the command string in the job object
        job.command = cmd

        # add atlasLocalSetup command + options (overwrite the old cmd since the new cmd is the containerised version)
        cmd = add_asetup(job, alrb_setup, queuedata.is_cvmfs, release_setup, container_script, queuedata.container_options)

        # add any container options if set
        execargs = job.containeroptions.get('execArgs', None)
        if execargs:
            cmd += ' ' + execargs

        # prepend the docker login if necessary
        # does the pandasecrets dictionary contain any docker login info?
        pandasecrets = str(job.pandasecrets)
        if pandasecrets and "token" in pandasecrets and \
                has_docker_pattern(pandasecrets, pattern=r'docker://[^/]+/'):
            # if so, add it do the container script
            logger.info('adding sensitive docker login info')
            cmd = add_docker_login(cmd, job.pandasecrets)

        _cmd = obscure_token(cmd)  # obscure any token if present
        logger.debug(f'\n\nfinal command:\n\n{_cmd}\n')
    else:
        logger.warning('container name not defined in CRIC')

    return cmd


def create_stagein_container_command(workdir: str, cmd: str):
    """
    Create the stage-in container command.

    The function takes the isolated stage-in command, adds bits and pieces needed for the containerisation and stores
    it in a stagein.sh script file. It then generates the actual command that will execute the stage-in script in a
    container.

    :param workdir: working directory where script will be stored (str)
    :param cmd: isolated stage-in command (str)
    :return: container command to be executed (str).
    """
    if workdir:  # to bypass pylint score 0
        pass

    return cmd


def set_platform(job: JobData, alrb_setup: str) -> str:
    """
    Set thePlatform variable and add it to the sub container command.

    :param job: job object (JobData)
    :param alrb_setup: ALRB setup (str)
    :return: updated ALRB setup (str).
    """
    if job.alrbuserplatform:
        alrb_setup += f'export thePlatform="{job.alrbuserplatform}";'
    elif job.preprocess and job.containeroptions:
        alrb_setup += f"export thePlatform=\"{job.containeroptions.get('containerImage')}\";"
    elif job.imagename:
        alrb_setup += f'export thePlatform="{job.imagename}";'
    elif job.platform:
        alrb_setup += f'export thePlatform="{job.platform}";'

    return alrb_setup
