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
# - Alexander Bogdanchikov, Alexander.Bogdanchikov@cern.ch, 2019-20

"""Functions related to containerisation for ATLAS."""

import fcntl
import json
import logging
import os
import re
import select
import shlex
import subprocess
import time

from collections.abc import Callable
from typing import Any

# for user container test: import urllib

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    PilotException,
    FileHandlingFailure
)
from pilot.user.atlas.setup import (
    get_asetup,
    get_file_system_root_path
)
from pilot.user.atlas.proxy import (
    get_and_verify_proxy,
    get_voms_role
)
from pilot.info import (
    InfoService,
    infosys,
    JobData
)
from pilot.util.config import config
from pilot.util.constants import get_rucio_client_version
from pilot.util.container import obscure_token
from pilot.util.filehandling import (
    grep,
    remove,
    write_file
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def do_use_container(**kwargs: dict) -> bool:
    """
    Decide whether to use a container or not.

    :param kwargs: dictionary of key-word arguments (dict)
    :return: True if function has decided that a container should be used, False otherwise (bool).
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
        fctn = container_wrapper
    return fctn(executable, workdir, job=job)


def extract_platform_and_os(platform: str) -> str:
    """
    Extract the platform and OS substring from platform.

    :param platform: platform info, e.g. "x86_64-slc6-gcc48-opt" (str)
    :return: extracted platform specifics, e.g. "x86_64-slc6". In case of failure, return the full platform (str).
    """
    pattern = r"([A-Za-z0-9_-]+)-.+-.+"
    found = re.findall(re.compile(pattern), platform)

    if found:
        ret = found[0]
    else:
        logger.warning(f"could not extract architecture and OS substring using pattern={pattern} from "
                       f"platform={platform} (will use {platform} for image name)")
        ret = platform

    return ret


def get_grid_image(platform: str) -> str:
    """
    Return the full path to the singularity/apptainer grid image.

    :param platform: E.g. "x86_64-slc6" (str)
    :return: full path to grid image (str).
    """
    if not platform or platform == "":
        platform = "x86_64-slc6"
        logger.warning(f"using default platform={platform} (cmtconfig not set)")

    arch_and_os = extract_platform_and_os(platform)
    image = arch_and_os + ".img"
    _path1 = os.path.join(get_file_system_root_path(), "atlas.cern.ch/repo/containers/images/apptainer")
    _path2 = os.path.join(get_file_system_root_path(), "atlas.cern.ch/repo/containers/images/singularity")
    paths = tuple(path for path in (_path1, _path2) if os.path.isdir(path))
    _path = paths[0]
    path = os.path.join(_path, image)
    if not os.path.exists(path):
        image = 'x86_64-centos7.img'
        logger.warning(f'path does not exist: {path} (trying with image {image} instead)')
        path = os.path.join(_path, image)
        if not os.path.exists(path):
            logger.warning(f'path does not exist either: {path}')
            path = ""

    return path


def get_middleware_type() -> str:
    """
    Return the middleware type from the container type.

    E.g. container_type = 'singularity:pilot;docker:wrapper;container:middleware'
    get_middleware_type() -> 'container', meaning that middleware should be taken from the container. The default
    is otherwise 'workernode', i.e. middleware is assumed to be present on the worker node.

    :return: middleware_type (str).
    """
    middleware_type = ""
    container_type = infosys.queuedata.container_type

    middleware = 'middleware'
    if container_type and container_type != "" and middleware in container_type:
        try:
            container_names = container_type.split(';')
            for name in container_names:
                _split = name.split(':')
                if middleware == _split[0]:
                    middleware_type = _split[1]
        except IndexError as exc:
            logger.warning(f"failed to parse the container name: {container_type}, {exc}")
    else:
        # logger.warning("container middleware type not specified in queuedata")
        # no middleware type was specified, assume that middleware is present on worker node
        middleware_type = "workernode"

    return middleware_type


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
        # export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
        #   source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
        cleaned_atlas_setup = asetup.replace(atlas_setup, '').replace(';;', ';')
        atlas_setup = atlas_setup.replace('source ', '')
    except AttributeError as exc:
        logger.debug(f'exception caught while extracting asetup command: {exc}')
        atlas_setup = ''
        cleaned_atlas_setup = ''

    return atlas_setup, cleaned_atlas_setup


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


def update_alrb_setup(cmd: str, use_release_setup: str) -> str:
    """
    Update the ALRB setup command.

    Add the ALRB_CONT_SETUPFILE in case the release setup file was created earlier (required available cvmfs).

    :param cmd: full ALRB setup command (string).
    :param use_release_setup: should the release setup file be added to the setup command? (Boolean).
    :return: updated ALRB setup command (string).
    """
    updated_cmds = []
    try:
        _cmd = cmd.split(';')
        for subcmd in _cmd:
            if subcmd.startswith('source ${ATLAS_LOCAL_ROOT_BASE}') and use_release_setup:
                updated_cmds.append(f'export ALRB_CONT_SETUPFILE="/srv/{config.Container.release_setup}"')
            updated_cmds.append(subcmd)
        updated_cmd = ';'.join(updated_cmds)
    except AttributeError as exc:
        logger.warning(f'exception caught while extracting full atlas setup: {exc}')
        updated_cmd = cmd

    return updated_cmd


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

        # download and verify payload proxy from the server if desired
        proxy_verification = os.environ.get('PILOT_PROXY_VERIFICATION') == 'True' and os.environ.get('PILOT_PAYLOAD_PROXY_VERIFICATION') == 'True'
        if proxy_verification and config.Pilot.payload_proxy_from_server and is_analysis and queue_type != 'unified':
            voms_role = get_voms_role(role='user')
            exit_code, diagnostics, x509 = get_and_verify_proxy(x509, voms_role=voms_role, proxy_type='payload')
            if exit_code != 0:  # do not return non-zero exit code if only download fails
                logger.warning('payload proxy verification failed')

        # add X509_USER_PROXY setting to the container setup command
        setup_cmd = f"export X509_USER_PROXY={x509};" + setup_cmd

    return exit_code, diagnostics, setup_cmd, cmd


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


def alrb_wrapper(cmd: str, workdir: str, job: JobData = None) -> str:
    """
    Wrap the given command with the special ALRB setup for containers
    E.g. cmd = /bin/bash hello_world.sh
    ->
    export thePlatform="x86_64-slc6-gcc48-opt el9"
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
        alrb_setup += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh '
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


def container_wrapper(cmd: str, workdir: str, job: JobData = None) -> str:
    """
    Prepend the given command with the singularity/apptainer execution command.

    E.g. cmd = /bin/bash hello_world.sh
    -> singularity_command = singularity exec -B <bindmountsfromcatchall> <img> /bin/bash hello_world.sh
    singularity exec -B <bindmountsfromcatchall>  /cvmfs/atlas.cern.ch/repo/images/singularity/x86_64-slc6.img <script>
    Note: if the job object is not set, then it is assumed that the middleware container is to be used.
    Note 2: if apptainer is specified in CRIC in the container type, it is assumes that the executable is called
    apptainer.

    :param cmd: command to be prepended (str)
    :param workdir: explicit work directory where the command should be executed (needs to be set for Singularity) (str)
    :param job: job object (JobData)
    :return: prepended command with singularity execution command (str).
    """
    if job:
        queuedata = job.infosys.queuedata
    else:
        infoservice = InfoService()
        infoservice.init(os.environ.get('PILOT_SITENAME'), infosys.confinfo, infosys.extinfo)
        queuedata = infoservice.queuedata

    container_name = queuedata.container_type.get("pilot")  # resolve container name for user=pilot
    if container_name in {'singularity', 'apptainer'}:
        logger.info("singularity/apptainer has been requested")

        # Get the container options
        options = queuedata.container_options
        if options != "":
            options += ","
        else:
            options = "-B "
        options += "/cvmfs,${workdir},/home"
        logger.debug(f"using options: {options}")

        # Get the image path
        if job:
            image_path = job.imagename or get_grid_image(job.platform)
        else:
            image_path = config.Container.middleware_container

        # Does the image exist?
        if image_path:
            # Prepend it to the given command
            quote = shlex.quote(f'cd $workdir;pwd;{cmd}')
            cmd = f"export workdir={workdir}; {container_name} --verbose exec {options} {image_path} " \
                  f"/bin/bash -c {quote}"

            # for testing user containers
            # singularity_options = "-B $PWD:/data --pwd / "
            # singularity_cmd = "singularity exec " + singularity_options + image_path
            # cmd = re.sub(r'-p "([A-Za-z0-9.%/]+)"', r'-p "%s\1"' % urllib.pathname2url(singularity_cmd), cmd)
        else:
            logger.warning("singularity/apptainer options found but image does not exist")

        logger.info(f"updated command: {cmd}")

    return cmd


def create_root_container_command(workdir: str, cmd: str, script: str) -> str:
    """
    Create the container command for root.

    :param workdir: workdir (str)
    :param cmd: command to be containerised (str)
    :param script: script content (str)
    :return: container command to be executed (str).
    """
    command = f'cd {workdir};'
    # parse the 'open_file.sh' script
    content = get_root_container_script(cmd, script)
    script_name = 'open_file.sh'
    logger.info(f'{script_name}:\n\n{content}\n\n')
    try:
        # overwrite the 'open_file.sh' script with updated information
        status = write_file(os.path.join(workdir, script_name), content)
    except PilotException as exc:
        raise exc

    if status:
        # generate the final container command
        x509 = os.environ.get('X509_UNIFIED_DISPATCH', os.environ.get('X509_USER_PROXY', ''))
        if x509:
            command += f'export X509_USER_PROXY={x509};'
        command += f'export ALRB_CONT_RUNPAYLOAD="source /srv/{script_name}";'
        _asetup = get_asetup(alrb=True)  # export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
        _asetup = fix_asetup(_asetup)
        command += _asetup
        command += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c el9'

    logger.debug(f'container command: {command}')

    return command


def execute_remote_file_open(path: str, python_script_timeout: int) -> tuple[int, str, int]:  # noqa: C901
    """
    Execute the remote file open script.

    :param path: path to container script (str)
    :param python_script_timeout: timeout (int)
    :return: exit code (int), stdout (str), lsetup time (int) (tuple).
    """
    lsetup_timeout = 600  # Timeout for 'lsetup' step
    exit_code = 1
    stdout = ""

    # Start the Bash script process with non-blocking I/O
    try:
        process = subprocess.Popen(["bash", path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        fcntl.fcntl(process.stdout.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)  # Set non-blocking
    except OSError as e:
        logger.warning(f"error starting subprocess: {e}")
        return exit_code, "", 0

    # Split the path at the last dot
    filename, _ = path.rsplit(".", 1)

    # Create the new path with the desired suffix
    new_path = f"{filename}.stdout"

    start_time = time.time()  # Track execution start time
    lsetup_start_time = start_time
    lsetup_completed = False  # Flag to track completion of 'lsetup' process
    python_completed = False  # Flag to track completion of 'python3' process
    lsetup_completed_at = None

    with open(new_path, "w", encoding='utf-8') as file:
        while True:
            # Check for timeout (once per second)
            if time.time() - start_time > lsetup_timeout and not lsetup_completed:
                logger.warning("timeout for 'lsetup' exceeded - killing script")
                exit_code = 2  # 'lsetup' timeout
                process.kill()
                break

            # Use select to check if there is data to read (to byspass any blocking operation that will prevent time-out checks)
            ready, _, _ = select.select([process.stdout], [], [], 1.0)
            if ready:
                output = process.stdout.readline()  # Read bytes directly
                if output:
                    output = output.decode().strip()
                    file.write(output + "\n")
                    stdout += output + "\n"

                    # Check for LSETUP_COMPLETED message
                    if output == "LSETUP_COMPLETED":
                        lsetup_completed = True
                        lsetup_completed_at = time.time()
                        start_time = time.time()  # Reset start time for 'python3' timeout

                    # Check for PYTHON_COMPLETED message
                    if "PYTHON_COMPLETED" in output:
                        python_completed = True
                        match = re.search(r"\d+$", output)
                        if match:
                            exit_code = int(match.group())
                            logger.info(f"python remote open command has completed with exit code {exit_code}")
                        else:
                            logger.info("python remote open command has completed without any exit code")
                        break

            # Timeout for python script after LSETUP_COMPLETED
            if lsetup_completed and ((time.time() - lsetup_completed_at) > python_script_timeout):
                logger.warning(f"(1) timeout for 'python3' subscript exceeded - killing script "
                               f"({time.time()} - {lsetup_completed_at} > {python_script_timeout})")
                exit_code = 3
                process.kill()
                break

            # Timeout for python script after LSETUP_COMPLETED
            if lsetup_completed and ((time.time() - start_time) > python_script_timeout):
                logger.warning(f"(2) timeout for 'python3' subscript exceeded - killing script "
                               f"({time.time()} - {start_time} > {python_script_timeout})")
                exit_code = 3
                process.kill()
                break

            if python_completed:
                logger.info('aborting since python command has finished')
                return_code = process.poll()
                if return_code:
                    logger.warning(f"script execution completed with return code: {return_code}")
                    # exit_code = return_code
                break

            # Check if script has completed normally
            return_code = process.poll()
            if return_code is not None:
                pass
            #    logger.info(f"script execution completed with return code: {return_code}")
            #    exit_code = return_code
            #    break

            time.sleep(0.5)

    # Ensure process is terminated
    if process.poll() is None:
        process.terminate()

    # Check if 'lsetup' was completed
    lsetup_time = int(lsetup_completed_at - lsetup_start_time) if lsetup_completed_at else 0

    return exit_code, stdout, lsetup_time


def execute_remote_file_open_old(path: str, python_script_timeout: int) -> tuple[int, str, int]:  # noqa: C901
    """
    Execute the remote file open script.

    :param path: path to container script (str)
    :param python_script_timeout: timeout (int)
    :return: exit code (int), stdout (str), lsetup time (int) (tuple).
    """
    lsetup_timeout = 600  # Timeout for 'lsetup' step
    exit_code = 1
    stdout = ""

    # Start the Bash script process with non-blocking I/O
    try:
        process = subprocess.Popen(["bash", path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=0)
        fcntl.fcntl(process.stdout.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)  # Set non-blocking
    except OSError as e:
        logger.warning(f"error starting subprocess: {e}")
        return exit_code, "", 0

    # Split the path at the last dot
    filename, _ = path.rsplit(".", 1)

    # Create the new path with the desired suffix
    new_path = f"{filename}.stdout"

    start_time = time.time()  # Track execution start time
    lsetup_start_time = start_time
    lsetup_completed = False  # Flag to track completion of 'lsetup' process
    python_completed = False  # Flag to track completion of 'python3' process
    lsetup_completed_at = None

    with open(new_path, "w", encoding='utf-8') as file:
        while True:
            # Check for timeout (once per second)
            if time.time() - start_time > lsetup_timeout and not lsetup_completed:
                logger.warning("timeout for 'lsetup' exceeded - killing script")
                exit_code = 2  # 'lsetup' timeout
                process.kill()
                break

            # Try to read output without blocking (might return None)
            try:
                output = process.stdout.readline()  # Read bytes directly
                if output is not None:  # Check if any output is available (not None)
                    output = output.decode().strip()
                    if output:
                        file.write(output + "\n")
                        # logger.info(f'remote file open: {output}')

                    # Check for LSETUP_COMPLETED message
                    if output == "LSETUP_COMPLETED":
                        logger.info('lsetup has completed (resetting start time)')
                        lsetup_completed = True
                        lsetup_completed_at = time.time()
                        start_time = time.time()  # Reset start time for 'python3' timeout

                    # Check for LSETUP_COMPLETED message
                    if "PYTHON_COMPLETED" in output:
                        python_completed = True
                        match = re.search(r"\d+$", output)
                        if match:
                            exit_code = int(match.group())
                            logger.info(f"python remote open command has completed with exit code {exit_code}")
                        else:
                            logger.info("python remote open command has completed without any exit code")

                    stdout += output + "\n"
            except BlockingIOError:
                time.sleep(0.1)  # No output available yet, continue the loop
                continue
            except (OSError, ValueError):  # Catch potential errors from process.stdout
                #        print(f"Error reading from subprocess output: {e}")
                #        # Handle the error (e.g., log it, retry, exit)
                #        break
                time.sleep(0.1)
                continue

            # Timeout for python script after LSETUP_COMPLETED
            if lsetup_completed and ((time.time() - start_time) > python_script_timeout):
                logger.warning(f"timeout for 'python3' subscript exceeded - killing script "
                               f"({time.time()} - {start_time} > {python_script_timeout})")
                exit_code = 3  # python script timeout
                process.kill()
                break

            if python_completed:
                logger.info('aborting since python command has finished')
                return_code = process.poll()
                if return_code:
                    logger.warning(f"script execution completed with return code: {return_code}")
                    # exit_code = return_code
                break

            # Check if script has completed normally
            return_code = process.poll()
            if return_code is not None:
                pass
            #    logger.info(f"script execution completed with return code: {return_code}")
            #    exit_code = return_code
            #    break

            time.sleep(0.5)

    # Ensure process is terminated
    if process.poll() is None:
        process.terminate()

    # Check if 'lsetup' was completed
    lsetup_time = int(lsetup_completed_at - lsetup_start_time) if lsetup_completed_at else 0

    return exit_code, stdout, lsetup_time


def fix_asetup(asetup: str) -> str:
    """
    Make sure that the command returned by get_asetup() contains a trailing ;-sign.

    :param asetup: asetup (str)
    :return: updated asetup (str).
    """
    if asetup and not asetup.strip().endswith(';'):
        asetup += '; '

    return asetup


def create_middleware_container_command(job: JobData, cmd: str, label: str = 'stage-in', proxy: bool = True) -> str:
    """
    Create the container command for stage-in/out or other middleware.

    The function takes the isolated middleware command, adds bits and pieces needed for the containerisation and stores
    it in a script file. It then generates the actual command that will execute the middleware script in a
    container.

    new cmd:
      lsetup rucio davis xrootd
      old cmd
      exit $?
    write new cmd to stage[in|out].sh script
    create container command and return it

    :param job: job object (JobData)
    :param cmd: command to be containerised (str)
    :param label: 'stage-[in|out]|setup' (str)
    :param proxy: add proxy export command (bool)
    :return: container command to be executed (str).
    """
    command = f'cd {job.workdir};'

    # add bits and pieces for the containerisation
    middleware_container = get_middleware_container(label=label)
    content = get_middleware_container_script(middleware_container, cmd, label=label)

    # store it in setup.sh
    if label == 'stage-in':
        script_name = 'stagein.sh'
    elif label == 'stage-out':
        script_name = 'stageout.sh'
    else:
        script_name = 'general.sh'

    # for setup container
    container_script_name = 'container_script.sh'
    try:
        logger.debug(f'command to be written to container setup file \n\n{script_name}:\n\n{content}\n')
        status = write_file(os.path.join(job.workdir, script_name), content)
        if status:
            content = 'echo \"Done\"'
            logger.debug(f'command to be written to container command file \n\n{container_script_name}:\n\n{content}\n')
            status = write_file(os.path.join(job.workdir, container_script_name), content)
    except PilotException as exc:
        raise exc

    if status:
        # generate the final container command
        if proxy:
            x509 = os.environ.get('X509_USER_PROXY', '')
            if x509:
                command += f'export X509_USER_PROXY={x509};'
        if label != 'setup':  # only for stage-in/out; for setup verification, use -s .. -r .. below
            command += f'export ALRB_CONT_RUNPAYLOAD="source /srv/{script_name}";'
            if 'ALRB_CONT_UNPACKEDDIR' in os.environ:
                command += f"export ALRB_CONT_UNPACKEDDIR={os.environ.get('ALRB_CONT_UNPACKEDDIR')};"
        command += fix_asetup(get_asetup(alrb=True))  # export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;
        if label == 'setup':
            # set the platform info
            command += f'export thePlatform="{job.platform}";'
        command += f'source ${{ATLAS_LOCAL_ROOT_BASE}}/user/atlasLocalSetup.sh -c '  # noqa: F541. pylint: disable=W1309
        if middleware_container:
            command += f'{middleware_container}'
        elif label in {'stage-in', 'stage-out'}:
            command += 'el9 '
        if label == 'setup':
            command += f' -s /srv/{script_name} -r /srv/{container_script_name}'
        else:
            command += ' ' + get_container_options(job.infosys.queuedata.container_options)
        command = command.replace('  ', ' ')

    logger.debug(f'container command: {command}')

    return command


def get_root_container_script(cmd: str, script: str) -> str:
    """
    Return the content of the root container script.

    :param cmd: root command (str)
    :param script: script content (str)
    :return: script content (str).
    """
    # content = f'date\nexport XRD_LOGLEVEL=Debug\nlsetup \'root pilot-default\'\ndate\nstdbuf -oL bash -c \"python3 {cmd}\"\nexit $?'
    return script.replace('REPLACE_ME_FOR_CMD', cmd)


def get_middleware_container_script(middleware_container: str, cmd: str, asetup: bool = False, label: str = '') -> str:
    """
    Return the content of the middleware container script.

    If asetup is True, atlasLocalSetup will be added to the command.

    :param middleware_container: container image (str)
    :param cmd: isolated stage-in/out command (str)
    :param asetup: optional True/False (bool)
    :param label: optional label (str)
    :return: script content (str).
    """
    sitename = f"export PILOT_RUCIO_SITENAME={os.environ.get('PILOT_RUCIO_SITENAME')}; "
    if label == 'setup':
        # source $AtlasSetup/scripts/asetup.sh AtlasOffline,21.0.16,notest --platform x86_64-slc6-gcc49-opt --makeflags='$MAKEFLAGS'
        content = cmd[cmd.find('source $AtlasSetup'):]
    elif 'rucio' in middleware_container:
        content = sitename
        #content += f'export ATLAS_LOCAL_ROOT_BASE={get_file_system_root_path()}/atlas.cern.ch/repo/ATLASLocalRootBase; '
        #content += "alias setupATLAS=\'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh\'; "
        #content += "setupATLAS -3; "
        rucio_version = get_rucio_client_version()
        if rucio_version:
            content += f'export ATLAS_LOCAL_RUCIOCLIENTS_VERSION={rucio_version}; '
        content += f'lsetup "python pilot-default";python3 {cmd} '
    else:
        content = 'export ALRB_LOCAL_PY3=YES; '
        if asetup:  # export ATLAS_LOCAL_ROOT_BASE=/cvmfs/..;source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;
            _asetup = get_asetup(asetup=False)
            _asetup = fix_asetup(_asetup)
            content += _asetup
        if label in {'stage-in', 'stage-out'}:
            content += sitename + 'lsetup rucio davix xrootd; '
            content += f'python3 {cmd} '
        else:
            content += cmd
    if not asetup:
        content += '\nexit $?'

    logger.debug(f'middleware container content:\n{content}')

    return content


def get_middleware_container(label: str = None) -> str:
    """
    Return the middleware container.

    :param label: label (str)
    :return: path (str).
    """
    if label and label == 'general':
        return 'el9'  #'CentOS7'

    if label == 'setup':
        path = '$thePlatform'
    elif 'ALRB_CONT_UNPACKEDDIR' in os.environ:
        path = config.Container.middleware_container_no_path
    else:
        path = config.Container.middleware_container
        if path.startswith('/') and not os.path.exists(path):
            logger.warning(f'requested middleware container path does not exist: {path} (switching to default value)')
            path = 'el9'  #'CentOS7'

    if not path:
        path = 'el9'  #'CentOS7'  # default value
    logger.info(f'using image: {path} for middleware container')

    return path


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


def verify_container_script(path: str):
    """
    Remove any sensitive token info from the container_script.sh if present.

    :param path: path to container script (str).
    """
    if os.path.exists(path):
        url_pattern = r'docker\ login'  # docker login <registry> -u <username> -p <token>
        lines = grep([url_pattern], path)
        if lines:
            has_token = has_docker_pattern(lines[0], pattern=url_pattern)
            if has_token:
                logger.warning(f'found sensitive token information in {path} - removing file')
                remove(path)
            else:
                logger.debug(f'no sensitive information in {path}')
        else:
            logger.debug(f'no sensitive information in {path}')
