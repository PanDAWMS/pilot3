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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025

"""Job setup and environment preparation for the ePIC experiment plugin."""

from __future__ import annotations
import glob
import logging
import os
import re
from time import sleep
from datetime import datetime

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    #FileHandlingFailure,
    NoSoftwareDir,
    #NoSuchFile
)
from pilot.info import (
    infosys,
    #JobData
)
from pilot.util.auxiliary import find_pattern_in_list
from pilot.util.container import execute
from pilot.util.filehandling import (
    copy,
    head,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def get_analysis_trf(transform: str, workdir: str, base_urls: list) -> tuple[int, str, str]:
    """Prepare to download the user analysis transform with curl.

    The function will verify the download location from a known list of hosts.

    Args:
        transform: full trf path (url).
        workdir: work directory.
        base_urls: list of base urls.

    Returns:
        tuple[int, str, str]: exit code, diagnostics, transform_name.
    """
    ec = 0
    diagnostics = ""

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        search_pattern = f"{harvester_workdir}/jobO.*.tar.gz"
        logger.debug(f"search_pattern - {search_pattern}")
        jobopt_files = glob.glob(search_pattern)
        for jobopt_file in jobopt_files:
            logger.debug(f"jobopt_file = {jobopt_file} workdir = {workdir}")
            try:
                copy(jobopt_file, workdir)
            except Exception as exc:
                logger.error(f"could not copy file {jobopt_file} to {workdir} : {exc}")

    if '/' in transform:
        transform_name = transform.split('/')[-1]
    else:
        logger.warning(f'did not detect any / in {transform} (using full transform name)')
        transform_name = transform

    # is the command already available? (e.g. if already downloaded by a preprocess/main process step)
    if os.path.exists(os.path.join(workdir, transform_name)):
        logger.info(f'script {transform_name} is already available - no need to download again')
        return ec, diagnostics, transform_name

    original_base_url = ""

    # verify the base URL
    for base_url in get_valid_base_urls(base_urls):
        if transform.startswith(base_url):
            original_base_url = base_url
            break

    if original_base_url == "":
        diagnostics = f"invalid base URL: {transform}"
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    # try to download from the required location, if not - switch to backup
    status = False
    for base_url in get_valid_base_urls(base_urls, order=original_base_url):
        trf = re.sub(original_base_url, base_url, transform)
        logger.debug(f"attempting to download script: {trf}")
        status, diagnostics = download_transform(trf, transform_name, workdir)
        if status:
            break

    if not status:
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    logger.info("successfully downloaded script")
    path = os.path.join(workdir, transform_name)
    logger.debug(f"changing permission of {path} to 0o755")
    try:
        os.chmod(path, 0o755)  # Python 2/3
    except Exception as exc:
        diagnostics = f"failed to chmod {transform_name}: {exc}"
        return errors.CHMODTRF, diagnostics, ""

    return ec, diagnostics, transform_name


def get_valid_base_urls(base_urls: list, order: str = None) -> list:
    """Return a list of valid base URLs from where the user analysis transform may be downloaded from.

    If order is defined, return given item first.
    E.g. order=http://atlpan.web.cern.ch/atlpan -> ['http://atlpan.web.cern.ch/atlpan', ...]
    NOTE: the URL list may be out of date.

    Args:
        base_urls: list of base URLs.
        order: order.

    Returns:
        list: valid base URLs.
    """
    valid_base_urls = []
    if not base_urls:
        base_urls = ["https://storage.googleapis.com/drp-us-central1-containers",
                     "https://pandaserver-doma.cern.ch/trf/user"]

    for base_url in base_urls:
        if not base_url.startswith(("http://", "https://")):
            valid_base_urls.append(f"http://{base_url}")
            valid_base_urls.append(f"https://{base_url}")
        else:
            valid_base_urls.append(base_url)

    if order:
        valid_base_urls = [order] + [url for url in valid_base_urls if url != order]

    return valid_base_urls


def download_transform(url: str, transform_name: str, workdir: str) -> tuple[bool, str]:
    """Download the transform from the given url.

    Args:
        url: download URL with path to transform.
        transform_name: trf name.
        workdir: work directory.

    Returns:
        tuple[bool, str]: status, diagnostics.
    """
    status = False
    diagnostics = ""
    path = os.path.join(workdir, transform_name)
    cmd = f'curl -sS "{url}" > {path}'
    trial = 1
    max_trials = 3

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        # skip curl by setting max_trials = 0
        max_trials = 0
        source_path = os.path.join(harvester_workdir, transform_name)
        try:
            copy(source_path, path)
            status = True
        except Exception as error:
            status = False
            diagnostics = f"Failed to copy file {source_path} to {path} : {error}"
            logger.error(diagnostics)

    # try to download the trf a maximum of 3 times
    while trial <= max_trials:
        logger.info(f"executing command [trial {trial}/{max_trials}]: {cmd}")

        exit_code, stdout, stderr = execute(cmd, mute=True)
        if not stdout:
            stdout = "(None)"
        if exit_code != 0:
            # Analyze exit code / output
            diagnostics = f"curl command failed: {exit_code}, {stdout}, {stderr}"
            logger.warning(diagnostics)
            if trial == max_trials:
                logger.fatal(f'could not download transform: {stdout}')
                status = False
                break
            else:
                logger.info("will try again after 60 s")
                sleep(60)
        else:
            logger.info(f"curl command returned: {stdout}")
            status = True
            break
        trial += 1

    return status, diagnostics


def get_end_setup_time(path: str, pattern: str = r'(\d{2}\:\d{2}\:\d{2}\ \d{4}\/\d{2}\/\d{2})') -> float:
    """Extract a more precise end of setup time from the payload stdout.

    File path should be verified already.
    The function will look for a date time in the beginning of the payload stdout with the given pattern.

    Args:
        path: path to payload stdout.
        pattern: regular expression pattern.

    Returns:
        float: time in seconds since epoch.
    """
    end_time = None
    head_list = head(path, count=50)
    time_string = find_pattern_in_list(head_list, pattern)
    if time_string:
        logger.debug(f"extracted time string=\'{time_string}\' from file \'{path}\'")
        end_time = datetime.strptime(time_string, '%H:%M:%S %Y/%m/%d').timestamp()  # since epoch

    return end_time


def get_schedconfig_priority() -> list:
    """Return the prioritized list for the schedconfig sources.

    This list is used to determine which source to use for the queuedatas, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_queuedata() (minimal set) and
    load_schedconfig_data() (full set).

    Returns:
        list: prioritized DDM source list.
    """
    return ['LOCAL', 'CVMFS', 'CRIC', 'PANDA']


def get_queuedata_priority() -> list:
    """Return the prioritized list for the schedconfig sources.

    This list is used to determine which source to use for the queuedatas, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_queuedata() (minimal set) and
    load_schedconfig_data() (full set).

    Returns:
        list: prioritized DDM source list.
    """
    return ['LOCAL', 'PANDA', 'CVMFS', 'CRIC']


def get_ddm_source_priority() -> list:
    """Return the prioritized list for the DDM sources.

    This list is used to determine which source to use for the DDM endpoints, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_storage_data().

    Returns:
        list: prioritized DDM source list.
    """
    return ['USER', 'LOCAL', 'CVMFS', 'CRIC', 'PANDA']


def should_verify_setup(job: object) -> bool:
    """Should the setup command be verified?

    Args:
        job: job object.

    Returns:
        bool: False.
    """
    return False


def get_file_system_root_path() -> str:
    """Return the root path of the local file system.

    The function returns "/cvmfs" or "/(some path)/cvmfs" in case the expected file system root path is not
    where it usually is (e.g. on an HPC). A site can set the base path by exporting ATLAS_SW_BASE.

    Returns:
        str: path.
    """
    return os.environ.get('ATLAS_SW_BASE', '/cvmfs')


def get_alrb_export(add_if: bool = False) -> str:
    """Return the export command for the ALRB path if it exists.

    If the path does not exist, return empty string.

    Args:
        add_if: True means that an if statement will be placed around the export.

    Returns:
        str: export command.
    """
    path = f"{get_file_system_root_path()}/atlas.cern.ch/repo"
    cmd = f"export ATLAS_LOCAL_ROOT_BASE={path}/ATLASLocalRootBase;" if os.path.exists(path) else ""

    # if [ -z "$ATLAS_LOCAL_ROOT_BASE" ]; then export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; fi;
    if cmd and add_if:
        cmd = 'if [ -z \"$ATLAS_LOCAL_ROOT_BASE\" ]; then ' + cmd + ' fi;'

    return cmd


def get_asetup(asetup: bool = True, alrb: bool = False, add_if: bool = False) -> str:
    """Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE.

    Only include the actual asetup script if asetup=True. This is not needed if the jobPars contain the payload command
    but the pilot still needs to add the exports and the atlasLocalSetup.

    Args:
        asetup: True value means that the pilot should include the asetup command.
        alrb: True value means that the function should return special setup used with ALRB and containers.
        add_if: True means that an if statement will be placed around the export.

    Returns:
        str: source <path>/asetup.sh.

    Raises:
        NoSoftwareDir: if appdir does not exist.
    """
    cmd = ""

    alrb_cmd = get_alrb_export(add_if=add_if)
    if alrb_cmd != "":
        cmd = alrb_cmd
        if not alrb:
            cmd += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --quiet;"
            if asetup:
                cmd += "source $AtlasSetup/scripts/asetup.sh"
    else:
        try:  # use try in case infosys has not been initiated
            appdir = infosys.queuedata.appdir
        except Exception:
            appdir = ""
        if appdir == "":
            appdir = os.environ.get('VO_ATLAS_SW_DIR', '')
        if appdir != "":
            # make sure that the appdir exists
            if not os.path.exists(appdir):
                msg = f'appdir does not exist: {appdir}'
                logger.warning(msg)
                raise NoSoftwareDir(msg)
            if asetup:
                cmd = f"source {appdir}/scripts/asetup.sh"

    return cmd
