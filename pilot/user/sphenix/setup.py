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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-23

import os
import re
import glob
from time import sleep
from datetime import datetime

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import NoSoftwareDir
from pilot.info import infosys
from pilot.util.auxiliary import find_pattern_in_list
from pilot.util.container import execute
from pilot.util.filehandling import copy, head

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def get_file_system_root_path():
    """
    Return the root path of the local file system.
    The function returns "/cvmfs" or "/(some path)/cvmfs" in case the expected file system root path is not
    where it usually is (e.g. on an HPC). A site can set the base path by exporting ATLAS_SW_BASE.

    :return: path (string)
    """

    return os.environ.get('ATLAS_SW_BASE', '/cvmfs')


def get_alrb_export(add_if=False):
    """
    Return the export command for the ALRB path if it exists.
    If the path does not exist, return empty string.

    :param add_if: Boolean. True means that an if statement will be placed around the export.
    :return: export command
    """

    path = "%s/atlas.cern.ch/repo" % get_file_system_root_path()
    cmd = "export ATLAS_LOCAL_ROOT_BASE=%s/ATLASLocalRootBase;" % path if os.path.exists(path) else ""

    # if [ -z "$ATLAS_LOCAL_ROOT_BASE" ]; then export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; fi;
    if cmd and add_if:
        cmd = 'if [ -z \"$ATLAS_LOCAL_ROOT_BASE\" ]; then ' + cmd + ' fi;'

    return cmd


def get_asetup(asetup=True, alrb=False, add_if=False):
    """
    Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE
    Only include the actual asetup script if asetup=True. This is not needed if the jobPars contain the payload command
    but the pilot still needs to add the exports and the atlasLocalSetup.

    :param asetup: Boolean. True value means that the pilot should include the asetup command.
    :param alrb: Boolean. True value means that the function should return special setup used with ALRB and containers.
    :param add_if: Boolean. True means that an if statement will be placed around the export.
    :raises: NoSoftwareDir if appdir does not exist.
    :return: source <path>/asetup.sh (string).
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
                msg = 'appdir does not exist: %s' % appdir
                logger.warning(msg)
                raise NoSoftwareDir(msg)
            if asetup:
                cmd = "source %s/scripts/asetup.sh" % appdir

    return cmd


def get_analysis_trf(transform, workdir):
    """
    Prepare to download the user analysis transform with curl.
    The function will verify the download location from a known list of hosts.

    :param transform: full trf path (url) (string).
    :param workdir: work directory (string).
    :return: exit code (int), diagnostics (string), transform_name (string)
    """

    ec = 0
    diagnostics = ""

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        search_pattern = "%s/jobO.*.tar.gz" % harvester_workdir
        logger.debug("search_pattern - %s" % search_pattern)
        jobopt_files = glob.glob(search_pattern)
        for jobopt_file in jobopt_files:
            logger.debug("jobopt_file = %s workdir = %s" % (jobopt_file, workdir))
            try:
                copy(jobopt_file, workdir)
            except Exception as e:
                logger.error("could not copy file %s to %s : %s" % (jobopt_file, workdir, e))

    if '/' in transform:
        transform_name = transform.split('/')[-1]
    else:
        logger.warning('did not detect any / in %s (using full transform name)' % transform)
        transform_name = transform

    # is the command already available? (e.g. if already downloaded by a preprocess/main process step)
    if os.path.exists(os.path.join(workdir, transform_name)):
        logger.info('script %s is already available - no need to download again' % transform_name)
        return ec, diagnostics, transform_name

    original_base_url = ""

    # verify the base URL
    for base_url in get_valid_base_urls():
        if transform.startswith(base_url):
            original_base_url = base_url
            break

    if original_base_url == "":
        diagnostics = "invalid base URL: %s" % transform
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    # try to download from the required location, if not - switch to backup
    status = False
    for base_url in get_valid_base_urls(order=original_base_url):
        trf = re.sub(original_base_url, base_url, transform)
        logger.debug("attempting to download script: %s" % trf)
        status, diagnostics = download_transform(trf, transform_name, workdir)
        if status:
            break

    if not status:
        return errors.TRFDOWNLOADFAILURE, diagnostics, ""

    logger.info("successfully downloaded script")
    path = os.path.join(workdir, transform_name)
    logger.debug("changing permission of %s to 0o755" % path)
    try:
        os.chmod(path, 0o755)  # Python 2/3
    except Exception as e:
        diagnostics = "failed to chmod %s: %s" % (transform_name, e)
        return errors.CHMODTRF, diagnostics, ""

    return ec, diagnostics, transform_name


def get_valid_base_urls(order=None):
    """
    Return a list of valid base URLs from where the user analysis transform may be downloaded from.
    If order is defined, return given item first.
    E.g. order=http://atlpan.web.cern.ch/atlpan -> ['http://atlpan.web.cern.ch/atlpan', ...]
    NOTE: the URL list may be out of date.

    :param order: order (string).
    :return: valid base URLs (list).
    """

    valid_base_urls = []
    _valid_base_urls = ["https://storage.googleapis.com/drp-us-central1-containers",
                        "http://pandaserver-doma.cern.ch:25080/trf/user"]

    if order:
        valid_base_urls.append(order)
        for url in _valid_base_urls:
            if url != order:
                valid_base_urls.append(url)
    else:
        valid_base_urls = _valid_base_urls

    return valid_base_urls


def download_transform(url, transform_name, workdir):
    """
    Download the transform from the given url
    :param url: download URL with path to transform (string).
    :param transform_name: trf name (string).
    :param workdir: work directory (string).
    :return:
    """

    status = False
    diagnostics = ""
    path = os.path.join(workdir, transform_name)
    cmd = 'curl -sS \"%s\" > %s' % (url, path)
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
            diagnostics = "Failed to copy file %s to %s : %s" % (source_path, path, error)
            logger.error(diagnostics)

    # try to download the trf a maximum of 3 times
    while trial <= max_trials:
        logger.info("executing command [trial %d/%d]: %s" % (trial, max_trials, cmd))

        exit_code, stdout, stderr = execute(cmd, mute=True)
        if not stdout:
            stdout = "(None)"
        if exit_code != 0:
            # Analyze exit code / output
            diagnostics = "curl command failed: %d, %s, %s" % (exit_code, stdout, stderr)
            logger.warning(diagnostics)
            if trial == max_trials:
                logger.fatal('could not download transform: %s' % stdout)
                status = False
                break
            else:
                logger.info("will try again after 60 s")
                sleep(60)
        else:
            logger.info("curl command returned: %s" % stdout)
            status = True
            break
        trial += 1

    return status, diagnostics


def get_end_setup_time(path, pattern=r'(\d{2}\:\d{2}\:\d{2}\ \d{4}\/\d{2}\/\d{2})'):
    """
    Extract a more precise end of setup time from the payload stdout.
    File path should be verified already.
    The function will look for a date time in the beginning of the payload stdout with the given pattern.

    :param path: path to payload stdout (string).
    :param pattern: regular expression pattern (raw string).
    :return: time in seconds since epoch (float).
    """

    end_time = None
    head_list = head(path, count=50)
    time_string = find_pattern_in_list(head_list, pattern)
    if time_string:
        logger.debug(f"extracted time string=\'{time_string}\' from file \'{path}\'")
        end_time = datetime.strptime(time_string, '%H:%M:%S %Y/%m/%d').timestamp()  # since epoch

    return end_time


def get_schedconfig_priority():
    """
    Return the prioritized list for the schedconfig sources.
    This list is used to determine which source to use for the queuedatas, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_queuedata() (minimal set) and
    load_schedconfig_data() (full set).

    :return: prioritized DDM source list.
    """

    return ['LOCAL', 'CVMFS', 'CRIC', 'PANDA']


def get_queuedata_priority():
    """
    Return the prioritized list for the schedconfig sources.
    This list is used to determine which source to use for the queuedatas, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_queuedata() (minimal set) and
    load_schedconfig_data() (full set).

    :return: prioritized DDM source list.
    """

    return ['LOCAL', 'PANDA', 'CVMFS', 'CRIC']


def get_ddm_source_priority():
    """
    Return the prioritized list for the DDM sources.
    This list is used to determine which source to use for the DDM endpoints, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_storage_data().

    :return: prioritized DDM source list.
    """

    return ['USER', 'LOCAL', 'CVMFS', 'CRIC', 'PANDA']


def should_verify_setup(job):
    """
    Should the setup command be verified?

    :param job: job object.
    :return: Boolean.
    """

    return False
