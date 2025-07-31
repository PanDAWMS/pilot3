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

import glob
import logging
import os
import re

from datetime import datetime
from time import sleep

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    FileHandlingFailure,
    NoSoftwareDir,
    NoSuchFile
)
from pilot.info import (
    infosys,
    JobData
)
from pilot.util.auxiliary import find_pattern_in_list
from pilot.util.filehandling import (
    copy,
    head,
    read_file,
    write_file,
)
from pilot.util.https import download_file
from .metadata import get_file_info_from_xml

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def get_file_system_root_path() -> str:
    """
    Return the root path of the local file system.

    The function returns "/cvmfs" or "/(some path)/cvmfs" in case the expected file system root path is not
    where it usually is (e.g. on an HPC). A site can set the base path by exporting ATLAS_SW_BASE.

    :return: path (str).
    """
    return os.environ.get('ATLAS_SW_BASE', '/cvmfs')


def should_pilot_prepare_setup(noexecstrcnv: bool, jobpars: str, imagename: str = None) -> bool:
    """
    Determine whether the pilot should add the setup to the payload command or not.

    The pilot will not add asetup if jobPars already contain the information (i.e. it was set by the payload creator).
    If noExecStrCnv is set, then jobPars is expected to contain asetup.sh + options
    If a stand-alone container / user defined container is used, pilot should not prepare asetup.

    :param noexecstrcnv: noExecStrCnv value (bool)
    :param jobpars: job parameters (str)
    :param imagename: container image (str)
    :return: True if the pilot should prepare the setup (bool).
    """
    if imagename:
        return False

    if noexecstrcnv:
        if "asetup.sh" in jobpars:
            logger.info("asetup will be taken from jobPars")
            preparesetup = False
        else:
            logger.info("noExecStrCnv is set but asetup command was not found in jobPars (pilot will prepare asetup)")
            preparesetup = True
    else:
        logger.info("pilot will prepare the setup")
        preparesetup = True

    return preparesetup


def get_alrb_export(add_if: bool = False) -> str:
    """
    Return the export command for the ALRB path if it exists.

    If the path does not exist, return empty string.

    :param add_if: True means that an if statement will be placed around the export (bool)
    :return: export command (str).
    """
    path = f"{get_file_system_root_path()}/atlas.cern.ch/repo"
    cmd = f"export ATLAS_LOCAL_ROOT_BASE={path}/ATLASLocalRootBase;" if os.path.exists(path) else ""

    # if [ -z "$ATLAS_LOCAL_ROOT_BASE" ]; then export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase; fi;
    if cmd and add_if:
        cmd = 'if [ -z \"$ATLAS_LOCAL_ROOT_BASE\" ]; then ' + cmd + ' fi;'

    return cmd


def get_asetup(asetup: bool = True, alrb: bool = False, add_if: bool = False) -> str:
    """
    Define the setup for asetup, i.e. including full path to asetup and setting of ATLAS_LOCAL_ROOT_BASE.

    Only include the actual asetup script if asetup=True. This is not needed if the jobPars contain the payload command
    but the pilot still needs to add the exports and the atlasLocalSetup.

    :param asetup: True value means that the pilot should include the asetup command (bool)
    :param alrb: True value means that the function should return special setup used with ALRB and containers (bool)
    :param add_if: True means that an if statement will be placed around the export (bool)
    :return: source <path>/asetup.sh (str).
    :raises: NoSoftwareDir if appdir does not exist.
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


def get_asetup_options(release: str, homepackage: str) -> str:
    """
    Determine the proper asetup options.

    :param release: ATLAS release (str)
    :param homepackage: ATLAS homePackage (str)
    :return: asetup options (str).
    """
    asetupopt = []
    release = re.sub('^Atlas-', '', release)

    # is it a user analysis homePackage?
    if 'AnalysisTransforms' in homepackage:

        _homepackage = re.sub('^AnalysisTransforms-*', '', homepackage)
        if _homepackage == '' or re.search(r'^\d+\.\d+\.\d+$', release) is None:
            if release != "":
                asetupopt.append(release)
        if _homepackage != '':
            asetupopt += _homepackage.split('_')
    else:
        asetupopt += homepackage.split('/')
        if release not in homepackage and release not in asetupopt:
            asetupopt.append(release)

    # Add the notest,here for all setups (not necessary for late releases but harmless to add)
    asetupopt.append('notest')
    # asetupopt.append('here')

    # Add the fast option if possible (for the moment, check for locally defined env variable)
    if "ATLAS_FAST_ASETUP" in os.environ:
        asetupopt.append('fast')

    return ','.join(asetupopt)


def is_standard_atlas_job(release: str) -> bool:
    """
    Check if it is a standard ATLAS job.

    A job is a standard ATLAS job if the release string begins with 'Atlas-'.

    :param release: release value (str)
    :return: returns True if standard ATLAS job (bool).
    """
    return release.startswith('Atlas-')


def set_inds(dataset: str):
    """
    Set the INDS environmental variable used by runAthena.

    :param dataset: dataset for input files (realDatasetsIn) (str).
    """
    inds = ""
    _dataset = dataset.split(',')
    for ds in _dataset:
        if "DBRelease" not in ds and ".lib." not in ds:
            inds = ds
            break
    if inds != "":
        logger.info(f"setting INDS environmental variable to: {inds}")
        os.environ['INDS'] = inds
    else:
        logger.warning("INDS unknown")
        os.environ['INDS'] = 'unknown'


def get_analysis_trf(transform: str, workdir: str, base_urls: list) -> tuple[int, str, str]:
    """
    Prepare to download the user analysis transform with curl.

    The function will verify the download location from a known list of hosts.

    :param transform: full trf path (url) (str)
    :param workdir: work directory (str)
    :param base_urls: base URLs for trf download (list)
    :return: exit code (int), diagnostics (str), transform_name (str) (tuple).
    """
    ec = 0
    diagnostics = ""

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        search_pattern = f"{harvester_workdir}/jobO.*.tar.gz"
        jobopt_files = glob.glob(search_pattern)
        for jobopt_file in jobopt_files:
            try:
                copy(jobopt_file, workdir)
            except (FileHandlingFailure, NoSuchFile) as error:
                logger.error(f"could not copy file {jobopt_file} to {workdir} : {error}")

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

    path = os.path.join(workdir, transform_name)
    logger.debug(f"changing permission of {path} to 0o755")
    try:
        os.chmod(path, 0o755)
    except Exception as error:
        diagnostics = f"failed to chmod {transform_name}: {error}"
        return errors.CHMODTRF, diagnostics, ""

    return ec, diagnostics, transform_name


def download_transform(url: str, transform_name: str, workdir: str) -> tuple[bool, str]:
    """
    Download the transform from the given url.

    :param url: download URL with path to transform (str)
    :param transform_name: trf name (str)
    :param workdir: work directory (str)
    :return: status (bool), diagnostics (str).
    """
    status = False
    diagnostics = ""
    path = os.path.join(workdir, transform_name)
    trial = 1
    max_trials = 3

    # test if $HARVESTER_WORKDIR is set
    harvester_workdir = os.environ.get('HARVESTER_WORKDIR')
    if harvester_workdir is not None:
        source_path = os.path.join(harvester_workdir, transform_name)
        try:
            copy(source_path, path)
            status = True
        except (FileHandlingFailure, NoSuchFile) as error:
            diagnostics = f"failed to copy file {source_path} to {path} : {error}"
            logger.error(diagnostics)
            status = False

        return status, diagnostics

    # try to download the trf a maximum of 3 times
    while trial <= max_trials:
        logger.info(f"downloading file {transform_name} [trial {trial}/{max_trials}]")

        content = download_file(url)
        with open(path, "wb+") as _file:  # note: binary mode, so no encoding is needed (or, encoding=None)
            if content:
                _file.write(content)
                logger.info(f'saved data from \"{url}\" resource into file {path}, '
                            f'length={len(content) / 1024.:.1f} kB')
                status = True

        if not status:
            # Analyze exit code / output
            diagnostics = f'no data was downloaded from {url}'
            logger.warning(diagnostics)
            if trial == max_trials:
                logger.fatal(f'could not download transform: {transform_name}')
                break

            logger.info("will try again after 60 s")
            sleep(60)
        else:
            logger.info(f"transform {transform_name} downloaded")
            break
        trial += 1

    return status, diagnostics


def get_valid_base_urls(base_urls: list, order: str = None) -> list:
    """
    Return a list of valid base URLs from where the user analysis transform may be downloaded from.

    If order is defined, return given item first.
    E.g. order=http://atlpan.web.cern.ch/atlpan -> ['http://atlpan.web.cern.ch/atlpan', ...]
    NOTE: the URL list may be out of date.

    :param base_urls: list of base URLs (list)
    :param order: order (str)
    :return: valid base URLs (list).
    """
    if not base_urls:
        base_urls = [
            "www.usatlas.bnl.gov",
            "pandaserver.cern.ch",
            "pandaserver-tb.cern.ch",
            "atlpan.web.cern.ch/atlpan",
            "classis01.roma1.infn.it",
            "atlas-install.roma1.infn.it"
        ]

    valid_base_urls = []
    for base_url in base_urls:
        if not base_url.startswith(("http://", "https://")):
            valid_base_urls.append(f"http://{base_url}")
            valid_base_urls.append(f"https://{base_url}")
        else:
            valid_base_urls.append(base_url)

    if order:
        valid_base_urls = [order] + [url for url in valid_base_urls if url != order]

    return valid_base_urls


def get_payload_environment_variables(cmd: str, job_id: str, task_id: str, attempt_nr: int, processing_type: str, site_name: str, analysis_job: bool) -> list:
    """
    Return an array with enviroment variables needed by the payload.

    :param cmd: payload execution command (str)
    :param job_id: PanDA job id (str)
    :param task_id: PanDA task id (str)
    :param attempt_nr: PanDA job attempt number (int)
    :param processing_type: processing type (str)
    :param site_name: site name (str)
    :param analysis_job: True for user analysis jobs, False otherwise (bool)
    :return: environment variables needed by the payload (list).
    """
    variables = []
    variables.append(f'export PANDA_RESOURCE=\'{site_name}\';')
    variables.append(f'export FRONTIER_ID="[{task_id}_{job_id}]";')
    variables.append('export CMSSW_VERSION=$FRONTIER_ID;')
    variables.append(f"export PandaID={os.environ.get('PANDAID', 'unknown')};")
    variables.append(f"export PanDA_TaskID='{os.environ.get('PanDA_TaskID', 'unknown')}';")
    variables.append(f'export PanDA_AttemptNr=\'{attempt_nr}\';')
    variables.append(f"export INDS='{os.environ.get('INDS', 'unknown')}';")

    # Unset ATHENA_PROC_NUMBER if set for event service Merge jobs
    if "Merge_tf" in cmd and 'ATHENA_PROC_NUMBER' in os.environ:
        variables.append('unset ATHENA_PROC_NUMBER;')
        variables.append('unset ATHENA_CORE_NUMBER;')

    if analysis_job:
        variables.append('export ROOT_TTREECACHE_SIZE=1;')
        try:
            core_count = int(os.environ.get('ATHENA_PROC_NUMBER'))
        except Exception:
            _core_count = 'export ROOTCORE_NCPUS=1;'
        else:
            _core_count = f'export ROOTCORE_NCPUS={core_count};'
        variables.append(_core_count)

    if processing_type == "":
        logger.warning("RUCIO_APPID needs job.processingType but it is not set!")
    else:
        variables.append(f'export RUCIO_APPID=\'{processing_type}\';')
    # variables.append(f"export RUCIO_ACCOUNT='{os.environ.get('RUCIO_ACCOUNT', 'pilot')}';")
    variables.append("unset RUCIO_ACCOUNT;")

    return variables


def get_writetoinput_filenames(writetofile: str) -> list:
    """
    Extract the writeToFile file name(s).

    writeToFile='tmpin_mc16_13TeV.blah:AOD.15760866._000002.pool.root.1'
    -> return 'tmpin_mc16_13TeV.blah'

    :param writetofile: string containing file name information (str)
    :return: file names (list).
    """
    filenames = []

    entries = writetofile.split('^')
    for entry in entries:
        if ':' in entry:
            name = entry.split(":")[0]
            name = name.replace('.pool.root.', '.txt.')  # not necessary?
            filenames.append(name)

    return filenames


def replace_lfns_with_turls(cmd: str, workdir: str, filename: str, infiles: list, writetofile: str = "") -> str:
    """
    Replace all LFNs with full TURLs in the payload execution command.

    This function is used with direct access in production jobs. Athena requires a full TURL instead of LFN.

    :param cmd: payload execution command (str)
    :param workdir: location of metadata file (str)
    :param filename: metadata file name (str)
    :param infiles: input files (list)
    :param writetofile: writeToFile file name (str)
    :return: updated cmd (str).
    """
    turl_dictionary = {}  # { LFN: TURL, ..}

    path = os.path.join(workdir, filename)
    if os.path.exists(path):
        file_info_dictionary = get_file_info_from_xml(workdir, filename=filename)
        for inputfile in infiles:
            if inputfile in cmd:
                turl = file_info_dictionary[inputfile][0]
                turl_dictionary[inputfile] = turl
                # if turl.startswith('root://') and turl not in cmd:
                if turl not in cmd:
                    cmd = cmd.replace(inputfile, turl)
                    logger.info(f"replaced '{inputfile}' with '{turl}' in the run command")

        # replace the LFNs with TURLs in the writetofile input file list (if it exists)
        if writetofile and turl_dictionary:
            filenames = get_writetoinput_filenames(writetofile)
            for fname in filenames:
                new_lines = []
                path = os.path.join(workdir, fname)
                if os.path.exists(path):
                    f = read_file(path)
                    for line in f.split('\n'):
                        fname = os.path.basename(line)
                        if fname in turl_dictionary:
                            turl = turl_dictionary[fname]
                            new_lines.append(turl)
                        elif line:
                            new_lines.append(line)

                    lines = '\n'.join(new_lines)
                    if lines:
                        write_file(path, lines)
                else:
                    logger.warning(f"file does not exist: {path}")
    else:
        logger.warning(f"could not find file: {filename} (cannot locate TURLs for direct access)")

    return cmd


def get_end_setup_time(path: str, pattern: str = r'(\d{2}\:\d{2}\:\d{2}\ \d{4}\/\d{2}\/\d{2})') -> float:
    """
    Extract a more precise end of setup time from the payload stdout.

    File path should be verified already.
    The function will look for a date time in the beginning of the payload stdout with the given pattern.

    :param path: path to payload stdout (str)
    :param pattern: regular expression pattern (str)
    :return: time in seconds since epoch (float).
    """
    end_time = None

    head_list = head(path, count=50)
    time_string = find_pattern_in_list(head_list, pattern)
    if time_string:
        logger.debug(f"extracted time string=\'{time_string}\' from file \'{path}\'")
        end_time = datetime.strptime(time_string, '%H:%M:%S %Y/%m/%d').timestamp()  # since epoch

    return end_time


def get_schedconfig_priority() -> list:
    """
    Return the prioritized list for the schedconfig sources.

    This list is used to determine which source to use for the queuedatas, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_queuedata() (minimal set) and
    load_schedconfig_data() (full set).

    :return: prioritized DDM sources (list).
    """
    return ['LOCAL', 'CVMFS', 'CRIC', 'PANDA']


def get_queuedata_priority() -> list:
    """
    Return the prioritized list for the schedconfig sources.

    This list is used to determine which source to use for the queuedatas, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_queuedata() (minimal set) and
    load_schedconfig_data() (full set).

    :return: prioritized DDM sources (list).
    """
    return ['LOCAL', 'PANDA', 'CVMFS', 'CRIC']


def get_ddm_source_priority() -> list:
    """
    Return the prioritized list for the DDM sources.

    This list is used to determine which source to use for the DDM endpoints, which can be different for
    different users. The sources themselves are defined in info/extinfo/load_storage_data().

    :return: prioritized DDM sources (list).
    """
    return ['USER', 'LOCAL', 'CVMFS', 'CRIC', 'PANDA']


def should_verify_setup(job: JobData) -> bool:
    """
    Check if the setup command should be verified.

    :param job: job object (JobData)
    :return: True if the setup command should be verified, False otherwise (bool).
    """
    return job.swrelease and job.swrelease != 'NULL'
