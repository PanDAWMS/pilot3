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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-24

import logging
import os
import re
import tarfile

from pilot.common.exception import (
    FileHandlingFailure,
    PilotException
)
from pilot.util.filehandling import (
    write_file,
    mkdirs,
    rmdirs
)

logger = logging.getLogger(__name__)


def extract_version(name: str) -> str:
    """
    Try to extract the version from the DBRelease string.

    :param name: DBRelease (str)
    :return: version (str).
    """
    version = ""

    re_v = re.compile(r'DBRelease-(\d+\.\d+\.\d+)\.tar\.gz')  # Python 3 (added r)
    ver = re_v.search(name)
    if ver:
        version = ver.group(1)
    else:
        re_v = re.compile(r'DBRelease-(\d+\.\d+\.\d+\.\d+)\.tar\.gz')  # Python 3 (added r)
        ver = re_v.search(name)
        if ver:
            version = ver.group(1)

    return version


def get_dbrelease_version(jobpars: str) -> str:
    """
    Get the DBRelease version from the job parameters.

    :param jobpars: job parameters (str)
    :return: DBRelease version (str).
    """
    return extract_version(jobpars)


def get_dbrelease_dir() -> str:
    """
    Return the proper DBRelease directory

    :return: path to DBRelease (str).
    """
    path = os.path.join(os.environ.get('VO_ATLAS_SW_DIR', 'OSG_APP'), 'database/DBRelease')
    if path == "" or path.startswith('OSG_APP'):
        logger.warning("note: the DBRelease database directory is not available (will not attempt to skip DBRelease stage-in)")
    elif os.path.exists(path):
        logger.info(f"local DBRelease path verified: {path} (will attempt to skip DBRelease stage-in)")
    else:
        logger.warning(f"note: local DBRelease path does not exist: {path} "
                       f"(will not attempt to skip DBRelease stage-in)")

    return path


def is_dbrelease_available(version: str) -> bool:
    """
    Check whether a given DBRelease file is already available.

    :param version: DBRelease version (str)
    :return: True is DBRelease is locally available, False otherwise (bool).
    """
    status = False

    # do not proceed if
    if 'ATLAS_DBREL_DWNLD' in os.environ:
        logger.info("ATLAS_DBREL_DWNLD is set: do not skip DBRelease stage-in")
        return status

    # get the local path to the DBRelease directory
    path = get_dbrelease_dir()

    if path != "" and os.path.exists(path):
        # get the list of available DBRelease directories
        dir_list = os.listdir(path)

        # is the required DBRelease version available?
        if dir_list:
            if version in dir_list:
                logger.info(f"found version {version} in path {path} ({len(dir_list)} releases found)")
                status = True
            else:
                logger.warning(f"did not find version {version} in path {path} ({len(dir_list)} releases found)")
        else:
            logger.warning(f"empty DBRelease directory list: {path}")
    else:
        logger.warning(f'no such DBRelease path: {path}')

    return status


def create_setup_file(version: str, path: str) -> bool:
    """
    Create the DBRelease setup file.

    :param version: DBRelease version (str)
    :param path: path to local DBReleases (str)
    :return: True if DBRelease setup file was successfully created, False otherwise (bool).
    """
    status = False

    # get the DBRelease directory
    _dir = get_dbrelease_dir()
    if _dir != "" and version != "":
        # create the python code string to be written to file
        txt = "import os\n"
        txt += f"os.environ['DBRELEASE'] = '{version}'\n"
        txt += f"os.environ['DATAPATH'] = '{_dir}/{version}:' + os.environ['DATAPATH']\n"
        txt += f"os.environ['DBRELEASE_REQUIRED'] = '{version}'\n"
        txt += f"os.environ['DBRELEASE_REQUESTED'] = '{version}'\n"
        txt += f"os.environ['CORAL_DBLOOKUP_PATH'] = '{_dir}/{version}/XMLConfig'\n"

        try:
            status = write_file(path, txt)
        except FileHandlingFailure as exc:
            logger.warning(f'failed to create DBRelease setup file: {exc}')
        else:
            logger.info(f"Created setup file with the following content:.................................\n{txt}")
            logger.info("...............................................................................")
    else:
        logger.warning(f'failed to create {path} for DBRelease version={version} and directory {_dir}')

    return status


def create_dbrelease(version: str, path: str) -> bool:
    """
    Create the DBRelease file only containing a setup file.

    :param version: DBRelease version (str)
    :param path: path to DBRelease (str)
    :return: True is DBRelease file was successfully created, False otherwise (bool).
    """
    status = False

    # create the DBRelease and version directories
    dbrelease_path = os.path.join(path, 'DBRelease')
    _path = os.path.join(dbrelease_path, version)
    try:
        mkdirs(_path, chmod=None)
    except PilotException as exc:
        logger.warning(f'failed to create directories for DBRelease: {exc}')
    else:
        logger.debug(f'created directories: {_path}')

        # create the setup file in the DBRelease directory
        version_path = os.path.join(dbrelease_path, version)
        setup_filename = "setup.py"
        _path = os.path.join(version_path, setup_filename)
        if create_setup_file(version, _path):
            logger.info(f"created DBRelease setup file: {_path}")

            # now create a new DBRelease tarball
            filename = os.path.join(path, f"DBRelease-{version}.tar.gz")
            logger.info(f"creating file: {filename}")
            try:
                with tarfile.open(filename, "w:gz") as tar:
                    # add the setup file to the tar file
                    tar.add(f"{path}/DBRelease/{version}/{setup_filename}")

                    # create the symbolic link DBRelease/current ->  12.2.1
                    try:
                        _link = os.path.join(path, "DBRelease/current")
                        os.symlink(version, _link)
                    except OSError as exc:
                        logger.warning(f"failed to create symbolic link {_link}: {exc}")
                    else:
                        logger.warning(f"created symbolic link: {_link}")

                        # add the symbolic link to the tar file
                        tar.add(_link)

                        logger.info(f"created new DBRelease tar file: {filename}")
                        status = True
            except OSError as exc:
                logger.warning(f"could not create DBRelease tar file: {exc}")

            # clean up
            if rmdirs(dbrelease_path):
                logger.debug(f"cleaned up directories in path: {dbrelease_path}")
        else:
            logger.warning("failed to create DBRelease setup file")
            if rmdirs(dbrelease_path):
                logger.debug(f"cleaned up directories in path: {dbrelease_path}")

    return status
