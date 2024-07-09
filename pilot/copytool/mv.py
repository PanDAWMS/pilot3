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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2023
# - Tobias Wegner, tobias.wegner@cern.ch, 2018
# - David Cameron, david.cameron@cern.ch, 2018-2022

"""mv/cp/ln copy tool."""

import logging
import os
import re

from pilot.common.exception import StageInFailure, StageOutFailure, MKDirFailure, ErrorCodes
from pilot.util.container import execute

logger = logging.getLogger(__name__)

require_replicas = False  # indicate if given copytool requires input replicas to be resolved
check_availablespace = False  # indicate whether space check should be applied before stage-in transfers using given copytool


def is_valid_for_copy_in(files: list) -> bool:
    """
    Determine if this copytool is valid for input for the given file list.

    Placeholder.

    :param files: list of FileSpec objects (list).
    :return: always True (for now) (bool).
    """
    if files:  # to get rid of pylint warning
        pass
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files: list) -> bool:
    """
    Determine if this copytool is valid for input for the given file list.

    Placeholder.

    :param files: list of FileSpec objects (list).
    :return: always True (for now) (bool).
    """
    if files:  # to get rid of pylint warning
        pass
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


def create_output_list(files: list, init_dir: str):
    """
    Add files to the output list which tells ARC CE which files to upload.

    :param files: list of FileSpec objects (list)
    :param init_dir: start directory (str).
    """
    for fspec in files:
        arcturl = fspec.turl
        if arcturl.startswith('s3://'):
            # Use Rucio proxy to upload to OS
            arcturl = re.sub(r'^s3', 's3+rucio', arcturl)
            # Add failureallowed option so failed upload does not fail job
            rucio = 'rucio://rucio-lb-prod.cern.ch;failureallowed=yes/objectstores'
            rse = fspec.ddmendpoint
            activity = 'write'
            arcturl = '/'.join([rucio, arcturl, rse, activity])
        else:
            # Add ARC options to TURL
            checksumtype, checksum = list(fspec.checksum.items())[0]
            arcturl += f':checksumtype={checksumtype}:checksumvalue={checksum}'

        logger.info(f'adding to output.list: {fspec.lfn} {arcturl}')
        # Write output.list
        with open(os.path.join(init_dir, 'output.list'), 'a', encoding='utf-8') as f:
            f.write(f'{fspec.lfn} {arcturl}\n')


def get_dir_path(turl: str, prefix: str = 'file://localhost') -> str:
    """
    Extract the directory path from the turl that has a given prefix.

    E.g. turl = 'file://localhost/sphenix/lustre01/sphnxpro/rucio/user/jwebb2/01/9f/user.jwebb2.66999._000001.top1outDS.tar'
        -> '/sphenix/lustre01/sphnxpro/rucio/user/jwebb2/01/9f'
    (some of these directories will typically have to be created in the next step).

    :param turl: TURL (str)
    :param prefix: file prefix (str)
    :return: directory path (str).
    """
    return os.path.dirname(turl.replace(prefix, ''))


def build_final_path(turl: str, prefix: str = 'file://localhost') -> (int, str, str):
    """
    Build the final path for the storage.

    :param turl: TURL (str)
    :param prefix: file prefix (str)
    :return: error code (int), diagnostics (str), path (str).
    """
    path = ''

    # first get the directory path to be created
    dirname = get_dir_path(turl, prefix=prefix)

    # now create any missing directories on the SE side
    try:
        os.makedirs(dirname)
    except FileExistsError:
        # ignore if sub dirs already exist
        pass
    except IsADirectoryError as exc:
        diagnostics = f'caught IsADirectoryError exception: {exc}'
        logger.warning(diagnostics)
        return ErrorCodes.MKDIR, diagnostics, path
    except OSError as exc:
        diagnostics = f'caught OSError exception: {exc}'
        logger.warning(diagnostics)
        return ErrorCodes.MKDIR, diagnostics, path
    else:
        logger.debug(f'created {dirname}')

    return 0, '', os.path.join(dirname, os.path.basename(turl))


def copy_in(files: list, copy_type: str = "symlink", **kwargs: dict) -> list:
    """
    Download the given files using mv directly.

    :param files: list of `FileSpec` objects (list)
    :param copy_type: copy type (str)
    :param kwargs: kwargs dictionary (dict)
    :raises PilotException: StageInFailure
    :return: updated files (list).
    """
    # make sure direct access is not attempted (wrong queue configuration - pilot should fail job)
    allow_direct_access = kwargs.get('allow_direct_access')
    for fspec in files:
        if fspec.is_directaccess(ensure_replica=False) and allow_direct_access and fspec.accessmode == 'direct':
            fspec.status_code = ErrorCodes.BADQUEUECONFIGURATION
            raise StageInFailure("bad queue configuration - mv does not support direct access")

        # for symlinked input files, the file size should not be included in the workdir size (since it is not present!)
        # the boolean will be checked by the caller
        if copy_type == 'symlink':
            fspec.checkinputsize = False

    if copy_type not in ["cp", "mv", "symlink"]:
        raise StageInFailure("incorrect method for copy in")

    if not kwargs.get('workdir'):
        raise StageInFailure("workdir is not specified")

    logger.debug(f"workdir={kwargs.get('workdir')}")
    logger.debug(f"jobworkdir={kwargs.get('jobworkdir')}")
    exit_code, stdout, _ = move_all_files(files,
                                          copy_type,
                                          kwargs.get('workdir'),
                                          kwargs.get('jobworkdir'),
                                          mvfinaldest=kwargs.get('mvfinaldest', False))
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)

    return files


def copy_out(files: list, copy_type: str = "mv", **kwargs: dict) -> list:
    """
    Upload the given files using mv directly.

    :param files: list of `FileSpec` objects (list)
    :param copy_type: copy type (str)
    :param kwargs: kwargs dictionary (dict)
    :return: updated files (list)
    :raises PilotException: StageOutFailure, MKDirFailure.
    """
    if copy_type not in ["cp", "mv"]:
        raise StageOutFailure("incorrect method for copy out")

    if not kwargs.get('workdir'):
        raise StageOutFailure("Workdir is not specified")

    exit_code, stdout, _ = move_all_files(files,
                                          copy_type,
                                          kwargs.get('workdir'),
                                          '',
                                          mvfinaldest=kwargs.get('mvfinaldest', False))
    if exit_code != 0:
        # raise failure
        if exit_code == ErrorCodes.MKDIR:
            raise MKDirFailure(stdout)
        raise StageOutFailure(stdout)

    # Create output list for ARC CE if necessary
    logger.debug(f"init_dir for output.list={os.path.dirname(kwargs.get('workdir'))}")
    output_dir = kwargs.get('output_dir', '')
    if not output_dir:
        create_output_list(files, os.path.dirname(kwargs.get('workdir')))

    return files


def move_all_files(files: list, copy_type: str, workdir: str, jobworkdir: str,
                   mvfinaldest: bool = False) -> (int, str, str):
    """
    Move all files.

    :param files: list of `FileSpec` objects (list)
    :param copy_type: copy type (str)
    :param workdir: work directory (str)
    :param jobworkdir: work directory for job (str)
    :param mvfinaldest: True if we can transfer to final SE destination, False otherwise (bool)
    :return: exit code (int), stdout (str), stderr (str).
    """
    exit_code = 0
    stdout = ""
    stderr = ""
    # copy_method = None

    if copy_type == "mv":
        copy_method = move
    elif copy_type == "cp":
        copy_method = copy
    elif copy_type == "symlink":
        copy_method = symlink
    else:
        return -1, "", "incorrect copy method"

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    user = __import__(f'pilot.user.{pilot_user}.copytool_definitions', globals(), locals(), [pilot_user], 0)
    for fspec in files:  # entry = {'name':<filename>, 'source':<dir>, 'destination':<dir>}

        name = fspec.lfn
        if fspec.filetype == 'input':
            if user.mv_to_final_destination() or mvfinaldest:
                subpath = user.get_path(fspec.scope, fspec.lfn)
                logger.debug(f'subpath={subpath}')
                source = os.path.join(workdir, subpath)
            else:
                # Assumes pilot runs in subdir one level down from working dir
                source = os.path.join(os.path.dirname(workdir), name)
            destination = os.path.join(jobworkdir, name) if jobworkdir else os.path.join(workdir, name)
        else:
            source = os.path.join(workdir, name)
            # is the copytool allowed to move files to the final destination (not in Nordugrid/ATLAS)
            if user.mv_to_final_destination() or mvfinaldest:
                # create any sub dirs if they don't exist already, and find the final destination path
                ec, diagnostics, destination = build_final_path(fspec.turl)
                if ec:
                    return ec, diagnostics, ''
            else:
                destination = os.path.join(os.path.dirname(workdir), name)

        # resolve canonical path
        source = os.path.realpath(source)

        logger.info(f"transferring file {name} from {source} to {destination}")

        exit_code, stdout, stderr = copy_method(source, destination)
        if exit_code != 0:
            logger.warning(f"transfer failed: exit code = {exit_code}, stdout = {stdout}, stderr = {stderr}")
            fspec.status = 'failed'
            if fspec.filetype == 'input':
                fspec.status_code = ErrorCodes.STAGEINFAILED
            else:
                fspec.status_code = ErrorCodes.STAGEOUTFAILED
            break

        fspec.status_code = 0
        fspec.status = 'transferred'

    return exit_code, stdout, stderr


def move(source: str, destination: str) -> (int, str, str):
    """
    Upload the given files using mv directly.

    :param source: file source path (str)
    :param destination: file destination path (str)
    :return: exit code (int), stdout (str), stderr (str).
    """
    executable = ['/usr/bin/env', 'mv', source, destination]
    return execute(' '.join(executable))


def copy(source: str, destination: str) -> (int, str, str):
    """
    Upload the given files using cp directly.

    :param source: file source path (str)
    :param destination: file destination path (str)
    :return: exit code (int), stdout (str), stderr (str).
    """
    executable = ['/usr/bin/env', 'cp', source, destination]
    return execute(' '.join(executable))


def symlink(source: str, destination: str) -> (int, str, str):
    """
    Create symbolic link ln the given file.

    :param source: file source path (str)
    :param destination: file destination path (str)
    :return: exit code (int), stdout (str), stderr (str).
    """
    executable = ['/usr/bin/env', 'ln', '-s', source, destination]
    return execute(' '.join(executable))
