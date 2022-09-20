#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2022
# - Tobias Wegner, tobias.wegner@cern.ch, 2018
# - David Cameron, david.cameron@cern.ch, 2018-2022

import os
import re

from pilot.common.exception import StageInFailure, StageOutFailure, MKDirFailure, ErrorCodes
from pilot.util.container import execute

import logging
logger = logging.getLogger(__name__)

require_replicas = False  # indicate if given copytool requires input replicas to be resolved
check_availablespace = False  # indicate whether space check should be applied before stage-in transfers using given copytool


def create_output_list(files, init_dir):
    """
    Add files to the output list which tells ARC CE which files to upload
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
            arcturl += ':checksumtype=%s:checksumvalue=%s' % (checksumtype, checksum)

        logger.info('Adding to output.list: %s %s', fspec.lfn, arcturl)
        # Write output.list
        with open(os.path.join(init_dir, 'output.list'), 'a') as f:
            f.write('%s %s\n' % (fspec.lfn, arcturl))


def is_valid_for_copy_in(files):
    return True  # FIX ME LATER
    #for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    #return True


def is_valid_for_copy_out(files):
    return True  # FIX ME LATER
    #for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    #return True


def get_dir_path(turl, prefix='file://localhost'):
    """
    Extract the directory path from the turl that has a given prefix
    E.g. turl = 'file://localhost/sphenix/lustre01/sphnxpro/rucio/user/jwebb2/01/9f/user.jwebb2.66999._000001.top1outDS.tar'
        -> '/sphenix/lustre01/sphnxpro/rucio/user/jwebb2/01/9f'
    (some of these directories will typically have to be created in the next step).

    :param turl: TURL (string).
    :param prefix: file prefix (string).
    :return: directory path (string).
    """

    path = turl.replace(prefix, '')
    return os.path.dirname(path)


def build_final_path(turl, prefix='file://localhost'):

    path = ''

    # first get the directory path to be created
    dirname = get_dir_path(turl, prefix=prefix)

    # now create any missing directories on the SE side
    try:
        os.makedirs(dirname)
    except FileExistsError:
        # ignore if sub dirs already exist
        pass
    except (IsADirectoryError, OSError) as exc:
        diagnostics = f'caught exception: {exc}'
        logger.warning(diagnostics)
        return ErrorCodes.MKDIR, diagnostics, path
    else:
        logger.debug(f'created {dirname}')

    return 0, '', os.path.join(dirname, os.path.basename(turl))


def copy_in(files, copy_type="symlink", **kwargs):
    """
    Tries to download the given files using mv directly.

    :param files: list of `FileSpec` objects
    :raises PilotException: StageInFailure
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
    exit_code, stdout, stderr = move_all_files(files, copy_type, kwargs.get('workdir'), kwargs.get('jobworkdir'))
    if exit_code != 0:
        # raise failure
        raise StageInFailure(stdout)

    return files


def copy_out(files, copy_type="mv", **kwargs):
    """
    Tries to upload the given files using mv directly.

    :param files: list of `FileSpec` objects
    :raises PilotException: StageOutFailure
    """

    if copy_type not in ["cp", "mv"]:
        raise StageOutFailure("incorrect method for copy out")

    if not kwargs.get('workdir'):
        raise StageOutFailure("Workdir is not specified")

    exit_code, stdout, stderr = move_all_files(files, copy_type, kwargs.get('workdir'), '')
    if exit_code != 0:
        # raise failure
        if exit_code == ErrorCodes.MKDIR:
            raise MKDirFailure(stdout)
        else:
            raise StageOutFailure(stdout)

    # Create output list for ARC CE if necessary
    logger.debug('init_dir for output.list=%s', os.path.dirname(kwargs.get('workdir')))
    output_dir = kwargs.get('output_dir', '')
    if not output_dir:
        create_output_list(files, os.path.dirname(kwargs.get('workdir')))

    return files


def move_all_files(files, copy_type, workdir, jobworkdir):
    """
    Move all files.

    :param files: list of `FileSpec` objects
    :return: exit_code, stdout, stderr
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
            if user.mv_to_final_destination():
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
            if user.mv_to_final_destination():
                # create any sub dirs if they don't exist already, and find the final destination path
                ec, diagnostics, destination = build_final_path(fspec.turl)
                if ec:
                    return ec, diagnostics, ''
            else:
                destination = os.path.join(os.path.dirname(workdir), name)

        # resolve canonical path
        source = os.path.realpath(source)

        logger.info("transferring file %s from %s to %s", name, source, destination)

        exit_code, stdout, stderr = copy_method(source, destination)
        if exit_code != 0:
            logger.warning("transfer failed: exit code = %d, stdout = %s, stderr = %s", exit_code, stdout, stderr)
            fspec.status = 'failed'
            if fspec.filetype == 'input':
                fspec.status_code = ErrorCodes.STAGEINFAILED
            else:
                fspec.status_code = ErrorCodes.STAGEOUTFAILED
            break
        else:
            fspec.status_code = 0
            fspec.status = 'transferred'

    return exit_code, stdout, stderr


def move(source, destination):
    """
    Tries to upload the given files using mv directly.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'mv', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def copy(source, destination):
    """
    Tries to upload the given files using xrdcp directly.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'cp', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr


def symlink(source, destination):
    """
    Tries to ln the given files.

    :param source:
    :param destination:

    :return: exit_code, stdout, stderr
    """

    executable = ['/usr/bin/env', 'ln', '-s', source, destination]
    cmd = ' '.join(executable)
    exit_code, stdout, stderr = execute(cmd)

    return exit_code, stdout, stderr
