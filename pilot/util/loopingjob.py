#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2022

from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import whoami, set_pilot_state, cut_output, locate_core_file
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import remove_files, find_latest_modified_file, verify_file_list, copy
from pilot.util.parameters import convert_to_int
from pilot.util.processes import kill_processes
from pilot.util.timing import time_stamp

import os
import time
import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()


def looping_job(job, montime):
    """
    Looping job detection algorithm.
    Identify hanging tasks/processes. Did the stage-in/out finish within allowed time limit, or did the payload update
    any files recently? The files must have been touched within the given looping_limit, or the process will be
    terminated.

    :param job: job object.
    :param montime: `MonitoringTime` object.
    :return: exit code (int), diagnostics (string).
    """

    exit_code = 0
    diagnostics = ""

    logger.info(f'checking for looping job (in state={job.state})')

    looping_limit = get_looping_job_limit()

    if job.state == 'stagein':
        # set job.state to stagein during stage-in before implementing this algorithm
        pass
    elif job.state == 'stageout':
        # set job.state to stageout during stage-out before implementing this algorithm
        pass
    elif job.state == 'running':
        # get the time when the files in the workdir were last touched. in case no file was touched since the last
        # check, the returned value will be the same as the previous time
        time_last_touched = get_time_for_last_touch(job, montime, looping_limit)

        # the payload process is considered to be looping if it's files have not been touched within looping_limit time
        if time_last_touched:
            currenttime = int(time.time())
            logger.info(f'current time: {currenttime}')
            logger.info(f'last time files were touched: {time_last_touched}')
            logger.info(f'looping limit: {looping_limit} s')
            if currenttime - time_last_touched > looping_limit:
                try:
                    # first produce core dump and copy it
                    create_core_dump(pid=job.pid, workdir=job.workdir)
                    # set debug mode to prevent core file from being removed before log creation
                    job.debug = True
                    kill_looping_job(job)
                except Exception as error:
                    logger.warning(f'exception caught: {error}')
        else:
            logger.info('no files were touched')

    return exit_code, diagnostics


def create_core_dump(pid=None, workdir=None):
    """
    Create core dump and copy it to work directory
    """

    if not pid or not workdir:
        logger.warning('cannot create core file since pid or workdir is unknown')
        return

    cmd = 'gdb --pid %d -ex \'generate-core-file\'' % pid
    exit_code, stdout, stderr = execute(cmd)
    if not exit_code:
        path = locate_core_file(pid=pid)
        if path:
            try:
                copy(path, workdir)
            except Exception as error:
                logger.warning(f'failed to copy core file: {error}')
            else:
                logger.debug('copied core dump to workdir')

    else:
        logger.warning(f'failed to execute command: {cmd}, stdout+stderr={stdout + stderr}')


def get_time_for_last_touch(job, montime, looping_limit):
    """
    Return the time when the files in the workdir were last touched.
    in case no file was touched since the last check, the returned value will be the same as the previous time.

    :param job: job object.
    :param montime: `MonitoringTime` object.
    :param looping_limit: looping limit in seconds.
    :return: time in seconds since epoch (int) (or None in case of failure).
    """

    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    loopingjob_definitions = __import__(f'pilot.user.{pilot_user}.loopingjob_definitions',
                                        globals(), locals(), [pilot_user], 0)

    # locate all files that were modified the last N minutes
    cmd = "find %s -mmin -%d" % (job.workdir, int(looping_limit / 60))
    exit_code, stdout, stderr = execute(cmd)
    if exit_code == 0:
        if stdout != "":
            files = stdout.split("\n")  # find might add a \n even for single entries

            # remove unwanted list items (*.py, *.pyc, workdir, ...)
            files = loopingjob_definitions.remove_unwanted_files(job.workdir, files)
            if files:
                logger.info(f'found {len(files)} files that were recently updated')
                #logger.debug('recent files:\n%s', files)
                updated_files = verify_file_list(files)

                # now get the mod times for these file, and identify the most recently update file
                latest_modified_file, mtime = find_latest_modified_file(updated_files)
                if latest_modified_file:
                    logger.info(f"file {latest_modified_file} is the most recently updated file (at time={mtime})")
                else:
                    logger.warning('looping job algorithm failed to identify latest updated file')
                    return montime.ct_looping_last_touched

                # store the time of the last file modification
                montime.update('ct_looping_last_touched', modtime=mtime)
            else:
                logger.warning("found no recently updated files!")
        else:
            logger.warning('found no recently updated files')
    else:
        # cut the output if too long
        stdout = cut_output(stdout)
        stderr = cut_output(stderr)
        logger.warning(f'find command failed: exitcode={exit_code}, stdout={stdout}, stderr={stderr}')

    return montime.ct_looping_last_touched


def kill_looping_job(job):
    """
    Kill the looping process.

    :param job: job object.
    :return: (updated job object.)
    """

    # the child process is looping, kill it
    diagnostics = f"pilot has decided to kill looping job {job.jobid} at {time_stamp()}"
    logger.fatal(diagnostics)

    cmds = [f'ps -fwu {whoami()}',
            f'ls -ltr {job.workdir}',
            f'ps -o pid,ppid,sid,pgid,tpgid,stat,comm -u {whoami()}',
            'pstree -g -a']
    for cmd in cmds:
        _, stdout, _ = execute(cmd, mute=True)
        logger.info(f"{cmd} + '\n': {stdout}")

    # set the relevant error code
    if job.state == 'stagein':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINTIMEOUT)
    elif job.state == 'stageout':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTTIMEOUT)
    else:
        # most likely in the 'running' state, but use the catch-all 'else'
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.LOOPINGJOB)
    set_pilot_state(job=job, state="failed")

    # remove any lingering input files from the work dir
    lfns, _ = job.get_lfns_and_guids()
    if lfns:
        _ec = remove_files(job.workdir, lfns)
        if _ec != 0:
            logger.warning('failed to remove all files')

    kill_processes(job.pid)


def get_looping_job_limit():
    """
    Get the time limit for looping job detection.

    :return: looping job time limit in seconds (int).
    """

    looping_limit = convert_to_int(config.Pilot.looping_limit_default, default=2 * 3600)
    looping_limit_min_default = convert_to_int(config.Pilot.looping_limit_min_default, default=2 * 3600)
    looping_limit = max(looping_limit, looping_limit_min_default)
    logger.info(f"using looping job limit: {looping_limit} s")

    return looping_limit
