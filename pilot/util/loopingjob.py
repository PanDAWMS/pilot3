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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-26

"""Functions for identifying looping payloads."""

from __future__ import annotations
import logging
import os
import time
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.util.auxiliary import (
    set_pilot_state,
    cut_output
)
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import (
    remove_files,
    find_latest_modified_file,
    verify_file_list,
    list_mod_files
)
from pilot.util.heartbeat import time_since_suspension
from pilot.util.loopingdumps import (
    create_core_dump,
    select_dump_candidates,
    take_looping_snapshot,
)
from pilot.util.math import convert_seconds_to_hours_minutes_seconds
from pilot.util.parameters import convert_to_int
from pilot.util.processes import (
    dump_stack_trace,
    kill_process,
    find_zombies,
    handle_zombies,
    reap_zombies
)
from pilot.util.psutils import (
    get_child_processes,
    get_pilot_process_tree,
    get_process_details,
)
from pilot.util.timing import time_stamp

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def looping_job(job: Any, montime: Any) -> tuple[int, str]:
    """Identify looping payload, processes and tasks.

    Determines whether the stage-in/out finished within the allowed time limit,
    or whether the payload updated any files recently. Files must have been
    touched within the given ``looping_limit``, or the process will be
    terminated.

    Args:
        job: Job object.
        montime: ``MonitoringTime`` object.

    Returns:
        A tuple of ``(exit_code, diagnostics)`` where ``exit_code`` is an
        integer error code (0 on success) and ``diagnostics`` is a descriptive
        message string.
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
        time_last_touched, recent_files = get_time_for_last_touch(job, montime, looping_limit)

        # correct for job suspension if detected
        time_since_job_suspension = time_since_suspension()
        if time_since_job_suspension:
            # if there was no measurement, the time_last_touched will be zero
            if not time_last_touched:
                logger.warning('no time_last_touched measurement found (setting to 0)')
                return 0, ""

            logger.info(f'looping job killer adjusting for job suspension: {time_since_job_suspension} s (adding to time_last_touched))')
            time_last_touched += time_since_job_suspension

        # the payload process is considered to be looping if it's files have not been touched within looping_limit time
        if time_last_touched:
            currenttime = int(time.time())
            since_touch = currenttime - time_last_touched
            hours, minutes, seconds = convert_seconds_to_hours_minutes_seconds(since_touch)
            logger.info(f'files were last touched {hours}h {minutes}m {seconds}s ago (current time: {currenttime})')

            if since_touch > looping_limit:
                try:
                    # which were the considered files?
                    list_mod_files(recent_files)
                    # overrule any other debug command before setting debug mode, so that
                    # setting job.debug cannot start real-time logging in the payload thread
                    job.debug_command = 'looping'
                    # set debug mode to prevent core file from being removed before log creation
                    job.debug = True
                    # produce the core dump (and the analysis information needed to read it)
                    # while the payload processes are still alive
                    create_core_dump(job)
                    kill_looping_job(job)
                    exit_code = errors.LOOPINGJOB
                    diagnostics = 'the payload was found to be looping - job will be failed in the next update'
                except Exception as error:  # pylint: disable=broad-exception-caught
                    logger.warning(f'exception caught: {error}')
            else:
                # not looping (yet) - record a cheap diagnostic snapshot if the job is
                # already a configurable fraction of the way to the looping limit, so
                # that the deltas between snapshots are available if it is killed later
                take_looping_snapshot(job, since_touch, looping_limit)
        else:
            logger.info('no files were touched')

    return exit_code, diagnostics


def _handle_zombies(job: Any):
    """Collect and handle any zombie processes belonging to this pilot.

    Args:
        job: Job object.
    """
    try:
        zombies = find_zombies(os.getpid())
        if zombies:
            logger.info(f'found zombies: {zombies}')
            handle_zombies(zombies, job=job)
        else:
            logger.info('found no zombies')
    except Exception as exp:  # pylint: disable=broad-exception-caught
        logger.warning(f'exception caught: {exp}')


def _dump_payload_stack_traces(job: Any):
    """Dump stack traces for the payload processes before they are killed.

    The generic kill path (:func:`pilot.util.processes.kill_processes`) dumps a
    stack trace for every process it kills, but the looping job path kills the
    children itself and would otherwise discard that information - which is
    precisely the information needed to explain the loop.

    Args:
        job: Job object.
    """
    try:
        for pid, cmdline in select_dump_candidates(job, label="before kill"):
            logger.info(f'stack trace for pid={pid} ({cmdline}):')
            dump_stack_trace(pid)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f'failed to dump payload stack traces: {exc}')


def _log_workdir_listing(workdir: str):
    """Log a time-sorted listing of the job work directory.

    Uses :func:`os.scandir` rather than a CLI ``ls`` call so that the output
    is scoped to this job's directory and avoids spawning a subprocess.

    Args:
        workdir: Absolute path to the job work directory.
    """
    try:
        entries = sorted(os.scandir(workdir), key=lambda e: e.stat().st_mtime)
    except OSError as exc:
        logger.warning(f'failed to list workdir {workdir}: {exc}')
        return

    lines = [f"workdir listing ({workdir}):"]
    for entry in entries:
        try:
            st = entry.stat()
            mtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(st.st_mtime))
            ftype = 'd' if entry.is_dir(follow_symlinks=False) else \
                    'l' if entry.is_symlink() else '-'
            lines.append(f"  {ftype} {st.st_size:>12}  {mtime}  {entry.name}")
        except OSError:
            lines.append(f"  ?            (stat failed)  {entry.name}")

    logger.info('\n'.join(lines))


def kill_looping_job(job: Any):
    """Kill the looping payload process and clean up.

    Collects and reaps zombie processes, logs a snapshot of the pilot process
    tree and work directory contents (scoped to this pilot only — not the full
    worker node), sets the appropriate error code, transitions the job to the
    ``failed`` state, removes any lingering stage-in files, and finally kills
    all child processes of the pilot.

    Args:
        job: Job object.
    """
    # the child process is looping, kill it
    diagnostics = f"pilot has decided to kill looping job {job.jobid} at {time_stamp()}"
    logger.fatal(diagnostics)
    job.debug_command = 'looping'  # overrule any other debug command - also prevents real time logging from starting

    # dump stack traces while the payload processes are still alive (the generic kill
    # path does this, but this path kills the children itself)
    _dump_payload_stack_traces(job)

    # process zombies
    if job.pid not in job.zombies:
        job.zombies.append(job.pid)
    logger.info("pass #1/2: collecting zombie processes")
    job.collect_zombies(depth=10)
    logger.debug('pass #2/2: reaping zombies')
    reap_zombies()
    _handle_zombies(job)

    # log diagnostics scoped to this pilot subtree only
    _log_workdir_listing(job.workdir)

    pilot_pid = os.getpid()
    pilot_tree = get_pilot_process_tree(pilot_pid)
    logger.info(f"pilot process tree (PID {pilot_pid}):\n{pilot_tree}")

    logger.info(f"payload process: {get_process_details(job.pid)}")

    # set the relevant error code
    if job.state == 'stagein':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEINTIMEOUT, priority=True)
    elif job.state == 'stageout':
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.STAGEOUTTIMEOUT, priority=True)
    else:
        # most likely in the 'running' state, but use the catch-all 'else'
        job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.LOOPINGJOB, priority=True)
    set_pilot_state(job=job, state="failed")

    # remove any lingering input files from the work dir
    lfns, _ = job.get_lfns_and_guids()
    if lfns:
        _ec = remove_files(lfns, workdir=job.workdir)
        if _ec != 0:
            logger.warning('failed to remove all files')

    child_processes = get_child_processes(pilot_pid)
    if child_processes:
        logger.info(f"child processes of pilot (PID {pilot_pid}) to be killed:")
        for pid, cmdline in child_processes:
            logger.info(f"PID {pid}: {cmdline}")
        for pid, _ in child_processes:
            kill_process(pid)


def get_time_for_last_touch(job: Any, montime: Any, looping_limit: int) -> tuple[int, list]:
    """Return the time when files in the work directory were last touched.

    If no file was touched since the last check, the returned value will be
    the same as the previous measurement stored in ``montime``.

    Args:
        job: Job object.
        montime: ``MonitoringTime`` object.
        looping_limit: Looping detection time limit in seconds.

    Returns:
        A tuple of ``(time_last_touched, recent_files)`` where
        ``time_last_touched`` is the time in seconds since the Unix epoch (or
        ``None`` on failure) and ``recent_files`` is a list of recently
        modified file paths.
    """
    updated_files = []
    pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
    loopingjob_definitions = __import__(f'pilot.user.{pilot_user}.loopingjob_definitions',
                                        globals(), locals(), [pilot_user], 0)

    # locate all files that were modified the last N minutes
    cmd = f"find {job.workdir} -mmin -{int(looping_limit / 60)}"
    exit_code, stdout, stderr = execute(cmd)
    if exit_code == 0:
        if stdout != "":
            files = stdout.split("\n")  # find might add a \n even for single entries

            # remove unwanted list items (*.py, *.pyc, workdir, ...)
            files = loopingjob_definitions.remove_unwanted_files(job.workdir, files)
            if files:
                logger.info(f'found {len(files)} files that were recently updated')
                updated_files = verify_file_list(files)

                # now get the mod times for these file, and identify the most recently update file
                latest_modified_file, mtime = find_latest_modified_file(updated_files)
                if latest_modified_file:
                    logger.info(f"file {latest_modified_file} is the most recently updated file (at time={mtime})")
                else:
                    logger.warning('looping job algorithm failed to identify latest updated file')
                    return montime.ct_looping_last_touched, updated_files

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

    return montime.ct_looping_last_touched, updated_files


def get_looping_job_limit() -> int:
    """Return the time limit for looping job detection.

    Reads ``looping_limit_default`` and ``looping_limit_min_default`` from the
    pilot configuration and returns the larger of the two values to ensure the
    effective limit never falls below the configured minimum.

    Returns:
        Looping job time limit in seconds.
    """
    looping_limit = convert_to_int(config.Pilot.looping_limit_default, default=2 * 3600)
    looping_limit_min_default = convert_to_int(config.Pilot.looping_limit_min_default, default=2 * 3600)
    looping_limit = max(looping_limit, looping_limit_min_default)
    logger.info(f"using looping job limit: {looping_limit} s")

    return looping_limit
