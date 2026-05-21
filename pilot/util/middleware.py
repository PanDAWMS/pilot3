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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-24

"""Middleware utilities for container and singularity command construction."""

from __future__ import annotations
import logging

from os import (
    environ,
    path,
)

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    PilotException,
    StageInFailure,
    StageOutFailure,
)
from pilot.info import JobData
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.filehandling import (
    copy,
    copy_pilot_source,
    read_json,
    write_json,
    write_file,
)

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def containerise_general_command(
    job: JobData,
    label: str = "command",
    container_type: str = "container",
) -> None:
    """Run a general command inside a container via the experiment plugin.

    Args:
        job: Job object providing the debug command and working directory.
        label: Human-readable label used in log messages.
        container_type: Execution mode; only ``'container'`` is currently
            supported (``'bash'`` raises ``PilotException``).

    Raises:
        PilotException: If the container command cannot be constructed or the
            ``container_type`` is unsupported.
    """
    if container_type == "container":
        # add bits and pieces needed to run the cmd in a container
        pilot_user = environ.get("PILOT_USER", "generic").lower()
        user = __import__(
            f"pilot.user.{pilot_user}.container", globals(), locals(), [pilot_user], 0
        )
        try:
            cmd = user.create_middleware_container_command(
                job, job.debug_command, label=label, proxy=False
            )
        except PilotException as exc:
            raise exc
    else:
        logger.warning("not yet implemented")
        raise PilotException

    try:
        logger.info(f"*** executing {label} (logging will be redirected) ***")
        exit_code, _, _ = execute(cmd, job=job, usecontainer=False)
    except Exception as exc:
        logger.info(f"*** {label} has failed ***")
        logger.warning(f"exception caught: {exc}")
    else:
        if exit_code == 0:
            logger.info(f"*** {label} has finished ***")
        else:
            logger.info(f"*** {label} has failed ***")
        logger.debug(f"{label} script returned exit_code={exit_code}")


def containerise_middleware(
    job: JobData,
    args: object,
    xdata: list,
    eventtype: str,
    localsite: str,
    remotesite: str,
    label: str = "stage-in",
    container_type: str = "container",
) -> None:
    """Run stage-in or stage-out in an isolation script, optionally inside a container.

    When *container_type* is ``'container'``, the script is wrapped in a
    container command via the experiment plugin. When it is ``'bash'``, the
    script runs as a plain bash process (not containerised).

    Args:
        job: Job object providing metadata, working directory, and queue info.
        args: Parsed pilot arguments with ``input_dir``, ``output_dir``,
            ``queue``, ``rucio_host``, and ``stageout_attempts`` attributes.
        xdata: List of :class:`~pilot.info.filespec.FileSpec` objects to
            stage in or out.
        eventtype: Event type string passed to the isolation script.
        localsite: Local site name passed to the isolation script.
        remotesite: Remote site name passed to the isolation script.
        label: Operation label; either ``'stage-in'`` or ``'stage-out'``.
        container_type: Execution mode; ``'container'`` or ``'bash'``.

    Raises:
        StageInFailure: If an error occurs during stage-in log writing or file
            status handling.
        StageOutFailure: If an error occurs during stage-out log writing or
            file status handling.
    """
    external_dir = args.input_dir if label == "stage-in" else args.output_dir

    # get the name of the stage-in/out isolation script
    script = (
        config.Container.middleware_container_stagein_script
        if label == "stage-in"
        else config.Container.middleware_container_stageout_script
    )

    try:
        stage_attempts = 1 if label == "stage-in" else args.stageout_attempts
        cmd = get_command(
            job,
            xdata,
            args.queue,
            script,
            eventtype,
            localsite,
            remotesite,
            external_dir,
            label=label,
            container_type=container_type,
            rucio_host=args.rucio_host,
            stage_attempts=stage_attempts,
        )
    except PilotException as exc:
        raise exc

    if container_type == "container":
        # add bits and pieces needed to run the cmd in a container
        pilot_user = environ.get("PILOT_USER", "generic").lower()
        user = __import__(
            f"pilot.user.{pilot_user}.container", globals(), locals(), [pilot_user], 0
        )
        try:
            cmd = user.create_middleware_container_command(job, cmd, label=label)
        except PilotException as exc:
            raise exc
    else:
        logger.warning(
            f"{label} will not be done in a container (but it will be done by a script)"
        )

    try:
        logger.info(f"*** executing {label} (logging will be redirected) ***")
        exit_code, stdout, stderr = execute(cmd, job=job, usecontainer=False)
    except Exception as exc:
        logger.info(f"*** {label} has failed ***")
        logger.warning(f"exception caught: {exc}")
    else:
        if exit_code == 0:
            logger.info(f"*** {label} has finished ***")
        else:
            logger.info(f"*** {label} has failed ***")
            logger.warning(f"stderr:\n{stderr}")
            logger.warning(f"stdout:\n{stdout}")
        logger.debug(f"{label} script returned exit_code={exit_code}")

        # write stdout+stderr to files
        try:
            _stdout_name, _stderr_name = get_logfile_names(label)
            write_file(path.join(job.workdir, _stdout_name), stdout, mute=False)
            write_file(path.join(job.workdir, _stderr_name), stderr, mute=False)
        except PilotException as exc:
            msg = f"exception caught: {exc}"
            if label == "stage-in":
                raise StageInFailure(msg) from exc
            raise StageOutFailure(msg) from exc

    # handle errors, file statuses, etc (the stage-in/out scripts write errors and file status to a json file)
    try:
        handle_updated_job_object(job, xdata, label=label)
    except PilotException as exc:
        raise exc


def get_script_path(script: str) -> str:
    """Return the absolute path to a pilot script.

    Searches under ``PILOT_SOURCE_DIR/pilot/scripts`` and falls back to
    ``PILOT_SOURCE_DIR/pilot3/pilot/scripts``.

    Args:
        script: Script filename (e.g. ``'stagein.py'``).

    Returns:
        Absolute path to the script, or an empty string if not found.
    """
    srcdir = environ.get("PILOT_SOURCE_DIR", ".")
    _path = path.join(srcdir, "pilot/scripts")
    if not path.exists(_path):
        _path = path.join(srcdir, "pilot3")
        _path = path.join(_path, "pilot/scripts")
    _path = path.join(_path, script)
    if not path.exists(_path):
        _path = ""

    return _path


def get_command(
    job: JobData,
    xdata: list,
    queue: str,
    script: str,
    eventtype: str,
    localsite: str,
    remotesite: str,
    external_dir: str,
    label: str = "stage-in",
    container_type: str = "container",
    rucio_host: str = "",
    stage_attempts: int = 1,
) -> str:
    """Build the middleware container execution command for stage-in or stage-out.

    Args:
        job: Job object providing metadata and working directory.
        xdata: List of :class:`~pilot.info.filespec.FileSpec` objects to
            stage in or out.
        queue: PanDA queue name.
        script: Filename of the stage-in/out isolation script.
        eventtype: Event type string passed to the script.
        localsite: Local site name passed to the script.
        remotesite: Remote site name passed to the script.
        external_dir: Directory for input (stage-in) or output (stage-out) files.
        label: Operation label; either ``'stage-in'`` or ``'stage-out'``.
        container_type: Execution mode; ``'container'`` or ``'bash'``.
        rucio_host: Optional Rucio host override passed to the script.
        stage_attempts: Number of allowed stage-out attempts.

    Returns:
        Full command string ready for execution.

    Raises:
        PilotException: If the replica dictionary cannot be written, the pilot
            source cannot be copied, or the container command cannot be
            constructed.
    """
    if label == "stage-out":
        filedata_dictionary = get_filedata_strings(xdata)
    else:
        filedata_dictionary = get_filedata(xdata)

        # write file data to file
        status = write_json(
            path.join(job.workdir, config.Container.stagein_replica_dictionary),
            filedata_dictionary,
        )
        if not status:
            diagnostics = "failed to write replica dictionary to file"
            logger.warning(diagnostics)
            raise PilotException(diagnostics)

    # copy pilot source into container directory, unless it is already there
    diagnostics = copy_pilot_source(job.workdir)
    if diagnostics:
        raise PilotException(diagnostics)

    final_script_path = path.join(job.workdir, script)
    environ["PYTHONPATH"] = environ.get("PYTHONPATH") + ":" + job.workdir
    script_path = path.join("pilot/scripts", script)
    full_script_path = path.join(path.join(job.workdir, script_path))
    copy(full_script_path, final_script_path)

    if container_type == "container":
        # correct the path when containers have been used
        final_script_path = path.join(".", script)
        workdir = "/srv"
    else:
        # for container_type=bash we need to add the rucio setup
        pilot_user = environ.get("PILOT_USER", "generic").lower()
        user = __import__(
            f"pilot.user.{pilot_user}.container", globals(), locals(), [pilot_user], 0
        )
        try:
            final_script_path = user.get_middleware_container_script(
                "", final_script_path, asetup=True
            )
        except PilotException:
            final_script_path = f"python {final_script_path}"
        workdir = job.workdir

    cmd = (
        f'{final_script_path} -d -w {workdir} -q {queue} --eventtype={eventtype} --localsite={localsite} '
        f'--remotesite={remotesite} --produserid="{job.produserid.replace(" ", "%20")}" --jobid={job.jobid}'
    )

    if label == "stage-in":
        cmd += (
            f" --eventservicemerge={job.is_eventservicemerge} --usepcache={job.infosys.queuedata.use_pcache} "
            f"--usevp={job.use_vp} --replicadictionary={config.Container.stagein_replica_dictionary}"
        )
        if external_dir:
            cmd += f" --inputdir={external_dir}"
    else:  # stage-out
        cmd += (
            f" --lfns={filedata_dictionary['lfns']} --scopes={filedata_dictionary['scopes']} "
            f"--datasets={filedata_dictionary['datasets']} --ddmendpoints={filedata_dictionary['ddmendpoints']} "
            f"--guids={filedata_dictionary['guids']} --stageout-attempts={stage_attempts} "
        )
        if external_dir:
            cmd += f" --outputdir={external_dir}"

    cmd += f" --taskid={job.taskid}"
    cmd += f" --jobdefinitionid={job.jobdefinitionid}"
    cmd += f" --catchall='{job.infosys.queuedata.catchall}'"
    cmd += f" --rucio_host='{rucio_host}'"

    if container_type == "bash":
        cmd += "\nexit $?"

    return cmd


def handle_updated_job_object(job: JobData, xdata: list, label: str = "stage-in") -> None:
    """Update job and file-spec state from the stage-in/out status JSON file.

    Reads the status dictionary written by the isolation script and propagates
    file status, URLs, checksums, and error codes back to *job* and *xdata*.

    Args:
        job: Job object whose error codes will be updated on failure.
        xdata: List of :class:`~pilot.info.filespec.FileSpec` objects to
            update with status, TURL, SURL, and checksum values.
        label: Operation label; either ``'stage-in'`` or ``'stage-out'``.

    Raises:
        StageInFailure: If the status file cannot be parsed during stage-in.
        StageOutFailure: If the status file cannot be parsed during stage-out.
    """
    dictionary_name = (
        config.Container.stagein_status_dictionary
        if label == "stage-in"
        else config.Container.stageout_status_dictionary
    )

    # read the JSON file created by the stage-in/out script
    if path.exists(path.join(job.workdir, dictionary_name + ".log")):
        dictionary_name += ".log"
    file_dictionary = read_json(path.join(job.workdir, dictionary_name))

    # update the job object accordingly
    if file_dictionary:
        # get file info and set essential parameters
        for fspec in xdata:
            try:
                fspec.status = file_dictionary[fspec.lfn][0]
                fspec.status_code = file_dictionary[fspec.lfn][1]
                if label == "stage-in":
                    fspec.turl = file_dictionary[fspec.lfn][2]
                    fspec.ddmendpoint = file_dictionary[fspec.lfn][3]
                else:
                    fspec.surl = file_dictionary[fspec.lfn][2]
                    fspec.turl = file_dictionary[fspec.lfn][3]
                    fspec.checksum[config.File.checksum_type] = file_dictionary[
                        fspec.lfn
                    ][4]
                    fspec.filesize = file_dictionary[fspec.lfn][5]
            except Exception as exc:
                msg = f"exception caught while reading file dictionary: {exc}"
                logger.warning(msg)
                if label == "stage-in":
                    raise StageInFailure(msg) from exc
                raise StageOutFailure(msg) from exc

        # get main error info ('error': [error_diag, error_code])
        error_diag = file_dictionary["error"][0]
        error_code = file_dictionary["error"][1]
        if error_code:
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(
                int(error_code), msg=error_diag
            )
    else:
        msg = f"{label} file dictionary not found"
        logger.warning(msg)
        if label == "stage-in":
            raise StageInFailure(msg)
        raise StageOutFailure(msg)


def get_logfile_names(label: str) -> tuple[str, str]:
    """Return the stdout and stderr log file names for a stage-in or stage-out operation.

    Names are read from the pilot config and fall back to hard-coded defaults
    when the config values are empty.

    Args:
        label: Operation label; either ``'stage-in'`` or ``'stage-out'``.

    Returns:
        Tuple of ``(stdout_filename, stderr_filename)``.
    """
    if label == "stage-in":
        _stdout_name = config.Container.middleware_stagein_stdout
        _stderr_name = config.Container.middleware_stagein_stderr
    else:
        _stdout_name = config.Container.middleware_stageout_stdout
        _stderr_name = config.Container.middleware_stageout_stderr
    if not _stdout_name:
        _stdout_name = (
            "stagein_stdout.txt" if label == "stage-in" else "stageout_stdout.txt"
        )
    if not _stderr_name:
        _stderr_name = (
            "stagein_stderr.txt" if label == "stage-in" else "stageout_stderr.txt"
        )

    return _stdout_name, _stderr_name


def get_filedata(data: list) -> dict:
    """Return a per-LFN file metadata dictionary for the stage-in container script.

    The returned dictionary is written to a JSON file and read back by the
    isolation script running inside the container. The format is::

        {
            lfn1: {'guid': .., 'scope': .., 'dataset': .., 'ddmendpoint': ..,
                   'filesize': .., 'checksum': .., 'allowlan': .., 'allowwan': ..,
                   'directaccesslan': .., 'directaccesswan': .., 'istar': ..,
                   'accessmode': .., 'storagetoken': ..},
            lfn2: ..
        }

    Args:
        data: List of :class:`~pilot.info.filespec.FileSpec` objects (job input
            or output data).

    Returns:
        Dictionary mapping LFN strings to their metadata dicts.
    """
    file_dictionary = {}
    for fspec in data:
        try:
            _type = (
                "md5"
                if ("md5" in fspec.checksum and "adler32" not in fspec.checksum)
                else "adler32"
            )
            file_dictionary[fspec.lfn] = {
                "guid": fspec.guid,
                "scope": fspec.scope,
                "dataset": fspec.dataset,
                "ddmendpoint": fspec.ddmendpoint,
                "filesize": fspec.filesize,
                "checksum": fspec.checksum.get(_type, "None"),
                "allowlan": fspec.allow_lan,
                "allowwan": fspec.allow_wan,
                "directaccesslan": fspec.direct_access_lan,
                "directaccesswan": fspec.direct_access_wan,
                "istar": fspec.is_tar,
                "accessmode": fspec.accessmode,
                "storagetoken": fspec.storage_token,
            }
        except Exception as exc:
            logger.warning(f"exception caught in get_filedata(): {exc}")

    return file_dictionary


def get_filedata_strings(data: list) -> dict:
    """Return a dictionary of comma-separated file attribute strings for the stage-out script.

    Encodes per-file attributes (LFNs, GUIDs, scopes, datasets, DDM endpoints,
    file sizes, checksums, access flags, and storage tokens) as flat
    comma-joined strings ready to be passed as command-line arguments.

    Args:
        data: List of :class:`~pilot.info.filespec.FileSpec` objects (job
            output data).

    Returns:
        Dictionary with keys ``'lfns'``, ``'guids'``, ``'scopes'``,
        ``'datasets'``, ``'ddmendpoints'``, ``'filesizes'``, ``'checksums'``,
        ``'allowlans'``, ``'allowwans'``, ``'directaccesslans'``,
        ``'directaccesswans'``, ``'istars'``, ``'accessmodes'``,
        ``'storagetokens'``, each containing a comma-separated string.
    """
    lfns = ""
    guids = ""
    scopes = ""
    datasets = ""
    ddmendpoints = ""
    filesizes = ""
    checksums = ""
    allowlans = ""
    allowwans = ""
    directaccesslans = ""
    directaccesswans = ""
    istars = ""
    accessmodes = ""
    storagetokens = ""
    for fspec in data:
        lfns = fspec.lfn if lfns == "" else lfns + f",{fspec.lfn}"
        guids = fspec.guid if guids == "" else guids + f",{fspec.guid}"
        scopes = fspec.scope if scopes == "" else scopes + f",{fspec.scope}"
        datasets = fspec.dataset if datasets == "" else datasets + f",{fspec.dataset}"
        ddmendpoints = (
            fspec.ddmendpoint
            if ddmendpoints == ""
            else ddmendpoints + f",{fspec.ddmendpoint}"
        )
        filesizes = (
            str(fspec.filesize)
            if filesizes == ""
            else filesizes + f",{fspec.filesize}"
        )
        _type = (
            "md5"
            if ("md5" in fspec.checksum and "adler32" not in fspec.checksum)
            else "adler32"
        )
        checksums = (
            fspec.checksum.get(_type, "None")
            if checksums == ""
            else checksums + f",{fspec.checksum.get(_type)}"
        )
        allowlans = (
            str(fspec.allow_lan)
            if allowlans == ""
            else allowlans + f",{fspec.allow_lan}"
        )
        allowwans = (
            str(fspec.allow_wan)
            if allowwans == ""
            else allowwans + f",{fspec.allow_wan}"
        )
        directaccesslans = (
            str(fspec.direct_access_lan)
            if directaccesslans == ""
            else directaccesslans + f",{fspec.direct_access_lan}"
        )
        directaccesswans = (
            str(fspec.direct_access_wan)
            if directaccesswans == ""
            else directaccesswans + f",{fspec.direct_access_wan}"
        )
        istars = str(fspec.is_tar) if istars == "" else istars + f",{fspec.is_tar}"
        _accessmode = fspec.accessmode if fspec.accessmode else "None"
        accessmodes = (
            _accessmode if accessmodes == "" else accessmodes + f",{_accessmode}"
        )
        _storagetoken = fspec.storage_token if fspec.storage_token else "None"
        storagetokens = (
            _storagetoken
            if storagetokens == ""
            else storagetokens + f",{_storagetoken}"
        )

    return {
        "lfns": lfns,
        "guids": guids,
        "scopes": scopes,
        "datasets": datasets,
        "ddmendpoints": ddmendpoints,
        "filesizes": filesizes,
        "checksums": checksums,
        "allowlans": allowlans,
        "allowwans": allowwans,
        "directaccesslans": directaccesslans,
        "directaccesswans": directaccesswans,
        "istars": istars,
        "accessmodes": accessmodes,
        "storagetokens": storagetokens,
    }


def use_middleware_script(container_type: str) -> bool:
    """Decide whether the pilot should use an isolation script for stage-in/out.

    Args:
        container_type: The ``middleware`` value from queue data; typically
            ``'container'``, ``'bash'``, or an empty string.

    Returns:
        True if *container_type* is ``'container'`` or ``'bash'``, False
        otherwise.
    """
    return container_type in {"container", "bash"}
