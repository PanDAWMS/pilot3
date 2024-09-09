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
):
    """
    Containerise a general command by execution in a script that can be run in a container.

    :param job: job object (object)
    :param label: label (str)
    :param container_type: optional 'container/bash'
    :raises PilotException: for general failures.
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
):
    """
    Containerise the middleware by performing stage-in/out steps in a script that in turn can be run in a container.

    Note: a container will only be used for option container_type='container'. If this is 'bash', then stage-in/out
    will still be done by a script, but not containerised.

    Note: this function is tailormade for stage-in/out.

    :param job: job object (JobData)
    :param args: command line arguments (dict)
    :param xdata: list of FileSpec objects (list)
    :param eventtype: event type (str)
    :param localsite: local site name (str)
    :param remotesite: remote site name (str)
    :param label: optional 'stage-in/out' (str)
    :param container_type: optional 'container/bash' (str)
    :raises StageInFailure: for stage-in failures
    :raises StageOutFailure: for stage-out failures.
    """
    external_dir = args.input_dir if label == "stage-in" else args.output_dir

    # get the name of the stage-in/out isolation script
    script = (
        config.Container.middleware_container_stagein_script
        if label == "stage-in"
        else config.Container.middleware_container_stageout_script
    )

    try:
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
    """
    Return the path for the script.

    :param script: script name (str)
    :return: path (str).
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
):
    """
    Get the middleware container execution command.

    Note: this function is tailormade for stage-in/out.

    :param job: job object (JobData)
    :param xdata: list of FileSpec objects (list)
    :param queue: queue name (str)
    :param script: name of stage-in/out script (str)
    :param eventtype: event type (str)
    :param localsite: local site name (str)
    :param remotesite: remote site name (str)
    :param external_dir: input or output files directory (str)
    :param label: optional 'stage-[in|out]' (str)
    :param container_type: optional 'container/bash' (str)
    :param rucio_host: optional rucio host (str)
    :return: stage-in/out command (str)
    :raises PilotException: for stage-in/out related failures.
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
            f"--guids={filedata_dictionary['guids']}"
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


def handle_updated_job_object(job: JobData, xdata: list, label: str = "stage-in"):
    """
    Handle updated job object fields.

    :param job: job object (JobData)
    :param xdata: list of FileSpec objects (list)
    :param label: 'stage-in/out' (str)
    :raises: StageInFailure, StageOutFailure.
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
                error_code, msg=error_diag
            )
    else:
        msg = f"{label} file dictionary not found"
        logger.warning(msg)
        if label == "stage-in":
            raise StageInFailure(msg)
        raise StageOutFailure(msg)


def get_logfile_names(label: str) -> tuple[str, str]:
    """
    Get the proper names for the redirected stage-in/out logs.

    :param label: 'stage-[in|out]' (string)
    :return: 'stage[in|out]_stdout' (string), 'stage[in|out]_stderr' (string) (tuple).
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
    """
    Return a dictionary with LFNs, guids, scopes, datasets, ddmendpoints, etc.

    Note: this dictionary will be written to a file that will be read back by the stage-in script inside the container.
    Dictionary format:
        { lfn1: { 'guid': guid1, 'scope': scope1, 'dataset': dataset1, 'ddmendpoint': ddmendpoint1,
                  'filesize': filesize1, 'checksum': checksum1, 'allowlan': allowlan1, 'allowwan': allowwan1,
                  'directaccesslan': directaccesslan1, 'directaccesswan': directaccesswan1, 'istar': istar1,
                  'accessmode': accessmode1, 'storagetoken': storagetoken1}, lfn2: .. }

    :param data: job [in|out]data (list of FileSpec objects)
    :return: file dictionary (dict).
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
    """
    Return a dictionary with comma-separated list of LFNs, guids, scopes, datasets, ddmendpoints, etc.

    :param data: job [in|out]data (list of FileSpec objects)
    :return: {'lfns': lfns, ..} (dict).
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
    """
    Decide if the pilot should use a script for the stage-in/out.

    Check the container_type (from queuedata) if 'middleware' is set to 'container' or 'bash'.

    :param container_type: container type (str)
    :return: Boolean (True if middleware should be containerised) (bool).
    """
    return container_type in {"container", "bash"}
