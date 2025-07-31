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
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-17
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-25

"""This is the entry point for the PanDA Pilot, executed with 'python3 pilot.py <args>'."""

import argparse
import logging
import os
import sys
import threading
import time
from os import getcwd, chdir, environ
from os.path import exists, join
from re import match
from shutil import rmtree
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.common.pilotcache import get_pilot_cache
from pilot.info import infosys
from pilot.util.auxiliary import (
    convert_signal_to_exit_code,
    pilot_version_banner,
    shell_exit_code,
)
from pilot.util.batchsystem import is_htcondor_version_sufficient
from pilot.util.cgroups import create_cgroup
from pilot.util.config import config
from pilot.util.constants import (
    get_pilot_version,
    ERRNO_NOJOBS,
    FAILURE,
    PILOT_END_TIME,
    PILOT_MULTIJOB_START_TIME,
    PILOT_START_TIME,
    SERVER_UPDATE_NOT_DONE,
)
from pilot.util.cvmfs import (
    cvmfs_diagnostics,
    get_last_update,
    is_cvmfs_available,
)
from pilot.util.filehandling import (
    get_pilot_work_dir,
    mkdirs,
)
from pilot.util.harvester import (
    is_harvester_mode,
    kill_worker,
)
from pilot.util.heartbeat import update_pilot_heartbeat
from pilot.util.https import (
    get_panda_server,
    https_setup,
    send_update,
    update_local_oidc_token_info,
    get_memory_limits
)
from pilot.util.loggingsupport import establish_logging
from pilot.util.networking import dump_ipv6_info
from pilot.util.processgroups import find_defunct_subprocesses
from pilot.util.timing import add_to_pilot_timing
from pilot.util.workernode import (
    get_node_name,
    get_workernode_map,
    get_workernode_gpu_map
)

errors = ErrorCodes()
pilot_cache = get_pilot_cache()
mainworkdir = ""
args = None
trace = None


def main() -> int:  # noqa: C901
    """
    Prepare for and execute the requested workflow.

    :return: exit code (int).
    """
    # get the logger
    logger = logging.getLogger(__name__)

    # print the pilot version and other information
    pilot_version_banner()
    dump_ipv6_info()

    # define threading events
    args.graceful_stop = threading.Event()
    args.abort_job = threading.Event()
    args.job_aborted = threading.Event()

    # define useful variables
    args.retrieve_next_job = True  # go ahead and download a new job
    args.signal = None  # to store any incoming signals
    args.signal_counter = (
        0  # keep track of number of received kill signal (suicide counter)
    )
    args.kill_time = 0  # keep track of when first kill signal arrived

    # perform https setup
    if args.use_https:
        https_setup(args, get_pilot_version())
    args.amq = None

    # let the server know that the worker has started
    if args.update_server and args.workerpilotstatusupdate:
        send_worker_status(
            "started", args.queue, args.url, args.port, logger, "IPv6"
        )  # note: assuming IPv6, fallback in place

    # check cvmfs if available (skip test if either NO_CVMFS_OK env var is set or pilot option --nocvmfs is used)
    if args.cvmfs:
        ec = check_cvmfs(logger)
        if ec:
            cvmfs_diagnostics()
            return ec

    if not args.rucio_host:
        args.rucio_host = config.Rucio.host

    # initialize InfoService
    try:
        infosys.init(args.queue)
        pilot_cache.queuedata = infosys.queuedata
        pilot_cache.harvester_submitmode = args.harvester_submitmode.lower()

        # check if queue is ACTIVE
        if infosys.queuedata.state != "ACTIVE":
            logger.critical(
                f"specified queue is NOT ACTIVE: {infosys.queuedata.name} -- aborting"
            )
            return errors.PANDAQUEUENOTACTIVE
    except PilotException as error:
        logger.fatal(error)
        return error.get_error_code()

    # update the OIDC token if necessary (after queuedata has been downloaded, since PQ.catchall can contain instruction to prevent token renewal)
    if 'no_token_renewal' in infosys.queuedata.catchall or args.token_renewal is False:
        logger.info("OIDC token will not be renewed by the pilot")
    else:
        update_local_oidc_token_info(args.url, args.port)

    # create and report the worker node map
    if args.update_server and args.pilot_user.lower() == "atlas":  # only send info for atlas for now
        try:
            send_workernode_map(infosys.queuedata.site, args.url, args.port, "IPv6", logger)  # note: assuming IPv6, fallback in place
        except Exception as error:
            logger.warning(f"exception caught when sending workernode map: {error}")
        try:
            memory_limits = get_memory_limits(args.url, args.port)
        except Exception as error:
            logger.warning(f"exception caught when getting resource types: {error}")
        else:
            logger.debug(f"resource types: {memory_limits}")
            if memory_limits:
                pilot_cache.resource_types = memory_limits

    # handle special CRIC variables via params
    # internet protocol versions 'IPv4' or 'IPv6' can be set via CRIC PQ.params.internet_protocol_version
    # (must be defined per PQ if wanted). The pilot default is IPv6
    args.internet_protocol_version = (
        infosys.queuedata.params.get("internet_protocol_version", "IPv6")
        if infosys.queuedata.params
        else "IPv6"
    )
    environ["PILOT_IP_VERSION"] = args.internet_protocol_version

    # set the site name for rucio
    environ["PILOT_RUCIO_SITENAME"] = (
        os.environ.get("PILOT_RUCIO_SITENAME", "") or infosys.queuedata.site
    )
    logger.debug(f'PILOT_RUCIO_SITENAME={os.environ.get("PILOT_RUCIO_SITENAME")}')

    #os.environ['RUCIO_ACCOUNT'] = 'atlpilo1'
    #logger.warning(f"enforcing RUCIO_ACCOUNT={os.environ.get('RUCIO_ACCOUNT')}")

    # store the site name as set with a pilot option
    environ[
        "PILOT_SITENAME"
    ] = infosys.queuedata.resource  # args.site  # TODO: replace with singleton

    # set requested workflow
    logger.info(f"pilot arguments: {args}")
    workflow = __import__(
        f"pilot.workflow.{args.workflow}", globals(), locals(), [args.workflow], 0
    )

    # update the pilot heartbeat file
    update_pilot_heartbeat(time.time())

    # execute workflow
    try:
        exitcode = workflow.run(args)
    except Exception as exc:
        logger.fatal(f"main pilot function caught exception: {exc}")
        exitcode = None

    # let the server know that the worker has finished
    if args.update_server and args.workerpilotstatusupdate:
        send_worker_status(
            "finished",
            args.queue,
            args.url,
            args.port,
            logger,
            args.internet_protocol_version,
        )

    return exitcode


def check_cvmfs(logger: Any) -> int:
    """
    Check if cvmfs is available.

    :param logger: logging object.
    :return: exit code (int).
    """
    # skip all tests if required
    if os.environ.get("NO_CVMFS_OK", False):
        logger.info("skipping cvmfs checks")
        return 0

    is_available = is_cvmfs_available()
    if is_available is None:
        pass  # ignore this case
    elif is_available is True:
        timestamp = get_last_update()
        if timestamp and timestamp > 0:
            logger.info('CVMFS has been validated')
        else:
            logger.warning('CVMFS is not responding - aborting pilot')
            return errors.CVMFSISNOTALIVE
    else:
        logger.warning('CVMFS is not alive - aborting pilot')
        return errors.CVMFSISNOTALIVE

    return 0


def str2bool(var: str) -> bool:
    """
    Convert string to bool.

    :param var: string to be converted to bool (str)
    :return: converted string (bool).
    """
    if isinstance(var, bool):  # does this ever happen?
        return var

    if var.lower() in {"yes", "true", "t", "y", "1"}:
        ret = True
    elif var.lower() in {"no", "false", "f", "n", "0"}:
        ret = False
    else:
        raise argparse.ArgumentTypeError(f"boolean value expected (var={var})")

    return ret


def validate_resource_type(value: str) -> str:
    """
    Validate the resource type.

    :param value: resource type (str)
    :return: resource type (str)
    :raises: argparse.ArgumentTypeError if the resource type is invalid.
    """
    # Define the allowed patterns
    allowed_patterns = ["", "SCORE", "MCORE", "SCORE_*", "MCORE_*"]
    if value in allowed_patterns:
        return value
    # Check for pattern matching
    for pattern in allowed_patterns:
        if pattern.endswith('*') and match(f"^{pattern[:-1]}", value):
            return value
    raise argparse.ArgumentTypeError(f"Invalid resource type: {value}")


def get_args() -> Any:
    """
    Return the args from the arg parser.

    :return: args (arg parser object - type <class 'argparse.Namespace'>).
    """
    arg_parser = argparse.ArgumentParser()

    # pilot log creation
    arg_parser.add_argument(
        "--no-pilot-log",
        dest="nopilotlog",
        action="store_true",
        default=False,
        help="Do not write the pilot log to file",
    )

    # pilot work directory
    arg_parser.add_argument(
        "-a", "--workdir", dest="workdir", default="", help="Pilot work directory"
    )

    # debug option to enable more log messages
    arg_parser.add_argument(
        "-d",
        "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="Enable debug mode for logging messages",
    )

    # the choices must match in name the python module in pilot/workflow/
    arg_parser.add_argument(
        "-w",
        "--workflow",
        dest="workflow",
        default="generic",
        choices=[
            "generic",
            "generic_hpc",
            "production",
            "production_hpc",
            "analysis",
            "analysis_hpc",
            "eventservice_hpc",
            "stager",
            "payload_stageout",
        ],
        help="Pilot workflow (default: generic)",
    )

    # graciously stop pilot process after hard limit
    arg_parser.add_argument(
        "-l",
        "--lifetime",
        dest="lifetime",
        default=324000,
        required=False,
        type=int,
        help="Pilot lifetime seconds (default: 324000 s)",
    )
    arg_parser.add_argument(
        "-L",
        "--leasetime",
        dest="leasetime",
        default=3600,
        required=False,
        type=int,
        help="Pilot leasetime seconds (default: 3600 s)",
    )

    # Disabe cvmfs checks
    arg_parser.add_argument(
        "-b",
        "--nocvmfs",
        dest="cvmfs",
        action="store_false",
        default=True,
        help="Disable cvmfs checks",
    )

    # set the appropriate site, resource and queue
    arg_parser.add_argument(
        "-q",
        "--queue",
        dest="queue",
        required=True,
        help="MANDATORY: queue name (e.g., AGLT2_TEST-condor)",
    )
    arg_parser.add_argument(
        "-r",
        "--resource",
        dest="resource",
        required=False,  # From v 2.2.0 the resource name is internally set
        help="OBSOLETE: resource name (e.g., AGLT2_TEST)",
    )
    arg_parser.add_argument(
        "-s",
        "--site",
        dest="site",
        required=False,  # From v 2.2.1 the site name is internally set
        help="OBSOLETE: site name (e.g., AGLT2_TEST)",
    )
    arg_parser.add_argument(
        "-j",
        "--joblabel",
        dest="job_label",
        default="ptest",
        help="Job prod/source label (default: ptest)",
    )
    arg_parser.add_argument(
        "-g",
        "--baseurls",
        dest="baseurls",
        default="",
        help="Comma separated list of base URLs for validation of trf download",
    )

    # pilot version tag; PR or RC
    arg_parser.add_argument(
        "-i",
        "--versiontag",
        dest="version_tag",
        default="PR",
        help="Version tag (default: PR, optional: RC)",
    )

    arg_parser.add_argument(
        "-z",
        "--noserverupdate",
        dest="update_server",
        action="store_false",
        default=True,
        help="Disable server updates",
    )

    arg_parser.add_argument(
        "-k",
        "--noworkerpilotstatusupdate",
        dest="workerpilotstatusupdate",
        action="store_false",
        default=True,
        help="Disable updates to updateWorkerPilotStatus",
    )

    arg_parser.add_argument(
        "-t",
        "--noproxyverification",
        dest="verify_proxy",
        action="store_false",
        default=True,
        help="Disable proxy verification",
    )

    arg_parser.add_argument(
        "-u",
        "--verifypayloadproxy",
        dest="verify_payload_proxy",
        action="store_false",
        default=True,
        help="Disable payload proxy verification",
    )

    # graciously stop pilot process after hard limit
    arg_parser.add_argument(
        "-v",
        "--getjobrequests",
        dest="getjob_requests",
        default=2,
        required=False,
        type=int,
        help="Number of getjob requests",
    )

    arg_parser.add_argument(
        "-x",
        "--getjobfailures",
        dest="getjob_failures",
        default=5,
        required=False,
        type=int,
        help="Maximum number of getjob request failures in Harvester mode",
    )

    # no_token_renewal
    arg_parser.add_argument(
        "-y",
        "--notokenrenewal",
        dest="token_renewal",
        action="store_false",
        default=True,
        help="Disable token renewal",
    )

    arg_parser.add_argument(
        "--subscribe-to-msgsvc",
        dest="subscribe_to_msgsvc",
        action="store_true",
        default=False,
        required=False,
        help="Ask Pilot to receive job/task info from ActiveMQ",
    )

    # SSL certificates
    arg_parser.add_argument(
        "--cacert",
        dest="cacert",
        default=None,
        help="CA certificate to use with HTTPS calls to server, commonly X509 proxy",
        metavar="path/to/your/certificate",
    )
    arg_parser.add_argument(
        "--capath",
        dest="capath",
        default=None,
        help="CA certificates path",
        metavar="path/to/certificates/",
    )

    # Server URLs and ports
    arg_parser.add_argument(
        "--url",
        dest="url",
        default="",  # the proper default is stored in default.cfg
        help="PanDA server URL",
    )
    arg_parser.add_argument(
        "-p",
        "--port",
        dest="port",
        type=int,
        default=25443,
        help="PanDA server port"
    )
    arg_parser.add_argument(
        "--queuedata-url",
        dest="queuedata_url",
        default="",
        help="Queuedata server URL"
    )
    arg_parser.add_argument(
        "--storagedata-url",
        dest="storagedata_url",
        default="",
        help="URL for downloading DDM end points data",
    )
    arg_parser.add_argument(
        "--rucio-host",
        dest="rucio_host",
        default="",
        help="URL for the Rucio host (optional)",
    )
    arg_parser.add_argument(
        "--redirect-stdout",
        dest="redirectstdout",
        default="",
        help="Redirect all stdout to given file, or /dev/null (optional)",
    )

    # Country group
    arg_parser.add_argument(
        "--country-group",
        dest="country_group",
        default="",
        help="Country group option for getjob request",
    )

    # Working group
    arg_parser.add_argument(
        "--working-group",
        dest="working_group",
        default="",
        help="Working group option for getjob request",
    )

    # Allow other country
    arg_parser.add_argument(
        "--allow-other-country",
        dest="allow_other_country",
        type=str2bool,
        default=False,
        help="Is the resource allowed to be used outside the privileged group?",
    )

    # Allow same user
    arg_parser.add_argument(
        "--allow-same-user",
        dest="allow_same_user",
        type=str2bool,
        default=True,
        help="Multi-jobs will only come from same taskID (and thus same user)",
    )

    # Experiment
    arg_parser.add_argument(
        "--pilot-user",
        dest="pilot_user",
        default="generic",
        required=True,
        help="Pilot user (e.g. name of experiment corresponding to pilot plug-in)",
    )

    # Kubernetes (pilot running in a pod)
    arg_parser.add_argument(
        "--pod",
        dest="pod",
        action="store_true",
        default=False,
        help="Pilot running in a Kubernetes pod",
    )

    # Harvester specific options (if any of the following options are used, args.harvester will be set to True)
    arg_parser.add_argument(
        "--harvester-workdir",
        dest="harvester_workdir",
        default="",
        help="Harvester work directory",
    )
    arg_parser.add_argument(
        "--harvester-datadir",
        dest="harvester_datadir",
        default="",
        help="Harvester data directory",
    )
    arg_parser.add_argument(
        "--harvester-eventstatusdump",
        dest="harvester_eventstatusdump",
        default="",
        help="Harvester event status dump json file containing processing status",
    )
    arg_parser.add_argument(
        "--harvester-workerattributes",
        dest="harvester_workerattributes",
        default="",
        help="Harvester worker attributes json file containing job status",
    )
    arg_parser.add_argument(
        "--harvester-submit-mode",
        dest="harvester_submitmode",
        default="PULL",
        help="Harvester submit mode (PUSH or PULL [default])",
    )
    arg_parser.add_argument(
        "--resource-type",
        dest="resource_type",
        default="",
        type=validate_resource_type,
        help="Resource type; MCORE, SCORE or patterns like SCORE_* and MCORE_*",
    )
    arg_parser.add_argument(
        "--use-https",
        dest="use_https",
        type=str2bool,
        default=True,
        help="Use HTTPS protocol for communications with server",
    )
    arg_parser.add_argument(
        "--cleanup",
        dest="cleanup",
        type=str2bool,
        default=True,
        help="Cleanup work directory after pilot has finished",
    )
    arg_parser.add_argument(
        "--use-realtime-logging",
        dest="use_realtime_logging",
        action="store_true",
        default=False,
        help="Use near real-time logging, default=False",
    )
    arg_parser.add_argument(
        "--realtime-logging-server",
        dest="realtime_logging_server",
        default=None,
        help="Realtime logging server [type:host:port]; e.g. logstash:12.23.34.45:80",
    )
    # arg_parser.add_argument('--realtime-logfiles',
    #                         dest='realtime_logfiles',
    #                         default=None,
    #                         help='The log filenames to be sent to the realtime logging server')
    arg_parser.add_argument(
        "--realtime-logname",
        dest="realtime_logname",
        default=None,
        help="The log name in the realtime logging server",
    )

    # Harvester and Nordugrid specific options
    arg_parser.add_argument(
        "--input-dir", dest="input_dir", default="", help="Input directory"
    )
    arg_parser.add_argument(
        "--input-destination-dir",
        dest="input_destination_dir",
        default="",
        help="Input destination directory",
    )
    arg_parser.add_argument(
        "--output-dir", dest="output_dir", default="", help="Output directory"
    )
    arg_parser.add_argument(
        "--job-type", dest="jobtype", default="", help="Job type (managed, user)"
    )
    arg_parser.add_argument(
        "--use-rucio-traces",
        dest="use_rucio_traces",
        type=str2bool,
        default=True,
        help="Use rucio traces",
    )

    # HPC options
    arg_parser.add_argument(
        "--hpc-resource",
        dest="hpc_resource",
        default="",
        help="Name of the HPC (e.g. Titan)",
    )
    arg_parser.add_argument(
        "--hpc-mode",
        dest="hpc_mode",
        default="manytoone",
        help="HPC mode (manytoone, jumbojobs)",
    )
    arg_parser.add_argument(
        "--es-executor-type",
        dest="executor_type",
        default="generic",
        help="Event service executor type (generic, raythena)",
    )

    return arg_parser.parse_args()


def create_main_work_dir() -> (int, str):
    """
    Create and return the pilot's main work directory.

    The function also sets args.mainworkdir and cd's into this directory.
    Note: args, used in this function, is defined in outer scope.

    :return: exit code (int), main work directory (string).
    """
    exitcode = 0

    if args.workdir != "":
        _mainworkdir = get_pilot_work_dir(args.workdir)
        try:
            # create the main PanDA Pilot work directory
            mkdirs(_mainworkdir)
        except PilotException as error:
            # print to stderr since logging has not been established yet
            print(
                f"failed to create workdir at {_mainworkdir} -- aborting: {error}",
                file=sys.stderr,
            )
            exitcode = shell_exit_code(error._error_code)
    else:
        _mainworkdir = getcwd()

    args.mainworkdir = _mainworkdir
    chdir(_mainworkdir)

    return exitcode, _mainworkdir


def set_environment_variables():
    """
    Set relevant environment variables.

    This function sets PILOT_WORK_DIR, PILOT_HOME, PILOT_SITENAME, PILOT_USER and PILOT_VERSION and others.
    Note: args and mainworkdir, used in this function, are defined in outer scope.
    """
    # working directory as set with a pilot option (e.g. ..)
    environ["PILOT_WORK_DIR"] = args.workdir  # TODO: replace with singleton
    pilot_cache.pilot_work_dir = args.workdir

    # main work directory (e.g. /scratch/PanDA_Pilot3_3908_1537173670)
    environ["PILOT_HOME"] = mainworkdir  # TODO: replace with singleton
    pilot_cache.pilot_home_dir = mainworkdir

    # pilot source directory (e.g. /cluster/home/usatlas1/gram_scratch_hHq4Ns/condorg_oqmHdWxz)
    if not environ.get("PILOT_SOURCE_DIR", None):
        environ["PILOT_SOURCE_DIR"] = args.sourcedir  # TODO: replace with singleton
        pilot_cache.pilot_source_dir = args.sourcedir

    # set the pilot user (e.g. ATLAS)
    environ["PILOT_USER"] = args.pilot_user  # TODO: replace with singleton

    # internal pilot state
    environ["PILOT_JOB_STATE"] = "startup"  # TODO: replace with singleton
    pilot_cache.pilot_job_state = "startup"

    # set the pilot version
    environ["PILOT_VERSION"] = get_pilot_version()
    pilot_cache.pilot_version = get_pilot_version()

    # set the default wrap-up/finish instruction
    environ["PILOT_WRAP_UP"] = "NORMAL"

    # proxy verifications
    environ["PILOT_PROXY_VERIFICATION"] = f"{args.verify_proxy}"
    environ["PILOT_PAYLOAD_PROXY_VERIFICATION"] = f"{args.verify_payload_proxy}"

    # keep track of the server updates, if any
    environ["SERVER_UPDATE"] = SERVER_UPDATE_NOT_DONE

    # set the (HPC) resource name (if set in options)
    environ["PILOT_RESOURCE_NAME"] = args.hpc_resource

    # allow for the possibility of turning off rucio traces
    environ["PILOT_USE_RUCIO_TRACES"] = f"{args.use_rucio_traces}"

    # event service executor type
    environ["PILOT_ES_EXECUTOR_TYPE"] = args.executor_type

    if args.output_dir:
        environ["PILOT_OUTPUT_DIR"] = args.output_dir

    # keep track of the server urls
    environ["PANDA_SERVER_URL"] = get_panda_server(
        args.url, args.port, update_server=args.update_server
    )
    environ["QUEUEDATA_SERVER_URL"] = f"{args.queuedata_url}"
    if args.storagedata_url:
        environ["STORAGEDATA_SERVER_URL"] = f"{args.storagedata_url}"

    # should cgroups be used for process management?
    pilot_cache.use_cgroups = is_htcondor_version_sufficient() if args.pilot_user.lower() == 'atlas' else False

    # create a cgroup for the pilot
    if pilot_cache.use_cgroups:
        _ = create_cgroup()


def wrap_up() -> int:
    """
    Perform cleanup and terminate logging.

    Note: args and mainworkdir, used in this function, are defined in outer scope.

    :return: exit code (int).
    """
    # cleanup pilot workdir if created
    if args.sourcedir != mainworkdir and args.cleanup:
        chdir(args.sourcedir)
        try:
            rmtree(mainworkdir)
        except OSError as exc:
            logging.warning(f"failed to remove {mainworkdir}: {exc}")
        else:
            logging.info(f"removed {mainworkdir}")

    # in Harvester mode, create a kill_worker file that will instruct Harvester that the pilot has finished
    if args.harvester:
        kill_worker()

    exitcode, shellexitcode = get_proper_exit_code()
    logging.info(f"pilot has finished (exit code={exitcode}, shell exit code={shellexitcode})")
    logging.shutdown()

    return shellexitcode


def get_proper_exit_code() -> (int, int):
    """
    Return the proper exit code.

    :return: exit code (int), shell exit code (int).
    """
    try:
        exitcode = trace.pilot["error_code"]
    except (KeyError, AttributeError):
        exitcode = trace
        logging.debug(f"trace was not a class, trace={trace}")
    else:
        logging.info(f"traces error code: {exitcode}")
        if trace.pilot["nr_jobs"] <= 1:
            if exitcode != 0:
                logging.info(
                    f"an exit code was already set: {exitcode} (will be converted to a standard shell code)"
                )
        elif trace.pilot["nr_jobs"] > 0:
            if trace.pilot["nr_jobs"] == 1:
                logging.getLogger(__name__).info(
                    "pilot has finished 1 job was processed)"
                )
            else:
                logging.getLogger(__name__).info(
                    f"pilot has finished ({trace.pilot['nr_jobs']} jobs were processed)"
                )
        elif trace.pilot["state"] == FAILURE:
            logging.critical("pilot workflow failure -- aborting")
        elif trace.pilot["state"] == ERRNO_NOJOBS:
            logging.critical("pilot did not process any events -- aborting")
            exitcode = ERRNO_NOJOBS

    try:
        exitcode = int(exitcode)
    except TypeError as exc:
        logging.warning(f"failed to convert exit code to int: {exitcode}, {exc}")
        exitcode = 1008

    if exitcode == 0 and args.signal:
        exitcode = convert_signal_to_exit_code(args.signal)
    sec = shell_exit_code(exitcode)

    return exitcode, sec


def get_pilot_source_dir() -> str:
    """
    Return the pilot source directory.

    :return: full path to pilot source directory (string).
    """
    cwd = getcwd()
    if exists(
        join(join(cwd, "pilot3"), "pilot.py")
    ):  # in case wrapper has untarred src as pilot3 in init dir
        cwd = join(cwd, "pilot3")
    return cwd


def send_worker_status(
    status: str,
    queue: str,
    url: str,
    port: int,
    logger: Any,
    internet_protocol_version: str,
):
    """
    Send worker info to the server to let it know that the worker has started.

    Note: the function can fail, but if it does, it will be ignored.

    :param status: 'started' or 'finished' (str)
    :param queue: PanDA queue name (str)
    :param url: server url (str)
    :param port: server port (int)
    :param logger: logging object (object)
    :param internet_protocol_version: internet protocol version, IPv4 or IPv6 (str).
    """
    # worker node structure to be sent to the server
    data = {}
    data["workerID"] = os.environ.get("HARVESTER_WORKER_ID", None)
    data["harvesterID"] = os.environ.get("HARVESTER_ID", None)
    data["status"] = status
    data["site"] = queue
    data["node_id"] = get_node_name()

    # attempt to send the worker info to the server
    if data["workerID"] and data["harvesterID"]:
        send_update(
            "updateWorkerPilotStatus", data, url, port, ipv=internet_protocol_version, max_attempts=2
        )
    else:
        logger.warning("workerID/harvesterID not known, will not send worker status to server")


def send_workernode_map(
        site: str,
        url: str,
        port: int,
        internet_protocol_version: str,
        logger: Any,
):
    """
    Send worker node map and GPU info to the server.

    :param site: ATLAS site name (str)
    :param url: server url (str)
    :param port: server port (int)
    :param internet_protocol_version: internet protocol version, IPv4 or IPv6 (str)
    :param logger: logging object (object).
    """
    # worker node structure to be sent to the server
    try:
        data = get_workernode_map(site)
    except Exception as e:
        logger.warning(f"exception caught when calling get_workernode_map(): {e}")
    try:
        send_update("api/v1/pilot/update_worker_node", data, url, port, ipv=internet_protocol_version, max_attempts=1)
    except Exception as e:
        logger.warning(f"exception caught when sending worker node map to server: {e}")

    # GPU info
    try:
        data = get_workernode_gpu_map(site)
    except Exception as e:
        logger.warning(f"exception caught when calling get_workernode_gpu_map(): {e}")
    try:
        if data:  # only send if data is not empty
            send_update("api/v1/pilot/update_worker_node_gpu", data, url, port, ipv=internet_protocol_version, max_attempts=1)
    except Exception as e:
        logger.warning(f"exception caught when sending worker node map to server: {e}")


def set_lifetime():
    """Update the pilot lifetime if set by an environment variable (PANDAPILOT_LIFETIME) (in seconds)."""
    lifetime = os.environ.get("PANDAPILOT_LIFETIME", None)
    if lifetime:
        try:
            _lifetime = int(lifetime)
        except (ValueError, TypeError):
            pass
        else:
            args.lifetime = _lifetime


def set_redirectall():
    """
    Set args redirectall field.

    Currently not used.
    """
    redirectall = os.environ.get("PANDAPILOT_REDIRECTALL", False)
    if not redirectall:
        try:
            redirectall = bool(redirectall)
        except (ValueError, TypeError):
            pass
        else:
            args.redirectall = redirectall


def list_zombies():
    """
    Make sure there are no remaining defunct processes still lingering.

    Note: can be used to find zombies, but zombies can't be killed..
    """
    found = find_defunct_subprocesses(os.getpid())
    if found:
        logging.info(f"found these defunct processes: {found}")
    else:
        logging.info("no defunct processes were found")


if __name__ == "__main__":
    # get the args from the arg parser
    args = get_args()
    args.last_heartbeat = time.time()  # keep track of server heartbeats
    args.pilot_heartbeat = time.time()  # keep track of pilot heartbeats

    # Define and set the main harvester control boolean
    args.harvester = is_harvester_mode(args)

    # initialize the pilot timing dictionary
    args.timing = {}  # TODO: move to singleton?

    # initialize job status dictionary (e.g. used to keep track of log transfers)
    args.job_status = {}  # TODO: move to singleton or to job object directly?

    # if 'BNL_OSG_SPHENIX_TEST' in args.queue:
    #    args.lifetime = 3600
    #    args.subscribe_to_msgsvc = True
    #    args.redirectstdout = '/dev/null'

    # store T0 time stamp
    add_to_pilot_timing("0", PILOT_START_TIME, time.time(), args)
    add_to_pilot_timing("1", PILOT_MULTIJOB_START_TIME, time.time(), args)

    # if requested by the wrapper via a pilot option, create the main pilot workdir and cd into it
    args.sourcedir = getcwd()  # get_pilot_source_dir()

    exit_code, mainworkdir = create_main_work_dir()
    if exit_code != 0:
        sys.exit(exit_code)

    set_lifetime()

    # setup and establish standard logging
    establish_logging(
        debug=args.debug, nopilotlog=args.nopilotlog, redirectstdout=args.redirectstdout
    )

    # set environment variables (to be replaced with singleton implementation)
    set_environment_variables()

    # execute main function
    trace = main()

    # store final time stamp (cannot be placed later since the mainworkdir is about to be purged)
    add_to_pilot_timing("0", PILOT_END_TIME, time.time(), args, store=False)

    # make sure the pilot does not leave any lingering defunct child processes behind
    if args.debug:
        list_zombies()

    # perform cleanup and terminate logging
    exit_code = wrap_up()

    # the end.
    sys.exit(exit_code)
