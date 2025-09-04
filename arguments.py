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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025

"""Argument parser for the pilot command line interface."""

import argparse
from typing import Any
from re import match


def str2bool(var: str) -> bool:
    """
    Convert string to bool.

    Args:
        var (str): String to be converted to bool.

    Returns:
        bool: Converted boolean value.
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

    Args:
        value (str): Resource type.

    Returns:
        str: Validated resource type.

    Raises:
        argparse.ArgumentTypeError: If the resource type is invalid.
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


def add_main_args(parser: argparse.ArgumentParser) -> None:
    """
    Add main arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the main arguments will be added.
    """
    parser.add_argument(
        "--pilot-user",
        dest="pilot_user",
        default="generic",
        required=True,
        help="Pilot user (e.g. name of experiment corresponding to pilot plug-in)",
    )
    parser.add_argument(
        "-a", "--workdir",
        dest="workdir",
        default="",
        help="Pilot work directory",
    )
    parser.add_argument(
        "--cleanup",
        dest="cleanup",
        type=str2bool,
        default=True,
        help="Cleanup work directory after pilot has finished",
    )
    parser.add_argument(
        "-i",
        "--versiontag",
        dest="version_tag",
        default="PR",
        help="Version tag (default: PR, optional: RC)",
    )
    parser.add_argument(
        "-z",
        "--noserverupdate",
        dest="update_server",
        action="store_false",
        default=True,
        help="Disable server updates",
    )


def add_logging_args(parser: argparse.ArgumentParser) -> None:
    """
    Add logging related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the logging arguments will be added.
    """
    parser.add_argument(
        "--no-pilot-log",
        dest="nopilotlog",
        action="store_true",
        default=False,
        help="Do not write the pilot log to file",
    )
    parser.add_argument(
        "-d", "--debug",
        dest="debug",
        action="store_true",
        default=False,
        help="Enable debug mode for logging messages",
    )
    parser.add_argument(
        "--redirect-stdout",
        dest="redirectstdout",
        default="",
        help="Redirect all stdout to given file, or /dev/null (optional)",
    )


def add_job_args(parser: argparse.ArgumentParser) -> None:
    """
    Add job related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the job arguments will be added.
    """
    parser.add_argument(
        "-j",
        "--joblabel",
        dest="job_label",
        default="ptest",
        help="Job prod/source label (default: ptest)",
    )
    parser.add_argument(
        "-v",
        "--getjobrequests",
        dest="getjob_requests",
        default=2,
        required=False,
        type=int,
        help="Number of getjob requests",
    )
    parser.add_argument(
        "-x",
        "--getjobfailures",
        dest="getjob_failures",
        default=5,
        required=False,
        type=int,
        help="Maximum number of getjob request failures in Harvester mode",
    )
    parser.add_argument(
        "--country-group",
        dest="country_group",
        default="",
        help="Country group option for getjob request",
    )
    parser.add_argument(
        "--working-group",
        dest="working_group",
        default="",
        help="Working group option for getjob request",
    )
    parser.add_argument(
        "--allow-other-country",
        dest="allow_other_country",
        type=str2bool,
        default=False,
        help="Is the resource allowed to be used outside the privileged group?",
    )
    parser.add_argument(
        "--allow-same-user",
        dest="allow_same_user",
        type=str2bool,
        default=True,
        help="Multi-jobs will only come from same taskID (and thus same user)",
    )
    parser.add_argument(
        "--job-type",
        dest="jobtype",
        default="",
        help="Job type (managed, user)"
    )


def add_workflow_args(parser: argparse.ArgumentParser) -> None:
    """
    Add workflow related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the workflow arguments will be added.
    """
    parser.add_argument(
        "-w", "--workflow",
        dest="workflow",
        default="generic",
        choices=[
            "generic", "generic_hpc", "production", "production_hpc",
            "analysis", "analysis_hpc", "eventservice_hpc",
            "stager", "payload_stageout",
        ],
        help="Pilot workflow (default: generic)",
    )


def add_lifetime_args(parser: argparse.ArgumentParser) -> None:
    """
    Add arguments for pilot lifetime and leasetime.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the arguments will be added.
    """
    parser.add_argument(
        "-l", "--lifetime",
        dest="lifetime",
        type=int,
        default=324000,
        help="Pilot lifetime seconds (default: 324000 s)",
    )
    parser.add_argument(
        "-L", "--leasetime",
        dest="leasetime",
        type=int,
        default=3600,
        help="Pilot leasetime seconds (default: 3600 s)",
    )


def add_queue_args(parser: argparse.ArgumentParser) -> None:
    """
    Add queue related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the queue arguments will be added.
    """
    parser.add_argument(
        "-q", "--queue",
        dest="queue",
        required=True,
        help="MANDATORY: queue name (e.g., AGLT2_TEST-condor)",
    )
    parser.add_argument(
        "-r",
        "--resource",
        dest="resource",
        required=False,  # From v 2.2.0 the resource name is internally set
        help="OBSOLETE: resource name (e.g., AGLT2_TEST)",
    )
    parser.add_argument(
        "-s",
        "--site",
        dest="site",
        required=False,  # From v 2.2.1 the site name is internally set
        help="OBSOLETE: site name (e.g., AGLT2_TEST)",
    )
    parser.add_argument(
        "--resource-type",
        dest="resource_type",
        default="",
        type=validate_resource_type,
        help="Resource type; MCORE, SCORE or patterns like SCORE_* and MCORE_*",
    )
    parser.add_argument(
        "-b",
        "--nocvmfs",
        dest="cvmfs",
        action="store_false",
        default=True,
        help="Disable cvmfs checks",
    )
    parser.add_argument(
        "--stageout-attempts",
        dest="stageout_attempts",
        type=int,
        default=2,
        help="Number of stage-out attempts per output file (default: 1)",
    )


def add_harvester_args(parser: argparse.ArgumentParser) -> None:
    """
    Add Harvester related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the Harvester arguments will be added.
    """
    parser.add_argument(
        "--harvester-workdir",
        dest="harvester_workdir",
        default="",
        help="Harvester work directory",
    )
    parser.add_argument(
        "--harvester-datadir",
        dest="harvester_datadir",
        default="",
        help="Harvester data directory",
    )
    parser.add_argument(
        "--harvester-eventstatusdump",
        dest="harvester_eventstatusdump",
        default="",
        help="Harvester event status dump json file containing processing status",
    )
    parser.add_argument(
        "--harvester-workerattributes",
        dest="harvester_workerattributes",
        default="",
        help="Harvester worker attributes json file containing job status",
    )
    parser.add_argument(
        "--harvester-submit-mode",
        dest="harvester_submitmode",
        default="PULL",
        help="Harvester submit mode (PUSH or PULL [default])",
    )
    parser.add_argument(
        "--input-dir",
        dest="input_dir",
        default="",
        help="Input directory"
    )
    parser.add_argument(
        "--input-destination-dir",
        dest="input_destination_dir",
        default="",
        help="Input destination directory",
    )
    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        default="",
        help="Output directory"
    )


def add_hpc_args(parser: argparse.ArgumentParser) -> None:
    """
    Add HPC related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the HPC arguments will be added.
    """
    parser.add_argument(
        "--hpc-resource",
        dest="hpc_resource",
        default="",
        help="Name of the HPC (e.g. Titan)",
    )
    parser.add_argument(
        "--hpc-mode",
        dest="hpc_mode",
        default="manytoone",
        help="HPC mode (manytoone, jumbojobs)",
    )
    parser.add_argument(
        "--es-executor-type",
        dest="executor_type",
        default="generic",
        help="Event service executor type (generic, raythena)",
    )
    parser.add_argument(
        "--pod",
        dest="pod",
        action="store_true",
        default=False,
        help="Pilot running in a Kubernetes pod",
    )


def add_proxy_args(parser: argparse.ArgumentParser) -> None:
    """
    Add proxy related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the proxy arguments will be added.
    """
    parser.add_argument(
        "-t",
        "--noproxyverification",
        dest="verify_proxy",
        action="store_false",
        default=True,
        help="Disable proxy verification",
    )
    parser.add_argument(
        "-u",
        "--verifypayloadproxy",
        dest="verify_payload_proxy",
        action="store_false",
        default=True,
        help="Disable payload proxy verification",
    )
    # no_token_renewal
    parser.add_argument(
        "-y",
        "--notokenrenewal",
        dest="token_renewal",
        action="store_false",
        default=True,
        help="Disable token renewal",
    )
    parser.add_argument(
        "--cacert",
        dest="cacert",
        default=None,
        help="CA certificate to use with HTTPS calls to server, commonly X509 proxy",
        metavar="path/to/your/certificate",
    )
    parser.add_argument(
        "--capath",
        dest="capath",
        default=None,
        help="CA certificates path",
        metavar="path/to/certificates/",
    )


def add_server_args(parser: argparse.ArgumentParser) -> None:
    """
    Add server related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the server arguments will be added.
    """
    parser.add_argument(
        "--url",
        dest="url",
        default="",  # the proper default is stored in default.cfg
        help="PanDA server URL",
    )
    parser.add_argument(
        "-p",
        "--port",
        dest="port",
        type=int,
        default=25443,
        help="PanDA server port"
    )
    parser.add_argument(
        "--queuedata-url",
        dest="queuedata_url",
        default="",
        help="Queuedata server URL"
    )
    parser.add_argument(
        "--storagedata-url",
        dest="storagedata_url",
        default="",
        help="URL for downloading DDM end points data",
    )
    parser.add_argument(
        "-k",
        "--noworkerpilotstatusupdate",
        dest="workerpilotstatusupdate",
        action="store_false",
        default=True,
        help="Disable updates to updateWorkerPilotStatus",
    )
    parser.add_argument(
        "--subscribe-to-msgsvc",
        dest="subscribe_to_msgsvc",
        action="store_true",
        default=False,
        required=False,
        help="Ask Pilot to receive job/task info from ActiveMQ",
    )
    parser.add_argument(
        "--use-https",
        dest="use_https",
        type=str2bool,
        default=True,
        help="Use HTTPS protocol for communications with server",
    )
    parser.add_argument(
        "-g",
        "--baseurls",
        dest="baseurls",
        default="",
        help="Comma separated list of base URLs for validation of trf download",
    )


def add_realtimelogging_args(parser: argparse.ArgumentParser) -> None:
    """
    Add arguments for near real-time logging to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the arguments will be added.
    """
    parser.add_argument(
        "--use-realtime-logging",
        dest="use_realtime_logging",
        action="store_true",
        default=False,
        help="Use near real-time logging, default=False",
    )
    parser.add_argument(
        "--realtime-logging-server",
        dest="realtime_logging_server",
        default=None,
        help="Realtime logging server [type:host:port]; e.g. logstash:12.23.34.45:80",
    )
    # parser.add_argument('--realtime-logfiles',
    #                         dest='realtime_logfiles',
    #                         default=None,
    #                         help='The log filenames to be sent to the realtime logging server')
    parser.add_argument(
        "--realtime-logname",
        dest="realtime_logname",
        default=None,
        help="The log name in the realtime logging server",
    )


def add_rucio_args(parser: argparse.ArgumentParser) -> None:
    """
    Add Rucio related arguments to the argument parser.

    Args:
        parser (argparse.ArgumentParser): The argument parser to which the Rucio arguments will be added.
    """
    parser.add_argument(
        "--use-rucio-traces",
        dest="use_rucio_traces",
        type=str2bool,
        default=True,
        help="Use rucio traces",
    )
    parser.add_argument(
        "--rucio-host",
        dest="rucio_host",
        default="",
        help="URL for the Rucio host (optional)",
    )


def get_args() -> Any:
    """
    Return the args from the arg parser.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser()

    # Add argument groups
    add_main_args(parser)
    add_logging_args(parser)
    add_job_args(parser)
    add_workflow_args(parser)
    add_lifetime_args(parser)
    add_queue_args(parser)
    add_harvester_args(parser)
    add_hpc_args(parser)
    add_proxy_args(parser)
    add_server_args(parser)
    add_realtimelogging_args(parser)
    add_rucio_args(parser)

    return parser.parse_args()
