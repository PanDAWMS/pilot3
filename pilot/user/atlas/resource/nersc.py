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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23

"""Resource related functions for NERSC."""

import logging
import os
from typing import Any

# from pilot.util.container import execute
from pilot.common.errorcodes import ErrorCodes

logger = logging.getLogger(__name__)
errors = ErrorCodes()


def verify_setup_command(cmd: str) -> (int, str):
    """
    Verify the setup command.

    :param cmd: command string to be verified (string).
    :return: pilot error code (int), diagnostics (string).
    """
    if not cmd:
        logger.debug('cmd is not used by this function')

    return 0, ""


def get_setup_command(job: Any, prepareasetup: bool) -> str:
    """
    Return the path to asetup command, the asetup command itself and add the options (if desired).

    If prepareasetup is False, the function will only return the path to the asetup script. It is then assumed
    to be part of the job parameters.

    Handle the case where environmental variables are set -
    HARVESTER_CONTAINER_RELEASE_SETUP_FILE, HARVESTER_LD_LIBRARY_PATH, HARVESTER_PYTHONPATH
    This will create the string need for the pilot to execute to setup the environment.

    :param job: job object (Any)
    :param prepareasetup: not used (bool)
    :return: setup command (str).
    """
    if not prepareasetup:
        logger.debug('prepareasetup is not used by this function')
    cmd = ""

    # return immediately if there is no release or if user containers are used
    if job.swrelease == 'NULL' or '--containerImage' in job.jobparams:
        logger.debug(f'get_setup_command return value: {cmd}')
        return cmd

    # test if environmental variable HARVESTER_CONTAINER_RELEASE_SETUP_FILE is defined
    setupfile = os.environ.get('HARVESTER_CONTAINER_RELEASE_SETUP_FILE', '')
    if setupfile != "":
        cmd = f"source {setupfile};"
        # test if HARVESTER_LD_LIBRARY_PATH is defined
        if os.environ.get('HARVESTER_LD_LIBRARY_PATH', '') != "":
            cmd += "export LD_LIBRARY_PATH=$HARVESTER_LD_LIBRARY_PATH:$LD_LIBRARY_PATH;"
        # test if HARVESTER_PYTHONPATH is defined
        if os.environ.get('HARVESTER_PYTHONPATH', '') != "":
            cmd += "export PYTHONPATH=$HARVESTER_PYTHONPATH:$PYTHONPATH;"
        #set FRONTIER_SERVER and ATLAS_POOLCOND_PATH for NERSC
        cmd += "export ATLAS_POOLCOND_PATH=/cvmfs/atlas-condb.cern.ch/repo/conditions;"
        cmd += ("export FRONTIER_SERVER="
                "\"(serverurl=http://atlasfrontier-ai.cern.ch:8000/atlr)"
                "(serverurl=http://atlasfrontier2-ai.cern.ch:8000/atlr)"
                "(serverurl=http://atlasfrontier1-ai.cern.ch:8000/atlr)"
                "(proxyurl=http://frontiercache.nersc.gov:3128)\"")

        logger.debug(f'get_setup_command return value: {cmd}')

    return cmd
