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
# Authors
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

from os import environ

# Pilot version
RELEASE = '3'   # released number should be fixed at 3 for Pilot 3
VERSION = '7'   # version number is '1' for first release, '0' until then, increased for bigger updates
REVISION = '0'  # revision number should be reset to '0' for every new version release, increased for small updates
BUILD = '36'     # build number should be reset to '1' for every new development cycle

SUCCESS = 0
FAILURE = 1

ERRNO_NOJOBS = 20

# Sorting order constants
UTILITY_BEFORE_PAYLOAD = 1
UTILITY_WITH_PAYLOAD = 2
UTILITY_AFTER_PAYLOAD_STARTED = 3
UTILITY_AFTER_PAYLOAD_STARTED2 = 4
UTILITY_AFTER_PAYLOAD_FINISHED = 5
UTILITY_AFTER_PAYLOAD_FINISHED2 = 6
UTILITY_BEFORE_STAGEIN = 7
UTILITY_WITH_STAGEIN = 8

# Timing constants that allow for additional constants to be defined for values before the pilot is started, ie for
# wrapper timing purposes.
PILOT_START_TIME = 'PILOT_START_TIME'
PILOT_MULTIJOB_START_TIME = 'PILOT_MULTIJOB_START_TIME'
PILOT_PRE_GETJOB = 'PILOT_PRE_GETJOB'
PILOT_POST_GETJOB = 'PILOT_POST_GETJOB'  # note: PILOT_POST_GETJOB corresponds to START_TIME in Pilot 1
PILOT_PRE_SETUP = 'PILOT_PRE_SETUP'
PILOT_POST_SETUP = 'PILOT_POST_SETUP'
PILOT_PRE_STAGEIN = 'PILOT_PRE_STAGEIN'
PILOT_POST_STAGEIN = 'PILOT_POST_STAGEIN'
PILOT_PRE_PAYLOAD = 'PILOT_PRE_PAYLOAD'
PILOT_POST_PAYLOAD = 'PILOT_POST_PAYLOAD'
PILOT_PRE_STAGEOUT = 'PILOT_PRE_STAGEOUT'
PILOT_POST_STAGEOUT = 'PILOT_POST_STAGEOUT'
PILOT_PRE_LOG_TAR = 'PILOT_PRE_LOG_TAR'
PILOT_POST_LOG_TAR = 'PILOT_POST_LOG_TAR'
PILOT_PRE_FINAL_UPDATE = 'PILOT_PRE_FINAL_UPDATE'
PILOT_POST_FINAL_UPDATE = 'PILOT_POST_FINAL_UPDATE'
PILOT_END_TIME = 'PILOT_END_TIME'
PILOT_KILL_SIGNAL = 'PILOT_KILL_SIGNAL'

# Keep track of log transfers
LOG_TRANSFER_NOT_DONE = 'NOT_DONE'
LOG_TRANSFER_IN_PROGRESS = 'IN_PROGRESS'
LOG_TRANSFER_DONE = 'DONE'
LOG_TRANSFER_FAILED = 'FAILED'

# Keep track of server updates
SERVER_UPDATE_NOT_DONE = 'NOT_DONE'
SERVER_UPDATE_RUNNING = 'RUNNING'
SERVER_UPDATE_UPDATING = 'UPDATING_FINAL'
SERVER_UPDATE_FINAL = 'DONE_FINAL'
SERVER_UPDATE_TROUBLE = 'LOST_HEARTBEAT'

# How long should the pilot wait before it should commit suicide after it has received a kill signal?
MAX_KILL_WAIT_TIME = 120  # twenty minutes


def get_pilot_version() -> str:
    """
    Return the current Pilot version string with the format <release>.<version>.<revision> (<build>).
    E.g. pilot_version = '2.1.3 (12)'
    :return: version string.
    """

    return f'{RELEASE}.{VERSION}.{REVISION}.{BUILD}'


def get_rucio_client_version() -> str:
    """
    Return the current Rucio client version string using the environmental variable ATLAS_LOCAL_RUCIOCLIENTS_VERSION.
    If the environmental variable is not set, then an empty string will be returned.

    :return: $ATLAS_LOCAL_RUCIOCLIENTS_VERSION (string).
    """

    return environ.get('ATLAS_LOCAL_RUCIOCLIENTS_VERSION', '')
