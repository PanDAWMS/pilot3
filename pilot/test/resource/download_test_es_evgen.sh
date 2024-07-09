#!/bin/bash
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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017

#ABSOLUTE_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")"
#ABSOLUTE_DIR="$(dirname "$ABSOLUTE_PATH")"
ABSOLUTE_DIR=/tmp/test_pilot3
FILE=${ABSOLUTE_DIR}/EVNT.08716373._000060.pool.root.1

if [ -f $FILE ]; then
    echo "File $FILE exists."
else
    export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase
    source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh
    source ${ATLAS_LOCAL_ROOT_BASE}/utilities/oldAliasSetup.sh rucio --skipConfirm --quiet

    rucio download --dir $ABSOLUTE_DIR --no-subdir mc15_13TeV:EVNT.08716373._000060.pool.root.1
fi
