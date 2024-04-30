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
# - Paul Nilsson, paul.nilsson@cern.ch, 2024

"""User specific functions/variables related to CVMFS operations."""

from .setup import get_file_system_root_path

# CVMFS mount points
cvmfs_mount_points = [
    'CVMFS_BASE/atlas.cern.ch/repo/sw',
    'CVMFS_BASE/atlas.cern.ch/repo/ATLASLocalRootBase/logDir/lastUpdate',
    'CVMFS_BASE/atlas-condb.cern.ch/repo/conditions/logDir/lastUpdate',
    'CVMFS_BASE/atlas-nightlies.cern.ch/repo/sw/logs/lastUpdate',
    'CVMFS_BASE/sft.cern.ch/lcg/lastUpdate',
    'CVMFS_BASE/unpacked.cern.ch/logDir/lastUpdate',
    'CVMFS_BASE/sft-nightlies.cern.ch/lcg/lastUpdate',
]
# when was the last cvmfs update?
last_update_file = '/cvmfs/sft.cern.ch/lcg/lastUpdate'


def get_cvmfs_base_path() -> str:
    """
    Return the base path for CVMFS.

    :return: base path for CVMFS (str).
    """
    return get_file_system_root_path()
