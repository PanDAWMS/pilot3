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

"""Persistent memory cache for data structures used by the pilot."""

from functools import lru_cache


@lru_cache(maxsize=1)
def get_pilot_cache():
    """ Get the dedicated memory cache for the pilot. """
    class PilotCache:
        def __init__(self):
            """ Define standard initialization for the cache. """
            self.use_cgroups = None  # for process management
            self.cgroups = {}  # for process management
            self.set_memory_limits = []
            self.proxy_lifetime = 0
            self.stageout_attempts = None
            self.queuedata = None
            self.pilot_version = None
            self.pilot_work_dir = None
            self.pilot_source_dir = None
            self.pilot_home_dir = None
            self.current_job_id = None
            self.current_job_state = None
            self.source_site = None
            self.destination_site = None
            self.resource_types = None
            self.harvester_submitmode = None

        def get_pids(self):
            """
            Get the list of process IDs (PIDs) from the cgroups dictionary.

            Returns:
                list: List of PIDs.
            """
            return list(self.cgroups.keys())

        def add_cgroup(self, key: str, value: str):
            """
            Add an entry to the cgroups dictionary.

            Normally, the process id would be used as the key, and a
            typical value will be the path to the cgroup.

            The key value can also be a string that identifies a group of processes,
            such as "subprocesses". This allows for grouping processes under a
            common identifier, which can be useful for monitoring or management purposes.

            Args:
                key (str): Key for the cgroups entry.
                value (str): Value for the cgroups entry.
            """
            self.cgroups[key] = value

        def get_cgroup(self, key: str, default: str = None):
            """
            Get an entry from the cgroups dictionary.

            Args:
                key (str): Key for the cgroups entry.
                default: Value to return if the key doesn't exist (default: None).

            Returns:
                The value associated with the key, or default if the key doesn't exist.
            """
            return self.cgroups.get(key, default)

    return PilotCache()
