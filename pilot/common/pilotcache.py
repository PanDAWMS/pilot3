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
            self.proxy_lifetime = 0
            self.queuedata = {}
            self.pilot_version = None
            self.pilot_work_dir = None
            self.pilot_source_dir = None
            self.pilot_home_dir = None
            self.current_job_id = None
            self.current_job_state = None

    return PilotCache()
