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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2023

"""API for memory monitoring."""

from os import getcwd
from .services import Services

import logging
logger = logging.getLogger(__name__)


class MemoryMonitoring(Services):
    """Memory monitoring service class."""

    user = ""     # Pilot user, e.g. 'ATLAS'
    pid = 0       # Job process id
    workdir = ""  # Job work directory
    _cmd = ""     # Memory monitoring command (full path, all options)

    def __init__(self, **kwargs):
        """
        Init function.

        :param kwargs: kwargs dictionary.
        """
        for key in kwargs:
            setattr(self, key, kwargs[key])

        if not self.workdir:
            self.workdir = getcwd()

        if self.user:
            user_utility = __import__('pilot.user.%s.utilities' % self.user, globals(), locals(), [self.user], 0)  # Python 2/3
            self._cmd = user_utility.get_memory_monitor_setup(self.pid, self.workdir)

    def get_command(self):
        """
        Return the full command for the memory monitor.

        :return: command string (str).
        """
        return self._cmd

    def execute(self):
        """
        Execute the memory monitor command.

        :return: process (currently None).
        """
        return None

    def get_filename(self):
        """
        Return the filename from the memory monitor tool.

        :return: fiename (str).
        """
        return ""

    def get_results(self):
        """
        Return the results from the memory monitoring.

        :return: results (currently None).
        """
        return None
