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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2024

"""API for memory monitoring."""

import logging
from os import getcwd
from typing import Any

from .services import Services

logger = logging.getLogger(__name__)


class MemoryMonitoring(Services):
    """Memory monitoring service class."""

    user = ""     # Pilot user, e.g. 'ATLAS'
    pid = 0       # Job process id
    workdir = ""  # Job work directory
    _cmd = ""     # Memory monitoring command (full path, all options)

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the memory monitoring service.

        Args:
            **kwargs: Keyword arguments set as instance attributes. Recognized
                keys include ``user``, ``pid``, and ``workdir``.
        """
        for key, value in kwargs.items():
            setattr(self, key, value)

        if not self.workdir:
            self.workdir = getcwd()

        if self.user:
            user_utility = __import__(f'pilot.user.{self.user}.utilities', globals(), locals(), [self.user], 0)  # Python 2/3
            self._cmd = user_utility.get_memory_monitor_setup(self.pid, self.workdir)

    def get_command(self) -> str:
        """Return the full command for the memory monitor.

        Returns:
            Full memory monitor command string.
        """
        return self._cmd

    def execute(self) -> None:
        """Execute the memory monitor command.

        Returns:
            None
        """
        return None

    def get_filename(self) -> str:
        """Return the filename produced by the memory monitor tool.

        Returns:
            Memory monitor output filename (empty string by default).
        """
        return ""

    def get_results(self) -> None:
        """Return the results from the memory monitoring.

        Returns:
            None
        """
        return None
