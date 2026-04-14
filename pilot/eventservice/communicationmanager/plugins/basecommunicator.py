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
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-2024

"""Base communicator."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class BaseCommunicator:
    """Base communicator class."""

    _instance = None

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        """Create new instance of class.

        Args:
            args: args object.
            kwargs: kwargs dictionary.

        Returns:
            Any: new class instance.
        """
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)

        return cls._instance

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize variables.

        Args:
            args: args object.
            kwargs: kwargs dictionary.
        """
        if args:  # to get rid of pylint warning
            pass
        super().__init__()
        for key, value in kwargs.items():
            setattr(self, key, value)

    def pre_check_get_jobs(self, req: Any) -> None:
        """Check whether it's ok to send a request to get jobs.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def request_get_jobs(self, req: Any) -> None:
        """Send a request to get jobs.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def check_get_jobs_status(self, req: Any) -> None:
        """Check whether jobs are prepared.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def get_jobs(self, req: Any) -> None:
        """Get the jobs.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def update_jobs(self, req: Any) -> None:
        """Update job statuses.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def pre_check_get_events(self, req: Any) -> None:
        """Check whether it's ok to send a request to get events.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def request_get_events(self, req: Any) -> None:
        """Send a request to get events.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def check_get_events_status(self, req: Any) -> None:
        """Check whether events prepared.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def get_events(self, req: Any) -> None:
        """Get events.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def pre_check_update_events(self, req: Any) -> None:
        """Check whether it's ok to update events.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def update_events(self, req: Any) -> None:
        """Update events.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()

    def pre_check_update_jobs(self, req: Any) -> None:
        """Check whether it's ok to update event ranges.

        Args:
            req: request.

        Raises:
            NotImplementedError: always raised by base class.
        """
        raise NotImplementedError()
