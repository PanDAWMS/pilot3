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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-23

"""Base communicator."""

import logging
from typing import Any

logger = logging.getLogger(__name__)


class BaseCommunicator:
    """Base communicator class."""

    _instance = None

    def __new__(class_, *args: Any, **kwargs: dict) -> Any:
        """
        Create new instance of class.

        :param args: args object (Any)
        :param kwargs: kwargs dictionary (dict)
        :return: new class instance (Any).
        """
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)

        return class_._instance

    def __init__(self, *args: Any, **kwargs: dict):
        """
        Initialize variables.

        :param args: args object (Any)
        :param kwargs: kwargs dictionary (dict)
        """
        super(BaseCommunicator, self).__init__()
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def pre_check_get_jobs(self, req: Any):
        """
        Check whether it's ok to send a request to get jobs.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def request_get_jobs(self, req: Any):
        """
        Send a request to get jobs.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def check_get_jobs_status(self, req: Any):
        """
        Check whether jobs are prepared.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def get_jobs(self, req: Any):
        """
        Get the jobs.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def update_jobs(self, req: Any):
        """
        Update job statuses.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def pre_check_get_events(self, req: Any):
        """
        Check whether it's ok to send a request to get events.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def request_get_events(self, req: Any):
        """
        Send a request to get events.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def check_get_events_status(self, req: Any):
        """
        Check whether events prepared.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def get_events(self, req: Any):
        """
        Get events.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def pre_check_update_events(self, req: Any):
        """
        Check whether it's ok to update events.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def update_events(self, req: Any):
        """
        Update events.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()

    def pre_check_update_jobs(self, req: Any):
        """
        Check whether it's ok to update event ranges.

        :param req: request (Any)
        :raises: NotImplementedError.
        """
        raise NotImplementedError()
