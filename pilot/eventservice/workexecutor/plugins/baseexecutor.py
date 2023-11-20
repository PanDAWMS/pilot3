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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23

"""Base executor."""

import logging
import os
import threading
from typing import Any

from pilot.common.pluginfactory import PluginFactory
from pilot.control.job import create_job
from pilot.eventservice.communicationmanager.communicationmanager import CommunicationManager

logger = logging.getLogger(__name__)


class BaseExecutor(threading.Thread, PluginFactory):
    """Base executor class."""

    def __init__(self, **kwargs):
        """
        Initialize base executor.

        :param kwargs: kwargs dictionary (dict).
        """
        super(BaseExecutor, self).__init__()
        self.setName("BaseExecutor")
        self.queue = None
        self.payload = None
        self.args = None

        for key in kwargs:
            setattr(self, key, kwargs[key])

        self.__stop = threading.Event()
        self.__event_ranges = []
        self.__is_set_payload = False
        self.__is_retrieve_payload = False
        self.communication_manager = None
        self.proc = None

    def get_pid(self) -> int:
        """Get the process id of the payload process."""
        return self.proc.pid if self.proc else None

    def __del__(self):
        """Delete the instance."""
        self.stop()
        if self.communication_manager:
            self.communication_manager.stop()

    def is_payload_started(self) -> bool:
        """Check if payload is started."""
        return False

    def start(self):
        """Start the instance."""
        super(BaseExecutor, self).start()
        self.communication_manager = CommunicationManager()
        self.communication_manager.start()

    def stop(self):
        """Stop the instance."""
        if not self.is_stop():
            self.__stop.set()

    def is_stop(self):
        """Check if the instance is stopped."""
        return self.__stop.is_set()

    def stop_communicator(self):
        """Stop the communication manager."""
        logger.info("stopping communication manager")
        if self.communication_manager:
            while self.communication_manager.is_alive():
                if not self.communication_manager.is_stop():
                    self.communication_manager.stop()
        logger.info("communication manager stopped")

    def set_payload(self, payload: dict):
        """
        Set payload to execute.

        :param payload: payload dictionary (dict).
        """
        self.payload = payload
        self.__is_set_payload = True
        job = self.get_job()
        if job and job.workdir:
            os.chdir(job.workdir)

    def is_set_payload(self) -> bool:
        """Check if payload is set."""
        return self.__is_set_payload

    def set_retrieve_payload(self):
        """Set flag to retrieve payload."""
        self.__is_retrieve_payload = True

    def is_retrieve_payload(self) -> bool:
        """Check if payload is retrieved."""
        return self.__is_retrieve_payload

    def retrieve_payload(self) -> dict:
        """
        Retrieve payload.

        :return: payload dictionary (dict).
        """
        logger.info(f"retrieving payload: {self.args}")
        jobs = self.communication_manager.get_jobs(njobs=1, args=self.args)
        logger.info(f"received jobs: {jobs}")
        payload = None
        if jobs:
            job = create_job(jobs[0], queue=self.queue)

            # get the payload command from the user specific code
            pilot_user = os.environ.get('PILOT_USER', 'atlas').lower()
            user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)  # Python 2/3
            cmd = user.get_payload_command(job)
            if cmd:
                logger.info(f"payload execution command: {cmd}")
                payload = {'executable': cmd,
                           'workdir': job.workdir,
                           'job': job}
                logger.info(f"Retrieved payload: {payload}")
            else:
                logger.warning("failed to get payload command from user code")
        return payload

    def get_payload(self) -> bool:
        """
        Get payload.

        :return: payload dictionary (dict).
        """
        if self.__is_set_payload:
            return self.payload
        else:
            return None

    def get_job(self) -> Any:
        """
        Get job.

        :return: job (Any).
        """
        return self.payload['job'] if self.payload and 'job' in list(self.payload.keys()) else None

    def get_event_ranges(self, num_event_ranges: int = 1, queue_factor: int = 2) -> list:
        """
        Get event ranges.

        :param num_event_ranges: number of event ranges to get (int)
        :param queue_factor: queue factor (int)
        :return: list of event ranges (list).
        """
        if os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') == 'raythena':
            old_queue_factor = queue_factor
            queue_factor = 1
            logger.info(f"raythena - Changing queue_factor from {old_queue_factor} to {queue_factor}")
        logger.info(f"Getting event ranges: (num_ranges: {num_event_ranges}) (queue_factor: {queue_factor})")
        if len(self.__event_ranges) < num_event_ranges:
            ret = self.communication_manager.get_event_ranges(num_event_ranges=num_event_ranges * queue_factor, job=self.get_job())
            for event_range in ret:
                self.__event_ranges.append(event_range)

        ret = []
        for _ in range(num_event_ranges):
            if len(self.__event_ranges) > 0:
                event_range = self.__event_ranges.pop(0)
                ret.append(event_range)
        logger.info(f"Received event ranges(num:{len(ret)}): {ret}")
        return ret

    def update_events(self, messages: Any) -> Any:
        """
        Update event ranges.

        :param messages: messages to update (Any)
        :return: status of the update (Any).
        """
        logger.info(f"Updating event ranges: {messages}")
        ret = self.communication_manager.update_events(messages)
        logger.info(f"Updated event ranges status: {ret}")

        return ret

    def update_jobs(self, jobs: Any) -> Any:
        """
        Update jobs.

        :param jobs: jobs to update (Any)
        :return: status of the update (Any).
        """
        logger.info(f"updating jobs: {jobs}")
        ret = self.communication_manager.update_jobs(jobs)
        logger.info(f"updated jobs status: {ret}")

        return ret

    def run(self):
        """Run main process."""
        raise NotImplementedError()
