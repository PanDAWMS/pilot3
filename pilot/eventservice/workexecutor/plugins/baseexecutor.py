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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-24

import logging
import os
import threading

from pilot.common.pluginfactory import PluginFactory
from pilot.control.job import create_job
from pilot.eventservice.communicationmanager.communicationmanager import CommunicationManager

logger = logging.getLogger(__name__)

"""
Base Executor with one process to manage EventService
"""


class BaseExecutor(threading.Thread, PluginFactory):

    def __init__(self, **kwargs: dict):
        """
        Init function for BaseExecutor.

        :param kwargs: keyword arguments (dict).
        """
        super().__init__()
        self.name = "BaseExecutor"
        self.queue = None
        self.payload = None
        self.args = None
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.__stop = threading.Event()
        self.__event_ranges = []
        self.__is_set_payload = False
        self.__is_retrieve_payload = False
        self.communication_manager = None
        self.proc = None
        self.current_dir = os.getcwd()

    def get_pid(self) -> int or None:
        """
        Return the process ID.

        :return: process ID (int or None).
        """
        return self.proc.pid if self.proc else None

    def __del__(self):
        """
        Destructor for the BaseExecutor class.

        This method is called when the BaseExecutor object is about to be destroyed.
        It ensures that the stop method is called and the communication manager is stopped, if it exists.
        """
        self.stop()
        if self.communication_manager:
            self.communication_manager.stop()

    def is_payload_started(self) -> bool:
        """
        Return a boolean indicating whether the payload has started.

        (Not properly implemented).

        :return: True if the payload has started, False otherwise (bool).
        """
        return False

    def start(self):
        """Start the BaseExecutor."""
        super().start()
        self.communication_manager = CommunicationManager()
        self.communication_manager.start()

    def stop(self):
        """Stop the BaseExecutor."""
        if not self.is_stop():
            self.__stop.set()
        if self.communication_manager:
            self.communication_manager.stop()

        logger.info(f"changing current dir from {os.getcwd()} to {self.current_dir}")
        os.chdir(self.current_dir)

    def is_stop(self):
        """
        Return a boolean indicating whether the BaseExecutor should be stopped.

        :return: True if the BaseExecutor should be stopped, False otherwise (bool).
        """
        return self.__stop.is_set()

    def stop_communicator(self):
        """Stop the communication manager."""
        logger.info("Stopping communication manager")
        if self.communication_manager:
            while self.communication_manager.is_alive():
                if not self.communication_manager.is_stop():
                    self.communication_manager.stop()

        logger.info("Communication manager stopped")

    def set_payload(self, payload: dict):
        """
        Set the payload.

        :param payload: payload (dict).
        """
        self.payload = payload
        self.__is_set_payload = True
        job = self.get_job()
        if job and job.workdir:
            logger.info("changing current dir from {os.getcwd()} to {job.workdir}")
            os.chdir(job.workdir)

    def is_set_payload(self) -> bool:
        """
        Return a boolean indicating whether the payload has been set.

        :return: True if the payload has been set, False otherwise (bool).
        """
        return self.__is_set_payload

    def set_retrieve_payload(self):
        """Set the retrieve payload flag."""
        self.__is_retrieve_payload = True

    def is_retrieve_payload(self) -> bool:
        """
        Return the retrieve payload flag.

        :return: True if the retrieve payload flag is set, False otherwise (bool).
        """
        return self.__is_retrieve_payload

    def retrieve_payload(self) -> dict or None:
        """
        Retrieve the payload.

        :return: payload (dict) or None.
        """
        logger.info(f"retrieving payload: {self.args}")
        jobs = self.communication_manager.get_jobs(njobs=1, args=self.args)
        logger.info(f"received jobs: {jobs}")
        if jobs:
            job = create_job(jobs[0], queuename=self.queue)

            # get the payload command from the user specific code
            pilot_user = os.environ.get('PILOT_USER', 'atlas').lower()
            user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
            cmd = user.get_payload_command(job)
            logger.info(f"payload execution command: {cmd}")

            payload = {'executable': cmd,
                       'workdir': job.workdir,
                       'job': job}
            logger.info(f"retrieved payload: {payload}")
            return payload

        return None

    def get_payload(self) -> dict or None:
        """
        Return the payload.

        :return: payload (dict or None).
        """
        if self.__is_set_payload:
            return self.payload

        return None

    def get_job(self):
        """
        Return the job.

        :return: job (dict or None).
        """
        return self.payload['job'] if self.payload and 'job' in list(self.payload.keys()) else None  # Python 2/3

    def get_event_ranges(self, num_event_ranges: int = 1, queue_factor: int = 2) -> list:
        """
        Get event ranges from the communication manager.

        :param num_event_ranges: number of event ranges (int)
        :param queue_factor: queue factor (int)
        :return: event ranges (list).
        """
        if os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') == 'raythena':
            old_queue_factor = queue_factor
            queue_factor = 1
            logger.info(f"raythena - Changing queue_factor from {old_queue_factor} to {queue_factor}")

        logger.info(f"getting event ranges: (num_ranges: {num_event_ranges}) (queue_factor: {queue_factor})")
        if len(self.__event_ranges) < num_event_ranges:
            ret = self.communication_manager.get_event_ranges(num_event_ranges=num_event_ranges * queue_factor, job=self.get_job())
            for event_range in ret:
                self.__event_ranges.append(event_range)

        ret = []
        for _ in range(num_event_ranges):
            if len(self.__event_ranges) > 0:
                event_range = self.__event_ranges.pop(0)
                ret.append(event_range)
        logger.info(f"received event ranges(num:{len(ret)}): {ret}")

        return ret

    def update_events(self, messages: list) -> bool:
        """
        Update event ranges.

        :param messages: messages (list)
        :return: True if the event ranges were updated successfully, False otherwise (bool).
        """
        logger.info(f"updating event ranges: {messages}")
        ret = self.communication_manager.update_events(messages)
        logger.info(f"updated event ranges status: {ret}")

        return ret

    def update_jobs(self, jobs: list) -> bool:
        """
        Update jobs.

        :param jobs: jobs (list)
        :return: True if the jobs were updated successfully, False otherwise (bool).
        """
        logger.info(f"updating jobs: {jobs}")
        ret = self.communication_manager.update_jobs(jobs)
        logger.info(f"updated jobs status: {ret}")

        return ret

    def run(self):
        """Main run process."""
        raise NotImplementedError()
