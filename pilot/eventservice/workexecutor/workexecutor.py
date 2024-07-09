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

"""Base executor - Main class to manage the event service work."""

import logging
import time
from typing import Any

from pilot.common import exception
from pilot.common.pluginfactory import PluginFactory

logger = logging.getLogger(__name__)


class WorkExecutor(PluginFactory):
    """Work executor class."""

    def __init__(self, args: Any = None):
        """
        Initialize work executor.

        :param args: args dictionary (Any).
        """
        super().__init__()
        self.payload = None
        self.plugin = None
        self.is_retrieve_payload = False
        self.args = args
        self.pid = None

    def get_pid(self) -> int:
        """
        Return the pid of the payload process.

        :return: pid (int).
        """
        return self.plugin.get_pid() if self.plugin else None

    def set_payload(self, payload: Any):
        """
        Set the payload.

        :param payload: payload (Any).
        """
        self.payload = payload

    def set_retrieve_payload(self):
        """Set the payload to be retrieved."""
        self.is_retrieve_payload = True

    def get_payload(self) -> Any:
        """
        Return the payload.

        :return: payload (Any).
        """
        return self.payload

    def get_plugin_confs(self) -> dict:
        """
        Return the plugin configurations.

        :return: plugin configurations (dict).
        """
        executor_type_to_class = {
            'hpo': 'pilot.eventservice.workexecutor.plugins.hpoexecutor.HPOExecutor',
            'raythena': 'pilot.eventservice.workexecutor.plugins.raythenaexecutor.RaythenaExecutor',
            'generic': 'pilot.eventservice.workexecutor.plugins.genericexecutor.GenericExecutor',
            'base': 'pilot.eventservice.workexecutor.plugins.baseexecutor.BaseExecutor',
            'nl': 'pilot.eventservice.workexecutor.plugins.nlexecutor.NLExecutor',
            'boinc': 'pilot.eventservice.workexecutor.plugins.boincexecutor.BOINCExecutor',
            'hammercloud': 'pilot.eventservice.workexecutor.plugins.hammercloudexecutor.HammerCloudExecutor',
            'mpi': 'pilot.eventservice.workexecutor.plugins.mpiexecutor.MPIExecutor',
            'fineGrainedProc': 'pilot.eventservice.workexecutor.plugins.finegrainedprocexecutor.FineGrainedProcExecutor'
        }

        executor_type = self.args.get('executor_type', 'generic')
        class_name = executor_type_to_class.get(executor_type,
                                                'pilot.eventservice.workexecutor.plugins.genericexecutor.GenericExecutor')
        plugin_confs = {'class': class_name, 'args': self.args}

        return plugin_confs

    def start(self):
        """
        Start the work executor.

        :raises SetupFailure: if no available executor plugin.
        """
        plugin_confs = self.get_plugin_confs()
        logger.info(f"Plugin confs: {plugin_confs}")
        self.plugin = self.get_plugin(plugin_confs)
        logger.info(f"WorkExecutor started with plugin: {self.plugin}")
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")

        if self.is_retrieve_payload:
            self.payload = self.plugin.set_retrieve_payload()
        else:
            if not self.get_payload():
                raise exception.SetupFailure("Payload is not assigned.")

            self.plugin.set_payload(self.get_payload())

        logger.info(f"Starting plugin: {self.plugin}")
        self.plugin.start()
        logger.info("Waiting for payload to start")
        while self.plugin.is_alive():
            if self.plugin.is_payload_started():
                logger.info(f"Payload started with pid: {self.get_pid()}")
                break
            time.sleep(1)

    def stop(self) -> int:
        """
        Stop the work executor.

        :return: exit code (int)
        :raises SetupFailure: if no available executor plugin.
        """
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")

        return self.plugin.stop()

    def is_alive(self) -> bool:
        """
        Check if the work executor is alive.

        :return: True if alive, otherwise False (bool)
        :raises SetupFailure: if no available executor plugin.
        """
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")

        return self.plugin.is_alive()

    def get_exit_code(self) -> int:
        """
        Return the exit code.

        :return: exit code (int)
        :raises SetupFailure: if no available executor plugin.
        """
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")

        return self.plugin.get_exit_code()

    def get_event_ranges(self) -> list:
        """
        Get event ranges.

        :return: event ranges (list)
        :raises SetupFailure: if no available executor plugin.
        """
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")

        return self.plugin.get_event_ranges()

    def update_events(self, messages: Any) -> bool:
        """
        Update events.

        :param messages: messages (Any)
        :return: True if events are updated, otherwise False (bool)
        :raises SetupFailure: if no available executor plugin.
        """
        if not self.plugin:
            raise exception.SetupFailure("No available executor plugin.")

        return self.plugin.update_events(messages)
