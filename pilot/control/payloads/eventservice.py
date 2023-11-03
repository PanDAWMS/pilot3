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
# - Wen Guan, wen.guan@cern.ch, 2017-2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-2023

"""Executor module for event service payloads."""

import logging
import os
import time
from typing import Any, TextIO

from pilot.common import exception
from pilot.control.payloads import generic
from pilot.eventservice.workexecutor.workexecutor import WorkExecutor

logger = logging.getLogger(__name__)


class Executor(generic.Executor):
    """Executor class for event service payloads."""

    def __init__(self, args: Any, job: Any, out: TextIO, err: TextIO, traces: Any):
        """
        Set initial values.

        :param args: args object (Any)
        :param job: job object (Any)
        :param out: stdout file object (TextIO)
        :param err: stderr file object (TextIO)
        :param traces: traces object (Any).
        """
        super().__init__(args, job, out, err, traces)

    def run_payload(self, job: Any, cmd: str, out: TextIO, err: TextIO) -> Any:
        """
        Run the payload for the given job and return the executor.

        :param job: job object
        :param cmd: (unused in ES mode)
        :param out: stdout file object
        :param err: stderr file object
        :return: executor instance.
        """
        self.pre_setup(job)

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'atlas').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        self.post_setup(job)

        self.utility_before_payload(job)

        self.utility_with_payload(job)

        try:
            executable = user.get_payload_command(job)
        except exception.PilotException:
            logger.fatal('could not define payload command')
            return None

        logger.info(f"payload execution command: {executable}")

        try:
            payload = {'executable': executable, 'workdir': job.workdir, 'output_file': out, 'error_file': err, 'job': job}
            logger.debug("payload: %s", payload)

            logger.info("starting EventService WorkExecutor")
            executor_type = self.get_executor_type()
            executor = WorkExecutor(args=executor_type)
            executor.set_payload(payload)
            executor.start()
            logger.info("EventService WorkExecutor started")

            logger.info("ESProcess started with pid: {executor.get_pid()}")
            job.pid = executor.get_pid()
            if job.pid:
                job.pgrp = os.getpgid(job.pid)

            self.utility_after_payload_started(job)
        except Exception as error:
            logger.error(f'could not execute: {error}')
            return None

        return executor

    def get_executor_type(self) -> dict:
        """
        Get the executor type.

        This is usually the 'generic' type, which means normal event service. It can also be 'raythena' if specified
        in the Pilot options.

        :return: executor type dictionary.
        """
        # executor_type = 'hpo' if job.is_hpo else os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic')
        # return {'executor_type': executor_type}
        return {'executor_type': os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic')}

    def wait_graceful(self, args: Any, proc: Any) -> int:
        """
        Wait for the graceful signal bit to be set in the args object.

        :param args: args object
        :param proc: process
        :return: exit code (int).
        """
        t_1 = time.time()
        while proc.is_alive():
            if args.graceful_stop.is_set():
                logger.debug("Graceful stop is set, stopping work executor")
                proc.stop()
                break
            if time.time() > t_1 + 300:  # 5 minutes
                logger.info("Process is still running")
                t_1 = time.time()
            time.sleep(2)

        while proc.is_alive():
            time.sleep(2)
        exit_code = proc.get_exit_code()
        return exit_code
