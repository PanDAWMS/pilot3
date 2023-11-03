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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-2023

"""Executor module for event service merge payloads."""

import logging
import os
from typing import Any, TextIO

from pilot.control.payloads import generic
from pilot.util.container import execute

logger = logging.getLogger(__name__)


class Executor(generic.Executor):
    """Executor class for event service merge payloads."""

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

    def untar_file(self, lfn: str, workdir: str):
        """
        Untar the given file.

        :param lfn: file name (str)
        :param workdir: payload workdir (str).
        """
        pfn = os.path.join(workdir, lfn)
        command = f"tar -xf {pfn} -C {workdir}"
        logger.info(f"untar file: {command}")
        exit_code, stdout, stderr = execute(command)
        logger.info(f"exit_code: {exit_code}, stdout: {stdout}, stderr: {stderr}\n")

    def utility_before_payload(self, job: Any):
        """
        Run utility functions before payload.

        Note: this function updates job.jobparams (process_writetofile() call)

        :param job: job object.
        """
        logger.info("untar input tar files for eventservicemerge job")
        for fspec in job.indata:
            if fspec.is_tar:
                self.untar_file(fspec.lfn, job.workdir)

        logger.info("processing writeToFile for eventservicemerge job")
        job.process_writetofile()

        super().utility_before_payload(job)
