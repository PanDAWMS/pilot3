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
# - Miha Muskinja, miha.muskinja@cern.ch, 2020
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-23

"""Raythena executor."""

import json
import logging
import os
import time
import traceback
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    FileHandlingFailure,
    NoSuchFile
)
from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.info.filespec import FileSpec
from pilot.util.config import config
from pilot.util.filehandling import (
    calculate_checksum,
    move
)
from .baseexecutor import BaseExecutor

logger = logging.getLogger(__name__)
errors = ErrorCodes()


class RaythenaExecutor(BaseExecutor):
    """Raythena executor class."""

    def __init__(self, **kwargs):
        """
        Initialize Raythena executor.

        :param kwargs: kwargs dictionary (dict).
        """
        super().__init__(**kwargs)
        self.name = "RaythenaExecutor"
        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []
        self.proc = None
        self.exit_code = None

    def is_payload_started(self) -> bool:
        """
        Check if payload is started.

        :return: True if payload is started, False otherwise (bool).
        """
        return self.proc.is_payload_started() if self.proc else False

    def get_pid(self) -> int:
        """
        Get the process id of the payload process.

        :return: the process id of the payload process (int).
        """
        return self.proc.pid if self.proc else None

    def get_exit_code(self) -> int:
        """
        Get the exit code of the payload process.

        :return: the exit code of the payload process (int).
        """
        return self.exit_code

    def create_file_spec(self, pfn: str) -> FileSpec:
        """
        Create a FileSpec object from a given PFN.

        :param pfn: physical file name (string).
        :return: a FileSpec object (FileSpec).
        """
        try:
            checksum = calculate_checksum(pfn, algorithm=config.File.checksum_type)
        except Exception as exc:
            logger.warning(f'caught exception: {exc}')
            checksum = ''  # fail later

        filesize = os.path.getsize(pfn)
        file_data = {'scope': 'transient',
                     'lfn': os.path.basename(pfn),
                     'checksum': checksum,
                     'filesize': filesize,
                     }
        file_spec = FileSpec(filetype='output', **file_data)

        return file_spec

    def move_output(self, pfn: str):
        """
        Move output file from given PFN path to PILOT_OUTPUT_DIR if set.

        :param pfn: physical file name (str).
        """
        outputdir = os.environ.get('PILOT_OUTPUT_DIR', None)
        if outputdir:
            try:
                move(pfn, outputdir)
            except (NoSuchFile, FileHandlingFailure) as exc:
                logger.warning(f'failed to move output: {exc}')

    def update_finished_event_ranges(self, out_messages: Any) -> None:
        """
        Update finished event ranges.

        :param out_messages: messages from AthenaMP (Any).
        """
        logger.info("update_finished_event_ranges:")

        if len(out_messages) == 0:
            return

        event_ranges = []
        for out_msg in out_messages:
            fspec = self.create_file_spec(out_msg['output'])
            event_range_status = {"eventRangeID": out_msg['id'], "eventStatus": 'finished', "pfn": out_msg['output'],
                                  "fsize": fspec.filesize}
            for checksum_key in fspec.checksum:
                event_range_status[checksum_key] = fspec.checksum[checksum_key]
            event_ranges.append(event_range_status)

            # move the output to a common area if necessary
            self.move_output(out_msg['output'])

        event_ranges_status = {"esOutput": {"numEvents": len(event_ranges)}, "eventRanges": event_ranges}
        event_range_message = {'version': 1, 'eventRanges': json.dumps([event_ranges_status])}
        self.update_events(event_range_message)

        job = self.get_job()
        job.nevents += len(event_ranges)

    def update_failed_event_ranges(self, out_messages: Any) -> None:
        """
        Update failed event ranges.

        :param out_messages: messages from AthenaMP (Any).
        """
        if len(out_messages) == 0:
            return

        event_ranges = []
        for message in out_messages:
            status = message['status'] if message['status'] in {'failed', 'fatal'} else 'failed'
            # ToBeFixed errorCode
            event_ranges.append({"errorCode": errors.UNKNOWNPAYLOADFAILURE, "eventRangeID": message['id'], "eventStatus": status})
            event_range_message = {'version': 0, 'eventRanges': json.dumps(event_ranges)}
            self.update_events(event_range_message)

    def handle_out_message(self, message: dict):
        """
        Handle ES output or error messages hook function for tests.

        Example:
                For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                For 'failed' event ranges, it's {'id': <id>, 'status': 'failed', 'message': <full message>}.

        :param message: dictionary of parsed message (dict).
        """
        logger.info(f"Handling out message: {message}")

        self.__all_out_messages.append(message)

        if message['status'] in {'failed', 'fatal'}:
            self.update_failed_event_ranges([message])
        else:
            if 'output' in message:
                message['output'] = os.path.join(self.get_job().workdir, message['output'])
            self.__queued_out_messages.append(message)

    def stageout_es(self, force: bool = False):
        """
        Stage out event service outputs.

        :param force: force stage out (bool).
        """
        job = self.get_job()
        if self.__queued_out_messages:
            if force or self.__last_stageout_time is None or (time.time() > self.__last_stageout_time + job.infosys.queuedata.es_stageout_gap):
                out_messages = []
                while len(self.__queued_out_messages) > 0:
                    out_messages.append(self.__queued_out_messages.pop())
                self.update_finished_event_ranges(out_messages)

    def clean(self):
        """Clean temp produced files."""
        logger.info("shutting down...")

        self.__queued_out_messages = []
        self.__last_stageout_time = None
        self.__all_out_messages = []

        if self.proc:
            self.proc.stop()
            while self.proc.is_alive():
                time.sleep(0.1)

        self.stop_communicator()

    def run(self):
        """Initialize and run ESProcess."""
        try:
            logger.info(f"starting ES RaythenaExecutor with thread ident: {self.ident}")
            if self.is_set_payload():
                payload = self.get_payload()
            elif self.is_retrieve_payload():
                payload = self.retrieve_payload()
            else:
                logger.error("payload is not set but is_retrieve_payload is also not set. No payloads.")
                payload = None

            logger.info(f"payload: {payload}")
            logger.info("starting ESProcess")
            proc = ESProcess(payload, waiting_time=999999)
            self.proc = proc
            logger.info("ESProcess initialized")

            proc.set_get_event_ranges_hook(self.get_event_ranges)
            proc.set_handle_out_message_hook(self.handle_out_message)

            logger.info('ESProcess starts to run')
            proc.start()
            logger.info('ESProcess started to run')

            iteration = 0
            while proc.is_alive():
                iteration += 1
                if self.is_stop():
                    logger.info(f'Stop is set. breaking -- stop process pid={proc.pid}')
                    proc.stop()
                    break
                self.stageout_es()

                exit_code = proc.poll()
                if iteration % 60 == 0:
                    logger.info(f'running: iteration={iteration} pid={proc.pid} exit_code={exit_code}')
                time.sleep(5)

            while proc.is_alive():
                time.sleep(1)
            logger.info("ESProcess finished")

            self.stageout_es(force=True)
            self.clean()

            self.exit_code = proc.poll()

        except Exception as exc:
            logger.error(f'execute payload failed: {exc}, {traceback.format_exc()}')
            self.clean()
            self.exit_code = -1
        logger.info('ES raythena executor finished')
