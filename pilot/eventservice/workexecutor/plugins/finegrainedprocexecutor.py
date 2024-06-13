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
# - Wen Guan, wen.guan@cern.ch, 2023-24
# - Paul Nilsson, paul.nilsson@cern.ch, 2024

import json
import logging
import os
import time
import traceback
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from .baseexecutor import BaseExecutor

logger = logging.getLogger(__name__)
errors = ErrorCodes()

"""
FineGrainedProc Executor with one process to manage EventService
"""


class FineGrainedProcExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        """
        Init function for FineGrainedProcExecutor.

        :param kwargs: keyword arguments (dict).
        """
        super().__init__(**kwargs)
        self.setName("FineGrainedProcExecutor")
        self.__queued_out_messages = []
        self.__stageout_failures = 0
        self.__max_allowed_stageout_failures = 20
        self.__last_stageout_time = None
        self.__all_out_messages = []
        self.proc = None
        self.exit_code = None

    def is_payload_started(self) -> bool:
        """
        Check if payload is started.

        :return: True if payload is started, False otherwise.
        """
        return self.proc.is_payload_started() if self.proc else False

    def get_pid(self) -> int or None:
        """
        Return the process ID.

        :return: process ID (int or None).
        """
        return self.proc.pid if self.proc else None

    def get_exit_code(self) -> int or None:
        """
        Get exit code of the process.

        :return: exit code of the process (int or None).
        """
        return self.exit_code

    def update_finished_event_ranges(self, out_messages: list, output_file: str, fsize: int, checksum: str, storage_id: Any):
        """
        Update finished event ranges

        :param out_messages: messages from AthenaMP (list)
        :param output_file: output file name (str)
        :param fsize: file size (int)
        :param checksum: checksum (adler32) of the file (str)
        :param storage_id: the id of the storage (Any).
        """
        if len(out_messages) == 0:
            return

        event_ranges = []
        for out_msg in out_messages:
            event_ranges.append({"eventRangeID": out_msg['id'], "eventStatus": 'finished'})
        event_range_status = {"zipFile": {"numEvents": len(event_ranges),
                                          "objstoreID": storage_id,
                                          "lfn": os.path.basename(output_file),
                                          "fsize": fsize,
                                          "pathConvention": 1000},
                              "eventRanges": event_ranges}
        for checksum_key in checksum:
            event_range_status["zipFile"][checksum_key] = checksum[checksum_key]
        event_range_message = {'version': 1, 'eventRanges': json.dumps([event_range_status])}
        self.update_events(event_range_message)

        job = self.get_job()
        job.nevents += len(event_ranges)

    def update_failed_event_ranges(self, out_messages: list):
        """
        Update failed event ranges.

        :param out_messages: messages from AthenaMP.
        """
        if len(out_messages) == 0:
            return

        event_ranges = []
        for message in out_messages:
            status = message['status'] if message['status'] in ['failed', 'fatal'] else 'failed'
            # ToBeFixed errorCode
            event_ranges.append({"errorCode": errors.UNKNOWNPAYLOADFAILURE, "eventRangeID": message['id'], "eventStatus": status})
            event_range_message = {'version': 0, 'eventRanges': json.dumps(event_ranges)}
            self.update_events(event_range_message)

    def update_terminated_event_ranges(self, out_messagess):
        """
        Update terminated event ranges

        :param out_messages: messages from AthenaMP.
        """

        if len(out_messagess) == 0:
            return

        event_ranges = []
        finished_events = 0
        for message in out_messagess:
            if message['status'] in ['failed', 'fatal', 'finished', 'running', 'transferring']:
                status = message['status']
                if message['status'] in ['finished']:
                    finished_events += 1
            else:
                logger.warn("status is unknown for messages, set it running: %s" % str(message))
                status = 'running'
            error_code = message.get("error_code", None)
            if status in ["failed", "fatal"] and error_code is None:
                error_code = errors.UNKNOWNPAYLOADFAILURE
            error_diag = message.get("error_diag")

            event_range = {"eventRangeID": message['id'], "eventStatus": status, "errorCode": error_code, "errorDiag": error_diag}
            event_ranges.append(event_range)
        event_range_message = {'version': 0, 'eventRanges': json.dumps(event_ranges)}
        self.update_events(event_range_message)

        job = self.get_job()
        job.nevents += finished_events

    def handle_out_message(self, message):
        """
        Handle ES output or error messages hook function for tests.

        :param message: a dict of parsed message.
                        For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                                                           'wall': <wall>, 'message': <full message>}.
                        Fro 'failed' event ranges, it's {'id': <id>, 'status': 'failed', 'message': <full message>}.
        """

        logger.info(f"handling out message: {message}")

        self.__all_out_messages.append(message)

        self.__queued_out_messages.append(message)

    def stageout_es(self, force=False):
        """
        Stage out event service outputs.
        When pilot fails to stage out a file, the file will be added back to the queue for staging out next period.
        """

        job = self.get_job()
        if len(self.__queued_out_messages):
            if force or self.__last_stageout_time is None or (time.time() > self.__last_stageout_time + job.infosys.queuedata.es_stageout_gap):

                out_messages = []
                while len(self.__queued_out_messages) > 0:
                    out_messages.append(self.__queued_out_messages.pop())

                if out_messages:
                    self.__last_stageout_time = time.time()
                    self.update_terminated_event_ranges(out_messages)

    def clean(self):
        """
        Clean temp produced files
        """

        for msg in self.__all_out_messages:
            if msg['status'] in ['failed', 'fatal']:
                pass
            elif 'output' in msg:
                try:
                    logger.info(f"removing ES pre-merge file: {msg['output']}")
                    os.remove(msg['output'])
                except Exception as exc:
                    logger.error(f"failed to remove file({msg['output']}): {exc}")
        self.__queued_out_messages = []
        self.__stageout_failures = 0
        self.__last_stageout_time = None
        self.__all_out_messages = []

        if self.proc:
            self.proc.stop()
            while self.proc.is_alive():
                time.sleep(0.1)

        self.stop_communicator()
        self.stop()

    def get_esprocess_finegrainedproc(self, payload):
        # get the payload command from the user specific code
        try:
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            esprocessfinegrainedproc = __import__(f'pilot.user.{pilot_user}.esprocessfinegrainedproc',
                                                  globals(), locals(), [pilot_user], 0)
            proc = esprocessfinegrainedproc.ESProcessFineGrainedProc(payload)
            return proc
        except Exception as ex:
            logger.warn("use specific ESProcessFineGrainedProc does not exist. Using the pilot.eventservice.esprocess.esprocessfinegrainedproc: " + str(ex))
            from pilot.eventservice.esprocess.esprocessfinegrainedproc import ESProcessFineGrainedProc
            proc = ESProcessFineGrainedProc(payload)
            return proc

    def run(self):
        """
        Initialize and run ESProcess.
        """

        try:
            logger.info("starting ES FineGrainedProcExecutor with thread identifier: %s" % (self.ident))
            if self.is_set_payload():
                payload = self.get_payload()
            elif self.is_retrieve_payload():
                payload = self.retrieve_payload()
            else:
                logger.error("payload is not set, is_retrieve_payload is also not set - no payloads")
                self.exit_code = -1
                return

            logger.info(f"payload: {payload}")
            logger.info("starting ESProcessFineGrainedProc")
            proc = self.get_esprocess_finegrainedproc(payload)
            self.proc = proc
            logger.info("ESProcessFineGrainedProc initialized")

            proc.set_get_event_ranges_hook(self.get_event_ranges)
            proc.set_handle_out_message_hook(self.handle_out_message)

            logger.info('ESProcessFineGrainedProc starts to run')
            proc.start()
            logger.info('ESProcessFineGrainedProc started to run')

            iteration = 0
            while proc.is_alive():
                iteration += 1
                if self.is_stop():
                    logger.info(f'stop is set -- stopping process pid={proc.pid}')
                    proc.stop()
                    break
                self.stageout_es()

                # have we passed the threshold for failed stage-outs?
                if self.__stageout_failures >= self.__max_allowed_stageout_failures:
                    logger.warning(f'too many stage-out failures ({self.__max_allowed_stageout_failures})')
                    logger.info(f'stopping process pid={proc.pid}')
                    proc.stop()
                    break

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
            logger.info("ESProcess exit_code: %s" % self.exit_code)

        except Exception as exc:
            logger.error(f'execute payload failed: {exc}, {traceback.format_exc()}')
            self.clean()
            self.exit_code = -1

        logger.info('ES fine grained proc executor finished')
