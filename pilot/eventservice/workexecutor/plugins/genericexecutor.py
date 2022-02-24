#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Wen Guan, wen.guan@cern.ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-2022

import json
import os
import time
import traceback

from pilot.api.es_data import StageOutESClient
from pilot.common.exception import PilotException, StageOutFailure

from pilot.common.errorcodes import ErrorCodes
from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.info.filespec import FileSpec
from pilot.info import infosys
from pilot.util.container import execute

from .baseexecutor import BaseExecutor

import logging
logger = logging.getLogger(__name__)

errors = ErrorCodes()

"""
Generic Executor with one process to manage EventService
"""


class GenericExecutor(BaseExecutor):
    def __init__(self, **kwargs):
        super(GenericExecutor, self).__init__(**kwargs)
        self.setName("GenericExecutor")

        self.__queued_out_messages = []
        self.__stageout_failures = 0
        self.__max_allowed_stageout_failures = 20
        self.__last_stageout_time = None
        self.__all_out_messages = []

        self.proc = None
        self.exit_code = None

    def is_payload_started(self):
        return self.proc.is_payload_started() if self.proc else False

    def get_pid(self):
        return self.proc.pid if self.proc else None

    def get_exit_code(self):
        return self.exit_code

    def update_finished_event_ranges(self, out_messagess, output_file, fsize, checksum, storage_id):
        """
        Update finished event ranges

        :param out_messages: messages from AthenaMP.
        :param output_file: output file name.
        :param fsize: file size.
        :param adler32: checksum (adler32) of the file.
        :param storage_id: the id of the storage.
        """

        if len(out_messagess) == 0:
            return

        event_ranges = []
        for out_msg in out_messagess:
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

    def update_failed_event_ranges(self, out_messagess):
        """
        Update failed event ranges

        :param out_messages: messages from AthenaMP.
        """

        if len(out_messagess) == 0:
            return

        event_ranges = []
        for message in out_messagess:
            status = message['status'] if message['status'] in ['failed', 'fatal'] else 'failed'
            # ToBeFixed errorCode
            event_ranges.append({"errorCode": errors.UNKNOWNPAYLOADFAILURE, "eventRangeID": message['id'], "eventStatus": status})
            event_range_message = {'version': 0, 'eventRanges': json.dumps(event_ranges)}
            self.update_events(event_range_message)

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

        if message['status'] in ['failed', 'fatal']:
            self.update_failed_event_ranges([message])
        else:
            self.__queued_out_messages.append(message)

    def tarzip_output_es(self):
        """
        Tar/zip eventservice outputs.

        :return: out_messages, output_file
        """

        out_messages = []
        while len(self.__queued_out_messages) > 0:
            out_messages.append(self.__queued_out_messages.pop())

        output_file = "EventService_premerge_%s.tar" % out_messages[0]['id']

        ret_messages = []
        try:
            for out_msg in out_messages:
                command = "tar -rf " + output_file + " --directory=%s %s" % (os.path.dirname(out_msg['output']), os.path.basename(out_msg['output']))
                exit_code, stdout, stderr = execute(command)
                if exit_code == 0:
                    ret_messages.append(out_msg)
                else:
                    logger.error(f"failed to add event output to tar/zip file: out_message: "
                                 f"{out_msg}, exit_code: {exit_code}, stdout: {stdout}, stderr: {stderr}")
                    if 'retries' in out_msg and out_msg['retries'] >= 3:
                        logger.error(f"discard out messages because it has been retried more than 3 times: {out_msg}")
                    else:
                        if 'retries' in out_msg:
                            out_msg['retries'] += 1
                        else:
                            out_msg['retries'] = 1
                        self.__queued_out_messages.append(out_msg)
        except Exception as exc:
            logger.error(f"failed to tar/zip event ranges: {exc}")
            self.__queued_out_messages += out_messages
            self.__stageout_failures += 1
            return None, None

        return ret_messages, output_file

    def stageout_es_real(self, output_file):  # noqa: C901
        """
        Stage out event service output file.

        :param output_file: output file name.
        """

        job = self.get_job()
        logger.info('prepare to stage-out event service files')

        _error = None
        file_data = {'scope': 'transient',
                     'lfn': os.path.basename(output_file),
                     }
        file_spec = FileSpec(filetype='output', **file_data)
        xdata = [file_spec]
        kwargs = dict(workdir=job.workdir, cwd=job.workdir, usecontainer=False, job=job)

        try_failover = False
        activity = ['es_events', 'pw']  ## FIX ME LATER: replace `pw` with `write_lan` once AGIS is updated (acopytools)

        try:
            client = StageOutESClient(job.infosys, logger=logger)
            try_failover = True

            client.prepare_destinations(xdata, activity)  ## IF ES job should be allowed to write only at `es_events` astorages, then fix activity names here
            client.transfer(xdata, activity=activity, **kwargs)
        except PilotException as error:
            logger.error(error.get_detail())
            _error = error
        except Exception as exc:
            logger.error(traceback.format_exc())
            _error = StageOutFailure(f"stage-out failed with error={exc}")

        try:
            if _error:
                pass
        except Exception as exc:
            logger.error(f'found no error object - stage-out must have failed: {exc}')
            _error = StageOutFailure("stage-out failed")

        logger.info('summary of transferred files:')
        logger.info(f" -- lfn={file_spec.lfn}, status_code={file_spec.status_code}, status={file_spec.status}")

        if _error:
            logger.error(f'failed to stage-out eventservice file({output_file}): error={_error.get_detail()}')
        elif file_spec.status != 'transferred':
            msg = f'failed to stage-out ES file({output_file}): unknown internal error, fspec={file_spec}'
            logger.error(msg)
            raise StageOutFailure(msg)

        failover_storage_activity = ['es_failover', 'pw']

        if try_failover and _error and _error.get_error_code() not in [ErrorCodes.MISSINGOUTPUTFILE]:  ## try to failover to other storage

            xdata2 = [FileSpec(filetype='output', **file_data)]

            try:
                client.prepare_destinations(xdata2, failover_storage_activity)
                if xdata2[0].ddmendpoint != xdata[0].ddmendpoint:  ## skip transfer to same output storage
                    msg = f'will try to failover ES transfer to astorage with activity={failover_storage_activity}, rse={xdata2[0].ddmendpoint}'
                    logger.info(msg)
                    client.transfer(xdata2, activity=activity, **kwargs)

                    logger.info('summary of transferred files (failover transfer):')
                    logger.info(f" -- lfn={xdata2[0].lfn}, status_code={xdata2[0].status_code}, status={xdata2[0].status}")

            except PilotException as exc:
                if exc.get_error_code() == ErrorCodes.NOSTORAGE:
                    logger.info(f'failover ES storage is not defined for activity={failover_storage_activity} .. skipped')
                else:
                    logger.error(f'transfer to failover storage={xdata2[0].ddmendpoint} failed .. skipped, error={exc.get_detail()}')
            except Exception:
                logger.error('failover ES stage-out failed .. skipped')
                logger.error(traceback.format_exc())

            if xdata2[0].status == 'transferred':
                _error = None
                file_spec = xdata2[0]

        if _error:
            raise _error

        storage_id = infosys.get_storage_id(file_spec.ddmendpoint)

        return file_spec.ddmendpoint, storage_id, file_spec.filesize, file_spec.checksum

    def stageout_es(self, force=False):
        """
        Stage out event service outputs.
        When pilot fails to stage out a file, the file will be added back to the queue for staging out next period.
        """

        job = self.get_job()
        if len(self.__queued_out_messages):
            if force or self.__last_stageout_time is None or (time.time() > self.__last_stageout_time + job.infosys.queuedata.es_stageout_gap):

                out_messagess, output_file = self.tarzip_output_es()
                logger.info(f"tar/zip event ranges: {out_messagess}, output_file: {output_file}")

                if out_messagess:
                    self.__last_stageout_time = time.time()
                    try:
                        logger.info(f"staging output file: {output_file}")
                        storage, storage_id, fsize, checksum = self.stageout_es_real(output_file)
                        logger.info(f"staged output file ({output_file}) to storage: {storage} storage_id: {storage_id}")

                        self.update_finished_event_ranges(out_messagess, output_file, fsize, checksum, storage_id)
                    except Exception as exc:
                        logger.error(f"failed to stage out file({output_file}): {exc}, {traceback.format_exc()}")

                        if force:
                            self.update_failed_event_ranges(out_messagess)
                        else:
                            logger.info("failed to stage out, adding messages back to the queued messages")
                            self.__queued_out_messages += out_messagess
                            self.__stageout_failures += 1

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

    def run(self):
        """
        Initialize and run ESProcess.
        """

        try:
            logger.info("starting ES GenericExecutor with thread identifier: %s" % (self.ident))
            if self.is_set_payload():
                payload = self.get_payload()
            elif self.is_retrieve_payload():
                payload = self.retrieve_payload()
            else:
                logger.error("payload is not set, is_retrieve_payload is also not set - no payloads")
                self.exit_code = -1
                return

            logger.info(f"payload: {payload}")
            logger.info("starting ESProcess")
            proc = ESProcess(payload)
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

        except Exception as exc:
            logger.error(f'execute payload failed: {exc}, {traceback.format_exc()}')
            self.clean()
            self.exit_code = -1

        logger.info('ES generic executor finished')
