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
# - Wen Guan, wen.guan@cern.ch, 2023
# - Paul Nilsson, paul.nilsson@cern.ch, 2024

import io
import logging
import os
import time
import threading
import traceback

from pilot.common.exception import PilotException, MessageFailure, SetupFailure, RunPayloadFailure, UnknownException


logger = logging.getLogger(__name__)

"""
Main process to handle event service.
It makes use of two hooks get_event_ranges_hook and handle_out_message_hook to communicate with other processes when
it's running. The process will handle the logic of Event service independently.
"""


class ESProcessFineGrainedProc(threading.Thread):
    """
    Main EventService Process.
    """
    def __init__(self, payload, waiting_time=30 * 60):
        """
        Init ESProcessFineGrainedProc.

        :param payload: a dict of {'executable': <cmd string>, 'output_file': <filename or without it>, 'error_file': <filename or without it>}
        """
        threading.Thread.__init__(self, name='esprocessFineGrainedProc')

        self.__payload = payload

        self.__process = None

        self.get_event_ranges_hook = None
        self.handle_out_message_hook = None

        self.__monitor_log_time = None
        self.is_no_more_events = False
        self.__no_more_event_time = None
        self.__waiting_time = waiting_time
        self.__stop = threading.Event()
        self.__stop_time = 180
        self.pid = None
        self.__is_payload_started = False

        self.__ret_code = None
        self.setName("ESProcessFineGrainedProc")
        self.corecount = 1

        self.event_ranges_cache = []

    def is_payload_started(self):
        return self.__is_payload_started

    def stop(self, delay=1800):
        if not self.__stop.is_set():
            self.__stop.set()
            self.__stop_set_time = time.time()
            self.__stop_delay = delay

    def get_job_id(self):
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].jobid:
            return self.__payload['job'].jobid
        return ''

    def get_corecount(self):
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].corecount:
            core_count = int(self.__payload['job'].corecount)
            return core_count
        return 1

    def get_file(self, workdir, file_label='output_file', file_name='ES_payload_output.txt'):
        """
        Return the requested file.

        :param file_label:
        :param workdir:
        :return:
        """

        try:
            file_type = file  # Python 2
        except NameError:
            file_type = io.IOBase  # Python 3

        if file_label in self.__payload:
            if isinstance(self.__payload[file_label], file_type):
                _file_fd = self.__payload[file_label]
            else:
                _file = self.__payload[file_label] if '/' in self.__payload[file_label] else os.path.join(workdir, self.__payload[file_label])
                _file_fd = open(_file, 'w')
        else:
            _file = os.path.join(workdir, file_name)
            _file_fd = open(_file, 'w')

        return _file_fd

    def get_workdir(self):
        """
        Return the workdir.
        If the workdir is set but is not a directory, return None.

        :return: workdir (string or None).
        :raises SetupFailure: in case workdir is not a directory.
        """

        workdir = ''
        if 'workdir' in self.__payload:
            workdir = self.__payload['workdir']
            if not os.path.exists(workdir):
                os.makedirs(workdir)
            elif not os.path.isdir(workdir):
                raise SetupFailure('workdir exists but is not a directory')
        return workdir

    def get_executable(self, workdir):
        """
        Return the executable string.

        :param workdir: work directory (string).
        :return: executable (string).
        """
        executable = self.__payload['executable']
        executable = self.get_payload_executable(executable)
        return 'cd %s; %s' % (workdir, executable)

    def set_get_event_ranges_hook(self, hook):
        """
        set get_event_ranges hook.

        :param hook: a hook method to get event ranges.
        """

        self.get_event_ranges_hook = hook

    def get_get_event_ranges_hook(self):
        """
        get get_event_ranges hook.

        :returns: The hook method to get event ranges.
        """

        return self.get_event_ranges_hook

    def set_handle_out_message_hook(self, hook):
        """
        set handle_out_message hook.

        :param hook: a hook method to handle payload output and error messages.
        """

        self.handle_out_message_hook = hook

    def get_handle_out_message_hook(self):
        """
        get handle_out_message hook.

        :returns: The hook method to handle payload output and error messages.
        """

        return self.handle_out_message_hook

    def init(self):
        """
        initialize message thread and payload process.
        """

        try:
            pass
        except Exception as e:
            # TODO: raise exceptions
            self.__ret_code = -1
            self.stop()
            raise e

    def monitor(self):
        """
        Monitor whether a process is dead.

        raises: MessageFailure: when the message thread is dead or exited.
                RunPayloadFailure: when the payload process is dead or exited.
        """
        pass

    def has_running_children(self):
        """
        Check whether it has running children

        :return: True if there are alive children, otherwise False
        """
        return False

    def is_payload_running(self):
        """
        Check whether the payload is still running

        :return: True if the payload is running, otherwise False
        """
        return False

    def get_event_ranges(self, num_ranges=None, queue_factor=1):
        """
        Calling get_event_ranges hook to get event ranges.

        :param num_ranges: number of event ranges to get.

        :raises: SetupFailure: If get_event_ranges_hook is not set.
                 MessageFailure: when failed to get event ranges.
        """
        if not num_ranges:
            num_ranges = self.corecount

        logger.debug('getting event ranges(num_ranges=%s)' % num_ranges)
        if not self.get_event_ranges_hook:
            raise SetupFailure("get_event_ranges_hook is not set")

        try:
            logger.debug('calling get_event_ranges hook(%s) to get event ranges.' % self.get_event_ranges_hook)
            event_ranges = self.get_event_ranges_hook(num_ranges, queue_factor=queue_factor)
            logger.debug('got event ranges: %s' % event_ranges)
            return event_ranges
        except Exception as e:
            raise MessageFailure("Failed to get event ranges: %s" % e)

    def parse_out_message(self, message):
        """
        Parse output or error messages from payload.

        :param message: The message string received from payload.

        :returns: a dict {'id': <id>, 'status': <status>, 'output': <output if produced>, 'cpu': <cpu>, 'wall': <wall>, 'message': <full message>}
        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """

        logger.debug('parsing message: %s' % message)
        return message

    def handle_out_message(self, message):
        """
        Handle output or error messages from payload.
        Messages from payload will be parsed and the handle_out_message hook is called.

        :param message: The message string received from payload.

        :raises: SetupFailure: when handle_out_message_hook is not set.
                 RunPayloadFailure: when failed to handle an output or error message.
        """

        logger.debug('handling out message: %s' % message)
        if not self.handle_out_message_hook:
            raise SetupFailure("handle_out_message_hook is not set")

        try:
            message_status = self.parse_out_message(message)
            logger.debug('parsed out message: %s' % message_status)
            logger.debug('calling handle_out_message hook(%s) to handle parsed message.' % self.handle_out_message_hook)
            self.handle_out_message_hook(message_status)
        except Exception as e:
            raise RunPayloadFailure("Failed to handle out message: %s" % e)

    def poll(self):
        """
        poll whether the process is still running.

        :returns: None: still running.
                  0: finished successfully.
                  others: failed.
        """
        return self.__ret_code

    def terminate(self, time_to_wait=1):
        """
        Terminate running threads and processes.

        :param time_to_wait: integer, seconds to wait to force kill the payload process.

        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes.')
        try:
            self.stop()
        except Exception as e:
            logger.error('Exception caught when terminating ESProcessFineGrainedProc: %s' % e)
            self.__ret_code = -1
            raise UnknownException(e)

    def kill(self):
        """
        Terminate running threads and processes.

        :param time_to_wait: integer, seconds to wait to force kill the payload process.

        :raises: PilotException: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes.')
        try:
            self.stop()
        except Exception as e:
            logger.error('Exception caught when terminating ESProcessFineGrainedProc: %s' % e)
            raise UnknownException(e)

    def clean(self):
        """
        Clean left resources
        """
        self.stop()

    def run(self):
        """
        Main run loops: monitor message thread and payload process.
                        handle messages from payload and response messages with injecting new event ranges or process outputs.

        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """

        logger.info('start esprocess with thread ident: %s' % (self.ident))
        logger.debug('initializing')
        self.init()
        logger.debug('initialization finished.')

        logger.info('starts to main loop')
        while self.is_payload_running():
            try:
                self.monitor()
                time.sleep(0.01)
            except PilotException as e:
                logger.error('PilotException caught in the main loop: %s, %s' % (e.get_detail(), traceback.format_exc()))
                # TODO: define output message exception. If caught 3 output message exception, terminate
                self.stop()
            except Exception as e:
                logger.error('Exception caught in the main loop: %s, %s' % (e, traceback.format_exc()))
                # TODO: catch and raise exceptions
                # if catching dead process exception, terminate.
                self.stop()
                break
        self.clean()
        logger.debug('main loop finished')
