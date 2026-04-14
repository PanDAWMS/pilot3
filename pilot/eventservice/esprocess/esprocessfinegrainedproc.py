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

"""Class with functions for finegrained ES processing."""

import io
import logging
import os
import time
import threading
import traceback
from typing import Any, IO

from pilot.common.exception import PilotException, MessageFailure, SetupFailure, RunPayloadFailure, UnknownException

logger = logging.getLogger(__name__)


class ESProcessFineGrainedProc(threading.Thread):
    """Main EventService Process."""

    def __init__(self, payload: dict, waiting_time: int = 30 * 60) -> None:
        """Init ESProcessFineGrainedProc.

        Args:
            payload: dict of {'executable': <cmd string>, 'output_file': <filename or without it>, 'error_file': <filename or without it>}.
            waiting_time: waiting time in seconds for the process to finish.
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

    def is_payload_started(self) -> bool:
        """Return boolean to indicate whether the payload has started.

        Returns:
            bool: True if the payload has started, otherwise False.
        """
        return self.__is_payload_started

    def stop(self, delay: int = 1800) -> None:
        """Set stop event."""
        if not self.__stop.is_set():
            self.__stop.set()
            self.__stop_set_time = time.time()
            self.__stop_delay = delay

    def get_job_id(self) -> int:
        """Return job id.

        Returns:
            int: job id, or 0 if not available.
        """
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].jobid:
            return self.__payload['job'].jobid

        return 0

    def get_corecount(self) -> int:
        """Return core count.

        Returns:
            int: core count, or 1 if not available.
        """
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].corecount:
            core_count = int(self.__payload['job'].corecount)
            return core_count

        return 1

    def get_file(self, workdir: str, file_label: str = 'output_file', file_name: str = 'ES_payload_output.txt') -> IO[str]:
        """Return the requested file.

        Args:
            workdir: work directory.
            file_label: file label.
            file_name: file name.

        Returns:
            IO[str]: file descriptor.
        """
        file_type = io.IOBase

        if file_label in self.__payload:
            if isinstance(self.__payload[file_label], file_type):
                _file_fd = self.__payload[file_label]
            else:
                _file = self.__payload[file_label] if '/' in self.__payload[file_label] else os.path.join(workdir, self.__payload[file_label])
                _file_fd = open(_file, 'w', encoding='utf-8')
        else:
            _file_fd = open(os.path.join(workdir, file_name), 'w', encoding='utf-8')

        return _file_fd

    def get_workdir(self) -> str:
        """Return the workdir.

        Returns:
            str: work directory.

        Raises:
            SetupFailure: in case workdir is not a directory.
        """
        workdir = ''
        if 'workdir' in self.__payload:
            workdir = self.__payload['workdir']
            if not os.path.exists(workdir):
                os.makedirs(workdir)
            elif not os.path.isdir(workdir):
                raise SetupFailure('workdir exists but is not a directory')

        return workdir

    def get_executable(self, workdir: str) -> str:
        """Return the executable string.

        Args:
            workdir: work directory.

        Returns:
            str: executable string.
        """
        # self.get_payload_executable() is not defined in this class
        # executable = self.get_payload_executable(self.__payload['executable'])
        executable = 'undefined'
        return f'cd {workdir}; {executable}'

    def set_get_event_ranges_hook(self, hook: Any) -> None:
        """Set get_event_ranges hook.

        Args:
            hook: a hook method to get event ranges.
        """
        self.get_event_ranges_hook = hook

    def get_get_event_ranges_hook(self) -> Any:
        """Return get_event_ranges hook.

        Returns:
            Any: the hook method to get event ranges.
        """
        return self.get_event_ranges_hook

    def set_handle_out_message_hook(self, hook: Any) -> None:
        """Set handle_out_message hook.

        Args:
            hook: a hook method to handle payload output and error messages.
        """
        self.handle_out_message_hook = hook

    def get_handle_out_message_hook(self) -> Any:
        """Return handle_out_message hook.

        Returns:
            Any: the hook method to handle payload output and error messages.
        """
        return self.handle_out_message_hook

    def init(self) -> None:
        """Initialize message thread and payload process (incomplete implementation).

        Raises:
            Exception: when an Exception is caught.
        """
        try:
            pass
        except Exception as e:
            # TODO: raise exceptions
            self.__ret_code = -1
            self.stop()
            raise e

    def monitor(self) -> None:
        """Monitor whether a process is dead (incomplete implementation)."""
        return

    def has_running_children(self) -> bool:
        """Check whether it has running children (incomplete implementation).

        Returns:
            bool: True if there are alive children, otherwise False.
        """
        return False

    def is_payload_running(self) -> bool:
        """Check whether the payload is still running (incomplete implementation).

        Returns:
            bool: True if the payload is running, otherwise False.
        """
        return False

    def get_event_ranges(self, num_ranges: int = None, queue_factor: int = 1) -> list:
        """Call get_event_ranges hook to get event ranges.

        Args:
            num_ranges: number of event ranges to get.
            queue_factor: queue factor.

        Returns:
            list: list of event ranges.

        Raises:
            SetupFailure: if get_event_ranges_hook is not set.
            MessageFailure: when failed to get event ranges.
        """
        if not num_ranges:
            num_ranges = self.corecount

        logger.debug(f'getting event ranges(num_ranges={num_ranges})')
        if not self.get_event_ranges_hook:
            raise SetupFailure("get_event_ranges_hook is not set")

        try:
            logger.debug(f'calling get_event_ranges hook({self.get_event_ranges_hook}) to get event ranges.')
            event_ranges = self.get_event_ranges_hook(num_ranges, queue_factor=queue_factor)
            logger.debug(f'got event ranges: {event_ranges}')
            return event_ranges
        except Exception as e:
            raise MessageFailure(f"Failed to get event ranges: {e}") from e

    def parse_out_message(self, message: Any) -> Any:
        """Parse output or error messages from payload.

        Args:
            message: the message string received from payload.

        Returns:
            Any: dict with keys 'id', 'status', 'output', 'cpu', 'wall', 'message'.

        Raises:
            PilotException: when a PilotException is caught.
            UnknownException: when other unknown exception is caught.
        """
        logger.debug(f'parsing message: {message}')
        return message

    def handle_out_message(self, message: Any) -> None:
        """Handle output or error messages from payload.

        Messages from payload will be parsed and the handle_out_message hook is called.

        Args:
            message: the message string received from payload.

        Raises:
            SetupFailure: when handle_out_message_hook is not set.
            RunPayloadFailure: when failed to handle an output or error message.
        """
        logger.debug(f'handling out message: {message}')
        if not self.handle_out_message_hook:
            raise SetupFailure("handle_out_message_hook is not set")

        try:
            message_status = self.parse_out_message(message)
            logger.debug(f'parsed out message: {message_status}')
            logger.debug(f'calling handle_out_message hook({self.handle_out_message_hook}) to handle parsed message.')
            self.handle_out_message_hook(message_status)
        except Exception as e:
            raise RunPayloadFailure(f"Failed to handle out message: {e}") from e

    def poll(self) -> Any:
        """Poll whether the process is still running.

        Returns:
            Any: None if still running, 0 if finished successfully, non-zero int if failed.
        """
        return self.__ret_code

    def terminate(self, time_to_wait: int = 1) -> None:
        """Terminate running threads and processes.

        Args:
            time_to_wait: seconds to wait before force killing the payload process.

        Raises:
            PilotException: when a PilotException is caught.
            UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes')
        if time_to_wait:
            pass  # to get rid of pylint warning
        try:
            self.stop()
        except Exception as e:
            logger.error(f'Exception caught when terminating ESProcessFineGrainedProc: {e}')
            self.__ret_code = -1
            raise UnknownException(e) from e

    def kill(self) -> None:
        """Terminate running threads and processes.

        Raises:
            PilotException: when a PilotException is caught.
            UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes.')
        try:
            self.stop()
        except Exception as e:
            logger.error(f'Exception caught when terminating ESProcessFineGrainedProc: {e}')
            raise UnknownException(e) from e

    def clean(self) -> None:
        """Clean left resources."""
        self.stop()

    def run(self) -> None:
        """Run main loops.

        1. Monitor message thread and payload process.
        2. Handle messages from payload and response messages with injecting new event ranges or process outputs.

        Raises:
            PilotException: when a PilotException is caught.
            UnknownException: when other unknown exception is caught.
        """
        logger.info(f'start esprocess with thread ident: {self.ident}')
        logger.debug('initializing')
        self.init()
        logger.debug('initialization finished.')

        logger.info('starts to main loop')
        while self.is_payload_running():
            try:
                self.monitor()
                time.sleep(0.01)
            except PilotException as e:
                logger.error(f'PilotException caught in the main loop: {e.get_detail()}, {traceback.format_exc()}')
                # TODO: define output message exception. If caught 3 output message exception, terminate
                self.stop()
            except Exception as e:
                logger.error(f'exception caught in the main loop: {e}, {traceback.format_exc()}')
                # TODO: catch and raise exceptions
                # if catching dead process exception, terminate.
                self.stop()
                break
        self.clean()
        logger.debug('main loop finished')
