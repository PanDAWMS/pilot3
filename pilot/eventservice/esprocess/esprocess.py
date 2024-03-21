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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

"""
Event Service process.

Main process to handle event service.
It makes use of two hooks get_event_ranges_hook and handle_out_message_hook to communicate with other processes when
it's running. The process will handle the logic of Event service independently.
"""

import io
import json
import logging
import os
import queue
import re
import subprocess
import time
import threading
import traceback
from typing import (
    Any,
    TextIO
)

from pilot.common.exception import (
    PilotException,
    MessageFailure,
    SetupFailure,
    RunPayloadFailure,
    UnknownException
)
from pilot.eventservice.esprocess.esmessage import MessageThread
from pilot.util.container import containerise_executable
from pilot.util.processes import kill_child_processes

logger = logging.getLogger(__name__)
athenopts_re = re.compile(r'--athenaopts=\'([\w\=\-\"\' ]+)\'')


class ESProcess(threading.Thread):
    """Main EventService Process."""

    def __init__(self, payload, waiting_time=30 * 60):
        """
        Initialize ESProcess.

        :param payload: {'executable': <cmd string>, 'output_file': <filename>, 'error_file': <filename>} (dict)
        :param waiting_time: waiting time for no more events (int).
        """
        threading.Thread.__init__(self, name='esprocess')

        self.__message_queue = queue.Queue()
        self.__payload = payload

        self.__message_thread = None
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
        self.setName("ESProcess")
        self.corecount = 1

        self.event_ranges_cache = []

    def __del__(self):
        """Handle destruction."""
        if self.__message_thread:
            self.__message_thread.stop()

    def is_payload_started(self):
        """
        Check whether the payload has started.

        :return: True if the payload has started, otherwise False (bool).
        """
        return self.__is_payload_started

    def stop(self, delay=1800):
        """
        Stop the process.

        :param delay: waiting time to stop the process (int).
        """
        if not self.__stop.is_set():
            self.__stop.set()
            self.__stop_set_time = time.time()
            self.__stop_delay = delay
            event_ranges = "No more events"
            self.send_event_ranges_to_payload(event_ranges)

    def init_message_thread(self, socketname: str = None, context: str = 'local'):
        """
        Initialize message thread.

        :param socket_name: name of the socket between current process and payload (str)
        :param context: name of the context between current process and payload, default is 'local' (str)
        :raises MessageFailure: when failed to init message thread.
        """
        logger.info("start to initialize message thread")
        try:
            self.__message_thread = MessageThread(self.__message_queue, socketname, context)
            self.__message_thread.start()
        except PilotException as exc:
            logger.error(f"failed to start message thread: {exc.get_detail()}")
            self.__ret_code = -1
        except Exception as exc:
            logger.error(f"failed to start message thread: {exc}")
            self.__ret_code = -1
            raise MessageFailure(exc)
        logger.info("finished initializing message thread")

    def stop_message_thread(self):
        """Stop message thread."""
        logger.info("Stopping message thread")
        if self.__message_thread:
            while self.__message_thread.is_alive():
                if not self.__message_thread.is_stopped():
                    self.__message_thread.stop()
        logger.info("Message thread stopped")

    def init_yampl_socket(self, executable: str) -> str:
        """
        Initialize yampl socket.

        :param executable: executable string.
        :return: executable string with yampl socket name (str).
        """
        socket_name = self.__message_thread.get_yampl_socket_name()

        is_ca = "--CA" in executable
        is_mt = "--multithreaded=true" in executable.lower()
        if is_ca:
            if is_mt:
                preexec_socket_config = f" --mtes=True --mtes_channel=\"{socket_name}\" "
            else:
                preexec_socket_config = f" --preExec 'ConfigFlags.MP.EventRangeChannel=\"{socket_name}\"' "
        else:
            preexec_socket_config = \
                f" --preExec 'from AthenaMP.AthenaMPFlags import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"{socket_name}\"' "
            if is_mt:
                logger.warning("event service is not supported in MT job without CA")

        if "PILOT_EVENTRANGECHANNEL" in executable:
            executable = f"export PILOT_EVENTRANGECHANNEL=\"{socket_name}\"; " + executable
        elif is_mt and is_ca:
            has_opts = "--athenaopts" in executable
            if has_opts:
                executable = athenopts_re.sub(fr"--athenaopts='\1 {preexec_socket_config}'", executable)
            else:
                executable = executable.strip()
                if executable.endswith(";"):
                    executable = executable[:-1]
                executable += f" --athenaopts='{preexec_socket_config}' "

        elif "--preExec" not in executable:
            executable = executable.strip()
            if executable.endswith(";"):
                executable = executable[:-1]
            executable += preexec_socket_config
        else:
            if "import jobproperties as jps" in executable:
                executable = executable.replace("import jobproperties as jps;",
                                                f"import jobproperties as jps;jps.AthenaMPFlags.EventRangeChannel=\"{socket_name}\";")
                if is_ca:
                    logger.warning("Found jobproperties config in CA job")
            else:
                if "--preExec " in executable:
                    executable = executable.replace("--preExec ", preexec_socket_config)
                else:
                    logger.warning(f"--preExec has an unknown format - expected '--preExec \"' or \"--preExec '\", got: {executable}")

        return executable

    def init_payload_process(self):
        """
        Initialize payload process.

        :raise SetupFailure: when failed to init payload process.
        """
        logger.info("initializing payload process")
        try:
            try:
                workdir = self.get_workdir()
            except Exception as exc:
                raise exc

            executable = self.get_executable(workdir)
            output_file_fd = self.get_file(workdir, file_label='output_file', file_name='ES_payload_output.txt')
            error_file_fd = self.get_file(workdir, file_label='error_file', file_name='ES_payload_error.txt')

            # containerise executable if required
            if 'job' in self.__payload and self.__payload['job']:
                try:
                    executable, diagnostics = containerise_executable(executable, job=self.__payload['job'], workdir=workdir)
                    if diagnostics:
                        msg = f'containerisation of executable failed: {diagnostics}'
                        logger.warning(msg)
                        raise SetupFailure(msg)
                except Exception as e:
                    msg = f'exception caught while preparing container command: {e}'
                    logger.warning(msg)
                    raise SetupFailure(msg)
            else:
                logger.warning('could not containerise executable')

            # get the process
            self.__process = subprocess.Popen(executable, stdout=output_file_fd, stderr=error_file_fd, shell=True)
            self.pid = self.__process.pid
            self.__payload['job'].pid = self.pid
            self.__is_payload_started = True
            logger.debug(f"started new processs (executable: {executable}, stdout: {output_file_fd}, "
                         f"stderr: {error_file_fd}, pid: {self.__process.pid})")
            if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].corecount:
                self.corecount = int(self.__payload['job'].corecount)
        except PilotException as exc:
            logger.error(f"failed to start payload process: {exc.get_detail()}, {traceback.format_exc()}")
            self.__ret_code = -1
        except Exception as exc:
            logger.error(f"failed to start payload process: {exc}, {traceback.format_exc()}")
            self.__ret_code = -1
            raise SetupFailure(exc)
        logger.info("finished initializing payload process")

    def get_file(self, workdir: str, file_label: str = 'output_file',
                 file_name: str = 'ES_payload_output.txt') -> TextIO:
        """
        Return the requested file.

        :param file_label: label of the file (str)
        :param workdir: work directory (str)
        :param file_name: name of the file (str)
        :return: file descriptor (TextIO).
        """
        file_type = io.IOBase

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

    def get_workdir(self) -> str:
        """
        Return the workdir.

        If the workdir is set but is not a directory, return None.

        :return: work directory (str)
        :raises SetupFailure: in case workdir is not a directory.
        """
        workdir = ''
        if 'workdir' in self.__payload:
            workdir = self.__payload['workdir']
            if not os.path.exists(workdir):
                try:
                    os.makedirs(workdir)
                except OSError as exc:
                    raise SetupFailure(f"failed to create workdir: {exc}")
            elif not os.path.isdir(workdir):
                raise SetupFailure('workdir exists but is not a directory')

        return workdir

    def get_executable(self, workdir: str) -> str:
        """
        Return the executable string.

        :param workdir: work directory (str)
        :return: executable (str).
        """
        executable = self.init_yampl_socket(self.__payload['executable'])
        return f'cd {workdir}; {executable}'

    def set_get_event_ranges_hook(self, hook: Any):
        """
        Set get_event_ranges hook.

        :param hook: a hook method to get event ranges (Any).
        """
        self.get_event_ranges_hook = hook

    def get_get_event_ranges_hook(self) -> Any:
        """
        Get get_event_ranges hook.

        :return: the hook method to get event ranges (Any).
        """
        return self.get_event_ranges_hook

    def set_handle_out_message_hook(self, hook: Any):
        """
        Set handle_out_message hook.

        :param hook: a hook method to handle payload output and error messages (Any).
        """
        self.handle_out_message_hook = hook

    def get_handle_out_message_hook(self) -> Any:
        """
        Get handle_out_message hook.

        :return: The hook method to handle payload output and error messages (Any).
        """
        return self.handle_out_message_hook

    def init(self):
        """
        Initialize message thread and payload process.

        :raises: SetupFailure, MessageFailure.
        """
        try:
            self.init_message_thread()
            self.init_payload_process()
        except Exception as exc:
            self.__ret_code = -1
            self.stop()
            raise exc

    def monitor(self):
        """
        Monitor whether a process is dead.

        raises: MessageFailure: when the message thread is dead or exited.
                RunPayloadFailure: when the payload process is dead or exited.
                Exception: when too long time since "No more events" is injected.
        """
        if self.__no_more_event_time and time.time() - self.__no_more_event_time > self.__waiting_time:
            self.__ret_code = -1
            raise Exception(f'Too long time ({time.time() - self.__no_more_event_time} seconds) '
                            f'since \"No more events\" is injected')

        if self.__monitor_log_time is None or self.__monitor_log_time < time.time() - 10 * 60:
            self.__monitor_log_time = time.time()
            logger.info('monitor is checking dead process.')

        if self.__message_thread is None:
            raise MessageFailure("Message thread has not started.")
        if not self.__message_thread.is_alive():
            raise MessageFailure("Message thread is not alive.")

        if self.__process is None:
            raise RunPayloadFailure("Payload process has not started.")
        if self.__process.poll() is not None:
            if self.is_no_more_events:
                logger.info("Payload finished with no more events")
            else:
                self.__ret_code = self.__process.poll()
                raise RunPayloadFailure(f"Payload process is not alive: {self.__process.poll()}")

        if self.__stop.is_set() and time.time() > self.__stop_set_time + self.__stop_delay:
            logger.info(f"Stop has been set for {self.__stop_delay} seconds, which is more than the stop wait time. Will terminate")
            self.terminate()

    def has_running_children(self) -> bool:
        """
        Check whether there are running children.

        :return: True if there are alive children, otherwise False (bool).
        """
        if self.__message_thread and self.__message_thread.is_alive():
            return True
        if self.__process and self.__process.poll() is None:
            return True

        return False

    def is_payload_running(self) -> bool:
        """
        Check whether the payload is still running.

        :return: True if the payload is running, otherwise False (bool).
        """
        if self.__process and self.__process.poll() is None:
            return True

        return False

    def get_event_range_to_payload(self) -> list:
        """
        Get one event range to be sent to payload.

        :return: list of event ranges (list).
        """
        logger.debug(f"number of cached event ranges: {len(self.event_ranges_cache)}")
        if not self.event_ranges_cache:
            event_ranges = self.get_event_ranges()
            if event_ranges:
                self.event_ranges_cache.extend(event_ranges)

        if self.event_ranges_cache:
            event_range = self.event_ranges_cache.pop(0)
            return event_range
        else:
            return []

    def get_event_ranges(self, num_ranges: int = None) -> list:
        """
        Call get_event_ranges hook to get event ranges.

        :param num_ranges: number of event ranges to get (int)
        :return: list of event ranges (list)
        :raises: SetupFailure: If get_event_ranges_hook is not set.
                 MessageFailure: when failed to get event ranges.
        """
        if not num_ranges:
            num_ranges = self.corecount

        logger.debug(f'getting event ranges(num_ranges={num_ranges})')
        if not self.get_event_ranges_hook:
            raise SetupFailure("get_event_ranges_hook is not set")

        try:
            logger.debug(f'calling get_event_ranges hook({self.get_event_ranges_hook}) to get event ranges.')
            event_ranges = self.get_event_ranges_hook(num_ranges)
            logger.debug(f'got event ranges: {event_ranges}')
            return event_ranges
        except Exception as e:
            raise MessageFailure(f"Failed to get event ranges: {e}")

    def send_event_ranges_to_payload(self, event_ranges: list):
        """
        Send event ranges to payload through message thread.

        :param event_ranges: list of event ranges (list).
        """
        if "No more events" in event_ranges:
            msg = event_ranges
            self.is_no_more_events = True
            self.__no_more_event_time = time.time()
        else:
            if type(event_ranges) is not list:
                event_ranges = [event_ranges]
            msg = json.dumps(event_ranges)
        logger.debug(f'send event ranges to payload: {msg}')
        self.__message_thread.send(msg)

    def parse_out_message(self, message: str) -> dict:
        """
        Parse output or error messages from payload.

        :param message: The message string received from payload (str)
        :return: {'id': <id>, 'status': <status>, 'output': <output>, 'cpu': <cpu>, 'wall': <wall>, 'message': <message>}
        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.debug(f'parsing message: {message}')
        try:
            if message.startswith("/"):
                parts = message.split(",")
                ret = {'output': parts[0]}
                parts = parts[1:]
                for part in parts:
                    name, value = part.split(":")
                    name = name.lower()
                    ret[name] = value
                ret['status'] = 'finished'
                return ret
            elif message.startswith('ERR'):
                if "ERR_ATHENAMP_PARSE" in message:
                    pattern = re.compile(r"(ERR\_[A-Z\_]+)\ (.+)\:\ ?(.+)")
                    found = re.findall(pattern, message)
                    event_range = found[0][1]
                    if "eventRangeID" in event_range:
                        pattern = re.compile(r"eventRangeID\'\:\ ?.?\'([0-9A-Za-z._\-]+)")
                        found = re.findall(pattern, event_range)
                        event_range_id = found[0]
                        ret = {'id': event_range_id, 'status': 'failed', 'message': message}
                        return ret
                    else:
                        raise Exception(f"Failed to parse {message}")
                else:
                    pattern = re.compile(r"(ERR\_[A-Z\_]+)\ ([0-9A-Za-z._\-]+)\:\ ?(.+)")
                    found = re.findall(pattern, message)
                    event_range_id = found[0][1]
                    ret = {'id': event_range_id, 'status': 'failed', 'message': message}
                    return ret
            else:
                raise UnknownException(f"Unknown message {message}")
        except PilotException as e:
            raise e
        except Exception as e:
            raise UnknownException(e)

    def handle_out_message(self, message: str):
        """
        Handle output or error messages from payload.

        Messages from payload will be parsed and the handle_out_message hook is called.

        :param message: message string received from payload (str)
        :raises: SetupFailure: when handle_out_message_hook is not set.
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
            raise RunPayloadFailure(f"Failed to handle out message: {e}")

    def handle_messages(self):
        """Monitor the message queue to get output or error messages from payload and response to different messages."""
        try:
            message = self.__message_queue.get(False)
        except queue.Empty:
            pass
        else:
            logger.debug(f'received message from payload: {message}')
            if "Ready for events" in message:
                event_ranges = self.get_event_range_to_payload()
                if not event_ranges:
                    event_ranges = "No more events"
                self.send_event_ranges_to_payload(event_ranges)
            else:
                self.handle_out_message(message)

    def poll(self) -> int:
        """
        Poll whether the process is still running.

        :returns: None: still running.
                  0: finished successfully.
                  others: failed.
        """
        return self.__ret_code

    def terminate(self, time_to_wait: int = 1):
        """
        Terminate running threads and processes.

        :param time_to_wait: integer, seconds to wait to force kill the payload process (int)
        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes.')
        try:
            self.stop()
            if self.__process:
                if not self.__process.poll() is None:
                    if self.__process.poll() == 0:
                        logger.info("payload finished successfully.")
                    else:
                        logger.error(f"payload finished with error code: {self.__process.poll()}")
                else:
                    for i in range(time_to_wait * 10):
                        if not self.__process.poll() is None:
                            break
                        time.sleep(1)

                    if not self.__process.poll() is None:
                        if self.__process.poll() == 0:
                            logger.info("payload finished successfully.")
                        else:
                            logger.error(f"payload finished with error code: {self.__process.poll()}")
                    else:
                        logger.info('terminating payload process.')
                        pgid = os.getpgid(self.__process.pid)
                        logger.info(f'got process group id for pid {self.__process.pid}: {pgid}')
                        logger.info(f'send SIGTERM to process: {self.__process.pid}')
                        kill_child_processes(self.__process.pid)
                self.__ret_code = self.__process.poll()
            else:
                self.__ret_code = -1
        except Exception as e:
            logger.error(f'Exception caught when terminating ESProcess: {e}')
            self.__ret_code = -1
            self.stop()
            raise UnknownException(e)

    def kill(self):
        """
        Terminate running threads and processes.

        :raises: PilotException: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.info('terminate running threads and processes.')
        try:
            self.stop()
            if self.__process:
                if not self.__process.poll() is None:
                    if self.__process.poll() == 0:
                        logger.info("payload finished successfully.")
                    else:
                        logger.error(f"payload finished with error code: {self.__process.poll()}")
                else:
                    logger.info('killing payload process.')
                    pgid = os.getpgid(self.__process.pid)
                    logger.info(f'got process group id for pid {self.__process.pid}: {pgid}')
                    logger.info(f'send SIGKILL to process: {self.__process.pid}')
                    kill_child_processes(self.__process.pid)
        except Exception as exc:
            logger.error(f'exception caught when terminating ESProcess: {exc}')
            self.stop()
            raise UnknownException(exc)

    def clean(self):
        """Clean left resources."""
        self.terminate()

    def run(self):
        """
        Run main loops.

        Monitor message thread and payload process.
        Handle messages from payload and response messages with injecting new event ranges or process outputs.

        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """
        logger.info(f'start esprocess with thread ident: {self.ident}')
        self.init()
        logger.debug('initialization finished.')

        logger.info('start main loop')
        while self.is_payload_running():
            try:
                self.monitor()
                self.handle_messages()
                time.sleep(0.01)
            except PilotException as e:
                logger.error(f'PilotException caught in the main loop: {e.get_detail()}, {traceback.format_exc()}')
                # TODO: define output message exception. If caught 3 output message exception, terminate
                self.stop()
            except Exception as exc:
                logger.error(f'exception caught in the main loop: {exc}, {traceback.format_exc()}')
                # TODO: catch and raise exceptions
                # if catching dead process exception, terminate.
                self.stop()
                break

        self.clean()
        self.stop_message_thread()
        logger.debug('main loop finished')
