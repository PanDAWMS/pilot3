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
# - Paul Nilsson, paul.nilsson@cern.ch, 2024-25

"""Main process to handle event service.

It makes use of two hooks get_event_ranges_hook and handle_out_message_hook to communicate with other
processes when it's running. The process will handle the logic of event service independently.
"""

import base64
import io
import json
import logging
import os
import queue
import re
import signal
import time
import threading
import traceback
from concurrent import futures
from typing import Any

# from pilot.util.auxiliary import set_pilot_state
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import (
    PilotException,
    MessageFailure,
    SetupFailure,
    RunPayloadFailure
)
from pilot.util.container import execute
from pilot.util.filehandling import read_file

logger = logging.getLogger(__name__)
errors = ErrorCodes()


class ESRunnerThreadPool(futures.ThreadPoolExecutor):
    """ThreadPoolExecutor extended with event-service tracking methods."""

    def __init__(self, max_workers: int = None, thread_name_prefix: str = '', initializer: Any = None, initargs: tuple = ()):
        """Initialize the thread pool with event and output tracking structures.

        Args:
            max_workers: Maximum number of worker threads.
            thread_name_prefix: Prefix for worker thread names.
            initializer: Optional callable run at thread start.
            initargs: Arguments passed to *initializer*.
        """
        self.futures = {}
        self.outputs = {}
        self._lock = threading.RLock()
        self.max_workers = max_workers
        super().__init__(max_workers=max_workers,
                         thread_name_prefix=thread_name_prefix,
                         initializer=initializer,
                         initargs=initargs)

    def submit(self, fn, *args, **kwargs):
        """Submit a callable and return the resulting Future.

        Args:
            fn: Callable to execute in the thread pool.
            *args: Positional arguments forwarded to *fn*.
            **kwargs: Keyword arguments forwarded to *fn*.

        Returns:
            Future representing the execution of the callable.
        """
        future = super().submit(fn, *args, **kwargs)
        return future

    def run_event(self, fn, event):
        """Submit an event for processing and register its Future.

        Args:
            fn: Callable that processes a single event dict.
            event: Event dict containing at least ``eventRangeID``.
        """
        future = super().submit(fn, event)
        with self._lock:
            self.futures[event['eventRangeID']] = {'event': event, 'future': future}

    def scan(self):
        """Move completed futures into the outputs dict."""
        with self._lock:
            for event_range_id in list(self.futures.keys()):
                event_future = self.futures[event_range_id]
                future = event_future['future']
                if future.done():
                    result = future.result()
                    self.outputs[event_range_id] = {'event': self.futures[event_range_id]['event'], 'result': result}
                    del self.futures[event_range_id]

    def get_outputs(self):
        """Return and clear all completed event results.

        Returns:
            List of result objects for all completed events.
        """
        outputs = []
        with self._lock:
            for event_range_id in self.outputs:
                outputs.append(self.outputs[event_range_id]['result'])
            self.outputs = {}
        return outputs

    def get_max_workers(self):
        """Return the maximum number of worker threads.

        Returns:
            Maximum worker count as set at construction time.
        """
        return self.max_workers

    def get_num_running_workers(self):
        """Return the number of currently running worker futures.

        Returns:
            Count of in-flight futures.
        """
        return len(list(self.futures.keys()))

    def get_num_free_workers(self):
        """Return the number of idle worker slots.

        Returns:
            Difference between max workers and currently running workers.
        """
        return self.max_workers - self.get_num_running_workers()


class ESProcessFineGrainedProc(threading.Thread):
    """Main event-service process thread for the Rubin fine-grained executor."""
    def __init__(self, payload, waiting_time=30 * 60):
        """Initialize ESProcessFineGrainedProc.

        Args:
            payload: dict with keys ``executable``, ``output_file``, ``error_file``, ``workdir``, and ``job``.
            waiting_time: seconds to wait for more events before declaring no-more-events; default 30 minutes.
        """
        threading.Thread.__init__(self, name='esprocessFineGrainedProc')

        self.__payload = payload

        self.__thread_pool = None

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
        self.event_execution_time = None

        self.rubin_es_map = {}

        self._worker_id = -1
        self._lock = threading.RLock()

    def __del__(self):
        """Shut down the thread pool on object destruction."""
        if self.__thread_pool:
            del self.__thread_pool

    def is_payload_started(self):
        """Return True if the payload has started.

        Returns:
            bool: True once the run loop has begun.
        """
        return self.__is_payload_started

    def stop(self, delay=1800):
        """Signal the process to stop and shut down the thread pool.

        Args:
            delay: Seconds to allow for graceful shutdown before forcing termination.
        """
        if not self.__stop.is_set():
            self.__stop.set()
            self.__stop_set_time = time.time()
            self.__stop_delay = delay
        self.close_logs()
        self.__thread_pool.shutdown(wait=False)

    def get_job_id(self) -> int:
        """Return the job ID from the payload, or 0 if not set.

        Returns:
            int: job ID.
        """
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].jobid:
            return self.__payload['job'].jobid
        return 0

    def get_job(self):
        """Return the job object from the payload, or None if not set.

        Returns:
            Job object or None.
        """
        if 'job' in self.__payload and self.__payload['job']:
            return self.__payload['job']
        return None

    def get_transformation(self):
        """Return the full path to the transformation executable in the workdir.

        Returns:
            str: Path to the transformation script, or None if not available.
        """
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].transformation:
            base_transform = os.path.basename(self.__payload['job'].transformation)
            transform = os.path.join(self.__payload['job'].workdir, base_transform)
            return transform
        return None

    def get_corecount(self):
        """Return the number of cores to use for event processing.

        Reads ``RUBIN_ES_CORES`` env var first, then the job corecount, defaulting to 1.

        Returns:
            int: core count.
        """
        try:
            if os.environ.get("RUBIN_ES_CORES", None) is not None:
                rubin_es_cores = int(os.environ.get("RUBIN_ES_CORES"))
                return rubin_es_cores
        except Exception as ex:
            logger.warning(f"RUBIN_ES_CORES is not defined correctly: {ex}")

        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].corecount:
            core_count = int(self.__payload['job'].corecount)
            return core_count
        return 1

    def get_file(self, workdir, file_label='output_file', file_name='payload.stdout'):
        """Return the requested file.

        Args:
            workdir: work directory.
            file_label: label for the file in the payload dict.
            file_name: fallback file name.

        Returns:
            file object.
        """
        file_type = io.IOBase

        if file_label in self.__payload:
            if isinstance(self.__payload[file_label], file_type):
                _file_fd = self.__payload[file_label]
            else:
                _file = self.__payload[file_label] if '/' in self.__payload[file_label] else os.path.join(workdir, self.__payload[file_label])
                _file_fd = open(_file, 'w', encoding='utf-8')
        else:
            _file = os.path.join(workdir, file_name)
            _file_fd = open(_file, 'w', encoding='utf-8')

        return _file_fd

    def get_workdir(self) -> str:
        """Return the workdir.

        If the workdir is set but is not a directory, return None.

        Returns:
            str: workdir (string or None).

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
            workdir: work directory (string).

        Returns:
            str: executable (string).
        """
        executable = self.__payload['executable']
        # return 'cd %s; %s' % (workdir, executable)
        return executable

    def init_logs(self):
        """Initialize stdout, stderr, and real-time log file handles and queues."""
        workdir = self.get_workdir()
        # logger.info("payload: %s", str(self.__payload))
        output_file_fd = self.get_file(workdir, file_label='output_file', file_name='payload.stdout')
        error_file_fd = self.get_file(workdir, file_label='error_file', file_name='payload.stderr')

        self.stdout_queue = queue.Queue()
        self.stderr_queue = queue.Queue()
        self.stdout_file = output_file_fd
        self.stderr_file = error_file_fd

        logger.info(f"stdout_file: {self.stdout_file}; stderr_file: {self.stderr_file}")

        realtime_log_files = os.environ.get('REALTIME_LOGFILES', None)
        realtime_log_files = re.split('[:,]', realtime_log_files)
        # realtime_log_files = [os.path.join(event_dir, f) for f in realtime_log_files]
        self.realtime_log_queues = {}
        self.realtime_log_files = {}
        for realtime_log_file in realtime_log_files:
            self.realtime_log_queues[realtime_log_file] = queue.Queue()
            self.realtime_log_files[realtime_log_file] = self.get_file(workdir, file_label=realtime_log_file, file_name=realtime_log_file)
            logger.info(f"realtime log {realtime_log_file}: {self.realtime_log_files[realtime_log_file]}")
        logger.info(f"self.realtime_log_queues: {self.realtime_log_queues}")

    def write_logs_from_queue(self):
        """Drain stdout, stderr, and real-time log queues into their respective files."""
        while not self.stdout_queue.empty():
            item = self.stdout_queue.get(block=False)
            itemb = item.encode('utf-8')
            self.stdout_file.write(itemb)
            # logger.debug("write stdout_file: %s" % item)
        while not self.stderr_queue.empty():
            item = self.stderr_queue.get(block=False)
            itemb = item.encode('utf-8')
            self.stderr_file.write(itemb)
            # logger.debug("write stderr_file: %s" % item)

        for fd in self.realtime_log_queues:
            while not self.realtime_log_queues[fd].empty():
                item = self.realtime_log_queues[fd].get(block=False)
                self.realtime_log_files[fd].write(json.dumps(item))
                # logger.debug("write realtime log %s: %s" % (fd, item))

    def close_logs(self):
        """Close all open log file handles."""
        try:
            # cmd = "pwd; ls -ltr"
            # execute(cmd, stdout=self.stdout_file, stderr=self.stderr_file, timeout=120)
            self.stdout_file.close()
            self.stderr_file.close()
            for fd in self.realtime_log_files:
                self.realtime_log_files[fd].close()
        except Exception as ex:
            logger.error(f"Failed to close logs: {ex}")

    def set_get_event_ranges_hook(self, hook) -> None:
        """Set get_event_ranges hook.

        Args:
            hook: a hook method to get event ranges.
        """
        self.get_event_ranges_hook = hook

    def get_get_event_ranges_hook(self):
        """Get get_event_ranges hook.

        Returns:
            The hook method to get event ranges.
        """
        return self.get_event_ranges_hook

    def set_handle_out_message_hook(self, hook) -> None:
        """Set handle_out_message hook.

        Args:
            hook: a hook method to handle payload output and error messages.
        """
        self.handle_out_message_hook = hook

    def get_handle_out_message_hook(self):
        """Get handle_out_message hook.

        Returns:
            The hook method to handle payload output and error messages.
        """
        return self.handle_out_message_hook

    def init(self) -> None:
        """Initialize message thread and payload process."""
        try:
            self.init_logs()
            self.__thread_pool = ESRunnerThreadPool(max_workers=self.get_corecount(),
                                                    thread_name_prefix='ESProcessRunner')
        except Exception as e:
            # TODO: raise exceptions
            self.__ret_code = -1
            self.stop()
            raise e

    def try_get_events(self, num_free_workers):
        """Fetch event ranges for all free worker slots and mark no-more-events when exhausted.

        Args:
            num_free_workers: Number of idle worker slots to fill.

        Returns:
            List of event range dicts, or an empty list when none are available.
        """
        events = []
        if num_free_workers:
            queue_factor = 1
            if self.event_execution_time and self.event_execution_time < 10 * 60:      # 10 minutes
                queue_factor = int(10 * 60 / self.event_execution_time)
            events = self.get_event_ranges(num_ranges=num_free_workers, queue_factor=queue_factor)
            if not events:
                self.is_no_more_events = True
                self.__no_more_event_time = time.time()
        return events

    def get_event_dir(self, event_range_id):
        """Return (and create if needed) the per-event working directory.

        Args:
            event_range_id: Event range identifier used as the directory name.

        Returns:
            str: Path to the event-specific subdirectory.
        """
        work_dir = self.get_workdir()
        event_dir = os.path.join(work_dir, event_range_id)
        if not os.path.exists(event_dir):
            os.makedirs(event_dir)
        return event_dir

    def get_env_item(self, env, str_item):
        """Extract the value of an env-var assignment from a semicolon-separated string.

        Args:
            env: Environment variable name including ``=`` (e.g. ``'FOO='``).
            str_item: Semicolon-separated string of ``KEY=value`` tokens.

        Returns:
            Value string after the ``=``, or None if *env* is not found.
        """
        items = str_item.replace(" ", ";").split(";")
        for item in items:
            if env in item:
                return item.replace(env, "")
        return None

    def get_event_range_map_info(self):
        """Populate ``rubin_es_map`` from ``RUBIN_ES_MAP_FILE`` or ``RUBIN_ES_MAP`` embedded in the executable."""
        executable = self.get_executable(self.get_workdir())
        exec_list = executable.split(" ")
        es_map_env, es_map_file = None, None
        for exec_item in exec_list:
            new_exec_item = None
            if self.is_base64(exec_item):
                new_exec_item = self.decode_base64(exec_item)
            else:
                new_exec_item = exec_item

            if "RUBIN_ES_MAP_FILE=" in new_exec_item:
                es_map_file = self.get_env_item("RUBIN_ES_MAP_FILE=", new_exec_item)
            if "RUBIN_ES_MAP=" in new_exec_item:
                es_map_env = self.get_env_item("RUBIN_ES_MAP=", new_exec_item)

        self.rubin_es_map = {}
        if es_map_file:
            try:
                with open(es_map_file, encoding='utf-8') as f:
                    rubin_es_map_from_file_content = json.load(f)
                    self.rubin_es_map.update(rubin_es_map_from_file_content)
            except Exception as ex:
                logger.error(f"failed to load RUBIN_ES_MAP_FILE: {ex}")
        if es_map_env:
            try:
                rubin_es_map_from_env = json.loads(es_map_env)
                self.rubin_es_map.update(rubin_es_map_from_env)
            except Exception as ex:
                logger.error(f"failed to load RUBIN_ES_MAP: {ex}")

    def get_event_range_file_map(self, event):
        """Return a mapping from input-file name to event-range identifier for *event*.

        Args:
            event: Event range dict containing ``LFN`` and ``startEvent`` keys.

        Returns:
            dict: Mapping of ``{input_file_name: range_id}``.
        """
        if not self.rubin_es_map:
            self.get_event_range_map_info()
        # input_file = self.__payload['job'].input_file
        # return {input_file: event['eventRangeID']}
        # label = input_file.split(":")[0]

        lfn = event['LFN']
        label = lfn.split(":")[1]
        input_file = lfn.split(":")[2]
        input_file_name = label + ":" + input_file
        event_base_index = int(input_file.split("_")[1])
        event_index = int(event['startEvent'])
        event_abs_index = str(event_base_index + event_index - 1)
        if label in self.rubin_es_map and event_abs_index in self.rubin_es_map[label]:
            return {input_file_name: self.rubin_es_map[label][event_abs_index]}
        return {input_file_name: input_file_name + "^" + str(event_index)}

    def is_base64(self, sb):
        """Return True if *sb* is a valid Base64-encoded string.

        Args:
            sb: String or bytes to test.

        Returns:
            bool: True if *sb* round-trips through base64 decode/encode unchanged.
        """
        try:
            if isinstance(sb, str):
                sb_bytes = bytes(sb, 'ascii')
            elif isinstance(sb, bytes):
                sb_bytes = sb
            else:
                return False
            return base64.b64encode(base64.b64decode(sb_bytes)) == sb_bytes
        except Exception:
            # logger.error("is_base64 %s: %s" % (sb, ex))
            return False

    def decode_base64(self, sb):
        """Decode a Base64-encoded string or bytes to a UTF-8 string.

        Args:
            sb: Base64-encoded string or bytes.

        Returns:
            Decoded UTF-8 string, or *sb* unchanged on failure or unsupported type.
        """
        try:
            if isinstance(sb, str):
                sb_bytes = bytes(sb, 'ascii')
            elif isinstance(sb, bytes):
                sb_bytes = sb
            else:
                return sb
            return base64.b64decode(sb_bytes).decode("utf-8")
        except Exception as ex:
            logger.error(f"decode_base64 {sb}: {ex}")
            return sb

    def encode_base64(self, sb):
        """Encode a string or bytes to a Base64 ASCII string.

        Args:
            sb: String or bytes to encode.

        Returns:
            Base64-encoded ASCII string, or None if encoding is not possible.
        """
        try:
            sb_bytes = None
            if isinstance(sb, str):
                sb_bytes = bytes(sb, 'ascii')
            elif isinstance(sb, bytes):
                sb_bytes = sb
            return base64.b64encode(sb_bytes).decode("utf-8") if sb_bytes else None
        except Exception as ex:
            logger.error(f"encode_base64 {sb}: {ex}")
            return sb

    def replace_executable(self, executable, event_range_file_map):
        """Substitute input-file references in the executable string with event-range identifiers.

        Args:
            executable: Shell command string, possibly containing Base64-encoded tokens.
            event_range_file_map: Mapping of ``{input_file: range_id}`` to substitute.

        Returns:
            str: Modified executable string with substitutions applied.
        """
        exec_list = executable.split(" ")
        new_exec_list = []
        for exec_item in exec_list:
            new_exec_item = None
            if self.is_base64(exec_item):
                new_exec_item = self.decode_base64(exec_item)
                for input_file in event_range_file_map:
                    new_exec_item = new_exec_item.replace(input_file, event_range_file_map[input_file])
                new_exec_item = self.encode_base64(new_exec_item)
            else:
                new_exec_item = exec_item
                for input_file in event_range_file_map:
                    new_exec_item = new_exec_item.replace(input_file, event_range_file_map[input_file])
            new_exec_list.append(new_exec_item)
        return " ".join(new_exec_list)

    def get_event_executable(self, event_dir, event):
        """Build the per-event executable command and open its stdout/stderr/realtime-log files.

        Args:
            event_dir: Working directory for this event.
            event: Event range dict used to resolve input-file substitutions.

        Returns:
            Tuple of ``(executable, stdout_file, stderr_file, stdout_filename, stderr_filename, realtime_log_files)``.
        """
        executable = self.get_executable(event_dir)
        event_range_file_map = self.get_event_range_file_map(event)
        executable = self.replace_executable(executable, event_range_file_map)
        # executable = "cd  " + event_dir + "; " + executable

        transformation = self.get_transformation()
        # base_transformation = os.path.basename(transformation)

        executable = "cp -f " + transformation + " " + event_dir + "; cd  " + event_dir + "; " + executable

        stdout_filename = os.path.join(event_dir, "payload.stdout")
        stderr_filename = os.path.join(event_dir, "payload.stderr")

        stdout_file = open(stdout_filename, 'a', encoding='utf-8')
        stderr_file = open(stderr_filename, 'a', encoding='utf-8')
        realtime_log_files = os.environ.get('REALTIME_LOGFILES', None)
        realtime_log_files = re.split('[:,]', realtime_log_files)
        realtime_log_files = [os.path.join(event_dir, f) for f in realtime_log_files]
        return executable, stdout_file, stderr_file, stdout_filename, stderr_filename, realtime_log_files

    def get_worker_id(self):
        """Return a unique, monotonically increasing worker ID under a lock.

        Returns:
            int: Next available worker ID.
        """
        worker_id = None
        with self._lock:
            self._worker_id += 1
            worker_id = self._worker_id
        return worker_id

    def open_log_file(self, filename, perm='r'):
        """Open *filename* and seek to the beginning if it exists, otherwise return None.

        Args:
            filename: Path to the log file.
            perm: File-open mode string; defaults to ``'r'``.

        Returns:
            Open file object, or None if the file does not exist.
        """
        if os.path.exists(filename):
            fd = open(filename, perm, encoding='utf-8')
            fd.seek(0)
            return fd
        return None

    def redirect_logs(self, graceful_stop, worker_id, stdout_filename, stderr_filename, realtime_log_files, event_dir):    # noqa C901
        stdout_file = None
        stderr_file = None
        realtime_logs = {}
        for rt in realtime_log_files:
            realtime_logs[rt] = None
        # logger.debug("self.realtime_log_queues: %s" % str(self.realtime_log_queues))
        while not graceful_stop.is_set():
            try:
                if stdout_file is None:
                    stdout_file = self.open_log_file(stdout_filename)
                if stderr_file is None:
                    stderr_file = self.open_log_file(stderr_filename)
                for rt in realtime_logs:
                    if realtime_logs[rt] is None:
                        realtime_logs[rt] = self.open_log_file(rt)

                if stdout_file:
                    # logger.debug("stdout_file location: %s" % stdout_file.tell())
                    lines = stdout_file.readlines()
                    for line in lines:
                        line = f"Worker {worker_id}: " + line
                        self.stdout_queue.put(line)
                if stderr_file:
                    lines = stderr_file.readlines()
                    for line in lines:
                        line = f"Worker {worker_id}: " + line
                        self.stderr_queue.put(line)
                for rt in realtime_logs:
                    if realtime_logs[rt]:
                        lines = realtime_logs[rt].readlines()
                        rt_base = os.path.basename(rt)
                        for line in lines:
                            try:
                                line = json.loads(line)
                                line.update({'worker_id': worker_id})
                            except Exception:
                                line = f"Worker {worker_id}: " + line
                            self.realtime_log_queues[rt_base].put(line)

                time.sleep(0.1)
            except Exception as ex:
                logger.warning(ex)
                logger.debug(traceback.format_exc())

        try:
            # cmd = "cd %s; pwd; ls -ltr" % event_dir
            # ls_status, ls_stdout, ls_stderr = execute(cmd, timeout=120)
            # logger.info("list files status: %s, output: %s, error: %s" % (ls_status, ls_stdout, ls_stderr))

            if stdout_file is None:
                stdout_file = self.open_log_file(stdout_filename)
            if stderr_file is None:
                stderr_file = self.open_log_file(stderr_filename)
            for rt in realtime_logs:
                if realtime_logs[rt] is None:
                    realtime_logs[rt] = self.open_log_file(rt)

            if stdout_file:
                lines = stdout_file.readlines()
                for line in lines:
                    line = f"Worker {worker_id}: " + line
                    self.stdout_queue.put(line)
                stdout_file.close()
            if stderr_file:
                lines = stderr_file.readlines()
                for line in lines:
                    line = f"Worker {worker_id}: " + line
                    self.stderr_queue.put(line)
                stderr_file.close()
            for rt in realtime_logs:
                if realtime_logs[rt]:
                    lines = realtime_logs[rt].readlines()
                    rt_base = os.path.basename(rt)
                    for line in lines:
                        try:
                            line = json.loads(line)
                            line.update({'worker_id': worker_id})
                        except Exception:
                            line = f"Worker {worker_id}: " + line
                        self.realtime_log_queues[rt_base].put(line)
                    realtime_logs[rt].close()
        except Exception as ex:
            logger.warning(ex)
            logger.debug(traceback.format_exc())

    def wait_graceful(self, proc: Any) -> int:
        """Wait for payload process to finish.

        Args:
            proc: subprocess object (Any).

        Returns:
            int: exit code.
        """
        breaker = False
        exit_code = None
        iteration = 0
        while True:
            time.sleep(0.1)

            iteration += 1
            for _ in range(60):
                if self.__stop.is_set():
                    breaker = True
                    logger.info(f'breaking -- sending SIGTERM to pid={proc.pid}')
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    break
                exit_code = proc.poll()
                if exit_code is not None:
                    break
                time.sleep(1)
            if breaker:
                logger.info(f'breaking -- sleep 3s before sending SIGKILL pid={proc.pid}')
                time.sleep(3)
                proc.kill()
                break

            exit_code = proc.poll()

            if iteration % 10 == 0:
                logger.info(f'running: iteration={iteration} pid={proc.pid} exit_code={exit_code}')
            if exit_code is not None:
                break
            continue

        return exit_code

    def run_event(self, event):
        """Execute a single event range as a subprocess and return its result.

        Args:
            event: Event range dict containing at least ``eventRangeID``, ``LFN``, and ``startEvent``.

        Returns:
            dict: Result with keys ``id``, ``status``, ``error_code``, ``error_diag``, and ``wall_time``.
        """
        time_start = time.time()
        ret = {}
        worker_id = self.get_worker_id()
        log_prefix = f"worker_id={worker_id}: "
        try:
            event_range_id = event['eventRangeID']
            logger.info(log_prefix + f"start to run event {event_range_id}")

            event_dir = self.get_event_dir(event_range_id)
            executable, stdout_file, stderr_file, stdout_filename, stderr_filename, realtime_log_files = self.get_event_executable(event_dir, event)
            logger.info(log_prefix + "executable: " + executable)
            logger.info(log_prefix + "stdout: " + stdout_filename)
            logger.info(log_prefix + "stderr: " + stderr_filename)

            # exit_code, stdout, stderr = execute(executable, workdir=event_dir, returnproc=True, stdout=stdout_file, stderr=stderr_file,
            #                                     cwd=event_dir, timeout=7 * 24 * 3600)
            # logger.info(log_prefix + "exit_code: " + str(exit_code))
            # logger.info(log_prefix + "stdout: " + str(stdout))
            # logger.info(log_prefix + "stderr: " + str(stderr))
            try:
                proc = execute(executable, returnproc=True, stdout=stdout_file, stderr=stderr_file, timeout=7 * 24 * 3600)
            except Exception as error:
                logger.error(f'could not execute: {error}')
                raise Exception(f'could not execute: {error}') from error
            if isinstance(proc, tuple) and not proc[0]:
                logger.error('failed to execute payload')
                raise Exception('failed to execute payload')

            logger.info(f'started -- pid={proc.pid} executable={executable}')
            # job = self.get_job()
            # if job:
            #     job.pid = proc.pid
            #     job.pgrp = os.getpgid(job.pid)
            #     set_pilot_state(job=job, state="running")

            # start a thread to redirect stdout/stderr and realtime logging
            graceful_stop = threading.Event()
            log_redirect_thread = threading.Thread(target=self.redirect_logs,
                                                   args=(graceful_stop, worker_id, stdout_filename, stderr_filename, realtime_log_files, event_dir))
            log_redirect_thread.start()

            exit_code = self.wait_graceful(proc)
            logger.info(log_prefix + f"exit_code: {exit_code}")
            stdout_file.close()
            stderr_file.close()

            cmd = f"cd {event_dir}; pwd; ls -ltr"
            ls_status, ls_stdout, ls_stderr = execute(cmd, timeout=120)
            logger.info(f"list files status: {ls_status}, output: {ls_stdout}, error: {ls_stderr}")

            # log_redirect_thread.stop()
            time.sleep(2)
            logger.info(log_prefix + "stopping log_redirect_thread")
            graceful_stop.set()

            diagnostics = None
            if exit_code:
                logger.warning(f'payload returned exit code={exit_code}')
                stdout = read_file(stdout_filename)
                stderr = read_file(stderr_filename)
                err_msg = errors.extract_stderr_error(stderr)
                if err_msg == "":
                    err_msg = errors.extract_stderr_warning(stderr)

                diagnostics = stderr + stdout if stdout and stderr else 'General payload setup verification error (check setup logs)'
                # check for special errors in thw output
                _exit_code, error_message = errors.resolve_transform_error(exit_code, diagnostics)
                if error_message:
                    logger.warning(f"found apptainer error in stderr: {error_message}")
                    if exit_code == 0 and _exit_code != 0:
                        logger.warning("will overwrite trf exit code 0 due to previous error")
                # need to pass the exit_code to the job.
                # exit_code = _exit_code
                # diagnostics = errors.format_diagnostics(exit_code, diagnostics)

                diagnostics = errors.format_diagnostics(exit_code, err_msg)
                _, diagnostics = errors.add_error_code(exit_code, msg=diagnostics)
            if stdout_file:
                stdout_file.close()
                logger.debug(f'closed {stdout_filename}')
            if stderr_file:
                stderr_file.close()
                logger.debug(f'closed {stderr_filename}')
            if exit_code:
                self.__ret_code = exit_code
                ret = {'id': event_range_id, 'status': 'failed', 'error_code': exit_code, 'error_diag': diagnostics}
            else:
                ret = {'id': event_range_id, 'status': 'finished', 'error_code': exit_code, 'error_diag': diagnostics}
        except Exception as ex:
            logger.error(ex)
            logger.error(traceback.format_exc())
            ret = {'id': event_range_id, 'status': 'failed', 'error_code': -1, 'error_diag': str(ex)}
            self.__ret_code = -1

        logger.info(log_prefix + f"ret: {ret}")

        time_used = time.time() - time_start
        logger.info(log_prefix + f"time used to process this event: {time_used}")

        ret['wall_time'] = time_used

        if self.event_execution_time is None or self.event_execution_time < time_used:
            self.event_execution_time = time_used
            logger.info(log_prefix + f"max event execution time: {time_used}")
        return ret

    def send_terminate_events(self, outputs):
        """Forward each completed event result to the out-message handler.

        Args:
            outputs: List of result dicts from :meth:`run_event`.
        """
        for output in outputs:
            self.handle_out_message(output)

    def monitor(self, terminate=False) -> None:
        """Monitor whether a process is dead.

        Raises:
            RunPayloadFailure: when the payload process is dead or exited.
        """
        if self.__thread_pool:
            self.__thread_pool.scan()
            if not terminate:
                num_free_workers = self.__thread_pool.get_num_free_workers()
                if num_free_workers > 0:
                    events = self.try_get_events(num_free_workers)
                    if events:
                        logger.info(f"Got {len(events)} events: {events}")
                    for event in events:
                        # self.run_event(event)
                        self.__thread_pool.run_event(self.run_event, event)

            outputs = self.__thread_pool.get_outputs()
            if outputs:
                logger.info(f"Got {len(outputs)} outputs: {outputs}")
                self.send_terminate_events(outputs)

    def get_event_ranges(self, num_ranges=None, queue_factor=1):
        """Call get_event_ranges hook to get event ranges.

        Args:
            num_ranges: number of event ranges to get.
            queue_factor: queue factor for prefetching.

        Raises:
            SetupFailure: If get_event_ranges_hook is not set.
            MessageFailure: when failed to get event ranges.
        """
        if not num_ranges:
            num_ranges = self.corecount

        logger.debug(f'getting event ranges(num_ranges={num_ranges})')
        if not self.get_event_ranges_hook:
            raise SetupFailure("get_event_ranges_hook is not set")

        try:
            logger.debug(f'calling get_event_ranges hook({self.get_event_ranges_hook}) to get event ranges.')
            event_ranges = self.get_event_ranges_hook(num_ranges, queue_factor)
            logger.debug(f'got event ranges: {event_ranges}')
            return event_ranges
        except Exception as e:
            raise MessageFailure(f"Failed to get event ranges: {e}") from e

    def parse_out_message(self, message):
        """Parse output or error messages from payload.

        Args:
            message: The message string received from payload.

        Returns:
            dict: a dict {'id': <id>, 'status': <status>, 'output': <output if produced>, 'cpu': <cpu>, 'wall': <wall>, 'message': <full message>}.

        Raises:
            PilotException: when a PilotException is caught.
            UnknownException: when other unknown exception is caught.
        """
        logger.debug(f'parsing message: {message}')
        return message

    def handle_out_message(self, message) -> None:
        """Handle output or error messages from payload.

        Messages from payload will be parsed and the handle_out_message hook is called.

        Args:
            message: The message string received from payload.

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
            raise RunPayloadFailure(f"Failed to handle out message: {e}")

    def is_payload_running(self) -> bool:
        """Check whether the payload is still running.

        Returns:
            bool: True if the payload is running, otherwise False.
        """
        if (self.__stop.is_set() or self.is_no_more_events) and self.__thread_pool.get_num_running_workers() < 1:
            return False
        return True

    def poll(self):
        """Poll whether the process is still running.

        Returns:
            None if still running, 0 if finished successfully, or a non-zero int if failed.
        """
        # if self.is_payload_running():
        #     return None
        logger.debug(f"is_alive: {self.is_alive()}, ret_code: {self.__ret_code}")
        # if self.is_alive():
        #     return None
        return self.__ret_code

    def clean(self) -> None:
        """Clean left resources."""
        self.stop()
        if self.__ret_code is None:
            self.__ret_code = 0

    def run(self) -> None:
        """Main run loop: monitor message thread and payload process.

        Handles messages from payload and responds with new event ranges or process outputs.

        Raises:
            PilotException: when a PilotException is caught.
            UnknownException: when other unknown exception is caught.
        """
        self.__is_payload_started = True
        logger.info(f'start esprocess with thread ident: {self.ident}')
        logger.debug('initializing')
        self.init()
        logger.debug('initialization finished.')

        logger.info('starts to main loop')
        while self.is_payload_running():
            try:
                self.monitor()
                self.write_logs_from_queue()
                time.sleep(0.01)
            except PilotException as e:
                logger.error(f'PilotException caught in the main loop: {e.get_detail()}, {traceback.format_exc()}')
                # TODO: define output message exception. If caught 3 output message exception, terminate
                self.stop()
            except Exception as e:
                logger.error(f'Exception caught in the main loop: {e}, {traceback.format_exc()}')
                # TODO: catch and raise exceptions
                # if catching dead process exception, terminate.
                self.stop()
                break
        logger.info("main loop ends")
        self.monitor(terminate=True)
        self.write_logs_from_queue()
        self.clean()
        logger.debug('main loop finished')
