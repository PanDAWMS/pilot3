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

"""
Main process to handle event service.
It makes use of two hooks get_event_ranges_hook and handle_out_message_hook to communicate with other processes when
it's running. The process will handle the logic of event service independently.
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
    """
    ThreadPoolExecutor with additional methods to handle event service.
    """
    def __init__(self, max_workers: int = None, thread_name_prefix: str = '', initializer: Any = None, initargs: tuple = ()):
        self.futures = {}
        self.outputs = {}
        self._lock = threading.RLock()
        self.max_workers = max_workers
        super().__init__(max_workers=max_workers,
                         thread_name_prefix=thread_name_prefix,
                         initializer=initializer,
                         initargs=initargs)

    def submit(self, fn, *args, **kwargs):
        future = super().submit(fn, *args, **kwargs)
        return future

    def run_event(self, fn, event):
        future = super().submit(fn, event)
        with self._lock:
            self.futures[event['eventRangeID']] = {'event': event, 'future': future}

    def scan(self):
        with self._lock:
            for event_range_id in list(self.futures.keys()):
                event_future = self.futures[event_range_id]
                future = event_future['future']
                if future.done():
                    result = future.result()
                    self.outputs[event_range_id] = {'event': self.futures[event_range_id]['event'], 'result': result}
                    del self.futures[event_range_id]

    def get_outputs(self):
        outputs = []
        with self._lock:
            for event_range_id in self.outputs:
                outputs.append(self.outputs[event_range_id]['result'])
            self.outputs = {}
        return outputs

    def get_max_workers(self):
        return self.max_workers

    def get_num_running_workers(self):
        return len(list(self.futures.keys()))

    def get_num_free_workers(self):
        return self.max_workers - self.get_num_running_workers()


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
        if self.__thread_pool:
            del self.__thread_pool

    def is_payload_started(self):
        return self.__is_payload_started

    def stop(self, delay=1800):
        if not self.__stop.is_set():
            self.__stop.set()
            self.__stop_set_time = time.time()
            self.__stop_delay = delay
        self.close_logs()
        self.__thread_pool.shutdown(wait=False)

    def get_job_id(self):
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].jobid:
            return self.__payload['job'].jobid
        return ''

    def get_job(self):
        if 'job' in self.__payload and self.__payload['job']:
            return self.__payload['job']
        return None

    def get_transformation(self):
        if 'job' in self.__payload and self.__payload['job'] and self.__payload['job'].transformation:
            return self.__payload['job'].transformation
        return None

    def get_corecount(self):
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
        """
        Return the requested file.

        :param file_label:
        :param workdir:
        :return:
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
        # return 'cd %s; %s' % (workdir, executable)
        return executable

    def init_logs(self):
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
        try:
            # cmd = "pwd; ls -ltr"
            # execute(cmd, stdout=self.stdout_file, stderr=self.stderr_file, timeout=120)
            self.stdout_file.close()
            self.stderr_file.close()
            for fd in self.realtime_log_files:
                self.realtime_log_files[fd].close()
        except Exception as ex:
            logger.error(f"Failed to close logs: {ex}")

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
            self.init_logs()
            self.__thread_pool = ESRunnerThreadPool(max_workers=self.get_corecount(),
                                                    thread_name_prefix='ESProcessRunner')
        except Exception as e:
            # TODO: raise exceptions
            self.__ret_code = -1
            self.stop()
            raise e

    def try_get_events(self, num_free_workers):
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
        work_dir = self.get_workdir()
        event_dir = os.path.join(work_dir, event_range_id)
        if not os.path.exists(event_dir):
            os.makedirs(event_dir)
        return event_dir

    def get_env_item(self, env, str_item):
        items = str_item.replace(" ", ";").split(";")
        for item in items:
            if env in item:
                return item.replace(env, "")
        return None

    def get_event_range_map_info(self):
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
        executable = self.get_executable(event_dir)
        event_range_file_map = self.get_event_range_file_map(event)
        executable = self.replace_executable(executable, event_range_file_map)
        # executable = "cd  " + event_dir + "; " + executable

        transformation = self.get_transformation()
        base_transformation = os.path.basename(transformation)

        executable = "cp -f " + base_transformation + " " + event_dir + "; cd  " + event_dir + "; " + executable

        stdout_filename = os.path.join(event_dir, "payload.stdout")
        stderr_filename = os.path.join(event_dir, "payload.stderr")

        stdout_file = open(stdout_filename, 'a', encoding='utf-8')
        stderr_file = open(stderr_filename, 'a', encoding='utf-8')
        realtime_log_files = os.environ.get('REALTIME_LOGFILES', None)
        realtime_log_files = re.split('[:,]', realtime_log_files)
        realtime_log_files = [os.path.join(event_dir, f) for f in realtime_log_files]
        return executable, stdout_file, stderr_file, stdout_filename, stderr_filename, realtime_log_files

    def get_worker_id(self):
        worker_id = None
        with self._lock:
            self._worker_id += 1
            worker_id = self._worker_id
        return worker_id

    def open_log_file(self, filename, perm='r'):
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
        """
        Wait for payload process to finish.

        :param proc: subprocess object (Any)
        :return: exit code (int).
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
                exit_code = _exit_code
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
        for output in outputs:
            self.handle_out_message(output)

    def monitor(self, terminate=False):
        """
        Monitor whether a process is dead.

        raises: RunPayloadFailure: when the payload process is dead or exited.
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
        """
        Calling get_event_ranges hook to get event ranges.

        :param num_ranges: number of event ranges to get.

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
            event_ranges = self.get_event_ranges_hook(num_ranges, queue_factor)
            logger.debug(f'got event ranges: {event_ranges}')
            return event_ranges
        except Exception as e:
            raise MessageFailure(f"Failed to get event ranges: {e}") from e

    def parse_out_message(self, message):
        """
        Parse output or error messages from payload.

        :param message: The message string received from payload.

        :returns: a dict {'id': <id>, 'status': <status>, 'output': <output if produced>, 'cpu': <cpu>, 'wall': <wall>, 'message': <full message>}
        :raises: PilotExecption: when a PilotException is caught.
                 UnknownException: when other unknown exception is caught.
        """

        logger.debug(f'parsing message: {message}')
        return message

    def handle_out_message(self, message):
        """
        Handle output or error messages from payload.
        Messages from payload will be parsed and the handle_out_message hook is called.

        :param message: The message string received from payload.

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

    def is_payload_running(self):
        """
        Check whether the payload is still running

        :return: True if the payload is running, otherwise False
        """
        if (self.__stop.is_set() or self.is_no_more_events) and self.__thread_pool.get_num_running_workers() < 1:
            return False
        return True

    def poll(self):
        """
        poll whether the process is still running.

        :returns: None: still running.
                  0: finished successfully.
                  others: failed.
        """
        # if self.is_payload_running():
        #     return None
        logger.debug(f"is_alive: {self.is_alive()}, ret_code: {self.__ret_code}")
        # if self.is_alive():
        #     return None
        return self.__ret_code

    def clean(self):
        """
        Clean left resources
        """
        self.stop()
        if self.__ret_code is None:
            self.__ret_code = 0

    def run(self):
        """
        Main run loops: monitor message thread and payload process.
                        handle messages from payload and response messages with injecting new event ranges or process outputs.

        :raises: PilotExecption: when a PilotException is caught.
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
