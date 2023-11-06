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
# - Mario Lassnig, mario.lassnig@cern.ch, 2016-2017
# - Daniel Drizhuk, d.drizhuk@gmail.com, 2017
# - Tobias Wegner, tobias.wegner@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2023
# - Wen Guan, wen.guan@cern.ch, 2018

"""Executor module for generic payloads."""

import logging
import os
import signal
import time
from subprocess import PIPE
from typing import Any, TextIO

from pilot.common.errorcodes import ErrorCodes
from pilot.control.job import send_state
from pilot.util.auxiliary import set_pilot_state  #, show_memory_usage
from pilot.util.config import config
from pilot.util.container import execute
from pilot.util.constants import (
    UTILITY_BEFORE_PAYLOAD,
    UTILITY_WITH_PAYLOAD,
    UTILITY_AFTER_PAYLOAD_STARTED,
    UTILITY_AFTER_PAYLOAD_FINISHED,
    PILOT_PRE_SETUP,
    PILOT_POST_SETUP,
    PILOT_PRE_PAYLOAD,
    PILOT_POST_PAYLOAD,
    UTILITY_AFTER_PAYLOAD_STARTED2,
    UTILITY_AFTER_PAYLOAD_FINISHED2
)
from pilot.util.filehandling import (
    write_file,
    read_file
)
from pilot.util.processes import kill_processes
from pilot.util.timing import (
    add_to_pilot_timing,
    get_time_measurement
)
from pilot.common.exception import PilotException

logger = logging.getLogger(__name__)
errors = ErrorCodes()


class Executor():
    """Executor class for generic payloads."""

    def __init__(self, args: Any, job: Any, out: TextIO, err: TextIO, traces: Any):
        """
        Set initial values.

        :param args: args object (Any)
        :param job: job object (Any)
        :param out: stdout file object (TextIO)
        :param err: stderr file object (TextIO)
        :param traces: traces object (Any).
        """
        self.__args = args
        self.__job = job
        self.__out = out  # payload stdout file object
        self.__err = err  # payload stderr file object
        self.__traces = traces
        self.__preprocess_stdout_name = ''
        self.__preprocess_stderr_name = ''
        self.__coprocess_stdout_name = 'coprocess_stdout.txt'
        self.__coprocess_stderr_name = 'coprocess_stderr.txt'
        self.__postprocess_stdout_name = ''
        self.__postprocess_stderr_name = ''

    def get_job(self):
        """
        Get the job object.

        :return: job object.
        """
        return self.__job

    def pre_setup(self, job: Any):
        """
        Run pre setup functions.

        :param job: job object (Any).
        """
        # write time stamps to pilot timing file
        update_time = time.time()
        logger.debug(f'setting pre-setup time to {update_time} s')
        logger.debug(f'gmtime is {time.gmtime(update_time)}')
        add_to_pilot_timing(job.jobid, PILOT_PRE_SETUP, update_time, self.__args)

    def post_setup(self, job: Any, update_time: bool = None):
        """
        Run post run functions.

        :param job: job object
        :param update_time: should time stamps be written to timing file? (bool)
        """
        # write time stamps to pilot timing file
        if not update_time:
            update_time = time.time()
        logger.debug(f'setting post-setup time to {update_time} s')
        logger.debug(f'gmtime is {time.gmtime(update_time)}')
        add_to_pilot_timing(job.jobid, PILOT_POST_SETUP, update_time, self.__args)

    def improve_post_setup(self):
        """
        Improve the post setup time if possible.

        More precise timing info can possibly be extracted from payload stdout.
        Read back the currently stored time for the post-setup, then add the elapsed time that passed between that time
        and the time stamp from the payload stdout (if found).
        """
        path = os.path.join(self.__job.workdir, config.Payload.payloadstdout)
        if not os.path.exists(path):
            return

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.setup', globals(), locals(), [pilot_user], 0)
        try:
            end_setup_time = user.get_end_setup_time(path)  # since epoch
        except Exception as exc:
            logger.debug(f'caught exception: {exc} (will not update setup time)')
            end_setup_time = None
        if end_setup_time:
            # get the currently stored post-setup time
            time_measurement_dictionary = self.__args.timing.get(self.__job.jobid, None)
            current_post_setup = get_time_measurement(PILOT_POST_SETUP, time_measurement_dictionary, self.__args.timing)
            if current_post_setup:
                logger.info(f'existing post-setup time: {current_post_setup} s (since epoch) (current time: {time.time()})')
                logger.debug(f'extracted end time from payload stdout: {end_setup_time} s')
                diff = end_setup_time - current_post_setup
                logger.info(f'payload setup finished {diff} s later than previously recorded')
                self.post_setup(self.__job, update_time=end_setup_time)

    def utility_before_payload(self, job: Any) -> str:
        """
        Prepare commands/utilities to run before payload.

        These commands will be executed later (as eg the payload command setup is unknown at this point, which is
        needed for the preprocessing. Preprocessing is prepared here).

        REFACTOR

        :param job: job object
        :return: utility command (str).
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        # should we run any additional commands? (e.g. special monitoring commands)
        cmd_dictionary = user.get_utility_commands(order=UTILITY_BEFORE_PAYLOAD, job=job)
        if cmd_dictionary:
            cmd = f"{cmd_dictionary.get('command')} {cmd_dictionary.get('args')}"
            _label = cmd_dictionary.get('label', 'utility')
            logger.info(f'utility command (\'{_label}\') to be executed before the payload: {cmd}')

        return cmd

    def utility_with_payload(self, job: Any) -> str:
        """
        Run functions alongside payload.

        REFACTOR

        :param job: job object.
        :return: utility command (str).
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        # should any additional commands be prepended to the payload execution string?
        cmd_dictionary = user.get_utility_commands(order=UTILITY_WITH_PAYLOAD, job=job)
        if cmd_dictionary:
            cmd = f"{cmd_dictionary.get('command')} {cmd_dictionary.get('args')}"
            _label = cmd_dictionary.get('label', 'utility')
            logger.info(f'utility command (\'{_label}\') to be executed with the payload: {cmd}')

        return cmd

    def get_utility_command(self, order: str = "") -> str:
        """
        Return the command for the requested utility command (will be downloaded if necessary).

        Note: the utility itself is defined in the user common code and is defined according to the order,
        e.g. UTILITY_AFTER_PAYLOAD_STARTED means a co-process (see ATLAS user code).

        :param order: order constant (str)
        :return: command to be executed (str).
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        # should any additional commands be executed after the payload?
        cmd_dictionary = user.get_utility_commands(order=order, job=self.__job)
        if cmd_dictionary:
            cmd = f"{cmd_dictionary.get('command')} {cmd_dictionary.get('args')}"
            _label = cmd_dictionary.get('label', 'utility')
            logger.info(f'utility command (\'{_label}\') to be executed after the payload: {cmd}')

        return cmd

    def utility_after_payload_started(self, job: Any):
        """
        Run utility functions after payload started.

        :param job: job object (Any).
        """
        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        # should any additional commands be executed after the payload?
        cmd_dictionary = user.get_utility_commands(order=UTILITY_AFTER_PAYLOAD_STARTED, job=job)
        if cmd_dictionary:
            cmd = f"{cmd_dictionary.get('command')} {cmd_dictionary.get('args')}"
            logger.info(f'utility command to be executed after the payload: {cmd}')

            # how should this command be executed?
            utilitycommand = user.get_utility_command_setup(cmd_dictionary.get('command'), job)
            if not utilitycommand:
                logger.warning('empty utility command - nothing to run')
                return
            try:
                proc1 = execute(utilitycommand, workdir=job.workdir, returnproc=True, usecontainer=False,
                                stdout=PIPE, stderr=PIPE, cwd=job.workdir, job=job)
            except Exception as error:
                logger.error(f'could not execute: {error}')
            else:
                # store process handle in job object, and keep track on how many times the command has been launched
                # also store the full command in case it needs to be restarted later (by the job_monitor() thread)
                logger.debug(f'storing process for {utilitycommand}')
                job.utilities[cmd_dictionary.get('command')] = [proc1, 1, utilitycommand]

                # wait for command to start
                #time.sleep(1)
                #cmd = utilitycommand.split(';')[-1]
                #prmon = f'prmon --pid {job.pid}'
                #pid = None
                #if prmon in cmd:
                #    import subprocess
                #    import re
                #    ps = subprocess.run(['ps', 'aux', str(os.getpid())], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                #                       encoding='utf-8')
                #    pattern = r'\b\d+\b'
                #    for line in ps.stdout.split('\n'):
                #        if prmon in line and f';{prmon}' not in line:  # ignore the line that includes the setup
                #            matches = re.findall(pattern, line)
                #            if matches:
                #                pid = matches[0]
                #                break
                #if pid:
                #    logger.info(f'{prmon} command has pid={pid} (appending to cmd dictionary)')
                #    job.utilities[cmd_dictionary.get('command')].append(pid)
                #else:
                #    logger.info(f'could not extract any pid from ps for cmd={cmd}')

    def utility_after_payload_started_new(self, job: Any) -> str:
        """
        Run utility functions after payload started.

        REFACTOR

        :param job: job object
        :return: utility command (str).
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        # should any additional commands be executed after the payload?
        cmd_dictionary = user.get_utility_commands(order=UTILITY_AFTER_PAYLOAD_STARTED, job=job)
        if cmd_dictionary:
            cmd = f"{cmd_dictionary.get('command')} {cmd_dictionary.get('args')}"
            logger.info(f'utility command to be executed after the payload: {cmd}')

        return cmd

#            # how should this command be executed?
#            utilitycommand = user.get_utility_command_setup(cmd_dictionary.get('command'), job)
#            if not utilitycommand:
#                logger.warning('empty utility command - nothing to run')
#                return
#            try:
#                proc = execute(utilitycommand, workdir=job.workdir, returnproc=True, usecontainer=False,
#                               stdout=PIPE, stderr=PIPE, cwd=job.workdir, job=job)
#            except Exception as error:
#                logger.error('could not execute: %s', error)
#            else:
#                # store process handle in job object, and keep track on how many times the command has been launched
#                # also store the full command in case it needs to be restarted later (by the job_monitor() thread)
#                job.utilities[cmd_dictionary.get('command')] = [proc, 1, utilitycommand]

    def utility_after_payload_finished(self, job: Any, order: str) -> (str, str, bool):
        """
        Prepare commands/utilities to run after payload has finished.

        This command will be executed later.

        The order constant can be UTILITY_AFTER_PAYLOAD_FINISHED, UTILITY_AFTER_PAYLOAD_FINISHED2

        :param job: job object
        :param order: string constant used for utility selection (str)
        :return: command (str), label (str), ignore failure (bool).
        """
        cmd = ""

        # get the payload command from the user specific code
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        # should any additional commands be prepended to the payload execution string?
        cmd_dictionary = user.get_utility_commands(order=order, job=job)
        label = cmd_dictionary.get('label') if cmd_dictionary else 'unknown'
        if cmd_dictionary:
            cmd = f"{cmd_dictionary.get('command')} {cmd_dictionary.get('args')}"
            logger.info(f'utility command (\'{label}\') to be executed after the payload has finished: {cmd}')

        ignore_failure = cmd_dictionary.get('ignore_failure') if cmd_dictionary else False
        return cmd, label, ignore_failure

    def execute_utility_command(self, cmd: str, job: Any, label: str) -> int:
        """
        Execute a utility command (e.g. pre/postprocess commands; label=preprocess etc).

        :param cmd: full command to be executed (str)
        :param job: job object
        :param label: command label (str)
        :return: exit code (int).
        """
        exit_code, stdout, stderr = execute(cmd, workdir=job.workdir, cwd=job.workdir, usecontainer=False)
        if exit_code:
            ignored_exit_codes = [160, 161, 162]
            logger.warning(
                f'command returned non-zero exit code: {cmd} (exit code = {exit_code}) - see utility logs for details'
            )
            if label == 'preprocess':
                err = errors.PREPROCESSFAILURE
            elif label == 'postprocess':
                err = errors.POSTPROCESSFAILURE
            else:
                err = 0  # ie ignore
                exit_code = 0
            if err and exit_code not in ignored_exit_codes:  # ignore no-more-data-points exit codes
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(err)
            if exit_code in ignored_exit_codes:
                job.transexitcode = exit_code

        # write output to log files
        self.write_utility_output(job.workdir, label, stdout, stderr)

        return exit_code

    def write_utility_output(self, workdir: str, step: str, stdout: str, stderr: str):
        """
        Write the utility command output to stdout, stderr files to the job.workdir for the current step.

        -> <step>_stdout.txt, <step>_stderr.txt
        Example of step: preprocess, postprocess.

        :param workdir: job workdir (str)
        :param step: utility step (str)
        :param stdout: command stdout (str)
        :param stderr: command stderr (str)
        """
        # dump to file
        try:
            name_stdout = step + '_stdout.txt'
            name_stderr = step + '_stderr.txt'
            if step == 'preprocess':
                self.__preprocess_stdout_name = name_stdout
                self.__preprocess_stderr_name = name_stderr
            elif step == 'postprocess':
                self.__postprocess_stdout_name = name_stdout
                self.__postprocess_stderr_name = name_stderr
            name = os.path.join(workdir, step + '_stdout.txt')
            write_file(name, stdout, unique=True)
        except PilotException as error:
            logger.warning(f'failed to write utility stdout to file: {error}, {stdout}')
        else:
            logger.debug(f'wrote {name}')

        try:
            name = os.path.join(workdir, step + '_stderr.txt')
            write_file(name, stderr, unique=True)
        except PilotException as error:
            logger.warning(f'failed to write utility stderr to file: {error}, {stderr}')
        else:
            logger.debug(f'wrote {name}')

    def pre_payload(self, job: Any):
        """
        Run functions before payload.

        E.g. write time stamps to timing file.

        :param job: job object.
        """
        # write time stamps to pilot timing file
        update_time = time.time()
        logger.debug(f'setting pre-payload time to {update_time} s')
        logger.debug(f'gmtime is {time.gmtime(update_time)}')
        add_to_pilot_timing(job.jobid, PILOT_PRE_PAYLOAD, update_time, self.__args)

    def post_payload(self, job: Any):
        """
        Run functions after payload.

        E.g. write time stamps to timing file.

        :param job: job object.
        """
        # write time stamps to pilot timing file
        update_time = time.time()
        logger.debug(f'setting post-payload time to {update_time} s')
        logger.debug(f'gmtime is {time.gmtime(update_time)}')
        add_to_pilot_timing(job.jobid, PILOT_POST_PAYLOAD, update_time, self.__args)

    def run_command(self, cmd: str, label: str = "") -> Any:
        """
        Execute the given command and return the process info.

        :param cmd: command (str)
        :param label: label (str)
        :return: subprocess object (Any).
        """
        if label:
            logger.info(f'\n\n{label}:\n\n{cmd}\n')
        if label == 'coprocess':
            try:
                out = open(os.path.join(self.__job.workdir, self.__coprocess_stdout_name), 'wb')
                err = open(os.path.join(self.__job.workdir, self.__coprocess_stderr_name), 'wb')
            except IOError as error:
                logger.warning(f'failed to open coprocess stdout/err: {error}')
                out = None
                err = None
        else:
            out = None
            err = None
        try:
            proc = execute(cmd, workdir=self.__job.workdir, returnproc=True, stdout=out, stderr=err,
                           usecontainer=False, cwd=self.__job.workdir, job=self.__job)
        except Exception as error:
            logger.error(f'could not execute: {error}')
            return None
        if isinstance(proc, tuple) and not proc[0]:
            logger.error('failed to execute command')
            return None

        logger.info(f'started {label} -- pid={proc.pid} executable={cmd}')

        return proc

    def run_payload(self, job: Any, cmd: str, out: Any, err: Any) -> Any:
        """
        Set up and execute the main payload process.

        REFACTOR using run_command()

        :param job: job object (Any)
        :param cmd: command (str)
        :param out: (currently not used; deprecated)
        :param err: (currently not used; deprecated)
        :return: proc (subprocess returned by Popen()).
        """
        # main payload process steps

        # add time for PILOT_PRE_PAYLOAD
        self.pre_payload(job)

        logger.info(f"\n\npayload execution command:\n\n{cmd}\n")
        try:
            proc = execute(cmd, workdir=job.workdir, returnproc=True,
                           usecontainer=True, stdout=out, stderr=err, cwd=job.workdir, job=job)
        except Exception as error:
            logger.error(f'could not execute: {error}')
            return None
        if isinstance(proc, tuple) and not proc[0]:
            logger.error('failed to execute payload')
            return None

        logger.info(f'started -- pid={proc.pid} executable={cmd}')
        job.pid = proc.pid
        job.pgrp = os.getpgid(job.pid)
        set_pilot_state(job=job, state="running")

        #_cmd = self.utility_with_payload(job)

        self.utility_after_payload_started(job)

        return proc

    def extract_setup(self, cmd: str) -> str:
        """
        Extract the setup from the payload command (cmd).

        E.g. extract the full setup from the payload command will be prepended to the pre/postprocess command.

        :param cmd: payload command (str)
        :return: updated secondary command (str).
        """
        def cut_str_from(_cmd: str, _str: str) -> str:
            """
            Cut the string from the position of the given _cmd.

            :param _cmd: command (str)
            :param _str: substring (str)
            :return: cut command (str).
            """
            return _cmd[:_cmd.find(_str)]

        def cut_str_from_last_semicolon(_cmd: str) -> str:
            """
            Cut the string from the last semicolon.

            NOTE: this will not work if jobParams also contain ;

            :param _cmd: command (str)
            :return: cut command (str).
            """
            # remove any trailing spaces and ;-signs
            _cmd = _cmd.strip()
            _cmd = _cmd[:-1] if _cmd.endswith(';') else _cmd
            last_bit = _cmd.split(';')[-1]
            return _cmd.replace(last_bit.strip(), '')

        if '/' in self.__job.transformation:  # e.g. http://pandaserver.cern.ch:25080/trf/user/runHPO-00-00-01
            trfname = self.__job.transformation[self.__job.transformation.rfind('/') + 1:]  # runHPO-00-00-01
            _trf = './' + trfname
        else:
            trfname = self.__job.transformation
            _trf = './' + self.__job.transformation

        if _trf in cmd:
            setup = cut_str_from(cmd, _trf)
        elif trfname in cmd:
            setup = cut_str_from(cmd, trfname)
        else:
            setup = cut_str_from_last_semicolon(cmd)

        return setup

    def wait_graceful(self, args: Any, proc: Any) -> int:
        """
        Wait for payload process to finish.

        :param args: pilot arguments object (Any)
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
                if args.graceful_stop.is_set():
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
            else:
                continue

        return exit_code

    def get_payload_command(self, job: Any) -> str:
        """
        Return the payload command string.

        :param job: job object (Any)
        :return: command (str).
        """
        cmd = ""
        # for testing looping job: cmd = user.get_payload_command(job) + ';sleep 240'
        try:
            pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
            user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
            cmd = user.get_payload_command(job)  #+ 'sleep 900'  # to test looping jobs
        except PilotException as error:
            self.post_setup(job)
            import traceback
            logger.error(traceback.format_exc())
            job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(error.get_error_code())
            self.__traces.pilot['error_code'] = job.piloterrorcodes[0]
            logger.fatal(
                f"could not define payload command (traces error set to: {self.__traces.pilot['error_code']})"
            )

        return cmd

    def run_preprocess(self, job: Any):
        """
        Run any preprocess payloads.

        :param job: job object (Any)
        :return: exit code (int)
        :raises: Exception.
        """
        exit_code = 0

        try:
            # note: this might update the jobparams
            cmd_before_payload = self.utility_before_payload(job)
        except Exception as error:
            logger.error(error)
            raise error

        if cmd_before_payload:
            cmd_before_payload = job.setup + cmd_before_payload
            logger.info(f"\n\npreprocess execution command:\n\n{cmd_before_payload}\n")
            exit_code = self.execute_utility_command(cmd_before_payload, job, 'preprocess')
            if exit_code == 160:
                logger.warning('no more HP points - time to abort processing loop')
            elif exit_code == 161:
                logger.warning('no more HP points but at least one point was processed - time to abort processing loop')
            elif exit_code == 162:
                logger.warning('loop count reached the limit - time to abort processing loop')
            elif exit_code:
                # set error code
                job.piloterrorcodes, job.piloterrordiags = errors.add_error_code(errors.PREPROCESSFAILURE)
                logger.fatal(f'cannot continue since preprocess failed: exit_code={exit_code}')
            else:
                # in case the preprocess produced a command, chmod it
                path = os.path.join(job.workdir, job.containeroptions.get('containerExec', 'does_not_exist'))
                if os.path.exists(path):
                    os.chmod(path, 0o755)

        return exit_code

    def should_verify_setup(self):
        """
        Determine if the setup command should be verified.

        :return: should verify setup (bool).
        """
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.setup', globals(), locals(), [pilot_user], 0)
        return user.should_verify_setup(self.__job)

    def run(self) -> int:  # noqa: C901
        """
        Run all payload processes (including pre- and post-processes, and utilities).

        In the case of HPO jobs, this function will loop over all processes until the preprocess returns a special
        exit code.

        :return: exit code (int).
        """
        diagnostics = ''

        # get the payload command from the user specific code
        self.pre_setup(self.__job)

        # get the user defined payload command
        cmd = self.get_payload_command(self.__job)
        if not cmd:
            logger.warning('aborting run() since payload command could not be defined')
            return errors.UNKNOWNPAYLOADFAILURE, 'undefined payload command'

        # extract the setup in case the preprocess command needs it
        self.__job.setup = self.extract_setup(cmd)
        # should the setup be verified? (user defined)
        verify_setup = self.should_verify_setup()
        if verify_setup:
            logger.debug(f'extracted setup to be verified:\n\n{self.__job.setup}')
            try:
                _cmd = self.__job.setup
                stdout_filename = os.path.join(self.__job.workdir, "setup.stdout")
                stderr_filename = os.path.join(self.__job.workdir, "setup.stderr")
                out = open(stdout_filename, 'wb')
                err = open(stderr_filename, 'wb')
                # remove any trailing spaces and ;-signs
                _cmd = _cmd.strip()
                trail = ';' if not _cmd.endswith(';') else ''
                _cmd += trail + 'echo \"Done.\"'
                exit_code, stdout, stderr = execute(_cmd, workdir=self.__job.workdir, returnproc=False, usecontainer=True,
                                                    stdout=out, stderr=err, cwd=self.__job.workdir, job=self.__job)
                if exit_code:
                    logger.warning(f'setup returned exit code={exit_code}')
                    diagnostics = stderr + stdout if stdout and stderr else ''
                    if not diagnostics:
                        stdout = read_file(stdout_filename)
                        stderr = read_file(stderr_filename)
                    diagnostics = stderr + stdout if stdout and stderr else 'General payload setup verification error (check setup logs)'
                    # check for special errors in thw output
                    exit_code = errors.resolve_transform_error(exit_code, diagnostics)
                    diagnostics = errors.format_diagnostics(exit_code, diagnostics)
                    return exit_code, diagnostics
                if out:
                    out.close()
                    logger.debug(f'closed {stdout_filename}')
                if err:
                    err.close()
                    logger.debug(f'closed {stderr_filename}')
            except Exception as error:
                diagnostics = f'could not execute: {error}'
                logger.error(diagnostics)
                return None, diagnostics

        self.post_setup(self.__job)

        # a loop is needed for HPO jobs
        # abort when nothing more to run, or when the preprocess returns a special exit code
        iteration = 0
        while True:

            logger.info(f'payload iteration loop #{iteration + 1}')
            os.environ['PILOT_EXEC_ITERATION_COUNT'] = f'{iteration}'
            #if self.__args.debug:
            #    show_memory_usage()

            # first run the preprocess (if necessary) - note: this might update jobparams -> must update cmd
            jobparams_pre = self.__job.jobparams
            exit_code = self.run_preprocess(self.__job)
            jobparams_post = self.__job.jobparams
            if exit_code:
                if 160 <= exit_code <= 162:
                    exit_code = 0
                    # wipe the output file list since there won't be any new files
                    # any output files from previous iterations, should have been transferred already
                    logger.debug('reset outdata since further output should not be expected after preprocess exit')
                    self.__job.outdata = []
                break
            if jobparams_pre != jobparams_post:
                logger.debug('jobparams were updated by utility_before_payload()')
                # must update cmd
                cmd = cmd.replace(jobparams_pre, jobparams_post)

            # now run the main payload, when it finishes, run the postprocess (if necessary)
            # note: no need to run any main payload in HPO Horovod jobs on Kubernetes
            if os.environ.get('HARVESTER_HOROVOD', '') == '':
                proc = self.run_payload(self.__job, cmd, self.__out, self.__err)
                if not proc:
                    # something went wrong, abort
                    exit_code = errors.UNKNOWNEXCEPTION
                    diagnostics = 'command execution failed, check log for clues'
                    break
            else:
                proc = None

            proc_co = None
            if proc is None:
                # run the post-process command even if there was no main payload
                if os.environ.get('HARVESTER_HOROVOD', '') != '':
                    logger.info('no need to execute any main payload')
                    exit_code = self.run_utility_after_payload_finished(exit_code, True, UTILITY_AFTER_PAYLOAD_FINISHED2)
                    self.post_payload(self.__job)
                else:
                    break
            else:
                # the process is now running, update the server
                # test 'tobekilled' from here to try payload kill
                send_state(self.__job, self.__args, self.__job.state)

                # note: when sending a state change to the server, the server might respond with 'tobekilled'
                if self.__job.state == 'failed':
                    logger.warning('job state is \'failed\' - abort payload and run()')
                    kill_processes(proc.pid)
                    break

                # allow for a secondary command to be started after the payload (e.g. a coprocess)
                utility_cmd = self.get_utility_command(order=UTILITY_AFTER_PAYLOAD_STARTED2)
                if utility_cmd:
                    logger.debug(f'starting utility command: {utility_cmd}')
                    label = 'coprocess' if 'coprocess' in utility_cmd else None
                    proc_co = self.run_command(utility_cmd, label=label)

                logger.info('will wait for graceful exit')
                exit_code = self.wait_graceful(self.__args, proc)
                # reset error if Raythena decided to kill payload (no error)
                if errors.KILLPAYLOAD in self.__job.piloterrorcodes:
                    logger.debug('ignoring KILLPAYLOAD error')
                    self.__job.piloterrorcodes, self.__job.piloterrordiags = errors.remove_error_code(errors.KILLPAYLOAD,
                                                                                                      pilot_error_codes=self.__job.piloterrorcodes,
                                                                                                      pilot_error_diags=self.__job.piloterrordiags)
                    exit_code = 0
                    state = 'finished'
                else:
                    state = 'finished' if exit_code == 0 else 'failed'
                set_pilot_state(job=self.__job, state=state)
                logger.info(f'\n\nfinished pid={proc.pid} exit_code={exit_code} state={self.__job.state}\n')

                # stop the utility command (e.g. a coprocess if necessary
                if proc_co:
                    logger.debug(f'stopping utility command: {utility_cmd}')
                    kill_processes(proc_co.pid)

                if exit_code is None:
                    logger.warning('detected unset exit_code from wait_graceful - reset to -1')
                    exit_code = -1

                for order in (UTILITY_AFTER_PAYLOAD_FINISHED, UTILITY_AFTER_PAYLOAD_FINISHED2):
                    exit_code = self.run_utility_after_payload_finished(exit_code, state, order)

                # keep track of post-payload timing
                self.post_payload(self.__job)

                # improve the post-setup timing if possible (more precise time info regarding setup can possibly be
                # extracted from the payload stdout)
                self.improve_post_setup()

                # stop any running utilities
                if self.__job.utilities != {}:
                    self.stop_utilities()

            if self.__job.is_hpo and state != 'failed':
                # in case there are more hyper-parameter points, move away the previous log files
                #self.rename_log_files(iteration)
                iteration += 1
            else:
                break

        return exit_code, diagnostics

    def run_utility_after_payload_finished(self, exit_code: int, state: str, order: str) -> int:
        """
        Run utility command after the main payload has finished.

        In horovod mode, select the corresponding post-process. Otherwise, select different post-process (e.g. Xcache).
        The order constant can be UTILITY_AFTER_PAYLOAD_FINISHED, UTILITY_AFTER_PAYLOAD_FINISHED2

        :param exit_code: transform exit code (int)
        :param state: payload state; finished/failed (str)
        :param order: constant used for utility selection (str)
        :return: exit code (int).
        """
        _exit_code = 0
        try:
            cmd_after_payload, label, ignore_failure = self.utility_after_payload_finished(self.__job, order)
        except Exception as error:
            logger.error(error)
            ignore_failure = False
        else:
            if cmd_after_payload and self.__job.postprocess and state != 'failed':
                cmd_after_payload = self.__job.setup + cmd_after_payload
                logger.info(f"\n\npostprocess execution command:\n\n{cmd_after_payload}\n")
                _exit_code = self.execute_utility_command(cmd_after_payload, self.__job, label)
            elif cmd_after_payload:
                logger.info(f"\n\npostprocess execution command:\n\n{cmd_after_payload}\n")
                _exit_code = self.execute_utility_command(cmd_after_payload, self.__job, label)

        # only set a new non-zero exit code if exit_code was not already set and ignore_failure is False
        # (e.g. any Xcache failure should be ignored to prevent job from failing since exit_code might get updated)
        if _exit_code and not exit_code and not ignore_failure:
            exit_code = _exit_code

        return exit_code

    def stop_utilities(self):
        """Stop any running utilities."""
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)

        for utcmd in list(self.__job.utilities.keys()):
            utproc = self.__job.utilities[utcmd][0]
            if utproc:
                # get the pid for prmon
                _list = self.__job.utilities.get('MemoryMonitor')
                if _list:
                    pid = int(_list[-1]) if len(_list) == 4 else utproc.pid
                    logger.info(f'using pid={pid} to kill prmon')
                else:
                    logger.warning(f'did not find the pid for the memory monitor in the utilities list: {self.__job.utilities}')
                    pid = utproc.pid
                status = self.kill_and_wait_for_process(pid, user, utcmd)
                logger.info(f'utility process {utproc.pid} cleanup finished with status={status}')
                user.post_utility_command_action(utcmd, self.__job)

    def kill_and_wait_for_process(self, pid: int, user: str, utcmd: str) -> int:
        """
        Kill utility process and wait for it to finish.

        :param pid: process id (int)
        :param user: pilot user/experiment (str)
        :param utcmd: utility command (str)
        :return: process exit status (int or None).
        """
        sig = user.get_utility_command_kill_signal(utcmd)
        logger.info(f"stopping utility process \'{utcmd}\' with signal {sig}")

        try:
            # Send SIGUSR1 signal to the process
            os.kill(pid, sig)

            # Check if the process exists
            if os.kill(pid, 0):
                # Wait for the process to finish
                _, status = os.waitpid(pid, 0)

                # Check the exit status of the process
                if os.WIFEXITED(status):
                    logger.debug('normal exit')
                    return os.WEXITSTATUS(status)
                else:
                    # Handle abnormal termination if needed
                    logger.warning('abnormal termination')
                    return None
            else:
                # Process doesn't exist - ignore
                logger.info(f'process {pid} no longer exists')
                return True

        except OSError as exc:
            # Handle errors, such as process not found
            logger.warning(f"Error sending signal to/waiting for process {pid}: {exc}")
            return None

#        try:
#            # Send SIGUSR1 signal to the process
#            os.kill(pid, sig)
#
#            try:
#                # Wait for the process to finish
#                _, status = os.waitpid(pid, 0)
#
#                # Check the exit status of the process
#                if os.WIFEXITED(status):
#                    return os.WEXITSTATUS(status)
#                else:
#                    # Handle abnormal termination if needed
#                    logger.warning('abnormal termination')
#                    return None
#            except OSError as exc:
#                # Handle errors related to waiting for the process
#                logger.warning(f"error waiting for process {pid}: {exc}")
#                return None
#
#        except OSError as exc:
#            # Handle errors, such as process not found
#            logger.warning(f"error sending signal to process {pid}: {exc}")
#            return None

#        try:
#            # Send SIGUSR1 signal to the process
#            os.kill(pid, sig)
#
#            # Wait for the process to finish
#            _, status = os.waitpid(pid, 0)
#
#            # Check the exit status of the process
#            if os.WIFEXITED(status):
#                return os.WEXITSTATUS(status)
#            else:
#                # Handle abnormal termination if needed
#                return None
#        except OSError as exc:
#            # Handle errors, such as process not found
#            logger.warning(f"exception caught: {exc}")
#            return None

    def rename_log_files(self, iteration: int):
        """
        Rename log files.

        :param iteration: iteration (int).
        """
        names = [self.__preprocess_stdout_name, self.__preprocess_stderr_name,
                 self.__postprocess_stdout_name, self.__postprocess_stderr_name]
        for name in names:
            if os.path.exists(name):
                os.rename(name, name + f'{iteration}')
            else:
                logger.warning(f'cannot rename {name} since it does not exist')
