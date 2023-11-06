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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

import os
import re
import subprocess
from signal import SIGTERM, SIGKILL
from time import sleep

import logging
logger = logging.getLogger(__name__)


def kill_process_group(pgrp, nap=10):
    """
    Kill the process group.
    DO NOT MOVE TO PROCESSES.PY - will lead to circular import since execute() needs it as well.
    :param pgrp: process group id (int).
    :param nap: napping time between kill signals in seconds (int)
    :return: boolean (True if SIGTERM followed by SIGKILL signalling was successful)
    """

    status = False
    _sleep = True

    # kill the process gracefully
    logger.info(f"killing group process {pgrp}")
    try:
        os.killpg(pgrp, SIGTERM)
    except Exception as error:
        logger.warning(f"exception thrown when killing child group process under SIGTERM: {error}")
        _sleep = False
    else:
        logger.info(f"SIGTERM sent to process group {pgrp}")

    if _sleep:
        logger.info(f"sleeping {nap} s to allow processes to exit")
        sleep(nap)

    try:
        os.killpg(pgrp, SIGKILL)
    except Exception as error:
        logger.warning(f"exception thrown when killing child group process with SIGKILL: {error}")
    else:
        logger.info(f"SIGKILL sent to process group {pgrp}")
        status = True

    return status


def kill_defunct_process(pid):
    """
    Kills a process if it is in a defunct state.

    :param pid: main process PID (int).
    """

    try:
        cmd = f"ps -p {pid} -o stat"
        logger.info(f'executing command with os.popen(): {cmd}')
        process = os.popen(cmd)
        output = process.read().strip()
        process.close()
    except Exception as exc:
        logger.warning(f'caught exception: {exc}')
    else:
        for line in output.split('\n'):
            if line.upper().startswith('Z') or line.upper().endswith('Z'):
                # The process is in a defunct state.
                logger.info(f'killing defunct process {pid}')
                os.kill(pid, SIGKILL)


def get_all_child_pids(parent_pid):

    def extract_pids_from_string(s):
        pid_pattern = re.compile(r'\((\d+)\)')
        pids = pid_pattern.findall(s)
        return [int(pid) for pid in pids]
    try:
        output = subprocess.check_output(["pstree", "-p", str(parent_pid)], universal_newlines=True)
        logger.debug(output)
        pids = extract_pids_from_string(output)
        return pids
    except subprocess.CalledProcessError:
        return []


def is_defunct(pid):
    def get_process_info(pid):
        process = subprocess.run(['ps', '-o', 'stat=', '-p', str(pid)], capture_output=True, text=True)
        return process.returncode, process.stdout, process.stderr

    try:
        #cmd = f'ps -p {pid} -o pid,vsz=MEMORY -o user,group=GROUP -o comm,args=ARGS'
        #result = subprocess.run(cmd.split(' '), capture_output=True, text=True)
        #logger.debug(f'{cmd}: {result}')

        returncode, stdout, stderr = get_process_info(pid)
        logger.debug(f'{pid}: return code={returncode}, stdout={stdout}, stderr={stderr}')

        return 'Z' in stdout.strip()
    except subprocess.CalledProcessError:
        return False


def find_defunct_subprocesses(parent_pid):
    """
    Finds all defunct subprocesses to the given process.

    :param pid: main process PID (int).
    :return: A list of all defunct subprocesses to the given process.
    """

    child_pids = get_all_child_pids(parent_pid)
    logger.info(f'child pids={child_pids}')
    defunct_subprocesses = []

    for child_pid in child_pids:
        if is_defunct(child_pid):
            defunct_subprocesses.append(child_pid)

    return defunct_subprocesses
