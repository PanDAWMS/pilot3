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
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-24

"""Functions related to containerisation for Rubin."""

# import logging
# logger = logging.getLogger(__name__)


def do_use_container(**kwargs: dict) -> bool:
    """
    Decide whether to use a container or not.

    :param kwargs: dictionary of key-word arguments (dict)
    :return: True is function has decided that a container should be used, False otherwise (bool).
    """
    if kwargs:  # to bypass pylint score 0
        pass

    return True


def wrapper(executable: str, **kwargs: dict) -> str:
    """
    Wrap given function for any container specific usage.

    This function will be called by pilot.util.container.execute() and prepends the executable with a container command.

    :param executable: command to be executed (str)
    :param kwargs: dictionary of key-word arguments (dict)
    :return: executable wrapped with container command (str).
    """
    if kwargs:  # to bypass pylint score 0
        pass

    return executable


def create_stagein_container_command(workdir: str, cmd: str) -> str:
    """
    Create the stage-in container command.

    The function takes the isolated stage-in command, adds bits and pieces needed for the containerisation and stores
    it in a stagein.sh script file. It then generates the actual command that will execute the stage-in script in a
    container.

    :param workdir: working directory where script will be stored (string).
    :param cmd: isolated stage-in command (string).
    :return: container command to be executed (string).
    """
    if workdir:  # to bypass pylint score 0
        pass

    return cmd
