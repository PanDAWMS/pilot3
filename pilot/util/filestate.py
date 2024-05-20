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
# - Paul Nilsson, paul.nilsson@cern.ch, 2022-2024

"""Handling of file states."""

import logging

logger = logging.getLogger(__name__)


class FileState(object):
    """
    File state class.

    FS = FileState(file_status={'lfns': ['LFN1.DAT', 'LFN2.DAT']})
    FS.update(lfn='LFN1.DAT', state='TRANSFERRED')
    print(FS.get_file_states())
    """

    _file_states = {}
    _lfns = []
    _state_list = ['NOT_YET_TRANSFERRED', 'TRANSFER_IN_PROGRESS', 'TRANSFERRED', 'TRANSFER_FAILED']

    def __init__(self, file_states: dict = None):
        """
        Initialize variables.

        :param file_states: file states (dict).
        """
        if file_states is None:
            file_states = {}
        self._lfns = file_states.get('lfns', [])
        self.set_initial_list()

    def set_initial_list(self):
        """Set the initial file states list."""
        for lfn in self._lfns:
            self._file_states[lfn] = 'NOT_YET_TRANSFERRED'

    def get_file_states(self) -> dict:
        """
        Return the current file states dictionary.

        :return: file states (dict).
        """
        return self._file_states

    def update(self, lfn: str = "", state: str = ""):
        """
        Update the state for a given LFN.

        :param lfn: file name (str)
        :param state: file state (str).
        """
        if not lfn or not state:
            logger.warning('must set lfn/state')
            return

        if state not in self._state_list:
            logger.warning(f'unknown state: {state} (must be in: {self._state_list})')
            return

        self._file_states[lfn] = state
