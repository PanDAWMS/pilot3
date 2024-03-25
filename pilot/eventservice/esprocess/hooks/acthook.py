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
# - Wen Guan, wen.guan@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

"""Hooks for ARC-ControlTower EventService."""

from pilot.eventservice.eshook import ESHook


class ACTESHook(ESHook):
    """ACT EventService hook class."""

    def get_payload(self) -> dict:
        """
        Get payload to execute.

        Should return: {'payload': <cmd string>, 'output_file': <filename>, 'error_file': <filename or without it>} (dict)
        :raises Exception: if anything goes wrong.
        """
        raise NotImplementedError("Not Implemented")

    def get_event_ranges(self, num_ranges: int = 1) -> dict:
        """
        Get event ranges.

        Should return: dictionary of event ranges (dict).

        :param num_ranges: number of event ranges to get (int)
        :raises Exception: if anything goes wrong.
        """
        raise NotImplementedError("Not Implemented")

    def handle_out_message(self, message: dict):
        """
        Handle ES output or error messages.

        Example
            For 'finished' event ranges, it's {'id': <id>, 'status': 'finished', 'output': <output>, 'cpu': <cpu>,
                    'wall': <wall>, 'message': <full message>}.
            For 'failed' event ranges, it's {'id': <id>, 'status': 'finished', 'message': <full message>}.

        :param message: dictionary of a parsed message (dict).
        :raises Exception: if anything goes wrong.
        """
        raise NotImplementedError("Not Implemented")
