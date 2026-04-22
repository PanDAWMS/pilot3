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
# - Paul Nilsson, paul.nilsson@cern.ch, 2023-24

"""Hooks for EventService."""


class ESHook:
    """Event Service Hook class."""

    def get_payload(self) -> dict:
        """Get payload to execute.

        Should return: {'payload': <cmd string>, 'output_file': <filenamet>, 'error_file': <filename>}.

        Raises:
            NotImplementedError: if anything goes wrong.
        """
        raise NotImplementedError("Not Implemented")

    def get_event_ranges(self, num_ranges: int = 1) -> dict:
        """Get event ranges.

        Should return: dictionary of event ranges.

        Args:
            num_ranges: Number of event ranges to download, default is 1.

        Raises:
            NotImplementedError: if anything goes wrong.
        """
        raise NotImplementedError("Not Implemented")

    def handle_out_message(self, message: dict):
        """
        Handle ES output or error message.

        Example:
            For 'finished' event ranges::

                {'id': <id>, 'status': 'finished', 'output': <o>, 'cpu': <cpu>, 'wall': <wall>, 'message': <full message>}

            For 'failed' event ranges::

                {'id': <id>, 'status': 'finished', 'message': <full message>}

        Args:
            message: dictionary of a parsed message.

        Raises:
            NotImplementedError: always, as this is an abstract method.
        """
        raise NotImplementedError("Not Implemented")
