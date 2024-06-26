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

"""Event Service manager to set up and run ESProcess."""

import logging
from typing import Any

from pilot.eventservice.esprocess.esprocess import ESProcess
from pilot.eventservice.esprocess.eshook import ESHook

logger = logging.getLogger(__name__)


class ESManager:
    """Event Service manager class."""

    def __init__(self, hook: Any):
        """
        Set up ES hooks.

        :param hook: an instance of ESHook (Any)
        :raises Exception: if hook is not an instance of ESHook.
        """
        logger.info('initializing hooks')
        if not isinstance(hook, ESHook):
            raise TypeError(f"hook({hook}) is not instance of {ESHook}")

        self.__hook = hook
        logger.info('initialized hooks')

    def run(self):
        """Initialize and run ESProcess."""
        logger.debug('gettting payload')
        payload = self.__hook.get_payload()
        logger.debug(f'got payload: {payload}')

        logger.info('init ESProcess')
        process = ESProcess(payload)
        process.set_get_event_ranges_hook(self.__hook.get_event_ranges)
        process.set_handle_out_message_hook(self.__hook.handle_out_message)

        logger.info('ESProcess starts to run')
        process.start()
        process.join()
        logger.info('ESProcess finishes')
