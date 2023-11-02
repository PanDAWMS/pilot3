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
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-2023

"""A factory to manage plugins."""

from typing import Any
import logging
logger = logging.getLogger(__name__)


class PluginFactory:
    """Plugin factory class."""

    def __init__(self, *args: Any, **kwargs: Any):
        """Set initial values."""
        self.classMap = {}

    def get_plugin(self, confs: dict) -> dict:
        """
        Load plugin class.

        :param confs: a dict of configurations.
        :return: plugin class.
        """
        class_name = confs['class']
        if class_name is None:
            logger.error(f"class is not defined in confs: {confs}")
            return None

        if class_name not in self.classMap:
            logger.info(f"trying to import {class_name}")
            components = class_name.split('.')
            mod = __import__('.'.join(components[:-1]))
            for comp in components[1:]:
                mod = getattr(mod, comp)
            self.classMap[class_name] = mod

        args = {}
        excluded_keys = {'class'}  # Use a set to store keys to exclude
        for key in confs:
            if key in excluded_keys:
                continue
            args[key] = confs[key]

        cls = self.classMap[class_name]
        logger.info(f"importing {cls} with args: {args}")
        impl = cls(**args)

        return impl
