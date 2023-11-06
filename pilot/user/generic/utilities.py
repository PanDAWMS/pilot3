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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-23

import logging
logger = logging.getLogger(__name__)


def precleanup():
    """
    Pre-cleanup at the beginning of the job to remove any pre-existing files from previous jobs in the main work dir.

    :return:
    """

    pass


def get_cpu_arch():
    """
    Return the CPU architecture string.

    If not returned by this function, the pilot will resort to use the internal scripts/cpu_arch.py.

    :return: CPU arch (string).
    """

    return ""
