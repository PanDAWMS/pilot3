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
# - Paul Nilsson, paul.nilsson@cern.ch, 2025


def allow_memory_usage_verifications() -> bool:
    """
    Return True if memory usage verifications should be performed.

    :return: False for generic jobs (bool).
    """
    return False


def memory_usage(job: object, resource_type: str) -> tuple[int, str]:
    """
    Perform memory usage verification.

    :param job: job object (object)
    :param resource_type: resource type (str)
    :return: exit code (int), diagnostics (str).
    """
    if job or resource_type:  # to bypass pylint score 0
        pass

    exit_code = 0
    diagnostics = ""

    return exit_code, diagnostics
