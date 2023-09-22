#!/usr/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Authors:
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-2023

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
