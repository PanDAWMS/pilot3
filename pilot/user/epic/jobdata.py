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

"""Functions related to job data."""

#import logging
#import re

#logger = logging.getLogger(__name__)


def jobparams_prefiltering(value: str) -> (dict, str):
    """
    Perform pre-filtering of raw job parameters to avoid problems with especially quotation marks.

    The function can extract some fields from the job parameters to be put back later after actual filtering.

    E.g. ' --athenaopts "HITtoRDO:--nprocs=$ATHENA_CORE_NUMBER" ' will otherwise become
    ' --athenaopts 'HITtoRDO:--nprocs=$ATHENA_CORE_NUMBER' ' which will prevent the environmental variable to be unfolded.

    :param value: job parameters (str)
    :return: dictionary of fields excluded from job parameters (dict), updated job parameters (str).
    """
    exclusions = {}

    # Add regex patterns here
    # ..
    return exclusions, value


def jobparams_postfiltering(value: str, exclusions: dict = None) -> str:
    """
    Perform post-filtering of raw job parameters.

    Any items in the optional exclusion list will be added (space separated) at the end of the job parameters.

    :param value: job parameters (str)
    :param exclusions: exclusions dictionary from pre-filtering function (dict)
    :return: updated job parameters (str).
    """
    if exclusions is None:  # avoid pylint warning
        exclusions = {}

    for item in exclusions:
        value = value.replace(item, exclusions[item])

    return value


def fail_at_getjob_none() -> bool:
    """
    Return a boolean value indicating whether to fail when getJob returns None.

    :return: True (bool).
    """
    return True
