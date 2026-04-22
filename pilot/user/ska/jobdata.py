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
from typing import Optional

#logger = logging.getLogger(__name__)


def jobparams_prefiltering(value: str) -> tuple[dict, str]:
    """Perform pre-filtering of raw job parameters to avoid problems with especially quotation marks.

    The function can extract some fields from the job parameters to be put back later after actual filtering.

    E.g. ' --athenaopts "HITtoRDO:--nprocs=$ATHENA_CORE_NUMBER" ' will otherwise become
    ' --athenaopts 'HITtoRDO:--nprocs=$ATHENA_CORE_NUMBER' ' which will prevent the environmental variable to be unfolded.

    Args:
        value: Job parameters string.

    Returns:
        tuple[dict, str]: Dictionary of fields excluded from job parameters, updated job parameters.
    """
    exclusions = {}

    # Add regex patterns here
    # ..
    return exclusions, value


def jobparams_postfiltering(value: str, exclusions: Optional[dict] = None) -> str:
    """Perform post-filtering of raw job parameters.

    Any items in the optional exclusion list will be added (space separated) at the end of the job parameters.

    Args:
        value: Job parameters string.
        exclusions: Exclusions dictionary from pre-filtering function.

    Returns:
        str: Updated job parameters.
    """
    if exclusions is None:  # avoid pylint warning
        exclusions = {}

    for item in exclusions:
        value = value.replace(item, exclusions[item])

    return value


def fail_at_getjob_none() -> bool:
    """Return a boolean value indicating whether to fail when getJob returns None.

    Returns:
        bool: True.
    """
    return True
