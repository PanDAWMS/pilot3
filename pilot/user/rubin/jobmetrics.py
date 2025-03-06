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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-25

"""Functions for building job metrics."""

import logging

from pilot.info import JobData
from pilot.util.jobmetrics import get_job_metrics_entry
from pilot.util.math import mean

logger = logging.getLogger(__name__)


def get_job_metrics_string(job: JobData, extra: dict = None) -> str:
    """
    Get the job metrics string.

    :param job: job object (JobData)
    :param extra: any extra information to be added (dict)
    :return: job metrics (str).
    """
    if extra is None:
        extra = {}
    job_metrics = ""

    if job.cpufrequencies:
        try:
            _mean = int(mean(job.cpufrequencies))
        except ValueError:
            pass
        else:
            job_metrics += get_job_metrics_entry("cpuFrequency", _mean)

    # add any additional info
    if extra:
        for entry in extra:
            job_metrics += get_job_metrics_entry(entry, extra.get(entry))

    return job_metrics


def get_job_metrics(job: JobData, extra: dict = None) -> str:
    """
    Return a properly formatted job metrics string.

    The format of the job metrics string is defined by the server. It will be reported to the server during updateJob.

    Example of job metrics:
    Number of events read | Number of events written | vmPeak maximum | vmPeak average | RSS average | ..
    Format: nEvents=<int> nEventsW=<int> vmPeakMax=<int> vmPeakMean=<int> RSSMean=<int> hs06=<float> shutdownTime=<int>
            cpuFactor=<float> cpuLimit=<float> diskLimit=<float> jobStart=<int> memLimit=<int> runLimit=<float>

    :param job: job object (JobData)
    :param extra: any extra information to be added (dict)
    :return: job metrics (str).
    """
    if extra is None:
        extra = {}
    # get job metrics string
    job_metrics = get_job_metrics_string(job, extra=extra)

    return job_metrics
