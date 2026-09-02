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

from __future__ import annotations
import json
import logging
import os
import platform
import re
import sys

from pilot.api import analytics
from pilot.common.exception import FileHandlingFailure
from pilot.info import JobData
from pilot.util.config import config
from pilot.util.features import (
    MachineFeatures,
    JobFeatures
)
from pilot.util.filehandling import (
    find_last_line,
    read_file
)
from pilot.util.jobmetrics import get_job_metrics_entry
from pilot.util.math import (
    float_to_rounded_string,
    mean
)
from .cpu import get_core_count
from .common import (
    get_db_info,
    get_resimevents
)
from .utilities import get_memory_monitor_output_filename

logger = logging.getLogger(__name__)


def get_os_and_python_versions() -> tuple[str, str, str]:
    """Return the OS identifier, OS version, and Python version of the worker node.

    The OS information is read from ``/etc/os-release`` (the canonical source on all
    modern Linux distributions used on grid sites, including RHEL/Alma/CentOS/SL/Ubuntu).
    If that file is absent or cannot be parsed, ``platform.system()`` and
    ``platform.release()`` are used as a fallback.  The Python version is taken from
    ``sys.version_info`` and formatted as ``major.minor.micro``.

    Returns:
        tuple: (os_id, os_version, python_version) where each element is a plain string
        suitable for use as a job metrics value.  Any element that cannot be determined
        is returned as an empty string.
    """
    os_id = ''
    os_version = ''
    try:
        with open('/etc/os-release', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line.startswith('ID='):
                    os_id = line.split('=', 1)[1].strip('"\'')
                elif line.startswith('VERSION_ID='):
                    os_version = line.split('=', 1)[1].strip('"\'')
                if os_id and os_version:
                    break
    except OSError as exc:
        logger.debug(f'/etc/os-release could not be read: {exc} - falling back to platform module')
        os_id = platform.system().lower()
        os_version = platform.release()

    python_version = '.'.join(str(v) for v in sys.version_info[:3])

    return os_id.replace(' ', '_'), os_version.replace(' ', '_'), python_version.replace(' ', '_')


def get_job_metrics_string(job: JobData, extra: dict = None) -> str:  # noqa: C901
    """Get the job metrics string.

    Args:
        job: job object.
        extra: any extra information to be added.

    Returns:
        str: job metrics.
    """
    if extra is None:
        extra = {}
    job_metrics = ""

    # report core count (will also set corecount in job object)
    corecount = get_core_count(job)
    logger.debug(f'job definition core count: {corecount}')

    # report number of actual used cores and add it to the list of measured core counts
    if job.actualcorecount:
        job_metrics += get_job_metrics_entry("actualCoreCount", job.actualcorecount)

    # report number of events
    if job.nevents > 0:
        job_metrics += get_job_metrics_entry("nEvents", job.nevents)
    if job.neventsw > 0:
        job_metrics += get_job_metrics_entry("nEventsW", job.neventsw)

    # add metadata from job report
    if job.metadata:
        job.dbtime, job.dbdata = get_db_info(job.metadata)
        job.resimevents = get_resimevents(job.metadata)
    if job.dbtime and job.dbtime != "":
        job_metrics += get_job_metrics_entry("dbTime", job.dbtime)
    if job.dbdata and job.dbdata != "":
        job_metrics += get_job_metrics_entry("dbData", job.dbdata)
    if job.resimevents is not None:
        job_metrics += get_job_metrics_entry("resimevents", job.resimevents)

    # get the max disk space used by the payload (at the end of a job)
    if job.state in {"finished", "failed", "holding"}:
        max_space = job.get_max_workdir_size()
        if max_space > 0:
            job_metrics += get_job_metrics_entry("workDirSize", max_space)
        else:
            logger.info(f"will not add max space = {max_space} B to job metrics")

    # is there a detected rucio trace service error?
    trace_exit_code = get_trace_exit_code(job.workdir)
    if trace_exit_code != '0':
        job_metrics += get_job_metrics_entry("rucioTraceError", trace_exit_code)

    # add job and machine feature data if available
    job_metrics = add_features(job_metrics, corecount, add=['hs06'])

    # get analytics data
    job_metrics = add_analytics_data(job_metrics, job.workdir, job.state)

    # extract event number from file and add to job metrics if it exists
    job_metrics = add_event_number(job_metrics, job.workdir)

    # report HEPscore benchmark for gangarobot-hepscore jobs (only meaningful once the payload has finished)
    if job.state == "finished" and job.processingtype == "gangarobot-hepscore":
        hepscore = get_hepscore(os.path.join(job.workdir, config.Payload.payloadstdout))
        job_metrics += get_job_metrics_entry("hepscore", hepscore)

    # add DASK IPs if set
    if job.dask_scheduler_ip and job.jupyter_session_ip:
        job_metrics += get_job_metrics_entry("schedulerIP", job.dask_scheduler_ip)
        job_metrics += get_job_metrics_entry("sessionIP", job.jupyter_session_ip)

    if job.cpufrequencies:
        try:
            _mean = int(mean(job.cpufrequencies))
        except ValueError:
            pass
        else:
            # job_metrics += get_job_metrics_entry("cpuFrequency", _mean)
            logger.info(f"could have reported an average CPU frequency of {_mean} MHz ({len(job.cpufrequencies)} samples)")

    # report any files that were transferred to an alternative destination (alt stage-out)
    # format: altTransferred=lfn1,lfn2,...
    if job.outdata:
        alt_lfns = [entry.lfn for entry in job.outdata if entry.is_altstaged]
        if alt_lfns:
            job_metrics += get_job_metrics_entry("altTransferred", ",".join(alt_lfns))

    # add any additional info
    if extra:
        for entry in extra:
            job_metrics += get_job_metrics_entry(entry, extra.get(entry))

    # report OS and Python versions for Grafana/Kibana monitoring
    _os_id, _os_version, _python_version = get_os_and_python_versions()
    if _os_id:
        job_metrics += get_job_metrics_entry("osId", _os_id)
    if _os_version:
        job_metrics += get_job_metrics_entry("osVersion", _os_version)
    if _python_version:
        job_metrics += get_job_metrics_entry("pythonVersion", _python_version)

    return job_metrics


def get_hepscore(payload_stdout_path: str) -> str:
    """Extract the HEPscore benchmark score from a payload stdout file.

    Searches for lines containing the string ``CPU_Model`` (the marker written
    by the HEPscore benchmark runner) and decodes the first valid JSON object
    found on each such line using ``JSONDecoder.raw_decode()``.  This handles
    lines where a second JSON blob or a filesystem path is appended immediately
    after the closing brace with no newline separator.

    If multiple matching lines are found, duplicates are collapsed and the
    first unique value is returned.  A warning is logged when more than one
    distinct score is present.

    Args:
        payload_stdout_path: absolute path to the payload stdout file.

    Returns:
        str: the score as a plain string (e.g. ``'97.1161'``), or
        ``'UNKNOWN'`` when the file is absent, no matching line is found,
        the JSON cannot be parsed, the score key is missing or null, or the
        value is not numeric.
    """
    try:
        with open(payload_stdout_path, encoding='utf-8') as fh:
            lines = fh.readlines()
    except OSError as exc:
        logger.warning(f'get_hepscore: cannot open {payload_stdout_path}: {exc}')
        return 'UNKNOWN'

    decoder = json.JSONDecoder()
    scores = []
    for line in lines:
        if 'CPU_Model' not in line:
            continue
        # Find the first '{' on the line and attempt to decode from there.
        # raw_decode() stops at the end of the first complete JSON object,
        # ignoring any trailing content (a second JSON blob, a filesystem path,
        # etc.) that the benchmark runner may have written without a separator.
        start = line.find('{')
        if start == -1:
            continue
        try:
            data, _ = decoder.raw_decode(line, start)
        except json.JSONDecodeError as exc:
            logger.warning(f'get_hepscore: JSON parse error: {exc}')
            continue
        score = data.get('profiles', {}).get('hepscore', {}).get('score')
        if score is None:
            logger.debug('get_hepscore: score key absent or null in JSON')
            continue
        try:
            scores.append(str(float(score)))
        except (TypeError, ValueError):
            logger.warning(f'get_hepscore: score value is not numeric: {score!r}')
            continue

    unique_scores = list(dict.fromkeys(scores))  # deduplicate, preserving order
    if not unique_scores:
        logger.warning(f'get_hepscore: no valid score found in {payload_stdout_path}')
        return 'UNKNOWN'
    if len(unique_scores) > 1:
        logger.warning(f'get_hepscore: multiple distinct scores found {unique_scores}, using first')
    logger.info(f'get_hepscore: score={unique_scores[0]}')
    return unique_scores[0]


def get_trace_exit_code(workdir: str) -> str:
    """Look for any rucio trace curl problems using an env var and a file.

    Args:
        workdir: payload work directory.

    Returns:
        str: curl exit code.
    """
    trace_exit_code = os.environ.get('RUCIO_TRACE_ERROR', '0')
    if trace_exit_code == '0':
        # look for rucio_trace_error_file in case middleware container is used
        path = os.path.join(workdir, config.Rucio.rucio_trace_error_file)
        if os.path.exists(path):
            try:
                trace_exit_code = read_file(path)
            except FileHandlingFailure as exc:
                logger.warning(f'failed to read {path}: {exc}')
            else:
                logger.debug(f'read {trace_exit_code} from file {path}')

    return trace_exit_code


def add_features(job_metrics: str, corecount: int, add: list = None) -> str:
    """Add job and machine feature data to the job metrics if available.

    If a non-empty add list is specified, only include the corresponding features. If empty/not specified, add all.

    Args:
        job_metrics: job metrics.
        corecount: core count.
        add: features to be added.

    Returns:
        str: updated job metrics.
    """
    if add is None:
        add = []
    if job_metrics and not job_metrics.endswith(' '):
        job_metrics += ' '

    def add_sub_features(features_dic: dict, _add: list = None):
        """Helper function."""
        if _add is None:
            _add = []
        features_str = ''
        for key in features_dic.keys():
            if _add and key not in _add:
                continue
            value = features_dic.get(key, None)
            if value:
                features_str += f'{key}={value} '
        return features_str

    machinefeatures = MachineFeatures().get()
    jobfeatures = JobFeatures().get()
    # correct hs06 for corecount
    hs06 = machinefeatures.get('hs06', 0)
    total_cpu = machinefeatures.get('total_cpu', 0)
    if hs06 and total_cpu and (total_cpu != '0' or total_cpu != 0):
        perf_scale = 1
        try:
            machinefeatures_hs06 = 1.0 * int(float(hs06)) * perf_scale * corecount / (1.0 * int(float(total_cpu)))
            machinefeatures['hs06'] = float_to_rounded_string(machinefeatures_hs06, precision=2)
            logger.info(f"hs06={machinefeatures.get('hs06')} ({hs06}) total_cpu={total_cpu} corecount={corecount} perf_scale={perf_scale}")
        except (TypeError, ValueError) as exc:
            logger.warning(f'cannot process hs06 machine feature: {exc} (hs06={hs06}, total_cpu={total_cpu}, corecount={corecount})')
    features_list = [machinefeatures, jobfeatures]
    for feature_item in features_list:
        features_str = add_sub_features(feature_item, _add=add)
        if features_str:
            job_metrics += features_str

    return job_metrics


def add_analytics_data(job_metrics: str, workdir: str, state: str) -> str:
    """Add the memory leak+chi2 analytics data to the job metrics.

    Args:
        job_metrics: job metrics.
        workdir: work directory.
        state: job state.

    Returns:
        str: updated job metrics.
    """
    path = os.path.join(workdir, get_memory_monitor_output_filename())
    if os.path.exists(path):
        client = analytics.Analytics()
        # do not include tails on final update
        tails = not (state in {"finished", "failed", "holding"})
        data = client.get_fitted_data(path, tails=tails)
        slope = data.get("slope", "")
        chi2 = data.get("chi2", "")
        intersect = data.get("intersect", "")
        if slope != "":
            job_metrics += get_job_metrics_entry("leak", slope)
        if chi2 != "":
            job_metrics += get_job_metrics_entry("chi2", chi2)
        if intersect != "":
            job_metrics += get_job_metrics_entry("intersect", intersect)

    return job_metrics


def add_event_number(job_metrics: str, workdir: str) -> str:
    """Extract event number from file and add to job metrics if it exists.

    Args:
        job_metrics: job metrics.
        workdir: work directory.

    Returns:
        str: updated job metrics.
    """
    path = os.path.join(workdir, 'eventLoopHeartBeat.txt')
    if os.path.exists(path):
        last_line = find_last_line(path)
        if last_line:
            event_number = get_number_in_string(last_line)
            if event_number:
                job_metrics += get_job_metrics_entry("eventnumber", event_number)
    else:
        logger.debug(f'file {path} does not exist (skip for now)')

    return job_metrics


def get_job_metrics(job: JobData, extra: dict = None) -> str:
    """Return a properly formatted job metrics string.

    The format of the job metrics string is defined by the server. It will be reported to the server during updateJob.

    Example of job metrics:
    Number of events read | Number of events written | vmPeak maximum | vmPeak average | RSS average | ..
    Format: nEvents=<int> nEventsW=<int> vmPeakMax=<int> vmPeakMean=<int> RSSMean=<int> hs06=<float> shutdownTime=<int>
            cpuFactor=<float> cpuLimit=<float> diskLimit=<float> jobStart=<int> memLimit=<int> runLimit=<float>

    Args:
        job: job object.
        extra: any extra information to be added.

    Returns:
        str: job metrics.
    """
    if extra is None:
        extra = {}
    # get job metrics string
    job_metrics = get_job_metrics_string(job, extra=extra)

    # correct for potential initial and trailing space
    job_metrics = job_metrics.lstrip().rstrip()

    if job_metrics != "":
        logger.debug(f'job metrics=\"{job_metrics}\"')
    else:
        logger.debug("no job metrics (all values are zero)")

    # is job_metrics within allowed size?
    if len(job_metrics) > 500:
        logger.warning(f"job_metrics out of size ({len(job_metrics)})")

        # try to reduce the field size and remove the last entry which might be cut
        job_metrics = job_metrics[:500]
        job_metrics = " ".join(job_metrics.split(" ")[:-1])
        logger.warning(f"job_metrics has been reduced to: {job_metrics}")

    return job_metrics


def get_number_in_string(line: str, pattern: str = r'\ done\ processing\ event\ \#(\d+)\,') -> int:
    """Extract a number from the given string.

    E.g. file eventLoopHeartBeat.txt contains
        done processing event #20166959, run #276689 22807 events read so far  <<<===
    This function will return 20166959 as in int.

    Args:
        line: line from a file.
        pattern: reg ex pattern.

    Returns:
        int: extracted number.
    """
    event_number = None
    match = re.search(pattern, line)
    if match:
        try:
            event_number = int(match.group(1))
        except (TypeError, ValueError):
            pass

    return event_number
