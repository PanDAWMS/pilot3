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
# Authors
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Structured pilot telemetry (pilot attributes) sent to the PanDA server once per job.

The document is sent after the final job update has been accepted by the server. It contains the raw pilot
timestamps, durations derived from them and a few file counters. It is independent of the job metadata (job
report) and of the ``pilotTiming`` field of the job update, which is left unchanged.

Request body (schema version 1)::

    {"job_id": <int>, "pilot_version": "<str>",
     "pilot_attributes": {"schema_version": 1,
                          "timestamps": {<name>: <int epoch seconds>, ..},
                          "durations": {<name>: <int seconds>, ..},
                          "counters": {<name>: <int or dict>, ..},
                          "truncated": [<dropped section>, ..]}}   # only if the size limit was hit

Missing values are omitted rather than reported as 0, and durations keep their sign. A section that could not be
assembled is omitted, while a section with nothing to report is present but empty. The size limit applies to the
uncompressed JSON of the pilot_attributes dictionary only.

The request body is logged and then sent to the server (see send_pilot_telemetry()).

Nothing in this module raises, and telemetry never affects the job.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

from pilot.util.constants import (
    PILOT_ATTRIBUTES_ENDPOINT,
    PILOT_ATTRIBUTES_KEY,
    PILOT_ATTRIBUTES_MAX_BYTES,
    PILOT_ATTRIBUTES_SCHEMA_VERSION,
    PILOT_KILL_SIGNAL,
    PILOT_MULTIJOB_START_TIME,
    PILOT_POST_FINAL_UPDATE,
    PILOT_POST_GETJOB,
    PILOT_POST_LOG_TAR,
    PILOT_POST_PAYLOAD,
    PILOT_POST_PILOT_SETUP,
    PILOT_POST_REMOTEIO,
    PILOT_POST_SETUP,
    PILOT_POST_STAGEIN,
    PILOT_POST_STAGEOUT,
    PILOT_PRE_FINAL_UPDATE,
    PILOT_PRE_GETJOB,
    PILOT_PRE_LOG_TAR,
    PILOT_PRE_PAYLOAD,
    PILOT_PRE_REMOTEIO,
    PILOT_PRE_SETUP,
    PILOT_PRE_STAGEIN,
    PILOT_PRE_STAGEOUT,
    PILOT_START_TIME,
    get_pilot_version,
)
from pilot.util.https import send_update

logger = logging.getLogger(__name__)

# Timing constants read from the job's own entry in the pilot timing dictionary.
JOB_TIMESTAMPS = (
    PILOT_PRE_GETJOB,
    PILOT_POST_GETJOB,
    PILOT_PRE_SETUP,
    PILOT_POST_SETUP,
    PILOT_POST_PILOT_SETUP,
    PILOT_PRE_STAGEIN,
    PILOT_POST_STAGEIN,
    PILOT_PRE_REMOTEIO,
    PILOT_POST_REMOTEIO,
    PILOT_PRE_PAYLOAD,
    PILOT_POST_PAYLOAD,
    PILOT_PRE_STAGEOUT,
    PILOT_POST_STAGEOUT,
    PILOT_PRE_LOG_TAR,
    PILOT_POST_LOG_TAR,
    PILOT_PRE_FINAL_UPDATE,
    PILOT_POST_FINAL_UPDATE,
)

# Timing constants read from the wrapper entries of the pilot timing dictionary: (entry, constant).
# PILOT_END_TIME is recorded at pilot exit and can therefore never be included.
WRAPPER_TIMESTAMPS = (
    ('0', PILOT_START_TIME),
    ('0', PILOT_KILL_SIGNAL),
    ('1', PILOT_MULTIJOB_START_TIME),
)

# Durations as raw intervals between two timestamps: (duration name, start timestamp, end timestamp).
# No correction is applied (e.g. remote i/o time is not moved from setup to stage-in as for pilotTiming).
DURATIONS = (
    ('getjob', 'pre_getjob', 'post_getjob'),
    ('initial_setup', 'multijob_start_time', 'pre_getjob'),
    ('setup', 'pre_setup', 'post_setup'),  # may include the setup done inside the payload
    ('setup_pilot', 'pre_setup', 'post_pilot_setup'),
    ('remoteio', 'pre_remoteio', 'post_remoteio'),
    ('stagein', 'pre_stagein', 'post_stagein'),
    ('payload', 'pre_payload', 'post_payload'),
    ('stageout', 'pre_stageout', 'post_stageout'),  # includes log tar creation and log stage-out
    ('log_tar', 'pre_log_tar', 'post_log_tar'),
    ('final_update', 'pre_final_update', 'post_final_update'),
    ('time_to_payload_start', 'post_getjob', 'pre_payload'),
)

# Sections that may be dropped (in this order) to respect the size limit. Timestamps are never dropped.
DROPPABLE_SECTIONS = ('counters', 'durations')

# Key used in input_status for files whose status was never set.
UNSET_STATUS = 'unset'


def timestamp_key(timing_constant: str) -> str:
    """Return the telemetry key for a timing constant.

    The leading ``PILOT_`` is removed and the rest is lowercased, e.g. ``PILOT_PRE_STAGEIN`` -> ``pre_stagein``
    and ``PILOT_POST_PILOT_SETUP`` -> ``post_pilot_setup``.

    Args:
        timing_constant: Timing constant name.

    Returns:
        Telemetry key.
    """
    prefix = 'PILOT_'
    if timing_constant.startswith(prefix):
        timing_constant = timing_constant[len(prefix):]

    return timing_constant.lower()


def to_int(value: Any) -> int | None:
    """Convert a recorded value (time measurement, size) to an integer.

    Args:
        value: Value to convert, e.g. a time measurement from the pilot timing dictionary.

    Returns:
        Integer value, or None if the value is missing or not a number.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        logger.debug(f'ignoring malformed value: {value!r}')
        return None


def _add_timestamp(timestamps: dict, entry: Any, timing_constant: str) -> None:
    """Add one timestamp to the given dictionary if it was recorded.

    Args:
        timestamps: Timestamp dictionary to update.
        entry: Dictionary of time measurements (one entry of the pilot timing dictionary).
        timing_constant: Timing constant to look up.
    """
    if not isinstance(entry, dict):
        return
    value = to_int(entry.get(timing_constant))
    if value is not None:
        timestamps[timestamp_key(timing_constant)] = value


def get_timestamps(job: Any, args: Any) -> dict:
    """Return the recorded timestamps for the given job.

    Timing constants that were never recorded are omitted.

    Args:
        job: Job object.
        args: Pilot arguments object (holds the pilot timing dictionary).

    Returns:
        Dictionary of telemetry key -> integer epoch seconds.
    """
    timing = getattr(args, 'timing', None)
    if not isinstance(timing, dict):
        return {}

    timestamps = {}
    for entry_id, timing_constant in WRAPPER_TIMESTAMPS:
        _add_timestamp(timestamps, timing.get(entry_id), timing_constant)

    job_entry = timing.get(job.jobid)
    for timing_constant in JOB_TIMESTAMPS:
        _add_timestamp(timestamps, job_entry, timing_constant)

    return timestamps


def get_durations(timestamps: dict, job: Any) -> dict:
    """Return the durations derived from the timestamps.

    A duration is omitted if either of its timestamps is missing. The sign is kept, since a negative interval is
    diagnostic information.

    Args:
        timestamps: Dictionary returned by get_timestamps().
        job: Job object (for the lsetup time).

    Returns:
        Dictionary of duration name -> integer seconds.
    """
    durations = {}
    for name, start, end in DURATIONS:
        if start in timestamps and end in timestamps:
            durations[name] = timestamps[end] - timestamps[start]

    # lsetuptime is only set when it was measured (the default 0 means not measured)
    lsetup = to_int(getattr(job, 'lsetuptime', None))
    if lsetup:
        durations['lsetup'] = lsetup

    return durations


def _total_size(files: list) -> int:
    """Return the total size of the given files.

    Args:
        files: List of FileSpec objects.

    Returns:
        Total size in bytes (unknown sizes count as 0).
    """
    return sum(to_int(getattr(fspec, 'filesize', None)) or 0 for fspec in files)


def get_counters(job: Any) -> dict:
    """Return file counters for the given job.

    Args:
        job: Job object.

    Returns:
        Dictionary of counters.
    """
    indata = list(getattr(job, 'indata', None) or [])
    outdata = list(getattr(job, 'outdata', None) or [])
    logdata = list(getattr(job, 'logdata', None) or [])

    input_status = {}
    for fspec in indata:
        status = getattr(fspec, 'status', None) or UNSET_STATUS
        input_status[status] = input_status.get(status, 0) + 1

    return {
        'n_input_files': len(indata),
        'input_bytes': _total_size(indata),
        'input_status': input_status,
        'n_output_files': len(outdata),
        'output_bytes': _total_size(outdata),
        # alternative stage-out applies to both output and log files
        'n_altstaged': sum(1 for fspec in outdata + logdata if getattr(fspec, 'is_altstaged', False)),
    }


def get_pilot_telemetry(job: Any, args: Any) -> dict:
    """Assemble the telemetry document for the given job.

    Each section is assembled independently, so a failing section is omitted and the rest is returned.

    Args:
        job: Job object.
        args: Pilot arguments object.

    Returns:
        Telemetry document (the value sent under PILOT_ATTRIBUTES_KEY).
    """
    document = {'schema_version': PILOT_ATTRIBUTES_SCHEMA_VERSION}

    try:
        document['timestamps'] = get_timestamps(job, args)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f'failed to assemble telemetry timestamps: {exc}')

    try:
        document['durations'] = get_durations(document.get('timestamps', {}), job)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f'failed to assemble telemetry durations: {exc}')

    try:
        document['counters'] = get_counters(job)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f'failed to assemble telemetry counters: {exc}')

    return document


def attributes_size(document: dict) -> int:
    """Return the size of the uncompressed JSON of the pilot attributes dictionary.

    The dictionary is encoded as by the transport (default JSON separators), which is at least as large as
    any more compact encoding the server may use when validating it.

    Args:
        document: Pilot attributes dictionary.

    Returns:
        Size in bytes.
    """
    return len(json.dumps(document).encode('utf-8'))


def apply_size_limit(document: dict, max_bytes: int) -> dict | None:
    """Make sure the pilot attributes dictionary does not exceed the size limit.

    Sections are dropped in the order of DROPPABLE_SECTIONS until the dictionary fits, and the dropped sections are
    listed under ``truncated``. Timestamps are never dropped: if the dictionary is still too large, nothing is sent.

    Args:
        document: Pilot attributes dictionary (modified in place).
        max_bytes: Maximum size of its uncompressed JSON.

    Returns:
        The dictionary if it fits within the limit, otherwise None.
    """
    size = attributes_size(document)
    if size <= max_bytes:
        return document

    truncated = []
    for section in DROPPABLE_SECTIONS:
        if section in document:
            del document[section]
            truncated.append(section)
            document['truncated'] = truncated
            size = attributes_size(document)
            if size <= max_bytes:
                logger.warning(f'telemetry exceeded {max_bytes} B - dropped section(s): {truncated}')
                return document

    logger.warning(f'telemetry is {size} B even without {truncated} (limit {max_bytes} B) - will not be sent')
    return None


def send_pilot_telemetry(job: Any, args: Any) -> None:
    """Assemble the telemetry document for the given job and send it to the server.

    The request body is logged and then sent to the server in a single attempt. Failures are logged and otherwise
    ignored.

    Args:
        job: Job object.
        args: Pilot arguments object.
    """
    try:
        document = apply_size_limit(get_pilot_telemetry(job, args), PILOT_ATTRIBUTES_MAX_BYTES)
        if document is None:
            return

        body = {
            'job_id': job.jobid,
            'pilot_version': get_pilot_version(),
            PILOT_ATTRIBUTES_KEY: document,
        }
        logger.info(f'pilot telemetry ready to be sent to {PILOT_ATTRIBUTES_ENDPOINT} '
                    f'({PILOT_ATTRIBUTES_KEY}: {attributes_size(document)} B, limit {PILOT_ATTRIBUTES_MAX_BYTES} B): '
                    f'{json.dumps(body)}')

        # send_update() gzips the body (with Content-Encoding: gzip) via request2(), with a curl fallback.
        time_before = time.time()
        result = send_update(PILOT_ATTRIBUTES_ENDPOINT, body, args.url, args.port, job=None,
                             ipv=args.internet_protocol_version, max_attempts=1)
        logger.info(f'pilot telemetry sent to {PILOT_ATTRIBUTES_ENDPOINT} in {time.time() - time_before:.1f} s: '
                    f'ok={result.ok}, message={result.message!r}')
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.warning(f'failed to send pilot telemetry: {exc}')


def handle_pilot_telemetry(job: Any, args: Any, final_update_accepted: bool) -> None:
    """Send the pilot telemetry after the final job update, if the conditions are met.

    The telemetry is sent when the pilot updates the server itself and the final update was accepted, i.e. the
    job was marked as completed. The caller only calls this function once per job, since update_server() refuses
    to run for a completed job.

    Args:
        job: Job object.
        args: Pilot arguments object.
        final_update_accepted: True if the server accepted the job update.
    """
    if not getattr(args, 'update_server', False):
        logger.debug('pilot telemetry is not supported when the pilot does not update the server - skipped')
        return

    if not (final_update_accepted and getattr(job, 'completed', False)):
        logger.debug('final job update was not accepted - pilot telemetry will not be sent')
        return

    send_pilot_telemetry(job, args)
