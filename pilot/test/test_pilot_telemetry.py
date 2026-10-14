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

"""Unit tests for the structured pilot telemetry (pilot attributes)."""

import json
import math
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.control import job as job_module
from pilot.control.payloads import generic as generic_payload
from pilot.util import telemetry
from pilot.util.config import config
from pilot.util.constants import (
    PILOT_ATTRIBUTES_ENDPOINT,
    PILOT_ATTRIBUTES_KEY,
    PILOT_ATTRIBUTES_MAX_BYTES,
    PILOT_ATTRIBUTES_SCHEMA_VERSION,
    PILOT_END_TIME,
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
from pilot.util.https import UpdateResult

JOB_ID = 6800000000
BASE = 1758800000

# a complete, realistic timing dictionary (floats, as recorded by time.time())
FULL_JOB_TIMING = {
    PILOT_PRE_GETJOB: BASE + 12.7,
    PILOT_POST_GETJOB: BASE + 14.2,
    PILOT_PRE_STAGEIN: BASE + 20.9,
    PILOT_POST_STAGEIN: BASE + 230.1,
    PILOT_PRE_SETUP: BASE + 231.5,
    PILOT_POST_PILOT_SETUP: BASE + 295.3,
    PILOT_PRE_REMOTEIO: BASE + 240,
    PILOT_POST_REMOTEIO: BASE + 271,
    PILOT_POST_SETUP: BASE + 349.8,
    PILOT_PRE_PAYLOAD: BASE + 416.4,
    PILOT_POST_PAYLOAD: BASE + 7416.9,
    PILOT_PRE_STAGEOUT: BASE + 7420.0,
    PILOT_PRE_LOG_TAR: BASE + 7500.5,
    PILOT_POST_LOG_TAR: BASE + 7510.2,
    PILOT_POST_STAGEOUT: BASE + 7530.8,
    PILOT_PRE_FINAL_UPDATE: BASE + 7531.1,
    PILOT_POST_FINAL_UPDATE: BASE + 7533.6,
}

EXPECTED_JOB_TIMESTAMPS = {
    'pre_getjob': BASE + 12,
    'post_getjob': BASE + 14,
    'pre_stagein': BASE + 20,
    'post_stagein': BASE + 230,
    'pre_setup': BASE + 231,
    'post_pilot_setup': BASE + 295,
    'pre_remoteio': BASE + 240,
    'post_remoteio': BASE + 271,
    'post_setup': BASE + 349,
    'pre_payload': BASE + 416,
    'post_payload': BASE + 7416,
    'pre_stageout': BASE + 7420,
    'pre_log_tar': BASE + 7500,
    'post_log_tar': BASE + 7510,
    'post_stageout': BASE + 7530,
    'pre_final_update': BASE + 7531,
    'post_final_update': BASE + 7533,
}

EXPECTED_DURATIONS = {
    'getjob': 2,
    'initial_setup': 12,
    'setup': 118,
    'setup_pilot': 64,
    'remoteio': 31,
    'stagein': 210,
    'payload': 7000,
    'stageout': 110,
    'log_tar': 10,
    'final_update': 2,
    'time_to_payload_start': 402,
}


def full_timing() -> dict:
    """Return a complete pilot timing dictionary (wrapper entries and one job).

    Returns:
        Pilot timing dictionary.
    """
    return {
        '0': {PILOT_START_TIME: BASE - 29.6},  # the pilot started before the (first) multijob start
        '1': {PILOT_MULTIJOB_START_TIME: BASE + 0.9},
        JOB_ID: dict(FULL_JOB_TIMING),
    }


def make_args(timing: dict = None, **kwargs) -> SimpleNamespace:
    """Return a minimal pilot arguments object.

    Args:
        timing: Pilot timing dictionary (default: empty).
        **kwargs: Attributes to add or override.

    Returns:
        Arguments object.
    """
    values = {
        'timing': {} if timing is None else timing,
        'update_server': True,
        'url': 'https://pandaserver.example.org',
        'port': 25443,
        'internet_protocol_version': 'IPv4',
    }
    values.update(kwargs)
    return SimpleNamespace(**values)


def make_file(status: str = None, filesize: int = 0, is_altstaged: bool = None) -> SimpleNamespace:
    """Return a fake FileSpec object.

    Args:
        status: Transfer status.
        filesize: File size in bytes.
        is_altstaged: Alternative stage-out flag.

    Returns:
        Fake file object.
    """
    return SimpleNamespace(status=status, filesize=filesize, is_altstaged=is_altstaged)


def make_job(**kwargs) -> SimpleNamespace:
    """Return a minimal job object.

    Args:
        **kwargs: Attributes to add or override.

    Returns:
        Job object.
    """
    values = {'jobid': JOB_ID, 'indata': [], 'outdata': [], 'logdata': [], 'lsetuptime': 0, 'completed': False}
    values.update(kwargs)
    return SimpleNamespace(**values)


class TelemetryTestCase(unittest.TestCase):
    """Base class resetting the pilot singletons around each test."""

    def setUp(self):
        """Reset the error code lists and snapshot the pilot cache."""
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []
        self._cache_snapshot = dict(vars(get_pilot_cache()))

    def tearDown(self):
        """Restore the pilot cache and reset the error code lists."""
        cache = get_pilot_cache()
        vars(cache).clear()
        vars(cache).update(self._cache_snapshot)
        ErrorCodes.pilot_error_codes = []
        ErrorCodes.pilot_error_diags = []


class TestTimestampKey(TelemetryTestCase):
    """Timing constant names are mapped to short lowercase keys."""

    def test_prefix_removed_and_lowercased(self):
        """The leading PILOT_ is removed."""
        self.assertEqual(telemetry.timestamp_key(PILOT_PRE_STAGEIN), 'pre_stagein')
        self.assertEqual(telemetry.timestamp_key(PILOT_START_TIME), 'start_time')

    def test_only_leading_prefix_removed(self):
        """An inner PILOT_ is kept."""
        self.assertEqual(telemetry.timestamp_key(PILOT_POST_PILOT_SETUP), 'post_pilot_setup')

    def test_name_without_prefix(self):
        """A name without the prefix is only lowercased."""
        self.assertEqual(telemetry.timestamp_key('SOME_TIME'), 'some_time')


class TestToInt(TelemetryTestCase):
    """Conversion of recorded values to integers."""

    def test_numbers(self):
        """Floats are truncated, ints and numeric strings are kept."""
        self.assertEqual(telemetry.to_int(12.9), 12)
        self.assertEqual(telemetry.to_int(7), 7)
        self.assertEqual(telemetry.to_int('42'), 42)

    def test_zero_is_a_value(self):
        """Zero is a value, not a missing one."""
        self.assertEqual(telemetry.to_int(0), 0)

    def test_missing_is_not_malformed(self):
        """A missing value is not reported as malformed, a malformed one is."""
        with patch.object(telemetry.logger, 'debug') as debug:
            self.assertIsNone(telemetry.to_int(None))
        debug.assert_not_called()
        with patch.object(telemetry.logger, 'debug') as debug:
            self.assertIsNone(telemetry.to_int('abc'))
        debug.assert_called_once()

    def test_missing_and_malformed(self):
        """Missing and malformed values give None."""
        for value in (None, True, False, 'abc', [], {}, math.nan, math.inf):
            with self.subTest(value=value):
                self.assertIsNone(telemetry.to_int(value))


class TestTimestamps(TelemetryTestCase):
    """Collection of the timestamps from the pilot timing dictionary."""

    def test_full(self):
        """All recorded timestamps are included as int epoch seconds."""
        timestamps = telemetry.get_timestamps(make_job(), make_args(full_timing()))
        expected = dict(EXPECTED_JOB_TIMESTAMPS, start_time=BASE - 30, multijob_start_time=BASE)
        self.assertEqual(timestamps, expected)
        self.assertTrue(all(isinstance(value, int) for value in timestamps.values()))

    def test_every_job_constant_is_collected(self):
        """Each job timing constant is collected on its own."""
        for constant in telemetry.JOB_TIMESTAMPS:
            with self.subTest(constant=constant):
                timestamps = telemetry.get_timestamps(make_job(), make_args({JOB_ID: {constant: BASE}}))
                self.assertEqual(timestamps, {telemetry.timestamp_key(constant): BASE})

    def test_partial_omits_missing(self):
        """Timestamps that were never recorded are omitted, not sent as 0."""
        timestamps = telemetry.get_timestamps(make_job(), make_args({JOB_ID: {PILOT_PRE_GETJOB: BASE}}))
        self.assertEqual(timestamps, {'pre_getjob': BASE})

    def test_zero_is_distinct_from_missing(self):
        """A recorded 0 is reported, a missing value is not."""
        timestamps = telemetry.get_timestamps(make_job(), make_args({JOB_ID: {PILOT_PRE_GETJOB: 0}}))
        self.assertEqual(timestamps, {'pre_getjob': 0})

    def test_wrapper_sources(self):
        """Start time and kill signal come from '0', the multijob start time from '1'."""
        timing = {
            '0': {PILOT_START_TIME: BASE + 1, PILOT_KILL_SIGNAL: BASE + 2, PILOT_MULTIJOB_START_TIME: BASE + 3,
                  PILOT_END_TIME: BASE + 4},
            '1': {PILOT_START_TIME: BASE + 5, PILOT_KILL_SIGNAL: BASE + 6, PILOT_MULTIJOB_START_TIME: BASE + 7},
        }
        timestamps = telemetry.get_timestamps(make_job(), make_args(timing))
        self.assertEqual(timestamps, {'start_time': BASE + 1, 'kill_signal': BASE + 2, 'multijob_start_time': BASE + 7})

    def test_end_time_never_included(self):
        """The pilot end time is not part of the telemetry, wherever it is found."""
        timing = {'0': {PILOT_END_TIME: BASE}, JOB_ID: {PILOT_END_TIME: BASE}}
        self.assertEqual(telemetry.get_timestamps(make_job(), make_args(timing)), {})

    def test_other_jobs_ignored(self):
        """Only the job's own entry is read."""
        timing = {JOB_ID + 1: {PILOT_PRE_GETJOB: BASE}}
        self.assertEqual(telemetry.get_timestamps(make_job(), make_args(timing)), {})

    def test_unknown_keys_ignored(self):
        """Keys that are not known timing constants are not included."""
        timing = {JOB_ID: {'PILOT_SOMETHING_NEW': BASE, PILOT_PRE_GETJOB: BASE}}
        self.assertEqual(telemetry.get_timestamps(make_job(), make_args(timing)), {'pre_getjob': BASE})

    def test_empty_and_missing_dictionary(self):
        """An empty, missing or non-dict timing dictionary gives no timestamps."""
        for timing in ({}, None, [], 'x'):
            with self.subTest(timing=timing):
                args = make_args()
                args.timing = timing
                self.assertEqual(telemetry.get_timestamps(make_job(), args), {})

    def test_args_without_timing(self):
        """An arguments object without a timing dictionary gives no timestamps."""
        self.assertEqual(telemetry.get_timestamps(make_job(), SimpleNamespace()), {})

    def test_malformed_entries(self):
        """Malformed entries and values are skipped, the rest is kept."""
        timing = {'0': ['not', 'a', 'dict'], '1': {PILOT_MULTIJOB_START_TIME: 'garbage'},
                  JOB_ID: {PILOT_PRE_GETJOB: None, PILOT_POST_GETJOB: BASE}}
        self.assertEqual(telemetry.get_timestamps(make_job(), make_args(timing)), {'post_getjob': BASE})

    def test_malformed_job_entry(self):
        """A job entry that is not a dictionary is skipped, the wrapper entries are kept."""
        timing = {'0': {PILOT_START_TIME: BASE}, JOB_ID: None}
        self.assertEqual(telemetry.get_timestamps(make_job(), make_args(timing)), {'start_time': BASE})


class TestDurations(TelemetryTestCase):
    """Durations derived from the timestamps."""

    def test_full(self):
        """All durations are computed from the int timestamps."""
        timestamps = telemetry.get_timestamps(make_job(), make_args(full_timing()))
        self.assertEqual(telemetry.get_durations(timestamps, make_job()), EXPECTED_DURATIONS)

    def test_each_duration(self):
        """Each duration uses its own pair of timestamps."""
        for name, start, end in telemetry.DURATIONS:
            with self.subTest(name=name):
                timestamps = {start: 100, end: 107}
                self.assertEqual(telemetry.get_durations(timestamps, make_job()), {name: 7})

    def test_missing_endpoint_omits_duration(self):
        """A duration is omitted when either timestamp is missing."""
        for name, start, end in telemetry.DURATIONS:
            with self.subTest(name=name):
                self.assertNotIn(name, telemetry.get_durations({start: 100}, make_job()))
                self.assertNotIn(name, telemetry.get_durations({end: 100}, make_job()))

    def test_sign_is_kept(self):
        """A negative interval is reported as negative."""
        durations = telemetry.get_durations({'pre_getjob': 110, 'post_getjob': 100}, make_job())
        self.assertEqual(durations, {'getjob': -10})

    def test_zero_duration_kept(self):
        """A zero interval is reported."""
        durations = telemetry.get_durations({'pre_getjob': 100, 'post_getjob': 100}, make_job())
        self.assertEqual(durations, {'getjob': 0})

    def test_lsetup_present(self):
        """A measured lsetup time is included."""
        self.assertEqual(telemetry.get_durations({}, make_job(lsetuptime=5)), {'lsetup': 5})

    def test_lsetup_absent(self):
        """The default 0 (not measured), a missing attribute and a malformed value are not included."""
        self.assertEqual(telemetry.get_durations({}, make_job(lsetuptime=0)), {})
        self.assertEqual(telemetry.get_durations({}, SimpleNamespace()), {})
        self.assertEqual(telemetry.get_durations({}, make_job(lsetuptime='abc')), {})


class TestCounters(TelemetryTestCase):
    """File counters."""

    def test_input_counters_and_statuses(self):
        """Input files are counted by status, and their sizes are summed."""
        indata = [make_file('transferred', 10), make_file('transferred', 20), make_file('remote_io', 30),
                  make_file('no_transfer', 40), make_file('failed', 50), make_file(None, 60)]
        counters = telemetry.get_counters(make_job(indata=indata))
        self.assertEqual(counters['n_input_files'], 6)
        self.assertEqual(counters['input_bytes'], 210)
        self.assertEqual(counters['input_status'],
                         {'transferred': 2, 'remote_io': 1, 'no_transfer': 1, 'failed': 1, telemetry.UNSET_STATUS: 1})

    def test_output_counters(self):
        """Output files are counted and their sizes summed; log files are not output files."""
        counters = telemetry.get_counters(make_job(outdata=[make_file(filesize=5), make_file(filesize=7)],
                                                   logdata=[make_file(filesize=1000)]))
        self.assertEqual(counters['n_output_files'], 2)
        self.assertEqual(counters['output_bytes'], 12)

    def test_altstaged(self):
        """Alternative stage-out is counted for output and log files, not for input files."""
        job = make_job(indata=[make_file(is_altstaged=True)],
                       outdata=[make_file(is_altstaged=True), make_file(is_altstaged=None), make_file(is_altstaged=False)],
                       logdata=[make_file(is_altstaged=True)])
        self.assertEqual(telemetry.get_counters(job)['n_altstaged'], 2)

    def test_unknown_sizes_count_as_zero(self):
        """Missing or malformed sizes do not break the sum."""
        indata = [make_file(filesize=None), make_file(filesize='abc'), make_file(filesize=3), SimpleNamespace()]
        self.assertEqual(telemetry.get_counters(make_job(indata=indata))['input_bytes'], 3)

    def test_no_files(self):
        """A job without files gives zero counters and an empty status dictionary."""
        expected = {'n_input_files': 0, 'input_bytes': 0, 'input_status': {}, 'n_output_files': 0,
                    'output_bytes': 0, 'n_altstaged': 0}
        self.assertEqual(telemetry.get_counters(make_job()), expected)
        self.assertEqual(telemetry.get_counters(SimpleNamespace(indata=None)), expected)


class TestGetPilotTelemetry(TelemetryTestCase):
    """Assembly of the full document."""

    def test_full_document(self):
        """The document holds the schema version and all sections."""
        job = make_job(indata=[make_file('transferred', 10)], lsetuptime=3)
        document = telemetry.get_pilot_telemetry(job, make_args(full_timing()))
        self.assertEqual(document['schema_version'], PILOT_ATTRIBUTES_SCHEMA_VERSION)
        self.assertEqual(document['timestamps'], dict(EXPECTED_JOB_TIMESTAMPS, start_time=BASE - 30, multijob_start_time=BASE))
        self.assertEqual(document['durations'], dict(EXPECTED_DURATIONS, lsetup=3))
        self.assertEqual(document['counters']['n_input_files'], 1)
        self.assertNotIn('transfers', document)
        self.assertNotIn('truncated', document)

    def test_empty_sections_are_present(self):
        """Sections with nothing to report are present but empty."""
        document = telemetry.get_pilot_telemetry(make_job(), make_args())
        self.assertEqual(document['timestamps'], {})
        self.assertEqual(document['durations'], {})
        self.assertIn('counters', document)

    def test_failing_section_is_omitted(self):
        """A section that fails is omitted and the others are kept."""
        for name in ('get_timestamps', 'get_durations', 'get_counters'):
            section = name.replace('get_', '')
            with self.subTest(section=section), patch.object(telemetry, name, side_effect=RuntimeError('boom')), \
                    self.assertLogs(telemetry.logger, level='WARNING'):
                document = telemetry.get_pilot_telemetry(make_job(), make_args(full_timing()))
                self.assertNotIn(section, document)
                self.assertEqual(set(document), {'schema_version', 'timestamps', 'durations', 'counters'} - {section})

    def test_durations_without_timestamps(self):
        """If the timestamps failed, durations are still assembled (lsetup only)."""
        with patch.object(telemetry, 'get_timestamps', side_effect=RuntimeError('boom')), \
                self.assertLogs(telemetry.logger, level='WARNING'):
            document = telemetry.get_pilot_telemetry(make_job(lsetuptime=4), make_args(full_timing()))
        self.assertEqual(document['durations'], {'lsetup': 4})


class TestSizeLimit(TelemetryTestCase):
    """The pilot attributes dictionary never exceeds the size limit."""

    @staticmethod
    def _document() -> dict:
        """Return a pilot attributes dictionary with all three sections.

        Returns:
            Pilot attributes dictionary.
        """
        return {'schema_version': 1,
                'timestamps': {'pre_getjob': BASE},
                'durations': {'getjob': 2, 'payload': 7000},
                'counters': {'n_input_files': 12, 'input_status': {'transferred': 12}}}

    def test_agreed_contract(self):
        """Endpoint, key and limit are the ones agreed with the server side."""
        self.assertEqual(PILOT_ATTRIBUTES_ENDPOINT, 'api/v1/pilot/update_pilot_attributes')
        self.assertEqual(PILOT_ATTRIBUTES_KEY, 'pilot_attributes')
        self.assertEqual(PILOT_ATTRIBUTES_MAX_BYTES, 65536)

    def test_size_matches_transport_encoding(self):
        """The size is that of the uncompressed JSON as encoded by the transport."""
        document = self._document()
        self.assertEqual(telemetry.attributes_size(document), len(json.dumps(document).encode('utf-8')))

    def test_under_limit(self):
        """A dictionary within the limit is returned unchanged."""
        document = self._document()
        self.assertEqual(telemetry.apply_size_limit(document, PILOT_ATTRIBUTES_MAX_BYTES), self._document())

    def test_exactly_at_limit(self):
        """A dictionary exactly at the limit is accepted."""
        document = self._document()
        self.assertIs(telemetry.apply_size_limit(document, telemetry.attributes_size(document)), document)
        self.assertNotIn('truncated', document)

    def test_drop_counters(self):
        """Counters are dropped first."""
        reduced = self._document()
        del reduced['counters']
        reduced['truncated'] = ['counters']
        with self.assertLogs(telemetry.logger, level='WARNING'):
            result = telemetry.apply_size_limit(self._document(), telemetry.attributes_size(reduced))
        self.assertEqual(result, reduced)

    def test_drop_counters_and_durations(self):
        """Durations are dropped next, and timestamps are kept."""
        reduced = self._document()
        del reduced['counters']
        del reduced['durations']
        reduced['truncated'] = ['counters', 'durations']
        with self.assertLogs(telemetry.logger, level='WARNING'):
            result = telemetry.apply_size_limit(self._document(), telemetry.attributes_size(reduced))
        self.assertEqual(result, reduced)
        self.assertEqual(result['timestamps'], {'pre_getjob': BASE})

    def test_missing_section_is_not_listed(self):
        """A section that is already missing is not listed as truncated."""
        document = self._document()
        del document['counters']
        reduced = self._document()
        del reduced['counters']
        del reduced['durations']
        reduced['truncated'] = ['durations']
        with self.assertLogs(telemetry.logger, level='WARNING'):
            result = telemetry.apply_size_limit(document, telemetry.attributes_size(reduced))
        self.assertEqual(result, reduced)

    def test_timestamps_never_dropped(self):
        """Nothing is sent rather than dropping the timestamps, even if that would make the dictionary fit."""
        without_timestamps = {'schema_version': 1, 'truncated': ['counters', 'durations', 'timestamps']}
        document = self._document()
        with self.assertLogs(telemetry.logger, level='WARNING'):
            result = telemetry.apply_size_limit(document, telemetry.attributes_size(without_timestamps))
        self.assertIsNone(result)
        self.assertEqual(document['timestamps'], {'pre_getjob': BASE})

    def test_still_too_large(self):
        """If the dictionary is too large even with timestamps only, nothing is returned."""
        with self.assertLogs(telemetry.logger, level='WARNING') as logs:
            self.assertIsNone(telemetry.apply_size_limit(self._document(), 10))
        self.assertIn('will not be sent', logs.output[-1])


def logged_body(output: list) -> dict:
    """Return the request body from the 'ready to be sent' log line.

    Args:
        output: Log output captured by assertLogs().

    Returns:
        Request body.
    """
    lines = [entry for entry in output if 'pilot telemetry ready to be sent' in entry]
    assert len(lines) == 1, lines
    return json.loads(lines[0].split(' B): ', 1)[1])


class TestSendPilotTelemetry(TelemetryTestCase):
    """The request body is logged and sent once to the server (send_update() is mocked)."""

    def setUp(self):
        """Mock send_update() so that no server request is made."""
        super().setUp()
        self.result = UpdateResult(ok=True, attempts=1, response={'success': True}, success=True, status_code=0,
                                   command=None, message='')
        patcher = patch.object(telemetry, 'send_update', return_value=self.result)
        self.send = patcher.start()
        self.addCleanup(patcher.stop)

    def test_body_logged(self):
        """The complete request body is logged as JSON."""
        job = make_job(indata=[make_file('transferred', 10)])
        args = make_args(full_timing())
        with self.assertLogs(telemetry.logger, level='INFO') as logs:
            telemetry.send_pilot_telemetry(job, args)
        body = logged_body(logs.output)
        self.assertEqual(set(body), {'job_id', 'pilot_version', PILOT_ATTRIBUTES_KEY})
        self.assertEqual(body['job_id'], JOB_ID)
        self.assertEqual(body['pilot_version'], get_pilot_version())
        self.assertEqual(body[PILOT_ATTRIBUTES_KEY], telemetry.get_pilot_telemetry(job, args))
        self.assertIn(PILOT_ATTRIBUTES_ENDPOINT, logs.output[-1])

    def test_logged_size_is_attributes_size(self):
        """The logged size is that of the pilot attributes dictionary only."""
        with self.assertLogs(telemetry.logger, level='INFO') as logs:
            telemetry.send_pilot_telemetry(make_job(), make_args(full_timing()))
        document = logged_body(logs.output)[PILOT_ATTRIBUTES_KEY]
        ready = [entry for entry in logs.output if 'pilot telemetry ready to be sent' in entry]
        self.assertIn(f'{PILOT_ATTRIBUTES_KEY}: {telemetry.attributes_size(document)} B', ready[0])

    def test_sent_once(self):
        """The body is sent once to the endpoint, without the job object."""
        job, args = make_job(), make_args(full_timing())
        with self.assertLogs(telemetry.logger, level='INFO') as logs:
            telemetry.send_pilot_telemetry(job, args)
        self.send.assert_called_once()
        call_args, call_kwargs = self.send.call_args
        self.assertEqual(call_args[0], PILOT_ATTRIBUTES_ENDPOINT)
        self.assertEqual(call_args[2:], (args.url, args.port))
        self.assertEqual(call_kwargs, {'job': None, 'ipv': 'IPv4', 'max_attempts': 1})
        self.assertEqual(call_args[1], logged_body(logs.output))
        self.assertEqual(call_args[1][PILOT_ATTRIBUTES_KEY], telemetry.get_pilot_telemetry(job, args))
        self.assertTrue(any('ok=True' in entry for entry in logs.output))

    def test_rejected_update_is_only_logged(self):
        """A rejected request is logged and nothing else happens."""
        self.send.return_value = UpdateResult(ok=False, attempts=1, response=None, success=None, status_code=None,
                                              command=None, message='No valid server response after retries')
        job = make_job()
        with self.assertLogs(telemetry.logger, level='INFO') as logs:
            telemetry.send_pilot_telemetry(job, make_args(full_timing()))
        self.assertTrue(any('ok=False' in entry and 'No valid server response' in entry for entry in logs.output))
        self.assertFalse(job.completed)
        self.assertEqual(ErrorCodes.pilot_error_codes, [])

    def test_send_exception_is_swallowed(self):
        """An exception while sending is logged as a warning."""
        self.send.side_effect = RuntimeError('boom')
        with self.assertLogs(telemetry.logger, level='WARNING') as logs:
            telemetry.send_pilot_telemetry(make_job(), make_args(full_timing()))
        self.assertIn('boom', logs.output[-1])

    def test_not_sent_when_too_large(self):
        """Nothing is sent when the size limit cannot be met."""
        with patch.object(telemetry, 'PILOT_ATTRIBUTES_MAX_BYTES', 10), \
                self.assertLogs(telemetry.logger, level='INFO'):
            telemetry.send_pilot_telemetry(make_job(), make_args(full_timing()))
        self.send.assert_not_called()

    def test_names_come_from_constants(self):
        """Changing the endpoint and key constants changes the request accordingly."""
        with patch.object(telemetry, 'PILOT_ATTRIBUTES_ENDPOINT', 'api/v1/pilot/some_other_name'), \
                patch.object(telemetry, 'PILOT_ATTRIBUTES_KEY', 'other_key'), \
                self.assertLogs(telemetry.logger, level='INFO') as logs:
            telemetry.send_pilot_telemetry(make_job(), make_args(full_timing()))
        self.assertIn('other_key', logged_body(logs.output))
        self.assertIn('api/v1/pilot/some_other_name', logs.output[-1])

    def test_size_limit_excludes_job_id_and_version(self):
        """A dictionary exactly at the limit is sent, although the whole body is larger."""
        job, args = make_job(), make_args(full_timing())
        size = telemetry.attributes_size(telemetry.get_pilot_telemetry(job, args))
        with patch.object(telemetry, 'PILOT_ATTRIBUTES_MAX_BYTES', size), \
                self.assertLogs(telemetry.logger, level='INFO') as logs:
            telemetry.send_pilot_telemetry(job, args)
        self.assertNotIn('truncated', logged_body(logs.output)[PILOT_ATTRIBUTES_KEY])

    def test_size_limit_applied(self):
        """The configured limit is applied before sending."""
        with patch.object(telemetry, 'PILOT_ATTRIBUTES_MAX_BYTES', 10), \
                self.assertLogs(telemetry.logger, level='INFO') as logs:
            telemetry.send_pilot_telemetry(make_job(), make_args(full_timing()))
        self.assertFalse(any('ready to be sent' in entry for entry in logs.output))

    def test_assembly_exception_is_swallowed(self):
        """An exception while assembling is logged as a warning."""
        with patch.object(telemetry, 'get_pilot_telemetry', side_effect=RuntimeError('assembly')), \
                self.assertLogs(telemetry.logger, level='WARNING') as logs:
            telemetry.send_pilot_telemetry(make_job(), make_args(full_timing()))
        self.assertIn('assembly', logs.output[-1])


class TestHandlePilotTelemetry(TelemetryTestCase):
    """The telemetry is only sent when all conditions hold."""

    def _handle(self, job: SimpleNamespace, args: SimpleNamespace, accepted: bool) -> MagicMock:
        """Call handle_pilot_telemetry() with the sender mocked.

        Args:
            job: Job object.
            args: Arguments object.
            accepted: Whether the final update was accepted.

        Returns:
            The mocked sender.
        """
        with patch.object(telemetry, 'send_pilot_telemetry') as send:
            telemetry.handle_pilot_telemetry(job, args, accepted)
        return send

    def test_all_conditions(self):
        """Server updated by the pilot and final update accepted: sent, with no opt-in needed."""
        job, args = make_job(completed=True), make_args()
        self._handle(job, args, True).assert_called_once_with(job, args)

    def test_no_update_server(self):
        """Not sent when the pilot does not update the server itself."""
        with self.assertLogs(telemetry.logger, level='DEBUG') as logs:
            send = self._handle(make_job(completed=True), make_args(update_server=False), True)
        send.assert_not_called()
        self.assertIn('does not update the server', logs.output[0])
        self._handle(make_job(completed=True), SimpleNamespace(), True).assert_not_called()

    def test_update_not_accepted(self):
        """Not sent when the job update was rejected, even if the job was marked completed."""
        self._handle(make_job(completed=True), make_args(), False).assert_not_called()

    def test_not_completed(self):
        """Not sent when the update was accepted but was not the final one."""
        self._handle(make_job(completed=False), make_args(), True).assert_not_called()
        self._handle(SimpleNamespace(jobid=JOB_ID), make_args(), True).assert_not_called()


class TestUpdateServerHook(TelemetryTestCase):
    """Integration of the telemetry with the final job update in update_server()."""

    def setUp(self):
        """Use the generic user plugin and a clean job/args pair."""
        super().setUp()
        self.env = patch.dict(os.environ, {'PILOT_USER': 'generic'}, clear=False)
        self.env.start()
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Restore the environment."""
        self.env.stop()
        shutil.rmtree(self.workdir, ignore_errors=True)
        super().tearDown()

    def _run(self, job: SimpleNamespace, args: SimpleNamespace, accepted: bool = True,
             completes: bool = True) -> tuple:
        """Run update_server() with send_state() and the sender mocked.

        Args:
            job: Job object.
            args: Arguments object.
            accepted: Return value of send_state().
            completes: Whether send_state() marks the job as completed.

        Returns:
            (send_state mock, send_pilot_telemetry mock).
        """
        def fake_send_state(_job, _args, _state, **_kwargs):
            if completes:
                _job.completed = True
            return accepted

        with patch.object(job_module, 'send_state', side_effect=fake_send_state) as send_state, \
                patch.object(telemetry, 'send_pilot_telemetry') as send:
            job_module.update_server(job, args)
        return send_state, send

    def _job(self, state: str = 'finished', **kwargs) -> SimpleNamespace:
        """Return a job object suitable for update_server().

        Args:
            state: Job state.
            **kwargs: Attributes to add or override.

        Returns:
            Job object.
        """
        return make_job(state=state, workdir=self.workdir, fileinfo={}, **kwargs)

    def test_sent_for_each_final_state(self):
        """Sent once for finished, failed and holding jobs."""
        for state in ('finished', 'failed', 'holding'):
            with self.subTest(state=state):
                job, args = self._job(state), make_args()
                _, send = self._run(job, args)
                send.assert_called_once_with(job, args)

    def test_final_update_timestamps_recorded(self):
        """The final update is timed around send_state()."""
        args = make_args()
        with patch.object(job_module.time, 'time', side_effect=[BASE + 1, BASE + 3]):
            self._run(self._job(), args)
        self.assertEqual(args.timing[JOB_ID][PILOT_PRE_FINAL_UPDATE], BASE + 1)
        self.assertEqual(args.timing[JOB_ID][PILOT_POST_FINAL_UPDATE], BASE + 3)

    def test_timestamps_recorded_without_telemetry(self):
        """The final update is timed even when the telemetry is not sent."""
        args = make_args()
        _, send = self._run(self._job(), args, accepted=False, completes=False)
        send.assert_not_called()
        self.assertIn(PILOT_PRE_FINAL_UPDATE, args.timing[JOB_ID])
        self.assertIn(PILOT_POST_FINAL_UPDATE, args.timing[JOB_ID])

    def test_not_sent_when_rejected(self):
        """Not sent when the final update was rejected."""
        _, send = self._run(self._job(), make_args(), accepted=False, completes=False)
        send.assert_not_called()

    def test_not_sent_when_rejected_but_completed(self):
        """Not sent when the job was marked completed although the update was rejected."""
        _, send = self._run(self._job(), make_args(), accepted=False, completes=True)
        send.assert_not_called()

    def test_not_sent_for_heartbeat(self):
        """Not sent when the update was accepted but did not complete the job."""
        _, send = self._run(self._job(), make_args(), accepted=True, completes=False)
        send.assert_not_called()

    def test_not_sent_without_server_updates(self):
        """Not sent when the pilot does not update the server."""
        _, send = self._run(self._job(), make_args(update_server=False))
        send.assert_not_called()

    def test_sent_once_per_job(self):
        """A second update_server() call for a completed job neither updates nor sends again."""
        job, args = self._job(), make_args()
        self._run(job, args)
        send_state, send = self._run(job, args)
        send_state.assert_not_called()
        send.assert_not_called()

    def test_with_fileinfo(self):
        """The file info path is hooked as well."""
        job, args = self._job(), make_args()
        job.fileinfo = {'file': {'guid': 'x'}}
        send_state, send = self._run(job, args)
        self.assertIn('xml', send_state.call_args[1])
        send.assert_called_once_with(job, args)

    def test_telemetry_failure_does_not_propagate(self):
        """A failing sender does not affect update_server() or the job."""
        job, args = self._job(), make_args()

        def complete(*_args, **_kwargs):
            job.completed = True
            return True

        with patch.object(job_module, 'send_state', side_effect=complete), \
                patch.object(telemetry, 'get_pilot_telemetry', side_effect=RuntimeError('boom')), \
                self.assertLogs(telemetry.logger, level='WARNING') as logs:
            job_module.update_server(job, args)
        self.assertTrue(job.completed)
        self.assertEqual(job.state, 'finished')
        self.assertIn('boom', logs.output[-1])


class TestPostPilotSetup(TelemetryTestCase):
    """The pilot-side end of setup is kept when the post-setup time is improved later."""

    def setUp(self):
        """Create an executor with a work directory."""
        super().setUp()
        self.workdir = tempfile.mkdtemp()
        self.args = make_args()
        self.job = make_job(workdir=self.workdir)
        self.executor = generic_payload.Executor(self.args, self.job, None, None, None)

    def tearDown(self):
        """Remove the work directory."""
        shutil.rmtree(self.workdir, ignore_errors=True)
        super().tearDown()

    def test_normal_call_records_both(self):
        """A call without an explicit time records both constants with the same value."""
        with patch.object(generic_payload.time, 'time', return_value=BASE + 5):
            self.executor.post_setup(self.job)
        entry = self.args.timing[JOB_ID]
        self.assertEqual(entry[PILOT_POST_SETUP], BASE + 5)
        self.assertEqual(entry[PILOT_POST_PILOT_SETUP], BASE + 5)

    def test_explicit_time_only_updates_post_setup(self):
        """A call with an explicit time does not touch the pilot-side value."""
        with patch.object(generic_payload.time, 'time', return_value=BASE + 5):
            self.executor.post_setup(self.job)
        self.executor.post_setup(self.job, update_time=BASE + 60)
        entry = self.args.timing[JOB_ID]
        self.assertEqual(entry[PILOT_POST_SETUP], BASE + 60)
        self.assertEqual(entry[PILOT_POST_PILOT_SETUP], BASE + 5)

    def test_improve_post_setup_keeps_pilot_value(self):
        """improve_post_setup() overwrites PILOT_POST_SETUP only."""
        with open(os.path.join(self.workdir, config.Payload.payloadstdout), 'w', encoding='utf-8') as _file:
            _file.write('payload stdout\n')
        with patch.object(generic_payload.time, 'time', return_value=BASE + 5):
            self.executor.post_setup(self.job)
        with patch.dict(os.environ, {'PILOT_USER': 'generic'}, clear=False), \
                patch('pilot.user.generic.setup.get_end_setup_time', return_value=BASE + 90):
            self.executor.improve_post_setup()
        entry = self.args.timing[JOB_ID]
        self.assertEqual(entry[PILOT_POST_SETUP], BASE + 90)
        self.assertEqual(entry[PILOT_POST_PILOT_SETUP], BASE + 5)


class TestPilotTimingUnchanged(TelemetryTestCase):
    """The pilotTiming field of the job update is not affected by the new timing constants."""

    @staticmethod
    def _pilot_timing(timing: dict) -> str:
        """Return the pilotTiming string for the given timing dictionary.

        Args:
            timing: Pilot timing dictionary.

        Returns:
            pilotTiming value.
        """
        data = {}
        job_module.add_timing_and_extracts(data, make_job(), 'finished', make_args(timing))
        return data['pilot_timing']

    def test_byte_identical(self):
        """Adding the new constants does not change pilotTiming."""
        timing = full_timing()
        del timing[JOB_ID][PILOT_POST_PILOT_SETUP]
        del timing[JOB_ID][PILOT_PRE_FINAL_UPDATE]
        del timing[JOB_ID][PILOT_POST_FINAL_UPDATE]
        before = self._pilot_timing(timing)
        timing[JOB_ID][PILOT_POST_PILOT_SETUP] = BASE + 1
        timing[JOB_ID][PILOT_PRE_FINAL_UPDATE] = BASE + 2
        timing[JOB_ID][PILOT_POST_FINAL_UPDATE] = BASE + 9999
        self.assertEqual(self._pilot_timing(timing), before)
        # getjob|stagein|payload|stageout|initial setup|setup, with remote i/o moved from setup to stage-in
        self.assertEqual(before, '1|240|7000|110|11|87')


if __name__ == '__main__':
    unittest.main()
