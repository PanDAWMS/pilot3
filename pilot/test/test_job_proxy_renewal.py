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
# - Paul Nilsson, paul.nilsson@cern.ch, 2026

"""Unit tests for the verification and renewal of job proxies.

Job proxies are the proxies the pilot downloads per job in addition to its own proxy: the payload
proxy on non-unified analysis queues and the user proxy on unified dispatch queues. Previously they
were downloaded once at job start and never verified or renewed, which limited user jobs to the
lifetime of the downloaded proxy (for direct I/O inside the payload, and on unified dispatch queues
also for the final stage-out).

The tests cover:

- the release guards for the test-mode switches (must fail if a test setting is left in place)
- naming and placement of job proxies in the work directory (visible as /srv in the container)
- the arcproxy cache: one arcproxy execution per downloaded proxy, no stale entries across jobs
- the periodic check, renewal (download to a temporary file, verify, then replace), retry on
  failure, and the hard floor below which a failed renewal fails the job
- the renewal threshold, the backoff after a renewal that did not help (e.g. the server returned the
  same proxy), and the check of the minimum remaining validity guaranteed by the PanDA server (72h)
- the ALRB_CONT_PRESETUP export that lets a running payload see a renewed proxy
- removal of job proxies from the work directory before the log tarball is created
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache
from pilot.control import data
from pilot.control import job as job_module
from pilot.info.jobdata import JobData
from pilot.user.atlas import container
from pilot.user.atlas import proxy as atlas_proxy
from pilot.user.atlas.container import get_alrb_presetup, update_for_user_proxy
from pilot.util import monitoring
from pilot.util.monitoringtime import MonitoringTime
from pilot.util.proxy import get_job_proxy_candidates, get_job_proxy_path, remove_job_proxies

errors = ErrorCodes()
pilot_cache = get_pilot_cache()

NOW = 1_800_000_000
HOUR = 3600
THRESHOLD = atlas_proxy.JOB_PROXY_RENEWAL_THRESHOLD


def _clear_cache():
    """Remove the job proxy entries from the arcproxy cache, and any renewal backoff."""
    for proxy_id in ('payload', 'unified'):
        atlas_proxy.invalidate_proxy_cache(proxy_id)
        atlas_proxy.clear_renewal_backoff(proxy_id)


class _FakeQueuedata:
    """Minimal queuedata stand-in."""

    def __init__(self, queue_type: str = 'production'):
        self.type = queue_type
        self.container_type = {'pilot': 'apptainer'}


class _FakeInfosys:
    """Minimal infosys stand-in."""

    def __init__(self, queue_type: str = 'production'):
        self.queuedata = _FakeQueuedata(queue_type)


class _FakeJob:
    """Minimal job stand-in."""

    def __init__(self, workdir: str = '/tmp/PanDA_Pilot-1', state: str = 'running', is_analysis: bool = True):
        self.workdir = workdir
        self.state = state
        self._is_analysis = is_analysis
        self.infosys = _FakeInfosys()
        self.jobid = '1'

    def is_analysis(self) -> bool:
        """Return True for a user analysis job."""
        return self._is_analysis


class TestReleaseGuards(unittest.TestCase):
    """The test-mode switches must be at their release defaults."""

    def test_test_job_proxy_renewal_is_disabled(self):
        """_TEST_JOB_PROXY_RENEWAL in pilot/user/atlas/proxy.py must be False in a release."""
        self.assertIs(atlas_proxy._TEST_JOB_PROXY_RENEWAL, False)

    def test_renewal_constants_are_release_values(self):
        """The agreed release values: renew below 24h, fail below 10 min, back off 1h, expect 72h from the server."""
        self.assertEqual(atlas_proxy.JOB_PROXY_RENEWAL_THRESHOLD, 24 * HOUR)
        self.assertEqual(atlas_proxy.JOB_PROXY_HARD_FLOOR, 600)
        self.assertEqual(atlas_proxy.JOB_PROXY_RENEWAL_BACKOFF, HOUR)
        self.assertEqual(atlas_proxy.JOB_PROXY_EXPECTED_LIFETIME, 72 * HOUR)
        self.assertEqual(atlas_proxy.JOB_PROXY_LIFETIME_GRACE, 20 * 60)
        self.assertEqual(atlas_proxy.TEST_JOB_PROXY_MARGIN, 300)

    def test_proxy_verification_time_is_default(self):
        """proxy_verification_time in pilot/util/default.cfg must be 600 in a release."""
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'util', 'default.cfg')
        with open(path, encoding='utf-8') as cfg:
            lines = [line.strip() for line in cfg if line.strip().startswith('proxy_verification_time')]
        self.assertEqual(lines, ['proxy_verification_time: 600'])


class TestGetJobProxyPath(unittest.TestCase):
    """get_job_proxy_path() must always produce a '.proxy' name, in the work directory if given."""

    def test_pilot_proxy_without_suffix(self):
        """The name must end with .proxy even if the pilot proxy name does not."""
        self.assertEqual(get_job_proxy_path('/tmp/x509up_u1', 'unified', workdir='/w'), '/w/x509up_u1-unified.proxy')

    def test_pilot_proxy_with_suffix(self):
        """The .proxy suffix of the pilot proxy must not be duplicated."""
        self.assertEqual(get_job_proxy_path('/tmp/x509up_u1.proxy', 'payload', workdir='/w'), '/w/x509up_u1-payload.proxy')

    def test_only_exact_suffix_is_stripped(self):
        """A name merely ending with 'proxy' (no dot) must be kept intact."""
        self.assertEqual(get_job_proxy_path('/tmp/myproxy', 'payload', workdir='/w'), '/w/myproxy-payload.proxy')

    def test_without_workdir(self):
        """Without a work directory, the proxy is placed next to the pilot's own proxy."""
        self.assertEqual(get_job_proxy_path('/tmp/x509up_u1', 'payload'), '/tmp/x509up_u1-payload.proxy')


class TestRemoveJobProxies(unittest.TestCase):
    """remove_job_proxies() must remove every job proxy from the work directory, and nothing else."""

    def setUp(self):
        """Create a work directory and a pilot proxy outside of it."""
        self.base = tempfile.mkdtemp()
        self.workdir = os.path.join(self.base, 'PanDA_Pilot-1')
        os.mkdir(self.workdir)
        self.pilot_proxy = os.path.join(self.base, 'x509up_u1')
        self._touch(self.pilot_proxy)
        pilot_cache.payload_proxy = None

    def tearDown(self):
        """Remove the directories and reset the cache."""
        shutil.rmtree(self.base, ignore_errors=True)
        pilot_cache.payload_proxy = None

    @staticmethod
    def _touch(path):
        with open(path, 'w', encoding='utf-8') as _file:
            _file.write('proxy')

    def test_known_proxies_are_removed_and_state_reset(self):
        """The unified and payload proxies known from env/cache are removed, and env/cache reset."""
        unified = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        payload = os.path.join(self.workdir, 'x509up_u1-payload.proxy')
        for path in (unified, payload):
            self._touch(path)
        pilot_cache.payload_proxy = payload
        env = {'X509_USER_PROXY': self.pilot_proxy, 'X509_UNIFIED_DISPATCH': unified}
        with patch.dict('os.environ', env, clear=False):
            removed = remove_job_proxies(self.workdir)
            self.assertEqual(os.environ['X509_UNIFIED_DISPATCH'], '')
        self.assertEqual(sorted(removed), sorted([unified, payload]))
        self.assertFalse(os.path.exists(unified))
        self.assertFalse(os.path.exists(payload))
        self.assertIsNone(pilot_cache.payload_proxy)

    def test_leftover_proxy_removed_after_env_was_reset(self):
        """A proxy recreated after the stage-out reset the env must still be found by its generated name."""
        unified = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        tmp = unified + '.tmp'
        self._touch(unified)
        self._touch(tmp)
        env = {'X509_USER_PROXY': self.pilot_proxy, 'X509_UNIFIED_DISPATCH': ''}
        with patch.dict('os.environ', env, clear=False):
            removed = remove_job_proxies(self.workdir)
        self.assertEqual(sorted(removed), sorted([unified, tmp]))

    def test_other_files_and_pilot_proxy_untouched(self):
        """Unrelated files, and anything outside the work directory, must never be removed."""
        other = os.path.join(self.workdir, 'payload.stdout')
        outside = os.path.join(self.base, 'x509up_u1-payload.proxy')
        self._touch(other)
        self._touch(outside)
        pilot_cache.payload_proxy = outside
        env = {'X509_USER_PROXY': self.pilot_proxy, 'X509_UNIFIED_DISPATCH': ''}
        with patch.dict('os.environ', env, clear=False):
            removed = remove_job_proxies(self.workdir)
        self.assertEqual(removed, [])
        for path in (other, outside, self.pilot_proxy):
            self.assertTrue(os.path.exists(path))

    def test_cached_payload_proxy_removed_even_if_name_differs(self):
        """The cached payload proxy is removed even when it cannot be derived from X509_USER_PROXY."""
        payload = os.path.join(self.workdir, 'renamed-payload.proxy')
        self._touch(payload)
        pilot_cache.payload_proxy = payload
        env = {'X509_USER_PROXY': '', 'X509_UNIFIED_DISPATCH': ''}
        with patch.dict('os.environ', env, clear=False):
            self.assertEqual(remove_job_proxies(self.workdir), [payload])
        self.assertFalse(os.path.exists(payload))

    def test_candidates_are_unique(self):
        """A proxy known both from the env and by its generated name is listed once."""
        unified = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        env = {'X509_USER_PROXY': self.pilot_proxy, 'X509_UNIFIED_DISPATCH': unified}
        with patch.dict('os.environ', env, clear=False):
            candidates = get_job_proxy_candidates(self.workdir)
        self.assertEqual(candidates.count(unified), 1)
        self.assertIn(unified + '.tmp', candidates)
        self.assertIn(os.path.join(self.workdir, 'x509up_u1-payload.proxy'), candidates)

    def test_no_pilot_proxy_set(self):
        """Without X509_USER_PROXY, only the known proxies are candidates."""
        env = {'X509_USER_PROXY': '', 'X509_UNIFIED_DISPATCH': ''}
        with patch.dict('os.environ', env, clear=False):
            self.assertEqual(get_job_proxy_candidates(self.workdir), [])


class TestCreateLogRemovesProxies(unittest.TestCase):
    """create_log() must remove the job proxies before anything is archived."""

    def test_proxy_removed_before_tarball(self):
        """The unified proxy must be gone by the time create_log() gets to the tarball."""
        base = tempfile.mkdtemp()
        try:
            workdir = os.path.join(base, 'PanDA_Pilot-1')
            os.mkdir(workdir)
            unified = os.path.join(workdir, 'x509up_u1-unified')
            with open(unified, 'w', encoding='utf-8') as _file:
                _file.write('proxy')
            env = {'PILOT_HOME': os.getcwd(), 'X509_USER_PROXY': os.path.join(base, 'x509up_u1'),
                   'X509_UNIFIED_DISPATCH': unified}
            with patch.dict('os.environ', env, clear=False), \
                 patch.object(data, 'copy_special_files'):
                # an empty logfile name returns just before the tarball would be created
                data.create_log(workdir, '', 'tarball', False)
            self.assertFalse(os.path.exists(unified))
        finally:
            shutil.rmtree(base, ignore_errors=True)


class TestArcproxyCache(unittest.TestCase):
    """Cache helpers and the one-arcproxy-per-download behaviour."""

    def setUp(self):
        """Start from an empty job proxy cache."""
        _clear_cache()

    def tearDown(self):
        """Leave an empty job proxy cache."""
        _clear_cache()

    def test_cached_validity_end(self):
        """The earliest end counts; failures and missing entries are distinguished."""
        self.assertIsNone(atlas_proxy.get_cached_validity_end('payload'))
        atlas_proxy.set_cache_entry('payload', [NOW + 20, NOW + 10])
        self.assertEqual(atlas_proxy.get_cached_validity_end('payload'), NOW + 10)
        atlas_proxy.set_cache_entry('payload', [NOW + 10, NOW + 20])
        self.assertEqual(atlas_proxy.get_cached_validity_end('payload'), NOW + 10)
        atlas_proxy.set_cache_entry('payload', [None, NOW + 20])
        self.assertEqual(atlas_proxy.get_cached_validity_end('payload'), NOW + 20)
        atlas_proxy.set_cache_entry('payload', [None, None])
        self.assertIsNone(atlas_proxy.get_cached_validity_end('payload'))
        atlas_proxy.set_cache_entry('payload', [-1, -1])
        self.assertEqual(atlas_proxy.get_cached_validity_end('payload'), -1)
        atlas_proxy.invalidate_proxy_cache('payload')
        self.assertIsNone(atlas_proxy.get_cache_entry('payload'))

    def test_set_cache_entry_creates_cache(self):
        """set_cache_entry() must work before verify_arcproxy() has ever created its cache."""
        saved = atlas_proxy.verify_arcproxy.__dict__.pop('cache', None)
        try:
            atlas_proxy.set_cache_entry('unified', [1, 2])
            self.assertEqual(atlas_proxy.verify_arcproxy.cache, {'unified': [1, 2]})
        finally:
            if saved is not None:
                atlas_proxy.verify_arcproxy.cache = saved

    def test_download_clears_stale_entry_and_caches_under_proxy_type(self):
        """A new download must not be judged by a stale entry, and must be cached for later checks."""
        atlas_proxy.set_cache_entry('payload', [NOW - 5, NOW - 5])  # entry from a previous job
        seen = {}

        def fake_verify(proxy_id=None, **_kwargs):
            seen['entry_at_verify'] = atlas_proxy.get_cache_entry(proxy_id)
            seen['proxy_id'] = proxy_id
            return 0, ''

        with patch('pilot.user.atlas.proxy.get_proxy', side_effect=lambda path, role: (True, path)), \
             patch('pilot.user.atlas.proxy.verify_proxy', side_effect=fake_verify):
            exit_code, _, path = atlas_proxy.get_and_verify_proxy('/tmp/x509up_u1', voms_role='atlas',
                                                                  proxy_type='payload', workdir='/w')
        self.assertEqual(exit_code, 0)
        self.assertEqual(path, '/w/x509up_u1-payload.proxy')
        self.assertEqual(seen, {'entry_at_verify': None, 'proxy_id': 'payload'})

    def test_pilot_proxy_download_is_not_cached_as_job_proxy(self):
        """A download without proxy type (pilot proxy renewal) keeps using no cache id."""
        with patch('pilot.user.atlas.proxy.get_proxy', side_effect=lambda path, role: (True, path)), \
             patch('pilot.user.atlas.proxy.verify_proxy', return_value=(0, '')) as mock_verify:
            _, _, path = atlas_proxy.get_and_verify_proxy('/tmp/x509up_u1', voms_role='atlas')
        self.assertEqual(path, '/tmp/x509up_u1')
        self.assertIsNone(mock_verify.call_args.kwargs['proxy_id'])

    def test_handle_payload_proxy_uses_workdir_and_resets_stale_path(self):
        """The payload proxy goes to the work directory; a stale path never survives a new job."""
        pilot_cache.payload_proxy = '/old/x509up_u1-payload.proxy'
        with patch('pilot.user.atlas.proxy.requires_payload_proxy', return_value=False):
            self.assertEqual(atlas_proxy.handle_payload_proxy(_FakeJob()), (0, ''))
        self.assertIsNone(pilot_cache.payload_proxy)

        env = {'X509_USER_PROXY': '/tmp/x509up_u1', 'X509_UNIFIED_DISPATCH': ''}
        with patch.dict('os.environ', env, clear=False), \
             patch('pilot.user.atlas.proxy.requires_payload_proxy', return_value=True), \
             patch('pilot.user.atlas.proxy.get_and_verify_proxy',
                   return_value=(0, '', '/w/x509up_u1-payload.proxy')) as mock_get:
            atlas_proxy.handle_payload_proxy(_FakeJob(workdir='/w'))
        self.assertEqual(mock_get.call_args.kwargs['workdir'], '/w')
        self.assertEqual(pilot_cache.payload_proxy, '/w/x509up_u1-payload.proxy')
        pilot_cache.payload_proxy = None


class TestTestLifetime(unittest.TestCase):
    """apply_test_lifetime() and its use in verify_arcproxy()."""

    def setUp(self):
        """Start from an empty job proxy cache."""
        _clear_cache()

    def tearDown(self):
        """Leave an empty job proxy cache."""
        _clear_cache()
        pilot_cache.proxy_validity_end = 0

    def test_disabled_by_default(self):
        """With the switch off, nothing is clamped."""
        with patch('pilot.user.atlas.proxy.time', return_value=NOW):
            self.assertEqual(atlas_proxy.apply_test_lifetime('payload', NOW + 96 * HOUR, NOW + 96 * HOUR),
                             (NOW + 96 * HOUR, NOW + 96 * HOUR))

    def test_test_lifetime_is_relative_to_threshold(self):
        """The clamp is the renewal threshold plus the margin, so a renewal follows each download after the margin."""
        self.assertEqual(atlas_proxy.get_test_lifetime(), THRESHOLD + atlas_proxy.TEST_JOB_PROXY_MARGIN)
        with patch.object(atlas_proxy, 'JOB_PROXY_RENEWAL_THRESHOLD', 12 * HOUR):
            self.assertEqual(atlas_proxy.get_test_lifetime(), 12 * HOUR + atlas_proxy.TEST_JOB_PROXY_MARGIN)

    def test_clamps_job_proxies_only(self):
        """Job proxies are clamped, the pilot proxy never, and a shorter real validity is kept."""
        clamp = NOW + atlas_proxy.get_test_lifetime()
        with patch.object(atlas_proxy, '_TEST_JOB_PROXY_RENEWAL', True), \
             patch('pilot.user.atlas.proxy.time', return_value=NOW):
            self.assertEqual(atlas_proxy.apply_test_lifetime('unified', NOW + 96 * HOUR, NOW + 96 * HOUR),
                             (clamp, clamp))
            self.assertEqual(atlas_proxy.apply_test_lifetime('payload', NOW + 100, None), (NOW + 100, None))
            self.assertEqual(atlas_proxy.apply_test_lifetime('payload', None, NOW + 96 * HOUR), (None, clamp))
            self.assertEqual(atlas_proxy.apply_test_lifetime('pilot', NOW + 96 * HOUR, NOW + 96 * HOUR),
                             (NOW + 96 * HOUR, NOW + 96 * HOUR))
            self.assertEqual(atlas_proxy.apply_test_lifetime(None, NOW + 96 * HOUR, NOW + 96 * HOUR),
                             (NOW + 96 * HOUR, NOW + 96 * HOUR))

    def test_verify_arcproxy_caches_clamped_validity(self):
        """The clamp must reach the cache (so the periodic check sees it) but not the pilot's validity."""
        end = NOW + 96 * HOUR
        clamp = NOW + atlas_proxy.get_test_lifetime()
        with patch.object(atlas_proxy, '_TEST_JOB_PROXY_RENEWAL', True), \
             patch('pilot.user.atlas.proxy.time', return_value=NOW), \
             patch('pilot.user.atlas.proxy.execute_nothreads', return_value=(0, 'a\nb', '')), \
             patch('pilot.user.atlas.proxy.interpret_proxy_info', return_value=(0, '', end, end)):
            exit_code, _ = atlas_proxy.verify_arcproxy('', 1, proxy_id='payload')
            self.assertEqual(exit_code, 0)
            self.assertEqual(atlas_proxy.get_cache_entry('payload'), [clamp, clamp])
            atlas_proxy.verify_arcproxy('', 1, proxy_id='pilot')
            self.assertEqual(pilot_cache.proxy_validity_end, end)
            self.assertEqual(atlas_proxy.get_cache_entry('pilot'), [end, end])
        atlas_proxy.invalidate_proxy_cache('pilot')


class TestGetJobProxies(unittest.TestCase):
    """get_job_proxies() and is_current_job_proxy()."""

    def setUp(self):
        """Create two proxy files."""
        self.base = tempfile.mkdtemp()
        self.unified = os.path.join(self.base, 'u-unified.proxy')
        self.payload = os.path.join(self.base, 'u-payload.proxy')
        for path in (self.unified, self.payload):
            with open(path, 'w', encoding='utf-8') as _file:
                _file.write('proxy')

    def tearDown(self):
        """Remove the files and reset the cache."""
        shutil.rmtree(self.base, ignore_errors=True)
        pilot_cache.payload_proxy = None

    def test_existing_proxies_are_returned(self):
        """Both proxies are returned when set and present."""
        pilot_cache.payload_proxy = self.payload
        with patch.dict('os.environ', {'X509_UNIFIED_DISPATCH': self.unified}, clear=False):
            self.assertEqual(atlas_proxy.get_job_proxies(), [('unified', self.unified), ('payload', self.payload)])

    def test_missing_or_unset_proxies_are_skipped(self):
        """A removed proxy (e.g. after stage-out) or an unset one is not returned."""
        pilot_cache.payload_proxy = os.path.join(self.base, 'gone.proxy')
        with patch.dict('os.environ', {'X509_UNIFIED_DISPATCH': os.path.join(self.base, 'gone2.proxy')}, clear=False):
            self.assertEqual(atlas_proxy.get_job_proxies(), [])
        pilot_cache.payload_proxy = None
        with patch.dict('os.environ', {'X509_UNIFIED_DISPATCH': ''}, clear=False):
            self.assertEqual(atlas_proxy.get_job_proxies(), [])

    def test_is_current_job_proxy(self):
        """The path must match the env (unified) or the cache (payload)."""
        pilot_cache.payload_proxy = self.payload
        with patch.dict('os.environ', {'X509_UNIFIED_DISPATCH': self.unified}, clear=False):
            self.assertTrue(atlas_proxy.is_current_job_proxy('unified', self.unified))
            self.assertFalse(atlas_proxy.is_current_job_proxy('unified', self.payload))
            self.assertTrue(atlas_proxy.is_current_job_proxy('payload', self.payload))
            self.assertFalse(atlas_proxy.is_current_job_proxy('payload', self.unified))


class TestVerifyJobProxies(unittest.TestCase):
    """verify_job_proxies() dispatches to check_job_proxy() for the requested proxies."""

    def test_no_proxies(self):
        """Nothing to do without job proxies."""
        with patch('pilot.user.atlas.proxy.get_job_proxies', return_value=[]), \
             patch('pilot.user.atlas.proxy.check_job_proxy') as mock_check:
            self.assertEqual(atlas_proxy.verify_job_proxies(), (0, ''))
        mock_check.assert_not_called()

    def test_filter_and_first_failure(self):
        """Only the requested proxies are checked; the first failure is returned."""
        proxies = [('unified', '/w/u'), ('payload', '/w/p')]
        with patch('pilot.user.atlas.proxy.get_job_proxies', return_value=proxies), \
             patch('pilot.user.atlas.proxy.check_job_proxy', return_value=(0, '')) as mock_check:
            self.assertEqual(atlas_proxy.verify_job_proxies(proxy_ids=('unified',)), (0, ''))
        mock_check.assert_called_once_with('unified', '/w/u')

        failure = (errors.PAYLOADPROXYDOWNLOADFAILURE, 'expired')
        with patch('pilot.user.atlas.proxy.get_job_proxies', return_value=proxies), \
             patch('pilot.user.atlas.proxy.check_job_proxy', side_effect=[failure, (0, '')]) as mock_check:
            self.assertEqual(atlas_proxy.verify_job_proxies(), failure)
        self.assertEqual(mock_check.call_count, 1)

        with patch('pilot.user.atlas.proxy.get_job_proxies', return_value=proxies), \
             patch('pilot.user.atlas.proxy.check_job_proxy', side_effect=[(0, ''), failure]) as mock_check:
            self.assertEqual(atlas_proxy.verify_job_proxies(), failure)
        self.assertEqual(mock_check.call_count, 2)


class TestCheckJobProxy(unittest.TestCase):
    """check_job_proxy(): cache-served checks, renewal, retry and hard floor."""

    def setUp(self):
        """Start from an empty job proxy cache at a fixed time."""
        _clear_cache()
        self.time_patch = patch('pilot.user.atlas.proxy.time', return_value=NOW)
        self.time_patch.start()

    def tearDown(self):
        """Stop the time patch and clear the cache."""
        self.time_patch.stop()
        _clear_cache()

    def _check(self, seconds_left, renew_result=(0, '')):
        atlas_proxy.set_cache_entry('payload', [NOW + 96 * HOUR, NOW + seconds_left])
        with patch('pilot.user.atlas.proxy.renew_job_proxy', return_value=renew_result) as mock_renew, \
             patch('pilot.user.atlas.proxy.verify_proxy') as mock_verify:
            result = atlas_proxy.check_job_proxy('payload', '/w/p')
        mock_verify.assert_not_called()  # served from the cache, arcproxy is never executed here
        return result, mock_renew

    def test_valid_proxy_is_not_renewed(self):
        """Above the threshold (inclusive), nothing happens."""
        for seconds_left in (THRESHOLD, 96 * HOUR):
            result, mock_renew = self._check(seconds_left)
            self.assertEqual(result, (0, ''))
            mock_renew.assert_not_called()

    def test_expiring_proxy_is_renewed(self):
        """Below the threshold, the proxy is renewed."""
        result, mock_renew = self._check(THRESHOLD - 1)
        self.assertEqual(result, (0, ''))
        mock_renew.assert_called_once_with('payload', '/w/p')

    def test_failed_renewal_is_retried_above_floor(self):
        """A failed renewal does not fail the job while the proxy is still usable (floor inclusive)."""
        failure = (errors.PAYLOADPROXYDOWNLOADFAILURE, 'server down')
        for seconds_left in (THRESHOLD - 1, atlas_proxy.JOB_PROXY_HARD_FLOOR):
            result, _ = self._check(seconds_left, renew_result=failure)
            self.assertEqual(result, (0, ''))

    def test_failed_renewal_below_floor_fails_the_job(self):
        """Below the hard floor, a failed renewal is fatal."""
        failure = (errors.PAYLOADPROXYDOWNLOADFAILURE, 'server down')
        result, _ = self._check(atlas_proxy.JOB_PROXY_HARD_FLOOR - 1, renew_result=failure)
        self.assertEqual(result[0], errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertIn('server down', result[1])
        self.assertIn('payload', result[1])

    def test_unknown_validity_is_remembered(self):
        """Without a cached validity, warn once, mark it, and never execute arcproxy or renew."""
        with patch('pilot.user.atlas.proxy.renew_job_proxy') as mock_renew, \
             patch('pilot.user.atlas.proxy.verify_proxy') as mock_verify, \
             patch.object(atlas_proxy.logger, 'warning') as mock_warning:
            self.assertEqual(atlas_proxy.check_job_proxy('payload', '/w/p'), (0, ''))
            self.assertEqual(atlas_proxy.get_cache_entry('payload'), [-1, -1])
            self.assertEqual(atlas_proxy.check_job_proxy('payload', '/w/p'), (0, ''))
        self.assertEqual(mock_warning.call_count, 1)
        mock_renew.assert_not_called()
        mock_verify.assert_not_called()


class TestRenewJobProxy(unittest.TestCase):
    """renew_job_proxy() / _renew_job_proxy(): safe replacement of the proxy in use."""

    OLD = 'old-proxy'
    NEW = 'new-proxy'

    def setUp(self):
        """Create a work directory with a proxy in use."""
        _clear_cache()
        self.base = tempfile.mkdtemp()
        self.path = os.path.join(self.base, 'x509up_u1-payload.proxy')
        with open(self.path, 'w', encoding='utf-8') as _file:
            _file.write(self.OLD)
        pilot_cache.payload_proxy = self.path
        self.old_entry = [NOW + 100, NOW + 100]
        atlas_proxy.set_cache_entry('payload', self.old_entry)
        self.time_patch = patch('pilot.user.atlas.proxy.time', return_value=NOW)
        self.time_patch.start()

    def tearDown(self):
        """Clean up."""
        self.time_patch.stop()
        shutil.rmtree(self.base, ignore_errors=True)
        pilot_cache.payload_proxy = None
        _clear_cache()

    def _read(self):
        with open(self.path, encoding='utf-8') as _file:
            return _file.read()

    def _fake_get_proxy(self, success=True, redirect=''):
        def fake(path, _role):
            target = redirect or path
            with open(target, 'w', encoding='utf-8') as _file:
                _file.write(self.NEW)
            if redirect:
                os.environ['X509_USER_PROXY'] = redirect
            return success, target
        return fake

    @staticmethod
    def _fake_verify(entry, result=(0, '')):
        def fake(proxy_id=None, **_kwargs):
            if entry:
                atlas_proxy.set_cache_entry(proxy_id, entry)
            return result
        return fake

    def _renew(self, get_proxy, verify):
        with patch('pilot.user.atlas.proxy.get_proxy', side_effect=get_proxy) as mock_get, \
             patch('pilot.user.atlas.proxy.verify_proxy', side_effect=verify) as mock_verify:
            result = atlas_proxy._renew_job_proxy('payload', self.path)
        return result, mock_get, mock_verify

    def _assert_unchanged(self, result):
        self.assertEqual(result[0], errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertTrue(result[1])
        self.assertEqual(self._read(), self.OLD)
        self.assertFalse(os.path.exists(self.path + '.tmp'))
        self.assertEqual(atlas_proxy.get_cache_entry('payload'), self.old_entry)

    def test_success_replaces_proxy(self):
        """The verified new proxy replaces the old one; the user role is requested."""
        new_entry = [NOW + 96 * HOUR, NOW + 96 * HOUR]
        result, mock_get, mock_verify = self._renew(self._fake_get_proxy(), self._fake_verify(new_entry))
        self.assertEqual(result, (0, ''))
        self.assertEqual(self._read(), self.NEW)
        self.assertFalse(os.path.exists(self.path + '.tmp'))
        self.assertEqual(atlas_proxy.get_cache_entry('payload'), new_entry)
        self.assertEqual(mock_get.call_args.args, (self.path + '.tmp', 'atlas'))
        self.assertEqual(mock_verify.call_args.kwargs, {'x509': self.path + '.tmp', 'proxy_id': 'payload'})

    def test_verification_starts_from_empty_cache(self):
        """The old cache entry must be cleared before the new proxy is verified."""
        seen = []

        def verify(proxy_id=None, **_kwargs):
            seen.append(atlas_proxy.get_cache_entry(proxy_id))
            return 0, ''

        self._renew(self._fake_get_proxy(), verify)
        self.assertEqual(seen, [None])

    def test_download_failure_keeps_old_proxy(self):
        """A failed download leaves the proxy in use and its cache entry alone."""
        result, _, mock_verify = self._renew(self._fake_get_proxy(success=False), self._fake_verify(None))
        self._assert_unchanged(result)
        mock_verify.assert_not_called()

    def test_verification_failure_keeps_old_proxy(self):
        """A new proxy failing verification (non-zero code or diagnostics) is discarded."""
        for verify_result in ((errors.NOVOMSPROXY, 'bad'), (0, 'all verifications failed'), (-1, 'no arcproxy')):
            with self.subTest(verify_result=verify_result):
                result, _, _ = self._renew(self._fake_get_proxy(), self._fake_verify([-1, -1], verify_result))
                self._assert_unchanged(result)

    def test_diagnostics_only_failure_with_long_validity_is_discarded(self):
        """A verification failing only through its diagnostics is a failure even if the cache claims a long validity."""
        long_entry = [NOW + 96 * HOUR, NOW + 96 * HOUR]
        result, _, _ = self._renew(self._fake_get_proxy(), self._fake_verify(long_entry, (0, 'all verifications failed')))
        self._assert_unchanged(result)
        self.assertIn('failed verification', result[1])

    def test_read_only_fallback_is_undone(self):
        """If get_proxy() wrote elsewhere and redirected X509_USER_PROXY, that must be undone."""
        stray = os.path.join(self.base, 'stray.proxy')
        with patch.dict('os.environ', {'X509_USER_PROXY': '/tmp/x509up_u1'}, clear=False):
            result, _, mock_verify = self._renew(self._fake_get_proxy(redirect=stray), self._fake_verify(None))
            self.assertEqual(os.environ['X509_USER_PROXY'], '/tmp/x509up_u1')
        self._assert_unchanged(result)
        self.assertFalse(os.path.exists(stray))
        mock_verify.assert_not_called()

    def test_replace_failure_keeps_old_proxy(self):
        """If the new proxy cannot be moved into place, the old one stays in use."""
        with patch('pilot.user.atlas.proxy.os.replace', side_effect=OSError('busy')):
            result, _, _ = self._renew(self._fake_get_proxy(), self._fake_verify([NOW + HOUR, NOW + HOUR]))
        self._assert_unchanged(result)
        self.assertIn('busy', result[1])

    def test_proxy_no_longer_in_use_is_removed(self):
        """If the job moved on meanwhile (stage-out removed the proxy), the renewed file is removed."""

        def get_proxy(path, role):
            pilot_cache.payload_proxy = None  # reset by the stage-out while downloading
            return self._fake_get_proxy()(path, role)

        result, _, _ = self._renew(get_proxy, self._fake_verify([NOW + HOUR, NOW + HOUR]))
        self.assertEqual(result, (0, ''))
        self.assertFalse(os.path.exists(self.path))
        self.assertIsNone(atlas_proxy.get_cache_entry('payload'))

    def test_renewal_holds_lock_and_skips_if_already_renewed(self):
        """Only one renewal at a time; a proxy renewed by another thread is left alone."""
        states = []
        with patch('pilot.user.atlas.proxy._renew_job_proxy',
                   side_effect=lambda proxy_id, path: states.append(atlas_proxy._renewal_lock.locked()) or (0, '')):
            self.assertEqual(atlas_proxy.renew_job_proxy('payload', self.path), (0, ''))
            self.assertEqual(states, [True])
            self.assertFalse(atlas_proxy._renewal_lock.locked())

            for entry in ([NOW + THRESHOLD, NOW + THRESHOLD], [NOW + 96 * HOUR, NOW + 96 * HOUR]):
                atlas_proxy.set_cache_entry('payload', entry)
                self.assertEqual(atlas_proxy.renew_job_proxy('payload', self.path), (0, ''))
            self.assertEqual(states, [True])  # not called again

            # a failed or missing entry means "renew" - without judging a bogus validity of -1
            with patch('pilot.user.atlas.proxy.check_time_left') as mock_check:
                for entry in ([-1, -1], None):
                    atlas_proxy.set_cache_entry('payload', entry)
                    atlas_proxy.renew_job_proxy('payload', self.path)
            mock_check.assert_not_called()
            self.assertEqual(len(states), 3)


class TestExpectedLifetime(unittest.TestCase):
    """check_downloaded_lifetime(): the server guarantees at least 72h; a shorter proxy is reported but used."""

    EXPECTED = atlas_proxy.JOB_PROXY_EXPECTED_LIFETIME - atlas_proxy.JOB_PROXY_LIFETIME_GRACE

    def setUp(self):
        """Start from an empty job proxy cache at a fixed time."""
        _clear_cache()
        self.time_patch = patch('pilot.user.atlas.proxy.time', return_value=NOW)
        self.time_patch.start()

    def tearDown(self):
        """Stop the time patch and clear the cache."""
        self.time_patch.stop()
        _clear_cache()

    def test_boundary(self):
        """At the expected lifetime (with grace, inclusive) all is well; one second less is reported."""
        atlas_proxy.set_cache_entry('payload', [NOW + 96 * HOUR, NOW + self.EXPECTED])
        with patch.object(atlas_proxy.logger, 'warning') as mock_warning:
            self.assertTrue(atlas_proxy.check_downloaded_lifetime('payload'))
        mock_warning.assert_not_called()

        atlas_proxy.set_cache_entry('payload', [NOW + self.EXPECTED - 1, NOW + 96 * HOUR])
        with patch.object(atlas_proxy.logger, 'warning') as mock_warning:
            self.assertFalse(atlas_proxy.check_downloaded_lifetime('payload'))
        mock_warning.assert_called_once()
        self.assertIn('payload', mock_warning.call_args.args[0])
        self.assertIn('72h', mock_warning.call_args.args[0])

    def test_unknown_validity_is_not_reported(self):
        """No entry, or a failed verification, is not a lifetime problem."""
        for entry in (None, [-1, -1]):
            atlas_proxy.set_cache_entry('unified', entry)
            with patch.object(atlas_proxy.logger, 'warning') as mock_warning:
                self.assertTrue(atlas_proxy.check_downloaded_lifetime('unified'))
            mock_warning.assert_not_called()

    def test_skipped_in_test_mode(self):
        """In test mode the validity is clamped, so it is not judged."""
        atlas_proxy.set_cache_entry('payload', [NOW + 100, NOW + 100])
        with patch.object(atlas_proxy, '_TEST_JOB_PROXY_RENEWAL', True), \
             patch.object(atlas_proxy.logger, 'warning') as mock_warning:
            self.assertTrue(atlas_proxy.check_downloaded_lifetime('payload'))
        mock_warning.assert_not_called()

    def test_checked_after_job_proxy_download_only(self):
        """get_and_verify_proxy() checks job proxies after a successful verification; never the pilot proxy."""
        with patch('pilot.user.atlas.proxy.get_proxy', side_effect=lambda path, role: (True, path)), \
             patch('pilot.user.atlas.proxy.verify_proxy', return_value=(0, '')), \
             patch('pilot.user.atlas.proxy.check_downloaded_lifetime', return_value=False) as mock_check:
            exit_code, _, _ = atlas_proxy.get_and_verify_proxy('/tmp/x509up_u1', voms_role='atlas',
                                                               proxy_type='payload', workdir='/w')
            self.assertEqual(exit_code, 0)  # a short proxy is still used
            mock_check.assert_called_once_with('payload')
            mock_check.reset_mock()
            atlas_proxy.get_and_verify_proxy('/tmp/x509up_u1', voms_role='atlas')
            mock_check.assert_not_called()
        with patch('pilot.user.atlas.proxy.get_proxy', side_effect=lambda path, role: (True, path)), \
             patch('pilot.user.atlas.proxy.verify_proxy', return_value=(errors.NOVOMSPROXY, 'bad')), \
             patch('pilot.user.atlas.proxy.check_downloaded_lifetime') as mock_check:
            atlas_proxy.get_and_verify_proxy('/tmp/x509up_u1', voms_role='atlas', proxy_type='unified', workdir='/w')
        mock_check.assert_not_called()

    def test_new_download_clears_backoff(self):
        """A proxy downloaded at job start must not inherit a backoff from a previous job."""
        atlas_proxy.set_renewal_backoff('payload')
        with patch('pilot.user.atlas.proxy.get_proxy', side_effect=lambda path, role: (True, path)), \
             patch('pilot.user.atlas.proxy.verify_proxy', return_value=(0, '')):
            atlas_proxy.get_and_verify_proxy('/tmp/x509up_u1', voms_role='atlas', proxy_type='payload', workdir='/w')
        self.assertFalse(atlas_proxy.is_renewal_backed_off('payload', 48 * HOUR))


class TestRenewalBackoff(unittest.TestCase):
    """is_renewal_backed_off(), set_renewal_backoff(), clear_renewal_backoff()."""

    BACKOFF = atlas_proxy.JOB_PROXY_RENEWAL_BACKOFF
    LAST_CHANCE = atlas_proxy.JOB_PROXY_HARD_FLOOR + atlas_proxy.JOB_PROXY_RENEWAL_BACKOFF

    def setUp(self):
        """Start without backoff."""
        _clear_cache()

    def tearDown(self):
        """Leave without backoff."""
        _clear_cache()

    def test_not_backed_off_by_default(self):
        """Without an earlier attempt, a renewal is never postponed."""
        self.assertFalse(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))

    def test_backoff_expires(self):
        """The backoff lasts exactly JOB_PROXY_RENEWAL_BACKOFF, and only for the given proxy."""
        with patch('pilot.user.atlas.proxy.time', return_value=NOW):
            atlas_proxy.set_renewal_backoff('payload')
            self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))
            self.assertFalse(atlas_proxy.is_renewal_backed_off('unified', 12 * HOUR))
        with patch('pilot.user.atlas.proxy.time', return_value=NOW + self.BACKOFF - 1):
            self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))
        with patch('pilot.user.atlas.proxy.time', return_value=NOW + self.BACKOFF):
            self.assertFalse(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))

    def test_no_backoff_close_to_the_floor(self):
        """During the last backoff interval before the hard floor, every check attempts a renewal."""
        with patch('pilot.user.atlas.proxy.time', return_value=NOW):
            atlas_proxy.set_renewal_backoff('payload')
            self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', self.LAST_CHANCE))
            self.assertFalse(atlas_proxy.is_renewal_backed_off('payload', self.LAST_CHANCE - 1))

    def test_clear(self):
        """Clearing the backoff allows the next attempt immediately (and is safe without backoff)."""
        with patch('pilot.user.atlas.proxy.time', return_value=NOW):
            atlas_proxy.set_renewal_backoff('payload')
            atlas_proxy.clear_renewal_backoff('payload')
            self.assertFalse(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))
            atlas_proxy.clear_renewal_backoff('payload')


class TestCheckJobProxyBackoff(unittest.TestCase):
    """check_job_proxy() and renew_job_proxy() respect the backoff."""

    def setUp(self):
        """Start from an empty cache at a fixed time, with a proxy below the threshold and a backoff in place."""
        _clear_cache()
        self.time_patch = patch('pilot.user.atlas.proxy.time', return_value=NOW)
        self.time_patch.start()
        atlas_proxy.set_renewal_backoff('payload')

    def tearDown(self):
        """Stop the time patch and clear the cache."""
        self.time_patch.stop()
        _clear_cache()

    def test_backed_off_proxy_is_not_renewed(self):
        """While backed off, the check neither renews nor fails."""
        atlas_proxy.set_cache_entry('payload', [NOW + 12 * HOUR, NOW + 12 * HOUR])
        with patch('pilot.user.atlas.proxy.renew_job_proxy') as mock_renew:
            self.assertEqual(atlas_proxy.check_job_proxy('payload', '/w/p'), (0, ''))
        mock_renew.assert_not_called()

    def test_last_chance_ignores_backoff(self):
        """Close to the floor the backoff is ignored, and a failure below the floor fails the job."""
        failure = (errors.PAYLOADPROXYDOWNLOADFAILURE, 'same proxy')
        atlas_proxy.set_cache_entry('payload', [NOW + 30 * 60, NOW + 30 * 60])
        with patch('pilot.user.atlas.proxy.renew_job_proxy', return_value=failure) as mock_renew:
            self.assertEqual(atlas_proxy.check_job_proxy('payload', '/w/p'), (0, ''))
        mock_renew.assert_called_once()
        atlas_proxy.set_cache_entry('payload', [NOW + 60, NOW + 60])
        with patch('pilot.user.atlas.proxy.renew_job_proxy', return_value=failure):
            self.assertEqual(atlas_proxy.check_job_proxy('payload', '/w/p')[0], errors.PAYLOADPROXYDOWNLOADFAILURE)

    def test_renew_job_proxy_skips_when_backed_off(self):
        """A renewal attempted by another thread while this one waited for the lock is not repeated."""
        atlas_proxy.set_cache_entry('payload', [NOW + 12 * HOUR, NOW + 12 * HOUR])
        with patch('pilot.user.atlas.proxy._renew_job_proxy') as mock_renew:
            self.assertEqual(atlas_proxy.renew_job_proxy('payload', '/w/p'), (0, ''))
        mock_renew.assert_not_called()
        atlas_proxy.clear_renewal_backoff('payload')
        with patch('pilot.user.atlas.proxy._renew_job_proxy', return_value=(0, '')) as mock_renew:
            atlas_proxy.renew_job_proxy('payload', '/w/p')
        mock_renew.assert_called_once_with('payload', '/w/p')


class TestRenewalOutcome(unittest.TestCase):
    """_renew_job_proxy(): a new proxy is only installed if it is valid longer; the backoff follows the outcome."""

    OLD = 'old-proxy'
    NEW = 'new-proxy'

    def setUp(self):
        """Create a work directory with a proxy in use that has 12h left."""
        _clear_cache()
        self.base = tempfile.mkdtemp()
        self.path = os.path.join(self.base, 'x509up_u1-payload.proxy')
        with open(self.path, 'w', encoding='utf-8') as _file:
            _file.write(self.OLD)
        pilot_cache.payload_proxy = self.path
        self.old_end = NOW + 12 * HOUR
        self.old_entry = [self.old_end, self.old_end]
        atlas_proxy.set_cache_entry('payload', self.old_entry)
        self.time_patch = patch('pilot.user.atlas.proxy.time', return_value=NOW)
        self.time_patch.start()

    def tearDown(self):
        """Clean up."""
        self.time_patch.stop()
        shutil.rmtree(self.base, ignore_errors=True)
        pilot_cache.payload_proxy = None
        _clear_cache()

    def _read(self):
        with open(self.path, encoding='utf-8') as _file:
            return _file.read()

    def _renew(self, new_entry, get_result=True):
        def fake_get_proxy(path, _role):
            with open(path, 'w', encoding='utf-8') as _file:
                _file.write(self.NEW)
            return get_result, path

        def fake_verify(proxy_id=None, **_kwargs):
            if new_entry is not None:
                atlas_proxy.set_cache_entry(proxy_id, new_entry)
            return 0, ''

        with patch('pilot.user.atlas.proxy.get_proxy', side_effect=fake_get_proxy), \
             patch('pilot.user.atlas.proxy.verify_proxy', side_effect=fake_verify), \
             patch('pilot.user.atlas.proxy.check_downloaded_lifetime') as mock_lifetime:
            result = atlas_proxy._renew_job_proxy('payload', self.path)
        return result, mock_lifetime

    def _assert_kept(self, result):
        self.assertEqual(result[0], errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertEqual(self._read(), self.OLD)
        self.assertFalse(os.path.exists(self.path + '.tmp'))
        self.assertEqual(atlas_proxy.get_cache_entry('payload'), self.old_entry)

    def test_same_proxy_is_not_installed(self):
        """The server returning the same proxy (same validity end) keeps the current one and backs off."""
        result, mock_lifetime = self._renew([self.old_end, self.old_end])
        self._assert_kept(result)
        self.assertIn('not valid longer', result[1])
        self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))
        mock_lifetime.assert_called_once_with('payload')

    def test_shorter_proxy_is_not_installed(self):
        """A proxy ending earlier than the current one is discarded."""
        result, _ = self._renew([self.old_end - 1, NOW + 96 * HOUR])
        self._assert_kept(result)
        self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))

    def test_unknown_validity_is_not_installed(self):
        """A new proxy without a cached validity would disable further renewals, so it is discarded."""
        result, _ = self._renew(None)
        self._assert_kept(result)
        self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))

    def test_longer_proxy_still_below_threshold_is_installed_with_backoff(self):
        """A proxy valid one second longer is better and installed, but the next attempt is postponed."""
        result, _ = self._renew([self.old_end + 1, self.old_end + 1])
        self.assertEqual(result, (0, ''))
        self.assertEqual(self._read(), self.NEW)
        self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))

    def test_proxy_at_threshold_clears_backoff(self):
        """A proxy valid for at least the threshold solves the problem: any earlier backoff is cleared."""
        atlas_proxy.set_renewal_backoff('payload')
        result, _ = self._renew([NOW + THRESHOLD, NOW + THRESHOLD])
        self.assertEqual(result, (0, ''))
        self.assertEqual(self._read(), self.NEW)
        self.assertFalse(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))
        # just below the threshold, the backoff is (re)set
        atlas_proxy.set_cache_entry('payload', self.old_entry)
        self._renew([NOW + THRESHOLD - 1, NOW + THRESHOLD - 1])
        self.assertTrue(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))

    def test_download_failure_is_retried_at_next_check(self):
        """A failed download is not a reason to back off (e.g. a transient server problem)."""
        result, mock_lifetime = self._renew([NOW + 96 * HOUR, NOW + 96 * HOUR], get_result=False)
        self._assert_kept(result)
        self.assertFalse(atlas_proxy.is_renewal_backed_off('payload', 12 * HOUR))
        mock_lifetime.assert_not_called()


class TestTestModeCadence(unittest.TestCase):
    """In test mode, a renewal is due TEST_JOB_PROXY_MARGIN seconds after each download."""

    def setUp(self):
        """Start from an empty cache."""
        _clear_cache()

    def tearDown(self):
        """Clear the cache."""
        _clear_cache()

    def test_renewal_follows_download_after_margin(self):
        """The clamped validity is not due at the margin (inclusive) but one second later."""
        end = NOW + 96 * HOUR
        margin = atlas_proxy.TEST_JOB_PROXY_MARGIN
        with patch.object(atlas_proxy, '_TEST_JOB_PROXY_RENEWAL', True), \
             patch('pilot.user.atlas.proxy.execute_nothreads', return_value=(0, 'a\nb', '')), \
             patch('pilot.user.atlas.proxy.interpret_proxy_info', return_value=(0, '', end, end)):
            with patch('pilot.user.atlas.proxy.time', return_value=NOW):
                self.assertEqual(atlas_proxy.verify_arcproxy('', 1, proxy_id='payload'), (0, ''))
            with patch('pilot.user.atlas.proxy.renew_job_proxy', return_value=(0, '')) as mock_renew:
                with patch('pilot.user.atlas.proxy.time', return_value=NOW + margin):
                    atlas_proxy.check_job_proxy('payload', '/w/p')
                mock_renew.assert_not_called()
                with patch('pilot.user.atlas.proxy.time', return_value=NOW + margin + 1):
                    atlas_proxy.check_job_proxy('payload', '/w/p')
                mock_renew.assert_called_once()


class TestMonitoringJobProxy(unittest.TestCase):
    """verify_job_proxy() and verify_proxies() in pilot.util.monitoring."""

    def setUp(self):
        """Create a monitoring time object with an elapsed job proxy timer."""
        self.mt = MonitoringTime()
        self.mt.update('ct_job_proxy', modtime=NOW - 601)

    def _verify(self, job, plugin_result=(0, ''), current_time=NOW):
        with patch.dict('os.environ', {'PILOT_USER': 'atlas'}, clear=False), \
             patch('pilot.user.atlas.proxy.verify_job_proxies', return_value=plugin_result) as mock_verify:
            result = monitoring.verify_job_proxy(current_time, self.mt, job)
        return result, mock_verify

    def test_monitoring_time_has_job_proxy_timer(self):
        """MonitoringTime must provide ct_job_proxy."""
        self.assertIsInstance(MonitoringTime().get('ct_job_proxy'), int)

    def test_only_while_running(self):
        """No check outside the running state, and the timer is left alone."""
        for state in ('starting', 'stageout', 'finished', 'failed', ''):
            result, mock_verify = self._verify(_FakeJob(state=state))
            self.assertEqual(result, (0, ''))
            mock_verify.assert_not_called()
            self.assertEqual(self.mt.get('ct_job_proxy'), NOW - 601)

    def test_interval(self):
        """The check runs only once the verification interval has passed (exclusive)."""
        self.mt.update('ct_job_proxy', modtime=NOW - 600)
        _, mock_verify = self._verify(_FakeJob())
        mock_verify.assert_not_called()
        self.mt.update('ct_job_proxy', modtime=NOW - 601)
        with patch('pilot.util.monitoring.time.time', return_value=NOW):
            result, mock_verify = self._verify(_FakeJob())
        self.assertEqual(result, (0, ''))
        mock_verify.assert_called_once_with()
        self.assertEqual(self.mt.get('ct_job_proxy'), NOW)

    def test_failure_is_returned(self):
        """A fatal plugin result is returned to the job monitor."""
        failure = (errors.PAYLOADPROXYDOWNLOADFAILURE, 'expired')
        result, _ = self._verify(_FakeJob(), plugin_result=failure)
        self.assertEqual(result, failure)

    def test_plugin_without_hook(self):
        """A plugin without verify_job_proxies() is simply skipped."""
        with patch.dict('os.environ', {'PILOT_USER': 'generic'}, clear=False):
            self.assertEqual(monitoring.verify_job_proxy(NOW, self.mt, _FakeJob()), (0, ''))

    def test_verify_proxies_order(self):
        """A pilot proxy problem is returned first (its renewal is handled by the job monitor)."""
        pilot_result = (errors.VOMSPROXYABOUTTOEXPIRE, 'about to expire')
        with patch('pilot.util.monitoring.verify_user_proxy', return_value=pilot_result), \
             patch('pilot.util.monitoring.verify_job_proxy') as mock_job:
            self.assertEqual(monitoring.verify_proxies(NOW, self.mt, _FakeJob()), pilot_result)
        mock_job.assert_not_called()
        job_result = (errors.PAYLOADPROXYDOWNLOADFAILURE, 'expired')
        with patch('pilot.util.monitoring.verify_user_proxy', return_value=(0, '')), \
             patch('pilot.util.monitoring.verify_job_proxy', return_value=job_result):
            self.assertEqual(monitoring.verify_proxies(NOW, self.mt, _FakeJob()), job_result)

    def test_fatal_code_kills_the_payload(self):
        """The job monitor must kill the payload for an unrenewable job proxy."""
        self.assertIn(errors.PAYLOADPROXYDOWNLOADFAILURE, job_module.PAYLOAD_KILL_EXIT_CODES)
        for code in (errors.KILLPAYLOAD, errors.NOVOMSPROXY, errors.CERTIFICATEHASEXPIRED):
            self.assertIn(code, job_module.PAYLOAD_KILL_EXIT_CODES)
        self.assertNotIn(errors.VOMSPROXYABOUTTOEXPIRE, job_module.PAYLOAD_KILL_EXIT_CODES)


class TestRefreshBeforeStageout(unittest.TestCase):
    """refresh_job_proxies_before_stageout() in pilot.control.data."""

    def test_no_unified_proxy(self):
        """Nothing happens without a unified dispatch proxy."""
        with patch.dict('os.environ', {'X509_UNIFIED_DISPATCH': '', 'PILOT_USER': 'atlas'}, clear=False), \
             patch('pilot.user.atlas.proxy.verify_job_proxies') as mock_verify:
            data.refresh_job_proxies_before_stageout()
        mock_verify.assert_not_called()

    def test_unified_proxy_is_refreshed(self):
        """Only the unified proxy is checked; a failure is logged, not raised."""
        env = {'X509_UNIFIED_DISPATCH': '/w/u-unified.proxy', 'PILOT_USER': 'atlas'}
        for result in ((0, ''), (errors.PAYLOADPROXYDOWNLOADFAILURE, 'expired')):
            with patch.dict('os.environ', env, clear=False), \
                 patch('pilot.user.atlas.proxy.verify_job_proxies', return_value=result) as mock_verify, \
                 patch.object(data.logger, 'warning') as mock_warning:
                data.refresh_job_proxies_before_stageout()
            mock_verify.assert_called_once_with(proxy_ids=('unified',))
            self.assertEqual(mock_warning.call_count, 1 if result[0] else 0)

    def test_plugin_without_hook(self):
        """A plugin without verify_job_proxies() is simply skipped."""
        env = {'X509_UNIFIED_DISPATCH': '/w/u-unified.proxy', 'PILOT_USER': 'generic'}
        with patch.dict('os.environ', env, clear=False):
            data.refresh_job_proxies_before_stageout()


class TestAlrbPresetup(unittest.TestCase):
    """ALRB_CONT_PRESETUP must source a pilot script pointing X509_USER_PROXY to the job proxy in /srv.

    ALRB evaluates ALRB_CONT_PRESETUP as a command inside the container. Pointing it directly at the
    proxy made ALRB try to execute the proxy ("/srv/x509up_u25606_prod-payload.proxy: Permission
    denied"), and executing a script would lose its export in the child process, so the script is sourced.
    """

    EXPORT = 'export ALRB_CONT_PRESETUP="source /srv/pilot_proxy_presetup.sh";'

    def setUp(self):
        """Create a work directory with a job proxy."""
        self.workdir = tempfile.mkdtemp()
        self.proxy = os.path.join(self.workdir, 'x509up_u25606_prod-payload.proxy')
        self.script = os.path.join(self.workdir, 'pilot_proxy_presetup.sh')

    def tearDown(self):
        """Remove the work directory and reset the cache."""
        shutil.rmtree(self.workdir, ignore_errors=True)
        pilot_cache.payload_proxy = None

    def _read_script(self):
        with open(self.script, encoding='utf-8') as script:
            return script.read()

    def test_script_content_and_mode(self):
        """The script exports the proxy path as seen in the container, and contains no credential."""
        self.assertTrue(container.write_proxy_presetup(self.proxy, self.workdir))
        content = self._read_script()
        self.assertTrue(content.startswith('#!/bin/bash\n'))
        self.assertIn('\nexport X509_USER_PROXY=/srv/x509up_u25606_prod-payload.proxy\n', content)
        self.assertNotIn(self.workdir, content)
        self.assertEqual(os.stat(self.script).st_mode & 0o777, 0o644)

    def test_script_only_rewritten_when_changed(self):
        """Requested twice per job (setup verification, payload): the second request must not rewrite it."""
        container.write_proxy_presetup(self.proxy, self.workdir)
        os.utime(self.script, ns=(0, 0))
        self.assertTrue(container.write_proxy_presetup(self.proxy, self.workdir))
        self.assertEqual(os.stat(self.script).st_mtime_ns, 0)
        other = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        self.assertTrue(container.write_proxy_presetup(other, self.workdir))
        self.assertIn('export X509_USER_PROXY=/srv/x509up_u1-unified.proxy\n', self._read_script())
        self.assertNotEqual(os.stat(self.script).st_mtime_ns, 0)

    def test_script_write_failure(self):
        """A failure to write the script is reported (and logged), not raised."""
        missing = os.path.join(self.workdir, 'missing')
        with patch.object(container.logger, 'warning') as mock_warning:
            self.assertFalse(container.write_proxy_presetup(os.path.join(missing, 'p.proxy'), missing))
        mock_warning.assert_called_once()

    def test_job_proxy_in_workdir(self):
        """A proxy in the work directory gets the export, and the script is written."""
        with patch.dict('os.environ', {'ALRB_CONT_PRESETUP': ''}, clear=False):
            self.assertEqual(get_alrb_presetup(self.proxy, self.workdir), self.EXPORT)
            self.assertEqual(get_alrb_presetup(self.proxy, self.workdir + '/'), self.EXPORT)
        self.assertTrue(os.path.exists(self.script))

    def test_no_export_without_script(self):
        """If the script cannot be written, nothing is exported (ALRB would fail to source it)."""
        with patch.dict('os.environ', {'ALRB_CONT_PRESETUP': ''}, clear=False), \
             patch.object(container, 'write_proxy_presetup', return_value=False):
            self.assertEqual(get_alrb_presetup(self.proxy, self.workdir), '')

    def test_not_for_other_proxies(self):
        """The pilot's own proxy (outside the work directory) and missing inputs get nothing."""
        with patch.dict('os.environ', {'ALRB_CONT_PRESETUP': ''}, clear=False), \
             patch.object(container, 'write_proxy_presetup', return_value=True) as mock_write:
            self.assertEqual(get_alrb_presetup('/tmp/x509up_u1', self.workdir), '')
            self.assertEqual(get_alrb_presetup(os.path.join(self.workdir, 'sub', 'p.proxy'), self.workdir), '')
            self.assertEqual(get_alrb_presetup(self.proxy, ''), '')
            self.assertEqual(get_alrb_presetup('', self.workdir), '')
            # the pilot runs in its launch directory, where e.g. the read-only fallback writes proxies
            cwd = os.getcwd()
            self.assertEqual(get_alrb_presetup(os.path.join(cwd, 'x509up_u1-payload.proxy'), ''), '')
            self.assertEqual(get_alrb_presetup('', cwd), '')
            with patch('os.getcwd', return_value='/'):  # an empty path must never produce an export
                self.assertEqual(get_alrb_presetup('', '/'), '')
        mock_write.assert_not_called()

    def test_site_setting_is_respected(self):
        """A site-level ALRB_CONT_PRESETUP is not overridden, and no script is written."""
        with patch.dict('os.environ', {'ALRB_CONT_PRESETUP': '/site/presetup.sh'}, clear=False), \
             patch.object(container.logger, 'warning') as mock_warning:
            self.assertEqual(container.get_alrb_presetup(self.proxy, self.workdir), '')
        mock_warning.assert_called_once()
        self.assertFalse(os.path.exists(self.script))

    def test_update_for_user_proxy_adds_presetup_before_setup(self):
        """A job proxy: X509_USER_PROXY is unset (ALRB copies nothing) and the presetup comes before setupATLAS."""
        pilot_cache.payload_proxy = self.proxy
        env = {'X509_USER_PROXY': '/tmp/x509up_u1', 'X509_UNIFIED_DISPATCH': '', 'ALRB_CONT_PRESETUP': ''}
        with patch.dict('os.environ', env, clear=False):
            _, _, setup_cmd, _ = update_for_user_proxy('source atlasLocalSetup.sh', 'payload', is_analysis=True,
                                                       queue_type='production', workdir=self.workdir)
            self.assertEqual(setup_cmd, f'unset X509_USER_PROXY;{self.EXPORT}source atlasLocalSetup.sh')
            # production job: the pilot's own proxy is still exported (and copied by ALRB)
            _, _, setup_cmd, _ = update_for_user_proxy('setup', 'payload', is_analysis=False,
                                                       queue_type='production', workdir=self.workdir)
            self.assertEqual(setup_cmd, 'export X509_USER_PROXY=/tmp/x509up_u1;setup')

    def test_update_for_user_proxy_unified(self):
        """On unified dispatch queues, the user proxy in the work directory gets the export."""
        unified = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        env = {'X509_USER_PROXY': '/tmp/x509up_u1', 'X509_UNIFIED_DISPATCH': unified, 'ALRB_CONT_PRESETUP': ''}
        with patch.dict('os.environ', env, clear=False):
            _, _, setup_cmd, _ = update_for_user_proxy('setup', 'payload', is_analysis=True,
                                                       queue_type='unified', workdir=self.workdir)
        self.assertIn(self.EXPORT, setup_cmd)
        self.assertIn('export X509_USER_PROXY=/srv/x509up_u1-unified.proxy\n', self._read_script())

    def test_sourced_script_sets_proxy_in_shell(self):
        """End to end: ALRB's eval of the export must leave X509_USER_PROXY pointing to the /srv proxy."""
        container.write_proxy_presetup(self.proxy, self.workdir)
        # emulate the container: /srv is the work directory; ALRB has pointed X509_USER_PROXY to its copy
        presetup = f'source {self.script}'
        bashrc = 'X509_USER_PROXY=/alrb/copy; eval $ALRB_CONT_PRESETUP; echo -n $X509_USER_PROXY'
        env = dict(os.environ, ALRB_CONT_PRESETUP=presetup)
        result = subprocess.run(['bash', '-c', bashrc], env=env, capture_output=True, text=True, check=False)
        self.assertEqual(result.stdout, '/srv/x509up_u25606_prod-payload.proxy')
        self.assertEqual(result.stderr, '')


class TestProdproxyRemoved(unittest.TestCase):
    """The unused job.prodproxy attribute has been removed."""

    def test_jobdata_has_no_prodproxy(self):
        """JobData must no longer define or parse prodproxy."""
        self.assertFalse(hasattr(JobData, 'prodproxy'))
        for names in JobData._keys.values():
            self.assertNotIn('prodproxy', names)


if __name__ == '__main__':
    unittest.main()
