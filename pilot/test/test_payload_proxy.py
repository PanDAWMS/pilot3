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

"""Unit tests for payload proxy handling.

Regression tests for the reported problem where the payload proxy download failed on an
analysis job (PanDA id 7299340608, ANALY_LRZ_VP, "[Errno 101] Network is unreachable") but the
payload was executed anyway, under the pilot's own proxy, and the job was reported with the
generic error 1163 ("Grid proxy not valid").

Three distinct defects were involved:

1. The download lived in ``update_for_user_proxy()``, which is called from ``alrb_wrapper()`` -
   a command-string builder invoked from ``pilot.util.container.execute()``. It therefore ran
   twice per job (once for the setup verification, once for the payload) and was reached far too
   late for the job to be failed. The download now happens once per job in
   ``handle_payload_proxy()`` during job validation, and ``update_for_user_proxy()`` only
   consumes the resolved path from the pilot cache.

2. ``get_and_verify_proxy()`` returned ``exit_code = 0`` when ``verify_proxy()`` reported all
   verifications as failed via a non-empty diagnostics string with a zero exit code. In that
   case ``x509`` was left unchanged, so the caller silently continued with the pilot's own proxy
   and no error was recorded at all.

3. ``verify_arcproxy()`` returns ``-1`` when arcproxy itself is unavailable on the queue. That
   value was propagated as an error code even though it has no entry in
   ``ErrorCodes._error_messages``, and would now fail every job at such sites. It must be
   treated as "downloaded but unverifiable" instead.
"""

import unittest
from unittest.mock import patch

from pilot.common.errorcodes import ErrorCodes
from pilot.common.pilotcache import get_pilot_cache

errors = ErrorCodes()
pilot_cache = get_pilot_cache()

PROXY = '/tmp/x509up_u12345.proxy'
PAYLOAD_PROXY = '/tmp/x509up_u12345-payload.proxy'


class _FakeQueuedata:
    """Minimal queuedata stand-in exposing only what requires_payload_proxy() reads."""

    def __init__(self, queue_type: str = 'production', container_type: dict = None):
        self.type = queue_type
        self.container_type = container_type if container_type is not None else {'pilot': 'apptainer'}


class _FakeInfosys:
    """Minimal infosys stand-in."""

    def __init__(self, queuedata):
        self.queuedata = queuedata


class _FakeJob:
    """Minimal job stand-in exposing only what the payload proxy code reads."""

    def __init__(self, is_analysis: bool = True, queue_type: str = 'production',
                 container_type: dict = None):
        self._is_analysis = is_analysis
        self.infosys = _FakeInfosys(_FakeQueuedata(queue_type, container_type))
        self.jobid = '7299340608'
        self.workdir = '/tmp/PanDA_Pilot-7299340608'
        self.prodproxy = ''

    def is_analysis(self) -> bool:
        """Return True for a user analysis job."""
        return self._is_analysis


def _proxy_env(**overrides) -> dict:
    """Return the environment under which a payload proxy download is expected."""
    env = {
        'X509_USER_PROXY': PROXY,
        'X509_UNIFIED_DISPATCH': '',
        'PILOT_PROXY_VERIFICATION': 'True',
        'PILOT_PAYLOAD_PROXY_VERIFICATION': 'True',
    }
    env.update(overrides)

    return env


class TestErrorCode(unittest.TestCase):
    """The dedicated error code must exist and carry a useful message."""

    def test_code_value(self):
        """PAYLOADPROXYDOWNLOADFAILURE must be 1391."""
        self.assertEqual(errors.PAYLOADPROXYDOWNLOADFAILURE, 1391)

    def test_message_is_specific(self):
        """The message must name the payload proxy download, not just an invalid proxy."""
        msg = errors.get_error_message(errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertIn('payload proxy', msg.lower())
        self.assertNotEqual(msg, errors.get_error_message(errors.NOPROXY))


class TestRequiresPayloadProxy(unittest.TestCase):
    """requires_payload_proxy() must match the conditions under which the proxy is used."""

    def test_analysis_job_on_container_queue_requires_proxy(self):
        """The reported case (analysis, non-unified, pilot container) requires a payload proxy."""
        from pilot.user.atlas.proxy import requires_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False):
            self.assertTrue(requires_payload_proxy(_FakeJob()))

    def test_production_job_does_not_require_proxy(self):
        """Production jobs must not trigger a payload proxy download."""
        from pilot.user.atlas.proxy import requires_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False):
            self.assertFalse(requires_payload_proxy(_FakeJob(is_analysis=False)))

    def test_unified_queue_does_not_require_proxy(self):
        """On unified dispatch queues the user proxy is downloaded by handle_proxy() instead."""
        from pilot.user.atlas.proxy import requires_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False):
            self.assertFalse(requires_payload_proxy(_FakeJob(queue_type='unified')))

    def test_queue_without_pilot_container_does_not_require_proxy(self):
        """Without a pilot container there is no container setup command to add the proxy to.

        This gate matters: the download used to sit inside alrb_wrapper(), which is only reached
        when a pilot container is configured. Without the gate, hoisting the download would start
        failing jobs that never had a payload proxy in the first place.
        """
        from pilot.user.atlas.proxy import requires_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False):
            self.assertFalse(requires_payload_proxy(_FakeJob(container_type={})))

    def test_payload_proxy_verification_disabled(self):
        """--verify-payload-proxy=false must suppress the download."""
        from pilot.user.atlas.proxy import requires_payload_proxy

        env = _proxy_env(PILOT_PAYLOAD_PROXY_VERIFICATION='False')
        with patch.dict('os.environ', env, clear=False):
            self.assertFalse(requires_payload_proxy(_FakeJob()))

    def test_no_x509_set(self):
        """Without an X509_USER_PROXY there is nothing to replace."""
        from pilot.user.atlas.proxy import requires_payload_proxy

        env = _proxy_env(X509_USER_PROXY='')
        with patch.dict('os.environ', env, clear=False):
            self.assertFalse(requires_payload_proxy(_FakeJob()))


class TestHandlePayloadProxy(unittest.TestCase):
    """handle_payload_proxy() must report failures and cache successes."""

    def setUp(self):
        """Reset the singleton cache entry."""
        pilot_cache.payload_proxy = None

    def tearDown(self):
        """Reset the singleton cache entry."""
        pilot_cache.payload_proxy = None

    def test_download_failure_is_reported(self):
        """A failed download must return PAYLOADPROXYDOWNLOADFAILURE with diagnostics."""
        from pilot.user.atlas.proxy import handle_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False), \
             patch('pilot.user.atlas.proxy.get_proxy', return_value=(False, PAYLOAD_PROXY)):
            exit_code, diagnostics = handle_payload_proxy(_FakeJob())

        self.assertEqual(exit_code, errors.PAYLOADPROXYDOWNLOADFAILURE)
        self.assertTrue(diagnostics)

    def test_download_failure_leaves_cache_empty(self):
        """A failed download must not leave a payload proxy in the cache."""
        from pilot.user.atlas.proxy import handle_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False), \
             patch('pilot.user.atlas.proxy.get_proxy', return_value=(False, PAYLOAD_PROXY)):
            handle_payload_proxy(_FakeJob())

        self.assertIsNone(pilot_cache.payload_proxy)

    def test_success_caches_the_proxy(self):
        """A successful download and verification must cache the payload proxy path."""
        from pilot.user.atlas.proxy import handle_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False), \
             patch('pilot.user.atlas.proxy.get_proxy', return_value=(True, PAYLOAD_PROXY)), \
             patch('pilot.user.atlas.proxy.verify_proxy', return_value=(0, '')):
            exit_code, _ = handle_payload_proxy(_FakeJob())

        self.assertEqual(exit_code, 0)
        self.assertEqual(pilot_cache.payload_proxy, PAYLOAD_PROXY)

    def test_no_download_attempted_when_not_required(self):
        """When no payload proxy is required, get_proxy() must not be called at all."""
        from pilot.user.atlas.proxy import handle_payload_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False), \
             patch('pilot.user.atlas.proxy.get_proxy') as mock_get_proxy:
            exit_code, _ = handle_payload_proxy(_FakeJob(is_analysis=False))

        self.assertEqual(exit_code, 0)
        mock_get_proxy.assert_not_called()


class TestVerificationFailureClassification(unittest.TestCase):
    """get_and_verify_proxy() must not silently accept an unverified proxy."""

    def test_zero_exit_code_with_diagnostics_is_a_failure(self):
        """verify_proxy() returning (0, <diagnostics>) must not be reported as success.

        This was the silent path: exit_code stayed 0 while x509 was left unchanged, so the
        caller continued with the pilot's own proxy without any error being recorded.
        """
        from pilot.user.atlas.proxy import get_and_verify_proxy

        with patch('pilot.user.atlas.proxy.get_proxy', return_value=(True, PAYLOAD_PROXY)), \
             patch('pilot.user.atlas.proxy.verify_proxy',
                   return_value=(0, 'arcproxy failed to parse the proxy')):
            exit_code, diagnostics, x509 = get_and_verify_proxy(
                PROXY, voms_role='atlas', proxy_type='payload'
            )

        self.assertNotEqual(exit_code, 0)
        self.assertTrue(diagnostics)
        self.assertEqual(x509, PROXY, 'the unverified proxy must not be returned')

    def test_missing_arcproxy_is_tolerated(self):
        """verify_arcproxy() returning -1 must not fail the job.

        -1 means arcproxy is unavailable on the queue, which says nothing about the downloaded
        proxy. It also has no entry in ErrorCodes._error_messages, so it must never reach
        add_error_code().
        """
        from pilot.user.atlas.proxy import get_and_verify_proxy

        with patch('pilot.user.atlas.proxy.get_proxy', return_value=(True, PAYLOAD_PROXY)), \
             patch('pilot.user.atlas.proxy.verify_proxy',
                   return_value=(-1, 'arcproxy is not available on this queue')):
            exit_code, _, x509 = get_and_verify_proxy(
                PROXY, voms_role='atlas', proxy_type='payload'
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(x509, PAYLOAD_PROXY, 'the downloaded proxy must still be used')

    def test_verification_error_code_is_propagated(self):
        """A real verification error code must be passed through unchanged."""
        from pilot.user.atlas.proxy import get_and_verify_proxy

        with patch('pilot.user.atlas.proxy.get_proxy', return_value=(True, PAYLOAD_PROXY)), \
             patch('pilot.user.atlas.proxy.verify_proxy',
                   return_value=(errors.CERTIFICATEHASEXPIRED, 'certificate has expired')):
            exit_code, _, _ = get_and_verify_proxy(
                PROXY, voms_role='atlas', proxy_type='payload'
            )

        self.assertEqual(exit_code, errors.CERTIFICATEHASEXPIRED)


class TestUpdateForUserProxyNoLongerDownloads(unittest.TestCase):
    """update_for_user_proxy() must be a pure string operation.

    It is called once per execute() with usecontainer=True, i.e. at least twice per job (setup
    verification and payload). Any download here is both duplicated and too late to fail the job.
    """

    def setUp(self):
        """Reset the singleton cache entry."""
        pilot_cache.payload_proxy = None

    def tearDown(self):
        """Reset the singleton cache entry."""
        pilot_cache.payload_proxy = None

    def test_does_not_call_get_proxy(self):
        """No proxy download may be triggered from the container command builder."""
        from pilot.user.atlas.container import update_for_user_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False), \
             patch('pilot.util.proxy.get_proxy') as mock_get_proxy:
            update_for_user_proxy('setup;', f'export X509_USER_PROXY={PROXY};payload',
                                  is_analysis=True, queue_type='production')

        mock_get_proxy.assert_not_called()

    def test_never_returns_a_non_zero_exit_code(self):
        """The builder can no longer fail, so it must always return exit_code 0."""
        from pilot.user.atlas.container import update_for_user_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False):
            exit_code, diagnostics, _, _ = update_for_user_proxy(
                'setup;', f'export X509_USER_PROXY={PROXY};payload',
                is_analysis=True, queue_type='production'
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(diagnostics, '')

    def test_uses_cached_payload_proxy(self):
        """The cached payload proxy must be exported in the container setup command."""
        from pilot.user.atlas.container import update_for_user_proxy

        pilot_cache.payload_proxy = PAYLOAD_PROXY
        with patch.dict('os.environ', _proxy_env(), clear=False):
            _, _, setup_cmd, cmd = update_for_user_proxy(
                'setup;', f'export X509_USER_PROXY={PROXY};payload',
                is_analysis=True, queue_type='production'
            )

        self.assertIn(f'export X509_USER_PROXY={PAYLOAD_PROXY};', setup_cmd)
        self.assertNotIn(f'export X509_USER_PROXY={PROXY};', cmd)

    def test_falls_back_to_pilot_proxy_when_no_payload_proxy(self):
        """Without a cached payload proxy, the pilot proxy is used (production jobs)."""
        from pilot.user.atlas.container import update_for_user_proxy

        with patch.dict('os.environ', _proxy_env(), clear=False):
            _, _, setup_cmd, _ = update_for_user_proxy(
                'setup;', f'export X509_USER_PROXY={PROXY};payload',
                is_analysis=False, queue_type='production'
            )

        self.assertIn(f'export X509_USER_PROXY={PROXY};', setup_cmd)

    def test_unified_queue_ignores_cached_payload_proxy(self):
        """On unified queues the X509_UNIFIED_DISPATCH proxy must win."""
        from pilot.user.atlas.container import update_for_user_proxy

        pilot_cache.payload_proxy = PAYLOAD_PROXY
        unified = '/tmp/PanDA_Pilot-7299340608/x509up_u12345-unified.proxy'
        env = _proxy_env(X509_UNIFIED_DISPATCH=unified)
        with patch.dict('os.environ', env, clear=False):
            _, _, setup_cmd, _ = update_for_user_proxy(
                'setup;', f'export X509_USER_PROXY={unified};payload',
                is_analysis=True, queue_type='unified'
            )

        self.assertIn(f'export X509_USER_PROXY={unified};', setup_cmd)
        self.assertNotIn(PAYLOAD_PROXY, setup_cmd)


class TestEmptyUnifiedDispatchDoesNotShadow(unittest.TestCase):
    """An empty X509_UNIFIED_DISPATCH must not hide X509_USER_PROXY.

    pilot/control/data.py resets X509_UNIFIED_DISPATCH to '' after a unified job's stage-out
    rather than deleting the key. The previous read pattern
    ``os.environ.get('X509_UNIFIED_DISPATCH', os.environ.get('X509_USER_PROXY', ''))``
    then returned '' for every subsequent job on a multi-job pilot, since the default only
    applies when the key is absent - so no proxy at all was exported into the container setup
    command.
    """

    def setUp(self):
        """Reset the singleton cache entry."""
        pilot_cache.payload_proxy = None

    def tearDown(self):
        """Reset the singleton cache entry."""
        pilot_cache.payload_proxy = None

    def test_proxy_still_exported(self):
        """With X509_UNIFIED_DISPATCH='' the pilot proxy must still be exported."""
        from pilot.user.atlas.container import update_for_user_proxy

        env = _proxy_env(X509_UNIFIED_DISPATCH='')
        with patch.dict('os.environ', env, clear=False):
            _, _, setup_cmd, _ = update_for_user_proxy(
                'setup;', f'export X509_USER_PROXY={PROXY};payload',
                is_analysis=False, queue_type='production'
            )

        self.assertIn(f'export X509_USER_PROXY={PROXY};', setup_cmd)

    def test_payload_proxy_still_required(self):
        """With X509_UNIFIED_DISPATCH='' a payload proxy must still be requested."""
        from pilot.user.atlas.proxy import requires_payload_proxy

        env = _proxy_env(X509_UNIFIED_DISPATCH='')
        with patch.dict('os.environ', env, clear=False):
            self.assertTrue(requires_payload_proxy(_FakeJob()))


class TestPluginInterface(unittest.TestCase):
    """Every experiment plugin must implement the payload proxy hooks."""

    EXPERIMENTS = ('atlas', 'darkside', 'epic', 'generic', 'rubin', 'ska', 'sphenix')

    def test_all_plugins_implement_the_hooks(self):
        """handle_payload_proxy() and requires_payload_proxy() must exist everywhere."""
        import importlib

        for experiment in self.EXPERIMENTS:
            with self.subTest(experiment=experiment):
                module = importlib.import_module(f'pilot.user.{experiment}.proxy')
                self.assertTrue(callable(getattr(module, 'handle_payload_proxy', None)))
                self.assertTrue(callable(getattr(module, 'requires_payload_proxy', None)))

    def test_stub_plugins_return_no_payload_proxy(self):
        """Plugins without payload proxy support must return (0, '') and require nothing."""
        import importlib

        for experiment in ('darkside', 'epic', 'generic', 'rubin', 'ska', 'sphenix'):
            with self.subTest(experiment=experiment):
                module = importlib.import_module(f'pilot.user.{experiment}.proxy')
                self.assertFalse(module.requires_payload_proxy(_FakeJob()))
                self.assertEqual(module.handle_payload_proxy(_FakeJob()), (0, ''))


if __name__ == '__main__':
    unittest.main()
