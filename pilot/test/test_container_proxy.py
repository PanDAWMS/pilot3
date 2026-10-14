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

"""Unit tests for the proxy given to ALRB containers.

ALRB copies the proxy in X509_USER_PROXY (whether exported in the container command or inherited
from the pilot's environment) into its own directories under ALRB_CONT_CHOME, which is the job work
directory. To keep proxies (in particular the pilot's own, more privileged proxy) out of ALRB's
directories, X509_USER_PROXY is unset in the container command and the container is pointed to a
proxy in the work directory (/srv inside the container) via ALRB_CONT_PRESETUP instead:

- the payload container with a job proxy (payload proxy or unified dispatch user proxy)
- the stage-in/out and file open containers, which get a temporary copy of the pilot's own proxy
  in the work directory, removed as soon as the container has finished
- containers that must not get a proxy at all (debug command) unset X509_USER_PROXY

If the presetup cannot be used (site-level ALRB_CONT_PRESETUP, or the script cannot be written), the
proxy is exported as before, so that a container is never left without a proxy.
"""

import os
import shutil
import subprocess
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pilot.common.exception import FileHandlingFailure, PilotException, StageInFailure
from pilot.common.pilotcache import get_pilot_cache
from pilot.user.atlas import common
from pilot.user.atlas import container
from pilot.util import middleware
from pilot.util.proxy import (
    copy_proxy_for_container,
    get_container_proxies,
    get_container_proxy_path,
    remove_container_proxies,
    remove_job_proxies,
)

pilot_cache = get_pilot_cache()

PROXY_CONTENT = 'pilot-proxy-content'


class _ProxyTestCase(unittest.TestCase):
    """Work directory with a pilot proxy outside of it, and a clean environment."""

    def setUp(self):
        """Create a work directory and a pilot proxy outside of it."""
        self.base = tempfile.mkdtemp()
        self.workdir = os.path.join(self.base, 'PanDA_Pilot-1')
        os.mkdir(self.workdir)
        self.pilot_proxy = os.path.join(self.base, 'x509up_u1')
        with open(self.pilot_proxy, 'w', encoding='utf-8') as _file:
            _file.write(PROXY_CONTENT)
        self.env = patch.dict('os.environ', {'X509_USER_PROXY': self.pilot_proxy, 'X509_UNIFIED_DISPATCH': '',
                                             'ALRB_CONT_PRESETUP': '', 'ALRB_CONT_CHOME': 'site'}, clear=False)
        self.env.start()
        pilot_cache.payload_proxy = None

    def tearDown(self):
        """Remove the directories and restore the environment."""
        self.env.stop()
        shutil.rmtree(self.base, ignore_errors=True)
        pilot_cache.payload_proxy = None

    def copy_path(self, label):
        """Return the path of the container copy of the pilot proxy for the given label."""
        return os.path.join(self.workdir, f'x509up_u1-container-{label}.proxy')

    def read(self, path):
        """Return the content of the given file."""
        with open(path, encoding='utf-8') as _file:
            return _file.read()


class TestContainerProxyFiles(_ProxyTestCase):
    """Copies of the pilot proxy made for single containers (pilot/util/proxy.py)."""

    def test_path(self):
        """The copy is named after the proxy and the container label, and ends with .proxy."""
        self.assertEqual(get_container_proxy_path('/tmp/x509up_u1', 'stage-in', '/w'), '/w/x509up_u1-container-stage-in.proxy')
        self.assertEqual(get_container_proxy_path('/tmp/p.proxy', 'file-open', '/w'), '/w/p-container-file-open.proxy')

    def test_copy_is_owner_only(self):
        """The copy has the same content and is only readable by its owner, even if a wider file existed."""
        path = self.copy_path('stage-in')
        with open(path, 'w', encoding='utf-8') as _file:
            _file.write('stale content that is longer than the proxy content')  # must be truncated
        os.chmod(path, 0o644)
        self.assertEqual(copy_proxy_for_container(self.pilot_proxy, 'stage-in', self.workdir), path)
        self.assertEqual(self.read(path), PROXY_CONTENT)
        self.assertEqual(os.stat(path).st_mode & 0o777, 0o600)

    def test_copy_failure(self):
        """A missing proxy or an unwritable work directory gives an empty path and leaves nothing behind."""
        with patch('pilot.util.proxy.logger') as mock_logger:
            self.assertEqual(copy_proxy_for_container(os.path.join(self.base, 'missing'), 'stage-in', self.workdir), '')
            mock_logger.warning.assert_called_once()
            self.assertEqual(copy_proxy_for_container(self.pilot_proxy, 'stage-in', os.path.join(self.base, 'nodir')), '')
        self.assertEqual(get_container_proxies(self.workdir), [])

    def test_copy_failure_after_creation_is_cleaned_up(self):
        """If writing the copy fails after the file was created, the partial file is removed."""
        with patch('pilot.util.proxy.os.chmod', side_effect=OSError('denied')):
            self.assertEqual(copy_proxy_for_container(self.pilot_proxy, 'stage-in', self.workdir), '')
        self.assertFalse(os.path.exists(self.copy_path('stage-in')))

    def test_remove_container_proxies_only(self):
        """Only container copies are removed; job proxies and other files stay."""
        copies = [copy_proxy_for_container(self.pilot_proxy, label, self.workdir) for label in ('stage-in', 'file-open')]
        keep = [os.path.join(self.workdir, name) for name in ('x509up_u1-payload.proxy', 'container-notes.proxy',
                                                              'x509up_u1-container-x.proxy.tmp', 'payload.stdout')]
        for path in keep:
            with open(path, 'w', encoding='utf-8') as _file:
                _file.write('x')
        self.assertEqual(get_container_proxies(self.workdir), sorted(copies))
        self.assertEqual(sorted(remove_container_proxies(self.workdir)), sorted(copies))
        self.assertEqual(get_container_proxies(self.workdir), [])
        for path in keep:
            self.assertTrue(os.path.exists(path))
        self.assertEqual(remove_container_proxies(self.workdir), [])

    def test_workdir_with_glob_characters(self):
        """A work directory name containing glob characters is matched literally."""
        workdir = os.path.join(self.base, 'PanDA_Pilot-[1]')
        os.mkdir(workdir)
        path = copy_proxy_for_container(self.pilot_proxy, 'stage-in', workdir)
        self.assertEqual(get_container_proxies(workdir), [path])

    def test_remove_job_proxies_includes_copies(self):
        """Before the log tarball, container copies are removed together with the job proxies."""
        path = copy_proxy_for_container(self.pilot_proxy, 'stage-out', self.workdir)
        self.assertEqual(remove_job_proxies(self.workdir), [path])
        self.assertFalse(os.path.exists(path))


class TestProxySetup(_ProxyTestCase):
    """get_presetup_script_name(), is_in_workdir(), get_proxy_setup() and remove_proxy_exports()."""

    def test_script_names(self):
        """The payload keeps the original script name; other containers get their own."""
        self.assertEqual(container.get_presetup_script_name(), 'pilot_proxy_presetup.sh')
        self.assertEqual(container.get_presetup_script_name('payload'), 'pilot_proxy_presetup.sh')
        self.assertEqual(container.get_presetup_script_name('stage-in'), 'pilot_proxy_presetup_stage-in.sh')

    def test_is_in_workdir(self):
        """Only files directly in the work directory count; empty inputs never do."""
        self.assertTrue(container.is_in_workdir(os.path.join(self.workdir, 'p.proxy'), self.workdir))
        self.assertTrue(container.is_in_workdir(os.path.join(self.workdir, 'p.proxy'), self.workdir + '/'))
        self.assertFalse(container.is_in_workdir(os.path.join(self.workdir, 'sub', 'p.proxy'), self.workdir))
        self.assertFalse(container.is_in_workdir(self.pilot_proxy, self.workdir))
        self.assertFalse(container.is_in_workdir('', self.workdir))
        self.assertFalse(container.is_in_workdir(self.pilot_proxy, ''))

    def test_proxy_in_workdir_is_unset_and_presetup(self):
        """A proxy in the work directory: unset X509_USER_PROXY, then the presetup for the label."""
        proxy = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        self.assertEqual(container.get_proxy_setup(proxy, self.workdir, label='stage-out'),
                         'unset X509_USER_PROXY;export ALRB_CONT_PRESETUP="source /srv/pilot_proxy_presetup_stage-out.sh";')
        self.assertIn('export X509_USER_PROXY=/srv/x509up_u1-unified.proxy\n',
                      self.read(os.path.join(self.workdir, 'pilot_proxy_presetup_stage-out.sh')))
        self.assertFalse(os.path.exists(os.path.join(self.workdir, 'pilot_proxy_presetup.sh')))

    def test_fallback_to_export(self):
        """Outside the work directory, with a site presetup, or if the script cannot be written: export."""
        proxy = os.path.join(self.workdir, 'x509up_u1-payload.proxy')
        self.assertEqual(container.get_proxy_setup(self.pilot_proxy, self.workdir), f'export X509_USER_PROXY={self.pilot_proxy};')
        with patch.object(container, 'write_proxy_presetup', return_value=False):
            self.assertEqual(container.get_proxy_setup(proxy, self.workdir), f'export X509_USER_PROXY={proxy};')
        with patch.dict('os.environ', {'ALRB_CONT_PRESETUP': 'source /site.sh'}, clear=False):
            self.assertEqual(container.get_proxy_setup(proxy, self.workdir), f'export X509_USER_PROXY={proxy};')

    def test_remove_proxy_exports(self):
        """Every X509_USER_PROXY export is removed from the command, including an empty one."""
        cmd = ('export X509_USER_PROXY=/tmp/x509up_u1;export A=1;export X509_USER_PROXY=;'
               'export X509_USER_PROXY=/w/x509up_u1-payload.proxy;./runGen.py')
        self.assertEqual(container.remove_proxy_exports(cmd), 'export A=1;./runGen.py')
        self.assertEqual(container.remove_proxy_exports('export X509_CERT_DIR=/c;run'), 'export X509_CERT_DIR=/c;run')


class TestContainerProxySetup(_ProxyTestCase):
    """get_container_proxy_setup(): stage-in/out and file open containers."""

    def test_no_proxy(self):
        """Without a proxy, nothing is added and nothing is copied."""
        self.assertEqual(container.get_container_proxy_setup('', self.workdir, 'stage-in'), '')
        self.assertEqual(get_container_proxies(self.workdir), [])

    def test_pilot_proxy_is_copied(self):
        """The pilot proxy is copied into the work directory and the container is pointed to the copy."""
        setup = container.get_container_proxy_setup(self.pilot_proxy, self.workdir, 'stage-in')
        self.assertEqual(setup, 'unset X509_USER_PROXY;export ALRB_CONT_PRESETUP="source /srv/pilot_proxy_presetup_stage-in.sh";')
        self.assertEqual(self.read(self.copy_path('stage-in')), PROXY_CONTENT)
        self.assertIn('export X509_USER_PROXY=/srv/x509up_u1-container-stage-in.proxy\n',
                      self.read(os.path.join(self.workdir, 'pilot_proxy_presetup_stage-in.sh')))
        self.assertNotIn(self.pilot_proxy, setup)

    def test_proxy_in_workdir_is_not_copied(self):
        """The unified dispatch user proxy (already in the work directory) is used directly."""
        unified = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        with open(unified, 'w', encoding='utf-8') as _file:
            _file.write('user')
        setup = container.get_container_proxy_setup(unified, self.workdir, 'stage-out')
        self.assertTrue(setup.startswith('unset X509_USER_PROXY;'))
        self.assertEqual(get_container_proxies(self.workdir), [])

    def test_site_presetup_exports_original(self):
        """With a site-level ALRB_CONT_PRESETUP, the original proxy is exported and no copy is ever made."""
        with patch.dict('os.environ', {'ALRB_CONT_PRESETUP': 'source /site.sh'}, clear=False), \
             patch.object(container, 'copy_proxy_for_container', wraps=container.copy_proxy_for_container) as mock_copy, \
             patch.object(container.logger, 'warning') as mock_warning:
            setup = container.get_container_proxy_setup(self.pilot_proxy, self.workdir, 'stage-in')
        self.assertEqual(setup, f'export X509_USER_PROXY={self.pilot_proxy};')
        mock_warning.assert_called_once()
        mock_copy.assert_not_called()
        self.assertEqual(get_container_proxies(self.workdir), [])

    def test_copy_failure_exports_original(self):
        """If the proxy cannot be copied, the original is exported."""
        with patch.object(container, 'copy_proxy_for_container', return_value=''):
            setup = container.get_container_proxy_setup(self.pilot_proxy, self.workdir, 'stage-in')
        self.assertEqual(setup, f'export X509_USER_PROXY={self.pilot_proxy};')

    def test_script_failure_removes_copy(self):
        """If the presetup script cannot be written, the copy is removed and the original exported."""
        with patch.object(container, 'write_proxy_presetup', return_value=False):
            setup = container.get_container_proxy_setup(self.pilot_proxy, self.workdir, 'stage-in')
        self.assertEqual(setup, f'export X509_USER_PROXY={self.pilot_proxy};')
        self.assertEqual(get_container_proxies(self.workdir), [])

    def test_command_leaves_no_proxy_for_alrb(self):
        """End to end: after the command prefix, ALRB finds no X509_USER_PROXY; the presetup sets the /srv copy."""
        setup = container.get_container_proxy_setup(self.pilot_proxy, self.workdir, 'stage-in')
        # emulate the container: /srv is the work directory (only for sourcing the script here)
        setup = setup.replace('/srv/', self.workdir + '/')
        script = (f'{setup} echo "alrb=$X509_USER_PROXY"; X509_USER_PROXY=/alrb/copy; '
                  'eval $ALRB_CONT_PRESETUP; echo -n "container=$X509_USER_PROXY"')
        env = dict(os.environ, X509_USER_PROXY=self.pilot_proxy)
        result = subprocess.run(['bash', '-c', script], env=env, capture_output=True, text=True, check=False)
        self.assertEqual(result.stdout, 'alrb=\ncontainer=/srv/x509up_u1-container-stage-in.proxy')
        self.assertEqual(result.stderr, '')


class TestPayloadContainer(_ProxyTestCase):
    """update_for_user_proxy(): payload container."""

    def test_job_proxy_replaces_any_export(self):
        """With a payload proxy, the setup unsets the proxy and every export is removed from the payload command."""
        payload = os.path.join(self.workdir, 'x509up_u1-payload.proxy')
        pilot_cache.payload_proxy = payload
        cmd = f'export X509_USER_PROXY={self.pilot_proxy};export X509_USER_PROXY=;./runGen.py'
        _, _, setup_cmd, cmd = container.update_for_user_proxy('setup;', cmd, is_analysis=True, queue_type='production',
                                                               workdir=self.workdir)
        self.assertTrue(setup_cmd.startswith('unset X509_USER_PROXY;export ALRB_CONT_PRESETUP='))
        self.assertEqual(cmd, './runGen.py')

    def test_user_job_without_job_proxy_is_reported(self):
        """A user job that has to use the pilot proxy is reported; a production job is not."""
        with patch.object(container.logger, 'warning') as mock_warning:
            _, _, setup_cmd, _ = container.update_for_user_proxy('setup;', 'run', is_analysis=True,
                                                                 queue_type='production', workdir=self.workdir)
        self.assertEqual(setup_cmd, f'export X509_USER_PROXY={self.pilot_proxy};setup;')
        mock_warning.assert_called_once()
        with patch.object(container.logger, 'warning') as mock_warning:
            container.update_for_user_proxy('setup;', 'run', is_analysis=False, queue_type='production', workdir=self.workdir)
        mock_warning.assert_not_called()

    def test_no_proxy_at_all(self):
        """Without any proxy in the environment, the commands are unchanged."""
        with patch.dict('os.environ', {'X509_USER_PROXY': ''}, clear=False):
            self.assertEqual(container.update_for_user_proxy('setup;', 'run', is_analysis=True, workdir=self.workdir),
                             (0, '', 'setup;', 'run'))


class TestContainerCommands(_ProxyTestCase):
    """The stage-in/out, debug command and file open container commands."""

    def _job(self):
        return SimpleNamespace(workdir=self.workdir, platform='el9',
                               infosys=SimpleNamespace(queuedata=SimpleNamespace(container_options='')))

    def _middleware_command(self, label='stage-in', proxy=True):
        with patch.object(container, 'get_middleware_container', return_value='el9'), \
             patch.object(container, 'get_middleware_container_script', return_value='content'), \
             patch.object(container, 'get_asetup', return_value='export ATLAS_LOCAL_ROOT_BASE=/cvmfs;'), \
             patch.object(container, 'get_container_options', return_value='-e "-c"'):
            return container.create_middleware_container_command(self._job(), 'cmd', label=label, proxy=proxy)

    def test_stagein_gets_copy_via_presetup(self):
        """Stage-in: the pilot proxy is never exported; the presetup precedes the ALRB setup."""
        cmd = self._middleware_command()
        self.assertNotIn('export X509_USER_PROXY', cmd)
        self.assertIn('unset X509_USER_PROXY;export ALRB_CONT_PRESETUP="source /srv/pilot_proxy_presetup_stage-in.sh";', cmd)
        self.assertLess(cmd.index('ALRB_CONT_PRESETUP'), cmd.index('atlasLocalSetup.sh'))
        self.assertTrue(os.path.exists(self.copy_path('stage-in')))

    def test_unified_stageout_uses_user_proxy(self):
        """Unified stage-out (X509_USER_PROXY switched to the user proxy in the work directory): no copy."""
        unified = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        with open(unified, 'w', encoding='utf-8') as _file:
            _file.write('user')
        with patch.dict('os.environ', {'X509_USER_PROXY': unified}, clear=False):
            cmd = self._middleware_command(label='stage-out')
        self.assertIn('unset X509_USER_PROXY;', cmd)
        self.assertIn('export X509_USER_PROXY=/srv/x509up_u1-unified.proxy\n',
                      self.read(os.path.join(self.workdir, 'pilot_proxy_presetup_stage-out.sh')))
        self.assertEqual(get_container_proxies(self.workdir), [])

    def test_no_proxy_container_unsets_proxy(self):
        """A container that must not get a proxy (debug command) unsets the inherited one."""
        cmd = self._middleware_command(label='debug', proxy=False)
        self.assertIn('unset X509_USER_PROXY;', cmd)
        self.assertNotIn('ALRB_CONT_PRESETUP', cmd)
        self.assertNotIn('export X509_USER_PROXY', cmd)
        self.assertEqual(get_container_proxies(self.workdir), [])

    def test_file_open_gets_copy_via_presetup(self):
        """File open test: the pilot proxy (or the unified user proxy if set) is given via the presetup."""
        with patch.object(container, 'get_asetup', return_value='export ATLAS_LOCAL_ROOT_BASE=/cvmfs;'):
            cmd = container.create_root_container_command(self.workdir, 'python3 open.py', 'REPLACE_ME_FOR_CMD')
        self.assertNotIn('export X509_USER_PROXY', cmd)
        self.assertIn('unset X509_USER_PROXY;export ALRB_CONT_PRESETUP="source /srv/pilot_proxy_presetup_file-open.sh";', cmd)
        self.assertTrue(os.path.exists(self.copy_path('file-open')))

        remove_container_proxies(self.workdir)
        unified = os.path.join(self.workdir, 'x509up_u1-unified.proxy')
        with patch.dict('os.environ', {'X509_UNIFIED_DISPATCH': unified}, clear=False), \
             patch.object(container, 'get_asetup', return_value='export ATLAS_LOCAL_ROOT_BASE=/cvmfs;'):
            container.create_root_container_command(self.workdir, 'python3 open.py', 'REPLACE_ME_FOR_CMD')
        self.assertIn('export X509_USER_PROXY=/srv/x509up_u1-unified.proxy\n',
                      self.read(os.path.join(self.workdir, 'pilot_proxy_presetup_file-open.sh')))
        self.assertEqual(get_container_proxies(self.workdir), [])


class TestCopiesRemovedAfterContainer(unittest.TestCase):
    """The callers remove the container copies once the container has finished, whatever the outcome."""

    def setUp(self):
        """Create a work directory."""
        self.workdir = tempfile.mkdtemp()

    def tearDown(self):
        """Remove the work directory."""
        shutil.rmtree(self.workdir, ignore_errors=True)

    def _containerise(self, execute=None, write_file=None):
        job = SimpleNamespace(workdir=self.workdir)
        args = SimpleNamespace(input_dir='', output_dir='', queue='q', rucio_host='', stageout_attempts=1)
        with patch.dict('os.environ', {'PILOT_USER': 'atlas'}, clear=False), \
             patch.object(middleware, 'get_command', return_value='cmd'), \
             patch.object(container, 'create_middleware_container_command', return_value='container cmd'), \
             patch.object(middleware, 'execute', execute or MagicMock(return_value=(0, 'out', 'err'))), \
             patch.object(middleware, 'write_file', write_file or MagicMock()), \
             patch.object(middleware, 'handle_updated_job_object'), \
             patch.object(middleware, 'remove_container_proxies') as mock_remove:
            try:
                middleware.containerise_middleware(job, args, [], 'get_sm', 'site', 'site', label='stage-in')
            finally:
                mock_remove.assert_called_once_with(self.workdir)

    def test_middleware_success(self):
        """After a successful stage-in."""
        self._containerise()

    def test_middleware_execute_exception(self):
        """After the container command raised."""
        self._containerise(execute=MagicMock(side_effect=RuntimeError('killed')))

    def test_middleware_failure_raises_after_cleanup(self):
        """Also when writing the logs fails and a StageInFailure is raised."""
        with self.assertRaises(StageInFailure):
            self._containerise(write_file=MagicMock(side_effect=PilotException('disk full')))

    def _open_remote_files(self, write_file=None, execute=None):
        home = os.path.join(self.workdir, 'home')
        scripts = os.path.join(home, 'pilot3', 'pilot', 'scripts')
        os.makedirs(scripts)
        for name in ('open_remote_file.py', 'open_file.sh'):
            with open(os.path.join(scripts, name), 'w', encoding='utf-8') as _file:
                _file.write('REPLACE_ME_FOR_CMD')
        env = {'PILOT_HOME': home, 'PYTHONPATH': os.environ.get('PYTHONPATH', '')}
        with patch.dict('os.environ', env, clear=False), \
             patch.object(common, 'extract_turls', return_value='root://host//file'), \
             patch.object(common, 'extract_rawfirst_turls', return_value=''), \
             patch.object(common, 'copy_pilot_source', return_value=''), \
             patch.object(common, 'get_file_open_command', return_value='python3 open_remote_file.py'), \
             patch.object(common, 'get_timeout_for_remoteio', return_value=10), \
             patch.object(common, 'create_root_container_command', return_value='container cmd'), \
             patch.object(common, 'write_file', write_file or MagicMock()), \
             patch.object(common, 'execute_remote_file_open', execute or MagicMock(return_value=(0, '', 1))), \
             patch.object(common, 'parse_remotefileverification_dictionary', return_value=(0, 'parsed', [])) as mock_parse, \
             patch.object(common, 'remove_container_proxies') as mock_remove:
            result = common.open_remote_files([], self.workdir, 1)
        mock_remove.assert_called_once_with(self.workdir)
        return result, mock_parse

    def test_file_open_success(self):
        """After a successful file open test (the result is taken from the verification dictionary)."""
        result, mock_parse = self._open_remote_files()
        self.assertEqual(result[:2], (0, 'parsed'))
        mock_parse.assert_called_once_with(self.workdir)

    def test_file_open_script_write_failure(self):
        """Also when the command script cannot be written (early return with the write diagnostics)."""
        result, mock_parse = self._open_remote_files(write_file=MagicMock(side_effect=FileHandlingFailure('ro')))
        self.assertEqual(result[0], 11)
        self.assertIn('failed to write file', result[1])
        mock_parse.assert_not_called()

    def test_file_open_execute_exception(self):
        """Also when the container command raised (its message reaches the error handling, not the early return)."""
        result, _ = self._open_remote_files(execute=MagicMock(side_effect=PilotException('timeout')))
        self.assertNotEqual(result[0], 0)
        self.assertIn('timeout', result[1])


if __name__ == '__main__':
    unittest.main()
