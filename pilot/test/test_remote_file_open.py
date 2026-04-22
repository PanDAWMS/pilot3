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

"""Unit tests for remote file open TURL handling (--turls / --turl-file)."""

import os
import shlex
import subprocess
import tempfile
import unittest

from pilot.user.atlas.common import (
    get_file_open_command,
    _TURL_CMDLINE_LIMIT,
)
from pilot.scripts.open_remote_file import (
    get_args,
    get_file_lists,
)


def make_turls(n: int, long_form: bool = False) -> str:
    """
    Return a comma-separated string of n synthetic TURLs.

    :param n: number of TURLs (int)
    :param long_form: if True, use realistic long-form grid PFNs (bool)
    :return: comma-separated TURLs (str)
    """
    if long_form:
        turl = ('root://eosatlas.cern.ch//eos/atlas/atlasscratchdisk/rucio/'
                'mc21_13p6TeV/00/00/EVNT.12345678._000{:03d}.pool.root.1')
    else:
        turl = 'root://xrootd.example.cern.ch//atlas/file_{:04d}.root'
    return ','.join(turl.format(i) for i in range(n))


class TestGetFileOpenCommand(unittest.TestCase):
    """Unit tests for get_file_open_command() in pilot/user/atlas/common.py."""

    def setUp(self):
        """Create a temporary directory with a dummy script file."""
        self.tmpdir = tempfile.mkdtemp()
        self.script = os.path.join(self.tmpdir, 'open_remote_file.py')
        open(self.script, 'w').close()

    # -- short lists: TURLs must be passed inline via --turls ------------------

    def test_short_list_uses_turls_arg(self):
        """Short TURL list should use --turls on the command line."""
        cmd = get_file_open_command(self.script, make_turls(10), nthreads=4)
        self.assertIn('--turls=', cmd)
        self.assertNotIn('--turl-file=', cmd)

    def test_short_list_exact_limit_uses_turls_arg(self):
        """A list of exactly _TURL_CMDLINE_LIMIT should still use --turls."""
        cmd = get_file_open_command(self.script, make_turls(_TURL_CMDLINE_LIMIT), nthreads=1)
        self.assertIn('--turls=', cmd)
        self.assertNotIn('--turl-file=', cmd)

    def test_short_list_contains_all_turls_inline(self):
        """Every TURL must appear verbatim in the command for short lists."""
        turls = make_turls(5)
        cmd = get_file_open_command(self.script, turls, nthreads=1)
        for turl in turls.split(','):
            self.assertIn(turl, cmd)

    # -- long lists: must write turls.txt and switch to --turl-file ------------

    def test_long_list_uses_turl_file_arg(self):
        """Lists above _TURL_CMDLINE_LIMIT must use --turl-file."""
        cmd = get_file_open_command(self.script, make_turls(_TURL_CMDLINE_LIMIT + 1), nthreads=4)
        self.assertIn('--turl-file=', cmd)
        self.assertNotIn('--turls=', cmd)

    def test_long_list_writes_turls_txt(self):
        """turls.txt must be created next to the script for long lists."""
        get_file_open_command(self.script, make_turls(1000), nthreads=4)
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'turls.txt')))

    def test_long_list_turls_txt_content(self):
        """turls.txt must contain exactly one TURL per line, in order."""
        turl_list = make_turls(1000).split(',')
        get_file_open_command(self.script, ','.join(turl_list), nthreads=4)
        with open(os.path.join(self.tmpdir, 'turls.txt'), encoding='utf-8') as fh:
            written = [line.strip() for line in fh if line.strip()]
        self.assertEqual(written, turl_list)

    def test_long_list_turl_file_path_in_cmd(self):
        """The --turl-file argument must point to turls.txt inside the workdir."""
        cmd = get_file_open_command(self.script, make_turls(1000), nthreads=4)
        self.assertIn(os.path.join(self.tmpdir, 'turls.txt'), cmd)

    def test_long_list_turls_not_in_cmd(self):
        """The actual TURL strings must not appear inline in the command for long lists."""
        cmd = get_file_open_command(self.script, make_turls(1000), nthreads=4)
        self.assertNotIn('root://xrootd.example.cern.ch', cmd)

    # -- command structure sanity checks ---------------------------------------

    def test_script_path_in_cmd(self):
        """The script path must appear as the first token in the command."""
        cmd = get_file_open_command(self.script, make_turls(5), nthreads=2)
        self.assertIn(self.script, cmd)

    def test_workdir_in_cmd(self):
        """The -w flag must point to the directory containing the script."""
        cmd = get_file_open_command(self.script, make_turls(5), nthreads=2)
        self.assertIn(f'-w {self.tmpdir}', cmd)

    def test_nthreads_in_cmd(self):
        """The -t flag must reflect the nthreads argument."""
        cmd = get_file_open_command(self.script, make_turls(5), nthreads=7)
        self.assertIn('-t 7', cmd)

    def test_stdout_stderr_redirection_present(self):
        """Default stdout/stderr redirection tokens must appear in the command."""
        cmd = get_file_open_command(self.script, make_turls(5), nthreads=1)
        self.assertIn('1>remote_open.stdout', cmd)
        self.assertIn('2>remote_open.stderr', cmd)

    def test_stdout_stderr_redirection_suppressed(self):
        """Passing empty stdout/stderr must suppress the redirection tokens."""
        cmd = get_file_open_command(self.script, make_turls(5), nthreads=1, stdout='', stderr='')
        self.assertNotIn('1>', cmd)
        self.assertNotIn('2>', cmd)

    def test_write_failure_falls_back_to_turls(self):
        """If turls.txt cannot be written, fall back gracefully to --turls inline."""
        bad_script = '/nonexistent_dir/open_remote_file.py'
        cmd = get_file_open_command(bad_script, make_turls(1000), nthreads=1)
        self.assertIn('--turls=', cmd)
        self.assertNotIn('--turl-file=', cmd)


class TestGetFileLists(unittest.TestCase):
    """Unit tests for get_file_lists() in pilot/scripts/open_remote_file.py."""

    def setUp(self):
        """Create a temporary directory for TURL file fixtures."""
        self.tmpdir = tempfile.mkdtemp()

    def _write_turl_file(self, turls: list) -> str:
        """Write a list of TURLs to a temp file, one per line, and return the path."""
        path = os.path.join(self.tmpdir, 'turls.txt')
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write('\n'.join(turls))
        return path

    # -- comma-separated string path -------------------------------------------

    def test_comma_string_parsed_correctly(self):
        """A comma-separated string must be split into the correct list."""
        turls = make_turls(5).split(',')
        self.assertEqual(get_file_lists(','.join(turls))['turls'], turls)

    def test_single_turl_string(self):
        """A single TURL string (no commas) must return a one-element list."""
        turl = 'root://xrootd.example.cern.ch//atlas/file_0000.root'
        self.assertEqual(get_file_lists(turl)['turls'], [turl])

    def test_bad_type_returns_empty(self):
        """A non-string turls_string with no turl_file must return an empty list."""
        self.assertEqual(get_file_lists(None)['turls'], [])  # type: ignore[arg-type]

    # -- turl_file path --------------------------------------------------------

    def test_turl_file_parsed_correctly(self):
        """A TURL file must be read and returned as a list in the correct order."""
        turls = make_turls(1000).split(',')
        path = self._write_turl_file(turls)
        self.assertEqual(get_file_lists(None, turl_file=path)['turls'], turls)

    def test_turl_file_strips_blank_lines(self):
        """Blank lines in the TURL file must be ignored."""
        turls = ['root://a.cern.ch//f1.root', '', 'root://a.cern.ch//f2.root', '']
        path = self._write_turl_file(turls)
        self.assertEqual(get_file_lists(None, turl_file=path)['turls'],
                         [t for t in turls if t])

    def test_turl_file_takes_priority_over_string(self):
        """turl_file must take priority over turls_string when both are provided."""
        file_turls = make_turls(3).split(',')
        path = self._write_turl_file(file_turls)
        result = get_file_lists('root://other.cern.ch//other.root', turl_file=path)
        self.assertEqual(result['turls'], file_turls)

    def test_missing_turl_file_returns_empty(self):
        """A missing turl_file must return an empty list without raising."""
        self.assertEqual(get_file_lists(None, turl_file='/nonexistent/turls.txt')['turls'], [])

    def test_large_file_round_trips(self):
        """1000 TURLs written to a file must be read back exactly."""
        turls = make_turls(1000).split(',')
        path = self._write_turl_file(turls)
        result = get_file_lists(None, turl_file=path)
        self.assertEqual(len(result['turls']), 1000)
        self.assertEqual(result['turls'], turls)


class TestGetArgs(unittest.TestCase):
    """Unit tests for get_args() in pilot/scripts/open_remote_file.py."""

    def test_turls_only(self):
        """--turls alone must be accepted and turl_file must be None."""
        args = get_args(['--turls', 'root://a/f1.root,root://a/f2.root'])
        self.assertEqual(args.turls, 'root://a/f1.root,root://a/f2.root')
        self.assertIsNone(args.turl_file)

    def test_turl_file_only(self):
        """--turl-file alone must be accepted and turls must be None."""
        args = get_args(['--turl-file', '/tmp/turls.txt'])
        self.assertEqual(args.turl_file, '/tmp/turls.txt')
        self.assertIsNone(args.turls)

    def test_both_accepted(self):
        """Both --turls and --turl-file together must be accepted."""
        args = get_args(['--turls', 'root://a/f.root', '--turl-file', '/tmp/t.txt'])
        self.assertIsNotNone(args.turls)
        self.assertIsNotNone(args.turl_file)

    def test_neither_raises(self):
        """Supplying neither --turls nor --turl-file must exit with an error."""
        with self.assertRaises(SystemExit):
            get_args([])

    def test_nthreads_default(self):
        """The default value of -t must be 1."""
        args = get_args(['--turls', 'root://a/f.root'])
        self.assertEqual(args.nthreads, 1)

    def test_nthreads_set(self):
        """An explicit -t value must be reflected in args.nthreads."""
        args = get_args(['--turls', 'root://a/f.root', '-t', '8'])
        self.assertEqual(args.nthreads, 8)


class TestEndToEnd(unittest.TestCase):
    """
    Integration tests: simulate the full path from open_remote_files() in
    common.py building the command, through to open_remote_file.py parsing
    it and recovering the original TURL list.
    """

    def setUp(self):
        """Create a temporary directory with a dummy script file."""
        self.tmpdir = tempfile.mkdtemp()
        self.script = os.path.join(self.tmpdir, 'open_remote_file.py')
        open(self.script, 'w').close()

    def _round_trip(self, n_turls: int, long_form: bool = False):
        """
        Build a command for n_turls TURLs then parse it back, returning
        (original_list, recovered_list).

        :param n_turls: number of TURLs to test (int)
        :param long_form: use realistic long-form grid PFNs (bool)
        :return: (original turl list, recovered turl list) (tuple)
        """
        original = make_turls(n_turls, long_form=long_form).split(',')
        cmd = get_file_open_command(self.script, ','.join(original), nthreads=4)

        tokens = shlex.split(cmd)
        argv = [t for t in tokens[1:] if not t.startswith('1>') and not t.startswith('2>')]
        args = get_args(argv)

        return original, get_file_lists(args.turls, turl_file=args.turl_file)['turls']

    def test_short_round_trip(self):
        """10 TURLs must survive the command-build/parse round-trip unchanged."""
        original, recovered = self._round_trip(10)
        self.assertEqual(original, recovered)

    def test_boundary_round_trip(self):
        """Exactly _TURL_CMDLINE_LIMIT TURLs must survive the round-trip unchanged."""
        original, recovered = self._round_trip(_TURL_CMDLINE_LIMIT)
        self.assertEqual(original, recovered)

    def test_long_round_trip_501(self):
        """_TURL_CMDLINE_LIMIT + 1 TURLs must trigger --turl-file and round-trip correctly."""
        original, recovered = self._round_trip(_TURL_CMDLINE_LIMIT + 1)
        self.assertEqual(original, recovered)

    def test_long_round_trip_1000(self):
        """1000 TURLs must round-trip correctly via --turl-file."""
        original, recovered = self._round_trip(1000)
        self.assertEqual(original, recovered)

    def test_long_round_trip_5000(self):
        """5000 TURLs must round-trip correctly via --turl-file."""
        original, recovered = self._round_trip(5000)
        self.assertEqual(original, recovered)

    def test_command_line_stays_under_arg_max(self):
        """
        The command line must stay well under ARG_MAX even for 1000 long-form
        grid PFNs -- the scenario that triggered the original failure reported
        by Rod Walker (PanDA job 7060826009).
        """
        result = subprocess.run(['getconf', 'ARG_MAX'], capture_output=True, text=True)
        arg_max = int(result.stdout.strip())

        original, recovered = self._round_trip(1000, long_form=True)
        cmd = get_file_open_command(self.script, ','.join(original), nthreads=4)

        self.assertLess(len(cmd), arg_max,
                        f'command line ({len(cmd)} bytes) exceeds ARG_MAX ({arg_max} bytes)')
        self.assertIn('--turl-file=', cmd)
        self.assertEqual(original, recovered)


if __name__ == '__main__':
    unittest.main()
