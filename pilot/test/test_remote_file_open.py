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
import queue
import shlex
import subprocess
import tempfile
import unittest

from pilot.util.filehandling import looks_like_root_file
from pilot.user.atlas.common import (
    extract_rawfirst_turls,
    get_file_open_command,
    get_timeout_for_remoteio,
    _TURL_CMDLINE_LIMIT,
)
from pilot.scripts import open_remote_file
from pilot.scripts.open_remote_file import (
    append_filetype_raw,
    get_args,
    get_file_lists,
    get_rawfirst_turls,
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
        cmd = get_file_open_command(self.script, make_turls(_TURL_CMDLINE_LIMIT + 1), nthreads=4,
                                    workdir=self.tmpdir)
        self.assertIn('--turl-file=', cmd)
        self.assertNotIn('--turls=', cmd)

    def test_long_list_writes_turls_txt(self):
        """turls.txt must be created in workdir for long lists."""
        get_file_open_command(self.script, make_turls(1000), nthreads=4, workdir=self.tmpdir)
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'turls.txt')))

    def test_long_list_turls_txt_content(self):
        """turls.txt must contain exactly one TURL per line, in order."""
        turl_list = make_turls(1000).split(',')
        get_file_open_command(self.script, ','.join(turl_list), nthreads=4, workdir=self.tmpdir)
        with open(os.path.join(self.tmpdir, 'turls.txt'), encoding='utf-8') as fh:
            written = [line.strip() for line in fh if line.strip()]
        self.assertEqual(written, turl_list)

    def test_long_list_turl_file_path_in_cmd(self):
        """The --turl-file argument must use the relative path ./turls.txt (container-safe)."""
        cmd = get_file_open_command(self.script, make_turls(1000), nthreads=4, workdir=self.tmpdir)
        self.assertIn("--turl-file='./turls.txt'", cmd)

    def test_long_list_turls_not_in_cmd(self):
        """The actual TURL strings must not appear inline in the command for long lists."""
        cmd = get_file_open_command(self.script, make_turls(1000), nthreads=4, workdir=self.tmpdir)
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
        """If turls.txt cannot be written (bad workdir), fall back gracefully to --turls inline."""
        cmd = get_file_open_command(self.script, make_turls(1000), nthreads=1,
                                    workdir='/nonexistent_dir')
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
        cmd = get_file_open_command(self.script, ','.join(original), nthreads=4,
                                    workdir=self.tmpdir)

        tokens = shlex.split(cmd)
        argv = [t for t in tokens[1:] if not t.startswith('1>') and not t.startswith('2>')]
        # --turl-file uses a relative path in the command; resolve it against tmpdir
        resolved_argv = []
        for tok in argv:
            if tok.startswith('--turl-file=./'):
                resolved_argv.append('--turl-file=' + os.path.join(self.tmpdir, tok[len('--turl-file=./'):]))
            else:
                resolved_argv.append(tok)
        args = get_args(resolved_argv)

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
        cmd = get_file_open_command(self.script, ','.join(original), nthreads=4,
                                    workdir=self.tmpdir)

        self.assertLess(len(cmd), arg_max,
                        f'command line ({len(cmd)} bytes) exceeds ARG_MAX ({arg_max} bytes)')
        self.assertIn('--turl-file=', cmd)
        self.assertEqual(original, recovered)


# ---------------------------------------------------------------------------
# Tests for append_filetype_raw()
# ---------------------------------------------------------------------------


class TestAppendFiletypeRaw(unittest.TestCase):
    """Unit tests for append_filetype_raw() in pilot/scripts/open_remote_file.py."""

    def test_plain_turl_uses_question_mark(self):
        """A turl without a query string must get the option after a '?'."""
        turl = 'root://eosatlas.cern.ch//eos/atlas/file.root'
        self.assertEqual(append_filetype_raw(turl), turl + '?filetype=raw')

    def test_existing_query_string_uses_ampersand(self):
        """A turl that already carries a query string must not get a second '?'."""
        turl = 'davs://webdav.example.cern.ch:2880/path/file.root?authz=Bearer%20abc'
        result = append_filetype_raw(turl)
        self.assertEqual(result, turl + '&filetype=raw')
        self.assertEqual(result.count('?'), 1,
                         'appending filetype=raw must not produce a second query separator')

    def test_append_is_idempotent(self):
        """Appending the option twice must not duplicate it."""
        turl = 'root://eosatlas.cern.ch//eos/atlas/file.root'
        once = append_filetype_raw(turl)
        self.assertEqual(append_filetype_raw(once), once)

    def test_xcache_proxy_prefix_preserved(self):
        """The ALRB_XCACHE_PROXY prefix must survive unchanged."""
        turl = '${ALRB_XCACHE_PROXY}root://eosatlas.cern.ch//eos/atlas/file.root'
        result = append_filetype_raw(turl)
        self.assertTrue(result.startswith('${ALRB_XCACHE_PROXY}'))
        self.assertTrue(result.endswith('?filetype=raw'))


# ---------------------------------------------------------------------------
# Tests for looks_like_root_file()
# ---------------------------------------------------------------------------


class TestLooksLikeRootFile(unittest.TestCase):
    """Unit tests for looks_like_root_file() in pilot/util/filehandling.py (called with LFNs)."""

    def test_pool_root_with_version_suffix(self):
        """A standard pool.root.1 file must be treated as a ROOT candidate."""
        self.assertTrue(looks_like_root_file('EVNT.12345678._000001.pool.root.1'))

    def test_raw_pool_root_is_root_candidate(self):
        """RAW.*.pool.root.1 is a ROOT file (ATLASPANDA-788) and must be tried with ROOT first."""
        self.assertTrue(looks_like_root_file('RAW.32340918._000001.pool.root.1'))

    def test_hdf5_is_not_root(self):
        """An h5 file (Rod's ML training case) must not be treated as a ROOT file."""
        self.assertFalse(looks_like_root_file('user.walkerr.training_data.h5'))

    def test_hdf5_with_version_suffix_is_not_root(self):
        """A Rucio version suffix must not hide the h5 extension."""
        self.assertFalse(looks_like_root_file('user.walkerr.training_data.h5.1'))

    def test_extension_matching_is_case_insensitive(self):
        """An upper-case extension must be recognised."""
        self.assertFalse(looks_like_root_file('TRAINING_DATA.H5'))

    def test_lib_tarball_is_not_root(self):
        """A lib tarball must not be treated as a ROOT file."""
        self.assertFalse(looks_like_root_file('user.walkerr.12345.lib.tgz'))

    def test_tar_gz_is_not_root(self):
        """A multi-part .tar.gz extension must be recognised."""
        self.assertFalse(looks_like_root_file('payload.tar.gz'))

    def test_unknown_extension_defaults_to_root_candidate(self):
        """An unrecognised extension must default to a ROOT candidate (conservative)."""
        self.assertTrue(looks_like_root_file(
            'data18_13TeV.00359541.physics_Main.daq.RAW._lb0316._SFO-2._0005.data'))

    def test_no_extension_defaults_to_root_candidate(self):
        """A file name without any extension must default to a ROOT candidate."""
        self.assertTrue(looks_like_root_file('nodots'))


# ---------------------------------------------------------------------------
# Tests for try_open_file() open-mode selection
# ---------------------------------------------------------------------------


class _FakeTFile:
    """Minimal stand-in for a ROOT TFile object."""

    def __init__(self, is_open: bool):
        """Store the desired open state."""
        self._is_open = is_open

    def IsOpen(self):  # noqa: N802  (mirrors the ROOT API)
        """Return whether the file is considered open."""
        return self._is_open

    def Close(self):  # noqa: N802  (mirrors the ROOT API)
        """No-op close."""


class _FakeROOT:
    """Minimal stand-in for the ROOT module, recording every open attempt."""

    def __init__(self, openable: set):
        """Store the set of paths that should open successfully."""
        self.openable = openable
        self.attempts = []
        self.TFile = self  # noqa: N803  (ROOT.TFile.Open / ROOT.TFile.SetOpenTimeout)

    def SetOpenTimeout(self, _milliseconds):  # noqa: N802  (mirrors the ROOT API)
        """Accept and ignore the time-out setting."""
        return 0

    def Open(self, path):  # noqa: N802  (mirrors the ROOT API)
        """Record the attempt and return a fake file if the path is openable."""
        self.attempts.append(path)

        return _FakeTFile(True) if path in self.openable else None


class _Queues:
    """Container mirroring the namedtuple of queues used by try_open_file()."""

    def __init__(self):
        """Create the three queues."""
        self.result = queue.Queue()
        self.opened = queue.Queue()
        self.unopened = queue.Queue()


class TestTryOpenFileModeSelection(unittest.TestCase):
    """Tests that try_open_file() picks the right open mode and never fails a readable file."""

    root_turl = 'root://eosatlas.cern.ch//eos/atlas/EVNT.12345678._000001.pool.root.1'
    h5_turl = 'root://eosatlas.cern.ch//eos/atlas/user.walkerr.training_data.h5'

    def setUp(self):
        """Silence message() and remember the real ROOT placeholder."""
        self._real_root = open_remote_file.ROOT
        self._real_message = open_remote_file.message
        self.messages = []
        open_remote_file.message = self.messages.append

    def tearDown(self):
        """Restore the patched module attributes."""
        open_remote_file.ROOT = self._real_root
        open_remote_file.message = self._real_message

    def _run(self, turl: str, openable: set, rawfirst: set = None):
        """Run try_open_file() against a fake ROOT and return (fake_root, queues)."""
        fake = _FakeROOT(openable)
        open_remote_file.ROOT = fake
        queues = _Queues()
        open_remote_file.try_open_file(turl, queues, rawfirst_turls=rawfirst)

        return fake, queues

    def test_root_file_opens_on_first_attempt_without_raw_retry(self):
        """A ROOT file that opens normally must not trigger the raw retry."""
        fake, queues = self._run(self.root_turl, {self.root_turl})
        self.assertEqual(fake.attempts, [self.root_turl])
        self.assertEqual(queues.opened.get_nowait(), self.root_turl)
        self.assertTrue(queues.unopened.empty())

    def test_root_file_falls_back_to_raw_mode(self):
        """A ROOT-named file that only opens in raw mode must be reported as opened."""
        raw = self.root_turl + '?filetype=raw'
        fake, queues = self._run(self.root_turl, {raw})
        self.assertEqual(fake.attempts, [self.root_turl, raw])
        self.assertEqual(queues.opened.get_nowait(), raw)
        self.assertTrue(queues.unopened.empty())

    def test_rawfirst_turl_tries_raw_mode_first_only(self):
        """A turl listed as raw-first must be opened in raw mode, skipping the ROOT attempt."""
        raw = self.h5_turl + '?filetype=raw'
        fake, queues = self._run(self.h5_turl, {raw}, rawfirst={self.h5_turl})
        self.assertEqual(fake.attempts, [raw],
                         'a known non-ROOT file must not spend an attempt on the ROOT open')
        self.assertEqual(queues.opened.get_nowait(), raw)
        self.assertTrue(queues.unopened.empty())

    def test_non_root_file_does_not_fail_the_check_without_being_listed(self):
        """A readable non-ROOT file must pass even if the pilot did not list it (ATLASPANDA-788)."""
        _, queues = self._run(self.h5_turl, {self.h5_turl + '?filetype=raw'})
        self.assertTrue(queues.unopened.empty(),
                        'the raw fallback must protect non-ROOT input regardless of ordering')

    def test_rawfirst_turl_still_falls_back_to_root_mode(self):
        """If raw mode fails, the plain turl must still be attempted."""
        raw = self.h5_turl + '?filetype=raw'
        fake, queues = self._run(self.h5_turl, {self.h5_turl}, rawfirst={self.h5_turl})
        self.assertEqual(fake.attempts, [raw, self.h5_turl])
        self.assertEqual(queues.opened.get_nowait(), self.h5_turl)

    def test_turl_not_listed_uses_root_mode_first(self):
        """An unlisted turl must keep the ROOT-first order even with a non-ROOT-looking name."""
        raw = self.h5_turl + '?filetype=raw'
        fake, _ = self._run(self.h5_turl, {raw}, rawfirst={'root://other//file.h5'})
        self.assertEqual(fake.attempts, [self.h5_turl, raw],
                         'the script must not second-guess the pilot from the turl')

    def test_unopenable_file_reports_original_turl(self):
        """A file that opens in neither mode must be queued as the unmodified turl."""
        fake, queues = self._run(self.root_turl, set())
        self.assertEqual(fake.attempts,
                         [self.root_turl, self.root_turl + '?filetype=raw'])
        self.assertTrue(queues.opened.empty())
        self.assertEqual(queues.unopened.get_nowait(), self.root_turl,
                         'the unopened queue must hold the original turl so that trace '
                         'reporting can match it against fspec.turl')

    def test_result_queue_always_holds_original_turl(self):
        """The result queue drives thread scheduling and must always get the original turl."""
        for openable in (set(), {self.h5_turl + '?filetype=raw'}):
            _, queues = self._run(self.h5_turl, openable, rawfirst={self.h5_turl})
            self.assertEqual(queues.result.get_nowait(), self.h5_turl)


# ---------------------------------------------------------------------------
# Tests for get_timeout_for_remoteio()
# ---------------------------------------------------------------------------


class _StubFileSpec:
    """Minimal FileSpec stand-in carrying a transfer status, LFN and turl."""

    def __init__(self, status: str, lfn: str = '', turl: str = ''):
        """Store the file status, LFN and turl."""
        self.status = status
        self.lfn = lfn
        self.turl = turl


class TestExtractRawfirstTurls(unittest.TestCase):
    """Unit tests for extract_rawfirst_turls() in pilot/user/atlas/common.py."""

    root_file = _StubFileSpec('remote_io', 'EVNT.12345678._000001.pool.root.1',
                              'root://eosatlas.cern.ch//eos/atlas/EVNT.12345678._000001.pool.root.1')
    h5_file = _StubFileSpec('remote_io', 'user.walkerr.training_data.h5',
                            'root://eosatlas.cern.ch//eos/atlas/user.walkerr.training_data.h5')

    def test_root_input_only_yields_empty_list(self):
        """Input that may be in ROOT format must not be listed."""
        self.assertEqual(extract_rawfirst_turls([self.root_file]), '')

    def test_non_root_input_is_listed_by_turl(self):
        """A non-ROOT LFN must contribute its turl to the list."""
        self.assertEqual(extract_rawfirst_turls([self.root_file, self.h5_file]),
                         self.h5_file.turl)

    def test_copy_to_scratch_input_is_excluded(self):
        """Only direct i/o files are verified, so only they may be listed."""
        staged = _StubFileSpec('no_transfer', 'user.walkerr.other.h5', 'root://host//other.h5')
        self.assertEqual(extract_rawfirst_turls([staged]), '')

    def test_decision_uses_the_lfn_not_the_turl(self):
        """A non-deterministic PFN must not defeat the classification (regression)."""
        fspec = _StubFileSpec('remote_io', 'user.walkerr.training_data.h5',
                              'root://tape.example.cern.ch//opaque/0a/3f/blob00417')
        self.assertEqual(extract_rawfirst_turls([fspec]), fspec.turl,
                         'the LFN, not the turl, must decide whether the file is ROOT format')

    def test_root_lfn_with_non_root_looking_pfn_is_not_listed(self):
        """A ROOT LFN must not be listed even if its PFN path ends in a non-ROOT extension."""
        fspec = _StubFileSpec('remote_io', 'EVNT.12345678._000001.pool.root.1',
                              'root://cache.example.cern.ch//spool/staged/blob.gz')
        self.assertEqual(extract_rawfirst_turls([fspec]), '')


class TestGetRawfirstTurls(unittest.TestCase):
    """Unit tests for get_rawfirst_turls() in pilot/scripts/open_remote_file.py."""

    def test_absent_list_yields_empty_set(self):
        """No raw-first list supplied must yield an empty set, not a guess."""
        self.assertEqual(get_rawfirst_turls(None), set())

    def test_comma_string_parsed_into_set(self):
        """A comma-separated list must be parsed into a set of turls."""
        self.assertEqual(get_rawfirst_turls('root://a//f.h5,root://b//g.h5'),
                         {'root://a//f.h5', 'root://b//g.h5'})

    def test_file_takes_priority_over_string(self):
        """The file form must take priority over the inline form."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, 'rawfirst.txt')
            with open(path, 'w', encoding='utf-8') as fh:
                fh.write('root://c//h.h5\n\nroot://d//i.h5\n')
            self.assertEqual(get_rawfirst_turls('root://a//f.h5', rawfirst_file=path),
                             {'root://c//h.h5', 'root://d//i.h5'})


class TestRawfirstCommandLine(unittest.TestCase):
    """Tests that the raw-first list reaches the file open script correctly."""

    def setUp(self):
        """Create a temporary directory with a dummy script file."""
        self.tmpdir = tempfile.mkdtemp()
        self.script = os.path.join(self.tmpdir, 'open_remote_file.py')
        open(self.script, 'w').close()

    def test_no_rawfirst_option_when_list_is_empty(self):
        """An empty raw-first list must not add any option to the command."""
        cmd = get_file_open_command(self.script, 'root://a//f.root', nthreads=1,
                                    workdir=self.tmpdir, rawfirst_turls='')
        self.assertNotIn('--rawfirst', cmd)

    def test_short_rawfirst_list_passed_inline(self):
        """A short raw-first list must be passed inline via --rawfirst."""
        cmd = get_file_open_command(self.script, 'root://a//f.root,root://b//g.h5', nthreads=1,
                                    workdir=self.tmpdir, rawfirst_turls='root://b//g.h5')
        self.assertIn("--rawfirst='root://b//g.h5'", cmd)
        self.assertNotIn('--rawfirst-file', cmd)

    def test_long_rawfirst_list_passed_by_file(self):
        """A long raw-first list must be written to rawfirst.txt and passed by reference."""
        turls = [f'root://host//file_{i:04d}.h5' for i in range(_TURL_CMDLINE_LIMIT + 1)]
        cmd = get_file_open_command(self.script, ','.join(turls), nthreads=1,
                                    workdir=self.tmpdir, rawfirst_turls=','.join(turls))
        self.assertIn("--rawfirst-file='./rawfirst.txt'", cmd)
        written = os.path.join(self.tmpdir, 'rawfirst.txt')
        self.assertTrue(os.path.exists(written))
        with open(written, encoding='utf-8') as fh:
            self.assertEqual([line.strip() for line in fh if line.strip()], turls)

    def test_rawfirst_and_turls_use_separate_files(self):
        """The turl list and the raw-first list must not overwrite each other's file."""
        turls = [f'root://host//file_{i:04d}.h5' for i in range(_TURL_CMDLINE_LIMIT + 1)]
        get_file_open_command(self.script, ','.join(turls), nthreads=1,
                              workdir=self.tmpdir, rawfirst_turls=','.join(turls[:1]))
        self.assertTrue(os.path.exists(os.path.join(self.tmpdir, 'turls.txt')))
        self.assertFalse(os.path.exists(os.path.join(self.tmpdir, 'rawfirst.txt')),
                         'a short raw-first list must stay inline')

    def test_round_trip_through_the_script_arguments(self):
        """The raw-first list must survive the command-build/parse round-trip."""
        cmd = get_file_open_command(self.script, 'root://a//f.root,root://b//g.h5', nthreads=1,
                                    workdir=self.tmpdir, rawfirst_turls='root://b//g.h5')
        tokens = shlex.split(cmd)
        argv = [t for t in tokens[1:] if not t.startswith('1>') and not t.startswith('2>')]
        args = get_args(argv)
        self.assertEqual(get_rawfirst_turls(args.rawfirst, rawfirst_file=args.rawfirst_file),
                         {'root://b//g.h5'})


class TestGetTimeoutForRemoteio(unittest.TestCase):
    """Unit tests for get_timeout_for_remoteio() in pilot/user/atlas/common.py."""

    def test_no_input_files_returns_base_timeout(self):
        """An empty input list must yield only the base time-out."""
        self.assertEqual(get_timeout_for_remoteio([]), 900)

    def test_no_remote_io_files_returns_base_timeout(self):
        """Copy-to-scratch files alone must not extend the time-out."""
        indata = [_StubFileSpec('no_transfer'), _StubFileSpec('transferred')]
        self.assertEqual(get_timeout_for_remoteio(indata), 900)

    def test_budget_is_sixty_seconds_per_remote_io_file(self):
        """Each remote i/o file must be budgeted for both open attempts (2 x 30 s)."""
        indata = [_StubFileSpec('remote_io') for _ in range(3)]
        self.assertEqual(get_timeout_for_remoteio(indata), 3 * 60 + 900)

    def test_copy_to_scratch_files_are_not_counted(self):
        """Only remote i/o files may contribute to the time-out (regression)."""
        indata = [_StubFileSpec('remote_io'),
                  _StubFileSpec('no_transfer'),
                  _StubFileSpec('no_transfer'),
                  _StubFileSpec('transferred')]
        self.assertEqual(get_timeout_for_remoteio(indata), 1 * 60 + 900,
                         'files not using remote i/o must not inflate the budget')


if __name__ == '__main__':
    unittest.main()
