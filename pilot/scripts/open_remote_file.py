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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-26

"""Script for remote file open verification."""

from __future__ import annotations
import argparse
import functools
import os
import logging
import queue
import signal
import subprocess
import sys
import threading
import traceback
from collections import namedtuple
from typing import Any

try:
    import ROOT  # optional runtime dependency; only available in ATLAS environments
except ModuleNotFoundError:
    ROOT = None  # type: ignore[assignment]

from pilot.util.config import config
from pilot.util.filehandling import write_json
from pilot.util.loggingsupport import (
    establish_logging,
    flush_handler,
)
from pilot.util.processes import kill_processes

logger = logging.getLogger(__name__)


def get_args(argv: list = None) -> argparse.Namespace:
    """Return the args from the arg parser.

    Args:
        argv: optional list of arguments to parse (defaults to sys.argv when None,
              which is the normal runtime behaviour). Pass an explicit list in tests.

    Returns:
        argparse.Namespace: Parsed argument namespace.
    """
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('-d',
                            dest='debug',
                            action='store_true',
                            default=False,
                            help='Enable debug mode for logging messages')
    arg_parser.add_argument('-t',
                            dest='nthreads',
                            default=1,
                            required=False,
                            type=int,
                            help='Number of concurrent file open threads')
    arg_parser.add_argument('-w',
                            dest='workdir',
                            required=False,
                            default=os.getcwd(),
                            help='Working directory')
    arg_parser.add_argument('--turls',
                            dest='turls',
                            required=False,          # no longer required; --turl-file is the alternative
                            default=None,
                            help='TURL list (e.g., filepath1,filepath2)')
    arg_parser.add_argument('--turl-file',
                            dest='turl_file',
                            required=False,
                            default=None,
                            help='Path to a file containing one TURL per line (alternative to --turls)')
    arg_parser.add_argument('--rawfirst',
                            dest='rawfirst',
                            required=False,
                            default=None,
                            help='TURLs of input that is known not to be in ROOT format, to be opened '
                                 'in raw mode first (e.g. filepath1,filepath2)')
    arg_parser.add_argument('--rawfirst-file',
                            dest='rawfirst_file',
                            required=False,
                            default=None,
                            help='Path to a file containing one raw-first TURL per line (alternative to --rawfirst)')
    arg_parser.add_argument('--no-pilot-log',
                            dest='nopilotlog',
                            action='store_true',
                            default=False,
                            help='Do not write the pilot log to file')

    args = arg_parser.parse_args(argv)
    if not args.turls and not args.turl_file:
        arg_parser.error('one of --turls or --turl-file is required')
    return args


def read_turl_list(turls_string: str, turl_file: str = None, quiet: bool = False) -> list:
    """Return a list of turls read from a comma-separated string or from a file.

    The file takes priority when both are provided.

    Args:
        turls_string: Comma-separated turls, or None.
        turl_file: Path to file with one turl per line, or None.
        quiet: If True, do not report an unexpected turls_string type (used for the
               optional raw-first list, which is legitimately absent).

    Returns:
        list: Turls.
    """
    if turl_file:
        try:
            with open(turl_file, encoding='utf-8') as fh:
                return [line.strip() for line in fh if line.strip()]
        except OSError as exc:
            message(f"failed to read turl file {turl_file!r}: {exc}")
            return []

    if isinstance(turls_string, str):
        return [turl.strip() for turl in turls_string.split(',') if turl.strip()]

    if not quiet:
        message(f"unexpected type for turls_string: {type(turls_string).__name__}")

    return []


def get_file_lists(turls_string: str, turl_file: str = None) -> dict:
    """Return a dictionary with the turls.

    Format: {'turls': <turl list>}

    Turls can be supplied either as a comma-separated string (turls_string) or
    as a path to a plain-text file containing one TURL per line (turl_file).
    turl_file takes priority when both are provided.

    Args:
        turls_string: Comma-separated turls, or None.
        turl_file: Path to file with one TURL per line, or None.

    Returns:
        dict: Turls dictionary.
    """
    return {'turls': read_turl_list(turls_string, turl_file=turl_file)}


def get_rawfirst_turls(rawfirst_string: str, rawfirst_file: str = None) -> set:
    """Return the set of turls that should be opened in raw mode first.

    The pilot determines this set from the LFN of each input file (the authoritative file
    name) and passes it in explicitly; the script does not attempt to infer it from the
    turls, since the trailing path component of a replica PFN is the LFN only for
    deterministically named replicas.

    Args:
        rawfirst_string: Comma-separated turls, or None if not supplied.
        rawfirst_file: Path to file with one turl per line, or None if not supplied.

    Returns:
        set: Turls to be opened in raw mode first (empty if none were supplied).
    """
    return set(read_turl_list(rawfirst_string, turl_file=rawfirst_file, quiet=True))


def message(msg: str) -> None:
    """Print message to stdout or to log.

    Args:
        msg: Message to print.
    """
    if logger:
        logger.info(msg)
        # make sure that stdout buffer gets flushed - in case of time-out exceptions
        flush_handler(name="stream_handler")
    else:
        print(msg, flush=True)

    # always write message to instant log file (message might otherwise get lost in case of time-outs)
    with open(config.Pilot.remotefileverification_instant, 'a', encoding='utf-8') as _file:
        _file.write(msg + '\n')


def append_filetype_raw(turl_str: str) -> str:
    """Return the turl with ROOT's ``filetype=raw`` option appended.

    ``filetype=raw`` instructs ROOT to open the file without interpreting it as a ROOT
    file, which is what allows non-ROOT input (e.g. HDF5 used for ML training) to be
    verified at all. The option is appended as a URL query parameter, using ``&`` when the
    turl already carries a query string so that signed or otherwise parameterised PFNs are
    not corrupted. Appending is idempotent.

    Args:
        turl_str: TURL string.

    Returns:
        str: TURL with the filetype=raw option present exactly once.
    """
    if 'filetype=raw' in turl_str:
        return turl_str

    separator = '&' if '?' in turl_str else '?'

    return f'{turl_str}{separator}filetype=raw'


# pylint: disable=useless-param-doc
def try_open_file(turl_str: str, _queues: namedtuple, rawfirst_turls: set = None) -> None:
    """Attempt to open a remote file.

    Two open modes are attempted: the plain TURL (ROOT format) and the TURL with
    ``filetype=raw`` appended (raw byte access, which succeeds for any readable file).
    The file is considered verified if either mode succeeds — the purpose of the check is to
    confirm that the payload will be able to read the file, and a non-ROOT input must not
    fail the job merely because it cannot be parsed by ROOT.

    TURLs listed in ``rawfirst_turls`` are attempted in raw mode first. The pilot builds
    that set from the LFNs of the input files. This is not a large time saving: a ROOT open
    of a non-ROOT file fails on the format check as soon as the header has been read, so the
    doomed attempt normally costs one open round trip rather than the 30 s open time-out set
    below (which is only reached when the endpoint itself is slow or unresponsive, in which
    case both modes are equally slow and reordering saves nothing). The reason to reorder is
    that the verification log then no longer contains a ROOT format error for input that is
    perfectly readable, which is easily misread as the cause of a failure.

    The turl of a successfully opened file is put in the queues.opened queue, in the form
    that actually worked. Files that could not be opened in either mode are put in the
    queues.unopened queue, always as the original unmodified turl.

    Args:
        turl_str: TURL string.
        _queues: Namedtuple with 'opened', 'unopened', 'result' queues.
        rawfirst_turls: Set of turls to be opened in raw mode first, or None.
    """

    def attempt_open(path: str) -> bool:
        """Return True if ROOT successfully opens the file."""
        try:
            message(f'opening {path}')
            _ = ROOT.TFile.SetOpenTimeout(
                30 * 1000)  # 30 seconds
            in_file = ROOT.TFile.Open(path)
        except Exception as exc:
            message(f'caught exception: {exc}')
            return False

        if in_file and in_file.IsOpen():
            in_file.Close()
            message(f'closed {path}')
            return True

        return False

    raw_turl = append_filetype_raw(turl_str)
    if rawfirst_turls and turl_str in rawfirst_turls:
        message(f'{turl_str} is not in ROOT format - will attempt raw mode first')
        attempts = [raw_turl, turl_str]
    else:
        attempts = [turl_str, raw_turl]

    opened_path = ''
    for position, path in enumerate(attempts):
        if attempt_open(path):
            opened_path = path
            break
        if position + 1 < len(attempts):
            message(f'failed to open {path} - retrying with {attempts[position + 1]}')

    if opened_path:
        _queues.opened.put(opened_path)
    else:
        message(f'failed to open {turl_str} in all {len(attempts)} attempted modes')
        _queues.unopened.put(turl_str)

    _queues.result.put(turl_str)


# pylint: disable=useless-param-doc
def spawn_file_open_thread(_queues: Any, file_list: list, rawfirst_turls: set = None) -> threading.Thread:
    """Spawn a thread for the try_open_file().

    Args:
        _queues: Queue collection.
        file_list: Files to open.
        rawfirst_turls: Set of turls to be opened in raw mode first, or None.

    Returns:
        threading.Thread: The spawned thread.
    """
    _thread = None
    try:
        _turl = file_list.pop(0)
    except IndexError:
        pass
    else:
        # create and start thread for the current turl
        _thread = threading.Thread(target=try_open_file, args=(_turl, _queues, rawfirst_turls))
        _thread.daemon = True
        _thread.start()

    return _thread


def register_signals(signals: list, _args: Any) -> None:
    """Register kill signals for intercept function.

    Args:
        signals: List of signals.
        _args: Pilot arguments object.
    """
    for sig in signals:
        signal.signal(sig, functools.partial(interrupt, _args))


def interrupt(_args: Any, signum: Any, frame: Any) -> None:
    """Receive and handle kill signals.

    Interrupt function on the receiving end of kill signals.
    This function is forwarded any incoming signals (SIGINT, SIGTERM, etc) and will set abort_job which instructs
    the threads to abort the job.

    Args:
        _args: Pilot arguments object.
        signum: Signal number.
        frame: Stack/execution frame pointing to the frame that was interrupted by the signal.
    """
    if _args.signal:
        logger.warning('process already being killed')
        return

    sig = [v for v, k in list(signal.__dict__.items()) if k == signum][0]
    tmp = '\n'.join(traceback.format_stack(frame))
    logger.warning(f'caught signal: {sig} in FRAME=\n{tmp}')
    cmd = f'ps aux | grep {os.getpid()}'
    out = subprocess.getoutput(cmd)
    logger.info(f'{cmd}:\n{out}')
    logger.warning(f'will terminate pid={os.getpid()}')
    logging.shutdown()
    _args.signal = sig
    kill_processes(os.getpid())


if __name__ == '__main__':  # noqa: C901
    # get the args from the arg parser
    args = get_args()
    args.debug = True
    args.nopilotlog = False
    args.signal = None

    try:
        logname = config.Pilot.remotefileverification_log
    except AttributeError as error:
        print(f"caught exception: {error} (skipping remote file open verification)")
        sys.exit(1)
    else:
        if not logname:
            print("remote file open verification not desired")
            sys.exit(0)

    establish_logging(debug=args.debug, nopilotlog=args.nopilotlog, filename=logname)
    logger = logging.getLogger(__name__)

    logger.info('setting up signal handling')
    register_signals([signal.SIGINT, signal.SIGTERM, signal.SIGQUIT, signal.SIGSEGV, signal.SIGXCPU, signal.SIGUSR1, signal.SIGBUS], args)

    # get the file info
    file_list_dictionary = get_file_lists(args.turls, turl_file=args.turl_file)
    turls = file_list_dictionary.get('turls')
    rawfirst = get_rawfirst_turls(args.rawfirst, rawfirst_file=args.rawfirst_file)
    if rawfirst:
        message(f'{len(rawfirst)} file(s) reported by the pilot as not being in ROOT format')
    processed_turls_dictionary = {}

    queues = namedtuple('queues', ['result', 'opened', 'unopened'])
    queues.result = queue.Queue()
    queues.opened = queue.Queue()
    queues.unopened = queue.Queue()
    threads = []

    message(f'will attempt to open {len(turls)} file(s) using {args.nthreads} thread(s)')

    if turls:
        # make N calls to begin with
        for index in range(args.nthreads):
            thread = spawn_file_open_thread(queues, turls, rawfirst_turls=rawfirst)
            if thread:
                threads.append(thread)

        while turls:

            try:
                _ = queues.result.get(block=True)
            except queue.Empty:
                message("reached time-out")
                break

            thread = spawn_file_open_thread(queues, turls, rawfirst_turls=rawfirst)
            if thread:
                threads.append(thread)

        # wait until all threads have finished
        for thread in threads:
            thread.join()
        logger.info('all remote file open threads have been joined')

        opened_turls = list(queues.opened.queue)
        opened_turls.sort()
        unopened_turls = list(queues.unopened.queue)
        unopened_turls.sort()

        for turl in opened_turls:
            processed_turls_dictionary[turl] = True
        for turl in unopened_turls:
            processed_turls_dictionary[turl] = False

        # write dictionary to file with results
        write_json(os.path.join(args.workdir, config.Pilot.remotefileverification_dictionary), processed_turls_dictionary)
    else:
        message('no TURLs to verify')

    message('file remote open script has finished')
    sys.exit(0)
