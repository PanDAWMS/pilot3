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
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-24

"""Script for remote file open verification."""

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

import ROOT

from pilot.util.config import config
from pilot.util.filehandling import write_json
from pilot.util.loggingsupport import (
    establish_logging,
    flush_handler,
)
from pilot.util.processes import kill_processes

logger = logging.getLogger(__name__)


def get_args() -> argparse.Namespace:
    """
    Return the args from the arg parser.

    :return: args (arg parser object).
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
                            required=True,
                            help='TURL list (e.g., filepath1,filepath2')
    arg_parser.add_argument('--no-pilot-log',
                            dest='nopilotlog',
                            action='store_true',
                            default=False,
                            help='Do not write the pilot log to file')

    return arg_parser.parse_args()


def message(msg: str):
    """
    Print message to stdout or to log.

    :param msg: message (str).
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


def get_file_lists(turls_string: str) -> dict:
    """
    Return a dictionary with the turls.

    Format: {'turls': <turl list>}

    :param turls_string: comma separated turls (str)
    :return: turls dictionary (dict).
    """
    _turls = []

    if isinstance(turls_string, str):
        _turls = turls_string.split(',')
    else:
        message(f"unexpected type for turls_string: {type(turls_string).__name__}")

    return {'turls': _turls}


# pylint: disable=useless-param-doc
def try_open_file(turl_str: str, _queues: namedtuple):
    """
    Attempt to open a remote file.

    Successfully opened turls will be put in the queues.opened queue. Unsuccessful turls will be placed in
    the queues.unopened queue.

    :param turl_str: turl (str)
    :param _queues: Namedtuple containing queues for opened and unopened turls.
                    Should have 'opened' and 'unopened' attributes to store respective turls.
    """
    turl_opened = False
    _timeout = 30 * 1000  # 30 s per file
    try:
        _ = ROOT.TFile.SetOpenTimeout(_timeout)
        # message(f"internal TFile.Open() time-out set to {_timeout} ms")
        message(f'opening {turl_str}')
        in_file = ROOT.TFile.Open(turl_str)
    except Exception as e:
        message(f'caught exception: {e}')
    else:
        if in_file and in_file.IsOpen():
            in_file.Close()
            turl_opened = True
            message(f'closed {turl_str}')
    if turl_opened:
        _queues.opened.put(turl_str)
    else:
        _queues.unopened.put(turl_str)
    _queues.result.put(turl_str)


# pylint: disable=useless-param-doc
def spawn_file_open_thread(_queues: Any, file_list: list) -> threading.Thread:
    """
    Spawn a thread for the try_open_file()..

    :param _queues: queue collection (Any)
    :param file_list: files to open (list)
    :return: thread (threading.Thread).
    """
    _thread = None
    try:
        _turl = file_list.pop(0)
    except IndexError:
        pass
    else:
        # create and start thread for the current turl
        _thread = threading.Thread(target=try_open_file, args=(_turl, _queues))
        _thread.daemon = True
        _thread.start()

    return _thread


def register_signals(signals: list, _args: Any):
    """
    Register kill signals for intercept function.

    :param signals: list of signals (list)
    :param _args: pilot arguments object (Any).
    """
    for sig in signals:
        signal.signal(sig, functools.partial(interrupt, _args))


def interrupt(_args: Any, signum: Any, frame: Any):
    """
    Receive and handle kill signals.

    Interrupt function on the receiving end of kill signals.
    This function is forwarded any incoming signals (SIGINT, SIGTERM, etc) and will set abort_job which instructs
    the threads to abort the job.

    :param _args: pilot arguments object (Any).
    :param signum: signal.
    :param frame: stack/execution frame pointing to the frame that was interrupted by the signal.
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
    file_list_dictionary = get_file_lists(args.turls)
    turls = file_list_dictionary.get('turls')
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
            thread = spawn_file_open_thread(queues, turls)
            if thread:
                threads.append(thread)

        while turls:

            try:
                _ = queues.result.get(block=True)
            except queue.Empty:
                message("reached time-out")
                break

            thread = spawn_file_open_thread(queues, turls)
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
