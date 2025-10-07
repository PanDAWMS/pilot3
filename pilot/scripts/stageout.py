#do not use: #!/usr/bin/env python3
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

"""This script is executed by the pilot in a container to perform stage-out of output files."""

import argparse
import logging
import os
import re
import sys
import traceback

from pilot.api.data import StageOutClient
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.info import (
    infosys,
    InfoService,
    FileSpec,
)
from pilot.util.config import config
from pilot.util.https import https_setup
from pilot.util.filehandling import write_json
from pilot.util.loggingsupport import establish_logging
from pilot.util.tracereport import TraceReport

errors = ErrorCodes()

# error codes
GENERAL_ERROR = 1
NO_QUEUENAME = 2
NO_SCOPES = 3
NO_LFNS = 4
NO_EVENTTYPE = 5
NO_LOCALSITE = 6
NO_REMOTESITE = 7
NO_PRODUSERID = 8
NO_JOBID = 9
NO_TASKID = 10
NO_JOBDEFINITIONID = 11
NO_DDMENDPOINTS = 12
NO_DATASETS = 13
NO_GUIDS = 14
TRANSFER_ERROR = 15


def get_args() -> argparse.Namespace:
    """
    Return the args from the arg parser.

    :return: args (argparse.Namespace).
    """
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument('-d',
                            dest='debug',
                            action='store_true',
                            default=False,
                            help='Enable debug mode for logging messages')
    arg_parser.add_argument('-q',
                            dest='queuename',
                            required=True,
                            help='Queue name (e.g., AGLT2_TEST-condor')
    arg_parser.add_argument('-w',
                            dest='workdir',
                            required=False,
                            default=os.getcwd(),
                            help='Working directory')
    arg_parser.add_argument('--scopes',
                            dest='scopes',
                            required=True,
                            help='List of Rucio scopes (e.g., mc16_13TeV,mc16_13TeV')
    arg_parser.add_argument('--lfns',
                            dest='lfns',
                            required=True,
                            help='LFN list (e.g., filename1,filename2')
    arg_parser.add_argument('--eventtype',
                            dest='eventtype',
                            required=True,
                            help='Event type')
    arg_parser.add_argument('--ddmendpoints',
                            dest='ddmendpoints',
                            required=True,
                            help='DDM endpoint')
    arg_parser.add_argument('--datasets',
                            dest='datasets',
                            required=True,
                            help='Dataset')
    arg_parser.add_argument('--guids',
                            dest='guids',
                            required=True,
                            help='GUIDs')
    arg_parser.add_argument('--localsite',
                            dest='localsite',
                            required=True,
                            help='Local site')
    arg_parser.add_argument('--remotesite',
                            dest='remotesite',
                            required=True,
                            help='Remote site')
    arg_parser.add_argument('--produserid',
                            dest='produserid',
                            required=True,
                            help='produserid')
    arg_parser.add_argument('--jobid',
                            dest='jobid',
                            required=True,
                            help='PanDA job id')
    arg_parser.add_argument('--taskid',
                            dest='taskid',
                            required=True,
                            help='PanDA task id')
    arg_parser.add_argument('--jobdefinitionid',
                            dest='jobdefinitionid',
                            required=True,
                            help='Job definition id')
    arg_parser.add_argument('--eventservicemerge',
                            dest='eventservicemerge',
                            type=str2bool,
                            default=False,
                            help='Event service merge boolean')
    arg_parser.add_argument('--usepcache',
                            dest='usepcache',
                            type=str2bool,
                            default=False,
                            help='pcache boolean from queuedata')
    arg_parser.add_argument('--no-pilot-log',
                            dest='nopilotlog',
                            action='store_true',
                            default=False,
                            help='Do not write the pilot log to file')
    arg_parser.add_argument('--outputdir',
                            dest='outputdir',
                            required=False,
                            default='',
                            help='Output files directory')
    arg_parser.add_argument('--catchall',
                            dest='catchall',
                            required=False,
                            default='',
                            help='PQ catchall field')
    arg_parser.add_argument('--rucio_host',
                            dest='rucio_host',
                            required=False,
                            default='',
                            help='Optional rucio host')
    arg_parser.add_argument('--stageout-attempts',
                            dest='stageout_attempts',
                            required=False,
                            default='1',
                            help='Optional number of stage-out attempts (default: 1)')

    return arg_parser.parse_args()


# pylint: disable=useless-param-doc
def str2bool(_str: str) -> bool:
    """
    Convert string to bool.

    :param _str: string to be converted (str)
    :return: boolean (bool)
    :raise: argparse.ArgumentTypeError.
    """
    if isinstance(_str, bool):
        return _str
    if _str.lower() in {'yes', 'true', 't', 'y', '1'}:
        return True
    if _str.lower() in {'no', 'false', 'f', 'n', '0'}:
        return False

    raise argparse.ArgumentTypeError('Boolean value expected.')


# logger is set in the main function
# pylint: disable=used-before-assignment
def message(msg: str):
    """
    Print message to stdout or to log.

    :param msg: message (str).
    """
    if not logger:
        print(msg)
    else:
        logger.info(msg)


def get_file_lists(_lfns: str, _scopes: str, _ddmendpoints: str, _datasets: str, _guids: str) -> tuple:
    """
    Return the file lists.

    :param _lfns: comma separated list of lfns (str)
    :param _scopes: comma separated list of scopes (str)
    :param _ddmendpoints: comma separated list of ddmendpoints (str)
    :param _datasets: comma separated list of datasets (str)
    :param _guids: comma separated list of guids (str)
    :return: tuple of lists (lfns, scopes, ddmendpoints, datasets, guids).
    """
    return _lfns.split(','), _scopes.split(','), _ddmendpoints.split(','), _datasets.split(','), _guids.split(',')


class Job:
    """A minimal implementation of the Pilot Job class with data members necessary for the trace report only."""

    produserid = ""
    jobid = ""
    taskid = ""
    jobdefinitionid = ""

    def __init__(self, produserid: str = "", jobid: str = "", taskid: str = "", jobdefinitionid: str = ""):
        """
        Initialize the Job object.

        :param produserid: produserid (str)
        :param jobid: jobid (str)
        :param taskid: taskid (str)
        :param jobdefinitionid: jobdefinitionid (str)
        """
        self.produserid = produserid.replace('%20', ' ')
        self.jobid = jobid
        self.taskid = taskid
        self.jobdefinitionid = jobdefinitionid


def add_to_dictionary(dictionary: dict, key: str, value1: str, value2: str, value3: str, value4: str, value5: str,
                      value6: str) -> dict:
    """
    Add key: [value1, value2, value3, value4, value5, value6] to dictionary.

    In practice; lfn: [status, status_code, surl, turl, checksum, fsize].

    :param dictionary: dictionary to be updated (dict)
    :param key: lfn key to be added (str)
    :param value1: status to be added to list belonging to key (str)
    :param value2: status_code to be added to list belonging to key (str)
    :param value3: surl to be added to list belonging to key (str)
    :param value4: turl to be added to list belonging to key (str)
    :param value5: checksum to be added to list belonging to key (str)
    :param value6: fsize to be added to list belonging to key (str)
    :return: updated dictionary (dict).
    """
    dictionary[key] = [value1, value2, value3, value4, value5, value6]

    return dictionary


def extract_error_info(_err: str) -> tuple:
    """
    Extract error code and error message from the given error string.

    :param _err: error string (str)
    :return: tuple of error code and error message (int, str).
    """
    error_code = 0
    error_message = ""

    _code = re.search(r'error code: (\d+)', _err)
    if _code:
        error_code = _code.group(1)

    _msg = re.search('details: (.+)', _err)
    if _msg:
        error_message = _msg.group(1)
        error_message = error_message.replace('[PilotException(', '').strip()

    return error_code, error_message


if __name__ == '__main__':  # noqa: C901
    # get the args from the arg parser
    args = get_args()
    args.debug = True
    args.nopilotlog = False

    establish_logging(debug=args.debug, nopilotlog=args.nopilotlog, filename=config.Pilot.stageoutlog)
    logger = logging.getLogger(__name__)

    # get the file info
    lfns, scopes, ddmendpoints, datasets, guids = get_file_lists(args.lfns, args.scopes, args.ddmendpoints, args.datasets, args.guids)
    if len(lfns) != len(scopes) or len(lfns) != len(ddmendpoints) or len(lfns) != len(datasets) or len(lfns) != len(guids):
        message(f'file lists not same length: len(lfns)={len(lfns)}, len(scopes)={len(scopes)}, '
                f'len(ddmendpoints)={len(ddmendpoints)}, len(datasets)={len(datasets)}, len(guids)={len(guids)}')

    # generate the trace report
    trace_report = TraceReport(pq=os.environ.get('PILOT_SITENAME', ''), localSite=args.localsite,
                               remoteSite=args.remotesite, dataset="", eventType=args.eventtype, workdir=args.workdir)
    job = Job(produserid=args.produserid, jobid=args.jobid, taskid=args.taskid, jobdefinitionid=args.jobdefinitionid)
    trace_report.init(job)

    https_setup()

    try:
        infoservice = InfoService()
        infoservice.init(args.queuename, infosys.confinfo, infosys.extinfo)
        infosys.init(args.queuename)  # is this correct? otherwise infosys.queuedata doesn't get set
    except Exception as e:
        message(e)

    # perform stage-out (single transfers)
    err = ""
    errcode = 0
    activity = 'pw'

    client = StageOutClient(infoservice, logger=logger, trace_report=trace_report, workdir=args.workdir)
    kwargs = {"workdir": args.workdir, "cwd": args.workdir, "usecontainer": False, "job": job,
              "output_dir": args.outputdir, "catchall": args.catchall, "rucio_host": args.rucio_host}  # , "mode"='stage-out'}
    xfiles = []

    # number of stage-out attempts
    os.environ['PILOT_STAGEOUT_ATTEMPTS'] = str(args.stageout_attempts)

    for lfn, scope, dataset, ddmendpoint, guid in list(zip(lfns, scopes, datasets, ddmendpoints, guids)):

        if 'job.log' in lfn:
            kwargs['rucio_host'] = ''

        files = [{'scope': scope, 'lfn': lfn, 'workdir': args.workdir, 'dataset': dataset, 'ddmendpoint': ddmendpoint,
                  'ddmendpoint_alt': None}]
        # do not abbreviate the following two lines as otherwise the content of xfiles will be a list of generator objects
        _xfiles = [FileSpec(filetype='output', **f) for f in files]
        xfiles += _xfiles

        # prod analy unification: use destination preferences from PanDA server for unified queues
        # alt stage-out for unified queues should be implemented later (TODO?)
        if infoservice.queuedata.type != 'unified':
            client.prepare_destinations(xfiles, activity)

    try:
        r = client.transfer(xfiles, activity=activity, **kwargs)
    except PilotException as error:
        error_msg = traceback.format_exc()
        logger.error(error_msg)
        err = errors.format_diagnostics(error.get_error_code(), error_msg)
    except Exception as error:
        err = str(error)
        errcode = -1
        message(err)

    # put file statuses in a dictionary to be written to file
    file_dictionary = {}  # { 'error': [error_diag, -1], 'lfn1': [status, status_code], 'lfn2':.., .. }
    if xfiles:
        message('stageout script summary of transferred files:')
        for fspec in xfiles:
            add_to_dictionary(file_dictionary, fspec.lfn, fspec.status, fspec.status_code,
                              fspec.surl, fspec.turl, fspec.checksum.get('adler32'), fspec.filesize)
            status = fspec.status if fspec.status else "(not transferred)"
            message(f" -- lfn={fspec.lfn}, status_code={fspec.status_code}, status={status}, surl={fspec.surl}, "
                    f"turl={fspec.turl}, checksum={fspec.checksum.get('adler32')}, filesize={fspec.filesize}")

    # add error info, if any
    if err:
        errcode, err = extract_error_info(err)
    add_to_dictionary(file_dictionary, 'error', err, errcode, None, None, None, None)
    path = os.path.join(args.workdir, config.Container.stageout_status_dictionary)
    if os.path.exists(path):
        path += '.log'
    write_json(path, file_dictionary)
    if err:
        message(f"containerised file transfers failed: {err}")
        sys.exit(TRANSFER_ERROR)

    message(f"wrote {path}")
    message("containerised file transfers finished")
    sys.exit(0)
