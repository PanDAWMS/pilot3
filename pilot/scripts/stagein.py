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

"""This script is executed by the pilot in a container to perform stage-in of input files."""

import argparse
import logging
import os
import re
import sys

from pilot.api.data import StageInClient
from pilot.api.es_data import StageInESClient
from pilot.common.exception import ConversionFailure
from pilot.info import (
    infosys,
    InfoService,
    FileSpec,
)
from pilot.util.config import config
from pilot.util.filehandling import (
    write_json,
    read_json,
)
from pilot.util.loggingsupport import establish_logging
from pilot.util.tracereport import TraceReport

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
TRANSFER_ERROR = 12


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
                            required=False,
                            help='List of Rucio scopes (e.g., mc16_13TeV,mc16_13TeV')
    arg_parser.add_argument('--lfns',
                            dest='lfns',
                            required=False,
                            help='LFN list (e.g., filename1,filename2')
    arg_parser.add_argument('--eventtype',
                            dest='eventtype',
                            required=True,
                            help='Event type')
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
    arg_parser.add_argument('--filesizes',
                            dest='filesizes',
                            required=False,
                            help='Replica file sizes')
    arg_parser.add_argument('--checksums',
                            dest='checksums',
                            required=False,
                            help='Replica checksums')
    arg_parser.add_argument('--allowlans',
                            dest='allowlans',
                            required=False,
                            help='Replica allow_lan')
    arg_parser.add_argument('--allowwans',
                            dest='allowwans',
                            required=False,
                            help='Replica allow_wan')
    arg_parser.add_argument('--directaccesslans',
                            dest='directaccesslans',
                            required=False,
                            help='Replica direct_access_lan')
    arg_parser.add_argument('--directaccesswans',
                            dest='directaccesswans',
                            required=False,
                            help='Replica direct_access_wan')
    arg_parser.add_argument('--istars',
                            dest='istars',
                            required=False,
                            help='Replica is_tar')
    arg_parser.add_argument('--usevp',
                            dest='usevp',
                            type=str2bool,
                            default=False,
                            help='Job object boolean use_vp')
    arg_parser.add_argument('--accessmodes',
                            dest='accessmodes',
                            required=False,
                            help='Replica accessmodes')
    arg_parser.add_argument('--storagetokens',
                            dest='storagetokens',
                            required=False,
                            help='Replica storagetokens')
    arg_parser.add_argument('--guids',
                            dest='guids',
                            required=False,
                            help='Replica guids')
    arg_parser.add_argument('--replicadictionary',
                            dest='replicadictionary',
                            required=True,
                            help='Replica dictionary')
    arg_parser.add_argument('--inputdir',
                            dest='inputdir',
                            required=False,
                            default='',
                            help='Input files directory')
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


def str_to_int_list(_list: list) -> list:
    """
    Convert list of strings to list of integers.

    :param _list: list of strings (list)
    :return: list of integers (list).
    """
    _new_list = []
    for val in _list:
        try:
            _val = int(val)
        except (ValueError, TypeError):
            _val = None
        _new_list.append(_val)

    return _new_list


def str_to_bool_list(_list: list) -> list:
    """
    Convert list of strings to list of booleans.

    :param _list: list of strings (list)
    :return: list of booleans (list).
    """
    changes = {"True": True, "False": False, "None": None, "NULL": None}

    return [changes.get(x, x) for x in _list]


def get_file_lists(lfns: str, scopes: str, filesizes: str, checksums: str, allowlans: str, allowwans: str,
                   directaccesslans: str, directaccesswans: str, istars: str, accessmodes: str,
                   storagetokens: str, guids: str) -> dict:
    """
    Return a dictionary with the file lists.

    Format: {'lfns': <lfn list>, 'scopes': <scope list>, 'filesizes': <filesize list>, 'checksums': <checksum list>,
                'allowlans': <allowlan list>, 'allowwans': <allowwan list>, 'directaccesslans': <directaccesslan list>,
                'directaccesswans': <directaccesswan list>, 'istars': <istar list>, 'accessmodes': <accessmode list>,
                'storagetokens': <storagetoken list>, 'guids': <guid list>}

    :param lfns: comma separated lfns (str)
    :param scopes: comma separated scopes (str)
    :param filesizes: comma separated filesizes (str)
    :param checksums: comma separated checksums (str)
    :param allowlans: comma separated allowlans (str)
    :param allowwans: comma separated allowwans (str)
    :param directaccesslans: comma separated directaccesslans (str)
    :param directaccesswans: comma separated directaccesswans (str)
    :param istars: comma separated istars (str)
    :param accessmodes: comma separated accessmodes (str)
    :param storagetokens: comma separated storagetokens (str)
    :param guids: comma separated guids (str)
    :return: file lists dictionary (dict).
    """
    _lfns = []
    _scopes = []
    _filesizes = []
    _checksums = []
    _allowlans = []
    _allowwans = []
    _directaccesslans = []
    _directaccesswans = []
    _istars = []
    _accessmodes = []
    _storagetokens = []
    _guids = []
    try:
        _lfns = lfns.split(',')
        _scopes = scopes.split(',')
        _filesizes = str_to_int_list(filesizes.split(','))
        _checksums = checksums.split(',')
        _allowlans = str_to_bool_list(allowlans.split(','))
        _allowwans = str_to_bool_list(allowwans.split(','))
        _directaccesslans = str_to_bool_list(directaccesslans.split(','))
        _directaccesswans = str_to_bool_list(directaccesswans.split(','))
        _istars = str_to_bool_list(istars.split(','))
        _accessmodes = accessmodes.split(',')
        _storagetokens = storagetokens.split(',')
        _guids = guids.split(',')
    except (NameError, TypeError, ValueError) as error:
        message(f"exception caught: {error}")

    file_list_dictionary = {'lfns': _lfns, 'scopes': _scopes, 'filesizes': _filesizes, 'checksums': _checksums,
                            'allowlans': _allowlans, 'allowwans': _allowwans, 'directaccesslans': _directaccesslans,
                            'directaccesswans': _directaccesswans, 'istars': _istars, 'accessmodes': _accessmodes,
                            'storagetokens': _storagetokens, 'guids': _guids}

    return file_list_dictionary


class Job:
    """A minimal implementation of the Pilot Job class with data members necessary for the trace report only."""

    produserid = ""
    jobid = ""
    taskid = ""
    jobdefinitionid = ""

    def __init__(self, produserid: str = "", jobid: str = "", taskid: str = "", jobdefinitionid: str = ""):
        """
        Initialize the Job class.

        :param produserid: produserid (str)
        :param jobid: jobid (str)
        :param taskid: taskid (str)
        :param jobdefinitionid: jobdefinitionid (str).
        """
        self.produserid = produserid.replace('%20', ' ')
        self.jobid = jobid
        self.taskid = taskid
        self.jobdefinitionid = jobdefinitionid


def add_to_dictionary(dictionary: dict, key: str, value1: str, value2: str, value3: str, value4: str) -> dict:
    """
    Add key: [value1, value2, ..] to dictionary.

    In practice; lfn: [status, status_code, turl, DDM endpoint].

    :param dictionary: dictionary to be updated (dict)
    :param key: lfn key to be added (str)
    :param value1: status to be added to list belonging to key (str)
    :param value2: status_code to be added to list belonging to key (str)
    :param value3: turl (str)
    :param value4: DDM endpoint (str)
    :return: updated dictionary (dict).
    """
    dictionary[key] = [value1, value2, value3, value4]

    return dictionary


def extract_error_info(errc: str) -> (int, str):
    """
    Extract error code and message from the error string.

    :param errc: error string (str)
    :return: error code (int), error message (str).
    """
    error_code = 0
    error_message = ""

    _code = re.search(r'error code: (\d+)', errc)
    if _code:
        error_code = _code.group(1)

    _msg = re.search('details: (.+)', errc)
    if _msg:
        error_message = _msg.group(1)
        error_message = error_message.replace('[PilotException(', '').strip()

    return error_code, error_message


if __name__ == '__main__':
    # get the args from the arg parser
    args = get_args()
    args.debug = True
    args.nopilotlog = False

    establish_logging(debug=args.debug, nopilotlog=args.nopilotlog, filename=config.Pilot.stageinlog)
    logger = logging.getLogger(__name__)

    # get the file info
    try:
        replica_dictionary = read_json(os.path.join(args.workdir, args.replicadictionary))
    except ConversionFailure as exc:
        message(f'exception caught reading json: {exc}')
        sys.exit(1)

#    file_list_dictionary = get_file_lists(args.lfns, args.scopes, args.filesizes, args.checksums, args.allowlans,
#                                          args.allowwans, args.directaccesslans, args.directaccesswans, args.istars,
#                                          args.accessmodes, args.storagetokens, args.guids)
#    lfns = file_list_dictionary.get('lfns')
#    scopes = file_list_dictionary.get('scopes')
#    filesizes = file_list_dictionary.get('filesizes')
#    checksums = file_list_dictionary.get('checksums')
#    allowlans = file_list_dictionary.get('allowlans')
#    allowwans = file_list_dictionary.get('allowwans')
#    directaccesslans = file_list_dictionary.get('directaccesslans')
#    directaccesswans = file_list_dictionary.get('directaccesswans')
#    istars = file_list_dictionary.get('istars')
#    accessmodes = file_list_dictionary.get('accessmodes')
#    storagetokens = file_list_dictionary.get('storagetokens')
#    guids = file_list_dictionary.get('guids')

    # generate the trace report
    trace_report = TraceReport(pq=os.environ.get('PILOT_SITENAME', ''), localSite=args.localsite, remoteSite=args.remotesite, dataset="",
                               eventType=args.eventtype, workdir=args.workdir)
    job = Job(produserid=args.produserid, jobid=args.jobid, taskid=args.taskid, jobdefinitionid=args.jobdefinitionid)
    trace_report.init(job)

    try:
        infoservice = InfoService()
        infoservice.init(args.queuename, infosys.confinfo, infosys.extinfo)
        infosys.init(args.queuename)  # is this correct? otherwise infosys.queuedata doesn't get set
    except Exception as exc:
        message(exc)

    # perform stage-in (single transfers)
    err = ""
    errcode = 0
    if args.eventservicemerge:
        client = StageInESClient(infoservice, logger=logger, trace_report=trace_report)
        activity = 'es_events_read'
    else:
        client = StageInClient(infoservice, logger=logger, trace_report=trace_report, workdir=args.workdir)
        activity = 'pr'
    kwargs = {"workdir": args.workdir, "cwd": args.workdir, "usecontainer": False, "use_pcache": args.usepcache,
              "use_bulk": False, "use_vp": args.usevp, "input_dir": args.inputdir, "catchall": args.catchall,
              "rucio_host": args.rucio_host}
    xfiles = []
    for lfn in replica_dictionary:
        files = [{'scope': replica_dictionary[lfn]['scope'],
                  'lfn': lfn,
                  'guid': replica_dictionary[lfn]['guid'],
                  'workdir': args.workdir,
                  'filesize': replica_dictionary[lfn]['filesize'],
                  'checksum': replica_dictionary[lfn]['checksum'],
                  'allow_lan': replica_dictionary[lfn]['allowlan'],
                  'allow_wan': replica_dictionary[lfn]['allowwan'],
                  'direct_access_lan': replica_dictionary[lfn]['directaccesslan'],
                  'direct_access_wan': replica_dictionary[lfn]['directaccesswan'],
                  'is_tar': replica_dictionary[lfn]['istar'],
                  'accessmode': replica_dictionary[lfn]['accessmode'],
                  'storage_token': replica_dictionary[lfn]['storagetoken'],
                  'checkinputsize': True}]

        # do not abbreviate the following two lines as otherwise the content of xfiles will be a list of generator objects
        _xfiles = [FileSpec(filetype='input', **f) for f in files]
        xfiles += _xfiles

    try:
        client.prepare_sources(xfiles)
        client.transfer(xfiles, activity=activity, **kwargs)
    except Exception as exc:
        err = str(exc)
        errcode = -1
        message(err)

    # put file statuses in a dictionary to be written to file
    file_dictionary = {}  # { 'error': [error_diag, -1], 'lfn1': [status, status_code], 'lfn2':.., .. }
    if xfiles:
        message('stagein script summary of transferred files:')
        for fspec in xfiles:
            add_to_dictionary(file_dictionary, fspec.lfn, fspec.status, fspec.status_code, fspec.turl, fspec.ddmendpoint)
            status = fspec.status if fspec.status else "(not transferred)"
            message(f" -- lfn={fspec.lfn}, ddmendpoint={fspec.ddmendpoint}, status_code={fspec.status_code}, status={status}")

    # add error info, if any
    if err:
        errcode, err = extract_error_info(err)
    add_to_dictionary(file_dictionary, 'error', err, errcode, None, None)
    write_json(os.path.join(args.workdir, config.Container.stagein_status_dictionary), file_dictionary)
    if err:
        message(f"containerised file transfers failed: {err}")
        sys.exit(TRANSFER_ERROR)

    message("containerised file transfers finished")
    sys.exit(0)
