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
# - Alexey Anisenkov, anisyonk@cern.ch, 2018-24
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-24
# - Wen Guan, wen.guan@cern.ch, 2018

"""
The implementation of data structure to host Job definition.

The main reasons for such incapsulation are to
 - apply in one place all data validation actions (for attributes and values)
 - introduce internal information schema (names of attributes) to remove dependency
 with data structure, formats, names from external source (PanDA)

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: February 2018
"""

import ast
import logging
import os
import re
import shlex
from json import dumps
from time import sleep
from typing import Any

from pilot.util.auxiliary import (
    get_object_size,
    get_key_value
)
from pilot.util.constants import LOG_TRANSFER_NOT_DONE
from pilot.util.filehandling import (
    get_guid,
    get_valid_path_from_list
)
from pilot.util.timing import get_elapsed_real_time
from .basedata import BaseData
from .filespec import FileSpec

logger = logging.getLogger(__name__)


class JobData(BaseData):
    """
    High-level object to host Job definition/settings
    """

    # ## put explicit list of all the attributes with comments for better inline-documentation by Sphinx
    # ## FIX ME LATER: use proper doc format
    # ## incomplete list of attributes .. to be extended once becomes used

    jobid = None                   # unique Job identifier (forced to be a string)
    taskid = None                  # unique Task identifier, the task that this job belongs to (forced to be a string)
    batchid = None                 # batch system job id (should be removed from here)
    batchtype = None               # batch system type (should be removed from here)
    jobparams = ""                 # job parameters defining the execution of the job
    transformation = ""            # script execution name
    # current job status; format = {key: value, ..} e.g. key='LOG_TRANSFER', value='DONE'
    status = {'LOG_TRANSFER': LOG_TRANSFER_NOT_DONE}
    corecount = 1                  # Number of cores as requested by the task
    platform = ""                  # cmtconfig value from the task definition
    is_eventservice = False        # True for event service jobs
    is_eventservicemerge = False   # True for event service merge jobs
    is_hpo = False                 # True for HPO jobs
    transfertype = ""              # direct access instruction from server
    accessmode = ""                # direct access instruction from jobparams
    processingtype = ""            # e.g. nightlies
    maxcpucount = 0                # defines what is a looping job (seconds)
    allownooutput = ""             # used to disregard empty files from job report
    realtimelogging = False        # True for real-time logging (set by server/job definition/args)
    pandasecrets = ""              # User defined secrets
    pilotsecrets = {}              # Real-time logging secrets
    requestid = None               # Request ID

    # set by the pilot (not from job definition)
    workdir = ""                   # working directory for this job
    workdirsizes = []              # time ordered list of work dir sizes
    fileinfo = {}                  #
    piloterrorcode = 0             # current pilot error code
    piloterrorcodes = []           # ordered list of stored pilot error codes
    piloterrordiag = ""            # current pilot error diagnostics
    piloterrordiags = []           # ordered list of stored pilot error diagnostics
    transexitcode = 0              # payload/trf exit code
    exeerrorcode = 0               #
    exeerrordiag = ""              #
    exitcode = 0                   #
    exitmsg = ""                   #
    state = ""                     # internal pilot states; running, failed, finished, holding, stagein, stageout
    serverstate = ""               # server job states; starting, running, finished, holding, failed
    stageout = ""                  # stage-out identifier, e.g. log
    metadata = {}                  # payload metadata (job report)
    cpuconsumptionunit = "s"       #
    cpuconsumptiontime = -1        #
    cpuconversionfactor = 1        #
    cpufrequencies = []           # list of CPU frequencies
    nevents = 0                    # number of events
    neventsw = 0                   # number of events written
    dbtime = None                  #
    dbdata = None                  #
    resimevents = None             # ReSim events from job report (ATLAS)
    payload = ""                   # payload name
    utilities = {}                 # utility processes { <name>: [<process handle>, number of launches, command string], .. }
    pid = None                     # payload pid
    pgrp = None                    # payload process group
    sizes = {}                     # job object sizes { timestamp: size, .. }
    currentsize = 0                # current job object size
    command = ""                   # full payload command (set for container jobs)
    setup = ""                     # full payload setup (needed by postprocess command)
    zombies = []                   # list of zombie process ids
    memorymonitor = ""             # memory monitor name, e.g. prmon
    actualcorecount = 0            # number of cores actually used by the payload
    corecounts = []                # keep track of all actual core count measurements
    looping_check = True           # perform looping payload check
    checkinputsize = True          # False when mv copytool is used and input reside on non-local disks
    subprocesses = []              # list of PIDs for payload subprocesses
    prodproxy = ""                 # to keep track of production proxy on unified queues
    completed = False              # True when job has finished or failed, used by https::send_update()
    lsetuptime = 0                 # payload setup time (lsetup)
    runningstart = None            # time when the payload started running (only for internal monitoring purposes, not the actual start time)
    starttime = None               # time when the job started running (epoch time in seconds)

    # time variable used for on-the-fly cpu consumption time measurements done by job monitoring
    t0 = None                      # payload startup time

    overwrite_queuedata = {}       # custom settings extracted from job parameters (--overwriteQueueData) to be used as master values for `QueueData`
    overwrite_storagedata = {}     # custom settings extracted from job parameters (--overwriteStorageData) to be used as master values for `StorageData`

    zipmap = ""                    # ZIP MAP values extracted from jobparameters
    imagename = ""                 # container image name extracted from job parameters or job definition
    imagename_jobdef = ""
    usecontainer = False           # boolean, True if a container is to be used for the payload

    # from job definition
    attemptnr = 0                        # job attempt number
    destinationdblock = ""          ## to be moved to FileSpec (job.outdata)
    datasetin = ""                        ## TO BE DEPRECATED: moved to FileSpec (job.indata)
    debug = False                       # debug mode, when True, pilot will send debug info back to the server
    debug_command = ''            # debug command (can be defined on the task side)
    produserid = ""                     # the user DN (added to trace report)
    jobdefinitionid = ""               # the job definition id (added to trace report)
    infilesguids = ""                    # guids for input files
    indata = []                             # list of `FileSpec` objects for input files (aggregated inFiles, ddmEndPointIn, scopeIn, filesizeIn, etc)
    outdata = []                          # list of `FileSpec` objects for output files
    logdata = []                          # list of `FileSpec` objects for log file(s)
    # preprocess = {u'args': u'preprocess', u'command': u'echo'}
    # postprocess = {u'args': u'postprocess', u'command': u'echo'}
    preprocess = {}                    # preprocess dictionary with command to execute before payload, {'command': '..', 'args': '..'}
    postprocess = {}                  # postprocess dictionary with command to execute after payload, {'command': '..', 'args': '..'}
    coprocess = {}                     # coprocess dictionary with command to execute during payload, {'command': '..', 'args': '..'}
    # coprocess = {u'args': u'coprocess', u'command': u'echo'}
    containeroptions = {}          #
    use_vp = False                    # True for VP jobs
    maxwalltime = 0                 # maxWalltime in s
    dask_scheduler_ip = ''        # enhanced job definition for Dask jobs
    jupyter_session_ip = ''        # enhanced job definition for Dask jobs
    minramcount = 0                # minimum number of RAM required by the payload
    altstageout = None            # alternative stage-out method: boolean, on (True), off (False)
    # home package string with additional payload release information; does not need to be added to
    # the conversion function since it's already lower case
    homepackage = ""              # home package for TRF
    jobsetid = ""                        # job set id
    noexecstrcnv = None        # server instruction to the pilot if it should take payload setup from job parameters
    swrelease = ""                    # software release string
    writetofile = ""                    #
    nucleus = ""

    # cmtconfig encoded info
    alrbuserplatform = ""          # ALRB_USER_PLATFORM encoded in platform/cmtconfig value

    # RAW data to keep backward compatible behavior for a while ## TO BE REMOVED once all job attributes will be covered
    _rawdata = {}

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['corecount', 'piloterrorcode', 'transexitcode', 'exitcode', 'cpuconversionfactor', 'exeerrorcode',
                   'attemptnr', 'nevents', 'neventsw', 'pid', 'cpuconsumptiontime', 'maxcpucount', 'actualcorecount',
                   'requestid', 'maxwalltime', 'minramcount'],
             str: ['jobid', 'taskid', 'jobparams', 'transformation', 'destinationdblock', 'exeerrordiag'
                   'state', 'serverstate', 'workdir', 'stageout',
                   'platform', 'piloterrordiag', 'exitmsg', 'produserid', 'jobdefinitionid', 'writetofile',
                   'cpuconsumptionunit', 'homepackage', 'jobsetid', 'payload', 'processingtype',
                   'swrelease', 'zipmap', 'imagename', 'imagename_jobdef', 'accessmode', 'transfertype',
                   'datasetin',    ## TO BE DEPRECATED: moved to FileSpec (job.indata)
                   'infilesguids', 'memorymonitor', 'allownooutput', 'pandasecrets', 'prodproxy', 'alrbuserplatform',
                   'debug_command', 'dask_scheduler_ip', 'jupyter_session_ip', 'altstageout', 'nucleus'],
             list: ['piloterrorcodes', 'piloterrordiags', 'workdirsizes', 'zombies', 'corecounts', 'subprocesses',
                    'logdata', 'outdata', 'indata', 'cpufrequencies'],
             dict: ['status', 'fileinfo', 'metadata', 'utilities', 'overwrite_queuedata', 'sizes', 'preprocess',
                    'postprocess', 'coprocess', 'containeroptions', 'pilotsecrets'],
             bool: ['is_eventservice', 'is_eventservicemerge', 'is_hpo', 'noexecstrcnv', 'debug', 'usecontainer',
                    'use_vp', 'looping_check']
             }

    def __init__(self, data: dict, use_kmap: bool = True):
        """
        Initialize JobData object.

        :param data: input dictionary of data settings (dict)
        :param use_kmap: use kmap for data conversion (bool).
        """
        self.infosys = None  # reference to Job specific InfoService instance
        self._rawdata = data
        self.load(data, use_kmap=use_kmap)

        # for native HPO pilot support
        # if self.is_hpo:
        #    self.is_eventservice = True

    def init(self, infosys: Any):
        """
        Initialize JobData object with InfoService instance.

        :param infosys: infosys object (Any).
        """
        self.infosys = infosys
        self.indata = self.prepare_infiles(self._rawdata)
        self.outdata, self.logdata = self.prepare_outfiles(self._rawdata)

        # overwrites
        if self.imagename_jobdef and not self.imagename:
            logger.debug(f'using imagename_jobdef as imagename (\"{self.imagename_jobdef}\")')
            self.imagename = self.imagename_jobdef
        elif self.imagename_jobdef and self.imagename:
            logger.debug('using imagename from jobparams (ignoring imagename_jobdef)')
        elif not self.imagename_jobdef and self.imagename:
            logger.debug('using imagename from jobparams (imagename_jobdef not set)')

        if self.imagename:
            # prepend IMAGE_BASE to imagename if necessary (for testing purposes)
            image_base = os.environ.get('IMAGE_BASE', '')
            if not image_base and 'IMAGE_BASE' in infosys.queuedata.catchall:
                image_base = get_key_value(infosys.queuedata.catchall, key='IMAGE_BASE')
            if image_base:
                os.environ['ALRB_CONT_UNPACKEDDIR'] = image_base
                paths = [os.path.join(image_base, os.path.basename(self.imagename)),
                         os.path.join(image_base, self.imagename)]
                local_path = get_valid_path_from_list(paths)
                if local_path:
                    self.imagename = local_path
            #if image_base and not os.path.isabs(self.imagename) and not self.imagename.startswith('docker'):
            #    self.imagename = os.path.join(image_base, self.imagename)

    def prepare_infiles(self, data: dict) -> list:
        """
        Construct FileSpec objects for input files from raw dict `data`.

        :param data: input dictionary of data settings (dict)
        :return: list of validated `FileSpec` objects.
        """
        # direct access handling
        self.set_accessmode()

        access_keys = {'allow_lan', 'allow_wan', 'direct_access_lan', 'direct_access_wan'}
        if not self.infosys or not self.infosys.queuedata:
            self.show_access_settings(access_keys)

        # form raw list data from input comma-separated values for further validation by FileSpec
        kmap = self.get_kmap()

        ksources = dict([item, self.clean_listdata(data.get(item, ''), list, item, [])] for item in list(kmap.values()))
        ret, lfns = [], set()
        for ind, lfn in enumerate(ksources.get('inFiles', [])):
            if lfn in {'', 'NULL'} or lfn in lfns:  # exclude null data and duplicates
                continue
            lfns.add(lfn)
            idat = {}

            for attrname, item in list(kmap.items()):
                idat[attrname] = ksources[item][ind] if len(ksources[item]) > ind else None
            accessmode = 'copy'  ## default settings

            # for prod jobs: use remoteio if transferType=direct and prodDBlockToken!=local
            # for analy jobs: use remoteio if prodDBlockToken!=local
            if (self.is_analysis() or self.transfertype == 'direct') and idat.get('storage_token') != 'local':  ## Job settings
                accessmode = 'direct'
            if self.accessmode:  ## Job input options (job params) overwrite any other settings
                accessmode = self.accessmode

            idat['accessmode'] = accessmode
            # init access setting from queuedata
            if self.infosys and self.infosys.queuedata:
                for key in access_keys:
                    idat[key] = getattr(self.infosys.queuedata, key)

            finfo = FileSpec(filetype='input', **idat)
            logger.info(f'added file \'{lfn}\' with accessmode \'{accessmode}\'')
            ret.append(finfo)

        return ret

    def set_accessmode(self):
        """Set the accessmode field using jobparams."""
        self.accessmode = None
        if '--accessmode=direct' in self.jobparams:
            self.accessmode = 'direct'
        if '--accessmode=copy' in self.jobparams or '--useLocalIO' in self.jobparams:
            self.accessmode = 'copy'

    @staticmethod
    def show_access_settings(access_keys: list):
        """
        Show access settings for the case job.infosys.queuedata is not initialized.

        :param access_keys: list of access keys (list).
        """
        dat = dict([item, getattr(FileSpec, item, None)] for item in access_keys)
        msg = ', '.join([f"{item}={value}" for item, value in sorted(dat.items())])
        logger.info(f'job.infosys.queuedata is not initialized: the following access settings will be used by default: {msg}')

    @staticmethod
    def get_kmap() -> dict:
        """
        Return the kmap dictionary for server data to pilot conversions.

        :return: kmap (dict).
        """
        kmap = {
            # 'internal_name': 'ext_key_structure'
            'lfn': 'inFiles',
            ##'??': 'dispatchDblock', '??define_proper_internal_name': 'dispatchDBlockToken',
            'dataset': 'realDatasetsIn', 'guid': 'GUID',
            'filesize': 'fsize', 'checksum': 'checksum', 'scope': 'scopeIn',
            ##'??define_internal_key': 'prodDBlocks',
            'storage_token': 'prodDBlockToken',
            'ddmendpoint': 'ddmEndPointIn',
            'requestid': 'reqID'
        }

        return kmap

    def prepare_outfiles(self, data: dict) -> tuple:
        """
        Construct validated FileSpec objects for output and log files from raw dict `data`.

        Note: final preparation for output files can only be done after the payload has finished in case the payload
        has produced a job report with e.g. output file guids. For ATLAS, this is verified in
        pilot/user/atlas/diagnose/process_job_report().

        :param data: input dictionary of data settings (dict)
        :return: (list of `FileSpec` for output, list of `FileSpec` for log) (tuple).
        """
        # form raw list data from input comma-separated values for further validataion by FileSpec
        kmap = {
            # 'internal_name': 'ext_key_structure'
            'lfn': 'outFiles',
            ##'??': 'destinationDblock', '??define_proper_internal_name': 'destinationDBlockToken',
            'dataset': 'realDatasets', 'scope': 'scopeOut',
            ##'??define_internal_key':'prodDBlocks', '??':'dispatchDBlockTokenForOut',
            'ddmendpoint': 'ddmEndPointOut',
        }

        ksources = dict([item, self.clean_listdata(data.get(item, ''), list, item, [])] for item in list(kmap.values()))

        # take the logfile name from the environment first (in case of raythena and aborted pilots)
        pilot_logfile = os.environ.get('PILOT_LOGFILE', None)
        if pilot_logfile:
            # update the data with the new name
            old_logfile = data.get('logFile')
            data['logFile'] = pilot_logfile
            # note: the logFile also appears in the outFiles list
            outfiles = ksources.get('outFiles', None)
            if outfiles and old_logfile in outfiles:
                # search and replace the old logfile name with the new from the environment
                ksources['outFiles'] = [pilot_logfile if item == old_logfile else item for item in ksources.get('outFiles')]

        log_lfn = data.get('logFile')
        if log_lfn:
            # unify scopeOut structure: add scope of log file
            scope_out = []
            for lfn in ksources.get('outFiles', []):
                if lfn == log_lfn:
                    scope_out.append(data.get('scopeLog'))
                else:
                    if not ksources['scopeOut']:
                        raise Exception('Failed to extract scopeOut parameter from Job structure sent by Panda, please check input format!')
                    scope_out.append(ksources['scopeOut'].pop(0))
            ksources['scopeOut'] = scope_out

        return self._get_all_output(ksources, kmap, log_lfn, data)

    def _get_all_output(self, ksources: dict, kmap: dict, log_lfn: str, data: dict) -> tuple:
        """
        Create lists of FileSpecs for output + log files.

        Helper function for prepare_output().

        :param ksources: dictionary of sources (dict)
        :param kmap: dictionary of mappings (dict)
        :param log_lfn: log file name (str)
        :param data: input dictionary of data settings (dict)
        :return: ret_output (list of FileSpec), ret_log (list of FileSpec).
        """
        ret_output, ret_log = [], []

        lfns = set()
        for ind, lfn in enumerate(ksources['outFiles']):
            if lfn in {'', 'NULL'} or lfn in lfns:  # exclude null data and duplicates
                continue
            lfns.add(lfn)
            idat = {}
            for attrname, item in list(kmap.items()):
                idat[attrname] = ksources[item][ind] if len(ksources[item]) > ind else None

            ftype = 'output'
            ret = ret_output
            if lfn == log_lfn:  # log file case
                ftype = 'log'
                idat['guid'] = data.get('logGUID')
                ret = ret_log
            elif lfn.endswith('.lib.tgz'):  # build job case, generate a guid for the lib file
                idat['guid'] = get_guid()

            finfo = FileSpec(filetype=ftype, **idat)
            ret.append(finfo)

        return ret_output, ret_log

    def __getitem__(self, key: str):
        """
        Return the value of the given key.

        Temporary Integration function to keep dict-based access for old logic in compatible way
        TO BE REMOVED ONCE all fields will be moved to Job object attributes

        :param key: key (str)
        :return: value (Any).
        """
        if key == 'infosys':
            return self.infosys

        #if hasattr(self, key):
        #    return getattr(self, key)

        return self._rawdata[key]

    def __setitem__(self, key, val):
        """
        Set the value of the given key.

        Temporary Integration function to keep dict-based access for old logic in compatible way
        TO BE REMOVED ONCE all fields will be moved to Job object attributes.

        :param key: key (str)
        :param val: value (Any).
        """
        self._rawdata[key] = val

    def __contains__(self, key: str) -> bool:
        """
        Check if the key is in the raw data.

        Temporary Integration function to keep dict-based access for old logic in compatible way
        TO BE REMOVED ONCE all fields will be moved to Job object attributes

        :param key: key (str)
        :return: boolean.
        """
        return key in self._rawdata

    def get(self, key: str, defval: Any = None):
        """
        Return the value of the given key.

        Temporary Integration function to keep dict-based access for old logic in compatible way
        TO BE REMOVED ONCE all fields will be moved to Job object attributes

        :param key: key (str)
        :param defval: default value (Any
        :return: value (Any).
        """
        return self._rawdata.get(key, defval)

    def load(self, data: dict, use_kmap: bool = True):
        """
        Construct and initialize data from ext source.

        :param data: input dictionary of job data settings (dict)
        :param use_kmap: use kmap for data conversion (bool).
        """
        ## the translation map of the container attributes from external data to internal schema
        ## 'internal_name':('ext_name1', 'extname2_if_any')
        ## 'internal_name2':'ext_name3'

        ## first defined ext field will be used
        ## if key is not explicitly specified then ext name will be used as is
        ## fix me later to proper internal names if need

        kmap = {
            'jobid': 'PandaID',
            'taskid': 'taskID',
            'jobparams': 'jobPars',
            'corecount': 'coreCount',
            'platform': 'cmtConfig',
            'infilesguids': 'GUID',                      ## TO BE DEPRECATED: moved to FileSpec
            'attemptnr': 'attemptNr',
            'datasetin': 'realDatasetsIn',               ## TO BE DEPRECATED: moved to FileSpec
            'processingtype': 'processingType',
            'transfertype': 'transferType',
            'destinationdblock': 'destinationDblock',
            'noexecstrcnv': 'noExecStrCnv',
            'swrelease': 'swRelease',
            'jobsetid': 'jobsetID',
            'produserid': 'prodUserID',
            'jobdefinitionid': 'jobDefinitionID',
            'writetofile': 'writeToFile',
            'is_eventservice': 'eventService',
            'is_eventservicemerge': 'eventServiceMerge',
            'is_hpo': 'isHPO',
            'use_vp': 'useVP',
            'maxcpucount': 'maxCpuCount',
            'allownooutput': 'allowNoOutput',
            'imagename_jobdef': 'container_name',
            'containeroptions': 'containerOptions',
            'looping_check': 'loopingCheck',
            'pandasecrets': 'secrets',
            'pilotsecrets': 'pilotSecrets',
            'requestid': 'reqID',
            'maxwalltime': 'maxWalltime',
            'dask_scheduler_ip': 'scheduler_ip',
            'jupyter_session_ip': 'session_ip',
            'minramcount': 'minRamCount',
            'altstageout': 'altStageOut'
        } if use_kmap else {}

        self._load_data(data, kmap)

    def is_analysis(self) -> bool:  ## if it's experiment specific logic then it could be isolated into extended JobDataATLAS class
        """
        Determine whether the job is an analysis user job or not.

        :return: True in case of user analysis job (bool).
        """
        return self.transformation.startswith('https://') or self.transformation.startswith('http://')

    def is_build_job(self) -> bool:
        """
        Check if the job is a build job.

        (i.e. check if the job has an output file that is a lib file).

        :return: boolean.
        """
        return any('.lib.' in fspec.lfn and '.log.' not in fspec.lfn for fspec in self.outdata)

    def is_local(self) -> bool:
        """
        Check if the input files should be accessed locally.

        Confusing function, since it does not consider real status of applied transfer, TOBE DEPRECATED, use `has_remoteio()` instead

        Note: all input files will have storage_token set to local in that case.

        :return: boolean.
        """
        return any(fspec.storage_token == 'local' and '.lib.' not in fspec.lfn for fspec in self.indata)

    def has_remoteio(self) -> bool:
        """
        Check status of input file transfers and determine if direct access mode will be used or not.

        :return: True if at least one file should use direct access mode (bool).
        """
        return any(fspec.status == 'remote_io' for fspec in self.indata)

    def allow_altstageout(self):
        """
        Resolve if alternative stageout is allowed for given job taking into account `queuedata` settings as well.
        `queuedata` specific settings overwrites job preferences for altstageout.

        :return: boolean.
        """

        # consider first the queue specific settings (if any)
        if self.infosys and self.infosys.queuedata:
            qval = self.infosys.queuedata.altstageout
            if qval is not None:
                return qval
        else:
            logger.info('job.infosys.queuedata is not initialized: PandaQueue specific settings for altstageout will not be considered')

        # apply additional job specific checks here if need

        return bool(self.altstageout)

    def clean(self):
        """
        Validate and finally clean up required data values (object properties) if needed.

        Not used.
        """
        pass

    ## custom function pattern to apply extra validation to the key values
    ##def clean__keyname(self, raw, value):
    ##  :param raw: raw value passed from ext source as input
    ##  :param value: preliminary cleaned and casted to proper type value
    ##
    ##    return value

    def clean__corecount(self, raw: Any, value: int) -> Any:
        """
        Verify and validate value for the corecount key (set to 1 if not set).

        :param raw: (unused) (Any)
        :param value: core count (int)
        :return: updated core count (int).
        """
        # note: experiment specific

        # Overwrite the corecount value with ATHENA_PROC_NUMBER if it is set
        athena_corecount = os.environ.get('ATHENA_PROC_NUMBER')
        if athena_corecount:
            try:
                value = int(athena_corecount)
            except Exception:
                logger.info(f"ATHENA_PROC_NUMBER is not properly set.. ignored, data={athena_corecount}")

        return value if value else 1

    def clean__platform(self, raw: Any, value: str) -> str:
        """
        Verify and validate value for the platform key.

        Set the alrbuserplatform value if encoded in platform/cmtconfig string.

        :param raw: (unused) (Any)
        :param value: platform (str)
        :return: updated platform (str).
        """
        v = value if value.lower() not in ['null', 'none'] else ''
        # handle encoded alrbuserplatform in cmtconfig/platform string
        if '@' in v:
            self.alrbuserplatform = v.split('@')[1]  # ALRB_USER_PLATFORM value
            v = v.split('@')[0]  # cmtconfig value

        return v

    def clean__altstageout(self, raw: Any, value: str) -> Any:
        """
        Verify and validate value for the altstageout key.

        :param raw: raw value (Any)
        :param value: parsed value (str)
        :return: cleaned value (bool or None).
        """
        if value == 'on':
            return True
        if value == 'off':
            return False

    def clean__jobparams(self, raw: Any, value: str) -> str:
        """
        Verify and validate value for the jobparams key.

        Extract value from jobparams not related to job options.
        The function will in particular extract and remove --overwriteQueueData, ZIP_MAP and --containerimage.
        It will remove the old Pilot 1 option --overwriteQueuedata which should be replaced with --overwriteQueueData.

        :param raw: (unused) (Any)
        :param value: job parameters (str)
        :return: updated job parameters (str).
        """
        #   value += ' --athenaopts "HITtoRDO:--nprocs=$ATHENA_CORE_NUMBER" someblah'
        logger.info(f'cleaning jobparams: {value}')

        # user specific pre-filtering
        # (return list of strings not to be filtered, which will be put back in the post-filtering below)
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        try:
            user = __import__(f'pilot.user.{pilot_user}.jobdata', globals(), locals(), [pilot_user], 0)
            exclusions, value = user.jobparams_prefiltering(value)
        except Exception as exc:
            logger.warning(f'caught exception in user code: {exc}')
            exclusions = {}

        ## clean job params from Pilot1 old-formatted options
        ret = re.sub(r"--overwriteQueuedata={.*?}", "", value)

        ## extract overwrite options
        options, ret = self.parse_args(ret, {'--overwriteQueueData': lambda x: ast.literal_eval(x) if x else {},
                                             '--overwriteStorageData': lambda x: ast.literal_eval(x) if x else {}}, remove=True)
        self.overwrite_queuedata = options.get('--overwriteQueueData', {})
        self.overwrite_storagedata = options.get('--overwriteStorageData', {})

        # extract zip map  ## TO BE FIXED? better to pass it via dedicated sub-option in jobParams from PanDA side: e.g. using --zipmap "content"
        # so that the zip_map can be handles more gracefully via parse_args

        pattern = r" \'?<ZIP_MAP>(.+)<\/ZIP_MAP>\'?"
        pattern = re.compile(pattern)

        result = re.findall(pattern, ret)
        if result:
            self.zipmap = result[0]
            # remove zip map from final jobparams
            ret = re.sub(pattern, '', ret)

        # extract and remove any present --containerimage XYZ options
        ret, imagename = self.extract_container_image(ret)
        if imagename != "":
            self.imagename = imagename

        try:
            ret = user.jobparams_postfiltering(ret, exclusions=exclusions)
        except Exception as exc:
            logger.warning(f'caught exception in user code: {exc}')

        logger.info(f'cleaned jobparams: {ret}')

        return ret

    def extract_container_image(self, jobparams: str) -> tuple:
        """
        Extract the container image from the job parameters if present, and remove it.

        :param jobparams: job parameters (str)
        :return: string with updated job parameters, string with extracted image name (tuple).
        """
        imagename = ""

        # define regexp pattern for the full container image option
        _pattern = r'(\ \-\-containerImage\=?\s?[\S]+)'
        pattern = re.compile(_pattern)
        image_option = re.findall(pattern, jobparams)

        if image_option and image_option[0] != "":

            imagepattern = re.compile(r" \'?\-\-containerImage\=?\ ?([\S]+)\ ?\'?")
            # imagepattern = re.compile(r'(\ \-\-containerImage\=?\s?([\S]+))')
            image = re.findall(imagepattern, jobparams)
            if image and image[0] != "":
                try:
                    imagename = image[0]  # removed [1]
                except Exception as exc:
                    logger.warning(f'failed to extract image name: {exc}')
                else:
                    logger.info(f"extracted image from jobparams: {imagename}")
            else:
                logger.warning(f"image could not be extract from {jobparams}")

            # remove the option from the job parameters
            jobparams = re.sub(_pattern, "", jobparams)
            logger.info(f"removed the {image_option[0]} option from job parameters: {jobparams}")

        return jobparams, imagename

    @classmethod
    def parse_args(cls, data: str, options: dict, remove: bool = False) -> tuple:
        """
        Extract option/values from string containing command line options (arguments).

        :param data: input command line arguments (str)
        :param options: dict of option names to be considered: (name, type), type is a cast function to be applied with result value (dict)
        :param remove: boolean, if True then exclude specified options from returned raw string of command line arguments (bool)
        :return: Dict of extracted options, raw string of final command line options (tuple).
        """
        logger.debug(f'extract options={list(options.keys())} from data={data}')

        if not options:
            return {}, data

        opts, pargs = cls.get_opts_pargs(data)
        if not opts:
            return {}, data

        ret = cls.get_ret(options, opts)

        ## serialize parameters back to string
        rawdata = data
        if remove:
            final_args = []
            for arg in pargs:
                if isinstance(arg, (tuple, list)):  ## parsed option
                    if arg[0] not in options:  # exclude considered options
                        if arg[1] is None:
                            arg.pop()
                        final_args.extend(arg)
                else:
                    final_args.append(arg)
            rawdata = " ".join(shlex.quote(e) for e in final_args)

        return ret, rawdata

    @staticmethod
    def get_opts_pargs(data: str) -> tuple[dict, list]:
        """
        Get the opts and pargs variables.

        :param data: input command line arguments (str)
        :return: opts dict, pargs list (tuple).
        """
        try:
            args = shlex.split(data)
        except ValueError as exc:
            logger.error(f'Failed to parse input arguments from data={data}, error={exc} .. skipped.')
            return {}, []

        opts, curopt, pargs = {}, None, []
        for arg in args:
            if arg.startswith('-'):
                if curopt is not None:
                    opts[curopt] = None
                    pargs.append([curopt, None])
                curopt = arg
                continue
            if curopt is None:  # no open option, ignore
                pargs.append(arg)
            else:
                opts[curopt] = arg
                pargs.append([curopt, arg])
                curopt = None
        if curopt:
            pargs.append([curopt, None])

        return opts, pargs

    @staticmethod
    def get_ret(options: dict, opts: dict):
        """
        Get the ret variable from the options.

        :param options: dict of option names to be considered: (name, type) (dict)
        :param opts: dict of extracted options (dict)
        :return: ret (dict).
        """
        ret = {}
        for opt, fcast in list(options.items()):
            val = opts.get(opt)
            try:
                val = fcast(val) if callable(fcast) else val
            except Exception as exc:
                logger.error(f'failed to extract value for option={opt} from data={val}: cast function={fcast} failed, exception={exc}')
                continue
            ret[opt] = val

        return ret

    def add_workdir_size(self, workdir_size: int):
        """
        Add a measured workdir size to the workdirsizes field.

        The function will deduce any input and output file sizes from the workdir size.

        :param workdir_size: workdir size (int).
        """
        if not isinstance(workdir_size, int):
            try:
                workdir_size = int(workdir_size)
            except Exception as exc:
                logger.warning(f'failed to convert {workdir_size} to int: {exc}')
                return

        total_size = 0  # B
        if os.path.exists(self.workdir):
            # Find out which input and output files have been transferred and add their sizes to the total size
            # (Note: output files should also be removed from the total size since outputfilesize is added in the
            # task def)

            # Then update the file list in case additional output files were produced
            # Note: don't do this deduction since it is not known by the task definition
            # out_files, dummy, dummy = discoverAdditionalOutputFiles(outFiles, job.workdir, job.destinationDblock,
            # job.scopeOut)

            for fspec in self.indata + self.outdata:
                if fspec.filetype == 'input' and (fspec.status != 'transferred' or not self.checkinputsize):
                    continue
                pfn = os.path.join(self.workdir, fspec.lfn)
                if not os.path.isfile(pfn):
                    logger.info(f"pfn file={pfn} does not exist (skip from workdir size calculation)")
                else:
                    total_size += os.path.getsize(pfn)

            logger.info(f'total size of present files: {total_size} B (workdir size: {workdir_size} B)')
            workdir_size -= total_size

        self.workdirsizes.append(workdir_size)

    def get_max_workdir_size(self) -> int:
        """
        Return the maximum disk space used by the payload.

        :return: workdir size (int).
        """
        maxdirsize = 0
        if self.workdirsizes:
            # Get the maximum value from the list
            maxdirsize = max(self.workdirsizes)
        else:
            logger.warning("found no stored workdir sizes")

        return maxdirsize

    def get_lfns_and_guids(self) -> tuple[list, list]:
        """
        Return ordered lists with the input file LFNs and GUIDs.

        :return: list of input files, list of corresponding GUIDs (tuple).
        """
        lfns = []
        guids = []

        for fspec in self.indata:
            lfns.append(fspec.lfn)
            guids.append(fspec.guid)

        return lfns, guids

    def get_status(self, key: str) -> str:
        """

        Return the value for the given key (e.g. LOG_TRANSFER) from the status dictionary.
        LOG_TRANSFER_NOT_DONE is returned if job object is not defined for key='LOG_TRANSFER'.
        If no key is found, None will be returned.

        :param key: key name (str)
        :return: corresponding key value in job.status dictionary (str).
        """
        log_transfer = self.status.get(key, None)

        if not log_transfer:
            if key == 'LOG_TRANSFER':
                log_transfer = LOG_TRANSFER_NOT_DONE

        return log_transfer

    def get_job_option_for_input_name(self, input_name: str) -> str or None:
        """
        Get the job option for the given input name.

        Expecting something like --inputHitsFile=@input_name in jobparams.

        :param input_name: input name (str)
        :return: job_option such as --inputHitsFile (str).
        """
        job_options = self.jobparams.split(' ')
        input_name_option = f'=@{input_name}'
        for job_option in job_options:
            if input_name_option in job_option:
                return job_option.split("=")[0]

        return None

    def process_writetofile(self):
        """
        Process the writetofile field.

        Expecting writetofile from the job definition.
        The format is 'inputFor_file1:lfn1,lfn2^inputFor_file2:lfn3,lfn4'

        format writetofile_dictionary = {'inputFor_file1': [lfn1, lfn2], 'inputFor_file2': [lfn3, lfn4]}
        """
        writetofile_dictionary = {}
        if self.writetofile:
            fileinfos = self.writetofile.split("^")
            for fileinfo in fileinfos:
                if ':' in fileinfo:
                    input_name, input_list = fileinfo.split(":")
                    writetofile_dictionary[input_name] = input_list.split(',')
                else:
                    logger.error(f"writeToFile doesn't have the correct format, expecting a separator \':\' for {fileinfo}")

        if writetofile_dictionary:
            for input_name, input_files in writetofile_dictionary.items():
                input_name_new = input_name + '.txt'
                input_name_full = os.path.join(self.workdir, input_name_new)

                with open(input_name_full, 'w', encoding='utf-8') as f:
                    job_option = self.get_job_option_for_input_name(input_name)
                    if not job_option:
                        logger.error("unknown job option format, "
                                     "expected job options such as \'--inputHitsFile\' for input file: {input_name}")
                    else:
                        f.write(f"{job_option}\n")
                    for input_file in input_files:
                        f.write(f"{input_file}\n")
                    logger.info(f"wrote input file list to file {input_name_full}: {input_files}")

                self.jobparams = self.jobparams.replace(input_name, input_name_new)
                if job_option:
                    self.jobparams = self.jobparams.replace(f'{job_option}=', '')
                self.jobparams = self.jobparams.replace('--autoConfiguration=everything', '')
                logger.info(f"jobparams after processing writeToFile: {self.jobparams}")

    def add_size(self, size: int):
        """
        Add a size measurement to the sizes field at the current time stamp.

        A size measurement is in Bytes.

        :param size: size of object in Bytes (int).
        """
        # is t0 set? if not, set it
        if not self.t0:
            self.t0 = os.times()

        # get the current time stamp relative to t0
        time_stamp = get_elapsed_real_time(t0=self.t0)

        # add a data point to the sizes dictionary
        self.sizes[time_stamp] = size

    def get_size(self) -> int:
        """
        Determine the size (B) of the job object.

        :return: size (int).
        """
        # protect against the case where the object changes size during calculation (rare)
        try:
            self.currentsize = get_object_size(self)
        except Exception:
            pass

        return self.currentsize

#    def collect_zombies(self, depth: int = None):
#        """
#        Collect zombie child processes.
#
#        Depth is the max number of loops, plus 1, to avoid infinite looping even if some child processes get really
#        wedged; depth=None means it will keep going until all child zombies have been collected.
#
#        :param depth: max depth (int).
#        """
#        sleep(1)
#
#        if self.zombies and depth > 1:
#            logger.info(f"--- collectZombieJob: --- {depth}, {self.zombies}")
#            depth -= 1
#            for zombie in self.zombies:
#                try:
#                    logger.info(f"zombie collector waiting for pid {zombie}")
#                    _id, _ = os.waitpid(zombie, os.WNOHANG)
#                except OSError as exc:
#                    logger.info(f"harmless exception when collecting zombies: {exc}")
#                    self.zombies.remove(zombie)
#                else:
#                    if _id:  # finished
#                        self.zombies.remove(zombie)
#                self.collect_zombies(depth=depth)  # recursion
#
#        if self.zombies and not depth:
#            # for the infinite waiting case, we have to use blocked waiting, otherwise it throws
#            # RuntimeError: maximum recursion depth exceeded
#            for zombie in self.zombies:
#                try:
#                    _id, _ = os.waitpid(zombie, 0)
#                except OSError as exc:
#                    logger.info(f"harmless exception when collecting zombie jobs: {exc}")
#                    self.zombies.remove(zombie)
#                else:
#                    if _id:  # finished
#                        self.zombies.remove(zombie)
#                self.collect_zombies(depth=depth)  # recursion

    def collect_zombies(self, depth: int = None):
        """
        Collect zombie child processes.

        Depth is the max number of loops, plus 1, to avoid infinite looping even if some child processes get really
        wedged; depth=None means it will keep going until all child zombies have been collected.

        :param depth: max depth (int).
        """
        sleep(1)

        current_depth = depth
        while self.zombies and (current_depth is None or current_depth > 0):
            if current_depth:
                logger.info(f"--- collectZombieJob: --- {current_depth}, {self.zombies}")
                current_depth -= 1

            zombies_to_remove = []
            for zombie in self.zombies:
                try:
                    logger.info(f"zombie collector waiting for pid {zombie}")
                    # dangerous, can lead to blocking : _id, _ = os.waitpid(zombie, os.WNOHANG if current_depth else 0)
                    _id, _ = os.waitpid(zombie, os.WNOHANG)
                except OSError as exc:
                    logger.info(f"harmless exception when collecting zombies: {exc}")
                    zombies_to_remove.append(zombie)
                else:
                    if _id:  # finished
                        zombies_to_remove.append(zombie)

            # Remove collected zombies from the list
            for zombie in zombies_to_remove:
                self.zombies.remove(zombie)
            if current_depth == 0:
                break

    def only_copy_to_scratch(self) -> bool:  ## TO BE DEPRECATED, use `has_remoteio()` instead of
        """
        Determine if the payload only has copy-to-scratch input.

        In this case, there should be no --usePFCTurl or --directIn in the job parameters.

        :return: True if only copy-to-scratch. False if at least one file should use direct access mode (bool)
        """
        return not any(fspec.status == 'remote_io' for fspec in self.indata)
        # for fspec in self.indata:
        #     if fspec.status == 'remote_io':
        #         return False

    def reset_errors(self):  # temporary fix, make sure all queues are empty before starting new job
        """Reset error codes and messages."""
        self.piloterrorcode = 0
        self.piloterrorcodes = []
        self.piloterrordiag = ""
        self.piloterrordiags = []
        self.transexitcode = 0
        self.exeerrorcode = 0
        self.exeerrordiag = ""
        self.exitcode = 0
        self.exitmsg = ""
        self.corecounts = []
        self.cpufrequencies = []
        self.subprocesses = []

    def to_json(self):
        """Convert class to dictionary."""
        return dumps(self, default=lambda par: par.__dict__)
