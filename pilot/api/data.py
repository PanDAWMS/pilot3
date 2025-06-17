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
# - Mario Lassnig, mario.lassnig@cern.ch, 2017
# - Paul Nilsson, paul.nilsson@cern.ch, 2017-2024
# - Tobias Wegner, tobias.wegner@cern.ch, 2017-2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2018-2024

"""API for data transfers."""

import os
import hashlib
import logging
import time
from functools import reduce
from typing import Any
try:
    import requests
except ImportError:
    pass

from pilot.info import infosys
from pilot.common.exception import (
    PilotException,
    ErrorCodes,
    SizeTooLarge,
    NoLocalSpace,
    ReplicasNotFound,
    FileHandlingFailure,
)
from pilot.util.config import config
from pilot.util.filehandling import (
    calculate_checksum,
    write_json,
)
from pilot.util.math import convert_mb_to_b
from pilot.util.parameters import get_maximum_input_sizes
from pilot.util.workernode import get_local_disk_space
from pilot.util.auxiliary import TimeoutException
from pilot.util.tracereport import TraceReport


class StagingClient:
    """Base Staging Client."""

    ipv = "IPv6"
    workdir = ''
    mode = ""  # stage-in/out, set by the inheritor of the class
    copytool_modules = {'rucio': {'module_name': 'rucio'},
                        'gfal': {'module_name': 'gfal'},
                        'gfalcopy': {'module_name': 'gfal'},
                        'xrdcp': {'module_name': 'xrdcp'},
                        'mv': {'module_name': 'mv'},
                        'objectstore': {'module_name': 'objectstore'},
                        's3': {'module_name': 's3'},
                        'gs': {'module_name': 'gs'},
                        'lsm': {'module_name': 'lsm'}
                        }

    # list of allowed schemas to be used for direct acccess mode from REMOTE replicas
    direct_remoteinput_allowed_schemas = ['root', 'https']
    # list of schemas to be used for direct acccess mode from LOCAL replicas
    direct_localinput_allowed_schemas = ['root', 'dcache', 'dcap', 'file', 'https']
    # list of allowed schemas to be used for transfers from REMOTE sites
    remoteinput_allowed_schemas = ['root', 'gsiftp', 'dcap', 'srm', 'storm', 'https']

    def __init__(self,
                 infosys_instance: Any = None,
                 acopytools: dict = None,
                 logger: Any = None,
                 default_copytools: str = 'rucio',
                 trace_report: dict = None,
                 ipv: str = 'IPv6',
                 workdir: str = ""):
        """
        Set default/init values.

        If `acopytools` is not specified then it will be automatically resolved via infosys. In this case `infosys`
        requires initialization.

        :param infosys_instance: infosys instance to be used for data resolution (Any)
        :param acopytools: copytool names per activity to be used for transfers. Accepts also list of names or string value without activity passed (dict)
        :param logger: logging.Logger object to use for logging (None means no logging) (Any)
        :param default_copytools: copytool name(s) to be used in case of unknown activity passed. Accepts either list of names or single string value (str)
        :param trace_report: trace report object (dict)
        :param ipv: internet protocol version (str)
        :param workdir: working directory (str).
        """
        super().__init__()

        if not logger:
            logger = logging.getLogger(__name__ + '.null')
            logger.disabled = True

        self.logger = logger
        self.infosys = infosys_instance or infosys
        self.ipv = ipv
        self.workdir = workdir

        if isinstance(acopytools, str):
            acopytools = {'default': [acopytools]} if acopytools else {}

        if isinstance(acopytools, (list, tuple)):
            acopytools = {'default': acopytools} if acopytools else {}

        self.acopytools = acopytools or {}

        if self.infosys.queuedata:
            self.set_acopytools()

        if not self.acopytools.get('default'):
            self.acopytools['default'] = self.get_default_copytools(default_copytools)

        # get an initialized trace report (has to be updated for get/put if not defined before)
        self.trace_report = trace_report if trace_report else TraceReport(pq=os.environ.get('PILOT_SITENAME', ''), ipv=self.ipv, workdir=self.workdir)

        if not self.acopytools:
            msg = f'failed to initialize StagingClient: no acopytools options found, acopytools={self.acopytools}'
            logger.error(msg)
            self.trace_report.update(clientState='BAD_COPYTOOL', stateReason=msg)
            self.trace_report.send()
            raise PilotException("failed to resolve acopytools settings")
        logger.info('configured copytools per activity: acopytools=%s', self.acopytools)

    def allow_mvfinaldest(self, catchall: str):
        """
        Check if there is an override in catchall to allow mv to final destination.

        :param catchall: catchall from queuedata (str)
        :return: True if 'mv_final_destination' is present in catchall, otherwise False (bool).
        """
        return catchall and 'mv_final_destination' in catchall

    def set_acopytools(self):
        """Set the internal acopytools."""
        if not self.acopytools:  # resolve from queuedata.acopytools using infosys
            self.acopytools = (self.infosys.queuedata.acopytools or {}).copy()
        if not self.acopytools:  # resolve from queuedata.copytools using infosys
            self.acopytools = {"default": list((self.infosys.queuedata.copytools or {}).keys())}

    @staticmethod
    def get_default_copytools(default_copytools: str):
        """
        Get the default copytools as a list.

        :param default_copytools: default copytools (str)
        :return: default copytools (str).
        """
        if isinstance(default_copytools, str):
            default_copytools = [default_copytools] if default_copytools else []

        return default_copytools

    @classmethod
    def get_preferred_replica(cls, replicas: list, allowed_schemas: list) -> Any or None:
        """
        Get preferred replica from the `replicas` list suitable for `allowed_schemas`.

        :param replicas: list of replicas (list)
        :param allowed_schemas: list of allowed schemas (list)
        :return: first matched replica or None if not found (Any or None).
        """
        for replica in replicas:
            pfn = replica.get('pfn')
            for schema in allowed_schemas:
                if pfn and (not schema or pfn.startswith(f'{schema}://')):
                    return replica
        return None

    def prepare_sources(self, files: list, activities: Any = None) -> None:
        """
        Prepare sources.

        Customize/prepare source data for each entry in `files` optionally checking data for requested `activities`.
        (custom StageClient could extend the logic if needed).

        :param files: list of `FileSpec` objects to be processed (list)
        :param activities: string or ordered list of activities to resolve `astorages` (optional) (Any)
        :return: (None)
        """
        return None

    def prepare_inputddms(self, files: list, activities: list = None):
        """
        Prepare input DDMs.

        Populates filespec.inputddms for each entry from `files` list.

        :param files: list of `FileSpec` objects
        :param activities: ordered list of activities to resolve `astorages` (optional)
        """
        astorages = self.infosys.queuedata.astorages if self.infosys and self.infosys.queuedata else {}
        activities = activities or ['read_lan']

        storages = next((astorages.get(a) for a in activities if astorages.get(a)), None) or []

        #if not storages:  ## ignore empty astorages
        #    activity = activities[0]
        #    raise PilotException("Failed to resolve input sources: no associated storages defined for activity=%s (%s)"
        #                         % (activity, ','.join(activities)), code=ErrorCodes.NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        for fdat in files:
            if not fdat.inputddms:
                fdat.inputddms = storages
            if not fdat.inputddms and fdat.ddmendpoint:
                fdat.inputddms = [fdat.ddmendpoint]

    def print_replicas(self, replicas: list, label: str = 'unsorted'):
        """
        Print replicas.

        :param replicas: list of replicas (Any)
        :param label: label (str).
        """
        number = 1
        maxnumber = 10
        self.logger.debug(f'{label} list of replicas: (max {maxnumber})')
        for pfn, xdat in replicas:
            self.logger.debug(f"{number}. "
                              f"lfn={pfn}, "
                              f"rse={xdat.get('ddmendpoint')}, "
                              f"domain={xdat.get('domain')}")
            number += 1
            if number > maxnumber:
                break

    @classmethod
    def sort_replicas(cls, replicas: list, inputddms: list) -> list:
        """
        Sort input replicas.

        Consider first affected replicas from inputddms.

        :param replicas: Prioritized list of replicas [(pfn, dat)] (list)
        :param inputddms: preferred list of ddmebdpoint (list)
        :return: sorted list of `replicas` (list).
        """
        if not inputddms:
            return replicas

        # group replicas by ddmendpoint to properly consider priority of inputddms
        ddmreplicas = {}
        for pfn, xdat in replicas:
            ddmreplicas.setdefault(xdat.get('rse'), []).append((pfn, xdat))

        # process LAN first (keep fspec.inputddms priorities)
        xreplicas = []
        for ddm in inputddms:
            xreplicas.extend(ddmreplicas.get(ddm) or [])

        for pfn, xdat in replicas:
            if (pfn, xdat) in xreplicas:
                continue
            xreplicas.append((pfn, xdat))

        return xreplicas

    def resolve_replicas(self, files: list, use_vp: bool = False) -> list:
        """
        Populate filespec.replicas for each entry from `files` list.

            fdat.replicas = [{'ddmendpoint':'ddmendpoint', 'pfn':'replica', 'domain':'domain value'}]

        :param files: list of `FileSpec` objects (list)
        :param use_vp: True for VP jobs (bool)
        :raise: Exception in case of list_replicas() failure
        :return: list of files (list).
        """
        logger = self.logger
        xfiles = []

        for fdat in files:
            # skip fdat if needed (e.g. to properly handle OS ddms)
            xfiles.append(fdat)

        if not xfiles:  # no files for replica look-up
            return files

        # get the list of replicas
        try:
            replicas = self.list_replicas(xfiles, use_vp)
        except Exception as exc:
            raise exc

        files_lfn = dict(((e.scope, e.lfn), e) for e in xfiles)
        for replica in replicas:
            k = replica['scope'], replica['name']
            fdat = files_lfn.get(k)
            if not fdat:  # not requested replica
                continue

            # add the replicas to the fdat structure
            fdat = self.add_replicas(fdat, replica)

            # verify filesize and checksum values
            self.trace_report.update(validateStart=time.time())
            status = True
            if fdat.filesize != replica['bytes']:
                logger.warning("Filesize of input file=%s mismatched with value from Rucio replica: filesize=%s, replica.filesize=%s, fdat=%s",
                               fdat.lfn, fdat.filesize, replica['bytes'], fdat)
                status = False

            if not fdat.filesize:
                fdat.filesize = replica['bytes']
                logger.warning("Filesize value for input file=%s is not defined, assigning info from Rucio replica: filesize=%s", fdat.lfn, replica['bytes'])

            for ctype in ('adler32', 'md5'):
                if fdat.checksum.get(ctype) != replica[ctype] and replica[ctype]:
                    logger.warning("Checksum value of input file=%s mismatched with info got from Rucio replica: checksum=%s, replica.checksum=%s, fdat=%s",
                                   fdat.lfn, fdat.checksum, replica[ctype], fdat)
                    status = False

                if not fdat.checksum.get(ctype) and replica[ctype]:
                    fdat.checksum[ctype] = replica[ctype]

            if not status:
                logger.info("filesize and checksum verification done")
                self.trace_report.update(clientState="DONE")

        logger.info('Number of resolved replicas:\n' +
                    '\n'.join([f"lfn={f.lfn}: nr replicas={len(f.replicas or [])}, is_directaccess={f.is_directaccess(ensure_replica=False)}" for f in files]))

        return files

    def list_replicas(self, xfiles: list, use_vp: bool) -> list:
        """
        List Rucio replicas.

        Wrapper around rucio_client.list_replicas()

        :param xfiles: list of files objects (list)
        :param use_vp: True for VP jobs (bool)
        :raise: PilotException in case of list_replicas() failure
        :return: replicas (list).
        """
        # load replicas from Rucio
        from rucio.client import Client
        rucio_client = Client()
        location, diagnostics = self.detect_client_location(use_vp=use_vp)
        if diagnostics:
            self.logger.warning(f'failed to get client location for rucio: {diagnostics}')
            #raise PilotException(f"failed to get client location for rucio: {diagnostics}", code=ErrorCodes.RUCIOLOCATIONFAILED)

        query = {
            'schemes': ['srm', 'root', 'davs', 'gsiftp', 'https', 'storm', 'file'],
            'dids': [{"scope": e.scope, "name": e.lfn} for e in xfiles],
        }
        query.update(sort='geoip', client_location=location)
        # reset the schemas for VP jobs
        if use_vp:
            query['schemes'] = ['root']
            query['rse_expression'] = 'istape=False\\type=SPECIAL'
            query['ignore_availability'] = False

        # add signature lifetime for signed URL storages
        query.update(signature_lifetime=24 * 3600)  # note: default is otherwise 1h

        self.logger.info(f'calling rucio.list_replicas() with query={query}')

        try:
            replicas = rucio_client.list_replicas(**query)
        except Exception as exc:
            raise PilotException(f"Failed to get replicas from Rucio: {exc}", code=ErrorCodes.RUCIOLISTREPLICASFAILED) from exc

        replicas = list(replicas)
        self.logger.debug(f"replicas received from Rucio: {replicas}")

        return replicas

    def add_replicas(self, fdat: Any, replica: Any) -> Any:
        """
        Add the replicas to the fdat structure.

        :param fdat: fdat object (Any)
        :param replica: replica object (Any)
        :return: updated fdat object (Any).
        """
        fdat.replicas = []  # reset replicas list

        # sort replicas by priority value
        sorted_replicas = sorted(iter(list(replica.get('pfns', {}).items())), key=lambda x: x[1]['priority'])

        # prefer replicas from inputddms first
        #self.print_replicas(sorted_replicas)
        xreplicas = self.sort_replicas(sorted_replicas, fdat.inputddms)
        self.print_replicas(xreplicas)

        for pfn, xdat in xreplicas:

            if xdat.get('type') != 'DISK':  # consider only DISK replicas
                continue

            rinfo = {'pfn': pfn, 'ddmendpoint': xdat.get('rse'), 'domain': xdat.get('domain')}

            ## (TEMPORARY?) consider fspec.inputddms as a primary source for local/lan source list definition
            ## backward compartible logic -- FIX ME LATER if NEED
            ## in case we should rely on domain value from Rucio, just remove the overwrite line below
            rinfo['domain'] = 'lan' if rinfo['ddmendpoint'] in fdat.inputddms else 'wan'

            if not fdat.allow_lan and rinfo['domain'] == 'lan':
                continue
            if not fdat.allow_wan and rinfo['domain'] == 'wan':
                continue

            fdat.replicas.append(rinfo)

        if not fdat.replicas:
            name = replica.get('name', '[unknown lfn]')
            self.logger.warning(
                f'{name}: no replicas were selected (verify replica type, read_lan/wan, allow_lan/wan and domain values)'
            )
            self.logger.warning('e.g. check that read_lan is set for the relevant RSE in CRIC')

        return fdat

    def detect_client_location(self, use_vp: bool = False) -> dict:
        """
        Detect the client location.

        Open a UDP socket to a machine on the internet, to get the local IPv4 and IPv6
        addresses of the requesting client.

        :param use_vp: is it a VP site? (bool)
        :return: client location (dict).
        """
        diagnostics = ''
        client_location = {}

        ip = '0.0.0.0'
        try:
            import socket
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
        except socket.gaierror as e:
            diagnostics = f'failed to get socket info due to address-related error: {e}'
            self.logger.warning(diagnostics)
        except socket.timeout as e:
            diagnostics = f'failed to get socket info due to timeout: {e}'
            self.logger.warning(diagnostics)
        except socket.error as e:
            diagnostics = f'failed to get socket info due to general socket error: {e}'
            self.logger.warning(diagnostics)

        client_location['ip'] = ip
        site = os.environ.get('PILOT_RUCIO_SITENAME', 'unknown')
        client_location['site'] = site

        if use_vp:
            latitude = os.environ.get('RUCIO_LATITUDE')
            longitude = os.environ.get('RUCIO_LONGITUDE')
            if latitude and longitude:
                try:
                    client_location['latitude'] = float(latitude)
                    client_location['longitude'] = float(longitude)
                except ValueError:
                    diagnostics = f'client set latitude (\"{latitude}\") and longitude (\"{longitude}\") are not valid'
                    self.logger.warning(diagnostics)
            else:
                try:
                    response = requests.post('https://location.cern.workers.dev',
                                             json={"site": site},
                                             timeout=10)
                    if response.status_code == 200 and 'application/json' in response.headers.get('Content-Type', ''):
                        client_location = response.json()
                        # put back the site
                        client_location['site'] = site
                except requests.exceptions.Timeout as exc:
                    diagnostics = f'requests.post timed out: {exc}'
                    self.logger.warning(diagnostics)
                except requests.exceptions.RequestException as exc:
                    diagnostics = f'requests.post failed with general exception: {exc}'
                    self.logger.warning(diagnostics)

        self.logger.debug(f'will use client_location={client_location}')
        return client_location, diagnostics

    def resolve_surl(self, fspec: Any, protocol: dict, ddmconf: dict, **kwargs: dict) -> dict:
        """
        Resolve SURL.

        Only needed in StageOutClient.

        Get final destination SURL for file to be transferred.
        Can be customized at the level of specific copytool.

        :param fspec: `FileSpec` object (Any)
        :param protocol: suggested protocol (dict)
        :param ddmconf: full ddmconf data (dict)
        :param kwargs: extra kwargs (dict)
        :return: dictionary with keys ('pfn', 'ddmendpoint') (dict).
        """
        raise NotImplementedError()

    def transfer_files(self, copytool: Any, files: list, activity: list, **kwargs: dict) -> list:
        """
        Transfer the files.

        Apply transfer of given `files` using passed `copytool` module.
        Should be implemented by custom Staging Client.

        :param copytool: copytool module (Any)
        :param files: list of `FileSpec` objects (list)
        :param activity: list of activity names used to determine appropriate copytool (list)
        :param kwargs: extra kwargs to be passed to copytool transfer handler (dict)
        :raise: PilotException in case of controlled error.
        """
        raise NotImplementedError()

    def transfer(self, files: list, activity: list or str = 'default', raise_exception: bool = True, **kwargs: dict) -> list:  # noqa: C901
        """
        Perform file transfer.

        Automatically stage passed files using copy tools related to given `activity`.

        :param files: list of `FileSpec` objects (list)
        :param activity: list of activity names used to determine appropriate copytool (list or str)
        :param raise_exception: boolean flag used to ignore transfer errors
        :param kwargs: extra kwargs to be passed to copytool transfer handler (dict)
        :raise: PilotException in case of controlled error if `raise_exception` is `True`
        :return: list of processed `FileSpec` objects (list).
        """
        self.trace_report.update(relativeStart=time.time(), transferStart=time.time())

        if isinstance(activity, str):
            activity = [activity]
        if 'default' not in activity:
            activity.append('default')

        copytools = None
        for aname in activity:
            copytools = self.acopytools.get(aname)
            if copytools:
                break

        if not copytools:
            raise PilotException(f'failed to resolve copytool by preferred activities={activity}, acopytools={self.acopytools}',
                                 code=ErrorCodes.UNKNOWNCOPYTOOL)

        # populate inputddms if needed
        self.prepare_inputddms(files)

        # initialize ddm_activity name for requested files if not set
        for fspec in files:
            if fspec.ddm_activity:  # skip already initialized data
                continue
            if self.mode == 'stage-in':
                if os.environ.get('PILOT_ES_EXECUTOR_TYPE', 'generic') == 'raythena':
                    fspec.status = 'no_transfer'

                fspec.ddm_activity = [_f for _f in
                                      ['read_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'read_wan'] if _f]
            else:
                fspec.ddm_activity = [_f for _f in
                                      ['write_lan' if fspec.ddmendpoint in fspec.inputddms else None, 'write_wan'] if _f]
        caught_errors = []

        for name in copytools:

            # get remain files that need to be transferred by copytool
            remain_files = [e for e in files if e.status not in ['remote_io', 'transferred', 'no_transfer']]

            if not remain_files:
                break

            try:
                if name not in self.copytool_modules:
                    raise PilotException(f'passed unknown copytool with name={name} .. skipped',
                                         code=ErrorCodes.UNKNOWNCOPYTOOL)

                module = self.copytool_modules[name]['module_name']
                self.logger.info(f'trying to use copytool={name} for activity={activity}')
                copytool = __import__(f'pilot.copytool.{module}', globals(), locals(), [module], 0)
                #self.trace_report.update(protocol=name)

            except PilotException as exc:
                caught_errors.append(exc)
                self.logger.debug(f'error: {exc}')
                continue
            except Exception as exc:
                self.logger.warning(f'failed to import copytool module={module}, error={exc}')
                continue

            try:
                result = self.transfer_files(copytool, remain_files, activity, **kwargs)
                self.logger.debug(f'transfer_files() using copytool={copytool} completed with result={result}')
                break
            except PilotException as exc:
                self.logger.warning(f'failed to transfer_files() using copytool={copytool} .. skipped; error={exc}')
                caught_errors.append(exc)
            except TimeoutException as exc:
                self.logger.warning(f'function timed out: {exc}')
                caught_errors.append(exc)
            except Exception as exc:
                self.logger.warning(f'failed to transfer files using copytool={copytool} .. skipped; error={exc}')
                caught_errors.append(exc)
                import traceback
                self.logger.error(traceback.format_exc())

            if caught_errors and isinstance(caught_errors[-1], PilotException) and \
                    caught_errors[-1].get_error_code() == ErrorCodes.MISSINGOUTPUTFILE:
                raise caught_errors[-1]

        remain_files = [fspec for fspec in files if fspec.status not in ['remote_io', 'transferred', 'no_transfer']]

        if remain_files:  # failed or incomplete transfer
            # propagate message from first error back up
            # errmsg = str(caught_errors[0]) if caught_errors else ''
            if caught_errors and "Cannot authenticate" in str(caught_errors):
                code = ErrorCodes.STAGEINAUTHENTICATIONFAILURE if self.mode == 'stage-in' else ErrorCodes.STAGEOUTAUTHENTICATIONFAILURE  # is it stage-in/out?
            elif caught_errors and "bad queue configuration" in str(caught_errors):
                code = ErrorCodes.BADQUEUECONFIGURATION
            elif caught_errors and isinstance(caught_errors[0], PilotException):
                code = caught_errors[0].get_error_code()
                # errmsg = caught_errors[0].get_last_error()
            elif caught_errors and isinstance(caught_errors[0], TimeoutException):
                code = ErrorCodes.STAGEINTIMEOUT if self.mode == 'stage-in' else ErrorCodes.STAGEOUTTIMEOUT  # is it stage-in/out?
                self.logger.warning(f'caught time-out exception: {caught_errors[0]}')
            else:
                code = ErrorCodes.STAGEINFAILED if self.mode == 'stage-in' else ErrorCodes.STAGEOUTFAILED  # is it stage-in/out?
            details = str(caught_errors) + ":" + f'failed to transfer files using copytools={copytools}'
            self.logger.fatal(details)
            if raise_exception:
                raise PilotException(details, code=code)

        return files

    def require_protocols(self, files: list, copytool: Any, activity: list or str, local_dir: str = ''):
        """
        Require protocols.

        Populates fspec.protocols and fspec.turl for each entry in `files` according to preferred fspec.ddm_activity

        :param files: list of `FileSpec` objects (list)
        :param copytool: copytool module (Any)
        :param activity: list of activity names used to determine appropriate copytool (list or str)
        :param local_dir: local directory (str).
        """
        allowed_schemas = getattr(copytool, 'allowed_schemas', None)

        if self.infosys and self.infosys.queuedata:
            copytool_name = copytool.__name__.rsplit('.', 1)[-1]
            allowed_schemas = self.infosys.queuedata.resolve_allowed_schemas(activity, copytool_name) or allowed_schemas

        if local_dir:
            for fdat in files:
                if not local_dir.endswith('/'):
                    local_dir += '/'
                fdat.protocols = [{'endpoint': local_dir, 'flavour': '', 'id': 0, 'path': ''}]
        else:
            files = self.resolve_protocols(files)

        ddmconf = self.infosys.resolve_storage_data()

        for fspec in files:

            protocols = self.resolve_protocol(fspec, allowed_schemas)
            if not protocols and 'mv' not in self.infosys.queuedata.copytools:  # no protocols found
                error = f'Failed to resolve protocol for file={fspec.lfn}, allowed_schemas={allowed_schemas}, fspec={fspec}'
                self.logger.error(f"resolve_protocol: {error}")
                raise PilotException(error, code=ErrorCodes.NOSTORAGEPROTOCOL)

            # take first available protocol for copytool: FIX ME LATER if need (do iterate over all allowed protocols?)
            protocol = protocols[0]

            self.logger.info(f"Resolved protocol to be used for transfer: \'{protocol}\': lfn=\'{fspec.lfn}\'")

            resolve_surl = getattr(copytool, 'resolve_surl', None)
            if not callable(resolve_surl):
                resolve_surl = self.resolve_surl

            r = resolve_surl(fspec, protocol, ddmconf, local_dir=local_dir)  # pass ddmconf for possible custom look up at the level of copytool
            if r.get('surl'):
                fspec.turl = r['surl']

            if r.get('ddmendpoint'):
                fspec.ddmendpoint = r['ddmendpoint']

    def resolve_protocols(self, files: list) -> list:
        """
        Resolve protocols.

        Populates filespec.protocols for each entry from `files` according to preferred `fspec.ddm_activity` value

        fdat.protocols = [dict(endpoint, path, flavour), ..]

        :param files: list of `FileSpec` objects (list)
        :return: list of `files` object (list).
        """
        ddmconf = self.infosys.resolve_storage_data()

        for fdat in files:
            ddm = ddmconf.get(fdat.ddmendpoint)
            if not ddm:
                error = f'Failed to resolve output ddmendpoint by name={fdat.ddmendpoint} (from PanDA), please check configuration.'
                self.logger.error(f"resolve_protocols: {error}, fspec={fdat}")
                raise PilotException(error, code=ErrorCodes.NOSTORAGE)

            protocols = []
            for aname in fdat.ddm_activity:
                protocols = ddm.arprotocols.get(aname)
                if protocols:
                    break

            fdat.protocols = protocols

        return files

    @classmethod
    def resolve_protocol(cls, fspec: Any, allowed_schemas: Any = None) -> list:
        """
        Resolve protocol.

        Resolve protocol according to allowed schema

        :param fspec: `FileSpec` instance (list)
        :param allowed_schemas: list of allowed schemas or any if None (Any)
        :return: list of dict(endpoint, path, flavour) (list).
        """
        if not fspec.protocols:
            return []

        protocols = []

        allowed_schemas = allowed_schemas or [None]
        for schema in allowed_schemas:
            for pdat in fspec.protocols:
                if schema is None or pdat.get('endpoint', '').startswith(f"{schema}://"):
                    protocols.append(pdat)

        return protocols


class StageInClient(StagingClient):
    """Stage-in client."""

    mode = "stage-in"

    def resolve_replica(self, fspec: Any, primary_schemas: Any = None, allowed_schemas: Any = None, domain: Any = None) -> dict or None:
        """
        Resolve replica.

        Resolve input replica (matched by `domain` if needed) first according to `primary_schemas`,
        if not found then look up within `allowed_schemas`.

        Primary schemas ignore replica priority (used to resolve direct access replica, which could be not with top priority set).

        :param fspec: input `FileSpec` objects (Any)
        :param primary_schemas: list of primary schemas or any if None (Any)
        :param allowed_schemas: list of allowed schemas or any if None (Any)
        :param domain: domain value to match (Any)
        :return: dict(surl, ddmendpoint, pfn, domain) or None if replica not found (dict or None).
        """
        if not fspec.replicas:
            self.logger.warning('resolve_replica() received no fspec.replicas')
            return None

        allowed_schemas = allowed_schemas or [None]
        primary_replica, replica = None, None

        # group by ddmendpoint to look up related surl/srm value
        replicas = {}

        for rinfo in fspec.replicas:

            replicas.setdefault(rinfo['ddmendpoint'], []).append(rinfo)

            if rinfo['domain'] != domain:
                continue
            if primary_schemas and not primary_replica:  # look up primary schemas if requested
                primary_replica = self.get_preferred_replica([rinfo], primary_schemas)
            if not replica:
                replica = self.get_preferred_replica([rinfo], allowed_schemas)

            if replica and primary_replica:
                break

        replica = primary_replica or replica

        if not replica:  # replica not found
            schemas = 'any' if not allowed_schemas[0] else ','.join(allowed_schemas)
            pschemas = 'any' if primary_schemas and not primary_schemas[0] else ','.join(primary_schemas or [])

            error = f'Failed to find replica for file={fspec.lfn}, domain={domain}, allowed_schemas={schemas}, pschemas={pschemas}, fspec={fspec}'
            self.logger.info("resolve_replica: %s", error)
            return None

        # prefer SRM protocol for surl -- to be verified, can it be deprecated?
        rse_replicas = replicas.get(replica['ddmendpoint'], [])
        surl = self.get_preferred_replica(rse_replicas, ['srm']) or rse_replicas[0]
        self.logger.info(f"[stage-in] surl (srm replica) from Rucio: pfn={surl['pfn']}, ddmendpoint={surl['ddmendpoint']}")

        return {'surl': surl['pfn'], 'ddmendpoint': replica['ddmendpoint'], 'pfn': replica['pfn'], 'domain': replica['domain']}

    def resolve_surl(self, fspec: Any, protocol: dict, ddmconf: dict, **kwargs: dict) -> dict:
        """
        Resolve SURL.

        Only needed in StageOutClient.

        Get final destination SURL for file to be transferred.
        Can be customized at the level of specific copytool.

        :param fspec: `FileSpec` object (Any)
        :param protocol: suggested protocol (dict)
        :param ddmconf: full ddmconf data (dict)
        :param kwargs: extra kwargs (dict)
        :return: dictionary with keys ('pfn', 'ddmendpoint') (dict).
        """
        raise NotImplementedError()

    def get_direct_access_variables(self, job: Any) -> (bool, str):
        """
        Return the direct access settings for the PQ.

        :param job: job object (Any)
        :return: allow_direct_access (bool), direct_access_type (str).
        """
        allow_direct_access, direct_access_type = False, ''
        if self.infosys.queuedata:  # infosys is initialized
            allow_direct_access = self.infosys.queuedata.direct_access_lan or self.infosys.queuedata.direct_access_wan
            if self.infosys.queuedata.direct_access_lan:
                direct_access_type = 'LAN'
            if self.infosys.queuedata.direct_access_wan:
                direct_access_type = 'WAN'
        else:
            self.logger.info('infosys.queuedata is not initialized: direct access mode will be DISABLED by default')

        if job and not job.is_analysis() and job.transfertype != 'direct':  # task forbids direct access
            allow_direct_access = False
            self.logger.info(f'switched off direct access mode for production job since transfertype={job.transfertype}')

        return allow_direct_access, direct_access_type

    def transfer_files(self, copytool: Any, files: list, activity: list = None, **kwargs: dict) -> list:  # noqa: C901
        """
        Automatically stage in files using the selected copy tool module.

        :param copytool: copytool module (Any)
        :param files: list of `FileSpec` objects (list)
        :param activity: list of activity names used to determine appropriate copytool (list or None)
        :param kwargs: extra kwargs to be passed to copytool transfer handler (dict)
        :return: list of processed `FileSpec` objects (list)
        :raise: PilotException in case of controlled error.
        """
        if getattr(copytool, 'require_replicas', False) and files:
            if files[0].replicas is None:  # look up replicas only once
                files = self.resolve_replicas(files, use_vp=kwargs.get('use_vp', False))

            allowed_schemas = getattr(copytool, 'allowed_schemas', None)

            if self.infosys and self.infosys.queuedata:
                copytool_name = copytool.__name__.rsplit('.', 1)[-1]
                allowed_schemas = self.infosys.queuedata.resolve_allowed_schemas(activity, copytool_name) or allowed_schemas

            # overwrite allowed_schemas for VP jobs
            if kwargs.get('use_vp', False):
                allowed_schemas = ['root']
                self.logger.debug('overwrote allowed_schemas for VP job: %s', str(allowed_schemas))

            for fspec in files:
                resolve_replica = getattr(copytool, 'resolve_replica', None)
                resolve_replica = self.resolve_replica if not callable(resolve_replica) else resolve_replica

                replica = None

                # process direct access logic  ## TODO move to upper level, should not be dependent on copytool (anisyonk)
                # check local replicas first
                if fspec.allow_lan:
                    # prepare schemas which will be used to look up first the replicas allowed for direct access mode
                    primary_schemas = (self.direct_localinput_allowed_schemas if fspec.direct_access_lan and
                                       fspec.is_directaccess(ensure_replica=False) else None)
                    replica = resolve_replica(fspec, primary_schemas, allowed_schemas, domain='lan')
                else:
                    self.logger.info("[stage-in] LAN access is DISABLED for lfn=%s (fspec.allow_lan=%s)", fspec.lfn, fspec.allow_lan)

                if not replica and fspec.allow_lan:
                    self.logger.info("[stage-in] No LAN replica found for lfn=%s, primary_schemas=%s, allowed_schemas=%s",
                                     fspec.lfn, primary_schemas, allowed_schemas)

                # check remote replicas
                if not replica and fspec.allow_wan:
                    # prepare schemas which will be used to look up first the replicas allowed for direct access mode
                    primary_schemas = (self.direct_remoteinput_allowed_schemas if fspec.direct_access_wan and
                                       fspec.is_directaccess(ensure_replica=False) else None)
                    xschemas = self.remoteinput_allowed_schemas
                    allowed_schemas = [schema for schema in allowed_schemas if schema in xschemas] if allowed_schemas else xschemas
                    replica = resolve_replica(fspec, primary_schemas, allowed_schemas, domain='wan')

                if not replica and fspec.allow_wan:
                    self.logger.info("[stage-in] No WAN replica found for lfn=%s, primary_schemas=%s, allowed_schemas=%s",
                                     fspec.lfn, primary_schemas, allowed_schemas)
                if not replica:
                    raise ReplicasNotFound(f'No replica found for lfn={fspec.lfn} (allow_lan={fspec.allow_lan}, allow_wan={fspec.allow_wan})')

                if replica.get('pfn'):
                    fspec.turl = replica['pfn']
                if replica.get('surl'):
                    fspec.surl = replica['surl']  # TO BE CLARIFIED if it's still used and need
                if replica.get('ddmendpoint'):
                    fspec.ddmendpoint = replica['ddmendpoint']
                if replica.get('domain'):
                    fspec.domain = replica['domain']

                self.logger.info("[stage-in] found replica to be used for lfn=%s: ddmendpoint=%s, pfn=%s", fspec.lfn, fspec.ddmendpoint, fspec.turl)

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_input_protocols', False) and files:
            args = kwargs.get('args')
            input_dir = kwargs.get('input_dir') if not args else args.input_dir
            self.require_protocols(files, copytool, activity, local_dir=input_dir)

        # mark direct access files with status=remote_io
        self.set_status_for_direct_access(files, kwargs.get('workdir', ''))

        # get remain files that need to be transferred by copytool
        remain_files = [e for e in files if e.status not in ['direct', 'remote_io', 'transferred', 'no_transfer']]

        if not remain_files:
            return files

        if not copytool.is_valid_for_copy_in(remain_files):
            msg = f'input is not valid for transfers using copytool={copytool}'
            self.logger.warning(msg)
            self.logger.debug('input: %s', remain_files)
            self.trace_report.update(clientState='NO_REPLICA', stateReason=msg)
            self.trace_report.send()
            raise PilotException('invalid input data for transfer operation')

        if self.infosys:
            if self.infosys.queuedata:
                kwargs['copytools'] = self.infosys.queuedata.copytools
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()
        kwargs['activity'] = activity

        # verify file sizes and available space for stage-in
        if getattr(copytool, 'check_availablespace', True):
            if self.infosys.queuedata.maxinputsize != -1:
                self.check_availablespace(remain_files)
            else:
                self.logger.info('skipping input file size check since maxinputsize=-1')

        # add the trace report
        kwargs['trace_report'] = self.trace_report
        self.logger.info('ready to transfer (stage-in) files: %s', remain_files)

        # is there an override in catchall to allow mv to final destination (relevant for mv copytool only)
        kwargs['mvfinaldest'] = self.allow_mvfinaldest(kwargs.get('catchall', ''))

        # use bulk downloads if necessary
        # if kwargs['use_bulk_transfer']
        # return copytool.copy_in_bulk(remain_files, **kwargs)
        return copytool.copy_in(remain_files, **kwargs)

    def set_status_for_direct_access(self, files: list, workdir: str):
        """
        Update the FileSpec status with 'remote_io' for direct access mode.

        Should be called only once since the function sends traces.

        :param files: list of FileSpec objects (list)
        :param workdir: work directory (str).
        """
        for fspec in files:
            direct_lan = (fspec.domain == 'lan' and fspec.direct_access_lan and
                          fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=self.direct_localinput_allowed_schemas))
            direct_wan = (fspec.domain == 'wan' and fspec.direct_access_wan and
                          fspec.is_directaccess(ensure_replica=True, allowed_replica_schemas=self.direct_remoteinput_allowed_schemas))

            # testing direct acess
            #if 'CYFRONET' in os.environ.get('PILOT_SITENAME', ''):
            #    if '.root.' in fspec.lfn:
            #        direct_lan = True

            if not direct_lan and not direct_wan:
                self.logger.debug('direct lan/wan transfer will not be used for lfn=%s', fspec.lfn)
            self.logger.debug('lfn=%s, direct_lan=%s, direct_wan=%s, direct_access_lan=%s, direct_access_wan=%s, '
                              'direct_localinput_allowed_schemas=%s, remoteinput_allowed_schemas=%s, domain=%s',
                              fspec.lfn, direct_lan, direct_wan, fspec.direct_access_lan, fspec.direct_access_wan,
                              str(self.direct_localinput_allowed_schemas), str(self.direct_remoteinput_allowed_schemas), fspec.domain)

            if direct_lan or direct_wan:
                fspec.status_code = 0
                fspec.status = 'remote_io'

                alrb_xcache_proxy = os.environ.get('ALRB_XCACHE_PROXY', None)
                if alrb_xcache_proxy and direct_lan:  #fspec.is_directaccess(ensure_replica=False):
                    fspec.turl = '${ALRB_XCACHE_PROXY}' + fspec.turl

                self.logger.info('stage-in: direct access (remote i/o) will be used for lfn=%s (direct_lan=%s, direct_wan=%s), turl=%s',
                                 fspec.lfn, direct_lan, direct_wan, fspec.turl)

                # send trace
                localsite = os.environ.get('RUCIO_LOCAL_SITE_ID')
                localsite = localsite or fspec.ddmendpoint
                self.trace_report.update(localSite=localsite, remoteSite=fspec.ddmendpoint, filesize=fspec.filesize)
                self.trace_report.update(filename=fspec.lfn, guid=fspec.guid.replace('-', ''))
                self.trace_report.update(scope=fspec.scope, dataset=fspec.dataset)
                self.trace_report.update(url=fspec.turl, clientState='FOUND_ROOT', stateReason='direct_access')

                # do not send the trace report at this point if remote file verification is to be done
                # note also that we can't verify the files at this point since root will not be available from inside
                # the rucio container
                if config.Pilot.remotefileverification_log:
                    # store the trace report for later use (the trace report class inherits from dict, so just write it as JSON)
                    # outside of the container, it will be available in the normal work dir
                    # use the normal work dir if we are not in a container
                    _workdir = workdir if os.path.exists(workdir) else '.'
                    path = os.path.join(_workdir, config.Pilot.base_trace_report)
                    if not os.path.exists(_workdir):
                        path = os.path.join('/srv', config.Pilot.base_trace_report)
                    if not os.path.exists(path):
                        self.logger.debug(f'writing base trace report to: {path}')
                        write_json(path, self.trace_report)
                else:
                    self.trace_report.send()

    def check_availablespace(self, files: list):
        """
        Verify that enough local space is available to stage in and run the job.

        :param files: list of FileSpec objects (list)
        :raise: PilotException in case of not enough space or total input size too large.
        """
        for f in files:
            self.logger.debug(f'lfn={f.lfn} filesize={f.filesize} accessmode={f.accessmode}')

        maxinputsize = convert_mb_to_b(get_maximum_input_sizes())
        totalsize = reduce(lambda x, y: x + y.filesize, files, 0)

        # verify total filesize
        if maxinputsize and totalsize > maxinputsize:
            error = f"too many/too large input files ({len(files)}). total file size={totalsize} B > maxinputsize={maxinputsize} B"
            raise SizeTooLarge(error)

        self.logger.info(f"total input file size={totalsize} B within allowed limit={maxinputsize} B (zero value means unlimited)")

        # get available space
        try:
            disk_space = get_local_disk_space(os.getcwd())
        except PilotException as exc:
            diagnostics = exc.get_detail()
            self.logger.warning(f'exception caught while executing df: {diagnostics} (ignoring)')
        else:
            if disk_space:
                available_space = convert_mb_to_b(disk_space)
                self.logger.info(f"locally available space: {available_space} B")

                # are we within the limit?
                if totalsize > available_space:
                    error = f"not enough local space for staging input files and run the job (need {totalsize} B, but only have {available_space} B)"
                    raise NoLocalSpace(error)
            else:
                self.logger.warning('get_local_disk_space() returned None')


class StageOutClient(StagingClient):
    """Stage-out client."""

    mode = "stage-out"

    def prepare_destinations(self, files: list, activities: list or str, alt_exclude: list = None) -> list:
        """
        Resolve destination RSE (filespec.ddmendpoint) for each entry from `files` according to requested `activities`.

        Apply Pilot-side logic to choose proper destination.

        :param files: list of FileSpec objects to be processed (list)
        :param activities: ordered list of activities to be used to resolve astorages (list or str)
        :param alt_exclude: global list of destinations that should be excluded / not used for alternative stage-out
        :return: updated fspec entries (list).
        """

        alt_exclude = list(alt_exclude or [])

        if not self.infosys.queuedata:  # infosys is not initialized: not able to fix destination if need, nothing to do
            return files

        if isinstance(activities, str):
            activities = [activities]

        if not activities:
            raise PilotException("Failed to resolve destination: passed empty activity list. Internal error.",
                                 code=ErrorCodes.INTERNALPILOTPROBLEM, state='INTERNAL_ERROR')

        astorages = self.infosys.queuedata.astorages or {}
        storages = None
        activity = activities[0]
        for a in activities:
            storages = astorages.get(a, {})
            if storages:
                break

        if not storages:
            if 'mv' in self.infosys.queuedata.copytools:
                return files

            act = ','.join(activities)
            raise PilotException(f"Failed to resolve destination: no associated storages defined for activity={activity} ({act})",
                                 code=ErrorCodes.NOSTORAGE, state='NO_ASTORAGES_DEFINED')

        def resolve_alt_destination(primary, exclude=None):
            """ resolve alt destination as the next to primary entry not equal to `primary` and `exclude` """

            cur = storages.index(primary) if primary in storages else 0
            inext = (cur + 1) % len(storages)  # cycle storages, take the first elem when reach end
            exclude = set([primary] + list(exclude or []))
            alt = None
            for attempt in range(len(exclude) or 1):  # apply several tries to jump exclude entries (in case of dublicated data will stack)
                inext = (cur + 1) % len(storages)  # cycle storages, start from the beginning when reach end
                if storages[inext] not in exclude:
                    alt = storages[inext]
                    break
                cur += 1
            return alt

        # default destination
        ddm = storages[0]  # take the fist choice for now, extend the logic later if need
        ddm_alt = resolve_alt_destination(ddm, exclude=alt_exclude)

        self.logger.info(f"[prepare_destinations][{activity}]: allowed (local) destinations: {storages}, alt_exclude={alt_exclude}")
        self.logger.info(f"[prepare_destinations][{activity}]: resolved default destination: ddm={ddm}, ddm_alt={ddm_alt}")

        for e in files:
            if not e.ddmendpoint:  # no preferences => use default destination
                self.logger.info("[prepare_destinations][%s]: fspec.ddmendpoint is not set for lfn=%s"
                                 " .. will use default ddm=%s as (local) destination; ddm_alt=%s", activity, e.lfn, ddm, ddm_alt)
                e.ddmendpoint = ddm
                e.ddmendpoint_alt = ddm_alt
            #elif e.ddmendpoint not in storages and is_unified:  ## customize nucleus logic if need
            #   pass
            elif e.ddmendpoint not in storages:  # fspec.ddmendpoint is not in associated storages => use it as (non local) alternative destination
                self.logger.info("[prepare_destinations][%s]: Requested fspec.ddmendpoint=%s is not in the list of allowed (local) destinations"
                                 " .. will consider default ddm=%s as primary and set %s as alt. location", activity, e.ddmendpoint, ddm, e.ddmendpoint)
                e.ddmendpoint_alt = e.ddmendpoint if e.ddmendpoint not in alt_exclude else None
                e.ddmendpoint = ddm  # use default destination, check/verify nucleus case
            else:  # set corresponding ddmendpoint_alt if exist (next entry in cycled storages list)
                e.ddmendpoint_alt = resolve_alt_destination(e.ddmendpoint, exclude=alt_exclude)

            self.logger.info("[prepare_destinations][%s]: use ddmendpoint_alt=%s for fspec.ddmendpoint=%s",
                             activity, e.ddmendpoint_alt, e.ddmendpoint)

        return files

    @classmethod
    def get_path(cls, scope: str, lfn: str) -> str:
        """
        Construct a partial Rucio PFN using the scope and the LFN.

            <prefix=rucio>/<scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>

        :param scope: replica scope (str)
        :param lfn: repliva LFN (str)
        :return: constructed path (str).
        """
        s = f'{scope}:{lfn}'
        hash_hex = hashlib.md5(s.encode('utf-8')).hexdigest()

        # exclude prefix from the path: this should be properly considered in protocol/AGIS for today
        paths = scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
        paths = [_f for _f in paths if _f]  # remove empty parts to avoid double /-chars

        return '/'.join(paths)

    def resolve_surl(self, fspec: Any, protocol: dict, ddmconf: dict, **kwargs: dict) -> dict:
        """
        Resolve SURL.

        Get final destination SURL for file to be transferred.
        Can be customized at the level of specific copytool.

        :param fspec: `FileSpec` object (Any)
        :param protocol: suggested protocol (dict)
        :param ddmconf: full ddmconf data (dict)
        :param kwargs: extra kwargs (dict)
        :return: dictionary with keys ('pfn', 'ddmendpoint') (dict).
        """
        local_dir = kwargs.get('local_dir', '')
        if not local_dir:
            # consider only deterministic sites (output destination) - unless local input/output
            ddm = ddmconf.get(fspec.ddmendpoint)
            if not ddm:
                raise PilotException(f'Failed to resolve ddmendpoint by name={fspec.ddmendpoint}')

            # path = protocol.get('path', '').rstrip('/')
            # if not (ddm.is_deterministic or (path and path.endswith('/rucio'))):
            if not ddm.is_deterministic:
                raise PilotException(f'resolve_surl(): Failed to construct SURL for non deterministic '
                                     f'ddm={fspec.ddmendpoint}: NOT IMPLEMENTED', code=ErrorCodes.NONDETERMINISTICDDM)

        surl = protocol.get('endpoint', '') + os.path.join(protocol.get('path', ''), self.get_path(fspec.scope, fspec.lfn))

        return {'surl': surl}

    def transfer_files(self, copytool: Any, files: list, activity: list, **kwargs: dict) -> list:
        """
        Transfer files.

        Automatically stage out files using the selected copy tool module.

        :param copytool: copytool module (Any)
        :param files: list of `FileSpec` objects (list)
        :param activity: ordered list of preferred activity names to resolve SE protocols (list)
        :param kwargs: extra kwargs to be passed to copytool transfer handler (dict)
        :return: the output of the copytool transfer operation (list)
        :raise: PilotException in case of controlled error.
        """
        # check if files exist before actual processing
        # populate filesize if needed, calculate checksum
        for fspec in files:
            if not fspec.ddmendpoint:  # ensure that output destination is properly set
                if 'mv' not in self.infosys.queuedata.copytools:
                    msg = f'no output RSE defined for file={fspec.lfn}'
                    self.logger.error(msg)
                    raise PilotException(msg, code=ErrorCodes.NOSTORAGE, state='NO_OUTPUTSTORAGE_DEFINED')

            pfn = fspec.surl or getattr(fspec, 'pfn', None) or os.path.join(kwargs.get('workdir', ''), fspec.lfn) or \
                os.path.join(os.path.join(kwargs.get('workdir', ''), '..'), fspec.lfn)
            if not os.path.exists(pfn) or not os.access(pfn, os.R_OK):
                msg = f"output pfn file/directory does not exist: {pfn}"
                self.logger.error(msg)
                self.trace_report.update(clientState='MISSINGOUTPUTFILE', stateReason=msg, filename=fspec.lfn)
                self.trace_report.send()
                raise PilotException(msg, code=ErrorCodes.MISSINGOUTPUTFILE, state="FILE_INFO_FAIL")
            if not fspec.filesize:
                fspec.filesize = os.path.getsize(pfn)

            if not fspec.filesize:
                msg = f'output file has size zero: {fspec.lfn}'
                self.logger.fatal(msg)
                raise PilotException(msg, code=ErrorCodes.ZEROFILESIZE, state="ZERO_FILE_SIZE")

            fspec.surl = pfn
            fspec.activity = activity
            if os.path.isfile(pfn) and not fspec.checksum.get(config.File.checksum_type):
                try:
                    fspec.checksum[config.File.checksum_type] = calculate_checksum(pfn,
                                                                                   algorithm=config.File.checksum_type)
                except (FileHandlingFailure, NotImplementedError) as exc:
                    raise exc
                except Exception as exc:
                    raise exc

        # prepare files (resolve protocol/transfer url)
        if getattr(copytool, 'require_protocols', True) and files:
            output_dir = kwargs.get('output_dir', '')
            self.require_protocols(files, copytool, activity, local_dir=output_dir)

        if not copytool.is_valid_for_copy_out(files):
            self.logger.warning(f'input is not valid for transfers using copytool={copytool}')
            self.logger.debug(f'input: {files}')
            raise PilotException('invalid input for transfer operation')

        self.logger.info(f'ready to transfer (stage-out) files: {files}')

        if self.infosys:
            kwargs['copytools'] = self.infosys.queuedata.copytools

            # some copytools will need to know endpoint specifics (e.g. the space token) stored in ddmconf, add it
            kwargs['ddmconf'] = self.infosys.resolve_storage_data()

        if not files:
            msg = 'nothing to stage-out - an internal Pilot error has occurred'
            self.logger.fatal(msg)
            raise PilotException(msg, code=ErrorCodes.INTERNALPILOTPROBLEM)

        # add the trace report
        kwargs['trace_report'] = self.trace_report

        # is there an override in catchall to allow mv to final destination (relevant for mv copytool only)
        kwargs['mvfinaldest'] = self.allow_mvfinaldest(kwargs.get('catchall', ''))

        return copytool.copy_out(files, **kwargs)

#class StageInClientAsync(object):
#
#    def __init__(self, site):
#        raise NotImplementedError
#
#    def queue(self, files):
#        raise NotImplementedError
#
#    def is_transferring(self):
#        raise NotImplementedError
#
#    def start(self):
#        raise NotImplementedError
#
#    def finish(self):
#        raise NotImplementedError
#
#    def status(self):
#        raise NotImplementedError
#
#
#class StageOutClientAsync(object):
#
#    def __init__(self, site):
#        raise NotImplementedError
#
#    def queue(self, files):
#        raise NotImplementedError
#
#    def is_transferring(self):
#        raise NotImplementedError
#
#    def start(self):
#        raise NotImplementedError
#
#    def finish(self):
#        raise NotImplementedError
#
#    def status(self):
#        raise NotImplementedError
