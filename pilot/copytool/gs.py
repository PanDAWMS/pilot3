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
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-2023
# - Shuwei Ye, yesw@bnl.gov, 2021

"""GS copy tool."""

import logging
import os
import pathlib
import re
import subprocess
from glob import glob
from typing import Any

from pilot.info import infosys

try:
    from google.cloud import storage
except Exception:
    storage_client = None
else:
    storage_client = storage.Client()

from .common import resolve_common_transfer_errors
from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.util.config import config

logger = logging.getLogger(__name__)
errors = ErrorCodes()

require_replicas = False    ## indicates if given copytool requires input replicas to be resolved
require_input_protocols = True    ## indicates if given copytool requires input protocols and manual generation of input replicas
require_protocols = True  ## indicates if given copytool requires protocols to be resolved first for stage-out

allowed_schemas = ['gs', 'srm', 'gsiftp', 'https', 'davs', 'root']


def is_valid_for_copy_in(files: list) -> bool:
    """
    Determine if this copytool is valid for input for the given file list.

    Placeholder.

    :param files: list of FileSpec objects (list).
    :return: always True (for now) (bool).
    """
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files: list) -> bool:
    """
    Determine if this copytool is valid for output for the given file list.

    Placeholder.

    :param files: list of FileSpec objects (list).
    :return: always True (for now) (bool).
    """
    # for f in files:
    #    if not all(key in f for key in ('name', 'source', 'destination')):
    #        return False
    return True  ## FIX ME LATER


def resolve_surl(fspec: Any, protocol: dict, ddmconf: dict, **kwargs: dict) -> dict:
    """
    Get final destination SURL for file to be transferred to Objectstore.

    Can be customized at the level of specific copytool.

    :param fspec: file spec data (Any)
    :param protocol: suggested protocol (dict)
    :param ddmconf: full ddm storage data (dict)
    :param kwargs: kwargs dictionary (dict)
    :return: SURL dictionary {'surl': surl} (dict).
    """
    try:
        pandaqueue = infosys.pandaqueue
    except Exception:
        pandaqueue = ""
    if pandaqueue is None:
        pandaqueue = ""

    ddm = ddmconf.get(fspec.ddmendpoint)
    if not ddm:
        raise PilotException(f'failed to resolve ddmendpoint by name={fspec.ddmendpoint}')

    dataset = fspec.dataset
    if dataset:
        dataset = dataset.replace("#{pandaid}", os.environ['PANDAID'])
    else:
        dataset = ""

    remote_path = os.path.join(protocol.get('path', ''), pandaqueue, dataset)
    surl = protocol.get('endpoint', '') + remote_path
    logger.info(f'for GCS bucket, set surl={surl}')

    # example:
    #   protocol = {u'path': u'/atlas-eventservice', u'endpoint': u's3://s3.cern.ch:443/', u'flavour': u'AWS-S3-SSL', u'id': 175}
    #   surl = 's3://s3.cern.ch:443//atlas-eventservice/EventService_premerge_24706191-5013009653-24039149400-322-5.tar'
    return {'surl': surl}


def copy_in(files: list, **kwargs: dict) -> list:
    """
    Download given files from a GCS bucket.

    :param files: list of `FileSpec` objects (list)
    :raise: PilotException in case of controlled error
    :return: updated files (list).
    """
    for fspec in files:

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        path = os.path.join(dst, fspec.lfn)
        logger.info(f'downloading surl={fspec.surl} to local file {path}')
        status, diagnostics = download_file(path, fspec.surl, object_name=fspec.lfn)

        if not status:  ## an error occurred
            error = resolve_common_transfer_errors(diagnostics, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def download_file(path: str, surl: str, object_name: str = None) -> (bool, str):
    """
    Download a file from a GS bucket.

    :param path: Path to local file after download (str)
    :param surl: remote path (str)
    :param object_name: GCS object name. If not specified then file_name from path is used (str)
    :return: True if file was uploaded - otherwise False (bool), diagnostics (str).
    """
    # if object_name was not specified, use file name from path
    if object_name is None:
        object_name = os.path.basename(path)

    try:
        target = pathlib.Path(object_name)
        with target.open(mode="wb") as downloaded_file:
            storage_client.download_blob_to_file(surl, downloaded_file)
    except Exception as error:
        diagnostics = f'exception caught in gs client: {error}'
        logger.critical(diagnostics)
        return False, diagnostics

    return True, ""


def copy_out(files: list, **kwargs: dict):
    """
    Upload given files to GS storage.

    :param files: list of `FileSpec` objects (list)
    :raise: PilotException in case of controlled error
    :return: updated files (list).
    """
    workdir = kwargs.pop('workdir')

    # if len(files) > 0:
    #     fspec = files[0]
    #     # bucket = re.sub(r'gs://(.*?)/.*', r'\1', fspec.turl)
    #     reobj = re.match(r'gs://([^/]*)/(.*)', fspec.turl)
    #     (bucket, remote_path) = reobj.groups()

    for fspec in files:
        logger.info(f'processing fspec.turl={fspec.turl}')

        fspec.status = None
        reobj = re.match(r'gs://([^/]*)/(.*)', fspec.turl)
        (bucket, remote_path) = reobj.groups()

        logfiles = []
        lfn = fspec.lfn.strip()
        if lfn == '/' or lfn.endswith("log.tgz"):
            # ["pilotlog.txt", "payload.stdout", "payload.stderr"]:
            logfiles += glob(workdir + '/payload*.*')
            logfiles += glob(workdir + '/memory_monitor*.*')
            # if lfn.find('/') < 0:
            #     lfn_path = os.path.join(workdir, lfn)
            #    if os.path.exists(lfn_path) and lfn_path not in logfiles:
            #        logfiles += [lfn_path]
            logfiles += glob(workdir + '/pilotlog*.*')
        else:
            logfiles = [os.path.join(workdir, lfn)]

        for path in logfiles:
            logfile = os.path.basename(path)
            if os.path.exists(path):
                if logfile == config.Pilot.pilotlog or logfile == config.Payload.payloadstdout or logfile == config.Payload.payloadstderr:
                    content_type = "text/plain"
                    logger.debug(f'change the file {logfile} content-type to text/plain')
                else:
                    content_type = None
                    try:
                        result = subprocess.check_output(["/bin/file", "-i", "-b", "-L", path])
                        if not isinstance(result, str):
                            result = result.decode('utf-8')
                        if result.find(';') > 0:
                            content_type = result.split(';')[0]
                            logger.debug(f'change the file {logfile} content-type to {content_type}')
                    except Exception:
                        pass

                object_name = os.path.join(remote_path, logfile)
                logger.info(f'uploading {path} to bucket {bucket} using object name {object_name}')
                status, diagnostics = upload_file(path, bucket, object_name=object_name, content_type=content_type)

                if not status:  ## an error occurred
                    # create new error code(s) in ErrorCodes.py and set it/them in resolve_common_transfer_errors()
                    error = resolve_common_transfer_errors(diagnostics, is_stagein=False)
                    fspec.status = 'failed'
                    fspec.status_code = error.get('rcode')
                    raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))
            else:
                diagnostics = f'local output file does not exist: {path}'
                logger.warning(diagnostics)
                fspec.status = 'failed'
                fspec.status_code = errors.STAGEOUTFAILED
                # raise PilotException(diagnostics, code=fspec.status_code, state=fspec.status)

        if fspec.status is None:
            fspec.status = 'transferred'
            fspec.status_code = 0

    return files


def upload_file(file_name: str, bucket: str, object_name: str = None, content_type: str = None) -> (bool, str):
    """
    Upload a file to a GCS bucket.

    :param file_name: file to upload (str)
    :param bucket: bucket to upload to (str)
    :param object_name: GCS object name. If not specified then file_name is used (str)
    :param content_type: content type (str)
    :return: True if file was uploaded (else False), diagnostics (string).
    """
    # if GCS object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # upload the file
    try:
        gs_bucket = storage_client.get_bucket(bucket)
        # remove any leading slash(es) in object_name
        object_name = object_name.lstrip('/')
        logger.info(f'uploading a file to bucket {bucket} in full path {object_name} in content_type {content_type}')
        blob = gs_bucket.blob(object_name)
        blob.upload_from_filename(filename=file_name, content_type=content_type)
        if file_name.endswith(config.Pilot.pilotlog):
            url_pilotlog = blob.public_url
            os.environ['GTAG'] = url_pilotlog
            logger.debug(f"set env var GTAG with the pilotLot URL={url_pilotlog}")
    except Exception as error:
        diagnostics = f'exception caught in gs client: {error}'
        logger.critical(diagnostics)
        return False, diagnostics

    return True, ""
