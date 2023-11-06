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
# - Shuwei

import os
import logging
from pilot.info import infosys
import subprocess
import re
from glob import glob

try:
    from google.cloud import storage
except Exception:
    storage_client = None
else:
    storage_client = storage.Client()

try:
    import pathlib  # Python 3
except Exception:
    pathlib = None

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


def is_valid_for_copy_in(files):
    return True  ## FIX ME LATER


def is_valid_for_copy_out(files):
    return True  ## FIX ME LATER


def resolve_surl(fspec, protocol, ddmconf, **kwargs):
    """
        Get final destination SURL for file to be transferred to Objectstore
        Can be customized at the level of specific copytool

        :param protocol: suggested protocol
        :param ddmconf: full ddm storage data
        :param fspec: file spec data
        :return: dictionary {'surl': surl}
    """

    try:
        pandaqueue = infosys.pandaqueue
    except Exception:
        pandaqueue = ""
    if pandaqueue is None:
        pandaqueue = ""

    ddm = ddmconf.get(fspec.ddmendpoint)
    if not ddm:
        raise PilotException('failed to resolve ddmendpoint by name=%s' % fspec.ddmendpoint)

    dataset = fspec.dataset
    if dataset:
        dataset = dataset.replace("#{pandaid}", os.environ['PANDAID'])
    else:
        dataset = ""

    remote_path = os.path.join(protocol.get('path', ''), pandaqueue, dataset)
    surl = protocol.get('endpoint', '') + remote_path
    logger.info('For GCS bucket, set surl=%s', surl)

    # example:
    #   protocol = {u'path': u'/atlas-eventservice', u'endpoint': u's3://s3.cern.ch:443/', u'flavour': u'AWS-S3-SSL', u'id': 175}
    #   surl = 's3://s3.cern.ch:443//atlas-eventservice/EventService_premerge_24706191-5013009653-24039149400-322-5.tar'
    return {'surl': surl}


def copy_in(files, **kwargs):
    """
    Download given files from a GCS bucket.

    :param files: list of `FileSpec` objects
    :raise: PilotException in case of controlled error
    """

    for fspec in files:

        dst = fspec.workdir or kwargs.get('workdir') or '.'
        path = os.path.join(dst, fspec.lfn)
        logger.info('downloading surl=%s to local file %s', fspec.surl, path)
        status, diagnostics = download_file(path, fspec.surl, object_name=fspec.lfn)

        if not status:  ## an error occurred
            error = resolve_common_transfer_errors(diagnostics, is_stagein=True)
            fspec.status = 'failed'
            fspec.status_code = error.get('rcode')
            raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))

        fspec.status_code = 0
        fspec.status = 'transferred'

    return files


def download_file(path, surl, object_name=None):
    """
    Download a file from a GS bucket.

    :param path: Path to local file after download (string).
    :param surl: remote path (string).
    :param object_name: GCS object name. If not specified then file_name from path is used.
    :return: True if file was uploaded (else False), diagnostics (string).
    """

    # if object_name was not specified, use file name from path
    if object_name is None:
        object_name = os.path.basename(path)

    try:
        target = pathlib.Path(object_name)
        with target.open(mode="wb") as downloaded_file:
            storage_client.download_blob_to_file(surl, downloaded_file)
    except Exception as error:
        diagnostics = 'exception caught in gs client: %s' % error
        logger.critical(diagnostics)
        return False, diagnostics

    return True, ""


def copy_out(files, **kwargs):
    """
    Upload given files to GS storage.

    :param files: list of `FileSpec` objects
    :raise: PilotException in case of controlled error
    """

    workdir = kwargs.pop('workdir')

    # if len(files) > 0:
    #     fspec = files[0]
    #     # bucket = re.sub(r'gs://(.*?)/.*', r'\1', fspec.turl)
    #     reobj = re.match(r'gs://([^/]*)/(.*)', fspec.turl)
    #     (bucket, remote_path) = reobj.groups()

    for fspec in files:
        logger.info('Going to process fspec.turl=%s', fspec.turl)

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
                    logger.info('Change the file=%s content-type to text/plain', logfile)
                else:
                    content_type = None
                    try:
                        result = subprocess.check_output(["/bin/file", "-i", "-b", "-L", path])
                        if not isinstance(result, str):
                            result = result.decode('utf-8')
                        if result.find(';') > 0:
                            content_type = result.split(';')[0]
                            logger.info('Change the file=%s content-type to %s', logfile, content_type)
                    except Exception:
                        pass

                object_name = os.path.join(remote_path, logfile)
                logger.info('uploading %s to bucket=%s using object name=%s', path, bucket, object_name)
                status, diagnostics = upload_file(path, bucket, object_name=object_name, content_type=content_type)

                if not status:  ## an error occurred
                    # create new error code(s) in ErrorCodes.py and set it/them in resolve_common_transfer_errors()
                    error = resolve_common_transfer_errors(diagnostics, is_stagein=False)
                    fspec.status = 'failed'
                    fspec.status_code = error.get('rcode')
                    raise PilotException(error.get('error'), code=error.get('rcode'), state=error.get('state'))
            else:
                diagnostics = 'local output file does not exist: %s' % path
                logger.warning(diagnostics)
                fspec.status = 'failed'
                fspec.status_code = errors.STAGEOUTFAILED
                # raise PilotException(diagnostics, code=fspec.status_code, state=fspec.status)

        if fspec.status is None:
            fspec.status = 'transferred'
            fspec.status_code = 0

    return files


def upload_file(file_name, bucket, object_name=None, content_type=None):
    """
    Upload a file to a GCS bucket.

    :param file_name: File to upload.
    :param bucket: Bucket to upload to (string).
    :param object_name: GCS object name. If not specified then file_name is used.
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
        logger.info('uploading a file to bucket=%s in full path=%s in content_type=%s', bucket, object_name, content_type)
        blob = gs_bucket.blob(object_name)
        blob.upload_from_filename(filename=file_name, content_type=content_type)
        if file_name.endswith(config.Pilot.pilotlog):
            url_pilotlog = blob.public_url
            os.environ['GTAG'] = url_pilotlog
            logger.debug("Set envvar GTAG with the pilotLot URL=%s", url_pilotlog)
    except Exception as error:
        diagnostics = 'exception caught in gs client: %s' % error
        logger.critical(diagnostics)
        return False, diagnostics

    return True, ""
