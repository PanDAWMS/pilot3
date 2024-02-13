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
# - Wen Guan, wen.guan@cern.ch, 2017-18
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

"""Unit tests for the esstager package."""

import logging
import os
import shutil
import sys
import traceback
import unittest
import uuid

from pilot.api.es_data import (
    StageOutESClient,
    StageInESClient
)
from pilot.common import exception
from pilot.info import (
    infosys,
    InfoService
)
from pilot.info.filespec import FileSpec
from pilot.util.https import https_setup

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)

https_setup(None, None)


def check_env() -> bool:
    """
    Check whether cvmfs is available.

    To be used to decide whether to skip some test functions.

    :returns: True if cvmfs is available, otherwise False (bool).
    """
    return os.path.exists('/cvmfs/atlas.cern.ch/repo/')


@unittest.skipIf(not check_env(), "No CVMFS")
class TestStager(unittest.TestCase):
    """Unit tests for event service Grid work executor."""

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_stageout_es_events(self):
        """
        Make sure there are no exceptions to stage out file.

        :raises: StageOutFailure in case of failure.
        """
        error = None
        try:
            infoservice = InfoService()
            infoservice.init('BNL_CLOUD_MCORE', infosys.confinfo, infosys.extinfo)

            output_file = os.path.join('/tmp', str(uuid.uuid4()))
            shutil.copy('/bin/hostname', output_file)
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         #'ddmendpoint': None,
                         #'type': 'es_events',
                         #'surl': output_file
                         #'turl': None,
                         #'filesize': None,
                         #'checksum': None
                         }
            file_spec = FileSpec(filetype='output', **file_data)
            xdata = [file_spec]
            workdir = os.path.dirname(output_file)
            client = StageOutESClient(infoservice)
            kwargs = {'workdir': workdir, 'cwd': workdir, 'usecontainer': False}
            client.prepare_destinations(xdata, activity='es_events')
            client.transfer(xdata, activity='es_events', **kwargs)
        except exception.PilotException as error:
            logger.error(f"Pilot Exception: {error.get_detail()}, {traceback.format_exc()}")
        except Exception as exc:
            logger.error(traceback.format_exc())
            error = exception.StageOutFailure(f"stageOut failed with error={exc}")
        else:
            logger.info('Summary of transferred files:')
            for e in xdata:
                logger.info(f" -- lfn={e.lfn}, status_code={e.status_code}, status={e.status}")

        if error:
            logger.error(f'Failed to stage-out eventservice file({output_file}): error={error.get_detail()}')
            raise error

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_stageout_es_events_pw(self):
        """
        Make sure there are no exceptions to stage out file.

        :raises: StageOutFailure in case of failure.
        """
        error = None
        try:
            infoservice = InfoService()
            infoservice.init('BNL_CLOUD_MCORE', infosys.confinfo, infosys.extinfo)

            output_file = os.path.join('/tmp', str(uuid.uuid4()))
            shutil.copy('/bin/hostname', output_file)
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         #'ddmendpoint': None,
                         #'type': 'es_events',
                         #'surl': output_file
                         #'turl': None,
                         #'filesize': None,
                         #'checksum': None
                         }
            file_spec = FileSpec(filetype='output', **file_data)
            xdata = [file_spec]
            workdir = os.path.dirname(output_file)
            client = StageOutESClient(infoservice)
            kwargs = {'workdir': workdir, 'cwd': workdir, 'usecontainer': False}
            client.prepare_destinations(xdata, activity=['es_events', 'pw'])  # allow to write to `es_events` and `pw` astorages
            client.transfer(xdata, activity=['es_events', 'pw'], **kwargs)
        except exception.PilotException as error:
            logger.error(f"Pilot Exeception: {error.get_detail()}, {traceback.format_exc()}")
        except Exception as exc:
            logger.error(traceback.format_exc())
            error = exception.StageOutFailure(f"stageOut failed with error={exc}")
        else:
            logger.info('Summary of transferred files:')
            for fil in xdata:
                logger.info(f" -- lfn={fil.lfn}, status_code={fil.status_code}, status={fil.status}")

        if error:
            logger.error(f'Failed to stage-out eventservice file({output_file}): error={error.get_detail()}')
            raise error

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_stageout_es_events_non_exist_pw(self):
        """
        Make sure there are no exceptions to stage out file.

        :raises: StageOutFailure in case of failure.
        """
        error = None
        try:
            infoservice = InfoService()
            infoservice.init('BNL_CLOUD_MCORE', infosys.confinfo, infosys.extinfo)

            output_file = os.path.join('/tmp', str(uuid.uuid4()))
            shutil.copy('/bin/hostname', output_file)
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         #'ddmendpoint': None,
                         #'type': 'es_events',
                         #'surl': output_file
                         #'turl': None,
                         #'filesize': None,
                         #'checksum': None
                         }
            file_spec = FileSpec(filetype='output', **file_data)
            xdata = [file_spec]
            workdir = os.path.dirname(output_file)
            client = StageOutESClient(infoservice)
            kwargs = {'workdir': workdir, 'cwd': workdir, 'usecontainer': False}
            client.prepare_destinations(xdata, activity=['es_events_non_exist', 'pw'])  # allow to write to `es_events_non_exist` and `pw` astorages
            client.transfer(xdata, activity=['es_events_non_exist', 'pw'], **kwargs)
        except exception.PilotException as error:
            logger.error(f"Pilot Exeception: {error.get_detail()}, {traceback.format_exc()}")
        except Exception as exc:
            logger.error(traceback.format_exc())
            error = exception.StageOutFailure(f"stageOut failed with error={exc}")
        else:
            logger.info('Summary of transferred files:')
            for fil in xdata:
                logger.info(f" -- lfn={fil.lfn}, status_code={fil.status_code}, status={fil.status}")

        if error:
            logger.error(f'Failed to stage-out eventservice file({output_file}): error={error.get_detail()}')
            raise error

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_stageout_stagein(self):
        """
        Make sure there are no exceptions to stage out file.

        :raises: StageOutFailure in case of failure.
        """
        error = None
        try:
            infoservice = InfoService()
            infoservice.init('BNL_CLOUD_MCORE', infosys.confinfo, infosys.extinfo)

            output_file = os.path.join('/tmp', str(uuid.uuid4()))
            shutil.copy('/bin/hostname', output_file)
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         #'ddmendpoint': None,
                         #'type': 'es_events',
                         #'surl': output_file
                         #'turl': None,
                         #'filesize': None,
                         #'checksum': None
                         }
            file_spec = FileSpec(filetype='output', **file_data)
            xdata = [file_spec]
            workdir = os.path.dirname(output_file)
            client = StageOutESClient(infoservice)
            kwargs = {'workdir': workdir, 'cwd': workdir, 'usecontainer': False}
            client.prepare_destinations(xdata, activity=['es_events', 'pw'])  # allow to write to `es_events` and `pw` astorages
            client.transfer(xdata, activity=['es_events', 'pw'], **kwargs)
        except exception.PilotException as error:
            logger.error(f"Pilot Exeception: {error.get_detail()}, {traceback.format_exc()}")
        except Exception as exc:
            logger.error(traceback.format_exc())
            error = exception.StageOutFailure(f"stageOut failed with error={exc}")
        else:
            logger.info('Summary of transferred files:')
            for fil in xdata:
                logger.info(f" -- lfn={fil.lfn}, status_code={fil.status_code}, status={fil.status}")

        if error:
            logger.error(f'Failed to stage-out eventservice file({output_file}): error={error.get_detail()}')
            raise error

        storage_id = infosys.get_storage_id(file_spec.ddmendpoint)
        logger.info(f'File {file_spec.lfn} staged out to {file_spec.ddmendpoint}(id: {storage_id})')

        new_file_data = {'scope': 'test',
                         'lfn': file_spec.lfn,
                         'storage_token': f'{storage_id}/1000'}
        try:
            new_file_spec = FileSpec(filetype='input', **new_file_data)

            xdata = [new_file_spec]
            workdir = os.path.dirname(output_file)
            client = StageInESClient(infoservice)
            kwargs = {'workdir': workdir, 'cwd': workdir, 'usecontainer': False}
            client.prepare_sources(xdata)
            client.transfer(xdata, activity=['es_events_read'], **kwargs)
        except exception.PilotException as error:
            logger.error(f"Pilot Exeception: {error.get_detail()}, {traceback.format_exc()}")
        except Exception as exc:
            logger.error(traceback.format_exc())
            error = exception.StageInFailure(f"stagein failed with error={exc}")
        else:
            logger.info('Summary of transferred files:')
            for fil in xdata:
                logger.info(f" -- lfn={fil.lfn}, status_code={fil.status_code}, status={fil.status}")

        if error:
            logger.error(f'Failed to stage-in eventservice file({output_file}): error={error.get_detail()}')
            raise error

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_stageout_noexist_activity_stagein(self):
        """
        Make sure there are no exceptions to stage out file.

        :raises: StageOutFailure in case of failure.
        """
        error = None
        try:
            infoservice = InfoService()
            infoservice.init('BNL_CLOUD_MCORE', infosys.confinfo, infosys.extinfo)

            output_file = os.path.join('/tmp', str(uuid.uuid4()))
            shutil.copy('/bin/hostname', output_file)
            file_data = {'scope': 'transient',
                         'lfn': os.path.basename(output_file),
                         #'ddmendpoint': None,
                         #'type': 'es_events',
                         #'surl': output_file
                         #'turl': None,
                         #'filesize': None,
                         #'checksum': None
                         }
            file_spec = FileSpec(filetype='output', **file_data)
            xdata = [file_spec]
            workdir = os.path.dirname(output_file)
            client = StageOutESClient(infoservice)
            kwargs = {'workdir': workdir, 'cwd': workdir, 'usecontainer': False}
            client.prepare_destinations(xdata, activity=['es_events_no_exist', 'pw'])  # allow to write to `es_events_no_exist` and `pw` astorages
            client.transfer(xdata, activity=['es_events_no_exist', 'pw'], **kwargs)
        except exception.PilotException as error:
            logger.error(f"Pilot Exeception: {error.get_detail()}, {traceback.format_exc()}")
        except Exception as exc:
            logger.error(traceback.format_exc())
            error = exception.StageOutFailure(f"stageOut failed with error={exc}")
        else:
            logger.info('Summary of transferred files:')
            for fil in xdata:
                logger.info(f" -- lfn={fil.lfn}, status_code={fil.status_code}, status={fil.status}")

        if error:
            logger.error(f'Failed to stage-out eventservice file({output_file}): error={error.get_detail()}')
            raise error

        storage_id = infosys.get_storage_id(file_spec.ddmendpoint)
        logger.info(f'File {file_spec.lfn} staged out to {file_spec.ddmendpoint}(id: {storage_id})')

        new_file_data = {'scope': 'test',
                         'lfn': file_spec.lfn,
                         'storage_token': f'{storage_id}/1000'}
        try:
            new_file_spec = FileSpec(filetype='input', **new_file_data)

            xdata = [new_file_spec]
            workdir = os.path.dirname(output_file)
            client = StageInESClient(infoservice)
            kwargs = {'workdir': workdir, 'cwd': workdir, 'usecontainer': False}
            client.prepare_sources(xdata)
            client.transfer(xdata, activity=['es_events_read'], **kwargs)
        except exception.PilotException as error:
            logger.error(f"Pilot Exeception: {error.get_detail()}, {traceback.format_exc()}")
        except Exception as exc:
            logger.error(traceback.format_exc())
            error = exception.StageInFailure(f"stagein failed with error={exc}")
        else:
            logger.info('Summary of transferred files:')
            for fil in xdata:
                logger.info(f" -- lfn={fil.lfn}, status_code={fil.status_code}, status={fil.status}")

        if error:
            logger.error(f'Failed to stage-in eventservice file({output_file}): error={error.get_detail()}')
            raise error
