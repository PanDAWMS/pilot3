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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23

"""Unit tests for the esprocess package."""

import logging
import json
import os
import sys
import socket
import time
import traceback
import unittest

from pilot.api.es_data import StageInESClient
from pilot.control.job import create_job
from pilot.eventservice.communicationmanager.communicationmanager import CommunicationManager
from pilot.eventservice.workexecutor.workexecutor import WorkExecutor
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
class TestESWorkExecutorGrid(unittest.TestCase):
    """Unit tests for event service Grid work executor."""

    @classmethod
    def setUpClass(cls):
        """
        Set up test fixtures.

        :raises Exception: in case of failure.
        """
        # set a timeout of 10 seconds to prevent potential hanging due to problems with DNS resolution, or if the DNS
        # server is slow to respond
        socket.setdefaulttimeout(10)
        try:
            fqdn = socket.getfqdn()
        except socket.herror:
            fqdn = 'localhost'
        try:
            args = {'workflow': 'eventservice_hpc',
                    'queue': 'BNL_CLOUD_MCORE',
                    'site': 'BNL_CLOUD_MCORE',
                    'port': 25443,
                    'url': 'https://aipanda007.cern.ch',
                    'job_label': 'ptest',
                    'pilot_user': 'ATLAS',
                    'node': fqdn,
                    'mem': 16000,
                    'disk_space': 160000,
                    'working_group': '',
                    'cpu': 2601.0,
                    'info': None}

            communicator_manager = CommunicationManager()
            cls._communicator_manager = communicator_manager
            communicator_manager.start()

            jobs = communicator_manager.get_jobs(njobs=1, args=args)
            job = create_job(jobs[0], queuename='BNL_CLOUD_MCORE')
            job.workdir = '/tmp/test_esworkexecutor'
            job.corecount = 1
            if not os.path.exists(job.workdir):
                os.makedirs(job.workdir)

            job_data = {}
            job_data['jobId'] = job['PandaID']
            job_data['siteName'] = 'BNL_CLOUD_MCORE'
            job_data['state'] = 'starting'
            job_data['attemptNr'] = job['attemptNr'] + 1
            job_data['node'] = 'pilot3_test'
            job_data['schedulerID'] = 'pilot3_test'
            job_data['coreCount'] = 1
            _ = communicator_manager.update_jobs(jobs=[job_data])
            job_data['state'] = 'running'
            _ = communicator_manager.update_jobs(jobs=[job_data])
            communicator_manager.stop()

            # download input files
            client = StageInESClient(job.infosys, logger=logger)
            kwargs = {'workdir': job.workdir, 'cwd': job.workdir, 'usecontainer': False, 'job': job}
            client.prepare_sources(job.indata)
            client.transfer(job.indata, activity='pr', **kwargs)

            # get the payload command from the user specific code
            pilot_user = os.environ.get('PILOT_USER', 'atlas').lower()
            user = __import__(f'pilot.user.{pilot_user}.common', globals(), locals(), [pilot_user], 0)
            cmd = user.get_payload_command(job)
            logger.info(f"payload execution command: {cmd}")

            payload = {'executable': cmd,
                       'workdir': job.workdir,
                       'output_file': f"pilot_test_{job['PandaID']}_stdout.txt",
                       'error_file': f"pilot_test_{job['PandaID']}_stderr.txt",
                       'job': job}
            cls._payload = payload
        except Exception as exc:
            if cls._communicator_manager:
                cls._communicator_manager.stop()
            raise exc

    @classmethod
    def tearDownClass(cls):
        """Remove test fixtures."""
        cls._communicator_manager.stop()

    def setup(self):
        """Set up test fixtures."""
        self.executor = None

    def tearDown(self):
        """Remove test fixtures."""
        if self._communicator_manager:
            self._communicator_manager.stop()
        if self.executor:
            self.executor.stop()

    def test_workexecutor_generic(self):
        """
        Make sure there are no exceptions when running work executor.

        :raises Exception: in case of failure.
        """
        try:
            executor = WorkExecutor()
            self.executor = executor
            executor.set_payload(self._payload)
            executor.start()

            t_start = time.time()
            t1 = time.time()
            while executor.is_alive():
                if time.time() > t1 + 300:
                    logging.info("work executor is running")
                    t1 = time.time()
                time.sleep(1)
                if time.time() > t_start + 20 * 60:
                    executor.stop()
                    break
            while executor.is_alive():
                time.sleep(0.1)
            exit_code = executor.get_exit_code()
            self.assertEqual(exit_code, 0)
        except Exception as exc:
            logger.debug(f"Exception: {exc}, {traceback.format_exc()}")
            if self.executor:
                self.executor.stop()
                while self.executor.is_alive():
                    time.sleep(0.1)
            raise exc

    @unittest.skipIf(True, "skip it")
    def test_workexecutor_update_events(self):
        """
        Make sure there are no exceptions when running work executor.

        :raises Exception: in case of failure.
        """
        try:
            executor = WorkExecutor()
            self.executor = executor
            executor.set_payload(self._payload)
            executor.start()
            ret = executor.get_event_ranges()
            logger.debug(ret)

            update_events = []
            for event in ret:
                event_range = {"eventRangeID": event['eventRangeID'], "eventStatus": 'finished'}
                update_events.append(event_range)
            event_range_status = [{"zipFile": {"numEvents": len(update_events),
                                               "objstoreID": 1318,
                                               "adler32": '000000',
                                               "lfn": 'test_file',
                                               "fsize": 100,
                                               "pathConvention": 1000},
                                   "eventRanges": update_events}]
            event_range_message = {'version': 1, 'eventRanges': json.dumps(event_range_status)}
            ret = executor.update_events(event_range_message)
            logger.debug(ret)

            executor.stop()
        except Exception as exc:
            if self.executor:
                self.executor.stop()
            raise exc
