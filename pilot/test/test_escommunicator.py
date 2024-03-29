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
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch , 2023

"""Unit tests for the ES communication module."""

import json
import logging
import os
import socket
import sys
import time
import unittest

from pilot.eventservice.communicationmanager.communicationmanager import CommunicationRequest, CommunicationResponse, CommunicationManager
from pilot.util.https import https_setup
from pilot.util.timing import time_stamp

logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
logger = logging.getLogger(__name__)

https_setup(None, None)


def check_env() -> bool:
    """
    Check whether cvmfs is available.

    To be used to decide whether to skip some test functions.

    :return: True if cvmfs is available, otherwise False (bool).
    """
    return os.path.exists('/cvmfs/')


class TestESCommunicationRequestResponse(unittest.TestCase):
    """Unit tests for event service communicator Request and Response."""

    def test_communicator_request(self):
        """Make sure that es message thread works as expected."""
        req_attrs = {'request_type': CommunicationRequest.RequestType.RequestJobs,
                     'num_jobs': 1, 'post_hook': None, 'response': None}
        req_job = CommunicationRequest(req_attrs)
        self.assertEqual(req_job.request_type, CommunicationRequest.RequestType.RequestJobs)

        req_attrs = {'request_type': CommunicationRequest.RequestType.RequestEvents,
                     'num_event_ranges': 1, 'post_hook': None, 'response': None}
        req_events = CommunicationRequest(req_attrs)
        self.assertEqual(req_events.request_type, CommunicationRequest.RequestType.RequestEvents)

        req_attrs = {'request_type': CommunicationRequest.RequestType.UpdateEvents,
                     'output_files': None, 'post_hook': None, 'response': None}
        req_output = CommunicationRequest(req_attrs)
        self.assertEqual(req_output.request_type, CommunicationRequest.RequestType.UpdateEvents)

        resp_attrs = {'status': 0, 'content': None, 'exception': None}
        resp = CommunicationResponse(resp_attrs)
        self.assertEqual(resp.status, 0)


class TestESCommunicationManagerPanda(unittest.TestCase):
    """Unit tests for event service communicator manager."""

    @unittest.skipIf(not check_env(), "No CVMFS")
    def test_communicator_manager(self):
        """Make sure that es communicator manager thread works as expected."""
        communicator_manager = None
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
            communicator_manager.start()
            self.assertTrue(communicator_manager.is_alive())

            jobs = communicator_manager.get_jobs(njobs=2, args=args)
            self.assertEqual(len(jobs), 2)

            jobs = communicator_manager.get_jobs(njobs=1, args=args)
            self.assertEqual(len(jobs), 1)

            job_list = []
            for job in jobs:
                job_data = {'node': socket.getfqdn(),
                            'pilotErrorCode': 0,
                            'startTime': time.time(),
                            'jobMetrics': 'coreCount=8',
                            'schedulerID': 'unknown',
                            'timestamp': time_stamp(),
                            'exeErrorCode': 0,
                            'pilotID': 'unknown|PR|2.0.0 (80)',
                            'transExitCode': 0,
                            'pilotErrorDiag': '',
                            'exeErrorDiag': ''}
                job_data['jobId'] = job['PandaID']
                job_data['siteName'] = 'BNL_CLOUD_MCORE'
                job_data['state'] = 'running'
                job_data['attemptNr'] = job['attemptNr'] + 1
                job_list.append(job_data)
            status = communicator_manager.update_jobs(jobs=job_list)
            self.assertEqual(status[0], True)

            events = communicator_manager.get_event_ranges(num_event_ranges=1, job=jobs[0])
            self.assertEqual(len(events), 1)

            for event in events:
                event_range_status = {"errorCode": 1220, "eventRangeID": event['eventRangeID'], "eventStatus": 'failed'}
                event_range_message = {'version': 0, 'eventRanges': json.dumps(event_range_status)}
                res = communicator_manager.update_events(update_events=event_range_message)
                self.assertEqual(res['StatusCode'], 0)

            events = communicator_manager.get_event_ranges(num_event_ranges=2, job=jobs[0])
            self.assertEqual(len(events), 2)

            update_events = []
            for event in events:
                event_range = {"eventRangeID": event['eventRangeID'], "eventStatus": 'finished'}
                update_events.append(event_range)
            event_range_status_list = [{"zipFile": {"numEvents": len(update_events),
                                                    "objstoreID": 1318,
                                                    "adler32": '000000',
                                                    "lfn": 'test_file',
                                                    "fsize": 100,
                                                    "pathConvention": 1000},
                                        "eventRanges": update_events}]

            event_range_message = {'version': 1, 'eventRanges': json.dumps(event_range_status_list)}
            res = communicator_manager.update_events(update_events=event_range_message)
            self.assertEqual(res['StatusCode'], 0)

            communicator_manager.stop()
            time.sleep(2)
            self.assertFalse(communicator_manager.is_alive())
        except Exception as exc:
            if communicator_manager:
                communicator_manager.stop()
            raise exc
