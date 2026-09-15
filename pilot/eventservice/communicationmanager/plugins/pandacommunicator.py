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
# - Wen Guan, wen.guan@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2020-2024

"""PanDA communicator."""

import logging
import threading
import traceback
from os import environ
from typing import Any

from pilot.common import exception
from pilot.util import https
from pilot.util.config import config
from ..communicationmanager import CommunicationResponse
from .basecommunicator import BaseCommunicator

logger = logging.getLogger(__name__)


class PandaCommunicator(BaseCommunicator):
    """PanDA communicator class."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize variables.

        Args:
            args: args object.
            kwargs: kwargs dictionary.
        """
        super().__init__(args, kwargs)
        self.get_jobs_lock = threading.Lock()
        self.get_events_lock = threading.Lock()
        self.update_events_lock = threading.Lock()
        self.update_jobs_lock = threading.Lock()

    def pre_check_get_jobs(self, req: Any = None) -> Any:
        """Check whether it's ok to send a request to get jobs.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        return CommunicationResponse({'status': 0})

    def request_get_jobs(self, req: Any) -> Any:
        """Send a request to get jobs.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        return CommunicationResponse({'status': 0})

    def check_get_jobs_status(self, req: Any = None) -> Any:
        """Check whether jobs are prepared.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        return CommunicationResponse({'status': 0})

    def get_data(self, req: Any) -> dict:
        """Get data from request.

        Args:
            req: request.

        Returns:
            dict: data dictionary.
        """
        data = {'getProxyKey': 'False'}
        kmap = {'node': 'node', 'mem': 'mem', 'getProxyKey': 'getProxyKey', 'computingElement': 'queue',
                'diskSpace': 'disk_space',
                'siteName': 'site', 'prodSourceLabel': 'job_label', 'workingGroup': 'working_group', 'cpu': 'cpu'}
        for key, value in list(kmap.items()):
            if hasattr(req, value):
                data[key] = getattr(req, value)

        return data

    def get_jobs(self, req: Any) -> dict:
        """Get the job definition from panda server.

        Args:
            req: request.

        Returns:
            dict: job definition dictionary.
        """
        self.get_jobs_lock.acquire()

        try:
            jobs = []
            resp_attrs = None

            # get the data dictionary
            data = self.get_data(req)

            for _ in range(req.num_jobs):
                logger.info(f"Getting jobs: {data}")
                url = environ.get('PANDA_SERVER_URL', config.Pilot.pandaserver)
                res = https.request(f'{url}/server/panda/getJob', data=data)
                logger.info(f"Got jobs returns: {res}")

                if res is None:
                    resp_attrs = {'status': None, 'content': None, 'exception': exception.CommunicationFailure("Get job failed to get response from Panda.")}
                    break
                if res['StatusCode'] == 20 and 'no jobs in PanDA' in res['errorDialog']:
                    resp_attrs = {'status': res['StatusCode'],
                                  'content': None,
                                  'exception': exception.CommunicationFailure("No jobs in panda")}
                elif res['StatusCode'] != 0:
                    resp_attrs = {'status': res['StatusCode'],
                                  'content': None,
                                  'exception': exception.CommunicationFailure(f"Get job from Panda returns a non-zero value: {res['StatusCode']}")}
                    break
                else:
                    jobs.append(res)

            if jobs:
                resp_attrs = {'status': 0, 'content': jobs, 'exception': None}
            elif not resp_attrs:
                resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException("Failed to get jobs")}

            resp = CommunicationResponse(resp_attrs)
        except Exception as e:
            logger.error(f"Failed to get jobs: {e}, {traceback.format_exc()}")
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException(f"Failed to get jobs: {traceback.format_exc()}")}
            resp = CommunicationResponse(resp_attrs)

        self.get_jobs_lock.release()

        return resp

    def pre_check_get_events(self, req: Any = None) -> Any:
        """Precheck whether it's ok to send a request to get events.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        return CommunicationResponse({'status': 0})

    def request_get_events(self, req: Any) -> Any:
        """Send a request to get events.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        return CommunicationResponse({'status': 0})

    def check_get_events_status(self, req: Any = None) -> Any:
        """Check whether events prepared.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        return CommunicationResponse({'status': 0})

    def get_events(self, req: Any) -> Any:
        """Get events.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with event ranges or error status.
        """
        self.get_events_lock.acquire()

        try:
            if not req.num_ranges:
                # ToBeFix num_ranges with corecount
                req.num_ranges = 1

            # api/v1: the server's event API (acquire_event_ranges) answers
            # {'success': bool, 'message': str, 'data': [event ranges]}; the
            # pre-v1 getEventRanges method no longer exists on the server
            data = {'job_id': req.jobid,
                    'jobset_id': req.jobsetid,
                    'task_id': req.taskid,
                    'n_ranges': req.num_ranges}

            logger.info(f"Downloading new event ranges: {data}")
            url = environ.get('PANDA_SERVER_URL', config.Pilot.pandaserver)
            res = https.request2(f'{url}/api/v1/event/acquire_event_ranges', json_body=data, panda=True)
            logger.info(f"Downloaded event ranges: {res}")

            if not isinstance(res, dict):
                resp_attrs = {'status': -1,
                              'content': None,
                              'exception': exception.CommunicationFailure(f"Get events from panda returned no response: {res}")}
            elif res.get('success'):
                resp_attrs = {'status': 0, 'content': res.get('data'), 'exception': None}
            else:
                resp_attrs = {'status': -1,
                              'content': None,
                              'exception': exception.CommunicationFailure(f"Get events from panda failed: {res.get('message')}")}

            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
            logger.error(f"Failed to download event ranges: {e}, {traceback.format_exc()}")
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException(f"Failed to get events: {traceback.format_exc()}")}
            resp = CommunicationResponse(resp_attrs)

        self.get_events_lock.release()

        return resp

    def pre_check_update_events(self, req: Any = None) -> Any:
        """Precheck whether it's ok to update events.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        self.update_events_lock.acquire()
        try:
            pass
        except Exception as e:  # Python 2/3
            logger.error(f"Failed to pre_check_update_events: {e}, {traceback.format_exc()}")
        self.update_events_lock.release()

        return CommunicationResponse({'status': 0})

    def update_events(self, req: Any) -> Any:
        """Update events.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        self.update_events_lock.acquire()

        try:
            logger.info(f"Updating events: {req}")
            # api/v1: update_event_ranges takes the JSON-encoded event range
            # list and the message version, and answers
            # {'success': bool, 'message': str, 'data': {'Returns': [...], 'Commands': {...}}}
            data = {'event_ranges': req.update_events.get('eventRanges'),
                    'version': req.update_events.get('version', 0)}
            url = environ.get('PANDA_SERVER_URL', config.Pilot.pandaserver)
            res = https.request2(f'{url}/api/v1/event/update_event_ranges', json_body=data, panda=True)

            logger.info(f"Updated event ranges status: {res}")
            if isinstance(res, dict) and res.get('success'):
                resp_attrs = {'status': 0, 'content': res.get('data'), 'exception': None}
            else:
                logger.warning(f"event range update not acknowledged by the server: {res}")
                resp_attrs = {'status': -1, 'content': res, 'exception': None}
            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
            logger.error(f"Failed to update event ranges: {e}, {traceback.format_exc()}")
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException(f"Failed to update events: {traceback.format_exc()}")}
            resp = CommunicationResponse(resp_attrs)

        self.update_events_lock.release()

        return resp

    def pre_check_update_jobs(self, req: Any = None) -> Any:
        """Check whether it's ok to update jobs.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        try:
            self.update_jobs_lock.acquire()

            self.update_jobs_lock.release()
        except Exception as exc:
            logger.error(f"failed in pre_check_update_jobs: {exc}, {traceback.format_exc()}")
        return CommunicationResponse({'status': 0})

    def update_job(self, job: Any) -> int:
        """Update job.

        Args:
            job: job definition.

        Returns:
            int: status code.
        """
        try:
            logger.info(f"Updating job: {job}")
            url = environ.get('PANDA_SERVER_URL', config.Pilot.pandaserver)
            res = https.request(f'{url}/server/panda/updateJob', data=job)

            logger.info(f"Updated jobs status: {res}")
            return res
        except Exception as exc:
            logger.error(f"failed to update jobs: {exc}, {traceback.format_exc()}")
            return -1

    def update_jobs(self, req: Any) -> Any:
        """Update jobs.

        Args:
            req: request.

        Returns:
            Any: CommunicationResponse with status 0.
        """
        self.update_jobs_lock.acquire()

        try:
            logger.info(f"Updating jobs: {req}")
            res_list = []
            for job in req.jobs:
                res = self.update_job(job)
                res_list.append(res)
            resp_attrs = {'status': 0, 'content': res_list, 'exception': None}
            resp = CommunicationResponse(resp_attrs)
        except Exception as e:  # Python 2/3
            logger.error(f"Failed to update jobs: {e}, {traceback.format_exc()}")
            resp_attrs = {'status': -1, 'content': None, 'exception': exception.UnknownException(f"Failed to update jobs: {traceback.format_exc()}")}
            resp = CommunicationResponse(resp_attrs)

        self.update_jobs_lock.release()

        return resp
