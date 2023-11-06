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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-23

import time


class MonitoringTime(object):
    """
    A simple class to store the various monitoring task times.
    Different monitoring tasks should be executed at different intervals. An object of this class is used to store
    the time when a specific monitoring task was last executed. The actual time interval for a given monitoring tasks
    is stored in the util/default.cfg file.
    """

    def __init__(self):
        """
        Return the initial MonitoringTime object with the current time as start values.
        """

        ct = int(time.time())
        self.ct_start = ct
        self.ct_proxy = ct
        self.ct_looping = ct
        self.ct_looping_last_touched = None
        self.ct_diskspace = ct
        self.ct_memory = ct
        self.ct_process = ct
        self.ct_heartbeat = ct
        self.ct_kill = ct
        self.ct_lease = ct

    def update(self, key, modtime=None):
        """
        Update a given key with the current time or given time.
        Usage: mt=MonitoringTime()
               mt.update('ct_proxy')

        :param key: name of key (string).
        :param modtime: modification time (int).
        :return:
        """

        ct = int(time.time()) if not modtime else modtime
        if hasattr(self, key):
            setattr(self, key, ct)

    def get(self, key):
        """
        Return the value for the given key.
        Usage: mt=MonitoringTime()
               mt.get('ct_proxy')
        The method throws an AttributeError in case of no such key.

        :param key: name of key (string).
        :return: key value (int).
        """

        return getattr(self, key)
