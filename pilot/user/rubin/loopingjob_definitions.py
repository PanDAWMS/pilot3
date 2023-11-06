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


def allow_loopingjob_detection():
    """
    Should the looping job detection algorithm be allowed?
    The looping job detection algorithm finds recently touched files within the job's workdir. If a found file has not
    been touched during the allowed time limit (see looping job section in util/default.cfg), the algorithm will kill
    the job/payload process.

    :return: boolean.
    """

    return True


def remove_unwanted_files(workdir, files):
    """
    Remove files from the list that are to be ignored by the looping job algorithm.

    :param workdir: working directory (string). Needed in case the find command includes the workdir in the list of
    recently touched files.
    :param files: list of recently touched files (file names).
    :return: filtered files list.
    """

    _files = []
    for _file in files:
        if not (workdir == _file or
                "prmon" in _file or
                "pilotlog" in _file or
                ".lib.tgz" in _file or
                ".py" in _file or
                "pandaJob" in _file):
            _files.append(_file)

    return _files
