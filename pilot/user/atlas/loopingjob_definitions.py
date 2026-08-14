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
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-26

"""Looping-job detection settings for the ATLAS experiment plugin."""

from __future__ import annotations
from os.path import join


def allow_loopingjob_detection() -> bool:
    """Check of the looping job detection algorithm should be allowed to run.

    The looping job detection algorithm finds recently touched files within the job's workdir. If a found file has not
    been touched during the allowed time limit (see looping job section in util/default.cfg), the algorithm will kill
    the job/payload process.

    Returns:
        bool: True if allowed, False otherwise.
    """
    return True


def remove_unwanted_files(workdir: str, files: list[str]) -> list[str]:
    """Remove files from the list that are to be ignored by the looping job algorithm.

    Args:
        workdir: working directory.
        files: recently touched files.

    Returns:
        list[str]: filtered files.
    """
    _files = []
    for _file in files:
        if not (workdir == _file or
                _file == join(workdir, 'workDir') or
                "prmon" in _file or
                "/pilot/" in _file or
                _file.endswith('/pilot') or
                "/pandawnutil" in _file or
                "pilotlog" in _file or
                ".lib.tgz" in _file or
                ".py" in _file or
                "PoolFileCatalog" in _file or
                "setup.sh" in _file or
                "pandaJob" in _file or
                "runjob" in _file or
                "memory_" in _file or
                "mem." in _file or
                "docs/" in _file or
                "DBRelease-" in _file or
                _file == ""):
            _files.append(_file)

    return _files


def get_payload_process_names() -> list:
    """Return the process names that identify the actual payload.

    Used by the looping job diagnostics to pick which process to snapshot and
    dump when a job is found to be looping, in preference to an arbitrary
    descendant of the payload process. Matched case-insensitively as substrings
    against the full command line.

    Deliberately empty for now. What an ATLAS payload tree actually contains
    during a loop has not been established, and a name that matches the wrong
    process is worse than no name at all: it would promote that process above
    the real payload, whereas an empty list simply leaves the ranking to
    accumulated CPU time and resident set. The pilot logs the complete,
    unfiltered inventory of the payload tree (grep 'PAYLOAD PROCESS INVENTORY'
    in the job log) precisely so that this list can be filled in from real
    looping jobs rather than guessed at in advance.

    Returns:
        list: process name fragments; empty until derived from logged inventories.
    """
    return []
