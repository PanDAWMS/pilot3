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
# - Alexey Anisenkov, anisyonk@cern.ch, 2018
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

"""
Job specific Info Service
It could customize/overwrite settings provided by the main Info Service

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

from .infoservice import InfoService
from .jobinfo import JobInfoProvider

import logging
logger = logging.getLogger(__name__)


class JobInfoService(InfoService):  ## TO BE DEPRECATED/REMOVED
    """
        Info service: Job specific
        Job could overwrite settings provided by Info Service

        *** KEPT for a while in repo .. most probably will be deprecated and removed soon **
    """

    def __init__(self, job):

        self.jobinfo = JobInfoProvider(job)
