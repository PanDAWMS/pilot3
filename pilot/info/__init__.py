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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-24

"""
Pilot Information component.

A set of low-level information providers to aggregate, prioritize (overwrite),
hide dependency to external storages and expose (queue, site, storage, etc) details
in a unified structured way to all Pilot modules by providing high-level API

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""


import logging
from collections import namedtuple
from typing import Any

from pilot.common.exception import PilotException
from .infoservice import InfoService
from .jobinfo import JobInfoProvider  # noqa
from .jobdata import JobData          # noqa
from .filespec import FileSpec        # noqa

logger = logging.getLogger(__name__)


def set_info(args: Any):   ## should be DEPRECATED: use `infosys.init(queuename)`
    """
    Set up all necessary site information for given PandaQueue name.

    Resolve everything from the specified queue name (passed via `args.queue`)
    and fill extra lookup structure (Populate `args.info`).

    :param args: input (shared) arguments (Any)
    :raises PilotException: in case of errors.
    """
    # initialize info service
    infosys.init(args.queue)

    args.info = namedtuple('info', ['queue', 'infoservice',
                                    # queuedata,
                                    'site', 'storages',
                                    # 'site_info',
                                    'storages_info'])
    args.info.queue = args.queue
    args.info.infoservice = infosys  # THIS is actually for tests and redundant - the pilot.info.infosys should be used
    # args.infoservice = infosys  # ??

    # check if queue is ACTIVE
    if infosys.queuedata.state != 'ACTIVE':
        logger.critical(f'specified queue is NOT ACTIVE: {infosys.queuedata.name} -- aborting')
        raise PilotException("Panda Queue is NOT ACTIVE")

    # do we need explicit varible declaration (queuedata)?
    # same as args.location.infoservice.queuedata
    #args.location.queuedata = infosys.queuedata

    # do we need explicit varible declaration (Experiment site name)?
    # same as args.location.infoservice.queuedata.site
    #args.location.site = infosys.queuedata.site

    # do we need explicit varible declaration (storages_info)?
    # same as args.location.infoservice.storages_info
    #args.location.storages_info = infosys.storages_info

    # find all enabled storages at site
    args.info.storages = [ddm for ddm, dat in list(infosys.storages_info.items()) if dat.site == infosys.queuedata.site]

    #args.info.sites_info = infosys.sites_info


# global InfoService Instance without Job specific settings applied (singleton shared object)
# normally we should create such instance for each job to properly consider overwrites coming from JonInfoProvider
# Initialization required to access the data
infosys = InfoService()
