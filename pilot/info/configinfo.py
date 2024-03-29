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
Pilot Config specific info provider.

Mainly used to customize Queue, Site, etc data of Information Service with details fetched directly from local
Pilot instance configuration.

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import ast
import logging
from typing import Any

from ..util.config import config

logger = logging.getLogger(__name__)


class PilotConfigProvider:
    """
    Pilot Config provider class.

    Info provider which is used to extract settings specific for local Pilot instance
    and overwrite general configuration used by Information Service.
    """

    config = None  # Pilot Config instance

    def __init__(self, conf: Any = None):
        """
        Init class instance.

        :param conf: Pilot Config instance (Any).
        """
        self.config = conf or config

    def resolve_schedconf_sources(self) -> None:
        """
        Resolve prioritized list of source names to be used for SchedConfig data load.

        Could return a prioritized list of source names (list).
        """
        # ## FIX ME LATER
        # an example of return data:
        # return ['AGIS', 'LOCAL', 'CVMFS']

        return None  # ## Not implemented yet

    def resolve_queuedata(self, pandaqueue: str, **kwargs: dict) -> dict:
        """
        Resolve queue data details.

        :param pandaqueue: name of PandaQueue (str)
        :param kwargs: other parameters (dict)
        :return: dictionary of settings for given PandaQueue as a key (dict).
        """
        data = {
            'maxwdir_broken': self.config.Pilot.maximum_input_file_sizes,  # ## Config API is broken -- FIXME LATER
            #'container_type': 'singularity:pilot;docker:wrapper',  # ## for testing
            #'container_options': '-B /cvmfs,/scratch,/etc/grid-security --contain',  ## for testing
            #'catchall': "singularity_options='-B /cvmfs000' catchx=1",  ## for testing
            'es_stageout_gap': 601,  # in seconds, for testing: FIXME LATER,
        }

        if hasattr(self.config.Information, 'acopytools'):  ## FIX ME LATER: Config API should reimplemented/fixed later
            data['acopytools'] = ast.literal_eval(self.config.Information.acopytools)

        logger.info(f'queuedata: following keys will be overwritten by config values: {data}')

        return {pandaqueue: data}
