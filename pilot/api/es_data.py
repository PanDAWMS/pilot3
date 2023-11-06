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
# - Wen Guan, wen.guan@cern,ch, 2018
# - Alexey Anisenkov, anisyonk@cern.ch, 2019
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-2023

"""API for event service data transfers."""

import logging

from pilot.api.data import StageInClient, StageOutClient

logger = logging.getLogger(__name__)


class StageInESClient(StageInClient):
    """Stage-in client."""

    def __init__(self, *argc, **kwargs):
        """Set default/init values."""
        super(StageInESClient, self).__init__(*argc, **kwargs)

        self.copytool_modules.setdefault('objectstore', {'module_name': 'objectstore'})
        self.acopytools.setdefault('es_events_read', ['objectstore'])

    def prepare_sources(self, files, activities=None):
        """
        Prepare sources.

        Customize/prepare source data for each entry in `files` optionally checking data for requested `activities`
        (custom StageClient could extend this logic if needed).

        If storage_id is specified, replace ddmendpoint by parsing storage_id.

        :param files: list of `FileSpec` objects to be processed
        :param activities: string or ordered list of activities to resolve `astorages` (optional)
        :return: None.
        """
        if not self.infosys:
            self.logger.warning('infosys instance is not initialized: skip calling prepare_sources()')
            return None

        for fspec in files:
            if fspec.storage_token:   ## FIX ME LATER: no need to parse each time storage_id, all this staff should be applied in FileSpec clean method
                storage_id, path_convention = fspec.get_storage_id_and_path_convention()
                if path_convention and path_convention == 1000:
                    fspec.scope = 'transient'
                if storage_id:
                    fspec.ddmendpoint = self.infosys.get_ddmendpoint(storage_id)
                logger.info(f"Processed file with storage id: {fspec}")
        return None


class StageOutESClient(StageOutClient):
    """Stage-out client."""

    def __init__(self, *argc, **kwargs):
        """Set default/init values."""
        super(StageOutESClient, self).__init__(*argc, **kwargs)

        self.copytool_modules.setdefault('objectstore', {'module_name': 'objectstore'})
        self.acopytools.setdefault('es_events', ['objectstore'])
