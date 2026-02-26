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
# - Paul Nilsson, paul.nilsson@cern.ch, 2021-2024

"""API for event service data transfers.

This module provides :class:`StageInESClient` and :class:`StageOutESClient`, which
extend the generic :class:`~pilot.api.data.StageInClient` and
:class:`~pilot.api.data.StageOutClient` with event-service-specific behaviour.

The key difference from the standard staging clients is the addition of the
``objectstore`` copytool and dedicated event-service activities (``es_events_read``
for stage-in and ``es_events`` for stage-out).  During stage-in, each file's
``storage_token`` is inspected to resolve the correct DDM endpoint and, when the
path convention signals a transient object, to set the file scope to ``"transient"``.
"""

import logging
from typing import Any

from pilot.api.data import StageInClient, StageOutClient

logger = logging.getLogger(__name__)


class StageInESClient(StageInClient):
    """Stage-in client for event service data transfers.

    Extends :class:`~pilot.api.data.StageInClient` by registering the
    ``objectstore`` copytool module and mapping the ``es_events_read`` activity
    to it.  The :meth:`prepare_sources` override additionally resolves each
    file's DDM endpoint from its ``storage_token`` field.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the event service stage-in client.

        Calls the parent initializer and then registers the ``objectstore``
        copytool module (if not already present) and sets the default copytool
        for the ``es_events_read`` activity to ``objectstore``.

        Args:
            **kwargs: Keyword arguments forwarded verbatim to
                :class:`~pilot.api.data.StageInClient`. Recognized keys include
                ``infosys_instance``, ``acopytools``, ``logger``,
                ``default_copytools``, and ``trace_report``.
        """
        super().__init__(**kwargs)

        self.copytool_modules.setdefault('objectstore', {'module_name': 'objectstore'})
        self.acopytools.setdefault('es_events_read', ['objectstore'])

    def prepare_sources(self, files: list, activities: Any = None) -> None:
        """Prepare event service source files before stage-in.

        Overrides :meth:`~pilot.api.data.StagingClient.prepare_sources` to
        resolve each file's DDM endpoint from its ``storage_token`` field.  For
        every :class:`~pilot.info.filespec.FileSpec` in ``files`` that carries a
        ``storage_token``, the token is parsed into a ``storage_id`` and a
        ``path_convention``:

        - If ``path_convention`` equals ``1000`` the file scope is set to
          ``"transient"``, indicating a short-lived object-store object.
        - If a ``storage_id`` is present it is looked up via
          :meth:`~pilot.info.infoservice.InfoService.get_ddmendpoint` and the
          resolved name is written to ``fspec.ddmendpoint``.

        The ``activities`` parameter is accepted for interface compatibility with
        the base class but is not used by this implementation.

        Args:
            files: List of :class:`~pilot.info.filespec.FileSpec` objects whose
                source locations should be prepared.
            activities: Activity name or ordered list of activity names used to
                resolve storage endpoints.  Accepted for interface compatibility
                but unused in this override.

        Returns:
            None
        """
        if not self.infosys:
            self.logger.warning('infosys instance is not initialized: skip calling prepare_sources()')
            return

        if activities:
            pass  # to bypass pylint complaint about activities not used (it cannot be removed)

        for fspec in files:
            if fspec.storage_token:   ## FIX ME LATER: no need to parse each time storage_id, all this staff should be applied in FileSpec clean method
                storage_id, path_convention = fspec.get_storage_id_and_path_convention()
                if path_convention and path_convention == 1000:
                    fspec.scope = 'transient'
                if storage_id:
                    fspec.ddmendpoint = self.infosys.get_ddmendpoint(storage_id)
                logger.info(f"Processed file with storage id: {fspec}")


class StageOutESClient(StageOutClient):
    """Stage-out client for event service data transfers.

    Extends :class:`~pilot.api.data.StageOutClient` by registering the
    ``objectstore`` copytool module and mapping the ``es_events`` activity to it.
    No additional source/destination preparation is required for event service
    stage-out beyond what the parent class provides.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the event service stage-out client.

        Calls the parent initializer and then registers the ``objectstore``
        copytool module (if not already present) and sets the default copytool
        for the ``es_events`` activity to ``objectstore``.

        Args:
            **kwargs: Keyword arguments forwarded verbatim to
                :class:`~pilot.api.data.StageOutClient`. Recognized keys include
                ``infosys_instance``, ``acopytools``, ``logger``,
                ``default_copytools``, and ``trace_report``.
        """
        super().__init__(**kwargs)

        self.copytool_modules.setdefault('objectstore', {'module_name': 'objectstore'})
        self.acopytools.setdefault('es_events', ['objectstore'])
