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
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-23


"""
Info Service module.

The implmemtation of high-level Info Service module,
which includes a set of low-level information providers to aggregate, prioritize (overwrite),
hide dependency to external storages and expose (queue, site, storage, etc) details
in a unified structured way via provided high-level API.

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import inspect
import logging
import traceback
from typing import Any

from pilot.common.exception import PilotException, NotDefined, QueuedataFailure
from .configinfo import PilotConfigProvider
from .extinfo import ExtInfoProvider
# from .jobinfo import JobInfoProvider
from .dataloader import merge_dict_data
from .queuedata import QueueData
from .storagedata import StorageData

logger = logging.getLogger(__name__)


class InfoService:
    """High-level Information Service."""

    cache_time = 60  # default cache time in seconds

    def require_init(self, func: Any) -> Any:  # noqa
        """
        Check if object is initialized.

        Method decorator.

        :param func: function to decorate (Any)
        :return: decorated function (Any).
        """
        key = 'pandaqueue'

        def inner(self, *args: Any, **kwargs: dict) -> Any:
            """
            Inner function.

            :param args: arguments (Any)
            :param kwargs: keyword arguments (dict).
            :return: decorated function (Any)
            :raises PilotException: in case of error.
            """
            if getattr(self, key, None) is None:
                raise PilotException(f"failed to call {func.__name__}(): InfoService instance is not initialized. "
                                     f"Call init() first!")

            return func(self, *args, **kwargs)

        return inner

    def __init__(self):
        """Init class instance."""
        self.pandaqueue = None
        self.queuedata = None   ## cache instance of QueueData for PandaQueue settings
        self.queues_info = {}    ## cache of QueueData objects for PandaQueue settings
        self.storages_info = {}   ## cache of QueueData objects for DDMEndpoint settings
        #self.sites_info = {}     ## cache for Site settings
        self.confinfo = None   ## by default (when non initalized) ignore overwrites/settings from Config
        self.jobinfo = None    ## by default (when non initalized) ignore overwrites/settings from Job
        self.extinfo = ExtInfoProvider(cache_time=self.cache_time)
        self.storage_id2ddmendpoint = {}
        self.ddmendpoint2storage_id = {}

    def init(self, pandaqueue: str, confinfo: Any = None, extinfo: Any = None, jobinfo: Any = None):
        """
        Initialize InfoService instance.

        :param pandaqueue: name of PandaQueue (str)
        :param confinfo: PilotConfigProvider instance (Any)
        :param extinfo: ExtInfoProvider instance (Any)
        :param jobinfo: JobInfoProvider instance (Any)
        :raises PilotException: in case of error.
        """
        self.confinfo = confinfo or PilotConfigProvider()
        self.jobinfo = jobinfo  # or JobInfoProvider()
        self.extinfo = extinfo or ExtInfoProvider(cache_time=self.cache_time)
        self.pandaqueue = pandaqueue

        if not self.pandaqueue:
            raise PilotException('failed to initialize InfoService: panda queue name is not set')

        self.queues_info = {}     ##  reset cache data
        self.storages_info = {}   ##  reset cache data
        #self.sites_info = {}     ##  reset cache data

        try:
            self.queuedata = self.resolve_queuedata(self.pandaqueue)
        except PilotException as exc:
            logger.warning(f"failed to resolve queuedata for queue={self.pandaqueue}, error={exc}")
            raise exc
        if not self.queuedata or not self.queuedata.name:
            raise QueuedataFailure(f"failed to resolve queuedata for queue={self.pandaqueue}, wrong PandaQueue name?")

        self.resolve_storage_data()  ## prefetch details for all storages

    @classmethod
    def whoami(cls):
        """
        Return current function name being executed.

        :return: Current function name (str).
        """
        return inspect.stack()[1][3]

    @classmethod
    def _resolve_data(cls, fname: Any, providers: list = [], args: list = [], kwargs: dict = {}, merge: bool = False) -> Any:
        """
        Resolve data by calling function `fname` of passed provider objects.

        Iterate over `providers`, merge data from all providers if merge is True,
        (consider 1st success result from prioritized list if `merge` mode is False)
        and resolve data by execution function `fname` with passed arguments `args` and `kwargs`

        :param fname: name of function to be called (Any)
        :param providers: list of provider objects (list)
        :param args: list of arguments to be passed to function (list)
        :param kwargs: list of keyword arguments to be passed to function (dict)
        :param merge: if True then merge data from all providers (bool)
        :return: The result of first successful execution will be returned (Any).
        """
        ret = None
        if merge:
            providers = list(providers)
            providers.reverse()
        for provider in providers:
            fcall = getattr(provider, fname, None)
            if callable(fcall):
                try:
                    r = fcall(*(args or []), **(kwargs or {}))
                    if not merge:
                        return r
                    ret = merge_dict_data(ret or {}, r or {})
                except Exception as exc:
                    logger.warning(f"failed to resolve data ({fcall.__name__}) from provider={provider} .. skipped, error={exc}")
                    logger.warning(traceback.format_exc())

        return ret

    @require_init
    def resolve_queuedata(self, pandaqueue: str) -> Any:  ## high level API
        """
        Resolve final full queue data details.

        :param pandaqueue: name of PandaQueue (str)
        :return: `QueueData` object or None if it does not exist (Any).
        """
        cache = self.queues_info

        if pandaqueue not in cache:  # not found in cache: do load and initialize data
            # the order of providers makes the priority
            r = self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo, self.extinfo), args=[pandaqueue],
                                   kwargs={'schedconf_priority': self.resolve_schedconf_sources()},
                                   merge=True)
            queuedata = r.get(pandaqueue)
            if queuedata:
                cache[pandaqueue] = QueueData(queuedata)

        return cache.get(pandaqueue)

    #@require_init
    def resolve_storage_data(self, ddmendpoints: list = []) -> dict:  ## high level API
        """
        Resolve final full storage data details.

        :param ddmendpoints: list of DDMEndpoint names (list)
        :return: dictionary of DDMEndpoint settings by DDMEndpoint name as a key (dict)
        :raises PilotException: in case of error.
        """
        if isinstance(ddmendpoints, str):
            ddmendpoints = [ddmendpoints]

        cache = self.storages_info

        miss_objs = set(ddmendpoints) - set(cache)
        if not ddmendpoints or miss_objs:  # not found in cache: do load and initialize data
            # the order of providers makes the priority
            r = self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo, self.extinfo),
                                   args=[miss_objs], merge=True)
            if ddmendpoints:
                not_resolved = set(ddmendpoints) - set(r)
                if not_resolved:
                    raise PilotException(f"internal error: Failed to load storage details for ddms={sorted(not_resolved)}")
            for ddm in r:
                cache[ddm] = StorageData(r[ddm])

        return cache

    @require_init
    def resolve_schedconf_sources(self) -> Any:  ## high level API
        """
        Resolve prioritized list of source names for Schedconfig data load.

        Consider first the config settings of pilot instance (via `confinfo`)
        and then Job specific settings (via `jobinfo` instance),
        and failover to default value (LOCAL, CVMFS, AGIS, PANDA).

        :return: list of source names (list).
        """
        defval = ['LOCAL', 'CVMFS', 'CRIC', 'PANDA']

        # look up priority order: either from job, local config or hardcoded in the logic
        return self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo)) or defval

    #@require_init
    #def resolve_field_value(self, name): ## high level API
    #
    #    """
    #        Return the value from the given schedconfig field
    #
    #        :param field: schedconfig field (string, e.g. catchall)
    #        :return: schedconfig field value (string)
    #    """
    #
    #    # look up priority order: either from job, local config, extinfo provider
    #    return self._resolve_data(self.whoami(), providers=(self.confinfo, self.jobinfo, self.extinfo), args=[name])

    def resolve_ddmendpoint_storageid(self, ddmendpoint: list = []):
        """
        Resolve the map between ddmendpoint and storage_id.

        :param ddmendpoint: ddmendpoint name (list).
        """
        if not ddmendpoint or ddmendpoint not in self.ddmendpoint2storage_id:
            storages = self.resolve_storage_data(ddmendpoint)
            for storage_name, storage in storages.items():
                storage_id = storage.pk
                self.ddmendpoint2storage_id[storage_name] = storage_id
                self.storage_id2ddmendpoint[storage_id] = storage_name
                if storage.resource:
                    bucket_id = storage.resource.get('bucket_id', None)
                    if bucket_id:
                        self.storage_id2ddmendpoint[bucket_id] = storage_name

    def get_storage_id(self, ddmendpoint: str) -> int:
        """
        Return the storage_id of a ddmendpoint.

        :param ddmendpoint: ddmendpoint name (str)
        :returns storage_id: storage_id of the ddmendpoint (int)
        :raises NotDefined: when storage_id is not defined.
        """
        if ddmendpoint not in self.ddmendpoint2storage_id:
            self.resolve_ddmendpoint_storageid(ddmendpoint)

        if ddmendpoint in self.ddmendpoint2storage_id:
            storage_id = self.ddmendpoint2storage_id[ddmendpoint]
            logger.info(f"found storage id for ddmendpoint({ddmendpoint}): {storage_id}")
            return storage_id

        raise NotDefined(f"cannot find the storage id for ddmendpoint: {ddmendpoint}")

    def get_ddmendpoint(self, storage_id: int) -> str:
        """
        Return the ddmendpoint name from a storage id.

        :param storage_id: storage_id (int)
        :returns ddmendpoint: ddmendpoint name (str)
        :raises NotDefined: when ddmendpoint is not defined for the given storage id.
        """
        storage_id = int(storage_id)
        if storage_id not in self.storage_id2ddmendpoint:
            self.resolve_ddmendpoint_storageid()

        if storage_id in self.storage_id2ddmendpoint:
            ddmendpoint = self.storage_id2ddmendpoint[storage_id]
            logger.info(f"found ddmendpoint for storage id({storage_id}): {ddmendpoint}")
            return ddmendpoint

        self.resolve_storage_data()
        raise NotDefined(f"cannot find ddmendpoint for storage id: {storage_id}")
