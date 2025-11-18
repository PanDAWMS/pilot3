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
# - Alexey Anisenkov, anisyonk@cern.ch, 2018-2021
# - Paul Nilsson, paul.nilsson@cern.ch, 2018-2024

"""
Information provider from external source(s).

Mainly used to retrieve Queue, Site, etc data required for the Information Service.

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import os
import json
import random
import logging
from typing import Any

from pilot.common.errorcodes import ErrorCodes
from pilot.common.exception import PilotException
from pilot.util.config import config
from .dataloader import DataLoader, merge_dict_data

logger = logging.getLogger(__name__)


class ExtInfoProvider(DataLoader):
    """
    Information provider to retrive data from external source(s).

    E.g. CRIC, PanDA, CVMFS.
    """

    def __init__(self, cache_time: int = 60):
        """
        Initialize  class instance.

        :param cache_time: default cache time in seconds (int).
        """
        self.cache_time = cache_time

    @classmethod
    def load_schedconfig_data(cls, pandaqueues: list = None, priority: list = None, cache_time: int = 60) -> dict:
        """
        Download the (CRIC-extended) data associated to PandaQueue from various sources (prioritized).

        Try to get data from CVMFS first, then CRIC or from Panda JSON sources (not implemented).
        At the moment PanDA source does not provide the full schedconfig description.

        :param pandaqueues: list of PandaQueues to be loaded (list)
        :param priority: list of sources to be used for data load (list)
        :param cache_time: default cache time in seconds (int).
        :return: dict of schedconfig settings by PandaQueue name as a key (dict).
        """
        if pandaqueues is None:
            pandaqueues = []
        if priority is None:
            priority = []
        pandaqueues = sorted(set(pandaqueues))

        cache_dir = config.Information.cache_dir
        if not cache_dir:
            cache_dir = os.environ.get('PILOT_HOME', '.')

        cric_url = getattr(config.Information, 'queues_url', None) or 'https://atlas-cric.cern.ch/cache/schedconfig/{pandaqueue}.json'
        cric_url = cric_url.format(pandaqueue=pandaqueues[0] if len(pandaqueues) == 1 else 'pandaqueues')
        cvmfs_path = cls.get_cvmfs_path(config.Information.queues_cvmfs, 'cric_pandaqueues.json')

        sources = {'CVMFS': {'url': cvmfs_path,
                             'nretry': 1,
                             'fname': os.path.join(cache_dir, 'agis_schedconf.cvmfs.json')},
                   'CRIC': {'url': cric_url,
                            'nretry': 3,
                            'sleep_time': lambda: 15 + random.randint(0, 30),  ## max sleep time 45 seconds between retries
                            'cache_time': 3 * 60 * 60,  # 3 hours
                            'fname': os.path.join(cache_dir, f"agis_schedconf.agis.{pandaqueues[0] if len(pandaqueues) == 1 else 'pandaqueues'}.json")},
                   'LOCAL': {'url': os.environ.get('LOCAL_AGIS_SCHEDCONF'),
                             'nretry': 1,
                             'cache_time': 3 * 60 * 60,  # 3 hours
                             'fname': os.path.join(cache_dir, getattr(config.Information, 'queues_cache', None) or 'agis_schedconf.json')},
                   'PANDA': None  ## NOT implemented, FIX ME LATER
                   }

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.setup', globals(), locals(), [pilot_user], 0)
        queuedata_source_priority = user.get_schedconfig_priority()
        priority = priority or queuedata_source_priority
        logger.debug(f'schedconfig priority={priority}')

        return cls.load_data(sources, priority, cache_time)

    @staticmethod
    def get_cvmfs_path(url: str, fname: str) -> str:
        """
        Return a proper path for cvmfs.

        :param url: URL (string).
        :param fname: file name for CRIC JSON (string).
        :return: cvmfs path (string).
        """
        if url:
            cvmfs_path = url.replace('CVMFS_PATH', os.environ.get('ATLAS_SW_BASE', '/cvmfs'))
        else:
            cvmfs_path = f"{os.environ.get('ATLAS_SW_BASE', '/cvmfs')}/atlas.cern.ch/repo/sw/local/etc/{fname}"

        return cvmfs_path

    @classmethod
    def load_queuedata(cls, pandaqueue: str, priority: list = None, cache_time: int = 60) -> dict:
        """
        Download the queuedata from various sources (prioritized).

        Try to get data from PanDA, CVMFS first, then CRIC.

        This function retrieves only min information of queuedata provided by PanDA cache for the moment.

        :param pandaqueue: PandaQueue name (str)
        :param priority: list of sources to be used for data load (list)
        :param cache_time: default cache time in seconds (str)
        :return: dict of queuedata settings by PandaQueue name as a key (dict)
        :raises PilotException: in case of error.
        """
        if priority is None:
            priority = []
        if not pandaqueue:
            raise PilotException('load_queuedata(): pandaqueue name is not specififed', code=ErrorCodes.QUEUEDATA)

        pandaqueues = [pandaqueue]

        cache_dir = config.Information.cache_dir
        if not cache_dir:
            cache_dir = os.environ.get('PILOT_HOME', '.')

        def jsonparser_panda(dat: Any) -> dict:
            """
            Parse json data from PanDA source.

            :param dat: data (Any)
            :return: parsed data (dict)
            :raises Exception: in case of error.
            """
            _dat = json.loads(dat)
            if _dat and isinstance(_dat, dict) and 'error' in _dat:
                raise PilotException(f'response contains error, data={_dat}', code=ErrorCodes.QUEUEDATA)

            return {pandaqueue: _dat}

        x509_org = os.environ.get('X509_USER_PROXY', '')
        logger.debug(f"x509_org='{x509_org}'")
        _url = os.environ.get('QUEUEDATA_SERVER_URL')
        queuedata_url = (_url or getattr(config.Information, 'queuedata_url', '')).format(**{'pandaqueue': pandaqueues[0]})
        logger.debug(f'xxx queuedata url={queuedata_url}')
        logger.debug(f'xxx QUEUEDATA_SERVER_URL={_url}')
        _inf = getattr(config.Information, 'queuedata_url', '')
        logger.debug(f'xxx config.Information={_inf}')
        logger.debug(f'xxx queuename={pandaqueues[0]}')
        cric_url = getattr(config.Information, 'queues_url', None)
        logger.debug(f'xxx cric url={cric_url}')
        cric_url = cric_url.format(pandaqueue=pandaqueues[0] if len(pandaqueues) == 1 else 'pandaqueues')
        cvmfs_path = cls.get_cvmfs_path(getattr(config.Information, 'queuedata_cvmfs', None), 'cric_pandaqueues.json')

        sources = {'CVMFS': {'url': cvmfs_path,
                             'nretry': 1,
                             'fname': os.path.join(cache_dir, 'agis_schedconf.cvmfs.json')},
                   'CRIC': {'url': cric_url,
                            'nretry': 3,
                            'sleep_time': lambda: 15 + random.randint(0, 30),  # max sleep time 45 seconds between retries
                            'cache_time': 3 * 60 * 60,  # 3 hours
                            'fname': os.path.join(cache_dir, f"agis_schedconf.agis.{pandaqueues[0] if len(pandaqueues) == 1 else 'pandaqueues'}.json")},
                   'LOCAL': {'url': None,
                             'nretry': 1,
                             'cache_time': 3 * 60 * 60,  # 3 hours
                             'fname': os.path.join(cache_dir, getattr(config.Information, 'queuedata_cache', None) or 'queuedata.json'),
                             'parser': jsonparser_panda
                             },
                   'PANDA': {'url': queuedata_url,
                             'nretry': 3,
                             'sleep_time': lambda: 15 + random.randint(0, 30),  # max sleep time 45 seconds between retries
                             'cache_time': 3 * 60 * 60,  # 3 hours,
                             'fname': os.path.join(cache_dir, getattr(config.Information, 'queuedata_cache', None) or 'queuedata.json'),
                             'parser': jsonparser_panda
                             }
                   }

        logger.debug(f'xxx sources={sources}')
        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.setup', globals(), locals(), [pilot_user], 0)
        queuedata_source_priority = user.get_queuedata_priority()
        priority = priority or queuedata_source_priority
        logger.debug(f'queuedata priority={priority}')

        return cls.load_data(sources, priority, cache_time)

    @classmethod
    def load_storage_data(cls, ddmendpoints: list = None, priority: list = None, cache_time: int = 60) -> dict:
        """
        Download DDM Storages details by given name (DDMEndpoint) from various sources (prioritized).

        Unless specified as an argument in the function call, the prioritized list will be read from the user plug-in.

        :param ddmendpoints: list of ddmendpoint names (list)
        :param priority: list of sources to be used for data load (list)
        :param cache_time: default cache time in seconds (int)
        :return: dictionary of DDMEndpoint settings by DDMendpoint name as a key (dict).
        """
        if ddmendpoints is None:
            ddmendpoints = []
        if priority is None:
            priority = []
        ddmendpoints = sorted(set(ddmendpoints))

        cache_dir = config.Information.cache_dir
        if not cache_dir:
            cache_dir = os.environ.get('PILOT_HOME', '.')

        # list of sources to fetch ddmconf data from
        _storagedata_url = os.environ.get('STORAGEDATA_SERVER_URL', '')
        storagedata_url = _storagedata_url if _storagedata_url else getattr(config.Information, 'storages_url', None)
        cvmfs_path = cls.get_cvmfs_path(config.Information.storages_cvmfs, 'cric_ddmendpoints.json')
        sources = {'USER': {'url': storagedata_url,
                            'nretry': 3,
                            'sleep_time': lambda: 15 + random.randint(0, 30),  ## max sleep time 45 seconds between retries
                            'cache_time': 3 * 60 * 60,  # 3 hours
                            'fname': os.path.join(cache_dir, f"agis_ddmendpoints.agis.{'_'.join(ddmendpoints) or 'ALL'}.json")},
                   'CVMFS': {'url': cvmfs_path,
                             'nretry': 1,
                             'fname': os.path.join(cache_dir, getattr(config.Information, 'storages_cache', None) or 'agis_ddmendpoints.json')},
                   'CRIC': {'url': (getattr(config.Information, 'storages_url', None) or 'https://atlas-cric.cern.ch/cache/ddmendpoints.json'),
                            'nretry': 3,
                            'sleep_time': lambda: 15 + random.randint(0, 30),
                            ## max sleep time 45 seconds between retries
                            'cache_time': 3 * 60 * 60,
                            # 3 hours
                            'fname': os.path.join(cache_dir, f"agis_ddmendpoints.agis.{'_'.join(ddmendpoints) or 'ALL'}.json")},
                   'LOCAL': {'url': None,
                             'nretry': 1,
                             'cache_time': 3 * 60 * 60,  # 3 hours
                             'fname': os.path.join(cache_dir, getattr(config.Information, 'storages_cache', None) or 'agis_ddmendpoints.json')},
                   'PANDA': None  ## NOT implemented, FIX ME LATER if need
                   }

        pilot_user = os.environ.get('PILOT_USER', 'generic').lower()
        user = __import__(f'pilot.user.{pilot_user}.setup', globals(), locals(), [pilot_user], 0)
        ddm_source_priority = user.get_ddm_source_priority()
        if os.environ.get('PILOT_QUEUE', '') == 'GOOGLE_DASK':
            priority = ['LOCAL']
        else:
            priority = priority or ddm_source_priority
        logger.debug(f'storage data priority={priority}')

        return cls.load_data(sources, priority, cache_time)

    def resolve_queuedata(self, pandaqueue: str, schedconf_priority: list = None) -> dict:
        """
        Resolve final full queue data details.

        (primary data provided by PanDA merged with overall queue details from AGIS)

        :param pandaqueue: name of PandaQueue
        :param schedconf_priority: list of sources to be used for schedconfig data load
        :return: dictionary of settings for given PandaQueue as a key (dict).
        """
        # load queuedata (min schedconfig settings)
        master_data = self.load_queuedata(pandaqueue, cache_time=self.cache_time)  ## use default priority

        # load full queue details
        r = self.load_schedconfig_data([pandaqueue], priority=schedconf_priority, cache_time=self.cache_time)

        # merge
        return merge_dict_data(r, master_data)

    def resolve_storage_data(self, ddmendpoints: list = None) -> dict:
        """
        Resolve final DDM Storages details by given names (DDMEndpoint).

        :param ddmendpoints: list of ddmendpoint names (list)
        :return: dictionary of settings for given DDMEndpoint as a key (dict).
        """
        if ddmendpoints is None:
            ddmendpoints = []
        # load ddmconf settings
        return self.load_storage_data(ddmendpoints, cache_time=self.cache_time)  ## use default priority
