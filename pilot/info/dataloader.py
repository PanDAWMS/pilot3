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
Base loader class to retrieve data from Ext sources (file, url).

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import json
import logging
import os
import time
from datetime import (
    datetime,
    timedelta
)
from typing import Any

from pilot.util.timer import timeout
from pilot.util.https import download_file

logger = logging.getLogger(__name__)


class DataLoader:
    """Base data loader."""

    @classmethod
    def is_file_expired(cls, fname: str, cache_time: int = 0) -> bool:
        """
        Check if file fname is older than cache_time seconds from its last_update_time.

        :param fname: file name (str)
        :param cache_time: cache time in seconds (int)
        :return: True if file is expired, False otherwise (bool).
        """
        if cache_time:
            lastupdate = cls.get_file_last_update_time(fname)
            return not (lastupdate and datetime.now() - lastupdate < timedelta(seconds=cache_time))

        return True

    @classmethod
    def get_file_last_update_time(cls, fname: str) -> datetime or None:
        """
        Return the last update time of the given file.

        :param fname: file name (str)
        :return: last update time in seconds or None if file does not exist (datetime or None).
        """
        try:
            lastupdate = datetime.fromtimestamp(os.stat(fname).st_mtime)
        except OSError:
            lastupdate = None

        return lastupdate

    @classmethod  # noqa: C901
    def load_url_data(cls, url: str, fname: str = None, cache_time: int = 0, nretry: int = 3, sleep_time: int = 60) -> Any:  # noqa: C901
        """
        Download data from url or file resource and optionally save it into cache file fname.

        The file will not be (re-)loaded again if cache age from last file modification does not exceed cache_time
        seconds.

        If url is None then data will be read from cache file fname (if any).

        :param url: URL to source of data (str)
        :param fname: cache file name. If given then loaded data will be saved into it (str)
        :param cache_time: cache time in seconds (int)
        :param nretry: number of retries (default is 3) (int)
        :param sleep_time: sleep time (default is 60 s) between retry attempts (int)
        :return: data loaded from the url or file content if url passed is a filename (Any).
        """
        @timeout(seconds=20)
        def _readfile(url: str) -> str:
            """
            Read file content.

            :param url: file name (str)
            :return: file content (str).
            """
            if os.path.isfile(url):
                try:
                    with open(url, "r", encoding='utf-8') as f:
                        content = f.read()
                except (OSError, UnicodeDecodeError) as exc:
                    logger.warning(f"failed to read file {url}: {exc}")
                    content = ""

                return content

            return ""

        content = None
        if url and cls.is_file_expired(fname, cache_time):  # load data into temporary cache file
            for trial in range(1, nretry + 1):
                if content:
                    break
                try:
                    native_access = '://' not in url  ## trival check for file access, non accurate.. FIXME later if need
                    if native_access:
                        logger.info(f'[attempt={trial}/{nretry}] loading data from file {url}')
                        content = _readfile(url)
                    else:
                        logger.info(f'[attempt={trial}/{nretry}] loading data from url {url}')
                        content = download_file(url)

                    if fname:  # save to cache
                        with open(fname, "w+", encoding='utf-8') as _file:
                            if isinstance(content, bytes):  # if-statement will always be needed for python 3
                                content = content.decode("utf-8")

                            if content:
                                _file.write(content)
                                logger.info(f'saved data from \"{url}\" resource into file {fname}, '
                                            f'length={len(content) / 1024.:.1f} kB')
                            else:
                                logger.warning('no data to save into cache file')
                                continue

                    return content
                except Exception as exc:  # ignore errors, try to use old cache if any
                    logger.warning(f"failed to load data from url {url}, error: {exc} .. trying to use data from cache={fname}")
                    # will try to use old cache below
                    if trial < nretry:
                        xsleep_time = sleep_time() if callable(sleep_time) else sleep_time
                        logger.info(f"will try again after {xsleep_time} s..")
                        time.sleep(xsleep_time)

        if content is not None:  # just loaded data
            return content

        # read data from old cache fname
        try:
            with open(fname, 'r', encoding='utf-8') as f:
                content = f.read()
        except (OSError, UnicodeDecodeError) as exc:
            logger.warning(f"cache file={fname} is not available: {exc} .. skipped")
            return None

        return content

    @classmethod
    def load_data(cls, sources: dict, priority: list, cache_time: int = 60, parser: Any = None) -> Any:
        """
        Download data from various sources (prioritized).

        Try to get data from sources according to priority values passed

        Expected format of source entry:
        sources = {'NAME':{'url':"source url", 'nretry':int, 'fname':'cache file (optional)',
                   'cache_time':int (optional), 'sleep_time':opt}}

        :param sources: dict of source configuration (dict)
        :param priority: ordered list of source names (list)
        :param cache_time: default cache time in seconds. Can be overwritten by cache_time value passed in sources (dict)
        :param parser: callback function to interpret/validate data which takes read data from source as input. Default is json.loads (Any)
        :return: data loaded and processed by parser callback (Any)
        """
        if not priority:  # no priority set ## randomly order if need (FIX ME LATER)
            priority = list(sources.keys())

        for key in priority:
            dat = sources.get(key)
            if not dat:
                continue

            accepted_keys = ['url', 'fname', 'cache_time', 'nretry', 'sleep_time']
            idat = dict([k, dat.get(k)] for k in accepted_keys if k in dat)
            idat.setdefault('cache_time', cache_time)

            content = cls.load_url_data(**idat)
            if isinstance(content, bytes):
                content = content.decode("utf-8")
                logger.debug('converted content to utf-8')
            if not content:
                continue
            if dat.get('parser'):
                parser = dat.get('parser')
            if not parser:
                def jsonparser(c):
                    dat = json.loads(c)
                    if dat and isinstance(dat, dict) and 'error' in dat:
                        raise Exception(f'response contains error, data={dat}')
                    return dat
                parser = jsonparser
            try:
                data = parser(content)
            except Exception as exc:
                logger.fatal(f"failed to parse data from source={dat.get('url')} "
                             f"(resource={key}, cache={dat.get('fname')}).. skipped, error={exc}")
                data = None
            if data:
                return data

        return None


def merge_dict_data(dic1: dict, dic2: dict, keys: list = None, common: bool = True, left: bool = True,
                    right: bool = True, rec: bool = False) -> dict:
    """
    Recursively merge two dictionary objects.

    Merge content of dic2 dict into copy of dic1.

    :param dic1: dictionary to merge into (dict)
    :param dic2: dictionary to merge from (dict)
    :param keys: list of keys to merge (list)
    :param common: if True then merge keys exist in both dictionaries (bool)
    :param left: if True then preserve keys exist only in dic1 (bool)
    :param right: if True then preserve keys exist only in dic2 (bool)
    :param rec: if True then merge recursively (bool)
    :return: merged dictionary (dict).
    """
    if keys is None:
        keys = []
    ### TODO: verify and configure logic later

    if not (isinstance(dic1, dict) and isinstance(dic2, dict)):
        return dic2

    ret = dic1.copy()

    if keys and rec:
        for k in set(keys) & set(dic2):
            ret[k] = dic2[k]
        return ret

    if common:  # common
        for k in set(dic1) & set(dic2):
            ret[k] = merge_dict_data(dic1[k], dic2[k], keys, rec=True)

    if not left:  # left
        for k in set(dic1) - set(dic2):
            ret.pop(k)

    if right:  # right
        for k in set(dic2) - set(dic1):
            ret[k] = dic2[k]

    return ret
