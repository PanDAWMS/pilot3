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
# - Alexey Anisenkov, anisyonk@cern.ch, 2018-19
# - Paul Nilsson, paul.nilsson@cern.ch, 2019-25


"""
The implementation of data structure to host queuedata settings.

The main reasons for such incapsulation are to
 - apply in one place all data validation actions (for attributes and values)
 - introduce internal information schema (names of attribues) to remove dependency
 with data structrure, formats, names from external sources (e.g. AGIS/CRIC)

This module should be standalone as much as possible and even does not depend
on the configuration settings
(for that purposed `PilotConfigProvider` can be user to customize data)

:author: Alexey Anisenkov
:contact: anisyonk@cern.ch
:date: January 2018
"""

import logging
import re
from typing import Any

from .basedata import BaseData

logger = logging.getLogger(__name__)


class QueueData(BaseData):
    """
    High-level object to host all queuedata settings associated to given PandaQueue
    """

    # ## put explicit list of all the attributes with comments for better inline-documentation by sphinx
    # ## FIX ME LATER: use proper doc format
    # ## incomplete list of attributes .. to be extended once becomes used

    name = ""       # Name of Panda Queue
    resource = ""   # Name of Panda Resource
    appdir = ""     #
    catchall = ""   # General catchall field
    environ = ""    # Special field for key=value pairs to be added as exports to payload command
    platform = ""     # cmtconfig value
    container_options = ""  # singularity only options? to be reviewed and forced to be a dict (support options for other containers?)
    container_type = {}  # dict of container names by user as a key
    resource_type = ""  # type of resource, e.g. 'grid', 'hpc'
    copytools = None
    acopytools = None

    ## allowed protocol schemas for requested copytool/activity
    ## if passed value (per activity) is a list, then given schemas will be used for all allowed copytools
    ## in case of dict-based value, it specifies allowed schemas per copytool for given activity
    ## e.g. {'pr':['root', 'srm'], 'pw':['webdav'], 'default':['root']}
    ##      {'pr': {'gfalcopy':['webdav'], 'pw':{'lsm':['root']}}}
    acopytools_schemas = {}
    astorages = None
    aprotocols = None
    params = {}
    state = None  # AGIS PQ state, e.g. ACTIVE
    status = ""   # PQ status, e.g. online
    site = None   # ATLAS Site name

    direct_access_lan = False  # Prefer remote io (True) or use only copy2scratch method (False) for stage-in over LAN
    direct_access_wan = False  # Prefer remote io (True) or use only copy2scratch method (False) for stage-in over WAN
    allow_lan = True  # Allow LAN access (whatever method) for stage-in
    allow_wan = False  # Allow WAN access (whatever method) for stage-in

    use_pcache = False
    maxwdir = 0    # in MB
    maxrss = 0
    maxinputsize = 0
    timefloor = 0  # The maximum time during which the pilot is allowed to start a new job, in seconds
    corecount = 1  #
    maxtime = 0  # maximum allowed lifetime for pilot to run on the resource (0 will be ignored, fallback to default)
    pledgedcpu = 0  #
    es_stageout_gap = 0  ## time gap value in seconds for ES stageout
    is_cvmfs = True  # has cvmfs installed
    memkillgrace = 100  # memory kill grace value in percentage

    altstageout = None  # allow altstageout: force (True) or disable (False) or no preferences (None)
    pilot_walltime_grace = 1.0  # pilot walltime grace factor
    pilot_rss_grace = 2.0  # pilot rss grace factor

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['timefloor', 'maxwdir', 'pledgedcpu', 'es_stageout_gap',
                   'corecount', 'maxrss', 'maxtime', 'maxinputsize', 'memkillgrace'],
             str: ['name', 'type', 'appdir', 'catchall', 'platform', 'container_options', 'container_type',
                   'resource', 'state', 'status', 'site', 'environ', 'resource_type'],
             dict: ['copytools', 'acopytools', 'astorages', 'aprotocols', 'acopytools_schemas', 'params'],
             bool: ['allow_lan', 'allow_wan', 'direct_access_lan', 'direct_access_wan', 'is_cvmfs', 'use_pcache']
             }

    def __init__(self, data: dict):
        """
        Initialize class instance.

        :param data: input dictionary of queue data settings (dict).
        """
        self.load(data)
        logger.debug(f'final parsed QueueData content:\n{self}')

    def load(self, data: dict):
        """
        Construct and initialize data from ext source

        :param data: input dictionary of queue data settings (dict).
        """
        # the translation map of the queue data attributes from external data to internal schema
        # 'internal_name':('ext_name1', 'extname2_if_any')
        # 'internal_name2':'ext_name3'

        # first defined ext field will be used
        # if key is not explicitly specified then ext name will be used as is
        ## fix me later to proper internal names if needed

        kmap = {
            'name': 'nickname',
            'resource': 'panda_resource',
            'platform': 'cmtconfig',
            'site': ('atlas_site', 'gstat'),
            'es_stageout_gap': 'zip_time_gap',
        }

        self._load_data(data, kmap)

    def resolve_allowed_schemas(self, activity: str or list, copytool: str = None) -> list:
        """
        Resolve list of allowed schemas for given activity and requested copytool based on `acopytools_schemas` settings

        :param activity: str or ordered list of transfer activity names to resolve acopytools related data (str or list)
        :param copytool: requested copytool name (str)
        :return: list of protocol schemes (list).
        """
        if not activity:
            activity = 'default'
        if isinstance(activity, str):
            activity_list = list(activity)
        else:
            activity_list = activity
        if 'default' not in activity_list:
            activity_list.append('default')

        adat = {}
        for aname in activity_list:
            adat = self.acopytools_schemas.get(aname)
            if adat:
                break
        if not adat:
            return []

        if not isinstance(adat, dict):
            adat = {'default': adat}

        if not copytool or copytool not in adat:
            copytool = 'default'

        return adat.get(copytool) or []

    def allow_altstageout(self):
        """
        Resolve if alternative stageout should be forced (True) or disabled (False); None value means no preferences defined.
        :return: boolean or None
        """

        val = self.params.get('allow_altstageout', None)
        if val is not None:  # cast to bool
            return bool(val)

    def set_pilot_walltime_grace(self):
        """Set pilot walltime grace factor based on the queuedata settings."""
        try:
            _pilot_walltime_grace = float(self.params.get('pilot_walltime_grace', 0))
            self.pilot_walltime_grace = 1.0 + _pilot_walltime_grace / 100.0
        except (ValueError, TypeError) as e:
            logger.warning(f"failed to set pilot_walltime_grace: {e}")
            self.pilot_walltime_grace = 1.0

    def set_pilot_rss_grace(self):
        """Set pilot rss grace factor based on the queuedata settings."""
        try:
            _pilot_rss_grace = float(self.params.get('pilot_rss_grace', 100))
            self.pilot_rss_grace = 1.0 + _pilot_rss_grace / 100.0
        except (ValueError, TypeError) as e:
            logger.warning(f"failed to set pilot_rss_grace: {e}")
            self.pilot_rss_grace = 2.0

    def clean(self):
        """Validate and finally clean up required data values (required object properties) if needed."""
        # validate es_stageout_gap value
        if not self.es_stageout_gap:
            is_opportunistic = self.pledgedcpu and self.pledgedcpu == -1
            self.es_stageout_gap = 600 if is_opportunistic else 7200  ## 10 mins for opportunistic or 5 hours for normal resources

        # validate container_options: extract from the catchall if not set
        if not self.container_options and self.catchall:
            # expected format
            # of catchall = "singularity_options=\'-B /etc/grid-security/certificates,/cvmfs,${workdir} --contain\'"
            pattern = re.compile("singularity_options=['\"]?([^'\"]+)['\"]?")  ### FIX ME LATER: move to proper args parsing via shlex at Job class
            found = re.findall(pattern, self.catchall)
            if not found:
                pattern = re.compile("apptainer_options=['\"]?([^'\"]+)['\"]?")
                found = re.findall(pattern, self.catchall)
            if found:
                self.container_options = found[0]
                logger.info(f'container_options extracted from catchall: {self.container_options}')

        # verify container_options: add the workdir if missing
        if self.container_options:
            if "${workdir}" not in self.container_options and " --contain" in self.container_options:  ## reimplement with shlex later
                self.container_options = self.container_options.replace(" --contain", ",${workdir} --contain")
                logger.info(f"note: added missing $workdir to container_options: {self.container_options}")

        # set altstageout settings
        self.altstageout = self.allow_altstageout()

        # set pilot walltime and rss grace factors
        self.set_pilot_walltime_grace()
        self.set_pilot_rss_grace()

    ## custom function pattern to apply extra validation to the key values
    ##def clean__keyname(self, raw, value):
    ##  :param raw: raw value passed from ext source as input
    ##  :param value: preliminary cleaned and casted to proper type value
    ##
    ##    return value

    def clean__timefloor(self, raw: Any, value: int) -> int:
        """
        Verify and validate value for the timefloor key (convert to seconds).

        :param raw: raw value passed from ext source as input - unused (Any)
        :param value: preliminary cleaned and cast to proper type value (int)
        :return: timefloor value in seconds (int).
        """
        return value * 60

    def clean__container_type(self, raw: Any, value: str) -> dict:
        """
        Parse and prepare value for the container_type key.

        Expected raw data in format 'container_name:user_name;'
        E.g. container_type = 'singularity:pilot;docker:wrapper', 'apptainer:pilot;docker:wrapper'

        :param raw: raw value passed from ext source as input - unused (Any)
        :param value: preliminary cleaned and cast to proper type value (str)
        :return: dictionary of container names by user as a key (dict).
        """
        ret = {}
        val = value or ''
        for e in val.split(';'):
            dat = e.split(':')
            if len(dat) == 2:
                name, user = dat[0].strip(), dat[1].strip()
                ret[user] = name

        return ret

    def clean__container_options(self, raw: Any, value: str) -> str:
        """
        Verify and validate value for the container_options key (remove bad values)

        :param raw: raw value passed from ext source as input - unused (Any)
        :param value: preliminary cleaned and cast to proper type value (str)
        :return: cleaned container_options value (str).
        """
        return value if value.lower() not in ['none'] else ''

    def clean__corecount(self, raw: Any, value: int) -> int:
        """
        Verify and validate value for the corecount key (set to 1 if not set)

        :param raw: raw value passed from ext source as input - unused (Any)
        :param value: preliminary cleaned and cast to proper type value (int)
        :return: corecount value (int).
        """
        return value if value else 1
