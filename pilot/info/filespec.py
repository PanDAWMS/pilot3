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
# - Paul Nilsson, paul.nilsson@cern.ch, 2022-23

"""
The implementation of data structure to host File related data description.

The main reasons for such encapsulation are to
 - apply in one place all data validation actions (for attributes and values)
 - introduce internal information schema (names of attributes) to remove direct dependency to ext storage/structures

:author: Alexey Anisenkov
:date: April 2018
"""

import logging
import os.path
from typing import Any

from .basedata import BaseData

logger = logging.getLogger(__name__)


class FileSpec(BaseData):
    """High-level object to host File Specification (meta data like lfn, checksum, replica details, etc.)."""

    ## put explicit list of all the attributes with comments for better inline-documentation by sphinx

    lfn = ""
    guid = ""
    filesize = 0
    checksum = {}    # file checksum values, allowed keys=['adler32', 'md5'], e.g. `fspec.checksum.get('adler32')`
    scope = ""       # file scope
    dataset = ""
    ddmendpoint = ""    ## DDMEndpoint name (input or output depending on FileSpec.filetype)
    accessmode = ""  # preferred access mode
    allow_lan = True
    allow_wan = False
    direct_access_lan = False
    direct_access_wan = False
    storage_token = ""  # prodDBlockToken = ""      # moved from Pilot1: suggest proper internal name (storage token?)
    ## dispatchDblock =  ""       # moved from Pilot1: is it needed? suggest proper internal name?
    ## dispatchDBlockToken = ""   # moved from Pilot1: is it needed? suggest proper internal name?
    ## prodDBlock = ""           # moved from Pilot1: is it needed? suggest proper internal name?

    ## local keys
    filetype = ''      # type of File: input, output of log
    replicas = None    # list of resolved input replicas
    protocols = None   # list of preferred protocols for requested activity
    surl = ''          # source url
    turl = ''          # transfer url
    domain = ""        # domain of resolved replica
    mtime = 0          # file modification time
    status = None      # file transfer status value
    status_code = 0    # file transfer status code
    inputddms = []     # list of DDMEndpoint names which will be considered by default (if set) as allowed local (LAN) storage for input replicas
    workdir = None     # used to declare file-specific work dir (location of given local file when it's used for transfer by copytool)
    protocol_id = None  # id of the protocol to be used to construct turl
    is_tar = False     # whether it's a tar file or not
    ddm_activity = None  # DDM activity names (e.g. [read_lan, read_wan]) which should be used to resolve appropriate protocols from StorageData.arprotocols
    checkinputsize = True
    is_altstaged = None  # indicates if file was transferred using alternative method (altstageout)

    # specify the type of attributes for proper data validation and casting
    _keys = {int: ['filesize', 'mtime', 'status_code'],
             str: ['lfn', 'guid', 'checksum', 'scope', 'dataset', 'ddmendpoint',
                   'filetype', 'surl', 'turl', 'domain', 'status', 'workdir', 'accessmode', 'storage_token'],
             list: ['replicas', 'inputddms', 'ddm_activity'],
             bool: ['allow_lan', 'allow_wan', 'direct_access_lan', 'direct_access_wan', 'checkinputsize']
             }

    def __init__(self, filetype: str = 'input', **data: dict):
        """
        Init class instance.

        FileSpec can be split into FileSpecInput + FileSpecOuput classes in case of significant logic changes.

        :param filetype: type of File: either input, output or log
        :param data: input dictionary with object description (dict)
        """
        self.filetype = filetype
        self.load(data)

    def load(self, data: dict):
        """
        Construct and initialize data from ext source for input `FileSpec`.

        :param data: input dictionary of object description.
        """
        # the translation map of the key attributes from external data to internal schema
        # if key is not explicitly specified then ext name will be used as is

        kmap = {
            # 'internal_name2': 'ext_name3'
        }

        self._load_data(data, kmap)

    ## custom function pattern to apply extra validation to the key values
    ##def clean__keyname(self, raw, value):
    ##  :param raw: raw value passed from ext source as input
    ##  :param value: preliminary cleaned and cast to proper type value
    ##
    ##    return value

    def clean__checksum(self, raw: Any, value: Any) -> dict:
        """
        Validate given value for the checksum key.

        Expected raw format is 'ad:value' or 'md:value'.

        :param raw: raw value passed from ext source as input (Any)
        :param value: preliminary cleaned and cast to proper type value (Any)
        :return: dictionary with checksum values (dict).
        """
        if isinstance(value, dict):
            return value

        cmap = {'ad': 'adler32', 'md': 'md5'}

        ctype, checksum = 'adler32', value
        cc = value.split(':')
        if len(cc) == 2:
            ctype, checksum = cc
            ctype = cmap.get(ctype) or 'adler32'

        return {ctype: checksum}

    def clean(self):
        """
        Validate and finally clean up required data values (required object properties) if needed.

        Executed once all fields have already passed field-specific validation checks.
        Could be customized by child object.
        """
        if self.lfn.startswith("zip://"):
            self.lfn = self.lfn.replace("zip://", "")
            self.is_tar = True
        elif self.lfn.startswith("gs://"):
            self.surl = self.lfn
            self.lfn = os.path.basename(self.lfn)

    def is_directaccess(self, ensure_replica: bool = True, allowed_replica_schemas: list = None) -> bool:
        """
        Check if given (input) file can be used for direct access mode by job transformation script.

        :param ensure_replica: if True then check by allowed schemas of file replica turl will be considered as well (bool)
        :param allowed_replica_schemas: list of allowed replica schemas (list)
        :return: True if file can be used for direct access mode (bool).
        """
        # check by filename pattern
        filename = self.lfn.lower()

        is_rootfile = True
        exclude_pattern = ['.tar.gz', '.lib.tgz', '.raw.']
        for exclude in exclude_pattern:
            if exclude in filename or filename.startswith('raw.'):
                is_rootfile = False
                break

        if not is_rootfile:
            return False

        _is_directaccess = False  ## default value
        if self.accessmode == 'direct':
            _is_directaccess = True
        elif self.accessmode == 'copy':
            _is_directaccess = False

        if ensure_replica:

            allowed_replica_schemas = allowed_replica_schemas or ['root', 'dcache', 'dcap', 'file', 'https']
            if not self.turl or not any(self.turl.startswith(f'{allowed}://') for allowed in allowed_replica_schemas):
                _is_directaccess = False

        return _is_directaccess

    def get_storage_id_and_path_convention(self) -> (str, str):
        """
        Parse storage_token to get storage_id and path_convention.

        Format for storage token: expected format is '<normal storage token as string>', '<storage_id as int>',
        <storage_id as int/path_convention as int>.

        :returns: storage_id (str), path_convention (str).
        """
        storage_id = None
        path_convention = None
        try:
            if self.storage_token:
                if self.storage_token.count('/') == 1:
                    storage_id, path_convention = self.storage_token.split('/')
                    storage_id = int(storage_id)
                    path_convention = int(path_convention)
                elif self.storage_token.isdigit():
                    storage_id = int(self.storage_token)
        except (ValueError, AttributeError, TypeError) as exc:
            logger.warning(f"failed to parse storage_token({self.storage_token}): {exc}")
        logger.info(f'storage_id: {storage_id}, path_convention: {path_convention}')

        return storage_id, path_convention

    def require_transfer(self) -> bool:
        """
        Check if File needs to be transferred (in error state or never has been started)
        """

        return self.status not in ['remote_io', 'transferred', 'no_transfer']
