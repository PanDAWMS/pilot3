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
# - Paul Nilsson, paul.nilsson@cern.ch, 2022-24

from hashlib import md5


def mv_to_final_destination() -> bool:
    """
    Check if mv is allowed to move files to/from final destination.

    In ATLAS, the Pilot will only move the output to a local directory. The aCT will pick it up from there and move it
    to the final destination.

    :return: True if allowed, False otherwise (bool).
    """
    return False


def get_path(scope: str, lfn: str) -> str:
    """
    Construct a partial Rucio PFN using the scope and the LFN.

    <scope>/md5(<scope>:<lfn>)[0:2]/md5(<scope:lfn>)[2:4]/<lfn>

    E.g. scope = 'user.jwebb2', lfn = 'user.jwebb2.66999._000001.top1outDS.tar'
        -> 'user/jwebb2/01/9f/user.jwebb2.66999._000001.top1outDS.tar'

    :param scope: scope (str)
    :param lfn: LFN (str)
    :return: partial rucio path (str).
    """
    s = f'{scope}:{lfn}'
    hash_hex = md5(s.encode('utf-8')).hexdigest()
    paths = scope.split('.') + [hash_hex[0:2], hash_hex[2:4], lfn]
    paths = [_f for _f in paths if _f]  # remove empty parts to avoid double /-chars

    return '/'.join(paths)
