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
# - Wen Guan, wen.guan@cern.ch, 2017-18
# - Paul Nilsson, paul.nilsson@cern.ch, 2023

import hashlib


def get_rucio_path(scope, name):
    """
    Construct Rucio standard path using the scope and the LFN
    """

    s = '%s:%s' % (scope, name)
    hash_hex = hashlib.md5(s.encode('utf-8')).hexdigest()  # Python 2/3

    paths = scope.split('.') + [hash_hex[0:2], hash_hex[2:4], name]

    return '/'.join(paths)
